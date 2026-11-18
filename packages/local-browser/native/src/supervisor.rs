// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// 启动固定版本的 workerd，传入私有服务 socket 和控制管道，并等待引擎 socket 的 listen
// 事件。stdout/stderr、控制管道和子进程退出并行监控；关闭时先发 SIGTERM，超时后升级为
// SIGKILL，并回收子进程。

use std::ffi::OsString;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::{timeout, Instant};

use crate::observe::Observer;
use crate::transport::UpstreamAddress;
use crate::HostResult;

const CONTROL_FD: RawFd = 3;
const LISTENER_FD: RawFd = 4;
const FONT_MARKER: &str = "[kitesurf-fonts] registered ";
const FONT_PHRASE: &str = " Latin UA fonts (";

pub enum EngineSocket {
    /// 该路径由 workerd 自己绑定；宿主随后连接它。
    Unix(PathBuf),
    /// 宿主已绑定监听器，并把其描述符交给 workerd。
    Inherited(TcpListener),
}

pub struct EngineSpec {
    program: PathBuf,
    args: Vec<OsString>,
    pub socket: EngineSocket,
    pub socket_name: String,
    pub startup_timeout: Duration,
    observer: Arc<Observer>,
}

impl EngineSpec {
    /// T2 命令行：私有外部 socket 按名称传入，引擎 socket 由 workerd 绑定或继承，
    /// 控制管道为 FD 3。
    pub fn workerd(
        program: impl Into<PathBuf>,
        config: &Path,
        socket: EngineSocket,
        externals: &[(String, PathBuf)],
        startup_timeout: Duration,
        observer: Arc<Observer>,
    ) -> Self {
        let mut args: Vec<OsString> = vec![
            "serve".into(),
            "--experimental".into(),
            config.as_os_str().to_owned(),
        ];
        match &socket {
            EngineSocket::Unix(path) => {
                args.push("--socket-addr".into());
                args.push(format!("engine=unix:{}", path.display()).into());
            }
            EngineSocket::Inherited(_) => {
                args.push("--socket-fd".into());
                args.push(format!("engine={LISTENER_FD}").into());
            }
        }
        for (name, path) in externals {
            args.push("--external-addr".into());
            args.push(format!("{name}=unix:{}", path.display()).into());
        }
        args.push("--control-fd".into());
        args.push(CONTROL_FD.to_string().into());
        Self {
            program: program.into(),
            args,
            socket,
            socket_name: "engine".to_string(),
            startup_timeout,
            observer,
        }
    }

    /// 测试用命令，不识别 workerd 参数，用于超时和退出测试。
    pub fn custom(
        program: impl Into<PathBuf>,
        args: Vec<OsString>,
        socket: EngineSocket,
        startup_timeout: Duration,
        observer: Arc<Observer>,
    ) -> Self {
        Self {
            program: program.into(),
            args,
            socket,
            socket_name: "engine".to_string(),
            startup_timeout,
            observer,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ListenInfo {
    pub port: u16,
}

#[derive(Clone, Copy, Debug)]
pub struct ExitInfo {
    pub code: Option<i32>,
    pub signal: Option<i32>,
}

impl ExitInfo {
    fn from_status(status: std::process::ExitStatus) -> Self {
        Self {
            code: status.code(),
            signal: status.signal(),
        }
    }
}

impl std::fmt::Display for ExitInfo {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (self.code, self.signal) {
            (Some(code), _) => write!(formatter, "code {code}"),
            (None, Some(signal)) => write!(formatter, "signal {signal}"),
            _ => write!(formatter, "status unknown"),
        }
    }
}

enum ControlMessage {
    Listen { socket: String, port: u16 },
    Invalid(String),
}

/// 引擎输出只以固定类别计数保留；原始行（含页面日志）不落盘、不回传给 client。
#[derive(Default)]
struct OutputCounts {
    stdout: AtomicU64,
    stderr: AtomicU64,
}

impl OutputCounts {
    fn bump(&self, stream: &'static str) {
        let counter = if stream == "stdout" {
            &self.stdout
        } else {
            &self.stderr
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    fn summary(&self) -> String {
        let (stdout, stderr) = self.lines();
        format!("; output lines: stdout {stdout}, stderr {stderr}")
    }

    fn lines(&self) -> (u64, u64) {
        (
            self.stdout.load(Ordering::Relaxed),
            self.stderr.load(Ordering::Relaxed),
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EngineDiagnostic {
    Uncaught,
    InternalError,
    OutputReadFailed,
}

impl EngineDiagnostic {
    pub fn category(self) -> &'static str {
        match self {
            Self::Uncaught => "uncaught",
            Self::InternalError => "internal-error",
            Self::OutputReadFailed => "output-read-failed",
        }
    }
}

struct Diagnostics {
    fatal: watch::Sender<Option<EngineDiagnostic>>,
    uncaught: AtomicU64,
    internal: AtomicU64,
    read_failed: AtomicU64,
}

impl Diagnostics {
    fn record(&self, diagnostic: EngineDiagnostic, observer: &Observer) {
        let counter = match diagnostic {
            EngineDiagnostic::Uncaught => &self.uncaught,
            EngineDiagnostic::InternalError => &self.internal,
            EngineDiagnostic::OutputReadFailed => &self.read_failed,
        };
        counter.fetch_add(1, Ordering::Relaxed);
        self.fatal.send_if_modified(|fatal| {
            if fatal.is_some() {
                return false;
            }
            *fatal = Some(diagnostic);
            true
        });
        observer.with(
            "engine-diagnostic",
            json!({ "category": diagnostic.category() }),
        );
        eprintln!("kitesurf: engine-diagnostic: {}", diagnostic.category());
    }
}

pub struct RunningEngine {
    pid: u32,
    pub socket: EngineSocket,
    control: mpsc::UnboundedReceiver<ControlMessage>,
    exit: Option<oneshot::Receiver<std::process::ExitStatus>>,
    exit_status: Option<ExitInfo>,
    counts: Arc<OutputCounts>,
    diagnostics: Arc<Diagnostics>,
    readers: Vec<JoinHandle<()>>,
    observer: Arc<Observer>,
    pub socket_name: String,
    startup_timeout: Duration,
    reaped: bool,
}

impl RunningEngine {
    /// 等待指定 socket 的 listen 事件；其他事件或提前退出均视为失败。
    pub async fn wait_ready(&mut self) -> HostResult<ListenInfo> {
        let mut fatal = self.fatal_watch();
        self.check_diagnostics()?;
        let deadline = Instant::now() + self.startup_timeout;
        let Some(exit) = self.exit.as_mut() else {
            return Err("engine exit watcher is gone".into());
        };
        let observer = self.observer.clone();
        let counts_handle = self.counts.clone();
        let socket_name = self.socket_name.clone();
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            let counts = counts_handle.summary();
            return Err(format!("engine did not listen on {socket_name} in time{counts}").into());
        }
        tokio::select! {
          _ = fatal.wait_for(|value| value.is_some()) => {
            Err("engine fatal diagnostic".into())
          }
          message = self.control.recv() => match message {
            Some(ControlMessage::Listen { socket, port }) => {
              if socket != socket_name {
                return Err(format!(
                  "engine listened on {socket}, expected {socket_name}"
                ).into());
              }
              let path = match &self.socket {
                EngineSocket::Unix(path) => Some(path.display().to_string()),
                EngineSocket::Inherited(_) => None,
              };
              observer.with(
                "engine-listen",
                json!({ "socket": socket, "port": port, "path": path }),
              );
              if self.diagnostics.fatal.borrow().is_some() {
                return Err("engine fatal diagnostic".into());
              }
              Ok(ListenInfo { port })
            }
            Some(ControlMessage::Invalid(line)) => {
              Err(format!("invalid engine control message: {line}").into())
            }
            None => {
              // 管道在退出时也会关闭：已知退出状态时优先采用退出状态。
              let recorded = timeout(Duration::from_millis(250), &mut *exit).await;
              if let Ok(status) = recorded {
                let info = ExitInfo::from_status(status.unwrap_or_else(|_| {
                  std::process::ExitStatus::from_raw(0)
                }));
                self.exit_status = Some(info);
                self.reaped = true;
                let counts = counts_handle.summary();
                Err(format!("engine exited during startup ({info}){counts}").into())
              } else {
                Err("engine control pipe closed during startup".into())
              }
            }
          },
          status = &mut *exit => {
            let info = ExitInfo::from_status(status.unwrap_or_else(|_| {
              std::process::ExitStatus::from_raw(0)
            }));
            self.exit_status = Some(info);
            self.reaped = true;
            let counts = counts_handle.summary();
            Err(format!("engine exited during startup ({info}){counts}").into())
          }
          _ = tokio::time::sleep(remaining) => {
            let counts = counts_handle.summary();
            Err(format!("engine did not listen on {socket_name} in time{counts}").into())
          }
        }
    }

    pub fn pid(&self) -> u32 {
        self.pid
    }

    pub fn endpoint(&self) -> UpstreamAddress {
        match &self.socket {
            EngineSocket::Unix(path) => UpstreamAddress::Unix(path.clone()),
            EngineSocket::Inherited(listener) => match listener.local_addr() {
                Ok(address) => UpstreamAddress::Tcp(address),
                Err(_) => UpstreamAddress::Tcp("127.0.0.1:0".parse().expect("literal")),
            },
        }
    }

    pub fn fatal_watch(&self) -> watch::Receiver<Option<EngineDiagnostic>> {
        self.diagnostics.fatal.subscribe()
    }

    pub fn check_diagnostics(&self) -> HostResult<()> {
        if self.diagnostics.fatal.borrow().is_some() {
            return Err("engine fatal diagnostic".into());
        }
        Ok(())
    }

    async fn drain_output(&mut self) {
        while let Some(reader) = self.readers.last_mut() {
            if reader.await.is_err() {
                self.diagnostics
                    .record(EngineDiagnostic::OutputReadFailed, &self.observer);
            }
            self.readers.pop();
        }
    }

    /// 等待取消不会丢失 reap receiver；返回前必须排空两条输出管道。
    pub async fn wait_exit(&mut self) -> ExitInfo {
        if self.exit_status.is_none() {
            if let Some(exit) = self.exit.as_mut() {
                let frame = exit
                    .await
                    .unwrap_or_else(|_| std::process::ExitStatus::from_raw(1 << 8));
                self.exit_status = Some(ExitInfo::from_status(frame));
                self.reaped = true;
                self.exit = None;
            }
        }
        self.drain_output().await;
        self.exit_status.unwrap_or(ExitInfo {
            code: None,
            signal: None,
        })
    }

    pub fn has_exited(&self) -> bool {
        self.exit_status.is_some()
    }

    /// 先 SIGTERM，限时等待，再 SIGKILL；等待任务在任一路径上都会回收子进程。
    pub async fn shutdown(&mut self) -> ExitInfo {
        if !self.has_exited() {
            kill(self.pid, libc::SIGTERM);
        }
        let status = match timeout(Duration::from_secs(5), self.wait_exit()).await {
            Ok(status) => status,
            Err(_) => {
                kill(self.pid, libc::SIGKILL);
                self.wait_exit().await
            }
        };
        let (stdout, stderr) = self.counts.lines();
        self.observer.with(
            "engine-exit",
            json!({ "code": status.code, "signal": status.signal, "reaped": self.reaped,
                "stdoutLines": stdout, "stderrLines": stderr,
                "fatal": self.check_diagnostics().is_err(),
                "uncaught": self.diagnostics.uncaught.load(Ordering::Relaxed),
                "internalErrors": self.diagnostics.internal.load(Ordering::Relaxed),
                "outputReadFailures": self.diagnostics.read_failed.load(Ordering::Relaxed) }),
        );
        status
    }
}

/// 只识别字体加载器自己的计数行；重新输出时仅保留两个数字字段，其他内容丢弃。
fn font_registration(line: &str) -> Option<(u64, u64)> {
    let rest = line.split_once(FONT_MARKER)?.1;
    let (families, rest) = rest.split_once(FONT_PHRASE)?;
    let (bytes, rest) = rest.split_once(" bytes)")?;
    let families = families.parse::<u64>().ok()?;
    let bytes = bytes.parse::<u64>().ok()?;
    let rest = rest.trim();
    let rest = rest.strip_prefix("in ")?;
    let rest = rest.strip_suffix("ms")?;
    rest.parse::<f64>().ok()?;
    Some((families, bytes))
}

impl Drop for RunningEngine {
    fn drop(&mut self) {
        if !self.reaped {
            kill(self.pid, libc::SIGKILL);
        }
    }
}

pub async fn spawn(spec: EngineSpec) -> HostResult<RunningEngine> {
    let (read_fd, write_fd) = pipe()?;
    let mut command = tokio::process::Command::new(&spec.program);
    command
        .args(&spec.args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(false);
    let mut forwarded = vec![(CONTROL_FD, write_fd)];
    if let EngineSocket::Inherited(listener) = &spec.socket {
        forwarded.push((LISTENER_FD, listener.as_raw_fd()));
    }
    let child_fds = forwarded.clone();
    unsafe {
        command.as_std_mut().pre_exec(move || {
            for (target, source) in &child_fds {
                if libc::dup2(*source, *target) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                if source != target {
                    libc::close(*source);
                }
                libc::fcntl(*target, libc::F_SETFD, 0);
            }
            Ok(())
        });
    }
    let mut child = command.spawn()?;
    unsafe {
        libc::close(write_fd);
    }
    let pid = child.id().expect("spawned child has a pid");
    spec.observer.with(
        "workerd-start",
        json!({ "kind": "workerd-start", "pid": pid }),
    );

    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let counts = Arc::new(OutputCounts::default());
    let (control_tx, control_rx) = mpsc::unbounded_channel();
    let control_file = unsafe { File::from_raw_fd(read_fd) };
    spawn_control_reader(control_file, control_tx);

    let (fatal, _) = watch::channel(None);
    let diagnostics = Arc::new(Diagnostics {
        fatal,
        uncaught: AtomicU64::new(0),
        internal: AtomicU64::new(0),
        read_failed: AtomicU64::new(0),
    });
    let mut readers = Vec::new();
    let observer = spec.observer.clone();
    if let Some(stdout) = stdout {
        readers.push(spawn_line_reader(
            "stdout",
            stdout,
            observer.clone(),
            counts.clone(),
            diagnostics.clone(),
        ));
    }
    if let Some(stderr) = stderr {
        readers.push(spawn_line_reader(
            "stderr",
            stderr,
            observer,
            counts.clone(),
            diagnostics.clone(),
        ));
    }

    let (exit_tx, exit_rx) = oneshot::channel();
    tokio::spawn(async move {
        let status = child.wait().await;
        let status = status.unwrap_or_else(|_| std::process::ExitStatus::from_raw(0));
        let _ = exit_tx.send(status);
    });

    Ok(RunningEngine {
        pid,
        socket: spec.socket,
        control: control_rx,
        exit: Some(exit_rx),
        exit_status: None,
        counts,
        diagnostics,
        readers,
        observer: spec.observer,
        socket_name: spec.socket_name,
        startup_timeout: spec.startup_timeout,
        reaped: false,
    })
}

fn pipe() -> std::io::Result<(RawFd, RawFd)> {
    let mut fds = [0 as RawFd; 2];
    if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    for fd in fds {
        set_cloexec(fd);
    }
    Ok((fds[0], fds[1]))
}

fn set_cloexec(fd: RawFd) {
    unsafe {
        let flags = libc::fcntl(fd, libc::F_GETFD);
        if flags >= 0 {
            libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC);
        }
    }
}

fn spawn_control_reader(file: File, sender: mpsc::UnboundedSender<ControlMessage>) {
    tokio::task::spawn_blocking(move || {
        let reader = BufReader::new(file);
        for line in reader.lines() {
            let Ok(line) = line else {
                break;
            };
            let message = match serde_json::from_str::<Value>(&line) {
                Ok(value) if value["event"] == "listen" => ControlMessage::Listen {
                    socket: value["socket"].as_str().unwrap_or_default().to_string(),
                    port: value["port"].as_u64().unwrap_or(0) as u16,
                },
                _ => ControlMessage::Invalid(line),
            };
            if sender.send(message).is_err() {
                break;
            }
        }
    });
}

struct OutputScanner {
    line: Vec<u8>,
    tail: Vec<u8>,
    oversized: bool,
    fatal: u8,
    seen: bool,
}

impl OutputScanner {
    fn new() -> Self {
        Self {
            line: Vec::new(),
            tail: Vec::new(),
            oversized: false,
            fatal: 0,
            seen: false,
        }
    }

    fn byte(&mut self, byte: u8, diagnostics: &Diagnostics, observer: &Observer) {
        self.seen = true;
        // ASCII 空白显式含 VT；Rust 的 is_ascii_whitespace 不含它。
        let normalized = if matches!(
            byte,
            b' ' | b'\t' | b'\n' | b'\x0b' | b'\x0c' | b'\r' | b'-' | b'_'
        ) {
            b' '
        } else {
            byte.to_ascii_lowercase()
        };
        if normalized != b' ' || self.tail.last() != Some(&b' ') {
            self.tail.push(normalized);
        }
        if self.tail.len() > 64 {
            self.tail.remove(0);
        }
        self.detect(
            b"uncaught",
            1,
            EngineDiagnostic::Uncaught,
            diagnostics,
            observer,
        );
        self.detect(
            b"internal error",
            2,
            EngineDiagnostic::InternalError,
            diagnostics,
            observer,
        );
        if !self.oversized {
            if self.line.len() == 8192 {
                self.line.clear();
                self.oversized = true;
            } else {
                self.line.push(byte);
            }
        }
    }

    fn detect(
        &mut self,
        pattern: &[u8],
        bit: u8,
        kind: EngineDiagnostic,
        diagnostics: &Diagnostics,
        observer: &Observer,
    ) {
        if self.fatal & bit == 0 && self.tail.ends_with(pattern) {
            self.fatal |= bit;
            diagnostics.record(kind, observer);
        }
    }

    fn finish(&mut self, stream: &'static str, counts: &OutputCounts, observer: &Observer) {
        if !self.seen {
            return;
        }
        counts.bump(stream);
        if self.fatal == 0 && !self.oversized {
            safe_output(&String::from_utf8_lossy(&self.line), observer);
        }
        self.line.clear();
        self.tail.clear();
        self.oversized = false;
        self.fatal = 0;
        self.seen = false;
    }
}

fn safe_output(line: &str, observer: &Observer) {
    if let Some(marker) = line.find("[ks-gc]") {
        if let Ok(value) = serde_json::from_str::<Value>(line[marker + 7..].trim()) {
            let numeric = |key: &str| {
                value.get(key).and_then(Value::as_f64).filter(|value| {
                    value.is_finite() && *value >= 0.0 && *value <= 9_007_199_254_740_991.0
                })
            };
            if let (Some(at), Some(done)) = (numeric("at"), numeric("done")) {
                observer.record(json!({ "kind": "gc", "at": at, "done": done }));
            }
        }
        return;
    }
    if let Some((families, bytes)) = font_registration(line) {
        eprintln!("[engine] fonts: registered {families} Latin UA fonts ({bytes} bytes)");
    }
}

fn spawn_line_reader<R: tokio::io::AsyncRead + Unpin + Send + 'static>(
    stream: &'static str,
    mut reader: R,
    observer: Arc<Observer>,
    counts: Arc<OutputCounts>,
    diagnostics: Arc<Diagnostics>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        use tokio::io::AsyncReadExt;
        let mut scanner = OutputScanner::new();
        let mut chunk = [0u8; 4096];
        loop {
            let length = match reader.read(&mut chunk).await {
                Ok(0) => break,
                Ok(length) => length,
                Err(_) => {
                    diagnostics.record(EngineDiagnostic::OutputReadFailed, &observer);
                    break;
                }
            };
            for byte in &chunk[..length] {
                if *byte == b'\n' {
                    scanner.finish(stream, &counts, &observer);
                } else {
                    scanner.byte(*byte, &diagnostics, &observer);
                }
            }
        }
        scanner.finish(stream, &counts, &observer);
    })
}

fn kill(pid: u32, signal: i32) {
    unsafe {
        libc::kill(pid as libc::pid_t, signal);
    }
}

#[cfg(test)]
mod output_tests {
    use super::*;

    #[test]
    fn bounded_scanner_finds_split_fatals_after_long_payload_before_markers() {
        let (fatal, _) = watch::channel(None);
        let diagnostics = Diagnostics {
            fatal,
            uncaught: AtomicU64::new(0),
            internal: AtomicU64::new(0),
            read_failed: AtomicU64::new(0),
        };
        let observer = Observer::disabled();
        let mut scanner = OutputScanner::new();
        for byte in std::iter::repeat_n(b'x', 100_000).chain(
            b" UnCaUgHt [ks-gc] INTERNAL\t ERROR [kitesurf-fonts]"
                .iter()
                .copied(),
        ) {
            scanner.byte(byte, &diagnostics, &observer);
            assert!(scanner.line.len() <= 8192);
            assert!(scanner.tail.len() <= 64);
        }
        assert_eq!(diagnostics.uncaught.load(Ordering::Relaxed), 1);
        assert_eq!(diagnostics.internal.load(Ordering::Relaxed), 1);
        assert_eq!(
            *diagnostics.fatal.borrow(),
            Some(EngineDiagnostic::Uncaught)
        );
    }

    #[test]
    fn ascii_whitespace_separated_internal_error_is_fatal_at_eof() {
        let observer = Observer::disabled();
        // 最终排空无换行：原反例的 VT 分隔必须命中同一固定类别。
        let (fatal, _) = watch::channel(None);
        let diagnostics = Diagnostics {
            fatal,
            uncaught: AtomicU64::new(0),
            internal: AtomicU64::new(0),
            read_failed: AtomicU64::new(0),
        };
        let mut scanner = OutputScanner::new();
        for chunk in b"workerd INTERNAL\x0bERROR".chunks(7) {
            for byte in chunk {
                scanner.byte(*byte, &diagnostics, &observer);
            }
        }
        assert_eq!(diagnostics.internal.load(Ordering::Relaxed), 1);
        assert_eq!(
            *diagnostics.fatal.borrow(),
            Some(EngineDiagnostic::InternalError)
        );

        // 单行 SP/TAB/VT/FF/CR 与连字符/下划线；LF 由生产 reader 划分日志行。
        for separator in *b" \t\x0b\x0c\r-_" {
            let (fatal, _) = watch::channel(None);
            let diagnostics = Diagnostics {
                fatal,
                uncaught: AtomicU64::new(0),
                internal: AtomicU64::new(0),
                read_failed: AtomicU64::new(0),
            };
            let mut scanner = OutputScanner::new();
            let mut bytes = vec![b'x'; 9_000];
            bytes.extend_from_slice(b"WoRkErD InTeRnAl");
            bytes.push(separator);
            bytes.extend_from_slice(b"ErRoR");
            for chunk in bytes.chunks(7) {
                for byte in chunk {
                    scanner.byte(*byte, &diagnostics, &observer);
                }
            }
            assert_eq!(
                diagnostics.internal.load(Ordering::Relaxed),
                1,
                "separator {separator:#04x}"
            );
            assert_eq!(
                *diagnostics.fatal.borrow(),
                Some(EngineDiagnostic::InternalError),
                "separator {separator:#04x}"
            );
        }
    }

    #[test]
    fn gc_reconstructs_numeric_allowlist_only() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("events");
        let observer = Observer::create(&path).unwrap();
        safe_output(
            r#"[ks-gc] {"at":1,"done":2,"extra":{"secret":"synthetic-output-sensitive"}}"#,
            &observer,
        );
        for invalid in [
            r#"{"at":"1","done":2}"#,
            r#"{"at":-1,"done":2}"#,
            r#"{"at":1,"done":{}}"#,
            r#"{"at":1e100,"done":2}"#,
        ] {
            safe_output(&format!("[ks-gc] {invalid}"), &observer);
        }
        let text = std::fs::read_to_string(path).unwrap();
        let values: Vec<Value> = text
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(values, vec![json!({"kind":"gc","at":1.0,"done":2.0})]);
    }
}
