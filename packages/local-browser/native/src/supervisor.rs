// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// 启动固定版本的 workerd，传入私有服务 socket 和控制管道，并等待引擎 socket 的 listen
// 事件。stdout/stderr、控制管道和子进程退出并行监控；关闭时先发 SIGTERM，超时后升级为
// SIGKILL，并回收子进程。

use std::collections::VecDeque;
use std::ffi::OsString;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{timeout, Instant};

use crate::observe::Observer;
use crate::transport::UpstreamAddress;
use crate::HostResult;

const CONTROL_FD: RawFd = 3;
const LISTENER_FD: RawFd = 4;
const LINE_TAIL_LIMIT: usize = 200;

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

pub struct RunningEngine {
    pid: u32,
    pub socket: EngineSocket,
    control: mpsc::UnboundedReceiver<ControlMessage>,
    exit: Option<oneshot::Receiver<std::process::ExitStatus>>,
    exit_status: Option<ExitInfo>,
    tail: Arc<Mutex<VecDeque<String>>>,
    observer: Arc<Observer>,
    pub socket_name: String,
    startup_timeout: Duration,
    reaped: bool,
}

impl RunningEngine {
    /// 等待指定 socket 的 listen 事件；其他事件或提前退出均视为失败。
    pub async fn wait_ready(&mut self) -> HostResult<ListenInfo> {
        let deadline = Instant::now() + self.startup_timeout;
        let Some(exit) = self.exit.as_mut() else {
            return Err("engine exit watcher is gone".into());
        };
        let observer = self.observer.clone();
        let tail_handle = self.tail.clone();
        let socket_name = self.socket_name.clone();
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            let tail = tail_text(&tail_handle);
            return Err(format!("engine did not listen on {socket_name} in time{tail}").into());
        }
        tokio::select! {
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
                let tail = tail_text(&tail_handle);
                Err(format!("engine exited during startup ({info}){tail}").into())
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
            let tail = tail_text(&tail_handle);
            Err(format!("engine exited during startup ({info}){tail}").into())
          }
          _ = tokio::time::sleep(remaining) => {
            let tail = tail_text(&tail_handle);
            Err(format!("engine did not listen on {socket_name} in time{tail}").into())
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

    /// 只等待一次子进程退出；后续调用返回已记住的状态。
    pub async fn wait_exit(&mut self) -> ExitInfo {
        if let Some(status) = self.exit_status {
            return status;
        }
        if let Some(exit) = self.exit.take() {
            let frame = exit
                .await
                .unwrap_or_else(|_| std::process::ExitStatus::from_raw(0));
            let info = ExitInfo::from_status(frame);
            self.exit_status = Some(info);
            self.reaped = true;
            return info;
        }
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
        if let Some(status) = self.exit_status {
            return status;
        }
        kill(self.pid, libc::SIGTERM);
        if let Ok(status) = timeout(Duration::from_secs(5), self.wait_exit()).await {
            self.observer.with(
        "engine-exit",
        json!({ "kind": "engine-exit", "code": status.code, "signal": status.signal, "reaped": true }),
      );
            return status;
        }
        kill(self.pid, libc::SIGKILL);
        let status = self.wait_exit().await;
        self.observer.with(
      "engine-exit",
      json!({ "kind": "engine-exit", "code": status.code, "signal": status.signal, "reaped": true }),
    );
        status
    }
}

fn tail_text(tail: &Arc<Mutex<VecDeque<String>>>) -> String {
    let tail = tail.lock().expect("tail");
    if tail.is_empty() {
        return String::new();
    }
    let joined = tail.iter().cloned().collect::<Vec<_>>().join(" | ");
    format!("; last output: {joined}")
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
    let tail = Arc::new(Mutex::new(VecDeque::with_capacity(LINE_TAIL_LIMIT)));
    let (control_tx, control_rx) = mpsc::unbounded_channel();
    let control_file = unsafe { File::from_raw_fd(read_fd) };
    spawn_control_reader(control_file, control_tx);

    let observer = spec.observer.clone();
    let tail_for_stdout = tail.clone();
    let tail_for_stderr = tail.clone();
    if let Some(stdout) = stdout {
        spawn_line_reader("stdout", stdout, observer.clone(), tail_for_stdout);
    }
    if let Some(stderr) = stderr {
        spawn_line_reader("stderr", stderr, observer, tail_for_stderr);
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
        tail,
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

fn spawn_line_reader<R: tokio::io::AsyncRead + Unpin + Send + 'static>(
    stream: &'static str,
    reader: R,
    observer: Arc<Observer>,
    tail: Arc<Mutex<VecDeque<String>>>,
) {
    tokio::spawn(async move {
        use tokio::io::AsyncBufReadExt;
        let mut lines = tokio::io::BufReader::new(reader).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if stream == "stdout" {
                eprintln!("[engine] {line}");
            } else {
                eprintln!("[engine:err] {line}");
            }
            if let Some(marker) = line.find("[ks-gc]") {
                if let Ok(value) = serde_json::from_str::<Value>(line[marker + 7..].trim()) {
                    let mut event = value;
                    if let Value::Object(map) = &mut event {
                        map.insert("kind".to_string(), json!("gc"));
                    }
                    observer.record(event);
                }
            }
            if let Ok(mut guard) = tail.lock() {
                if guard.len() == LINE_TAIL_LIMIT {
                    guard.pop_front();
                }
                guard.push_back(line);
            }
        }
    });
}

fn kill(pid: u32, signal: i32) {
    unsafe {
        libc::kill(pid as libc::pid_t, signal);
    }
}
