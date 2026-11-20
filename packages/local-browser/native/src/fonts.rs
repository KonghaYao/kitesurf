// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// 进程内系统字体服务：一个串行有界线程拥有字体库，HTTP 只在私有 Unix socket 上提供
// 现有 /match、/face 与 /stats。它不接收路径、任意字体族或 Fontconfig 配置，也不启动
// 子进程。

use std::io::Write;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::{TokioIo, TokioTimer};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot, watch, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;

use kitesurf_font_host::{
    ErrorCode, FaceSummary, FontHost, HostError, MatchRequest, MatchResponse, StatsResponse,
    MAX_FACE_BYTES,
};

use crate::services::{bind_private_socket, json_response, remove_socket, ConnectionTasks};
use crate::HostResult;

pub(crate) const STARTUP_FAILURE: &str =
    "No usable system sans-serif font is installed; the local browser cannot start without a base font.";

const MAX_REQUEST_BYTES: usize = 64 * 1024;
const MAX_MATCH_RESPONSE_BYTES: usize = 64 * 1024;
const MAX_STATS_RESPONSE_BYTES: usize = 64 * 1024;
const MAX_CONCURRENT_REQUESTS: usize = 8;
const MAX_CONNECTIONS: usize = 16;
const RESPONSE_RESERVATION_BYTES: usize = MAX_FACE_BYTES;
const MAX_HTTP_BUFFER_BYTES: usize = 64 * 1024;
const MAX_HTTP_HEADERS: usize = 64;
const MAX_TEXT_BYTES: usize = 4096;
const MAX_TEXT_CODEPOINTS: usize = 256;
const FAMILIES: &[&str] = &[
    "sans",
    "sans-serif",
    "ui-sans",
    "serif",
    "ui-serif",
    "mono",
    "monospace",
    "ui-mono",
    "system-ui",
    "emoji",
    "script",
];
const STARTUP_SAMPLE: &str = "Aa1";
const DEFAULT_QUEUE: usize = 4;
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
const DEFAULT_BODY_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_HEADERS_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_WRITER_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_CONNECTION_TIMEOUT: Duration = Duration::from_secs(50);
const DEFAULT_STARTUP_TIMEOUT: Duration = Duration::from_secs(20);

pub(crate) trait FontBackend {
    fn match_faces(&mut self, request: &MatchRequest) -> Result<MatchResponse, HostError>;
    fn face_bytes(&mut self, id: &str) -> Result<Arc<Vec<u8>>, HostError>;
    fn stats(&mut self) -> StatsResponse;
}

impl FontBackend for FontHost {
    fn match_faces(&mut self, request: &MatchRequest) -> Result<MatchResponse, HostError> {
        FontHost::match_faces(self, request)
    }

    fn face_bytes(&mut self, id: &str) -> Result<Arc<Vec<u8>>, HostError> {
        FontHost::face_bytes(self, id)
    }

    fn stats(&mut self) -> StatsResponse {
        FontHost::stats(self)
    }
}

enum FontCommand {
    Match {
        request: MatchRequest,
        reply: oneshot::Sender<Result<MatchResponse, HostError>>,
        _lease: RequestLease,
    },
    Face {
        id: String,
        reply: oneshot::Sender<Result<Arc<Vec<u8>>, HostError>>,
        _lease: RequestLease,
    },
    Stats {
        reply: oneshot::Sender<StatsResponse>,
        _lease: RequestLease,
    },
    Stop,
}

#[derive(Clone, Copy)]
struct FontLimits {
    queue: usize,
    request: Duration,
    body: Duration,
    headers: Duration,
    writer: Duration,
    connection: Duration,
    startup: Duration,
}

impl Default for FontLimits {
    fn default() -> Self {
        Self {
            queue: DEFAULT_QUEUE,
            request: DEFAULT_REQUEST_TIMEOUT,
            body: DEFAULT_BODY_TIMEOUT,
            headers: DEFAULT_HEADERS_TIMEOUT,
            writer: DEFAULT_WRITER_TIMEOUT,
            connection: DEFAULT_CONNECTION_TIMEOUT,
            startup: DEFAULT_STARTUP_TIMEOUT,
        }
    }
}

struct FontBudget {
    connections: Arc<Semaphore>,
    requests: Arc<Semaphore>,
    reserved: AtomicUsize,
    #[cfg(test)]
    active_connections: AtomicUsize,
    #[cfg(test)]
    dispatched: AtomicUsize,
}

impl FontBudget {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
            requests: Arc::new(Semaphore::new(MAX_CONCURRENT_REQUESTS)),
            reserved: AtomicUsize::new(0),
            #[cfg(test)]
            active_connections: AtomicUsize::new(0),
            #[cfg(test)]
            dispatched: AtomicUsize::new(0),
        })
    }

    fn try_connection(self: &Arc<Self>) -> Option<ConnectionLease> {
        let permit = self.connections.clone().try_acquire_owned().ok()?;
        #[cfg(test)]
        self.active_connections.fetch_add(1, Ordering::SeqCst);
        Some(ConnectionLease {
            _permit: permit,
            #[cfg(test)]
            budget: self.clone(),
        })
    }

    fn try_request(self: &Arc<Self>) -> Option<RequestLease> {
        let permit = self.requests.clone().try_acquire_owned().ok()?;
        self.reserved
            .fetch_add(RESPONSE_RESERVATION_BYTES, Ordering::SeqCst);
        Some(RequestLease {
            _inner: Arc::new(RequestLeaseInner {
                budget: self.clone(),
                _permit: permit,
            }),
        })
    }

    #[cfg(test)]
    fn reserved_bytes(&self) -> usize {
        self.reserved.load(Ordering::SeqCst)
    }

    #[cfg(test)]
    fn active_connections(&self) -> usize {
        self.active_connections.load(Ordering::SeqCst)
    }

    #[cfg(test)]
    fn dispatched(&self) -> usize {
        self.dispatched.load(Ordering::SeqCst)
    }

    #[cfg(test)]
    fn available_connections(&self) -> usize {
        self.connections.available_permits()
    }

    #[cfg(test)]
    fn available_requests(&self) -> usize {
        self.requests.available_permits()
    }
}

struct ConnectionLease {
    _permit: OwnedSemaphorePermit,
    #[cfg(test)]
    budget: Arc<FontBudget>,
}

impl Drop for ConnectionLease {
    fn drop(&mut self) {
        #[cfg(test)]
        self.budget
            .active_connections
            .fetch_sub(1, Ordering::SeqCst);
    }
}

#[derive(Clone)]
struct RequestLease {
    _inner: Arc<RequestLeaseInner>,
}

struct RequestLeaseInner {
    budget: Arc<FontBudget>,
    _permit: OwnedSemaphorePermit,
}

impl Drop for RequestLeaseInner {
    fn drop(&mut self) {
        self.budget
            .reserved
            .fetch_sub(RESPONSE_RESERVATION_BYTES, Ordering::SeqCst);
    }
}

#[derive(Clone, Default)]
struct FontHooks {
    #[cfg(test)]
    probe: Option<Arc<dyn Fn(UnixStream) -> FontStream + Send + Sync>>,
}

impl FontHooks {
    fn wrap(&self, stream: UnixStream) -> FontStream {
        #[cfg(test)]
        if let Some(probe) = &self.probe {
            return probe(stream);
        }
        FontStream::Plain(stream)
    }
}

pub struct FontService {
    path: PathBuf,
    commands: Option<mpsc::Sender<FontCommand>>,
    worker: Option<std::thread::JoinHandle<()>>,
    shutdown: watch::Sender<bool>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    connections: Arc<ConnectionTasks>,
    #[cfg(test)]
    pending: Arc<AtomicUsize>,
    #[cfg(test)]
    budget: Arc<FontBudget>,
}

impl FontService {
    /// 在绑定 socket 之前完成真实 sans 匹配与 face 读取；失败不启动 workerd。
    pub async fn start(run_dir: &Path) -> HostResult<Self> {
        let factory: Box<dyn FnOnce() -> Box<dyn FontBackend> + Send> =
            Box::new(|| -> Box<dyn FontBackend> { Box::new(FontHost::new()) });
        Self::start_with(run_dir, factory, FontLimits::default()).await
    }

    async fn start_with(
        run_dir: &Path,
        factory: Box<dyn FnOnce() -> Box<dyn FontBackend> + Send>,
        limits: FontLimits,
    ) -> HostResult<Self> {
        Self::start_with_hooks(run_dir, factory, limits, FontHooks::default()).await
    }

    async fn start_with_hooks(
        run_dir: &Path,
        factory: Box<dyn FnOnce() -> Box<dyn FontBackend> + Send>,
        limits: FontLimits,
        hooks: FontHooks,
    ) -> HostResult<Self> {
        let path = run_dir.join("fonts.sock");
        let (commands, receiver) = mpsc::channel(limits.queue.max(1));
        let (ready, ready_rx) = oneshot::channel();
        let worker = std::thread::Builder::new()
            .name("kitesurf-fonts".to_string())
            .spawn(move || run_worker(factory, receiver, ready))
            .map_err(|error| format!("cannot start the system font worker: {error}"))?;
        match tokio::time::timeout(limits.startup, ready_rx).await {
            Ok(result) => {
                if let Err(message) = result.unwrap_or(Err(STARTUP_FAILURE)) {
                    let _ = worker.join();
                    return Err(message.into());
                }
            }
            // 平台调用不可中断；超时后线程可能仍在运行，但启动已失败而不是成功。
            Err(_) => return Err(STARTUP_FAILURE.into()),
        }
        remove_socket(&path);
        let listener = match bind_private_socket(&path) {
            Ok(listener) => listener,
            Err(error) => {
                let _ = commands.send(FontCommand::Stop).await;
                drop(commands);
                let _ = tokio::task::spawn_blocking(move || worker.join()).await;
                return Err(error);
            }
        };
        let (shutdown, shutdown_rx) = watch::channel(false);
        let connections = Arc::new(ConnectionTasks::new());
        let pending = Arc::new(AtomicUsize::new(0));
        let budget = FontBudget::new();
        let handler = Arc::new(FontHandler {
            commands: commands.clone(),
            limits,
            pending: pending.clone(),
            budget: budget.clone(),
        });
        let task = spawn_font_listener(listener, shutdown_rx, handler, connections.clone(), hooks);
        Ok(Self {
            path,
            commands: Some(commands),
            worker: Some(worker),
            shutdown,
            tasks: Mutex::new(vec![task]),
            connections,
            #[cfg(test)]
            pending,
            #[cfg(test)]
            budget,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// 停止监听与连接，随后把 Stop 排在已接受命令之后，并等待字体线程真正结束。
    pub async fn shutdown(&mut self) -> bool {
        let _ = self.shutdown.send(true);
        let listeners = self
            .tasks
            .lock()
            .map(|mut tasks| std::mem::take(&mut *tasks))
            .unwrap_or_default();
        for listener in listeners {
            let _ = listener.await;
        }
        self.connections.close().await;
        let sent = match self.commands.take() {
            Some(commands) => commands.send(FontCommand::Stop).await.is_ok(),
            None => true,
        };
        let Some(worker) = self.worker.take() else {
            return sent;
        };
        let joined = matches!(
            tokio::task::spawn_blocking(move || worker.join()).await,
            Ok(Ok(()))
        );
        let _ = std::fs::remove_file(&self.path);
        sent && joined
    }
}

impl Drop for FontService {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
        if let Some(commands) = self.commands.take() {
            let _ = commands.try_send(FontCommand::Stop);
        }
        if let Ok(mut tasks) = self.tasks.lock() {
            for task in tasks.drain(..) {
                task.abort();
            }
        }
        self.connections.abort();
    }
}

fn spawn_font_listener(
    listener: UnixListener,
    mut shutdown: watch::Receiver<bool>,
    handler: Arc<FontHandler>,
    connections: Arc<ConnectionTasks>,
    hooks: FontHooks,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
              changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                  break;
                }
              }
              accepted = listener.accept() => match accepted {
                Ok((stream, _)) => {
                  // 连接额度在 spawn 与 HTTP 解析之前取得；无额度直接关闭，不排队等待。
                  let Some(lease) = handler.budget.try_connection() else {
                    drop(stream);
                    continue;
                  };
                  let stream = hooks.wrap(stream);
                  let handler = handler.clone();
                  let task = tokio::spawn(async move {
                    run_font_connection(stream, handler, lease).await;
                  });
                  connections.track(task);
                }
                Err(_) => break,
              }
            }
        }
    })
}

async fn run_font_connection(
    stream: FontStream,
    handler: Arc<FontHandler>,
    lease: ConnectionLease,
) {
    // 先声明连接 lease：future 被 abort 时也最后释放，Hyper 与 IO 先销毁。
    let connection_lease = lease;
    let connection_state = Arc::new(FontConnection::new());
    let (write_started, write_started_rx) = oneshot::channel();
    let write_started = Arc::new(Mutex::new(Some(write_started)));
    let service = service_fn({
        let handler = handler.clone();
        let connection_state = connection_state.clone();
        let write_started = write_started.clone();
        move |request| {
            let handler = handler.clone();
            let connection_state = connection_state.clone();
            let write_started = write_started.clone();
            async move {
                let response = handler.handle(request, &connection_state).await;
                if let Ok(mut slot) = write_started.lock() {
                    if let Some(sender) = slot.take() {
                        let _ = sender.send(());
                    }
                }
                Ok::<_, std::convert::Infallible>(response)
            }
        }
    });
    let limits = handler.limits;
    let http = http1::Builder::new()
        .timer(TokioTimer::new())
        .header_read_timeout(limits.headers)
        .keep_alive(false)
        .pipeline_flush(false)
        .writev(true)
        .max_headers(MAX_HTTP_HEADERS)
        .max_buf_size(MAX_HTTP_BUFFER_BYTES)
        .serve_connection(TokioIo::new(stream), service);
    let mut http = Box::pin(http);
    let writer_deadline = async move {
        if write_started_rx.await.is_ok() {
            tokio::time::sleep(limits.writer).await;
        } else {
            std::future::pending::<()>().await;
        }
    };
    let mut writer_deadline = Box::pin(writer_deadline);
    let mut total_deadline = Box::pin(tokio::time::sleep(limits.connection));
    tokio::select! {
        _ = &mut http => {}
        _ = &mut writer_deadline => {}
        _ = &mut total_deadline => {}
    }
    // 写出超时或总超时都直接关闭，不向已开始的响应追加错误 JSON。
    drop(http);
    drop(connection_state);
    drop(connection_lease);
}

struct FontConnection {
    dispatched: AtomicBool,
    lease: Mutex<Option<RequestLease>>,
}

impl FontConnection {
    fn new() -> Self {
        Self {
            dispatched: AtomicBool::new(false),
            lease: Mutex::new(None),
        }
    }

    fn hold(&self, lease: &RequestLease) {
        if let Ok(mut slot) = self.lease.lock() {
            *slot = Some(lease.clone());
        }
    }
}

enum FontStream {
    Plain(UnixStream),
    #[cfg(test)]
    Probe(probe::ProbeStream),
}

impl AsyncRead for FontStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            FontStream::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            #[cfg(test)]
            FontStream::Probe(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for FontStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            FontStream::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            #[cfg(test)]
            FontStream::Probe(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            FontStream::Plain(stream) => Pin::new(stream).poll_write_vectored(cx, bufs),
            #[cfg(test)]
            FontStream::Probe(stream) => Pin::new(stream).poll_write_vectored(cx, bufs),
        }
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            FontStream::Plain(stream) => Pin::new(stream).poll_flush(cx),
            #[cfg(test)]
            FontStream::Probe(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            FontStream::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            #[cfg(test)]
            FontStream::Probe(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

fn run_worker(
    factory: Box<dyn FnOnce() -> Box<dyn FontBackend> + Send>,
    mut commands: mpsc::Receiver<FontCommand>,
    ready: oneshot::Sender<Result<(), &'static str>>,
) {
    let mut backend = factory();
    let readiness = validate_startup(&mut *backend);
    if ready.send(readiness).is_err() || readiness.is_err() {
        return;
    }
    while let Some(command) = commands.blocking_recv() {
        match command {
            FontCommand::Match {
                request,
                reply,
                _lease,
            } => {
                let _ = reply.send(backend.match_faces(&request));
            }
            FontCommand::Face { id, reply, _lease } => {
                let _ = reply.send(backend.face_bytes(&id));
            }
            FontCommand::Stats { reply, _lease } => {
                let _ = reply.send(backend.stats());
            }
            FontCommand::Stop => break,
        }
    }
}

fn validate_startup(backend: &mut dyn FontBackend) -> Result<(), &'static str> {
    let request = MatchRequest {
        family: Some("sans-serif".to_string()),
        text: Some(STARTUP_SAMPLE.to_string()),
        weight: Some(400.0),
        style: Some("normal".to_string()),
        ..MatchRequest::default()
    };
    let response = backend.match_faces(&request).map_err(|error| {
        if error.code == ErrorCode::EntropyUnavailable {
            error.message
        } else {
            STARTUP_FAILURE
        }
    })?;
    if !response.covered {
        return Err(STARTUP_FAILURE);
    }
    let face: &FaceSummary = response.faces.first().ok_or(STARTUP_FAILURE)?;
    if face.bytes == 0 || face.bytes > MAX_FACE_BYTES {
        return Err(STARTUP_FAILURE);
    }
    let bytes = backend.face_bytes(&face.id).map_err(|_| STARTUP_FAILURE)?;
    if bytes.is_empty() || bytes.len() > MAX_FACE_BYTES {
        return Err(STARTUP_FAILURE);
    }
    Ok(())
}

struct FontHandler {
    commands: mpsc::Sender<FontCommand>,
    limits: FontLimits,
    pending: Arc<AtomicUsize>,
    budget: Arc<FontBudget>,
}

// Bazel 为 native 和 font-host 各建一套 crate 仓库，serde trait 不能跨这两套实例；
// HTTP 边界因此在本地 DTO 上序列化，字体库仍只按字段使用。
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MatchRequestInput {
    #[serde(default)]
    family: Option<String>,
    #[serde(default)]
    script: Option<String>,
    #[serde(default)]
    locale: Option<String>,
    #[serde(default)]
    weight: Option<f32>,
    #[serde(default)]
    style: Option<String>,
    #[serde(default)]
    text: Option<String>,
}

impl From<MatchRequestInput> for MatchRequest {
    fn from(input: MatchRequestInput) -> Self {
        Self {
            family: input.family,
            script: input.script,
            locale: input.locale,
            weight: input.weight,
            style: input.style,
            text: input.text,
        }
    }
}

fn valid_match_input(input: &MatchRequestInput) -> bool {
    if let Some(family) = &input.family {
        if family.len() > 32 || !FAMILIES.contains(&family.as_str()) {
            return false;
        }
    }
    if let Some(script) = &input.script {
        if script.len() != 4 || !script.bytes().all(|byte| byte.is_ascii_alphabetic()) {
            return false;
        }
    }
    if let Some(locale) = &input.locale {
        if !(1..=32).contains(&locale.len())
            || !locale
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
        {
            return false;
        }
    }
    if let Some(style) = &input.style {
        if !matches!(style.as_str(), "normal" | "italic" | "oblique") {
            return false;
        }
    }
    if let Some(weight) = input.weight {
        if !weight.is_finite() || !(1.0..=1000.0).contains(&weight) {
            return false;
        }
    }
    if let Some(text) = &input.text {
        if text.len() > MAX_TEXT_BYTES || text.chars().count() > MAX_TEXT_CODEPOINTS {
            return false;
        }
    }
    true
}

#[derive(Serialize)]
struct MatchOutput {
    faces: Vec<FaceOutput>,
    covered: bool,
    missing: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<&'static str>,
}

#[derive(Serialize)]
struct FaceOutput {
    id: String,
    family: String,
    weight: f32,
    style: &'static str,
    width: f32,
    covered: bool,
    missing: u32,
    bytes: usize,
}

impl From<&MatchResponse> for MatchOutput {
    fn from(response: &MatchResponse) -> Self {
        Self {
            faces: response
                .faces
                .iter()
                .map(|face| FaceOutput {
                    id: face.id.clone(),
                    family: face.family.clone(),
                    weight: face.weight,
                    style: face.style,
                    width: face.width,
                    covered: face.covered,
                    missing: face.missing,
                    bytes: face.bytes,
                })
                .collect(),
            covered: response.covered,
            missing: response.missing,
            reason: response.reason,
        }
    }
}

#[derive(Serialize)]
struct StatsOutput {
    faces: usize,
    cached_faces: usize,
    cached_bytes: usize,
    matches: u64,
    served: u64,
    cache_hits: u64,
    cache_misses: u64,
    limited_emoji: u64,
    rejected: u64,
}

impl From<&StatsResponse> for StatsOutput {
    fn from(stats: &StatsResponse) -> Self {
        Self {
            faces: stats.faces,
            cached_faces: stats.cached_faces,
            cached_bytes: stats.cached_bytes,
            matches: stats.matches,
            served: stats.served,
            cache_hits: stats.cache_hits,
            cache_misses: stats.cache_misses,
            limited_emoji: stats.limited_emoji,
            rejected: stats.rejected,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FaceRequest {
    id: String,
}

impl FontHandler {
    async fn handle(
        &self,
        request: Request<Incoming>,
        connection: &FontConnection,
    ) -> Response<Full<Bytes>> {
        if connection.dispatched.swap(true, Ordering::SeqCst) {
            return status_response(StatusCode::TOO_MANY_REQUESTS);
        }
        // 请求 lease 在读取 body、创建命令或分配响应之前取得。
        let Some(lease) = self.budget.try_request() else {
            return status_response(StatusCode::TOO_MANY_REQUESTS);
        };
        connection.hold(&lease);
        if request.method() != Method::POST {
            return leased_empty(StatusCode::METHOD_NOT_ALLOWED, lease);
        }
        let path = request.uri().path().to_string();
        if declared_length(&request).is_some_and(|length| length > MAX_REQUEST_BYTES as u64) {
            return invalid_request(lease);
        }
        let body = match self.read_body(request.into_body()).await {
            Ok(body) => body,
            Err(status) => return leased_json(status, br#"{"error":"invalid request"}"#, lease),
        };
        match path.as_str() {
            "/match" => self.match_response(&body, lease).await,
            "/face" => self.face_response(&body, lease).await,
            "/stats" => self.stats_response(lease).await,
            _ => leased_empty(StatusCode::NOT_FOUND, lease),
        }
    }

    async fn read_body(&self, body: Incoming) -> Result<Bytes, StatusCode> {
        match tokio::time::timeout(self.limits.body, read_bounded(body, MAX_REQUEST_BYTES)).await {
            Ok(Ok(bytes)) => Ok(bytes),
            Ok(Err(())) => Err(StatusCode::BAD_REQUEST),
            Err(_) => Err(StatusCode::REQUEST_TIMEOUT),
        }
    }

    fn enqueue(&self, command: FontCommand) -> Result<(), StatusCode> {
        match self.commands.try_send(command) {
            Ok(()) => {
                self.pending.fetch_add(1, Ordering::SeqCst);
                #[cfg(test)]
                self.budget.dispatched.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
            Err(TrySendError::Full(_)) => Err(StatusCode::TOO_MANY_REQUESTS),
            Err(TrySendError::Closed(_)) => Err(StatusCode::BAD_GATEWAY),
        }
    }

    async fn match_response(&self, body: &[u8], lease: RequestLease) -> Response<Full<Bytes>> {
        let Ok(input) = serde_json::from_slice::<MatchRequestInput>(body) else {
            return invalid_request(lease);
        };
        if !valid_match_input(&input) {
            return invalid_request(lease);
        }
        let request: MatchRequest = input.into();
        let (reply, receiver) = oneshot::channel();
        let command = FontCommand::Match {
            request,
            reply,
            _lease: lease.clone(),
        };
        if let Err(status) = self.enqueue(command) {
            return leased_status(status, lease);
        }
        let _pending = PendingGuard(self.pending.clone());
        match tokio::time::timeout(self.limits.request, receiver).await {
            Err(_) => leased_json(StatusCode::BAD_GATEWAY, br#"{"error":"timeout"}"#, lease),
            Ok(Err(_)) => unavailable(lease),
            Ok(Ok(Err(error))) => font_error_response(error, lease),
            Ok(Ok(Ok(response))) => {
                let output = MatchOutput::from(&response);
                match bounded_json(&output, MAX_MATCH_RESPONSE_BYTES) {
                    Some(bytes) => leased_json(StatusCode::OK, &bytes, lease),
                    None => unavailable(lease),
                }
            }
        }
    }

    async fn face_response(&self, body: &[u8], lease: RequestLease) -> Response<Full<Bytes>> {
        let Ok(request) = serde_json::from_slice::<FaceRequest>(body) else {
            return invalid_request(lease);
        };
        if !valid_id(&request.id) {
            return invalid_request(lease);
        }
        let (reply, receiver) = oneshot::channel();
        let command = FontCommand::Face {
            id: request.id,
            reply,
            _lease: lease.clone(),
        };
        if let Err(status) = self.enqueue(command) {
            return leased_status(status, lease);
        }
        let _pending = PendingGuard(self.pending.clone());
        match tokio::time::timeout(self.limits.request, receiver).await {
            Err(_) => leased_json(StatusCode::BAD_GATEWAY, br#"{"error":"timeout"}"#, lease),
            Ok(Err(_)) => unavailable(lease),
            Ok(Ok(Err(error))) => font_error_response(error, lease),
            Ok(Ok(Ok(bytes))) if bytes.is_empty() || bytes.len() > MAX_FACE_BYTES => {
                unavailable(lease)
            }
            Ok(Ok(Ok(bytes))) => {
                let length = bytes.len();
                // 只共享 Arc，不复制整个 face；lease 随 Bytes owner 留到 writer 结束。
                let body = Bytes::from_owner(FaceBody {
                    bytes,
                    _lease: lease,
                });
                Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "font/otf")
                    .header("content-length", length.to_string())
                    .header("cache-control", "no-store")
                    .body(Full::new(body))
                    .expect("font response")
            }
        }
    }

    async fn stats_response(&self, lease: RequestLease) -> Response<Full<Bytes>> {
        let (reply, receiver) = oneshot::channel();
        let command = FontCommand::Stats {
            reply,
            _lease: lease.clone(),
        };
        if let Err(status) = self.enqueue(command) {
            return leased_status(status, lease);
        }
        let _pending = PendingGuard(self.pending.clone());
        match tokio::time::timeout(self.limits.request, receiver).await {
            Err(_) => leased_json(StatusCode::BAD_GATEWAY, br#"{"error":"timeout"}"#, lease),
            Ok(Err(_)) => unavailable(lease),
            Ok(Ok(stats)) => {
                let output = StatsOutput::from(&stats);
                match bounded_json(&output, MAX_STATS_RESPONSE_BYTES) {
                    Some(bytes) => leased_json(StatusCode::OK, &bytes, lease),
                    None => unavailable(lease),
                }
            }
        }
    }
}

struct PendingGuard(Arc<AtomicUsize>);

impl Drop for PendingGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

struct LeasedBody {
    bytes: Vec<u8>,
    _lease: RequestLease,
}

impl AsRef<[u8]> for LeasedBody {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

struct FaceBody {
    bytes: Arc<Vec<u8>>,
    _lease: RequestLease,
}

impl AsRef<[u8]> for FaceBody {
    fn as_ref(&self) -> &[u8] {
        self.bytes.as_ref()
    }
}

struct BoundedJsonWriter {
    bytes: Vec<u8>,
    limit: usize,
}

impl Write for BoundedJsonWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if self.bytes.len().saturating_add(buf.len()) > self.limit {
            return Err(std::io::Error::other("response too large"));
        }
        self.bytes.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn bounded_json<T: Serialize>(value: &T, limit: usize) -> Option<Vec<u8>> {
    let mut writer = BoundedJsonWriter {
        bytes: Vec::new(),
        limit,
    };
    serde_json::to_writer(&mut writer, value).ok()?;
    Some(writer.bytes)
}

fn leased_response(
    status: StatusCode,
    content_type: Option<&'static str>,
    bytes: Vec<u8>,
    lease: RequestLease,
) -> Response<Full<Bytes>> {
    let length = bytes.len();
    let body = Bytes::from_owner(LeasedBody {
        bytes,
        _lease: lease,
    });
    let mut builder = Response::builder()
        .status(status)
        .header("content-length", length.to_string())
        .header("cache-control", "no-store");
    if let Some(content_type) = content_type {
        builder = builder.header("content-type", content_type);
    }
    builder.body(Full::new(body)).expect("leased response")
}

fn leased_json(status: StatusCode, body: &[u8], lease: RequestLease) -> Response<Full<Bytes>> {
    leased_response(status, Some("application/json"), body.to_vec(), lease)
}

fn leased_empty(status: StatusCode, lease: RequestLease) -> Response<Full<Bytes>> {
    leased_response(status, None, Vec::new(), lease)
}

fn leased_status(status: StatusCode, lease: RequestLease) -> Response<Full<Bytes>> {
    let error = match status {
        StatusCode::TOO_MANY_REQUESTS => "busy",
        _ => "unavailable",
    };
    leased_json(
        status,
        json!({ "error": error }).to_string().as_bytes(),
        lease,
    )
}

fn status_response(status: StatusCode) -> Response<Full<Bytes>> {
    let error = match status {
        StatusCode::TOO_MANY_REQUESTS => "busy",
        _ => "unavailable",
    };
    json_response(status, json!({ "error": error }).to_string())
}

fn invalid_request(lease: RequestLease) -> Response<Full<Bytes>> {
    leased_json(
        StatusCode::BAD_REQUEST,
        br#"{"error":"invalid request"}"#,
        lease,
    )
}

fn unavailable(lease: RequestLease) -> Response<Full<Bytes>> {
    leased_json(
        StatusCode::BAD_GATEWAY,
        br#"{"error":"unavailable"}"#,
        lease,
    )
}

fn font_error_response(error: HostError, lease: RequestLease) -> Response<Full<Bytes>> {
    let status = match error.code {
        ErrorCode::BadRequest | ErrorCode::UnknownId | ErrorCode::ExpiredId => {
            StatusCode::NOT_FOUND
        }
        _ => StatusCode::BAD_GATEWAY,
    };
    let body = json!({ "error": error.code.as_str(), "message": error.message }).to_string();
    leased_json(status, body.as_bytes(), lease)
}

fn declared_length(request: &Request<Incoming>) -> Option<u64> {
    request
        .headers()
        .get(hyper::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
}

async fn read_bounded(mut body: Incoming, limit: usize) -> Result<Bytes, ()> {
    let mut bytes = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|_| ())?;
        if let Some(data) = frame.data_ref() {
            if bytes.len().saturating_add(data.len()) > limit {
                return Err(());
            }
            bytes.extend_from_slice(data);
        }
    }
    Ok(Bytes::from(bytes))
}

fn valid_id(id: &str) -> bool {
    id.len() == 32
        && id
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod probe {
    use super::*;
    use tokio::sync::Notify;

    pub(super) struct ProbeStream {
        inner: UnixStream,
        signal: Arc<ProbeSignal>,
    }

    impl ProbeStream {
        pub(super) fn new(inner: UnixStream, signal: Arc<ProbeSignal>) -> Self {
            Self { inner, signal }
        }
    }

    pub(super) struct ProbeSignal {
        blocked: AtomicBool,
        notify: Notify,
    }

    impl ProbeSignal {
        pub(super) fn new() -> Self {
            Self {
                blocked: AtomicBool::new(false),
                notify: Notify::new(),
            }
        }

        pub(super) async fn wait_blocked(&self) {
            loop {
                let notified = self.notify.notified();
                if self.blocked.load(Ordering::SeqCst) {
                    return;
                }
                notified.await;
            }
        }

        fn mark_blocked(&self) {
            self.blocked.store(true, Ordering::SeqCst);
            self.notify.notify_waiters();
        }
    }

    impl AsyncRead for ProbeStream {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for ProbeStream {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            let this = self.get_mut();
            let result = Pin::new(&mut this.inner).poll_write(cx, buf);
            if result.is_pending() {
                this.signal.mark_blocked();
            }
            result
        }

        fn poll_write_vectored(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            bufs: &[std::io::IoSlice<'_>],
        ) -> Poll<std::io::Result<usize>> {
            let this = self.get_mut();
            let result = Pin::new(&mut this.inner).poll_write_vectored(cx, bufs);
            if result.is_pending() {
                this.signal.mark_blocked();
            }
            result
        }

        fn is_write_vectored(&self) -> bool {
            true
        }

        fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.get_mut().inner).poll_flush(cx)
        }

        fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::atomic::AtomicBool;
    use std::sync::{Condvar, Weak};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::Notify;

    const FACE_ID: &str = "0123456789abcdef0123456789abcdef";
    const FACE_BYTES: [u8; 8] = [0, 1, 0, 0, 0, 0, 0, 0];
    const LARGE_FACE_BYTES: usize = 8 * 1024 * 1024;
    type FaceProbe = Arc<Mutex<Option<Weak<Vec<u8>>>>>;

    fn limits(queue: usize, request: Duration) -> FontLimits {
        FontLimits {
            queue,
            request,
            body: Duration::from_secs(2),
            headers: Duration::from_secs(10),
            writer: Duration::from_secs(10),
            connection: Duration::from_secs(30),
            startup: Duration::from_secs(2),
        }
    }

    fn face() -> FaceSummary {
        FaceSummary {
            id: FACE_ID.to_string(),
            family: "Fake Sans".to_string(),
            weight: 400.0,
            style: "normal",
            width: 1.0,
            covered: true,
            missing: 0,
            bytes: FACE_BYTES.len(),
        }
    }

    fn empty_stats() -> StatsResponse {
        StatsResponse {
            faces: 0,
            cached_faces: 0,
            cached_bytes: 0,
            matches: 0,
            served: 0,
            cache_hits: 0,
            cache_misses: 0,
            limited_emoji: 0,
            rejected: 0,
        }
    }

    struct StaticBackend;

    impl FontBackend for StaticBackend {
        fn match_faces(&mut self, _request: &MatchRequest) -> Result<MatchResponse, HostError> {
            Ok(MatchResponse {
                faces: vec![face()],
                covered: true,
                missing: 0,
                reason: None,
            })
        }

        fn face_bytes(&mut self, id: &str) -> Result<Arc<Vec<u8>>, HostError> {
            if id == FACE_ID {
                Ok(Arc::new(FACE_BYTES.to_vec()))
            } else {
                Err(HostError::new(ErrorCode::UnknownId, "unknown font id"))
            }
        }

        fn stats(&mut self) -> StatsResponse {
            StatsResponse {
                faces: 1,
                cached_faces: 1,
                cached_bytes: FACE_BYTES.len(),
                matches: 1,
                served: 1,
                ..empty_stats()
            }
        }
    }

    struct MissingBackend(Option<HostError>);

    impl FontBackend for MissingBackend {
        fn match_faces(&mut self, _request: &MatchRequest) -> Result<MatchResponse, HostError> {
            if let Some(error) = &self.0 {
                return Err(error.clone());
            }
            Ok(MatchResponse {
                faces: Vec::new(),
                covered: false,
                missing: STARTUP_SAMPLE.chars().count() as u32,
                reason: Some("no_usable_face"),
            })
        }

        fn face_bytes(&mut self, _id: &str) -> Result<Arc<Vec<u8>>, HostError> {
            Err(HostError::new(ErrorCode::UnknownId, "unexpected face"))
        }

        fn stats(&mut self) -> StatsResponse {
            empty_stats()
        }
    }

    struct UnreadableBackend;

    impl FontBackend for UnreadableBackend {
        fn match_faces(&mut self, _request: &MatchRequest) -> Result<MatchResponse, HostError> {
            Ok(MatchResponse {
                faces: vec![face()],
                covered: true,
                missing: 0,
                reason: None,
            })
        }

        fn face_bytes(&mut self, _id: &str) -> Result<Arc<Vec<u8>>, HostError> {
            Err(HostError::new(ErrorCode::Io, "unreadable source"))
        }

        fn stats(&mut self) -> StatsResponse {
            empty_stats()
        }
    }

    struct GatedBackend {
        armed: Arc<AtomicBool>,
        started: Arc<tokio::sync::Notify>,
        gate: Arc<(Mutex<bool>, Condvar)>,
    }

    impl GatedBackend {
        fn block(&self) {
            self.started.notify_one();
            let (lock, cvar) = &*self.gate;
            let mut open = lock.lock().expect("gate");
            while !*open {
                open = cvar.wait(open).expect("gate wait");
            }
        }
    }

    impl FontBackend for GatedBackend {
        fn match_faces(&mut self, _request: &MatchRequest) -> Result<MatchResponse, HostError> {
            if self.armed.load(Ordering::SeqCst) {
                self.block();
            }
            Ok(MatchResponse {
                faces: vec![face()],
                covered: true,
                missing: 0,
                reason: None,
            })
        }

        fn face_bytes(&mut self, _id: &str) -> Result<Arc<Vec<u8>>, HostError> {
            if self.armed.load(Ordering::SeqCst) {
                self.block();
            }
            Ok(Arc::new(FACE_BYTES.to_vec()))
        }

        fn stats(&mut self) -> StatsResponse {
            empty_stats()
        }
    }

    struct ResourceBackend {
        face_len: usize,
        face: FaceProbe,
        match_started: Arc<Notify>,
        match_armed: Arc<AtomicBool>,
        gate: Arc<(Mutex<bool>, Condvar)>,
    }

    impl ResourceBackend {
        fn new(face_len: usize) -> (Self, FaceProbe) {
            let face = Arc::new(Mutex::new(None));
            let backend = Self {
                face_len,
                face: face.clone(),
                match_started: Arc::new(Notify::new()),
                match_armed: Arc::new(AtomicBool::new(false)),
                gate: Arc::new((Mutex::new(false), Condvar::new())),
            };
            (backend, face)
        }
    }

    impl FontBackend for ResourceBackend {
        fn match_faces(&mut self, _request: &MatchRequest) -> Result<MatchResponse, HostError> {
            if self.match_armed.load(Ordering::SeqCst) {
                self.match_started.notify_one();
                let (lock, cvar) = &*self.gate;
                let mut open = lock.lock().expect("gate");
                while !*open {
                    open = cvar.wait(open).expect("gate wait");
                }
            }
            Ok(MatchResponse {
                faces: vec![FaceSummary {
                    bytes: self.face_len,
                    ..face()
                }],
                covered: true,
                missing: 0,
                reason: None,
            })
        }

        fn face_bytes(&mut self, _id: &str) -> Result<Arc<Vec<u8>>, HostError> {
            let bytes = Arc::new(vec![0x5a; self.face_len]);
            *self.face.lock().expect("face probe") = Some(Arc::downgrade(&bytes));
            Ok(bytes)
        }

        fn stats(&mut self) -> StatsResponse {
            empty_stats()
        }
    }
    struct PanicBackend {
        armed: Arc<AtomicBool>,
    }

    impl FontBackend for PanicBackend {
        fn match_faces(&mut self, _request: &MatchRequest) -> Result<MatchResponse, HostError> {
            assert!(
                !self.armed.load(Ordering::SeqCst),
                "system font backend panicked"
            );
            Ok(MatchResponse {
                faces: vec![face()],
                covered: true,
                missing: 0,
                reason: None,
            })
        }

        fn face_bytes(&mut self, _id: &str) -> Result<Arc<Vec<u8>>, HostError> {
            Ok(Arc::new(FACE_BYTES.to_vec()))
        }

        fn stats(&mut self) -> StatsResponse {
            empty_stats()
        }
    }

    async fn start(
        factory: Box<dyn FnOnce() -> Box<dyn FontBackend> + Send>,
        limits: FontLimits,
    ) -> (FontService, PathBuf, tempfile::TempDir) {
        let directory = tempfile::tempdir().expect("temp dir");
        let service = FontService::start_with(directory.path(), factory, limits)
            .await
            .expect("font service starts");
        let path = directory.path().join("fonts.sock");
        (service, path, directory)
    }

    async fn start_static(limits: FontLimits) -> (FontService, PathBuf, tempfile::TempDir) {
        start(
            Box::new(|| -> Box<dyn FontBackend> { Box::new(StaticBackend) }),
            limits,
        )
        .await
    }

    async fn start_hooked(
        factory: Box<dyn FnOnce() -> Box<dyn FontBackend> + Send>,
        limits: FontLimits,
        hooks: FontHooks,
    ) -> (FontService, PathBuf, tempfile::TempDir) {
        let directory = tempfile::tempdir().expect("temp dir");
        let service = FontService::start_with_hooks(directory.path(), factory, limits, hooks)
            .await
            .expect("font service starts");
        let path = directory.path().join("fonts.sock");
        (service, path, directory)
    }

    fn probe_hooks(signal: Arc<probe::ProbeSignal>) -> FontHooks {
        FontHooks {
            probe: Some(Arc::new(move |stream| {
                FontStream::Probe(probe::ProbeStream::new(stream, signal.clone()))
            })),
        }
    }

    fn request_bytes(target: &str, body: &[u8]) -> Vec<u8> {
        let mut bytes = format!(
            "POST {target} HTTP/1.1\r\nhost: fonts\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n",
            body.len()
        )
        .into_bytes();
        bytes.extend_from_slice(body);
        bytes
    }

    async fn raw_request(path: &Path, target: &str, body: &[u8]) -> UnixStream {
        let mut stream = UnixStream::connect(path).await.expect("raw connect");
        stream
            .write_all(&request_bytes(target, body))
            .await
            .expect("write request");
        stream.flush().await.expect("flush request");
        stream
    }

    async fn wait_for(mut condition: impl FnMut() -> bool, what: &str) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !condition() {
            assert!(tokio::time::Instant::now() < deadline, "{what}");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    struct LargeSetup {
        service: FontService,
        path: PathBuf,
        _directory: tempfile::TempDir,
        face: FaceProbe,
        signal: Arc<probe::ProbeSignal>,
        match_armed: Arc<AtomicBool>,
        match_started: Arc<Notify>,
        gate: Arc<(Mutex<bool>, Condvar)>,
    }

    async fn start_large(limits: FontLimits) -> LargeSetup {
        let (backend, face) = ResourceBackend::new(LARGE_FACE_BYTES);
        let signal = Arc::new(probe::ProbeSignal::new());
        let match_armed = backend.match_armed.clone();
        let match_started = backend.match_started.clone();
        let gate = backend.gate.clone();
        let (service, path, directory) = start_hooked(
            Box::new(move || -> Box<dyn FontBackend> { Box::new(backend) }),
            limits,
            probe_hooks(signal.clone()),
        )
        .await;
        // 启动校验也读一次 face；测试只关心实际 /face 请求留下的 Arc。
        *face.lock().expect("face probe") = None;
        LargeSetup {
            service,
            path,
            _directory: directory,
            face,
            signal,
            match_armed,
            match_started,
            gate,
        }
    }

    fn face_request_body() -> Vec<u8> {
        format!(r#"{{"id":"{FACE_ID}"}}"#).into_bytes()
    }

    fn assert_baseline(budget: &FontBudget) {
        assert_eq!(budget.reserved_bytes(), 0);
        assert_eq!(budget.active_connections(), 0);
        assert_eq!(budget.available_requests(), MAX_CONCURRENT_REQUESTS);
        assert_eq!(budget.available_connections(), MAX_CONNECTIONS);
    }

    async fn request(path: &Path, method: &str, target: &str, body: &[u8]) -> (u16, Bytes) {
        let stream = UnixStream::connect(path)
            .await
            .expect("connect fonts socket");
        let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .expect("http handshake");
        let driver = tokio::spawn(async move {
            let _ = connection.await;
        });
        let request = Request::builder()
            .method(method)
            .uri(format!("http://fonts{target}"))
            .header("content-type", "application/json")
            .body(Full::new(Bytes::copy_from_slice(body)))
            .expect("request");
        let response = sender.send_request(request).await.expect("response");
        let status = response.status().as_u16();
        let bytes = response
            .into_body()
            .collect()
            .await
            .expect("response body")
            .to_bytes();
        drop(sender);
        tokio::time::timeout(Duration::from_secs(2), driver)
            .await
            .expect("client connection settles")
            .expect("client driver");
        (status, bytes)
    }

    async fn wait_pending(service: &FontService, expected: usize) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while service.pending.load(Ordering::SeqCst) < expected {
            assert!(
                tokio::time::Instant::now() < deadline,
                "queued font requests were not dispatched"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    #[tokio::test]
    async fn faces_body_validation_and_opaque_ids() {
        let (mut service, path, _directory) = start_static(limits(4, Duration::from_secs(2))).await;
        let (status, body) = request(
            &path,
            "POST",
            "/match",
            br#"{"family":"sans-serif","text":"Aa1"}"#,
        )
        .await;
        assert_eq!(status, 200);
        let value: serde_json::Value = serde_json::from_slice(&body).expect("match json");
        assert_eq!(value["covered"], true);
        assert_eq!(value["faces"][0]["id"], FACE_ID);
        let (status, body) = request(
            &path,
            "POST",
            "/face",
            format!(r#"{{"id":"{FACE_ID}"}}"#).as_bytes(),
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(body.as_ref(), FACE_BYTES.as_slice());
        assert_eq!(
            request(&path, "POST", "/face", br#"{"id":"0123456789abcdef"}"#)
                .await
                .0,
            400
        );
        assert_eq!(
            request(
                &path,
                "POST",
                "/face",
                br#"{"id":"0123456789ABCDEF0123456789ABCDEF"}"#
            )
            .await
            .0,
            400
        );
        assert_eq!(
            request(
                &path,
                "POST",
                "/face",
                br#"{"id":"ffffffffffffffffffffffffffffffff"}"#
            )
            .await
            .0,
            404
        );
        assert_eq!(
            request(
                &path,
                "POST",
                "/face",
                br#"{"id":"0123456789abcdef0123456789abcdef","extra":1}"#
            )
            .await
            .0,
            400
        );
        assert_eq!(request(&path, "POST", "/match", b"not json").await.0, 400);
        assert_eq!(
            request(
                &path,
                "POST",
                "/match",
                br#"{"family":"sans-serif","extra":1}"#
            )
            .await
            .0,
            400
        );
        assert_eq!(request(&path, "GET", "/match", b"").await.0, 405);
        assert_eq!(request(&path, "POST", "/missing", b"").await.0, 404);
        for invalid in [
            br#"{"family":"Papyrus"}"#.as_slice(),
            br#"{"family":"sans-serif","script":"Lat"}"#,
            br#"{"family":"sans-serif","locale":"../x"}"#,
            br#"{"family":"sans-serif","style":"condensed"}"#,
            br#"{"family":"sans-serif","weight":0}"#,
            br#"{"family":"sans-serif","weight":1001}"#,
        ] {
            assert_eq!(request(&path, "POST", "/match", invalid).await.0, 400);
        }
        let long_text = format!(r#"{{"family":"sans-serif","text":"{}"}}"#, "a".repeat(257));
        assert_eq!(
            request(&path, "POST", "/match", long_text.as_bytes())
                .await
                .0,
            400
        );
        let (status, body) = request(&path, "POST", "/stats", b"").await;
        assert_eq!(status, 200);
        let stats: serde_json::Value = serde_json::from_slice(&body).expect("stats json");
        assert_eq!(stats["faces"], 1);
        assert!(service.shutdown().await);
    }

    #[tokio::test]
    async fn oversized_bodies_fail_before_the_worker() {
        let (mut service, path, _directory) = start_static(limits(4, Duration::from_secs(2))).await;
        let oversized = vec![b' '; MAX_REQUEST_BYTES + 16];
        assert_eq!(request(&path, "POST", "/match", &oversized).await.0, 400);
        assert!(service.shutdown().await);
    }

    #[tokio::test]
    async fn concurrent_bodies_are_bounded_before_the_worker() {
        let (mut service, path, _directory) = start_static(limits(4, Duration::from_secs(2))).await;
        let mut stalled = Vec::new();
        for _ in 0..MAX_CONCURRENT_REQUESTS {
            let mut stream = UnixStream::connect(&path).await.expect("raw connect");
            stream
                .write_all(
                    b"POST /match HTTP/1.1\r\nhost: fonts\r\ncontent-type: application/json\r\ncontent-length: 2\r\n\r\n",
                )
                .await
                .expect("write head");
            stream.flush().await.expect("flush");
            stalled.push(stream);
        }
        wait_for(
            || service.budget.available_requests() == 0,
            "stalled bodies did not occupy every request permit",
        )
        .await;
        assert_eq!(
            service.budget.reserved_bytes(),
            MAX_CONCURRENT_REQUESTS * RESPONSE_RESERVATION_BYTES
        );
        assert_eq!(
            request(&path, "POST", "/match", br#"{"family":"sans-serif"}"#)
                .await
                .0,
            429
        );
        for mut stream in stalled {
            let _ = stream.shutdown().await;
        }
        assert!(service.shutdown().await);
        assert_eq!(service.budget.reserved_bytes(), 0);
        assert_eq!(service.budget.available_requests(), MAX_CONCURRENT_REQUESTS);
        assert_eq!(service.budget.active_connections(), 0);
    }

    #[tokio::test]
    async fn queue_full_is_429_and_drains_after_release() {
        let armed = Arc::new(AtomicBool::new(false));
        let started = Arc::new(tokio::sync::Notify::new());
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let backend = GatedBackend {
            armed: armed.clone(),
            started: started.clone(),
            gate: gate.clone(),
        };
        let limits = limits(4, Duration::from_secs(5));
        let (mut service, path, _directory) = start(
            Box::new(move || -> Box<dyn FontBackend> { Box::new(backend) }),
            limits,
        )
        .await;
        armed.store(true, Ordering::SeqCst);
        let first_path = path.clone();
        let first = tokio::spawn(async move {
            request(&first_path, "POST", "/match", br#"{"family":"sans-serif"}"#).await
        });
        started.notified().await;
        let mut queued = Vec::new();
        for _ in 0..DEFAULT_QUEUE {
            let queued_path = path.clone();
            queued.push(tokio::spawn(async move {
                request(
                    &queued_path,
                    "POST",
                    "/match",
                    br#"{"family":"sans-serif"}"#,
                )
                .await
            }));
        }
        wait_pending(&service, 1 + DEFAULT_QUEUE).await;
        assert_eq!(
            service.budget.reserved_bytes(),
            (1 + DEFAULT_QUEUE) * RESPONSE_RESERVATION_BYTES
        );
        let extra = request(&path, "POST", "/match", br#"{"family":"sans-serif"}"#).await;
        assert_eq!(extra.0, 429);
        let (lock, cvar) = &*gate;
        *lock.lock().expect("gate") = true;
        cvar.notify_all();
        assert_eq!(first.await.expect("first task").0, 200);
        for task in queued {
            assert_eq!(task.await.expect("queued task").0, 200);
        }
        assert!(service.shutdown().await);
        assert_eq!(service.budget.reserved_bytes(), 0);
        assert_eq!(service.budget.active_connections(), 0);
    }

    #[tokio::test]
    async fn request_timeout_returns_502_without_stopping_the_worker() {
        let armed = Arc::new(AtomicBool::new(false));
        let started = Arc::new(tokio::sync::Notify::new());
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let backend = GatedBackend {
            armed: armed.clone(),
            started: started.clone(),
            gate: gate.clone(),
        };
        let (mut service, path, _directory) = start(
            Box::new(move || -> Box<dyn FontBackend> { Box::new(backend) }),
            limits(4, Duration::from_millis(100)),
        )
        .await;
        armed.store(true, Ordering::SeqCst);
        let (status, body) = request(&path, "POST", "/match", br#"{"family":"sans-serif"}"#).await;
        assert_eq!(status, 502);
        assert_eq!(body.as_ref(), br#"{"error":"timeout"}"#.as_slice());
        // HTTP 已结束，但 backend 仍阻塞；命令继续持有 lease，预算不释放。
        assert_eq!(service.budget.reserved_bytes(), RESPONSE_RESERVATION_BYTES);
        let (lock, cvar) = &*gate;
        *lock.lock().expect("gate") = true;
        cvar.notify_all();
        wait_for(
            || service.budget.reserved_bytes() == 0,
            "timed out command did not release its lease",
        )
        .await;
        assert!(service.shutdown().await);
        assert_eq!(service.budget.active_connections(), 0);
    }

    #[tokio::test]
    async fn disconnect_does_not_break_later_requests() {
        let armed = Arc::new(AtomicBool::new(false));
        let started = Arc::new(tokio::sync::Notify::new());
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let backend = GatedBackend {
            armed: armed.clone(),
            started: started.clone(),
            gate: gate.clone(),
        };
        let (mut service, path, _directory) = start(
            Box::new(move || -> Box<dyn FontBackend> { Box::new(backend) }),
            limits(4, Duration::from_secs(2)),
        )
        .await;
        armed.store(true, Ordering::SeqCst);
        let body = br#"{"family":"sans-serif"}"#;
        let mut stream = UnixStream::connect(&path).await.expect("raw connect");
        let head = format!(
            "POST /match HTTP/1.1\r\nhost: fonts\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n",
            body.len()
        );
        stream.write_all(head.as_bytes()).await.expect("write head");
        stream.write_all(body).await.expect("write body");
        stream.flush().await.expect("flush");
        wait_pending(&service, 1).await;
        drop(stream);
        let (lock, cvar) = &*gate;
        *lock.lock().expect("gate") = true;
        cvar.notify_all();
        wait_for(
            || service.budget.reserved_bytes() == 0,
            "disconnected request did not release its lease",
        )
        .await;
        assert_eq!(request(&path, "POST", "/match", body).await.0, 200);
        assert!(service.shutdown().await);
    }

    #[tokio::test]
    async fn shutdown_reports_a_panicked_worker() {
        let armed = Arc::new(AtomicBool::new(false));
        let backend = PanicBackend {
            armed: armed.clone(),
        };
        let (mut service, path, _directory) = start(
            Box::new(move || -> Box<dyn FontBackend> { Box::new(backend) }),
            limits(4, Duration::from_secs(2)),
        )
        .await;
        armed.store(true, Ordering::SeqCst);
        assert_eq!(
            request(&path, "POST", "/match", br#"{"family":"sans-serif"}"#)
                .await
                .0,
            502
        );
        assert!(!service.shutdown().await);
    }

    #[tokio::test]
    async fn startup_failure_is_fixed_and_leaves_no_socket() {
        let missing = FontService::start_with(
            tempfile::tempdir().expect("dir").path(),
            Box::new(|| -> Box<dyn FontBackend> { Box::new(MissingBackend(None)) }),
            limits(1, Duration::from_secs(1)),
        )
        .await;
        let error = match missing {
            Ok(_) => panic!("a missing sans-serif face must fail startup"),
            Err(error) => error,
        };
        assert_eq!(error.to_string(), STARTUP_FAILURE);
        let directory = tempfile::tempdir().expect("dir");
        let unreadable = FontService::start_with(
            directory.path(),
            Box::new(|| -> Box<dyn FontBackend> { Box::new(UnreadableBackend) }),
            limits(1, Duration::from_secs(1)),
        )
        .await;
        let error = match unreadable {
            Ok(_) => panic!("an unreadable face must fail startup"),
            Err(error) => error,
        };
        assert_eq!(error.to_string(), STARTUP_FAILURE);
        assert!(!directory.path().join("fonts.sock").exists());
    }

    #[tokio::test]
    async fn startup_reports_unavailable_entropy_without_binding_a_socket() {
        let directory = tempfile::tempdir().expect("dir");
        let result = FontService::start_with(
            directory.path(),
            Box::new(|| -> Box<dyn FontBackend> {
                Box::new(MissingBackend(Some(HostError::new(
                    ErrorCode::EntropyUnavailable,
                    "system entropy is unavailable",
                ))))
            }),
            limits(1, Duration::from_secs(1)),
        )
        .await;
        let error = match result {
            Ok(_) => panic!("unavailable entropy must fail startup"),
            Err(error) => error,
        };
        assert_eq!(error.to_string(), "system entropy is unavailable");
        assert!(!directory.path().join("fonts.sock").exists());
    }

    #[tokio::test]
    async fn socket_is_private_and_shutdown_removes_it() {
        let (mut service, path, _directory) = start_static(limits(4, Duration::from_secs(2))).await;
        let mode = std::fs::metadata(&path)
            .expect("socket metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
        assert!(service.shutdown().await);
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn blocked_writer_holds_connection_and_response_lease() {
        let mut setup = start_large(limits(4, Duration::from_secs(5))).await;
        let client = raw_request(&setup.path, "/face", &face_request_body()).await;
        tokio::time::timeout(Duration::from_secs(5), setup.signal.wait_blocked())
            .await
            .expect("server writer never blocked");
        // handler 已返回且 body 已交给 writer；连接与 response 预算仍然占用。
        assert_eq!(setup.service.budget.active_connections(), 1);
        assert_eq!(
            setup.service.budget.available_connections(),
            MAX_CONNECTIONS - 1
        );
        assert_eq!(
            setup.service.budget.reserved_bytes(),
            RESPONSE_RESERVATION_BYTES
        );
        assert_eq!(
            setup.service.budget.available_requests(),
            MAX_CONCURRENT_REQUESTS - 1
        );
        let weak = setup
            .face
            .lock()
            .expect("face probe")
            .clone()
            .expect("face weak");
        assert!(
            weak.upgrade().is_some(),
            "face bytes were copied before the writer finished"
        );
        assert_eq!(weak.strong_count(), 1);
        drop(client);
        wait_for(
            || {
                setup.service.budget.active_connections() == 0
                    && setup.service.budget.reserved_bytes() == 0
            },
            "connection did not settle after the writer was dropped",
        )
        .await;
        assert_baseline(&setup.service.budget);
        assert!(weak.upgrade().is_none(), "writer kept the face owner alive");
        assert!(setup.service.shutdown().await);
    }

    #[tokio::test]
    async fn disconnect_releases_writer_leases_to_strict_baseline() {
        let mut setup = start_large(limits(4, Duration::from_secs(5))).await;
        let client = raw_request(&setup.path, "/face", &face_request_body()).await;
        tokio::time::timeout(Duration::from_secs(5), setup.signal.wait_blocked())
            .await
            .expect("server writer never blocked");
        drop(client);
        wait_for(
            || {
                setup.service.budget.active_connections() == 0
                    && setup.service.budget.reserved_bytes() == 0
            },
            "disconnect did not release the writer leases",
        )
        .await;
        assert_baseline(&setup.service.budget);
        assert!(setup.service.shutdown().await);
    }

    #[tokio::test]
    async fn writer_deadline_closes_without_appending_json() {
        let mut limits = limits(4, Duration::from_secs(5));
        limits.writer = Duration::from_millis(200);
        limits.connection = Duration::from_secs(10);
        let mut setup = start_large(limits).await;
        let mut client = raw_request(&setup.path, "/face", &face_request_body()).await;
        tokio::time::timeout(Duration::from_secs(5), setup.signal.wait_blocked())
            .await
            .expect("server writer never blocked");
        let started = tokio::time::Instant::now();
        wait_for(
            || setup.service.budget.active_connections() == 0,
            "writer deadline did not close the connection",
        )
        .await;
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "connection was closed by the total deadline, not the writer deadline"
        );
        let mut data = Vec::new();
        tokio::time::timeout(Duration::from_secs(2), client.read_to_end(&mut data))
            .await
            .expect("client did not observe the closed writer")
            .expect("read face response");
        assert!(
            !data.windows(9).any(|window| window == b"{\"error\":"),
            "a started response must not append an error JSON document"
        );
        wait_for(
            || setup.service.budget.reserved_bytes() == 0,
            "writer deadline did not release the response budget",
        )
        .await;
        assert_baseline(&setup.service.budget);
        assert!(setup.service.shutdown().await);
    }

    #[tokio::test]
    async fn extra_connection_is_closed_before_a_handler_starts() {
        let (mut service, path, _directory) = start_static(limits(4, Duration::from_secs(2))).await;
        let mut idle = Vec::new();
        for _ in 0..16 {
            idle.push(UnixStream::connect(&path).await.expect("idle connect"));
        }
        let mut extra = UnixStream::connect(&path).await.expect("extra connect");
        let mut byte = [0u8; 1];
        let read = tokio::time::timeout(
            Duration::from_secs(2),
            AsyncReadExt::read(&mut extra, &mut byte),
        )
        .await
        .expect("extra connection was not closed");
        assert_eq!(read.expect("extra read"), 0);
        drop(idle);
        assert!(service.shutdown().await);
    }

    #[tokio::test]
    async fn connection_limit_rejects_the_seventeenth_and_recovers() {
        let (mut service, path, _directory) = start_static(limits(4, Duration::from_secs(5))).await;
        let mut idle = Vec::new();
        for _ in 0..MAX_CONNECTIONS {
            idle.push(UnixStream::connect(&path).await.expect("idle connect"));
        }
        wait_for(
            || service.budget.active_connections() == MAX_CONNECTIONS,
            "listener did not accept every connection",
        )
        .await;
        assert_eq!(service.budget.available_connections(), 0);
        let mut rejected = UnixStream::connect(&path).await.expect("rejected connect");
        let mut byte = [0u8; 1];
        let read = tokio::time::timeout(Duration::from_secs(2), rejected.read(&mut byte))
            .await
            .expect("seventeenth connection was not closed");
        assert_eq!(read.expect("rejected read"), 0);
        assert_eq!(service.budget.active_connections(), MAX_CONNECTIONS);
        assert_eq!(service.budget.dispatched(), 0);
        drop(idle.pop().expect("one idle connection"));
        wait_for(
            || service.budget.active_connections() == MAX_CONNECTIONS - 1,
            "released connection slot did not return",
        )
        .await;
        let (status, _) = request(&path, "POST", "/stats", b"").await;
        assert_eq!(status, 200);
        for stream in idle {
            drop(stream);
        }
        assert!(service.shutdown().await);
        assert_baseline(&service.budget);
    }

    #[tokio::test]
    async fn pipelined_requests_dispatch_one_backend_and_close() {
        let (mut service, path, _directory) = start_static(limits(4, Duration::from_secs(5))).await;
        let mut pipeline = request_bytes("/match", br#"{"family":"sans-serif"}"#);
        pipeline.extend_from_slice(&request_bytes("/stats", b""));
        let mut stream = UnixStream::connect(&path).await.expect("pipeline connect");
        stream.write_all(&pipeline).await.expect("write pipeline");
        stream.flush().await.expect("flush pipeline");
        let mut data = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut data))
            .await
            .expect("pipelined connection did not close")
            .expect("read pipelined response");
        assert!(data.starts_with(b"HTTP/1.1 200"));
        assert_eq!(service.budget.dispatched(), 1);
        assert!(service.shutdown().await);
        assert_baseline(&service.budget);
    }

    #[tokio::test]
    async fn shutdown_waits_for_connections_and_a_blocked_backend() {
        let mut limits = limits(4, Duration::from_secs(5));
        limits.writer = Duration::from_secs(10);
        let setup = start_large(limits).await;
        let writer_client = raw_request(&setup.path, "/face", &face_request_body()).await;
        tokio::time::timeout(Duration::from_secs(5), setup.signal.wait_blocked())
            .await
            .expect("server writer never blocked");
        setup.match_armed.store(true, Ordering::SeqCst);
        let match_client = raw_request(&setup.path, "/match", br#"{"family":"sans-serif"}"#).await;
        tokio::time::timeout(Duration::from_secs(5), setup.match_started.notified())
            .await
            .expect("font backend did not start");
        let mut slow_body = UnixStream::connect(&setup.path)
            .await
            .expect("slow body connect");
        slow_body
            .write_all(
                b"POST /match HTTP/1.1\r\nhost: fonts\r\ncontent-type: application/json\r\ncontent-length: 2\r\n\r\n",
            )
            .await
            .expect("write slow body head");
        slow_body.flush().await.expect("flush slow body head");
        wait_for(
            || setup.service.budget.available_requests() == MAX_CONCURRENT_REQUESTS - 3,
            "slow body, blocked match and blocked writer did not all hold leases",
        )
        .await;
        assert_eq!(
            setup.service.budget.reserved_bytes(),
            3 * RESPONSE_RESERVATION_BYTES
        );
        assert_eq!(setup.service.budget.active_connections(), 3);
        let budget = setup.service.budget.clone();
        let socket = setup.path.clone();
        let service = setup.service;
        let mut shutdown = tokio::spawn(async move {
            let mut service = service;
            service.shutdown().await
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(150), &mut shutdown)
                .await
                .is_err(),
            "shutdown finished while the font thread was still blocked"
        );
        let (lock, cvar) = &*setup.gate;
        *lock.lock().expect("gate") = true;
        cvar.notify_all();
        let joined = tokio::time::timeout(Duration::from_secs(5), shutdown)
            .await
            .expect("shutdown did not settle")
            .expect("shutdown task");
        assert!(joined);
        assert_baseline(&budget);
        assert!(!socket.exists());
        drop(writer_client);
        drop(match_client);
        drop(slow_body);
    }
}
