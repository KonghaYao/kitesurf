// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// 进程内系统字体服务：一个串行有界线程拥有字体库，HTTP 只在私有 Unix socket 上提供
// 现有 /match、/face 与 /stats。它不接收路径、任意字体族或 Fontconfig 配置，也不启动
// 子进程。

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::{Method, Request, Response, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot, watch, Semaphore};
use tokio::task::JoinHandle;

use kitesurf_font_host::{
    ErrorCode, FaceSummary, FontHost, HostError, MatchRequest, MatchResponse, StatsResponse,
    MAX_FACE_BYTES,
};

use crate::services::{
    bind_private_socket, empty_response, json_response, remove_socket, spawn_listener,
    ConnectionHandler, ConnectionTasks,
};
use crate::HostResult;

pub(crate) const STARTUP_FAILURE: &str =
    "No usable system sans-serif font is installed; the local browser cannot start without a base font.";

const MAX_REQUEST_BYTES: usize = 64 * 1024;
const MAX_MATCH_RESPONSE_BYTES: usize = 64 * 1024;
const MAX_CONCURRENT_REQUESTS: usize = 8;
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
    },
    Face {
        id: String,
        reply: oneshot::Sender<Result<Arc<Vec<u8>>, HostError>>,
    },
    Stats {
        reply: oneshot::Sender<StatsResponse>,
    },
    Stop,
}

#[derive(Clone, Copy)]
struct FontLimits {
    queue: usize,
    request: Duration,
    body: Duration,
    startup: Duration,
}

impl Default for FontLimits {
    fn default() -> Self {
        Self {
            queue: DEFAULT_QUEUE,
            request: DEFAULT_REQUEST_TIMEOUT,
            body: DEFAULT_BODY_TIMEOUT,
            startup: DEFAULT_STARTUP_TIMEOUT,
        }
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
    permits: Arc<Semaphore>,
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
        let path = run_dir.join("fonts.sock");
        let (commands, receiver) = mpsc::channel(limits.queue.max(1));
        let (ready, ready_rx) = oneshot::channel();
        let worker = std::thread::Builder::new()
            .name("kitesurf-fonts".to_string())
            .spawn(move || run_worker(factory, receiver, ready))
            .map_err(|error| format!("cannot start the system font worker: {error}"))?;
        match tokio::time::timeout(limits.startup, ready_rx).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(()))) | Ok(Err(_)) => {
                let _ = worker.join();
                return Err(STARTUP_FAILURE.into());
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
        let permits = Arc::new(Semaphore::new(MAX_CONCURRENT_REQUESTS));
        let handler = Arc::new(FontHandler {
            commands: commands.clone(),
            limits,
            pending: pending.clone(),
            permits: permits.clone(),
        });
        let task = spawn_listener(listener, shutdown_rx, handler, connections.clone());
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
            permits,
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

fn run_worker(
    factory: Box<dyn FnOnce() -> Box<dyn FontBackend> + Send>,
    mut commands: mpsc::Receiver<FontCommand>,
    ready: oneshot::Sender<Result<(), ()>>,
) {
    let mut backend = factory();
    if validate_startup(&mut *backend).is_err() {
        let _ = ready.send(Err(()));
        return;
    }
    if ready.send(Ok(())).is_err() {
        return;
    }
    while let Some(command) = commands.blocking_recv() {
        match command {
            FontCommand::Match { request, reply } => {
                let _ = reply.send(backend.match_faces(&request));
            }
            FontCommand::Face { id, reply } => {
                let _ = reply.send(backend.face_bytes(&id));
            }
            FontCommand::Stats { reply } => {
                let _ = reply.send(backend.stats());
            }
            FontCommand::Stop => break,
        }
    }
}

fn validate_startup(backend: &mut dyn FontBackend) -> Result<(), ()> {
    let request = MatchRequest {
        family: Some("sans-serif".to_string()),
        text: Some(STARTUP_SAMPLE.to_string()),
        weight: Some(400.0),
        style: Some("normal".to_string()),
        ..MatchRequest::default()
    };
    let response = backend.match_faces(&request).map_err(|_| ())?;
    if !response.covered {
        return Err(());
    }
    let face: &FaceSummary = response.faces.first().ok_or(())?;
    if face.bytes == 0 || face.bytes > MAX_FACE_BYTES {
        return Err(());
    }
    let bytes = backend.face_bytes(&face.id).map_err(|_| ())?;
    if bytes.is_empty() || bytes.len() > MAX_FACE_BYTES {
        return Err(());
    }
    Ok(())
}

struct FontHandler {
    commands: mpsc::Sender<FontCommand>,
    limits: FontLimits,
    pending: Arc<AtomicUsize>,
    permits: Arc<Semaphore>,
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
    async fn serve(&self, request: Request<Incoming>) -> Response<Full<Bytes>> {
        let Ok(_permit) = self.permits.clone().try_acquire_owned() else {
            return status_response(StatusCode::TOO_MANY_REQUESTS);
        };
        if request.method() != Method::POST {
            return empty_response(StatusCode::METHOD_NOT_ALLOWED);
        }
        let path = request.uri().path().to_string();
        if declared_length(&request).is_some_and(|length| length > MAX_REQUEST_BYTES as u64) {
            return invalid_request();
        }
        let body = match self.read_body(request.into_body()).await {
            Ok(body) => body,
            Err(status) => return json_response(status, r#"{"error":"invalid request"}"#),
        };
        match path.as_str() {
            "/match" => self.match_response(&body).await,
            "/face" => self.face_response(&body).await,
            "/stats" => self.stats_response().await,
            _ => empty_response(StatusCode::NOT_FOUND),
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
                Ok(())
            }
            Err(TrySendError::Full(_)) => Err(StatusCode::TOO_MANY_REQUESTS),
            Err(TrySendError::Closed(_)) => Err(StatusCode::BAD_GATEWAY),
        }
    }

    async fn match_response(&self, body: &[u8]) -> Response<Full<Bytes>> {
        let Ok(input) = serde_json::from_slice::<MatchRequestInput>(body) else {
            return invalid_request();
        };
        if !valid_match_input(&input) {
            return invalid_request();
        }
        let request: MatchRequest = input.into();
        let (reply, receiver) = oneshot::channel();
        if let Err(status) = self.enqueue(FontCommand::Match { request, reply }) {
            return status_response(status);
        }
        let _pending = PendingGuard(self.pending.clone());
        match tokio::time::timeout(self.limits.request, receiver).await {
            Err(_) => json_response(StatusCode::BAD_GATEWAY, r#"{"error":"timeout"}"#),
            Ok(Err(_)) => unavailable(),
            Ok(Ok(Err(error))) => font_error_response(error),
            Ok(Ok(Ok(response))) => {
                let output = MatchOutput::from(&response);
                match serde_json::to_vec(&output) {
                    Ok(bytes) if bytes.len() <= MAX_MATCH_RESPONSE_BYTES => {
                        json_response(StatusCode::OK, bytes)
                    }
                    _ => unavailable(),
                }
            }
        }
    }

    async fn face_response(&self, body: &[u8]) -> Response<Full<Bytes>> {
        let Ok(request) = serde_json::from_slice::<FaceRequest>(body) else {
            return invalid_request();
        };
        if !valid_id(&request.id) {
            return invalid_request();
        }
        let (reply, receiver) = oneshot::channel();
        if let Err(status) = self.enqueue(FontCommand::Face {
            id: request.id,
            reply,
        }) {
            return status_response(status);
        }
        let _pending = PendingGuard(self.pending.clone());
        match tokio::time::timeout(self.limits.request, receiver).await {
            Err(_) => json_response(StatusCode::BAD_GATEWAY, r#"{"error":"timeout"}"#),
            Ok(Err(_)) => unavailable(),
            Ok(Ok(Err(error))) => font_error_response(error),
            Ok(Ok(Ok(bytes))) if bytes.is_empty() || bytes.len() > MAX_FACE_BYTES => unavailable(),
            Ok(Ok(Ok(bytes))) => Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "font/otf")
                .header("content-length", bytes.len().to_string())
                .header("cache-control", "no-store")
                .body(Full::new(Bytes::from(bytes.as_ref().clone())))
                .expect("font response"),
        }
    }

    async fn stats_response(&self) -> Response<Full<Bytes>> {
        let (reply, receiver) = oneshot::channel();
        if let Err(status) = self.enqueue(FontCommand::Stats { reply }) {
            return status_response(status);
        }
        let _pending = PendingGuard(self.pending.clone());
        match tokio::time::timeout(self.limits.request, receiver).await {
            Err(_) => json_response(StatusCode::BAD_GATEWAY, r#"{"error":"timeout"}"#),
            Ok(Err(_)) => unavailable(),
            Ok(Ok(stats)) => {
                let output = StatsOutput::from(&stats);
                match serde_json::to_vec(&output) {
                    Ok(bytes) => json_response(StatusCode::OK, bytes),
                    Err(_) => unavailable(),
                }
            }
        }
    }
}

impl ConnectionHandler for FontHandler {
    fn handle(
        &self,
        request: Request<Incoming>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Response<Full<Bytes>>> + Send + '_>>
    {
        Box::pin(async move { self.serve(request).await })
    }
}

struct PendingGuard(Arc<AtomicUsize>);

impl Drop for PendingGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

fn status_response(status: StatusCode) -> Response<Full<Bytes>> {
    let error = match status {
        StatusCode::TOO_MANY_REQUESTS => "busy",
        _ => "unavailable",
    };
    json_response(status, json!({ "error": error }).to_string())
}

fn invalid_request() -> Response<Full<Bytes>> {
    json_response(StatusCode::BAD_REQUEST, r#"{"error":"invalid request"}"#)
}

fn unavailable() -> Response<Full<Bytes>> {
    json_response(StatusCode::BAD_GATEWAY, r#"{"error":"unavailable"}"#)
}

fn font_error_response(error: HostError) -> Response<Full<Bytes>> {
    let status = match error.code {
        ErrorCode::BadRequest | ErrorCode::UnknownId | ErrorCode::ExpiredId => {
            StatusCode::NOT_FOUND
        }
        _ => StatusCode::BAD_GATEWAY,
    };
    json_response(
        status,
        json!({ "error": error.code.as_str(), "message": error.message }).to_string(),
    )
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
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::atomic::AtomicBool;
    use std::sync::Condvar;

    use hyper_util::rt::TokioIo;
    use tokio::io::AsyncWriteExt;
    use tokio::net::UnixStream;

    const FACE_ID: &str = "0123456789abcdef0123456789abcdef";
    const FACE_BYTES: [u8; 8] = [0, 1, 0, 0, 0, 0, 0, 0];

    fn limits(queue: usize, request: Duration) -> FontLimits {
        FontLimits {
            queue,
            request,
            body: Duration::from_secs(2),
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

    struct MissingBackend;

    impl FontBackend for MissingBackend {
        fn match_faces(&mut self, _request: &MatchRequest) -> Result<MatchResponse, HostError> {
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

    async fn request(path: &Path, method: &str, target: &str, body: &[u8]) -> (u16, Bytes) {
        let stream = UnixStream::connect(path)
            .await
            .expect("connect fonts socket");
        let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .expect("http handshake");
        tokio::spawn(async move {
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
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while service.permits.available_permits() > 0 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "stalled bodies did not occupy every request permit"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
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
        let limits = limits(2, Duration::from_secs(5));
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
        let path_a = path.clone();
        let queued_a = tokio::spawn(async move {
            request(&path_a, "POST", "/match", br#"{"family":"sans-serif"}"#).await
        });
        let path_b = path.clone();
        let queued_b = tokio::spawn(async move {
            request(&path_b, "POST", "/match", br#"{"family":"sans-serif"}"#).await
        });
        wait_pending(&service, 3).await;
        let extra = request(&path, "POST", "/match", br#"{"family":"sans-serif"}"#).await;
        assert_eq!(extra.0, 429);
        let (lock, cvar) = &*gate;
        *lock.lock().expect("gate") = true;
        cvar.notify_all();
        assert_eq!(first.await.expect("first task").0, 200);
        assert_eq!(queued_a.await.expect("queued task").0, 200);
        assert_eq!(queued_b.await.expect("queued task").0, 200);
        assert!(service.shutdown().await);
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
        let (lock, cvar) = &*gate;
        *lock.lock().expect("gate") = true;
        cvar.notify_all();
        assert!(service.shutdown().await);
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
        tokio::time::sleep(Duration::from_millis(50)).await;
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
            Box::new(|| -> Box<dyn FontBackend> { Box::new(MissingBackend) }),
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
}
