// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// workerd 通过 Unix socket 访问的两个私有服务。profile 默认是显式 --profile 打开的
// SQLite 存储，--test-memory-profile 才启用内存替身；两者共用同一套 owner/revision 与
// 原始 JSON 文本规则。资源只从显式构建清单提供，并对照清单做哈希校验。

use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::header::{ETAG, IF_NONE_MATCH, RANGE};
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::net::UnixListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::observe::Observer;
use crate::profile::{self, ProfileError, ProfileErrorKind};
use crate::profile_state::{self, Json as StateJson, JsonString};
use crate::HostResult;

const DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

pub struct ServicePaths {
    pub profile: PathBuf,
    pub assets: PathBuf,
}

#[derive(Debug)]
pub struct LoadResult {
    pub revision: i64,
    pub state_text: String,
}

/// 私有 profile 的两种实现：显式测试替身和正式 SQLite；HTTP 语义不区分二者。
pub trait ProfileBackend: Send + Sync {
    fn load(&self, owner: &[u16]) -> Result<LoadResult, ProfileError>;
    fn save(
        &self,
        owner: &[u16],
        revision: &StateJson,
        state: Option<&StateJson>,
    ) -> Result<(), ProfileError>;
    fn status(&self) -> Result<Value, ProfileError>;
    fn kind(&self) -> &'static str;
    fn display(&self) -> String;
}

pub struct ProfileService {
    backend: Arc<dyn ProfileBackend>,
    inflight: Arc<AtomicUsize>,
}

impl ProfileService {
    pub fn new(backend: Arc<dyn ProfileBackend>) -> Self {
        Self {
            backend,
            inflight: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn kind(&self) -> &'static str {
        self.backend.kind()
    }

    pub fn display(&self) -> String {
        self.backend.display()
    }

    pub async fn load(&self, owner: JsonString) -> Result<LoadResult, ProfileError> {
        let guard = self.begin();
        let backend = self.backend.clone();
        let result = tokio::task::spawn_blocking(move || {
            let _guard = guard;
            backend.load(&owner)
        })
        .await;
        result.map_err(|_| ProfileError::conflict())?
    }

    pub async fn save(
        &self,
        owner: JsonString,
        revision: StateJson,
        state: Option<StateJson>,
    ) -> Result<(), ProfileError> {
        let guard = self.begin();
        let backend = self.backend.clone();
        let result = tokio::task::spawn_blocking(move || {
            let _guard = guard;
            backend.save(&owner, &revision, state.as_ref())
        })
        .await;
        result.map_err(|_| ProfileError::conflict())?
    }

    pub async fn status(&self) -> Result<Value, ProfileError> {
        let guard = self.begin();
        let backend = self.backend.clone();
        let result = tokio::task::spawn_blocking(move || {
            let _guard = guard;
            backend.status()
        })
        .await;
        result.map_err(|_| ProfileError::conflict())?
    }

    /// 计数归阻塞闭包所有：调用方被取消时，数据库操作结束前计数不会归零。
    fn begin(&self) -> InflightGuard {
        self.inflight.fetch_add(1, Ordering::SeqCst);
        InflightGuard {
            counter: self.inflight.clone(),
        }
    }

    async fn drain(&self) {
        while self.inflight.load(Ordering::SeqCst) != 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}

struct InflightGuard {
    counter: Arc<AtomicUsize>,
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::SeqCst);
    }
}

pub struct MemoryProfile {
    inner: Mutex<MemoryInner>,
}

struct MemoryInner {
    owner: Option<Vec<u16>>,
    revision: i64,
    state_text: String,
    loads: u64,
    saves: u64,
}

impl MemoryProfile {
    /// 仅由显式 --test-memory-profile 启用，seed 数据是合成 fixture。
    pub fn new(origin: &str) -> HostResult<Self> {
        Ok(Self {
            inner: Mutex::new(MemoryInner {
                owner: None,
                revision: 0,
                state_text: initial_state(origin)?,
                loads: 0,
                saves: 0,
            }),
        })
    }
}

impl ProfileBackend for MemoryProfile {
    fn load(&self, owner: &[u16]) -> Result<LoadResult, ProfileError> {
        if owner.is_empty() {
            return Err(ProfileError::conflict());
        }
        let mut inner = self.inner.lock().map_err(|_| ProfileError::conflict())?;
        match &inner.owner {
            Some(current) if current.as_slice() != owner => {
                return Err(ProfileError::conflict());
            }
            Some(_) => {}
            None => inner.owner = Some(owner.to_vec()),
        }
        inner.loads += 1;
        Ok(LoadResult {
            revision: inner.revision,
            state_text: inner.state_text.clone(),
        })
    }

    fn save(
        &self,
        owner: &[u16],
        revision: &StateJson,
        state: Option<&StateJson>,
    ) -> Result<(), ProfileError> {
        let mut inner = self.inner.lock().map_err(|_| ProfileError::conflict())?;
        if inner.owner.as_deref() != Some(owner) {
            return Err(ProfileError::conflict());
        }
        let revision = profile_state::safe_integer(revision).ok_or_else(ProfileError::conflict)?;
        let state = state.ok_or_else(ProfileError::invalid_state)?;
        let text =
            profile_state::canonicalize_state(state).map_err(|_| ProfileError::invalid_state())?;
        if revision == inner.revision {
            return (inner.state_text == text)
                .then_some(())
                .ok_or_else(ProfileError::conflict);
        }
        if revision != inner.revision.saturating_add(1) {
            return Err(ProfileError::conflict());
        }
        inner.revision = revision;
        inner.state_text = text;
        inner.saves += 1;
        Ok(())
    }

    fn status(&self) -> Result<Value, ProfileError> {
        let inner = self.inner.lock().map_err(|_| ProfileError::conflict())?;
        Ok(json!({
          "revision": inner.revision,
          "loads": inner.loads,
          "saves": inner.saves,
          "bytes": inner.state_text.len(),
        }))
    }

    fn kind(&self) -> &'static str {
        "memory"
    }

    fn display(&self) -> String {
        "memory".to_string()
    }
}

pub struct SqliteProfile {
    store: profile::ProfileStore,
}

impl SqliteProfile {
    pub fn open(path: &Path, create: bool) -> HostResult<Self> {
        Ok(Self {
            store: profile::ProfileStore::open(path, create)?,
        })
    }
}

impl ProfileBackend for SqliteProfile {
    fn load(&self, owner: &[u16]) -> Result<LoadResult, ProfileError> {
        let read = self.store.acquire(owner)?;
        Ok(LoadResult {
            revision: read.revision,
            state_text: read.state_text,
        })
    }

    fn save(
        &self,
        owner: &[u16],
        revision: &StateJson,
        state: Option<&StateJson>,
    ) -> Result<(), ProfileError> {
        self.store.save(owner, revision, state)
    }

    fn status(&self) -> Result<Value, ProfileError> {
        self.store.status().map(|status| status.to_json())
    }

    fn kind(&self) -> &'static str {
        "sqlite"
    }

    fn display(&self) -> String {
        self.store.status().map_or_else(
            |_| "sqlite".to_string(),
            |status| status.path.display().to_string(),
        )
    }
}

struct ConnectionTasks {
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl ConnectionTasks {
    fn new() -> Self {
        Self {
            tasks: Mutex::new(Vec::new()),
        }
    }

    fn track(&self, handle: JoinHandle<()>) {
        let Ok(mut tasks) = self.tasks.lock() else {
            handle.abort();
            return;
        };
        tasks.retain(|task| !task.is_finished());
        tasks.push(handle);
    }

    async fn close(&self) {
        let handles = self
            .tasks
            .lock()
            .map(|mut tasks| std::mem::take(&mut *tasks))
            .unwrap_or_default();
        for handle in &handles {
            handle.abort();
        }
        for handle in handles {
            let _ = handle.await;
        }
    }
}

pub struct PrivateServices {
    shutdown: watch::Sender<bool>,
    profile: Arc<ProfileService>,
    paths: ServicePaths,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    connections: Arc<ConnectionTasks>,
}

impl PrivateServices {
    /// 在 workerd 启动前绑定两个 socket；调用方把它们的路径作为外部地址传入。
    pub fn bind(
        run_dir: &Path,
        assets_root: &Path,
        manifest_path: &Path,
        profile: Arc<ProfileService>,
        observer: Arc<Observer>,
    ) -> HostResult<Self> {
        let profile_path = run_dir.join("profile.sock");
        let assets_path = run_dir.join("assets.sock");
        remove_socket(&profile_path);
        remove_socket(&assets_path);
        let profile_listener = bind_private_socket(&profile_path)?;
        let assets_listener = bind_private_socket(&assets_path)?;

        let connections = Arc::new(ConnectionTasks::new());
        let handler = Arc::new(ProfileHandler {
            profile: profile.clone(),
            observer: observer.clone(),
        });
        let assets = Arc::new(AssetStore::load(assets_root, manifest_path, observer)?);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let tasks = vec![
            spawn_listener(
                profile_listener,
                shutdown_rx.clone(),
                handler,
                connections.clone(),
            ),
            spawn_listener(assets_listener, shutdown_rx, assets, connections.clone()),
        ];
        Ok(Self {
            shutdown,
            profile,
            paths: ServicePaths {
                profile: profile_path,
                assets: assets_path,
            },
            tasks: Mutex::new(tasks),
            connections,
        })
    }

    pub fn paths(&self) -> &ServicePaths {
        &self.paths
    }

    pub fn profile(&self) -> Arc<ProfileService> {
        self.profile.clone()
    }

    pub fn stop(&self) {
        let _ = self.shutdown.send(true);
    }

    /// 先停止监听，再关闭已接受的连接，最后等到已开始的数据库操作结束。被取消的连接可能
    /// 收不到响应，但提交结果由 revision 重试确定，且关闭前一定释放 profile 锁。
    pub async fn shutdown(&self) {
        self.stop();
        let listeners = self
            .tasks
            .lock()
            .map(|mut tasks| std::mem::take(&mut *tasks))
            .unwrap_or_default();
        for listener in listeners {
            let _ = listener.await;
        }
        self.connections.close().await;
        let _ = tokio::time::timeout(DRAIN_TIMEOUT, self.profile.drain()).await;
    }
}

impl Drop for PrivateServices {
    fn drop(&mut self) {
        self.stop();
        if let Ok(mut guard) = self.tasks.lock() {
            for task in guard.drain(..) {
                task.abort();
            }
        }
    }
}

fn remove_socket(path: &Path) {
    if path.exists() {
        let _ = std::fs::remove_file(path);
    }
}

/// socket 只允许当前用户读写；父目录仍由 main 的 0700 run dir 保护。
fn bind_private_socket(path: &Path) -> HostResult<UnixListener> {
    let listener = UnixListener::bind(path)?;
    if let Err(error) = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)) {
        drop(listener);
        let _ = std::fs::remove_file(path);
        return Err(format!("cannot secure private socket: {error}").into());
    }
    Ok(listener)
}

fn spawn_listener(
    listener: UnixListener,
    shutdown: watch::Receiver<bool>,
    handler: Arc<dyn ConnectionHandler>,
    connections: Arc<ConnectionTasks>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut shutdown = shutdown;
        loop {
            tokio::select! {
              changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                  break;
                }
              }
              accepted = listener.accept() => match accepted {
                Ok((stream, _)) => {
                  let handler = handler.clone();
                  let task = tokio::spawn(async move {
                    let service = service_fn(move |request| {
                      let handler = handler.clone();
                      async move { Ok::<_, std::convert::Infallible>(handler.handle(request).await) }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                      .serve_connection(TokioIo::new(stream), service)
                      .await;
                  });
                  connections.track(task);
                }
                Err(_) => break,
              }
            }
        }
    })
}

trait ConnectionHandler: Send + Sync {
    fn handle(
        &self,
        request: Request<Incoming>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Response<Full<Bytes>>> + Send + '_>>;
}

fn empty_response(status: StatusCode) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .body(Full::new(Bytes::new()))
        .expect("static response")
}

fn json_response(status: StatusCode, body: impl Into<Bytes>) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .header("cache-control", "no-store")
        .body(Full::new(body.into()))
        .expect("json response")
}

async fn collect_body(body: Incoming) -> std::result::Result<Vec<u8>, ()> {
    use http_body_util::BodyExt;
    match body.collect().await {
        Ok(collected) => Ok(collected.to_bytes().to_vec()),
        Err(_) => Err(()),
    }
}

struct ProfileHandler {
    profile: Arc<ProfileService>,
    observer: Arc<Observer>,
}

impl ProfileHandler {
    async fn load(&self, owner: JsonString) -> (Response<Full<Bytes>>, Result<i64, ProfileError>) {
        match self.profile.load(owner).await {
            Ok(read) => {
                let mut body = format!("{{\"revision\":{},\"state\":", read.revision).into_bytes();
                body.extend_from_slice(read.state_text.as_bytes());
                body.push(b'}');
                (json_response(StatusCode::OK, body), Ok(read.revision))
            }
            Err(error) => (profile_error_response(error), Err(error)),
        }
    }

    async fn save(
        &self,
        owner: JsonString,
        revision: StateJson,
        state: Option<StateJson>,
    ) -> (Response<Full<Bytes>>, Result<i64, ProfileError>) {
        let requested = profile_state::safe_integer(&revision);
        match self.profile.save(owner, revision, state).await {
            Ok(()) => (
                json_response(StatusCode::OK, r#"{"saved":true}"#),
                Ok(requested.unwrap_or(0)),
            ),
            Err(error) => (profile_error_response(error), Err(error)),
        }
    }

    fn observe(&self, operation: &str, revision: Option<i64>, status: u16) {
        self.observer.with(
            "profile",
            json!({
              "kind": "profile",
              "operation": operation,
              "revision": revision,
              "status": status,
            }),
        );
    }
}

impl ConnectionHandler for ProfileHandler {
    fn handle(
        &self,
        request: Request<Incoming>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Response<Full<Bytes>>> + Send + '_>>
    {
        Box::pin(async move {
            if request.method() != Method::POST {
                return empty_response(StatusCode::NOT_FOUND);
            }
            let path = request.uri().path().to_string();
            if path != "/load" && path != "/save" {
                return empty_response(StatusCode::NOT_FOUND);
            }
            let Ok(bytes) = collect_body(request.into_body()).await else {
                self.observe(&path[1..], None, 409);
                return profile_error_response(ProfileError::conflict());
            };
            let Ok(text) = std::str::from_utf8(&bytes) else {
                self.observe(&path[1..], None, 409);
                return profile_error_response(ProfileError::conflict());
            };
            let Ok(body) = profile_state::parse(text) else {
                self.observe(&path[1..], None, 409);
                return profile_error_response(ProfileError::conflict());
            };
            let Some(owner) = request_owner(&body) else {
                self.observe(&path[1..], None, 409);
                return profile_error_response(ProfileError::conflict());
            };
            if path == "/load" {
                let (response, result) = self.load(owner).await;
                match result {
                    Ok(revision) => {
                        self.observe("load", Some(revision), response.status().as_u16())
                    }
                    Err(_) => self.observe("load", None, response.status().as_u16()),
                }
                return response;
            }
            let revision = body.get("revision").cloned().unwrap_or(StateJson::Null);
            let requested = profile_state::safe_integer(&revision);
            let state = body.get("state").cloned();
            let (response, result) = self.save(owner, revision, state).await;
            match result {
                Ok(revision) => self.observe("save", Some(revision), response.status().as_u16()),
                Err(_) => self.observe("save", requested, response.status().as_u16()),
            }
            response
        })
    }
}

fn request_owner(body: &StateJson) -> Option<JsonString> {
    match body.get("owner") {
        Some(StateJson::String(owner)) if !owner.is_empty() => Some(owner.clone()),
        _ => None,
    }
}

fn profile_error_response(error: ProfileError) -> Response<Full<Bytes>> {
    let (status, code) = match error.kind() {
        ProfileErrorKind::InvalidState => (StatusCode::UNPROCESSABLE_ENTITY, "INVALID_STATE"),
        ProfileErrorKind::Conflict => (StatusCode::CONFLICT, "COMMIT_UNKNOWN"),
    };
    json_response(
        status,
        json!({ "code": code, "message": error.to_string() }).to_string(),
    )
}

fn initial_state(origin: &str) -> HostResult<String> {
    let authority = origin
        .split_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or(origin)
        .split('/')
        .next()
        .unwrap_or("");
    let host = authority
        .rsplit_once(':')
        .filter(|(_, port)| port.chars().all(|c| c.is_ascii_digit()) && !port.is_empty())
        .map(|(host, _)| host)
        .unwrap_or(authority);
    if host.is_empty() {
        return Err("profile origin needs a host".into());
    }
    let state = json!({
      "cookies": [
        {
          "name": "seed",
          "value": "initial",
          "host": host,
          "path": "/",
          "secure": false,
          "httpOnly": false,
          "sameSite": "Lax",
          "hostOnly": true,
          "expiresAt": null,
          "createdAt": 1,
        },
        {
          "name": "sid",
          "value": "http-only-seed",
          "host": host,
          "path": "/",
          "secure": false,
          "httpOnly": true,
          "sameSite": "Lax",
          "hostOnly": true,
          "expiresAt": null,
          "createdAt": 2,
        }
      ],
      "localStorage": [[origin, [["seed", "initial"]]]],
    });
    let text = serde_json::to_string(&state)?;
    let parsed = profile_state::parse(&text).map_err(|_| "invalid seed state".to_string())?;
    profile_state::canonicalize_state(&parsed).map_err(|_| "invalid seed state".into())
}

#[derive(Deserialize)]
struct AssetManifest {
    files: Vec<ManifestFile>,
}

#[derive(Deserialize)]
struct ManifestFile {
    path: String,
    size: u64,
    sha256: String,
}

struct AssetEntry {
    path: PathBuf,
    size: u64,
    sha256: String,
}

pub struct AssetStore {
    root: PathBuf,
    files: HashMap<String, AssetEntry>,
    observer: Arc<Observer>,
}

impl AssetStore {
    fn load(root: &Path, manifest_path: &Path, observer: Arc<Observer>) -> HostResult<Self> {
        let manifest_text = std::fs::read_to_string(manifest_path)?;
        let manifest: AssetManifest = serde_json::from_str(&manifest_text)?;
        let mut files = HashMap::new();
        for file in manifest.files {
            validate_asset_path(&file.path)?;
            if files.contains_key(&file.path) {
                return Err(format!("duplicate asset path {}", file.path).into());
            }
            files.insert(
                file.path.clone(),
                AssetEntry {
                    path: root.join(file.path.trim_start_matches('/')),
                    size: file.size,
                    sha256: file.sha256,
                },
            );
        }
        Ok(Self {
            root: root.to_path_buf(),
            files,
            observer,
        })
    }

    async fn serve(
        &self,
        method: &Method,
        path: &str,
        range: Option<String>,
        if_none_match: Option<String>,
    ) -> Response<Full<Bytes>> {
        if method != Method::GET && method != Method::HEAD {
            return empty_response(StatusCode::METHOD_NOT_ALLOWED);
        }
        let Some(entry) = self.files.get(path) else {
            self.observer.with(
                "asset",
                json!({ "kind": "asset", "path": path, "status": 404 }),
            );
            return empty_response(StatusCode::NOT_FOUND);
        };
        if !entry.path.starts_with(&self.root) {
            return empty_response(StatusCode::FORBIDDEN);
        }
        let expected_size = entry.size;
        let expected_hash = entry.sha256.clone();
        let file = entry.path.clone();
        let read = tokio::task::spawn_blocking(move || -> HostResult<Vec<u8>> {
            let bytes = std::fs::read(&file)?;
            if bytes.len() as u64 != expected_size {
                return Err(format!("asset size mismatch for {}", file.display()).into());
            }
            let digest = Sha256::digest(&bytes);
            let mut hex = String::with_capacity(digest.len() * 2);
            for byte in digest {
                use std::fmt::Write;
                let _ = write!(hex, "{byte:02x}");
            }
            if hex != expected_hash {
                return Err(format!("asset hash mismatch for {}", file.display()).into());
            }
            Ok(bytes)
        })
        .await;
        let bytes = match read {
            Ok(Ok(bytes)) => bytes,
            Ok(Err(error)) => {
                eprintln!("[assets] {error}");
                return empty_response(StatusCode::INTERNAL_SERVER_ERROR);
            }
            Err(error) => {
                eprintln!("[assets] read task failed: {error}");
                return empty_response(StatusCode::INTERNAL_SERVER_ERROR);
            }
        };
        let etag = format!("\"{}\"", entry.sha256.chars().take(32).collect::<String>());
        if if_none_match.as_deref() == Some(etag.as_str()) {
            return Response::builder()
                .status(StatusCode::NOT_MODIFIED)
                .header(ETAG, etag)
                .body(Full::new(Bytes::new()))
                .expect("304 response");
        }
        let total = bytes.len();
        let content_type = content_type(path);
        let (status, body, content_range) = match range {
            None => (StatusCode::OK, bytes, None),
            Some(value) => match parse_range(&value, total) {
                RangeOutcome::Satisfiable(start, end) => {
                    let slice = bytes[start..=end].to_vec();
                    (
                        StatusCode::PARTIAL_CONTENT,
                        slice,
                        Some(format!("bytes {start}-{end}/{total}")),
                    )
                }
                RangeOutcome::Unsatisfiable => {
                    self.observer.with(
                        "asset",
                        json!({ "kind": "asset", "path": path, "status": 416, "bytes": 0 }),
                    );
                    return Response::builder()
                        .status(StatusCode::RANGE_NOT_SATISFIABLE)
                        .header("content-range", format!("bytes */{total}"))
                        .body(Full::new(Bytes::new()))
                        .expect("416 response");
                }
                RangeOutcome::Ignore => (StatusCode::OK, bytes, None),
            },
        };
        let length = body.len();
        let body = if method == Method::HEAD {
            Bytes::new()
        } else {
            Bytes::from(body)
        };
        self.observer.with(
            "asset",
            json!({
              "kind": "asset",
              "path": path,
              "status": status.as_u16(),
              "bytes": length,
            }),
        );
        let mut builder = Response::builder()
            .status(status)
            .header("content-type", content_type)
            .header("content-length", length.to_string())
            .header(ETAG, etag)
            .header("cache-control", "no-store");
        if let Some(content_range) = content_range {
            builder = builder.header("content-range", content_range);
        }
        builder.body(Full::new(body)).expect("asset response")
    }
}

impl ConnectionHandler for AssetStore {
    fn handle(
        &self,
        request: Request<Incoming>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Response<Full<Bytes>>> + Send + '_>>
    {
        Box::pin(async move {
            let path = request.uri().path().to_string();
            let range = request
                .headers()
                .get(RANGE)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string);
            let if_none_match = request
                .headers()
                .get(IF_NONE_MATCH)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string);
            self.serve(request.method(), &path, range, if_none_match)
                .await
        })
    }
}

fn validate_asset_path(path: &str) -> HostResult<()> {
    if !path.starts_with('/') || path.contains('\\') || path.contains("//") {
        return Err(format!("invalid asset path {path}").into());
    }
    for segment in path.split('/').skip(1) {
        if segment.is_empty() || segment == "." || segment == ".." {
            return Err(format!("invalid asset path {path}").into());
        }
    }
    Ok(())
}

fn content_type(path: &str) -> &'static str {
    match path.rsplit_once('.').map(|(_, extension)| extension) {
        Some("ttf") => "font/ttf",
        Some("otf") => "font/otf",
        Some("woff2") => "font/woff2",
        Some("png") => "image/png",
        Some("wasm") => "application/wasm",
        Some("json") => "application/json",
        Some("css") => "text/css",
        Some("js") | Some("mjs") => "text/javascript",
        Some("html") => "text/html",
        _ => "application/octet-stream",
    }
}

enum RangeOutcome {
    Satisfiable(usize, usize),
    Unsatisfiable,
    Ignore,
}

fn parse_range(header: &str, length: usize) -> RangeOutcome {
    let Some(spec) = header.strip_prefix("bytes=") else {
        return RangeOutcome::Ignore;
    };
    if spec.contains(',') {
        return RangeOutcome::Ignore;
    }
    let Some((start, end)) = spec.split_once('-') else {
        return RangeOutcome::Ignore;
    };
    if start.is_empty() {
        let Ok(suffix) = end.parse::<usize>() else {
            return RangeOutcome::Ignore;
        };
        if suffix == 0 || length == 0 {
            return RangeOutcome::Unsatisfiable;
        }
        let suffix = suffix.min(length);
        return RangeOutcome::Satisfiable(length - suffix, length - 1);
    }
    let Ok(start) = start.parse::<usize>() else {
        return RangeOutcome::Ignore;
    };
    let end = if end.is_empty() {
        length.saturating_sub(1)
    } else {
        match end.parse::<usize>() {
            Ok(end) => end,
            Err(_) => return RangeOutcome::Ignore,
        }
    };
    if start >= length || start > end {
        return RangeOutcome::Unsatisfiable;
    }
    RangeOutcome::Satisfiable(start, end.min(length - 1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ranges_are_single_and_exact() {
        assert!(matches!(
            parse_range("bytes=0-3", 10),
            RangeOutcome::Satisfiable(0, 3)
        ));
        assert!(matches!(
            parse_range("bytes=7-", 10),
            RangeOutcome::Satisfiable(7, 9)
        ));
        assert!(matches!(
            parse_range("bytes=-3", 10),
            RangeOutcome::Satisfiable(7, 9)
        ));
        assert!(matches!(
            parse_range("bytes=20-", 10),
            RangeOutcome::Unsatisfiable
        ));
        assert!(matches!(
            parse_range("bytes=0-1,3-4", 10),
            RangeOutcome::Ignore
        ));
    }

    #[test]
    fn asset_paths_reject_traversal() {
        assert!(validate_asset_path("/fonts/NotoSans.ttf").is_ok());
        assert!(validate_asset_path("../fonts/NotoSans.ttf").is_err());
        assert!(validate_asset_path("/fonts/../secret").is_err());
        assert!(validate_asset_path("/fonts//NotoSans.ttf").is_err());
    }

    #[test]
    fn memory_profile_owner_revision_and_canonical_state() {
        let store = MemoryProfile::new("http://fixture.test:8080").expect("profile store");
        let alpha: Vec<u16> = "alpha".encode_utf16().collect();
        let beta: Vec<u16> = "beta".encode_utf16().collect();
        let read = store.load(&alpha).expect("load");
        assert!(read.state_text.contains("fixture.test"));
        assert!(matches!(
            store.load(&beta).unwrap_err().kind(),
            ProfileErrorKind::Conflict
        ));
        let revision_one = profile_state::parse("1").expect("revision");
        let state =
            profile_state::parse(r#"{"localStorage":[],"cookies":[],"Z":1,"a":2}"#).expect("state");
        store
            .save(&alpha, &revision_one, Some(&state))
            .expect("first save");
        assert_eq!(
            store.load(&alpha).expect("reload").state_text,
            r#"{"localStorage":[],"cookies":[],"Z":1,"a":2}"#
        );
        store
            .save(&alpha, &revision_one, Some(&state))
            .expect("lost-reply retry");
        let different = profile_state::parse(r#"{"cookies":[],"localStorage":[]}"#).expect("state");
        assert!(matches!(
            store
                .save(&alpha, &revision_one, Some(&different))
                .unwrap_err()
                .kind(),
            ProfileErrorKind::Conflict
        ));
        let revision_three = profile_state::parse("3").expect("revision");
        assert!(matches!(
            store
                .save(&alpha, &revision_three, Some(&state))
                .unwrap_err()
                .kind(),
            ProfileErrorKind::Conflict
        ));
        assert!(matches!(
            store
                .save(&beta, &revision_one, Some(&state))
                .unwrap_err()
                .kind(),
            ProfileErrorKind::Conflict
        ));
        let array = profile_state::parse("[]").expect("state");
        assert!(matches!(
            store
                .save(&alpha, &profile_state::parse("2").unwrap(), Some(&array))
                .unwrap_err()
                .kind(),
            ProfileErrorKind::InvalidState
        ));
        let unsafe_revision = profile_state::parse("9007199254740992").expect("revision");
        assert!(matches!(
            store
                .save(&alpha, &unsafe_revision, Some(&state))
                .unwrap_err()
                .kind(),
            ProfileErrorKind::Conflict
        ));
        let status = store.status().expect("status");
        assert_eq!(status["loads"], 2);
        assert_eq!(status["saves"], 1);
        assert_eq!(status["revision"], 1);
    }
}
