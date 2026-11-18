// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// workerd 通过 Unix socket 访问的两个私有服务。profile 存储是 T2 的内存测试替身，不是
// SQLite 存储：它保留引擎桥接已覆盖的原始保存 JSON 以及 owner/revision 规则。资源只从
// 显式构建清单提供，并对照清单做哈希校验。

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::header::{ETAG, IF_NONE_MATCH, RANGE};
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use serde_json::value::RawValue;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::net::UnixListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::observe::Observer;
use crate::HostResult;

const MAX_PROFILE_BODY: usize = 70 * 1024 * 1024;
const OBSERVED_STATE_LIMIT: usize = 64 * 1024;
const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

pub struct ServicePaths {
    pub profile: PathBuf,
    pub assets: PathBuf,
}

pub struct PrivateServices {
    shutdown: watch::Sender<bool>,
    profile: Arc<ProfileStore>,
    paths: ServicePaths,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl PrivateServices {
    /// 在 workerd 启动前绑定两个 socket；调用方把它们的路径作为外部地址传入。
    pub fn bind(
        run_dir: &Path,
        assets_root: &Path,
        manifest_path: &Path,
        profile_origin: &str,
        observer: Arc<Observer>,
    ) -> HostResult<Self> {
        let profile_path = run_dir.join("profile.sock");
        let assets_path = run_dir.join("assets.sock");
        remove_socket(&profile_path);
        remove_socket(&assets_path);
        let profile_listener = UnixListener::bind(&profile_path)?;
        let assets_listener = UnixListener::bind(&assets_path)?;

        let profile = Arc::new(ProfileStore::new(profile_origin, observer.clone())?);
        let assets = Arc::new(AssetStore::load(assets_root, manifest_path, observer)?);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let tasks = vec![
            spawn_listener(profile_listener, shutdown_rx.clone(), profile.clone()),
            spawn_listener(assets_listener, shutdown_rx, assets),
        ];
        Ok(Self {
            shutdown,
            profile,
            paths: ServicePaths {
                profile: profile_path,
                assets: assets_path,
            },
            tasks: Mutex::new(tasks),
        })
    }

    pub fn paths(&self) -> &ServicePaths {
        &self.paths
    }

    pub fn profile(&self) -> Arc<ProfileStore> {
        self.profile.clone()
    }

    pub fn stop(&self) {
        let _ = self.shutdown.send(true);
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

fn spawn_listener(
    listener: UnixListener,
    shutdown: watch::Receiver<bool>,
    handler: Arc<dyn ConnectionHandler>,
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
                  tokio::spawn(async move {
                    let service = service_fn(move |request| {
                      let handler = handler.clone();
                      async move { Ok::<_, std::convert::Infallible>(handler.handle(request).await) }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                      .serve_connection(TokioIo::new(stream), service)
                      .await;
                  });
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

async fn body_text(body: Incoming, limit: usize) -> std::result::Result<String, StatusCode> {
    use http_body_util::BodyExt;
    let limited = http_body_util::Limited::new(body, limit);
    match limited.collect().await {
        Ok(collected) => {
            let bytes = collected.to_bytes();
            String::from_utf8(bytes.to_vec()).map_err(|_| StatusCode::BAD_REQUEST)
        }
        Err(_) => Err(StatusCode::PAYLOAD_TOO_LARGE),
    }
}

struct ProfileInner {
    owner: Option<String>,
    revision: u64,
    state: Value,
    state_text: String,
    loads: u64,
    saves: u64,
}

pub struct ProfileStore {
    inner: Mutex<ProfileInner>,
    observer: Arc<Observer>,
}

impl ProfileStore {
    fn new(origin: &str, observer: Arc<Observer>) -> HostResult<Self> {
        let (state, state_text) = initial_state(origin)?;
        Ok(Self {
            inner: Mutex::new(ProfileInner {
                owner: None,
                revision: 0,
                state,
                state_text,
                loads: 0,
                saves: 0,
            }),
            observer,
        })
    }

    pub fn status(&self) -> Value {
        let Ok(inner) = self.inner.lock() else {
            return json!({ "revision": 0, "loads": 0, "saves": 0, "bytes": 0 });
        };
        json!({
          "revision": inner.revision,
          "loads": inner.loads,
          "saves": inner.saves,
          "bytes": inner.state_text.len(),
        })
    }

    fn load(&self, body: &str) -> Response<Full<Bytes>> {
        #[derive(Deserialize)]
        struct LoadRequest {
            owner: String,
        }
        let Ok(request) = serde_json::from_str::<LoadRequest>(body) else {
            return json_response(StatusCode::BAD_REQUEST, r#"{"error":"invalid body"}"#);
        };
        if request.owner.is_empty() || request.owner.len() > 128 {
            return json_response(StatusCode::BAD_REQUEST, r#"{"error":"invalid owner"}"#);
        }
        let Ok(mut inner) = self.inner.lock() else {
            return json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                r#"{"error":"profile lock"}"#,
            );
        };
        if let Some(owner) = &inner.owner {
            if owner != &request.owner {
                return json_response(
                    StatusCode::CONFLICT,
                    r#"{"error":"profile owned elsewhere"}"#,
                );
            }
        } else {
            inner.owner = Some(request.owner.clone());
        }
        inner.loads += 1;
        let revision = inner.revision;
        let state_text = inner.state_text.clone();
        let state = inner.state.clone();
        let loads = inner.loads;
        drop(inner);
        self.observer.with(
            "profile-load",
            json!({
              "kind": "profile-load",
              "owner": request.owner,
              "revision": revision,
              "loads": loads,
              "state": state,
            }),
        );
        json_response(
            StatusCode::OK,
            format!("{{\"state\":{state_text},\"revision\":{revision}}}"),
        )
    }

    fn save(&self, body: &str) -> Response<Full<Bytes>> {
        #[derive(Deserialize)]
        struct SaveRequest<'a> {
            owner: String,
            revision: u64,
            #[serde(borrow)]
            state: &'a RawValue,
        }
        let Ok(request) = serde_json::from_str::<SaveRequest>(body) else {
            return json_response(StatusCode::BAD_REQUEST, r#"{"error":"invalid body"}"#);
        };
        if request.owner.is_empty() || request.owner.len() > 128 {
            return json_response(StatusCode::BAD_REQUEST, r#"{"error":"invalid owner"}"#);
        }
        if request.revision > MAX_SAFE_INTEGER {
            return json_response(StatusCode::BAD_REQUEST, r#"{"error":"unsafe revision"}"#);
        }
        let raw = request.state.get();
        let Ok(parsed) = serde_json::from_str::<Value>(raw) else {
            return json_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                r#"{"error":"INVALID_STATE"}"#,
            );
        };
        if !parsed.is_object() {
            return json_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                r#"{"error":"INVALID_STATE"}"#,
            );
        }
        let Ok(mut inner) = self.inner.lock() else {
            return json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                r#"{"error":"profile lock"}"#,
            );
        };
        if inner.owner.as_deref() != Some(request.owner.as_str()) {
            return json_response(
                StatusCode::CONFLICT,
                r#"{"error":"profile owner mismatch"}"#,
            );
        }
        if request.revision == inner.revision {
            if raw == inner.state_text {
                return json_response(
                    StatusCode::OK,
                    format!("{{\"revision\":{},\"idempotent\":true}}", request.revision),
                );
            }
            return json_response(
                StatusCode::CONFLICT,
                r#"{"error":"revision already committed"}"#,
            );
        }
        if request.revision != inner.revision + 1 {
            return json_response(StatusCode::CONFLICT, r#"{"error":"revision out of order"}"#);
        }
        inner.revision = request.revision;
        inner.state = parsed;
        inner.state_text = raw.to_string();
        inner.saves += 1;
        let revision = inner.revision;
        let saves = inner.saves;
        let bytes = inner.state_text.len();
        drop(inner);
        let observed = if raw.len() <= OBSERVED_STATE_LIMIT {
            json!({ "stateRaw": raw })
        } else {
            json!({ "stateBytes": bytes, "truncated": true })
        };
        let mut event = observed;
        if let Value::Object(map) = &mut event {
            map.insert("kind".to_string(), json!("profile-save"));
            map.insert("owner".to_string(), json!(request.owner));
            map.insert("revision".to_string(), json!(revision));
            map.insert("saves".to_string(), json!(saves));
        }
        self.observer.record(event);
        json_response(StatusCode::OK, format!("{{\"revision\":{revision}}}"))
    }
}

impl ConnectionHandler for ProfileStore {
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
            let body = match body_text(request.into_body(), MAX_PROFILE_BODY).await {
                Ok(body) => body,
                Err(status) => return empty_response(status),
            };
            if path == "/load" {
                self.load(&body)
            } else {
                self.save(&body)
            }
        })
    }
}

fn initial_state(origin: &str) -> HostResult<(Value, String)> {
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
    Ok((state, text))
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
    fn profile_owner_revision_and_raw_state() {
        let store = ProfileStore::new("http://fixture.test:8080", Arc::new(Observer::disabled()))
            .expect("profile store");
        let state = store.inner.lock().expect("profile").state_text.clone();
        assert!(state.contains("fixture.test"));
        assert_eq!(store.load(r#"{"owner":"alpha"}"#).status(), StatusCode::OK);
        assert_eq!(
            store.load(r#"{"owner":"beta"}"#).status(),
            StatusCode::CONFLICT
        );
        assert_eq!(store.load("{}").status(), StatusCode::BAD_REQUEST);
        let save = r#"{"owner":"alpha","revision":1,"state":{"Z":1,"a":2}}"#;
        assert_eq!(store.save(save).status(), StatusCode::OK);
        let raw = store.inner.lock().expect("profile").state_text.clone();
        assert_eq!(raw, r#"{"Z":1,"a":2}"#);
        assert_eq!(store.save(save).status(), StatusCode::OK);
        assert_eq!(
            store
                .save(r#"{"owner":"alpha","revision":1,"state":{"a":2}}"#)
                .status(),
            StatusCode::CONFLICT
        );
        assert_eq!(
            store
                .save(r#"{"owner":"alpha","revision":3,"state":{}}"#)
                .status(),
            StatusCode::CONFLICT
        );
        assert_eq!(
            store
                .save(r#"{"owner":"beta","revision":2,"state":{}}"#)
                .status(),
            StatusCode::CONFLICT
        );
        assert_eq!(
            store
                .save(r#"{"owner":"alpha","revision":2,"state":[]}"#)
                .status(),
            StatusCode::UNPROCESSABLE_ENTITY
        );
        assert_eq!(
            store
                .save(r#"{"owner":"alpha","revision":9007199254740992,"state":{}}"#)
                .status(),
            StatusCode::BAD_REQUEST
        );
        let status = store.status();
        assert_eq!(status["loads"], 1);
        assert_eq!(status["saves"], 1);
        assert_eq!(status["revision"], 1);
    }
}
