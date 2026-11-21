// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// 公共 loopback 入口只做协议与预算校验：HTTP 管理请求转交 broker，WS 帧交给 broker
// 的有界通道。权限、请求 ID 映射、attachment 归属和生命周期全部由 broker actor 决定；
// 本文件不广播上游事件，也不自行授予 controller。

use std::collections::HashMap;
use std::convert::Infallible;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http_body_util::BodyExt;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::header::{CONNECTION, HOST, ORIGIN, UPGRADE};
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, Role as WsRole};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

use crate::broker::{
    BrokerHandle, CloseKind, ClientCommand, Endpoint, ReserveRequest, Reservation, Role,
};
use crate::observe::Observer;
use crate::services::ProfileService;
use crate::transport::{
    websocket_config, BudgetedSocket, LeasedMessage, PayloadLease, CODEC_BYTES, READ_BUFFER_BYTES,
};
use crate::HostResult;

const MAX_CONNECTIONS: usize = 128;
const MAX_BODY_BYTES: usize = 64 * 1024;
const MAX_BACKUP_PATH_BYTES: usize = 4096;
const CLIENT_QUEUE: usize = 64;
const WRITE_TIMEOUT: Duration = Duration::from_secs(10);
const BODY_DEADLINE: Duration = Duration::from_secs(5);
const UPGRADE_DEADLINE: Duration = Duration::from_secs(10);
/// 关闭时先给已经生成的 HTTP 应答一个写出窗口，再中止长连接任务。
const SHUTDOWN_GRACE: Duration = Duration::from_secs(1);

#[derive(Clone)]
pub struct SessionInfo {
    pub session_id: crate::cdp_raw::LeasedIdentity,
    pub build_version: crate::cdp_raw::LeasedIdentity,
    pub environment: crate::cdp_raw::LeasedIdentity,
}

impl SessionInfo {
    pub fn from_raw(
        raw: &crate::cdp_raw::LeasedRaw,
        budget: &crate::transport::PayloadBudget,
        version: &str,
    ) -> HostResult<Self> {
        let mut scratch = budget.json_scratch();
        let mut field = |name, fallback: &str| -> HostResult<crate::cdp_raw::LeasedIdentity> {
            if let Some(node) = raw.node().field(name, &mut scratch) {
                if node.text()?.starts_with('"') {
                    return Ok(crate::cdp_raw::decode_text(
                        node.raw(&mut scratch)?,
                        budget,
                        crate::transport::MAX_FRAME_BYTES,
                        &mut scratch,
                    )?);
                }
            }
            Ok(crate::cdp_raw::LeasedIdentity::literal(fallback, budget)?)
        };
        Ok(Self {
            session_id: field("sessionId", "")?,
            build_version: field("buildVersion", version)?,
            environment: field("environment", "local")?,
        })
    }
}

struct HostContext {
    broker: BrokerHandle,
    session: SessionInfo,
    generation: String,
    recovery: String,
    profile: Arc<ProfileService>,
    port: u16,
    active: AtomicUsize,
    connections: Mutex<JoinSet<()>>,
}

impl HostContext {
    fn spawn(&self, task: impl std::future::Future<Output = ()> + Send + 'static) {
        if let Ok(mut connections) = self.connections.lock() {
            // 完成的任务立即回收，避免长期会话中 JoinSet 只增不减。
            while connections.try_join_next().is_some() {}
            connections.spawn(task);
        }
    }

    fn take_connections(&self) -> JoinSet<()> {
        self.connections
            .lock()
            .map(|mut connections| std::mem::take(&mut *connections))
            .unwrap_or_default()
    }
}

pub struct HostServer {
    port: u16,
    shutdown: watch::Sender<bool>,
    task: Mutex<Option<JoinHandle<()>>>,
    context: Arc<HostContext>,
}

impl HostServer {
    /// 绑定公共 loopback 监听器；端口 0 在此解析，并在 ready 前上报。
    #[allow(
        clippy::too_many_arguments,
        reason = "host wiring passes the fixed startup state"
    )]
    pub fn bind(
        port: u16,
        broker: BrokerHandle,
        session: SessionInfo,
        generation: String,
        recovery: String,
        profile: Arc<ProfileService>,
        observer: Arc<Observer>,
    ) -> HostResult<Self> {
        let listener = std::net::TcpListener::bind(("127.0.0.1", port))?;
        listener.set_nonblocking(true)?;
        let listener = TcpListener::from_std(listener)?;
        let port = listener.local_addr()?.port();
        let context = Arc::new(HostContext {
            broker,
            session,
            generation,
            recovery,
            profile,
            port,
            active: AtomicUsize::new(0),
            connections: Mutex::new(JoinSet::new()),
        });
        let (shutdown, mut shutdown_rx) = watch::channel(false);
        let loop_context = context.clone();
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                  changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                      break;
                    }
                  }
                  _ = connections.join_next(), if !connections.is_empty() => {}
                  accepted = listener.accept() => match accepted {
                    Ok((stream, _)) => {
                      let active = loop_context.active.fetch_add(1, Ordering::SeqCst);
                      if active >= MAX_CONNECTIONS {
                        loop_context.active.fetch_sub(1, Ordering::SeqCst);
                        continue;
                      }
                      let Some(http_lease) = loop_context.broker.payload_budget().try_acquire(READ_BUFFER_BYTES, true) else {
                        loop_context.active.fetch_sub(1, Ordering::SeqCst);
                        continue;
                      };
                      let http_lease = Arc::new(http_lease);
                      let context = loop_context.clone();
                      let service_context = context.clone();
                      let client_shutdown = shutdown_rx.clone();
                      connections.spawn(async move {
                        let service = service_fn(move |request| {
                          let context = service_context.clone();
                          let shutdown = client_shutdown.clone();
                          let http_lease = http_lease.clone();
                          async move {
                            let mut request = request;
                            request.extensions_mut().insert(http_lease);
                            Ok::<_, Infallible>(handle_request(request, context, shutdown).await)
                          }
                        });
                        let _ = hyper::server::conn::http1::Builder::new()
                          .max_buf_size(READ_BUFFER_BYTES)
                          .serve_connection(TokioIo::new(stream), service)
                          .with_upgrades()
                          .await;
                        context.active.fetch_sub(1, Ordering::SeqCst);
                      });
                    }
                    Err(_) => break,
                  }
                }
            }
            // 在途应答（尤其 /close 成功响应）先写完；keep-alive 连接随后统一中止。
            let _ = timeout(SHUTDOWN_GRACE, async {
                while connections.join_next().await.is_some() {}
            })
            .await;
            connections.abort_all();
            while connections.join_next().await.is_some() {}
        });
        observer.with(
            "public-ready",
            json!({ "kind": "public-ready", "port": port }),
        );
        Ok(Self {
            port,
            shutdown,
            task: Mutex::new(Some(task)),
            context,
        })
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    /// 停止接受连接并等待本宿主持有的 HTTP/Upgrade 任务结束。
    pub async fn stop(&self) {
        let _ = self.shutdown.send(true);
        let task = self.task.lock().ok().and_then(|mut guard| guard.take());
        if let Some(task) = task {
            let _ = timeout(Duration::from_secs(2), task).await;
        }
        let mut connections = self.context.take_connections();
        connections.abort_all();
        let _ = timeout(Duration::from_secs(2), async {
            while connections.join_next().await.is_some() {}
        })
        .await;
    }
}

fn json_response(status: StatusCode, body: impl Into<Bytes>) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .header("cache-control", "no-store")
        .body(Full::new(body.into()))
        .expect("json response")
}

fn error_response(status: StatusCode, message: &str) -> Response<Full<Bytes>> {
    json_response(status, json!({ "error": message }).to_string())
}

fn allowed_origin(request: &Request<Incoming>, port: u16) -> bool {
    let hosts = request.headers().get_all(HOST).iter().count();
    if hosts != 1 {
        return false;
    }
    let host = request
        .headers()
        .get(HOST)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    if host != format!("127.0.0.1:{port}") && host != format!("localhost:{port}") {
        return false;
    }
    let origins = request.headers().get_all(ORIGIN).iter().count();
    if origins > 1 {
        return false;
    }
    let origin = request
        .headers()
        .get(ORIGIN)
        .and_then(|value| value.to_str().ok());
    match origin {
        None => true,
        Some(origin) => {
            origin == format!("http://{host}")
                || origin == "devtools://devtools"
                || origin == "https://chrome-devtools-frontend.appspot.com"
        }
    }
}

fn upgrade_requested(request: &Request<Incoming>) -> bool {
    request
        .headers()
        .get(UPGRADE)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.eq_ignore_ascii_case("websocket"))
        .unwrap_or(false)
}

async fn handle_request(
    request: Request<Incoming>,
    context: Arc<HostContext>,
    shutdown: watch::Receiver<bool>,
) -> Response<Full<Bytes>> {
    if !allowed_origin(&request, context.port) {
        return error_response(StatusCode::FORBIDDEN, "Local browser origin rejected");
    }
    if upgrade_requested(&request) {
        return upgrade(request, context, shutdown).await;
    }
    let path = request.uri().path().to_string();
    let method = request.method().clone();
    let normalized = if path == "/" {
        "/"
    } else {
        path.trim_end_matches('/')
    };
    match (method, normalized) {
        (Method::GET, "/health") => {
            let alive = context
                .broker
                .session()
                .await
                .is_some_and(|session| session.alive);
            json_response(
                StatusCode::OK,
                json!({ "alive": alive, "version": context.session.build_version }).to_string(),
            )
        }
        (Method::GET, "/session") => session_body(&context).await,
        (Method::GET, "/json/version") => json_response(
            StatusCode::OK,
            json!({
              "Browser": format!("Kitesurf/{}", context.session.build_version),
              "Protocol-Version": "1.3",
              "webSocketDebuggerUrl": format!("ws://127.0.0.1:{}/devtools/browser", context.port),
            })
            .to_string(),
        ),
        (Method::GET, "/json") | (Method::GET, "/json/list") => {
            let query = match parse_query(request.uri().query()) {
                Ok(query) => query,
                Err(message) => return error_response(StatusCode::BAD_REQUEST, &message),
            };
            targets_body(&context, &query).await
        }
        (Method::POST, "/flush") => match context.broker.flush().await {
            Ok(status) => match budgeted_body(&context.broker.payload_budget(), &status) {
                Some(body) => json_response(StatusCode::OK, body),
                None => error_response(StatusCode::CONFLICT, "Profile status unavailable"),
            },
            Err(message) => error_response(StatusCode::CONFLICT, &message),
        },
        (Method::POST, "/control") => control(&context, request).await,
        (Method::POST, "/close") => match context
            .broker
            .close(CloseKind::Http, None, None, None)
            .await
        {
            Ok(()) => json_response(StatusCode::OK, r#"{"closed":true}"#),
            Err(message) => error_response(StatusCode::CONFLICT, &message),
        },
        (Method::POST, "/backup") => backup(&context, request).await,
        (Method::GET, "/") => json_response(
            StatusCode::OK,
            json!({
              "browser": format!("Kitesurf {}", context.session.build_version),
              "session": "/session",
              "cdp": format!("ws://127.0.0.1:{}/devtools/browser", context.port),
              "inspect": "chrome://inspect — configure this address",
              "profile": context.profile.display(),
            })
            .to_string(),
        ),
        _ => error_response(StatusCode::NOT_FOUND, "Not found"),
    }
}

async fn backup(context: &HostContext, request: Request<Incoming>) -> Response<Full<Bytes>> {
    let body = match json_body(request).await {
        Ok(body) => body,
        Err(message) => return error_response(StatusCode::BAD_REQUEST, &message),
    };
    let Some(path) = body.get("path").and_then(Value::as_str).filter(|path| {
        !path.is_empty()
            && path.len() <= MAX_BACKUP_PATH_BYTES
            && !path.contains('\0')
            && Path::new(path).file_name().is_some()
    }) else {
        return error_response(StatusCode::BAD_REQUEST, "A backup output path is required");
    };
    let Ok(destination) =
        crate::cdp_raw::LeasedIdentity::literal(path, &context.broker.payload_budget())
    else {
        return error_response(StatusCode::SERVICE_UNAVAILABLE, "Payload budget exhausted");
    };
    drop(body);
    match context.broker.backup(destination).await {
        Ok(path) => budgeted_json(context, &json!({ "path": path })),
        Err(message) => error_response(StatusCode::CONFLICT, &message),
    }
}

async fn control(context: &HostContext, request: Request<Incoming>) -> Response<Full<Bytes>> {
    let body = match json_body(request).await {
        Ok(body) => body,
        Err(message) => return error_response(StatusCode::CONFLICT, &message),
    };
    let Some(owner) = body.get("owner").and_then(Value::as_str) else {
        return error_response(StatusCode::CONFLICT, "A short owner name is required");
    };
    if owner.is_empty() || owner.encode_utf16().count() > 128 {
        return error_response(StatusCode::CONFLICT, "A short owner name is required");
    }
    let takeover = body
        .get("takeover")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    match context.broker.control(owner.to_string(), takeover).await {
        Ok((controller, epoch)) => json_response(
            StatusCode::OK,
            json!({ "controller": controller, "controlEpoch": epoch }).to_string(),
        ),
        Err(error) => {
            let mut value = json!({ "error": error.message() });
            if let Some(controller) = error.controller() {
                value["controller"] = json!(controller);
            }
            json_response(StatusCode::CONFLICT, value.to_string())
        }
    }
}

async fn json_body(request: Request<Incoming>) -> Result<Value, String> {
    let collected = timeout(BODY_DEADLINE, async {
        let mut body = request.into_body();
        let mut data = Vec::new();
        while let Some(frame) = body.frame().await {
            let frame = frame.map_err(|_| "Request body is invalid".to_string())?;
            let Ok(chunk) = frame.into_data() else {
                continue;
            };
            if data.len().saturating_add(chunk.len()) > MAX_BODY_BYTES {
                return Err("Request too large".to_string());
            }
            data.extend_from_slice(&chunk);
        }
        Ok::<Vec<u8>, String>(data)
    })
    .await
    .map_err(|_| "Request body timed out".to_string())??;
    if collected.is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_slice(&collected).map_err(|_| "Invalid JSON body".to_string())
}

fn budgeted_json<T: serde::Serialize>(context: &HostContext, value: &T) -> Response<Full<Bytes>> {
    let Some(body) = budgeted_body(&context.broker.payload_budget(), value) else {
        return error_response(StatusCode::SERVICE_UNAVAILABLE, "Payload budget exhausted");
    };
    json_response(StatusCode::OK, body)
}

pub(crate) fn budgeted_body<T: serde::Serialize>(
    budget: &crate::transport::PayloadBudget,
    value: &T,
) -> Option<Bytes> {
    struct Body {
        text: String,
        _lease: PayloadLease,
        _metadata: PayloadLease,
    }
    impl AsRef<[u8]> for Body {
        fn as_ref(&self) -> &[u8] {
            self.text.as_bytes()
        }
    }
    // bytes 的 owner 只有固定记录、引用计数和对齐；与输出 backing 一起留到最后一个 clone。
    let metadata = budget.try_acquire(
        std::mem::size_of::<Body>() + 2 * std::mem::size_of::<usize>(),
        false,
    )?;
    let (text, lease) = crate::transport::serialize_payload(budget, value, false)?;
    if text.capacity() > lease.bytes() {
        return None;
    }
    Some(Bytes::from_owner(Body {
        text,
        _lease: lease,
        _metadata: metadata,
    }))
}

#[cfg(all(test, feature = "test-faults"))]
pub(crate) async fn test_session_body(
    broker: BrokerHandle,
    profile: Arc<ProfileService>,
) -> Response<Full<Bytes>> {
    let raw = crate::transport::LeasedRaw::literal("{}", &broker.payload_budget()).unwrap();
    let session = SessionInfo::from_raw(&raw, &broker.payload_budget(), "test").unwrap();
    let context = HostContext {
        broker,
        profile,
        session,
        generation: "test".into(),
        recovery: "test".into(),
        port: 1,
        active: AtomicUsize::new(0),
        connections: Mutex::new(JoinSet::new()),
    };
    session_body(&context).await
}

async fn session_body(context: &HostContext) -> Response<Full<Bytes>> {
    let Some(snapshot) = context.broker.session().await else {
        return error_response(StatusCode::CONFLICT, "Browser session is lost");
    };
    let tabs = match context.broker.targets().await {
        Ok(tabs) => tabs,
        Err(message) => return error_response(StatusCode::CONFLICT, &message),
    };
    let profile = match context
        .profile
        .leased_status(context.broker.payload_budget())
        .await
    {
        Ok(status) => status,
        Err(_) => return error_response(StatusCode::CONFLICT, "Profile status unavailable"),
    };
    #[derive(serde::Serialize)]
    #[serde(rename_all = "camelCase")]
    struct SessionBody<'a, T> {
        version: &'a str,
        session_id: &'a str,
        generation: &'a str,
        alive: bool,
        recovery: &'a str,
        profile: &'a crate::services::LeasedProfileStatus,
        controller: &'a Option<String>,
        control_epoch: u64,
        tabs: &'a T,
    }
    let budget = context.broker.payload_budget();
    let mut scratch = budget.json_scratch();
    let tabs_view = TargetsView {
        tabs: &tabs,
        scratch: std::cell::RefCell::new(&mut scratch),
        port: None,
    };
    budgeted_json(
        context,
        &SessionBody {
            version: &context.session.build_version,
            session_id: &context.session.session_id,
            generation: &context.generation,
            alive: snapshot.alive,
            recovery: &context.recovery,
            profile: &profile,
            controller: &snapshot.controller,
            control_epoch: snapshot.control_epoch,
            tabs: &tabs_view,
        },
    )
}

async fn targets_body(context: &HostContext, query: &Query) -> Response<Full<Bytes>> {
    let tabs = match &query.page {
        Some(page) => context.broker.targets_for_page(page.clone()).await,
        None => context.broker.targets().await,
    };
    let tabs = match tabs {
        Ok(tabs) => tabs,
        Err(message) => return error_response(StatusCode::CONFLICT, &message),
    };
    let budget = context.broker.payload_budget();
    let mut scratch = budget.json_scratch();
    let view = TargetsView {
        tabs: &tabs,
        scratch: std::cell::RefCell::new(&mut scratch),
        port: Some(context.port),
    };
    budgeted_json(context, &view)
}

struct TargetsView<'a, 'b> {
    tabs: &'a crate::broker::LeasedTargets,
    scratch: std::cell::RefCell<&'a mut crate::transport::JsonScratchGuard<'b>>,
    port: Option<u16>,
}

struct TargetUrl<'a> {
    id: crate::cdp_raw::RawText<'a>,
    port: u16,
    inspector: bool,
}
impl std::fmt::Display for TargetUrl<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.inspector {
            write!(
                f,
                "devtools://devtools/bundled/inspector.html?ws=127.0.0.1:{}/devtools/page/{}",
                self.port, self.id
            )
        } else {
            write!(f, "ws://127.0.0.1:{}/devtools/page/{}", self.port, self.id)
        }
    }
}
impl serde::Serialize for TargetUrl<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl serde::Serialize for TargetsView<'_, '_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::{Error, SerializeSeq};
        let mut scratch = self.scratch.borrow_mut();
        let mut array = self
            .tabs
            .raw
            .node()
            .array(&mut scratch)
            .ok_or_else(|| S::Error::custom("invalid targets"))?;
        let mut seq = serializer.serialize_seq(None)?;
        while let Some(target) = array.next(&mut scratch).map_err(S::Error::custom)? {
            if !target
                .field("type", &mut scratch)
                .is_some_and(|node| node.equals("page", &mut scratch))
            {
                continue;
            }
            let Some(port) = self.port else {
                seq.serialize_element(target.raw(&mut scratch).map_err(S::Error::custom)?)?;
                continue;
            };
            serialize_target(&mut seq, target, port, &mut scratch)?;
        }
        seq.end()
    }
}

fn serialize_target<S: serde::ser::SerializeSeq>(
    seq: &mut S,
    target: crate::cdp_raw::RawNode<'_>,
    port: u16,
    scratch: &mut crate::transport::JsonScratchGuard<'_>,
) -> Result<(), S::Error> {
    use serde::ser::Error;
    #[derive(serde::Serialize)]
    struct Entry<'a> {
        id: crate::cdp_raw::RawText<'a>,
        #[serde(rename = "type")]
        kind: &'static str,
        title: Option<&'a serde_json::value::RawValue>,
        url: Option<&'a serde_json::value::RawValue>,
        #[serde(rename = "webSocketDebuggerUrl")]
        websocket: TargetUrl<'a>,
        #[serde(rename = "devtoolsFrontendUrl")]
        devtools: TargetUrl<'a>,
    }
    let id_node = target.field("targetId", scratch).unwrap_or(target);
    let title = target
        .field("title", scratch)
        .map(|node| node.raw(scratch))
        .transpose()
        .map_err(S::Error::custom)?;
    let url = target
        .field("url", scratch)
        .map(|node| node.raw(scratch))
        .transpose()
        .map_err(S::Error::custom)?;
    seq.serialize_element(&Entry {
        id: id_node.decoded(),
        kind: "page",
        title,
        url,
        websocket: TargetUrl {
            id: id_node.decoded(),
            port,
            inspector: false,
        },
        devtools: TargetUrl {
            id: id_node.decoded(),
            port,
            inspector: true,
        },
    })
}

#[cfg(all(test, feature = "test-faults"))]
mod raw_output_tests {
    use super::*;
    use crate::cdp_raw::LeasedRaw;
    use crate::transport::{PayloadBudget, MAX_FRAME_BYTES};

    #[tokio::test]
    async fn typed_profile_body_cancel_writer_failure_and_budget_release() {
        use crate::services::{MemoryProfile, ProfileBackend, SqliteProfile};
        use crate::transport::ORDINARY_BYTES;
        use tokio::io::AsyncWriteExt;
        let budget = PayloadBudget::default();
        let directory = tempfile::tempdir().unwrap();
        let memory = MemoryProfile::new("http://fixture.invalid").unwrap();
        let sqlite = SqliteProfile::open(&directory.path().join("状态.db"), true).unwrap();
        for backend in [
            &memory as &dyn ProfileBackend,
            &sqlite as &dyn ProfileBackend,
        ] {
            let status = backend.leased_status(&budget).unwrap();
            let baseline = budget.used();
            let held = budget
                .try_acquire(ORDINARY_BYTES - baseline.0, false)
                .unwrap();
            assert!(budgeted_body(&budget, &status).is_none());
            drop(held);
            assert_eq!(budget.used(), baseline);
            let (tx, rx) = tokio::sync::oneshot::channel();
            tx.send(status.clone()).ok().unwrap();
            drop(rx);
            assert_eq!(budget.used(), baseline);
            let bytes = budgeted_body(&budget, &status).unwrap();
            let body = Full::new(bytes);
            drop(status);
            assert!(budget.used().0 > MAX_FRAME_BYTES);
            let writer_held = body.collect().await.unwrap().to_bytes();
            let (mut writer, reader) = tokio::io::duplex(1);
            drop(reader);
            assert!(writer.write_all(&writer_held).await.is_err());
            assert!(budget.used().0 >= MAX_FRAME_BYTES + writer_held.len());
            drop(writer_held);
            assert_eq!(budget.used(), (MAX_FRAME_BYTES, 0));
            let status = backend.leased_status(&budget).unwrap();
            let body = Full::new(budgeted_body(&budget, &status).unwrap());
            drop(status);
            drop(body);
            assert_eq!(budget.used(), (MAX_FRAME_BYTES, 0));
        }
    }

    #[test]
    fn typed_body_serialization_failure_releases_provisional_output() {
        use std::cell::Cell;
        struct FailsOnWrite(Cell<bool>);
        impl serde::Serialize for FailsOnWrite {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                if self.0.replace(true) {
                    return Err(serde::ser::Error::custom("synthetic serialization failure"));
                }
                serializer.serialize_str("fixture")
            }
        }
        let budget = PayloadBudget::default();
        assert!(budgeted_body(&budget, &FailsOnWrite(Cell::new(false))).is_none());
        assert_eq!(budget.used(), (MAX_FRAME_BYTES, 0));
    }

    #[test]
    fn session_fields_drop_unknown_raw_and_lease_long_decoded_text() {
        let budget = PayloadBudget::default();
        let long = "x".repeat(8192);
        let text = format!(
            r#"{{"sessionId":"{long}","environment":null,"unknown":"{}"}}"#,
            "z".repeat(128 * 1024)
        );
        let raw = LeasedRaw::literal(&text, &budget).unwrap();
        let session = SessionInfo::from_raw(&raw, &budget, "fallback").unwrap();
        drop(raw);
        assert_eq!(session.session_id.as_str(), long);
        assert_eq!(session.build_version.as_str(), "fallback");
        assert_eq!(session.environment.as_str(), "local");
        assert!(budget.used().0 < MAX_FRAME_BYTES + 16 * 1024);
        drop(session);
        assert_eq!(budget.used(), (MAX_FRAME_BYTES, 0));
    }

    #[test]
    fn targets_stream_preserves_last_wins_null_and_long_escaped_text() {
        let budget = PayloadBudget::default();
        let title = "\\u0061".repeat(20_000);
        let text = format!(
            r#"[{{"type":"worker","targetId":"skip"}},{{"type":"worker","\u0074ype":"page","targetId":"old","target\u0049d":"t\u0061b","title":"{title}","url":null}},{{"type":"page"}}]"#
        );
        let tabs = crate::broker::LeasedTargets {
            raw: LeasedRaw::literal(&text, &budget).unwrap(),
        };
        for port in [None, Some(9222)] {
            let output = {
                let mut scratch = budget.json_scratch();
                let view = TargetsView {
                    tabs: &tabs,
                    scratch: std::cell::RefCell::new(&mut scratch),
                    port,
                };
                crate::transport::serialize_payload(&budget, &view, false).unwrap()
            };
            let parsed: Value = serde_json::from_str(&output.0).unwrap();
            assert_eq!(parsed.as_array().unwrap().len(), 2);
            assert_eq!(parsed[0]["title"].as_str().unwrap().len(), 20_000);
            assert!(parsed[0]["url"].is_null());
            if port.is_some() {
                assert_eq!(parsed[0]["id"], "tab");
                assert_eq!(
                    parsed[0]["webSocketDebuggerUrl"],
                    "ws://127.0.0.1:9222/devtools/page/tab"
                );
                assert_eq!(parsed[1]["id"], "");
                assert!(parsed[1]["title"].is_null());
                assert!(parsed[1]["url"].is_null());
            }
            drop(output);
        }
        drop(tabs);
        assert_eq!(budget.used(), (MAX_FRAME_BYTES, 0));
    }
}

struct Query {
    session: Option<String>,
    role: Option<String>,
    owner: Option<String>,
    /// `/json/list?page=<targetId>` 把发现范围限定到该 page session。
    page: Option<String>,
}

fn parse_query(query: Option<&str>) -> Result<Query, String> {
    let mut values: HashMap<String, String> = HashMap::new();
    for pair in query
        .unwrap_or("")
        .split('&')
        .filter(|pair| !pair.is_empty())
    {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        if values.insert(key.to_string(), value.to_string()).is_some() {
            return Err("Duplicate query parameter".to_string());
        }
    }
    Ok(Query {
        session: values.remove("session"),
        role: values.remove("role"),
        owner: values.remove("owner"),
        page: values.remove("page"),
    })
}

fn parse_endpoint(path: &str) -> Option<Endpoint> {
    if path == "/devtools/browser" {
        return Some(Endpoint::Browser);
    }
    let target = path.strip_prefix("/devtools/page/")?;
    if target.is_empty() || target.contains('/') {
        return None;
    }
    Some(Endpoint::Page {
        target_id: target.to_string(),
    })
}

async fn upgrade(
    mut request: Request<Incoming>,
    context: Arc<HostContext>,
    shutdown: watch::Receiver<bool>,
) -> Response<Full<Bytes>> {
    let Some(endpoint) = parse_endpoint(request.uri().path()) else {
        return error_response(StatusCode::CONFLICT, "Browser unavailable");
    };
    let query = match parse_query(request.uri().query()) {
        Ok(query) => query,
        Err(message) => return error_response(StatusCode::CONFLICT, &message),
    };
    let Some(snapshot) = context.broker.session().await else {
        return error_response(StatusCode::CONFLICT, "Browser unavailable");
    };
    if !snapshot.alive {
        return error_response(StatusCode::CONFLICT, "Browser unavailable");
    }
    if let Some(session) = &query.session {
        if session.as_str() != context.session.session_id.as_str() {
            return error_response(StatusCode::CONFLICT, "Session expired");
        }
    }
    let role = if query.role.as_deref() == Some("viewer") {
        Role::Viewer
    } else {
        Role::Controller
    };
    let owner = query.owner.unwrap_or_else(|| "default".to_string());
    if owner.is_empty() || owner.encode_utf16().count() > 128 {
        return error_response(StatusCode::CONFLICT, "Owner name is too long");
    }
    let reservation = match context
        .broker
        .reserve(ReserveRequest {
            endpoint,
            role,
            owner,
        })
        .await
    {
        Ok(reservation) => reservation,
        Err(message) => return error_response(StatusCode::CONFLICT, &message),
    };
    let reservation_id = reservation.id();
    let version = request
        .headers()
        .get("sec-websocket-version")
        .and_then(|value| value.to_str().ok());
    if version != Some("13") {
        context.broker.rollback(reservation_id);
        return Response::builder()
            .status(StatusCode::UPGRADE_REQUIRED)
            .body(Full::new(Bytes::new()))
            .expect("upgrade required response");
    }
    let Some(key) = request
        .headers()
        .get("sec-websocket-key")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
    else {
        context.broker.rollback(reservation_id);
        return Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(Full::new(Bytes::new()))
            .expect("bad request response");
    };
    let Some(codec) = context
        .broker
        .payload_budget()
        .try_acquire(CODEC_BYTES, true)
    else {
        context.broker.rollback(reservation_id);
        return error_response(StatusCode::SERVICE_UNAVAILABLE, "Codec budget exhausted");
    };
    let http_lease = request.extensions().get::<Arc<PayloadLease>>().cloned();
    let accept = tokio_tungstenite::tungstenite::handshake::derive_accept_key(key.as_bytes());
    let upgrade = hyper::upgrade::on(&mut request);
    let client_context = context.clone();
    context.spawn(async move {
        let upgraded = match timeout(UPGRADE_DEADLINE, upgrade).await {
            Ok(Ok(upgraded)) => upgraded,
            _ => {
                client_context.broker.rollback(reservation_id);
                return;
            }
        };
        let config = websocket_config();
        let socket =
            WebSocketStream::from_raw_socket(TokioIo::new(upgraded), WsRole::Server, Some(config))
                .await;
        let socket =
            BudgetedSocket::new(socket, client_context.broker.payload_budget(), codec, false);
        let _http_lease = http_lease;
        run_client(socket, client_context, shutdown, reservation).await;
    });
    Response::builder()
        .status(StatusCode::SWITCHING_PROTOCOLS)
        .header(UPGRADE, "websocket")
        .header(CONNECTION, "Upgrade")
        .header("sec-websocket-accept", accept)
        .body(Full::new(Bytes::new()))
        .expect("upgrade response")
}

async fn run_client<S>(
    socket: BudgetedSocket<S>,
    context: Arc<HostContext>,
    mut shutdown: watch::Receiver<bool>,
    reservation: Reservation,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let client = reservation.id();
    let (mut sink, mut stream) = socket.split();
    let (writer, mut commands) = mpsc::channel::<ClientCommand>(CLIENT_QUEUE);
    let (cancel_tx, cancel) = watch::channel(false);
    if !context.broker.commit(reservation, writer, cancel_tx).await {
        return;
    }
    let writer_context = context.clone();
    let mut writer_cancel = cancel.clone();
    let mut writer_task = tokio::spawn(async move {
        loop {
            let command = tokio::select! {
              biased;
              command = commands.recv() => match command {
                Some(command) => command,
                None => break,
              },
              _ = writer_cancel.changed() => {
                if *writer_cancel.borrow() { break; }
                continue;
              }
            };
            let (message, lease, closing) = match command {
                ClientCommand::Text(text, lease) => (Message::Text(text.into()), lease, false),
                ClientCommand::Pong(payload, lease) => {
                    (Message::Pong(payload.into()), lease, false)
                }
                ClientCommand::Close { code, reason } => {
                    let message = Message::Close(Some(CloseFrame {
                        code: CloseCode::from(code),
                        reason: reason.into(),
                    }));
                    let Some(lease) = writer_context
                        .broker
                        .payload_budget()
                        .try_acquire(message.len(), true)
                    else {
                        writer_context.broker.writer_gone(client);
                        return;
                    };
                    (message, lease, true)
                }
            };
            let sent = timeout(WRITE_TIMEOUT, sink.send(LeasedMessage { message, lease })).await;
            if !matches!(sent, Ok(Ok(()))) {
                writer_context.broker.writer_gone(client);
                return;
            }
            if closing {
                let _ = timeout(WRITE_TIMEOUT, sink.close()).await;
                writer_context.broker.writer_gone(client);
                return;
            }
        }
    });
    let mut reader_cancel = cancel.clone();
    loop {
        tokio::select! {
          _ = shutdown.changed() => break,
          _ = reader_cancel.changed() => {
            if *reader_cancel.borrow() { break; }
          }
          incoming = stream.next() => match incoming {
            Some(Ok(frame @ LeasedMessage { message: Message::Text(_), .. })) => {
              let accepted = tokio::select! {
                result = context.broker.leased_frame(client, frame) => result,
                _ = shutdown.changed() => false,
                _ = reader_cancel.changed() => false,
              };
              if !accepted { break; }
            }
            Some(Ok(LeasedMessage { message: Message::Ping(payload), lease })) => {
              let Some(copy) = context.broker.payload_budget().try_acquire(payload.len(), false) else { break; };
              let payload = payload.to_vec();
              drop(lease);
              if !context.broker.pong(client, payload, copy) { break; }
            }
            Some(Ok(LeasedMessage { message: Message::Close(_), .. })) | None => break,
            Some(Ok(_)) => {}
            Some(Err(_)) => break,
          }
        }
    }
    let broker_requested = *reader_cancel.borrow();
    context.broker.closed(client);
    if broker_requested {
        // 让已排队的 Close 帧先发出；只有写入卡住时才强制中止。
        if timeout(WRITE_TIMEOUT, &mut writer_task).await.is_err() {
            writer_task.abort();
            let _ = writer_task.await;
        }
    } else {
        writer_task.abort();
        let _ = writer_task.await;
    }
}
