// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// 公共 loopback 入口只做协议与预算校验：HTTP 管理请求转交 broker，WS 帧交给 broker
// 的有界通道。权限、请求 ID 映射、attachment 归属和生命周期全部由 broker actor 决定；
// 本文件不广播上游事件，也不自行授予 controller。

use std::collections::HashMap;
use std::convert::Infallible;
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
const CLIENT_QUEUE: usize = 64;
const WRITE_TIMEOUT: Duration = Duration::from_secs(10);
const BODY_DEADLINE: Duration = Duration::from_secs(5);
const UPGRADE_DEADLINE: Duration = Duration::from_secs(10);
/// 关闭时先给已经生成的 HTTP 应答一个写出窗口，再中止长连接任务。
const SHUTDOWN_GRACE: Duration = Duration::from_secs(1);

#[derive(Clone)]
pub struct SessionInfo {
    pub session_id: String,
    pub build_version: String,
    pub environment: String,
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
        (Method::GET, "/json") | (Method::GET, "/json/list") => targets_body(&context).await,
        (Method::POST, "/flush") => match context.broker.flush().await {
            Ok(status) => json_response(StatusCode::OK, status.to_string()),
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
        (Method::POST, "/backup") => error_response(
            StatusCode::NOT_IMPLEMENTED,
            "Online profile backup is not available",
        ),
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
    struct Body {
        text: String,
        _lease: PayloadLease,
    }
    impl AsRef<[u8]> for Body {
        fn as_ref(&self) -> &[u8] {
            self.text.as_bytes()
        }
    }
    let Some((text, lease)) =
        crate::transport::serialize_payload(&context.broker.payload_budget(), value, false)
    else {
        return error_response(StatusCode::SERVICE_UNAVAILABLE, "Payload budget exhausted");
    };
    json_response(
        StatusCode::OK,
        Bytes::from_owner(Body {
            text,
            _lease: lease,
        }),
    )
}

async fn session_body(context: &HostContext) -> Response<Full<Bytes>> {
    let Some(snapshot) = context.broker.session().await else {
        return error_response(StatusCode::CONFLICT, "Browser session is lost");
    };
    let tabs = match context.broker.targets().await {
        Ok(tabs) => tabs,
        Err(message) => return error_response(StatusCode::CONFLICT, &message),
    };
    let _tabs_lease = tabs.lease;
    let tabs = tabs.values;
    let profile = match context.profile.status().await {
        Ok(status) => status,
        Err(_) => return error_response(StatusCode::CONFLICT, "Profile status unavailable"),
    };
    #[derive(serde::Serialize)]
    #[serde(rename_all = "camelCase")]
    struct SessionBody<'a> {
        version: &'a str,
        session_id: &'a str,
        generation: &'a str,
        alive: bool,
        recovery: &'a str,
        profile: &'a Value,
        controller: &'a Option<String>,
        control_epoch: u64,
        tabs: &'a [Value],
    }
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
            tabs: &tabs,
        },
    )
}

async fn targets_body(context: &HostContext) -> Response<Full<Bytes>> {
    let tabs = match context.broker.targets().await {
        Ok(tabs) => tabs,
        Err(message) => return error_response(StatusCode::CONFLICT, &message),
    };
    let _tabs_lease = tabs.lease;
    let entries = tabs
        .values
        .iter()
        .map(|target| {
            let id = target["targetId"].as_str().unwrap_or_default();
            #[derive(serde::Serialize)]
            struct Entry<'a> {
                id: &'a str,
                #[serde(rename = "type")]
                kind: &'static str,
                title: &'a Value,
                url: &'a Value,
                #[serde(rename = "webSocketDebuggerUrl")]
                websocket: String,
                #[serde(rename = "devtoolsFrontendUrl")]
                devtools: String,
            }
            Entry {
                id,
                kind: "page",
                title: &target["title"],
                url: &target["url"],
                websocket: format!("ws://127.0.0.1:{}/devtools/page/{id}", context.port),
                devtools: format!(
                    "devtools://devtools/bundled/inspector.html?ws=127.0.0.1:{}/devtools/page/{id}",
                    context.port
                ),
            }
        })
        .collect::<Vec<_>>();
    budgeted_json(context, &entries)
}

struct Query {
    session: Option<String>,
    role: Option<String>,
    owner: Option<String>,
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
        if session != &context.session.session_id {
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
