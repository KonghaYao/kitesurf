// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// T2 仅用于测试的中继：同一时刻只有一个外部客户端，帧原样转发到唯一上游。它没有
// controller/viewer、接管、attachment 归属或多客户端请求映射；这些属于 T4 代理。关闭命令
// 排在中继排空之后执行。

use std::convert::Infallible;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::header::{CONNECTION, UPGRADE};
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

use crate::observe::Observer;
use crate::services::ProfileService;
use crate::transport::{Frame, Upstream};
use crate::HostResult;

const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;
const CONTROL_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct SessionInfo {
    pub session_id: String,
    pub build_version: String,
    pub environment: String,
}

struct HostContext {
    upstream: Arc<Upstream>,
    session: SessionInfo,
    generation: String,
    recovery: String,
    profile: Arc<ProfileService>,
    observer: Arc<Observer>,
    client_active: Arc<AtomicBool>,
    close_requests: mpsc::UnboundedSender<()>,
    port: u16,
}

pub struct HostServer {
    port: u16,
    shutdown: watch::Sender<bool>,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl HostServer {
    /// 绑定公共 loopback 监听器；端口 0 在此解析，并在 ready 前上报。
    pub fn bind(
        port: u16,
        upstream: Arc<Upstream>,
        session: SessionInfo,
        generation: String,
        recovery: String,
        profile: Arc<ProfileService>,
        observer: Arc<Observer>,
    ) -> HostResult<(Self, mpsc::UnboundedReceiver<()>)> {
        let listener = std::net::TcpListener::bind(("127.0.0.1", port))?;
        listener.set_nonblocking(true)?;
        let listener = TcpListener::from_std(listener)?;
        let port = listener.local_addr()?.port();
        let (close_tx, close_rx) = mpsc::unbounded_channel();
        let context = Arc::new(HostContext {
            upstream,
            session,
            generation,
            recovery,
            profile,
            observer: observer.clone(),
            client_active: Arc::new(AtomicBool::new(false)),
            close_requests: close_tx,
            port,
        });
        let (shutdown, mut shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                  changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                      break;
                    }
                  }
                  accepted = listener.accept() => match accepted {
                    Ok((stream, _)) => {
                      let context = context.clone();
                      tokio::spawn(async move {
                        let service = service_fn(move |request| {
                          let context = context.clone();
                          async move { Ok::<_, Infallible>(handle_request(request, context).await) }
                        });
                        let _ = hyper::server::conn::http1::Builder::new()
                          .serve_connection(TokioIo::new(stream), service)
                          .with_upgrades()
                          .await;
                      });
                    }
                    Err(_) => break,
                  }
                }
            }
        });
        observer.with(
            "public-ready",
            json!({ "kind": "public-ready", "port": port }),
        );
        Ok((
            Self {
                port,
                shutdown,
                task: Mutex::new(Some(task)),
            },
            close_rx,
        ))
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    /// 停止接受连接并丢弃监听器，早于触发关闭的响应消失。
    pub async fn stop(&self) {
        let _ = self.shutdown.send(true);
        let task = self.task.lock().ok().and_then(|mut guard| guard.take());
        if let Some(task) = task {
            let _ = timeout(Duration::from_secs(2), task).await;
        }
    }
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

fn allowed_origin(request: &Request<Incoming>, port: u16) -> bool {
    let host = request
        .headers()
        .get("host")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    if host != format!("127.0.0.1:{port}") && host != format!("localhost:{port}") {
        return false;
    }
    let origin = request
        .headers()
        .get("origin")
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

async fn handle_request(
    request: Request<Incoming>,
    context: Arc<HostContext>,
) -> Response<Full<Bytes>> {
    if !allowed_origin(&request, context.port) {
        return json_response(
            StatusCode::FORBIDDEN,
            r#"{"error":"Local browser origin rejected"}"#,
        );
    }
    let upgrade_requested = request
        .headers()
        .get(UPGRADE)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.eq_ignore_ascii_case("websocket"))
        .unwrap_or(false);
    if upgrade_requested {
        return upgrade(request, context).await;
    }
    let path = request.uri().path().to_string();
    let method = request.method().clone();
    match (method, path.as_str()) {
        (Method::GET, "/health") => json_response(
            StatusCode::OK,
            json!({
              "alive": context.upstream.is_alive(),
              "version": context.session.build_version,
            })
            .to_string(),
        ),
        (Method::GET, "/session") => match session_body(&context).await {
            Ok(body) => json_response(StatusCode::OK, body),
            Err(error) => json_response(
                StatusCode::CONFLICT,
                json!({ "error": error.to_string() }).to_string(),
            ),
        },
        (Method::GET, "/json/version") => json_response(
            StatusCode::OK,
            json!({
              "Browser": format!("Kitesurf/{}", context.session.build_version),
              "Protocol-Version": "1.3",
              "webSocketDebuggerUrl": format!("ws://127.0.0.1:{}/devtools/browser", context.port),
            })
            .to_string(),
        ),
        (Method::GET, "/json") | (Method::GET, "/json/list") => match targets(&context).await {
            Ok(tabs) => {
                let entries = tabs
          .into_iter()
          .map(|target| {
            let id = target["targetId"].as_str().unwrap_or_default();
            json!({
              "id": id,
              "type": "page",
              "title": target["title"].clone(),
              "url": target["url"].clone(),
              "webSocketDebuggerUrl":
                format!("ws://127.0.0.1:{}/devtools/page/{id}", context.port),
              "devtoolsFrontendUrl": format!(
                "devtools://devtools/bundled/inspector.html?ws=127.0.0.1:{}/devtools/page/{id}",
                context.port
              ),
            })
          })
          .collect::<Vec<_>>();
                json_response(StatusCode::OK, Value::Array(entries).to_string())
            }
            Err(error) => json_response(
                StatusCode::CONFLICT,
                json!({ "error": error.to_string() }).to_string(),
            ),
        },
        (Method::POST, "/flush") => match flush(&context).await {
            Ok(()) => match profile_status(&context.profile).await {
                Ok(status) => json_response(StatusCode::OK, status.to_string()),
                Err(error) => json_response(
                    StatusCode::CONFLICT,
                    json!({ "error": error.to_string() }).to_string(),
                ),
            },
            Err(error) => json_response(
                StatusCode::CONFLICT,
                json!({ "error": error.to_string() }).to_string(),
            ),
        },
        (Method::POST, "/close") => {
            let _ = context.close_requests.send(());
            json_response(StatusCode::OK, r#"{"closed":true}"#)
        }
        (Method::GET, "/") => json_response(
            StatusCode::OK,
            json!({
              "browser": format!("Kitesurf {}", context.session.build_version),
              "session": "/session",
              "cdp": format!("ws://127.0.0.1:{}/devtools/browser", context.port),
              "inspect": "chrome://inspect - configure this address",
              "profile": context.profile.display(),
            })
            .to_string(),
        ),
        _ => empty_response(StatusCode::NOT_FOUND),
    }
}

async fn upgrade(
    mut request: Request<Incoming>,
    context: Arc<HostContext>,
) -> Response<Full<Bytes>> {
    let path = request.uri().path().to_string();
    if !(path == "/devtools/browser" || path.starts_with("/devtools/page/")) {
        return empty_response(StatusCode::NOT_FOUND);
    }
    if context
        .client_active
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return json_response(
            StatusCode::CONFLICT,
            r#"{"error":"A client is already connected"}"#,
        );
    }
    let version = request
        .headers()
        .get("sec-websocket-version")
        .and_then(|value| value.to_str().ok());
    if version != Some("13") {
        context.client_active.store(false, Ordering::SeqCst);
        return empty_response(StatusCode::UPGRADE_REQUIRED);
    }
    let Some(key) = request
        .headers()
        .get("sec-websocket-key")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
    else {
        context.client_active.store(false, Ordering::SeqCst);
        return empty_response(StatusCode::BAD_REQUEST);
    };
    let accept = tokio_tungstenite::tungstenite::handshake::derive_accept_key(key.as_bytes());
    let upgrade = hyper::upgrade::on(&mut request);
    let context = context.clone();
    tokio::spawn(async move {
        let upgraded = match upgrade.await {
            Ok(upgraded) => upgraded,
            Err(error) => {
                eprintln!("[relay] upgrade failed: {error}");
                context.client_active.store(false, Ordering::SeqCst);
                return;
            }
        };
        run_client(TokioIo::new(upgraded), context.clone()).await;
        context.client_active.store(false, Ordering::SeqCst);
    });
    Response::builder()
        .status(StatusCode::SWITCHING_PROTOCOLS)
        .header(UPGRADE, "websocket")
        .header(CONNECTION, "Upgrade")
        .header("sec-websocket-accept", accept)
        .body(Full::new(Bytes::new()))
        .expect("upgrade response")
}

async fn run_client<S>(stream: S, context: Arc<HostContext>)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let mut config = WebSocketConfig::default();
    config.max_message_size = Some(MAX_FRAME_BYTES);
    config.max_frame_size = Some(MAX_FRAME_BYTES);
    let socket = WebSocketStream::from_raw_socket(
        stream,
        tokio_tungstenite::tungstenite::protocol::Role::Server,
        Some(config),
    )
    .await;
    context.observer.with(
        "client-open",
        json!({ "kind": "client-open", "clients": 1 }),
    );
    let mut frames = context.upstream.subscribe();
    let (mut sink, mut stream) = socket.split();
    loop {
        tokio::select! {
          incoming = stream.next() => match incoming {
            Some(Ok(Message::Text(text))) => {
              // Browser.close 是宿主命令：先应答，再走有序的 flush-and-close 路径，
              // 避免引擎 socket 关闭被当作引擎丢失。
              let close_id = serde_json::from_str::<Value>(&text)
                .ok()
                .filter(|value| value["method"] == "Browser.close")
                .and_then(|value| value.get("id").cloned());
              if let Some(id) = close_id {
                let reply = json!({ "id": id, "result": {} }).to_string();
                if sink.send(Message::Text(reply.into())).await.is_err() {
                  break;
                }
                let _ = context.close_requests.send(());
                break;
              }
              if !context.upstream.send_client_text(text.to_string()) {
                break;
              }
            }
            Some(Ok(Message::Ping(payload))) => {
              if sink.send(Message::Pong(payload)).await.is_err() {
                break;
              }
            }
            Some(Ok(Message::Close(_))) | None => break,
            Some(Ok(_)) => {}
            Some(Err(error)) => {
              eprintln!("[relay] client failed: {error}");
              break;
            }
          },
          frame = frames.recv() => match frame {
            Ok(Frame::Text(text)) => {
              if sink.send(Message::Text(text.into())).await.is_err() {
                break;
              }
            }
            Ok(Frame::Closed) => {
              let _ = sink.send(Message::Close(None)).await;
              break;
            }
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
              eprintln!("[relay] client missed {skipped} frames");
            }
            Err(_) => break,
          }
        }
    }
    context.observer.with(
        "client-closed",
        json!({ "kind": "client-closed", "clients": 0 }),
    );
}

async fn flush(context: &HostContext) -> HostResult<()> {
    timeout(
        CONTROL_TIMEOUT,
        context.upstream.request("Kitesurf.flushProfile", None),
    )
    .await
    .map_err(|_| "flush timed out")?
    .map_err(|error| error.to_string())?;
    Ok(())
}

async fn targets(context: &HostContext) -> HostResult<Vec<Value>> {
    let result = timeout(
        CONTROL_TIMEOUT,
        context.upstream.request("Target.getTargets", None),
    )
    .await
    .map_err(|_| "target discovery timed out")?
    .map_err(|error| error.to_string())?;
    let infos = result["targetInfos"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    Ok(infos
        .into_iter()
        .filter(|target| target["type"] == "page")
        .collect())
}

async fn profile_status(profile: &Arc<ProfileService>) -> HostResult<Value> {
    profile
        .status()
        .await
        .map_err(|error| error.to_string().into())
}

async fn session_body(context: &HostContext) -> HostResult<String> {
    let tabs = targets(context).await?;
    let profile = profile_status(&context.profile).await?;
    Ok(json!({
      "version": context.session.build_version,
      "sessionId": context.session.session_id,
      "generation": context.generation,
      "alive": context.upstream.is_alive(),
      "recovery": context.recovery,
      "profile": profile,
      "controller": Value::Null,
      "controlEpoch": 1,
      "environment": context.session.environment,
      "tabs": tabs,
    })
    .to_string())
}
