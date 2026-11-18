// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// 唯一的上游 CDP WebSocket。宿主经私有引擎 socket 连接，用保留 id 发送自己的生命周期命令，
// 并把其余帧广播给仅用于测试的中继。上游丢失即结束当前 generation；不重连，也不重放。

use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpStream, UnixStream};
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::Message;

use crate::observe::Observer;
use crate::HostResult;

const HOST_ID_BASE: u64 = 1_000_000_000;
const _: () = assert!(HOST_ID_BASE > 999_999_999);
const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

pub enum UpstreamAddress {
    Unix(PathBuf),
    Tcp(SocketAddr),
}

impl fmt::Display for UpstreamAddress {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unix(path) => write!(formatter, "unix:{}", path.display()),
            Self::Tcp(address) => write!(formatter, "tcp:{address}"),
        }
    }
}

trait Transport: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Transport for T {}

type CommandReply = std::result::Result<Value, String>;
type PendingCommands = Arc<Mutex<HashMap<u64, oneshot::Sender<CommandReply>>>>;

fn command_reply(value: &Value) -> CommandReply {
    match value.get("error") {
        Some(error) => Err(error
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("CDP command failed")
            .to_string()),
        None => Ok(value.get("result").cloned().unwrap_or(Value::Null)),
    }
}

/// 给每个中继客户端克隆一帧，或发送终止关闭通知。
#[derive(Clone, Debug)]
pub enum Frame {
    Text(String),
    Closed,
}

pub struct Upstream {
    sink: mpsc::UnboundedSender<Message>,
    pending: PendingCommands,
    next_id: AtomicU64,
    frames: broadcast::Sender<Frame>,
    closed: watch::Receiver<Option<String>>,
    observer: Arc<Observer>,
}

impl Upstream {
    /// 打开唯一的 `/devtools/browser` 连接，并启动其读取与写入任务。
    pub async fn connect(address: &UpstreamAddress, observer: Arc<Observer>) -> HostResult<Self> {
        let stream: Box<dyn Transport> = match address {
            UpstreamAddress::Unix(path) => Box::new(UnixStream::connect(path).await?),
            UpstreamAddress::Tcp(target) => Box::new(TcpStream::connect(target).await?),
        };
        let mut config = WebSocketConfig::default();
        config.max_message_size = Some(MAX_FRAME_BYTES);
        config.max_frame_size = Some(MAX_FRAME_BYTES);
        let (socket, _response) = tokio_tungstenite::client_async_with_config(
            "ws://localhost/devtools/browser",
            stream,
            Some(config),
        )
        .await?;
        observer.with(
            "upstream-open",
            json!({ "kind": "upstream-open", "address": address.to_string(), "connections": 1 }),
        );
        let (mut sink, mut stream) = socket.split();
        let (outgoing, mut outgoing_rx) = mpsc::unbounded_channel::<Message>();
        tokio::spawn(async move {
            while let Some(message) = outgoing_rx.recv().await {
                if sink.send(message).await.is_err() {
                    break;
                }
            }
            let _ = sink.close().await;
        });

        let pending: PendingCommands = Arc::new(Mutex::new(HashMap::new()));
        let (frames, _) = broadcast::channel::<Frame>(1024);
        let (closed_tx, closed_rx) = watch::channel::<Option<String>>(None);
        let reader_pending = pending.clone();
        let reader_frames = frames.clone();
        let reader_observer = observer.clone();
        tokio::spawn(async move {
            let mut reason = None;
            while let Some(message) = stream.next().await {
                match message {
                    Ok(Message::Text(text)) => {
                        let text = text.to_string();
                        let value = match serde_json::from_str::<Value>(&text) {
                            Ok(value) => value,
                            Err(_) => continue,
                        };
                        let id = value.get("id").and_then(Value::as_u64);
                        if let Some(id) = id {
                            let waiting = reader_pending
                                .lock()
                                .ok()
                                .and_then(|mut guard| guard.remove(&id));
                            if let Some(waiting) = waiting {
                                let reply = command_reply(&value);
                                let _ = waiting.send(reply);
                                continue;
                            }
                        }
                        let _ = reader_frames.send(Frame::Text(text));
                    }
                    Ok(Message::Binary(bytes)) => {
                        reader_observer.with(
                            "upstream-binary",
                            json!({ "kind": "upstream-binary", "bytes": bytes.len() }),
                        );
                    }
                    Ok(Message::Close(_)) => {
                        reason = Some("engine closed the CDP connection".to_string());
                        break;
                    }
                    Ok(_) => {}
                    Err(error) => {
                        reason = Some(format!("engine CDP transport failed: {error}"));
                        break;
                    }
                }
            }
            let reason = reason.unwrap_or_else(|| "engine CDP connection closed".to_string());
            reader_observer.with(
                "upstream-closed",
                json!({ "kind": "upstream-closed", "reason": reason }),
            );
            if let Ok(mut guard) = reader_pending.lock() {
                for (_, waiting) in guard.drain() {
                    let _ = waiting.send(Err("engine connection closed".to_string()));
                }
            }
            let _ = reader_frames.send(Frame::Closed);
            let _ = closed_tx.send(Some(reason));
        });

        Ok(Self {
            sink: outgoing,
            pending,
            next_id: AtomicU64::new(HOST_ID_BASE),
            frames,
            closed: closed_rx,
            observer,
        })
    }

    /// 发送一条宿主生命周期命令并等待回复，绝不用于页面流量。
    pub async fn request(&self, method: &str, params: Option<Value>) -> HostResult<Value> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = oneshot::channel();
        if let Ok(mut guard) = self.pending.lock() {
            guard.insert(id, sender);
        }
        let payload =
            json!({ "id": id, "method": method, "params": params.unwrap_or_else(|| json!({})) });
        if self
            .sink
            .send(Message::Text(payload.to_string().into()))
            .is_err()
        {
            self.forget(id);
            return Err("engine connection is closed".into());
        }
        match tokio::time::timeout(REQUEST_TIMEOUT, receiver).await {
            Ok(Ok(Ok(value))) => Ok(value),
            Ok(Ok(Err(message))) => Err(message.into()),
            Ok(Err(_)) => Err("engine connection closed while waiting for a reply".into()),
            Err(_) => {
                self.forget(id);
                Err(format!("CDP timeout: {method}; outcome may be unknown").into())
            }
        }
    }

    /// 把一个中继客户端帧原样送入上游，不做改写。
    pub fn send_client_text(&self, text: String) -> bool {
        self.sink.send(Message::Text(text.into())).is_ok()
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Frame> {
        self.frames.subscribe()
    }

    pub fn closed_receiver(&self) -> watch::Receiver<Option<String>> {
        self.closed.clone()
    }

    pub fn is_alive(&self) -> bool {
        self.closed.borrow().is_none()
    }

    pub fn close(&self) {
        let _ = self.sink.send(Message::Close(None));
        self.observer.kind("upstream-close-requested");
    }

    fn forget(&self, id: u64) {
        if let Ok(mut guard) = self.pending.lock() {
            guard.remove(&id);
        }
    }
}
