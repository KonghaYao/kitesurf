// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// 唯一的长期 `/devtools/browser` 上游。传输层只做三件事：把解析后的上行事件交给
// broker、把 broker 生成的帧写入 socket、在丢失时报告一次 Lost。它不判断客户端权限，
// 不做请求 ID 映射，也不向任何客户端广播。所有 ID 由共享分配器产生，broker 与启动探测
// 使用同一序列，避免内外部请求碰撞。

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{Sink, SinkExt, Stream, StreamExt};
use serde_json::{json, Value};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpStream, UnixStream};
use tokio::sync::{mpsc, watch, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_tungstenite::WebSocketStream;

use crate::observe::Observer;
use crate::HostResult;

pub const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;
pub const READ_BUFFER_BYTES: usize = 32 * 1024;
pub const CODEC_BYTES: usize = 64 * 1024;
pub const ORDINARY_BYTES: usize = 128 * 1024 * 1024;
pub const INTERNAL_BYTES: usize = 20 * 1024 * 1024;

/// 同一宿主共享此预算；复制额度已从普通预算扣除，不代表常驻数组或 RSS 上限。
#[derive(Clone)]
pub struct PayloadBudget {
    ordinary: Arc<Semaphore>,
    internal: Arc<Semaphore>,
    upstream_receive: Arc<Semaphore>,
    copy: Arc<Mutex<()>>,
    high: Arc<(AtomicUsize, AtomicUsize)>,
    _copy_lease: Arc<OwnedSemaphorePermit>,
}

impl Default for PayloadBudget {
    fn default() -> Self {
        let ordinary = Arc::new(Semaphore::new(ORDINARY_BYTES));
        let copy = ordinary
            .clone()
            .try_acquire_many_owned(MAX_FRAME_BYTES as u32)
            .unwrap();
        Self {
            ordinary,
            internal: Arc::new(Semaphore::new(INTERNAL_BYTES - MAX_FRAME_BYTES)),
            upstream_receive: Arc::new(Semaphore::new(MAX_FRAME_BYTES)),
            copy: Arc::new(Mutex::new(())),
            high: Arc::new((AtomicUsize::new(MAX_FRAME_BYTES), AtomicUsize::new(0))),
            _copy_lease: Arc::new(copy),
        }
    }
}

impl PayloadBudget {
    pub fn try_acquire(&self, bytes: usize, internal: bool) -> Option<PayloadLease> {
        let pool = self.pool(internal);
        let permit = pool
            .try_acquire_many_owned(u32::try_from(bytes).ok()?)
            .ok()?;
        self.record_high();
        Some(PayloadLease {
            permit,
            internal,
            extra: Vec::new(),
        })
    }

    pub async fn acquire(&self, bytes: usize, internal: bool) -> PayloadLease {
        let permit = self
            .pool(internal)
            .acquire_many_owned(bytes.try_into().unwrap())
            .await
            .unwrap();
        self.record_high();
        PayloadLease {
            permit,
            internal,
            extra: Vec::new(),
        }
    }

    async fn receive(&self, upstream: bool) -> PayloadLease {
        if !upstream {
            return self.acquire(MAX_FRAME_BYTES, false).await;
        }
        let permit = self
            .upstream_receive
            .clone()
            .acquire_many_owned(MAX_FRAME_BYTES as u32)
            .await
            .unwrap();
        self.record_high();
        PayloadLease {
            permit,
            internal: true,
            extra: Vec::new(),
        }
    }

    fn pool(&self, internal: bool) -> Arc<Semaphore> {
        if internal {
            self.internal.clone()
        } else {
            self.ordinary.clone()
        }
    }

    fn record_high(&self) {
        let (ordinary, internal) = self.used();
        self.high.0.fetch_max(ordinary, Ordering::Relaxed);
        self.high.1.fetch_max(internal, Ordering::Relaxed);
    }

    pub fn high_water(&self) -> (usize, usize) {
        (
            self.high.0.load(Ordering::Relaxed),
            self.high.1.load(Ordering::Relaxed),
        )
    }

    pub fn used(&self) -> (usize, usize) {
        (
            ORDINARY_BYTES - self.ordinary.available_permits(),
            INTERNAL_BYTES
                - self.internal.available_permits()
                - self.upstream_receive.available_permits(),
        )
    }
}

/// 非 Clone 的字节所有权；队列、pending 和 writer 可以直接移动同一个 lease。
#[derive(Debug)]
pub struct PayloadLease {
    permit: OwnedSemaphorePermit,
    internal: bool,
    extra: Vec<OwnedSemaphorePermit>,
}

impl PayloadLease {
    pub fn hold(&mut self, permit: OwnedSemaphorePermit) {
        self.extra.push(permit);
    }

    pub fn bytes(&self) -> usize {
        self.permit.num_permits()
    }
    pub fn is_internal(&self) -> bool {
        self.internal
    }
    pub fn shrink(&mut self, bytes: usize) {
        assert!(bytes <= self.bytes());
        drop(self.permit.split(self.bytes() - bytes));
    }
}

#[derive(Debug)]
pub struct LeasedMessage {
    pub message: Message,
    pub lease: PayloadLease,
}

/// 先计算长度并取得预算，再通过硬上限 writer 分配序列化副本。
pub fn payload_size<T: serde::Serialize>(value: &T) -> Option<usize> {
    struct Counter(usize);
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len())
                .filter(|n| *n <= MAX_FRAME_BYTES)
                .ok_or_else(|| std::io::Error::other("payload limit exceeded"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut count = Counter(0);
    serde_json::to_writer(&mut count, value).ok()?;
    Some(count.0)
}

pub fn serialize_payload<T: serde::Serialize>(
    budget: &PayloadBudget,
    value: &T,
    internal: bool,
) -> Option<(String, PayloadLease)> {
    struct BoundedWriter {
        bytes: Vec<u8>,
        length: usize,
        limit: usize,
        counting: bool,
    }
    impl std::io::Write for BoundedWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            let length = self
                .length
                .checked_add(bytes.len())
                .filter(|n| *n <= self.limit)
                .ok_or_else(|| std::io::Error::other("payload limit exceeded"))?;
            if !self.counting {
                self.bytes.extend_from_slice(bytes);
            }
            self.length = length;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = BoundedWriter {
        bytes: Vec::new(),
        length: 0,
        limit: MAX_FRAME_BYTES,
        counting: true,
    };
    serde_json::to_writer(&mut writer, value).ok()?;
    let lease = budget.try_acquire(writer.length, internal)?;
    writer.limit = writer.length;
    writer.bytes = Vec::with_capacity(writer.length);
    writer.length = 0;
    writer.counting = false;
    serde_json::to_writer(&mut writer, value).ok()?;
    Some((String::from_utf8(writer.bytes).ok()?, lease))
}

type ReceiveWait = Pin<Box<dyn Future<Output = PayloadLease> + Send>>;

pub fn websocket_config() -> WebSocketConfig {
    let mut config = WebSocketConfig::default();
    config.read_buffer_size = READ_BUFFER_BYTES;
    config.write_buffer_size = 0;
    config.max_write_buffer_size = MAX_FRAME_BYTES + 4096;
    config.max_message_size = Some(MAX_FRAME_BYTES);
    config.max_frame_size = Some(MAX_FRAME_BYTES);
    config
}

/// 必须在 split 前包装；socket 先销毁，残留 receive/write lease 随后才归还。
pub struct BudgetedSocket<S> {
    socket: WebSocketStream<S>,
    budget: PayloadBudget,
    receive: Option<PayloadLease>,
    waiting: Option<ReceiveWait>,
    writer: Vec<PayloadLease>,
    writer_failed: bool,
    handshake: Option<PayloadLease>,
    _codec: PayloadLease,
    upstream: bool,
}

impl<S> BudgetedSocket<S> {
    pub fn new(
        socket: WebSocketStream<S>,
        budget: PayloadBudget,
        codec: PayloadLease,
        upstream: bool,
    ) -> Self {
        assert!(codec.is_internal() && codec.bytes() >= CODEC_BYTES);
        Self {
            socket,
            budget,
            receive: None,
            waiting: None,
            writer: Vec::new(),
            writer_failed: false,
            handshake: None,
            _codec: codec,
            upstream,
        }
    }

    pub fn with_handshake(mut self, lease: PayloadLease) -> Self {
        assert!(lease.is_internal() && lease.bytes() >= CODEC_BYTES);
        self.handshake = Some(lease);
        self
    }

    fn idle(&self) -> bool {
        let state = self.socket.read_buffer_state();
        state.buffered_bytes == 0
            && state.frame_payload_len.is_none()
            && state.fragmented_message_bytes.is_none()
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> Stream for BudgetedSocket<S> {
    type Item = Result<LeasedMessage, WsError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.receive.is_none() {
            let budget = this.budget.clone();
            let internal = this.upstream;
            let waiting = this
                .waiting
                .get_or_insert_with(|| Box::pin(async move { budget.receive(internal).await }));
            this.receive = Some(futures_util::ready!(waiting.as_mut().poll(cx)));
            this.waiting = None;
        }
        let copy = this.budget.copy.clone();
        let _guard = copy.lock().unwrap();
        let result = Pin::new(&mut this.socket).poll_next(cx);
        this.socket.release_empty_buffers();
        // 101 tail 在首次接收归一化前可能超过单个 read chunk，握手 lease 延续至此。
        if this.socket.read_buffer_state().buffered_bytes <= READ_BUFFER_BYTES {
            this.handshake = None;
        }
        match result {
            Poll::Pending => {
                if this.idle() {
                    this.receive = None;
                }
                Poll::Pending
            }
            Poll::Ready(Some(Ok(message))) if message.is_text() || message.is_binary() => {
                let mut lease = this.receive.take().unwrap();
                if !this.upstream {
                    lease.shrink(message.len());
                }
                Poll::Ready(Some(Ok(LeasedMessage { message, lease })))
            }
            Poll::Ready(Some(Ok(message))) => {
                // codec 内控制帧由固定额度覆盖；交付后独立持有，不能借 codec 释放来逃账。
                let Some(lease) = this.budget.try_acquire(message.len(), false) else {
                    return Poll::Ready(Some(Err(WsError::Io(std::io::Error::other(
                        "control budget exhausted",
                    )))));
                };
                if !message.is_close() && this.idle() {
                    this.receive = None;
                }
                Poll::Ready(Some(Ok(LeasedMessage { message, lease })))
            }
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(error))),
            Poll::Ready(None) => Poll::Ready(None),
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> Sink<LeasedMessage> for BudgetedSocket<S> {
    type Error = WsError;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), WsError>> {
        self.as_mut().poll_flush(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: LeasedMessage) -> Result<(), WsError> {
        let this = self.get_mut();
        assert!(item.lease.bytes() >= item.message.len());
        let copy = this.budget.copy.clone();
        let _guard = copy.lock().unwrap();
        this.writer.push(item.lease);
        let result = Pin::new(&mut this.socket).start_send(item.message);
        this.writer_failed |= result.is_err();
        result
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), WsError>> {
        let this = self.get_mut();
        let copy = this.budget.copy.clone();
        let _guard = copy.lock().unwrap();
        let result = Pin::new(&mut this.socket).poll_flush(cx);
        this.socket.release_empty_buffers();
        this.writer_failed |= matches!(result, Poll::Ready(Err(_)));
        if matches!(result, Poll::Ready(Ok(()))) && !this.writer_failed {
            this.writer.clear();
        }
        result
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), WsError>> {
        let this = self.get_mut();
        let copy = this.budget.copy.clone();
        let _guard = copy.lock().unwrap();
        let result = Pin::new(&mut this.socket).poll_close(cx);
        this.socket.release_empty_buffers();
        this.writer_failed |= matches!(result, Poll::Ready(Err(_)));
        if matches!(result, Poll::Ready(Ok(()))) && !this.writer_failed {
            this.writer.clear();
        }
        result
    }
}
const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
const OUTBOUND_CAPACITY: usize = 256;
const EVENT_CAPACITY: usize = 1024;
const WRITE_TIMEOUT: Duration = Duration::from_secs(10);

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

#[derive(Debug)]
pub enum UpstreamEvent {
    /// 带 id 的 engine 回复；映射与归属由 broker 决定。
    Response(Value, PayloadLease),
    /// 无 id 的事件；是否可见由 broker 决定。
    Event(Value, PayloadLease),
    /// 一条出站帧已被 socket 接受，用于释放队列字节预算。
    Written { bytes: usize },
    /// 连接结束；保证只发送一次。
    Lost(String),
}

/// broker 使用的上行句柄；克隆只共享有界发送端。
#[derive(Clone)]
pub struct UpstreamHandle {
    outgoing: mpsc::Sender<LeasedMessage>,
    pub budget: PayloadBudget,
    ids: Arc<AtomicU64>,
    observer: Arc<Observer>,
}

impl UpstreamHandle {
    pub fn allocate_id(&self) -> Option<u64> {
        let id = self.ids.fetch_add(1, Ordering::Relaxed);
        (id <= MAX_SAFE_INTEGER).then_some(id)
    }

    pub fn send_value(&self, value: &Value) -> bool {
        let Some((text, lease)) = serialize_payload(&self.budget, value, false) else {
            return false;
        };
        self.send_leased(text, lease)
    }

    pub fn send_text(&self, text: String) -> bool {
        let Some(lease) = self.budget.try_acquire(text.len(), false) else {
            return false;
        };
        self.send_leased(text, lease)
    }

    pub fn send_leased(&self, text: String, lease: PayloadLease) -> bool {
        self.outgoing
            .try_send(LeasedMessage {
                message: Message::Text(text.into()),
                lease,
            })
            .is_ok()
    }

    /// 连接已被 broker 判定结束时使用；不等待握手。
    pub fn close(&self) {
        let lease = self.budget.try_acquire(0, true).unwrap();
        let _ = self.outgoing.try_send(LeasedMessage {
            message: Message::Close(None),
            lease,
        });
        self.observer.kind("upstream-close-requested");
    }
}

pub struct Upstream {
    _lost_tx: watch::Sender<Option<&'static str>>,
    pub lost: watch::Receiver<Option<&'static str>>,
    /// broker 独占消费；只允许一个消费者。
    pub events: mpsc::Receiver<UpstreamEvent>,
    pub handle: UpstreamHandle,
    tasks: JoinSet<()>,
    observer: Arc<Observer>,
}

impl Upstream {
    /// 打开唯一的 `/devtools/browser` 连接，并启动读写任务。
    pub async fn connect(address: &UpstreamAddress, observer: Arc<Observer>) -> HostResult<Self> {
        let stream: Box<dyn Transport> = match address {
            UpstreamAddress::Unix(path) => Box::new(UnixStream::connect(path).await?),
            UpstreamAddress::Tcp(target) => Box::new(TcpStream::connect(target).await?),
        };
        let budget = PayloadBudget::default();
        let codec = budget
            .try_acquire(CODEC_BYTES, true)
            .ok_or("codec reserve exhausted")?;
        let handshake = budget
            .try_acquire(CODEC_BYTES, true)
            .ok_or("handshake reserve exhausted")?;
        let config = websocket_config();
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
        let socket =
            BudgetedSocket::new(socket, budget.clone(), codec, true).with_handshake(handshake);
        let (mut sink, mut source) = socket.split();
        let (outgoing, mut outgoing_rx) = mpsc::channel::<LeasedMessage>(OUTBOUND_CAPACITY);
        let (event_tx, event_rx) = mpsc::channel::<UpstreamEvent>(EVENT_CAPACITY);
        let mut tasks = JoinSet::new();
        let (lost_tx, lost) = watch::channel(None);
        let writer_lost = lost_tx.clone();
        let keep_lost = lost_tx.clone();

        let writer_events = event_tx.clone();
        let writer_observer = observer.clone();
        tasks.spawn(async move {
            while let Some(message) = outgoing_rx.recv().await {
                let closing = matches!(message.message, Message::Close(_));
                let bytes = match &message.message {
                    Message::Text(text) => text.len(),
                    Message::Binary(bytes) => bytes.len(),
                    _ => 0,
                };
                let written = timeout(WRITE_TIMEOUT, sink.send(message)).await;
                if !matches!(written, Ok(Ok(()))) {
                    writer_observer.kind("upstream-write-failed");
                    writer_lost.send_replace(Some("engine write failed"));
                    return;
                }
                if closing {
                    break;
                }
                if writer_events
                    .send(UpstreamEvent::Written { bytes })
                    .await
                    .is_err()
                {
                    return;
                }
            }
            let _ = timeout(WRITE_TIMEOUT, sink.close()).await;
        });

        let reader_observer = observer.clone();
        let reader_budget = budget.clone();
        tasks.spawn(async move {
            let reason = loop {
                match source.next().await {
                    Some(Ok(LeasedMessage {
                        message: Message::Text(text),
                        lease,
                    })) => {
                        // 唯一 receive lease 随消息到 actor；下一次最大接收必须等其释放。
                        // 同步解析副本与 codec 共用已扣除的 copy 额度，不借给长期队列。
                        let value = {
                            let _copy = reader_budget.copy.lock().unwrap();
                            let parsed = serde_json::from_str::<Value>(&text);
                            drop(text);
                            parsed
                        };
                        let Ok(value) = value else {
                            continue;
                        };
                        let event = if value.get("id").and_then(Value::as_u64).is_some() {
                            UpstreamEvent::Response(value, lease)
                        } else {
                            UpstreamEvent::Event(value, lease)
                        };
                        if event_tx.send(event).await.is_err() {
                            return;
                        }
                    }
                    Some(Ok(LeasedMessage {
                        message: Message::Binary(bytes),
                        ..
                    })) => {
                        reader_observer.with(
                            "upstream-binary",
                            json!({ "kind": "upstream-binary", "bytes": bytes.len() }),
                        );
                    }
                    Some(Ok(LeasedMessage {
                        message: Message::Close(_),
                        ..
                    })) => break "engine closed the CDP connection",
                    Some(Ok(_)) => {}
                    Some(Err(_)) => break "engine CDP transport failed",
                    None => break "engine CDP connection closed",
                }
            };
            reader_observer.with(
                "upstream-closed",
                json!({ "kind": "upstream-closed", "reason": reason }),
            );
            lost_tx.send_replace(Some(reason));
        });

        Ok(Self {
            _lost_tx: keep_lost,
            lost,
            events: event_rx,
            handle: UpstreamHandle {
                budget,
                outgoing,
                ids: Arc::new(AtomicU64::new(1)),
                observer: observer.clone(),
            },
            tasks,
            observer,
        })
    }

    /// 启动阶段的一次性宿主请求；客户端尚未接入，事件被丢弃。
    pub async fn request(
        &mut self,
        method: &str,
        params: Option<Value>,
        deadline: Duration,
    ) -> HostResult<Value> {
        let Some(id) = self.handle.allocate_id() else {
            return Err("CDP request ids are exhausted".into());
        };
        let mut payload = json!({ "id": id, "method": method });
        if let Some(params) = params {
            payload["params"] = params;
        }
        if !self.handle.send_value(&payload) {
            return Err("engine connection is closed".into());
        }
        let wait = async {
            loop {
                let event = tokio::select! {
                    event = self.events.recv() => event,
                    _ = self.lost.changed() => {
                        return Err("engine connection lost while waiting for a reply".into());
                    }
                };
                let Some(event) = event else {
                    break;
                };
                match event {
                    UpstreamEvent::Response(value, _lease) => {
                        if value.get("id").and_then(Value::as_u64) != Some(id) {
                            continue;
                        }
                        if let Some(error) = value.get("error") {
                            let message = error
                                .get("message")
                                .and_then(Value::as_str)
                                .unwrap_or("CDP command failed");
                            return Err(message.to_string().into());
                        }
                        return Ok(value.get("result").cloned().unwrap_or(Value::Null));
                    }
                    UpstreamEvent::Lost(reason) => return Err(reason.into()),
                    _ => {}
                }
            }
            Err("engine connection closed while waiting for a reply".into())
        };
        match timeout(deadline, wait).await {
            Ok(result) => result,
            Err(_) => Err(format!("CDP timeout: {method}; outcome may be unknown").into()),
        }
    }

    pub fn handle(&self) -> UpstreamHandle {
        self.handle.clone()
    }

    pub async fn shutdown(&mut self) {
        self.handle.close();
        let _ = timeout(Duration::from_secs(2), async {
            while self.tasks.join_next().await.is_some() {}
        })
        .await;
        self.tasks.abort_all();
        while self.tasks.join_next().await.is_some() {}
        self.observer.kind("upstream-shutdown");
    }
}
