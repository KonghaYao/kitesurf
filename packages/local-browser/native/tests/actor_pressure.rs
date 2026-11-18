// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

use super::*;
use std::future::Future;
use std::sync::Mutex;
use crate::transport::{PayloadBudget, INTERNAL_BYTES, MAX_FRAME_BYTES, ORDINARY_BYTES};
use tokio::sync::{oneshot, Notify, Semaphore};

// 固定表覆盖场景线程、blocking worker 与任意线程的 free/realloc；不忽略小 allocation。
// runtime 与有限 harness 也在观测值中；fixture/伪上游排除，不是 SQLite/native/RSS 保证。
// scope 只决定新分配是否入表；释放不依赖 scope，线程 join 后必须 live=0。
// 阶段与小尺寸事件环只写固定数组，不能在 tracking 路径分配；供残留归因。
pub(crate) mod allocations {
    use std::cell::Cell;
    use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
    use std::sync::Mutex;

    static LIVE: AtomicUsize = AtomicUsize::new(0);
    static PHASE: AtomicU32 = AtomicU32::new(0);
    pub static SERIAL: Mutex<()> = Mutex::new(());

    pub const PHASE_SETUP: u32 = 1;
    pub const PHASE_INGRESS: u32 = 2;
    pub const PHASE_UNKNOWN: u32 = 3;
    pub const PHASE_SLOW: u32 = 4;
    pub const PHASE_BACKFILL: u32 = 5;
    pub const PHASE_STATUS: u32 = 6;
    pub const PHASE_CANCEL: u32 = 7;
    pub const PHASE_RESOLVED: u32 = 8;
    pub const PHASE_LATE: u32 = 9;
    pub const PHASE_TEARDOWN: u32 = 10;
    pub const PHASE_SETTLE: u32 = 11;
    pub const PHASE_RUNTIME: u32 = 12;

    pub fn phase(value: u32) {
        PHASE.store(value, Ordering::Relaxed);
    }

    #[derive(Clone, Copy, Debug, Default)]
    pub struct Stats {
        pub live: usize,
        pub peak: usize,
        pub count: usize,
        pub cross_thread_frees: usize,
        pub overflow: bool,
    }
    #[derive(Clone, Copy, Debug, Default)]
    pub struct Record {
        pub size: usize,
        pub thread: usize,
        pub phase: u32,
        pub seq: u32,
    }
    #[derive(Clone, Copy, Debug, Default)]
    pub struct Event {
        pub add: bool,
        pub found: bool,
        pub size: usize,
        pub pointer: usize,
        pub thread: usize,
        pub phase: u32,
    }
    const SMALL: usize = 128;
    const EVENTS: usize = 256;
    pub const RECORDS: usize = 32;
    struct Tracker {
        stats: Stats,
        seq: u32,
        slots: [(usize, usize, usize, u32, u32); 32768],
        events: [Event; EVENTS],
        event_head: usize,
        event_count: usize,
    }
    static TRACKER: Mutex<Tracker> = Mutex::new(Tracker {
        stats: Stats {
            live: 0,
            peak: 0,
            count: 0,
            cross_thread_frees: 0,
            overflow: false,
        },
        seq: 0,
        slots: [(0, 0, 0, 0, 0); 32768],
        events: [Event {
            add: false,
            found: false,
            size: 0,
            pointer: 0,
            thread: 0,
            phase: 0,
        }; EVENTS],
        event_head: 0,
        event_count: 0,
    });
    thread_local! {
        static ENABLED: Cell<bool> = const { Cell::new(false) };
        static THREAD: Cell<u8> = const { Cell::new(0) };
    }
    pub struct Scope(bool);
    impl Scope {
        pub fn enter() -> Self {
            Self(ENABLED.with(|enabled| enabled.replace(true)))
        }
        pub fn suspend() -> Self {
            Self(ENABLED.with(|enabled| enabled.replace(false)))
        }
    }
    pub async fn untracked<F: std::future::Future>(future: F) -> F::Output {
        tokio::pin!(future);
        std::future::poll_fn(|cx| {
            let _scope = Scope::suspend();
            future.as_mut().poll(cx)
        })
        .await
    }
    impl Drop for Scope {
        fn drop(&mut self) {
            ENABLED.with(|enabled| enabled.set(self.0));
        }
    }
    fn thread() -> usize {
        THREAD.with(|value| value as *const Cell<u8> as usize)
    }
    impl Tracker {
        fn index(&self, pointer: usize) -> Option<usize> {
            let start = (pointer >> 4).wrapping_mul(0x9e3779b1) % self.slots.len();
            for offset in 0..self.slots.len() {
                let index = (start + offset) % self.slots.len();
                if self.slots[index].0 == pointer {
                    return Some(index);
                }
                if self.slots[index].0 == 0 {
                    return None;
                }
            }
            None
        }
        fn event(&mut self, add: bool, found: bool, size: usize, pointer: usize) {
            if size > SMALL {
                return;
            }
            self.events[self.event_head] = Event {
                add,
                found,
                size,
                pointer,
                thread: thread(),
                phase: PHASE.load(Ordering::Relaxed),
            };
            self.event_head = (self.event_head + 1) % EVENTS;
            self.event_count = (self.event_count + 1).min(EVENTS);
        }
        fn add(&mut self, pointer: usize, size: usize) {
            if pointer == 0 {
                return;
            }
            self.stats.live += size;
            self.stats.peak = self.stats.peak.max(self.stats.live);
            self.stats.count += 1;
            self.seq = self.seq.wrapping_add(1);
            let phase = PHASE.load(Ordering::Relaxed);
            let start = (pointer >> 4).wrapping_mul(0x9e3779b1) % self.slots.len();
            for offset in 0..self.slots.len() {
                let index = (start + offset) % self.slots.len();
                if matches!(self.slots[index].0, 0 | usize::MAX) {
                    self.slots[index] = (pointer, size, thread(), phase, self.seq);
                    LIVE.fetch_add(1, Ordering::Relaxed);
                    self.event(true, true, size, pointer);
                    return;
                }
            }
            self.stats.overflow = true;
            self.event(true, false, size, pointer);
        }
        fn remove(&mut self, pointer: usize) -> bool {
            if let Some(index) = self.index(pointer) {
                let slot = self.slots[index];
                self.stats.live -= slot.1;
                self.stats.cross_thread_frees += usize::from(slot.2 != thread());
                self.slots[index] = (usize::MAX, 0, 0, 0, 0);
                LIVE.fetch_sub(1, Ordering::Relaxed);
                self.event(false, true, slot.1, pointer);
                return true;
            }
            false
        }
    }
    pub fn allocated(size: usize, alloc: impl FnOnce() -> *mut u8) -> *mut u8 {
        if !ENABLED.try_with(Cell::get).unwrap_or(false) {
            return alloc();
        }
        let mut tracker = TRACKER.lock().unwrap();
        let pointer = alloc();
        tracker.add(pointer as usize, size);
        pointer
    }
    pub fn freed(pointer: *mut u8, size: usize) {
        if LIVE.load(Ordering::Relaxed) == 0 {
            return;
        }
        let mut tracker = TRACKER.lock().unwrap();
        if !tracker.remove(pointer as usize) && ENABLED.try_with(Cell::get).unwrap_or(false) {
            tracker.event(false, false, size, pointer as usize);
        }
    }
    // mutex 覆盖 System.realloc；旧地址重用不能抢先覆盖记录，峰值按旧新并存计。
    pub fn reallocated(old: *mut u8, size: usize, realloc: impl FnOnce() -> *mut u8) -> *mut u8 {
        if LIVE.load(Ordering::Relaxed) == 0 && !ENABLED.try_with(Cell::get).unwrap_or(false) {
            return realloc();
        }
        let mut tracker = TRACKER.lock().unwrap();
        let tracked = tracker.index(old as usize).is_some();
        let new = realloc();
        if new.is_null() {
            return new;
        }
        if tracked {
            tracker.stats.peak = tracker.stats.peak.max(tracker.stats.live + size);
            tracker.remove(old as usize);
        }
        if tracked || ENABLED.try_with(Cell::get).unwrap_or(false) {
            tracker.add(new as usize, size);
        }
        new
    }
    pub fn reset() {
        let mut tracker = TRACKER.lock().unwrap();
        assert_eq!(tracker.stats.live, 0);
        tracker.stats = Stats::default();
        tracker.seq = 0;
        tracker.slots.fill((0, 0, 0, 0, 0));
        tracker.events.fill(Event::default());
        tracker.event_head = 0;
        tracker.event_count = 0;
    }
    pub fn stats() -> Stats {
        TRACKER.lock().unwrap().stats
    }
    pub fn records() -> ([Record; RECORDS], usize) {
        let tracker = TRACKER.lock().unwrap();
        let mut records = [Record::default(); RECORDS];
        let mut count = 0;
        for slot in tracker.slots.iter() {
            if slot.0 == 0 || slot.0 == usize::MAX || count == RECORDS {
                continue;
            }
            records[count] = Record {
                size: slot.1,
                thread: slot.2,
                phase: slot.3,
                seq: slot.4,
            };
            count += 1;
        }
        (records, count)
    }
    pub fn events() -> ([Event; RECORDS], usize) {
        let tracker = TRACKER.lock().unwrap();
        let take = tracker.event_count.min(RECORDS);
        let mut events = [Event::default(); RECORDS];
        for (output, offset) in events.iter_mut().zip(0..take) {
            let index = (tracker.event_head + EVENTS - take + offset) % EVENTS;
            *output = tracker.events[index];
        }
        (events, take)
    }
    // 只用固定快照；格式化在释放 tracker 锁并暂停 tracking 后，避免递归分配。
    pub fn describe() -> String {
        use std::fmt::Write as _;
        let stats = stats();
        let (records, record_count) = records();
        let (events, event_count) = events();
        let _quiet = Scope::suspend();
        let mut text = format!("{stats:?}");
        for record in &records[..record_count] {
            let _ = write!(
                text,
                " rec(size={},thread={},phase={},seq={})",
                record.size, record.thread, record.phase, record.seq
            );
        }
        for event in &events[..event_count] {
            let _ = write!(
                text,
                " ev(add={},found={},size={},ptr={},thread={},phase={})",
                event.add, event.found, event.size, event.pointer, event.thread, event.phase
            );
        }
        text
    }
}

struct SyntheticStatus {
    path: std::path::PathBuf,
    id: String,
}
impl crate::services::ProfileBackend for SyntheticStatus {
    fn load(&self, _: &[u16]) -> Result<crate::services::LoadResult, crate::profile::ProfileError> {
        unreachable!("status-only fixture")
    }
    fn save(
        &self,
        _: &[u16],
        _: &crate::profile_state::Json,
        _: Option<&crate::profile_state::Json>,
    ) -> Result<(), crate::profile::ProfileError> {
        unreachable!("status-only fixture")
    }
    fn status(&self) -> Result<Value, crate::profile::ProfileError> {
        unreachable!("typed path only")
    }
    fn leased_status(
        &self,
        budget: &PayloadBudget,
    ) -> Result<crate::services::LeasedProfileStatus, crate::profile::ProfileError> {
        let _scope = allocations::Scope::enter();
        crate::services::LeasedProfileStatus::sqlite(budget, &self.path, 1, 1, &self.id)
    }
    fn kind(&self) -> &'static str {
        "synthetic-status"
    }
    fn display(&self) -> String {
        "synthetic-status".into()
    }
}

// 每个 gate 属于单个 service；无全局开关、环境入口或生产诊断接口。
#[derive(Default)]
pub(crate) struct OutcomeGate {
    pub(crate) ready: Notify,
    pub(crate) resume: Notify,
    command: Notify,
    sent: Notify,
}

pub(in crate::broker) async fn send_profile_command(
    commands: mpsc::Sender<crate::broker::BrokerCommand>,
    command: crate::broker::BrokerCommand,
    gate: Arc<OutcomeGate>,
) {
    let send = commands.send(command);
    tokio::pin!(send);
    std::future::poll_fn(|cx| {
        match send.as_mut().poll(cx) {
            std::task::Poll::Pending => gate.command.notify_one(),
            std::task::Poll::Ready(_) => panic!("real actor command queue must be full"),
        }
        std::task::Poll::Ready(())
    })
    .await;
    send.await.ok().unwrap();
    gate.sent.notify_one();
}

#[derive(Debug)]
pub(crate) struct Presence {
    result_capacity: usize,
    targets_capacity: usize,
    span_capacity: usize,
    slots: usize,
    unknown: usize,
    output_frames: usize,
}

impl crate::broker::Broker {
    pub(crate) fn pressure_presence(&self) -> Presence {
        let active = self.management_active.as_ref();
        let result_capacity = active
            .and_then(|active| active.response.as_ref())
            .map_or(0, |raw| raw.test_capacity());
        let (targets_capacity, span_capacity) = match active.map(|active| &active.stage) {
            Some(crate::broker::ManagedStage::BackfillAttach { targets, .. }) => (
                targets.raw.test_capacity(),
                targets.spans.capacity() * std::mem::size_of::<crate::cdp_raw::RawSpan>(),
            ),
            _ => (0, 0),
        };
        if let Some(crate::broker::ManagedStage::BackfillAttach { targets, .. }) =
            active.map(|active| &active.stage)
        {
            assert_eq!(targets._lease.bytes(), span_capacity);
        }
        for unknown in self.unknown.values() {
            assert!(unknown
                ._request
                .as_ref()
                .is_some_and(|request| request._metadata.bytes() >= 65536));
        }
        Presence {
            result_capacity,
            targets_capacity,
            span_capacity,
            slots: crate::broker::MAX_PENDING_GLOBAL - self.request_slots.available_permits(),
            unknown: self.unknown.len(),
            output_frames: self
                .clients
                .values()
                .map(|client| {
                    crate::broker::CLIENT_QUEUE_FRAMES - client.output_frames.available_permits()
                })
                .sum(),
        }
    }
}

pub(crate) struct Pause {
    pub(crate) presence: oneshot::Sender<Presence>,
    pub(crate) resume: oneshot::Receiver<()>,
}

async fn pause(broker: &BrokerHandle) -> (Presence, oneshot::Sender<()>) {
    let (tx, rx) = oneshot::channel();
    let (resume, wait) = oneshot::channel();
    broker
        .commands
        .send(crate::broker::BrokerCommand::PressurePause(Pause {
            presence: tx,
            resume: wait,
        }))
        .await
        .ok()
        .unwrap();
    (timeout(WAIT, rx).await.unwrap().unwrap(), resume)
}

fn check_budget(budget: &PayloadBudget) {
    let (ordinary, internal) = budget.used();
    let high = budget.high_water();
    assert!(ordinary <= ORDINARY_BYTES && high.0 <= ORDINARY_BYTES);
    assert!(internal <= INTERNAL_BYTES && high.1 <= INTERNAL_BYTES);
    let heap = allocations::stats();
    assert!(!heap.overflow);
    assert!(heap.live <= ordinary + internal);
    assert!(heap.peak <= high.0 + high.1);
}

struct PressureSocket {
    socket: crate::transport::BudgetedSocket<TcpStream>,
    peer: TcpStream,
}

impl PressureSocket {
    async fn new(budget: &PayloadBudget) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (stream, _) = listener.accept().await.unwrap();
        let codec = budget
            .try_acquire(crate::transport::CODEC_BYTES, true)
            .unwrap();
        let socket = tokio_tungstenite::WebSocketStream::from_raw_socket(
            stream,
            tokio_tungstenite::tungstenite::protocol::Role::Server,
            Some(crate::transport::websocket_config()),
        )
        .await;
        Self {
            socket: crate::transport::BudgetedSocket::new(socket, budget.clone(), codec, false),
            peer,
        }
    }

    async fn partial(&mut self) {
        use futures_util::Stream;
        use tokio::io::AsyncWriteExt;
        // 声明 65535 bytes 的 masked text frame，只交付前 1024 bytes。
        let mut frame = vec![0x81, 0xfe, 0xff, 0xff, 0, 0, 0, 0];
        frame.resize(1032, b'x');
        let prefix = br#"{"id":6,"method":"Browser.getVersion","params":{"padding":""#;
        frame[8..8 + prefix.len()].copy_from_slice(prefix);
        self.peer.write_all(&frame).await.unwrap();
        timeout(
            WAIT,
            std::future::poll_fn(|cx| {
                assert!(std::pin::Pin::new(&mut self.socket)
                    .poll_next(cx)
                    .is_pending());
                if self.socket.test_presence().0 {
                    std::task::Poll::Ready(())
                } else {
                    cx.waker().wake_by_ref();
                    std::task::Poll::Pending
                }
            }),
        )
        .await
        .unwrap();
    }

    async fn finish_ingress(&mut self, broker: &BrokerHandle, client: &TestClient) {
        use tokio::io::AsyncWriteExt;
        allocations::phase(allocations::PHASE_INGRESS);
        let mut tail = vec![b'x'; 65535 - 1024];
        let end = tail.len();
        tail[end - 3..].copy_from_slice(b"\"}}");
        self.peer.write_all(&tail).await.unwrap();
        let frame = timeout(WAIT, self.socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let capacity = frame.message.len();
        assert!(broker.leased_frame(client.id, frame).await);
        println!("reader-to-real-frame-queue bytes={capacity}");
    }

    async fn hold_output(&mut self, client: &mut TestClient) -> usize {
        use futures_util::Sink;
        let command = timeout(WAIT, client.frames.recv()).await.unwrap().unwrap();
        let ClientCommand::Text(text, lease) = command else {
            panic!("text output required")
        };
        let bytes = text.capacity();
        std::pin::Pin::new(&mut self.socket)
            .start_send(crate::transport::LeasedMessage {
                message: Message::Text(text.into()),
                lease,
            })
            .unwrap();
        // 等到真实 TCP poll_flush 返回 Pending；不以 sleep 或独立 queue 代替背压。
        timeout(
            WAIT,
            std::future::poll_fn(
                |cx| match std::pin::Pin::new(&mut self.socket).poll_flush(cx) {
                    std::task::Poll::Pending => std::task::Poll::Ready(()),
                    std::task::Poll::Ready(Ok(())) => {
                        panic!("fixture did not create TCP backpressure")
                    }
                    std::task::Poll::Ready(Err(error)) => {
                        panic!("unexpected writer failure: {error}")
                    }
                },
            ),
        )
        .await
        .unwrap();
        assert!(self.socket.test_presence().2 >= bytes);
        bytes
    }
}

async fn settle_actor(broker: &BrokerHandle, engine: &mut FakeEngine) {
    broker.engine_lost();
    assert!(!timeout(WAIT, broker.wait_terminal()).await.unwrap());
    let task = broker.test_task.lock().unwrap().take().unwrap();
    timeout(WAIT, task).await.unwrap().unwrap();
    engine.kill();
    timeout(WAIT, engine.task.take().unwrap())
        .await
        .unwrap()
        .unwrap();
}

struct HttpIo {
    stream: TcpStream,
    blocked: Arc<Notify>,
}
impl tokio::io::AsyncRead for HttpIo {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bytes: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.stream).poll_read(cx, bytes)
    }
}
impl tokio::io::AsyncWrite for HttpIo {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bytes: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let result = std::pin::Pin::new(&mut self.stream).poll_write(cx, bytes);
        if result.is_pending() {
            self.blocked.notify_one();
        }
        result
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.stream).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

struct HttpStall {
    peer: TcpStream,
    task: tokio::task::JoinHandle<Result<(), hyper::Error>>,
}
impl HttpStall {
    async fn start(bytes: bytes::Bytes) -> Self {
        use tokio::io::AsyncWriteExt;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut peer = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (stream, _) = listener.accept().await.unwrap();
        let blocked = Arc::new(Notify::new());
        let io = HttpIo {
            stream,
            blocked: blocked.clone(),
        };
        let response = Arc::new(Mutex::new(Some(bytes)));
        let task = tokio::spawn(async move {
            let service =
                hyper::service::service_fn(move |_: hyper::Request<hyper::body::Incoming>| {
                    let bytes = response.lock().unwrap().take().expect("one HTTP request");
                    async move {
                        Ok::<_, std::convert::Infallible>(hyper::Response::new(
                            http_body_util::Full::new(bytes),
                        ))
                    }
                });
            hyper::server::conn::http1::Builder::new()
                .serve_connection(hyper_util::rt::TokioIo::new(io), service)
                .await
        });
        peer.write_all(b"GET /fixture HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        timeout(WAIT, blocked.notified()).await.unwrap();
        Self { peer, task }
    }
    async fn finish(self, cancel: bool) {
        use tokio::io::AsyncReadExt;
        if cancel {
            drop(self.peer);
            assert!(timeout(WAIT, self.task).await.unwrap().unwrap().is_err());
        } else {
            let mut peer = self.peer;
            let mut chunk = [0; 8192];
            timeout(WAIT, async {
                while peer.read(&mut chunk).await.unwrap() != 0 {}
            })
            .await
            .unwrap();
            timeout(WAIT, self.task).await.unwrap().unwrap().unwrap();
        }
    }
}

// pump_flush 等 management 排空，pump_management 与 pump_retirements 互相阻挡。
// 因此真实 flush command 与 backfill、未结 retirement 与 backfill 分开测；不造 actor owner。
#[derive(Clone, Copy, Debug)]
enum Branch {
    Complete,
    Reject,
    BackfillFailure,
    CancelAndLost,
    CancelAndLostResolved,
}

async fn output_reply(
    engine: &mut FakeEngine,
    broker: &BrokerHandle,
    client: &TestClient,
    id: u64,
    padding: &str,
) {
    send(
        client,
        broker,
        &format!(r#"{{"id":{id},"method":"Browser.getVersion"}}"#),
    )
    .await;
    let request = next_request(engine).await;
    let _fixture = allocations::Scope::suspend();
    engine.raw(format!(
        r#"{{"id":{},"result":{{"padding":"{padding}"}}}}"#,
        request_id(&request)
    ));
}

async fn combined_window(branch: Branch, padding: &str, path: std::path::PathBuf, id: String) {
    use futures_util::Sink;
    let mut engine = fake_engine().await;
    let observer = Arc::new(Observer::disabled());
    let upstream = Upstream::connect(&UpstreamAddress::Tcp(engine.address), observer.clone())
        .await
        .unwrap();
    let profile = Arc::new(ProfileService::new(Arc::new(SyntheticStatus { path, id })));
    let gate = Arc::new(OutcomeGate::default());
    *profile.outcome_gate.lock().unwrap() = Some(gate.clone());
    let broker = BrokerHandle::start_with_deadlines(
        upstream,
        profile.clone(),
        observer,
        Duration::from_millis(100),
        Duration::from_secs(30),
    );
    let budget = broker.payload_budget();
    allocations::phase(allocations::PHASE_SETUP);
    let mut viewer = connect(&broker, Endpoint::Browser, Role::Viewer, "viewer").await;
    let mut manager = connect(&broker, Endpoint::Browser, Role::Controller, "owner").await;
    let mut slow = connect(&broker, Endpoint::Browser, Role::Viewer, "slow").await;
    let mut cleanup = connect(&broker, Endpoint::Browser, Role::Viewer, "cleanup").await;
    send(
        &cleanup,
        &broker,
        r#"{"id":40,"method":"Target.attachToTarget","params":{"targetId":"cleanup-target"}}"#,
    )
    .await;
    let cleanup_attach = next_request(&mut engine).await;
    engine.reply(
        request_id(&cleanup_attach),
        json!({"sessionId":"cleanup-session"}),
    );
    assert_eq!(next_frame(&mut cleanup).await["id"], 40);
    let mut socket = PressureSocket::new(&budget).await;
    socket.partial().await;
    allocations::phase(allocations::PHASE_UNKNOWN);
    send(
        &viewer,
        &broker,
        r#"{"id":1,"method":"Browser.getVersion"}"#,
    )
    .await;
    let unknown = next_request(&mut engine).await;
    assert!(message(&next_frame(&mut viewer).await).contains("outcome may be unknown"));
    assert_eq!(broker.budget_snapshot().await.2, 1);
    allocations::phase(allocations::PHASE_SLOW);
    output_reply(&mut engine, &broker, &slow, 2, padding).await;
    let writer_bytes = socket.hold_output(&mut slow).await;
    output_reply(&mut engine, &broker, &slow, 3, padding).await;
    allocations::phase(allocations::PHASE_BACKFILL);
    send(
        &manager,
        &broker,
        r#"{"id":4,"method":"Target.setAutoAttach","params":{"autoAttach":true,"flatten":true}}"#,
    )
    .await;
    let auto = next_request(&mut engine).await;
    {
        let _fixture = allocations::Scope::suspend();
        engine.raw(format!(
            r#"{{"id":{},"result":{{"padding":"{}"}}}}"#,
            request_id(&auto),
            &padding[..65536]
        ));
    }
    let targets = next_request(&mut engine).await;
    assert_eq!(targets["method"], "Target.getTargets");
    engine.reply(
        request_id(&targets),
        json!({"targetInfos":[{"targetId":"one","type":"page"},{"targetId":"two","type":"page"}]}),
    );
    let attach = next_request(&mut engine).await;
    assert_eq!(attach["method"], "Target.attachToTarget");
    let http_bytes = crate::relay::budgeted_body(&budget, &padding).unwrap();
    let http_last = http_bytes.clone();
    let http_stall = HttpStall::start(http_bytes).await;
    let earlier_status = profile.leased_status(budget.clone());
    // 先建立一个 HTTP 最后 Bytes owner，再与下一次 worker outcome 共同持有。
    *profile.outcome_gate.lock().unwrap() = None;
    let earlier_status = earlier_status.await.unwrap();
    let earlier_body = crate::relay::budgeted_body(&budget, &earlier_status).unwrap();
    let earlier_last = earlier_body.clone();
    drop(earlier_body);
    drop(earlier_status);
    *profile.outcome_gate.lock().unwrap() = Some(gate.clone());
    allocations::phase(allocations::PHASE_STATUS);
    let status_task = {
        let profile = profile.clone();
        let budget = budget.clone();
        tokio::spawn(async move { profile.leased_status(budget).await })
    };
    timeout(WAIT, gate.ready.notified()).await.unwrap();
    let (presence, resume) = pause(&broker).await;
    assert!(presence.result_capacity >= 65536);
    assert!(presence.targets_capacity > 0);
    assert_eq!(presence.span_capacity, 2 * 16);
    assert_eq!(presence.unknown, 1);
    assert_eq!(presence.slots, 2);
    assert_eq!(presence.output_frames, 2);
    assert!(socket.socket.test_presence().0);
    assert_eq!(socket.socket.test_presence().3, 1);
    let held_outcome = budget.used();
    gate.resume.notify_one();
    let status = status_task.await.unwrap().unwrap();
    let before_body = budget.used();
    let body = crate::relay::budgeted_body(&budget, &status).unwrap();
    let last = body.clone();
    drop(body);
    assert!(budget.used().0 > before_body.0 + last.len());
    drop(status);
    let body_held = budget.used();
    check_budget(&budget);
    println!("combined {branch:?} presence={presence:?} partial={:?} writer_backing={writer_bytes} outcome={held_outcome:?} body={body_held:?} high={:?} heap={:?}", socket.socket.test_presence(), budget.high_water(), allocations::stats());
    resume.send(()).unwrap();
    if matches!(branch, Branch::Reject) {
        let occupied = budget
            .try_acquire(ORDINARY_BYTES - budget.used().0, false)
            .unwrap();
        assert!(crate::relay::budgeted_body(&budget, &false).is_none());
        assert!(budget.try_acquire(1, false).is_none());
        assert!(broker.session().await.unwrap().alive);
        assert_eq!(
            receive_ack(&broker, r#"{"method":"Unknown.opaque","params":{}}"#).await,
            (true, 1)
        );
        engine.reply(request_id(&unknown), json!({}));
        timeout(WAIT, async {
            while broker.budget_snapshot().await.2 != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(occupied);
        assert!(crate::relay::budgeted_body(&budget, &false).is_some());
        send(
            &viewer,
            &broker,
            r#"{"id":5,"method":"Browser.getVersion"}"#,
        )
        .await;
        let healthy = next_request(&mut engine).await;
        engine.reply(request_id(&healthy), json!({}));
        assert_eq!(next_frame(&mut viewer).await["id"], 5);
    }
    if matches!(
        branch,
        Branch::CancelAndLost | Branch::CancelAndLostResolved
    ) {
        allocations::phase(allocations::PHASE_CANCEL);
        drop(socket.peer);
        timeout(
            WAIT,
            std::future::poll_fn(|cx| std::pin::Pin::new(&mut socket.socket).poll_flush(cx)),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert_eq!(socket.socket.test_presence().3, 1);
        broker.writer_gone(slow.id);
        broker.closed(viewer.id);
        assert_eq!(broker.budget_snapshot().await.2, 1);
        if matches!(branch, Branch::CancelAndLostResolved) {
            allocations::phase(allocations::PHASE_RESOLVED);
            engine.reply(request_id(&attach), json!({"sessionId":"first"}));
            let second = next_method(&mut engine, "Target.attachToTarget").await;
            engine.reply(request_id(&second), json!({"sessionId":"second"}));
            assert_eq!(next_frame(&mut manager).await["id"], 4);
            let (finished, resume) = pause(&broker).await;
            assert_eq!(finished.result_capacity, 0);
            assert_eq!(finished.targets_capacity, 0);
            assert_eq!(finished.span_capacity, 0);
            assert_eq!(finished.unknown, 1);
            assert_eq!(finished.slots, 1);
            resume.send(()).unwrap();
            allocations::phase(allocations::PHASE_LATE);
            engine.reply(request_id(&unknown), json!({}));
            timeout(WAIT, async {
                while broker.budget_snapshot().await.2 != 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            let (cleared, resume) = pause(&broker).await;
            assert_eq!(cleared.unknown, 0);
            assert_eq!(cleared.slots, 0);
            resume.send(()).unwrap();
        }
    } else {
        if matches!(branch, Branch::BackfillFailure) {
            engine.push(json!({"id":request_id(&attach),"error":{"message":"fixture failure"}}));
        } else {
            engine.reply(request_id(&attach), json!({"sessionId":"first"}));
            let second = next_method(&mut engine, "Target.attachToTarget").await;
            engine.reply(request_id(&second), json!({"sessionId":"second"}));
        }
        assert_eq!(next_frame(&mut manager).await["id"], 4);
        let (finished, resume) = pause(&broker).await;
        assert_eq!(finished.result_capacity, 0);
        assert_eq!(finished.targets_capacity, 0);
        assert_eq!(finished.span_capacity, 0);
        resume.send(()).unwrap();
        if matches!(branch, Branch::Reject) {
            let occupied = budget
                .try_acquire(ORDINARY_BYTES - budget.used().0, false)
                .unwrap();
            broker.closed(cleanup.id);
            let detach = next_method(&mut engine, "Target.detachFromTarget").await;
            assert_eq!(detach["params"]["sessionId"], "cleanup-session");
            engine.reply(request_id(&detach), json!({}));
            timeout(WAIT, async {
                while broker.budget_snapshot().await.3 != 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert!(broker.session().await.unwrap().alive);
            drop(occupied);
        }
        engine.reply(request_id(&unknown), json!({}));
        timeout(WAIT, async {
            while broker.budget_snapshot().await.2 != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        socket.finish_ingress(&broker, &viewer).await;
        let ingress = next_method(&mut engine, "Browser.getVersion").await;
        engine.reply(request_id(&ingress), json!({}));
        assert_eq!(next_frame(&mut viewer).await["id"], 6);
        drop(socket.peer);
    }
    allocations::phase(allocations::PHASE_TEARDOWN);
    drop(socket.socket);
    drop(last);
    let before_http_finish = budget.used();
    http_stall
        .finish(matches!(
            branch,
            Branch::CancelAndLost | Branch::CancelAndLostResolved
        ))
        .await;
    assert_eq!(budget.used().0, before_http_finish.0);
    drop(http_last);
    drop(earlier_last);
    drop(slow);
    drop(cleanup);
    drop(manager);
    drop(viewer);
    allocations::phase(allocations::PHASE_SETTLE);
    settle_actor(&broker, &mut engine).await;
    assert_eq!(budget.used(), (MAX_FRAME_BYTES, 0));
    check_budget(&budget);
    println!(
        "settled {branch:?} payload={:?} high={:?}",
        budget.used(),
        budget.high_water()
    );
    drop(broker);
    drop(profile);
    drop(gate);
    drop(engine);
    drop(budget);
}

#[test]
fn actor_reachable_combined_windows() {
    let _serial = allocations::SERIAL.lock().unwrap();
    assert_eq!(ORDINARY_BYTES, 128 * 1024 * 1024);
    assert_eq!(INTERNAL_BYTES, 20 * 1024 * 1024);
    assert_eq!(MAX_FRAME_BYTES, 16 * 1024 * 1024);
    for branch in [
        Branch::Complete,
        Branch::Reject,
        Branch::BackfillFailure,
        Branch::CancelAndLost,
    ] {
        run_combined(branch);
    }
}

// 独立可达分支：writer 失败与 viewer 关闭后，backfill 完成、late reply 才清 unknown。
// 独立验证首轮曾见 128 B 残留；本地复跑未复现，保留全部断言与残留归因输出，不提前清 identity。
#[test]
fn actor_cancel_keeps_unknown_until_late_reply() {
    let _serial = allocations::SERIAL.lock().unwrap();
    assert_eq!(ORDINARY_BYTES, 128 * 1024 * 1024);
    assert_eq!(INTERNAL_BYTES, 20 * 1024 * 1024);
    assert_eq!(MAX_FRAME_BYTES, 16 * 1024 * 1024);
    run_combined(Branch::CancelAndLostResolved);
}

fn run_combined(branch: Branch) {
    let padding = "x".repeat(8 * 1024 * 1024);
    let path = std::path::PathBuf::from("/状态/".repeat(8192));
    let id = "synthetic".repeat(8192);
    allocations::reset();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async { tokio::task::spawn_blocking(|| {}).await.unwrap() });
        let scope = allocations::Scope::enter();
        runtime.block_on(combined_window(branch, &padding, path, id));
        allocations::phase(allocations::PHASE_RUNTIME);
        drop(runtime);
        drop(scope);
    })
    .join()
    .unwrap();
    let stats = allocations::stats();
    assert!(!stats.overflow);
    assert_eq!(stats.live, 0, "{}", allocations::describe());
    assert!(stats.cross_thread_frees > 0);
    println!("combined-final {branch:?} {stats:?}");
}

// 64 B 来自 PayloadLease 的 hold 向量：owner 是 lease，drop 时释放并归还 permits。
// 预算回到基线仍见 64 B 残留时，不能归因于仍存活的 lease（permit 会占预算）。
#[test]
fn lease_hold_small_owner_is_tracked_and_released() {
    let _serial = allocations::SERIAL.lock().unwrap();
    allocations::reset();
    let held = std::thread::spawn(|| {
        let budget = crate::transport::PayloadBudget::default();
        let slots = Arc::new(Semaphore::new(4));
        let scope = allocations::Scope::enter();
        allocations::phase(allocations::PHASE_SLOW);
        let mut lease = budget.try_acquire(64, false).unwrap();
        lease.hold(slots.clone().try_acquire_owned().unwrap());
        lease.hold(slots.clone().try_acquire_owned().unwrap());
        let (records, count) = allocations::records();
        assert!(
            records[..count].iter().any(|record| record.size == 64),
            "{}",
            allocations::describe()
        );
        let held = allocations::stats().live;
        drop(lease);
        drop(scope);
        held
    })
    .join()
    .unwrap();
    assert_eq!(held, 64);
    assert_eq!(allocations::stats().live, 0, "{}", allocations::describe());
}

#[test]
fn flush_real_command_queue_callback_and_http_cancel() {
    let _serial = allocations::SERIAL.lock().unwrap();
    for cancel in [false, true] {
        allocations::reset();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async { tokio::task::spawn_blocking(|| {}).await.unwrap() });
            let scope = allocations::Scope::enter();
            runtime.block_on(flush_window(cancel));
            allocations::phase(allocations::PHASE_RUNTIME);
            drop(runtime);
            drop(scope);
        })
        .join()
        .unwrap();
        let stats = allocations::stats();
        assert_eq!(stats.live, 0, "{}", allocations::describe());
        assert!(!stats.overflow);
        println!("flush-final cancel={cancel} {stats:?}");
    }
}

async fn flush_window(cancel: bool) {
    let mut engine = fake_engine().await;
    let observer = Arc::new(Observer::disabled());
    let upstream = Upstream::connect(&UpstreamAddress::Tcp(engine.address), observer.clone())
        .await
        .unwrap();
    let profile = Arc::new(ProfileService::new(Arc::new(SyntheticStatus {
        path: "/synthetic".into(),
        id: "fixture".into(),
    })));
    let gate = Arc::new(OutcomeGate::default());
    *profile.outcome_gate.lock().unwrap() = Some(gate.clone());
    let broker = BrokerHandle::start_with_deadlines(
        upstream,
        profile.clone(),
        observer,
        Duration::from_millis(100),
        Duration::from_secs(30),
    );
    let budget = broker.payload_budget();
    let mut viewer = connect(&broker, Endpoint::Browser, Role::Viewer, "unknown-flush").await;
    send(
        &viewer,
        &broker,
        r#"{"id":30,"method":"Browser.getVersion"}"#,
    )
    .await;
    let unknown = next_request(&mut engine).await;
    assert!(message(&next_frame(&mut viewer).await).contains("outcome may be unknown"));
    let mut slow = connect(&broker, Endpoint::Browser, Role::Viewer, "slow-flush").await;
    let mut socket = PressureSocket::new(&budget).await;
    socket.partial().await;
    let padding = {
        let _fixture = allocations::Scope::suspend();
        "f".repeat(8 * 1024 * 1024)
    };
    output_reply(&mut engine, &broker, &slow, 20, &padding).await;
    socket.hold_output(&mut slow).await;
    output_reply(&mut engine, &broker, &slow, 21, &padding).await;
    let http_bytes = crate::relay::budgeted_body(&budget, &padding).unwrap();
    let http_last = http_bytes.clone();
    let http_stall = HttpStall::start(http_bytes).await;
    let callback = {
        let broker = broker.clone();
        tokio::spawn(async move { broker.flush().await })
    };
    let request = next_method(&mut engine, "Kitesurf.flushProfile").await;
    engine.reply(request_id(&request), json!({}));
    timeout(WAIT, gate.ready.notified()).await.unwrap();
    let (presence, resume) = pause(&broker).await;
    assert_eq!(presence.result_capacity, 0);
    assert_eq!(presence.span_capacity, 0);
    assert_eq!(presence.unknown, 1);
    assert_eq!(presence.slots, 1);
    assert_eq!(presence.output_frames, 2);
    assert!(socket.socket.test_presence().0);
    let held = budget.used();
    let mut replies = Vec::with_capacity(crate::broker::COMMAND_CAPACITY);
    for _ in 0..crate::broker::COMMAND_CAPACITY {
        let (tx, rx) = oneshot::channel();
        broker
            .commands
            .try_send(crate::broker::BrokerCommand::Session { reply: tx })
            .ok()
            .unwrap();
        replies.push(rx);
    }
    assert_eq!(broker.commands.capacity(), 0);
    gate.resume.notify_one();
    timeout(WAIT, gate.command.notified()).await.unwrap();
    assert_eq!(budget.used(), held);
    check_budget(&budget);
    println!("flush-command cancel={cancel} presence={presence:?} command_capacity={} partial={:?} held={held:?} high={:?} heap={:?}", broker.commands.capacity(), socket.socket.test_presence(), budget.high_water(), allocations::stats());
    if cancel {
        callback.abort();
        assert!(matches!(callback.await, Err(error) if error.is_cancelled()));
    } else {
        resume.send(()).unwrap();
        timeout(WAIT, gate.sent.notified()).await.unwrap();
        let status = callback.await.unwrap().unwrap();
        assert_eq!(budget.used().0, held.0);
        let body = crate::relay::budgeted_body(&budget, &status).unwrap();
        let last = body.clone();
        drop(body);
        drop(status);
        assert!(budget.used().0 > MAX_FRAME_BYTES + last.len());
        drop(last);
        engine.reply(request_id(&unknown), json!({}));
        http_stall.finish(false).await;
        drop(http_last);
        drop(viewer);
        drop(socket);
        drop(slow);
        drop(padding);
        drop(replies);
        return finish_flush_window(broker, profile, gate, budget, engine, cancel).await;
    }
    resume.send(()).unwrap();
    timeout(WAIT, gate.sent.notified()).await.unwrap();
    drop(replies);
    assert!(broker.session().await.unwrap().alive);
    // 调用真实 /session handler；在 worker outcome 暂停时取消并 join。
    let handler = {
        let broker = broker.clone();
        let profile = profile.clone();
        tokio::spawn(async move { crate::relay::test_session_body(broker, profile).await })
    };
    let targets = next_method(&mut engine, "Target.getTargets").await;
    engine.reply(request_id(&targets), json!({"targetInfos":[]}));
    timeout(WAIT, gate.ready.notified()).await.unwrap();
    handler.abort();
    assert!(handler.await.unwrap_err().is_cancelled());
    broker.closed(viewer.id);
    assert_eq!(broker.budget_snapshot().await.2, 1);
    http_stall.finish(true).await;
    drop(http_last);
    drop(viewer);
    drop(unknown);
    drop(socket);
    drop(slow);
    drop(padding);
    finish_flush_window(broker, profile, gate, budget, engine, cancel).await;
}

async fn finish_flush_window(
    broker: BrokerHandle,
    profile: Arc<ProfileService>,
    gate: Arc<OutcomeGate>,
    budget: PayloadBudget,
    mut engine: FakeEngine,
    cancel: bool,
) {
    check_budget(&budget);
    settle_actor(&broker, &mut engine).await;
    assert_eq!(budget.used(), (MAX_FRAME_BYTES, 0));
    println!(
        "flush cancel={cancel} payload={:?} high={:?} heap={:?}",
        budget.used(),
        budget.high_water(),
        allocations::stats()
    );
    drop(engine);
    drop(broker);
    drop(profile);
    drop(gate);
    drop(budget);
}

#[test]
fn cross_thread_all_sizes_realloc_and_join_settle() {
    let _serial = allocations::SERIAL.lock().unwrap();
    allocations::reset();
    let worker = std::thread::spawn(|| {
        let _scope = allocations::Scope::enter();
        let mut bytes = Vec::with_capacity(3);
        bytes.extend_from_slice(&[1, 2, 3]);
        bytes.reserve_exact(8193);
        bytes
    });
    let mut bytes = worker.join().unwrap();
    let held = allocations::stats();
    assert!(held.live >= bytes.capacity());
    let old = bytes.capacity();
    bytes.reserve_exact(old + 1);
    assert!(allocations::stats().peak >= old + bytes.capacity());
    std::thread::spawn(move || drop(bytes)).join().unwrap();
    let final_stats = allocations::stats();
    assert!(!final_stats.overflow);
    assert_eq!(final_stats.live, 0);
    assert!(final_stats.cross_thread_frees > 0);
    assert!(final_stats.peak >= 3 + 8196);
    println!("cross-thread-all-sizes {final_stats:?}");
}
