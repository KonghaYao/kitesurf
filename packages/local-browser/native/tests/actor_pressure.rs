// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

use super::*;
use std::sync::Mutex;
use crate::transport::{PayloadBudget, INTERNAL_BYTES, MAX_FRAME_BYTES, ORDINARY_BYTES};
use tokio::sync::{oneshot, Notify, Semaphore};

// 固定表覆盖场景线程、blocking worker 与任意线程的 free/realloc；不忽略小 allocation。
// runtime 与有限 harness 也在观测值中；fixture/伪上游排除，不是 SQLite/native/RSS 保证。
// scope 只决定新分配是否入表；释放不依赖 scope，线程 join 后必须 live=0。
// 固定数组不分配；phase 是时间窗，owner 仅显式测试标签，丢失由 snapshot flags 报告。
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

    // owner 只在测试显式 scope 内设置；未标注的分配不能由 phase 反推 owner。
    pub const OWNER_NONE: u32 = 0;
    pub const OWNER_LEASE_HOLD: u32 = 1;
    thread_local! {
        static OWNER: Cell<u32> = const { Cell::new(OWNER_NONE) };
    }
    pub struct OwnerScope(u32);
    pub fn owner_scope(value: u32) -> OwnerScope {
        OwnerScope(OWNER.with(|owner| owner.replace(value)))
    }
    impl Drop for OwnerScope {
        fn drop(&mut self) {
            OWNER.with(|owner| owner.set(self.0));
        }
    }
    fn owner() -> u32 {
        OWNER.try_with(Cell::get).unwrap_or(OWNER_NONE)
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
        pub pointer: usize,
        pub size: usize,
        pub thread: usize,
        pub phase: u32,
        pub owner: u32,
        pub seq: u64,
    }
    impl Record {
        const EMPTY: Self = Self {
            pointer: 0,
            size: 0,
            thread: 0,
            phase: 0,
            owner: OWNER_NONE,
            seq: 0,
        };
    }
    #[derive(Clone, Copy, Debug, Default)]
    pub struct Event {
        pub add: bool,
        pub found: bool,
        pub size: usize,
        pub pointer: usize,
        pub thread: usize,
        pub phase: u32,
        pub owner: u32,
        pub seq: u64,
    }
    impl Event {
        const EMPTY: Self = Self {
            add: false,
            found: false,
            size: 0,
            pointer: 0,
            thread: 0,
            phase: 0,
            owner: OWNER_NONE,
            seq: 0,
        };
    }
    // 容量有限时用 flags 与总数报告丢失；records 的 live 是快照时真实 live 条数。
    #[derive(Clone, Copy, Debug)]
    pub struct Records {
        pub items: [Record; RECORDS],
        pub count: usize,
        pub live: usize,
        pub truncated: bool,
    }
    // 只覆盖 <=SMALL 的事件；total 是写入总数，count 是当前可读窗口。
    #[derive(Clone, Copy, Debug)]
    pub struct Events {
        pub items: [Event; EVENTS],
        pub count: usize,
        pub total: u64,
        pub overwritten: bool,
    }
    #[derive(Clone, Copy, Debug)]
    pub struct Snapshot {
        pub stats: Stats,
        pub records: Records,
        pub events: Events,
    }
    const SMALL: usize = 128;
    pub const EVENTS: usize = 256;
    pub const RECORDS: usize = 32;
    #[derive(Clone, Copy)]
    struct Slot {
        pointer: usize,
        size: usize,
        thread: usize,
        phase: u32,
        owner: u32,
        seq: u64,
    }
    impl Slot {
        const EMPTY: Self = Self {
            pointer: 0,
            size: 0,
            thread: 0,
            phase: 0,
            owner: OWNER_NONE,
            seq: 0,
        };
        const GONE: Self = Self {
            pointer: usize::MAX,
            size: 0,
            thread: 0,
            phase: 0,
            owner: OWNER_NONE,
            seq: 0,
        };
        fn live(&self) -> bool {
            !matches!(self.pointer, 0 | usize::MAX)
        }
        fn record(&self) -> Record {
            Record {
                pointer: self.pointer,
                size: self.size,
                thread: self.thread,
                phase: self.phase,
                owner: self.owner,
                seq: self.seq,
            }
        }
    }
    struct Tracker {
        stats: Stats,
        seq: u64,
        slots: [Slot; 32768],
        events: [Event; EVENTS],
        event_head: usize,
        events_total: u64,
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
        slots: [Slot::EMPTY; 32768],
        events: [Event::EMPTY; EVENTS],
        event_head: 0,
        events_total: 0,
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
                if self.slots[index].pointer == pointer {
                    return Some(index);
                }
                if self.slots[index].pointer == 0 {
                    return None;
                }
            }
            None
        }
        fn event(
            &mut self,
            add: bool,
            found: bool,
            size: usize,
            pointer: usize,
            seq: u64,
            owner: u32,
        ) {
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
                owner,
                seq,
            };
            self.event_head = (self.event_head + 1) % EVENTS;
            self.events_total = self.events_total.saturating_add(1);
        }
        fn add(&mut self, pointer: usize, size: usize) {
            if pointer == 0 {
                return;
            }
            self.stats.live += size;
            self.stats.peak = self.stats.peak.max(self.stats.live);
            self.stats.count += 1;
            self.seq = self.seq.wrapping_add(1);
            let seq = self.seq;
            let phase = PHASE.load(Ordering::Relaxed);
            let owner = owner();
            let start = (pointer >> 4).wrapping_mul(0x9e3779b1) % self.slots.len();
            for offset in 0..self.slots.len() {
                let index = (start + offset) % self.slots.len();
                if !self.slots[index].live() {
                    self.slots[index] = Slot {
                        pointer,
                        size,
                        thread: thread(),
                        phase,
                        owner,
                        seq,
                    };
                    LIVE.fetch_add(1, Ordering::Relaxed);
                    self.event(true, true, size, pointer, seq, owner);
                    return;
                }
            }
            self.stats.overflow = true;
            self.event(true, false, size, pointer, seq, owner);
        }
        fn remove(&mut self, pointer: usize) -> bool {
            if let Some(index) = self.index(pointer) {
                let slot = self.slots[index];
                self.stats.live -= slot.size;
                self.stats.cross_thread_frees += usize::from(slot.thread != thread());
                self.slots[index] = Slot::GONE;
                LIVE.fetch_sub(1, Ordering::Relaxed);
                self.event(false, true, slot.size, pointer, slot.seq, slot.owner);
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
            tracker.event(false, false, size, pointer as usize, 0, owner());
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
        tracker.slots.fill(Slot::EMPTY);
        tracker.events.fill(Event::EMPTY);
        tracker.event_head = 0;
        tracker.events_total = 0;
    }
    pub fn stats() -> Stats {
        TRACKER.lock().unwrap().stats
    }
    impl Tracker {
        fn record_snapshot(&self) -> Records {
            let mut items = [Record::EMPTY; RECORDS];
            let mut live = 0;
            for slot in &self.slots {
                if !slot.live() {
                    continue;
                }
                live += 1;
                if live <= RECORDS {
                    items[live - 1] = slot.record();
                }
            }
            Records {
                items,
                count: live.min(RECORDS),
                live,
                truncated: live > RECORDS,
            }
        }
        fn event_snapshot(&self) -> Events {
            let total = self.events_total;
            let count = total.min(EVENTS as u64) as usize;
            let start = if total >= EVENTS as u64 {
                self.event_head
            } else {
                0
            };
            let mut items = [Event::EMPTY; EVENTS];
            for (offset, event) in items[..count].iter_mut().enumerate() {
                *event = self.events[(start + offset) % EVENTS];
            }
            Events {
                items,
                count,
                total,
                overwritten: total > EVENTS as u64,
            }
        }
    }
    // 单次加锁取得一致的 stats/records/events；不返回被覆盖区间，也不隐藏省略数。
    pub fn snapshot() -> Snapshot {
        let tracker = TRACKER.lock().unwrap();
        Snapshot {
            stats: tracker.stats,
            records: tracker.record_snapshot(),
            events: tracker.event_snapshot(),
        }
    }
    // 只用固定快照；格式化在释放 tracker 锁并暂停 tracking 后，避免递归分配。
    pub fn describe() -> String {
        use std::fmt::Write as _;
        let view = snapshot();
        let _quiet = Scope::suspend();
        let mut text = format!(
            "{:?} records=(shown={},live={},truncated={},omitted={}) events=(shown={},total={},overwritten={},dropped={}) attribution=(phase=global-window,owner=test-tag-only)",
            view.stats,
            view.records.count,
            view.records.live,
            view.records.truncated,
            view.records.live - view.records.count,
            view.events.count,
            view.events.total,
            view.events.overwritten,
            view.events.total - view.events.count as u64,
        );
        for record in &view.records.items[..view.records.count] {
            let _ = write!(
                text,
                " rec(ptr={},size={},thread={},phase={},owner={},seq={})",
                record.pointer, record.size, record.thread, record.phase, record.owner, record.seq
            );
        }
        for event in &view.events.items[..view.events.count] {
            let _ = write!(
                text,
                " ev(add={},found={},size={},ptr={},thread={},phase={},owner={},seq={})",
                event.add,
                event.found,
                event.size,
                event.pointer,
                event.thread,
                event.phase,
                event.owner,
                event.seq
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
    pub(crate) sent: Notify,
}

pub(in crate::broker) async fn send_profile_command(
    commands: mpsc::Sender<crate::broker::BrokerCommand>,
    command: crate::broker::BrokerCommand,
    gate: Arc<OutcomeGate>,
) {
    // 队列已满时等真实 actor 腾出容量；未满时直接入队，两种都由 sent 证明可观测。
    let command = match commands.try_send(command) {
        Ok(()) => {
            gate.sent.notify_one();
            return;
        }
        Err(mpsc::error::TrySendError::Full(command)) => {
            gate.command.notify_one();
            command
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            panic!("actor command queue closed")
        }
    };
    commands.send(command).await.ok().unwrap();
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

const PRESSURE_PAYLOAD_BYTES: usize = 8 * 1024 * 1024;
const PRESSURE_CLIENT_DEADLINE: Duration = Duration::from_secs(2);

async fn loopback_with_backpressure() -> (TcpStream, TcpStream) {
    use tokio::net::TcpSocket;

    // Limit both ends before the handshake. Loopback autotuning can otherwise buffer
    // the entire 8 MiB reply on some kernels even while the peer is not reading.
    const BUFFER_BYTES: u32 = 256 * 1024;
    let listener = TcpSocket::new_v4().unwrap();
    listener.set_send_buffer_size(BUFFER_BYTES).unwrap();
    listener.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let listener = listener.listen(1).unwrap();
    let peer = TcpSocket::new_v4().unwrap();
    peer.set_recv_buffer_size(BUFFER_BYTES).unwrap();
    let peer = peer.connect(listener.local_addr().unwrap()).await.unwrap();
    let (stream, _) = listener.accept().await.unwrap();
    (stream, peer)
}

struct PressureSocket {
    socket: crate::transport::BudgetedSocket<TcpStream>,
    peer: TcpStream,
}

impl PressureSocket {
    async fn new(budget: &PayloadBudget) -> Self {
        let (stream, peer) = loopback_with_backpressure().await;
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
        assert!(
            text.len() >= PRESSURE_PAYLOAD_BYTES,
            "fixture expected a full reply, got {} bytes: {:?}",
            text.len(),
            text.get(..text.len().min(160))
        );
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
        let (stream, mut peer) = loopback_with_backpressure().await;
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
        PRESSURE_CLIENT_DEADLINE,
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
    // Keep the withheld reply long enough to expire; large real replies need their full deadline.
    let expired = match timeout(PRESSURE_CLIENT_DEADLINE + WAIT, viewer.frames.recv())
        .await
        .unwrap()
        .unwrap()
    {
        ClientCommand::Text(text, _lease) => serde_json::from_str(&text).unwrap(),
        _ => panic!("the unanswered request must return a timeout reply"),
    };
    assert!(message(&expired).contains("outcome may be unknown"));
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
    let padding = "x".repeat(PRESSURE_PAYLOAD_BYTES);
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
// owner 标签由测试在真实 hold 入口前设置，不是生产调用点的自动归因。
// 两 semaphore 首次 release 会惰性分配 64 B；在 tracking 外预热，避免混入 hold 归因。
#[test]
fn lease_hold_small_owner_is_tracked_and_released() {
    let _serial = allocations::SERIAL.lock().unwrap();
    allocations::reset();
    let held = std::thread::spawn(|| {
        let budget = crate::transport::PayloadBudget::default();
        let slots = Arc::new(Semaphore::new(4));
        drop(budget.try_acquire(64, false).unwrap());
        drop(slots.clone().try_acquire_owned().unwrap());
        let scope = allocations::Scope::enter();
        allocations::phase(allocations::PHASE_SLOW);
        let mut lease = budget.try_acquire(64, false).unwrap();
        let permit = slots.clone().try_acquire_owned().unwrap();
        let baseline = allocations::snapshot();
        {
            let _owner = allocations::owner_scope(allocations::OWNER_LEASE_HOLD);
            lease.hold(permit);
        }
        let tagged = allocations::snapshot();
        assert_eq!(tagged.records.live, baseline.records.live + 1);
        assert_eq!(tagged.stats.live, baseline.stats.live + 64);
        let record = tagged.records.items[..tagged.records.count]
            .iter()
            .find(|record| record.owner == allocations::OWNER_LEASE_HOLD)
            .unwrap_or_else(|| panic!("{}", allocations::describe()));
        assert_eq!(record.size, 64);
        let (pointer, seq) = (record.pointer, record.seq);
        let held = tagged.stats.live;
        drop(lease);
        let released = allocations::snapshot();
        assert_eq!(
            released.stats.live,
            baseline.stats.live,
            "{}",
            allocations::describe()
        );
        assert!(released.records.items[..released.records.count]
            .iter()
            .all(|record| record.seq != seq));
        let free = released.events.items[..released.events.count]
            .iter()
            .rev()
            .find(|event| !event.add && event.found && event.seq == seq)
            .unwrap_or_else(|| panic!("{}", allocations::describe()));
        assert_eq!(free.pointer, pointer);
        assert_eq!(free.owner, allocations::OWNER_LEASE_HOLD);
        drop(scope);
        held - baseline.stats.live
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
        PRESSURE_CLIENT_DEADLINE,
        Duration::from_secs(30),
    );
    let budget = broker.payload_budget();
    let mut slow = connect(&broker, Endpoint::Browser, Role::Viewer, "slow-flush").await;
    let mut socket = PressureSocket::new(&budget).await;
    socket.partial().await;
    let padding = {
        let _fixture = allocations::Scope::suspend();
        "f".repeat(PRESSURE_PAYLOAD_BYTES)
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
    // 修正后的屏障不会在 flush 已发出时保留未结算客户端结果；
    // unknown 元数据压力由 combined_window 覆盖。
    assert_eq!(presence.unknown, 0);
    assert_eq!(presence.slots, 0);
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
        // abort 是 caller 取消，不是 2s/30s request timeout；actor 仍须结算。
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
        http_stall.finish(false).await;
        drop(http_last);
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
    assert_eq!(broker.budget_snapshot().await.2, 0);
    http_stall.finish(true).await;
    drop(http_last);
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

// 负控一：最早 64 B leak 的 add 已被 300 次分配/释放挤出可读环，必须由总数报告。
#[test]
fn diagnostic_event_ring_reports_overwrite_and_identity() {
    let _serial = allocations::SERIAL.lock().unwrap();
    allocations::reset();
    let _scope = allocations::Scope::enter();
    let leak = Vec::<u8>::with_capacity(64);
    for _ in 0..300 {
        let block = Vec::<u8>::with_capacity(64);
        assert_eq!(block.capacity(), 64);
    }
    let view = allocations::snapshot();
    assert_eq!(view.events.total, 601);
    assert_eq!(view.events.count, allocations::EVENTS);
    assert!(view.events.overwritten);
    assert_eq!(view.events.total - view.events.count as u64, 345);
    assert_eq!(view.records.live, 1);
    assert!(!view.records.truncated);
    let record = view.records.items[0];
    assert_eq!(record.size, 64);
    assert_eq!(record.pointer, leak.as_ptr() as usize);
    assert!(
        view.events.items[..view.events.count]
            .iter()
            .all(|event| event.seq != record.seq),
        "{}",
        allocations::describe()
    );
    let oldest = &view.events.items[..2];
    assert!(oldest[0].add && oldest[0].found);
    assert!(!oldest[1].add && oldest[1].found);
    assert_eq!(oldest[0].seq, oldest[1].seq);
    let tail = &view.events.items[view.events.count - 2..view.events.count];
    assert!(tail[0].add && tail[0].found);
    assert!(!tail[1].add && tail[1].found);
    assert_eq!(tail[0].seq, tail[1].seq);
    assert_eq!(tail[0].pointer, tail[1].pointer);
    drop(leak);
    let final_view = allocations::snapshot();
    assert_eq!(final_view.stats.live, 0);
    assert_eq!(final_view.events.total, 602);
}

// 负控二：41 条 live 只展示 32 条，必须带 truncated/omitted；恰好 32 条不能误报。
#[test]
fn diagnostic_records_report_truncation_and_exact_capacity() {
    let _serial = allocations::SERIAL.lock().unwrap();
    allocations::reset();
    let _scope = allocations::Scope::enter();
    let mut blocks: [Vec<u8>; 41] = std::array::from_fn(|_| Vec::<u8>::with_capacity(64));
    let mut pointers = [0usize; 41];
    for (pointer, block) in pointers.iter_mut().zip(&blocks) {
        *pointer = block.as_ptr() as usize;
    }
    let full = allocations::snapshot();
    assert_eq!(full.records.live, 41);
    assert_eq!(full.records.count, allocations::RECORDS);
    assert!(full.records.truncated);
    assert_eq!(full.records.live - full.records.count, 9);
    assert_eq!(full.events.total, 41);
    assert!(!full.events.overwritten);
    for (index, record) in full.records.items[..full.records.count].iter().enumerate() {
        assert_eq!(record.size, 64);
        assert!(pointers.contains(&record.pointer));
        assert!(record.seq != 0);
        assert!(full.records.items[..index]
            .iter()
            .all(|prior| prior.pointer != record.pointer && prior.seq != record.seq));
    }
    for block in &mut blocks[..9] {
        *block = Vec::new();
    }
    let boundary = allocations::snapshot();
    assert_eq!(boundary.records.live, allocations::RECORDS);
    assert_eq!(boundary.records.count, allocations::RECORDS);
    assert!(!boundary.records.truncated);
    assert_eq!(boundary.records.live - boundary.records.count, 0);
    assert_eq!(boundary.events.total, 50);
    for record in &boundary.records.items[..boundary.records.count] {
        assert!(pointers[9..].contains(&record.pointer));
    }
    drop(blocks);
    let final_view = allocations::snapshot();
    assert_eq!(final_view.stats.live, 0);
    assert_eq!(final_view.events.total, 82);
}

// 合成地址只作 tracker key，不解引用也不交给 allocator；验证地址重用后的 identity。
#[test]
fn diagnostic_identity_distinguishes_reused_address() {
    let _serial = allocations::SERIAL.lock().unwrap();
    allocations::reset();
    let _scope = allocations::Scope::enter();
    let address = 0x1000 as *mut u8;
    let first = allocations::allocated(64, || address);
    let first_view = allocations::snapshot();
    assert_eq!(first_view.records.live, 1);
    let first_record = first_view.records.items[0];
    assert_eq!(first_record.pointer, address as usize);
    allocations::freed(first, 64);
    let empty = allocations::snapshot();
    assert_eq!(empty.records.live, 0);
    assert_eq!(empty.events.total, 2);
    assert!(!empty.events.items[1].add && empty.events.items[1].found);
    assert_eq!(empty.events.items[1].seq, first_record.seq);
    let second = allocations::allocated(64, || address);
    let second_view = allocations::snapshot();
    assert_eq!(second_view.records.live, 1);
    let second_record = second_view.records.items[0];
    assert_eq!(second_record.pointer, address as usize);
    assert_ne!(second_record.seq, first_record.seq);
    assert_eq!(second_view.events.total, 3);
    assert_eq!(second_view.events.items[1].seq, first_record.seq);
    assert!(second_view.events.items[2].add && second_view.events.items[2].found);
    assert_eq!(second_view.events.items[2].seq, second_record.seq);
    allocations::freed(second, 64);
    let done = allocations::snapshot();
    assert_eq!(done.stats.live, 0);
    assert_eq!(done.stats.count, 2);
    assert_eq!(done.stats.peak, 64);
    assert_eq!(done.events.items[3].seq, second_record.seq);
}

// describe 只暂停自身格式化；快照前 live 与 realloc 旧/新峰仍由真实 allocator 参与。
#[test]
fn diagnostic_snapshot_survives_describe_and_reports_realloc_peak() {
    let _serial = allocations::SERIAL.lock().unwrap();
    allocations::reset();
    let _scope = allocations::Scope::enter();
    let block = Vec::<u8>::with_capacity(64);
    let pointer = block.as_ptr() as usize;
    let before = allocations::snapshot();
    assert_eq!(before.records.live, 1);
    let record = before.records.items[0];
    assert_eq!(record.pointer, pointer);
    let described = allocations::describe();
    let after = allocations::snapshot();
    assert_eq!(before.stats.live, after.stats.live);
    assert!(described.contains("records=(shown=1,live=1,truncated=false,omitted=0)"));
    assert!(described.contains("events=(shown=1,total=1,overwritten=false,dropped=0)"));
    assert!(described.contains("rec(ptr="));
    {
        let _quiet = allocations::Scope::suspend();
        let worker = std::thread::spawn(move || drop(block));
        worker.join().unwrap();
    }
    let freed = allocations::snapshot();
    assert_eq!(freed.stats.live, 0);
    assert_eq!(freed.stats.cross_thread_frees, 1);
    let mut bytes = Vec::<u8>::with_capacity(64);
    let old = bytes.capacity();
    assert_eq!(old, 64);
    bytes.reserve_exact(65);
    assert_eq!(bytes.capacity(), 65);
    let reallocated = allocations::snapshot();
    assert_eq!(reallocated.stats.live, bytes.capacity());
    assert_eq!(reallocated.stats.peak, old + bytes.capacity());
    assert!(
        reallocated.records.items[..reallocated.records.count]
            .iter()
            .any(|record| record.pointer == bytes.as_ptr() as usize
                && record.size == bytes.capacity())
    );
    drop(bytes);
    let final_view = allocations::snapshot();
    assert_eq!(final_view.stats.live, 0);
}
