// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// 一个 broker actor 独占控制权、控制 epoch、请求 ID 映射、pending 与 attachment 归属。
// 网络与 SQLite 等待都以阶段推进的方式回到 actor，任何跨 await 的共享 Mutex 都不参与
// 权限判断。客户端只通过有界帧通道和 oneshot 与管理操作通信；响应只回原客户端。

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::ops::Deref;
use serde::Deserialize;
use serde_json::value::RawValue;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{Instant, MissedTickBehavior};

use crate::attachments::Attachments;
use crate::observe::Observer;
use crate::services::ProfileService;
use crate::transport::{LeasedMessage, PayloadBudget, PayloadLease, Upstream, UpstreamEvent};

pub type ClientId = u64;

const VIEW_METHODS: &[&str] = &[
    "Browser.getVersion",
    "Target.getTargets",
    "Target.getTargetInfo",
    "Target.attachToTarget",
    "Target.detachFromTarget",
    "Target.setDiscoverTargets",
    "Page.enable",
    "Page.disable",
    "Page.captureScreenshot",
    "Page.startScreencast",
    "Page.stopScreencast",
    "Page.screencastFrameAck",
    "Page.getFrameTree",
    "Page.getLayoutMetrics",
    "DOM.getDocument",
    "DOM.getOuterHTML",
    "DOM.querySelector",
    "DOM.querySelectorAll",
    "Accessibility.getFullAXTree",
    "Kitesurf.getSessionInfo",
];

const MAX_OWNER_UTF16: usize = 128;
const MAX_CLIENTS: usize = 32;
const MAX_RESERVED: usize = 128;
const MAX_PENDING_PER_CLIENT: usize = 64;
const MAX_PENDING_GLOBAL: usize = 512;
const MAX_INTERNAL_PENDING: usize = 32;
const MAX_ATTACHMENTS: usize = 1024;
const MAX_ATTACHMENTS_PER_CLIENT: usize = 256;
const MAX_UNKNOWN: usize = MAX_PENDING_GLOBAL + MAX_INTERNAL_PENDING;
const MAX_MANAGEMENT_QUEUE: usize = 64;
const MAX_CONTROL_QUEUE: usize = 8;
const MAX_PAGE_QUEUE: usize = 64;
const MAX_PAGE_QUEUE_BYTES: usize = 8 * 1024 * 1024;
const CLIENT_QUEUE_FRAMES: usize = 64;
const CLIENT_QUEUE_BYTES: usize = 32 * 1024 * 1024;
const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
const CLIENT_DEADLINE: Duration = Duration::from_secs(120);
const INTERNAL_DEADLINE: Duration = Duration::from_secs(30);
const CONTROL_DEADLINE: Duration = Duration::from_secs(10);
const FINALIZE_TIMEOUT: Duration = Duration::from_secs(2);
const TICK: Duration = Duration::from_millis(50);

const FRAME_CAPACITY: usize = 8;
const COMMAND_CAPACITY: usize = 256;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Role {
    Controller,
    Viewer,
}

#[derive(Clone, Debug)]
pub enum Endpoint {
    Browser,
    Page { target_id: String },
}

#[derive(Clone)]
pub struct ReserveRequest {
    pub endpoint: Endpoint,
    pub role: Role,
    pub owner: String,
}

#[derive(Clone)]
pub struct Reservation {
    id: ClientId,
    endpoint: Endpoint,
    role: Role,
    owner: String,
}

impl Reservation {
    pub fn id(&self) -> ClientId {
        self.id
    }
}

pub enum ClientCommand {
    Text(String, PayloadLease),
    Pong(Vec<u8>, PayloadLease),
    Close { code: u16, reason: String },
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum CloseKind {
    Http,
    Cdp,
    Host,
}

pub struct LeasedTargets {
    pub values: Vec<Value>,
    pub lease: PayloadLease,
}

pub struct SessionSnapshot {
    pub alive: bool,
    pub controller: Option<String>,
    pub control_epoch: u64,
}

enum BrokerCommand {
    Reserve(ReserveRequest, oneshot::Sender<Result<Reservation, String>>),
    Commit {
        reservation: Reservation,
        writer: mpsc::Sender<ClientCommand>,
        cancel: watch::Sender<bool>,
        reply: oneshot::Sender<bool>,
    },
    Rollback(ClientId),
    Pong(ClientId, Vec<u8>, PayloadLease),
    Control {
        owner: String,
        takeover: bool,
        reply: oneshot::Sender<Result<(String, u64), ControlError>>,
    },
    Flush {
        reply: oneshot::Sender<Result<Value, String>>,
    },
    Close {
        kind: CloseKind,
        client: Option<ClientId>,
        external_id: Option<i64>,
        explicit_session: Option<String>,
        reply: oneshot::Sender<Result<(), String>>,
    },
    Session {
        reply: oneshot::Sender<SessionSnapshot>,
    },
    Targets {
        reply: oneshot::Sender<Result<LeasedTargets, String>>,
    },
    #[cfg(all(test, feature = "test-faults"))]
    BudgetSnapshot(oneshot::Sender<(usize, usize, usize, usize)>),
    ProfileStatus {
        token: u64,
        result: Result<Value, String>,
    },
}

pub enum ControlError {
    Conflict {
        message: String,
        controller: Option<String>,
    },
    Other(String),
}

impl ControlError {
    pub fn conflict(controller: Option<String>) -> Self {
        Self::Conflict {
            message: "Browser has another controller".to_string(),
            controller,
        }
    }

    pub fn message(&self) -> String {
        match self {
            Self::Conflict { message, .. } => message.clone(),
            Self::Other(message) => message.clone(),
        }
    }

    pub fn controller(&self) -> Option<String> {
        match self {
            Self::Conflict { controller, .. } => controller.clone(),
            Self::Other(_) => None,
        }
    }
}

#[derive(Clone, Default)]
struct BrokerNotes {
    lost: bool,
    clients: HashMap<ClientId, (bool, bool)>,
}

#[derive(Clone)]
pub struct BrokerHandle {
    frames: mpsc::Sender<(ClientId, String, PayloadLease)>,
    budget: PayloadBudget,
    notes: watch::Sender<BrokerNotes>,
    commands: mpsc::Sender<BrokerCommand>,
    lost: watch::Receiver<Option<String>>,
    closed: watch::Receiver<bool>,
    diagnostics: Option<watch::Receiver<Option<crate::supervisor::EngineDiagnostic>>>,
}

impl BrokerHandle {
    /// 启动 actor；main 持有本句柄并等待 lost/closed 之一。
    pub fn start(
        upstream: Upstream,
        profile: Arc<ProfileService>,
        observer: Arc<Observer>,
    ) -> Self {
        Self::start_with(
            upstream,
            profile,
            observer,
            CLIENT_DEADLINE,
            INTERNAL_DEADLINE,
        )
    }

    /// 仅 test-faults 变体：确定性测试可以缩短“结果未知”的等待，不改变发行路径。
    #[cfg(feature = "test-faults")]
    pub fn start_with_deadlines(
        upstream: Upstream,
        profile: Arc<ProfileService>,
        observer: Arc<Observer>,
        client: Duration,
        internal: Duration,
    ) -> Self {
        Self::start_with(upstream, profile, observer, client, internal)
    }

    fn start_with(
        upstream: Upstream,
        profile: Arc<ProfileService>,
        observer: Arc<Observer>,
        client_deadline: Duration,
        internal_deadline: Duration,
    ) -> Self {
        let budget = upstream.handle.budget.clone();
        let (frames, frame_rx) = mpsc::channel::<(ClientId, String, PayloadLease)>(FRAME_CAPACITY);
        let (notes, note_rx) = watch::channel(BrokerNotes::default());
        let (commands, command_rx) = mpsc::channel(COMMAND_CAPACITY);
        let (lost_tx, lost) = watch::channel::<Option<String>>(None);
        let (closed_tx, closed) = watch::channel(false);
        let scratch = budget
            .try_acquire(64 * 1024, true)
            .expect("actor metadata reserve");
        let handle = Self {
            budget,
            frames: frames.clone(),
            notes: notes.clone(),
            commands: commands.clone(),
            lost,
            closed,
            diagnostics: None,
        };
        let broker = Broker {
            _scratch: scratch,
            attachments: Attachments::new(upstream.handle.budget.clone()),
            upstream,
            frames: frame_rx,
            notes: note_rx,
            notes_tx: notes,
            commands: command_rx,
            commands_tx: commands,
            profile,
            observer,
            clients: HashMap::new(),
            reservations: HashMap::new(),
            next_client: 1,
            pending: HashMap::new(),
            unknown: HashMap::new(),
            client_pending: 0,
            request_slots: Arc::new(Semaphore::new(MAX_PENDING_GLOBAL)),
            internal_pending: 0,
            internal_slots: Arc::new(Semaphore::new(MAX_INTERNAL_PENDING)),
            upstream_bytes: 0,
            client_deadline,
            internal_deadline,
            lifecycle: Lifecycle::Running,
            controller: None,
            control_epoch: 1,
            active_controller: None,
            control_queue: VecDeque::new(),
            control_active: None,
            management_queue: VecDeque::new(),
            management_active: None,
            retirements: Vec::new(),
            next_retirement: 1,
            flush: None,
            close: None,
            next_token: 1,
            lost_tx,
            closed_tx,
            finished: false,
        };
        tokio::spawn(broker.run());
        handle
    }

    pub fn lost_watch(&self) -> watch::Receiver<Option<String>> {
        self.lost.clone()
    }

    pub fn closed_watch(&self) -> watch::Receiver<bool> {
        self.closed.clone()
    }

    pub fn with_diagnostics(
        mut self,
        diagnostics: watch::Receiver<Option<crate::supervisor::EngineDiagnostic>>,
    ) -> Self {
        self.diagnostics = Some(diagnostics);
        self
    }

    pub fn engine_lost(&self) {
        self.notes.send_modify(|notes| notes.lost = true);
    }

    /// EOF 只结束通知，不代表 Lost；每次唤醒先检查实际 Lost，再检查 Closed。
    pub async fn wait_terminal(&self) -> bool {
        wait_terminal(self.lost_watch(), self.closed_watch()).await
    }

    #[cfg(all(test, feature = "test-faults"))]
    async fn budget_snapshot(&self) -> (usize, usize, usize, usize) {
        self.command(BrokerCommand::BudgetSnapshot).await.unwrap()
    }

    pub fn payload_budget(&self) -> PayloadBudget {
        self.budget.clone()
    }

    pub fn try_frame(&self, client: ClientId, text: String) -> bool {
        let Some(lease) = self.budget.try_acquire(text.len(), false) else {
            return false;
        };
        self.frames.try_send((client, text, lease)).is_ok()
    }

    /// 读端使用有界队列施加背压；broker 结束时返回 false。
    pub async fn frame(&self, client: ClientId, text: String) -> bool {
        let Some(lease) = self.budget.try_acquire(text.len(), false) else {
            return false;
        };
        self.frames.send((client, text, lease)).await.is_ok()
    }

    pub async fn leased_frame(&self, client: ClientId, frame: LeasedMessage) -> bool {
        let LeasedMessage { message, lease } = frame;
        let tokio_tungstenite::tungstenite::Message::Text(text) = message else {
            return false;
        };
        let Some(copy) = self.budget.try_acquire(text.len(), false) else {
            return false;
        };
        let text = text.to_string();
        drop(lease);
        self.frames.send((client, text, copy)).await.is_ok()
    }

    pub fn pong(&self, client: ClientId, payload: Vec<u8>, lease: PayloadLease) -> bool {
        self.commands
            .try_send(BrokerCommand::Pong(client, payload, lease))
            .is_ok()
    }

    pub fn writer_gone(&self, client: ClientId) {
        self.notes.send_modify(|notes| {
            if let Some(state) = notes.clients.get_mut(&client) {
                state.1 = true;
            }
        });
    }

    pub fn closed(&self, client: ClientId) {
        self.notes.send_modify(|notes| {
            if let Some(state) = notes.clients.get_mut(&client) {
                state.0 = true;
            }
        });
    }

    pub async fn reserve(&self, request: ReserveRequest) -> Result<Reservation, String> {
        self.command(|reply| BrokerCommand::Reserve(request, reply))
            .await
            .and_then(|result| result)
    }

    pub async fn commit(
        &self,
        reservation: Reservation,
        writer: mpsc::Sender<ClientCommand>,
        cancel: watch::Sender<bool>,
    ) -> bool {
        let (tx, rx) = oneshot::channel();
        let command = BrokerCommand::Commit {
            reservation,
            writer,
            cancel,
            reply: tx,
        };
        if self.commands.send(command).await.is_err() {
            return false;
        }
        rx.await.unwrap_or(false)
    }

    pub fn rollback(&self, client: ClientId) {
        let _ = self.commands.try_send(BrokerCommand::Rollback(client));
    }

    pub async fn control(
        &self,
        owner: String,
        takeover: bool,
    ) -> Result<(String, u64), ControlError> {
        let (tx, rx) = oneshot::channel();
        let command = BrokerCommand::Control {
            owner,
            takeover,
            reply: tx,
        };
        if self.commands.send(command).await.is_err() {
            return Err(ControlError::Other("browser session is lost".into()));
        }
        rx.await
            .unwrap_or_else(|_| Err(ControlError::Other("browser session is lost".into())))
    }

    pub async fn flush(&self) -> Result<Value, String> {
        self.command(|reply| BrokerCommand::Flush { reply })
            .await
            .and_then(|result| result)
    }

    pub async fn close(
        &self,
        kind: CloseKind,
        client: Option<ClientId>,
        external_id: Option<i64>,
        explicit_session: Option<String>,
    ) -> Result<(), String> {
        let (tx, rx) = oneshot::channel();
        let command = BrokerCommand::Close {
            kind,
            client,
            external_id,
            explicit_session,
            reply: tx,
        };
        if self.commands.send(command).await.is_err() {
            return Err("browser session is lost".into());
        }
        rx.await
            .unwrap_or_else(|_| Err("browser session is lost".into()))
    }

    pub async fn session(&self) -> Option<SessionSnapshot> {
        if self
            .diagnostics
            .as_ref()
            .is_some_and(|watch| watch.borrow().is_some())
        {
            self.engine_lost();
            return None;
        }
        self.command(|reply| BrokerCommand::Session { reply })
            .await
            .ok()
    }

    pub async fn targets(&self) -> Result<LeasedTargets, String> {
        self.command(|reply| BrokerCommand::Targets { reply })
            .await
            .and_then(|result| result)
    }

    async fn command<T>(
        &self,
        build: impl FnOnce(oneshot::Sender<T>) -> BrokerCommand,
    ) -> Result<T, String> {
        let (tx, rx) = oneshot::channel();
        if self.commands.send(build(tx)).await.is_err() {
            return Err("browser session is lost".into());
        }
        rx.await.map_err(|_| "browser session is lost".to_string())
    }
}

#[cfg(test)]
mod terminal_tests {
    use super::*;

    #[tokio::test]
    async fn closed_sender_eof_is_not_engine_loss_256_trials() {
        for _ in 0..256 {
            let (lost_tx, lost) = watch::channel(None);
            let (closed_tx, closed) = watch::channel(false);
            closed_tx.send(true).unwrap();
            drop(closed_tx);
            drop(lost_tx);
            assert!(wait_terminal(lost, closed).await);
        }
    }

    #[tokio::test]
    async fn actual_loss_wins_over_closed_and_eof() {
        for closed_value in [false, true] {
            let (lost_tx, lost) = watch::channel(Some("actual loss".to_string()));
            let (closed_tx, closed) = watch::channel(closed_value);
            drop(lost_tx);
            drop(closed_tx);
            assert!(!wait_terminal(lost, closed).await);
        }
        let (_, lost) = watch::channel(None);
        let (_, closed) = watch::channel(false);
        assert!(!wait_terminal(lost, closed).await);
    }
}

async fn wait_terminal(
    mut lost: watch::Receiver<Option<String>>,
    mut closed: watch::Receiver<bool>,
) -> bool {
    let (mut lost_open, mut closed_open) = (true, true);
    loop {
        if lost.borrow().is_some() {
            return false;
        }
        if *closed.borrow() {
            return true;
        }
        if !lost_open && !closed_open {
            return false;
        }
        tokio::select! {
            result = lost.changed(), if lost_open => lost_open = result.is_ok(),
            result = closed.changed(), if closed_open => closed_open = result.is_ok(),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Lifecycle {
    Running,
    Flushing,
    Closing,
    Lost,
    Closed,
}

/// 外部逻辑请求只在准入时取一次槽；backfill 共享所有权，unknown 延续到确定回复。
struct RequestLease {
    _client: OwnedSemaphorePermit,
    _global: OwnedSemaphorePermit,
    ids: Arc<Mutex<std::collections::HashSet<i64>>>,
    id: i64,
    _metadata: PayloadLease,
}

impl Drop for RequestLease {
    fn drop(&mut self) {
        self.ids.lock().unwrap().remove(&self.id);
    }
}

struct Request {
    envelope: Value,
    params: Option<Box<RawValue>>,
    metadata: Option<PayloadLease>,
    _payload: PayloadLease,
    lease: Option<Arc<RequestLease>>,
}

impl Deref for Request {
    type Target = Value;
    fn deref(&self) -> &Value {
        &self.envelope
    }
}

#[derive(Deserialize)]
struct BorrowedRequest<'a> {
    #[serde(borrow)]
    id: Option<&'a RawValue>,
    #[serde(borrow)]
    method: Option<&'a RawValue>,
    #[serde(rename = "sessionId", borrow)]
    session: Option<&'a RawValue>,
    #[serde(borrow)]
    params: Option<&'a RawValue>,
}

impl Request {
    fn parse(text: &str, payload: PayloadLease, metadata_lease: PayloadLease) -> Option<Self> {
        let raw: BorrowedRequest<'_> = serde_json::from_str(text).ok()?;
        let mut envelope = serde_json::Map::new();
        for (key, raw) in [
            ("id", raw.id),
            ("method", raw.method),
            ("sessionId", raw.session),
        ] {
            if let Some(raw) = raw {
                // 身份字段不可成为大 payload 的第二份隐藏副本。
                if raw.get().len() > 4096 {
                    return None;
                }
                envelope.insert(key.into(), serde_json::from_str(raw.get()).ok()?);
            }
        }
        let params = raw
            .params
            .map(|raw| RawValue::from_string(raw.get().into()))
            .transpose()
            .ok()?;
        let mut metadata = serde_json::Map::new();
        if let Some(raw) = raw.params {
            #[derive(Default, Deserialize)]
            #[serde(rename_all = "camelCase")]
            struct Fields<'a> {
                #[serde(borrow)]
                target_id: Option<&'a RawValue>,
                #[serde(borrow)]
                session_id: Option<&'a RawValue>,
                #[serde(borrow)]
                auto_attach: Option<&'a RawValue>,
                #[serde(borrow)]
                discover: Option<&'a RawValue>,
            }
            let fields: Fields<'_> = serde_json::from_str(raw.get()).unwrap_or_default();
            for (key, raw) in [
                ("targetId", fields.target_id),
                ("sessionId", fields.session_id),
                ("autoAttach", fields.auto_attach),
                ("discover", fields.discover),
            ] {
                if let Some(raw) = raw {
                    if raw.get().len() > 4096 {
                        return None;
                    }
                    metadata.insert(key.into(), serde_json::from_str(raw.get()).ok()?);
                }
            }
        }
        envelope.insert("params".into(), Value::Object(metadata));
        Some(Self {
            envelope: Value::Object(envelope),
            params,
            metadata: Some(metadata_lease),
            _payload: payload,
            lease: None,
        })
    }
}

struct Client {
    slots: Arc<Semaphore>,
    ids: Arc<Mutex<std::collections::HashSet<i64>>>,
    owner: String,
    role: Role,
    epoch: u64,
    endpoint: Endpoint,
    primary: Option<String>,
    page_ready: bool,
    page_queue: VecDeque<Request>,
    page_queue_bytes: usize,
    writer: mpsc::Sender<ClientCommand>,
    cancel: watch::Sender<bool>,
    output_frames: Arc<Semaphore>,
    output_bytes: Arc<Semaphore>,
    pending: usize,
    discover: bool,
    closed: bool,
    retired: bool,
    writer_gone: bool,
}

struct InternalRequestLease {
    _slot: OwnedSemaphorePermit,
    _metadata: PayloadLease,
}

struct Pending {
    internal: Option<Arc<InternalRequestLease>>,
    request: Option<Arc<RequestLease>>,
    client: Option<ClientId>,
    role: Option<Role>,
    external_id: Option<i64>,
    explicit_session: Option<String>,
    epoch: u64,
    method: String,
    kind: PendingKind,
    deadline: Instant,
}

enum PendingKind {
    Client,
    PageAttach { target: String },
    Managed,
    Flush,
    Close,
    Targets(oneshot::Sender<Result<LeasedTargets, String>>),
    RetirementAuto { id: u64 },
    RetirementDetach { id: u64, session: String },
}

struct Unknown {
    _internal: Option<Arc<InternalRequestLease>>,
    _request: Option<Arc<RequestLease>>,
    client: Option<ClientId>,
    role: Option<Role>,
    epoch: u64,
    method: String,
    kind: UnknownKind,
}

enum UnknownKind {
    Plain,
    PageAttach {
        target: String,
    },
    Managed {
        target: Option<String>,
        session: Option<String>,
        auto: Option<bool>,
    },
    Flush,
    Close,
    RetirementAuto {
        id: u64,
    },
    RetirementDetach {
        session: String,
    },
}

struct ManagedWork {
    request: Request,
    client: ClientId,
    epoch: u64,
    external_id: i64,
    explicit_session: Option<String>,
    method: String,
    params: Value,
    retained: Vec<PayloadLease>,
}

struct ActiveManaged {
    stage: ManagedStage,
    response: Value,
    work: ManagedWork,
}

#[derive(Clone)]
enum ManagedStage {
    Sent {
        id: u64,
    },
    BackfillTargets {
        id: u64,
    },
    BackfillAttach {
        id: u64,
        targets: Vec<String>,
        index: usize,
        target: String,
    },
}

struct Retirement {
    id: u64,
    client: ClientId,
    pending: Option<u64>,
    stage: RetireStage,
    blocked: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RetireStage {
    Auto,
    Detach,
}

struct ActiveControl {
    owner: String,
    old_epoch: u64,
    reply: Option<oneshot::Sender<Result<(String, u64), ControlError>>>,
    deadline: Instant,
    replied: bool,
}

struct FlushState {
    reply: Option<oneshot::Sender<Result<Value, String>>>,
    stage: FlushStage,
}

enum FlushStage {
    Drain,
    Sent,
    Status(u64),
}

struct CloseState {
    _request: Option<Arc<RequestLease>>,
    kind: CloseKind,
    client: Option<ClientId>,
    external_id: Option<i64>,
    explicit_session: Option<String>,
    reply: Option<oneshot::Sender<Result<(), String>>>,
    stage: CloseStage,
    finalize_until: Option<Instant>,
    started: Instant,
    notified: bool,
    succeeded: bool,
}

enum CloseStage {
    Drain,
    Flush,
    Browser,
}

type ControlWaiter = oneshot::Sender<Result<(String, u64), ControlError>>;

struct Broker {
    upstream: Upstream,
    frames: mpsc::Receiver<(ClientId, String, PayloadLease)>,
    notes: watch::Receiver<BrokerNotes>,
    notes_tx: watch::Sender<BrokerNotes>,
    commands: mpsc::Receiver<BrokerCommand>,
    commands_tx: mpsc::Sender<BrokerCommand>,
    profile: Arc<ProfileService>,
    observer: Arc<Observer>,
    clients: HashMap<ClientId, Client>,
    reservations: HashMap<ClientId, Reservation>,
    next_client: ClientId,
    attachments: Attachments,
    pending: HashMap<u64, Pending>,
    unknown: HashMap<u64, Unknown>,
    client_pending: usize,
    request_slots: Arc<Semaphore>,
    internal_pending: usize,
    internal_slots: Arc<Semaphore>,
    upstream_bytes: usize,
    client_deadline: Duration,
    internal_deadline: Duration,
    lifecycle: Lifecycle,
    controller: Option<String>,
    control_epoch: u64,
    active_controller: Option<ClientId>,
    control_queue: VecDeque<(String, bool, ControlWaiter)>,
    control_active: Option<ActiveControl>,
    management_queue: VecDeque<ManagedWork>,
    management_active: Option<ActiveManaged>,
    retirements: Vec<Retirement>,
    next_retirement: u64,
    flush: Option<FlushState>,
    close: Option<CloseState>,
    next_token: u64,
    lost_tx: watch::Sender<Option<String>>,
    closed_tx: watch::Sender<bool>,
    finished: bool,
    _scratch: PayloadLease,
}

enum BackfillStep {
    Sent,
    Done,
    Failed(String),
}
impl Broker {
    async fn run(mut self) {
        let mut ticker = tokio::time::interval(TICK);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        while !self.finished {
            tokio::select! {
              _ = self.upstream.lost.changed() => {
                let reason = *self.upstream.lost.borrow_and_update();
                if let Some(reason) = reason { self.handle_lost(reason.into()); }
              }
              event = self.upstream.events.recv() => {
                if let Some(event) = event {
                  self.upstream_event(event);
                }
              }
              frame = self.frames.recv() => {
                if let Some((client, text, lease)) = frame {
                  self.client_frame(client, text, lease);
                }
              }
              _ = self.notes.changed() => {
                let notes = self.notes.borrow_and_update().clone();
                self.note(notes);
                self.notes_tx.send_if_modified(|notes| {
                    let before = notes.clients.len();
                    notes.clients.retain(|id, _| self.clients.contains_key(id));
                    before != notes.clients.len()
                });
              }
              command = self.commands.recv() => {
                if let Some(command) = command {
                  self.command(command);
                }
              }
              _ = ticker.tick() => self.tick(),
            }
        }
        self.upstream.shutdown().await;
    }

    fn command(&mut self, command: BrokerCommand) {
        match command {
            #[cfg(all(test, feature = "test-faults"))]
            BrokerCommand::BudgetSnapshot(reply) => {
                let _ = reply.send((
                    MAX_PENDING_GLOBAL - self.request_slots.available_permits(),
                    self.management_queue.len(),
                    self.unknown.len(),
                    MAX_INTERNAL_PENDING - self.internal_slots.available_permits(),
                ));
            }
            BrokerCommand::Reserve(request, reply) => {
                let result = self.reserve(request);
                let _ = reply.send(result);
            }
            BrokerCommand::Commit {
                reservation,
                writer,
                cancel,
                reply,
            } => {
                let committed = self.commit(reservation, writer, cancel);
                let _ = reply.send(committed);
            }
            BrokerCommand::Pong(client, payload, mut lease) => {
                let Some(state) = self.clients.get_mut(&client) else {
                    return;
                };
                let permits = state.output_frames.clone().try_acquire_owned().ok().zip(
                    state
                        .output_bytes
                        .clone()
                        .try_acquire_many_owned(payload.len() as u32)
                        .ok(),
                );
                let Some((frame, bytes)) = permits else {
                    self.disconnect_client(client, 1011, "Slow client", true);
                    return;
                };
                lease.hold(frame);
                lease.hold(bytes);
                if state
                    .writer
                    .try_send(ClientCommand::Pong(payload, lease))
                    .is_err()
                {
                    self.disconnect_client(client, 1011, "Slow client", true);
                }
            }
            BrokerCommand::Rollback(client) => {
                self.reservations.remove(&client);
            }
            BrokerCommand::Control {
                owner,
                takeover,
                reply,
            } => {
                if self.control_queue.len() >= MAX_CONTROL_QUEUE {
                    let _ = reply.send(Err(ControlError::Other(
                        "control queue is full".to_string(),
                    )));
                } else {
                    self.control_queue.push_back((owner, takeover, reply));
                    self.pump_control();
                }
            }
            BrokerCommand::Flush { reply } => self.start_flush(reply),
            BrokerCommand::Close {
                kind,
                client,
                external_id,
                explicit_session,
                reply,
            } => {
                let _ = self.start_close(kind, client, external_id, explicit_session, Some(reply));
            }
            BrokerCommand::Session { reply } => {
                let _ = reply.send(SessionSnapshot {
                    alive: !matches!(self.lifecycle, Lifecycle::Lost | Lifecycle::Closed),
                    controller: self.controller.clone(),
                    control_epoch: self.control_epoch,
                });
            }
            BrokerCommand::Targets { reply } => self.request_targets(reply),
            BrokerCommand::ProfileStatus { token, result } => self.profile_status(token, result),
        }
    }

    fn note(&mut self, notes: BrokerNotes) {
        if notes.lost {
            if let Some(state) = self.close.as_mut() {
                state.succeeded = false;
            }
            self.handle_lost("engine fatal diagnostic or process exit".to_string());
            return;
        }
        for (client, (closed, gone)) in notes.clients {
            if gone {
                if let Some(state) = self.clients.get_mut(&client) {
                    state.writer_gone = true;
                }
            }
            if closed || gone {
                self.client_closed(client);
            }
        }
        self.pump_finalize();
    }

    fn reserve(&mut self, request: ReserveRequest) -> Result<Reservation, String> {
        if self.lifecycle != Lifecycle::Running {
            return Err("Browser unavailable".to_string());
        }
        if !valid_owner(&request.owner) {
            return Err("Owner name is too long".to_string());
        }
        if self.clients.len() >= MAX_CLIENTS || self.reservations.len() >= MAX_RESERVED {
            return Err("Too many browser connections".to_string());
        }
        if request.role == Role::Controller {
            if self.control_active.is_some() {
                return Err("Control change is in progress".to_string());
            }
            if let Some(controller) = &self.controller {
                if controller != &request.owner {
                    return Err(
                        "Another controller owns the browser; explicitly take control".to_string(),
                    );
                }
            }
            if self.active_controller.is_some() {
                return Err("Controller is already connected".to_string());
            }
        }
        let id = self.next_client;
        self.next_client = self.next_client.saturating_add(1);
        let reservation = Reservation {
            id,
            endpoint: request.endpoint,
            role: request.role,
            owner: request.owner,
        };
        self.reservations.insert(id, reservation.clone());
        Ok(reservation)
    }

    fn commit(
        &mut self,
        reservation: Reservation,
        writer: mpsc::Sender<ClientCommand>,
        cancel: watch::Sender<bool>,
    ) -> bool {
        let id = reservation.id;
        if self.reservations.remove(&id).is_none() {
            return false;
        }
        if self.lifecycle != Lifecycle::Running || self.clients.len() >= MAX_CLIENTS {
            return false;
        }
        if reservation.role == Role::Controller {
            if self.control_active.is_some() || self.active_controller.is_some() {
                return false;
            }
            if let Some(controller) = &self.controller {
                if controller != &reservation.owner {
                    return false;
                }
            } else {
                self.controller = Some(reservation.owner.clone());
            }
            self.active_controller = Some(id);
        }
        let page = matches!(reservation.endpoint, Endpoint::Page { .. });
        self.notes_tx.send_modify(|notes| {
            notes.clients.retain(|id, _| self.clients.contains_key(id));
            notes.clients.insert(id, (false, false));
        });
        let client = Client {
            slots: Arc::new(Semaphore::new(MAX_PENDING_PER_CLIENT)),
            ids: Arc::new(Mutex::new(std::collections::HashSet::new())),
            owner: reservation.owner,
            role: reservation.role,
            epoch: self.control_epoch,
            endpoint: reservation.endpoint.clone(),
            primary: None,
            page_ready: !page,
            page_queue: VecDeque::new(),
            page_queue_bytes: 0,
            writer,
            cancel,
            output_frames: Arc::new(Semaphore::new(CLIENT_QUEUE_FRAMES)),
            output_bytes: Arc::new(Semaphore::new(CLIENT_QUEUE_BYTES)),
            pending: 0,
            discover: false,
            closed: false,
            retired: false,
            writer_gone: false,
        };
        self.clients.insert(id, client);
        self.observer.with(
            "client-open",
            json!({ "kind": "client-open", "clients": self.clients.len() }),
        );
        if let Endpoint::Page { target_id } = reservation.endpoint {
            if !self.start_page_attach(id, &target_id) {
                self.disconnect_client(id, 1008, "Target expired", true);
            }
        }
        true
    }

    fn client_frame(&mut self, client_id: ClientId, text: String, ingress: PayloadLease) {
        let Some(payload) = self.upstream.handle.budget.try_acquire(text.len(), false) else {
            self.disconnect_client(client_id, 1011, "Payload budget exhausted", true);
            return;
        };
        let Some(metadata) = self.upstream.handle.budget.try_acquire(64 * 1024, false) else {
            self.disconnect_client(client_id, 1011, "Payload budget exhausted", true);
            return;
        };
        let Some(value) = Request::parse(&text, payload, metadata) else {
            self.send_error(client_id, 0, -32000, "Invalid CDP request", None);
            return;
        };
        let Some(external_id) = value.get("id").and_then(safe_integer) else {
            self.send_error(
                client_id,
                0,
                -32000,
                "Invalid CDP request",
                error_session(&value),
            );
            return;
        };
        let Some(method) = value
            .get("method")
            .and_then(Value::as_str)
            .map(str::to_string)
        else {
            self.send_error(
                client_id,
                external_id,
                -32000,
                "Invalid CDP request",
                error_session(&value),
            );
            return;
        };
        drop(text);
        drop(ingress);
        let mut value = value;
        let Some(client) = self.clients.get(&client_id) else {
            return;
        };
        if client.ids.lock().unwrap().contains(&external_id) {
            self.send_error(
                client_id,
                external_id,
                -32000,
                "Duplicate in-flight request id",
                error_session(&value),
            );
            return;
        }
        let permits = client
            .slots
            .clone()
            .try_acquire_owned()
            .ok()
            .zip(self.request_slots.clone().try_acquire_owned().ok());
        let Some((local, global)) = permits else {
            self.send_error(
                client_id,
                external_id,
                -32000,
                "Too many in-flight CDP requests",
                error_session(&value),
            );
            return;
        };
        let metadata = value.metadata.take().unwrap();
        client.ids.lock().unwrap().insert(external_id);
        value.lease = Some(Arc::new(RequestLease {
            _client: local,
            _global: global,
            ids: client.ids.clone(),
            id: external_id,
            _metadata: metadata,
        }));
        self.ensure_page_ready(client_id, value, external_id, &method);
    }

    fn ensure_page_ready(
        &mut self,
        client_id: ClientId,
        request: Request,
        external_id: i64,
        method: &str,
    ) {
        let Some(client) = self.clients.get_mut(&client_id) else {
            return;
        };
        if client.page_ready {
            self.admit_request(client_id, request, external_id, method);
            return;
        }
        let bytes = request._payload.bytes();
        if client.page_queue.len() >= MAX_PAGE_QUEUE
            || client.page_queue_bytes.saturating_add(bytes) > MAX_PAGE_QUEUE_BYTES
        {
            self.disconnect_client(client_id, 1011, "Slow client", true);
            return;
        }
        client.page_queue_bytes = client.page_queue_bytes.saturating_add(bytes);
        client.page_queue.push_back(request);
    }

    fn admit_request(
        &mut self,
        client_id: ClientId,
        request: Request,
        external_id: i64,
        method: &str,
    ) {
        let Some(client) = self.clients.get(&client_id) else {
            return;
        };
        let session = error_session(&request);
        if self.lifecycle != Lifecycle::Running {
            let message = if self.lifecycle == Lifecycle::Flushing {
                "Browser is flushing"
            } else {
                "Browser is closing"
            };
            self.send_error(client_id, external_id, -32000, message, session);
            return;
        }
        if client.role == Role::Viewer && !VIEW_METHODS.contains(&method) {
            self.send_error(
                client_id,
                external_id,
                -32000,
                "Viewer connection cannot control the browser",
                session,
            );
            return;
        }
        if client.role == Role::Controller {
            let valid = !client.retired
                && self.active_controller == Some(client_id)
                && self.controller.as_deref() == Some(client.owner.as_str())
                && client.epoch == self.control_epoch;
            if !valid {
                self.send_error(
                    client_id,
                    external_id,
                    -32000,
                    "Control has changed; reconnect and observe before acting",
                    session,
                );
                return;
            }
        }
        if self.has_external_pending(client_id, external_id) && request.lease.is_none() {
            self.send_error(
                client_id,
                external_id,
                -32000,
                "Duplicate in-flight request id",
                session,
            );
            return;
        }
        if let Err(message) = self.validate_session(client_id, session.as_deref()) {
            self.send_error(client_id, external_id, -32000, message, session);
            return;
        }
        if method == "Browser.close" {
            self.request_close_from_client(client_id, external_id, session);
            if let Some(close) = self.close.as_mut() {
                close._request = request.lease;
            }
            return;
        }
        if is_managed_method(method) {
            self.queue_managed(client_id, request, external_id, method);
            return;
        }
        let Some(resolved) = self.resolve_session(client_id, &request, method) else {
            return;
        };
        self.forward_request(client_id, &request, external_id, method, resolved);
    }

    fn request_close_from_client(
        &mut self,
        client_id: ClientId,
        external_id: i64,
        session: Option<String>,
    ) {
        if self.lifecycle != Lifecycle::Running {
            self.send_error(
                client_id,
                external_id,
                -32000,
                "Browser is closing",
                session,
            );
            return;
        }
        let error = self.start_close(
            CloseKind::Cdp,
            Some(client_id),
            Some(external_id),
            session,
            None,
        );
        if let Err(message) = error {
            self.send_error(client_id, external_id, -32000, &message, None);
        }
    }

    fn validate_session(
        &self,
        client: ClientId,
        session: Option<&str>,
    ) -> Result<(), &'static str> {
        if session.is_some_and(|session| self.attachments.owner(session) != Some(client)) {
            return Err("CDP attachment expired or belongs to another connection; attach again");
        }
        Ok(())
    }

    fn resolve_session(
        &mut self,
        client_id: ClientId,
        request: &Value,
        method: &str,
    ) -> Option<Option<String>> {
        let explicit = request
            .get("sessionId")
            .and_then(Value::as_str)
            .map(str::to_string);
        let external_id = request.get("id").and_then(safe_integer).unwrap_or_default();
        let session = error_session(request);
        if let Err(message) = self.validate_session(client_id, explicit.as_deref()) {
            self.send_error(client_id, external_id, -32000, message, session);
            return None;
        }
        if method == "Target.detachFromTarget" {
            let target_session = request
                .get("params")
                .and_then(|params| params.get("sessionId"))
                .and_then(Value::as_str);
            if let Some(target_session) = target_session {
                if self.attachments.owner(target_session) != Some(client_id) {
                    self.send_error(
                        client_id,
                        external_id,
                        -32000,
                        "This connection does not own that attachment",
                        session,
                    );
                    return None;
                }
            }
        }
        let resolved = match explicit {
            Some(session_id) => Some(session_id),
            None => self.synthetic_session(client_id, method, external_id, session)?,
        };
        if method == "Target.setDiscoverTargets" {
            let discover = request
                .get("params")
                .and_then(|params| params.get("discover"))
                .and_then(Value::as_bool)
                .unwrap_or(false);
            if let Some(client) = self.clients.get_mut(&client_id) {
                client.discover = discover;
            }
        }
        Some(resolved)
    }

    fn synthetic_session(
        &mut self,
        client_id: ClientId,
        method: &str,
        external_id: i64,
        session: Option<String>,
    ) -> Option<Option<String>> {
        let client = self.clients.get(&client_id)?;
        if let Endpoint::Page { .. } = client.endpoint {
            let primary = client.primary.clone();
            if primary.is_none() && method != "Target.attachToTarget" {
                self.send_error(client_id, external_id, -32000, "Target expired", session);
                return None;
            }
            return Some(primary);
        }
        Some(None)
    }

    fn forward_request(
        &mut self,
        client_id: ClientId,
        request: &Request,
        external_id: i64,
        method: &str,
        session: Option<String>,
    ) {
        let params = request.params.as_deref();
        let client = match self.clients.get(&client_id) {
            Some(client) => client,
            None => return,
        };
        let spec = PendingSpec {
            client: Some(client_id),
            role: Some(client.role),
            external_id: Some(external_id),
            explicit_session: error_session(request),
            epoch: client.epoch,
            method: method.to_string(),
            kind: PendingKind::Client,
            deadline: self.client_deadline,
        };
        let result = self.send_raw_request(
            method,
            params,
            session.as_deref(),
            spec,
            request.lease.clone(),
        );
        match result {
            Ok(_) => {}
            Err(message) => {
                self.send_error(client_id, external_id, -32000, &message, session);
            }
        }
    }

    fn queue_managed(
        &mut self,
        client_id: ClientId,
        request: Request,
        external_id: i64,
        method: &str,
    ) {
        if self.management_queue.len() >= MAX_MANAGEMENT_QUEUE {
            self.send_error(
                client_id,
                external_id,
                -32000,
                "Too many pending attachment operations",
                error_session(&request),
            );
            return;
        }
        let Some(client) = self.clients.get(&client_id) else {
            return;
        };
        let explicit_session = request
            .get("sessionId")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| match &client.endpoint {
                Endpoint::Page { .. } => client.primary.clone(),
                Endpoint::Browser => None,
            });
        // 入队时校验顶层与 detach 参数 session；排队期间归属可能变化，派发前再校验。
        if let Some(message) =
            self.managed_session_error(client_id, method, &request, explicit_session.as_deref())
        {
            self.send_error(
                client_id,
                external_id,
                -32000,
                message,
                error_session(&request),
            );
            return;
        }
        let work = ManagedWork {
            retained: Vec::new(),
            client: client_id,
            epoch: client.epoch,
            external_id,
            explicit_session,
            method: method.to_string(),
            params: request.get("params").cloned().unwrap_or(Value::Null),
            request,
        };
        self.management_queue.push_back(work);
        self.pump_management();
    }

    fn has_external_pending(&self, client: ClientId, external_id: i64) -> bool {
        self.pending.values().any(|pending| {
            pending.client == Some(client) && pending.external_id == Some(external_id)
        }) || self
            .management_queue
            .iter()
            .any(|work| work.client == client && work.external_id == external_id)
    }

    fn send_error(
        &mut self,
        client: ClientId,
        external_id: i64,
        code: i64,
        message: &str,
        session: Option<String>,
    ) {
        #[derive(serde::Serialize)]
        struct ErrorBody<'a> {
            code: i64,
            message: &'a str,
        }
        #[derive(serde::Serialize)]
        struct ErrorReply<'a> {
            id: i64,
            error: ErrorBody<'a>,
            #[serde(rename = "sessionId", skip_serializing_if = "Option::is_none")]
            session: Option<&'a str>,
        }
        let reply = ErrorReply {
            id: external_id,
            error: ErrorBody { code, message },
            session: session.as_deref(),
        };
        let Some((text, lease)) =
            crate::transport::serialize_payload(&self.upstream.handle.budget, &reply, false)
        else {
            self.disconnect_client(client, 1011, "Payload budget exhausted", true);
            return;
        };
        self.enqueue(client, text, lease);
    }

    fn enqueue(&mut self, client: ClientId, text: String, mut lease: PayloadLease) -> bool {
        let Some(state) = self.clients.get_mut(&client) else {
            return false;
        };
        if state.closed || state.writer_gone {
            return false;
        }
        let bytes = text.len();
        let permits = state.output_frames.clone().try_acquire_owned().ok().zip(
            state
                .output_bytes
                .clone()
                .try_acquire_many_owned(bytes as u32)
                .ok(),
        );
        let Some((frame, bytes)) = permits else {
            self.disconnect_client(client, 1011, "Slow client", true);
            return false;
        };
        lease.hold(frame);
        lease.hold(bytes);
        if state
            .writer
            .try_send(ClientCommand::Text(text, lease))
            .is_err()
        {
            self.disconnect_client(client, 1011, "Slow client", true);
            return false;
        }
        true
    }

    fn send_reply(
        &mut self,
        client: ClientId,
        external_id: i64,
        result: Value,
        session: Option<String>,
    ) {
        #[derive(serde::Serialize)]
        struct Reply<'a> {
            id: i64,
            result: &'a Value,
            #[serde(rename = "sessionId", skip_serializing_if = "Option::is_none")]
            session: Option<&'a str>,
        }
        let reply = Reply {
            id: external_id,
            result: &result,
            session: session.as_deref(),
        };
        let Some((text, lease)) =
            crate::transport::serialize_payload(&self.upstream.handle.budget, &reply, false)
        else {
            self.disconnect_client(client, 1011, "Payload budget exhausted", true);
            return;
        };
        self.enqueue(client, text, lease);
    }

    fn send_upstream_reply(
        &mut self,
        client: ClientId,
        external_id: i64,
        session: Option<String>,
        response: &Value,
    ) {
        #[derive(serde::Serialize)]
        struct Reply<'a> {
            id: i64,
            #[serde(skip_serializing_if = "Option::is_none")]
            result: Option<&'a Value>,
            #[serde(skip_serializing_if = "Option::is_none")]
            error: Option<&'a Value>,
            #[serde(rename = "sessionId", skip_serializing_if = "Option::is_none")]
            session: Option<&'a str>,
        }
        let reply = Reply {
            id: external_id,
            result: response.get("result"),
            error: response.get("error"),
            session: session.as_deref(),
        };
        let Some((text, lease)) =
            crate::transport::serialize_payload(&self.upstream.handle.budget, &reply, false)
        else {
            self.disconnect_client(client, 1011, "Payload budget exhausted", true);
            return;
        };
        self.enqueue(client, text, lease);
    }

    fn deliver_event(&mut self, client: ClientId, value: &Value) {
        let Some(state) = self.clients.get(&client) else {
            return;
        };
        if state.closed || state.retired || state.writer_gone {
            return;
        }
        if matches!(state.endpoint, Endpoint::Page { .. }) {
            let primary = state.primary.clone();
            let outer = value
                .get("sessionId")
                .and_then(Value::as_str)
                .map(str::to_string);
            if primary.is_none() || outer.as_deref() != primary.as_deref() {
                return;
            }
        }
        // 只重写 envelope；大 params/result 始终借用，广播不 clone Value。
        let filtered = value.as_object().map(|map| {
            map.iter()
                .filter(|(key, _)| {
                    key.as_str() != "sessionId" || !matches!(state.endpoint, Endpoint::Page { .. })
                })
                .collect::<std::collections::BTreeMap<_, _>>()
        });
        let Some((text, lease)) =
            crate::transport::serialize_payload(&self.upstream.handle.budget, &filtered, false)
        else {
            self.disconnect_client(client, 1011, "Payload budget exhausted", true);
            return;
        };
        self.enqueue(client, text, lease);
    }

    fn disconnect_client(&mut self, client: ClientId, code: u16, reason: &str, retire: bool) {
        let Some(state) = self.clients.get_mut(&client) else {
            return;
        };
        let was_closed = state.closed;
        state.closed = true;
        state.page_queue.clear();
        state.page_queue_bytes = 0;
        self.management_queue.retain(|work| work.client != client);
        let writer = state.writer.clone();
        let cancel = state.cancel.clone();
        let attached =
            self.attachments.count_for(client) > 0 || self.attachments.auto_owner == Some(client);
        let _ = writer.try_send(ClientCommand::Close {
            code,
            reason: reason.to_string(),
        });
        let _ = cancel.send(true);
        if self.active_controller == Some(client) {
            self.active_controller = None;
        }
        self.observer.with(
            "client-closed",
            json!({ "kind": "client-closed", "clients": self.clients.len() }),
        );
        if retire && !was_closed && attached && self.lifecycle != Lifecycle::Lost {
            self.start_retirement(client);
        }
        if !attached && self.clients.get(&client).is_some_and(|state| state.closed) {
            let pending = self
                .pending
                .values()
                .any(|pending| pending.client == Some(client));
            if !pending {
                self.clients.remove(&client);
            }
        }
    }

    fn client_closed(&mut self, client: ClientId) {
        let Some(state) = self.clients.get_mut(&client) else {
            return;
        };
        if state.closed {
            return;
        }
        state.closed = true;
        state.page_queue.clear();
        state.page_queue_bytes = 0;
        self.management_queue.retain(|work| work.client != client);
        if self.active_controller == Some(client) {
            self.active_controller = None;
        }
        let needs_retire =
            self.attachments.count_for(client) > 0 || self.attachments.auto_owner == Some(client);
        if needs_retire && self.lifecycle != Lifecycle::Lost {
            self.start_retirement(client);
            return;
        }
        let has_pending = self
            .pending
            .values()
            .any(|pending| pending.client == Some(client));
        if !has_pending {
            self.clients.remove(&client);
        }
    }
}

fn valid_owner(owner: &str) -> bool {
    let units = owner.encode_utf16().count();
    !owner.is_empty() && units <= MAX_OWNER_UTF16
}

fn safe_integer(value: &Value) -> Option<i64> {
    if let Some(number) = value.as_i64() {
        return (number.unsigned_abs() <= MAX_SAFE_INTEGER).then_some(number);
    }
    let number = value.as_f64()?;
    if number.fract() != 0.0 || number.abs() > MAX_SAFE_INTEGER as f64 {
        return None;
    }
    Some(number as i64)
}

fn error_session(request: &Value) -> Option<String> {
    request
        .get("sessionId")
        .and_then(Value::as_str)
        .map(str::to_string)
}

fn is_managed_method(method: &str) -> bool {
    matches!(
        method,
        "Target.attachToTarget"
            | "Target.setAutoAttach"
            | "Target.detachFromTarget"
            | "Target.attachToBrowserTarget"
    )
}

struct PendingSpec {
    client: Option<ClientId>,
    role: Option<Role>,
    external_id: Option<i64>,
    explicit_session: Option<String>,
    epoch: u64,
    method: String,
    kind: PendingKind,
    deadline: Duration,
}
impl Broker {
    fn send_request(
        &mut self,
        method: &str,
        params: Option<&Value>,
        session: Option<&str>,
        spec: PendingSpec,
    ) -> Result<u64, String> {
        let raw = params
            .map(serde_json::value::to_raw_value)
            .transpose()
            .map_err(|_| "invalid CDP request")?;
        self.send_raw_request(method, raw.as_deref(), session, spec, None)
    }

    fn send_raw_request(
        &mut self,
        method: &str,
        params: Option<&RawValue>,
        session: Option<&str>,
        spec: PendingSpec,
        request: Option<Arc<RequestLease>>,
    ) -> Result<u64, String> {
        if self.unknown.len() >= MAX_UNKNOWN {
            return Err("Too many unresolved CDP requests".to_string());
        }
        if let Some(client_id) = spec.client {
            let Some(client) = self.clients.get(&client_id) else {
                return Err("CDP connection ended".to_string());
            };
            let _ = client;
        } else if self.internal_pending >= MAX_INTERNAL_PENDING {
            return Err("Too many in-flight host operations".to_string());
        }
        let internal = if request.is_none() {
            let slot = self
                .internal_slots
                .clone()
                .try_acquire_owned()
                .map_err(|_| "Too many in-flight host operations")?;
            let metadata = self
                .upstream
                .handle
                .budget
                .try_acquire(16 * 1024, true)
                .ok_or("Host metadata budget exhausted")?;
            Some(Arc::new(InternalRequestLease {
                _slot: slot,
                _metadata: metadata,
            }))
        } else {
            None
        };
        let Some(id) = self.upstream.handle.allocate_id() else {
            return Err("CDP request ids are exhausted".to_string());
        };
        #[derive(serde::Serialize)]
        struct Outbound<'a> {
            id: u64,
            method: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            params: Option<&'a RawValue>,
            #[serde(rename = "sessionId", skip_serializing_if = "Option::is_none")]
            session: Option<&'a str>,
        }
        let (text, lease) = crate::transport::serialize_payload(
            &self.upstream.handle.budget,
            &Outbound {
                id,
                method,
                params,
                session,
            },
            spec.client.is_none(),
        )
        .ok_or("Browser transport is congested")?;
        let bytes = text.len();
        if !self.upstream.handle.send_leased(text, lease) {
            return Err("Browser session is lost".to_string());
        }
        self.upstream_bytes = self.upstream_bytes.saturating_add(bytes);
        if let Some(client_id) = spec.client {
            if let Some(client) = self.clients.get_mut(&client_id) {
                client.pending += 1;
            }
            self.client_pending += 1;
        } else {
            self.internal_pending += 1;
        }
        self.pending.insert(
            id,
            Pending {
                internal,
                request,
                client: spec.client,
                role: spec.role,
                external_id: spec.external_id,
                explicit_session: spec.explicit_session,
                epoch: spec.epoch,
                method: spec.method,
                kind: spec.kind,
                deadline: Instant::now() + spec.deadline,
            },
        );
        Ok(id)
    }

    fn request_targets(&mut self, reply: oneshot::Sender<Result<LeasedTargets, String>>) {
        if matches!(self.lifecycle, Lifecycle::Lost | Lifecycle::Closed) {
            let _ = reply.send(Err("browser session is lost".to_string()));
            return;
        }
        let spec = PendingSpec {
            client: None,
            role: None,
            external_id: None,
            explicit_session: None,
            epoch: self.control_epoch,
            method: "Target.getTargets".to_string(),
            kind: PendingKind::Targets(reply),
            deadline: self.internal_deadline,
        };
        if let Err(message) = self.send_request("Target.getTargets", None, None, spec) {
            self.observer.kind("targets-unavailable");
            let _ = message;
        }
    }

    fn start_page_attach(&mut self, client_id: ClientId, target: &str) -> bool {
        let Some(client) = self.clients.get(&client_id) else {
            return false;
        };
        let params = json!({ "targetId": target, "flatten": true });
        if !self.attachments.set_intent(target, client_id) {
            return false;
        }
        let spec = PendingSpec {
            client: Some(client_id),
            role: Some(client.role),
            external_id: None,
            explicit_session: None,
            epoch: client.epoch,
            method: "Target.attachToTarget".to_string(),
            kind: PendingKind::PageAttach {
                target: target.to_string(),
            },
            deadline: self.internal_deadline,
        };
        match self.send_request("Target.attachToTarget", Some(&params), None, spec) {
            Ok(_) => true,
            Err(_) => {
                self.attachments.clear_intent(target, client_id);
                false
            }
        }
    }

    fn upstream_event(&mut self, event: UpstreamEvent) {
        // 仅同步 envelope/intent 副本借 cleanup scratch，不能跨 actor 迭代保留。
        let Some(_scratch) = self.upstream.handle.budget.try_acquire(64 * 1024, true) else {
            self.handle_lost("upstream metadata budget exhausted".into());
            return;
        };
        if let UpstreamEvent::Response(value, _) | UpstreamEvent::Event(value, _) = &event {
            let params = value.get("params");
            let result = value.get("result");
            let fields = [
                value.get("method"),
                value.get("sessionId"),
                params.and_then(|p| p.get("sessionId")),
                params
                    .and_then(|p| p.get("targetInfo"))
                    .and_then(|p| p.get("targetId")),
                result.and_then(|p| p.get("sessionId")),
            ];
            if fields
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .any(|s| s.len() > 4096)
            {
                self.handle_lost("upstream identity limit exceeded".into());
                return;
            }
        }
        match event {
            UpstreamEvent::Response(value, _lease) => self.handle_response(value),
            UpstreamEvent::Event(value, _lease) => self.handle_event(value),
            UpstreamEvent::Written { bytes } => {
                self.upstream_bytes = self.upstream_bytes.saturating_sub(bytes);
            }
            UpstreamEvent::Lost(reason) => self.handle_lost(reason),
        }
    }

    /// 事件只有在能证明归属时才投递；无 session 的普通事件不广播。
    fn own_attachment(
        &mut self,
        client: ClientId,
        epoch: u64,
        session: &str,
        target: &str,
        parent: Option<String>,
    ) {
        if !self.attachments.own(client, epoch, session, target, parent) {
            self.handle_lost("attachment metadata budget exhausted".into());
        }
    }

    fn handle_event(&mut self, value: Value) {
        let Some(method) = value
            .get("method")
            .and_then(Value::as_str)
            .map(str::to_string)
        else {
            return;
        };
        let outer = value
            .get("sessionId")
            .and_then(Value::as_str)
            .map(str::to_string);
        if method == "Target.attachedToTarget" {
            self.attached_event(value, outer);
            return;
        }
        if method == "Target.detachedFromTarget" {
            self.detached_event(value);
            return;
        }
        if outer.is_none()
            && matches!(
                method.as_str(),
                "Target.targetCreated" | "Target.targetInfoChanged" | "Target.targetDestroyed"
            )
        {
            let recipients = self
                .clients
                .iter()
                .filter(|(_, state)| {
                    state.discover
                        && !state.closed
                        && !state.retired
                        && matches!(state.endpoint, Endpoint::Browser)
                })
                .map(|(client, _)| *client)
                .collect::<Vec<_>>();
            for client in recipients {
                self.deliver_event(client, &value);
            }
            return;
        }
        let Some(outer) = outer else {
            return;
        };
        let Some(owner) = self.attachments.owner(&outer) else {
            return;
        };
        self.deliver_event(owner, &value);
    }

    fn attached_event(&mut self, value: Value, outer: Option<String>) {
        let params = value.get("params");
        let Some(session) = params
            .and_then(|params| params.get("sessionId"))
            .and_then(Value::as_str)
            .map(str::to_string)
        else {
            return;
        };
        let Some(target) = params
            .and_then(|params| params.get("targetInfo"))
            .and_then(|info| info.get("targetId"))
            .and_then(Value::as_str)
            .map(str::to_string)
        else {
            return;
        };
        let Some(owner) = self.attachments.resolve_owner(outer.as_deref(), &target) else {
            return;
        };
        let Some(state) = self.clients.get(&owner) else {
            return;
        };
        let epoch = state.epoch;
        let retired = state.closed || state.retired;
        let known = self.attachments.attachment(&session).is_some();
        if !known
            && (self.attachments.count() >= MAX_ATTACHMENTS
                || self.attachments.count_for(owner) >= MAX_ATTACHMENTS_PER_CLIENT)
        {
            return;
        }
        self.own_attachment(owner, epoch, &session, &target, outer);
        if retired {
            self.start_retirement(owner);
            return;
        }
        self.deliver_event(owner, &value);
    }

    fn detached_event(&mut self, value: Value) {
        let Some(session) = value
            .get("params")
            .and_then(|params| params.get("sessionId"))
            .and_then(Value::as_str)
            .map(str::to_string)
        else {
            return;
        };
        let Some(recipient) = self.attachments.owner(&session) else {
            return;
        };
        let primary = self
            .clients
            .get(&recipient)
            .is_some_and(|state| state.primary.as_deref() == Some(session.as_str()));
        self.attachments.forget(&session);
        if primary {
            self.disconnect_client(recipient, 1008, "Target expired", true);
            return;
        }
        self.deliver_event(recipient, &value);
    }

    fn handle_response(&mut self, value: Value) {
        let Some(id) = value.get("id").and_then(Value::as_u64) else {
            return;
        };
        if self.pending.contains_key(&id) {
            self.complete_pending(id, &value);
            return;
        }
        if self.unknown.contains_key(&id) {
            self.complete_unknown(id, &value);
        }
    }

    fn record_unknown(&mut self, id: u64, unknown: Unknown) {
        if self.unknown.len() < MAX_UNKNOWN {
            self.unknown.insert(id, unknown);
        } else {
            self.handle_lost("unresolved request metadata capacity exhausted".into());
        }
    }

    /// 超时不是取消：已发送的写命令保留“结果未知”，迟到回复仍修复归属但不再算成功。
    fn expire_pending(&mut self, id: u64) {
        let Some(pending) = self.pending.remove(&id) else {
            return;
        };
        self.release_pending(&pending);
        let message = format!("CDP timeout: {}; outcome may be unknown", pending.method);
        let client = pending.client;
        let role = pending.role;
        let external_id = pending.external_id;
        let explicit_session = pending.explicit_session.clone();
        let epoch = pending.epoch;
        let method = pending.method.clone();
        match pending.kind {
            PendingKind::Targets(reply) => {
                let _ = reply.send(Err("Target discovery timed out".to_string()));
            }
            PendingKind::Flush => {
                self.fail_flush(message);
                self.record_unknown(
                    id,
                    Unknown {
                        _request: pending.request.clone(),
                        _internal: pending.internal.clone(),
                        client,
                        role,
                        epoch,
                        method,
                        kind: UnknownKind::Flush,
                    },
                );
            }
            PendingKind::Close => {
                let browser_stage = self
                    .close
                    .as_ref()
                    .is_some_and(|state| matches!(state.stage, CloseStage::Browser));
                if browser_stage {
                    if self.close.as_ref().is_some_and(|state| !state.notified) {
                        if let Some(state) = self.close.as_mut() {
                            state.notified = true;
                        }
                        self.close_restricted(message);
                    }
                    self.record_unknown(
                        id,
                        Unknown {
                            _request: pending.request.clone(),
                            _internal: pending.internal.clone(),
                            client,
                            role,
                            epoch,
                            method,
                            kind: UnknownKind::Close,
                        },
                    );
                } else {
                    self.fail_close(message);
                }
            }
            PendingKind::PageAttach { target } => {
                if let Some(client) = client {
                    self.disconnect_client(client, 1008, "Target expired", true);
                    self.record_unknown(
                        id,
                        Unknown {
                            _request: pending.request.clone(),
                            _internal: pending.internal.clone(),
                            client: Some(client),
                            role,
                            epoch,
                            method,
                            kind: UnknownKind::PageAttach { target },
                        },
                    );
                }
            }
            PendingKind::Managed => {
                let active = self
                    .management_active
                    .take()
                    .filter(|active| stage_id(&active.stage) == Some(id));
                if let Some(active) = active {
                    let managed_client = active.work.client;
                    let managed_external = active.work.external_id;
                    let managed_session = active.work.explicit_session.clone();
                    let managed_role = self
                        .clients
                        .get(&managed_client)
                        .map_or(role, |state| Some(state.role));
                    let target = active
                        .work
                        .params
                        .get("targetId")
                        .and_then(Value::as_str)
                        .map(str::to_string);
                    let session = active
                        .work
                        .params
                        .get("sessionId")
                        .and_then(Value::as_str)
                        .map(str::to_string);
                    let auto = active
                        .work
                        .params
                        .get("autoAttach")
                        .and_then(Value::as_bool);
                    // 迟到回复仍可能改变归属，所以保留 intent 与 auto owner 直到它到达。
                    self.send_error(
                        managed_client,
                        managed_external,
                        -32000,
                        &message,
                        managed_session.clone(),
                    );
                    self.record_unknown(
                        id,
                        Unknown {
                            _request: pending.request.clone(),
                            _internal: pending.internal.clone(),
                            client: Some(managed_client),
                            role: managed_role,
                            epoch,
                            method,
                            kind: UnknownKind::Managed {
                                target,
                                session,
                                auto,
                            },
                        },
                    );
                } else if let Some(client) = client {
                    self.send_error(
                        client,
                        external_id.unwrap_or_default(),
                        -32000,
                        &message,
                        explicit_session,
                    );
                }
            }
            PendingKind::RetirementAuto { id: retirement } => {
                self.block_retirement(retirement);
                let retire_client = self
                    .retirements
                    .iter()
                    .find(|retire| retire.id == retirement)
                    .map(|retire| retire.client);
                if let Some(retire_client) = retire_client {
                    self.record_unknown(
                        id,
                        Unknown {
                            _request: pending.request.clone(),
                            _internal: pending.internal.clone(),
                            client: Some(retire_client),
                            role: Some(Role::Controller),
                            epoch,
                            method,
                            kind: UnknownKind::RetirementAuto { id: retirement },
                        },
                    );
                }
            }
            PendingKind::RetirementDetach {
                id: retirement,
                session,
            } => {
                self.block_retirement(retirement);
                let retire_client = self
                    .retirements
                    .iter()
                    .find(|retire| retire.id == retirement)
                    .map(|retire| retire.client);
                if let Some(retire_client) = retire_client {
                    self.record_unknown(
                        id,
                        Unknown {
                            _request: pending.request.clone(),
                            _internal: pending.internal.clone(),
                            client: Some(retire_client),
                            role: Some(Role::Controller),
                            epoch,
                            method,
                            kind: UnknownKind::RetirementDetach { session },
                        },
                    );
                }
            }
            PendingKind::Client => {
                if let Some(client) = client {
                    self.send_error(
                        client,
                        external_id.unwrap_or_default(),
                        -32000,
                        &message,
                        explicit_session,
                    );
                }
                {
                    self.record_unknown(
                        id,
                        Unknown {
                            _request: pending.request.clone(),
                            _internal: pending.internal.clone(),
                            client,
                            role,
                            epoch,
                            method,
                            kind: UnknownKind::Plain,
                        },
                    );
                }
            }
        }
        self.pump_after_change();
    }

    fn block_retirement(&mut self, id: u64) {
        if let Some(retire) = self.retirements.iter_mut().find(|retire| retire.id == id) {
            retire.pending = None;
            retire.blocked = true;
        }
    }

    fn own_late(&mut self, client: Option<ClientId>, epoch: u64, session: &str, target: &str) {
        let Some(client) = client else {
            return;
        };
        if let Some(existing) = self.attachments.attachment(session) {
            if existing.client == client {
                return;
            }
        }
        if self.attachments.count() >= MAX_ATTACHMENTS
            || self.attachments.count_for(client) >= MAX_ATTACHMENTS_PER_CLIENT
        {
            return;
        }
        self.own_attachment(client, epoch, session, target, None);
        let retired = self
            .clients
            .get(&client)
            .is_none_or(|state| state.closed || state.retired);
        if retired {
            self.start_retirement(client);
        }
    }

    fn complete_unknown(&mut self, id: u64, value: &Value) {
        let Some(unknown) = self.unknown.remove(&id) else {
            return;
        };
        let attached = attach_session(value);
        if let Some(session) = attached.as_deref() {
            let target = match &unknown.kind {
                UnknownKind::PageAttach { target } => Some(target.clone()),
                UnknownKind::Managed { target, .. } => target.clone(),
                _ => None,
            };
            if let Some(target) = target {
                self.own_late(unknown.client, unknown.epoch, session, &target);
            }
        }
        let failed = value.get("error").is_some();
        match unknown.kind {
            UnknownKind::Managed {
                target,
                session,
                auto,
            } => {
                if let Some(client) = unknown.client {
                    if let Some(target) = target {
                        self.attachments.clear_intent(&target, client);
                    }
                    if unknown.method == "Target.detachFromTarget" {
                        if let Some(session) = session {
                            self.attachments.forget(&session);
                        }
                    }
                    if let Some(auto) = auto {
                        self.apply_auto_result(client, auto, failed);
                    }
                    if unknown.method == "Target.attachToBrowserTarget" {
                        if let Some(session) = attached.as_deref() {
                            self.own_late(Some(client), unknown.epoch, session, "browser");
                        }
                    }
                }
            }
            UnknownKind::RetirementAuto { id } => {
                let retire_client = self
                    .retirements
                    .iter()
                    .find(|retire| retire.id == id)
                    .map(|retire| retire.client);
                if let Some(client) = retire_client {
                    if self.attachments.auto_owner == Some(client) {
                        self.attachments.auto_owner = None;
                    }
                }
            }
            UnknownKind::RetirementDetach { session } => {
                self.attachments.forget(&session);
            }
            UnknownKind::Close => {
                if failed {
                    self.fail_close("browser close failed".to_string());
                } else {
                    self.finish_close_success();
                }
            }
            UnknownKind::Plain | UnknownKind::PageAttach { .. } | UnknownKind::Flush => {}
        }
        self.pump_after_change();
    }

    fn complete_pending(&mut self, id: u64, value: &Value) {
        let Some(pending) = self.pending.remove(&id) else {
            return;
        };
        self.release_pending(&pending);
        match pending.kind {
            PendingKind::Client => {
                if let Some(client) = pending.client {
                    self.send_upstream_reply(
                        client,
                        pending.external_id.unwrap_or_default(),
                        pending.explicit_session.clone(),
                        value,
                    );
                }
            }
            PendingKind::PageAttach { target } => {
                self.page_attach_response(pending.client, &target, value);
            }
            PendingKind::Managed => self.managed_response(id, value),
            PendingKind::Flush => self.flush_response(value),
            PendingKind::Close => self.close_response(value),
            PendingKind::Targets(reply) => {
                let result = targets_from(value, &self.upstream.handle.budget);
                let _ = reply.send(result);
            }
            PendingKind::RetirementAuto { id: retirement } => {
                self.retirement_auto_response(retirement, value);
            }
            PendingKind::RetirementDetach {
                id: retirement,
                session,
            } => {
                self.retirement_detach_response(retirement, &session, value);
            }
        }
        self.pump_after_change();
    }

    fn release_pending(&mut self, pending: &Pending) {
        if let Some(client) = pending.client {
            if let Some(state) = self.clients.get_mut(&client) {
                state.pending = state.pending.saturating_sub(1);
            }
            self.client_pending = self.client_pending.saturating_sub(1);
        } else {
            self.internal_pending = self.internal_pending.saturating_sub(1);
        }
    }

    fn page_attach_response(&mut self, client: Option<ClientId>, target: &str, value: &Value) {
        let Some(client_id) = client else {
            return;
        };
        let session = attach_session(value);
        self.attachments.clear_intent(target, client_id);
        let Some(session) = session else {
            self.disconnect_client(client_id, 1008, "Target expired", true);
            return;
        };
        let Some(state) = self.clients.get_mut(&client_id) else {
            return;
        };
        if self.attachments.count() >= MAX_ATTACHMENTS
            || self.attachments.count_for(client_id) >= MAX_ATTACHMENTS_PER_CLIENT
        {
            self.disconnect_client(client_id, 1008, "Target expired", true);
            return;
        }
        let epoch = state.epoch;
        state.primary = Some(session.clone());
        state.page_ready = true;
        let queued = std::mem::take(&mut state.page_queue);
        state.page_queue_bytes = 0;
        self.own_attachment(client_id, epoch, &session, target, None);
        for request in queued {
            let external_id = request.get("id").and_then(safe_integer).unwrap_or_default();
            let method = request
                .get("method")
                .and_then(Value::as_str)
                .map(str::to_string)
                .unwrap_or_default();
            self.admit_request(client_id, request, external_id, &method);
        }
    }

    fn managed_response(&mut self, id: u64, value: &Value) {
        // 回调中的错误 message/result 副本先收费；长期 backfill 持有自己的 lease。
        let Some(_copy) = crate::transport::payload_size(value)
            .and_then(|bytes| self.upstream.handle.budget.try_acquire(bytes, false))
        else {
            if let Some(active) = self.management_active.take() {
                self.finish_managed(
                    active.work,
                    Err((-32000, "Payload budget exhausted".into())),
                );
            }
            return;
        };
        let Some(mut active) = self.management_active.take() else {
            return;
        };
        let stage = std::mem::replace(&mut active.stage, ManagedStage::Sent { id: 0 });
        match stage {
            ManagedStage::Sent { id: current } if current == id => {
                self.managed_sent_response(active, value);
            }
            ManagedStage::BackfillTargets { id: current } if current == id => {
                self.backfill_targets(active, value);
            }
            ManagedStage::BackfillAttach {
                id: current,
                targets,
                index,
                target,
            } if current == id => {
                active.stage = ManagedStage::BackfillAttach {
                    id: current,
                    targets,
                    index,
                    target: target.clone(),
                };
                self.backfill_attach_response(active, &target, value);
            }
            stage => {
                active.stage = stage;
                self.management_active = Some(active);
            }
        }
    }

    fn managed_sent_response(&mut self, mut active: ActiveManaged, value: &Value) {
        let method = active.work.method.clone();
        let client = active.work.client;
        let params = active.work.params.clone();
        if let Some(error) = value.get("error") {
            self.clear_management_intent(&active);
            let message = error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("CDP command failed")
                .to_string();
            let code = error.get("code").and_then(Value::as_i64).unwrap_or(-32000);
            self.finish_managed(active.work, Err((code, message)));
            return;
        }
        let result_ref = value.get("result").unwrap_or(&Value::Null);
        let Some(retained) = crate::transport::payload_size(result_ref)
            .and_then(|bytes| self.upstream.handle.budget.try_acquire(bytes, false))
        else {
            self.finish_managed(
                active.work,
                Err((-32000, "Payload budget exhausted".into())),
            );
            return;
        };
        active.work.retained.push(retained);
        let result = result_ref.clone();
        if method == "Target.attachToTarget" {
            let Some(target) = params.get("targetId").and_then(Value::as_str) else {
                self.finish_managed(active.work, Err((-32000, "Invalid target".into())));
                return;
            };
            let Some(session) = attach_session(value) else {
                self.clear_management_intent(&active);
                self.finish_managed(
                    active.work,
                    Err((-32000, "Target attach returned no session".into())),
                );
                return;
            };
            self.clear_management_intent(&active);
            let epoch = self
                .clients
                .get(&client)
                .map_or(active.work.epoch, |state| state.epoch);
            self.own_attachment(
                client,
                epoch,
                &session,
                target,
                active.work.explicit_session.clone(),
            );
            self.finish_managed(active.work, Ok(result));
            return;
        }
        if method == "Target.detachFromTarget" {
            if let Some(session) = params.get("sessionId").and_then(Value::as_str) {
                self.attachments.forget(session);
            }
            self.finish_managed(active.work, Ok(result));
            return;
        }
        if method == "Target.attachToBrowserTarget" {
            if let Some(session) = attach_session(value) {
                let epoch = self
                    .clients
                    .get(&client)
                    .map_or(active.work.epoch, |state| state.epoch);
                self.own_attachment(client, epoch, &session, "browser", None);
            }
            self.finish_managed(active.work, Ok(result));
            return;
        }
        let auto_attach = params
            .get("autoAttach")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let backfill = auto_attach && active.work.explicit_session.is_none();
        active.response = result;
        if backfill && self.start_backfill(&mut active).is_ok() {
            self.management_active = Some(active);
            return;
        }
        self.finish_managed(active.work, Ok(active.response));
    }

    fn start_backfill(&mut self, active: &mut ActiveManaged) -> Result<(), ()> {
        let work = &active.work;
        let spec = PendingSpec {
            client: Some(work.client),
            role: self.clients.get(&work.client).map(|state| state.role),
            external_id: None,
            explicit_session: None,
            epoch: work.epoch,
            method: "Target.getTargets".to_string(),
            kind: PendingKind::Managed,
            deadline: self.internal_deadline,
        };
        let request = active.work.request.lease.clone();
        let id = self
            .send_raw_request("Target.getTargets", None, None, spec, request)
            .map_err(|_| ())?;
        active.stage = ManagedStage::BackfillTargets { id };
        Ok(())
    }

    fn backfill_targets(&mut self, mut active: ActiveManaged, value: &Value) {
        if let Some(error) = value.get("error") {
            let message = error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("CDP command failed")
                .to_string();
            self.finish_managed(active.work, Err((-32000, message)));
            return;
        }
        let infos = value
            .get("result")
            .and_then(|result| result.get("targetInfos"))
            .and_then(Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or_default();
        if infos
            .iter()
            .filter_map(|info| info.get("targetId"))
            .filter_map(Value::as_str)
            .any(|id| id.len() > 4096)
        {
            self.handle_lost("upstream identity limit exceeded".into());
            return;
        }
        let Some(retained) = crate::transport::payload_size(&infos).and_then(|bytes| {
            self.upstream
                .handle
                .budget
                .try_acquire(bytes.saturating_mul(4), false)
        }) else {
            self.finish_managed(
                active.work,
                Err((-32000, "Payload budget exhausted".into())),
            );
            return;
        };
        active.work.retained.push(retained);
        let targets = infos
            .iter()
            .filter(|info| info.get("type").and_then(Value::as_str) == Some("page"))
            .filter_map(|info| info.get("targetId").and_then(Value::as_str))
            .filter(|target| !self.attachments.has_target(active.work.client, target))
            .map(str::to_string)
            .collect::<Vec<_>>();
        active.stage = ManagedStage::BackfillAttach {
            id: 0,
            targets,
            index: 0,
            target: String::new(),
        };
        self.continue_backfill(active);
    }

    fn continue_backfill(&mut self, mut active: ActiveManaged) {
        match self.dispatch_next_backfill(&mut active) {
            BackfillStep::Sent => self.management_active = Some(active),
            BackfillStep::Done => self.finish_managed(active.work, Ok(active.response)),
            BackfillStep::Failed(message) => {
                self.finish_managed(active.work, Err((-32000, message)))
            }
        }
    }

    fn dispatch_next_backfill(&mut self, active: &mut ActiveManaged) -> BackfillStep {
        let client = active.work.client;
        let epoch = active.work.epoch;
        let explicit_session = active.work.explicit_session.clone();
        let (candidate, next) = {
            let ManagedStage::BackfillAttach {
                targets,
                index,
                target,
                ..
            } = &mut active.stage
            else {
                return BackfillStep::Done;
            };
            let mut next = *index;
            let mut candidate = None;
            while next < targets.len() {
                let current = targets[next].clone();
                next += 1;
                if self.attachments.has_target(client, &current) {
                    continue;
                }
                candidate = Some(current);
                break;
            }
            let Some(candidate) = candidate else {
                return BackfillStep::Done;
            };
            *index = next;
            *target = candidate.clone();
            (candidate, next)
        };
        let _ = next;
        if !self.attachments.set_intent(&candidate, client) {
            return BackfillStep::Failed("Payload budget exhausted".into());
        }
        let params = json!({ "targetId": candidate, "flatten": true });
        let spec = PendingSpec {
            client: Some(client),
            role: self.clients.get(&client).map(|state| state.role),
            external_id: None,
            explicit_session,
            epoch,
            method: "Target.attachToTarget".to_string(),
            kind: PendingKind::Managed,
            deadline: self.internal_deadline,
        };
        let raw = serde_json::value::to_raw_value(&params).unwrap();
        match self.send_raw_request(
            "Target.attachToTarget",
            Some(&raw),
            None,
            spec,
            active.work.request.lease.clone(),
        ) {
            Ok(id) => {
                if let ManagedStage::BackfillAttach { id: slot, .. } = &mut active.stage {
                    *slot = id;
                }
                BackfillStep::Sent
            }
            Err(_) => {
                self.attachments.clear_intent(&candidate, client);
                BackfillStep::Failed("Attachment backfill failed".into())
            }
        }
    }

    fn backfill_attach_response(&mut self, active: ActiveManaged, target: &str, value: &Value) {
        let client = active.work.client;
        self.attachments.clear_intent(target, client);
        if let Some(error) = value.get("error") {
            let message = error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("CDP command failed")
                .to_string();
            self.finish_managed(active.work, Err((-32000, message)));
            return;
        }
        let Some(session) = attach_session(value) else {
            self.finish_managed(
                active.work,
                Err((-32000, "Target attach returned no session".into())),
            );
            return;
        };
        let epoch = self
            .clients
            .get(&client)
            .map_or(active.work.epoch, |state| state.epoch);
        self.own_attachment(client, epoch, &session, target, None);
        self.continue_backfill(active);
    }

    fn clear_management_intent(&mut self, active: &ActiveManaged) {
        if active.work.method != "Target.attachToTarget" {
            return;
        }
        if let Some(target) = active.work.params.get("targetId").and_then(Value::as_str) {
            self.attachments.clear_intent(target, active.work.client);
        }
    }

    fn finish_managed(&mut self, work: ManagedWork, result: Result<Value, (i64, String)>) {
        let exists = self
            .clients
            .get(&work.client)
            .is_some_and(|state| !state.closed);
        if exists {
            match result {
                Ok(value) => self.send_reply(
                    work.client,
                    work.external_id,
                    value,
                    work.explicit_session.clone(),
                ),
                Err((code, message)) => self.send_error(
                    work.client,
                    work.external_id,
                    code,
                    &message,
                    work.explicit_session.clone(),
                ),
            }
        }
        self.management_active = None;
        self.pump_management();
    }

    fn pump_management(&mut self) {
        if self.management_active.is_some()
            || self.lifecycle != Lifecycle::Running
            || self.flush.is_some()
            || self.close.is_some()
            || self.control_active.is_some()
            || !self.retirements.is_empty()
            || self
                .unknown
                .values()
                .any(|unknown| matches!(unknown.kind, UnknownKind::Managed { .. }))
        {
            return;
        }
        loop {
            let Some(work) = self.management_queue.pop_front() else {
                return;
            };
            if self.dispatch_managed(work) {
                return;
            }
        }
    }

    fn dispatch_managed(&mut self, mut work: ManagedWork) -> bool {
        if !self.client_can_act(work.client, work.epoch) {
            let session = work.explicit_session.clone();
            self.send_error(
                work.client,
                work.external_id,
                -32000,
                "Control has changed; reconnect and observe before acting",
                session,
            );
            return false;
        }
        // 顶层 session 与 detach 参数 session 都必须属于本连接；排队期间归属可能已变化。
        if let Some(session) = &work.explicit_session {
            if self.attachments.owner(session) != Some(work.client) {
                self.send_error(
                    work.client,
                    work.external_id,
                    -32000,
                    "CDP attachment expired or belongs to another connection; attach again",
                    work.explicit_session.clone(),
                );
                return false;
            }
        }
        let target = work
            .params
            .get("targetId")
            .and_then(Value::as_str)
            .map(str::to_string);
        let session = work.explicit_session.clone();
        let prepared = self.prepare_managed(&work, target.as_deref());
        let Ok(()) = prepared else {
            let message = "This connection does not own that attachment".to_string();
            self.send_error(
                work.client,
                work.external_id,
                -32000,
                &message,
                work.explicit_session.clone(),
            );
            return false;
        };
        let spec = PendingSpec {
            client: Some(work.client),
            role: self.clients.get(&work.client).map(|state| state.role),
            external_id: work.external_id.into(),
            explicit_session: work.explicit_session.clone(),
            epoch: work.epoch,
            method: work.method.clone(),
            kind: PendingKind::Managed,
            deadline: self.internal_deadline,
        };
        match self.send_raw_request(
            &work.method,
            work.request.params.as_deref(),
            session.as_deref(),
            spec,
            work.request.lease.clone(),
        ) {
            Ok(id) => {
                // 已派发后只保留有界 intent；原始大 params 不属于未知结果身份。
                work.request.params = None;
                work.request._payload.shrink(0);
                self.management_active = Some(ActiveManaged {
                    work,
                    stage: ManagedStage::Sent { id },
                    response: Value::Null,
                });
                true
            }
            Err(message) => {
                self.clear_intent_for_work(&work);
                self.send_error(
                    work.client,
                    work.external_id,
                    -32000,
                    &message,
                    work.explicit_session.clone(),
                );
                false
            }
        }
    }

    fn managed_session_error(
        &self,
        client_id: ClientId,
        method: &str,
        request: &Value,
        explicit_session: Option<&str>,
    ) -> Option<&'static str> {
        let foreign = |session: &str| self.attachments.owner(session) != Some(client_id);
        if explicit_session.is_some_and(foreign) {
            return Some("CDP attachment expired or belongs to another connection; attach again");
        }
        if method == "Target.detachFromTarget" {
            let session = request
                .get("params")
                .and_then(|params| params.get("sessionId"))
                .and_then(Value::as_str);
            if session.is_some_and(foreign) {
                return Some("This connection does not own that attachment");
            }
        }
        None
    }

    fn apply_auto_result(&mut self, client: ClientId, auto: bool, failed: bool) {
        if !failed {
            return;
        }
        if auto {
            if self.attachments.auto_owner == Some(client) {
                self.attachments.auto_owner = None;
            }
        } else if self.attachments.auto_owner.is_none() {
            self.attachments.auto_owner = Some(client);
        }
    }

    fn prepare_managed(&mut self, work: &ManagedWork, target: Option<&str>) -> Result<(), ()> {
        match work.method.as_str() {
            "Target.attachToTarget" => {
                let target = target.ok_or(())?;
                if self.attachments.intent(target).is_some() {
                    return Err(());
                }
                self.attachments
                    .set_intent(target, work.client)
                    .then_some(())
                    .ok_or(())
            }
            "Target.detachFromTarget" => {
                let session = work
                    .params
                    .get("sessionId")
                    .and_then(Value::as_str)
                    .ok_or(())?;
                if self.attachments.owner(session) != Some(work.client) {
                    return Err(());
                }
                Ok(())
            }
            "Target.setAutoAttach" => {
                // Node 在发送前记录 auto owner，失败不回滚；这里保持同一可见性。
                let auto = work
                    .params
                    .get("autoAttach")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                if auto {
                    self.attachments.auto_owner = Some(work.client);
                } else {
                    self.attachments.auto_owner = None;
                }
                Ok(())
            }
            "Target.attachToBrowserTarget" => Ok(()),
            _ => Ok(()),
        }
    }

    fn clear_intent_for_work(&mut self, work: &ManagedWork) {
        if let Some(target) = work.params.get("targetId").and_then(Value::as_str) {
            self.attachments.clear_intent(target, work.client);
        }
    }

    fn client_can_act(&self, client: ClientId, epoch: u64) -> bool {
        let Some(state) = self.clients.get(&client) else {
            return false;
        };
        if state.closed || state.retired || state.epoch != epoch {
            return false;
        }
        if state.role == Role::Viewer {
            return true;
        }
        self.controller.as_deref() == Some(state.owner.as_str())
            && state.epoch == self.control_epoch
            && self.active_controller == Some(client)
    }
}

fn attach_session(value: &Value) -> Option<String> {
    value
        .get("result")
        .and_then(|result| result.get("sessionId"))
        .and_then(Value::as_str)
        .map(str::to_string)
}

fn stage_id(stage: &ManagedStage) -> Option<u64> {
    match stage {
        ManagedStage::Sent { id } => Some(*id),
        ManagedStage::BackfillTargets { id } => Some(*id),
        ManagedStage::BackfillAttach { id, .. } => Some(*id),
    }
}

fn targets_from(value: &Value, budget: &PayloadBudget) -> Result<LeasedTargets, String> {
    if let Some(error) = value.get("error") {
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("Target discovery failed");
        return Err(message.to_string());
    }
    let infos = value
        .get("result")
        .and_then(|result| result.get("targetInfos"))
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();
    let lease = crate::transport::payload_size(&infos)
        .and_then(|bytes| budget.try_acquire(bytes, false))
        .ok_or("Target discovery budget exhausted")?;
    Ok(LeasedTargets {
        values: infos
            .iter()
            .filter(|target| target.get("type").and_then(Value::as_str) == Some("page"))
            .cloned()
            .collect(),
        lease,
    })
}
impl Broker {
    fn start_retirement(&mut self, client: ClientId) {
        if self
            .retirements
            .iter()
            .any(|retire| retire.client == client)
        {
            self.pump_retirements();
            return;
        }
        let was_auto = self.attachments.auto_owner == Some(client);
        if self.attachments.count_for(client) == 0 && !was_auto {
            if self.clients.get(&client).is_some_and(|state| state.closed) {
                self.clients.remove(&client);
            }
            return;
        }
        let id = self.next_retirement;
        self.next_retirement = self.next_retirement.saturating_add(1);
        self.retirements.push(Retirement {
            id,
            client,
            pending: None,
            stage: if was_auto {
                RetireStage::Auto
            } else {
                RetireStage::Detach
            },
            blocked: false,
        });
        self.pump_retirements();
    }

    fn pump_retirements(&mut self) {
        if matches!(self.lifecycle, Lifecycle::Lost | Lifecycle::Closed) {
            self.retirements.clear();
            return;
        }
        if self.management_active.is_some() {
            return;
        }
        loop {
            let index = self.retirements.iter().position(|retire| {
                retire.pending.is_none()
                    && !retire.blocked
                    && !self
                        .pending
                        .values()
                        .any(|pending| pending.client == Some(retire.client))
            });
            let Some(index) = index else {
                return;
            };
            let stage = self.retirements[index].stage;
            let client = self.retirements[index].client;
            let id = self.retirements[index].id;
            if stage == RetireStage::Auto && self.attachments.auto_owner != Some(client) {
                self.retirements[index].stage = RetireStage::Detach;
                continue;
            }
            if stage == RetireStage::Auto {
                if self.spawn_retire_auto(id, client).is_none() {
                    self.retirements[index].blocked = true;
                }
                return;
            }
            let session = self.attachments.first_session(client);
            let Some(session) = session else {
                self.retirements.remove(index);
                if self.clients.get(&client).is_some_and(|state| state.closed) {
                    self.clients.remove(&client);
                }
                continue;
            };
            if self.spawn_retire_detach(id, client, &session).is_none() {
                self.retirements[index].blocked = true;
            }
            return;
        }
    }

    fn spawn_retire_auto(&mut self, retirement: u64, client: ClientId) -> Option<u64> {
        let params = json!({ "autoAttach": false, "flatten": true });
        let spec = PendingSpec {
            client: None,
            role: None,
            external_id: None,
            explicit_session: None,
            epoch: self.control_epoch,
            method: "Target.setAutoAttach".to_string(),
            kind: PendingKind::RetirementAuto { id: retirement },
            deadline: self.internal_deadline,
        };
        match self.send_request("Target.setAutoAttach", Some(&params), None, spec) {
            Ok(id) => {
                if let Some(retire) = self
                    .retirements
                    .iter_mut()
                    .find(|retire| retire.id == retirement)
                {
                    retire.pending = Some(id);
                }
                let _ = client;
                Some(id)
            }
            Err(_) => None,
        }
    }

    fn spawn_retire_detach(
        &mut self,
        retirement: u64,
        client: ClientId,
        session: &str,
    ) -> Option<u64> {
        let params = json!({ "sessionId": session });
        let spec = PendingSpec {
            client: None,
            role: None,
            external_id: None,
            explicit_session: None,
            epoch: self.control_epoch,
            method: "Target.detachFromTarget".to_string(),
            kind: PendingKind::RetirementDetach {
                id: retirement,
                session: session.to_string(),
            },
            deadline: self.internal_deadline,
        };
        match self.send_request("Target.detachFromTarget", Some(&params), None, spec) {
            Ok(id) => {
                if let Some(retire) = self
                    .retirements
                    .iter_mut()
                    .find(|retire| retire.id == retirement)
                {
                    retire.pending = Some(id);
                }
                let _ = client;
                Some(id)
            }
            Err(_) => None,
        }
    }

    fn retirement_auto_response(&mut self, retirement: u64, value: &Value) {
        let Some(retire) = self
            .retirements
            .iter_mut()
            .find(|retire| retire.id == retirement)
        else {
            return;
        };
        retire.pending = None;
        if value.get("error").is_some() {
            retire.blocked = true;
            return;
        }
        if self.attachments.auto_owner == Some(retire.client) {
            self.attachments.auto_owner = None;
        }
        retire.stage = RetireStage::Detach;
    }

    fn retirement_detach_response(&mut self, retirement: u64, session: &str, value: &Value) {
        let Some(retire) = self
            .retirements
            .iter_mut()
            .find(|retire| retire.id == retirement)
        else {
            return;
        };
        retire.pending = None;
        if value.get("error").is_some() {
            retire.blocked = true;
            return;
        }
        self.attachments.forget(session);
    }

    fn pump_control(&mut self) {
        if self.control_active.is_some() {
            self.control_progress();
            return;
        }
        if self.lifecycle != Lifecycle::Running {
            self.reply_control_queue("Browser is closing");
            return;
        }
        if self.flush.is_some() || self.close.is_some() {
            self.reply_control_queue("Browser is busy");
            return;
        }
        let Some((owner, takeover, reply)) = self.control_queue.pop_front() else {
            return;
        };
        if !valid_owner(&owner) {
            let _ = reply.send(Err(ControlError::Other(
                "A short owner name is required".to_string(),
            )));
            self.pump_control();
            return;
        }
        if let Some(current) = &self.controller {
            if current != &owner && !takeover {
                let _ = reply.send(Err(ControlError::conflict(Some(current.clone()))));
                self.pump_control();
                return;
            }
        }
        let old_epoch = self.control_epoch;
        self.control_epoch = self.control_epoch.saturating_add(1);
        self.controller = Some(owner.clone());
        let previous = self.active_controller.take();
        if let Some(previous) = previous {
            if let Some(state) = self.clients.get_mut(&previous) {
                state.retired = true;
            }
            self.start_retirement(previous);
        }
        self.control_active = Some(ActiveControl {
            owner,
            old_epoch,
            reply: Some(reply),
            deadline: Instant::now() + CONTROL_DEADLINE,
            replied: false,
        });
        self.control_progress();
        self.pump_retirements();
    }

    fn control_progress(&mut self) {
        let Some(active) = self.control_active.as_ref() else {
            return;
        };
        let owner = active.owner.clone();
        let epoch = active.old_epoch + 1;
        let drained = self.management_active.is_none()
            && self.retirements.is_empty()
            && !self.controller_pending()
            && !self.unknown_blocks_control();
        if drained {
            let Some(mut active) = self.control_active.take() else {
                return;
            };
            if let Some(reply) = active.reply.take() {
                let _ = reply.send(Ok((owner, epoch)));
            }
            self.observer
                .with("control", json!({ "kind": "control", "epoch": epoch }));
            self.pump_control();
            self.pump_after_change();
            return;
        }
        if active.deadline <= Instant::now() && !active.replied {
            let Some(mut active) = self.control_active.take() else {
                return;
            };
            if let Some(reply) = active.reply.take() {
                let _ = reply.send(Err(ControlError::Other(
                    "Control cleanup could not be confirmed; retry after the old session settles"
                        .to_string(),
                )));
            }
            active.replied = true;
            self.control_active = Some(active);
            self.observer.kind("control-blocked");
        }
    }

    fn reply_control_queue(&mut self, message: &str) {
        while let Some((_, _, reply)) = self.control_queue.pop_front() {
            let _ = reply.send(Err(ControlError::Other(message.to_string())));
        }
    }

    fn controller_pending(&self) -> bool {
        self.pending
            .values()
            .any(|pending| pending.role == Some(Role::Controller))
    }

    fn unknown_blocks_control(&self) -> bool {
        self.outcome_unknown()
    }

    fn outcome_unknown(&self) -> bool {
        self.unknown
            .values()
            .any(|unknown| unknown.role != Some(Role::Viewer))
    }

    fn start_flush(&mut self, reply: oneshot::Sender<Result<Value, String>>) {
        if self.lifecycle == Lifecycle::Flushing {
            let _ = reply.send(Err("Browser is already flushing".to_string()));
            return;
        }
        if self.lifecycle != Lifecycle::Running {
            let _ = reply.send(Err("Browser is closing".to_string()));
            return;
        }
        if self.close.is_some() {
            let _ = reply.send(Err("Browser is closing".to_string()));
            return;
        }
        if self.control_active.is_some() {
            let _ = reply.send(Err("Control change is in progress".to_string()));
            return;
        }
        self.lifecycle = Lifecycle::Flushing;
        self.flush = Some(FlushState {
            reply: Some(reply),
            stage: FlushStage::Drain,
        });
        self.pump_flush();
    }

    fn pump_flush(&mut self) {
        let Some(flush) = self.flush.as_ref() else {
            return;
        };
        if self.lifecycle != Lifecycle::Flushing {
            return;
        }
        if !matches!(flush.stage, FlushStage::Drain) {
            return;
        }
        if self.outcome_unknown() {
            self.fail_flush("previous request outcome is unknown".to_string());
            return;
        }
        if self.management_active.is_some()
            || !self.retirements.is_empty()
            || self.has_write_pending()
        {
            return;
        }
        let spec = PendingSpec {
            client: None,
            role: None,
            external_id: None,
            explicit_session: None,
            epoch: self.control_epoch,
            method: "Kitesurf.flushProfile".to_string(),
            kind: PendingKind::Flush,
            deadline: self.internal_deadline,
        };
        match self.send_request("Kitesurf.flushProfile", None, None, spec) {
            Ok(_) => {
                if let Some(flush) = self.flush.as_mut() {
                    flush.stage = FlushStage::Sent;
                }
            }
            Err(message) => self.fail_flush(message),
        }
    }

    fn flush_response(&mut self, value: &Value) {
        if value.get("error").is_some() {
            // 宿主/数据库错误不把引擎文本或路径回传；会话仍可重试。
            self.fail_flush("Profile flush failed".to_string());
            return;
        }
        let token = self.next_token;
        self.next_token = self.next_token.saturating_add(1);
        if let Some(flush) = self.flush.as_mut() {
            flush.stage = FlushStage::Status(token);
        }
        let profile = self.profile.clone();
        let commands = self.commands_tx.clone();
        tokio::spawn(async move {
            let result = profile
                .status()
                .await
                .map_err(|_| "Profile status unavailable".to_string());
            let _ = commands
                .send(BrokerCommand::ProfileStatus { token, result })
                .await;
        });
    }

    fn profile_status(&mut self, token: u64, result: Result<Value, String>) {
        let Some(flush) = self.flush.as_ref() else {
            return;
        };
        if !matches!(flush.stage, FlushStage::Status(current) if current == token) {
            return;
        }
        let Some(mut flush) = self.flush.take() else {
            return;
        };
        self.lifecycle = Lifecycle::Running;
        if let Some(reply) = flush.reply.take() {
            let _ = reply.send(result);
        }
        self.pump_after_change();
    }

    fn fail_flush(&mut self, message: String) {
        let Some(mut flush) = self.flush.take() else {
            return;
        };
        if self.lifecycle == Lifecycle::Flushing {
            self.lifecycle = Lifecycle::Running;
        }
        self.observer.kind("flush-failed");
        if let Some(reply) = flush.reply.take() {
            let _ = reply.send(Err(message));
        }
        self.pump_after_change();
    }

    fn start_close(
        &mut self,
        kind: CloseKind,
        client: Option<ClientId>,
        external_id: Option<i64>,
        explicit_session: Option<String>,
        reply: Option<oneshot::Sender<Result<(), String>>>,
    ) -> Result<(), String> {
        if matches!(self.lifecycle, Lifecycle::Lost | Lifecycle::Closed) {
            return Err("Browser session is lost".to_string());
        }
        if self.lifecycle == Lifecycle::Flushing {
            return Err("Browser is flushing".to_string());
        }
        if self.close.is_some() {
            return Err("Browser is closing".to_string());
        }
        if kind == CloseKind::Cdp {
            let id = client.ok_or("Browser session is lost")?;
            let state = self.clients.get(&id).ok_or("Browser session is lost")?;
            if state.role != Role::Controller {
                return Err("Viewer connection cannot control the browser".to_string());
            }
            if state.retired
                || self.active_controller != Some(id)
                || self.controller.as_deref() != Some(state.owner.as_str())
                || state.epoch != self.control_epoch
            {
                return Err("Control has changed; reconnect and observe before acting".to_string());
            }
            self.validate_session(id, explicit_session.as_deref())?;
        }
        if self.control_active.is_some() {
            return Err("Control change is in progress".to_string());
        }
        self.close = Some(CloseState {
            _request: None,
            kind,
            client,
            external_id,
            explicit_session,
            reply,
            stage: CloseStage::Drain,
            finalize_until: None,
            started: Instant::now(),
            notified: false,
            succeeded: false,
        });
        self.lifecycle = Lifecycle::Closing;
        self.pump_close();
        Ok(())
    }

    fn pump_close(&mut self) {
        let Some(state) = self.close.as_ref() else {
            return;
        };
        if state.succeeded {
            self.pump_finalize();
            return;
        }
        if self.lifecycle != Lifecycle::Closing {
            return;
        }
        if self.outcome_unknown() {
            let overdue = self.close.as_ref().is_some_and(|state| {
                !state.notified && state.started + self.client_deadline <= Instant::now()
            });
            if overdue {
                if let Some(state) = self.close.as_mut() {
                    state.notified = true;
                }
                self.close_restricted("previous request outcome is unknown".to_string());
            }
            return;
        }
        if !self.unknown.is_empty() {
            return;
        }
        let stage = match state.stage {
            CloseStage::Drain => 0,
            CloseStage::Flush => 1,
            CloseStage::Browser => 2,
        };
        if stage == 0 {
            if self.management_active.is_some()
                || !self.retirements.is_empty()
                || self.has_write_pending()
            {
                return;
            }
            let spec = PendingSpec {
                client: None,
                role: None,
                external_id: None,
                explicit_session: None,
                epoch: self.control_epoch,
                method: "Kitesurf.flushProfile".to_string(),
                kind: PendingKind::Close,
                deadline: self.internal_deadline,
            };
            match self.send_request("Kitesurf.flushProfile", None, None, spec) {
                Ok(_) => {
                    if let Some(state) = self.close.as_mut() {
                        state.stage = CloseStage::Flush;
                    }
                }
                Err(_) => self.fail_close("Profile flush failed".to_string()),
            }
        }
    }

    fn close_response(&mut self, value: &Value) {
        let Some(state) = self.close.as_ref() else {
            return;
        };
        let stage = match state.stage {
            CloseStage::Flush => 1,
            CloseStage::Browser => 2,
            CloseStage::Drain => 0,
        };
        if stage == 2 {
            if value.get("error").is_some() {
                self.fail_close("Browser close failed".to_string());
            } else {
                self.finish_close_success();
            }
            return;
        }
        if stage != 1 {
            return;
        }
        if value.get("error").is_some() {
            // 与 /flush 一致：宿主/数据库失败只返回固定类别。
            self.fail_close("Profile flush failed".to_string());
            return;
        }
        let spec = PendingSpec {
            client: None,
            role: None,
            external_id: None,
            explicit_session: None,
            epoch: self.control_epoch,
            method: "Browser.close".to_string(),
            kind: PendingKind::Close,
            deadline: self.internal_deadline,
        };
        match self.send_request("Browser.close", None, None, spec) {
            Ok(_) => {
                if let Some(state) = self.close.as_mut() {
                    state.stage = CloseStage::Browser;
                }
            }
            Err(_) => self.fail_close("Browser close failed".to_string()),
        }
    }

    fn fail_close(&mut self, message: String) {
        let message = match self.close.as_ref().map(|state| &state.stage) {
            Some(CloseStage::Flush | CloseStage::Drain) => "Profile flush failed".to_string(),
            _ if message.to_ascii_lowercase().contains("unknown") => {
                "Browser close outcome unknown".to_string()
            }
            _ => "Browser close failed".to_string(),
        };
        let Some(mut state) = self.close.take() else {
            return;
        };
        if self.lifecycle == Lifecycle::Closing {
            self.lifecycle = Lifecycle::Running;
        }
        self.observer
            .with("close-failed", json!({ "category": message }));
        if let Some(reply) = state.reply.take() {
            let _ = reply.send(Err(message.clone()));
        }
        if let (CloseKind::Cdp, Some(client), Some(external)) =
            (state.kind, state.client, state.external_id)
        {
            self.send_error(client, external, -32000, &message, state.explicit_session);
        }
        self.pump_after_change();
    }

    fn close_restricted(&mut self, _message: String) {
        let message = "Browser close outcome unknown".to_string();
        self.observer
            .with("close-restricted", json!({ "category": message }));
        let Some(state) = self.close.as_mut() else {
            return;
        };
        if let Some(reply) = state.reply.take() {
            let _ = reply.send(Err(message.clone()));
        }
        let kind = state.kind;
        let client = state.client.take();
        let external = state.external_id.take();
        let session = state.explicit_session.take();
        if let (CloseKind::Cdp, Some(client), Some(external)) = (kind, client, external) {
            self.send_error(client, external, -32000, &message, session);
        }
    }

    fn finish_close_success(&mut self) {
        let Some(state) = self.close.as_mut() else {
            return;
        };
        if state.succeeded {
            return;
        }
        state.succeeded = true;
        state.finalize_until = Some(Instant::now() + FINALIZE_TIMEOUT);
        self.lifecycle = Lifecycle::Closed;
        if let Some(reply) = state.reply.take() {
            let _ = reply.send(Ok(()));
        }
        let kind = state.kind;
        let cdp_reply = state.explicit_session.clone();
        let cdp_target = match kind {
            CloseKind::Cdp => state.client.zip(state.external_id),
            CloseKind::Http | CloseKind::Host => None,
        };
        if let Some((client, external)) = cdp_target {
            self.send_reply(client, external, json!({}), cdp_reply);
        }
        let ids = self.clients.keys().copied().collect::<Vec<_>>();
        for client in ids {
            if let Some(state) = self.clients.get_mut(&client) {
                state.closed = true;
                if state
                    .writer
                    .try_send(ClientCommand::Close {
                        code: 1000,
                        reason: "Browser closed".to_string(),
                    })
                    .is_err()
                {
                    let _ = state.cancel.send(true);
                }
            }
        }
        self.observer.kind("close-succeeded");
    }

    fn pump_finalize(&mut self) {
        if self.finished {
            return;
        }
        let Some(state) = self.close.as_ref() else {
            return;
        };
        if !state.succeeded {
            return;
        }
        let deadline = state.finalize_until.unwrap_or_else(Instant::now);
        let writers_done = self.clients.values().all(|client| client.writer_gone);
        if !writers_done && Instant::now() < deadline {
            return;
        }
        self.clients.clear();
        let _ = self.closed_tx.send(true);
        self.finished = true;
    }

    fn handle_lost(&mut self, reason: String) {
        if self.finished {
            return;
        }
        if self.close.as_ref().is_some_and(|state| state.succeeded) {
            return;
        }
        self.lifecycle = Lifecycle::Lost;
        let failures = self
            .pending
            .values()
            .filter_map(|pending| {
                pending.client.map(|client| {
                    (
                        client,
                        pending.external_id.unwrap_or_default(),
                        pending.explicit_session.clone(),
                    )
                })
            })
            .collect::<Vec<_>>();
        self.pending.clear();
        self.unknown.clear();
        self.client_pending = 0;
        self.internal_pending = 0;
        for (client, external, session) in failures {
            self.send_error(
                client,
                external,
                -32000,
                "Browser session ended; outcome may be unknown",
                session,
            );
        }
        if let Some(mut flush) = self.flush.take() {
            if let Some(reply) = flush.reply.take() {
                let _ = reply.send(Err(
                    "Browser session ended; outcome may be unknown".to_string()
                ));
            }
        }
        if let Some(mut close) = self.close.take() {
            if let Some(reply) = close.reply.take() {
                let _ = reply.send(Err(
                    "Browser session ended; outcome may be unknown".to_string()
                ));
            }
            if let (CloseKind::Cdp, Some(client), Some(external)) =
                (close.kind, close.client, close.external_id)
            {
                self.send_error(
                    client,
                    external,
                    -32000,
                    "Browser session ended; outcome may be unknown",
                    close.explicit_session,
                );
            }
        }
        self.close_all_clients(1008, "Browser session lost");
        self.observer.with(
            "engine-lost",
            json!({ "kind": "engine-lost", "reason": reason.clone() }),
        );
        let _ = self.lost_tx.send(Some(reason));
        self.finished = true;
    }

    fn close_all_clients(&mut self, code: u16, reason: &str) {
        for state in self.clients.values_mut() {
            state.closed = true;
            if state
                .writer
                .try_send(ClientCommand::Close {
                    code,
                    reason: reason.to_string(),
                })
                .is_err()
            {
                let _ = state.cancel.send(true);
            }
        }
    }

    fn has_write_pending(&self) -> bool {
        self.pending.values().any(|pending| {
            pending.client.is_some()
                || matches!(
                    pending.kind,
                    PendingKind::Managed
                        | PendingKind::PageAttach { .. }
                        | PendingKind::RetirementAuto { .. }
                        | PendingKind::RetirementDetach { .. }
                )
        })
    }

    fn pump_after_change(&mut self) {
        self.pump_control();
        self.pump_retirements();
        self.pump_management();
        self.pump_flush();
        self.pump_close();
        self.pump_finalize();
    }

    fn tick(&mut self) {
        if self.finished {
            return;
        }
        let now = Instant::now();
        let expired = self
            .pending
            .iter()
            .filter(|(_, pending)| pending.deadline <= now)
            .map(|(id, _)| *id)
            .collect::<Vec<_>>();
        for id in expired {
            self.expire_pending(id);
        }
        self.pump_control();
        self.pump_retirements();
        self.pump_management();
        self.pump_flush();
        self.pump_close();
        self.pump_finalize();
    }
}

#[cfg(all(test, feature = "test-faults"))]
#[path = "../tests/broker_test.rs"]
mod tests;
