// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// 一个 broker actor 独占控制权、控制 epoch、请求 ID 映射、pending 与 attachment 归属。
// 网络与 SQLite 等待都以阶段推进的方式回到 actor，任何跨 await 的共享 Mutex 都不参与
// 权限判断。客户端只通过有界帧通道和 oneshot 与管理操作通信；响应只回原客户端。

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use crate::cdp_raw::{
    self, DuplicatePolicy, LeasedIdentity, LeasedRaw, ParsePolicy, RawEnvelope, RawPresence,
    RawSpan,
};
use serde_json::value::RawValue;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{Instant, MissedTickBehavior};

use crate::attachments::{AttachmentReservation, Attachments, QuarantineResult, Registration};
use crate::observe::Observer;
use crate::services::{LeasedProfileStatus, ProfileService};
use crate::transport::{LeasedMessage, PayloadBudget, PayloadLease, Upstream, UpstreamEvent};

impl Upstream {
    /// 启动探测只复制必要 result，receive lease 在返回普通 owner 前释放。
    pub async fn request(
        &mut self,
        method: &str,
        params: Option<Value>,
        deadline: Duration,
    ) -> crate::HostResult<LeasedRaw> {
        let id = self
            .handle
            .allocate_id()
            .ok_or("CDP request ids are exhausted")?;
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
                    _ = self.lost.changed() => return Err("engine connection lost while waiting for a reply".into()),
                };
                match event {
                    Some(UpstreamEvent::Raw(raw)) => {
                        let mut scratch = self.handle.budget.json_scratch();
                        let Ok(value) =
                            RawEnvelope::parse(raw, ParsePolicy::UpstreamStrict, &mut scratch)
                        else {
                            continue;
                        };
                        if value.id(&mut scratch) != Some(id) {
                            continue;
                        }
                        if value.has_error() {
                            return Err("CDP command failed".into());
                        }
                        return Ok(match value.node("result") {
                            Some(node) => node.copy(&self.handle.budget, &mut scratch)?,
                            None => LeasedRaw::literal("null", &self.handle.budget)?,
                        });
                    }
                    Some(UpstreamEvent::Lost(reason)) => return Err(reason.into()),
                    None => return Err("engine connection closed while waiting for a reply".into()),
                    _ => {}
                }
            }
        };
        tokio::time::timeout(deadline, wait)
            .await
            .unwrap_or_else(
                |_| Err(format!("CDP timeout: {method}; outcome may be unknown").into()),
            )
    }
}

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
const ATTACH_LIMIT_MESSAGE: &str = "Attachment limit reached";
const MAX_UNKNOWN: usize = MAX_PENDING_GLOBAL + MAX_INTERNAL_PENDING;
const MAX_MANAGEMENT_QUEUE: usize = 64;
const MAX_CONTROL_QUEUE: usize = 8;
const MAX_PAGE_QUEUE: usize = 64;
const MAX_PAGE_QUEUE_BYTES: usize = 8 * 1024 * 1024;
const CLIENT_QUEUE_FRAMES: usize = 64;
const CLIENT_QUEUE_BYTES: usize = 32 * 1024 * 1024;
#[cfg(all(test, feature = "test-faults"))]
const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
const CLIENT_DEADLINE: Duration = Duration::from_secs(120);
const INTERNAL_DEADLINE: Duration = Duration::from_secs(30);
const CONTROL_DEADLINE: Duration = Duration::from_secs(10);
const DRAIN_TIMEOUT_MESSAGE: &str = "Browser drain timed out";
const QUARANTINE_UNCONFIRMED_MESSAGE: &str = "CDP cleanup could not be confirmed";
const FINALIZE_TIMEOUT: Duration = Duration::from_secs(2);
const TICK: Duration = Duration::from_millis(50);

const FRAME_CAPACITY: usize = 8;
const COMMAND_CAPACITY: usize = 256;

const PAGE_SESSION_CONTROL_MESSAGE: &str = "Another controller owns this page session";
const PAGE_SESSION_DENIED_MESSAGE: &str =
    "This method is not available on a page-session connection";
const FOREIGN_TARGET_MESSAGE: &str = "Target belongs to another page session";
const FOREIGN_SESSION_MESSAGE: &str = "Session is not part of this page session";
const PAGE_SESSION_UNOWNED_MESSAGE: &str = "Target is not part of this page session";
/// page 端点只能承载顶层 page；frame/popup 必须先由父会话 attach。
const PAGE_ENDPOINT_TARGET_MESSAGE: &str =
    "Only a top-level page target can back a page connection";
/// 实例级或无法限定到会话的方法；page session 只操作自己的 target 与 CDP session。
/// Browser.getVersion 是只读发现字段，DevTools 页面连接仍可使用。Page 的预导航脚本表挂在
/// 共享 BrowserState 上，page 连接写入会影响其他页面的下次导航。
const PAGE_SESSION_DENIED_METHODS: &[&str] = &[
    "Browser.close",
    "Browser.crash",
    "Browser.setDownloadBehavior",
    "Browser.grantPermissions",
    "Browser.resetPermissions",
    "Kitesurf.flushProfile",
    "Network.clearBrowserCache",
    "Network.clearBrowserCookies",
    "Page.addScriptToEvaluateOnNewDocument",
    "Page.removeScriptToEvaluateOnNewDocument",
    "Security.setIgnoreCertificateErrors",
    "Storage.clearCookies",
    "Storage.clearDataForOrigin",
    "Storage.clearTrustTokens",
    "Target.attachToBrowserTarget",
    "Target.createBrowserContext",
    "Target.createTarget",
    "Target.disposeBrowserContext",
    "Target.getBrowserContexts",
    "Target.setAutoAttach",
    "Target.setDiscoverTargets",
    "Target.setRemoteLocations",
];

/// 引擎的实例级 target ID；page session 不能 attach，也不能按它读取实例状态。
const INSTANCE_TARGET_IDS: &[&str] = &["browser", "kitesurf-page-renderer", "kitesurf-engine"];

/// Kitesurf 方法只有显式携带本会话 targetId 时才可按页转发；其余 Kitesurf.* 属实例级。
/// 引擎在不带 targetId 时会读取或改写第一张页面/共享渲染器状态，不能透传。
const PAGE_SCOPED_KITESURF_METHODS: &[&str] = &[
    "Kitesurf.setLayoutMode",
    "Kitesurf.getLayoutMode",
    "Kitesurf.startSceneStream",
    "Kitesurf.stopSceneStream",
    "Kitesurf.sceneFrameAck",
    "Kitesurf.getSceneResources",
    "Kitesurf.getNavigatePhases",
    "Kitesurf.hasPendingCriticalResources",
    "Kitesurf.getPageState",
    "Kitesurf.setControlValue",
    "Kitesurf.markPaintDamage",
    "Kitesurf.getPaintDamageSince",
    "Kitesurf.getMemoryStats",
    "Kitesurf.recordScene",
    "Kitesurf.getScreencastStatus",
    "Kitesurf.getRbiResources",
    "Kitesurf.getFontFaces",
    "Kitesurf.getMemprofDump",
    "Kitesurf.resetMemprof",
    "Kitesurf.boaForceCollect",
    "Kitesurf.getSceneStats",
];

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
    pub raw: LeasedRaw,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum EventMethod {
    Attached,
    Detached,
    Discovery,
    Other,
}

struct Received {
    raw: RawEnvelope,
    method: Option<EventMethod>,
    outer: Option<RawSpan>,
    session: Option<RawSpan>,
    target: Option<RawSpan>,
    /// 引擎目标信息里的 openerId/openerFrameId；用于绑定 frame/popup 归属。
    opener: Option<RawSpan>,
    /// targetInfo.type 是否为 page；None 表示事件没有 targetInfo。
    page_target: Option<bool>,
    /// 无 targetInfo 的 Target.targetDestroyed：清除该 target 的父链。
    destroyed: bool,
    attached: Option<LeasedIdentity>,
}

impl Received {
    fn parse(raw: RawEnvelope, budget: &PayloadBudget) -> Result<Self, cdp_raw::RawError> {
        let mut scratch = budget.json_scratch();
        let response = raw.id(&mut scratch).is_some();
        let string = |node: Option<cdp_raw::RawNode<'_>>,
                      scratch: &mut crate::transport::JsonScratchGuard<'_>| {
            let mut buffer = [0; 4096];
            node.map(|node| {
                node.identity_text(&mut buffer, scratch)
                    .map(|text| text.map(|_| node.span()))
            })
            .transpose()
            .map(Option::flatten)
        };
        let method = if response {
            None
        } else {
            string(raw.node("method"), &mut scratch)?
                .map(|span| Self::event_method(raw.at(span), &mut scratch))
        };
        let outer = if response {
            None
        } else {
            string(raw.node("sessionId"), &mut scratch)?
        };
        let params = raw.node("params");
        let session = if matches!(method, Some(EventMethod::Attached | EventMethod::Detached)) {
            string(
                params.and_then(|node| node.field("sessionId", &mut scratch)),
                &mut scratch,
            )?
        } else {
            None
        };
        // attach 与发现事件都携带 targetInfo；targetDestroyed 只有 params.targetId。
        let target_info = matches!(method, Some(EventMethod::Attached | EventMethod::Discovery))
            .then(|| params.and_then(|node| node.field("targetInfo", &mut scratch)))
            .flatten();
        let target = match method {
            Some(EventMethod::Attached | EventMethod::Discovery) => match target_info {
                Some(info) => string(info.field("targetId", &mut scratch), &mut scratch)?,
                None => string(
                    params.and_then(|node| node.field("targetId", &mut scratch)),
                    &mut scratch,
                )?,
            },
            _ => None,
        };
        let opener = match (method, target_info) {
            (Some(EventMethod::Attached | EventMethod::Discovery), Some(info)) => {
                let opener = info
                    .field("openerId", &mut scratch)
                    .or_else(|| info.field("openerFrameId", &mut scratch));
                string(opener, &mut scratch)?
            }
            _ => None,
        };
        let page_target = target_info
            .and_then(|info| info.field("type", &mut scratch))
            .map(|node| node.equals("page", &mut scratch));
        let destroyed = method == Some(EventMethod::Discovery) && target_info.is_none();
        let attached = None;
        Ok(Self {
            raw,
            method,
            outer,
            session,
            target,
            opener,
            page_target,
            destroyed,
            attached,
        })
    }

    fn event_method(
        node: cdp_raw::RawNode<'_>,
        scratch: &mut crate::transport::JsonScratchGuard<'_>,
    ) -> EventMethod {
        if node.equals("Target.attachedToTarget", scratch) {
            EventMethod::Attached
        } else if node.equals("Target.detachedFromTarget", scratch) {
            EventMethod::Detached
        } else if [
            "Target.targetCreated",
            "Target.targetInfoChanged",
            "Target.targetDestroyed",
        ]
        .iter()
        .any(|method| node.equals(method, scratch))
        {
            EventMethod::Discovery
        } else {
            EventMethod::Other
        }
    }

    fn has_error(&self) -> bool {
        self.raw.has_error()
    }

    fn serialize_reply(
        &self,
        budget: &PayloadBudget,
        external_id: i64,
        session: Option<&str>,
    ) -> Option<(String, PayloadLease)> {
        #[derive(serde::Serialize)]
        struct Reply<'a> {
            id: i64,
            #[serde(skip_serializing_if = "Option::is_none")]
            result: Option<&'a RawValue>,
            #[serde(skip_serializing_if = "Option::is_none")]
            error: Option<&'a RawValue>,
            #[serde(rename = "sessionId", skip_serializing_if = "Option::is_none")]
            session: Option<&'a str>,
        }
        let mut scratch = budget.json_scratch();
        let reply = Reply {
            id: external_id,
            result: self
                .raw
                .node("result")
                .and_then(|node| node.raw(&mut scratch).ok()),
            error: self
                .raw
                .node("error")
                .and_then(|node| node.raw(&mut scratch).ok()),
            session,
        };
        crate::transport::serialize_payload(budget, &reply, false)
    }
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
        operation: FlushOperation,
        reply: FlushReply,
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
    TargetsForPage(String, oneshot::Sender<Result<LeasedTargets, String>>),
    #[cfg(all(test, feature = "test-faults"))]
    BudgetSnapshot(oneshot::Sender<(usize, usize, usize, usize)>),
    #[cfg(all(test, feature = "test-faults"))]
    ReceiveAck(LeasedRaw, oneshot::Sender<(bool, usize)>),
    #[cfg(all(test, feature = "test-faults"))]
    PressurePause(tests::pressure::Pause),
    ProfileBarrierComplete {
        token: u64,
        result: Result<FlushOutcome, String>,
    },
}

#[derive(Clone)]
enum FlushOperation {
    Status,
    Backup(LeasedIdentity),
}

enum FlushOutcome {
    Status(LeasedProfileStatus),
    Backup(PathBuf),
}

type FlushReply = oneshot::Sender<Result<FlushOutcome, String>>;
type CloseReply = oneshot::Sender<Result<(), String>>;

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
    #[cfg(all(test, feature = "test-faults"))]
    test_task: Arc<std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>>,
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
            #[cfg(all(test, feature = "test-faults"))]
            test_task: Arc::new(std::sync::Mutex::new(None)),
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
            page_sessions: HashMap::new(),
            next_page_epoch: 1,
            control_queue: VecDeque::new(),
            next_admission_seq: 1,
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
        let task = tokio::spawn(broker.run());
        #[cfg(all(test, feature = "test-faults"))]
        {
            *handle.test_task.lock().unwrap() = Some(task);
        }
        #[cfg(not(all(test, feature = "test-faults")))]
        drop(task);
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
        let Some(lease) = self.budget.try_acquire(text.capacity(), false) else {
            return false;
        };
        self.frames.try_send((client, text, lease)).is_ok()
    }

    /// 读端使用有界队列施加背压；broker 结束时返回 false。
    pub async fn frame(&self, client: ClientId, text: String) -> bool {
        let Some(lease) = self.budget.try_acquire(text.capacity(), false) else {
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

    pub async fn flush(&self) -> Result<LeasedProfileStatus, String> {
        match self.flush_operation(FlushOperation::Status).await? {
            FlushOutcome::Status(status) => Ok(status),
            FlushOutcome::Backup(_) => Err("Profile status unavailable".into()),
        }
    }

    pub async fn backup(&self, destination: LeasedIdentity) -> Result<PathBuf, String> {
        match self
            .flush_operation(FlushOperation::Backup(destination))
            .await?
        {
            FlushOutcome::Backup(path) => Ok(path),
            FlushOutcome::Status(_) => Err("Profile backup failed".into()),
        }
    }

    async fn flush_operation(&self, operation: FlushOperation) -> Result<FlushOutcome, String> {
        self.command(|reply| BrokerCommand::Flush { operation, reply })
            .await?
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

    /// 会话范围内的发现：主 target、已 attach 的 target 及其引擎报告的子 frame/popup。
    pub async fn targets_for_page(&self, page: String) -> Result<LeasedTargets, String> {
        self.command(|reply| BrokerCommand::TargetsForPage(page, reply))
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
    seq: u64,
    _metadata: PayloadLease,
}

impl Drop for RequestLease {
    fn drop(&mut self) {
        self.ids.lock().unwrap().remove(&self.id);
    }
}

struct Request {
    raw: Option<RawEnvelope>,
    id: Option<i64>,
    method: Option<LeasedIdentity>,
    session: Option<LeasedIdentity>,
    params: Option<RawSpan>,
    observed: ObservedParams,
    metadata: Option<PayloadLease>,
    lease: Option<Arc<RequestLease>>,
}

#[derive(Clone, Default)]
struct ObservedParams {
    target: Option<LeasedIdentity>,
    session: Option<LeasedIdentity>,
    auto_attach: Option<bool>,
    discover: Option<bool>,
    fallback: bool,
}

impl ObservedParams {
    fn string(&self, name: &str) -> Option<&str> {
        if self.fallback {
            return None;
        }
        match name {
            "targetId" => self.target.as_ref().map(LeasedIdentity::as_str),
            "sessionId" => self.session.as_ref().map(LeasedIdentity::as_str),
            _ => None,
        }
    }
}

impl Request {
    #[cfg(all(test, feature = "test-faults"))]
    fn parse(text: String, payload: PayloadLease, budget: &PayloadBudget) -> Option<Self> {
        let metadata = budget.try_acquire(64 * 1024, false)?;
        Self::parse_with_metadata(text, payload, metadata, budget)
    }

    fn parse_with_metadata(
        text: String,
        payload: PayloadLease,
        metadata: PayloadLease,
        budget: &PayloadBudget,
    ) -> Option<Self> {
        // 64 KiB 随 request/unknown lease 保留：固定记录、Arc 头、owner token、
        // 实际 16-byte span 及最多 12 份 <=4096-byte identity 临时/持久副本。
        // 动态 identity 字符容量还由 metadata_scalar 单独收费；测试核对固定容量余量。
        let mut scratch = budget.json_scratch();
        let raw = RawEnvelope::parse(
            LeasedRaw::new(text, payload).ok()?,
            ParsePolicy::ClientOpaque,
            &mut scratch,
        )
        .ok()?;
        let backing = raw.backing();
        let id = match Self::scalar(backing, raw.field("id"), budget, &mut scratch)? {
            cdp_raw::MetadataScalar::Number(number) => {
                if let Some(value) = number.as_i64() {
                    (value.unsigned_abs() <= 9_007_199_254_740_991).then_some(value)
                } else {
                    number
                        .as_f64()
                        .filter(|value| {
                            value.fract() == 0.0 && value.abs() <= 9_007_199_254_740_991f64
                        })
                        .map(|value| value as i64)
                }
            }
            _ => None,
        };
        let method = Self::identity(backing, raw.field("method"), budget, &mut scratch)?;
        let session = Self::identity(backing, raw.field("sessionId"), budget, &mut scratch)?;
        let params = match raw.field("params") {
            RawPresence::Present(span) => Some(span),
            _ => None,
        };
        let observed = Self::observe(backing, params, budget, &mut scratch)?;
        Some(Self {
            raw: Some(raw),
            id,
            method,
            session,
            params,
            observed,
            metadata: Some(metadata),
            lease: None,
        })
    }

    fn identity(
        backing: &LeasedRaw,
        field: RawPresence,
        budget: &PayloadBudget,
        scratch: &mut crate::transport::JsonScratchGuard<'_>,
    ) -> Option<Option<LeasedIdentity>> {
        Some(match Self::scalar(backing, field, budget, scratch)? {
            cdp_raw::MetadataScalar::Text(text) => Some(text),
            _ => None,
        })
    }

    fn scalar(
        backing: &LeasedRaw,
        field: RawPresence,
        budget: &PayloadBudget,
        scratch: &mut crate::transport::JsonScratchGuard<'_>,
    ) -> Option<cdp_raw::MetadataScalar> {
        let Some(span) = field.span() else {
            return Some(cdp_raw::MetadataScalar::Other);
        };
        if backing.text(span).ok()?.len() > 4096 {
            return None;
        }
        let raw = backing.raw(span, scratch).ok()?;
        cdp_raw::metadata_scalar(raw, budget, scratch).ok()
    }

    fn observe(
        backing: &LeasedRaw,
        params: Option<RawSpan>,
        budget: &PayloadBudget,
        scratch: &mut crate::transport::JsonScratchGuard<'_>,
    ) -> Option<ObservedParams> {
        let Some(span) = params else {
            return Some(ObservedParams::default());
        };
        let fields = if backing.text(span).ok()?.starts_with('[') {
            cdp_raw::select_sequence::<4>(backing, span, scratch)
        } else {
            cdp_raw::select_fields(
                backing,
                span,
                ["targetId", "sessionId", "autoAttach", "discover"],
                DuplicatePolicy::Reject,
                scratch,
            )
        };
        let Ok(fields) = fields else {
            return Some(ObservedParams {
                fallback: true,
                ..Default::default()
            });
        };
        // 扫描回调只保存 span；退出 serde 后才逐字段解码，避免重入共享 scratch。
        // metadata_scalar 在 owned allocation 前执行旧 4096 raw-byte 限制。
        let mut boolean = |field| {
            Some(match Self::scalar(backing, field, budget, scratch)? {
                cdp_raw::MetadataScalar::Bool(value) => Some(value),
                _ => None,
            })
        };
        let auto_attach = boolean(fields[2])?;
        let discover = boolean(fields[3])?;
        Some(ObservedParams {
            target: Self::identity(backing, fields[0], budget, scratch)?,
            session: Self::identity(backing, fields[1], budget, scratch)?,
            auto_attach,
            discover,
            fallback: false,
        })
    }

    fn method(&self) -> Option<&str> {
        self.method.as_ref().map(LeasedIdentity::as_str)
    }
    fn session(&self) -> Option<&str> {
        self.session.as_ref().map(LeasedIdentity::as_str)
    }
    fn bytes(&self) -> usize {
        self.raw
            .as_ref()
            .map_or(0, |raw| raw.backing().as_str().len())
    }
    fn raw_params<'a>(
        &'a self,
        scratch: &mut crate::transport::JsonScratchGuard<'_>,
    ) -> Option<&'a RawValue> {
        self.raw.as_ref()?.backing().raw(self.params?, scratch).ok()
    }
    fn release_raw(&mut self) {
        self.params = None;
        self.raw = None;
    }
}

struct Client {
    slots: Arc<Semaphore>,
    ids: Arc<Mutex<std::collections::HashSet<i64>>>,
    owner: String,
    role: Role,
    epoch: u64,
    endpoint: Endpoint,
    /// page session ID（Endpoint::Page 的主 targetId）；Browser 连接为 None。
    page: Option<String>,
    primary: Option<LeasedIdentity>,
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
    seq: Option<u64>,
    method: String,
    kind: PendingKind,
    deadline: Instant,
}

enum PendingKind {
    Client,
    PageAttach {
        target: String,
    },
    Managed,
    Flush,
    Close,
    Targets(oneshot::Sender<Result<LeasedTargets, String>>),
    /// page session 的 Target.getTargets：在 broker 内过滤上游全量结果。
    TargetsForClient {
        page: String,
    },
    /// 会话范围内的发现请求（HTTP 入口）；过滤后返回持有的 targetInfos。
    ScopedTargets {
        page: String,
        reply: oneshot::Sender<Result<LeasedTargets, String>>,
    },
    /// 拒绝跨 session attach 后收回上游 session；只等结算，不登记 attachment。
    ScopeDetach,
    RetirementAuto {
        id: u64,
    },
    RetirementDetach {
        id: u64,
        session: String,
    },
    QuarantineDetach {
        session: String,
        client: ClientId,
        epoch: u64,
    },
}

struct Unknown {
    _internal: Option<Arc<InternalRequestLease>>,
    _request: Option<Arc<RequestLease>>,
    client: Option<ClientId>,
    epoch: u64,
    seq: Option<u64>,
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
        id: u64,
        session: String,
    },
    QuarantineDetach {
        session: String,
        client: ClientId,
        epoch: u64,
    },
}

struct ManagedWork {
    request: Request,
    client: ClientId,
    epoch: u64,
    external_id: i64,
    explicit_session: Option<String>,
    method: String,
    params: ObservedParams,
}

struct ActiveManaged {
    stage: ManagedStage,
    response: Option<LeasedRaw>,
    work: ManagedWork,
}

struct BackfillSelection {
    raw: LeasedRaw,
    spans: Vec<RawSpan>,
    _lease: PayloadLease,
}

enum ManagedStage {
    Sent {
        id: u64,
    },
    BackfillTargets {
        id: u64,
    },
    BackfillAttach {
        id: u64,
        targets: BackfillSelection,
        index: usize,
        target: Option<LeasedIdentity>,
    },
}

/// 已发送的退休命令由 retirement ID、上游 ID 与 operation 共同标识。
#[derive(Clone, Copy, PartialEq, Eq)]
struct RetireOp {
    upstream: u64,
    operation: RetireOperation,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RetireOperation {
    AutoDisable,
    Detach,
}

struct Retirement {
    id: u64,
    client: ClientId,
    pending: Option<RetireOp>,
    stage: RetireStage,
    awaiting: Option<RetireOp>,
    failed: bool,
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
    operation: FlushOperation,
    reply: FlushReply,
    host_close: Option<CloseReply>,
    stage: FlushStage,
    cutoff: u64,
    drain_until: Instant,
}

enum FlushStage {
    Drain,
    Sent,
    Completing(u64),
}

struct CloseState {
    _request: Option<Arc<RequestLease>>,
    kind: CloseKind,
    client: Option<ClientId>,
    external_id: Option<i64>,
    explicit_session: Option<String>,
    reply: Option<oneshot::Sender<Result<(), String>>>,
    stage: CloseStage,
    cutoff: u64,
    drain_until: Instant,
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

/// 已接受但尚未开始的 control；seq 仅保留准入次序，不进普通 drain。
struct ControlEntry {
    _seq: u64,
    owner: String,
    takeover: bool,
    reply: ControlWaiter,
}

/// 一个 page session 的控制者与权限代次；target 集合由 attachments 的 claims 持有。
struct PageSession {
    controller: ClientId,
    epoch: u64,
}

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
    page_sessions: HashMap<String, PageSession>,
    next_page_epoch: u64,
    control_queue: VecDeque<ControlEntry>,
    next_admission_seq: u64,
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
                  #[cfg(all(test, feature = "test-faults"))]
                  if let BrokerCommand::PressurePause(pause) = command {
                    let _ = pause.presence.send(self.pressure_presence());
                    let _ = pause.resume.await;
                    continue;
                  }
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
            BrokerCommand::PressurePause(_) => unreachable!("handled by actor run loop"),
            #[cfg(all(test, feature = "test-faults"))]
            BrokerCommand::ReceiveAck(raw, reply) => {
                self.upstream_event(UpstreamEvent::Raw(raw));
                let _ = reply.send((
                    matches!(self.lifecycle, Lifecycle::Running),
                    self.attachments.count(),
                ));
            }
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
                if self.lifecycle != Lifecycle::Running {
                    let _ = reply.send(Err(ControlError::Other("Browser is closing".to_string())));
                    return;
                }
                if self.control_queue.len() >= MAX_CONTROL_QUEUE {
                    let _ = reply.send(Err(ControlError::Other(
                        "control queue is full".to_string(),
                    )));
                } else if let Some(seq) = self.take_admission_seq() {
                    self.control_queue.push_back(ControlEntry {
                        _seq: seq,
                        owner,
                        takeover,
                        reply,
                    });
                    self.pump_control();
                } else {
                    let _ = reply.send(Err(ControlError::Other("Browser is busy".to_string())));
                }
            }
            BrokerCommand::Flush { operation, reply } => self.start_flush(operation, reply),
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
            BrokerCommand::TargetsForPage(page, reply) => {
                self.request_targets_for_page(page, reply)
            }
            BrokerCommand::ProfileBarrierComplete { token, result } => {
                self.profile_barrier_complete(token, result)
            }
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
        if let Endpoint::Page { target_id } = &request.endpoint {
            if let Some(message) = self.page_endpoint_error(target_id) {
                return Err(message.to_string());
            }
        }
        if request.role == Role::Controller {
            match &request.endpoint {
                Endpoint::Browser => {
                    if self.control_active.is_some() {
                        return Err("Control change is in progress".to_string());
                    }
                    if let Some(controller) = &self.controller {
                        if controller != &request.owner {
                            return Err(
                                "Another controller owns the browser; explicitly take control"
                                    .to_string(),
                            );
                        }
                    }
                    if self.active_controller.is_some() {
                        return Err("Controller is already connected".to_string());
                    }
                }
                Endpoint::Page { target_id } => {
                    if self.page_controller_taken(target_id) {
                        return Err(PAGE_SESSION_CONTROL_MESSAGE.to_string());
                    }
                }
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

    /// page 端点主目标只能是未归属（或同页）的顶层 page；实例与子目标一律拒绝。
    fn page_endpoint_error(&self, target_id: &str) -> Option<&'static str> {
        if INSTANCE_TARGET_IDS.contains(&target_id)
            || self.attachments.is_not_top_level_page(target_id)
        {
            return Some(PAGE_ENDPOINT_TARGET_MESSAGE);
        }
        if self.attachments.page_target_claimed_by_other(target_id) {
            return Some(FOREIGN_TARGET_MESSAGE);
        }
        None
    }

    /// page controller 只在没有活动控制者、也没有同会话的另一个预留时才能接入。
    fn page_controller_taken(&self, target_id: &str) -> bool {
        self.page_sessions.contains_key(target_id)
            || self.reservations.values().any(|reservation| {
                reservation.role == Role::Controller
                    && matches!(&reservation.endpoint,
                        Endpoint::Page { target_id: other } if other == target_id)
            })
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
        // 预留后目标归属可能变化；提交前按同一规则复核，viewer 也受限。
        if let Endpoint::Page { target_id } = &reservation.endpoint {
            if self.page_endpoint_error(target_id).is_some() {
                return false;
            }
        }
        let page = matches!(reservation.endpoint, Endpoint::Page { .. });
        let mut epoch = self.control_epoch;
        // 先验证控制权身份，再取得准入序号；耗尽时不改变 owner/epoch。
        if reservation.role == Role::Controller {
            match &reservation.endpoint {
                Endpoint::Browser => {
                    if self.control_active.is_some() || self.active_controller.is_some() {
                        return false;
                    }
                    if let Some(controller) = &self.controller {
                        if controller != &reservation.owner {
                            return false;
                        }
                    }
                }
                Endpoint::Page { target_id } => {
                    if self.page_controller_taken(target_id) {
                        return false;
                    }
                    match take_sequence(&mut self.next_page_epoch) {
                        Some(page_epoch) => epoch = page_epoch,
                        None => return false,
                    }
                }
            }
        }
        let page_seq = if page {
            match self.take_admission_seq() {
                Some(seq) => Some(seq),
                None => return false,
            }
        } else {
            None
        };
        if reservation.role == Role::Controller {
            match &reservation.endpoint {
                Endpoint::Browser => {
                    if self.controller.is_none() {
                        self.controller = Some(reservation.owner.clone());
                    }
                    self.active_controller = Some(id);
                }
                Endpoint::Page { target_id } => {
                    // 主 target 在连接建立时即归属该页面，先于它的 attach 回复。
                    if !self.attachments.claim_primary(target_id) {
                        return false;
                    }
                    self.page_sessions.insert(
                        target_id.clone(),
                        PageSession {
                            controller: id,
                            epoch,
                        },
                    );
                }
            }
        }
        self.notes_tx.send_modify(|notes| {
            notes.clients.retain(|id, _| self.clients.contains_key(id));
            notes.clients.insert(id, (false, false));
        });
        let page_id = match &reservation.endpoint {
            Endpoint::Page { target_id } => Some(target_id.clone()),
            Endpoint::Browser => None,
        };
        let client = Client {
            slots: Arc::new(Semaphore::new(MAX_PENDING_PER_CLIENT)),
            ids: Arc::new(Mutex::new(std::collections::HashSet::new())),
            owner: reservation.owner,
            role: reservation.role,
            epoch,
            endpoint: reservation.endpoint.clone(),
            page: page_id,
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
            let Some(seq) = page_seq else {
                self.disconnect_client(id, 1008, "Target expired", true);
                return false;
            };
            if !self.start_page_attach(id, &target_id, seq) {
                self.disconnect_client(id, 1008, "Target expired", true);
            }
        }
        true
    }

    fn client_frame(&mut self, client_id: ClientId, text: String, ingress: PayloadLease) {
        let Some(metadata) = self.upstream.handle.budget.try_acquire(64 * 1024, false) else {
            self.disconnect_client(client_id, 1011, "Payload budget exhausted", true);
            return;
        };
        let Some(value) =
            Request::parse_with_metadata(text, ingress, metadata, &self.upstream.handle.budget)
        else {
            self.send_error(client_id, 0, -32000, "Invalid CDP request", None);
            return;
        };
        let Some(external_id) = value.id else {
            self.send_error(
                client_id,
                0,
                -32000,
                "Invalid CDP request",
                error_session(&value),
            );
            return;
        };
        let Some(method) = value.method().map(str::to_string) else {
            self.send_error(
                client_id,
                external_id,
                -32000,
                "Invalid CDP request",
                error_session(&value),
            );
            return;
        };
        let mut value = value;
        if let Err(message) = self.check_request(client_id, &value, external_id, &method) {
            self.send_error(
                client_id,
                external_id,
                -32000,
                message,
                error_session(&value),
            );
            return;
        }
        let Some((slots, ids)) = self
            .clients
            .get(&client_id)
            .map(|client| (client.slots.clone(), client.ids.clone()))
        else {
            return;
        };
        let permits = slots
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
        // actor 内无 await；权限检查及双槽成功后登记 ID，构成唯一语义准入点。
        let Some(seq) = self.take_admission_seq() else {
            self.send_error(
                client_id,
                external_id,
                -32000,
                "CDP admission sequence exhausted",
                error_session(&value),
            );
            return;
        };
        ids.lock().unwrap().insert(external_id);
        value.lease = Some(Arc::new(RequestLease {
            _client: local,
            _global: global,
            ids,
            id: external_id,
            seq,
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
        let Some(client) = self.clients.get(&client_id) else {
            return;
        };
        // Browser.close 完成 controller/session 检查后直接进入共用 close barrier。
        if method == "Browser.close" {
            self.admit_request(client_id, request, external_id, method);
            return;
        }
        if client.page_ready {
            self.admit_request(client_id, request, external_id, method);
            return;
        }
        let bytes = request.bytes();
        let Some(client) = self.clients.get_mut(&client_id) else {
            return;
        };
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
        if let Err(message) = self.check_dispatch(client_id, &request, external_id, method) {
            self.send_error(
                client_id,
                external_id,
                -32000,
                message,
                error_session(&request),
            );
            return;
        }
        let session = error_session(&request);
        if method == "Target.getTargets" && self.client_has_page(client_id) {
            self.request_page_targets(client_id, request, external_id, session);
            return;
        }
        // page 连接的 Target.getTargetInfo 默认指向本会话的主 target，而不是浏览器 target。
        if method == "Target.getTargetInfo" {
            if let Some(page) = self
                .clients
                .get(&client_id)
                .and_then(|state| state.page.clone())
            {
                if request.observed.string("targetId").is_none() {
                    self.request_page_target_info(
                        client_id,
                        &page,
                        session.as_deref(),
                        request,
                        external_id,
                    );
                    return;
                }
            }
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
        request: &Request,
        method: &str,
    ) -> Option<Option<String>> {
        let explicit = request.session().map(str::to_string);
        let external_id = request.id.unwrap_or_default();
        let session = error_session(request);
        if let Err(message) = self.validate_session(client_id, explicit.as_deref()) {
            self.send_error(client_id, external_id, -32000, message, session);
            return None;
        }
        if method == "Target.detachFromTarget" {
            let target_session = request.observed.string("sessionId");
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
            let discover = request.observed.discover.unwrap_or(false);
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
            return Some(primary.as_deref().map(str::to_owned));
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
        let budget = self.upstream.handle.budget.clone();
        let mut scratch = budget.json_scratch();
        let params = request.raw_params(&mut scratch);
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
            seq: None,
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
        drop(scratch);
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
        let explicit_session =
            request
                .session()
                .map(str::to_string)
                .or_else(|| match &client.endpoint {
                    Endpoint::Page { .. } => client.primary.as_deref().map(str::to_owned),
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
            client: client_id,
            epoch: client.epoch,
            external_id,
            explicit_session,
            method: method.to_string(),
            params: request.observed.clone(),
            request,
        };
        self.management_queue.push_back(work);
        self.pump_management();
    }

    /// 无副作用预检先于槽位/身份准入；排队后派发再次校验，不把 timeout 当取消。
    fn check_request(
        &self,
        client_id: ClientId,
        request: &Request,
        external_id: i64,
        method: &str,
    ) -> Result<(), &'static str> {
        if self.lifecycle != Lifecycle::Running {
            return Err(if self.lifecycle == Lifecycle::Flushing {
                "Browser is flushing"
            } else {
                "Browser is closing"
            });
        }
        self.check_client_work(client_id, request, external_id, method)
    }

    /// Drain 只放行 cutoff 前已准入的工作；身份与 session 仍逐次重验。
    fn check_dispatch(
        &self,
        client_id: ClientId,
        request: &Request,
        external_id: i64,
        method: &str,
    ) -> Result<(), &'static str> {
        let cutoff = self.dispatch_cutoff().ok_or("Browser is closing")?;
        if !request
            .lease
            .as_ref()
            .is_some_and(|lease| lease.seq <= cutoff)
        {
            return Err(if self.lifecycle == Lifecycle::Flushing {
                "Browser is flushing"
            } else {
                "Browser is closing"
            });
        }
        self.check_client_work(client_id, request, external_id, method)
    }

    fn check_client_work(
        &self,
        client_id: ClientId,
        request: &Request,
        external_id: i64,
        method: &str,
    ) -> Result<(), &'static str> {
        let client = self.clients.get(&client_id).ok_or("Client is closed")?;
        if client.closed {
            return Err("Client is closed");
        }
        if client.role == Role::Viewer && !VIEW_METHODS.contains(&method) {
            return Err("Viewer connection cannot control the browser");
        }
        if client.role == Role::Controller && (client.retired || !self.controller_active(client_id))
        {
            return Err("Control has changed; reconnect and observe before acting");
        }
        if let Some(page) = client.page.as_deref() {
            self.check_page_scope(page, request, method)?;
        }
        let owns_id = request
            .lease
            .as_ref()
            .is_some_and(|lease| lease.id == external_id && Arc::ptr_eq(&lease.ids, &client.ids));
        if !owns_id && client.ids.lock().unwrap().contains(&external_id) {
            return Err("Duplicate in-flight request id");
        }
        let session = request.session();
        self.validate_session(client_id, session)?;
        if let Some(message) = self.managed_session_error(client_id, method, request, session) {
            return Err(message);
        }
        Ok(())
    }

    /// page session 的方法与 target 都必须限定在该会话内；无法归属的参数 fail closed。
    fn check_page_scope(
        &self,
        page: &str,
        request: &Request,
        method: &str,
    ) -> Result<(), &'static str> {
        if method.starts_with("Fetch.") || PAGE_SESSION_DENIED_METHODS.contains(&method) {
            return Err(PAGE_SESSION_DENIED_MESSAGE);
        }
        if request.observed.fallback {
            return Err("Invalid CDP request");
        }
        // 未列入 allowlist 或缺少 targetId 的 Kitesurf 方法会落到实例级默认页面/渲染器。
        if method.starts_with("Kitesurf.") {
            if !PAGE_SCOPED_KITESURF_METHODS.contains(&method)
                || request.observed.string("targetId").is_none()
            {
                return Err(PAGE_SESSION_DENIED_MESSAGE);
            }
        }
        // 用参数寻址上游 session 的方法同样不能跨 page session。
        if let Some(session) = request.observed.string("sessionId") {
            let local = self.attachments.owner(session).is_some_and(|owner| {
                self.clients
                    .get(&owner)
                    .and_then(|state| state.page.as_deref())
                    == Some(page)
            });
            if !local {
                return Err(FOREIGN_SESSION_MESSAGE);
            }
        }
        let Some(target) = request.observed.string("targetId") else {
            return Ok(());
        };
        if INSTANCE_TARGET_IDS.contains(&target) {
            return Err(PAGE_SESSION_DENIED_MESSAGE);
        }
        match self.attachments.page_of_target(target) {
            Some(owner) if owner != page => Err(FOREIGN_TARGET_MESSAGE),
            // 已 attach、主 target 或引擎报告的 opener/父 frame 落在本页。
            Some(_) => Ok(()),
            // 无归属的 frame/popup 不能由 page session 先到先得。
            None => Err(PAGE_SESSION_UNOWNED_MESSAGE),
        }
    }

    /// Browser 端点沿用实例级单控制者；page session 使用自己的控制者与代次。
    fn controller_active(&self, client_id: ClientId) -> bool {
        let Some(client) = self.clients.get(&client_id) else {
            return false;
        };
        match &client.endpoint {
            Endpoint::Browser => {
                self.active_controller == Some(client_id)
                    && self.controller.as_deref() == Some(client.owner.as_str())
                    && client.epoch == self.control_epoch
            }
            Endpoint::Page { .. } => client.page.as_deref().is_some_and(|page| {
                self.page_sessions.get(page).is_some_and(|session| {
                    session.controller == client_id && session.epoch == client.epoch
                })
            }),
        }
    }

    /// 单调准入序号；耗尽后 fixed 拒绝，不 wrap、不 saturate。
    fn take_admission_seq(&mut self) -> Option<u64> {
        take_sequence(&mut self.next_admission_seq)
    }

    fn admission_cutoff(&self) -> u64 {
        self.next_admission_seq.saturating_sub(1)
    }

    /// Running 放行全部已准入工作；只有 barrier 的 Drain 阶段继续派发旧工作。
    fn dispatch_cutoff(&self) -> Option<u64> {
        match self.lifecycle {
            Lifecycle::Running => Some(u64::MAX),
            Lifecycle::Flushing => self
                .flush
                .as_ref()
                .filter(|state| matches!(state.stage, FlushStage::Drain))
                .map(|state| state.cutoff),
            Lifecycle::Closing => self
                .close
                .as_ref()
                .filter(|state| matches!(state.stage, CloseStage::Drain))
                .map(|state| state.cutoff),
            Lifecycle::Lost | Lifecycle::Closed => None,
        }
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

    fn send_reply<T: serde::Serialize>(
        &mut self,
        client: ClientId,
        external_id: i64,
        result: T,
        session: Option<String>,
    ) {
        #[derive(serde::Serialize)]
        struct Reply<'a, T> {
            id: i64,
            result: &'a T,
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
        response: &Received,
    ) {
        let serialized = response.serialize_reply(
            &self.upstream.handle.budget,
            external_id,
            session.as_deref(),
        );
        let Some((text, lease)) = serialized else {
            self.disconnect_client(client, 1011, "Payload budget exhausted", true);
            return;
        };
        self.enqueue(client, text, lease);
    }

    fn deliver_event(&mut self, client: ClientId, value: &Received) {
        let Some(state) = self.clients.get(&client) else {
            return;
        };
        if state.closed || state.retired || state.writer_gone {
            return;
        }
        if matches!(state.endpoint, Endpoint::Page { .. }) {
            let allowed = self.page_event_allowed(client, value);
            if !allowed {
                return;
            }
        }
        let budget = self.upstream.handle.budget.clone();
        let serialized = {
            let mut scratch = budget.json_scratch();
            cdp_raw::serialize_event(
                &value.raw,
                matches!(state.endpoint, Endpoint::Page { .. }),
                &budget,
                &mut scratch,
            )
        };
        let Ok((text, lease)) = serialized else {
            self.disconnect_client(client, 1011, "Payload budget exhausted", true);
            return;
        };
        self.enqueue(client, text, lease);
    }

    /// page 连接只接收自己拥有的 session 事件；attach 事件在注册后按 owner 再验一次。
    fn page_event_allowed(&self, client: ClientId, value: &Received) -> bool {
        match value.method {
            Some(EventMethod::Attached) => self.page_has_session(client, value, value.session),
            // detached_event 已按被移除 attachment 的 owner 选定唯一接收者。
            Some(EventMethod::Detached) => true,
            // 无 session 的发现事件只广播给 Browser 端点；这里只放行本连接自己的会话。
            _ => self.page_has_session(client, value, value.outer),
        }
    }

    fn page_has_session(&self, client: ClientId, value: &Received, span: Option<RawSpan>) -> bool {
        let Some(span) = span else {
            return false;
        };
        let mut scratch = self.upstream.handle.budget.json_scratch();
        let mut buffer = [0; 4096];
        let Ok(session) = value.raw.at(span).identity_text(&mut buffer, &mut scratch) else {
            return false;
        };
        session.is_some_and(|session| {
            self.attachments.owner(session) == Some(client)
                || self
                    .clients
                    .get(&client)
                    .and_then(|state| state.primary.as_deref())
                    == Some(session)
        })
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
        self.release_page_session(client);
        self.observer.with(
            "client-closed",
            json!({ "kind": "client-closed", "clients": self.clients.len() }),
        );
        if retire && !was_closed && attached && self.lifecycle != Lifecycle::Lost {
            self.start_retirement(client);
        }
        self.maybe_reap_client(client);
    }

    fn client_closed(&mut self, client: ClientId) {
        if self.clients.get(&client).is_some_and(|state| state.closed) {
            self.maybe_reap_client(client);
            return;
        }
        let Some(state) = self.clients.get_mut(&client) else {
            return;
        };
        state.closed = true;
        state.page_queue.clear();
        state.page_queue_bytes = 0;
        self.management_queue.retain(|work| work.client != client);
        if self.active_controller == Some(client) {
            self.active_controller = None;
        }
        self.release_page_session(client);
        let needs_retire =
            self.attachments.count_for(client) > 0 || self.attachments.auto_owner == Some(client);
        if needs_retire && self.lifecycle != Lifecycle::Lost {
            self.start_retirement(client);
            return;
        }
        self.maybe_reap_client(client);
    }

    /// 控制者断开后 page session 没有活动控制权；target 归属仍由 attachments 持有。
    fn release_page_session(&mut self, client: ClientId) {
        let pages = self
            .page_sessions
            .iter()
            .filter(|(_, session)| session.controller == client)
            .map(|(page, _)| page.clone())
            .collect::<Vec<_>>();
        for page in pages {
            self.page_sessions.remove(&page);
            self.attachments.release_primary(&page);
        }
    }

    /// 关闭且 pending/unknown、管理操作、retirement、attachment 引用全部清空才可回收。
    fn client_referenced(&self, client: ClientId) -> bool {
        self.pending
            .values()
            .any(|pending| pending.client == Some(client))
            || self
                .unknown
                .values()
                .any(|unknown| unknown.client == Some(client))
            || self
                .management_queue
                .iter()
                .any(|work| work.client == client)
            || self
                .management_active
                .as_ref()
                .is_some_and(|active| active.work.client == client)
            || self
                .retirements
                .iter()
                .any(|retire| retire.client == client)
            || self
                .close
                .as_ref()
                .is_some_and(|state| state.client == Some(client))
            || self.attachments.has_client(client)
    }

    /// 只在完整状态转换之后调用；lease 随各自 owner 结束，不能按计数提前释放。
    fn maybe_reap_client(&mut self, client: ClientId) {
        if self.close.as_ref().is_some_and(|state| state.succeeded) {
            return;
        }
        if !self.clients.get(&client).is_some_and(|state| state.closed)
            || self.client_referenced(client)
        {
            return;
        }
        self.clients.remove(&client);
    }
}

fn valid_owner(owner: &str) -> bool {
    let units = owner.encode_utf16().count();
    !owner.is_empty() && units <= MAX_OWNER_UTF16
}

#[cfg(all(test, feature = "test-faults"))]
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

fn error_session(request: &Request) -> Option<String> {
    request.session().map(str::to_string)
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
    seq: Option<u64>,
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
        let seq = spec.seq.or_else(|| request.as_ref().map(|lease| lease.seq));
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
                seq,
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
        if self.lifecycle != Lifecycle::Running {
            let _ = reply.send(Err("Browser is busy".to_string()));
            return;
        }
        let spec = PendingSpec {
            client: None,
            role: None,
            external_id: None,
            explicit_session: None,
            epoch: self.control_epoch,
            seq: None,
            method: "Target.getTargets".to_string(),
            kind: PendingKind::Targets(reply),
            deadline: self.internal_deadline,
        };
        if let Err(message) = self.send_request("Target.getTargets", None, None, spec) {
            self.observer.kind("targets-unavailable");
            let _ = message;
        }
    }

    fn request_targets_for_page(
        &mut self,
        page: String,
        reply: oneshot::Sender<Result<LeasedTargets, String>>,
    ) {
        if matches!(self.lifecycle, Lifecycle::Lost | Lifecycle::Closed) {
            let _ = reply.send(Err("browser session is lost".to_string()));
            return;
        }
        if self.lifecycle != Lifecycle::Running {
            let _ = reply.send(Err("Browser is busy".to_string()));
            return;
        }
        let spec = PendingSpec {
            client: None,
            role: None,
            external_id: None,
            explicit_session: None,
            epoch: self.control_epoch,
            seq: None,
            method: "Target.getTargets".to_string(),
            kind: PendingKind::ScopedTargets { page, reply },
            deadline: self.internal_deadline,
        };
        if let Err(message) = self.send_request("Target.getTargets", None, None, spec) {
            self.observer.kind("targets-unavailable");
            let _ = message;
        }
    }

    /// 该连接是否按某个 page session 限定作用域。
    fn client_has_page(&self, client: ClientId) -> bool {
        self.clients
            .get(&client)
            .is_some_and(|state| state.page.is_some())
    }

    /// page 连接的 Target.getTargets 只在 broker 内过滤；实例级 Browser 端点保持原转发路径。
    fn request_page_targets(
        &mut self,
        client_id: ClientId,
        request: Request,
        external_id: i64,
        session: Option<String>,
    ) {
        let Some((page, role, epoch)) = self.clients.get(&client_id).and_then(|state| {
            state
                .page
                .clone()
                .map(|page| (page, state.role, state.epoch))
        }) else {
            return;
        };
        let spec = PendingSpec {
            client: Some(client_id),
            role: Some(role),
            external_id: Some(external_id),
            explicit_session: session.clone(),
            epoch,
            seq: None,
            method: "Target.getTargets".to_string(),
            kind: PendingKind::TargetsForClient { page },
            deadline: self.client_deadline,
        };
        let lease = request.lease.clone();
        if let Err(message) = self.send_raw_request("Target.getTargets", None, None, spec, lease) {
            self.send_error(client_id, external_id, -32000, &message, session);
        }
    }

    /// page 连接的 Target.getTargetInfo 默认查询本会话的主 target（或显式 session 的 target）。
    fn request_page_target_info(
        &mut self,
        client_id: ClientId,
        page: &str,
        explicit_session: Option<&str>,
        request: Request,
        external_id: i64,
    ) {
        let Some((role, epoch, target)) = self.clients.get(&client_id).map(|state| {
            let target = explicit_session
                .and_then(|session| self.attachments.attachment(session))
                .map_or_else(|| page.to_string(), |attachment| attachment.target.clone());
            (state.role, state.epoch, target)
        }) else {
            return;
        };
        // 注入的 targetId 不经过 check_page_scope，这里必须再验证归属与实例 ID。
        let owned = target == page || self.attachments.page_of_target(&target) == Some(page);
        if INSTANCE_TARGET_IDS.contains(&target.as_str()) || !owned {
            let message = if INSTANCE_TARGET_IDS.contains(&target.as_str()) {
                PAGE_SESSION_DENIED_MESSAGE
            } else {
                PAGE_SESSION_UNOWNED_MESSAGE
            };
            self.send_error(
                client_id,
                external_id,
                -32000,
                message,
                explicit_session.map(str::to_string),
            );
            return;
        }
        let spec = PendingSpec {
            client: Some(client_id),
            role: Some(role),
            external_id: Some(external_id),
            explicit_session: explicit_session.map(str::to_string),
            epoch,
            seq: None,
            method: "Target.getTargetInfo".to_string(),
            kind: PendingKind::Client,
            deadline: self.client_deadline,
        };
        let params = json!({ "targetId": target });
        let Ok(raw) = serde_json::value::to_raw_value(&params) else {
            self.send_error(
                client_id,
                external_id,
                -32000,
                "Invalid CDP request",
                explicit_session.map(str::to_string),
            );
            return;
        };
        let lease = request.lease.clone();
        if let Err(message) =
            self.send_raw_request("Target.getTargetInfo", Some(&raw), None, spec, lease)
        {
            self.send_error(
                client_id,
                external_id,
                -32000,
                &message,
                explicit_session.map(str::to_string),
            );
        }
    }

    /// getTargets 的 targetInfos 也带 opener/父 frame；分页过滤前先记录归属关系。
    fn record_target_infos(&mut self, value: &Received) {
        if value.has_error() {
            return;
        }
        let budget = self.upstream.handle.budget.clone();
        let mut scratch = budget.json_scratch();
        let Some(infos) = value
            .raw
            .node("result")
            .and_then(|node| node.field("targetInfos", &mut scratch))
        else {
            return;
        };
        if !infos.is_array() {
            return;
        }
        let Some(mut array) = infos.array(&mut scratch) else {
            return;
        };
        let mut target_buffer = [0; 4096];
        let mut opener_buffer = [0; 4096];
        loop {
            let Ok(Some(info)) = array.next(&mut scratch) else {
                break;
            };
            let target = info
                .field("targetId", &mut scratch)
                .map(|node| node.identity_text(&mut target_buffer, &mut scratch))
                .transpose()
                .ok()
                .flatten()
                .flatten()
                .map(str::to_string);
            let Some(target) = target else {
                continue;
            };
            if let Some(kind) = info.field("type", &mut scratch) {
                let is_page = kind.equals("page", &mut scratch);
                self.attachments.record_target_kind(&target, is_page);
            }
            let opener = info
                .field("openerId", &mut scratch)
                .or_else(|| info.field("openerFrameId", &mut scratch))
                .map(|node| node.identity_text(&mut opener_buffer, &mut scratch))
                .transpose()
                .ok()
                .flatten()
                .flatten()
                .map(str::to_string);
            self.attachments.record_relation(&target, opener.as_deref());
        }
    }

    fn send_page_targets(
        &mut self,
        client: ClientId,
        external_id: i64,
        page: &str,
        session: Option<String>,
        value: &Received,
    ) {
        self.record_target_infos(value);
        if value.has_error() {
            self.send_upstream_reply(client, external_id, session, value);
            return;
        }
        let budget = self.upstream.handle.budget.clone();
        let filtered = {
            let mut scratch = budget.json_scratch();
            scoped_target_infos(value, &budget, &mut scratch, |target| {
                target == page || self.attachments.owns_page_target(page, target)
            })
        };
        let Ok(raw) = filtered else {
            self.send_error(
                client,
                external_id,
                -32000,
                "Payload budget exhausted",
                session,
            );
            return;
        };
        let list = {
            let mut scratch = budget.json_scratch();
            raw.raw(raw.full_span(), &mut scratch)
        };
        let Ok(list) = list else {
            self.send_error(
                client,
                external_id,
                -32000,
                "Payload budget exhausted",
                session,
            );
            return;
        };
        #[derive(serde::Serialize)]
        struct TargetList<'a> {
            #[serde(rename = "targetInfos")]
            target_infos: &'a RawValue,
        }
        self.send_reply(
            client,
            external_id,
            TargetList { target_infos: list },
            session,
        );
    }

    /// 拒绝跨 session attach 后收回上游 session；不登记 attachment，只等结算。
    fn detach_scoped(&mut self, session: &str) {
        let params = json!({ "sessionId": session });
        let spec = PendingSpec {
            client: None,
            role: None,
            external_id: None,
            explicit_session: None,
            epoch: self.control_epoch,
            seq: None,
            method: "Target.detachFromTarget".to_string(),
            kind: PendingKind::ScopeDetach,
            deadline: self.internal_deadline,
        };
        if self
            .send_request("Target.detachFromTarget", Some(&params), None, spec)
            .is_err()
        {
            self.observer.kind("scope-detach-unavailable");
        }
    }

    fn start_page_attach(&mut self, client_id: ClientId, target: &str, seq: u64) -> bool {
        if self.page_endpoint_error(target).is_some() {
            return false;
        }
        let Some(client) = self.clients.get(&client_id) else {
            return false;
        };
        let params = json!({ "targetId": target, "flatten": true });
        if self
            .attachments
            .reserve(target, client_id, client.epoch, Some(target))
            != AttachmentReservation::Reserved
        {
            return false;
        }
        let spec = PendingSpec {
            client: Some(client_id),
            role: Some(client.role),
            external_id: None,
            explicit_session: None,
            epoch: client.epoch,
            seq: Some(seq),
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
        if let UpstreamEvent::Raw(raw) = event {
            let parsed = {
                let budget = self.upstream.handle.budget.clone();
                let mut scratch = budget.json_scratch();
                RawEnvelope::parse(raw, ParsePolicy::UpstreamStrict, &mut scratch)
            };
            let Ok(raw) = parsed else {
                return;
            };
            let response = {
                let budget = self.upstream.handle.budget.clone();
                let mut scratch = budget.json_scratch();
                raw.id(&mut scratch).is_some()
            };
            let value = match Received::parse(raw, &self.upstream.handle.budget) {
                Ok(value) => value,
                Err(error) => {
                    self.receive_error(error);
                    return;
                }
            };
            if response {
                self.handle_response(value);
            } else {
                self.handle_event(value);
            }
            return;
        }
        match event {
            UpstreamEvent::Raw(_) => unreachable!(),
            UpstreamEvent::Written { bytes } => {
                self.upstream_bytes = self.upstream_bytes.saturating_sub(bytes);
                // writer 结算必须直接唤醒 Drain，不能等下一次 tick。
                self.pump_after_change();
            }
            UpstreamEvent::Lost(reason) => self.handle_lost(reason),
        }
    }

    fn receive_error(&mut self, error: cdp_raw::RawError) {
        let reason = match error {
            cdp_raw::RawError::Budget => "upstream cleanup metadata budget exhausted",
            cdp_raw::RawError::Limit => "upstream identity limit exceeded",
            cdp_raw::RawError::Invalid => "invalid upstream metadata owner",
            cdp_raw::RawError::Duplicate => "invalid upstream metadata fields",
        };
        self.handle_lost(reason.into());
    }

    /// 超限 session 只登记清理身份；容量或预算无法记录时不能静默忘记。
    fn quarantine_attachment(&mut self, client: ClientId, epoch: u64, session: &str, target: &str) {
        match self.attachments.quarantine(client, epoch, session, target) {
            QuarantineResult::Recorded
            | QuarantineResult::Existing
            | QuarantineResult::Owned
            | QuarantineResult::Foreign => {}
            QuarantineResult::Full => {
                self.handle_lost("attachment quarantine capacity exhausted".into());
            }
            QuarantineResult::Budget => {
                self.handle_lost("attachment metadata budget exhausted".into());
            }
        }
    }

    fn auto_overflow(&mut self, client: ClientId) {
        if self.attachments.auto_owner == Some(client) {
            self.start_retirement(client);
        }
    }

    /// 迟到 attach 回复仍按原 client/epoch 归属；容量不足时转入 quarantine。
    fn late_attachment(
        &mut self,
        client: Option<ClientId>,
        epoch: u64,
        session: &str,
        target: &str,
    ) {
        let Some(client) = client else {
            return;
        };
        let page = self
            .clients
            .get(&client)
            .and_then(|state| state.page.clone());
        let outcome =
            self.attachments
                .register(client, epoch, session, target, None, page.as_deref());
        self.attachments.clear_intent(target, client);
        match outcome {
            Registration::New | Registration::Existing => {}
            Registration::Full => {
                self.quarantine_attachment(client, epoch, session, target);
            }
            Registration::Foreign | Registration::Quarantined => {}
            Registration::ForeignPage => {
                self.detach_scoped(session);
                return;
            }
            Registration::Budget => {
                self.handle_lost("attachment metadata budget exhausted".into());
                return;
            }
        }
        let retired = self
            .clients
            .get(&client)
            .is_none_or(|state| state.closed || state.retired);
        if retired && matches!(outcome, Registration::New | Registration::Existing) {
            self.start_retirement(client);
        }
        self.pump_quarantine();
    }

    /// 事件只有在能证明归属时才投递；无 session 的普通事件不广播。
    fn handle_event(&mut self, value: Received) {
        let Some(method) = value.method else {
            return;
        };
        if method == EventMethod::Attached {
            self.attached_event(value);
            return;
        }
        if method == EventMethod::Detached {
            self.detached_event(value);
            return;
        }
        if method == EventMethod::Discovery {
            self.record_target_relation(&value);
        }
        if value.outer.is_none() && method == EventMethod::Discovery {
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
        let Some(outer) = value.outer else {
            return;
        };
        let budget = self.upstream.handle.budget.clone();
        let owner = {
            let mut scratch = budget.json_scratch();
            let mut buffer = [0; 4096];
            let session = value
                .raw
                .at(outer)
                .identity_text(&mut buffer, &mut scratch)
                .ok()
                .flatten();
            session.and_then(|session| self.attachments.owner(session))
        };
        if let Some(owner) = owner {
            self.deliver_event(owner, &value);
        }
    }

    /// 权威 detached 移除记录后，未结算的 cleanup detach 仍按同一 sessionId 寻址。
    /// 只扫描现有有界 pending/unknown；不删 unknown、不重放，也不保留 tombstone。
    fn cleanup_detach_unsettled(&self, session: &str) -> bool {
        let pending = self.pending.values().any(|pending| match &pending.kind {
            PendingKind::RetirementDetach {
                session: current, ..
            }
            | PendingKind::QuarantineDetach {
                session: current, ..
            } => current == session,
            _ => false,
        });
        pending
            || self.unknown.values().any(|unknown| match &unknown.kind {
                UnknownKind::RetirementDetach {
                    session: current, ..
                }
                | UnknownKind::QuarantineDetach {
                    session: current, ..
                } => current == session,
                _ => false,
            })
    }

    /// 目标信息里的关系决定子 frame/popup 的归属；缺失时目标保持无归属。
    fn record_target_relation(&mut self, value: &Received) {
        let budget = self.upstream.handle.budget.clone();
        let mut scratch = budget.json_scratch();
        let target = received_identity(value, value.target, &mut scratch);
        let opener = received_identity(value, value.opener, &mut scratch);
        let Some(target) = target else {
            return;
        };
        if value.destroyed {
            self.attachments.forget_relation(&target);
            self.attachments.forget_target_kind(&target);
        } else {
            if let Some(is_page) = value.page_target {
                self.attachments.record_target_kind(&target, is_page);
            }
            if let Some(opener) = opener.as_deref() {
                self.attachments.record_relation(&target, Some(opener));
            }
        }
    }

    // span 已在 parse 中验证类型、4096-byte 上限和 Unicode；后续只借用同一不可变 owner。
    fn attached_event(&mut self, value: Received) {
        let Some(session_span) = value.session else {
            return;
        };
        let Some(target_span) = value.target else {
            return;
        };
        let budget = self.upstream.handle.budget.clone();
        let mut session_buffer = [0; 4096];
        let mut target_buffer = [0; 4096];
        let mut outer_buffer = [0; 4096];
        let (session, target, outer) = {
            let mut scratch = budget.json_scratch();
            let session = value
                .raw
                .at(session_span)
                .identity_text(&mut session_buffer, &mut scratch)
                .ok()
                .flatten();
            let target = value
                .raw
                .at(target_span)
                .identity_text(&mut target_buffer, &mut scratch)
                .ok()
                .flatten();
            let outer = value.outer.and_then(|span| {
                value
                    .raw
                    .at(span)
                    .identity_text(&mut outer_buffer, &mut scratch)
                    .ok()
                    .flatten()
            });
            (session, target, outer)
        };
        let (Some(session), Some(target)) = (session, target) else {
            return;
        };
        // attach 事件同样携带 opener/父 frame；先记录关系，再据此判定 page 归属。
        if let Some(is_page) = value.page_target {
            self.attachments.record_target_kind(target, is_page);
        }
        if let Some(opener) = received_identity(&value, value.opener, &mut budget.json_scratch()) {
            self.attachments
                .record_relation(target, Some(opener.as_str()));
        }
        // 未结算 cleanup detach 仍按同一 sessionId 在线寻址；窗口内同 ID attached 不是
        // 新 attachment，也不派第二次 detach。存活 attachment 仍走 register 原分支。
        if self.attachments.attachment(session).is_none() && self.cleanup_detach_unsettled(session)
        {
            return;
        }
        let Some(owner) = self.attachments.resolve_owner(outer, target) else {
            return;
        };
        let Some(state) = self.clients.get(&owner) else {
            return;
        };
        let epoch = state.epoch;
        let retired = state.closed || state.retired;
        let page = state.page.clone();
        // page 连接只能登记自己 attach 过的 target，或 opener/父 frame 属于本页的目标；
        // 引擎主动附加的其他会话目标不投递，并收回上游 session。
        if let Some(page) = page.as_deref() {
            let intent = self.attachments.intent(target) == Some(owner);
            if !intent && !self.attachments.owns_page_target(page, target) {
                self.detach_scoped(session);
                return;
            }
        }
        let auto = self.attachments.intent(target).is_none()
            && outer
                .and_then(|session| self.attachments.owner(session))
                .is_none();
        let outcome =
            self.attachments
                .register(owner, epoch, session, target, outer, page.as_deref());
        match outcome {
            Registration::New | Registration::Existing => {}
            Registration::Foreign | Registration::Quarantined => return,
            // 目标已被另一个 page session 登记：不投递，也不留下匿名上游 session。
            Registration::ForeignPage => {
                self.detach_scoped(session);
                return;
            }
            Registration::Full => {
                self.quarantine_attachment(owner, epoch, session, target);
                if auto {
                    self.auto_overflow(owner);
                }
                self.pump_quarantine();
                return;
            }
            Registration::Budget => {
                self.handle_lost("attachment metadata budget exhausted".into());
                return;
            }
        }
        if retired {
            self.start_retirement(owner);
            return;
        }
        self.deliver_event(owner, &value);
    }

    fn detached_event(&mut self, value: Received) {
        let Some(span) = value.session else { return };
        let budget = self.upstream.handle.budget.clone();
        let session = {
            let mut scratch = budget.json_scratch();
            let mut buffer = [0; 4096];
            value
                .raw
                .at(span)
                .identity_text(&mut buffer, &mut scratch)
                .ok()
                .flatten()
                .map(str::to_string)
        };
        let Some(session) = session else { return };
        let Some(attachment) = self.attachments.forget(&session) else {
            if let Some(entry) = self.attachments.release_quarantine(&session) {
                self.maybe_reap_client(entry.client);
            }
            // attachment 事件同样必须唤醒 Drain，而不是只依赖 tick。
            self.pump_after_change();
            return;
        };
        let recipient = attachment.client;
        let primary = self
            .clients
            .get(&recipient)
            .is_some_and(|state| state.primary.as_deref() == Some(attachment.session.as_str()));
        if primary {
            self.disconnect_client(recipient, 1008, "Target expired", true);
            self.pump_after_change();
            return;
        }
        self.deliver_event(recipient, &value);
        self.maybe_reap_client(recipient);
        self.pump_after_change();
    }

    fn handle_response(&mut self, mut value: Received) {
        let id = {
            let budget = self.upstream.handle.budget.clone();
            let mut scratch = budget.json_scratch();
            value.raw.id(&mut scratch)
        };
        let Some(id) = id else {
            return;
        };
        let method = self
            .pending
            .get(&id)
            .map(|pending| pending.method.as_str())
            .or_else(|| self.unknown.get(&id).map(|unknown| unknown.method.as_str()));
        let Some(method) = method else { return };
        if matches!(
            method,
            "Target.attachToTarget" | "Target.attachToBrowserTarget"
        ) {
            let budget = self.upstream.handle.budget.clone();
            let mut scratch = budget.json_scratch();
            let attached = value
                .raw
                .node("result")
                .and_then(|node| node.field("sessionId", &mut scratch));
            value.attached = match attached
                .map(|node| node.cleanup_identity(&budget, &mut scratch))
                .transpose()
            {
                Ok(attached) => attached.flatten(),
                Err(error) => {
                    self.receive_error(error);
                    return;
                }
            };
        }
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
        let external_id = pending.external_id;
        let explicit_session = pending.explicit_session.clone();
        let epoch = pending.epoch;
        let method = pending.method.clone();
        match pending.kind {
            PendingKind::Targets(reply) => {
                let _ = reply.send(Err("Target discovery timed out".to_string()));
            }
            PendingKind::TargetsForClient { .. } => {
                if let Some(client) = client {
                    self.send_error(
                        client,
                        external_id.unwrap_or_default(),
                        -32000,
                        "Target discovery timed out",
                        explicit_session,
                    );
                }
            }
            PendingKind::ScopeDetach => {}
            PendingKind::ScopedTargets { reply, .. } => {
                let _ = reply.send(Err("Target discovery timed out".to_string()));
            }
            PendingKind::Flush => {
                // 先保留未结算身份，再失败本次屏障；新 flush 在结算前固定拒绝。
                self.record_unknown(
                    id,
                    Unknown {
                        _request: pending.request.clone(),
                        _internal: pending.internal.clone(),
                        client,
                        epoch,
                        seq: pending.seq,
                        method,
                        kind: UnknownKind::Flush,
                    },
                );
                self.fail_flush(message);
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
                            epoch,
                            seq: pending.seq,
                            method,
                            kind: UnknownKind::Close,
                        },
                    );
                } else {
                    self.record_unknown(
                        id,
                        Unknown {
                            _request: pending.request.clone(),
                            _internal: pending.internal.clone(),
                            client,
                            epoch,
                            seq: pending.seq,
                            method,
                            kind: UnknownKind::Flush,
                        },
                    );
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
                            epoch,
                            seq: pending.seq,
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
                    let target = match &active.stage {
                        // backfill 的 work 是 setAutoAttach，候选 target 只在 stage 里。
                        ManagedStage::BackfillAttach {
                            target: Some(target),
                            ..
                        } => Some(target.as_str().to_string()),
                        _ => active.work.params.string("targetId").map(str::to_string),
                    };
                    let session = active.work.params.string("sessionId").map(str::to_string);
                    let auto = active.work.params.auto_attach;
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
                            epoch,
                            seq: pending.seq,
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
                let op = RetireOp {
                    upstream: id,
                    operation: RetireOperation::AutoDisable,
                };
                if let Some(retire_client) = self.await_retirement(retirement, op) {
                    self.record_unknown(
                        id,
                        Unknown {
                            _request: pending.request.clone(),
                            _internal: pending.internal.clone(),
                            client: Some(retire_client),
                            epoch,
                            seq: pending.seq,
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
                let op = RetireOp {
                    upstream: id,
                    operation: RetireOperation::Detach,
                };
                if let Some(retire_client) = self.await_retirement(retirement, op) {
                    self.record_unknown(
                        id,
                        Unknown {
                            _request: pending.request.clone(),
                            _internal: pending.internal.clone(),
                            client: Some(retire_client),
                            epoch,
                            seq: pending.seq,
                            method,
                            kind: UnknownKind::RetirementDetach {
                                id: retirement,
                                session,
                            },
                        },
                    );
                }
            }
            PendingKind::QuarantineDetach {
                session,
                client,
                epoch: detach_epoch,
            } => {
                // early detached 可先移除记录，但未结算的上游写身份仍必须保留。
                let _ =
                    self.attachments
                        .mark_quarantine_awaiting(&session, id, client, detach_epoch);
                self.record_unknown(
                    id,
                    Unknown {
                        _request: pending.request.clone(),
                        _internal: pending.internal.clone(),
                        client: None,
                        epoch: detach_epoch,
                        seq: pending.seq,
                        method,
                        kind: UnknownKind::QuarantineDetach {
                            session,
                            client,
                            epoch: detach_epoch,
                        },
                    },
                );
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
                            epoch,
                            seq: pending.seq,
                            method,
                            kind: UnknownKind::Plain,
                        },
                    );
                }
            }
        }
        self.pump_after_change();
        if let Some(client) = client {
            self.maybe_reap_client(client);
        }
    }

    /// 超时只把已发送的退休操作转入等待槽，保留清理身份且绝不重放。
    fn await_retirement(&mut self, id: u64, op: RetireOp) -> Option<ClientId> {
        let retire = self.retirements.iter_mut().find(|retire| retire.id == id)?;
        if retire.pending != Some(op) {
            return None;
        }
        retire.pending = None;
        retire.awaiting = Some(op);
        Some(retire.client)
    }

    fn complete_unknown(&mut self, id: u64, value: &Received) {
        let Some(unknown) = self.unknown.remove(&id) else {
            return;
        };
        let client = unknown.client;
        let attached = attach_session(value);
        let target = if unknown.method == "Target.attachToBrowserTarget" {
            Some("browser".to_string())
        } else {
            match &unknown.kind {
                UnknownKind::PageAttach { target } => Some(target.clone()),
                UnknownKind::Managed { target, .. } => target.clone(),
                _ => None,
            }
        };
        if let (Some(session), Some(target)) = (attached.as_deref(), target.as_deref()) {
            self.late_attachment(unknown.client, unknown.epoch, session, target);
        }
        let failed = value.has_error();
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
                    if unknown.method == "Target.attachToBrowserTarget" {
                        self.attachments.clear_intent("browser", client);
                    }
                    if unknown.method == "Target.detachFromTarget" {
                        if let Some(session) = session {
                            self.attachments.forget(&session);
                        }
                    }
                    if let Some(auto) = auto {
                        self.apply_auto_result(client, auto, failed);
                    }
                }
            }
            UnknownKind::PageAttach { target } => {
                if let Some(client) = unknown.client {
                    self.attachments.clear_intent(&target, client);
                }
            }
            UnknownKind::QuarantineDetach {
                session,
                client,
                epoch: detach_epoch,
            } => {
                // 迟到结果仍按 upstream/client/epoch/session 精确匹配，不误删其他清理身份。
                if failed {
                    self.attachments
                        .finish_quarantine_failure(&session, id, client, detach_epoch);
                } else if let Some(entry) =
                    self.attachments
                        .finish_quarantine_success(&session, id, client, detach_epoch)
                {
                    self.maybe_reap_client(entry.client);
                }
            }
            UnknownKind::RetirementAuto { id: retirement } => {
                self.late_retirement_auto(id, retirement, failed);
            }
            UnknownKind::RetirementDetach {
                id: retirement,
                session,
            } => {
                self.late_retirement_detach(id, retirement, &session, failed);
            }
            UnknownKind::Close => {
                if failed {
                    self.fail_close("browser close failed".to_string());
                } else {
                    self.finish_close_success();
                }
            }
            UnknownKind::Plain | UnknownKind::Flush => {}
        }
        self.pump_after_change();
        if let Some(client) = client {
            self.maybe_reap_client(client);
        }
    }

    fn complete_pending(&mut self, id: u64, value: &Received) {
        let Some(pending) = self.pending.remove(&id) else {
            return;
        };
        let client = pending.client;
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
            PendingKind::TargetsForClient { page } => {
                if let Some(client) = pending.client {
                    self.send_page_targets(
                        client,
                        pending.external_id.unwrap_or_default(),
                        &page,
                        pending.explicit_session.clone(),
                        value,
                    );
                }
            }
            // 收回被拒绝的上游 session；没有 attachment 需要登记或通知。
            PendingKind::ScopeDetach => {}
            PendingKind::ScopedTargets { page, reply } => {
                let result = if value.has_error() {
                    Err("Target discovery failed".to_string())
                } else {
                    self.record_target_infos(value);
                    let budget = self.upstream.handle.budget.clone();
                    let filtered = {
                        let mut scratch = budget.json_scratch();
                        scoped_target_infos(value, &budget, &mut scratch, |target| {
                            target == page.as_str()
                                || self.attachments.owns_page_target(&page, target)
                        })
                    };
                    filtered
                        .map(|raw| LeasedTargets { raw })
                        .map_err(|_| "Target discovery budget exhausted".to_string())
                };
                let _ = reply.send(result);
            }
            PendingKind::RetirementAuto { id: retirement } => {
                self.retirement_auto_response(id, retirement, value);
            }
            PendingKind::RetirementDetach {
                id: retirement,
                session,
            } => {
                self.retirement_detach_response(id, retirement, &session, value);
            }
            PendingKind::QuarantineDetach {
                session,
                client,
                epoch: detach_epoch,
            } => {
                self.quarantine_detach_response(&session, id, client, detach_epoch, value);
            }
        }
        self.pump_after_change();
        if let Some(client) = client {
            self.maybe_reap_client(client);
        }
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

    fn page_attach_response(&mut self, client: Option<ClientId>, target: &str, value: &Received) {
        let Some(client_id) = client else {
            return;
        };
        let session = attach_session(value);
        let Some(session) = session else {
            self.attachments.clear_intent(target, client_id);
            self.disconnect_client(client_id, 1008, "Target expired", true);
            return;
        };
        let Some(epoch) = self.clients.get(&client_id).map(|state| state.epoch) else {
            return;
        };
        let page = self
            .clients
            .get(&client_id)
            .and_then(|state| state.page.clone());
        let outcome =
            self.attachments
                .register(client_id, epoch, &session, target, None, page.as_deref());
        self.attachments.clear_intent(target, client_id);
        match outcome {
            Registration::New | Registration::Existing => {}
            Registration::Full => {
                self.quarantine_attachment(client_id, epoch, &session, target);
                self.disconnect_client(client_id, 1008, "Target expired", true);
                return;
            }
            Registration::Foreign | Registration::Quarantined => {
                self.disconnect_client(client_id, 1008, "Target expired", true);
                return;
            }
            // reserve 已按页面归属拒绝；若仍竞争失败，只结算上游 session。
            Registration::ForeignPage => {
                self.detach_scoped(&session);
                self.disconnect_client(client_id, 1008, "Target expired", true);
                return;
            }
            Registration::Budget => {
                self.handle_lost("attachment metadata budget exhausted".into());
                return;
            }
        }
        let Some(state) = self.clients.get_mut(&client_id) else {
            return;
        };
        state.primary = Some(session.clone());
        state.page_ready = true;
        let queued = std::mem::take(&mut state.page_queue);
        state.page_queue_bytes = 0;
        for request in queued {
            let external_id = request.id.unwrap_or_default();
            let method = request.method().map(str::to_string).unwrap_or_default();
            self.admit_request(client_id, request, external_id, &method);
        }
    }

    fn managed_response(&mut self, id: u64, value: &Received) {
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
                self.backfill_attach_response(active, target.as_deref().unwrap_or_default(), value);
            }
            stage => {
                active.stage = stage;
                self.management_active = Some(active);
            }
        }
    }

    fn managed_sent_response(&mut self, mut active: ActiveManaged, value: &Received) {
        let method = active.work.method.clone();
        let client = active.work.client;
        let params = active.work.params.clone();
        if value.has_error() {
            self.clear_management_intent(&active);
            self.finish_managed_reply(active.work, value);
            return;
        }
        if method == "Target.attachToTarget" || method == "Target.attachToBrowserTarget" {
            let target = if method == "Target.attachToTarget" {
                let Some(target) = params.string("targetId") else {
                    self.finish_managed(active.work, Err((-32000, "Invalid target".into())));
                    return;
                };
                target.to_string()
            } else {
                "browser".to_string()
            };
            let Some(session) = attach_session(value) else {
                self.attachments.clear_intent(&target, client);
                self.finish_managed(
                    active.work,
                    Err((-32000, "Target attach returned no session".into())),
                );
                return;
            };
            // 已发出的 attach 按发起时的 client/epoch 登记，不借用当前 controller 身份。
            let epoch = active.work.epoch;
            let parent = active.work.explicit_session.clone();
            let page = self
                .clients
                .get(&client)
                .and_then(|state| state.page.clone());
            let outcome = self.attachments.register(
                client,
                epoch,
                &session,
                &target,
                parent.as_deref(),
                page.as_deref(),
            );
            self.attachments.clear_intent(&target, client);
            match outcome {
                Registration::New | Registration::Existing => {
                    self.finish_managed_reply(active.work, value);
                }
                Registration::Full => {
                    self.quarantine_attachment(client, epoch, &session, &target);
                    self.finish_managed(active.work, Err((-32000, ATTACH_LIMIT_MESSAGE.into())));
                }
                Registration::Foreign | Registration::Quarantined => {
                    self.finish_managed(
                        active.work,
                        Err((
                            -32000,
                            "CDP attachment expired or belongs to another connection; attach again"
                                .into(),
                        )),
                    );
                }
                Registration::ForeignPage => {
                    self.detach_scoped(&session);
                    self.finish_managed(active.work, Err((-32000, FOREIGN_TARGET_MESSAGE.into())));
                }
                Registration::Budget => {
                    self.handle_lost("attachment metadata budget exhausted".into());
                    self.finish_managed(
                        active.work,
                        Err((
                            -32000,
                            "Browser session ended; outcome may be unknown".into(),
                        )),
                    );
                }
            }
            return;
        }
        if method == "Target.detachFromTarget" {
            if let Some(session) = params.string("sessionId") {
                self.attachments.forget(session);
            }
            self.finish_managed_reply(active.work, value);
            return;
        }
        let auto_attach = params.auto_attach.unwrap_or(false);
        let backfill = auto_attach && active.work.explicit_session.is_none();
        if backfill {
            let saved = {
                let budget = self.upstream.handle.budget.clone();
                let mut scratch = budget.json_scratch();
                match value.raw.node("result") {
                    Some(result) => result.copy(&budget, &mut scratch),
                    None => LeasedRaw::literal("null", &budget),
                }
            };
            let Ok(saved) = saved else {
                self.finish_managed(
                    active.work,
                    Err((-32000, "Payload budget exhausted".into())),
                );
                return;
            };
            active.response = Some(saved);
            match self.start_backfill(&mut active) {
                Ok(()) => {
                    self.management_active = Some(active);
                    return;
                }
                // 权限拒绝必须终结原请求，不能被普通发送失败的 fallback 当成成功回复。
                Err(Some(message)) => {
                    self.finish_managed(active.work, Err((-32000, message.into())));
                    return;
                }
                Err(None) => {}
            }
        }
        self.finish_managed_reply(active.work, value);
    }

    /// 发送 getTargets 前重验原工作；授权拒绝与普通发送失败分别返回。
    fn start_backfill(&mut self, active: &mut ActiveManaged) -> Result<(), Option<&'static str>> {
        if let Err(message) = self.check_managed_dispatch(&active.work) {
            return Err(Some(message));
        }
        let work = &active.work;
        let spec = PendingSpec {
            client: Some(work.client),
            role: self.clients.get(&work.client).map(|state| state.role),
            external_id: None,
            explicit_session: None,
            epoch: work.epoch,
            seq: None,
            method: "Target.getTargets".to_string(),
            kind: PendingKind::Managed,
            deadline: self.internal_deadline,
        };
        let request = active.work.request.lease.clone();
        let id = self
            .send_raw_request("Target.getTargets", None, None, spec, request)
            .map_err(|_| None)?;
        active.stage = ManagedStage::BackfillTargets { id };
        Ok(())
    }

    fn backfill_targets(&mut self, mut active: ActiveManaged, value: &Received) {
        if value.has_error() {
            self.finish_managed_reply(active.work, value);
            return;
        }
        let selected = self.select_backfill(value, active.work.client);
        let targets = match selected {
            Ok(targets) => targets,
            Err(cdp_raw::RawError::Limit) => {
                self.handle_lost("upstream identity limit exceeded".into());
                return;
            }
            Err(_) => {
                self.finish_managed(
                    active.work,
                    Err((-32000, "Payload budget exhausted".into())),
                );
                return;
            }
        };
        active.stage = ManagedStage::BackfillAttach {
            id: 0,
            targets,
            index: 0,
            target: None,
        };
        self.continue_backfill(active);
    }

    fn select_backfill(
        &self,
        value: &Received,
        client: ClientId,
    ) -> Result<BackfillSelection, cdp_raw::RawError> {
        let budget = &self.upstream.handle.budget;
        let mut scratch = budget.json_scratch();
        let raw = target_infos(value, budget, &mut scratch)?;
        let mut count = 0usize;
        for pass in 0..2 {
            let mut array = raw
                .node()
                .array(&mut scratch)
                .ok_or(cdp_raw::RawError::Invalid)?;
            let mut spans = if pass == 1 {
                let bytes = count
                    .checked_mul(std::mem::size_of::<RawSpan>())
                    .ok_or(cdp_raw::RawError::Limit)?;
                let lease = budget
                    .try_acquire(bytes, false)
                    .ok_or(cdp_raw::RawError::Budget)?;
                Some((Vec::with_capacity(count), lease))
            } else {
                None
            };
            while let Some(info) = array.next(&mut scratch)? {
                let id = info
                    .field("targetId", &mut scratch)
                    .map(|node| node.string(budget, &mut scratch))
                    .transpose()?
                    .flatten();
                let page = info
                    .field("type", &mut scratch)
                    .is_some_and(|node| node.equals("page", &mut scratch));
                if !page
                    || !id
                        .as_ref()
                        .is_some_and(|id| !self.attachments.has_target(client, id))
                {
                    continue;
                }
                if let Some((spans, _)) = spans.as_mut() {
                    if spans.len() >= spans.capacity() {
                        return Err(cdp_raw::RawError::Limit);
                    }
                    spans.push(
                        info.field("targetId", &mut scratch)
                            .ok_or(cdp_raw::RawError::Invalid)?
                            .span(),
                    );
                } else {
                    count += 1;
                }
            }
            if let Some((spans, lease)) = spans {
                return Ok(BackfillSelection {
                    raw,
                    spans,
                    _lease: lease,
                });
            }
        }
        Err(cdp_raw::RawError::Invalid)
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
        // 重验先于解析候选、推进 index、预约 intent 与发送，空列表也不能算成功。
        if let Err(message) = self.check_managed_dispatch(&active.work) {
            return BackfillStep::Failed(message.into());
        }
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
            while next < targets.spans.len() {
                let budget = self.upstream.handle.budget.clone();
                let decoded = {
                    let mut scratch = budget.json_scratch();
                    targets
                        .raw
                        .raw(targets.spans[next], &mut scratch)
                        .and_then(|raw| cdp_raw::decode_identity(raw, &budget, &mut scratch))
                };
                let Ok(current) = decoded else {
                    return BackfillStep::Failed("Payload budget exhausted".into());
                };
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
            *target = Some(candidate.clone());
            (candidate, next)
        };
        let _ = next;
        let page = self
            .clients
            .get(&client)
            .and_then(|state| state.page.clone());
        match self
            .attachments
            .reserve(&candidate, client, epoch, page.as_deref())
        {
            AttachmentReservation::Reserved => {}
            AttachmentReservation::Full => {
                self.auto_overflow(client);
                return BackfillStep::Failed(ATTACH_LIMIT_MESSAGE.into());
            }
            AttachmentReservation::ForeignPage => {
                return BackfillStep::Failed(FOREIGN_TARGET_MESSAGE.into());
            }
            AttachmentReservation::Busy | AttachmentReservation::Budget => {
                return BackfillStep::Failed("Payload budget exhausted".into());
            }
        }
        let params = json!({ "targetId": candidate.as_str(), "flatten": true });
        let spec = PendingSpec {
            client: Some(client),
            role: self.clients.get(&client).map(|state| state.role),
            external_id: None,
            explicit_session,
            epoch,
            seq: None,
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

    fn backfill_attach_response(&mut self, active: ActiveManaged, target: &str, value: &Received) {
        let client = active.work.client;
        if value.has_error() {
            // 先清匹配 intent，再按原身份决定回复；不创建 attachment，也不发 detach。
            self.attachments.clear_intent(target, client);
            if let Err(message) = self.check_managed_dispatch(&active.work) {
                self.finish_managed(active.work, Err((-32000, message.into())));
            } else {
                self.finish_managed_reply(active.work, value);
            }
            return;
        }
        let Some(session) = attach_session(value) else {
            self.attachments.clear_intent(target, client);
            self.finish_managed(
                active.work,
                Err((-32000, "Target attach returned no session".into())),
            );
            return;
        };
        // 已发出的 attach 始终按原 client/epoch 登记，不读取当前 controller 身份。
        let epoch = active.work.epoch;
        let page = self
            .clients
            .get(&client)
            .and_then(|state| state.page.clone());
        let outcome =
            self.attachments
                .register(client, epoch, &session, target, None, page.as_deref());
        self.attachments.clear_intent(target, client);
        match outcome {
            Registration::New | Registration::Existing => self.continue_backfill(active),
            Registration::Full => {
                self.quarantine_attachment(client, epoch, &session, target);
                self.auto_overflow(client);
                self.finish_managed(active.work, Err((-32000, ATTACH_LIMIT_MESSAGE.into())));
            }
            Registration::Foreign | Registration::Quarantined => {
                self.finish_managed(
                    active.work,
                    Err((
                        -32000,
                        "CDP attachment expired or belongs to another connection; attach again"
                            .into(),
                    )),
                );
            }
            Registration::ForeignPage => {
                self.detach_scoped(&session);
                self.finish_managed(active.work, Err((-32000, FOREIGN_TARGET_MESSAGE.into())));
            }
            Registration::Budget => {
                self.handle_lost("attachment metadata budget exhausted".into());
                self.finish_managed(
                    active.work,
                    Err((
                        -32000,
                        "Browser session ended; outcome may be unknown".into(),
                    )),
                );
            }
        }
    }

    fn clear_management_intent(&mut self, active: &ActiveManaged) {
        match active.work.method.as_str() {
            "Target.attachToTarget" => {
                if let Some(target) = active.work.params.string("targetId") {
                    self.attachments.clear_intent(target, active.work.client);
                }
            }
            "Target.attachToBrowserTarget" => {
                self.attachments.clear_intent("browser", active.work.client);
            }
            _ => {}
        }
    }

    fn finish_managed_reply(&mut self, work: ManagedWork, value: &Received) {
        if !value.has_error() && value.raw.node("result").is_none() {
            self.send_reply(
                work.client,
                work.external_id,
                Option::<&RawValue>::None,
                work.explicit_session.clone(),
            );
        } else {
            self.send_upstream_reply(
                work.client,
                work.external_id,
                work.explicit_session.clone(),
                value,
            );
        }
        self.management_active = None;
        self.pump_management();
    }

    fn finish_managed(
        &mut self,
        work: ManagedWork,
        result: Result<Option<LeasedRaw>, (i64, String)>,
    ) {
        let exists = self
            .clients
            .get(&work.client)
            .is_some_and(|state| !state.closed);
        if exists {
            match result {
                Ok(value) => {
                    let serialized = {
                        let budget = self.upstream.handle.budget.clone();
                        let mut scratch = budget.json_scratch();
                        value
                            .as_ref()
                            .map(|raw| raw.raw(raw.full_span(), &mut scratch))
                            .transpose()
                    };
                    match serialized {
                        Ok(raw) => self.send_reply(
                            work.client,
                            work.external_id,
                            raw,
                            work.explicit_session.clone(),
                        ),
                        Err(_) => self.send_error(
                            work.client,
                            work.external_id,
                            -32000,
                            "Payload budget exhausted",
                            work.explicit_session.clone(),
                        ),
                    }
                }
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
        if self.management_active.is_some() {
            return;
        }
        let Some(cutoff) = self.dispatch_cutoff() else {
            return;
        };
        if self.control_active.is_some()
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
            if !work
                .request
                .lease
                .as_ref()
                .is_some_and(|lease| lease.seq <= cutoff)
            {
                let session = work.explicit_session.clone();
                let message = self.lifecycle_busy_message();
                self.send_error(work.client, work.external_id, -32000, message, session);
                continue;
            }
            if self.dispatch_managed(work) {
                return;
            }
        }
    }

    fn lifecycle_busy_message(&self) -> &'static str {
        if self.lifecycle == Lifecycle::Flushing {
            "Browser is flushing"
        } else {
            "Browser is closing"
        }
    }

    fn dispatch_managed(&mut self, mut work: ManagedWork) -> bool {
        // 排队期间 control、session 与 lease 归属都可能变化；派发前统一重验原身份。
        if let Err(message) = self.check_managed_dispatch(&work) {
            self.send_error(
                work.client,
                work.external_id,
                -32000,
                message,
                work.explicit_session.clone(),
            );
            return false;
        }
        let target = work.params.string("targetId").map(str::to_string);
        let session = work.explicit_session.clone();
        if let Err(message) = self.prepare_managed(&work, target.as_deref()) {
            self.send_error(
                work.client,
                work.external_id,
                -32000,
                message,
                work.explicit_session.clone(),
            );
            return false;
        }
        let spec = PendingSpec {
            client: Some(work.client),
            role: self.clients.get(&work.client).map(|state| state.role),
            external_id: work.external_id.into(),
            explicit_session: work.explicit_session.clone(),
            epoch: work.epoch,
            seq: None,
            method: work.method.clone(),
            kind: PendingKind::Managed,
            deadline: self.internal_deadline,
        };
        let budget = self.upstream.handle.budget.clone();
        let mut scratch = budget.json_scratch();
        let params = work.request.raw_params(&mut scratch);
        let sent = self.send_raw_request(
            &work.method,
            params,
            session.as_deref(),
            spec,
            work.request.lease.clone(),
        );
        drop(scratch);
        match sent {
            Ok(id) => {
                // 已派发后只保留有界 intent；原始大 params 不属于未知结果身份。
                work.request.release_raw();
                self.management_active = Some(ActiveManaged {
                    work,
                    stage: ManagedStage::Sent { id },
                    response: None,
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
        request: &Request,
        explicit_session: Option<&str>,
    ) -> Option<&'static str> {
        let foreign = |session: &str| self.attachments.owner(session) != Some(client_id);
        if explicit_session.is_some_and(foreign) {
            return Some("CDP attachment expired or belongs to another connection; attach again");
        }
        if method == "Target.detachFromTarget" {
            let session = request.observed.string("sessionId");
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
        // page 连接从不持有实例级 auto owner，失败回滚也不能改写它。
        if self
            .clients
            .get(&client)
            .is_none_or(|state| state.page.is_some())
        {
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

    fn prepare_managed(
        &mut self,
        work: &ManagedWork,
        target: Option<&str>,
    ) -> Result<(), &'static str> {
        match work.method.as_str() {
            "Target.attachToTarget" => {
                let target = target.ok_or("Invalid target")?;
                self.reserve_attachment(target, work.client, work.epoch)
            }
            "Target.attachToBrowserTarget" => {
                self.reserve_attachment("browser", work.client, work.epoch)
            }
            "Target.detachFromTarget" => {
                let session = work.params.string("sessionId").ok_or("Invalid target")?;
                if self.attachments.owner(session) != Some(work.client) {
                    return Err("This connection does not own that attachment");
                }
                Ok(())
            }
            "Target.setAutoAttach" => {
                // Node 在发送前记录 auto owner，失败不回滚；这里保持同一可见性。
                // page 连接在准入时已明确拒绝，只有 Browser 端点能改写实例级 auto owner。
                if self
                    .clients
                    .get(&work.client)
                    .is_some_and(|state| state.page.is_none())
                {
                    let auto = work.params.auto_attach.unwrap_or(false);
                    if auto {
                        self.attachments.auto_owner = Some(work.client);
                    } else {
                        self.attachments.auto_owner = None;
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn reserve_attachment(
        &mut self,
        target: &str,
        client: ClientId,
        epoch: u64,
    ) -> Result<(), &'static str> {
        let page = self
            .clients
            .get(&client)
            .and_then(|state| state.page.clone());
        match self
            .attachments
            .reserve(target, client, epoch, page.as_deref())
        {
            AttachmentReservation::Reserved => Ok(()),
            AttachmentReservation::Full => Err(ATTACH_LIMIT_MESSAGE),
            // 目标属于另一个 page session；明确拒绝，不静默转发。
            AttachmentReservation::ForeignPage => Err(FOREIGN_TARGET_MESSAGE),
            AttachmentReservation::Busy | AttachmentReservation::Budget => {
                Err("This connection does not own that attachment")
            }
        }
    }

    fn clear_intent_for_work(&mut self, work: &ManagedWork) {
        if work.method == "Target.attachToBrowserTarget" {
            self.attachments.clear_intent("browser", work.client);
        } else if let Some(target) = work.params.string("targetId") {
            self.attachments.clear_intent(target, work.client);
        }
    }

    /// backfill 派生阶段不是新准入：只放行仍持有原 lease 且身份、session 未失效的工作。
    fn check_managed_dispatch(&self, work: &ManagedWork) -> Result<(), &'static str> {
        self.check_dispatch(work.client, &work.request, work.external_id, &work.method)?;
        if !self.client_can_act(work.client, work.epoch) {
            return Err("Control has changed; reconnect and observe before acting");
        }
        let client = self.clients.get(&work.client).ok_or("Client is closed")?;
        let lease = work
            .request
            .lease
            .as_ref()
            .ok_or("CDP request is no longer in flight")?;
        if lease.id != work.external_id
            || !Arc::ptr_eq(&lease.ids, &client.ids)
            || !lease.ids.lock().unwrap().contains(&work.external_id)
        {
            return Err("Duplicate in-flight request id");
        }
        if let Some(message) = self.managed_session_error(
            work.client,
            &work.method,
            &work.request,
            work.explicit_session.as_deref(),
        ) {
            return Err(message);
        }
        Ok(())
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
        self.controller_active(client)
    }
}

fn attach_session(value: &Received) -> Option<LeasedIdentity> {
    value.attached.clone()
}

fn take_sequence(next: &mut u64) -> Option<u64> {
    let seq = *next;
    *next = seq.checked_add(1)?;
    Some(seq)
}

fn stage_id(stage: &ManagedStage) -> Option<u64> {
    match stage {
        ManagedStage::Sent { id } => Some(*id),
        ManagedStage::BackfillTargets { id } => Some(*id),
        ManagedStage::BackfillAttach { id, .. } => Some(*id),
    }
}

fn received_identity(
    value: &Received,
    span: Option<RawSpan>,
    scratch: &mut crate::transport::JsonScratchGuard<'_>,
) -> Option<String> {
    let mut buffer = [0; 4096];
    value
        .raw
        .at(span?)
        .identity_text(&mut buffer, scratch)
        .ok()
        .flatten()
        .map(str::to_string)
}

fn target_infos(
    value: &Received,
    budget: &PayloadBudget,
    scratch: &mut crate::transport::JsonScratchGuard<'_>,
) -> Result<LeasedRaw, cdp_raw::RawError> {
    let infos = value
        .raw
        .node("result")
        .and_then(|node| node.field("targetInfos", scratch));
    match infos.filter(|node| node.is_array()) {
        Some(infos) => infos.copy(budget, scratch),
        None => LeasedRaw::literal("[]", budget),
    }
}

/// 保持 targetInfos 每个元素的原始字节，只保留本 page session 可见的 target。
fn scoped_target_infos(
    value: &Received,
    budget: &PayloadBudget,
    scratch: &mut crate::transport::JsonScratchGuard<'_>,
    owned: impl Fn(&str) -> bool,
) -> Result<LeasedRaw, cdp_raw::RawError> {
    let infos = target_infos(value, budget, scratch)?;
    let capacity = infos.as_str().len().saturating_add(1);
    let lease = budget
        .try_acquire(capacity, false)
        .ok_or(cdp_raw::RawError::Budget)?;
    let mut text = String::with_capacity(capacity);
    text.push('[');
    let mut array = infos
        .node()
        .array(scratch)
        .ok_or(cdp_raw::RawError::Invalid)?;
    let mut buffer = [0; 4096];
    let mut first = true;
    while let Some(info) = array.next(scratch)? {
        let target = info
            .field("targetId", scratch)
            .map(|node| node.identity_text(&mut buffer, scratch))
            .transpose()?
            .flatten();
        if !target.is_some_and(&owned) {
            continue;
        }
        if !first {
            text.push(',');
        }
        first = false;
        text.push_str(info.raw(scratch)?.get());
    }
    text.push(']');
    LeasedRaw::new(text, lease)
}

fn targets_from(value: &Received, budget: &PayloadBudget) -> Result<LeasedTargets, String> {
    if value.has_error() {
        return Err("Target discovery failed".into());
    }
    let mut scratch = budget.json_scratch();
    let raw = target_infos(value, budget, &mut scratch)
        .map_err(|_| "Target discovery budget exhausted")?;
    Ok(LeasedTargets { raw })
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
            self.maybe_reap_client(client);
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
            awaiting: None,
            failed: false,
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
                    && retire.awaiting.is_none()
                    && !retire.failed
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
                // 发送前容量不足保持 Ready；不记 unknown，等释放后由 pump 重试。
                let _ = self.spawn_retire_auto(id, client);
                return;
            }
            let session = self.attachments.first_session(client);
            let Some(session) = session else {
                self.retirements.remove(index);
                self.maybe_reap_client(client);
                continue;
            };
            // 发送前容量不足保持 Ready；不记 unknown，等释放后由 pump 重试。
            let _ = self.spawn_retire_detach(id, client, &session);
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
            seq: None,
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
                    retire.pending = Some(RetireOp {
                        upstream: id,
                        operation: RetireOperation::AutoDisable,
                    });
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
            seq: None,
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
                    retire.pending = Some(RetireOp {
                        upstream: id,
                        operation: RetireOperation::Detach,
                    });
                }
                let _ = client;
                Some(id)
            }
            Err(_) => None,
        }
    }

    fn retirement_auto_response(&mut self, upstream: u64, retirement: u64, value: &Received) {
        let Some(retire) = self
            .retirements
            .iter_mut()
            .find(|retire| retire.id == retirement)
        else {
            return;
        };
        let op = RetireOp {
            upstream,
            operation: RetireOperation::AutoDisable,
        };
        if retire.pending != Some(op) {
            return;
        }
        retire.pending = None;
        if value.has_error() {
            retire.failed = true;
            return;
        }
        let client = retire.client;
        if self.attachments.auto_owner == Some(client) {
            self.attachments.auto_owner = None;
        }
        retire.stage = RetireStage::Detach;
    }

    fn retirement_detach_response(
        &mut self,
        upstream: u64,
        retirement: u64,
        session: &str,
        value: &Received,
    ) {
        let Some(retire) = self
            .retirements
            .iter_mut()
            .find(|retire| retire.id == retirement)
        else {
            return;
        };
        let op = RetireOp {
            upstream,
            operation: RetireOperation::Detach,
        };
        if retire.pending != Some(op) {
            return;
        }
        retire.pending = None;
        if value.has_error() {
            retire.failed = true;
            return;
        }
        let client = retire.client;
        if self.attachments.owner(session) == Some(client) {
            self.attachments.forget(session);
        }
    }

    /// 迟到成功只解除精确匹配的等待操作；迟到错误保留归属并转入确认失败。
    fn late_retirement_auto(&mut self, upstream: u64, retirement: u64, failed: bool) {
        let op = RetireOp {
            upstream,
            operation: RetireOperation::AutoDisable,
        };
        let Some(retire) = self
            .retirements
            .iter_mut()
            .find(|retire| retire.id == retirement)
        else {
            return;
        };
        if retire.awaiting != Some(op) {
            return;
        }
        retire.awaiting = None;
        if failed {
            retire.failed = true;
            return;
        }
        let client = retire.client;
        if self.attachments.auto_owner == Some(client) {
            self.attachments.auto_owner = None;
        }
        retire.stage = RetireStage::Detach;
    }

    /// 迟到 detach 成功只删除仍属于本 retirement 的 session，不碰新 owner。
    fn late_retirement_detach(
        &mut self,
        upstream: u64,
        retirement: u64,
        session: &str,
        failed: bool,
    ) {
        let op = RetireOp {
            upstream,
            operation: RetireOperation::Detach,
        };
        let Some(retire) = self
            .retirements
            .iter_mut()
            .find(|retire| retire.id == retirement)
        else {
            return;
        };
        if retire.awaiting != Some(op) {
            return;
        }
        retire.awaiting = None;
        if failed {
            retire.failed = true;
            return;
        }
        let client = retire.client;
        if self.attachments.owner(session) == Some(client) {
            self.attachments.forget(session);
        }
    }

    /// quarantine 清理只用不占 attachment 槽的内部 reserve；容量不足时留待下次 tick。
    fn pump_quarantine(&mut self) {
        if matches!(self.lifecycle, Lifecycle::Lost | Lifecycle::Closed) {
            return;
        }
        loop {
            let Some((session, owner, epoch)) = self.attachments.next_quarantine() else {
                return;
            };
            // 自动溢出先确认禁用 autoattach，再清理该 session。
            if self
                .retirements
                .iter()
                .any(|retire| retire.client == owner && retire.stage == RetireStage::Auto)
            {
                return;
            }
            let Some(id) = self.spawn_quarantine_detach(&session, owner, epoch) else {
                return;
            };
            // 同一步 actor 内记录不会消失；失败仅表示身份已变，保留 pending 等待结算。
            let _ = self
                .attachments
                .mark_quarantine_sent(&session, id, owner, epoch);
        }
    }

    fn spawn_quarantine_detach(
        &mut self,
        session: &str,
        client: ClientId,
        epoch: u64,
    ) -> Option<u64> {
        let params = json!({ "sessionId": session });
        let spec = PendingSpec {
            client: None,
            role: None,
            external_id: None,
            explicit_session: None,
            epoch: self.control_epoch,
            seq: None,
            method: "Target.detachFromTarget".to_string(),
            kind: PendingKind::QuarantineDetach {
                session: session.to_string(),
                client,
                epoch,
            },
            deadline: self.internal_deadline,
        };
        self.send_request("Target.detachFromTarget", Some(&params), None, spec)
            .ok()
    }

    /// 成功与失败都按 upstream/client/epoch/session 精确匹配，不碰其他清理身份。
    fn quarantine_detach_response(
        &mut self,
        session: &str,
        upstream: u64,
        client: ClientId,
        epoch: u64,
        value: &Received,
    ) {
        if value.has_error() {
            self.attachments
                .finish_quarantine_failure(session, upstream, client, epoch);
        } else if let Some(entry) = self
            .attachments
            .finish_quarantine_success(session, upstream, client, epoch)
        {
            self.maybe_reap_client(entry.client);
        }
    }

    /// 新的显式 flush/close 才复活 ConfirmedFailure；有匹配在途身份时不重试。
    fn retry_quarantines(&mut self) {
        for (session, client, epoch) in self.attachments.confirmed_quarantine() {
            let busy = self.pending.values().any(|pending| {
                matches!(
                    &pending.kind,
                    PendingKind::QuarantineDetach { session: current, .. } if current == &session
                )
            }) || self.unknown.values().any(|unknown| {
                matches!(
                    &unknown.kind,
                    UnknownKind::QuarantineDetach { session: current, .. } if current == &session
                )
            });
            if !busy {
                self.attachments.retry_quarantine(&session, client, epoch);
            }
        }
    }

    fn quarantine_blocked(&self) -> bool {
        self.attachments.quarantine_awaiting_late()
            || self.attachments.quarantine_confirmed_failure()
    }

    fn pump_control(&mut self) {
        if self.control_active.is_some() {
            self.pump_blocked_control();
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
        let Some(entry) = self.control_queue.pop_front() else {
            return;
        };
        let ControlEntry {
            owner,
            takeover,
            reply,
            ..
        } = entry;
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
        // 显式 control 是确认失败后唯一允许的清理重试；等待中的结果绝不重放。
        let mut retried = false;
        for retire in &mut self.retirements {
            if retire.failed && retire.awaiting.is_none() {
                retire.failed = false;
                retried = true;
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
        if retried {
            self.control_progress();
        }
    }

    /// 已回复的屏障接受新显式请求重试确认失败清理；未决结果只回固定冲突。
    fn pump_blocked_control(&mut self) {
        let Some(active) = self.control_active.as_ref() else {
            return;
        };
        if !active.replied || self.control_queue.is_empty() {
            return;
        }
        if self.cleanup_blocks_control() {
            self.reply_control_queue(
                "Control cleanup could not be confirmed; retry after the old session settles",
            );
            return;
        }
        if !self.retirements.iter().any(|retire| retire.failed) {
            return;
        }
        let Some(entry) = self.control_queue.pop_front() else {
            return;
        };
        let ControlEntry {
            owner,
            takeover,
            reply,
            ..
        } = entry;
        if !valid_owner(&owner) {
            let _ = reply.send(Err(ControlError::Other(
                "A short owner name is required".to_string(),
            )));
            return;
        }
        if let Some(current) = &self.controller {
            if current != &owner && !takeover {
                let _ = reply.send(Err(ControlError::conflict(Some(current.clone()))));
                return;
            }
        }
        self.control_active = None;
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
        for retire in &mut self.retirements {
            retire.failed = false;
        }
        self.control_active = Some(ActiveControl {
            owner,
            old_epoch,
            reply: Some(reply),
            deadline: Instant::now() + CONTROL_DEADLINE,
            replied: false,
        });
        self.pump_retirements();
        self.control_progress();
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
        let confirmed_failure = self.retirements.iter().any(|retire| retire.failed);
        if !active.replied && (confirmed_failure || active.deadline <= Instant::now()) {
            self.fail_control(
                "Control cleanup could not be confirmed; retry after the old session settles",
            );
        }
    }

    /// 只回复一次固定的 blocked 错误；屏障保留到清理解除或显式重试。
    fn fail_control(&mut self, message: &str) {
        let Some(mut active) = self.control_active.take() else {
            return;
        };
        if let Some(reply) = active.reply.take() {
            let _ = reply.send(Err(ControlError::Other(message.to_string())));
        }
        active.replied = true;
        self.control_active = Some(active);
        self.observer.kind("control-blocked");
    }

    fn reply_control_queue(&mut self, message: &str) {
        while let Some(entry) = self.control_queue.pop_front() {
            let _ = entry
                .reply
                .send(Err(ControlError::Other(message.to_string())));
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

    /// 待发送、已发送或等待迟到结果的清理都未决；仅确认失败允许显式重试。
    fn cleanup_blocks_control(&self) -> bool {
        self.retirements
            .iter()
            .any(|retire| retire.pending.is_some() || retire.awaiting.is_some() || !retire.failed)
            || self.unknown_blocks_control()
    }

    /// 任何未结算的 upstream 结果都阻断屏障；只读 viewer 请求不是例外。
    fn outcome_unknown(&self) -> bool {
        !self.unknown.is_empty()
    }

    fn start_flush(&mut self, operation: FlushOperation, reply: FlushReply) {
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
        if self.control_active.is_some() || !self.control_queue.is_empty() {
            let _ = reply.send(Err("Control change is in progress".to_string()));
            return;
        }
        self.lifecycle = Lifecycle::Flushing;
        let cutoff = self.admission_cutoff();
        self.flush = Some(FlushState {
            operation,
            reply,
            host_close: None,
            stage: FlushStage::Drain,
            cutoff,
            drain_until: Instant::now() + self.internal_deadline,
        });
        // 只有显式 barrier 可以复活确认失败的清理；本次失败将固定拒绝。
        self.retry_quarantines();
        self.pump_after_change();
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
        // 结果未知或清理无法确认时立即固定失败，不发送 flush、不清除原工作。
        if self.quarantine_blocked() {
            self.fail_flush(QUARANTINE_UNCONFIRMED_MESSAGE.to_string());
            return;
        }
        if self.outcome_unknown() {
            self.fail_flush("previous request outcome is unknown".to_string());
            return;
        }
        let Some((cutoff, drain_until)) = self
            .flush
            .as_ref()
            .map(|state| (state.cutoff, state.drain_until))
        else {
            return;
        };
        if !self.drain_complete(cutoff) {
            if drain_until <= Instant::now() {
                self.fail_flush(DRAIN_TIMEOUT_MESSAGE.to_string());
            }
            return;
        }
        let spec = PendingSpec {
            client: None,
            role: None,
            external_id: None,
            explicit_session: None,
            epoch: self.control_epoch,
            seq: None,
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

    fn flush_response(&mut self, value: &Received) {
        if value.has_error() {
            // 宿主/数据库错误不把引擎文本或路径回传；会话仍可重试。
            self.fail_flush("Profile flush failed".to_string());
            return;
        }
        let Some(flush) = self.flush.as_mut() else {
            return;
        };
        let token = self.next_token;
        self.next_token = self.next_token.saturating_add(1);
        flush.stage = FlushStage::Completing(token);
        let operation = flush.operation.clone();
        let profile = self.profile.clone();
        let budget = self.upstream.handle.budget.clone();
        let commands = self.commands_tx.clone();
        tokio::spawn(async move {
            let result = match operation {
                FlushOperation::Status => profile
                    .leased_status(budget)
                    .await
                    .map(FlushOutcome::Status)
                    .map_err(|_| "Profile status unavailable".to_string()),
                FlushOperation::Backup(destination) => profile
                    .backup(destination)
                    .await
                    .map(FlushOutcome::Backup)
                    .map_err(|_| "Profile backup failed".to_string()),
            };
            let command = BrokerCommand::ProfileBarrierComplete { token, result };
            #[cfg(all(test, feature = "test-faults"))]
            {
                let gate = profile.outcome_gate.lock().unwrap().clone();
                if let Some(gate) = gate {
                    tests::pressure::send_profile_command(commands, command, gate).await;
                    return;
                }
            }
            let _ = commands.send(command).await;
        });
    }

    fn profile_barrier_complete(&mut self, token: u64, result: Result<FlushOutcome, String>) {
        let Some(flush) = self.flush.as_ref() else {
            return;
        };
        if !matches!(flush.stage, FlushStage::Completing(current) if current == token) {
            return;
        }
        self.finish_flush(result);
    }

    fn finish_flush(&mut self, result: Result<FlushOutcome, String>) {
        let Some(flush) = self.flush.take() else {
            return;
        };
        if self.lifecycle == Lifecycle::Flushing {
            self.lifecycle = Lifecycle::Running;
        }
        let _ = flush.reply.send(result);
        if let Some(reply) = flush.host_close {
            let _ = self.start_close(CloseKind::Host, None, None, None, Some(reply));
        } else {
            self.pump_after_change();
        }
    }

    fn fail_flush(&mut self, message: String) {
        if self.flush.is_some() {
            self.observer.kind("flush-failed");
            self.finish_flush(Err(message));
        }
    }

    fn start_close(
        &mut self,
        kind: CloseKind,
        client: Option<ClientId>,
        external_id: Option<i64>,
        explicit_session: Option<String>,
        reply: Option<oneshot::Sender<Result<(), String>>>,
    ) -> Result<(), String> {
        // 早退也必须答复 caller；否则 HTTP close 会把固定拒绝误报成 session lost。
        let reject = |reply: Option<oneshot::Sender<Result<(), String>>>,
                      message: &str|
         -> Result<(), String> {
            let message = message.to_string();
            if let Some(reply) = reply {
                let _ = reply.send(Err(message.clone()));
            }
            Err(message)
        };
        if matches!(self.lifecycle, Lifecycle::Lost | Lifecycle::Closed) {
            return reject(reply, "Browser session is lost");
        }
        if self.lifecycle == Lifecycle::Flushing {
            // The running process can finish its profile barrier before handling a signal.
            if let Some(flush) = self
                .flush
                .as_mut()
                .filter(|flush| kind == CloseKind::Host && flush.host_close.is_none())
            {
                flush.host_close = reply;
                return Ok(());
            }
            return reject(reply, "Browser is flushing");
        }
        if self.close.is_some() {
            return reject(reply, "Browser is closing");
        }
        if kind == CloseKind::Cdp {
            let Some(id) = client else {
                return reject(reply, "Browser session is lost");
            };
            let Some(state) = self.clients.get(&id) else {
                return reject(reply, "Browser session is lost");
            };
            if state.role != Role::Controller {
                return reject(reply, "Viewer connection cannot control the browser");
            }
            if state.retired
                || self.active_controller != Some(id)
                || self.controller.as_deref() != Some(state.owner.as_str())
                || state.epoch != self.control_epoch
            {
                return reject(
                    reply,
                    "Control has changed; reconnect and observe before acting",
                );
            }
            if let Err(message) = self.validate_session(id, explicit_session.as_deref()) {
                return reject(reply, message);
            }
        }
        if self.control_active.is_some() || !self.control_queue.is_empty() {
            return reject(reply, "Control change is in progress");
        }
        self.close = Some(CloseState {
            _request: None,
            kind,
            client,
            external_id,
            explicit_session,
            reply,
            stage: CloseStage::Drain,
            cutoff: self.admission_cutoff(),
            drain_until: Instant::now() + self.internal_deadline,
            finalize_until: None,
            started: Instant::now(),
            notified: false,
            succeeded: false,
        });
        self.lifecycle = Lifecycle::Closing;
        // close 与 flush 对称：准入时给明确失败的清理一次显式重试。
        self.retry_quarantines();
        self.pump_after_change();
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
        let stage = match state.stage {
            CloseStage::Drain => 0,
            CloseStage::Flush => 1,
            CloseStage::Browser => 2,
        };
        let (cutoff, drain_until) = (state.cutoff, state.drain_until);
        if stage == 0 {
            // 清理无法确认时保持受限状态；不发送 flush，也不伪报成功。
            if self.quarantine_blocked() {
                let notify = self.close.as_ref().is_some_and(|state| !state.notified);
                if notify {
                    if let Some(state) = self.close.as_mut() {
                        state.notified = true;
                    }
                    self.close_restricted(QUARANTINE_UNCONFIRMED_MESSAGE.to_string());
                }
                return;
            }
            if !self.drain_complete(cutoff) {
                if drain_until <= Instant::now() {
                    self.fail_close(DRAIN_TIMEOUT_MESSAGE.to_string());
                }
                return;
            }
            let spec = PendingSpec {
                client: None,
                role: None,
                external_id: None,
                explicit_session: None,
                epoch: self.control_epoch,
                seq: None,
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

    fn close_response(&mut self, value: &Received) {
        let Some(state) = self.close.as_ref() else {
            return;
        };
        let stage = match state.stage {
            CloseStage::Flush => 1,
            CloseStage::Browser => 2,
            CloseStage::Drain => 0,
        };
        if stage == 2 {
            if value.has_error() {
                self.fail_close("Browser close failed".to_string());
            } else {
                self.finish_close_success();
            }
            return;
        }
        if stage != 1 {
            return;
        }
        if value.has_error() {
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
            seq: None,
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
            Some(CloseStage::Drain) if message == DRAIN_TIMEOUT_MESSAGE => message,
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
        let client = state.client;
        if let (CloseKind::Cdp, Some(client), Some(external)) =
            (state.kind, client, state.external_id)
        {
            self.send_error(client, external, -32000, &message, state.explicit_session);
        }
        self.pump_after_change();
        if let Some(client) = client {
            self.maybe_reap_client(client);
        }
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
        if let Some(client) = client {
            self.maybe_reap_client(client);
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
        for page in std::mem::take(&mut self.page_sessions).into_keys() {
            self.attachments.release_primary(&page);
        }
        for (client, external, session) in failures {
            self.send_error(
                client,
                external,
                -32000,
                "Browser session ended; outcome may be unknown",
                session,
            );
        }
        if let Some(flush) = self.flush.take() {
            let message = "Browser session ended; outcome may be unknown";
            let _ = flush.reply.send(Err(message.to_string()));
            if let Some(reply) = flush.host_close {
                let _ = reply.send(Err(message.to_string()));
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

    /// cutoff 内队列、在途、清理与 writer-held 写全部结算才算 Drain 完成。
    fn drain_complete(&self, cutoff: u64) -> bool {
        if self.management_active.is_some() || !self.retirements.is_empty() {
            return false;
        }
        if self.attachments.quarantine_count() > 0 {
            return false;
        }
        // 相关 unknown 与 pending 一样按 cutoff 保留；只读结果同样未结算，不能绕过。
        if self
            .unknown
            .values()
            .any(|unknown| unknown.seq.is_none_or(|seq| seq <= cutoff))
        {
            return false;
        }
        if self.management_queue.iter().any(|work| {
            work.request
                .lease
                .as_ref()
                .is_none_or(|lease| lease.seq <= cutoff)
        }) {
            return false;
        }
        if self
            .clients
            .values()
            .any(|client| !client.page_queue.is_empty())
        {
            return false;
        }
        if self.pending.values().any(|pending| match pending.kind {
            PendingKind::Flush | PendingKind::Close => false,
            _ => pending.seq.is_none_or(|seq| seq <= cutoff),
        }) {
            return false;
        }
        self.upstream_bytes == 0
    }

    fn pump_after_change(&mut self) {
        self.pump_control();
        self.pump_retirements();
        self.pump_quarantine();
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
        self.pump_quarantine();
        self.pump_management();
        self.pump_flush();
        self.pump_close();
        self.pump_finalize();
    }
}

#[cfg(all(test, feature = "test-faults"))]
#[path = "../tests/broker_test.rs"]
pub(crate) mod tests;
