// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// 确定性伪上游上的 broker 契约：跨 client 的 ID/权限隔离、attachment 归属、takeover
// 屏障、flush/close 顺序、慢客户端和 engine loss。真实 workerd 由 broker_engine_test 覆盖。

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;

use crate::broker::{
    BrokerHandle, ClientCommand, ClientId, CloseKind, ControlError, Endpoint, ReserveRequest, Role,
};
use crate::observe::Observer;
use crate::services::{MemoryProfile, ProfileService};
use crate::transport::{Upstream, UpstreamAddress};

// 保留迁移前 Request 的解析策略作为 oracle；仅测试允许构造 metadata Value。
fn old_request(text: &str) -> Option<Value> {
    use serde_json::value::RawValue;
    #[derive(serde::Deserialize)]
    struct Envelope<'a> {
        #[serde(borrow)]
        id: Option<&'a RawValue>,
        #[serde(borrow)]
        method: Option<&'a RawValue>,
        #[serde(rename = "sessionId", borrow)]
        session: Option<&'a RawValue>,
        #[serde(borrow)]
        params: Option<&'a RawValue>,
    }
    #[derive(Default, serde::Deserialize)]
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
    let raw: Envelope<'_> = serde_json::from_str(text).ok()?;
    let fields: Fields<'_> = raw
        .params
        .map(|raw| serde_json::from_str(raw.get()).unwrap_or_default())
        .unwrap_or_default();
    let mut result = serde_json::Map::new();
    for (name, field) in [
        ("id", raw.id),
        ("method", raw.method),
        ("sessionId", raw.session),
        ("targetId", fields.target_id),
        ("paramSession", fields.session_id),
        ("autoAttach", fields.auto_attach),
        ("discover", fields.discover),
    ] {
        if let Some(raw) = field {
            if raw.get().len() > 4096 {
                return None;
            }
            result.insert(name.into(), serde_json::from_str(raw.get()).ok()?);
        }
    }
    Some(Value::Object(result))
}

#[test]
fn client_raw_fixed_accounting_and_failure_release() {
    use super::{ManagedWork, ObservedParams, Pending, Request, RequestLease, Unknown};
    use std::mem::size_of;
    use crate::transport::{PayloadBudget, MAX_FRAME_BYTES, ORDINARY_BYTES};
    let fixed = size_of::<Request>()
        + size_of::<ManagedWork>()
        + size_of::<Pending>()
        + size_of::<Unknown>()
        + size_of::<RequestLease>()
        + size_of::<ObservedParams>()
        + 16 * size_of::<crate::cdp_raw::RawSpan>()
        + 16 * 128;
    assert!(fixed + 12 * 4096 < 64 * 1024);
    println!("client metadata fixed={fixed}, identity copies<=49152, lease=65536");
    let budget = PayloadBudget::default();
    for text in [
        r#"{"id":1,"method":"x","params":{"targetId":"\uD800"}}"#,
        r#"{"id":1,"method":"x","params":{"targetId":"a","targetId":"b"}}"#,
    ] {
        let owned = text.to_string();
        let lease = budget.try_acquire(owned.capacity(), false).unwrap();
        drop(Request::parse(owned, lease, &budget));
        assert_eq!(budget.used().0, MAX_FRAME_BYTES);
    }
    let identity_text = r#"{"id":1,"method":"x","params":{"targetId":"held"}}"#.to_string();
    let payload = budget.try_acquire(identity_text.capacity(), false).unwrap();
    let request = Request::parse(identity_text, payload, &budget).unwrap();
    let observed = request.observed.clone();
    drop(request);
    assert!(budget.used().0 > MAX_FRAME_BYTES);
    assert_eq!(observed.string("targetId"), Some("held"));
    drop(observed);
    assert_eq!(budget.used().0, MAX_FRAME_BYTES);
    let text = r#"{"id":1,"method":"x"}"#.to_string();
    let payload = budget.try_acquire(text.capacity(), false).unwrap();
    let remaining = ORDINARY_BYTES - budget.used().0;
    let held = budget.try_acquire(remaining, false).unwrap();
    assert!(Request::parse(text, payload, &budget).is_none());
    drop(held);
    assert_eq!(budget.used().0, MAX_FRAME_BYTES);
}

#[test]
fn client_request_matches_old_policy_and_keeps_raw_opaque() {
    let budget = crate::transport::PayloadBudget::default();
    let deep = format!("{}0{}", "[".repeat(4096), "]".repeat(4096));
    let long = format!("\"{}\"", "x".repeat(4096));
    let values = [
        r#"["t","s",true,false]"#,
        r#"["t"]"#,
        "null",
        "false",
        "[]",
        "{}",
        "1e400",
        r#""\uD800""#,
        r#"{"targetId":"t","autoAttach":true}"#,
        r#"{"targetId":"a","targetId":"b","autoAttach":true}"#,
        r#"{"targetId":null,"targetId":"b","autoAttach":true}"#,
        r#"{"targetId":{},"sessionId":7,"autoAttach":"true"}"#,
        r#"{"targetId":"\uD800"}"#,
        r#"{"ignored":"\uD800"}"#,
        r#"{"ignored":1e400}"#,
        r#"{"autoAttach":1e400}"#,
        r#"{"targetId":{"$serde_json::private::RawValue":"1"}}"#,
        r#"{"targetId":{"$serde_json::private::RawValue":1}}"#,
        r#"{"targetId":{"$serde_json::private::Number":"1"}}"#,
        r#"{"targ\u0065tId":"t","discover":true}"#,
        &deep,
        &long,
    ];
    let ids = [
        "1",
        "-0",
        "1.0",
        "1e0",
        "9007199254740991",
        "9007199254740992",
        "-9007199254740991",
        "18446744073709551615",
        "1e400",
        "1e-400",
        "null",
    ];
    let mut count = 0;
    for params in values {
        for id in ids {
            let text = format!(
                r#"{{"id":{id},"method":"Runtime.evaluate","params":{params},"extra":{{"ignored":true}}}}"#
            );
            compare_old_request(&text, &budget);
            count += 1;
        }
    }
    for text in [
        r#"[1,"x",null,{}]"#,
        r#"[1,"x"]"#,
        r#"{"id":1,"id":2,"method":"x"}"#,
        r#"{"id":null,"id":2,"method":"x"}"#,
        r#"{"method":null,"method":"x"}"#,
        r#"{"params":null,"params":{}}"#,
        r#"{"id":1,"method":"x","meth\u006fd":"y"}"#,
        r#"{"id":1,"method":"\uD800"}"#,
        r#"{"id":1,"method":"x","params":[1,]}"#,
    ] {
        compare_old_request(text, &budget);
        count += 1;
    }
    for size in [4094, 4095, 4096] {
        let token = serde_json::to_string(&"x".repeat(size)).unwrap();
        for key in ["method", "sessionId"] {
            let text = format!(r#"{{"id":1,"{key}":{token}}}"#);
            compare_old_request(&text, &budget);
            count += 1;
        }
        let text = format!(r#"{{"id":1,"method":"x","params":{{"targetId":{token}}}}}"#);
        compare_old_request(&text, &budget);
        count += 1;
    }
    for payload in [r#""\"decoded\"""#, "true", "1e400", r#""\uD800""#] {
        let wrapped = json!({"$serde_json::private::RawValue": payload}).to_string();
        let text = format!(
            r#"{{"id":1,"method":"x","params":{{"targetId":{wrapped},"autoAttach":{wrapped}}}}}"#
        );
        compare_old_request(&text, &budget);
        count += 1;
    }
    assert_eq!(budget.used().0, crate::transport::MAX_FRAME_BYTES);
    println!("client production Request old-policy cases={count}");
}

fn compare_old_request(text: &str, budget: &crate::transport::PayloadBudget) {
    use super::Request;
    let old = old_request(text);
    let owned = text.to_string();
    let lease = budget.try_acquire(owned.capacity(), false).unwrap();
    let new = Request::parse(owned, lease, budget);
    if let Some(request) = &new {
        if request.observed.fallback {
            assert!(request.observed.string("targetId").is_none());
            assert!(request.observed.auto_attach.is_none());
            assert!(request.observed.discover.is_none());
        }
    }
    assert_eq!(new.is_some(), old.is_some(), "{text}");
    if let (Some(new), Some(old)) = (new, old) {
        assert_eq!(
            new.id,
            old.get("id").and_then(super::safe_integer),
            "{text}"
        );
        assert_eq!(
            new.method(),
            old.get("method").and_then(Value::as_str),
            "{text}"
        );
        assert_eq!(
            new.session(),
            old.get("sessionId").and_then(Value::as_str),
            "{text}"
        );
        assert_eq!(
            new.observed.string("targetId"),
            old.get("targetId").and_then(Value::as_str),
            "{text}"
        );
        assert_eq!(
            new.observed.string("sessionId"),
            old.get("paramSession").and_then(Value::as_str),
            "{text}"
        );
        assert_eq!(
            new.observed.auto_attach,
            old.get("autoAttach").and_then(Value::as_bool),
            "{text}"
        );
        assert_eq!(
            new.observed.discover,
            old.get("discover").and_then(Value::as_bool),
            "{text}"
        );
        let mut scratch = budget.json_scratch();
        if let Some(raw) = new.raw_params(&mut scratch) {
            assert!(text.contains(raw.get()));
        }
    }
}

mod request_allocations {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::RefCell;

    #[derive(Clone, Copy, Default, Debug)]
    struct Stats {
        live: usize,
        peak: usize,
        largest: usize,
        count: usize,
        overflow: bool,
    }
    struct Tracking {
        active: bool,
        stats: Stats,
        slots: [(usize, usize); 1024],
    }
    thread_local! {
        static TRACK: RefCell<Tracking> = const { RefCell::new(Tracking {
            active: false, stats: Stats { live: 0, peak: 0, largest: 0, count: 0, overflow: false },
            slots: [(0, 0); 1024],
        }) };
    }
    impl Tracking {
        fn add(&mut self, ptr: *mut u8, size: usize) {
            if !self.active || ptr.is_null() {
                return;
            }
            self.stats.live += size;
            self.stats.peak = self.stats.peak.max(self.stats.live);
            self.stats.largest = self.stats.largest.max(size);
            self.stats.count += 1;
            if let Some(slot) = self.slots.iter_mut().find(|slot| slot.0 == 0) {
                *slot = (ptr as usize, size);
            } else {
                self.stats.overflow = true;
            }
        }
        fn remove(&mut self, ptr: *mut u8) {
            if let Some(slot) = self.slots.iter_mut().find(|slot| slot.0 == ptr as usize) {
                self.stats.live -= slot.1;
                *slot = (0, 0);
            }
        }
    }
    struct Allocator;
    // 仅测试按线程记录全部大小；realloc 以旧新并存计峰，槽溢出使测试失败。
    unsafe impl GlobalAlloc for Allocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let ptr = System.alloc(layout);
            let _ = TRACK.try_with(|state| state.borrow_mut().add(ptr, layout.size()));
            ptr
        }
        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            let _ = TRACK.try_with(|state| state.borrow_mut().remove(ptr));
            System.dealloc(ptr, layout);
        }
        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
            let new = System.realloc(ptr, layout, size);
            if !new.is_null() {
                let _ = TRACK.try_with(|state| {
                    let mut state = state.borrow_mut();
                    if state.active {
                        state.stats.peak = state.stats.peak.max(state.stats.live + size);
                    }
                    state.remove(ptr);
                    state.add(new, size);
                });
            }
            new
        }
    }
    #[global_allocator]
    static ALLOCATOR: Allocator = Allocator;

    #[test]
    fn production_request_has_no_params_tree_or_copy() {
        use crate::transport::{PayloadBudget, MAX_FRAME_BYTES};
        use super::super::Request;
        let budget = PayloadBudget::default();
        drop(budget.json_scratch());
        drop(budget.try_acquire(64 * 1024, false).unwrap());
        let escaped = format!(r#"{{"data":"{}"}}"#, "\\u0061".repeat(500_000));
        let objects = format!("[{}0]", "{\"x\":[1,2]},".repeat(100_000));
        let keys = format!(r#"{{"{}":0}}"#, "\\u0061".repeat(500_000));
        let fields = format!(r#"{{{}"last":0}}"#, "\"x\":{},".repeat(100_000));
        for (name, params) in [
            ("escape", escaped),
            ("objects", objects),
            ("key", keys),
            ("fields", fields),
        ] {
            let text = format!(r#"{{"id":1,"method":"Target.attachToTarget","params":{params}}}"#);
            let pointer = text.as_ptr();
            let lease = budget.try_acquire(text.capacity(), false).unwrap();
            TRACK.with(|track| {
                let mut track = track.borrow_mut();
                track.stats = Stats::default();
                track.slots.fill((0, 0));
                track.active = true;
            });
            let mut request = Request::parse(text, lease, &budget).unwrap();
            assert_eq!(
                request.raw.as_ref().unwrap().backing().as_str().as_ptr(),
                pointer
            );
            let observed = request.observed.clone();
            request.release_raw();
            drop(request);
            drop(observed);
            let stats = TRACK.with(|track| {
                let mut track = track.borrow_mut();
                track.active = false;
                track.stats
            });
            println!("production Request {name}: {stats:?}");
            assert!(!stats.overflow);
            assert_eq!(stats.live, 0);
            assert!(stats.peak < 64 * 1024, "{name}: {stats:?}");
            assert_eq!(budget.used().0, MAX_FRAME_BYTES);
        }
    }
}

const WAIT: Duration = Duration::from_secs(2);
const QUIET: Duration = Duration::from_millis(200);

#[tokio::test]
async fn client_dispatch_preserves_opaque_params_and_drops_extra_fields() {
    use serde_json::value::RawValue;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (captured, mut frames) = mpsc::channel(4);
    let (stop, mut stopped) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        loop {
            tokio::select! {
                _ = &mut stopped => return,
                frame = socket.next() => {
                    let Some(Ok(Message::Text(text))) = frame else { return };
                    captured.send(text.to_string()).await.unwrap();
                }
            }
        }
    });
    let observer = Arc::new(Observer::disabled());
    let upstream = Upstream::connect(&UpstreamAddress::Tcp(address), observer.clone())
        .await
        .unwrap();
    let profile = Arc::new(ProfileService::new(Arc::new(
        MemoryProfile::new("http://profile.invalid").unwrap(),
    )));
    let broker = BrokerHandle::start(upstream, profile, observer);
    let client = connect(&broker, Endpoint::Browser, Role::Controller, "raw-owner").await;
    let deep = format!("{}0{}", "[".repeat(4096), "]".repeat(4096));
    for (index, params) in [
        r#"{ "unknown":1e400,"s":"\uD800","x":1,"x":2 }"#,
        &deep,
        r#"{"targetId":"t","padding":{"a":1,"a":2}}"#,
    ]
    .iter()
    .enumerate()
    {
        let method = if index == 2 {
            "Target.attachToTarget"
        } else {
            "Runtime.evaluate"
        };
        let text = format!(
            r#"{{"id":{}e0,"method":"{method}","params":{params},"extra":"not-forwarded"}}"#,
            9000 + index
        );
        send(&client, &broker, &text).await;
        let outbound = timeout(WAIT, frames.recv()).await.unwrap().unwrap();
        #[derive(serde::Deserialize)]
        struct Wire<'a> {
            id: u64,
            method: String,
            #[serde(borrow)]
            params: &'a RawValue,
        }
        let wire: Wire<'_> = serde_json::from_str(&outbound).unwrap();
        assert_ne!(wire.id, 9000 + index as u64);
        assert_eq!(wire.method, method);
        assert_eq!(wire.params.get(), *params);
        assert!(!outbound.contains("not-forwarded"));
        assert!(!outbound.contains("sessionId"));
    }
    stop.send(()).unwrap();
    server.await.unwrap();
    assert!(!broker.wait_terminal().await);
}

struct FakeEngine {
    address: SocketAddr,
    send: mpsc::UnboundedSender<Value>,
    receive: mpsc::UnboundedReceiver<Value>,
    kill: mpsc::UnboundedSender<()>,
}

impl FakeEngine {
    fn reply(&self, id: u64, result: Value) {
        self.push(json!({ "id": id, "result": result }));
    }

    fn event(&self, value: Value) {
        self.push(value);
    }

    fn push(&self, value: Value) {
        self.send.send(value).expect("fake engine outbound");
    }

    fn kill(&self) {
        let _ = self.kill.send(());
    }
}

async fn fake_engine() -> FakeEngine {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fake engine");
    let address = listener.local_addr().expect("fake engine address");
    let (send, mut outbound) = mpsc::unbounded_channel::<Value>();
    let (inbound, receive) = mpsc::unbounded_channel::<Value>();
    let (kill, mut killed) = mpsc::unbounded_channel::<()>();
    tokio::spawn(async move {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        let Ok(socket) = tokio_tungstenite::accept_async(stream).await else {
            return;
        };
        serve_fake(socket, &mut outbound, &inbound, &mut killed).await;
    });
    FakeEngine {
        address,
        send,
        receive,
        kill,
    }
}

async fn serve_fake(
    socket: tokio_tungstenite::WebSocketStream<TcpStream>,
    outbound: &mut mpsc::UnboundedReceiver<Value>,
    inbound: &mpsc::UnboundedSender<Value>,
    killed: &mut mpsc::UnboundedReceiver<()>,
) {
    let (mut sink, mut source) = socket.split();
    loop {
        tokio::select! {
          _ = killed.recv() => return,
          Some(value) = outbound.recv() => {
            let text = value.to_string();
            if sink.send(Message::Text(text.into())).await.is_err() {
              return;
            }
          }
          message = source.next() => match message {
            Some(Ok(Message::Text(text))) => {
              let Ok(value) = serde_json::from_str::<Value>(&text) else { continue };
              if inbound.send(value).is_err() {
                return;
              }
            }
            Some(Ok(_)) => {}
            _ => return,
          }
        }
    }
}

struct TestClient {
    id: ClientId,
    frames: mpsc::Receiver<ClientCommand>,
    cancel: watch::Receiver<bool>,
}

async fn broker(engine: &FakeEngine) -> BrokerHandle {
    let observer = Arc::new(Observer::disabled());
    let upstream = Upstream::connect(&UpstreamAddress::Tcp(engine.address), observer.clone())
        .await
        .expect("connect fake upstream");
    let profile = Arc::new(ProfileService::new(Arc::new(
        MemoryProfile::new("http://profile.invalid").expect("memory profile"),
    )));
    BrokerHandle::start(upstream, profile, observer)
}

async fn broker_with_deadlines(
    engine: &FakeEngine,
    client: Duration,
    internal: Duration,
) -> BrokerHandle {
    let observer = Arc::new(Observer::disabled());
    let upstream = Upstream::connect(&UpstreamAddress::Tcp(engine.address), observer.clone())
        .await
        .expect("connect fake upstream");
    let profile = Arc::new(ProfileService::new(Arc::new(
        MemoryProfile::new("http://profile.invalid").expect("memory profile"),
    )));
    BrokerHandle::start_with_deadlines(upstream, profile, observer, client, internal)
}

async fn connect(broker: &BrokerHandle, endpoint: Endpoint, role: Role, owner: &str) -> TestClient {
    let reservation = broker
        .reserve(ReserveRequest {
            endpoint,
            role,
            owner: owner.to_string(),
        })
        .await
        .expect("reserve client");
    let id = reservation.id();
    let (writer, frames) = mpsc::channel(64);
    let (cancel, cancelled) = watch::channel(false);
    assert!(broker.commit(reservation, writer, cancel).await, "commit");
    TestClient {
        id,
        frames,
        cancel: cancelled,
    }
}

async fn send(client: &TestClient, broker: &BrokerHandle, text: &str) {
    assert!(broker.frame(client.id, text.to_string()).await);
}

async fn next_frame(client: &mut TestClient) -> Value {
    loop {
        let command = timeout(WAIT, client.frames.recv())
            .await
            .expect("client frame")
            .expect("client open");
        if let ClientCommand::Text(text, _lease) = command {
            return serde_json::from_str(&text).expect("client json");
        }
    }
}

async fn next_request(engine: &mut FakeEngine) -> Value {
    timeout(WAIT, engine.receive.recv())
        .await
        .expect("upstream request")
        .expect("upstream open")
}

async fn next_method(engine: &mut FakeEngine, method: &str) -> Value {
    loop {
        let value = next_request(engine).await;
        if value["method"] == method {
            return value;
        }
        engine.reply(request_id(&value), json!({}));
    }
}

async fn next_close(client: &mut TestClient) -> (u16, String) {
    loop {
        let command = timeout(WAIT, client.frames.recv())
            .await
            .expect("client close")
            .expect("client open");
        if let ClientCommand::Close { code, reason } = command {
            return (code, reason);
        }
    }
}

async fn expect_no_request(engine: &mut FakeEngine) {
    if let Ok(Some(value)) = timeout(QUIET, engine.receive.recv()).await {
        panic!("unexpected upstream request: {value}");
    }
}

async fn expect_silence(client: &mut TestClient) {
    if timeout(QUIET, client.frames.recv()).await.is_ok() {
        panic!("unexpected client frame");
    }
}

fn request_id(value: &Value) -> u64 {
    value.get("id").and_then(Value::as_u64).expect("request id")
}

fn message(value: &Value) -> &str {
    value
        .get("error")
        .and_then(|error| error.get("message"))
        .and_then(Value::as_str)
        .expect("error message")
}

#[test]
fn production_request_and_output_limits_stay_fixed() {
    assert_eq!(super::MAX_PENDING_PER_CLIENT, 64);
    assert_eq!(super::MAX_PENDING_GLOBAL, 512);
    assert_eq!(super::MAX_INTERNAL_PENDING, 32);
    assert_eq!(super::CLIENT_QUEUE_FRAMES, 64);
    assert_eq!(super::CLIENT_QUEUE_BYTES, 32 * 1024 * 1024);
}

#[tokio::test]
async fn page_waiting_rejects_unauthorized_requests_before_slot_admission() {
    let mut engine = fake_engine().await;
    let broker = broker(&engine).await;
    let mut page = connect(
        &broker,
        Endpoint::Page {
            target_id: "page".into(),
        },
        Role::Viewer,
        "viewer",
    )
    .await;
    let attach = next_request(&mut engine).await;
    for text in [
        r#"{"id":1,"method":"Runtime.evaluate","params":{"expression":"1"}}"#,
        r#"{"id":1,"method":"Page.enable","sessionId":"foreign"}"#,
        r#"{"id":1,"method":"Target.detachFromTarget","params":{"sessionId":"foreign"}}"#,
    ] {
        send(&page, &broker, text).await;
        let rejected = next_frame(&mut page).await;
        assert_eq!(rejected["id"], 1);
        assert!(rejected.get("error").is_some());
        assert_eq!(broker.budget_snapshot().await.0, 0);
    }
    send(&page, &broker, r#"{"id":1,"method":"Page.enable"}"#).await;
    send(&page, &broker, r#"{"id":1,"method":"Page.disable"}"#).await;
    assert!(message(&next_frame(&mut page).await).contains("Duplicate"));
    assert_eq!(broker.budget_snapshot().await.0, 1);
    engine.reply(request_id(&attach), json!({"sessionId":"primary"}));
    let dispatched = next_request(&mut engine).await;
    assert_eq!(dispatched["method"], "Page.enable");
    engine.reply(request_id(&dispatched), json!({}));
    assert!(next_frame(&mut page).await.get("result").is_some());
    assert_eq!(broker.budget_snapshot().await.0, 0);
    expect_no_request(&mut engine).await;
    engine.kill();
    assert!(!broker.wait_terminal().await);
}

#[tokio::test]
async fn active_and_queued_management_share_the_64_request_limit() {
    let mut engine = fake_engine().await;
    let broker = broker(&engine).await;
    let mut client = connect(&broker, Endpoint::Browser, Role::Viewer, "viewer").await;
    send(
        &client,
        &broker,
        r#"{"id":1,"method":"Target.attachToTarget","params":{"targetId":"held","flatten":true}}"#,
    )
    .await;
    let first = next_request(&mut engine).await;
    for id in 2..=65 {
        send(&client, &broker, &json!({"id":id,"method":"Target.attachToTarget","params":{"targetId":"queued","flatten":true}}).to_string()).await;
    }
    // 后续普通请求是 actor 的有序 ingress 屏障；不能用 sleep 推断队列已经处理。
    send(
        &client,
        &broker,
        r#"{"id":66,"method":"Browser.getVersion"}"#,
    )
    .await;
    let rejected = next_frame(&mut client).await;
    assert_eq!(rejected["id"], 65, "active + queued must reject request 65");
    assert!(rejected.get("error").is_some());
    engine.reply(request_id(&first), json!({"sessionId":"held-session"}));
    engine.kill();
    assert!(!broker.wait_terminal().await);
}

#[tokio::test]
async fn management_payloads_cannot_retain_18_times_8_mib() {
    let mut engine = fake_engine().await;
    let broker = broker(&engine).await;
    let client = connect(&broker, Endpoint::Browser, Role::Viewer, "viewer").await;
    let mut cancelled = client.cancel.clone();
    send(
        &client,
        &broker,
        r#"{"id":1,"method":"Target.attachToTarget","params":{"targetId":"held","flatten":true}}"#,
    )
    .await;
    let _first = next_request(&mut engine).await;
    let padding = "x".repeat(8 * 1024 * 1024);
    let mut accepted = 0;
    for id in 2..=19 {
        let text = json!({"id":id,"method":"Target.attachToTarget","params":{"targetId":"queued","padding":padding}}).to_string();
        if !broker.frame(client.id, text).await {
            break;
        }
        // 同一 ingress 的小请求在大帧之后；收到其 reply 才统计已处理的大帧。
        send(
            &client,
            &broker,
            &json!({"id":100+id,"method":"Target.setDiscoverTargets"}).to_string(),
        )
        .await;
        let barrier = timeout(WAIT, async {
            tokio::select! {
                request = next_method(&mut engine, "Target.setDiscoverTargets") => Some(request),
                _ = cancelled.wait_for(|closed| *closed) => None,
            }
        })
        .await
        .expect("budget pressure must reply or cancel promptly");
        let Some(barrier) = barrier else {
            assert!(*client.cancel.borrow());
            assert!(broker.session().await.unwrap().alive);
            break;
        };
        engine.reply(request_id(&barrier), json!({}));
        if *client.cancel.borrow() {
            break;
        }
        accepted += 1;
    }
    engine.kill();
    assert!(!broker.wait_terminal().await);
    let (ordinary, internal) = broker.payload_budget().high_water();
    println!("management accepted={accepted}/18 ordinary_high={ordinary} internal_high={internal}; not an RSS limit");
    assert!(ordinary <= 128 * 1024 * 1024);
    assert!(internal <= 20 * 1024 * 1024);
    assert!(accepted < 18, "all 144 MiB management params were retained");
}

#[tokio::test]
async fn global_request_slots_include_queued_and_unknown_and_release_on_late_reply() {
    let mut engine = fake_engine().await;
    let broker =
        broker_with_deadlines(&engine, Duration::from_millis(100), Duration::from_secs(30)).await;
    let mut clients = Vec::new();
    for index in 0..9 {
        let endpoint = if index == 1 {
            Endpoint::Page {
                target_id: "page".into(),
            }
        } else {
            Endpoint::Browser
        };
        clients.push(connect(&broker, endpoint, Role::Viewer, &format!("viewer-{index}")).await);
    }
    let _page_attach = next_method(&mut engine, "Target.attachToTarget").await;
    for id in 1..=64 {
        send(
            &clients[0],
            &broker,
            &json!({"id":id,"method":"Target.attachToTarget","params":{"targetId":"held"}})
                .to_string(),
        )
        .await;
        if id == 1 {
            let _active = next_method(&mut engine, "Target.attachToTarget").await;
        }
        send(
            &clients[1],
            &broker,
            &json!({"id":id,"method":"Page.enable"}).to_string(),
        )
        .await;
    }
    // 1 active + 63 queued management、64 page queued、384 normal 共用 512 槽。
    let mut held = Vec::new();
    for client in clients.iter().skip(2).take(6) {
        for id in 1..=64 {
            send(
                client,
                &broker,
                &json!({"id":id,"method":"Browser.getVersion"}).to_string(),
            )
            .await;
            held.push(request_id(&next_request(&mut engine).await));
        }
    }
    let snapshot = broker.budget_snapshot().await;
    assert_eq!(snapshot.0, 512);
    assert_eq!(snapshot.1, 63);
    assert_eq!(snapshot.3, 1);
    send(
        &clients[8],
        &broker,
        r#"{"id":1,"method":"Browser.getVersion"}"#,
    )
    .await;
    let rejected = next_frame(&mut clients[8]).await;
    assert_eq!(rejected["id"], 1);
    assert!(message(&rejected).contains("Too many"));
    // 等待第一个真实 timeout 回复，不通过 sleep 推断 unknown 状态。
    let timed_out = next_frame(&mut clients[2]).await;
    assert!(message(&timed_out).contains("outcome may be unknown"));
    assert_eq!(broker.budget_snapshot().await.0, 512);
    engine.reply(held[0], json!({}));
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        if broker.budget_snapshot().await.0 == 511 {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::task::yield_now().await;
    }
    send(
        &clients[8],
        &broker,
        r#"{"id":2,"method":"Browser.getVersion"}"#,
    )
    .await;
    let admitted = next_request(&mut engine).await;
    assert_eq!(admitted["method"], "Browser.getVersion");
    engine.kill();
    assert!(!broker.wait_terminal().await);
}

#[tokio::test]
async fn writer_held_frames_keep_the_same_output_slots_until_drop() {
    let mut engine = fake_engine().await;
    let broker = broker(&engine).await;
    let mut client = connect(&broker, Endpoint::Browser, Role::Viewer, "viewer").await;
    let mut held = Vec::new();
    for id in 1..=64 {
        send(
            &client,
            &broker,
            &json!({"id":id,"method":"Browser.getVersion"}).to_string(),
        )
        .await;
        let request = next_request(&mut engine).await;
        engine.reply(request_id(&request), json!({"product":"held"}));
        held.push(timeout(WAIT, client.frames.recv()).await.unwrap().unwrap());
    }
    // mpsc 已空，但 writer-held 的 64 帧仍占槽。
    send(
        &client,
        &broker,
        r#"{"id":65,"method":"Browser.getVersion"}"#,
    )
    .await;
    let request = next_request(&mut engine).await;
    engine.reply(request_id(&request), json!({}));
    assert_eq!(next_close(&mut client).await.1, "Slow client");
    let before = broker.payload_budget().used().0;
    drop(held);
    assert!(broker.payload_budget().used().0 < before);
    engine.kill();
    assert!(!broker.wait_terminal().await);
}

#[tokio::test]
async fn management_unknown_keeps_one_logical_slot_and_not_the_large_params() {
    let mut engine = fake_engine().await;
    let broker =
        broker_with_deadlines(&engine, Duration::from_secs(30), Duration::from_millis(100)).await;
    let mut client = connect(&broker, Endpoint::Browser, Role::Viewer, "viewer").await;
    let text = json!({"id":1,"method":"Target.attachToTarget","params":{"targetId":"held","padding":"x".repeat(1024*1024)}}).to_string();
    send(&client, &broker, &text).await;
    let first = next_request(&mut engine).await;
    let timed_out = next_frame(&mut client).await;
    assert!(message(&timed_out).contains("outcome may be unknown"));
    assert_eq!(broker.budget_snapshot().await.0, 1);
    assert_eq!(broker.budget_snapshot().await.2, 1);
    assert!(broker.payload_budget().used().0 < 17 * 1024 * 1024);
    send(
        &client,
        &broker,
        r#"{"id":1,"method":"Browser.getVersion"}"#,
    )
    .await;
    assert!(message(&next_frame(&mut client).await).contains("Duplicate"));
    engine.reply(request_id(&first), json!({"sessionId":"late"}));
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        if broker.budget_snapshot().await.0 == 0 {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::task::yield_now().await;
    }
    engine.kill();
    assert!(!broker.wait_terminal().await);
}

#[tokio::test]
async fn page_queue_and_management_share_admission_and_cancel_releases_queued_leases() {
    let mut engine = fake_engine().await;
    let broker = broker(&engine).await;
    let mut page = connect(
        &broker,
        Endpoint::Page {
            target_id: "page".into(),
        },
        Role::Viewer,
        "viewer",
    )
    .await;
    let attach = next_request(&mut engine).await;
    for id in 1..=65 {
        send(
            &page,
            &broker,
            &json!({"id":id,"method":"Page.enable"}).to_string(),
        )
        .await;
    }
    // page queue 未 ready 时第 65 条同样在准入点拒绝。
    assert_eq!(next_frame(&mut page).await["id"], 65);
    assert_eq!(broker.budget_snapshot().await.0, 64);
    broker.closed(page.id);
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        if broker.budget_snapshot().await.0 == 0 {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::task::yield_now().await;
    }
    // 内部 attach 尚未完成，但外部 queued lease 已释放，不改变后继 client retirement。
    engine.reply(request_id(&attach), json!({"sessionId":"late-page"}));
    engine.kill();
    assert!(!broker.wait_terminal().await);
}

#[tokio::test]
async fn exhausted_payload_budget_still_accepts_status_disconnect_and_cleanup_reply() {
    let mut engine = fake_engine().await;
    let broker = broker(&engine).await;
    let client = connect(&broker, Endpoint::Browser, Role::Viewer, "viewer").await;
    send(
        &client,
        &broker,
        r#"{"id":1,"method":"Browser.getVersion"}"#,
    )
    .await;
    let request = next_request(&mut engine).await;
    let budget = broker.payload_budget();
    let free = 128 * 1024 * 1024 - budget.used().0;
    let occupied = budget.try_acquire(free, false).unwrap();
    assert!(broker.session().await.unwrap().alive);
    engine.reply(request_id(&request), json!({}));
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        if broker.budget_snapshot().await.0 == 0 {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::task::yield_now().await;
    }
    broker.closed(client.id);
    assert!(broker.session().await.unwrap().alive);
    drop(occupied);
    engine.kill();
    assert!(!broker.wait_terminal().await);
}

#[tokio::test]
async fn autoattach_backfill_inherits_one_external_logical_slot() {
    let mut engine = fake_engine().await;
    let broker = broker(&engine).await;
    let mut client = connect(&broker, Endpoint::Browser, Role::Controller, "owner").await;
    send(
        &client,
        &broker,
        r#"{"id":1,"method":"Target.setAutoAttach","params":{"autoAttach":true,"flatten":true}}"#,
    )
    .await;
    let auto = next_request(&mut engine).await;
    assert_eq!(broker.budget_snapshot().await.0, 1);
    engine.reply(request_id(&auto), json!({}));
    let targets = next_request(&mut engine).await;
    assert_eq!(targets["method"], "Target.getTargets");
    assert_eq!(broker.budget_snapshot().await.0, 1);
    engine.reply(
        request_id(&targets),
        json!({"targetInfos":[{"targetId":"one","type":"page"},{"targetId":"two","type":"page"}]}),
    );
    for index in 0..2 {
        let attach = next_request(&mut engine).await;
        assert_eq!(attach["method"], "Target.attachToTarget");
        assert_eq!(broker.budget_snapshot().await.0, 1);
        engine.reply(
            request_id(&attach),
            json!({"sessionId":format!("session-{index}")}),
        );
    }
    assert_eq!(next_frame(&mut client).await["id"], 1);
    assert_eq!(broker.budget_snapshot().await.0, 0);
    engine.kill();
    assert!(!broker.wait_terminal().await);
}

#[tokio::test]
async fn diagnostic_latch_invalidates_health_before_actor_dispatch() {
    use crate::supervisor::EngineDiagnostic;
    let mut engine = fake_engine().await;
    let (fatal, watch) = watch::channel(None);
    let broker = broker(&engine).await.with_diagnostics(watch);
    assert!(broker.session().await.unwrap().alive);
    fatal.send(Some(EngineDiagnostic::Uncaught)).unwrap();
    assert!(broker.session().await.is_none());
    assert!(!broker.wait_terminal().await);
    expect_no_request(&mut engine).await;
    engine.kill();
}

#[tokio::test]
async fn foreign_session_close_never_dispatches_flush_or_close() {
    let mut engine = fake_engine().await;
    let broker = broker(&engine).await;
    let mut alice = connect(&broker, Endpoint::Browser, Role::Controller, "alice").await;
    let mut viewer = connect(&broker, Endpoint::Browser, Role::Viewer, "viewer").await;
    send(
        &viewer,
        &broker,
        r#"{"id":1,"method":"Target.attachToTarget","params":{"targetId":"page","flatten":true}}"#,
    )
    .await;
    let attach = next_request(&mut engine).await;
    engine.reply(request_id(&attach), json!({"sessionId":"viewer-session"}));
    assert_eq!(
        next_frame(&mut viewer).await["result"]["sessionId"],
        "viewer-session"
    );
    send(
        &alice,
        &broker,
        r#"{"id":2,"method":"Browser.close","sessionId":"viewer-session"}"#,
    )
    .await;
    assert!(message(&next_frame(&mut alice).await).contains("belongs to another connection"));
    expect_no_request(&mut engine).await;
    for client in [&mut alice, &mut viewer] {
        send(client, &broker, r#"{"id":3,"method":"Browser.getVersion"}"#).await;
        let version = next_request(&mut engine).await;
        assert_eq!(version["method"], "Browser.getVersion");
        engine.reply(request_id(&version), json!({"product":"alive"}));
        assert_eq!(next_frame(client).await["result"]["product"], "alive");
    }
    engine.kill();
}

#[tokio::test]
async fn close_errors_are_redacted_and_retryable_http_and_cdp() {
    for cdp in [false, true] {
        let mut engine = fake_engine().await;
        let broker = broker(&engine).await;
        let mut alice = connect(&broker, Endpoint::Browser, Role::Controller, "alice").await;
        for stage in ["Kitesurf.flushProfile", "Browser.close"] {
            let closing = if cdp {
                send(&alice, &broker, r#"{"id":1,"method":"Browser.close"}"#).await;
                None
            } else {
                let broker = broker.clone();
                Some(tokio::spawn(async move {
                    broker.close(CloseKind::Http, None, None, None).await
                }))
            };
            let flush = next_request(&mut engine).await;
            assert_eq!(flush["method"], "Kitesurf.flushProfile");
            let request = if stage == "Browser.close" {
                engine.reply(request_id(&flush), json!({}));
                next_request(&mut engine).await
            } else {
                flush
            };
            engine.push(json!({"id":request_id(&request),"error":{"code":-32000,"message":"synthetic-close-sensitive-data"}}));
            let expected = if stage == "Browser.close" {
                "Browser close failed"
            } else {
                "Profile flush failed"
            };
            if let Some(closing) = closing {
                assert_eq!(closing.await.unwrap().unwrap_err(), expected);
            } else {
                assert_eq!(message(&next_frame(&mut alice).await), expected);
            }
            send(&alice, &broker, r#"{"id":2,"method":"Browser.getVersion"}"#).await;
            let version = next_request(&mut engine).await;
            engine.push(json!({"id":request_id(&version),"error":{"code":-32001,"message":"ordinary protocol error"}}));
            assert_eq!(
                message(&next_frame(&mut alice).await),
                "ordinary protocol error"
            );
        }
        let closing = if cdp {
            send(&alice, &broker, r#"{"id":3,"method":"Browser.close"}"#).await;
            None
        } else {
            let broker = broker.clone();
            Some(tokio::spawn(async move {
                broker.close(CloseKind::Http, None, None, None).await
            }))
        };
        let flush = next_request(&mut engine).await;
        engine.reply(request_id(&flush), json!({}));
        let close = next_request(&mut engine).await;
        assert_eq!(close["method"], "Browser.close");
        engine.reply(request_id(&close), json!({}));
        if let Some(closing) = closing {
            assert!(closing.await.unwrap().is_ok());
        } else {
            assert_eq!(next_frame(&mut alice).await["result"], json!({}));
        }
        broker.writer_gone(alice.id);
        assert!(broker.wait_terminal().await);
        engine.kill();
    }
}

#[tokio::test]
async fn http_close_redacts_sensitive_upstream_failure_and_retries() {
    use crate::relay::{HostServer, SessionInfo};
    let mut engine = fake_engine().await;
    let broker = broker(&engine).await;
    let profile = Arc::new(ProfileService::new(Arc::new(
        MemoryProfile::new("http://profile.invalid").unwrap(),
    )));
    let host = HostServer::bind(
        0,
        broker.clone(),
        SessionInfo {
            session_id: "fake-session".into(),
            build_version: "test".into(),
            environment: "local".into(),
        },
        "generation".into(),
        "new-profile".into(),
        profile,
        Arc::new(Observer::disabled()),
    )
    .unwrap();
    let mut alice = connect(&broker, Endpoint::Browser, Role::Controller, "alice").await;
    let port = host.port();
    let closing = tokio::spawn(http_close(port));
    let flush = next_request(&mut engine).await;
    engine.reply(request_id(&flush), json!({}));
    let close = next_request(&mut engine).await;
    engine.push(json!({"id":request_id(&close),"error":{"code":-32000,"message":"synthetic-close-sensitive-data"}}));
    let failed = closing.await.unwrap();
    assert!(failed.starts_with("HTTP/1.1 409"), "{failed}");
    assert!(failed.contains("Browser close failed"));
    assert!(!failed.contains("synthetic-close-sensitive-data"));
    assert!(broker.session().await.unwrap().alive);
    send(&alice, &broker, r#"{"id":2,"method":"Browser.getVersion"}"#).await;
    let version = next_request(&mut engine).await;
    engine.reply(request_id(&version), json!({"product":"alive"}));
    assert_eq!(next_frame(&mut alice).await["result"]["product"], "alive");
    let closing = tokio::spawn(http_close(port));
    let flush = next_request(&mut engine).await;
    engine.reply(request_id(&flush), json!({}));
    let close = next_request(&mut engine).await;
    engine.reply(request_id(&close), json!({}));
    let success = closing.await.unwrap();
    assert!(success.starts_with("HTTP/1.1 200"), "{success}");
    broker.writer_gone(alice.id);
    assert!(broker.wait_terminal().await);
    host.stop().await;
    engine.kill();
}

async fn http_close(port: u16) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    stream.write_all(format!("POST /close HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
    let mut bytes = Vec::new();
    timeout(WAIT, stream.read_to_end(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    String::from_utf8(bytes).unwrap()
}

#[tokio::test]
async fn cross_client_same_external_id_stays_isolated() {
    let mut engine = fake_engine().await;
    let broker = broker(&engine).await;
    let mut alice = connect(&broker, Endpoint::Browser, Role::Controller, "alice").await;
    let mut bob = connect(&broker, Endpoint::Browser, Role::Viewer, "bob").await;
    send(&alice, &broker, r#"{"id":1,"method":"Target.getTargets"}"#).await;
    send(&bob, &broker, r#"{"id":1,"method":"Target.getTargets"}"#).await;
    let first = next_request(&mut engine).await;
    let second = next_request(&mut engine).await;
    assert_ne!(request_id(&first), request_id(&second));
    assert_eq!(first["method"], "Target.getTargets");
    engine.reply(
        request_id(&second),
        json!({ "targetInfos": [{ "targetId": "B" }] }),
    );
    engine.reply(
        request_id(&first),
        json!({ "targetInfos": [{ "targetId": "A" }] }),
    );
    let from_alice = next_frame(&mut alice).await;
    let from_bob = next_frame(&mut bob).await;
    assert_eq!(from_alice["id"], 1);
    assert_eq!(from_alice["result"]["targetInfos"][0]["targetId"], "A");
    assert_eq!(from_bob["id"], 1);
    assert_eq!(from_bob["result"]["targetInfos"][0]["targetId"], "B");
}

#[tokio::test]
async fn duplicate_inflight_id_is_rejected_without_dispatch() {
    let mut engine = fake_engine().await;
    let broker = broker(&engine).await;
    let mut alice = connect(&broker, Endpoint::Browser, Role::Controller, "alice").await;
    send(&alice, &broker, r#"{"id":7,"method":"Browser.getVersion"}"#).await;
    let held = next_request(&mut engine).await;
    send(&alice, &broker, r#"{"id":7,"method":"Browser.getVersion"}"#).await;
    let duplicate = next_frame(&mut alice).await;
    assert_eq!(duplicate["id"], 7);
    assert_eq!(message(&duplicate), "Duplicate in-flight request id");
    expect_no_request(&mut engine).await;
    engine.reply(request_id(&held), json!({ "userAgent": "fake" }));
    let reply = next_frame(&mut alice).await;
    assert_eq!(reply["id"], 7);
    assert_eq!(reply["result"]["userAgent"], "fake");
}

#[tokio::test]
async fn queued_managed_duplicate_id_is_rejected() {
    let mut engine = fake_engine().await;
    let broker = broker(&engine).await;
    let mut alice = connect(&broker, Endpoint::Browser, Role::Controller, "alice").await;
    send(
        &alice,
        &broker,
        r#"{"id":1,"method":"Target.attachToTarget","params":{"targetId":"T1","flatten":true}}"#,
    )
    .await;
    let attach = next_request(&mut engine).await;
    send(
        &alice,
        &broker,
        r#"{"id":9,"method":"Target.setAutoAttach","params":{"autoAttach":true,"flatten":true}}"#,
    )
    .await;
    send(
        &alice,
        &broker,
        r#"{"id":9,"method":"Target.setAutoAttach","params":{"autoAttach":false,"flatten":true}}"#,
    )
    .await;
    let duplicate = next_frame(&mut alice).await;
    assert_eq!(duplicate["id"], 9);
    assert_eq!(message(&duplicate), "Duplicate in-flight request id");
    engine.reply(request_id(&attach), json!({ "sessionId": "S1" }));
    let attached = next_frame(&mut alice).await;
    assert_eq!(attached["result"]["sessionId"], "S1");
    let auto = next_request(&mut engine).await;
    assert_eq!(auto["method"], "Target.setAutoAttach");
    assert_eq!(auto["params"]["autoAttach"], true);
    engine.reply(request_id(&auto), json!({}));
    let targets = next_request(&mut engine).await;
    assert_eq!(targets["method"], "Target.getTargets");
    engine.reply(request_id(&targets), json!({ "targetInfos": [] }));
    let reply = next_frame(&mut alice).await;
    assert_eq!(reply["id"], 9);
    expect_no_request(&mut engine).await;
}

#[tokio::test]
async fn managed_requests_reject_foreign_top_level_session() {
    let mut engine = fake_engine().await;
    let broker = broker(&engine).await;
    let mut alice = connect(&broker, Endpoint::Browser, Role::Controller, "alice").await;
    let mut bob = connect(&broker, Endpoint::Browser, Role::Viewer, "bob").await;
    send(
        &alice,
        &broker,
        r#"{"id":1,"method":"Target.attachToTarget","params":{"targetId":"T1","flatten":true}}"#,
    )
    .await;
    let attach = next_request(&mut engine).await;
    engine.reply(request_id(&attach), json!({ "sessionId": "S1" }));
    let attached = next_frame(&mut alice).await;
    assert_eq!(attached["result"]["sessionId"], "S1");
    send(
        &bob,
        &broker,
        r#"{"id":3,"method":"Target.attachToTarget","params":{"targetId":"T2","flatten":true}}"#,
    )
    .await;
    let viewer_attach = next_request(&mut engine).await;
    engine.reply(request_id(&viewer_attach), json!({ "sessionId": "S2" }));
    let viewer_attached = next_frame(&mut bob).await;
    assert_eq!(viewer_attached["result"]["sessionId"], "S2");
    send(
        &bob,
        &broker,
        r#"{"id":4,"method":"Target.detachFromTarget","params":{"sessionId":"S2"},"sessionId":"S1"}"#,
    )
    .await;
    let foreign = next_frame(&mut bob).await;
    assert_eq!(
        message(&foreign),
        "CDP attachment expired or belongs to another connection; attach again"
    );
    send(
        &alice,
        &broker,
        r#"{"id":5,"method":"Target.setAutoAttach","params":{"autoAttach":true,"flatten":true},"sessionId":"S2"}"#,
    )
    .await;
    let stolen = next_frame(&mut alice).await;
    assert_eq!(
        message(&stolen),
        "CDP attachment expired or belongs to another connection; attach again"
    );
    expect_no_request(&mut engine).await;
}

#[tokio::test]
async fn managed_sessions_are_rejected_at_enqueue_while_dispatch_is_blocked() {
    let mut engine = fake_engine().await;
    let broker = broker(&engine).await;
    let mut alice = connect(&broker, Endpoint::Browser, Role::Controller, "alice").await;
    let mut bob = connect(&broker, Endpoint::Browser, Role::Viewer, "bob").await;
    send(
        &alice,
        &broker,
        r#"{"id":1,"method":"Target.attachToTarget","params":{"targetId":"T1","flatten":true}}"#,
    )
    .await;
    let first = next_request(&mut engine).await;
    engine.reply(request_id(&first), json!({ "sessionId": "S1" }));
    let attached = next_frame(&mut alice).await;
    assert_eq!(attached["result"]["sessionId"], "S1");
    send(
        &alice,
        &broker,
        r#"{"id":2,"method":"Target.attachToTarget","params":{"targetId":"T2","flatten":true}}"#,
    )
    .await;
    let held = next_request(&mut engine).await;
    assert_eq!(held["method"], "Target.attachToTarget");
    // 管理队列被占住时仍立即拒绝，证明校验发生在入队而非派发。
    send(
        &bob,
        &broker,
        r#"{"id":3,"method":"Target.detachFromTarget","params":{"sessionId":"S1"}}"#,
    )
    .await;
    let foreign_param = next_frame(&mut bob).await;
    assert_eq!(foreign_param["id"], 3);
    assert_eq!(
        message(&foreign_param),
        "This connection does not own that attachment"
    );
    send(
        &bob,
        &broker,
        r#"{"id":4,"method":"Target.attachToTarget","params":{"targetId":"T3","flatten":true},"sessionId":"S1"}"#,
    )
    .await;
    let foreign_top = next_frame(&mut bob).await;
    assert_eq!(foreign_top["id"], 4);
    assert_eq!(
        message(&foreign_top),
        "CDP attachment expired or belongs to another connection; attach again"
    );
    expect_no_request(&mut engine).await;
    engine.reply(request_id(&held), json!({ "sessionId": "S2" }));
    let second = next_frame(&mut alice).await;
    assert_eq!(second["result"]["sessionId"], "S2");
    expect_no_request(&mut engine).await;
}

#[tokio::test]
async fn viewer_and_cross_owner_requests_never_reach_upstream() {
    let mut engine = fake_engine().await;
    let broker = broker(&engine).await;
    let mut alice = connect(&broker, Endpoint::Browser, Role::Controller, "alice").await;
    let mut bob = connect(&broker, Endpoint::Browser, Role::Viewer, "bob").await;
    send(
        &bob,
        &broker,
        r#"{"id":1,"method":"Runtime.evaluate","params":{"expression":"1"}}"#,
    )
    .await;
    let denied = next_frame(&mut bob).await;
    assert_eq!(
        message(&denied),
        "Viewer connection cannot control the browser"
    );
    send(
        &alice,
        &broker,
        r#"{"id":2,"method":"Target.attachToTarget","params":{"targetId":"T1","flatten":true}}"#,
    )
    .await;
    let attach = next_request(&mut engine).await;
    engine.reply(request_id(&attach), json!({ "sessionId": "S1" }));
    let attached = next_frame(&mut alice).await;
    assert_eq!(attached["result"]["sessionId"], "S1");
    send(
        &bob,
        &broker,
        r#"{"id":3,"method":"Target.detachFromTarget","params":{"sessionId":"S1"}}"#,
    )
    .await;
    let foreign = next_frame(&mut bob).await;
    assert_eq!(
        message(&foreign),
        "This connection does not own that attachment"
    );
    send(
        &bob,
        &broker,
        r#"{"id":4,"method":"Runtime.evaluate","sessionId":"S1"}"#,
    )
    .await;
    let viewer = next_frame(&mut bob).await;
    assert_eq!(
        message(&viewer),
        "Viewer connection cannot control the browser"
    );
    expect_no_request(&mut engine).await;
    send(
        &alice,
        &broker,
        r#"{"id":5,"method":"Runtime.evaluate","sessionId":"S1"}"#,
    )
    .await;
    let owned = next_request(&mut engine).await;
    assert_eq!(owned["sessionId"], "S1");
    engine.reply(
        request_id(&owned),
        json!({ "result": { "type": "undefined" } }),
    );
    let evaluated = next_frame(&mut alice).await;
    assert_eq!(evaluated["id"], 5);
}

#[tokio::test]
async fn unattributed_events_and_discovery_are_not_broadcast() {
    let mut engine = fake_engine().await;
    let broker = broker(&engine).await;
    let mut alice = connect(&broker, Endpoint::Browser, Role::Controller, "alice").await;
    let mut bob = connect(&broker, Endpoint::Browser, Role::Viewer, "bob").await;
    engine.event(json!({ "method": "Runtime.consoleAPICalled", "params": { "secret": "x" } }));
    engine.event(json!({
      "method": "Target.attachedToTarget",
      "params": { "sessionId": "ghost", "targetInfo": { "targetId": "ghost" } }
    }));
    engine.event(json!({ "sessionId": "missing", "method": "Page.loadEventFired" }));
    engine.event(json!({
      "method": "Target.targetCreated",
      "params": { "targetInfo": { "targetId": "T1", "type": "page" } }
    }));
    expect_silence(&mut alice).await;
    expect_silence(&mut bob).await;
    send(
        &alice,
        &broker,
        r#"{"id":1,"method":"Target.setDiscoverTargets","params":{"discover":true}}"#,
    )
    .await;
    let discover = next_request(&mut engine).await;
    engine.reply(request_id(&discover), json!({}));
    let _ = next_frame(&mut alice).await;
    engine.event(json!({
      "method": "Target.targetCreated",
      "params": { "targetInfo": { "targetId": "T1", "type": "page" } }
    }));
    let created = next_frame(&mut alice).await;
    assert_eq!(created["method"], "Target.targetCreated");
    expect_silence(&mut bob).await;
}

#[tokio::test]
async fn early_attached_event_is_routed_before_the_reply() {
    let mut engine = fake_engine().await;
    let broker = broker(&engine).await;
    let mut alice = connect(&broker, Endpoint::Browser, Role::Controller, "alice").await;
    let mut bob = connect(&broker, Endpoint::Browser, Role::Viewer, "bob").await;
    send(
        &alice,
        &broker,
        r#"{"id":8,"method":"Target.attachToTarget","params":{"targetId":"T2","flatten":true}}"#,
    )
    .await;
    let attach = next_request(&mut engine).await;
    engine.event(json!({
      "method": "Target.attachedToTarget",
      "params": { "sessionId": "S2", "targetInfo": { "targetId": "T2" } }
    }));
    let early = next_frame(&mut alice).await;
    assert_eq!(early["method"], "Target.attachedToTarget");
    engine.reply(request_id(&attach), json!({ "sessionId": "S2" }));
    let reply = next_frame(&mut alice).await;
    assert_eq!(reply["id"], 8);
    engine.event(json!({ "sessionId": "S2", "method": "Page.loadEventFired" }));
    let routed = next_frame(&mut alice).await;
    assert_eq!(routed["method"], "Page.loadEventFired");
    expect_silence(&mut bob).await;
}

#[tokio::test]
async fn takeover_fences_the_old_epoch_until_its_write_settles() {
    let mut engine = fake_engine().await;
    let broker = broker(&engine).await;
    let mut alice = connect(&broker, Endpoint::Browser, Role::Controller, "alice").await;
    send(
        &alice,
        &broker,
        r#"{"id":9,"method":"Page.navigate","params":{"url":"about:blank"}}"#,
    )
    .await;
    let held = next_request(&mut engine).await;
    let mut control = {
        let broker = broker.clone();
        tokio::spawn(async move { broker.control("bob".to_string(), true).await })
    };
    assert!(
        timeout(Duration::from_millis(150), &mut control)
            .await
            .is_err(),
        "takeover must wait for the old write"
    );
    wait_for_controller(&broker, "bob").await;
    send(
        &alice,
        &broker,
        r#"{"id":10,"method":"Page.navigate","params":{"url":"about:blank"}}"#,
    )
    .await;
    let fenced = next_frame(&mut alice).await;
    assert_eq!(
        message(&fenced),
        "Control has changed; reconnect and observe before acting"
    );
    engine.reply(request_id(&held), json!({ "frameId": "F1" }));
    let settled = next_frame(&mut alice).await;
    assert_eq!(settled["id"], 9);
    let takeover = timeout(WAIT, control)
        .await
        .expect("takeover completes")
        .expect("control task");
    let takeover = takeover.ok().expect("takeover ok");
    assert_eq!(takeover, ("bob".to_string(), 2));
    let mut bob = connect(&broker, Endpoint::Browser, Role::Controller, "bob").await;
    send(&bob, &broker, r#"{"id":1,"method":"Target.getTargets"}"#).await;
    let request = next_request(&mut engine).await;
    engine.reply(request_id(&request), json!({ "targetInfos": [] }));
    let reply = next_frame(&mut bob).await;
    assert_eq!(reply["id"], 1);
}

async fn wait_for_controller(broker: &BrokerHandle, owner: &str) {
    for _ in 0..100 {
        if let Some(session) = broker.session().await {
            if session.controller.as_deref() == Some(owner) {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("controller never changed to {owner}");
}

#[tokio::test]
async fn control_conflict_and_same_owner_epoch_bump() {
    let mut engine = fake_engine().await;
    let broker = broker(&engine).await;
    let mut alice = connect(&broker, Endpoint::Browser, Role::Controller, "alice").await;
    let conflict = broker
        .control("bob".to_string(), false)
        .await
        .expect_err("conflict");
    assert!(matches!(&conflict, ControlError::Conflict { .. }));
    assert_eq!(conflict.controller().as_deref(), Some("alice"));
    let (owner, epoch) = broker
        .control("alice".to_string(), false)
        .await
        .ok()
        .expect("same owner");
    assert_eq!(owner, "alice");
    assert_eq!(epoch, 2);
    send(&alice, &broker, r#"{"id":1,"method":"Target.getTargets"}"#).await;
    let fenced = next_frame(&mut alice).await;
    assert_eq!(
        message(&fenced),
        "Control has changed; reconnect and observe before acting"
    );
    expect_no_request(&mut engine).await;
    let mut next = connect(&broker, Endpoint::Browser, Role::Controller, "alice").await;
    send(&next, &broker, r#"{"id":1,"method":"Target.getTargets"}"#).await;
    let request = next_request(&mut engine).await;
    engine.reply(request_id(&request), json!({ "targetInfos": [] }));
    let reply = next_frame(&mut next).await;
    assert_eq!(reply["id"], 1);
}

#[tokio::test]
async fn flush_drains_writes_and_reports_profile_status() {
    let mut engine = fake_engine().await;
    let broker = broker(&engine).await;
    let mut alice = connect(&broker, Endpoint::Browser, Role::Controller, "alice").await;
    send(
        &alice,
        &broker,
        r#"{"id":1,"method":"Page.navigate","params":{"url":"about:blank"}}"#,
    )
    .await;
    let write = next_request(&mut engine).await;
    let mut flushing = {
        let broker = broker.clone();
        tokio::spawn(async move { broker.flush().await })
    };
    assert!(
        timeout(Duration::from_millis(150), &mut flushing)
            .await
            .is_err(),
        "flush must wait for the write"
    );
    engine.reply(request_id(&write), json!({}));
    let _ = next_frame(&mut alice).await;
    let flush = next_request(&mut engine).await;
    assert_eq!(flush["method"], "Kitesurf.flushProfile");
    engine.reply(request_id(&flush), json!({ "saved": true }));
    let status = timeout(WAIT, flushing)
        .await
        .expect("flush completes")
        .expect("flush task")
        .expect("flush ok");
    assert_eq!(status["revision"], 0);
    send(&alice, &broker, r#"{"id":2,"method":"Target.getTargets"}"#).await;
    let request = next_request(&mut engine).await;
    engine.reply(request_id(&request), json!({ "targetInfos": [] }));
    let reply = next_frame(&mut alice).await;
    assert_eq!(reply["id"], 2);
}

#[tokio::test]
async fn flush_failure_restores_running() {
    let mut engine = fake_engine().await;
    let broker = broker(&engine).await;
    let mut alice = connect(&broker, Endpoint::Browser, Role::Controller, "alice").await;
    let flushing = {
        let broker = broker.clone();
        tokio::spawn(async move { broker.flush().await })
    };
    let flush = next_request(&mut engine).await;
    engine.push(json!({
      "id": request_id(&flush),
      "error": { "code": -32000, "message": "profile unavailable" }
    }));
    let failed = timeout(WAIT, flushing)
        .await
        .expect("flush completes")
        .expect("flush task");
    assert!(failed.is_err());
    send(&alice, &broker, r#"{"id":1,"method":"Target.getTargets"}"#).await;
    let request = next_request(&mut engine).await;
    engine.reply(request_id(&request), json!({ "targetInfos": [] }));
    let reply = next_frame(&mut alice).await;
    assert_eq!(reply["id"], 1);
}

#[tokio::test]
async fn close_flushes_before_browser_close_and_reports_success() {
    let mut engine = fake_engine().await;
    let broker = broker(&engine).await;
    let mut alice = connect(&broker, Endpoint::Browser, Role::Controller, "alice").await;
    let mut closed = broker.closed_watch();
    let mut closing = {
        let broker = broker.clone();
        tokio::spawn(async move { broker.close(CloseKind::Http, None, None, None).await })
    };
    let flush = next_method(&mut engine, "Kitesurf.flushProfile").await;
    assert!(
        timeout(Duration::from_millis(100), &mut closing)
            .await
            .is_err(),
        "close must wait for the flush"
    );
    engine.reply(request_id(&flush), json!({ "saved": true }));
    let browser = next_method(&mut engine, "Browser.close").await;
    engine.reply(request_id(&browser), json!({}));
    timeout(WAIT, closing)
        .await
        .expect("close completes")
        .expect("close task")
        .expect("close ok");
    assert_eq!(
        next_close(&mut alice).await,
        (1000, "Browser closed".to_string())
    );
    broker.writer_gone(alice.id);
    timeout(WAIT, closed.changed())
        .await
        .expect("closed watch")
        .expect("closed sender");
    assert!(*closed.borrow());
}

#[tokio::test]
async fn slow_client_is_disconnected_without_blocking_others() {
    let mut engine = fake_engine().await;
    let broker = broker(&engine).await;
    let mut alice = connect(&broker, Endpoint::Browser, Role::Controller, "alice").await;
    let mut bob = connect(&broker, Endpoint::Browser, Role::Viewer, "bob").await;
    send(
        &alice,
        &broker,
        r#"{"id":1,"method":"Target.attachToTarget","params":{"targetId":"T1","flatten":true}}"#,
    )
    .await;
    let attach = next_request(&mut engine).await;
    engine.reply(request_id(&attach), json!({ "sessionId": "S1" }));
    let _ = next_frame(&mut alice).await;
    for _ in 0..70 {
        engine.event(json!({
          "sessionId": "S1",
          "method": "Page.screencastFrame",
          "params": { "data": "frame" }
        }));
    }
    timeout(WAIT, alice.cancel.changed())
        .await
        .expect("slow client cancelled")
        .expect("cancel watch");
    send(&bob, &broker, r#"{"id":1,"method":"Target.getTargets"}"#).await;
    let request = next_method(&mut engine, "Target.getTargets").await;
    engine.reply(request_id(&request), json!({ "targetInfos": [] }));
    let reply = next_frame(&mut bob).await;
    assert_eq!(reply["id"], 1);
}

#[tokio::test]
async fn page_endpoint_byte_budget_disconnects_only_that_client() {
    let mut engine = fake_engine().await;
    let broker = broker(&engine).await;
    let mut page = connect(
        &broker,
        Endpoint::Page {
            target_id: "T1".to_string(),
        },
        Role::Controller,
        "alice",
    )
    .await;
    let attach = next_request(&mut engine).await;
    assert_eq!(attach["method"], "Target.attachToTarget");
    let payload = "x".repeat(5 * 1024 * 1024);
    for id in [1, 2] {
        let request = json!({
          "id": id,
          "method": "Runtime.evaluate",
          "params": { "expression": payload }
        })
        .to_string();
        assert!(broker.frame(page.id, request).await);
    }
    assert_eq!(
        next_close(&mut page).await,
        (1011, "Slow client".to_string())
    );
    let mut bob = connect(&broker, Endpoint::Browser, Role::Viewer, "bob").await;
    send(&bob, &broker, r#"{"id":1,"method":"Target.getTargets"}"#).await;
    let request = next_request(&mut engine).await;
    assert_eq!(request["method"], "Target.getTargets");
    engine.reply(request_id(&request), json!({ "targetInfos": [] }));
    assert_eq!(next_frame(&mut bob).await["id"], 1);
}

#[tokio::test]
async fn engine_loss_fails_pending_and_closes_clients() {
    let mut engine = fake_engine().await;
    let broker = broker(&engine).await;
    let mut alice = connect(&broker, Endpoint::Browser, Role::Controller, "alice").await;
    let mut lost = broker.lost_watch();
    send(&alice, &broker, r#"{"id":1,"method":"Target.getTargets"}"#).await;
    let _ = next_request(&mut engine).await;
    engine.kill();
    timeout(WAIT, lost.changed())
        .await
        .expect("lost watch")
        .expect("lost sender");
    assert!(lost.borrow().is_some());
    let failure = next_frame(&mut alice).await;
    assert_eq!(
        message(&failure),
        "Browser session ended; outcome may be unknown"
    );
}

#[tokio::test]
async fn engine_loss_during_flush_fails_the_operation() {
    let mut engine = fake_engine().await;
    let broker = broker(&engine).await;
    let _alice = connect(&broker, Endpoint::Browser, Role::Controller, "alice").await;
    let mut lost = broker.lost_watch();
    let flushing = {
        let broker = broker.clone();
        tokio::spawn(async move { broker.flush().await })
    };
    let flush = next_request(&mut engine).await;
    assert_eq!(flush["method"], "Kitesurf.flushProfile");
    engine.kill();
    let failed = timeout(WAIT, flushing)
        .await
        .expect("flush completes")
        .expect("flush task");
    assert!(failed.is_err());
    timeout(WAIT, lost.changed())
        .await
        .expect("lost watch")
        .expect("lost sender");
    assert!(lost.borrow().is_some());
}

#[tokio::test]
async fn engine_loss_during_close_reports_an_unknown_outcome() {
    let mut engine = fake_engine().await;
    let broker = broker(&engine).await;
    let _alice = connect(&broker, Endpoint::Browser, Role::Controller, "alice").await;
    let mut lost = broker.lost_watch();
    let mut closing = {
        let broker = broker.clone();
        tokio::spawn(async move { broker.close(CloseKind::Http, None, None, None).await })
    };
    let flush = next_method(&mut engine, "Kitesurf.flushProfile").await;
    engine.reply(request_id(&flush), json!({ "saved": true }));
    let browser = next_method(&mut engine, "Browser.close").await;
    assert_eq!(browser["method"], "Browser.close");
    engine.kill();
    let failed = timeout(WAIT, &mut closing)
        .await
        .expect("close completes")
        .expect("close task");
    assert!(
        failed
            .as_ref()
            .is_err_and(|message| message.contains("outcome may be unknown")),
        "{failed:?}"
    );
    timeout(WAIT, lost.changed())
        .await
        .expect("lost watch")
        .expect("lost sender");
    assert!(lost.borrow().is_some());
}

#[tokio::test]
async fn timed_out_attach_blocks_takeover_until_the_late_reply() {
    let mut engine = fake_engine().await;
    let broker = broker_with_deadlines(
        &engine,
        Duration::from_secs(120),
        Duration::from_millis(400),
    )
    .await;
    let mut alice = connect(&broker, Endpoint::Browser, Role::Controller, "alice").await;
    send(
        &alice,
        &broker,
        r#"{"id":1,"method":"Target.attachToTarget","params":{"targetId":"T1","flatten":true}}"#,
    )
    .await;
    let attach = next_request(&mut engine).await;
    let expired = next_frame(&mut alice).await;
    assert_eq!(expired["id"], 1);
    assert!(
        message(&expired).contains("outcome may be unknown"),
        "{}",
        message(&expired)
    );
    let mut control = {
        let broker = broker.clone();
        tokio::spawn(async move { broker.control("bob".to_string(), true).await })
    };
    assert!(
        timeout(Duration::from_millis(150), &mut control)
            .await
            .is_err(),
        "an unknown writer result must block takeover"
    );
    engine.reply(request_id(&attach), json!({ "sessionId": "late" }));
    let detach = next_request(&mut engine).await;
    assert_eq!(detach["method"], "Target.detachFromTarget");
    assert_eq!(detach["params"]["sessionId"], "late");
    engine.reply(request_id(&detach), json!({}));
    let takeover = timeout(WAIT, control)
        .await
        .expect("takeover completes after the late reply")
        .expect("control task")
        .ok()
        .expect("takeover ok");
    assert_eq!(takeover, ("bob".to_string(), 2));
    let mut bob = connect(&broker, Endpoint::Browser, Role::Controller, "bob").await;
    send(
        &bob,
        &broker,
        r#"{"id":1,"method":"Runtime.evaluate","sessionId":"late"}"#,
    )
    .await;
    let rejected = next_frame(&mut bob).await;
    assert!(
        message(&rejected).contains("attachment expired or belongs to another"),
        "{}",
        message(&rejected)
    );
    expect_no_request(&mut engine).await;
}

#[tokio::test]
async fn retirement_during_attach_releases_late_session_first() {
    let mut engine = fake_engine().await;
    let broker = broker(&engine).await;
    let mut alice = connect(&broker, Endpoint::Browser, Role::Controller, "alice").await;
    send(
        &alice,
        &broker,
        r#"{"id":1,"method":"Target.attachToTarget","params":{"targetId":"page","flatten":true}}"#,
    )
    .await;
    let attach = next_request(&mut engine).await;
    engine.event(json!({
      "method": "Target.attachedToTarget",
      "params": { "sessionId": "late", "targetInfo": { "targetId": "page" } }
    }));
    let early = next_frame(&mut alice).await;
    assert_eq!(early["method"], "Target.attachedToTarget");
    let mut control = {
        let broker = broker.clone();
        tokio::spawn(async move { broker.control("bob".to_string(), true).await })
    };
    assert!(
        timeout(Duration::from_millis(150), &mut control)
            .await
            .is_err(),
        "takeover waits for the attach"
    );
    engine.reply(request_id(&attach), json!({ "sessionId": "late" }));
    let reply = next_frame(&mut alice).await;
    assert_eq!(reply["id"], 1);
    let detach = next_request(&mut engine).await;
    assert_eq!(detach["method"], "Target.detachFromTarget");
    assert_eq!(detach["params"]["sessionId"], "late");
    engine.reply(request_id(&detach), json!({}));
    let takeover = timeout(WAIT, control)
        .await
        .expect("takeover completes")
        .expect("control task")
        .ok()
        .expect("takeover ok");
    assert_eq!(takeover, ("bob".to_string(), 2));
    let mut bob = connect(&broker, Endpoint::Browser, Role::Controller, "bob").await;
    send(&bob, &broker, r#"{"id":1,"method":"Browser.getVersion"}"#).await;
    let version = next_request(&mut engine).await;
    assert_eq!(version["method"], "Browser.getVersion");
    engine.reply(request_id(&version), json!({ "userAgent": "fake" }));
    assert_eq!(next_frame(&mut bob).await["id"], 1);
    send(
        &bob,
        &broker,
        r#"{"id":2,"method":"Runtime.evaluate","sessionId":"late"}"#,
    )
    .await;
    let expired = next_frame(&mut bob).await;
    assert!(
        message(&expired).contains("attachment expired or belongs to another"),
        "{}",
        message(&expired)
    );
    expect_no_request(&mut engine).await;
}

#[tokio::test]
async fn viewer_attachment_survives_controller_retirement() {
    let mut engine = fake_engine().await;
    let broker = broker(&engine).await;
    let mut alice = connect(&broker, Endpoint::Browser, Role::Controller, "alice").await;
    let mut bob = connect(&broker, Endpoint::Browser, Role::Viewer, "bob").await;
    send(
        &alice,
        &broker,
        r#"{"id":1,"method":"Target.setAutoAttach","params":{"autoAttach":true,"flatten":true}}"#,
    )
    .await;
    let enable = next_request(&mut engine).await;
    assert_eq!(enable["method"], "Target.setAutoAttach");
    engine.reply(request_id(&enable), json!({}));
    let targets = next_method(&mut engine, "Target.getTargets").await;
    engine.reply(request_id(&targets), json!({ "targetInfos": [] }));
    assert_eq!(next_frame(&mut alice).await["id"], 1);
    send(
        &bob,
        &broker,
        r#"{"id":1,"method":"Target.attachToTarget","params":{"targetId":"page","flatten":true}}"#,
    )
    .await;
    let attach = next_request(&mut engine).await;
    engine.reply(request_id(&attach), json!({ "sessionId": "viewer" }));
    assert_eq!(next_frame(&mut bob).await["result"]["sessionId"], "viewer");
    let control = {
        let broker = broker.clone();
        tokio::spawn(async move { broker.control("bob".to_string(), true).await })
    };
    let disable = next_request(&mut engine).await;
    assert_eq!(disable["method"], "Target.setAutoAttach");
    assert_eq!(disable["params"]["autoAttach"], false);
    engine.reply(request_id(&disable), json!({}));
    timeout(WAIT, control)
        .await
        .expect("takeover completes")
        .expect("control task")
        .ok()
        .expect("takeover ok");
    engine.event(json!({ "sessionId": "viewer", "method": "Page.loadEventFired" }));
    assert_eq!(next_frame(&mut bob).await["method"], "Page.loadEventFired");
    expect_silence(&mut alice).await;
}

#[tokio::test]
async fn page_auto_attachments_detach_without_closing_targets() {
    let mut engine = fake_engine().await;
    let broker = broker(&engine).await;
    let mut alice = connect(&broker, Endpoint::Browser, Role::Controller, "alice").await;
    send(
        &alice,
        &broker,
        r#"{"id":1,"method":"Target.attachToTarget","params":{"targetId":"page","flatten":true}}"#,
    )
    .await;
    let attach = next_request(&mut engine).await;
    engine.reply(request_id(&attach), json!({ "sessionId": "page-session" }));
    assert_eq!(next_frame(&mut alice).await["id"], 1);
    send(
        &alice,
        &broker,
        r#"{"id":2,"method":"Target.setAutoAttach","sessionId":"page-session","params":{"autoAttach":true,"flatten":true}}"#,
    )
    .await;
    let enable = next_request(&mut engine).await;
    assert_eq!(enable["sessionId"], "page-session");
    engine.reply(request_id(&enable), json!({}));
    assert_eq!(next_frame(&mut alice).await["id"], 2);
    engine.event(json!({
      "method": "Target.attachedToTarget",
      "params": { "sessionId": "automatic", "targetInfo": { "targetId": "another-page" } }
    }));
    assert_eq!(
        next_frame(&mut alice).await["method"],
        "Target.attachedToTarget"
    );
    let mut control = {
        let broker = broker.clone();
        tokio::spawn(async move { broker.control("bob".to_string(), true).await })
    };
    let disable = next_request(&mut engine).await;
    assert_eq!(disable["method"], "Target.setAutoAttach");
    assert_eq!(disable["params"]["autoAttach"], false);
    engine.reply(request_id(&disable), json!({}));
    let mut detached = Vec::new();
    let outcome = timeout(WAIT, async {
        loop {
            tokio::select! {
              result = &mut control => return result,
              request = engine.receive.recv() => {
                let request = request.expect("upstream open");
                assert_eq!(request["method"], "Target.detachFromTarget");
                detached.push(
                    request["params"]["sessionId"]
                        .as_str()
                        .expect("detach session")
                        .to_string(),
                );
                engine.reply(request_id(&request), json!({}));
              }
            }
        }
    })
    .await
    .expect("takeover completes");
    let _ = outcome.expect("control task").ok().expect("takeover ok");
    detached.sort();
    assert_eq!(
        detached,
        vec!["automatic".to_string(), "page-session".to_string()]
    );
}
