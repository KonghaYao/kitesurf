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
        + 16 * 256;
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
    fn typed_profile_status_all_sizes_and_body_writer_ownership() {
        use crate::services::LeasedProfileStatus;
        use crate::transport::{PayloadBudget, MAX_FRAME_BYTES, ORDINARY_BYTES, INTERNAL_BYTES};
        use http_body_util::BodyExt;
        let budget = PayloadBudget::default();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let path = std::path::PathBuf::from("/状态/\"\\\n\u{1f600}".repeat(20_000));
        let id = "id\"\\\n\u{1f600}".repeat(10_000);
        drop(crate::relay::budgeted_body(&budget, &false).unwrap());
        runtime.block_on(async {});
        for sqlite in [false, true] {
            TRACK.with(|track| {
                let mut track = track.borrow_mut();
                track.stats = Stats::default();
                track.slots.fill((0, 0));
                track.active = true;
            });
            let status = if sqlite {
                LeasedProfileStatus::sqlite(&budget, &path, 9_007_199_254_740_991, 1, &id)
            } else {
                LeasedProfileStatus::memory(&budget, usize::MAX, u64::MAX, i64::MIN, u64::MAX)
            }
            .unwrap();
            let held_snapshot = budget.used();
            let clone = status.clone();
            drop(status);
            assert_eq!(budget.used(), held_snapshot);
            let (tx, rx) = tokio::sync::oneshot::channel();
            tx.send(clone).ok().unwrap();
            assert_eq!(budget.used(), held_snapshot);
            let status = runtime.block_on(rx).unwrap();
            let bytes = crate::relay::budgeted_body(&budget, &status).unwrap();
            let held_body = budget.used();
            assert!(held_body.0 > held_snapshot.0 + bytes.len());
            drop(status);
            assert!(budget.used().0 > MAX_FRAME_BYTES + bytes.len());
            let body = http_body_util::Full::new(bytes.clone());
            drop(bytes);
            let writer_held = runtime.block_on(body.collect()).unwrap().to_bytes();
            assert!(budget.used().0 > MAX_FRAME_BYTES + writer_held.len());
            drop(writer_held);
            let stats = TRACK.with(|track| {
                let mut track = track.borrow_mut();
                track.active = false;
                track.stats
            });
            println!("typed profile sqlite={sqlite}: {stats:?}; snapshot={held_snapshot:?}; body={held_body:?}; high={:?}", budget.high_water());
            assert!(!stats.overflow);
            assert_eq!(stats.live, 0);
            assert!(
                stats.count < 32,
                "fixed fields must not allocate per text node: {stats:?}"
            );
            assert!(
                stats.peak <= held_body.0 - MAX_FRAME_BYTES + 4096,
                "{stats:?}"
            );
            assert_eq!(held_body.1, 0);
            assert_eq!(budget.used(), (MAX_FRAME_BYTES, 0));
            assert!(budget.high_water().0 <= ORDINARY_BYTES);
            assert!(budget.high_water().1 <= INTERNAL_BYTES);
        }
    }

    #[test]
    fn production_received_allocations_cover_raw_reply_and_event_outputs() {
        use crate::cdp_raw::{self, LeasedRaw, ParsePolicy, RawEnvelope};
        use crate::transport::{PayloadBudget, MAX_FRAME_BYTES};
        use super::super::Received;
        let budget = PayloadBudget::default();
        drop(budget.json_scratch());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        drop(runtime.block_on(budget.receive(true)));
        let warm = LeasedRaw::literal("{}", &budget).unwrap();
        let warm = RawEnvelope::parse(
            warm,
            ParsePolicy::UpstreamStrict,
            &mut budget.json_scratch(),
        )
        .unwrap();
        drop(cdp_raw::serialize_event(&warm, true, &budget, &mut budget.json_scratch()).unwrap());
        drop(warm);
        let warm =
            LeasedRaw::accounted("{}".into(), runtime.block_on(budget.receive(true)), &budget)
                .unwrap();
        let warm = RawEnvelope::parse(
            warm,
            ParsePolicy::UpstreamStrict,
            &mut budget.json_scratch(),
        )
        .unwrap();
        drop(Received::parse(warm, &budget).unwrap());
        let fixtures = [
            (
                "result",
                format!(
                    r#"{{"id":1,"result":{{"data":"{}"}}}}"#,
                    "\\u0061".repeat(500_000)
                ),
                false,
            ),
            (
                "nodes",
                format!(
                    r#"{{"method":"Runtime.consoleAPICalled","sessionId":"s","params":[{}0]}}"#,
                    "{\"x\":[1,2]},".repeat(100_000)
                ),
                true,
            ),
            (
                "key",
                format!(
                    r#"{{"method":"Runtime.consoleAPICalled","sessionId":"s","{}":0}}"#,
                    "\\u0061".repeat(500_000)
                ),
                true,
            ),
            (
                "unknown",
                format!(
                    r#"{{"method":"Runtime.consoleAPICalled","sessionId":"s","unknown":{{{}"end":0}}}}"#,
                    "\"x\":{},".repeat(100_000)
                ),
                true,
            ),
        ];
        for (name, text, page) in fixtures {
            let wire = text.len();
            let lease = runtime.block_on(budget.receive(true));
            TRACK.with(|track| {
                let mut track = track.borrow_mut();
                track.stats = Stats::default();
                track.slots.fill((0, 0));
                track.active = true;
            });
            let raw = LeasedRaw::accounted(text, lease, &budget).unwrap();
            let envelope = {
                let mut scratch = budget.json_scratch();
                RawEnvelope::parse(raw, ParsePolicy::UpstreamStrict, &mut scratch).unwrap()
            };
            let received = Received::parse(envelope, &budget).unwrap();
            let output = if name == "result" {
                received
                    .serialize_reply(&budget, 42, Some("external"))
                    .unwrap()
            } else {
                let mut scratch = budget.json_scratch();
                cdp_raw::serialize_event(&received.raw, page, &budget, &mut scratch).unwrap()
            };
            let held = budget.used();
            assert!(held.0 >= MAX_FRAME_BYTES + output.0.capacity());
            assert!(held.1 >= wire);
            drop(received);
            assert_eq!(budget.used().1, 0);
            drop(output);
            let stats = TRACK.with(|track| {
                let mut track = track.borrow_mut();
                track.active = false;
                track.stats
            });
            println!("production upstream {name}: {stats:?}; held ordinary/internal={held:?}");
            assert!(!stats.overflow);
            assert_eq!(stats.live, 0);
            assert!(stats.peak < wire + 64 * 1024, "{name}: {stats:?}");
            assert_eq!(budget.used(), (MAX_FRAME_BYTES, 0));
        }
    }

    #[test]
    fn exhausted_unknown_near_frame_limit_has_no_owned_payload_allocation() {
        use crate::cdp_raw::{LeasedRaw, ParsePolicy, RawEnvelope};
        use crate::transport::{PayloadBudget, MAX_FRAME_BYTES, ORDINARY_BYTES, INTERNAL_BYTES};
        use super::super::{EventMethod, Received};
        let budget = PayloadBudget::default();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        drop(runtime.block_on(budget.receive(true)));
        drop(budget.json_scratch());
        let warm =
            LeasedRaw::accounted("{}".into(), runtime.block_on(budget.receive(true)), &budget)
                .unwrap();
        let warm = RawEnvelope::parse(
            warm,
            ParsePolicy::UpstreamStrict,
            &mut budget.json_scratch(),
        )
        .unwrap();
        drop(Received::parse(warm, &budget).unwrap());
        let mut text = format!(
            r#"{{"method":"Unknown.\u006fpaque","params":{{"data":"{}"}}}}"#,
            "\\u0061".repeat((MAX_FRAME_BYTES - 256) / 6)
        );
        text.shrink_to_fit();
        let wire = text.len();
        assert!(wire > MAX_FRAME_BYTES - 512);
        let held = budget
            .try_acquire(ORDINARY_BYTES - MAX_FRAME_BYTES, false)
            .unwrap();
        let lease = runtime.block_on(budget.receive(true));
        TRACK.with(|track| {
            let mut track = track.borrow_mut();
            track.stats = Stats::default();
            track.slots.fill((0, 0));
            track.active = true;
        });
        let raw = LeasedRaw::accounted(text, lease, &budget).unwrap();
        let raw = RawEnvelope::parse(raw, ParsePolicy::UpstreamStrict, &mut budget.json_scratch())
            .unwrap();
        let received = Received::parse(raw, &budget).unwrap();
        assert!(received.method == Some(EventMethod::Other));
        assert!(received.outer.is_none());
        assert_eq!(budget.used(), (ORDINARY_BYTES, MAX_FRAME_BYTES + 256));
        drop(received);
        let stats = TRACK.with(|track| {
            let mut track = track.borrow_mut();
            track.active = false;
            track.stats
        });
        println!(
            "exhausted unknown wire={wire}: {stats:?}; high={:?}",
            budget.high_water()
        );
        assert!(!stats.overflow);
        assert_eq!(stats.live, 0);
        assert!(stats.peak < 64 * 1024);
        assert_eq!(budget.used(), (ORDINARY_BYTES, 0));
        assert!(budget.high_water().1 <= INTERNAL_BYTES);
        drop(held);
        assert_eq!(budget.used(), (MAX_FRAME_BYTES, 0));
    }

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

#[test]
fn upstream_accessors_match_pre_migration_value_policy() {
    use crate::cdp_raw::{LeasedRaw, ParsePolicy, RawEnvelope};
    use crate::transport::{PayloadBudget, MAX_FRAME_BYTES};
    let budget = PayloadBudget::default();
    let mut messages = Vec::new();
    for id in [
        "0",
        "-0",
        "1.0",
        "1e0",
        "-1",
        "9007199254740991",
        "9007199254740992",
        "18446744073709551615",
        "18446744073709551616",
        "1e400",
        "null",
    ] {
        messages.push(format!(r#"{{"id":{id},"result":{{"sessionId":"s"}}}}"#));
    }
    messages.extend([
        r#"{"id":1,"\u0069d":2,"method":"x","\u006dethod":"Target.attachedToTarget","sessionId":"old","session\u0049d":"parent","params":{"sessionId":"old","\u0073essionId":"child","targetInfo":{"targetId":"bad","target\u0049d":"target"}},"result":{"sessionId":"old","session\u0049d":"new"},"error":{},"\u0065rror":null}"#.into(),
        r#"{"id":1,"result":{},"\u0072esult":null}"#.into(),
        r#"{"id":1,"error":null}"#.into(),
        r#"{"id":1,"unknown":"\uD800"}"#.into(),
        r#"{"id":1,"unknown":1e400}"#.into(),
        r#"{"id":1,"unknown":[1,]}"#.into(),
    ]);
    for depth in [125, 126, 127, 128] {
        messages.push(format!(
            r#"{{"id":1,"unknown":{}0{}}}"#,
            "[".repeat(depth),
            "]".repeat(depth)
        ));
    }
    for text in messages {
        // HEAD^ transport 的原路径：serde Value + as_u64/get，绝不用于生产。
        let old = serde_json::from_str::<Value>(&text);
        let lease = budget.try_acquire(text.capacity(), false).unwrap();
        let raw = LeasedRaw::accounted(text, lease, &budget).unwrap();
        let parsed = {
            let mut scratch = budget.json_scratch();
            RawEnvelope::parse(raw, ParsePolicy::UpstreamStrict, &mut scratch)
        };
        assert_eq!(parsed.is_ok(), old.is_ok());
        if let (Ok(raw), Ok(old)) = (parsed, old) {
            let value = super::Received::parse(raw, &budget).unwrap();
            let mut scratch = budget.json_scratch();
            assert_eq!(
                value.raw.id(&mut scratch),
                old.get("id").and_then(Value::as_u64)
            );
            assert_eq!(value.has_error(), old.get("error").is_some());
            for (path, expected) in [
                (vec!["method"], old.get("method")),
                (vec!["sessionId"], old.get("sessionId")),
                (
                    vec!["params", "sessionId"],
                    old.pointer("/params/sessionId"),
                ),
                (
                    vec!["params", "targetInfo", "targetId"],
                    old.pointer("/params/targetInfo/targetId"),
                ),
                (
                    vec!["result", "sessionId"],
                    old.pointer("/result/sessionId"),
                ),
            ] {
                let mut node = value.raw.node(path[0]);
                for key in &path[1..] {
                    node = node.and_then(|node| node.field(key, &mut scratch));
                }
                let actual = node.and_then(|node| node.string(&budget, &mut scratch).unwrap());
                assert_eq!(actual.as_deref(), expected.and_then(Value::as_str));
            }
        }
        assert_eq!(budget.used(), (MAX_FRAME_BYTES, 0));
    }
}

#[tokio::test]
async fn upstream_receive_owner_releases_on_parse_drop_and_backfill_handoff() {
    use crate::transport::{PayloadBudget, MAX_FRAME_BYTES};
    use crate::cdp_raw::{LeasedRaw, ParsePolicy, RawEnvelope};
    let budget = PayloadBudget::default();
    let lease = budget.receive(true).await;
    let raw = LeasedRaw::accounted("{invalid".into(), lease, &budget).unwrap();
    assert!(
        RawEnvelope::parse(raw, ParsePolicy::UpstreamStrict, &mut budget.json_scratch()).is_err()
    );
    assert_eq!(budget.used(), (MAX_FRAME_BYTES, 0));
    let lease = budget.receive(true).await;
    let raw = LeasedRaw::accounted(
        r#"{"result":{"large":"discard","sessionId":"keep"}}"#.into(),
        lease,
        &budget,
    )
    .unwrap();
    let saved = {
        let mut scratch = budget.json_scratch();
        let envelope = RawEnvelope::parse(raw, ParsePolicy::UpstreamStrict, &mut scratch).unwrap();
        let saved = envelope
            .node("result")
            .unwrap()
            .field("sessionId", &mut scratch)
            .unwrap()
            .copy(&budget, &mut scratch)
            .unwrap();
        assert_eq!(saved.as_str(), r#""keep""#);
        saved
    };
    assert_eq!(budget.used().1, 0);
    drop(timeout(WAIT, budget.receive(true)).await.unwrap());
    drop(saved);
    assert_eq!(budget.used(), (MAX_FRAME_BYTES, 0));

    let mut engine = fake_engine().await;
    let broker = broker(&engine).await;
    let mut client = connect(&broker, Endpoint::Browser, Role::Controller, "owner").await;
    send(
        &client,
        &broker,
        r#"{"id":1,"method":"Target.setAutoAttach","params":{"autoAttach":true}}"#,
    )
    .await;
    let request = next_request(&mut engine).await;
    engine.raw("{invalid".into());
    engine.raw(r#"{"id":999999,"result":{"unknown":true}}"#.into());
    engine.raw(format!(
        r#"{{"id":{},"result":{{"opaque":"{}"}}}}"#,
        request_id(&request),
        "x".repeat(256 * 1024)
    ));
    let targets = next_request(&mut engine).await;
    engine.raw(format!(r#"{{"id":{},"result":{{"targetInfos":[{{"type":"page","targetId":"old","target\u0049d":"tab"}}]}}}}"#, request_id(&targets)));
    let attached = next_request(&mut engine).await;
    assert_eq!(attached["params"]["targetId"], "tab");
    engine.reply(request_id(&attached), json!({"sessionId":"attached"}));
    assert_eq!(
        next_frame(&mut client).await["result"]["opaque"]
            .as_str()
            .unwrap()
            .len(),
        256 * 1024
    );
    engine.event(json!({"method":"Runtime.consoleAPICalled","sessionId":"attached","params":{}}));
    assert_eq!(
        next_frame(&mut client).await["method"],
        "Runtime.consoleAPICalled"
    );
    engine.kill();
    assert!(!broker.wait_terminal().await);
}

#[test]
fn upstream_raw_sentinel_is_only_an_application_field() {
    use crate::cdp_raw::{LeasedRaw, ParsePolicy, RawEnvelope};
    let budget = crate::transport::PayloadBudget::default();
    let text =
        r#"{"id":1,"result":{"$serde_json::private::RawValue":"{\"sessionId\":\"hidden\"}"}}"#;
    let raw = LeasedRaw::literal(text, &budget).unwrap();
    let envelope = {
        let mut scratch = budget.json_scratch();
        RawEnvelope::parse(raw, ParsePolicy::UpstreamStrict, &mut scratch).unwrap()
    };
    let value = super::Received::parse(envelope, &budget).unwrap();
    assert!(value.attached.is_none());
    let mut scratch = budget.json_scratch();
    assert!(value
        .raw
        .node("result")
        .unwrap()
        .field("$serde_json::private::RawValue", &mut scratch)
        .is_some());
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

#[tokio::test]
async fn raw_upstream_reply_and_page_event_use_production_ingress() {
    let mut engine = fake_engine().await;
    let broker = broker(&engine).await;
    let mut browser = connect(&broker, Endpoint::Browser, Role::Controller, "owner").await;
    let mut page = connect(
        &broker,
        Endpoint::Page {
            target_id: "tab".into(),
        },
        Role::Viewer,
        "viewer",
    )
    .await;
    let attach = next_request(&mut engine).await;
    engine.reply(request_id(&attach), json!({"sessionId":"page-session"}));
    send(&page, &broker, r#"{"id":1,"method":"Page.enable"}"#).await;
    let enabled = next_request(&mut engine).await;
    engine.reply(request_id(&enabled), json!({}));
    assert_eq!(next_frame(&mut page).await["id"], 1);
    let event = r#"{ "method":"Runtime.consoleAPICalled", "sessionId":"wrong", "session\u0049d":"page-session", "params":{"x":1}, "$serde_json::private::RawValue":"opaque" }"#;
    engine.raw(event.into());
    let frame = next_frame(&mut page).await;
    assert!(frame.get("sessionId").is_none());
    assert_eq!(frame["params"]["x"], 1);
    assert_eq!(frame["$serde_json::private::RawValue"], "opaque");
    expect_silence(&mut browser).await;
    send(
        &browser,
        &broker,
        r#"{"id":9,"method":"Browser.getVersion"}"#,
    )
    .await;
    let request = next_request(&mut engine).await;
    let id = request_id(&request);
    engine.raw(format!(
        r#"{{"id":{id}.0,"method":"Runtime.consoleAPICalled","params":{{"secret":true}}}}"#
    ));
    engine.raw(r#"{"id":999999,"result":{"secret":true}}"#.to_string());
    engine.raw(format!(
        r#"{{"id":999998,"\u0069d":{id},"result":{{ "opaque" : "\u0061", "sessionId":"{}" }},"error":null}}"#,
        "x".repeat(8192),
    ));
    let command = timeout(WAIT, browser.frames.recv()).await.unwrap().unwrap();
    let ClientCommand::Text(text, _lease) = command else {
        panic!("expected reply")
    };
    assert!(text.contains(r#"{ "opaque" : "\u0061", "sessionId":""#));
    let reply: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(reply["id"], 9);
    assert_eq!(reply["result"]["opaque"], "a");
    assert!(reply.get("error").is_some_and(Value::is_null));
    expect_silence(&mut browser).await;
    expect_silence(&mut page).await;
    send(
        &browser,
        &broker,
        r#"{"id":10,"method":"Target.setDiscoverTargets","params":{"discover":true}}"#,
    )
    .await;
    let discover = next_request(&mut engine).await;
    engine.reply(request_id(&discover), json!({}));
    assert_eq!(next_frame(&mut browser).await["id"], 10);
    let exact = r#" { "method" : "Target.targetCreated", "params" : {"targetInfo":{"type":"page","targetId":"next"}}, "unknown" : [1, 2] } "#;
    engine.raw(exact.into());
    let command = timeout(WAIT, browser.frames.recv()).await.unwrap().unwrap();
    let ClientCommand::Text(text, _lease) = command else {
        panic!("expected browser event")
    };
    assert_eq!(text, exact);
    expect_silence(&mut page).await;
    engine.kill();
    assert!(!broker.wait_terminal().await);
}

struct FakeEngine {
    address: SocketAddr,
    send: mpsc::UnboundedSender<String>,
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
        self.raw(value.to_string());
    }

    fn raw(&self, text: String) {
        self.send.send(text).expect("fake engine outbound");
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
    let (send, mut outbound) = mpsc::unbounded_channel::<String>();
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
    outbound: &mut mpsc::UnboundedReceiver<String>,
    inbound: &mpsc::UnboundedSender<Value>,
    killed: &mut mpsc::UnboundedReceiver<()>,
) {
    let (mut sink, mut source) = socket.split();
    loop {
        tokio::select! {
          _ = killed.recv() => return,
          Some(text) = outbound.recv() => {
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

async fn receive_ack(broker: &BrokerHandle, text: &str) -> (bool, usize) {
    let budget = broker.payload_budget();
    let raw =
        crate::cdp_raw::LeasedRaw::accounted(text.to_owned(), budget.receive(true).await, &budget)
            .unwrap();
    broker
        .command(|reply| super::BrokerCommand::ReceiveAck(raw, reply))
        .await
        .unwrap()
}

#[tokio::test]
async fn exhausted_ordinary_empty_reply_keeps_broker_alive() {
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
    let held = budget
        .try_acquire(crate::transport::ORDINARY_BYTES - budget.used().0, false)
        .unwrap();
    assert!(budget.try_acquire(1, false).is_none());
    assert!(
        receive_ack(
            &broker,
            &format!(r#"{{"id":{},"result":{{}}}}"#, request_id(&request))
        )
        .await
        .0
    );
    assert_eq!(broker.budget_snapshot().await.0, 0);
    drop(held);
    broker.engine_lost();
    assert!(!broker.wait_terminal().await);
    engine.kill();
}

#[tokio::test]
async fn exhausted_ordinary_known_detach_removes_attachment_without_lost() {
    let mut engine = fake_engine().await;
    let broker = broker(&engine).await;
    let mut client = connect(&broker, Endpoint::Browser, Role::Viewer, "viewer").await;
    send(
        &client,
        &broker,
        r#"{"id":1,"method":"Target.attachToTarget","params":{"targetId":"T"}}"#,
    )
    .await;
    let attach = next_request(&mut engine).await;
    engine.reply(request_id(&attach), json!({"sessionId":"known"}));
    assert_eq!(next_frame(&mut client).await["id"], 1);
    let budget = broker.payload_budget();
    let held = budget
        .try_acquire(crate::transport::ORDINARY_BYTES - budget.used().0, false)
        .unwrap();
    assert!(budget.try_acquire(1, false).is_none());
    assert_eq!(
        receive_ack(
            &broker,
            r#"{"method":"Target.detachedFromTarget","params":{"sessionId":"known"}}"#
        )
        .await,
        (true, 0)
    );
    assert!(broker.session().await.unwrap().alive);
    drop(held);
    broker.engine_lost();
    assert!(!broker.wait_terminal().await);
    engine.kill();
}

#[tokio::test]
async fn exhausted_ordinary_unknown_event_is_discarded_without_lost() {
    let engine = fake_engine().await;
    let broker = broker(&engine).await;
    assert!(broker.session().await.unwrap().alive);
    let budget = broker.payload_budget();
    let baseline = budget.used();
    let held = budget
        .try_acquire(crate::transport::ORDINARY_BYTES - baseline.0, false)
        .unwrap();
    assert!(budget.try_acquire(1, false).is_none());
    assert_eq!(
        receive_ack(&broker, r#"{"method":"Unknown.opaque","params":{}}"#).await,
        (true, 0)
    );
    assert_eq!(budget.used().1, baseline.1);
    drop(held);
    assert_eq!(budget.used(), baseline);
    broker.engine_lost();
    assert!(!broker.wait_terminal().await);
    engine.kill();
}

#[tokio::test]
async fn exhausted_ordinary_discards_unowned_event_families_without_broadcast() {
    let engine = fake_engine().await;
    let broker = broker(&engine).await;
    let mut alice = connect(&broker, Endpoint::Browser, Role::Viewer, "alice").await;
    let mut bob = connect(&broker, Endpoint::Browser, Role::Viewer, "bob").await;
    let budget = broker.payload_budget();
    let baseline = budget.used();
    let held = budget
        .try_acquire(crate::transport::ORDINARY_BYTES - baseline.0, false)
        .unwrap();
    for text in [
        r#"{"method":"Runtime.consoleAPICalled","params":{"secret":"private"}}"#,
        r#"{"method":"Network.requestWillBeSent"}"#,
        r#"{"method":"Runtime.exceptionThrown"}"#,
        r#"{"method":"Kitesurf.scene"}"#,
        r#"{"method":"Target.targetCreated"}"#,
        r#"{"method":"Target.targetInfoChanged"}"#,
        r#"{"method":"Target.targetDestroyed"}"#,
        r#"{"method":"Unknown.opaque","\u006dethod":"Unknown.\u006fpaque"}"#,
        r#"{"method":"Runtime.consoleAPICalled","sessionId":"old","session\u0049d":"missing"}"#,
        r#"{"method":"Target.attachedToTarget","params":{"sessionId":"ghost","targetInfo":{"targetId":"ghost"}}}"#,
    ] {
        assert!(budget.try_acquire(1, false).is_none());
        assert_eq!(receive_ack(&broker, text).await, (true, 0));
        assert!(alice.frames.try_recv().is_err());
        assert!(bob.frames.try_recv().is_err());
        assert!(!*alice.cancel.borrow());
        assert!(!*bob.cancel.borrow());
        assert_eq!(budget.used().1, baseline.1);
    }
    drop(held);
    assert_eq!(budget.used(), baseline);
    assert!(budget.high_water().1 <= crate::transport::INTERNAL_BYTES);
    broker.engine_lost();
    assert!(!broker.wait_terminal().await);
    engine.kill();
}

#[tokio::test]
async fn exhausted_ordinary_escaped_detach_and_runtime_isolate_slow_owner() {
    let mut engine = fake_engine().await;
    let broker = broker(&engine).await;
    let mut alice = connect(&broker, Endpoint::Browser, Role::Viewer, "alice").await;
    let mut bob = connect(&broker, Endpoint::Browser, Role::Viewer, "bob").await;
    let mut healthy = connect(&broker, Endpoint::Browser, Role::Viewer, "healthy").await;
    for (client, session) in [(&mut alice, "known😀"), (&mut bob, "other")] {
        send(
            client,
            &broker,
            r#"{"id":1,"method":"Target.attachToTarget","params":{"targetId":"T"}}"#,
        )
        .await;
        let attach = next_request(&mut engine).await;
        engine.reply(request_id(&attach), json!({"sessionId": session}));
        assert_eq!(next_frame(client).await["id"], 1);
    }
    let budget = broker.payload_budget();
    let held = budget
        .try_acquire(crate::transport::ORDINARY_BYTES - budget.used().0, false)
        .unwrap();
    assert_eq!(receive_ack(&broker, r#"{"method":"Runtime.consoleAPICalled","sessionId":"known\ud83d\ude00","session\u0049d":"missing"}"#).await, (true, 2));
    assert!(!*alice.cancel.borrow());
    assert!(!*bob.cancel.borrow());
    assert_eq!(receive_ack(&broker, r#"{"method":"wrong","\u006dethod":"Target.detachedFromTarget","params":{"sessionId":"wrong","\u0073essionId":"known\ud83d\ude00"}}"#).await, (true, 1));
    assert!(matches!(
        alice.frames.try_recv(),
        Ok(ClientCommand::Close { code: 1011, .. })
    ));
    assert!(!*bob.cancel.borrow());
    assert!(bob.frames.try_recv().is_err());
    // detach 释放少量 metadata；继续填满，固定后续输出失败而不是依赖 payload 大小。
    let rest = budget
        .try_acquire(crate::transport::ORDINARY_BYTES - budget.used().0, false)
        .unwrap();
    assert_eq!(
        receive_ack(
            &broker,
            r#"{"method":"Runtime.consoleAPICalled","sessionId":"other"}"#
        )
        .await,
        (true, 1)
    );
    assert!(matches!(
        bob.frames.try_recv(),
        Ok(ClientCommand::Close { code: 1011, .. })
    ));
    assert!(alice.frames.try_recv().is_err());
    assert!(healthy.frames.try_recv().is_err());
    assert!(!*healthy.cancel.borrow());
    assert!(broker.session().await.unwrap().alive);
    drop(rest);
    drop(held);
    let detach = next_method(&mut engine, "Target.detachFromTarget").await;
    assert_eq!(detach["params"]["sessionId"], "other");
    assert_eq!(
        receive_ack(
            &broker,
            &format!(r#"{{"id":{},"result":{{}}}}"#, request_id(&detach))
        )
        .await,
        (true, 0)
    );
    assert_eq!(broker.budget_snapshot().await.3, 0);
    assert!(budget.high_water().1 <= crate::transport::INTERNAL_BYTES);
    broker.engine_lost();
    assert!(!broker.wait_terminal().await);
    engine.kill();
}

#[tokio::test]
async fn exhausted_ordinary_sent_attach_retains_cleanup_until_detach_reply() {
    let mut engine = fake_engine().await;
    let broker = broker(&engine).await;
    let mut client = connect(&broker, Endpoint::Browser, Role::Viewer, "viewer").await;
    send(
        &client,
        &broker,
        r#"{"id":1,"method":"Target.attachToTarget","params":{"targetId":"T"}}"#,
    )
    .await;
    let attach = next_request(&mut engine).await;
    let budget = broker.payload_budget();
    let baseline = budget.used().1;
    let held = budget
        .try_acquire(crate::transport::ORDINARY_BYTES - budget.used().0, false)
        .unwrap();
    assert_eq!(
        receive_ack(
            &broker,
            &format!(
                r#"{{"id":{},"result":{{"sessionId":"retained"}}}}"#,
                request_id(&attach)
            )
        )
        .await,
        (true, 1)
    );
    assert!(matches!(
        client.frames.try_recv(),
        Ok(ClientCommand::Close { code: 1011, .. })
    ));
    let detach = next_method(&mut engine, "Target.detachFromTarget").await;
    assert_eq!(detach["params"]["sessionId"], "retained");
    assert!(budget.used().1 > baseline);
    assert_eq!(
        receive_ack(
            &broker,
            &format!(r#"{{"id":{},"result":{{}}}}"#, request_id(&detach))
        )
        .await,
        (true, 0)
    );
    assert_eq!(broker.budget_snapshot().await, (0, 0, 0, 0));
    assert_eq!(budget.used().1, baseline);
    println!(
        "sent attach cleanup ordinary/internal={:?}; high={:?}",
        budget.used(),
        budget.high_water()
    );
    assert!(budget.high_water().1 <= crate::transport::INTERNAL_BYTES);
    drop(held);
    broker.engine_lost();
    assert!(!broker.wait_terminal().await);
    engine.kill();
}

#[tokio::test]
async fn borrowed_event_routes_keep_intent_parent_and_auto_owner_priority() {
    let mut engine = fake_engine().await;
    let broker = broker(&engine).await;
    let mut alice = connect(&broker, Endpoint::Browser, Role::Controller, "alice").await;
    let mut bob = connect(&broker, Endpoint::Browser, Role::Viewer, "bob").await;
    send(
        &alice,
        &broker,
        r#"{"id":1,"method":"Target.setAutoAttach","params":{"autoAttach":true}}"#,
    )
    .await;
    let auto = next_request(&mut engine).await;
    engine.reply(request_id(&auto), json!({}));
    let targets = next_method(&mut engine, "Target.getTargets").await;
    engine.reply(request_id(&targets), json!({"targetInfos":[]}));
    assert_eq!(next_frame(&mut alice).await["id"], 1);
    send(
        &bob,
        &broker,
        r#"{"id":2,"method":"Target.attachToTarget","params":{"targetId":"explicit"}}"#,
    )
    .await;
    let attach = next_request(&mut engine).await;
    assert_eq!(receive_ack(&broker, r#"{"method":"Target.attachedToTarget","params":{"sessionId":"parent😀","targetInfo":{"targetId":"explicit"}}}"#).await, (true, 1));
    assert_eq!(
        next_frame(&mut bob).await["params"]["sessionId"],
        "parent😀"
    );
    engine.reply(request_id(&attach), json!({"sessionId":"parent😀"}));
    assert_eq!(next_frame(&mut bob).await["id"], 2);
    assert_eq!(receive_ack(&broker, r#"{"method":"Target.attachedToTarget","sessionId":"wrong","session\u0049d":"parent\ud83d\ude00","params":{"sessionId":"child","targetInfo":{"targetId":"child-target"}}}"#).await, (true, 2));
    assert_eq!(next_frame(&mut bob).await["params"]["sessionId"], "child");
    assert!(alice.frames.try_recv().is_err());
    assert_eq!(receive_ack(&broker, r#"{"method":"Target.attachedToTarget","params":{"sessionId":"auto","targetInfo":{"targetId":"auto-target"}}}"#).await, (true, 3));
    assert_eq!(next_frame(&mut alice).await["params"]["sessionId"], "auto");
    assert!(bob.frames.try_recv().is_err());
    assert_eq!(
        receive_ack(
            &broker,
            r#"{"method":"Runtime.consoleAPICalled","sessionId":"child"}"#
        )
        .await,
        (true, 3)
    );
    assert_eq!(next_frame(&mut bob).await["sessionId"], "child");
    assert!(alice.frames.try_recv().is_err());
    broker.engine_lost();
    assert!(!broker.wait_terminal().await);
    engine.kill();
}

#[tokio::test]
async fn exhausted_ordinary_true_identity_limit_is_not_silently_discarded() {
    let engine = fake_engine().await;
    let broker = broker(&engine).await;
    let budget = broker.payload_budget();
    let held = budget
        .try_acquire(crate::transport::ORDINARY_BYTES - budget.used().0, false)
        .unwrap();
    let text = format!(r#"{{"method":"{}"}}"#, "x".repeat(4097));
    assert!(!receive_ack(&broker, &text).await.0);
    assert_eq!(
        broker.lost_watch().borrow().as_deref(),
        Some("upstream identity limit exceeded")
    );
    drop(held);
    assert!(!broker.wait_terminal().await);
    engine.kill();
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
        SessionInfo::from_raw(
            &crate::cdp_raw::LeasedRaw::literal(
                r#"{"sessionId":"fake-session","buildVersion":"test","environment":"local"}"#,
                &broker.payload_budget(),
            )
            .unwrap(),
            &broker.payload_budget(),
            "test",
        )
        .unwrap(),
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
    assert_eq!(serde_json::to_value(&status).unwrap()["revision"], 0);
    send(&alice, &broker, r#"{"id":2,"method":"Target.getTargets"}"#).await;
    let request = next_request(&mut engine).await;
    engine.reply(request_id(&request), json!({ "targetInfos": [] }));
    let reply = next_frame(&mut alice).await;
    assert_eq!(reply["id"], 2);
}

struct GatedStatus {
    inner: Arc<dyn crate::services::ProfileBackend>,
    entered: tokio::sync::Notify,
    resume: std::sync::Mutex<bool>,
    gate: std::sync::Condvar,
}

impl crate::services::ProfileBackend for GatedStatus {
    fn load(
        &self,
        owner: &[u16],
    ) -> Result<crate::services::LoadResult, crate::profile::ProfileError> {
        self.inner.load(owner)
    }
    fn save(
        &self,
        owner: &[u16],
        revision: &crate::profile_state::Json,
        state: Option<&crate::profile_state::Json>,
    ) -> Result<(), crate::profile::ProfileError> {
        self.inner.save(owner, revision, state)
    }
    fn status(&self) -> Result<Value, crate::profile::ProfileError> {
        panic!("broker must use typed status, never Value")
    }
    fn leased_status(
        &self,
        budget: &crate::transport::PayloadBudget,
    ) -> Result<crate::services::LeasedProfileStatus, crate::profile::ProfileError> {
        let status = self.inner.leased_status(budget)?;
        self.entered.notify_one();
        let guard = self.resume.lock().unwrap();
        let (guard, waited) = self
            .gate
            .wait_timeout_while(guard, WAIT, |resume| !*resume)
            .unwrap();
        assert!(!waited.timed_out(), "status test gate timed out");
        drop(guard);
        Ok(status)
    }
    fn kind(&self) -> &'static str {
        self.inner.kind()
    }
    fn display(&self) -> String {
        self.inner.display()
    }
}

#[tokio::test]
async fn typed_status_callbacks_cancel_lost_and_exhaustion_release_both_profiles() {
    use crate::transport::{MAX_FRAME_BYTES, ORDINARY_BYTES};
    for sqlite in [false, true] {
        for lost in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let inner: Arc<dyn crate::services::ProfileBackend> = if sqlite {
                Arc::new(
                    crate::services::SqliteProfile::open(&directory.path().join("状态.db"), true)
                        .unwrap(),
                )
            } else {
                Arc::new(MemoryProfile::new("http://fixture.invalid").unwrap())
            };
            let expected = inner.status().unwrap();
            let gated = Arc::new(GatedStatus {
                inner,
                entered: tokio::sync::Notify::new(),
                resume: std::sync::Mutex::new(false),
                gate: std::sync::Condvar::new(),
            });
            let mut engine = fake_engine().await;
            let observer = Arc::new(Observer::disabled());
            let upstream =
                Upstream::connect(&UpstreamAddress::Tcp(engine.address), observer.clone())
                    .await
                    .unwrap();
            let broker = BrokerHandle::start(
                upstream,
                Arc::new(ProfileService::new(gated.clone())),
                observer,
            );
            let budget = broker.payload_budget();
            assert!(broker.session().await.unwrap().alive);
            let baseline = budget.used();
            let flushing = {
                let broker = broker.clone();
                tokio::spawn(async move { broker.flush().await })
            };
            let flush = next_method(&mut engine, "Kitesurf.flushProfile").await;
            engine.reply(request_id(&flush), json!({}));
            timeout(WAIT, gated.entered.notified()).await.unwrap();
            assert!(budget.used().0 > baseline.0);
            assert!(
                budget.used().1 <= baseline.1,
                "status cannot borrow internal reserve"
            );
            assert!(
                broker.session().await.unwrap().alive,
                "blocking status must not block actor"
            );
            flushing.abort();
            assert!(matches!(flushing.await, Err(error) if error.is_cancelled()));
            if lost {
                broker.engine_lost();
                assert!(!timeout(WAIT, broker.wait_terminal()).await.unwrap());
            }
            *gated.resume.lock().unwrap() = true;
            gated.gate.notify_all();
            timeout(WAIT, async {
                loop {
                    let used = budget.used();
                    if used.0 == baseline.0 && (!lost || used.1 == 0) {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            if !lost {
                let held = budget
                    .try_acquire(ORDINARY_BYTES - budget.used().0, false)
                    .unwrap();
                let flushing = {
                    let broker = broker.clone();
                    tokio::spawn(async move { broker.flush().await })
                };
                let flush = next_method(&mut engine, "Kitesurf.flushProfile").await;
                engine.reply(request_id(&flush), json!({}));
                let error = timeout(WAIT, flushing)
                    .await
                    .unwrap()
                    .unwrap()
                    .err()
                    .unwrap();
                assert_eq!(error, "Profile status unavailable");
                assert!(broker.session().await.unwrap().alive);
                assert_eq!(budget.used().0, ORDINARY_BYTES);
                drop(held);
                let flushing = {
                    let broker = broker.clone();
                    tokio::spawn(async move { broker.flush().await })
                };
                let flush = next_method(&mut engine, "Kitesurf.flushProfile").await;
                engine.reply(request_id(&flush), json!({}));
                let status = timeout(WAIT, flushing).await.unwrap().unwrap().unwrap();
                assert_eq!(serde_json::to_value(&status).unwrap(), expected);
                drop(status);
                broker.engine_lost();
                assert!(!timeout(WAIT, broker.wait_terminal()).await.unwrap());
            }
            engine.kill();
            timeout(WAIT, async {
                while budget.used() != (MAX_FRAME_BYTES, 0) {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert_eq!(budget.used(), (MAX_FRAME_BYTES, 0));
        }
    }
}

#[tokio::test]
async fn typed_profile_http_status_flush_fields_and_exhaustion_stay_compatible() {
    use crate::relay::{HostServer, SessionInfo};
    use crate::services::{ProfileBackend, SqliteProfile};
    use crate::transport::{MAX_FRAME_BYTES, ORDINARY_BYTES};
    for sqlite in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let backend: Arc<dyn ProfileBackend> = if sqlite {
            Arc::new(SqliteProfile::open(&directory.path().join("状态.db"), true).unwrap())
        } else {
            Arc::new(MemoryProfile::new("http://fixture.invalid").unwrap())
        };
        let expected = backend.status().unwrap();
        let profile = Arc::new(ProfileService::new(backend));
        let mut engine = fake_engine().await;
        let observer = Arc::new(Observer::disabled());
        let upstream = Upstream::connect(&UpstreamAddress::Tcp(engine.address), observer.clone())
            .await
            .unwrap();
        let broker = BrokerHandle::start(upstream, profile.clone(), observer.clone());
        let budget = broker.payload_budget();
        let raw = crate::cdp_raw::LeasedRaw::literal("{}", &budget).unwrap();
        let session = SessionInfo::from_raw(&raw, &budget, "test").unwrap();
        drop(raw);
        let host = HostServer::bind(
            0,
            broker.clone(),
            session,
            "generation".into(),
            "recovery".into(),
            profile,
            observer,
        )
        .unwrap();
        let request = tokio::spawn(status_http(host.port(), false));
        let targets = next_method(&mut engine, "Target.getTargets").await;
        engine.reply(request_id(&targets), json!({"targetInfos":[]}));
        let (code, body) = timeout(WAIT, request).await.unwrap().unwrap();
        assert_eq!(code, "HTTP/1.1 200 OK");
        assert_eq!(body["profile"], expected);
        let held = budget
            .try_acquire(ORDINARY_BYTES - budget.used().0, false)
            .unwrap();
        let request = tokio::spawn(status_http(host.port(), true));
        let flush = next_method(&mut engine, "Kitesurf.flushProfile").await;
        engine.reply(request_id(&flush), json!({}));
        let (code, body) = timeout(WAIT, request).await.unwrap().unwrap();
        assert_eq!(code, "HTTP/1.1 409 Conflict");
        assert_eq!(body, json!({"error":"Profile status unavailable"}));
        assert!(broker.session().await.unwrap().alive);
        drop(held);
        let request = tokio::spawn(status_http(host.port(), true));
        let flush = next_method(&mut engine, "Kitesurf.flushProfile").await;
        engine.reply(request_id(&flush), json!({}));
        let (code, body) = timeout(WAIT, request).await.unwrap().unwrap();
        assert_eq!(code, "HTTP/1.1 200 OK");
        assert_eq!(body, expected);
        broker.engine_lost();
        assert!(!timeout(WAIT, broker.wait_terminal()).await.unwrap());
        host.stop().await;
        drop(host);
        engine.kill();
        timeout(WAIT, async {
            while budget.used() != (MAX_FRAME_BYTES, 0) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}

async fn status_http(port: u16, flush: bool) -> (String, Value) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let route = if flush { "POST /flush" } else { "GET /session" };
    stream.write_all(format!("{route} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
    let mut bytes = Vec::new();
    timeout(WAIT, stream.read_to_end(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    let text = String::from_utf8(bytes).unwrap();
    let (head, body) = text.split_once("\r\n\r\n").unwrap();
    (
        head.lines().next().unwrap().to_owned(),
        serde_json::from_str(body).unwrap(),
    )
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
