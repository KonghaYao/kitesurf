// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

#[allow(dead_code)]
#[path = "../src/transport.rs"]
mod transport;
#[allow(dead_code)]
#[path = "../src/cdp_raw.rs"]
mod cdp_raw;
use kitesurf_local_host::{observe, HostResult};
use cdp_raw::*;
use transport::{PayloadBudget, MAX_FRAME_BYTES, ORDINARY_BYTES};

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::RefCell;
use std::fmt;

use serde::de::{MapAccess, Visitor};
use serde::Deserializer;
use serde_json::value::RawValue;

#[derive(Clone, Copy, Default, Debug)]
struct Stats {
    live: usize,
    peak: usize,
    largest: usize,
    allocations: usize,
    overflow: bool,
}

struct Tracking {
    active: bool,
    stats: Stats,
    slots: [(usize, usize); 1024],
}

impl Tracking {
    const fn new() -> Self {
        Self {
            active: false,
            stats: Stats {
                live: 0,
                peak: 0,
                largest: 0,
                allocations: 0,
                overflow: false,
            },
            slots: [(0, 0); 1024],
        }
    }

    fn add(&mut self, ptr: *mut u8, size: usize) {
        if !self.active || ptr.is_null() {
            return;
        }
        self.stats.live += size;
        self.stats.peak = self.stats.peak.max(self.stats.live);
        self.stats.largest = self.stats.largest.max(size);
        self.stats.allocations += 1;
        if let Some(slot) = self.slots.iter_mut().find(|slot| slot.0 == 0) {
            *slot = (ptr as usize, size);
        } else {
            self.stats.overflow = true;
        }
    }

    fn remove(&mut self, ptr: *mut u8) {
        if self.slots.iter().all(|slot| slot.0 == 0) {
            return;
        }
        if let Some(slot) = self.slots.iter_mut().find(|slot| slot.0 == ptr as usize) {
            self.stats.live -= slot.1;
            *slot = (0, 0);
        }
    }
}

thread_local! {
    static TRACKING: RefCell<Tracking> = const { RefCell::new(Tracking::new()) };
}

struct TrackingAllocator;

// unsafe 仅在测试 allocator：观测当前测试线程的全部大小，不调用分配型日志。
// realloc 按旧新同时存活计峰；固定槽只服务观测，溢出会使测试失败。
unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc(layout);
        let _ = TRACKING.try_with(|state| state.borrow_mut().add(ptr, layout.size()));
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        let _ = TRACKING.try_with(|state| state.borrow_mut().remove(ptr));
        System.dealloc(ptr, layout);
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let new = System.realloc(ptr, layout, size);
        if !new.is_null() {
            let _ = TRACKING.try_with(|state| {
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
static ALLOCATOR: TrackingAllocator = TrackingAllocator;

fn measure(f: impl FnOnce()) -> Stats {
    TRACKING.with(|state| {
        let mut state = state.borrow_mut();
        assert!(!state.active);
        *state = Tracking::new();
        state.active = true;
    });
    f();
    let stats = TRACKING.with(|state| {
        let mut state = state.borrow_mut();
        state.active = false;
        state.stats
    });
    assert!(!stats.overflow);
    stats
}

#[test]
fn probe_pinned_serde_behavior_and_raw_scratch() {
    struct Keys;
    impl<'de> Visitor<'de> for Keys {
        type Value = (usize, bool);
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("object")
        }
        fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
            let mut count = 0;
            let mut escaped = false;
            while let Some(key) = map.next_key::<&RawValue>()? {
                escaped |= key.get() == r#""sess\u0069onId""#;
                let _: &RawValue = map.next_value()?;
                count += 1;
            }
            Ok((count, escaped))
        }
    }
    let mut de = serde_json::Deserializer::from_str(r#"{"sess\u0069onId":1,"unknown":2}"#);
    assert_eq!(de.deserialize_map(Keys).unwrap(), (2, true));
    de.end().unwrap();
    for text in ["1e400", r#""\uD800""#] {
        assert!(serde_json::from_str::<&RawValue>(text).is_ok());
        assert!(serde_json::from_str::<serde_json::Value>(text).is_err());
    }
    for depth in [126, 127, 128, 129] {
        let text = format!("{}0{}", "[".repeat(depth), "]".repeat(depth));
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&text).is_ok(),
            depth < 128
        );
        assert!(serde_json::from_str::<&RawValue>(&text).is_ok());
    }
    let depth = 8 * 1024 * 1024 - 1;
    let text = format!("{}0{}", "[".repeat(depth), "]".repeat(depth));
    let stats = measure(|| {
        assert!(serde_json::from_str::<&RawValue>(&text).is_ok());
    });
    println!("pinned raw scratch: {stats:?}");
    assert_eq!(stats.live, 0);
    assert_eq!(stats.largest, 8 * 1024 * 1024);
    assert_eq!(stats.peak, 12 * 1024 * 1024);
}

fn owned(text: String, budget: &PayloadBudget) -> LeasedRaw {
    let lease = budget.try_acquire(text.capacity(), false).unwrap();
    LeasedRaw::new(text, lease).unwrap()
}

fn parses(text: &str, policy: ParsePolicy) -> bool {
    let budget = PayloadBudget::default();
    let backing = owned(text.into(), &budget);
    let result = RawEnvelope::parse(backing, policy, &mut budget.json_scratch()).is_ok();
    assert_eq!(budget.used().0, MAX_FRAME_BYTES);
    result
}

#[derive(serde::Deserialize)]
#[allow(dead_code)]
struct OldClient<'a> {
    #[serde(borrow)]
    id: Option<&'a RawValue>,
    #[serde(borrow)]
    method: Option<&'a RawValue>,
    #[serde(rename = "sessionId", borrow)]
    session: Option<&'a RawValue>,
    #[serde(borrow)]
    params: Option<&'a RawValue>,
}

#[test]
fn policies_differential_numbers_unicode_depth_and_malformed() {
    let values = [
        "0",
        "-0",
        "1.0",
        "1e0",
        "9007199254740991",
        "9007199254740992",
        "-9007199254740991",
        "-9007199254740992",
        "18446744073709551615",
        "18446744073709551616",
        "-9223372036854775808",
        "-9223372036854775809",
        "1e400",
        "1e-400",
        "0e999999999",
        "1e999999999",
        "01",
        "1.",
        "--1",
        r#""\uD800""#,
        r#""\uDC00""#,
        r#""\uD800\uDC00""#,
        r#""\uZZZZ""#,
        r#""\q""#,
        "\"a\nb\"",
        "\"中文🪁\"",
        "[1,]",
        "{\"a\":}",
        "true false",
        "[}",
        "[[",
        "null",
        "false",
        "{}",
        "[]",
    ];
    for value in values {
        let text = format!(r#"{{"id":1,"method":"x","params":{value}}}"#);
        assert_eq!(
            parses(&text, ParsePolicy::ClientOpaque),
            serde_json::from_str::<OldClient<'_>>(&text).is_ok(),
            "client {value}"
        );
        assert_eq!(
            parses(&text, ParsePolicy::UpstreamStrict),
            serde_json::from_str::<serde_json::Value>(&text).is_ok(),
            "upstream {value}"
        );
    }
    for depth in [125, 126, 127, 128, 4096] {
        let value = format!("{}0{}", "[".repeat(depth), "]".repeat(depth));
        let text = format!(r#"{{"params":{value}}}"#);
        assert_eq!(
            parses(&text, ParsePolicy::UpstreamStrict),
            serde_json::from_str::<serde_json::Value>(&text).is_ok()
        );
        assert_eq!(
            parses(&text, ParsePolicy::ClientOpaque),
            serde_json::from_str::<OldClient<'_>>(&text).is_ok()
        );
    }
    for text in [
        r#"{"x":1} trailing"#,
        r#"{"x":1,}"#,
        "[]",
        "null",
        "{",
        "[}",
    ] {
        assert!(!parses(text, ParsePolicy::ClientOpaque));
        assert!(!parses(text, ParsePolicy::UpstreamStrict));
    }
    let unusual = [
        r#"{"\u006dy":1,"\u006dethod":"x"}"#,
        r#"{"\uD800":0}"#,
        r#"{"\uD83E\uDE81":0}"#,
        r#"{"unknown":"\uD800"}"#,
        r#"{"\uZZZZ":0}"#,
        r#"{"\uD800\u12中":0}"#,
    ];
    for text in unusual {
        assert_eq!(
            parses(text, ParsePolicy::ClientOpaque),
            serde_json::from_str::<OldClient<'_>>(text).is_ok(),
            "client key {text}"
        );
        assert_eq!(
            parses(text, ParsePolicy::UpstreamStrict),
            serde_json::from_str::<serde_json::Value>(text).is_ok(),
            "upstream key {text}"
        );
    }
    let invalid = vec![b'{', b'"', 0xff, b'"', b':', b'0', b'}'];
    assert!(String::from_utf8(invalid).is_err());
}

#[test]
fn scalar_ids_match_actual_old_accessors() {
    let budget = PayloadBudget::default();
    let mut scratch = budget.json_scratch();
    for text in [
        "-0",
        "1.0",
        "1e0",
        "0",
        "-1",
        "9007199254740991",
        "9007199254740992",
        "18446744073709551615",
        "-9223372036854775808",
        "1e-400",
        "1e400",
        "null",
        "true",
    ] {
        let raw: &RawValue = serde_json::from_str(text).unwrap();
        let value = serde_json::from_str::<serde_json::Value>(text).ok();
        let old_safe = value.as_ref().and_then(|value| {
            if let Some(n) = value.as_i64() {
                return (n.unsigned_abs() <= 9_007_199_254_740_991).then_some(n);
            }
            let n = value.as_f64()?;
            (n.fract() == 0.0 && n.abs() <= 9_007_199_254_740_991f64).then_some(n as i64)
        });
        assert_eq!(safe_integer(raw, &mut scratch), old_safe, "{text}");
        assert_eq!(
            upstream_id(raw, &mut scratch),
            value.as_ref().and_then(serde_json::Value::as_u64),
            "{text}"
        );
    }
}

#[test]
fn escaped_keys_duplicates_presence_and_fixed_selection() {
    let budget = PayloadBudget::default();
    let mut scratch = budget.json_scratch();
    for text in [
        r#"{"id":1,"\u0069d":2}"#,
        r#"{"params":{},"params":{}}"#,
        r#"{"id":1,"id":null}"#,
    ] {
        assert!(serde_json::from_str::<OldClient<'_>>(text).is_err());
        assert!(RawEnvelope::parse(
            owned(text.into(), &budget),
            ParsePolicy::ClientOpaque,
            &mut scratch
        )
        .is_err());
    }
    for text in [r#"{"id":null,"id":1}"#, r#"{"params":null,"params":{}}"#] {
        assert!(serde_json::from_str::<OldClient<'_>>(text).is_err());
        assert!(RawEnvelope::parse(
            owned(text.into(), &budget),
            ParsePolicy::ClientOpaque,
            &mut scratch
        )
        .is_err());
    }
    let text = r#"{"id":1,"\u0069d":2,"sessionId":"a","sess\u0069onId":"b","error":null,"params":{"targetId":"a","target\u0049d":"b"},"$serde_json::private::RawValue":"not special"}"#;
    let envelope = RawEnvelope::parse(
        owned(text.into(), &budget),
        ParsePolicy::UpstreamStrict,
        &mut scratch,
    )
    .unwrap();
    assert_eq!(
        envelope
            .backing()
            .text(envelope.field("id").span().unwrap())
            .unwrap(),
        "2"
    );
    assert!(matches!(envelope.field("error"), RawPresence::Null(_)));
    assert_eq!(envelope.field("result"), RawPresence::Missing);
    let params = envelope.field("params").span().unwrap();
    assert!(select_fields(
        envelope.backing(),
        params,
        ["targetId"],
        DuplicatePolicy::Reject,
        &mut scratch
    )
    .is_err());
    let fields = select_fields(
        envelope.backing(),
        params,
        ["targetId"],
        DuplicatePolicy::LastWins,
        &mut scratch,
    )
    .unwrap();
    assert_eq!(
        envelope.backing().text(fields[0].span().unwrap()).unwrap(),
        r#""b""#
    );
    let mut sessions = 0;
    let mut sentinel = 0;
    visit_object(
        envelope.backing(),
        envelope.backing().full_span(),
        &mut scratch,
        |key, _, text| {
            sessions += usize::from(text.equals(key, "sessionId")?);
            sentinel += usize::from(text.equals(key, "$serde_json::private::RawValue")?);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(sessions, 2);
    assert_eq!(sentinel, 1);
}

#[test]
fn chunk_boundaries_identity_and_shared_owner_leases() {
    let budget = PayloadBudget::default();
    let mut scratch = budget.json_scratch();
    for padding in 4080..4100 {
        let decoded = format!("{}🪁\"\\中文", "a".repeat(padding));
        let text = format!(r#""{}\uD83E\uDE81\"\\中文""#, "a".repeat(padding));
        let backing = owned(text, &budget);
        let raw = backing.raw(backing.full_span(), &mut scratch).unwrap();
        let mut matches = false;
        let object = owned(format!("{{{}:0}}", raw.get()), &budget);
        visit_object(&object, object.full_span(), &mut scratch, |key, _, text| {
            matches = text.equals(key, &decoded)?;
            Ok(())
        })
        .unwrap();
        assert!(matches);
    }
    let text = format!(r#""{}""#, r"\u0061".repeat(4096));
    let backing = owned(text, &budget);
    let raw = backing.raw(backing.full_span(), &mut scratch).unwrap();
    let identity = decode_identity(raw, &budget, &mut scratch).unwrap();
    assert_eq!(identity.as_str().len(), 4096);
    let shared = identity.clone();
    drop(identity);
    assert_eq!(shared.as_str().len(), 4096);
    drop(shared);
    let raw: &RawValue = serde_json::from_str(r#""\uD800""#).unwrap();
    assert!(decode_identity(raw, &budget, &mut scratch).is_err());
    let large = format!(r#""{}""#, "x".repeat(4097));
    let raw = serde_json::from_str(&large).unwrap();
    assert!(decode_identity(raw, &budget, &mut scratch).is_err());
    let copy = backing
        .copy_span_to_ordinary(backing.full_span(), &budget, &mut scratch)
        .unwrap();
    let shared = copy.clone();
    let ptr = copy.as_str().as_ptr();
    drop(copy);
    assert_eq!(shared.as_str().as_ptr(), ptr);
    drop(shared);
    drop(backing);
    assert_eq!(budget.used().0, MAX_FRAME_BYTES);
}

#[test]
fn large_payloads_keys_and_many_nodes_have_bounded_live_allocations() {
    let budget = PayloadBudget::default();
    drop(budget.json_scratch());
    for field in ["params", "result"] {
        let mut text = format!(r#"{{"{field}":""#);
        while text.len() + 8 <= MAX_FRAME_BYTES {
            text.push_str(r"\u0061");
        }
        text.push_str("\"}");
        let backing = owned(text, &budget);
        let stats = measure(|| {
            let envelope = RawEnvelope::parse(
                backing.clone(),
                ParsePolicy::UpstreamStrict,
                &mut budget.json_scratch(),
            )
            .unwrap();
            drop(envelope);
        });
        println!("escaped {field}: {stats:?}");
        assert_eq!(stats.live, 0);
        assert!(stats.peak <= 64 * 1024);
    }
    let key = r"\u0061".repeat((MAX_FRAME_BYTES - 32) / 6);
    let backing = owned(format!(r#"{{"{key}":0,"method":"x"}}"#), &budget);
    let stats = measure(|| {
        assert!(RawEnvelope::parse(
            backing.clone(),
            ParsePolicy::ClientOpaque,
            &mut budget.json_scratch()
        )
        .is_ok());
    });
    println!("escaped long key: {stats:?}");
    assert_eq!(stats.live, 0);
    assert!(stats.peak <= 64 * 1024);
    drop(backing);
    for unit in ["{},", "[],", r#"{"x":0},"#] {
        let text = format!(
            r#"{{"params":[{}0]}}"#,
            unit.repeat((MAX_FRAME_BYTES - 32) / unit.len())
        );
        let backing = owned(text, &budget);
        let stats = measure(|| {
            assert!(RawEnvelope::parse(
                backing.clone(),
                ParsePolicy::UpstreamStrict,
                &mut budget.json_scratch()
            )
            .is_ok());
        });
        println!("many nodes {unit}: {stats:?}");
        assert_eq!(stats.live, 0);
        assert!(stats.peak <= 64 * 1024);
    }
    assert_eq!(budget.used().0, MAX_FRAME_BYTES);
}

#[test]
fn deep_client_and_errors_release_all_parser_allocations() {
    let budget = PayloadBudget::default();
    drop(budget.json_scratch());
    let depth = (MAX_FRAME_BYTES - 32) / 2;
    for suffix in ["", " trailing"] {
        let text = format!(
            r#"{{"params":{}0{}}}{suffix}"#,
            "[".repeat(depth),
            "]".repeat(depth)
        );
        let backing = owned(text, &budget);
        let stats = measure(|| {
            let parsed = RawEnvelope::parse(
                backing.clone(),
                ParsePolicy::ClientOpaque,
                &mut budget.json_scratch(),
            );
            assert_eq!(parsed.is_ok(), suffix.is_empty());
            drop(parsed);
        });
        println!("deep suffix {suffix:?}: {stats:?}");
        assert_eq!(stats.live, 0);
        assert!(stats.peak <= 12 * 1024 * 1024 + 64 * 1024);
    }
    let text = format!(r#"{{"params":{}"#, "[".repeat(MAX_FRAME_BYTES - 32));
    let backing = owned(text, &budget);
    let stats = measure(|| {
        assert!(RawEnvelope::parse(
            backing.clone(),
            ParsePolicy::ClientOpaque,
            &mut budget.json_scratch()
        )
        .is_err());
    });
    assert!(stats.peak < 64 * 1024);
    drop(backing);
    assert_eq!(budget.used().0, MAX_FRAME_BYTES);
}

#[test]
fn adversarial_chunks_keys_and_scalars_remain_bounded() {
    let budget = PayloadBudget::default();
    drop(budget.json_scratch());
    let mut scratch = budget.json_scratch();
    // owner token + 范围；后继 BackfillSelection 必须按 size_of 计费。
    assert_eq!(std::mem::size_of::<RawSpan>(), 16);
    let mut fields = String::with_capacity(MAX_FRAME_BYTES);
    fields.push('{');
    while fields.len() + 12 <= MAX_FRAME_BYTES {
        fields.push_str(r#""x":0,"#);
    }
    fields.push_str(r#""z":0}"#);
    let backing = owned(fields, &budget);
    let stats = measure(|| {
        assert!(
            RawEnvelope::parse(backing.clone(), ParsePolicy::UpstreamStrict, &mut scratch).is_ok()
        );
    });
    println!("many top-level fields: {stats:?}");
    assert_eq!(stats.live, 0);
    assert!(stats.peak <= 64 * 1024);
    drop(backing);
    for padding in 4080..4100 {
        let token = format!(r#""\n{}\uD83E\uDE81""#, "中".repeat(padding));
        let backing = owned(token, &budget);
        let raw = backing.raw(backing.full_span(), &mut scratch).unwrap();
        let stats = measure(|| {
            assert!(decode_identity(raw, &budget, &mut scratch).is_err());
            assert_eq!(safe_integer(raw, &mut scratch), None);
            assert_eq!(upstream_id(raw, &mut scratch), None);
        });
        assert_eq!(stats.live, 0);
        assert!(stats.largest <= 8192);
        assert!(stats.peak <= 12288 + 64 * 1024);
    }
    let token = format!(r#""\n{}""#, "x".repeat(MAX_FRAME_BYTES - 8));
    let backing = owned(token, &budget);
    let raw = backing.raw(backing.full_span(), &mut scratch).unwrap();
    let stats = measure(|| {
        assert_eq!(safe_integer(raw, &mut scratch), None);
        assert_eq!(upstream_id(raw, &mut scratch), None);
    });
    assert_eq!(stats.live, 0);
    assert_eq!(stats.allocations, 0);
    drop(backing);
    assert_eq!(budget.used().0, MAX_FRAME_BYTES);
}

#[test]
fn array_spans_fixed_output_and_failure_paths() {
    use std::cell::Cell;
    let budget = PayloadBudget::default();
    let mut scratch = budget.json_scratch();
    let backing = owned("[1,{\"x\":2},null]".into(), &budget);
    let mut selected = None;
    let mut count = 0;
    visit_array(&backing, backing.full_span(), &mut scratch, |raw| {
        count += 1;
        if raw.get().starts_with('{') {
            selected = Some(backing.span_of(raw)?);
        }
        Ok(())
    })
    .unwrap();
    assert_eq!(count, 3);
    let selected = selected.unwrap();
    let copy = backing
        .copy_span_to_ordinary(selected, &budget, &mut scratch)
        .unwrap();
    assert_eq!(copy.as_str(), r#"{"x":2}"#);
    drop(copy);
    struct Growing(Cell<usize>);
    impl serde::Serialize for Growing {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            let pass = self.0.get();
            self.0.set(pass + 1);
            serializer.serialize_str(if pass == 0 { "x" } else { "much longer" })
        }
    }
    let baseline = budget.used();
    let stats = measure(|| {
        assert!(
            serialize_payload_with_scratch(&Growing(Cell::new(0)), &budget, &mut scratch).is_err()
        );
    });
    assert_eq!(stats.live, 0);
    assert_eq!(budget.used(), baseline);
    let occupied = budget
        .try_acquire(ORDINARY_BYTES - baseline.0, false)
        .unwrap();
    let stats = measure(|| {
        assert!(backing
            .copy_span_to_ordinary(selected, &budget, &mut scratch)
            .is_err());
        let raw: &RawValue = serde_json::from_str(r#""abc""#).unwrap();
        assert!(decode_identity(raw, &budget, &mut scratch).is_err());
        assert!(serialize_payload_with_scratch(&"abc", &budget, &mut scratch).is_err());
    });
    assert_eq!(stats.live, 0);
    assert!(stats.largest <= 8192);
    drop(occupied);
    drop(backing);
    assert_eq!(budget.used().0, MAX_FRAME_BYTES);
}

#[test]
fn spans_are_bound_to_their_owning_backing() {
    let budget = PayloadBudget::default();
    let mut scratch = budget.json_scratch();
    let a = owned(r#"{"id":1,"params":{"targetId":"a"}}"#.into(), &budget);
    let b = owned(r#"{"id":9,"params":{"targetId":"b"}}"#.into(), &budget);
    let a_env = RawEnvelope::parse(a.clone(), ParsePolicy::ClientOpaque, &mut scratch).unwrap();
    let b_env = RawEnvelope::parse(b.clone(), ParsePolicy::ClientOpaque, &mut scratch).unwrap();
    let id = a_env.field("id").span().unwrap();
    let params = a_env.field("params").span().unwrap();
    assert_eq!(a_env.backing().text(id).unwrap(), "1");
    assert!(b_env.backing().text(id).is_err());
    assert!(b_env.backing().raw(id, &mut scratch).is_err());
    assert!(b_env
        .backing()
        .copy_span_to_ordinary(id, &budget, &mut scratch)
        .is_err());
    assert!(select_fields(
        b_env.backing(),
        params,
        ["targetId"],
        DuplicatePolicy::LastWins,
        &mut scratch
    )
    .is_err());
    assert!(visit_object(b_env.backing(), params, &mut scratch, |_, _, _| Ok(())).is_err());
    let arrays_a = owned(r#"{"params":[{"x":1}]}"#.into(), &budget);
    let arrays_b = owned(r#"{"params":[{"x":9}]}"#.into(), &budget);
    let arrays_a = RawEnvelope::parse(arrays_a, ParsePolicy::ClientOpaque, &mut scratch).unwrap();
    let arrays_b = RawEnvelope::parse(arrays_b, ParsePolicy::ClientOpaque, &mut scratch).unwrap();
    let array_span = arrays_a.field("params").span().unwrap();
    assert!(visit_array(arrays_b.backing(), array_span, &mut scratch, |_| Ok(())).is_err());
    // 同长度 full span：不同 owner 即同内容也拒绝。
    assert!(b_env
        .backing()
        .raw(a_env.backing().full_span(), &mut scratch)
        .is_err());
    let cloned = a.clone();
    assert_eq!(
        cloned
            .raw(a_env.backing().full_span(), &mut scratch)
            .unwrap()
            .get(),
        a.as_str()
    );
    // copy 产生新 owner，即使文本相同也双向拒绝。
    let copied = a
        .copy_span_to_ordinary(a.full_span(), &budget, &mut scratch)
        .unwrap();
    assert_eq!(copied.as_str(), a.as_str());
    assert!(a.raw(copied.full_span(), &mut scratch).is_err());
    assert!(copied.raw(a.full_span(), &mut scratch).is_err());
    assert_eq!(
        copied.raw(copied.full_span(), &mut scratch).unwrap().get(),
        a.as_str()
    );
    // UTF-8 多字节 child span 同 offset 也不同 owner。
    let utf8_a = owned(r#"{"id":"中"}"#.into(), &budget);
    let utf8_b = owned(r#"{"id":"文"}"#.into(), &budget);
    let utf8_a = RawEnvelope::parse(utf8_a, ParsePolicy::ClientOpaque, &mut scratch).unwrap();
    let utf8_b = RawEnvelope::parse(utf8_b, ParsePolicy::ClientOpaque, &mut scratch).unwrap();
    let utf8_span = utf8_a.field("id").span().unwrap();
    assert!(utf8_b.backing().raw(utf8_span, &mut scratch).is_err());
    assert_eq!(utf8_a.backing().text(utf8_span).unwrap(), r#""中""#);
    // 嵌套 visitor 保存的子 span 同样绑定原 owner。
    let nested_a = owned(r#"{"params":{"targetId":"a"}}"#.into(), &budget);
    let nested_b = owned(r#"{"params":{"targetId":"b"}}"#.into(), &budget);
    let nested_a = RawEnvelope::parse(nested_a, ParsePolicy::ClientOpaque, &mut scratch).unwrap();
    let nested_b = RawEnvelope::parse(nested_b, ParsePolicy::ClientOpaque, &mut scratch).unwrap();
    let nested_params = nested_a.field("params").span().unwrap();
    let mut child = None;
    visit_object(
        nested_a.backing(),
        nested_params,
        &mut scratch,
        |key, value, text| {
            if text.equals(key, "targetId")? {
                child = Some(nested_a.backing().span_of(value)?);
            }
            Ok(())
        },
    )
    .unwrap();
    let child = child.unwrap();
    assert!(nested_b.backing().raw(child, &mut scratch).is_err());
    assert_eq!(nested_a.backing().text(child).unwrap(), r#""a""#);
    // 原 owner 销毁后重建，旧 span 不得因地址复用而别名。
    let stale = {
        let old = owned(r#"{"id":7}"#.into(), &budget);
        let old_env = RawEnvelope::parse(old, ParsePolicy::ClientOpaque, &mut scratch).unwrap();
        old_env.field("id").span().unwrap()
    };
    let replacement = owned(r#"{"id":7}"#.into(), &budget);
    let replacement =
        RawEnvelope::parse(replacement, ParsePolicy::ClientOpaque, &mut scratch).unwrap();
    assert_eq!(replacement.backing().text(stale), Err(RawError::Invalid));
    assert_eq!(
        replacement
            .backing()
            .text(replacement.field("id").span().unwrap()),
        Ok("7")
    );
    // 失败后 scratch 仍可继续使用。
    assert_eq!(a_env.backing().raw(id, &mut scratch).unwrap().get(), "1");
}

#[test]
fn array_callbacks_preserve_raw_error_categories_and_resources() {
    let budget = PayloadBudget::default();
    let mut scratch = budget.json_scratch();
    let array = owned("[1,2,3]".into(), &budget);
    let object = owned(r#"{"a":1}"#.into(), &budget);
    let categories = [
        (RawError::Invalid, "invalid"),
        (RawError::Limit, "limit"),
        (RawError::Budget, "budget"),
        (RawError::Duplicate, "duplicate"),
    ];
    for (error, label) in categories {
        assert_eq!(
            visit_array(&array, array.full_span(), &mut scratch, |_| Err(error)),
            Err(error),
            "array {label}"
        );
        assert_eq!(
            visit_object(&object, object.full_span(), &mut scratch, |_, _, _| Err(
                error
            )),
            Err(error),
            "object {label}"
        );
    }
    // serde 自身格式错误仍是 Invalid。
    for text in ["[1,", "[1] trailing", "{}"] {
        let malformed = owned(text.into(), &budget);
        assert_eq!(
            visit_array(&malformed, malformed.full_span(), &mut scratch, |_| Ok(())),
            Err(RawError::Invalid),
            "{text}"
        );
    }
    for text in [r#"{"a":1"#, r#"{"a":1} trailing"#, "[]"] {
        let malformed = owned(text.into(), &budget);
        assert_eq!(
            visit_object(
                &malformed,
                malformed.full_span(),
                &mut scratch,
                |_, _, _| { Ok(()) }
            ),
            Err(RawError::Invalid),
            "{text}"
        );
    }
    // 回调错误优先于 trailing 语法错误，成功回调不吞 trailing。
    let trailing = owned("[1] trailing".into(), &budget);
    assert_eq!(
        visit_array(&trailing, trailing.full_span(), &mut scratch, |_| Err(
            RawError::Budget
        )),
        Err(RawError::Budget)
    );
    assert_eq!(
        visit_array(&trailing, trailing.full_span(), &mut scratch, |_| Ok(())),
        Err(RawError::Invalid)
    );
    // 错误注入不泄漏临时分配，scratch/lease 之后仍可复用。
    let stats = measure(|| {
        for (error, _) in categories {
            assert_eq!(
                visit_array(&array, array.full_span(), &mut scratch, |_| Err(error)),
                Err(error)
            );
        }
        assert_eq!(
            visit_array(&array, array.full_span(), &mut scratch, |_| Ok(())),
            Ok(())
        );
    });
    assert_eq!(stats.live, 0);
    assert_eq!(
        decode_identity(
            serde_json::from_str::<&RawValue>(r#""ok""#).unwrap(),
            &budget,
            &mut scratch
        )
        .unwrap()
        .as_str(),
        "ok"
    );
    drop(array);
    drop(object);
    drop(trailing);
    assert_eq!(budget.used().0, MAX_FRAME_BYTES);
}

#[test]
fn output_budget_failure_cancel_and_budget_destruction() {
    let stats = measure(|| {
        let budget = PayloadBudget::default();
        let mut scratch = budget.json_scratch();
        let raw = serialize_payload_with_scratch(
            &serde_json::json!({"id":1,"result":null}),
            &budget,
            &mut scratch,
        )
        .unwrap();
        let clone = raw.clone();
        assert_eq!(raw.as_str().as_ptr(), clone.as_str().as_ptr());
        drop(raw);
        let (sender, receiver) = tokio::sync::oneshot::channel();
        sender.send(clone).ok().unwrap();
        drop(receiver);
        assert_eq!(budget.used().0, MAX_FRAME_BYTES);
        let occupied = budget
            .try_acquire(ORDINARY_BYTES - MAX_FRAME_BYTES, false)
            .unwrap();
        assert!(serialize_payload_with_scratch(&"x", &budget, &mut scratch).is_err());
        drop(occupied);
        assert_eq!(budget.used().0, MAX_FRAME_BYTES);
    });
    assert_eq!(stats.live, 0);
}
