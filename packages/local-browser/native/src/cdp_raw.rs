// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

use std::fmt;
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use serde::de::{DeserializeSeed, Error as _, MapAccess, SeqAccess, Visitor};
use serde::{Deserializer, Serialize};
use serde_json::value::RawValue;

use crate::transport::{JsonScratchGuard, PayloadBudget, PayloadLease, MAX_FRAME_BYTES};

const STRING_CHUNK: usize = 4096;
const IDENTITY_BYTES: usize = 4096;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RawError {
    Invalid,
    Limit,
    Budget,
    Duplicate,
}

impl fmt::Display for RawError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid or unbudgeted CDP payload")
    }
}
impl std::error::Error for RawError {}

type Result<T> = std::result::Result<T, RawError>;

// owner token 只分配不回收；溢出 fail-closed，绝不 wrap 后复用旧 token。
static NEXT_OWNER: AtomicU64 = AtomicU64::new(1);

fn next_owner_token() -> Result<u64> {
    NEXT_OWNER
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |token| {
            token.checked_add(1)
        })
        .map_err(|_| RawError::Limit)
}

// token 是每个 owner 固定 8 字节 metadata；无全局注册表，span 复制不分配。
// client 由 RequestLease 的 fixed accounting 覆盖；后继 raw owner 仍须接线计费。
struct RawOwner {
    token: u64,
    text: String,
    _lease: PayloadLease,
}

// client 的 64 KiB fixed accounting 为每个 owner（含 Arc 头）保留 128 bytes。
const _: () = assert!(std::mem::size_of::<RawOwner>() + 2 * std::mem::size_of::<usize>() <= 128);

/// clone 只共享 backing、lease 与 token；复制必须经 copy_span_to_ordinary 先取得普通预算。
#[derive(Clone)]
pub struct LeasedRaw(Arc<RawOwner>);

impl LeasedRaw {
    pub fn new(text: String, lease: PayloadLease) -> Result<Self> {
        if text.len() > MAX_FRAME_BYTES || text.capacity() > lease.bytes() {
            return Err(RawError::Limit);
        }
        Ok(Self(Arc::new(RawOwner {
            token: next_owner_token()?,
            text,
            _lease: lease,
        })))
    }

    pub fn as_str(&self) -> &str {
        &self.0.text
    }

    pub fn full_span(&self) -> RawSpan {
        RawSpan {
            owner: self.0.token,
            start: 0,
            end: self.0.text.len() as u32,
        }
    }

    pub fn text(&self, span: RawSpan) -> Result<&str> {
        if span.owner != self.0.token {
            return Err(RawError::Invalid);
        }
        self.as_str()
            .get(span.start as usize..span.end as usize)
            .ok_or(RawError::Invalid)
    }

    pub fn copy_span_to_ordinary(
        &self,
        span: RawSpan,
        budget: &PayloadBudget,
        _scratch: &mut JsonScratchGuard<'_>,
    ) -> Result<Self> {
        let text = self.text(span)?;
        let lease = budget
            .try_acquire(text.len(), false)
            .ok_or(RawError::Budget)?;
        let mut copy = String::with_capacity(text.len());
        copy.push_str(text);
        Self::new(copy, lease)
    }

    pub fn raw<'a>(
        &'a self,
        span: RawSpan,
        _scratch: &mut JsonScratchGuard<'_>,
    ) -> Result<&'a RawValue> {
        let text = self.text(span)?;
        preflight(text, ParsePolicy::ClientOpaque)?;
        serde_json::from_str(text).map_err(|_| RawError::Invalid)
    }

    pub fn span_of(&self, raw: &RawValue) -> Result<RawSpan> {
        let start = (raw.get().as_ptr() as usize)
            .checked_sub(self.as_str().as_ptr() as usize)
            .ok_or(RawError::Invalid)?;
        let end = start
            .checked_add(raw.get().len())
            .ok_or(RawError::Invalid)?;
        if end > self.as_str().len() {
            return Err(RawError::Invalid);
        }
        Ok(RawSpan {
            owner: self.0.token,
            start: start as u32,
            end: end as u32,
        })
    }
}

/// 16 bytes：owner token 加范围。clone 共享 token，跨 owner 一律拒绝。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RawSpan {
    owner: u64,
    start: u32,
    end: u32,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RawPresence {
    #[default]
    Missing,
    Null(RawSpan),
    Present(RawSpan),
}

impl RawPresence {
    pub fn span(self) -> Option<RawSpan> {
        match self {
            Self::Missing => None,
            Self::Null(span) | Self::Present(span) => Some(span),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParsePolicy {
    ClientOpaque,
    UpstreamStrict,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DuplicatePolicy {
    Reject,
    LastWins,
}

pub struct RawEnvelope {
    backing: LeasedRaw,
    fields: [RawPresence; 6],
}

impl RawEnvelope {
    pub fn parse(
        backing: LeasedRaw,
        policy: ParsePolicy,
        scratch: &mut JsonScratchGuard<'_>,
    ) -> Result<Self> {
        preflight(backing.as_str(), policy)?;
        let mut fields = [RawPresence::Missing; 6];
        let names = ["id", "method", "sessionId", "params", "result", "error"];
        if policy == ParsePolicy::ClientOpaque && backing.as_str().trim_start().starts_with('[') {
            let selected = select_sequence::<4>(&backing, backing.full_span(), scratch)?;
            fields[..4].copy_from_slice(&selected);
        } else {
            visit_object(&backing, backing.full_span(), scratch, |key, raw, text| {
                for (index, name) in names.iter().enumerate() {
                    if !text.equals(key, name)? {
                        continue;
                    }
                    if policy == ParsePolicy::ClientOpaque
                        && index < 4
                        && fields[index] != RawPresence::Missing
                    {
                        return Err(RawError::Duplicate);
                    }
                    fields[index] = presence(&backing, raw)?;
                }
                Ok(())
            })?;
        }
        if policy == ParsePolicy::ClientOpaque {
            validate_client_fields(&backing, &fields)?;
        }
        Ok(Self { backing, fields })
    }

    pub fn backing(&self) -> &LeasedRaw {
        &self.backing
    }

    pub fn field(&self, name: &str) -> RawPresence {
        ["id", "method", "sessionId", "params", "result", "error"]
            .iter()
            .position(|field| *field == name)
            .map(|index| self.fields[index])
            .unwrap_or_default()
    }
}

fn validate_client_fields(backing: &LeasedRaw, fields: &[RawPresence; 6]) -> Result<()> {
    for field in &fields[..3] {
        let RawPresence::Present(span) = field else {
            continue;
        };
        let text = backing.text(*span)?;
        if text.len() > IDENTITY_BYTES {
            return Err(RawError::Limit);
        }
        preflight(text, ParsePolicy::UpstreamStrict)?;
    }
    Ok(())
}

fn presence(backing: &LeasedRaw, raw: &RawValue) -> Result<RawPresence> {
    let span = backing.span_of(raw)?;
    Ok(if raw.get() == "null" {
        RawPresence::Null(span)
    } else {
        RawPresence::Present(span)
    })
}

/// 回调只能分块解码文本，不能重入 raw 扫描；外层 serde 可能仍持有大嵌套 scratch。
pub struct TextScratch {
    _private: (),
}

impl TextScratch {
    pub fn equals(&mut self, raw: &RawValue, expected: &str) -> Result<bool> {
        let mut offset = 0usize;
        let mut equal = true;
        string_chunks(raw.get(), |chunk| {
            equal &=
                expected.as_bytes().get(offset..offset + chunk.len()) == Some(chunk.as_bytes());
            offset += chunk.len();
            Ok(())
        })?;
        Ok(equal && offset == expected.len())
    }
}

struct ObjectVisitor<'a, F> {
    visitor: &'a mut F,
    error: &'a mut Option<RawError>,
}
impl<'de, F> Visitor<'de> for ObjectVisitor<'_, F>
where
    F: FnMut(&'de RawValue, &'de RawValue, &mut TextScratch) -> Result<()>,
{
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("object")
    }
    fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> std::result::Result<(), M::Error> {
        let mut text = TextScratch { _private: () };
        while let Some(key) = map.next_key::<&RawValue>()? {
            let value = map.next_value::<&RawValue>()?;
            if let Err(error) = (self.visitor)(key, value, &mut text) {
                *self.error = Some(error);
                return Err(M::Error::custom(error));
            }
        }
        Ok(())
    }
}

pub fn visit_object<'a>(
    backing: &'a LeasedRaw,
    span: RawSpan,
    _scratch: &mut JsonScratchGuard<'_>,
    mut visitor: impl FnMut(&'a RawValue, &'a RawValue, &mut TextScratch) -> Result<()>,
) -> Result<()> {
    let text = backing.text(span)?;
    preflight(text, ParsePolicy::ClientOpaque)?;
    let mut de = serde_json::Deserializer::from_str(text);
    let mut error = None;
    de.deserialize_map(ObjectVisitor {
        visitor: &mut visitor,
        error: &mut error,
    })
    .map_err(|_| error.unwrap_or(RawError::Invalid))?;
    de.end().map_err(|_| RawError::Invalid)
}

struct ArrayVisitor<'a, F> {
    visitor: &'a mut F,
    error: &'a mut Option<RawError>,
}
impl<'de, F> Visitor<'de> for ArrayVisitor<'_, F>
where
    F: FnMut(&'de RawValue) -> Result<()>,
{
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("array")
    }
    fn visit_seq<S: SeqAccess<'de>>(self, mut seq: S) -> std::result::Result<(), S::Error> {
        while let Some(value) = seq.next_element::<&RawValue>()? {
            if let Err(error) = (self.visitor)(value) {
                *self.error = Some(error);
                return Err(S::Error::custom(error));
            }
        }
        Ok(())
    }
}

pub fn visit_array<'a>(
    backing: &'a LeasedRaw,
    span: RawSpan,
    _scratch: &mut JsonScratchGuard<'_>,
    mut visitor: impl FnMut(&'a RawValue) -> Result<()>,
) -> Result<()> {
    let text = backing.text(span)?;
    preflight(text, ParsePolicy::ClientOpaque)?;
    let mut de = serde_json::Deserializer::from_str(text);
    let mut error = None;
    de.deserialize_seq(ArrayVisitor {
        visitor: &mut visitor,
        error: &mut error,
    })
    .map_err(|_| error.unwrap_or(RawError::Invalid))?;
    de.end().map_err(|_| RawError::Invalid)
}

/// serde struct 的序列形式必须恰有 N 个元素；只记录固定 span，不解码值。
pub(crate) fn select_sequence<const N: usize>(
    backing: &LeasedRaw,
    span: RawSpan,
    scratch: &mut JsonScratchGuard<'_>,
) -> Result<[RawPresence; N]> {
    let mut fields = [RawPresence::Missing; N];
    let mut index = 0;
    visit_array(backing, span, scratch, |raw| {
        let field = fields.get_mut(index).ok_or(RawError::Invalid)?;
        *field = presence(backing, raw)?;
        index += 1;
        Ok(())
    })?;
    if index != N {
        return Err(RawError::Invalid);
    }
    Ok(fields)
}

/// 固定字段选择：重复拒绝可供 params metadata 的整体 fallback 使用，不决定 broker 策略。
pub fn select_fields<const N: usize>(
    backing: &LeasedRaw,
    span: RawSpan,
    names: [&str; N],
    duplicate: DuplicatePolicy,
    scratch: &mut JsonScratchGuard<'_>,
) -> Result<[RawPresence; N]> {
    let mut fields = [RawPresence::Missing; N];
    visit_object(backing, span, scratch, |key, value, text| {
        for (index, name) in names.iter().enumerate() {
            if !text.equals(key, name)? {
                continue;
            }
            if duplicate == DuplicatePolicy::Reject && fields[index] != RawPresence::Missing {
                return Err(RawError::Duplicate);
            }
            fields[index] = presence(backing, value)?;
        }
        Ok(())
    })?;
    Ok(fields)
}

/// 只界定词法 token 与深度；完整 grammar 和 trailing data 始终交给 serde。
// serde_json 1.0.154 / nightly-2026-05-14：raw scratch <=8 MiB，realloc 峰 <=12 MiB。
// 必须先拒绝不平衡输入；辅助分块 <64 KiB，与 codec 共用已扣除的 16 MiB。
fn preflight(text: &str, policy: ParsePolicy) -> Result<()> {
    if text.len() > MAX_FRAME_BYTES {
        return Err(RawError::Limit);
    }
    let bytes = text.as_bytes();
    let mut depth = 0usize;
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'"' => {
                let end = string_end(bytes, index)?;
                if policy == ParsePolicy::UpstreamStrict {
                    string_chunks(&text[index..end], |_| Ok(()))?;
                }
                index = end;
            }
            b'[' | b'{' => {
                depth += 1;
                if policy == ParsePolicy::UpstreamStrict && depth >= 128 {
                    return Err(RawError::Limit);
                }
                index += 1;
            }
            b']' | b'}' => {
                depth = depth.checked_sub(1).ok_or(RawError::Invalid)?;
                index += 1;
            }
            b'-' | b'0'..=b'9' => {
                let start = index;
                index += 1;
                while index < bytes.len()
                    && matches!(bytes[index], b'0'..=b'9' | b'.' | b'e' | b'E' | b'+' | b'-')
                {
                    index += 1;
                }
                if policy == ParsePolicy::UpstreamStrict {
                    serde_json::from_str::<serde_json::Number>(&text[start..index])
                        .map_err(|_| RawError::Invalid)?;
                }
            }
            _ => index += 1,
        }
    }
    if depth != 0 {
        return Err(RawError::Invalid);
    }
    Ok(())
}

fn string_end(bytes: &[u8], start: usize) -> Result<usize> {
    let mut index = start + 1;
    while index < bytes.len() {
        match bytes[index] {
            b'"' => return Ok(index + 1),
            b'\\' => index += 2,
            _ => index += 1,
        }
    }
    Err(RawError::Invalid)
}

struct StringSeed<'a, F>(&'a mut F);
impl<'de, F: FnMut(&str) -> Result<()>> DeserializeSeed<'de> for StringSeed<'_, F> {
    type Value = ();
    fn deserialize<D: Deserializer<'de>>(self, de: D) -> std::result::Result<(), D::Error> {
        de.deserialize_str(self)
    }
}
impl<'de, F: FnMut(&str) -> Result<()>> Visitor<'de> for StringSeed<'_, F> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("string")
    }
    fn visit_str<E: serde::de::Error>(self, value: &str) -> std::result::Result<(), E> {
        (self.0)(value).map_err(E::custom)
    }
}

fn string_unit(text: &str, index: usize) -> Result<usize> {
    let bytes = text.as_bytes();
    if bytes[index] != b'\\' {
        return Ok(text[index..]
            .chars()
            .next()
            .ok_or(RawError::Invalid)?
            .len_utf8());
    }
    let next = *bytes.get(index + 1).ok_or(RawError::Invalid)?;
    if next != b'u' {
        return if matches!(next, b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't') {
            Ok(2)
        } else {
            Err(RawError::Invalid)
        };
    }
    let hex = text.get(index + 2..index + 6).ok_or(RawError::Invalid)?;
    let unit = u16::from_str_radix(hex, 16).map_err(|_| RawError::Invalid)?;
    if (0xd800..=0xdbff).contains(&unit) {
        if bytes.get(index + 6..index + 8) != Some(b"\\u") {
            return Err(RawError::Invalid);
        }
        let low = text.get(index + 8..index + 12).ok_or(RawError::Invalid)?;
        let low = u16::from_str_radix(low, 16).map_err(|_| RawError::Invalid)?;
        if !(0xdc00..=0xdfff).contains(&low) {
            return Err(RawError::Invalid);
        }
        return Ok(12);
    }
    Ok(6)
}

fn string_chunks(text: &str, mut visitor: impl FnMut(&str) -> Result<()>) -> Result<()> {
    if !text.starts_with('"') || !text.ends_with('"') || text.len() < 2 {
        return Err(RawError::Invalid);
    }
    let end = text.len() - 1;
    let mut index = 1;
    let mut buffer = [0u8; STRING_CHUNK + 2];
    while index < end {
        let start = index;
        while index < end {
            let unit = string_unit(text, index)?;
            if index + unit > end {
                return Err(RawError::Invalid);
            }
            if index + unit - start > STRING_CHUNK {
                break;
            }
            index += unit;
        }
        let length = index - start;
        buffer[0] = b'"';
        buffer[1..length + 1].copy_from_slice(&text.as_bytes()[start..index]);
        buffer[length + 1] = b'"';
        let mut de = serde_json::Deserializer::from_slice(&buffer[..length + 2]);
        StringSeed(&mut visitor)
            .deserialize(&mut de)
            .map_err(|_| RawError::Invalid)?;
        de.end().map_err(|_| RawError::Invalid)?;
    }
    Ok(())
}

#[derive(Clone)]
pub struct LeasedIdentity(Arc<IdentityOwner>);
struct IdentityOwner {
    text: String,
    _lease: PayloadLease,
}
impl LeasedIdentity {
    pub fn as_str(&self) -> &str {
        &self.0.text
    }
}

pub fn decode_identity(
    raw: &RawValue,
    budget: &PayloadBudget,
    _scratch: &mut JsonScratchGuard<'_>,
) -> Result<LeasedIdentity> {
    let mut length = 0usize;
    string_chunks(raw.get(), |chunk| {
        length = length
            .checked_add(chunk.len())
            .filter(|n| *n <= IDENTITY_BYTES)
            .ok_or(RawError::Limit)?;
        Ok(())
    })?;
    let lease = budget.try_acquire(length, false).ok_or(RawError::Budget)?;
    let mut text = String::with_capacity(length);
    string_chunks(raw.get(), |chunk| {
        if text.len() + chunk.len() > text.capacity() {
            return Err(RawError::Limit);
        }
        text.push_str(chunk);
        Ok(())
    })?;
    Ok(LeasedIdentity(Arc::new(IdentityOwner {
        text,
        _lease: lease,
    })))
}

pub(crate) enum MetadataScalar {
    Other,
    Text(LeasedIdentity),
    Bool(bool),
    Number(serde_json::Number),
}

struct MetadataSeed<'a> {
    budget: &'a PayloadBudget,
    capture: bool,
}

impl<'de> DeserializeSeed<'de> for MetadataSeed<'_> {
    type Value = MetadataScalar;
    fn deserialize<D: Deserializer<'de>>(
        self,
        de: D,
    ) -> std::result::Result<Self::Value, D::Error> {
        de.deserialize_any(self)
    }
}

// 仅用于旧路径已限制到 4096 raw bytes 的 metadata，不用于 opaque params。
// 无容器树；普通嵌套共用 serde scratch。sentinel 每层至少缩短 35 raw bytes，
// 深度 <128；每层 buffer/grow/栈保守计 32 KiB，总量 <4 MiB 且与 raw 扫描串行。
impl<'de> Visitor<'de> for MetadataSeed<'_> {
    type Value = MetadataScalar;
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("metadata")
    }
    fn visit_bool<E: serde::de::Error>(self, value: bool) -> std::result::Result<Self::Value, E> {
        Ok(MetadataScalar::Bool(value))
    }
    fn visit_i64<E: serde::de::Error>(self, value: i64) -> std::result::Result<Self::Value, E> {
        Ok(MetadataScalar::Number(value.into()))
    }
    fn visit_u64<E: serde::de::Error>(self, value: u64) -> std::result::Result<Self::Value, E> {
        Ok(MetadataScalar::Number(value.into()))
    }
    fn visit_f64<E: serde::de::Error>(self, value: f64) -> std::result::Result<Self::Value, E> {
        serde_json::Number::from_f64(value)
            .map(MetadataScalar::Number)
            .ok_or_else(|| E::custom(RawError::Invalid))
    }
    fn visit_unit<E: serde::de::Error>(self) -> std::result::Result<Self::Value, E> {
        Ok(MetadataScalar::Other)
    }
    fn visit_str<E: serde::de::Error>(self, value: &str) -> std::result::Result<Self::Value, E> {
        if !self.capture {
            return Ok(MetadataScalar::Other);
        }
        // identity clone 可短暂晚于 RequestLease drop；Arc 头和 owner 随自己的 lease 计费。
        let charged =
            value.len() + std::mem::size_of::<IdentityOwner>() + 2 * std::mem::size_of::<usize>();
        let lease = self
            .budget
            .try_acquire(charged, false)
            .ok_or_else(|| E::custom(RawError::Budget))?;
        let mut text = String::with_capacity(value.len());
        text.push_str(value);
        Ok(MetadataScalar::Text(LeasedIdentity(Arc::new(
            IdentityOwner {
                text,
                _lease: lease,
            },
        ))))
    }
    fn visit_seq<S: SeqAccess<'de>>(
        self,
        mut seq: S,
    ) -> std::result::Result<Self::Value, S::Error> {
        while seq
            .next_element_seed(MetadataSeed {
                budget: self.budget,
                capture: false,
            })?
            .is_some()
        {}
        Ok(MetadataScalar::Other)
    }
    fn visit_map<M: MapAccess<'de>>(
        self,
        mut map: M,
    ) -> std::result::Result<Self::Value, M::Error> {
        let Some(key) = map.next_key::<String>()? else {
            return Ok(MetadataScalar::Other);
        };
        if key == "$serde_json::private::RawValue" {
            let text: String = map.next_value()?;
            return metadata_scalar_inner(&text, self.budget, self.capture)
                .map_err(M::Error::custom);
        }
        map.next_value_seed(MetadataSeed {
            budget: self.budget,
            capture: false,
        })?;
        while map.next_key::<String>()?.is_some() {
            map.next_value_seed(MetadataSeed {
                budget: self.budget,
                capture: false,
            })?;
        }
        Ok(MetadataScalar::Other)
    }
}

fn metadata_scalar_inner(
    text: &str,
    budget: &PayloadBudget,
    capture: bool,
) -> Result<MetadataScalar> {
    if text.len() > IDENTITY_BYTES {
        return Err(RawError::Limit);
    }
    let mut de = serde_json::Deserializer::from_str(text);
    let scalar = MetadataSeed { budget, capture }
        .deserialize(&mut de)
        .map_err(|_| RawError::Invalid)?;
    de.end().map_err(|_| RawError::Invalid)?;
    Ok(scalar)
}

pub(crate) fn metadata_scalar(
    raw: &RawValue,
    budget: &PayloadBudget,
    _scratch: &mut JsonScratchGuard<'_>,
) -> Result<MetadataScalar> {
    metadata_scalar_inner(raw.get(), budget, true)
}

fn number(raw: &RawValue) -> Option<serde_json::Number> {
    if !matches!(raw.get().as_bytes().first(), Some(b'-' | b'0'..=b'9')) {
        return None;
    }
    serde_json::from_str(raw.get()).ok()
}

pub fn safe_integer(raw: &RawValue, _scratch: &mut JsonScratchGuard<'_>) -> Option<i64> {
    let number = number(raw)?;
    if let Some(value) = number.as_i64() {
        return (value.unsigned_abs() <= 9_007_199_254_740_991).then_some(value);
    }
    let value = number.as_f64()?;
    (value.fract() == 0.0 && value.abs() <= 9_007_199_254_740_991f64).then_some(value as i64)
}

pub fn upstream_id(raw: &RawValue, _scratch: &mut JsonScratchGuard<'_>) -> Option<u64> {
    number(raw)?.as_u64()
}

struct FixedWriter {
    bytes: Vec<u8>,
    limit: usize,
}
impl Write for FixedWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.limit - self.bytes.len()
            || self.bytes.len() + bytes.len() > self.bytes.capacity()
        {
            return Err(std::io::Error::other("payload limit exceeded"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub fn serialize_payload_with_scratch<T: Serialize>(
    value: &T,
    budget: &PayloadBudget,
    _scratch: &mut JsonScratchGuard<'_>,
) -> Result<LeasedRaw> {
    let length = crate::transport::payload_size(value).ok_or(RawError::Limit)?;
    let lease = budget.try_acquire(length, false).ok_or(RawError::Budget)?;
    let mut writer = FixedWriter {
        bytes: Vec::with_capacity(length),
        limit: length,
    };
    serde_json::to_writer(&mut writer, value).map_err(|_| RawError::Invalid)?;
    LeasedRaw::new(
        String::from_utf8(writer.bytes).map_err(|_| RawError::Invalid)?,
        lease,
    )
}
