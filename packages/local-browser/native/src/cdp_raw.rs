// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

use std::fmt;
use std::io::Write;
use std::sync::Arc;

use serde::de::{DeserializeSeed, Error as _, MapAccess, SeqAccess, Visitor};
use serde::{Deserializer, Serialize};
use serde_json::value::RawValue;

use crate::transport::{JsonScratchGuard, PayloadBudget, PayloadLease, MAX_FRAME_BYTES};

const STRING_CHUNK: usize = 4096;
const IDENTITY_BYTES: usize = 4096;

pub use crate::transport::{LeasedRaw, RawError, RawSpan};
type Result<T> = std::result::Result<T, RawError>;

impl LeasedRaw {
    pub fn raw<'a>(
        &'a self,
        span: RawSpan,
        _scratch: &mut JsonScratchGuard<'_>,
    ) -> Result<&'a RawValue> {
        let text = self.text(span)?;
        preflight(text, ParsePolicy::ClientOpaque)?;
        serde_json::from_str(text).map_err(|_| RawError::Invalid)
    }
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

/// 固定 span view；只借用 owner 和同步 scratch，扫描结束后才能读取下一层。
#[derive(Clone, Copy)]
pub struct RawNode<'a> {
    backing: &'a LeasedRaw,
    span: RawSpan,
}

impl RawEnvelope {
    pub fn at(&self, span: RawSpan) -> RawNode<'_> {
        RawNode {
            backing: self.backing(),
            span,
        }
    }

    pub fn node(&self, name: &str) -> Option<RawNode<'_>> {
        self.field(name).span().map(|span| RawNode {
            backing: &self.backing,
            span,
        })
    }

    pub fn id(&self, scratch: &mut JsonScratchGuard<'_>) -> Option<u64> {
        self.node("id")?.u64(scratch)
    }

    pub fn has_error(&self) -> bool {
        self.field("error") != RawPresence::Missing
    }
}

impl LeasedRaw {
    pub fn node(&self) -> RawNode<'_> {
        RawNode {
            backing: self,
            span: self.full_span(),
        }
    }
}

impl<'a> RawNode<'a> {
    pub fn span(self) -> RawSpan {
        self.span
    }

    pub fn field(self, name: &str, scratch: &mut JsonScratchGuard<'_>) -> Option<Self> {
        let [field] = select_fields(
            self.backing,
            self.span,
            [name],
            DuplicatePolicy::LastWins,
            scratch,
        )
        .ok()?;
        field.span().map(|span| Self {
            backing: self.backing,
            span,
        })
    }

    pub fn raw(self, scratch: &mut JsonScratchGuard<'_>) -> Result<&'a RawValue> {
        self.backing.raw(self.span, scratch)
    }

    pub fn text(self) -> Result<&'a str> {
        self.backing.text(self.span)
    }

    pub fn string(
        self,
        budget: &PayloadBudget,
        scratch: &mut JsonScratchGuard<'_>,
    ) -> Result<Option<LeasedIdentity>> {
        if !self.text()?.starts_with('"') {
            return Ok(None);
        }
        decode_identity(self.raw(scratch)?, budget, scratch).map(Some)
    }

    /// 同步身份视图只借调用方固定栈；不申请普通预算，也不能越过 raw owner。
    pub fn identity_text<'b>(
        self,
        buffer: &'b mut [u8; IDENTITY_BYTES],
        _scratch: &mut JsonScratchGuard<'_>,
    ) -> Result<Option<&'b str>> {
        let text = self.text()?;
        if !text.starts_with('"') {
            return Ok(None);
        }
        let mut length = 0usize;
        string_chunks(text, |chunk| {
            let end = length.saturating_add(chunk.len());
            if let Some(output) = buffer.get_mut(length..end) {
                output.copy_from_slice(chunk.as_bytes());
            }
            length = end;
            Ok(())
        })?;
        if length > buffer.len() {
            return Err(RawError::Limit);
        }
        std::str::from_utf8(&buffer[..length])
            .map(Some)
            .map_err(|_| RawError::Invalid)
    }

    pub fn cleanup_identity(
        self,
        budget: &PayloadBudget,
        scratch: &mut JsonScratchGuard<'_>,
    ) -> Result<Option<LeasedIdentity>> {
        if !self.text()?.starts_with('"') {
            return Ok(None);
        }
        decode_text_in(self.raw(scratch)?, budget, IDENTITY_BYTES, true, scratch).map(Some)
    }

    pub fn equals(self, expected: &str, scratch: &mut JsonScratchGuard<'_>) -> bool {
        self.raw(scratch)
            .and_then(|raw| TextScratch { _private: () }.equals(raw, expected))
            .unwrap_or(false)
    }

    pub fn u64(self, scratch: &mut JsonScratchGuard<'_>) -> Option<u64> {
        upstream_id(self.raw(scratch).ok()?, scratch)
    }

    pub fn is_array(self) -> bool {
        self.text().is_ok_and(|text| text.starts_with('['))
    }

    pub fn copy(
        self,
        budget: &PayloadBudget,
        scratch: &mut JsonScratchGuard<'_>,
    ) -> Result<LeasedRaw> {
        self.backing
            .copy_span_to_ordinary(self.span, budget, scratch)
    }
}

pub struct RawText<'a> {
    raw: &'a str,
}

impl RawNode<'_> {
    pub fn decoded(&self) -> RawText<'_> {
        let raw = self.text().unwrap_or("\"\"");
        RawText {
            raw: if raw.starts_with('"') { raw } else { "\"\"" },
        }
    }
}

impl Serialize for RawText<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl fmt::Display for RawText<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        string_chunks(self.raw, |chunk| {
            f.write_str(chunk).map_err(|_| RawError::Limit)
        })
        .map_err(|_| fmt::Error)
    }
}

pub struct RawArray<'a> {
    backing: &'a LeasedRaw,
    remaining: &'a str,
}

impl<'a> RawNode<'a> {
    pub fn array(self, scratch: &mut JsonScratchGuard<'_>) -> Option<RawArray<'a>> {
        let validated = self.raw(scratch).ok()?;
        let text = validated.get().strip_prefix('[')?;
        Some(RawArray {
            backing: self.backing,
            remaining: text,
        })
    }
}

impl<'a> RawArray<'a> {
    pub fn next(&mut self, _scratch: &mut JsonScratchGuard<'_>) -> Result<Option<RawNode<'a>>> {
        let text = self.remaining.trim_start();
        if text.starts_with(']') {
            return Ok(None);
        }
        let mut stream = serde_json::Deserializer::from_str(text).into_iter::<&RawValue>();
        let raw = stream
            .next()
            .ok_or(RawError::Invalid)?
            .map_err(|_| RawError::Invalid)?;
        self.remaining = text[stream.byte_offset()..].trim_start();
        if let Some(rest) = self.remaining.strip_prefix(',') {
            self.remaining = rest;
        }
        Ok(Some(RawNode {
            backing: self.backing,
            span: self.backing.span_of(raw)?,
        }))
    }
}

impl fmt::Display for LeasedIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
impl Serialize for LeasedIdentity {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl std::ops::Deref for LeasedIdentity {
    type Target = str;
    fn deref(&self) -> &str {
        self.as_str()
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
    pub fn literal(text: &str, budget: &PayloadBudget) -> Result<Self> {
        let charged =
            text.len() + std::mem::size_of::<IdentityOwner>() + 2 * std::mem::size_of::<usize>();
        let lease = budget.try_acquire(charged, false).ok_or(RawError::Budget)?;
        Ok(Self(Arc::new(IdentityOwner {
            text: text.to_owned(),
            _lease: lease,
        })))
    }

    pub fn as_str(&self) -> &str {
        &self.0.text
    }
}

pub fn decode_identity(
    raw: &RawValue,
    budget: &PayloadBudget,
    scratch: &mut JsonScratchGuard<'_>,
) -> Result<LeasedIdentity> {
    decode_text(raw, budget, IDENTITY_BYTES, scratch)
}

pub fn decode_text(
    raw: &RawValue,
    budget: &PayloadBudget,
    limit: usize,
    scratch: &mut JsonScratchGuard<'_>,
) -> Result<LeasedIdentity> {
    decode_text_in(raw, budget, limit, false, scratch)
}

fn decode_text_in(
    raw: &RawValue,
    budget: &PayloadBudget,
    limit: usize,
    internal: bool,
    _scratch: &mut JsonScratchGuard<'_>,
) -> Result<LeasedIdentity> {
    let mut length = 0usize;
    string_chunks(raw.get(), |chunk| {
        length = length.saturating_add(chunk.len());
        Ok(())
    })?;
    if length > limit {
        return Err(RawError::Limit);
    }
    let charged = length + std::mem::size_of::<IdentityOwner>() + 2 * std::mem::size_of::<usize>();
    let lease = budget
        .try_acquire(charged, internal)
        .ok_or(RawError::Budget)?;
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

/// 两遍同步输出：先计长再租用固定容量，callback 不得重入 JSON 扫描。
pub fn serialize_event(
    envelope: &RawEnvelope,
    page: bool,
    budget: &PayloadBudget,
    scratch: &mut JsonScratchGuard<'_>,
) -> Result<(String, PayloadLease)> {
    fn emit(
        raw: &LeasedRaw,
        page: bool,
        scratch: &mut JsonScratchGuard<'_>,
        out: &mut dyn Write,
    ) -> Result<()> {
        if !page {
            return out
                .write_all(raw.as_str().as_bytes())
                .map_err(|_| RawError::Limit);
        }
        out.write_all(b"{").map_err(|_| RawError::Limit)?;
        let mut first = true;
        visit_object(raw, raw.full_span(), scratch, |key, value, text| {
            if text.equals(key, "sessionId")? {
                return Ok(());
            }
            if !first {
                out.write_all(b",").map_err(|_| RawError::Limit)?;
            }
            first = false;
            out.write_all(key.get().as_bytes())
                .map_err(|_| RawError::Limit)?;
            out.write_all(b":").map_err(|_| RawError::Limit)?;
            out.write_all(value.get().as_bytes())
                .map_err(|_| RawError::Limit)
        })?;
        out.write_all(b"}").map_err(|_| RawError::Limit)
    }
    struct Count(usize);
    impl Write for Count {
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
    let mut count = Count(0);
    emit(envelope.backing(), page, scratch, &mut count)?;
    let lease = budget.try_acquire(count.0, false).ok_or(RawError::Budget)?;
    let mut writer = FixedWriter {
        bytes: Vec::with_capacity(count.0),
        limit: count.0,
    };
    emit(envelope.backing(), page, scratch, &mut writer)?;
    Ok((
        String::from_utf8(writer.bytes).map_err(|_| RawError::Invalid)?,
        lease,
    ))
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
