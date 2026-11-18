// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// 与 profile-store.mjs 对齐的 state 解析、校验和 JSON.stringify 等价编码。字符串以 UTF-16
// code unit 保存，对象保留 JS 属性顺序；序列化结果只用于校验长度和写入，不能被返回给调用方
// 覆盖数据库中的原文。实现不递归，避免深层输入耗尽原生栈。

use std::collections::HashSet;
use std::fmt;

use url::Url;

const MAX_COOKIES: usize = 3000;
const MAX_ORIGIN_BYTES: u64 = 5 * 1024 * 1024;
const MAX_STATE_BYTES: usize = 64 * 1024 * 1024;

pub type JsonString = Vec<u16>;

#[derive(Clone, Debug, PartialEq)]
pub enum Json {
    Null,
    Bool(bool),
    Number(f64),
    String(JsonString),
    Array(Vec<Json>),
    Object(Vec<(JsonString, Json)>),
}

impl Json {
    pub fn get(&self, key: &str) -> Option<&Json> {
        let Json::Object(entries) = self else {
            return None;
        };
        let key = utf16(key);
        entries
            .iter()
            .find(|(name, _)| *name == key)
            .map(|(_, value)| value)
    }

    fn as_array(&self) -> Option<&Vec<Json>> {
        match self {
            Json::Array(items) => Some(items),
            _ => None,
        }
    }

    fn as_object(&self) -> Option<&Vec<(JsonString, Json)>> {
        match self {
            Json::Object(entries) => Some(entries),
            _ => None,
        }
    }

    fn as_string(&self) -> Option<&JsonString> {
        match self {
            Json::String(value) => Some(value),
            _ => None,
        }
    }

    fn as_bool(&self) -> Option<bool> {
        match self {
            Json::Bool(value) => Some(*value),
            _ => None,
        }
    }

    fn as_finite_number(&self) -> Option<f64> {
        match self {
            Json::Number(value) if value.is_finite() => Some(*value),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StateError;

impl fmt::Display for StateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Invalid profile state")
    }
}

impl std::error::Error for StateError {}

fn utf16(text: &str) -> JsonString {
    text.encode_utf16().collect()
}

/// JSON 文本解析失败与字段校验失败使用同一脱敏错误。
pub fn parse(text: &str) -> Result<Json, StateError> {
    Parser::new(text.as_bytes()).document()
}

/// 校验 state 并返回与 Node `JSON.stringify` 等价的紧凑文本。
pub fn canonicalize_state(state: &Json) -> Result<String, StateError> {
    validate_leave(state)?;
    let text = serialize(state);
    if text.len() > MAX_STATE_BYTES {
        return Err(StateError);
    }
    Ok(text)
}

/// 只校验不重写，用于读取数据库已有 TEXT。
pub fn validate_state(state: &Json) -> Result<(), StateError> {
    canonicalize_state(state).map(|_| ())
}

/// 非负 JS safe integer 才能作为 revision；JSON 数字先按 Number 语义解析。
pub fn safe_integer(value: &Json) -> Option<i64> {
    let Json::Number(number) = value else {
        return None;
    };
    let safe =
        number.is_finite() && number.fract() == 0.0 && number.abs() <= 9_007_199_254_740_991.0;
    safe.then_some(*number as i64)
}

pub fn serialize(state: &Json) -> String {
    let mut output = String::new();
    let mut tasks = vec![Task::Value(state)];
    while let Some(task) = tasks.pop() {
        match task {
            Task::Value(Json::Null) => output.push_str("null"),
            Task::Value(Json::Bool(value)) => {
                output.push_str(if *value { "true" } else { "false" })
            }
            Task::Value(Json::Number(value)) => write_number(&mut output, *value),
            Task::Value(Json::String(value)) => write_string(&mut output, value),
            Task::Value(Json::Array(items)) => {
                output.push('[');
                tasks.push(Task::Array {
                    items,
                    next: 0,
                    first: true,
                });
            }
            Task::Value(Json::Object(entries)) => {
                output.push('{');
                tasks.push(Task::Object {
                    entries,
                    order: property_order(entries),
                    next: 0,
                    first: true,
                });
            }
            Task::Array { items, next, first } => {
                let Some(item) = items.get(next) else {
                    output.push(']');
                    continue;
                };
                if !first {
                    output.push(',');
                }
                tasks.push(Task::Array {
                    items,
                    next: next + 1,
                    first: false,
                });
                tasks.push(Task::Value(item));
            }
            Task::Object {
                entries,
                order,
                next,
                first,
            } => {
                let Some(index) = order.get(next).copied() else {
                    output.push('}');
                    continue;
                };
                let (key, value) = &entries[index];
                if !first {
                    output.push(',');
                }
                write_string(&mut output, key);
                output.push(':');
                tasks.push(Task::Object {
                    entries,
                    order,
                    next: next + 1,
                    first: false,
                });
                tasks.push(Task::Value(value));
            }
        }
    }
    output
}

enum Task<'a> {
    Value(&'a Json),
    Array {
        items: &'a [Json],
        next: usize,
        first: bool,
    },
    Object {
        entries: &'a [(JsonString, Json)],
        order: Vec<usize>,
        next: usize,
        first: bool,
    },
}

/// JS 对象先按数组索引数值升序，再按其余字符串属性的插入顺序。
fn property_order(entries: &[(JsonString, Json)]) -> Vec<usize> {
    let mut indices: Vec<(u32, usize)> = Vec::new();
    let mut others: Vec<usize> = Vec::new();
    for (position, (key, _)) in entries.iter().enumerate() {
        match array_index(key) {
            Some(index) => indices.push((index, position)),
            None => others.push(position),
        }
    }
    indices.sort_by_key(|(index, _)| *index);
    let mut order: Vec<usize> = indices.into_iter().map(|(_, position)| position).collect();
    order.extend(others);
    order
}

fn array_index(key: &[u16]) -> Option<u32> {
    if key.is_empty() || key.len() > 10 || (key.len() > 1 && key[0] == b'0' as u16) {
        return None;
    }
    let mut value: u64 = 0;
    for unit in key {
        if !(b'0' as u16..=b'9' as u16).contains(unit) {
            return None;
        }
        value = value
            .checked_mul(10)?
            .checked_add(u64::from(*unit - b'0' as u16))?;
    }
    (value < 4_294_967_295).then_some(value as u32)
}

fn write_string(output: &mut String, units: &[u16]) {
    output.push('"');
    let mut position = 0;
    while position < units.len() {
        let unit = units[position];
        match unit {
            0x22 => output.push_str("\\\""),
            0x5c => output.push_str("\\\\"),
            0x08 => output.push_str("\\b"),
            0x09 => output.push_str("\\t"),
            0x0a => output.push_str("\\n"),
            0x0c => output.push_str("\\f"),
            0x0d => output.push_str("\\r"),
            0x00..=0x1f => push_escape(output, unit),
            0xd800..=0xdbff => {
                let low = units.get(position + 1).copied();
                match low {
                    Some(0xdc00..=0xdfff) => {
                        let code = 0x10000
                            + ((u32::from(unit) - 0xd800) << 10)
                            + (u32::from(low.unwrap()) - 0xdc00);
                        output.push(char::from_u32(code).unwrap_or('\u{fffd}'));
                        position += 1;
                    }
                    _ => push_escape(output, unit),
                }
            }
            0xdc00..=0xdfff => push_escape(output, unit),
            _ => {
                output.push(char::from_u32(u32::from(unit)).unwrap_or('\u{fffd}'));
            }
        }
        position += 1;
    }
    output.push('"');
}

fn push_escape(output: &mut String, unit: u16) {
    use std::fmt::Write;
    let _ = write!(output, "\\u{unit:04x}");
}

/// ECMAScript Number::toString 的十进制形态；ryu-js 给出最短可回读数字并按 ES 选择
/// 指数/小数阈值和 tie 规则。非有限值按 JSON.stringify 写成 null。
fn write_number(output: &mut String, value: f64) {
    if !value.is_finite() {
        output.push_str("null");
        return;
    }
    let mut buffer = ryu_js::Buffer::new();
    output.push_str(buffer.format(value));
}

fn validate_leave(state: &Json) -> Result<(), StateError> {
    let object = state.as_object().ok_or(StateError)?;
    let cookies = field(object, "cookies")?.as_array().ok_or(StateError)?;
    if cookies.len() > MAX_COOKIES {
        return Err(StateError);
    }
    for cookie in cookies {
        validate_cookie(cookie)?;
    }
    let storage = field(object, "localStorage")?
        .as_array()
        .ok_or(StateError)?;
    let mut origins: HashSet<&[u16]> = HashSet::new();
    for entry in storage {
        validate_origin_entry(entry, &mut origins)?;
    }
    Ok(())
}

fn validate_cookie(cookie: &Json) -> Result<(), StateError> {
    let object = cookie.as_object().ok_or(StateError)?;
    for name in ["name", "value", "host", "path"] {
        field(object, name)?.as_string().ok_or(StateError)?;
    }
    for name in ["secure", "httpOnly", "hostOnly"] {
        field(object, name)?.as_bool().ok_or(StateError)?;
    }
    match field(object, "sameSite")? {
        Json::Null => {}
        Json::String(value) if is_same_site(value) => {}
        _ => return Err(StateError),
    }
    field(object, "createdAt")?
        .as_finite_number()
        .ok_or(StateError)?;
    match field(object, "expiresAt")? {
        Json::Null => {}
        Json::Number(value) if value.is_finite() => {}
        _ => return Err(StateError),
    }
    Ok(())
}

fn is_same_site(value: &[u16]) -> bool {
    value == utf16("Strict") || value == utf16("Lax") || value == utf16("None")
}

fn validate_origin_entry<'a>(
    entry: &'a Json,
    origins: &mut HashSet<&'a [u16]>,
) -> Result<(), StateError> {
    let parts = entry.as_array().ok_or(StateError)?;
    if parts.len() != 2 {
        return Err(StateError);
    }
    let origin = parts[0].as_string().ok_or(StateError)?;
    let pairs = parts[1].as_array().ok_or(StateError)?;
    if origins.contains(origin.as_slice()) || !valid_origin(origin) {
        return Err(StateError);
    }
    origins.insert(origin);
    let mut keys: HashSet<&[u16]> = HashSet::new();
    let mut bytes: u64 = 0;
    for pair in pairs {
        let values = pair.as_array().ok_or(StateError)?;
        if values.len() != 2 {
            return Err(StateError);
        }
        let key = values[0].as_string().ok_or(StateError)?;
        let value = values[1].as_string().ok_or(StateError)?;
        if !keys.insert(key) {
            return Err(StateError);
        }
        let length = key.len() as u64 + value.len() as u64;
        bytes = bytes
            .checked_add(length.checked_mul(2).ok_or(StateError)?)
            .ok_or(StateError)?;
    }
    if bytes > MAX_ORIGIN_BYTES {
        return Err(StateError);
    }
    Ok(())
}

fn field<'a>(object: &'a [(JsonString, Json)], name: &str) -> Result<&'a Json, StateError> {
    let wanted = utf16(name);
    object
        .iter()
        .find(|(key, _)| *key == wanted)
        .map(|(_, value)| value)
        .ok_or(StateError)
}

/// 只接受已按 WHATWG 序列化、且与 Node `new URL(x).origin` 相等的 HTTP(S) origin。
pub fn valid_origin(origin: &[u16]) -> bool {
    let Some(text) = String::from_utf16(origin).ok() else {
        return false;
    };
    let Ok(url) = Url::parse(&text) else {
        return false;
    };
    if url.scheme() != "http" && url.scheme() != "https" {
        return false;
    }
    url.origin().ascii_serialization() == text
}

struct Parser<'a> {
    bytes: &'a [u8],
    position: usize,
}

enum Frame {
    Array(Vec<Json>),
    Object(Vec<(JsonString, Json)>, Option<JsonString>),
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Expect {
    Value,
    ValueOrEnd,
    KeyOrEnd,
    Key,
    Colon,
    CommaOrEnd,
    Done,
}

impl<'a> Parser<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn document(mut self) -> Result<Json, StateError> {
        let mut stack: Vec<Frame> = Vec::new();
        let mut root: Option<Json> = None;
        let mut expect = Expect::Value;
        while expect != Expect::Done {
            self.skip_space();
            match expect {
                Expect::Value => {
                    let value = self.value(&mut stack)?;
                    if let Some(value) = value {
                        self.attach(&mut stack, &mut root, &mut expect, value)?;
                    } else {
                        expect = if matches!(stack.last(), Some(Frame::Object(..))) {
                            Expect::KeyOrEnd
                        } else {
                            Expect::ValueOrEnd
                        };
                    }
                }
                Expect::ValueOrEnd => {
                    if self.peek() == Some(b']') {
                        self.position += 1;
                        self.close(&mut stack, &mut root, &mut expect)?;
                    } else {
                        expect = Expect::Value;
                    }
                }
                Expect::KeyOrEnd => {
                    if self.peek() == Some(b'}') {
                        self.position += 1;
                        self.close(&mut stack, &mut root, &mut expect)?;
                    } else {
                        self.key(&mut stack)?;
                        expect = Expect::Colon;
                    }
                }
                Expect::Key => {
                    self.key(&mut stack)?;
                    expect = Expect::Colon;
                }
                Expect::Colon => {
                    if self.next() != Some(b':') {
                        return Err(StateError);
                    }
                    expect = Expect::Value;
                }
                Expect::CommaOrEnd => {
                    let byte = self.next().ok_or(StateError)?;
                    match (byte, stack.last()) {
                        (b',', Some(Frame::Array(_))) => expect = Expect::Value,
                        (b',', Some(Frame::Object(..))) => expect = Expect::Key,
                        (b']', Some(Frame::Array(_))) | (b'}', Some(Frame::Object(..))) => {
                            self.close(&mut stack, &mut root, &mut expect)?
                        }
                        _ => return Err(StateError),
                    }
                }
                Expect::Done => break,
            }
        }
        let Some(root) = root else {
            return Err(StateError);
        };
        self.skip_space();
        (self.position == self.bytes.len())
            .then_some(root)
            .ok_or(StateError)
    }

    fn value(&mut self, stack: &mut Vec<Frame>) -> Result<Option<Json>, StateError> {
        match self.peek().ok_or(StateError)? {
            b'{' => {
                self.position += 1;
                stack.push(Frame::Object(Vec::new(), None));
                Ok(None)
            }
            b'[' => {
                self.position += 1;
                stack.push(Frame::Array(Vec::new()));
                Ok(None)
            }
            b'"' => Ok(Some(Json::String(self.string()?))),
            b't' => self.literal(b"true", Json::Bool(true)).map(Some),
            b'f' => self.literal(b"false", Json::Bool(false)).map(Some),
            b'n' => self.literal(b"null", Json::Null).map(Some),
            b'-' | b'0'..=b'9' => Ok(Some(Json::Number(self.number()?))),
            _ => Err(StateError),
        }
    }

    fn attach(
        &self,
        stack: &mut [Frame],
        root: &mut Option<Json>,
        expect: &mut Expect,
        value: Json,
    ) -> Result<(), StateError> {
        match stack.last_mut() {
            None => {
                *root = Some(value);
                *expect = Expect::Done;
            }
            Some(Frame::Array(items)) => {
                items.push(value);
                *expect = Expect::CommaOrEnd;
            }
            Some(Frame::Object(entries, key)) => {
                let key = key.take().ok_or(StateError)?;
                insert_property(entries, key, value);
                *expect = Expect::CommaOrEnd;
            }
        }
        Ok(())
    }

    fn close(
        &self,
        stack: &mut Vec<Frame>,
        root: &mut Option<Json>,
        expect: &mut Expect,
    ) -> Result<(), StateError> {
        let frame = stack.pop().ok_or(StateError)?;
        let value = match frame {
            Frame::Array(items) => Json::Array(items),
            Frame::Object(entries, _) => Json::Object(entries),
        };
        self.attach(stack, root, expect, value)
    }

    fn key(&mut self, stack: &mut [Frame]) -> Result<(), StateError> {
        self.skip_space();
        if self.peek() != Some(b'"') {
            return Err(StateError);
        }
        let key = self.string()?;
        self.skip_space();
        let Some(Frame::Object(_, slot)) = stack.last_mut() else {
            return Err(StateError);
        };
        *slot = Some(key);
        Ok(())
    }

    fn literal(&mut self, text: &[u8], value: Json) -> Result<Json, StateError> {
        if !self.bytes[self.position..].starts_with(text) {
            return Err(StateError);
        }
        self.position += text.len();
        Ok(value)
    }

    fn string(&mut self) -> Result<JsonString, StateError> {
        self.position += 1;
        let mut units: JsonString = Vec::new();
        loop {
            let byte = self.next().ok_or(StateError)?;
            match byte {
                b'"' => return Ok(units),
                b'\\' => self.escape(&mut units)?,
                0x00..=0x1f => return Err(StateError),
                0x20..=0x7f => units.push(u16::from(byte)),
                _ => self.utf8(byte, &mut units)?,
            }
        }
    }

    fn escape(&mut self, units: &mut JsonString) -> Result<(), StateError> {
        let byte = self.next().ok_or(StateError)?;
        let simple = match byte {
            b'"' => Some(0x22),
            b'\\' => Some(0x5c),
            b'/' => Some(0x2f),
            b'b' => Some(0x08),
            b'f' => Some(0x0c),
            b'n' => Some(0x0a),
            b'r' => Some(0x0d),
            b't' => Some(0x09),
            b'u' => {
                let mut value: u16 = 0;
                for _ in 0..4 {
                    let digit = self.next().ok_or(StateError)?;
                    value = value
                        .checked_mul(16)
                        .and_then(|value| value.checked_add(u16::from(hex(digit)?)))
                        .ok_or(StateError)?;
                }
                Some(value)
            }
            _ => None,
        };
        units.push(simple.ok_or(StateError)?);
        Ok(())
    }

    fn utf8(&mut self, first: u8, units: &mut JsonString) -> Result<(), StateError> {
        let start = self.position - 1;
        let width = match first {
            0xc2..=0xdf => 2,
            0xe0..=0xef => 3,
            0xf0..=0xf4 => 4,
            _ => return Err(StateError),
        };
        let end = start.checked_add(width).ok_or(StateError)?;
        let slice = self.bytes.get(start..end).ok_or(StateError)?;
        let text = std::str::from_utf8(slice).map_err(|_| StateError)?;
        let character = text.chars().next().ok_or(StateError)?;
        let mut buffer = [0u16; 2];
        units.extend_from_slice(character.encode_utf16(&mut buffer));
        self.position = end;
        Ok(())
    }

    fn number(&mut self) -> Result<f64, StateError> {
        let start = self.position;
        if self.peek() == Some(b'-') {
            self.position += 1;
        }
        match self.peek().ok_or(StateError)? {
            b'0' => self.position += 1,
            b'1'..=b'9' => self.digits(),
            _ => return Err(StateError),
        }
        if self.peek() == Some(b'.') {
            self.position += 1;
            if !matches!(self.peek(), Some(b'0'..=b'9')) {
                return Err(StateError);
            }
            self.digits();
        }
        if matches!(self.peek(), Some(b'e' | b'E')) {
            self.position += 1;
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.position += 1;
            }
            if !matches!(self.peek(), Some(b'0'..=b'9')) {
                return Err(StateError);
            }
            self.digits();
        }
        let text =
            std::str::from_utf8(&self.bytes[start..self.position]).map_err(|_| StateError)?;
        text.parse::<f64>().map_err(|_| StateError)
    }

    fn digits(&mut self) {
        while matches!(self.peek(), Some(b'0'..=b'9')) {
            self.position += 1;
        }
    }

    fn skip_space(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.position += 1;
        }
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.position).copied()
    }

    fn next(&mut self) -> Option<u8> {
        let byte = self.peek()?;
        self.position += 1;
        Some(byte)
    }
}

fn hex(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// JSON.parse 遇到重复键时保留首次插入位置并替换值。
fn insert_property(entries: &mut Vec<(JsonString, Json)>, key: JsonString, value: Json) {
    if let Some((_, existing)) = entries.iter_mut().find(|(name, _)| *name == key) {
        *existing = value;
    } else {
        entries.push((key, value));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state(text: &str) -> Json {
        parse(text).expect("valid json")
    }

    #[test]
    fn canonical_text_matches_js_ordering_and_numbers() {
        let cases = [
            (
                r#"{"cookies":[],"localStorage":[]}"#,
                r#"{"cookies":[],"localStorage":[]}"#,
            ),
            (
                r#" { "b" : 1, "2" : 2, "a" : 3, "1" : 4 } "#,
                r#"{"1":4,"2":2,"b":1,"a":3}"#,
            ),
            (r#"{"x":1.0}"#, r#"{"x":1}"#),
            (r#"{"x":1e21}"#, r#"{"x":1e+21}"#),
            (r#"{"x":1e-7}"#, r#"{"x":1e-7}"#),
            (r#"{"x":0.000001}"#, r#"{"x":0.000001}"#),
            (r#"{"x":1e20}"#, r#"{"x":100000000000000000000}"#),
            (r#"{"x":667082108456853.2}"#, r#"{"x":667082108456853.2}"#),
            (r#"{"x":667082108456853.3}"#, r#"{"x":667082108456853.2}"#),
            (r#"{"x":-667082108456853.2}"#, r#"{"x":-667082108456853.2}"#),
            (r#"{"x":5e-324}"#, r#"{"x":5e-324}"#),
            (r#"{"x":-5e-324}"#, r#"{"x":-5e-324}"#),
            (
                r#"{"x":1.7976931348623157e308}"#,
                r#"{"x":1.7976931348623157e+308}"#,
            ),
            (
                r#"{"x":2.2250738585072014e-308}"#,
                r#"{"x":2.2250738585072014e-308}"#,
            ),
            (
                r#"{"x":0.30000000000000004}"#,
                r#"{"x":0.30000000000000004}"#,
            ),
            (r#"{"x":-0}"#, r#"{"x":0}"#),
            (r#"{"x":9007199254740993}"#, r#"{"x":9007199254740992}"#),
            (r#"{"x":1e400}"#, r#"{"x":null}"#),
            (r#"{"a":1,"b":2,"a":3}"#, r#"{"a":3,"b":2}"#),
            (r#"{"x":"\ud800"}"#, r#"{"x":"\ud800"}"#),
            (r#"{"x":"\ud83d\ude00"}"#, "{\"x\":\"\u{1f600}\"}"),
        ];
        for (input, expected) in cases {
            assert_eq!(serialize(&state(input)), expected, "{input}");
        }
    }

    #[test]
    fn state_validation_matches_the_persisted_contract() {
        let valid = r#"{"cookies":[{"name":"a","value":"b","host":"h","path":"/","secure":false,"httpOnly":true,"hostOnly":true,"sameSite":null,"createdAt":1.5,"expiresAt":null,"extra":{"x":1}}],"localStorage":[["http://example.com",[["k","v"]]]],"extra":true}"#;
        assert!(canonicalize_state(&state(valid)).is_ok());
        let invalid = [
            r#"{"cookies":[],"localStorage":{}}"#,
            r#"{"cookies":[{ "name":"a"}],"localStorage":[]}"#,
            r#"{"cookies":[],"localStorage":[["http://example.com",[["k","v"],["k","w"]]]]}"#,
            r#"{"cookies":[],"localStorage":[["https://example.com:443",[]]]}"#,
            r#"{"cookies":[],"localStorage":[["file:///x",[]]]}"#,
            r#"{"cookies":[],"localStorage":[["http://example.com",[]],["http://example.com",[]]]}"#,
            r#"{"cookies":[],"localStorage":[["http://example.com",[["k","v","x"]]]]}"#,
        ];
        for input in invalid {
            assert!(canonicalize_state(&state(input)).is_err(), "{input}");
        }
    }

    #[test]
    fn overall_state_limit_is_enforced_at_sixty_four_mebibytes() {
        let value = "a".repeat(2_621_439);
        let mut text = String::with_capacity(70 * 1024 * 1024);
        text.push_str(r#"{"cookies":[],"localStorage":["#);
        for index in 0..26 {
            if index > 0 {
                text.push(',');
            }
            text.push_str(&format!(r#"["http://o{index}.example",[["k","{value}"]]]"#));
        }
        text.push_str("]}");
        assert!(text.len() > 64 * 1024 * 1024);
        assert!(canonicalize_state(&parse(&text).expect("state")).is_err());
    }

    #[test]
    fn origin_validation_is_url_serialization() {
        for origin in [
            "http://example.com",
            "https://example.com",
            "http://127.0.0.1:8080",
            "https://[::1]",
            "http://xn--bcher-kva.example",
        ] {
            assert!(valid_origin(&utf16(origin)), "{origin}");
        }
        for origin in [
            "http://example.com/",
            "http://example.com:80",
            "HTTP://EXAMPLE.COM",
            "https://user@example.com",
            "ftp://example.com",
            "",
        ] {
            assert!(!valid_origin(&utf16(origin)), "{origin}");
        }
    }
}
