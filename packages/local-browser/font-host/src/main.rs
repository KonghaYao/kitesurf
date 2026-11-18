// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

//! Length-framed stdin/stdout entry point for the local system-font helper.

use std::io::{BufReader, Read, Write};

use kitesurf_font_host::{
    encode_response, parse_frame_header, FontHost, HostError, MatchRequest, MAX_FACE_BYTES,
    MAX_FRAME_HEADER_BYTES,
};
use serde_json::{json, Value};

const MAX_REQUEST_BODY_BYTES: u32 = 64 * 1024;

fn main() {
    let stdin = std::io::stdin();
    let mut reader = BufReader::new(stdin.lock());
    let stdout = std::io::stdout();
    let mut writer = stdout.lock();
    let mut host = FontHost::new();
    loop {
        let Ok(Some(header)) = read_frame(&mut reader) else {
            break;
        };
        let response = match handle(&mut host, &header) {
            Ok((header, body, shutdown)) => {
                write_frame(&mut writer, &header, &body);
                shutdown
            }
            Err(error) => {
                write_frame(&mut writer, &error_json(&error), &[]);
                false
            }
        };
        if response {
            break;
        }
    }
}

fn handle(host: &mut FontHost, header: &[u8]) -> Result<(Value, Vec<u8>, bool), HostError> {
    let value = parse_frame_header(header)?;
    let op = value
        .get("op")
        .and_then(Value::as_str)
        .ok_or_else(|| HostError::new(kitesurf_font_host::ErrorCode::BadRequest, "missing op"))?
        .to_string();
    match op.as_str() {
        "probe" => {
            let probe = host.probe();
            Ok((json!({"ok": true, "probe": probe}), Vec::new(), false))
        }
        "match" => {
            let mut object = value.as_object().cloned().ok_or_else(|| {
                HostError::new(kitesurf_font_host::ErrorCode::BadRequest, "invalid match")
            })?;
            object.remove("op");
            let request: MatchRequest =
                serde_json::from_value(Value::Object(object)).map_err(|_| {
                    HostError::new(kitesurf_font_host::ErrorCode::BadRequest, "invalid match")
                })?;
            let response = host.match_faces(&request)?;
            Ok((json!({"ok": true, "match": response}), Vec::new(), false))
        }
        "face" => {
            let id = value.get("id").and_then(Value::as_str).ok_or_else(|| {
                HostError::new(kitesurf_font_host::ErrorCode::BadRequest, "missing id")
            })?;
            let bytes = host.face_bytes(id)?;
            if bytes.len() > MAX_FACE_BYTES {
                return Err(HostError::new(
                    kitesurf_font_host::ErrorCode::FaceTooLarge,
                    "font response exceeds the size budget",
                ));
            }
            Ok((
                json!({"ok": true, "id": id, "bytes": bytes.len()}),
                bytes.as_ref().clone(),
                false,
            ))
        }
        "stats" => {
            let stats = host.stats();
            Ok((json!({"ok": true, "stats": stats}), Vec::new(), false))
        }
        "shutdown" => Ok((json!({"ok": true}), Vec::new(), true)),
        _ => Err(HostError::new(
            kitesurf_font_host::ErrorCode::BadRequest,
            "unknown op",
        )),
    }
}

fn read_frame(reader: &mut impl Read) -> std::io::Result<Option<Vec<u8>>> {
    let mut length = [0u8; 4];
    match reader.read_exact(&mut length) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error),
    }
    parse_length(length, MAX_FRAME_HEADER_BYTES as usize)?;
    let mut header = vec![0u8; u32::from_be_bytes(length) as usize];
    reader.read_exact(&mut header)?;
    let mut body_length = [0u8; 4];
    reader.read_exact(&mut body_length)?;
    parse_length(body_length, MAX_REQUEST_BODY_BYTES as usize)?;
    let mut body = vec![0u8; u32::from_be_bytes(body_length) as usize];
    reader.read_exact(&mut body)?;
    Ok(Some(header))
}

fn parse_length(length: [u8; 4], max: usize) -> std::io::Result<[u8; 4]> {
    let value = u32::from_be_bytes(length);
    if value as usize > max {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "frame exceeds the size limit",
        ));
    }
    Ok(length)
}

fn write_frame(writer: &mut impl Write, header: &Value, body: &[u8]) {
    let encoded = encode_response(header, body);
    let _ = writer.write_all(&encoded);
    let _ = writer.flush();
}

fn error_json(error: &HostError) -> Value {
    json!({"ok": false, "code": error.code.as_str(), "message": error.message})
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn frames_round_trip() {
        let mut frames = Vec::new();
        frames.extend_from_slice(&2u32.to_be_bytes());
        frames.extend_from_slice(b"{}");
        frames.extend_from_slice(&0u32.to_be_bytes());
        let mut cursor = Cursor::new(frames);
        let header = read_frame(&mut cursor).unwrap().unwrap();
        assert_eq!(header, b"{}");
        assert!(read_frame(&mut cursor).unwrap().is_none());
    }

    #[test]
    fn oversized_frames_are_rejected() {
        let mut frames = Vec::new();
        frames.extend_from_slice(&(MAX_FRAME_HEADER_BYTES + 1).to_be_bytes());
        let mut cursor = Cursor::new(frames);
        assert!(read_frame(&mut cursor).is_err());
    }

    #[test]
    fn response_encoding_has_header_and_body_lengths() {
        let encoded = encode_response(&json!({"ok": true}), b"abc");
        assert_eq!(
            u32::from_be_bytes(encoded[0..4].try_into().unwrap()) as usize,
            11
        );
        assert_eq!(&encoded[4..15], br#"{"ok":true}"#);
        assert_eq!(u32::from_be_bytes(encoded[15..19].try_into().unwrap()), 3);
        assert_eq!(&encoded[19..], b"abc");
    }
}
