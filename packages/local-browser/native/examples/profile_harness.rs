// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// 仅测试使用的 profile 驱动：从 stdin 逐行读取 JSON 命令并输出结果，不安装、不提供 HTTP。
// revision 和 state 以原始 JSON 文本字符串传递，避免测试协议本身重写数字或孤立 surrogate。

use std::io::{BufRead, Write};
use std::path::Path;

use kitesurf_local_host::profile::{ProfileError, ProfileStore};
use kitesurf_local_host::profile_state;
use serde::Deserialize;
use serde_json::{json, Value};

#[derive(Deserialize)]
struct Command {
    command: String,
    path: Option<String>,
    create: Option<bool>,
    owner: Option<String>,
    revision: Option<String>,
    state: Option<String>,
    destination: Option<String>,
}

fn main() {
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    let mut store: Option<ProfileStore> = None;
    for line in stdin.lock().lines() {
        let Ok(line) = line else { break };
        if line.trim().is_empty() {
            continue;
        }
        let response = match serde_json::from_str::<Command>(&line) {
            Ok(command) => handle(command, &mut store),
            Err(_) => error("protocol", "invalid command"),
        };
        let _ = writeln!(stdout, "{response}");
        let _ = stdout.flush();
    }
}

fn handle(command: Command, store: &mut Option<ProfileStore>) -> Value {
    match command.command.as_str() {
        "open" => {
            let Some(path) = command.path else {
                return error("protocol", "missing path");
            };
            *store = None;
            match ProfileStore::open(Path::new(&path), command.create.unwrap_or(false)) {
                Ok(opened) => {
                    *store = Some(opened);
                    status_value(store.as_ref().expect("just opened"))
                }
                Err(_) => error("open", "cannot open profile"),
            }
        }
        "acquire" => {
            let (Some(store), Some(owner)) = (store.as_mut(), command.owner) else {
                return error("protocol", "missing open profile or owner");
            };
            let owner: Vec<u16> = owner.encode_utf16().collect();
            match store.acquire(&owner) {
                Ok(read) => json!({
                  "ok": true,
                  "revision": read.revision,
                  "state": read.state_text,
                }),
                Err(failure) => profile_error(failure),
            }
        }
        "save" => {
            let Some(store) = store.as_mut() else {
                return error("protocol", "no open profile");
            };
            let (Some(owner), Some(revision), Some(state_text)) =
                (command.owner, command.revision, command.state)
            else {
                return error("protocol", "missing save fields");
            };
            let owner: Vec<u16> = owner.encode_utf16().collect();
            let Ok(revision) = profile_state::parse(&revision) else {
                return error("protocol", "invalid revision fixture");
            };
            let Ok(state) = profile_state::parse(&state_text) else {
                return error("protocol", "invalid state fixture");
            };
            match store.save(&owner, &revision, Some(&state)) {
                Ok(()) => json!({"ok": true}),
                Err(failure) => profile_error(failure),
            }
        }
        "status" => match store.as_ref() {
            Some(store) => status_value(store),
            None => error("protocol", "no open profile"),
        },
        "backup" => {
            let (Some(store), Some(destination)) = (store.as_ref(), command.destination) else {
                return error("protocol", "no open profile or destination");
            };
            match store.backup(Path::new(&destination)) {
                Ok(produced) => json!({"ok": true, "path": produced.display().to_string()}),
                Err(_) => error("backup", "backup failed"),
            }
        }
        "close" => {
            *store = None;
            json!({"ok": true})
        }
        "quit" => json!({"ok": true}),
        _ => error("protocol", "unknown command"),
    }
}

fn status_value(store: &ProfileStore) -> Value {
    match store.status() {
        Ok(status) => json!({
          "ok": true,
          "path": status.path.display().to_string(),
          "revision": status.revision,
          "format": status.format,
          "id": status.id,
        }),
        Err(failure) => profile_error(failure),
    }
}

fn profile_error(failure: ProfileError) -> Value {
    let kind = match failure.kind() {
        kitesurf_local_host::profile::ProfileErrorKind::InvalidState => "invalid_state",
        kitesurf_local_host::profile::ProfileErrorKind::Conflict => "conflict",
    };
    error(kind, &failure.to_string())
}

fn error(kind: &str, message: &str) -> Value {
    json!({"ok": false, "kind": kind, "message": message})
}
