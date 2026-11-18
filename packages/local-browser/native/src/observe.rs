// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// 仅用于测试的观察汇：每次运行一个 JSONL 文件。它由 --test-observe 启用，不会通过公共
// 监听器暴露，内容只留在测试临时目录内。生产 serve 从不打开它。

use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde_json::{json, Value};

pub struct Observer {
    file: Mutex<Option<File>>,
    path: Option<PathBuf>,
}

impl Observer {
    pub fn disabled() -> Self {
        Self {
            file: Mutex::new(None),
            path: None,
        }
    }

    pub fn create(path: &Path) -> std::io::Result<Self> {
        let file = File::options().create(true).append(true).open(path)?;
        Ok(Self {
            file: Mutex::new(Some(file)),
            path: Some(path.to_path_buf()),
        })
    }

    pub fn enabled(&self) -> bool {
        self.path.is_some()
    }

    pub fn record(&self, event: Value) {
        let Ok(mut guard) = self.file.lock() else {
            return;
        };
        let Some(file) = guard.as_mut() else {
            return;
        };
        let line = format!("{event}\n");
        if let Err(error) = file.write_all(line.as_bytes()) {
            eprintln!("[observe] write failed: {error}");
            return;
        }
        let _ = file.flush();
    }

    pub fn kind(&self, kind: &str) {
        self.record(json!({ "kind": kind }));
    }

    pub fn with(&self, kind: &str, fields: Value) {
        let mut event = match fields {
            Value::Object(map) => Value::Object(map),
            other => json!({ "value": other }),
        };
        if let Value::Object(map) = &mut event {
            map.insert("kind".to_string(), json!(kind));
        }
        self.record(event);
    }
}
