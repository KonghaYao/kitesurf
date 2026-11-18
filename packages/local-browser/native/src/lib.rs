// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// T0-T2 的原生本地宿主：进程监管、私有 profile 与资源服务，以及仅用于测试的单客户端 CDP
// 中继。正式的 SQLite profile 与多客户端代理属于后续任务。

pub mod attachments;
pub mod broker;
pub mod cdp_raw;
pub mod observe;
pub mod profile;
pub mod profile_state;
pub mod relay;
pub mod services;
pub mod supervisor;
pub mod transport;

/// 跨任务边界的错误必须具体；这些错误只需可打印且开销低。
pub type HostResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// 格式化错误链但不分配回溯：宿主只负责打印。
pub fn describe_error(error: &(dyn std::error::Error + 'static)) -> String {
    let mut message = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        message.push_str(": ");
        message.push_str(&cause.to_string());
        source = cause.source();
    }
    message
}
