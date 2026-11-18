// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// attachment 归属只在 broker actor 内同步维护；本模块不启动任务、不加锁，也不决定
// 可见性。归属优先级与 Node 宿主一致：显式 attach intent、已知 parent session、当前
// autoattach owner。事件先解析接收者再修改状态，断开时只回收连接自己的 attachment。
// 数量上限在这里统一检查；超限但已在上游创建的 session 只进入有界 quarantine。

use std::collections::HashMap;

use crate::broker::ClientId;
use crate::transport::{PayloadBudget, PayloadLease};

pub const MAX_ATTACHMENTS: usize = 1024;
pub const MAX_ATTACHMENTS_PER_CLIENT: usize = 256;
pub const MAX_QUARANTINE: usize = 32;

pub struct Attachment {
    pub session: String,
    pub target: String,
    pub parent: Option<String>,
    pub client: ClientId,
    pub epoch: u64,
    _payload: PayloadLease,
}

struct Intent {
    client: ClientId,
    _epoch: u64,
    reserved: bool,
    _payload: PayloadLease,
}

pub struct Quarantine {
    pub session: String,
    pub target: String,
    pub client: ClientId,
    pub epoch: u64,
    pub pending: Option<u64>,
    pub blocked: bool,
    _payload: PayloadLease,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AttachmentReservation {
    Reserved,
    Busy,
    Full,
    Budget,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Registration {
    New,
    Existing,
    Foreign,
    Quarantined,
    Full,
    Budget,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QuarantineResult {
    Recorded,
    Existing,
    Owned,
    Foreign,
    Full,
    Budget,
}

#[derive(Default)]
pub struct Attachments {
    sessions: HashMap<String, Attachment>,
    explicit: HashMap<String, Intent>,
    quarantine: HashMap<String, Quarantine>,
    budget: PayloadBudget,
    pub auto_owner: Option<ClientId>,
}

impl Attachments {
    pub fn new(budget: PayloadBudget) -> Self {
        Self {
            budget,
            ..Self::default()
        }
    }

    pub fn count(&self) -> usize {
        self.sessions.len()
    }

    pub fn count_for(&self, client: ClientId) -> usize {
        self.sessions
            .values()
            .filter(|attachment| attachment.client == client)
            .count()
    }

    /// client 槽只有在 session、intent、quarantine 与 auto owner 全部清空后才可回收。
    pub fn has_client(&self, client: ClientId) -> bool {
        self.auto_owner == Some(client)
            || self
                .sessions
                .values()
                .any(|attachment| attachment.client == client)
            || self.explicit.values().any(|intent| intent.client == client)
            || self.quarantine.values().any(|entry| entry.client == client)
    }

    /// 派发前占一个 client/global 槽；在途 attach 与早期事件共享该预留。
    pub fn reserve(&mut self, target: &str, client: ClientId, epoch: u64) -> AttachmentReservation {
        if self.explicit.contains_key(target) {
            return AttachmentReservation::Busy;
        }
        if !self.can_reserve(client) {
            return AttachmentReservation::Full;
        }
        let Some(payload) = self.budget.try_acquire(target.len(), false) else {
            return AttachmentReservation::Budget;
        };
        self.explicit.insert(
            target.to_string(),
            Intent {
                client,
                _epoch: epoch,
                reserved: true,
                _payload: payload,
            },
        );
        AttachmentReservation::Reserved
    }

    /// 统一注册入口。已有同一 session 不重复计槽；预留只在真正插入时消耗。
    pub fn register(
        &mut self,
        client: ClientId,
        epoch: u64,
        session: &str,
        target: &str,
        parent: Option<&str>,
    ) -> Registration {
        if let Some(existing) = self.sessions.get(session) {
            return if existing.client == client {
                Registration::Existing
            } else {
                Registration::Foreign
            };
        }
        if self.quarantine.contains_key(session) {
            return Registration::Quarantined;
        }
        let mut reserved = false;
        if let Some(intent) = self.explicit.get(target) {
            if intent.client != client {
                return Registration::Foreign;
            }
            reserved = intent.reserved;
        }
        if reserved {
            if !self.can_attach(client) {
                return Registration::Full;
            }
        } else if !self.can_reserve(client) {
            return Registration::Full;
        }
        let bytes = session
            .len()
            .saturating_mul(2)
            .saturating_add(target.len())
            .saturating_add(parent.map_or(0, str::len));
        // 已送出的 attach 必须登记归属以便退休清理；只允许有 attachment 槽的 metadata
        // 借 cleanup reserve，不能借给事件 payload 或客户端输出，forget 时随 attachment 释放。
        let Some(payload) = self.payload(bytes) else {
            return Registration::Budget;
        };
        if reserved {
            if let Some(intent) = self.explicit.get_mut(target) {
                intent.reserved = false;
            }
        }
        self.sessions.insert(
            session.to_string(),
            Attachment {
                _payload: payload,
                session: session.to_string(),
                target: target.to_string(),
                parent: parent.map(str::to_owned),
                client,
                epoch,
            },
        );
        Registration::New
    }

    /// 超限但上游已创建：只记清理身份，不进入 sessions/权限查询。
    pub fn quarantine(
        &mut self,
        client: ClientId,
        epoch: u64,
        session: &str,
        target: &str,
    ) -> QuarantineResult {
        if let Some(existing) = self.sessions.get(session) {
            return if existing.client == client {
                QuarantineResult::Owned
            } else {
                QuarantineResult::Foreign
            };
        }
        if self.quarantine.contains_key(session) {
            return QuarantineResult::Existing;
        }
        if self.quarantine.len() >= MAX_QUARANTINE {
            return QuarantineResult::Full;
        }
        let bytes = session.len().saturating_mul(2).saturating_add(target.len());
        let Some(payload) = self.payload(bytes) else {
            return QuarantineResult::Budget;
        };
        self.quarantine.insert(
            session.to_string(),
            Quarantine {
                _payload: payload,
                session: session.to_string(),
                target: target.to_string(),
                client,
                epoch,
                pending: None,
                blocked: false,
            },
        );
        QuarantineResult::Recorded
    }

    pub fn next_quarantine(&self) -> Option<(String, ClientId)> {
        self.quarantine
            .values()
            .find(|entry| entry.pending.is_none() && !entry.blocked)
            .map(|entry| (entry.session.clone(), entry.client))
    }

    pub fn set_quarantine_pending(&mut self, session: &str, pending: Option<u64>) -> bool {
        let Some(entry) = self.quarantine.get_mut(session) else {
            return false;
        };
        entry.pending = pending;
        true
    }

    pub fn block_quarantine(&mut self, session: &str) -> bool {
        let Some(entry) = self.quarantine.get_mut(session) else {
            return false;
        };
        entry.pending = None;
        entry.blocked = true;
        true
    }

    pub fn release_quarantine(&mut self, session: &str) -> Option<Quarantine> {
        self.quarantine.remove(session)
    }

    pub fn owner(&self, session: &str) -> Option<ClientId> {
        self.sessions
            .get(session)
            .map(|attachment| attachment.client)
    }

    pub fn attachment(&self, session: &str) -> Option<&Attachment> {
        self.sessions.get(session)
    }

    /// 先返回接收者，再由调用方决定投递；移除后不会再被当作广播事件。
    pub fn forget(&mut self, session: &str) -> Option<Attachment> {
        self.sessions.remove(session)
    }

    pub fn first_session(&self, client: ClientId) -> Option<String> {
        self.sessions
            .values()
            .filter(|attachment| attachment.client == client)
            .map(|attachment| &attachment.session)
            .min()
            .cloned()
    }

    pub fn has_target(&self, client: ClientId, target: &str) -> bool {
        self.sessions
            .values()
            .any(|attachment| attachment.client == client && attachment.target == target)
    }

    pub fn intent(&self, target: &str) -> Option<ClientId> {
        self.explicit.get(target).map(|intent| intent.client)
    }

    pub fn clear_intent(&mut self, target: &str, client: ClientId) {
        if self.intent(target) == Some(client) {
            self.explicit.remove(target);
        }
    }

    /// 早到的 attached 事件和 attach 回复使用同一优先级。
    pub fn resolve_owner(&self, parent: Option<&str>, target: &str) -> Option<ClientId> {
        self.intent(target)
            .or_else(|| parent.and_then(|session| self.owner(session)))
            .or(self.auto_owner)
    }

    fn payload(&self, bytes: usize) -> Option<PayloadLease> {
        self.budget.try_acquire(bytes, false).or_else(|| {
            (bytes <= 4 * 4096)
                .then(|| self.budget.try_acquire(bytes, true))
                .flatten()
        })
    }

    fn can_attach(&self, client: ClientId) -> bool {
        self.sessions.len() < MAX_ATTACHMENTS && self.count_for(client) < MAX_ATTACHMENTS_PER_CLIENT
    }

    fn can_reserve(&self, client: ClientId) -> bool {
        let reserved = self
            .explicit
            .values()
            .filter(|intent| intent.reserved)
            .count();
        let reserved_for = self
            .explicit
            .values()
            .filter(|intent| intent.reserved && intent.client == client)
            .count();
        self.sessions.len() + reserved < MAX_ATTACHMENTS
            && self.count_for(client) + reserved_for < MAX_ATTACHMENTS_PER_CLIENT
    }
}
