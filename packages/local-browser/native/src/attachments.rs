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
    page: Option<String>,
    _payload: PayloadLease,
}

struct Intent {
    client: ClientId,
    _epoch: u64,
    reserved: bool,
    _payload: PayloadLease,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QuarantineState {
    Ready,
    InFlight,
    AwaitingLate,
    ConfirmedFailure,
}

pub struct Quarantine {
    pub session: String,
    pub target: String,
    pub client: ClientId,
    pub epoch: u64,
    pub pending: Option<u64>,
    pub state: QuarantineState,
    _payload: PayloadLease,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AttachmentReservation {
    Reserved,
    Busy,
    Full,
    Budget,
    /// target 已登记给另一个 page session。
    ForeignPage,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Registration {
    New,
    Existing,
    Foreign,
    Quarantined,
    Full,
    Budget,
    /// target 已登记给另一个 page session，不能为当前连接建立 attachment。
    ForeignPage,
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

struct PageClaim {
    page: String,
    attachments: usize,
}

#[derive(Default)]
pub struct Attachments {
    sessions: HashMap<String, Attachment>,
    explicit: HashMap<String, Intent>,
    quarantine: HashMap<String, Quarantine>,
    /// target 独占：同一 target 只归属一个 page session，最后一个 attachment 释放时归还。
    /// Browser 端点连接不登记，保持实例级可见性。
    claims: HashMap<String, PageClaim>,
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
    pub fn reserve(
        &mut self,
        target: &str,
        client: ClientId,
        epoch: u64,
        page: Option<&str>,
    ) -> AttachmentReservation {
        if self.explicit.contains_key(target) {
            return AttachmentReservation::Busy;
        }
        if page.is_some_and(|page| !self.page_accepts(target, page)) {
            return AttachmentReservation::ForeignPage;
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
        page: Option<&str>,
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
        if page.is_some_and(|page| !self.page_accepts(target, page)) {
            return Registration::ForeignPage;
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
                page: page.map(str::to_owned),
            },
        );
        if let Some(page) = page {
            match self.claims.get_mut(target) {
                Some(claim) => claim.attachments = claim.attachments.saturating_add(1),
                None => {
                    self.claims.insert(
                        target.to_string(),
                        PageClaim {
                            page: page.to_string(),
                            attachments: 1,
                        },
                    );
                }
            }
        }
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
                state: QuarantineState::Ready,
            },
        );
        QuarantineResult::Recorded
    }

    pub fn next_quarantine(&self) -> Option<(String, ClientId, u64)> {
        self.quarantine
            .values()
            .find(|entry| entry.pending.is_none() && entry.state == QuarantineState::Ready)
            .map(|entry| (entry.session.clone(), entry.client, entry.epoch))
    }

    /// 发送成功后才进入 InFlight；身份不匹配时不改写记录。
    pub fn mark_quarantine_sent(
        &mut self,
        session: &str,
        upstream: u64,
        client: ClientId,
        epoch: u64,
    ) -> bool {
        let Some(entry) = self.quarantine.get_mut(session) else {
            return false;
        };
        if entry.client != client || entry.epoch != epoch || entry.state != QuarantineState::Ready {
            return false;
        }
        entry.pending = Some(upstream);
        entry.state = QuarantineState::InFlight;
        true
    }

    /// 超时只把精确匹配的 InFlight 转为等待迟到结果，保留 upstream 身份。
    pub fn mark_quarantine_awaiting(
        &mut self,
        session: &str,
        upstream: u64,
        client: ClientId,
        epoch: u64,
    ) -> bool {
        let Some(entry) = self.quarantine.get_mut(session) else {
            return false;
        };
        if entry.client != client
            || entry.epoch != epoch
            || entry.state != QuarantineState::InFlight
            || entry.pending != Some(upstream)
        {
            return false;
        }
        entry.state = QuarantineState::AwaitingLate;
        true
    }

    /// 成功只释放匹配 upstream/client/epoch 的记录，不碰其他 session。
    pub fn finish_quarantine_success(
        &mut self,
        session: &str,
        upstream: u64,
        client: ClientId,
        epoch: u64,
    ) -> Option<Quarantine> {
        let matches = self.quarantine.get(session).is_some_and(|entry| {
            entry.client == client && entry.epoch == epoch && entry.pending == Some(upstream)
        });
        matches.then(|| self.quarantine.remove(session)).flatten()
    }

    /// 明确失败保留清理身份，等待新的显式 flush/close 单次重试。
    pub fn finish_quarantine_failure(
        &mut self,
        session: &str,
        upstream: u64,
        client: ClientId,
        epoch: u64,
    ) -> bool {
        let Some(entry) = self.quarantine.get_mut(session) else {
            return false;
        };
        if entry.client != client || entry.epoch != epoch || entry.pending != Some(upstream) {
            return false;
        }
        entry.pending = None;
        entry.state = QuarantineState::ConfirmedFailure;
        true
    }

    /// 只有新的显式屏障可以复活确认失败；tick 与在途回复不得自动重试。
    pub fn retry_quarantine(&mut self, session: &str, client: ClientId, epoch: u64) -> bool {
        let Some(entry) = self.quarantine.get_mut(session) else {
            return false;
        };
        if entry.client != client || entry.epoch != epoch {
            return false;
        }
        if entry.state != QuarantineState::ConfirmedFailure {
            return false;
        }
        entry.state = QuarantineState::Ready;
        true
    }

    pub fn confirmed_quarantine(&self) -> Vec<(String, ClientId, u64)> {
        self.quarantine
            .values()
            .filter(|entry| entry.state == QuarantineState::ConfirmedFailure)
            .map(|entry| (entry.session.clone(), entry.client, entry.epoch))
            .collect()
    }

    pub fn quarantine_awaiting_late(&self) -> bool {
        self.quarantine
            .values()
            .any(|entry| entry.state == QuarantineState::AwaitingLate)
    }

    pub fn quarantine_confirmed_failure(&self) -> bool {
        self.quarantine
            .values()
            .any(|entry| entry.state == QuarantineState::ConfirmedFailure)
    }

    pub fn quarantine_count(&self) -> usize {
        self.quarantine.len()
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
    /// 释放最后一个页面归属引用时归还 target。
    pub fn forget(&mut self, session: &str) -> Option<Attachment> {
        let attachment = self.sessions.remove(session)?;
        if attachment.page.is_some() {
            self.release_claim(&attachment.target);
        }
        Some(attachment)
    }

    /// page session 是否仍可登记该 target；未登记的 target 由第一个 page session 取得。
    fn page_accepts(&self, target: &str, page: &str) -> bool {
        self.page_of_target(target)
            .is_none_or(|owner| owner == page)
    }

    fn release_claim(&mut self, target: &str) {
        let Some(claim) = self.claims.get_mut(target) else {
            return;
        };
        claim.attachments = claim.attachments.saturating_sub(1);
        if claim.attachments == 0 {
            self.claims.remove(target);
        }
    }

    /// Browser 端点连接不登记归属，因此这里只反映 page session 的 target 集合。
    pub fn page_of_target(&self, target: &str) -> Option<&str> {
        self.claims.get(target).map(|claim| claim.page.as_str())
    }

    pub fn owns_page_target(&self, page: &str, target: &str) -> bool {
        self.page_of_target(target) == Some(page)
    }

    pub fn targets_for_page(&self, page: &str) -> Vec<String> {
        self.claims
            .iter()
            .filter(|(_, claim)| claim.page == page)
            .map(|(target, _)| target.clone())
            .collect()
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
