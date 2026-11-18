// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// attachment 归属只在 broker actor 内同步维护；本模块不启动任务、不加锁，也不决定
// 可见性。归属优先级与 Node 宿主一致：显式 attach intent、已知 parent session、当前
// autoattach owner。事件先解析接收者再修改状态，断开时只回收连接自己的 attachment。

use std::collections::HashMap;

use crate::broker::ClientId;
use crate::transport::{PayloadBudget, PayloadLease};

pub struct Attachment {
    pub session: String,
    pub target: String,
    pub parent: Option<String>,
    pub client: ClientId,
    pub epoch: u64,
    _payload: PayloadLease,
}

#[derive(Default)]
pub struct Attachments {
    sessions: HashMap<String, Attachment>,
    explicit: HashMap<String, (ClientId, PayloadLease)>,
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

    pub fn own(
        &mut self,
        client: ClientId,
        epoch: u64,
        session: &str,
        target: &str,
        parent: Option<String>,
    ) -> bool {
        let bytes = session
            .len()
            .saturating_mul(2)
            .saturating_add(target.len())
            .saturating_add(parent.as_ref().map_or(0, String::len));
        let Some(payload) = self.budget.try_acquire(bytes, false) else {
            return false;
        };
        self.sessions.insert(
            session.to_string(),
            Attachment {
                _payload: payload,
                session: session.to_string(),
                target: target.to_string(),
                parent,
                client,
                epoch,
            },
        );
        true
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
        self.explicit.get(target).map(|(client, _)| *client)
    }

    pub fn set_intent(&mut self, target: &str, client: ClientId) -> bool {
        let Some(lease) = self.budget.try_acquire(target.len(), false) else {
            return false;
        };
        self.explicit.insert(target.to_string(), (client, lease));
        true
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
}
