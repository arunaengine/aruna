//! Reads automation firings and kept events with group-scoped authorization.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::cmp::Reverse;

use aruna_core::automation::firing::{Firing, FiringState};
use aruna_core::automation::keys::{FIRING_ROW, firing_key, firing_prefix, index_key, kept_prefix};
use aruna_core::automation::record::{Automation, KeptEvent};
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{AUTOMATION_RUNS_KEYSPACE, AUTOMATIONS_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::group_permission_path;
use aruna_core::types::{GroupId, Key};
use ulid::Ulid;

use super::{AutoAccessOperation, AutoError, Need, allowed, authorize_path};

pub(crate) fn decode_ulid(bytes: &[u8]) -> Option<Ulid> {
    <[u8; 16]>::try_from(bytes).ok().map(Ulid::from_bytes)
}

#[derive(Debug, PartialEq)]
enum GetStep {
    Index,
    Access,
    Firing(Box<Automation>),
}

/// Loads one firing by `firing_id` after verifying the caller reads its automation's group.
#[derive(Debug, PartialEq)]
pub struct GetFiringOperation {
    auth: AuthContext,
    firing_id: Ulid,
    local: NodeId,
    step: GetStep,
}

impl GetFiringOperation {
    pub fn new(auth: &AuthContext, firing_id: Ulid, local: NodeId) -> Self {
        Self {
            auth: auth.clone(),
            firing_id,
            local,
            step: GetStep::Index,
        }
    }
}

impl Operation for GetFiringOperation {
    type Output = (Firing, Automation);
    type Error = AutoError;

    fn first(&mut self) -> Result<Next<Self::Output>, AutoError> {
        Ok(Next::Call(IoCall::Read(vec![(
            AUTOMATION_RUNS_KEYSPACE.to_string(),
            index_key(self.firing_id),
        )])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, AutoError> {
        match std::mem::replace(&mut self.step, GetStep::Index) {
            GetStep::Index => {
                let bytes = result.value()?.ok_or(AutoError::NotFound)?;
                let auto_id = decode_ulid(&bytes).ok_or(AutoError::NotFound)?;
                self.step = GetStep::Access;
                let check = AutoAccessOperation::new(&self.auth, auto_id, self.local, Need::Read);
                Ok(Next::Call(IoCall::child(check)))
            }
            GetStep::Access => {
                let auto = result.child::<Automation, AutoError>()??;
                let key = firing_key(auto.automation_id, self.firing_id);
                self.step = GetStep::Firing(Box::new(auto));
                Ok(Next::Call(IoCall::Read(vec![(
                    AUTOMATION_RUNS_KEYSPACE.to_string(),
                    key,
                )])))
            }
            GetStep::Firing(auto) => {
                let bytes = result.value()?.ok_or(AutoError::NotFound)?;
                let firing = Firing::from_bytes(&bytes)?;
                Ok(Next::Done((firing, *auto)))
            }
        }
    }

    fn expected_error(error: &AutoError) -> bool {
        matches!(error, AutoError::NotFound | AutoError::Forbidden)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct FiringPage {
    pub items: Vec<Firing>,
    pub next: Option<Ulid>,
}

/// Lists firings for one automation or one group, newest first, filtered by optional state.
#[derive(Debug, PartialEq)]
pub struct ListFiringsOperation {
    auth: AuthContext,
    group_id: Option<GroupId>,
    automation_id: Option<Ulid>,
    state: Option<FiringState>,
    local: NodeId,
    after: Option<Ulid>,
    limit: usize,
    authorized: bool,
    found: Vec<Firing>,
}

impl ListFiringsOperation {
    pub fn new(
        auth: &AuthContext,
        (group_id, automation_id): (Option<GroupId>, Option<Ulid>),
        state: Option<FiringState>,
        local: NodeId,
        after: Option<Ulid>,
        limit: usize,
    ) -> Self {
        Self {
            auth: auth.clone(),
            group_id,
            automation_id,
            state,
            local,
            after,
            limit: limit.clamp(1, 500),
            authorized: false,
            found: Vec::new(),
        }
    }

    fn page(&self, after: Option<Key>) -> Next<FiringPage> {
        let prefix = match self.automation_id {
            Some(id) => firing_prefix(id),
            None => Key::from(vec![FIRING_ROW]),
        };
        Next::Call(IoCall::Page {
            space: AUTOMATION_RUNS_KEYSPACE.to_string(),
            prefix,
            after,
            limit: 256,
        })
    }
}

impl Operation for ListFiringsOperation {
    type Output = FiringPage;
    type Error = AutoError;

    fn first(&mut self) -> Result<Next<FiringPage>, AutoError> {
        if let Some(auto_id) = self.automation_id {
            let check = AutoAccessOperation::new(&self.auth, auto_id, self.local, Need::Read);
            return Ok(Next::Call(IoCall::child(check)));
        }
        let group = self
            .group_id
            .ok_or_else(|| AutoError::Invalid("provide group_id or automation_id".into()))?;
        let path = group_permission_path(self.auth.realm_id, group, self.local);
        Ok(Next::Call(authorize_path(
            &self.auth,
            &path,
            Permission::READ,
        )))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<FiringPage>, AutoError> {
        if !self.authorized {
            if self.automation_id.is_some() {
                let auto = result.child::<Automation, AutoError>()??;
                self.group_id = Some(auto.group_id);
            } else if !allowed(result)? {
                return Err(AutoError::NotFound);
            }
            self.authorized = true;
            return Ok(self.page(None));
        }
        let (rows, next) = result.page()?;
        for (_, value) in rows {
            let firing = Firing::from_bytes(&value)?;
            if self.group_id.is_none_or(|g| firing.group_id == g)
                && self.state.is_none_or(|s| firing.state == s)
            {
                self.found.push(firing);
            }
        }
        if let Some(after) = next {
            return Ok(self.page(Some(after)));
        }
        let mut items = std::mem::take(&mut self.found);
        items.sort_by_key(|firing| Reverse(firing.firing_id));
        if let Some(after) = self.after {
            items.retain(|firing| firing.firing_id < after);
        }
        let has_more = items.len() > self.limit;
        items.truncate(self.limit);
        let cursor = has_more
            .then(|| items.last().map(|item| item.firing_id))
            .flatten();
        Ok(Next::Done(FiringPage {
            items,
            next: cursor,
        }))
    }

    fn expected_error(error: &AutoError) -> bool {
        !matches!(error, AutoError::Storage(_))
    }
}

/// Lists kept (unstarted/paused/throttled) matched events for an automation, newest first.
#[derive(Debug, PartialEq)]
pub struct ListEventsOperation {
    auth: AuthContext,
    id: Ulid,
    local: NodeId,
    limit: usize,
    authorized: bool,
    found: Vec<KeptEvent>,
}

impl ListEventsOperation {
    pub fn new(auth: &AuthContext, id: Ulid, local: NodeId, limit: usize) -> Self {
        Self {
            auth: auth.clone(),
            id,
            local,
            limit: limit.clamp(1, 500),
            authorized: false,
            found: Vec::new(),
        }
    }

    fn page(&self, after: Option<Key>) -> Next<Vec<KeptEvent>> {
        Next::Call(IoCall::Page {
            space: AUTOMATIONS_KEYSPACE.to_string(),
            prefix: kept_prefix(self.id),
            after,
            limit: 256,
        })
    }
}

impl Operation for ListEventsOperation {
    type Output = Vec<KeptEvent>;
    type Error = AutoError;

    fn first(&mut self) -> Result<Next<Vec<KeptEvent>>, AutoError> {
        let check = AutoAccessOperation::new(&self.auth, self.id, self.local, Need::Read);
        Ok(Next::Call(IoCall::child(check)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Vec<KeptEvent>>, AutoError> {
        if !self.authorized {
            result.child::<Automation, AutoError>()??;
            self.authorized = true;
            return Ok(self.page(None));
        }
        let (rows, next) = result.page()?;
        for (_, value) in rows {
            self.found.push(KeptEvent::from_bytes(&value)?);
        }
        if let Some(after) = next {
            return Ok(self.page(Some(after)));
        }
        let mut items = std::mem::take(&mut self.found);
        items.sort_by_key(|ev| Reverse(ev.event_id));
        items.truncate(self.limit);
        Ok(Next::Done(items))
    }

    fn expected_error(error: &AutoError) -> bool {
        matches!(error, AutoError::NotFound | AutoError::Forbidden)
    }
}
