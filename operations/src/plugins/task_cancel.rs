//! Cancels plugin task families: from the receiver through the holders in order, and by a
//! holder for the unfinished families of removed installs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::sync::Arc;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::jobs::{JobRequest, JobResponse};
use aruna_core::keyspaces::{FAMILY_PROJECTION_KEYSPACE, FAMILY_RECORD_KEYSPACE};
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::placement::resolve_shard_holders;
use aruna_core::plugin_installs::InstallState;
use aruna_core::reducer::InstallEntry;
use aruna_core::structs::execution::job::record_keys::kind_prefix;
use aruna_core::structs::execution::job::record_rows::{ProjectionCache, from_bytes};
use aruna_core::structs::execution::job::{
    CancelAuthority, JobFamilyId, JobFamilyRecord, JobId, JobRecordEnvelope, JobRecordKind,
    LogicalJobSpec, LogicalJobState, SubmissionId,
};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::types::{Key, Value};
use aruna_core::{NodeId, UserId};
use tracing::warn;

use super::records::{ReadEntry, RecordError};
use super::rows::{StoreError, unexpected};
use super::screen::tagged;
use crate::jobs::lifecycle::cancel::{
    CancelFamilyOperation, CancelLocalRunsOperation, PublishCancelOperation,
};
use crate::jobs::records::verify::FamilyView;
use crate::jobs::route::{JobRouteError, JobRouteOperation, JobRouteOutcome};

/// Projection rows one orphan pass reads per page.
const ORPHAN_PAGE: usize = 256;

fn account_auth(account: UserId) -> AuthContext {
    AuthContext {
        user_id: account,
        realm_id: account.realm_id,
        path_restrictions: None,
        session: None,
    }
}

#[derive(Debug, PartialEq)]
enum CancelPhase {
    Family,
    Holder(VecDeque<NodeId>),
    Local,
}

/// Cancels a task's family for its install account: published here when this node holds the
/// family, else asked of the holders in order; this node's own runs of it stop at once.
/// Answers whether a holder accepted the cancel.
#[derive(Debug, PartialEq)]
pub struct CancelTask {
    config: Arc<RealmConfigDocument>,
    local: NodeId,
    task: (UserId, JobId, JobFamilyId),
    accepted: bool,
    now_ms: u64,
    phase: CancelPhase,
}

type CancelNext = Result<Next<bool>, IoError>;

impl CancelTask {
    pub fn new(
        config: Arc<RealmConfigDocument>,
        local: NodeId,
        task: (UserId, JobId, JobFamilyId),
    ) -> Self {
        Self {
            config,
            local,
            task,
            accepted: false,
            now_ms: 0,
            phase: CancelPhase::Local,
        }
    }

    fn serving(&self) -> (NodeId, RealmId) {
        (self.local, self.config.realm_id)
    }

    fn ask_next(&mut self, mut holders: VecDeque<NodeId>) -> CancelNext {
        let Some(holder) = holders.pop_front() else {
            return self.stop_local();
        };
        let (account, alias, _) = self.task;
        let request = JobRequest::Cancel {
            auth_token: AuthToken::internal(account_auth(account)),
            job_id: alias,
        };
        let (local, realm_id) = self.serving();
        let route = JobRouteOperation::new(realm_id, local, alias, Some(request))
            .with_responder(Some(holder));
        self.phase = CancelPhase::Holder(holders);
        Ok(Next::Call(IoCall::child(route)))
    }

    fn stop_local(&mut self) -> CancelNext {
        let stop = CancelLocalRunsOperation::new(self.task.2, Some(self.serving()), self.now_ms);
        self.phase = CancelPhase::Local;
        Ok(Next::Call(IoCall::child(stop)))
    }
}

impl Operation for CancelTask {
    type Output = bool;
    type Error = IoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> CancelNext {
        let (account, alias, family) = self.task;
        let Ok(placement) = self.config.family_placement(family.submission_id) else {
            return Ok(Next::Done(false));
        };
        let holders = resolve_shard_holders(&self.config, &placement);
        if !holders.contains(&self.local) {
            return self.ask_next(holders.into());
        }
        let auth = account_auth(account);
        let serving = Some(self.serving());
        let cancel = CancelFamilyOperation::new(&auth, alias, None, serving, self.now_ms);
        self.phase = CancelPhase::Family;
        Ok(Next::Call(IoCall::child(cancel)))
    }

    fn next(&mut self, result: IoResult) -> CancelNext {
        match std::mem::replace(&mut self.phase, CancelPhase::Local) {
            CancelPhase::Family => {
                let published = result.child::<Option<()>, JobRouteError>()?;
                self.accepted = matches!(published, Ok(Some(())));
                self.stop_local()
            }
            CancelPhase::Holder(holders) => {
                match result.child::<JobRouteOutcome, JobRouteError>()? {
                    Ok(JobRouteOutcome::Remote(JobResponse::Cancelled { .. })) => {
                        self.accepted = true;
                        self.stop_local()
                    }
                    _ => self.ask_next(holders),
                }
            }
            CancelPhase::Local => {
                let _ = result.child::<(), IoError>()?;
                Ok(Next::Done(self.accepted))
            }
        }
    }
}

/// Whether a holder may cancel a task family for its install's removal: the install's entry is
/// settled as `Removing` with the family's account, or settled as the deletion. A delayed
/// config, a conflict, another account or any other state proves nothing.
pub fn removal_confirmed(entry: &InstallEntry, account: UserId) -> bool {
    match entry {
        InstallEntry::Settled(None) => true,
        InstallEntry::Settled(Some(install)) => {
            install.state == InstallState::Removing && install.account == account
        }
        InstallEntry::Absent | InstallEntry::Conflict { .. } => false,
    }
}

/// The family a projection row is kept under: its first 64 key bytes.
fn family_at(key: &[u8]) -> Option<JobFamilyId> {
    let submission_id = SubmissionId(key.get(..32)?.try_into().ok()?);
    let request_digest = key.get(32..64)?.try_into().ok()?;
    Some(JobFamilyId {
        submission_id,
        request_digest,
    })
}

#[derive(Debug, PartialEq)]
enum OrphanPhase {
    Page,
    Spec,
    Entry(Box<LogicalJobSpec>),
    Publish,
}

/// Cancels the unfinished task families this node holds whose install was removed, so a
/// queued orphan no longer counts against the group quota. Answers how many it cancelled.
#[derive(Debug, PartialEq)]
pub struct CancelOrphans {
    config: Arc<RealmConfigDocument>,
    local: NodeId,
    families: VecDeque<JobFamilyId>,
    after: Option<Key>,
    more: bool,
    cancelled: usize,
    now_ms: u64,
    phase: OrphanPhase,
}

type OrphanNext = Result<Next<usize>, StoreError>;

impl CancelOrphans {
    pub fn new(config: Arc<RealmConfigDocument>, local: NodeId) -> Self {
        Self {
            config,
            local,
            families: VecDeque::new(),
            after: None,
            more: true,
            cancelled: 0,
            now_ms: 0,
            phase: OrphanPhase::Page,
        }
    }

    fn page(&mut self) -> OrphanNext {
        if !self.more {
            return Ok(Next::Done(self.cancelled));
        }
        self.phase = OrphanPhase::Page;
        Ok(Next::Call(IoCall::Page {
            space: FAMILY_PROJECTION_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after: self.after.clone(),
            limit: ORPHAN_PAGE,
        }))
    }

    /// Reads the spec of the next open family this node holds.
    fn next_family(&mut self) -> OrphanNext {
        let Some(family) = self.families.pop_front() else {
            return self.page();
        };
        self.phase = OrphanPhase::Spec;
        Ok(Next::Call(IoCall::Page {
            space: FAMILY_RECORD_KEYSPACE.to_string(),
            prefix: kind_prefix(&family, JobRecordKind::Spec),
            after: None,
            limit: ORPHAN_PAGE,
        }))
    }

    /// Keeps the open families of a projection page that this node holds.
    fn judge(&mut self, rows: Vec<(Key, Value)>, next: Option<Key>) {
        (self.after, self.more) = (next.clone(), next.is_some());
        let realm_id = self.config.realm_id;
        for (key, value) in rows {
            let open = (ProjectionCache::decode(value.as_ref()))
                .and_then(|cache| cache.projection)
                .is_some_and(|projection| {
                    let running = matches!(
                        projection.state,
                        LogicalJobState::Queued
                            | LogicalJobState::Running
                            | LogicalJobState::Indeterminate
                    );
                    running && !projection.cancel_requested
                });
            let Some(family) = family_at(key.as_ref()).filter(|_| open) else {
                continue;
            };
            let view = FamilyView::resolve(&self.config, realm_id, family);
            if view.is_some_and(|view| view.holds(self.local)) {
                self.families.push_back(family);
            }
        }
    }
}

impl Operation for CancelOrphans {
    type Output = usize;
    type Error = StoreError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> OrphanNext {
        self.page()
    }

    fn next(&mut self, result: IoResult) -> OrphanNext {
        match std::mem::replace(&mut self.phase, OrphanPhase::Page) {
            OrphanPhase::Page => {
                let IoResult::Page { rows, next } = result else {
                    return Err(unexpected(result));
                };
                self.judge(rows, next);
                self.next_family()
            }
            OrphanPhase::Spec => {
                let IoResult::Page { rows, .. } = result else {
                    return Err(unexpected(result));
                };
                let spec = rows.iter().find_map(|(_, value)| {
                    let envelope = from_bytes::<JobRecordEnvelope>(value.as_ref()).ok()?;
                    match envelope.record {
                        JobFamilyRecord::Spec(spec) => Some(spec),
                        _ => None,
                    }
                });
                let Some(spec) = spec else {
                    return self.next_family();
                };
                let Some(Ok(task)) = tagged(&spec.payload) else {
                    return self.next_family();
                };
                let read = ReadEntry::new(self.config.realm_id, task.install_id);
                self.phase = OrphanPhase::Entry(spec);
                Ok(Next::Call(IoCall::child(read)))
            }
            OrphanPhase::Entry(spec) => {
                let entry = result.child::<InstallEntry, RecordError>()?;
                let entry = entry.map_err(|error| StoreError(error.to_string()))?;
                if !removal_confirmed(&entry, spec.created_by) {
                    return self.next_family();
                }
                let auth = account_auth(spec.created_by);
                let serving = Some((self.local, self.config.realm_id));
                let authority = CancelAuthority::InstallRemoved;
                let publish =
                    PublishCancelOperation::new(*spec, &auth, authority, serving, self.now_ms);
                self.phase = OrphanPhase::Publish;
                Ok(Next::Call(IoCall::child(publish)))
            }
            OrphanPhase::Publish => {
                match result.child::<(), JobRouteError>()? {
                    Ok(()) => self.cancelled += 1,
                    Err(error) => warn!(%error, "A removed install's task family kept running"),
                }
                self.next_family()
            }
        }
    }
}

#[cfg(test)]
#[path = "task_cancel_tests.rs"]
mod tests;
