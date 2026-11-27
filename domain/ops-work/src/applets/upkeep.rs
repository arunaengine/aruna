//! One upkeep pass over hosted applets: expires drafts, prunes versions, rechecks domains,
//! repairs accounts, refreshes holder copies, and drops stale counters and notices.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;

use aruna_core::applet::notices::{NOTICE_KEEP_MS, notice_prefix, notice_start};
use aruna_core::applet::record::{Applet, record_prefix};
use aruna_core::applet::usage::{usage_ended, usage_prefix};
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::APPLETS_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::types::Key;
use tracing::warn;

use super::AppletError;
use super::change::{AppletChange, WriteChangeOperation};
use super::domains::VerifyDomainOperation;
use super::identity::SyncIdentityOperation;
use super::replica::ReplicateAppletOperation;

/// Rows one page reads.
const PAGE: usize = 256;
/// How long a custom domain goes unchecked at most.
pub const DOMAIN_CHECK_MS: u64 = 3_600_000;

fn notices_page() -> IoCall {
    IoCall::Page {
        space: APPLETS_KEYSPACE.to_string(),
        prefix: notice_prefix(),
        after: None,
        limit: PAGE,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Task {
    Prune,
    Verify(String),
    Identity,
    Replicate,
    Counters(Option<Key>),
}

#[derive(Debug, PartialEq)]
enum Phase {
    Records(Option<Key>),
    Task(Task),
    Notices,
}

/// What one pass did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct UpkeepReport {
    pub applets: usize,
    pub failed: usize,
}

/// Runs one pass over every applet hosted on `local`; one that fails is logged and the others
/// are still kept.
#[derive(Debug, PartialEq)]
pub struct UpkeepOperation {
    local: (NodeId, RealmId),
    now_ms: u64,
    queue: VecDeque<Applet>,
    more: Option<Key>,
    current: Option<Applet>,
    tasks: VecDeque<Task>,
    report: UpkeepReport,
    phase: Phase,
}

impl UpkeepOperation {
    pub fn new(local: (NodeId, RealmId), now_ms: u64) -> Self {
        Self {
            local,
            now_ms,
            queue: VecDeque::new(),
            more: None,
            current: None,
            tasks: VecDeque::new(),
            report: UpkeepReport::default(),
            phase: Phase::Records(None),
        }
    }

    fn records(&mut self, after: Option<Key>) -> IoCall {
        self.phase = Phase::Records(after.clone());
        IoCall::Page {
            space: APPLETS_KEYSPACE.to_string(),
            prefix: record_prefix(),
            after,
            limit: PAGE,
        }
    }

    /// The upkeep tasks of one hosted applet, in order.
    fn plan(&self, applet: &Applet) -> VecDeque<Task> {
        let mut tasks = VecDeque::new();
        if applet.clone().retain(self.now_ms) {
            tasks.push_back(Task::Prune);
        }
        let due = (applet.domains.iter())
            .filter(|domain| domain.checked_at_ms.saturating_add(DOMAIN_CHECK_MS) <= self.now_ms);
        tasks.extend(due.map(|domain| Task::Verify(domain.host.clone())));
        if applet.account.is_some() || !applet.grants.is_empty() {
            tasks.push_back(Task::Identity);
        }
        tasks.push_back(Task::Replicate);
        tasks.push_back(Task::Counters(None));
        tasks
    }

    fn task_call(&mut self, task: Task) -> Result<IoCall, AppletError> {
        let applet = self
            .current
            .as_ref()
            .ok_or(aruna_core::calls::IoError::Unfinished)?;
        let id = applet.applet_id;
        let call = match &task {
            Task::Prune => {
                let write = WriteChangeOperation::node(id, AppletChange::Prune, applet.created_by);
                IoCall::retry(write, &id.to_bytes())
            }
            Task::Verify(host) => IoCall::child(VerifyDomainOperation::new(None, (id, host))),
            Task::Identity => IoCall::child(SyncIdentityOperation::new(applet, self.local)),
            Task::Replicate => IoCall::child(ReplicateAppletOperation::new(applet, self.local)),
            Task::Counters(after) => IoCall::Page {
                space: APPLETS_KEYSPACE.to_string(),
                prefix: usage_prefix(id),
                after: after.clone(),
                limit: PAGE,
            },
        };
        self.phase = Phase::Task(task);
        Ok(call)
    }

    /// The next task of the current applet, else the next hosted applet, else the next page.
    fn advance(&mut self) -> Result<Next<UpkeepReport>, AppletError> {
        loop {
            if self.current.is_some()
                && let Some(task) = self.tasks.pop_front()
            {
                return Ok(Next::Call(self.task_call(task)?));
            }
            self.current = None;
            let Some(applet) = self.queue.pop_front() else {
                break;
            };
            if applet.host_node != self.local.0 {
                continue;
            }
            self.report.applets += 1;
            self.tasks = self.plan(&applet);
            self.current = Some(applet);
        }
        if let Some(after) = self.more.take() {
            return Ok(Next::Call(self.records(Some(after))));
        }
        self.phase = Phase::Notices;
        Ok(Next::Call(notices_page()))
    }

    /// Logs a failed task of the current applet and skips its remaining ones.
    fn failed(&mut self, task: &Task, error: &AppletError) {
        let applet = self.current.as_ref().map(|applet| applet.applet_id);
        warn!(?applet, ?task, %error, "An applet upkeep task failed");
        self.report.failed += 1;
        self.tasks.clear();
    }

    fn task_done(
        &mut self,
        task: Task,
        result: IoResult,
    ) -> Result<Next<UpkeepReport>, AppletError> {
        let outcome = match &task {
            Task::Prune | Task::Verify(_) | Task::Identity => {
                result.child::<Applet, AppletError>()?.map(|applet| {
                    self.current = Some(applet);
                })
            }
            Task::Replicate => result.child::<usize, AppletError>()?.map(drop),
            Task::Counters(_) => {
                let (rows, next) = result.page()?;
                let ended: Vec<_> = (rows.into_iter())
                    .filter(|(key, _)| usage_ended(key, self.now_ms))
                    .map(|(key, _)| (APPLETS_KEYSPACE.to_string(), key))
                    .collect();
                if next.is_some() {
                    self.tasks.push_front(Task::Counters(next));
                }
                if !ended.is_empty() {
                    self.phase = Phase::Task(Task::Counters(None));
                    return Ok(Next::Call(IoCall::Delete(ended)));
                }
                Ok(())
            }
        };
        if let Err(error) = outcome {
            self.failed(&task, &error);
        }
        self.advance()
    }
}

impl Operation for UpkeepOperation {
    type Output = UpkeepReport;
    type Error = AppletError;

    fn first(&mut self) -> Result<Next<UpkeepReport>, AppletError> {
        Ok(Next::Call(self.records(None)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<UpkeepReport>, AppletError> {
        match std::mem::replace(&mut self.phase, Phase::Notices) {
            Phase::Records(_) => {
                let (rows, next) = result.page()?;
                for (_, value) in rows {
                    self.queue.push_back(Applet::from_bytes(&value)?);
                }
                self.more = next;
                self.advance()
            }
            // A delete of ended counters answers with `Done`.
            Phase::Task(_) if result == IoResult::Done => self.advance(),
            Phase::Task(task) => self.task_done(task, result),
            // Old notices were deleted; read the oldest again until none is old.
            Phase::Notices if result == IoResult::Done => Ok(Next::Call(notices_page())),
            Phase::Notices => {
                let cut = notice_start(self.now_ms.saturating_sub(NOTICE_KEEP_MS));
                let (rows, _) = result.page()?;
                let old: Vec<_> = (rows.into_iter())
                    .filter(|(key, _)| *key < cut)
                    .map(|(key, _)| (APPLETS_KEYSPACE.to_string(), key))
                    .collect();
                if old.is_empty() {
                    return Ok(Next::Done(self.report));
                }
                Ok(Next::Call(IoCall::Delete(old)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::applets::change::tests::applet;
    use aruna_core::applet::record::{CustomDomain, DomainState, Draft};
    use ulid::Ulid;

    // Only due domains are checked again, pruning runs only with something to drop, and every
    // hosted applet is copied to the holders and has its counters swept.
    #[test]
    fn plans_due_work() {
        let mut applet = applet();
        let local = (applet.host_node, RealmId([2; 32]));
        let upkeep = UpkeepOperation::new(local, DOMAIN_CHECK_MS + 10);
        assert_eq!(
            upkeep.plan(&applet),
            VecDeque::from([Task::Replicate, Task::Counters(None)])
        );
        for (host, checked_at_ms) in [("due.lab.org", 10), ("fresh.lab.org", DOMAIN_CHECK_MS)] {
            applet.domains.push(CustomDomain {
                host: host.into(),
                nonce: "00".into(),
                state: DomainState::Active,
                failures: 0,
                checked_at_ms,
            });
        }
        applet.drafts.push(Draft {
            build_id: Ulid::from_bytes([1; 16]),
            job_id: Ulid::from_bytes([1; 16]),
            commit: "c".into(),
            started_by: applet.created_by,
            started_at_ms: 0,
            expires_at_ms: 5,
            manifest: None,
            failed: None,
        });
        let tasks: Vec<Task> = upkeep.plan(&applet).into_iter().collect();
        assert_eq!(
            tasks,
            vec![
                Task::Prune,
                Task::Verify("due.lab.org".into()),
                Task::Replicate,
                Task::Counters(None)
            ]
        );
    }
}
