//! Runs the usage, watch, mail and metadata follow-ups of the reconciles shard pulls applied,
//! oldest first, and deletes each row after them; a failed follow-up keeps its row for a rerun.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::{AppliedStep, AppliedSync, applied_sync_entry};
use aruna_core::keyspaces::APPLIED_SYNC_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::types::Key;
use tracing::warn;

use crate::metadata::after_sync::{AfterSyncOperation, MetadataTimings};
use crate::node::summary_refresh::{RefreshUsageError, RefreshUsageOperation};
use crate::notifications::email::RefreshMailOperation;
use crate::notifications::watch::interest::RefreshInterestOperation;

/// Rows one page of the drain reads at most.
pub(crate) const APPLIED_DRAIN_PAGE: usize = 16;
/// When the drain runs again after a follow-up failed; its row waits at that follow-up.
pub const APPLIED_RETRY_AFTER: Duration = Duration::from_secs(30);

type Turn = Result<Next<Option<Duration>>, IoError>;

#[derive(Clone, Debug, PartialEq)]
enum DrainStep {
    Page,
    Run(AppliedStep),
    Progress,
    Delete,
}

/// One drain run over every queued reconcile, page by page. Each follow-up that succeeded is
/// stored in its row and the last one deletes it; a failed one keeps the row for a rerun, and
/// an unreadable row is deleted.
#[derive(Clone, Debug, PartialEq)]
pub struct AppliedDrainOperation {
    step: DrainStep,
    pending: VecDeque<(Key, Option<AppliedSync>)>,
    current: Option<Key>,
    applied: Option<AppliedSync>,
    after: Option<Key>,
    failed: bool,
}

impl Default for AppliedDrainOperation {
    fn default() -> Self {
        Self::new()
    }
}

impl AppliedDrainOperation {
    pub fn new() -> Self {
        Self {
            step: DrainStep::Page,
            pending: VecDeque::new(),
            current: None,
            applied: None,
            after: None,
            failed: false,
        }
    }

    fn read_page(&mut self) -> Turn {
        self.step = DrainStep::Page;
        Ok(Next::Call(IoCall::Page {
            space: APPLIED_SYNC_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after: self.after.clone(),
            limit: APPLIED_DRAIN_PAGE,
        }))
    }

    fn next_row(&mut self) -> Turn {
        let Some((key, applied)) = self.pending.pop_front() else {
            return match self.after {
                Some(_) => self.read_page(),
                None => Ok(Next::Done(self.failed.then_some(APPLIED_RETRY_AFTER))),
            };
        };
        self.current = Some(key);
        let Some(applied) = applied else {
            return self.delete();
        };
        let next = applied.next;
        self.applied = Some(applied);
        self.run(next)
    }

    fn applied(&mut self) -> Result<&mut AppliedSync, IoError> {
        self.applied.as_mut().ok_or(IoError::Unfinished)
    }

    fn run(&mut self, step: AppliedStep) -> Turn {
        let applied = self.applied()?;
        let targets = &applied.result.targets;
        let call = match step {
            AppliedStep::Usage => IoCall::child(RefreshUsageOperation::new(
                applied.node_id,
                applied.realm_id,
                targets,
                applied.id,
            )),
            AppliedStep::Interest => match applied.realm_id {
                Some(realm_id) => IoCall::child(RefreshInterestOperation::new(targets, realm_id)),
                None => return self.run(AppliedStep::Mail),
            },
            AppliedStep::Mail => IoCall::child(RefreshMailOperation::applied(targets)),
            AppliedStep::Metadata => {
                let result = std::mem::take(&mut applied.result);
                IoCall::child(AfterSyncOperation::new(
                    Some(applied.node_id),
                    result,
                    false,
                ))
            }
        };
        self.step = DrainStep::Run(step);
        Ok(Next::Call(call))
    }

    /// Stores that `step` ran, so a rerun goes on with the next one; the last one ends the row.
    fn finished(&mut self, step: AppliedStep) -> Turn {
        let Some(next) = step.after() else {
            return self.delete();
        };
        let applied = self.applied()?;
        applied.next = next;
        let entry = applied_sync_entry(applied);
        let entry = entry.map_err(|error| IoError::Codec(error.to_string()))?;
        self.step = DrainStep::Progress;
        Ok(Next::Call(IoCall::Write(vec![entry])))
    }

    fn delete(&mut self) -> Turn {
        let key = self.current.take().ok_or(IoError::Unfinished)?;
        self.applied = None;
        self.step = DrainStep::Delete;
        let row = (APPLIED_SYNC_KEYSPACE.to_string(), key);
        Ok(Next::Call(IoCall::Delete(vec![row])))
    }
}

fn error_text<T, E: std::fmt::Display>(outcome: Result<T, E>) -> Option<String> {
    outcome.err().map(|error| error.to_string())
}

impl Operation for AppliedDrainOperation {
    type Output = Option<Duration>;
    type Error = IoError;

    fn first(&mut self) -> Turn {
        self.read_page()
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match std::mem::replace(&mut self.step, DrainStep::Page) {
            DrainStep::Page => {
                let (rows, next) = result.page()?;
                // Rows kept after a failed follow-up are passed: the next page starts after them.
                self.after = next;
                for (key, value) in rows {
                    let applied = AppliedSync::from_bytes(value.as_ref())
                        .inspect_err(|error| warn!(%error, "Deleting an unreadable applied sync"))
                        .ok();
                    self.pending.push_back((key, applied));
                }
                self.next_row()
            }
            DrainStep::Run(step) => {
                let failure = match step {
                    AppliedStep::Usage => error_text(result.child::<(), RefreshUsageError>()?),
                    AppliedStep::Interest => error_text(result.child::<(), IoError>()?),
                    AppliedStep::Mail => error_text(result.child::<(), IoError>()?),
                    AppliedStep::Metadata => {
                        error_text(result.child::<MetadataTimings, IoError>()?)
                    }
                };
                if let Some(error) = failure {
                    warn!(%error, ?step, "A follow-up stopped after the shard restore; it reruns");
                    self.failed = true;
                    self.applied = None;
                    self.current = None;
                    return self.next_row();
                }
                self.finished(step)
            }
            DrainStep::Progress => {
                result.done()?;
                let next = self.applied()?.next;
                self.run(next)
            }
            DrainStep::Delete => {
                result.done()?;
                self.next_row()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::document::{DocumentReconcileResult, DocumentTarget};
    use aruna_core::id::NodeId;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::types::Value;
    use ulid::Ulid;

    fn node() -> NodeId {
        iroh::SecretKey::from_bytes(&[1; 32]).public()
    }

    fn realm() -> RealmId {
        RealmId::from_bytes([7; 32])
    }

    fn targets() -> Vec<DocumentTarget> {
        vec![DocumentTarget::RealmConfig { realm_id: realm() }]
    }

    fn applied(realm_id: Option<RealmId>, next: AppliedStep) -> AppliedSync {
        AppliedSync {
            id: Ulid::from_parts(1_000, 7),
            node_id: node(),
            realm_id,
            result: DocumentReconcileResult {
                targets: targets(),
                ..DocumentReconcileResult::default()
            },
            next,
        }
    }

    fn queued(applied: &AppliedSync) -> IoResult {
        let (_, key, value) = applied_sync_entry(applied).unwrap();
        IoResult::Page {
            rows: vec![(key, value)],
            next: None,
        }
    }

    /// The row write that stores `next` as the follow-up a rerun starts at.
    fn progress(realm_id: Option<RealmId>, next: AppliedStep) -> Turn {
        let entry = applied_sync_entry(&applied(realm_id, next)).unwrap();
        Ok(Next::Call(IoCall::Write(vec![entry])))
    }

    fn child<T, E>(outcome: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(ChildOutcome::new(outcome))
    }

    fn after_sync() -> IoCall {
        let result = DocumentReconcileResult {
            targets: targets(),
            ..DocumentReconcileResult::default()
        };
        IoCall::child(AfterSyncOperation::new(Some(node()), result, false))
    }

    // The follow-ups run in order and each is stored once it succeeded; the row goes last.
    #[test]
    fn followups_run_ordered() {
        let realm_id = Some(realm());
        let mut drain = AppliedDrainOperation::new();
        assert!(matches!(drain.first(), Ok(Next::Call(IoCall::Page { .. }))));
        let generation = Ulid::from_parts(1_000, 7);
        let usage = RefreshUsageOperation::new(node(), realm_id, &targets(), generation);
        let row = applied(realm_id, AppliedStep::Usage);
        assert_eq!(
            drain.next(queued(&row)),
            Ok(Next::Call(IoCall::child(usage)))
        );
        let turn = drain.next(child::<(), RefreshUsageError>(Ok(())));
        assert_eq!(turn, progress(realm_id, AppliedStep::Interest));
        let interest = RefreshInterestOperation::new(&targets(), realm());
        assert_eq!(
            drain.next(IoResult::Done),
            Ok(Next::Call(IoCall::child(interest)))
        );
        let turn = drain.next(child::<(), IoError>(Ok(())));
        assert_eq!(turn, progress(realm_id, AppliedStep::Mail));
        let mail = RefreshMailOperation::applied(&targets());
        assert_eq!(
            drain.next(IoResult::Done),
            Ok(Next::Call(IoCall::child(mail)))
        );
        let turn = drain.next(child::<(), IoError>(Ok(())));
        assert_eq!(turn, progress(realm_id, AppliedStep::Metadata));
        assert_eq!(drain.next(IoResult::Done), Ok(Next::Call(after_sync())));
        let timings = MetadataTimings::default();
        let turn = drain.next(child::<_, IoError>(Ok(timings)));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Delete(_)))));
        assert_eq!(drain.next(IoResult::Done), Ok(Next::Done(None)));
    }

    // A failed follow-up stores no progress and keeps its row, the last one too; the drain
    // goes on with the next row and asks to run again.
    #[test]
    fn failed_followup_kept() {
        let mut drain = AppliedDrainOperation::new();
        drain.first().unwrap();
        let usage = applied(Some(realm()), AppliedStep::Usage);
        let mut last = applied(Some(realm()), AppliedStep::Metadata);
        last.id = Ulid::from_parts(2_000, 7);
        let rows = [&usage, &last].map(|row| {
            let (_, key, value) = applied_sync_entry(row).unwrap();
            (key, value)
        });
        let page = IoResult::Page {
            rows: rows.to_vec(),
            next: None,
        };
        assert!(matches!(drain.next(page), Ok(Next::Call(IoCall::Child(_)))));
        let failed = RefreshUsageError::Io(IoError::Aborted);
        let turn = drain.next(child::<(), _>(Err(failed)));
        assert_eq!(turn, Ok(Next::Call(after_sync())));
        let turn = drain.next(child::<MetadataTimings, _>(Err(IoError::Aborted)));
        assert_eq!(turn, Ok(Next::Done(Some(APPLIED_RETRY_AFTER))));
    }

    // A rerun after a restart starts at the stored follow-up: the refreshes that ran already
    // do not run again.
    #[test]
    fn rerun_skips_finished() {
        let mut drain = AppliedDrainOperation::new();
        drain.first().unwrap();
        let row = applied(Some(realm()), AppliedStep::Metadata);
        assert_eq!(drain.next(queued(&row)), Ok(Next::Call(after_sync())));
        let timings = MetadataTimings::default();
        let turn = drain.next(child::<_, IoError>(Ok(timings)));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Delete(_)))));
    }

    #[test]
    fn realmless_skips_interest() {
        let mut drain = AppliedDrainOperation::new();
        drain.first().unwrap();
        drain
            .next(queued(&applied(None, AppliedStep::Usage)))
            .unwrap();
        let turn = drain.next(child::<(), RefreshUsageError>(Ok(())));
        assert_eq!(turn, progress(None, AppliedStep::Interest));
        let mail = RefreshMailOperation::applied(&targets());
        assert_eq!(
            drain.next(IoResult::Done),
            Ok(Next::Call(IoCall::child(mail)))
        );
    }

    #[test]
    fn unreadable_row_deleted() {
        let mut drain = AppliedDrainOperation::new();
        drain.first().unwrap();
        let rows = vec![(Key::from(vec![9]), Value::from(vec![0xff]))];
        let turn = drain.next(IoResult::Page { rows, next: None });
        assert!(matches!(turn, Ok(Next::Call(IoCall::Delete(_)))));
    }
}
