//! Runs the usage, watch, mail and metadata follow-ups of the reconciles shard pulls applied,
//! oldest first, and deletes each row after them; a failed follow-up only warns.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::AppliedSync;
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

type Turn = Result<Next<()>, IoError>;

#[derive(Clone, Debug, PartialEq)]
enum DrainStep {
    Page,
    Usage,
    Interest,
    Mail,
    Metadata,
    Delete,
}

/// One drain run: the follow-ups of every queued reconcile run and its row is deleted, page by
/// page, until the keyspace is empty. A row that does not decode is deleted without follow-ups.
#[derive(Clone, Debug, PartialEq)]
pub struct AppliedDrainOperation {
    step: DrainStep,
    pending: VecDeque<(Key, Option<AppliedSync>)>,
    current: Option<Key>,
    applied: Option<AppliedSync>,
    more: bool,
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
            more: false,
        }
    }

    fn read_page(&mut self) -> Turn {
        self.step = DrainStep::Page;
        Ok(Next::Call(IoCall::Page {
            space: APPLIED_SYNC_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after: None,
            limit: APPLIED_DRAIN_PAGE,
        }))
    }

    fn next_row(&mut self) -> Turn {
        let Some((key, applied)) = self.pending.pop_front() else {
            return match self.more {
                true => self.read_page(),
                false => Ok(Next::Done(())),
            };
        };
        self.current = Some(key);
        let Some(applied) = applied else {
            return self.delete();
        };
        self.step = DrainStep::Usage;
        let refresh = RefreshUsageOperation::new(
            applied.node_id,
            applied.realm_id,
            &applied.result.targets,
            applied.id,
        );
        self.applied = Some(applied);
        Ok(Next::Call(IoCall::child(refresh)))
    }

    fn applied(&mut self) -> Result<&mut AppliedSync, IoError> {
        self.applied.as_mut().ok_or(IoError::Unfinished)
    }

    fn mail(&mut self) -> Turn {
        self.step = DrainStep::Mail;
        let refresh = RefreshMailOperation::applied(&self.applied()?.result.targets);
        Ok(Next::Call(IoCall::child(refresh)))
    }

    fn delete(&mut self) -> Turn {
        let key = self.current.take().ok_or(IoError::Unfinished)?;
        self.applied = None;
        self.step = DrainStep::Delete;
        let row = (APPLIED_SYNC_KEYSPACE.to_string(), key);
        Ok(Next::Call(IoCall::Delete(vec![row])))
    }
}

impl Operation for AppliedDrainOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Turn {
        self.read_page()
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match self.step {
            DrainStep::Page => {
                let (rows, next) = result.page()?;
                self.more = next.is_some();
                for (key, value) in rows {
                    let applied = AppliedSync::from_bytes(value.as_ref())
                        .inspect_err(|error| warn!(%error, "Deleting an unreadable applied sync"))
                        .ok();
                    self.pending.push_back((key, applied));
                }
                self.next_row()
            }
            DrainStep::Usage => {
                if let Err(error) = result.child::<(), RefreshUsageError>()? {
                    warn!(%error, "Realm usage refresh stopped after the shard restore");
                }
                let Some(realm_id) = self.applied()?.realm_id else {
                    return self.mail();
                };
                let targets = &self.applied()?.result.targets;
                let refresh = RefreshInterestOperation::new(targets, realm_id);
                self.step = DrainStep::Interest;
                Ok(Next::Call(IoCall::child(refresh)))
            }
            DrainStep::Interest => {
                if let Err(error) = result.child::<(), IoError>()? {
                    warn!(%error, "Watch interest refresh stopped after the shard restore");
                }
                self.mail()
            }
            DrainStep::Mail => {
                if let Err(error) = result.child::<(), IoError>()? {
                    warn!(%error, "Mail settings refresh stopped after the shard restore");
                }
                let applied = self.applied()?;
                let node_id = applied.node_id;
                let result = std::mem::take(&mut applied.result);
                self.step = DrainStep::Metadata;
                let after = AfterSyncOperation::new(Some(node_id), result, false);
                Ok(Next::Call(IoCall::child(after)))
            }
            DrainStep::Metadata => {
                if let Err(error) = result.child::<MetadataTimings, IoError>()? {
                    warn!(%error, "Metadata follow-up stopped after the shard restore");
                }
                self.delete()
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
    use aruna_core::NodeId;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::document::{DocumentReconcileResult, DocumentTarget, applied_sync_entry};
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

    fn queued(realm_id: Option<RealmId>) -> IoResult {
        let applied = AppliedSync {
            id: Ulid::from_parts(1_000, 7),
            node_id: node(),
            realm_id,
            result: DocumentReconcileResult {
                targets: targets(),
                ..DocumentReconcileResult::default()
            },
        };
        let (_, key, value) = applied_sync_entry(&applied).unwrap();
        IoResult::Page {
            rows: vec![(key, value)],
            next: None,
        }
    }

    fn child<T, E>(outcome: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(ChildOutcome::new(outcome))
    }

    // The follow-ups run in order, a failed one does not stop the next, and the row goes last.
    #[test]
    fn followups_run_ordered() {
        let mut drain = AppliedDrainOperation::new();
        assert!(matches!(drain.first(), Ok(Next::Call(IoCall::Page { .. }))));
        let generation = Ulid::from_parts(1_000, 7);
        let usage = RefreshUsageOperation::new(node(), Some(realm()), &targets(), generation);
        assert_eq!(
            drain.next(queued(Some(realm()))),
            Ok(Next::Call(IoCall::child(usage)))
        );
        let failed = RefreshUsageError::Io(IoError::Aborted);
        let interest = RefreshInterestOperation::new(&targets(), realm());
        let turn = drain.next(child::<(), _>(Err(failed)));
        assert_eq!(turn, Ok(Next::Call(IoCall::child(interest))));
        let mail = RefreshMailOperation::applied(&targets());
        let turn = drain.next(child::<(), IoError>(Ok(())));
        assert_eq!(turn, Ok(Next::Call(IoCall::child(mail))));
        let result = DocumentReconcileResult {
            targets: targets(),
            ..DocumentReconcileResult::default()
        };
        let after = AfterSyncOperation::new(Some(node()), result, false);
        let turn = drain.next(child::<(), IoError>(Ok(())));
        assert_eq!(turn, Ok(Next::Call(IoCall::child(after))));
        let timings = MetadataTimings::default();
        let turn = drain.next(child::<_, IoError>(Ok(timings)));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Delete(_)))));
        assert_eq!(drain.next(IoResult::Done), Ok(Next::Done(())));
    }

    #[test]
    fn realmless_skips_interest() {
        let mut drain = AppliedDrainOperation::new();
        drain.first().unwrap();
        drain.next(queued(None)).unwrap();
        let mail = RefreshMailOperation::applied(&targets());
        let turn = drain.next(child::<(), RefreshUsageError>(Ok(())));
        assert_eq!(turn, Ok(Next::Call(IoCall::child(mail))));
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
