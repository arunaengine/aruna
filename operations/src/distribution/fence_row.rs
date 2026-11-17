//! Changes a set's local fence row in one transaction. A write that read the row before
//! conflicts with this commit, and a conflicted change is driven again.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect, abort_effects};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::DISTRIBUTION_FENCE_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::storage::distribution::fence::{LocalFence, RangeFence};
use aruna_core::types::{Effects, Key, TxnId};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

/// One change of a fence row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum FenceChange {
    /// Closes the set to incoming publications.
    Close,
    /// Fences every range for the deletion with this identity and closes the set.
    Delete([u8; 32]),
    /// Fences one range and keeps how many byte copies it had then.
    Range { fence: RangeFence, copies: u32 },
    /// Closes the uploads of one fenced range.
    Uploads(Ulid),
}

impl FenceChange {
    /// Applies the change and says whether it changed anything.
    fn apply(&self, local: &mut LocalFence) -> bool {
        match self {
            FenceChange::Close => !std::mem::replace(&mut local.closed, true),
            FenceChange::Delete(identity) => {
                let changed = local.deletion != Some(*identity) || !local.closed;
                local.deletion = Some(*identity);
                local.closed = true;
                changed
            }
            FenceChange::Range { fence, copies } => {
                let fenced = local.fence_range(fence.clone());
                if fenced {
                    local.copies.retain(|(id, _)| *id != fence.range_id);
                    local.copies.push((fence.range_id, *copies));
                }
                fenced
            }
            FenceChange::Uploads(range_id) => local.close_uploads(*range_id),
        }
    }
}

#[derive(Debug, Error, PartialEq)]
pub(super) enum FenceError {
    #[error("the fence row was not committed: {0}")]
    Commit(StorageError),
    #[error("{0}")]
    Failed(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FenceStep {
    Init,
    Start,
    Read,
    Write,
    Commit,
    Abort,
    Finish,
}

/// Applies one change to the fence row of `set_id` and outputs the row as it is afterwards.
#[derive(Debug, PartialEq)]
pub(super) struct UpdateFenceOperation {
    set_id: Ulid,
    change: FenceChange,
    step: FenceStep,
    txn_id: Option<TxnId>,
    fence: LocalFence,
    output: Option<Result<LocalFence, FenceError>>,
}

impl UpdateFenceOperation {
    pub(super) fn new(set_id: Ulid, change: FenceChange) -> Self {
        Self {
            set_id,
            change,
            step: FenceStep::Init,
            txn_id: None,
            fence: LocalFence::default(),
            output: None,
        }
    }

    fn key(&self) -> Key {
        LocalFence::key(self.set_id).into()
    }

    /// Ends with `output`, aborting the transaction first when one is open.
    fn end(&mut self, output: Result<LocalFence, FenceError>) -> Effects {
        self.output = Some(output);
        let abort = abort_effects(&mut self.txn_id);
        self.step = match abort.is_empty() {
            true => FenceStep::Finish,
            false => FenceStep::Abort,
        };
        abort
    }

    fn change(&mut self, value: Option<&[u8]>) -> Effects {
        let mut fence = match LocalFence::decode(value) {
            Ok(fence) => fence,
            Err(error) => return self.end(Err(FenceError::Failed(error.to_string()))),
        };
        if !self.change.apply(&mut fence) {
            return self.end(Ok(fence));
        }
        let value = match fence.to_bytes() {
            Ok(value) => value,
            Err(error) => return self.end(Err(FenceError::Failed(error.to_string()))),
        };
        self.fence = fence;
        self.step = FenceStep::Write;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: DISTRIBUTION_FENCE_KEYSPACE.to_string(),
            key: self.key(),
            value: value.into(),
            txn_id: self.txn_id,
        })]
    }
}

impl Operation for UpdateFenceOperation {
    type Output = LocalFence;
    type Error = FenceError;

    fn start(&mut self) -> Effects {
        self.step = FenceStep::Start;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (self.step, event) {
            (FenceStep::Start, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.txn_id = Some(txn_id);
                self.step = FenceStep::Read;
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: DISTRIBUTION_FENCE_KEYSPACE.to_string(),
                    key: self.key(),
                    txn_id: Some(txn_id),
                })]
            }
            (FenceStep::Start, event) => {
                let label = event.label();
                let error = format!("the fence transaction did not start: {label}");
                self.end(Err(FenceError::Failed(error)))
            }
            (FenceStep::Read, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                self.change(value.as_deref())
            }
            (FenceStep::Read, event) => {
                let label = event.label();
                let error = format!("the fence row was not read: {label}");
                self.end(Err(FenceError::Failed(error)))
            }
            (FenceStep::Write, Event::Storage(StorageEvent::WriteResult { .. })) => {
                let Some(txn_id) = self.txn_id.take() else {
                    let error = "the fence transaction was lost".to_string();
                    return self.end(Err(FenceError::Failed(error)));
                };
                self.step = FenceStep::Commit;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            (FenceStep::Write, _) => {
                let error = "the fence row was not written".to_string();
                self.end(Err(FenceError::Failed(error)))
            }
            (FenceStep::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                let fence = std::mem::take(&mut self.fence);
                self.end(Ok(fence))
            }
            (FenceStep::Commit, Event::Storage(StorageEvent::Error { error })) => {
                self.end(Err(FenceError::Commit(error)))
            }
            (FenceStep::Commit, event) => {
                let label = event.label();
                let error = format!("the fence row was not committed: {label}");
                self.end(Err(FenceError::Failed(error)))
            }
            (FenceStep::Abort, _) => {
                self.step = FenceStep::Finish;
                smallvec![]
            }
            (FenceStep::Init | FenceStep::Finish, event) => {
                let label = event.label();
                self.end(Err(FenceError::Failed(format!("unexpected {label}"))))
            }
        }
    }

    fn is_complete(&self) -> bool {
        self.step == FenceStep::Finish
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        let unfinished = FenceError::Failed("the fence change did not finish".to_string());
        self.output.unwrap_or(Err(unfinished))
    }

    fn abort(&mut self) -> Effects {
        abort_effects(&mut self.txn_id)
    }

    fn is_conflict(error: &Self::Error) -> bool {
        matches!(error, FenceError::Commit(error) if error.proves_no_commit())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::operation_check::{
        check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
    };

    const TXN: TxnId = Ulid::from_bytes([9; 16]);

    fn set() -> Ulid {
        Ulid::from_bytes([1; 16])
    }

    fn storage(event: StorageEvent) -> Event {
        Event::Storage(event)
    }

    fn read(value: Option<&LocalFence>) -> Event {
        storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: value.map(|fence| fence.to_bytes().unwrap().into()),
        })
    }

    fn abort() -> Effect {
        Effect::Storage(StorageEffect::AbortTransaction { txn_id: TXN })
    }

    // A change is written and committed; the row is answered as it is afterwards.
    #[test]
    fn closes_open_set() {
        let closed = LocalFence {
            closed: true,
            ..LocalFence::default()
        };
        let mut update = UpdateFenceOperation::new(set(), FenceChange::Close);
        let key: Key = LocalFence::key(set()).into();
        check_trace(
            &mut update,
            &[Effect::Storage(StorageEffect::StartTransaction {
                read: false,
            })],
            vec![
                (
                    storage(StorageEvent::TransactionStarted { txn_id: TXN }),
                    vec![Effect::Storage(StorageEffect::Read {
                        key_space: DISTRIBUTION_FENCE_KEYSPACE.to_string(),
                        key: key.clone(),
                        txn_id: Some(TXN),
                    })],
                ),
                (
                    read(None),
                    vec![Effect::Storage(StorageEffect::Write {
                        key_space: DISTRIBUTION_FENCE_KEYSPACE.to_string(),
                        key: key.clone(),
                        value: closed.to_bytes().unwrap().into(),
                        txn_id: Some(TXN),
                    })],
                ),
                (
                    storage(StorageEvent::WriteResult { key }),
                    vec![Effect::Storage(StorageEffect::CommitTransaction {
                        txn_id: TXN,
                    })],
                ),
                (
                    storage(StorageEvent::TransactionCommitted { txn_id: TXN }),
                    Vec::new(),
                ),
            ],
        );
        assert_eq!(update.finalize(), Ok(closed));
    }

    // An unchanged row is answered without a write, and the transaction is aborted.
    #[test]
    fn unchanged_row_aborted() {
        let identity = [4; 32];
        let deleted = LocalFence {
            deletion: Some(identity),
            closed: true,
            ..LocalFence::default()
        };
        let started = || storage(StorageEvent::TransactionStarted { txn_id: TXN });
        let mut update = UpdateFenceOperation::new(set(), FenceChange::Delete(identity));
        update.start();
        update.step(started());
        check_abort(
            &mut UpdateFenceOperation {
                txn_id: Some(TXN),
                ..UpdateFenceOperation::new(set(), FenceChange::Close)
            },
            &[abort()],
        );
        assert_eq!(update.step(read(Some(&deleted))).into_vec(), vec![abort()]);
        let aborted = storage(StorageEvent::TransactionAborted { txn_id: TXN });
        assert!(update.step(aborted).is_empty());
        assert_eq!(update.finalize(), Ok(deleted));
    }

    // A conflicted commit is a conflict to drive again; other events are refused.
    #[test]
    fn conflict_retried() {
        let conflict = FenceError::Commit(StorageError::TransactionConflict);
        assert!(UpdateFenceOperation::is_conflict(&conflict));
        let failed = FenceError::Failed("the fence row was not written".to_string());
        assert!(!UpdateFenceOperation::is_conflict(&failed));

        let fresh = || UpdateFenceOperation::new(set(), FenceChange::Uploads(set()));
        let all = sample_events().len();
        assert_eq!(rejects_events(fresh, sample_events, |_| false).len(), all);
        rejects_finalize(fresh());
        assert!(fresh().abort().is_empty());
    }
}
