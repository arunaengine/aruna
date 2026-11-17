//! Checks, takes and releases the per bucket write fence of a permanent purge in storage.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect, abort_effects};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::PURGE_FENCE_KEYSPACE;
use aruna_core::operation::EffectOperation;
use aruna_core::structs::execution::job::JobId;
use aruna_core::structs::storage::storage_purge::{StoragePurgeFence, StoragePurgeScope};
use aruna_core::types::{Effects, Key, TxnId, Value};
use smallvec::smallvec;

use crate::s3::purge_fence::{PurgeFenceError, check_write_fence, fence_key, write_fence_read};

/// Cheap rejection before a writer moves payload bytes. Mutating operations still read the
/// fence again in their commit transaction to close the race with a fence acquired later.
#[derive(Debug, PartialEq)]
pub struct FenceCheckOperation {
    bucket: String,
    key: String,
    output: Option<Result<(), PurgeFenceError>>,
}

impl FenceCheckOperation {
    pub fn new(bucket: &str, key: &str) -> Self {
        Self {
            bucket: bucket.to_string(),
            key: key.to_string(),
            output: None,
        }
    }
}

impl EffectOperation for FenceCheckOperation {
    type Output = ();
    type Error = PurgeFenceError;

    fn start(&mut self) -> Effects {
        smallvec![write_fence_read(&self.bucket, None)]
    }

    fn step(&mut self, event: Event) -> Effects {
        let checked = match self.output {
            None => check_write_fence(event, &self.bucket, &self.key),
            Some(_) => Err(PurgeFenceError::Unexpected(event.label().to_string())),
        };
        self.output = Some(checked);
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<(), PurgeFenceError> {
        self.output
            .unwrap_or_else(|| Err(PurgeFenceError::Unexpected("not finished".to_string())))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[derive(Debug, PartialEq)]
enum AcquireState {
    Init,
    StartTransaction,
    ReadFence,
    WriteFence,
    Commit,
    Finish,
    Error,
}

/// Takes the write fence of `scope` for `job_id`, or confirms that the job already holds it.
/// A commit conflict is retried by the runner (`drive_retry`).
#[derive(Debug, PartialEq)]
pub struct AcquireFenceOperation {
    job_id: JobId,
    scope: StoragePurgeScope,
    txn_id: Option<TxnId>,
    state: AcquireState,
    error: Option<PurgeFenceError>,
}

impl AcquireFenceOperation {
    pub fn new(job_id: JobId, scope: StoragePurgeScope) -> Self {
        Self {
            job_id,
            scope,
            txn_id: None,
            state: AcquireState::Init,
            error: None,
        }
    }

    fn begin(&mut self) -> Effects {
        self.state = AcquireState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn fail(&mut self, error: PurgeFenceError) -> Effects {
        self.state = AcquireState::Error;
        self.error = Some(error);
        abort_effects(&mut self.txn_id)
    }

    fn finish(&mut self, result: Result<(), PurgeFenceError>) -> Effects {
        let effects = abort_effects(&mut self.txn_id);
        match result {
            Ok(()) => self.state = AcquireState::Finish,
            Err(error) => {
                self.state = AcquireState::Error;
                self.error = Some(error);
            }
        }
        effects
    }

    fn fence_read(&mut self, value: Option<Value>) -> Effects {
        let existing = match value.map(|value| StoragePurgeFence::from_bytes(value.as_ref())) {
            None => None,
            Some(Ok(fence)) => Some(fence),
            Some(Err(_)) => return self.fail(PurgeFenceError::Invalid),
        };
        if let Some(existing) = existing {
            // Holding the fence already is success; the read-only transaction is dropped.
            return self.finish(if existing.job_id != self.job_id {
                Err(PurgeFenceError::Busy)
            } else if existing.scope == self.scope {
                Ok(())
            } else {
                Err(PurgeFenceError::Invalid)
            });
        }
        let fence = StoragePurgeFence {
            job_id: self.job_id,
            scope: self.scope.clone(),
        };
        let Ok(value) = fence.to_bytes() else {
            return self.fail(PurgeFenceError::Invalid);
        };
        self.state = AcquireState::WriteFence;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: PURGE_FENCE_KEYSPACE.to_string(),
            key: fence_key(self.scope.bucket()),
            value: value.into(),
            txn_id: self.txn_id,
        })]
    }

    fn commit_failed(&mut self, error: StorageError) -> Effects {
        // The failed commit closed the transaction.
        self.txn_id = None;
        self.fail(error.into())
    }
}

impl EffectOperation for AcquireFenceOperation {
    type Output = ();
    type Error = PurgeFenceError;

    fn start(&mut self) -> Effects {
        self.begin()
    }

    fn step(&mut self, event: Event) -> Effects {
        match (&self.state, event) {
            (AcquireState::Commit, Event::Storage(StorageEvent::Error { error })) => {
                self.commit_failed(error)
            }
            (
                AcquireState::StartTransaction | AcquireState::ReadFence | AcquireState::WriteFence,
                Event::Storage(StorageEvent::Error { error }),
            ) => self.fail(error.into()),
            (
                AcquireState::StartTransaction,
                Event::Storage(StorageEvent::TransactionStarted { txn_id }),
            ) => {
                self.txn_id = Some(txn_id);
                self.state = AcquireState::ReadFence;
                smallvec![write_fence_read(self.scope.bucket(), Some(txn_id))]
            }
            (AcquireState::ReadFence, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                self.fence_read(value)
            }
            (AcquireState::WriteFence, Event::Storage(StorageEvent::WriteResult { .. })) => {
                let Some(txn_id) = self.txn_id else {
                    return self.fail(PurgeFenceError::Unexpected("no transaction".to_string()));
                };
                self.state = AcquireState::Commit;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            (AcquireState::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.txn_id = None;
                self.state = AcquireState::Finish;
                smallvec![]
            }
            (_, event) => self.fail(PurgeFenceError::Unexpected(event.label().to_string())),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, AcquireState::Finish | AcquireState::Error)
    }

    fn finalize(self) -> Result<(), PurgeFenceError> {
        match (self.state, self.error) {
            (AcquireState::Finish, _) => Ok(()),
            (_, Some(error)) => Err(error),
            _ => Err(PurgeFenceError::Unexpected("not finished".to_string())),
        }
    }

    fn abort(&mut self) -> Effects {
        abort_effects(&mut self.txn_id)
    }

    fn is_conflict(error: &PurgeFenceError) -> bool {
        *error == PurgeFenceError::Storage(StorageError::TransactionConflict)
    }
}

/// The delete of the bucket fence row, read in a job-terminal transaction, when the terminal
/// job still owns it. A cancelled waiter must never clear the fence of the purge that won.
pub fn owned_fence_delete(
    value: Option<&[u8]>,
    job_id: JobId,
    scope: &StoragePurgeScope,
) -> Result<Option<(String, Key)>, PurgeFenceError> {
    let fence = value
        .map(|value| StoragePurgeFence::from_bytes(value).map_err(|_| PurgeFenceError::Invalid))
        .transpose()?;
    match fence {
        Some(fence) if fence.job_id == job_id && fence.scope == *scope => Ok(Some((
            PURGE_FENCE_KEYSPACE.to_string(),
            fence_key(scope.bucket()),
        ))),
        Some(fence) if fence.job_id == job_id => Err(PurgeFenceError::Invalid),
        Some(_) | None => Ok(None),
    }
}

#[cfg(test)]
#[path = "fence_store_tests.rs"]
mod tests;
