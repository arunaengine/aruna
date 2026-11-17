//! Remote waits on this node's bucket keys and the wakes owed to the waiting nodes.
//! A wake row stays until the waiting node acknowledges it; delivery may repeat.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::encryption::BucketKeyRef;

use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::types::{Effects, TxnId};
use smallvec::smallvec;
use tracing::warn;

use super::*;

/// Prefix of a registration: key reference, waiting node, job.
const REMOTE_PREFIX: u8 = b'r';
/// Prefix of an owed wake: waiting node, job, key reference.
const DELIVERY_PREFIX: u8 = b'd';

fn registration_key(key: BucketKeyRef, waiter: NodeId, job_id: JobId) -> Key {
    ByteView::from(
        [
            &[REMOTE_PREFIX][..],
            &key.key(),
            waiter.as_bytes(),
            &job_id.to_bytes(),
        ]
        .concat(),
    )
}

fn delivery_key(waiter: NodeId, job_id: JobId, key: BucketKeyRef) -> Key {
    ByteView::from(
        [
            &[DELIVERY_PREFIX][..],
            waiter.as_bytes(),
            &job_id.to_bytes(),
            &key.key(),
        ]
        .concat(),
    )
}

/// Waiting node, job and key of a delivery row.
fn parse_delivery(row: &[u8]) -> Option<(NodeId, JobId, BucketKeyRef)> {
    let rest = row.strip_prefix(&[DELIVERY_PREFIX])?;
    let (node, rest) = rest.split_first_chunk::<32>()?;
    let (job, rest) = rest.split_first_chunk::<16>()?;
    let waiter = NodeId::from_bytes(node).ok()?;
    let key = BucketKeyRef::from_key(rest).ok()?;
    Some((waiter, JobId::from_bytes(*job), key))
}

/// One wake owed to a waiting node, with the user its node vouched for at registration.
#[derive(Clone, Debug, PartialEq)]
pub struct OwedWake {
    pub waiter: NodeId,
    pub job_id: JobId,
    pub key: BucketKeyRef,
    pub auth: AuthContext,
}

/// Writes or drops one wait row: a registration, or an owed wake its waiter acknowledged.
#[derive(Debug, PartialEq)]
pub struct WaitRowOperation {
    effect: Option<StorageEffect>,
    outcome: Option<Result<(), JobMutationError>>,
}

impl WaitRowOperation {
    /// Records that `waiter` parks `job_id` on `key` for `auth`. The caller checks the registry
    /// afterwards and answers "already available" when the key is unlocked, so no unlock is missed.
    pub fn register(key: BucketKeyRef, waiter: NodeId, job_id: JobId, auth: &AuthContext) -> Self {
        let row = registration_key(key, waiter, job_id);
        match postcard::to_allocvec(auth) {
            Ok(value) => Self::new(StorageEffect::BatchWrite {
                writes: vec![(KEY_WAIT_KEYSPACE.to_string(), row, ByteView::from(value))],
                txn_id: None,
            }),
            Err(error) => Self {
                effect: None,
                outcome: Some(Err(JobMutationError::Storage(error.to_string()))),
            },
        }
    }

    /// Drops an owed wake once the waiting node acknowledged it.
    pub fn acknowledge(waiter: NodeId, job_id: JobId, key: BucketKeyRef) -> Self {
        let row = delivery_key(waiter, job_id, key);
        Self::new(StorageEffect::BatchDelete {
            deletes: vec![(KEY_WAIT_KEYSPACE.to_string(), row)],
            txn_id: None,
        })
    }

    fn new(effect: StorageEffect) -> Self {
        Self {
            effect: Some(effect),
            outcome: None,
        }
    }
}

impl Operation for WaitRowOperation {
    type Output = ();
    type Error = JobMutationError;

    fn start(&mut self) -> Effects {
        match self.effect.take() {
            Some(effect) => smallvec![Effect::Storage(effect)],
            None => smallvec![],
        }
    }

    fn step(&mut self, event: Event) -> Effects {
        self.outcome = Some(match event {
            _ if self.outcome.is_some() => Err(unexpected(&event)),
            Event::Storage(
                StorageEvent::BatchWriteResult { .. } | StorageEvent::BatchDeleteResult { .. },
            ) => Ok(()),
            Event::Storage(StorageEvent::Error { error }) => {
                Err(JobMutationError::Storage(error.to_string()))
            }
            other => Err(unexpected(&other)),
        });
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        self.outcome.is_some()
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.outcome.unwrap_or(Err(not_finished()))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

/// Turns one page of registrations on `key` into owed wakes, atomically per page.
/// Reports whether a further page may exist.
#[derive(Debug, PartialEq)]
pub struct QueueWakesOperation {
    key: BucketKeyRef,
    phase: QueuePhase,
    full: bool,
    /// The registrations a page clears once its owed wakes are written.
    pending: JobDeletes,
    outcome: Option<Result<bool, JobMutationError>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueuePhase {
    Init,
    Begin,
    Page(TxnId),
    Write(TxnId),
    Delete(TxnId),
    Commit(TxnId),
    Abort,
    Done,
}

impl QueueWakesOperation {
    pub fn new(key: BucketKeyRef) -> Self {
        Self {
            key,
            phase: QueuePhase::Init,
            full: false,
            pending: Vec::new(),
            outcome: None,
        }
    }

    fn prefix(&self) -> Key {
        ByteView::from([&[REMOTE_PREFIX][..], &self.key.key()].concat())
    }

    fn finish(&mut self, outcome: Result<bool, JobMutationError>) -> Effects {
        self.outcome = Some(outcome);
        self.phase = QueuePhase::Done;
        smallvec![]
    }

    /// Ends the page with `error`, aborting its open transaction first.
    fn fail(&mut self, txn_id: TxnId, error: JobMutationError) -> Effects {
        self.outcome = Some(Err(error));
        self.phase = QueuePhase::Abort;
        smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
    }

    /// The owed wakes and cleared registrations of one page of registration rows.
    fn rewrite(&self, rows: &[(Key, Value)]) -> (JobWrites, JobDeletes) {
        let prefix = self.prefix();
        let mut writes = Vec::new();
        let mut deletes = Vec::new();
        for (row, value) in rows {
            let rest = &row[prefix.len().min(row.len())..];
            let parsed = rest.split_first_chunk::<32>().and_then(|(node, job)| {
                let job: [u8; 16] = job.try_into().ok()?;
                Some((NodeId::from_bytes(node).ok()?, JobId::from_bytes(job)))
            });
            if let Some((waiter, job_id)) = parsed {
                let owed = delivery_key(waiter, job_id, self.key);
                writes.push((KEY_WAIT_KEYSPACE.to_string(), owed, value.clone()));
            }
            deletes.push((KEY_WAIT_KEYSPACE.to_string(), row.clone()));
        }
        (writes, deletes)
    }

    /// The next write of the page, skipping an empty batch as the old helper did.
    fn write_or_commit(
        &mut self,
        txn_id: TxnId,
        writes: JobWrites,
        deletes: JobDeletes,
    ) -> Effects {
        if !writes.is_empty() {
            self.phase = QueuePhase::Write(txn_id);
            self.pending = deletes;
            return smallvec![Effect::Storage(StorageEffect::BatchWrite {
                writes,
                txn_id: Some(txn_id),
            })];
        }
        self.delete_or_commit(txn_id, deletes)
    }

    fn delete_or_commit(&mut self, txn_id: TxnId, deletes: JobDeletes) -> Effects {
        if !deletes.is_empty() {
            self.phase = QueuePhase::Delete(txn_id);
            return smallvec![Effect::Storage(StorageEffect::BatchDelete {
                deletes,
                txn_id: Some(txn_id),
            })];
        }
        self.phase = QueuePhase::Commit(txn_id);
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }
}

impl Operation for QueueWakesOperation {
    type Output = bool;
    type Error = JobMutationError;

    fn start(&mut self) -> Effects {
        self.phase = QueuePhase::Begin;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (self.phase, event) {
            (QueuePhase::Begin, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.phase = QueuePhase::Page(txn_id);
                smallvec![Effect::Storage(StorageEffect::Iter {
                    key_space: KEY_WAIT_KEYSPACE.to_string(),
                    prefix: Some(self.prefix()),
                    start: None,
                    limit: WAKE_PAGE,
                    txn_id: Some(txn_id),
                })]
            }
            (QueuePhase::Page(txn_id), Event::Storage(StorageEvent::IterResult { values, .. })) => {
                self.full = values.len() == WAKE_PAGE;
                let (writes, deletes) = self.rewrite(&values);
                self.write_or_commit(txn_id, writes, deletes)
            }
            (QueuePhase::Write(txn_id), Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                let deletes = std::mem::take(&mut self.pending);
                self.delete_or_commit(txn_id, deletes)
            }
            (
                QueuePhase::Delete(txn_id),
                Event::Storage(StorageEvent::BatchDeleteResult { .. }),
            ) => {
                self.phase = QueuePhase::Commit(txn_id);
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            (QueuePhase::Commit(_), Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                let full = self.full;
                self.finish(Ok(full))
            }
            (QueuePhase::Commit(_), Event::Storage(StorageEvent::Error { error })) => {
                let error = match error {
                    StorageError::TransactionConflict => JobMutationError::Conflict,
                    error => JobMutationError::Storage(error.to_string()),
                };
                self.finish(Err(error))
            }
            (QueuePhase::Begin, Event::Storage(StorageEvent::Error { error })) => {
                self.finish(Err(JobMutationError::Storage(error.to_string())))
            }
            (
                QueuePhase::Page(txn_id) | QueuePhase::Write(txn_id) | QueuePhase::Delete(txn_id),
                Event::Storage(StorageEvent::Error { error }),
            ) => self.fail(txn_id, JobMutationError::Storage(error.to_string())),
            (QueuePhase::Abort, Event::Storage(StorageEvent::TransactionAborted { .. })) => {
                self.phase = QueuePhase::Done;
                smallvec![]
            }
            (QueuePhase::Abort, Event::Storage(StorageEvent::Error { error })) => {
                warn!(error = %error, "Failed to abort the remote wake transaction");
                self.phase = QueuePhase::Done;
                smallvec![]
            }
            (
                QueuePhase::Page(txn_id) | QueuePhase::Write(txn_id) | QueuePhase::Delete(txn_id),
                other,
            ) => self.fail(txn_id, unexpected(&other)),
            (_, other) => self.finish(Err(unexpected(&other))),
        }
    }

    fn is_complete(&self) -> bool {
        self.phase == QueuePhase::Done
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.outcome.unwrap_or(Err(not_finished()))
    }

    fn abort(&mut self) -> Effects {
        match self.phase {
            QueuePhase::Page(txn_id) | QueuePhase::Write(txn_id) | QueuePhase::Delete(txn_id) => {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            _ => smallvec![],
        }
    }

    fn is_conflict(error: &Self::Error) -> bool {
        matches!(error, JobMutationError::Conflict)
    }
}

/// One page of owed wakes after `start_after`, and the cursor of the next page when this one
/// is full. Undecodable rows are skipped but still advance the cursor.
#[derive(Debug, PartialEq)]
pub struct OwedWakesOperation {
    start_after: Option<Key>,
    started: bool,
    outcome: Option<Result<(Vec<OwedWake>, Option<Key>), JobMutationError>>,
}

impl OwedWakesOperation {
    pub fn new(start_after: Option<Key>) -> Self {
        Self {
            start_after,
            started: false,
            outcome: None,
        }
    }
}

impl Operation for OwedWakesOperation {
    type Output = (Vec<OwedWake>, Option<Key>);
    type Error = JobMutationError;

    fn start(&mut self) -> Effects {
        self.started = true;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: KEY_WAIT_KEYSPACE.to_string(),
            prefix: Some(ByteView::from(vec![DELIVERY_PREFIX])),
            start: self.start_after.take().map(IterStart::After),
            limit: WAKE_PAGE,
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        self.outcome = Some(match event {
            _ if !self.started || self.outcome.is_some() => Err(unexpected(&event)),
            Event::Storage(StorageEvent::IterResult { values, .. }) => Ok(owed_page(&values)),
            Event::Storage(StorageEvent::Error { error }) => {
                Err(JobMutationError::Storage(error.to_string()))
            }
            other => Err(unexpected(&other)),
        });
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        self.outcome.is_some()
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.outcome.unwrap_or(Err(not_finished()))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

fn owed_page(rows: &[(Key, Value)]) -> (Vec<OwedWake>, Option<Key>) {
    let next = (rows.len() == WAKE_PAGE)
        .then(|| rows.last().map(|(row, _)| row.clone()))
        .flatten();
    let owed = rows
        .iter()
        .filter_map(|(row, value)| {
            let (waiter, job_id, key) = parse_delivery(row)?;
            let auth = postcard::from_bytes(value).ok()?;
            Some(OwedWake {
                waiter,
                job_id,
                key,
                auth,
            })
        })
        .collect();
    (owed, next)
}

fn unexpected(event: &Event) -> JobMutationError {
    JobMutationError::Storage(format!("unexpected storage event: {}", event.label()))
}

fn not_finished() -> JobMutationError {
    JobMutationError::Storage("remote wait operation did not finish".to_string())
}

#[cfg(test)]
#[path = "remote_wait_tests.rs"]
mod state_machine_tests;
