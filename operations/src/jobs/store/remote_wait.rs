//! Remote waits on this node's bucket keys and the wakes owed to the waiting nodes.
//! A wake row stays until the waiting node acknowledges it; delivery may repeat.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::encryption::BucketKeyRef;

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::rows::encoded_row;

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
    call: Option<Result<IoCall, JobMutationError>>,
}

impl WaitRowOperation {
    /// Records that `waiter` parks `job_id` on `key` for `auth`. The caller checks the registry
    /// afterwards and answers "already available" when the key is unlocked, so no unlock is missed.
    pub fn register(key: BucketKeyRef, waiter: NodeId, job_id: JobId, auth: &AuthContext) -> Self {
        let row = registration_key(key, waiter, job_id);
        let write = encoded_row(KEY_WAIT_KEYSPACE, row, auth).map(|row| IoCall::Write(vec![row]));
        Self {
            call: Some(write.map_err(JobMutationError::from)),
        }
    }

    /// Drops an owed wake once the waiting node acknowledged it.
    pub fn acknowledge(waiter: NodeId, job_id: JobId, key: BucketKeyRef) -> Self {
        let row = delivery_key(waiter, job_id, key);
        Self {
            call: Some(Ok(IoCall::Delete(vec![(KEY_WAIT_KEYSPACE.to_string(), row)]))),
        }
    }
}

impl Operation for WaitRowOperation {
    type Output = ();
    type Error = JobMutationError;

    fn first(&mut self) -> Result<Next<()>, JobMutationError> {
        let call = self.call.take().unwrap_or(Err(not_finished()))?;
        Ok(Next::Call(call))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, JobMutationError> {
        match result {
            IoResult::Done => Ok(Next::Done(())),
            other => Err(unexpected(&other)),
        }
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueuePhase {
    Page,
    Write,
    Delete,
}

impl QueueWakesOperation {
    pub fn new(key: BucketKeyRef) -> Self {
        Self {
            key,
            phase: QueuePhase::Page,
            full: false,
            pending: Vec::new(),
        }
    }

    fn prefix(&self) -> Key {
        ByteView::from([&[REMOTE_PREFIX][..], &self.key.key()].concat())
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
}

impl Operation for QueueWakesOperation {
    type Output = bool;
    type Error = JobMutationError;

    fn first(&mut self) -> Result<Next<bool>, JobMutationError> {
        Ok(Next::Call(IoCall::Page {
            space: KEY_WAIT_KEYSPACE.to_string(),
            prefix: self.prefix(),
            after: None,
            limit: WAKE_PAGE,
        }))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, JobMutationError> {
        match (self.phase, result) {
            (QueuePhase::Page, IoResult::Page { rows, .. }) => {
                self.full = rows.len() == WAKE_PAGE;
                let (writes, deletes) = self.rewrite(&rows);
                self.pending = deletes;
                self.phase = QueuePhase::Write;
                Ok(Next::Call(IoCall::Write(writes)))
            }
            (QueuePhase::Write, IoResult::Done) => {
                self.phase = QueuePhase::Delete;
                Ok(Next::Call(IoCall::Delete(std::mem::take(
                    &mut self.pending,
                ))))
            }
            (QueuePhase::Delete, IoResult::Done) => Ok(Next::Done(self.full)),
            (_, other) => Err(unexpected(&other)),
        }
    }

    fn transaction(&self) -> bool {
        true
    }

    fn is_conflict(error: &Self::Error) -> bool {
        matches!(error, JobMutationError::Conflict)
    }
}

/// Owed wakes of one page and the cursor of the next page.
pub type OwedPage = (Vec<OwedWake>, Option<Key>);

/// One page of owed wakes after `start_after`, and the cursor of the next page when this one
/// is full. Undecodable rows are skipped but still advance the cursor.
#[derive(Debug, PartialEq)]
pub struct OwedWakesOperation {
    start_after: Option<Key>,
}

impl OwedWakesOperation {
    pub fn new(start_after: Option<Key>) -> Self {
        Self { start_after }
    }
}

impl Operation for OwedWakesOperation {
    type Output = OwedPage;
    type Error = JobMutationError;

    fn first(&mut self) -> Result<Next<OwedPage>, JobMutationError> {
        Ok(Next::Call(IoCall::Page {
            space: KEY_WAIT_KEYSPACE.to_string(),
            prefix: ByteView::from(vec![DELIVERY_PREFIX]),
            after: self.start_after.take(),
            limit: WAKE_PAGE,
        }))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<OwedPage>, JobMutationError> {
        match result {
            IoResult::Page { rows, .. } => Ok(Next::Done(owed_page(&rows))),
            other => Err(unexpected(&other)),
        }
    }
}

fn owed_page(rows: &[(Key, Value)]) -> OwedPage {
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

fn unexpected(result: &IoResult) -> JobMutationError {
    JobMutationError::Storage(format!("unexpected storage result: {}", result.label()))
}

fn not_finished() -> JobMutationError {
    JobMutationError::Storage("remote wait operation did not finish".to_string())
}

#[cfg(test)]
#[path = "remote_wait_tests.rs"]
mod state_machine_tests;
