//! Queues blob replication jobs and live copy obligations for committed versions.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::id_mint::IdMint;
use aruna_core::keyspaces::REPLICATION_JOB_KEYSPACE;
use aruna_core::live_version::drain_scheduled;
use aruna_core::operation::Operation;
use aruna_core::rows::read_row;
use aruna_core::structs::storage::replication_queue::{
    BlobJobRecord, ReplicateScopeInput, blob_job_entry, blob_job_key, blob_job_preferred,
    schedule_blob_drain,
};
use aruna_core::types::{Key, KeySpace, Value};
use aruna_core::user::id::UserId;
use thiserror::Error;
use ulid::Ulid;

use super::plaintext::{consent_delete, consent_row, job_consent};
use super::version_replication::ReplicateScopeError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueBlobResult {
    pub queued: usize,
    pub scheduled: bool,
}

#[derive(Debug, Error, PartialEq)]
pub enum BlobQueueError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Replication(#[from] ReplicateScopeError),
    #[error("unexpected event while processing blob replication queue: {0}")]
    UnexpectedEvent(String),
    #[error("a parked copy job could not wake: {0}")]
    Wake(String),
    #[error("blob replication queue I/O failed: {0}")]
    Io(IoError),
}

impl From<IoError> for BlobQueueError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::Storage(error),
            other => Self::Io(other),
        }
    }
}

type QueueTurn = Result<Next<QueueBlobResult>, BlobQueueError>;

#[derive(Clone, Debug, PartialEq, Eq)]
enum QueueBlobState {
    ClearConsent,
    ReadExisting,
    WriteJob,
    ScheduleDrain,
}

#[derive(Debug, PartialEq)]
pub struct QueueBlobOperation {
    now_ms: u64,
    job: BlobJobRecord,
    state: QueueBlobState,
    /// The plaintext request this queueing records: `Some(None)` withdraws an earlier one.
    consent: Option<Option<UserId>>,
}

impl QueueBlobOperation {
    pub fn new(input: ReplicateScopeInput, source_delete_marker: Option<bool>) -> Self {
        Self {
            now_ms: 0,
            job: BlobJobRecord::new(input, source_delete_marker, 0, Ulid::nil()),
            state: QueueBlobState::ReadExisting,
            consent: None,
        }
    }

    /// Records whether the requester of this explicit copy asked for plaintext. A withdrawn
    /// request is deleted before the job is written; a new one commits with it.
    pub fn with_plaintext(mut self, plaintext: bool) -> Self {
        let requester = self.job.input.auth_context.user_id;
        self.consent = Some(plaintext.then_some(requester));
        self
    }

    pub fn new_relationship(
        input: ReplicateScopeInput,
        source_delete_marker: Option<bool>,
        relationship_id: Ulid,
    ) -> Self {
        Self {
            now_ms: 0,
            job: BlobJobRecord::new_relationship(
                input,
                source_delete_marker,
                relationship_id,
                0,
                Ulid::nil(),
            ),
            state: QueueBlobState::ReadExisting,
            consent: None,
        }
    }

    fn call(&mut self, state: QueueBlobState, call: IoCall) -> QueueTurn {
        self.state = state;
        Ok(Next::Call(call))
    }

    fn read_existing(&mut self) -> QueueTurn {
        let key = blob_job_key(&self.job)?;
        let read = read_row(REPLICATION_JOB_KEYSPACE, key);
        self.call(QueueBlobState::ReadExisting, read)
    }

    fn write_job(&mut self) -> QueueTurn {
        let entry = blob_job_entry(&self.job)?;
        let consent = self.consent_entry(&entry.1);
        let rows = std::iter::once(entry).chain(consent).collect();
        self.call(QueueBlobState::WriteJob, IoCall::Write(rows))
    }

    /// The row of a granted plaintext request for the job stored under `key`.
    fn consent_entry(&self, key: &Key) -> Option<(KeySpace, Key, Value)> {
        let requester = self.consent.flatten()?;
        consent_row(job_consent(key), requester).ok()
    }

    /// Withdraws an earlier plaintext request before the job is written again.
    fn clear_consent(&mut self) -> QueueTurn {
        let key = blob_job_key(&self.job)?;
        let delete = consent_delete(job_consent(&key));
        self.call(QueueBlobState::ClearConsent, delete)
    }

    /// A preferred existing job stays, but a granted request is still recorded for it.
    fn keep_existing(&mut self) -> QueueTurn {
        let key = blob_job_key(&self.job)?;
        match self.consent_entry(&key) {
            Some(row) => self.call(QueueBlobState::WriteJob, IoCall::Write(vec![row])),
            None => self.schedule_drain(),
        }
    }

    fn existing_read(&mut self, value: Option<Value>) -> QueueTurn {
        let existing = value.and_then(|value| BlobJobRecord::from_bytes(&value).ok());
        match existing {
            Some(existing) if blob_job_preferred(&existing, &self.job) => self.keep_existing(),
            _ => self.write_job(),
        }
    }

    fn schedule_drain(&mut self) -> QueueTurn {
        self.call(QueueBlobState::ScheduleDrain, schedule_blob_drain().into())
    }
}

impl Operation for QueueBlobOperation {
    type Output = QueueBlobResult;
    type Error = BlobQueueError;

    fn first(&mut self) -> QueueTurn {
        // The job is due and enqueued when the run starts.
        self.job.due_at_ms = self.now_ms;
        self.job.enqueued_at_ms = self.now_ms;
        match self.consent {
            Some(None) => self.clear_consent(),
            _ => self.read_existing(),
        }
    }

    fn next(&mut self, result: IoResult) -> QueueTurn {
        match self.state {
            QueueBlobState::ClearConsent => {
                result.done()?;
                self.read_existing()
            }
            QueueBlobState::ReadExisting => self.existing_read(result.value()?),
            QueueBlobState::WriteJob => {
                result.done()?;
                self.schedule_drain()
            }
            QueueBlobState::ScheduleDrain => Ok(Next::Done(QueueBlobResult {
                queued: 1,
                scheduled: drain_scheduled(result),
            })),
        }
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    /// Mints the job id from the run, after the stamp.
    fn seed(&mut self, entropy: u128) {
        self.job.job_id = IdMint::new(entropy).mint(self.now_ms);
    }
}

/// Rows of the job queue one page of the statistics reads.
const STATS_PAGE_SIZE: usize = 256;

/// Counts queued replication jobs bound to a relationship and the oldest enqueue time among
/// them, in Unix milliseconds.
#[derive(Debug, PartialEq)]
pub struct JobStatsOperation {
    relationship_id: Ulid,
    pending: usize,
    oldest: Option<u64>,
}

impl JobStatsOperation {
    pub fn new(relationship_id: Ulid) -> Self {
        Self {
            relationship_id,
            pending: 0,
            oldest: None,
        }
    }

    fn read_page(after: Option<Key>) -> IoCall {
        IoCall::Page {
            space: REPLICATION_JOB_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after,
            limit: STATS_PAGE_SIZE,
        }
    }
}

impl Operation for JobStatsOperation {
    type Output = (usize, Option<u64>);
    type Error = BlobQueueError;

    fn first(&mut self) -> Result<Next<Self::Output>, BlobQueueError> {
        Ok(Next::Call(Self::read_page(None)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, BlobQueueError> {
        let (values, next) = result.page()?;
        for (_, value) in values {
            let record = BlobJobRecord::from_bytes(value.as_ref())?;
            if record.relationship_id == Some(self.relationship_id) {
                self.pending = self.pending.saturating_add(1);
                let enqueued = record.enqueued_at_ms;
                self.oldest = Some(self.oldest.map_or(enqueued, |oldest| oldest.min(enqueued)));
            }
        }
        Ok(match next {
            Some(next) => Next::Call(Self::read_page(Some(next))),
            None => Next::Done((self.pending, self.oldest)),
        })
    }
}

#[cfg(test)]
#[path = "queue_tests.rs"]
mod tests;
