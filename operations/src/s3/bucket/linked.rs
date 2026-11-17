//! Starts, pauses, resumes and aborts the one long job of a bucket: a key re-mapping, a
//! duplicate file cleanup or a rename of linked files.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BUCKET_ENCRYPTION_KEYSPACE, COMPRESSION_MIGRATION_KEYSPACE, LINKED_JOB_KEYSPACE,
    RENAME_MOVE_KEYSPACE, RENAME_TOTAL_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::backend_import::{RenameRequest, RenameTotal};
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::encryption::BucketEncryption;
use aruna_core::structs::storage::format::CompressionMigration;
use aruna_core::structs::storage::linked_job::{LinkedJob, LinkedState};
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::types::{Effects, GroupId, Key, TxnId, Value};
use smallvec::smallvec;
use thiserror::Error;

/// What the caller asks of the bucket's job.
#[derive(Clone, Debug, PartialEq)]
pub enum LinkedAction {
    Start(Box<LinkedJob>),
    Pause,
    Resume,
    Abort,
}

/// The job an action changed: a linked job, or the storage move that renames linked files.
#[derive(Clone, Debug, PartialEq)]
pub enum BucketJob {
    Linked(LinkedJob),
    Rename(CompressionMigration, RenameRequest),
}

#[derive(Debug, Error, PartialEq)]
pub enum LinkedError {
    #[error("{0}")]
    Invalid(String),
    #[error("bucket not found")]
    NotFound,
    /// Another job holds the bucket.
    #[error("{0}")]
    Busy(String),
    /// No running or paused job to act on, or the action does not fit its state.
    #[error("{0}")]
    NoJob(String),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("unexpected event in a bucket job change")]
    Unexpected,
}

/// The rows a bucket job change reads, in this order.
fn reads(bucket: &str) -> Vec<(String, Key)> {
    let key: Key = bucket.as_bytes().to_vec().into();
    [
        S3_BUCKET_KEYSPACE,
        BUCKET_ENCRYPTION_KEYSPACE,
        LINKED_JOB_KEYSPACE,
        COMPRESSION_MIGRATION_KEYSPACE,
        RENAME_MOVE_KEYSPACE,
    ]
    .map(|space| (space.to_string(), key.clone()))
    .into()
}

/// The rows of a bucket the job change decides on.
#[derive(Default)]
struct Rows {
    bucket: Option<BucketInfo>,
    encrypted: bool,
    linked: Option<LinkedJob>,
    migration: Option<CompressionMigration>,
    rename: Option<RenameRequest>,
}

impl Rows {
    fn parse(values: Vec<(Key, Option<Value>)>) -> Result<Self, LinkedError> {
        let Ok([bucket, encryption, linked, migration, rename]) =
            <[_; 5]>::try_from(values).map(|values| values.map(|(_, value)| value))
        else {
            return Err(LinkedError::Unexpected);
        };
        Ok(Self {
            bucket: bucket.as_deref().map(BucketInfo::from_bytes).transpose()?,
            encrypted: BucketEncryption::from_row(encryption.as_deref())?.is_encrypted(),
            linked: linked.as_deref().map(LinkedJob::from_bytes).transpose()?,
            migration: (migration.as_deref())
                .map(CompressionMigration::from_bytes)
                .transpose()?,
            rename: rename
                .as_deref()
                .map(RenameRequest::from_bytes)
                .transpose()?,
        })
    }

    /// The rename request of the unfinished move it belongs to.
    fn running_rename(&self) -> Option<(&CompressionMigration, &RenameRequest)> {
        let migration = self.migration.as_ref()?;
        let request = self.rename.as_ref()?;
        let running = migration.moving && migration.finished_at_ms.is_none();
        (running && request.started_at_ms == migration.started_at_ms)
            .then_some((migration, request))
    }
}

/// The rows an action writes and the job it leaves, decided on what the transaction read.
/// The rows a job change writes and deletes, and the job it leaves.
type Decided = (Vec<(String, Key, Value)>, Vec<(String, Key)>, BucketJob);

fn decide(
    rows: Rows,
    bucket: &str,
    group_id: GroupId,
    action: &LinkedAction,
    now_ms: u64,
) -> Result<Decided, LinkedError> {
    match rows.bucket.as_ref() {
        Some(info) if info.group_id == group_id => {}
        _ => return Err(LinkedError::NotFound),
    }
    let key: Key = bucket.as_bytes().to_vec().into();
    let active = rows.linked.clone().filter(LinkedJob::is_active);
    if let LinkedAction::Start(job) = action {
        if active.is_some() {
            return Err(LinkedError::Busy("another job of this bucket runs".into()));
        }
        let moving = (rows.migration.as_ref())
            .is_some_and(|record| record.moving && record.finished_at_ms.is_none());
        if moving {
            return Err(LinkedError::Busy(
                "a storage move of this bucket runs".into(),
            ));
        }
        if rows.encrypted {
            return Err(LinkedError::Invalid(
                "an encrypted bucket keeps its keys and files".into(),
            ));
        }
        let job = (**job).clone();
        let value = job.to_bytes()?.into();
        let writes = vec![(LINKED_JOB_KEYSPACE.to_string(), key, value)];
        return Ok((writes, Vec::new(), BucketJob::Linked(job)));
    }
    if let Some(mut job) = active {
        job.state = match (job.state, action) {
            (LinkedState::Running, LinkedAction::Pause) => LinkedState::Paused,
            (LinkedState::Paused, LinkedAction::Resume) => LinkedState::Running,
            (_, LinkedAction::Abort) => LinkedState::Aborted,
            (state, _) => return Err(LinkedError::NoJob(format!("the job is {state:?}"))),
        };
        job.updated_at_ms = now_ms;
        if job.state == LinkedState::Aborted {
            job.finished_at_ms = Some(now_ms);
        }
        let value = job.to_bytes()?.into();
        let writes = vec![(LINKED_JOB_KEYSPACE.to_string(), key, value)];
        return Ok((writes, Vec::new(), BucketJob::Linked(job)));
    }
    let Some((migration, request)) = rows.running_rename() else {
        return Err(LinkedError::NoJob("no job of this bucket runs".into()));
    };
    let (migration, mut request) = (migration.clone(), request.clone());
    if request.aborted_at_ms.is_some() {
        return Err(LinkedError::NoJob("the rename is aborted".into()));
    }
    match (request.paused, action) {
        (false, LinkedAction::Pause) => request.paused = true,
        (true, LinkedAction::Resume) => request.paused = false,
        // The move ends once the version in flight is done, with its counts.
        (_, LinkedAction::Abort) => request.aborted_at_ms = Some(now_ms),
        (paused, _) => {
            return Err(LinkedError::NoJob(format!(
                "the rename is {}",
                if paused { "paused" } else { "running" }
            )));
        }
    }
    let writes = vec![(
        RENAME_MOVE_KEYSPACE.to_string(),
        key,
        request.to_bytes()?.into(),
    )];
    Ok((writes, Vec::new(), BucketJob::Rename(migration, request)))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ControlState {
    Init,
    Start,
    Read,
    Delete,
    Write,
    Commit,
    Finish,
}

/// Applies one action to the bucket's job in one transaction and wakes the worker for a job
/// that runs again.
#[derive(Debug, PartialEq)]
pub struct BucketJobOperation {
    bucket: String,
    group_id: GroupId,
    action: LinkedAction,
    now_ms: u64,
    state: ControlState,
    txn_id: Option<TxnId>,
    writes: Vec<(String, Key, Value)>,
    output: Option<Result<BucketJob, LinkedError>>,
}

impl BucketJobOperation {
    pub fn new(bucket: String, group_id: GroupId, action: LinkedAction, now_ms: u64) -> Self {
        Self {
            bucket,
            group_id,
            action,
            now_ms,
            state: ControlState::Init,
            txn_id: None,
            writes: Vec::new(),
            output: None,
        }
    }

    fn fail(&mut self, error: LinkedError) -> Effects {
        self.state = ControlState::Finish;
        self.output = Some(Err(error));
        self.abort()
    }

    fn read(&mut self, values: Vec<(Key, Option<Value>)>) -> Effects {
        let decided = Rows::parse(values)
            .and_then(|rows| decide(rows, &self.bucket, self.group_id, &self.action, self.now_ms));
        match decided {
            Ok((writes, deletes, job)) => {
                self.writes = writes;
                self.output = Some(Ok(job));
                self.state = ControlState::Delete;
                smallvec![Effect::Storage(StorageEffect::BatchDelete {
                    deletes,
                    txn_id: self.txn_id,
                })]
            }
            Err(error) => self.fail(error),
        }
    }

    fn wakes(&self) -> bool {
        // An abort wakes the worker too, which ends an aborted rename.
        !matches!(self.action, LinkedAction::Pause)
    }
}

impl Operation for BucketJobOperation {
    type Output = BucketJob;
    type Error = LinkedError;

    fn start(&mut self) -> Effects {
        self.state = ControlState::Start;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        let Event::Storage(event) = event else {
            return self.fail(LinkedError::Unexpected);
        };
        match (self.state, event) {
            (_, StorageEvent::Error { error }) => self.fail(error.into()),
            (ControlState::Start, StorageEvent::TransactionStarted { txn_id }) => {
                self.txn_id = Some(txn_id);
                self.state = ControlState::Read;
                smallvec![Effect::Storage(StorageEffect::BatchRead {
                    reads: reads(&self.bucket),
                    txn_id: Some(txn_id),
                })]
            }
            (ControlState::Read, StorageEvent::BatchReadResult { values }) => self.read(values),
            (ControlState::Delete, StorageEvent::BatchDeleteResult { .. }) => {
                self.state = ControlState::Write;
                smallvec![Effect::Storage(StorageEffect::BatchWrite {
                    writes: std::mem::take(&mut self.writes),
                    txn_id: self.txn_id,
                })]
            }
            (ControlState::Write, StorageEvent::BatchWriteResult { .. }) => {
                let Some(txn_id) = self.txn_id else {
                    return self.fail(LinkedError::Unexpected);
                };
                self.state = ControlState::Commit;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            (ControlState::Commit, StorageEvent::TransactionCommitted { .. }) => {
                self.txn_id = None;
                self.state = ControlState::Finish;
                match self.wakes() {
                    true => smallvec![Effect::Task(TaskEffect::ShortenTimer {
                        key: TaskKey::MigrateCompression,
                        after: Duration::ZERO,
                    })],
                    false => smallvec![],
                }
            }
            _ => self.fail(LinkedError::Unexpected),
        }
    }

    fn is_complete(&self) -> bool {
        self.state == ControlState::Finish
    }

    fn finalize(self) -> Result<BucketJob, LinkedError> {
        match self.state {
            ControlState::Finish => self.output.unwrap_or(Err(LinkedError::Unexpected)),
            _ => Err(LinkedError::Unexpected),
        }
    }

    fn abort(&mut self) -> Effects {
        self.txn_id
            .take()
            .map(|txn_id| smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })])
            .unwrap_or_default()
    }
}

/// The job of a bucket as stored: the running one, else the one started last.
pub struct ShownJobOperation {
    bucket: String,
    result: Option<Result<Option<BucketJob>, LinkedError>>,
}

impl ShownJobOperation {
    pub fn new(bucket: &str) -> Self {
        Self {
            bucket: bucket.to_string(),
            result: None,
        }
    }
}

impl Operation for ShownJobOperation {
    type Output = Option<BucketJob>;
    type Error = LinkedError;

    fn start(&mut self) -> Effects {
        let mut reads = reads(&self.bucket);
        reads.drain(..2);
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        self.result = Some(match event {
            Event::Storage(StorageEvent::BatchReadResult { values }) => {
                shown_job(values).map_err(LinkedError::from)
            }
            Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
            _ => Err(LinkedError::Unexpected),
        });
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        self.result.is_some()
    }

    fn finalize(self) -> Result<Option<BucketJob>, LinkedError> {
        self.result.unwrap_or(Err(LinkedError::Unexpected))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

/// The shown job of the linked job, migration and rename rows, read in that order.
fn shown_job(values: Vec<(Key, Option<Value>)>) -> Result<Option<BucketJob>, ConversionError> {
    let mut values = values.into_iter().map(|(_, value)| value);
    let mut next = || values.next().flatten();
    let (linked, migration, rename) = (next(), next(), next());
    let linked = linked.as_deref().map(LinkedJob::from_bytes).transpose()?;
    let migration = migration.as_deref().map(CompressionMigration::from_bytes);
    let migration = migration.transpose()?;
    let rename = rename
        .as_deref()
        .map(RenameRequest::from_bytes)
        .transpose()?;
    let rename = match (migration, rename) {
        (Some(migration), Some(request))
            if migration.moving && migration.started_at_ms == request.started_at_ms =>
        {
            Some((migration, request))
        }
        _ => None,
    };
    let rename_runs = (rename.as_ref()).is_some_and(|(migration, request)| {
        migration.finished_at_ms.is_none() && request.aborted_at_ms.is_none()
    });
    Ok(match (linked, rename) {
        (Some(job), _) if job.is_active() => Some(BucketJob::Linked(job)),
        (_, Some((migration, request))) if rename_runs => {
            Some(BucketJob::Rename(migration, request))
        }
        (Some(job), Some((migration, request))) => {
            match job.started_at_ms > migration.started_at_ms {
                true => Some(BucketJob::Linked(job)),
                false => Some(BucketJob::Rename(migration, request)),
            }
        }
        (Some(job), None) => Some(BucketJob::Linked(job)),
        (None, Some((migration, request))) => Some(BucketJob::Rename(migration, request)),
        (None, None) => None,
    })
}

/// The files the rename of the move started at `started_at_ms` planned to rename; `None` when
/// none was stored for that move.
pub struct RenameTotalOperation {
    bucket: String,
    started_at_ms: u64,
    result: Option<Result<Option<u64>, LinkedError>>,
}

impl RenameTotalOperation {
    pub fn new(bucket: &str, started_at_ms: u64) -> Self {
        Self {
            bucket: bucket.to_string(),
            started_at_ms,
            result: None,
        }
    }
}

impl Operation for RenameTotalOperation {
    type Output = Option<u64>;
    type Error = LinkedError;

    fn start(&mut self) -> Effects {
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: RENAME_TOTAL_KEYSPACE.to_string(),
            key: self.bucket.as_bytes().to_vec().into(),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        self.result = Some(match event {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                let row = value.as_deref().map(RenameTotal::from_bytes).transpose();
                row.map(|row| row.and_then(|row| row.for_move(self.started_at_ms)))
                    .map_err(LinkedError::from)
            }
            Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
            _ => Err(LinkedError::Unexpected),
        });
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        self.result.is_some()
    }

    fn finalize(self) -> Result<Option<u64>, LinkedError> {
        self.result.unwrap_or(Err(LinkedError::Unexpected))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
#[path = "linked_tests.rs"]
mod tests;
