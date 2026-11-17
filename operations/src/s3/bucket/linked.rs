//! Starts, pauses, resumes and aborts the one long job of a bucket: a key re-mapping, a
//! duplicate file cleanup or a rename of linked files.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::driver::DriverContext;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::errors::{ConversionError, StorageError};
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
use aruna_core::types::{GroupId, Key, Value};
use std::time::Duration;
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
    #[error(transparent)]
    Io(IoError),
}

impl From<IoError> for LinkedError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::Storage(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::Storage(error),
            other => Self::Io(other),
        }
    }
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
    fn parse(values: Vec<Option<Value>>) -> Result<Self, LinkedError> {
        let Ok([bucket, encryption, linked, migration, rename]) = <[_; 5]>::try_from(values) else {
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
    Read,
    Delete,
    Write,
}

type ControlStep = Result<Next<BucketJob>, LinkedError>;

/// Applies one action to the bucket's job in one transaction and wakes the worker for a job
/// that runs again, once the change committed.
#[derive(Debug, PartialEq)]
pub struct BucketJobOperation {
    bucket: String,
    group_id: GroupId,
    action: LinkedAction,
    now_ms: u64,
    state: ControlState,
    writes: Vec<(String, Key, Value)>,
    output: Option<BucketJob>,
}

impl BucketJobOperation {
    pub fn new(bucket: String, group_id: GroupId, action: LinkedAction, now_ms: u64) -> Self {
        Self {
            bucket,
            group_id,
            action,
            now_ms,
            state: ControlState::Read,
            writes: Vec::new(),
            output: None,
        }
    }

    fn read(&mut self, values: Vec<Option<Value>>) -> ControlStep {
        let rows = Rows::parse(values)?;
        let (writes, deletes, job) =
            decide(rows, &self.bucket, self.group_id, &self.action, self.now_ms)?;
        self.writes = writes;
        self.output = Some(job);
        self.state = ControlState::Delete;
        Ok(Next::Call(IoCall::Delete(deletes)))
    }

    /// The job left, with the worker's wake after the commit unless the job only paused. An
    /// abort wakes the worker too, which ends an aborted rename.
    fn finish(&mut self) -> ControlStep {
        let output = self.output.take().ok_or(IoError::Unfinished)?;
        if matches!(self.action, LinkedAction::Pause) {
            return Ok(Next::Done(output));
        }
        let wake = TaskEffect::ShortenTimer {
            key: TaskKey::MigrateCompression,
            after: Duration::ZERO,
        };
        let calls = vec![IoCall::Effect(Box::new(Effect::Task(wake)))];
        Ok(Next::Then { output, calls })
    }
}

impl Operation for BucketJobOperation {
    type Output = BucketJob;
    type Error = LinkedError;

    fn first(&mut self) -> ControlStep {
        Ok(Next::Call(IoCall::Read(reads(&self.bucket))))
    }

    fn next(&mut self, result: IoResult) -> ControlStep {
        match self.state {
            ControlState::Read => self.read(result.values()?),
            ControlState::Delete => {
                result.done()?;
                self.state = ControlState::Write;
                Ok(Next::Call(IoCall::Write(std::mem::take(&mut self.writes))))
            }
            ControlState::Write => {
                result.done()?;
                self.finish()
            }
        }
    }

    fn transaction(&self) -> bool {
        true
    }
}

/// The job of a bucket as stored: the running one, else the one started last.
#[derive(Debug, PartialEq)]
pub struct ShownJobOperation {
    bucket: String,
}

impl ShownJobOperation {
    pub fn new(bucket: &str) -> Self {
        Self {
            bucket: bucket.to_string(),
        }
    }
}

impl Operation for ShownJobOperation {
    type Output = Option<BucketJob>;
    type Error = LinkedError;

    fn first(&mut self) -> Result<Next<Option<BucketJob>>, LinkedError> {
        let mut reads = reads(&self.bucket);
        reads.drain(..2);
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<BucketJob>>, LinkedError> {
        Ok(Next::Done(shown_job(result.values()?)?))
    }
}

/// The shown job of the linked job, migration and rename rows, read in that order.
fn shown_job(values: Vec<Option<Value>>) -> Result<Option<BucketJob>, ConversionError> {
    let mut values = values.into_iter();
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
#[derive(Debug, PartialEq)]
pub struct RenameTotalOperation {
    bucket: String,
    started_at_ms: u64,
}

impl RenameTotalOperation {
    pub fn new(bucket: &str, started_at_ms: u64) -> Self {
        Self {
            bucket: bucket.to_string(),
            started_at_ms,
        }
    }
}

impl Operation for RenameTotalOperation {
    type Output = Option<u64>;
    type Error = LinkedError;

    fn first(&mut self) -> Result<Next<Option<u64>>, LinkedError> {
        let key: Key = self.bucket.as_bytes().to_vec().into();
        Ok(Next::Call(IoCall::Read(vec![(
            RENAME_TOTAL_KEYSPACE.to_string(),
            key,
        )])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<u64>>, LinkedError> {
        let row = result
            .value()?
            .as_deref()
            .map(RenameTotal::from_bytes)
            .transpose()?;
        Ok(Next::Done(
            row.and_then(|row| row.for_move(self.started_at_ms)),
        ))
    }
}

#[cfg(test)]
#[path = "linked_tests.rs"]
mod tests;
