//! Tells the job that runs or is paused on a bucket, from the job rows kept per bucket.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::blob::pause::{PauseError, PausedOperation};
use crate::s3::imports::cron::is_running;
use crate::s3::imports::{ImportRuleError, LastRun, LastRunOperation};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::ConversionError;
use aruna_core::keyspaces::{
    COMPRESSION_MIGRATION_KEYSPACE, JOB_KEYSPACE, LINKED_JOB_KEYSPACE, PURGE_FENCE_KEYSPACE,
    RENAME_MOVE_KEYSPACE, RENAME_TOTAL_KEYSPACE, STORAGE_CHECK_KEYSPACE, TRANSITION_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::rows::read_row;
use aruna_core::structs::execution::job::{JobId, JobRecord, job_record_key};
use aruna_core::structs::storage::backend_import::{BackendImport, RenameRequest, RenameTotal};
use aruna_core::structs::storage::broken::StorageCheck;
use aruna_core::structs::storage::format::CompressionMigration;
use aruna_core::structs::storage::linked_job::{LinkedJob, LinkedState, LinkedTask};
use aruna_core::structs::storage::storage_purge::StoragePurgeFence;
use aruna_core::structs::storage::transition::EncryptionTransition;
use aruna_core::types::{Key, Value};
use std::collections::VecDeque;
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum ActiveJobError {
    #[error(transparent)]
    Io(#[from] IoError),
    #[error(transparent)]
    Paused(#[from] PauseError),
    #[error(transparent)]
    Import(#[from] ImportRuleError),
}

/// What the job of a bucket does.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobKind {
    /// A storage move that also renames linked files.
    Rename,
    /// A storage move to the storage the bucket's rules choose now.
    Move,
    Compression,
    Encryption,
    /// A cleanup of linked files with the same content.
    Duplicates,
    /// A re-mapping of keys to new names.
    KeyMove,
    Check,
    Purge,
    Import,
}

/// A job that runs or is paused on a bucket, with its counts so far.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ActiveJob {
    pub kind: JobKind,
    pub paused: bool,
    /// Items the job finished so far, changed or left as they are.
    pub done: u64,
    pub failed: u64,
    /// The files a rename planned to rename, where its start stored them.
    pub total: Option<u64>,
}

impl ActiveJob {
    fn running(kind: JobKind, done: u64, failed: u64) -> Self {
        Self {
            kind,
            paused: false,
            done,
            failed,
            total: None,
        }
    }
}

/// The job rows kept per bucket, in the order `Rows::parse` takes them.
const SPACES: [&str; 7] = [
    COMPRESSION_MIGRATION_KEYSPACE,
    RENAME_MOVE_KEYSPACE,
    RENAME_TOTAL_KEYSPACE,
    LINKED_JOB_KEYSPACE,
    TRANSITION_KEYSPACE,
    STORAGE_CHECK_KEYSPACE,
    PURGE_FENCE_KEYSPACE,
];

#[derive(Default)]
struct Rows {
    migration: Option<CompressionMigration>,
    rename: Option<RenameRequest>,
    total: Option<RenameTotal>,
    linked: Option<LinkedJob>,
    transition: Option<EncryptionTransition>,
    check: Option<StorageCheck>,
    fence: Option<StoragePurgeFence>,
}

impl Rows {
    fn parse(values: Vec<Option<Value>>) -> Result<Self, ConversionError> {
        let mut values = values.into_iter();
        let mut next = || values.next().flatten();
        Ok(Self {
            migration: (next().as_deref())
                .map(CompressionMigration::from_bytes)
                .transpose()?,
            rename: next()
                .as_deref()
                .map(RenameRequest::from_bytes)
                .transpose()?,
            total: next().as_deref().map(RenameTotal::from_bytes).transpose()?,
            linked: next().as_deref().map(LinkedJob::from_bytes).transpose()?,
            transition: (next().as_deref())
                .map(EncryptionTransition::from_bytes)
                .transpose()?,
            check: next()
                .as_deref()
                .map(StorageCheck::from_bytes)
                .transpose()?,
            fence: (next().as_deref())
                .map(StoragePurgeFence::from_bytes)
                .transpose()?,
        })
    }

    /// The job the rows tell, in this order: the migration record (a rename, move or compression),
    /// a key move or duplicate cleanup, an encryption change, then a storage check.
    fn active(&self) -> Option<ActiveJob> {
        let migration = self.migration.as_ref();
        if let Some(record) = migration.filter(|record| record.finished_at_ms.is_none()) {
            let rename = (self.rename.as_ref())
                .filter(|request| record.moving && request.started_at_ms == record.started_at_ms);
            let kind = match (rename, record.moving) {
                (Some(_), _) => JobKind::Rename,
                (None, true) => JobKind::Move,
                (None, false) => JobKind::Compression,
            };
            // An aborted rename shows as running until its move ends.
            let paused =
                rename.is_some_and(|request| request.paused && request.aborted_at_ms.is_none());
            // A rename counts the files it moved, as its own job shows, never versions that stay.
            let done = match rename {
                Some(_) => record.migrated,
                None => record.migrated.saturating_add(record.skipped),
            };
            let total = rename.and(self.total);
            return Some(ActiveJob {
                paused,
                total: total.and_then(|row| row.for_move(record.started_at_ms)),
                ..ActiveJob::running(kind, done, record.failed)
            });
        }
        if let Some(job) = self.linked.as_ref().filter(|job| job.is_active()) {
            let kind = match job.task {
                LinkedTask::Remap(_) => JobKind::KeyMove,
                LinkedTask::Unify { .. } => JobKind::Duplicates,
            };
            let done = job.done.saturating_add(job.skipped);
            return Some(ActiveJob {
                paused: job.state == LinkedState::Paused,
                ..ActiveJob::running(kind, done, job.failed)
            });
        }
        let transition = self.transition.as_ref();
        if let Some(record) = transition.filter(|record| record.finished_at_ms.is_none()) {
            let job = ActiveJob::running(JobKind::Encryption, record.done, record.failed);
            return Some(job);
        }
        let check = (self.check.as_ref()).filter(|check| check.finished_at_ms.is_none())?;
        let done = check.checked.saturating_sub(check.failed);
        Some(ActiveJob::running(JobKind::Check, done, check.failed))
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum JobStep {
    Rows,
    PurgeRecord(JobId),
    PurgePaused(u64),
    LastRun,
}

type JobTurn = Result<Next<Option<ActiveJob>>, ActiveJobError>;

/// The job that runs or is paused on `bucket`: the one its job rows tell, else a running purge,
/// else a running import run of one of the bucket's rules among `imports`.
#[derive(Debug, PartialEq)]
pub struct ActiveJobOperation {
    bucket: String,
    rules: VecDeque<BackendImport>,
    step: JobStep,
}

impl ActiveJobOperation {
    pub fn new(bucket: &str, imports: &[BackendImport]) -> Self {
        Self {
            bucket: bucket.to_string(),
            rules: (imports.iter())
                .filter(|rule| rule.bucket == bucket)
                .cloned()
                .collect(),
            step: JobStep::Rows,
        }
    }

    fn call(&mut self, step: JobStep, call: IoCall) -> JobTurn {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn rows_read(&mut self, values: Vec<Option<Value>>) -> JobTurn {
        let rows = match Rows::parse(values) {
            Ok(rows) => rows,
            // A job row that cannot be read hides the bucket's job, never the whole listing.
            Err(error) => {
                tracing::warn!(bucket = %self.bucket, %error, "A job row of the bucket cannot be read");
                return Ok(Next::Done(None));
            }
        };
        if let Some(job) = rows.active() {
            return Ok(Next::Done(Some(job)));
        }
        match rows.fence {
            Some(fence) => {
                let read = read_row(JOB_KEYSPACE, job_record_key(fence.job_id));
                self.call(JobStep::PurgeRecord(fence.job_id), read)
            }
            None => self.next_rule(),
        }
    }

    /// The purge holding the bucket's write fence while its job runs. A malformed job row is
    /// retained for offline recovery and reads as absent.
    fn purge_read(&mut self, job_id: JobId, value: Option<Value>) -> JobTurn {
        let record = value.and_then(|value| JobRecord::from_bytes(&value).ok());
        let Some(record) = record.filter(|record| is_running(record.state)) else {
            return self.next_rule();
        };
        let paused = PausedOperation::new(job_id.as_ulid());
        let done = record.progress.current;
        self.call(JobStep::PurgePaused(done), IoCall::child(paused))
    }

    fn next_rule(&mut self) -> JobTurn {
        let Some(rule) = self.rules.pop_front() else {
            return Ok(Next::Done(None));
        };
        let run = LastRunOperation::new(&rule);
        self.call(JobStep::LastRun, IoCall::child(run))
    }

    fn run_read(&mut self, run: Option<LastRun>) -> JobTurn {
        let Some(run) = run.filter(|run| is_running(run.state)) else {
            return self.next_rule();
        };
        let counts = run.checkpoint;
        let done = counts.imported.saturating_add(counts.skipped);
        let job = ActiveJob::running(JobKind::Import, done, counts.failed);
        Ok(Next::Done(Some(job)))
    }
}

impl Operation for ActiveJobOperation {
    type Output = Option<ActiveJob>;
    type Error = ActiveJobError;

    fn first(&mut self) -> JobTurn {
        let key: Key = self.bucket.as_bytes().to_vec().into();
        let reads = SPACES.map(|space| (space.to_string(), key.clone())).into();
        self.call(JobStep::Rows, IoCall::Read(reads))
    }

    fn next(&mut self, result: IoResult) -> JobTurn {
        match self.step {
            JobStep::Rows => self.rows_read(result.values()?),
            JobStep::PurgeRecord(job_id) => self.purge_read(job_id, result.value()?),
            // A purge counts no failures: a failure retries or ends the whole job.
            JobStep::PurgePaused(done) => {
                let paused = result.child::<bool, PauseError>()??;
                Ok(Next::Done(Some(ActiveJob {
                    paused,
                    ..ActiveJob::running(JobKind::Purge, done, 0)
                })))
            }
            JobStep::LastRun => self.run_read(result.child::<Option<LastRun>, ImportRuleError>()??),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::records::{REALM, node};
    use aruna_core::structs::identity::auth::AuthContext;
    use aruna_core::structs::storage::backend_import::TokenGrant;
    use aruna_core::structs::storage::format::Compression;
    use ulid::Ulid;

    fn auth() -> AuthContext {
        AuthContext {
            user_id: Default::default(),
            realm_id: REALM,
            path_restrictions: None,
            session: None,
        }
    }

    fn grant() -> TokenGrant {
        TokenGrant {
            token_hash: "token".to_string(),
            issuer: REALM,
            issued_at_secs: 1,
            expires_at_secs: u64::MAX,
            via: None,
        }
    }

    fn rename(started_at_ms: u64) -> RenameRequest {
        RenameRequest {
            started_at_ms,
            auth: auth(),
            node_id: node(4),
            grant: grant(),
            paused: true,
            aborted_at_ms: None,
        }
    }

    fn unify() -> LinkedJob {
        LinkedJob {
            id: Ulid::from_parts(1, 1),
            task: LinkedTask::Unify {
                prefix: String::new(),
            },
            group_id: Ulid::from_parts(2, 2),
            auth: auth(),
            node_id: node(4),
            grant: grant(),
            state: LinkedState::Paused,
            cursor: None,
            total: 9,
            counted: true,
            done: 4,
            skipped: 1,
            failed: 2,
            waiting: None,
            started_at_ms: 1,
            updated_at_ms: 1,
            finished_at_ms: None,
        }
    }

    #[test]
    fn picks_bucket_job() {
        let mut record = CompressionMigration::new(Compression::Off, 7);
        (record.migrated, record.skipped, record.failed) = (3, 2, 1);
        let mut rows = Rows {
            migration: Some(record.clone()),
            rename: Some(rename(7)),
            linked: Some(unify()),
            ..Rows::default()
        };
        let job = |kind, paused, done, failed| {
            let job = ActiveJob::running(kind, done, failed);
            Some(ActiveJob { paused, ..job })
        };
        // A compression record ignores a rename request; only a move renames.
        assert_eq!(rows.active(), job(JobKind::Compression, false, 5, 1));
        record.moving = true;
        rows.migration = Some(record.clone());
        assert_eq!(rows.active(), job(JobKind::Rename, true, 3, 1));
        // A rename shows the total its start stored, never one of an older move.
        let total = |started_at_ms| RenameTotal {
            started_at_ms,
            total: 9,
        };
        rows.total = Some(total(6));
        assert_eq!(rows.active().and_then(|job| job.total), None);
        rows.total = Some(total(7));
        assert_eq!(rows.active().and_then(|job| job.total), Some(9));
        rows.total = None;
        // An aborted rename runs until its move ends; a request of another move is not its own.
        rows.rename = rows.rename.map(|request| RenameRequest {
            aborted_at_ms: Some(9),
            ..request
        });
        assert_eq!(rows.active(), job(JobKind::Rename, false, 3, 1));
        rows.rename = Some(rename(6));
        assert_eq!(rows.active(), job(JobKind::Move, false, 5, 1));
        // A finished move leaves the paused duplicate cleanup.
        record.finished_at_ms = Some(9);
        rows.migration = Some(record);
        assert_eq!(rows.active(), job(JobKind::Duplicates, true, 5, 2));
        rows.linked = rows.linked.map(|job| LinkedJob {
            state: LinkedState::Finished,
            ..job
        });
        rows.check = Some(StorageCheck {
            checked: 6,
            failed: 2,
            ..StorageCheck::new(1)
        });
        assert_eq!(rows.active(), job(JobKind::Check, false, 4, 2));
        rows.check = rows.check.map(|check| StorageCheck {
            finished_at_ms: Some(2),
            ..check
        });
        assert_eq!(rows.active(), None);
    }

    fn no_rows() -> IoResult {
        IoResult::Values(vec![None; SPACES.len()])
    }

    #[test]
    fn hides_unreadable_job() {
        // A job row that cannot be read, such as one of an older layout, hides only that job.
        let mut operation = ActiveJobOperation::new("b", &[]);
        assert!(matches!(operation.first(), Ok(Next::Call(IoCall::Read(_)))));
        let mut values = vec![None; SPACES.len()];
        values[0] = Some(Value::from(vec![0xff; 3]));
        let turn = operation.next(IoResult::Values(values));
        assert_eq!(turn, Ok(Next::Done(None)));
    }

    #[test]
    fn no_rows_no_job() {
        let mut operation = ActiveJobOperation::new("b", &[]);
        operation.first().unwrap();
        assert_eq!(operation.next(no_rows()), Ok(Next::Done(None)));
    }

    #[test]
    fn other_buckets_skipped() {
        let mut rule = crate::s3::bucket::rename::tests::rule();
        rule.bucket = "other".to_string();
        let mut operation = ActiveJobOperation::new("b", &[rule]);
        operation.first().unwrap();
        assert_eq!(operation.next(no_rows()), Ok(Next::Done(None)));
    }

    #[test]
    fn finished_run_passes() {
        let mut rule = crate::s3::bucket::rename::tests::rule();
        rule.bucket = "b".to_string();
        let mut operation = ActiveJobOperation::new("b", &[rule]);
        operation.first().unwrap();
        let turn = operation.next(no_rows());
        assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
        let none = Ok::<Option<LastRun>, ImportRuleError>(None);
        let answer = IoResult::Child(aruna_core::calls::ChildOutcome::new(none));
        assert_eq!(operation.next(answer), Ok(Next::Done(None)));
    }
}
