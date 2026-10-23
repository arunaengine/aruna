//! Tells the job that runs or is paused on a bucket, from the job rows kept per bucket.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::blob::purge::is_paused;
use crate::driver::DriverContext;
use crate::groups::imports::last_run;
use crate::groups::imports::schedule::is_running;
use crate::jobs::store::read_job_record;
use aruna_core::effects::StorageEffect;
use aruna_core::errors::ConversionError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    COMPRESSION_MIGRATION_KEYSPACE, LINKED_JOB_KEYSPACE, PURGE_FENCE_KEYSPACE,
    RENAME_MOVE_KEYSPACE, RENAME_TOTAL_KEYSPACE, STORAGE_CHECK_KEYSPACE, TRANSITION_KEYSPACE,
};
use aruna_core::structs::execution::job::JobId;
use aruna_core::structs::storage::backend_import::{BackendImport, RenameRequest, RenameTotal};
use aruna_core::structs::storage::broken::StorageCheck;
use aruna_core::structs::storage::format::CompressionMigration;
use aruna_core::structs::storage::linked_job::{LinkedJob, LinkedState, LinkedTask};
use aruna_core::structs::storage::storage_purge::StoragePurgeFence;
use aruna_core::structs::storage::transition::EncryptionTransition;
use aruna_core::types::{Key, Value};

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

/// The job that runs or is paused on `bucket`: the one its job rows tell, else a running purge,
/// else a running import run of one of the bucket's rules among `imports`.
pub async fn active_job(
    context: &DriverContext,
    bucket: &str,
    imports: &[BackendImport],
) -> Result<Option<ActiveJob>, String> {
    let key: Key = bucket.as_bytes().to_vec().into();
    let reads = SPACES.map(|space| (space.to_string(), key.clone())).into();
    let read = StorageEffect::BatchRead {
        reads,
        txn_id: None,
    };
    let Event::Storage(StorageEvent::BatchReadResult { values }) =
        context.storage_handle.send_storage_effect(read).await
    else {
        return Err("could not read the jobs of the bucket".to_string());
    };
    let values = values.into_iter().map(|(_, value)| value).collect();
    let rows = match Rows::parse(values) {
        Ok(rows) => rows,
        // A job row that cannot be read hides the bucket's job, never the whole listing.
        Err(error) => {
            tracing::warn!(bucket, %error, "A job row of the bucket cannot be read");
            return Ok(None);
        }
    };
    if let Some(job) = rows.active() {
        return Ok(Some(job));
    }
    if let Some(fence) = rows.fence
        && let Some(job) = purge_job(context, fence.job_id).await?
    {
        return Ok(Some(job));
    }
    for rule in imports.iter().filter(|rule| rule.bucket == bucket) {
        let run = last_run(context, rule).await;
        let run = run.map_err(|error| error.to_string())?;
        if let Some(run) = run.filter(|run| is_running(run.state)) {
            let counts = run.checkpoint;
            let done = counts.imported.saturating_add(counts.skipped);
            return Ok(Some(ActiveJob::running(
                JobKind::Import,
                done,
                counts.failed,
            )));
        }
    }
    Ok(None)
}

/// The purge holding the bucket's write fence while its job runs. A purge counts no failures:
/// a failure retries or ends the whole job.
async fn purge_job(context: &DriverContext, job_id: JobId) -> Result<Option<ActiveJob>, String> {
    let record = read_job_record(&context.storage_handle, job_id, None).await?;
    let Some(record) = record.filter(|record| is_running(record.state)) else {
        return Ok(None);
    };
    let paused = is_paused(context, job_id.as_ulid()).await?;
    Ok(Some(ActiveJob {
        paused,
        ..ActiveJob::running(JobKind::Purge, record.progress.current, 0)
    }))
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

    #[tokio::test]
    async fn hides_unreadable_job() {
        // A job row that cannot be read, such as one of an older layout, hides only that job.
        let temp = tempfile::tempdir().unwrap();
        let storage = aruna_storage::FjallStorage::open(temp.path().to_str().unwrap()).unwrap();
        let context = crate::tests::s3::test_context(storage.clone());
        let write = StorageEffect::Write {
            key_space: COMPRESSION_MIGRATION_KEYSPACE.to_string(),
            key: b"b".to_vec().into(),
            value: vec![0xff; 3].into(),
            txn_id: None,
        };
        storage.send_storage_effect(write).await;
        assert_eq!(active_job(&context, "b", &[]).await, Ok(None));
    }
}
