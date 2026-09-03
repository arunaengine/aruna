//! Rewrites records stored before per-run workspace buckets were dropped and
//! multipart completions gained a lease. Safe to repeat: a current record is
//! left untouched.

use crate::error::CliError;
use crate::explorer::ExplorerError;
use aruna_core::keyspaces::{JOB_KEYSPACE, S3_MULTIPART_UPLOAD_KEYSPACE};
use aruna_core::structs::{
    AttemptIntent, CapturedInput, JobClaim, JobError, JobExecutionClass, JobId, JobPayload,
    JobProgress, JobRecord, JobResultPayload, JobState, MultipartUpload, WorkspaceMode,
};
use aruna_core::types::{NodeId, UserId};
use fjall::{KeyspaceCreateOptions, OptimisticTxDatabase, Readable};
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Serialize)]
pub struct MigrateOutput {
    pub database_path: String,
    pub uploads_scanned: usize,
    pub uploads_patched: usize,
    pub jobs_scanned: usize,
    pub jobs_rewritten: usize,
    /// Unfinished runs settled because the workspace bucket they would have
    /// captured into no longer exists.
    pub jobs_failed: usize,
}

pub async fn migrate(database_path: String) -> Result<(), CliError> {
    let output = tokio::task::spawn_blocking({
        let database_path = database_path.clone();
        move || migrate_output(&database_path)
    })
    .await
    .map_err(std::io::Error::other)??;

    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

fn migrate_output(database_path: &str) -> Result<MigrateOutput, ExplorerError> {
    let db = OptimisticTxDatabase::builder(Path::new(database_path)).open()?;
    let uploads = db.keyspace(S3_MULTIPART_UPLOAD_KEYSPACE, KeyspaceCreateOptions::default)?;
    let jobs = db.keyspace(JOB_KEYSPACE, KeyspaceCreateOptions::default)?;

    let mut uploads_scanned = 0;
    let mut patched = Vec::new();
    for entry in db.read_tx().iter(&uploads) {
        let (key, value) = entry.into_inner()?;
        uploads_scanned += 1;
        if let Some(value) =
            patched_upload(value.as_ref()).map_err(|error| decode_error(&key, error))?
        {
            patched.push((key.to_vec(), value));
        }
    }

    let mut jobs_scanned = 0;
    let mut jobs_failed = 0;
    let mut rewritten = Vec::new();
    for entry in db.read_tx().iter(&jobs) {
        let (key, value) = entry.into_inner()?;
        jobs_scanned += 1;
        if let Some((value, settled)) =
            rewritten_job(value.as_ref()).map_err(|error| decode_error(&key, error))?
        {
            jobs_failed += usize::from(settled);
            rewritten.push((key.to_vec(), value));
        }
    }

    let mut txn = db.write_tx()?;
    for (key, value) in &patched {
        txn.insert(uploads.clone(), key.clone(), value.clone());
    }
    for (key, value) in &rewritten {
        txn.insert(jobs.clone(), key.clone(), value.clone());
    }
    txn.commit()?.map_err(|_| {
        ExplorerError::Decode("migration conflicted with a running node".to_string())
    })?;

    Ok(MigrateOutput {
        database_path: database_path.to_string(),
        uploads_scanned,
        uploads_patched: patched.len(),
        jobs_scanned,
        jobs_rewritten: rewritten.len(),
        jobs_failed,
    })
}

fn decode_error(key: &[u8], error: impl std::fmt::Display) -> ExplorerError {
    ExplorerError::Decode(format!("{}: {error}", hex::encode(key)))
}

/// A record written before the completion lease lacks its trailing `None`.
fn patched_upload(value: &[u8]) -> Result<Option<Vec<u8>>, aruna_core::errors::ConversionError> {
    if MultipartUpload::from_bytes(value).is_ok() {
        return Ok(None);
    }
    let mut patched = value.to_vec();
    patched.push(0);
    MultipartUpload::from_bytes(&patched)?;
    Ok(Some(patched))
}

/// Message a settled run carries, so its submitter knows to resubmit it.
const WORKSPACE_DROPPED: &str =
    "per-run workspace buckets were dropped during an upgrade; submit the run again";

/// The pre-drop layout of `JobRecord`: identical except for the mode, which is
/// decoded as the legacy enum so a dropped per-run bucket stays visible.
#[derive(Deserialize)]
struct LegacyJobRecord {
    job_id: JobId,
    payload: JobPayload,
    state: JobState,
    created_by: UserId,
    owner_node_id: NodeId,
    created_at_ms: u64,
    started_at_ms: Option<u64>,
    updated_at_ms: u64,
    due_at_ms: u64,
    finished_at_ms: Option<u64>,
    attempts: u32,
    next_attempt_epoch: u64,
    has_run: bool,
    last_error: Option<JobError>,
    progress: JobProgress,
    cancel_requested: bool,
    claim: Option<JobClaim>,
    dedup_key: Option<Vec<u8>>,
    result: Option<JobResultPayload>,
    execution_class: JobExecutionClass,
    plan_digest: Option<[u8; 32]>,
    attempt_intent: Option<AttemptIntent>,
    workspace_bucket: Option<String>,
    workspace_mode: LegacyWorkspaceMode,
    captured_inputs: Vec<CapturedInput>,
    report_digest: Option<[u8; 32]>,
    retention_ms: u64,
    locally_exhausted: bool,
}

/// Variant order of the dropped enum; a current record reads as its first two.
#[derive(Deserialize)]
enum LegacyWorkspaceMode {
    Temporary,
    Kept,
    Existing,
    None,
}

impl LegacyJobRecord {
    /// Rebuilds the current record. A run that would have captured into a
    /// per-run bucket which no longer exists is settled rather than left to
    /// succeed without its workspace outputs.
    fn into_record(self) -> (JobRecord, bool) {
        let kept_bucket = matches!(
            self.workspace_mode,
            LegacyWorkspaceMode::Kept | LegacyWorkspaceMode::Existing
        )
        .then_some(self.workspace_bucket.clone())
        .flatten();
        // A dropped bucket is only ever a `Temporary` one, and a current
        // record reads as `Temporary` exactly when it has no bucket at all.
        let stranded = match self.workspace_mode {
            LegacyWorkspaceMode::Temporary => self.workspace_bucket.is_some(),
            LegacyWorkspaceMode::Kept => self.workspace_bucket.is_none(),
            _ => false,
        };
        let settle = stranded && !self.state.is_terminal();
        let workspace_mode = match kept_bucket {
            Some(_) => WorkspaceMode::Existing,
            None => WorkspaceMode::None,
        };
        let mut record = JobRecord {
            job_id: self.job_id,
            payload: self.payload,
            state: self.state,
            created_by: self.created_by,
            owner_node_id: self.owner_node_id,
            created_at_ms: self.created_at_ms,
            started_at_ms: self.started_at_ms,
            updated_at_ms: self.updated_at_ms,
            due_at_ms: self.due_at_ms,
            finished_at_ms: self.finished_at_ms,
            attempts: self.attempts,
            next_attempt_epoch: self.next_attempt_epoch,
            has_run: self.has_run,
            last_error: self.last_error,
            progress: self.progress,
            cancel_requested: self.cancel_requested,
            claim: self.claim,
            dedup_key: self.dedup_key,
            result: self.result,
            execution_class: self.execution_class,
            plan_digest: self.plan_digest,
            attempt_intent: self.attempt_intent,
            workspace_bucket: kept_bucket,
            workspace_mode,
            captured_inputs: self.captured_inputs,
            report_digest: self.report_digest,
            retention_ms: self.retention_ms,
            locally_exhausted: self.locally_exhausted,
        };
        if settle {
            record.state = JobState::Failed;
            record.last_error = Some(JobError::permanent(WORKSPACE_DROPPED));
            record.finished_at_ms = Some(record.updated_at_ms);
            record.claim = None;
        }
        (record, settle)
    }
}

/// Only an `Existing` run keeps a bucket; a dropped `ws-` bucket is gone. The
/// flag reports whether the run was settled as failed.
fn rewritten_job(value: &[u8]) -> Result<Option<(Vec<u8>, bool)>, postcard::Error> {
    let legacy: LegacyJobRecord = postcard::from_bytes(value)?;
    let (record, settled) = legacy.into_record();
    let bytes = postcard::to_allocvec(&record)?;
    if bytes == value {
        return Ok(None);
    }
    Ok(Some((bytes, settled)))
}

#[cfg(test)]
mod tests {
    use super::migrate_output;
    use aruna_core::keyspaces::{JOB_KEYSPACE, S3_MULTIPART_UPLOAD_KEYSPACE};
    use aruna_core::structs::{
        BackendRef, JobId, JobPayload, JobRecord, JobState, MultipartUpload, MultipartUploadStatus,
        RealmId, WorkspaceMode,
    };
    use aruna_core::types::UserId;
    use fjall::{KeyspaceCreateOptions, OptimisticTxDatabase, Readable};
    use std::collections::{BTreeMap, HashMap};
    use std::path::Path;
    use std::time::UNIX_EPOCH;
    use tempfile::tempdir;
    use ulid::Ulid;

    fn owner() -> UserId {
        UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32]))
    }

    fn upload() -> MultipartUpload {
        MultipartUpload {
            upload_id: Ulid::from_bytes([4u8; 16]),
            backend: BackendRef::node_default(),
            storage_class: None,
            bucket: "data".to_string(),
            key: "reads/pending.fastq".to_string(),
            group_id: Ulid::from_bytes([5u8; 16]),
            created_by: owner(),
            created_at: UNIX_EPOCH,
            status: MultipartUploadStatus::Open,
            checksum_hint: None,
            metadata: HashMap::new(),
            placement_policies: Vec::new(),
            subject_generation: 0,
            completing_since_ms: None,
        }
    }

    fn job(mode: WorkspaceMode, bucket: Option<&str>, state: JobState) -> JobRecord {
        let payload = JobPayload::Probe {
            steps: 1,
            step_sleep_ms: 0,
            fail_at: None,
            panic_at: None,
            cleanup_marker: None,
        };
        let node = iroh::SecretKey::from_bytes(&[7u8; 32]).public();
        let mut record = JobRecord::new(
            JobId::from_bytes([3u8; 16]),
            payload,
            owner(),
            node,
            0,
            0,
            None,
        );
        record.workspace_mode = mode;
        record.workspace_bucket = bucket.map(str::to_string);
        record.state = state;
        record
    }

    /// An old record differs from a current one only in the mode's discriminant.
    fn legacy_job(discriminant: u8, bucket: Option<&str>, state: JobState) -> Vec<u8> {
        let mut bytes = job(WorkspaceMode::None, bucket, state).to_bytes().unwrap();
        let existing = job(WorkspaceMode::Existing, bucket, state)
            .to_bytes()
            .unwrap();
        let index = bytes
            .iter()
            .zip(&existing)
            .position(|(a, b)| a != b)
            .unwrap();
        bytes[index] = discriminant;
        bytes
    }

    fn write(path: &Path, keyspace: &str, entries: Vec<(&[u8], Vec<u8>)>) {
        let db = OptimisticTxDatabase::builder(path).open().unwrap();
        let keyspace = db
            .keyspace(keyspace, KeyspaceCreateOptions::default)
            .unwrap();
        let mut txn = db.write_tx().unwrap();
        for (key, value) in entries {
            txn.insert(keyspace.clone(), key, value);
        }
        txn.commit().unwrap().unwrap();
    }

    fn read(path: &Path, keyspace: &str) -> BTreeMap<Vec<u8>, Vec<u8>> {
        let db = OptimisticTxDatabase::builder(path).open().unwrap();
        let keyspace = db
            .keyspace(keyspace, KeyspaceCreateOptions::default)
            .unwrap();
        db.read_tx()
            .iter(&keyspace)
            .map(|entry| {
                let (key, value) = entry.into_inner().unwrap();
                (key.to_vec(), value.to_vec())
            })
            .collect()
    }

    #[test]
    fn patches_legacy_uploads() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("db");
        let current = upload().to_bytes().unwrap();
        let mut legacy = current.clone();
        legacy.pop();
        write(
            &path,
            S3_MULTIPART_UPLOAD_KEYSPACE,
            vec![(b"old", legacy), (b"new", current.clone())],
        );

        let output = migrate_output(path.to_str().unwrap()).unwrap();

        assert_eq!((output.uploads_scanned, output.uploads_patched), (2, 1));
        for value in read(&path, S3_MULTIPART_UPLOAD_KEYSPACE).into_values() {
            assert_eq!(value, current);
        }
        let again = migrate_output(path.to_str().unwrap()).unwrap();
        assert_eq!(again.uploads_patched, 0);
    }

    #[test]
    fn rewrites_legacy_jobs() {
        // The dropped enum read Temporary 0, Kept 1, Existing 2, None 3.
        let temp = tempdir().unwrap();
        let path = temp.path().join("db");
        write(
            &path,
            JOB_KEYSPACE,
            vec![
                (
                    b"temporary",
                    legacy_job(0, Some("ws-run"), JobState::Queued),
                ),
                (
                    b"finished",
                    legacy_job(0, Some("ws-run"), JobState::Succeeded),
                ),
                (b"kept", legacy_job(1, Some("ws-kept"), JobState::Queued)),
                (b"stranded", legacy_job(1, None, JobState::Running)),
                (b"existing", legacy_job(2, Some("shared"), JobState::Queued)),
                (b"none", legacy_job(3, None, JobState::Queued)),
            ],
        );

        let output = migrate_output(path.to_str().unwrap()).unwrap();

        assert_eq!(
            (
                output.jobs_scanned,
                output.jobs_rewritten,
                output.jobs_failed
            ),
            (6, 5, 2)
        );
        let records: BTreeMap<Vec<u8>, JobRecord> = read(&path, JOB_KEYSPACE)
            .into_iter()
            .map(|(key, value)| (key, JobRecord::from_bytes(&value).unwrap()))
            .collect();
        let settled = |key: &[u8]| {
            let record = &records[key];
            (record.workspace_mode, record.workspace_bucket.clone())
        };
        assert_eq!(settled(b"temporary"), (WorkspaceMode::None, None));
        assert_eq!(settled(b"finished"), (WorkspaceMode::None, None));
        assert_eq!(
            settled(b"kept"),
            (WorkspaceMode::Existing, Some("ws-kept".to_string()))
        );
        assert_eq!(settled(b"stranded"), (WorkspaceMode::None, None));
        // An unfinished run whose per-run bucket is gone must not run on and
        // report success without its workspace outputs.
        for key in [b"temporary".as_slice(), b"stranded".as_slice()] {
            assert_eq!(records[key].state, JobState::Failed);
            assert!(records[key].last_error.is_some());
            assert_eq!(
                records[key].finished_at_ms,
                Some(records[key].updated_at_ms)
            );
            assert!(records[key].claim.is_none());
        }
        assert_eq!(records[b"finished".as_slice()].state, JobState::Succeeded);
        assert_eq!(
            settled(b"existing"),
            (WorkspaceMode::Existing, Some("shared".to_string()))
        );
        assert_eq!(settled(b"none"), (WorkspaceMode::None, None));
        let again = migrate_output(path.to_str().unwrap()).unwrap();
        assert_eq!(again.jobs_rewritten, 0);
    }
}
