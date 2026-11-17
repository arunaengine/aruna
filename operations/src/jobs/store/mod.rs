//! Owns the job store reads and the guarded job mutation helpers over the shared job rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::driver::DriverContext;
use aruna_core::UserId;
use aruna_core::compute::{AttemptRef, FenceContext};
use aruna_core::effects::StorageEffect;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::PURGE_FENCE_KEYSPACE;
use aruna_core::keyspaces::{
    ARTIFACT_TOMBSTONE_KEYSPACE, ATTEMPT_CONTROL_KEYSPACE, DEDUP_INDEX_KEYSPACE,
    JOB_CLAIM_KEYSPACE, JOB_ENTRY_KEYSPACE, JOB_INDEX_KEYSPACE, JOB_KEYSPACE, KEY_WAIT_KEYSPACE,
    LAUNCH_RECORD_KEYSPACE, OUTPUT_RECORD_KEYSPACE, PURGE_CHECKPOINT_KEYSPACE, RUN_CRATE_KEYSPACE,
    SCHEDULE_INDEX_KEYSPACE,
};
use aruna_core::structs::execution::job::{
    AttemptControl, AttemptIntent, JobClaim, JobError, JobErrorKind, JobExecutionClass, JobId,
    JobPayload, JobProgress, JobRecord, JobRecordEnvelope, JobRecordError, JobResultPayload,
    JobState, JobTransitionError, LaunchRecord, RunCrateStatus, attempt_control_key,
    cleanup_dedup_key, cleanup_job_id, crate_dedup_key, crate_job_id, job_entry_key,
    job_entry_prefix, job_owner_cursor, job_prune_key, job_record_key, job_wait_key,
    owner_index_key, owner_index_prefix, parse_dedup_value, parse_entry_key, parse_owner_key,
    run_crate_key, validate_transition, workspace_credential_id,
};
use aruna_core::structs::execution::job::{
    ClaimGuard, JobDeletes, JobWrites, dedup_index_key, index_deltas, job_insert_entries,
    prune_delete_entries,
};
use aruna_core::structs::storage::blob::UserAccess;
use aruna_core::structs::storage::storage_purge::StoragePurgeCheckpoint;
use aruna_core::types::{Key, KeySpace, TxnId, Value};
use byteview::ByteView;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use super::lifecycle::ids::session_of;
use super::{JOB_LEASE_MS, JOB_MAX_ATTEMPTS, JOB_PRUNE_PAGE, MUTATE_MAX_ATTEMPTS};
use crate::storage_read::iter_prefix_page;
use crate::tasks::queue_backoff::retry_delay_ms;

pub mod attempt;
pub mod attempt_intent;
pub mod control_mutation;
pub mod job_rows;
pub mod key_wait;
pub mod list_jobs;
pub mod mark_start;
pub mod mutation;
pub mod park_wake;
pub mod query;
pub mod remote_wait;
pub mod run_delete;
pub mod state;

use crate::driver::drive;
use aruna_core::operation::Runnable;
use attempt::*;
use job_rows::{PutEntryOperation, PutRowsOperation};
use key_wait::*;
use mutation::{MutateJobOperation, NoGuard};
use query::*;
use run_delete::DeleteRunOperation;
use state::*;

#[derive(Debug, Serialize, Deserialize)]
struct ArtifactTombstone {
    owner: UserId,
    expires_at_ms: u64,
}

#[derive(Debug, PartialEq, Error)]
pub enum JobMutationError {
    #[error("job not found")]
    NotFound,
    #[error("job claim token mismatch")]
    TokenMismatch,
    #[error("attempt intent conflicts with the committed lineage")]
    IntentConflict,
    #[error("attempt epoch exhausted")]
    EpochExhausted,
    #[error("controller generation exhausted")]
    GenerationExhausted,
    #[error("attempt control record is missing")]
    MissingControl,
    #[error("job report is frozen")]
    ReportFrozen,
    #[error("attempt control epoch mismatch")]
    EpochMismatch,
    #[error("execution output record digest conflicts with the stored digest")]
    OutputRecordConflict,
    #[error(transparent)]
    IllegalTransition(#[from] JobTransitionError),
    #[error("execution outputs are not proven durable: {0}")]
    OutputsUnproven(#[from] JobRecordError),
    #[error("{0}")]
    Storage(String),
    /// The write transaction lost to a concurrent one; a fresh run may succeed.
    #[error("job storage transaction conflicted")]
    Conflict,
}

impl From<aruna_core::calls::IoError> for JobMutationError {
    fn from(error: aruna_core::calls::IoError) -> Self {
        match error {
            aruna_core::calls::IoError::Conflict => Self::Conflict,
            error => Self::Storage(error.to_string()),
        }
    }
}

/// Outcome of a mutation closure: whether the caller wants the record persisted.
pub enum JobMutation {
    Persist,
    Skip,
}

fn guard_token(record: &JobRecord, token: Ulid) -> Result<(), JobMutationError> {
    match &record.claim {
        Some(claim) if claim.claim_token == token => Ok(()),
        _ => Err(JobMutationError::TokenMismatch),
    }
}

/// Read, mutate, and persist a job with its index deltas in one transaction, with
/// bounded OCC retry so a commit conflict re-reads and re-applies rather than losing.
pub async fn mutate_job<F>(
    context: &DriverContext,
    job_id: JobId,
    mutate: F,
) -> Result<JobRecord, JobMutationError>
where
    F: FnMut(&mut JobRecord) -> Result<JobMutation, JobMutationError> + Send,
{
    let mut mutate = mutate;
    let mut attempt = 0;
    loop {
        let result = drive(
            MutateJobOperation::new(job_id, &mut mutate, None::<NoGuard>),
            context,
        )
        .await;
        match settle_attempt(result, attempt, "job mutation exhausted conflict retries").await {
            Some(result) => return result,
            None => attempt += 1,
        }
    }
}

/// What one attempt of a job write leaves: `None` after a lost conflict that is retried, once
/// a short growing pause passed; otherwise the result, or the `exhausted` error after the last
/// of [`MUTATE_MAX_ATTEMPTS`] attempts.
pub(crate) async fn settle_attempt<T>(
    result: Result<T, JobMutationError>,
    attempt: u32,
    exhausted: &str,
) -> Option<Result<T, JobMutationError>> {
    match result {
        Err(JobMutationError::Conflict) if attempt + 1 < MUTATE_MAX_ATTEMPTS => {
            tokio::time::sleep(std::time::Duration::from_millis(1 << attempt.min(6))).await;
            None
        }
        Err(JobMutationError::Conflict) => {
            Some(Err(JobMutationError::Storage(exhausted.to_string())))
        }
        other => Some(other),
    }
}

/// Drives a fresh `build()` per attempt while attempts lose a conflict; see [`settle_attempt`].
pub(super) async fn drive_settled<O, K>(
    context: &DriverContext,
    mut build: impl FnMut() -> O,
    exhausted: &str,
) -> Result<O::Output, JobMutationError>
where
    O: Runnable<K, Error = JobMutationError>,
{
    let mut attempt = 0;
    loop {
        match settle_attempt(drive(build(), context).await, attempt, exhausted).await {
            Some(result) => return result,
            None => attempt += 1,
        }
    }
}

/// What deleting a run from this node's lists found.
#[derive(Debug, PartialEq, Eq)]
pub enum RunDelete {
    Deleted,
    /// No run of this caller is listed on this node.
    NotFound,
    /// The job is in this state and still live; it must be cancelled first.
    Unfinished(JobState),
    /// The run's container cleanup is still queued.
    CleanupPending,
    /// The run's deletion could not be recorded in its replicated family, so it was kept.
    Unrecorded(String),
}

/// Removes a finished run of `user_id` from this node in one transaction: its row, list index
/// and every local row pruning removes. An execution run goes only once its deletion was
/// `recorded` in its replicated family, whose records stay.
pub async fn delete_finished_run(
    context: &DriverContext,
    user_id: UserId,
    job_id: JobId,
    recorded: bool,
) -> Result<RunDelete, JobMutationError> {
    let build = || DeleteRunOperation::new(user_id, job_id, recorded);
    drive_settled(context, build, "run delete exhausted conflict retries").await
}

/// Same transaction as [`mutate_job`], with `guard` re-checking the mutated
/// record against the attempt control read in that transaction; a rejection
/// aborts before any write, so the invariant is atomic with the state change.
pub async fn mutate_job_guarded<F, G>(
    context: &DriverContext,
    job_id: JobId,
    mut mutate: F,
    mut guard: G,
) -> Result<JobRecord, JobMutationError>
where
    F: FnMut(&mut JobRecord) -> Result<JobMutation, JobMutationError> + Send,
    G: FnMut(&JobRecord, Option<&AttemptControl>) -> Result<(), JobMutationError> + Send,
{
    let mut attempt = 0;
    loop {
        let result = drive(
            MutateJobOperation::new(job_id, &mut mutate, Some(&mut guard)),
            context,
        )
        .await;
        match settle_attempt(result, attempt, "job mutation exhausted conflict retries").await {
            Some(result) => return result,
            None => attempt += 1,
        }
    }
}

/// Read and postcard-decode a state row, keeping a missing row distinct from a malformed one.
pub(super) async fn read_state<T: DeserializeOwned>(
    context: &DriverContext,
    key_space: &str,
    key: Key,
    label: &str,
) -> Result<Option<T>, String> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: key_space.to_string(),
            key,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => postcard::from_bytes(value.as_ref())
            .map(Some)
            .map_err(|error| error.to_string()),
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Ok(None),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected {label} event: {}", other.label())),
    }
}

/// Postcard-encode `row` and persist it through the token-guarded checkpoint write.
pub(super) async fn put_state<T: Serialize>(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    key_space: &str,
    key: Key,
    row: &T,
) -> Result<(), JobMutationError> {
    let value = postcard::to_allocvec(row)
        .map(ByteView::from)
        .map_err(|error| JobMutationError::Storage(error.to_string()))?;
    put_job_checkpoint(context, job_id, token, key_space, key, value).await
}

pub async fn put_purge_checkpoint(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    checkpoint: &StoragePurgeCheckpoint,
) -> Result<(), JobMutationError> {
    let value = checkpoint
        .to_bytes()
        .map(ByteView::from)
        .map_err(|error| JobMutationError::Storage(error.to_string()))?;
    put_job_checkpoint(
        context,
        job_id,
        token,
        PURGE_CHECKPOINT_KEYSPACE,
        ByteView::from(job_id.to_bytes().to_vec()),
        value,
    )
    .await
}

pub async fn read_purge_checkpoint(
    context: &DriverContext,
    job_id: JobId,
) -> Result<Option<StoragePurgeCheckpoint>, JobMutationError> {
    read_raw(
        context,
        PURGE_CHECKPOINT_KEYSPACE,
        ByteView::from(job_id.to_bytes().to_vec()),
        None,
    )
    .await
    .map_err(JobMutationError::Storage)?
    .map(|value| {
        StoragePurgeCheckpoint::from_bytes(value.as_ref())
            .map_err(|error| JobMutationError::Storage(error.to_string()))
    })
    .transpose()
}

async fn put_job_checkpoint(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    key_space: &str,
    key: Key,
    value: Value,
) -> Result<(), JobMutationError> {
    let writes = vec![(key_space.to_string(), key, value)];
    put_job_rows(context, job_id, token, writes, Vec::new()).await
}

/// Writes and deletes a running job's own rows in one transaction, guarded by its claim token.
pub(crate) async fn put_job_rows(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    writes: Vec<(KeySpace, Key, Value)>,
    deletes: Vec<(KeySpace, Key)>,
) -> Result<(), JobMutationError> {
    let build = || PutRowsOperation::new(job_id, token, writes.clone(), deletes.clone());
    drive_settled(
        context,
        build,
        "checkpoint write exhausted conflict retries",
    )
    .await
}

pub async fn put_job_entry<T: Serialize>(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    entry_key: &[u8],
    row: &T,
) -> Result<(), JobMutationError> {
    let value = postcard::to_allocvec(row)
        .map(ByteView::from)
        .map_err(|error| JobMutationError::Storage(error.to_string()))?;
    let build = || PutEntryOperation::new(job_id, token, entry_key, value.clone());
    drive_settled(context, build, "job entry write exhausted conflict retries").await
}

fn hash_report_value(hasher: &mut blake3::Hasher, value: &[u8]) {
    hasher.update(&(value.len() as u64).to_be_bytes());
    hasher.update(value);
}

#[cfg(test)]
mod tests;
