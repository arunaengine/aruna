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
pub mod key_wait;
pub mod list_jobs;
pub mod mutation;
pub mod query;
pub mod remote_wait;
pub mod state;

use crate::driver::drive;
use attempt::*;
use key_wait::*;
use mutation::{MutateJobOperation, NoGuard, settle_attempt};
use query::*;
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
    for attempt in 0..MUTATE_MAX_ATTEMPTS {
        let txn_id = start_write_txn(context)
            .await
            .map_err(JobMutationError::Storage)?;
        match Box::pin(run_delete_txn(context, txn_id, (user_id, job_id), recorded)).await {
            Ok(RunDelete::Deleted) => match commit_txn(context, txn_id).await {
                CommitResult::Committed => return Ok(RunDelete::Deleted),
                CommitResult::Conflict if attempt + 1 < MUTATE_MAX_ATTEMPTS => {
                    tokio::time::sleep(std::time::Duration::from_millis(1 << attempt.min(6))).await;
                }
                CommitResult::Conflict => break,
                CommitResult::Failed(error) => return Err(JobMutationError::Storage(error)),
            },
            outcome => {
                abort_txn(context, txn_id).await;
                return outcome;
            }
        }
    }
    Err(JobMutationError::Storage(
        "run delete exhausted conflict retries".to_string(),
    ))
}

async fn run_delete_txn(
    context: &DriverContext,
    txn_id: TxnId,
    (user_id, job_id): (UserId, JobId),
    recorded: bool,
) -> Result<RunDelete, JobMutationError> {
    let Some(record) = read_job_record(context, job_id, Some(txn_id))
        .await
        .map_err(JobMutationError::Storage)?
    else {
        return Ok(RunDelete::NotFound);
    };
    if record.created_by != user_id || record.payload.is_internal() {
        return Ok(RunDelete::NotFound);
    }
    if !matches!(record.payload, JobPayload::Execution(_)) {
        return hide_settled(context, txn_id, record).await;
    }
    if !record.state.is_terminal() {
        return Ok(RunDelete::Unfinished(record.state));
    }
    // A queued cleanup still needs this row to fence and remove the container.
    if read_job_record(context, cleanup_job_id(job_id), Some(txn_id))
        .await
        .map_err(JobMutationError::Storage)?
        .is_some_and(|cleanup| !cleanup.state.is_terminal())
    {
        return Ok(RunDelete::CleanupPending);
    }
    // A run became deletable after its eligibility was read, so no deletion was recorded yet.
    if !recorded {
        return Ok(RunDelete::Unrecorded(
            "the run settled while it was deleted; retry".to_string(),
        ));
    }
    let mut deletes = prune_delete_entries(&record);
    let mut start_after = None;
    loop {
        let (values, next) = iter_prefix_page(
            &context.storage_handle,
            JOB_ENTRY_KEYSPACE,
            Some(job_entry_prefix(job_id)),
            start_after.take(),
            JOB_PRUNE_PAGE,
            Some(txn_id),
        )
        .await
        .map_err(JobMutationError::Storage)?;
        deletes.extend(
            values
                .into_iter()
                .map(|(key, _)| (JOB_ENTRY_KEYSPACE.to_string(), key)),
        );
        match next {
            Some(next) => start_after = Some(next),
            None => break,
        }
    }
    batch_delete(context, deletes, Some(txn_id))
        .await
        .map_err(JobMutationError::Storage)?;
    Ok(RunDelete::Deleted)
}

/// Takes a settled job other than a run off this node's list and makes it due for pruning,
/// which removes its rows page by page, as staging rows can be too many for one transaction.
async fn hide_settled(
    context: &DriverContext,
    txn_id: TxnId,
    record: JobRecord,
) -> Result<RunDelete, JobMutationError> {
    if !record.is_settled() {
        return Ok(RunDelete::Unfinished(record.state));
    }
    let listed = owner_index_key(record.created_by, record.created_at_ms, record.job_id);
    let row = read_raw(context, JOB_INDEX_KEYSPACE, listed.clone(), Some(txn_id)).await;
    if row.map_err(JobMutationError::Storage)?.is_none() {
        return Ok(RunDelete::NotFound);
    }
    let mut hidden = record.clone();
    hidden.retention_ms = 0;
    let (writes, mut deletes) = index_deltas(&record, &hidden)
        .map_err(|error| JobMutationError::Storage(error.to_string()))?;
    deletes.push((JOB_INDEX_KEYSPACE.to_string(), listed));
    batch_write(context, writes, Some(txn_id))
        .await
        .map_err(JobMutationError::Storage)?;
    batch_delete(context, deletes, Some(txn_id))
        .await
        .map_err(JobMutationError::Storage)?;
    Ok(RunDelete::Deleted)
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
    for attempt in 0..MUTATE_MAX_ATTEMPTS {
        let txn_id = start_write_txn(context)
            .await
            .map_err(JobMutationError::Storage)?;
        let result = async {
            // The claim row, not the record a progress flush rewrites, fences the write.
            let key = job_record_key(job_id);
            let claim = read_raw(context, JOB_CLAIM_KEYSPACE, key, Some(txn_id))
                .await
                .map_err(JobMutationError::Storage)?
                .map(|value| ClaimGuard::from_bytes(value.as_ref()))
                .transpose()
                .map_err(|error| JobMutationError::Storage(error.to_string()))?;
            if claim.is_none_or(|claim| claim.claim_token != token) {
                return Err(JobMutationError::TokenMismatch);
            }
            batch_write(context, writes.clone(), Some(txn_id))
                .await
                .map_err(JobMutationError::Storage)?;
            batch_delete(context, deletes.clone(), Some(txn_id))
                .await
                .map_err(JobMutationError::Storage)
        }
        .await;
        match result {
            Ok(()) => match commit_write(
                context,
                txn_id,
                attempt,
                "checkpoint write exhausted conflict retries",
            )
            .await?
            {
                CommitStep::Committed => return Ok(()),
                CommitStep::Retry => continue,
            },
            Err(error) => {
                abort_txn(context, txn_id).await;
                return Err(error);
            }
        }
    }
    Err(JobMutationError::Storage(
        "checkpoint write exhausted conflict retries".to_string(),
    ))
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
    for attempt in 0..MUTATE_MAX_ATTEMPTS {
        let txn_id = start_write_txn(context)
            .await
            .map_err(JobMutationError::Storage)?;
        let result = async {
            let record = read_job_record(context, job_id, Some(txn_id))
                .await
                .map_err(JobMutationError::Storage)?
                .ok_or(JobMutationError::NotFound)?;
            if record.is_settled() {
                return Err(JobMutationError::ReportFrozen);
            }
            guard_token(&record, token)?;
            batch_write(
                context,
                vec![(
                    JOB_ENTRY_KEYSPACE.to_string(),
                    job_entry_key(job_id, entry_key),
                    value.clone(),
                )],
                Some(txn_id),
            )
            .await
            .map_err(JobMutationError::Storage)
        }
        .await;
        match result {
            Ok(()) => match commit_write(
                context,
                txn_id,
                attempt,
                "job entry write exhausted conflict retries",
            )
            .await?
            {
                CommitStep::Committed => return Ok(()),
                CommitStep::Retry => continue,
            },
            Err(error) => {
                abort_txn(context, txn_id).await;
                return Err(error);
            }
        }
    }
    Err(JobMutationError::Storage(
        "job entry write exhausted conflict retries".to_string(),
    ))
}

fn hash_report_value(hasher: &mut blake3::Hasher, value: &[u8]) {
    hasher.update(&(value.len() as u64).to_be_bytes());
    hasher.update(value);
}

#[cfg(test)]
mod tests;
