//! Owns the job store reads and the guarded job mutation helpers over the shared job rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::driver::DriverContext;
use aruna_core::UserId;
use aruna_core::compute::{AttemptRef, FenceContext};
use aruna_core::errors::ConversionError;
use aruna_core::id::NodeId;
use aruna_core::keyspaces::PURGE_FENCE_KEYSPACE;
use aruna_core::keyspaces::{
    ARTIFACT_TOMBSTONE_KEYSPACE, ATTEMPT_CONTROL_KEYSPACE, DEDUP_INDEX_KEYSPACE,
    JOB_CLAIM_KEYSPACE, JOB_ENTRY_KEYSPACE, JOB_INDEX_KEYSPACE, JOB_KEYSPACE, KEY_WAIT_KEYSPACE,
    LAUNCH_RECORD_KEYSPACE, OUTPUT_RECORD_KEYSPACE, PURGE_CHECKPOINT_KEYSPACE, RUN_CRATE_KEYSPACE,
    SCHEDULE_INDEX_KEYSPACE,
};
use aruna_core::structs::execution::job::AttemptControl;
use aruna_core::structs::execution::job::AttemptIntent;
use aruna_core::structs::execution::job::JobClaim;
use aruna_core::structs::execution::job::JobError;
use aruna_core::structs::execution::job::JobErrorKind;
use aruna_core::structs::execution::job::JobExecutionClass;
use aruna_core::structs::execution::job::JobId;
use aruna_core::structs::execution::job::JobPayload;
use aruna_core::structs::execution::job::JobProgress;
use aruna_core::structs::execution::job::JobRecord;
use aruna_core::structs::execution::job::JobRecordEnvelope;
use aruna_core::structs::execution::job::JobResultPayload;
use aruna_core::structs::execution::job::JobState;
use aruna_core::structs::execution::job::LaunchRecord;
use aruna_core::structs::execution::job::RunCrateStatus;
use aruna_core::structs::execution::job::attempt_control_key;
use aruna_core::structs::execution::job::cleanup_dedup_key;
use aruna_core::structs::execution::job::cleanup_job_id;
use aruna_core::structs::execution::job::crate_dedup_key;
use aruna_core::structs::execution::job::crate_job_id;
use aruna_core::structs::execution::job::job_entry_key;
use aruna_core::structs::execution::job::job_entry_prefix;
use aruna_core::structs::execution::job::job_owner_cursor;
use aruna_core::structs::execution::job::job_prune_key;
use aruna_core::structs::execution::job::job_record_key;
use aruna_core::structs::execution::job::job_wait_key;
use aruna_core::structs::execution::job::owner_index_key;
use aruna_core::structs::execution::job::owner_index_prefix;
use aruna_core::structs::execution::job::parse_dedup_value;
use aruna_core::structs::execution::job::parse_entry_key;
use aruna_core::structs::execution::job::parse_owner_key;
use aruna_core::structs::execution::job::run_crate_key;
use aruna_core::structs::execution::job::validate_transition;
use aruna_core::structs::execution::job::workspace_credential_id;
use aruna_core::structs::execution::job::{
    ClaimGuard, JobDeletes, JobWrites, dedup_index_key, index_deltas, job_insert_entries,
    prune_delete_entries,
};
use aruna_core::structs::storage::blob::UserAccess;
use aruna_core::structs::storage::storage_purge::StoragePurgeCheckpoint;
use aruna_core::types::{Key, TxnId, Value};
use byteview::ByteView;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tracing::warn;
use ulid::Ulid;

use super::lifecycle::ids::session_of;
use super::{JOB_LEASE_MS, JOB_MAX_ATTEMPTS, JOB_PRUNE_PAGE, MUTATE_MAX_ATTEMPTS};
use crate::tasks::queue_backoff::retry_delay_ms;

pub mod attempt;
pub mod attempt_intent;
pub mod control_mutation;
pub mod job_calls;
pub mod job_rows;
pub mod key_wait;
pub mod list_jobs;
pub mod mark_start;
pub mod mutation;
pub mod mutation_error;
pub mod park_wake;
pub mod query;
pub mod remote_wait;
pub mod row_calls;
pub mod run_delete;
pub mod state;
pub mod store_call;

use crate::driver::drive;
use mutation::{MutateJobOperation, NoGuard};

use mutation_error::JobMutationError;

#[derive(Debug, Serialize, Deserialize)]
struct ArtifactTombstone {
    owner: UserId,
    expires_at_ms: u64,
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

/// Read and decode a state row, keeping a missing row distinct from a malformed one.
pub(super) async fn read_state<T: DeserializeOwned + Send + std::fmt::Debug + 'static>(
    context: &DriverContext,
    key_space: &str,
    key: Key,
    _label: &str,
) -> Result<Option<T>, String> {
    drive(row_calls::read_state(key_space, key), context)
        .await
        .map_err(|error| error.to_string())
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
    drive(
        job_calls::put_state(job_id, token, key_space, key, row),
        context,
    )
    .await
}

pub async fn put_purge_checkpoint(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    checkpoint: &StoragePurgeCheckpoint,
) -> Result<(), JobMutationError> {
    drive(
        job_calls::put_purge_checkpoint(job_id, token, checkpoint),
        context,
    )
    .await
}

pub async fn read_purge_checkpoint(
    context: &DriverContext,
    job_id: JobId,
) -> Result<Option<StoragePurgeCheckpoint>, JobMutationError> {
    drive(row_calls::read_purge_checkpoint(job_id), context).await
}

pub async fn put_job_entry<T: Serialize>(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    entry_key: &[u8],
    row: &T,
) -> Result<(), JobMutationError> {
    drive(
        job_calls::put_job_entry(job_id, token, entry_key, row),
        context,
    )
    .await
}

fn hash_report_value(hasher: &mut blake3::Hasher, value: &[u8]) {
    hasher.update(&(value.len() as u64).to_be_bytes());
    hasher.update(value);
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) mod test_calls;
#[cfg(test)]
mod tests;
