//! Owns the job store reads and the guarded job mutation helpers over the shared job rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

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
use aruna_core::types::{Key, Value};
use byteview::ByteView;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use thiserror::Error;
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
pub mod park_wake;
pub mod remote_wait;
pub mod row_calls;
pub mod run_delete;
pub mod state;
pub mod store_call;

use mutation::{MutateJobOperation, NoGuard};

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

fn hash_report_value(hasher: &mut blake3::Hasher, value: &[u8]) {
    hasher.update(&(value.len() as u64).to_be_bytes());
    hasher.update(value);
}

#[cfg(test)]
pub(crate) mod test_calls;
#[cfg(test)]
mod tests;
