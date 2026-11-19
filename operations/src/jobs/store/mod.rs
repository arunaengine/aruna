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
use aruna_core::types::{Key, Value};
use byteview::ByteView;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tracing::warn;
use ulid::Ulid;

use super::lifecycle::ids::session_of;
use super::{JOB_LEASE_MS, JOB_MAX_ATTEMPTS, JOB_PRUNE_PAGE, MUTATE_MAX_ATTEMPTS};
use crate::queue_backoff::retry_delay_ms;

pub mod attempt_intent;
pub mod control_mutation;
pub mod job_calls;
pub mod job_rows;
pub mod list_jobs;
pub mod mark_start;
pub mod mutation;
pub mod mutation_error;
pub mod park_wake;
pub mod remote_wait;
pub mod row_calls;
pub mod run_delete;
pub mod store_call;

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
mod key_wait_tests;
#[cfg(any(test, feature = "test-hooks"))]
pub mod test_calls;
#[cfg(test)]
mod tests;
