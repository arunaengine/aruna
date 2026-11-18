//! Reads and mutates the fenced attempt control row that guards one job execution.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub use super::job_calls::{AdoptOutcome, CancelRequestOutcome, ParkOutcome};
use super::*;
use super::{job_calls, row_calls};
use crate::driver::drive;
use aruna_core::structs::storage::distribution::hint::SetHint;
use std::collections::BTreeMap;

pub async fn read_attempt_control(
    context: &DriverContext,
    job_id: JobId,
    attempt_epoch: u64,
    _txn_id: Option<TxnId>,
) -> Result<Option<AttemptControl>, JobMutationError> {
    drive(
        row_calls::read_attempt_control(job_id, attempt_epoch),
        context,
    )
    .await
}

/// Write-ahead one VersionId per output destination in the attempt-control transaction that
/// fences this physical execution; it commits before any output write, so a replayed capture
/// reuses the reserved identities. `hints` maps distributed destinations to their set hints.
pub async fn reserve_output_commits(
    context: &DriverContext,
    job_id: JobId,
    destinations: &[(NodeId, String, String)],
    hints: &BTreeMap<(NodeId, String), SetHint>,
) -> Result<AttemptControl, JobMutationError> {
    drive(
        job_calls::reserve_output_commits(job_id, destinations.to_vec(), hints.clone()),
        context,
    )
    .await
}

pub async fn record_attempt_tombstone(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    attempt_epoch: u64,
    tombstone_ref: String,
) -> Result<AttemptControl, JobMutationError> {
    drive(
        job_calls::record_attempt_tombstone(job_id, token, attempt_epoch, tombstone_ref),
        context,
    )
    .await
}

/// Advance a claimed execution job to `Preparing`, renewing the lease. The
/// workspace is built and inputs staged in this phase.
pub async fn transition_to_preparing(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    now_ms: u64,
) -> Result<JobRecord, JobMutationError> {
    drive(
        job_calls::transition_to_preparing(job_id, token, now_ms),
        context,
    )
    .await
}

/// `Preparing -> Ready`: inputs staged and credentials minted.
pub async fn transition_to_ready(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    now_ms: u64,
) -> Result<JobRecord, JobMutationError> {
    drive(
        job_calls::transition_to_ready(job_id, token, now_ms),
        context,
    )
    .await
}

/// Commit `Running` for an external attempt: from `Ready` after the backend
/// accepts the fenced attempt, or from `Indeterminate` when reconcile re-adopts a
/// still-running container.
pub async fn begin_external_running(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    started_at_ms: Option<u64>,
    now_ms: u64,
) -> Result<JobRecord, JobMutationError> {
    drive(
        job_calls::begin_external_running(job_id, token, started_at_ms, now_ms),
        context,
    )
    .await
}

/// Persist backend start evidence without changing execution state or timestamps.
pub async fn record_attempt_started(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    started_at_ms: u64,
) -> Result<JobRecord, JobMutationError> {
    drive(
        job_calls::record_attempt_started(job_id, token, started_at_ms),
        context,
    )
    .await
}

/// The executed launch record of the record's final attempt, if any.
pub async fn read_launch_record(
    context: &DriverContext,
    record: &JobRecord,
) -> Result<Option<LaunchRecord>, String> {
    drive(row_calls::read_launch_record(record), context)
        .await
        .map_err(|error| error.to_string())
}

/// `Running/Indeterminate -> Cancelling`: a durable cancel intent precedes the
/// backend stop so no evidence is lost across a crash.
pub async fn transition_to_cancelling(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    now_ms: u64,
) -> Result<JobRecord, JobMutationError> {
    drive(
        job_calls::transition_to_cancelling(job_id, token, now_ms),
        context,
    )
    .await
}

/// Take over a lost external attempt with a fresh claim token so the reconciler
/// can supervise it. No token guard: the previous holder is provably dead (lease
/// swept or node restarted). State is preserved for the reconcile decision.
pub async fn adopt_external_attempt(
    context: &DriverContext,
    job_id: JobId,
    holder_node_id: NodeId,
    now_ms: u64,
) -> Result<AdoptOutcome, JobMutationError> {
    drive(
        job_calls::adopt_external_attempt(job_id, holder_node_id, now_ms),
        context,
    )
    .await
}

/// Persist the run-crate obligation status in its side keyspace, leaving the
/// terminal parent record untouched.
pub async fn put_crate_status(
    context: &DriverContext,
    job_id: JobId,
    status: &aruna_core::structs::execution::job::RunCrateStatus,
) -> Result<(), String> {
    drive(row_calls::put_crate_status(job_id, status), context)
        .await
        .map_err(|error| error.to_string())
}

/// Records the crate outcome and the run link in one batch, so a run whose crate status is
/// final always has its link.
pub async fn put_crate_outcome(
    context: &DriverContext,
    job_id: JobId,
    status: &aruna_core::structs::execution::job::RunCrateStatus,
    link: &aruna_core::structs::execution::job::RunLink,
) -> Result<(), String> {
    drive(row_calls::put_crate_outcome(job_id, status, link), context)
        .await
        .map_err(|error| error.to_string())
}

/// Stores the crate of an unpublished local run with its `Local` status in one batch.
pub async fn put_local_crate(
    context: &DriverContext,
    job_id: JobId,
    local: &aruna_core::structs::execution::job::LocalCrate,
) -> Result<(), String> {
    drive(row_calls::put_local_crate(job_id, local), context)
        .await
        .map_err(|error| error.to_string())
}

/// The crate of a local run and its owner, while this device keeps one.
pub async fn read_local_crate(
    context: &DriverContext,
    job_id: JobId,
) -> Result<Option<aruna_core::structs::execution::job::LocalCrate>, String> {
    drive(row_calls::read_local_crate(job_id), context)
        .await
        .map_err(|error| error.to_string())
}

/// Read the run link of a physical job id, if recorded.
pub async fn read_run_link(
    context: &DriverContext,
    job_id: JobId,
) -> Result<Option<aruna_core::structs::execution::job::RunLink>, String> {
    drive(row_calls::read_run_link(job_id), context)
        .await
        .map_err(|error| error.to_string())
}

/// Read the run-crate obligation status for `job_id`, if recorded.
pub async fn read_crate_status(
    context: &DriverContext,
    job_id: JobId,
) -> Result<Option<aruna_core::structs::execution::job::RunCrateStatus>, String> {
    drive(row_calls::read_crate_status(job_id), context)
        .await
        .map_err(|error| error.to_string())
}

/// See [`job_calls::set_cancel_requested`].
pub async fn set_cancel_requested(
    context: &DriverContext,
    job_id: JobId,
    now_ms: u64,
) -> Result<CancelRequestOutcome, JobMutationError> {
    drive(job_calls::set_cancel_requested(job_id, now_ms), context).await
}
