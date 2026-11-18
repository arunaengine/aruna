//! Runs the job store operations for the route tests.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::id::NodeId;
use aruna_core::structs::execution::job::JobId;
use aruna_core::structs::execution::job::JobProgress;
use aruna_core::structs::execution::job::JobRecord;
use aruna_core::structs::execution::job::JobResultPayload;
use aruna_core::types::TxnId;
use aruna_operations::driver::DriverContext;
use aruna_operations::driver::drive;
use aruna_operations::jobs::store::JobMutation;
use aruna_operations::jobs::store::JobMutationError;
use aruna_operations::jobs::store::job_calls;
use aruna_operations::jobs::store::job_calls::ClaimOutcome;
use aruna_operations::jobs::store::row_calls;
use serde::Serialize;
use ulid::Ulid;

pub(crate) async fn cancel_running_job(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    now_ms: u64,
) -> Result<JobRecord, JobMutationError> {
    drive(
        job_calls::cancel_running_job(job_id, token, now_ms),
        context,
    )
    .await
}

pub(crate) async fn claim_job(
    context: &DriverContext,
    job_id: JobId,
    holder_node_id: NodeId,
    now_ms: u64,
) -> Result<ClaimOutcome, JobMutationError> {
    drive(
        job_calls::claim_job(job_id, holder_node_id, now_ms),
        context,
    )
    .await
}

pub(crate) async fn complete_job(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    result: JobResultPayload,
    final_progress: JobProgress,
    now_ms: u64,
) -> Result<JobRecord, JobMutationError> {
    drive(
        job_calls::complete_job(job_id, token, result, final_progress, now_ms),
        context,
    )
    .await
}

pub(crate) async fn insert_job(context: &DriverContext, record: &JobRecord) -> Result<(), String> {
    drive(row_calls::insert_job(record), context)
        .await
        .map_err(|error| error.to_string())
}

pub(crate) async fn mutate_job<F>(
    context: &DriverContext,
    job_id: JobId,
    mutate: F,
) -> Result<JobRecord, JobMutationError>
where
    F: FnMut(&mut JobRecord) -> Result<JobMutation, JobMutationError> + Clone + Send + 'static,
{
    drive(job_calls::change_job(job_id, mutate), context).await
}

pub(crate) async fn put_crate_outcome(
    context: &DriverContext,
    job_id: JobId,
    status: &aruna_core::structs::execution::job::RunCrateStatus,
    link: &aruna_core::structs::execution::job::RunLink,
) -> Result<(), String> {
    drive(row_calls::put_crate_outcome(job_id, status, link), context)
        .await
        .map_err(|error| error.to_string())
}

pub(crate) async fn put_job_entry<T: Serialize>(
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

pub(crate) async fn read_job_record(
    context: &DriverContext,
    job_id: JobId,
    _txn_id: Option<TxnId>,
) -> Result<Option<JobRecord>, String> {
    drive(row_calls::read_job_record(job_id), context)
        .await
        .map_err(|error| error.to_string())
}

pub(crate) async fn transition_to_running(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    now_ms: u64,
) -> Result<JobRecord, JobMutationError> {
    drive(
        job_calls::transition_to_running(job_id, token, now_ms),
        context,
    )
    .await
}
