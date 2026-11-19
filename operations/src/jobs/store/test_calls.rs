//! Async forms of the job store calls, for tests that drive one call at a time.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::attempt_intent::AttemptCommit;
use super::job_calls::AdoptOutcome;
use super::job_calls::CancelRequestOutcome;
use super::job_calls::ClaimOutcome;
use super::job_calls::ExecutionCompleteOutcome;
use super::job_calls::ParkOutcome;
use super::job_calls::ReleaseOutcome;
use super::job_calls::RenewOutcome;
use super::job_calls::RequeueOutcome;
use super::job_calls::RunSuccess;
use super::list_jobs::ListJobsOperation;
use super::mutation::{MutateJobOperation, NoGuard};
use super::park_wake::AwaitOutcome;
use super::*;
use super::{job_calls, row_calls};
use crate::driver::DriverContext;
use crate::driver::drive;
use aruna_core::structs::execution::job::KeyWait;
use aruna_core::structs::storage::distribution::hint::SetHint;
use aruna_core::structs::storage::encryption::BucketKeyRef;
use aruna_core::types::TxnId;
use std::collections::BTreeMap;

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

pub async fn cancel_running_job(
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

pub async fn claim_job(
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

pub async fn complete_execution(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    result: JobResultPayload,
    final_progress: JobProgress,
    now_ms: u64,
) -> Result<ExecutionCompleteOutcome, JobMutationError> {
    drive(
        job_calls::complete_execution(
            job_id,
            token,
            RunSuccess {
                result,
                progress: final_progress,
                now_ms,
            },
        ),
        context,
    )
    .await
}

pub async fn complete_job(
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

pub async fn defer_job(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    now_ms: u64,
    retry_after_ms: u64,
    error: JobError,
) -> Result<ReleaseOutcome, JobMutationError> {
    drive(
        job_calls::defer_job(job_id, token, now_ms, retry_after_ms, error),
        context,
    )
    .await
}

pub async fn delete_finished_run(
    context: &DriverContext,
    user_id: UserId,
    job_id: JobId,
    recorded: bool,
) -> Result<RunDelete, JobMutationError> {
    drive(
        job_calls::delete_finished_run(user_id, job_id, recorded),
        context,
    )
    .await
}

pub async fn exhaust_job(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    error: JobError,
    now_ms: u64,
) -> Result<JobRecord, JobMutationError> {
    drive(
        job_calls::exhaust_job(job_id, token, error, now_ms),
        context,
    )
    .await
}

pub async fn fail_job(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    error: JobError,
    now_ms: u64,
) -> Result<JobRecord, JobMutationError> {
    drive(job_calls::fail_job(job_id, token, error, now_ms), context).await
}

pub async fn find_dedup_job(
    context: &DriverContext,
    created_by: UserId,
    dedup_key: &[u8],
    _txn_id: Option<TxnId>,
) -> Result<Option<JobId>, String> {
    drive(row_calls::find_dedup_job(created_by, dedup_key), context)
        .await
        .map_err(|error| error.to_string())
}

pub async fn find_dedup_plan(
    context: &DriverContext,
    created_by: UserId,
    dedup_key: &[u8],
    _txn_id: Option<TxnId>,
) -> Result<Option<(JobId, [u8; 32])>, String> {
    drive(row_calls::find_dedup_plan(created_by, dedup_key), context)
        .await
        .map_err(|error| error.to_string())
}

pub async fn flush_progress(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    progress: JobProgress,
    now_ms: u64,
) -> Result<RenewOutcome, JobMutationError> {
    drive(
        job_calls::flush_progress(job_id, token, progress, now_ms),
        context,
    )
    .await
}

pub async fn handoff_external_attempt(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    now_ms: u64,
) -> Result<ReleaseOutcome, JobMutationError> {
    drive(
        job_calls::handoff_external_attempt(job_id, token, now_ms),
        context,
    )
    .await
}

pub async fn insert_job(context: &DriverContext, record: &JobRecord) -> Result<(), String> {
    drive(row_calls::insert_job(record), context)
        .await
        .map_err(|error| error.to_string())
}

pub async fn list_job_entries(
    context: &DriverContext,
    job_id: JobId,
    last_key: Option<Vec<u8>>,
    limit: usize,
) -> Result<(Vec<(Vec<u8>, Value)>, Option<Vec<u8>>), String> {
    drive(
        row_calls::list_job_entries(job_id, last_key, limit),
        context,
    )
    .await
    .map_err(|error| error.to_string())
}

pub async fn list_user_jobs(
    context: &DriverContext,
    user_id: UserId,
    cursor: Option<Vec<u8>>,
    limit: usize,
    filter: impl Fn(&JobRecord) -> bool + Send,
) -> Result<(Vec<JobRecord>, Option<Vec<u8>>), String> {
    drive(
        ListJobsOperation::new(user_id, cursor, limit, filter),
        context,
    )
    .await
    .map_err(|error| error.to_string())
}

pub async fn mark_indeterminate(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    error: JobError,
    now_ms: u64,
) -> Result<ParkOutcome, JobMutationError> {
    drive(
        job_calls::mark_indeterminate(job_id, token, error, now_ms),
        context,
    )
    .await
}

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

pub async fn park_job(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    now_ms: u64,
    waits: Vec<KeyWait>,
) -> Result<AwaitOutcome, JobMutationError> {
    drive(row_calls::park_job(job_id, token, now_ms, waits), context).await
}

pub async fn persist_output_record(
    context: &DriverContext,
    job_id: JobId,
    control: &AttemptControl,
    digest: [u8; 32],
    envelope: Vec<u8>,
) -> Result<(), JobMutationError> {
    drive(
        job_calls::persist_output_record(job_id, control, digest, envelope),
        context,
    )
    .await
}

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

pub async fn put_crate_status(
    context: &DriverContext,
    job_id: JobId,
    status: &aruna_core::structs::execution::job::RunCrateStatus,
) -> Result<(), String> {
    drive(row_calls::put_crate_status(job_id, status), context)
        .await
        .map_err(|error| error.to_string())
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

pub async fn put_state<T: Serialize>(
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

pub async fn read_crate_status(
    context: &DriverContext,
    job_id: JobId,
) -> Result<Option<aruna_core::structs::execution::job::RunCrateStatus>, String> {
    drive(row_calls::read_crate_status(job_id), context)
        .await
        .map_err(|error| error.to_string())
}

pub async fn read_job_record(
    context: &DriverContext,
    job_id: JobId,
    _txn_id: Option<TxnId>,
) -> Result<Option<JobRecord>, String> {
    drive(row_calls::read_job_record(job_id), context)
        .await
        .map_err(|error| error.to_string())
}

pub async fn read_key_waits(
    context: &DriverContext,
    job_id: JobId,
) -> Result<Vec<KeyWait>, String> {
    drive(row_calls::read_key_waits(job_id), context)
        .await
        .map_err(|error| error.to_string())
}

pub async fn read_launch_record(
    context: &DriverContext,
    record: &JobRecord,
) -> Result<Option<LaunchRecord>, String> {
    drive(row_calls::read_launch_record(record), context)
        .await
        .map_err(|error| error.to_string())
}

pub async fn read_output_record(
    context: &DriverContext,
    job_id: JobId,
    attempt_epoch: u64,
) -> Result<Option<JobRecordEnvelope>, JobMutationError> {
    drive(
        row_calls::read_output_record(job_id, attempt_epoch),
        context,
    )
    .await
}

pub async fn read_run_link(
    context: &DriverContext,
    job_id: JobId,
) -> Result<Option<aruna_core::structs::execution::job::RunLink>, String> {
    drive(row_calls::read_run_link(job_id), context)
        .await
        .map_err(|error| error.to_string())
}

pub async fn read_state<T: DeserializeOwned + Send + std::fmt::Debug + 'static>(
    context: &DriverContext,
    key_space: &str,
    key: Key,
    _label: &str,
) -> Result<Option<T>, String> {
    drive(row_calls::read_state(key_space, key), context)
        .await
        .map_err(|error| error.to_string())
}

pub async fn record_attempt_intent(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    draft: AttemptIntent,
    execution_id: Option<Ulid>,
    launch: Option<&LaunchRecord>,
    now_ms: u64,
) -> Result<AttemptCommit, JobMutationError> {
    drive(
        job_calls::record_attempt_intent(
            job_id,
            token,
            draft,
            execution_id,
            launch.cloned(),
            now_ms,
        ),
        context,
    )
    .await
}

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

pub async fn release_job(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    now_ms: u64,
) -> Result<ReleaseOutcome, JobMutationError> {
    drive(job_calls::release_job(job_id, token, now_ms), context).await
}

pub async fn renew_lease(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    now_ms: u64,
    progress: Option<JobProgress>,
) -> Result<RenewOutcome, JobMutationError> {
    drive(
        job_calls::renew_lease(job_id, token, now_ms, progress),
        context,
    )
    .await
}

pub async fn requeue_before_attempt(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    now_ms: u64,
    error: JobError,
) -> Result<JobRecord, JobMutationError> {
    drive(
        job_calls::requeue_before_attempt(job_id, token, now_ms, error),
        context,
    )
    .await
}

pub async fn requeue_job(
    context: &DriverContext,
    job_id: JobId,
    token: Option<Ulid>,
    now_ms: u64,
    require_expired_before: Option<u64>,
    error: Option<JobError>,
) -> Result<RequeueOutcome, JobMutationError> {
    drive(
        job_calls::requeue_job(job_id, token, now_ms, require_expired_before, error),
        context,
    )
    .await
}

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

pub async fn set_cancel_requested(
    context: &DriverContext,
    job_id: JobId,
    now_ms: u64,
) -> Result<CancelRequestOutcome, JobMutationError> {
    drive(job_calls::set_cancel_requested(job_id, now_ms), context).await
}

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

pub async fn transition_to_running(
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

pub async fn wake_key_page(
    context: &DriverContext,
    key: BucketKeyRef,
    now_ms: u64,
) -> Result<(usize, bool), JobMutationError> {
    drive(row_calls::wake_key_page(key, now_ms), context).await
}

pub async fn wake_key_waits(
    context: &DriverContext,
    key: BucketKeyRef,
    now_ms: u64,
) -> Result<usize, JobMutationError> {
    drive(row_calls::wake_key_waits(key, now_ms), context).await
}

pub async fn settle_attempt<T>(
    result: Result<T, JobMutationError>,
    attempt: u32,
    exhausted: &str,
) -> Option<Result<T, JobMutationError>> {
    match result {
        Err(JobMutationError::Conflict) if attempt + 1 < MUTATE_MAX_ATTEMPTS => {
            // Lets the competing writer finish before the retry, without real time.
            tokio::task::yield_now().await;
            None
        }
        Err(JobMutationError::Conflict) => {
            Some(Err(JobMutationError::Storage(exhausted.to_string())))
        }
        other => Some(other),
    }
}
