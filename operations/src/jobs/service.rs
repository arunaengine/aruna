use aruna_core::events::Event;
use aruna_core::handle::Handle;
use aruna_core::structs::{
    ExecutionSpec, JobId, JobPayload, JobRecord, JobState, RunCrateStatus, user_dedup_key,
};
use aruna_core::task::TaskEvent;
use aruna_core::types::{NodeId, UserId};
use aruna_core::util::unix_timestamp_millis;
use tracing::warn;

use super::runtime::JobsRuntime;
use super::store::{
    CancelRequestOutcome, JobMutationError, list_jobs_for_user, read_job_record,
    read_run_crate_status, set_cancel_requested,
};
use super::submit::{
    SubmitJobError, SubmitJobOperation, SubmitJobResult, SubmitJobSpec, schedule_job_drain_effect,
};
use super::workflow::finalize_followups;
use crate::driver::{DriverContext, drive};

/// Submit a container execution job on behalf of `created_by`. The drain claims it
/// and drives the fenced external attempt lifecycle. The idempotency key is
/// namespaced per user, disjoint from internal obligation keys.
pub async fn submit_execution_job(
    context: &DriverContext,
    spec: ExecutionSpec,
    created_by: UserId,
    owner_node_id: NodeId,
    idempotency_key: Option<String>,
) -> Result<SubmitJobResult, SubmitJobError> {
    let dedup_key = idempotency_key.map(|key| user_dedup_key(created_by, &key));
    drive(
        SubmitJobOperation::new(SubmitJobSpec {
            payload: JobPayload::Execution(spec),
            created_by,
            owner_node_id,
            dedup_key,
            now_ms: unix_timestamp_millis(),
        }),
        context,
    )
    .await
}

/// Read the run-crate obligation status surfaced alongside an execution job.
pub async fn read_job_run_crate_status(
    context: &DriverContext,
    job_id: JobId,
) -> Result<Option<RunCrateStatus>, String> {
    read_run_crate_status(&context.storage_handle, job_id).await
}

/// API-facing helpers so REST handlers never orchestrate storage/task effects directly.
pub async fn list_owned_jobs(
    context: &DriverContext,
    user_id: UserId,
    cursor: Option<Vec<u8>>,
    limit: usize,
    state_filter: Option<JobState>,
) -> Result<(Vec<JobRecord>, Option<Vec<u8>>), String> {
    list_jobs_for_user(
        &context.storage_handle,
        user_id,
        cursor,
        limit,
        state_filter,
    )
    .await
}

pub async fn read_owned_job(
    context: &DriverContext,
    user_id: UserId,
    job_id: JobId,
) -> Result<Option<JobRecord>, String> {
    Ok(
        match read_job_record(&context.storage_handle, job_id, None).await? {
            Some(record) if record.created_by == user_id => Some(record),
            _ => None,
        },
    )
}

pub enum CancelJobOutcome {
    NotFound,
    AlreadyTerminal(JobRecord),
    Requested(JobRecord),
}

pub async fn cancel_owned_job(
    context: &DriverContext,
    runtime: &JobsRuntime,
    user_id: UserId,
    job_id: JobId,
) -> Result<CancelJobOutcome, String> {
    if read_owned_job(context, user_id, job_id).await?.is_none() {
        return Ok(CancelJobOutcome::NotFound);
    }
    // The job may be pruned between the ownership read and here; treat that as a 404
    // rather than a 500.
    let outcome = match set_cancel_requested(
        &context.storage_handle,
        job_id,
        unix_timestamp_millis(),
    )
    .await
    {
        Ok(outcome) => outcome,
        Err(JobMutationError::NotFound) => return Ok(CancelJobOutcome::NotFound),
        Err(error) => return Err(error.to_string()),
    };
    Ok(match outcome {
        CancelRequestOutcome::AlreadyTerminal(record) => CancelJobOutcome::AlreadyTerminal(record),
        // Already terminalized in the store transaction: wake the durable run-crate child.
        CancelRequestOutcome::Cancelled(record) => {
            if matches!(&record.payload, JobPayload::Execution(_)) {
                finalize_followups(context, job_id).await;
            }
            CancelJobOutcome::Requested(record)
        }
        CancelRequestOutcome::Flagged(record) => {
            runtime.request_cancel(job_id);
            kick_drain(context).await;
            CancelJobOutcome::Requested(record)
        }
    })
}

async fn kick_drain(context: &DriverContext) {
    if let Some(task_handle) = context.task_handle.as_ref()
        && let Event::Task(TaskEvent::Error { message, .. }) =
            task_handle.send_effect(schedule_job_drain_effect()).await
    {
        warn!(message = %message, "Failed to kick job drain after cancel");
    }
}
