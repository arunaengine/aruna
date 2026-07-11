use aruna_core::events::Event;
use aruna_core::handle::Handle;
use aruna_core::structs::{JobId, JobRecord, JobState};
use aruna_core::task::TaskEvent;
use aruna_core::types::UserId;
use aruna_core::util::unix_timestamp_millis;
use tracing::warn;

use super::runtime::JobsRuntime;
use super::store::{
    CancelRequestOutcome, list_jobs_for_user, read_job_record, set_cancel_requested,
};
use super::submit::schedule_job_drain_effect;
use crate::driver::DriverContext;

/// API-facing helpers so REST handlers never orchestrate storage or task effects
/// directly (enforced by the api effect-boundary guard).
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
    let outcome = set_cancel_requested(&context.storage_handle, job_id, unix_timestamp_millis())
        .await
        .map_err(|error| error.to_string())?;
    Ok(match outcome {
        CancelRequestOutcome::AlreadyTerminal(record) => CancelJobOutcome::AlreadyTerminal(record),
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
