use aruna_core::events::Event;
use aruna_core::handle::Handle;
use aruna_core::structs::{
    ExecutionSpec, JobId, JobPayload, JobRecord, RunCrateStatus, user_dedup_key,
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
    filter: impl Fn(&JobRecord) -> bool,
) -> Result<(Vec<JobRecord>, Option<Vec<u8>>), String> {
    list_jobs_for_user(&context.storage_handle, user_id, cursor, limit, filter).await
}

pub async fn read_owned_job(
    context: &DriverContext,
    user_id: UserId,
    job_id: JobId,
) -> Result<Option<JobRecord>, String> {
    Ok(
        match read_job_record(&context.storage_handle, job_id, None).await? {
            Some(record)
                if record.created_by == user_id
                    && !matches!(&record.payload, JobPayload::WriteRunCrate { .. }) =>
            {
                Some(record)
            }
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

#[cfg(test)]
mod tests {
    use super::super::store::{insert_job, read_job_record};
    use super::*;
    use aruna_core::structs::{JobState, RealmId};
    use aruna_storage::FjallStorage;
    use aruna_tasks::TaskHandle;
    use tempfile::tempdir;
    use ulid::Ulid;

    fn node_id() -> NodeId {
        iroh::SecretKey::from_bytes(&[7u8; 32]).public()
    }

    #[tokio::test]
    async fn internal_access_hidden() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: Some(TaskHandle::new()),
            compute_handle: None,
        };
        let owner = UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32]));
        let job_id = JobId::from_bytes([0xC3; 16]);
        let record = JobRecord::new(
            job_id,
            JobPayload::WriteRunCrate {
                for_job: JobId::from_bytes([0xC4; 16]),
            },
            owner,
            node_id(),
            1_000,
            1_000,
            None,
        );
        insert_job(&storage, &record).await.unwrap();

        assert!(
            read_owned_job(&context, owner, job_id)
                .await
                .unwrap()
                .is_none()
        );
        let runtime = JobsRuntime::new();
        assert!(matches!(
            cancel_owned_job(&context, &runtime, owner, job_id)
                .await
                .unwrap(),
            CancelJobOutcome::NotFound
        ));
        let stored = read_job_record(&storage, job_id, None)
            .await
            .unwrap()
            .unwrap();
        assert!(!stored.cancel_requested);
        assert_eq!(stored.state, JobState::Queued);
    }
}
