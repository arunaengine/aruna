use std::sync::{Arc, Weak};

use aruna_core::compute::{
    AttemptPhase, AttemptRef, ExecutorKind, FenceContext, ReconcileEvidence,
};
use aruna_core::structs::{ExecutionSpec, JobError, JobErrorKind, JobPayload, JobRecord, JobState};
use aruna_core::util::unix_timestamp_millis;
use aruna_storage::StorageHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::super::reconcile::ExternalReconciler;
use super::super::runtime::JobsRuntime;
use super::super::store::{
    AdoptOutcome, adopt_external_attempt, handoff_external_attempt, read_job_record,
    record_attempt_started, record_attempt_tombstone, release_job, transition_external_to_running,
};
use super::{
    DEFAULT_WALLTIME, build_task_spec, fail_and_crate, finalize_attempt, finalize_cancel,
    job_bucket, park_attempt, reload_task, requeue_after_tombstone, supervise_and_finalize,
    with_execution_heartbeat,
};
use crate::driver::DriverContext;

/// The real Stage-0 reconcile seam: a lost external attempt is adopted by name and
/// resolved by evidence (adopt-and-resume, terminalize, or Indeterminate). It never
/// submits a new attempt name, so a container is never double-run.
pub struct ComputeReconciler {
    context: Arc<DriverContext>,
    runtime: Weak<JobsRuntime>,
}

impl ComputeReconciler {
    pub fn new(context: Arc<DriverContext>, runtime: Weak<JobsRuntime>) -> Arc<Self> {
        Arc::new(Self { context, runtime })
    }
}

#[async_trait::async_trait]
impl ExternalReconciler for ComputeReconciler {
    async fn reconcile_lost_attempt(&self, storage: &StorageHandle, record: JobRecord) {
        let job_id = record.job_id;
        let JobPayload::Execution(mut spec) = record.payload.clone() else {
            return;
        };

        // Take over with a fresh claim token, but only if the old holder is really gone.
        let (adopted, control) = match adopt_external_attempt(
            storage,
            job_id,
            holder(&self.context),
            unix_timestamp_millis(),
        )
        .await
        {
            Ok(AdoptOutcome::Adopted(record, control)) => (record, control),
            Ok(AdoptOutcome::Skipped) => {
                info!(job_id = %job_id, "Attempt is terminal or still held; not adopting");
                return;
            }
            Err(error) => {
                warn!(job_id = %job_id, error = %error, "Adoption failed");
                return;
            }
        };
        let Some(token) = adopted.claim.as_ref().map(|claim| claim.claim_token) else {
            return;
        };

        // No durable attempt intent means the attempt was never submitted; release
        // the adopted lease without charging an attempt.
        let Some(intent) = adopted.attempt_intent.clone() else {
            let _ = release_job(storage, job_id, token, unix_timestamp_millis()).await;
            return;
        };

        // Query the executor persisted in the intent: registry-first selection
        // could ask the wrong backend and park a live attempt as absent.
        let kind = ExecutorKind::from_wire(&intent.executor_kind);
        let Some(backend) = self
            .context
            .compute_handle
            .as_ref()
            .and_then(|registry| registry.get(&kind))
            .cloned()
        else {
            // A node without this backend cannot observe the attempt: charging here
            // would terminalize a healthy container it can never see. Hand it back
            // with an expired lease so a node that has the backend can reconcile it.
            warn!(job_id = %job_id, kind = %intent.executor_kind, "Reconcile backend unavailable; handing back");
            if let Err(error) =
                handoff_external_attempt(storage, job_id, token, unix_timestamp_millis()).await
            {
                warn!(job_id = %job_id, error = %error, "Failed to hand back an unreconciled attempt");
            }
            return;
        };

        let bucket = job_bucket(&adopted);
        if !bucket.is_empty() {
            spec.resolve_outputs(&bucket);
        }
        let attempt = AttemptRef::new(job_id.to_string().to_lowercase(), intent.attempt_no);
        let fence = FenceContext {
            attempt: attempt.clone(),
            attempt_epoch: intent.attempt_epoch,
            controller_generation: control.controller_generation,
        };
        // Every unresolved exit below parks: the sweep that routed us here spends no
        // attempt, so the park is what bounds a condition that never resolves.
        if let Err(error) = backend.fence(&fence).await {
            warn!(job_id = %job_id, error = %error, "Backend fence failed");
            park_attempt(
                &self.context,
                job_id,
                token,
                JobError::retryable(format!("reconcile fence failed: {error}")),
            )
            .await;
            return;
        }

        if adopted.cancel_requested {
            let _ = with_execution_heartbeat(
                storage.clone(),
                job_id,
                token,
                CancellationToken::new(),
                Box::pin(finalize_cancel(
                    &self.context,
                    job_id,
                    token,
                    &backend,
                    &fence,
                    &spec,
                    &bucket,
                )),
            )
            .await;
            return;
        }

        let outcome = backend.reconcile(&fence).await;
        let started_at_ms = match &outcome {
            ReconcileEvidence::Adoptable(evidence) => evidence.status.started_at_ms,
            _ => None,
        };
        if let Some(started_at_ms) = started_at_ms
            && let Err(error) = record_attempt_started(storage, job_id, token, started_at_ms).await
        {
            warn!(job_id = %job_id, error = %error, "Attempt start evidence write failed during adoption");
            park_attempt(
                &self.context,
                job_id,
                token,
                JobError::retryable(format!("start evidence write failed: {error}")),
            )
            .await;
            return;
        }
        if matches!(
            &outcome,
            ReconcileEvidence::Adoptable(evidence)
                if !matches!(evidence.status.phase, AttemptPhase::Submitted)
        ) && matches!(adopted.state, JobState::Indeterminate | JobState::Ready)
        {
            let running = match transition_external_to_running(
                storage,
                job_id,
                token,
                started_at_ms,
                unix_timestamp_millis(),
            )
            .await
            {
                Ok(record) => record,
                Err(error) => {
                    park_attempt(
                        &self.context,
                        job_id,
                        token,
                        JobError::retryable(format!("adopted attempt cannot run: {error}")),
                    )
                    .await;
                    return;
                }
            };
            if running.cancel_requested {
                let _ = with_execution_heartbeat(
                    storage.clone(),
                    job_id,
                    token,
                    CancellationToken::new(),
                    Box::pin(finalize_cancel(
                        &self.context,
                        job_id,
                        token,
                        &backend,
                        &fence,
                        &spec,
                        &bucket,
                    )),
                )
                .await;
                return;
            }
        }

        match outcome {
            ReconcileEvidence::Adoptable(evidence) if !evidence.status.is_terminal() => {
                self.resume(
                    job_id,
                    token,
                    backend,
                    fence,
                    evidence.status.phase,
                    spec,
                    bucket,
                    adopted,
                )
                .await;
            }
            // Terminal evidence: commit it now (correlates the cancel intent).
            ReconcileEvidence::Adoptable(evidence) => {
                let mut status = evidence.status;
                let max_walltime_ms = spec
                    .resources
                    .max_walltime_ms
                    .unwrap_or(DEFAULT_WALLTIME.as_millis() as u64);
                if status.started_at_ms.zip(status.finished_at_ms).is_some_and(
                    |(started, finished)| finished.saturating_sub(started) > max_walltime_ms,
                ) {
                    status.phase = AttemptPhase::Failed {
                        reason: "walltime limit exceeded while unobserved".to_string(),
                    };
                }
                let finalized = with_execution_heartbeat(
                    storage.clone(),
                    job_id,
                    token,
                    CancellationToken::new(),
                    Box::pin(finalize_attempt(
                        &self.context,
                        job_id,
                        token,
                        &backend,
                        &fence,
                        &spec,
                        &bucket,
                        Ok(status),
                    )),
                )
                .await;
                if finalized.is_none() {
                    info!(job_id = %job_id, "Reconcile finalizer superseded; abandoning");
                }
            }
            // Post-submit absence retries the same attempt name.
            ReconcileEvidence::Absent => {
                self.resume(
                    job_id,
                    token,
                    backend,
                    fence,
                    AttemptPhase::Submitted,
                    spec,
                    bucket,
                    adopted,
                )
                .await;
            }
            ReconcileEvidence::Unavailable(error) => {
                park_attempt(
                    &self.context,
                    job_id,
                    token,
                    JobError::retryable(format!("backend unavailable: {error}")),
                )
                .await;
            }
            ReconcileEvidence::Unadoptable(artifact) => {
                if artifact.exact_identity {
                    let _ = backend.cancel(&fence).await;
                }
                park_attempt(
                    &self.context,
                    job_id,
                    token,
                    JobError::retryable("backend artifact is not adoptable"),
                )
                .await;
            }
            ReconcileEvidence::Tombstoned(tombstone) => {
                if record_attempt_tombstone(
                    storage,
                    job_id,
                    token,
                    fence.attempt_epoch,
                    tombstone.backend_ref,
                )
                .await
                .is_ok()
                {
                    requeue_after_tombstone(
                        &self.context,
                        job_id,
                        token,
                        &adopted,
                        &aruna_core::compute::BackendError::Unavailable(
                            "attempt tombstoned".to_string(),
                        ),
                    )
                    .await;
                }
            }
        }
    }
}

impl ComputeReconciler {
    /// Adopt a live container without blocking the drain loop, registering its
    /// supervisor with `JobsRuntime` for caps, cancellation, and shutdown.
    #[allow(clippy::too_many_arguments)]
    async fn resume(
        &self,
        job_id: aruna_core::structs::JobId,
        token: ulid::Ulid,
        backend: Arc<dyn aruna_compute::ExecutorBackend>,
        fence: FenceContext,
        phase: AttemptPhase,
        spec: ExecutionSpec,
        bucket: String,
        record: JobRecord,
    ) {
        let context = self.context.clone();
        let supervisor_record = record.clone();
        let supervisor = move |cancel| {
            resume_attempt(
                context,
                job_id,
                token,
                backend,
                fence,
                phase,
                spec,
                bucket,
                supervisor_record,
                cancel,
            )
        };
        if !self.runtime.upgrade().is_some_and(|runtime| {
            runtime.spawn_recovered(self.context.clone(), &record, supervisor)
        }) && let Err(error) = handoff_external_attempt(
            &self.context.storage_handle,
            job_id,
            token,
            unix_timestamp_millis(),
        )
        .await
        {
            warn!(job_id = %job_id, error = %error, "Failed to hand off unregistered recovered attempt");
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn resume_attempt(
    context: Arc<DriverContext>,
    job_id: aruna_core::structs::JobId,
    token: ulid::Ulid,
    backend: Arc<dyn aruna_compute::ExecutorBackend>,
    fence: FenceContext,
    phase: AttemptPhase,
    spec: ExecutionSpec,
    bucket: String,
    record: JobRecord,
    cancel: CancellationToken,
) {
    let current = match read_job_record(&context.storage_handle, job_id, None).await {
        Ok(Some(record)) => record,
        Ok(None) => return,
        Err(error) => {
            warn!(job_id = %job_id, error = %error, "Recovered job lookup failed");
            return;
        }
    };
    if current.cancel_requested {
        let _ = with_execution_heartbeat(
            context.storage_handle.clone(),
            job_id,
            token,
            cancel,
            Box::pin(finalize_cancel(
                &context, job_id, token, &backend, &fence, &spec, &bucket,
            )),
        )
        .await;
        return;
    }
    if matches!(phase, AttemptPhase::Submitted) {
        let resumed = with_execution_heartbeat(
            context.storage_handle.clone(),
            job_id,
            token,
            cancel.clone(),
            Box::pin(async {
                let Some(node_id) = context.net_handle.as_ref().map(|net| net.node_id()) else {
                    Box::pin(fail_or_park(
                        &context,
                        job_id,
                        token,
                        &record,
                        JobError::permanent("execution needs a net handle"),
                    ))
                    .await;
                    return false;
                };
                let prepared =
                    match Box::pin(reload_task(&context, &spec, &record, node_id, &bucket)).await {
                        Ok(prepared) => prepared,
                        Err(error) => {
                            Box::pin(fail_or_park(&context, job_id, token, &record, error)).await;
                            return false;
                        }
                    };
                let Some(pinned_image) = record
                    .attempt_intent
                    .as_ref()
                    .filter(|intent| intent.attempt_epoch == fence.attempt_epoch)
                    .map(|intent| intent.pinned_image.as_str())
                else {
                    Box::pin(park_attempt(
                        &context,
                        job_id,
                        token,
                        JobError::retryable("attempt intent mismatch"),
                    ))
                    .await;
                    return false;
                };
                let task_spec = build_task_spec(
                    &spec,
                    &fence.attempt,
                    pinned_image,
                    prepared,
                    backend.run_identity(),
                );
                let status = match backend.submit(&fence, &task_spec, &cancel).await {
                    Ok(status) => status,
                    Err(aruna_core::compute::BackendError::Cancelled) => {
                        Box::pin(finalize_cancel(
                            &context, job_id, token, &backend, &fence, &spec, &bucket,
                        ))
                        .await;
                        return false;
                    }
                    Err(error) => {
                        let error = if error.retryable() {
                            JobError::retryable(format!("resubmit failed: {error}"))
                        } else {
                            JobError::permanent(format!("resubmit failed: {error}"))
                        };
                        Box::pin(fail_or_park(&context, job_id, token, &record, error)).await;
                        return false;
                    }
                };
                if let Some(started_at_ms) = status.started_at_ms
                    && record_attempt_started(&context.storage_handle, job_id, token, started_at_ms)
                        .await
                        .is_err()
                {
                    return false;
                }
                let running = match transition_external_to_running(
                    &context.storage_handle,
                    job_id,
                    token,
                    status.started_at_ms,
                    unix_timestamp_millis(),
                )
                .await
                {
                    Ok(record) => record,
                    Err(_) => return false,
                };
                if running.cancel_requested {
                    Box::pin(finalize_cancel(
                        &context, job_id, token, &backend, &fence, &spec, &bucket,
                    ))
                    .await;
                    return false;
                }
                true
            }),
        )
        .await;
        if resumed != Some(true) {
            return;
        }
    }
    Box::pin(supervise_and_finalize(
        context, job_id, token, backend, fence, spec, bucket, cancel,
    ))
    .await;
}

async fn fail_or_park(
    context: &Arc<DriverContext>,
    job_id: aruna_core::structs::JobId,
    token: ulid::Ulid,
    record: &JobRecord,
    error: JobError,
) {
    if error.kind == JobErrorKind::Permanent {
        fail_and_crate(context, job_id, token, record, error).await;
    } else {
        park_attempt(context, job_id, token, error).await;
    }
}

fn holder(context: &DriverContext) -> aruna_core::types::NodeId {
    context
        .net_handle
        .as_ref()
        .map(|net| net.node_id())
        .unwrap_or_else(|| iroh::SecretKey::from_bytes(&[0u8; 32]).public())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::JOB_MAX_ATTEMPTS;
    use crate::jobs::store::{insert_job, record_attempt_intent};
    use crate::jobs::workflow::tests::{execution_spec, node_id};
    use aruna_compute::ExecutorRegistry;
    use aruna_core::structs::{AttemptIntent, JobClaim, JobId, RealmId};
    use aruna_core::types::UserId;
    use aruna_storage::FjallStorage;
    use aruna_tasks::TaskHandle;
    use tempfile::tempdir;
    use ulid::Ulid;

    // A node whose registry lacks the executor cannot observe the container, so no
    // number of sweeps by it may spend an attempt or terminalize the job.
    #[tokio::test]
    async fn missing_backend_spares() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = Arc::new(DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: Some(TaskHandle::new()),
            compute_handle: Some(Arc::new(ExecutorRegistry::new())),
        });
        let job_id = JobId::new();
        let token = Ulid::generate();
        let mut record = JobRecord::new(
            job_id,
            JobPayload::Execution(execution_spec()),
            UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32])),
            node_id(7),
            1,
            1,
            None,
        );
        record.state = JobState::Running;
        record.claim = Some(JobClaim {
            holder_node_id: node_id(7),
            claim_token: token,
            lease_expires_at_ms: 1,
        });
        insert_job(&storage, &record).await.unwrap();
        record_attempt_intent(
            &storage,
            job_id,
            token,
            AttemptIntent {
                attempt_no: 1,
                external_name: job_id.to_string().to_lowercase(),
                executor_kind: "kubernetes".to_string(),
                pinned_image: "alpine@sha256:digest".to_string(),
                attempt_epoch: 0,
            },
            unix_timestamp_millis(),
        )
        .await
        .unwrap();

        let runtime = JobsRuntime::new();
        let reconciler = ComputeReconciler::new(context, Arc::downgrade(&runtime));
        for sweep in 0..(JOB_MAX_ATTEMPTS + 2) {
            let lost = read_job_record(&storage, job_id, None)
                .await
                .unwrap()
                .unwrap();
            reconciler.reconcile_lost_attempt(&storage, lost).await;
            let after = read_job_record(&storage, job_id, None)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(after.state, JobState::Running, "sweep {sweep}");
            assert_eq!(after.attempts, 0, "sweep {sweep}");
            assert!(after.attempt_intent.is_some(), "sweep {sweep}");
        }
    }
}
