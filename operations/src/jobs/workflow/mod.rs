pub mod cleanup;
pub mod compute;
pub mod finalize;
pub mod prepare;
pub mod purge;
pub mod reconcile;
pub mod recovery;
pub mod run_crate;
pub mod session;
pub mod supervise;
pub mod workspace;

use std::sync::Arc;
use std::time::Duration;

use aruna_compute::ExecutorBackend;
use aruna_core::compute::{AttemptRef, BackendError, ExecutorKind, FenceContext};
use aruna_core::structs::{
    AttemptIntent, ExecutionSpec, JobError, JobId, JobPayload, JobRecord, PhysicalExecutionState,
};
use aruna_core::time::unix_timestamp_millis;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::store::{
    JobMutationError, begin_external_running, cancel_running_job, read_job_record,
    record_attempt_intent, transition_to_preparing, transition_to_ready,
};
use crate::driver::DriverContext;
use crate::jobs::lifecycle::reservation::job_reservation;
use crate::jobs::lifecycle::updates::publish_progress;
use crate::placement::policy::subject::read_local_subject;
use finalize::{cleanup_and_crate, fail_and_crate, finalize_cancel};
use prepare::{build_task_spec, job_bucket, prepare_task};
use recovery::{pre_submit_failure, recover_failed_submit};
use supervise::{execution_heartbeat, supervise_and_finalize};

/// Fallback walltime when a spec declares none. Enforcement, reconcile
/// validation, and credential expiry must all agree on this value.
pub(crate) const DEFAULT_WALLTIME: Duration = Duration::from_secs(24 * 60 * 60);

/// Drive a claimed execution job through prepare -> submit -> supervise -> finalize.
/// External attempts never share the generic in-process supervisor because a lost
/// lease must not requeue a container (spec 16.7); this owns the fenced lifecycle.
pub async fn run_execution_job(
    context: Arc<DriverContext>,
    record: JobRecord,
    cancel: CancellationToken,
) {
    let storage = &context.storage_handle;
    let job_id = record.job_id;
    let Some(token) = record.claim.as_ref().map(|claim| claim.claim_token) else {
        warn!(job_id = %job_id, "Execution job has no claim token; skipping");
        return;
    };
    let JobPayload::Execution(mut spec) = record.payload.clone() else {
        return;
    };
    let bucket = job_bucket(&record);
    if !bucket.is_empty() {
        spec.resolve_outputs(&bucket, record.owner_node_id);
    }

    // A fresh cancel before any attempt was submitted: no container exists, so
    // terminalize directly (Claimed -> Cancelled).
    if record.cancel_requested && record.attempt_intent.is_none() {
        match cancel_running_job(storage, job_id, token, unix_timestamp_millis()).await {
            Ok(record) => Box::pin(cleanup_and_crate(&context, job_id, Some(record))).await,
            Err(error) => {
                warn!(job_id = %job_id, error = %error, "Fresh cancellation write failed")
            }
        }
        return;
    }

    let Some(node_id) = context.net_handle.as_ref().map(|net| net.node_id()) else {
        Box::pin(fail_and_crate(
            &context,
            job_id,
            token,
            &record,
            JobError::permanent("execution needs a net handle"),
        ))
        .await;
        return;
    };

    // A fenced refusal is retryable: the job returns to the queue, because the
    // site may come back or another target may take it.
    let backend = match resolve_backend(&context, &spec, job_id).await {
        Ok(backend) => backend,
        Err(error) => {
            Box::pin(pre_submit_failure(
                &context, job_id, token, &record, error, false,
            ))
            .await;
            return;
        }
    };

    // Claimed -> Preparing.
    if transition_to_preparing(storage, job_id, token, unix_timestamp_millis())
        .await
        .is_err()
    {
        warn!(job_id = %job_id, "Lost claim before preparing; aborting");
        return;
    }
    publish_progress(&context, job_id, PhysicalExecutionState::Preparing).await;

    let stop = CancellationToken::new();
    let heartbeat = tokio::spawn(execution_heartbeat(
        storage.clone(),
        job_id,
        token,
        cancel.clone(),
        stop.clone(),
    ));
    tokio::pin!(heartbeat);

    // Boxed so the large per-stage futures never inflate caller stacks.
    let mut prepare_and_submit = Box::pin(async {
        let prepared =
            match Box::pin(prepare_task(&context, &spec, &record, node_id, &bucket)).await {
                Ok(prepared) => prepared,
                Err(error) => {
                    Box::pin(pre_submit_failure(
                        &context, job_id, token, &record, error, false,
                    ))
                    .await;
                    return None;
                }
            };

        // Preparing -> Ready.
        if transition_to_ready(storage, job_id, token, unix_timestamp_millis())
            .await
            .is_err()
        {
            return None;
        }

        let attempt_no = record.attempts;
        // Lowercased: attempt ids must be injective under the backend name mapping.
        let attempt = AttemptRef::new(job_id.to_string().to_lowercase(), attempt_no);
        let pinned_image = match backend.resolve_image(&spec.image, &cancel).await {
            Ok(image) => image,
            Err(error) => {
                warn!(job_id = %job_id, image = %spec.image, %error, "image resolution failed");
                let job_error = if error.retryable() {
                    JobError::retryable(format!("image resolution failed: {error}"))
                } else {
                    JobError::permanent(format!("image resolution failed: {error}"))
                };
                Box::pin(pre_submit_failure(
                    &context, job_id, token, &record, job_error, true,
                ))
                .await;
                return None;
            }
        };
        let task_spec = build_task_spec(
            &spec,
            &attempt,
            &pinned_image,
            prepared,
            backend.run_identity(),
        );

        // A receipted execution already has its identity: the fenced attempt
        // binds that exact ExecutionId so its outputs chain to the receipt.
        let receipted = match job_reservation(&context, job_id).await {
            Ok(reservation) => reservation.map(|reservation| reservation.execution_id),
            Err(error) => {
                warn!(job_id = %job_id, %error, "Execution reservation lookup failed");
                return None;
            }
        };
        // Write-ahead the attempt intent BEFORE submit so a lost attempt is adoptable.
        let intent = AttemptIntent {
            attempt_no,
            external_name: attempt.external_name(),
            executor_kind: backend.kind().as_wire(),
            pinned_image,
            attempt_epoch: 0,
        };
        let intent_commit = match Box::pin(record_attempt_intent(
            storage,
            job_id,
            token,
            intent,
            receipted,
            unix_timestamp_millis(),
        ))
        .await
        {
            Ok(record) => record,
            Err(JobMutationError::IntentConflict) => {
                if read_job_record(storage, job_id, None)
                    .await
                    .ok()
                    .flatten()
                    .is_some_and(|record| record.cancel_requested)
                {
                    match cancel_running_job(storage, job_id, token, unix_timestamp_millis()).await
                    {
                        Ok(record) => {
                            Box::pin(cleanup_and_crate(&context, job_id, Some(record))).await
                        }
                        Err(error) => {
                            warn!(job_id = %job_id, error = %error, "Pre-submit cancellation write failed")
                        }
                    }
                }
                return None;
            }
            Err(_) => return None,
        };
        if intent_commit.record.cancel_requested {
            match cancel_running_job(storage, job_id, token, unix_timestamp_millis()).await {
                Ok(record) => Box::pin(cleanup_and_crate(&context, job_id, Some(record))).await,
                Err(error) => {
                    warn!(job_id = %job_id, error = %error, "Pre-submit cancellation write failed")
                }
            }
            return None;
        }

        let fence = FenceContext {
            attempt: attempt.clone(),
            attempt_epoch: intent_commit.control.attempt_epoch,
            controller_generation: intent_commit.control.controller_generation,
        };
        if backend.fence(&fence).await.is_err() {
            return None;
        }

        let submitted = match backend.submit(&fence, &task_spec, &cancel).await {
            Ok(status) => status,
            Err(BackendError::Cancelled) => {
                Box::pin(finalize_cancel(
                    &context, job_id, token, &backend, &fence, &spec, &bucket,
                ))
                .await;
                return None;
            }
            Err(error) => {
                return Some(Err((backend, fence, spec, bucket, cancel, error)));
            }
        };

        // Ready -> Running only after the backend accepted the fenced attempt.
        let running = match begin_external_running(
            storage,
            job_id,
            token,
            submitted.started_at_ms,
            unix_timestamp_millis(),
        )
        .await
        {
            Ok(record) => record,
            Err(_) => return None,
        };
        publish_progress(&context, job_id, PhysicalExecutionState::Running).await;
        if running.cancel_requested {
            Box::pin(finalize_cancel(
                &context, job_id, token, &backend, &fence, &spec, &bucket,
            ))
            .await;
            return None;
        }

        Some(Ok((backend, fence, spec, bucket, cancel)))
    });

    let prepared = tokio::select! {
        result = &mut prepare_and_submit => result,
        _ = &mut heartbeat => return,
    };
    let Some(prepared) = prepared else {
        stop.cancel();
        let _ = (&mut heartbeat).await;
        return;
    };
    match prepared {
        Ok((backend, fence, spec, bucket, cancel)) => {
            stop.cancel();
            let _ = (&mut heartbeat).await;
            Box::pin(supervise_and_finalize(
                context.clone(),
                job_id,
                token,
                backend,
                fence,
                spec,
                bucket,
                cancel,
            ))
            .await;
        }
        Err((backend, fence, spec, bucket, cancel, error)) => {
            let resumed = {
                let mut recovery = Box::pin(recover_failed_submit(
                    &context, job_id, token, &backend, &fence, &spec, &bucket, &cancel, error,
                ));
                tokio::select! {
                    result = &mut recovery => {
                        stop.cancel();
                        let _ = (&mut heartbeat).await;
                        Some(result)
                    }
                    _ = &mut heartbeat => None,
                }
            };
            if resumed == Some(true) {
                Box::pin(supervise_and_finalize(
                    context.clone(),
                    job_id,
                    token,
                    backend,
                    fence,
                    spec,
                    bucket,
                    cancel,
                ))
                .await;
            }
        }
    }
}

/// Resolve the backend for a spec, or a permanent error when none is eligible.
/// A receipted execution is fenced to the receipt's execution site: subject drift
/// refuses the start. A local job without a receipt keeps the unfenced selection.
pub async fn resolve_backend(
    context: &DriverContext,
    spec: &ExecutionSpec,
    job_id: JobId,
) -> Result<Arc<dyn ExecutorBackend>, JobError> {
    // A node without compute is a local gap, never a verdict on the job.
    let Some(registry) = context.compute_handle.as_ref() else {
        return Err(JobError::retryable("no compute backend configured"));
    };
    let constraint = spec
        .executor_constraint
        .as_deref()
        .map(ExecutorKind::from_wire);
    let selected = registry
        .select(constraint.as_ref())
        .cloned()
        .ok_or_else(|| JobError::permanent("no eligible executor for job"))?;
    let Some(stored) = stored_site(context, job_id)
        .await
        .map_err(JobError::retryable)?
    else {
        return Ok(selected);
    };
    let Some(subject) = read_local_subject(context)
        .await
        .ok()
        .flatten()
        .map(|record| record.subject)
    else {
        return Err(JobError::retryable(
            "receipted execution cannot start without a local placement subject",
        ));
    };
    match registry.fenced(&selected.kind(), &subject, stored.0, &stored.1) {
        Ok(backend) => Ok(backend.clone()),
        Err(BackendError::Fenced) => {
            warn!(
                job_id = %job_id,
                stored_generation = stored.0,
                current_generation = subject.generation,
                "Receipted execution refused: the execution site drifted from its receipt"
            );
            Err(JobError::retryable(format!(
                "execution site drifted from its receipt: stored subject generation {}, current {}",
                stored.0, subject.generation
            )))
        }
        Err(error) => Err(JobError::permanent(format!(
            "no eligible executor for job: {error}"
        ))),
    }
}

/// Subject generation and digest one receipted execution was accepted under.
/// `None` for a local job that never reserved capacity: the unfenced path.
async fn stored_site(
    context: &DriverContext,
    job_id: JobId,
) -> Result<Option<(u64, [u8; 32])>, String> {
    let Some(reservation) = job_reservation(context, job_id).await? else {
        return Ok(None);
    };
    Ok(match reservation.subject_generation {
        0 => None,
        generation => Some((generation, reservation.subject_digest)),
    })
}

#[cfg(test)]
mod tests;
