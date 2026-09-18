//! Waits for a running attempt while renewing its lease, then finalizes the outcome.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::time::Duration;

use aruna_compute::ExecutorBackend;
use aruna_compute::session::EndReason;
use aruna_core::compute::{AttemptStatus, BackendError, FenceContext};
use aruna_core::structs::execution::job::{ExecutionSpec, JobId};
use aruna_core::time::unix_timestamp_millis;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::super::JOB_HEARTBEAT_MS;
use super::super::store::{JobMutationError, read_job_record, record_attempt_started, renew_lease};
use super::DEFAULT_WALLTIME;
use super::finalize::{finalize_attempt, finalize_session, finalize_walltime};
use super::session::{start_session, write_session_report};
use crate::driver::DriverContext;

/// Heartbeat + backend wait race, then evidence-based terminalization. Shared by
/// the fresh path and reconcile adoption.
#[allow(clippy::too_many_arguments)]
pub async fn supervise_and_finalize(
    context: Arc<DriverContext>,
    job_id: JobId,
    token: ulid::Ulid,
    backend: Arc<dyn ExecutorBackend>,
    fence: FenceContext,
    spec: ExecutionSpec,
    bucket: String,
    cancel: CancellationToken,
) {
    let storage = context.storage_handle.clone();
    let walltime_ms = spec
        .resources
        .max_walltime_ms
        .unwrap_or(DEFAULT_WALLTIME.as_millis() as u64);
    let started_at_ms = Box::pin(walltime_anchor(&storage, job_id, token)).await;
    let walltime_left = Duration::from_millis(
        started_at_ms
            .saturating_add(walltime_ms)
            .saturating_sub(unix_timestamp_millis()),
    );
    let session = start_session(&context, &backend, &fence, job_id, &spec, &bucket).await;
    let wait_and_finalize = async {
        // A session also stops when the client ends it or it goes idle; every
        // other reason leaves the attempt wait in charge.
        let stopped = async {
            match &session {
                Some(session) => {
                    let reason = session.finished().await;
                    match reason {
                        EndReason::Ended | EndReason::Idle | EndReason::KernelExit => reason,
                        _ => std::future::pending().await,
                    }
                }
                None => std::future::pending().await,
            }
        };
        let outcome = tokio::select! {
            result = backend.wait(&fence, &cancel) => SessionOutcome::Attempt(result),
            _ = tokio::time::sleep(walltime_left) => SessionOutcome::Walltime,
            reason = stopped => SessionOutcome::Stopped(reason),
        };
        match outcome {
            SessionOutcome::Attempt(result) => {
                if let Some(session) = &session {
                    // A cancelled attempt ends its session for that reason; any
                    // other return means the container stopped on its own.
                    let reason = match cancel.is_cancelled() {
                        true => EndReason::Cancelled,
                        false => EndReason::KernelExit,
                    };
                    session.end(reason);
                    Box::pin(write_session_report(
                        &context.storage_handle,
                        job_id,
                        token,
                        Some(session),
                        reason,
                    ))
                    .await;
                }
                Box::pin(finalize_attempt(
                    &context, job_id, token, &backend, &fence, &spec, &bucket, result,
                ))
                .await;
            }
            SessionOutcome::Walltime => {
                if let Some(session) = &session {
                    session.end(EndReason::Walltime);
                    Box::pin(write_session_report(
                        &context.storage_handle,
                        job_id,
                        token,
                        Some(session),
                        EndReason::Walltime,
                    ))
                    .await;
                }
                Box::pin(finalize_walltime(
                    &context, job_id, token, &backend, &fence, &bucket,
                ))
                .await;
            }
            SessionOutcome::Stopped(reason) => {
                Box::pin(finalize_session(
                    &context,
                    job_id,
                    token,
                    &backend,
                    &fence,
                    &bucket,
                    session.as_ref(),
                    reason,
                ))
                .await;
            }
        }
    };
    let superseded = with_execution_heartbeat(
        storage,
        job_id,
        token,
        cancel.clone(),
        Box::pin(wait_and_finalize),
    )
    .await
    .is_none();
    if superseded {
        info!(job_id = %job_id, "Execution supervisor superseded; abandoning");
        // This node no longer owns the attempt, so its session stops here even
        // though nothing else delivered an end for it.
        if let Some(session) = &session {
            session.end(EndReason::Cancelled);
        }
    }
    // The session outlives its end until here, so a client that reads or ends
    // it during teardown still gets the ended state, not 409. The identity
    // check keeps this cleanup from dropping a same-id replacement.
    if let (Some(session), Some(registry)) = (&session, context.compute_handle.as_ref()) {
        registry.sessions().close(session);
    }
}

/// The walltime cap needs an anchor even without backend start evidence. The
/// first supervision pass persists the one it derives, so a restarted
/// supervisor cannot hand the attempt a fresh window.
pub(super) async fn walltime_anchor(
    storage: &aruna_storage::StorageHandle,
    job_id: JobId,
    token: ulid::Ulid,
) -> u64 {
    if let Some(started_at_ms) = read_job_record(storage, job_id, None)
        .await
        .ok()
        .flatten()
        .and_then(|record| record.started_at_ms)
    {
        return started_at_ms;
    }
    let anchor = unix_timestamp_millis();
    match record_attempt_started(storage, job_id, token, anchor).await {
        Ok(record) => record.started_at_ms.unwrap_or(anchor),
        Err(error) => {
            warn!(job_id = %job_id, error = %error, "Walltime anchor write failed");
            anchor
        }
    }
}

/// Why the supervisor left its wait.
enum SessionOutcome {
    Attempt(Result<AttemptStatus, BackendError>),
    Walltime,
    Stopped(EndReason),
}
pub(super) async fn with_execution_heartbeat<T>(
    storage: aruna_storage::StorageHandle,
    job_id: JobId,
    token: ulid::Ulid,
    cancel: CancellationToken,
    work: impl std::future::Future<Output = T>,
) -> Option<T> {
    let stop = CancellationToken::new();
    let heartbeat = tokio::spawn(execution_heartbeat(
        storage,
        job_id,
        token,
        cancel,
        stop.clone(),
    ));
    tokio::pin!(heartbeat);
    tokio::pin!(work);
    tokio::select! {
        result = &mut work => {
            stop.cancel();
            let _ = (&mut heartbeat).await;
            Some(result)
        }
        _ = &mut heartbeat => None,
    }
}

/// Renew the lease and surface `cancel_requested`. Returns on a lost token so the
/// supervisor treats it as a takeover.
pub(super) async fn execution_heartbeat(
    storage: aruna_storage::StorageHandle,
    job_id: JobId,
    token: ulid::Ulid,
    cancel: CancellationToken,
    stop: CancellationToken,
) {
    let mut interval = tokio::time::interval(Duration::from_millis(JOB_HEARTBEAT_MS));
    interval.tick().await;
    loop {
        tokio::select! {
            _ = stop.cancelled() => return,
            _ = interval.tick() => {
                match renew_lease(&storage, job_id, token, unix_timestamp_millis(), None).await {
                    Ok(renew) => if renew.cancel_requested { cancel.cancel(); }
                    Err(JobMutationError::TokenMismatch) => return,
                    Err(error) => warn!(job_id = %job_id, error = %error, "Execution lease renew failed"),
                }
            }
        }
    }
}
