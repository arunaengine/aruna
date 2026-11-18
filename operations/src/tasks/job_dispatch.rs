//! Runs one leased job's payload on this node: the dispatch to its runner and the small payloads
//! that are nothing but a run.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::calls::IoError;
use aruna_core::id::NodeId;
use aruna_core::structs::execution::job::{JobError, JobPayload, JobResultPayload};
use aruna_core::time::unix_timestamp_millis;

use super::job_context::JobContext;
use super::job_host::{run_backend_import, run_hosted, run_staging_job};
use crate::driver::drive;
use crate::jobs::executor::JobRunOutcome;
use crate::jobs::persistent_id::{MintPidOperation, RegisterIdentifiersOperation};
use crate::notifications::email_job::EmailJobOperation;
use crate::notifications::settings::{BACKOFF_START_MS, DeliveryOutcome};
use crate::notifications::webhook_job::WebhookJobOperation;

/// Payload dispatch, mirroring how the task handler matches `TaskKey`. Execution
/// payloads never reach here: the runtime routes them to the fenced external path
/// before this seam.
pub(crate) async fn dispatch_payload(ctx: &JobContext, payload: &JobPayload) -> JobRunOutcome {
    // Each payload is boxed, so this future and every future that holds it stay small.
    match payload {
        JobPayload::Probe {
            steps,
            step_sleep_ms,
            fail_at,
            panic_at,
            cleanup_marker,
        } => {
            Box::pin(run_probe(
                ctx,
                *steps,
                *step_sleep_ms,
                *fail_at,
                *panic_at,
                cleanup_marker.as_deref(),
            ))
            .await
        }
        JobPayload::WriteRunCrate { for_job } => {
            let write = crate::jobs::workflow::run_crate::WriteCrateOperation::new(
                *for_job,
                ctx.driver.node_and_realm(),
            );
            let result = Box::pin(crate::driver::drive(write, &ctx.driver)).await;
            crate::jobs::workflow::run_crate::crate_outcome(result)
        }
        JobPayload::TerminalCleanup {
            for_job,
            attempt,
            access_key,
        } => {
            let cleanup = crate::jobs::workflow::cleanup::TerminalCleanupOperation::new(
                *for_job,
                ctx.claim_token,
                attempt.clone(),
                access_key,
            );
            let result = Box::pin(crate::driver::drive(cleanup, &ctx.driver)).await;
            crate::jobs::workflow::cleanup::cleanup_outcome(result)
        }
        JobPayload::Staging(spec) => Box::pin(run_staging_job(ctx, spec)).await,
        JobPayload::ExportRoCrate(spec) => {
            Box::pin(crate::jobs::export::run_export_job(ctx, spec)).await
        }
        JobPayload::ImportRoCrate(spec) => {
            Box::pin(crate::jobs::import::run_rocrate_import(ctx, spec)).await
        }
        JobPayload::Harvest(spec) => crate::jobs::harvest::harvest_outcome(
            Box::pin(crate::driver::drive(
                crate::jobs::harvest::HarvestJobOperation::new(
                    crate::jobs::repository::NodeScope::of(&ctx.driver),
                    crate::jobs::repository::JobScope::of(ctx),
                    spec.clone(),
                ),
                &ctx.driver,
            ))
            .await,
        ),
        JobPayload::MintPersistentId(spec) => {
            let realm_id = ctx.driver.serving_realm(spec.minted_by.realm_id);
            let node = (realm_id, local_node(ctx));
            let now_ms = unix_timestamp_millis();
            let job = MintPidOperation::new(spec, node, ctx.final_attempt, now_ms);
            Box::pin(run_hosted(ctx, job)).await
        }
        JobPayload::StoragePurge(spec) => {
            Box::pin(super::job_host::run_storage_purge(ctx, spec, false)).await
        }
        JobPayload::StoragePurgeFiles(spec) => {
            Box::pin(super::job_host::run_storage_purge(ctx, spec, true)).await
        }
        JobPayload::CopyObject(spec) => Box::pin(crate::jobs::copy::run_copy_job(ctx, spec)).await,
        JobPayload::BackendImport(spec) => Box::pin(run_backend_import(ctx, spec)).await,
        JobPayload::RegisterIdentifiers(spec) => {
            let queued_at_ms = ctx.job_id.timestamp_ms();
            let now_ms = unix_timestamp_millis();
            let job =
                RegisterIdentifiersOperation::new(spec, local_node(ctx), queued_at_ms, now_ms);
            Box::pin(run_hosted(ctx, job)).await
        }
        JobPayload::WebhookDelivery(spec) => {
            let now_ms = unix_timestamp_millis();
            let job = WebhookJobOperation::new(spec.clone(), delivery_node(ctx), now_ms);
            Box::pin(run_delivery(ctx, drive(job, &ctx.driver))).await
        }
        JobPayload::EmailDelivery(spec) => {
            let now_ms = unix_timestamp_millis();
            let job = EmailJobOperation::new(spec.clone(), delivery_node(ctx), now_ms);
            Box::pin(run_delivery(ctx, drive(job, &ctx.driver))).await
        }
        JobPayload::PluginWork(_) => {
            let handled = (ctx.handlers.iter()).find_map(|handler| handler.run(ctx, payload));
            match handled {
                Some(run) => run.await,
                None => JobRunOutcome::Failed(JobError::permanent(
                    "no handler is registered for this payload",
                )),
            }
        }
        // Guard: an execution job must run through the external attempt path.
        JobPayload::Execution(_) => JobRunOutcome::Failed(JobError::permanent(
            "execution payload dispatched through the in-process seam",
        )),
    }
}

/// This node when it is on the network.
fn local_node(ctx: &JobContext) -> Option<NodeId> {
    ctx.driver.node_and_realm().map(|(node_id, _)| node_id)
}

/// The node a delivery job decides for.
fn delivery_node(ctx: &JobContext) -> NodeId {
    (ctx.driver.net_handle.as_ref()).map_or(ctx.owner_node_id, |net| net.node_id())
}

/// Runs a delivery job until it ends, is cancelled or the node shuts down. A node without a
/// metadata transport cannot open secrets, so the job waits; a failed run may pass on a retry.
async fn run_delivery(
    ctx: &JobContext,
    delivery: impl Future<Output = Result<DeliveryOutcome, IoError>>,
) -> JobRunOutcome {
    if ctx.driver.metadata_handle.is_none() || ctx.driver.outbound_handle.is_none() {
        return JobRunOutcome::DeferredFor(
            JobError::retryable("the metadata transport is unavailable"),
            Duration::from_millis(BACKOFF_START_MS),
        );
    }
    tokio::select! {
        biased;
        () = ctx.cancel.cancelled() => JobRunOutcome::Cancelled,
        () = ctx.shutdown.cancelled() => JobRunOutcome::Interrupted,
        outcome = delivery => match outcome {
            Ok(DeliveryOutcome::Succeeded(payload)) => JobRunOutcome::Succeeded(payload),
            Ok(DeliveryOutcome::Failed(error)) => JobRunOutcome::Failed(error),
            Ok(DeliveryOutcome::DeferredFor(error, delay)) => {
                JobRunOutcome::DeferredFor(error, delay)
            }
            Err(error) => JobRunOutcome::Failed(JobError::retryable(error.to_string())),
        },
    }
}

async fn run_probe(
    ctx: &JobContext,
    steps: u32,
    step_sleep_ms: u64,
    fail_at: Option<u32>,
    panic_at: Option<u32>,
    cleanup_marker: Option<&str>,
) -> JobRunOutcome {
    ctx.progress.set_total(steps as u64);
    if let Some(marker) = cleanup_marker {
        let _ = std::fs::write(marker, b"running");
    }
    for step in 0..steps {
        // A zero-sleep probe never awaits otherwise, so it would starve everything else
        // sharing its worker until the whole run finished.
        tokio::task::yield_now().await;
        if ctx.cancel.is_cancelled() {
            return JobRunOutcome::Cancelled;
        }
        if ctx.shutdown.is_cancelled() {
            return JobRunOutcome::Interrupted;
        }
        if panic_at == Some(step) {
            panic!("probe panic at step {step}");
        }
        if fail_at == Some(step) {
            return JobRunOutcome::Failed(JobError::retryable(format!(
                "probe failed at step {step}"
            )));
        }
        if step_sleep_ms > 0 {
            tokio::select! {
                biased;
                _ = ctx.cancel.cancelled() => return JobRunOutcome::Cancelled,
                _ = ctx.shutdown.cancelled() => return JobRunOutcome::Interrupted,
                _ = tokio::time::sleep(Duration::from_millis(step_sleep_ms)) => {}
            }
        }
        ctx.progress.advance(1);
    }
    if let Some(marker) = cleanup_marker {
        let _ = std::fs::remove_file(marker);
    }
    JobRunOutcome::Succeeded(JobResultPayload::Probe {
        completed_steps: steps,
    })
}
