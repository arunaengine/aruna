//! Builds the runs of hosted job payloads from the job context and runs them under its host.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use aruna_blob::pace::{Pace, paced};
use aruna_core::structs::execution::job::{CopyJobSpec, StagingJobSpec};
use aruna_core::structs::storage::backend_import::BackendImportSpec;
use aruna_core::structs::storage::storage_purge::StoragePurgeSpec;
use rand::RngExt;

use aruna_operations::driver::node_routing;
use aruna_operations::effect_adapters::job::run_hosted;
use aruna_operations::jobs::backend_import::{BackendImportOperation, ImportJob};
use aruna_operations::jobs::context::JobContext;
use aruna_operations::jobs::copy::CopyJobOperation;
use aruna_operations::jobs::executor::JobRunOutcome;
use aruna_operations::jobs::staging::{StagingJobOperation, StagingRun};
use aruna_operations::jobs::workflow::purge::{PurgeJobOperation, PurgeRun};

/// Runs a storage purge; with `files` it also releases the linked-storage files of the versions.
pub async fn run_storage_purge(
    ctx: &JobContext,
    spec: &StoragePurgeSpec,
    files: bool,
) -> JobRunOutcome {
    let wiring = (node_routing(&ctx.driver), ctx.driver.node_and_realm());
    let ids = (ctx.job_id, ctx.claim_token);
    let run = PurgeRun::new(ids, spec, files, wiring, ctx.progress.snapshot());
    run_hosted(ctx, PurgeJobOperation::new(run)).await
}

pub async fn run_backend_import(ctx: &JobContext, spec: &BackendImportSpec) -> JobRunOutcome {
    let job = ImportJob {
        spec: Arc::new(spec.clone()),
        job_id: ctx.job_id,
        claim_token: ctx.claim_token,
        routing: node_routing(&ctx.driver),
        seed: rand::rng().random(),
    };
    run_hosted(ctx, BackendImportOperation::new(job)).await
}

pub async fn run_staging_job(ctx: &JobContext, spec: &StagingJobSpec) -> JobRunOutcome {
    let run = StagingRun {
        spec: Arc::new(spec.clone()),
        job_id: ctx.job_id,
        claim_token: ctx.claim_token,
        routing: node_routing(&ctx.driver),
        seed: rand::rng().random(),
    };
    let job = run_hosted(ctx, StagingJobOperation::new(run));
    // Every source request of a table import, adapter retries included, takes a token; a
    // pause a source asks for ends with the job and shows in its progress.
    match spec.table.as_ref() {
        Some(table) => {
            let progress = ctx.progress.clone();
            let pace = Pace {
                stops: vec![ctx.cancel.clone(), ctx.shutdown.clone()],
                notice: Some(Arc::new(move |until| progress.set_pause(until))),
                ..Pace::new(spec.connector_id, table.rate())
            };
            Box::pin(paced(pace, job)).await
        }
        None => Box::pin(job).await,
    }
}

/// How often the bytes pulled so far reach the job's progress.
const PROGRESS_TICK: Duration = Duration::from_secs(1);

/// Copies one object, reporting the bytes read so far while the source is pulled.
/// Cancellation drops the copy unwritten.
pub async fn run_copy_job(ctx: &JobContext, spec: &CopyJobSpec) -> JobRunOutcome {
    let pulled = Arc::new(AtomicU64::new(0));
    let routing = node_routing(&ctx.driver);
    let copy = CopyJobOperation::new(spec.clone(), ctx.job_id, routing, pulled.clone());
    let run = run_hosted(ctx, copy);
    tokio::pin!(run);
    let mut tick = tokio::time::interval(PROGRESS_TICK);
    loop {
        tokio::select! {
            outcome = &mut run => return outcome,
            _ = tick.tick() => ctx.progress.set_current(pulled.load(Ordering::Relaxed)),
            _ = ctx.cancel.cancelled() => return JobRunOutcome::Cancelled,
            _ = ctx.shutdown.cancelled() => return JobRunOutcome::Interrupted,
        }
    }
}
