//! Runs job operations with their host: the stop signals, progress and turns of one job run.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::any::Any;
use std::collections::BTreeSet;
use std::fmt::Display;
use std::sync::Arc;

use aruna_blob::pace::{Pace, paced};
use aruna_core::jobs::{JobReport, JobStops, ObjectBody, ObjectRead, ObjectReadFailure};
use aruna_core::operation::Runnable;
use aruna_core::structs::execution::job::{JobError, StagingJobSpec};
use aruna_core::structs::storage::backend_import::BackendImportSpec;
use aruna_core::structs::storage::storage_purge::StoragePurgeSpec;
use futures_util::future::BoxFuture;
use rand::RngExt;
use tokio_util::sync::CancellationToken;
use ulid::Ulid;

use super::job_context::JobContext;
use crate::driver::{DriverContext, drive, node_routing};
use crate::effect_adapters::job::{JobHost, with_job_host};
use crate::jobs::backend_import::{BackendImportOperation, ImportJob};
use crate::jobs::executor::{JobRunOutcome, ProgressReporter, RangeTurns};
use crate::jobs::staging::{StagingJobOperation, StagingRun};
use crate::jobs::workflow::purge::{PurgeJobOperation, PurgeRun};
use crate::s3::object::get::{GetObjectError, GetObjectInput, ObjectRangeRequest};
use crate::s3::object::read::GetRoutedOperation;

/// The running job's signals and shared state, as its operations reach them through effects.
struct ContextHost {
    driver: Arc<DriverContext>,
    cancel: CancellationToken,
    shutdown: CancellationToken,
    progress: ProgressReporter,
    turns: RangeTurns,
}

impl JobHost for ContextHost {
    fn stops(&self) -> JobStops {
        JobStops {
            cancelled: self.cancel.is_cancelled(),
            shutdown: self.shutdown.is_cancelled(),
        }
    }

    fn report(&self, report: JobReport) {
        if let Some(current) = report.current {
            self.progress.set_current(current);
        }
        if let Some(total) = report.total {
            self.progress.set_total(total);
        }
        if report.advance > 0 {
            self.progress.advance(report.advance);
        }
        if report.checkpoint {
            self.progress.checkpoint();
        }
    }

    fn stopped(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            tokio::select! {
                _ = self.cancel.cancelled() => {}
                _ = self.shutdown.cancelled() => {}
            }
        })
    }

    fn take_turns<'a>(
        &'a self,
        turns: &'a BTreeSet<(Ulid, Ulid)>,
    ) -> BoxFuture<'a, Box<dyn Any + Send>> {
        Box::pin(async move {
            let locks = self.turns.turn_locks(turns);
            let mut guards = Vec::with_capacity(locks.len());
            for lock in locks {
                guards.push(lock.lock_owned().await);
            }
            Box::new(guards) as Box<dyn Any + Send>
        })
    }

    fn read_object(
        &self,
        read: ObjectRead,
    ) -> BoxFuture<'_, Result<ObjectBody, ObjectReadFailure>> {
        Box::pin(async move {
            let input = GetObjectInput {
                bucket: read.bucket,
                key: read.key,
                version_id: read.version_id,
                range: (read.range).map(|(start, end)| ObjectRangeRequest::StartEnd { start, end }),
                group_id: read.group_id,
                user_identity: read.user_id,
                realm_id: read.realm_id,
                node_id: read.node_id,
            };
            let found = drive(
                GetRoutedOperation::new(input, read.restrictions, None),
                &self.driver,
            )
            .await;
            match found {
                Ok(found) => Ok(ObjectBody {
                    version_id: found.resolved_version_id.or(found.version_id),
                    size: found.info.size,
                    stream: found.blob,
                }),
                Err(GetObjectError::InvalidRange) => Err(ObjectReadFailure::InvalidRange),
                Err(
                    error @ (GetObjectError::NoSuchKey
                    | GetObjectError::NoSuchVersion
                    | GetObjectError::DeleteMarker),
                ) => Err(ObjectReadFailure::Gone(error.to_string())),
                Err(error) => Err(ObjectReadFailure::Failed(error.to_string())),
            }
        })
    }
}

/// Drives a job's operation with the job's host answering its effects. A failure of the run
/// itself is retryable, since the job's checkpoint stays.
pub(crate) async fn run_hosted<O, K>(ctx: &JobContext, operation: O) -> JobRunOutcome
where
    O: Runnable<K, Output = JobRunOutcome>,
    O::Error: Display,
{
    let host = Arc::new(ContextHost {
        driver: ctx.driver.clone(),
        cancel: ctx.cancel.clone(),
        shutdown: ctx.shutdown.clone(),
        progress: ctx.progress.clone(),
        turns: ctx.range_turns.clone(),
    });
    let run = with_job_host(host, drive(operation, &ctx.driver));
    match Box::pin(run).await {
        Ok(outcome) => outcome,
        Err(error) => JobRunOutcome::Failed(JobError::retryable(error.to_string())),
    }
}

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
