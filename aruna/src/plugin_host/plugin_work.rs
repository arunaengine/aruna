//! Runs one native work of a plugin install on its receiver: every attempt checks the install,
//! its account, the work's binding and the grants again, then runs the action as the account.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use aruna_core::plugin_installs::PluginInstall;
use aruna_core::structs::execution::job::{
    JobError, JobPayload, JobResultPayload, PluginWorkAction, PluginWorkSpec,
};
use aruna_plugins::broker::{ErrorKind, HostError};
use futures_util::future::BoxFuture;
use ulid::Ulid;

use aruna_operations::driver::{DriverContext, drive, node_routing};
use aruna_operations::jobs::executor::JobRunOutcome;
use aruna_operations::plugins::copy_steps::{CopyWork, LockedSource, PrepareCopy, copy_locked};
use aruna_operations::plugins::work::holds_job;
use aruna_operations::plugins::work_calls::{AdmitWork, AdmittedWork, WorkRefusal};
use aruna_operations::tasks::job_context::{JobContext, PayloadHandler};

use super::transfers::{Download, Upload, download_work, upload_work};
use aruna_operations::s3::object::copy::CopyObjectOperation;

/// How often the bytes moved so far reach the job's progress.
pub(super) const PROGRESS_TICK: Duration = Duration::from_secs(1);
/// How long a paused install's work waits before its next attempt.
const PAUSED_RETRY: Duration = Duration::from_secs(60);

/// Runs plugin work jobs and keeps each job until its work row holds the summary.
#[derive(Debug, Default)]
pub struct PluginWorkHandler;

impl PayloadHandler for PluginWorkHandler {
    fn run<'a>(
        &'a self,
        ctx: &'a JobContext,
        payload: &'a JobPayload,
    ) -> Option<BoxFuture<'a, JobRunOutcome>> {
        let JobPayload::PluginWork(spec) = payload else {
            return None;
        };
        Some(Box::pin(run_plugin_work(ctx, spec)))
    }

    fn keeps_job<'a>(
        &'a self,
        context: &'a DriverContext,
        payload: &'a JobPayload,
    ) -> BoxFuture<'a, Result<bool, String>> {
        Box::pin(async move {
            let JobPayload::PluginWork(spec) = payload else {
                return Ok(false);
            };
            drive(holds_job(spec), context)
                .await
                .map_err(|error| error.0)
        })
    }
}

/// Runs `spec` once. A refused check is final; state that cannot be read now is deferred.
pub async fn run_plugin_work(ctx: &JobContext, spec: &PluginWorkSpec) -> JobRunOutcome {
    let admit = AdmitWork::new(spec, ctx.owner_node_id);
    let AdmittedWork {
        install,
        config,
        row,
    } = match drive(admit, &ctx.driver).await {
        Ok(admitted) => admitted,
        Err(refusal) => return not_admitted(refusal),
    };
    match &spec.action {
        PluginWorkAction::Copy {
            source,
            source_version,
            target,
            version_id,
        } => {
            let work = CopyWork {
                node: (spec.realm_id, ctx.owner_node_id),
                job: ctx.job_id.as_ulid(),
                config: &config,
                install: &install,
                source,
                version: *source_version,
                target,
            };
            copy_work(ctx, work, *version_id).await
        }
        PluginWorkAction::Download {
            target,
            version_id,
            checksums,
            max_bytes,
        } => {
            let download = Download {
                target,
                version_id: *version_id,
                checksums,
                max_bytes: *max_bytes,
            };
            let admitted = (&install, &config, &row);
            download_work(ctx, spec, admitted, download).await
        }
        PluginWorkAction::Upload {
            source,
            version,
            form,
        } => {
            let upload = Upload {
                source,
                version: *version,
                form: form.as_ref(),
            };
            let admitted = (&install, &config, &row);
            upload_work(ctx, spec, admitted, upload).await
        }
    }
}

/// The outcome of an attempt that was not admitted.
fn not_admitted(refusal: WorkRefusal) -> JobRunOutcome {
    match refusal {
        WorkRefusal::Deferred(message) => JobRunOutcome::Deferred(JobError::retryable(message)),
        WorkRefusal::Held(message) => {
            JobRunOutcome::DeferredFor(JobError::retryable(message), PAUSED_RETRY)
        }
        WorkRefusal::Permanent(message) => permanent(message),
        WorkRefusal::Refused(error) => refused(error),
    }
}

/// Copies one object as the account. The version was minted once, so a retry after a lost answer
/// replays it.
async fn copy_work(ctx: &JobContext, work: CopyWork<'_>, version_id: Ulid) -> JobRunOutcome {
    let install = work.install;
    let input = match drive(PrepareCopy::new(work), &ctx.driver).await {
        Ok(input) => input,
        Err(error) => return refused(error),
    };
    let read = (
        input.source_bucket.clone(),
        input.source_key.clone(),
        input.source_version_id,
    );
    let pulled = Arc::new(AtomicU64::new(0));
    let copy = drive(
        CopyObjectOperation::new(input, node_routing(&ctx.driver))
            .at_version(version_id)
            .with_progress(pulled.clone()),
        &ctx.driver,
    );
    tokio::pin!(copy);
    let mut tick = tokio::time::interval(PROGRESS_TICK);
    let result = loop {
        tokio::select! {
            result = &mut copy => break result,
            _ = tick.tick() => ctx.progress.set_current(pulled.load(Ordering::Relaxed)),
            _ = ctx.cancel.cancelled() => return JobRunOutcome::Cancelled,
            _ = ctx.shutdown.cancelled() => return JobRunOutcome::Interrupted,
        }
    };
    match result {
        Ok(copied) => {
            ctx.progress.set_current(copied.size);
            JobRunOutcome::Succeeded(JobResultPayload::PluginWork {
                bytes: copied.size,
                version_id: Some(copied.version_id),
            })
        }
        Err(error) if copy_locked(&error) => key_wait(ctx, install, read).await,
        Err(error) => JobRunOutcome::Failed(aruna_operations::jobs::copy::copy_error(error)),
    }
}

/// Parks the work on the key of the source version it reads, so it runs again once that key is
/// unlocked; a key unlocked meanwhile is tried again at once.
pub(super) async fn key_wait(
    ctx: &JobContext,
    install: &PluginInstall,
    read: (String, String, Option<Ulid>),
) -> JobRunOutcome {
    let deferred = |message: String| JobRunOutcome::Deferred(JobError::retryable(message));
    let locked = LockedSource::new(ctx.owner_node_id, install, read);
    match drive(locked, &ctx.driver).await {
        Ok(Some(wait)) => JobRunOutcome::AwaitingKey(vec![wait]),
        Ok(None) => deferred("the source bucket was unlocked meanwhile".into()),
        Err(error) => deferred(error.0),
    }
}

/// A refused check ends the work; a check that could not run is tried again later.
pub(super) fn refused(error: impl Into<HostError>) -> JobRunOutcome {
    let error = error.into();
    match error.kind {
        ErrorKind::Unavailable => JobRunOutcome::Deferred(JobError::retryable(error.message)),
        _ => permanent(error.message),
    }
}

pub(super) fn permanent(message: impl Into<String>) -> JobRunOutcome {
    JobRunOutcome::Failed(JobError::permanent(message.into()))
}

#[cfg(test)]
#[path = "plugin_work_tests.rs"]
pub(crate) mod tests;
