//! Runs one native work of a plugin install on its receiver: every attempt checks the install,
//! its account, the work's binding and the grants again, then runs the action as the account.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use aruna_core::errors::ConversionError;
use aruna_core::plugin_installs::PluginInstall;
use aruna_core::structs::execution::job::{
    JobError, JobPayload, JobResultPayload, PluginWorkAction, PluginWorkSpec,
};
use aruna_core::structs::identity::auth::{AuthContext, PathRestriction, Permission as Access};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::storage::blob::VersionKey;
use aruna_core::structs::storage::encryption::BucketKeyError;
use aruna_plugins::broker::{ErrorKind, HostError};
use futures_util::future::BoxFuture;

use crate::driver::{DriverContext, drive};
use crate::jobs::executor::{JobContext, JobRunOutcome, PayloadHandler};
use crate::jobs::key_locks::VersionWaitOperation;
use crate::plugins::check::CopyGrants;
use crate::plugins::objects::{LocalObject, Unmarked};
use crate::plugins::work::holds_job;
use crate::plugins::work_calls::{AdmitWork, AdmittedWork, WorkRefusal};

use super::transfers::{Download, Upload, download_work, upload_work};
use crate::s3::object::copy::{
    CopyObjectError, CopyObjectInput, CopyReferences, CopySourceConditions, copy_object_at,
};
use crate::s3::object::get::GetObjectError;
use crate::s3::object::head::{HeadObjectInput, HeadObjectOperation};

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
        PluginWorkAction::Copy { .. } => copy_work(ctx, spec, (&install, &config)).await,
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

/// Copies one object as the account, restricted to its current read patterns of the source and
/// write patterns of the target.
async fn copy_work(
    ctx: &JobContext,
    spec: &PluginWorkSpec,
    (install, config): (&PluginInstall, &RealmConfigDocument),
) -> JobRunOutcome {
    let PluginWorkAction::Copy {
        source,
        source_version,
        target,
        version_id,
    } = &spec.action
    else {
        return permanent("the work is not a copy");
    };
    let patterns = match drive(CopyGrants::new(install, (source, target)), &ctx.driver).await {
        Ok(found) => found,
        Err(error) => return refused(error),
    };
    let place = (spec.realm_id, ctx.owner_node_id);
    let source_object = LocalObject::new(place, install, source);
    let (source_bucket, source_key) = match drive(source_object, &ctx.driver).await {
        Ok(found) => found,
        Err(error) => return refused(error),
    };
    let target_object = LocalObject::new(place, install, target);
    let (dest_bucket, dest_key) = match drive(target_object, &ctx.driver).await {
        Ok(found) => found,
        Err(error) => return refused(error),
    };
    // The version checked is the version copied, so a newer marked one never slips in.
    let readable = Unmarked::new((&source_bucket, &source_key), *source_version);
    let source_version = match drive(readable, &ctx.driver).await {
        Ok(found) => found.or(*source_version),
        Err(error) => return refused(error),
    };
    let auth = account(install, &patterns);
    let read = (source_bucket.clone(), source_key.clone(), source_version);
    let input = CopyObjectInput {
        source_bucket,
        source_key,
        source_version_id: source_version,
        source_group_id: install.group_id,
        source_auth_context: auth.clone(),
        dest_bucket,
        dest_key,
        user_id: install.account,
        group_id: install.group_id,
        realm_id: spec.realm_id,
        node_id: ctx.owner_node_id,
        quota_ceiling: config.quota.effective_group_ceiling(&install.group_id),
        conditions: CopySourceConditions::default(),
        metadata: None,
        restrictions: auth.path_restrictions,
        references: CopyReferences::Materialize,
        job_id: Some(ctx.job_id.as_ulid()),
    };
    let pulled = Arc::new(AtomicU64::new(0));
    // The version was minted once, so a retry after a lost answer replays it.
    let copy = copy_object_at(&ctx.driver, input, *version_id, Some(pulled.clone()));
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
        Err(error) if source_locked(&error) => key_wait(ctx, install, read).await,
        Err(error) => JobRunOutcome::Failed(crate::jobs::copy::copy_error(error)),
    }
}

fn source_locked(error: &CopyObjectError) -> bool {
    matches!(
        error,
        CopyObjectError::Get(GetObjectError::ConversionError(ConversionError::BucketKey(
            BucketKeyError::Locked(_)
        )))
    )
}

/// Parks the work on the key of the source version it reads, so it runs again once that key is
/// unlocked; a key unlocked meanwhile is tried again at once.
pub(super) async fn key_wait(
    ctx: &JobContext,
    install: &PluginInstall,
    (bucket, key, version_id): (String, String, Option<ulid::Ulid>),
) -> JobRunOutcome {
    let deferred = |message: String| JobRunOutcome::Deferred(JobError::retryable(message));
    let head = HeadObjectInput {
        bucket: bucket.clone(),
        key: key.clone(),
        version_id,
    };
    let head = drive(HeadObjectOperation::new(head), &ctx.driver).await;
    let Some(version_id) = head.ok().and_then(|head| head.resolved_version_id) else {
        return deferred("the locked source version could not be described".into());
    };
    let version = VersionKey::new(bucket, key, version_id);
    let place = (ctx.owner_node_id, install.group_id);
    match drive(VersionWaitOperation::new(place, version), &ctx.driver).await {
        Ok(Some(wait)) => JobRunOutcome::AwaitingKey(vec![wait]),
        Ok(None) => deferred("the source bucket was unlocked meanwhile".into()),
        Err(error) => deferred(error.0),
    }
}

/// The install's account restricted to `patterns` with WRITE, which also reads.
pub(super) fn account(install: &PluginInstall, patterns: &BTreeSet<String>) -> AuthContext {
    let restrictions = (patterns.iter())
        .map(|pattern| PathRestriction {
            pattern: pattern.clone(),
            permission: Access::WRITE,
        })
        .collect();
    AuthContext {
        user_id: install.account,
        realm_id: install.account.realm_id,
        path_restrictions: Some(restrictions),
        session: None,
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
