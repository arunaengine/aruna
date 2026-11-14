//! Runs one native work of a plugin install on its receiver: every attempt checks the install,
//! its account, the work's binding and the grants again, then runs the action as the account.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use aruna_core::plugin_installs::PluginInstall;
use aruna_core::structs::execution::job::{
    JobError, JobResultPayload, PluginWorkAction, PluginWorkSpec,
};
use aruna_core::structs::identity::auth::{AuthContext, PathRestriction, Permission as Access};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_plugins::broker::{ErrorKind, HostError};
use aruna_plugins::permission::Permission;

use super::executor::{JobContext, JobRunOutcome};
use crate::plugins::account::read_user;
use crate::plugins::broker::local_object;
use crate::plugins::check::{authorize_call, check_install, matching};
use crate::plugins::install::read_config;
use crate::plugins::work::read_work;
use crate::s3::object::copy::{
    CopyObjectInput, CopyReferences, CopySourceConditions, copy_object_tracked,
};

/// How often the bytes moved so far reach the job's progress.
const PROGRESS_TICK: Duration = Duration::from_secs(1);
/// How long a paused install's work waits before its next attempt.
const PAUSED_RETRY: Duration = Duration::from_secs(60);

/// Runs `spec` once. A refused check is final; state that cannot be read now is deferred.
pub async fn run_plugin_work(ctx: &JobContext, spec: &PluginWorkSpec) -> JobRunOutcome {
    let (install, config) = match admit_work(ctx, spec).await {
        Ok(admitted) => admitted,
        Err(outcome) => return outcome,
    };
    match &spec.action {
        PluginWorkAction::Copy { .. } => copy_work(ctx, spec, (&install, &config)).await,
    }
}

/// The install, account and binding checks of one attempt, before any grant check.
async fn admit_work(
    ctx: &JobContext,
    spec: &PluginWorkSpec,
) -> Result<(PluginInstall, RealmConfigDocument), JobRunOutcome> {
    let deferred = |message: String| JobRunOutcome::Deferred(JobError::retryable(message));
    let config = read_config(&ctx.driver, spec.realm_id).await;
    let config = config.map_err(|error| deferred(error.to_string()))?;
    let row = read_work(&ctx.driver, spec.install_id, spec.work_id).await;
    let row = row.map_err(|error| deferred(error.to_string()))?;
    let Some(row) = row else {
        return Err(permanent("the plugin work is no longer recorded"));
    };
    let user = read_user(&ctx.driver, row.account)
        .await
        .map_err(deferred)?;
    let place = (&config, spec.realm_id, ctx.owner_node_id);
    let binding = (spec.install_id, row.binding.digest.as_str());
    let install = check_install(place, binding, user.as_ref()).map_err(refused)?;
    if install.account != row.account {
        return Err(permanent("the plugin work belongs to an earlier install"));
    }
    if install.paused {
        let message = JobError::retryable("the install is paused");
        return Err(JobRunOutcome::DeferredFor(message, PAUSED_RETRY));
    }
    Ok((install.clone(), config))
}

/// Checks `permission` on `target` with the current grants, as a plugin call does.
async fn granted(
    ctx: &JobContext,
    install: &PluginInstall,
    (permission, target): (Permission, &str),
) -> Result<BTreeSet<String>, JobRunOutcome> {
    let patterns = matching(install, permission, target).map_err(refused)?;
    let authorized = authorize_call(&ctx.driver, install, permission, target, &patterns).await;
    authorized.map_err(refused)?;
    Ok(patterns)
}

/// Copies one object as the account, restricted to its current `objects.copy` patterns.
async fn copy_work(
    ctx: &JobContext,
    spec: &PluginWorkSpec,
    (install, config): (&PluginInstall, &RealmConfigDocument),
) -> JobRunOutcome {
    let PluginWorkAction::Copy {
        source,
        source_version,
        target,
        ..
    } = &spec.action;
    let mut patterns = BTreeSet::new();
    for path in [source, target] {
        match granted(ctx, install, (Permission::ObjectsCopy, path)).await {
            Ok(found) => patterns.extend(found),
            Err(outcome) => return outcome,
        }
    }
    let place = (spec.realm_id, ctx.owner_node_id);
    let (source_bucket, source_key) = match local_object(&ctx.driver, place, install, source).await
    {
        Ok(found) => found,
        Err(error) => return refused(error),
    };
    let (dest_bucket, dest_key) = match local_object(&ctx.driver, place, install, target).await {
        Ok(found) => found,
        Err(error) => return refused(error),
    };
    let auth = account(install, &patterns);
    let input = CopyObjectInput {
        source_bucket,
        source_key,
        source_version_id: *source_version,
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
    let copy = copy_object_tracked(&ctx.driver, input, Some(pulled.clone()));
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
        Err(error) => JobRunOutcome::Failed(super::copy::copy_error(error)),
    }
}

/// The install's account restricted to `patterns` with WRITE, which also reads.
fn account(install: &PluginInstall, patterns: &BTreeSet<String>) -> AuthContext {
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
fn refused(error: HostError) -> JobRunOutcome {
    match error.kind {
        ErrorKind::Unavailable => JobRunOutcome::Deferred(JobError::retryable(error.message)),
        _ => permanent(error.message),
    }
}

fn permanent(message: impl Into<String>) -> JobRunOutcome {
    JobRunOutcome::Failed(JobError::permanent(message.into()))
}

#[cfg(test)]
#[path = "plugin_work_tests.rs"]
mod tests;
