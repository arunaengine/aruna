//! Runs running key re-mapping and duplicate cleanup jobs of buckets page by page.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::keyspaces::LINKED_JOB_KEYSPACE;
use aruna_core::structs::identity::auth::Permission;
use aruna_core::structs::storage::linked_job::{LinkedJob, LinkedState, LinkedTask};
use aruna_core::types::GroupId;
use tracing::warn;

use super::job_progress::{JobStep, StoreProgressError, StoreProgressOperation};
use crate::auth::error::AuthorizeError;
use crate::auth::request_authorization::BucketAccess;
use crate::driver::{DriverContext, drive_retry};
use crate::s3::bucket::remap::{RemapJobOperation, RemapScope};

/// Delay between pages of a running job, and before a waiting job tries again.
const CONTINUE: Duration = Duration::from_secs(1);
const WAIT: Duration = Duration::from_secs(30);
const JOB_PAGE: usize = 64;

/// Runs one page of every running linked job; answers when the worker must run again.
pub async fn process_linked(context: &DriverContext) -> Result<Option<Duration>, String> {
    let mut next = None::<Duration>;
    let mut after = None;
    loop {
        let (rows, more) = crate::storage_read::iter_prefix_page(
            &context.storage_handle,
            LINKED_JOB_KEYSPACE,
            None,
            after.take(),
            JOB_PAGE,
            None,
        )
        .await?;
        for (key, value) in rows {
            let job = LinkedJob::from_bytes(&value).map_err(|error| error.to_string())?;
            if job.state != LinkedState::Running {
                continue;
            }
            let bucket = String::from_utf8(key.to_vec()).map_err(|error| error.to_string())?;
            let now = aruna_core::time::unix_timestamp_millis();
            let due = job.updated_at_ms + WAIT.as_millis() as u64;
            let after = match job.waiting.is_some() && now < due {
                true => Duration::from_millis(due - now),
                false => run_page(context, &bucket, job).await?,
            };
            next = Some(next.map_or(after, |next| next.min(after)));
        }
        match more {
            Some(more) => after = Some(more),
            None => return Ok(next),
        }
    }
}

/// One page of `job`, stored unless another job replaced it meanwhile.
async fn run_page(
    context: &DriverContext,
    bucket: &str,
    job: LinkedJob,
) -> Result<Duration, String> {
    let step = match job_access(context, bucket, &job).await {
        Ok(Some(access)) => page(context, bucket, &job, &access).await,
        Ok(None) => {
            let ended = Some(LinkedState::Aborted);
            return store(context, bucket, &job, ended_step(), ended)
                .await
                .map(|_| CONTINUE);
        }
        Err(error) => JobStep {
            cursor: job.cursor.clone(),
            waiting: Some(error),
            ..JobStep::default()
        },
    };
    let wait = step.waiting.is_some();
    store(context, bucket, &job, step, None).await?;
    Ok(if wait { WAIT } else { CONTINUE })
}

fn ended_step() -> JobStep {
    JobStep {
        waiting: Some("the caller's token or access ended".to_string()),
        ..JobStep::default()
    }
}

/// The caller's access to the bucket for one page; `None` once its token or access ended.
pub(super) async fn job_access(
    context: &DriverContext,
    bucket: &str,
    job: &LinkedJob,
) -> Result<Option<BucketAccess>, String> {
    let valid = crate::auth::bearer_token::grant_valid(
        &context.storage_handle,
        &job.grant,
        &job.auth.user_id,
    );
    if !valid.await.map_err(|error| error.to_string())? {
        return Ok(None);
    }
    if !still_admin(context, &job.auth, job.group_id).await? {
        return Ok(None);
    }
    let scope = (job.auth.realm_id, job.group_id, job.node_id);
    match BucketAccess::load(context, &job.auth, scope, bucket).await {
        Ok(access) if access.allows(None, &Permission::WRITE) => Ok(Some(access)),
        Ok(_) | Err(AuthorizeError::PermissionDenied) => Ok(None),
        Err(AuthorizeError::Policy(crate::auth::error::PolicyEnforcementError::Denied {
            ..
        })) => Ok(None),
        Err(error) => Err(error.to_string()),
    }
}

/// Whether `auth` still administers `group_id`, as starting a bucket job required; a denial
/// is a verdict, unreadable state an error to retry.
pub(crate) async fn still_admin(
    context: &DriverContext,
    auth: &aruna_core::structs::identity::auth::AuthContext,
    group_id: GroupId,
) -> Result<bool, String> {
    use crate::auth::error::PolicyEnforcementError;
    use crate::auth::request_policy::PolicyRequestExtras;
    let path = format!("/{}/g/{group_id}/admin/**", auth.realm_id);
    let checked = crate::auth::request_authorization::authorize(
        context,
        auth.realm_id,
        auth,
        &path,
        &Permission::WRITE,
        PolicyRequestExtras::rest(),
    );
    match checked.await {
        Ok(_) => Ok(true),
        Err(AuthorizeError::PermissionDenied)
        | Err(AuthorizeError::Policy(PolicyEnforcementError::Denied { .. })) => Ok(false),
        Err(error) => Err(error.to_string()),
    }
}

async fn page(
    context: &DriverContext,
    bucket: &str,
    job: &LinkedJob,
    access: &BucketAccess,
) -> JobStep {
    let waiting = |error: String| JobStep {
        cursor: job.cursor.clone(),
        waiting: Some(error),
        ..JobStep::default()
    };
    match &job.task {
        LinkedTask::Remap(plan) => {
            let scope = RemapScope {
                realm_id: job.auth.realm_id,
                group_id: job.group_id,
                node_id: job.node_id,
                bucket,
                access,
            };
            match job_page(context, plan, &scope, job).await {
                Ok(step) => step,
                Err(error) => waiting(error.to_string()),
            }
        }
        LinkedTask::Unify { prefix } => {
            let page = crate::blob::unify::job_page(context, (bucket, prefix), access, job);
            match page.await {
                Ok(step) => step,
                Err(error) => waiting(error),
            }
        }
    }
}

/// Stores a page's progress on the job it ran for; a pause or an abort meanwhile stays.
pub(super) async fn store(
    context: &DriverContext,
    bucket: &str,
    job: &LinkedJob,
    step: JobStep,
    state: Option<LinkedState>,
) -> Result<(), String> {
    let build = || {
        let now_ms = aruna_core::time::unix_timestamp_millis();
        StoreProgressOperation::new(bucket, job.id, step.clone(), state, now_ms)
    };
    match drive_retry(build, context, bucket.as_bytes()).await {
        Ok(()) => Ok(()),
        Err(StoreProgressError::Conflict) => {
            warn!(bucket, "The progress of a bucket job could not be stored");
            Ok(())
        }
        Err(error) => Err(error.to_string()),
    }
}
