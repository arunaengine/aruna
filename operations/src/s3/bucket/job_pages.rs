//! Reads the job of a bucket and runs running key re-mapping and duplicate cleanup jobs page
//! by page.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::effects::StorageEffect;
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{LINKED_JOB_KEYSPACE, RENAME_TOTAL_KEYSPACE};
use aruna_core::structs::identity::auth::Permission;
use aruna_core::structs::storage::backend_import::{RenameRequest, RenameTotal};
use aruna_core::structs::storage::format::CompressionMigration;
use aruna_core::structs::storage::linked_job::{LinkedJob, LinkedState, LinkedTask};
use aruna_core::types::{GroupId, Key};
use tracing::warn;

use super::linked::{BucketJob, reads};
use crate::auth::request_authorization::{AuthorizeError, BucketAccess};
use crate::driver::DriverContext;
use crate::s3::bucket::remap::{RemapScope, job_page};

/// Delay between pages of a running job, and before a waiting job tries again.
const CONTINUE: Duration = Duration::from_secs(1);
const WAIT: Duration = Duration::from_secs(30);
const JOB_PAGE: usize = 64;
const STORE_TRIES: usize = 3;

/// The job of a bucket as stored: the running one, else the one started last.
pub async fn read_bucket_job(
    context: &DriverContext,
    bucket: &str,
) -> Result<Option<BucketJob>, String> {
    let mut reads = reads(bucket);
    reads.remove(0);
    reads.remove(0);
    let read = StorageEffect::BatchRead {
        reads,
        txn_id: None,
    };
    let values = match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::BatchReadResult { values }) => values,
        other => return Err(format!("could not read the bucket job: {}", other.label())),
    };
    let mut values = values.into_iter().map(|(_, value)| value);
    let mut next = || values.next().flatten();
    let (linked, migration, rename) = (next(), next(), next());
    let linked = linked.as_deref().map(LinkedJob::from_bytes).transpose();
    let linked = linked.map_err(|error| error.to_string())?;
    let migration = migration.as_deref().map(CompressionMigration::from_bytes);
    let migration = migration.transpose().map_err(|error| error.to_string())?;
    let rename = rename.as_deref().map(RenameRequest::from_bytes).transpose();
    let rename = rename.map_err(|error| error.to_string())?;
    let rename = match (migration, rename) {
        (Some(migration), Some(request))
            if migration.moving && migration.started_at_ms == request.started_at_ms =>
        {
            Some((migration, request))
        }
        _ => None,
    };
    let rename_runs = (rename.as_ref()).is_some_and(|(migration, request)| {
        migration.finished_at_ms.is_none() && request.aborted_at_ms.is_none()
    });
    Ok(match (linked, rename) {
        (Some(job), _) if job.is_active() => Some(BucketJob::Linked(job)),
        (_, Some((migration, request))) if rename_runs => {
            Some(BucketJob::Rename(migration, request))
        }
        (Some(job), Some((migration, request))) => {
            match job.started_at_ms > migration.started_at_ms {
                true => Some(BucketJob::Linked(job)),
                false => Some(BucketJob::Rename(migration, request)),
            }
        }
        (Some(job), None) => Some(BucketJob::Linked(job)),
        (None, Some((migration, request))) => Some(BucketJob::Rename(migration, request)),
        (None, None) => None,
    })
}

/// The files the rename of the move started at `started_at_ms` planned to rename; `None` when
/// none was stored for that move.
pub async fn rename_total(
    context: &DriverContext,
    bucket: &str,
    started_at_ms: u64,
) -> Result<Option<u64>, String> {
    let read = StorageEffect::Read {
        key_space: RENAME_TOTAL_KEYSPACE.to_string(),
        key: bucket.as_bytes().to_vec().into(),
        txn_id: None,
    };
    let Event::Storage(StorageEvent::ReadResult { value, .. }) =
        context.storage_handle.send_storage_effect(read).await
    else {
        return Err("could not read the rename total".to_string());
    };
    let row = value.as_deref().map(RenameTotal::from_bytes).transpose();
    let row = row.map_err(|error| error.to_string())?;
    Ok(row.and_then(|row| row.for_move(started_at_ms)))
}

/// What one job page did; a waiting page keeps its cursor.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct JobStep {
    pub cursor: Option<String>,
    pub done: u64,
    pub skipped: u64,
    pub failed: u64,
    pub finished: bool,
    pub waiting: Option<String>,
}

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
            let now = crate::effect_adapters::routing::now_ms();
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
async fn job_access(
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
        Err(AuthorizeError::Policy(
            crate::auth::request_policy::PolicyEnforcementError::Denied { .. },
        )) => Ok(None),
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
    use crate::auth::request_policy::{PolicyEnforcementError, PolicyRequestExtras};
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
async fn store(
    context: &DriverContext,
    bucket: &str,
    job: &LinkedJob,
    step: JobStep,
    state: Option<LinkedState>,
) -> Result<(), String> {
    for _ in 0..STORE_TRIES {
        if store_once(context, bucket, job, step.clone(), state).await? {
            return Ok(());
        }
    }
    warn!(bucket, "The progress of a bucket job could not be stored");
    Ok(())
}

/// One try of `store`; `false` when a change of the job committed first.
async fn store_once(
    context: &DriverContext,
    bucket: &str,
    job: &LinkedJob,
    step: JobStep,
    state: Option<LinkedState>,
) -> Result<bool, String> {
    let storage = &context.storage_handle;
    let key: Key = bucket.as_bytes().to_vec().into();
    let txn_id = match storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        other => {
            return Err(format!(
                "could not start a job transaction: {}",
                other.label()
            ));
        }
    };
    let read = StorageEffect::Read {
        key_space: LINKED_JOB_KEYSPACE.to_string(),
        key: key.clone(),
        txn_id: Some(txn_id),
    };
    let stored = match storage.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
        other => {
            storage
                .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                .await;
            return Err(format!("could not read the job: {}", other.label()));
        }
    };
    let stored = stored.as_deref().map(LinkedJob::from_bytes).transpose();
    let mut current = match stored {
        Ok(Some(current)) if current.id == job.id => current,
        other => {
            storage
                .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                .await;
            return other.map(|_| true).map_err(|error| error.to_string());
        }
    };
    let now = crate::effect_adapters::routing::now_ms();
    current.cursor = step.cursor;
    current.done += step.done;
    current.skipped += step.skipped;
    current.failed += step.failed;
    current.waiting = step.waiting;
    current.updated_at_ms = now;
    let next = state.or((step.finished).then_some(LinkedState::Finished));
    if let (LinkedState::Running, Some(next)) = (current.state, next) {
        current.state = next;
        current.finished_at_ms = Some(now);
    }
    let value = current.to_bytes().map_err(|error| error.to_string())?;
    let write = StorageEffect::Write {
        key_space: LINKED_JOB_KEYSPACE.to_string(),
        key,
        value: value.into(),
        txn_id: Some(txn_id),
    };
    if !matches!(
        storage.send_storage_effect(write).await,
        Event::Storage(StorageEvent::WriteResult { .. })
    ) {
        storage
            .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
            .await;
        return Err("could not write the job".to_string());
    }
    match storage
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(true),
        // A pause or an abort committed first; the progress goes onto it on the next try.
        Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionConflict,
        }) => Ok(false),
        other => Err(format!("could not store the job: {}", other.label())),
    }
}
