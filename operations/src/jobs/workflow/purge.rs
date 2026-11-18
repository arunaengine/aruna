//! Runs the storage purge job that deletes object versions and aborts multipart uploads.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{BUCKET_CREATION_KEYSPACE, S3_BUCKET_KEYSPACE};
use aruna_core::structs::execution::job::{JobError, JobProgress, JobResultPayload};
use aruna_core::structs::identity::auth::Permission;
use aruna_core::structs::storage::blob::object_permission_path;
use aruna_core::structs::storage::blob::{BucketInfo, incarnation};
use aruna_core::structs::storage::multipart::MultipartUpload;
use aruna_core::structs::storage::storage_purge::{
    StoragePurgeCheckpoint, StoragePurgeResult, StoragePurgeScope, StoragePurgeSpec,
};
use aruna_core::time::unix_timestamp_millis;

use super::super::executor::{JobContext, JobRunOutcome};
use super::super::store::state::flush_progress;
use super::super::store::{put_purge_checkpoint, read_purge_checkpoint};
use super::purge_audit::{WriteAuditOperation, purge_audit_record};
use crate::auth::error::AuthorizeError;
use crate::auth::error::PolicyEnforcementError;
use crate::auth::request_authorization::authorize;
use crate::auth::request_policy::PolicyRequestExtras;
use crate::driver::drive_retry;
use crate::driver::{DriverContext, drive};
use crate::s3::bucket::delete::{DeleteBucketError, DeleteBucketOperation};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use crate::s3::bucket::release::BucketScope;
use crate::s3::fence_store::AcquireFenceOperation;
use crate::s3::multipart::abort::{AbortUploadError, AbortUploadInput, AbortUploadOperation};
use crate::s3::multipart::uploads::{ListUploadsInput, ListUploadsOperation};
use crate::s3::object::delete::DeleteObjectError;
use crate::s3::object::delete::bulk::{BulkDeleteEntry, BulkDeleteInput, delete_objects};
use crate::s3::object::delete::pinned::{PinnedError, ReleaseGuards, version_pins};
use crate::s3::object::versions::{ListVersionsInput, ListVersionsItem, ListVersionsOperation};
use crate::s3::purge_fence::PurgeFenceError;
use aruna_core::structs::storage::release::MembershipQuery;

const PURGE_BATCH_SIZE: usize = 1_000;
/// How often a paused purge looks whether it was resumed.
const PAUSE_POLL: std::time::Duration = std::time::Duration::from_secs(2);

/// Runs a purge; with `files` it also releases the linked-storage files of the versions.
pub async fn run_storage_purge(
    ctx: &JobContext,
    spec: &StoragePurgeSpec,
    files: bool,
) -> JobRunOutcome {
    let acquire = || AcquireFenceOperation::new(ctx.job_id, spec.scope.clone());
    let seed = ctx.job_id.to_bytes();
    if let Err(error) = drive_retry(acquire, &ctx.driver, &seed).await {
        return JobRunOutcome::Failed(fence_error(error));
    }
    // The planned files wait in the cleanup queue until the job has ended.
    let outcome = run_fenced_purge(ctx, spec, files).await;
    if !matches!(outcome, Err(PurgeRunError::Interrupted)) {
        let resume = crate::blob::pause::PausePurgeOperation::new(ctx.job_id.as_ulid(), false);
        if let Err(error) = drive(resume, &ctx.driver).await {
            tracing::warn!(%error, "A purge's pause mark could not be removed");
        }
    }
    match outcome {
        Ok(result) => JobRunOutcome::Succeeded(JobResultPayload::StoragePurge(result)),
        Err(PurgeRunError::Cancelled) => JobRunOutcome::Cancelled,
        Err(PurgeRunError::Interrupted) => JobRunOutcome::Interrupted,
        Err(PurgeRunError::Job(error)) => JobRunOutcome::Failed(error),
    }
}

enum PurgeRunError {
    Cancelled,
    Interrupted,
    Job(JobError),
}

impl From<JobError> for PurgeRunError {
    fn from(error: JobError) -> Self {
        Self::Job(error)
    }
}

async fn run_fenced_purge(
    ctx: &JobContext,
    spec: &StoragePurgeSpec,
    files: bool,
) -> Result<StoragePurgeResult, PurgeRunError> {
    check_stop(ctx)?;
    // The trail is published only for the bucket incarnation the purge began on.
    let mut incarnation = None;
    let mut bucket_info = None;
    let bucket_exists = match drive(
        GetBucketOperation::new(spec.scope.bucket().to_string()),
        &ctx.driver,
    )
    .await
    {
        // A member bucket of a distributed bucket is purged only by deleting its set.
        Ok(info) if info.distribution.is_some() => {
            return Err(JobError::permanent(
                "purge bucket is a member of a distributed bucket; delete the distributed bucket",
            )
            .into());
        }
        Ok(info) if info.group_id == spec.group_id => {
            incarnation = read_incarnation(&ctx.driver, spec.scope.bucket())
                .await
                .map_err(|event| {
                    JobError::retryable(format!("purge bucket read failed: {}", event.label()))
                })?;
            bucket_info = Some(info);
            true
        }
        Ok(_) => {
            return Err(JobError::permanent("purge bucket is outside the authorized group").into());
        }
        Err(GetBucketError::NotFound) => false,
        Err(error) => {
            return Err(JobError::retryable(format!("purge bucket read failed: {error}")).into());
        }
    };

    let mut checkpoint = match read_purge_checkpoint(&ctx.driver, ctx.job_id).await {
        Ok(Some(checkpoint)) => checkpoint,
        Ok(None) => {
            let initial_versions = count_versions(ctx, &spec.scope).await?;
            let initial_multipart_uploads = count_multipart(ctx, &spec.scope).await?;
            let checkpoint = StoragePurgeCheckpoint {
                initial_versions,
                initial_multipart_uploads,
                batches_completed: 0,
            };
            put_purge_checkpoint(&ctx.driver, ctx.job_id, ctx.claim_token, &checkpoint)
                .await
                .map_err(|error| {
                    JobError::retryable(format!("purge checkpoint write failed: {error}"))
                })?;
            checkpoint
        }
        Err(error) => {
            return Err(
                JobError::retryable(format!("purge checkpoint read failed: {error}")).into(),
            );
        }
    };

    let total = checkpoint
        .initial_versions
        .saturating_add(checkpoint.initial_multipart_uploads)
        .saturating_add(u64::from(spec.scope.is_bucket()));
    ctx.progress.set_total(total);
    let remaining_versions = count_versions(ctx, &spec.scope).await?;
    let remaining_uploads = count_multipart(ctx, &spec.scope).await?;
    let remaining_bucket = u64::from(spec.scope.is_bucket() && bucket_exists);
    ctx.progress.set_current(
        total.saturating_sub(
            remaining_versions
                .saturating_add(remaining_uploads)
                .saturating_add(remaining_bucket),
        ),
    );
    persist_progress(ctx).await?;

    if let Some(info) = &bucket_info {
        abort_uploads(ctx, spec, &mut checkpoint).await?;
        delete_versions(ctx, (spec, files), info, &mut checkpoint).await?;
        final_relist(ctx, &spec.scope).await?;
    }

    let bucket_deleted = if spec.scope.is_bucket() {
        if bucket_exists {
            match drive(
                DeleteBucketOperation::new(spec.scope.bucket().to_string())
                    .purge_notices(ctx.driver.node_and_realm()),
                &ctx.driver,
            )
            .await
            {
                Ok(()) | Err(DeleteBucketError::NotFound) => {}
                Err(error) => {
                    return Err(JobError::retryable(format!(
                        "purge bucket delete failed: {error}"
                    ))
                    .into());
                }
            }
            checkpoint.batches_completed = checkpoint.batches_completed.saturating_add(1);
            persist_batch(ctx, &checkpoint, 1).await?;
        } else {
            ctx.progress.set_current(total);
            persist_progress(ctx).await?;
        }
        prove_bucket_absent(ctx, spec.scope.bucket()).await?;
        true
    } else {
        false
    };

    let incarnation = incarnation.filter(|_| !bucket_deleted);
    write_purge_audit(ctx, spec, incarnation).await?;
    ctx.progress.set_current(total);
    Ok(StoragePurgeResult {
        scope: spec.scope.clone(),
        versions_removed: checkpoint.initial_versions,
        multipart_uploads_removed: checkpoint.initial_multipart_uploads,
        batches_completed: checkpoint.batches_completed,
        bucket_deleted,
        emptiness_proven: true,
    })
}

/// The incarnation of `bucket` while it exists, from its row and its creation row.
async fn read_incarnation(context: &DriverContext, bucket: &str) -> Result<Option<u128>, Event> {
    let key: aruna_core::types::Key = bucket.as_bytes().to_vec().into();
    let read = StorageEffect::BatchRead {
        reads: vec![
            (S3_BUCKET_KEYSPACE.to_string(), key.clone()),
            (BUCKET_CREATION_KEYSPACE.to_string(), key),
        ],
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::BatchReadResult { values }) if values.len() == 2 => {
            let (bucket, creation) = (&values[0].1, &values[1].1);
            let creation = creation.as_ref().map(|value| value.as_ref());
            Ok(bucket.as_ref().map(|_| incarnation(creation)))
        }
        other => Err(other),
    }
}

/// Records the completed purge once; see [`WriteAuditOperation`].
async fn write_purge_audit(
    ctx: &JobContext,
    spec: &StoragePurgeSpec,
    incarnation: Option<u128>,
) -> Result<(), PurgeRunError> {
    let record = purge_audit_record(spec, unix_timestamp_millis());
    let operation = WriteAuditOperation::new(record, ctx.job_id.as_ulid(), incarnation)
        .map_err(|error| JobError::retryable(format!("purge audit encode failed: {error}")))?;
    drive(operation, &ctx.driver)
        .await
        .map_err(|error| JobError::retryable(format!("purge audit write failed: {error}")).into())
}

async fn abort_uploads(
    ctx: &JobContext,
    spec: &StoragePurgeSpec,
    checkpoint: &mut StoragePurgeCheckpoint,
) -> Result<(), PurgeRunError> {
    loop {
        check_stop(ctx)?;
        wait_paused(ctx).await?;
        let page = list_multipart_page(ctx, &spec.scope, PURGE_BATCH_SIZE).await?;
        if page.is_empty() {
            return Ok(());
        }
        let mut removed = 0u64;
        for upload in page {
            check_stop(ctx)?;
            authorize_object(ctx, spec, &upload.key).await?;
            match drive(
                AbortUploadOperation::new(AbortUploadInput {
                    bucket: upload.bucket,
                    key: upload.key,
                    upload_id: upload.upload_id,
                    now_ms: aruna_core::time::unix_timestamp_millis(),
                })
                .including_in_progress(),
                &ctx.driver,
            )
            .await
            {
                Ok(()) | Err(AbortUploadError::NoSuchUpload) => removed += 1,
                Err(AbortUploadError::UploadNotOpen) => {
                    return Err(JobError::retryable(
                        "matching multipart upload changed state during purge",
                    )
                    .into());
                }
                Err(error) => {
                    return Err(
                        JobError::retryable(format!("multipart abort failed: {error}")).into(),
                    );
                }
            }
        }
        checkpoint.batches_completed = checkpoint.batches_completed.saturating_add(1);
        persist_batch(ctx, checkpoint, removed).await?;
    }
}

async fn delete_versions(
    ctx: &JobContext,
    (spec, files): (&StoragePurgeSpec, bool),
    info: &BucketInfo,
    checkpoint: &mut StoragePurgeCheckpoint,
) -> Result<(), PurgeRunError> {
    let guards = release_guards(ctx, spec, info).await?;
    loop {
        check_stop(ctx)?;
        wait_paused(ctx).await?;
        let page = list_version_page(ctx, &spec.scope, PURGE_BATCH_SIZE, None, None).await?;
        if page.items.is_empty() {
            return Ok(());
        }
        let entries = page
            .items
            .into_iter()
            .map(|item| match item {
                ListVersionsItem::Version {
                    key, version_id, ..
                }
                | ListVersionsItem::DeleteMarker {
                    key, version_id, ..
                } => BulkDeleteEntry {
                    key,
                    version_id: Some(version_id),
                },
            })
            .collect::<Vec<_>>();
        for key in entries
            .iter()
            .map(|entry| entry.key.as_str())
            .collect::<BTreeSet<_>>()
        {
            authorize_object(ctx, spec, key).await?;
        }
        let indexes = match files {
            true => plan_files(ctx, spec, &entries).await?,
            false => Vec::new(),
        };
        let batch_len = entries.len() as u64;
        let outcomes = delete_objects(
            &ctx.driver,
            BulkDeleteInput {
                bucket: spec.scope.bucket().to_string(),
                entries,
                group_id: spec.group_id,
                realm_id: spec.auth_context.realm_id,
                node_id: spec.node_id,
                deleted_by: spec.auth_context.user_id,
                restrictions: spec.auth_context.path_restrictions.clone(),
                guards: guards.clone(),
            },
        )
        .await;
        if let Some(error) = outcomes
            .into_iter()
            .find_map(|outcome| match outcome.result {
                Ok(_) | Err(DeleteObjectError::NoSuchVersion) => None,
                Err(error) => Some(error),
            })
        {
            return Err(
                JobError::retryable(format!("versioned purge delete failed: {error}")).into(),
            );
        }
        drop_indexes(ctx, indexes).await?;
        checkpoint.batches_completed = checkpoint.batches_completed.saturating_add(1);
        persist_batch(ctx, checkpoint, batch_len).await?;
    }
}

/// Refuses the purge while a release pins any version in its scope, also one published after the
/// purge was submitted; else the guards of the first check, which every delete keeps.
async fn release_guards(
    ctx: &JobContext,
    spec: &StoragePurgeSpec,
    info: &BucketInfo,
) -> Result<ReleaseGuards, PurgeRunError> {
    let scope = BucketScope {
        realm_id: spec.auth_context.realm_id,
        node_id: spec.node_id,
        bucket: spec.scope.bucket(),
        info,
    };
    let (mut guards, mut key_marker, mut version_marker) = (None, None, None);
    loop {
        check_stop(ctx)?;
        let page = list_version_page(
            ctx,
            &spec.scope,
            PURGE_BATCH_SIZE,
            key_marker,
            version_marker,
        )
        .await?;
        let queries: Vec<MembershipQuery> = (page.items.iter())
            .map(|item| match item {
                ListVersionsItem::Version {
                    key, version_id, ..
                }
                | ListVersionsItem::DeleteMarker {
                    key, version_id, ..
                } => MembershipQuery {
                    key: key.clone(),
                    version_id: *version_id,
                },
            })
            .collect();
        match version_pins(&ctx.driver, scope, &spec.auth_context, &queries).await {
            Ok((pins, _)) if pins.contains(&true) => {
                return Err(JobError::permanent(PinnedError::Scope.to_string()).into());
            }
            Ok((_, read)) => guards = guards.or(Some(read)),
            Err(error) => return Err(JobError::retryable(error.to_string()).into()),
        }
        if !page.is_truncated {
            return Ok(guards.unwrap_or_default());
        }
        key_marker = page.next_key_marker;
        version_marker = page.next_version_marker;
        if key_marker.is_none() {
            return Err(JobError::retryable("version inventory truncated without a cursor").into());
        }
    }
}

async fn authorize_object(
    ctx: &JobContext,
    spec: &StoragePurgeSpec,
    key: &str,
) -> Result<(), PurgeRunError> {
    let path = object_permission_path(
        spec.auth_context.realm_id,
        spec.group_id,
        spec.node_id,
        spec.scope.bucket(),
        key,
    );
    authorize(
        &ctx.driver,
        spec.auth_context.realm_id,
        &spec.auth_context,
        &path,
        &Permission::WRITE,
        PolicyRequestExtras::rest(),
    )
    .await
    .map_err(|error| match error {
        AuthorizeError::PermissionDenied
        | AuthorizeError::Policy(PolicyEnforcementError::Denied { .. }) => {
            JobError::permanent(format!("purge object authorization failed: {error}"))
        }
        AuthorizeError::CheckFailed(_)
        | AuthorizeError::Storage(_)
        | AuthorizeError::Policy(PolicyEnforcementError::Unavailable(_)) => {
            JobError::retryable(format!("purge object authorization failed: {error}"))
        }
    })?;
    Ok(())
}

async fn final_relist(ctx: &JobContext, scope: &StoragePurgeScope) -> Result<(), PurgeRunError> {
    check_stop(ctx)?;
    if !list_version_page(ctx, scope, 1, None, None)
        .await?
        .items
        .is_empty()
    {
        return Err(JobError::retryable("final purge version re-list was not empty").into());
    }
    if !list_multipart_page(ctx, scope, 1).await?.is_empty() {
        return Err(JobError::retryable("final purge multipart re-list was not empty").into());
    }
    Ok(())
}

async fn prove_bucket_absent(ctx: &JobContext, bucket: &str) -> Result<(), PurgeRunError> {
    match drive(GetBucketOperation::new(bucket.to_string()), &ctx.driver).await {
        Err(GetBucketError::NotFound) => Ok(()),
        Ok(_) => Err(JobError::retryable("bucket still exists after purge").into()),
        Err(error) => {
            Err(JobError::retryable(format!("bucket emptiness proof failed: {error}")).into())
        }
    }
}

async fn count_versions(ctx: &JobContext, scope: &StoragePurgeScope) -> Result<u64, PurgeRunError> {
    let mut key_marker = None;
    let mut version_marker = None;
    let mut total = 0u64;
    loop {
        check_stop(ctx)?;
        let page =
            list_version_page(ctx, scope, PURGE_BATCH_SIZE, key_marker, version_marker).await?;
        total = total.saturating_add(page.items.len() as u64);
        if !page.is_truncated {
            return Ok(total);
        }
        key_marker = page.next_key_marker;
        version_marker = page.next_version_marker;
        if key_marker.is_none() {
            return Err(JobError::retryable("version inventory truncated without a cursor").into());
        }
    }
}

async fn count_multipart(
    ctx: &JobContext,
    scope: &StoragePurgeScope,
) -> Result<u64, PurgeRunError> {
    let mut key_marker = None;
    let mut upload_marker = None;
    let mut total = 0u64;
    loop {
        check_stop(ctx)?;
        let page =
            list_multipart_cursor(ctx, scope, PURGE_BATCH_SIZE, key_marker, upload_marker).await?;
        total = total.saturating_add(page.uploads.len() as u64);
        if !page.is_truncated {
            return Ok(total);
        }
        key_marker = page.next_key_marker;
        upload_marker = page.next_upload_marker;
        if key_marker.is_none() {
            return Err(
                JobError::retryable("multipart inventory truncated without a cursor").into(),
            );
        }
    }
}

struct VersionPage {
    items: Vec<ListVersionsItem>,
    is_truncated: bool,
    next_key_marker: Option<String>,
    next_version_marker: Option<ulid::Ulid>,
}

async fn list_version_page(
    ctx: &JobContext,
    scope: &StoragePurgeScope,
    limit: usize,
    key_marker: Option<String>,
    version_id_marker: Option<ulid::Ulid>,
) -> Result<VersionPage, PurgeRunError> {
    let result = drive(
        ListVersionsOperation::new(ListVersionsInput {
            bucket: scope.bucket().to_string(),
            prefix: scope.list_prefix().map(str::to_string),
            delimiter: None,
            key_marker,
            version_id_marker,
            max_keys: Some(limit),
        }),
        &ctx.driver,
    )
    .await
    .map_err(|error| JobError::retryable(format!("purge version list failed: {error}")))?;

    let mut items = result.items;
    let mut is_truncated = result.is_truncated;
    let mut next_key_marker = result.next_key_marker;
    let mut next_version_marker = result.next_version_marker;
    if let StoragePurgeScope::File { key, .. } = scope {
        items.retain(|item| match item {
            ListVersionsItem::Version { key: item, .. }
            | ListVersionsItem::DeleteMarker { key: item, .. } => item.as_str() == key.as_str(),
        });
        if next_key_marker.as_deref() != Some(key.as_str()) {
            is_truncated = false;
            next_key_marker = None;
            next_version_marker = None;
        }
    }
    Ok(VersionPage {
        items,
        is_truncated,
        next_key_marker,
        next_version_marker,
    })
}

async fn list_multipart_page(
    ctx: &JobContext,
    scope: &StoragePurgeScope,
    limit: usize,
) -> Result<Vec<MultipartUpload>, PurgeRunError> {
    Ok(list_multipart_cursor(ctx, scope, limit, None, None)
        .await?
        .uploads)
}

struct MultipartPage {
    uploads: Vec<MultipartUpload>,
    is_truncated: bool,
    next_key_marker: Option<String>,
    next_upload_marker: Option<ulid::Ulid>,
}

async fn list_multipart_cursor(
    ctx: &JobContext,
    scope: &StoragePurgeScope,
    limit: usize,
    key_marker: Option<String>,
    upload_id_marker: Option<ulid::Ulid>,
) -> Result<MultipartPage, PurgeRunError> {
    let result = drive(
        ListUploadsOperation::new(ListUploadsInput {
            bucket: scope.bucket().to_string(),
            prefix: scope.list_prefix().map(str::to_string),
            delimiter: None,
            key_marker,
            upload_id_marker,
            max_uploads: limit,
        })
        .including_in_progress()
        .complete_scan(),
        &ctx.driver,
    )
    .await
    .map_err(|error| JobError::retryable(format!("purge multipart list failed: {error}")))?;

    let mut uploads = result.uploads;
    let mut is_truncated = result.is_truncated;
    let mut next_key_marker = result.next_key_marker;
    let mut next_upload_marker = result.next_upload_marker;
    if let StoragePurgeScope::File { key, .. } = scope {
        uploads.retain(|upload| upload.key == *key);
        if next_key_marker.as_deref() != Some(key.as_str()) {
            is_truncated = false;
            next_key_marker = None;
            next_upload_marker = None;
        }
    }
    Ok(MultipartPage {
        uploads,
        is_truncated,
        next_key_marker,
        next_upload_marker,
    })
}

async fn persist_batch(
    ctx: &JobContext,
    checkpoint: &StoragePurgeCheckpoint,
    completed: u64,
) -> Result<(), PurgeRunError> {
    ctx.progress.advance(completed);
    persist_progress(ctx).await?;
    put_purge_checkpoint(&ctx.driver, ctx.job_id, ctx.claim_token, checkpoint)
        .await
        .map_err(|error| JobError::retryable(format!("purge checkpoint write failed: {error}")))?;
    Ok(())
}

async fn persist_progress(ctx: &JobContext) -> Result<(), PurgeRunError> {
    let progress: JobProgress = ctx.progress.snapshot();
    let renew = flush_progress(
        &ctx.driver,
        ctx.job_id,
        ctx.claim_token,
        progress,
        unix_timestamp_millis(),
    )
    .await
    .map_err(|error| JobError::retryable(format!("purge progress write failed: {error}")))?;
    if renew.cancel_requested {
        return Err(PurgeRunError::Cancelled);
    }
    Ok(())
}

/// Plans the release of the linked-storage files the entries read, before they go. Answers
/// the key index rows of the second copies among them.
async fn plan_files(
    ctx: &JobContext,
    spec: &StoragePurgeSpec,
    entries: &[BulkDeleteEntry],
) -> Result<Vec<aruna_core::types::Key>, PurgeRunError> {
    let versions: Vec<_> = entries
        .iter()
        .filter_map(|entry| Some((entry.key.clone(), entry.version_id?)))
        .collect();
    let scope = (spec.scope.bucket(), spec.group_id);
    let failed = |error: String| JobError::retryable(format!("purge file plan failed: {error}"));
    let found = crate::blob::purge::storage_files(&ctx.driver, scope, &versions, None)
        .await
        .map_err(failed)?;
    crate::blob::purge::plan_files(&ctx.driver, ctx.job_id.as_ulid(), &found)
        .await
        .map_err(failed)?;
    Ok(found.into_iter().filter_map(|file| file.index).collect())
}

/// Drops the key index rows of second copies whose versions are gone; their rows and files
/// go through the planned releases.
async fn drop_indexes(
    ctx: &JobContext,
    indexes: Vec<aruna_core::types::Key>,
) -> Result<(), PurgeRunError> {
    if indexes.is_empty() {
        return Ok(());
    }
    let deletes = (indexes.into_iter())
        .map(|key| (aruna_core::keyspaces::RENAMED_ROW_KEYSPACE.to_string(), key))
        .collect();
    let delete = StorageEffect::BatchDelete {
        deletes,
        txn_id: None,
    };
    match ctx.driver.storage_handle.send_storage_effect(delete).await {
        Event::Storage(StorageEvent::BatchDeleteResult { .. }) => Ok(()),
        other => {
            let error = format!("purge could not drop copy indexes: {}", other.label());
            Err(JobError::retryable(error).into())
        }
    }
}

/// Waits while the caller paused the purge; a cancel or a shutdown ends the wait.
async fn wait_paused(ctx: &JobContext) -> Result<(), PurgeRunError> {
    loop {
        let paused = crate::blob::pause::PausedOperation::new(ctx.job_id.as_ulid());
        let paused = drive(paused, &ctx.driver).await;
        match paused {
            Ok(false) => return Ok(()),
            Ok(true) => {}
            Err(error) => {
                return Err(
                    JobError::retryable(format!("purge pause read failed: {error}")).into(),
                );
            }
        }
        tokio::select! {
            () = ctx.cancel.cancelled() => {}
            () = ctx.shutdown.cancelled() => {}
            () = tokio::time::sleep(PAUSE_POLL) => {}
        }
        check_stop(ctx)?;
    }
}

fn check_stop(ctx: &JobContext) -> Result<(), PurgeRunError> {
    if ctx.cancel.is_cancelled() {
        Err(PurgeRunError::Cancelled)
    } else if ctx.shutdown.is_cancelled() {
        Err(PurgeRunError::Interrupted)
    } else {
        Ok(())
    }
}

fn fence_error(error: PurgeFenceError) -> JobError {
    match error {
        PurgeFenceError::Invalid => JobError::permanent(error.to_string()),
        PurgeFenceError::Suspended
        | PurgeFenceError::Busy
        | PurgeFenceError::Storage(_)
        | PurgeFenceError::Unexpected(_)
        | PurgeFenceError::Distribution(_) => JobError::retryable(error.to_string()),
    }
}
