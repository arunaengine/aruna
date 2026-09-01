use std::collections::BTreeSet;

use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::BLOB_DELETE_AUDIT_KEYSPACE;
use aruna_core::structs::{
    BlobDeleteAuditKind, BlobDeleteAuditRecord, BlobPurgeScopeKind, JobError, JobProgress,
    JobResultPayload, MultipartUpload, Permission, StoragePurgeCheckpoint, StoragePurgeResult,
    StoragePurgeScope, StoragePurgeSpec, blob_object_permission_path, delete_audit_key,
};
use aruna_core::util::unix_timestamp_millis;

use super::super::executor::{JobContext, JobRunOutcome};
use super::super::store::{flush_progress, put_purge_checkpoint, read_purge_checkpoint};
use crate::driver::drive;
use crate::request_authorization::{AuthorizeError, authorize};
use crate::request_policy::{PolicyEnforcementError, PolicyRequestExtras};
use crate::s3::abort_multipart_upload::{
    AbortMultipartUploadError, AbortMultipartUploadInput, AbortMultipartUploadOperation,
};
use crate::s3::delete_bucket::{DeleteBucketError, DeleteBucketOperation};
use crate::s3::delete_object::DeleteObjectError;
use crate::s3::delete_objects::{DeleteObjectsEntry, DeleteObjectsInput, delete_objects};
use crate::s3::get_bucket_info::{GetBucketInfoError, GetBucketInfoOperation};
use crate::s3::list_multipart_uploads::{ListMultipartUploadsInput, ListMultipartUploadsOperation};
use crate::s3::list_object_versions::{
    ListObjectVersionsInput, ListObjectVersionsItem, ListObjectVersionsOperation,
};
use crate::s3::purge_fence::{PurgeFenceError, acquire_purge_fence};

const PURGE_BATCH_SIZE: usize = 1_000;

pub async fn run_storage_purge(ctx: &JobContext, spec: &StoragePurgeSpec) -> JobRunOutcome {
    if let Err(error) =
        acquire_purge_fence(&ctx.driver.storage_handle, ctx.job_id, &spec.scope).await
    {
        return JobRunOutcome::Failed(fence_error(error));
    }
    match run_fenced_purge(ctx, spec).await {
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
) -> Result<StoragePurgeResult, PurgeRunError> {
    check_stop(ctx)?;
    let bucket_exists = match drive(
        GetBucketInfoOperation::new(spec.scope.bucket().to_string()),
        &ctx.driver,
    )
    .await
    .and_then(|result| result.transpose())
    {
        Ok(Some(info)) if info.group_id == spec.group_id => true,
        Ok(Some(_)) => {
            return Err(JobError::permanent("purge bucket is outside the authorized group").into());
        }
        Ok(None) | Err(GetBucketInfoError::NotFound) => false,
        Err(error) => {
            return Err(JobError::retryable(format!("purge bucket read failed: {error}")).into());
        }
    };

    let mut checkpoint = match read_purge_checkpoint(&ctx.driver.storage_handle, ctx.job_id).await {
        Ok(Some(checkpoint)) => checkpoint,
        Ok(None) => {
            let initial_versions = count_versions(ctx, &spec.scope).await?;
            let initial_multipart_uploads = count_multipart(ctx, &spec.scope).await?;
            let checkpoint = StoragePurgeCheckpoint {
                initial_versions,
                initial_multipart_uploads,
                batches_completed: 0,
            };
            put_purge_checkpoint(
                &ctx.driver.storage_handle,
                ctx.job_id,
                ctx.claim_token,
                &checkpoint,
            )
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

    if bucket_exists {
        abort_uploads(ctx, spec, &mut checkpoint).await?;
        delete_versions(ctx, spec, &mut checkpoint).await?;
        final_relist(ctx, &spec.scope).await?;
    }

    let bucket_deleted = if spec.scope.is_bucket() {
        if bucket_exists {
            match drive(
                DeleteBucketOperation::new(spec.scope.bucket().to_string()),
                &ctx.driver,
            )
            .await
            .and_then(|result| result.transpose())
            {
                Ok(Some(())) | Err(DeleteBucketError::NotFound) => {}
                Ok(None) => {
                    return Err(
                        JobError::retryable("purge bucket delete returned no result").into(),
                    );
                }
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

    write_purge_audit(ctx, spec).await?;
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

fn purge_audit_record(spec: &StoragePurgeSpec, occurred_at_ms: u64) -> BlobDeleteAuditRecord {
    let (scope, key) = match &spec.scope {
        StoragePurgeScope::File { key, .. } => (BlobPurgeScopeKind::File, key.clone()),
        StoragePurgeScope::Prefix { prefix, .. } => (BlobPurgeScopeKind::Prefix, prefix.clone()),
        StoragePurgeScope::Bucket { .. } => (BlobPurgeScopeKind::Bucket, String::new()),
    };
    BlobDeleteAuditRecord {
        realm_id: spec.auth_context.realm_id,
        group_id: spec.group_id,
        node_id: spec.node_id,
        user_id: spec.auth_context.user_id,
        kind: BlobDeleteAuditKind::Purge(scope),
        bucket: spec.scope.bucket().to_string(),
        key,
        version_id: None,
        occurred_at_ms,
    }
}

/// Records the completed purge once. The job id is the record id, so a
/// re-driven job overwrites its own row instead of adding a second one.
async fn write_purge_audit(ctx: &JobContext, spec: &StoragePurgeSpec) -> Result<(), PurgeRunError> {
    let record = purge_audit_record(spec, unix_timestamp_millis());
    let value = record
        .to_bytes()
        .map_err(|error| JobError::retryable(format!("purge audit encode failed: {error}")))?;
    match ctx
        .driver
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: BLOB_DELETE_AUDIT_KEYSPACE.to_string(),
            key: delete_audit_key(spec.group_id, ctx.job_id.as_ulid()).into(),
            value: value.into(),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        other => Err(JobError::retryable(format!("purge audit write failed: {other:?}")).into()),
    }
}

async fn abort_uploads(
    ctx: &JobContext,
    spec: &StoragePurgeSpec,
    checkpoint: &mut StoragePurgeCheckpoint,
) -> Result<(), PurgeRunError> {
    loop {
        check_stop(ctx)?;
        let page = list_multipart_page(ctx, &spec.scope, PURGE_BATCH_SIZE).await?;
        if page.is_empty() {
            return Ok(());
        }
        let mut removed = 0u64;
        for upload in page {
            check_stop(ctx)?;
            authorize_object(ctx, spec, &upload.key).await?;
            match drive(
                AbortMultipartUploadOperation::new(AbortMultipartUploadInput {
                    bucket: upload.bucket,
                    key: upload.key,
                    upload_id: upload.upload_id,
                })
                .including_in_progress(),
                &ctx.driver,
            )
            .await
            .and_then(|result| result.transpose())
            {
                Ok(Some(())) | Err(AbortMultipartUploadError::NoSuchUpload) => removed += 1,
                Ok(None) => {
                    return Err(JobError::retryable("multipart abort returned no result").into());
                }
                Err(AbortMultipartUploadError::UploadNotOpen) => {
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
    spec: &StoragePurgeSpec,
    checkpoint: &mut StoragePurgeCheckpoint,
) -> Result<(), PurgeRunError> {
    loop {
        check_stop(ctx)?;
        let page = list_version_page(ctx, &spec.scope, PURGE_BATCH_SIZE, None, None).await?;
        if page.items.is_empty() {
            return Ok(());
        }
        let entries = page
            .items
            .into_iter()
            .map(|item| match item {
                ListObjectVersionsItem::Version {
                    key, version_id, ..
                }
                | ListObjectVersionsItem::DeleteMarker {
                    key, version_id, ..
                } => DeleteObjectsEntry {
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
        let batch_len = entries.len() as u64;
        let outcomes = delete_objects(
            &ctx.driver,
            DeleteObjectsInput {
                bucket: spec.scope.bucket().to_string(),
                entries,
                group_id: spec.group_id,
                realm_id: spec.auth_context.realm_id,
                node_id: spec.node_id,
                deleted_by: spec.auth_context.user_id,
                restrictions: spec.auth_context.path_restrictions.clone(),
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
        checkpoint.batches_completed = checkpoint.batches_completed.saturating_add(1);
        persist_batch(ctx, checkpoint, batch_len).await?;
    }
}

async fn authorize_object(
    ctx: &JobContext,
    spec: &StoragePurgeSpec,
    key: &str,
) -> Result<(), PurgeRunError> {
    let path = blob_object_permission_path(
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
    match drive(GetBucketInfoOperation::new(bucket.to_string()), &ctx.driver)
        .await
        .and_then(|result| result.transpose())
    {
        Ok(None) | Err(GetBucketInfoError::NotFound) => Ok(()),
        Ok(Some(_)) => Err(JobError::retryable("bucket still exists after purge").into()),
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
        version_marker = page.next_version_id_marker;
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
        let page = list_multipart_page_with_cursor(
            ctx,
            scope,
            PURGE_BATCH_SIZE,
            key_marker,
            upload_marker,
        )
        .await?;
        total = total.saturating_add(page.uploads.len() as u64);
        if !page.is_truncated {
            return Ok(total);
        }
        key_marker = page.next_key_marker;
        upload_marker = page.next_upload_id_marker;
        if key_marker.is_none() {
            return Err(
                JobError::retryable("multipart inventory truncated without a cursor").into(),
            );
        }
    }
}

struct VersionPage {
    items: Vec<ListObjectVersionsItem>,
    is_truncated: bool,
    next_key_marker: Option<String>,
    next_version_id_marker: Option<ulid::Ulid>,
}

async fn list_version_page(
    ctx: &JobContext,
    scope: &StoragePurgeScope,
    limit: usize,
    key_marker: Option<String>,
    version_id_marker: Option<ulid::Ulid>,
) -> Result<VersionPage, PurgeRunError> {
    let result = drive(
        ListObjectVersionsOperation::new(ListObjectVersionsInput {
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
    .and_then(|result| result.transpose())
    .map_err(|error| JobError::retryable(format!("purge version list failed: {error}")))?
    .ok_or_else(|| JobError::retryable("purge version list returned no result"))?;

    let mut items = result.items;
    let mut is_truncated = result.is_truncated;
    let mut next_key_marker = result.next_key_marker;
    let mut next_version_id_marker = result.next_version_id_marker;
    if let StoragePurgeScope::File { key, .. } = scope {
        items.retain(|item| match item {
            ListObjectVersionsItem::Version { key: item, .. }
            | ListObjectVersionsItem::DeleteMarker { key: item, .. } => item == key,
        });
        if next_key_marker.as_deref() != Some(key.as_str()) {
            is_truncated = false;
            next_key_marker = None;
            next_version_id_marker = None;
        }
    }
    Ok(VersionPage {
        items,
        is_truncated,
        next_key_marker,
        next_version_id_marker,
    })
}

async fn list_multipart_page(
    ctx: &JobContext,
    scope: &StoragePurgeScope,
    limit: usize,
) -> Result<Vec<MultipartUpload>, PurgeRunError> {
    Ok(
        list_multipart_page_with_cursor(ctx, scope, limit, None, None)
            .await?
            .uploads,
    )
}

struct MultipartPage {
    uploads: Vec<MultipartUpload>,
    is_truncated: bool,
    next_key_marker: Option<String>,
    next_upload_id_marker: Option<ulid::Ulid>,
}

async fn list_multipart_page_with_cursor(
    ctx: &JobContext,
    scope: &StoragePurgeScope,
    limit: usize,
    key_marker: Option<String>,
    upload_id_marker: Option<ulid::Ulid>,
) -> Result<MultipartPage, PurgeRunError> {
    let result = drive(
        ListMultipartUploadsOperation::new(ListMultipartUploadsInput {
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
    .and_then(|result| result.transpose())
    .map_err(|error| JobError::retryable(format!("purge multipart list failed: {error}")))?
    .ok_or_else(|| JobError::retryable("purge multipart list returned no result"))?;

    let mut uploads = result.uploads;
    let mut is_truncated = result.is_truncated;
    let mut next_key_marker = result.next_key_marker;
    let mut next_upload_id_marker = result.next_upload_id_marker;
    if let StoragePurgeScope::File { key, .. } = scope {
        uploads.retain(|upload| upload.key == *key);
        if next_key_marker.as_deref() != Some(key.as_str()) {
            is_truncated = false;
            next_key_marker = None;
            next_upload_id_marker = None;
        }
    }
    Ok(MultipartPage {
        uploads,
        is_truncated,
        next_key_marker,
        next_upload_id_marker,
    })
}

async fn persist_batch(
    ctx: &JobContext,
    checkpoint: &StoragePurgeCheckpoint,
    completed: u64,
) -> Result<(), PurgeRunError> {
    ctx.progress.advance(completed);
    persist_progress(ctx).await?;
    put_purge_checkpoint(
        &ctx.driver.storage_handle,
        ctx.job_id,
        ctx.claim_token,
        checkpoint,
    )
    .await
    .map_err(|error| JobError::retryable(format!("purge checkpoint write failed: {error}")))?;
    Ok(())
}

async fn persist_progress(ctx: &JobContext) -> Result<(), PurgeRunError> {
    let progress: JobProgress = ctx.progress.snapshot();
    let renew = flush_progress(
        &ctx.driver.storage_handle,
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
        | PurgeFenceError::Unexpected(_) => JobError::retryable(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::purge_audit_record;
    use aruna_core::structs::RealmId;
    use aruna_core::structs::{
        AuthContext, BlobDeleteAuditKind, BlobPurgeScopeKind, StoragePurgeScope, StoragePurgeSpec,
    };
    use aruna_core::types::UserId;
    use ulid::Ulid;

    fn spec(scope: StoragePurgeScope) -> StoragePurgeSpec {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        StoragePurgeSpec {
            scope,
            group_id: Ulid::from_bytes([2u8; 16]),
            auth_context: AuthContext {
                user_id: UserId::local(Ulid::from_bytes([3u8; 16]), realm_id),
                realm_id,
                path_restrictions: None,
                session: None,
            },
            node_id: iroh::SecretKey::from_bytes(&[4u8; 32]).public(),
        }
    }

    #[test]
    fn audits_purge_scope() {
        let file = purge_audit_record(
            &spec(StoragePurgeScope::File {
                bucket: "bucket".to_string(),
                key: "reports/a.csv".to_string(),
            }),
            7,
        );
        assert_eq!(
            file.kind,
            BlobDeleteAuditKind::Purge(BlobPurgeScopeKind::File)
        );
        assert_eq!(file.bucket, "bucket");
        assert_eq!(file.key, "reports/a.csv");
        assert_eq!(file.version_id, None);
        assert_eq!(file.occurred_at_ms, 7);

        let prefix = purge_audit_record(
            &spec(StoragePurgeScope::Prefix {
                bucket: "bucket".to_string(),
                prefix: "reports/".to_string(),
            }),
            8,
        );
        assert_eq!(
            prefix.kind,
            BlobDeleteAuditKind::Purge(BlobPurgeScopeKind::Prefix)
        );
        assert_eq!(prefix.key, "reports/");

        let bucket = purge_audit_record(
            &spec(StoragePurgeScope::Bucket {
                bucket: "bucket".to_string(),
            }),
            9,
        );
        assert_eq!(
            bucket.kind,
            BlobDeleteAuditKind::Purge(BlobPurgeScopeKind::Bucket)
        );
        assert!(bucket.key.is_empty());
    }
}
