use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use aruna_core::structs::checksum::HASH_BLAKE3;
use aruna_core::structs::{BucketInfo, CopyJobSpec, JobError, JobResultPayload};

use super::executor::{JobContext, JobRunOutcome};
use crate::driver::drive;
use crate::get_realm_config::GetRealmConfigOperation;
use crate::s3::copy_object::{
    CopyObjectError, CopyObjectInput, CopySourceConditions, copy_object_tracked,
};
use crate::s3::get_bucket_info::{GetBucketInfoError, GetBucketInfoOperation};
use crate::s3::get_object::GetObjectError;
use crate::s3::head_object::{HeadObjectInput, HeadObjectOperation};
use crate::s3::put_object::PutObjectError;

/// How often the bytes pulled so far reach the job's progress.
const PROGRESS_TICK: Duration = Duration::from_secs(1);

/// Copies one object as the request path would, reporting the bytes read so
/// far while the source is pulled. Cancellation drops the copy unwritten.
pub async fn run_copy_job(ctx: &JobContext, spec: &CopyJobSpec) -> JobRunOutcome {
    let source = match live_bucket(ctx, &spec.source_bucket).await {
        Ok(bucket) => bucket,
        Err(outcome) => return outcome,
    };
    let dest = match live_bucket(ctx, &spec.dest_bucket).await {
        Ok(bucket) => bucket,
        Err(outcome) => return outcome,
    };
    if source.group_id != spec.source_group_id || dest.group_id != spec.group_id {
        return permanent("a bucket changed its group after the copy was queued");
    }
    let quota_ceiling = match drive(
        GetRealmConfigOperation::new(spec.auth_context.realm_id),
        &ctx.driver,
    )
    .await
    {
        Ok(config) => config.quota.effective_group_ceiling(&spec.group_id),
        Err(error) => return retryable(error.to_string()),
    };
    if let Some(total) = source_length(ctx, spec).await {
        ctx.progress.set_total(total);
    }

    let pulled = Arc::new(AtomicU64::new(0));
    let copy = copy_object_tracked(
        &ctx.driver,
        CopyObjectInput {
            source_bucket: spec.source_bucket.clone(),
            source_key: spec.source_key.clone(),
            source_version_id: spec.source_version_id,
            source_group_id: spec.source_group_id,
            source_auth_context: spec.auth_context.clone(),
            dest_bucket: spec.dest_bucket.clone(),
            dest_key: spec.dest_key.clone(),
            user_id: spec.auth_context.user_id,
            group_id: spec.group_id,
            realm_id: spec.auth_context.realm_id,
            node_id: spec.node_id,
            quota_ceiling,
            conditions: CopySourceConditions::default(),
            metadata: None,
            restrictions: spec.auth_context.path_restrictions.clone(),
        },
        Some(pulled.clone()),
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
            ctx.progress.set_current(copied.location.blob_size);
            JobRunOutcome::Succeeded(JobResultPayload::CopyObject {
                version_id: copied
                    .source_version_id
                    .map(|version| version.to_string())
                    .unwrap_or_default(),
                bytes: copied.location.blob_size,
                blake3: copied
                    .location
                    .hashes
                    .get(HASH_BLAKE3)
                    .map(hex::encode)
                    .unwrap_or_default(),
            })
        }
        Err(error) => JobRunOutcome::Failed(copy_error(error)),
    }
}

/// The source's size as described here, so the bar has an end before the
/// first byte moves. A reference answers from its cached observation.
async fn source_length(ctx: &JobContext, spec: &CopyJobSpec) -> Option<u64> {
    let head = drive(
        HeadObjectOperation::new(HeadObjectInput {
            bucket: spec.source_bucket.clone(),
            key: spec.source_key.clone(),
            version_id: spec.source_version_id,
        }),
        &ctx.driver,
    )
    .await
    .ok()??
    .ok()?;
    head.location
        .as_ref()
        .map(|location| location.blob_size)
        .or_else(|| {
            head.source_metadata
                .as_ref()
                .map(|metadata| metadata.content_length)
        })
}

async fn live_bucket(ctx: &JobContext, bucket: &str) -> Result<BucketInfo, JobRunOutcome> {
    match drive(GetBucketInfoOperation::new(bucket.to_string()), &ctx.driver).await {
        Ok(Some(Ok(info))) => Ok(info),
        Ok(Some(Err(GetBucketInfoError::NotFound))) | Ok(None) => {
            Err(permanent("a bucket of the copy no longer exists"))
        }
        Ok(Some(Err(error))) => Err(retryable(error.to_string())),
        Err(error) => Err(retryable(error.to_string())),
    }
}

/// Storage, routing and gate failures are the node's, so the job retries;
/// anything else names the source or destination and is final.
fn copy_error(error: CopyObjectError) -> JobError {
    match &error {
        CopyObjectError::Get(GetObjectError::StorageError(_))
        | CopyObjectError::Put(PutObjectError::StorageError(_))
        | CopyObjectError::Routing(_)
        | CopyObjectError::Gate(_) => JobError::retryable(error.to_string()),
        CopyObjectError::Get(_) | CopyObjectError::Put(_) | CopyObjectError::PreconditionFailed => {
            JobError::permanent(error.to_string())
        }
    }
}

fn retryable(message: impl Into<String>) -> JobRunOutcome {
    JobRunOutcome::Failed(JobError::retryable(message.into()))
}

fn permanent(message: impl Into<String>) -> JobRunOutcome {
    JobRunOutcome::Failed(JobError::permanent(message.into()))
}

