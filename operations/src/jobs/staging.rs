//! Runs the staging job: walks source listings page by page with a provider cursor and records
//! each page together with a small checkpoint.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::keyspaces::STAGING_STATE_KEYSPACE;
use aruna_core::structs::execution::job::{
    JobError, JobId, JobResultPayload, StagingJobCheckpoint, StagingJobPhase, StagingJobSpec,
};
use aruna_core::structs::execution::staging::StagingStrategy;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::types::{Key, KeySpace, Value};
use byteview::ByteView;
use tracing::warn;

use super::executor::{JobContext, JobRunOutcome};
use super::staging_manifest::read_manifest_chunk;
use super::staging_walk::{
    PageFailure, check_access, clear_queue, finish_failed, item_phase, record_failure, start_walk,
    walk_page,
};
use super::store::{put_job_rows, read_state};
use crate::connectors::get_connector::{GetSourceInput, GetSourceOperation};
use crate::driver::drive;
use crate::realm::get_config::GetConfigOperation;
use crate::replication::queue::{LiveVersionInput, LiveVersionOperation};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use crate::staging::reference::{MaterializeReferenceInput, stage_reference_blob};
use crate::staging::snapshot::{MaterializeSnapshotInput, stage_snapshot_blob};
use aruna_core::structs::checksum::ExpectedChecksum;

pub async fn read_staging_checkpoint(
    context: &crate::driver::DriverContext,
    job_id: JobId,
) -> Result<Option<StagingJobCheckpoint>, String> {
    read_state(
        &context.storage_handle,
        STAGING_STATE_KEYSPACE,
        staging_checkpoint_key(job_id),
        "staging checkpoint read",
    )
    .await
}

pub async fn run_staging_job(ctx: &JobContext, spec: &StagingJobSpec) -> JobRunOutcome {
    if spec.strategy == StagingStrategy::Sync {
        return permanent_error("sync is not a staging job strategy");
    }
    let mut checkpoint = match read_staging_checkpoint(&ctx.driver, ctx.job_id).await {
        Ok(Some(checkpoint)) => checkpoint,
        Ok(None) => initial_checkpoint(spec),
        Err(error) => return retryable_error(error),
    };
    if checkpoint.flat.is_none() {
        match drive(
            GetSourceOperation::new(GetSourceInput {
                group_id: spec.group_id,
                connector_id: spec.connector_id,
            }),
            &ctx.driver,
        )
        .await
        {
            Ok(source) => checkpoint.flat = Some(source.connector.kind.lists_flat()),
            Err(error) => return finish_failed(ctx, permanent_error(error.to_string())).await,
        }
    }
    if let Err(error) = persist_rows(ctx, &checkpoint, Vec::new(), Vec::new()).await {
        return retryable_error(error);
    }

    loop {
        if ctx.cancel.is_cancelled() {
            clear_queue(ctx).await;
            return JobRunOutcome::Cancelled;
        }
        if ctx.shutdown.is_cancelled() {
            return JobRunOutcome::Interrupted;
        }

        if let Some(item) = usize::try_from(checkpoint.items_done)
            .ok()
            .and_then(|done| spec.items.get(done))
        {
            checkpoint.current_path = Some(item.source_path.clone());
            checkpoint.phase = item_phase(spec);
            match stage_item(ctx, spec, &item.source_path, &item.target_key, Vec::new()).await {
                Ok(size) => {
                    checkpoint.items_current = checkpoint.items_current.saturating_add(1);
                    checkpoint.items_succeeded = checkpoint.items_succeeded.saturating_add(1);
                    if spec.strategy == StagingStrategy::Snapshot {
                        checkpoint.bytes_current = checkpoint.bytes_current.saturating_add(size);
                    }
                }
                Err(ItemFailure::Denied(message) | ItemFailure::Stage(message)) => record_failure(
                    &mut checkpoint,
                    &item.source_path,
                    &item.target_key,
                    message,
                ),
                Err(ItemFailure::System(message)) => return retryable_error(message),
                Err(ItemFailure::Lasting(message)) => {
                    return finish_failed(ctx, permanent_error(message)).await;
                }
            }
            checkpoint.items_done += 1;
            sync_progress(ctx, &checkpoint);
            if let Err(error) = persist_rows(ctx, &checkpoint, Vec::new(), Vec::new()).await {
                return retryable_error(error);
            }
            continue;
        }

        if checkpoint.manifest.is_none() && checkpoint.walk.is_none() {
            match start_walk(ctx, spec, &mut checkpoint).await {
                Ok(true) => continue,
                Ok(false) => break,
                Err(error) => return retryable_error(error),
            }
        }
        let page = match checkpoint.manifest.is_some() {
            true => read_manifest_chunk(ctx, spec, &mut checkpoint).await,
            false => walk_page(ctx, spec, &mut checkpoint).await,
        };
        if let Err(outcome) = page {
            return match outcome {
                PageFailure::Retry(message) => retryable_error(message),
                PageFailure::Permanent(message) => {
                    finish_failed(ctx, permanent_error(message)).await
                }
            };
        }
    }

    checkpoint.phase = StagingJobPhase::Completed;
    checkpoint.current_path = None;
    checkpoint.items_total = Some(checkpoint.items_current);
    if spec.strategy == StagingStrategy::Snapshot {
        checkpoint.bytes_total = Some(checkpoint.bytes_current);
    }
    sync_progress(ctx, &checkpoint);
    if let Err(error) = persist_rows(ctx, &checkpoint, Vec::new(), Vec::new()).await {
        return retryable_error(error);
    }
    clear_queue(ctx).await;
    JobRunOutcome::Succeeded(JobResultPayload::Staging {
        completed_items: checkpoint.items_succeeded,
        failed_items: checkpoint.items_failed,
    })
}

fn initial_checkpoint(spec: &StagingJobSpec) -> StagingJobCheckpoint {
    StagingJobCheckpoint {
        phase: StagingJobPhase::Queued,
        flat: None,
        items_done: 0,
        prefixes_done: 0,
        walk: None,
        manifest: None,
        queue_head: 0,
        queue_tail: 0,
        items_current: 0,
        items_succeeded: 0,
        items_failed: 0,
        items_total: spec.prefixes.is_empty().then_some(spec.items.len() as u64),
        bytes_current: 0,
        bytes_total: None,
        bytes_discovered: 0,
        current_path: None,
        errors: Vec::new(),
    }
}

pub(super) async fn stage_item(
    ctx: &JobContext,
    spec: &StagingJobSpec,
    source_path: &str,
    target_key: &str,
    expected_checksums: Vec<ExpectedChecksum>,
) -> Result<u64, ItemFailure> {
    let expected_bucket = ensure_item_permission(ctx, spec, source_path, target_key).await?;
    let (size, version_id) = match spec.strategy {
        StagingStrategy::Reference => stage_reference_blob(
            &ctx.driver,
            MaterializeReferenceInput {
                group_id: spec.group_id,
                user_id: spec.auth_context.user_id,
                realm_id: spec.auth_context.realm_id,
                node_id: spec.node_id,
                connector_id: spec.connector_id,
                source_path: source_path.to_string(),
                bucket: spec.bucket.clone(),
                key: target_key.to_string(),
                expected_bucket,
                inherited_policies: Vec::new(),
            },
        )
        .await
        .map(|result| (result.source_metadata.content_length, result.version_id))
        .map_err(|error| ItemFailure::Stage(error.to_string()))?,
        StagingStrategy::Snapshot => {
            let quota_ceiling = current_quota(ctx, spec).await?;
            stage_snapshot_blob(
                &ctx.driver,
                MaterializeSnapshotInput {
                    group_id: spec.group_id,
                    user_id: spec.auth_context.user_id,
                    realm_id: spec.auth_context.realm_id,
                    node_id: spec.node_id,
                    connector_id: spec.connector_id,
                    source_path: source_path.to_string(),
                    bucket: spec.bucket.clone(),
                    key: target_key.to_string(),
                    quota_ceiling,
                    retry_key: Some(ctx.job_id.to_string()),
                    expected_bucket,
                    restrictions: spec.auth_context.path_restrictions.clone(),
                    expected_checksums,
                },
            )
            .await
            .map(|result| (result.location.blob_size, result.version_id))
            .map_err(|error| ItemFailure::Stage(error.to_string()))?
        }
        StagingStrategy::Sync => {
            return Err(ItemFailure::Stage(
                "sync is not a staging job strategy".to_string(),
            ));
        }
    };
    // Without the replication seed peers never learn about the staged version, so
    // the item is not a success.
    if let Err(error) = drive(
        LiveVersionOperation::new(LiveVersionInput {
            local_node_id: spec.node_id,
            auth_context: spec.auth_context.clone(),
            bucket: spec.bucket.clone(),
            key: target_key.to_string(),
            version_id,
            delete_marker: false,
        }),
        &ctx.driver,
    )
    .await
    {
        warn!(
            bucket = %spec.bucket,
            key = %target_key,
            error = %error,
            "Staging replication queue write failed"
        );
        return Err(ItemFailure::Stage(format!(
            "replication queue failed: {error}"
        )));
    }
    Ok(size)
}

async fn ensure_item_permission(
    ctx: &JobContext,
    spec: &StagingJobSpec,
    source_path: &str,
    target_key: &str,
) -> Result<BucketInfo, ItemFailure> {
    let bucket_info = pinned_bucket(ctx, spec).await?;
    check_access(ctx, spec, source_path, target_key).await?;
    Ok(bucket_info)
}

/// The live destination bucket, only while it is still the one the job was submitted for.
pub(super) async fn pinned_bucket(
    ctx: &JobContext,
    spec: &StagingJobSpec,
) -> Result<BucketInfo, ItemFailure> {
    let bucket_info = load_live_bucket(ctx, &spec.bucket).await?;
    if bucket_info.identity() != spec.bucket_identity {
        return Err(ItemFailure::Lasting(
            "destination bucket was replaced after job submission".to_string(),
        ));
    }
    Ok(bucket_info)
}

pub(super) async fn load_live_bucket(
    ctx: &JobContext,
    bucket: &str,
) -> Result<BucketInfo, ItemFailure> {
    match drive(GetBucketOperation::new(bucket.to_string()), &ctx.driver).await {
        Ok(bucket_info) => Ok(bucket_info),
        Err(GetBucketError::NotFound) => Err(ItemFailure::Stage(
            "destination bucket no longer exists".to_string(),
        )),
        Err(error) => Err(ItemFailure::System(error.to_string())),
    }
}

async fn current_quota(
    ctx: &JobContext,
    spec: &StagingJobSpec,
) -> Result<Option<u64>, ItemFailure> {
    drive(
        GetConfigOperation::new(spec.auth_context.realm_id),
        &ctx.driver,
    )
    .await
    .map(|config| config.quota.effective_group_ceiling(&spec.group_id))
    .map_err(|error| ItemFailure::System(error.to_string()))
}

pub(super) fn sync_progress(ctx: &JobContext, checkpoint: &StagingJobCheckpoint) {
    ctx.progress.set_current(checkpoint.items_current);
    if let Some(total) = checkpoint.items_total {
        ctx.progress.set_total(total);
    }
}

/// Writes the checkpoint and the given job rows in one claim-guarded transaction.
pub(super) async fn persist_rows(
    ctx: &JobContext,
    checkpoint: &StagingJobCheckpoint,
    mut writes: Vec<(KeySpace, Key, Value)>,
    deletes: Vec<(KeySpace, Key)>,
) -> Result<(), String> {
    let value = postcard::to_allocvec(checkpoint).map_err(|error| error.to_string())?;
    writes.push((
        STAGING_STATE_KEYSPACE.to_string(),
        staging_checkpoint_key(ctx.job_id),
        ByteView::from(value),
    ));
    put_job_rows(
        &ctx.driver.storage_handle,
        ctx.job_id,
        ctx.claim_token,
        writes,
        deletes,
    )
    .await
    .map_err(|error| error.to_string())
}

fn staging_checkpoint_key(job_id: JobId) -> ByteView {
    ByteView::from(job_id.to_bytes().to_vec())
}

pub(super) fn queue_key(job_id: JobId, position: u64) -> Key {
    let mut key = job_id.to_bytes().to_vec();
    key.extend_from_slice(&position.to_be_bytes());
    ByteView::from(key)
}

pub(super) enum ItemFailure {
    Denied(String),
    Stage(String),
    System(String),
    /// Ends the whole job, such as a replaced destination bucket.
    Lasting(String),
}

fn retryable_error(message: impl Into<String>) -> JobRunOutcome {
    JobRunOutcome::Failed(JobError::retryable(message.into()))
}

fn permanent_error(message: impl Into<String>) -> JobRunOutcome {
    JobRunOutcome::Failed(JobError::permanent(message.into()))
}

#[cfg(test)]
mod pure_tests {
    use super::super::staging_walk::{directory_list_path, record_failure};
    use super::*;
    use crate::staging::page_rows::MAX_ERROR_SAMPLE;
    use ulid::Ulid;

    fn test_spec() -> StagingJobSpec {
        let realm_id = aruna_core::structs::identity::realm::RealmId::from_bytes([1; 32]);
        StagingJobSpec {
            auth_context: aruna_core::structs::identity::auth::AuthContext {
                user_id: aruna_core::UserId::local(Ulid::from_bytes([2; 16]), realm_id),
                realm_id,
                path_restrictions: None,
                session: None,
            },
            group_id: Ulid::from_bytes([3; 16]),
            node_id: iroh::SecretKey::from_bytes(&[4; 32]).public(),
            connector_id: Ulid::from_bytes([5; 16]),
            bucket: "bucket".to_string(),
            bucket_identity: (
                Ulid::nil(),
                std::time::SystemTime::UNIX_EPOCH,
                Default::default(),
            ),
            strategy: StagingStrategy::Reference,
            items: Vec::new(),
            prefixes: vec![aruna_core::structs::execution::job::StagingJobPrefix {
                source_prefix: "refseq".to_string(),
                target_prefix: String::new(),
            }],
            checksum_manifest: None,
        }
    }

    #[test]
    fn checkpoint_stays_small() {
        let spec = test_spec();
        let mut checkpoint = initial_checkpoint(&spec);
        for _ in 0..1000 {
            record_failure(&mut checkpoint, "a", "b", "denied".to_string());
        }

        // Failures are counted, but only a bounded sample is kept in the checkpoint.
        assert_eq!(checkpoint.items_failed, 1000);
        assert_eq!(checkpoint.errors.len(), MAX_ERROR_SAMPLE);
        assert_eq!(checkpoint.items_total, None);
        assert_eq!(
            directory_list_path(&spec.prefixes[0].source_prefix),
            "refseq/"
        );
    }

    // A bucket recreated under the submitted name ends the job instead of receiving its files.
    #[tokio::test]
    async fn replaced_bucket_ends() {
        use crate::jobs::executor::ProgressReporter;
        use crate::tests::staging::{create_test_bucket, setup_driver_context};
        use aruna_core::structs::execution::job::JobProgress;
        use std::sync::Arc;
        use tokio_util::sync::CancellationToken;

        let test = setup_driver_context().await;
        let spec = test_spec();
        let bucket = create_test_bucket(
            &test.driver_context,
            spec.group_id,
            spec.auth_context.user_id,
            &spec.bucket,
        )
        .await;
        let ctx = JobContext {
            driver: Arc::new(test.driver_context),
            job_id: JobId::from_bytes([7; 16]),
            owner_node_id: spec.node_id,
            claim_token: Ulid::from_bytes([6; 16]),
            final_attempt: false,
            cancel: CancellationToken::new(),
            shutdown: CancellationToken::new(),
            progress: ProgressReporter::from_progress(&JobProgress::new("items")),
        };

        let pinned = StagingJobSpec {
            bucket_identity: bucket.identity(),
            ..spec.clone()
        };
        assert!(pinned_bucket(&ctx, &pinned).await.is_ok());
        assert!(matches!(
            pinned_bucket(&ctx, &spec).await,
            Err(ItemFailure::Lasting(_))
        ));
    }
}
