use std::path::{Component, Path};

use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::STAGING_JOB_STATE_KEYSPACE;
use aruna_core::structs::{
    BucketInfo, JobError, JobId, JobResultPayload, Permission, SourceEntry, SourceEntryKind,
    StagingJobCheckpoint, StagingJobDirectory, StagingJobError, StagingJobPhase, StagingJobSpec,
    StagingPendingItem, StagingStrategy, blob_object_permission_path,
};
use aruna_core::types::Value;
use byteview::ByteView;

use super::executor::{JobContext, JobRunOutcome};
use super::store::put_staging_checkpoint;
use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::driver::drive;
use crate::get_realm_config::GetRealmConfigOperation;
use crate::replication::queue::{
    QueueLiveVersionReplicationInput, QueueLiveVersionReplicationOperation,
};
use crate::s3::get_bucket_info::{GetBucketInfoError, GetBucketInfoOperation};
use crate::staging::head_source::{HeadStagingSourceInput, HeadStagingSourceOperation};
use crate::staging::list_source::{ListStagingSourceInput, ListStagingSourceOperation};
use crate::staging::reference::{MaterializeReferenceInput, stage_reference_blob};
use crate::staging::snapshot::{MaterializeSnapshotInput, stage_snapshot_blob};

const STAGING_LIST_PAGE_SIZE: usize = 500;

pub async fn read_staging_checkpoint(
    context: &crate::driver::DriverContext,
    job_id: JobId,
) -> Result<Option<StagingJobCheckpoint>, String> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: STAGING_JOB_STATE_KEYSPACE.to_string(),
            key: staging_checkpoint_key(job_id),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => postcard::from_bytes(value.as_ref())
            .map(Some)
            .map_err(|error| error.to_string()),
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Ok(None),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!(
            "unexpected staging checkpoint read event: {other:?}"
        )),
    }
}

pub async fn run_staging_job(ctx: &JobContext, spec: &StagingJobSpec) -> JobRunOutcome {
    let job_id = ctx.job_id;
    let mut checkpoint = match read_staging_checkpoint(&ctx.driver, job_id).await {
        Ok(Some(checkpoint)) => checkpoint,
        Ok(None) => initial_checkpoint(spec),
        Err(error) => return retryable_error(error),
    };
    if let Err(error) = persist_checkpoint(ctx, job_id, &checkpoint).await {
        return retryable_error(error);
    }

    loop {
        if ctx.cancel.is_cancelled() {
            return JobRunOutcome::Cancelled;
        }
        if ctx.shutdown.is_cancelled() {
            return JobRunOutcome::Interrupted;
        }

        if let Some(mut item) = checkpoint.pending_items.first().cloned() {
            checkpoint.current_path = Some(item.source_path.clone());
            checkpoint.phase = match spec.strategy {
                StagingStrategy::Reference => StagingJobPhase::Inspecting,
                StagingStrategy::Snapshot => StagingJobPhase::Downloading,
                StagingStrategy::Sync => {
                    return permanent_error("sync is not a staging job strategy");
                }
            };
            if let Err(error) = persist_checkpoint(ctx, job_id, &checkpoint).await {
                return retryable_error(error);
            }

            if spec.strategy == StagingStrategy::Snapshot && item.size.is_none() {
                match inspect_item(ctx, spec, &item).await {
                    Ok(size) => {
                        item.size = Some(size);
                        checkpoint.pending_items[0].size = Some(size);
                        checkpoint.bytes_discovered =
                            checkpoint.bytes_discovered.saturating_add(size);
                        checkpoint.unknown_sizes = checkpoint.unknown_sizes.saturating_sub(1);
                        if checkpoint.pending_directories.is_empty()
                            && checkpoint.unknown_sizes == 0
                        {
                            checkpoint.bytes_total = Some(checkpoint.bytes_discovered);
                        }
                    }
                    Err(ItemFailure::Denied(message) | ItemFailure::Stage(message)) => {
                        checkpoint.pending_items.remove(0);
                        checkpoint.items_current = checkpoint.items_current.saturating_add(1);
                        checkpoint.items_failed = checkpoint.items_failed.saturating_add(1);
                        checkpoint.errors.push(StagingJobError {
                            source_path: item.source_path,
                            target_key: item.target_key,
                            error: message,
                        });
                        sync_progress(ctx, &checkpoint);
                        if let Err(error) = persist_checkpoint(ctx, job_id, &checkpoint).await {
                            return retryable_error(error);
                        }
                        continue;
                    }
                    Err(ItemFailure::System(message)) => return retryable_error(message),
                }
            }

            let result = stage_item(ctx, spec, &item).await;
            checkpoint.pending_items.remove(0);
            checkpoint.items_current = checkpoint.items_current.saturating_add(1);
            match result {
                Ok(size) => {
                    checkpoint.items_succeeded = checkpoint.items_succeeded.saturating_add(1);
                    if spec.strategy == StagingStrategy::Snapshot {
                        checkpoint.bytes_current = checkpoint.bytes_current.saturating_add(size);
                    }
                    checkpoint.phase = match spec.strategy {
                        StagingStrategy::Reference => StagingJobPhase::Registering,
                        StagingStrategy::Snapshot => StagingJobPhase::Writing,
                        StagingStrategy::Sync => unreachable!(),
                    };
                }
                Err(ItemFailure::Denied(message) | ItemFailure::Stage(message)) => {
                    checkpoint.items_failed = checkpoint.items_failed.saturating_add(1);
                    checkpoint.errors.push(StagingJobError {
                        source_path: item.source_path,
                        target_key: item.target_key,
                        error: message,
                    });
                }
                Err(ItemFailure::System(message)) => return retryable_error(message),
            }
            sync_progress(ctx, &checkpoint);
            if let Err(error) = persist_checkpoint(ctx, job_id, &checkpoint).await {
                return retryable_error(error);
            }
            continue;
        }

        if let Some(directory) = checkpoint.pending_directories.first().cloned() {
            checkpoint.phase = StagingJobPhase::Discovering;
            checkpoint.current_path = Some(directory.source_path.clone());
            if let Err(error) = persist_checkpoint(ctx, job_id, &checkpoint).await {
                return retryable_error(error);
            }
            match discover_page(ctx, spec, &directory).await {
                Ok(page) => {
                    checkpoint.pending_directories.remove(0);
                    if let Some(next_offset) = page.next_offset {
                        if next_offset <= directory.offset || page.entries.is_empty() {
                            return permanent_error("source listing pagination made no progress");
                        }
                        checkpoint.pending_directories.insert(
                            0,
                            StagingJobDirectory {
                                offset: next_offset,
                                ..directory.clone()
                            },
                        );
                    }
                    for entry in page.entries {
                        match map_source_entry(&directory, entry) {
                            Ok(MappedEntry::File(item)) => {
                                if spec.strategy == StagingStrategy::Snapshot {
                                    match item.size {
                                        Some(size) => {
                                            checkpoint.bytes_discovered =
                                                checkpoint.bytes_discovered.saturating_add(size)
                                        }
                                        None => {
                                            checkpoint.unknown_sizes =
                                                checkpoint.unknown_sizes.saturating_add(1)
                                        }
                                    }
                                }
                                checkpoint.pending_items.push(item)
                            }
                            Ok(MappedEntry::Directory(directory)) => {
                                checkpoint.pending_directories.push(directory)
                            }
                            Err(message) => {
                                checkpoint.items_current =
                                    checkpoint.items_current.saturating_add(1);
                                checkpoint.items_failed = checkpoint.items_failed.saturating_add(1);
                                checkpoint.errors.push(StagingJobError {
                                    source_path: directory.source_path.clone(),
                                    target_key: directory.target_prefix.clone(),
                                    error: message,
                                });
                            }
                        }
                    }
                }
                Err(ItemFailure::Denied(message) | ItemFailure::Stage(message)) => {
                    checkpoint.pending_directories.remove(0);
                    checkpoint.items_current = checkpoint.items_current.saturating_add(1);
                    checkpoint.items_failed = checkpoint.items_failed.saturating_add(1);
                    checkpoint.errors.push(StagingJobError {
                        source_path: directory.source_path,
                        target_key: directory.target_prefix,
                        error: message,
                    });
                }
                Err(ItemFailure::System(message)) => return retryable_error(message),
            }
            if checkpoint.pending_directories.is_empty() {
                checkpoint.items_total = Some(
                    checkpoint
                        .items_current
                        .saturating_add(checkpoint.pending_items.len() as u64),
                );
                if spec.strategy == StagingStrategy::Snapshot && checkpoint.unknown_sizes == 0 {
                    checkpoint.bytes_total = Some(checkpoint.bytes_discovered);
                }
            }
            sync_progress(ctx, &checkpoint);
            if let Err(error) = persist_checkpoint(ctx, job_id, &checkpoint).await {
                return retryable_error(error);
            }
            continue;
        }

        checkpoint.phase = StagingJobPhase::Completed;
        checkpoint.current_path = None;
        checkpoint.items_total = Some(checkpoint.items_current);
        sync_progress(ctx, &checkpoint);
        if let Err(error) = persist_checkpoint(ctx, job_id, &checkpoint).await {
            return retryable_error(error);
        }
        return JobRunOutcome::Succeeded(JobResultPayload::Staging {
            completed_items: checkpoint.items_succeeded,
            failed_items: checkpoint.items_failed,
        });
    }
}

fn initial_checkpoint(spec: &StagingJobSpec) -> StagingJobCheckpoint {
    let pending_items = spec
        .items
        .iter()
        .map(|item| StagingPendingItem {
            source_path: item.source_path.clone(),
            target_key: item.target_key.clone(),
            size: None,
        })
        .collect::<Vec<_>>();
    let pending_directories = spec
        .prefixes
        .iter()
        .map(|prefix| StagingJobDirectory {
            source_path: directory_list_path(&prefix.source_prefix),
            target_prefix: prefix.target_prefix.clone(),
            offset: 0,
        })
        .collect::<Vec<_>>();
    let unknown_sizes = if spec.strategy == StagingStrategy::Snapshot {
        pending_items.len() as u64
    } else {
        0
    };
    StagingJobCheckpoint {
        phase: StagingJobPhase::Queued,
        items_total: pending_directories
            .is_empty()
            .then_some(pending_items.len() as u64),
        pending_items,
        pending_directories,
        items_current: 0,
        items_succeeded: 0,
        items_failed: 0,
        bytes_current: 0,
        bytes_total: None,
        bytes_discovered: 0,
        unknown_sizes,
        current_path: None,
        errors: Vec::new(),
    }
}

async fn stage_item(
    ctx: &JobContext,
    spec: &StagingJobSpec,
    item: &StagingPendingItem,
) -> Result<u64, ItemFailure> {
    let expected_bucket = ensure_item_permission(ctx, spec, item).await?;
    let (size, version_id) = match spec.strategy {
        StagingStrategy::Reference => stage_reference_blob(
            &ctx.driver,
            MaterializeReferenceInput {
                group_id: spec.group_id,
                user_id: spec.auth_context.user_id,
                realm_id: spec.auth_context.realm_id,
                node_id: spec.node_id,
                connector_id: spec.connector_id,
                source_path: item.source_path.clone(),
                bucket: spec.bucket.clone(),
                key: item.target_key.clone(),
                expected_bucket,
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
                    source_path: item.source_path.clone(),
                    bucket: spec.bucket.clone(),
                    key: item.target_key.clone(),
                    quota_ceiling,
                    retry_key: Some(ctx.job_id.to_string()),
                    expected_bucket,
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
    let _ = drive(
        QueueLiveVersionReplicationOperation::new(QueueLiveVersionReplicationInput {
            local_node_id: spec.node_id,
            auth_context: spec.auth_context.clone(),
            bucket: spec.bucket.clone(),
            key: item.target_key.clone(),
            version_id,
            delete_marker: false,
        }),
        &ctx.driver,
    )
    .await;
    Ok(size)
}

async fn inspect_item(
    ctx: &JobContext,
    spec: &StagingJobSpec,
    item: &StagingPendingItem,
) -> Result<u64, ItemFailure> {
    let _ = ensure_item_permission(ctx, spec, item).await?;
    drive(
        HeadStagingSourceOperation::new(HeadStagingSourceInput {
            group_id: spec.group_id,
            connector_id: spec.connector_id,
            source_path: item.source_path.clone(),
        }),
        &ctx.driver,
    )
    .await
    .map(|result| result.metadata.content_length)
    .map_err(|error| ItemFailure::Stage(error.to_string()))
}

async fn ensure_item_permission(
    ctx: &JobContext,
    spec: &StagingJobSpec,
    item: &StagingPendingItem,
) -> Result<BucketInfo, ItemFailure> {
    let bucket_info = load_live_bucket(ctx, &spec.bucket).await?;
    if bucket_info.group_id != spec.group_id {
        return Err(ItemFailure::Stage(
            "destination bucket group changed after job submission".to_string(),
        ));
    }
    let source_path = source_permission_path(spec, &item.source_path);
    let target_path = blob_object_permission_path(
        spec.auth_context.realm_id,
        spec.group_id,
        spec.node_id,
        &spec.bucket,
        &item.target_key,
    );
    for (path, required_permission) in [
        (source_path, Permission::READ),
        (target_path, Permission::WRITE),
    ] {
        let allowed = drive(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context: spec.auth_context.clone(),
                path,
                required_permission,
            }),
            &ctx.driver,
        )
        .await
        .map_err(|error| ItemFailure::System(error.to_string()))?;
        if !allowed {
            return Err(ItemFailure::Denied("permission denied".to_string()));
        }
    }
    Ok(bucket_info)
}

async fn load_live_bucket(
    ctx: &JobContext,
    bucket: &str,
) -> Result<aruna_core::structs::BucketInfo, ItemFailure> {
    match drive(GetBucketInfoOperation::new(bucket.to_string()), &ctx.driver).await {
        Ok(Some(Ok(bucket_info))) => Ok(bucket_info),
        Ok(Some(Err(GetBucketInfoError::NotFound))) | Ok(None) => Err(ItemFailure::Stage(
            "destination bucket no longer exists".to_string(),
        )),
        Ok(Some(Err(error))) | Err(error) => Err(ItemFailure::System(error.to_string())),
    }
}

async fn current_quota(
    ctx: &JobContext,
    spec: &StagingJobSpec,
) -> Result<Option<u64>, ItemFailure> {
    drive(
        GetRealmConfigOperation::new(spec.auth_context.realm_id),
        &ctx.driver,
    )
    .await
    .map(|config| config.quota.effective_group_ceiling(&spec.group_id))
    .map_err(|error| ItemFailure::System(error.to_string()))
}

struct DiscoveryPage {
    entries: Vec<SourceEntry>,
    next_offset: Option<usize>,
}

async fn discover_page(
    ctx: &JobContext,
    spec: &StagingJobSpec,
    directory: &StagingJobDirectory,
) -> Result<DiscoveryPage, ItemFailure> {
    let allowed = drive(
        CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: spec.auth_context.clone(),
            path: source_permission_path(spec, &directory.source_path),
            required_permission: Permission::READ,
        }),
        &ctx.driver,
    )
    .await
    .map_err(|error| ItemFailure::System(error.to_string()))?;
    if !allowed {
        return Err(ItemFailure::Denied("permission denied".to_string()));
    }
    drive(
        ListStagingSourceOperation::new(ListStagingSourceInput {
            group_id: spec.group_id,
            connector_id: spec.connector_id,
            source_path: directory.source_path.clone(),
            offset: directory.offset,
            limit: STAGING_LIST_PAGE_SIZE,
            recursive: false,
            files_only: false,
        }),
        &ctx.driver,
    )
    .await
    .map(|result| DiscoveryPage {
        entries: result.entries,
        next_offset: result.next_offset,
    })
    .map_err(|error| ItemFailure::Stage(error.to_string()))
}

enum MappedEntry {
    File(StagingPendingItem),
    Directory(StagingJobDirectory),
}

fn map_source_entry(
    directory: &StagingJobDirectory,
    entry: SourceEntry,
) -> Result<MappedEntry, String> {
    let entry_path = canonical_walker_path(&entry.path)?;
    let relative = relative_entry_path(&directory.source_path, &entry_path)?;
    let target = join_target_prefix(&directory.target_prefix, &relative);
    Ok(match entry.kind {
        SourceEntryKind::File => MappedEntry::File(StagingPendingItem {
            source_path: entry_path,
            target_key: target,
            size: entry.size,
        }),
        SourceEntryKind::Directory => MappedEntry::Directory(StagingJobDirectory {
            source_path: directory_list_path(&entry_path),
            target_prefix: target,
            offset: 0,
        }),
    })
}

fn directory_list_path(path: &str) -> String {
    if path.is_empty() || path.ends_with('/') {
        path.to_string()
    } else {
        format!("{path}/")
    }
}

fn relative_entry_path(base: &str, entry: &str) -> Result<String, String> {
    let base = base.trim_matches('/');
    let relative = if base.is_empty() {
        entry
    } else {
        entry
            .strip_prefix(base)
            .and_then(|suffix| suffix.strip_prefix('/'))
            .ok_or_else(|| "source listing returned an entry outside its directory".to_string())?
    };
    if relative.is_empty() || relative.contains('/') {
        return Err("source listing returned a non-child entry".to_string());
    }
    Ok(relative.to_string())
}

fn canonical_walker_path(path: &str) -> Result<String, String> {
    let mut path = path.trim();
    while let Some(stripped) = path.strip_prefix("./") {
        path = stripped;
    }
    let path = path.trim_end_matches('/');
    if path.is_empty() {
        return Err("source listing returned an empty entry path".to_string());
    }
    if path
        .split('/')
        .any(|segment| segment.is_empty() || segment == "." || segment == "..")
        || Path::new(path)
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err("source listing returned an invalid entry path".to_string());
    }
    Ok(path.to_string())
}

fn join_target_prefix(prefix: &str, relative: &str) -> String {
    let prefix = prefix.trim_matches('/');
    if prefix.is_empty() {
        relative.to_string()
    } else {
        format!("{prefix}/{relative}")
    }
}

fn source_permission_path(spec: &StagingJobSpec, source_path: &str) -> String {
    let root = format!(
        "/{}/g/{}/data/{}/_sources/{}",
        spec.auth_context.realm_id, spec.group_id, spec.node_id, spec.connector_id
    );
    if source_path.is_empty() {
        root
    } else {
        format!("{root}/{source_path}")
    }
}

fn sync_progress(ctx: &JobContext, checkpoint: &StagingJobCheckpoint) {
    ctx.progress.set_current(checkpoint.items_current);
    if let Some(total) = checkpoint.items_total {
        ctx.progress.set_total(total);
    }
}

async fn persist_checkpoint(
    ctx: &JobContext,
    job_id: JobId,
    checkpoint: &StagingJobCheckpoint,
) -> Result<(), String> {
    let value = postcard::to_allocvec(checkpoint).map_err(|error| error.to_string())?;
    put_staging_checkpoint(
        &ctx.driver.storage_handle,
        job_id,
        ctx.claim_token,
        Value::from(value),
    )
    .await
    .map_err(|error| error.to_string())
}

fn staging_checkpoint_key(job_id: JobId) -> ByteView {
    ByteView::from(job_id.to_bytes().to_vec())
}

enum ItemFailure {
    Denied(String),
    Stage(String),
    System(String),
}

fn retryable_error(message: impl Into<String>) -> JobRunOutcome {
    JobRunOutcome::Failed(JobError::retryable(message.into()))
}

fn permanent_error(message: impl Into<String>) -> JobRunOutcome {
    JobRunOutcome::Failed(JobError::permanent(message.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ulid::Ulid;

    #[test]
    fn directory_keeps_marker() {
        let root = StagingJobDirectory {
            source_path: String::new(),
            target_prefix: String::new(),
            offset: 0,
        };
        let entry = SourceEntry {
            name: "refseq".to_string(),
            path: "refseq/".to_string(),
            kind: SourceEntryKind::Directory,
            size: None,
            modified: None,
        };

        let MappedEntry::Directory(directory) = map_source_entry(&root, entry).unwrap() else {
            panic!("expected directory")
        };
        assert_eq!(directory.source_path, "refseq/");
        assert_eq!(directory.target_prefix, "refseq");
    }

    #[test]
    fn initial_prefix_marked() {
        let realm_id = aruna_core::structs::RealmId::from_bytes([1; 32]);
        let mut spec = StagingJobSpec {
            auth_context: aruna_core::structs::AuthContext {
                user_id: aruna_core::UserId::local(Ulid::from_bytes([2; 16]), realm_id),
                realm_id,
                path_restrictions: None,
            },
            group_id: Ulid::from_bytes([3; 16]),
            node_id: iroh::SecretKey::from_bytes(&[4; 32]).public(),
            connector_id: Ulid::from_bytes([5; 16]),
            bucket: "bucket".to_string(),
            strategy: StagingStrategy::Reference,
            items: Vec::new(),
            prefixes: vec![aruna_core::structs::StagingJobPrefix {
                source_prefix: "refseq".to_string(),
                target_prefix: String::new(),
            }],
        };

        assert_eq!(
            initial_checkpoint(&spec).pending_directories[0].source_path,
            "refseq/"
        );
        spec.prefixes[0].source_prefix.clear();
        assert_eq!(
            initial_checkpoint(&spec).pending_directories[0].source_path,
            ""
        );
    }
}
