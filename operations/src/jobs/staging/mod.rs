//! Runs the staging job: walks source listings page by page with a provider cursor and records
//! each page together with a small checkpoint.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod lanes;
mod limiter;
mod manifest;
mod mirrors;
mod table;
mod walk;

use std::sync::Arc;

use aruna_blob::pace::{Pace, paced};
use aruna_core::keyspaces::STAGING_STATE_KEYSPACE;
use aruna_core::structs::execution::job::{
    JobError, JobErrorKind, JobId, JobResultPayload, ManifestRead, StagingJobCheckpoint,
    StagingJobPhase, StagingJobSpec,
};
use aruna_core::structs::execution::staging::StagingStrategy;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::types::{Key, KeySpace, Value};
use byteview::ByteView;
use tracing::warn;

use self::manifest::read_manifest_chunk;
use self::walk::{
    PageFailure, check_access, clear_queue, finish_failed, item_phase, record_failure, start_walk,
    walk_page,
};
use super::executor::{JobContext, JobRunOutcome};
use super::store::{put_job_rows, read_state};
use crate::connectors::get_connector::{GetSourceInput, GetSourceOperation};
use crate::driver::drive;
use crate::realm::get_config::GetConfigOperation;
use crate::replication::queue::{LiveVersionInput, LiveVersionOperation};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use crate::staging::reference::{MaterializeReferenceInput, stage_reference_blob};
use crate::staging::snapshot::{MaterializeSnapshotInput, stage_snapshot_blob};
use aruna_core::structs::checksum::ExpectedChecksum;

/// The job's progress with its open prefix lanes counted in; a lane's prefix reads as not
/// started yet, as a job handed over goes on from there.
pub async fn read_staging_checkpoint(
    context: &crate::driver::DriverContext,
    job_id: JobId,
) -> Result<Option<StagingJobCheckpoint>, String> {
    let Some(main) = read_main(context, job_id).await? else {
        return Ok(None);
    };
    let open = lanes::read_lanes(context, job_id).await?;
    Ok(Some(lanes::merged(main, open)))
}

async fn read_main(
    context: &crate::driver::DriverContext,
    job_id: JobId,
) -> Result<Option<StagingJobCheckpoint>, String> {
    read_state(
        &context.storage_handle,
        STAGING_STATE_KEYSPACE,
        checkpoint_key(job_id, None),
        "staging checkpoint read",
    )
    .await
}

pub async fn run_staging_job(ctx: &JobContext, spec: &StagingJobSpec) -> JobRunOutcome {
    // Every source request of a table import, adapter retries included, takes a token; a
    // pause a source asks for ends with the job and shows in its progress.
    match spec.table.as_ref() {
        Some(table) => {
            let progress = ctx.progress.clone();
            let pace = Pace {
                stops: vec![ctx.cancel.clone(), ctx.shutdown.clone()],
                notice: Some(Arc::new(move |until| progress.set_pause(until))),
                ..Pace::new(spec.connector_id, table.rate())
            };
            let outcome = Box::pin(paced(pace, run_job(ctx, spec))).await;
            stopped_outcome(ctx, outcome).await
        }
        None => Box::pin(run_job(ctx, spec)).await,
    }
}

/// A failure while the job was cancelled or shut down, such as a paced wait the stop ended,
/// is that stop's outcome and spends no attempt.
async fn stopped_outcome(ctx: &JobContext, outcome: JobRunOutcome) -> JobRunOutcome {
    if !matches!(
        outcome,
        JobRunOutcome::Failed(_) | JobRunOutcome::Deferred(_)
    ) {
        return outcome;
    }
    if ctx.cancel.is_cancelled() {
        clear_queue(ctx).await;
        return JobRunOutcome::Cancelled;
    }
    // A permanent failure already cleared the job's rows, so it stays terminal.
    let permanent = matches!(
        &outcome,
        JobRunOutcome::Failed(error) if error.kind == JobErrorKind::Permanent
    );
    match ctx.shutdown.is_cancelled() && !permanent {
        true => JobRunOutcome::Interrupted,
        false => outcome,
    }
}

async fn run_job(ctx: &JobContext, spec: &StagingJobSpec) -> JobRunOutcome {
    if spec.strategy == StagingStrategy::Sync {
        return permanent_error("sync is not a staging job strategy");
    }
    let mut checkpoint = match read_main(&ctx.driver, ctx.job_id).await {
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
    let mut ahead = None;
    let mut gzip = None;

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
            // Another coordinator of a distributed bucket imports this key with its own job.
            let elsewhere = match imported_elsewhere(ctx, spec, &item.target_key).await {
                Ok(elsewhere) => elsewhere,
                Err(ItemFailure::System(message)) => return retryable_error(message),
                // A bucket that is gone or replaced fails the item below, as for any bucket.
                Err(_) => false,
            };
            let staged = match elsewhere {
                true => Ok(None),
                false => stage_item(ctx, spec, &item.source_path, &item.target_key, Vec::new())
                    .await
                    .map(Some),
            };
            match staged {
                Ok(None) => {}
                Ok(Some(size)) => {
                    checkpoint.items_current = checkpoint.items_current.saturating_add(1);
                    checkpoint.items_succeeded = checkpoint.items_succeeded.saturating_add(1);
                    if spec.strategy == StagingStrategy::Snapshot {
                        checkpoint.bytes_current = checkpoint.bytes_current.saturating_add(size);
                    }
                }
                // A key whose range moved meanwhile belongs to its new coordinator's job.
                Err(ItemFailure::Denied(message) | ItemFailure::Stage(message)) => {
                    if !moved_away(ctx, spec, &item.target_key).await {
                        record_failure(
                            &mut checkpoint,
                            &item.source_path,
                            &item.target_key,
                            message,
                        )
                    }
                }
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
            ctx.progress.checkpoint();
            continue;
        }

        if lanes::applies(spec, &checkpoint) {
            match lanes::run_lanes(ctx, spec, &mut checkpoint).await {
                // Every prefix is walked, so the walk below finds nothing left.
                Ok(true) => {}
                Ok(false) => continue,
                Err(PageFailure::Retry(message)) => return retryable_error(message),
                Err(PageFailure::Conflict(message)) => return conflict_error(message),
                Err(PageFailure::Permanent(message)) => {
                    return finish_failed(ctx, permanent_error(message)).await;
                }
            }
        }
        if table::reads_table(&checkpoint) {
            // Boxed, as its page import and object writes make a large future.
            let reading = table::read_table_chunk(ctx, spec, &mut checkpoint, &mut gzip);
            match Box::pin(reading).await {
                Ok(()) => continue,
                Err(PageFailure::Retry(message)) => return retryable_error(message),
                Err(PageFailure::Conflict(message)) => return conflict_error(message),
                Err(PageFailure::Permanent(message)) => {
                    return finish_failed(ctx, permanent_error(message)).await;
                }
            }
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
            false => {
                let walking = (&mut ahead, None);
                walk_page(ctx, spec, (None, None), &mut checkpoint, walking).await
            }
        };
        if let Err(outcome) = page {
            return match outcome {
                PageFailure::Retry(message) => retryable_error(message),
                PageFailure::Conflict(message) => conflict_error(message),
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
        table: spec.table.as_ref().map(|_| Default::default()),
        queue_head: 0,
        queue_tail: 0,
        items_current: 0,
        items_succeeded: 0,
        items_failed: 0,
        items_total: (spec.prefixes.is_empty() && spec.table.is_none())
            .then_some(spec.items.len() as u64),
        bytes_current: 0,
        bytes_total: None,
        bytes_discovered: 0,
        current_path: None,
        errors: Vec::new(),
    }
}

/// Where a job a range's new coordinator takes over goes on: the former job's position without
/// its counters, errors or directory queue. A flat walk keeps its listing cursor and reads its
/// manifest again; a manifest read, a table or a directory walk, whose queue stays behind, starts
/// over.
pub(crate) fn handed_checkpoint(
    spec: &StagingJobSpec,
    at: &StagingJobCheckpoint,
) -> StagingJobCheckpoint {
    let mut next = initial_checkpoint(spec);
    // The table is read again from its start, pinned to what the former job first read.
    if let (Some(table), Some(read)) = (next.table.as_mut(), at.table.as_ref()) {
        table.version_id = read.version_id;
        table.observation = read.observation.clone();
    }
    next.flat = at.flat;
    next.items_done = at.items_done;
    let queued = at.queue_head < at.queue_tail;
    let open = at.walk.is_some() || at.manifest.is_some() || queued;
    let flat_walk = at.flat == Some(true)
        && at.manifest.is_none()
        && !queued
        && at
            .walk
            .as_ref()
            .is_some_and(|walk| walk.queue_row.is_none());
    match flat_walk {
        true => {
            next.prefixes_done = at.prefixes_done;
            next.walk = at.walk.clone();
            // The manifest's rows stay with the former job, so this one reads it before its walk.
            let manifest = spec.checksum_manifest.as_ref().zip(at.walk.as_ref());
            next.manifest = manifest.map(|(name, walk)| ManifestRead {
                source_path: format!("{}{name}", walk.source_path),
                offset: 0,
                observation: None,
            });
        }
        false => next.prefixes_done = at.prefixes_done.saturating_sub(u64::from(open)),
    }
    next
}

/// Stores the checkpoint a job submitted as `job_id` starts from, before it can run.
pub(crate) async fn seed_checkpoint(
    context: &crate::driver::DriverContext,
    job_id: JobId,
    checkpoint: &StagingJobCheckpoint,
) -> Result<(), String> {
    let value = postcard::to_allocvec(checkpoint).map_err(|error| error.to_string())?;
    let write = aruna_core::effects::StorageEffect::Write {
        key_space: STAGING_STATE_KEYSPACE.to_string(),
        key: checkpoint_key(job_id, None),
        value: ByteView::from(value),
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(write).await {
        aruna_core::events::Event::Storage(aruna_core::events::StorageEvent::WriteResult {
            ..
        }) => Ok(()),
        other => Err(format!("staging checkpoint not seeded: {other:?}")),
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
    let mirrors = mirrors::item_mirrors(ctx, spec, source_path).await?;
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
                restrictions: spec.auth_context.path_restrictions.clone(),
                mirrors,
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

/// Whether another coordinator of the distributed destination imports `key`.
async fn imported_elsewhere(
    ctx: &JobContext,
    spec: &StagingJobSpec,
    key: &str,
) -> Result<bool, ItemFailure> {
    let bucket = pinned_bucket(ctx, spec).await?;
    let own = walk::coordinated(ctx, &bucket, &[key])
        .await
        .map_err(ItemFailure::System)?;
    Ok(own.first() == Some(&false))
}

/// Whether `key` of the distributed destination moved to another coordinator after this job
/// took it, so a failed write of it is that coordinator's to retry.
pub(super) async fn moved_away(ctx: &JobContext, spec: &StagingJobSpec, key: &str) -> bool {
    matches!(imported_elsewhere(ctx, spec, key).await, Ok(true))
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
    writes: Vec<(KeySpace, Key, Value)>,
    deletes: Vec<(KeySpace, Key)>,
) -> Result<(), String> {
    persist_slot(ctx, None, checkpoint, writes, deletes).await
}

/// Writes the checkpoint of the job or of prefix lane `slot` with the given job rows.
pub(super) async fn persist_slot(
    ctx: &JobContext,
    slot: Option<u64>,
    checkpoint: &StagingJobCheckpoint,
    mut writes: Vec<(KeySpace, Key, Value)>,
    deletes: Vec<(KeySpace, Key)>,
) -> Result<(), String> {
    let value = postcard::to_allocvec(checkpoint).map_err(|error| error.to_string())?;
    writes.push((
        STAGING_STATE_KEYSPACE.to_string(),
        checkpoint_key(ctx.job_id, slot),
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

/// The checkpoint row of a job, or of its prefix lane `slot`, which extends the job's key.
pub(crate) fn checkpoint_key(job_id: JobId, slot: Option<u64>) -> ByteView {
    let mut key = job_id.to_bytes().to_vec();
    if let Some(slot) = slot {
        key.extend_from_slice(&slot.to_be_bytes());
    }
    ByteView::from(key)
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

/// Storage conflicts are transient: the job runs again later without spending an attempt.
fn conflict_error(message: impl Into<String>) -> JobRunOutcome {
    JobRunOutcome::Deferred(JobError::retryable(message.into()))
}

fn permanent_error(message: impl Into<String>) -> JobRunOutcome {
    JobRunOutcome::Failed(JobError::permanent(message.into()))
}

#[cfg(test)]
mod pure_tests {
    use super::walk::{directory_list_path, record_failure};
    use super::*;
    use crate::staging::page::rows::MAX_ERROR_SAMPLE;
    use ulid::Ulid;

    pub(super) fn test_spec() -> StagingJobSpec {
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
            table: None,
            mirrors: Vec::new(),
        }
    }

    // A handed-over job keeps a flat walk's position and restarts a prefix whose directory
    // queue stayed with the former coordinator; counters start at zero.
    #[test]
    fn handed_job_resumes() {
        use aruna_core::structs::execution::job::StagingWalk;
        let spec = test_spec();
        let walk = StagingWalk {
            source_path: "refseq/".to_string(),
            target_prefix: String::new(),
            cursor: None,
            queue_row: None,
            from: Some("refseq/k".to_string()),
        };
        let mut at = initial_checkpoint(&spec);
        at.flat = Some(true);
        at.items_done = 3;
        at.prefixes_done = 1;
        at.walk = Some(walk.clone());
        at.items_succeeded = 40;
        record_failure(&mut at, "a", "b", "denied".to_string());
        let flat = handed_checkpoint(&spec, &at);
        assert_eq!((flat.items_done, flat.prefixes_done), (3, 1));
        assert_eq!(flat.walk, Some(walk.clone()));
        assert_eq!((flat.items_succeeded, flat.items_failed), (0, 0));
        assert!(flat.errors.is_empty());
        let mut queued = at.clone();
        queued.flat = Some(false);
        queued.queue_tail = 2;
        queued.walk = Some(StagingWalk {
            queue_row: Some(0),
            ..walk
        });
        let restarted = handed_checkpoint(&spec, &queued);
        assert_eq!((restarted.prefixes_done, restarted.walk), (0, None));
        assert_eq!((restarted.queue_head, restarted.queue_tail), (0, 0));
        let mut finished = at;
        finished.walk = None;
        assert_eq!(handed_checkpoint(&spec, &finished).prefixes_done, 1);
    }

    // The manifest's checksums stay with the former job's rows, so a flat walk taken over
    // reads its manifest again from the start before its next page.
    #[test]
    fn handed_manifest_rereads() {
        use aruna_core::structs::execution::job::StagingWalk;
        let spec = StagingJobSpec {
            checksum_manifest: Some("SHA256SUMS".to_string()),
            ..test_spec()
        };
        let walk = StagingWalk {
            source_path: "refseq/".to_string(),
            target_prefix: String::new(),
            cursor: None,
            queue_row: None,
            from: Some("refseq/k".to_string()),
        };
        let mut at = initial_checkpoint(&spec);
        at.flat = Some(true);
        at.prefixes_done = 1;
        at.walk = Some(walk.clone());
        let handed = handed_checkpoint(&spec, &at);
        assert_eq!(handed.walk, Some(walk));
        let manifest = handed.manifest.expect("the manifest is read again");
        assert_eq!(manifest.source_path, "refseq/SHA256SUMS");
        assert_eq!((manifest.offset, manifest.observation), (0, None));
        assert_eq!(handed_checkpoint(&test_spec(), &at).manifest, None);
    }

    // An uploaded table read to its end while folders still wait keeps its version on handover.
    #[test]
    fn handed_table_pinned() {
        use aruna_core::structs::execution::job::TableRead;
        let mut spec = test_spec();
        spec.prefixes.clear();
        let rows = serde_json::json!({ "kind": "folder", "source": "{p}", "target": "{p}" });
        let table = serde_json::json!({ "source": { "object": { "bucket": "b", "key": "t.csv" } },
            "rows": rows });
        spec.table = Some(serde_json::from_value(table).unwrap());
        let mut at = initial_checkpoint(&spec);
        let version = Ulid::from_bytes([9; 16]);
        at.table = Some(Box::new(TableRead {
            offset: 4096,
            version_id: Some(version),
            columns: Some(vec!["p".to_string()]),
            done: true,
            ..TableRead::default()
        }));
        at.queue_tail = 3;
        let handed = handed_checkpoint(&spec, &at);
        let table = handed.table.expect("the table is read again");
        assert_eq!((table.offset, table.done, table.columns), (0, false, None));
        assert_eq!(table.version_id, Some(version));
        assert_eq!((handed.queue_head, handed.queue_tail), (0, 0));
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
            range_turns: Default::default(),
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
