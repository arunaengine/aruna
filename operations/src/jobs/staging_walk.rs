//! Walks one source listing page of a staging job: lists it, maps entries to keys and queue
//! rows, and records the page through the page import or the snapshot copy.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::path::{Component, Path};
use std::time::SystemTime;

use aruna_core::effects::StorageEffect;
use aruna_core::errors::StagingSourceError;
use aruna_core::keyspaces::{STAGING_MANIFEST_KEYSPACE, STAGING_QUEUE_KEYSPACE};
use aruna_core::structs::checksum::ExpectedChecksum;
use aruna_core::structs::execution::job::{
    ManifestRead, StagingJobCheckpoint, StagingJobDirectory, StagingJobError, StagingJobPhase,
    StagingJobSpec, StagingWalk,
};
use aruna_core::structs::execution::source_access::{
    DigestOrigin, DigestScope, ListRequest, SourceDigest, SourceEntry, SourceEntryKind,
    SourceMetadata,
};
use aruna_core::structs::execution::staging::StagingStrategy;
use aruna_core::structs::identity::auth::Permission;
use aruna_core::structs::storage::blob::{BucketInfo, object_permission_path};
use aruna_core::structs::storage::distribution::{DistributionView, WriteRoute};
use aruna_core::types::{Key, KeySpace, Value};
use byteview::ByteView;
use tracing::warn;

use super::executor::{JobContext, JobRunOutcome};
use super::staging::{
    ItemFailure, moved_away, persist_rows, pinned_bucket, queue_key, stage_item, sync_progress,
};
use super::staging_manifest::{
    ManifestDigest, lasting_head, lasting_resolution, lasting_source, manifest_digests,
};
use super::store::{batch_delete, iter_prefix_page, read_state};
use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::distribution::guard::{GuardError, authority_reads, authority_rows, write_target};
use crate::driver::drive;
use crate::s3::object::origin::database_entry;
use crate::staging::head_source::{HeadSourceInput, HeadSourceOperation};
use crate::staging::list_source::{
    ListStagingError, ListStagingInput, ListStagingOperation, ListStagingResult,
};
use crate::staging::page_import::{
    PageEntry, PageImportError, PageImportInput, PageImportOperation,
};
use crate::staging::page_rows::MAX_ERROR_SAMPLE;

/// Entries one reference page lists and registers in one transaction.
const STAGING_PAGE_SIZE: usize = 500;
/// Snapshot pages copy every file before their checkpoint, so they stay smaller.
const SNAPSHOT_PAGE_SIZE: usize = 100;

/// Queue rows one cleanup pass deletes.
const QUEUE_CLEANUP_PAGE: usize = 256;

/// Starts the next listing: the next queued directory, else the next spec prefix.
/// `Ok(false)` once nothing is left to list.
pub(super) async fn start_walk(
    ctx: &JobContext,
    spec: &StagingJobSpec,
    checkpoint: &mut StagingJobCheckpoint,
) -> Result<bool, String> {
    if checkpoint.queue_head < checkpoint.queue_tail {
        let row = queue_key(ctx.job_id, checkpoint.queue_head);
        match read_state::<StagingJobDirectory>(
            &ctx.driver.storage_handle,
            STAGING_QUEUE_KEYSPACE,
            row,
            "staging queue read",
        )
        .await?
        {
            Some(directory) => {
                checkpoint.walk = Some(StagingWalk {
                    source_path: directory.source_path,
                    target_prefix: directory.target_prefix,
                    cursor: None,
                    queue_row: Some(checkpoint.queue_head),
                    from: None,
                });
            }
            None => checkpoint.queue_head += 1,
        }
        return Ok(true);
    }
    let Some(prefix) = usize::try_from(checkpoint.prefixes_done)
        .ok()
        .and_then(|done| spec.prefixes.get(done))
    else {
        return Ok(false);
    };
    checkpoint.walk = Some(StagingWalk {
        source_path: directory_list_path(&prefix.source_prefix),
        target_prefix: prefix.target_prefix.clone(),
        cursor: None,
        queue_row: None,
        from: None,
    });
    // The prefix's manifest is read before its first page, so every page can look it up.
    checkpoint.manifest = spec.checksum_manifest.as_ref().map(|name| ManifestRead {
        source_path: format!("{}{name}", directory_list_path(&prefix.source_prefix)),
        offset: 0,
        observation: None,
    });
    checkpoint.prefixes_done += 1;
    Ok(true)
}

/// Deletes the job's queue and manifest rows page by page; rows left by a failed pass stay
/// until the next run and do not change the outcome.
pub(super) async fn clear_queue(ctx: &JobContext) {
    for key_space in [STAGING_QUEUE_KEYSPACE, STAGING_MANIFEST_KEYSPACE] {
        let prefix = ByteView::from(ctx.job_id.to_bytes().to_vec());
        if let Err(error) = clear_rows(ctx, key_space, prefix).await {
            warn!(job_id = %ctx.job_id, error = %error, "Staging job row cleanup failed");
        }
    }
}

/// Deletes every row of `key_space` under `prefix`, page by page.
pub(super) async fn clear_rows(
    ctx: &JobContext,
    key_space: &str,
    prefix: ByteView,
) -> Result<(), String> {
    let storage = &ctx.driver.storage_handle;
    loop {
        let (rows, _) = iter_prefix_page(
            storage,
            key_space,
            Some(prefix.clone()),
            None,
            QUEUE_CLEANUP_PAGE,
            None,
        )
        .await?;
        if rows.is_empty() {
            return Ok(());
        }
        let deletes = rows
            .into_iter()
            .map(|(key, _)| (key_space.to_string(), key))
            .collect();
        batch_delete(storage, deletes, None).await?;
    }
}

pub(super) async fn finish_failed(ctx: &JobContext, outcome: JobRunOutcome) -> JobRunOutcome {
    clear_queue(ctx).await;
    outcome
}

pub(super) fn item_phase(spec: &StagingJobSpec) -> StagingJobPhase {
    match spec.strategy {
        StagingStrategy::Snapshot => StagingJobPhase::Downloading,
        StagingStrategy::Reference | StagingStrategy::Sync => StagingJobPhase::Registering,
    }
}

/// Counts one failed item and keeps its error while the sample has room.
pub(super) fn record_failure(
    checkpoint: &mut StagingJobCheckpoint,
    source_path: &str,
    target_key: &str,
    error: String,
) {
    checkpoint.items_current = checkpoint.items_current.saturating_add(1);
    checkpoint.items_failed = checkpoint.items_failed.saturating_add(1);
    if checkpoint.errors.len() < MAX_ERROR_SAMPLE {
        checkpoint.errors.push(StagingJobError {
            source_path: source_path.to_string(),
            target_key: target_key.to_string(),
            error,
        });
    }
}

pub(super) enum PageFailure {
    Retry(String),
    Permanent(String),
}

/// A file a listing page found, mapped to its destination key.
struct StagedFile {
    source_path: String,
    target_key: String,
    size: Option<u64>,
    observation: Option<SourceMetadata>,
    manifest: Option<ManifestDigest>,
}

/// Lists one page of the current walk and records it, with the walk's next position and the
/// queue changes, in one write.
pub(super) async fn walk_page(
    ctx: &JobContext,
    spec: &StagingJobSpec,
    checkpoint: &mut StagingJobCheckpoint,
) -> Result<(), PageFailure> {
    let Some(walk) = checkpoint.walk.clone() else {
        return Ok(());
    };
    let flat = checkpoint.flat == Some(true);
    checkpoint.phase = StagingJobPhase::Discovering;
    checkpoint.current_path = Some(walk.source_path.clone());
    let mut next = checkpoint.clone();
    let mut writes = Vec::new();
    let mut deletes = Vec::new();
    // A flat walk into a distributed bucket lists only the key ranges this node coordinates.
    let segment = match (flat, walk.queue_row) {
        (true, None) => match segment_of(ctx, spec, &walk).await? {
            Bounds::Whole => None,
            Bounds::Segment(segment) => Some(segment),
            Bounds::Done => {
                next.walk = None;
                return commit_rows(ctx, checkpoint, next, writes, deletes).await;
            }
        },
        _ => None,
    };
    let mut page = match list_page(ctx, spec, &walk, (flat, segment.as_ref())).await {
        Ok(page) => Some(page),
        Err(ItemFailure::Denied(message) | ItemFailure::Stage(message)) => {
            record_failure(&mut next, &walk.source_path, &walk.target_prefix, message);
            None
        }
        Err(ItemFailure::System(message)) => return Err(PageFailure::Retry(message)),
        Err(ItemFailure::Lasting(message)) => return Err(PageFailure::Permanent(message)),
    };
    let next_cursor = page.as_ref().and_then(|page| page.next_cursor.clone());
    if next_cursor.is_some() && next_cursor == walk.cursor {
        return Err(PageFailure::Permanent(
            "source listing pagination made no progress".to_string(),
        ));
    }
    match next_cursor {
        Some(cursor) => {
            next.walk = Some(StagingWalk {
                cursor: Some(cursor),
                ..walk.clone()
            })
        }
        None => {
            // The next coordinated range continues where this one ends.
            next.walk = (segment.as_ref())
                .and_then(|segment| segment.upper.clone())
                .map(|upper| StagingWalk {
                    cursor: None,
                    from: Some(upper),
                    ..walk.clone()
                });
            if let Some(row) = walk.queue_row {
                deletes.push((
                    STAGING_QUEUE_KEYSPACE.to_string(),
                    queue_key(ctx.job_id, row),
                ));
                next.queue_head = row + 1;
            }
        }
    }
    let mut files = Vec::new();
    let listed = page
        .as_mut()
        .map(|page| std::mem::take(&mut page.entries))
        .unwrap_or_default();
    for entry in listed {
        match map_source_entry(&walk, entry, flat) {
            Ok(MappedEntry::File(file)) => {
                next.bytes_discovered =
                    next.bytes_discovered.saturating_add(file.size.unwrap_or(0));
                files.push(file);
            }
            Ok(MappedEntry::Directory(directory)) => {
                let value = postcard::to_allocvec(&directory)
                    .map_err(|error| PageFailure::Permanent(error.to_string()))?;
                let key = queue_key(ctx.job_id, next.queue_tail);
                writes.push((
                    STAGING_QUEUE_KEYSPACE.to_string(),
                    key,
                    ByteView::from(value),
                ));
                next.queue_tail += 1;
            }
            Err(message) => {
                record_failure(&mut next, &walk.source_path, &walk.target_prefix, message)
            }
        }
    }
    if spec.checksum_manifest.is_some() {
        let paths: Vec<&str> = files.iter().map(|file| file.source_path.as_str()).collect();
        let digests = manifest_digests(ctx, &paths)
            .await
            .map_err(PageFailure::Retry)?;
        for (file, digest) in files.iter_mut().zip(digests) {
            file.manifest = digest;
        }
    }
    next.phase = item_phase(spec);
    let Some(page) = page else {
        // The listing failed for good: record that and move on.
        return commit_rows(ctx, checkpoint, next, writes, deletes).await;
    };
    let rows = PageRows {
        files,
        writes,
        deletes,
    };
    match spec.strategy {
        StagingStrategy::Reference => import_page(ctx, spec, checkpoint, next, rows, page).await,
        StagingStrategy::Snapshot | StagingStrategy::Sync => {
            snapshot_page(ctx, spec, checkpoint, next, rows).await
        }
    }
}

/// What one listing page adds: files to stage and the job's queue changes.
struct PageRows {
    files: Vec<StagedFile>,
    writes: Vec<(KeySpace, Key, Value)>,
    deletes: Vec<(KeySpace, Key)>,
}

pub(super) async fn commit_rows(
    ctx: &JobContext,
    checkpoint: &mut StagingJobCheckpoint,
    next: StagingJobCheckpoint,
    writes: Vec<(KeySpace, Key, Value)>,
    deletes: Vec<(KeySpace, Key)>,
) -> Result<(), PageFailure> {
    persist_rows(ctx, &next, writes, deletes)
        .await
        .map_err(PageFailure::Retry)?;
    *checkpoint = next;
    sync_progress(ctx, checkpoint);
    Ok(())
}

async fn list_page(
    ctx: &JobContext,
    spec: &StagingJobSpec,
    walk: &StagingWalk,
    (flat, segment): (bool, Option<&Segment>),
) -> Result<ListStagingResult, ItemFailure> {
    let allowed = drive(
        CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: spec.auth_context.clone(),
            path: source_permission_path(spec, &walk.source_path),
            required_permission: Permission::READ,
        }),
        &ctx.driver,
    )
    .await
    .map_err(|error| ItemFailure::System(error.to_string()))?;
    if !allowed {
        return Err(ItemFailure::Denied("permission denied".to_string()));
    }
    let limit = match spec.strategy {
        StagingStrategy::Snapshot => SNAPSHOT_PAGE_SIZE,
        StagingStrategy::Reference | StagingStrategy::Sync => STAGING_PAGE_SIZE,
    };
    drive(
        ListStagingOperation::new(ListStagingInput {
            group_id: spec.group_id,
            connector_id: spec.connector_id,
            source_path: walk.source_path.clone(),
            request: ListRequest {
                cursor: walk.cursor.clone(),
                lower: segment.and_then(|segment| segment.lower.clone()),
                stop: segment.and_then(|segment| segment.stop.clone()),
                limit,
                recursive: flat,
                files_only: flat,
            },
        }),
        &ctx.driver,
    )
    .await
    .map_err(list_failure)
}

/// A source that refuses the path skips it; any other listing error retries the page, so one
/// transient failure never abandons the rest of a large walk.
fn list_failure(error: ListStagingError) -> ItemFailure {
    let lasting = match &error {
        ListStagingError::Resolve(error) => lasting_resolution(error),
        ListStagingError::Staging(StagingSourceError::InvalidListing(_)) => true,
        ListStagingError::Staging(error) => lasting_source(error),
        _ => false,
    };
    match lasting {
        true => ItemFailure::Stage(error.to_string()),
        false => ItemFailure::System(error.to_string()),
    }
}

/// Which of `keys` this node imports: in a distributed bucket only the keys of ranges it
/// coordinates, since every coordinator runs its own job for its keys. A range this node
/// fenced for a transfer is skipped; its new coordinator imports it on its next run.
pub(super) async fn coordinated(
    ctx: &JobContext,
    bucket: &BucketInfo,
    keys: &[&str],
) -> Result<Vec<bool>, String> {
    let Some(binding) = bucket.distribution.as_ref() else {
        return Ok(vec![true; keys.len()]);
    };
    let read = StorageEffect::BatchRead {
        reads: authority_reads(binding.set_id),
        txn_id: None,
    };
    let event = ctx.driver.storage_handle.send_storage_effect(read).await;
    let (record, fence) = authority_rows(event).map_err(|error| error.to_string())?;
    let (record, fence) = (record.as_deref(), fence.as_deref());
    (keys.iter())
        .map(|key| match write_target(binding, key, record, fence) {
            Ok(WriteRoute::Local { .. }) => Ok(true),
            Ok(WriteRoute::Forward { .. }) => Ok(false),
            Err(error) if error.moved_away() => Ok(false),
            Err(error) => Err(error.to_string()),
        })
        .collect()
}

/// Where a target key falls among the source keys of a flat walk.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Mapped {
    /// Before every target key of the walk.
    Before,
    /// The source key that maps to it.
    At(String),
    /// After every target key of the walk.
    After,
}

/// The source key of a flat walk that target key `key` stands for: target keys are the walk's
/// target prefix plus the path below its source path, so the mapping keeps key order.
fn source_key(walk: &StagingWalk, key: &str) -> Mapped {
    let target = walk.target_prefix.trim_matches('/');
    let target = match target.is_empty() {
        true => String::new(),
        false => format!("{target}/"),
    };
    let relative = match key.strip_prefix(target.as_str()) {
        Some(relative) if !relative.is_empty() => relative,
        Some(_) => return Mapped::Before,
        None if key < target.as_str() => return Mapped::Before,
        None => return Mapped::After,
    };
    let base = walk.source_path.trim_matches('/');
    Mapped::At(match base.is_empty() {
        true => relative.to_string(),
        false => format!("{base}/{relative}"),
    })
}

/// The source keys of one coordinated key range: listed from `lower`, ending before `stop`.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Segment {
    lower: Option<String>,
    stop: Option<String>,
    /// The range's exclusive upper target key, where the next range starts.
    upper: Option<String>,
}

/// What a flat walk lists next.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Bounds {
    /// The whole prefix: the destination is an ordinary bucket.
    Whole,
    Segment(Segment),
    /// No coordinated range of the walk's prefix is left.
    Done,
}

/// The first coordinated range at or after `from` that holds target keys of the walk.
fn next_segment(
    walk: &StagingWalk,
    ranges: impl IntoIterator<Item = (String, Option<String>)>,
) -> Bounds {
    let from = walk.from.clone().unwrap_or_default();
    for (lower, upper) in ranges {
        if upper.as_deref().is_some_and(|upper| upper <= from.as_str()) {
            continue;
        }
        let stop = match upper.as_deref().map(|upper| source_key(walk, upper)) {
            Some(Mapped::Before) => continue,
            Some(Mapped::At(stop)) => Some(stop),
            Some(Mapped::After) | None => None,
        };
        let lower = match source_key(walk, lower.max(from.clone()).as_str()) {
            Mapped::Before => None,
            Mapped::At(lower) => Some(lower),
            Mapped::After => return Bounds::Done,
        };
        return Bounds::Segment(Segment { lower, stop, upper });
    }
    Bounds::Done
}

/// The bounds of the walk's next listing in a distributed destination.
async fn segment_of(
    ctx: &JobContext,
    spec: &StagingJobSpec,
    walk: &StagingWalk,
) -> Result<Bounds, PageFailure> {
    let bucket = match pinned_bucket(ctx, spec).await {
        Ok(bucket) => bucket,
        Err(ItemFailure::System(message)) => return Err(PageFailure::Retry(message)),
        Err(
            ItemFailure::Denied(message)
            | ItemFailure::Stage(message)
            | ItemFailure::Lasting(message),
        ) => return Err(PageFailure::Permanent(message)),
    };
    let Some(binding) = bucket.distribution.as_ref() else {
        return Ok(Bounds::Whole);
    };
    let read = StorageEffect::BatchRead {
        reads: authority_reads(binding.set_id),
        txn_id: None,
    };
    let event = ctx.driver.storage_handle.send_storage_effect(read).await;
    let retry = |error: GuardError| PageFailure::Retry(error.to_string());
    let (record, fence) = authority_rows(event).map_err(retry)?;
    let (record, fence) = (record.as_deref(), fence.as_deref());
    let view = (record.map(DistributionView::from_bytes).transpose())
        .map_err(|error| retry(error.into()))?
        .ok_or_else(|| retry(GuardError::RecordMissing(binding.set_id)))?;
    // A range this node fenced for a transfer is left to its new coordinator.
    let mut coordinated = Vec::new();
    for range in &view.snapshot().config.ranges {
        match write_target(binding, &range.lower, record, fence) {
            Ok(WriteRoute::Local { .. }) => {
                coordinated.push((range.lower.clone(), range.upper.clone()))
            }
            Ok(WriteRoute::Forward { .. }) => {}
            Err(error) if error.moved_away() => {}
            Err(error) => return Err(retry(error)),
        }
    }
    Ok(next_segment(walk, coordinated))
}

/// The files of a page this node imports.
async fn own_files(
    ctx: &JobContext,
    bucket: &BucketInfo,
    files: Vec<StagedFile>,
) -> Result<Vec<StagedFile>, PageFailure> {
    let keys: Vec<&str> = files.iter().map(|file| file.target_key.as_str()).collect();
    let own = coordinated(ctx, bucket, &keys)
        .await
        .map_err(PageFailure::Retry)?;
    Ok((files.into_iter().zip(own))
        .filter_map(|(file, own)| own.then_some(file))
        .collect())
}

/// Registers a page's files as references with listing metadata, in one transaction with the
/// checkpoint. Files a listing gives no size for are read with one HEAD each.
async fn import_page(
    ctx: &JobContext,
    spec: &StagingJobSpec,
    checkpoint: &mut StagingJobCheckpoint,
    mut next: StagingJobCheckpoint,
    rows: PageRows,
    page: ListStagingResult,
) -> Result<(), PageFailure> {
    let bucket = match pinned_bucket(ctx, spec).await {
        Ok(bucket) => bucket,
        Err(ItemFailure::System(message)) => return Err(PageFailure::Retry(message)),
        Err(
            ItemFailure::Denied(message)
            | ItemFailure::Stage(message)
            | ItemFailure::Lasting(message),
        ) => {
            return Err(PageFailure::Permanent(message));
        }
    };
    let files = own_files(ctx, &bucket, rows.files).await?;
    let mut entries = Vec::with_capacity(files.len());
    for file in files {
        let observation = match check_access(ctx, spec, &file.source_path, &file.target_key).await {
            Ok(()) => match file.observation {
                Some(observation) => Ok(observation),
                None => head_observation(ctx, spec, &file.source_path).await,
            }
            .map(|observation| with_manifest(observation, file.manifest)),
            Err(failure) => Err(failure),
        };
        match observation {
            Ok(observation) => entries.push(PageEntry {
                source_path: file.source_path,
                target_key: file.target_key,
                observation,
            }),
            Err(ItemFailure::Denied(message) | ItemFailure::Stage(message)) => {
                record_failure(&mut next, &file.source_path, &file.target_key, message)
            }
            Err(ItemFailure::System(message)) => return Err(PageFailure::Retry(message)),
            Err(ItemFailure::Lasting(message)) => return Err(PageFailure::Permanent(message)),
        }
    }
    let database = database_entry(
        &ctx.driver,
        (spec.auth_context.realm_id, spec.node_id),
        &spec.bucket,
        (bucket.distribution.as_ref()).map(|binding| binding.set_id),
    )
    .await;
    let result = drive(
        PageImportOperation::new(PageImportInput {
            job_id: ctx.job_id,
            claim_token: ctx.claim_token,
            realm_id: spec.auth_context.realm_id,
            group_id: spec.group_id,
            user_id: spec.auth_context.user_id,
            auth_context: spec.auth_context.clone(),
            node_id: spec.node_id,
            bucket: spec.bucket.clone(),
            expected_bucket: bucket,
            connector: page.connector,
            secret_fingerprint: page.secret_fingerprint,
            entries,
            checkpoint: next,
            writes: rows.writes,
            deletes: rows.deletes,
            now: SystemTime::now(),
            database,
        }),
        &ctx.driver,
    )
    .await;
    match result {
        Ok(result) => {
            *checkpoint = result.checkpoint;
            sync_progress(ctx, checkpoint);
            Ok(())
        }
        Err(error @ (PageImportError::BucketChanged | PageImportError::TooManyBytes(_))) => {
            Err(PageFailure::Permanent(error.to_string()))
        }
        // A changed connector, a lost claim or a conflict replays the page from its cursor.
        Err(error) => Err(PageFailure::Retry(error.to_string())),
    }
}

/// Copies a page's files one by one; the page's checkpoint follows its last file, and a
/// replayed page finds already copied files through the job's retry key.
async fn snapshot_page(
    ctx: &JobContext,
    spec: &StagingJobSpec,
    checkpoint: &mut StagingJobCheckpoint,
    mut next: StagingJobCheckpoint,
    rows: PageRows,
) -> Result<(), PageFailure> {
    let bucket = match pinned_bucket(ctx, spec).await {
        Ok(bucket) => bucket,
        Err(ItemFailure::System(message)) => return Err(PageFailure::Retry(message)),
        Err(
            ItemFailure::Denied(message)
            | ItemFailure::Stage(message)
            | ItemFailure::Lasting(message),
        ) => return Err(PageFailure::Permanent(message)),
    };
    for file in own_files(ctx, &bucket, rows.files).await? {
        if ctx.cancel.is_cancelled() || ctx.shutdown.is_cancelled() {
            return Ok(());
        }
        next.current_path = Some(file.source_path.clone());
        let expected = file
            .manifest
            .map(|(algorithm, digest)| vec![ExpectedChecksum { algorithm, digest }])
            .unwrap_or_default();
        match stage_item(ctx, spec, &file.source_path, &file.target_key, expected).await {
            Ok(size) => {
                next.items_current = next.items_current.saturating_add(1);
                next.items_succeeded = next.items_succeeded.saturating_add(1);
                next.bytes_current = next.bytes_current.saturating_add(size);
            }
            Err(ItemFailure::Denied(message) | ItemFailure::Stage(message)) => {
                // A key whose range moved meanwhile belongs to its new coordinator's job.
                if !moved_away(ctx, spec, &file.target_key).await {
                    record_failure(&mut next, &file.source_path, &file.target_key, message)
                }
            }
            Err(ItemFailure::System(message)) => return Err(PageFailure::Retry(message)),
            Err(ItemFailure::Lasting(message)) => return Err(PageFailure::Permanent(message)),
        }
        ctx.progress.set_current(next.items_current);
    }
    commit_rows(ctx, checkpoint, next, rows.writes, rows.deletes).await
}

/// Adds a shipped manifest digest to an observation; it binds every complete read.
fn with_manifest(
    mut observation: SourceMetadata,
    digest: Option<ManifestDigest>,
) -> SourceMetadata {
    if let Some((algorithm, digest)) = digest {
        let origin = DigestOrigin::Manifest;
        let scope = DigestScope::FullObject;
        let digest = SourceDigest {
            digest,
            scope,
            origin,
        };
        observation.digests.insert(algorithm, digest);
    }
    observation
}

async fn head_observation(
    ctx: &JobContext,
    spec: &StagingJobSpec,
    source_path: &str,
) -> Result<SourceMetadata, ItemFailure> {
    drive(
        HeadSourceOperation::new(HeadSourceInput {
            group_id: spec.group_id,
            connector_id: spec.connector_id,
            source_path: source_path.to_string(),
        }),
        &ctx.driver,
    )
    .await
    .map(|result| result.metadata)
    .map_err(|error| match lasting_head(&error) {
        true => ItemFailure::Stage(error.to_string()),
        // A transient failure replays the page instead of failing the file for good.
        false => ItemFailure::System(error.to_string()),
    })
}

/// READ on the source file and WRITE on the destination key, as the submitting user.
pub(super) async fn check_access(
    ctx: &JobContext,
    spec: &StagingJobSpec,
    source_path: &str,
    target_key: &str,
) -> Result<(), ItemFailure> {
    let source_path = source_permission_path(spec, source_path);
    let target_path = object_permission_path(
        spec.auth_context.realm_id,
        spec.group_id,
        spec.node_id,
        &spec.bucket,
        target_key,
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
    Ok(())
}

enum MappedEntry {
    File(StagedFile),
    Directory(StagingJobDirectory),
}

/// Maps a listed entry below the walk's path to its destination. A flat listing returns
/// files at any depth; a directory listing returns direct children only.
fn map_source_entry(
    walk: &StagingWalk,
    entry: SourceEntry,
    flat: bool,
) -> Result<MappedEntry, String> {
    let entry_path = canonical_walker_path(&entry.path)?;
    let relative = relative_entry_path(&walk.source_path, &entry_path, flat)?;
    let target = join_target_prefix(&walk.target_prefix, &relative);
    Ok(match entry.kind {
        SourceEntryKind::File => MappedEntry::File(StagedFile {
            observation: entry.observation(),
            manifest: None,
            source_path: entry_path,
            target_key: target,
            size: entry.size,
        }),
        SourceEntryKind::Directory => MappedEntry::Directory(StagingJobDirectory {
            source_path: directory_list_path(&entry_path),
            target_prefix: target,
        }),
    })
}

pub(super) fn directory_list_path(path: &str) -> String {
    if path.is_empty() || path.ends_with('/') {
        path.to_string()
    } else {
        format!("{path}/")
    }
}

fn relative_entry_path(base: &str, entry: &str, nested: bool) -> Result<String, String> {
    let base = base.trim_matches('/');
    let relative = if base.is_empty() {
        entry
    } else {
        entry
            .strip_prefix(base)
            .and_then(|suffix| suffix.strip_prefix('/'))
            .ok_or_else(|| "source listing returned an entry outside its directory".to_string())?
    };
    if relative.is_empty() || (!nested && relative.contains('/')) {
        return Err("source listing returned a non-child entry".to_string());
    }
    Ok(relative.to_string())
}

pub(super) fn canonical_walker_path(path: &str) -> Result<String, String> {
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

pub(super) fn source_permission_path(spec: &StagingJobSpec, source_path: &str) -> String {
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

#[cfg(test)]
mod pure_tests {
    use super::*;

    fn bounded(from: Option<&str>) -> StagingWalk {
        StagingWalk {
            source_path: "src/".to_string(),
            target_prefix: "copy".to_string(),
            cursor: None,
            queue_row: None,
            from: from.map(str::to_string),
        }
    }

    // Transient listing failures retry the page; only a refusing source skips its path.
    #[test]
    fn transient_listing_retries() {
        use aruna_core::errors::SourceResolutionError;
        let retried =
            |error: ListStagingError| matches!(list_failure(error), ItemFailure::System(_));
        let storage =
            SourceResolutionError::StorageError(aruna_core::errors::StorageError::CommitFailed);
        assert!(retried(ListStagingError::Resolve(storage)));
        assert!(retried(ListStagingError::Resolve(
            SourceResolutionError::ResolveFailed
        )));
        assert!(retried(
            StagingSourceError::ListError("reset".into()).into()
        ));
        assert!(!retried(ListStagingError::Resolve(
            SourceResolutionError::NotFound
        )));
        assert!(!retried(StagingSourceError::AccessDenied.into()));
    }

    #[test]
    fn target_keys_map_sources() {
        let walk = bounded(None);
        assert_eq!(source_key(&walk, "a"), Mapped::Before);
        assert_eq!(source_key(&walk, "copy/"), Mapped::Before);
        assert_eq!(
            source_key(&walk, "copy/x/1"),
            Mapped::At("src/x/1".to_string())
        );
        assert_eq!(source_key(&walk, "d"), Mapped::After);
    }

    #[test]
    fn segments_follow_ranges() {
        let ranges = || {
            vec![
                (String::new(), Some("copy/g".to_string())),
                ("copy/p".to_string(), Some("e".to_string())),
            ]
        };
        let first = Segment {
            lower: None,
            stop: Some("src/g".to_string()),
            upper: Some("copy/g".to_string()),
        };
        assert_eq!(
            next_segment(&bounded(None), ranges()),
            Bounds::Segment(first)
        );
        // The next walk starts at the end of the first range and skips to the second.
        let second = Segment {
            lower: Some("src/p".to_string()),
            stop: None,
            upper: Some("e".to_string()),
        };
        let after = next_segment(&bounded(Some("copy/g")), ranges());
        assert_eq!(after, Bounds::Segment(second));
        assert_eq!(next_segment(&bounded(Some("e")), ranges()), Bounds::Done);
        // A range wholly before the prefix is skipped.
        let early = vec![(String::new(), Some("b".to_string()))];
        assert_eq!(next_segment(&bounded(None), early), Bounds::Done);
    }

    fn walk(source_path: &str) -> StagingWalk {
        StagingWalk {
            source_path: source_path.to_string(),
            target_prefix: "copy".to_string(),
            cursor: None,
            queue_row: None,
            from: None,
        }
    }

    fn file(path: &str) -> SourceEntry {
        SourceEntry {
            name: path.rsplit('/').next().unwrap().to_string(),
            path: path.to_string(),
            kind: SourceEntryKind::File,
            size: Some(4),
            modified: None,
            etag: Some("\"9e107d9d372bb6826bd81d3542a419d6\"".to_string()),
            stat: None,
        }
    }

    #[test]
    fn directory_keeps_marker() {
        let entry = SourceEntry {
            name: "refseq".to_string(),
            kind: SourceEntryKind::Directory,
            size: None,
            etag: None,
            ..file("refseq/")
        };

        let MappedEntry::Directory(directory) = map_source_entry(&walk(""), entry, false).unwrap()
        else {
            panic!("expected directory")
        };
        assert_eq!(directory.source_path, "refseq/");
        assert_eq!(directory.target_prefix, "copy/refseq");
    }

    #[test]
    fn flat_maps_nested() {
        let MappedEntry::File(mapped) =
            map_source_entry(&walk("data/"), file("data/a/b.txt"), true).unwrap()
        else {
            panic!("expected file")
        };
        assert_eq!(mapped.target_key, "copy/a/b.txt");
        let observation = mapped.observation.unwrap();
        assert_eq!(observation.content_length, 4);
        assert_eq!(observation.content_type, None);
        assert_eq!(observation.digests.len(), 1);

        assert!(map_source_entry(&walk("data/"), file("data/a/b.txt"), false).is_err());
        assert!(map_source_entry(&walk("data/"), file("other/b.txt"), true).is_err());
    }
}
