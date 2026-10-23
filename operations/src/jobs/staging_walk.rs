//! Walks one source listing page of a staging job: lists it, maps entries to keys and queue
//! rows, and records the page through the page import or the snapshot copy.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::path::{Component, Path};
use std::time::SystemTime;

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
use aruna_core::structs::storage::blob::object_permission_path;
use aruna_core::types::{Key, KeySpace, Value};
use byteview::ByteView;
use tracing::warn;

use super::executor::{JobContext, JobRunOutcome};
use super::staging::{
    ItemFailure, load_live_bucket, persist_rows, queue_key, stage_item, sync_progress,
};
use super::staging_manifest::{ManifestDigest, manifest_digests};
use super::store::{batch_delete, iter_prefix_page, read_state};
use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::driver::drive;
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
        if let Err(error) = clear_rows(ctx, key_space).await {
            warn!(job_id = %ctx.job_id, error = %error, "Staging job row cleanup failed");
        }
    }
}

async fn clear_rows(ctx: &JobContext, key_space: &str) -> Result<(), String> {
    let storage = &ctx.driver.storage_handle;
    let prefix = ByteView::from(ctx.job_id.to_bytes().to_vec());
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
    let mut page = match list_page(ctx, spec, &walk, flat).await {
        Ok(page) => Some(page),
        Err(ItemFailure::Denied(message) | ItemFailure::Stage(message)) => {
            record_failure(&mut next, &walk.source_path, &walk.target_prefix, message);
            None
        }
        Err(ItemFailure::System(message)) => return Err(PageFailure::Retry(message)),
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
            next.walk = None;
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
    flat: bool,
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
                limit,
                recursive: flat,
                files_only: flat,
                ..Default::default()
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
    match error {
        ListStagingError::Resolve(_)
        | ListStagingError::Staging(
            StagingSourceError::NotFound
            | StagingSourceError::AccessDenied
            | StagingSourceError::UnsupportedKind(_)
            | StagingSourceError::InvalidListing(_)
            | StagingSourceError::EgressDenied(_),
        ) => ItemFailure::Stage(error.to_string()),
        error => ItemFailure::System(error.to_string()),
    }
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
    let bucket = match load_live_bucket(ctx, &spec.bucket).await {
        Ok(bucket) if bucket.group_id == spec.group_id => bucket,
        Ok(_) => {
            return Err(PageFailure::Permanent(
                "destination bucket group changed after job submission".to_string(),
            ));
        }
        Err(ItemFailure::System(message)) => return Err(PageFailure::Retry(message)),
        Err(ItemFailure::Denied(message) | ItemFailure::Stage(message)) => {
            return Err(PageFailure::Permanent(message));
        }
    };
    let mut entries = Vec::with_capacity(rows.files.len());
    for file in rows.files {
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
        }
    }
    let result = drive(
        PageImportOperation::new(PageImportInput {
            job_id: ctx.job_id,
            claim_token: ctx.claim_token,
            realm_id: spec.auth_context.realm_id,
            group_id: spec.group_id,
            user_id: spec.auth_context.user_id,
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
    for file in rows.files {
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
                record_failure(&mut next, &file.source_path, &file.target_key, message)
            }
            Err(ItemFailure::System(message)) => return Err(PageFailure::Retry(message)),
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
    .map_err(|error| ItemFailure::Stage(error.to_string()))
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

    fn walk(source_path: &str) -> StagingWalk {
        StagingWalk {
            source_path: source_path.to_string(),
            target_prefix: "copy".to_string(),
            cursor: None,
            queue_row: None,
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
