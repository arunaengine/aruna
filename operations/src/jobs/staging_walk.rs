//! Walks one source listing page of a staging job: lists it, maps entries to keys and queue
//! rows, and records the page through the page import or the snapshot copy.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::path::{Component, Path};
use std::time::SystemTime;

use aruna_core::keyspaces::STAGING_QUEUE_KEYSPACE;
use aruna_core::structs::execution::job::{
    StagingJobCheckpoint, StagingJobDirectory, StagingJobError, StagingJobPhase, StagingJobSpec,
    StagingWalk,
};
use aruna_core::structs::execution::source_access::{
    ListRequest, SourceEntry, SourceEntryKind, SourceMetadata,
};
use aruna_core::structs::execution::staging::StagingStrategy;
use aruna_core::structs::identity::auth::Permission;
use aruna_core::structs::storage::blob::object_permission_path;
use aruna_core::types::{Key, KeySpace, Value};
use byteview::ByteView;

use super::executor::JobContext;
use super::staging::{
    ItemFailure, load_live_bucket, persist_rows, queue_key, stage_item, sync_progress,
};
use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::driver::drive;
use crate::staging::head_source::{HeadSourceInput, HeadSourceOperation};
use crate::staging::list_source::{ListStagingInput, ListStagingOperation, ListStagingResult};
use crate::staging::page_import::{
    PageEntry, PageImportError, PageImportInput, PageImportOperation,
};
use crate::staging::page_rows::MAX_ERROR_SAMPLE;

/// Entries one reference page lists and registers in one transaction.
const STAGING_PAGE_SIZE: usize = 500;
/// Snapshot pages copy every file before their checkpoint, so they stay smaller.
const SNAPSHOT_PAGE_SIZE: usize = 100;

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

async fn commit_rows(
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
    .map_err(|error| ItemFailure::Stage(error.to_string()))
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
            },
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
        match stage_item(ctx, spec, &file.source_path, &file.target_key).await {
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
            source_path: entry_path,
            target_key: target,
            size: entry.size,
        }),
        SourceEntryKind::Directory => MappedEntry::Directory(StagingJobDirectory {
            source_path: directory_list_path(&entry_path),
            target_prefix: target,
            cursor: None,
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
