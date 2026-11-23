//! Reads staging checkpoints alpha.159 stored with a listing offset, where the walk cursor now
//! sits in the same enum slot; a finished one keeps its counts, an unfinished one walks again.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::execution::job::input::{
    ManifestRead, StagingJobCheckpoint, StagingJobError, StagingJobPhase, StagingWalk, TableRead,
};
use serde::{Deserialize, Serialize};

/// `ListCursor` as alpha.159 stored it: entries to skip where the walk cursor is now.
#[derive(Serialize, Deserialize)]
enum OffsetCursor {
    After(String),
    At(String),
    Offset(u64),
}

/// `StagingWalk` as alpha.159 stored it.
#[derive(Serialize, Deserialize)]
struct OffsetWalk {
    source_path: String,
    target_prefix: String,
    cursor: Option<OffsetCursor>,
    queue_row: Option<u64>,
    from: Option<String>,
}

/// `StagingJobCheckpoint` as alpha.159 stored it.
#[derive(Serialize, Deserialize)]
struct OffsetCheckpoint {
    phase: StagingJobPhase,
    flat: Option<bool>,
    items_done: u64,
    prefixes_done: u64,
    walk: Option<OffsetWalk>,
    manifest: Option<ManifestRead>,
    table: Option<Box<TableRead>>,
    queue_head: u64,
    queue_tail: u64,
    items_current: u64,
    items_succeeded: u64,
    items_failed: u64,
    items_total: Option<u64>,
    bytes_current: u64,
    bytes_total: Option<u64>,
    bytes_discovered: u64,
    current_path: Option<String>,
    errors: Vec<StagingJobError>,
}

/// What a checkpoint stopped at a listing offset becomes.
#[derive(Debug, PartialEq)]
pub(super) enum OffsetRow {
    /// A finished job's checkpoint without the cursor, which nothing reads any more.
    Finished(Vec<u8>),
    /// An unfinished job's checkpoint, removed so the job walks its source again: an offset
    /// names no path to continue after.
    Unfinished,
}

/// `None` unless every byte of `value` reads as an alpha.159 checkpoint stopped at an offset.
/// A finished row that also reads whole in the current shape stays as it is, so a row the
/// current node wrote is never rewritten from a misread; an unfinished one only walks again.
pub(super) fn offset_row(value: &[u8]) -> Option<OffsetRow> {
    let (old, rest) = postcard::take_from_bytes::<OffsetCheckpoint>(value).ok()?;
    let walk = old.walk.as_ref()?;
    if !rest.is_empty() || !matches!(walk.cursor, Some(OffsetCursor::Offset(_))) {
        return None;
    }
    if !matches!(
        old.phase,
        StagingJobPhase::Completed | StagingJobPhase::Failed
    ) {
        return Some(OffsetRow::Unfinished);
    }
    let current = postcard::from_bytes::<StagingJobCheckpoint>(value);
    if current
        .is_ok_and(|current| postcard::to_allocvec(&current).is_ok_and(|bytes| bytes == value))
    {
        return None;
    }
    let bytes = postcard::to_allocvec(&StagingJobCheckpoint::from(old)).ok()?;
    Some(OffsetRow::Finished(bytes))
}

impl From<OffsetCheckpoint> for StagingJobCheckpoint {
    fn from(old: OffsetCheckpoint) -> Self {
        Self {
            phase: old.phase,
            flat: old.flat,
            items_done: old.items_done,
            prefixes_done: old.prefixes_done,
            walk: old.walk.map(|walk| StagingWalk {
                source_path: walk.source_path,
                target_prefix: walk.target_prefix,
                cursor: None,
                queue_row: walk.queue_row,
                from: walk.from,
            }),
            manifest: old.manifest,
            table: old.table,
            queue_head: old.queue_head,
            queue_tail: old.queue_tail,
            items_current: old.items_current,
            items_succeeded: old.items_succeeded,
            items_failed: old.items_failed,
            items_total: old.items_total,
            bytes_current: old.bytes_current,
            bytes_total: old.bytes_total,
            bytes_discovered: old.bytes_discovered,
            current_path: old.current_path,
            errors: old.errors,
        }
    }
}

#[cfg(test)]
#[path = "walk_cursor_tests.rs"]
mod tests;
