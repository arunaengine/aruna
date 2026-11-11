//! Rewrites, in batches, rows whose record gained trailing fields; a missing field reads as none
//! or false.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::session_refs::{self, Layout};
use super::{decode_error, row_note};
use crate::explorer::ExplorerError;
use aruna_core::keyspaces::{
    ADMIN_AUDIT_KEYSPACE, HARVEST_SOURCE_KEYSPACE, MIRROR_REPAIR_KEYSPACE,
};
use fjall::{OptimisticTxDatabase, OptimisticTxKeyspace, Readable};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::ops::Bound;

/// A keyspace and the stored field counts of its record's older layouts, newest first.
pub(super) type Shapes = (&'static str, &'static [Layout]);

/// Admin audit rows before `secret_use` (alpha.188), before `plugin`, and before `secret`.
pub(super) const AUDIT: Shapes = (
    ADMIN_AUDIT_KEYSPACE,
    &[
        &[("AdminAuditRecord", 8)],
        &[("AdminAuditRecord", 7)],
        &[("AdminAuditRecord", 6)],
    ],
);
/// Harvest sources before `last_job`.
pub(super) const HARVEST: Shapes = (HARVEST_SOURCE_KEYSPACE, &[&[("HarvestSource", 12)]]);
/// Pending sync mirror repairs before `rollback`.
pub(super) const MIRROR: Shapes = (MIRROR_REPAIR_KEYSPACE, &[&[("SyncMirrorRecord", 5)]]);

/// Rows read, and rewritten at most, per transaction.
const BATCH_ROWS: usize = 1024;

/// Rows read and rewritten in one keyspace.
#[derive(Debug, Default, PartialEq)]
pub(super) struct Counts {
    pub(super) scanned: usize,
    pub(super) rewritten: usize,
}

/// Rows of `T` in an older layout gain the missing fields; current rows stay as they are. Rows
/// that match no layout are added to `unknown` and left unchanged.
pub(super) fn missing_fields<T: Serialize + DeserializeOwned>(
    db: &OptimisticTxDatabase,
    keyspace: &OptimisticTxKeyspace,
    shapes: Shapes,
    unknown: &mut Vec<String>,
) -> Result<Counts, ExplorerError> {
    batched::<T>(db, keyspace, shapes, BATCH_ROWS, unknown)
}

fn batched<T: Serialize + DeserializeOwned>(
    db: &OptimisticTxDatabase,
    keyspace: &OptimisticTxKeyspace,
    shapes: Shapes,
    size: usize,
    unknown: &mut Vec<String>,
) -> Result<Counts, ExplorerError> {
    let mut counts = Counts::default();
    let mut after = None;
    loop {
        after = next_batch::<T>(db, keyspace, shapes, (after, size), &mut counts, unknown)?;
        if after.is_none() {
            return Ok(counts);
        }
    }
}

/// Rewrites up to `size` rows after the key `after` in one commit. Returns the last key read
/// while more rows may follow.
fn next_batch<T: Serialize + DeserializeOwned>(
    db: &OptimisticTxDatabase,
    keyspace: &OptimisticTxKeyspace,
    (name, layouts): Shapes,
    (after, size): (Option<Vec<u8>>, usize),
    counts: &mut Counts,
    unknown: &mut Vec<String>,
) -> Result<Option<Vec<u8>>, ExplorerError> {
    let start = after.map_or(Bound::Unbounded, Bound::Excluded);
    let mut rows = Vec::new();
    let mut read = 0;
    let mut last = None;
    for entry in db
        .read_tx()
        .range(keyspace, (start, Bound::Unbounded))
        .take(size)
    {
        let (key, value) = entry.into_inner()?;
        read += 1;
        match classify::<T>(&value, layouts) {
            Row::Current => {}
            Row::Older(record) => {
                let bytes = postcard::to_allocvec(&record)
                    .map_err(|error| decode_error(name, &key, error))?;
                rows.push((key.to_vec(), bytes));
            }
            Row::Unknown => unknown.push(row_note(name, &key, "matches no known shape")),
        }
        last = Some(key.to_vec());
    }
    counts.scanned += read;
    counts.rewritten += rows.len();
    if !rows.is_empty() {
        let mut txn = db.write_tx()?;
        for (key, value) in rows {
            txn.insert(keyspace.clone(), key, value);
        }
        txn.commit()?.map_err(|_| {
            ExplorerError::Decode("migration conflicted with a running node".to_string())
        })?;
    }
    Ok(last.filter(|_| read == size))
}

/// What one stored row is, read with all of its bytes.
#[derive(Debug, PartialEq)]
enum Row<T> {
    Current,
    Older(T),
    Unknown,
}

fn classify<T: DeserializeOwned>(value: &[u8], layouts: &[Layout]) -> Row<T> {
    if session_refs::decode::<T>(value, &[]).is_ok() {
        return Row::Current;
    }
    layouts
        .iter()
        .find_map(|layout| session_refs::decode::<T>(value, layout).ok())
        .map_or(Row::Unknown, Row::Older)
}

#[cfg(test)]
#[path = "trailing_tests.rs"]
mod tests;
