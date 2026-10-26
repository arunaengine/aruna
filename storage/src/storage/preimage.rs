//! Keeps the rows a usage rebuild reads as they were when it began and reads them back that way.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// The first change of a row after the rebuild began stores its earlier value in the same commit.
// A resumed walk reads those values over its own snapshot, so it sees the rows it began with.

use std::collections::BTreeMap;

use aruna_core::effects::IterStart;
use aruna_core::errors::StorageError;
use aruna_core::keyspaces::USAGE_PREIMAGE_KEYSPACE;
use aruna_core::structs::storage::usage::USAGE_SOURCE_KEYSPACES;
use byteview::ByteView;
use fjall::Readable;

use super::{FjallStorage, iterate_page};

type PageResult = (Vec<(ByteView, ByteView)>, Option<ByteView>);

/// Value tag of a row that did not exist when the rebuild began.
const ABSENT: u8 = 0;
const PRESENT: u8 = 1;

pub(super) fn is_source(key_space: &str) -> bool {
    USAGE_SOURCE_KEYSPACES.contains(&key_space)
}

/// The keyspace name and a zero byte, which no keyspace name contains.
fn tag(key_space: &str) -> Vec<u8> {
    let mut tag = key_space.as_bytes().to_vec();
    tag.push(0);
    tag
}

fn tagged(key_space: &str, key: &[u8]) -> ByteView {
    let mut row = tag(key_space);
    row.extend_from_slice(key);
    ByteView::from(row)
}

fn kept_value(row: &[u8]) -> Result<Option<ByteView>, StorageError> {
    match row.split_first() {
        Some((&PRESENT, value)) => Ok(Some(ByteView::from(value))),
        Some((&ABSENT, [])) => Ok(None),
        _ => Err(StorageError::ReadError("malformed rebuild preimage".into())),
    }
}

fn read_error(error: impl std::fmt::Display) -> StorageError {
    StorageError::ReadError(error.to_string())
}

impl FjallStorage {
    /// Stages in `tx` the value each source row had at the last commit, unless a value is kept
    /// already. Only this worker commits, so nothing lands between this read and the commit.
    pub(super) fn keep_preimages<'a>(
        &self,
        tx: &mut fjall::OptimisticWriteTx,
        rows: impl IntoIterator<Item = (&'a str, &'a ByteView)>,
    ) -> Result<(), StorageError> {
        if self.usage_shadow.is_empty() {
            return Ok(());
        }
        let mut rows = rows.into_iter().filter(|(space, _)| is_source(space));
        let Some(first) = rows.next() else {
            return Ok(());
        };
        let snapshot = self.store.db.read_tx();
        let kept = self.store.resolve_keyspace(USAGE_PREIMAGE_KEYSPACE)?;
        for (key_space, key) in std::iter::once(first).chain(rows) {
            let row = tagged(key_space, key);
            if snapshot.get(&kept, &row).map_err(read_error)?.is_some() {
                continue;
            }
            let keyspace = self.store.resolve_keyspace(key_space)?;
            let value = match snapshot.get(&keyspace, key).map_err(read_error)? {
                Some(value) => [&[PRESENT][..], value.as_ref()].concat(),
                None => vec![ABSENT],
            };
            tx.insert(kept.clone(), row, ByteView::from(value));
        }
        Ok(())
    }

    /// Remembers the source rows a write transaction changed; their values are kept at commit.
    pub(super) fn note_rows<'a>(
        &mut self,
        txn_id: ulid::Ulid,
        rows: impl IntoIterator<Item = (&'a str, &'a ByteView)>,
    ) {
        let rows = rows.into_iter().filter(|(space, _)| is_source(space));
        let noted = rows.map(|(space, key)| (space.to_string(), key.clone()));
        let mut noted = noted.peekable();
        if noted.peek().is_some() {
            self.txn_rows.entry(txn_id).or_default().extend(noted);
        }
    }

    /// Drops every kept value; called only while no rebuild runs.
    pub(super) fn clear_preimages(&self) -> Result<(), StorageError> {
        let kept = self.store.resolve_keyspace(USAGE_PREIMAGE_KEYSPACE)?;
        kept.inner()
            .clear()
            .map_err(|error| StorageError::WriteError(error.to_string()))
    }

    /// Reads `key` as it was when the rebuild began.
    pub(super) fn view_get(
        &self,
        snapshot: &fjall::Snapshot,
        key_space: &str,
        key: &[u8],
    ) -> Result<Option<ByteView>, StorageError> {
        if is_source(key_space)
            && let Some(kept) = self.store.existing_keyspace(USAGE_PREIMAGE_KEYSPACE)?
            && let Some(row) = snapshot
                .get(&kept, tagged(key_space, key))
                .map_err(read_error)?
        {
            return kept_value(row.as_ref());
        }
        let Some(keyspace) = self.store.existing_keyspace(key_space)? else {
            return Ok(None);
        };
        Ok(snapshot
            .get(&keyspace, key)
            .map_err(read_error)?
            .map(Into::into))
    }

    /// One page of `key_space` as it was when the rebuild began: the snapshot's rows with the
    /// kept values laid over them, up to the last key both pages cover.
    pub(super) fn view_page(
        &self,
        snapshot: &fjall::Snapshot,
        key_space: &str,
        prefix: Option<&ByteView>,
        start: Option<&IterStart>,
        limit: usize,
    ) -> Result<PageResult, StorageError> {
        let (rows, next) = match self.store.existing_keyspace(key_space)? {
            Some(keyspace) => iterate_page(snapshot, &keyspace, prefix, start, limit)?,
            None => (Vec::new(), None),
        };
        let kept = match self.store.existing_keyspace(USAGE_PREIMAGE_KEYSPACE)? {
            Some(kept) if is_source(key_space) => kept,
            _ => return Ok((rows, next)),
        };
        let tag = tag(key_space);
        let kept_prefix = tagged(key_space, prefix.map_or(&[][..], |prefix| prefix.as_ref()));
        let kept_start = start.map(|start| match start {
            IterStart::After(key) => IterStart::After(tagged(key_space, key)),
            IterStart::At(key) => IterStart::At(tagged(key_space, key)),
        });
        let (values, kept_next) = iterate_page(
            snapshot,
            &kept,
            Some(&kept_prefix),
            kept_start.as_ref(),
            limit,
        )?;
        let strip = |key: &ByteView| ByteView::from(&key.as_ref()[tag.len()..]);
        // A page that ended early covers keys only up to its last row.
        let bound = [
            next.and(rows.last().map(|(key, _)| key.clone())),
            kept_next.and(values.last().map(|(key, _)| strip(key))),
        ]
        .into_iter()
        .flatten()
        .min();
        let mut merged: BTreeMap<ByteView, Option<ByteView>> = rows
            .into_iter()
            .map(|(key, value)| (key, Some(value)))
            .collect();
        for (key, row) in values {
            merged.insert(strip(&key), kept_value(row.as_ref())?);
        }
        let mut page: Vec<(ByteView, ByteView)> = merged
            .into_iter()
            .filter(|(key, _)| bound.as_ref().is_none_or(|bound| key <= bound))
            .filter_map(|(key, value)| value.map(|value| (key, value)))
            .collect();
        if page.len() > limit {
            page.truncate(limit);
            let last = page.last().map(|(key, _)| key.clone());
            return Ok((page, last));
        }
        Ok((page, bound))
    }
}
