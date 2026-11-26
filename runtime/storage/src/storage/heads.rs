//! Removes the watch key of an unpublished name in each commit that changes its real head, and
//! tells the head observer about committed head changes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;
use std::sync::{Arc, OnceLock};

use aruna_core::errors::StorageError;
use aruna_core::keyspaces::BLOB_HEAD_KEYSPACE;
use aruna_core::structs::storage::blob::BlobHeadKey;
use aruna_core::structs::storage::vfs::{HeadChangeObserver, VfsWatchKey};
use byteview::ByteView;
use fjall::Readable;
use ulid::Ulid;

use super::worker::FjallStorage;

/// The observer of committed head changes, shared by a handle, its clones and its store.
#[derive(Clone, Default)]
pub(super) struct HeadObservers(Arc<OnceLock<Arc<dyn HeadChangeObserver>>>);

impl std::fmt::Debug for HeadObservers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HeadObservers")
    }
}

impl HeadObservers {
    /// Installs `observer` once; false when one is installed already.
    pub(super) fn install(&self, observer: Arc<dyn HeadChangeObserver>) -> bool {
        self.0.set(observer).is_ok()
    }

    pub(super) fn notify(&self, heads: &HeadRows) {
        if let Some(observer) = self.0.get()
            && !heads.heads.is_empty()
        {
            observer.on_heads_changed(&heads.parsed());
        }
    }
}

/// The real heads one change writes or deletes, and the watch keys it writes or deletes itself.
#[derive(Debug, Default)]
pub(super) struct HeadRows {
    heads: BTreeSet<ByteView>,
    watches: BTreeSet<ByteView>,
}

impl HeadRows {
    pub(super) fn of<'a>(rows: impl IntoIterator<Item = (&'a str, &'a ByteView)>) -> Self {
        let mut found = Self::default();
        let rows = rows
            .into_iter()
            .filter(|(space, _)| *space == BLOB_HEAD_KEYSPACE);
        for (_, key) in rows {
            let set = match VfsWatchKey::is_watch_key(key.as_ref()) {
                true => &mut found.watches,
                false => &mut found.heads,
            };
            set.insert(key.clone());
        }
        found
    }

    pub(super) fn is_empty(&self) -> bool {
        self.heads.is_empty() && self.watches.is_empty()
    }

    pub(super) fn extend(&mut self, other: Self) {
        self.heads.extend(other.heads);
        self.watches.extend(other.watches);
    }

    /// The watch keys of the changed heads that the change does not write or delete itself.
    pub(super) fn watch_keys(&self) -> Vec<ByteView> {
        let watch = |raw: &ByteView| {
            let head = BlobHeadKey::from_bytes(raw.as_ref()).ok()?;
            VfsWatchKey::of_head(&head)
                .to_bytes()
                .ok()
                .map(ByteView::from)
        };
        let keys = self.heads.iter().filter_map(watch);
        keys.filter(|key| !self.watches.contains(key)).collect()
    }

    fn parsed(&self) -> Vec<BlobHeadKey> {
        let heads = self.heads.iter();
        heads
            .filter_map(|raw| BlobHeadKey::from_bytes(raw.as_ref()).ok())
            .collect()
    }
}

impl FjallStorage {
    /// Removes in `tx` each present watch key of `heads` and keeps its preimage. The reads join
    /// the conflict set of `tx`, so a watch key installed meanwhile fails the commit.
    pub(super) fn drop_watches(
        &self,
        tx: &mut fjall::OptimisticWriteTx,
        heads: &HeadRows,
    ) -> Result<(), StorageError> {
        let keys = heads.watch_keys();
        if keys.is_empty() {
            return Ok(());
        }
        let keyspace = self.store.resolve_keyspace(BLOB_HEAD_KEYSPACE)?;
        let mut present = Vec::new();
        for key in keys {
            let value = tx.get(&keyspace, &key);
            let value = value.map_err(|error| StorageError::ReadError(error.to_string()))?;
            present.extend(value.map(|_| key));
        }
        self.keep_preimages(tx, present.iter().map(|key| (BLOB_HEAD_KEYSPACE, key)))?;
        for key in present {
            tx.remove(keyspace.clone(), key);
        }
        Ok(())
    }

    /// Remembers the heads and watch keys a write transaction changed, for its commit.
    pub(super) fn note_heads<'a>(
        &mut self,
        txn_id: Ulid,
        rows: impl IntoIterator<Item = (&'a str, &'a ByteView)>,
    ) {
        let rows = HeadRows::of(rows);
        if !rows.is_empty() {
            self.txn_heads.entry(txn_id).or_default().extend(rows);
        }
    }

    /// Commits an immediate change and tells the observer about its heads, also when the
    /// outcome is unknown; a conflict changed nothing.
    pub(super) fn commit_heads(
        &self,
        tx: fjall::OptimisticWriteTx,
        heads: &HeadRows,
    ) -> Result<(), StorageError> {
        let committed = self.commit_buffered(tx);
        if !matches!(committed, Err(StorageError::TransactionConflict)) {
            self.heads.notify(heads);
        }
        committed
    }
}
