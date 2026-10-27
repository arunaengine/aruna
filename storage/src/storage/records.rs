//! Runs read, write, delete, batch, iterate and last effects on a keyspace or transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{IterEnd, IterStart, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::StorageEvent;
use byteview::ByteView;
use fjall::{OptimisticTxKeyspace, Readable};
use ulid::Ulid;

use super::{
    FjallStorage, Store, Txn, batch_read_with, iterate_page, range_page, read_last_with,
    store_batch_read, store_iterate, store_last, store_read,
};

type PageResult = (Vec<(ByteView, ByteView)>, Option<ByteView>);

impl FjallStorage {
    /// The open transaction `txn_id` names, if any.
    fn txn_of(&self, txn_id: Option<Ulid>) -> Result<Option<&Txn>, StorageError> {
        let txn = |txn_id| {
            self.txns
                .get(&txn_id)
                .ok_or(StorageError::TransactionNotFound)
        };
        txn_id.map(txn).transpose()
    }

    /// The open write transaction `txn_id`.
    fn write_txn(&mut self, txn_id: Ulid) -> Result<&mut fjall::OptimisticWriteTx, StorageError> {
        match self.txns.get_mut(&txn_id) {
            Some(Txn::Write(txn)) => Ok(txn.as_mut()),
            _ => Err(StorageError::TransactionNotFound),
        }
    }

    /// The snapshot of a usage rebuild's view, which reads rows as the rebuild began.
    fn view(&self, txn_id: Option<Ulid>) -> Option<&fjall::Snapshot> {
        let txn_id = txn_id.filter(|txn_id| self.usage_views.contains(txn_id))?;
        match self.txns.get(&txn_id) {
            Some(Txn::Read(snapshot)) => Some(snapshot),
            _ => None,
        }
    }

    #[tracing::instrument(
        name = "storage.read",
        level = "debug",
        skip(self, key),
        fields(key_space = %key_space, key_len = key.as_ref().len(), txn_id = ?txn_id)
    )]
    pub(super) fn read(
        &mut self,
        key_space: String,
        key: ByteView,
        txn_id: Option<Ulid>,
    ) -> StorageEvent {
        if let Some(snapshot) = self.view(txn_id) {
            return match self.view_get(snapshot, &key_space, &key) {
                Ok(value) => StorageEvent::ReadResult { key, value },
                Err(error) => StorageEvent::Error { error },
            };
        }
        match self.txn_of(txn_id) {
            Ok(txn) => read_row(&self.store, txn, &key_space, key),
            Err(error) => StorageEvent::Error { error },
        }
    }

    pub(super) fn batch_read(
        &mut self,
        reads: Vec<(String, ByteView)>,
        txn_id: Option<Ulid>,
    ) -> StorageEvent {
        if let Some(snapshot) = self.view(txn_id) {
            let mut values = Vec::with_capacity(reads.len());
            for (key_space, key) in reads {
                match self.view_get(snapshot, &key_space, &key) {
                    Ok(value) => values.push((key, value)),
                    Err(error) => return StorageEvent::Error { error },
                }
            }
            return StorageEvent::BatchReadResult { values };
        }
        match self.txn_of(txn_id) {
            Ok(txn) => read_rows(&self.store, txn, reads),
            Err(error) => StorageEvent::Error { error },
        }
    }

    #[tracing::instrument(
        name = "storage.write",
        level = "debug",
        skip(self, key, value),
        fields(key_space = %key_space, key_len = key.as_ref().len(), value_len = value.as_ref().len(), txn_id = ?txn_id)
    )]
    pub(super) fn write(
        &mut self,
        key_space: String,
        key: ByteView,
        value: ByteView,
        txn_id: Option<Ulid>,
    ) -> StorageEvent {
        let keyspace = match self.store.resolve_keyspace(&key_space) {
            Ok(keyspace) => keyspace,
            Err(error) => return StorageEvent::Error { error },
        };
        if let Some(txn_id) = txn_id {
            return match self.write_txn(txn_id) {
                Ok(txn) => {
                    txn.insert(keyspace, key.clone(), value);
                    self.note_rows(txn_id, [(key_space.as_str(), &key)]);
                    StorageEvent::WriteResult { key }
                }
                Err(error) => StorageEvent::Error { error },
            };
        }
        let result = self.buffered_write_tx().and_then(|mut tx| {
            self.keep_preimages(&mut tx, [(key_space.as_str(), &key)])?;
            tx.insert(keyspace, key.clone(), value);
            self.commit_buffered(tx)?;
            self.persist_journal()
        });
        match result {
            Ok(()) => StorageEvent::WriteResult { key },
            Err(error) => StorageEvent::Error { error },
        }
    }

    #[tracing::instrument(
        name = "storage.batch_write",
        level = "debug",
        skip(self, writes),
        fields(write_count = writes.len(), txn_id = ?txn_id)
    )]
    pub(super) fn batch_write(
        &mut self,
        writes: Vec<(String, ByteView, ByteView)>,
        txn_id: Option<Ulid>,
    ) -> StorageEvent {
        let mut entries = Vec::with_capacity(writes.len());
        let mut resolved = Vec::with_capacity(writes.len());
        for (key_space, key, value) in writes {
            let keyspace = match self.store.resolve_keyspace(&key_space) {
                Ok(keyspace) => keyspace,
                Err(error) => return StorageEvent::Error { error },
            };
            resolved.push((keyspace, key_space, key, value));
        }
        if let Some(txn_id) = txn_id {
            let txn = match self.write_txn(txn_id) {
                Ok(txn) => txn,
                Err(error) => return StorageEvent::Error { error },
            };
            for (keyspace, key_space, key, value) in resolved {
                txn.insert(keyspace, key.clone(), value);
                entries.push((key_space, key));
            }
            self.note_rows(
                txn_id,
                entries.iter().map(|(space, key)| (space.as_str(), key)),
            );
        } else {
            let mut tx = match self.buffered_write_tx() {
                Ok(tx) => tx,
                Err(error) => return StorageEvent::Error { error },
            };
            for (keyspace, key_space, key, value) in resolved {
                tx.insert(keyspace, key.clone(), value);
                entries.push((key_space, key));
            }
            let rows = entries.iter().map(|(space, key)| (space.as_str(), key));
            if let Err(error) = self
                .keep_preimages(&mut tx, rows)
                .and_then(|()| self.commit_buffered(tx))
                .and_then(|()| self.persist_journal())
            {
                return StorageEvent::Error { error };
            }
        }
        StorageEvent::BatchWriteResult { entries }
    }

    #[tracing::instrument(
        name = "storage.delete",
        level = "debug",
        skip(self, key),
        fields(key_space = %key_space, key_len = key.as_ref().len(), txn_id = ?txn_id)
    )]
    pub(super) fn delete(
        &mut self,
        key_space: String,
        key: ByteView,
        txn_id: Option<Ulid>,
    ) -> StorageEvent {
        let keyspace = match self.store.resolve_keyspace(&key_space) {
            Ok(keyspace) => keyspace,
            Err(error) => return StorageEvent::Error { error },
        };
        if let Some(txn_id) = txn_id {
            return match self.write_txn(txn_id) {
                Ok(txn) => {
                    txn.remove(keyspace, key.clone());
                    self.hold_txn_delete(txn_id, &key_space);
                    self.note_rows(txn_id, [(key_space.as_str(), &key)]);
                    StorageEvent::DeleteResult { key }
                }
                Err(error) => StorageEvent::Error { error },
            };
        }
        let mut tx = match self.buffered_write_tx() {
            Ok(tx) => tx,
            Err(error) => return StorageEvent::Error { error },
        };
        tx.remove(keyspace, key.clone());
        if let Err(error) = self
            .keep_preimages(&mut tx, [(key_space.as_str(), &key)])
            .and_then(|()| self.commit_buffered(tx))
            .and_then(|()| self.persist_journal())
        {
            return StorageEvent::Error { error };
        }
        self.note_deletes(&key_space, 1);
        StorageEvent::DeleteResult { key }
    }

    #[tracing::instrument(
        name = "storage.batch_delete",
        level = "debug",
        skip(self, deletes),
        fields(delete_count = deletes.len(), txn_id = ?txn_id)
    )]
    pub(super) fn batch_delete(
        &mut self,
        deletes: Vec<(String, ByteView)>,
        txn_id: Option<Ulid>,
    ) -> StorageEvent {
        let mut entries = Vec::with_capacity(deletes.len());
        let mut resolved = Vec::with_capacity(deletes.len());
        for (key_space, key) in deletes {
            let keyspace = match self.store.resolve_keyspace(&key_space) {
                Ok(keyspace) => keyspace,
                Err(error) => return StorageEvent::Error { error },
            };
            resolved.push((keyspace, key_space, key));
        }
        if let Some(txn_id) = txn_id {
            let txn = match self.write_txn(txn_id) {
                Ok(txn) => txn,
                Err(error) => return StorageEvent::Error { error },
            };
            for (keyspace, key_space, key) in resolved {
                txn.remove(keyspace, key.clone());
                entries.push((key_space, key));
            }
            for (key_space, _) in &entries {
                self.hold_txn_delete(txn_id, key_space);
            }
            self.note_rows(
                txn_id,
                entries.iter().map(|(space, key)| (space.as_str(), key)),
            );
        } else {
            let mut tx = match self.buffered_write_tx() {
                Ok(tx) => tx,
                Err(error) => return StorageEvent::Error { error },
            };
            for (keyspace, key_space, key) in resolved {
                tx.remove(keyspace, key.clone());
                entries.push((key_space, key));
            }
            let rows = entries.iter().map(|(space, key)| (space.as_str(), key));
            if let Err(error) = self
                .keep_preimages(&mut tx, rows)
                .and_then(|()| self.commit_buffered(tx))
                .and_then(|()| self.persist_journal())
            {
                return StorageEvent::Error { error };
            }
            for (key_space, _) in &entries {
                self.note_deletes(key_space, 1);
            }
        }
        StorageEvent::BatchDeleteResult { entries }
    }

    #[tracing::instrument(
        name = "storage.iterate",
        level = "debug",
        skip(self, prefix, start),
        fields(key_space = %key_space, has_prefix = prefix.is_some(), has_cursor = start.is_some(), limit, txn_id = ?txn_id)
    )]
    pub(super) fn iterate(
        &mut self,
        key_space: String,
        prefix: Option<ByteView>,
        start: Option<IterStart>,
        limit: usize,
        txn_id: Option<Ulid>,
    ) -> StorageEvent {
        if let Some(snapshot) = self.view(txn_id) {
            let page = match limit {
                0 => Ok((Vec::new(), None)),
                _ => self.view_page(snapshot, &key_space, prefix.as_ref(), start.as_ref(), limit),
            };
            return page_event(page);
        }
        match self.txn_of(txn_id) {
            Ok(txn) => iterate_rows(&self.store, txn, &key_space, (prefix, start, limit)),
            Err(error) => StorageEvent::Error { error },
        }
    }

    pub(super) fn iterate_range(
        &mut self,
        key_space: String,
        start: IterStart,
        end: IterEnd,
        budget: (usize, usize),
        txn_id: Option<Ulid>,
    ) -> StorageEvent {
        if self.view(txn_id).is_some() {
            return StorageEvent::Error {
                error: StorageError::InvalidEffect,
            };
        }
        match self.txn_of(txn_id) {
            Ok(txn) => range_rows(&self.store, txn, &key_space, (&start, &end), budget),
            Err(error) => StorageEvent::Error { error },
        }
    }

    pub(super) fn last(
        &mut self,
        key_space: String,
        prefix: Option<ByteView>,
        txn_id: Option<Ulid>,
    ) -> StorageEvent {
        if self.view(txn_id).is_some() {
            return StorageEvent::Error {
                error: StorageError::InvalidEffect,
            };
        }
        match self.txn_of(txn_id) {
            Ok(txn) => last_row(&self.store, txn, &key_space, prefix),
            Err(error) => StorageEvent::Error { error },
        }
    }
}

/// Runs a read effect in `txn`, or on the latest committed rows without one.
pub(super) fn run_read(store: &Store, txn: Option<&Txn>, effect: StorageEffect) -> StorageEvent {
    match effect {
        StorageEffect::Read { key_space, key, .. } => read_row(store, txn, &key_space, key),
        StorageEffect::BatchRead { reads, .. } => read_rows(store, txn, reads),
        StorageEffect::Iter {
            key_space,
            prefix,
            start,
            limit,
            ..
        } => iterate_rows(store, txn, &key_space, (prefix, start, limit)),
        StorageEffect::IterRange {
            key_space,
            start,
            end,
            limit,
            max_bytes,
            ..
        } => range_rows(store, txn, &key_space, (&start, &end), (limit, max_bytes)),
        StorageEffect::Last {
            key_space, prefix, ..
        } => last_row(store, txn, &key_space, prefix),
        _ => StorageEvent::Error {
            error: StorageError::InvalidEffect,
        },
    }
}

/// A write transaction creates the keyspace it reads; other reads see an absent one as empty.
fn read_keyspace(
    store: &Store,
    txn: Option<&Txn>,
    key_space: &str,
) -> Result<Option<OptimisticTxKeyspace>, StorageError> {
    match txn {
        Some(Txn::Write(_)) => store.resolve_keyspace(key_space).map(Some),
        _ => store.existing_keyspace(key_space),
    }
}

fn page_event(page: Result<PageResult, StorageError>) -> StorageEvent {
    match page {
        Ok((values, next_start_after)) => StorageEvent::IterResult {
            values,
            next_start_after,
        },
        Err(error) => StorageEvent::Error { error },
    }
}

fn read_row(store: &Store, txn: Option<&Txn>, key_space: &str, key: ByteView) -> StorageEvent {
    let keyspace = match read_keyspace(store, txn, key_space) {
        Ok(Some(keyspace)) => keyspace,
        Ok(None) => return StorageEvent::ReadResult { key, value: None },
        Err(error) => return StorageEvent::Error { error },
    };
    let value = match txn {
        Some(Txn::Read(snapshot)) => snapshot.get(keyspace, &key),
        Some(Txn::Write(txn)) => txn.get(keyspace, &key),
        None => return store_read(store, keyspace, key),
    };
    match value {
        Ok(value) => StorageEvent::ReadResult {
            key,
            value: value.map(Into::into),
        },
        Err(error) => StorageEvent::Error {
            error: StorageError::ReadError(error.to_string()),
        },
    }
}

fn read_rows(store: &Store, txn: Option<&Txn>, reads: Vec<(String, ByteView)>) -> StorageEvent {
    match txn {
        Some(Txn::Read(snapshot)) => batch_read_with(store, snapshot, reads, false),
        Some(Txn::Write(txn)) => batch_read_with(store, txn.as_ref(), reads, true),
        None => store_batch_read(store, reads),
    }
}

fn iterate_rows(
    store: &Store,
    txn: Option<&Txn>,
    key_space: &str,
    (prefix, start, limit): (Option<ByteView>, Option<IterStart>, usize),
) -> StorageEvent {
    let keyspace = match read_keyspace(store, txn, key_space) {
        Ok(Some(keyspace)) if limit > 0 => keyspace,
        Ok(_) => return page_event(Ok((Vec::new(), None))),
        Err(error) => return StorageEvent::Error { error },
    };
    let page = match txn {
        Some(Txn::Read(snapshot)) => {
            iterate_page(snapshot, &keyspace, prefix.as_ref(), start.as_ref(), limit)
        }
        Some(Txn::Write(txn)) => iterate_page(
            txn.as_ref(),
            &keyspace,
            prefix.as_ref(),
            start.as_ref(),
            limit,
        ),
        None => return store_iterate(store, keyspace, prefix, start, limit),
    };
    page_event(page)
}

fn range_rows(
    store: &Store,
    txn: Option<&Txn>,
    key_space: &str,
    (start, end): (&IterStart, &IterEnd),
    budget: (usize, usize),
) -> StorageEvent {
    let keyspace = match read_keyspace(store, txn, key_space) {
        Ok(Some(keyspace)) => keyspace,
        Ok(None) => return page_event(Ok((Vec::new(), None))),
        Err(error) => return StorageEvent::Error { error },
    };
    page_event(match txn {
        Some(Txn::Read(snapshot)) => range_page(snapshot, &keyspace, start, end, budget),
        Some(Txn::Write(txn)) => range_page(txn.as_ref(), &keyspace, start, end, budget),
        None => range_page(&store.db.read_tx(), &keyspace, start, end, budget),
    })
}

fn last_row(
    store: &Store,
    txn: Option<&Txn>,
    key_space: &str,
    prefix: Option<ByteView>,
) -> StorageEvent {
    let keyspace = match read_keyspace(store, txn, key_space) {
        Ok(Some(keyspace)) => keyspace,
        Ok(None) => return page_event(Ok((Vec::new(), None))),
        Err(error) => return StorageEvent::Error { error },
    };
    match txn {
        Some(Txn::Read(snapshot)) => read_last_with(snapshot, &keyspace, prefix.as_ref()),
        Some(Txn::Write(txn)) => read_last_with(txn.as_ref(), &keyspace, prefix.as_ref()),
        None => store_last(store, keyspace, prefix),
    }
}
