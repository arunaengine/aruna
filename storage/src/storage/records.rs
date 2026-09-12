use aruna_core::effects::IterStart;
use aruna_core::errors::StorageError;
use aruna_core::events::StorageEvent;
use byteview::ByteView;
use fjall::Readable;
use ulid::Ulid;

use super::{
    FjallStorage, Txn, batch_read_with, iterate_page, read_last_with, store_batch_read,
    store_iterate, store_last, store_read,
};

impl FjallStorage {
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
        let keyspace = match self.store.resolve_keyspace(&key_space) {
            Ok(keyspace) => keyspace,
            Err(error) => return StorageEvent::Error { error },
        };
        if let Some(txn_id) = txn_id {
            return match self.txns.get(&txn_id) {
                Some(Txn::Read(txn)) => match txn.get(keyspace, &key) {
                    Ok(value) => StorageEvent::ReadResult {
                        key,
                        value: value.map(|value| value.into()),
                    },
                    Err(error) => StorageEvent::Error {
                        error: StorageError::ReadError(error.to_string()),
                    },
                },
                Some(Txn::Write(txn)) => match txn.get(keyspace, &key) {
                    Ok(value) => StorageEvent::ReadResult {
                        key,
                        value: value.map(|value| value.into()),
                    },
                    Err(error) => StorageEvent::Error {
                        error: StorageError::ReadError(error.to_string()),
                    },
                },
                None => StorageEvent::Error {
                    error: StorageError::TransactionNotFound,
                },
            };
        }
        store_read(&self.store, keyspace, key)
    }

    pub(super) fn batch_read(
        &mut self,
        reads: Vec<(String, ByteView)>,
        txn_id: Option<Ulid>,
    ) -> StorageEvent {
        if let Some(txn_id) = txn_id {
            return match self.txns.get(&txn_id) {
                Some(Txn::Read(txn)) => batch_read_with(&self.store, txn, reads),
                Some(Txn::Write(txn)) => batch_read_with(&self.store, txn.as_ref(), reads),
                None => StorageEvent::Error {
                    error: StorageError::TransactionNotFound,
                },
            };
        }
        store_batch_read(&self.store, reads)
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
            return if let Some(Txn::Write(txn)) = self.txns.get_mut(&txn_id) {
                txn.insert(keyspace, key.clone(), value);
                StorageEvent::WriteResult { key }
            } else {
                StorageEvent::Error {
                    error: StorageError::TransactionNotFound,
                }
            };
        }
        let result = self.buffered_write_tx().and_then(|mut tx| {
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
            let Some(Txn::Write(txn)) = self.txns.get_mut(&txn_id) else {
                return StorageEvent::Error {
                    error: StorageError::TransactionNotFound,
                };
            };
            for (keyspace, key_space, key, value) in resolved {
                txn.insert(keyspace, key.clone(), value);
                entries.push((key_space, key));
            }
        } else {
            let mut tx = match self.buffered_write_tx() {
                Ok(tx) => tx,
                Err(error) => return StorageEvent::Error { error },
            };
            for (keyspace, key_space, key, value) in resolved {
                tx.insert(keyspace, key.clone(), value);
                entries.push((key_space, key));
            }
            if let Err(error) = self
                .commit_buffered(tx)
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
            return if let Some(Txn::Write(txn)) = self.txns.get_mut(&txn_id) {
                txn.remove(keyspace, key.clone());
                self.hold_txn_delete(txn_id, &key_space);
                StorageEvent::DeleteResult { key }
            } else {
                StorageEvent::Error {
                    error: StorageError::TransactionNotFound,
                }
            };
        }
        let mut tx = match self.buffered_write_tx() {
            Ok(tx) => tx,
            Err(error) => return StorageEvent::Error { error },
        };
        tx.remove(keyspace, key.clone());
        if let Err(error) = self
            .commit_buffered(tx)
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
            let Some(Txn::Write(txn)) = self.txns.get_mut(&txn_id) else {
                return StorageEvent::Error {
                    error: StorageError::TransactionNotFound,
                };
            };
            for (keyspace, key_space, key) in resolved {
                txn.remove(keyspace, key.clone());
                entries.push((key_space, key));
            }
            for (key_space, _) in &entries {
                self.hold_txn_delete(txn_id, key_space);
            }
        } else {
            let mut tx = match self.buffered_write_tx() {
                Ok(tx) => tx,
                Err(error) => return StorageEvent::Error { error },
            };
            for (keyspace, key_space, key) in resolved {
                tx.remove(keyspace, key.clone());
                entries.push((key_space, key));
            }
            if let Err(error) = self
                .commit_buffered(tx)
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
        let keyspace = match self.store.resolve_keyspace(&key_space) {
            Ok(keyspace) => keyspace,
            Err(error) => return StorageEvent::Error { error },
        };
        if limit == 0 {
            return StorageEvent::IterResult {
                values: Vec::new(),
                next_start_after: None,
            };
        }
        let result = if let Some(txn_id) = txn_id {
            match self.txns.get(&txn_id) {
                Some(Txn::Read(txn)) => {
                    iterate_page(txn, &keyspace, prefix.as_ref(), start.as_ref(), limit)
                }
                Some(Txn::Write(txn)) => iterate_page(
                    txn.as_ref(),
                    &keyspace,
                    prefix.as_ref(),
                    start.as_ref(),
                    limit,
                ),
                None => {
                    return StorageEvent::Error {
                        error: StorageError::TransactionNotFound,
                    };
                }
            }
        } else {
            return store_iterate(&self.store, keyspace, prefix, start, limit);
        };
        match result {
            Ok((values, next_start_after)) => StorageEvent::IterResult {
                values,
                next_start_after,
            },
            Err(error) => StorageEvent::Error { error },
        }
    }

    pub(super) fn last(
        &mut self,
        key_space: String,
        prefix: Option<ByteView>,
        txn_id: Option<Ulid>,
    ) -> StorageEvent {
        let keyspace = match self.store.resolve_keyspace(&key_space) {
            Ok(keyspace) => keyspace,
            Err(error) => return StorageEvent::Error { error },
        };
        if let Some(txn_id) = txn_id {
            return match self.txns.get(&txn_id) {
                Some(Txn::Read(txn)) => read_last_with(txn, &keyspace, prefix.as_ref()),
                Some(Txn::Write(txn)) => read_last_with(txn.as_ref(), &keyspace, prefix.as_ref()),
                None => StorageEvent::Error {
                    error: StorageError::TransactionNotFound,
                },
            };
        }
        store_last(&self.store, keyspace, prefix)
    }
}
