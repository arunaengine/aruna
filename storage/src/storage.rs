use std::collections::HashMap;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::thread;

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use async_trait::async_trait;
use byteview::ByteView;
use crossfire::{mpsc, oneshot};
use fjall::{KeyspaceCreateOptions, OptimisticTxDatabase, OptimisticTxKeyspace, Readable};
use ulid::Ulid;

use crate::errors::StorageLibError;
pub type EffectHandle = (StorageEffect, oneshot::TxOneshot<StorageEvent>);
pub type EffectSender = crossfire::MTx<mpsc::Array<EffectHandle>>;
pub type EffectReceiver = crossfire::Rx<mpsc::Array<EffectHandle>>;

enum Txn {
    Read(fjall::Snapshot),
    Write(Box<fjall::OptimisticWriteTx>),
}

type PageResult = (Vec<(ByteView, ByteView)>, Option<ByteView>);

pub struct FjallStorage {
    db: OptimisticTxDatabase,
    keyspaces: HashMap<String, OptimisticTxKeyspace>,
    txns: HashMap<Ulid, Txn>,
}

#[derive(Clone, Debug)]
pub struct StorageHandle {
    write_channel: EffectSender,
}

impl StorageHandle {
    pub fn new() -> (Self, EffectReceiver) {
        let (sender, receiver) = mpsc::bounded_blocking(2048);
        (
            StorageHandle {
                write_channel: sender,
            },
            receiver,
        )
    }

    pub async fn send_storage_effect(&self, effect: StorageEffect) -> Event {
        let storage_event = {
            let (response_tx, response_rx) = crossfire::oneshot::oneshot();
            if self.write_channel.send((effect, response_tx)).is_err() {
                return Event::Storage(StorageEvent::Error {
                    error: StorageError::ChannelClosed,
                });
            }
            match response_rx.await {
                Ok(event) => event,
                Err(_) => StorageEvent::Error {
                    error: StorageError::ChannelClosed,
                },
            }
        };
        Event::Storage(storage_event)
    }
}

#[async_trait]
impl Handle for StorageHandle {
    async fn send_effect(&self, effect: Effect) -> Event {
        match effect {
            Effect::Storage(storage_effect) => {
                let (response_tx, response_rx) = crossfire::oneshot::oneshot();
                if self
                    .write_channel
                    .send((storage_effect, response_tx))
                    .is_err()
                {
                    return Event::Storage(StorageEvent::Error {
                        error: StorageError::ChannelClosed,
                    });
                }
                match response_rx.await {
                    Ok(event) => Event::Storage(event),
                    Err(_) => Event::Storage(StorageEvent::Error {
                        error: StorageError::ChannelClosed,
                    }),
                }
            }
            _ => Event::Storage(StorageEvent::Error {
                error: StorageError::InvalidEffect,
            }),
        }
    }
}

impl FjallStorage {
    pub fn open(path: &str) -> Result<StorageHandle, StorageLibError> {
        let db = OptimisticTxDatabase::builder(path).open()?;

        let (sender, receiver) = StorageHandle::new();

        thread::spawn(move || {
            let mut storage = FjallStorage {
                db,
                keyspaces: HashMap::new(),
                txns: HashMap::new(),
            };
            storage.receive_loop(receiver);
        });

        Ok(sender)
    }

    pub fn receive_loop(&mut self, receiver: EffectReceiver) {
        loop {
            match receiver.recv() {
                Ok((effect, response_tx)) => {
                    let event = match effect {
                        StorageEffect::StartTransaction { read } => self.start_transaction(read),
                        StorageEffect::AbortTransaction { txn_id } => {
                            self.abort_transaction(txn_id)
                        }
                        StorageEffect::Read {
                            key_space,
                            key,
                            txn_id,
                        } => self.read(key_space, key, txn_id),
                        StorageEffect::Write {
                            key_space,
                            key,
                            value,
                            txn_id,
                        } => self.write(key_space, key, value, txn_id),
                        StorageEffect::CommitTransaction { txn_id } => {
                            self.commit_transaction(txn_id)
                        }
                        StorageEffect::Delete {
                            key_space,
                            key,
                            txn_id,
                        } => self.delete(key_space, key, txn_id),
                        StorageEffect::Iter {
                            key_space,
                            prefix,
                            start_after,
                            limit,
                            txn_id,
                        } => self.iterate(key_space, prefix, start_after, limit, txn_id),
                    };
                    response_tx.send(event);
                }
                Err(_) => {
                    tracing::warn!(
                        "Storage receiver channel closed, shutting down storage thread."
                    );
                    break;
                }
            }
        }
    }

    fn start_transaction(&mut self, read: bool) -> StorageEvent {
        let txn_id = Ulid::new();

        if read {
            let txn = self.db.read_tx();
            self.txns.insert(txn_id, Txn::Read(txn));
        } else {
            match self.db.write_tx() {
                Ok(txn) => {
                    self.txns.insert(txn_id, Txn::Write(Box::new(txn)));
                }
                Err(_e) => {
                    return StorageEvent::Error {
                        error: StorageError::TransactionConflict,
                    };
                }
            };
        }
        StorageEvent::TransactionStarted { txn_id }
    }

    fn abort_transaction(&mut self, txn_id: Ulid) -> StorageEvent {
        let txn = self.txns.remove(&txn_id);

        match txn {
            Some(Txn::Write(txn)) => {
                txn.rollback();
                StorageEvent::TransactionAborted { txn_id }
            }
            Some(Txn::Read(_txn)) => StorageEvent::TransactionAborted { txn_id },
            None => StorageEvent::Error {
                error: StorageError::TransactionNotFound,
            },
        }
    }

    fn get_or_create_keyspace(&mut self, name: &str) -> Result<OptimisticTxKeyspace, StorageError> {
        if let Some(ks) = self.keyspaces.get(name) {
            return Ok(ks.clone());
        }

        match self.db.keyspace(name, KeyspaceCreateOptions::default) {
            Ok(ks) => {
                self.keyspaces.insert(name.to_string(), ks.clone());
                Ok(ks)
            }
            Err(_) => Err(StorageError::KeyspaceError),
        }
    }

    fn read(&mut self, key_space: String, key: ByteView, txn_id: Option<Ulid>) -> StorageEvent {
        let keyspace = match self.get_or_create_keyspace(&key_space) {
            Ok(ks) => ks,
            Err(e) => return StorageEvent::Error { error: e },
        };

        if let Some(txn_id) = txn_id {
            if let Some(txn) = self.txns.get(&txn_id) {
                match txn {
                    Txn::Read(txn) => match txn.get(keyspace, &key) {
                        Ok(value_opt) => StorageEvent::ReadResult {
                            key,
                            value: value_opt.map(|v| v.into()),
                        },
                        Err(_e) => StorageEvent::Error {
                            error: StorageError::ReadError,
                        },
                    },
                    Txn::Write(txn) => match txn.get(keyspace, &key) {
                        Ok(value_opt) => StorageEvent::ReadResult {
                            key,
                            value: value_opt.map(|v| v.into()),
                        },
                        Err(_e) => StorageEvent::Error {
                            error: StorageError::ReadError,
                        },
                    },
                }
            } else {
                StorageEvent::Error {
                    error: StorageError::TransactionNotFound,
                }
            }
        } else {
            // Non-transactional read
            let snapshot = self.db.read_tx();
            match snapshot.get(&keyspace, &key) {
                Ok(value_opt) => StorageEvent::ReadResult {
                    key,
                    value: value_opt.map(|v| v.into()),
                },
                Err(_e) => StorageEvent::Error {
                    error: StorageError::ReadError,
                },
            }
        }
    }

    fn write(
        &mut self,
        key_space: String,
        key: ByteView,
        value: ByteView,
        txn_id: Option<Ulid>,
    ) -> StorageEvent {
        let keyspace = match self.get_or_create_keyspace(&key_space) {
            Ok(ks) => ks,
            Err(e) => return StorageEvent::Error { error: e },
        };

        if let Some(txn_id) = txn_id {
            if let Some(Txn::Write(txn)) = self.txns.get_mut(&txn_id) {
                txn.insert(keyspace, key.clone(), value);
                StorageEvent::WriteResult { key }
            } else {
                StorageEvent::Error {
                    error: StorageError::TransactionNotFound,
                }
            }
        } else {
            match keyspace.insert(key.clone(), value) {
                Ok(_) => StorageEvent::WriteResult { key },
                Err(_e) => StorageEvent::Error {
                    error: StorageError::WriteError,
                },
            }
        }
    }

    fn commit_transaction(&mut self, txn_id: Ulid) -> StorageEvent {
        match self.txns.remove(&txn_id) {
            Some(Txn::Read(_txn)) => {
                // Read-only transactions do not need to commit
                StorageEvent::TransactionCommitted { txn_id }
            }

            Some(Txn::Write(txn)) => match txn.commit() {
                Ok(_) => StorageEvent::TransactionCommitted { txn_id },
                Err(_e) => StorageEvent::Error {
                    error: StorageError::TransactionConflict,
                },
            },
            None => StorageEvent::Error {
                error: StorageError::TransactionNotFound,
            },
        }
    }

    fn delete(&mut self, key_space: String, key: ByteView, txn_id: Option<Ulid>) -> StorageEvent {
        let keyspace = match self.get_or_create_keyspace(&key_space) {
            Ok(ks) => ks,
            Err(e) => return StorageEvent::Error { error: e },
        };

        if let Some(txn_id) = txn_id {
            if let Some(Txn::Write(txn)) = self.txns.get_mut(&txn_id) {
                txn.remove(keyspace, key.clone());
                StorageEvent::DeleteResult { key }
            } else {
                StorageEvent::Error {
                    error: StorageError::TransactionNotFound,
                }
            }
        } else {
            match keyspace.remove(key.clone()) {
                Ok(_) => StorageEvent::DeleteResult { key },
                Err(_e) => StorageEvent::Error {
                    error: StorageError::DeleteError,
                },
            }
        }
    }

    fn iterate(
        &mut self,
        key_space: String,
        prefix: Option<ByteView>,
        start_after: Option<ByteView>,
        limit: usize,
        txn_id: Option<Ulid>,
    ) -> StorageEvent {
        let keyspace = match self.get_or_create_keyspace(&key_space) {
            Ok(ks) => ks,
            Err(e) => return StorageEvent::Error { error: e },
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
                    iterate_page(txn, &keyspace, prefix.as_ref(), start_after.as_ref(), limit)
                }
                Some(Txn::Write(txn)) => iterate_page(
                    txn.as_ref(),
                    &keyspace,
                    prefix.as_ref(),
                    start_after.as_ref(),
                    limit,
                ),
                None => {
                    return StorageEvent::Error {
                        error: StorageError::TransactionNotFound,
                    };
                }
            }
        } else {
            let snapshot = self.db.read_tx();
            iterate_page(
                &snapshot,
                &keyspace,
                prefix.as_ref(),
                start_after.as_ref(),
                limit,
            )
        };

        match result {
            Ok((values, next_start_after)) => StorageEvent::IterResult {
                values,
                next_start_after,
            },
            Err(error) => StorageEvent::Error { error },
        }
    }
}

fn iterate_page<R: Readable>(
    reader: &R,
    keyspace: &OptimisticTxKeyspace,
    prefix: Option<&ByteView>,
    start_after: Option<&ByteView>,
    limit: usize,
) -> Result<PageResult, StorageError> {
    let prefix_bytes = prefix.map(|p| p.as_ref().to_vec());
    let start_after_bytes = start_after.map(|s| s.as_ref().to_vec());

    let iter = match (prefix_bytes.as_ref(), start_after_bytes.as_ref()) {
        (Some(prefix), Some(start_after)) => {
            let start_bound = if start_after < prefix {
                Included(prefix.clone())
            } else {
                Excluded(start_after.clone())
            };

            match prefix_upper_bound(prefix) {
                Some(end) => reader.range(keyspace, (start_bound, Excluded(end))),
                None => reader.range(keyspace, (start_bound, Unbounded::<Vec<u8>>)),
            }
        }
        (Some(prefix), None) => match prefix_upper_bound(prefix) {
            Some(end) => reader.range(keyspace, (Included(prefix.clone()), Excluded(end))),
            None => reader.range(keyspace, (Included(prefix.clone()), Unbounded::<Vec<u8>>)),
        },
        (None, Some(start_after)) => reader.range(
            keyspace,
            (Excluded(start_after.clone()), Unbounded::<Vec<u8>>),
        ),
        (None, None) => reader.iter(keyspace),
    };

    collect_page(iter, limit)
}

fn collect_page(iter: fjall::Iter, limit: usize) -> Result<PageResult, StorageError> {
    let mut iter = iter.peekable();
    let mut values: Vec<(ByteView, ByteView)> = Vec::with_capacity(limit.min(1024));

    while let Some(guard) = iter.next() {
        let (key, value) = guard.into_inner().map_err(|_| StorageError::ReadError)?;
        values.push((ByteView::from(key.as_ref()), ByteView::from(value.as_ref())));

        if values.len() == limit {
            let next_start_after = if iter.peek().is_some() {
                values.last().map(|(k, _)| k.clone())
            } else {
                None
            };
            return Ok((values, next_start_after));
        }
    }

    Ok((values, None))
}

fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper = prefix.to_vec();
    for idx in (0..upper.len()).rev() {
        if upper[idx] != u8::MAX {
            upper[idx] = upper[idx].saturating_add(1);
            upper.truncate(idx + 1);
            return Some(upper);
        }
    }

    None
}
