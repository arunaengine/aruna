use std::collections::HashMap;
use std::thread;

use aruna_core::events::{Event, StorageEvent};
use aruna_core::{effects::StorageEffect, errors::StorageError};
use byteview::ByteView;
use crossfire::{mpsc, oneshot};
use fjall::{KeyspaceCreateOptions, OptimisticTxDatabase, Readable};
use ulid::Ulid;

use crate::errors::StorageLibError;
pub type Channel = crossfire::MTx<mpsc::Array<(StorageEffect, oneshot::TxOneshot<StorageEvent>)>>;

pub struct FjallStorage {
    db: OptimisticTxDatabase,
    read_txns: HashMap<Ulid, fjall::Snapshot>,
    write_txns: HashMap<Ulid, fjall::OptimisticWriteTx>,
}

pub struct StorageHandle {
    write_channel: Channel,
}

impl StorageHandle {
    pub fn new() -> (
        Self,
        crossfire::Rx<mpsc::Array<(StorageEffect, oneshot::TxOneshot<StorageEvent>)>>,
    ) {
        let (sender, receiver) = mpsc::bounded_blocking(2048);
        (
            StorageHandle {
                write_channel: sender,
            },
            receiver,
        )
    }

    pub async fn send_effect(&self, effect: StorageEffect) -> Event {
        let storage_event = {
            let (response_tx, response_rx) = crossfire::oneshot::oneshot();
            if let Err(_) = self.write_channel.send((effect, response_tx)) {
                Event::Storage(StorageEvent::Error {
                    error: StorageError::Unknown,
                });
            }
            match response_rx.await {
                Ok(event) => event,
                Err(_) => StorageEvent::Error {
                    error: StorageError::Unknown,
                },
            }
        };
        Event::Storage(storage_event)
    }
}

impl FjallStorage {
    pub fn new(path: &str) -> Result<StorageHandle, StorageLibError> {
        let db = OptimisticTxDatabase::builder(path).open()?;

        let (sender, receiver) = StorageHandle::new();

        thread::spawn(move || {
            let mut storage = FjallStorage {
                db,
                read_txns: HashMap::new(),
                write_txns: HashMap::new(),
            };
            storage.receive_loop(receiver);
        });

        Ok(sender)
    }

    pub fn receive_loop(
        &mut self,
        receiver: crossfire::Rx<mpsc::Array<(StorageEffect, oneshot::TxOneshot<StorageEvent>)>>,
    ) -> ! {
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
                    };
                    let _ = response_tx.send(event);
                }
                Err(_) => {
                    tracing::warn!(
                        "Storage receiver channel closed, shutting down storage thread."
                    );
                }
            }
        }
    }

    fn start_transaction(&mut self, read: bool) -> StorageEvent {
        let txn_id = Ulid::new();

        if read {
            let txn = self.db.read_tx();
            self.read_txns.insert(txn_id, txn);
        } else {
            match self.db.write_tx() {
                Ok(txn) => {
                    self.write_txns.insert(txn_id, txn);
                }
                Err(_e) => {
                    return StorageEvent::Error {
                        error: aruna_core::errors::StorageError::TransactionConflict,
                    };
                }
            };
        }
        StorageEvent::TransactionStarted { txn_id }
    }

    fn abort_transaction(&mut self, txn_id: Ulid) -> StorageEvent {
        let write_txn = self.write_txns.remove(&txn_id);
        let read_txn = self.read_txns.remove(&txn_id);

        if let Some(txn) = write_txn {
            txn.rollback();
            return StorageEvent::TransactionAborted { txn_id };
        }

        if read_txn.is_some() {
            // No rollback needed for read transactions
            StorageEvent::TransactionAborted { txn_id }
        } else {
            StorageEvent::Error {
                error: aruna_core::errors::StorageError::TransactionNotFound,
            }
        }
    }

    fn read(&mut self, key_space: String, key: ByteView, txn_id: Option<Ulid>) -> StorageEvent {
        let Ok(keyspace) = self
            .db
            .keyspace(&key_space, || KeyspaceCreateOptions::default())
        else {
            return StorageEvent::Error {
                error: aruna_core::errors::StorageError::Unknown,
            };
        };

        if let Some(txn_id) = txn_id {
            if let Some(txn) = self.read_txns.get(&txn_id) {
                match txn.get(keyspace, &key) {
                    Ok(value_opt) => StorageEvent::ReadResult {
                        key,
                        value: value_opt.map(|v| v.into()),
                    },
                    Err(_e) => StorageEvent::Error {
                        error: aruna_core::errors::StorageError::Unknown,
                    },
                }
            } else {
                StorageEvent::Error {
                    error: aruna_core::errors::StorageError::TransactionNotFound,
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
                    error: aruna_core::errors::StorageError::Unknown,
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
        let Ok(keyspace) = self
            .db
            .keyspace(&key_space, || KeyspaceCreateOptions::default())
        else {
            return StorageEvent::Error {
                error: aruna_core::errors::StorageError::Unknown,
            };
        };

        if let Some(txn_id) = txn_id {
            if let Some(txn) = self.write_txns.get_mut(&txn_id) {
                txn.insert(keyspace, key.clone(), value);
                StorageEvent::WriteResult { key }
            } else {
                StorageEvent::Error {
                    error: aruna_core::errors::StorageError::TransactionNotFound,
                }
            }
        } else {
            match keyspace.insert(key.clone(), value) {
                Ok(_) => StorageEvent::WriteResult { key },
                Err(_e) => StorageEvent::Error {
                    error: aruna_core::errors::StorageError::Unknown,
                },
            }
        }
    }

    fn commit_transaction(&mut self, txn_id: Ulid) -> StorageEvent {
        if let Some(_txn) = self.read_txns.remove(&txn_id) {
            // Read-only transactions do not need to commit
            return StorageEvent::TransactionCommitted { txn_id };
        }

        if let Some(txn) = self.write_txns.remove(&txn_id) {
            match txn.commit() {
                Ok(_) => StorageEvent::TransactionCommitted { txn_id },
                Err(_e) => StorageEvent::Error {
                    error: aruna_core::errors::StorageError::TransactionConflict,
                },
            }
        } else {
            StorageEvent::Error {
                error: aruna_core::errors::StorageError::TransactionNotFound,
            }
        }
    }

    fn delete(&mut self, key_space: String, key: ByteView, txn_id: Option<Ulid>) -> StorageEvent {
        let Ok(keyspace) = self
            .db
            .keyspace(&key_space, || KeyspaceCreateOptions::default())
        else {
            return StorageEvent::Error {
                error: aruna_core::errors::StorageError::Unknown,
            };
        };

        if let Some(txn_id) = txn_id {
            if let Some(txn) = self.write_txns.get_mut(&txn_id) {
                txn.remove(keyspace, key.clone());
                StorageEvent::DeleteResult { key }
            } else {
                StorageEvent::Error {
                    error: aruna_core::errors::StorageError::TransactionNotFound,
                }
            }
        } else {
            match keyspace.remove(key.clone()) {
                Ok(_) => StorageEvent::DeleteResult { key },
                Err(_e) => StorageEvent::Error {
                    error: aruna_core::errors::StorageError::Unknown,
                },
            }
        }
    }
}
