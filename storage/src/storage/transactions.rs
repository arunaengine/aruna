use aruna_core::effects::StorageEffect;
use aruna_core::errors::StorageError;
use aruna_core::events::StorageEvent;
use tracing::warn;
use ulid::Ulid;

use crate::compaction::{DELETE_THRESHOLD, compactable};

use super::{CleanupEntry, CleanupKind, FjallStorage, MAX_TRANSACTION_CLEANUP, Txn};

impl FjallStorage {
    #[tracing::instrument(
        name = "storage.start_transaction",
        level = "debug",
        skip(self),
        fields(read)
    )]
    pub(super) fn start_transaction(&mut self, read: bool) -> StorageEvent {
        let txn_id = loop {
            let candidate = Ulid::generate();
            let mut pending = self
                .transaction_cleanup
                .lock()
                .expect("transaction cleanup mutex poisoned");
            if pending.len() >= MAX_TRANSACTION_CLEANUP {
                return StorageEvent::Error {
                    error: StorageError::CleanupCapacity,
                };
            }
            if pending.contains_key(&candidate) {
                continue;
            }
            pending.insert(
                candidate,
                CleanupEntry {
                    kind: CleanupKind::Open,
                    attempts: 0,
                    queued: false,
                },
            );
            break candidate;
        };

        let txn = if read {
            Txn::Read(self.store.db.read_tx())
        } else {
            match self.store.db.write_tx() {
                Ok(txn) => Txn::Write(Box::new(
                    txn.durability(Some(self.persist_policy.as_fjall())),
                )),
                Err(_) => {
                    self.transaction_cleanup
                        .lock()
                        .expect("transaction cleanup mutex poisoned")
                        .remove(&txn_id);
                    return StorageEvent::Error {
                        error: StorageError::TransactionConflict,
                    };
                }
            }
        };

        self.txns.insert(txn_id, txn);
        StorageEvent::TransactionStarted { txn_id }
    }

    #[tracing::instrument(name = "storage.abort_transaction", level = "debug", skip(self), fields(txn_id = %txn_id))]
    pub(super) fn abort_transaction(&mut self, txn_id: Ulid) -> StorageEvent {
        if self
            .transaction_cleanup
            .lock()
            .expect("transaction cleanup mutex poisoned")
            .get(&txn_id)
            .is_some_and(|entry| {
                matches!(
                    entry.kind,
                    CleanupKind::CommitQueued
                        | CleanupKind::CommitUnknown
                        | CleanupKind::Committed
                        | CleanupKind::Aborted
                )
            })
        {
            return StorageEvent::Error {
                error: StorageError::TransactionConflict,
            };
        }
        self.take_txn_deletes(txn_id, false);
        match self.txns.remove(&txn_id) {
            Some(Txn::Write(txn)) => {
                txn.rollback();
                StorageEvent::TransactionAborted { txn_id }
            }
            Some(Txn::Read(_)) => StorageEvent::TransactionAborted { txn_id },
            None => StorageEvent::Error {
                error: StorageError::TransactionNotFound,
            },
        }
    }

    #[tracing::instrument(name = "storage.commit_transaction", level = "debug", skip(self), fields(txn_id = %txn_id))]
    pub(super) fn commit_transaction(&mut self, txn_id: Ulid) -> StorageEvent {
        let state = self
            .transaction_cleanup
            .lock()
            .expect("transaction cleanup mutex poisoned")
            .get(&txn_id)
            .map(|entry| entry.kind);
        match state {
            Some(CleanupKind::Abort | CleanupKind::Aborted) => {
                return StorageEvent::Error {
                    error: StorageError::TransactionConflict,
                };
            }
            Some(CleanupKind::CommitQueued) => {}
            Some(CleanupKind::CommitUnknown) if self.txns.contains_key(&txn_id) => {}
            Some(CleanupKind::CommitUnknown) => {
                return StorageEvent::Error {
                    error: StorageError::CommitFailed,
                };
            }
            Some(CleanupKind::Open) => {
                if let Some(entry) = self
                    .transaction_cleanup
                    .lock()
                    .expect("transaction cleanup mutex poisoned")
                    .get_mut(&txn_id)
                {
                    entry.kind = CleanupKind::CommitQueued;
                }
            }
            Some(CleanupKind::Committed) => {
                return StorageEvent::TransactionCommitted { txn_id };
            }
            None => {}
        }

        match self.txns.remove(&txn_id) {
            Some(Txn::Read(_)) => StorageEvent::TransactionCommitted { txn_id },
            Some(Txn::Write(txn)) => {
                let committed = txn.commit();
                self.take_txn_deletes(txn_id, matches!(committed, Ok(Ok(()))));
                match committed {
                    Ok(Ok(())) => StorageEvent::TransactionCommitted { txn_id },
                    Ok(Err(_)) => StorageEvent::Error {
                        error: StorageError::TransactionConflict,
                    },
                    Err(error) => {
                        warn!(
                            event = "storage.transaction.commit_failed",
                            txn_id = %txn_id,
                            error = %error,
                            "Storage transaction commit failed with an unknown outcome"
                        );
                        StorageEvent::Error {
                            error: StorageError::CommitFailed,
                        }
                    }
                }
            }
            None => StorageEvent::Error {
                error: StorageError::TransactionNotFound,
            },
        }
    }

    pub(super) fn note_deletes(&mut self, key_space: &str, count: u64) {
        let total = self.deletes.entry(key_space.to_string()).or_default();
        *total += count;
        if *total < DELETE_THRESHOLD {
            return;
        }
        let deletes = std::mem::take(total);
        match self.store.resolve_keyspace(key_space) {
            Ok(keyspace) => {
                let disk_space = AsRef::<fjall::Keyspace>::as_ref(&keyspace).disk_space();
                if !compactable(disk_space) {
                    tracing::debug!(
                        event = "storage.keyspace.compact_skipped",
                        key_space,
                        disk_space,
                        deletes,
                        "Keyspace is too large for delete-triggered compaction"
                    );
                    return;
                }
                self.compactor.submit(key_space, deletes, keyspace);
            }
            Err(error) => warn!(
                event = "storage.keyspace.compact_failed",
                key_space,
                error = %error,
                "Keyspace compaction could not resolve the keyspace"
            ),
        }
    }

    pub(super) fn note_effect_deletes(&mut self, effect: &StorageEffect) {
        match effect {
            StorageEffect::Delete { key_space, .. } => self.note_deletes(key_space, 1),
            StorageEffect::BatchDelete { deletes, .. } => {
                for (key_space, _) in deletes {
                    self.note_deletes(key_space, 1);
                }
            }
            _ => {}
        }
    }

    pub(super) fn hold_txn_delete(&mut self, txn_id: Ulid, key_space: &str) {
        *self
            .txn_deletes
            .entry(txn_id)
            .or_default()
            .entry(key_space.to_string())
            .or_default() += 1;
    }

    pub(super) fn take_txn_deletes(&mut self, txn_id: Ulid, committed: bool) {
        let Some(deletes) = self.txn_deletes.remove(&txn_id) else {
            return;
        };
        if committed {
            for (key_space, count) in deletes {
                self.note_deletes(&key_space, count);
            }
        }
    }
}
