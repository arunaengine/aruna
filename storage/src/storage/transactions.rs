//! Starts, commits and aborts worker transactions and counts deletes for compaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::StorageEffect;
use aruna_core::errors::StorageError;
use aruna_core::events::StorageEvent;
use aruna_core::structs::storage::usage::{UsageCounters, UsageDelta};
use byteview::ByteView;
use fjall::Readable;
use tracing::warn;
use ulid::Ulid;

use crate::compaction::{DELETE_THRESHOLD, compactable};

use super::{CleanupEntry, CleanupKind, FjallStorage, MAX_TRANSACTION_CLEANUP, Txn};

impl FjallStorage {
    #[tracing::instrument(
        name = "storage.add_usage",
        level = "debug",
        skip(self, deltas),
        fields(key_space = %key_space, delta_count = deltas.len(), txn_id = %txn_id)
    )]
    pub(super) fn add_usage(
        &mut self,
        key_space: String,
        deltas: Vec<(ByteView, UsageDelta)>,
        txn_id: Ulid,
    ) -> StorageEvent {
        if let Err(error) = self.store.resolve_keyspace(&key_space) {
            return StorageEvent::Error { error };
        }
        if !matches!(self.txns.get(&txn_id), Some(Txn::Write(_))) {
            return StorageEvent::Error {
                error: StorageError::TransactionNotFound,
            };
        }
        let pending = self.txn_usage.entry(txn_id).or_default();
        let mut entries = Vec::with_capacity(deltas.len());
        for (key, delta) in deltas {
            entries.push((key_space.clone(), key.clone()));
            match pending
                .iter_mut()
                .find(|(space, existing, _)| *space == key_space && *existing == key)
            {
                Some((_, _, existing)) => *existing = existing.merge(delta),
                None => pending.push((key_space.clone(), key, delta)),
            }
        }
        StorageEvent::BatchWriteResult { entries }
    }

    /// Writes the transaction's usage deltas on top of the latest committed counters. Only
    /// this worker commits, so no other commit can land between this read and the commit.
    fn stage_usage(
        &mut self,
        txn_id: Ulid,
        txn: &mut fjall::OptimisticWriteTx,
    ) -> Result<(), StorageError> {
        let Some(pending) = self.txn_usage.remove(&txn_id) else {
            return Ok(());
        };
        let snapshot = self.store.db.read_tx();
        for (key_space, key, delta) in pending {
            let keyspace = self.store.resolve_keyspace(&key_space)?;
            let stored = snapshot
                .get(&keyspace, &key)
                .map_err(|error| StorageError::ReadError(error.to_string()))?;
            let mut counters = match stored {
                Some(bytes) => UsageCounters::from_bytes(&bytes)
                    .map_err(|error| StorageError::WriteError(error.to_string()))?,
                None => UsageCounters::default(),
            };
            let shortfalls = counters
                .apply(&delta)
                .map_err(|error| StorageError::WriteError(error.to_string()))?;
            for shortfall in shortfalls {
                warn!(
                    event = "storage.usage.clamped",
                    field = shortfall.field,
                    key = hex::encode(&key),
                    shortfall = shortfall.missing,
                    "usage counter decrement clamped at zero"
                );
            }
            let value = counters
                .to_bytes()
                .map_err(|error| StorageError::WriteError(error.to_string()))?;
            txn.insert(keyspace, key, value);
        }
        Ok(())
    }

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
        self.txn_usage.remove(&txn_id);
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
            Some(Txn::Write(mut txn)) => {
                if let Err(error) = self.stage_usage(txn_id, &mut txn) {
                    self.take_txn_deletes(txn_id, false);
                    txn.rollback();
                    return StorageEvent::Error { error };
                }
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
