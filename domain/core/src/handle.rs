//! Declares the handle trait, the storage port the backends drive and the transaction owner.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use async_trait::async_trait;
use tracing::warn;
use ulid::Ulid;

use crate::effects::{Effect, StorageEffect};
use crate::errors::StorageError;
use crate::events::{Event, StorageEvent};

/// Effect executors return immediate `Event` result values to the operation
/// driver. Durable domain event records are owned by operations/outbox flows;
/// handles should not be treated as the origin of those records.
#[async_trait]
pub trait Handle: Clone + Send + Sync {
    async fn send_effect(&self, effect: Effect) -> Event;
}

/// The storage calls the blob and network backends make, without naming the storage adapter.
#[async_trait]
pub trait StoragePort: std::fmt::Debug + Send + Sync {
    async fn send_storage_effect(&self, effect: StorageEffect) -> Event;

    async fn start_transaction(&self, read: bool) -> Result<TransactionOwner, StorageError>;

    /// Count of commit attempts that may have changed a realm config row. A reader that sees a
    /// new count also sees the committed row.
    fn config_changes(&self) -> u64;
}

#[async_trait]
impl StoragePort for Arc<dyn StoragePort> {
    async fn send_storage_effect(&self, effect: StorageEffect) -> Event {
        self.as_ref().send_storage_effect(effect).await
    }

    async fn start_transaction(&self, read: bool) -> Result<TransactionOwner, StorageError> {
        self.as_ref().start_transaction(read).await
    }

    fn config_changes(&self) -> u64 {
        self.as_ref().config_changes()
    }
}

#[async_trait]
impl Handle for Arc<dyn StoragePort> {
    async fn send_effect(&self, effect: Effect) -> Event {
        match effect {
            Effect::Storage(effect) => self.send_storage_effect(effect).await,
            _ => Event::Storage(StorageEvent::Error {
                error: StorageError::InvalidEffect,
            }),
        }
    }
}

/// Takes over a transaction its owner gave up, so the storage node ends it once.
pub trait TransactionRetainer: Send + Sync {
    /// Retains `txn_id`; an unknown commit is kept for reconciliation, any other state is
    /// aborted. `false` means the handoff capacity is full.
    fn retain_transaction(&self, txn_id: Ulid, commit_unknown: bool) -> bool;
}

/// Cancellation-safe owner for a manually driven storage transaction.
pub struct TransactionOwner {
    handle: Arc<dyn TransactionRetainer>,
    txn_id: Option<Ulid>,
}

impl TransactionOwner {
    pub fn new(handle: Arc<dyn TransactionRetainer>, txn_id: Ulid) -> Self {
        Self {
            handle,
            txn_id: Some(txn_id),
        }
    }

    pub fn id(&self) -> Option<Ulid> {
        self.txn_id
    }

    /// Releases ownership after storage proved the transaction terminal.
    pub fn finish(&mut self) {
        self.txn_id = None;
    }

    /// Retains an ambiguous commit without issuing an abort.
    pub fn unknown(&mut self) {
        if let Some(txn_id) = self.txn_id {
            if self.handle.retain_transaction(txn_id, true) {
                self.txn_id = None;
            } else {
                warn!(%txn_id, "Transaction cleanup handoff capacity reached");
            }
        }
    }
}

impl Drop for TransactionOwner {
    fn drop(&mut self) {
        if let Some(txn_id) = self.txn_id {
            if self.handle.retain_transaction(txn_id, false) {
                self.txn_id = None;
            } else {
                warn!(%txn_id, "Transaction cleanup handoff capacity reached");
            }
        }
    }
}
