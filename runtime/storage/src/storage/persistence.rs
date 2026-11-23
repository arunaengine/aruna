//! Picks the fjall persist mode and runs the journal and commit durability calls.
//! A failed commit is reported as a conflict or as a write error, never as success.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Instant;

use aruna_core::errors::StorageError;
use aruna_core::events::StorageEvent;
pub use aruna_core::persistence::FjallPersistPolicy;
use aruna_core::telemetry::duration_ms;
use fjall::PersistMode;
use tracing::Span;

use super::worker::FjallStorage;

/// Maps the persist policy to the fjall persist mode.
pub fn persist_mode(policy: FjallPersistPolicy) -> PersistMode {
    match policy {
        FjallPersistPolicy::Buffer => PersistMode::Buffer,
        FjallPersistPolicy::SyncAll => PersistMode::SyncAll,
    }
}

impl FjallStorage {
    pub(super) fn sync_all(&self) -> StorageEvent {
        match self.persist_with_mode(PersistMode::SyncAll) {
            Ok(()) => StorageEvent::SyncAllFinished,
            Err(error) => StorageEvent::Error { error },
        }
    }

    pub(super) fn persist_journal(&self) -> Result<(), StorageError> {
        self.persist_with_mode(persist_mode(self.persist_policy))
    }

    fn persist_with_mode(&self, mode: PersistMode) -> Result<(), StorageError> {
        let persist_started = Instant::now();
        self.store
            .db
            .persist(mode)
            .map_err(|error| StorageError::PersistError(error.to_string()))?;
        Span::current().record("persist_ms", duration_ms(persist_started.elapsed()));
        Ok(())
    }

    pub(super) fn buffered_write_tx(&self) -> Result<fjall::OptimisticWriteTx, StorageError> {
        self.store
            .db
            .write_tx()
            .map(|tx| tx.durability(Some(persist_mode(self.persist_policy))))
            .map_err(|error| StorageError::WriteError(error.to_string()))
    }

    pub(super) fn commit_buffered(&self, tx: fjall::OptimisticWriteTx) -> Result<(), StorageError> {
        let commit_started = Instant::now();
        match tx.commit() {
            Ok(Ok(())) => {
                Span::current().record("commit_ms", duration_ms(commit_started.elapsed()));
                Ok(())
            }
            Ok(Err(_)) => {
                Span::current().record("commit_ms", duration_ms(commit_started.elapsed()));
                Err(StorageError::TransactionConflict)
            }
            Err(error) => {
                Span::current().record("commit_ms", duration_ms(commit_started.elapsed()));
                Err(StorageError::WriteError(error.to_string()))
            }
        }
    }
}
