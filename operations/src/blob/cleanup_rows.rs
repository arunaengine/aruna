//! Queues blob cleanup rows that operations emit and schedules the cleanup drain.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::keyspaces::BLOB_CLEANUP_KEYSPACE;
use aruna_core::structs::storage::blob::BlobCleanupWork;
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::types::Key;
use tracing::{error, warn};
use ulid::Ulid;

pub(crate) const MAX_CLEANUP_RETRIES: u8 = 3;

/// A cleanup row an operation has emitted but storage has not yet accepted.
/// The row is the only durable record that the written object exists, so the
/// work is held until the write succeeds.
#[derive(Debug, Default, PartialEq)]
pub struct PendingCleanup {
    work: Option<BlobCleanupWork>,
    key: Option<Key>,
    channel_closed: bool,
    attempts: u8,
}

impl PendingCleanup {
    /// The write effect for one row. `None` means the row will never encode,
    /// leaving the caller with the failure it already carries.
    pub fn queue(&mut self, work: BlobCleanupWork) -> Option<Effect> {
        let key = cleanup_row_key(&work);
        let effect = cleanup_row_write(&work, &key)?;
        self.work = Some(work);
        self.key = Some(key);
        self.channel_closed = false;
        self.attempts = 0;
        Some(effect)
    }

    pub fn accepted(&mut self) {
        self.work = None;
        self.key = None;
    }

    /// Re-emit one write per temporary rejection; the caller remains queued.
    /// The retries are bounded: an endless loop would pin the blob reservation
    /// and never let the request finish.
    pub fn retry(&mut self, error: &StorageError) -> Option<Effect> {
        if self.channel_closed {
            return None;
        }
        if matches!(error, StorageError::ChannelClosed) {
            self.channel_closed = true;
            return None;
        }
        self.attempts = self.attempts.saturating_add(1);
        if self.attempts > MAX_CLEANUP_RETRIES {
            error!(error = %error, "Giving up on a rejected blob cleanup row");
            return None;
        }
        cleanup_row_write(self.work.as_ref()?, self.key.as_ref()?)
    }
}

fn cleanup_row_key(work: &BlobCleanupWork) -> Key {
    match work {
        BlobCleanupWork::ReconcileWrite { location, .. } => {
            location.ulid.to_bytes().to_vec().into()
        }
        BlobCleanupWork::DeleteBlob { .. }
        | BlobCleanupWork::RegisterDht { .. }
        | BlobCleanupWork::ReconcileReservation { .. }
        | BlobCleanupWork::ReleaseRenamed { .. }
        | BlobCleanupWork::DropTrail { .. }
        | BlobCleanupWork::DropClaims { .. }
        | BlobCleanupWork::ReleasePlanned { .. }
        | BlobCleanupWork::SettleUpload { .. }
        | BlobCleanupWork::SettleMove { .. }
        | BlobCleanupWork::DropRows { .. } => Ulid::generate().to_bytes().to_vec().into(),
    }
}

fn cleanup_row_write(work: &BlobCleanupWork, key: &Key) -> Option<Effect> {
    match work.to_bytes() {
        Ok(bytes) => Some(Effect::Storage(StorageEffect::Write {
            key_space: BLOB_CLEANUP_KEYSPACE.to_string(),
            key: key.clone(),
            value: bytes.into(),
            txn_id: None,
        })),
        Err(error) => {
            warn!(error = %error, "Blob cleanup row could not be encoded");
            None
        }
    }
}

pub fn schedule_cleanup_effect() -> Effect {
    Effect::Task(TaskEffect::ShortenTimer {
        key: TaskKey::DrainCleanupQueue,
        after: Duration::ZERO,
    })
}
