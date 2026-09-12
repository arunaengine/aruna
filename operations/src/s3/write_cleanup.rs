//! Reservation release, durable cleanup queue and pending error shared by S3 writes.

use crate::blob::cleanup::PendingCleanup;
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{S3_MULTIPART_UPLOAD_KEYSPACE, S3_MULTIPART_UPLOAD_PART_KEYSPACE};
use aruna_core::structs::{BlobCleanupWork, MultipartUploadPart, MultipartUploadPartKey};
use aruna_core::types::TxnId;
use ulid::Ulid;

/// Storage's answer for a queued cleanup row, reduced to the choice each
/// operation makes next.
#[derive(Debug, PartialEq)]
pub(crate) enum CleanupStep {
    /// Re-emit the retried row and stay queued.
    Retry(Effect),
    /// The pending row was accepted.
    Accepted,
    /// Retries were exhausted while the channel stayed open.
    Exhausted,
    /// The storage channel closed.
    Closed,
    /// The event does not belong to the queue state.
    Invalid,
}

/// Deferred reservation release, durable cleanup queue and pending error of one
/// write. Each operation maps a [`CleanupStep`] to its own continuation.
#[derive(Debug, PartialEq)]
pub(crate) struct WriteCleanup<E> {
    pending_cleanup: PendingCleanup,
    release_id: Option<Ulid>,
    pending_error: Option<E>,
}

impl<E> Default for WriteCleanup<E> {
    fn default() -> Self {
        Self {
            pending_cleanup: PendingCleanup::default(),
            release_id: None,
            pending_error: None,
        }
    }
}

impl<E> WriteCleanup<E> {
    pub(crate) fn set_error(&mut self, error: E) {
        self.pending_error = Some(error);
    }

    pub(crate) fn take_error(&mut self) -> Option<E> {
        self.pending_error.take()
    }

    pub(crate) fn error_pending(&self) -> bool {
        self.pending_error.is_some()
    }

    pub(crate) fn set_release(&mut self, id: Ulid) {
        self.release_id = Some(id);
    }

    pub(crate) fn release_id(&self) -> Option<Ulid> {
        self.release_id
    }

    pub(crate) fn take_release(&mut self) -> Option<Ulid> {
        self.release_id.take()
    }

    pub(crate) fn clear_release(&mut self) {
        self.release_id = None;
    }

    /// The release effect for a still-pending reservation, without clearing it.
    pub(crate) fn release_effect(&self) -> Option<Effect> {
        self.release_id
            .map(|id| Effect::Blob(BlobEffect::ReleaseReservation { id }))
    }

    pub(crate) fn queue(&mut self, work: BlobCleanupWork) -> Option<Effect> {
        self.pending_cleanup.queue(work)
    }

    pub(crate) fn retry(&mut self, error: &StorageError) -> Option<Effect> {
        self.pending_cleanup.retry(error)
    }

    pub(crate) fn handle_queued(&mut self, event: Event) -> CleanupStep {
        match event {
            Event::Storage(StorageEvent::WriteResult { .. }) => {
                self.pending_cleanup.accepted();
                CleanupStep::Accepted
            }
            Event::Storage(StorageEvent::Error { error }) => {
                let closed = matches!(error, StorageError::ChannelClosed);
                match self.pending_cleanup.retry(&error) {
                    Some(effect) => CleanupStep::Retry(effect),
                    None if closed => CleanupStep::Closed,
                    None => CleanupStep::Exhausted,
                }
            }
            _ => CleanupStep::Invalid,
        }
    }
}

/// The batch delete that removes every part row and the upload record itself.
pub(crate) fn delete_records_effect(
    upload_id: Ulid,
    parts: &[MultipartUploadPart],
    txn_id: Option<TxnId>,
) -> Result<Effect, ConversionError> {
    let mut deletes = Vec::with_capacity(parts.len() + 1);
    for part in parts {
        let key = MultipartUploadPartKey::new(upload_id, part.part_number).to_bytes()?;
        deletes.push((S3_MULTIPART_UPLOAD_PART_KEYSPACE.to_string(), key.into()));
    }
    deletes.push((
        S3_MULTIPART_UPLOAD_KEYSPACE.to_string(),
        upload_id.to_bytes().to_vec().into(),
    ));
    Ok(Effect::Storage(StorageEffect::BatchDelete {
        deletes,
        txn_id,
    }))
}
