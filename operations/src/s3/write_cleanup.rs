//! Reservation release, durable cleanup queue and pending error shared by the
//! S3 write operations, plus the multipart upload target check they all use.

use crate::blob::cleanup::PendingCleanup;
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{S3_MULTIPART_UPLOAD_KEYSPACE, S3_MULTIPART_UPLOAD_PART_KEYSPACE};
use aruna_core::structs::{
    BlobCleanupWork, MultipartUpload, MultipartUploadPart, MultipartUploadPartKey,
    MultipartUploadStatus,
};
use aruna_core::types::TxnId;
use thiserror::Error;
use ulid::Ulid;

/// Storage's answer for a queued cleanup row, reduced to the choice each
/// operation makes next.
#[derive(Debug, PartialEq)]
pub(crate) enum CleanupEvent {
    /// Re-emit the retried row and stay queued.
    Effect(Effect),
    /// The row was accepted, so the queue is empty.
    Accepted,
    /// Retries were exhausted while the channel stayed open.
    Exhausted,
    /// The storage channel closed.
    Closed,
    /// The event does not belong to the queue state.
    Invalid,
}

/// Deferred reservation release, durable cleanup queue and pending error of one
/// write. Each operation maps a [`CleanupEvent`] to its own continuation.
#[derive(Debug, PartialEq)]
pub(crate) struct WriteCleanup<E> {
    pub(crate) pending_cleanup: PendingCleanup,
    pub(crate) release_id: Option<Ulid>,
    pub(crate) pending_error: Option<E>,
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

    pub(crate) fn handle_queued(&mut self, event: Event) -> CleanupEvent {
        match event {
            Event::Storage(StorageEvent::WriteResult { .. }) => {
                self.pending_cleanup.accepted();
                CleanupEvent::Accepted
            }
            Event::Storage(StorageEvent::Error { error }) => {
                let closed = matches!(error, StorageError::ChannelClosed);
                match self.pending_cleanup.retry(&error) {
                    Some(effect) => CleanupEvent::Effect(effect),
                    None if closed => CleanupEvent::Closed,
                    None => CleanupEvent::Exhausted,
                }
            }
            _ => CleanupEvent::Invalid,
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

#[derive(Debug, Error, PartialEq)]
pub(crate) enum UploadTargetError {
    #[error("The specified multipart upload does not match the target object.")]
    TargetMismatch,
    #[error("The multipart upload is no longer open.")]
    NotOpen,
    #[error("The upload is being completed, retry shortly.")]
    CompletionInProgress,
}

/// Rejects a record whose bucket, key or status does not admit the caller.
/// `stale_ms` marks the lapsed completion leases a caller may take over; `None`
/// keeps the check strict for callers that only accept `Open` uploads.
pub(crate) fn validate_upload_target(
    record: &MultipartUpload,
    bucket: &str,
    key: &str,
    allow_in_progress: bool,
    stale_ms: Option<u64>,
) -> Result<(), UploadTargetError> {
    if record.bucket != bucket || record.key != key {
        return Err(UploadTargetError::TargetMismatch);
    }
    if allow_in_progress {
        return Ok(());
    }
    match record.status {
        MultipartUploadStatus::Open => Ok(()),
        MultipartUploadStatus::Completing => match stale_ms {
            Some(now_ms) if record.completion_stale(now_ms) => Ok(()),
            Some(_) => Err(UploadTargetError::CompletionInProgress),
            None => Err(UploadTargetError::NotOpen),
        },
        MultipartUploadStatus::Aborting => Err(UploadTargetError::NotOpen),
    }
}
