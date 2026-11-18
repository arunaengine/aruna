//! Tracks the reservation release, durable cleanup queue and pending error of an S3 write.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::blob::cleanup_rows::PendingCleanup;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{
    ABE_PENDING_KEYSPACE, BLOB_CLEANUP_KEYSPACE, UPLOAD_KEYSPACE, UPLOAD_PART_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::blob::{BackendLocation, BlobCleanupWork};
use aruna_core::structs::storage::multipart::{MultipartPart, MultipartPartKey};
use aruna_core::types::Key;
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

/// Every part row, the upload record and its pending envelope, for one delete call.
pub(crate) fn record_rows(
    upload_id: Ulid,
    parts: &[MultipartPart],
) -> Result<Vec<(String, Key)>, ConversionError> {
    let mut deletes = Vec::with_capacity(parts.len() + 2);
    for part in parts {
        let key = MultipartPartKey::new(upload_id, part.part_number).to_bytes()?;
        deletes.push((UPLOAD_PART_KEYSPACE.to_string(), key.into()));
    }
    deletes.push((
        UPLOAD_KEYSPACE.to_string(),
        upload_id.to_bytes().to_vec().into(),
    ));
    deletes.push((
        ABE_PENDING_KEYSPACE.to_string(),
        upload_id.to_bytes().to_vec().into(),
    ));
    Ok(deletes)
}

/// Deletes written bytes no version owns; a refused delete leaves them to the durable cleanup
/// queue, so they are never lost to both.
#[derive(Debug, PartialEq)]
pub(crate) struct RollbackWrite {
    location: BackendLocation,
    queued: bool,
}

impl RollbackWrite {
    pub(crate) fn new(location: BackendLocation) -> Self {
        Self {
            location,
            queued: false,
        }
    }
}

impl Operation for RollbackWrite {
    type Output = ();
    type Error = StorageError;

    fn first(&mut self) -> Result<Next<()>, StorageError> {
        let location = self.location.clone();
        Ok(Next::Call(
            Effect::Blob(BlobEffect::Delete { location }).into(),
        ))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, StorageError> {
        if self.queued {
            result.done()?;
            return Ok(Next::Done(()));
        }
        let Event::Blob(BlobEvent::Error(_)) = result.event()? else {
            return Ok(Next::Done(()));
        };
        let location = self.location.clone();
        let work = BlobCleanupWork::DeleteBlob { location };
        let key = Ulid::generate().to_bytes().to_vec().into();
        let value = work.to_bytes();
        let value = value.map_err(|error| IoError::Codec(error.to_string()))?;
        let row = (BLOB_CLEANUP_KEYSPACE.to_string(), key, value.into());
        self.queued = true;
        Ok(Next::Call(IoCall::Write(vec![row])))
    }
}

/// Releases the reservation of committed bytes; a refused release writes `reconcile` itself, so
/// the cleanup worker settles the reservation later and nothing it frees is leaked.
#[derive(Debug, PartialEq)]
pub(crate) struct ReleaseWrite {
    id: Ulid,
    reconcile: Option<BlobCleanupWork>,
    queued: bool,
}

impl ReleaseWrite {
    pub(crate) fn new(id: Ulid, reconcile: Option<BlobCleanupWork>) -> Self {
        Self {
            id,
            reconcile,
            queued: false,
        }
    }
}

impl Operation for ReleaseWrite {
    type Output = ();
    type Error = StorageError;

    fn first(&mut self) -> Result<Next<()>, StorageError> {
        let release = BlobEffect::ReleaseReservation { id: self.id };
        Ok(Next::Call(Effect::Blob(release).into()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, StorageError> {
        if self.queued {
            result.done()?;
            return Ok(Next::Done(()));
        }
        if let Event::Blob(BlobEvent::ReservationReleased { id }) = result.event()?
            && id == self.id
        {
            return Ok(Next::Done(()));
        }
        let Some(work) = self.reconcile.take() else {
            return Ok(Next::Done(()));
        };
        tracing::warn!(
            event = "put_object.release_deferred",
            release_id = %self.id,
            "Queuing the blob reservation for reconciliation"
        );
        let key = match &work {
            BlobCleanupWork::ReconcileWrite { location, .. } => location.ulid,
            _ => Ulid::generate(),
        };
        let value = work.to_bytes();
        let value = value.map_err(|error| IoError::Codec(error.to_string()))?;
        let key = key.to_bytes().to_vec().into();
        let row = (BLOB_CLEANUP_KEYSPACE.to_string(), key, value.into());
        self.queued = true;
        Ok(Next::Call(IoCall::Write(vec![row])))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::storage::blob::BackendRef;
    use aruna_core::structs::storage::format::StoredFormat;

    fn location() -> BackendLocation {
        BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/tmp".to_string(),
            storage_bucket: "bucket".to_string(),
            backend_path: "path".to_string(),
            ulid: Ulid::from_parts(1, 1),
            format: StoredFormat::default(),
            created_by: Default::default(),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 1,
            hashes: Default::default(),
        }
    }

    fn answer(event: BlobEvent) -> IoResult {
        IoResult::Event(Box::new(Event::Blob(event)))
    }

    #[test]
    fn rollback_queues_refused() {
        // A deleted blob needs nothing more.
        let mut rollback = RollbackWrite::new(location());
        let delete = Effect::Blob(BlobEffect::Delete {
            location: location(),
        });
        assert_eq!(rollback.first(), Ok(Next::Call(delete.into())));
        let turn = rollback.next(answer(BlobEvent::DeleteFinished));
        assert_eq!(turn, Ok(Next::Done(())));

        // A refused delete leaves the blob to the cleanup queue.
        let mut rollback = RollbackWrite::new(location());
        rollback.first().unwrap();
        let refused = BlobEvent::Error(aruna_core::errors::BlobError::DeleteError(
            "unreachable".to_string(),
        ));
        let Ok(Next::Call(IoCall::Write(rows))) = rollback.next(answer(refused)) else {
            panic!("expected the cleanup row")
        };
        assert_eq!(rows[0].0, BLOB_CLEANUP_KEYSPACE);
        assert_eq!(
            BlobCleanupWork::from_bytes(rows[0].2.as_ref()).unwrap(),
            BlobCleanupWork::DeleteBlob {
                location: location()
            }
        );
        assert_eq!(rollback.next(IoResult::Done), Ok(Next::Done(())));
    }

    #[test]
    fn release_queues_reconcile() {
        let work = BlobCleanupWork::ReconcileWrite {
            location: location(),
            owner: aruna_core::structs::storage::blob::WriteOwner::Pending,
        };
        let id = location().ulid;
        // A released reservation needs nothing more.
        let mut release = ReleaseWrite::new(id, Some(work.clone()));
        let effect = Effect::Blob(BlobEffect::ReleaseReservation { id });
        assert_eq!(release.first(), Ok(Next::Call(effect.into())));
        let released = BlobEvent::ReservationReleased { id };
        assert_eq!(release.next(answer(released)), Ok(Next::Done(())));

        // A refused release writes the reconcile row under the location's id.
        let mut release = ReleaseWrite::new(id, Some(work.clone()));
        release.first().unwrap();
        let refused = BlobEvent::Error(aruna_core::errors::BlobError::DeleteError(
            "unreachable".to_string(),
        ));
        let Ok(Next::Call(IoCall::Write(rows))) = release.next(answer(refused)) else {
            panic!("expected the reconcile row")
        };
        let [(space, key, value)] = rows.as_slice() else {
            panic!("expected one row, got {rows:?}")
        };
        assert_eq!(space, BLOB_CLEANUP_KEYSPACE);
        assert_eq!(key.as_ref(), id.to_bytes());
        assert_eq!(BlobCleanupWork::from_bytes(value.as_ref()).unwrap(), work);
        assert_eq!(release.next(IoResult::Done), Ok(Next::Done(())));

        // Without a reconcile row to keep, a refused release ends the child.
        let mut release = ReleaseWrite::new(id, None);
        release.first().unwrap();
        let other = BlobEvent::ReservationReleased {
            id: Ulid::from_parts(9, 9),
        };
        assert_eq!(release.next(answer(other)), Ok(Next::Done(())));
    }
}
