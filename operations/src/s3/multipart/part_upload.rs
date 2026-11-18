//! Stores one uploaded part: writes the blob, records the part and retries write conflicts.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::groups::backends::{BackendFenceError, RecordReadError, backend_key};
use crate::placement::policy::PolicyGateError;
use crate::s3::multipart::target::{StatusCheck, UploadTargetError, validate_upload};
use crate::s3::purge_fence::{PurgeFenceError, check_fence_row, fence_key};
use crate::s3::write_cleanup::{ReleaseWrite, RollbackWrite};
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event};
use aruna_core::keyspaces::{
    ABE_PENDING_KEYSPACE, BLOB_CLEANUP_KEYSPACE, NODE_SUBJECT_KEYSPACE, PURGE_FENCE_KEYSPACE,
    STORAGE_BACKEND_KEYSPACE, UPLOAD_KEYSPACE, UPLOAD_PART_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{read_row, values};
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::checksum::ExpectedChecksum;
use aruna_core::structs::placement::node_subject::{NODE_SUBJECT_KEY, NodeSubjectRecord};
use aruna_core::structs::storage::abe::ObjectEnvelope;
use aruna_core::structs::storage::blob::{
    BackendLocation, BackendRef, BlobCleanupWork, ResolvedBackend, WriteOwner,
};
use aruna_core::structs::storage::group_backend::GroupStorage;
use aruna_core::structs::storage::multipart::{
    MultipartPart, MultipartPartKey, MultipartUpload, PartPiece, content_offset,
};
use aruna_core::types::{Key, Value};
use bytes::Bytes;
use std::time::SystemTime;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

/// Bounded retries for an SSI conflict on the part commit. A refused
/// transaction is contention, not a failed request.
const CONFLICT_RETRIES: u8 = 4;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PartStep {
    CheckFence,
    ReadUpload,
    ReadSaved,
    WritePart,
    Publish,
    Release,
}

#[derive(Debug, Error, PartialEq)]
pub enum UploadPartError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    BackendFenceError(#[from] BackendFenceError),
    #[error("Invalid operation state")]
    InvalidOperationState,
    #[error("The specified upload does not exist.")]
    NoSuchUpload,
    #[error("The specified multipart upload does not match the target object.")]
    UploadTargetMismatch,
    #[error("The multipart upload is no longer open.")]
    UploadNotOpen,
    #[error("request body missing")]
    MissingBody,
    #[error("body size did not match Content-Length header")]
    IncompleteBody,
    #[error("a part of this upload needs its Content-Length")]
    MissingContentLength,
    #[error("missing stored checksum for {0}")]
    MissingExpectedChecksum(&'static str),
    #[error("checksum mismatch for {0}")]
    ChecksumMismatch(&'static str),
    #[error("blob write failed: {0}")]
    WriteFailed(String),
    #[error("blob backend write failed: {0}")]
    BlobWriteFailed(String),
    #[error(transparent)]
    PolicyGateError(#[from] PolicyGateError),
    #[error(transparent)]
    PurgeFence(#[from] PurgeFenceError),
}

impl From<IoError> for UploadPartError {
    fn from(error: IoError) -> Self {
        Self::StorageError(error.into())
    }
}

impl From<UploadTargetError> for UploadPartError {
    fn from(error: UploadTargetError) -> Self {
        match error {
            UploadTargetError::TargetMismatch => Self::UploadTargetMismatch,
            UploadTargetError::NotOpen | UploadTargetError::CompletionInProgress => {
                Self::UploadNotOpen
            }
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct UploadPartInput {
    pub bucket: String,
    pub key: String,
    pub upload_id: Ulid,
    pub part_number: u16,
    pub content_length: Option<u64>,
    pub body: Option<BackendStream<Result<Bytes, StreamError>>>,
    pub created_by: UserId,
    pub expected_checksums: Vec<ExpectedChecksum>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UploadPartResult {
    pub location: BackendLocation,
}

type PartTurn = Result<Next<UploadPartResult>, UploadPartError>;

fn part_key(upload_id: Ulid, part_number: u16) -> Result<Key, ConversionError> {
    Ok(MultipartPartKey::new(upload_id, part_number)
        .to_bytes()?
        .into())
}

/// Writes the part's bytes, then records them in a transaction of their own. Bytes no record
/// owns are rolled back by `abort`; bytes a commit may own go to reconciliation.
#[derive(Debug, PartialEq)]
pub struct UploadPartOperation {
    step: PartStep,
    input: UploadPartInput,
    conflicts: u8,
    written_location: Option<BackendLocation>,
    /// The provider's ETag of a part written into its multipart upload.
    backend_etag: Option<String>,
    /// An encrypted upload's part waits here while the saved parts give its content offset.
    sealing: Option<(ResolvedBackend, BackendStream<Result<Bytes, StreamError>>)>,
    /// The piece record of a part of an encrypted upload.
    piece: Option<PartPiece>,
    /// The object public key of the upload's pending envelope, granted every piece.
    object: Option<[u8; 32]>,
    /// Bytes a commit of unknown outcome may own, and their reservation.
    reconcile: Option<BackendLocation>,
    release: Option<Ulid>,
    finished: Option<UploadPartResult>,
}

impl UploadPartOperation {
    pub fn new(input: UploadPartInput) -> Self {
        Self {
            step: PartStep::CheckFence,
            input,
            conflicts: 0,
            written_location: None,
            backend_etag: None,
            sealing: None,
            piece: None,
            object: None,
            reconcile: None,
            release: None,
            finished: None,
        }
    }

    fn call(&mut self, step: PartStep, call: IoCall) -> PartTurn {
        self.step = step;
        Ok(Next::Call(call))
    }

    /// Rejects an already-fenced destination before accepting the part body. The commit
    /// transaction rechecks the same row after the bytes move. One round trip then answers
    /// "does this upload exist", "does the subject that admitted it still hold" and "is this
    /// part number already acknowledged".
    fn fence_checked(&mut self, value: Option<Value>) -> PartTurn {
        let input = &self.input;
        check_fence_row(value.as_deref(), &input.bucket, &input.key)?;
        let id: Key = input.upload_id.to_bytes().to_vec().into();
        let reads = vec![
            (UPLOAD_KEYSPACE.to_string(), id.clone()),
            (
                NODE_SUBJECT_KEYSPACE.to_string(),
                Key::from(NODE_SUBJECT_KEY.to_vec()),
            ),
            (
                UPLOAD_PART_KEYSPACE.to_string(),
                part_key(input.upload_id, input.part_number)?,
            ),
            (ABE_PENDING_KEYSPACE.to_string(), id),
        ];
        self.call(PartStep::ReadUpload, IoCall::Read(reads))
    }

    fn upload_read(
        &mut self,
        [value, subject, acknowledged, pending]: [Option<Value>; 4],
    ) -> PartTurn {
        let subject = (subject.as_deref())
            .map(NodeSubjectRecord::from_bytes)
            .transpose()?;
        let value = value.ok_or(UploadPartError::NoSuchUpload)?;
        let record = MultipartUpload::from_bytes(value.as_ref())?;
        let input = &self.input;
        validate_upload(&record, &input.bucket, &input.key, StatusCheck::Open)?;
        // Cheap re-check of the create-time refs: no ref is resolved again, but
        // a subject that moved since then stops the part before any byte moves.
        if !record.admits_part(subject.as_ref()) {
            return Err(PolicyGateError::Drift.into());
        }
        // A provider upload takes the part size before the first byte.
        if record.backend_upload.is_some() && input.content_length.is_none() {
            return Err(UploadPartError::MissingContentLength);
        }
        let blob = self.input.body.take().ok_or(UploadPartError::MissingBody)?;
        if let Some(encryption) = record.encryption {
            // Without a pending envelope the piece goes to the bucket key only.
            if let Some(pending) = pending {
                let envelope = ObjectEnvelope::from_bytes(pending.as_ref())
                    .map_err(|error| UploadPartError::BlobWriteFailed(error.to_string()))?;
                self.object = Some(envelope.context.public_key);
            }
            let resolved = ResolvedBackend::new(record.backend, record.storage_class)
                .with_compression(encryption.compression)
                .with_encryption(Some(encryption.plan));
            return self.read_saved_parts(resolved, blob);
        }
        let write = Effect::Blob(BlobEffect::WritePart {
            upload_id: self.input.upload_id,
            part_number: self.input.part_number,
            resolved: ResolvedBackend::new(record.backend, record.storage_class),
            created_by: self.input.created_by,
            // A replacement waits in a blob of its own: a rejected one must leave the
            // acknowledged provider part intact, and completion copies it in.
            backend_upload: record
                .backend_upload
                .filter(|_| acknowledged.is_none())
                .map(Box::new),
            size: self.input.content_length,
            blob,
        });
        self.call(PartStep::WritePart, write.into())
    }

    /// The content offset of a sealed part comes from the parts saved before its bytes arrive.
    fn read_saved_parts(
        &mut self,
        resolved: ResolvedBackend,
        blob: BackendStream<Result<Bytes, StreamError>>,
    ) -> PartTurn {
        let prefix = MultipartPartKey::prefix(self.input.upload_id)?;
        self.sealing = Some((resolved, blob));
        let page = IoCall::Page {
            space: UPLOAD_PART_KEYSPACE.to_string(),
            prefix: prefix.into(),
            after: None,
            limit: 10_000,
        };
        self.call(PartStep::ReadSaved, page)
    }

    fn write_piece(&mut self, rows: Vec<(Key, Value)>) -> PartTurn {
        let saved = rows
            .iter()
            .map(|(_, value)| MultipartPart::from_bytes(value.as_ref()))
            .map(|part| part.map(|part| (part.part_number, part.location.blob_size)))
            .collect::<Result<Vec<_>, _>>()?;
        let (resolved, blob) = self.sealing.take().ok_or(IoError::Unexpected {
            expected: "a sealed part body",
            got: "none",
        })?;
        let number = self.input.part_number;
        let write = Effect::Blob(BlobEffect::WritePiece {
            upload_id: self.input.upload_id,
            part_number: number,
            resolved,
            created_by: self.input.created_by,
            content_offset: content_offset(number, &saved, self.input.content_length),
            object: self.object,
            blob,
        });
        self.call(PartStep::WritePart, write.into())
    }

    fn write_finished(&mut self, event: Event) -> PartTurn {
        let location = match event {
            Event::Blob(BlobEvent::WriteFinished { location }) => location,
            Event::Blob(BlobEvent::PieceWritten { location, piece }) => {
                self.piece = Some(piece);
                location
            }
            Event::Blob(BlobEvent::PartWritten {
                location,
                backend_etag,
            }) => {
                self.backend_etag = Some(backend_etag);
                location
            }
            // Only a client-sourced stream fault may become a client error; a
            // server-side write fault must stay retryable, never a bad digest.
            Event::Blob(BlobEvent::Error(BlobError::StreamFailed(message))) => {
                return Err(UploadPartError::WriteFailed(message));
            }
            Event::Blob(BlobEvent::Error(BlobError::WriteCleanup { location, message })) => {
                self.written_location = Some(location);
                return Err(UploadPartError::BlobWriteFailed(message));
            }
            Event::Blob(BlobEvent::Error(error)) => {
                return Err(UploadPartError::BlobWriteFailed(error.to_string()));
            }
            other => {
                return Err(IoError::Unexpected {
                    expected: "the written part",
                    got: other.label(),
                }
                .into());
            }
        };
        self.written_location = Some(location.clone());
        let expected = self.input.content_length;
        if expected.is_some_and(|expected| location.blob_size != expected) {
            return Err(UploadPartError::IncompleteBody);
        }
        for expected in &self.input.expected_checksums {
            let name = expected.algorithm.s3_name();
            let actual = location.hashes.get(expected.algorithm.hash_key());
            let actual = actual.ok_or(UploadPartError::MissingExpectedChecksum(name))?;
            if actual != &expected.digest {
                return Err(UploadPartError::ChecksumMismatch(name));
            }
        }
        self.publish()
    }

    /// A refused commit discarded its writes, so only the record transaction runs again: the
    /// part bytes are already on the backend and are not streamed twice.
    fn publish(&mut self) -> PartTurn {
        let location = self.written_location.clone().ok_or(IoError::Unexpected {
            expected: "the written part",
            got: "none",
        })?;
        let input = &self.input;
        let record = PartRecordWrite {
            bucket: input.bucket.clone(),
            key: input.key.clone(),
            upload_id: input.upload_id,
            part_number: input.part_number,
            location,
            backend_etag: self.backend_etag.clone(),
            piece: self.piece.clone(),
            step: RecordStep::Fence,
        };
        self.call(PartStep::Publish, IoCall::child(record))
    }

    fn published(&mut self, published: Result<(), UploadPartError>) -> PartTurn {
        let error = match published {
            Ok(()) => return self.committed(),
            Err(error) => error,
        };
        let UploadPartError::StorageError(storage) = &error else {
            return Err(error);
        };
        if *storage == StorageError::TransactionConflict && self.conflicts < CONFLICT_RETRIES {
            self.conflicts += 1;
            return self.publish();
        }
        // Only a proven refusal rolls the part back; any other failure may already own it.
        if !storage.proves_no_commit() {
            self.keep_written(storage);
        }
        Err(error)
    }

    /// The bytes go to the reconciliation queue: the committed part record decides their fate.
    fn keep_written(&mut self, error: &StorageError) {
        let Some(location) = self.written_location.take() else {
            return;
        };
        warn!(
            event = "upload_part.commit_outcome_unknown",
            backend = %location.backend,
            blob_size = location.blob_size,
            error = %error,
            "Queuing the written part for reconciliation"
        );
        self.release = Some(location.ulid);
        self.reconcile = Some(location);
    }

    /// The committed record owns the blob now. A refused release does not fail the request:
    /// the reconciliation row committed with the record clears the reservation later.
    fn committed(&mut self) -> PartTurn {
        let location = self.written_location.take().ok_or(IoError::Unexpected {
            expected: "the written part",
            got: "none",
        })?;
        let release = ReleaseWrite::new(location.ulid, None);
        self.finished = Some(UploadPartResult { location });
        self.call(PartStep::Release, IoCall::child(release))
    }

    fn released(&mut self, released: Result<(), StorageError>) -> PartTurn {
        if let Err(error) = released {
            warn!(
                event = "upload_part.release_deferred",
                %error,
                "Deferring the part reservation to reconciliation"
            );
        }
        let finished = self.finished.take().ok_or(IoError::Unexpected {
            expected: "the committed part",
            got: "none",
        })?;
        Ok(Next::Done(finished))
    }

    fn reconcile_row(&self, location: BackendLocation) -> Option<IoCall> {
        let key = location.ulid.to_bytes().to_vec().into();
        let work = reconcile_work(location, self.input.upload_id, self.input.part_number);
        match work.to_bytes() {
            Ok(value) => {
                let row = (BLOB_CLEANUP_KEYSPACE.to_string(), key, value.into());
                Some(IoCall::Write(vec![row]))
            }
            Err(error) => {
                warn!(%error, "A part reconciliation row could not be encoded");
                None
            }
        }
    }
}

fn reconcile_work(location: BackendLocation, upload_id: Ulid, part_number: u16) -> BlobCleanupWork {
    BlobCleanupWork::ReconcileWrite {
        location,
        owner: WriteOwner::UploadPart {
            upload_id,
            part_number,
        },
    }
}

impl Operation for UploadPartOperation {
    type Output = UploadPartResult;
    type Error = UploadPartError;

    fn first(&mut self) -> PartTurn {
        let fence = read_row(PURGE_FENCE_KEYSPACE, fence_key(&self.input.bucket));
        self.call(PartStep::CheckFence, fence)
    }

    fn next(&mut self, result: IoResult) -> PartTurn {
        match self.step {
            PartStep::CheckFence => self.fence_checked(result.value()?),
            PartStep::ReadUpload => self.upload_read(values::<4>(result)?),
            PartStep::ReadSaved => self.write_piece(result.page()?.0),
            PartStep::WritePart => self.write_finished(result.event()?),
            PartStep::Publish => self.published(result.child::<(), UploadPartError>()?),
            PartStep::Release => self.released(result.child::<(), StorageError>()?),
        }
    }

    /// Rolls written bytes back, or leaves them to reconciliation when a commit may own them,
    /// and releases their reservation, before the run ends.
    fn abort(&mut self) -> Vec<IoCall> {
        // The record transaction was cut off: its commit may have applied.
        if self.step == PartStep::Publish
            && let Some(location) = self.written_location.take()
        {
            self.release = Some(location.ulid);
            self.reconcile = Some(location);
        }
        let mut calls = Vec::new();
        if let Some(location) = self.reconcile.take() {
            calls.extend(self.reconcile_row(location));
        }
        if let Some(location) = self.written_location.take() {
            calls.push(IoCall::child(RollbackWrite::new(location)));
        }
        if let Some(id) = self.release.take() {
            calls.push(Effect::Blob(BlobEffect::ReleaseReservation { id }).into());
        }
        calls
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RecordStep {
    Fence,
    Backend,
    Upload,
    Existing,
    Write,
}

type RecordTurn = Result<Next<()>, UploadPartError>;

/// Records a written part in one transaction, with the row that reconciles its bytes and the
/// cleanup of the part it replaces.
#[derive(Debug, PartialEq)]
struct PartRecordWrite {
    bucket: String,
    key: String,
    upload_id: Ulid,
    part_number: u16,
    location: BackendLocation,
    backend_etag: Option<String>,
    piece: Option<PartPiece>,
    step: RecordStep,
}

impl PartRecordWrite {
    fn call(&mut self, step: RecordStep, call: IoCall) -> RecordTurn {
        self.step = step;
        Ok(Next::Call(call))
    }

    /// The pin was taken at creation, so the part write must still prove the tenant backend
    /// is enabled before its record joins the transaction.
    fn fence_checked(&mut self, value: Option<Value>) -> RecordTurn {
        check_fence_row(value.as_deref(), &self.bucket, &self.key)?;
        match self.location.backend {
            BackendRef::Group(backend_id) => {
                let read = read_row(STORAGE_BACKEND_KEYSPACE, backend_key(backend_id));
                self.call(RecordStep::Backend, read)
            }
            _ => self.read_upload(),
        }
    }

    fn backend_read(&mut self, value: Option<Value>) -> RecordTurn {
        let record = (value.as_deref())
            .map(GroupStorage::from_bytes)
            .transpose()
            .map_err(|error| BackendFenceError::Read(RecordReadError::Conversion(error)))?;
        match record {
            Some(record) if !record.disabled => self.read_upload(),
            _ => Err(BackendFenceError::Unavailable.into()),
        }
    }

    fn read_upload(&mut self) -> RecordTurn {
        let read = read_row(UPLOAD_KEYSPACE, self.upload_id.to_bytes().to_vec());
        self.call(RecordStep::Upload, read)
    }

    fn upload_read(&mut self, value: Option<Value>) -> RecordTurn {
        let value = value.ok_or(UploadPartError::NoSuchUpload)?;
        let record = MultipartUpload::from_bytes(value.as_ref())?;
        validate_upload(&record, &self.bucket, &self.key, StatusCheck::Open)?;
        let key = part_key(self.upload_id, self.part_number)?;
        self.call(RecordStep::Existing, read_row(UPLOAD_PART_KEYSPACE, key))
    }

    /// The reconciliation row commits with the record, so the commit can never be ambiguous
    /// about who settles the reservation; a replaced part's bytes go through the cleanup queue.
    fn existing_read(&mut self, value: Option<Value>) -> RecordTurn {
        let replaced = (value.as_deref())
            .map(MultipartPart::from_bytes)
            .transpose()?
            .map(|existing| existing.location);
        let record = MultipartPart {
            part_number: self.part_number,
            location: self.location.clone(),
            created_at: SystemTime::now(),
            backend_etag: self.backend_etag.clone(),
            piece: self.piece.clone(),
        };
        let reconcile = reconcile_work(self.location.clone(), self.upload_id, self.part_number);
        let mut writes = vec![
            (
                UPLOAD_PART_KEYSPACE.to_string(),
                part_key(self.upload_id, self.part_number)?,
                record.to_bytes()?.into(),
            ),
            (
                BLOB_CLEANUP_KEYSPACE.to_string(),
                self.location.ulid.to_bytes().to_vec().into(),
                reconcile.to_bytes()?.into(),
            ),
        ];
        if let Some(location) = replaced {
            let work = BlobCleanupWork::DeleteBlob { location };
            writes.push((
                BLOB_CLEANUP_KEYSPACE.to_string(),
                Ulid::generate().to_bytes().to_vec().into(),
                work.to_bytes()?.into(),
            ));
        }
        self.call(RecordStep::Write, IoCall::Write(writes))
    }
}

impl Operation for PartRecordWrite {
    type Output = ();
    type Error = UploadPartError;

    fn first(&mut self) -> RecordTurn {
        let fence = read_row(PURGE_FENCE_KEYSPACE, fence_key(&self.bucket));
        self.call(RecordStep::Fence, fence)
    }

    fn next(&mut self, result: IoResult) -> RecordTurn {
        match self.step {
            RecordStep::Fence => self.fence_checked(result.value()?),
            RecordStep::Backend => self.backend_read(result.value()?),
            RecordStep::Upload => self.upload_read(result.value()?),
            RecordStep::Existing => self.existing_read(result.value()?),
            RecordStep::Write => {
                result.done()?;
                Ok(Next::Done(()))
            }
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::storage::format::StoredFormat;
    use aruna_core::structs::storage::multipart::{BackendUpload, MultipartUploadStatus};
    use aruna_storage::storage;
    use tempfile::tempdir;

    fn test_user_id() -> UserId {
        UserId::local(Ulid::generate(), RealmId::from_bytes([1u8; 32]))
    }

    fn body() -> Option<BackendStream<Result<Bytes, StreamError>>> {
        Some(BackendStream::new(tokio_util::io::ReaderStream::new(
            &b"part"[..],
        )))
    }

    fn blob(event: BlobEvent) -> IoResult {
        IoResult::Event(Box::new(Event::Blob(event)))
    }

    fn child<T, E>(result: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(ChildOutcome::new(result))
    }

    fn effect_of(turn: &PartTurn) -> &Effect {
        match turn {
            Ok(Next::Call(IoCall::Effect(effect))) => effect,
            other => panic!("expected an effect call, got {other:?}"),
        }
    }

    /// An operation about to read the record of an in-place upload, with that record.
    fn in_place_op(content_length: Option<u64>) -> (UploadPartOperation, MultipartUpload) {
        let mut op = upload_part_op(Ulid::from_bytes([5u8; 16]));
        let upload = BackendUpload {
            location: op.written_location.take().unwrap(),
            upload_id: "provider-upload".to_string(),
            record_id: Ulid::from_bytes([9u8; 16]),
        };
        op.input.content_length = content_length;
        op.input.body = Some(BackendStream::new(tokio_util::io::ReaderStream::new(
            &b"part"[..],
        )));
        op.state = UploadPartState::ReadUpload;
        let record = MultipartUpload {
            upload_id: op.input.upload_id,
            backend: upload.location.backend.clone(),
            storage_class: None,
            bucket: "mybucket".to_string(),
            key: "object.txt".to_string(),
            group_id: Ulid::generate(),
            created_by: test_user_id(),
            created_at: SystemTime::UNIX_EPOCH,
            status: MultipartUploadStatus::Open,
            checksum_hint: None,
            metadata: std::collections::HashMap::new(),
            placement_policies: Vec::new(),
            subject_generation: 0,
            completing_since_ms: None,
            backend_upload: Some(upload),
            encryption: None,
        };
        (op, record)
    }

    /// Answers the first read; `acknowledged` is the part record already stored, if any.
    fn read_record(record: &MultipartUpload, acknowledged: Option<Vec<u8>>) -> Event {
        Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (
                    record.upload_id.to_bytes().to_vec().into(),
                    Some(record.to_bytes().unwrap().into()),
                ),
                (NODE_SUBJECT_KEY.to_vec().into(), None),
                (b"part".to_vec().into(), acknowledged.map(Into::into)),
                (record.upload_id.to_bytes().to_vec().into(), None),
            ],
        })
    }

    #[test]
    fn replacement_waits_staged() {
        // A replaced part must not touch the acknowledged provider part before completion.
        let (mut op, record) = in_place_op(Some(4));

        let effects = op.step(read_record(&record, Some(b"acknowledged".to_vec())));

        let [Effect::Blob(BlobEffect::WritePart { backend_upload, .. })] = effects.as_slice()
        else {
            panic!("expected one part write, got {effects:?}")
        };
        assert!(backend_upload.is_none());
    }

    #[test]
    fn provider_needs_length() {
        let (mut op, record) = in_place_op(None);

        let effects = op.step(read_record(&record, None));

        assert!(
            !effects
                .iter()
                .any(|effect| matches!(effect, Effect::Blob(_)))
        );
        assert_eq!(
            op.finalize().unwrap_err(),
            UploadPartError::MissingContentLength
        );
    }

    #[test]
    fn keeps_provider_etag() {
        // A part streamed into the provider upload records the ETag completion must name.
        let (mut op, record) = in_place_op(Some(4));
        let upload = record.backend_upload.clone().unwrap();

        let effects = op.step(read_record(&record, None));
        let [
            Effect::Blob(BlobEffect::WritePart {
                backend_upload,
                size,
                ..
            }),
        ] = effects.as_slice()
        else {
            panic!("expected one part write, got {effects:?}")
        };
        assert_eq!(backend_upload.as_deref(), Some(&upload));
        assert_eq!(*size, Some(4));

        let mut written = upload.location.clone();
        written.partial = true;
        let effects = op.step(Event::Blob(BlobEvent::PartWritten {
            location: written,
            backend_etag: "\"etag\"".to_string(),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::StartTransaction { .. })]
        ));

        op.txn_id = Some(Ulid::from_bytes([3u8; 16]));
        op.state = UploadPartState::ReadExistingPart;
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: b"part".to_vec().into(),
            value: None,
        }));
        let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
            panic!("expected the part record, got {effects:?}")
        };
        let part = MultipartPart::from_bytes(value.as_ref()).unwrap();
        assert_eq!(part.backend_etag.as_deref(), Some("\"etag\""));
        assert!(part.location.partial);
    }

    #[test]
    fn rejects_write_error() {
        // A rejected body stream (e.g. trailer checksum mismatch) must
        // surface WriteFailed instead of InvalidOperationState.
        let mut op = UploadPartOperation::new(UploadPartInput {
            bucket: "mybucket".to_string(),
            key: "object.txt".to_string(),
            upload_id: Ulid::generate(),
            part_number: 1,
            content_length: None,
            body: None,
            created_by: test_user_id(),
            expected_checksums: Vec::new(),
        });
        op.state = UploadPartState::WritePart;

        let effects = op.step(Event::Blob(BlobEvent::Error(BlobError::StreamFailed(
            "checksum mismatch".to_string(),
        ))));

        assert!(effects.is_empty());
        assert!(op.is_complete());
        assert!(matches!(
            op.finalize(),
            Err(UploadPartError::WriteFailed(_))
        ));
    }

    #[test]
    fn rejects_server_write() {
        // A full or flapping disk must never be reported as a client bad digest.
        let mut op = UploadPartOperation::new(UploadPartInput {
            bucket: "mybucket".to_string(),
            key: "object.txt".to_string(),
            upload_id: Ulid::generate(),
            part_number: 1,
            content_length: None,
            body: None,
            created_by: test_user_id(),
            expected_checksums: Vec::new(),
        });
        op.state = UploadPartState::WritePart;

        let effects = op.step(Event::Blob(BlobEvent::Error(BlobError::WriteError(
            "No space left on device".to_string(),
        ))));

        assert!(effects.is_empty());
        assert!(op.is_complete());
        assert!(matches!(
            op.finalize(),
            Err(UploadPartError::BlobWriteFailed(_))
        ));
    }

    #[test]
    fn preserves_write_cleanup() {
        let backend_id = Ulid::from_bytes([5u8; 16]);
        let location = part_location(backend_id);
        let mut op = upload_part_op(backend_id);
        op.written_location = None;
        op.state = UploadPartState::WritePart;

        let effects = op.step(Event::Blob(BlobEvent::Error(BlobError::WriteCleanup {
            location: location.clone(),
            message: "cleanup failed".to_string(),
        })));
        assert_eq!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::Delete {
                location: location.clone()
            })]
        );

        let effects = op.step(Event::Blob(BlobEvent::Error(BlobError::DeleteError(
            "backend unavailable".to_string(),
        ))));
        let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
            panic!("expected durable delete, got {effects:?}")
        };
        assert_eq!(
            BlobCleanupWork::from_bytes(value.as_ref()).unwrap(),
            BlobCleanupWork::DeleteBlob { location }
        );
    }

    #[test]
    fn refuses_disabled_backend() {
        // The pin predates the disable, so only the finalize fence can catch it.
        let backend_id = Ulid::from_bytes([5u8; 16]);
        let mut op = upload_part_op(backend_id);
        op.state = UploadPartState::StartTransaction;
        op.written_location = Some(part_location(backend_id));

        op.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: Ulid::from_bytes([3u8; 16]),
        }));
        let effects = op.step(fence_clear());
        assert_eq!(op.state, UploadPartState::FenceBackend);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { .. })]
        ));

        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: b"x".to_vec().into(),
            value: Some(disabled_record(backend_id).into()),
        }));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::Delete { .. })]
        ));
        op.step(Event::Blob(BlobEvent::DeleteFinished));
        assert!(matches!(
            op.finalize(),
            Err(UploadPartError::BackendFenceError(
                BackendFenceError::Unavailable
            ))
        ));
    }

    #[test]
    fn fence_rejects_stray() {
        let backend_id = Ulid::from_bytes([5u8; 16]);
        let mut op = upload_part_op(backend_id);
        op.state = UploadPartState::FenceBackend;
        op.written_location = Some(part_location(backend_id));

        op.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));
        op.step(Event::Blob(BlobEvent::DeleteFinished));

        assert!(matches!(
            op.finalize(),
            Err(UploadPartError::BackendFenceError(BackendFenceError::Read(
                _
            )))
        ));
    }

    #[test]
    fn queues_part_row() {
        // Part reconciliation is written before its transaction can be ambiguous.
        let mut op = upload_part_op(Ulid::from_bytes([5u8; 16]));
        let txn_id = Ulid::from_bytes([3u8; 16]);
        op.txn_id = Some(txn_id);
        let location = op.written_location.clone().unwrap();

        let effects = op.write_cleanup_row();
        let [
            Effect::Storage(StorageEffect::Write {
                key_space,
                key,
                txn_id: observed,
                value,
                ..
            }),
        ] = effects.as_slice()
        else {
            panic!("expected a transactional reconciliation row, got {effects:?}")
        };
        assert_eq!(key_space, BLOB_CLEANUP_KEYSPACE);
        assert_eq!(key.as_ref(), location.ulid.to_bytes());
        assert_eq!(*observed, Some(txn_id));
        assert_eq!(
            BlobCleanupWork::from_bytes(value.as_ref()).unwrap(),
            BlobCleanupWork::ReconcileWrite {
                location,
                owner: WriteOwner::UploadPart {
                    upload_id: op.input.upload_id,
                    part_number: 1,
                },
            }
        );

        let effects = op.step(Event::Storage(StorageEvent::WriteResult {
            key: b"cleanup".to_vec().into(),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::CommitTransaction { txn_id: observed })]
                if *observed == txn_id
        ));
    }

    #[test]
    fn retries_row_failure() {
        let backend_id = Ulid::from_bytes([5u8; 16]);
        let txn_id = Ulid::from_bytes([3u8; 16]);
        let mut op = upload_part_op(backend_id);
        let location = op.written_location.clone().unwrap();
        op.state = UploadPartState::WriteCleanupRow;
        op.txn_id = Some(txn_id);

        let effects = op.step(Event::Storage(StorageEvent::Error {
            error: StorageError::WriteError("boom".to_string()),
        }));
        assert_eq!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::Delete {
                location: location.clone()
            })]
        );

        let effects = op.step(Event::Blob(BlobEvent::Error(BlobError::DeleteError(
            "backend unavailable".to_string(),
        ))));
        let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
            panic!("expected durable delete, got {effects:?}")
        };
        assert_eq!(
            BlobCleanupWork::from_bytes(value.as_ref()).unwrap(),
            BlobCleanupWork::DeleteBlob { location }
        );

        let effects = op.step(Event::Storage(StorageEvent::WriteResult {
            key: b"cleanup".to_vec().into(),
        }));
        assert_eq!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
        );
    }

    #[test]
    fn unknown_keeps_part() {
        // Only a proven refusal rolls the part back; any other commit failure
        // may already own the record, so the copy goes to reconciliation.
        for error in [
            StorageError::CommitFailed,
            StorageError::PersistError("journal".to_string()),
            StorageError::Timeout,
        ] {
            let mut op = upload_part_op(Ulid::from_bytes([5u8; 16]));
            op.state = UploadPartState::CommitTransaction;
            op.txn_id = Some(Ulid::from_bytes([3u8; 16]));
            let upload_id = op.input.upload_id;
            let location = op.written_location.clone().unwrap();

            let effects = op.step(Event::Storage(StorageEvent::Error {
                error: error.clone(),
            }));

            let [
                Effect::Storage(StorageEffect::Write {
                    key_space, value, ..
                }),
            ] = effects.as_slice()
            else {
                panic!("{error} must queue reconciliation, got {effects:?}")
            };
            assert_eq!(key_space, BLOB_CLEANUP_KEYSPACE);
            assert_eq!(
                BlobCleanupWork::from_bytes(value.as_ref()).unwrap(),
                BlobCleanupWork::ReconcileWrite {
                    location,
                    owner: WriteOwner::UploadPart {
                        upload_id,
                        part_number: 1,
                    },
                }
            );

            let effects = op.step(Event::Storage(StorageEvent::WriteResult {
                key: b"k".to_vec().into(),
            }));
            let [Effect::Blob(BlobEffect::ReleaseReservation { id })] = effects.as_slice() else {
                panic!("expected reservation release, got {effects:?}")
            };
            let id = *id;
            let effects = op.step(Event::Blob(BlobEvent::ReservationReleased { id }));
            assert!(effects.is_empty());
            assert!(op.is_complete());
            assert!(matches!(
                op.finalize(),
                Err(UploadPartError::StorageError(observed)) if observed == error
            ));
        }
    }

    #[test]
    fn retries_commit_conflict() {
        // The part bytes stay staged; only the record transaction reopens.
        let mut op = upload_part_op(Ulid::from_bytes([5u8; 16]));
        let location = op.written_location.clone().unwrap();
        op.state = UploadPartState::CommitTransaction;
        op.txn_id = Some(Ulid::from_bytes([3u8; 16]));

        let effects = op.step(Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionConflict,
        }));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::StartTransaction {
                read: false
            })]
        ));
        assert_eq!(op.state, UploadPartState::StartTransaction);
        assert!(op.txn_id.is_none());
        assert_eq!(op.written_location, Some(location.clone()));

        let retry_txn = TxnId::from_bytes([4u8; 16]);
        op.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: retry_txn,
        }));
        assert_eq!(op.state, UploadPartState::CheckPurgeFence);

        op.state = UploadPartState::CommitTransaction;
        let effects = op.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id: retry_txn,
        }));
        assert_eq!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::ReleaseReservation {
                id: location.ulid
            })]
        );
    }

    #[test]
    fn conflict_exhausts_retries() {
        let mut op = upload_part_op(Ulid::from_bytes([5u8; 16]));
        op.state = UploadPartState::CommitTransaction;
        op.txn_id = Some(Ulid::from_bytes([3u8; 16]));
        op.conflicts = CONFLICT_RETRIES;

        let effects = op.step(Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionConflict,
        }));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::Delete { .. })]
        ));
        assert!(op.step(Event::Blob(BlobEvent::DeleteFinished)).is_empty());
        assert!(matches!(
            op.finalize(),
            Err(UploadPartError::StorageError(
                StorageError::TransactionConflict
            ))
        ));
    }

    #[test]
    fn conflict_queues_delete() {
        // A rollback delete the backend refuses has to survive as cleanup work.
        let mut op = upload_part_op(Ulid::from_bytes([5u8; 16]));
        op.state = UploadPartState::CommitTransaction;
        op.txn_id = Some(Ulid::from_bytes([3u8; 16]));
        op.conflicts = CONFLICT_RETRIES;

        op.step(Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionConflict,
        }));
        let effects = op.step(Event::Blob(BlobEvent::Error(BlobError::DeleteError(
            "gone".to_string(),
        ))));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Write { key_space, .. })]
                if key_space == BLOB_CLEANUP_KEYSPACE
        ));
    }

    #[test]
    fn writes_old_cleanup() {
        let backend_id = Ulid::from_bytes([5u8; 16]);
        let old = BackendLocation {
            backend_path: "mybucket/old-part".to_string(),
            ..part_location(backend_id)
        };
        let mut op = upload_part_op(backend_id);
        let new = op.written_location.clone().unwrap();
        op.replaced_location = Some(old.clone());
        op.state = UploadPartState::WriteCleanupRow;
        op.txn_id = Some(TxnId::from_bytes([3u8; 16]));

        let effects = op.step(Event::Storage(StorageEvent::WriteResult {
            key: b"reconcile".to_vec().into(),
        }));
        let [
            Effect::Storage(StorageEffect::Write {
                key_space,
                value,
                txn_id,
                ..
            }),
        ] = effects.as_slice()
        else {
            panic!("expected transactional old-part cleanup, got {effects:?}")
        };
        assert_eq!(key_space, BLOB_CLEANUP_KEYSPACE);
        assert_eq!(*txn_id, Some(TxnId::from_bytes([3u8; 16])));
        assert_eq!(
            BlobCleanupWork::from_bytes(value.as_ref()).unwrap(),
            BlobCleanupWork::DeleteBlob { location: old }
        );

        let effects = op.step(Event::Storage(StorageEvent::WriteResult {
            key: b"cleanup".to_vec().into(),
        }));
        assert_eq!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::CommitTransaction {
                txn_id: TxnId::from_bytes([3u8; 16])
            })]
        );
        assert!(op.output.is_none());

        let effects = op.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id: TxnId::from_bytes([3u8; 16]),
        }));
        let [Effect::Blob(BlobEffect::ReleaseReservation { id })] = effects.as_slice() else {
            panic!("expected reservation release, got {effects:?}")
        };
        let id = *id;
        assert_eq!(op.state, UploadPartState::ReleaseReservation);
        assert!(
            op.step(Event::Blob(BlobEvent::ReservationReleased { id }))
                .is_empty()
        );
        assert_eq!(op.state, UploadPartState::Finish);
        let result = op.finalize().expect("expected successful replacement");
        assert_eq!(result.location, new);
    }

    #[test]
    fn release_failure_succeeds() {
        // A refused release runs after the part is committed, so it must not
        // report failure; the reconciliation row clears the reservation.
        let mut op = upload_part_op(Ulid::from_bytes([5u8; 16]));
        let location = op.written_location.clone().unwrap();
        op.state = UploadPartState::CommitTransaction;
        op.txn_id = Some(TxnId::from_bytes([3u8; 16]));

        let effects = op.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id: TxnId::from_bytes([3u8; 16]),
        }));
        assert_eq!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::ReleaseReservation {
                id: location.ulid
            })]
        );

        let effects = op.step(Event::Blob(BlobEvent::Error(BlobError::HandleMissing)));

        assert!(effects.is_empty());
        assert_eq!(op.state, UploadPartState::Finish);
        let result = op
            .finalize()
            .expect("expected the committed part to succeed");
        assert_eq!(result.location, location);
    }

    #[test]
    fn abort_queues_delete() {
        let backend_id = Ulid::from_bytes([5u8; 16]);
        let mut op = upload_part_op(backend_id);
        let location = op.written_location.clone().unwrap();
        op.state = UploadPartState::StartTransaction;

        let effects = op.abort();
        let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
            panic!("expected durable abort cleanup")
        };
        assert_eq!(
            BlobCleanupWork::from_bytes(value.as_ref()).unwrap(),
            BlobCleanupWork::DeleteBlob { location }
        );
        assert_eq!(op.state, UploadPartState::QueueCleanupRow);

        assert!(
            op.step(Event::Storage(StorageEvent::WriteResult {
                key: b"cleanup".to_vec().into(),
            }))
            .is_empty()
        );
        assert!(matches!(
            op.finalize(),
            Err(UploadPartError::UploadPartFailed)
        ));
    }

    #[test]
    fn queued_retries_exhaust() {
        // Rejected queued rows are re-emitted a bounded number of times; at
        // exhaustion the pending error surfaces instead of an endless retry.
        let backend_id = Ulid::from_bytes([5u8; 16]);
        let mut op = upload_part_op(backend_id);
        op.state = UploadPartState::StartTransaction;

        assert!(matches!(
            op.abort().as_slice(),
            [Effect::Storage(StorageEffect::Write { .. })]
        ));
        assert_eq!(op.state, UploadPartState::QueueCleanupRow);

        for _ in 0..3 {
            assert!(matches!(
                op.step(Event::Storage(StorageEvent::Error {
                    error: StorageError::Timeout,
                }))
                .as_slice(),
                [Effect::Storage(StorageEffect::Write { .. })]
            ));
        }
        let effects = op.step(Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        }));

        assert!(effects.is_empty());
        assert_eq!(op.state, UploadPartState::Error);
        assert!(matches!(
            op.finalize(),
            Err(UploadPartError::UploadPartFailed)
        ));
    }

    fn upload_part_op(backend_id: Ulid) -> UploadPartOperation {
        let mut op = UploadPartOperation::new(UploadPartInput {
            bucket: "mybucket".to_string(),
            key: "object.txt".to_string(),
            upload_id: Ulid::generate(),
            part_number: 1,
            content_length: None,
            body: None,
            created_by: test_user_id(),
            expected_checksums: Vec::new(),
        });
        op.written_location = Some(part_location(backend_id));
        op
    }

    fn part_location(backend_id: Ulid) -> BackendLocation {
        BackendLocation {
            backend: aruna_core::structs::storage::blob::BackendRef::Group(backend_id),
            storage_class: None,
            root: "root".to_string(),
            storage_bucket: "storage".to_string(),
            backend_path: "mybucket/object.txt".to_string(),
            ulid: Ulid::from_bytes([6u8; 16]),
            format: StoredFormat::default(),
            created_by: test_user_id(),
            created_at: SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 4,
            hashes: std::collections::HashMap::new(),
        }
    }

    fn disabled_record(backend_id: Ulid) -> Vec<u8> {
        aruna_core::structs::storage::group_backend::GroupStorage {
            backend_id,
            group_id: Ulid::from_bytes([7u8; 16]),
            name: "tenant".to_string(),
            kind: aruna_core::structs::storage::group_backend::GroupBackendKind::S3,
            public_config: std::collections::HashMap::new(),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            disabled: true,
            cleanup: aruna_core::structs::storage::cleanup::CleanupStrategy::Retain,
        }
        .to_bytes()
        .unwrap()
    }

    #[tokio::test]
    async fn missing_upload_rejected() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };

        let result = drive(
            UploadPartOperation::new(UploadPartInput {
                bucket: "mybucket".to_string(),
                key: "object.txt".to_string(),
                upload_id: Ulid::generate(),
                part_number: 1,
                content_length: None,
                body: None,
                created_by: test_user_id(),
                expected_checksums: Vec::new(),
            }),
            &context,
        )
        .await;

        assert!(matches!(result, Err(UploadPartError::NoSuchUpload)));
    }
}
