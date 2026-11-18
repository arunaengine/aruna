//! Aborts a multipart upload, deletes its records and queues the staged blobs for cleanup.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::distribution::inventory::{InventoryChange, InventoryError, InventoryUpdate};
use crate::s3::multipart::target::{StatusCheck, UploadTargetError, validate_upload};
use crate::s3::usage::upload_count;
use crate::s3::write_cleanup::{WriteCleanup, record_rows};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event};
use aruna_core::keyspaces::{
    BLOB_CLEANUP_KEYSPACE, S3_BUCKET_KEYSPACE, UPLOAD_KEYSPACE, UPLOAD_PART_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::read_row;
use aruna_core::structs::storage::blob::{BlobCleanupWork, BucketInfo};
use aruna_core::structs::storage::multipart::{
    MultipartPart, MultipartPartKey, MultipartUpload, MultipartUploadStatus,
};
use aruna_core::types::Value;
use thiserror::Error;
use ulid::Ulid;

#[derive(Debug, Eq, PartialEq)]
pub enum AbortUploadState {
    /// Marks the upload aborting in its own transaction.
    MarkUpload,
    AbortBackendUpload,
    ReadUploadParts,
    /// Deletes the records and queues the part blobs in one transaction.
    DeleteUploadRecords,
    /// Reopens the upload after a failure, then reports the failure.
    ResetUpload,
}

#[derive(Debug, Error, PartialEq)]
pub enum AbortUploadError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("Invalid operation state")]
    InvalidOperationState,
    #[error("The specified upload does not exist.")]
    NoSuchUpload,
    #[error("The specified multipart upload does not match the target object.")]
    UploadTargetMismatch,
    #[error("The multipart upload is no longer open.")]
    UploadNotOpen,
    #[error("The upload is being completed, retry shortly.")]
    CompletionInProgress,
    #[error("AbortMultipartUpload failed")]
    AbortUploadFailed,
    #[error(transparent)]
    BlobError(#[from] BlobError),
    #[error(transparent)]
    Inventory(#[from] InventoryError),
}

impl From<UploadTargetError> for AbortUploadError {
    fn from(error: UploadTargetError) -> Self {
        match error {
            UploadTargetError::TargetMismatch => Self::UploadTargetMismatch,
            UploadTargetError::NotOpen => Self::UploadNotOpen,
            UploadTargetError::CompletionInProgress => Self::CompletionInProgress,
        }
    }
}

impl From<IoError> for AbortUploadError {
    fn from(error: IoError) -> Self {
        Self::StorageError(error.into())
    }
}

type Turn<T> = Result<Next<T>, AbortUploadError>;

#[derive(Clone, Debug, PartialEq)]
pub struct AbortUploadInput {
    pub bucket: String,
    pub key: String,
    pub upload_id: Ulid,
    /// Wall clock (epoch ms) an in-progress completion lease is judged against.
    pub now_ms: u64,
}

impl AbortUploadInput {
    fn upload_key(&self) -> Vec<u8> {
        self.upload_id.to_bytes().to_vec()
    }
}

#[derive(Debug, PartialEq)]
pub struct AbortUploadOperation {
    input: AbortUploadInput,
    state: AbortUploadState,
    upload_record: Option<MultipartUpload>,
    upload_parts: Vec<MultipartPart>,
    skip_status_check: bool,
    resume_abort: bool,
    cleanup: WriteCleanup<AbortUploadError>,
}

impl AbortUploadOperation {
    pub fn new(input: AbortUploadInput) -> Self {
        Self {
            input,
            state: AbortUploadState::MarkUpload,
            upload_record: None,
            upload_parts: Vec::new(),
            skip_status_check: false,
            resume_abort: false,
            cleanup: WriteCleanup::default(),
        }
    }

    /// A purge may skip status checks after it owns the destination write fence.
    pub fn including_in_progress(mut self) -> Self {
        self.skip_status_check = true;
        self
    }

    /// Also finishes an abort that stopped midway, judged on the record read in this transaction.
    pub fn resuming_abort(mut self) -> Self {
        self.resume_abort = true;
        self
    }

    fn check(&self) -> StatusCheck {
        let now_ms = self.input.now_ms;
        match (self.skip_status_check, self.resume_abort) {
            (true, _) => StatusCheck::Skip,
            (false, true) => StatusCheck::Recover { now_ms },
            (false, false) => StatusCheck::Takeover { now_ms },
        }
    }

    /// Reopens the upload, then reports `error`.
    fn reset(&mut self, error: AbortUploadError) -> Turn<()> {
        self.cleanup.set_error(error);
        self.state = AbortUploadState::ResetUpload;
        let reset = ResetUpload {
            input: self.input.clone(),
            read: false,
        };
        Ok(Next::Call(IoCall::child(reset)))
    }

    fn marked(&mut self, record: MultipartUpload) -> Turn<()> {
        // The provider parts go first: a failure reopens the upload for a retry.
        let backend_upload = record.backend_upload.clone();
        self.upload_record = Some(record);
        let Some(backend_upload) = backend_upload else {
            return self.read_upload_parts();
        };
        self.state = AbortUploadState::AbortBackendUpload;
        let abort = Effect::Blob(BlobEffect::AbortUpload {
            backend_upload: Box::new(backend_upload),
        });
        Ok(Next::Call(abort.into()))
    }

    fn backend_aborted(&mut self, event: Event) -> Turn<()> {
        match event {
            Event::Blob(BlobEvent::UploadAborted) => self.read_upload_parts(),
            Event::Blob(BlobEvent::Error(error)) => self.reset(error.into()),
            _ => self.reset(AbortUploadError::InvalidOperationState),
        }
    }

    fn read_upload_parts(&mut self) -> Turn<()> {
        let prefix = match MultipartPartKey::prefix(self.input.upload_id) {
            Ok(prefix) => prefix,
            Err(err) => return self.reset(err.into()),
        };
        self.state = AbortUploadState::ReadUploadParts;
        Ok(Next::Call(IoCall::Page {
            space: UPLOAD_PART_KEYSPACE.to_string(),
            prefix: prefix.into(),
            after: None,
            limit: 10_000,
        }))
    }

    fn upload_parts_read(&mut self, result: IoResult) -> Turn<()> {
        let rows = match result.page() {
            Ok((rows, _)) => rows,
            Err(error) => return self.reset(error.into()),
        };
        self.upload_parts = rows
            .into_iter()
            .filter_map(|(_, value)| MultipartPart::from_bytes(value.as_ref()).ok())
            .collect();
        self.state = AbortUploadState::DeleteUploadRecords;
        let delete = DeleteRecords::new(self.input.clone(), self.upload_parts.clone());
        Ok(Next::Call(IoCall::child(delete)))
    }

    /// The part blobs go once their records are gone; a failed delete leaves them to reclaim.
    fn deleted(&mut self) -> Turn<()> {
        self.upload_record = None;
        let calls = (self.upload_parts.iter())
            .map(|part| {
                let location = part.location.clone();
                IoCall::from(Effect::Blob(BlobEffect::Delete { location }))
            })
            .collect();
        Ok(Next::Then { output: (), calls })
    }
}

impl Operation for AbortUploadOperation {
    type Output = ();
    type Error = AbortUploadError;

    fn first(&mut self) -> Turn<()> {
        self.state = AbortUploadState::MarkUpload;
        let mark = MarkAborting {
            input: self.input.clone(),
            check: self.check(),
            record: None,
        };
        Ok(Next::Call(IoCall::child(mark)))
    }

    fn next(&mut self, result: IoResult) -> Turn<()> {
        match self.state {
            AbortUploadState::MarkUpload => {
                match result.child::<MultipartUpload, AbortUploadError>()? {
                    Ok(record) => self.marked(record),
                    // A commit that may have applied leaves the upload aborting: reopen it.
                    Err(AbortUploadError::StorageError(error)) if !error.proves_no_commit() => {
                        self.reset(error.into())
                    }
                    Err(error) => Err(error),
                }
            }
            AbortUploadState::AbortBackendUpload => self.backend_aborted(result.event()?),
            AbortUploadState::ReadUploadParts => self.upload_parts_read(result),
            AbortUploadState::DeleteUploadRecords => {
                match result.child::<(), AbortUploadError>()? {
                    Ok(()) => self.deleted(),
                    Err(error) => self.reset(error),
                }
            }
            AbortUploadState::ResetUpload => {
                result.child::<(), AbortUploadError>()??;
                let error = self.cleanup.take_error();
                Err(error.unwrap_or(AbortUploadError::AbortUploadFailed))
            }
        }
    }

    fn expected_error(error: &Self::Error) -> bool {
        matches!(
            error,
            AbortUploadError::NoSuchUpload
                | AbortUploadError::UploadTargetMismatch
                | AbortUploadError::UploadNotOpen
                | AbortUploadError::CompletionInProgress
        )
    }
}

/// Marks the upload aborting in one transaction; its output is the record it wrote. A failure
/// before the commit is never a storage error, so the caller reopens only after the commit.
#[derive(Debug, PartialEq)]
struct MarkAborting {
    input: AbortUploadInput,
    check: StatusCheck,
    record: Option<MultipartUpload>,
}

impl Operation for MarkAborting {
    type Output = MultipartUpload;
    type Error = AbortUploadError;

    fn first(&mut self) -> Turn<MultipartUpload> {
        Ok(Next::Call(read_row(
            UPLOAD_KEYSPACE,
            self.input.upload_key(),
        )))
    }

    fn next(&mut self, result: IoResult) -> Turn<MultipartUpload> {
        if let Some(record) = self.record.take() {
            result
                .done()
                .map_err(|_| AbortUploadError::InvalidOperationState)?;
            return Ok(Next::Done(record));
        }
        let value = (result.value()).map_err(|_| AbortUploadError::InvalidOperationState)?;
        let value = value.ok_or(AbortUploadError::NoSuchUpload)?;
        let mut record = MultipartUpload::from_bytes(value.as_ref())?;
        validate_upload(&record, &self.input.bucket, &self.input.key, self.check)?;
        record.status = MultipartUploadStatus::Aborting;
        let row = (
            UPLOAD_KEYSPACE.to_string(),
            self.input.upload_key().into(),
            record.to_bytes()?.into(),
        );
        self.record = Some(record);
        Ok(Next::Call(IoCall::Write(vec![row])))
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

#[derive(Debug, PartialEq)]
enum DeleteStep {
    /// Reads the record in the delete transaction, so only the delete that finds it counts.
    ReadRecord,
    /// Reads the bucket's distribution binding, whose upload inventory the delete closes.
    ReadBinding,
    CountUpload,
    CountInventory,
    DeleteRecords,
    WriteCleanup,
}

/// Deletes the upload's records and queues its part blobs for cleanup in one transaction.
#[derive(Debug, PartialEq)]
struct DeleteRecords {
    input: AbortUploadInput,
    parts: Vec<MultipartPart>,
    step: DeleteStep,
    inventory: Option<InventoryUpdate>,
}

impl DeleteRecords {
    fn new(input: AbortUploadInput, parts: Vec<MultipartPart>) -> Self {
        Self {
            input,
            parts,
            step: DeleteStep::ReadRecord,
            inventory: None,
        }
    }

    /// A concurrent delete of the record conflicts with this read, so a record is counted
    /// closed once.
    fn record_read(&mut self, value: Option<Value>) -> Turn<()> {
        if value.is_none() {
            return self.delete_records();
        }
        self.step = DeleteStep::ReadBinding;
        let bucket = self.input.bucket.as_bytes().to_vec();
        Ok(Next::Call(read_row(S3_BUCKET_KEYSPACE, bucket)))
    }

    fn binding_read(&mut self, value: Option<Value>) -> Turn<()> {
        let bucket = value
            .map(|value| BucketInfo::from_bytes(&value))
            .transpose()?;
        self.inventory = bucket
            .and_then(|bucket| bucket.distribution)
            .map(|binding| {
                let mut change = InventoryChange::new(binding.set_id, self.input.key.clone());
                change.upload = Some((self.input.upload_id, false));
                InventoryUpdate::new(change)
            });
        self.step = DeleteStep::CountUpload;
        Ok(Next::Call(upload_count(&self.input.bucket, -1)))
    }

    fn upload_counted(&mut self) -> Turn<()> {
        let call = self
            .inventory
            .as_mut()
            .and_then(InventoryUpdate::start_calls);
        match call {
            Some(call) => {
                self.step = DeleteStep::CountInventory;
                Ok(Next::Call(call.into_call()))
            }
            None => self.delete_records(),
        }
    }

    fn inventory_counted(&mut self, result: IoResult) -> Turn<()> {
        let inventory = self.inventory.as_mut();
        let inventory = inventory.ok_or(AbortUploadError::InvalidOperationState)?;
        match inventory.step_calls(result)? {
            Some(call) => Ok(Next::Call(call.into_call())),
            None => self.delete_records(),
        }
    }

    fn delete_records(&mut self) -> Turn<()> {
        let deletes = record_rows(self.input.upload_id, &self.parts)?;
        self.step = DeleteStep::DeleteRecords;
        Ok(Next::Call(IoCall::Delete(deletes)))
    }

    fn cleanup_records(&mut self) -> Turn<()> {
        let mut writes = Vec::with_capacity(self.parts.len());
        for part in &self.parts {
            let work = BlobCleanupWork::DeleteBlob {
                location: part.location.clone(),
            };
            writes.push((
                BLOB_CLEANUP_KEYSPACE.to_string(),
                Ulid::generate().to_bytes().to_vec().into(),
                work.to_bytes()?.into(),
            ));
        }
        self.step = DeleteStep::WriteCleanup;
        Ok(Next::Call(IoCall::Write(writes)))
    }
}

impl Operation for DeleteRecords {
    type Output = ();
    type Error = AbortUploadError;

    fn first(&mut self) -> Turn<()> {
        Ok(Next::Call(read_row(
            UPLOAD_KEYSPACE,
            self.input.upload_key(),
        )))
    }

    fn next(&mut self, result: IoResult) -> Turn<()> {
        match self.step {
            DeleteStep::ReadRecord => self.record_read(result.value()?),
            DeleteStep::ReadBinding => self.binding_read(result.value()?),
            DeleteStep::CountUpload => {
                result.done()?;
                self.upload_counted()
            }
            DeleteStep::CountInventory => self.inventory_counted(result),
            DeleteStep::DeleteRecords => {
                result.done()?;
                self.cleanup_records()
            }
            DeleteStep::WriteCleanup => {
                result.done()?;
                Ok(Next::Done(()))
            }
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

/// Reopens a marked upload in one transaction; a record already gone stays gone.
#[derive(Debug, PartialEq)]
struct ResetUpload {
    input: AbortUploadInput,
    read: bool,
}

impl Operation for ResetUpload {
    type Output = ();
    type Error = AbortUploadError;

    fn first(&mut self) -> Turn<()> {
        Ok(Next::Call(read_row(
            UPLOAD_KEYSPACE,
            self.input.upload_key(),
        )))
    }

    fn next(&mut self, result: IoResult) -> Turn<()> {
        if self.read {
            result.done()?;
            return Ok(Next::Done(()));
        }
        let Some(value) = result.value()? else {
            return Ok(Next::Done(()));
        };
        let mut record = MultipartUpload::from_bytes(value.as_ref())?;
        record.status = MultipartUploadStatus::Open;
        let row = (
            UPLOAD_KEYSPACE.to_string(),
            self.input.upload_key().into(),
            record.to_bytes()?.into(),
        );
        self.read = true;
        Ok(Next::Call(IoCall::Write(vec![row])))
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

#[cfg(test)]
mod pure_tests {
    use super::*;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::storage::blob::{BackendLocation, BackendRef};
    use aruna_core::structs::storage::format::StoredFormat;
    use aruna_core::structs::storage::multipart::BackendUpload;

    use std::collections::HashMap;
    use std::time::SystemTime;

    fn input() -> AbortUploadInput {
        AbortUploadInput {
            bucket: "bucket".to_string(),
            key: "object".to_string(),
            upload_id: Ulid::from_bytes([1u8; 16]),
            now_ms: 1_000,
        }
    }

    fn record() -> MultipartUpload {
        let input = input();
        MultipartUpload {
            upload_id: input.upload_id,
            backend: BackendRef::node_default(),
            storage_class: None,
            bucket: input.bucket,
            key: input.key,
            group_id: Ulid::from_bytes([2u8; 16]),
            created_by: Default::default(),
            created_at: SystemTime::UNIX_EPOCH,
            status: MultipartUploadStatus::Aborting,
            checksum_hint: None,
            metadata: HashMap::new(),
            placement_policies: Vec::new(),
            subject_generation: 0,
            completing_since_ms: None,
            backend_upload: None,
            encryption: None,
        }
    }

    /// An abort whose mark transaction answers with `marked`.
    fn marked(
        marked: Result<MultipartUpload, AbortUploadError>,
    ) -> (AbortUploadOperation, Turn<()>) {
        let mut operation = AbortUploadOperation::new(input());
        operation.first().unwrap();
        let turn = operation.next(IoResult::Child(ChildOutcome::new(marked)));
        (operation, turn)
    }

    fn part() -> MultipartPart {
        MultipartPart {
            part_number: 1,
            location: part_location(),
            created_at: SystemTime::UNIX_EPOCH,
            backend_etag: None,
            piece: None,
        }
    }

    #[test]
    fn resume_spares_completion() {
        let mut record = record();
        record.status = MultipartUploadStatus::Completing;
        record.completing_since_ms = Some(input().now_ms);
        let operation = AbortUploadOperation::new(input()).resuming_abort();
        let mut mark = MarkAborting {
            input: input(),
            check: operation.check(),
            record: None,
        };
        mark.first().unwrap();

        let read = IoResult::Values(vec![Some(record.to_bytes().unwrap().into())]);
        let turn = mark.next(read);

        assert_eq!(turn.err(), Some(AbortUploadError::CompletionInProgress));
    }

    #[test]
    fn aborts_provider_first() {
        // The provider parts go before any record, and a failed provider abort reopens the upload.
        for (answer, aborted) in [
            (BlobEvent::UploadAborted, true),
            (
                BlobEvent::Error(BlobError::DeleteError("unreachable".to_string())),
                false,
            ),
        ] {
            let mut record = record();
            record.backend_upload = Some(BackendUpload {
                location: part_location(),
                upload_id: "provider".to_string(),
                record_id: Ulid::from_bytes([9u8; 16]),
            });
            let (mut operation, turn) = marked(Ok(record));
            let Ok(Next::Call(IoCall::Effect(effect))) = turn else {
                panic!("expected the provider abort")
            };
            assert!(matches!(
                *effect,
                Effect::Blob(BlobEffect::AbortUpload { .. })
            ));

            let turn = operation.next(IoResult::Event(Box::new(Event::Blob(answer))));
            match aborted {
                true => assert!(matches!(turn, Ok(Next::Call(IoCall::Page { .. })))),
                false => {
                    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
                    assert_eq!(operation.state, AbortUploadState::ResetUpload);
                }
            }
        }
    }

    #[test]
    fn unknown_mark_resets() {
        let failed = Err(AbortUploadError::StorageError(StorageError::CommitFailed));
        let (mut operation, turn) = marked(failed);

        assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
        assert_eq!(operation.state, AbortUploadState::ResetUpload);
        assert_eq!(
            operation.cleanup.take_error(),
            Some(AbortUploadError::StorageError(StorageError::CommitFailed))
        );
    }

    #[test]
    fn conflict_mark_aborts() {
        let conflict = AbortUploadError::StorageError(StorageError::TransactionConflict);
        let (_, turn) = marked(Err(conflict));

        assert_eq!(
            turn.err(),
            Some(AbortUploadError::StorageError(
                StorageError::TransactionConflict
            ))
        );
    }

    #[test]
    fn committed_mark_continues() {
        let (operation, turn) = marked(Ok(record()));

        assert!(matches!(
            turn,
            Ok(Next::Call(IoCall::Page { space, .. })) if space == UPLOAD_PART_KEYSPACE
        ));
        assert_eq!(operation.state, AbortUploadState::ReadUploadParts);
    }

    #[test]
    fn deleted_frees_parts() {
        // The part blobs go only after the delete transaction committed.
        let (mut operation, _) = marked(Ok(record()));
        let rows = vec![(Vec::new().into(), part().to_bytes().unwrap().into())];
        let turn = operation.next(IoResult::Page { rows, next: None });
        assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));

        let turn = operation.next(IoResult::Child(ChildOutcome::new(
            Ok::<(), AbortUploadError>(()),
        )));

        let Ok(Next::Then { calls, .. }) = turn else {
            panic!("expected the blob deletes")
        };
        let location = part_location();
        let delete = IoCall::from(Effect::Blob(BlobEffect::Delete { location }));
        assert_eq!(calls, [delete]);
    }

    #[test]
    fn queues_cleanup_commit() {
        let location = part_location();
        let mut delete = DeleteRecords::new(input(), vec![part()]);
        delete.step = DeleteStep::DeleteRecords;

        let turn = delete.next(IoResult::Done);

        let Ok(Next::Call(IoCall::Write(writes))) = turn else {
            panic!("expected cleanup records")
        };
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].0, BLOB_CLEANUP_KEYSPACE);
        assert_eq!(
            BlobCleanupWork::from_bytes(writes[0].2.as_ref()).unwrap(),
            BlobCleanupWork::DeleteBlob { location }
        );
        assert!(matches!(delete.next(IoResult::Done), Ok(Next::Done(()))));
    }

    #[test]
    fn counts_found_record() {
        // Only a delete that still finds the record in its transaction counts the upload closed.
        for found in [true, false] {
            let mut delete = DeleteRecords::new(input(), Vec::new());
            let turn = delete.first();
            assert!(matches!(
                turn,
                Ok(Next::Call(IoCall::Read(reads))) if reads[0].0 == UPLOAD_KEYSPACE
            ));
            let value = found.then(|| vec![1u8].into());
            let mut turn = delete.next(IoResult::Values(vec![value]));
            if found {
                assert!(matches!(
                    &turn,
                    Ok(Next::Call(IoCall::Read(reads))) if reads[0].0 == S3_BUCKET_KEYSPACE
                ));
                turn = delete.next(IoResult::Values(vec![None]));
                let Ok(Next::Call(IoCall::AddUsage { buckets, .. })) = &turn else {
                    panic!("expected the upload count, got {turn:?}")
                };
                let uploads: Vec<_> = buckets
                    .iter()
                    .map(|(_, delta)| delta.open_uploads)
                    .collect();
                assert_eq!(uploads, [-1]);
                turn = delete.next(IoResult::Done);
            }
            assert!(matches!(turn, Ok(Next::Call(IoCall::Delete(_)))));
        }
    }

    #[test]
    fn delete_batches_records() {
        // The abort removes every part row, the upload record and its pending envelope in one
        // batch inside the delete transaction.
        let mut delete = DeleteRecords::new(input(), vec![part()]);

        let turn = delete.delete_records();

        let Ok(Next::Call(IoCall::Delete(deletes))) = turn else {
            panic!("expected upload records delete")
        };
        assert_eq!(deletes.len(), 3);
        let upload_key = input().upload_key();
        for keyspace in [UPLOAD_KEYSPACE, aruna_core::keyspaces::ABE_PENDING_KEYSPACE] {
            assert!(deletes.iter().any(|(key_space, key)| {
                key_space == keyspace && key.as_ref() == upload_key.as_slice()
            }));
        }
        let part_key = MultipartPartKey::new(input().upload_id, 1)
            .to_bytes()
            .unwrap();
        assert!(deletes.iter().any(|(key_space, key)| {
            key_space == UPLOAD_PART_KEYSPACE && key.as_ref() == part_key.as_slice()
        }));
    }

    fn part_location() -> BackendLocation {
        BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "root".to_string(),
            storage_bucket: "parts".to_string(),
            backend_path: "upload/part".to_string(),
            ulid: Ulid::from_bytes([4u8; 16]),
            format: StoredFormat::default(),
            created_by: Default::default(),
            created_at: SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 4,
            hashes: HashMap::new(),
        }
    }
}
