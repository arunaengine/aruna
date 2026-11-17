//! Aborts a multipart upload, deletes its records and queues the staged blobs for cleanup.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::distribution::inventory::{InventoryChange, InventoryError, InventoryUpdate};
use crate::s3::multipart::target::{StatusCheck, UploadTargetError, validate_upload};
use crate::s3::usage::upload_count;
use crate::s3::write_cleanup::{WriteCleanup, delete_records_effect};
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
        let delete = delete_records_effect(self.input.upload_id, &self.parts, None)?;
        self.step = DeleteStep::DeleteRecords;
        Ok(Next::Call(delete.into()))
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
                result.event()?;
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
