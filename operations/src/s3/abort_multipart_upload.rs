use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{S3_MULTIPART_UPLOAD_KEYSPACE, S3_MULTIPART_UPLOAD_PART_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::{
    MultipartUpload, MultipartUploadPart, MultipartUploadPartKey, MultipartUploadStatus,
};
use aruna_core::types::{Effects, TxnId, Value};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

#[derive(Debug, Eq, PartialEq)]
pub enum AbortMultipartUploadState {
    Init,
    StartMarkTransaction,
    ReadUploadForMark,
    WriteUploadAborting,
    CommitMarkTransaction,
    ReadUploadParts,
    StartDeleteTransaction,
    DeleteUploadRecords,
    CommitDeleteTransaction,
    CleanupPartBlobs,
    ResetUploadTransaction,
    ReadUploadForReset,
    WriteUploadReset,
    CommitResetTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum AbortMultipartUploadError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("Invalid operation state")]
    InvalidOperationState,
    #[error("No transaction found")]
    NoTransactionFound,
    #[error("The specified upload does not exist.")]
    NoSuchUpload,
    #[error("The specified multipart upload does not match the target object.")]
    UploadTargetMismatch,
    #[error("The multipart upload is no longer open.")]
    UploadNotOpen,
    #[error("AbortMultipartUpload failed")]
    AbortMultipartUploadFailed,
}

#[derive(Debug, PartialEq)]
pub struct AbortMultipartUploadInput {
    pub bucket: String,
    pub key: String,
    pub upload_id: Ulid,
}

#[derive(Debug, PartialEq)]
pub struct AbortMultipartUploadOperation {
    input: AbortMultipartUploadInput,
    state: AbortMultipartUploadState,
    txn_id: Option<TxnId>,
    upload_record: Option<MultipartUpload>,
    upload_parts: Vec<MultipartUploadPart>,
    cleanup_index: usize,
    pending_error: Option<AbortMultipartUploadError>,
    output: Option<Result<(), AbortMultipartUploadError>>,
}

impl AbortMultipartUploadOperation {
    pub fn new(input: AbortMultipartUploadInput) -> Self {
        Self {
            input,
            state: AbortMultipartUploadState::Init,
            txn_id: None,
            upload_record: None,
            upload_parts: Vec::new(),
            cleanup_index: 0,
            pending_error: None,
            output: None,
        }
    }

    fn emit_error(&mut self, error: AbortMultipartUploadError) -> Effects {
        self.state = AbortMultipartUploadState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn schedule_error(&mut self, error: AbortMultipartUploadError) -> Effects {
        self.pending_error = Some(error);
        if self.upload_record.is_some() {
            self.state = AbortMultipartUploadState::ResetUploadTransaction;
            smallvec![Effect::Storage(StorageEffect::StartTransaction {
                read: false,
            })]
        } else {
            self.emit_pending_error()
        }
    }

    fn emit_pending_error(&mut self) -> Effects {
        let Some(error) = self.pending_error.take() else {
            return self.emit_error(AbortMultipartUploadError::AbortMultipartUploadFailed);
        };
        self.emit_error(error)
    }

    fn validate_upload(&self, record: &MultipartUpload) -> Result<(), AbortMultipartUploadError> {
        if record.bucket != self.input.bucket || record.key != self.input.key {
            return Err(AbortMultipartUploadError::UploadTargetMismatch);
        }
        if record.status != MultipartUploadStatus::Open {
            return Err(AbortMultipartUploadError::UploadNotOpen);
        }
        Ok(())
    }

    fn handle_init(&mut self) -> Effects {
        self.state = AbortMultipartUploadState::StartMarkTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn handle_mark_transaction_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.emit_error(AbortMultipartUploadError::InvalidOperationState);
        };
        self.txn_id = Some(txn_id);
        self.state = AbortMultipartUploadState::ReadUploadForMark;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_MULTIPART_UPLOAD_KEYSPACE.to_string(),
            key: self.input.upload_id.to_bytes().to_vec().into(),
            txn_id: Some(txn_id),
        })]
    }

    fn handle_upload_read_for_mark(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(AbortMultipartUploadError::InvalidOperationState);
        };
        let Some(value) = value else {
            return self.emit_error(AbortMultipartUploadError::NoSuchUpload);
        };
        let mut record = match MultipartUpload::from_bytes(value.as_ref()) {
            Ok(record) => record,
            Err(err) => return self.emit_error(err.into()),
        };
        if let Err(err) = self.validate_upload(&record) {
            return self.emit_error(err);
        }

        record.status = MultipartUploadStatus::Aborting;
        self.upload_record = Some(record.clone());
        let value = match record.to_bytes() {
            Ok(value) => value,
            Err(err) => return self.emit_error(err.into()),
        };
        self.state = AbortMultipartUploadState::WriteUploadAborting;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: S3_MULTIPART_UPLOAD_KEYSPACE.to_string(),
            key: self.input.upload_id.to_bytes().to_vec().into(),
            value: value.into(),
            txn_id: self.txn_id,
        })]
    }

    fn handle_upload_marked(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.emit_error(AbortMultipartUploadError::InvalidOperationState);
        };
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(AbortMultipartUploadError::NoTransactionFound);
        };

        self.state = AbortMultipartUploadState::CommitMarkTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_mark_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.emit_error(AbortMultipartUploadError::InvalidOperationState);
        };
        self.txn_id = None;

        let prefix = match MultipartUploadPartKey::prefix(self.input.upload_id) {
            Ok(prefix) => prefix,
            Err(err) => return self.schedule_error(err.into()),
        };
        self.state = AbortMultipartUploadState::ReadUploadParts;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: S3_MULTIPART_UPLOAD_PART_KEYSPACE.to_string(),
            prefix: Some(prefix.into()),
            start_after: None,
            limit: 10_000,
            txn_id: None,
        })]
    }

    fn handle_upload_parts_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult { values, .. }) = event else {
            return self.schedule_error(AbortMultipartUploadError::InvalidOperationState);
        };

        self.upload_parts = values
            .into_iter()
            .filter_map(|(_, value)| MultipartUploadPart::from_bytes(value.as_ref()).ok())
            .collect();
        self.state = AbortMultipartUploadState::StartDeleteTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn handle_delete_transaction_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.schedule_error(AbortMultipartUploadError::InvalidOperationState);
        };
        self.txn_id = Some(txn_id);
        self.delete_upload_records()
    }

    fn delete_upload_records(&mut self) -> Effects {
        let mut deletes = Vec::with_capacity(self.upload_parts.len() + 1);
        for part in &self.upload_parts {
            let key = match MultipartUploadPartKey::new(self.input.upload_id, part.part_number)
                .to_bytes()
            {
                Ok(key) => key,
                Err(err) => return self.schedule_error(err.into()),
            };
            deletes.push((S3_MULTIPART_UPLOAD_PART_KEYSPACE.to_string(), key.into()));
        }
        deletes.push((
            S3_MULTIPART_UPLOAD_KEYSPACE.to_string(),
            self.input.upload_id.to_bytes().to_vec().into(),
        ));

        self.state = AbortMultipartUploadState::DeleteUploadRecords;
        smallvec![Effect::Storage(StorageEffect::BatchDelete {
            deletes,
            txn_id: self.txn_id,
        })]
    }

    fn handle_upload_records_deleted(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchDeleteResult { .. }) = event else {
            return self.schedule_error(AbortMultipartUploadError::InvalidOperationState);
        };
        let Some(txn_id) = self.txn_id else {
            return self.schedule_error(AbortMultipartUploadError::NoTransactionFound);
        };
        self.state = AbortMultipartUploadState::CommitDeleteTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_delete_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.schedule_error(AbortMultipartUploadError::InvalidOperationState);
        };
        self.txn_id = None;
        self.upload_record = None;
        self.cleanup_index = 0;
        self.state = AbortMultipartUploadState::CleanupPartBlobs;
        self.cleanup_next_part_blob()
    }

    fn cleanup_next_part_blob(&mut self) -> Effects {
        let Some(part) = self.upload_parts.get(self.cleanup_index) else {
            self.state = AbortMultipartUploadState::Finish;
            self.output = Some(Ok(()));
            return smallvec![];
        };

        self.state = AbortMultipartUploadState::CleanupPartBlobs;
        smallvec![Effect::Blob(BlobEffect::Delete {
            location: part.location.clone(),
        })]
    }

    fn handle_cleanup_part_blob(&mut self, event: Event) -> Effects {
        match event {
            Event::Blob(BlobEvent::DeleteFinished) | Event::Blob(BlobEvent::Error(_)) => {
                self.cleanup_index += 1;
                self.cleanup_next_part_blob()
            }
            _ => self.emit_error(AbortMultipartUploadError::InvalidOperationState),
        }
    }

    fn handle_reset_transaction_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.emit_error(AbortMultipartUploadError::InvalidOperationState);
        };
        self.txn_id = Some(txn_id);
        self.state = AbortMultipartUploadState::ReadUploadForReset;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_MULTIPART_UPLOAD_KEYSPACE.to_string(),
            key: self.input.upload_id.to_bytes().to_vec().into(),
            txn_id: Some(txn_id),
        })]
    }

    fn handle_upload_read_for_reset(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(AbortMultipartUploadError::InvalidOperationState);
        };
        let Some(value) = value else {
            return self.txn_id.take().map_or_else(
                || self.emit_pending_error(),
                |txn_id| smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            );
        };
        let mut record = match MultipartUpload::from_bytes(value.as_ref()) {
            Ok(record) => record,
            Err(err) => return self.emit_error(err.into()),
        };
        record.status = MultipartUploadStatus::Open;
        self.upload_record = Some(record.clone());
        let value: Value = match record.to_bytes() {
            Ok(value) => value.into(),
            Err(err) => return self.emit_error(err.into()),
        };
        self.state = AbortMultipartUploadState::WriteUploadReset;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: S3_MULTIPART_UPLOAD_KEYSPACE.to_string(),
            key: self.input.upload_id.to_bytes().to_vec().into(),
            value,
            txn_id: self.txn_id,
        })]
    }

    fn handle_upload_reset_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.emit_error(AbortMultipartUploadError::InvalidOperationState);
        };
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(AbortMultipartUploadError::NoTransactionFound);
        };
        self.state = AbortMultipartUploadState::CommitResetTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_reset_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.emit_error(AbortMultipartUploadError::InvalidOperationState);
        };
        self.txn_id = None;
        self.emit_pending_error()
    }
}

impl Operation for AbortMultipartUploadOperation {
    type Output = Option<Result<(), AbortMultipartUploadError>>;
    type Error = AbortMultipartUploadError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            AbortMultipartUploadState::Init => self.handle_init(),
            AbortMultipartUploadState::StartMarkTransaction => {
                self.handle_mark_transaction_started(event)
            }
            AbortMultipartUploadState::ReadUploadForMark => self.handle_upload_read_for_mark(event),
            AbortMultipartUploadState::WriteUploadAborting => self.handle_upload_marked(event),
            AbortMultipartUploadState::CommitMarkTransaction => self.handle_mark_committed(event),
            AbortMultipartUploadState::ReadUploadParts => self.handle_upload_parts_read(event),
            AbortMultipartUploadState::StartDeleteTransaction => {
                self.handle_delete_transaction_started(event)
            }
            AbortMultipartUploadState::DeleteUploadRecords => {
                self.handle_upload_records_deleted(event)
            }
            AbortMultipartUploadState::CommitDeleteTransaction => {
                self.handle_delete_committed(event)
            }
            AbortMultipartUploadState::CleanupPartBlobs => self.handle_cleanup_part_blob(event),
            AbortMultipartUploadState::ResetUploadTransaction => {
                self.handle_reset_transaction_started(event)
            }
            AbortMultipartUploadState::ReadUploadForReset => {
                self.handle_upload_read_for_reset(event)
            }
            AbortMultipartUploadState::WriteUploadReset => self.handle_upload_reset_written(event),
            AbortMultipartUploadState::CommitResetTransaction => self.handle_reset_committed(event),
            AbortMultipartUploadState::Finish => smallvec![],
            AbortMultipartUploadState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            AbortMultipartUploadState::Finish | AbortMultipartUploadState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == AbortMultipartUploadState::Error {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(AbortMultipartUploadError::AbortMultipartUploadFailed);
        }

        Ok(self.output)
    }

    fn abort(&mut self) -> Effects {
        self.txn_id.map_or_else(smallvec::SmallVec::new, |txn_id| {
            smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
        })
    }
}
