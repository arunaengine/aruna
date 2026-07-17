use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{S3_MULTIPART_UPLOAD_KEYSPACE, S3_MULTIPART_UPLOAD_PART_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::checksum::ExpectedChecksum;
use aruna_core::structs::{
    BackendLocation, MultipartUpload, MultipartUploadPart, MultipartUploadPartKey,
    MultipartUploadStatus,
};
use aruna_core::types::{Effects, TxnId, UserId};
use bytes::Bytes;
use smallvec::smallvec;
use std::time::SystemTime;
use thiserror::Error;
use ulid::Ulid;

#[derive(Debug, Eq, PartialEq)]
pub enum UploadPartState {
    Init,
    ReadUpload,
    WritePart,
    CleanupFailedWrite,
    StartTransaction,
    ReReadUpload,
    ReadExistingPart,
    WritePartRecord,
    CommitTransaction,
    CleanupReplacedPart,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum UploadPartError {
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
    #[error("request body missing")]
    MissingBody,
    #[error("body size did not match Content-Length header")]
    IncompleteBody,
    #[error("missing stored checksum for {0}")]
    MissingExpectedChecksum(&'static str),
    #[error("checksum mismatch for {0}")]
    ChecksumMismatch(&'static str),
    #[error("blob write failed: {0}")]
    WriteFailed(String),
    #[error("UploadPart failed")]
    UploadPartFailed,
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
    pub compressed: bool,
    pub encrypted: bool,
    pub expected_checksums: Vec<ExpectedChecksum>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UploadPartResult {
    pub location: BackendLocation,
}

#[derive(Debug, PartialEq)]
pub struct UploadPartOperation {
    state: UploadPartState,
    input: UploadPartInput,
    txn_id: Option<TxnId>,
    written_location: Option<BackendLocation>,
    replaced_location: Option<BackendLocation>,
    pending_error: Option<UploadPartError>,
    output: Option<Result<UploadPartResult, UploadPartError>>,
}

impl UploadPartOperation {
    pub fn new(input: UploadPartInput) -> Self {
        Self {
            state: UploadPartState::Init,
            input,
            txn_id: None,
            written_location: None,
            replaced_location: None,
            pending_error: None,
            output: None,
        }
    }

    fn emit_error(&mut self, error: UploadPartError) -> Effects {
        self.state = UploadPartState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn handle_init(&mut self) -> Effects {
        self.state = UploadPartState::ReadUpload;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_MULTIPART_UPLOAD_KEYSPACE.to_string(),
            key: self.input.upload_id.to_bytes().to_vec().into(),
            txn_id: None,
        })]
    }

    fn validate_upload_record(&self, record: &MultipartUpload) -> Result<(), UploadPartError> {
        if record.bucket != self.input.bucket || record.key != self.input.key {
            return Err(UploadPartError::UploadTargetMismatch);
        }
        if record.status != MultipartUploadStatus::Open {
            return Err(UploadPartError::UploadNotOpen);
        }
        Ok(())
    }

    fn handle_upload_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(UploadPartError::InvalidOperationState);
        };

        let Some(value) = value else {
            return self.emit_error(UploadPartError::NoSuchUpload);
        };
        let record = match MultipartUpload::from_bytes(value.as_ref()) {
            Ok(record) => record,
            Err(err) => return self.emit_error(err.into()),
        };
        if let Err(err) = self.validate_upload_record(&record) {
            return self.emit_error(err);
        }

        let Some(blob) = self.input.body.take() else {
            return self.emit_error(UploadPartError::MissingBody);
        };
        self.state = UploadPartState::WritePart;
        smallvec![Effect::Blob(BlobEffect::WritePart {
            upload_id: self.input.upload_id,
            part_number: self.input.part_number,
            created_by: self.input.created_by,
            compressed: self.input.compressed,
            encrypted: self.input.encrypted,
            blob,
        })]
    }

    fn handle_write_finished(&mut self, event: Event) -> Effects {
        let location = match event {
            Event::Blob(BlobEvent::WriteFinished { location }) => location,
            // A failed body stream (e.g. trailer checksum mismatch) must map
            // to a client error, not an invalid-state 500.
            Event::Blob(BlobEvent::Error(error)) => {
                return self.cleanup_failed_write(UploadPartError::WriteFailed(error.to_string()));
            }
            _ => return self.emit_error(UploadPartError::InvalidOperationState),
        };
        self.written_location = Some(location.clone());

        if self
            .input
            .content_length
            .is_some_and(|expected| location.blob_size != expected)
        {
            return self.cleanup_failed_write(UploadPartError::IncompleteBody);
        }

        for expected in &self.input.expected_checksums {
            let Some(actual) = location.hashes.get(expected.algorithm.hash_key()) else {
                return self.cleanup_failed_write(UploadPartError::MissingExpectedChecksum(
                    expected.algorithm.s3_name(),
                ));
            };
            if actual != &expected.digest {
                return self.cleanup_failed_write(UploadPartError::ChecksumMismatch(
                    expected.algorithm.s3_name(),
                ));
            }
        }

        self.state = UploadPartState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn cleanup_failed_write(&mut self, error: UploadPartError) -> Effects {
        self.pending_error = Some(error);
        self.state = UploadPartState::CleanupFailedWrite;
        if let Some(location) = self.written_location.clone() {
            smallvec![Effect::Blob(BlobEffect::Delete { location })]
        } else {
            self.emit_pending_error()
        }
    }

    fn handle_failed_write_cleanup(&mut self, event: Event) -> Effects {
        match event {
            Event::Blob(BlobEvent::DeleteFinished) | Event::Blob(BlobEvent::Error(_)) => {
                self.emit_pending_error()
            }
            _ => self.emit_error(UploadPartError::InvalidOperationState),
        }
    }

    fn emit_pending_error(&mut self) -> Effects {
        let Some(error) = self.pending_error.take() else {
            return self.emit_error(UploadPartError::UploadPartFailed);
        };
        self.emit_error(error)
    }

    fn handle_transaction_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.emit_error(UploadPartError::InvalidOperationState);
        };

        self.txn_id = Some(txn_id);
        self.state = UploadPartState::ReReadUpload;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_MULTIPART_UPLOAD_KEYSPACE.to_string(),
            key: self.input.upload_id.to_bytes().to_vec().into(),
            txn_id: Some(txn_id),
        })]
    }

    fn handle_upload_reread(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(UploadPartError::InvalidOperationState);
        };

        let Some(value) = value else {
            return self.cleanup_failed_write(UploadPartError::NoSuchUpload);
        };
        let record = match MultipartUpload::from_bytes(value.as_ref()) {
            Ok(record) => record,
            Err(err) => return self.cleanup_failed_write(err.into()),
        };
        if let Err(err) = self.validate_upload_record(&record) {
            return self.cleanup_failed_write(err);
        }

        self.state = UploadPartState::ReadExistingPart;
        let key = match MultipartUploadPartKey::new(self.input.upload_id, self.input.part_number)
            .to_bytes()
        {
            Ok(key) => key,
            Err(err) => return self.cleanup_failed_write(err.into()),
        };
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_MULTIPART_UPLOAD_PART_KEYSPACE.to_string(),
            key: key.into(),
            txn_id: self.txn_id,
        })]
    }

    fn handle_existing_part_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(UploadPartError::InvalidOperationState);
        };

        if let Some(value) = value {
            let existing = match MultipartUploadPart::from_bytes(value.as_ref()) {
                Ok(existing) => existing,
                Err(err) => return self.cleanup_failed_write(err.into()),
            };
            self.replaced_location = Some(existing.location);
        }

        let Some(location) = self.written_location.clone() else {
            return self.emit_error(UploadPartError::UploadPartFailed);
        };
        let record = MultipartUploadPart {
            part_number: self.input.part_number,
            location,
            created_at: SystemTime::now(),
        };
        let key = match MultipartUploadPartKey::new(self.input.upload_id, self.input.part_number)
            .to_bytes()
        {
            Ok(key) => key,
            Err(err) => return self.cleanup_failed_write(err.into()),
        };
        let value = match record.to_bytes() {
            Ok(value) => value,
            Err(err) => return self.cleanup_failed_write(err.into()),
        };

        self.state = UploadPartState::WritePartRecord;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: S3_MULTIPART_UPLOAD_PART_KEYSPACE.to_string(),
            key: key.into(),
            value: value.into(),
            txn_id: self.txn_id,
        })]
    }

    fn handle_part_record_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.emit_error(UploadPartError::InvalidOperationState);
        };
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(UploadPartError::NoTransactionFound);
        };

        self.state = UploadPartState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_transaction_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.emit_error(UploadPartError::InvalidOperationState);
        };
        self.txn_id = None;

        if let Some(location) = self.replaced_location.take() {
            self.state = UploadPartState::CleanupReplacedPart;
            smallvec![Effect::Blob(BlobEffect::Delete { location })]
        } else {
            self.state = UploadPartState::Finish;
            let Some(location) = self.written_location.clone() else {
                return self.emit_error(UploadPartError::UploadPartFailed);
            };
            self.output = Some(Ok(UploadPartResult { location }));
            smallvec![]
        }
    }

    fn handle_replaced_part_cleanup(&mut self, event: Event) -> Effects {
        match event {
            Event::Blob(BlobEvent::DeleteFinished) | Event::Blob(BlobEvent::Error(_)) => {
                self.state = UploadPartState::Finish;
                let Some(location) = self.written_location.clone() else {
                    return self.emit_error(UploadPartError::UploadPartFailed);
                };
                self.output = Some(Ok(UploadPartResult { location }));
                smallvec![]
            }
            _ => self.emit_error(UploadPartError::InvalidOperationState),
        }
    }
}

impl Operation for UploadPartOperation {
    type Output = Option<Result<UploadPartResult, UploadPartError>>;
    type Error = UploadPartError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            UploadPartState::Init => self.handle_init(),
            UploadPartState::ReadUpload => self.handle_upload_read(event),
            UploadPartState::WritePart => self.handle_write_finished(event),
            UploadPartState::CleanupFailedWrite => self.handle_failed_write_cleanup(event),
            UploadPartState::StartTransaction => self.handle_transaction_started(event),
            UploadPartState::ReReadUpload => self.handle_upload_reread(event),
            UploadPartState::ReadExistingPart => self.handle_existing_part_read(event),
            UploadPartState::WritePartRecord => self.handle_part_record_written(event),
            UploadPartState::CommitTransaction => self.handle_transaction_committed(event),
            UploadPartState::CleanupReplacedPart => self.handle_replaced_part_cleanup(event),
            UploadPartState::Finish => smallvec![],
            UploadPartState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, UploadPartState::Finish | UploadPartState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == UploadPartState::Error {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(UploadPartError::UploadPartFailed);
        }

        Ok(self.output)
    }

    fn abort(&mut self) -> Effects {
        let mut effects = smallvec![];
        if let Some(location) = self.written_location.take() {
            effects.push(Effect::Blob(BlobEffect::Delete { location }));
        }
        if let Some(txn_id) = self.txn_id.take() {
            effects.push(Effect::Storage(StorageEffect::AbortTransaction { txn_id }));
        }
        effects
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use aruna_core::structs::RealmId;
    use aruna_storage::storage;
    use tempfile::tempdir;

    fn test_user_id() -> UserId {
        UserId::local(Ulid::r#gen(), RealmId::from_bytes([1u8; 32]))
    }

    #[test]
    fn rejects_write_error() {
        // A rejected body stream (e.g. trailer checksum mismatch) must
        // surface WriteFailed instead of InvalidOperationState.
        let mut op = UploadPartOperation::new(UploadPartInput {
            bucket: "mybucket".to_string(),
            key: "object.txt".to_string(),
            upload_id: Ulid::r#gen(),
            part_number: 1,
            content_length: None,
            body: None,
            created_by: test_user_id(),
            compressed: false,
            encrypted: false,
            expected_checksums: Vec::new(),
        });
        op.state = UploadPartState::WritePart;

        let effects = op.step(Event::Blob(BlobEvent::Error(
            aruna_core::errors::BlobError::WriteError("checksum mismatch".to_string()),
        )));

        assert!(effects.is_empty());
        assert!(op.is_complete());
        assert!(matches!(
            op.finalize(),
            Err(UploadPartError::WriteFailed(_))
        ));
    }

    #[tokio::test]
    async fn drive_upload_part_missing_upload_returns_no_such_upload() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let result = drive(
            UploadPartOperation::new(UploadPartInput {
                bucket: "mybucket".to_string(),
                key: "object.txt".to_string(),
                upload_id: Ulid::r#gen(),
                part_number: 1,
                content_length: None,
                body: None,
                created_by: test_user_id(),
                compressed: false,
                encrypted: false,
                expected_checksums: Vec::new(),
            }),
            &context,
        )
        .await;

        assert!(matches!(result, Err(UploadPartError::NoSuchUpload)));
    }
}
