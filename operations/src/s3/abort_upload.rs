use crate::s3::upload_target::{StatusCheck, UploadTargetError, validate_upload};
use crate::s3::write_cleanup::{WriteCleanup, delete_records_effect};
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_CLEANUP_KEYSPACE, S3_MULTIPART_UPLOAD_KEYSPACE, S3_MULTIPART_UPLOAD_PART_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::{
    BlobCleanupWork, MultipartUpload, MultipartUploadPart, MultipartUploadPartKey,
    MultipartUploadStatus,
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
    WriteCleanupRecords,
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
    #[error("The upload is being completed, retry shortly.")]
    CompletionInProgress,
    #[error("AbortMultipartUpload failed")]
    AbortMultipartUploadFailed,
}

impl From<UploadTargetError> for AbortMultipartUploadError {
    fn from(error: UploadTargetError) -> Self {
        match error {
            UploadTargetError::TargetMismatch => Self::UploadTargetMismatch,
            UploadTargetError::NotOpen => Self::UploadNotOpen,
            UploadTargetError::CompletionInProgress => Self::CompletionInProgress,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct AbortMultipartUploadInput {
    pub bucket: String,
    pub key: String,
    pub upload_id: Ulid,
    /// Wall clock (epoch ms) an in-progress completion lease is judged against.
    pub now_ms: u64,
}

#[derive(Debug, PartialEq)]
pub struct AbortMultipartUploadOperation {
    input: AbortMultipartUploadInput,
    state: AbortMultipartUploadState,
    txn_id: Option<TxnId>,
    upload_record: Option<MultipartUpload>,
    upload_parts: Vec<MultipartUploadPart>,
    cleanup_index: usize,
    skip_status_check: bool,
    cleanup: WriteCleanup<AbortMultipartUploadError>,
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
            skip_status_check: false,
            cleanup: WriteCleanup::default(),
            output: None,
        }
    }

    /// A purge may skip status checks after it owns the destination write fence.
    pub fn including_in_progress(mut self) -> Self {
        self.skip_status_check = true;
        self
    }

    /// The terminal state is complete, so the driver never calls `abort` for us;
    /// releasing the transaction here is what keeps it from outliving the
    /// operation. `abort` takes the id, so it cannot run twice.
    fn emit_error(&mut self, error: AbortMultipartUploadError) -> Effects {
        self.state = AbortMultipartUploadState::Error;
        self.output = Some(Err(error));
        self.abort()
    }

    fn schedule_error(&mut self, error: AbortMultipartUploadError) -> Effects {
        self.cleanup.set_error(error);
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
        let Some(error) = self.cleanup.take_error() else {
            return self.emit_error(AbortMultipartUploadError::AbortMultipartUploadFailed);
        };
        self.emit_error(error)
    }

    fn handle_init(&mut self) -> Effects {
        self.state = AbortMultipartUploadState::StartMarkTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn mark_started(&mut self, event: Event) -> Effects {
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

    fn mark_upload_read(&mut self, event: Event) -> Effects {
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
        if let Err(err) = validate_upload(
            &record,
            &self.input.bucket,
            &self.input.key,
            if self.skip_status_check {
                StatusCheck::Skip
            } else {
                StatusCheck::Takeover {
                    now_ms: self.input.now_ms,
                }
            },
        ) {
            return self.emit_error(err.into());
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
        match event {
            Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                self.txn_id = None;

                let prefix = match MultipartUploadPartKey::prefix(self.input.upload_id) {
                    Ok(prefix) => prefix,
                    Err(err) => return self.schedule_error(err.into()),
                };
                self.state = AbortMultipartUploadState::ReadUploadParts;
                smallvec![Effect::Storage(StorageEffect::Iter {
                    key_space: S3_MULTIPART_UPLOAD_PART_KEYSPACE.to_string(),
                    prefix: Some(prefix.into()),
                    start: None,
                    limit: 10_000,
                    txn_id: None,
                })]
            }
            Event::Storage(StorageEvent::Error { error }) if error.proves_no_commit() => {
                self.emit_error(error.into())
            }
            Event::Storage(StorageEvent::Error { error }) => {
                self.txn_id = None;
                self.schedule_error(error.into())
            }
            _ => self.emit_error(AbortMultipartUploadError::InvalidOperationState),
        }
    }

    fn upload_parts_read(&mut self, event: Event) -> Effects {
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

    fn delete_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.schedule_error(AbortMultipartUploadError::InvalidOperationState);
        };
        self.txn_id = Some(txn_id);
        self.delete_upload_records()
    }

    fn delete_upload_records(&mut self) -> Effects {
        let effect =
            match delete_records_effect(self.input.upload_id, &self.upload_parts, self.txn_id) {
                Ok(effect) => effect,
                Err(err) => return self.schedule_error(err.into()),
            };
        self.state = AbortMultipartUploadState::DeleteUploadRecords;
        smallvec![effect]
    }

    fn records_deleted(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::BatchDeleteResult { .. }) => self.write_cleanup_records(),
            Event::Storage(StorageEvent::Error { error }) => self.schedule_error(error.into()),
            _ => self.schedule_error(AbortMultipartUploadError::InvalidOperationState),
        }
    }

    fn write_cleanup_records(&mut self) -> Effects {
        let mut writes = Vec::with_capacity(self.upload_parts.len());
        for part in &self.upload_parts {
            let work = BlobCleanupWork::DeleteBlob {
                location: part.location.clone(),
            };
            let value = match work.to_bytes() {
                Ok(value) => value,
                Err(err) => return self.schedule_error(err.into()),
            };
            writes.push((
                BLOB_CLEANUP_KEYSPACE.to_string(),
                Ulid::generate().to_bytes().to_vec().into(),
                value.into(),
            ));
        }

        self.state = AbortMultipartUploadState::WriteCleanupRecords;
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: self.txn_id,
        })]
    }

    fn handle_cleanup_written(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::BatchWriteResult { .. }) => {
                let Some(txn_id) = self.txn_id else {
                    return self.schedule_error(AbortMultipartUploadError::NoTransactionFound);
                };
                self.state = AbortMultipartUploadState::CommitDeleteTransaction;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            Event::Storage(StorageEvent::Error { error }) => self.schedule_error(error.into()),
            _ => self.schedule_error(AbortMultipartUploadError::InvalidOperationState),
        }
    }

    fn handle_delete_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.schedule_error(AbortMultipartUploadError::InvalidOperationState);
        };
        self.txn_id = None;
        self.upload_record = None;
        self.cleanup_index = 0;
        self.state = AbortMultipartUploadState::CleanupPartBlobs;
        self.next_blob()
    }

    fn next_blob(&mut self) -> Effects {
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

    fn blob_cleaned(&mut self, event: Event) -> Effects {
        match event {
            Event::Blob(BlobEvent::DeleteFinished) | Event::Blob(BlobEvent::Error(_)) => {
                self.cleanup_index += 1;
                self.next_blob()
            }
            _ => self.emit_error(AbortMultipartUploadError::InvalidOperationState),
        }
    }

    fn reset_started(&mut self, event: Event) -> Effects {
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

    fn reset_upload_read(&mut self, event: Event) -> Effects {
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

    fn upload_reset(&mut self, event: Event) -> Effects {
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
                self.mark_started(event)
            }
            AbortMultipartUploadState::ReadUploadForMark => self.mark_upload_read(event),
            AbortMultipartUploadState::WriteUploadAborting => self.handle_upload_marked(event),
            AbortMultipartUploadState::CommitMarkTransaction => self.handle_mark_committed(event),
            AbortMultipartUploadState::ReadUploadParts => self.upload_parts_read(event),
            AbortMultipartUploadState::StartDeleteTransaction => {
                self.delete_started(event)
            }
            AbortMultipartUploadState::DeleteUploadRecords => {
                self.records_deleted(event)
            }
            AbortMultipartUploadState::WriteCleanupRecords => self.handle_cleanup_written(event),
            AbortMultipartUploadState::CommitDeleteTransaction => {
                self.handle_delete_committed(event)
            }
            AbortMultipartUploadState::CleanupPartBlobs => self.blob_cleaned(event),
            AbortMultipartUploadState::ResetUploadTransaction => {
                self.reset_started(event)
            }
            AbortMultipartUploadState::ReadUploadForReset => {
                self.reset_upload_read(event)
            }
            AbortMultipartUploadState::WriteUploadReset => self.upload_reset(event),
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

    fn expected_error(error: &Self::Error) -> bool {
        matches!(
            error,
            AbortMultipartUploadError::NoSuchUpload
                | AbortMultipartUploadError::UploadTargetMismatch
                | AbortMultipartUploadError::UploadNotOpen
                | AbortMultipartUploadError::CompletionInProgress
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.state {
            AbortMultipartUploadState::Finish => Ok(self.output),
            AbortMultipartUploadState::Error => {
                if let Some(Err(error)) = self.output {
                    return Err(error);
                }
                Err(AbortMultipartUploadError::AbortMultipartUploadFailed)
            }
            _ => Err(AbortMultipartUploadError::InvalidOperationState),
        }
    }

    fn abort(&mut self) -> Effects {
        self.txn_id
            .take()
            .map_or_else(smallvec::SmallVec::new, |txn_id| {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::keyspaces::BLOB_CLEANUP_KEYSPACE;
    use aruna_core::structs::{BackendLocation, BackendRef};

    use std::collections::HashMap;
    use std::time::SystemTime;

    fn input() -> AbortMultipartUploadInput {
        AbortMultipartUploadInput {
            bucket: "bucket".to_string(),
            key: "object".to_string(),
            upload_id: Ulid::from_bytes([1u8; 16]),
            now_ms: 1_000,
        }
    }

    fn marked() -> AbortMultipartUploadOperation {
        let input = input();
        let mut operation = AbortMultipartUploadOperation::new(input);
        operation.upload_record = Some(MultipartUpload {
            upload_id: operation.input.upload_id,
            backend: BackendRef::node_default(),
            storage_class: None,
            bucket: operation.input.bucket.clone(),
            key: operation.input.key.clone(),
            group_id: Ulid::from_bytes([2u8; 16]),
            created_by: Default::default(),
            created_at: SystemTime::UNIX_EPOCH,
            status: MultipartUploadStatus::Aborting,
            checksum_hint: None,
            metadata: HashMap::new(),
            placement_policies: Vec::new(),
            subject_generation: 0,
            completing_since_ms: None,
        });
        operation.txn_id = Some(TxnId::from_bytes([3u8; 16]));
        operation.state = AbortMultipartUploadState::CommitMarkTransaction;
        operation
    }

    #[test]
    fn unknown_mark_resets() {
        let mut operation = marked();

        let effects = operation.step(Event::Storage(StorageEvent::Error {
            error: StorageError::CommitFailed,
        }));

        assert_eq!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::StartTransaction {
                read: false
            })]
        );
        assert_eq!(
            operation.state,
            AbortMultipartUploadState::ResetUploadTransaction
        );
        assert_eq!(operation.txn_id, None);
        assert_eq!(
            operation.cleanup.take_error(),
            Some(AbortMultipartUploadError::StorageError(
                StorageError::CommitFailed
            ))
        );
    }

    #[test]
    fn conflict_mark_aborts() {
        let mut operation = marked();
        let txn_id = operation.txn_id.unwrap();

        let effects = operation.step(Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionConflict,
        }));

        assert_eq!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
        );
        assert_eq!(operation.state, AbortMultipartUploadState::Error);
        assert_eq!(operation.txn_id, None);
    }

    #[test]
    fn committed_mark_continues() {
        let mut operation = marked();

        let effects = operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id: TxnId::from_bytes([3u8; 16]),
        }));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Iter { key_space, .. })]
                if key_space == S3_MULTIPART_UPLOAD_PART_KEYSPACE
        ));
        assert_eq!(operation.state, AbortMultipartUploadState::ReadUploadParts);
        assert_eq!(operation.txn_id, None);
    }

    #[test]
    fn queues_cleanup_commit() {
        let location = part_location();
        let mut operation = AbortMultipartUploadOperation::new(input());
        operation.upload_parts.push(MultipartUploadPart {
            part_number: 1,
            location: location.clone(),
            created_at: SystemTime::UNIX_EPOCH,
        });
        let txn_id = TxnId::from_bytes([5u8; 16]);
        operation.txn_id = Some(txn_id);
        operation.state = AbortMultipartUploadState::DeleteUploadRecords;

        let effects = operation.step(Event::Storage(StorageEvent::BatchDeleteResult {
            entries: Vec::new(),
        }));
        let [Effect::Storage(StorageEffect::BatchWrite { writes, txn_id: id })] =
            effects.as_slice()
        else {
            panic!("expected cleanup records, got {effects:?}")
        };
        assert_eq!(*id, Some(txn_id));
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].0, BLOB_CLEANUP_KEYSPACE);
        assert_eq!(
            BlobCleanupWork::from_bytes(writes[0].2.as_ref()).unwrap(),
            BlobCleanupWork::DeleteBlob { location }
        );
        assert_eq!(
            operation.state,
            AbortMultipartUploadState::WriteCleanupRecords
        );

        let effects = operation.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));
        assert_eq!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
        );
        assert_eq!(
            operation.state,
            AbortMultipartUploadState::CommitDeleteTransaction
        );
    }

    #[test]
    fn delete_batches_records() {
        // The abort removes every part row and the upload record in one batch
        // inside the delete transaction.
        let mut operation = AbortMultipartUploadOperation::new(input());
        operation.upload_parts.push(MultipartUploadPart {
            part_number: 1,
            location: part_location(),
            created_at: SystemTime::UNIX_EPOCH,
        });
        let txn_id = TxnId::from_bytes([5u8; 16]);
        operation.txn_id = Some(txn_id);

        let effects = operation.delete_upload_records();

        let [
            Effect::Storage(StorageEffect::BatchDelete {
                deletes,
                txn_id: observed,
            }),
        ] = effects.as_slice()
        else {
            panic!("expected upload records delete, got {effects:?}")
        };
        assert_eq!(*observed, Some(txn_id));
        assert_eq!(deletes.len(), 2);
        let upload_key = operation.input.upload_id.to_bytes().to_vec();
        assert!(deletes.iter().any(|(key_space, key)| {
            key_space == S3_MULTIPART_UPLOAD_KEYSPACE && key.as_ref() == upload_key.as_slice()
        }));
        let part_key = MultipartUploadPartKey::new(operation.input.upload_id, 1)
            .to_bytes()
            .unwrap();
        assert!(deletes.iter().any(|(key_space, key)| {
            key_space == S3_MULTIPART_UPLOAD_PART_KEYSPACE && key.as_ref() == part_key.as_slice()
        }));
        assert_eq!(
            operation.state,
            AbortMultipartUploadState::DeleteUploadRecords
        );
    }

    #[test]
    fn rejects_incomplete() {
        let operation = AbortMultipartUploadOperation::new(input());

        assert_eq!(
            operation.finalize(),
            Err(AbortMultipartUploadError::InvalidOperationState)
        );
    }

    #[test]
    fn finalizes_finished() {
        let mut operation = AbortMultipartUploadOperation::new(input());
        operation.state = AbortMultipartUploadState::Finish;
        operation.output = Some(Ok(()));

        assert_eq!(operation.finalize(), Ok(Some(Ok(()))));
    }

    fn part_location() -> BackendLocation {
        BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "root".to_string(),
            storage_bucket: "parts".to_string(),
            backend_path: "upload/part".to_string(),
            ulid: Ulid::from_bytes([4u8; 16]),
            compressed: false,
            encrypted: false,
            created_by: Default::default(),
            created_at: SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 4,
            hashes: HashMap::new(),
        }
    }
}
