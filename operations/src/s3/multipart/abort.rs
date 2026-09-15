use crate::s3::multipart::target::{StatusCheck, UploadTargetError, validate_upload};
use crate::s3::write_cleanup::{WriteCleanup, delete_records_effect};
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{BLOB_CLEANUP_KEYSPACE, UPLOAD_KEYSPACE, UPLOAD_PART_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::blob::BlobCleanupWork;
use aruna_core::structs::storage::multipart::{
    MultipartPart, MultipartPartKey, MultipartUpload, MultipartUploadStatus,
};
use aruna_core::types::{Effects, TxnId, Value};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

#[derive(Debug, Eq, PartialEq)]
pub enum AbortUploadState {
    Init,
    StartMarkTransaction,
    ReadUploadMark,
    WriteUploadAborting,
    CommitMarkTransaction,
    ReadUploadParts,
    StartDeleteTransaction,
    DeleteUploadRecords,
    WriteCleanupRecords,
    CommitDeleteTransaction,
    CleanupPartBlobs,
    ResetUploadTransaction,
    ReadUploadReset,
    WriteUploadReset,
    CommitResetTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum AbortUploadError {
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
    AbortUploadFailed,
    #[error("operation did not finish")]
    NotFinished,
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

#[derive(Debug, PartialEq)]
pub struct AbortUploadInput {
    pub bucket: String,
    pub key: String,
    pub upload_id: Ulid,
    /// Wall clock (epoch ms) an in-progress completion lease is judged against.
    pub now_ms: u64,
}

#[derive(Debug, PartialEq)]
pub struct AbortUploadOperation {
    input: AbortUploadInput,
    state: AbortUploadState,
    txn_id: Option<TxnId>,
    upload_record: Option<MultipartUpload>,
    upload_parts: Vec<MultipartPart>,
    cleanup_index: usize,
    skip_status_check: bool,
    cleanup: WriteCleanup<AbortUploadError>,
    output: Option<Result<(), AbortUploadError>>,
}

impl AbortUploadOperation {
    pub fn new(input: AbortUploadInput) -> Self {
        Self {
            input,
            state: AbortUploadState::Init,
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
    fn emit_error(&mut self, error: AbortUploadError) -> Effects {
        self.state = AbortUploadState::Error;
        self.output = Some(Err(error));
        self.abort()
    }

    fn schedule_error(&mut self, error: AbortUploadError) -> Effects {
        self.cleanup.set_error(error);
        if self.upload_record.is_some() {
            self.state = AbortUploadState::ResetUploadTransaction;
            smallvec![Effect::Storage(StorageEffect::StartTransaction {
                read: false,
            })]
        } else {
            self.emit_pending_error()
        }
    }

    fn emit_pending_error(&mut self) -> Effects {
        let Some(error) = self.cleanup.take_error() else {
            return self.emit_error(AbortUploadError::AbortUploadFailed);
        };
        self.emit_error(error)
    }

    fn handle_init(&mut self) -> Effects {
        self.state = AbortUploadState::StartMarkTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn mark_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.emit_error(AbortUploadError::InvalidOperationState);
        };
        self.txn_id = Some(txn_id);
        self.state = AbortUploadState::ReadUploadMark;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: UPLOAD_KEYSPACE.to_string(),
            key: self.input.upload_id.to_bytes().to_vec().into(),
            txn_id: Some(txn_id),
        })]
    }

    fn mark_upload_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(AbortUploadError::InvalidOperationState);
        };
        let Some(value) = value else {
            return self.emit_error(AbortUploadError::NoSuchUpload);
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
        self.state = AbortUploadState::WriteUploadAborting;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: UPLOAD_KEYSPACE.to_string(),
            key: self.input.upload_id.to_bytes().to_vec().into(),
            value: value.into(),
            txn_id: self.txn_id,
        })]
    }

    fn handle_upload_marked(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.emit_error(AbortUploadError::InvalidOperationState);
        };
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(AbortUploadError::NoTransactionFound);
        };

        self.state = AbortUploadState::CommitMarkTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_mark_committed(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                self.txn_id = None;

                let prefix = match MultipartPartKey::prefix(self.input.upload_id) {
                    Ok(prefix) => prefix,
                    Err(err) => return self.schedule_error(err.into()),
                };
                self.state = AbortUploadState::ReadUploadParts;
                smallvec![Effect::Storage(StorageEffect::Iter {
                    key_space: UPLOAD_PART_KEYSPACE.to_string(),
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
            _ => self.emit_error(AbortUploadError::InvalidOperationState),
        }
    }

    fn upload_parts_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult { values, .. }) = event else {
            return self.schedule_error(AbortUploadError::InvalidOperationState);
        };

        self.upload_parts = values
            .into_iter()
            .filter_map(|(_, value)| MultipartPart::from_bytes(value.as_ref()).ok())
            .collect();
        self.state = AbortUploadState::StartDeleteTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn delete_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.schedule_error(AbortUploadError::InvalidOperationState);
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
        self.state = AbortUploadState::DeleteUploadRecords;
        smallvec![effect]
    }

    fn records_deleted(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::BatchDeleteResult { .. }) => self.write_cleanup_records(),
            Event::Storage(StorageEvent::Error { error }) => self.schedule_error(error.into()),
            _ => self.schedule_error(AbortUploadError::InvalidOperationState),
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

        self.state = AbortUploadState::WriteCleanupRecords;
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: self.txn_id,
        })]
    }

    fn handle_cleanup_written(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::BatchWriteResult { .. }) => {
                let Some(txn_id) = self.txn_id else {
                    return self.schedule_error(AbortUploadError::NoTransactionFound);
                };
                self.state = AbortUploadState::CommitDeleteTransaction;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            Event::Storage(StorageEvent::Error { error }) => self.schedule_error(error.into()),
            _ => self.schedule_error(AbortUploadError::InvalidOperationState),
        }
    }

    fn handle_delete_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.schedule_error(AbortUploadError::InvalidOperationState);
        };
        self.txn_id = None;
        self.upload_record = None;
        self.cleanup_index = 0;
        self.state = AbortUploadState::CleanupPartBlobs;
        self.next_blob()
    }

    fn next_blob(&mut self) -> Effects {
        let Some(part) = self.upload_parts.get(self.cleanup_index) else {
            self.state = AbortUploadState::Finish;
            self.output = Some(Ok(()));
            return smallvec![];
        };

        self.state = AbortUploadState::CleanupPartBlobs;
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
            _ => self.emit_error(AbortUploadError::InvalidOperationState),
        }
    }

    fn reset_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.emit_error(AbortUploadError::InvalidOperationState);
        };
        self.txn_id = Some(txn_id);
        self.state = AbortUploadState::ReadUploadReset;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: UPLOAD_KEYSPACE.to_string(),
            key: self.input.upload_id.to_bytes().to_vec().into(),
            txn_id: Some(txn_id),
        })]
    }

    fn reset_upload_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(AbortUploadError::InvalidOperationState);
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
        self.state = AbortUploadState::WriteUploadReset;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: UPLOAD_KEYSPACE.to_string(),
            key: self.input.upload_id.to_bytes().to_vec().into(),
            value,
            txn_id: self.txn_id,
        })]
    }

    fn upload_reset(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.emit_error(AbortUploadError::InvalidOperationState);
        };
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(AbortUploadError::NoTransactionFound);
        };
        self.state = AbortUploadState::CommitResetTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_reset_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.emit_error(AbortUploadError::InvalidOperationState);
        };
        self.txn_id = None;
        self.emit_pending_error()
    }
}

impl Operation for AbortUploadOperation {
    type Output = ();
    type Error = AbortUploadError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            AbortUploadState::Init => self.handle_init(),
            AbortUploadState::StartMarkTransaction => self.mark_started(event),
            AbortUploadState::ReadUploadMark => self.mark_upload_read(event),
            AbortUploadState::WriteUploadAborting => self.handle_upload_marked(event),
            AbortUploadState::CommitMarkTransaction => self.handle_mark_committed(event),
            AbortUploadState::ReadUploadParts => self.upload_parts_read(event),
            AbortUploadState::StartDeleteTransaction => self.delete_started(event),
            AbortUploadState::DeleteUploadRecords => self.records_deleted(event),
            AbortUploadState::WriteCleanupRecords => self.handle_cleanup_written(event),
            AbortUploadState::CommitDeleteTransaction => self.handle_delete_committed(event),
            AbortUploadState::CleanupPartBlobs => self.blob_cleaned(event),
            AbortUploadState::ResetUploadTransaction => self.reset_started(event),
            AbortUploadState::ReadUploadReset => self.reset_upload_read(event),
            AbortUploadState::WriteUploadReset => self.upload_reset(event),
            AbortUploadState::CommitResetTransaction => self.handle_reset_committed(event),
            AbortUploadState::Finish => smallvec![],
            AbortUploadState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            AbortUploadState::Finish | AbortUploadState::Error
        )
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

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.output {
            Some(Ok(value)) => Ok(value),
            Some(Err(error)) => Err(error),
            None => Err(AbortUploadError::NotFinished),
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
mod pure_tests {
    use super::*;
    use aruna_core::keyspaces::BLOB_CLEANUP_KEYSPACE;
    use aruna_core::structs::storage::blob::{BackendLocation, BackendRef};

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

    fn marked() -> AbortUploadOperation {
        let input = input();
        let mut operation = AbortUploadOperation::new(input);
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
        operation.state = AbortUploadState::CommitMarkTransaction;
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
        assert_eq!(operation.state, AbortUploadState::ResetUploadTransaction);
        assert_eq!(operation.txn_id, None);
        assert_eq!(
            operation.cleanup.take_error(),
            Some(AbortUploadError::StorageError(StorageError::CommitFailed))
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
        assert_eq!(operation.state, AbortUploadState::Error);
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
                if key_space == UPLOAD_PART_KEYSPACE
        ));
        assert_eq!(operation.state, AbortUploadState::ReadUploadParts);
        assert_eq!(operation.txn_id, None);
    }

    #[test]
    fn queues_cleanup_commit() {
        let location = part_location();
        let mut operation = AbortUploadOperation::new(input());
        operation.upload_parts.push(MultipartPart {
            part_number: 1,
            location: location.clone(),
            created_at: SystemTime::UNIX_EPOCH,
        });
        let txn_id = TxnId::from_bytes([5u8; 16]);
        operation.txn_id = Some(txn_id);
        operation.state = AbortUploadState::DeleteUploadRecords;

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
        assert_eq!(operation.state, AbortUploadState::WriteCleanupRecords);

        let effects = operation.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));
        assert_eq!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
        );
        assert_eq!(operation.state, AbortUploadState::CommitDeleteTransaction);
    }

    #[test]
    fn delete_batches_records() {
        // The abort removes every part row and the upload record in one batch
        // inside the delete transaction.
        let mut operation = AbortUploadOperation::new(input());
        operation.upload_parts.push(MultipartPart {
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
            key_space == UPLOAD_KEYSPACE && key.as_ref() == upload_key.as_slice()
        }));
        let part_key = MultipartPartKey::new(operation.input.upload_id, 1)
            .to_bytes()
            .unwrap();
        assert!(deletes.iter().any(|(key_space, key)| {
            key_space == UPLOAD_PART_KEYSPACE && key.as_ref() == part_key.as_slice()
        }));
        assert_eq!(operation.state, AbortUploadState::DeleteUploadRecords);
    }

    #[test]
    fn rejects_incomplete() {
        let operation = AbortUploadOperation::new(input());

        assert_eq!(operation.finalize(), Err(AbortUploadError::NotFinished));
    }

    #[test]
    fn finalizes_finished() {
        let mut operation = AbortUploadOperation::new(input());
        operation.state = AbortUploadState::Finish;
        operation.output = Some(Ok(()));

        assert_eq!(operation.finalize(), Ok(()));
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
