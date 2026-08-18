use crate::blob::cleanup::PendingCleanup;
use crate::group_backends::{BackendFenceError, check_fence, fence_backend};
use crate::placement_policy::PolicyGateError;
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{
    NODE_SUBJECT_KEYSPACE, S3_MULTIPART_UPLOAD_KEYSPACE, S3_MULTIPART_UPLOAD_PART_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::checksum::ExpectedChecksum;
use aruna_core::structs::{
    BackendLocation, BlobCleanupWork, MultipartUpload, MultipartUploadPart, MultipartUploadPartKey,
    MultipartUploadStatus, NODE_SUBJECT_KEY, NodeSubjectRecord, ResolvedBackend, WriteOwner,
};
use aruna_core::types::{Effects, Key, TxnId, UserId};
use bytes::Bytes;
use smallvec::smallvec;
use std::time::SystemTime;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

#[derive(Debug, Eq, PartialEq)]
pub enum UploadPartState {
    Init,
    ReadUpload,
    WritePart,
    CleanupFailedWrite,
    QueueCleanupRow,
    WriteCleanupRow,
    StartTransaction,
    FenceBackend,
    ReReadUpload,
    ReadExistingPart,
    WritePartRecord,
    WriteReplacedRow,
    CommitTransaction,
    ReleaseReservation,
    Finish,
    Error,
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
    #[error("blob backend write failed: {0}")]
    BlobWriteFailed(String),
    #[error(transparent)]
    PolicyGateError(#[from] PolicyGateError),
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
    rollback_location: Option<BackendLocation>,
    release_id: Option<Ulid>,
    pending_cleanup: PendingCleanup,
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
            rollback_location: None,
            release_id: None,
            pending_cleanup: PendingCleanup::default(),
            pending_error: None,
            output: None,
        }
    }

    /// The terminal state is complete, so the driver never calls `abort` for us;
    /// rolling back here is what keeps an open transaction from outliving the
    /// operation. `abort` takes what it releases, so it cannot run twice.
    fn emit_error(&mut self, error: UploadPartError) -> Effects {
        self.state = UploadPartState::Error;
        self.output = Some(Err(error));
        self.abort()
    }

    /// One round trip answers both "does this upload exist" and "does the
    /// subject that admitted it still hold".
    fn handle_init(&mut self) -> Effects {
        self.state = UploadPartState::ReadUpload;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (
                    S3_MULTIPART_UPLOAD_KEYSPACE.to_string(),
                    self.input.upload_id.to_bytes().to_vec().into(),
                ),
                (
                    NODE_SUBJECT_KEYSPACE.to_string(),
                    Key::from(NODE_SUBJECT_KEY.to_vec()),
                ),
            ],
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
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.emit_error(UploadPartError::InvalidOperationState);
        };
        let mut values = values.into_iter();
        let Some((_, value)) = values.next() else {
            return self.emit_error(UploadPartError::InvalidOperationState);
        };
        let subject = match values.next() {
            Some((_, Some(value))) => match NodeSubjectRecord::from_bytes(value.as_ref()) {
                Ok(record) => Some(record),
                Err(err) => return self.emit_error(err.into()),
            },
            Some((_, None)) => None,
            None => return self.emit_error(UploadPartError::InvalidOperationState),
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
        // Cheap re-check of the create-time seal: no ref is resolved again, but
        // a subject that moved since then stops the part before any byte moves.
        if !record.admits_part(subject.as_ref()) {
            return self.emit_error(PolicyGateError::Drift.into());
        }

        let Some(blob) = self.input.body.take() else {
            return self.emit_error(UploadPartError::MissingBody);
        };
        self.state = UploadPartState::WritePart;
        smallvec![Effect::Blob(BlobEffect::WritePart {
            upload_id: self.input.upload_id,
            part_number: self.input.part_number,
            resolved: ResolvedBackend::new(record.backend, record.storage_class),
            created_by: self.input.created_by,
            compressed: self.input.compressed,
            encrypted: self.input.encrypted,
            blob,
        })]
    }

    fn handle_write_finished(&mut self, event: Event) -> Effects {
        let location = match event {
            Event::Blob(BlobEvent::WriteFinished { location }) => location,
            // Only a client-sourced stream fault may become a client error; a
            // server-side write fault must stay retryable, never a bad digest.
            Event::Blob(BlobEvent::Error(BlobError::StreamFailed(message))) => {
                return self.cleanup_failed_write(UploadPartError::WriteFailed(message));
            }
            Event::Blob(BlobEvent::Error(BlobError::WriteCleanup { location, message })) => {
                self.written_location = Some(location);
                return self.cleanup_failed_write(UploadPartError::BlobWriteFailed(message));
            }
            Event::Blob(BlobEvent::Error(error)) => {
                return self
                    .cleanup_failed_write(UploadPartError::BlobWriteFailed(error.to_string()));
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

    /// Takes the location: once its delete is queued the rollback in `abort`
    /// must not queue a second one. A copy stays behind so a delete that fails
    /// can still be handed to the durable cleanup queue.
    fn cleanup_failed_write(&mut self, error: UploadPartError) -> Effects {
        self.pending_error = Some(error);
        self.state = UploadPartState::CleanupFailedWrite;
        if let Some(location) = self.written_location.take() {
            self.rollback_location = Some(location.clone());
            smallvec![Effect::Blob(BlobEffect::Delete { location })]
        } else {
            self.emit_pending_error()
        }
    }

    fn handle_failed_write_cleanup(&mut self, event: Event) -> Effects {
        match event {
            Event::Blob(BlobEvent::DeleteFinished) => {
                self.rollback_location = None;
                self.emit_pending_error()
            }
            // The part bytes are still on the backend, and this operation is
            // over; only a queued delete can still reach them.
            Event::Blob(BlobEvent::Error(_)) => self.queue_rollback_delete(),
            _ => self.emit_error(UploadPartError::InvalidOperationState),
        }
    }

    fn queue_rollback_delete(&mut self) -> Effects {
        let Some(location) = self.rollback_location.clone() else {
            return self.emit_pending_error();
        };
        let Some(effect) = self.pending_cleanup.queue(BlobCleanupWork::DeleteBlob {
            location: location.clone(),
        }) else {
            return self.emit_pending_error();
        };
        self.rollback_location = None;
        self.state = UploadPartState::QueueCleanupRow;
        smallvec![effect]
    }

    /// Hands one row to the durable cleanup queue outside any transaction. The
    /// row keeps the location until storage accepts it, so a refused write can
    /// still be retried rather than losing the only record of the bytes.
    fn queue_cleanup_work(&mut self, work: BlobCleanupWork) -> Effects {
        let Some(effect) = self.pending_cleanup.queue(work) else {
            return self.release_or_error();
        };
        self.state = UploadPartState::QueueCleanupRow;
        smallvec![effect]
    }

    fn handle_cleanup_queued(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::WriteResult { .. }) => {
                self.pending_cleanup.accepted();
                if self.rollback_location.is_some() || self.written_location.is_some() {
                    return self.abort();
                }
                self.release_or_error()
            }
            Event::Storage(StorageEvent::Error { error }) => {
                match self.pending_cleanup.retry(&error) {
                    Some(effect) => smallvec![effect],
                    None => self.release_or_error(),
                }
            }
            _ => self.emit_error(UploadPartError::InvalidOperationState),
        }
    }

    fn emit_pending_error(&mut self) -> Effects {
        if let Some(error) = self.pending_error.take() {
            return self.emit_error(error);
        }
        if self.output.is_some() {
            self.state = UploadPartState::Error;
            return smallvec![];
        }
        self.emit_error(UploadPartError::UploadPartFailed)
    }

    fn release_or_error(&mut self) -> Effects {
        let Some(id) = self.release_id else {
            return self.emit_pending_error();
        };
        self.state = UploadPartState::ReleaseReservation;
        smallvec![Effect::Blob(BlobEffect::ReleaseReservation { id })]
    }

    /// The part is durably committed, so a refused release must not fail the
    /// request: the reconciliation row that committed with the part record
    /// clears this reservation on the next cleanup drain.
    fn defer_release(&mut self) -> Effects {
        let Some(id) = self.release_id.take() else {
            return self.emit_error(UploadPartError::InvalidOperationState);
        };
        warn!(
            event = "upload_part.release_deferred",
            release_id = %id,
            "Deferring the part reservation to reconciliation"
        );
        if matches!(self.output, Some(Ok(_))) {
            self.state = UploadPartState::Finish;
            return smallvec![];
        }
        self.emit_pending_error()
    }

    fn handle_release(&mut self, event: Event) -> Effects {
        let Event::Blob(BlobEvent::ReservationReleased { id }) = event else {
            if self.pending_error.is_none() {
                return self.defer_release();
            }
            return self.emit_error(UploadPartError::InvalidOperationState);
        };
        if self.release_id != Some(id) {
            return self.emit_error(UploadPartError::InvalidOperationState);
        }
        self.release_id = None;
        if self.pending_error.is_some() {
            self.emit_pending_error()
        } else {
            self.state = UploadPartState::Finish;
            smallvec![]
        }
    }

    fn handle_transaction_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.emit_error(UploadPartError::InvalidOperationState);
        };

        self.txn_id = Some(txn_id);
        // The pin was taken at creation, so the part write must still prove the
        // tenant backend is enabled before its record joins the transaction.
        match self
            .written_location
            .as_ref()
            .and_then(|location| fence_backend(&location.backend, self.txn_id))
        {
            Some(effect) => {
                self.state = UploadPartState::FenceBackend;
                smallvec![effect]
            }
            None => self.reread_upload(),
        }
    }

    fn handle_backend_fenced(&mut self, event: Event) -> Effects {
        match check_fence(event) {
            Ok(()) => self.reread_upload(),
            Err(error) => self.cleanup_failed_write(error.into()),
        }
    }

    fn reread_upload(&mut self) -> Effects {
        self.state = UploadPartState::ReReadUpload;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_MULTIPART_UPLOAD_KEYSPACE.to_string(),
            key: self.input.upload_id.to_bytes().to_vec().into(),
            txn_id: self.txn_id,
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
        self.write_cleanup_row()
    }

    fn write_cleanup_row(&mut self) -> Effects {
        let Some(location) = self.written_location.clone() else {
            return self.emit_error(UploadPartError::UploadPartFailed);
        };
        let key = location.ulid.to_bytes().to_vec().into();
        let work = BlobCleanupWork::ReconcileWrite {
            location,
            owner: WriteOwner::UploadPart {
                upload_id: self.input.upload_id,
                part_number: self.input.part_number,
            },
        };
        let value = match work.to_bytes() {
            Ok(value) => value,
            Err(error) => return self.emit_error(error.into()),
        };
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(UploadPartError::NoTransactionFound);
        };

        self.state = UploadPartState::WriteCleanupRow;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: aruna_core::keyspaces::BLOB_CLEANUP_KEYSPACE.to_string(),
            key,
            value: value.into(),
            txn_id: Some(txn_id),
        })]
    }

    fn handle_cleanup_row(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::WriteResult { .. }) => {
                if self.replaced_location.is_some() {
                    return self.write_replaced_row();
                }
                let Some(txn_id) = self.txn_id else {
                    return self.emit_error(UploadPartError::NoTransactionFound);
                };
                self.state = UploadPartState::CommitTransaction;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            Event::Storage(StorageEvent::Error { error }) => {
                self.cleanup_failed_write(error.into())
            }
            _ => self.emit_error(UploadPartError::InvalidOperationState),
        }
    }

    fn write_replaced_row(&mut self) -> Effects {
        let Some(location) = self.replaced_location.clone() else {
            return self.emit_error(UploadPartError::UploadPartFailed);
        };
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(UploadPartError::NoTransactionFound);
        };
        let work = BlobCleanupWork::DeleteBlob { location };
        let value = match work.to_bytes() {
            Ok(value) => value,
            Err(error) => return self.emit_error(error.into()),
        };

        self.replaced_location = None;
        self.state = UploadPartState::WriteReplacedRow;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: aruna_core::keyspaces::BLOB_CLEANUP_KEYSPACE.to_string(),
            key: Ulid::generate().to_bytes().to_vec().into(),
            value: value.into(),
            txn_id: Some(txn_id),
        })]
    }

    fn handle_replaced_row(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::WriteResult { .. }) => {
                let Some(txn_id) = self.txn_id else {
                    return self.emit_error(UploadPartError::NoTransactionFound);
                };
                self.state = UploadPartState::CommitTransaction;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            Event::Storage(StorageEvent::Error { error }) => {
                self.cleanup_failed_write(error.into())
            }
            _ => self.emit_error(UploadPartError::InvalidOperationState),
        }
    }

    fn handle_transaction_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.handle_commit_failure(event);
        };
        self.txn_id = None;
        // The committed part record owns the blob now, so the rollback must not
        // still hold it.
        let Some(location) = self.written_location.take() else {
            return self.emit_error(UploadPartError::UploadPartFailed);
        };
        self.release_id = Some(location.ulid);
        self.output = Some(Ok(UploadPartResult { location }));
        self.release_or_error()
    }

    /// A commit whose outcome is unknown may already own the part bytes, so only
    /// a proven refusal rolls them back. The rest go to the reconciliation
    /// queue: the committed part record is what decides their fate.
    fn handle_commit_failure(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::Error { error }) = event else {
            return self.emit_error(UploadPartError::InvalidOperationState);
        };
        self.txn_id = None;
        if error.proves_no_commit() {
            return self.cleanup_failed_write(UploadPartError::StorageError(error));
        }
        let Some(location) = self.written_location.take() else {
            return self.emit_error(error.into());
        };
        self.release_id = Some(location.ulid);
        warn!(
            event = "upload_part.commit_outcome_unknown",
            backend = %location.backend,
            blob_size = location.blob_size,
            error = %error,
            "Queuing the written part for reconciliation"
        );
        self.pending_error = Some(error.into());
        self.queue_cleanup_work(BlobCleanupWork::ReconcileWrite {
            location,
            owner: WriteOwner::UploadPart {
                upload_id: self.input.upload_id,
                part_number: self.input.part_number,
            },
        })
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
            UploadPartState::QueueCleanupRow => self.handle_cleanup_queued(event),
            UploadPartState::WriteCleanupRow => self.handle_cleanup_row(event),
            UploadPartState::StartTransaction => self.handle_transaction_started(event),
            UploadPartState::FenceBackend => self.handle_backend_fenced(event),
            UploadPartState::ReReadUpload => self.handle_upload_reread(event),
            UploadPartState::ReadExistingPart => self.handle_existing_part_read(event),
            UploadPartState::WritePartRecord => self.handle_part_record_written(event),
            UploadPartState::WriteReplacedRow => self.handle_replaced_row(event),
            UploadPartState::CommitTransaction => self.handle_transaction_committed(event),
            UploadPartState::ReleaseReservation => self.handle_release(event),
            UploadPartState::Finish => smallvec![],
            UploadPartState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, UploadPartState::Finish | UploadPartState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if let Some(Err(error)) = self.output {
            return Err(error);
        }
        if self.state != UploadPartState::Finish {
            return Err(UploadPartError::UploadPartFailed);
        }

        Ok(self.output)
    }

    fn abort(&mut self) -> Effects {
        let mut effects = smallvec![];
        if let Some(effect) = self.pending_cleanup.retry(&StorageError::Timeout) {
            if matches!(self.output, Some(Ok(_))) {
                self.output = None;
            }
            self.state = UploadPartState::QueueCleanupRow;
            effects.push(effect);
        } else {
            if self.rollback_location.is_none() {
                self.rollback_location = self.written_location.take();
            }
            if let Some(location) = self.rollback_location.take() {
                let work = BlobCleanupWork::DeleteBlob {
                    location: location.clone(),
                };
                if let Some(effect) = self.pending_cleanup.queue(work) {
                    if matches!(self.output, Some(Ok(_))) {
                        self.output = None;
                    }
                    if self.output.is_none() && self.pending_error.is_none() {
                        self.pending_error = Some(UploadPartError::UploadPartFailed);
                    }
                    self.state = UploadPartState::QueueCleanupRow;
                    effects.push(effect);
                } else {
                    self.rollback_location = Some(location);
                    self.state = UploadPartState::Error;
                    if self.output.is_none() {
                        self.output = Some(Err(UploadPartError::UploadPartFailed));
                    }
                }
            } else if self.state != UploadPartState::Finish {
                self.state = UploadPartState::Error;
                if self.output.is_none() {
                    self.output = Some(Err(UploadPartError::UploadPartFailed));
                }
            }
        }
        if let Some(id) = self.release_id.take() {
            effects.push(Effect::Blob(BlobEffect::ReleaseReservation { id }));
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
    use aruna_core::keyspaces::BLOB_CLEANUP_KEYSPACE;
    use aruna_core::structs::RealmId;
    use aruna_storage::storage;
    use tempfile::tempdir;

    fn test_user_id() -> UserId {
        UserId::local(Ulid::generate(), RealmId::from_bytes([1u8; 32]))
    }

    #[test]
    fn part_follows_pin() {
        // The pinned backend on the upload record reaches WritePart unchanged.
        let upload_id = Ulid::generate();
        let mut op = UploadPartOperation::new(UploadPartInput {
            bucket: "mybucket".to_string(),
            key: "object.txt".to_string(),
            upload_id,
            part_number: 2,
            content_length: None,
            body: Some(BackendStream::new(tokio_util::io::ReaderStream::new(
                &b"part"[..],
            ))),
            created_by: test_user_id(),
            compressed: false,
            encrypted: false,
            expected_checksums: Vec::new(),
        });
        op.state = UploadPartState::ReadUpload;
        let record = MultipartUpload {
            upload_id,
            backend: aruna_core::structs::BackendRef::Node("cold".to_string()),
            storage_class: Some("cold".to_string()),
            bucket: "mybucket".to_string(),
            key: "object.txt".to_string(),
            group_id: Ulid::generate(),
            created_by: test_user_id(),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            status: MultipartUploadStatus::Open,
            checksum_hint: None,
            metadata: std::collections::HashMap::new(),
            placement_policies: Vec::new(),
        };

        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            value: Some(record.to_bytes().unwrap().into()),
            key: upload_id.to_bytes().to_vec().into(),
        }));

        let [Effect::Blob(BlobEffect::WritePart { resolved, .. })] = effects.as_slice() else {
            panic!("expected one part write, got {effects:?}")
        };
        assert_eq!(resolved.backend, record.backend);
        assert_eq!(resolved.storage_class, record.storage_class);
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
            compressed: false,
            encrypted: false,
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
            compressed: false,
            encrypted: false,
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

        let effects = op.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: Ulid::from_bytes([3u8; 16]),
        }));
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
            error: StorageError::WriteError,
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
        // Only a proven refusal rolls the part back; every other commit failure
        // may already have committed the record that names these bytes, so the
        // copy is handed to reconciliation instead of deleted or forgotten.
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
    fn conflict_deletes_part() {
        let mut op = upload_part_op(Ulid::from_bytes([5u8; 16]));
        op.state = UploadPartState::CommitTransaction;
        op.txn_id = Some(Ulid::from_bytes([3u8; 16]));

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
        let Some(Ok(result)) = op.finalize().unwrap() else {
            panic!("expected successful replacement")
        };
        assert_eq!(result.location, new);
    }

    #[test]
    fn release_failure_succeeds() {
        // A refused release runs after the part is durably committed, so it must
        // not report the committed part as failed; the reconciliation row that
        // committed with it clears the reservation.
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
        let Some(Ok(result)) = op.finalize().unwrap() else {
            panic!("expected the committed part to succeed")
        };
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

    fn upload_part_op(backend_id: Ulid) -> UploadPartOperation {
        let mut op = UploadPartOperation::new(UploadPartInput {
            bucket: "mybucket".to_string(),
            key: "object.txt".to_string(),
            upload_id: Ulid::generate(),
            part_number: 1,
            content_length: None,
            body: None,
            created_by: test_user_id(),
            compressed: false,
            encrypted: false,
            expected_checksums: Vec::new(),
        });
        op.written_location = Some(part_location(backend_id));
        op
    }

    fn part_location(backend_id: Ulid) -> BackendLocation {
        BackendLocation {
            backend: aruna_core::structs::BackendRef::Group(backend_id),
            storage_class: None,
            root: "root".to_string(),
            storage_bucket: "storage".to_string(),
            backend_path: "mybucket/object.txt".to_string(),
            ulid: Ulid::from_bytes([6u8; 16]),
            compressed: false,
            encrypted: false,
            created_by: test_user_id(),
            created_at: SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 4,
            hashes: std::collections::HashMap::new(),
        }
    }

    fn disabled_record(backend_id: Ulid) -> Vec<u8> {
        aruna_core::structs::GroupStorageBackend {
            backend_id,
            group_id: Ulid::from_bytes([7u8; 16]),
            name: "tenant".to_string(),
            kind: aruna_core::structs::GroupBackendKind::S3,
            public_config: std::collections::HashMap::new(),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            disabled: true,
            cleanup: aruna_core::structs::CleanupStrategy::Retain,
        }
        .to_bytes()
        .unwrap()
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
                upload_id: Ulid::generate(),
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
