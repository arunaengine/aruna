use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::S3_MULTIPART_UPLOAD_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::{MultipartUpload, MultipartUploadChecksumHint, MultipartUploadStatus};
use aruna_core::types::{Effects, GroupId, TxnId, UserId};
use smallvec::smallvec;
use std::time::SystemTime;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CreateMultipartUploadState {
    Init,
    StartTransaction,
    WriteUpload,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum CreateMultipartUploadError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("Transaction id missing")]
    TransactionMissing,
    #[error("State [{state:?}] invalid: expected [{expected:?}] - received [{received:?}]")]
    InvalidStateEvent {
        state: CreateMultipartUploadState,
        expected: &'static str,
        received: Event,
    },
    #[error("CreateMultipartUpload failed")]
    CreateMultipartUploadFailed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateMultipartUploadInput {
    pub bucket: String,
    pub key: String,
    pub group_id: GroupId,
    pub created_by: UserId,
    pub checksum_hint: Option<MultipartUploadChecksumHint>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateMultipartUploadResult {
    pub record: MultipartUpload,
}

#[derive(Debug, PartialEq)]
pub struct CreateMultipartUploadOperation {
    input: CreateMultipartUploadInput,
    state: CreateMultipartUploadState,
    txn_id: Option<TxnId>,
    record: Option<MultipartUpload>,
    output: Option<Result<CreateMultipartUploadResult, CreateMultipartUploadError>>,
}

impl CreateMultipartUploadOperation {
    pub fn new(input: CreateMultipartUploadInput) -> Self {
        Self {
            input,
            state: CreateMultipartUploadState::Init,
            txn_id: None,
            record: None,
            output: None,
        }
    }

    fn emit_error(&mut self, error: CreateMultipartUploadError) -> Effects {
        self.state = CreateMultipartUploadState::Error;
        self.output = Some(Err(error));
        self.abort()
    }

    fn handle_init(&mut self) -> Effects {
        self.state = CreateMultipartUploadState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn handle_transaction_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.emit_error(CreateMultipartUploadError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionStarted)",
                received: event,
            });
        };

        let record = MultipartUpload {
            upload_id: Ulid::new(),
            bucket: self.input.bucket.clone(),
            key: self.input.key.clone(),
            group_id: self.input.group_id,
            created_by: self.input.created_by,
            created_at: SystemTime::now(),
            status: MultipartUploadStatus::Open,
            checksum_hint: self.input.checksum_hint.clone(),
        };
        let value = match record.to_bytes() {
            Ok(value) => value,
            Err(err) => return self.emit_error(err.into()),
        };

        self.txn_id = Some(txn_id);
        self.record = Some(record.clone());
        self.state = CreateMultipartUploadState::WriteUpload;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: S3_MULTIPART_UPLOAD_KEYSPACE.to_string(),
            key: record.upload_id.to_bytes().to_vec().into(),
            value: value.into(),
            txn_id: Some(txn_id),
        })]
    }

    fn handle_record_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.emit_error(CreateMultipartUploadError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::WriteResult)",
                received: event,
            });
        };

        let Some(txn_id) = self.txn_id else {
            return self.emit_error(CreateMultipartUploadError::TransactionMissing);
        };
        self.state = CreateMultipartUploadState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_transaction_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.emit_error(CreateMultipartUploadError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionCommitted)",
                received: event,
            });
        };

        let Some(record) = self.record.clone() else {
            return self.emit_error(CreateMultipartUploadError::CreateMultipartUploadFailed);
        };
        self.txn_id = None;
        self.state = CreateMultipartUploadState::Finish;
        self.output = Some(Ok(CreateMultipartUploadResult { record }));
        smallvec![]
    }
}

impl Operation for CreateMultipartUploadOperation {
    type Output = Option<Result<CreateMultipartUploadResult, CreateMultipartUploadError>>;
    type Error = CreateMultipartUploadError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            CreateMultipartUploadState::Init => self.handle_init(),
            CreateMultipartUploadState::StartTransaction => self.handle_transaction_started(event),
            CreateMultipartUploadState::WriteUpload => self.handle_record_written(event),
            CreateMultipartUploadState::CommitTransaction => {
                self.handle_transaction_committed(event)
            }
            CreateMultipartUploadState::Finish => smallvec![],
            CreateMultipartUploadState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            CreateMultipartUploadState::Finish | CreateMultipartUploadState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == CreateMultipartUploadState::Error {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(CreateMultipartUploadError::CreateMultipartUploadFailed);
        }

        Ok(self.output)
    }

    fn abort(&mut self) -> Effects {
        self.txn_id.map_or_else(smallvec::SmallVec::new, |txn_id| {
            smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
        })
    }
}
