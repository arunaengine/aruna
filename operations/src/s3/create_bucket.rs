use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::S3_BUCKET_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::BucketInfo;
use aruna_core::types::Effects;
use smallvec::smallvec;
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CreateBucketState {
    Init,
    StartTransaction,
    CheckExists,
    CreateBucket,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum CreateBucketError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("Bucket already exists")]
    BucketAlreadyExists,
    #[error("No transaction found")]
    NoTransactionFound,
    #[error("Invalid state [{current:?}] - expected [{expected:?}]")]
    InvalidState {
        current: CreateBucketState,
        expected: CreateBucketState,
    },
    #[error("State [{state:?}] invalid: expected [{expected:?}] - received [{received:?}]")]
    InvalidStateEvent {
        state: CreateBucketState,
        expected: &'static str,
        received: Event,
    },
    #[error("CreateBucket failed")]
    CreateBucketFailed,
}

#[derive(Debug, PartialEq)]
pub struct CreateBucketOperation {
    bucket: String,
    bucket_info: BucketInfo,
    state: CreateBucketState,
    txn_id: Option<ulid::Ulid>,
    output: Option<Result<BucketInfo, CreateBucketError>>,
}

impl CreateBucketOperation {
    pub fn new(bucket: String, bucket_info: BucketInfo) -> Self {
        Self {
            bucket,
            bucket_info,
            state: CreateBucketState::Init,
            txn_id: None,
            output: None,
        }
    }

    fn emit_error(&mut self, error: CreateBucketError) -> Effects {
        self.state = CreateBucketState::Error;
        self.output = Some(Err(error));
        self.abort()
    }

    fn handle_init(&mut self) -> Effects {
        if self.state != CreateBucketState::Init {
            return self.emit_error(CreateBucketError::InvalidState {
                current: self.state.clone(),
                expected: CreateBucketState::Init,
            });
        }

        self.state = CreateBucketState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn handle_transaction_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.emit_error(CreateBucketError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionStarted)",
                received: event,
            });
        };

        self.txn_id = Some(txn_id);
        self.state = CreateBucketState::CheckExists;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: self.bucket.as_bytes().into(),
            txn_id: Some(txn_id),
        })]
    }

    fn handle_bucket_checked(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(CreateBucketError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received: event,
            });
        };

        if value.is_some() {
            return self.emit_error(CreateBucketError::BucketAlreadyExists);
        }

        let Some(txn_id) = self.txn_id else {
            return self.emit_error(CreateBucketError::NoTransactionFound);
        };
        let bytes = match self.bucket_info.to_bytes() {
            Ok(bytes) => bytes,
            Err(err) => return self.emit_error(err.into()),
        };

        self.state = CreateBucketState::CreateBucket;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: self.bucket.as_bytes().into(),
            value: bytes.into(),
            txn_id: Some(txn_id),
        })]
    }

    fn handle_bucket_created(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.emit_error(CreateBucketError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::WriteResult)",
                received: event,
            });
        };

        let Some(txn_id) = self.txn_id else {
            return self.emit_error(CreateBucketError::NoTransactionFound);
        };
        self.state = CreateBucketState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_transaction_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.emit_error(CreateBucketError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionCommitted)",
                received: event,
            });
        };

        self.txn_id = None;
        self.state = CreateBucketState::Finish;
        self.output = Some(Ok(self.bucket_info.clone()));
        smallvec![]
    }
}

impl Operation for CreateBucketOperation {
    type Output = Option<Result<BucketInfo, CreateBucketError>>;
    type Error = CreateBucketError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            CreateBucketState::Init => self.handle_init(),
            CreateBucketState::StartTransaction => self.handle_transaction_started(event),
            CreateBucketState::CheckExists => self.handle_bucket_checked(event),
            CreateBucketState::CreateBucket => self.handle_bucket_created(event),
            CreateBucketState::CommitTransaction => self.handle_transaction_committed(event),
            CreateBucketState::Finish => smallvec![],
            CreateBucketState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            CreateBucketState::Finish | CreateBucketState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if CreateBucketState::Error == self.state {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(CreateBucketError::CreateBucketFailed);
        }
        Ok(self.output)
    }

    fn abort(&mut self) -> Effects {
        self.txn_id.map_or_else(smallvec::SmallVec::new, |txn_id| {
            smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use aruna_storage::storage;
    use std::time::SystemTime;
    use tempfile::tempdir;
    use ulid::Ulid;

    #[tokio::test]
    async fn test_create_bucket() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            automerge_handle: None,
            metadata_handle: None,
            task_handle: None,
        };

        let bucket_info = BucketInfo {
            group_id: Ulid::new(),
            created_at: SystemTime::now(),
            created_by: Ulid::new(),
        };

        let result = drive(
            CreateBucketOperation::new("bucket-a".to_string(), bucket_info.clone()),
            &driver_ctx,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        assert_eq!(result, bucket_info);
    }
}
