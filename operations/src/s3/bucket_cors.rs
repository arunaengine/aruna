use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::S3_BUCKET_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::{BucketCorsConfiguration, BucketInfo};
use aruna_core::types::{Effects, Key};
use smallvec::smallvec;
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
enum PutBucketCorsState {
    Init,
    StartTransaction,
    ReadBucket,
    WriteBucket,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum PutBucketCorsError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("Bucket not found")]
    NotFound,
    #[error("No transaction found")]
    NoTransactionFound,
    #[error("Unexpected event in state {state:?}: expected {expected}, got {received:?}")]
    InvalidStateEvent {
        state: &'static str,
        expected: &'static str,
        received: Event,
    },
}

#[derive(Debug, PartialEq)]
pub struct PutBucketCorsOperation {
    bucket: String,
    config: BucketCorsConfiguration,
    state: PutBucketCorsState,
    txn_id: Option<ulid::Ulid>,
    output: Option<Result<BucketCorsConfiguration, PutBucketCorsError>>,
}

impl PutBucketCorsOperation {
    pub fn new(bucket: String, config: BucketCorsConfiguration) -> Self {
        Self {
            bucket,
            config,
            state: PutBucketCorsState::Init,
            txn_id: None,
            output: None,
        }
    }

    fn fail(&mut self, err: PutBucketCorsError) -> Effects {
        self.state = PutBucketCorsState::Error;
        self.output = Some(Err(err));
        self.abort()
    }

    fn write_key(&self) -> Key {
        self.bucket.as_bytes().to_vec().into()
    }

    fn state_name(&self) -> &'static str {
        match self.state {
            PutBucketCorsState::Init => "Init",
            PutBucketCorsState::StartTransaction => "StartTransaction",
            PutBucketCorsState::ReadBucket => "ReadBucket",
            PutBucketCorsState::WriteBucket => "WriteBucket",
            PutBucketCorsState::CommitTransaction => "CommitTransaction",
            PutBucketCorsState::Finish => "Finish",
            PutBucketCorsState::Error => "Error",
        }
    }
}

impl Operation for PutBucketCorsOperation {
    type Output = Option<Result<BucketCorsConfiguration, PutBucketCorsError>>;
    type Error = PutBucketCorsError;

    fn start(&mut self) -> Effects {
        self.state = PutBucketCorsState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            PutBucketCorsState::Init => self.start(),
            PutBucketCorsState::StartTransaction => {
                let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
                    return self.fail(PutBucketCorsError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::TransactionStarted)",
                        received: event,
                    });
                };
                self.txn_id = Some(txn_id);
                self.state = PutBucketCorsState::ReadBucket;
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: S3_BUCKET_KEYSPACE.to_string(),
                    key: self.write_key(),
                    txn_id: Some(txn_id),
                })]
            }
            PutBucketCorsState::ReadBucket => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.fail(PutBucketCorsError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::ReadResult)",
                        received: event,
                    });
                };
                let Some(txn_id) = self.txn_id else {
                    return self.fail(PutBucketCorsError::NoTransactionFound);
                };
                let Some(bytes) = value else {
                    return self.fail(PutBucketCorsError::NotFound);
                };
                let mut bucket_info = match BucketInfo::from_bytes(&bytes) {
                    Ok(info) => info,
                    Err(err) => return self.fail(err.into()),
                };
                bucket_info.cors_configuration = Some(self.config.clone());
                let value = match bucket_info.to_bytes() {
                    Ok(value) => value,
                    Err(err) => return self.fail(err.into()),
                };
                self.state = PutBucketCorsState::WriteBucket;
                smallvec![Effect::Storage(StorageEffect::Write {
                    key_space: S3_BUCKET_KEYSPACE.to_string(),
                    key: self.write_key(),
                    value: value.into(),
                    txn_id: Some(txn_id),
                })]
            }
            PutBucketCorsState::WriteBucket => {
                let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
                    return self.fail(PutBucketCorsError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::WriteResult)",
                        received: event,
                    });
                };
                let Some(txn_id) = self.txn_id else {
                    return self.fail(PutBucketCorsError::NoTransactionFound);
                };
                self.state = PutBucketCorsState::CommitTransaction;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            PutBucketCorsState::CommitTransaction => {
                let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
                    return self.fail(PutBucketCorsError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::TransactionCommitted)",
                        received: event,
                    });
                };
                self.txn_id = None;
                self.state = PutBucketCorsState::Finish;
                self.output = Some(Ok(self.config.clone()));
                smallvec![]
            }
            PutBucketCorsState::Finish => smallvec![],
            PutBucketCorsState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            PutBucketCorsState::Finish | PutBucketCorsState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == PutBucketCorsState::Error
            && let Some(Err(err)) = self.output
        {
            return Err(err);
        }
        Ok(self.output)
    }

    fn abort(&mut self) -> Effects {
        self.txn_id.map_or_else(smallvec::SmallVec::new, |txn_id| {
            smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum GetBucketCorsState {
    Init,
    ReadBucket,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum GetBucketCorsError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("Bucket not found")]
    BucketNotFound,
    #[error("Bucket CORS configuration not found")]
    CorsNotFound,
    #[error("Unexpected event in state {state:?}: expected {expected}, got {received:?}")]
    InvalidStateEvent {
        state: &'static str,
        expected: &'static str,
        received: Event,
    },
}

#[derive(Debug, PartialEq)]
pub struct GetBucketCorsOperation {
    bucket: String,
    state: GetBucketCorsState,
    output: Option<Result<BucketCorsConfiguration, GetBucketCorsError>>,
}

impl GetBucketCorsOperation {
    pub fn new(bucket: String) -> Self {
        Self {
            bucket,
            state: GetBucketCorsState::Init,
            output: None,
        }
    }

    fn fail(&mut self, err: GetBucketCorsError) -> Effects {
        self.state = GetBucketCorsState::Error;
        self.output = Some(Err(err));
        smallvec![]
    }

    fn state_name(&self) -> &'static str {
        match self.state {
            GetBucketCorsState::Init => "Init",
            GetBucketCorsState::ReadBucket => "ReadBucket",
            GetBucketCorsState::Finish => "Finish",
            GetBucketCorsState::Error => "Error",
        }
    }
}

impl Operation for GetBucketCorsOperation {
    type Output = Option<Result<BucketCorsConfiguration, GetBucketCorsError>>;
    type Error = GetBucketCorsError;

    fn start(&mut self) -> Effects {
        self.state = GetBucketCorsState::ReadBucket;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: self.bucket.as_bytes().to_vec().into(),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            GetBucketCorsState::Init => self.start(),
            GetBucketCorsState::ReadBucket => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.fail(GetBucketCorsError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::ReadResult)",
                        received: event,
                    });
                };

                self.state = GetBucketCorsState::Finish;
                self.output = Some(match value {
                    Some(bytes) => match BucketInfo::from_bytes(&bytes) {
                        Ok(info) => info
                            .cors_configuration
                            .ok_or(GetBucketCorsError::CorsNotFound),
                        Err(err) => Err(GetBucketCorsError::ConversionError(err)),
                    },
                    None => Err(GetBucketCorsError::BucketNotFound),
                });
                smallvec![]
            }
            GetBucketCorsState::Finish => smallvec![],
            GetBucketCorsState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            GetBucketCorsState::Finish | GetBucketCorsState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == GetBucketCorsState::Error
            && let Some(Err(err)) = self.output
        {
            return Err(err);
        }
        Ok(self.output)
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
