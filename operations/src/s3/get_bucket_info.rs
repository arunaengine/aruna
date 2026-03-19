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
pub enum GetBucketInfoState {
    Init,
    ReadBucket,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum GetBucketInfoError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("Bucket not found")]
    NotFound,
    #[error("State [{state:?}] invalid: expected [{expected:?}] - received [{received:?}]")]
    InvalidStateEvent {
        state: GetBucketInfoState,
        expected: &'static str,
        received: Event,
    },
    #[error("GetBucketInfo failed")]
    GetBucketInfoFailed,
}

#[derive(Debug, PartialEq)]
pub struct GetBucketInfoOperation {
    bucket: String,
    state: GetBucketInfoState,
    output: Option<Result<BucketInfo, GetBucketInfoError>>,
}

impl GetBucketInfoOperation {
    pub fn new(bucket: String) -> Self {
        Self {
            bucket,
            state: GetBucketInfoState::Init,
            output: None,
        }
    }

    fn emit_error(&mut self, error: GetBucketInfoError) -> Effects {
        self.state = GetBucketInfoState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn handle_init(&mut self) -> Effects {
        self.state = GetBucketInfoState::ReadBucket;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: self.bucket.as_bytes().into(),
            txn_id: None,
        })]
    }

    fn handle_bucket_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(GetBucketInfoError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received: event,
            });
        };

        self.state = GetBucketInfoState::Finish;
        self.output = Some(match value {
            Some(bytes) => {
                BucketInfo::from_bytes(&bytes).map_err(GetBucketInfoError::ConversionError)
            }
            None => Err(GetBucketInfoError::NotFound),
        });
        smallvec![]
    }
}

impl Operation for GetBucketInfoOperation {
    type Output = Option<Result<BucketInfo, GetBucketInfoError>>;
    type Error = GetBucketInfoError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            GetBucketInfoState::Init => self.handle_init(),
            GetBucketInfoState::ReadBucket => self.handle_bucket_read(event),
            GetBucketInfoState::Finish => smallvec![],
            GetBucketInfoState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            GetBucketInfoState::Finish | GetBucketInfoState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if GetBucketInfoState::Error == self.state {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(GetBucketInfoError::GetBucketInfoFailed);
        }
        Ok(self.output)
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
