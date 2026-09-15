use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::S3_BUCKET_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::types::Effects;
use smallvec::smallvec;
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GetBucketState {
    Init,
    ReadBucket,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum GetBucketError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("Bucket not found")]
    NotFound,
    #[error("State [{state:?}] invalid: expected [{expected:?}] - received [{received:?}]")]
    InvalidStateEvent {
        state: GetBucketState,
        expected: &'static str,
        received: Event,
    },
    #[error("GetBucketInfo has no result yet")]
    Incomplete,
}

#[derive(Debug, PartialEq)]
pub struct GetBucketOperation {
    bucket: String,
    state: GetBucketState,
    result: Option<Result<BucketInfo, GetBucketError>>,
}

impl GetBucketOperation {
    pub fn new(bucket: String) -> Self {
        Self {
            bucket,
            state: GetBucketState::Init,
            result: None,
        }
    }

    fn emit_error(&mut self, error: GetBucketError) -> Effects {
        self.state = GetBucketState::Error;
        self.result = Some(Err(error));
        smallvec![]
    }

    fn handle_init(&mut self) -> Effects {
        self.state = GetBucketState::ReadBucket;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: self.bucket.as_bytes().into(),
            txn_id: None,
        })]
    }

    fn handle_bucket_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(GetBucketError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received: event,
            });
        };

        self.state = GetBucketState::Finish;
        self.result = Some(match value {
            Some(bytes) => BucketInfo::from_bytes(&bytes).map_err(GetBucketError::ConversionError),
            None => Err(GetBucketError::NotFound),
        });
        smallvec![]
    }
}

impl Operation for GetBucketOperation {
    type Output = BucketInfo;
    type Error = GetBucketError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = &event {
            return self.emit_error(error.clone().into());
        }
        match self.state {
            GetBucketState::Init => self.handle_init(),
            GetBucketState::ReadBucket => self.handle_bucket_read(event),
            GetBucketState::Finish => smallvec![],
            GetBucketState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, GetBucketState::Finish | GetBucketState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.result.unwrap_or(Err(GetBucketError::Incomplete))
    }

    fn expected_error(error: &Self::Error) -> bool {
        matches!(error, GetBucketError::NotFound)
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod state_machine_tests {
    use super::{GetBucketError, GetBucketOperation};
    use aruna_core::UserId;
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::S3_BUCKET_KEYSPACE;
    use aruna_core::operation::Operation;
    use aruna_core::structs::storage::blob::BucketInfo;
    use aruna_core::structs::identity::realm::RealmId;
    use ulid::Ulid;

    fn fixed_bucket_info() -> BucketInfo {
        BucketInfo {
            group_id: Ulid::from_bytes([7u8; 16]),
            created_at: std::time::UNIX_EPOCH,
            created_by: UserId::new(Ulid::from_bytes([8u8; 16]), RealmId([9u8; 32])),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
        }
    }

    fn read_result(bucket: &str, value: Option<Vec<u8>>) -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: bucket.as_bytes().to_vec().into(),
            value: value.map(Into::into),
        })
    }

    #[test]
    fn bucket_not_found() {
        let mut operation = GetBucketOperation::new("missing".to_owned());

        assert!(!operation.is_complete());
        let effects = operation.start();
        assert_eq!(
            effects.as_slice(),
            &[Effect::Storage(StorageEffect::Read {
                key_space: S3_BUCKET_KEYSPACE.to_string(),
                key: b"missing".to_vec().into(),
                txn_id: None,
            })]
        );
        assert!(!operation.is_complete());

        let effects = operation.step(read_result("missing", None));

        assert!(effects.is_empty());
        assert!(operation.is_complete());
        assert_eq!(operation.finalize(), Err(GetBucketError::NotFound));
    }

    #[test]
    fn found_bucket_returns() {
        let info = fixed_bucket_info();
        let bytes = info.to_bytes().expect("fixture encodes");
        let mut operation = GetBucketOperation::new("bucket".to_owned());
        operation.start();

        let effects = operation.step(read_result("bucket", Some(bytes)));

        assert!(effects.is_empty());
        assert!(operation.is_complete());
        assert_eq!(operation.finalize(), Ok(info));
    }

    #[test]
    fn malformed_record_rejected() {
        let mut operation = GetBucketOperation::new("bucket".to_owned());
        operation.start();

        operation.step(read_result("bucket", Some(vec![0xff, 0x00, 0x01])));
        let error = operation
            .finalize()
            .expect_err("malformed record must fail");
        assert!(
            matches!(error, GetBucketError::ConversionError(_)),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn storage_error_finalize() {
        let mut operation = GetBucketOperation::new("bucket".to_owned());
        operation.start();

        let effects = operation.step(Event::Storage(StorageEvent::Error {
            error: aruna_core::errors::StorageError::ReadError("boom".to_string()),
        }));

        assert!(effects.is_empty());
        assert!(operation.is_complete());
        assert_eq!(
            operation.finalize(),
            Err(GetBucketError::StorageError(
                aruna_core::errors::StorageError::ReadError("boom".to_string())
            ))
        );
    }

    #[test]
    fn rejects_wrong_event() {
        let mut operation = GetBucketOperation::new("bucket".to_owned());
        operation.start();

        assert!(
            operation
                .step(Event::Storage(StorageEvent::SyncAllFinished))
                .is_empty(),
            "invalid event emits no effects"
        );
        let error = operation.finalize().expect_err("invalid event must fail");
        assert!(matches!(error, GetBucketError::InvalidStateEvent { .. }));
    }

    #[test]
    fn absence_is_expected() {
        assert!(GetBucketOperation::expected_error(
            &GetBucketError::NotFound
        ));
        assert!(!GetBucketOperation::expected_error(
            &GetBucketError::Incomplete
        ));
    }

    #[test]
    fn early_finalize_incomplete() {
        let operation = GetBucketOperation::new("bucket".to_owned());
        assert_eq!(operation.finalize(), Err(GetBucketError::Incomplete));
    }
}
