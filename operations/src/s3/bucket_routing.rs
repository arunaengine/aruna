use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::S3_BUCKET_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::{BucketInfo, RoutingError, StorageRoutingRule, validate_tenant_rules};
use aruna_core::types::{Effects, Key};
use smallvec::smallvec;
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
enum PutBucketRoutingState {
    Init,
    StartTransaction,
    ReadBucket,
    WriteBucket,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum PutBucketRoutingError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    InvalidRules(#[from] RoutingError),
    #[error("The specified bucket does not exist.")]
    NoSuchBucket,
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
pub struct PutBucketRoutingOperation {
    bucket: String,
    rules: Vec<StorageRoutingRule>,
    state: PutBucketRoutingState,
    txn_id: Option<ulid::Ulid>,
    output: Option<Result<Vec<StorageRoutingRule>, PutBucketRoutingError>>,
}

impl PutBucketRoutingOperation {
    pub fn new(bucket: String, rules: Vec<StorageRoutingRule>) -> Self {
        Self {
            bucket,
            rules,
            state: PutBucketRoutingState::Init,
            txn_id: None,
            output: None,
        }
    }

    fn fail(&mut self, err: PutBucketRoutingError) -> Effects {
        self.state = PutBucketRoutingState::Error;
        self.output = Some(Err(err));
        self.abort()
    }

    fn write_key(&self) -> Key {
        self.bucket.as_bytes().to_vec().into()
    }

    fn state_name(&self) -> &'static str {
        match self.state {
            PutBucketRoutingState::Init => "Init",
            PutBucketRoutingState::StartTransaction => "StartTransaction",
            PutBucketRoutingState::ReadBucket => "ReadBucket",
            PutBucketRoutingState::WriteBucket => "WriteBucket",
            PutBucketRoutingState::CommitTransaction => "CommitTransaction",
            PutBucketRoutingState::Finish => "Finish",
            PutBucketRoutingState::Error => "Error",
        }
    }
}

impl Operation for PutBucketRoutingOperation {
    type Output = Option<Result<Vec<StorageRoutingRule>, PutBucketRoutingError>>;
    type Error = PutBucketRoutingError;

    fn start(&mut self) -> Effects {
        if let Err(error) = validate_tenant_rules(&self.rules) {
            return self.fail(error.into());
        }
        self.state = PutBucketRoutingState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            PutBucketRoutingState::Init => self.start(),
            PutBucketRoutingState::StartTransaction => {
                let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
                    return self.fail(PutBucketRoutingError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::TransactionStarted)",
                        received: event,
                    });
                };
                self.txn_id = Some(txn_id);
                self.state = PutBucketRoutingState::ReadBucket;
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: S3_BUCKET_KEYSPACE.to_string(),
                    key: self.write_key(),
                    txn_id: Some(txn_id),
                })]
            }
            PutBucketRoutingState::ReadBucket => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.fail(PutBucketRoutingError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::ReadResult)",
                        received: event,
                    });
                };
                let Some(value) = value else {
                    return self.fail(PutBucketRoutingError::NoSuchBucket);
                };
                let mut info = match BucketInfo::from_bytes(value.as_ref()) {
                    Ok(info) => info,
                    Err(err) => return self.fail(err.into()),
                };
                info.storage_routing = self.rules.clone();
                let value = match info.to_bytes() {
                    Ok(value) => value,
                    Err(err) => return self.fail(err.into()),
                };
                self.state = PutBucketRoutingState::WriteBucket;
                smallvec![Effect::Storage(StorageEffect::Write {
                    key_space: S3_BUCKET_KEYSPACE.to_string(),
                    key: self.write_key(),
                    value: value.into(),
                    txn_id: self.txn_id,
                })]
            }
            PutBucketRoutingState::WriteBucket => {
                let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
                    return self.fail(PutBucketRoutingError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::WriteResult)",
                        received: event,
                    });
                };
                let Some(txn_id) = self.txn_id else {
                    return self.fail(PutBucketRoutingError::NoTransactionFound);
                };
                self.state = PutBucketRoutingState::CommitTransaction;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            PutBucketRoutingState::CommitTransaction => {
                let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
                    return self.fail(PutBucketRoutingError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::TransactionCommitted)",
                        received: event,
                    });
                };
                self.txn_id = None;
                self.state = PutBucketRoutingState::Finish;
                self.output = Some(Ok(self.rules.clone()));
                smallvec![]
            }
            PutBucketRoutingState::Finish => smallvec![],
            PutBucketRoutingState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            PutBucketRoutingState::Finish | PutBucketRoutingState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == PutBucketRoutingState::Error
            && let Some(Err(err)) = self.output
        {
            return Err(err);
        }
        Ok(self.output)
    }

    fn abort(&mut self) -> Effects {
        self.txn_id
            .take()
            .map_or_else(smallvec::SmallVec::new, |txn_id| {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum GetBucketRoutingState {
    Init,
    ReadBucket,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum GetBucketRoutingError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("The specified bucket does not exist.")]
    NoSuchBucket,
    #[error("Unexpected event in state {state:?}: expected {expected}, got {received:?}")]
    InvalidStateEvent {
        state: &'static str,
        expected: &'static str,
        received: Event,
    },
}

#[derive(Debug, PartialEq)]
pub struct GetBucketRoutingOperation {
    bucket: String,
    state: GetBucketRoutingState,
    output: Option<Result<Vec<StorageRoutingRule>, GetBucketRoutingError>>,
}

impl GetBucketRoutingOperation {
    pub fn new(bucket: String) -> Self {
        Self {
            bucket,
            state: GetBucketRoutingState::Init,
            output: None,
        }
    }

    fn state_name(&self) -> &'static str {
        match self.state {
            GetBucketRoutingState::Init => "Init",
            GetBucketRoutingState::ReadBucket => "ReadBucket",
            GetBucketRoutingState::Finish => "Finish",
            GetBucketRoutingState::Error => "Error",
        }
    }

    fn fail(&mut self, err: GetBucketRoutingError) -> Effects {
        self.state = GetBucketRoutingState::Error;
        self.output = Some(Err(err));
        smallvec![]
    }
}

impl Operation for GetBucketRoutingOperation {
    type Output = Option<Result<Vec<StorageRoutingRule>, GetBucketRoutingError>>;
    type Error = GetBucketRoutingError;

    fn start(&mut self) -> Effects {
        self.state = GetBucketRoutingState::ReadBucket;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: self.bucket.as_bytes().to_vec().into(),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            GetBucketRoutingState::Init => self.start(),
            GetBucketRoutingState::ReadBucket => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.fail(GetBucketRoutingError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::ReadResult)",
                        received: event,
                    });
                };
                let Some(value) = value else {
                    return self.fail(GetBucketRoutingError::NoSuchBucket);
                };
                match BucketInfo::from_bytes(value.as_ref()) {
                    Ok(info) => {
                        self.state = GetBucketRoutingState::Finish;
                        self.output = Some(Ok(info.storage_routing));
                        smallvec![]
                    }
                    Err(err) => self.fail(err.into()),
                }
            }
            GetBucketRoutingState::Finish | GetBucketRoutingState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            GetBucketRoutingState::Finish | GetBucketRoutingState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == GetBucketRoutingState::Error
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

#[cfg(test)]
mod tests {
    use super::{
        GetBucketRoutingError, GetBucketRoutingOperation, PutBucketRoutingError,
        PutBucketRoutingOperation,
    };
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::operation::Operation;
    use aruna_core::structs::{
        BackendRef, BucketInfo, RoutingError, RoutingTarget, StorageRoutingRule,
    };
    use aruna_core::types::TxnId;
    use std::time::SystemTime;
    use ulid::Ulid;

    fn rule(target: RoutingTarget) -> StorageRoutingRule {
        StorageRoutingRule {
            key_prefix: "archive/".to_string(),
            exact: false,
            target,
        }
    }

    fn bucket() -> BucketInfo {
        BucketInfo {
            group_id: Ulid::from_bytes([1u8; 16]),
            created_at: SystemTime::UNIX_EPOCH,
            created_by: aruna_core::UserId::default(),
            cors_configuration: None,
            replication: None,
            storage_routing: Vec::new(),
        }
    }

    #[test]
    fn writes_bucket_rules() {
        let rules = vec![rule(RoutingTarget::Class("cold".to_string()))];
        let mut operation = PutBucketRoutingOperation::new("b".to_string(), rules.clone());
        operation.start();
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TxnId::default(),
        }));

        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: b"b".to_vec().into(),
            value: Some(bucket().to_bytes().unwrap().into()),
        }));

        let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
            panic!("expected one record write, got {effects:?}")
        };
        let written = BucketInfo::from_bytes(value.as_ref()).unwrap();
        assert_eq!(written.storage_routing, rules);
    }

    #[test]
    fn rejects_operator_target() {
        // Tenant rules must not bind an operator backend name.
        let rules = vec![rule(RoutingTarget::Backend(BackendRef::Node(
            "cold".to_string(),
        )))];
        let mut operation = PutBucketRoutingOperation::new("b".to_string(), rules);

        let effects = operation.start();

        assert!(effects.is_empty());
        assert!(operation.is_complete());
        assert!(matches!(
            operation.finalize(),
            Err(PutBucketRoutingError::InvalidRules(
                RoutingError::OperatorBackendTarget
            ))
        ));
    }

    #[test]
    fn missing_bucket_aborts() {
        let mut operation = PutBucketRoutingOperation::new("b".to_string(), Vec::new());
        operation.start();
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TxnId::default(),
        }));

        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: b"b".to_vec().into(),
            value: None,
        }));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { .. })]
        ));
        assert!(matches!(
            operation.finalize(),
            Err(PutBucketRoutingError::NoSuchBucket)
        ));
    }

    #[test]
    fn rejects_unexpected_event() {
        let mut operation = PutBucketRoutingOperation::new("b".to_string(), Vec::new());
        operation.start();

        operation.step(Event::Storage(StorageEvent::WriteResult {
            key: b"b".to_vec().into(),
        }));

        assert!(matches!(
            operation.finalize(),
            Err(PutBucketRoutingError::InvalidStateEvent { .. })
        ));
    }

    #[test]
    fn reads_stored_rules() {
        let rules = vec![rule(RoutingTarget::Class("cold".to_string()))];
        let mut info = bucket();
        info.storage_routing = rules.clone();
        let mut operation = GetBucketRoutingOperation::new("b".to_string());
        operation.start();

        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: b"b".to_vec().into(),
            value: Some(info.to_bytes().unwrap().into()),
        }));

        assert_eq!(operation.finalize().unwrap(), Some(Ok(rules)));
    }

    #[test]
    fn missing_bucket_errors() {
        let mut operation = GetBucketRoutingOperation::new("b".to_string());
        operation.start();

        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: b"b".to_vec().into(),
            value: None,
        }));

        assert!(matches!(
            operation.finalize(),
            Err(GetBucketRoutingError::NoSuchBucket)
        ));
    }
}
