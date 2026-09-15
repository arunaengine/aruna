use crate::groups::storage_routing::load_group_inputs;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::S3_BUCKET_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::{BucketInfo, RoutingError, StorageRoutingRule, validate_tenant_rules};
use aruna_core::types::{Effects, GroupId, Key};
use smallvec::smallvec;
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
enum PutRoutingState {
    Init,
    LoadInputs,
    StartTransaction,
    ReadBucket,
    WriteBucket,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum PutRoutingError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    InvalidRules(#[from] RoutingError),
    #[error("The specified bucket does not exist.")]
    NoSuchBucket,
    #[error("could not load the group's storage backends: {0}")]
    InputsUnavailable(String),
    #[error("The bucket changed owner while the rules were written")]
    GroupMismatch,
    #[error("No transaction found")]
    NoTransactionFound,
    #[error("PutBucketRouting did not finish")]
    NotFinished,
    #[error("Unexpected event in state {state:?}: expected {expected}, got {received:?}")]
    InvalidStateEvent {
        state: &'static str,
        expected: &'static str,
        received: Event,
    },
}

#[derive(Debug, PartialEq)]
pub struct PutRoutingOperation {
    bucket: String,
    group_id: GroupId,
    rules: Vec<StorageRoutingRule>,
    state: PutRoutingState,
    txn_id: Option<ulid::Ulid>,
    output: Option<Result<Vec<StorageRoutingRule>, PutRoutingError>>,
}

impl PutRoutingOperation {
    pub fn new(bucket: String, group_id: GroupId, rules: Vec<StorageRoutingRule>) -> Self {
        Self {
            bucket,
            group_id,
            rules,
            state: PutRoutingState::Init,
            txn_id: None,
            output: None,
        }
    }

    fn fail(&mut self, err: PutRoutingError) -> Effects {
        self.state = PutRoutingState::Error;
        self.output = Some(Err(err));
        self.abort()
    }

    fn write_key(&self) -> Key {
        self.bucket.as_bytes().to_vec().into()
    }

    fn state_name(&self) -> &'static str {
        match self.state {
            PutRoutingState::Init => "Init",
            PutRoutingState::LoadInputs => "LoadInputs",
            PutRoutingState::StartTransaction => "StartTransaction",
            PutRoutingState::ReadBucket => "ReadBucket",
            PutRoutingState::WriteBucket => "WriteBucket",
            PutRoutingState::CommitTransaction => "CommitTransaction",
            PutRoutingState::Finish => "Finish",
            PutRoutingState::Error => "Error",
        }
    }
}

impl Operation for PutRoutingOperation {
    type Output = Vec<StorageRoutingRule>;
    type Error = PutRoutingError;

    fn start(&mut self) -> Effects {
        // A `Group` target is checked against the ids the bucket's own group
        // registered, so a rule can never name another tenant's backend.
        self.state = PutRoutingState::LoadInputs;
        smallvec![load_group_inputs(self.group_id)]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = &event {
            return self.fail(error.clone().into());
        }
        match self.state {
            PutRoutingState::Init => self.start(),
            PutRoutingState::LoadInputs => {
                let Event::SubOperation(SubOperationEvent::GroupRoutingLoaded { result }) = event
                else {
                    return self.fail(PutRoutingError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::SubOperation(SubOperationEvent::GroupRoutingLoaded)",
                        received: event,
                    });
                };
                let owned = match result {
                    Ok(inputs) => inputs.backend_ids,
                    Err(error) => {
                        return self.fail(PutRoutingError::InputsUnavailable(error));
                    }
                };
                if let Err(error) = validate_tenant_rules(&self.rules, &owned) {
                    return self.fail(error.into());
                }
                self.state = PutRoutingState::StartTransaction;
                smallvec![Effect::Storage(StorageEffect::StartTransaction {
                    read: false,
                })]
            }
            PutRoutingState::StartTransaction => {
                let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
                    return self.fail(PutRoutingError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::TransactionStarted)",
                        received: event,
                    });
                };
                self.txn_id = Some(txn_id);
                self.state = PutRoutingState::ReadBucket;
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: S3_BUCKET_KEYSPACE.to_string(),
                    key: self.write_key(),
                    txn_id: Some(txn_id),
                })]
            }
            PutRoutingState::ReadBucket => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.fail(PutRoutingError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::ReadResult)",
                        received: event,
                    });
                };
                let Some(value) = value else {
                    return self.fail(PutRoutingError::NoSuchBucket);
                };
                let mut info = match BucketInfo::from_bytes(value.as_ref()) {
                    Ok(info) => info,
                    Err(err) => return self.fail(err.into()),
                };
                // The owned backend ids were loaded for the authorized group, so
                // the record has to still belong to it.
                if info.group_id != self.group_id {
                    return self.fail(PutRoutingError::GroupMismatch);
                }
                info.storage_routing = self.rules.clone();
                let value = match info.to_bytes() {
                    Ok(value) => value,
                    Err(err) => return self.fail(err.into()),
                };
                self.state = PutRoutingState::WriteBucket;
                smallvec![Effect::Storage(StorageEffect::Write {
                    key_space: S3_BUCKET_KEYSPACE.to_string(),
                    key: self.write_key(),
                    value: value.into(),
                    txn_id: self.txn_id,
                })]
            }
            PutRoutingState::WriteBucket => {
                let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
                    return self.fail(PutRoutingError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::WriteResult)",
                        received: event,
                    });
                };
                let Some(txn_id) = self.txn_id else {
                    return self.fail(PutRoutingError::NoTransactionFound);
                };
                self.state = PutRoutingState::CommitTransaction;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            PutRoutingState::CommitTransaction => {
                let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
                    return self.fail(PutRoutingError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::TransactionCommitted)",
                        received: event,
                    });
                };
                self.txn_id = None;
                self.state = PutRoutingState::Finish;
                self.output = Some(Ok(self.rules.clone()));
                smallvec![]
            }
            PutRoutingState::Finish => smallvec![],
            PutRoutingState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, PutRoutingState::Finish | PutRoutingState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(PutRoutingError::NotFinished))
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
enum GetRoutingState {
    Init,
    ReadBucket,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum GetRoutingError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("The specified bucket does not exist.")]
    NoSuchBucket,
    #[error("GetBucketRouting did not finish")]
    NotFinished,
    #[error("Unexpected event in state {state:?}: expected {expected}, got {received:?}")]
    InvalidStateEvent {
        state: &'static str,
        expected: &'static str,
        received: Event,
    },
}

#[derive(Debug, PartialEq)]
pub struct GetRoutingOperation {
    bucket: String,
    state: GetRoutingState,
    output: Option<Result<Vec<StorageRoutingRule>, GetRoutingError>>,
}

impl GetRoutingOperation {
    pub fn new(bucket: String) -> Self {
        Self {
            bucket,
            state: GetRoutingState::Init,
            output: None,
        }
    }

    fn state_name(&self) -> &'static str {
        match self.state {
            GetRoutingState::Init => "Init",
            GetRoutingState::ReadBucket => "ReadBucket",
            GetRoutingState::Finish => "Finish",
            GetRoutingState::Error => "Error",
        }
    }

    fn fail(&mut self, err: GetRoutingError) -> Effects {
        self.state = GetRoutingState::Error;
        self.output = Some(Err(err));
        smallvec![]
    }
}

impl Operation for GetRoutingOperation {
    type Output = Vec<StorageRoutingRule>;
    type Error = GetRoutingError;

    fn start(&mut self) -> Effects {
        self.state = GetRoutingState::ReadBucket;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: self.bucket.as_bytes().to_vec().into(),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = &event {
            return self.fail(error.clone().into());
        }
        match self.state {
            GetRoutingState::Init => self.start(),
            GetRoutingState::ReadBucket => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.fail(GetRoutingError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::ReadResult)",
                        received: event,
                    });
                };
                let Some(value) = value else {
                    return self.fail(GetRoutingError::NoSuchBucket);
                };
                match BucketInfo::from_bytes(value.as_ref()) {
                    Ok(info) => {
                        self.state = GetRoutingState::Finish;
                        self.output = Some(Ok(info.storage_routing));
                        smallvec![]
                    }
                    Err(err) => self.fail(err.into()),
                }
            }
            GetRoutingState::Finish | GetRoutingState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, GetRoutingState::Finish | GetRoutingState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(GetRoutingError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod pure_tests {
    use super::{GetRoutingError, GetRoutingOperation, PutRoutingError, PutRoutingOperation};
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
    use aruna_core::operation::Operation;
    use aruna_core::structs::{
        BackendRef, BucketInfo, GroupRoutingInputs, RoutingError, RoutingTarget, StorageRoutingRule,
    };
    use aruna_core::types::{Effects, TxnId};
    use std::collections::BTreeSet;
    use std::time::SystemTime;
    use ulid::Ulid;

    fn rule(target: RoutingTarget) -> StorageRoutingRule {
        StorageRoutingRule {
            key_prefix: "archive/".to_string(),
            exact: false,
            target,
        }
    }

    fn group() -> Ulid {
        Ulid::from_bytes([1u8; 16])
    }

    /// Replays the loader sub-operation with the ids the group owns.
    fn loaded(operation: &mut PutRoutingOperation, owned: BTreeSet<Ulid>) -> Effects {
        operation.start();
        operation.step(Event::SubOperation(SubOperationEvent::GroupRoutingLoaded {
            result: Ok(GroupRoutingInputs {
                default_target: None,
                backend_ids: owned,
            }),
        }))
    }

    fn bucket() -> BucketInfo {
        BucketInfo {
            group_id: group(),
            created_at: SystemTime::UNIX_EPOCH,
            created_by: aruna_core::UserId::default(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
        }
    }

    #[test]
    fn writes_bucket_rules() {
        let rules = vec![rule(RoutingTarget::Class("cold".to_string()))];
        let mut operation = PutRoutingOperation::new("b".to_string(), group(), rules.clone());
        loaded(&mut operation, BTreeSet::new());
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
        let mut operation = PutRoutingOperation::new("b".to_string(), group(), rules);

        let effects = loaded(&mut operation, BTreeSet::new());

        assert!(effects.is_empty());
        assert!(operation.is_complete());
        assert!(matches!(
            operation.finalize(),
            Err(PutRoutingError::InvalidRules(
                RoutingError::OperatorBackendTarget
            ))
        ));
    }

    #[test]
    fn rejects_foreign_backend() {
        // A rule naming a backend this group does not own must not be stored.
        let foreign = Ulid::from_bytes([9u8; 16]);
        let rules = vec![rule(RoutingTarget::Backend(BackendRef::Group(foreign)))];
        let mut operation = PutRoutingOperation::new("b".to_string(), group(), rules);

        let effects = loaded(
            &mut operation,
            BTreeSet::from([Ulid::from_bytes([4u8; 16])]),
        );

        assert!(effects.is_empty(), "expected no write, got {effects:?}");
        assert_eq!(
            operation.finalize(),
            Err(PutRoutingError::InvalidRules(RoutingError::ForeignBackend(
                foreign
            )))
        );
    }

    #[test]
    fn accepts_owned_backend() {
        let owned = Ulid::from_bytes([4u8; 16]);
        let rules = vec![rule(RoutingTarget::Backend(BackendRef::Group(owned)))];
        let mut operation = PutRoutingOperation::new("b".to_string(), group(), rules);

        let effects = loaded(&mut operation, BTreeSet::from([owned]));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::StartTransaction { .. })]
        ));
    }

    #[test]
    fn rejects_foreign_bucket() {
        // The loaded ids belong to the authorized group, so a record that moved
        // to another group must not take its rules.
        let mut operation = PutRoutingOperation::new("b".to_string(), group(), Vec::new());
        loaded(&mut operation, BTreeSet::new());
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TxnId::default(),
        }));
        let mut moved = bucket();
        moved.group_id = Ulid::from_bytes([2u8; 16]);

        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: b"b".to_vec().into(),
            value: Some(moved.to_bytes().unwrap().into()),
        }));

        assert_eq!(operation.finalize(), Err(PutRoutingError::GroupMismatch));
    }

    #[test]
    fn missing_bucket_aborts() {
        let mut operation = PutRoutingOperation::new("b".to_string(), group(), Vec::new());
        loaded(&mut operation, BTreeSet::new());
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
            Err(PutRoutingError::NoSuchBucket)
        ));
    }

    #[test]
    fn rejects_unexpected_event() {
        let mut operation = PutRoutingOperation::new("b".to_string(), group(), Vec::new());
        loaded(&mut operation, BTreeSet::new());

        operation.step(Event::Storage(StorageEvent::WriteResult {
            key: b"b".to_vec().into(),
        }));

        assert!(matches!(
            operation.finalize(),
            Err(PutRoutingError::InvalidStateEvent { .. })
        ));
    }

    #[test]
    fn reads_stored_rules() {
        let rules = vec![rule(RoutingTarget::Class("cold".to_string()))];
        let mut info = bucket();
        info.storage_routing = rules.clone();
        let mut operation = GetRoutingOperation::new("b".to_string());
        operation.start();

        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: b"b".to_vec().into(),
            value: Some(info.to_bytes().unwrap().into()),
        }));

        assert_eq!(operation.finalize().unwrap(), rules);
    }

    #[test]
    fn missing_bucket_errors() {
        let mut operation = GetRoutingOperation::new("b".to_string());
        operation.start();

        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: b"b".to_vec().into(),
            value: None,
        }));

        assert!(matches!(
            operation.finalize(),
            Err(GetRoutingError::NoSuchBucket)
        ));
    }
}
