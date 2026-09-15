use crate::groups::backends::{RecordReadError, index_prefix, parse_iter, parse_read};
use aruna_core::UserId;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::{GROUP_STORAGE_BACKEND_INDEX_KEYSPACE, GROUP_STORAGE_ROUTING_KEYSPACE};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::storage::routing::{
    GroupRoutingInputs, GroupStorageRouting, RoutingError, RoutingTarget, validate_tenant_target,
};
use aruna_core::structs::storage::group_backend::GroupStorage;
use aruna_core::types::{Effects, GroupId, Key};
use smallvec::smallvec;
use std::collections::BTreeSet;
use std::time::SystemTime;
use thiserror::Error;
use ulid::Ulid;

const BACKEND_PAGE_SIZE: usize = 128;

fn routing_key(group_id: GroupId) -> Key {
    group_id.to_bytes().to_vec().into()
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum LoadInputsState {
    Init,
    ReadDefault,
    ScanBackends,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum GroupInputsError {
    #[error(transparent)]
    Read(#[from] RecordReadError),
    #[error("group routing inputs never completed")]
    Incomplete,
}

/// Loads one group's routing inputs: its default write target and the ids of
/// the backends it registered. The scan is prefixed by group, so a write never
/// pays for another tenant's backends.
#[derive(Debug, PartialEq)]
pub struct GroupInputsOperation {
    group_id: GroupId,
    state: LoadInputsState,
    inputs: GroupRoutingInputs,
    output: Option<Result<GroupRoutingInputs, GroupInputsError>>,
}

impl GroupInputsOperation {
    pub fn new(group_id: GroupId) -> Self {
        Self {
            group_id,
            state: LoadInputsState::Init,
            inputs: GroupRoutingInputs::default(),
            output: None,
        }
    }

    fn scan_backends(&mut self, start_after: Option<Key>) -> Effects {
        self.state = LoadInputsState::ScanBackends;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: GROUP_STORAGE_BACKEND_INDEX_KEYSPACE.to_string(),
            prefix: Some(index_prefix(self.group_id)),
            start: start_after.map(IterStart::After),
            limit: BACKEND_PAGE_SIZE,
            txn_id: None,
        })]
    }

    fn fail(&mut self, error: GroupInputsError) -> Effects {
        self.state = LoadInputsState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }
}

impl Operation for GroupInputsOperation {
    type Output = GroupRoutingInputs;
    type Error = GroupInputsError;

    fn start(&mut self) -> Effects {
        self.state = LoadInputsState::ReadDefault;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: GROUP_STORAGE_ROUTING_KEYSPACE.to_string(),
            key: routing_key(self.group_id),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            LoadInputsState::Init => self.start(),
            LoadInputsState::ReadDefault => {
                match parse_read(event, GroupStorageRouting::from_bytes) {
                    Ok(record) => {
                        self.inputs.default_target = record.and_then(|it| it.default_target);
                    }
                    Err(error) => return self.fail(error.into()),
                }
                self.scan_backends(None)
            }
            LoadInputsState::ScanBackends => match parse_iter(event, GroupStorage::from_bytes) {
                Ok((records, next_start_after)) => {
                    self.inputs.backend_ids.extend(
                        records
                            .into_iter()
                            .filter(|record| record.group_id == self.group_id && !record.disabled)
                            .map(|record| record.backend_id),
                    );
                    if let Some(start_after) = next_start_after {
                        return self.scan_backends(Some(start_after));
                    }
                    self.state = LoadInputsState::Finish;
                    self.output = Some(Ok(std::mem::take(&mut self.inputs)));
                    smallvec![]
                }
                Err(error) => self.fail(error.into()),
            },
            LoadInputsState::Finish | LoadInputsState::Error => {
                self.fail(RecordReadError::Unexpected.into())
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, LoadInputsState::Finish | LoadInputsState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(GroupInputsError::Incomplete))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

/// Emits the sub-operation that loads a group's routing inputs.
pub fn load_group_inputs(group_id: GroupId) -> Effect {
    Effect::SubOperation(boxed_suboperation(
        GroupInputsOperation::new(group_id),
        |result| {
            Event::SubOperation(SubOperationEvent::GroupRoutingLoaded {
                result: result.map_err(|error| error.to_string()),
            })
        },
    ))
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum PutGroupState {
    Init,
    LoadInputs,
    WriteRecord,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum PutGroupError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    InvalidTarget(#[from] RoutingError),
    #[error("could not load the group's storage backends: {0}")]
    InputsUnavailable(String),
    #[error("group routing write did not finish")]
    NotFinished,
    #[error("Unexpected event in state {state:?}: expected {expected}, got {received:?}")]
    InvalidStateEvent {
        state: &'static str,
        expected: &'static str,
        received: Event,
    },
}

/// Writes the group's default write target. Clearing it is a write with
/// `default_target = None`, so the record always records who decided last.
#[derive(Debug, PartialEq)]
pub struct PutGroupOperation {
    record: GroupStorageRouting,
    state: PutGroupState,
    output: Option<Result<GroupStorageRouting, PutGroupError>>,
}

impl PutGroupOperation {
    pub fn new(
        group_id: GroupId,
        default_target: Option<RoutingTarget>,
        updated_by: UserId,
        updated_at: SystemTime,
    ) -> Self {
        Self {
            record: GroupStorageRouting {
                group_id,
                default_target,
                updated_at,
                updated_by,
            },
            state: PutGroupState::Init,
            output: None,
        }
    }

    fn fail(&mut self, err: PutGroupError) -> Effects {
        self.state = PutGroupState::Error;
        self.output = Some(Err(err));
        smallvec![]
    }

    fn write_record(&mut self, owned: &BTreeSet<Ulid>) -> Effects {
        if let Some(target) = self.record.default_target.as_ref()
            && let Err(error) = validate_tenant_target(target, owned)
        {
            return self.fail(error.into());
        }
        let value = match self.record.to_bytes() {
            Ok(value) => value,
            Err(err) => return self.fail(err.into()),
        };
        self.state = PutGroupState::WriteRecord;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: GROUP_STORAGE_ROUTING_KEYSPACE.to_string(),
            key: routing_key(self.record.group_id),
            value: value.into(),
            txn_id: None,
        })]
    }
}

impl Operation for PutGroupOperation {
    type Output = GroupStorageRouting;
    type Error = PutGroupError;

    fn start(&mut self) -> Effects {
        // A `Group` target is checked against the ids this group registered, so
        // the record can never name another tenant's backend.
        self.state = PutGroupState::LoadInputs;
        smallvec![load_group_inputs(self.record.group_id)]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            PutGroupState::Init => self.start(),
            PutGroupState::LoadInputs => {
                let Event::SubOperation(SubOperationEvent::GroupRoutingLoaded { result }) = event
                else {
                    return self.fail(PutGroupError::InvalidStateEvent {
                        state: "LoadInputs",
                        expected: "Event::SubOperation(SubOperationEvent::GroupRoutingLoaded)",
                        received: event,
                    });
                };
                match result {
                    Ok(inputs) => self.write_record(&inputs.backend_ids),
                    Err(error) => self.fail(PutGroupError::InputsUnavailable(error)),
                }
            }
            PutGroupState::WriteRecord => {
                let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
                    return self.fail(PutGroupError::InvalidStateEvent {
                        state: "WriteRecord",
                        expected: "Event::Storage(StorageEvent::WriteResult)",
                        received: event,
                    });
                };
                self.state = PutGroupState::Finish;
                self.output = Some(Ok(self.record.clone()));
                smallvec![]
            }
            PutGroupState::Finish | PutGroupState::Error => {
                self.fail(PutGroupError::InvalidStateEvent {
                    state: "terminal",
                    expected: "no event",
                    received: event,
                })
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, PutGroupState::Finish | PutGroupState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.output {
            Some(Ok(record)) => Ok(record),
            Some(Err(error)) => Err(error),
            None => Err(PutGroupError::NotFinished),
        }
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum GroupRoutingState {
    Init,
    ReadRecord,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum GroupRoutingError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("group routing read did not finish")]
    NotFinished,
    #[error("Unexpected event in state {state:?}: expected {expected}, got {received:?}")]
    InvalidStateEvent {
        state: &'static str,
        expected: &'static str,
        received: Event,
    },
}

/// Reads the group's default write target. An absent record is no default, not
/// an error: most groups never set one.
#[derive(Debug, PartialEq)]
pub struct GroupRoutingOperation {
    group_id: GroupId,
    state: GroupRoutingState,
    output: Option<Result<Option<GroupStorageRouting>, GroupRoutingError>>,
}

impl GroupRoutingOperation {
    pub fn new(group_id: GroupId) -> Self {
        Self {
            group_id,
            state: GroupRoutingState::Init,
            output: None,
        }
    }

    fn fail(&mut self, err: GroupRoutingError) -> Effects {
        self.state = GroupRoutingState::Error;
        self.output = Some(Err(err));
        smallvec![]
    }
}

impl Operation for GroupRoutingOperation {
    type Output = Option<GroupStorageRouting>;
    type Error = GroupRoutingError;

    fn start(&mut self) -> Effects {
        self.state = GroupRoutingState::ReadRecord;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: GROUP_STORAGE_ROUTING_KEYSPACE.to_string(),
            key: routing_key(self.group_id),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            GroupRoutingState::Init => self.start(),
            GroupRoutingState::ReadRecord => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.fail(GroupRoutingError::InvalidStateEvent {
                        state: "ReadRecord",
                        expected: "Event::Storage(StorageEvent::ReadResult)",
                        received: event,
                    });
                };
                let record = match value
                    .map(|value| GroupStorageRouting::from_bytes(value.as_ref()))
                    .transpose()
                {
                    Ok(record) => record,
                    Err(err) => return self.fail(err.into()),
                };
                self.state = GroupRoutingState::Finish;
                self.output = Some(Ok(record));
                smallvec![]
            }
            GroupRoutingState::Finish | GroupRoutingState::Error => {
                self.fail(GroupRoutingError::InvalidStateEvent {
                    state: "terminal",
                    expected: "no event",
                    received: event,
                })
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            GroupRoutingState::Finish | GroupRoutingState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(GroupRoutingError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod pure_tests {
    use super::{
        GroupInputsError, GroupInputsOperation, GroupRoutingError, GroupRoutingOperation,
        PutGroupError, PutGroupOperation, RecordReadError, routing_key,
    };
    use crate::groups::backends::{index_key, index_prefix};
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
    use aruna_core::keyspaces::{
        GROUP_STORAGE_BACKEND_INDEX_KEYSPACE, GROUP_STORAGE_ROUTING_KEYSPACE,
    };
    use aruna_core::operation::Operation;
    use aruna_core::structs::storage::blob::BackendRef;
    use aruna_core::structs::storage::group_backend::{GroupBackendKind, GroupStorage};
    use aruna_core::structs::storage::routing::{
        GroupRoutingInputs, GroupStorageRouting, RoutingError, RoutingTarget,
    };
    use aruna_core::types::Effects;
    use std::collections::{BTreeSet, HashMap};
    use std::time::SystemTime;
    use ulid::Ulid;

    fn group() -> Ulid {
        Ulid::from_bytes([2u8; 16])
    }

    /// Replays the loader sub-operation with the ids the group owns.
    fn loaded(operation: &mut PutGroupOperation, owned: BTreeSet<Ulid>) -> Effects {
        operation.start();
        operation.step(Event::SubOperation(SubOperationEvent::GroupRoutingLoaded {
            result: Ok(GroupRoutingInputs {
                default_target: None,
                backend_ids: owned,
            }),
        }))
    }

    fn backend(backend_id: Ulid, group_id: Ulid) -> GroupStorage {
        GroupStorage {
            backend_id,
            group_id,
            name: "tenant".to_string(),
            kind: GroupBackendKind::B2,
            public_config: HashMap::new(),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            created_by: aruna_core::UserId::default(),
            disabled: false,
            cleanup: aruna_core::structs::storage::cleanup::CleanupStrategy::Retain,
        }
    }

    fn record(target: Option<RoutingTarget>) -> GroupStorageRouting {
        GroupStorageRouting {
            group_id: group(),
            default_target: target,
            updated_at: SystemTime::UNIX_EPOCH,
            updated_by: aruna_core::UserId::default(),
        }
    }

    #[test]
    fn writes_group_default() {
        let target = RoutingTarget::Class("cold".to_string());
        let mut operation = PutGroupOperation::new(
            group(),
            Some(target.clone()),
            aruna_core::UserId::default(),
            SystemTime::UNIX_EPOCH,
        );

        let effects = loaded(&mut operation, BTreeSet::new());

        let [
            Effect::Storage(StorageEffect::Write {
                key_space,
                key,
                value,
                ..
            }),
        ] = effects.as_slice()
        else {
            panic!("expected one record write, got {effects:?}")
        };
        assert_eq!(key_space, GROUP_STORAGE_ROUTING_KEYSPACE);
        assert_eq!(key, &routing_key(group()));
        assert_eq!(
            GroupStorageRouting::from_bytes(value.as_ref()).unwrap(),
            record(Some(target))
        );
    }

    #[test]
    fn rejects_operator_target() {
        let mut operation = PutGroupOperation::new(
            group(),
            Some(RoutingTarget::Backend(BackendRef::Node("cold".to_string()))),
            aruna_core::UserId::default(),
            SystemTime::UNIX_EPOCH,
        );

        let effects = loaded(&mut operation, BTreeSet::new());

        assert!(effects.is_empty());
        assert!(operation.is_complete());
        assert!(matches!(
            operation.finalize(),
            Err(PutGroupError::InvalidTarget(
                RoutingError::OperatorBackendTarget
            ))
        ));
    }

    #[test]
    fn rejects_unexpected_event() {
        let mut operation = PutGroupOperation::new(
            group(),
            None,
            aruna_core::UserId::default(),
            SystemTime::UNIX_EPOCH,
        );
        loaded(&mut operation, BTreeSet::new());

        operation.step(Event::Storage(StorageEvent::DeleteResult {
            key: routing_key(group()),
        }));

        assert!(matches!(
            operation.finalize(),
            Err(PutGroupError::InvalidStateEvent { .. })
        ));
    }

    #[test]
    fn rejects_foreign_backend() {
        // A default naming a backend this group does not own must not be stored.
        let foreign = Ulid::from_bytes([9u8; 16]);
        let mut operation = PutGroupOperation::new(
            group(),
            Some(RoutingTarget::Backend(BackendRef::Group(foreign))),
            aruna_core::UserId::default(),
            SystemTime::UNIX_EPOCH,
        );

        let effects = loaded(
            &mut operation,
            BTreeSet::from([Ulid::from_bytes([4u8; 16])]),
        );

        assert!(effects.is_empty(), "expected no write, got {effects:?}");
        assert_eq!(
            operation.finalize(),
            Err(PutGroupError::InvalidTarget(RoutingError::ForeignBackend(
                foreign
            )))
        );
    }

    #[test]
    fn loads_own_backends() {
        // The scan is prefixed by group, and a disabled backend cannot be routed to.
        let mine = Ulid::from_bytes([4u8; 16]);
        let disabled = Ulid::from_bytes([5u8; 16]);
        let mut operation = GroupInputsOperation::new(group());
        operation.start();

        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: routing_key(group()),
            value: Some(
                record(Some(RoutingTarget::Class("cold".to_string())))
                    .to_bytes()
                    .unwrap()
                    .into(),
            ),
        }));

        let [
            Effect::Storage(StorageEffect::Iter {
                key_space,
                prefix: Some(prefix),
                ..
            }),
        ] = effects.as_slice()
        else {
            panic!("expected a prefixed backend scan, got {effects:?}")
        };
        assert_eq!(key_space, GROUP_STORAGE_BACKEND_INDEX_KEYSPACE);
        assert_eq!(prefix, &index_prefix(group()));

        let mut leaving = backend(disabled, group());
        leaving.disabled = true;
        operation.step(Event::Storage(StorageEvent::IterResult {
            values: vec![
                (
                    index_key(group(), mine),
                    backend(mine, group()).to_bytes().unwrap().into(),
                ),
                (
                    index_key(group(), disabled),
                    leaving.to_bytes().unwrap().into(),
                ),
            ],
            next_start_after: None,
        }));

        assert_eq!(
            operation.finalize().unwrap(),
            GroupRoutingInputs {
                default_target: Some(RoutingTarget::Class("cold".to_string())),
                backend_ids: BTreeSet::from([mine]),
            }
        );
    }

    #[test]
    fn reads_absent_record() {
        let mut operation = GroupRoutingOperation::new(group());
        operation.start();

        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: routing_key(group()),
            value: None,
        }));

        assert_eq!(operation.finalize().unwrap(), None);
    }

    #[test]
    fn reads_stored_target() {
        let stored = record(Some(RoutingTarget::Class("cold".to_string())));
        let mut operation = GroupRoutingOperation::new(group());
        operation.start();

        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: routing_key(group()),
            value: Some(stored.to_bytes().unwrap().into()),
        }));

        assert_eq!(operation.finalize().unwrap(), Some(stored));
    }

    // A state that expects no event must reject one instead of ignoring it.
    #[test]
    fn terminal_rejects_event() {
        let stray = || {
            Event::Storage(StorageEvent::DeleteResult {
                key: routing_key(group()),
            })
        };

        let mut inputs = GroupInputsOperation::new(group());
        inputs.start();
        inputs.step(Event::Storage(StorageEvent::ReadResult {
            key: routing_key(group()),
            value: None,
        }));
        inputs.step(Event::Storage(StorageEvent::IterResult {
            values: Vec::new(),
            next_start_after: None,
        }));
        inputs.step(stray());
        assert!(matches!(
            inputs.finalize(),
            Err(GroupInputsError::Read(RecordReadError::Unexpected))
        ));

        let mut put = PutGroupOperation::new(
            group(),
            None,
            aruna_core::UserId::default(),
            SystemTime::UNIX_EPOCH,
        );
        loaded(&mut put, BTreeSet::new());
        put.step(Event::Storage(StorageEvent::WriteResult {
            key: routing_key(group()),
        }));
        put.step(stray());
        assert!(matches!(
            put.finalize(),
            Err(PutGroupError::InvalidStateEvent { .. })
        ));

        let mut get = GroupRoutingOperation::new(group());
        get.start();
        get.step(Event::Storage(StorageEvent::ReadResult {
            key: routing_key(group()),
            value: None,
        }));
        get.step(stray());
        assert!(matches!(
            get.finalize(),
            Err(GroupRoutingError::InvalidStateEvent { .. })
        ));
    }
}
