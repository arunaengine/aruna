use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::GROUP_STORAGE_ROUTING_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::{
    GroupStorageRouting, RoutingError, RoutingTarget, validate_tenant_target,
};
use aruna_core::types::{Effects, GroupId, Key, UserId};
use smallvec::smallvec;
use std::time::SystemTime;
use thiserror::Error;

fn routing_key(group_id: GroupId) -> Key {
    group_id.to_bytes().to_vec().into()
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum PutGroupRoutingState {
    Init,
    WriteRecord,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum PutGroupRoutingError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    InvalidTarget(#[from] RoutingError),
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
pub struct PutGroupRoutingOperation {
    record: GroupStorageRouting,
    state: PutGroupRoutingState,
    output: Option<Result<GroupStorageRouting, PutGroupRoutingError>>,
}

impl PutGroupRoutingOperation {
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
            state: PutGroupRoutingState::Init,
            output: None,
        }
    }

    fn fail(&mut self, err: PutGroupRoutingError) -> Effects {
        self.state = PutGroupRoutingState::Error;
        self.output = Some(Err(err));
        smallvec![]
    }
}

impl Operation for PutGroupRoutingOperation {
    type Output = Option<Result<GroupStorageRouting, PutGroupRoutingError>>;
    type Error = PutGroupRoutingError;

    fn start(&mut self) -> Effects {
        if let Some(target) = self.record.default_target.as_ref()
            && let Err(error) = validate_tenant_target(target)
        {
            return self.fail(error.into());
        }
        let value = match self.record.to_bytes() {
            Ok(value) => value,
            Err(err) => return self.fail(err.into()),
        };
        self.state = PutGroupRoutingState::WriteRecord;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: GROUP_STORAGE_ROUTING_KEYSPACE.to_string(),
            key: routing_key(self.record.group_id),
            value: value.into(),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            PutGroupRoutingState::Init => self.start(),
            PutGroupRoutingState::WriteRecord => {
                let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
                    return self.fail(PutGroupRoutingError::InvalidStateEvent {
                        state: "WriteRecord",
                        expected: "Event::Storage(StorageEvent::WriteResult)",
                        received: event,
                    });
                };
                self.state = PutGroupRoutingState::Finish;
                self.output = Some(Ok(self.record.clone()));
                smallvec![]
            }
            PutGroupRoutingState::Finish | PutGroupRoutingState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            PutGroupRoutingState::Finish | PutGroupRoutingState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == PutGroupRoutingState::Error
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

#[derive(Clone, Debug, Eq, PartialEq)]
enum GetGroupRoutingState {
    Init,
    ReadRecord,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum GetGroupRoutingError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
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
pub struct GetGroupRoutingOperation {
    group_id: GroupId,
    state: GetGroupRoutingState,
    output: Option<Result<Option<GroupStorageRouting>, GetGroupRoutingError>>,
}

impl GetGroupRoutingOperation {
    pub fn new(group_id: GroupId) -> Self {
        Self {
            group_id,
            state: GetGroupRoutingState::Init,
            output: None,
        }
    }

    fn fail(&mut self, err: GetGroupRoutingError) -> Effects {
        self.state = GetGroupRoutingState::Error;
        self.output = Some(Err(err));
        smallvec![]
    }
}

impl Operation for GetGroupRoutingOperation {
    type Output = Option<Result<Option<GroupStorageRouting>, GetGroupRoutingError>>;
    type Error = GetGroupRoutingError;

    fn start(&mut self) -> Effects {
        self.state = GetGroupRoutingState::ReadRecord;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: GROUP_STORAGE_ROUTING_KEYSPACE.to_string(),
            key: routing_key(self.group_id),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            GetGroupRoutingState::Init => self.start(),
            GetGroupRoutingState::ReadRecord => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.fail(GetGroupRoutingError::InvalidStateEvent {
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
                self.state = GetGroupRoutingState::Finish;
                self.output = Some(Ok(record));
                smallvec![]
            }
            GetGroupRoutingState::Finish | GetGroupRoutingState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            GetGroupRoutingState::Finish | GetGroupRoutingState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == GetGroupRoutingState::Error
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
        GetGroupRoutingOperation, PutGroupRoutingError, PutGroupRoutingOperation, routing_key,
    };
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::GROUP_STORAGE_ROUTING_KEYSPACE;
    use aruna_core::operation::Operation;
    use aruna_core::structs::{BackendRef, GroupStorageRouting, RoutingError, RoutingTarget};
    use std::time::SystemTime;
    use ulid::Ulid;

    fn group() -> Ulid {
        Ulid::from_bytes([2u8; 16])
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
        let mut operation = PutGroupRoutingOperation::new(
            group(),
            Some(target.clone()),
            aruna_core::UserId::default(),
            SystemTime::UNIX_EPOCH,
        );

        let effects = operation.start();

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
        let mut operation = PutGroupRoutingOperation::new(
            group(),
            Some(RoutingTarget::Backend(BackendRef::Node("cold".to_string()))),
            aruna_core::UserId::default(),
            SystemTime::UNIX_EPOCH,
        );

        let effects = operation.start();

        assert!(effects.is_empty());
        assert!(operation.is_complete());
        assert!(matches!(
            operation.finalize(),
            Err(PutGroupRoutingError::InvalidTarget(
                RoutingError::OperatorBackendTarget
            ))
        ));
    }

    #[test]
    fn rejects_unexpected_event() {
        let mut operation = PutGroupRoutingOperation::new(
            group(),
            None,
            aruna_core::UserId::default(),
            SystemTime::UNIX_EPOCH,
        );
        operation.start();

        operation.step(Event::Storage(StorageEvent::DeleteResult {
            key: routing_key(group()),
        }));

        assert!(matches!(
            operation.finalize(),
            Err(PutGroupRoutingError::InvalidStateEvent { .. })
        ));
    }

    #[test]
    fn reads_absent_record() {
        let mut operation = GetGroupRoutingOperation::new(group());
        operation.start();

        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: routing_key(group()),
            value: None,
        }));

        assert_eq!(operation.finalize().unwrap(), Some(Ok(None)));
    }

    #[test]
    fn reads_stored_target() {
        let stored = record(Some(RoutingTarget::Class("cold".to_string())));
        let mut operation = GetGroupRoutingOperation::new(group());
        operation.start();

        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: routing_key(group()),
            value: Some(stored.to_bytes().unwrap().into()),
        }));

        assert_eq!(operation.finalize().unwrap(), Some(Ok(Some(stored))));
    }
}
