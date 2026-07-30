use super::{RecordReadError, backend_key, parse_read};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    GROUP_STORAGE_BACKEND_KEYSPACE, GROUP_STORAGE_BACKEND_SECRET_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::GroupStorageBackend;
use aruna_core::types::{Effects, GroupId};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, Eq, PartialEq)]
enum DeleteState {
    Init,
    ReadRecord,
    DeleteRecords,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum DeleteGroupBackendError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    Read(#[from] RecordReadError),
    #[error("storage backend not found")]
    NotFound,
    #[error("DeleteGroupBackend failed")]
    Failed,
    #[error("State [{state:?}] invalid: expected [{expected}] - received [{received:?}]")]
    InvalidStateEvent {
        state: &'static str,
        expected: &'static str,
        received: Event,
    },
}

/// Removes a tenant backend and its credentials together. Objects already
/// written there keep their stamped reference and stop resolving, which is the
/// intended loud failure rather than a silent reroute.
#[derive(Debug, PartialEq)]
pub struct DeleteGroupBackendOperation {
    group_id: GroupId,
    backend_id: Ulid,
    state: DeleteState,
    output: Option<Result<(), DeleteGroupBackendError>>,
}

impl DeleteGroupBackendOperation {
    pub fn new(group_id: GroupId, backend_id: Ulid) -> Self {
        Self {
            group_id,
            backend_id,
            state: DeleteState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: DeleteGroupBackendError) -> Effects {
        self.state = DeleteState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }
}

impl Operation for DeleteGroupBackendOperation {
    type Output = ();
    type Error = DeleteGroupBackendError;

    fn start(&mut self) -> Effects {
        self.state = DeleteState::ReadRecord;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: GROUP_STORAGE_BACKEND_KEYSPACE.to_string(),
            key: backend_key(self.backend_id),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            DeleteState::Init => self.start(),
            DeleteState::ReadRecord => {
                let record = match parse_read(event, GroupStorageBackend::from_bytes) {
                    Ok(record) => record,
                    Err(error) => return self.fail(error.into()),
                };
                match record {
                    Some(record) if record.group_id == self.group_id => {}
                    _ => return self.fail(DeleteGroupBackendError::NotFound),
                }
                self.state = DeleteState::DeleteRecords;
                smallvec![Effect::Storage(StorageEffect::BatchDelete {
                    deletes: vec![
                        (
                            GROUP_STORAGE_BACKEND_KEYSPACE.to_string(),
                            backend_key(self.backend_id),
                        ),
                        (
                            GROUP_STORAGE_BACKEND_SECRET_KEYSPACE.to_string(),
                            backend_key(self.backend_id),
                        ),
                    ],
                    txn_id: None,
                })]
            }
            DeleteState::DeleteRecords => {
                let Event::Storage(StorageEvent::BatchDeleteResult { .. }) = event else {
                    return self.fail(DeleteGroupBackendError::InvalidStateEvent {
                        state: "DeleteRecords",
                        expected: "Event::Storage(StorageEvent::BatchDeleteResult)",
                        received: event,
                    });
                };
                self.state = DeleteState::Finish;
                self.output = Some(Ok(()));
                smallvec![]
            }
            DeleteState::Finish | DeleteState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, DeleteState::Finish | DeleteState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(DeleteGroupBackendError::Failed))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::{DeleteGroupBackendError, DeleteGroupBackendOperation};
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::operation::Operation;
    use aruna_core::structs::{GroupBackendKind, GroupStorageBackend};
    use std::collections::HashMap;
    use std::time::SystemTime;
    use ulid::Ulid;

    fn record(group_id: Ulid) -> GroupStorageBackend {
        GroupStorageBackend {
            backend_id: Ulid::from_bytes([9u8; 16]),
            group_id,
            name: "tenant".to_string(),
            kind: GroupBackendKind::Gcs,
            public_config: HashMap::new(),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            created_by: aruna_core::UserId::default(),
        }
    }

    #[test]
    fn deletes_both_records() {
        let group_id = Ulid::from_bytes([1u8; 16]);
        let mut operation = DeleteGroupBackendOperation::new(group_id, Ulid::from_bytes([9u8; 16]));
        operation.start();

        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: b"x".to_vec().into(),
            value: Some(record(group_id).to_bytes().unwrap().into()),
        }));

        let [Effect::Storage(StorageEffect::BatchDelete { deletes, .. })] = effects.as_slice()
        else {
            panic!("expected one batch delete, got {effects:?}")
        };
        assert_eq!(deletes.len(), 2);
    }

    #[test]
    fn rejects_foreign_group() {
        // The key carries no group, so the record's own group is the gate.
        let mut operation = DeleteGroupBackendOperation::new(
            Ulid::from_bytes([1u8; 16]),
            Ulid::from_bytes([9u8; 16]),
        );
        operation.start();

        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: b"x".to_vec().into(),
            value: Some(
                record(Ulid::from_bytes([2u8; 16]))
                    .to_bytes()
                    .unwrap()
                    .into(),
            ),
        }));

        assert_eq!(operation.finalize(), Err(DeleteGroupBackendError::NotFound));
    }
}
