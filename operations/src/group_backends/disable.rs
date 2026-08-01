use super::{RecordReadError, backend_key, parse_read, record_writes};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::GROUP_STORAGE_BACKEND_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::GroupStorageBackend;
use aruna_core::types::{Effects, GroupId, TxnId};
use smallvec::smallvec;
use std::time::SystemTime;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, Eq, PartialEq)]
enum DisableState {
    Init,
    StartTransaction,
    ReadRecord,
    WriteRecord,
    CommitTransaction,
    AbortTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum SetDisabledError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Read(#[from] RecordReadError),
    #[error("storage backend not found")]
    NotFound,
    #[error("SetDisabled failed")]
    Failed,
    #[error("State [{state:?}] invalid: expected [{expected}] - received [{received:?}]")]
    InvalidStateEvent {
        state: &'static str,
        expected: &'static str,
        received: Event,
    },
}

/// Turns a tenant backend's writes off or back on. Reads keep working either
/// way: the record and its credentials stay, so objects already stored on the
/// backend still resolve and queued cleanups still reach it.
#[derive(Debug, PartialEq)]
pub struct SetDisabledOperation {
    group_id: GroupId,
    backend_id: Ulid,
    disabled: bool,
    state: DisableState,
    txn_id: Option<TxnId>,
    output: Option<Result<GroupStorageBackend, SetDisabledError>>,
}

impl SetDisabledOperation {
    pub fn new(group_id: GroupId, backend_id: Ulid, disabled: bool) -> Self {
        Self {
            group_id,
            backend_id,
            disabled,
            state: DisableState::Init,
            txn_id: None,
            output: None,
        }
    }

    fn fail(&mut self, error: SetDisabledError) -> Effects {
        self.output = Some(Err(error));
        if let Some(txn_id) = self.txn_id.take() {
            self.state = DisableState::AbortTransaction;
            return smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })];
        }
        self.state = DisableState::Error;
        smallvec![]
    }

    fn handle_txn_started(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                self.txn_id = Some(txn_id);
                self.state = DisableState::ReadRecord;
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: GROUP_STORAGE_BACKEND_KEYSPACE.to_string(),
                    key: backend_key(self.backend_id),
                    txn_id: self.txn_id,
                })]
            }
            Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
            received => self.fail(SetDisabledError::InvalidStateEvent {
                state: "StartTransaction",
                expected: "Event::Storage(StorageEvent::TransactionStarted)",
                received,
            }),
        }
    }

    /// The key carries no group, so the record's own group is the gate. An
    /// unchanged flag commits nothing, which makes repeating the call harmless.
    fn handle_record(&mut self, event: Event) -> Effects {
        let record = match parse_read(event, GroupStorageBackend::from_bytes) {
            Ok(Some(record)) if record.group_id == self.group_id => record,
            Ok(_) => return self.fail(SetDisabledError::NotFound),
            Err(error) => return self.fail(error.into()),
        };
        if record.disabled == self.disabled {
            self.output = Some(Ok(record));
            return self.commit();
        }

        // The stamp is what removal waits on: a writer that resolved the
        // backend just before this commit needs its credentials to survive.
        let updated = GroupStorageBackend {
            disabled: self.disabled,
            updated_at: SystemTime::now(),
            ..record
        };
        let writes = match record_writes(&updated) {
            Ok(writes) => writes,
            Err(error) => return self.fail(error.into()),
        };
        self.output = Some(Ok(updated));
        self.state = DisableState::WriteRecord;
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: self.txn_id,
        })]
    }

    fn handle_written(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::BatchWriteResult { .. }) => self.commit(),
            Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
            received => self.fail(SetDisabledError::InvalidStateEvent {
                state: "WriteRecord",
                expected: "Event::Storage(StorageEvent::BatchWriteResult)",
                received,
            }),
        }
    }

    fn commit(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(SetDisabledError::Failed);
        };
        self.state = DisableState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_committed(&mut self, event: Event) -> Effects {
        self.txn_id = None;
        match event {
            Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                self.state = DisableState::Finish;
                smallvec![]
            }
            Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
            received => self.fail(SetDisabledError::InvalidStateEvent {
                state: "CommitTransaction",
                expected: "Event::Storage(StorageEvent::TransactionCommitted)",
                received,
            }),
        }
    }

    fn handle_aborted(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::TransactionAborted { .. })
            | Event::Storage(StorageEvent::Error { .. }) => {
                self.state = DisableState::Error;
                smallvec![]
            }
            received => self.fail(SetDisabledError::InvalidStateEvent {
                state: "AbortTransaction",
                expected: "Event::Storage(StorageEvent::TransactionAborted)",
                received,
            }),
        }
    }
}

impl Operation for SetDisabledOperation {
    type Output = GroupStorageBackend;
    type Error = SetDisabledError;

    fn start(&mut self) -> Effects {
        self.state = DisableState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            DisableState::Init => self.start(),
            DisableState::StartTransaction => self.handle_txn_started(event),
            DisableState::ReadRecord => self.handle_record(event),
            DisableState::WriteRecord => self.handle_written(event),
            DisableState::CommitTransaction => self.handle_committed(event),
            DisableState::AbortTransaction => self.handle_aborted(event),
            DisableState::Finish | DisableState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, DisableState::Finish | DisableState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.state {
            DisableState::Finish => self.output.unwrap_or(Err(SetDisabledError::Failed)),
            _ => match self.output {
                Some(Err(error)) => Err(error),
                _ => Err(SetDisabledError::Failed),
            },
        }
    }

    fn abort(&mut self) -> Effects {
        if let Some(txn_id) = self.txn_id.take() {
            self.state = DisableState::AbortTransaction;
            return smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })];
        }
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::{SetDisabledError, SetDisabledOperation};
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{
        GROUP_STORAGE_BACKEND_INDEX_KEYSPACE, GROUP_STORAGE_BACKEND_KEYSPACE,
    };
    use aruna_core::operation::Operation;
    use aruna_core::structs::{GroupBackendKind, GroupStorageBackend};
    use aruna_core::types::{Effects, TxnId};
    use std::collections::HashMap;
    use std::time::SystemTime;
    use ulid::Ulid;

    fn backend_id() -> Ulid {
        Ulid::from_bytes([9u8; 16])
    }

    fn record(group_id: Ulid, disabled: bool) -> GroupStorageBackend {
        GroupStorageBackend {
            backend_id: backend_id(),
            group_id,
            name: "tenant".to_string(),
            kind: GroupBackendKind::Gcs,
            public_config: HashMap::new(),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            created_by: aruna_core::UserId::default(),
            disabled,
            cleanup: aruna_core::structs::CleanupStrategy::Retain,
        }
    }

    /// Drives the operation to the record read inside its transaction.
    fn reading(operation: &mut SetDisabledOperation) {
        operation.start();
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TxnId::from(7),
        }));
    }

    fn read_result(stored: GroupStorageBackend) -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: b"x".to_vec().into(),
            value: Some(stored.to_bytes().unwrap().into()),
        })
    }

    fn written(operation: &mut SetDisabledOperation) -> Effects {
        operation.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }))
    }

    fn committed(operation: &mut SetDisabledOperation) {
        operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id: TxnId::from(7),
        }));
    }

    #[test]
    fn disable_stamps_record() {
        // Removal waits on this stamp, so a disable has to move it.
        let group_id = Ulid::from_bytes([1u8; 16]);
        let mut operation = SetDisabledOperation::new(group_id, backend_id(), true);
        reading(&mut operation);

        operation.step(read_result(record(group_id, false)));
        written(&mut operation);
        committed(&mut operation);

        assert!(operation.finalize().unwrap().updated_at > SystemTime::UNIX_EPOCH);
    }

    #[test]
    fn disable_sets_flag() {
        // Both copies of the record must carry the flag, or routing disagrees.
        let group_id = Ulid::from_bytes([1u8; 16]);
        let mut operation = SetDisabledOperation::new(group_id, backend_id(), true);
        reading(&mut operation);

        let effects = operation.step(read_result(record(group_id, false)));

        let [Effect::Storage(StorageEffect::BatchWrite { writes, txn_id })] = effects.as_slice()
        else {
            panic!("expected one transacted write, got {effects:?}")
        };
        assert_eq!(*txn_id, Some(TxnId::from(7)));
        assert_eq!(
            writes
                .iter()
                .map(|(key_space, ..)| key_space.as_str())
                .collect::<Vec<_>>(),
            [
                GROUP_STORAGE_BACKEND_KEYSPACE,
                GROUP_STORAGE_BACKEND_INDEX_KEYSPACE
            ]
        );
        assert!(writes.iter().all(|(.., value)| {
            GroupStorageBackend::from_bytes(value.as_ref())
                .unwrap()
                .disabled
        }));

        written(&mut operation);
        committed(&mut operation);
        assert!(operation.finalize().unwrap().disabled);
    }

    #[test]
    fn disable_is_idempotent() {
        // Repeating the call must succeed without writing anything.
        let group_id = Ulid::from_bytes([1u8; 16]);
        let mut operation = SetDisabledOperation::new(group_id, backend_id(), true);
        reading(&mut operation);

        let effects = operation.step(read_result(record(group_id, true)));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::CommitTransaction { .. })]
        ));
        committed(&mut operation);
        assert!(operation.finalize().unwrap().disabled);
    }

    #[test]
    fn enable_clears_flag() {
        let group_id = Ulid::from_bytes([1u8; 16]);
        let mut operation = SetDisabledOperation::new(group_id, backend_id(), false);
        reading(&mut operation);

        operation.step(read_result(record(group_id, true)));
        written(&mut operation);
        committed(&mut operation);

        assert!(!operation.finalize().unwrap().disabled);
    }

    #[test]
    fn rejects_foreign_group() {
        let mut operation =
            SetDisabledOperation::new(Ulid::from_bytes([1u8; 16]), backend_id(), true);
        reading(&mut operation);

        let effects = operation.step(read_result(record(Ulid::from_bytes([2u8; 16]), false)));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { .. })]
        ));
        operation.step(Event::Storage(StorageEvent::TransactionAborted {
            txn_id: TxnId::from(7),
        }));
        assert_eq!(operation.finalize(), Err(SetDisabledError::NotFound));
    }

    #[test]
    fn rejects_stray_event() {
        // A write acknowledgement is not a transaction start.
        let mut operation =
            SetDisabledOperation::new(Ulid::from_bytes([1u8; 16]), backend_id(), true);
        operation.start();

        operation.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));

        assert!(matches!(
            operation.finalize(),
            Err(SetDisabledError::InvalidStateEvent { .. })
        ));
    }
}
