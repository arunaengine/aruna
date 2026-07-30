use super::{RecordReadError, backend_key, parse_iter, parse_read};
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_LOCATIONS_KEYSPACE, GROUP_STORAGE_BACKEND_KEYSPACE, GROUP_STORAGE_BACKEND_SECRET_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::{BackendLocation, BackendRef, GroupStorageBackend};
use aruna_core::types::{Effects, GroupId, Key, TxnId};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

const LOCATION_SCAN_PAGE_SIZE: usize = 128;

#[derive(Clone, Debug, Eq, PartialEq)]
enum DeleteState {
    Init,
    ReadRecord,
    StartTransaction,
    ScanLocations,
    DeleteRecords,
    CommitTransaction,
    AbortTransaction,
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
    #[error("storage backend still holds stored object data")]
    StillReferenced,
    #[error("DeleteGroupBackend failed")]
    Failed,
    #[error("State [{state:?}] invalid: expected [{expected}] - received [{received:?}]")]
    InvalidStateEvent {
        state: &'static str,
        expected: &'static str,
        received: Event,
    },
}

/// Removes a tenant backend and its credentials together. Deletion is refused
/// while any stored location still names the backend, so no object is ever
/// stranded behind an unresolvable reference.
#[derive(Debug, PartialEq)]
pub struct DeleteGroupBackendOperation {
    group_id: GroupId,
    backend_id: Ulid,
    state: DeleteState,
    txn_id: Option<TxnId>,
    output: Option<Result<(), DeleteGroupBackendError>>,
}

impl DeleteGroupBackendOperation {
    pub fn new(group_id: GroupId, backend_id: Ulid) -> Self {
        Self {
            group_id,
            backend_id,
            state: DeleteState::Init,
            txn_id: None,
            output: None,
        }
    }

    fn fail(&mut self, error: DeleteGroupBackendError) -> Effects {
        self.output = Some(Err(error));
        if let Some(txn_id) = self.txn_id.take() {
            self.state = DeleteState::AbortTransaction;
            return smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })];
        }
        self.state = DeleteState::Error;
        smallvec![]
    }

    /// The scan spans the whole locations keyspace, so it stays outside the
    /// write transaction: joining it to the read set would conflict with every
    /// concurrent blob write.
    fn scan_locations(&mut self, start_after: Option<Key>) -> Effects {
        self.state = DeleteState::ScanLocations;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: BLOB_LOCATIONS_KEYSPACE.to_string(),
            prefix: None,
            start: start_after.map(IterStart::After),
            limit: LOCATION_SCAN_PAGE_SIZE,
            txn_id: None,
        })]
    }

    fn handle_record_read(&mut self, event: Event) -> Effects {
        let record = match parse_read(event, GroupStorageBackend::from_bytes) {
            Ok(record) => record,
            Err(error) => return self.fail(error.into()),
        };
        match record {
            Some(record) if record.group_id == self.group_id => {}
            _ => return self.fail(DeleteGroupBackendError::NotFound),
        }
        self.scan_locations(None)
    }

    fn start_transaction(&mut self) -> Effects {
        self.state = DeleteState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn handle_txn_started(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                self.txn_id = Some(txn_id);
                self.delete_records()
            }
            Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
            received => self.fail(DeleteGroupBackendError::InvalidStateEvent {
                state: "StartTransaction",
                expected: "Event::Storage(StorageEvent::TransactionStarted)",
                received,
            }),
        }
    }

    fn handle_locations_scanned(&mut self, event: Event) -> Effects {
        let (locations, next_start_after) = match parse_iter(event, BackendLocation::from_bytes) {
            Ok(page) => page,
            Err(error) => return self.fail(error.into()),
        };

        if locations
            .iter()
            .any(|location| location.backend == BackendRef::Group(self.backend_id))
        {
            return self.fail(DeleteGroupBackendError::StillReferenced);
        }

        if let Some(start_after) = next_start_after {
            return self.scan_locations(Some(start_after));
        }

        self.start_transaction()
    }

    fn delete_records(&mut self) -> Effects {
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
            txn_id: self.txn_id,
        })]
    }

    fn handle_records_deleted(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::BatchDeleteResult { .. }) => {}
            Event::Storage(StorageEvent::Error { error }) => return self.fail(error.into()),
            received => {
                return self.fail(DeleteGroupBackendError::InvalidStateEvent {
                    state: "DeleteRecords",
                    expected: "Event::Storage(StorageEvent::BatchDeleteResult)",
                    received,
                });
            }
        }

        let Some(txn_id) = self.txn_id else {
            self.state = DeleteState::Finish;
            self.output = Some(Ok(()));
            return smallvec![];
        };
        self.state = DeleteState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_txn_committed(&mut self, event: Event) -> Effects {
        self.txn_id = None;
        match event {
            Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                self.state = DeleteState::Finish;
                self.output = Some(Ok(()));
                smallvec![]
            }
            Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
            received => self.fail(DeleteGroupBackendError::InvalidStateEvent {
                state: "CommitTransaction",
                expected: "Event::Storage(StorageEvent::TransactionCommitted)",
                received,
            }),
        }
    }

    fn handle_txn_aborted(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::TransactionAborted { .. })
            | Event::Storage(StorageEvent::Error { .. }) => {
                self.state = DeleteState::Error;
                smallvec![]
            }
            received => self.fail(DeleteGroupBackendError::InvalidStateEvent {
                state: "AbortTransaction",
                expected: "Event::Storage(StorageEvent::TransactionAborted)",
                received,
            }),
        }
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
            DeleteState::ReadRecord => self.handle_record_read(event),
            DeleteState::StartTransaction => self.handle_txn_started(event),
            DeleteState::ScanLocations => self.handle_locations_scanned(event),
            DeleteState::DeleteRecords => self.handle_records_deleted(event),
            DeleteState::CommitTransaction => self.handle_txn_committed(event),
            DeleteState::AbortTransaction => self.handle_txn_aborted(event),
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
        if let Some(txn_id) = self.txn_id.take() {
            self.state = DeleteState::AbortTransaction;
            return smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })];
        }
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::{DeleteGroupBackendError, DeleteGroupBackendOperation};
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::operation::Operation;
    use aruna_core::structs::{BackendLocation, BackendRef, GroupBackendKind, GroupStorageBackend};
    use aruna_core::types::TxnId;
    use std::collections::HashMap;
    use std::time::SystemTime;
    use ulid::Ulid;

    fn backend_id() -> Ulid {
        Ulid::from_bytes([9u8; 16])
    }

    fn record(group_id: Ulid) -> GroupStorageBackend {
        GroupStorageBackend {
            backend_id: backend_id(),
            group_id,
            name: "tenant".to_string(),
            kind: GroupBackendKind::Gcs,
            public_config: HashMap::new(),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            created_by: aruna_core::UserId::default(),
        }
    }

    fn location(backend: BackendRef) -> BackendLocation {
        BackendLocation {
            backend,
            storage_class: None,
            root: "root".to_string(),
            storage_bucket: "bucket".to_string(),
            backend_path: "bucket/object".to_string(),
            ulid: Ulid::from_bytes([3u8; 16]),
            compressed: false,
            encrypted: false,
            created_by: aruna_core::UserId::default(),
            created_at: SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 1,
            hashes: HashMap::new(),
        }
    }

    fn scanning(group_id: Ulid) -> DeleteGroupBackendOperation {
        let mut operation = DeleteGroupBackendOperation::new(group_id, backend_id());
        operation.start();
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: b"x".to_vec().into(),
            value: Some(record(group_id).to_bytes().unwrap().into()),
        }));
        operation
    }

    fn page(locations: Vec<BackendLocation>) -> Event {
        Event::Storage(StorageEvent::IterResult {
            values: locations
                .into_iter()
                .map(|location| (b"k".to_vec().into(), location.to_bytes().unwrap().into()))
                .collect(),
            next_start_after: None,
        })
    }

    #[test]
    fn scan_avoids_transaction() {
        // A keyspace-wide read set would conflict with every concurrent write.
        let mut operation =
            DeleteGroupBackendOperation::new(Ulid::from_bytes([1u8; 16]), backend_id());
        operation.start();

        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: b"x".to_vec().into(),
            value: Some(
                record(Ulid::from_bytes([1u8; 16]))
                    .to_bytes()
                    .unwrap()
                    .into(),
            ),
        }));

        let [Effect::Storage(StorageEffect::Iter { txn_id: None, .. })] = effects.as_slice() else {
            panic!("expected an untransacted location scan, got {effects:?}")
        };
    }

    #[test]
    fn refuses_referenced_backend() {
        let group_id = Ulid::from_bytes([1u8; 16]);
        let mut operation = scanning(group_id);

        let effects = operation.step(page(vec![location(BackendRef::Group(backend_id()))]));

        assert!(effects.is_empty(), "expected no cleanup, got {effects:?}");
        assert!(operation.is_complete());
        assert_eq!(
            operation.finalize(),
            Err(DeleteGroupBackendError::StillReferenced)
        );
    }

    #[test]
    fn deletes_both_records() {
        let group_id = Ulid::from_bytes([1u8; 16]);
        let mut operation = scanning(group_id);

        let effects = operation.step(page(vec![
            location(BackendRef::node_default()),
            location(BackendRef::Group(Ulid::from_bytes([8u8; 16]))),
        ]));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::StartTransaction {
                read: false
            })]
        ));
        let effects = operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TxnId::from(7),
        }));

        let [Effect::Storage(StorageEffect::BatchDelete { deletes, txn_id })] = effects.as_slice()
        else {
            panic!("expected one batch delete, got {effects:?}")
        };
        assert_eq!(deletes.len(), 2);
        assert_eq!(*txn_id, Some(TxnId::from(7)));

        operation.step(Event::Storage(StorageEvent::BatchDeleteResult {
            entries: Vec::new(),
        }));
        operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id: TxnId::from(7),
        }));
        assert_eq!(operation.finalize(), Ok(()));
    }

    #[test]
    fn rejects_foreign_group() {
        // The key carries no group, so the record's own group is the gate.
        let mut operation =
            DeleteGroupBackendOperation::new(Ulid::from_bytes([1u8; 16]), backend_id());
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
