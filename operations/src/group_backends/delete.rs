use super::{RecordReadError, backend_key, parse_pairs, parse_read};
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_CLEANUP_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, GROUP_STORAGE_BACKEND_KEYSPACE,
    GROUP_STORAGE_BACKEND_SECRET_KEYSPACE, S3_MULTIPART_UPLOAD_KEYSPACE,
    S3_MULTIPART_UPLOAD_PART_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::{
    BackendRef, BlobCleanupWork, BlobLocationKey, GroupStorageBackend, MultipartUpload,
    MultipartUploadPart,
};
use aruna_core::types::{Effects, GroupId, Key, TxnId};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

const SCAN_PAGE_SIZE: usize = 128;

/// Keyspaces a stored reference can live in, in the order they are scanned.
/// Every transaction that moves a reference moves it from an earlier keyspace
/// to a later one, so one of the two scans always observes it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Scan {
    Uploads,
    Parts,
    Locations,
    Cleanup,
}

impl Scan {
    fn key_space(self) -> &'static str {
        match self {
            Scan::Uploads => S3_MULTIPART_UPLOAD_KEYSPACE,
            Scan::Parts => S3_MULTIPART_UPLOAD_PART_KEYSPACE,
            Scan::Locations => BLOB_LOCATIONS_KEYSPACE,
            Scan::Cleanup => BLOB_CLEANUP_KEYSPACE,
        }
    }

    fn next(self) -> Option<Scan> {
        match self {
            Scan::Uploads => Some(Scan::Parts),
            Scan::Parts => Some(Scan::Locations),
            Scan::Locations => Some(Scan::Cleanup),
            Scan::Cleanup => None,
        }
    }
}

fn names_backend(
    scan: Scan,
    key: &Key,
    value: &[u8],
    backend: &BackendRef,
) -> Result<bool, ConversionError> {
    Ok(match scan {
        Scan::Uploads => &MultipartUpload::from_bytes(value)?.backend == backend,
        Scan::Parts => &MultipartUploadPart::from_bytes(value)?.location.backend == backend,
        // The key already names the backend, so no location value is decoded.
        Scan::Locations => &BlobLocationKey::from_bytes(key.as_ref())?.backend == backend,
        Scan::Cleanup => match BlobCleanupWork::from_bytes(value)? {
            BlobCleanupWork::DeleteBlob { location } => &location.backend == backend,
            BlobCleanupWork::RegisterDht { .. } => false,
        },
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum DeleteState {
    Init,
    ReadRecord,
    Retire,
    Scan(Scan),
    Restore,
    StartTransaction,
    VerifyRecord,
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
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Read(#[from] RecordReadError),
    #[error("storage backend not found")]
    NotFound,
    #[error("storage backend still holds stored object data")]
    StillReferenced,
    #[error("storage backend was changed while it was retiring")]
    Changed,
    #[error("DeleteGroupBackend failed")]
    Failed,
    #[error("State [{state:?}] invalid: expected [{expected}] - received [{received:?}]")]
    InvalidStateEvent {
        state: &'static str,
        expected: &'static str,
        received: Event,
    },
}

/// Removes a tenant backend and its credentials together. The record is retired
/// first, so writers that already resolved it lose their commit, and deletion is
/// then refused while any stored reference survives.
#[derive(Debug, PartialEq)]
pub struct DeleteGroupBackendOperation {
    group_id: GroupId,
    backend_id: Ulid,
    state: DeleteState,
    record: Option<GroupStorageBackend>,
    txn_id: Option<TxnId>,
    output: Option<Result<(), DeleteGroupBackendError>>,
}

impl DeleteGroupBackendOperation {
    pub fn new(group_id: GroupId, backend_id: Ulid) -> Self {
        Self {
            group_id,
            backend_id,
            state: DeleteState::Init,
            record: None,
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

    fn write_record(&self, record: &GroupStorageBackend) -> Result<Effect, ConversionError> {
        Ok(Effect::Storage(StorageEffect::Write {
            key_space: GROUP_STORAGE_BACKEND_KEYSPACE.to_string(),
            key: backend_key(self.backend_id),
            value: record.to_bytes()?.into(),
            txn_id: None,
        }))
    }

    fn handle_record_read(&mut self, event: Event) -> Effects {
        let record = match parse_read(event, GroupStorageBackend::from_bytes) {
            Ok(Some(record)) if record.group_id == self.group_id => record,
            Ok(_) => return self.fail(DeleteGroupBackendError::NotFound),
            Err(error) => return self.fail(error.into()),
        };

        let retiring = GroupStorageBackend {
            retiring: true,
            ..record.clone()
        };
        let effect = match self.write_record(&retiring) {
            Ok(effect) => effect,
            Err(error) => return self.fail(error.into()),
        };
        self.record = Some(record);
        self.state = DeleteState::Retire;
        smallvec![effect]
    }

    fn handle_retired(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::WriteResult { .. }) => self.scan(Scan::Uploads, None),
            Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
            received => self.fail(DeleteGroupBackendError::InvalidStateEvent {
                state: "Retire",
                expected: "Event::Storage(StorageEvent::WriteResult)",
                received,
            }),
        }
    }

    /// The scans stay outside the write transaction: a keyspace-wide read set
    /// would conflict with every concurrent blob write.
    fn scan(&mut self, scan: Scan, start_after: Option<Key>) -> Effects {
        self.state = DeleteState::Scan(scan);
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: scan.key_space().to_string(),
            prefix: None,
            start: start_after.map(IterStart::After),
            limit: SCAN_PAGE_SIZE,
            txn_id: None,
        })]
    }

    fn handle_scanned(&mut self, scan: Scan, event: Event) -> Effects {
        let backend = BackendRef::Group(self.backend_id);
        let (found, next_start_after) = match parse_pairs(event, |key, value| {
            names_backend(scan, key, value, &backend)
        }) {
            Ok(page) => page,
            Err(error) => return self.fail(error.into()),
        };

        if found.into_iter().any(|referenced| referenced) {
            return self.restore(DeleteGroupBackendError::StillReferenced);
        }
        if let Some(start_after) = next_start_after {
            return self.scan(scan, Some(start_after));
        }
        match scan.next() {
            Some(next) => self.scan(next, None),
            None => self.start_transaction(),
        }
    }

    fn restore(&mut self, error: DeleteGroupBackendError) -> Effects {
        let Some(record) = self.record.clone() else {
            return self.fail(DeleteGroupBackendError::Failed);
        };
        let effect = match self.write_record(&record) {
            Ok(effect) => effect,
            Err(error) => return self.fail(error.into()),
        };
        self.output = Some(Err(error));
        self.state = DeleteState::Restore;
        smallvec![effect]
    }

    fn handle_restored(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            tracing::error!(backend_id = %self.backend_id, %error, "group backend stayed retired");
        }
        self.state = DeleteState::Error;
        smallvec![]
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
                self.state = DeleteState::VerifyRecord;
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: GROUP_STORAGE_BACKEND_KEYSPACE.to_string(),
                    key: backend_key(self.backend_id),
                    txn_id: self.txn_id,
                })]
            }
            Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
            received => self.fail(DeleteGroupBackendError::InvalidStateEvent {
                state: "StartTransaction",
                expected: "Event::Storage(StorageEvent::TransactionStarted)",
                received,
            }),
        }
    }

    /// Reading the record inside the deleting transaction closes the window in
    /// which a replacement clears the retirement after the scans passed.
    fn handle_record_verified(&mut self, event: Event) -> Effects {
        match parse_read(event, GroupStorageBackend::from_bytes) {
            Ok(Some(record)) if record.retiring && record.group_id == self.group_id => {}
            Ok(_) => return self.fail(DeleteGroupBackendError::Changed),
            Err(error) => return self.fail(error.into()),
        }
        self.delete_records()
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
            return self.fail(DeleteGroupBackendError::Failed);
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
            DeleteState::Retire => self.handle_retired(event),
            DeleteState::Scan(scan) => self.handle_scanned(scan, event),
            DeleteState::Restore => self.handle_restored(event),
            DeleteState::StartTransaction => self.handle_txn_started(event),
            DeleteState::VerifyRecord => self.handle_record_verified(event),
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
    use aruna_core::keyspaces::{
        BLOB_CLEANUP_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, S3_MULTIPART_UPLOAD_KEYSPACE,
        S3_MULTIPART_UPLOAD_PART_KEYSPACE,
    };
    use aruna_core::operation::Operation;
    use aruna_core::structs::{
        BackendLocation, BackendRef, BlobCleanupWork, BlobLocationKey, GroupBackendKind,
        GroupStorageBackend, MultipartUpload, MultipartUploadPart, MultipartUploadStatus,
    };
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
            retiring: false,
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

    fn upload(backend: BackendRef) -> Vec<u8> {
        MultipartUpload {
            backend,
            storage_class: None,
            upload_id: Ulid::from_bytes([4u8; 16]),
            bucket: "bucket".to_string(),
            key: "key".to_string(),
            group_id: Ulid::from_bytes([1u8; 16]),
            created_by: aruna_core::UserId::default(),
            created_at: SystemTime::UNIX_EPOCH,
            status: MultipartUploadStatus::Open,
            checksum_hint: None,
            metadata: HashMap::new(),
        }
        .to_bytes()
        .unwrap()
    }

    fn part(backend: BackendRef) -> Vec<u8> {
        MultipartUploadPart {
            part_number: 1,
            location: location(backend),
            created_at: SystemTime::UNIX_EPOCH,
        }
        .to_bytes()
        .unwrap()
    }

    fn cleanup(backend: BackendRef) -> Vec<u8> {
        BlobCleanupWork::DeleteBlob {
            location: location(backend),
        }
        .to_bytes()
        .unwrap()
    }

    /// Drives the operation up to the first reference scan.
    fn scanning(group_id: Ulid) -> DeleteGroupBackendOperation {
        let mut operation = DeleteGroupBackendOperation::new(group_id, backend_id());
        operation.start();
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: b"x".to_vec().into(),
            value: Some(record(group_id).to_bytes().unwrap().into()),
        }));
        operation.step(Event::Storage(StorageEvent::WriteResult {
            key: b"x".to_vec().into(),
        }));
        operation
    }

    /// Location entries answer from the key, so the page must carry real keys.
    fn location_page(locations: Vec<BackendLocation>) -> Event {
        Event::Storage(StorageEvent::IterResult {
            values: locations
                .into_iter()
                .map(|location| {
                    (
                        BlobLocationKey::new([5u8; 32], location.backend.clone())
                            .to_bytes()
                            .into(),
                        location.to_bytes().unwrap().into(),
                    )
                })
                .collect(),
            next_start_after: None,
        })
    }

    fn page(values: Vec<Vec<u8>>) -> Event {
        Event::Storage(StorageEvent::IterResult {
            values: values
                .into_iter()
                .map(|value| (b"k".to_vec().into(), value.into()))
                .collect(),
            next_start_after: None,
        })
    }

    fn empty() -> Event {
        page(Vec::new())
    }

    #[test]
    fn retires_before_scanning() {
        // A writer that already resolved the backend must lose its commit.
        let group_id = Ulid::from_bytes([1u8; 16]);
        let mut operation = DeleteGroupBackendOperation::new(group_id, backend_id());
        operation.start();

        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: b"x".to_vec().into(),
            value: Some(record(group_id).to_bytes().unwrap().into()),
        }));

        let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
            panic!("expected a retirement write, got {effects:?}")
        };
        assert!(
            GroupStorageBackend::from_bytes(value.as_ref())
                .unwrap()
                .retiring
        );

        let effects = operation.step(Event::Storage(StorageEvent::WriteResult {
            key: b"x".to_vec().into(),
        }));
        let [Effect::Storage(StorageEffect::Iter { txn_id: None, .. })] = effects.as_slice() else {
            panic!("expected an untransacted scan, got {effects:?}")
        };
    }

    #[test]
    fn refuses_every_reference() {
        // Each keyspace can hold the last reference to the backend.
        let group_id = Ulid::from_bytes([1u8; 16]);
        let ours = BackendRef::Group(backend_id());
        let cases = [
            (
                S3_MULTIPART_UPLOAD_KEYSPACE,
                page(vec![upload(ours.clone())]),
                0,
            ),
            (
                S3_MULTIPART_UPLOAD_PART_KEYSPACE,
                page(vec![part(ours.clone())]),
                1,
            ),
            (
                BLOB_LOCATIONS_KEYSPACE,
                location_page(vec![location(ours.clone())]),
                2,
            ),
            (BLOB_CLEANUP_KEYSPACE, page(vec![cleanup(ours)]), 3),
        ];

        for (key_space, found, skipped) in cases {
            let mut operation = scanning(group_id);
            for _ in 0..skipped {
                operation.step(empty());
            }
            let effects = operation.step(found);

            let [Effect::Storage(StorageEffect::Write { .. })] = effects.as_slice() else {
                panic!("expected a restore write for {key_space}, got {effects:?}")
            };
            operation.step(Event::Storage(StorageEvent::WriteResult {
                key: b"x".to_vec().into(),
            }));
            assert!(operation.is_complete());
            assert_eq!(
                operation.finalize(),
                Err(DeleteGroupBackendError::StillReferenced),
                "{key_space} did not block deletion"
            );
        }
    }

    #[test]
    fn deletes_both_records() {
        let group_id = Ulid::from_bytes([1u8; 16]);
        let other = BackendRef::Group(Ulid::from_bytes([8u8; 16]));
        let mut operation = scanning(group_id);

        operation.step(page(vec![upload(other.clone())]));
        operation.step(page(vec![part(other.clone())]));
        operation.step(location_page(vec![
            location(BackendRef::node_default()),
            location(other.clone()),
        ]));
        let effects = operation.step(page(vec![cleanup(other)]));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::StartTransaction {
                read: false
            })]
        ));
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TxnId::from(7),
        }));
        let mut retired = record(group_id);
        retired.retiring = true;
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: b"x".to_vec().into(),
            value: Some(retired.to_bytes().unwrap().into()),
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
    fn refuses_cleared_retirement() {
        // A replacement between the scans and the delete must abort the delete.
        let group_id = Ulid::from_bytes([1u8; 16]);
        let mut operation = scanning(group_id);
        for _ in 0..4 {
            operation.step(empty());
        }
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TxnId::from(7),
        }));

        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: b"x".to_vec().into(),
            value: Some(record(group_id).to_bytes().unwrap().into()),
        }));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { .. })]
        ));
        operation.step(Event::Storage(StorageEvent::TransactionAborted {
            txn_id: TxnId::from(7),
        }));
        assert_eq!(operation.finalize(), Err(DeleteGroupBackendError::Changed));
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
