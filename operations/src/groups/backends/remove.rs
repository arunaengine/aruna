//! Deletes disabled tenant backends that hold nothing any more, records and secrets alike.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{backend_key, index_key};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BACKEND_INDEX_KEYSPACE, BLOB_CLEANUP_KEYSPACE, BLOB_LOCATIONS_KEYSPACE,
    STORAGE_BACKEND_KEYSPACE, UPLOAD_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{done, values};
use aruna_core::structs::storage::blob::{BackendRef, BlobCleanupWork, BlobLocationKey};
use aruna_core::structs::storage::group_backend::{
    BUCKET_DELETING, GroupStorage, GroupStorageSecret,
};
use aruna_core::structs::storage::multipart::MultipartUpload;
use aruna_core::types::{Key, Value};
use std::collections::BTreeSet;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

const SCAN_PAGE_SIZE: usize = 512;

/// Pages through every tenant backend record and keeps the disabled ones.
#[derive(Debug, Default, PartialEq)]
pub struct DisabledScanOperation {
    disabled: Vec<GroupStorage>,
}

impl Operation for DisabledScanOperation {
    type Output = Vec<GroupStorage>;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<Self::Output>, Self::Error> {
        Ok(scan_page(STORAGE_BACKEND_KEYSPACE, None))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, Self::Error> {
        let (rows, next) = scan_rows(result)?;
        for (_, value) in rows {
            match GroupStorage::from_bytes(value.as_ref()) {
                Ok(record) if record.disabled => self.disabled.push(record),
                Ok(_) => {}
                Err(error) => warn!(error = %error, "Skipping undecodable storage backend record"),
            }
        }
        Ok(match next {
            Some(next) => scan_page(STORAGE_BACKEND_KEYSPACE, Some(next)),
            None => Next::Done(std::mem::take(&mut self.disabled)),
        })
    }
}

/// Where a backend can still be named: stored copies, queued cleanup work and open uploads.
const HOLDING_KEYSPACES: [&str; 3] = [
    BLOB_LOCATIONS_KEYSPACE,
    BLOB_CLEANUP_KEYSPACE,
    UPLOAD_KEYSPACE,
];

/// Every backend still named by a stored copy, a queued cleanup row or an open
/// multipart upload. Parts have no location row, and they are deleted in the
/// same transaction as the upload record, so that record covers them.
#[derive(Debug, Default, PartialEq)]
pub struct HoldingScanOperation {
    keyspace: usize,
    holding: BTreeSet<BackendRef>,
}

impl HoldingScanOperation {
    fn collect(&mut self, values: Vec<(Key, Value)>) {
        for (key, value) in values {
            match HOLDING_KEYSPACES[self.keyspace] {
                BLOB_LOCATIONS_KEYSPACE => {
                    if let Ok(key) = BlobLocationKey::from_bytes(key.as_ref()) {
                        self.holding.insert(key.backend);
                    }
                }
                BLOB_CLEANUP_KEYSPACE => {
                    // Every cleanup work that names a location holds its storage.
                    let work = BlobCleanupWork::from_bytes(value.as_ref());
                    if let Some(location) = work.as_ref().ok().and_then(BlobCleanupWork::location) {
                        self.holding.insert(location.backend.clone());
                    }
                    // A move to settle may still need to release a copy on the storage of its
                    // intents.
                    if let Ok(BlobCleanupWork::SettleMove { intents, .. }) = &work {
                        let backends = intents.iter().map(|(_, intent)| intent.backend.clone());
                        self.holding.extend(backends);
                    }
                }
                _ => {
                    if let Ok(upload) = MultipartUpload::from_bytes(value.as_ref()) {
                        self.holding.insert(upload.backend);
                    }
                }
            }
        }
    }
}

impl Operation for HoldingScanOperation {
    type Output = BTreeSet<BackendRef>;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<Self::Output>, Self::Error> {
        Ok(scan_page(HOLDING_KEYSPACES[0], None))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, Self::Error> {
        let (rows, next) = scan_rows(result)?;
        self.collect(rows);
        if let Some(next) = next {
            return Ok(scan_page(HOLDING_KEYSPACES[self.keyspace], Some(next)));
        }
        self.keyspace += 1;
        Ok(match HOLDING_KEYSPACES.get(self.keyspace) {
            Some(key_space) => scan_page(key_space, None),
            None => Next::Done(std::mem::take(&mut self.holding)),
        })
    }
}

/// The next page of a whole keyspace; the empty prefix matches every row.
fn scan_page<T>(key_space: &str, after: Option<Key>) -> Next<T> {
    Next::Call(IoCall::Page {
        space: key_space.to_string(),
        prefix: Key::from(Vec::new()),
        after,
        limit: SCAN_PAGE_SIZE,
    })
}

/// The rows of a page, or the runner's refusal of any other answer.
fn scan_rows(result: IoResult) -> Result<(Vec<(Key, Value)>, Option<Key>), IoError> {
    match result {
        IoResult::Page { rows, next } => Ok((rows, next)),
        _ => Err(IoError::Unexpected {
            expected: "a page of a backend scan",
            got: "another result",
        }),
    }
}

/// What the removal transaction waits for.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RemoveState {
    ReadRecord,
    DeleteRecords,
    DeleteSecret,
}

#[derive(Debug, Error, PartialEq)]
pub enum RemoveBackendError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("storage backend is not removable")]
    NotRemovable,
    /// The removal mark still needs the storage's keys to delete the bucket.
    #[error("a provider bucket of this storage is still being removed")]
    CleanupPending,
    #[error("remove failed")]
    Failed,
}

impl From<IoError> for RemoveBackendError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::Storage(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::Storage(error),
            _ => Self::Failed,
        }
    }
}

/// Erases a drained tenant backend. The record is re-read inside the
/// transaction and must still be disabled, so a concurrent enable, replacement
/// or write that fenced on the record loses one of the two commits.
#[derive(Debug, PartialEq)]
pub struct RemoveBackendOperation {
    group_id: Ulid,
    backend_id: Ulid,
    state: RemoveState,
}

impl RemoveBackendOperation {
    pub fn new(group_id: Ulid, backend_id: Ulid) -> Self {
        Self {
            group_id,
            backend_id,
            state: RemoveState::ReadRecord,
        }
    }

    fn handle_record(&mut self, value: Option<Value>) -> Result<Next<()>, RemoveBackendError> {
        let record = value
            .map(|value| GroupStorage::from_bytes(value.as_ref()))
            .transpose()?
            .filter(|record| record.group_id == self.group_id && record.disabled)
            .ok_or(RemoveBackendError::NotRemovable)?;
        let mut buckets = record.checked_buckets();
        if buckets.any(|bucket| record.bucket_state(bucket) == Some(BUCKET_DELETING)) {
            return Err(RemoveBackendError::CleanupPending);
        }

        self.state = RemoveState::DeleteRecords;
        Ok(Next::Call(IoCall::Delete(vec![
            (
                STORAGE_BACKEND_KEYSPACE.to_string(),
                backend_key(record.backend_id),
            ),
            (
                BACKEND_INDEX_KEYSPACE.to_string(),
                index_key(record.group_id, record.backend_id),
            ),
        ])))
    }
}

impl Operation for RemoveBackendOperation {
    type Output = ();
    type Error = RemoveBackendError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<()>, RemoveBackendError> {
        let row = (
            STORAGE_BACKEND_KEYSPACE.to_string(),
            backend_key(self.backend_id),
        );
        Ok(Next::Call(IoCall::Read(vec![row])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, RemoveBackendError> {
        match self.state {
            RemoveState::ReadRecord => {
                let [value] = values::<1>(result)?;
                self.handle_record(value)
            }
            RemoveState::DeleteRecords => {
                done(result)?;
                self.state = RemoveState::DeleteSecret;
                // The runner puts the vault delete into this operation's transaction.
                let entry = GroupStorageSecret::vault_entry(self.backend_id);
                let delete = crate::node_vault::delete_secret(entry, None);
                Ok(Next::Call(IoCall::Effect(Box::new(delete))))
            }
            RemoveState::DeleteSecret => match result {
                IoResult::Event(event) => match *event {
                    Event::Storage(StorageEvent::DeleteResult { .. }) => Ok(Next::Done(())),
                    Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
                    _ => Err(RemoveBackendError::Failed),
                },
                _ => Err(RemoveBackendError::Failed),
            },
        }
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use aruna_core::structs::storage::cleanup::CleanupStrategy;
    use aruna_core::structs::storage::format::EncodingClass;
    use aruna_core::structs::storage::group_backend::GroupBackendKind;
    use std::collections::HashMap;
    use std::time::SystemTime;

    pub(crate) fn record(backend_id: Ulid, disabled: bool) -> GroupStorage {
        GroupStorage {
            backend_id,
            group_id: Ulid::from_bytes([1u8; 16]),
            name: "tenant".to_string(),
            kind: GroupBackendKind::S3,
            public_config: HashMap::new(),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            disabled,
            cleanup: CleanupStrategy::Retain,
        }
    }

    fn read(stored: &GroupStorage) -> IoResult {
        IoResult::Values(vec![Some(stored.to_bytes().unwrap().into())])
    }

    fn removal(backend_id: Ulid) -> RemoveBackendOperation {
        let mut operation = RemoveBackendOperation::new(Ulid::from_bytes([1u8; 16]), backend_id);
        assert_eq!(operation.transaction(), TransactionMode::Write);
        operation.first().unwrap();
        operation
    }

    #[test]
    fn refuses_enabled_record() {
        // The in-transaction re-read is the fence against a concurrent enable.
        let backend_id = Ulid::from_bytes([8u8; 16]);
        let enabled = read(&record(backend_id, false));
        assert_eq!(
            removal(backend_id).next(enabled),
            Err(RemoveBackendError::NotRemovable)
        );
    }

    #[test]
    fn pending_cleanup_refuses() {
        // The secret stays while a removal mark still deletes a provider bucket with it.
        let backend_id = Ulid::from_bytes([8u8; 16]);
        let mut drained = record(backend_id, true);
        let state = ("bucket:team-a-x".to_string(), "deleting".to_string());
        drained.public_config.extend([state]);
        assert_eq!(
            removal(backend_id).next(read(&drained)),
            Err(RemoveBackendError::CleanupPending)
        );
    }

    #[test]
    fn removes_records_secret() {
        // Both records and the vault secret go in the one transaction.
        let backend_id = Ulid::from_bytes([8u8; 16]);
        let mut operation = removal(backend_id);
        let Ok(Next::Call(IoCall::Delete(rows))) = operation.next(read(&record(backend_id, true)))
        else {
            panic!("expected the record deletes");
        };
        assert_eq!(rows.len(), 2);
        let Ok(Next::Call(IoCall::Effect(delete))) = operation.next(IoResult::Done) else {
            panic!("expected the vault delete");
        };
        let entry = GroupStorageSecret::vault_entry(backend_id);
        assert_eq!(*delete, crate::node_vault::delete_secret(entry, None));
        let deleted = Event::Storage(StorageEvent::DeleteResult {
            key: Key::from(Vec::new()),
        });
        assert_eq!(
            operation.next(IoResult::Event(Box::new(deleted))),
            Ok(Next::Done(()))
        );
    }

    fn page(rows: Vec<(Key, Value)>, next: Option<Key>) -> IoResult {
        IoResult::Page { rows, next }
    }

    #[test]
    fn scans_disabled_records() {
        // Every page is read; only disabled records are kept and an undecodable row is skipped.
        let disabled = record(Ulid::from_bytes([2u8; 16]), true);
        let enabled = record(Ulid::from_bytes([3u8; 16]), false);
        let row = |stored: &GroupStorage| {
            let value = Value::from(stored.to_bytes().unwrap());
            (backend_key(stored.backend_id), value)
        };
        let cursor = backend_key(enabled.backend_id);
        let broken = (Key::from(vec![1u8]), Value::from(vec![0xff; 3]));
        let mut operation = DisabledScanOperation::default();
        assert_eq!(
            operation.first(),
            Ok(scan_page(STORAGE_BACKEND_KEYSPACE, None))
        );
        let rows = vec![row(&disabled), row(&enabled)];
        let answer = operation.next(page(rows, Some(cursor.clone())));
        assert_eq!(
            answer,
            Ok(scan_page(STORAGE_BACKEND_KEYSPACE, Some(cursor)))
        );
        let answer = operation.next(page(vec![broken], None));
        assert_eq!(answer, Ok(Next::Done(vec![disabled])));
    }

    #[test]
    fn scans_holding_keyspaces() {
        // Locations, cleanup work and uploads are scanned in turn; a location names its backend.
        let backend = BackendRef::Group(Ulid::from_bytes([4u8; 16]));
        let key = BlobLocationKey::new([6u8; 32], EncodingClass::Raw, backend.clone());
        let mut operation = HoldingScanOperation::default();
        assert_eq!(
            operation.first(),
            Ok(scan_page(BLOB_LOCATIONS_KEYSPACE, None))
        );
        let rows = vec![(key.to_bytes().into(), Value::from(Vec::new()))];
        let answer = operation.next(page(rows, None));
        assert_eq!(answer, Ok(scan_page(BLOB_CLEANUP_KEYSPACE, None)));
        let answer = operation.next(page(Vec::new(), None));
        assert_eq!(answer, Ok(scan_page(UPLOAD_KEYSPACE, None)));
        let answer = operation.next(page(Vec::new(), None));
        assert_eq!(answer, Ok(Next::Done(BTreeSet::from([backend]))));
    }

    #[test]
    fn scan_failures_end() {
        // An answer that is not a page ends either scan.
        let mut operation = HoldingScanOperation::default();
        operation.first().unwrap();
        assert!(matches!(
            operation.next(IoResult::Done),
            Err(IoError::Unexpected { .. })
        ));
        let mut operation = DisabledScanOperation::default();
        operation.first().unwrap();
        assert!(matches!(
            operation.next(IoResult::Woke),
            Err(IoError::Unexpected { .. })
        ));
    }
}
