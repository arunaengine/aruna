//! Deletes disabled tenant backends that hold nothing any more, records and secrets alike.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{RecordReadError, backend_key, index_key, parse_read};
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BACKEND_INDEX_KEYSPACE, BLOB_CLEANUP_KEYSPACE, BLOB_LOCATIONS_KEYSPACE,
    STORAGE_BACKEND_KEYSPACE, UPLOAD_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::blob::{BackendRef, BlobCleanupWork, BlobLocationKey};
use aruna_core::structs::storage::group_backend::{
    BUCKET_DELETING, GroupStorage, GroupStorageSecret,
};
use aruna_core::structs::storage::multipart::MultipartUpload;
use aruna_core::types::{Effects, Key, TxnId, Value};
use smallvec::smallvec;
use std::collections::BTreeSet;
use thiserror::Error;
use tracing::{info, warn};
use ulid::Ulid;

use crate::driver::{DriverContext, drive};

const SCAN_PAGE_SIZE: usize = 512;

/// Deletes the record and credentials of every disabled tenant backend that no
/// longer holds anything. Runs after the reclaim sweep, which is what empties
/// them; a backend on `retain` keeps its rows and so is never removed.
pub async fn remove_drained_backends(context: &DriverContext) -> Result<usize, String> {
    let disabled = drive(DisabledScanOperation::default(), context).await?;
    if disabled.is_empty() {
        return Ok(0);
    }
    // Read before the scan: a backend held at no point since here can have
    // gained neither a copy nor a cleanup row that the scan would then miss.
    let idle = idle_backends(context, disabled);
    if idle.is_empty() {
        return Ok(0);
    }
    let holding = drive(HoldingScanOperation::default(), context).await?;

    let mut removed = 0usize;
    for (record, generation) in idle {
        let backend = BackendRef::Group(record.backend_id);
        if holding.contains(&backend) {
            continue;
        }
        let _claim = match context.blob_handle.as_ref() {
            Some(blob_handle) => match blob_handle.claim_backend(record.backend_id, generation) {
                Some(claim) => Some(claim),
                None => continue,
            },
            None => None,
        };
        match drive(
            RemoveBackendOperation::new(record.group_id, record.backend_id),
            context,
        )
        .await
        {
            Ok(()) => {
                info!(backend = %backend, "Removed drained storage backend");
                removed = removed.saturating_add(1);
            }
            Err(error) => warn!(backend = %backend, error = %error, "Backend removal failed"),
        }
    }
    Ok(removed)
}

/// Every disabled backend nothing is currently holding, with the hold
/// generation the later claim has to still match.
fn idle_backends(context: &DriverContext, disabled: Vec<GroupStorage>) -> Vec<(GroupStorage, u64)> {
    let Some(blob_handle) = context.blob_handle.as_ref() else {
        // Nothing in this process can run a blob effect, so nothing to exclude.
        return disabled.into_iter().map(|record| (record, 0)).collect();
    };
    disabled
        .into_iter()
        .filter_map(|record| {
            blob_handle
                .idle_generation(record.backend_id)
                .map(|generation| (record, generation))
        })
        .collect()
}

/// Pages through every tenant backend record and keeps the disabled ones.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct DisabledScanOperation {
    disabled: Vec<GroupStorage>,
    reading: bool,
    output: Option<Result<Vec<GroupStorage>, String>>,
}

impl Operation for DisabledScanOperation {
    type Output = Vec<GroupStorage>;
    type Error = String;

    fn start(&mut self) -> Effects {
        self.reading = true;
        smallvec![scan_page(STORAGE_BACKEND_KEYSPACE, None)]
    }

    fn step(&mut self, event: Event) -> Effects {
        let (values, next) = match scan_result(self.reading, event) {
            Ok(page) => page,
            Err(error) => return finish(&mut self.reading, &mut self.output, Err(error)),
        };
        for (_, value) in values {
            match GroupStorage::from_bytes(value.as_ref()) {
                Ok(record) if record.disabled => self.disabled.push(record),
                Ok(_) => {}
                Err(error) => warn!(error = %error, "Skipping undecodable storage backend record"),
            }
        }
        match next {
            Some(next) => smallvec![scan_page(STORAGE_BACKEND_KEYSPACE, Some(next))],
            None => {
                let disabled = std::mem::take(&mut self.disabled);
                finish(&mut self.reading, &mut self.output, Ok(disabled))
            }
        }
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .unwrap_or_else(|| Err("backend scan never finished".to_string()))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
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
pub(crate) struct HoldingScanOperation {
    keyspace: usize,
    holding: BTreeSet<BackendRef>,
    reading: bool,
    output: Option<Result<BTreeSet<BackendRef>, String>>,
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
    type Error = String;

    fn start(&mut self) -> Effects {
        self.reading = true;
        smallvec![scan_page(HOLDING_KEYSPACES[0], None)]
    }

    fn step(&mut self, event: Event) -> Effects {
        let (values, next) = match scan_result(self.reading, event) {
            Ok(page) => page,
            Err(error) => return finish(&mut self.reading, &mut self.output, Err(error)),
        };
        self.collect(values);
        if let Some(next) = next {
            return smallvec![scan_page(HOLDING_KEYSPACES[self.keyspace], Some(next))];
        }
        self.keyspace += 1;
        match HOLDING_KEYSPACES.get(self.keyspace) {
            Some(key_space) => smallvec![scan_page(key_space, None)],
            None => {
                let holding = std::mem::take(&mut self.holding);
                finish(&mut self.reading, &mut self.output, Ok(holding))
            }
        }
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .unwrap_or_else(|| Err("holding scan never finished".to_string()))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

fn scan_page(key_space: &str, start: Option<Key>) -> Effect {
    Effect::Storage(StorageEffect::Iter {
        key_space: key_space.to_string(),
        prefix: None,
        start: start.map(IterStart::After),
        limit: SCAN_PAGE_SIZE,
        txn_id: None,
    })
}

/// One page of a running scan; anything else ends it with the reason.
fn scan_result(reading: bool, event: Event) -> Result<(Vec<(Key, Value)>, Option<Key>), String> {
    match event {
        Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) if reading => Ok((values, next_start_after)),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected storage event: {}", other.label())),
    }
}

fn finish<T>(
    reading: &mut bool,
    output: &mut Option<Result<T, String>>,
    result: Result<T, String>,
) -> Effects {
    *reading = false;
    *output = Some(result);
    smallvec![]
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum RemoveState {
    Init,
    StartTransaction,
    ReadRecord,
    DeleteRecords,
    DeleteSecret,
    CommitTransaction,
    AbortTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum RemoveBackendError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Read(#[from] RecordReadError),
    #[error("storage backend is not removable")]
    NotRemovable,
    /// The removal mark still needs the storage's keys to delete the bucket.
    #[error("a provider bucket of this storage is still being removed")]
    CleanupPending,
    #[error("remove failed")]
    Failed,
    #[error("State [{state:?}] invalid: expected [{expected}] - received [{received:?}]")]
    InvalidStateEvent {
        state: &'static str,
        expected: &'static str,
        received: Event,
    },
}

/// Erases a drained tenant backend. The record is re-read inside the
/// transaction and must still be disabled, so a concurrent enable, replacement
/// or write that fenced on the record loses one of the two commits.
#[derive(Debug, PartialEq)]
pub struct RemoveBackendOperation {
    group_id: Ulid,
    backend_id: Ulid,
    state: RemoveState,
    txn_id: Option<TxnId>,
    output: Option<Result<(), RemoveBackendError>>,
}

impl RemoveBackendOperation {
    pub fn new(group_id: Ulid, backend_id: Ulid) -> Self {
        Self {
            group_id,
            backend_id,
            state: RemoveState::Init,
            txn_id: None,
            output: None,
        }
    }

    fn fail(&mut self, error: RemoveBackendError) -> Effects {
        self.output = Some(Err(error));
        if let Some(txn_id) = self.txn_id.take() {
            self.state = RemoveState::AbortTransaction;
            return smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })];
        }
        self.state = RemoveState::Error;
        smallvec![]
    }

    fn handle_txn_started(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                self.txn_id = Some(txn_id);
                self.state = RemoveState::ReadRecord;
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: STORAGE_BACKEND_KEYSPACE.to_string(),
                    key: backend_key(self.backend_id),
                    txn_id: self.txn_id,
                })]
            }
            Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
            received => self.fail(RemoveBackendError::InvalidStateEvent {
                state: "StartTransaction",
                expected: "Event::Storage(StorageEvent::TransactionStarted)",
                received,
            }),
        }
    }

    fn handle_record(&mut self, event: Event) -> Effects {
        let record = match parse_read(event, GroupStorage::from_bytes) {
            Ok(Some(record)) if record.group_id == self.group_id && record.disabled => record,
            Ok(_) => return self.fail(RemoveBackendError::NotRemovable),
            Err(error) => return self.fail(error.into()),
        };
        let mut buckets = record.checked_buckets();
        if buckets.any(|bucket| record.bucket_state(bucket) == Some(BUCKET_DELETING)) {
            return self.fail(RemoveBackendError::CleanupPending);
        }

        self.state = RemoveState::DeleteRecords;
        smallvec![Effect::Storage(StorageEffect::BatchDelete {
            deletes: vec![
                (
                    STORAGE_BACKEND_KEYSPACE.to_string(),
                    backend_key(record.backend_id),
                ),
                (
                    BACKEND_INDEX_KEYSPACE.to_string(),
                    index_key(record.group_id, record.backend_id),
                ),
            ],
            txn_id: self.txn_id,
        })]
    }

    fn handle_deleted(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::BatchDeleteResult { .. }) => {
                self.state = RemoveState::DeleteSecret;
                smallvec![crate::node_vault::delete_secret(
                    GroupStorageSecret::vault_entry(self.backend_id),
                    self.txn_id,
                )]
            }
            Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
            received => self.fail(RemoveBackendError::InvalidStateEvent {
                state: "DeleteRecords",
                expected: "Event::Storage(StorageEvent::BatchDeleteResult)",
                received,
            }),
        }
    }

    fn handle_secret_deleted(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::DeleteResult { .. }) => {
                let Some(txn_id) = self.txn_id else {
                    return self.fail(RemoveBackendError::Failed);
                };
                self.output = Some(Ok(()));
                self.state = RemoveState::CommitTransaction;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
            received => self.fail(RemoveBackendError::InvalidStateEvent {
                state: "DeleteSecret",
                expected: "Event::Storage(StorageEvent::DeleteResult)",
                received,
            }),
        }
    }

    fn handle_committed(&mut self, event: Event) -> Effects {
        self.txn_id = None;
        match event {
            Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                self.state = RemoveState::Finish;
                smallvec![]
            }
            Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
            received => self.fail(RemoveBackendError::InvalidStateEvent {
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
                self.state = RemoveState::Error;
                smallvec![]
            }
            received => self.fail(RemoveBackendError::InvalidStateEvent {
                state: "AbortTransaction",
                expected: "Event::Storage(StorageEvent::TransactionAborted)",
                received,
            }),
        }
    }
}

impl Operation for RemoveBackendOperation {
    type Output = ();
    type Error = RemoveBackendError;

    fn start(&mut self) -> Effects {
        self.state = RemoveState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            RemoveState::Init => self.start(),
            RemoveState::StartTransaction => self.handle_txn_started(event),
            RemoveState::ReadRecord => self.handle_record(event),
            RemoveState::DeleteRecords => self.handle_deleted(event),
            RemoveState::DeleteSecret => self.handle_secret_deleted(event),
            RemoveState::CommitTransaction => self.handle_committed(event),
            RemoveState::AbortTransaction => self.handle_aborted(event),
            RemoveState::Finish | RemoveState::Error => {
                self.fail(RemoveBackendError::InvalidStateEvent {
                    state: "terminal",
                    expected: "no event",
                    received: event,
                })
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, RemoveState::Finish | RemoveState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.state {
            RemoveState::Finish => self.output.unwrap_or(Err(RemoveBackendError::Failed)),
            _ => match self.output {
                Some(Err(error)) => Err(error),
                _ => Err(RemoveBackendError::Failed),
            },
        }
    }

    fn abort(&mut self) -> Effects {
        if let Some(txn_id) = self.txn_id.take() {
            self.state = RemoveState::AbortTransaction;
            return smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })];
        }
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::handle::Handle;
    use aruna_core::operation_check::{
        check_trace, rejects_events, rejects_finalize, sample_events,
    };
    use aruna_core::structs::storage::blob::BackendLocation;
    use aruna_core::structs::storage::cleanup::CleanupStrategy;
    use aruna_core::structs::storage::format::EncodingClass;
    use aruna_core::structs::storage::format::StoredFormat;
    use aruna_core::structs::storage::group_backend::GroupBackendKind;
    use aruna_core::structs::storage::multipart::MultipartUploadStatus;
    use std::collections::HashMap;
    use std::time::SystemTime;
    use tempfile::tempdir;

    fn context(root: &str) -> DriverContext {
        let storage_handle = aruna_storage::FjallStorage::open(root).unwrap();
        storage_handle.open_vault(aruna_core::node_vault::NodeVaultKey::random());
        DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        }
    }

    fn record(backend_id: Ulid, disabled: bool) -> GroupStorage {
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

    async fn write(context: &DriverContext, key_space: &str, key: Key, value: Vec<u8>) {
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: key_space.to_string(),
                key,
                value: value.into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    async fn seed(context: &DriverContext, backend_id: Ulid, disabled: bool) {
        let stored = record(backend_id, disabled);
        for (key_space, key, value) in super::super::record_writes(&stored).unwrap() {
            write(context, &key_space, key, value.to_vec()).await;
        }
        let secret = GroupStorageSecret {
            backend_id,
            secret_config: HashMap::new(),
            updated_at: SystemTime::UNIX_EPOCH,
        };
        let entry = GroupStorageSecret::vault_entry(backend_id);
        let effect = crate::node_vault::write_secret(entry, secret.to_secret().unwrap(), None);
        context.storage_handle.send_effect(effect).await;
    }

    async fn stored_secret(
        context: &DriverContext,
        backend_id: Ulid,
    ) -> Option<GroupStorageSecret> {
        let entry = GroupStorageSecret::vault_entry(backend_id);
        let event = context
            .storage_handle
            .send_effect(crate::node_vault::read_secret(entry, None))
            .await;
        crate::node_vault::parse_secret(event, entry, GroupStorageSecret::from_bytes).unwrap()
    }

    async fn read(context: &DriverContext, key_space: &str, key: Key) -> Option<Vec<u8>> {
        match context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: key_space.to_string(),
                key,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                value.map(|value| value.to_vec())
            }
            other => panic!("unexpected read event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn removes_drained_backend() {
        // Record, index and secret go together once nothing names the backend.
        let dir = tempdir().unwrap();
        let ctx = context(dir.path().to_str().unwrap());
        let backend_id = Ulid::from_bytes([2u8; 16]);
        seed(&ctx, backend_id, true).await;

        assert_eq!(remove_drained_backends(&ctx).await.unwrap(), 1);

        assert!(
            read(&ctx, STORAGE_BACKEND_KEYSPACE, backend_key(backend_id))
                .await
                .is_none()
        );
        assert!(stored_secret(&ctx, backend_id).await.is_none());
        assert!(
            read(
                &ctx,
                BACKEND_INDEX_KEYSPACE,
                index_key(Ulid::from_bytes([1u8; 16]), backend_id)
            )
            .await
            .is_none()
        );
    }

    #[tokio::test]
    async fn enabled_backend_survives() {
        let dir = tempdir().unwrap();
        let ctx = context(dir.path().to_str().unwrap());
        let backend_id = Ulid::from_bytes([3u8; 16]);
        seed(&ctx, backend_id, false).await;

        assert_eq!(remove_drained_backends(&ctx).await.unwrap(), 0);
        assert!(
            read(&ctx, STORAGE_BACKEND_KEYSPACE, backend_key(backend_id))
                .await
                .is_some()
        );
        assert!(stored_secret(&ctx, backend_id).await.is_some());
    }

    #[tokio::test]
    async fn holding_backend_survives() {
        // A stored copy or a queued physical delete both keep the record alive.
        let dir = tempdir().unwrap();
        let ctx = context(dir.path().to_str().unwrap());
        let backend_id = Ulid::from_bytes([4u8; 16]);
        seed(&ctx, backend_id, true).await;
        let location = BackendLocation {
            backend: BackendRef::Group(backend_id),
            storage_class: None,
            root: "/data".to_string(),
            storage_bucket: "storage".to_string(),
            backend_path: "bucket/key_01".to_string(),
            ulid: Ulid::from_bytes([5u8; 16]),
            format: StoredFormat::default(),
            created_by: Default::default(),
            created_at: SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 10,
            hashes: HashMap::new(),
        };
        write(
            &ctx,
            BLOB_LOCATIONS_KEYSPACE,
            BlobLocationKey::new([6u8; 32], EncodingClass::Raw, BackendRef::Group(backend_id))
                .to_bytes()
                .into(),
            location.to_bytes().unwrap(),
        )
        .await;

        assert_eq!(remove_drained_backends(&ctx).await.unwrap(), 0);

        // Every queued work that names a location of the storage keeps it, also the newer ones.
        let queued = [
            BlobCleanupWork::DeleteBlob {
                location: location.clone(),
            },
            BlobCleanupWork::ReleasePlanned {
                job_id: Ulid::from_bytes([9u8; 16]),
                location: location.clone(),
                import_id: Ulid::nil(),
                version_id: Ulid::nil(),
                version: "v1".to_string(),
            },
            BlobCleanupWork::SettleUpload {
                attempt: Ulid::from_bytes([9u8; 16]),
                location,
                upload_id: "upload".to_string(),
                since_ms: 0,
            },
        ];
        for queued in queued {
            let dir = tempdir().unwrap();
            let ctx = context(dir.path().to_str().unwrap());
            seed(&ctx, backend_id, true).await;
            write(
                &ctx,
                BLOB_CLEANUP_KEYSPACE,
                Ulid::from_bytes([7u8; 16]).to_bytes().to_vec().into(),
                queued.to_bytes().unwrap(),
            )
            .await;
            assert_eq!(remove_drained_backends(&ctx).await.unwrap(), 0);
        }

        // An open upload's parts have no location row, yet its abort still needs
        // the credentials to delete them.
        let upload_id = Ulid::from_bytes([8u8; 16]);
        let dir = tempdir().unwrap();
        let ctx = context(dir.path().to_str().unwrap());
        seed(&ctx, backend_id, true).await;
        write(
            &ctx,
            UPLOAD_KEYSPACE,
            upload_id.to_bytes().to_vec().into(),
            MultipartUpload {
                upload_id,
                backend: BackendRef::Group(backend_id),
                storage_class: None,
                bucket: "bucket".to_string(),
                key: "object".to_string(),
                group_id: Ulid::from_bytes([1u8; 16]),
                created_by: Default::default(),
                created_at: SystemTime::UNIX_EPOCH,
                status: MultipartUploadStatus::Open,
                checksum_hint: None,
                metadata: HashMap::new(),
                placement_policies: Vec::new(),
                subject_generation: 0,
                completing_since_ms: None,
                backend_upload: None,
                encryption: None,
            }
            .to_bytes()
            .unwrap(),
        )
        .await;

        assert_eq!(remove_drained_backends(&ctx).await.unwrap(), 0);
    }

    #[test]
    fn refuses_enabled_record() {
        // The in-transaction re-read is the fence against a concurrent enable.
        let backend_id = Ulid::from_bytes([8u8; 16]);
        let mut operation = RemoveBackendOperation::new(Ulid::from_bytes([1u8; 16]), backend_id);
        operation.start();
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: Ulid::from_bytes([9u8; 16]),
        }));

        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: b"x".to_vec().into(),
            value: Some(record(backend_id, false).to_bytes().unwrap().into()),
        }));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { .. })]
        ));
        operation.step(Event::Storage(StorageEvent::TransactionAborted {
            txn_id: Ulid::from_bytes([9u8; 16]),
        }));
        assert_eq!(operation.finalize(), Err(RemoveBackendError::NotRemovable));
    }

    #[test]
    fn pending_cleanup_refuses() {
        let backend_id = Ulid::from_bytes([8u8; 16]);
        let mut operation = RemoveBackendOperation::new(Ulid::from_bytes([1u8; 16]), backend_id);
        operation.start();
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: Ulid::from_bytes([9u8; 16]),
        }));
        let mut drained = record(backend_id, true);
        let state = ("bucket:team-a-x".to_string(), "deleting".to_string());
        drained.public_config.extend([state]);
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: b"x".to_vec().into(),
            value: Some(drained.to_bytes().unwrap().into()),
        }));
        // The secret stays while a removal mark still deletes a provider bucket with it.
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { .. })]
        ));
        assert_eq!(
            operation.finalize(),
            Err(RemoveBackendError::CleanupPending)
        );
    }

    // A state that expects no event must reject one instead of ignoring it.
    #[test]
    fn terminal_rejects_event() {
        let backend_id = Ulid::from_bytes([8u8; 16]);
        let make = || {
            let mut operation =
                RemoveBackendOperation::new(Ulid::from_bytes([1u8; 16]), backend_id);
            operation.start();
            operation.step(Event::Storage(StorageEvent::TransactionStarted {
                txn_id: Ulid::from_bytes([9u8; 16]),
            }));
            operation.step(Event::Storage(StorageEvent::ReadResult {
                key: b"x".to_vec().into(),
                value: Some(record(backend_id, false).to_bytes().unwrap().into()),
            }));
            operation.step(Event::Storage(StorageEvent::TransactionAborted {
                txn_id: Ulid::from_bytes([9u8; 16]),
            }));
            operation
        };
        for (_, effects, error) in rejects_events(make, sample_events, |_| false) {
            assert!(effects.is_empty());
            assert!(
                matches!(error, RemoveBackendError::InvalidStateEvent { .. }),
                "{error:?}"
            );
        }
    }

    fn page(values: Vec<(Key, Value)>, next_start_after: Option<Key>) -> Event {
        Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        })
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
        check_trace(
            &mut operation,
            &[scan_page(STORAGE_BACKEND_KEYSPACE, None)],
            vec![
                (
                    page(vec![row(&disabled), row(&enabled)], Some(cursor.clone())),
                    vec![scan_page(STORAGE_BACKEND_KEYSPACE, Some(cursor))],
                ),
                (page(vec![broken], None), vec![]),
            ],
        );
        assert_eq!(operation.finalize(), Ok(vec![disabled]));
    }

    #[test]
    fn scans_holding_keyspaces() {
        // Locations, cleanup work and uploads are scanned in turn; a location names its backend.
        let backend = BackendRef::Group(Ulid::from_bytes([4u8; 16]));
        let key = BlobLocationKey::new([6u8; 32], EncodingClass::Raw, backend.clone());
        let mut operation = HoldingScanOperation::default();
        check_trace(
            &mut operation,
            &[scan_page(BLOB_LOCATIONS_KEYSPACE, None)],
            vec![
                (
                    page(vec![(key.to_bytes().into(), Value::from(Vec::new()))], None),
                    vec![scan_page(BLOB_CLEANUP_KEYSPACE, None)],
                ),
                (
                    page(Vec::new(), None),
                    vec![scan_page(UPLOAD_KEYSPACE, None)],
                ),
                (page(Vec::new(), None), vec![]),
            ],
        );
        assert_eq!(operation.finalize(), Ok(BTreeSet::from([backend])));
    }

    #[test]
    fn scan_failures_end() {
        // A storage error ends a scan with its reason, and a page before start is refused.
        let mut operation = HoldingScanOperation::default();
        operation.start();
        let error = Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        });
        assert!(operation.step(error).is_empty());
        assert!(operation.finalize().is_err());
        let refusals = rejects_events(DisabledScanOperation::default, sample_events, |_| false);
        assert!(refusals.iter().all(|(_, effects, _)| effects.is_empty()));
        let refusals = rejects_events(HoldingScanOperation::default, sample_events, |_| false);
        assert!(refusals.iter().all(|(_, effects, _)| effects.is_empty()));
    }

    #[test]
    fn scan_abort_finalize() {
        // The scans only read: abort has nothing to clean up, and an early finalize fails.
        let mut operation = DisabledScanOperation::default();
        operation.start();
        assert!(operation.abort().is_empty());
        rejects_finalize(operation);
        let mut operation = HoldingScanOperation::default();
        operation.start();
        assert!(operation.abort().is_empty());
        rejects_finalize(operation);
    }
}
