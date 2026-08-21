use super::{RecordReadError, backend_key, index_key, parse_read};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_CLEANUP_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, GROUP_STORAGE_BACKEND_INDEX_KEYSPACE,
    GROUP_STORAGE_BACKEND_KEYSPACE, GROUP_STORAGE_BACKEND_SECRET_KEYSPACE,
    S3_MULTIPART_UPLOAD_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::{
    BackendRef, BlobCleanupWork, BlobLocationKey, GroupStorageBackend, MultipartUpload,
};
use aruna_core::types::{Effects, TxnId};
use smallvec::smallvec;
use std::collections::BTreeSet;
use thiserror::Error;
use tracing::{info, warn};
use ulid::Ulid;

use crate::driver::{DriverContext, drive};
use crate::jobs::store::iter_prefix_page;

const SCAN_PAGE_SIZE: usize = 512;

/// Deletes the record and credentials of every disabled tenant backend that no
/// longer holds anything. Runs after the reclaim sweep, which is what empties
/// them; a backend on `retain` keeps its rows and so is never removed.
pub async fn remove_drained_backends(context: &DriverContext) -> Result<usize, String> {
    let disabled = disabled_backends(context).await?;
    if disabled.is_empty() {
        return Ok(0);
    }
    // Read before the scan: a backend held at no point since here can have
    // gained neither a copy nor a cleanup row that the scan would then miss.
    let idle = idle_backends(context, disabled);
    if idle.is_empty() {
        return Ok(0);
    }
    let holding = backends_holding_data(context).await?;

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
fn idle_backends(
    context: &DriverContext,
    disabled: Vec<GroupStorageBackend>,
) -> Vec<(GroupStorageBackend, u64)> {
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

async fn disabled_backends(context: &DriverContext) -> Result<Vec<GroupStorageBackend>, String> {
    let mut disabled = Vec::new();
    let mut start_after = None;
    loop {
        let (values, next) = iter_prefix_page(
            &context.storage_handle,
            GROUP_STORAGE_BACKEND_KEYSPACE,
            None,
            start_after,
            SCAN_PAGE_SIZE,
            None,
        )
        .await?;
        for (_, value) in values {
            match GroupStorageBackend::from_bytes(value.as_ref()) {
                Ok(record) if record.disabled => disabled.push(record),
                Ok(_) => {}
                Err(error) => warn!(error = %error, "Skipping undecodable storage backend record"),
            }
        }
        match next {
            Some(next) => start_after = Some(next),
            None => return Ok(disabled),
        }
    }
}

/// Every backend still named by a stored copy, a queued cleanup row or an open
/// multipart upload. Parts have no location row, and they are deleted in the
/// same transaction as the upload record, so that record covers them.
async fn backends_holding_data(context: &DriverContext) -> Result<BTreeSet<BackendRef>, String> {
    let mut holding = BTreeSet::new();
    let mut start_after = None;
    loop {
        let (values, next) = iter_prefix_page(
            &context.storage_handle,
            BLOB_LOCATIONS_KEYSPACE,
            None,
            start_after,
            SCAN_PAGE_SIZE,
            None,
        )
        .await?;
        for (key, _) in values {
            if let Ok(key) = BlobLocationKey::from_bytes(key.as_ref()) {
                holding.insert(key.backend);
            }
        }
        match next {
            Some(next) => start_after = Some(next),
            None => break,
        }
    }

    let mut start_after = None;
    loop {
        let (values, next) = iter_prefix_page(
            &context.storage_handle,
            BLOB_CLEANUP_KEYSPACE,
            None,
            start_after,
            SCAN_PAGE_SIZE,
            None,
        )
        .await?;
        for (_, value) in values {
            if let Ok(
                BlobCleanupWork::DeleteBlob { location }
                | BlobCleanupWork::ReconcileWrite { location, .. }
                | BlobCleanupWork::ReconcileReservation { location },
            ) = BlobCleanupWork::from_bytes(value.as_ref())
            {
                holding.insert(location.backend);
            }
        }
        match next {
            Some(next) => start_after = Some(next),
            None => break,
        }
    }

    let mut start_after = None;
    loop {
        let (values, next) = iter_prefix_page(
            &context.storage_handle,
            S3_MULTIPART_UPLOAD_KEYSPACE,
            None,
            start_after,
            SCAN_PAGE_SIZE,
            None,
        )
        .await?;
        for (_, value) in values {
            if let Ok(upload) = MultipartUpload::from_bytes(value.as_ref()) {
                holding.insert(upload.backend);
            }
        }
        match next {
            Some(next) => start_after = Some(next),
            None => return Ok(holding),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum RemoveState {
    Init,
    StartTransaction,
    ReadRecord,
    DeleteRecords,
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
                    key_space: GROUP_STORAGE_BACKEND_KEYSPACE.to_string(),
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
        let record = match parse_read(event, GroupStorageBackend::from_bytes) {
            Ok(Some(record)) if record.group_id == self.group_id && record.disabled => record,
            Ok(_) => return self.fail(RemoveBackendError::NotRemovable),
            Err(error) => return self.fail(error.into()),
        };

        self.state = RemoveState::DeleteRecords;
        smallvec![Effect::Storage(StorageEffect::BatchDelete {
            deletes: vec![
                (
                    GROUP_STORAGE_BACKEND_KEYSPACE.to_string(),
                    backend_key(record.backend_id),
                ),
                (
                    GROUP_STORAGE_BACKEND_INDEX_KEYSPACE.to_string(),
                    index_key(record.group_id, record.backend_id),
                ),
                (
                    GROUP_STORAGE_BACKEND_SECRET_KEYSPACE.to_string(),
                    backend_key(record.backend_id),
                ),
            ],
            txn_id: self.txn_id,
        })]
    }

    fn handle_deleted(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::BatchDeleteResult { .. }) => {
                let Some(txn_id) = self.txn_id else {
                    return self.fail(RemoveBackendError::Failed);
                };
                self.output = Some(Ok(()));
                self.state = RemoveState::CommitTransaction;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
            received => self.fail(RemoveBackendError::InvalidStateEvent {
                state: "DeleteRecords",
                expected: "Event::Storage(StorageEvent::BatchDeleteResult)",
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
            RemoveState::CommitTransaction => self.handle_committed(event),
            RemoveState::AbortTransaction => self.handle_aborted(event),
            RemoveState::Finish | RemoveState::Error => smallvec![],
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
    use aruna_core::structs::{
        BackendLocation, CleanupStrategy, GroupBackendKind, GroupStorageBackendSecret,
        MultipartUploadStatus,
    };
    use aruna_core::types::Key;
    use std::collections::HashMap;
    use std::time::SystemTime;
    use tempfile::tempdir;

    fn context(root: &str) -> DriverContext {
        DriverContext {
            storage_handle: aruna_storage::FjallStorage::open(root).unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        }
    }

    fn record(backend_id: Ulid, disabled: bool) -> GroupStorageBackend {
        GroupStorageBackend {
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
        write(
            context,
            GROUP_STORAGE_BACKEND_SECRET_KEYSPACE,
            backend_key(backend_id),
            GroupStorageBackendSecret {
                backend_id,
                secret_config: HashMap::new(),
                updated_at: SystemTime::UNIX_EPOCH,
            }
            .to_bytes()
            .unwrap(),
        )
        .await;
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

        for key_space in [
            GROUP_STORAGE_BACKEND_KEYSPACE,
            GROUP_STORAGE_BACKEND_SECRET_KEYSPACE,
        ] {
            assert!(
                read(&ctx, key_space, backend_key(backend_id))
                    .await
                    .is_none()
            );
        }
        assert!(
            read(
                &ctx,
                GROUP_STORAGE_BACKEND_INDEX_KEYSPACE,
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
            read(
                &ctx,
                GROUP_STORAGE_BACKEND_KEYSPACE,
                backend_key(backend_id)
            )
            .await
            .is_some()
        );
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
            compressed: false,
            encrypted: false,
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
            BlobLocationKey::new([6u8; 32], BackendRef::Group(backend_id))
                .to_bytes()
                .into(),
            location.to_bytes().unwrap(),
        )
        .await;

        assert_eq!(remove_drained_backends(&ctx).await.unwrap(), 0);

        let queued = BlobCleanupWork::DeleteBlob { location };
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

        // An open upload's parts have no location row, yet its abort still needs
        // the credentials to delete them.
        let upload_id = Ulid::from_bytes([8u8; 16]);
        let dir = tempdir().unwrap();
        let ctx = context(dir.path().to_str().unwrap());
        seed(&ctx, backend_id, true).await;
        write(
            &ctx,
            S3_MULTIPART_UPLOAD_KEYSPACE,
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
}
