//! Deletes a source connector and its secret unless an object version still uses it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::types::{Effects, GroupId, TxnId};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::connectors::reference_prune::schedule_prune;
use crate::connectors::reference_scan::{ReferenceScan, ScanStep};
use crate::connectors::repository::{
    StorageReadError, delete_secret_effect, parse_connector_read, parse_secret_read,
    read_connector_effect, read_secret_effect, source_connector_key,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteSourceInput {
    pub group_id: GroupId,
    pub connector_id: Ulid,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteSourceResult;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeleteSourceState {
    Init,
    ReadConnector,
    ReadSecret,
    StartTransaction,
    ScanReferenceVersions,
    /// More reference rows follow than one request checks; their pruning is being queued.
    CommitDeferred,
    ScheduleDeferred,
    DeleteRecords,
    DeleteSecret,
    CommitTransaction,
    AbortTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum DeleteSourceError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("Connector not found")]
    NotFound,
    #[error("Connector credentials are referenced by object versions")]
    ReferencedObjectVersion,
    #[error("Connector references are still being checked; retry later")]
    ReferencesPending,
    #[error("DeleteSourceConnector failed")]
    DeleteConnectorFailed,
    #[error("State [{state:?}] invalid: expected [{expected}] - received [{received:?}]")]
    InvalidStateEvent {
        state: DeleteSourceState,
        expected: &'static str,
        received: Event,
    },
}

impl From<StorageReadError> for DeleteSourceError {
    fn from(value: StorageReadError) -> Self {
        match value {
            StorageReadError::Storage(error) => Self::StorageError(error),
            StorageReadError::Conversion(error) => Self::ConversionError(error),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct DeleteSourceOperation {
    input: DeleteSourceInput,
    state: DeleteSourceState,
    txn_id: Option<TxnId>,
    has_secret: bool,
    scan: Option<ReferenceScan>,
    output: Option<Result<DeleteSourceResult, DeleteSourceError>>,
}

impl DeleteSourceOperation {
    pub fn new(input: DeleteSourceInput) -> Self {
        Self {
            input,
            state: DeleteSourceState::Init,
            txn_id: None,
            has_secret: false,
            scan: None,
            output: None,
        }
    }

    fn handle_init(&mut self) -> Effects {
        self.state = DeleteSourceState::ReadConnector;
        smallvec![read_connector_effect(
            self.input.group_id,
            self.input.connector_id,
            None,
        )]
    }

    fn handle_connector_read(&mut self, event: Event) -> Effects {
        match parse_connector_read(event) {
            Ok(Some(_)) => {
                self.state = DeleteSourceState::ReadSecret;
                smallvec![read_secret_effect(self.input.connector_id, None)]
            }
            Ok(None) => self.emit_error(DeleteSourceError::NotFound),
            Err(error) => self.emit_error(error.into()),
        }
    }

    fn handle_secret_read(&mut self, event: Event) -> Effects {
        let secret = match parse_secret_read(event, self.input.connector_id) {
            Ok(secret) => secret,
            Err(error) => return self.emit_error(error.into()),
        };

        self.has_secret = secret.is_some();

        self.state = DeleteSourceState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn handle_transaction_started(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                self.txn_id = Some(txn_id);
                if self.has_secret {
                    self.scan_reference_versions()
                } else {
                    self.delete_records()
                }
            }
            Event::Storage(StorageEvent::Error { error }) => self.emit_error(error.into()),
            received => self.emit_error(DeleteSourceError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionStarted)",
                received,
            }),
        }
    }

    fn emit_error(&mut self, error: DeleteSourceError) -> Effects {
        self.state = DeleteSourceState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn abort_with_error(&mut self, error: DeleteSourceError) -> Effects {
        let Some(txn_id) = self.txn_id.take() else {
            return self.emit_error(error);
        };

        self.state = DeleteSourceState::AbortTransaction;
        self.output = Some(Err(error));
        smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
    }

    fn fail_or_abort(&mut self, error: DeleteSourceError) -> Effects {
        if self.txn_id.is_some() {
            self.abort_with_error(error)
        } else {
            self.emit_error(error)
        }
    }

    fn scan_reference_versions(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(StorageError::TransactionNotFound.into());
        };

        self.state = DeleteSourceState::ScanReferenceVersions;
        let mut scan = ReferenceScan::new(self.input.connector_id);
        let effects = scan.start(txn_id);
        self.scan = Some(scan);
        effects
    }

    fn handle_scan_page(&mut self, event: Event) -> Effects {
        let (Some(scan), Some(txn_id)) = (self.scan.as_mut(), self.txn_id) else {
            return self.abort_with_error(StorageError::TransactionNotFound.into());
        };
        match scan.step(event, txn_id) {
            Ok(ScanStep::Referenced) => {
                self.abort_with_error(DeleteSourceError::ReferencedObjectVersion)
            }
            Ok(ScanStep::Next(effects)) => effects,
            Ok(ScanStep::Deferred) => {
                // The stale page and the pruning queue row commit; the caller retries later.
                self.state = DeleteSourceState::CommitDeferred;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            Ok(ScanStep::Complete) => self.delete_records(),
            Err(error) => self.abort_with_error(error.into()),
        }
    }

    fn handle_deferred_commit(&mut self, event: Event) -> Effects {
        self.txn_id = None;
        match event {
            Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                self.state = DeleteSourceState::ScheduleDeferred;
                smallvec![schedule_prune()]
            }
            Event::Storage(StorageEvent::Error { error }) => self.emit_error(error.into()),
            received => self.emit_error(DeleteSourceError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionCommitted)",
                received,
            }),
        }
    }

    fn handle_transaction_committed(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                self.txn_id = None;
                self.state = DeleteSourceState::Finish;
                self.output = Some(Ok(DeleteSourceResult));
                smallvec![]
            }
            Event::Storage(StorageEvent::Error { error }) => {
                self.txn_id = None;
                self.emit_error(error.into())
            }
            received => self.fail_or_abort(DeleteSourceError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionCommitted)",
                received,
            }),
        }
    }

    fn handle_transaction_aborted(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::TransactionAborted { .. })
            | Event::Storage(StorageEvent::Error { .. }) => {
                self.state = DeleteSourceState::Error;
                smallvec![]
            }
            received => self.emit_error(DeleteSourceError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionAborted)",
                received,
            }),
        }
    }

    fn delete_records(&mut self) -> Effects {
        let deletes = vec![(
            aruna_core::keyspaces::SOURCE_INDEX_KEYSPACE.to_string(),
            source_connector_key(self.input.group_id, self.input.connector_id),
        )];

        self.state = DeleteSourceState::DeleteRecords;
        smallvec![Effect::Storage(StorageEffect::BatchDelete {
            deletes,
            txn_id: self.txn_id,
        })]
    }

    fn handle_records_deleted(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::BatchDeleteResult { .. }) => {
                self.state = DeleteSourceState::DeleteSecret;
                smallvec![delete_secret_effect(self.input.connector_id, self.txn_id)]
            }
            Event::Storage(StorageEvent::Error { error }) if self.txn_id.is_some() => {
                self.abort_with_error(error.into())
            }
            Event::Storage(StorageEvent::Error { error }) => self.emit_error(error.into()),
            received => self.fail_or_abort(DeleteSourceError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::BatchDeleteResult)",
                received,
            }),
        }
    }

    fn handle_secret_deleted(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::DeleteResult { .. }) => {
                if let Some(txn_id) = self.txn_id {
                    self.state = DeleteSourceState::CommitTransaction;
                    return smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })];
                }
            }
            Event::Storage(StorageEvent::Error { error }) if self.txn_id.is_some() => {
                return self.abort_with_error(error.into());
            }
            Event::Storage(StorageEvent::Error { error }) => return self.emit_error(error.into()),
            received => {
                return self.fail_or_abort(DeleteSourceError::InvalidStateEvent {
                    state: self.state.clone(),
                    expected: "Event::Storage(StorageEvent::DeleteResult)",
                    received,
                });
            }
        }

        self.state = DeleteSourceState::Finish;
        self.output = Some(Ok(DeleteSourceResult));
        smallvec![]
    }
}

impl Operation for DeleteSourceOperation {
    type Output = DeleteSourceResult;
    type Error = DeleteSourceError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            DeleteSourceState::Init => self.handle_init(),
            DeleteSourceState::ReadConnector => self.handle_connector_read(event),
            DeleteSourceState::ReadSecret => self.handle_secret_read(event),
            DeleteSourceState::StartTransaction => self.handle_transaction_started(event),
            DeleteSourceState::ScanReferenceVersions => self.handle_scan_page(event),
            DeleteSourceState::CommitDeferred => self.handle_deferred_commit(event),
            DeleteSourceState::ScheduleDeferred => match event {
                Event::Task(_) => self.emit_error(DeleteSourceError::ReferencesPending),
                received => self.emit_error(DeleteSourceError::InvalidStateEvent {
                    state: self.state.clone(),
                    expected: "Event::Task",
                    received,
                }),
            },
            DeleteSourceState::DeleteRecords => self.handle_records_deleted(event),
            DeleteSourceState::DeleteSecret => self.handle_secret_deleted(event),
            DeleteSourceState::CommitTransaction => self.handle_transaction_committed(event),
            DeleteSourceState::AbortTransaction => self.handle_transaction_aborted(event),
            DeleteSourceState::Finish => smallvec![],
            DeleteSourceState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            DeleteSourceState::Finish | DeleteSourceState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == DeleteSourceState::Error {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(DeleteSourceError::DeleteConnectorFailed);
        }

        self.output
            .ok_or(DeleteSourceError::DeleteConnectorFailed)?
    }

    fn abort(&mut self) -> Effects {
        if let Some(txn_id) = self.txn_id.take() {
            self.state = DeleteSourceState::AbortTransaction;
            return smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })];
        }
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectors::create_connector::{SourceConnectorInput, SourceConnectorOperation};
    use crate::connectors::replace_connector::{ReplaceSourceInput, ReplaceSourceOperation};
    use crate::connectors::repository::{
        connector_reference_key, connector_reference_write, parse_secret_read, read_secret_effect,
    };
    use crate::connectors::resolver::{ResolveBindingInput, ResolveBindingOperation};
    use crate::driver::{DriverContext, drive};
    use crate::staging::descriptor::build_source_binding;
    use aruna_core::handle::Handle;
    use aruna_core::keyspaces::{
        BLOB_VERSIONS_KEYSPACE, CONNECTOR_PRUNE_KEYSPACE, CONNECTOR_REFERENCE_KEYSPACE,
        SOURCE_INDEX_KEYSPACE,
    };
    use aruna_core::structs::execution::source_access::{ResolvedSourceAccess, SourceMetadata};
    use aruna_core::structs::execution::source_connector::{
        SourceConnector, SourceConnectorKind, SourceConnectorSecret,
    };
    use aruna_core::structs::execution::staging::{StagingStrategy, VersionSourceBinding};
    use aruna_core::structs::storage::blob::{BlobVersion, VersionKey};
    use aruna_core::types::Key;
    use aruna_storage::storage;
    use std::collections::HashMap;
    use std::time::SystemTime;
    use tempfile::{TempDir, tempdir};

    fn test_context() -> (TempDir, DriverContext) {
        let tempdir = tempdir().unwrap();
        let storage_handle = storage::FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        storage_handle.open_vault(aruna_core::node_vault::NodeVaultKey::random());
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        (tempdir, context)
    }

    fn source_metadata() -> SourceMetadata {
        SourceMetadata {
            content_length: 42,
            content_type: Some("text/plain".to_string()),
            etag: None,
            last_modified: None,
            source_version: None,
            digests: Default::default(),
        }
    }

    async fn create_connector(context: &DriverContext) -> SourceConnector {
        drive(
            SourceConnectorOperation::new(SourceConnectorInput {
                group_id: ulid::Ulid::generate(),
                created_by: Default::default(),
                name: "dav-source".to_string(),
                kind: SourceConnectorKind::Webdav,
                public_config: HashMap::from([
                    (
                        "endpoint".to_string(),
                        "https://dav.example.org".to_string(),
                    ),
                    ("root".to_string(), "/datasets".to_string()),
                ]),
                secret_config: HashMap::from([
                    ("username".to_string(), "alice".to_string()),
                    ("password".to_string(), "secret".to_string()),
                ]),
            }),
            context,
        )
        .await
        .unwrap()
        .connector
    }

    async fn create_public_connector(context: &DriverContext) -> SourceConnector {
        drive(
            SourceConnectorOperation::new(SourceConnectorInput {
                group_id: ulid::Ulid::generate(),
                created_by: Default::default(),
                name: "http-source".to_string(),
                kind: SourceConnectorKind::Http,
                public_config: HashMap::from([(
                    "endpoint".to_string(),
                    "https://example.org".to_string(),
                )]),
                secret_config: HashMap::new(),
            }),
            context,
        )
        .await
        .unwrap()
        .connector
    }

    async fn write_reference_version(context: &DriverContext, source: VersionSourceBinding) {
        let version_key = VersionKey::new("bucket", "key", ulid::Ulid::generate());
        let version = BlobVersion::reference(
            source,
            source_metadata(),
            SystemTime::UNIX_EPOCH,
            Default::default(),
            SystemTime::UNIX_EPOCH,
        );
        let mut writes = vec![(
            BLOB_VERSIONS_KEYSPACE.to_string(),
            version_key.to_bytes().unwrap().into(),
            version.to_bytes().unwrap().into(),
        )];
        writes.extend(connector_reference_write(&version, &version_key).unwrap());

        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::BatchWrite {
                writes,
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::BatchWriteResult { .. })
        ));
    }

    fn connector_secret(connector_id: Ulid) -> SourceConnectorSecret {
        SourceConnectorSecret::new(
            connector_id,
            HashMap::from([("token".to_string(), "secret".to_string())]),
            SystemTime::UNIX_EPOCH,
        )
        .unwrap()
    }

    fn reference_blob_version(connector_id: Ulid) -> BlobVersion {
        let connector = SourceConnector::new(
            connector_id,
            Ulid::generate(),
            "ftp-source".to_string(),
            SourceConnectorKind::Ftp,
            HashMap::new(),
            SystemTime::UNIX_EPOCH,
            SystemTime::UNIX_EPOCH,
            Default::default(),
        );
        let source = build_source_binding(
            StagingStrategy::Reference,
            &connector,
            &source_metadata(),
            "run-1/data.txt".to_string(),
            None,
            Some(connector_id),
        );

        BlobVersion::reference(
            source,
            source_metadata(),
            SystemTime::UNIX_EPOCH,
            Default::default(),
            SystemTime::UNIX_EPOCH,
        )
    }

    #[test]
    fn scan_delete_transaction() {
        let connector_id = Ulid::from_bytes([2u8; 16]);
        let group_id = Ulid::from_bytes([1u8; 16]);
        let txn_id = Ulid::from_bytes([3u8; 16]);
        let mut operation = DeleteSourceOperation::new(DeleteSourceInput {
            group_id,
            connector_id,
        });
        operation.state = DeleteSourceState::ReadSecret;

        let effects = operation.step(Event::Storage(StorageEvent::VaultResult {
            entry: SourceConnectorSecret::vault_entry(connector_id),
            secret: Some(connector_secret(connector_id).to_secret().unwrap()),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::StartTransaction {
                read: false
            })]
        ));

        let effects = operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Iter { txn_id: Some(scan_txn), .. })]
                if *scan_txn == txn_id
        ));

        let effects = operation.step(Event::Storage(StorageEvent::IterResult {
            values: vec![],
            next_start_after: None,
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::BatchDelete { txn_id: Some(delete_txn), .. })]
                if *delete_txn == txn_id
        ));

        let effects = operation.step(Event::Storage(StorageEvent::BatchDeleteResult {
            entries: vec![],
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::VaultDelete { entry, txn_id: Some(delete_txn) })]
                if entry.id == connector_id && *delete_txn == txn_id
        ));

        let effects = operation.step(Event::Storage(StorageEvent::DeleteResult {
            key: vec![].into(),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::CommitTransaction { txn_id: commit_txn })]
                if *commit_txn == txn_id
        ));
    }

    /// An operation inside its transaction with the reference scan started.
    fn scanning(connector_id: Ulid, txn_id: Ulid) -> DeleteSourceOperation {
        let mut operation = DeleteSourceOperation::new(DeleteSourceInput {
            group_id: Ulid::from_bytes([1u8; 16]),
            connector_id,
        });
        operation.state = DeleteSourceState::StartTransaction;
        operation.has_secret = true;
        operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        operation
    }

    fn reference_rows(connector_id: Ulid, next: Option<Key>) -> Event {
        let version_key = VersionKey::new("bucket", "key", Ulid::nil());
        Event::Storage(StorageEvent::IterResult {
            values: vec![(
                connector_reference_key(connector_id, &version_key).unwrap(),
                Vec::new().into(),
            )],
            next_start_after: next,
        })
    }

    fn version_read(version: Option<BlobVersion>) -> Event {
        Event::Storage(StorageEvent::BatchReadResult {
            values: vec![(
                vec![1].into(),
                version.map(|version| version.to_bytes().unwrap().into()),
            )],
        })
    }

    fn pruned() -> Event {
        Event::Storage(StorageEvent::BatchDeleteResult {
            entries: Vec::new(),
        })
    }

    #[test]
    fn reference_aborts_delete() {
        let connector_id = Ulid::from_bytes([2u8; 16]);
        let txn_id = Ulid::from_bytes([3u8; 16]);
        let mut operation = scanning(connector_id, txn_id);

        assert!(matches!(
            operation.step(reference_rows(connector_id, None)).as_slice(),
            [Effect::Storage(StorageEffect::BatchRead { txn_id: Some(read_txn), .. })]
                if *read_txn == txn_id
        ));
        let effects = operation.step(version_read(Some(reference_blob_version(connector_id))));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { txn_id: abort_txn })]
                if *abort_txn == txn_id
        ));

        operation.step(Event::Storage(StorageEvent::TransactionAborted { txn_id }));
        assert_eq!(
            operation.finalize(),
            Err(DeleteSourceError::ReferencedObjectVersion)
        );
    }

    // A stale page with more rows behind it commits its pruning and the queued background work,
    // then asks the caller to retry instead of scanning on.
    #[test]
    fn pending_defers_check() {
        let connector_id = Ulid::from_bytes([2u8; 16]);
        let txn_id = Ulid::from_bytes([3u8; 16]);
        let mut operation = scanning(connector_id, txn_id);

        operation.step(reference_rows(connector_id, Some(vec![9u8].into())));
        assert!(matches!(
            operation.step(version_read(None)).as_slice(),
            [Effect::Storage(StorageEffect::BatchDelete { deletes, .. })]
                if deletes[0].0 == CONNECTOR_REFERENCE_KEYSPACE
        ));
        assert!(matches!(
            operation.step(pruned()).as_slice(),
            [Effect::Storage(StorageEffect::Write { key_space, .. })]
                if key_space == CONNECTOR_PRUNE_KEYSPACE
        ));
        let effects = operation.step(Event::Storage(StorageEvent::WriteResult {
            key: vec![].into(),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::CommitTransaction { txn_id: commit_txn })]
                if *commit_txn == txn_id
        ));
        let effects = operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id,
        }));
        assert!(matches!(effects.as_slice(), [Effect::Task(_)]));
        operation.step(Event::Task(aruna_core::task::TaskEvent::TimerScheduled {
            key: aruna_core::task::TaskKey::PruneConnectorReferences,
            after: std::time::Duration::ZERO,
        }));

        assert_eq!(
            operation.finalize(),
            Err(DeleteSourceError::ReferencesPending)
        );
    }

    #[test]
    fn scan_failure_aborts() {
        let txn_id = Ulid::from_bytes([3u8; 16]);
        let mut operation = scanning(Ulid::from_bytes([2u8; 16]), txn_id);

        let effects = operation.step(Event::Storage(StorageEvent::WriteResult {
            key: vec![].into(),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { txn_id: abort_txn })]
                if *abort_txn == txn_id
        ));

        operation.step(Event::Storage(StorageEvent::TransactionAborted { txn_id }));
        assert!(matches!(
            operation.finalize(),
            Err(DeleteSourceError::StorageError(StorageError::ReadError(_)))
        ));
    }

    #[test]
    fn commit_failure_reports() {
        let txn_id = Ulid::from_bytes([3u8; 16]);
        let mut operation = DeleteSourceOperation::new(DeleteSourceInput {
            group_id: Ulid::from_bytes([1u8; 16]),
            connector_id: Ulid::from_bytes([2u8; 16]),
        });
        operation.state = DeleteSourceState::CommitTransaction;
        operation.txn_id = Some(txn_id);

        let effects = operation.step(Event::Storage(StorageEvent::Error {
            error: StorageError::ReadError("commit failed".to_string()),
        }));
        assert!(effects.is_empty());
        assert!(operation.txn_id.is_none());
        assert_eq!(
            operation.finalize(),
            Err(DeleteSourceError::StorageError(StorageError::ReadError(
                "commit failed".to_string()
            )))
        );
    }

    #[test]
    fn invalid_commit_aborts() {
        let txn_id = Ulid::from_bytes([3u8; 16]);
        let mut operation = DeleteSourceOperation::new(DeleteSourceInput {
            group_id: Ulid::from_bytes([1u8; 16]),
            connector_id: Ulid::from_bytes([2u8; 16]),
        });
        operation.state = DeleteSourceState::CommitTransaction;
        operation.txn_id = Some(txn_id);

        let effects = operation.step(Event::Storage(StorageEvent::WriteResult {
            key: vec![].into(),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { txn_id: abort_txn })]
                if *abort_txn == txn_id
        ));

        operation.step(Event::Storage(StorageEvent::TransactionAborted { txn_id }));
        assert!(matches!(
            operation.finalize(),
            Err(DeleteSourceError::InvalidStateEvent {
                expected: "Event::Storage(StorageEvent::TransactionCommitted)",
                ..
            })
        ));
    }

    #[test]
    fn abort_emits_transaction() {
        let txn_id = Ulid::from_bytes([3u8; 16]);
        let mut operation = DeleteSourceOperation::new(DeleteSourceInput {
            group_id: Ulid::from_bytes([1u8; 16]),
            connector_id: Ulid::from_bytes([2u8; 16]),
        });
        operation.txn_id = Some(txn_id);

        let effects = operation.abort();
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { txn_id: abort_txn })]
                if *abort_txn == txn_id
        ));
        assert!(operation.txn_id.is_none());
    }

    #[tokio::test]
    async fn rejects_referenced_connector() {
        let (_tempdir, context) = test_context();
        let connector = create_connector(&context).await;
        let source = build_source_binding(
            StagingStrategy::Reference,
            &connector,
            &source_metadata(),
            "run-1/data.txt".to_string(),
            None,
            Some(connector.connector_id),
        );
        write_reference_version(&context, source.clone()).await;

        let result = drive(
            DeleteSourceOperation::new(DeleteSourceInput {
                group_id: connector.group_id,
                connector_id: connector.connector_id,
            }),
            &context,
        )
        .await;

        assert_eq!(result, Err(DeleteSourceError::ReferencedObjectVersion));

        let access = drive(
            ResolveBindingOperation::new(ResolveBindingInput { source }),
            &context,
        )
        .await
        .unwrap();
        let ResolvedSourceAccess::OpenDal { config, .. } = access;
        assert_eq!(config.get("username").map(String::as_str), Some("alice"));
    }

    #[tokio::test]
    async fn allows_public_references() {
        let (_tempdir, context) = test_context();
        let connector = create_public_connector(&context).await;
        let source = build_source_binding(
            StagingStrategy::Reference,
            &connector,
            &source_metadata(),
            "run-1/data.txt".to_string(),
            None,
            Some(connector.connector_id),
        );
        write_reference_version(&context, source.clone()).await;

        drive(
            DeleteSourceOperation::new(DeleteSourceInput {
                group_id: connector.group_id,
                connector_id: connector.connector_id,
            }),
            &context,
        )
        .await
        .unwrap();

        let access = drive(
            ResolveBindingOperation::new(ResolveBindingInput { source }),
            &context,
        )
        .await
        .unwrap();
        let ResolvedSourceAccess::OpenDal { config, .. } = access;
        assert_eq!(
            config.get("endpoint").map(String::as_str),
            Some("https://example.org")
        );
    }

    #[tokio::test]
    async fn public_delete_abort() {
        let (_tempdir, context) = test_context();
        let connector = create_public_connector(&context).await;
        let input = DeleteSourceInput {
            group_id: connector.group_id,
            connector_id: connector.connector_id,
        };
        let mut operation = DeleteSourceOperation::new(input.clone());
        let mut effects = operation.start();
        for _ in 0..2 {
            assert_eq!(effects.len(), 1);
            let event = context.storage_handle.send_effect(effects.remove(0)).await;
            effects = operation.step(event);
        }

        drive(
            ReplaceSourceOperation::new(ReplaceSourceInput {
                group_id: connector.group_id,
                connector_id: connector.connector_id,
                name: connector.name.clone(),
                kind: connector.kind,
                public_config: connector.public_config.clone(),
                secret_config: HashMap::from([("token".to_string(), "secret".to_string())]),
            }),
            &context,
        )
        .await
        .unwrap();

        if matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::StartTransaction { .. })]
        ) {
            let event = context.storage_handle.send_effect(effects.remove(0)).await;
            effects = operation.step(event);
        }
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::BatchDelete { .. })]
        ));
        let event = context.storage_handle.send_effect(effects.remove(0)).await;
        let effects = operation.step(event);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::VaultDelete { .. })]
        ));
        for effect in operation.abort() {
            let event = context.storage_handle.send_effect(effect).await;
            assert!(matches!(
                event,
                Event::Storage(StorageEvent::TransactionAborted { .. })
            ));
        }

        let event = context
            .storage_handle
            .send_effect(read_connector_effect(
                input.group_id,
                input.connector_id,
                None,
            ))
            .await;
        assert!(parse_connector_read(event).unwrap().is_some());
        let event = context
            .storage_handle
            .send_effect(read_secret_effect(input.connector_id, None))
            .await;
        assert!(
            parse_secret_read(event, input.connector_id)
                .unwrap()
                .is_some()
        );

        drive(DeleteSourceOperation::new(input.clone()), &context)
            .await
            .unwrap();
        let event = context
            .storage_handle
            .send_effect(read_connector_effect(
                input.group_id,
                input.connector_id,
                None,
            ))
            .await;
        assert!(parse_connector_read(event).unwrap().is_none());
        let event = context
            .storage_handle
            .send_effect(read_secret_effect(input.connector_id, None))
            .await;
        assert!(
            parse_secret_read(event, input.connector_id)
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn deletes_unreferenced_secret() {
        let (_tempdir, context) = test_context();
        let connector = create_connector(&context).await;

        drive(
            DeleteSourceOperation::new(DeleteSourceInput {
                group_id: connector.group_id,
                connector_id: connector.connector_id,
            }),
            &context,
        )
        .await
        .unwrap();

        let secret_event = context
            .storage_handle
            .send_effect(read_secret_effect(connector.connector_id, None))
            .await;
        assert!(
            parse_secret_read(secret_event, connector.connector_id)
                .unwrap()
                .is_none()
        );
    }
}
