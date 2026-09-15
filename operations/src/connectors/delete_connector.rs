use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::types::{Effects, GroupId, Key, TxnId};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::connectors::reference_scan::{ScanStep, parse_scan_page};
use crate::connectors::repository::{
    StorageReadError, connector_secret_key, parse_connector_read, parse_secret_read,
    read_connector_effect, read_secret_effect, reference_scan_effect, source_connector_key,
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
    DeleteRecords,
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
    ReferencedByObjectVersion,
    #[error("DeleteSourceConnector failed")]
    DeleteSourceConnectorFailed,
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
    output: Option<Result<DeleteSourceResult, DeleteSourceError>>,
}

impl DeleteSourceOperation {
    pub fn new(input: DeleteSourceInput) -> Self {
        Self {
            input,
            state: DeleteSourceState::Init,
            txn_id: None,
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
        let secret = match parse_secret_read(event) {
            Ok(secret) => secret,
            Err(error) => return self.emit_error(error.into()),
        };

        if secret.is_none() {
            return self.delete_records();
        }

        self.state = DeleteSourceState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn handle_transaction_started(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                self.txn_id = Some(txn_id);
                self.scan_reference_versions(None)
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

    fn scan_reference_versions(&mut self, start_after: Option<Key>) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(StorageError::TransactionNotFound.into());
        };

        self.state = DeleteSourceState::ScanReferenceVersions;
        smallvec![reference_scan_effect(start_after, Some(txn_id),)]
    }

    fn handle_scan_page(&mut self, event: Event) -> Effects {
        match parse_scan_page(event, self.input.connector_id) {
            Ok(ScanStep::Referenced) => {
                self.abort_with_error(DeleteSourceError::ReferencedByObjectVersion)
            }
            Ok(ScanStep::NextPage(start_after)) => self.scan_reference_versions(Some(start_after)),
            Ok(ScanStep::Complete) => self.delete_records(),
            Err(error) => self.abort_with_error(error.into()),
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
        let deletes = vec![
            (
                aruna_core::keyspaces::SOURCE_CONNECTOR_INDEX_KEYSPACE.to_string(),
                source_connector_key(self.input.group_id, self.input.connector_id),
            ),
            (
                aruna_core::keyspaces::SOURCE_CONNECTOR_SECRET_KEYSPACE.to_string(),
                connector_secret_key(self.input.connector_id),
            ),
        ];

        self.state = DeleteSourceState::DeleteRecords;
        smallvec![Effect::Storage(StorageEffect::BatchDelete {
            deletes,
            txn_id: self.txn_id,
        })]
    }

    fn handle_records_deleted(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::BatchDeleteResult { .. }) => {
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
                    expected: "Event::Storage(StorageEvent::BatchDeleteResult)",
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
            DeleteSourceState::DeleteRecords => self.handle_records_deleted(event),
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
            return Err(DeleteSourceError::DeleteSourceConnectorFailed);
        }

        self.output
            .ok_or(DeleteSourceError::DeleteSourceConnectorFailed)?
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
    use crate::connectors::repository::{parse_secret_read, read_secret_effect};
    use crate::connectors::resolver::{ResolveBindingInput, ResolveBindingOperation};
    use crate::driver::{DriverContext, drive};
    use crate::staging::descriptor::build_source_binding;
    use aruna_core::effects::IterStart;
    use aruna_core::handle::Handle;
    use aruna_core::keyspaces::BLOB_VERSIONS_KEYSPACE;
    use aruna_core::structs::storage::blob::{BlobVersion, VersionKey};
    use aruna_core::structs::execution::source_access::{ResolvedSourceAccess, SourceMetadata};
    use aruna_core::structs::execution::source_connector::{
        SourceConnector, SourceConnectorKind, SourceConnectorSecret,
    };
    use aruna_core::structs::execution::staging::{StagingStrategy, VersionSourceBinding};
    use aruna_storage::storage;
    use std::collections::HashMap;
    use std::time::SystemTime;
    use tempfile::{TempDir, tempdir};

    fn test_context() -> (TempDir, DriverContext) {
        let tempdir = tempdir().unwrap();
        let storage_handle = storage::FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
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
        let version_id = ulid::Ulid::generate();
        let key = VersionKey::new("bucket", "key", version_id)
            .to_bytes()
            .unwrap();
        let value = BlobVersion::reference(
            source,
            source_metadata(),
            SystemTime::UNIX_EPOCH,
            Default::default(),
            SystemTime::UNIX_EPOCH,
        )
        .to_bytes()
        .unwrap();

        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: key.into(),
                value: value.into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
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

        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![].into(),
            value: Some(connector_secret(connector_id).to_bytes().unwrap().into()),
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
            [Effect::Storage(StorageEffect::CommitTransaction { txn_id: commit_txn })]
                if *commit_txn == txn_id
        ));
    }

    #[test]
    fn reference_aborts_delete() {
        let connector_id = Ulid::from_bytes([2u8; 16]);
        let txn_id = Ulid::from_bytes([3u8; 16]);
        let mut operation = DeleteSourceOperation::new(DeleteSourceInput {
            group_id: Ulid::from_bytes([1u8; 16]),
            connector_id,
        });
        operation.state = DeleteSourceState::ScanReferenceVersions;
        operation.txn_id = Some(txn_id);

        let effects = operation.step(Event::Storage(StorageEvent::IterResult {
            values: vec![(
                vec![1].into(),
                reference_blob_version(connector_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
            )],
            next_start_after: None,
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { txn_id: abort_txn })]
                if *abort_txn == txn_id
        ));

        operation.step(Event::Storage(StorageEvent::TransactionAborted { txn_id }));
        assert_eq!(
            operation.finalize(),
            Err(DeleteSourceError::ReferencedByObjectVersion)
        );
    }

    #[test]
    fn scans_multiple_pages() {
        let connector_id = Ulid::from_bytes([2u8; 16]);
        let txn_id = Ulid::from_bytes([3u8; 16]);
        let next_key: Key = vec![9u8].into();
        let mut operation = DeleteSourceOperation::new(DeleteSourceInput {
            group_id: Ulid::from_bytes([1u8; 16]),
            connector_id,
        });
        operation.state = DeleteSourceState::ScanReferenceVersions;
        operation.txn_id = Some(txn_id);

        let effects = operation.step(Event::Storage(StorageEvent::IterResult {
            values: vec![],
            next_start_after: Some(next_key.clone()),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Iter {
                start: Some(IterStart::After(key)),
                txn_id: Some(scan_txn),
                ..
            })] if *scan_txn == txn_id && key.as_ref() == next_key.as_ref()
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
    }

    #[test]
    fn later_page_aborts() {
        let connector_id = Ulid::from_bytes([2u8; 16]);
        let txn_id = Ulid::from_bytes([3u8; 16]);
        let next_key: Key = vec![9u8].into();
        let mut operation = DeleteSourceOperation::new(DeleteSourceInput {
            group_id: Ulid::from_bytes([1u8; 16]),
            connector_id,
        });
        operation.state = DeleteSourceState::ScanReferenceVersions;
        operation.txn_id = Some(txn_id);

        let effects = operation.step(Event::Storage(StorageEvent::IterResult {
            values: vec![],
            next_start_after: Some(next_key.clone()),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Iter {
                start: Some(IterStart::After(key)),
                ..
            })] if key.as_ref() == next_key.as_ref()
        ));

        let effects = operation.step(Event::Storage(StorageEvent::IterResult {
            values: vec![(
                vec![1].into(),
                reference_blob_version(connector_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
            )],
            next_start_after: None,
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { txn_id: abort_txn })]
                if *abort_txn == txn_id
        ));

        operation.step(Event::Storage(StorageEvent::TransactionAborted { txn_id }));
        assert_eq!(
            operation.finalize(),
            Err(DeleteSourceError::ReferencedByObjectVersion)
        );
    }

    #[test]
    fn scan_failure_aborts() {
        let txn_id = Ulid::from_bytes([3u8; 16]);
        let mut operation = DeleteSourceOperation::new(DeleteSourceInput {
            group_id: Ulid::from_bytes([1u8; 16]),
            connector_id: Ulid::from_bytes([2u8; 16]),
        });
        operation.state = DeleteSourceState::ScanReferenceVersions;
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
        assert_eq!(
            operation.finalize(),
            Err(DeleteSourceError::StorageError(StorageError::ReadError(
                "unexpected event".to_string()
            )))
        );
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

        assert_eq!(result, Err(DeleteSourceError::ReferencedByObjectVersion));

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
        assert!(parse_secret_read(secret_event).unwrap().is_none());
    }
}
