use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::types::{Effects, GroupId, TxnId};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::connectors::repository::{
    StorageReadError, blob_version_references_connector, iter_connector_reference_versions_effect,
    parse_blob_version_iter, parse_connector_read, parse_connector_secret_read,
    read_connector_effect, read_connector_secret_effect, source_connector_key,
    source_connector_secret_key,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteSourceConnectorInput {
    pub group_id: GroupId,
    pub connector_id: Ulid,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteSourceConnectorResult;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeleteSourceConnectorState {
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
pub enum DeleteSourceConnectorError {
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
        state: DeleteSourceConnectorState,
        expected: &'static str,
        received: Event,
    },
}

impl From<StorageReadError> for DeleteSourceConnectorError {
    fn from(value: StorageReadError) -> Self {
        match value {
            StorageReadError::Storage(error) => Self::StorageError(error),
            StorageReadError::Conversion(error) => Self::ConversionError(error),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct DeleteSourceConnectorOperation {
    input: DeleteSourceConnectorInput,
    state: DeleteSourceConnectorState,
    txn_id: Option<TxnId>,
    output: Option<Result<DeleteSourceConnectorResult, DeleteSourceConnectorError>>,
}

impl DeleteSourceConnectorOperation {
    pub fn new(input: DeleteSourceConnectorInput) -> Self {
        Self {
            input,
            state: DeleteSourceConnectorState::Init,
            txn_id: None,
            output: None,
        }
    }

    fn emit_error(&mut self, error: DeleteSourceConnectorError) -> Effects {
        self.state = DeleteSourceConnectorState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn abort_with_error(&mut self, error: DeleteSourceConnectorError) -> Effects {
        let Some(txn_id) = self.txn_id.take() else {
            return self.emit_error(error);
        };

        self.state = DeleteSourceConnectorState::AbortTransaction;
        self.output = Some(Err(error));
        smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
    }

    fn fail_or_abort(&mut self, error: DeleteSourceConnectorError) -> Effects {
        if self.txn_id.is_some() {
            self.abort_with_error(error)
        } else {
            self.emit_error(error)
        }
    }

    fn handle_init(&mut self) -> Effects {
        self.state = DeleteSourceConnectorState::ReadConnector;
        smallvec![read_connector_effect(
            self.input.group_id,
            self.input.connector_id,
            None,
        )]
    }

    fn handle_connector_read(&mut self, event: Event) -> Effects {
        match parse_connector_read(event) {
            Ok(Some(_)) => {
                self.state = DeleteSourceConnectorState::ReadSecret;
                smallvec![read_connector_secret_effect(self.input.connector_id, None)]
            }
            Ok(None) => self.emit_error(DeleteSourceConnectorError::NotFound),
            Err(error) => self.emit_error(error.into()),
        }
    }

    fn handle_secret_read(&mut self, event: Event) -> Effects {
        let secret = match parse_connector_secret_read(event) {
            Ok(secret) => secret,
            Err(error) => return self.emit_error(error.into()),
        };

        if secret.is_none() {
            return self.delete_records();
        }

        self.state = DeleteSourceConnectorState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn handle_transaction_started(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                self.txn_id = Some(txn_id);
                self.state = DeleteSourceConnectorState::ScanReferenceVersions;
                smallvec![iter_connector_reference_versions_effect(None, Some(txn_id))]
            }
            Event::Storage(StorageEvent::Error { error }) => self.emit_error(error.into()),
            received => self.emit_error(DeleteSourceConnectorError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionStarted)",
                received,
            }),
        }
    }

    fn current_txn_id(&mut self) -> Result<TxnId, Effects> {
        self.txn_id.ok_or_else(|| {
            self.emit_error(DeleteSourceConnectorError::StorageError(
                StorageError::TransactionNotFound,
            ))
        })
    }

    fn scan_reference_versions(&mut self, start_after: Option<aruna_core::types::Key>) -> Effects {
        let txn_id = match self.current_txn_id() {
            Ok(txn_id) => txn_id,
            Err(effects) => return effects,
        };

        self.state = DeleteSourceConnectorState::ScanReferenceVersions;
        smallvec![iter_connector_reference_versions_effect(
            start_after,
            Some(txn_id),
        )]
    }

    fn handle_reference_versions_scanned(&mut self, event: Event) -> Effects {
        let (versions, next_start_after) = match parse_blob_version_iter(event) {
            Ok(result) => result,
            Err(error) => return self.abort_with_error(error.into()),
        };

        if versions
            .iter()
            .any(|version| blob_version_references_connector(version, self.input.connector_id))
        {
            return self.abort_with_error(DeleteSourceConnectorError::ReferencedByObjectVersion);
        }

        if let Some(start_after) = next_start_after {
            return self.scan_reference_versions(Some(start_after));
        }

        self.delete_records()
    }

    fn delete_records(&mut self) -> Effects {
        let deletes = vec![
            (
                aruna_core::keyspaces::SOURCE_CONNECTOR_INDEX_KEYSPACE.to_string(),
                source_connector_key(self.input.group_id, self.input.connector_id),
            ),
            (
                aruna_core::keyspaces::SOURCE_CONNECTOR_SECRET_KEYSPACE.to_string(),
                source_connector_secret_key(self.input.connector_id),
            ),
        ];

        self.state = DeleteSourceConnectorState::DeleteRecords;
        smallvec![Effect::Storage(StorageEffect::BatchDelete {
            deletes,
            txn_id: self.txn_id,
        })]
    }

    fn handle_records_deleted(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::BatchDeleteResult { .. }) => {
                if let Some(txn_id) = self.txn_id {
                    self.state = DeleteSourceConnectorState::CommitTransaction;
                    return smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })];
                }
            }
            Event::Storage(StorageEvent::Error { error }) if self.txn_id.is_some() => {
                return self.abort_with_error(error.into());
            }
            Event::Storage(StorageEvent::Error { error }) => return self.emit_error(error.into()),
            received => {
                return self.fail_or_abort(DeleteSourceConnectorError::InvalidStateEvent {
                    state: self.state.clone(),
                    expected: "Event::Storage(StorageEvent::BatchDeleteResult)",
                    received,
                });
            }
        }

        self.state = DeleteSourceConnectorState::Finish;
        self.output = Some(Ok(DeleteSourceConnectorResult));
        smallvec![]
    }

    fn handle_transaction_committed(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                self.txn_id = None;
                self.state = DeleteSourceConnectorState::Finish;
                self.output = Some(Ok(DeleteSourceConnectorResult));
                smallvec![]
            }
            Event::Storage(StorageEvent::Error { error }) => {
                self.txn_id = None;
                self.emit_error(error.into())
            }
            received => self.fail_or_abort(DeleteSourceConnectorError::InvalidStateEvent {
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
                self.state = DeleteSourceConnectorState::Error;
                smallvec![]
            }
            received => self.emit_error(DeleteSourceConnectorError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionAborted)",
                received,
            }),
        }
    }
}

impl Operation for DeleteSourceConnectorOperation {
    type Output = DeleteSourceConnectorResult;
    type Error = DeleteSourceConnectorError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            DeleteSourceConnectorState::Init => self.handle_init(),
            DeleteSourceConnectorState::ReadConnector => self.handle_connector_read(event),
            DeleteSourceConnectorState::ReadSecret => self.handle_secret_read(event),
            DeleteSourceConnectorState::StartTransaction => self.handle_transaction_started(event),
            DeleteSourceConnectorState::ScanReferenceVersions => {
                self.handle_reference_versions_scanned(event)
            }
            DeleteSourceConnectorState::DeleteRecords => self.handle_records_deleted(event),
            DeleteSourceConnectorState::CommitTransaction => {
                self.handle_transaction_committed(event)
            }
            DeleteSourceConnectorState::AbortTransaction => self.handle_transaction_aborted(event),
            DeleteSourceConnectorState::Finish => smallvec![],
            DeleteSourceConnectorState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            DeleteSourceConnectorState::Finish | DeleteSourceConnectorState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == DeleteSourceConnectorState::Error {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(DeleteSourceConnectorError::DeleteSourceConnectorFailed);
        }

        self.output
            .ok_or(DeleteSourceConnectorError::DeleteSourceConnectorFailed)?
    }

    fn abort(&mut self) -> Effects {
        if let Some(txn_id) = self.txn_id.take() {
            self.state = DeleteSourceConnectorState::AbortTransaction;
            return smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })];
        }
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectors::create_source_connector::{
        CreateSourceConnectorInput, CreateSourceConnectorOperation,
    };
    use crate::connectors::repository::{
        parse_connector_secret_read, read_connector_secret_effect,
    };
    use crate::connectors::resolver::{
        ResolveVersionSourceBindingInput, ResolveVersionSourceBindingOperation,
    };
    use crate::driver::{DriverContext, drive};
    use crate::staging::descriptor::build_version_source_binding;
    use aruna_core::handle::Handle;
    use aruna_core::keyspaces::BLOB_VERSIONS_KEYSPACE;
    use aruna_core::structs::{
        BlobVersion, ResolvedSourceAccess, SourceConnector, SourceConnectorKind,
        SourceConnectorSecret, SourceMetadata, StagingStrategy, VersionKey, VersionSourceBinding,
    };
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
            automerge_handle: None,
            metadata_handle: None,
            task_handle: None,
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
            CreateSourceConnectorOperation::new(CreateSourceConnectorInput {
                group_id: ulid::Ulid::new(),
                created_by: Default::default(),
                name: "ftp-source".to_string(),
                kind: SourceConnectorKind::Ftp,
                public_config: HashMap::from([
                    (
                        "endpoint".to_string(),
                        "ftp://ftp.example.org:21".to_string(),
                    ),
                    ("root".to_string(), "/datasets".to_string()),
                ]),
                secret_config: HashMap::from([
                    ("user".to_string(), "alice".to_string()),
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
            CreateSourceConnectorOperation::new(CreateSourceConnectorInput {
                group_id: ulid::Ulid::new(),
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
        let version_id = ulid::Ulid::new();
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
            Ulid::new(),
            "ftp-source".to_string(),
            SourceConnectorKind::Ftp,
            HashMap::new(),
            SystemTime::UNIX_EPOCH,
            SystemTime::UNIX_EPOCH,
            Default::default(),
        );
        let source = build_version_source_binding(
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
    fn delete_with_secret_scans_deletes_and_commits_in_same_transaction() {
        let connector_id = Ulid::from_bytes([2u8; 16]);
        let group_id = Ulid::from_bytes([1u8; 16]);
        let txn_id = Ulid::from_bytes([3u8; 16]);
        let mut operation = DeleteSourceConnectorOperation::new(DeleteSourceConnectorInput {
            group_id,
            connector_id,
        });
        operation.state = DeleteSourceConnectorState::ReadSecret;

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
    fn delete_with_secret_aborts_transaction_when_reference_is_found() {
        let connector_id = Ulid::from_bytes([2u8; 16]);
        let txn_id = Ulid::from_bytes([3u8; 16]);
        let mut operation = DeleteSourceConnectorOperation::new(DeleteSourceConnectorInput {
            group_id: Ulid::from_bytes([1u8; 16]),
            connector_id,
        });
        operation.state = DeleteSourceConnectorState::ScanReferenceVersions;
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
            Err(DeleteSourceConnectorError::ReferencedByObjectVersion)
        );
    }

    #[tokio::test]
    async fn delete_source_connector_rejects_when_reference_versions_exist() {
        let (_tempdir, context) = test_context();
        let connector = create_connector(&context).await;
        let source = build_version_source_binding(
            StagingStrategy::Reference,
            &connector,
            &source_metadata(),
            "run-1/data.txt".to_string(),
            None,
            Some(connector.connector_id),
        );
        write_reference_version(&context, source.clone()).await;

        let result = drive(
            DeleteSourceConnectorOperation::new(DeleteSourceConnectorInput {
                group_id: connector.group_id,
                connector_id: connector.connector_id,
            }),
            &context,
        )
        .await;

        assert_eq!(
            result,
            Err(DeleteSourceConnectorError::ReferencedByObjectVersion)
        );

        let access = drive(
            ResolveVersionSourceBindingOperation::new(ResolveVersionSourceBindingInput { source }),
            &context,
        )
        .await
        .unwrap();
        let ResolvedSourceAccess::OpenDal { config, .. } = access;
        assert_eq!(config.get("user").map(String::as_str), Some("alice"));
    }

    #[tokio::test]
    async fn delete_source_connector_allows_public_only_connector_with_reference_versions() {
        let (_tempdir, context) = test_context();
        let connector = create_public_connector(&context).await;
        let source = build_version_source_binding(
            StagingStrategy::Reference,
            &connector,
            &source_metadata(),
            "run-1/data.txt".to_string(),
            None,
            Some(connector.connector_id),
        );
        write_reference_version(&context, source.clone()).await;

        drive(
            DeleteSourceConnectorOperation::new(DeleteSourceConnectorInput {
                group_id: connector.group_id,
                connector_id: connector.connector_id,
            }),
            &context,
        )
        .await
        .unwrap();

        let access = drive(
            ResolveVersionSourceBindingOperation::new(ResolveVersionSourceBindingInput { source }),
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
    async fn delete_source_connector_deletes_secret_without_reference_versions() {
        let (_tempdir, context) = test_context();
        let connector = create_connector(&context).await;

        drive(
            DeleteSourceConnectorOperation::new(DeleteSourceConnectorInput {
                group_id: connector.group_id,
                connector_id: connector.connector_id,
            }),
            &context,
        )
        .await
        .unwrap();

        let secret_event = context
            .storage_handle
            .send_effect(read_connector_secret_effect(connector.connector_id, None))
            .await;
        assert!(parse_connector_secret_read(secret_event).unwrap().is_none());
    }
}
