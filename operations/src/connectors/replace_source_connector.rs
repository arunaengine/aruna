use std::collections::HashMap;
use std::time::SystemTime;

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::{SourceConnector, SourceConnectorKind, SourceConnectorSecret};
use aruna_core::types::{Effects, GroupId, TxnId};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::connectors::repository::{
    StorageReadError, blob_version_references_connector, delete_connector_secret_effect,
    iter_connector_reference_versions_effect, parse_blob_version_iter, parse_connector_read,
    parse_connector_secret_read, read_connector_effect, read_connector_secret_effect,
    source_connector_key, source_connector_secret_key,
};
use crate::connectors::validation::{ValidationError, validate_connector_input};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplaceSourceConnectorInput {
    pub group_id: GroupId,
    pub connector_id: Ulid,
    pub name: String,
    pub kind: SourceConnectorKind,
    pub public_config: HashMap<String, String>,
    pub secret_config: HashMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplaceSourceConnectorResult {
    pub connector: SourceConnector,
    pub has_secret_config: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplaceSourceConnectorState {
    Init,
    ReadCurrent,
    ReadSecret,
    StartTransaction,
    ScanReferenceVersions,
    WriteRecords,
    DeleteSecret,
    CommitTransaction,
    AbortTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum ReplaceSourceConnectorError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    ValidationError(#[from] ValidationError),
    #[error("Connector not found")]
    NotFound,
    #[error("Connector credentials are referenced by object versions")]
    ReferencedByObjectVersion,
    #[error("ReplaceSourceConnector failed")]
    ReplaceSourceConnectorFailed,
    #[error("State [{state:?}] invalid: expected [{expected}] - received [{received:?}]")]
    InvalidStateEvent {
        state: ReplaceSourceConnectorState,
        expected: &'static str,
        received: Event,
    },
}

impl From<StorageReadError> for ReplaceSourceConnectorError {
    fn from(value: StorageReadError) -> Self {
        match value {
            StorageReadError::Storage(error) => Self::StorageError(error),
            StorageReadError::Conversion(error) => Self::ConversionError(error),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct ReplaceSourceConnectorOperation {
    input: ReplaceSourceConnectorInput,
    state: ReplaceSourceConnectorState,
    replacement: Option<SourceConnector>,
    replacement_secret: Option<SourceConnectorSecret>,
    txn_id: Option<TxnId>,
    output: Option<Result<ReplaceSourceConnectorResult, ReplaceSourceConnectorError>>,
}

impl ReplaceSourceConnectorOperation {
    pub fn new(input: ReplaceSourceConnectorInput) -> Self {
        Self {
            input,
            state: ReplaceSourceConnectorState::Init,
            replacement: None,
            replacement_secret: None,
            txn_id: None,
            output: None,
        }
    }

    fn emit_error(&mut self, error: ReplaceSourceConnectorError) -> Effects {
        self.state = ReplaceSourceConnectorState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn abort_with_error(&mut self, error: ReplaceSourceConnectorError) -> Effects {
        let Some(txn_id) = self.txn_id.take() else {
            return self.emit_error(error);
        };

        self.state = ReplaceSourceConnectorState::AbortTransaction;
        self.output = Some(Err(error));
        smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
    }

    fn fail_or_abort(&mut self, error: ReplaceSourceConnectorError) -> Effects {
        if self.txn_id.is_some() {
            self.abort_with_error(error)
        } else {
            self.emit_error(error)
        }
    }

    fn handle_init(&mut self) -> Effects {
        if let Err(error) = validate_connector_input(
            &self.input.name,
            self.input.kind,
            &self.input.public_config,
            &self.input.secret_config,
        ) {
            return self.emit_error(error.into());
        }

        self.state = ReplaceSourceConnectorState::ReadCurrent;
        smallvec![read_connector_effect(
            self.input.group_id,
            self.input.connector_id,
            None,
        )]
    }

    fn handle_current_read(&mut self, event: Event) -> Effects {
        match parse_connector_read(event) {
            Ok(Some(existing)) => {
                self.prepare_replacement(existing);
                self.state = ReplaceSourceConnectorState::ReadSecret;
                smallvec![read_connector_secret_effect(self.input.connector_id, None)]
            }
            Ok(None) => self.emit_error(ReplaceSourceConnectorError::NotFound),
            Err(error) => self.emit_error(error.into()),
        }
    }

    fn prepare_replacement(&mut self, existing: SourceConnector) {
        let now = SystemTime::now();
        self.replacement = Some(SourceConnector::new(
            existing.connector_id,
            existing.group_id,
            self.input.name.clone(),
            self.input.kind,
            self.input.public_config.clone(),
            existing.created_at,
            now,
            existing.created_by,
        ));
        self.replacement_secret = SourceConnectorSecret::new(
            existing.connector_id,
            self.input.secret_config.clone(),
            now,
        );
    }

    fn handle_current_secret_read(&mut self, event: Event) -> Effects {
        let current_secret = match parse_connector_secret_read(event) {
            Ok(secret) => secret,
            Err(error) => return self.emit_error(error.into()),
        };

        if secret_config_changed(current_secret.as_ref(), self.replacement_secret.as_ref()) {
            self.state = ReplaceSourceConnectorState::StartTransaction;
            return smallvec![Effect::Storage(StorageEffect::StartTransaction {
                read: false
            })];
        }

        self.write_records()
    }

    fn handle_transaction_started(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                self.txn_id = Some(txn_id);
                self.scan_reference_versions(None)
            }
            Event::Storage(StorageEvent::Error { error }) => self.emit_error(error.into()),
            received => self.emit_error(ReplaceSourceConnectorError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionStarted)",
                received,
            }),
        }
    }

    fn current_txn_id(&mut self) -> Result<TxnId, Effects> {
        self.txn_id.ok_or_else(|| {
            self.emit_error(ReplaceSourceConnectorError::StorageError(
                StorageError::TransactionNotFound,
            ))
        })
    }

    fn scan_reference_versions(&mut self, start_after: Option<aruna_core::types::Key>) -> Effects {
        let txn_id = match self.current_txn_id() {
            Ok(txn_id) => txn_id,
            Err(effects) => return effects,
        };

        self.state = ReplaceSourceConnectorState::ScanReferenceVersions;
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
            return self.abort_with_error(ReplaceSourceConnectorError::ReferencedByObjectVersion);
        }

        if let Some(start_after) = next_start_after {
            return self.scan_reference_versions(Some(start_after));
        }

        self.write_records()
    }

    fn write_records(&mut self) -> Effects {
        let Some(replacement) = self.replacement.clone() else {
            return self.fail_or_abort(ReplaceSourceConnectorError::ReplaceSourceConnectorFailed);
        };

        self.state = ReplaceSourceConnectorState::WriteRecords;

        if let Some(secret) = self.replacement_secret.as_ref() {
            let connector_bytes = match replacement.to_bytes() {
                Ok(bytes) => bytes,
                Err(error) => return self.fail_or_abort(error.into()),
            };
            let secret_bytes = match secret.to_bytes() {
                Ok(bytes) => bytes,
                Err(error) => return self.fail_or_abort(error.into()),
            };
            smallvec![Effect::Storage(StorageEffect::BatchWrite {
                writes: vec![
                    (
                        aruna_core::keyspaces::SOURCE_CONNECTOR_INDEX_KEYSPACE.to_string(),
                        source_connector_key(replacement.group_id, replacement.connector_id),
                        connector_bytes.into(),
                    ),
                    (
                        aruna_core::keyspaces::SOURCE_CONNECTOR_SECRET_KEYSPACE.to_string(),
                        source_connector_secret_key(secret.connector_id),
                        secret_bytes.into(),
                    ),
                ],
                txn_id: self.txn_id,
            })]
        } else {
            let connector_bytes = match replacement.to_bytes() {
                Ok(bytes) => bytes,
                Err(error) => return self.fail_or_abort(error.into()),
            };
            smallvec![Effect::Storage(StorageEffect::Write {
                key_space: aruna_core::keyspaces::SOURCE_CONNECTOR_INDEX_KEYSPACE.to_string(),
                key: source_connector_key(replacement.group_id, replacement.connector_id),
                value: connector_bytes.into(),
                txn_id: self.txn_id,
            })]
        }
    }

    fn handle_records_written(&mut self, event: Event) -> Effects {
        match (&self.replacement_secret, event) {
            (Some(_), Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                self.commit_or_finish()
            }
            (None, Event::Storage(StorageEvent::WriteResult { .. })) => {
                self.state = ReplaceSourceConnectorState::DeleteSecret;
                smallvec![delete_connector_secret_effect(
                    self.input.connector_id,
                    self.txn_id,
                )]
            }
            (_, Event::Storage(StorageEvent::Error { error })) if self.txn_id.is_some() => {
                self.abort_with_error(error.into())
            }
            (_, Event::Storage(StorageEvent::Error { error })) => self.emit_error(error.into()),
            (_, received) => self.fail_or_abort(ReplaceSourceConnectorError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::BatchWriteResult | WriteResult)",
                received,
            }),
        }
    }

    fn handle_secret_deleted(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::DeleteResult { .. }) => {}
            Event::Storage(StorageEvent::Error { error }) if self.txn_id.is_some() => {
                return self.abort_with_error(error.into());
            }
            Event::Storage(StorageEvent::Error { error }) => return self.emit_error(error.into()),
            received => {
                return self.fail_or_abort(ReplaceSourceConnectorError::InvalidStateEvent {
                    state: self.state.clone(),
                    expected: "Event::Storage(StorageEvent::DeleteResult)",
                    received,
                });
            }
        }

        self.commit_or_finish()
    }

    fn commit_or_finish(&mut self) -> Effects {
        if let Some(txn_id) = self.txn_id {
            self.state = ReplaceSourceConnectorState::CommitTransaction;
            return smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })];
        }

        self.finish()
    }

    fn handle_transaction_committed(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                self.txn_id = None;
                self.finish()
            }
            Event::Storage(StorageEvent::Error { error }) => {
                self.txn_id = None;
                self.emit_error(error.into())
            }
            received => self.fail_or_abort(ReplaceSourceConnectorError::InvalidStateEvent {
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
                self.state = ReplaceSourceConnectorState::Error;
                smallvec![]
            }
            received => self.emit_error(ReplaceSourceConnectorError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionAborted)",
                received,
            }),
        }
    }

    fn finish(&mut self) -> Effects {
        let Some(connector) = self.replacement.clone() else {
            return self.emit_error(ReplaceSourceConnectorError::ReplaceSourceConnectorFailed);
        };

        self.state = ReplaceSourceConnectorState::Finish;
        self.output = Some(Ok(ReplaceSourceConnectorResult {
            connector,
            has_secret_config: self.replacement_secret.is_some(),
        }));
        smallvec![]
    }
}

fn secret_config_changed(
    current: Option<&SourceConnectorSecret>,
    replacement: Option<&SourceConnectorSecret>,
) -> bool {
    current.map(|secret| &secret.secret_config) != replacement.map(|secret| &secret.secret_config)
}

impl Operation for ReplaceSourceConnectorOperation {
    type Output = ReplaceSourceConnectorResult;
    type Error = ReplaceSourceConnectorError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            ReplaceSourceConnectorState::Init => self.handle_init(),
            ReplaceSourceConnectorState::ReadCurrent => self.handle_current_read(event),
            ReplaceSourceConnectorState::ReadSecret => self.handle_current_secret_read(event),
            ReplaceSourceConnectorState::StartTransaction => self.handle_transaction_started(event),
            ReplaceSourceConnectorState::ScanReferenceVersions => {
                self.handle_reference_versions_scanned(event)
            }
            ReplaceSourceConnectorState::WriteRecords => self.handle_records_written(event),
            ReplaceSourceConnectorState::DeleteSecret => self.handle_secret_deleted(event),
            ReplaceSourceConnectorState::CommitTransaction => {
                self.handle_transaction_committed(event)
            }
            ReplaceSourceConnectorState::AbortTransaction => self.handle_transaction_aborted(event),
            ReplaceSourceConnectorState::Finish => smallvec![],
            ReplaceSourceConnectorState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            ReplaceSourceConnectorState::Finish | ReplaceSourceConnectorState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == ReplaceSourceConnectorState::Error {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(ReplaceSourceConnectorError::ReplaceSourceConnectorFailed);
        }

        self.output
            .ok_or(ReplaceSourceConnectorError::ReplaceSourceConnectorFailed)?
    }

    fn abort(&mut self) -> Effects {
        if let Some(txn_id) = self.txn_id.take() {
            self.state = ReplaceSourceConnectorState::AbortTransaction;
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
    use crate::connectors::resolver::{
        ResolveVersionSourceBindingInput, ResolveVersionSourceBindingOperation,
    };
    use crate::driver::{DriverContext, drive};
    use crate::staging::descriptor::build_version_source_binding;
    use aruna_core::keyspaces::BLOB_VERSIONS_KEYSPACE;
    use aruna_core::structs::{
        BlobVersion, ResolvedSourceAccess, SourceMetadata, StagingStrategy, VersionKey,
        VersionSourceBinding,
    };
    use aruna_storage::storage;
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

    fn connector_secret(connector_id: Ulid, user: &str) -> SourceConnectorSecret {
        SourceConnectorSecret::new(
            connector_id,
            HashMap::from([
                ("user".to_string(), user.to_string()),
                ("password".to_string(), "secret".to_string()),
            ]),
            SystemTime::UNIX_EPOCH,
        )
        .unwrap()
    }

    fn replacement_connector(group_id: Ulid, connector_id: Ulid) -> SourceConnector {
        SourceConnector::new(
            connector_id,
            group_id,
            "new".to_string(),
            SourceConnectorKind::Ftp,
            HashMap::from([("endpoint".to_string(), "ftp://ftp.example.org".to_string())]),
            SystemTime::UNIX_EPOCH,
            SystemTime::UNIX_EPOCH,
            Default::default(),
        )
    }

    fn reference_blob_version(connector_id: Ulid) -> BlobVersion {
        let connector = replacement_connector(Ulid::new(), connector_id);
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
    fn replace_secret_change_scans_writes_and_commits_in_same_transaction() {
        let group_id = Ulid::from_bytes([1u8; 16]);
        let connector_id = Ulid::from_bytes([2u8; 16]);
        let txn_id = Ulid::from_bytes([3u8; 16]);
        let mut operation = ReplaceSourceConnectorOperation::new(ReplaceSourceConnectorInput {
            group_id,
            connector_id,
            name: "new".to_string(),
            kind: SourceConnectorKind::Ftp,
            public_config: HashMap::new(),
            secret_config: HashMap::new(),
        });
        operation.state = ReplaceSourceConnectorState::ReadSecret;
        operation.replacement = Some(replacement_connector(group_id, connector_id));
        operation.replacement_secret = Some(connector_secret(connector_id, "bob"));

        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![].into(),
            value: Some(
                connector_secret(connector_id, "alice")
                    .to_bytes()
                    .unwrap()
                    .into(),
            ),
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
            [Effect::Storage(StorageEffect::BatchWrite { txn_id: Some(write_txn), .. })]
                if *write_txn == txn_id
        ));

        let effects = operation.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: vec![],
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::CommitTransaction { txn_id: commit_txn })]
                if *commit_txn == txn_id
        ));
    }

    #[test]
    fn replace_secret_change_aborts_transaction_when_reference_is_found() {
        let connector_id = Ulid::from_bytes([2u8; 16]);
        let txn_id = Ulid::from_bytes([3u8; 16]);
        let mut operation = ReplaceSourceConnectorOperation::new(ReplaceSourceConnectorInput {
            group_id: Ulid::from_bytes([1u8; 16]),
            connector_id,
            name: "new".to_string(),
            kind: SourceConnectorKind::Ftp,
            public_config: HashMap::new(),
            secret_config: HashMap::new(),
        });
        operation.state = ReplaceSourceConnectorState::ScanReferenceVersions;
        operation.txn_id = Some(txn_id);
        operation.replacement = Some(replacement_connector(Ulid::new(), connector_id));
        operation.replacement_secret = Some(connector_secret(connector_id, "bob"));

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
            Err(ReplaceSourceConnectorError::ReferencedByObjectVersion)
        );
    }

    #[tokio::test]
    async fn replace_source_connector_can_remove_secret_config() {
        let tempdir = tempdir().unwrap();
        let storage_handle = storage::FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        };

        let created = drive(
            CreateSourceConnectorOperation::new(CreateSourceConnectorInput {
                group_id: ulid::Ulid::new(),
                created_by: Default::default(),
                name: "old".to_string(),
                kind: SourceConnectorKind::Http,
                public_config: HashMap::from([(
                    "endpoint".to_string(),
                    "https://example.org".to_string(),
                )]),
                secret_config: HashMap::from([("token".to_string(), "secret".to_string())]),
            }),
            &context,
        )
        .await
        .unwrap();

        let replaced = drive(
            ReplaceSourceConnectorOperation::new(ReplaceSourceConnectorInput {
                group_id: created.connector.group_id,
                connector_id: created.connector.connector_id,
                name: "new".to_string(),
                kind: SourceConnectorKind::Http,
                public_config: HashMap::from([(
                    "endpoint".to_string(),
                    "https://example.org/v2".to_string(),
                )]),
                secret_config: HashMap::new(),
            }),
            &context,
        )
        .await
        .unwrap();

        assert_eq!(replaced.connector.name, "new");
        assert!(!replaced.has_secret_config);
    }

    #[tokio::test]
    async fn replace_source_connector_rejects_secret_change_when_reference_versions_exist() {
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
        write_reference_version(&context, source).await;

        let result = drive(
            ReplaceSourceConnectorOperation::new(ReplaceSourceConnectorInput {
                group_id: connector.group_id,
                connector_id: connector.connector_id,
                name: "new".to_string(),
                kind: SourceConnectorKind::Ftp,
                public_config: connector.public_config.clone(),
                secret_config: HashMap::from([
                    ("user".to_string(), "bob".to_string()),
                    ("password".to_string(), "secret".to_string()),
                ]),
            }),
            &context,
        )
        .await;

        assert_eq!(
            result,
            Err(ReplaceSourceConnectorError::ReferencedByObjectVersion)
        );
    }

    #[tokio::test]
    async fn replace_source_connector_allows_public_change_when_references_keep_same_secret() {
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

        let replaced = drive(
            ReplaceSourceConnectorOperation::new(ReplaceSourceConnectorInput {
                group_id: connector.group_id,
                connector_id: connector.connector_id,
                name: "new".to_string(),
                kind: SourceConnectorKind::Ftp,
                public_config: HashMap::from([
                    (
                        "endpoint".to_string(),
                        "ftp://ftp.example.org/v2".to_string(),
                    ),
                    ("root".to_string(), "/datasets-v2".to_string()),
                ]),
                secret_config: HashMap::from([
                    ("user".to_string(), "alice".to_string()),
                    ("password".to_string(), "secret".to_string()),
                ]),
            }),
            &context,
        )
        .await
        .unwrap();

        assert_eq!(replaced.connector.name, "new");

        let access = drive(
            ResolveVersionSourceBindingOperation::new(ResolveVersionSourceBindingInput { source }),
            &context,
        )
        .await
        .unwrap();
        let ResolvedSourceAccess::OpenDal { config, .. } = access;
        assert_eq!(
            config.get("endpoint").map(String::as_str),
            Some("ftp://ftp.example.org:21")
        );
        assert_eq!(config.get("root").map(String::as_str), Some("/datasets"));
        assert_eq!(config.get("user").map(String::as_str), Some("alice"));
    }
}
