use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::types::{Effects, GroupId};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::connectors::repository::{
    StorageReadError, iter_connector_reference_versions_effect, parse_connector_read,
    parse_connector_secret_read, parse_version_metadata_iter, read_connector_effect,
    read_connector_secret_effect, source_connector_key, source_connector_secret_key,
    version_metadata_references_connector,
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
    ScanReferenceVersions,
    DeleteRecords,
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
    output: Option<Result<DeleteSourceConnectorResult, DeleteSourceConnectorError>>,
}

impl DeleteSourceConnectorOperation {
    pub fn new(input: DeleteSourceConnectorInput) -> Self {
        Self {
            input,
            state: DeleteSourceConnectorState::Init,
            output: None,
        }
    }

    fn emit_error(&mut self, error: DeleteSourceConnectorError) -> Effects {
        self.state = DeleteSourceConnectorState::Error;
        self.output = Some(Err(error));
        smallvec![]
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

        self.state = DeleteSourceConnectorState::ScanReferenceVersions;
        smallvec![iter_connector_reference_versions_effect(None, None)]
    }

    fn handle_reference_versions_scanned(&mut self, event: Event) -> Effects {
        let (versions, next_start_after) = match parse_version_metadata_iter(event) {
            Ok(result) => result,
            Err(error) => return self.emit_error(error.into()),
        };

        if versions.iter().any(|metadata| {
            version_metadata_references_connector(metadata, self.input.connector_id)
        }) {
            return self.emit_error(DeleteSourceConnectorError::ReferencedByObjectVersion);
        }

        if let Some(start_after) = next_start_after {
            return smallvec![iter_connector_reference_versions_effect(
                Some(start_after),
                None,
            )];
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
            txn_id: None,
        })]
    }

    fn handle_records_deleted(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchDeleteResult { .. }) = event else {
            return self.emit_error(DeleteSourceConnectorError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::BatchDeleteResult)",
                received: event,
            });
        };

        self.state = DeleteSourceConnectorState::Finish;
        self.output = Some(Ok(DeleteSourceConnectorResult));
        smallvec![]
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
            DeleteSourceConnectorState::ScanReferenceVersions => {
                self.handle_reference_versions_scanned(event)
            }
            DeleteSourceConnectorState::DeleteRecords => self.handle_records_deleted(event),
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
    use aruna_core::keyspaces::S3_VERSION_KEYSPACE;
    use aruna_core::structs::{
        ResolvedSourceAccess, SourceConnector, SourceConnectorKind, SourceMetadata,
        StagingStrategy, VersionKey, VersionMetadata, VersionSourceBinding,
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
        let value = VersionMetadata::reference(
            version_id,
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
                key_space: S3_VERSION_KEYSPACE.to_string(),
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
