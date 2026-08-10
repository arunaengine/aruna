use std::time::SystemTime;

use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::{HarvestSelector, HarvestSource, RepositoryConnector};
use aruna_core::types::{Effects, GroupId, UserId};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::harvest::oai::request::DEFAULT_METADATA_PREFIX;
use crate::harvest::repository::{StorageReadError, read_connector_effect, write_source_effect};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateSourceInput {
    pub group_id: GroupId,
    pub created_by: UserId,
    pub connector_id: Ulid,
    pub namespace: String,
    pub target_prefix: String,
    pub selector: HarvestSelector,
    pub schedule_interval_ms: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum State {
    Init,
    ReadConnector,
    WriteSource,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum CreateSourceError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("harvest namespace must not be empty")]
    EmptyNamespace,
    #[error("harvest target prefix must not be empty")]
    EmptyTargetPrefix,
    #[error("unsupported harvest metadata prefix [{0}]: only [oai_dc] is mapped")]
    UnsupportedMetadataPrefix(String),
    #[error("repository connector not found")]
    ConnectorNotFound,
    #[error("CreateHarvestSource failed")]
    Failed,
    #[error("state [{state:?}] invalid: expected [{expected}] - received [{received:?}]")]
    InvalidStateEvent {
        state: String,
        expected: &'static str,
        received: Event,
    },
}

impl From<StorageReadError> for CreateSourceError {
    fn from(error: StorageReadError) -> Self {
        match error {
            StorageReadError::Storage(error) => Self::Storage(error),
            StorageReadError::Conversion(error) => Self::Conversion(error),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct CreateSourceOperation {
    input: CreateSourceInput,
    state: State,
    source: Option<HarvestSource>,
    output: Option<Result<HarvestSource, CreateSourceError>>,
}

impl CreateSourceOperation {
    pub fn new(input: CreateSourceInput) -> Self {
        Self {
            input,
            state: State::Init,
            source: None,
            output: None,
        }
    }

    fn emit_error(&mut self, error: CreateSourceError) -> Effects {
        self.state = State::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn handle_init(&mut self) -> Effects {
        if self.input.namespace.trim().is_empty() {
            return self.emit_error(CreateSourceError::EmptyNamespace);
        }
        if self.input.target_prefix.trim().is_empty() {
            return self.emit_error(CreateSourceError::EmptyTargetPrefix);
        }
        // The parser and the RO-Crate mapper are `oai_dc`-typed; any other
        // prefix would silently import identifier-only records.
        if let Some(prefix) = self.input.selector.metadata_prefix.as_deref()
            && !prefix.trim().is_empty()
            && prefix.trim() != DEFAULT_METADATA_PREFIX
        {
            return self.emit_error(CreateSourceError::UnsupportedMetadataPrefix(
                prefix.to_string(),
            ));
        }
        self.state = State::ReadConnector;
        smallvec![read_connector_effect(
            self.input.group_id,
            self.input.connector_id,
            None,
        )]
    }

    fn handle_connector_read(&mut self, event: Event) -> Effects {
        let connector = match event {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => match value {
                Some(bytes) => match RepositoryConnector::from_bytes(bytes.as_ref()) {
                    Ok(connector) => Some(connector),
                    Err(error) => return self.emit_error(error.into()),
                },
                None => None,
            },
            Event::Storage(StorageEvent::Error { error }) => {
                return self.emit_error(CreateSourceError::Storage(error));
            }
            other => {
                return self.emit_error(CreateSourceError::InvalidStateEvent {
                    state: format!("{:?}", self.state),
                    expected: "Event::Storage(StorageEvent::ReadResult)",
                    received: other,
                });
            }
        };
        if connector.is_none() {
            return self.emit_error(CreateSourceError::ConnectorNotFound);
        }

        let source = HarvestSource::new(
            ulid::Ulid::generate(),
            self.input.group_id,
            self.input.connector_id,
            self.input.namespace.clone(),
            self.input.target_prefix.clone(),
            self.input.selector.clone(),
            self.input.schedule_interval_ms,
            SystemTime::now(),
            self.input.created_by,
        );
        let write = match write_source_effect(&source, None) {
            Ok(write) => write,
            Err(error) => return self.emit_error(error.into()),
        };
        self.source = Some(source);
        self.state = State::WriteSource;
        smallvec![write]
    }

    fn handle_written(&mut self, event: Event) -> Effects {
        let Event::Storage(aruna_core::events::StorageEvent::WriteResult { .. }) = event else {
            return self.emit_error(CreateSourceError::InvalidStateEvent {
                state: format!("{:?}", self.state),
                expected: "Event::Storage(StorageEvent::WriteResult)",
                received: event,
            });
        };
        let Some(source) = self.source.clone() else {
            return self.emit_error(CreateSourceError::Failed);
        };
        self.state = State::Finish;
        self.output = Some(Ok(source));
        smallvec![]
    }
}

impl Operation for CreateSourceOperation {
    type Output = HarvestSource;
    type Error = CreateSourceError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            State::Init => self.handle_init(),
            State::ReadConnector => self.handle_connector_read(event),
            State::WriteSource => self.handle_written(event),
            State::Finish => smallvec![],
            State::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, State::Finish | State::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == State::Error {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(CreateSourceError::Failed);
        }
        self.output.ok_or(CreateSourceError::Failed)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use crate::harvest::create_repository_connector::{
        CreateConnectorInput, CreateConnectorOperation,
    };
    use crate::harvest::repository::{parse_source_read, read_source_effect};
    use aruna_core::events::StorageEvent;
    use aruna_core::handle::Handle;
    use aruna_core::structs::RepositoryConnectorKind;
    use aruna_storage::storage;
    use std::collections::HashMap;
    use tempfile::tempdir;

    fn input(group: GroupId, connector: Ulid) -> CreateSourceInput {
        CreateSourceInput {
            group_id: group,
            created_by: Default::default(),
            connector_id: connector,
            namespace: "zenodo".to_string(),
            target_prefix: "imported/zenodo".to_string(),
            selector: HarvestSelector::default(),
            schedule_interval_ms: Some(3_600_000),
        }
    }

    #[test]
    fn empty_namespace_errors_before_read() {
        let mut op = CreateSourceOperation::new(CreateSourceInput {
            namespace: " ".to_string(),
            ..input(ulid::Ulid::generate(), ulid::Ulid::generate())
        });
        assert!(op.start().is_empty());
        assert_eq!(
            op.finalize().unwrap_err(),
            CreateSourceError::EmptyNamespace
        );
    }

    #[test]
    fn foreign_metadata_prefix_is_rejected() {
        for prefix in ["datacite", "marc21", "oai_datacite"] {
            let mut op = CreateSourceOperation::new(CreateSourceInput {
                selector: HarvestSelector {
                    set: None,
                    metadata_prefix: Some(prefix.to_string()),
                },
                ..input(ulid::Ulid::generate(), ulid::Ulid::generate())
            });
            assert!(op.start().is_empty());
            assert_eq!(
                op.finalize().unwrap_err(),
                CreateSourceError::UnsupportedMetadataPrefix(prefix.to_string())
            );
        }
    }

    #[test]
    fn omitted_and_explicit_oai_dc_agree() {
        for prefix in [None, Some("oai_dc".to_string()), Some(String::new())] {
            let mut op = CreateSourceOperation::new(CreateSourceInput {
                selector: HarvestSelector {
                    set: None,
                    metadata_prefix: prefix,
                },
                ..input(ulid::Ulid::generate(), ulid::Ulid::generate())
            });
            assert_eq!(op.start().len(), 1);
        }
    }

    #[test]
    fn missing_connector_errors_not_found() {
        let mut op =
            CreateSourceOperation::new(input(ulid::Ulid::generate(), ulid::Ulid::generate()));
        assert_eq!(op.start().len(), 1);
        op.step(Event::Storage(StorageEvent::ReadResult {
            key: byteview::ByteView::from(Vec::new()),
            value: None,
        }));
        assert_eq!(
            op.finalize().unwrap_err(),
            CreateSourceError::ConnectorNotFound
        );
    }

    #[tokio::test]
    async fn persists_source_after_connector_exists() {
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

        let connector = drive(
            CreateConnectorOperation::new(CreateConnectorInput {
                group_id: ulid::Ulid::generate(),
                created_by: Default::default(),
                name: "zenodo".to_string(),
                kind: RepositoryConnectorKind::OaiPmh,
                endpoint: "https://zenodo.org/oai2d".to_string(),
                public_config: HashMap::new(),
                secret_config: HashMap::new(),
            }),
            &context,
        )
        .await
        .unwrap()
        .connector;

        let source = drive(
            CreateSourceOperation::new(input(connector.group_id, connector.connector_id)),
            &context,
        )
        .await
        .unwrap();

        let stored = context
            .storage_handle
            .send_effect(read_source_effect(source.group_id, source.source_id, None))
            .await;
        assert_eq!(parse_source_read(stored).unwrap().unwrap(), source);
    }
}
