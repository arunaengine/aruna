use std::time::SystemTime;

use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::{HarvestSelector, HarvestSource, RepositoryConnector};
use aruna_core::types::{Effects, GroupId, UserId};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::harvest::oai::request::{normalize_metadata_prefix, normalize_set};
use crate::harvest::repository::{StorageReadError, read_connector_effect, write_source_effect};
use crate::harvest::target_path::{normalize_target_prefix, target_prefix_is_blank};

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
    #[error("harvest target prefix [{0}] leaves no room for a record identifier")]
    TargetPrefixTooLong(String),
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

    /// Validate and canonicalize the input, so only values the request builder
    /// and the path encoder can use are ever stored.
    fn handle_init(&mut self) -> Effects {
        if self.input.namespace.trim().is_empty() {
            return self.emit_error(CreateSourceError::EmptyNamespace);
        }
        let Some(target_prefix) = normalize_target_prefix(&self.input.target_prefix) else {
            return self.emit_error(if target_prefix_is_blank(&self.input.target_prefix) {
                CreateSourceError::EmptyTargetPrefix
            } else {
                CreateSourceError::TargetPrefixTooLong(self.input.target_prefix.clone())
            });
        };
        // The parser and the RO-Crate mapper are `oai_dc`-typed; any other
        // prefix would silently import identifier-only records.
        let metadata_prefix =
            match normalize_metadata_prefix(self.input.selector.metadata_prefix.as_deref()) {
                Ok(prefix) => prefix,
                Err(prefix) => {
                    return self.emit_error(CreateSourceError::UnsupportedMetadataPrefix(prefix));
                }
            };
        self.input.target_prefix = target_prefix;
        self.input.selector.metadata_prefix = Some(metadata_prefix);
        self.input.selector.set = normalize_set(self.input.selector.set.as_deref());
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
    use crate::harvest::target_path::{DIGEST_SEGMENT_BYTES, HARVEST_PATH_BYTES};
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

    /// Omitted, empty, whitespace, padded and exact prefixes are one source
    /// configuration, stored in exactly one form.
    #[test]
    fn omitted_and_explicit_oai_dc_agree() {
        for prefix in [
            None,
            Some("oai_dc".to_string()),
            Some(String::new()),
            Some("   ".to_string()),
            Some(" oai_dc ".to_string()),
        ] {
            let mut op = CreateSourceOperation::new(CreateSourceInput {
                selector: HarvestSelector {
                    set: Some("  ".to_string()),
                    metadata_prefix: prefix,
                },
                ..input(ulid::Ulid::generate(), ulid::Ulid::generate())
            });
            assert_eq!(op.start().len(), 1);
            assert_eq!(op.input.selector.metadata_prefix.as_deref(), Some("oai_dc"));
            assert_eq!(op.input.selector.set, None);
        }
    }

    #[test]
    fn target_prefix_is_stored_canonical() {
        let mut op = CreateSourceOperation::new(CreateSourceInput {
            target_prefix: "  /imported/zenodo/  ".to_string(),
            ..input(ulid::Ulid::generate(), ulid::Ulid::generate())
        });
        assert_eq!(op.start().len(), 1);
        assert_eq!(op.input.target_prefix, "imported/zenodo");
    }

    /// A prefix one byte past the path budget makes every record fail later, so
    /// it is refused at creation; the longest usable prefix is still accepted.
    #[test]
    fn target_prefix_budget_is_enforced() {
        let longest = HARVEST_PATH_BYTES - DIGEST_SEGMENT_BYTES - 1;
        let mut accepted = CreateSourceOperation::new(CreateSourceInput {
            target_prefix: "p".repeat(longest),
            ..input(ulid::Ulid::generate(), ulid::Ulid::generate())
        });
        assert_eq!(accepted.start().len(), 1);

        let too_long = "p".repeat(longest + 1);
        let mut refused = CreateSourceOperation::new(CreateSourceInput {
            target_prefix: too_long.clone(),
            ..input(ulid::Ulid::generate(), ulid::Ulid::generate())
        });
        assert!(refused.start().is_empty());
        assert_eq!(
            refused.finalize().unwrap_err(),
            CreateSourceError::TargetPrefixTooLong(too_long)
        );
    }

    #[test]
    fn blank_target_prefix_is_rejected() {
        for prefix in ["", " ", "/", "  //  "] {
            let mut op = CreateSourceOperation::new(CreateSourceInput {
                target_prefix: prefix.to_string(),
                ..input(ulid::Ulid::generate(), ulid::Ulid::generate())
            });
            assert!(op.start().is_empty());
            assert_eq!(
                op.finalize().unwrap_err(),
                CreateSourceError::EmptyTargetPrefix
            );
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
            CreateSourceOperation::new(CreateSourceInput {
                target_prefix: " /imported/zenodo/ ".to_string(),
                selector: HarvestSelector {
                    set: Some(" ".to_string()),
                    metadata_prefix: Some(" oai_dc ".to_string()),
                },
                ..input(connector.group_id, connector.connector_id)
            }),
            &context,
        )
        .await
        .unwrap();

        let stored = context
            .storage_handle
            .send_effect(read_source_effect(source.group_id, source.source_id, None))
            .await;
        let stored = parse_source_read(stored).unwrap().unwrap();
        assert_eq!(stored, source);
        assert_eq!(stored.target_prefix, "imported/zenodo");
        assert_eq!(stored.selector.metadata_prefix.as_deref(), Some("oai_dc"));
        assert_eq!(stored.selector.set, None);
    }
}
