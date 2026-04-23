use std::collections::HashMap;
use std::path::{Component, Path};

use aruna_core::effects::Effect;
use aruna_core::errors::SourceConnectorResolutionError;
use aruna_core::events::{Event, SubOperationEvent};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::{
    ResolvedSourceAccess, ResolvedSourceConnector, SourceConnector, SourceConnectorKind,
};
use aruna_core::types::{Effects, GroupId};
use smallvec::smallvec;
use ulid::Ulid;

use crate::connectors::repository::{
    StorageReadError, parse_connector_read, parse_connector_secret_read, read_connector_effect,
    read_connector_secret_effect,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolveSourceConnectorInput {
    pub group_id: GroupId,
    pub connector_id: Ulid,
    pub source_path: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ResolveSourceConnectorState {
    Init,
    ReadConnector,
    ReadSecret,
    Finish,
    Error,
}

impl From<StorageReadError> for SourceConnectorResolutionError {
    fn from(value: StorageReadError) -> Self {
        match value {
            StorageReadError::Storage(error) => Self::StorageError(error),
            StorageReadError::Conversion(error) => Self::ConversionError(error),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct ResolveSourceConnectorOperation {
    input: ResolveSourceConnectorInput,
    state: ResolveSourceConnectorState,
    connector: Option<SourceConnector>,
    output: Option<Result<ResolvedSourceConnector, SourceConnectorResolutionError>>,
}

impl ResolveSourceConnectorOperation {
    pub fn new(input: ResolveSourceConnectorInput) -> Self {
        Self {
            input,
            state: ResolveSourceConnectorState::Init,
            connector: None,
            output: None,
        }
    }

    fn emit_error(&mut self, error: SourceConnectorResolutionError) -> Effects {
        self.state = ResolveSourceConnectorState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn handle_init(&mut self) -> Effects {
        if !is_valid_relative_source_path(&self.input.source_path) {
            return self.emit_error(SourceConnectorResolutionError::InvalidSourcePath);
        }

        self.state = ResolveSourceConnectorState::ReadConnector;
        smallvec![read_connector_effect(
            self.input.group_id,
            self.input.connector_id,
            None,
        )]
    }

    fn handle_connector_read(&mut self, event: Event) -> Effects {
        match parse_connector_read(event) {
            Ok(Some(connector)) => {
                self.connector = Some(connector);
                self.state = ResolveSourceConnectorState::ReadSecret;
                smallvec![read_connector_secret_effect(self.input.connector_id, None)]
            }
            Ok(None) => self.emit_error(SourceConnectorResolutionError::NotFound),
            Err(error) => self.emit_error(error.into()),
        }
    }

    fn handle_secret_read(&mut self, event: Event) -> Effects {
        let secret = match parse_connector_secret_read(event) {
            Ok(secret) => secret,
            Err(error) => return self.emit_error(error.into()),
        };

        let Some(connector) = self.connector.clone() else {
            return self.emit_error(SourceConnectorResolutionError::ResolveFailed);
        };

        let access = match resolve_access(
            &connector,
            secret.map(|secret| secret.secret_config),
            &self.input.source_path,
        ) {
            Ok(access) => access,
            Err(error) => return self.emit_error(error),
        };

        self.state = ResolveSourceConnectorState::Finish;
        self.output = Some(Ok(ResolvedSourceConnector { connector, access }));
        smallvec![]
    }
}

impl Operation for ResolveSourceConnectorOperation {
    type Output = ResolvedSourceConnector;
    type Error = SourceConnectorResolutionError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            ResolveSourceConnectorState::Init => self.handle_init(),
            ResolveSourceConnectorState::ReadConnector => self.handle_connector_read(event),
            ResolveSourceConnectorState::ReadSecret => self.handle_secret_read(event),
            ResolveSourceConnectorState::Finish => smallvec![],
            ResolveSourceConnectorState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            ResolveSourceConnectorState::Finish | ResolveSourceConnectorState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == ResolveSourceConnectorState::Error {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(SourceConnectorResolutionError::ResolveFailed);
        }

        self.output
            .ok_or(SourceConnectorResolutionError::ResolveFailed)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

pub fn resolve_source_connector_suboperation(input: ResolveSourceConnectorInput) -> Effect {
    Effect::SubOperation(boxed_suboperation(
        ResolveSourceConnectorOperation::new(input),
        |result| Event::SubOperation(SubOperationEvent::SourceConnectorResolved { result }),
    ))
}

fn resolve_access(
    connector: &SourceConnector,
    secret_config: Option<HashMap<String, String>>,
    source_path: &str,
) -> Result<ResolvedSourceAccess, SourceConnectorResolutionError> {
    if connector.kind == SourceConnectorKind::ArunaNative {
        return Err(SourceConnectorResolutionError::UnsupportedConnectorKind(
            SourceConnectorKind::ArunaNative,
        ));
    }

    let mut config = connector.public_config.clone();
    if let Some(secret_config) = secret_config {
        config.extend(secret_config);
    }

    Ok(ResolvedSourceAccess::OpenDal {
        kind: connector.kind,
        config,
        path: source_path.to_string(),
    })
}

fn is_valid_relative_source_path(path: &str) -> bool {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return false;
    }

    let mut has_normal_component = false;
    for component in Path::new(trimmed).components() {
        match component {
            Component::Normal(_) => has_normal_component = true,
            Component::CurDir
            | Component::ParentDir
            | Component::RootDir
            | Component::Prefix(_) => return false,
        }
    }

    has_normal_component
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectors::create_source_connector::{
        CreateSourceConnectorInput, CreateSourceConnectorOperation,
    };
    use crate::driver::{DriverContext, drive};
    use aruna_storage::storage;
    use std::collections::HashMap;
    use tempfile::tempdir;

    #[tokio::test]
    async fn resolve_source_connector_merges_public_and_secret_config() {
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
        let group_id = Ulid::new();

        let created = drive(
            CreateSourceConnectorOperation::new(CreateSourceConnectorInput {
                group_id,
                created_by: Ulid::new(),
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
            &context,
        )
        .await
        .unwrap();

        let resolved = drive(
            ResolveSourceConnectorOperation::new(ResolveSourceConnectorInput {
                group_id,
                connector_id: created.connector.connector_id,
                source_path: "run-1/data.txt".to_string(),
            }),
            &context,
        )
        .await
        .unwrap();

        let ResolvedSourceAccess::OpenDal { kind, config, path } = resolved.access;
        assert_eq!(kind, SourceConnectorKind::Ftp);
        assert_eq!(path, "run-1/data.txt");
        assert_eq!(config.get("user").map(String::as_str), Some("alice"));
        assert_eq!(config.get("root").map(String::as_str), Some("/datasets"));
    }

    #[test]
    fn reject_absolute_source_paths() {
        assert!(!is_valid_relative_source_path("/absolute/file.txt"));
        assert!(is_valid_relative_source_path("nested/file.txt"));
    }

    #[test]
    fn reject_non_normal_relative_source_paths() {
        assert!(!is_valid_relative_source_path(""));
        assert!(!is_valid_relative_source_path("   "));
        assert!(!is_valid_relative_source_path("./file.txt"));
        assert!(!is_valid_relative_source_path("nested/../file.txt"));
    }

    #[test]
    fn reject_aruna_native_in_phase_three() {
        let connector = SourceConnector::new(
            Ulid::new(),
            Ulid::new(),
            "native".to_string(),
            SourceConnectorKind::ArunaNative,
            HashMap::from([(
                "endpoint".to_string(),
                "https://native.example.org".to_string(),
            )]),
            std::time::SystemTime::UNIX_EPOCH,
            std::time::SystemTime::UNIX_EPOCH,
            Ulid::new(),
        );

        let error = resolve_access(&connector, None, "bucket/key").unwrap_err();
        assert_eq!(
            error,
            SourceConnectorResolutionError::UnsupportedConnectorKind(
                SourceConnectorKind::ArunaNative
            )
        );
    }
}
