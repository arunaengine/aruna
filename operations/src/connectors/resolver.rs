use std::collections::HashMap;
use std::path::{Component, Path};

use aruna_core::effects::Effect;
use aruna_core::errors::SourceConnectorResolutionError;
use aruna_core::events::{Event, SubOperationEvent};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::{
    ResolvedSourceAccess, ResolvedSourceConnector, SourceConnector, SourceConnectorKind,
    VersionSourceBinding,
};
use aruna_core::types::{Effects, GroupId, TxnId};
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolveVersionSourceBindingInput {
    pub source: VersionSourceBinding,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ResolveSourceConnectorState {
    Init,
    ReadConnector,
    ReadSecret,
    Finish,
    Error,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ResolveVersionSourceBindingState {
    Init,
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

#[derive(Debug, PartialEq)]
pub struct ResolveVersionSourceBindingOperation {
    input: ResolveVersionSourceBindingInput,
    state: ResolveVersionSourceBindingState,
    output: Option<Result<ResolvedSourceAccess, SourceConnectorResolutionError>>,
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

        let access = match build_source_access(
            connector.kind,
            &connector.public_config,
            secret.clone().map(|secret| secret.secret_config),
            &self.input.source_path,
            None,
        ) {
            Ok(access) => access,
            Err(error) => return self.emit_error(error),
        };

        self.state = ResolveSourceConnectorState::Finish;
        let secret_fingerprint = secret.as_ref().map(secret_fingerprint);

        self.output = Some(Ok(ResolvedSourceConnector {
            connector,
            secret_fingerprint,
            access,
        }));
        smallvec![]
    }
}

impl ResolveVersionSourceBindingOperation {
    pub fn new(input: ResolveVersionSourceBindingInput) -> Self {
        Self {
            input,
            state: ResolveVersionSourceBindingState::Init,
            output: None,
        }
    }

    fn emit_error(&mut self, error: SourceConnectorResolutionError) -> Effects {
        self.state = ResolveVersionSourceBindingState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn handle_init(&mut self) -> Effects {
        let effect = match read_source_binding_secret_effect(&self.input.source, None) {
            Ok(effect) => effect,
            Err(error) => return self.emit_error(error),
        };

        self.state = ResolveVersionSourceBindingState::ReadSecret;
        smallvec![effect]
    }

    fn handle_secret_read(&mut self, event: Event) -> Effects {
        let access = match resolve_source_binding_access(&self.input.source, event) {
            Ok(access) => access,
            Err(error) => return self.emit_error(error),
        };

        self.state = ResolveVersionSourceBindingState::Finish;
        self.output = Some(Ok(access));
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

impl Operation for ResolveVersionSourceBindingOperation {
    type Output = ResolvedSourceAccess;
    type Error = SourceConnectorResolutionError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            ResolveVersionSourceBindingState::Init => self.handle_init(),
            ResolveVersionSourceBindingState::ReadSecret => self.handle_secret_read(event),
            ResolveVersionSourceBindingState::Finish => smallvec![],
            ResolveVersionSourceBindingState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            ResolveVersionSourceBindingState::Finish | ResolveVersionSourceBindingState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == ResolveVersionSourceBindingState::Error {
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
        |result| {
            Event::SubOperation(SubOperationEvent::SourceConnectorResolved {
                result: Box::new(result),
            })
        },
    ))
}

pub fn resolve_version_source_binding_suboperation(
    input: ResolveVersionSourceBindingInput,
) -> Effect {
    Effect::SubOperation(boxed_suboperation(
        ResolveVersionSourceBindingOperation::new(input),
        |result| Event::SubOperation(SubOperationEvent::VersionSourceAccessResolved { result }),
    ))
}

pub(crate) fn build_source_access(
    kind: SourceConnectorKind,
    public_config: &HashMap<String, String>,
    secret_config: Option<HashMap<String, String>>,
    source_path: &str,
    version: Option<String>,
) -> Result<ResolvedSourceAccess, SourceConnectorResolutionError> {
    if !is_valid_relative_source_path(source_path) {
        return Err(SourceConnectorResolutionError::InvalidSourcePath);
    }

    if kind == SourceConnectorKind::ArunaNative {
        return Err(SourceConnectorResolutionError::UnsupportedConnectorKind(
            SourceConnectorKind::ArunaNative,
        ));
    }

    let mut config = public_config.clone();
    if let Some(secret_config) = secret_config {
        config.extend(secret_config);
    }

    Ok(ResolvedSourceAccess::OpenDal {
        kind,
        config,
        path: source_path.to_string(),
        version,
    })
}

pub(crate) fn secret_fingerprint(secret: &aruna_core::structs::SourceConnectorSecret) -> [u8; 16] {
    let mut entries = secret.secret_config.iter().collect::<Vec<_>>();
    entries.sort_unstable_by_key(|(key, _)| *key);

    let mut hasher = blake3::Hasher::new();
    for (key, value) in entries {
        hasher.update(&(key.len() as u64).to_le_bytes());
        hasher.update(key.as_bytes());
        hasher.update(&(value.len() as u64).to_le_bytes());
        hasher.update(value.as_bytes());
    }

    let hash = hasher.finalize();
    let mut fingerprint = [0u8; 16];
    fingerprint.copy_from_slice(&hash.as_bytes()[..16]);
    fingerprint
}

pub(crate) fn build_source_access_from_binding(
    source: &VersionSourceBinding,
    secret_config: Option<HashMap<String, String>>,
) -> Result<ResolvedSourceAccess, SourceConnectorResolutionError> {
    build_source_access(
        source.descriptor.kind,
        &source.descriptor.public_config,
        secret_config,
        &source.descriptor.source_path,
        source_binding_version(source)?,
    )
}

fn source_binding_version(
    source: &VersionSourceBinding,
) -> Result<Option<String>, SourceConnectorResolutionError> {
    let Some(selector) = source
        .descriptor
        .version_selector
        .as_deref()
        .map(str::trim)
        .filter(|selector| !selector.is_empty())
    else {
        return Ok(None);
    };

    if let Some(version) = selector.strip_prefix("version:").map(str::trim) {
        return (!version.is_empty())
            .then(|| Some(version.to_string()))
            .ok_or(SourceConnectorResolutionError::ResolveFailed);
    }

    if selector.contains(':') {
        return Err(SourceConnectorResolutionError::ResolveFailed);
    }

    Ok(Some(selector.to_string()))
}

pub(crate) fn read_source_binding_secret_effect(
    source: &VersionSourceBinding,
    txn_id: Option<TxnId>,
) -> Result<Effect, SourceConnectorResolutionError> {
    let Some(connector_id) = source.connector_id else {
        return Err(SourceConnectorResolutionError::ResolveFailed);
    };

    Ok(read_connector_secret_effect(connector_id, txn_id))
}

pub(crate) fn resolve_source_binding_access(
    source: &VersionSourceBinding,
    event: Event,
) -> Result<ResolvedSourceAccess, SourceConnectorResolutionError> {
    let secret =
        parse_connector_secret_read(event).map_err(SourceConnectorResolutionError::from)?;

    build_source_access_from_binding(source, secret.map(|secret| secret.secret_config))
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
    use crate::connectors::repository::delete_connector_effect;
    use crate::driver::{DriverContext, drive};
    use crate::staging::descriptor::build_version_source_binding;
    use aruna_core::events::StorageEvent;
    use aruna_core::handle::Handle;
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
            metadata_handle: None,
            task_handle: None,
        };
        let group_id = Ulid::r#gen();

        let created = drive(
            CreateSourceConnectorOperation::new(CreateSourceConnectorInput {
                group_id,
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

        let ResolvedSourceAccess::OpenDal {
            kind,
            config,
            path,
            version,
        } = resolved.access;
        assert_eq!(kind, SourceConnectorKind::Ftp);
        assert_eq!(path, "run-1/data.txt");
        assert_eq!(version, None);
        assert_eq!(config.get("user").map(String::as_str), Some("alice"));
        assert_eq!(config.get("root").map(String::as_str), Some("/datasets"));
    }

    #[test]
    fn build_source_access_from_binding_merges_descriptor_and_secret_config() {
        let source = VersionSourceBinding {
            strategy: aruna_core::structs::StagingStrategy::Reference,
            descriptor: aruna_core::structs::PortableSourceDescriptor {
                kind: SourceConnectorKind::Ftp,
                public_config: HashMap::from([
                    (
                        "endpoint".to_string(),
                        "ftp://ftp.example.org:21".to_string(),
                    ),
                    ("root".to_string(), "/datasets".to_string()),
                ]),
                source_path: "run-1/data.txt".to_string(),
                version_selector: Some("version:v42".to_string()),
                capabilities: Vec::new(),
                origin_node_id: None,
            },
            connector_id: Some(Ulid::from_bytes([9u8; 16])),
        };

        let access = build_source_access_from_binding(
            &source,
            Some(HashMap::from([
                ("user".to_string(), "alice".to_string()),
                ("password".to_string(), "secret".to_string()),
            ])),
        )
        .unwrap();

        let ResolvedSourceAccess::OpenDal {
            kind,
            config,
            path,
            version,
        } = access;
        assert_eq!(kind, SourceConnectorKind::Ftp);
        assert_eq!(path, "run-1/data.txt");
        assert_eq!(version.as_deref(), Some("v42"));
        assert_eq!(config.get("root").map(String::as_str), Some("/datasets"));
        assert_eq!(config.get("user").map(String::as_str), Some("alice"));
    }

    #[test]
    fn build_source_access_from_binding_rejects_invalid_version_selector() {
        let source = VersionSourceBinding {
            strategy: aruna_core::structs::StagingStrategy::Reference,
            descriptor: aruna_core::structs::PortableSourceDescriptor {
                kind: SourceConnectorKind::Http,
                public_config: HashMap::from([(
                    "endpoint".to_string(),
                    "https://example.org".to_string(),
                )]),
                source_path: "file.txt".to_string(),
                version_selector: Some("etag:abc".to_string()),
                capabilities: Vec::new(),
                origin_node_id: None,
            },
            connector_id: Some(Ulid::from_bytes([9u8; 16])),
        };

        assert_eq!(
            build_source_access_from_binding(&source, None),
            Err(SourceConnectorResolutionError::ResolveFailed)
        );
    }

    #[test]
    fn build_source_access_from_binding_accepts_raw_persisted_version_selector() {
        let source = VersionSourceBinding {
            strategy: aruna_core::structs::StagingStrategy::Reference,
            descriptor: aruna_core::structs::PortableSourceDescriptor {
                kind: SourceConnectorKind::Http,
                public_config: HashMap::from([(
                    "endpoint".to_string(),
                    "https://example.org".to_string(),
                )]),
                source_path: "file.txt".to_string(),
                version_selector: Some("v42".to_string()),
                capabilities: Vec::new(),
                origin_node_id: None,
            },
            connector_id: Some(Ulid::from_bytes([9u8; 16])),
        };

        let access = build_source_access_from_binding(&source, None).unwrap();
        let ResolvedSourceAccess::OpenDal { version, .. } = access;
        assert_eq!(version.as_deref(), Some("v42"));
    }

    #[test]
    fn read_source_binding_secret_effect_requires_exact_connector_id() {
        let source = VersionSourceBinding {
            strategy: aruna_core::structs::StagingStrategy::Reference,
            descriptor: aruna_core::structs::PortableSourceDescriptor {
                kind: SourceConnectorKind::Http,
                public_config: HashMap::from([(
                    "endpoint".to_string(),
                    "https://example.org".to_string(),
                )]),
                source_path: "file.txt".to_string(),
                version_selector: None,
                capabilities: Vec::new(),
                origin_node_id: None,
            },
            connector_id: None,
        };

        assert_eq!(
            read_source_binding_secret_effect(&source, None),
            Err(SourceConnectorResolutionError::ResolveFailed)
        );
    }

    #[tokio::test]
    async fn resolve_version_source_binding_uses_stored_descriptor_without_public_connector_lookup()
    {
        let tempdir = tempdir().unwrap();
        let storage_handle = storage::FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        };
        let group_id = Ulid::r#gen();

        let created = drive(
            CreateSourceConnectorOperation::new(CreateSourceConnectorInput {
                group_id,
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
            &context,
        )
        .await
        .unwrap();

        let delete_event = context
            .storage_handle
            .send_effect(delete_connector_effect(
                created.connector.group_id,
                created.connector.connector_id,
                None,
            ))
            .await;
        assert!(matches!(
            delete_event,
            Event::Storage(StorageEvent::DeleteResult { .. })
        ));

        let source = build_version_source_binding(
            aruna_core::structs::StagingStrategy::Reference,
            &created.connector,
            &aruna_core::structs::SourceMetadata {
                content_length: 42,
                content_type: Some("text/plain".to_string()),
                etag: None,
                last_modified: None,
                source_version: None,
            },
            "run-1/data.txt".to_string(),
            None,
            Some(created.connector.connector_id),
        );

        let access = drive(
            ResolveVersionSourceBindingOperation::new(ResolveVersionSourceBindingInput { source }),
            &context,
        )
        .await
        .unwrap();

        let ResolvedSourceAccess::OpenDal {
            kind,
            config,
            path,
            version,
        } = access;
        assert_eq!(kind, SourceConnectorKind::Ftp);
        assert_eq!(path, "run-1/data.txt");
        assert_eq!(version, None);
        assert_eq!(
            config.get("endpoint").map(String::as_str),
            Some("ftp://ftp.example.org:21")
        );
        assert_eq!(config.get("root").map(String::as_str), Some("/datasets"));
        assert_eq!(config.get("user").map(String::as_str), Some("alice"));
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
        let error = build_source_access(
            SourceConnectorKind::ArunaNative,
            &HashMap::from([(
                "endpoint".to_string(),
                "https://native.example.org".to_string(),
            )]),
            None,
            "bucket/key",
            None,
        )
        .unwrap_err();
        assert_eq!(
            error,
            SourceConnectorResolutionError::UnsupportedConnectorKind(
                SourceConnectorKind::ArunaNative
            )
        );
    }
}
