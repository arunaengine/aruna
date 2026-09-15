use std::collections::HashMap;
use std::path::{Component, Path};

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::SourceResolutionError;
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::OFFERED_DIRECTORY_KEYSPACE;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::execution::offered_directory::{
    OFFERED_DIRECTORY_BUCKET, OFFERED_DIRECTORY_ROOT, OfferedDirectory,
};
use aruna_core::structs::execution::source_access::{ResolvedSourceAccess, ResolvedSourceConnector};
use aruna_core::structs::execution::source_connector::{SourceConnector, SourceConnectorKind};
use aruna_core::structs::execution::staging::{StagingStrategy, VersionSourceBinding};
use aruna_core::types::{Effects, GroupId, TxnId};
use smallvec::smallvec;
use ulid::Ulid;

use crate::connectors::repository::{
    StorageReadError, parse_connector_read, parse_secret_read, read_connector_effect,
    read_secret_effect,
};

pub(crate) const ARUNA_NATIVE_RELATIONSHIP_ID: &str = "relationship_id";
pub(crate) const ARUNA_NATIVE_ORIGIN_NODE_ID: &str = "origin_node_id";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolveConnectorInput {
    pub group_id: GroupId,
    pub connector_id: Ulid,
    pub source_path: String,
    pub allow_root: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolveBindingInput {
    pub source: VersionSourceBinding,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ResolveConnectorState {
    Init,
    ReadConnector,
    ReadSecret,
    Finish,
    Error,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ResolveBindingState {
    Init,
    ReadSecret,
    ReadOfferedDirectory,
    Finish,
    Error,
}

impl From<StorageReadError> for SourceResolutionError {
    fn from(value: StorageReadError) -> Self {
        match value {
            StorageReadError::Storage(error) => Self::StorageError(error),
            StorageReadError::Conversion(error) => Self::ConversionError(error),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct ResolveConnectorOperation {
    input: ResolveConnectorInput,
    state: ResolveConnectorState,
    connector: Option<SourceConnector>,
    output: Option<Result<ResolvedSourceConnector, SourceResolutionError>>,
}

#[derive(Debug, PartialEq)]
pub struct ResolveBindingOperation {
    input: ResolveBindingInput,
    state: ResolveBindingState,
    output: Option<Result<ResolvedSourceAccess, SourceResolutionError>>,
}

impl ResolveConnectorOperation {
    pub fn new(input: ResolveConnectorInput) -> Self {
        Self {
            input,
            state: ResolveConnectorState::Init,
            connector: None,
            output: None,
        }
    }

    fn emit_error(&mut self, error: SourceResolutionError) -> Effects {
        self.state = ResolveConnectorState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn handle_init(&mut self) -> Effects {
        if let Err(error) = validate_source_path(&self.input.source_path, self.input.allow_root) {
            return self.emit_error(error);
        }

        self.state = ResolveConnectorState::ReadConnector;
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
                self.state = ResolveConnectorState::ReadSecret;
                smallvec![read_secret_effect(self.input.connector_id, None)]
            }
            Ok(None) => self.emit_error(SourceResolutionError::NotFound),
            Err(error) => self.emit_error(error.into()),
        }
    }

    fn handle_secret_read(&mut self, event: Event) -> Effects {
        let secret = match parse_secret_read(event) {
            Ok(secret) => secret,
            Err(error) => return self.emit_error(error.into()),
        };

        let Some(connector) = self.connector.clone() else {
            return self.emit_error(SourceResolutionError::ResolveFailed);
        };

        let access = match build_source_access(
            connector.kind,
            &connector.public_config,
            secret.clone().map(|secret| secret.secret_config),
            &self.input.source_path,
            None,
            self.input.allow_root,
        ) {
            Ok(access) => access,
            Err(error) => return self.emit_error(error),
        };

        self.state = ResolveConnectorState::Finish;
        let secret_fingerprint = secret.as_ref().map(secret_fingerprint);

        self.output = Some(Ok(ResolvedSourceConnector {
            connector,
            secret_fingerprint,
            access,
        }));
        smallvec![]
    }
}

impl ResolveBindingOperation {
    pub fn new(input: ResolveBindingInput) -> Self {
        Self {
            input,
            state: ResolveBindingState::Init,
            output: None,
        }
    }

    fn emit_error(&mut self, error: SourceResolutionError) -> Effects {
        self.state = ResolveBindingState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn handle_init(&mut self) -> Effects {
        if self.input.source.descriptor.kind == SourceConnectorKind::ArunaNative {
            let access = match build_binding_access(&self.input.source, None) {
                Ok(access) => access,
                Err(error) => return self.emit_error(error),
            };
            self.state = ResolveBindingState::Finish;
            self.output = Some(Ok(access));
            return smallvec![];
        }

        if self.input.source.descriptor.kind == SourceConnectorKind::LocalDirectory {
            let effect = match read_offered_effect(&self.input.source) {
                Ok(effect) => effect,
                Err(error) => return self.emit_error(error),
            };
            self.state = ResolveBindingState::ReadOfferedDirectory;
            return smallvec![effect];
        }

        let effect = match binding_secret_effect(&self.input.source, None) {
            Ok(effect) => effect,
            Err(error) => return self.emit_error(error),
        };

        self.state = ResolveBindingState::ReadSecret;
        smallvec![effect]
    }

    fn handle_secret_read(&mut self, event: Event) -> Effects {
        let access = match resolve_binding_access(&self.input.source, event) {
            Ok(access) => access,
            Err(error) => return self.emit_error(error),
        };

        self.state = ResolveBindingState::Finish;
        self.output = Some(Ok(access));
        smallvec![]
    }

    fn handle_offered_read(&mut self, event: Event) -> Effects {
        let access = match resolve_offered_access(&self.input.source, event) {
            Ok(access) => access,
            Err(error) => return self.emit_error(error),
        };

        self.state = ResolveBindingState::Finish;
        self.output = Some(Ok(access));
        smallvec![]
    }
}

impl Operation for ResolveConnectorOperation {
    type Output = ResolvedSourceConnector;
    type Error = SourceResolutionError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            ResolveConnectorState::Init => self.handle_init(),
            ResolveConnectorState::ReadConnector => self.handle_connector_read(event),
            ResolveConnectorState::ReadSecret => self.handle_secret_read(event),
            ResolveConnectorState::Finish => smallvec![],
            ResolveConnectorState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            ResolveConnectorState::Finish | ResolveConnectorState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == ResolveConnectorState::Error {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(SourceResolutionError::ResolveFailed);
        }

        self.output.ok_or(SourceResolutionError::ResolveFailed)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

impl Operation for ResolveBindingOperation {
    type Output = ResolvedSourceAccess;
    type Error = SourceResolutionError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            ResolveBindingState::Init => self.handle_init(),
            ResolveBindingState::ReadSecret => self.handle_secret_read(event),
            ResolveBindingState::ReadOfferedDirectory => self.handle_offered_read(event),
            ResolveBindingState::Finish => smallvec![],
            ResolveBindingState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            ResolveBindingState::Finish | ResolveBindingState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == ResolveBindingState::Error {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(SourceResolutionError::ResolveFailed);
        }

        self.output.ok_or(SourceResolutionError::ResolveFailed)?
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

pub fn resolve_connector_effect(input: ResolveConnectorInput) -> Effect {
    Effect::SubOperation(boxed_suboperation(
        ResolveConnectorOperation::new(input),
        |result| {
            Event::SubOperation(SubOperationEvent::SourceConnectorResolved {
                result: Box::new(result),
            })
        },
    ))
}

pub fn resolve_binding_effect(input: ResolveBindingInput) -> Effect {
    Effect::SubOperation(boxed_suboperation(
        ResolveBindingOperation::new(input),
        |result| Event::SubOperation(SubOperationEvent::VersionSourceAccessResolved { result }),
    ))
}

pub(crate) fn build_source_access(
    kind: SourceConnectorKind,
    public_config: &HashMap<String, String>,
    secret_config: Option<HashMap<String, String>>,
    source_path: &str,
    version: Option<String>,
    allow_root: bool,
) -> Result<ResolvedSourceAccess, SourceResolutionError> {
    validate_source_path(source_path, allow_root)?;

    if kind == SourceConnectorKind::ArunaNative {
        return Err(SourceResolutionError::UnsupportedConnectorKind(
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

pub fn resolve_inline_access(
    kind: SourceConnectorKind,
    public_config: &HashMap<String, String>,
    secret_config: HashMap<String, String>,
) -> Result<ResolvedSourceAccess, SourceResolutionError> {
    build_source_access(kind, public_config, Some(secret_config), "", None, true)
}

pub(crate) fn secret_fingerprint(secret: &aruna_core::structs::execution::source_connector::SourceConnectorSecret) -> [u8; 16] {
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

pub(crate) fn build_binding_access(
    source: &VersionSourceBinding,
    secret_config: Option<HashMap<String, String>>,
) -> Result<ResolvedSourceAccess, SourceResolutionError> {
    if source.descriptor.kind == SourceConnectorKind::ArunaNative {
        return build_native_access(source);
    }
    // An offered directory resolves only against this device's own registration,
    // which needs a storage read, so the operation owns that path.
    if source.descriptor.kind == SourceConnectorKind::LocalDirectory {
        return Err(SourceResolutionError::UnsupportedConnectorKind(
            SourceConnectorKind::LocalDirectory,
        ));
    }

    build_source_access(
        source.descriptor.kind,
        &source.descriptor.public_config,
        secret_config,
        &source.descriptor.source_path,
        source_binding_version(source)?,
        false,
    )
}

fn build_native_access(
    source: &VersionSourceBinding,
) -> Result<ResolvedSourceAccess, SourceResolutionError> {
    if source.strategy != StagingStrategy::Reference || source.connector_id.is_some() {
        return Err(SourceResolutionError::ResolveFailed);
    }
    validate_source_path(&source.descriptor.source_path, false)?;
    let (bucket, key) = source
        .descriptor
        .source_path
        .split_once('/')
        .filter(|(bucket, key)| !bucket.is_empty() && !key.is_empty())
        .ok_or(SourceResolutionError::InvalidSourcePath)?;
    if bucket.contains('/') || key.trim().is_empty() {
        return Err(SourceResolutionError::InvalidSourcePath);
    }

    let relationship_id = source
        .descriptor
        .public_config
        .get(ARUNA_NATIVE_RELATIONSHIP_ID)
        .and_then(|value| Ulid::from_string(value).ok())
        .ok_or(SourceResolutionError::ResolveFailed)?;
    let origin_node_id = source
        .descriptor
        .origin_node_id
        .ok_or(SourceResolutionError::ResolveFailed)?;
    let version = source
        .descriptor
        .version_selector
        .as_deref()
        .and_then(|selector| selector.strip_prefix("version:"))
        .and_then(|value| Ulid::from_string(value.trim()).ok())
        .ok_or(SourceResolutionError::ResolveFailed)?;

    let mut config = source.descriptor.public_config.clone();
    config.insert(
        ARUNA_NATIVE_RELATIONSHIP_ID.to_string(),
        relationship_id.to_string(),
    );
    config.insert(
        ARUNA_NATIVE_ORIGIN_NODE_ID.to_string(),
        origin_node_id.to_string(),
    );
    Ok(ResolvedSourceAccess::OpenDal {
        kind: SourceConnectorKind::ArunaNative,
        config,
        path: source.descriptor.source_path.clone(),
        version: Some(version.to_string()),
    })
}

/// Reads the device-local registration a local-directory binding names. The
/// binding carries the offered bucket, never a path: the root exists only here.
pub(crate) fn read_offered_effect(
    source: &VersionSourceBinding,
) -> Result<Effect, SourceResolutionError> {
    if source.strategy != StagingStrategy::Reference || source.connector_id.is_some() {
        return Err(SourceResolutionError::ResolveFailed);
    }
    validate_source_path(&source.descriptor.source_path, false)?;
    let bucket = source
        .descriptor
        .public_config
        .get(OFFERED_DIRECTORY_BUCKET)
        .filter(|bucket| !bucket.is_empty())
        .ok_or(SourceResolutionError::ResolveFailed)?;
    Ok(Effect::Storage(StorageEffect::Read {
        key_space: OFFERED_DIRECTORY_KEYSPACE.to_string(),
        key: bucket.as_bytes().into(),
        txn_id: None,
    }))
}

fn resolve_offered_access(
    source: &VersionSourceBinding,
    event: Event,
) -> Result<ResolvedSourceAccess, SourceResolutionError> {
    let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
        return Err(SourceResolutionError::ResolveFailed);
    };
    let Some(value) = value else {
        return Err(SourceResolutionError::NotFound);
    };
    let record = OfferedDirectory::from_bytes(&value)?;
    Ok(ResolvedSourceAccess::OpenDal {
        kind: SourceConnectorKind::LocalDirectory,
        config: HashMap::from([(OFFERED_DIRECTORY_ROOT.to_string(), record.root)]),
        path: source.descriptor.source_path.clone(),
        version: None,
    })
}

fn source_binding_version(
    source: &VersionSourceBinding,
) -> Result<Option<String>, SourceResolutionError> {
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
            .ok_or(SourceResolutionError::ResolveFailed);
    }

    if selector.contains(':') {
        return Err(SourceResolutionError::ResolveFailed);
    }

    Ok(Some(selector.to_string()))
}

pub(crate) fn binding_secret_effect(
    source: &VersionSourceBinding,
    txn_id: Option<TxnId>,
) -> Result<Effect, SourceResolutionError> {
    let Some(connector_id) = source.connector_id else {
        return Err(SourceResolutionError::ResolveFailed);
    };

    Ok(read_secret_effect(connector_id, txn_id))
}

pub(crate) fn resolve_binding_access(
    source: &VersionSourceBinding,
    event: Event,
) -> Result<ResolvedSourceAccess, SourceResolutionError> {
    let secret = parse_secret_read(event).map_err(SourceResolutionError::from)?;

    build_binding_access(source, secret.map(|secret| secret.secret_config))
}

pub fn validate_source_path(path: &str, allow_root: bool) -> Result<(), SourceResolutionError> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return allow_root
            .then_some(())
            .ok_or(SourceResolutionError::InvalidSourcePath);
    }

    let mut has_normal_component = false;
    for component in Path::new(trimmed).components() {
        match component {
            Component::Normal(_) => has_normal_component = true,
            Component::CurDir
            | Component::ParentDir
            | Component::RootDir
            | Component::Prefix(_) => {
                return Err(SourceResolutionError::InvalidSourcePath);
            }
        }
    }

    has_normal_component
        .then_some(())
        .ok_or(SourceResolutionError::InvalidSourcePath)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectors::create_connector::{SourceConnectorInput, SourceConnectorOperation};
    use crate::connectors::repository::delete_connector_effect;
    use crate::driver::{DriverContext, drive};
    use crate::staging::descriptor::build_source_binding;
    use aruna_core::events::StorageEvent;
    use aruna_core::handle::Handle;
    use aruna_storage::storage;
    use std::collections::HashMap;
    use tempfile::tempdir;

    #[tokio::test]
    async fn merges_public_secret() {
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
        let group_id = Ulid::generate();

        let created = drive(
            SourceConnectorOperation::new(SourceConnectorInput {
                group_id,
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
            &context,
        )
        .await
        .unwrap();

        let resolved = drive(
            ResolveConnectorOperation::new(ResolveConnectorInput {
                group_id,
                connector_id: created.connector.connector_id,
                source_path: "run-1/data.txt".to_string(),
                allow_root: false,
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
        assert_eq!(kind, SourceConnectorKind::Webdav);
        assert_eq!(path, "run-1/data.txt");
        assert_eq!(version, None);
        assert_eq!(config.get("username").map(String::as_str), Some("alice"));
        assert_eq!(config.get("root").map(String::as_str), Some("/datasets"));
    }

    #[test]
    fn merges_descriptor_secret() {
        let source = VersionSourceBinding {
            strategy: aruna_core::structs::execution::staging::StagingStrategy::Reference,
            descriptor: aruna_core::structs::execution::staging::PortableSourceDescriptor {
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

        let access = build_binding_access(
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
    fn rejects_invalid_selector() {
        let source = VersionSourceBinding {
            strategy: aruna_core::structs::execution::staging::StagingStrategy::Reference,
            descriptor: aruna_core::structs::execution::staging::PortableSourceDescriptor {
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
            build_binding_access(&source, None),
            Err(SourceResolutionError::ResolveFailed)
        );
    }

    #[test]
    fn accepts_persisted_selector() {
        let source = VersionSourceBinding {
            strategy: aruna_core::structs::execution::staging::StagingStrategy::Reference,
            descriptor: aruna_core::structs::execution::staging::PortableSourceDescriptor {
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

        let access = build_binding_access(&source, None).unwrap();
        let ResolvedSourceAccess::OpenDal { version, .. } = access;
        assert_eq!(version.as_deref(), Some("v42"));
    }

    #[test]
    fn requires_connector_id() {
        let source = VersionSourceBinding {
            strategy: aruna_core::structs::execution::staging::StagingStrategy::Reference,
            descriptor: aruna_core::structs::execution::staging::PortableSourceDescriptor {
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
            binding_secret_effect(&source, None),
            Err(SourceResolutionError::ResolveFailed)
        );
    }

    #[tokio::test]
    async fn uses_stored_descriptor() {
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
        let group_id = Ulid::generate();

        let created = drive(
            SourceConnectorOperation::new(SourceConnectorInput {
                group_id,
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

        let source = build_source_binding(
            aruna_core::structs::execution::staging::StagingStrategy::Reference,
            &created.connector,
            &aruna_core::structs::execution::source_access::SourceMetadata {
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
            ResolveBindingOperation::new(ResolveBindingInput { source }),
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
        assert_eq!(kind, SourceConnectorKind::Webdav);
        assert_eq!(path, "run-1/data.txt");
        assert_eq!(version, None);
        assert_eq!(
            config.get("endpoint").map(String::as_str),
            Some("https://dav.example.org")
        );
        assert_eq!(config.get("root").map(String::as_str), Some("/datasets"));
        assert_eq!(config.get("username").map(String::as_str), Some("alice"));
    }

    #[test]
    fn rejects_absolute_paths() {
        assert!(validate_source_path("/absolute/file.txt", false).is_err());
        assert!(validate_source_path("nested/file.txt", false).is_ok());
    }

    #[test]
    fn rejects_dot_paths() {
        assert!(validate_source_path("", false).is_err());
        assert!(validate_source_path("   ", false).is_err());
        assert!(validate_source_path("./file.txt", false).is_err());
        assert!(validate_source_path("nested/../file.txt", false).is_err());
    }

    #[test]
    fn rejects_aruna_native() {
        let error = build_source_access(
            SourceConnectorKind::ArunaNative,
            &HashMap::from([(
                "endpoint".to_string(),
                "https://native.example.org".to_string(),
            )]),
            None,
            "bucket/key",
            None,
            false,
        )
        .unwrap_err();
        assert_eq!(
            error,
            SourceResolutionError::UnsupportedConnectorKind(SourceConnectorKind::ArunaNative)
        );
    }

    #[test]
    fn native_binding_resolves() {
        let relationship_id = Ulid::from_bytes([7u8; 16]);
        let version_id = Ulid::from_bytes([8u8; 16]);
        let origin = iroh::SecretKey::from_bytes(&[9u8; 32]).public();
        let source = VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: aruna_core::structs::execution::staging::PortableSourceDescriptor {
                kind: SourceConnectorKind::ArunaNative,
                public_config: HashMap::from([(
                    ARUNA_NATIVE_RELATIONSHIP_ID.to_string(),
                    relationship_id.to_string(),
                )]),
                source_path: "source-bucket/nested/data.txt".to_string(),
                version_selector: Some(format!("version:{version_id}")),
                capabilities: Vec::new(),
                origin_node_id: Some(origin),
            },
            connector_id: None,
        };

        let mut operation = ResolveBindingOperation::new(ResolveBindingInput { source });
        assert!(operation.start().is_empty());
        assert!(operation.is_complete());
        assert_eq!(
            operation.finalize().unwrap(),
            ResolvedSourceAccess::OpenDal {
                kind: SourceConnectorKind::ArunaNative,
                config: HashMap::from([
                    (
                        ARUNA_NATIVE_RELATIONSHIP_ID.to_string(),
                        relationship_id.to_string(),
                    ),
                    (ARUNA_NATIVE_ORIGIN_NODE_ID.to_string(), origin.to_string()),
                ]),
                path: "source-bucket/nested/data.txt".to_string(),
                version: Some(version_id.to_string()),
            }
        );
    }

    #[test]
    fn native_binding_rejects() {
        let origin = iroh::SecretKey::from_bytes(&[9u8; 32]).public();
        let source = VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: aruna_core::structs::execution::staging::PortableSourceDescriptor {
                kind: SourceConnectorKind::ArunaNative,
                public_config: HashMap::new(),
                source_path: "source-bucket/data.txt".to_string(),
                version_selector: Some(format!("version:{}", Ulid::generate())),
                capabilities: Vec::new(),
                origin_node_id: Some(origin),
            },
            connector_id: None,
        };

        assert_eq!(
            build_binding_access(&source, None),
            Err(SourceResolutionError::ResolveFailed)
        );
    }

    fn offered_binding(bucket: &str, connector_id: Option<Ulid>) -> VersionSourceBinding {
        VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: aruna_core::structs::execution::staging::PortableSourceDescriptor {
                kind: SourceConnectorKind::LocalDirectory,
                public_config: HashMap::from([(
                    OFFERED_DIRECTORY_BUCKET.to_string(),
                    bucket.to_string(),
                )]),
                source_path: "photos/one.jpg".to_string(),
                version_selector: None,
                capabilities: Vec::new(),
                origin_node_id: None,
            },
            connector_id,
        }
    }

    // The root lives only in the device-local registration, so the sync builder
    // must refuse the kind outright instead of inventing an access from it.
    #[test]
    fn offered_needs_registration() {
        assert_eq!(
            build_binding_access(&offered_binding("photos", None), None),
            Err(SourceResolutionError::UnsupportedConnectorKind(
                SourceConnectorKind::LocalDirectory
            ))
        );
    }

    #[test]
    fn offered_read_rejects() {
        assert!(read_offered_effect(&offered_binding("photos", Some(Ulid::generate()))).is_err());
        assert!(read_offered_effect(&offered_binding("", None)).is_err());
        assert!(read_offered_effect(&offered_binding("photos", None)).is_ok());
    }

    // A registration that is not there must not resolve to some other root.
    #[test]
    fn offered_missing_registration() {
        assert_eq!(
            resolve_offered_access(
                &offered_binding("photos", None),
                Event::Storage(StorageEvent::ReadResult {
                    key: Vec::<u8>::new().into(),
                    value: None,
                })
            ),
            Err(SourceResolutionError::NotFound)
        );
    }
}
