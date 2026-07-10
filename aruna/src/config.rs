use aruna_core::document::DocumentSyncTarget;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{NODE_STATE_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::onboarding::{
    BootstrapOnboardingRequest, BootstrapOnboardingResponse, OnboardingMode, OnboardingPhase,
    OnboardingSecret, OnboardingSecretError, OnboardingSyncTicket, bootstrap_issuer_proof_message,
    bootstrap_node_proof_message,
};
use aruna_core::structs::{
    BlobTimeoutConfig, DynamicDiscoveryMethod, KIND_LABEL_KEY, NodeCapabilities,
    OidcProviderConfig, RealmConfigDocument, RealmDiscoveryConfig, RealmId, RelayPolicy,
};
use aruna_core::util::unix_timestamp_secs;
use aruna_net::{
    DiscoveryMethod, IrohRuntimeConfig, RelayMethod, endpoint_addr_from_config_string,
};
use aruna_operations::metadata::MetadataSearchStorage;
use aruna_storage::{FjallPersistPolicy, FjallStorage, StorageHandle, errors::StorageLibError};
use base64::Engine;
use byteview::ByteView;
use crypto_box::{
    PublicKey as TransportPublicKey, SalsaBox, SecretKey as TransportSecretKey,
    aead::{Aead, OsRng as CryptoOsRng},
};
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use ed25519_dalek::pkcs8::{DecodePrivateKey, EncodePrivateKey, EncodePublicKey};
use ed25519_dalek::{Signer, SigningKey};
use iroh::EndpointAddr;
use iroh::KeyParsingError;
use serde::{Deserialize, Serialize};
use std::array::TryFromSliceError;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::num::ParseIntError;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;
use thiserror::Error;

const NODE_STATE_RECORD_KEY: &[u8] = b"node_state";
const ONBOARDING_BOOTSTRAP_HTTP_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const ONBOARDING_BOOTSTRAP_HTTP_TIMEOUT: Duration = Duration::from_secs(30);

pub struct Config {
    pub storage_path: String,
    pub metadata_storage_path: String,
    pub metadata_search_storage: MetadataSearchStorage,
    pub fjall_persist_policy: FjallPersistPolicy,
    pub document_sync_storage_path: PathBuf,
    pub blob_root: String,
    pub blob_bucket_prefix: Option<String>,
    pub blob_max_bucket_size: Option<u64>,
    pub blob_multipart_bucket: Option<String>,
    pub blob_control_plane_connect_timeout_secs: u64,
    pub blob_control_plane_io_timeout_secs: u64,
    pub blob_transfer_idle_timeout_secs: u64,
    pub http_socket_addr: SocketAddr,
    pub max_http_body_size: usize,
    pub cors_allowed_origins: Vec<String>,
    pub p2p_socket_addr: SocketAddr,
    pub max_concurrent_uni_streams: Option<u64>,
    pub max_concurrent_bidi_streams: Option<u64>,
    pub node_capabilities: NodeCapabilities,
    pub realm_id: RealmId,
    pub node_id: iroh::PublicKey,
    pub net_secret_key: iroh::SecretKey,
    pub peer_nodes: Vec<iroh::PublicKey>,
    pub peer_endpoints: Vec<EndpointAddr>,
    pub document_sync_runtime: IrohRuntimeConfig,
    pub temporary_bootstrap_active: bool,
    pub discovery_method: DiscoveryMethod,
    pub relay_method: RelayMethod,
    pub default_metadata_replication_factor: u32,
    pub s3_host: String,
    pub s3_public_url: Option<String>,
    pub s3_address: String,
    pub onboarding_secret: Option<String>,
    pub oidc_providers: Vec<OidcProviderConfig>,
    pub realm_description: String,
    pub portal: PortalConfig,
    pub startup_mode: StartupMode,
    pub node_state: PersistedNodeState,
    pub node_labels: BTreeMap<String, String>,
    pub node_location: Option<String>,
    pub node_weight: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PortalConfig {
    Disabled,
    Artifact(PortalArtifactConfig),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PortalArtifactConfig {
    pub artifact_url: Option<String>,
    pub artifact_sha256: Option<String>,
    pub portal_dir: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StartupMode {
    InitializeRealm { realm_description: String },
    JoinRealm { phase: OnboardingPhase },
    Provisioned,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BootOrigin {
    InitializedRealm,
    Onboarded,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PersistedNodeStatus {
    PendingInitialization,
    PendingOnboarding,
    Complete,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PersistedNodeIdentity {
    Management {
        realm_private_key_pem: String,
    },
    Server {
        issuer_private_key_pem: String,
        delegation_signature: String,
    },
    Local,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedNodeState {
    pub boot_origin: BootOrigin,
    pub status: PersistedNodeStatus,
    pub realm_id: RealmId,
    pub net_secret_key: [u8; 32],
    pub onboarding_phase: Option<OnboardingPhase>,
    pub onboarding_sync_ticket: Option<String>,
    pub identity: PersistedNodeIdentity,
}

#[derive(Error, Debug)]
pub enum SetupError {
    #[error(transparent)]
    ConfigValueNotFound(#[from] dotenvy::Error),
    #[error(transparent)]
    SocketParsingError(#[from] std::net::AddrParseError),
    #[error(transparent)]
    KeyPairParsingError(#[from] ConversionError),
    #[error(transparent)]
    FromSliceError(#[from] TryFromSliceError),
    #[error(transparent)]
    Base64Error(#[from] base64::DecodeError),
    #[error(transparent)]
    SPKIError(#[from] ed25519_dalek::pkcs8::spki::Error),
    #[error(transparent)]
    PKCSError(#[from] ed25519_dalek::pkcs8::Error),
    #[error(transparent)]
    IrohKeyError(#[from] KeyParsingError),
    #[error(transparent)]
    ParseIntError(#[from] ParseIntError),
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    StorageLibError(#[from] StorageLibError),
    #[error("persisted node state is incompatible with this binary")]
    UnsupportedNodeIdentity,
    #[error("persisted node state does not match derived realm id")]
    PersistedNodeStateMismatch,
    #[error(transparent)]
    OnboardingSecretError(#[from] OnboardingSecretError),
    #[error(transparent)]
    ReqwestError(#[from] reqwest::Error),
    #[error(transparent)]
    Utf8Error(#[from] std::string::FromUtf8Error),
    #[error("onboarding bootstrap failed: {0}")]
    OnboardingBootstrapFailed(String),
    #[error("missing onboarding bootstrap material for {0:?} node")]
    MissingOnboardingMaterial(OnboardingMode),
    #[error("missing required config value {0}")]
    MissingConfigValue(&'static str),
    #[error("onboarding mode mismatch between secret and bootstrap response")]
    OnboardingModeMismatch,
    #[error("unexpected storage event while loading node state: {0}")]
    UnexpectedStorageEvent(String),
    #[error("invalid {key} value {value:?}: {message}")]
    InvalidConfigValue {
        key: &'static str,
        value: String,
        message: String,
    },
}

impl Config {
    pub fn is_initial_node(&self) -> bool {
        matches!(self.node_state.boot_origin, BootOrigin::InitializedRealm)
    }

    pub fn blob_timeout_config(&self) -> BlobTimeoutConfig {
        BlobTimeoutConfig {
            control_plane_connect_timeout: std::time::Duration::from_secs(
                self.blob_control_plane_connect_timeout_secs,
            ),
            control_plane_io_timeout: std::time::Duration::from_secs(
                self.blob_control_plane_io_timeout_secs,
            ),
            transfer_idle_timeout: std::time::Duration::from_secs(
                self.blob_transfer_idle_timeout_secs,
            ),
        }
    }
}

pub async fn load() -> Result<(Config, StorageHandle), SetupError> {
    let storage_path = dotenvy::var("STORAGE_PATH")?;
    let metadata_storage_path =
        dotenvy::var("CRAQLE_STORAGE_PATH").unwrap_or_else(|_| format!("{storage_path}/craqle"));
    let metadata_search_storage = metadata_search_storage_env()?;
    let fjall_persist_policy = fjall_persist_policy_env()?;
    let document_sync_storage_path = dotenvy::var("DOCUMENT_SYNC_STORAGE_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(format!("{storage_path}/document-sync")));
    let document_sync_runtime = load_document_sync_runtime_config()?;
    let blob_root =
        dotenvy::var("BLOB_ROOT").unwrap_or_else(|_| format!("{storage_path}/blobstore"));
    let blob_bucket_prefix = dotenvy::var("BLOB_BUCKET_PREFIX").ok();

    let max_concurrent_uni_streams = dotenvy::var("MAX_CONCURRENT_UNI_STREAMS")
        .ok()
        .map(|value| value.parse::<u64>())
        .transpose()?;
    let max_concurrent_bidi_streams = dotenvy::var("MAX_CONCURRENT_BIDI_STREAMS")
        .ok()
        .map(|value| value.parse::<u64>())
        .transpose()?;

    let blob_max_bucket_size = dotenvy::var("BLOB_MAX_BUCKET_SIZE")
        .ok()
        .map(|value| value.parse::<u64>())
        .transpose()?
        .or(Some(100_000));
    let blob_multipart_bucket = dotenvy::var("BLOB_MULTIPART_BUCKET")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or(Some("uploaded-parts".to_string()));
    let blob_control_plane_connect_timeout_secs =
        dotenvy::var("BLOB_CONTROL_PLANE_CONNECT_TIMEOUT_SECS")
            .ok()
            .map(|value| value.parse::<u64>())
            .transpose()?
            .unwrap_or(30);
    let blob_control_plane_io_timeout_secs = dotenvy::var("BLOB_CONTROL_PLANE_IO_TIMEOUT_SECS")
        .ok()
        .map(|value| value.parse::<u64>())
        .transpose()?
        .unwrap_or(30);
    let blob_transfer_idle_timeout_secs = dotenvy::var("BLOB_TRANSFER_IDLE_TIMEOUT_SECS")
        .ok()
        .map(|value| value.parse::<u64>())
        .transpose()?
        .unwrap_or(30 * 60);
    let http_socket_addr = SocketAddr::from_str(&dotenvy::var("SOCKET_ADDRESS")?)?;
    let max_http_body_size = dotenvy::var("MAX_HTTP_BODY_SIZE")
        .ok()
        .map(|value| value.parse::<usize>())
        .transpose()?
        .unwrap_or(aruna_api::server::DEFAULT_MAX_HTTP_BODY_SIZE);
    let cors_allowed_origins = parse_list_env("CORS_ALLOWED_ORIGINS");
    let p2p_socket_addr = SocketAddr::from_str(
        &dotenvy::var("P2P_SOCKET_ADDRESS").unwrap_or_else(|_| http_socket_addr.to_string()),
    )?;
    let additional_relay_urls = parse_list_env("P2P_ADDITIONAL_RELAY_URLS");
    validate_relay_urls("P2P_ADDITIONAL_RELAY_URLS", &additional_relay_urls)?;
    let default_metadata_replication_factor = dotenvy::var("METADATA_REPLICATION_FACTOR")
        .ok()
        .map(|value| value.parse::<u32>())
        .transpose()?
        .unwrap_or(3)
        .max(1);
    let s3_host = dotenvy::var("S3_HOST")?;
    let s3_public_url = optional_nonempty_env("S3_PUBLIC_URL")?;
    if let Some(url) = &s3_public_url {
        validate_s3_public_url(url)?;
    }
    let s3_address = dotenvy::var("S3_ADDRESS")?;
    SocketAddr::from_str(&s3_address)?;
    let node_labels = parse_node_labels_env()?;
    let node_location = dotenvy::var("ARUNA_NODE_LOCATION")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let node_weight = dotenvy::var("ARUNA_NODE_WEIGHT")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(|value| value.trim().parse::<u32>())
        .transpose()?;
    let realm_description = dotenvy::var("REALM_DESCRIPTION")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "Aruna Realm".to_string());
    let onboarding_secret = dotenvy::var("ONBOARDING_SECRET")
        .ok()
        .filter(|value| !value.trim().is_empty());
    let oidc_providers = load_oidc_providers_from_env()?;
    let portal = portal_config_env()?;

    let storage_handle =
        FjallStorage::open_with_persist_policy(&storage_path, fjall_persist_policy)?;
    let mut temporary_bootstrap_endpoint = None;
    let node_state = match load_persisted_node_state(&storage_handle).await? {
        Some(state) => state,
        None => {
            let state = match onboarding_secret.as_deref() {
                Some(onboarding_secret) => {
                    let bootstrapped = bootstrap_onboarded_node_state(
                        onboarding_secret,
                        node_location.clone(),
                        node_weight,
                        node_labels.clone(),
                    )
                    .await?;
                    temporary_bootstrap_endpoint = Some(bootstrapped.temporary_bootstrap_endpoint);
                    bootstrapped.node_state
                }
                None => generate_initialized_node_state()?,
            };
            persist_node_state(&storage_handle, &state).await?;
            state
        }
    };

    let net_secret_key = iroh::SecretKey::from_bytes(&node_state.net_secret_key);
    let node_id = net_secret_key.public();
    let (realm_id, node_capabilities) = node_capabilities_from_state(&node_state)?;
    if realm_id != node_state.realm_id {
        return Err(SetupError::PersistedNodeStateMismatch);
    }

    let mut node_state = node_state;
    if matches!(node_state.status, PersistedNodeStatus::PendingOnboarding) {
        let phase = node_state
            .onboarding_phase
            .unwrap_or(OnboardingPhase::Bootstrapped);
        if matches!(phase, OnboardingPhase::Bootstrapped) {
            let onboarding_secret = onboarding_secret.as_deref().ok_or_else(|| {
                SetupError::OnboardingBootstrapFailed(
                    "pending bootstrapped onboarding requires ONBOARDING_SECRET to refresh bootstrap material"
                        .to_string(),
                )
            })?;
            let response = refresh_onboarding_bootstrap(
                onboarding_secret,
                &node_state,
                node_location.clone(),
                node_weight,
                node_labels.clone(),
            )
            .await?;
            temporary_bootstrap_endpoint = Some(response.temporary_bootstrap_endpoint);
            node_state.onboarding_sync_ticket = Some(response.onboarding_sync_ticket);
            persist_node_state(&storage_handle, &node_state).await?;
        }
    }

    let startup_mode = match node_state.status {
        PersistedNodeStatus::PendingInitialization => StartupMode::InitializeRealm {
            realm_description: realm_description.clone(),
        },
        PersistedNodeStatus::PendingOnboarding => StartupMode::JoinRealm {
            phase: node_state
                .onboarding_phase
                .unwrap_or(OnboardingPhase::Bootstrapped),
        },
        PersistedNodeStatus::Complete => StartupMode::Provisioned,
    };

    let realm_config = load_realm_config_document(&storage_handle, &realm_id).await?;
    let (peer_nodes, mut peer_endpoints, discovery_method, relay_method) =
        realm_network_config(realm_config.as_ref(), node_id)?;
    let relay_method = relay_method.with_additional_relays(additional_relay_urls);
    let temporary_bootstrap_active = temporary_bootstrap_endpoint.is_some();
    if let Some(endpoint) = temporary_bootstrap_endpoint {
        peer_endpoints.push(endpoint);
    }

    Ok((
        Config {
            storage_path,
            metadata_storage_path,
            metadata_search_storage,
            fjall_persist_policy,
            document_sync_storage_path,
            blob_root,
            blob_bucket_prefix,
            blob_max_bucket_size,
            blob_multipart_bucket,
            blob_control_plane_connect_timeout_secs,
            blob_control_plane_io_timeout_secs,
            blob_transfer_idle_timeout_secs,
            http_socket_addr,
            max_http_body_size,
            cors_allowed_origins,
            p2p_socket_addr,
            max_concurrent_uni_streams,
            max_concurrent_bidi_streams,
            node_capabilities,
            realm_id,
            node_id,
            net_secret_key,
            peer_nodes,
            peer_endpoints,
            document_sync_runtime,
            temporary_bootstrap_active,
            discovery_method,
            relay_method,
            default_metadata_replication_factor,
            s3_host,
            s3_public_url,
            s3_address,
            onboarding_secret,
            oidc_providers,
            realm_description,
            portal,
            startup_mode,
            node_state,
            node_labels,
            node_location,
            node_weight,
        },
        storage_handle,
    ))
}

fn normalize_env_value(value: &str) -> String {
    value.trim().to_ascii_lowercase().replace('-', "_")
}

fn invalid_config_value(
    key: &'static str,
    value: impl Into<String>,
    message: impl std::fmt::Display,
) -> SetupError {
    SetupError::InvalidConfigValue {
        key,
        value: value.into(),
        message: message.to_string(),
    }
}

fn metadata_search_storage_env() -> Result<MetadataSearchStorage, SetupError> {
    const KEY: &str = "CRAQLE_SEARCH_STORAGE";
    let Some(value) = dotenvy::var(KEY).ok() else {
        return Ok(MetadataSearchStorage::Disk);
    };

    match normalize_env_value(&value).as_str() {
        "disk" => Ok(MetadataSearchStorage::Disk),
        "memory" | "in_memory" | "ram" => Ok(MetadataSearchStorage::Memory),
        _ => Err(invalid_config_value(
            KEY,
            value,
            "expected one of: disk, memory",
        )),
    }
}

fn fjall_persist_policy_env() -> Result<FjallPersistPolicy, SetupError> {
    const KEY: &str = "ARUNA_FJALL_PERSIST_MODE";
    let Some(value) = dotenvy::var(KEY).ok() else {
        return Ok(FjallPersistPolicy::default());
    };

    value
        .parse::<FjallPersistPolicy>()
        .map_err(|message| invalid_config_value(KEY, value, message))
}

fn portal_config_env() -> Result<PortalConfig, SetupError> {
    const MODE_KEY: &str = "PORTAL_MODE";
    let Some(mode) = dotenvy::var(MODE_KEY).ok() else {
        return Ok(PortalConfig::Disabled);
    };

    match normalize_env_value(&mode).as_str() {
        "disabled" => Ok(PortalConfig::Disabled),
        "artifact" => {
            let artifact_url = optional_nonempty_env("PORTAL_ARTIFACT_URL")?;
            if let Some(artifact_url) = &artifact_url {
                reqwest::Url::parse(artifact_url).map_err(|error| {
                    invalid_config_value("PORTAL_ARTIFACT_URL", artifact_url, error)
                })?;
            }
            let artifact_sha256 = optional_nonempty_env("PORTAL_ARTIFACT_SHA256")?
                .map(|value| normalize_sha256_env("PORTAL_ARTIFACT_SHA256", &value))
                .transpose()?;
            let portal_dir = PathBuf::from(required_nonempty_env("PORTAL_DIR")?);
            Ok(PortalConfig::Artifact(PortalArtifactConfig {
                artifact_url,
                artifact_sha256,
                portal_dir,
            }))
        }
        _ => Err(invalid_config_value(
            MODE_KEY,
            mode,
            "expected one of: disabled, artifact",
        )),
    }
}

fn optional_nonempty_env(key: &'static str) -> Result<Option<String>, SetupError> {
    match dotenvy::var(key) {
        Ok(value) if value.trim().is_empty() => Ok(None),
        Ok(value) => Ok(Some(value)),
        Err(dotenvy::Error::EnvVar(std::env::VarError::NotPresent)) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn required_nonempty_env(key: &'static str) -> Result<String, SetupError> {
    match dotenvy::var(key) {
        Ok(value) if !value.trim().is_empty() => Ok(value),
        Ok(value) => Err(invalid_config_value(key, value, "must not be empty")),
        Err(_) => Err(SetupError::MissingConfigValue(key)),
    }
}

fn validate_s3_public_url(value: &str) -> Result<(), SetupError> {
    let url = reqwest::Url::parse(value)
        .map_err(|error| invalid_config_value("S3_PUBLIC_URL", value, error))?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        return Err(invalid_config_value(
            "S3_PUBLIC_URL",
            value,
            "expected an absolute HTTP or HTTPS URL with a host",
        ));
    }
    Ok(())
}

fn normalize_sha256_env(key: &'static str, value: &str) -> Result<String, SetupError> {
    let trimmed = value.trim();
    if trimmed.len() != 64 || !trimmed.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return Err(invalid_config_value(
            key,
            value,
            "expected 64 hex characters",
        ));
    }
    Ok(trimmed.to_ascii_lowercase())
}

fn parse_list_env(key: &str) -> Vec<String> {
    dotenvy::var(key)
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect()
}

/// Parses the placement-map initialization/onboarding input `ARUNA_NODE_LABELS`
/// in `k=v,k2=v2` form. Rejects malformed pairs and the derived-only
/// [`KIND_LABEL_KEY`], which is stamped from `RealmNode.kind`.
fn parse_node_labels_env() -> Result<BTreeMap<String, String>, SetupError> {
    const KEY: &str = "ARUNA_NODE_LABELS";
    let raw = dotenvy::var(KEY).unwrap_or_default();
    let mut labels = BTreeMap::new();
    for pair in raw
        .split(',')
        .map(str::trim)
        .filter(|pair| !pair.is_empty())
    {
        let (label_key, label_value) = pair
            .split_once('=')
            .ok_or_else(|| invalid_config_value(KEY, pair, "expected key=value"))?;
        let label_key = label_key.trim();
        let label_value = label_value.trim();
        if label_key.is_empty() {
            return Err(invalid_config_value(KEY, pair, "empty label key"));
        }
        if label_key == KIND_LABEL_KEY {
            return Err(invalid_config_value(
                KEY,
                pair,
                format!("{KIND_LABEL_KEY} is a reserved derived label"),
            ));
        }
        labels.insert(label_key.to_string(), label_value.to_string());
    }
    Ok(labels)
}

fn load_document_sync_runtime_config() -> Result<IrohRuntimeConfig, SetupError> {
    let default = IrohRuntimeConfig::default();
    Ok(IrohRuntimeConfig {
        connect_timeout: duration_secs_env(
            "DOCUMENT_SYNC_CONNECT_TIMEOUT_SECS",
            default.connect_timeout,
        )?,
        sync_io_timeout: duration_secs_env(
            "DOCUMENT_SYNC_IO_TIMEOUT_SECS",
            default.sync_io_timeout,
        )?,
        resync_interval: duration_secs_env(
            "DOCUMENT_SYNC_RESYNC_INTERVAL_SECS",
            default.resync_interval,
        )?,
        resync_initial_backoff: duration_secs_env(
            "DOCUMENT_SYNC_RESYNC_INITIAL_BACKOFF_SECS",
            default.resync_initial_backoff,
        )?,
        resync_max_backoff: duration_secs_env(
            "DOCUMENT_SYNC_RESYNC_MAX_BACKOFF_SECS",
            default.resync_max_backoff,
        )?,
        full_sweep_interval: duration_secs_env(
            "DOCUMENT_SYNC_FULL_SWEEP_INTERVAL_SECS",
            default.full_sweep_interval,
        )?,
        full_sweep_time_of_day: duration_secs_env(
            "DOCUMENT_SYNC_FULL_SWEEP_TIME_OF_DAY_SECS",
            default.full_sweep_time_of_day,
        )?,
    })
}

fn duration_secs_env(key: &'static str, default: Duration) -> Result<Duration, SetupError> {
    let Some(value) = dotenvy::var(key).ok() else {
        return Ok(default);
    };
    let seconds = value
        .parse::<u64>()
        .map_err(|error| invalid_config_value(key, value, error))?;
    Ok(Duration::from_secs(seconds))
}

fn load_oidc_providers_from_env() -> Result<Vec<OidcProviderConfig>, SetupError> {
    let Some(provider_ids) = dotenvy::var("OIDC_PROVIDER_IDS").ok() else {
        return Ok(Vec::new());
    };

    provider_ids
        .split(',')
        .map(str::trim)
        .filter(|provider_id| !provider_id.is_empty())
        .map(|provider_id| {
            let env_prefix = provider_id.to_ascii_uppercase().replace('-', "_");
            Ok(OidcProviderConfig {
                id: provider_id.to_string(),
                issuer: dotenvy::var(format!("OIDC_{env_prefix}_ISSUER"))?,
                audience: dotenvy::var(format!("OIDC_{env_prefix}_AUDIENCE"))?,
                discovery_url: dotenvy::var(format!("OIDC_{env_prefix}_DISCOVERY_URL"))?,
            })
        })
        .collect()
}

pub async fn mark_node_state_complete(
    storage: &StorageHandle,
    node_state: &PersistedNodeState,
) -> Result<(), SetupError> {
    if matches!(node_state.status, PersistedNodeStatus::Complete) {
        return Ok(());
    }

    let mut updated_state = node_state.clone();
    updated_state.status = PersistedNodeStatus::Complete;
    updated_state.onboarding_phase = None;
    updated_state.onboarding_sync_ticket = None;
    persist_node_state(storage, &updated_state).await
}

pub async fn mark_onboarding_phase(
    storage: &StorageHandle,
    node_state: &PersistedNodeState,
    phase: OnboardingPhase,
) -> Result<(), SetupError> {
    let mut updated_state = node_state.clone();
    updated_state.onboarding_phase = Some(phase);
    persist_node_state(storage, &updated_state).await
}

fn node_capabilities_from_state(
    node_state: &PersistedNodeState,
) -> Result<(RealmId, NodeCapabilities), SetupError> {
    match &node_state.identity {
        PersistedNodeIdentity::Management {
            realm_private_key_pem,
        } => {
            let realm_signing_key = SigningKey::from_pkcs8_pem(realm_private_key_pem)?;
            let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
            let realm_verifying_key = realm_signing_key
                .verifying_key()
                .to_public_key_pem(LineEnding::default())?
                .as_bytes()
                .try_into()?;
            let realm_encoding_key = realm_signing_key
                .to_pkcs8_pem(LineEnding::default())?
                .as_bytes()
                .try_into()?;

            Ok((
                realm_id,
                NodeCapabilities::Management {
                    realm_signing_key,
                    realm_verifying_key,
                    realm_encoding_key,
                },
            ))
        }
        PersistedNodeIdentity::Server {
            issuer_private_key_pem,
            delegation_signature,
        } => Ok((
            node_state.realm_id,
            NodeCapabilities::server_node(
                SigningKey::from_pkcs8_pem(issuer_private_key_pem)?,
                node_state.realm_id,
                delegation_signature.clone(),
            )?,
        )),
        PersistedNodeIdentity::Local => Ok((
            node_state.realm_id,
            NodeCapabilities::local_node(node_state.realm_id)?,
        )),
    }
}

fn generate_initialized_node_state() -> Result<PersistedNodeState, SetupError> {
    let mut csprng = jsonwebtoken::signature::rand_core::OsRng;
    let realm_signing_key = SigningKey::generate(&mut csprng);
    let node_signing_key = SigningKey::generate(&mut csprng);

    Ok(PersistedNodeState {
        boot_origin: BootOrigin::InitializedRealm,
        status: PersistedNodeStatus::PendingInitialization,
        realm_id: RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes()),
        net_secret_key: node_signing_key.to_bytes(),
        onboarding_phase: None,
        onboarding_sync_ticket: None,
        identity: PersistedNodeIdentity::Management {
            realm_private_key_pem: realm_signing_key
                .to_pkcs8_pem(LineEnding::default())?
                .to_string(),
        },
    })
}

struct BootstrappedNodeState {
    node_state: PersistedNodeState,
    temporary_bootstrap_endpoint: EndpointAddr,
}

async fn bootstrap_onboarded_node_state(
    onboarding_secret: &str,
    node_location: Option<String>,
    node_weight: Option<u32>,
    node_labels: BTreeMap<String, String>,
) -> Result<BootstrappedNodeState, SetupError> {
    let decoded_secret = OnboardingSecret::decode(onboarding_secret)?;
    let mut csprng = jsonwebtoken::signature::rand_core::OsRng;
    let node_signing_key = SigningKey::generate(&mut csprng);
    let net_secret_key = node_signing_key.to_bytes();
    let node_id = iroh::SecretKey::from_bytes(&net_secret_key).public();

    let issuer_signing_key = if matches!(decoded_secret.mode, OnboardingMode::Server) {
        Some(SigningKey::generate(&mut csprng))
    } else {
        None
    };
    let issuer_public_key = issuer_signing_key.as_ref().map(|issuer_signing_key| {
        base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(issuer_signing_key.verifying_key().to_bytes())
    });
    let transport_secret_key = if matches!(decoded_secret.mode, OnboardingMode::Management) {
        Some(TransportSecretKey::generate(&mut CryptoOsRng))
    } else {
        None
    };
    let transport_public_key = transport_secret_key.as_ref().map(|transport_secret_key| {
        base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(transport_secret_key.public_key().as_bytes())
    });
    let node_id_string = node_id.to_string();
    let node_proof = node_signing_key
        .sign(&bootstrap_node_proof_message(
            onboarding_secret,
            &node_id_string,
            transport_public_key.as_deref(),
        ))
        .to_string();
    let issuer_proof = issuer_signing_key
        .as_ref()
        .zip(issuer_public_key.as_ref())
        .map(|(issuer_signing_key, issuer_public_key)| {
            issuer_signing_key
                .sign(&bootstrap_issuer_proof_message(
                    onboarding_secret,
                    &node_id_string,
                    issuer_public_key,
                ))
                .to_string()
        });

    let response = onboarding_bootstrap_client()?
        .post(format!(
            "{}/api/v1/onboarding/bootstrap",
            decoded_secret.seed_url.trim_end_matches('/'),
        ))
        .json(&BootstrapOnboardingRequest {
            onboarding_secret: onboarding_secret.to_string(),
            node_id: node_id_string,
            node_proof,
            transport_public_key,
            issuer_public_key,
            issuer_proof,
            node_location,
            node_weight,
            node_labels,
        })
        .send()
        .await?;

    if !response.status().is_success() {
        return Err(SetupError::OnboardingBootstrapFailed(format!(
            "bootstrap endpoint returned {}",
            response.status()
        )));
    }

    let response: BootstrapOnboardingResponse = response.json().await?;
    if response.mode != decoded_secret.mode {
        return Err(SetupError::OnboardingModeMismatch);
    }
    let realm_id = response.realm_id()?;
    validate_bootstrap_response(&response, decoded_secret.mode, realm_id, node_id)?;
    let temporary_bootstrap_endpoint = response.temporary_bootstrap_endpoint.clone();
    let identity =
        match response.mode {
            OnboardingMode::Management => {
                let wrapped_key = response.wrapped_realm_private_key.ok_or(
                    SetupError::MissingOnboardingMaterial(OnboardingMode::Management),
                )?;
                let wrapped_nonce = response.wrapped_realm_private_key_nonce.ok_or(
                    SetupError::MissingOnboardingMaterial(OnboardingMode::Management),
                )?;
                let wrapping_public_key =
                    response
                        .wrapping_public_key
                        .ok_or(SetupError::MissingOnboardingMaterial(
                            OnboardingMode::Management,
                        ))?;
                let transport_secret_key = transport_secret_key.ok_or(
                    SetupError::MissingOnboardingMaterial(OnboardingMode::Management),
                )?;
                let wrapping_public_key_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .decode(wrapping_public_key)
                    .map_err(SetupError::Base64Error)?;
                let wrapping_public_key = TransportPublicKey::from(
                    <[u8; 32]>::try_from(wrapping_public_key_bytes.as_slice())
                        .map_err(SetupError::FromSliceError)?,
                );
                let cipher = SalsaBox::new(&wrapping_public_key, &transport_secret_key);
                let nonce_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .decode(wrapped_nonce)
                    .map_err(SetupError::Base64Error)?;
                let nonce = crypto_box::Nonce::from(
                    <[u8; 24]>::try_from(nonce_bytes.as_slice())
                        .map_err(SetupError::FromSliceError)?,
                );
                let ciphertext = base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .decode(wrapped_key)
                    .map_err(SetupError::Base64Error)?;
                let realm_private_key_pem =
                    String::from_utf8(cipher.decrypt(&nonce, ciphertext.as_ref()).map_err(
                        |error| SetupError::OnboardingBootstrapFailed(error.to_string()),
                    )?)?;

                PersistedNodeIdentity::Management {
                    realm_private_key_pem,
                }
            }
            OnboardingMode::Server => PersistedNodeIdentity::Server {
                issuer_private_key_pem: issuer_signing_key
                    .ok_or(SetupError::MissingOnboardingMaterial(
                        OnboardingMode::Server,
                    ))?
                    .to_pkcs8_pem(LineEnding::default())?
                    .to_string(),
                delegation_signature: response.delegation_signature.ok_or(
                    SetupError::MissingOnboardingMaterial(OnboardingMode::Server),
                )?,
            },
            OnboardingMode::Local => PersistedNodeIdentity::Local,
        };

    Ok(BootstrappedNodeState {
        temporary_bootstrap_endpoint,
        node_state: PersistedNodeState {
            boot_origin: BootOrigin::Onboarded,
            status: PersistedNodeStatus::PendingOnboarding,
            realm_id,
            net_secret_key,
            onboarding_phase: Some(OnboardingPhase::Bootstrapped),
            onboarding_sync_ticket: Some(response.onboarding_sync_ticket),
            identity,
        },
    })
}

async fn refresh_onboarding_bootstrap(
    onboarding_secret: &str,
    node_state: &PersistedNodeState,
    node_location: Option<String>,
    node_weight: Option<u32>,
    node_labels: BTreeMap<String, String>,
) -> Result<BootstrapOnboardingResponse, SetupError> {
    let decoded_secret = OnboardingSecret::decode(onboarding_secret)?;
    let node_signing_key = SigningKey::from_bytes(&node_state.net_secret_key);
    let node_id = iroh::SecretKey::from_bytes(&node_state.net_secret_key).public();

    let mut transport_secret_key = None;
    let transport_public_key = if matches!(decoded_secret.mode, OnboardingMode::Management) {
        let secret_key = TransportSecretKey::generate(&mut CryptoOsRng);
        let public_key = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(secret_key.public_key().as_bytes());
        transport_secret_key = Some(secret_key);
        Some(public_key)
    } else {
        None
    };

    let issuer_signing_key = match (&decoded_secret.mode, &node_state.identity) {
        (
            OnboardingMode::Server,
            PersistedNodeIdentity::Server {
                issuer_private_key_pem,
                ..
            },
        ) => Some(SigningKey::from_pkcs8_pem(issuer_private_key_pem)?),
        (OnboardingMode::Server, _) => {
            return Err(SetupError::MissingOnboardingMaterial(
                OnboardingMode::Server,
            ));
        }
        _ => None,
    };
    let issuer_public_key = issuer_signing_key.as_ref().map(|issuer_signing_key| {
        base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(issuer_signing_key.verifying_key().to_bytes())
    });
    let node_id_string = node_id.to_string();
    let node_proof = node_signing_key
        .sign(&bootstrap_node_proof_message(
            onboarding_secret,
            &node_id_string,
            transport_public_key.as_deref(),
        ))
        .to_string();
    let issuer_proof = issuer_signing_key
        .as_ref()
        .zip(issuer_public_key.as_ref())
        .map(|(issuer_signing_key, issuer_public_key)| {
            issuer_signing_key
                .sign(&bootstrap_issuer_proof_message(
                    onboarding_secret,
                    &node_id_string,
                    issuer_public_key,
                ))
                .to_string()
        });

    let response = onboarding_bootstrap_client()?
        .post(format!(
            "{}/api/v1/onboarding/bootstrap",
            decoded_secret.seed_url.trim_end_matches('/'),
        ))
        .json(&BootstrapOnboardingRequest {
            onboarding_secret: onboarding_secret.to_string(),
            node_id: node_id_string,
            node_proof,
            transport_public_key,
            issuer_public_key,
            issuer_proof,
            node_location,
            node_weight,
            node_labels,
        })
        .send()
        .await?;

    if !response.status().is_success() {
        return Err(SetupError::OnboardingBootstrapFailed(format!(
            "bootstrap endpoint returned {}",
            response.status()
        )));
    }

    let response: BootstrapOnboardingResponse = response.json().await?;
    if response.mode != decoded_secret.mode {
        return Err(SetupError::OnboardingModeMismatch);
    }
    let response_realm_id = response.realm_id()?;
    if response_realm_id != node_state.realm_id {
        return Err(SetupError::OnboardingBootstrapFailed(
            "bootstrap response realm does not match persisted node state".to_string(),
        ));
    }
    validate_bootstrap_response(&response, decoded_secret.mode, node_state.realm_id, node_id)?;

    drop(transport_secret_key);
    Ok(response)
}

fn onboarding_bootstrap_client() -> Result<reqwest::Client, SetupError> {
    Ok(reqwest::Client::builder()
        .connect_timeout(ONBOARDING_BOOTSTRAP_HTTP_CONNECT_TIMEOUT)
        .timeout(ONBOARDING_BOOTSTRAP_HTTP_TIMEOUT)
        .build()?)
}

fn validate_bootstrap_response(
    response: &BootstrapOnboardingResponse,
    expected_mode: OnboardingMode,
    expected_realm_id: RealmId,
    expected_node_id: iroh::PublicKey,
) -> Result<(), SetupError> {
    if response.mode != expected_mode {
        return Err(SetupError::OnboardingModeMismatch);
    }
    if response.realm_id()? != expected_realm_id {
        return Err(SetupError::OnboardingBootstrapFailed(
            "bootstrap response realm does not match expected realm".to_string(),
        ));
    }

    let ticket = OnboardingSyncTicket::decode(&response.onboarding_sync_ticket)?;
    if ticket.payload.realm_id != expected_realm_id.to_string() {
        return Err(SetupError::OnboardingBootstrapFailed(
            "onboarding sync ticket realm does not match bootstrap response".to_string(),
        ));
    }
    if ticket.payload.node_id != expected_node_id.to_string() {
        return Err(SetupError::OnboardingBootstrapFailed(
            "onboarding sync ticket node does not match local node".to_string(),
        ));
    }
    ticket.verify(
        expected_node_id,
        &DocumentSyncTarget::RealmConfig {
            realm_id: expected_realm_id,
        },
        unix_timestamp_secs(),
    )?;

    Ok(())
}

async fn load_realm_config_document(
    storage: &StorageHandle,
    realm_id: &RealmId,
) -> Result<Option<RealmConfigDocument>, SetupError> {
    match storage
        .send_effect(Effect::Storage(StorageEffect::Read {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key: ByteView::from(*realm_id.as_bytes()),
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(bytes), ..
        }) => Ok(Some(RealmConfigDocument::from_bytes(&bytes)?)),
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Ok(None),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(SetupError::UnexpectedStorageEvent(format!("{other:?}"))),
    }
}

fn realm_network_config(
    realm_config: Option<&RealmConfigDocument>,
    local_node_id: iroh::PublicKey,
) -> Result<
    (
        Vec<iroh::PublicKey>,
        Vec<EndpointAddr>,
        DiscoveryMethod,
        RelayMethod,
    ),
    SetupError,
> {
    let Some(realm_config) = realm_config else {
        return Ok((
            Vec::new(),
            Vec::new(),
            DiscoveryMethod::N0Dns,
            RelayMethod::N0,
        ));
    };

    let nodes = realm_config
        .nodes
        .iter()
        .map(|node| iroh::PublicKey::from_str(&node.node_id))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .filter(|node_id| *node_id != local_node_id)
        .collect::<Vec<_>>();

    match &realm_config.discovery {
        RealmDiscoveryConfig::Static { endpoints } => {
            let mut endpoint_addrs = Vec::new();
            for endpoint in endpoints {
                let declared_node_id =
                    iroh::PublicKey::from_str(&endpoint.node_id).map_err(|error| {
                        invalid_config_value(
                            "realm_static_endpoint_node_id",
                            endpoint.node_id.as_str(),
                            error,
                        )
                    })?;
                let endpoint_addr = endpoint_addr_from_config_string(&endpoint.endpoint_addr)
                    .map_err(|message| {
                        invalid_config_value(
                            "realm_static_endpoint",
                            endpoint.endpoint_addr.as_str(),
                            message,
                        )
                    })?;
                if endpoint_addr.id != declared_node_id {
                    return Err(invalid_config_value(
                        "realm_static_endpoint",
                        endpoint.endpoint_addr.as_str(),
                        "endpoint_addr id does not match node_id",
                    ));
                }
                if endpoint_addr.id != local_node_id {
                    endpoint_addrs.push(endpoint_addr);
                }
            }
            Ok((
                nodes,
                endpoint_addrs,
                DiscoveryMethod::None,
                RelayMethod::None,
            ))
        }
        RealmDiscoveryConfig::Dynamic { methods } => {
            let discovery_methods = methods
                .iter()
                .map(|method| -> Result<DiscoveryMethod, SetupError> {
                    Ok(match method {
                        DynamicDiscoveryMethod::IrohDns { origins, .. } => {
                            discovery_method_from_dns_origins(origins)?
                        }
                        DynamicDiscoveryMethod::DhtSigned {
                            ttl_secs,
                            refresh_after_secs,
                        } => DiscoveryMethod::DhtSigned {
                            ttl: std::time::Duration::from_secs(*ttl_secs),
                            refresh_after: std::time::Duration::from_secs(*refresh_after_secs),
                        },
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let discovery_method = DiscoveryMethod::ordered(discovery_methods);
            let mut relay_method = RelayMethod::None;
            for method in methods {
                if let DynamicDiscoveryMethod::IrohDns { relay_policy, .. } = method {
                    relay_method = relay_method_from_policy(relay_policy)?;
                    break;
                }
            }
            Ok((nodes, Vec::new(), discovery_method, relay_method))
        }
    }
}

fn discovery_method_from_dns_origins(origins: &[String]) -> Result<DiscoveryMethod, SetupError> {
    if origins.is_empty() {
        return Err(invalid_config_value(
            "realm_discovery_origins",
            "[]",
            "at least one DNS origin is required",
        ));
    }

    let mut custom = Vec::new();
    let mut has_n0 = false;
    for origin in origins {
        let trimmed = origin.trim();
        if trimmed.is_empty() {
            return Err(invalid_config_value(
                "realm_discovery_origin",
                origin.as_str(),
                "origin must not be empty",
            ));
        }
        if normalize_env_value(trimmed) == "n0" {
            has_n0 = true;
        } else {
            custom.push(trimmed.to_string());
        }
    }

    if has_n0 && custom.is_empty() && origins.len() == 1 {
        return Ok(DiscoveryMethod::N0Dns);
    }
    if has_n0 {
        return Err(invalid_config_value(
            "realm_discovery_origins",
            origins.join(","),
            "n0 cannot be mixed with custom DNS origins",
        ));
    }

    Ok(DiscoveryMethod::CustomDns(custom))
}

fn relay_method_from_policy(policy: &RelayPolicy) -> Result<RelayMethod, SetupError> {
    match policy {
        RelayPolicy::Disabled => Ok(RelayMethod::None),
        RelayPolicy::Default => Ok(RelayMethod::N0),
        RelayPolicy::Custom { relays } => {
            validate_relay_urls("realm_relay", relays)?;
            Ok(RelayMethod::Custom(relays.clone()))
        }
    }
}

fn validate_relay_urls(key: &'static str, relays: &[String]) -> Result<(), SetupError> {
    for relay in relays {
        relay
            .parse::<iroh::RelayUrl>()
            .map_err(|error| invalid_config_value(key, relay.as_str(), error))?;
    }
    Ok(())
}

async fn load_persisted_node_state(
    storage: &StorageHandle,
) -> Result<Option<PersistedNodeState>, SetupError> {
    match storage
        .send_effect(Effect::Storage(StorageEffect::Read {
            key_space: NODE_STATE_KEYSPACE.to_string(),
            key: ByteView::from(NODE_STATE_RECORD_KEY),
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(bytes), ..
        }) => Ok(Some(
            postcard::from_bytes(&bytes).map_err(ConversionError::from)?,
        )),
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Ok(None),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(SetupError::UnexpectedStorageEvent(format!("{other:?}"))),
    }
}

async fn persist_node_state(
    storage: &StorageHandle,
    node_state: &PersistedNodeState,
) -> Result<(), SetupError> {
    let value = postcard::to_allocvec(node_state).map_err(ConversionError::from)?;
    match storage
        .send_effect(Effect::Storage(StorageEffect::Write {
            key_space: NODE_STATE_KEYSPACE.to_string(),
            key: ByteView::from(NODE_STATE_RECORD_KEY),
            value: ByteView::from(value),
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(SetupError::UnexpectedStorageEvent(format!("{other:?}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BootOrigin, PersistedNodeIdentity, PersistedNodeState, PersistedNodeStatus, PortalConfig,
        fjall_persist_policy_env, load, load_oidc_providers_from_env, parse_node_labels_env,
        persist_node_state, portal_config_env, validate_s3_public_url,
    };
    use aruna_core::structs::{
        DynamicDiscoveryMethod, RealmConfigDocument, RealmDiscoveryConfig, RealmId, RelayPolicy,
        StaticRealmEndpoint,
    };
    use aruna_net::{DiscoveryMethod, RelayMethod, endpoint_addr_to_config_string};
    use aruna_storage::{FjallPersistPolicy, FjallStorage};
    use ed25519_dalek::SigningKey;
    use std::sync::OnceLock;
    use tempfile::tempdir;
    use tokio::sync::Mutex;

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn restore_env(previous: Vec<(String, Option<String>)>) {
        for (key, value) in previous {
            match value {
                Some(value) => unsafe { std::env::set_var(key, value) },
                None => unsafe { std::env::remove_var(key) },
            }
        }
    }

    fn portal_env_keys() -> [&'static str; 4] {
        [
            "PORTAL_MODE",
            "PORTAL_ARTIFACT_URL",
            "PORTAL_ARTIFACT_SHA256",
            "PORTAL_DIR",
        ]
    }

    fn clear_portal_env() {
        for key in portal_env_keys() {
            unsafe { std::env::remove_var(key) };
        }
    }

    #[test]
    fn s3_public_url_accepts_absolute_http_and_https_urls() {
        for value in [
            "http://localhost:1337",
            "https://s3.example.test/base/path/",
        ] {
            validate_s3_public_url(value).unwrap();
        }
    }

    #[test]
    fn s3_public_url_rejects_invalid_schemes_and_hostless_values() {
        for value in [
            "file:///tmp/s3",
            "mailto:admin@example.test",
            "ftp://s3.example.test",
            "https://",
            "/relative/path",
        ] {
            let error = validate_s3_public_url(value).expect_err("invalid URL should fail");
            assert!(matches!(
                error,
                super::SetupError::InvalidConfigValue {
                    key: "S3_PUBLIC_URL",
                    ..
                }
            ));
        }
    }

    #[tokio::test]
    async fn fjall_persist_policy_env_defaults_to_buffer() {
        let _guard = env_lock().lock().await;
        let key = "ARUNA_FJALL_PERSIST_MODE";
        let previous = vec![(key.to_string(), std::env::var(key).ok())];
        unsafe { std::env::remove_var(key) };

        assert_eq!(
            fjall_persist_policy_env().unwrap(),
            FjallPersistPolicy::Buffer
        );

        restore_env(previous);
    }

    #[tokio::test]
    async fn fjall_persist_policy_env_accepts_sync_all() {
        let _guard = env_lock().lock().await;
        let key = "ARUNA_FJALL_PERSIST_MODE";
        let previous = vec![(key.to_string(), std::env::var(key).ok())];
        unsafe { std::env::set_var(key, "sync_all") };

        assert_eq!(
            fjall_persist_policy_env().unwrap(),
            FjallPersistPolicy::SyncAll
        );

        restore_env(previous);
    }

    #[tokio::test]
    async fn fjall_persist_policy_env_rejects_invalid_value() {
        let _guard = env_lock().lock().await;
        let key = "ARUNA_FJALL_PERSIST_MODE";
        let previous = vec![(key.to_string(), std::env::var(key).ok())];
        unsafe { std::env::set_var(key, "always") };

        let error = fjall_persist_policy_env().expect_err("invalid policy should fail");
        assert!(matches!(
            error,
            super::SetupError::InvalidConfigValue {
                key: "ARUNA_FJALL_PERSIST_MODE",
                ..
            }
        ));

        restore_env(previous);
    }

    #[tokio::test]
    async fn node_labels_env_defaults_to_empty() {
        let _guard = env_lock().lock().await;
        let key = "ARUNA_NODE_LABELS";
        let previous = vec![(key.to_string(), std::env::var(key).ok())];
        unsafe { std::env::remove_var(key) };

        assert!(parse_node_labels_env().unwrap().is_empty());

        restore_env(previous);
    }

    #[tokio::test]
    async fn node_labels_env_parses_comma_separated_pairs() {
        let _guard = env_lock().lock().await;
        let key = "ARUNA_NODE_LABELS";
        let previous = vec![(key.to_string(), std::env::var(key).ok())];
        unsafe { std::env::set_var(key, "tier=hot, zone = eu-west ") };

        let labels = parse_node_labels_env().unwrap();
        assert_eq!(labels.get("tier"), Some(&"hot".to_string()));
        assert_eq!(labels.get("zone"), Some(&"eu-west".to_string()));
        assert_eq!(labels.len(), 2);

        restore_env(previous);
    }

    #[tokio::test]
    async fn node_labels_env_rejects_reserved_kind_key() {
        let _guard = env_lock().lock().await;
        let key = "ARUNA_NODE_LABELS";
        let previous = vec![(key.to_string(), std::env::var(key).ok())];
        unsafe { std::env::set_var(key, "aruna.io/kind=Server") };

        let error = parse_node_labels_env().expect_err("reserved key should fail");
        assert!(matches!(
            error,
            super::SetupError::InvalidConfigValue {
                key: "ARUNA_NODE_LABELS",
                ..
            }
        ));

        restore_env(previous);
    }

    #[tokio::test]
    async fn node_labels_env_rejects_malformed_pair() {
        let _guard = env_lock().lock().await;
        let key = "ARUNA_NODE_LABELS";
        let previous = vec![(key.to_string(), std::env::var(key).ok())];
        unsafe { std::env::set_var(key, "tier") };

        let error = parse_node_labels_env().expect_err("missing '=' should fail");
        assert!(matches!(
            error,
            super::SetupError::InvalidConfigValue {
                key: "ARUNA_NODE_LABELS",
                ..
            }
        ));

        restore_env(previous);
    }

    #[tokio::test]
    async fn loads_oidc_providers_from_env() {
        let _guard = env_lock().lock().await;
        let vars = [
            ("OIDC_PROVIDER_IDS", "main,internal".to_string()),
            ("OIDC_MAIN_ISSUER", "https://issuer.example".to_string()),
            ("OIDC_MAIN_AUDIENCE", "aruna-api".to_string()),
            (
                "OIDC_MAIN_DISCOVERY_URL",
                "https://issuer.example/.well-known/openid-configuration".to_string(),
            ),
            (
                "OIDC_INTERNAL_ISSUER",
                "https://internal.example".to_string(),
            ),
            ("OIDC_INTERNAL_AUDIENCE", "internal-client".to_string()),
            (
                "OIDC_INTERNAL_DISCOVERY_URL",
                "https://internal.example/.well-known/openid-configuration".to_string(),
            ),
        ];
        let previous: Vec<_> = vars
            .iter()
            .map(|(key, _)| ((*key).to_string(), std::env::var(key).ok()))
            .collect();

        for (key, value) in &vars {
            unsafe { std::env::set_var(key, value) };
        }

        let providers = load_oidc_providers_from_env().unwrap();
        assert_eq!(providers.len(), 2);
        assert_eq!(providers[0].id, "main");
        assert_eq!(providers[0].issuer, "https://issuer.example");
        assert_eq!(providers[1].id, "internal");
        assert_eq!(providers[1].audience, "internal-client");

        restore_env(previous);
    }

    #[tokio::test]
    async fn oidc_provider_env_requires_all_fields() {
        let _guard = env_lock().lock().await;
        let vars = [
            ("OIDC_PROVIDER_IDS", "main".to_string()),
            ("OIDC_MAIN_ISSUER", "https://issuer.example".to_string()),
            ("OIDC_MAIN_AUDIENCE", "aruna-api".to_string()),
        ];
        let cleanup_keys = [
            "OIDC_PROVIDER_IDS",
            "OIDC_MAIN_ISSUER",
            "OIDC_MAIN_AUDIENCE",
            "OIDC_MAIN_DISCOVERY_URL",
        ];
        let previous: Vec<_> = cleanup_keys
            .iter()
            .map(|key| ((*key).to_string(), std::env::var(key).ok()))
            .collect();

        unsafe { std::env::remove_var("OIDC_MAIN_DISCOVERY_URL") };
        for (key, value) in &vars {
            unsafe { std::env::set_var(key, value) };
        }

        assert!(load_oidc_providers_from_env().is_err());

        restore_env(previous);
    }

    #[tokio::test]
    async fn load_includes_oidc_providers_in_config() {
        let _guard = env_lock().lock().await;
        let tempdir = tempdir().unwrap();
        let vars = [
            ("STORAGE_PATH", tempdir.path().to_str().unwrap().to_string()),
            ("SOCKET_ADDRESS", "127.0.0.1:3000".to_string()),
            ("P2P_SOCKET_ADDRESS", "127.0.0.1:3001".to_string()),
            ("S3_HOST", "127.0.0.1:1337".to_string()),
            ("S3_ADDRESS", "127.0.0.1:1337".to_string()),
            ("ARUNA_FJALL_PERSIST_MODE", "sync_all".to_string()),
            ("OIDC_PROVIDER_IDS", "main".to_string()),
            ("OIDC_MAIN_ISSUER", "https://issuer.example".to_string()),
            ("OIDC_MAIN_AUDIENCE", "aruna-api".to_string()),
            (
                "OIDC_MAIN_DISCOVERY_URL",
                "https://issuer.example/.well-known/openid-configuration".to_string(),
            ),
            ("PORTAL_MODE", "disabled".to_string()),
        ];
        let cleanup_keys = [
            "STORAGE_PATH",
            "SOCKET_ADDRESS",
            "P2P_SOCKET_ADDRESS",
            "S3_HOST",
            "S3_ADDRESS",
            "ARUNA_FJALL_PERSIST_MODE",
            "OIDC_PROVIDER_IDS",
            "OIDC_MAIN_ISSUER",
            "OIDC_MAIN_AUDIENCE",
            "OIDC_MAIN_DISCOVERY_URL",
            "PORTAL_MODE",
            "PORTAL_ARTIFACT_URL",
            "PORTAL_ARTIFACT_SHA256",
            "PORTAL_DIR",
        ];
        let previous: Vec<_> = cleanup_keys
            .iter()
            .map(|key| ((*key).to_string(), std::env::var(key).ok()))
            .collect();
        for (key, value) in &vars {
            unsafe { std::env::set_var(key, value) };
        }

        let (config, _storage) = load().await.unwrap();
        assert_eq!(config.oidc_providers.len(), 1);
        assert_eq!(config.oidc_providers[0].id, "main");
        assert_eq!(config.oidc_providers[0].audience, "aruna-api");
        assert_eq!(config.discovery_method, DiscoveryMethod::N0Dns);
        assert_eq!(config.relay_method, RelayMethod::N0);
        assert_eq!(config.fjall_persist_policy, FjallPersistPolicy::SyncAll);
        assert!(matches!(config.portal, PortalConfig::Disabled));

        restore_env(previous);
    }

    #[tokio::test]
    async fn portal_config_defaults_to_disabled() {
        let _guard = env_lock().lock().await;
        let previous: Vec<_> = portal_env_keys()
            .iter()
            .map(|key| ((*key).to_string(), std::env::var(key).ok()))
            .collect();
        clear_portal_env();

        assert!(matches!(
            portal_config_env().unwrap(),
            PortalConfig::Disabled
        ));

        restore_env(previous);
    }

    #[tokio::test]
    async fn portal_config_accepts_artifact_mode() {
        let _guard = env_lock().lock().await;
        let previous: Vec<_> = portal_env_keys()
            .iter()
            .map(|key| ((*key).to_string(), std::env::var(key).ok()))
            .collect();
        let portal_dir = tempdir().unwrap();
        unsafe {
            std::env::set_var("PORTAL_MODE", "artifact");
            std::env::set_var("PORTAL_ARTIFACT_URL", "https://example.test/portal.tar.gz");
            std::env::set_var(
                "PORTAL_ARTIFACT_SHA256",
                "0DCA71F9A1193B09A55843B1D5ABC1E99445A9E1226CE42FBA05EDBC80B5DB61",
            );
            std::env::set_var("PORTAL_DIR", portal_dir.path());
        }

        let PortalConfig::Artifact(config) = portal_config_env().unwrap() else {
            panic!("expected artifact portal config");
        };
        assert_eq!(
            config.artifact_url.as_deref(),
            Some("https://example.test/portal.tar.gz")
        );
        assert_eq!(
            config.artifact_sha256.as_deref(),
            Some("0dca71f9a1193b09a55843b1d5abc1e99445a9e1226ce42fba05edbc80b5db61")
        );
        assert_eq!(config.portal_dir, portal_dir.path());

        restore_env(previous);
    }

    #[tokio::test]
    async fn portal_config_requires_portal_dir() {
        let _guard = env_lock().lock().await;
        let previous: Vec<_> = portal_env_keys()
            .iter()
            .map(|key| ((*key).to_string(), std::env::var(key).ok()))
            .collect();
        clear_portal_env();
        unsafe { std::env::set_var("PORTAL_MODE", "artifact") };

        let error = portal_config_env().expect_err("missing artifact fields should fail");
        assert!(matches!(
            error,
            super::SetupError::MissingConfigValue("PORTAL_DIR")
        ));

        restore_env(previous);
    }

    #[tokio::test]
    async fn portal_config_allows_existing_portal_without_download_fields() {
        let _guard = env_lock().lock().await;
        let previous: Vec<_> = portal_env_keys()
            .iter()
            .map(|key| ((*key).to_string(), std::env::var(key).ok()))
            .collect();
        let portal_dir = tempdir().unwrap();
        clear_portal_env();
        unsafe {
            std::env::set_var("PORTAL_MODE", "artifact");
            std::env::set_var("PORTAL_DIR", portal_dir.path());
        }

        let PortalConfig::Artifact(config) = portal_config_env().unwrap() else {
            panic!("expected artifact portal config");
        };
        assert_eq!(config.artifact_url, None);
        assert_eq!(config.artifact_sha256, None);
        assert_eq!(config.portal_dir, portal_dir.path());

        restore_env(previous);
    }

    #[tokio::test]
    async fn portal_config_rejects_invalid_mode() {
        let _guard = env_lock().lock().await;
        let previous: Vec<_> = portal_env_keys()
            .iter()
            .map(|key| ((*key).to_string(), std::env::var(key).ok()))
            .collect();
        clear_portal_env();
        unsafe { std::env::set_var("PORTAL_MODE", "local") };

        let error = portal_config_env().expect_err("invalid portal mode should fail");
        assert!(matches!(
            error,
            super::SetupError::InvalidConfigValue {
                key: "PORTAL_MODE",
                ..
            }
        ));

        restore_env(previous);
    }

    #[test]
    fn realm_discovery() {
        let realm_id = RealmId::from_bytes([41u8; 32]);
        let local_node_id = iroh::SecretKey::from_bytes(&[42u8; 32]).public();
        let realm_config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        let (_, _, discovery_method, relay_method) =
            super::realm_network_config(Some(&realm_config), local_node_id).unwrap();

        assert_eq!(
            discovery_method.enabled_methods(),
            vec!["n0_dns".to_string(), "dht_signed".to_string()]
        );
        assert_eq!(relay_method, RelayMethod::N0);
    }

    #[test]
    fn realm_discovery_rejects_invalid_dns_origins() {
        let realm_id = RealmId::from_bytes([43u8; 32]);
        let local_node_id = iroh::SecretKey::from_bytes(&[44u8; 32]).public();
        let mut realm_config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());

        realm_config.discovery = RealmDiscoveryConfig::Dynamic {
            methods: vec![DynamicDiscoveryMethod::IrohDns {
                origins: Vec::new(),
                relay_policy: RelayPolicy::Default,
            }],
        };
        assert!(super::realm_network_config(Some(&realm_config), local_node_id).is_err());

        realm_config.discovery = RealmDiscoveryConfig::Dynamic {
            methods: vec![DynamicDiscoveryMethod::IrohDns {
                origins: vec!["n0".to_string(), "https://dns.example".to_string()],
                relay_policy: RelayPolicy::Default,
            }],
        };
        assert!(super::realm_network_config(Some(&realm_config), local_node_id).is_err());
    }

    #[test]
    fn static_realm_endpoint_validates_declared_node_id() {
        let realm_id = RealmId::from_bytes([45u8; 32]);
        let local_node_id = iroh::SecretKey::from_bytes(&[46u8; 32]).public();
        let declared_node = iroh::SecretKey::from_bytes(&[47u8; 32]).public();
        let endpoint_node = iroh::SecretKey::from_bytes(&[48u8; 32]).public();
        let endpoint_addr =
            iroh::EndpointAddr::new(endpoint_node).with_ip_addr("127.0.0.1:3001".parse().unwrap());
        let mut realm_config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        realm_config.discovery = RealmDiscoveryConfig::Static {
            endpoints: vec![StaticRealmEndpoint {
                node_id: declared_node.to_string(),
                endpoint_addr: endpoint_addr_to_config_string(&endpoint_addr),
            }],
        };

        assert!(super::realm_network_config(Some(&realm_config), local_node_id).is_err());
    }

    #[tokio::test]
    async fn extra_relays() {
        let _guard = env_lock().lock().await;
        let tempdir = tempdir().unwrap();
        let vars = [
            ("STORAGE_PATH", tempdir.path().to_str().unwrap().to_string()),
            ("SOCKET_ADDRESS", "127.0.0.1:3000".to_string()),
            ("P2P_SOCKET_ADDRESS", "127.0.0.1:3001".to_string()),
            ("S3_HOST", "127.0.0.1:1337".to_string()),
            ("S3_ADDRESS", "127.0.0.1:1337".to_string()),
            ("ARUNA_FJALL_PERSIST_MODE", "buffer".to_string()),
            (
                "P2P_ADDITIONAL_RELAY_URLS",
                "https://relay-a.example, https://relay-b.example".to_string(),
            ),
            ("PORTAL_MODE", "disabled".to_string()),
        ];
        let cleanup_keys = [
            "STORAGE_PATH",
            "SOCKET_ADDRESS",
            "P2P_SOCKET_ADDRESS",
            "S3_HOST",
            "S3_ADDRESS",
            "ARUNA_FJALL_PERSIST_MODE",
            "P2P_ADDITIONAL_RELAY_URLS",
            "PORTAL_MODE",
            "PORTAL_ARTIFACT_URL",
            "PORTAL_ARTIFACT_SHA256",
            "PORTAL_DIR",
        ];
        let previous: Vec<_> = cleanup_keys
            .iter()
            .map(|key| ((*key).to_string(), std::env::var(key).ok()))
            .collect();
        for (key, value) in &vars {
            unsafe { std::env::set_var(key, value) };
        }

        let (config, _storage) = load().await.unwrap();

        assert_eq!(
            config.relay_method,
            RelayMethod::N0WithCustom(vec![
                "https://relay-a.example".to_string(),
                "https://relay-b.example".to_string(),
            ])
        );

        restore_env(previous);
    }

    #[tokio::test]
    async fn ignores_onboarding_secret_when_node_state_exists() {
        let _guard = env_lock().lock().await;
        let tempdir = tempdir().unwrap();
        let storage = FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();

        let mut csprng = jsonwebtoken::signature::rand_core::OsRng;
        let realm_signing_key = SigningKey::generate(&mut csprng);
        let net_signing_key = SigningKey::generate(&mut csprng);
        let node_state = PersistedNodeState {
            boot_origin: BootOrigin::Onboarded,
            status: PersistedNodeStatus::Complete,
            realm_id: RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes()),
            net_secret_key: net_signing_key.to_bytes(),
            onboarding_phase: None,
            onboarding_sync_ticket: None,
            identity: PersistedNodeIdentity::Local,
        };
        persist_node_state(&storage, &node_state).await.unwrap();
        drop(storage);

        let vars = [
            ("STORAGE_PATH", tempdir.path().to_str().unwrap().to_string()),
            ("SOCKET_ADDRESS", "127.0.0.1:3000".to_string()),
            ("P2P_SOCKET_ADDRESS", "127.0.0.1:3001".to_string()),
            ("S3_HOST", "127.0.0.1:1337".to_string()),
            ("S3_ADDRESS", "127.0.0.1:1337".to_string()),
            ("ARUNA_FJALL_PERSIST_MODE", "buffer".to_string()),
            (
                "ONBOARDING_SECRET",
                "definitely-not-a-valid-secret".to_string(),
            ),
            ("PORTAL_MODE", "disabled".to_string()),
        ];
        let cleanup_keys = [
            "STORAGE_PATH",
            "SOCKET_ADDRESS",
            "P2P_SOCKET_ADDRESS",
            "S3_HOST",
            "S3_ADDRESS",
            "ARUNA_FJALL_PERSIST_MODE",
            "ONBOARDING_SECRET",
            "PORTAL_MODE",
            "PORTAL_ARTIFACT_URL",
            "PORTAL_ARTIFACT_SHA256",
            "PORTAL_DIR",
        ];
        let previous: Vec<_> = cleanup_keys
            .iter()
            .map(|key| ((*key).to_string(), std::env::var(key).ok()))
            .collect();

        for (key, value) in &vars {
            unsafe { std::env::set_var(key, value) };
        }

        let (config, _storage) = load().await.unwrap();
        assert!(!config.is_initial_node());
        assert!(matches!(
            config.startup_mode,
            super::StartupMode::Provisioned
        ));

        restore_env(previous);
    }

    #[tokio::test]
    async fn pending_core_documents_fetched_does_not_require_onboarding_secret() {
        let _guard = env_lock().lock().await;
        let tempdir = tempdir().unwrap();
        let storage = FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();

        let mut csprng = jsonwebtoken::signature::rand_core::OsRng;
        let realm_signing_key = SigningKey::generate(&mut csprng);
        let net_signing_key = SigningKey::generate(&mut csprng);
        let node_state = PersistedNodeState {
            boot_origin: BootOrigin::Onboarded,
            status: PersistedNodeStatus::PendingOnboarding,
            realm_id: RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes()),
            net_secret_key: net_signing_key.to_bytes(),
            onboarding_phase: Some(aruna_core::onboarding::OnboardingPhase::CoreDocumentsFetched),
            onboarding_sync_ticket: Some("already-fetched".to_string()),
            identity: PersistedNodeIdentity::Local,
        };
        persist_node_state(&storage, &node_state).await.unwrap();
        drop(storage);

        let vars = [
            ("STORAGE_PATH", tempdir.path().to_str().unwrap().to_string()),
            ("SOCKET_ADDRESS", "127.0.0.1:3000".to_string()),
            ("P2P_SOCKET_ADDRESS", "127.0.0.1:3001".to_string()),
            ("S3_HOST", "127.0.0.1:1337".to_string()),
            ("S3_ADDRESS", "127.0.0.1:1337".to_string()),
            ("ARUNA_FJALL_PERSIST_MODE", "buffer".to_string()),
            ("PORTAL_MODE", "disabled".to_string()),
        ];
        let cleanup_keys = [
            "STORAGE_PATH",
            "SOCKET_ADDRESS",
            "P2P_SOCKET_ADDRESS",
            "S3_HOST",
            "S3_ADDRESS",
            "ARUNA_FJALL_PERSIST_MODE",
            "ONBOARDING_SECRET",
            "PORTAL_MODE",
            "PORTAL_ARTIFACT_URL",
            "PORTAL_ARTIFACT_SHA256",
            "PORTAL_DIR",
        ];
        let previous: Vec<_> = cleanup_keys
            .iter()
            .map(|key| ((*key).to_string(), std::env::var(key).ok()))
            .collect();

        unsafe { std::env::remove_var("ONBOARDING_SECRET") };
        for (key, value) in &vars {
            unsafe { std::env::set_var(key, value) };
        }

        let (config, _storage) = load().await.unwrap();
        assert!(matches!(
            config.startup_mode,
            super::StartupMode::JoinRealm {
                phase: aruna_core::onboarding::OnboardingPhase::CoreDocumentsFetched
            }
        ));
        assert!(!config.temporary_bootstrap_active);

        restore_env(previous);
    }
}
