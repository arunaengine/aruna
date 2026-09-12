use aruna_api::s3::server::S3ServerTimeouts;
use aruna_core::document::DocumentSyncTarget;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keys::generate_signing_key;
use aruna_core::keyspaces::{NODE_STATE_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::onboarding::{
    BootstrapOnboardingRequest, BootstrapOnboardingResponse, OnboardingMode, OnboardingPhase,
    OnboardingSecret, OnboardingSecretError, OnboardingSyncTicket, issuer_proof_message,
    node_proof_message,
};
use aruna_core::structs::{
    BlobTimeoutConfig, DynamicDiscoveryMethod, NodeBackendsConfig, NodeCapabilities,
    OidcProviderConfig, RealmConfigDocument, RealmDiscoveryConfig, RealmId, RelayPolicy,
    RoCrateLimits, StaticRealmEndpoint,
};
use aruna_core::time::unix_timestamp_secs;
use aruna_core::types::UserId;
use aruna_net::{DiscoveryMethod, IrohRuntimeConfig, RelayMethod, parse_endpoint_config};

use crate::identity::{EnrollmentPlan, IdentityStore, plan_enrollment};
use crate::settings::{Settings, invalid_config_value, normalize_env_value, validate_relay_urls};
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

pub struct Config {
    pub storage_path: String,
    pub metadata_storage_path: String,
    pub metadata_search_storage: MetadataSearchStorage,
    pub fjall_persist_policy: FjallPersistPolicy,
    pub document_sync_storage_path: PathBuf,
    pub blob_root: String,
    pub blob_backends: NodeBackendsConfig,
    pub blob_bucket_prefix: Option<String>,
    pub blob_max_bucket_size: Option<u64>,
    pub blob_multipart_bucket: Option<String>,
    pub blob_control_plane_connect_timeout_secs: u64,
    pub blob_control_plane_io_timeout_secs: u64,
    pub blob_transfer_idle_timeout_secs: u64,
    pub onboarding_bootstrap_timeout_secs: u64,
    pub onboarding_document_sync_timeout_secs: u64,
    pub s3_initial_request_timeout_secs: u64,
    pub s3_connection_idle_timeout_secs: u64,
    pub s3_stream_lifetime_timeout_secs: u64,
    pub http_socket_addr: SocketAddr,
    pub ops_socket_addr: SocketAddr,
    pub max_http_body_size: usize,
    pub cors_allowed_origins: Vec<String>,
    pub desktop_cors: bool,
    pub mcp_enabled: bool,
    pub portal_csp_extra_origins: Vec<String>,
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
    /// `None` when `S3_HOST`/`S3_ADDRESS` are unset or empty: no S3 listener.
    pub s3_host: Option<String>,
    pub api_public_url: Option<String>,
    pub s3_public_url: Option<String>,
    pub trusted_proxies: Vec<ipnet::IpNet>,
    pub rocrate_limits: RoCrateLimits,
    pub rate_limits: RateLimitSettings,
    pub s3_address: Option<String>,
    pub onboarding_secret: Option<String>,
    pub oidc_providers: Vec<OidcProviderConfig>,
    pub realm_description: String,
    pub portal: PortalConfig,
    pub assistant_proxy: bool,
    pub startup_mode: StartupMode,
    pub node_state: PersistedNodeState,
    pub node_labels: BTreeMap<String, String>,
    pub node_location: Option<String>,
    pub node_weight: Option<u32>,
}

/// Operator-configurable request admission limits for the REST and S3 planes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RateLimitSettings {
    pub ip_per_minute: u32,
    pub ip_burst: u32,
    pub principal_per_minute: u32,
    pub principal_burst: u32,
    pub s3_max_connections: u32,
    pub s3_max_requests: u32,
}

impl Default for RateLimitSettings {
    fn default() -> Self {
        // Mirrors the api-layer defaults: generous and identical for every caller.
        Self {
            ip_per_minute: 6_000,
            ip_burst: 1_000,
            principal_per_minute: 3_000,
            principal_burst: 500,
            s3_max_connections: 1_024,
            s3_max_requests: 512,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PortalConfig {
    Disabled,
    /// The portal serves the SPA on its own listener, so an enabled portal
    /// without an address is a configuration error rather than a default.
    Artifact {
        artifact: PortalArtifactConfig,
        socket_addr: SocketAddr,
    },
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

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PersistedNodeIdentity {
    Management {
        realm_private_key_pem: String,
    },
    Server {
        issuer_private_key_pem: String,
        delegation_signature: String,
    },
    /// Owner-bound device. The owner is copied from the enrollment answer: a
    /// device holds no realm state to read it back from before it has joined.
    User {
        owner: UserId,
    },
}

impl std::fmt::Debug for PersistedNodeIdentity {
    /// Redacts private key material: this record may be printed in a diagnostic
    /// path, and keys are not diagnostic output.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Management { .. } => f
                .debug_struct("Management")
                .field("realm_private_key_pem", &"<redacted>")
                .finish(),
            Self::Server { .. } => f
                .debug_struct("Server")
                .field("issuer_private_key_pem", &"<redacted>")
                .field("delegation_signature", &"<redacted>")
                .finish(),
            Self::User { owner } => f.debug_struct("User").field("owner", owner).finish(),
        }
    }
}

impl PersistedNodeIdentity {
    /// Owner of a device identity; `None` for infrastructure nodes.
    pub fn owner(&self) -> Option<UserId> {
        match self {
            Self::User { owner } => Some(*owner),
            Self::Management { .. } | Self::Server { .. } => None,
        }
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedNodeState {
    pub boot_origin: BootOrigin,
    pub status: PersistedNodeStatus,
    pub realm_id: RealmId,
    pub net_secret_key: [u8; 32],
    pub onboarding_phase: Option<OnboardingPhase>,
    pub onboarding_sync_ticket: Option<String>,
    pub identity: PersistedNodeIdentity,
}

impl std::fmt::Debug for PersistedNodeState {
    /// Redacts the network secret; the rest of the record is diagnostic.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PersistedNodeState")
            .field("boot_origin", &self.boot_origin)
            .field("status", &self.status)
            .field("realm_id", &self.realm_id)
            .field("net_secret_key", &"<redacted>")
            .field("onboarding_phase", &self.onboarding_phase)
            .field("onboarding_sync_ticket", &self.onboarding_sync_ticket)
            .field("identity", &self.identity)
            .finish()
    }
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
    #[error("persisted node state does not match derived realm id")]
    PersistedNodeStateMismatch,
    #[error(transparent)]
    OnboardingSecretError(#[from] OnboardingSecretError),
    #[error(transparent)]
    ReqwestError(#[from] reqwest::Error),
    #[error("failed to read the backends file: {0}")]
    BackendsFileError(#[from] std::io::Error),
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

    /// Owner this node is enrolled for when it is a device; `None` otherwise.
    pub fn device_owner(&self) -> Option<UserId> {
        self.node_state.identity.owner()
    }

    /// Budget for the onboarding document sync and the placement wait built on
    /// it, both driven from `main.rs` and the integration harness.
    pub fn onboarding_sync_timeout(&self) -> Duration {
        Duration::from_secs(self.onboarding_document_sync_timeout_secs)
    }

    pub fn s3_timeouts(&self) -> S3ServerTimeouts {
        S3ServerTimeouts {
            initial_request: Duration::from_secs(self.s3_initial_request_timeout_secs),
            connection_idle: Duration::from_secs(self.s3_connection_idle_timeout_secs),
            stream_lifetime: Duration::from_secs(self.s3_stream_lifetime_timeout_secs),
        }
    }
}

/// Opens local storage, resolves or bootstraps the persisted node state and
/// derives the realm network configuration from it.
pub async fn resolve_settings(settings: Settings) -> Result<(Config, StorageHandle), SetupError> {
    let Settings {
        storage_path,
        metadata_storage_path,
        metadata_search_storage,
        fjall_persist_policy,
        document_sync_storage_path,
        blob_root,
        blob_backends,
        blob_bucket_prefix,
        blob_max_bucket_size,
        blob_multipart_bucket,
        blob_control_plane_connect_timeout_secs,
        blob_control_plane_io_timeout_secs,
        blob_transfer_idle_timeout_secs,
        onboarding_bootstrap_timeout_secs,
        onboarding_document_sync_timeout_secs,
        s3_initial_request_timeout_secs,
        s3_connection_idle_timeout_secs,
        s3_stream_lifetime_timeout_secs,
        http_socket_addr,
        ops_socket_addr,
        max_http_body_size,
        cors_allowed_origins,
        desktop_cors,
        mcp_enabled,
        portal_csp_extra_origins,
        p2p_socket_addr,
        additional_relay_urls,
        max_concurrent_uni_streams,
        max_concurrent_bidi_streams,
        document_sync_runtime,
        default_metadata_replication_factor,
        s3_host,
        api_public_url,
        s3_public_url,
        trusted_proxies,
        rocrate_limits,
        rate_limits,
        s3_address,
        onboarding_secret,
        oidc_providers,
        realm_description,
        portal,
        assistant_proxy,
        node_labels,
        node_location,
        node_weight,
    } = settings;
    let bootstrap_timeout = Duration::from_secs(onboarding_bootstrap_timeout_secs);
    let storage_handle = FjallStorage::open_with_policy(&storage_path, fjall_persist_policy)?;
    // Opening storage is explicit here: the identity store owns every read and
    // write of the persisted identity record.
    let identity = IdentityStore::open(storage_handle.clone());
    let loaded = identity.load().await?;
    let plan = plan_enrollment(loaded.as_ref(), onboarding_secret.is_some())?;
    let (mut node_state, mut temporary_bootstrap_endpoint, mut enrollment_endpoints) = match plan {
        EnrollmentPlan::Generate => {
            let state = identity.generate()?;
            identity.persist(&state).await?;
            (state, None, Vec::new())
        }
        EnrollmentPlan::Bootstrap => {
            let secret = onboarding_secret
                .as_deref()
                .expect("the bootstrap plan requires an onboarding secret");
            let bootstrapped = bootstrap_node_state(
                secret,
                node_location.clone(),
                node_weight,
                node_labels.clone(),
                bootstrap_timeout,
            )
            .await?;
            identity.persist(&bootstrapped.node_state).await?;
            (
                bootstrapped.node_state,
                Some(bootstrapped.temporary_bootstrap_endpoint),
                bootstrapped.realm_endpoints,
            )
        }
        EnrollmentPlan::Refresh | EnrollmentPlan::Ready => (
            loaded.expect("a refresh or ready plan has persisted identity"),
            None,
            Vec::new(),
        ),
    };

    let net_secret_key = iroh::SecretKey::from_bytes(&node_state.net_secret_key);
    let node_id = net_secret_key.public();
    let (realm_id, node_capabilities) = identity.capabilities(&node_state)?;
    if realm_id != node_state.realm_id {
        return Err(SetupError::PersistedNodeStateMismatch);
    }
    validate_s3_profile(s3_address.as_deref(), &node_capabilities)?;

    // A pending bootstrapped enrollment refreshes after the local checks, so a
    // bad local configuration never sends an enrollment request.
    if matches!(plan, EnrollmentPlan::Refresh) {
        let secret = onboarding_secret
            .as_deref()
            .expect("the refresh plan requires an onboarding secret");
        let response = refresh_onboarding_bootstrap(
            secret,
            &node_state,
            node_location.clone(),
            node_weight,
            node_labels.clone(),
            bootstrap_timeout,
        )
        .await?;
        temporary_bootstrap_endpoint = Some(response.temporary_bootstrap_endpoint);
        enrollment_endpoints = IdentityStore::onboarding_endpoints(&response.realm_endpoints);
        node_state.onboarding_sync_ticket = Some(response.onboarding_sync_ticket);
        identity.persist(&node_state).await?;
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

    let realm_config = load_realm_config(&storage_handle, &realm_id).await?;
    let (peer_nodes, mut peer_endpoints, discovery_method, relay_method) =
        realm_network_config(realm_config.as_ref(), node_id)?;
    let relay_method = relay_method.with_additional_relays(additional_relay_urls);
    let temporary_bootstrap_active = temporary_bootstrap_endpoint.is_some();
    if let Some(endpoint) = temporary_bootstrap_endpoint {
        peer_endpoints.push(endpoint);
    }
    // Keep the bootstrap endpoint first for onboarding reads. Enrollment endpoints retain
    // realm reachability after it disappears and before discovery completes.
    peer_endpoints.extend(enrollment_endpoints);

    Ok((
        Config {
            storage_path,
            metadata_storage_path,
            metadata_search_storage,
            fjall_persist_policy,
            document_sync_storage_path,
            blob_root,
            blob_backends,
            blob_bucket_prefix,
            blob_max_bucket_size,
            blob_multipart_bucket,
            blob_control_plane_connect_timeout_secs,
            blob_control_plane_io_timeout_secs,
            blob_transfer_idle_timeout_secs,
            onboarding_bootstrap_timeout_secs,
            onboarding_document_sync_timeout_secs,
            s3_initial_request_timeout_secs,
            s3_connection_idle_timeout_secs,
            s3_stream_lifetime_timeout_secs,
            http_socket_addr,
            ops_socket_addr,
            max_http_body_size,
            cors_allowed_origins,
            desktop_cors,
            mcp_enabled,
            portal_csp_extra_origins,
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
            api_public_url,
            s3_public_url,
            trusted_proxies,
            rocrate_limits,
            rate_limits,
            s3_address,
            onboarding_secret,
            oidc_providers,
            realm_description,
            portal,
            assistant_proxy,
            startup_mode,
            node_state,
            node_labels,
            node_location,
            node_weight,
        },
        storage_handle,
    ))
}

pub async fn load() -> Result<(Config, StorageHandle), SetupError> {
    resolve_settings(crate::settings::read_settings()?).await
}

pub async fn mark_state_complete(
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

/// Only a device may run without an S3 listener. An infrastructure node whose
/// `S3_HOST`/`S3_ADDRESS` pair is accidentally absent must fail its start
/// instead of coming up silently serving no S3 endpoint at all.
fn validate_s3_profile(
    s3_address: Option<&str>,
    capabilities: &NodeCapabilities,
) -> Result<(), SetupError> {
    if s3_address.is_some() || matches!(capabilities, NodeCapabilities::User { .. }) {
        return Ok(());
    }
    Err(SetupError::MissingConfigValue("S3_ADDRESS"))
}

/// Refuses a device profile whose wipe would take more than this node's own
/// storage: the filesystem root, the owner's home, or the parent of another
/// root is a configuration mistake this node must not start with.
pub fn validate_wipe_roots(
    roots: &[std::path::PathBuf],
    home: Option<&std::path::Path>,
) -> Result<Vec<std::path::PathBuf>, SetupError> {
    // Without a home to compare against, no root can be shown to be safe.
    let home = normalize_root(home.ok_or(SetupError::MissingConfigValue("HOME"))?);
    let roots: Vec<std::path::PathBuf> = roots.iter().map(|root| normalize_root(root)).collect();
    for root in &roots {
        let refuse = |message: &str| SetupError::InvalidConfigValue {
            key: "STORAGE_PATH",
            value: root.display().to_string(),
            message: message.to_string(),
        };
        if root.parent().is_none() {
            return Err(refuse("a wipe root may not be the filesystem root"));
        }
        if home.starts_with(root) {
            return Err(refuse("a wipe root may not contain a home directory"));
        }
        if roots
            .iter()
            .any(|other| other != root && other.starts_with(root))
        {
            return Err(refuse("a wipe root may not contain another wipe root"));
        }
    }
    Ok(roots)
}

/// Drops every root that lies inside another: erasing the outer one takes it
/// along, and the default layout keeps the blob store under the storage path.
pub fn outermost_roots(roots: &[std::path::PathBuf]) -> Vec<std::path::PathBuf> {
    let normalized: Vec<std::path::PathBuf> =
        roots.iter().map(|root| normalize_root(root)).collect();
    let mut kept: Vec<std::path::PathBuf> = normalized
        .iter()
        .filter(|root| {
            !normalized
                .iter()
                .any(|other| other != *root && root.starts_with(other))
        })
        .cloned()
        .collect();
    kept.sort();
    kept.dedup();
    kept
}

/// The path a wipe would really erase: absolute, and resolved where it exists,
/// so a relative root or a link cannot hide what it covers.
fn normalize_root(path: &std::path::Path) -> std::path::PathBuf {
    let absolute = std::path::absolute(path).unwrap_or_else(|_| path.to_path_buf());
    // POSIX absolute paths retain `..`, and nonexistent paths cannot be canonicalized.
    // Fold components first so a root cannot hide the parent it would erase.
    let folded = fold_components(&absolute);
    std::fs::canonicalize(&folded).unwrap_or(folded)
}

/// Resolves `.` and `..` lexically. It is not link-aware, which is why the
/// canonical form still wins wherever the path exists.
fn fold_components(path: &std::path::Path) -> std::path::PathBuf {
    let mut folded = std::path::PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                if !folded.pop() {
                    folded.push(component.as_os_str());
                }
            }
            other => folded.push(other.as_os_str()),
        }
    }
    folded
}

pub(crate) fn node_capabilities(
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
        PersistedNodeIdentity::User { .. } => Ok((
            node_state.realm_id,
            NodeCapabilities::user_node(node_state.realm_id)?,
        )),
    }
}

pub(crate) fn generate_node_state() -> Result<PersistedNodeState, SetupError> {
    let realm_signing_key = generate_signing_key();
    let node_signing_key = generate_signing_key();

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
    realm_endpoints: Vec<EndpointAddr>,
}

/// Realm endpoints handed over at enrollment, so a joiner dials the realm
/// without a discovery read of its own. An entry that does not parse, or whose
/// address names another node, is dropped instead of failing the join.
pub(crate) fn onboarding_realm_endpoints(endpoints: &[StaticRealmEndpoint]) -> Vec<EndpointAddr> {
    endpoints
        .iter()
        .filter_map(
            |endpoint| match parse_endpoint_config(&endpoint.endpoint_addr) {
                Ok(endpoint_addr) if endpoint_addr.id.to_string() == endpoint.node_id => {
                    Some(endpoint_addr)
                }
                Ok(_) => {
                    tracing::warn!(
                        node_id = %endpoint.node_id,
                        "Enrollment endpoint address names a different node; ignoring it"
                    );
                    None
                }
                Err(message) => {
                    tracing::warn!(
                        node_id = %endpoint.node_id,
                        %message,
                        "Enrollment handed over an unusable realm endpoint; ignoring it"
                    );
                    None
                }
            },
        )
        .collect()
}

/// The decode half of the enrollment client, separated so response mapping is
/// testable without an HTTP server.
fn decode_bootstrap_response(body: &[u8]) -> Result<BootstrapOnboardingResponse, SetupError> {
    serde_json::from_slice(body).map_err(|error| {
        SetupError::OnboardingBootstrapFailed(format!("invalid bootstrap response: {error}"))
    })
}

async fn bootstrap_node_state(
    onboarding_secret: &str,
    node_location: Option<String>,
    node_weight: Option<u32>,
    node_labels: BTreeMap<String, String>,
    timeout: Duration,
) -> Result<BootstrappedNodeState, SetupError> {
    let decoded_secret = OnboardingSecret::decode(onboarding_secret)?;
    let node_signing_key = generate_signing_key();
    let net_secret_key = node_signing_key.to_bytes();
    let node_id = iroh::SecretKey::from_bytes(&net_secret_key).public();

    let issuer_signing_key = if matches!(decoded_secret.mode, OnboardingMode::Server) {
        Some(generate_signing_key())
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
        .sign(&node_proof_message(
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
                .sign(&issuer_proof_message(
                    onboarding_secret,
                    &node_id_string,
                    issuer_public_key,
                ))
                .to_string()
        });

    let response = onboarding_bootstrap_client(timeout)?
        .post(format!(
            "{}/api/v1/access/onboarding/bootstrap",
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

    let body = response.bytes().await?;
    let response = decode_bootstrap_response(&body)?;
    if response.mode != decoded_secret.mode {
        return Err(SetupError::OnboardingModeMismatch);
    }
    let realm_id = response.realm_id()?;
    validate_bootstrap_response(&response, decoded_secret.mode, realm_id, node_id)?;
    let temporary_bootstrap_endpoint = response.temporary_bootstrap_endpoint.clone();
    let realm_endpoints = onboarding_realm_endpoints(&response.realm_endpoints);
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
            // Devices carry no issuer keys. Their finalized membership grants owner authority,
            // and the saved owner identifies that authority before the record arrives.
            OnboardingMode::User { owner } => PersistedNodeIdentity::User { owner },
        };

    Ok(BootstrappedNodeState {
        temporary_bootstrap_endpoint,
        realm_endpoints,
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
    timeout: Duration,
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
        .sign(&node_proof_message(
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
                .sign(&issuer_proof_message(
                    onboarding_secret,
                    &node_id_string,
                    issuer_public_key,
                ))
                .to_string()
        });

    let response = onboarding_bootstrap_client(timeout)?
        .post(format!(
            "{}/api/v1/access/onboarding/bootstrap",
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

    let body = response.bytes().await?;
    let response = decode_bootstrap_response(&body)?;
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

fn onboarding_bootstrap_client(timeout: Duration) -> Result<reqwest::Client, SetupError> {
    Ok(reqwest::Client::builder()
        .connect_timeout(ONBOARDING_BOOTSTRAP_HTTP_CONNECT_TIMEOUT)
        .timeout(timeout)
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

async fn load_realm_config(
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
                let endpoint_addr =
                    parse_endpoint_config(&endpoint.endpoint_addr).map_err(|message| {
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
                            dns_discovery_method(origins)?
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
                    relay_method = policy_relay_method(relay_policy)?;
                    break;
                }
            }
            Ok((nodes, Vec::new(), discovery_method, relay_method))
        }
    }
}

fn dns_discovery_method(origins: &[String]) -> Result<DiscoveryMethod, SetupError> {
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

fn policy_relay_method(policy: &RelayPolicy) -> Result<RelayMethod, SetupError> {
    match policy {
        RelayPolicy::Disabled => Ok(RelayMethod::None),
        RelayPolicy::Default => Ok(RelayMethod::N0),
        RelayPolicy::Custom { relays } => {
            validate_relay_urls("realm_relay", relays)?;
            Ok(RelayMethod::Custom(relays.clone()))
        }
    }
}

pub(crate) async fn load_node_state(
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

pub(crate) async fn persist_node_state(
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
        BootOrigin, PersistedNodeIdentity, PersistedNodeState, PersistedNodeStatus, SetupError,
        decode_bootstrap_response, normalize_root, outermost_roots, persist_node_state,
        resolve_settings, validate_s3_profile, validate_wipe_roots,
    };
    use crate::settings::read_settings_from;
    use aruna_core::keys::generate_signing_key;
    use aruna_core::structs::{
        DynamicDiscoveryMethod, NodeCapabilities, RealmConfigDocument, RealmDiscoveryConfig,
        RealmId, RelayPolicy, StaticRealmEndpoint,
    };
    use aruna_net::{RelayMethod, format_endpoint_config};
    use aruna_storage::FjallStorage;
    use std::collections::BTreeMap;
    use tempfile::tempdir;

    /// Operator inputs for resolution tests: a minimal valid environment plus
    /// the case's overrides.
    fn settings_env(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        let mut env = BTreeMap::from([
            ("STORAGE_PATH".to_string(), "/tmp/aruna-none".to_string()),
            ("SOCKET_ADDRESS".to_string(), "127.0.0.1:0".to_string()),
            ("S3_HOST".to_string(), "127.0.0.1:0".to_string()),
            ("S3_ADDRESS".to_string(), "127.0.0.1:0".to_string()),
            ("PORTAL_MODE".to_string(), "disabled".to_string()),
            ("ARUNA_FJALL_PERSIST_MODE".to_string(), "buffer".to_string()),
        ]);
        for (key, value) in pairs {
            env.insert((*key).to_string(), (*value).to_string());
        }
        env
    }

    // Wipe roots cannot contain the owner's home, another root, or the filesystem.
    // These paths deliberately do not exist because safety must not depend on presence.
    #[test]
    fn folds_nested_roots() {
        // The blob store defaults to a directory under the storage path; the
        // wipe visits the storage path once and never lists the nested one.
        let home = std::env::temp_dir().join("aruna-wipe-home");
        let store = home.join("data");
        let blobs = store.join("blobstore");
        let folded = outermost_roots(&[blobs.clone(), store.clone(), store.clone()]);
        assert_eq!(folded, vec![normalize_root(&store)]);
        assert!(validate_wipe_roots(&folded, Some(&home)).is_ok());
    }

    #[test]
    fn refuses_unsafe_roots() {
        let home = std::path::PathBuf::from("/nonexistent-aruna-test/ada");
        let store = home.join(".aruna/store");
        let blobs = home.join(".aruna/blobs");
        assert_eq!(
            validate_wipe_roots(&[store.clone(), blobs.clone()], Some(&home)).unwrap(),
            vec![store.clone(), blobs.clone()]
        );

        for unsafe_root in [
            std::path::PathBuf::from("/"),
            home.clone(),
            std::path::PathBuf::from("/nonexistent-aruna-test"),
            home.join(".aruna"),
            // The same directories, reached through paths that hide them.
            home.join(".aruna/store/../.."),
            home.join("./.aruna/./store/../../"),
        ] {
            assert!(
                validate_wipe_roots(
                    &[store.clone(), blobs.clone(), unsafe_root.clone()],
                    Some(&home)
                )
                .is_err(),
                "{} must be refused",
                unsafe_root.display()
            );
        }
        // A device that cannot say where its owner's home is cannot be judged.
        assert!(matches!(
            validate_wipe_roots(&[store], None),
            Err(SetupError::MissingConfigValue("HOME"))
        ));
    }

    // Only a device serves no S3 endpoint. An infrastructure node whose pair is
    // accidentally absent must fail its start instead of coming up without one.
    #[test]
    fn devices_skip_s3() {
        // A realm id is a verifying key, so it has to be one a device can hold.
        let realm_id = RealmId::from_bytes(generate_signing_key().verifying_key().to_bytes());
        let device = NodeCapabilities::user_node(realm_id).expect("device capabilities");
        let management =
            NodeCapabilities::management_node(generate_signing_key()).expect("management");

        assert!(validate_s3_profile(None, &device).is_ok());
        assert!(validate_s3_profile(Some("0.0.0.0:9000"), &management).is_ok());
        assert!(matches!(
            validate_s3_profile(None, &management),
            Err(SetupError::MissingConfigValue("S3_ADDRESS"))
        ));
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
    fn discovery_rejects_origins() {
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
    fn endpoint_validates_node() {
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
                endpoint_addr: format_endpoint_config(&endpoint_addr),
            }],
        };

        assert!(super::realm_network_config(Some(&realm_config), local_node_id).is_err());
    }

    #[test]
    fn filters_enrollment_endpoints() {
        // A malformed or mismatched hand-over entry is dropped, never fatal:
        // the joiner still dials the endpoints that do parse.
        let endpoint_node = iroh::SecretKey::from_bytes(&[51u8; 32]).public();
        let other_node = iroh::SecretKey::from_bytes(&[52u8; 32]).public();
        let endpoint_addr =
            iroh::EndpointAddr::new(endpoint_node).with_ip_addr("127.0.0.1:3001".parse().unwrap());
        let endpoints = vec![
            StaticRealmEndpoint {
                node_id: endpoint_node.to_string(),
                endpoint_addr: format_endpoint_config(&endpoint_addr),
            },
            StaticRealmEndpoint {
                node_id: other_node.to_string(),
                endpoint_addr: format_endpoint_config(&endpoint_addr),
            },
            StaticRealmEndpoint {
                node_id: endpoint_node.to_string(),
                endpoint_addr: "not-an-endpoint".to_string(),
            },
        ];

        let parsed = super::onboarding_realm_endpoints(&endpoints);
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].id, endpoint_node);
    }

    #[tokio::test]
    async fn extra_relays() {
        let env = settings_env(&[(
            "P2P_ADDITIONAL_RELAY_URLS",
            "https://relay-a.example, https://relay-b.example",
        )]);

        let (config, _storage) = resolve_settings(read_settings_from(&env).unwrap())
            .await
            .unwrap();

        assert_eq!(
            config.relay_method,
            RelayMethod::N0WithCustom(vec![
                "https://relay-a.example".to_string(),
                "https://relay-b.example".to_string(),
            ])
        );
    }

    #[tokio::test]
    async fn existing_ignores_secret() {
        let tempdir = tempdir().unwrap();
        let storage = FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();

        let realm_signing_key = generate_signing_key();
        let net_signing_key = generate_signing_key();
        let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
        let node_state = PersistedNodeState {
            boot_origin: BootOrigin::Onboarded,
            status: PersistedNodeStatus::Complete,
            realm_id,
            net_secret_key: net_signing_key.to_bytes(),
            onboarding_phase: None,
            onboarding_sync_ticket: None,
            identity: PersistedNodeIdentity::User {
                owner: aruna_core::types::UserId::nil(realm_id),
            },
        };
        persist_node_state(&storage, &node_state).await.unwrap();
        drop(storage);

        let env = settings_env(&[
            ("STORAGE_PATH", tempdir.path().to_str().unwrap()),
            ("ONBOARDING_SECRET", "definitely-not-a-valid-secret"),
        ]);

        let (config, _storage) = resolve_settings(read_settings_from(&env).unwrap())
            .await
            .unwrap();
        assert!(!config.is_initial_node());
        assert!(matches!(
            config.startup_mode,
            super::StartupMode::Provisioned
        ));
    }

    #[tokio::test]
    async fn pending_ignores_secret() {
        let tempdir = tempdir().unwrap();
        let storage = FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();

        let realm_signing_key = generate_signing_key();
        let net_signing_key = generate_signing_key();
        let realm_id = RealmId::from_bytes(realm_signing_key.verifying_key().to_bytes());
        let node_state = PersistedNodeState {
            boot_origin: BootOrigin::Onboarded,
            status: PersistedNodeStatus::PendingOnboarding,
            realm_id,
            net_secret_key: net_signing_key.to_bytes(),
            onboarding_phase: Some(aruna_core::onboarding::OnboardingPhase::CoreDocumentsFetched),
            onboarding_sync_ticket: Some("already-fetched".to_string()),
            identity: PersistedNodeIdentity::User {
                owner: aruna_core::types::UserId::nil(realm_id),
            },
        };
        persist_node_state(&storage, &node_state).await.unwrap();
        drop(storage);

        // No ONBOARDING_SECRET: a phase past the bootstrap refresh must not
        // require one.
        let env = settings_env(&[("STORAGE_PATH", tempdir.path().to_str().unwrap())]);

        let (config, _storage) = resolve_settings(read_settings_from(&env).unwrap())
            .await
            .unwrap();
        assert!(matches!(
            config.startup_mode,
            super::StartupMode::JoinRealm {
                phase: aruna_core::onboarding::OnboardingPhase::CoreDocumentsFetched
            }
        ));
        assert!(!config.temporary_bootstrap_active);
    }
    #[test]
    fn bootstrap_response_decode_is_a_boundary() {
        assert!(matches!(
            decode_bootstrap_response(b"not json"),
            Err(SetupError::OnboardingBootstrapFailed(_))
        ));
    }

    #[tokio::test]
    async fn pending_initialization_restarts_as_initialize_realm() {
        // An incomplete first startup must resume realm initialization rather
        // than mint a second identity.
        let tempdir = tempdir().unwrap();
        let storage = FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        let store = crate::identity::IdentityStore::open(storage);
        let generated = store.generate().unwrap();
        assert!(matches!(
            generated.status,
            PersistedNodeStatus::PendingInitialization
        ));
        store.persist(&generated).await.unwrap();
        drop(store);

        let env = settings_env(&[("STORAGE_PATH", tempdir.path().to_str().unwrap())]);
        let (config, _storage) = resolve_settings(read_settings_from(&env).unwrap())
            .await
            .unwrap();
        assert!(matches!(
            config.startup_mode,
            super::StartupMode::InitializeRealm { .. }
        ));
        assert_eq!(config.node_state, generated);
    }
}
