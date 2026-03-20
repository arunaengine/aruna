use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::NODE_STATE_KEYSPACE;
use aruna_core::structs::{NodeCapabilities, RealmId};
use aruna_storage::{FjallStorage, StorageHandle, errors::StorageLibError};
use byteview::ByteView;
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use ed25519_dalek::pkcs8::{DecodePrivateKey, EncodePrivateKey, EncodePublicKey};
use ed25519_dalek::SigningKey;
use iroh::KeyParsingError;
use serde::{Deserialize, Serialize};
use std::array::TryFromSliceError;
use std::net::SocketAddr;
use std::num::ParseIntError;
use std::str::FromStr;
use thiserror::Error;

const NODE_STATE_RECORD_KEY: &[u8] = b"node_state";

pub struct Config {
    pub storage_path: String,
    pub blob_root: String,
    pub blob_bucket_prefix: Option<String>,
    pub blob_max_bucket_size: Option<u64>,
    pub http_socket_addr: SocketAddr,
    pub p2p_socket_addr: SocketAddr,
    pub node_capabilities: NodeCapabilities,
    pub realm_id: RealmId,
    pub node_id: iroh::PublicKey,
    pub net_secret_key: iroh::SecretKey,
    pub bootstrap_nodes: Vec<iroh::PublicKey>,
    pub default_metadata_replication_factor: u32,
    pub s3_port: u16,
    pub s3_host: String,
    pub s3_address: String,
    pub onboarding_secret: Option<String>,
    pub startup_mode: StartupMode,
    pub node_state: PersistedNodeState,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StartupMode {
    InitializeRealm { realm_description: String },
    Provisioned,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BootOrigin {
    InitializedRealm,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PersistedNodeStatus {
    PendingInitialization,
    Complete,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PersistedNodeIdentity {
    Management { realm_private_key_pem: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedNodeState {
    pub boot_origin: BootOrigin,
    pub status: PersistedNodeStatus,
    pub realm_id: RealmId,
    pub net_secret_key: [u8; 32],
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
    #[error("onboarding bootstrap is not implemented yet")]
    OnboardingBootstrapNotImplemented,
    #[error("unexpected storage event while loading node state: {0}")]
    UnexpectedStorageEvent(String),
}

impl Config {
    pub fn is_initial_node(&self) -> bool {
        matches!(self.node_state.boot_origin, BootOrigin::InitializedRealm)
    }
}

pub async fn load() -> Result<(Config, StorageHandle), SetupError> {
    let storage_path = dotenvy::var("STORAGE_PATH")?;
    let blob_root =
        dotenvy::var("BLOB_ROOT").unwrap_or_else(|_| format!("{storage_path}/blobstore"));
    let blob_bucket_prefix = dotenvy::var("BLOB_BUCKET_PREFIX").ok();
    let blob_max_bucket_size = dotenvy::var("BLOB_MAX_BUCKET_SIZE")
        .ok()
        .map(|value| value.parse::<u64>())
        .transpose()?
        .or(Some(100_000));
    let http_socket_addr = SocketAddr::from_str(&dotenvy::var("SOCKET_ADDRESS")?)?;
    let p2p_socket_addr = SocketAddr::from_str(
        &dotenvy::var("P2P_SOCKET_ADDRESS").unwrap_or_else(|_| http_socket_addr.to_string()),
    )?;
    let bootstrap_nodes = dotenvy::var("BOOTSTRAP_NODES")
        .ok()
        .map(|nodes| {
            nodes
                .split(',')
                .map(str::trim)
                .filter(|node| !node.is_empty())
                .map(iroh::PublicKey::from_str)
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?
        .unwrap_or_default();
    let default_metadata_replication_factor = dotenvy::var("METADATA_REPLICATION_FACTOR")
        .ok()
        .map(|value| value.parse::<u32>())
        .transpose()?
        .unwrap_or(3)
        .max(1);
    let s3_port = dotenvy::var("S3_PORT")?.parse::<u16>()?;
    let s3_host = dotenvy::var("S3_HOST")?;
    let s3_address = dotenvy::var("S3_ADDRESS")?;
    let realm_description = dotenvy::var("REALM_DESCRIPTION")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "Aruna Realm".to_string());
    let onboarding_secret = dotenvy::var("ONBOARDING_SECRET")
        .ok()
        .filter(|value| !value.trim().is_empty());

    let storage_handle = FjallStorage::open(&storage_path)?;
    let node_state = match load_persisted_node_state(&storage_handle).await? {
        Some(state) => state,
        None => {
            if onboarding_secret.is_some() {
                return Err(SetupError::OnboardingBootstrapNotImplemented);
            }

            let state = generate_initialized_node_state()?;
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

    let startup_mode = match node_state.status {
        PersistedNodeStatus::PendingInitialization => StartupMode::InitializeRealm {
            realm_description: realm_description.clone(),
        },
        PersistedNodeStatus::Complete => StartupMode::Provisioned,
    };

    Ok((
        Config {
            storage_path,
            blob_root,
            blob_bucket_prefix,
            blob_max_bucket_size,
            http_socket_addr,
            p2p_socket_addr,
            node_capabilities,
            realm_id,
            node_id,
            net_secret_key,
            bootstrap_nodes,
            default_metadata_replication_factor,
            s3_port,
            s3_host,
            s3_address,
            onboarding_secret,
            startup_mode,
            node_state,
        },
        storage_handle,
    ))
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
        identity: PersistedNodeIdentity::Management {
            realm_private_key_pem: realm_signing_key
                .to_pkcs8_pem(LineEnding::default())?
                .to_string(),
        },
    })
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
        }) => Ok(Some(postcard::from_bytes(&bytes).map_err(ConversionError::from)?)),
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
