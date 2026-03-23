use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::NODE_STATE_KEYSPACE;
use aruna_core::onboarding::{
    BootstrapOnboardingRequest, BootstrapOnboardingResponse, OnboardingMode, OnboardingSecret,
    OnboardingPhase, OnboardingSecretError,
    bootstrap_issuer_proof_message, bootstrap_node_proof_message,
};
use aruna_core::structs::{NodeCapabilities, RealmId};
use aruna_storage::{FjallStorage, StorageHandle, errors::StorageLibError};
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
    pub bootstrap_endpoints: Vec<EndpointAddr>,
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
    Management { realm_private_key_pem: String },
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
    pub bootstrap_endpoints: Vec<EndpointAddr>,
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
    #[error("onboarding mode mismatch between secret and bootstrap response")]
    OnboardingModeMismatch,
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
            let state = match onboarding_secret.as_deref() {
                Some(onboarding_secret) => bootstrap_onboarded_node_state(onboarding_secret).await?,
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
            bootstrap_endpoints: node_state.bootstrap_endpoints.clone(),
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
            node_state.realm_id.clone(),
            NodeCapabilities::server_node(
                SigningKey::from_pkcs8_pem(issuer_private_key_pem)?,
                node_state.realm_id.clone(),
                delegation_signature.clone(),
            )?,
        )),
        PersistedNodeIdentity::Local => Ok((
            node_state.realm_id.clone(),
            NodeCapabilities::local_node(node_state.realm_id.clone())?,
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
        bootstrap_endpoints: Vec::new(),
        onboarding_phase: None,
        onboarding_sync_ticket: None,
        identity: PersistedNodeIdentity::Management {
            realm_private_key_pem: realm_signing_key
                .to_pkcs8_pem(LineEnding::default())?
                .to_string(),
        },
    })
}

async fn bootstrap_onboarded_node_state(
    onboarding_secret: &str,
) -> Result<PersistedNodeState, SetupError> {
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

    let response = reqwest::Client::new()
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
    if response.bootstrap_endpoints.is_empty() {
        return Err(SetupError::OnboardingBootstrapFailed(
            "bootstrap endpoint list was empty".to_string(),
        ));
    }

    let realm_id = response.realm_id()?;
    let identity = match response.mode {
        OnboardingMode::Management => {
            let wrapped_key = response
                .wrapped_realm_private_key
                .ok_or(SetupError::MissingOnboardingMaterial(OnboardingMode::Management))?;
            let wrapped_nonce = response
                .wrapped_realm_private_key_nonce
                .ok_or(SetupError::MissingOnboardingMaterial(OnboardingMode::Management))?;
            let wrapping_public_key = response
                .wrapping_public_key
                .ok_or(SetupError::MissingOnboardingMaterial(OnboardingMode::Management))?;
            let transport_secret_key = transport_secret_key
                .ok_or(SetupError::MissingOnboardingMaterial(OnboardingMode::Management))?;
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
                <[u8; 24]>::try_from(nonce_bytes.as_slice()).map_err(SetupError::FromSliceError)?,
            );
            let ciphertext = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(wrapped_key)
                .map_err(SetupError::Base64Error)?;
            let realm_private_key_pem = String::from_utf8(
                cipher
                    .decrypt(&nonce, ciphertext.as_ref())
                    .map_err(|error| SetupError::OnboardingBootstrapFailed(error.to_string()))?,
            )?;

            PersistedNodeIdentity::Management {
                realm_private_key_pem,
            }
        }
        OnboardingMode::Server => PersistedNodeIdentity::Server {
            issuer_private_key_pem: issuer_signing_key
                .ok_or(SetupError::MissingOnboardingMaterial(OnboardingMode::Server))?
                .to_pkcs8_pem(LineEnding::default())?
                .to_string(),
            delegation_signature: response
                .delegation_signature
                .ok_or(SetupError::MissingOnboardingMaterial(OnboardingMode::Server))?,
        },
        OnboardingMode::Local => PersistedNodeIdentity::Local,
    };

    Ok(PersistedNodeState {
        boot_origin: BootOrigin::Onboarded,
        status: PersistedNodeStatus::PendingOnboarding,
        realm_id,
        net_secret_key,
        bootstrap_endpoints: response.bootstrap_endpoints,
        onboarding_phase: Some(OnboardingPhase::Bootstrapped),
        onboarding_sync_ticket: Some(response.onboarding_sync_ticket),
        identity,
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

#[cfg(test)]
mod tests {
    use super::{
        BootOrigin, PersistedNodeIdentity, PersistedNodeState, PersistedNodeStatus, load,
        persist_node_state,
    };
    use aruna_core::structs::RealmId;
    use aruna_storage::FjallStorage;
    use ed25519_dalek::SigningKey;
    use std::sync::{Mutex, OnceLock};
    use tempfile::tempdir;

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[tokio::test]
    async fn ignores_onboarding_secret_when_node_state_exists() {
        let _guard = env_lock().lock().unwrap();
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
            bootstrap_endpoints: Vec::new(),
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
            ("S3_PORT", "1337".to_string()),
            ("S3_HOST", "localhost".to_string()),
            ("S3_ADDRESS", "127.0.0.1".to_string()),
            ("ONBOARDING_SECRET", "definitely-not-a-valid-secret".to_string()),
        ];
        let previous: Vec<_> = vars
            .iter()
            .map(|(key, _)| ((*key).to_string(), std::env::var(key).ok()))
            .collect();

        for (key, value) in &vars {
            unsafe { std::env::set_var(key, value) };
        }

        let (config, _storage) = load().await.unwrap();
        assert!(!config.is_initial_node());
        assert!(matches!(config.startup_mode, super::StartupMode::Provisioned));

        for (key, value) in previous {
            match value {
                Some(value) => unsafe { std::env::set_var(key, value) },
                None => unsafe { std::env::remove_var(key) },
            }
        }
    }
}
