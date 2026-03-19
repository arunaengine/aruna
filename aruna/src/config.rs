use aruna_core::errors::ConversionError;
use aruna_core::structs::{NodeCapabilities, RealmId};
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use ed25519_dalek::pkcs8::{DecodePrivateKey, DecodePublicKey};
use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
use ed25519_dalek::{SigningKey, VerifyingKey};
use iroh::KeyParsingError;
use std::array::TryFromSliceError;
use std::net::SocketAddr;
use std::num::ParseIntError;
use std::str::FromStr;
use thiserror::Error;

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
    #[error("REALM_PUBLIC_KEY does not match REALM_PRIVATE_KEY")]
    RealmKeyMismatch,
    #[error("NODE_PUBLIC_KEY does not match NODE_PRIVATE_KEY")]
    NodeKeyMismatch,
}

pub fn read_config() -> Result<Config, SetupError> {
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
    let realm_pubkey = dotenvy::var("REALM_PUBLIC_KEY")?;
    let realm_privkey = dotenvy::var("REALM_PRIVATE_KEY")?;
    let node_pubkey = dotenvy::var("NODE_PUBLIC_KEY")?;
    let node_privkey = dotenvy::var("NODE_PRIVATE_KEY")?;
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

    let realm_key = VerifyingKey::from_public_key_pem(&realm_pubkey)?;
    let realm_id = RealmId::from_bytes(*realm_key.as_bytes());
    let realm_verifying_key = realm_key
        .to_public_key_pem(LineEnding::default())?
        .as_bytes()
        .try_into()?;

    let realm_signing_key = SigningKey::from_pkcs8_pem(&realm_privkey)?;
    if realm_signing_key.verifying_key().to_bytes() != realm_key.to_bytes() {
        return Err(SetupError::RealmKeyMismatch);
    }
    let realm_encoding_key = realm_signing_key
        .to_pkcs8_pem(LineEnding::default())?
        .as_bytes()
        .try_into()?;

    let node_key = VerifyingKey::from_public_key_pem(&node_pubkey)?;
    let node_signing_key = SigningKey::from_pkcs8_pem(&node_privkey)?;
    let net_secret_key = iroh::SecretKey::from_bytes(&node_signing_key.to_bytes());
    let node_id = net_secret_key.public();
    if node_id.as_bytes() != node_key.to_bytes().as_slice() {
        return Err(SetupError::NodeKeyMismatch);
    }

    let s3_port = dotenvy::var("S3_PORT")?.parse::<u16>()?;
    let s3_host = dotenvy::var("S3_HOST")?;
    let s3_address = dotenvy::var("S3_ADDRESS")?;

    // TODO: Configure capabilities
    let node_capabilities = NodeCapabilities::Management {
        realm_signing_key,
        realm_verifying_key,
        realm_encoding_key,
    };

    Ok(Config {
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
    })
}
