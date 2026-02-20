use aruna_core::errors::ConversionError;
use aruna_core::structs::{NodeCapabilities, RealmId};
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use ed25519_dalek::pkcs8::{DecodePrivateKey, DecodePublicKey};
use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
use ed25519_dalek::{SigningKey, VerifyingKey};
use std::array::TryFromSliceError;
use std::net::SocketAddr;
use std::str::FromStr;
use thiserror::Error;

pub struct Config {
    pub storage_path: String,
    pub socket_addr: SocketAddr,
    pub node_capabilities: NodeCapabilities,
    pub realm_id: RealmId,
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
}

pub fn read_config() -> Result<Config, SetupError> {
    let storage_path = dotenvy::var("STORAGE_PATH")?;
    let socket_addr = SocketAddr::from_str(&dotenvy::var("SOCKET_ADDRESS")?)?;
    let pubkey = dotenvy::var("REALM_PUBLIC_KEY")?;
    let privkey = dotenvy::var("REALM_PRIVATE_KEY")?;

    let realm_key = VerifyingKey::from_public_key_pem(&pubkey)?;
    let realm_id = RealmId::from_bytes(*realm_key.as_bytes());
    let realm_verifying_key = realm_key
        .to_public_key_pem(LineEnding::default())?
        .as_bytes()
        .try_into()?;

    let realm_signing_key = SigningKey::from_pkcs8_pem(&privkey)?;
    let realm_encoding_key = realm_signing_key
        .to_pkcs8_pem(LineEnding::default())
        .unwrap()
        .as_bytes()
        .try_into()?;

    let node_capabilities = NodeCapabilities::Management {
        realm_signing_key,
        realm_verifying_key,
        realm_encoding_key,
    };

    Ok(Config {
        storage_path,
        socket_addr,
        node_capabilities,
        realm_id,
    })
}
