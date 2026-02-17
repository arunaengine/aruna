use aruna_core::errors::ParseRealmIdError;
use aruna_core::structs::RealmId;
use ed25519_dalek::pkcs8::{DecodePrivateKey, DecodePublicKey};
use ed25519_dalek::{SigningKey, VerifyingKey};
use std::array::TryFromSliceError;
use std::net::SocketAddr;
use std::str::FromStr;
use thiserror::Error;

pub struct Config {
    pub storage_path: String,
    pub socket_addr: SocketAddr,
    pub realm_keypair: [u8; 64],
    pub realm_id: RealmId,
}

#[derive(Error, Debug)]
pub enum SetupError {
    #[error(transparent)]
    ConfigValueNotFound(#[from] dotenvy::Error),
    #[error(transparent)]
    SocketParsingError(#[from] std::net::AddrParseError),
    #[error(transparent)]
    KeyPairParsingError(#[from] ParseRealmIdError),
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

    let keypair = SigningKey::from_pkcs8_pem(&privkey)?;
    let realm_keypair = keypair.to_keypair_bytes();

    Ok(Config {
        storage_path,
        socket_addr,
        realm_keypair,
        realm_id,
    })
}
