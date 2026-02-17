use aruna_api::server::{Server, ServerConfig};
use aruna_api::server_state::ServerState;
use aruna_core::errors::ParseRealmIdError;
use aruna_core::structs::RealmId;
use aruna_operations::driver::DriverContext;
use aruna_storage::storage;
use ed25519_dalek::pkcs8::{DecodePrivateKey, DecodePublicKey};
use ed25519_dalek::{SigningKey, VerifyingKey};
use std::array::TryFromSliceError;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use thiserror::Error;

#[tokio::main]
async fn main() {
    let config = read_config().unwrap();
    let storage_handle = storage::FjallStorage::open(&config.storage_path).unwrap();

    let driver_ctx = Arc::new(DriverContext {
        storage_handle,
        net_handle: None,
    });
    let state = Arc::new(ServerState::new(
        driver_ctx,
        Some(config.realm_keypair),
        Some(config.realm_id),
        None,
    ));

    let config = ServerConfig {
        http_addr: config.socket_addr,
    };
    let server = Server::new(state, config);
    if let Err(err) = server.run().await {
        eprintln!("{}", err);
    }
}

struct Config {
    storage_path: String,
    socket_addr: SocketAddr,
    realm_keypair: [u8; 64],
    realm_id: RealmId,
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

fn read_config() -> Result<Config, SetupError> {
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
