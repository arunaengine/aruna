use crate::{lmdbstore::LmdbStore, CONFIG};
use crypto_kx::Keypair;
use s3s::{
    auth::{S3Auth, SecretKey},
    s3_error, S3Result,
};
use sha3::{Digest, Sha3_512};
use std::sync::Arc;
use tracing::trace;

pub struct AuthProvider {
    database: Arc<LmdbStore>,
}

impl AuthProvider {
    pub fn new(database: Arc<LmdbStore>) -> Self {
        Self { database }
    }
}

#[async_trait::async_trait]
impl S3Auth for AuthProvider {
    async fn get_secret_key(&self, access_key: &str) -> S3Result<SecretKey> {
        // Get shared secret for server and proxy
        Ok(SecretKey::from(get_shared_secret(access_key).ok_or_else(
            || s3_error!(AccessDenied, "Invalid access key"),
        )?))
    }
}

fn get_shared_secret(access_key: &str) -> Option<String> {
    // Server pubkey
    let server_pubkey =
        aruna_server::crypto::ed25519_to_x25519_pubkey(&CONFIG.proxy.server_pubkey).ok()?;
    // Proxy privkey
    let proxy_privkey =
        aruna_server::crypto::ed25519_to_x25519_privatekey(CONFIG.proxy.private_key.as_ref()?)
            .ok()?;

    // Calculate Proxy Keypair
    // TODO: This can be cached
    let proxy_secret_key = Keypair::from(crypto_kx::SecretKey::from(proxy_privkey));
    let server_pubkey = crypto_kx::PublicKey::from(server_pubkey);

    // Calculate SessionKey
    // Proxy must use session_keys_from .rx
    let key = proxy_secret_key.session_keys_from(&server_pubkey).rx;

    // Hash Key + Access Key
    let mut hasher = Sha3_512::new();
    hasher.update(key.as_ref());
    hasher.update(access_key.as_bytes());
    let result = Some(hex::encode(hasher.finalize()));
    trace!(?result);
    result
}