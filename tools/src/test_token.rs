use std::sync::Arc;
use aruna::config::read_config;
use aruna_core::structs::RealmId;
use aruna_operations::create_token::{CreateTokenConfig, CreateTokenOperation};
use aruna_operations::driver::{DriverContext, drive};
use aruna_storage::storage;
use ed25519_dalek::SigningKey;
use tempfile::tempdir;
use ulid::Ulid;

pub async fn create_token() -> String {
    let config = read_config().unwrap();
    let tempdir = tempdir().unwrap();

    let storage_handle = storage::FjallStorage::open(&tempdir.path().to_string_lossy()).unwrap();

    let driver_ctx = Arc::new(DriverContext {
        storage_handle,
        net_handle: None,
    });

    let token_config = CreateTokenConfig {
        time: chrono::Utc::now().timestamp() as u64,
        expiry: None,
        user_id: Ulid::new(),
        realm_id: config.realm_id.clone(),
        keypair: config.realm_keypair,
    };
    let token_operation = CreateTokenOperation::new(token_config.clone());
    let token = drive(token_operation, &driver_ctx).await.unwrap();
    token
}
