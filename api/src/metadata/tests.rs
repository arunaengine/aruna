//! Adapter tests that need a storage handle: the read/lookup boundary and the
//! realm/group permission gate.

use super::*;
use crate::tests::routes::{seed_realm_auth, test_context, test_state, test_storage};
use aruna_core::structs::identity::auth::{Actor, NodeCapabilities};
use aruna_core::structs::identity::realm::RealmId;
use ed25519_dalek::SigningKey;
use std::sync::Arc;
use ulid::Ulid;

fn test_realm_id() -> RealmId {
    RealmId::from_bytes(
        SigningKey::from_bytes(&[3u8; 32])
            .verifying_key()
            .to_bytes(),
    )
}

#[tokio::test]
async fn storage_failure_internal() {
    let (storage_handle, receivers) = aruna_storage::storage::StorageHandle::new();
    drop(receivers);

    let realm_id = test_realm_id();
    let node_id = iroh::SecretKey::from_bytes(&[14u8; 32]).public();
    let state = test_state(
        Arc::new(test_context(storage_handle)),
        realm_id,
        node_id,
        NodeCapabilities::user_node(realm_id).unwrap(),
    )
    .await;

    let result = load_document_record(&state, Ulid::generate()).await;

    assert!(matches!(
        result,
        Err(ServerError::InternalError(message)) if message == "Channel closed"
    ));
}

#[tokio::test]
async fn missing_group_forbidden() {
    let (_storage_dir, storage_handle) = test_storage();
    let realm_id = test_realm_id();
    let node_id = iroh::SecretKey::from_bytes(&[11u8; 32]).public();
    let user_id = aruna_core::UserId::local(Ulid::generate(), realm_id);
    let actor = Actor {
        node_id,
        user_id,
        realm_id,
    };
    let driver_ctx = Arc::new(test_context(storage_handle));
    seed_realm_auth(&driver_ctx, realm_id, &actor).await;

    let state = test_state(
        driver_ctx,
        realm_id,
        node_id,
        NodeCapabilities::user_node(realm_id).unwrap(),
    )
    .await;
    let missing_group = Ulid::generate();
    let path = format!("/{realm_id}/g/{missing_group}/meta/**");

    let result = ensure_permission(
        &state,
        AuthContext {
            user_id,
            realm_id,
            path_restrictions: None,
            session: None,
        },
        path,
        Permission::WRITE,
    )
    .await;

    assert!(matches!(result, Err(ServerError::Forbidden)));
}
