//! Tests the metadata adapter record lookup and its realm and group permission gate.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

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

#[tokio::test]
async fn device_edit_owner() {
    // A device edits its replica without a holder's check, so only its unrestricted owner passes.
    use aruna_core::effects::StorageEffect;
    use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
    use aruna_core::structs::identity::auth::{PathRestriction, Permission};
    use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmNodeKind};
    let (_storage_dir, storage_handle) = test_storage();
    let realm_id = test_realm_id();
    let node_id = iroh::SecretKey::from_bytes(&[12u8; 32]).public();
    let owner = aruna_core::UserId::local(Ulid::generate(), realm_id);
    let actor = Actor {
        node_id,
        user_id: owner,
        realm_id,
    };
    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config.ensure_node(node_id, RealmNodeKind::User { owner });
    storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key: realm_id.as_bytes().to_vec().into(),
            value: config.to_bytes(&actor).unwrap().into(),
            txn_id: None,
        })
        .await;
    let state = test_state(
        Arc::new(test_context(storage_handle)),
        realm_id,
        node_id,
        NodeCapabilities::user_node(realm_id).unwrap(),
    )
    .await;
    let auth = AuthContext {
        user_id: owner,
        realm_id,
        path_restrictions: None,
        session: None,
    };
    let document_id = Ulid::generate();
    let extras = PolicyRequestExtras::rest;

    // The owner passes the gate and only then meets the missing placement.
    let owned = local_write_record(&state, &auth, document_id, extras()).await;
    assert!(
        matches!(owned, Err(ServerError::ServiceUnavailable)),
        "{owned:?}"
    );
    let restricted = AuthContext {
        path_restrictions: Some(vec![PathRestriction {
            pattern: format!("/{realm_id}/g/{}/meta/**", Ulid::generate()),
            permission: Permission::WRITE,
        }]),
        ..auth.clone()
    };
    let refused = local_write_record(&state, &restricted, document_id, extras()).await;
    assert!(matches!(refused, Err(ServerError::Forbidden)));
    let stranger = AuthContext {
        user_id: aruna_core::UserId::local(Ulid::generate(), realm_id),
        ..auth
    };
    let refused = local_write_record(&state, &stranger, document_id, extras()).await;
    assert!(matches!(refused, Err(ServerError::Forbidden)));
}
