//! Tests that webhook routes show only the caller's confirmed subscription, never its secret,
//! and refuse writes without a management quorum.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::tests::routes::{test_context, test_state, test_storage};
use aruna_core::UserId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::handle::Handle;
use aruna_core::structs::execution::setting::SettingChannel;
use aruna_core::structs::identity::auth::{Actor, NodeCapabilities, PathRestriction, Permission};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId, RealmNodeKind};
use ed25519_dalek::SigningKey;
use ulid::Ulid;

#[tokio::test]
async fn get_hides_secret() {
    let (_dir, storage) = test_storage();
    let realm_key = SigningKey::from_bytes(&[5u8; 32]);
    let realm_id = RealmId::from_bytes(realm_key.verifying_key().to_bytes());
    let node_id = iroh::SecretKey::from_bytes(&[19u8; 32]).public();
    let context = Arc::new(test_context(storage));
    let capabilities = NodeCapabilities::management_node(realm_key).unwrap();
    let state = Arc::new(test_state(context.clone(), realm_id, node_id, capabilities).await);
    let auth = |user: UserId| AuthContext {
        user_id: user,
        realm_id,
        path_restrictions: None,
        session: None,
    };
    let owner = auth(UserId::local(Ulid::generate(), realm_id));
    let other = auth(UserId::local(Ulid::generate(), realm_id));
    let unreadable = get_webhook(State(state.clone()), Extension(Some(owner.clone()))).await;
    assert!(matches!(
        unreadable,
        Err(ServerError::ServiceUnavailableReason(_))
    ));

    let actor = Actor {
        node_id,
        user_id: owner.user_id,
        realm_id,
    };
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    config.ensure_node(node_id, RealmNodeKind::Management);
    let write = StorageEffect::Write {
        key_space: aruna_core::keyspaces::REALM_CONFIG_KEYSPACE.to_string(),
        key: realm_id.as_bytes().to_vec().into(),
        value: config.to_bytes(&actor).unwrap().into(),
        txn_id: None,
    };
    context
        .storage_handle
        .send_effect(Effect::Storage(write))
        .await;
    let subscription = WebhookSubscription {
        subscription_id: Ulid::from_parts(1, 1),
        user_id: owner.user_id,
        version: 1,
        url: Some("https://hooks.example.org/aruna".to_string()),
        sealed: Some(b"sealed-canary".to_vec()),
        created_at_ms: 1,
    };
    let target = SettingChannel::Webhook.target(owner.user_id, subscription.subscription_id);
    let write = StorageEffect::Write {
        key_space: target.storage_keyspace().to_string(),
        key: target.storage_key(),
        value: subscription.to_bytes().unwrap().into(),
        txn_id: None,
    };
    context
        .storage_handle
        .send_effect(Effect::Storage(write))
        .await;

    let Json(read) = get_webhook(State(state.clone()), Extension(Some(owner.clone())))
        .await
        .unwrap();
    assert_eq!(
        (read.secret.as_deref(), read.event.as_str()),
        (None, "share.upload")
    );
    assert_eq!(
        read.subscription_id,
        subscription.subscription_id.to_string()
    );
    assert!(!serde_json::to_string(&read).unwrap().contains("canary"));
    let foreign = get_webhook(State(state.clone()), Extension(Some(other))).await;
    assert!(matches!(foreign, Err(ServerError::NotFound)));
    let restricted = AuthContext {
        path_restrictions: Some(vec![PathRestriction {
            pattern: "/**".to_string(),
            permission: Permission::READ,
        }]),
        ..owner.clone()
    };
    let refused = get_webhook(State(state.clone()), Extension(Some(restricted))).await;
    assert!(matches!(refused, Err(ServerError::Forbidden)));
    let invalid = set_webhook(
        State(state.clone()),
        Extension(Some(owner.clone())),
        Json(WebhookRequest {
            url: "ftp://example.org".to_string(),
        }),
    )
    .await;
    assert!(matches!(invalid, Err(ServerError::BadRequestReason(_))));
    // This node is the only manager but holds no realm key in the test, so no quorum answers.
    let removed = delete_webhook(State(state.clone()), Extension(Some(owner.clone()))).await;
    assert!(matches!(
        removed,
        Err(ServerError::ServiceUnavailableReason(_))
    ));
}

#[tokio::test]
async fn device_node_refuses() {
    let (_dir, storage) = test_storage();
    let realm_id = RealmId([3u8; 32]);
    let node_id = iroh::SecretKey::from_bytes(&[20u8; 32]).public();
    let capabilities = NodeCapabilities::user_node(realm_id).unwrap();
    let context = Arc::new(test_context(storage));
    let state = Arc::new(test_state(context, realm_id, node_id, capabilities).await);
    let owner = Some(AuthContext {
        user_id: UserId::local(Ulid::generate(), realm_id),
        realm_id,
        path_restrictions: None,
        session: None,
    });
    let read = get_webhook(State(state.clone()), Extension(owner.clone())).await;
    assert!(matches!(read, Err(ServerError::Conflict(_))));
    let request = Json(WebhookRequest {
        url: "https://hooks.example.org/aruna".to_string(),
    });
    let set = set_webhook(State(state.clone()), Extension(owner.clone()), request).await;
    assert!(matches!(set, Err(ServerError::Conflict(_))));
    let removed = delete_webhook(State(state), Extension(owner)).await;
    assert!(matches!(removed, Err(ServerError::Conflict(_))));
}
