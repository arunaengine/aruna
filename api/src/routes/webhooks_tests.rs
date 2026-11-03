//! Tests that webhook routes keep the subscription and secret to their own caller.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::tests::routes::{test_context, test_state, test_storage};
use aruna_core::UserId;
use aruna_core::structs::identity::auth::{NodeCapabilities, PathRestriction, Permission};
use aruna_core::structs::identity::realm::RealmId;
use ulid::Ulid;

#[tokio::test]
async fn secret_shown_once() {
    let (_dir, storage) = test_storage();
    let realm_id = RealmId([3u8; 32]);
    let node_id = iroh::SecretKey::from_bytes(&[19u8; 32]).public();
    let state = Arc::new(
        test_state(
            Arc::new(test_context(storage)),
            realm_id,
            node_id,
            NodeCapabilities::user_node(realm_id).unwrap(),
        )
        .await,
    );
    let auth = |user: UserId| AuthContext {
        user_id: user,
        realm_id,
        path_restrictions: None,
        session: None,
    };
    let owner = auth(UserId::local(Ulid::generate(), realm_id));
    let other = auth(UserId::local(Ulid::generate(), realm_id));
    let url = "https://hooks.example.org/aruna".to_string();

    let Json(created) = set_webhook(
        State(state.clone()),
        Extension(Some(owner.clone())),
        Json(WebhookRequest { url: url.clone() }),
    )
    .await
    .unwrap();
    assert_eq!(created.event, "share.upload");
    assert!(created.secret.is_some());
    let Json(read) = get_webhook(State(state.clone()), Extension(Some(owner.clone())))
        .await
        .unwrap();
    assert_eq!(read.secret, None);
    assert_eq!(read.subscription_id, created.subscription_id);

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

    let removed = delete_webhook(State(state.clone()), Extension(Some(owner.clone()))).await;
    assert_eq!(removed.unwrap(), StatusCode::NO_CONTENT);
    let gone = get_webhook(State(state), Extension(Some(owner))).await;
    assert!(matches!(gone, Err(ServerError::NotFound)));
}
