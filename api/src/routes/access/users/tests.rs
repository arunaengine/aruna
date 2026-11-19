//! Tests user registration through OIDC and enrollment secrets, tokens, and profile reads.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::enrollment_status;
use aruna_core::UserId;
use aruna_core::handle::Handle;
use aruna_core::keys::generate_signing_key;
use aruna_core::structs::identity::auth::{Actor, NodeCapabilities, SessionKind};
use aruna_core::structs::identity::realm::RealmId;
use aruna_operations::driver::drive;
use aruna_operations::realm::create_realm::{CreateRealmConfig, CreateRealmOperation};
use axum::extract::State;
use axum::http::StatusCode;
use base64::Engine;
use ed25519_dalek::pkcs8::EncodePrivateKey;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use ulid::Ulid;

#[path = "merge_tests.rs"]
mod merge;

// An unclaimed enrollment past its expiry is dead: reporting it as pending
// would show an enrollment in flight that can never complete.
#[test]
fn reports_expired_enrollment() {
    assert_eq!(enrollment_status(false, 100, 99), "pending");
    assert_eq!(enrollment_status(false, 100, 100), "expired");
    assert_eq!(enrollment_status(false, 100, 101), "expired");
    assert_eq!(enrollment_status(true, 100, 101), "claimed");
}

#[tokio::test]
async fn federated_profile_synthesized() {
    // A federated session sees its full id and token name, and cannot renew its token.
    use aruna_core::structs::identity::auth::{AuthContext, SessionRef};
    use axum::Extension;
    use axum::response::IntoResponse;
    let (_dir, storage) = crate::tests::routes::test_storage();
    let context = Arc::new(aruna_operations::testing::test_context(storage));
    let signing_key = generate_signing_key();
    let realm_id = RealmId::from_bytes(signing_key.verifying_key().to_bytes());
    let node_id = iroh::SecretKey::generate().public();
    drive(
        CreateRealmOperation::new(CreateRealmConfig {
            actor: Actor {
                node_id,
                user_id: UserId::nil(realm_id),
                realm_id,
            },
            realm_description: "Realm".to_string(),
            oidc_providers: Vec::new(),
            node_location: None,
            node_weight: None,
            node_labels: Default::default(),
        }),
        &context,
    )
    .await
    .unwrap();
    let state = Arc::new(
        crate::tests::routes::test_state(
            context,
            realm_id,
            node_id,
            NodeCapabilities::management_node(signing_key).unwrap(),
        )
        .await,
    );
    let foreign = UserId::new(Ulid::generate(), RealmId::from_bytes([9; 32]));
    let auth = AuthContext {
        user_id: foreign,
        realm_id,
        path_restrictions: None,
        session: Some(SessionRef {
            sid: Ulid::generate().to_string(),
            kind: SessionKind::Federated,
            name: Some("Ada".to_string()),
            via: None,
        }),
    };
    let info = super::build_user_response(&state, auth.clone())
        .await
        .unwrap();
    assert_eq!(info.user.user_id, foreign.to_string());
    assert_eq!(info.user.name, "Ada");
    assert!(info.user.attributes.is_empty());
    let error = super::get_token(
        State(state),
        axum::http::HeaderMap::new(),
        Extension(Some(auth)),
        axum::extract::Query(super::TokenQuery {
            expires_in_seconds: None,
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(error.into_response().status(), StatusCode::FORBIDDEN);
}
