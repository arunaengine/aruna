//! Tests who may read and replace the realm portal feature defaults.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::tests::routes::{test_context, test_state, test_storage};
use aruna_core::UserId;
use aruna_core::keys::generate_signing_key;
use aruna_core::structs::identity::auth::NodeCapabilities;
use aruna_core::structs::identity::realm::RealmId;
use aruna_operations::realm::claim_admin::{ClaimInitialInput, ClaimInitialOperation};
use aruna_operations::realm::create_realm::{CreateRealmConfig, CreateRealmOperation};
use tempfile::TempDir;
use ulid::Ulid;

/// A management node state with one realm admin; returns the admin's id.
async fn setup_state() -> (TempDir, Arc<ServerState>, UserId) {
    let (dir, storage) = test_storage();
    let context = Arc::new(test_context(storage));
    let signing_key = generate_signing_key();
    let realm_id = RealmId::from_bytes(signing_key.verifying_key().to_bytes());
    let actor = Actor {
        node_id: iroh::SecretKey::generate().public(),
        user_id: UserId::local(Ulid::generate(), realm_id),
        realm_id,
    };
    drive(
        CreateRealmOperation::new(CreateRealmConfig {
            actor: actor.clone(),
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
    drive(
        ClaimInitialOperation::new(ClaimInitialInput {
            actor: actor.clone(),
        }),
        &context,
    )
    .await
    .unwrap();
    let capabilities = NodeCapabilities::management_node(signing_key).unwrap();
    let state = Arc::new(test_state(context, realm_id, actor.node_id, capabilities).await);
    (dir, state, actor.user_id)
}

fn auth(state: &ServerState, user_id: UserId) -> Option<AuthContext> {
    Some(AuthContext {
        user_id,
        realm_id: state.get_realm_id(),
        path_restrictions: None,
        session: None,
    })
}

fn body() -> Json<PortalFeatures> {
    Json(PortalFeatures {
        features: BTreeMap::from([("compute".to_string(), false)]),
    })
}

#[tokio::test]
async fn member_reads_defaults() {
    // Every portal needs the defaults, so a member without admin rights reads them.
    let (_dir, state, admin) = setup_state().await;
    let member = UserId::local(Ulid::generate(), state.get_realm_id());
    let Json(stored) =
        set_portal_features(State(state.clone()), Extension(auth(&state, admin)), body())
            .await
            .unwrap();
    assert_eq!(stored, body().0);

    let Json(read) = get_portal_features(State(state.clone()), Extension(auth(&state, member)))
        .await
        .unwrap();
    assert_eq!(read, body().0);

    let error = get_portal_features(State(state), Extension(None))
        .await
        .unwrap_err();
    assert!(matches!(error, ServerError::Unauthorized));
}

#[tokio::test]
async fn member_cannot_replace() {
    let (_dir, state, _admin) = setup_state().await;
    let member = UserId::local(Ulid::generate(), state.get_realm_id());
    let error = set_portal_features(
        State(state.clone()),
        Extension(auth(&state, member)),
        body(),
    )
    .await
    .unwrap_err();
    assert!(matches!(error, ServerError::Forbidden));

    let Json(read) = get_portal_features(State(state.clone()), Extension(auth(&state, member)))
        .await
        .unwrap();
    assert!(read.features.is_empty());
}

#[tokio::test]
async fn rejects_invalid_name() {
    let (_dir, state, admin) = setup_state().await;
    let invalid = Json(PortalFeatures {
        features: BTreeMap::from([("Compute".to_string(), false)]),
    });
    let error = set_portal_features(
        State(state.clone()),
        Extension(auth(&state, admin)),
        invalid,
    )
    .await
    .unwrap_err();
    assert!(matches!(error, ServerError::BadRequestReason(_)));
}
