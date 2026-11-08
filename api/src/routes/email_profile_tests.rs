//! Tests who may read and change the realm email profile and that no password is ever shown.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::tests::routes::{test_context, test_state, test_storage};
use aruna_core::UserId;
use aruna_core::keys::generate_signing_key;
use aruna_core::structs::identity::realm::RealmId;
use aruna_operations::realm::claim_admin::{ClaimInitialInput, ClaimInitialOperation};
use aruna_operations::realm::create_realm::{CreateRealmConfig, CreateRealmOperation};
use tempfile::TempDir;

const PASSWORD: &str = "canary-route-77";

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

fn body(password: Option<&str>) -> Json<EmailProfileRequest> {
    Json(EmailProfileRequest {
        host: "smtp.example.org".to_string(),
        port: Some(587),
        tls: TlsMode::Starttls,
        username: Some("aruna".to_string()),
        password: password.map(str::to_string),
        from: "Aruna <aruna@example.org>".to_string(),
    })
}

#[tokio::test]
async fn admin_sets_profile() {
    let (_dir, state, admin) = setup_state().await;
    let member = UserId::local(Ulid::generate(), state.get_realm_id());
    let get = |user_id| get_email_profile(State(state.clone()), Extension(auth(&state, user_id)));
    let Json(empty) = get(member).await.unwrap();
    assert_eq!(
        empty,
        EmailProfileResponse {
            configured: false,
            profile: None
        }
    );

    let refused = set_email_profile(
        State(state.clone()),
        Extension(auth(&state, member)),
        body(Some(PASSWORD)),
    )
    .await;
    assert!(matches!(refused, Err(ServerError::Forbidden)));
    let Json(stored) = set_email_profile(
        State(state.clone()),
        Extension(auth(&state, admin)),
        body(Some(PASSWORD)),
    )
    .await
    .unwrap();
    assert!(stored.has_password);

    let Json(seen) = get(member).await.unwrap();
    assert_eq!(
        seen,
        EmailProfileResponse {
            configured: true,
            profile: None
        }
    );
    let Json(full) = get(admin).await.unwrap();
    assert_eq!(full.profile, Some(stored));
    assert!(!serde_json::to_string(&full).unwrap().contains(PASSWORD));

    let renamed = Json(EmailProfileRequest {
        username: Some("other".to_string()),
        ..body(None).0
    });
    let refused = set_email_profile(
        State(state.clone()),
        Extension(auth(&state, admin)),
        renamed,
    );
    assert!(matches!(
        refused.await,
        Err(ServerError::BadRequestReason(_))
    ));
    let bad_sender = Json(EmailProfileRequest {
        from: "not an address".to_string(),
        ..body(None).0
    });
    let refused = set_email_profile(
        State(state.clone()),
        Extension(auth(&state, admin)),
        bad_sender,
    );
    assert!(matches!(
        refused.await,
        Err(ServerError::BadRequestReason(_))
    ));

    let removed = delete_email_profile(State(state.clone()), Extension(auth(&state, admin))).await;
    assert_eq!(removed.unwrap(), StatusCode::NO_CONTENT);
    let Json(gone) = get(admin).await.unwrap();
    assert!(!gone.configured);
}
