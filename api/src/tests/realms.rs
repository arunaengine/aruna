//! Builds management node states on a fresh realm, with a member or a claimed admin.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::server::state::ServerState;
use crate::tests::routes::{test_state, test_storage};
use aruna_core::UserId;
use aruna_core::keys::generate_signing_key;
use aruna_core::structs::identity::auth::{Actor, AuthContext, NodeCapabilities};
use aruna_core::structs::identity::realm::RealmId;
use aruna_operations::driver::drive;
use aruna_operations::realm::claim_admin::{ClaimInitialInput, ClaimInitialOperation};
use aruna_operations::realm::create_realm::{CreateRealmConfig, CreateRealmOperation};
use aruna_operations::testing::test_context;
use std::sync::Arc;
use tempfile::TempDir;
use ulid::Ulid;

/// A management node on a new realm and the auth of a member without any role.
pub(crate) async fn member_state() -> (TempDir, Arc<ServerState>, AuthContext) {
    let (dir, storage) = test_storage();
    let context = Arc::new(test_context(storage));
    let signing_key = generate_signing_key();
    let realm_id = RealmId::from_bytes(signing_key.verifying_key().to_bytes());
    let user_id = UserId::local(Ulid::generate(), realm_id);
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
        test_state(
            context,
            realm_id,
            node_id,
            NodeCapabilities::management_node(signing_key).unwrap(),
        )
        .await,
    );
    let auth = AuthContext {
        user_id,
        realm_id,
        path_restrictions: None,
        session: None,
    };
    (dir, state, auth)
}

/// A management node on a new realm whose initial admin is claimed, with that admin's id.
pub(crate) async fn admin_state() -> (TempDir, Arc<ServerState>, UserId) {
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

/// The auth of `user_id` in the realm of `state`.
pub(crate) fn state_auth(state: &ServerState, user_id: UserId) -> Option<AuthContext> {
    Some(AuthContext {
        user_id,
        realm_id: state.get_realm_id(),
        path_restrictions: None,
        session: None,
    })
}
