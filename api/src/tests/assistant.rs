//! Builds assistant test fixtures: server state, an auth context and a mock HTTP server.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::server::state::ServerState;
use crate::tests::routes::{test_context, test_state, test_storage};
use aruna_core::UserId;
use aruna_core::structs::identity::auth::{AuthContext, NodeCapabilities};
use aruna_core::structs::identity::realm::RealmId;
use axum::Router;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::net::TcpListener;
use ulid::Ulid;

pub(crate) async fn setup_state() -> (TempDir, ServerState, AuthContext) {
    let (dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([3; 32]);
    let user_id = UserId::local(Ulid::from_bytes([4; 16]), realm_id);
    let state = test_state(
        Arc::new(test_context(storage)),
        realm_id,
        iroh::SecretKey::generate().public(),
        NodeCapabilities::user_node(realm_id).unwrap(),
    )
    .await;
    let auth = AuthContext {
        user_id,
        realm_id,
        path_restrictions: None,
        session: None,
    };
    (dir, state, auth)
}

pub(crate) async fn spawn_mock(router: Router) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    (format!("http://{address}"), handle)
}
