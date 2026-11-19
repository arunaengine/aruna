//! Builds the realm, node, auth and server states the execution route tests share.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::server::state::ServerState;
use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::structs::identity::auth::{AuthContext, NodeCapabilities};
use aruna_core::structs::identity::realm::RealmId;
use aruna_operations::jobs::runtime::JobsRuntime;
use aruna_operations::testing::test_context;
use aruna_storage::FjallStorage;
use std::sync::Arc;
use tempfile::TempDir;

pub(crate) fn realm() -> RealmId {
    RealmId([1u8; 32])
}

pub(crate) fn node_id() -> NodeId {
    iroh::SecretKey::from_bytes(&[7u8; 32]).public()
}

pub(crate) fn auth_for(user_id: UserId) -> Option<AuthContext> {
    Some(AuthContext {
        user_id,
        realm_id: realm(),
        path_restrictions: None,
        session: None,
    })
}

/// A user node state on a fresh store.
pub(crate) async fn build_state() -> (TempDir, Arc<ServerState>) {
    node_state(NodeCapabilities::user_node(realm()).unwrap()).await
}

/// A management node state on a fresh store.
pub(crate) async fn management_state() -> (TempDir, Arc<ServerState>) {
    let signing_key = aruna_core::keys::generate_signing_key();
    node_state(NodeCapabilities::management_node(signing_key).unwrap()).await
}

async fn node_state(capabilities: NodeCapabilities) -> (TempDir, Arc<ServerState>) {
    let dir = tempfile::tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let ctx = Arc::new(test_context(storage));
    let state = ServerState::new(
        ctx,
        realm(),
        node_id(),
        capabilities,
        false,
        None,
        JobsRuntime::new(),
    )
    .await;
    (dir, Arc::new(state))
}
