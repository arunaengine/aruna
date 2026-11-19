//! Builds the realm ids, documents, graph access and background drains the metadata route tests share.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::RealmAuth;
use crate::error::ServerResult;
use crate::metadata::*;
use crate::routes::metadata::documents::*;
use crate::routes::metadata::query::*;
use crate::routes::metadata::references::*;
use crate::routes::metadata::rocrate::*;
use crate::routes::metadata::validation::*;
use crate::server::state::ServerState;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_operations::driver::DriverContext;
use aruna_operations::driver::drive;
use aruna_operations::metadata::api::node_fanout::DiscoveryOperation;
use aruna_operations::testing::process_materialization_batch;
use aruna_operations::testing::{drain_projection_queue, replay_event_log};
use axum::extract::State;
use axum::{Extension, Json};
use ed25519_dalek::SigningKey;
use std::sync::Arc;
use ulid::Ulid;

pub async fn state_realm_nodes(state: &ServerState) -> ServerResult<Vec<aruna_core::NodeId>> {
    let ctx = state.get_ctx();
    let discovery = DiscoveryOperation::new(state.get_realm_id(), state.get_node_id());
    let discovery = drive(discovery, ctx.as_ref()).await;
    Ok(discovery.map_or_else(|_| vec![state.get_node_id()], |discovery| discovery.nodes))
}

pub async fn create_test_document(
    state: Arc<ServerState>,
    auth: AuthContext,
    group_id: Ulid,
    path: &str,
    name: &str,
    public: bool,
) {
    let _ = create_metadata_document(
        State(state),
        RealmAuth(auth),
        Extension(None),
        Json(CreateMetadataRequest::Scaffold(CreateScaffoldRequest {
            group_id: group_id.to_string(),
            path: path.to_string(),
            name: name.to_string(),
            description: "Remote metadata access fixture".to_string(),
            date_published: "2026-01-01".to_string(),
            license: Some("https://creativecommons.org/licenses/by/4.0/".to_string()),
            public,
            message: None,
            storage_location: None,
        })),
    )
    .await
    .unwrap();
}

pub fn test_realm_id(seed: u8) -> RealmId {
    RealmId::from_bytes(
        SigningKey::from_bytes(&[seed; 32])
            .verifying_key()
            .to_bytes(),
    )
}

pub async fn drain_metadata_background(state: &ServerState) {
    let ctx = state.get_ctx();
    let drained = drain_projection_queue(ctx.as_ref()).await.unwrap();
    if drained.markers_examined == 0 {
        replay_event_log(ctx.as_ref()).await.unwrap();
    }
    process_materialization_batch(ctx.as_ref()).await.unwrap();
}

pub async fn write_doc(
    driver_ctx: &Arc<DriverContext>,
    key_space: &str,
    key: byteview::ByteView,
    value: byteview::ByteView,
) {
    let event = driver_ctx
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Write {
            key_space: key_space.to_string(),
            key,
            value,
            txn_id: None,
        }))
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
}
