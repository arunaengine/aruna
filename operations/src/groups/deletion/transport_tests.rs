//! Exercises the deletion decision over authenticated peer streams.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::time::Duration;

use aruna_core::keyspaces::{GROUP_DELETE_KEYSPACE, GROUP_KEYSPACE};
use aruna_core::structs::identity::group_delete::{GroupDeletePhase, GroupDeleteRecord};
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};

use super::tests::{Fixture, read};
use crate::metadata::MetadataHandle;
use crate::sync::incoming::initialize_incoming_fixture;

async fn connect_fixture(fixture: &mut Fixture, seed: u8) -> NetHandle {
    let net = NetHandle::new(
        NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            secret_key: Some(iroh::SecretKey::from_bytes(&[seed; 32])),
            realm_id: fixture.plan.realm_id,
            discovery_method: DiscoveryMethod::None,
            relay_method: RelayMethod::None,
            sync_storage_path: Some(fixture.dir.path().join("sync")),
            ..Default::default()
        },
        fixture.context.storage_handle.clone(),
    )
    .await
    .unwrap();
    let metadata = MetadataHandle::new(
        fixture.dir.path().join("metadata"),
        net.node_id(),
        fixture.context.storage_handle.clone(),
        Some(net.clone()),
        Some(net.document_sync_node()),
        Some(net.document_sync_database()),
    )
    .unwrap();
    fixture.context.net_handle = Some(net.clone());
    fixture.context.metadata_handle = Some(metadata);
    net.reload_realm_peers().await.unwrap();
    net
}

#[tokio::test]
async fn peers_delete_together() {
    let mut first = Fixture::new(&[1, 2]).await;
    let mut second = Fixture::new(&[1, 2]).await;
    let first_net = connect_fixture(&mut first, 1).await;
    let second_net = connect_fixture(&mut second, 2).await;
    first_net.add_peer_addr(second_net.endpoint_addr()).await;
    second_net.add_peer_addr(first_net.endpoint_addr()).await;
    let coordinator = first.plan.coordinator;
    let auth = first.auth.clone();
    let group_id = first.plan.group_id;
    let first_context = Arc::new(first.context);
    let second_context = Arc::new(second.context);
    let first_handler = initialize_incoming_fixture(first_context.clone()).unwrap();
    let second_handler = initialize_incoming_fixture(second_context.clone()).unwrap();
    let context = if first_net.node_id() == coordinator {
        &first_context
    } else {
        &second_context
    };
    for _ in 0..2 {
        super::delete_group(context, auth.clone(), None, group_id)
            .await
            .unwrap();
    }
    for context in [&first_context, &second_context] {
        assert!(
            read(context, GROUP_KEYSPACE, group_id.to_bytes().to_vec())
                .await
                .is_none()
        );
        let value = read(context, GROUP_DELETE_KEYSPACE, group_id.to_bytes().to_vec())
            .await
            .unwrap();
        let record = GroupDeleteRecord::from_bytes(&value).unwrap();
        assert_eq!(record.phase, GroupDeletePhase::Deleted);
        assert!(record.event.is_some());
    }
    assert!(first_handler.stop(Duration::from_secs(60)).await);
    assert!(second_handler.stop(Duration::from_secs(60)).await);
    first_net.shutdown().await;
    second_net.shutdown().await;
}
