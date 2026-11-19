//! Exercises the deletion decision over authenticated peer streams.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::time::Duration;

use aruna_core::keyspaces::{GROUP_DELETE_KEYSPACE, GROUP_KEYSPACE};
use aruna_core::structs::identity::group_delete::{
    GroupDeletePhase, GroupDeleteRecord, GroupDeletionError,
};
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};

use ulid::Ulid;

use aruna::inbound::initialize_incoming_fixture;
use aruna_operations::groups::deletion::tests::{Fixture, read};
use aruna_operations::metadata::MetadataHandle;

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
    let graph = aruna_graph::GraphHandle::open(
        fixture.dir.path().join("metadata"),
        net.node_id(),
        Some(net.document_sync_node()),
        Some(net.document_sync_database()),
        aruna_graph::GraphOptions::default(),
    )
    .unwrap();
    let metadata = MetadataHandle::new(
        fixture.dir.path().join("metadata"),
        fixture.context.storage_handle.clone(),
        Some(net.clone()),
        Default::default(),
    );
    fixture.context.net_handle = Some(net.clone());
    fixture.context.metadata_handle = Some(metadata);
    fixture.context.graph_handle = Some(graph);
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
        aruna_operations::driver::drive(
            aruna_operations::groups::deletion::DeleteGroupOperation::new(
                context.node_and_realm(),
                auth.clone(),
                None,
                group_id,
                Ulid::generate(),
            ),
            context,
        )
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

#[tokio::test]
async fn check_reports_nodes() {
    let mut first = Fixture::new(&[1, 2, 3]).await;
    let mut second = Fixture::new(&[1, 2, 3]).await;
    let first_net = connect_fixture(&mut first, 1).await;
    let second_net = connect_fixture(&mut second, 2).await;
    first_net.add_peer_addr(second_net.endpoint_addr()).await;
    second_net.add_peer_addr(first_net.endpoint_addr()).await;
    aruna_operations::groups::deletion::check_tests::buckets(&second, 2, &["owned", "hidden"])
        .await;
    let auth = first.auth.clone();
    let group_id = first.plan.group_id;
    let first_context = Arc::new(first.context);
    let second_context = Arc::new(second.context);
    let first_handler = initialize_incoming_fixture(first_context.clone()).unwrap();
    let second_handler = initialize_incoming_fixture(second_context.clone()).unwrap();
    let replies = aruna_operations::driver::drive(
        aruna_operations::groups::deletion::CheckGroupOperation::new(
            first_context.node_and_realm(),
            auth,
            group_id,
        ),
        &first_context,
    )
    .await
    .unwrap();
    let reply = |seed| {
        replies
            .iter()
            .find(|(node, _)| *node == aruna_operations::groups::deletion::tests::node(seed))
            .map(|(_, result)| result.clone())
            .unwrap()
    };
    assert_eq!(replies.len(), 3);
    assert_eq!(reply(1), Ok(()));
    assert_eq!(
        reply(2),
        Err(GroupDeletionError::NotEmpty {
            kinds: vec!["S3 buckets".into()],
            buckets: vec!["owned".into()],
            more: false,
            hidden: 1,
        })
    );
    assert!(matches!(reply(3), Err(GroupDeletionError::Unavailable(_))));
    assert!(
        read(
            &second_context,
            GROUP_DELETE_KEYSPACE,
            group_id.to_bytes().to_vec()
        )
        .await
        .is_none()
    );
    assert!(first_handler.stop(Duration::from_secs(60)).await);
    assert!(second_handler.stop(Duration::from_secs(60)).await);
    first_net.shutdown().await;
    second_net.shutdown().await;
}
