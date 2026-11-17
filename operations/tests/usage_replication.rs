//! Tests that node usage snapshots publish to peers and add up to the realm total.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::storage::format::Compression;

use aruna_core::document::DocumentTarget;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{NODE_STATS_KEYSPACE, USAGE_STATS_KEYSPACE};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::usage::{
    DIRTY_GLOBAL_KEY, NodeUsageSnapshot, UsageCounters, global_group_key, usage_global_key,
    usage_group_key,
};
use aruna_operations::driver::drive;
use aruna_operations::node::usage_stats::{
    RealmUsageScope, load_realm_usage, publish_usage_snapshots,
};
use aruna_operations::s3::bucket::create::CreateBucketOperation;
use aruna_operations::sync::replicate_documents::{
    ReplicateDocumentsConfig, ReplicateDocumentsOperation,
};
use ulid::Ulid;

use crate::convergence::wait_for_convergence;
use crate::topology::store_realm_config;
use crate::topology::{TestNode, spawn_replicas};

// Node A's usage counters must become visible in node B's realm-wide aggregate via the real
// document sync apply path.
#[tokio::test]
async fn snapshot_reaches_peer() -> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([37u8; 32]);
    let nodes = build_realm_nodes(&realm_id, 2).await?;
    let node_a = &nodes[0];
    let node_b = &nodes[1];
    let group_id = Ulid::generate();
    bootstrap_usage_genesis(node_a, node_b, realm_id).await?;

    drive(
        CreateBucketOperation::new(
            "usage-bucket".to_string(),
            BucketInfo {
                group_id,
                created_at: std::time::SystemTime::now(),
                created_by: Default::default(),
                cors_configuration: None,
                storage_routing: Vec::new(),
                placement_policies: Vec::new(),
                placement_policy_generation: 0,
                compression: Compression::Off,
                distribution: None,
            },
        ),
        node_a.context.as_ref(),
    )
    .await?;

    publish_usage_snapshots(
        node_a.context.as_ref(),
        node_a.net.node_id(),
        realm_id,
        false,
    )
    .await?;

    // Node B receives A's snapshots over the sync layer and folds them into the
    // realm aggregate (global and per-group).
    let a_snapshot_key = usage_global_key(node_a.net.node_id());
    wait_for_convergence::<_, _, Box<dyn std::error::Error>>(
        "node B never observed A's usage snapshot",
        || async {
            let received = read_node_stats(node_b, a_snapshot_key.clone())
                .await
                .is_some();
            let global = load_realm_usage(
                node_b.context.as_ref(),
                node_b.net.node_id(),
                RealmUsageScope::Global,
            )
            .await
            .map(|usage| usage.counters)
            .unwrap_or_default();
            let group = load_realm_usage(
                node_b.context.as_ref(),
                node_b.net.node_id(),
                RealmUsageScope::Group(group_id),
            )
            .await
            .map(|usage| usage.counters)
            .unwrap_or_default();
            Ok(usize::from(
                !(received && global.buckets == 1 && group.buckets == 1),
            ))
        },
    )
    .await?;

    // Realm aggregate on B includes A's bucket even though B created nothing.
    let realm_global = load_realm_usage(
        node_b.context.as_ref(),
        node_b.net.node_id(),
        RealmUsageScope::Global,
    )
    .await?
    .counters;
    assert_eq!(realm_global.buckets, 1, "realm global should include A");
    let realm_group = load_realm_usage(
        node_b.context.as_ref(),
        node_b.net.node_id(),
        RealmUsageScope::Group(group_id),
    )
    .await?
    .counters;
    assert_eq!(realm_group.buckets, 1, "realm group should include A");

    // Ingest is counter-neutral: B's node-local usage keyspace is untouched.
    let local_stats = iter_usage_stats(node_b).await;
    assert!(
        local_stats.is_empty(),
        "replication ingest must not write B's node-local usage counters, found {} keys",
        local_stats.len()
    );

    shutdown_nodes(nodes).await;
    Ok(())
}

// Snapshot ingest updates realm aggregates without touching local incremental counters.
#[tokio::test]
async fn snapshot_ingest_neutral() -> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([41u8; 32]);
    let nodes = build_realm_nodes(&realm_id, 2).await?;
    let node_a = &nodes[0];
    let node_b = &nodes[1];
    let group_id = Ulid::generate();
    bootstrap_usage_genesis(node_a, node_b, realm_id).await?;

    // Seed node A's live counters with a full, non-trivial usage total, then let
    // the real publisher distribute it as node-usage snapshot documents.
    let rich = UsageCounters {
        buckets: 2,
        objects: 5,
        stored_blobs: 3,
        stored_bytes: 4096,
        logical_bytes: 8192,
        referenced_bytes: 2048,
    };
    write_usage_stat(node_a, global_group_key(group_id), rich).await;
    write_usage_stat(node_a, usage_group_key(group_id), rich).await;

    publish_usage_snapshots(
        node_a.context.as_ref(),
        node_a.net.node_id(),
        realm_id,
        true,
    )
    .await?;

    let a_snapshot_key = usage_global_key(node_a.net.node_id());
    wait_for_convergence::<_, _, Box<dyn std::error::Error>>(
        "node B never observed A's rich usage snapshot",
        || async {
            let received = read_node_stats(node_b, a_snapshot_key.clone())
                .await
                .is_some();
            let global = load_realm_usage(
                node_b.context.as_ref(),
                node_b.net.node_id(),
                RealmUsageScope::Global,
            )
            .await
            .map(|usage| usage.counters)
            .unwrap_or_default();
            let group = load_realm_usage(
                node_b.context.as_ref(),
                node_b.net.node_id(),
                RealmUsageScope::Group(group_id),
            )
            .await
            .map(|usage| usage.counters)
            .unwrap_or_default();
            Ok(usize::from(
                !(received && global.objects == rich.objects && group.objects == rich.objects),
            ))
        },
    )
    .await?;

    // Every field of A's snapshot surfaces in B's realm aggregate.
    let realm_global = load_realm_usage(
        node_b.context.as_ref(),
        node_b.net.node_id(),
        RealmUsageScope::Global,
    )
    .await?
    .counters;
    assert_eq!(
        realm_global, rich,
        "realm global aggregate must equal A's snapshot"
    );
    let realm_group = load_realm_usage(
        node_b.context.as_ref(),
        node_b.net.node_id(),
        RealmUsageScope::Group(group_id),
    )
    .await?
    .counters;
    assert_eq!(
        realm_group, rich,
        "realm group aggregate must equal A's snapshot"
    );

    // Ingest never wrote a single live incremental counter on B.
    let local_stats = iter_usage_stats(node_b).await;
    assert!(
        local_stats.is_empty(),
        "counter-bearing snapshot ingest must not touch B's live counter keyspace, found {} keys",
        local_stats.len()
    );

    shutdown_nodes(nodes).await;
    Ok(())
}

// A steady-state write must publish without relying on restart recovery.
#[tokio::test]
async fn steady_write_publishes() -> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([53u8; 32]);
    let nodes = build_realm_nodes(&realm_id, 1).await?;
    let node = &nodes[0];
    let group_id = Ulid::generate();

    // A normal write commit updates counters and stamps dirty markers, but never
    // schedules a publish itself and never restarts the node.
    drive(
        CreateBucketOperation::new(
            "steady-bucket".to_string(),
            BucketInfo {
                group_id,
                created_at: std::time::SystemTime::now(),
                created_by: Default::default(),
                cors_configuration: None,
                storage_routing: Vec::new(),
                placement_policies: Vec::new(),
                placement_policy_generation: 0,
                compression: Compression::Off,
                distribution: None,
            },
        ),
        node.context.as_ref(),
    )
    .await?;

    let snapshot_key = usage_global_key(node.net.node_id());
    wait_for_convergence::<_, _, Box<dyn std::error::Error>>(
        "steady-state write never published a snapshot",
        || async {
            let published = read_node_stats(node, snapshot_key.clone()).await.is_some();
            let dirty_cleared = read_node_stats(node, DIRTY_GLOBAL_KEY.to_vec())
                .await
                .is_none();
            Ok(usize::from(!(published && dirty_cleared)))
        },
    )
    .await?;

    shutdown_nodes(nodes).await;
    Ok(())
}

async fn write_usage_stat(node: &TestNode, key: Vec<u8>, counters: UsageCounters) {
    match node
        .context
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Write {
            key_space: USAGE_STATS_KEYSPACE.to_string(),
            key: key.into(),
            value: counters.to_bytes().unwrap().into(),
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => panic!("unexpected usage stat write event: {other:?}"),
    }
}

async fn bootstrap_usage_genesis(
    publisher: &TestNode,
    subscriber: &TestNode,
    realm_id: RealmId,
) -> Result<(), Box<dyn std::error::Error>> {
    let node_id = publisher.net.node_id();
    let snapshot_key = usage_global_key(node_id);
    let snapshot = NodeUsageSnapshot {
        node_id,
        counters: UsageCounters::default(),
        replicas: Default::default(),
        revisions: Default::default(),
    };
    match publisher
        .context
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Write {
            key_space: NODE_STATS_KEYSPACE.to_string(),
            key: snapshot_key.clone().into(),
            value: snapshot.to_bytes()?.into(),
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => return Err(format!("unexpected node usage snapshot write: {other:?}").into()),
    }

    drive(
        ReplicateDocumentsOperation::new(ReplicateDocumentsConfig {
            realm_id,
            local_node_id: node_id,
            excluded_peers: Vec::new(),
            documents: vec![DocumentTarget::NodeUsage {
                realm_id,
                node_id,
                group_id: None,
            }],
            allow_genesis: true,
        }),
        publisher.context.as_ref(),
    )
    .await?;

    wait_for_convergence(
        "bootstrap node-usage genesis did not reach peer",
        || async {
            Ok(usize::from(
                read_node_stats(subscriber, snapshot_key.clone())
                    .await
                    .is_none(),
            ))
        },
    )
    .await
}
async fn read_node_stats(node: &TestNode, key: Vec<u8>) -> Option<Vec<u8>> {
    match node
        .context
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Read {
            key_space: aruna_core::keyspaces::NODE_STATS_KEYSPACE.to_string(),
            key: key.into(),
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value.map(|b| b.to_vec()),
        other => panic!("unexpected read event: {other:?}"),
    }
}

async fn iter_usage_stats(node: &TestNode) -> Vec<UsageCounters> {
    match node
        .context
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Iter {
            key_space: USAGE_STATS_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit: 4096,
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => values
            .into_iter()
            .map(|(_, value)| UsageCounters::from_bytes(value.as_ref()).unwrap())
            .collect(),
        other => panic!("unexpected iter event: {other:?}"),
    }
}

async fn build_realm_nodes(
    realm_id: &RealmId,
    count: usize,
) -> Result<Vec<TestNode>, Box<dyn std::error::Error>> {
    let nodes = spawn_replicas(*realm_id, count, false, false).await?;

    install_realm_config(&nodes, realm_id).await?;
    Ok(nodes)
}

async fn install_realm_config(
    nodes: &[TestNode],
    realm_id: &RealmId,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut config = RealmConfigDocument::default_for_realm(*realm_id, Vec::new());
    config.seed_default_placement();
    for node in nodes {
        config.ensure_node(node.net.node_id(), RealmNodeKind::Management);
    }

    for node in nodes {
        store_realm_config(&node.net, &node.context, *realm_id, &config).await?;
    }
    // Config apply hook: the shard's rank-0 holder eagerly creates each
    // shard topic genesis (mirrors the production realm-config apply path).
    for node in nodes {
        aruna_operations::placement::process_placements::process_shard_placements(
            &node.context,
            *realm_id,
            node.net.node_id(),
        )
        .await;
    }

    Ok(())
}

async fn shutdown_nodes(nodes: Vec<TestNode>) {
    for node in nodes {
        node.net.shutdown().await;
    }
}
