//! Tests that a created group replicates to every realm node, holder or not.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::document::DocumentTarget;
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::placement::record::{PlacementRef, shard_for_subject};
use aruna_operations::driver::drive;
use aruna_operations::groups::create_group::{CreateGroupConfig, CreateGroupOperation};
use aruna_operations::groups::get_group::{GetGroupConfig, GetGroupOperation};
use aruna_operations::placement::{
    PlacementResolutionContext, resolve_shard_holders, target_placement_ref,
};
use ulid::Ulid;

use crate::convergence::wait_for_convergence;
use crate::topology::{TestNode, install_replica_config, spawn_replicas};

#[tokio::test]
async fn creation_replicates_globally() -> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([31u8; 32]);
    let (nodes, _config) = build_realm_nodes(&realm_id, 3).await?;

    let creator = Actor {
        node_id: nodes[0].net.node_id(),
        user_id: aruna_core::UserId::local(Ulid::generate(), realm_id),
        realm_id,
    };

    let expected = drive(
        CreateGroupOperation::new(CreateGroupConfig {
            actor: creator,
            display_name: "replicated group".to_string(),
            owner_cap: None,
        }),
        nodes[0].context.as_ref(),
    )
    .await?;

    wait_group_convergence(&nodes, expected.0.group_id, &expected.0, &expected.1).await?;
    shutdown_nodes(nodes).await;
    Ok(())
}

/// Five nodes at replication factor three, so a replica-capped bucket leaves real non-holders;
/// the three- and four-node fixtures cannot see this class of bug, since there every node holds
/// every bucket.
#[tokio::test]
async fn unheld_group_replicates() -> Result<(), Box<dyn std::error::Error>> {
    let realm_id = RealmId([33u8; 32]);
    let (nodes, config) = build_realm_nodes(&realm_id, 5).await?;
    let origin = nodes[0].net.node_id();

    let mut expected = None;
    for attempt in 0..32 {
        let created = drive(
            CreateGroupOperation::new(CreateGroupConfig {
                actor: Actor {
                    node_id: origin,
                    user_id: aruna_core::UserId::local(Ulid::generate(), realm_id),
                    realm_id,
                },
                display_name: format!("unheld group {attempt}"),
                owner_cap: None,
            }),
            nodes[0].context.as_ref(),
        )
        .await?;
        if !capped_holders(&config, created.0.group_id).contains(&origin) {
            expected = Some(created);
            break;
        }
    }
    let expected = expected.ok_or("no group id hashed outside the origin's capped buckets")?;

    wait_group_convergence(&nodes, expected.0.group_id, &expected.0, &expected.1).await?;

    // What makes the create publishable from an origin the capped strategy would
    // have excluded: every node holds the group's real bucket.
    let holders = group_holders(&config, expected.0.group_id);
    for node in &nodes {
        assert!(holders.contains(&node.net.node_id()));
    }

    shutdown_nodes(nodes).await;
    Ok(())
}

/// Holders of the bucket the group's authorization document hashes into, under
/// the strategy the realm actually binds the group class to.
fn group_holders(config: &RealmConfigDocument, group_id: Ulid) -> Vec<NodeId> {
    let target = DocumentTarget::GroupAuthorization { group_id };
    let placement = target_placement_ref(
        config,
        &target,
        PlacementResolutionContext {
            group_id: Some(group_id),
            metadata_path: None,
        },
    );
    resolve_shard_holders(config, &placement)
}

/// Holders the group would have had under the realm's replica-capped default
/// strategy: the set the group class was wrongly bound to, and the one that
/// leaves non-holders at all.
fn capped_holders(config: &RealmConfigDocument, group_id: Ulid) -> Vec<NodeId> {
    let strategy = config
        .default_strategy_id
        .and_then(|id| config.strategy(&id))
        .expect("seeded default strategy");
    assert_eq!(strategy.replica_count, Some(3));
    resolve_shard_holders(
        config,
        &PlacementRef {
            strategy_id: strategy.strategy_id,
            shard: shard_for_subject(&group_id.to_bytes(), strategy.shard_count),
        },
    )
}

async fn build_realm_nodes(
    realm_id: &RealmId,
    count: usize,
) -> Result<(Vec<TestNode>, RealmConfigDocument), Box<dyn std::error::Error>> {
    let nodes = spawn_replicas(*realm_id, count, false, true).await?;

    let config = install_replica_config(&nodes, realm_id).await?;
    Ok((nodes, config))
}

async fn wait_group_convergence(
    nodes: &[TestNode],
    group_id: Ulid,
    expected_group: &Group,
    expected_auth: &GroupAuthorizationDocument,
) -> Result<(), Box<dyn std::error::Error>> {
    wait_for_convergence("group state did not converge", || async {
        let mut pending = 0;
        for node in nodes {
            match drive(
                GetGroupOperation::new(GetGroupConfig { group_id }),
                node.context.as_ref(),
            )
            .await
            {
                Ok((group, auth)) if group == *expected_group && auth == *expected_auth => {}
                _ => pending += 1,
            }
        }
        Ok(pending)
    })
    .await
}

async fn shutdown_nodes(nodes: Vec<TestNode>) {
    for node in nodes {
        node.net.shutdown().await;
    }
}
