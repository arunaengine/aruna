//! Tests that an S3 credential minted on one node never replicates to a realm peer.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::{Duration, SystemTime};

use crate::topology::store_realm_config;
use crate::topology::{TestNode, spawn_replicas};
use aruna_core::UserId;
use aruna_core::structs::identity::auth::PathRestriction;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_operations::driver::drive;
use aruna_operations::s3::access::create::{CreateUserConfig, CreateUserOperation};
use aruna_operations::s3::access::get::GetAccessOperation;
use aruna_operations::s3::access::revoke::RevokeUserOperation;
use ulid::Ulid;

#[tokio::test]
async fn credential_stays_local() -> Result<(), Box<dyn std::error::Error>> {
    // Issuer-local by design: a credential minted on node 0 must never appear
    // on node 1, even after a full explicit sync of the shared realm topic.
    let realm_id = RealmId([41u8; 32]);
    let nodes = build_realm_nodes(&realm_id, 2).await?;

    let restrictions = vec![PathRestriction {
        pattern: "datasets/**".to_string(),
        permission: aruna_core::structs::identity::auth::Permission::READ,
    }];
    let config = CreateUserConfig {
        user_identity: UserId::local(Ulid::generate(), realm_id),
        group_id: Ulid::generate(),
        expiry: SystemTime::now() + Duration::from_secs(3600),
        path_restrictions: Some(restrictions.clone()),
        issued_by: *nodes[0].net.node_id().as_bytes(),
    };
    let encryption_key = aruna_core::credential_encryption::CredentialEncryptionKey::random();
    let (access_key, _, _) = drive(
        CreateUserOperation::new(config, encryption_key),
        nodes[0].context.as_ref(),
    )
    .await?;

    let local = drive(
        GetAccessOperation::new(access_key.clone()),
        nodes[0].context.as_ref(),
    )
    .await?;
    let _ = local;

    // Positive control: pull the whole shared realm topic from the issuer, so
    // absence afterwards proves non-replication rather than sync lag.
    let topic = aruna_core::document::DocumentTarget::RealmAuthorization { realm_id }
        .sync_topic_id(
            realm_id,
            &aruna_core::structs::placement::record::PlacementRef::NIL,
        );
    nodes[1]
        .net
        .sync_document_topics(vec![topic], vec![nodes[0].net.node_id()])
        .await;

    let remote = drive(
        GetAccessOperation::new(access_key.clone()),
        nodes[1].context.as_ref(),
    )
    .await;
    assert!(matches!(
        remote,
        Err(aruna_operations::s3::access::get::GetAccessError::NotFound)
    ));

    drive(
        RevokeUserOperation::new(access_key.clone()),
        nodes[0].context.as_ref(),
    )
    .await?;

    let revoked = drive(
        GetAccessOperation::new(access_key),
        nodes[0].context.as_ref(),
    )
    .await;
    assert!(matches!(
        revoked,
        Err(aruna_operations::s3::access::get::GetAccessError::NotFound)
    ));

    shutdown_nodes(nodes).await;
    Ok(())
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
) -> Result<RealmConfigDocument, Box<dyn std::error::Error>> {
    let mut config = RealmConfigDocument::default_for_realm(*realm_id, Vec::new());
    config.seed_default_placement();
    for node in nodes {
        config.ensure_node(node.net.node_id(), RealmNodeKind::Management);
    }

    for node in nodes {
        store_realm_config(&node.net, &node.context, *realm_id, &config).await?;
    }

    for _ in 0..5 {
        for node in nodes {
            aruna_operations::node::startup::restore_shard_subscriptions(
                &node.context,
                node.net.node_id(),
                *realm_id,
            )
            .await;
        }
        let mut retry = false;
        for node in nodes {
            retry |= aruna_operations::placement::process_placements::process_shard_placements(
                &node.context,
                *realm_id,
                node.net.node_id(),
            )
            .await
            .retry_scheduled;
        }
        if !retry {
            break;
        }
    }

    Ok(config)
}

async fn shutdown_nodes(nodes: Vec<TestNode>) {
    for node in nodes {
        node.net.shutdown().await;
    }
}
