//! Ignored load tests that metadata creates complete and converge on every node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::time::Duration;

use aruna_core::structs::identity::realm::RealmId;
use aruna_core::types::GroupId;
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::realm::announce_presence::{
    AnnouncePresenceConfig, AnnouncePresenceOperation,
};
use ulid::Ulid;

use crate::convergence::{wait_empty_queues, wait_for_visibility, wait_storage_released};
use crate::topology::{
    BoxError, WriterSettings, build_realm_nodes, init_logging, install_default_config,
    make_runtime, mesh, mixed_payload, node_targets, respawn_with_retry, run_writer, run_writers,
    shutdown_nodes, spawn_load_node, spawn_node_with, wait_node_convergence,
};

const TOTAL_CREATES: usize = 2000;

#[test]
#[ignore]
fn writer_levels_complete() -> Result<(), BoxError> {
    init_logging();

    for (level, &writers) in [1usize, 8, 32, 64].iter().enumerate() {
        let runtime = make_runtime()?;
        let per_writer = TOTAL_CREATES / writers;
        let total = runtime.block_on(async {
            let realm_id = RealmId([91u8 + level as u8; 32]);
            let nodes = build_realm_nodes(&realm_id, 3).await?;
            let targets = node_targets(&nodes);
            let group_id = Ulid::generate();
            let label = format!("tp{writers}");
            let created = run_writers(
                &WriterSettings {
                    realm_id,
                    group_id,
                    label: label.to_string(),
                    period: None,
                    payload: mixed_payload,
                },
                writers,
                per_writer,
                &targets,
            )
            .await;
            shutdown_nodes(nodes).await;
            Ok::<usize, BoxError>(created?.len())
        })?;
        runtime.shutdown_timeout(Duration::from_secs(10));
        assert_eq!(
            total,
            per_writer * writers,
            "every create must succeed with {writers} writers"
        );
    }
    Ok(())
}

#[test]
#[ignore]
fn convergence_gate() -> Result<(), BoxError> {
    init_logging();
    let runtime = make_runtime()?;
    runtime.block_on(async {
        let realm_id = RealmId([122u8; 32]);
        let nodes = build_realm_nodes(&realm_id, 3).await?;
        let targets = node_targets(&nodes);
        let group_id = Ulid::generate();

        let mut created = run_writers(
            &WriterSettings {
                realm_id,
                group_id,
                label: "conv".to_string(),
                period: None,
                payload: mixed_payload,
            },
            64,
            16,
            &targets,
        )
        .await?;
        println!(
            "created={} docs, polling for convergence of last 100",
            created.len()
        );

        created.sort_by_key(|(_, _, at)| *at);
        let last: Vec<(GroupId, Ulid)> = created
            .iter()
            .rev()
            .take(100)
            .map(|(group_id, document_id, _)| (*group_id, *document_id))
            .collect();

        let contexts: Vec<Arc<DriverContext>> = nodes.iter().map(|n| n.context.clone()).collect();
        let result = wait_for_visibility(&contexts, &last, Duration::from_millis(200)).await;
        shutdown_nodes(nodes).await;
        Ok::<(), BoxError>(result?)
    })?;
    runtime.shutdown_timeout(Duration::from_secs(10));
    Ok(())
}

// Exercises the production trigger chain end to end.
#[test]
#[ignore]
fn production_convergence_gate() -> Result<(), BoxError> {
    init_logging();
    let runtime = make_runtime()?;
    runtime.block_on(async {
        let realm_id = RealmId([124u8; 32]);
        let nodes = build_realm_nodes(&realm_id, 3).await?;
        let targets = node_targets(&nodes);
        let group_id = Ulid::generate();

        let created = run_writers(
            &WriterSettings {
                realm_id,
                group_id,
                label: "prod".to_string(),
                period: None,
                payload: mixed_payload,
            },
            32,
            128,
            &targets,
        )
        .await?;
        println!(
            "created={} docs, polling for full cluster convergence",
            created.len()
        );

        let pairs: Vec<(GroupId, Ulid)> = created
            .iter()
            .map(|(group_id, document_id, _)| (*group_id, *document_id))
            .collect();
        let contexts: Vec<Arc<DriverContext>> = nodes.iter().map(|n| n.context.clone()).collect();
        wait_for_visibility(&contexts, &pairs, Duration::from_millis(200)).await?;
        wait_empty_queues(&contexts).await?;
        shutdown_nodes(nodes).await;
        Ok::<(), BoxError>(())
    })?;
    runtime.shutdown_timeout(Duration::from_secs(10));
    Ok(())
}

#[test]
#[ignore]
fn churn_convergence_gate() -> Result<(), BoxError> {
    init_logging();
    let runtime = make_runtime()?;
    runtime.block_on(churn_convergence_body())?;
    runtime.shutdown_timeout(Duration::from_secs(10));
    Ok(())
}

async fn churn_convergence_body() -> Result<(), BoxError> {
    let realm_id = RealmId([123u8; 32]);
    let node2_dir = tempfile::tempdir()?;
    let secret = iroh::SecretKey::from_bytes(&[7u8; 32]);

    let aux = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;

    let mut nodes = Vec::with_capacity(3);
    nodes.push(spawn_load_node(realm_id, false).await?);
    nodes.push(spawn_load_node(realm_id, false).await?);
    let dir2 = node2_dir.path().to_path_buf();
    let secret2 = secret.clone();
    let node2 = aux
        .handle()
        .spawn(async move { spawn_node_with(realm_id, Some(secret2), dir2, false).await })
        .await??;
    nodes.push(node2);

    mesh(nodes.iter().map(|node| &node.net)).await;
    for (index, node) in nodes.iter().enumerate() {
        let op = AnnouncePresenceOperation::new(AnnouncePresenceConfig {
            realm_id,
            node_id: node.net.node_id(),
            schedule_refresh: true,
        });
        if index == 2 {
            let ctx = node.context.clone();
            aux.handle()
                .spawn(async move {
                    drive(op, ctx.as_ref())
                        .await
                        .map_err(|error| format!("announce failed: {error:?}"))
                })
                .await??;
        } else {
            drive(op, node.context.as_ref()).await?;
        }
    }
    wait_node_convergence(&nodes, &realm_id).await?;
    install_default_config(&nodes, &realm_id).await?;

    let group_id = Ulid::generate();
    let targets0 = vec![(nodes[0].net.node_id(), nodes[0].context.clone())];
    let initial = run_writer(
        &WriterSettings {
            realm_id,
            group_id,
            label: "seed".to_string(),
            period: None,
            payload: mixed_payload,
        },
        0,
        1,
        targets0.clone(),
    )
    .await?;
    let initial_pair = vec![(initial[0].0, initial[0].1)];
    {
        let contexts: Vec<Arc<DriverContext>> = nodes.iter().map(|n| n.context.clone()).collect();
        wait_for_visibility(&contexts, &initial_pair, Duration::from_millis(200)).await?;
    }
    println!("initial doc converged to all 3 nodes");

    let node2 = nodes.pop().expect("node 2 present");
    node2.net.clear_inbound_handler();
    node2
        .context
        .task_handle
        .as_ref()
        .ok_or("node 2 needs a task handle")?
        .clear_inbound_handler()
        .await;
    node2.net.shutdown().await;
    let storage = node2.context.storage_handle.clone();
    drop(node2);
    tokio::task::spawn_blocking(move || aux.shutdown_timeout(Duration::from_secs(10))).await?;
    wait_storage_released(storage).await?;
    println!("node 2 shut down");

    let created = run_writer(
        &WriterSettings {
            realm_id,
            group_id,
            label: "churn".to_string(),
            period: None,
            payload: mixed_payload,
        },
        0,
        200,
        targets0,
    )
    .await?;
    let pairs: Vec<(GroupId, Ulid)> = created.iter().map(|(g, d, _)| (*g, *d)).collect();
    println!("created {} docs while node 2 was down", pairs.len());

    let node2 = respawn_with_retry(realm_id, secret, node2_dir.path(), false).await?;
    for other in &nodes {
        node2.net.add_peer_addr(other.net.endpoint_addr()).await;
        other.net.add_peer_addr(node2.net.endpoint_addr()).await;
    }
    drive(
        AnnouncePresenceOperation::new(AnnouncePresenceConfig {
            realm_id,
            node_id: node2.net.node_id(),
            schedule_refresh: true,
        }),
        node2.context.as_ref(),
    )
    .await?;
    println!("node 2 respawned, polling for catch-up");

    let result = wait_for_visibility(
        std::slice::from_ref(&node2.context),
        &pairs,
        Duration::from_millis(500),
    )
    .await;
    nodes.push(node2);
    shutdown_nodes(nodes).await;
    Ok(result?)
}
