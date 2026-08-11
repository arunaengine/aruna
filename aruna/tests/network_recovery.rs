#![recursion_limit = "256"]

mod shared;

use aruna_core::effects::{DhtEffect, Effect, NetEffect};
use aruna_core::events::{DhtEvent, Event, NetEvent};
use aruna_core::handle::Handle;
use aruna_core::keys::realm_presence_key;
use aruna_core::structs::RealmId;
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_operations::driver::DriverContext;
use aruna_operations::incoming::initialize_net_incoming;
use aruna_operations::metadata::MetadataHandle;
use aruna_tasks::TaskHandle;
use reqwest::StatusCode;
use serde_json::{Value, json};
use shared::{
    TestResult, create_bearer_token, create_group_via_http, create_onboarding_secret_via_http,
    spawn_full_joiner_node, spawn_full_seed_node, wait_for_realm_nodes, wait_until,
};
use std::sync::Arc;
use std::time::Duration;

async fn realm_info(base_url: &str, token: &str) -> TestResult<Value> {
    let response = reqwest::Client::new()
        .get(format!("{base_url}/api/v1/info/realm"))
        .bearer_auth(token)
        .send()
        .await?;
    if response.status() != StatusCode::OK {
        return Err(
            std::io::Error::other(format!("realm info returned {}", response.status())).into(),
        );
    }
    Ok(response.json().await?)
}

async fn metadata_query(base_url: &str, token: &str) -> TestResult<Value> {
    let response = reqwest::Client::new()
        .post(format!("{base_url}/api/v1/metadata/sparql/query"))
        .bearer_auth(token)
        .json(&json!({
            "query": "SELECT DISTINCT ?s WHERE { ?s ?p ?o }",
            "mode": "distributed",
            "allow_partial": true
        }))
        .send()
        .await?;
    if response.status() != StatusCode::OK {
        return Err(std::io::Error::other(format!(
            "metadata query returned {}",
            response.status()
        ))
        .into());
    }
    Ok(response.json().await?)
}

async fn rejoin_peer(
    joiner: &shared::JoinerNode,
    seed: &shared::SeedNode,
) -> TestResult<RejoinedPeer> {
    let directory = tempfile::tempdir()?;
    let context_storage = joiner.context.storage_handle.clone();
    let config = NetConfig {
        bind_addr: "127.0.0.1:0".parse()?,
        secret_key: Some(joiner.config.net_secret_key.clone()),
        realm_id: joiner.config.realm_id,
        peer_nodes: vec![seed.net.node_id()],
        peer_endpoints: vec![seed.net.endpoint_addr()],
        temporary_bootstrap_active: false,
        discovery_method: DiscoveryMethod::None,
        relay_method: RelayMethod::None,
        max_concurrent_uni_streams: joiner.config.max_concurrent_uni_streams,
        max_concurrent_bidi_streams: joiner.config.max_concurrent_bidi_streams,
        document_sync_storage_path: Some(directory.path().join("document-sync")),
        document_sync_runtime: Some(joiner.config.document_sync_runtime),
        fjall_persist_policy: joiner.config.fjall_persist_policy,
    };
    let peer = NetHandle::new(config, context_storage.clone()).await?;
    let metadata = match MetadataHandle::new(
        directory.path().join("metadata"),
        peer.node_id(),
        context_storage.clone(),
        Some(peer.clone()),
        Some(peer.document_sync_node()),
        Some(peer.document_sync_database()),
    ) {
        Ok(metadata) => metadata,
        Err(error) => {
            peer.shutdown().await;
            return Err(error.into());
        }
    };
    let context = Arc::new(DriverContext {
        storage_handle: context_storage,
        net_handle: Some(peer.clone()),
        blob_handle: None,
        metadata_handle: Some(metadata),
        task_handle: Some(TaskHandle::new()),
        compute_handle: None,
    });
    initialize_net_incoming(context.clone());
    peer.add_peer_addr(seed.net.endpoint_addr()).await;
    seed.net.add_peer_addr(peer.endpoint_addr()).await;
    Ok(RejoinedPeer {
        net: peer,
        _directory: directory,
        _context: context,
    })
}

struct RejoinedPeer {
    net: NetHandle,
    _directory: tempfile::TempDir,
    _context: Arc<DriverContext>,
}

async fn announce_peer(peer: &NetHandle, realm_id: RealmId) -> TestResult<()> {
    let event = peer
        .send_effect(Effect::Net(NetEffect::Dht(DhtEffect::Put {
            key: realm_presence_key(&realm_id),
            realm_id,
            value: peer.node_id().as_bytes().to_vec(),
            ttl: Duration::from_secs(60),
        })))
        .await;
    if !matches!(
        event,
        Event::Net(NetEvent::Dht(DhtEvent::PutComplete { .. }))
    ) {
        return Err(
            std::io::Error::other(format!("presence announcement failed: {event:?}")).into(),
        );
    }
    Ok(())
}

async fn check_healthy(seed: &shared::SeedNode, token: &str) -> TestResult<()> {
    let info = realm_info(&seed.base_url, token).await?;
    assert_eq!(info["nodes"].as_array().map(Vec::len), Some(2));
    assert!(
        info["nodes"]
            .as_array()
            .is_some_and(|nodes| nodes.iter().all(|node| node["configured"] == true))
    );
    let query = metadata_query(&seed.base_url, token).await?;
    assert_eq!(query["complete"], true);
    assert_eq!(query["nodes_failed"], 0);
    Ok(())
}

async fn check_outage(
    seed: &shared::SeedNode,
    joiner: &shared::JoinerNode,
    token: &str,
) -> TestResult<()> {
    let before = seed.net.pool_counts();
    joiner.net.shutdown().await;
    let partial = metadata_query(&seed.base_url, token).await?;
    assert_eq!(partial["complete"], false);
    assert_eq!(partial["nodes_queried"], 2);
    assert_eq!(partial["nodes_failed"], 1);
    assert!(
        partial["failed_partitions"]
            .as_array()
            .is_some_and(|nodes| nodes
                .iter()
                .any(|node| node == &json!(joiner.config.node_id.to_string())))
    );
    let after = seed.net.pool_counts();
    assert!(after.dials > before.dials);

    let retry = metadata_query(&seed.base_url, token).await?;
    assert_eq!(retry["complete"], false);
    assert_eq!(retry["nodes_queried"], 2);
    assert_eq!(retry["nodes_failed"], 1);
    let retried = seed.net.pool_counts();
    // A request may cross the five-second cooldown; then one re-probe is valid.
    let retry_dials = retried.dials - after.dials;
    assert!(retry_dials <= 1);
    if retry_dials == 0 {
        assert!(retried.cooldown_hits > after.cooldown_hits);
    }
    Ok(())
}

async fn check_recovery(
    seed: &shared::SeedNode,
    joiner: &shared::JoinerNode,
    token: &str,
) -> TestResult<()> {
    let peer = rejoin_peer(joiner, seed).await?;
    let result = async {
        announce_peer(&peer.net, seed.realm_id).await?;
        wait_until(
            "metadata fan-out recovery",
            Duration::from_secs(30),
            Duration::from_millis(100),
            || {
                let seed_url = seed.base_url.clone();
                let token = token.to_string();
                async move {
                    metadata_query(&seed_url, &token).await.is_ok_and(|result| {
                        result["complete"] == true && result["nodes_failed"] == 0
                    })
                }
            },
        )
        .await?;
        let info = realm_info(&seed.base_url, token).await?;
        assert!(
            info["nodes"]
                .as_array()
                .is_some_and(|nodes| nodes.iter().all(|node| node["configured"] == true))
        );
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    peer.net.shutdown().await;
    result
}

#[tokio::test]
async fn realm_outage_recovers() -> TestResult<()> {
    let seed = spawn_full_seed_node().await?;
    let onboarding =
        create_onboarding_secret_via_http(&seed, aruna_core::onboarding::OnboardingMode::Local)
            .await?;
    let joiner = spawn_full_joiner_node(&seed, onboarding).await?;
    let result = async {
        let token = create_bearer_token(
            seed.context.as_ref(),
            seed.user_id,
            seed.realm_id,
            seed.capabilities.clone(),
        )
        .await?;
        wait_for_realm_nodes(
            &[seed.context.as_ref(), joiner.context.as_ref()],
            &seed.realm_id,
            2,
        )
        .await?;
        let group = create_group_via_http(&seed.base_url, &token, "network-recovery").await?;
        check_healthy(&seed, &token).await?;

        let created = reqwest::Client::new()
            .post(format!("{}/api/v1/metadata", seed.base_url))
            .bearer_auth(&token)
            .json(&json!({
                "group_id": group.group_id,
                "path": "datasets/network-recovery",
                "name": "Network recovery",
                "description": "Outage test",
                "date_published": "2026-08-11",
                "public": true
            }))
            .send()
            .await?;
        assert_eq!(created.status(), StatusCode::CREATED);
        check_outage(&seed, &joiner, &token).await?;
        check_recovery(&seed, &joiner, &token).await?;
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    joiner.shutdown().await;
    seed.shutdown().await;
    result
}
