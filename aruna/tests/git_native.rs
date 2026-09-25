//! Runs real Git/LFS clients and ARCitect against the native Aruna endpoint.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

#![recursion_limit = "256"]

mod shared;

use aruna_api::cors::CorsConfig;
use aruna_api::server::state::ServerState;
use aruna_api::server::{Server, ServerConfig};
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use shared::{
    TestResult, create_bearer_token, create_group_http, create_s3_credentials, spawn_complete_seed,
};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::process::Command;

type ServerTask = tokio::task::JoinHandle<Result<(), aruna_api::error::ServerSetupError>>;

/// Serves one node's REST API with native Git hosting from `root`.
async fn git_server(
    context: Arc<aruna_operations::driver::DriverContext>,
    realm_id: aruna_core::structs::identity::realm::RealmId,
    node_id: iroh::PublicKey,
    capabilities: aruna_core::structs::identity::auth::NodeCapabilities,
    root: std::path::PathBuf,
) -> TestResult<(String, tokio_util::sync::CancellationToken, ServerTask)> {
    let state = Arc::new(
        ServerState::new(
            context,
            realm_id,
            node_id,
            capabilities,
            false,
            None,
            aruna_operations::jobs::runtime::JobsRuntime::new(),
        )
        .await
        .with_git(root, env!("CARGO_BIN_EXE_aruna").into()),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = Server::new(
        state,
        ServerConfig {
            http_addr: address,
            max_body_size: aruna_api::server::MAX_BODY_SIZE,
            cors: CorsConfig::new(vec![]),
        },
    );
    let shutdown = tokio_util::sync::CancellationToken::new();
    let task = tokio::spawn(server.run_with_listener(listener, shutdown.clone()));
    Ok((format!("http://{address}"), shutdown, task))
}

#[tokio::test]
#[ignore = "requires ARUNA_ARC_PYTHON, Git LFS and optionally ARUNA_ARCITECT"]
async fn native_clients() -> TestResult<()> {
    let python = std::env::var("ARUNA_ARC_PYTHON")?;
    let seed = spawn_complete_seed().await?;
    let directory = tempfile::tempdir()?;
    let (base, shutdown, server_task) = git_server(
        seed.context.clone(),
        seed.realm_id,
        seed.net.node_id(),
        seed.capabilities.clone(),
        directory.path().join("git"),
    )
    .await?;
    let result = async {
        let token = create_bearer_token(seed.context.as_ref(), seed.user_id, seed.realm_id, seed.capabilities.clone()).await?;
        let group = create_group_http(&seed.base_url, &token, "native-arc").await?;
        let credentials = create_s3_credentials(&seed.base_url, &token, &group.group_id).await?;
        let endpoint = seed.s3.as_ref().ok_or_else(|| std::io::Error::other("S3 unavailable"))?;
        let client = reqwest::Client::new();
        let document: serde_json::Value = client.post(format!("{base}/api/v1/metadata")).bearer_auth(&token)
            .json(&serde_json::json!({"group_id":group.group_id,"path":"native-arc","name":"Native ARC",
                "description":"Native Git and LFS integration","date_published":"2026-09-22",
                "license":"https://creativecommons.org/licenses/by/4.0/","public":false}))
            .send().await?.error_for_status()?.json().await?;
        let id = document["document_id"].as_str().ok_or_else(|| std::io::Error::other("document ID missing"))?;
        shared::wait_until("metadata registry visibility", shared::WAIT_CAP, Duration::from_millis(100), || async {
            client.get(format!("{base}/api/v1/metadata/{id}")).bearer_auth(&token).send().await
                .is_ok_and(|response| response.status().is_success())
        }).await?;
        let read_token = shared::sign_scoped_token(&seed, seed.user_id, vec![
            aruna_core::structs::identity::auth::PathRestriction {
                pattern: aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord::permission_path_for(
                    &seed.realm_id, group.group_id.parse()?, "native-arc", id.parse()?),
                permission: aruna_core::structs::identity::auth::Permission::READ,
            },
        ])?;
        shared::wait_until("automatic ARC repository", shared::WAIT_CAP, Duration::from_millis(100), || async {
            matches!(seed.context.storage_handle.send_storage_effect(StorageEffect::Read {
                key_space: aruna_core::git::STATUS.into(), key: id.parse::<ulid::Ulid>().expect("document ID").to_bytes().to_vec().into(), txn_id: None,
            }).await, Event::Storage(StorageEvent::ReadResult { value: Some(_), .. }))
        }).await?;
        assert!(directory.path().join("git").join(format!("{id}.git/refs/heads/main")).is_file(), "automatic ARC snapshot did not publish main");
        let repository: serde_json::Value = client.get(format!("{base}/api/v1/metadata/{id}/git")).bearer_auth(&token)
            .send().await?.error_for_status()?.json().await?;
        assert!(repository["error"].is_null(), "ARC conversion failed");
        let bucket = repository["bucket"].as_str().ok_or_else(|| std::io::Error::other("automatic LFS bucket missing"))?;
        let other = aruna_core::UserId::local(ulid::Ulid::generate(), seed.realm_id);
        let other_token = create_bearer_token(seed.context.as_ref(), other, seed.realm_id, seed.capabilities.clone()).await?;
        client.post(format!("{base}/api/v1/access/groups/{}/members", group.group_id)).bearer_auth(&token)
            .json(&serde_json::json!({"user_id": other.to_string()})).send().await?.error_for_status()?;
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().expect("workspace root");
        let mut child = Command::new(python).arg(root.join("scripts/arc-native/test_native.py"))
            .env("ARUNA_GIT_URL", repository["clone_url"].as_str().ok_or_else(|| std::io::Error::other("Git URL missing"))?)
            .env("ARUNA_TOKEN", &token).env("ARUNA_API_URL", &base).env("ARUNA_DOCUMENT_ID", id)
            .env("ARUNA_READ_TOKEN", read_token).env("ARUNA_OTHER_TOKEN", &other_token)
            .env("ARUNA_ARN_PREFIX", format!("arn:aruna:{}:{}:s3", seed.realm_id, seed.net.node_id()))
            .env("ARUNA_GIT_ROOT", directory.path().join("git"))
            .env("ARUNA_GROUP_ID", &group.group_id).env("ARUNA_BUCKET", &bucket)
            .env("ARUNA_S3_URL", &endpoint.endpoint_url).env("AWS_ACCESS_KEY_ID", &credentials.access_key_id)
            .env("AWS_SECRET_ACCESS_KEY", &credentials.access_secret).env("AWS_DEFAULT_REGION", shared::AWS_REGION)
            .kill_on_drop(true).spawn()?;
        if !tokio::time::timeout(Duration::from_secs(1200), child.wait()).await??.success() {
            return Err(std::io::Error::other("native Git/LFS client test failed").into());
        }
        let event = seed.context.storage_handle.send_storage_effect(StorageEffect::Iter {
            key_space: aruna_core::keyspaces::GIT_RECORD_KEYSPACE.into(),
            prefix: Some(aruna_core::git::git_record_prefix(id.parse()?)), start: None, limit: 1000, txn_id: None,
        }).await;
        let Event::Storage(StorageEvent::IterResult { values, .. }) = event else {
            return Err(std::io::Error::other("Git records unreadable").into());
        };
        let checkpoints = values.iter().filter(|(_, value)| {
            postcard::from_bytes::<aruna_core::git::GitRecord>(value)
                .is_ok_and(|record| matches!(record.change, aruna_core::git::GitChange::Checkpoint(_)))
        }).count();
        assert!(checkpoints > 0, "no Git checkpoint was written");
        Ok(())
    }.await;
    shutdown.cancel();
    server_task.await??;
    seed.shutdown().await;
    result
}

#[tokio::test]
#[ignore = "requires ARUNA_ARC_PYTHON and Git LFS"]
async fn holder_failover() -> TestResult<()> {
    let python = std::env::var("ARUNA_ARC_PYTHON")?;
    let seed = spawn_complete_seed().await?;
    let secret =
        shared::create_onboarding_secret(&seed, aruna_core::onboarding::OnboardingMode::Server)
            .await?;
    let joiner = shared::spawn_complete_joiner(&seed, secret).await?;
    shared::wait_realm_nodes(&[seed.context.as_ref(), joiner.context.as_ref()], &seed.realm_id, 2).await?;
    let directory = tempfile::tempdir()?;
    let (base_a, stop_a, task_a) = git_server(
        seed.context.clone(),
        seed.realm_id,
        seed.net.node_id(),
        seed.capabilities.clone(),
        directory.path().join("a"),
    )
    .await?;
    let (base_b, stop_b, task_b) = git_server(
        joiner.context.clone(),
        joiner.config.realm_id,
        joiner.config.node_id,
        joiner.config.node_capabilities.clone(),
        directory.path().join("b"),
    )
    .await?;
    let result = async {
        let token = create_bearer_token(seed.context.as_ref(), seed.user_id, seed.realm_id, seed.capabilities.clone()).await?;
        let group = create_group_http(&seed.base_url, &token, "holder-arc").await?;
        shared::wait_group_http(&joiner.base_url, &token, &group.group_id).await?;
        let client = reqwest::Client::new();
        let document: serde_json::Value = client.post(format!("{base_a}/api/v1/metadata")).bearer_auth(&token)
            .json(&serde_json::json!({"group_id":group.group_id,"path":"holder-arc","name":"Holder ARC",
                "description":"Two holders serve one ARC","date_published":"2026-09-25",
                "license":"https://creativecommons.org/licenses/by/4.0/","public":false}))
            .send().await?.error_for_status()?.json().await?;
        let id = document["document_id"].as_str().ok_or_else(|| std::io::Error::other("document ID missing"))?;
        for base in [&base_a, &base_b] {
            shared::wait_until("ARC repository on each holder", shared::WAIT_CAP, Duration::from_millis(250), || async {
                let Ok(response) = client.get(format!("{base}/api/v1/metadata/{id}/git")).bearer_auth(&token).send().await else {
                    return false;
                };
                response.status().is_success()
                    && response.json::<serde_json::Value>().await.is_ok_and(|status| status["commit"].is_string())
            }).await?;
        }
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().expect("workspace root");
        let mut child = Command::new(python).arg(root.join("scripts/arc-native/test_holders.py"))
            .env("ARUNA_GIT_URL_A", format!("{base_a}/api/v1/git/{id}.git"))
            .env("ARUNA_GIT_URL_B", format!("{base_b}/api/v1/git/{id}.git"))
            .env("ARUNA_API_A", &base_a).env("ARUNA_API_B", &base_b)
            .env("ARUNA_TOKEN", &token).env("ARUNA_DOCUMENT_ID", id)
            .kill_on_drop(true).spawn()?;
        if !tokio::time::timeout(Duration::from_secs(1200), child.wait()).await??.success() {
            return Err(std::io::Error::other("two-holder Git test failed").into());
        }
        Ok(())
    }.await;
    stop_a.cancel();
    stop_b.cancel();
    task_a.await??;
    task_b.await??;
    shared::shutdown_pair(joiner, seed).await;
    result
}
