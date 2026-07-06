#![allow(clippy::result_large_err)]

use aruna::bootstrap::{
    announce_core_documents, ensure_initial_local_onboarding_secret,
    fetch_core_onboarding_documents, realm_bootstrap_exists, wait_for_onboarding_placement,
};
use aruna::config::{Config, StartupMode, load, mark_node_state_complete, mark_onboarding_phase};
use aruna::portal;
use aruna::telemetry::{init_tracing, shutdown_tracing};
use aruna_api::auth::OidcValidator;
use aruna_api::cors::CorsConfig;
use aruna_api::ops::{OpsState, Readiness, serve_ops};
use aruna_api::s3::s3_server::S3Server;
use aruna_api::server::{Server, ServerConfig};
use aruna_api::server_state::ServerState;
use aruna_blob::blob::BlobHandler;
use aruna_core::UserId;
use aruna_core::metrics::NodeMetrics;
use aruna_core::onboarding::OnboardingPhase;
use aruna_core::structs::Backend::FileSystem;
use aruna_core::structs::BackendConfig;
use aruna_core::structs::NodeCapabilities;
use aruna_core::structs::{Actor, NodeUrls, RealmNodeKind};
use aruna_net::{NetConfig, NetHandle};
use aruna_operations::announce_realm_presence::{
    AnnounceRealmPresenceConfig, AnnounceRealmPresenceOperation,
};
use aruna_operations::create_realm::{CreateRealmConfig, CreateRealmOperation};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::ensure_realm_config::{EnsureRealmConfigConfig, EnsureRealmConfigOperation};
use aruna_operations::incoming::initialize_net_incoming;
use aruna_operations::metadata::projector::replay_metadata_event_log;
use aruna_operations::metadata::{MetadataHandle, MetadataHandleOptions, spawn_metadata_warmup};
use aruna_operations::process_placements::{PlacementConfig, ProcessPlacementsOperation};
use aruna_operations::startup::RestoreTopicSubscriptionsOperation;
use aruna_operations::task_incoming::initialize_task_incoming;
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{error, info, warn};

#[tokio::main]
async fn main() {
    dotenvy::dotenv().expect("Failed to load .env file");
    init_tracing();

    let result = run().await;
    shutdown_tracing();

    if let Err(err) = result {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let (config, storage_handle) = load().await?;
    let net_handle = NetHandle::new(
        NetConfig {
            bind_addr: config.p2p_socket_addr,
            secret_key: Some(config.net_secret_key.clone()),
            realm_id: config.realm_id,
            peer_nodes: config.peer_nodes.clone(),
            peer_endpoints: config.peer_endpoints.clone(),
            temporary_bootstrap_active: config.temporary_bootstrap_active,
            discovery_method: config.discovery_method.clone(),
            relay_method: config.relay_method.clone(),
            max_concurrent_uni_streams: config.max_concurrent_uni_streams,
            max_concurrent_bidi_streams: config.max_concurrent_bidi_streams,
            document_sync_storage_path: Some(config.document_sync_storage_path.clone()),
            document_sync_runtime: Some(config.document_sync_runtime),
            fjall_persist_policy: config.fjall_persist_policy,
        },
        storage_handle.clone(),
    )
    .await?;
    if let Err(error) = net_handle.reload_realm_peers().await {
        warn!(error = %error, "Failed to refresh realm peers from persisted config during startup");
    }
    let task_handle = TaskHandle::new();
    let metadata_handle = MetadataHandle::new_with_options(
        &config.metadata_storage_path,
        config.node_id,
        storage_handle.clone(),
        Some(net_handle.clone()),
        Some(net_handle.document_sync_node()),
        Some(net_handle.document_sync_database()),
        MetadataHandleOptions::default()
            .with_search_storage(config.metadata_search_storage)
            .with_document_sync_persist_policy(config.fjall_persist_policy),
    )?;
    let blob_handle = BlobHandler::new(
        BackendConfig {
            backend_type: FileSystem,
            root: config.blob_root.clone(),
            service_config: HashMap::new(),
            bucket_prefix: config.blob_bucket_prefix.clone(),
            max_bucket_size: config.blob_max_bucket_size,
            multipart_bucket: config.blob_multipart_bucket.clone(),
            timeouts: config.blob_timeout_config(),
        },
        storage_handle.clone(),
        net_handle.clone(),
    )
    .await?;

    let driver_ctx = Arc::new(DriverContext {
        storage_handle,
        net_handle: Some(net_handle.clone()),
        blob_handle: Some(blob_handle),
        metadata_handle: Some(metadata_handle),
        task_handle: Some(task_handle.clone()),
    });

    // Start the ops listener before realm bootstrap so `/readyz` reports 503
    // (startup) and `/metrics` is scrapable while the node is still coming up.
    let metrics = Arc::new(NodeMetrics::new());
    let readiness = Readiness::new();
    if let Some(ops_addr) = config.ops_socket_addr {
        let ops_state = OpsState::new(driver_ctx.clone(), metrics.clone(), readiness.clone()).await;
        let ops_listener = TcpListener::bind(ops_addr).await?;
        let bound = ops_listener.local_addr()?;
        tokio::spawn(async move {
            if let Err(error) = serve_ops(ops_listener, ops_state).await {
                error!(error = %error, "Ops server stopped");
            }
        });
        info!(ops_address = %bound, "Ops server listening");
    }

    ensure_usage_counters(driver_ctx.as_ref()).await?;

    initialize_net_incoming(driver_ctx.clone());
    initialize_task_incoming(driver_ctx.clone(), task_handle).await;

    // Republish a full set of node usage snapshots at startup so realm peers see
    // this node's totals again after a restart, dirty-marker loss, or a counter
    // rebuild. Best-effort: failures are retried by the debounced publisher.
    if let Err(error) = aruna_operations::usage_stats::publish_and_refresh_usage_snapshots(
        driver_ctx.as_ref(),
        config.node_id,
        config.realm_id,
        true,
    )
    .await
    {
        warn!(error = %error, "Failed to publish initial node usage snapshots");
    }

    let replayed_metadata_events = replay_metadata_event_log(driver_ctx.as_ref()).await?;
    if replayed_metadata_events > 0 {
        info!(
            replayed_metadata_events,
            "Replayed metadata event log during startup"
        );
    }
    spawn_metadata_warmup(driver_ctx.clone());

    match &config.startup_mode {
        StartupMode::InitializeRealm { realm_description } => {
            if !realm_bootstrap_exists(driver_ctx.as_ref(), &config.realm_id).await? {
                drive(
                    CreateRealmOperation::new(CreateRealmConfig {
                        actor: Actor {
                            node_id: config.node_id,
                            user_id: UserId::nil(config.realm_id),
                            realm_id: config.realm_id,
                        },
                        realm_description: realm_description.clone(),
                        oidc_providers: config.oidc_providers.clone(),
                        node_location: config.node_location.clone(),
                        node_weight: config.node_weight,
                        node_labels: config.node_labels.clone(),
                    }),
                    driver_ctx.as_ref(),
                )
                .await?;
            }
            seed_local_node_info(driver_ctx.as_ref(), &config).await?;
            // CreateRealm mints the realm-auth/realm-config genesis via its admin
            // operation outbox records, but not the shared realm topics. Seed
            // NodeInfo first so its stored document is included in bootstrap.
            announce_core_documents(driver_ctx.as_ref(), config.node_id, &config.realm_id, true)
                .await?;

            if config.is_initial_node() {
                match ensure_initial_local_onboarding_secret(
                    driver_ctx.as_ref(),
                    format!("http://{}", config.http_socket_addr),
                )
                .await
                {
                    Ok(secret) => warn!(
                        onboarding_secret = %secret
                            .encode()
                            .expect("initial onboarding secret encoding should succeed"),
                        "Created initial local onboarding secret for first user registration"
                    ),
                    Err(error) => {
                        return Err(format!(
                            "failed to create initial local onboarding secret: {error}"
                        )
                        .into());
                    }
                }
            }

            mark_node_state_complete(&driver_ctx.storage_handle, &config.node_state).await?;
        }
        StartupMode::JoinRealm { phase } => {
            let bootstrap_peer = config
                .peer_endpoints
                .first()
                .map(|endpoint| endpoint.id)
                .or_else(|| config.peer_nodes.first().copied());
            if matches!(phase, OnboardingPhase::Bootstrapped) {
                fetch_core_onboarding_documents(
                    driver_ctx.as_ref(),
                    &config.node_state,
                    &config.realm_id,
                    bootstrap_peer,
                )
                .await?;
            }
            wait_for_onboarding_placement(
                driver_ctx.as_ref(),
                config.realm_id,
                config.node_id,
                bootstrap_peer,
            )
            .await?;
            if matches!(phase, OnboardingPhase::Bootstrapped) {
                mark_onboarding_phase(
                    &driver_ctx.storage_handle,
                    &config.node_state,
                    OnboardingPhase::CoreDocumentsFetched,
                )
                .await?;
                if let Err(error) = net_handle.reload_realm_peers().await {
                    warn!(error = %error, "Failed to refresh realm peers after onboarding document fetch");
                }
            }
            seed_local_node_info(driver_ctx.as_ref(), &config).await?;
            // A joining node is never the realm-bootstrap node: it announces the
            // shared topics with allow_genesis=false after onboarding fetched a
            // representative target for each.
            announce_core_documents(driver_ctx.as_ref(), config.node_id, &config.realm_id, false)
                .await?;
            mark_node_state_complete(&driver_ctx.storage_handle, &config.node_state).await?;
        }
        StartupMode::Provisioned => {
            drive(
                RestoreTopicSubscriptionsOperation::new(config.node_id, config.realm_id),
                driver_ctx.as_ref(),
            )
            .await?;

            if matches!(
                &config.node_capabilities,
                NodeCapabilities::Management { .. }
            ) {
                drive(
                    EnsureRealmConfigOperation::new(EnsureRealmConfigConfig {
                        actor: Actor {
                            node_id: config.node_id,
                            user_id: UserId::nil(config.realm_id),
                            realm_id: config.realm_id,
                        },
                        target_node_id: config.node_id,
                        target_node_kind: RealmNodeKind::Management,
                        default_metadata_replication_factor: config
                            .default_metadata_replication_factor,
                        realm_description: config.realm_description.clone(),
                        create_if_missing: true,
                        reject_kind_mismatch: false,
                    }),
                    driver_ctx.as_ref(),
                )
                .await?;
            }

            seed_local_node_info(driver_ctx.as_ref(), &config).await?;
            // The realm-bootstrap node retains genesis authority so a missing
            // shared topic can be repaired after a partial first boot. Onboarded
            // nodes still wait for that authoritative genesis.
            announce_core_documents(
                driver_ctx.as_ref(),
                config.node_id,
                &config.realm_id,
                config.is_initial_node(),
            )
            .await?;
        }
    }

    // Republish a full set of node usage snapshots after startup-mode core
    // document announcement has had a chance to queue the shared node-usage topic
    // genesis. Best-effort: failures are retried by the debounced publisher.
    if let Err(error) = aruna_operations::usage_stats::publish_and_refresh_usage_snapshots(
        driver_ctx.as_ref(),
        config.node_id,
        config.realm_id,
        true,
    )
    .await
    {
        warn!(error = %error, "Failed to publish initial node usage snapshots");
    }

    drive(
        ProcessPlacementsOperation::new(PlacementConfig {
            realm_id: config.realm_id,
            local_node_id: config.node_id,
            retry_after: aruna_operations::sync_placement::SYNC_PLACEMENT_RETRY_AFTER,
        }),
        driver_ctx.as_ref(),
    )
    .await?;

    drive(
        AnnounceRealmPresenceOperation::new(AnnounceRealmPresenceConfig {
            realm_id: config.realm_id,
            node_id: config.node_id,
            schedule_refresh: true,
        }),
        driver_ctx.as_ref(),
    )
    .await?;

    // REST Server
    let is_initial_node = config.is_initial_node();
    let state = Arc::new(
        ServerState::new(
            driver_ctx.clone(),
            config.realm_id,
            config.node_id,
            config.node_capabilities,
            is_initial_node,
            Some(Arc::new(OidcValidator::new()?)),
        )
        .await
        .with_metrics(metrics.clone()),
    );
    portal::initialize(config.portal.clone(), state.clone()).await;

    let cors = CorsConfig::new(config.cors_allowed_origins.clone());
    let server_config = ServerConfig {
        http_addr: config.http_socket_addr,
        max_http_body_size: config.max_http_body_size,
        cors: cors.clone(),
    };
    let server = Server::new(state.clone(), server_config);

    // S3 Server
    let s3_server = S3Server::new(
        &config.s3_address,
        &config.s3_host,
        driver_ctx.clone(),
        config.realm_id,
        config.node_id,
        cors,
        metrics.clone(),
    )
    .await
    .unwrap();

    let s3_listener = TcpListener::bind(&config.s3_address).await.unwrap();
    let s3_bound_addr = s3_listener.local_addr().unwrap();
    state
        .register_s3_interface(
            s3_bound_addr,
            config.s3_public_url.as_deref().unwrap_or(&config.s3_host),
        )
        .await;
    let (_s3_addr, server_handle) = s3_server.run_with_listener(s3_listener).unwrap();

    let rest_listener = TcpListener::bind(config.http_socket_addr).await?;

    // Both request listeners are bound and bootstrap has completed: the node is
    // ready to serve, so `/readyz` may now return 200.
    readiness.set_ready();

    tokio::select! {
        res = server_handle => {
            match res {
                Ok(_) => info!("S3 Server stopped normally"),
                Err(e) => error!("S3 Server panicked: {:?}", e),
            }
        }
        res = server.run_with_listener(rest_listener) => {
            match res {
                Ok(_) => info!("REST Server stopped normally"),
                Err(e) => error!("REST Server panicked: {:?}", e),
            }
        }
        // You can add other signals here, e.g., Ctrl+C
        _ = tokio::signal::ctrl_c() => {
            info!("Shutting down S3 interface");
        }
    }

    shutdown_runtime(
        driver_ctx.net_handle.as_ref(),
        driver_ctx.metadata_handle.as_ref(),
        &driver_ctx.storage_handle,
    )
    .await;

    Ok(())
}

async fn seed_local_node_info(ctx: &DriverContext, config: &Config) -> Result<(), String> {
    aruna_operations::node_info::seed_node_info_document(
        ctx,
        config.node_id,
        config.realm_id,
        NodeUrls {
            api: config.api_public_url.clone(),
            s3: config.s3_public_url.clone(),
        },
    )
    .await
}

async fn shutdown_runtime(
    net_handle: Option<&NetHandle>,
    metadata_handle: Option<&MetadataHandle>,
    storage_handle: &StorageHandle,
) {
    if let Some(net_handle) = net_handle {
        info!("Shutting down network services");
        net_handle.shutdown().await;
    }

    if let Some(metadata_handle) = metadata_handle {
        info!("Flushing metadata persistence");
        if let Err(error) = metadata_handle.flush_persistence().await {
            error!(error = %error, "Failed to flush metadata persistence during shutdown");
        }
    }

    if let Err(error) = storage_handle.sync_all().await {
        error!(error = %error, "Failed to sync storage during shutdown");
    }
}

/// Ensures the maintained usage counter shards exist before background writes start.
async fn ensure_usage_counters(
    driver_ctx: &DriverContext,
) -> Result<(), Box<dyn std::error::Error>> {
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::USAGE_STATS_KEYSPACE;
    use aruna_core::structs::usage_global_shard_keys;
    use aruna_operations::usage_stats::RebuildUsageStatsOperation;

    let shard_keys = usage_global_shard_keys();
    let event = driver_ctx
        .storage_handle
        .send_storage_effect(StorageEffect::BatchRead {
            reads: shard_keys
                .iter()
                .map(|key| (USAGE_STATS_KEYSPACE.to_string(), key.clone().into()))
                .collect(),
            txn_id: None,
        })
        .await;

    match event {
        Event::Storage(StorageEvent::BatchReadResult { values }) => {
            if values.len() != shard_keys.len() {
                return Err(format!(
                    "usage counter probe returned {} values for {} shards",
                    values.len(),
                    shard_keys.len()
                )
                .into());
            }
            if values.iter().any(|(_, value)| value.is_none()) {
                drive(RebuildUsageStatsOperation::new(), driver_ctx).await?;
            }
        }
        Event::Storage(StorageEvent::Error { error }) => {
            return Err(format!("usage counter probe failed: {error}").into());
        }
        other => {
            return Err(format!("usage counter probe received unexpected event: {other:?}").into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::effects::StorageEffect;
    use aruna_core::errors::StorageError;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::USAGE_STATS_KEYSPACE;
    use aruna_core::structs::{UsageCounters, usage_global_shard_keys};
    use std::thread;
    use tempfile::tempdir;

    fn test_driver_ctx(storage_handle: StorageHandle) -> DriverContext {
        DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        }
    }

    #[tokio::test]
    async fn shutdown_runtime_syncs_storage_without_net() {
        let (storage_handle, receiver) = StorageHandle::new();
        let worker = thread::spawn(move || {
            let (effect, response_tx, _span, _queued_at) = receiver
                .recv()
                .expect("shutdown should request storage sync_all");
            assert!(matches!(effect, StorageEffect::SyncAll));
            response_tx.send(StorageEvent::SyncAllFinished);
        });

        shutdown_runtime(None, None, &storage_handle).await;

        worker.join().expect("storage responder should finish");
    }

    #[tokio::test]
    async fn ensure_usage_counters_rebuilds_missing_shards() {
        let temp = tempdir().expect("temp dir");
        let storage_handle = aruna_storage::FjallStorage::open(
            temp.path().to_str().expect("temp path should be utf8"),
        )
        .expect("storage opens");
        let driver_ctx = test_driver_ctx(storage_handle.clone());

        ensure_usage_counters(&driver_ctx).await.unwrap();

        for key in usage_global_shard_keys() {
            let event = storage_handle
                .send_storage_effect(StorageEffect::Read {
                    key_space: USAGE_STATS_KEYSPACE.to_string(),
                    key: key.into(),
                    txn_id: None,
                })
                .await;
            let Event::Storage(StorageEvent::ReadResult {
                value: Some(bytes), ..
            }) = event
            else {
                panic!("expected rebuilt usage shard, got {event:?}");
            };
            assert_eq!(
                UsageCounters::from_bytes(bytes.as_ref()).unwrap(),
                UsageCounters::default()
            );
        }
    }

    #[tokio::test]
    async fn ensure_usage_counters_returns_probe_errors() {
        let (storage_handle, receiver) = StorageHandle::new();
        let worker = thread::spawn(move || {
            let (effect, response_tx, _span, _queued_at) = receiver
                .recv()
                .expect("usage counter ensure should probe storage");
            assert!(matches!(effect, StorageEffect::BatchRead { .. }));
            response_tx.send(StorageEvent::Error {
                error: StorageError::ReadError,
            });
        });
        let driver_ctx = test_driver_ctx(storage_handle);

        let error = ensure_usage_counters(&driver_ctx)
            .await
            .unwrap_err()
            .to_string();

        assert!(error.contains("usage counter probe failed"));
        worker.join().expect("storage responder should finish");
    }

    #[tokio::test]
    async fn ensure_usage_counters_rejects_unexpected_probe_events() {
        let (storage_handle, receiver) = StorageHandle::new();
        let worker = thread::spawn(move || {
            let (effect, response_tx, _span, _queued_at) = receiver
                .recv()
                .expect("usage counter ensure should probe storage");
            assert!(matches!(effect, StorageEffect::BatchRead { .. }));
            response_tx.send(StorageEvent::SyncAllFinished);
        });
        let driver_ctx = test_driver_ctx(storage_handle);

        let error = ensure_usage_counters(&driver_ctx)
            .await
            .unwrap_err()
            .to_string();

        assert!(error.contains("usage counter probe received unexpected event"));
        worker.join().expect("storage responder should finish");
    }
}
