#![allow(clippy::result_large_err)]

use aruna::bootstrap::{
    announce_core_documents, ensure_initial_local_onboarding_secret,
    fetch_core_onboarding_documents, realm_bootstrap_exists,
};
use aruna::config::{StartupMode, load, mark_node_state_complete, mark_onboarding_phase};
use aruna::portal;
use aruna::telemetry::{init_tracing, shutdown_tracing};
use aruna_api::auth::OidcValidator;
use aruna_api::cors::CorsConfig;
use aruna_api::s3::s3_server::S3Server;
use aruna_api::server::{Server, ServerConfig};
use aruna_api::server_state::ServerState;
use aruna_blob::blob::BlobHandler;
use aruna_core::UserId;
use aruna_core::onboarding::OnboardingPhase;
use aruna_core::structs::Backend::FileSystem;
use aruna_core::structs::BackendConfig;
use aruna_core::structs::NodeCapabilities;
use aruna_core::structs::{Actor, RealmNodeKind};
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

    initialize_net_incoming(driver_ctx.clone());
    initialize_task_incoming(driver_ctx.clone(), task_handle).await;

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
            if realm_bootstrap_exists(driver_ctx.as_ref(), &config.realm_id).await? {
                announce_core_documents(driver_ctx.as_ref(), config.node_id, &config.realm_id)
                    .await?;
            } else {
                drive(
                    CreateRealmOperation::new(CreateRealmConfig {
                        actor: Actor {
                            node_id: config.node_id,
                            user_id: UserId::nil(config.realm_id),
                            realm_id: config.realm_id,
                        },
                        realm_description: realm_description.clone(),
                        oidc_providers: config.oidc_providers.clone(),
                    }),
                    driver_ctx.as_ref(),
                )
                .await?;
            }

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
            if matches!(phase, OnboardingPhase::Bootstrapped) {
                fetch_core_onboarding_documents(
                    driver_ctx.as_ref(),
                    &config.node_state,
                    &config.realm_id,
                    config.peer_endpoints.first().map(|endpoint| endpoint.id),
                )
                .await?;
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
            announce_core_documents(driver_ctx.as_ref(), config.node_id, &config.realm_id).await?;
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
                        create_if_missing: true,
                        reject_kind_mismatch: false,
                    }),
                    driver_ctx.as_ref(),
                )
                .await?;
            }

            announce_core_documents(driver_ctx.as_ref(), config.node_id, &config.realm_id).await?;
        }
    }

    drive(
        ProcessPlacementsOperation::new(PlacementConfig {
            realm_id: config.realm_id,
            local_node_id: config.node_id,
        }),
        driver_ctx.as_ref(),
    )
    .await?;

    ensure_usage_counters(driver_ctx.as_ref()).await?;

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
        .await,
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
    )
    .await
    .unwrap();

    let s3_listener = TcpListener::bind(&config.s3_address).await.unwrap();
    let s3_bound_addr = s3_listener.local_addr().unwrap();
    state
        .register_s3_interface(s3_bound_addr, &config.s3_host)
        .await;
    let (_s3_addr, server_handle) = s3_server.run_with_listener(s3_listener).unwrap();

    let rest_listener = TcpListener::bind(config.http_socket_addr).await?;

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

/// Builds the maintained usage counters once for stores that predate them.
async fn ensure_usage_counters(
    driver_ctx: &DriverContext,
) -> Result<(), Box<dyn std::error::Error>> {
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::USAGE_STATS_KEYSPACE;
    use aruna_core::structs::USAGE_GLOBAL_KEY;
    use aruna_operations::usage_stats::RebuildUsageStatsOperation;

    let event = driver_ctx
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: USAGE_STATS_KEYSPACE.to_string(),
            key: USAGE_GLOBAL_KEY.to_vec().into(),
            txn_id: None,
        })
        .await;

    if matches!(
        event,
        Event::Storage(StorageEvent::ReadResult { value: None, .. })
    ) {
        drive(RebuildUsageStatsOperation::new(), driver_ctx).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::StorageEvent;
    use std::thread;

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
}
