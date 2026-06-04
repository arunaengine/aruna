#![allow(clippy::result_large_err)]

use aruna::bootstrap::{
    announce_core_documents, ensure_initial_local_onboarding_secret,
    fetch_core_onboarding_documents, realm_bootstrap_exists,
};
use aruna::config::{StartupMode, load, mark_node_state_complete, mark_onboarding_phase};
use aruna::telemetry::{init_tracing, shutdown_tracing};
use aruna_api::auth::OidcValidator;
use aruna_api::s3::s3_server::S3Server;
use aruna_api::server::{Server, ServerConfig};
use aruna_api::server_state::ServerState;
use aruna_blob::blob::BlobHandler;
use aruna_core::UserId;
use aruna_core::onboarding::OnboardingPhase;
use aruna_core::structs::Actor;
use aruna_core::structs::Backend::FileSystem;
use aruna_core::structs::BackendConfig;
use aruna_core::structs::NodeCapabilities;
use aruna_net::{NetConfig, NetHandle};
use aruna_operations::announce_realm_presence::{
    AnnounceRealmPresenceConfig, AnnounceRealmPresenceOperation,
};
use aruna_operations::create_realm::{CreateRealmConfig, CreateRealmOperation};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::ensure_realm_config::{EnsureRealmConfigConfig, EnsureRealmConfigOperation};
use aruna_operations::incoming::initialize_net_incoming;
use aruna_operations::metadata::MetadataHandle;
use aruna_operations::process_placements::{PlacementConfig, ProcessPlacementsOperation};
use aruna_operations::startup::RestoreTopicSubscriptionsOperation;
use aruna_operations::task_incoming::initialize_task_incoming;
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
            irokle_storage_path: Some(config.irokle_storage_path.clone()),
            irokle_runtime: Some(config.irokle_runtime),
        },
        storage_handle.clone(),
    )
    .await?;
    if let Err(error) = net_handle.reload_realm_peers().await {
        warn!(error = %error, "Failed to refresh realm peers from persisted config during startup");
    }
    let task_handle = TaskHandle::new();
    let metadata_handle = MetadataHandle::new(
        &config.metadata_storage_path,
        config.node_id,
        storage_handle.clone(),
        Some(net_handle.clone()),
        Some(net_handle.irokle_node()),
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
                        bootstrap_peers: config.peer_nodes.clone(),
                        default_metadata_replication_factor: config
                            .default_metadata_replication_factor,
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

    let server_config = ServerConfig {
        http_addr: config.http_socket_addr,
    };
    let server = Server::new(state.clone(), server_config);

    // S3 Server
    let s3_server = S3Server::new(
        &config.s3_address,
        &config.s3_host,
        driver_ctx,
        config.realm_id,
        config.node_id,
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

    Ok(())
}
