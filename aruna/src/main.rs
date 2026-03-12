use aruna::config::read_config;
use aruna_api::server::{Server, ServerConfig};
use aruna_api::server_state::ServerState;
use aruna_net::{NetConfig, NetHandle};
use aruna_operations::announce_realm_presence::{
    AnnounceRealmPresenceConfig, AnnounceRealmPresenceOperation,
};
use aruna_operations::automerge::AutomergeHandle;
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::ensure_realm_config::{EnsureRealmConfigConfig, EnsureRealmConfigOperation};
use aruna_storage::storage;
use aruna_tasks::TaskHandle;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;
use ulid::Ulid;

#[tokio::main]
async fn main() {
    init_tracing();

    if let Err(err) = run().await {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .try_init();
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let config = read_config()?;
    let storage_handle = storage::FjallStorage::open(&config.storage_path)?;
    let net_handle = NetHandle::new(
        NetConfig {
            bind_addr: config.p2p_socket_addr,
            secret_key: Some(config.net_secret_key.clone()),
            bootstrap_nodes: config.bootstrap_nodes.clone(),
            use_dns_discovery: false,
        },
        storage_handle.clone(),
    )
    .await?;
    let task_handle = TaskHandle::new();
    let automerge_handle = AutomergeHandle::new(Some(net_handle.clone()));

    let driver_ctx = Arc::new(DriverContext {
        storage_handle,
        net_handle: Some(net_handle.clone()),
        automerge_handle: Some(automerge_handle),
        task_handle: Some(task_handle.clone()),
    });

    aruna_operations::incoming::initialize_net_incoming(driver_ctx.clone());
    aruna_operations::task_incoming::initialize_task_incoming(driver_ctx.clone(), task_handle)
        .await;
    drive(
        aruna_operations::startup::RestoreAutomergeSubscriptionsOperation::new(),
        driver_ctx.as_ref(),
    )
    .await?;
    drive(
        EnsureRealmConfigOperation::new(EnsureRealmConfigConfig {
            actor: aruna_core::structs::Actor {
                node_id: config.node_id,
                user_id: Ulid::from_bytes([0u8; 16]),
                realm_id: config.realm_id.clone(),
            },
            bootstrap_peers: config.bootstrap_nodes.clone(),
            default_metadata_replication_factor: config.default_metadata_replication_factor,
        }),
        driver_ctx.as_ref(),
    )
    .await?;
    drive(
        AnnounceRealmPresenceOperation::new(AnnounceRealmPresenceConfig {
            realm_id: config.realm_id.clone(),
            node_id: config.node_id,
            schedule_refresh: true,
        }),
        driver_ctx.as_ref(),
    )
    .await?;

    let state = Arc::new(
        ServerState::new(
            driver_ctx,
            config.realm_id,
            config.node_id,
            config.node_capabilities,
            None,
        )
        .await,
    );

    let config = ServerConfig {
        http_addr: config.http_socket_addr,
    };
    let server = Server::new(state, config);
    server.run().await?;

    Ok(())
}
