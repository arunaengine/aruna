use aruna::config::read_config;
use aruna_api::server::{Server, ServerConfig};
use aruna_api::server_state::ServerState;
use aruna_net::{NetConfig, NetHandle};
use aruna_operations::automerge::AutomergeHandle;
use aruna_operations::driver::{DriverContext, drive};
use aruna_storage::storage;
use aruna_tasks::TaskHandle;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    let config = read_config().unwrap();
    let storage_handle = storage::FjallStorage::open(&config.storage_path).unwrap();
    let net_handle = NetHandle::new(
        NetConfig {
            bind_addr: config.p2p_socket_addr,
            secret_key: Some(config.net_secret_key.clone()),
            bootstrap_nodes: config.bootstrap_nodes.clone(),
            use_dns_discovery: false,
        },
        storage_handle.clone(),
    )
    .await
    .unwrap();
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
    if let Err(err) = drive(
        aruna_operations::startup::RestoreAutomergeSubscriptionsOperation::new(),
        driver_ctx.as_ref(),
    )
    .await
    {
        eprintln!("failed to restore automerge subscriptions: {err}");
    }

    let state = Arc::new(ServerState::new(
        driver_ctx,
        config.realm_id,
        config.node_id,
        config.node_capabilities,
        None,
    ));

    let config = ServerConfig {
        http_addr: config.http_socket_addr,
    };
    let server = Server::new(state, config);
    if let Err(err) = server.run().await {
        eprintln!("{}", err);
    }
}
