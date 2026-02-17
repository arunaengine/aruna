use aruna::config::read_config;
use aruna_api::server::{Server, ServerConfig};
use aruna_api::server_state::ServerState;
use aruna_operations::driver::DriverContext;
use aruna_storage::storage;
use std::sync::Arc;


#[tokio::main]
async fn main() {
    let config = read_config().unwrap();
    let storage_handle = storage::FjallStorage::open(&config.storage_path).unwrap();

    let driver_ctx = Arc::new(DriverContext {
        storage_handle,
        net_handle: None,
    });
    let state = Arc::new(ServerState::new(
        driver_ctx,
        Some(config.realm_keypair),
        Some(config.realm_id),
        None,
    ));

    let config = ServerConfig {
        http_addr: config.socket_addr,
    };
    let server = Server::new(state, config);
    if let Err(err) = server.run().await {
        eprintln!("{}", err);
    }
}
