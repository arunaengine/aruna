use aruna_api::s3::s3_server::S3Server;
use aruna_blob::blob::BlobHandler;
use aruna_core::structs::Backend::FileSystem;
use aruna_core::structs::BackendConfig;
use aruna::config::read_config;
use aruna_api::server::{Server, ServerConfig};
use aruna_api::server_state::ServerState;
use aruna_operations::driver::DriverContext;
use aruna_storage::FjallStorage;
use std::backtrace::Backtrace;
use std::collections::HashMap;
use std::panic;
use aruna_storage::storage;
use std::sync::Arc;
use tracing::{error, info};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};
use ulid::Ulid;

#[tokio::main]
async fn main() {
    panic::set_hook(Box::new(|info| {
        let stacktrace = Backtrace::force_capture();
        println!("Got panic. @info:{info}\n@stackTrace:{stacktrace}");
        std::process::abort();
    }));

    let logging_env_filter = EnvFilter::try_from_default_env()
        .unwrap_or("none".into())
        .add_directive("aruna_api=trace".parse().unwrap());

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_file(true)
        .with_line_number(true)
        .with_target(false)
        .with_filter(logging_env_filter);

    tracing_subscriber::registry().with(fmt_layer).init();
    
    let config = read_config().unwrap();
    let storage_handle = storage::FjallStorage::open(&config.storage_path).unwrap();
    let blob_handle = BlobHandler::new(
        BackendConfig {
            backend_type: FileSystem,
            root: "/tmp".to_string(),
            service_config: HashMap::new(),
            bucket_prefix: None,
            max_bucket_size: Some(100000),
        },
        storage_handle.clone(),
    )
        .await
        .unwrap();
    
    let driver_ctx = Arc::new(DriverContext {
        storage_handle,
        net_handle: None,
        blob_handle: Some(blob_handle),
    });
    
    // REST Server
    let state = Arc::new(ServerState::new(
        driver_ctx,
        config.realm_id,
        config.node_id,
        config.node_capabilities,
        None,
    ));

    let config = ServerConfig {
        http_addr: config.socket_addr,
    };
    let server = Server::new(state, config);
    
    // S3 Server
    let s3_server = S3Server::new("0.0.0.0:1337", "localhost:1337", driver_ctx)
        .await
        .unwrap();

    let server_handle = s3_server.run().await.unwrap();

    tokio::select! {
        res = server_handle => {
            match res {
                Ok(_) => info!("S3 Server stopped normally"),
                Err(e) => error!("S3 Server panicked: {:?}", e),
            }
        }
        res = server.run() => {
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
}
