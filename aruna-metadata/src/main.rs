use crate::network::network_trait::NetworkConfig;
use crate::persistence::persistence::Persistor;
use crate::persistence::persistence::tables::*;
use api::server::RestServer;
use aruna_storage::storage::fjall::FjallConfig;
use aruna_storage::storage::fjall::FjallStore;
use aruna_storage::storage::lmdb::LmdbConfig;
use aruna_storage::storage::lmdb::LmdbStore;
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::DecodePrivateKey;
use iroh::KeyParsingError;
use iroh::NodeAddr;
use iroh::PublicKey;
use iroh::SecretKey;
use network::network_trait::Network;
use network::network_trait::P2PNetwork;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::trace::SdkTracerProvider;
use persistence::search::tantivy::TantivyConfig;
use persistence::search::tantivy::TantivySearch;
use std::net::Ipv4Addr;
use std::net::SocketAddrV4;
use std::str::FromStr;
use std::sync::Arc;
use tracing::{error, trace};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::prelude::*;
use transactions::controller::Controller;

mod api;
mod error;
mod models;
mod network;
mod persistence;
mod transactions;

#[tokio::main]
async fn main() {
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint("http://localhost:4317")
        .with_timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .build()
        .tracer("aruna1");
    let tracing_env_filter = EnvFilter::try_from_default_env()
        .unwrap_or("none".into())
        .add_directive("aruna_metadata=trace".parse().unwrap())
        .add_directive("tower_http=debug".parse().unwrap())
        .add_directive("aruna_net=trace".parse().unwrap());

    let logging_env_filter = EnvFilter::try_from_default_env()
        .unwrap_or("none".into())
        .add_directive("aruna_metadata=trace".parse().unwrap())
        .add_directive("aruna_storage=trace".parse().unwrap())
        .add_directive("tower_http=info".parse().unwrap())
        .add_directive("aruna_net=info".parse().unwrap());

    let telemetry_layer = tracing_opentelemetry::layer()
        .with_tracer(provider)
        .with_filter(tracing_env_filter);

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_file(true)
        .with_line_number(true)
        .with_filter(logging_env_filter);

    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(telemetry_layer)
        .init();
    if let Ok(file) = dotenvy::var("ENV") {
        dotenvy::from_filename_override(file).unwrap();
    }
    let path = dotenvy::var("DBPATH").unwrap();
    let api_port = dotenvy::var("API_PORT").unwrap().parse().unwrap();
    let p2p_port = dotenvy::var("P2P_PORT").unwrap().parse().unwrap();
    let p2p_secret_key = if let Ok(key) = dotenvy::var("P2P_SECRET_KEY") {
        if key.is_empty() {
            None
        } else {
            Some(SecretKey::from_str(&key).unwrap())
        }
    } else {
        None
    };
    let realm_key = if let Ok(key) = dotenvy::var("REALM_KEY") {
        if key.is_empty() {
            None
        } else {
            Some(SigningKey::from_pkcs8_pem(&key).unwrap())
        }
    } else {
        None
    };
    let bootstrap_nodes: Vec<NodeAddr> = match dotenvy::var("BOOTSTRAP_NODES") {
        Ok(env_var) => env_var
            .split(";")
            .map(|key| {
                Ok(NodeAddr::from_parts(
                    PublicKey::from_str(key)?,
                    None,
                    Some(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 1230).into()),
                ))
            })
            .collect::<Result<Vec<NodeAddr>, KeyParsingError>>()
            .unwrap(),
        Err(_) => vec![],
    };
    let variant = dotenvy::var("VARIANT").unwrap();

    trace!(
        "DB_VARIANT: {variant} on {path} API: {api_port} P2P: {p2p_port} P2P_KEY: {p2p_secret_key:?} BOOTSTRAP_NODES: {bootstrap_nodes:?}",
    );

    let (res_sdx, res_rcv) = tokio::sync::mpsc::channel(1000);
    let tantivy_path = format!("{path}/tantivy");
    let search_config = TantivyConfig {
        path: tantivy_path,
        index_buffer: 1_000_000_000,
        resources: res_rcv,
    };

    use parking_lot::deadlock;
    use std::time::Duration;

    // Create a background thread which checks for deadlocks every 10s
    std::thread::spawn(move || {
        loop {
            std::thread::sleep(Duration::from_secs(10));
            let deadlocks = deadlock::check_deadlock();
            if deadlocks.is_empty() {
                continue;
            }

            error!("{} deadlocks detected", deadlocks.len());
            for (i, threads) in deadlocks.iter().enumerate() {
                trace!("Deadlock #{}", i);
                for t in threads {
                    trace!("Thread Id {:#?}", t.thread_id());
                    trace!("{:#?}", t.backtrace());
                }
            }
            panic!("Deadlock detected");
        }
    });

    match variant.as_ref() {
        "LMDB" => {
            let store_path = format!("{path}/heed");
            let store_config = LmdbConfig {
                path: store_path,
                databases: vec![
                    RESOURCE_DB_NAME,
                    RESOURCE_MAPPINGS_DB_NAME,
                    USER_DB_NAME,
                    USER_MAPPINGS_DB_NAME,
                    PUBLIC_MAPPINGS_DB_NAME,
                ],
            };
            let persistor: Arc<Persistor<LmdbStore, TantivySearch>> = Arc::new(
                Persistor::new(res_sdx, store_config, search_config)
                    .await
                    .unwrap(),
            );
            let network = Arc::new(
                P2PNetwork::new(NetworkConfig {
                    secret_key: p2p_secret_key,
                    socket_addr: SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), p2p_port),
                    bootstrap_nodes,
                    realm_key,
                })
                .await
                .unwrap(),
            );

            let controller = Arc::new(Controller::<LmdbStore, TantivySearch, P2PNetwork>::new(
                persistor,
                network.clone(),
            ));
            Network::start_actor(network, controller.clone())
                .await
                .unwrap();

            tokio::spawn(async move { RestServer::run(controller, api_port).await })
                .await
                .unwrap()
                .unwrap();
        }
        // "REDB" => {
        //     let store_path = format!("{path}/redb");
        //     let store_config = RedbConfig {
        //         path: store_path,
        //         res_sdx,
        //         idx_sdx,
        //     };

        //     let persistor = Arc::new(
        //         Persistor::new(idx_rcv, store_config, search_config)
        //             .await
        //             .unwrap(),
        //     );

        //     let network =
        //         P2PNetwork::<Redb, TantivySearch>::new(NetworkConfig::<Redb, TantivySearch> {
        //             secret_key: p2p_secret_key,
        //             socket_addr: SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), p2p_port),
        //             bootstrap_nodes,
        //             persistor: persistor.clone(),
        //         })
        //         .await;

        //     let controller = Arc::new(Controller::<
        //         Redb,
        //         TantivySearch,
        //         P2PNetwork<Redb, TantivySearch>,
        //     >::new(persistor, network));

        //     tokio::spawn(async move { RestServer::run(controller, api_port).await })
        //         .await
        //         .unwrap()
        //         .unwrap();
        // }
        "FJALL" => {
            let store_path = format!("{path}/fjall");
            let store_config = FjallConfig {
                path: store_path,
                databases: vec![
                    RESOURCE_DB_NAME,
                    RESOURCE_MAPPINGS_DB_NAME,
                    USER_DB_NAME,
                    USER_MAPPINGS_DB_NAME,
                    PUBLIC_MAPPINGS_DB_NAME,
                ],
            };
            let persistor: Arc<Persistor<FjallStore, TantivySearch>> = Arc::new(
                Persistor::new(res_sdx, store_config, search_config)
                    .await
                    .unwrap(),
            );
            let network = Arc::new(
                P2PNetwork::new(NetworkConfig {
                    secret_key: p2p_secret_key,
                    socket_addr: SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), p2p_port),
                    bootstrap_nodes,
                    realm_key,
                })
                .await
                .unwrap(),
            );
            let controller = Arc::new(Controller::<FjallStore, TantivySearch, P2PNetwork>::new(
                persistor,
                network.clone(),
            ));
            Network::start_actor(network, controller.clone())
                .await
                .unwrap();

            tokio::spawn(async move { RestServer::run(controller, api_port).await })
                .await
                .unwrap()
                .unwrap();
        }
        _ => panic!("Invalid variant selected"),
    }
}
