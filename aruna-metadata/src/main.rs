use crate::network::network_trait::NetworkConfig;
use crate::persistence::persistence::Persistor;
use api::server::RestServer;
use network::network_trait::Network;
use network::network_trait::P2PNetwork;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::trace::SdkTracerProvider;
use persistence::search::tantivy::TantivyConfig;
use persistence::search::tantivy::TantivySearch;
use persistence::storage::fjall::FjallStore;
use persistence::storage::fjall::FjallConfig;
use persistence::storage::lmdb::LmdbConfig;
use persistence::storage::lmdb::LmdbStore;
use persistence::storage::redb::Redb;
use persistence::storage::redb::RedbConfig;
use std::net::Ipv4Addr;
use std::net::SocketAddrV4;
use std::sync::Arc;
use std::time::Duration;
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
        .add_directive("aruna_server=trace".parse().unwrap())
        .add_directive("tower_http=debug".parse().unwrap())
        .add_directive("synevi_core=trace".parse().unwrap());

    let logging_env_filter = EnvFilter::try_from_default_env()
        .unwrap_or("none".into())
        .add_directive("aruna_server=info".parse().unwrap())
        .add_directive("tower_http=info".parse().unwrap())
        .add_directive("synevi_core=info".parse().unwrap());

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

    let path = dotenvy::var("DBPATH").unwrap();
    let port = dotenvy::var("PORT").unwrap().parse().unwrap();
    let variant = dotenvy::var("VARIANT").unwrap();

    println!(
        "{path}
{port}
{variant}
"
    );

    let (res_sdx, res_rcv) = tokio::sync::mpsc::channel(1000);
    let (idx_sdx, idx_rcv) = tokio::sync::oneshot::channel();
    let tantivy_path = format!("{path}/tantivy");
    let search_config = TantivyConfig {
        path: tantivy_path,
        index_buffer: 1_000_000_000,
        resources: res_rcv,
    };
    match variant.as_ref() {
        "LMDB" => {
            let store_path = format!("{path}/heed");
            let store_config = LmdbConfig {
                path: store_path,
                res_sdx,
                idx_sdx,
            };
            let persistor: Arc<Persistor<LmdbStore, TantivySearch>> = Arc::new(
                Persistor::new(idx_rcv, store_config, search_config)
                    .await
                    .unwrap(),
            );
            let network = P2PNetwork::<LmdbStore, TantivySearch>::new(NetworkConfig::<
                LmdbStore,
                TantivySearch,
            > {
                secret_key: None,
                socket_addr: SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 8080),
                bootstrap_nodes: vec![],
                persistor: persistor.clone(),
            })
            .await;
            //let network =
            //    NetworkDummy::<LmdbTantivyPersistence, LmdbStore, TantivySearch>::new(()).await;
            let controller = Arc::new(Controller::<
                LmdbStore,
                TantivySearch,
                P2PNetwork<LmdbStore, TantivySearch>,
            >::new(persistor, network));

            tokio::spawn(async move { RestServer::run(controller, port).await })
                .await
                .unwrap()
                .unwrap();
        }
        "REDB" => {
            let store_path = format!("{path}/redb");
            let store_config = RedbConfig {
                path: store_path,
                res_sdx,
                idx_sdx,
            };

            let persistor = Arc::new(
                Persistor::new(idx_rcv, store_config, search_config)
                    .await
                    .unwrap(),
            );

            let network =
                P2PNetwork::<Redb, TantivySearch>::new(NetworkConfig::<Redb, TantivySearch> {
                    secret_key: None,
                    socket_addr: SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 8080),
                    bootstrap_nodes: vec![],
                    persistor: persistor.clone(),
                })
                .await;
            //let network =
            //    NetworkDummy::<RedbTantivyPersistence, Redb, TantivySearch>::new(()).await;
            let controller = Arc::new(Controller::<
                Redb,
                TantivySearch,
                P2PNetwork<Redb, TantivySearch>,
            >::new(persistor, network));

            tokio::spawn(async move { RestServer::run(controller, port).await })
                .await
                .unwrap()
                .unwrap();
        }
        "FJALL" => {
            let store_path = format!("{path}/fjall");
            let store_config = FjallConfig {
                path: store_path,
                res_sdx,
                idx_sdx,
            };
            let persistor = Arc::new(
                Persistor::new(idx_rcv, store_config, search_config)
                    .await
                    .unwrap(),
            );
            let network = P2PNetwork::<FjallStore, TantivySearch>::new(NetworkConfig::<
                FjallStore,
                TantivySearch,
            > {
                secret_key: None,
                socket_addr: SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 8080),
                bootstrap_nodes: vec![],
                persistor: persistor.clone(),
            })
            .await;

            let controller = Arc::new(Controller::<
                FjallStore,
                TantivySearch,
                P2PNetwork<FjallStore, TantivySearch>,
            >::new(persistor, network));

            tokio::spawn(async move { RestServer::run(controller, port).await })
                .await
                .unwrap()
                .unwrap();
        }
        _ => panic!("Invalid variant selected"),
    }
}
