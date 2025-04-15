use crate::persistence::persistence::Persistor;
use api::server::RestServer;
use network::network_trait::Network;
use network::network_trait::NetworkDummy;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::trace::TracerProvider;
use persistence::persistors::fjall_persistor::FjallTantivyPersistance;
use persistence::persistors::lmdb_persistor::LmdbTantivyPersistance;
use persistence::persistors::redb_persistor::RedbTantivyPersistance;
use persistence::search::tantivy::TantivySearch;
use persistence::storage::fjall::FjallStore;
use persistence::storage::lmdb::LmdbStore;
use persistence::storage::redb::Redb;
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

    let provider = TracerProvider::builder()
        .with_batch_exporter(exporter, opentelemetry_sdk::runtime::Tokio)
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

    let network = NetworkDummy::new(()).await;
    match variant.as_ref() {
        "LMDB" => {
            let persistor = LmdbTantivyPersistance::new(path).await.unwrap();
            let controller = Arc::new(Controller::<
                LmdbStore,
                TantivySearch,
                NetworkDummy,
                LmdbTantivyPersistance,
            >::new(persistor, network));

            tokio::spawn(async move { RestServer::run(controller, port).await })
                .await
                .unwrap()
                .unwrap();
        }
        "REDB" => {
            let persistor = RedbTantivyPersistance::new(path).await.unwrap();
            let controller = Arc::new(Controller::<
                Redb,
                TantivySearch,
                NetworkDummy,
                RedbTantivyPersistance,
            >::new(persistor, network));

            tokio::spawn(async move { RestServer::run(controller, port).await })
                .await
                .unwrap()
                .unwrap();
        }
        "FJALL" => {
            let persistor = FjallTantivyPersistance::new(path).await.unwrap();
            let controller = Arc::new(Controller::<
                FjallStore,
                TantivySearch,
                NetworkDummy,
                FjallTantivyPersistance,
            >::new(persistor, network));

            tokio::spawn(async move { RestServer::run(controller, port).await })
                .await
                .unwrap()
                .unwrap();
        }
        _ => panic!("Invalid variant selected"),
    }
}
