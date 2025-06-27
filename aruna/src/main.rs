use aruna_metadata::{
    api::server::RestServer,
    network::network_trait::{Network, NetworkConfig, P2PNetwork},
    persistence::{
        persistence::{Persistor, tables::*},
        search::tantivy::{TantivyConfig, TantivySearch},
    },
    transactions::controller::Controller,
};
use aruna_permission::token::OidcTrustConfig;
use aruna_storage::storage::{
    lmdb::{LmdbConfig, LmdbStore},
    store::Store,
};
use ed25519_dalek::SigningKey;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::prelude::*;
use std::{
    net::{Ipv4Addr, SocketAddrV4},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

#[tokio::main]
pub async fn main() {
    // Init tracing
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

    // parse config
    let config: Config = todo!();

    let databases = vec![
        aruna_permission::DBNAME,
        aruna_permission::RESOURCE_DB,
        aruna_permission::OIDC_IDENTITIES_DB,
        aruna_permission::IDENTITY_PERMISSIONS_DB,
        RESOURCE_DB_NAME,
        RESOURCE_MAPPINGS_DB_NAME,
        USER_DB_NAME,
        GROUPS_DB_NAME,
        GROUPS_MAPPINGS_DB_NAME,
        PUBLIC_MAPPINGS_DB_NAME,
    ];

    let store_path = format!("{}/heed", config.path);
    let store_config = LmdbConfig {
        path: store_path,
        databases: databases.clone(),
    };

    let store = LmdbStore::new(store_config).unwrap();

    start_metadata(config, store).await;
}

pub struct Config {
    pub path: String,
    pub oidc_trust_config: OidcTrustConfig,
    pub token_handler_realm_keys: String,
    pub realm_key: SigningKey,
    pub p2p_address: String,
    pub p2p_port: u16,
    pub api_port: u16,
}

pub async fn start_metadata<S>(config: Config, store: S)
where
    for<'a> S: Store<'a> + 'static,
{
    let (res_sdx, res_rcv) = tokio::sync::mpsc::channel(1000);
    let tantivy_path = format!("{}/tantivy", config.path);
    let search_config = TantivyConfig {
        path: tantivy_path,
        index_buffer: 1_000_000_000,
        resources: res_rcv,
    };

    let persistor: Arc<Persistor<S, TantivySearch>> = Arc::new(
        Persistor::new(
            res_sdx,
            store,
            search_config,
            config.realm_key.verifying_key().to_bytes().clone(),
            config.oidc_trust_config,
        )
        .await
        .unwrap(),
    );

    persistor.token_handler.write().add_realm_public_key(
        config.realm_key.verifying_key().to_bytes().clone(),
        config.token_handler_realm_keys,
    );

    let network = Arc::new(
        P2PNetwork::new(NetworkConfig {
            secret_key: None,
            socket_addr: SocketAddrV4::new(
                Ipv4Addr::from_str(&config.p2p_address).unwrap(),
                config.p2p_port,
            ),
            bootstrap_nodes: vec![],
            realm_key: config.realm_key.clone(),
        })
        .await
        .unwrap(),
    );

    let controller = Arc::new(Controller::<S, TantivySearch, P2PNetwork>::new(
        persistor,
        network.clone(),
    ));
    Network::start_actor(network, controller.clone())
        .await
        .unwrap();

    let controller_clone = controller.clone();
    tokio::spawn(async move { RestServer::run(controller_clone, config.api_port).await });

    controller.network.get_addr().await.unwrap();
}
