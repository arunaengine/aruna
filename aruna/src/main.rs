use aruna_data::{
    ACCESS_DB_NAME, BUCKET_LOCATION_DB_NAME, BUCKET_STATE_DB_NAME, LOCATION_DB_NAME,
    LOCATION_STATS_DB_NAME, PATH_LOCATION_DB_NAME, REPLICATION_RULES_DB_NAME,
};
use aruna_metadata::{
    error::ArunaMetadataError,
    network::{
        network_trait::Network,
        p2p_network::{NetworkConfig, P2PNetwork},
    },
    persistence::persistor::tables::{
        GROUPS_DB_NAME, GROUPS_MAPPINGS_DB_NAME, PUBLIC_MAPPINGS_DB_NAME, RESOURCE_DB_NAME,
        RESOURCE_MAPPINGS_DB_NAME, USER_DB_NAME,
    },
};
use aruna_permission::{PermissionManager, TokenSystem};
use aruna_storage::storage::{
    lmdb::{LmdbConfig, LmdbStore},
    store::Store,
};
use aruna_task::TaskHandler;
use config::{Config, parse_config, start_data, start_metadata};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::trace::SdkTracerProvider;
use parking_lot::RwLock;
use std::{
    net::{Ipv4Addr, SocketAddrV4},
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use tracing::trace;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::prelude::*;

pub mod config;
pub mod error;

#[tokio::main]
pub async fn main() {
    // parse config
    let config: Config = parse_config().unwrap();

    // Init tracing
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(config.otel_server.clone())
        .with_timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .build()
        .tracer(config.otel_service_name.clone());

    let tracing_env_filter = EnvFilter::try_from_default_env()
        .unwrap_or("none".into())
        .add_directive("aruna=trace".parse().unwrap())
        .add_directive("aruna_metadata=trace".parse().unwrap())
        .add_directive("aruna_storage=trace".parse().unwrap())
        .add_directive("aruna_net=trace".parse().unwrap())
        .add_directive("aruna_realm=trace".parse().unwrap())
        .add_directive("aruna_permission=trace".parse().unwrap());

    let logging_env_filter = EnvFilter::try_from_default_env()
        .unwrap_or("none".into())
        .add_directive("aruna=info".parse().unwrap())
        .add_directive("aruna_metadata=info".parse().unwrap())
        .add_directive("aruna_storage=info".parse().unwrap())
        .add_directive("aruna_net=info".parse().unwrap())
        .add_directive("aruna_realm=info".parse().unwrap())
        .add_directive("aruna_permission=info".parse().unwrap());

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

    trace!("{:?}", config);

    let databases = vec![
        ACCESS_DB_NAME,
        LOCATION_DB_NAME,
        LOCATION_STATS_DB_NAME,
        PATH_LOCATION_DB_NAME,
        BUCKET_STATE_DB_NAME,
        BUCKET_LOCATION_DB_NAME,
        REPLICATION_RULES_DB_NAME,
        aruna_task::TASK_DB_NAME,
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

    let network = Arc::new(
        P2PNetwork::new(NetworkConfig {
            secret_key: Some(config.config.general.node_key.clone()),
            socket_addr: SocketAddrV4::new(
                Ipv4Addr::from_str(&config.config.general.p2p_address)
                    .map_err(|e| ArunaMetadataError::ConfigError(e.to_string()))
                    .unwrap(),
                config.config.general.p2p_port,
            ),
            bootstrap_nodes: config.bootstrap_nodes.clone(),
            realm_key: config.config.general.realm_key.clone(),
        })
        .await
        .unwrap(),
    );

    let addr = network.get_addr().await.unwrap();
    trace!("{}", serde_json::to_string(&addr).unwrap());

    let store_path = format!("{}/heed", config.path);
    let store_config = LmdbConfig {
        path: store_path,
        databases: databases.clone(),
    };
    let store = LmdbStore::new(store_config).unwrap();

    let permission_manager = PermissionManager::new().await.unwrap();
    let read_txn = store.create_txn(false).unwrap();
    permission_manager
        .load_policies(&store, &read_txn)
        .await
        .unwrap();
    store.commit(read_txn).unwrap();

    // Token Handler
    let token_handler = Arc::new(RwLock::new(
        TokenSystem::new(
            &config.config.general.realm_key.to_bytes(),
            config.issuers.clone(),
        )
        .unwrap(),
    ));
    let task_handler = TaskHandler::new(store.clone()).await.unwrap();

    let metadata_future = start_metadata(
        config.clone(),
        store.clone(),
        network.clone(),
        permission_manager.clone(),
        token_handler.clone(),
        task_handler.clone(),
    );
    let data_future = start_data(
        config,
        store,
        network,
        permission_manager,
        token_handler,
        task_handler,
    );

    let (metadata, data) = tokio::join!(metadata_future, data_future);
    metadata.unwrap();
    data.unwrap();
}
