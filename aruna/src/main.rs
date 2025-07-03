use anyhow::anyhow;
use aruna_data::api_s3::s3server::S3Server;
use aruna_data::config::config::Config as DataConfig;
use aruna_data::util::opendal::get_operator;
use aruna_data::{ACCESS_DB_NAME, IOHandler, LOCATION_DB_NAME, PATH_LOCATION_DB_NAME};
use aruna_metadata::{
    api::server::RestServer,
    error::ArunaMetadataError,
    network::network_trait::{Network, NetworkConfig, P2PNetwork},
    persistence::{
        persistor::{Persistor, tables::*},
        search::tantivy::{TantivyConfig, TantivySearch},
    },
    transactions::controller::Controller,
};
use aruna_permission::{
    PermissionError, PermissionManager, TokenSystem,
    token::Issuer,
};
use aruna_storage::storage::{
    lmdb::{LmdbConfig, LmdbStore},
    store::Store,
};
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::DecodePrivateKey;
use futures_util::TryFutureExt;
use iroh::KeyParsingError;
use iroh::NodeAddr;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::trace::SdkTracerProvider;
use parking_lot::RwLock;
use std::{
    net::{Ipv4Addr, SocketAddrV4},
    num::ParseIntError,
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use thiserror::Error;
use tokio::try_join;
use tracing::{debug, error, trace};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::prelude::*;

#[derive(Debug, Error)]
pub enum ArunaError {
    #[error("ArunaMetadataError: {0}")]
    MetadataError(#[from] ArunaMetadataError),
    #[error("DotenvyError: {0}")]
    DotenvyError(#[from] dotenvy::Error),
    #[error("DataError: {0}")]
    DataError(#[from] anyhow::Error),
    #[error("KeyParsingError: {0}")]
    KeyParsingError(#[from] KeyParsingError),
    #[error("ParseIntError: {0}")]
    ParseIntError(#[from] ParseIntError),
    #[error("SigningKeyError: {0}")]
    SigningKeyError(#[from] ed25519_dalek::pkcs8::Error),
    #[error("PermissionError: {0}")]
    PermissionError(#[from] PermissionError),
    #[error("SerdeError: {0}")]
    SerdeError(#[from] serde_json::error::Error),
}

#[tokio::main]
pub async fn main() {
    // parse config
    let config: Config = parse_config().unwrap();

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
        .tracer("aruna");

    let tracing_env_filter = EnvFilter::try_from_default_env()
        .unwrap_or("none".into())
        .add_directive("aruna_metadata=trace".parse().unwrap())
        .add_directive("aruna_storage=trace".parse().unwrap())
        .add_directive("aruna_net=trace".parse().unwrap())
        .add_directive("aruna_realm=trace".parse().unwrap())
        .add_directive("aruna_permission=trace".parse().unwrap());

    let logging_env_filter = EnvFilter::try_from_default_env()
        .unwrap_or("none".into())
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
        PATH_LOCATION_DB_NAME,
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

    let metadata_future = start_metadata(
        config.clone(),
        store.clone(),
        network.clone(),
        permission_manager.clone(),
        token_handler.clone(),
    );
    let data_future = start_data(config, store, network, permission_manager, token_handler);

    let (metadata, data) = tokio::join!(metadata_future, data_future);
    metadata.unwrap();
    data.unwrap();
}

#[derive(Clone, Debug)]
pub struct Config {
    pub bootstrap_nodes: Vec<NodeAddr>,
    pub path: String,
    pub issuers: Vec<Issuer>,
    pub realm_key: SigningKey,
    pub search_config: TantivyConfig,
    pub config: DataConfig,
}

pub async fn start_data(
    config: Config,
    store: LmdbStore,
    network: Arc<P2PNetwork>,
    perm_manager: PermissionManager,
    token_handler: Arc<RwLock<TokenSystem>>,
) -> Result<(), ArunaError> {
    let op_conf = config.config.backend.access.clone();
    let operator = get_operator(&config.config.backend.backend_type, op_conf).await?;
    match operator.check().await {
        Ok(_) => debug!(
            "Connection to {} backend succeeded",
            config.config.backend.backend_type
        ),
        Err(err) => {
            error!("Connection to backend failed: {}", err);
            //anyhow::bail!("Connection to backend failed: {}", err);
        }
    }

    let kademlia = network.chandler.get_kademlia_actor_handle().await?;
    let node_addr = network.chandler.get_node_addr().await?;

    // Create and run IOHandler
    let io_handler = IOHandler::<LmdbStore>::new(
        node_addr.clone(),
        operator,
        network.chandler.clone(),
        kademlia,
        Arc::new(store),
        perm_manager.clone(),
        config.config.general.realm_key.to_bytes(),
    )
    .await?;
    let s3server = S3Server::new(
        config.config.frontend.s3_frontend.clone(),
        io_handler.clone(),
        perm_manager.clone(),
        node_addr.node_id,
        config.config.general.realm_key.to_bytes(),
    )
    .await?;

    let controller = Arc::new(aruna_data::io::controller::Controller::<LmdbStore>::new(
        io_handler,
        perm_manager,
        token_handler,
    ));
    let rest_handle = tokio::spawn(async move {
        aruna_data::api_json::server::RestServer::run(
            controller,
            Ipv4Addr::from_str(&config.config.frontend.openapi_frontend.address)?,
            8081, //config.config.frontend.openapi_frontend.port,
        )
        .await
    })
    .map_err(|e| {
        error!(error = ?e, msg = e.to_string());
        anyhow!("an error occurred {e}")
    });

    match try_join!(s3server.run(), rest_handle) {
        Ok(_) => Ok(()),
        Err(err) => {
            error!("{}", err);
            panic!("Meh.")
        }
    }
}

pub async fn start_metadata<S>(
    config: Config,
    store: S,
    network: Arc<P2PNetwork>,
    perm_manager: PermissionManager,
    token_handler: Arc<RwLock<TokenSystem>>,
) -> Result<(), ArunaError>
where
    for<'a> S: Store<'a> + 'static,
{
    let persistor: Arc<Persistor<S, TantivySearch>> =
        Arc::new(Persistor::new(store, config.search_config, perm_manager, token_handler).await?);

    let controller = Arc::new(Controller::<S, TantivySearch, P2PNetwork>::new(
        persistor,
        network.clone(),
    ));
    Network::start_actor(network, controller.clone()).await?;

    let controller_clone = controller.clone();
    tokio::spawn(async move {
        RestServer::run(
            controller_clone,
            config.config.frontend.openapi_frontend.port,
        )
        .await
    });

    Ok(())
}

pub fn parse_config() -> Result<Config, ArunaError> {
    if let Ok(file) = dotenvy::var("ENV") {
        dotenvy::from_filename_override(file)?;
    }

    let path = dotenvy::var("DB_PATH")?;
    let key = &dotenvy::var("REALM_KEY")?;
    let signing_key = SigningKey::from_pkcs8_pem(key)?;

    let issuers = serde_json::from_str(&dotenvy::var("ISSUERS")?)?;
    let bootstrap_nodes: Vec<NodeAddr> = match dotenvy::var("BOOTSTRAP_NODES") {
        Ok(env_var) => serde_json::from_str(&env_var)?,
        Err(_) => vec![],
    };

    let tantivy_path = format!("{path}/tantivy");
    let search_config = TantivyConfig {
        path: tantivy_path,
        index_buffer: 1_000_000_000,
    };

    let data_config = DataConfig::load_from_env()?;

    Ok(Config {
        bootstrap_nodes,
        path,
        issuers,
        search_config,
        realm_key: signing_key,
        config: data_config,
    })
}
