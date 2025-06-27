use aruna_metadata::{
    api::server::RestServer,
    error::ArunaMetadataError,
    models::models::Resource,
    network::network_trait::{Network, NetworkConfig, P2PNetwork},
    persistence::{
        persistence::{Persistor, tables::*},
        search::tantivy::{TantivyConfig, TantivySearch},
    },
    transactions::controller::Controller,
};
use aruna_permission::{
    PermissionError,
    token::{Ed25519KeyPair, OidcTrustConfig},
};
use aruna_storage::storage::{
    lmdb::{LmdbConfig, LmdbStore},
    store::Store,
};
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::DecodePrivateKey;
use iroh::KeyParsingError;
use iroh::NodeAddr;
use iroh::PublicKey;
use iroh::SecretKey;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::trace::SdkTracerProvider;
use std::{
    net::{Ipv4Addr, SocketAddrV4},
    num::ParseIntError,
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use thiserror::Error;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::prelude::*;

#[derive(Debug, Error)]
pub enum ArunaError {
    #[error("ArunaMetadataError: {0}")]
    MetadataError(#[from] ArunaMetadataError),
    #[error("DotenvyError: {0}")]
    DotenvyError(#[from] dotenvy::Error),
    #[error("KeyParsingError: {0}")]
    KeyParsingError(#[from] KeyParsingError),
    #[error("ParseIntError: {0}")]
    ParseIntError(#[from] ParseIntError),
    #[error("SigningKeyError: {0}")]
    SigningKeyError(#[from] ed25519_dalek::pkcs8::Error),
    #[error("PermissionError: {0}")]
    PermissionError(#[from] PermissionError),
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

    let metadata_future = start_metadata(config, store);
    let data_future = async move { todo!() };

    tokio::join!(metadata_future, data_future);
}

pub struct Config {
    pub bootstrap_nodes: Vec<NodeAddr>,
    pub path: String,
    pub oidc_trust_config: OidcTrustConfig,
    pub token_handler_realm_keys: Ed25519KeyPair,
    pub realm_key: SigningKey,
    pub p2p_address: String,
    pub p2p_port: u16,
    pub p2p_secret_key: Option<SecretKey>,
    pub api_port: u16,
    pub search_config: TantivyConfig,
    pub resource_to_search_sender: tokio::sync::mpsc::Sender<(u32, Resource)>,
}

pub async fn start_metadata<S>(config: Config, store: S) -> Result<(), ArunaError>
where
    for<'a> S: Store<'a> + 'static,
{
    let persistor: Arc<Persistor<S, TantivySearch>> = Arc::new(
        Persistor::new(
            config.resource_to_search_sender,
            store,
            config.search_config,
            config.realm_key.verifying_key().to_bytes().clone(),
            config.oidc_trust_config,
        )
        .await?,
    );

    persistor.token_handler.write().add_realm_public_key(
        config.realm_key.verifying_key().to_bytes().clone(),
        config.token_handler_realm_keys.verifying_key_pem()?,
    );

    let network = Arc::new(
        P2PNetwork::new(NetworkConfig {
            secret_key: config.p2p_secret_key,
            socket_addr: SocketAddrV4::new(
                Ipv4Addr::from_str(&config.p2p_address)
                    .map_err(|e| ArunaMetadataError::ConfigError(e.to_string()))?,
                config.p2p_port,
            ),
            bootstrap_nodes: config.bootstrap_nodes,
            realm_key: config.realm_key.clone(),
        })
        .await?,
    );

    let controller = Arc::new(Controller::<S, TantivySearch, P2PNetwork>::new(
        persistor,
        network.clone(),
    ));
    Network::start_actor(network, controller.clone()).await?;

    let controller_clone = controller.clone();
    tokio::spawn(async move { RestServer::run(controller_clone, config.api_port).await });

    Ok(())
}

pub fn parse_config() -> Result<Config, ArunaError> {
    let path = dotenvy::var("DBPATH")?;
    let api_port = dotenvy::var("API_PORT")?.parse()?;
    let p2p_port = dotenvy::var("P2P_PORT")?.parse()?;
    let p2p_address = dotenvy::var("P2P_ADDRESS")?;
    let p2p_secret_key = if let Ok(key) = dotenvy::var("P2P_SECRET_KEY") {
        if key.is_empty() {
            None
        } else {
            Some(SecretKey::from_str(&key)?)
        }
    } else {
        None
    };
    let realm_key = SigningKey::from_pkcs8_pem(&dotenvy::var("REALM_KEY")?)?;
    // TODO: Parse from config
    let signing_key = SigningKey::from_pkcs8_pem(&dotenvy::var("TOKEN_SIGNING_KEY")?)?;
    let verifying_key = signing_key.verifying_key();
    let token_handler_realm_keys = Ed25519KeyPair {
        signing_key,
        verifying_key,
    };

    let oidc_trust_config = OidcTrustConfig::TrustAll;
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

    let (res_sdx, res_rcv) = tokio::sync::mpsc::channel(1000);
    let tantivy_path = format!("{path}/tantivy");
    let search_config = TantivyConfig {
        path: tantivy_path,
        index_buffer: 1_000_000_000,
        resources: res_rcv,
    };

    Ok(Config {
        bootstrap_nodes,
        path,
        oidc_trust_config,
        realm_key,
        p2p_port,
        api_port,
        p2p_address,
        p2p_secret_key,
        search_config,
        resource_to_search_sender: res_sdx,
        token_handler_realm_keys,
    })
}
