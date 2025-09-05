use aruna_data::api_s3::s3server::S3Server;
use aruna_data::config::config::Config as DataConfig;
use aruna_data::error::ArunaDataError;
use aruna_data::{IOHandler, network::network_handler::NetworkHandler};
use aruna_metadata::{
    api::server::RestServer,
    network::{network_trait::Network, p2p_network::P2PNetwork},
    persistence::{
        persistor::Persistor,
        search::tantivy::{TantivyConfig, TantivySearch},
    },
    transactions::controller::Controller,
};
use aruna_permission::{PermissionManager, TokenSystem, token::Issuer};
use aruna_storage::storage::{lmdb::LmdbStore, store::Store};
use aruna_task::TaskHandler;
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::DecodePrivateKey;
use futures_util::TryFutureExt;
use iroh::NodeAddr;
use parking_lot::RwLock;
use std::{net::Ipv4Addr, str::FromStr, sync::Arc};
use tokio::try_join;
use tracing::error;

use crate::error::ArunaError;

#[derive(Clone, Debug)]
pub struct Config {
    pub metadata_openapi_address: Ipv4Addr,
    pub metadata_openapi_port: u16,
    pub otel_server: String,
    pub otel_service_name: String,
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
    task_handler: TaskHandler<LmdbStore>,
) -> Result<(), ArunaError> {
    // Create and run IOHandler
    let io_handler = IOHandler::<LmdbStore>::new(
        config.config.backend.clone(),
        store,
        perm_manager.clone(),
        token_handler,
    )
    .await?;

    let network_handler = NetworkHandler::new(
        network.chandler.clone(),
        network.chandler.get_kademlia_actor_handle().await?,
        network.get_realm_key().await?,
    )
    .await?;

    let controller = aruna_data::controller::controller::Controller::<LmdbStore>::new(
        io_handler,
        network_handler,
        task_handler,
    )
    .await;
    let s3server = S3Server::new(
        config.config.frontend.s3_frontend.clone(),
        controller.clone(),
    )
    .await?;

    let rest_handle = tokio::spawn(async move {
        aruna_data::api_json::server::RestServer::run(
            controller,
            Ipv4Addr::from_str(&config.config.frontend.openapi_frontend.address).map_err(|_| {
                ArunaDataError::ConversionError {
                    from: "&str".to_string(),
                    to: "Ipv4Addr".to_string(),
                }
            })?,
            config.config.frontend.openapi_frontend.port,
        )
        .await
    })
    .map_err(|e| {
        error!(error = ?e, msg = e.to_string());
        ArunaDataError::ServerError(e.to_string())
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
    task_handler: TaskHandler<S>,
) -> Result<(), ArunaError>
where
    for<'a> S: Store<'a> + 'static,
{
    let persistor: Arc<Persistor<S, TantivySearch>> =
        Arc::new(Persistor::new(store, config.search_config, perm_manager, token_handler).await?);

    let controller = Arc::new(
        Controller::<S, TantivySearch, P2PNetwork>::new(persistor, network.clone(), task_handler)
            .await,
    );
    Network::start_actor(network, controller.clone()).await?;

    let controller_clone = controller.clone();
    tokio::spawn(async move {
        RestServer::run(
            controller_clone,
            config.metadata_openapi_address,
            config.metadata_openapi_port,
        )
        .await
    });

    Ok(())
}

pub fn parse_config() -> Result<Config, ArunaError> {
    if let Ok(file) = dotenvy::var("ENV") {
        dotenvy::from_filename_override(file)?;
    }

    let otel_server = dotenvy::var("OTEL_SERVER")?;
    let otel_service_name = dotenvy::var("OTEL_SERVICE_NAME")?;

    let metadata_openapi_address = Ipv4Addr::from_str(&dotenvy::var("METADATA_OPENAPI_ADDRESS")?)?;
    let metadata_openapi_port: u16 = dotenvy::var("METADATA_OPENAPI_PORT")?.parse()?;

    // let p2p_key = SecretKey::generate(&mut OsRng);
    // println!("{:?}", &HEXLOWER.encode(&p2p_key.to_bytes()));

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
        metadata_openapi_address,
        metadata_openapi_port,
        otel_server,
        otel_service_name,
        bootstrap_nodes,
        path,
        issuers,
        search_config,
        realm_key: signing_key,
        config: data_config,
    })
}
