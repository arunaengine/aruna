use std::{net::SocketAddr, sync::Arc};

use api::{
    grpc::{
        group_service::GroupServiceImpl, realm_service::RealmServiceImpl,
        resource_service::ResourceServiceImpl, user_service::UserServiceImpl,
    },
    rest::server::RestServer,
};
use aruna_rust_api::v3::aruna::api::v3::{
    group_service_server::GroupServiceServer, realm_service_server::RealmServiceServer,
    resource_service_server::ResourceServiceServer, user_service_server::UserServiceServer,
};
use error::ArunaError;
use models::requests::AddOidcProviderRequest;
use tokio::{sync::Notify, task::JoinSet};
use tonic::transport::Server;
use tracing::info;
use transactions::{auth::AddOidcProviderRequestTx, controller::Controller};
use ulid::Ulid;

pub mod api;
pub mod constants;
pub mod context;
pub mod crypto;
pub mod error;
pub mod macros;
pub mod models;
pub mod storage;
pub mod transactions;

#[derive(Debug, Clone)]
pub struct Config {
    pub node_id: Ulid,
    pub grpc_port: u16,
    pub rest_port: u16,
    pub node_serial: u16,
    pub database_path: String,
    pub init_node: Option<String>,
    pub key_config: (u32, String, String),
    pub socket_addr: SocketAddr,
    pub issuer_config: Option<(String, String, String)>,
    pub opentelemetry_name: String,
    pub opentelemetry_endpoint: String,
}

pub fn config_from_env() -> Config {
    // Env setup
    if let Ok(file) = dotenvy::var("ENV_FILE") {
        dotenvy::from_filename_override(file).unwrap();
    }

    let opentelemetry_name = dotenvy::var("OTEL_SERVICE_NAME").unwrap();
    let opentelemetry_endpoint = dotenvy::var("OTEL_ENDPOINT").unwrap();
    let node_id = Ulid::from_string(&dotenvy::var("NODE_ID").unwrap()).unwrap();
    let grpc_port = dotenvy::var("GRPC_PORT").unwrap().parse::<u16>().unwrap();
    let grpc_port_consensus = dotenvy::var("GRPC_PORT_CONSENSUS")
        .unwrap()
        .parse::<u16>()
        .unwrap();
    let rest_port = dotenvy::var("REST_PORT").unwrap().parse::<u16>().unwrap();
    let node_serial = dotenvy::var("NODE_SERIAL").unwrap().parse::<u16>().unwrap();
    let database_path = dotenvy::var("DB_PATH").unwrap();
    let init_node = dotenvy::var("INIT_NODE").ok();

    let socket_addr = SocketAddr::from(([0, 0, 0, 0], grpc_port_consensus));

    let key_config = (
        dotenvy::var("KEY_SERIAL").unwrap().parse().unwrap(),
        dotenvy::var("ENCODING_KEY").unwrap(),
        dotenvy::var("DECODING_KEY").unwrap(),
    );
    let issuer_config = match (
        dotenvy::var("OIDC_ISSUER_NAME").ok(),
        dotenvy::var("OIDC_ISSUER_ENDPOINT").ok(),
        dotenvy::var("OIDC_ISSUER_AUDIENCES").ok(),
    ) {
        (Some(name), Some(endpoint), Some(aud)) => Some((name, endpoint, aud)),
        _ => None,
    };

    Config {
        node_id,
        grpc_port,
        rest_port,
        node_serial,
        database_path,
        init_node,
        key_config,
        socket_addr,
        issuer_config,
        opentelemetry_name,
        opentelemetry_endpoint,
    }
}

#[tracing::instrument(level = "trace", skip(key_config))]
pub async fn start_server(
    Config {
        database_path,
        node_id,
        node_serial,
        socket_addr,
        key_config,
        grpc_port,
        rest_port,
        init_node,
        issuer_config,
        ..
    }: Config,
    notify: Option<Arc<Notify>>,
) -> Result<(), ArunaError> {
    let first_node = init_node.is_none();

    let controller = Controller::new(
        database_path,
        node_id,
        node_serial,
        socket_addr,
        init_node,
        key_config,
        //sdx,
    )
    .await
    .unwrap();

    match issuer_config {
        Some((issuer_name, issuer_endpoint, aud)) => {
            if first_node {
                info!("Adding OIDC provider");
                let audiences = aud.split(';').map(|s| s.to_string()).collect();
                let request_tx = AddOidcProviderRequestTx {
                    req: AddOidcProviderRequest {
                        issuer_name,
                        issuer_endpoint,
                        audiences,
                    },
                    requester: None,
                };
                controller.transaction(Ulid::new().0, &request_tx).await?;
            } else {
                info!("Ignoring OIDC config, because consensus is needed");
            }
        }
        _ => {}
    };

    let mut join_set = JoinSet::new();

    info!("Starting server");

    let controller_clone = controller.clone();

    join_set.spawn(async move {
        Server::builder()
            .http2_keepalive_interval(Some(std::time::Duration::from_secs(15)))
            .add_service(ResourceServiceServer::new(ResourceServiceImpl::new(
                controller_clone.clone(),
            )))
            .add_service(RealmServiceServer::new(RealmServiceImpl::new(
                controller_clone.clone(),
            )))
            .add_service(UserServiceServer::new(UserServiceImpl::new(
                controller_clone.clone(),
            )))
            .add_service(GroupServiceServer::new(GroupServiceImpl::new(
                controller_clone,
            )))
            .serve(SocketAddr::from(([0, 0, 0, 0], grpc_port)))
            .await
            .map_err(|e| ArunaError::ServerError(e.to_string()))
            .inspect_err(logerr!())?;
        Ok(())
    });

    join_set.spawn(async move { RestServer::run(controller.clone(), rest_port).await });

    info!("Server started");
    if let Some(notify) = notify {
        notify.notify_waiters();
    }

    join_set.join_all().await;

    Ok(())
}
