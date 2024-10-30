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
use transactions::controller::Controller;
use tokio::{sync::Notify, task::JoinSet};
use tonic::transport::Server;
use tracing::info;
use ulid::Ulid;

pub mod api;
pub mod constants;
pub mod context;
pub mod error;
pub mod macros;
pub mod models;
pub mod transactions;
pub mod storage;

#[derive(Debug, Clone)]
pub struct Config {
    pub node_id: Ulid,
    pub grpc_port: u16,
    pub rest_port: u16,
    pub node_serial: u16,
    pub database_path: String,
    pub members: Vec<(Ulid, u16, String)>,
    pub key_config: (u32, String, String),
    pub socket_addr: SocketAddr,
}

pub fn config_from_env() -> Config {
    // Env setup
    if let Ok(file) = dotenvy::var("ENV_FILE") {
        dotenvy::from_filename_override(file).unwrap();
    }

    let node_id = Ulid::from_string(&dotenvy::var("NODE_ID").unwrap()).unwrap();
    let grpc_port = dotenvy::var("GRPC_PORT").unwrap().parse::<u16>().unwrap();
    let grpc_port_consensus = dotenvy::var("GRPC_PORT_CONSENSUS")
        .unwrap()
        .parse::<u16>()
        .unwrap();
    let rest_port = dotenvy::var("REST_PORT").unwrap().parse::<u16>().unwrap();
    let node_serial = dotenvy::var("NODE_SERIAL").unwrap().parse::<u16>().unwrap();
    let database_path = dotenvy::var("DB_PATH").unwrap();

    // ID,SERIAL,HOST;
    let members = dotenvy::var("MEMBERS")
        .unwrap()
        .split(";")
        .map(|x| {
            let parts: Vec<&str> = x.split(",").collect();
            (
                Ulid::from_string(parts[0]).unwrap(),
                parts[1].parse::<u16>().unwrap(),
                parts[2].to_string(),
            )
        })
        .collect::<Vec<(Ulid, u16, String)>>();

    let socket_addr = SocketAddr::from(([0, 0, 0, 0], grpc_port_consensus));

    let key_config = (
        dotenvy::var("KEY_SERIAL").unwrap().parse().unwrap(),
        dotenvy::var("ENCODING_KEY").unwrap(),
        dotenvy::var("DECODING_KEY").unwrap(),
    );

    Config {
        node_id,
        grpc_port,
        rest_port,
        node_serial,
        database_path,
        members,
        key_config,
        socket_addr,
    }
}

#[tracing::instrument(level = "trace")]
pub async fn start_server(
    Config {
        database_path,
        node_id,
        node_serial,
        socket_addr,
        members,
        key_config,
        grpc_port,
        rest_port,
    }: Config,
    notify: Option<Arc<Notify>>,
) -> Result<(), ArunaError> {
    let controller = Controller::new(
        database_path,
        node_id,
        node_serial,
        socket_addr,
        members,
        key_config,
    )
    .await
    .unwrap();

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

    join_set.join_next().await;

    Ok(())
}
