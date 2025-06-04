use anyhow::{Result, anyhow};
use aruna_data::api_json::server::RestServer;
use aruna_data::api_json::util::xor_ulids;
use aruna_data::api_s3::auth::UserAccess;
use aruna_data::api_s3::s3server::S3Server;
use aruna_data::io::controller::Controller;
use aruna_data::io::io_handler::{ACCESS_DB_NAME, PATH_LOCATION_DB_NAME, REPLICATION_PROTOCOL_ID};
use aruna_data::io::io_handler::{IOHandler, LOCATION_DB_NAME};
use aruna_data::{config::config::Config, util::opendal::get_operator};
use aruna_net::actor::NetworkActorBuilder;
use aruna_permission::manager::RESOURCE_DB;
use aruna_storage::storage::lmdb::{LmdbConfig, LmdbStore};
use aruna_storage::storage::store::Store;
use futures_util::TryFutureExt;
use iroh::SecretKey;
use lazy_static::lazy_static;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::str::FromStr;
use std::sync::Arc;
use tokio::try_join;
use tracing::{Level, debug, error};
use tracing_subscriber::EnvFilter;
use ulid::Ulid;

lazy_static! {
    static ref CONFIG: Config = {
        dotenvy::from_filename(".env").ok();
        let config_file = dotenvy::var("CONFIG").unwrap_or("config.toml".to_string());
        let config: Config =
            toml::from_str(std::fs::read_to_string(config_file).unwrap().as_str()).unwrap();
        config
    };
}

#[tokio::main]
async fn main() -> Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or("none".into())
        .add_directive("aruna_data=trace".parse()?)
        .add_directive("aruna_net=trace".parse()?)
        .add_directive("iroh=info".parse()?);

    let subscriber = tracing_subscriber::fmt()
        //.with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        // Use a more compact, abbreviated log format
        .compact()
        .with_max_level(Level::DEBUG)
        .with_env_filter(filter)
        // Display source code file paths
        .with_file(true)
        // Display source code line numbers
        .with_line_number(true)
        .with_target(false)
        .finish();

    tracing::subscriber::set_global_default(subscriber)?;

    debug!(config = ?*CONFIG);
    let node_key = SecretKey::from_str(&CONFIG.general.node_key)?;
    let rest_addr = Ipv4Addr::from_str(&CONFIG.frontend.openapi_frontend.address)?;

    // Dummy access conf which is provided by user/request/node
    let conf = CONFIG.backend.access.clone();
    let operator = get_operator(&CONFIG.backend.backend_type, conf).await?;

    match operator.check().await {
        Ok(_) => debug!(
            "Connection to {} backend succeeded",
            CONFIG.backend.backend_type
        ),
        Err(err) => error!("Connection to backend failed: {}", err),
    }

    // Create an endpoint, it allows creating and accepting connections in the iroh p2p world
    let network_handle_01 = NetworkActorBuilder::new(Some(node_key))
        .await
        .add_bind_addr_v4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 31337))
        .build(vec![])
        .await?;
    let node_addr = network_handle_01.get_node_addr().await?;
    debug!("Node 1 address: {:?}", node_addr);
    let actor_handle = network_handle_01
        .new_actor_handle(REPLICATION_PROTOCOL_ID)
        .await?;
    let kademlia = actor_handle.get_kademlia_actor_handle().await?;

    let lmdb_config = LmdbConfig {
        path: CONFIG.persistence.path.to_string(),
        databases: vec![
            ACCESS_DB_NAME,
            LOCATION_DB_NAME,
            PATH_LOCATION_DB_NAME,
            RESOURCE_DB,
        ],
    };
    let lmdb_store = Arc::new(LmdbStore::new(lmdb_config)?);

    let io_handler = IOHandler::<LmdbStore>::new(
        node_addr.clone(),
        operator,
        actor_handle,
        kademlia,
        lmdb_store,
    )
    .await?;

    //TODO: Remove later
    //create_dummy_access(io_handler.store.clone()).await?;

    let s3server = S3Server::new(
        CONFIG.frontend.s3_frontend.clone(),
        io_handler.clone(),
        node_addr.node_id,
        Ulid::from_string(&CONFIG.general.realm_id)?,
    )
    .await?;

    let controller = Arc::new(Controller::<LmdbStore>::new(io_handler));
    let rest_handle = tokio::spawn(async move {
        RestServer::run(controller, rest_addr, CONFIG.frontend.openapi_frontend.port).await
    })
    .map_err(|e| {
        error!(error = ?e, msg = e.to_string());
        anyhow!("an error occurred {e}")
    });

    match try_join!(s3server.run(), rest_handle) {
        Ok(_) => Ok(()),
        Err(err) => {
            error!("{}", err);
            Err(err)
        }
    }
}

async fn _create_dummy_access<St>(store: Arc<St>) -> Result<()>
where
    for<'a> St: Store<'a> + 'static,
{
    // Just put a dummy user with default credentials into the store
    let store_clone = store.clone();
    tokio::task::spawn_blocking(move || {
        let mut write_txn = store_clone.create_txn(true)?;

        let access_info = UserAccess {
            user_id: Ulid::from_string("01JWB4X5TY0K776QDDCHGK3KT2")?,
            group_id: Ulid::from_string("01JWB4XFCRJX53Q839QMHPGSXH")?,
            secret: "PT97PKZjFdtj59umdqHuU54rTauknd".to_string(),
        };

        // Store object info with blake3 hash as key
        store_clone.put(
            &mut write_txn,
            ACCESS_DB_NAME,
            access_info.user_id.to_string().as_bytes(), // Currently user id
            &bincode::serde::encode_to_vec(access_info, bincode::config::standard())?,
        )?;
        
        store_clone.commit(write_txn)?;

        Ok::<(), anyhow::Error>(())
    });

    Ok(())
}
