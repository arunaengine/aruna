use anyhow::{Result, anyhow};
use aruna_data::LOCATION_STATS_DB_NAME;
use aruna_data::api_json::server::RestServer;
use aruna_data::api_s3::auth::UserAccess;
use aruna_data::api_s3::s3server::S3Server;
use aruna_data::config::config;
use aruna_data::io::controller::Controller;
use aruna_data::io::io_handler::tables::{ACCESS_DB_NAME, LOCATION_DB_NAME, PATH_LOCATION_DB_NAME};
use aruna_data::io::io_handler::{IOHandler, REPLICATION_PROTOCOL_ID};
use aruna_net::actor::NetworkActorBuilder;
use aruna_permission::manager::PermissionManager;
use aruna_permission::paths::RealmKey;
use aruna_permission::token::Issuer;
use aruna_permission::{TokenSystem, UserIdentity};
use aruna_storage::storage::lmdb::{LmdbConfig, LmdbStore};
use aruna_storage::storage::store::Store;
use futures_util::TryFutureExt;
use parking_lot::RwLock;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::str::FromStr;
use std::sync::Arc;
use tokio::try_join;
use tracing::{Level, debug, error};
use tracing_subscriber::EnvFilter;
use ulid::Ulid;

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

    let config = config::Config::load_from_env()?;
    debug!(?config);

    // Create an endpoint, it allows creating and accepting connections in the iroh p2p world
    let rest_addr = Ipv4Addr::from_str(&config.frontend.openapi_frontend.address)?;
    let network_handle_01 = NetworkActorBuilder::new(Some(config.general.node_key))
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
        path: config.persistence.path.to_string(),
        databases: vec![
            ACCESS_DB_NAME,
            LOCATION_DB_NAME,
            LOCATION_STATS_DB_NAME,
            PATH_LOCATION_DB_NAME,
            aruna_permission::RESOURCE_DB,
            aruna_permission::DBNAME,
            aruna_permission::IDENTITY_PERMISSIONS_DB,
            aruna_permission::OIDC_IDENTITIES_DB,
        ],
    };
    let lmdb_store = Arc::new(LmdbStore::new(lmdb_config)?);

    // Init PermissionManager and load policies from persistence
    let permission_manager = PermissionManager::new().await?;
    let mut read_txn = lmdb_store.create_txn(false)?;
    permission_manager
        .load_policies(lmdb_store.as_ref(), &mut read_txn)
        .await?;
    lmdb_store.commit(read_txn)?;

    //TODO: Token handler trusted issuer
    let issuers = vec![Issuer {
        issuer_name: "".to_string(),
        pubkey_url: "".to_string(),
        aud: vec![],
    }];
    let token_handler = Arc::new(RwLock::new(TokenSystem::new(
        &config.general.realm_key.to_bytes(),
        issuers,
    )?));

    // Create and run IOHandler
    let io_handler = IOHandler::<LmdbStore>::new(
        node_addr.clone(),
        actor_handle,
        kademlia,
        config.backend.clone(),
        lmdb_store,
        permission_manager.clone(),
        config.general.realm_key.to_bytes(),
    )
    .await?;

    //TODO: ----- Remove later ----------
    create_dummy_access(
        io_handler.store.clone(),
        config.general.realm_key.to_bytes(),
        permission_manager.clone(),
    )
    .await?;
    // -----------------------------------

    let s3server = S3Server::new(
        config.frontend.s3_frontend.clone(),
        io_handler.clone(),
        config.backend.clone(),
        permission_manager.clone(),
        config.general.realm_key.to_bytes(),
    )
    .await?;

    let controller = Arc::new(Controller::<LmdbStore>::new(
        io_handler,
        permission_manager,
        token_handler,
    ));
    let rest_handle = tokio::spawn(async move {
        RestServer::run(controller, rest_addr, config.frontend.openapi_frontend.port).await
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

async fn create_dummy_access<St>(
    store: Arc<St>,
    realm_key: RealmKey,
    manager: PermissionManager,
) -> Result<()>
where
    for<'a> St: Store<'a> + 'static,
{
    // Create a dummy user with some permissions and credentials
    let user_id = Ulid::from_string("01JWB4X5TY0K776QDDCHGK3KT2")?;
    let group_id = Ulid::from_string("01JWB4XFCRJX53Q839QMHPGSXH")?;
    let user_identity = UserIdentity {
        user_ulid: user_id,
        realm_key,
    };

    let mut write_txn = store.create_txn(true)?;
    manager
        .create_group(
            group_id,
            &user_identity,
            realm_key,
            store.as_ref(),
            &mut write_txn,
        )
        .await?;
    manager
        .add_user(
            group_id,
            &user_identity,
            "admin",
            store.as_ref(),
            &mut write_txn,
        )
        .await?;

    let access_key_id = Ulid::from_string("01JYEH75CM8DGP5PZWZ3K2BVVN")?;
    let access_info = UserAccess {
        user_id: user_identity.clone(),
        group_id,
        secret: "PT97PKZjFdtj59umdqHuU54rTauknd".to_string(),
    };
    store.put(
        &mut write_txn,
        ACCESS_DB_NAME,
        &access_key_id.to_bytes(), //&xor_ulids(&user_id, &group_id).unwrap(),
        &bincode::serde::encode_to_vec(access_info, bincode::config::standard())?,
    )?;

    let access_key_id = Ulid::from_string("01JYEH7Y5QAE4XN3TDZCM4K8VS")?;
    let access_info = UserAccess {
        user_id: user_identity,
        group_id: Ulid::from_string("01JWX8CR80KRHW5XFVKHPJ6Z5D")?, // Other group
        secret: "gSVpgA2sFFPv06yiS87kEhKGe8IL02".to_string(),
    };
    store.put(
        &mut write_txn,
        ACCESS_DB_NAME,
        &access_key_id.to_bytes(), //&xor_ulids(&user_id, &access_info.group_id).unwrap(),
        &bincode::serde::encode_to_vec(access_info, bincode::config::standard())?,
    )?;

    store.commit(write_txn)?;

    tracing::info!("{:?}", manager.get_group_roles(group_id).await);
    tracing::info!("{:?}", manager.get_role_users(group_id, "admin").await);
    tracing::info!("{:?}", manager.get_role_policies(group_id, "admin").await);

    Ok(())
}
