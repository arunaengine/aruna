use anyhow::Result;
use aruna_data::IOHandler;
use aruna_data::api_json::request::Request;
use aruna_data::api_json::requests::{CreateS3CredentialsRequest, CreateS3CredentialsResponse};
use aruna_data::api_json::server::RestServer;
use aruna_data::api_s3::s3server::S3Server;
use aruna_data::config::config::Config;
use aruna_data::io::controller::Controller;
use aruna_data::io::io_handler::REPLICATION_PROTOCOL_ID;
use aruna_net::actor::NetworkActorBuilder;
use aruna_permission::paths::RealmKey;
use aruna_permission::token::Issuer;
use aruna_permission::{OidcToken, PermissionManager, TokenSystem, UserIdentity};
use aruna_storage::storage::lmdb::{LmdbConfig, LmdbStore};
use aruna_storage::storage::store::Store;
use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{Credentials, RequestChecksumCalculation};
use chrono::Months;
use ed25519_dalek::SigningKey;
use parking_lot::RwLock;
use rand::rngs::OsRng;
use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::AtomicU16;
use ulid::Ulid;
use utoipa::r#gen::serde_json;

pub static SUBSCRIBERS: AtomicU16 = AtomicU16::new(0);

const OPENAPI_BASE_PORT: u16 = 33000;
const S3_BASE_PORT: u16 = 34000;
const P2P_BASE_PORT: u16 = 35000;

pub struct TestServers {
    pub realm_key: SigningKey,
    pub node_services: Vec<NodeServices>,
}

#[derive(Clone)]
pub struct NodeServices {
    pub s3_endpoint: String,
    pub openapi_data_endpoint: (Arc<Controller<LmdbStore>>, String),
}

pub async fn init_test_nodes(num: usize, port_offset: u16) -> anyhow::Result<TestServers> {
    let realm_key = SigningKey::generate(&mut OsRng);
    let mut node_services = vec![];
    let mut bootstrap_nodes = vec![];
    let databases = vec![
        aruna_permission::DBNAME,
        aruna_permission::RESOURCE_DB,
        aruna_permission::OIDC_IDENTITIES_DB,
        aruna_permission::IDENTITY_PERMISSIONS_DB,
        aruna_data::ACCESS_DB_NAME,
        aruna_data::LOCATION_DB_NAME,
        aruna_data::LOCATION_STATS_DB_NAME,
        aruna_data::PATH_LOCATION_DB_NAME,
    ];

    for _ in 0..num {
        // Create dummy config
        let node_idx = SUBSCRIBERS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let p2p_port = P2P_BASE_PORT + port_offset + node_idx;
        let openapi_port = OPENAPI_BASE_PORT + port_offset + node_idx;
        let s3_port = S3_BASE_PORT + port_offset + node_idx;
        let config = Config::create_dummy_config(
            node_idx,
            Some(realm_key.clone()),
            p2p_port,
            openapi_port,
            s3_port,
        );

        // Init network
        let rest_addr = Ipv4Addr::from_str(&config.frontend.openapi_frontend.address)?;
        let network_handle_01 = NetworkActorBuilder::new(Some(config.general.node_key))
            .await
            .add_bind_addr_v4(SocketAddrV4::new(
                Ipv4Addr::new(127, 0, 0, 1),
                config.general.p2p_port,
            ))
            .build(bootstrap_nodes.clone())
            .await?;
        let node_addr = network_handle_01.get_node_addr().await?;
        let actor_handle = network_handle_01
            .new_actor_handle(REPLICATION_PROTOCOL_ID)
            .await?;
        let kademlia = actor_handle.get_kademlia_actor_handle().await?;

        // Add self to bootstrap nodes for following nodes
        bootstrap_nodes.push(node_addr.clone());

        // Init store
        let lmdb_config = LmdbConfig {
            path: config.persistence.path.to_string(),
            databases: databases.clone(),
        };
        let lmdb_store = Arc::new(LmdbStore::new(lmdb_config)?);

        // Init PermissionManager and load policies from persistence
        let permission_manager = PermissionManager::new().await?;
        let mut read_txn = lmdb_store.create_txn(false)?;
        permission_manager
            .load_policies(lmdb_store.as_ref(), &mut read_txn)
            .await?;
        lmdb_store.commit(read_txn)?;

        // Init token handler
        let realm_key_clone = realm_key.clone();
        let token_handler = Arc::new(RwLock::new(
            tokio::task::spawn_blocking(move || {
                Ok::<TokenSystem, anyhow::Error>(TokenSystem::new(
                    &realm_key_clone.verifying_key().to_bytes(),
                    vec![Issuer {
                        issuer_name: "http://localhost:1998/realms/test".to_string(),
                        pubkey_url:
                            "http://localhost:1998/realms/test/protocol/openid-connect/certs"
                                .to_string(),
                        aud: vec!["test".to_string(), "test-long".to_string()],
                    }],
                )?)
            })
            .await??,
        ));

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

        // Create and run S3 server
        let s3server = S3Server::new(
            config.frontend.s3_frontend.clone(),
            io_handler.clone(),
            config.backend.clone(),
            permission_manager.clone(),
            config.general.realm_key.to_bytes(),
        )
        .await?;
        tokio::spawn(async move { s3server.run().await });

        // Create and run rest server
        let controller = Arc::new(Controller::<LmdbStore>::new(
            io_handler,
            permission_manager,
            token_handler,
        ));
        let controller_clone = controller.clone();
        tokio::spawn(async move {
            RestServer::run(
                controller_clone,
                rest_addr,
                config.frontend.openapi_frontend.port,
            )
            .await
        });

        node_services.push(NodeServices {
            s3_endpoint: config.frontend.s3_frontend.server,
            openapi_data_endpoint: (
                controller,
                format!(
                    "{}:{}",
                    config.frontend.openapi_frontend.address, openapi_port
                ),
            ),
        })
    }

    Ok(TestServers {
        realm_key,
        node_services,
    })
}

pub async fn create_user_with_group_and_credentials<St>(
    name: &str,
    group_id: Ulid,
    realm_key: RealmKey,
    store: &St,
    token_handler: Arc<RwLock<TokenSystem>>,
    permission_handler: PermissionManager,
    controller: Arc<Controller<LmdbStore>>,
) -> Result<(UserIdentity, String, CreateS3CredentialsResponse)>
where
    for<'a> St: Store<'a> + 'static,
{
    // Create user and generate an Aruna token
    let user_identity = register_oidc_user(name, store, token_handler.clone())?;
    let token = fetch_user_token(&user_identity, token_handler.clone())?;

    // Create group with user as admin
    let mut txn = store.create_txn(true)?;
    permission_handler
        .create_group(group_id, &user_identity, realm_key, store, &mut txn)
        .await?;
    store.commit(txn)?;

    // Create S3 credentials with the controller of a specific node
    let credentials = CreateS3CredentialsRequest {
        group_id: group_id.to_string(),
    }
    .run_request(Some(user_identity.clone()), controller.as_ref())
    .await?;

    Ok((user_identity, token, credentials))
}

pub fn register_oidc_user<St>(
    name: &str,
    store: &St,
    token_handler: Arc<RwLock<TokenSystem>>,
) -> Result<UserIdentity>
where
    for<'a> St: Store<'a> + 'static,
{
    let token = OidcToken {
        iss: "http://localhost:1998/realms/test".to_string(),
        sub: format!("{name}@test.org"),
        exp: chrono::Utc::now()
            .checked_add_months(Months::new(12))
            .unwrap()
            .timestamp_millis() as u64,
        iat: chrono::Utc::now().timestamp_millis() as u64,
        aud: Some(serde_json::Value::String("test".to_string())),
        email: None,
        email_verified: None,
        preferred_username: None,
        name: None,
        given_name: None,
        family_name: None,
        additional_claims: HashMap::default(),
    };

    let mut write_txn = store.create_txn(true)?;
    let user_identity = token_handler.read().register_user_from_oidc_claims(
        &token.iss,
        &token.sub,
        store,
        &mut write_txn,
    )?;
    store.commit(write_txn)?;

    Ok(user_identity)
}

pub fn fetch_user_token(
    user_identity: &UserIdentity,
    token_handler: Arc<RwLock<TokenSystem>>,
) -> Result<String> {
    Ok(token_handler.read().generate_token(&user_identity)?)
}

pub async fn upload_data(
    client: &Client,
    bucket: &str,
    key: &str,
    data: &'static [u8],
) -> Result<String> {
    let hash = Hasher::new().update(data).finalize();
    let body = aws_sdk_s3::primitives::ByteStream::from_static(data);
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(body)
        .send()
        .await?;
    Ok(hash.to_string())
}
