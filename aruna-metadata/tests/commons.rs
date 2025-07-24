use anyhow::Result;
use aruna_metadata::{
    api::server::RestServer,
    models::requests::{AddUserRequest, Request},
    network::network_trait::{Network, NetworkConfig, P2PNetwork},
    persistence::{
        persistor::{
            Persistor,
            tables::{
                GROUPS_DB_NAME, GROUPS_MAPPINGS_DB_NAME, PUBLIC_MAPPINGS_DB_NAME, RESOURCE_DB_NAME,
                RESOURCE_MAPPINGS_DB_NAME, USER_DB_NAME,
            },
        },
        search::tantivy::{TantivyConfig, TantivySearch},
    },
    transactions::controller::Controller,
};
use aruna_permission::{OidcToken, PermissionManager, TokenSystem, UserIdentity};
use aruna_storage::storage::{
    lmdb::{LmdbConfig, LmdbStore},
    store::Store,
};
use chrono::Months;
use ed25519_dalek::SigningKey;
use iroh::NodeAddr;
use parking_lot::RwLock;
use rand::rngs::OsRng;
#[allow(unused)] // used for tracing of commented in
use std::{
    collections::HashMap,
    net::{Ipv4Addr, SocketAddrV4},
    str::FromStr,
    sync::{Arc, atomic::AtomicU16},
    time::Duration,
};
#[allow(unused)]
use tracing_subscriber::EnvFilter;
#[allow(unused)]
use tracing_subscriber::prelude::*;
use ulid::Ulid;

pub static SUBSCRIBERS: AtomicU16 = AtomicU16::new(0);
pub const TEST_CONFIG: TestConfig = TestConfig {
    socket_addr: "127.0.0.1",
    path: "/dev/shm/tests",
    p2p_port: 50000,
    api_port: 8080,
};

pub struct TestConfig {
    pub socket_addr: &'static str,
    pub path: &'static str,
    pub p2p_port: u16,
    pub api_port: u16,
}

pub struct TestServers {
    pub realm_key: SigningKey,
    pub addr_server_pairs: Vec<Server>,
}

#[derive(Clone)]
pub struct Server {
    pub controller: Arc<Controller<LmdbStore, TantivySearch, P2PNetwork>>,
    pub path: String,
    pub addr: NodeAddr,
}

pub async fn init_server(
    config: TestConfig,
    realm_key: SigningKey,
    offset: u16,
    bootstrap_addr: Option<NodeAddr>,
) -> Result<Server> {
    let subscriber = SUBSCRIBERS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

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

    let tantivy_path = format!(
        "{}/test-{}-{}/node_{}/tantivy",
        config.path,
        subscriber,
        Ulid::new(),
        0
    );
    let search_config = TantivyConfig {
        path: tantivy_path,
        index_buffer: 1_000_000_000,
    };
    let store_path = format!(
        "{}/test-{}-{}/node_{}/heed",
        config.path,
        Ulid::new(),
        subscriber,
        0
    );
    let store_config = LmdbConfig {
        path: store_path,
        databases: databases.clone(),
    };
    let store = LmdbStore::new(store_config)?;

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
            &realm_key.as_bytes().clone(),
            vec![aruna_permission::token::Issuer {
                issuer_name: "http://localhost:1998/realms/test".to_string(),
                pubkey_url: "http://localhost:1998/realms/test/protocol/openid-connect/certs"
                    .to_string(),
                aud: vec!["test".to_string(), "test-long".to_string()],
            }],
        )
        .unwrap(),
    ));

    let persistor: Arc<Persistor<LmdbStore, TantivySearch>> = Arc::new(
        Persistor::new(store, search_config, permission_manager, token_handler)
            .await
            .unwrap(),
    );

    let network = Arc::new(
        P2PNetwork::new(NetworkConfig {
            secret_key: None,
            socket_addr: SocketAddrV4::new(
                Ipv4Addr::from_str(config.socket_addr).unwrap(),
                config.p2p_port + offset + subscriber,
            ),
            bootstrap_nodes: bootstrap_addr.map(|addr| vec![addr]).unwrap_or_default(),
            realm_key: realm_key.clone(),
        })
        .await
        .unwrap(),
    );

    let controller = Arc::new(Controller::<LmdbStore, TantivySearch, P2PNetwork>::new(
        persistor,
        network.clone(),
    ));
    Network::start_actor(network, controller.clone())
        .await
        .unwrap();

    let api_port = config.api_port + offset + subscriber;
    let controller_clone = controller.clone();
    let rest_addr = Ipv4Addr::new(0, 0, 0, 0);
    tokio::spawn(
        async move { RestServer::run(controller_clone, rest_addr.clone(), api_port).await },
    );

    let bootstrap_addr = controller.network.get_addr().await.unwrap();
    Ok(Server {
        controller,
        path: format!("http://localhost:{}/api/v3", api_port),
        addr: bootstrap_addr,
    })
}

pub async fn init_lmdb_servers(offset: u16) -> Result<TestServers> {
    //let logging_env_filter = EnvFilter::try_from_default_env()
    //    .unwrap_or("none".into())
    //    .add_directive("aruna_metadata=trace".parse().unwrap())
    //    .add_directive("aruna_permission=trace".parse().unwrap());
    ////add_directive("tower_http=info".parse().unwrap())
    ////add_directive("aruna_net=info".parse().unwrap());

    //let fmt_layer = tracing_subscriber::fmt::layer()
    //    .with_file(true)
    //    .with_line_number(true)
    //    .with_filter(logging_env_filter);
    //tracing_subscriber::registry().with(fmt_layer).init();

    let realm_key = SigningKey::generate(&mut OsRng);
    let mut servers = Vec::new();
    let init_node = init_server(TEST_CONFIG, realm_key.clone(), offset, None).await?;

    let bootstrap_addr = init_node.addr.clone();
    servers.push(init_node);

    for _ in 1..5 {
        let next_node = init_server(
            TEST_CONFIG,
            realm_key.clone(),
            offset,
            Some(bootstrap_addr.clone()),
        )
        .await?;
        servers.push(next_node);
    }

    for Server { controller, .. } in &servers {
        controller.network.update_realm().await?;
    }

    Ok(TestServers {
        realm_key,
        addr_server_pairs: servers,
    })
}

pub async fn create_user_with_token(
    server: &Server,
    name: String,
) -> Result<(UserIdentity, String)> {
    let request = AddUserRequest { name: name.clone() };
    let response = request
        .run_request(
            OidcToken {
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
            },
            &server.controller,
        )
        .await?;
    let user_identity = response.user.id;
    let user_token = server
        .controller
        .persistence
        .token_handler
        .read()
        .generate_token(&user_identity)?;

    Ok((user_identity, user_token))
}
