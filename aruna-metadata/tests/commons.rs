use anyhow::Result;
use aruna_metadata::{
    api::server::RestServer,
    models::requests::AddUserRequest,
    network::network_trait::{Network, NetworkConfig, P2PNetwork},
    persistence::{
        persistence::{
            Persistor,
            tables::{
                GROUPS_DB_NAME, GROUPS_MAPPINGS_DB_NAME, PUBLIC_MAPPINGS_DB_NAME, RESOURCE_DB_NAME,
                RESOURCE_MAPPINGS_DB_NAME, USER_DB_NAME,
            },
        },
        search::tantivy::{TantivyConfig, TantivySearch},
    },
    transactions::{controller::Controller, request::Request},
};
use aruna_permission::{
    OidcToken, PermissionManager, TokenSystem, UserIdentity, token::Ed25519KeyPair,
};
use aruna_storage::storage::{
    lmdb::{LmdbConfig, LmdbStore},
    store::Store,
};
use chrono::Months;
use ed25519_dalek::SigningKey;
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

pub static SUBSCRIBERS: AtomicU16 = AtomicU16::new(0);
const TEST_CONFIG: TestConfig = TestConfig {
    socket_addr: "127.0.0.1",
    path: "/dev/shm/tests",
    p2p_port: 50000,
    api_port: 8080,
};

struct TestConfig {
    socket_addr: &'static str,
    path: &'static str,
    p2p_port: u16,
    api_port: u16,
}

pub struct TestServers {
    pub realm_keys: Ed25519KeyPair,
    pub addr_server_pairs: Vec<(
        Arc<Controller<LmdbStore, TantivySearch, P2PNetwork>>,
        String,
    )>,
}

pub async fn init_lmdb_servers(offset: u16) -> Result<TestServers> {
    //let logging_env_filter = EnvFilter::try_from_default_env()
    //   .unwrap_or("none".into())
    //   .add_directive("aruna_metadata=error".parse().unwrap());
    //add_directive("aruna_storage=info".parse().unwrap())
    //add_directive("tower_http=info".parse().unwrap())
    //add_directive("aruna_net=info".parse().unwrap());

    // let fmt_layer = tracing_subscriber::fmt::layer()
    //     .with_file(true)
    //     .with_line_number(true)
    //     .with_filter(logging_env_filter);
    // tracing_subscriber::registry().with(fmt_layer).init();

    let realm_keys = Ed25519KeyPair::generate();

    let mut server_url_pairs = Vec::new();
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

    let subscriber = SUBSCRIBERS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    let tantivy_path = format!("{}/{}/node_{}/tantivy", TEST_CONFIG.path, subscriber, 0);
    let search_config = TantivyConfig {
        path: tantivy_path,
        index_buffer: 1_000_000_000,
    };
    let store_path = format!("{}/{}/node_{}/heed", TEST_CONFIG.path, subscriber, 0);
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
            realm_keys.verifying_key.as_bytes().clone(),
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
                Ipv4Addr::from_str(TEST_CONFIG.socket_addr).unwrap(),
                TEST_CONFIG.p2p_port + offset + subscriber,
            ),
            bootstrap_nodes: vec![],
            realm_key: realm_keys.signing_key.clone(),
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

    let api_port = TEST_CONFIG.api_port + offset + subscriber;
    let controller_clone = controller.clone();
    tokio::spawn(async move { RestServer::run(controller_clone, api_port).await });

    let bootstrap_addr = controller.network.get_addr().await.unwrap();
    server_url_pairs.push((controller, format!("http://localhost:{}/api/v3", api_port)));

    for node in 1..5 {
        let subscriber = SUBSCRIBERS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let tantivy_path = format!("{}/{}/node_{}/tantivy", TEST_CONFIG.path, subscriber, node);
        let search_config = TantivyConfig {
            path: tantivy_path,
            index_buffer: 1_000_000_000,
        };
        let store_path = format!("{}/{}/node_{}/heed", TEST_CONFIG.path, subscriber, node);
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
                realm_keys.verifying_key.as_bytes().clone(),
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
                    Ipv4Addr::from_str(TEST_CONFIG.socket_addr).unwrap(),
                    TEST_CONFIG.p2p_port + offset + subscriber,
                ),
                bootstrap_nodes: vec![bootstrap_addr.clone()],
                realm_key: realm_keys.signing_key.clone(),
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

        let api_port = TEST_CONFIG.api_port + offset + subscriber;
        let controller_clone = controller.clone();
        tokio::spawn(async move { RestServer::run(controller_clone, api_port).await });
        server_url_pairs.push((controller, format!("http://localhost:{}/api/v3", api_port)));
    }

    for (controller, _url) in &server_url_pairs {
        controller.network.update_realm().await?;
    }

    Ok(TestServers {
        realm_keys,
        addr_server_pairs: server_url_pairs,
    })
}

pub async fn create_user_with_token(
    test: &TestServers,
    name: String,
) -> Result<(UserIdentity, String)> {
    let (controller, _) = test.addr_server_pairs.first().unwrap();

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
            controller,
        )
        .await?;
    let user_identity = response.user.id;
    let user_token = controller
        .persistence
        .token_handler
        .read()
        .generate_token(&user_identity, &test.realm_keys.signing_key_pem()?)?;

    Ok((user_identity, user_token))
}
