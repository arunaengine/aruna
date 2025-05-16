use std::{
    net::{Ipv4Addr, SocketAddrV4},
    str::FromStr,
    sync::{Arc, atomic::AtomicU16},
};

use anyhow::Result;
use aruna_metadata::{
    api::server::RestServer,
    network::network_trait::{Network, NetworkConfig, P2PNetwork},
    persistence::{
        persistence::{
            Persistor,
            tables::{
                PUBLIC_MAPPINGS_DB_NAME, RESOURCE_DB_NAME, RESOURCE_MAPPINGS_DB_NAME, USER_DB_NAME,
                USER_MAPPINGS_DB_NAME,
            },
        },
        search::tantivy::{TantivyConfig, TantivySearch},
    },
    transactions::controller::Controller,
};
use aruna_storage::storage::lmdb::{LmdbConfig, LmdbStore};
use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;

pub static SUBSCRIBERS: AtomicU16 = AtomicU16::new(0);
const TEST_CONFIG: TestConfig = TestConfig {
    socket_addr: "127.0.0.1",
    path: "/dev/shm",
    p2p_port: 50000,
    api_port: 8080,
};

struct TestConfig {
    socket_addr: &'static str,
    path: &'static str,
    p2p_port: u16,
    api_port: u16,
}

pub async fn init_lmdb_servers(
    offset: u16,
) -> Result<
    Vec<(
        Arc<Controller<LmdbStore, TantivySearch, P2PNetwork>>,
        String,
    )>,
> {
    let realm_key = Some(SigningKey::generate(&mut OsRng));
    let mut base_urls = Vec::new();

    let subscriber = SUBSCRIBERS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    let (res_sdx, res_rcv) = tokio::sync::mpsc::channel(1000);
    let tantivy_path = format!("{}/{}/node_{}/tantivy", TEST_CONFIG.path, subscriber, 0);
    let search_config = TantivyConfig {
        path: tantivy_path,
        index_buffer: 1_000_000_000,
        resources: res_rcv,
    };
    let store_path = format!("{}/{}/node_{}/heed", TEST_CONFIG.path, subscriber, 0);
    let store_config = LmdbConfig {
        path: store_path,
        databases: vec![
            RESOURCE_DB_NAME,
            RESOURCE_MAPPINGS_DB_NAME,
            USER_DB_NAME,
            USER_MAPPINGS_DB_NAME,
            PUBLIC_MAPPINGS_DB_NAME,
        ],
    };

    let persistor: Arc<Persistor<LmdbStore, TantivySearch>> = Arc::new(
        Persistor::new(res_sdx, store_config, search_config)
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

    let api_port = TEST_CONFIG.api_port + offset + subscriber;
    let controller_clone = controller.clone();
    tokio::spawn(async move { RestServer::run(controller_clone, api_port).await });

    let bootstrap_addr = controller.network.get_addr().await.unwrap();
    base_urls.push((controller, format!("http://localhost:{}/api/v3", api_port)));

    for node in 1..5 {
        let subscriber = SUBSCRIBERS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let (res_sdx, res_rcv) = tokio::sync::mpsc::channel(1000);
        let tantivy_path = format!("{}/{}/node_{}/tantivy", TEST_CONFIG.path, subscriber, node);
        let search_config = TantivyConfig {
            path: tantivy_path,
            index_buffer: 1_000_000_000,
            resources: res_rcv,
        };
        let store_path = format!("{}/{}/node_{}/heed", TEST_CONFIG.path, subscriber, node);
        let store_config = LmdbConfig {
            path: store_path,
            databases: vec![
                RESOURCE_DB_NAME,
                RESOURCE_MAPPINGS_DB_NAME,
                USER_DB_NAME,
                USER_MAPPINGS_DB_NAME,
                PUBLIC_MAPPINGS_DB_NAME,
            ],
        };

        let persistor: Arc<Persistor<LmdbStore, TantivySearch>> = Arc::new(
            Persistor::new(res_sdx, store_config, search_config)
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

        let api_port = TEST_CONFIG.api_port + offset + subscriber;
        let controller_clone = controller.clone();
        tokio::spawn(async move { RestServer::run(controller_clone, api_port).await });
        base_urls.push((controller, format!("http://localhost:{}/api/v3", api_port)));
    }

    Ok(base_urls)
}
