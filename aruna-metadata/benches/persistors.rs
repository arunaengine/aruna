use aruna_metadata::{
    models::requests::{AddUserRequest, CreateResourceRequest, SearchRequest},
    network::network_trait::{Network, NetworkDummy},
    persistence::{
        persistence::Persistor,
        search::tantivy::{TantivyConfig, TantivySearch},
        storage::{
            fjall::{FjallConfig, FjallStore},
            lmdb::{LmdbConfig, LmdbStore},
            redb::{Redb, RedbConfig},
        },
    },
    transactions::controller::Controller,
};
use std::sync::Arc;
use ulid::Ulid;

pub struct TantivyFjall;
impl TantivyFjall {
    pub async fn start() -> Arc<Controller<FjallStore, TantivySearch, NetworkDummy>> {
        let path = "/dev/shm/fjall_tantivy".to_string();
        let (res_sdx, res_rcv) = tokio::sync::mpsc::channel(1000);
        let (idx_sdx, idx_rcv) = tokio::sync::oneshot::channel();
        let tantivy_path = format!("{path}/tantivy");
        let search_config = TantivyConfig {
            path: tantivy_path,
            index_buffer: 1_000_000_000,
            resources: res_rcv,
        };

        let store_path = format!("{path}/fjall");
        let store_config = FjallConfig {
            path: store_path,
            res_sdx,
            idx_sdx,
        };
        let persistor = Arc::new(
            Persistor::new(idx_rcv, store_config, search_config)
                .await
                .unwrap(),
        );
        let network = NetworkDummy::new(()).await;
        let controller = Arc::new(Controller::<FjallStore, TantivySearch, NetworkDummy>::new(
            persistor, network,
        ));
        controller
    }

    pub async fn create_user(
        controller: Arc<Controller<FjallStore, TantivySearch, NetworkDummy>>,
    ) -> (Ulid, Ulid) {
        let create_user = AddUserRequest {
            name: "bench_user1".to_string(),
        };
        let res = controller.request(create_user, None).await.unwrap();
        let user1 = res.user.id;

        let create_user = AddUserRequest {
            name: "bench_user2".to_string(),
        };
        let res = controller.request(create_user, None).await.unwrap();
        let user2 = res.user.id;
        (user1, user2)
    }

    pub async fn bench_create(
        controller: Arc<Controller<FjallStore, TantivySearch, NetworkDummy>>,
        user1: Ulid,
        user2: Ulid,
    ) {
        for i in 0..10_000 {
            let create_resource = CreateResourceRequest {
                name: format!("res{i}"),
                ..Default::default()
            };
            if i < 4999 {
                controller
                    .request(create_resource, Some(user1.to_string()))
                    .await
                    .unwrap();
            } else {
                controller
                    .request(create_resource, Some(user2.to_string()))
                    .await
                    .unwrap();
            }
        }
    }

    pub async fn bench_search(
        controller: Arc<Controller<FjallStore, TantivySearch, NetworkDummy>>,
        user1: Ulid,
        user2: Ulid,
    ) {
        controller
            .request(
                SearchRequest {
                    query: "name:res".to_string(),
                },
                Some(user1.to_string()),
            )
            .await
            .unwrap();

        controller
            .request(
                SearchRequest {
                    query: "name:res".to_string(),
                },
                Some(user2.to_string()),
            )
            .await
            .unwrap();
    }
}

pub struct TantivyHeed;
impl TantivyHeed {
    pub async fn start() -> Arc<Controller<LmdbStore, TantivySearch, NetworkDummy>> {
        let path = "/dev/shm/lmdb_tantivy".to_string();
        let (res_sdx, res_rcv) = tokio::sync::mpsc::channel(1000);
        let (idx_sdx, idx_rcv) = tokio::sync::oneshot::channel();
        let tantivy_path = format!("{path}/tantivy");
        let search_config = TantivyConfig {
            path: tantivy_path,
            index_buffer: 1_000_000_000,
            resources: res_rcv,
        };

        let store_path = format!("{path}/lmdb");
        let store_config = LmdbConfig {
            path: store_path,
            res_sdx,
            idx_sdx,
        };
        let persistor = Arc::new(
            Persistor::new(idx_rcv, store_config, search_config)
                .await
                .unwrap(),
        );
        let network = NetworkDummy::new(()).await;
        let controller = Arc::new(Controller::<LmdbStore, TantivySearch, NetworkDummy>::new(
            persistor, network,
        ));
        controller
    }

    pub async fn create_user(
        controller: Arc<Controller<LmdbStore, TantivySearch, NetworkDummy>>,
    ) -> (Ulid, Ulid) {
        let create_user = AddUserRequest {
            name: "bench_user1".to_string(),
        };
        let res = controller.request(create_user, None).await.unwrap();
        let user1 = res.user.id;

        let create_user = AddUserRequest {
            name: "bench_user2".to_string(),
        };
        let res = controller.request(create_user, None).await.unwrap();
        let user2 = res.user.id;
        (user1, user2)
    }

    pub async fn bench_create(
        controller: Arc<Controller<LmdbStore, TantivySearch, NetworkDummy>>,
        user1: Ulid,
        user2: Ulid,
    ) {
        for i in 0..10_000 {
            let create_resource = CreateResourceRequest {
                name: format!("res{i}"),
                ..Default::default()
            };
            if i < 4999 {
                controller
                    .request(create_resource, Some(user1.to_string()))
                    .await
                    .unwrap();
            } else {
                controller
                    .request(create_resource, Some(user2.to_string()))
                    .await
                    .unwrap();
            }
        }
    }

    pub async fn bench_search(
        controller: Arc<Controller<LmdbStore, TantivySearch, NetworkDummy>>,
        user1: Ulid,
        user2: Ulid,
    ) {
        controller
            .request(
                SearchRequest {
                    query: "name:res".to_string(),
                },
                Some(user1.to_string()),
            )
            .await
            .unwrap();

        controller
            .request(
                SearchRequest {
                    query: "name:res".to_string(),
                },
                Some(user2.to_string()),
            )
            .await
            .unwrap();
    }
}

pub struct TantivyRedb;
impl TantivyRedb {
    pub async fn start() -> Arc<Controller<Redb, TantivySearch, NetworkDummy>> {
        let path = "/dev/shm/redb_tantivy".to_string();
        let (res_sdx, res_rcv) = tokio::sync::mpsc::channel(1000);
        let (idx_sdx, idx_rcv) = tokio::sync::oneshot::channel();
        let tantivy_path = format!("{path}/tantivy");
        let search_config = TantivyConfig {
            path: tantivy_path,
            index_buffer: 1_000_000_000,
            resources: res_rcv,
        };

        let store_path = format!("{path}/redb");
        let store_config = RedbConfig {
            path: store_path,
            res_sdx,
            idx_sdx,
        };
        let persistor = Arc::new(
            Persistor::new(idx_rcv, store_config, search_config)
                .await
                .unwrap(),
        );
        let network = NetworkDummy::new(()).await;
        let controller = Arc::new(Controller::<Redb, TantivySearch, NetworkDummy>::new(
            persistor, network,
        ));
        controller
    }

    pub async fn create_user(
        controller: Arc<Controller<Redb, TantivySearch, NetworkDummy>>,
    ) -> (Ulid, Ulid) {
        let create_user = AddUserRequest {
            name: "bench_user1".to_string(),
        };
        let res = controller.request(create_user, None).await.unwrap();
        let user1 = res.user.id;

        let create_user = AddUserRequest {
            name: "bench_user2".to_string(),
        };
        let res = controller.request(create_user, None).await.unwrap();
        let user2 = res.user.id;
        (user1, user2)
    }

    pub async fn bench_create(
        controller: Arc<Controller<Redb, TantivySearch, NetworkDummy>>,
        user1: Ulid,
        user2: Ulid,
    ) {
        for i in 0..10_000 {
            let create_resource = CreateResourceRequest {
                name: format!("res{i}"),
                ..Default::default()
            };
            if i < 4999 {
                controller
                    .request(create_resource, Some(user1.to_string()))
                    .await
                    .unwrap();
            } else {
                controller
                    .request(create_resource, Some(user2.to_string()))
                    .await
                    .unwrap();
            }
        }
    }

    pub async fn bench_search(
        controller: Arc<Controller<Redb, TantivySearch, NetworkDummy>>,
        user1: Ulid,
        user2: Ulid,
    ) {
        controller
            .request(
                SearchRequest {
                    query: "name:res".to_string(),
                },
                Some(user1.to_string()),
            )
            .await
            .unwrap();

        controller
            .request(
                SearchRequest {
                    query: "name:res".to_string(),
                },
                Some(user2.to_string()),
            )
            .await
            .unwrap();
    }
}
