use metadata::{
    models::requests::{AddUserRequest, CreateResourceRequest, SearchRequest},
    network::network_trait::{Network, NetworkDummy},
    persistence::{
        persistence::Persistor,
        persistors::{
            fjall_persistor::FjallTantivyPersistance, lmdb_persistor::LmdbTantivyPersistance,
            redb_persistor::RedbTantivyPersistance,
        },
        search::tantivy::TantivySearch,
        storage::{fjall::FjallStore, lmdb::LmdbStore, redb::Redb},
    },
    transactions::controller::Controller,
};
use std::sync::Arc;
use ulid::Ulid;

pub struct TantivyFjall;
impl TantivyFjall {
    pub async fn start()
    -> Arc<Controller<FjallStore, TantivySearch, NetworkDummy, FjallTantivyPersistance>> {
        let network = NetworkDummy::new(());
        let persistor = FjallTantivyPersistance::new("./database/fjall_tantivy".to_string())
            .await
            .unwrap();
        let controller = Arc::new(Controller::<
            FjallStore,
            TantivySearch,
            NetworkDummy,
            FjallTantivyPersistance,
        >::new(persistor, network));
        controller
    }

    pub async fn create_user(
        controller: Arc<
            Controller<FjallStore, TantivySearch, NetworkDummy, FjallTantivyPersistance>,
        >,
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
        controller: Arc<
            Controller<FjallStore, TantivySearch, NetworkDummy, FjallTantivyPersistance>,
        >,
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
        controller: Arc<
            Controller<FjallStore, TantivySearch, NetworkDummy, FjallTantivyPersistance>,
        >,
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
    pub async fn start()
    -> Arc<Controller<LmdbStore, TantivySearch, NetworkDummy, LmdbTantivyPersistance>> {
        let network = NetworkDummy::new(());
        let persistor = LmdbTantivyPersistance::new("./database/heed_tantivy".to_string())
            .await
            .unwrap();
        let controller = Arc::new(Controller::<
            LmdbStore,
            TantivySearch,
            NetworkDummy,
            LmdbTantivyPersistance,
        >::new(persistor, network));
        controller
    }

    pub async fn create_user(
        controller: Arc<Controller<LmdbStore, TantivySearch, NetworkDummy, LmdbTantivyPersistance>>,
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
        controller: Arc<Controller<LmdbStore, TantivySearch, NetworkDummy, LmdbTantivyPersistance>>,
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
        controller: Arc<Controller<LmdbStore, TantivySearch, NetworkDummy, LmdbTantivyPersistance>>,
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
    pub async fn start()
    -> Arc<Controller<Redb, TantivySearch, NetworkDummy, RedbTantivyPersistance>> {
        let network = NetworkDummy::new(());
        let persistor = RedbTantivyPersistance::new("./database/redb_tantivy".to_string())
            .await
            .unwrap();
        let controller = Arc::new(Controller::<
            Redb,
            TantivySearch,
            NetworkDummy,
            RedbTantivyPersistance,
        >::new(persistor, network));
        controller
    }

    pub async fn create_user(
        controller: Arc<Controller<Redb, TantivySearch, NetworkDummy, RedbTantivyPersistance>>,
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
        controller: Arc<Controller<Redb, TantivySearch, NetworkDummy, RedbTantivyPersistance>>,
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
        controller: Arc<Controller<Redb, TantivySearch, NetworkDummy, RedbTantivyPersistance>>,
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
