use metadata::{
    models::requests::{AddUserRequest, CreateResourceRequest, SearchRequest},
    network::network_trait::{Network, NetworkDummy},
    persistence::{
        persistence::Persistor,
        persistors::{
            fjall_persistor::FjallTantivyPersistence, lmdb_persistor::LmdbTantivyPersistence,
            redb_persistor::RedbTantivyPersistence,
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
    pub async fn start() -> Arc<
        Controller<
            FjallStore,
            TantivySearch,
            NetworkDummy<FjallTantivyPersistence, FjallStore, TantivySearch>,
            FjallTantivyPersistence,
        >,
    > {
        let network = NetworkDummy::new(()).await;
        let persistor = Arc::new(
            FjallTantivyPersistence::new("./database/fjall_tantivy".to_string())
                .await
                .unwrap(),
        );
        let controller = Arc::new(Controller::<
            FjallStore,
            TantivySearch,
            NetworkDummy<FjallTantivyPersistence, FjallStore, TantivySearch>,
            FjallTantivyPersistence,
        >::new(persistor, network));
        controller
    }

    pub async fn create_user(
        controller: Arc<
            Controller<
                FjallStore,
                TantivySearch,
                NetworkDummy<FjallTantivyPersistence, FjallStore, TantivySearch>,
                FjallTantivyPersistence,
            >,
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
            Controller<
                FjallStore,
                TantivySearch,
                NetworkDummy<FjallTantivyPersistence, FjallStore, TantivySearch>,
                FjallTantivyPersistence,
            >,
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
            Controller<
                FjallStore,
                TantivySearch,
                NetworkDummy<FjallTantivyPersistence, FjallStore, TantivySearch>,
                FjallTantivyPersistence,
            >,
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
    pub async fn start() -> Arc<
        Controller<
            LmdbStore,
            TantivySearch,
            NetworkDummy<LmdbTantivyPersistence, LmdbStore, TantivySearch>,
            LmdbTantivyPersistence,
        >,
    > {
        let network = NetworkDummy::new(()).await;
        let persistor = Arc::new(
            LmdbTantivyPersistence::new("./database/heed_tantivy".to_string())
                .await
                .unwrap(),
        );
        let controller = Arc::new(Controller::<
            LmdbStore,
            TantivySearch,
            NetworkDummy<LmdbTantivyPersistence, LmdbStore, TantivySearch>,
            LmdbTantivyPersistence,
        >::new(persistor, network));
        controller
    }

    pub async fn create_user(
        controller: Arc<
            Controller<
                LmdbStore,
                TantivySearch,
                NetworkDummy<LmdbTantivyPersistence, LmdbStore, TantivySearch>,
                LmdbTantivyPersistence,
            >,
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
            Controller<
                LmdbStore,
                TantivySearch,
                NetworkDummy<LmdbTantivyPersistence, LmdbStore, TantivySearch>,
                LmdbTantivyPersistence,
            >,
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
            Controller<
                LmdbStore,
                TantivySearch,
                NetworkDummy<LmdbTantivyPersistence, LmdbStore, TantivySearch>,
                LmdbTantivyPersistence,
            >,
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

pub struct TantivyRedb;
impl TantivyRedb {
    pub async fn start() -> Arc<
        Controller<
            Redb,
            TantivySearch,
            NetworkDummy<RedbTantivyPersistence, Redb, TantivySearch>,
            RedbTantivyPersistence,
        >,
    > {
        let network = NetworkDummy::new(()).await;
        let persistor = Arc::new(
            RedbTantivyPersistence::new("./database/redb_tantivy".to_string())
                .await
                .unwrap(),
        );
        let controller = Arc::new(Controller::<
            Redb,
            TantivySearch,
            NetworkDummy<RedbTantivyPersistence, Redb, TantivySearch>,
            RedbTantivyPersistence,
        >::new(persistor, network));
        controller
    }

    pub async fn create_user(
        controller: Arc<
            Controller<
                Redb,
                TantivySearch,
                NetworkDummy<RedbTantivyPersistence, Redb, TantivySearch>,
                RedbTantivyPersistence,
            >,
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
            Controller<
                Redb,
                TantivySearch,
                NetworkDummy<RedbTantivyPersistence, Redb, TantivySearch>,
                RedbTantivyPersistence,
            >,
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
            Controller<
                Redb,
                TantivySearch,
                NetworkDummy<RedbTantivyPersistence, Redb, TantivySearch>,
                RedbTantivyPersistence,
            >,
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
