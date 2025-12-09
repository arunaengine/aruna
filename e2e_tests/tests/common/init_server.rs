use super::test_utils::DEFAULT_ENDPOINT_ULID;
use crate::common::init::{init_database, TestContainers};
use crate::common::init_proxy::PROXY_01_DEFAULT_ULID;
use aruna_rust_api::api::hooks::services::v2::hooks_service_server::HooksServiceServer;
use aruna_rust_api::api::notification::services::v2::event_notification_service_server::EventNotificationServiceServer;
use aruna_rust_api::api::storage::services::v2::authorization_service_server::AuthorizationServiceServer;
use aruna_rust_api::api::storage::services::v2::collection_service_server::CollectionServiceServer;
use aruna_rust_api::api::storage::services::v2::data_replication_service_server::DataReplicationServiceServer;
use aruna_rust_api::api::storage::services::v2::dataset_service_server::DatasetServiceServer;
use aruna_rust_api::api::storage::services::v2::endpoint_service_server::EndpointServiceServer;
use aruna_rust_api::api::storage::services::v2::license_service_server::LicenseServiceServer;
use aruna_rust_api::api::storage::services::v2::object_service_server::ObjectServiceServer;
use aruna_rust_api::api::storage::services::v2::project_service_server::ProjectServiceServer;
use aruna_rust_api::api::storage::services::v2::relations_service_server::RelationsServiceServer;
use aruna_rust_api::api::storage::services::v2::rules_service_server::RulesServiceServer;
use aruna_rust_api::api::storage::services::v2::search_service_server::SearchServiceServer;
use aruna_rust_api::api::storage::services::v2::storage_status_service_server::StorageStatusServiceServer;
use aruna_rust_api::api::storage::services::v2::user_service_server::UserServiceServer;
use aruna_server::auth::permission_handler::PermissionHandler;
use aruna_server::auth::token_handler::TokenHandler;
use aruna_server::caching::cache::Cache;
use aruna_server::caching::notifications_handler::NotificationHandler;
use aruna_server::database::connection::Database;
use aruna_server::database::dsls::stats_dsl::start_refresh_loop;
use aruna_server::grpc::authorization::AuthorizationServiceImpl;
use aruna_server::grpc::collections::CollectionServiceImpl;
use aruna_server::grpc::data_replication::DataReplicationServiceImpl;
use aruna_server::grpc::datasets::DatasetServiceImpl;
use aruna_server::grpc::endpoints::EndpointServiceImpl;
use aruna_server::grpc::hooks::HookServiceImpl;
use aruna_server::grpc::info::StorageStatusServiceImpl;
use aruna_server::grpc::licenses::LicensesServiceImpl;
use aruna_server::grpc::notification::NotificationServiceImpl;
use aruna_server::grpc::object::ObjectServiceImpl;
use aruna_server::grpc::projects::ProjectServiceImpl;
use aruna_server::grpc::relations::RelationsServiceImpl;
use aruna_server::grpc::rules::RuleServiceImpl;
use aruna_server::grpc::search::SearchServiceImpl;
use aruna_server::grpc::users::UserServiceImpl;
use aruna_server::hooks;
use aruna_server::hooks::hook_handler::HookMessage;
use aruna_server::middlelayer::db_handler::DatabaseHandler;
use aruna_server::notification::natsio_handler::NatsIoHandler;
use aruna_server::search::meilisearch_client::{MeilisearchClient, MeilisearchIndexes};
use async_channel::Sender;
use std::sync::atomic::AtomicU16;
use std::sync::Arc;
use testcontainers::core::IntoContainerPort;
use testcontainers::{ContainerAsync, GenericImage};
use tokio::task::JoinHandle;
use tonic::transport::{Channel, Server};

pub const SERVER_GRPC_PORT: u16 = 50051;
pub static SERVER_OFFSET: AtomicU16 = AtomicU16::new(0);
const DEFAULT_SERVER_ENCODING_KEY: &str =
    "MC4CAQAwBQYDK2VwBCIEICHl/V9wxvENDJKePwusDhnC7xgaHYV6iHLb0ENJZndj";
const DEFAULT_SERVER_DECODING_KEY: &str =
    "MCowBQYDK2VwAyEA2YfYTgb8Y0LTFr+2Rm2Fkdu38eJTfnsMDH2iZHErBH0=";

pub enum Component {
    Server,
    Proxy,
}

#[allow(dead_code)]
pub struct ServiceBlock {
    // Internal components
    pub db_conn: Arc<Database>,
    pub db_handler: Arc<DatabaseHandler>,
    pub cache: Arc<Cache>,
    pub token_handler: Arc<TokenHandler>,
    pub auth_handler: Arc<PermissionHandler>,
    pub nats_handler: Arc<NatsIoHandler>,
    pub search_handler: Arc<MeilisearchClient>,
    // gRPC services
    pub user_service: UserServiceImpl,
    pub auth_service: AuthorizationServiceImpl,
    pub project_service: ProjectServiceImpl,
    pub collection_service: CollectionServiceImpl,
    pub dataset_service: DatasetServiceImpl,
    pub object_service: ObjectServiceImpl,
    pub search_service: SearchServiceImpl,
    pub license_service: LicensesServiceImpl,
}

/*
#[allow(dead_code)]
/// Creates a database with a random name and
/// optionally imports the server schema and loads initial data
pub async fn init_database(
    postgres: &ContainerAsync<GenericImage>,
    component: Component,
) -> Arc<Database> {
    let host = postgres.get_bridge_ip_address().await.unwrap();
    let port = postgres.get_host_port_ipv4(5432.tcp()).await.unwrap();
    let db_name = util::create_random_database().await;

    // Init database connection
    let db = Database::new(
        host.to_string(),
        port,
        db_name.clone(),
        "postgres".to_string(),
        "postgres".to_string(),
    )
    .unwrap();

    // Load schema and data based on provided component
    match component {
        Component::Server => {
            util::load_schema(&db_name, "../components/server/src/database/schema.sql").await;
            util::load_schema(&db_name, "tests/common/data/initial_data.sql").await;
        }
        Component::Proxy => {
            util::load_schema(&db_name, "../components/data_proxy/src/database/schema.sql").await;
        }
    }

    Arc::new(db)
}
*/

#[allow(dead_code)]
pub async fn init_cache(db: Arc<Database>, sync: bool) -> Arc<Cache> {
    // Init cache
    let cache = Cache::new();

    // Sync cache on demand
    if sync {
        cache.sync_cache(db.clone()).await.unwrap();
    }

    // Return cache
    cache
}

#[allow(dead_code)]
pub async fn init_nats_client(nats: &ContainerAsync<GenericImage>) -> Arc<NatsIoHandler> {
    let host = format!(
        "{}:{}",
        nats.get_bridge_ip_address().await.unwrap(),
        nats.get_host_port_ipv4(4222.tcp()).await.unwrap()
    );

    // Init NatsIo handler
    let client = async_nats::connect(host).await.unwrap();
    let natsio_handler = NatsIoHandler::new(client, "ThisIsASecretToken".to_string(), None)
        .await
        .unwrap();

    Arc::new(natsio_handler)
}

#[allow(dead_code)]
pub async fn init_search_client<'a>(
    meilisearch: &Option<ContainerAsync<GenericImage>>,
) -> Arc<MeilisearchClient> {
    let port = if let Some(container) = meilisearch {
        container.get_host_port_ipv4(7700.tcp()).await.unwrap()
    } else {
        7700
    };
    let host = format!("http://localhost:{port}");

    // Init MeilisearchClient
    let meilisearch_client = MeilisearchClient::new(&host, Some("MASTER_KEY")).unwrap();

    // Create index if not exists on startup
    meilisearch_client
        .get_or_create_index(&MeilisearchIndexes::OBJECT.to_string(), Some("id"))
        .await
        .unwrap();

    Arc::new(meilisearch_client)
}

#[allow(dead_code)]
pub async fn init_database_handler_middlelayer(
    postgres: &ContainerAsync<GenericImage>,
    nats: &ContainerAsync<GenericImage>,
) -> Arc<DatabaseHandler> {
    // Init internal components
    let (_, db) = init_database(postgres, Component::Server).await;
    let nats = init_nats_client(nats).await;
    let cache = init_cache(db.clone(), true).await;
    let (hook_sender, hook_receiver) = async_channel::unbounded();
    let db_handler = init_database_handler(db.clone(), nats, cache.clone(), hook_sender).await;
    let token_handler = init_token_handler(db.clone(), cache.clone()).await;
    let auth = init_permission_handler(cache.clone(), token_handler).await;
    let auth_clone = auth.clone();
    let db_clone = db_handler.clone();
    tokio::spawn(async move {
        let hook_executor =
            hooks::hook_handler::HookHandler::new(hook_receiver, auth_clone, db_clone).await;
        if let Err(err) = hook_executor.run().await {
            tracing::warn!("Hook execution error: {err}")
        }
    });
    db_handler
}

#[allow(dead_code)]
pub async fn init_database_handler(
    db_conn: Arc<Database>,
    nats_handler: Arc<NatsIoHandler>,
    cache: Arc<Cache>,
    hook_sender: Sender<HookMessage>,
) -> Arc<DatabaseHandler> {
    // Init DatabaseHandler
    Arc::new(DatabaseHandler {
        database: db_conn,
        natsio_handler: nats_handler,
        cache,
        hook_sender,
    })
}

#[allow(dead_code)]
pub async fn init_token_handler(db_conn: Arc<Database>, cache: Arc<Cache>) -> Arc<TokenHandler> {
    // Init DatabaseHandler
    Arc::new(
        TokenHandler::new(
            cache,
            db_conn,
            DEFAULT_SERVER_ENCODING_KEY.to_string(),
            DEFAULT_SERVER_DECODING_KEY.to_string(),
        )
        .await
        .unwrap(),
    )
}

#[allow(dead_code)]
pub async fn init_permission_handler(
    cache: Arc<Cache>,
    token_handler: Arc<TokenHandler>,
) -> Arc<PermissionHandler> {
    // Init PermissionHandler
    Arc::new(PermissionHandler::new(cache, token_handler))
}

#[allow(dead_code)]
pub async fn init_storage_status_service(
    postgres: &ContainerAsync<GenericImage>,
    nats: &ContainerAsync<GenericImage>,
) -> StorageStatusServiceImpl {
    // Init database connection
    let (_, db_conn) = init_database(postgres, Component::Server).await;
    // Init Cache
    let cache = init_cache(db_conn.clone(), true).await;
    // Init TokenHandler
    let token_handler = Arc::new(
        TokenHandler::new(
            cache.clone(),
            db_conn.clone(),
            DEFAULT_SERVER_ENCODING_KEY.to_string(),
            DEFAULT_SERVER_DECODING_KEY.to_string(),
        )
        .await
        .unwrap(),
    );
    // Init PermissionHandler
    let perm_handler = Arc::new(PermissionHandler::new(cache.clone(), token_handler.clone()));
    // Init NatsIoHandler
    let nats_client = init_nats_client(nats).await;
    // Init DatabaseHandler
    let (hook_sender, hook_reciever) = async_channel::unbounded();
    let database_handler = init_database_handler(
        db_conn.clone(),
        nats_client.clone(),
        cache.clone(),
        hook_sender,
    )
    .await;
    // Init HookExecutor
    let auth_clone = perm_handler.clone();
    let db_clone = database_handler.clone();
    tokio::spawn(async move {
        let hook_executor =
            hooks::hook_handler::HookHandler::new(hook_reciever, auth_clone, db_clone).await;
        if let Err(err) = hook_executor.run().await {
            tracing::warn!("Hook execution error: {err}")
        }
    });
    // Init project service
    StorageStatusServiceImpl::new(database_handler, perm_handler, cache).await
}

#[allow(dead_code)]
pub async fn init_endpoint_service(
    postgres: &ContainerAsync<GenericImage>,
    nats: &ContainerAsync<GenericImage>,
) -> EndpointServiceImpl {
    // Init database connection
    let (_, db_conn) = init_database(postgres, Component::Server).await;

    // Init Cache
    let cache = init_cache(db_conn.clone(), true).await;

    // Init TokenHandler
    let token_handler = Arc::new(
        TokenHandler::new(
            cache.clone(),
            db_conn.clone(),
            DEFAULT_SERVER_ENCODING_KEY.to_string(),
            DEFAULT_SERVER_DECODING_KEY.to_string(),
        )
        .await
        .unwrap(),
    );

    // Init PermissionHandler
    let perm_handler = Arc::new(PermissionHandler::new(cache.clone(), token_handler.clone()));

    // Init NatsIoHandler
    let nats_client = init_nats_client(nats).await;

    let (hook_sender, hook_reciever) = async_channel::unbounded();
    // Init DatabaseHandler
    let database_handler = init_database_handler(
        db_conn.clone(),
        nats_client.clone(),
        cache.clone(),
        hook_sender,
    )
    .await;
    // Init HookExecutor
    let auth_clone = perm_handler.clone();
    let db_clone = database_handler.clone();
    tokio::spawn(async move {
        let hook_executor =
            hooks::hook_handler::HookHandler::new(hook_reciever, auth_clone, db_clone).await;
        if let Err(err) = hook_executor.run().await {
            tracing::warn!("Hook execution error: {err}")
        }
    });
    // Init project service
    EndpointServiceImpl::new(
        database_handler,
        perm_handler,
        cache,
        DEFAULT_ENDPOINT_ULID.to_string(),
    )
    .await
}

#[allow(dead_code)]
pub async fn init_endpoint_service_manual(
    db: Arc<DatabaseHandler>,
    auth: Arc<PermissionHandler>,
    cache: Arc<Cache>,
) -> EndpointServiceImpl {
    // Init authorization service
    EndpointServiceImpl::new(db, auth, cache, DEFAULT_ENDPOINT_ULID.to_string()).await
}

#[allow(dead_code)]
pub async fn init_user_service_manual(
    db: Arc<DatabaseHandler>,
    auth: Arc<PermissionHandler>,
    cache: Arc<Cache>,
    token_handler: Arc<TokenHandler>,
) -> UserServiceImpl {
    // Init authorization service
    UserServiceImpl::new(db, auth, cache, token_handler, Arc::new(None)).await
}

#[allow(dead_code)]
pub async fn init_auth_service_manual(
    db: Arc<DatabaseHandler>,
    auth: Arc<PermissionHandler>,
    cache: Arc<Cache>,
) -> AuthorizationServiceImpl {
    // Init authorization service
    AuthorizationServiceImpl::new(db, auth, cache).await
}

#[allow(dead_code)]
pub async fn init_project_service_manual(
    db: Arc<DatabaseHandler>,
    auth: Arc<PermissionHandler>,
    cache: Arc<Cache>,
    search: Arc<MeilisearchClient>,
    ep: String,
) -> ProjectServiceImpl {
    // Init project service
    ProjectServiceImpl::new(db, auth, cache, search, ep).await
}

#[allow(dead_code)]
pub async fn init_collection_service_manual(
    db: Arc<DatabaseHandler>,
    auth: Arc<PermissionHandler>,
    cache: Arc<Cache>,
    search: Arc<MeilisearchClient>,
) -> CollectionServiceImpl {
    // Init collection service
    CollectionServiceImpl::new(db, auth, cache, search).await
}

#[allow(dead_code)]
pub async fn init_dataset_service_manual(
    db: Arc<DatabaseHandler>,
    auth: Arc<PermissionHandler>,
    cache: Arc<Cache>,
    search: Arc<MeilisearchClient>,
) -> DatasetServiceImpl {
    // Init collection service
    DatasetServiceImpl::new(db, auth, cache, search).await
}

#[allow(dead_code)]
pub async fn init_object_service_manual(
    db: Arc<DatabaseHandler>,
    auth: Arc<PermissionHandler>,
    cache: Arc<Cache>,
    search: Arc<MeilisearchClient>,
) -> ObjectServiceImpl {
    // Init collection service
    ObjectServiceImpl::new(db, auth, cache, search).await
}

#[allow(dead_code)]
pub async fn init_relation_service_manual(
    db: Arc<DatabaseHandler>,
    auth: Arc<PermissionHandler>,
    cache: Arc<Cache>,
    search: Arc<MeilisearchClient>,
) -> RelationsServiceImpl {
    // Init collection service
    RelationsServiceImpl::new(db, auth, cache, search).await
}

#[allow(dead_code)]
pub async fn init_search_service_manual(
    db: Arc<DatabaseHandler>,
    auth: Arc<PermissionHandler>,
    cache: Arc<Cache>,
    search: Arc<MeilisearchClient>,
) -> SearchServiceImpl {
    // Init collection service
    SearchServiceImpl::new(db, auth, cache, search).await
}

#[allow(dead_code)]
pub async fn init_licenses_service_manual(
    db: Arc<DatabaseHandler>,
    auth: Arc<PermissionHandler>,
    cache: Arc<Cache>,
) -> LicensesServiceImpl {
    // Init collection service
    LicensesServiceImpl::new(db, auth, cache).await
}

pub async fn init_server_services(infra: &TestContainers<'_>) -> ServiceBlock {
    // Init server services
    let (_, db_conn) = init_database(&infra.postgres, Component::Server).await;
    let nats_handler = init_nats_client(&infra.nats).await;
    let cache = init_cache(db_conn.clone(), false).await;
    let (hook_sender, hook_receiver) = async_channel::unbounded();
    let db_handler = init_database_handler(
        db_conn.clone(),
        nats_handler.clone(),
        cache.clone(),
        hook_sender,
    )
    .await;
    let token_handler = init_token_handler(db_conn.clone(), cache.clone()).await;
    let auth_handler = init_permission_handler(cache.clone(), token_handler.clone()).await;
    let search_handler = init_search_client(&infra.meili_clean).await;
    let auth_clone = auth_handler.clone();
    let db_clone = db_handler.clone();
    tokio::spawn(async move {
        let hook_executor =
            hooks::hook_handler::HookHandler::new(hook_receiver, auth_clone, db_clone).await;
        if let Err(err) = hook_executor.run().await {
            tracing::warn!("Hook execution error: {err}")
        }
    });
    cache.sync_cache(db_conn.clone()).await.unwrap();

    // Init gRPC service implementations
    ServiceBlock {
        db_conn,
        db_handler: db_handler.clone(),
        cache: cache.clone(),
        token_handler: token_handler.clone(),
        auth_handler: auth_handler.clone(),
        nats_handler: nats_handler.clone(),
        search_handler: search_handler.clone(),
        user_service: init_user_service_manual(
            db_handler.clone(),
            auth_handler.clone(),
            cache.clone(),
            token_handler,
        )
        .await,
        auth_service: init_auth_service_manual(
            db_handler.clone(),
            auth_handler.clone(),
            cache.clone(),
        )
        .await,
        project_service: init_project_service_manual(
            db_handler.clone(),
            auth_handler.clone(),
            cache.clone(),
            search_handler.clone(),
            DEFAULT_ENDPOINT_ULID.to_string(),
        )
        .await,
        collection_service: init_collection_service_manual(
            db_handler.clone(),
            auth_handler.clone(),
            cache.clone(),
            search_handler.clone(),
        )
        .await,
        dataset_service: init_dataset_service_manual(
            db_handler.clone(),
            auth_handler.clone(),
            cache.clone(),
            search_handler.clone(),
        )
        .await,
        object_service: init_object_service_manual(
            db_handler.clone(),
            auth_handler.clone(),
            cache.clone(),
            search_handler.clone(),
        )
        .await,
        search_service: init_search_service_manual(
            db_handler.clone(),
            auth_handler.clone(),
            cache.clone(),
            search_handler.clone(),
        )
        .await,
        license_service: init_licenses_service_manual(
            db_handler.clone(),
            auth_handler.clone(),
            cache.clone(),
        )
        .await,
    }
}

pub async fn init_server(infra: &TestContainers<'_>) -> anyhow::Result<(JoinHandle<()>, u16)> {
    let service_block = init_server_services(infra).await;

    // Create channel for MV refresh notifications
    let (n_send, n_recv) = async_channel::unbounded();
    // NotificationHandler
    let _ = NotificationHandler::new(
        service_block.db_conn.clone(),
        service_block.cache.clone(),
        service_block.nats_handler.clone(),
        service_block.search_handler.clone(),
        n_send,
    )
    .await
    .unwrap();

    // Init object stats loop
    start_refresh_loop(
        service_block.db_conn.clone(),
        service_block.cache.clone(),
        service_block.search_handler.clone(),
        service_block.nats_handler.clone(),
        n_recv,
        30000,
    )
    .await;

    // Generate ULID for default proxy of server
    let default_endpoint = PROXY_01_DEFAULT_ULID; //DieselUlid::generate();

    // Init server builder
    let builder = Server::builder()
        .http2_keepalive_interval(Some(std::time::Duration::from_secs(15)))
        .add_service(EndpointServiceServer::new(
            EndpointServiceImpl::new(
                service_block.db_handler.clone(),
                service_block.auth_handler.clone(),
                service_block.cache.clone(),
                default_endpoint.to_string(),
            )
            .await,
        ))
        .add_service(AuthorizationServiceServer::new(
            AuthorizationServiceImpl::new(
                service_block.db_handler.clone(),
                service_block.auth_handler.clone(),
                service_block.cache.clone(),
            )
            .await,
        ))
        .add_service(UserServiceServer::new(
            UserServiceImpl::new(
                service_block.db_handler.clone(),
                service_block.auth_handler.clone(),
                service_block.cache.clone(),
                service_block.token_handler.clone(),
                Arc::new(None),
            )
            .await,
        ))
        .add_service(ProjectServiceServer::new(
            ProjectServiceImpl::new(
                service_block.db_handler.clone(),
                service_block.auth_handler.clone(),
                service_block.cache.clone(),
                service_block.search_handler.clone(),
                default_endpoint.to_string(),
            )
            .await,
        ))
        .add_service(CollectionServiceServer::new(
            CollectionServiceImpl::new(
                service_block.db_handler.clone(),
                service_block.auth_handler.clone(),
                service_block.cache.clone(),
                service_block.search_handler.clone(),
            )
            .await,
        ))
        .add_service(DatasetServiceServer::new(
            DatasetServiceImpl::new(
                service_block.db_handler.clone(),
                service_block.auth_handler.clone(),
                service_block.cache.clone(),
                service_block.search_handler.clone(),
            )
            .await,
        ))
        .add_service(ObjectServiceServer::new(
            ObjectServiceImpl::new(
                service_block.db_handler.clone(),
                service_block.auth_handler.clone(),
                service_block.cache.clone(),
                service_block.search_handler.clone(),
            )
            .await,
        ))
        .add_service(RelationsServiceServer::new(
            RelationsServiceImpl::new(
                service_block.db_handler.clone(),
                service_block.auth_handler.clone(),
                service_block.cache.clone(),
                service_block.search_handler.clone(),
            )
            .await,
        ))
        .add_service(EventNotificationServiceServer::new(
            NotificationServiceImpl::new(
                service_block.db_handler.clone(),
                service_block.auth_handler.clone(),
                service_block.cache.clone(),
                service_block.nats_handler.clone(),
            )
            .await,
        ))
        .add_service(SearchServiceServer::new(
            SearchServiceImpl::new(
                service_block.db_handler.clone(),
                service_block.auth_handler.clone(),
                service_block.cache.clone(),
                service_block.search_handler.clone(),
            )
            .await,
        ))
        .add_service(StorageStatusServiceServer::new(
            StorageStatusServiceImpl::new(
                service_block.db_handler.clone(),
                service_block.auth_handler.clone(),
                service_block.cache.clone(),
            )
            .await,
        ))
        .add_service(HooksServiceServer::new(
            HookServiceImpl::new(
                service_block.db_handler.clone(),
                service_block.auth_handler.clone(),
                service_block.cache.clone(),
            )
            .await,
        ))
        .add_service(RulesServiceServer::new(
            RuleServiceImpl::new(
                service_block.db_handler.clone(),
                service_block.auth_handler.clone(),
                service_block.cache.clone(),
            )
            .await,
        ))
        .add_service(LicenseServiceServer::new(
            LicensesServiceImpl::new(
                service_block.db_handler.clone(),
                service_block.auth_handler.clone(),
                service_block.cache.clone(),
            )
            .await,
        ))
        .add_service(DataReplicationServiceServer::new(
            DataReplicationServiceImpl::new(
                service_block.db_handler.clone(),
                service_block.auth_handler.clone(),
                service_block.cache.clone(),
            )
            .await,
        ));

    let port = SERVER_GRPC_PORT + SERVER_OFFSET.fetch_add(10, std::sync::atomic::Ordering::SeqCst);
    let addr: std::net::SocketAddr = format!("0.0.0.0:{}", port).parse().unwrap();
    let handle = tokio::spawn(async move { builder.serve(addr).await.unwrap() });

    // Wait until server accepts connections
    let mut retry_counter: u32 = 0;
    const RETRY_BASE: u64 = 2;
    const RETRIES_MAX: u32 = 10;
    let endpoint = Channel::from_shared(format!("http://0.0.0.0:{port}")).unwrap();
    while let Err(_e) = endpoint.connect().await {
        if retry_counter >= RETRIES_MAX {
            handle.abort();
            anyhow::bail!("Connection retries to Server exceeded.")
        }
        retry_counter += 1;
        let retry_millis = RETRY_BASE.pow(retry_counter);
        tokio::time::sleep(std::time::Duration::from_millis(retry_millis)).await;
    }

    // Return:
    //   - handle for manual task abort
    //   - server port and default endpoint ulid for proxy config
    Ok((handle, port))
}
