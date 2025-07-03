use aruna_server::auth::permission_handler::PermissionHandler;
use aruna_server::auth::token_handler::TokenHandler;
use aruna_server::caching::cache::Cache;
use aruna_server::database::connection::Database;
use aruna_server::grpc::authorization::AuthorizationServiceImpl;
use aruna_server::grpc::collections::CollectionServiceImpl;
use aruna_server::grpc::datasets::DatasetServiceImpl;
use aruna_server::grpc::endpoints::EndpointServiceImpl;
use aruna_server::grpc::info::StorageStatusServiceImpl;
use aruna_server::grpc::licenses::LicensesServiceImpl;
use aruna_server::grpc::object::ObjectServiceImpl;
use aruna_server::grpc::projects::ProjectServiceImpl;
use aruna_server::grpc::relations::RelationsServiceImpl;
use aruna_server::grpc::search::SearchServiceImpl;
use aruna_server::grpc::users::UserServiceImpl;
use aruna_server::hooks;
use aruna_server::hooks::hook_handler::HookMessage;
use aruna_server::middlelayer::db_handler::DatabaseHandler;
use aruna_server::notification::natsio_handler::NatsIoHandler;
use aruna_server::search::meilisearch_client::{MeilisearchClient, MeilisearchIndexes};
use async_channel::Sender;
use std::sync::Arc;

use super::test_utils::DEFAULT_ENDPOINT_ULID;

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

#[allow(dead_code)]
pub async fn init_database() -> Arc<Database> {
    // Load env
    dotenvy::from_filename(".env").unwrap();

    // Init database connection
    let db = Database::new(
        dotenvy::var("DATABASE_HOST").unwrap(),
        dotenvy::var("DATABASE_PORT")
            .unwrap()
            .parse::<u16>()
            .unwrap(),
        dotenvy::var("DATABASE_DB").unwrap(),
        dotenvy::var("DATABASE_USER").unwrap(),
        dotenvy::var("DATABASE_PASSWORD").unwrap(),
    )
    .unwrap();

    db.initialize_db().await.unwrap();

    Arc::new(db)
}

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
pub async fn init_nats_client() -> Arc<NatsIoHandler> {
    // Load env
    dotenvy::from_filename(".env").unwrap();

    // Init NatsIoHandler
    let client = async_nats::connect(dotenvy::var("NATS_HOST").unwrap())
        .await
        .unwrap();

    let natsio_handler = NatsIoHandler::new(client, dotenvy::var("REPLY_SECRET").unwrap(), None)
        .await
        .unwrap();

    Arc::new(natsio_handler)
}

#[allow(dead_code)]
pub async fn init_search_client() -> Arc<MeilisearchClient> {
    // Load env
    dotenvy::from_filename(".env").unwrap();

    // Init MeilisearchClient
    let meilisearch_client = MeilisearchClient::new(
        &dotenvy::var("MEILISEARCH_HOST").unwrap(),
        Some(&dotenvy::var("MEILISEARCH_API_KEY").unwrap()),
    )
    .unwrap();

    // Create index if not exists on startup
    meilisearch_client
        .get_or_create_index(&MeilisearchIndexes::OBJECT.to_string(), Some("id"))
        .await
        .unwrap();

    Arc::new(meilisearch_client)
}

#[allow(dead_code)]
pub async fn init_database_handler_middlelayer() -> Arc<DatabaseHandler> {
    // Init internal components
    let db = init_database().await;
    let nats = init_nats_client().await;
    let cache = init_cache(db.clone(), true).await;
    let (hook_sender, hook_reciever) = async_channel::unbounded();
    let db_handler = init_database_handler(db.clone(), nats, cache.clone(), hook_sender).await;
    let token_handler = init_token_handler(db.clone(), cache.clone()).await;
    let auth = init_permission_handler(cache.clone(), token_handler).await;
    let auth_clone = auth.clone();
    let db_clone = db_handler.clone();
    tokio::spawn(async move {
        let hook_executor =
            hooks::hook_handler::HookHandler::new(hook_reciever, auth_clone, db_clone).await;
        if let Err(err) = hook_executor.run().await {
            log::warn!("Hook execution error: {err}")
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
            dotenvy::var("ENCODING_KEY").unwrap(),
            dotenvy::var("DECODING_KEY").unwrap(),
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
pub async fn init_storage_status_service() -> StorageStatusServiceImpl {
    // Load env
    dotenvy::from_filename(".env").unwrap();
    // Init database connection
    let db_conn = init_database().await;
    // Init Cache
    let cache = init_cache(db_conn.clone(), true).await;
    // Init TokenHandler
    let token_handler = Arc::new(
        TokenHandler::new(
            cache.clone(),
            db_conn.clone(),
            dotenvy::var("ENCODING_KEY").unwrap(),
            dotenvy::var("DECODING_KEY").unwrap(),
        )
        .await
        .unwrap(),
    );
    // Init PermissionHandler
    let perm_handler = Arc::new(PermissionHandler::new(cache.clone(), token_handler.clone()));
    // Init NatsIoHandler
    let nats_client = init_nats_client().await;
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
            log::warn!("Hook execution error: {err}")
        }
    });
    // Init project service
    StorageStatusServiceImpl::new(database_handler, perm_handler, cache).await
}

#[allow(dead_code)]
pub async fn init_endpoint_service() -> EndpointServiceImpl {
    // Load env
    dotenvy::from_filename(".env").unwrap();

    // Init database connection
    let db_conn = init_database().await;

    // Init Cache
    let cache = init_cache(db_conn.clone(), true).await;

    // Init TokenHandler
    let token_handler = Arc::new(
        TokenHandler::new(
            cache.clone(),
            db_conn.clone(),
            dotenvy::var("ENCODING_KEY").unwrap(),
            dotenvy::var("DECODING_KEY").unwrap(),
        )
        .await
        .unwrap(),
    );

    // Init PermissionHandler
    let perm_handler = Arc::new(PermissionHandler::new(cache.clone(), token_handler.clone()));

    // Init NatsIoHandler
    let nats_client = init_nats_client().await;

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
            log::warn!("Hook execution error: {err}")
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
pub async fn init_project_service() -> ProjectServiceImpl {
    // Load env
    dotenvy::from_filename(".env").unwrap();

    // Init database connection
    let db_conn = init_database().await;

    // Init Cache
    let cache = init_cache(db_conn.clone(), true).await;

    // Init TokenHandler
    let token_handler = Arc::new(
        TokenHandler::new(
            cache.clone(),
            db_conn.clone(),
            dotenvy::var("ENCODING_KEY").unwrap(),
            dotenvy::var("DECODING_KEY").unwrap(),
        )
        .await
        .unwrap(),
    );

    // Init PermissionHandler
    let perm_handler = Arc::new(PermissionHandler::new(cache.clone(), token_handler.clone()));

    // Init MeilisearchClient
    let search_client = init_search_client().await;

    // Init NatsIoHandler
    let nats_client = init_nats_client().await;

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
            log::warn!("Hook execution error: {err}")
        }
    });
    // Init project service
    ProjectServiceImpl::new(
        database_handler,
        perm_handler,
        cache,
        search_client,
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

#[allow(dead_code)]
pub async fn init_grpc_services() -> (
    AuthorizationServiceImpl,
    ProjectServiceImpl,
    CollectionServiceImpl,
    DatasetServiceImpl,
    ObjectServiceImpl,
    RelationsServiceImpl,
) {
    // Init internal components
    let db = init_database().await;
    let nats = init_nats_client().await;
    let cache = init_cache(db.clone(), true).await;
    let (hook_sender, hook_reciever) = async_channel::unbounded();
    let db_handler = init_database_handler(db.clone(), nats, cache.clone(), hook_sender).await;
    let token_handler = init_token_handler(db.clone(), cache.clone()).await;
    let auth = init_permission_handler(cache.clone(), token_handler).await;
    let search = init_search_client().await;
    let auth_clone = auth.clone();
    let db_clone = db_handler.clone();
    tokio::spawn(async move {
        let hook_executor =
            hooks::hook_handler::HookHandler::new(hook_reciever, auth_clone, db_clone).await;
        if let Err(err) = hook_executor.run().await {
            log::warn!("Hook execution error: {err}")
        }
    });

    // Init gRPC service implementations
    (
        init_auth_service_manual(db_handler.clone(), auth.clone(), cache.clone()).await,
        init_project_service_manual(
            db_handler.clone(),
            auth.clone(),
            cache.clone(),
            search.clone(),
            DEFAULT_ENDPOINT_ULID.to_string(),
        )
        .await,
        init_collection_service_manual(
            db_handler.clone(),
            auth.clone(),
            cache.clone(),
            search.clone(),
        )
        .await,
        init_dataset_service_manual(
            db_handler.clone(),
            auth.clone(),
            cache.clone(),
            search.clone(),
        )
        .await,
        init_object_service_manual(
            db_handler.clone(),
            auth.clone(),
            cache.clone(),
            search.clone(),
        )
        .await,
        init_relation_service_manual(db_handler, auth, cache, search).await,
    )
}

#[allow(dead_code)]
pub async fn init_service_block() -> ServiceBlock {
    // Init internal components
    let db_conn = init_database().await;
    let nats_handler = init_nats_client().await;
    let cache = init_cache(db_conn.clone(), true).await;
    let (hook_sender, hook_reciever) = async_channel::unbounded();
    let db_handler = init_database_handler(
        db_conn.clone(),
        nats_handler.clone(),
        cache.clone(),
        hook_sender,
    )
    .await;
    let token_handler = init_token_handler(db_conn.clone(), cache.clone()).await;
    let auth_handler = init_permission_handler(cache.clone(), token_handler.clone()).await;
    let search_handler = init_search_client().await;
    let auth_clone = auth_handler.clone();
    let db_clone = db_handler.clone();
    tokio::spawn(async move {
        let hook_executor =
            hooks::hook_handler::HookHandler::new(hook_reciever, auth_clone, db_clone).await;
        if let Err(err) = hook_executor.run().await {
            log::warn!("Hook execution error: {err}")
        }
    });

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
