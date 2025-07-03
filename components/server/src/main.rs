use std::{str::FromStr, sync::Arc};

use anyhow::Result;
use aruna_rust_api::api::{
    hooks::services::v2::hooks_service_server::HooksServiceServer,
    notification::services::v2::event_notification_service_server::EventNotificationServiceServer,
    storage::services::v2::{
        authorization_service_server::AuthorizationServiceServer,
        collection_service_server::CollectionServiceServer,
        data_replication_service_server::DataReplicationServiceServer,
        dataset_service_server::DatasetServiceServer,
        endpoint_service_server::EndpointServiceServer,
        license_service_server::LicenseServiceServer, object_service_server::ObjectServiceServer,
        project_service_server::ProjectServiceServer,
        relations_service_server::RelationsServiceServer, rules_service_server::RulesServiceServer,
        search_service_server::SearchServiceServer,
        storage_status_service_server::StorageStatusServiceServer,
        user_service_server::UserServiceServer,
    },
};
use aruna_server::{
    auth::{permission_handler::PermissionHandler, token_handler::TokenHandler},
    caching::{cache::Cache, notifications_handler::NotificationHandler},
    database::{
        self,
        crud::CrudDb,
        dsls::{endpoint_dsl::Endpoint, stats_dsl::start_refresh_loop},
    },
    grpc::{
        authorization::AuthorizationServiceImpl, collections::CollectionServiceImpl,
        data_replication::DataReplicationServiceImpl, datasets::DatasetServiceImpl,
        endpoints::EndpointServiceImpl, hooks::HookServiceImpl, info::StorageStatusServiceImpl,
        licenses::LicensesServiceImpl, notification::NotificationServiceImpl,
        object::ObjectServiceImpl, projects::ProjectServiceImpl, relations::RelationsServiceImpl,
        rules::RuleServiceImpl, search::SearchServiceImpl, users::UserServiceImpl,
    },
    hooks,
    middlelayer::db_handler::DatabaseHandler,
    notification::natsio_handler::NatsIoHandler,
    search::meilisearch_client::{MeilisearchClient, MeilisearchIndexes},
    utils::{mailclient::MailClient, search_utils},
};
use diesel_ulid::DieselUlid;
use log::{error, info, warn};
use simple_logger::SimpleLogger;
use tonic::transport::Server;

//noinspection RsTypeCheck
#[tokio::main]
pub async fn main() -> Result<()> {
    // Init logger
    SimpleLogger::new()
        .with_level(log::LevelFilter::Debug)
        .with_module_level("async_nats", log::LevelFilter::Error)
        .with_module_level("h2", log::LevelFilter::Error)
        .with_module_level("hyper", log::LevelFilter::Error)
        .with_module_level("isahc", log::LevelFilter::Error)
        .with_module_level("tokio_postgres", log::LevelFilter::Error)
        .env()
        .init()?;

    // Load env
    dotenvy::from_filename(".env")?;

    // Init database connection
    let db = database::connection::Database::new(
        dotenvy::var("DATABASE_HOST")?,
        dotenvy::var("DATABASE_PORT")?.parse::<u16>()?,
        dotenvy::var("DATABASE_DB")?,
        dotenvy::var("DATABASE_USER")?,
        dotenvy::var("DATABASE_PASSWORD")?,
    )?;
    db.initialize_db().await?;
    let db_arc = Arc::new(db);

    // Init cache
    let cache_arc = Cache::new();

    // Init TokenHandler
    let token_handler = TokenHandler::new(
        cache_arc.clone(),
        db_arc.clone(),
        dotenvy::var("ENCODING_KEY")?,
        dotenvy::var("DECODING_KEY")?,
    )
    .await?;
    let token_handler_arc = Arc::new(token_handler);
    cache_arc.sync_cache(db_arc.clone()).await?;

    // Init PermissionHandler
    let authorizer = PermissionHandler::new(cache_arc.clone(), token_handler_arc.clone());
    let auth_arc = Arc::new(authorizer);

    // Init NatsIoHandler
    let client = async_nats::connect(dotenvy::var("NATS_HOST")?).await?;
    let natsio_handler = NatsIoHandler::new(client, dotenvy::var("REPLY_SECRET")?, None)
        .await
        .map_err(|_| anyhow::anyhow!("NatsIoHandler init failed"))?;
    let natsio_arc = Arc::new(natsio_handler);

    // Create channel for HookHandler
    let (hook_sender, hook_reciever) = async_channel::unbounded();

    // Init DatabaseHandler
    let database_handler = DatabaseHandler {
        database: db_arc.clone(),
        natsio_handler: natsio_arc.clone(),
        cache: cache_arc.clone(),
        hook_sender,
    };
    let db_handler_arc = Arc::new(database_handler);

    // Init HookHandler
    let auth_clone = auth_arc.clone();
    let db_clone = db_handler_arc.clone();
    let hook_handler =
        hooks::hook_handler::HookHandler::new(hook_reciever, auth_clone, db_clone).await;
    hook_handler.run().await?;

    // MeilisearchClient
    let meilisearch_client = MeilisearchClient::new(
        &dotenvy::var("MEILISEARCH_HOST")?,
        Some(&dotenvy::var("MEILISEARCH_API_KEY")?),
    )?;
    let meilisearch_arc = Arc::new(meilisearch_client);

    let db_clone = db_arc.clone();
    let cache_clone = cache_arc.clone();
    let search_clone = meilisearch_arc.clone();
    tokio::spawn(async move {
        // Delete existing index
        if let Err(err) = search_clone.delete_index(MeilisearchIndexes::OBJECT).await {
            warn!("Search index deletion failed: {err}")
        }

        // Re-create index with current config
        if let Err(err) = search_clone
            .get_or_create_index(&MeilisearchIndexes::OBJECT.to_string(), Some("id"))
            .await
        {
            warn!("Search index creation failed: {err}")
        };

        // Full sync search index with database content
        if let Err(err) =
            search_utils::full_sync_search_index(db_clone, cache_clone, search_clone).await
        {
            warn!("Search index full sync failed: {err}")
        };

        Ok::<(), anyhow::Error>(())
    });

    // Create channel for MV refresh notifications
    let (n_send, n_recv) = async_channel::unbounded();
    // NotificationHandler
    let _ = NotificationHandler::new(
        db_arc.clone(),
        cache_arc.clone(),
        natsio_arc.clone(),
        meilisearch_arc.clone(),
        n_send,
    )
    .await?;

    // Init object stats loop
    let refresh_interval = dotenvy::var("REFRESH_INTERVAL")?
        .parse::<i64>()
        .unwrap_or_else(|err| {
            error!("Could not parse refresh interval: {err}");
            300000 // 5 minutes is default
        });

    start_refresh_loop(
        db_arc.clone(),
        cache_arc.clone(),
        meilisearch_arc.clone(),
        natsio_arc.clone(),
        n_recv,
        refresh_interval,
    )
    .await;

    // init MailClient
    let mailclient: Arc<Option<MailClient>> = if !dotenvy::var("ARUNA_DEV_ENV")?.parse::<bool>()? {
        Arc::new(Some(MailClient::new()?))
    } else {
        Arc::new(None)
    };

    let default_endpoint = dotenvy::var("DEFAULT_DATAPROXY_ULID")?;

    // Init server builder
    let mut builder = Server::builder()
        .http2_keepalive_interval(Some(std::time::Duration::from_secs(15)))
        .add_service(EndpointServiceServer::new(
            EndpointServiceImpl::new(
                db_handler_arc.clone(),
                auth_arc.clone(),
                cache_arc.clone(),
                default_endpoint.to_string(),
            )
            .await,
        ));

    // Check default endpoint -> Only endpoint service available
    let client = db_arc.get_client().await?;

    if !&default_endpoint.is_empty()
        && Endpoint::get(DieselUlid::from_str(&default_endpoint)?, &client)
            .await?
            .is_some()
    {
        // Add other services
        builder = builder
            .add_service(AuthorizationServiceServer::new(
                AuthorizationServiceImpl::new(
                    db_handler_arc.clone(),
                    auth_arc.clone(),
                    cache_arc.clone(),
                )
                .await,
            ))
            .add_service(UserServiceServer::new(
                UserServiceImpl::new(
                    db_handler_arc.clone(),
                    auth_arc.clone(),
                    cache_arc.clone(),
                    token_handler_arc.clone(),
                    mailclient.clone(),
                )
                .await,
            ))
            .add_service(ProjectServiceServer::new(
                ProjectServiceImpl::new(
                    db_handler_arc.clone(),
                    auth_arc.clone(),
                    cache_arc.clone(),
                    meilisearch_arc.clone(),
                    default_endpoint.clone(),
                )
                .await,
            ))
            .add_service(CollectionServiceServer::new(
                CollectionServiceImpl::new(
                    db_handler_arc.clone(),
                    auth_arc.clone(),
                    cache_arc.clone(),
                    meilisearch_arc.clone(),
                )
                .await,
            ))
            .add_service(DatasetServiceServer::new(
                DatasetServiceImpl::new(
                    db_handler_arc.clone(),
                    auth_arc.clone(),
                    cache_arc.clone(),
                    meilisearch_arc.clone(),
                )
                .await,
            ))
            .add_service(ObjectServiceServer::new(
                ObjectServiceImpl::new(
                    db_handler_arc.clone(),
                    auth_arc.clone(),
                    cache_arc.clone(),
                    meilisearch_arc.clone(),
                )
                .await,
            ))
            .add_service(RelationsServiceServer::new(
                RelationsServiceImpl::new(
                    db_handler_arc.clone(),
                    auth_arc.clone(),
                    cache_arc.clone(),
                    meilisearch_arc.clone(),
                )
                .await,
            ))
            .add_service(EventNotificationServiceServer::new(
                NotificationServiceImpl::new(
                    db_handler_arc.clone(),
                    auth_arc.clone(),
                    cache_arc.clone(),
                    natsio_arc.clone(),
                )
                .await,
            ))
            .add_service(SearchServiceServer::new(
                SearchServiceImpl::new(
                    db_handler_arc.clone(),
                    auth_arc.clone(),
                    cache_arc.clone(),
                    meilisearch_arc.clone(),
                )
                .await,
            ))
            .add_service(StorageStatusServiceServer::new(
                StorageStatusServiceImpl::new(
                    db_handler_arc.clone(),
                    auth_arc.clone(),
                    cache_arc.clone(),
                )
                .await,
            ))
            .add_service(HooksServiceServer::new(
                HookServiceImpl::new(db_handler_arc.clone(), auth_arc.clone(), cache_arc.clone())
                    .await,
            ))
            .add_service(RulesServiceServer::new(
                RuleServiceImpl::new(db_handler_arc.clone(), auth_arc.clone(), cache_arc.clone())
                    .await,
            ))
            .add_service(LicenseServiceServer::new(
                LicensesServiceImpl::new(
                    db_handler_arc.clone(),
                    auth_arc.clone(),
                    cache_arc.clone(),
                )
                .await,
            ))
            .add_service(DataReplicationServiceServer::new(
                DataReplicationServiceImpl::new(
                    db_handler_arc.clone(),
                    auth_arc.clone(),
                    cache_arc.clone(),
                )
                .await,
            ));
    }

    // Do it.
    //let addr: std::net::SocketAddr = "0.0.0.0:50051".parse()?;
    let addr: std::net::SocketAddr = dotenvy::var("ARUNA_SOCKET_ADDRESS")?.parse()?;
    info!("ArunaServer listening on {addr}");
    builder.serve(addr).await?;

    // Cron scheduler?

    Ok(())
}
