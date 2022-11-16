use std::sync::Arc;

use crate::config::ArunaServerConfig;
use crate::database::connection::Database;
use crate::database::cron::{Scheduler, Task};
use crate::server::services::authz::Authz;
use crate::server::services::endpoint::EndpointServiceImpl;
use crate::server::services::info::{ResourceInfoServiceImpl, StorageInfoServiceImpl};
use crate::server::services::internal_notifications::InternalEventServiceImpl;
use crate::server::services::objectgroup::ObjectGroupServiceImpl;
use crate::server::services::project::ProjectServiceImpl;
use crate::server::services::user::UserServiceImpl;
use aruna_rust_api::api::internal::v1::internal_event_service_server::InternalEventServiceServer;
use aruna_rust_api::api::storage::services::v1::collection_service_server::CollectionServiceServer;
use aruna_rust_api::api::storage::services::v1::endpoint_service_server::EndpointServiceServer;
use aruna_rust_api::api::storage::services::v1::object_group_service_server::ObjectGroupServiceServer;
use aruna_rust_api::api::storage::services::v1::object_service_server::ObjectServiceServer;
use aruna_rust_api::api::storage::services::v1::project_service_server::ProjectServiceServer;
use aruna_rust_api::api::storage::services::v1::resource_info_service_server::ResourceInfoServiceServer;
use aruna_rust_api::api::storage::services::v1::storage_info_service_server::StorageInfoServiceServer;
use aruna_rust_api::api::storage::services::v1::user_service_server::UserServiceServer;
use tonic::transport::Server;

use super::services::collection::CollectionServiceImpl;
use super::services::object::ObjectServiceImpl;

pub struct ServiceServer {}

impl ServiceServer {
    pub async fn run(&self) {
        // Read config relative to binary
        let config = ArunaServerConfig::new();

        // Connects to database
        let db = Database::new(&config.config.database_url);
        let db_ref = Arc::new(db);

        // Initialize instance default data proxy endpoint
        let default_endpoint = db_ref
            .init_default_endpoint(config.config.default_endpoint)
            .unwrap();

        let mut cron_scheduler = Scheduler::new();
        cron_scheduler.add(Task::new(
            |db| {
                let res = db.update_collection_views();
                if res.is_err() {
                    log::info!(
                        "Update of cron: materialized collection view failed, with: {:#?}",
                        res
                    )
                }
            },
            "collection_views",
            300,
            db_ref.clone(),
        ));
        cron_scheduler.add(Task::new(
            |db| {
                let res = db.update_object_group_views();
                if res.is_err() {
                    log::info!(
                        "Update of cron: materialized object_group view failed, with: {:#?}",
                        res
                    )
                }
            },
            "object_views",
            300,
            db_ref.clone(),
        ));

        tokio::spawn(async move {
            cron_scheduler.run().await;
        });

        // Upstart server
        let addr = "0.0.0.0:50051".parse().unwrap();
        let authz = Arc::new(Authz::new(db_ref.clone()).await);

        let endpoint_service =
            EndpointServiceImpl::new(db_ref.clone(), authz.clone(), default_endpoint.clone()).await;
        let project_service = ProjectServiceImpl::new(db_ref.clone(), authz.clone()).await;
        let user_service = UserServiceImpl::new(db_ref.clone(), authz.clone()).await;
        let collection_service = CollectionServiceImpl::new(db_ref.clone(), authz.clone()).await;
        let object_service =
            ObjectServiceImpl::new(db_ref.clone(), authz.clone(), default_endpoint.clone()).await;
        let object_group_service = ObjectGroupServiceImpl::new(db_ref.clone(), authz.clone()).await;

        let resource_info_service =
            ResourceInfoServiceImpl::new(db_ref.clone(), authz.clone()).await;

        let storage_info_service = StorageInfoServiceImpl::new(db_ref.clone(), authz.clone()).await;

        let internal_event_service =
            InternalEventServiceImpl::new(db_ref.clone(), authz.clone()).await;

        log::info!("ArunaServer listening on {}", addr);

        Server::builder()
            .add_service(EndpointServiceServer::new(endpoint_service))
            .add_service(UserServiceServer::new(user_service))
            .add_service(ProjectServiceServer::new(project_service))
            .add_service(CollectionServiceServer::new(collection_service))
            .add_service(ObjectServiceServer::new(object_service))
            .add_service(ObjectGroupServiceServer::new(object_group_service))
            .add_service(ResourceInfoServiceServer::new(resource_info_service))
            .add_service(StorageInfoServiceServer::new(storage_info_service))
            .add_service(InternalEventServiceServer::new(internal_event_service))
            .serve(addr)
            .await
            .unwrap();
    }
}
