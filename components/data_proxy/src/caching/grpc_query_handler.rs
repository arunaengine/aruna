use crate::replication::replication_handler::Direction;
use crate::replication::replication_handler::ReplicationMessage;
use crate::structs::FileFormat;
use crate::structs::Object as DPObject;
use crate::structs::ObjectType;
use crate::structs::PubKey;
use crate::structs::TypedRelation;
use anyhow::anyhow;
use anyhow::Result;
use aruna_rust_api::api::dataproxy::services::v2::dataproxy_replication_service_client::DataproxyReplicationServiceClient;
use aruna_rust_api::api::dataproxy::services::v2::IngestResource;
use aruna_rust_api::api::dataproxy::services::v2::PullReplicationRequest;
use aruna_rust_api::api::dataproxy::services::v2::PullReplicationResponse;
use aruna_rust_api::api::notification::services::v2::announcement_event;
use aruna_rust_api::api::notification::services::v2::event_message::MessageVariant;
use aruna_rust_api::api::notification::services::v2::AcknowledgeMessageBatchRequest;
use aruna_rust_api::api::notification::services::v2::AnnouncementEvent;
use aruna_rust_api::api::notification::services::v2::EventMessage;
use aruna_rust_api::api::notification::services::v2::EventVariant;
use aruna_rust_api::api::notification::services::v2::GetEventMessageStreamRequest;
use aruna_rust_api::api::notification::services::v2::Reply;
use aruna_rust_api::api::notification::services::v2::ResourceEvent;
use aruna_rust_api::api::notification::services::v2::UserEvent;
use aruna_rust_api::api::storage::models::v2::data_endpoint::Variant;
use aruna_rust_api::api::storage::models::v2::generic_resource::Resource;
use aruna_rust_api::api::storage::models::v2::Collection;
use aruna_rust_api::api::storage::models::v2::Dataset;
use aruna_rust_api::api::storage::models::v2::EndpointHostVariant;
use aruna_rust_api::api::storage::models::v2::FullSync;
use aruna_rust_api::api::storage::models::v2::GenericResource;
use aruna_rust_api::api::storage::models::v2::Hash;
use aruna_rust_api::api::storage::models::v2::KeyValue;
use aruna_rust_api::api::storage::models::v2::KeyValueVariant;
use aruna_rust_api::api::storage::models::v2::Object;
use aruna_rust_api::api::storage::models::v2::Project;
use aruna_rust_api::api::storage::models::v2::Pubkey;
use aruna_rust_api::api::storage::models::v2::ReplicationStatus;
use aruna_rust_api::api::storage::models::v2::User as GrpcUser;
use aruna_rust_api::api::storage::services::v2::create_dataset_request;
use aruna_rust_api::api::storage::services::v2::create_object_request;
use aruna_rust_api::api::storage::services::v2::data_replication_service_client::DataReplicationServiceClient;
use aruna_rust_api::api::storage::services::v2::full_sync_endpoint_response::Target;
use aruna_rust_api::api::storage::services::v2::get_endpoint_request::Endpoint;
use aruna_rust_api::api::storage::services::v2::CreateCollectionRequest;
use aruna_rust_api::api::storage::services::v2::CreateDatasetRequest;
use aruna_rust_api::api::storage::services::v2::CreateObjectRequest;
use aruna_rust_api::api::storage::services::v2::CreateProjectRequest;
use aruna_rust_api::api::storage::services::v2::DeleteObjectRequest;
use aruna_rust_api::api::storage::services::v2::FinishObjectStagingRequest;
use aruna_rust_api::api::storage::services::v2::FullSyncEndpointRequest;
use aruna_rust_api::api::storage::services::v2::GetCollectionRequest;
use aruna_rust_api::api::storage::services::v2::GetDatasetRequest;
use aruna_rust_api::api::storage::services::v2::GetEndpointRequest;
use aruna_rust_api::api::storage::services::v2::GetObjectRequest;
use aruna_rust_api::api::storage::services::v2::GetProjectRequest;
use aruna_rust_api::api::storage::services::v2::GetPubkeysRequest;
use aruna_rust_api::api::storage::services::v2::GetUserRedactedRequest;
use aruna_rust_api::api::storage::services::v2::SetObjectHashesRequest;
use aruna_rust_api::api::storage::services::v2::UpdateObjectRequest;
use aruna_rust_api::api::storage::services::v2::UpdateProjectKeyValuesRequest;
use aruna_rust_api::api::storage::services::v2::UpdateReplicationStatusRequest;
use aruna_rust_api::api::{
    notification::services::v2::event_notification_service_client::EventNotificationServiceClient,
    storage::services::v2::{
        collection_service_client::CollectionServiceClient,
        dataset_service_client::DatasetServiceClient,
        endpoint_service_client::EndpointServiceClient, object_service_client::ObjectServiceClient,
        project_service_client::ProjectServiceClient,
        storage_status_service_client::StorageStatusServiceClient,
        user_service_client::UserServiceClient,
    },
};
use diesel_ulid::DieselUlid;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::AsciiMetadataKey;
use tonic::metadata::AsciiMetadataValue;
use tonic::metadata::MetadataMap;
use tonic::transport::Channel;
use tonic::transport::ClientTlsConfig;
use tonic::Request;
use tonic::Streaming;
use tracing::debug;
use tracing::error;
use tracing::trace;
use tracing::Instrument;

use super::cache::Cache;

pub struct GrpcQueryHandler {
    project_service: ProjectServiceClient<Channel>,
    collection_service: CollectionServiceClient<Channel>,
    dataset_service: DatasetServiceClient<Channel>,
    object_service: ObjectServiceClient<Channel>,
    user_service: UserServiceClient<Channel>,
    endpoint_service: EndpointServiceClient<Channel>,
    storage_status_service: StorageStatusServiceClient<Channel>,
    event_notification_service: EventNotificationServiceClient<Channel>,
    data_replication_service: DataReplicationServiceClient<Channel>,
    cache: Arc<Cache>,
    endpoint_id: String,
    long_lived_token: String,
}

impl GrpcQueryHandler {
    #[tracing::instrument(level = "trace", skip(server, cache, endpoint_id))]
    #[allow(dead_code)]
    pub async fn new(
        server: impl Into<String>,
        cache: Arc<Cache>,
        endpoint_id: String,
    ) -> Result<Self> {
        // Check if server host url is tls
        let server_url: String = server.into();
        let endpoint = if server_url.starts_with("https") {
            Channel::from_shared(server_url)
                .map_err(|e| {
                    error!(error = ?e, msg = e.to_string());
                    e
                })?
                .tls_config(ClientTlsConfig::new())
                .map_err(|e| {
                    error!(error = ?e, msg = e.to_string());
                    e
                })?
        } else {
            Channel::from_shared(server_url).map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                e
            })?
        };
        let channel = endpoint.connect().await.map_err(|e| {
            error!(error = ?e, msg = e.to_string());
            e
        })?;

        let project_service = ProjectServiceClient::new(channel.clone());

        let collection_service = CollectionServiceClient::new(channel.clone());

        let dataset_service = DatasetServiceClient::new(channel.clone());

        let object_service = ObjectServiceClient::new(channel.clone());

        let user_service = UserServiceClient::new(channel.clone());

        let endpoint_service = EndpointServiceClient::new(channel.clone());

        let storage_status_service = StorageStatusServiceClient::new(channel.clone());

        let event_notification_service = EventNotificationServiceClient::new(channel.clone());

        let data_replication_service = DataReplicationServiceClient::new(channel.clone());

        let long_lived_token = cache
            .auth
            .read()
            .await
            .as_ref()
            .ok_or_else(|| {
                error!(error = "No auth found");
                anyhow!("No auth found")
            })?
            .sign_notification_token()?;

        let handler = GrpcQueryHandler {
            project_service,
            collection_service,
            dataset_service,
            object_service,
            user_service,
            endpoint_service,
            storage_status_service,
            event_notification_service,
            data_replication_service,
            cache,
            endpoint_id,
            long_lived_token,
        };

        let pks = handler
            .get_pubkeys()
            .await?
            .into_iter()
            .map(PubKey::from)
            .collect();
        handler.cache.set_pubkeys(pks).await?;

        Ok(handler)
    }
}

// Aruna grpc request section
impl GrpcQueryHandler {
    pub fn add_token_to_md(md: &mut MetadataMap, token: &str) -> Result<()> {
        let key = AsciiMetadataKey::from_bytes("authorization".as_bytes()).map_err(|e| {
            error!(error = ?e, msg = e.to_string());
            e
        })?;
        let value = AsciiMetadataValue::try_from(format!("Bearer {token}")).map_err(|e| {
            error!(error = ?e, msg = e.to_string());
            e
        })?;
        md.append(key, value);
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, _checksum))]
    pub async fn get_user(&self, id: DieselUlid, _checksum: String) -> Result<GrpcUser> {
        let mut req = Request::new(GetUserRedactedRequest {
            user_id: id.to_string(),
        });

        Self::add_token_to_md(req.metadata_mut(), &self.long_lived_token)?;

        let user = self
            .user_service
            .clone()
            .get_user_redacted(req)
            .await
            .map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                e
            })?
            .into_inner()
            .user
            .ok_or_else(|| {
                error!(error = "Unknown user");
                anyhow!("Unknown user")
            })?;
        Ok(user)
    }
    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_pubkeys(&self) -> Result<Vec<Pubkey>> {
        let mut req = Request::new(GetPubkeysRequest {});

        Self::add_token_to_md(req.metadata_mut(), &self.long_lived_token)?;

        Ok(self
            .storage_status_service
            .clone()
            .get_pubkeys(req)
            .await
            .map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                e
            })?
            .into_inner()
            .pubkeys)
    }

    #[tracing::instrument(level = "trace", skip(self, object, token))]
    pub async fn create_project(&self, object: DPObject, token: &str) -> Result<DPObject> {
        let mut req = Request::new(CreateProjectRequest::from(object));

        req.get_mut()
            .preferred_endpoint
            .clone_from(&self.endpoint_id);
        Self::add_token_to_md(req.metadata_mut(), token)?;

        let response = self
            .project_service
            .clone()
            .create_project(req)
            .await
            .map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                e
            })?
            .into_inner()
            .project
            .ok_or_else(|| {
                error!(error = "unknown project");
                anyhow!("unknown project")
            })?;

        let object = DPObject::try_from(response)?;

        self.cache.upsert_object(object.clone()).await?;

        Ok(object)
    }

    #[tracing::instrument(level = "trace", skip(self, id, _checksum))]
    async fn get_project(&self, id: &DieselUlid, _checksum: String) -> Result<Project> {
        let mut req = Request::new(GetProjectRequest {
            project_id: id.to_string(),
        });

        Self::add_token_to_md(req.metadata_mut(), &self.long_lived_token)?;

        self.project_service
            .clone()
            .get_project(req)
            .await
            .map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                e
            })?
            .into_inner()
            .project
            .ok_or_else(|| {
                error!(error = "unknown project");
                anyhow!("unknown project")
            })
    }

    #[tracing::instrument(level = "trace", skip(self, _checksum))]
    async fn get_collection(&self, id: &DieselUlid, _checksum: String) -> Result<Collection> {
        let mut req = Request::new(GetCollectionRequest {
            collection_id: id.to_string(),
        });

        Self::add_token_to_md(req.metadata_mut(), &self.long_lived_token)?;

        self.collection_service
            .clone()
            .get_collection(req)
            .await
            .map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                e
            })?
            .into_inner()
            .collection
            .ok_or_else(|| {
                error!(error = "unknown collection");
                anyhow!("unknown collection")
            })
    }

    #[tracing::instrument(level = "trace", skip(self, object, token))]
    pub async fn create_collection(&self, object: DPObject, token: &str) -> Result<DPObject> {
        let mut req = Request::new(CreateCollectionRequest::from(object));

        Self::add_token_to_md(req.metadata_mut(), token)?;

        let response = self
            .collection_service
            .clone()
            .create_collection(req)
            .await
            .map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                e
            })?
            .into_inner()
            .collection
            .ok_or_else(|| {
                error!(error = "unknown collection");
                anyhow!("unknown collection")
            })?;

        let object = DPObject::try_from(response)?;

        self.cache.upsert_object(object.clone()).await?;

        Ok(object)
    }

    #[tracing::instrument(level = "trace", skip(self, id, _checksum))]
    async fn get_dataset(&self, id: &DieselUlid, _checksum: String) -> Result<Dataset> {
        let mut req = Request::new(GetDatasetRequest {
            dataset_id: id.to_string(),
        });

        Self::add_token_to_md(req.metadata_mut(), &self.long_lived_token)?;

        self.dataset_service
            .clone()
            .get_dataset(req)
            .await
            .map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                e
            })?
            .into_inner()
            .dataset
            .ok_or_else(|| {
                error!(error = "unknown dataset");
                anyhow!("unknown dataset")
            })
    }

    #[tracing::instrument(level = "trace", skip(self, object, token))]
    pub async fn create_dataset(&self, object: DPObject, token: &str) -> Result<DPObject> {
        let mut req = Request::new(CreateDatasetRequest::from(object));

        Self::add_token_to_md(req.metadata_mut(), token)?;

        let response = self
            .dataset_service
            .clone()
            .create_dataset(req)
            .await
            .map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                e
            })?
            .into_inner()
            .dataset
            .ok_or_else(|| {
                error!(error = "unknown dataset");
                anyhow!("unknown dataset")
            })?;

        let object = DPObject::try_from(response)?;

        self.cache.upsert_object(object.clone()).await?;

        Ok(object)
    }

    #[tracing::instrument(level = "trace", skip(self, _checksum))]
    async fn get_object(&self, id: &DieselUlid, _checksum: String) -> Result<Object> {
        let mut req = Request::new(GetObjectRequest {
            object_id: id.to_string(),
        });

        Self::add_token_to_md(req.metadata_mut(), &self.long_lived_token)?;

        self.object_service
            .clone()
            .get_object(req)
            .await
            .map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                e
            })?
            .into_inner()
            .object
            .ok_or_else(|| {
                error!(error = "unknown object");
                anyhow!("unknown object")
            })
    }

    #[tracing::instrument(level = "trace", skip(self, object, token))]
    pub async fn create_object(&self, object: DPObject, token: &str) -> Result<DPObject> {
        trace!(?object, "Creating object");

        let mut req = Request::new(CreateObjectRequest::from(object));

        Self::add_token_to_md(req.metadata_mut(), token)?;

        let response = self
            .object_service
            .clone()
            .create_object(req)
            .await
            .map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                e
            })?
            .into_inner()
            .object
            .ok_or_else(|| {
                error!(error = "unknown object");
                anyhow!("unknown object")
            })?;

        let object = DPObject::try_from(response)?;

        self.cache.upsert_object(object.clone()).await?;
        Ok(object)
    }

    #[tracing::instrument(level = "trace", skip(self, object_id, token))]
    pub async fn delete_object(&self, object_id: &DieselUlid, token: &str) -> Result<()> {
        trace!(?object_id, "Deleting object");

        // Check if data proxy local temp resource
        if let Some(loc) = self.cache.get_resource_cloned(object_id, false).await?.1 {
            if loc.file_format == FileFormat::Pseudo {
                // Early return as the resource does only exist locally
                return Ok(());
            }
        }

        // Request object deletion in server
        let mut req = Request::new(DeleteObjectRequest {
            object_id: object_id.to_string(),
            with_revisions: true,
        });

        Self::add_token_to_md(req.metadata_mut(), token)?;

        self.object_service
            .clone()
            .delete_object(req)
            .await
            .map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                e
            })?;

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, obj, token))]
    pub async fn add_cors_to_project(
        &self,
        token: &str,
        obj: DPObject,
        cors_config: crate::structs::CORSConfiguration,
    ) -> Result<()> {
        let kv = KeyValue {
            key: "app.aruna-storage.org/cors".to_string(),
            value: serde_json::to_string(&cors_config)?,
            variant: KeyValueVariant::Label as i32,
        };

        let remove_key_values = obj
            .key_values
            .into_iter()
            .filter(|k| k.key == "app.aruna-storage.org/cors")
            .collect();

        let mut req = Request::new(UpdateProjectKeyValuesRequest {
            project_id: obj.id.to_string(),
            add_key_values: vec![kv],
            remove_key_values: remove_key_values,
        });

        Self::add_token_to_md(req.metadata_mut(), token)?;

        self.project_service
            .clone()
            .update_project_key_values(req)
            .await
            .map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                e
            })?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, obj, token))]
    pub async fn remove_cors_from_project(&self, token: &str, obj: DPObject) -> Result<()> {
        let cors = match obj
            .key_values
            .iter()
            .find(|e| e.key == "app.aruna-storage.org/cors")
            .cloned()
        {
            Some(cors) => cors,
            None => KeyValue {
                key: "app.aruna-storage.org/cors".to_string(),
                value: String::new(),
                variant: KeyValueVariant::Label as i32,
            },
        };
        let mut req = Request::new(UpdateProjectKeyValuesRequest {
            project_id: obj.id.to_string(),
            add_key_values: vec![],
            remove_key_values: vec![cors],
        });

        Self::add_token_to_md(req.metadata_mut(), token)?;

        self.project_service
            .clone()
            .update_project_key_values(req)
            .await
            .map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                e
            })?;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, object, token, force_update))]
    pub async fn init_object_update(
        &self,
        object: DPObject,
        token: &str,
        force_update: bool,
    ) -> Result<DPObject> {
        trace!(?object, "Initializing object update");

        // Create UpdateObjectRequest with provided value for force_revision parameter
        let mut inner_request = UpdateObjectRequest::from(object);
        inner_request.force_revision = force_update;

        // Crate gRPC request with provided token in header
        let mut req = Request::new(inner_request);

        Self::add_token_to_md(req.metadata_mut(), token)?;

        // Update Object in ArunaServer and validate response
        let response = self
            .object_service
            .clone()
            .update_object(req)
            .await
            .map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                e
            })?
            .into_inner();

        let object = DPObject::try_from(response.object.ok_or_else(|| {
            error!(error = "response does not contain object");
            anyhow!("response does not contain object")
        })?)?;

        self.cache.upsert_object(object.clone()).await?;

        Ok(object)
    }

    #[tracing::instrument(level = "trace", skip(self, hashes, token))]
    pub async fn finish_object(
        &self,
        object_id: DieselUlid,
        content_len: i64,
        hashes: Vec<Hash>,
        token: &str,
    ) -> Result<DPObject> {
        trace!(?object_id, ?content_len, ?hashes, "Finishing object");

        let mut req = Request::new(FinishObjectStagingRequest {
            object_id: object_id.to_string(),
            content_len,
            hashes,
            completed_parts: vec![],
            upload_id: "".to_string(), // Upload id only needed in requests from users
        });

        Self::add_token_to_md(req.metadata_mut(), token)?;

        let response = self
            .object_service
            .clone()
            .finish_object_staging(req)
            .await
            .map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                e
            })?
            .into_inner()
            .object
            .ok_or_else(|| {
                error!(error = "unknown object");
                anyhow!("unknown object")
            })?;

        let object = DPObject::try_from(response)?;

        self.cache.upsert_object(object.clone()).await?;

        Ok(object)
    }

    #[tracing::instrument(level = "trace", skip(self, proxy_object, token))]
    pub async fn create_and_finish(
        &self,
        proxy_object: DPObject,
        content_len: i64,
        token: &str,
    ) -> Result<DPObject> {
        trace!(?proxy_object, "Creating and finishing object");

        // Create Object in Aruna Server
        let mut req = Request::new(CreateObjectRequest::from(proxy_object.clone()));

        Self::add_token_to_md(req.metadata_mut(), token)?;

        let server_object: DPObject = self
            .object_service
            .clone()
            .create_object(req)
            .await
            .map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                e
            })?
            .into_inner()
            .object
            .ok_or_else(|| {
                error!(error = "Object missing in CreateObjectResponse");
                anyhow!("Object missing in CreateObjectResponse")
            })?
            .try_into()?;

        let mut req = Request::new(FinishObjectStagingRequest {
            object_id: server_object.id.to_string(),
            content_len,
            hashes: proxy_object.get_hashes(), // Hashes stay the same
            completed_parts: vec![],
            upload_id: "".to_string(), // Upload id only needed in requests from users
        });

        Self::add_token_to_md(req.metadata_mut(), token)?;

        let response = self
            .object_service
            .clone()
            .finish_object_staging(req)
            .await
            .map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                e
            })?
            .into_inner()
            .object
            .ok_or_else(|| {
                error!(error = "Object missing in FinishObjectResponse");
                anyhow!("Object missing in FinishObjectResponse")
            })?;

        // Id of location record should be set to Dataproxy Object id but is set to Server Object id... the fuck?
        let object = DPObject::try_from(response)?;

        // Persist Object and Location in cache/database
        self.cache.upsert_object(object.clone()).await?;

        Ok(object)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn create_notifications_channel(&self) -> Result<()> {
        let mut req = Request::new(GetEventMessageStreamRequest {
            stream_consumer: self.endpoint_id.to_string(),
        });

        Self::add_token_to_md(req.metadata_mut(), &self.long_lived_token)?;

        let stream = self
            .event_notification_service
            .clone()
            .get_event_message_stream(req)
            .await
            .map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                e
            })?;

        let mut inner_stream = stream.into_inner();

        // Fullsync
        let mut req = Request::new(FullSyncEndpointRequest {});
        Self::add_token_to_md(req.metadata_mut(), &self.long_lived_token)?;
        let mut full_sync_stream = self
            .endpoint_service
            .clone()
            .full_sync_endpoint(req)
            .await
            .map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                e
            })?
            .into_inner();
        let mut resources = Vec::new();
        while let Some(full_sync_message) = full_sync_stream.message().await.map_err(|e| {
            error!(error = ?e, msg = e.to_string());
            e
        })? {
            debug!("received full_sync_message");
            trace!(?full_sync_message);
            match full_sync_message.target.ok_or_else(|| {
                error!(error = "Missing target in full_sync");
                anyhow!("Missing target in full_sync")
            })? {
                Target::GenericResource(GenericResource { resource: Some(r) }) => {
                    resources.push(r);
                }
                Target::User(u) => self.cache.clone().upsert_user(u).await?,
                Target::Pubkey(pk) => {
                    self.cache.add_pubkey(pk.clone().into()).await?;
                }
                _ => (),
            }
        }

        sort_resources(&mut resources);
        for res in resources {
            let object = DPObject::try_from(res)?;
            self.cache.upsert_object(object).await?
        }

        let (keep_alive_tx, mut keep_alive_rx) = tokio::sync::mpsc::channel::<()>(1);
        tokio::spawn(
            async move {
                while keep_alive_rx.try_recv().is_ok() {
                    tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                }
                // ABORT!
                error!("keep alive failed");
                panic!("keep alive failed");
            }
            .instrument(tracing::info_span!("keep_alive")),
        );

        debug!("querying events");
        while let Some(m) = inner_stream.message().await? {
            if let Some(message) = m.message {
                debug!(?message, "received event message");

                if let Ok(Some(r)) = self.process_message(message).await {
                    for a in 0..5 {
                        let mut req = Request::new(AcknowledgeMessageBatchRequest {
                            replies: vec![r.clone()],
                        });

                        Self::add_token_to_md(req.metadata_mut(), &self.long_lived_token)?;
                        match self
                            .event_notification_service
                            .clone()
                            .acknowledge_message_batch(req)
                            .await
                        {
                            Ok(_) => break,
                            Err(e) => {
                                error!(error = ?e, msg = e.to_string());
                                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                                if a == 4 {
                                    return Err(e.into());
                                }
                            }
                        };
                    }
                    debug!("acknowledged message");
                }
            } else {
                let _ = keep_alive_tx.try_send(());
                trace!("received ping");
            }
        }
        error!("Stream was closed by sender");
        Err(anyhow!("Stream was closed by sender"))
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn pull_replication(
        &self,
        init_request: PullReplicationRequest,
        endpoint_ulid: DieselUlid,
    ) -> Result<(
        Sender<PullReplicationRequest>,
        Streaming<PullReplicationResponse>,
    )> {
        let get_ep_request = Request::new(GetEndpointRequest {
            endpoint: Some(Endpoint::EndpointId(endpoint_ulid.to_string())),
        });
        let get_ep_response = self
            .endpoint_service
            .clone()
            .get_endpoint(get_ep_request)
            .await
            .map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                e
            })?
            .into_inner();
        let endpoint = get_ep_response.endpoint.ok_or_else(|| {
            error!(error = "No endpoint found in GetEndpointResponse");
            anyhow!("No endpoint found in GetEndpointResponse")
        })?;
        let config = endpoint
            .host_configs
            .iter()
            .find(|config| config.host_variant() == EndpointHostVariant::Grpc)
            .ok_or_else(|| {
                error!(error = "No grpc config found for endpoint");
                anyhow!("No grpc config found for endpoint")
            })?;
        let channel = if config.ssl {
            let proxy_channel = Channel::from_shared(config.url.clone()).map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                e
            })?;
            let tls_config = ClientTlsConfig::new();
            proxy_channel
                .tls_config(tls_config)
                .map_err(|e| {
                    error!(error = ?e, msg = e.to_string());
                    e
                })?
                .connect()
                .await
                .map_err(|e| {
                    error!(error = ?e, msg = e.to_string());
                    e
                })?
        } else {
            Channel::from_shared(config.url.clone())
                .map_err(|e| {
                    error!(error = ?e, msg = e.to_string());
                    e
                })?
                .connect()
                .await
                .map_err(|e| {
                    error!(error = ?e, msg = e.to_string());
                    e
                })?
        };
        let token = if let Some(auth) = self.cache.auth.read().await.as_ref() {
            auth.sign_dataproxy_token(endpoint_ulid)?
        } else {
            error!(error = "Cannot read auth handler");
            Err(anyhow!("Cannot read auth handler"))?
        };

        let dataproxy_service = DataproxyReplicationServiceClient::new(channel.clone())
            .max_decoding_message_size(1024 * 1024 * 10);
        let (request_stream_sender, request_stream_receiver) = tokio::sync::mpsc::channel(1000);
        let mut req = Request::new(ReceiverStream::new(request_stream_receiver));
        Self::add_token_to_md(req.metadata_mut(), &token)?;
        let response_stream = dataproxy_service
            .clone()
            .pull_replication(req)
            .await
            .map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                e
            })?
            .into_inner();
        request_stream_sender
            .send(init_request)
            .await
            .map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                e
            })?;
        Ok((request_stream_sender, response_stream))
    }

    #[tracing::instrument(level = "trace", skip(self, request))]
    pub async fn update_replication_status(
        &self,
        request: UpdateReplicationStatusRequest,
    ) -> Result<()> {
        let mut request = Request::new(request);
        Self::add_token_to_md(request.metadata_mut(), &self.long_lived_token)?;
        self.data_replication_service
            .clone()
            .update_replication_status(request)
            .await
            .map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                e
            })?;
        Ok(())
    }
}

/// Request handling section
impl GrpcQueryHandler {
    #[tracing::instrument(level = "trace", skip(self, message))]
    async fn process_message(&self, message: EventMessage) -> Result<Option<Reply>> {
        match message.message_variant.unwrap() {
            MessageVariant::ResourceEvent(r_event) => self.process_resource_event(r_event).await,
            MessageVariant::UserEvent(u_event) => self.process_user_event(u_event).await,
            MessageVariant::AnnouncementEvent(a_event) => {
                self.process_announcements_event(a_event).await
            }
        }
        .map_err(|e| {
            error!(error = ?e, msg = e.to_string(), "Error in processing message");
            e
        })
    }

    #[tracing::instrument(level = "trace", skip(self, message))]
    async fn process_announcements_event(
        &self,
        message: AnnouncementEvent,
    ) -> Result<Option<Reply>> {
        debug!("processing announcement event");
        match message.event_variant.ok_or_else(|| {
            error!(error = "No event variant");
            anyhow!("No event variant")
        })? {
            announcement_event::EventVariant::NewPubkey(_)
            | announcement_event::EventVariant::RemovePubkey(_)
            | announcement_event::EventVariant::NewDataProxyId(_)
            | announcement_event::EventVariant::RemoveDataProxyId(_)
            | announcement_event::EventVariant::UpdateDataProxyId(_) => {
                let pks = self
                    .get_pubkeys()
                    .await?
                    .into_iter()
                    .map(PubKey::from)
                    .collect();
                self.cache.set_pubkeys(pks).await?;
            }
            announcement_event::EventVariant::Downtime(_) => (),
            announcement_event::EventVariant::Version(_) => (),
        };
        Ok(message.reply)
    }

    #[tracing::instrument(level = "trace", skip(self, message))]
    async fn process_user_event(&self, message: UserEvent) -> Result<Option<Reply>> {
        debug!("processing user event");
        match message.event_variant() {
            EventVariant::Created | EventVariant::Available | EventVariant::Updated => {
                let uid = DieselUlid::from_str(&message.user_id)?;
                let user_info = self.get_user(uid, message.checksum.clone()).await?;
                self.cache.clone().upsert_user(user_info.clone()).await?;
            }
            EventVariant::Deleted => {
                let uid = DieselUlid::from_str(&message.user_id)?;

                self.cache.remove_user(uid).await?;
            }
            _ => (),
        }

        Ok(message.reply)
    }

    #[tracing::instrument(level = "trace", skip(self, event))]
    async fn process_resource_event(&self, event: ResourceEvent) -> Result<Option<Reply>> {
        debug!("processing resource event");
        match event.event_variant() {
            EventVariant::Created | EventVariant::Updated => {
                trace!("upserting object");
                if let Some(r) = event.resource {
                    match r.resource_variant() {
                        aruna_rust_api::api::storage::models::v2::ResourceVariant::Project => {
                            let object = self
                                .get_project(
                                    &DieselUlid::from_str(&r.resource_id).inspect_err(|&e| {
                                        error!(error = ?e, msg = e.to_string());
                                    })?,
                                    r.checksum,
                                )
                                .await?;

                            self.cache.upsert_object(object.try_into()?).await?;
                        }
                        aruna_rust_api::api::storage::models::v2::ResourceVariant::Collection => {
                            let object = self
                                .get_collection(
                                    &DieselUlid::from_str(&r.resource_id).inspect_err(|&e| {
                                        error!(error = ?e, msg = e.to_string());
                                    })?,
                                    r.checksum,
                                )
                                .await?;
                            self.cache.upsert_object(object.try_into()?).await?;
                        }
                        aruna_rust_api::api::storage::models::v2::ResourceVariant::Dataset => {
                            let object = self
                                .get_dataset(
                                    &DieselUlid::from_str(&r.resource_id).inspect_err(|&e| {
                                        error!(error = ?e, msg = e.to_string());
                                    })?,
                                    r.checksum,
                                )
                                .await?;
                            self.cache.upsert_object(object.try_into()?).await?;
                        }
                        aruna_rust_api::api::storage::models::v2::ResourceVariant::Object => {
                            let object = self
                                .get_object(
                                    &DieselUlid::from_str(&r.resource_id).inspect_err(|&e| {
                                        error!(error = ?e, msg = e.to_string());
                                    })?,
                                    r.checksum,
                                )
                                .await?;
                            // Update anyway
                            self.cache.upsert_object(object.clone().try_into()?).await?;
                            // Try pull replication
                            self.handle_replication(object).await?;
                        }
                        _ => (),
                    }
                }
            }
            EventVariant::Deleted => {
                trace!("deleting object");
                if let Some(r) = event.resource {
                    self.cache
                        .delete_object(DieselUlid::from_str(&r.resource_id)?)
                        .await?;
                }
            }
            _ => (),
        }
        Ok(event.reply)
    }

    #[tracing::instrument(level = "trace", skip(self, object))]
    async fn handle_replication(&self, object: Object) -> Result<()> {
        // if ObjectStatus::AVAILABLE ...
        if object.status == 3 {
            // ... object should be synced in at least one ep
            for ep in &object.endpoints {
                // ... then find out if I have to do anything ...
                match (ep.status(), &ep.id, &ep.variant) {
                    // ... if my id, waiting and FullSync -> I should request a FullSync
                    (ReplicationStatus::Waiting, id, Some(Variant::FullSync(FullSync { .. })))
                        if id == &self.endpoint_id =>
                    {
                        // Find a proxy that has a fullsync
                        let full_sync_proxy = &object.endpoints.iter().find_map(|ep| {
                            match (&ep.variant, ep.status()) {
                                (
                                    Some(Variant::FullSync(FullSync { .. })),
                                    ReplicationStatus::Finished,
                                ) => Some(ep.id.clone()),
                                _ => None,
                            }
                        });
                        match full_sync_proxy {
                            Some(ep_id) => {
                                let direction = Direction::Pull(
                                    DieselUlid::from_str(&object.id).inspect_err(|&e| {
                                        error!(error = ?e, msg = e.to_string());
                                    })?,
                                );
                                let endpoint_id =
                                    DieselUlid::from_str(ep_id).inspect_err(|&e| {
                                        error!(error = ?e, msg = e.to_string());
                                    })?;

                                self.cache
                                    .sender
                                    .send(ReplicationMessage {
                                        direction,
                                        endpoint_id,
                                    })
                                    .await
                                    .map_err(|e| {
                                        error!(error = ?e, msg = e.to_string());
                                        e
                                    })?;
                            }
                            None => {
                                error!("ReplicationError: No available proxy found");
                                self.update_replication_status(UpdateReplicationStatusRequest {
                                    object_id: object.id.to_string(),
                                    endpoint_id: self.endpoint_id.clone(),
                                    status: ReplicationStatus::Error as i32,
                                })
                                .await
                                .map_err(|e| {
                                    error!(error = ?e, msg = e.to_string());
                                    e
                                })?;
                            }
                        }
                    }
                    // ... if my id, waiting and partial sync -> I should request a PartialSync
                    (ReplicationStatus::Waiting, id, Some(Variant::PartialSync(_)))
                        if id == &self.endpoint_id =>
                    {
                        // Find the full sync proxy and partial sync from there
                        let full_sync_proxy = &object.endpoints.iter().find_map(|ep| {
                            match (&ep.variant, ep.status()) {
                                (Some(Variant::FullSync(_)), ReplicationStatus::Finished) => {
                                    Some(ep.id.clone())
                                }
                                _ => None,
                            }
                        });
                        match full_sync_proxy {
                            Some(ep_id) => {
                                let direction = Direction::Pull(
                                    DieselUlid::from_str(&object.id).inspect_err(|&e| {
                                        error!(error = ?e, msg = e.to_string());
                                    })?,
                                );
                                let endpoint_id =
                                    DieselUlid::from_str(ep_id).inspect_err(|&e| {
                                        error!(error = ?e, msg = e.to_string());
                                    })?;
                                self.cache
                                    .sender
                                    .send(ReplicationMessage {
                                        direction,
                                        endpoint_id,
                                    })
                                    .await
                                    .map_err(|e| {
                                        error!(error = ?e, msg = e.to_string());
                                        e
                                    })?;
                            }
                            None => {
                                error!("ReplicationError: No available proxy found");
                                self.update_replication_status(UpdateReplicationStatusRequest {
                                    object_id: object.id.to_string(),
                                    endpoint_id: self.endpoint_id.clone(),
                                    status: ReplicationStatus::Error as i32,
                                })
                                .await
                                .map_err(|e| {
                                    error!(error = ?e, msg = e.to_string());
                                    e
                                })?;
                            }
                        }
                    }
                    // ... if others are waiting, and I finished -> Should I sync to other dataproxies?
                    (ReplicationStatus::Waiting, id, _) if id != &self.endpoint_id => {
                        // TODO
                        // - How to find out if pushing is appropriate for a given dataproxy that is
                        //   not this one? -> Am I the only one/main dataproxy?
                        // - Check if object location exists here
                        // - Create presigned url for object
                        // - send message to replication handler
                    }
                    _ => (),
                }
            }
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, col, parent, token))]
    pub async fn create_collection_ingestion(
        &self,
        col: IngestResource,
        parent: DieselUlid,
        token: &str,
    ) -> Result<DPObject> {
        let md_license = if col.metadata_license_tag.is_empty() {
            None
        } else {
            Some(col.metadata_license_tag)
        };

        let data_license = if col.data_license_tag.is_empty() {
            None
        } else {
            Some(col.data_license_tag)
        };

        let mut req = Request::new(CreateCollectionRequest {
            name: col.name,
            title: col.title,
            description: col.description,
            key_values: col.key_values,
            relations: col.relations,
            data_class: col.data_class,
            metadata_license_tag: md_license,
            default_data_license_tag: data_license,
            authors: col.authors,
            parent: Some(aruna_rust_api::api::storage::services::v2::create_collection_request::Parent::ProjectId(parent.to_string())),
        });

        Self::add_token_to_md(req.metadata_mut(), token)?;

        let response = self
            .collection_service
            .clone()
            .create_collection(req)
            .await
            .map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                e
            })?
            .into_inner()
            .collection
            .ok_or_else(|| {
                error!(error = "unknown collection");
                anyhow!("unknown collection")
            })?;

        let object = DPObject::try_from(response)?;

        self.cache.upsert_object(object.clone()).await?;

        Ok(object)
    }

    #[tracing::instrument(level = "trace", skip(self, ds, parent, token))]
    pub async fn create_dataset_ingest(
        &self,
        ds: IngestResource,
        parent: TypedRelation,
        token: &str,
    ) -> Result<DPObject> {
        let parent = match parent {
            TypedRelation::Project(id) => create_dataset_request::Parent::ProjectId(id.to_string()),
            TypedRelation::Collection(id) => {
                create_dataset_request::Parent::CollectionId(id.to_string())
            }
            _ => return Err(anyhow!("Invalid parent type")),
        };

        let md_license = if ds.metadata_license_tag.is_empty() {
            None
        } else {
            Some(ds.metadata_license_tag)
        };

        let data_license = if ds.data_license_tag.is_empty() {
            None
        } else {
            Some(ds.data_license_tag)
        };

        let mut req = Request::new(CreateDatasetRequest {
            name: ds.name,
            title: ds.title,
            description: ds.description,
            key_values: ds.key_values,
            relations: ds.relations,
            data_class: ds.data_class,
            metadata_license_tag: md_license,
            default_data_license_tag: data_license,
            authors: ds.authors,
            parent: Some(parent),
        });

        Self::add_token_to_md(req.metadata_mut(), token)?;

        let response = self
            .dataset_service
            .clone()
            .create_dataset(req)
            .await
            .map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                e
            })?
            .into_inner()
            .dataset
            .ok_or_else(|| {
                error!(error = "unknown collection");
                anyhow!("unknown collection")
            })?;

        let object = DPObject::try_from(response)?;

        self.cache.upsert_object(object.clone()).await?;

        Ok(object)
    }

    #[tracing::instrument(level = "trace", skip(self, obj, parent, token))]
    pub async fn create_and_finish_ingest(
        &self,
        obj: IngestResource,
        content_len: i64,
        parent: TypedRelation,
        token: &str,
    ) -> Result<DPObject> {
        let parent = match parent {
            TypedRelation::Project(id) => create_object_request::Parent::ProjectId(id.to_string()),
            TypedRelation::Collection(id) => {
                create_object_request::Parent::CollectionId(id.to_string())
            }
            TypedRelation::Dataset(id) => create_object_request::Parent::DatasetId(id.to_string()),
            _ => return Err(anyhow!("Invalid parent type")),
        };

        let mut req = Request::new(CreateObjectRequest {
            name: obj.name,
            title: obj.title,
            description: obj.description,
            key_values: obj.key_values,
            relations: obj.relations,
            data_class: obj.data_class,
            metadata_license_tag: obj.metadata_license_tag,
            data_license_tag: obj.data_license_tag,
            authors: obj.authors,
            parent: Some(parent),
            hashes: vec![],
        });

        Self::add_token_to_md(req.metadata_mut(), token)?;

        let response = self
            .object_service
            .clone()
            .create_object(req)
            .await
            .map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                e
            })?
            .into_inner()
            .object
            .ok_or_else(|| {
                error!(error = "unknown object");
                anyhow!("unknown object")
            })?;

        let mut req = Request::new(FinishObjectStagingRequest {
            object_id: response.id,
            content_len,
            hashes: vec![],
            completed_parts: vec![],
            upload_id: "".to_string(), // Upload id only needed in requests from users
        });

        Self::add_token_to_md(req.metadata_mut(), token)?;

        let response = self
            .object_service
            .clone()
            .finish_object_staging(req)
            .await
            .map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                e
            })?
            .into_inner()
            .object
            .ok_or_else(|| {
                error!(error = "unknown object");
                anyhow!("unknown object")
            })?;

        let object = DPObject::try_from(response)?;

        self.cache.upsert_object(object.clone()).await?;

        Ok(object)
    }

    #[tracing::instrument(level = "trace", skip(self, id, hashes, token))]
    pub async fn set_object_hashes(
        &self,
        id: &DieselUlid,
        hashes: Vec<Hash>,
        token: &str,
    ) -> Result<()> {
        let mut req = Request::new(SetObjectHashesRequest {
            object_id: id.to_string(),
            hashes,
        });

        Self::add_token_to_md(req.metadata_mut(), token)?;

        self.object_service
            .clone()
            .set_object_hashes(req)
            .await
            .map_err(|e| {
                error!(error = ?e, msg = e.to_string());
                e
            })?;

        Ok(())
    }
}

#[tracing::instrument(level = "trace", skip(res))]
pub fn sort_resources(res: &mut [Resource]) {
    res.sort_by(|x, y| match (x, y) {
        (Resource::Project(_), Resource::Project(_)) => std::cmp::Ordering::Equal,
        (Resource::Project(_), Resource::Collection(_))
        | (Resource::Project(_), Resource::Dataset(_))
        | (Resource::Project(_), Resource::Object(_)) => std::cmp::Ordering::Less,
        (Resource::Collection(_), Resource::Project(_)) => std::cmp::Ordering::Greater,
        (Resource::Collection(_), Resource::Collection(_)) => std::cmp::Ordering::Equal,
        (Resource::Collection(_), Resource::Dataset(_)) => std::cmp::Ordering::Less,
        (Resource::Collection(_), Resource::Object(_)) => std::cmp::Ordering::Less,
        (Resource::Dataset(_), Resource::Project(_)) => std::cmp::Ordering::Greater,
        (Resource::Dataset(_), Resource::Collection(_)) => std::cmp::Ordering::Greater,
        (Resource::Dataset(_), Resource::Dataset(_)) => std::cmp::Ordering::Equal,
        (Resource::Dataset(_), Resource::Object(_)) => std::cmp::Ordering::Less,
        (Resource::Object(_), Resource::Project(_)) => std::cmp::Ordering::Greater,
        (Resource::Object(_), Resource::Collection(_)) => std::cmp::Ordering::Greater,
        (Resource::Object(_), Resource::Dataset(_)) => std::cmp::Ordering::Greater,
        (Resource::Object(_), Resource::Object(_)) => std::cmp::Ordering::Equal,
    })
}

#[tracing::instrument(level = "trace", skip(res))]
pub fn sort_objects(res: &mut [DPObject]) {
    res.sort_by(|x, y| match (&x.object_type, &y.object_type) {
        (ObjectType::Project, ObjectType::Project) => std::cmp::Ordering::Equal,
        (ObjectType::Project, ObjectType::Collection)
        | (ObjectType::Project, ObjectType::Dataset)
        | (ObjectType::Project, ObjectType::Object) => std::cmp::Ordering::Less,

        (ObjectType::Collection, ObjectType::Project) => std::cmp::Ordering::Greater,
        (ObjectType::Collection, ObjectType::Collection) => std::cmp::Ordering::Equal,
        (ObjectType::Collection, ObjectType::Dataset)
        | (ObjectType::Collection, ObjectType::Object) => std::cmp::Ordering::Less,

        (ObjectType::Dataset, ObjectType::Project)
        | (ObjectType::Dataset, ObjectType::Collection) => std::cmp::Ordering::Greater,
        (ObjectType::Dataset, ObjectType::Dataset) => std::cmp::Ordering::Equal,
        (ObjectType::Dataset, ObjectType::Object) => std::cmp::Ordering::Less,

        (ObjectType::Object, ObjectType::Project)
        | (ObjectType::Object, ObjectType::Collection)
        | (ObjectType::Object, ObjectType::Dataset) => std::cmp::Ordering::Greater,
        (ObjectType::Object, ObjectType::Object) => std::cmp::Ordering::Equal,
    })
}
