use anyhow::{anyhow, Result};
use aruna_rust_api::api::notification::services::v2::EventVariant;
use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::{config::Credentials, types::CompletedMultipartUpload, Client};
use diesel_ulid::DieselUlid;
use std::sync::Arc;

use crate::{
    auth::permission_handler::PermissionHandler,
    caching::cache::Cache,
    database::{
        crud::CrudDb,
        dsls::{
            hook_dsl::TriggerVariant,
            object_dsl::{Object, ObjectWithRelations},
        },
        enums::ObjectStatus,
    },
    middlelayer::db_handler::DatabaseHandler,
};

use super::finish_request_types::FinishRequest;

impl DatabaseHandler {
    pub async fn complete_multipart_upload(
        &self,
        request: FinishRequest,
        user_id: DieselUlid,
        cache: Arc<Cache>,
        authorizer: Arc<PermissionHandler>,
        token: Option<DieselUlid>,
    ) -> Result<()> {
        // Get staging object meta info
        let (project_id, bucket_name, key) =
            DatabaseHandler::get_path(request.get_object_id()?, cache.clone()).await?;

        // Get endpoint
        let endpoint = self.get_fullsync_endpoint(project_id).await?;

        let (_, endpoint_s3_url, _, credentials) =
            DatabaseHandler::get_or_create_credentials(authorizer, user_id, token, endpoint, true)
                .await?;

        // Impersonate User for CompleteMultiPartUpload at endpoint_s3_url
        let creds = Credentials::new(
            credentials.access_key,
            credentials.secret_key,
            None,
            None,
            "ARUNA_SERVER", // Endpoint name?
        );
        let config = aws_config::defaults(BehaviorVersion::v2025_01_17())
            .credentials_provider(creds)
            .load()
            .await;
        let s3_config = aws_sdk_s3::config::Builder::from(&config)
            .region(Region::new("RegionOne"))
            .endpoint_url(endpoint_s3_url)
            .build();

        let s3_client = Client::from_conf(s3_config);

        let completed_multipart_upload: CompletedMultipartUpload =
            CompletedMultipartUpload::builder()
                .set_parts(Some(request.get_parts()))
                .build();

        let _res = s3_client
            .complete_multipart_upload()
            .bucket(&bucket_name)
            .key(&key)
            .upload_id(request.get_upload_id())
            .multipart_upload(completed_multipart_upload)
            .send()
            .await?;

        Ok(())
    }

    pub async fn finish_object(
        &self,
        request: FinishRequest,
        dataproxy_id: Option<DieselUlid>,
    ) -> Result<ObjectWithRelations> {
        let mut client = self.database.get_client().await?;
        let id = request.get_object_id()?;
        let object = Object::get(id, &client)
            .await?
            .ok_or_else(|| anyhow!("Object not found"))?;
        let (endpoint_id, endpoint_info) = if let Some(id) = dataproxy_id {
            let temp = object
                .endpoints
                .0
                .get(&id)
                .ok_or_else(|| anyhow!("No endpoints defined in object"))?;
            (id, temp.clone())
        } else {
            return Err(anyhow!("Could not retrieve endpoint info"));
        };

        let transaction = client.transaction().await?;
        let transaction_client = transaction.client();
        let hashes = Some(request.get_hashes()?);
        let content_len = request.get_content_len();
        Object::finish_object_staging(
            &id,
            transaction_client,
            hashes,
            content_len,
            ObjectStatus::AVAILABLE,
        )
        .await?;
        Object::update_endpoints(
            endpoint_id,
            crate::database::dsls::object_dsl::EndpointInfo {
                replication: endpoint_info.replication,
                status: Some(crate::database::enums::ReplicationStatus::Finished),
            },
            vec![id],
            transaction_client,
        )
        .await?;

        self.evaluate_rules(&vec![id], transaction_client).await?;
        transaction.commit().await?;

        let object = Object::get_object_with_relations(&id, &client).await?;
        let db_handler = DatabaseHandler {
            database: self.database.clone(),
            natsio_handler: self.natsio_handler.clone(),
            cache: self.cache.clone(),
            hook_sender: self.hook_sender.clone(),
        };
        let owr = object.clone();
        tokio::spawn(async move {
            let call = db_handler
                .trigger_hooks(owr, vec![TriggerVariant::OBJECT_FINISHED], None)
                .await;
            if call.is_err() {
                log::error!("{call:?}");
            }
        });

        // Try to emit object updated notification(s)
        let hierarchies = object.object.fetch_object_hierarchies(&client).await?;
        if let Err(err) = self
            .natsio_handler
            .register_resource_event(
                &object,
                hierarchies,
                EventVariant::Updated,
                Some(&DieselUlid::generate()), // block_id for deduplication
            )
            .await
        {
            // Log error, rollback transaction and return
            log::error!("{err}");
            //transaction.rollback().await?;
            Err(anyhow::anyhow!("Notification emission failed"))
        } else {
            //transaction.commit().await?;
            Ok(object)
        }
    }
}
