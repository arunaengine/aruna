use crate::database::crud::CrudDb;
use crate::database::dsls::endpoint_dsl::Endpoint;
use crate::database::dsls::object_dsl::Object;
use crate::database::dsls::pub_key_dsl::PubKey;
use crate::database::dsls::user_dsl::User;
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::endpoints_request_types::{CreateEP, DeleteEP, GetBy, GetEP};

use anyhow::{anyhow, Result};
use aruna_rust_api::api::notification::services::v2::announcement_event::EventVariant as AnnouncementVariant;
use tokio_postgres::GenericClient;

impl DatabaseHandler {
    pub async fn create_endpoint(&self, request: CreateEP) -> Result<(Endpoint, PubKey)> {
        let mut client = self.database.get_client().await?;
        let transaction = client.transaction().await?;
        let transaction_client = transaction.client();
        let (mut endpoint, pubkey) = request.build_endpoint()?;
        endpoint.create(transaction_client).await?;
        let pubkey =
            PubKey::create_or_get_without_id(Some(endpoint.id), &pubkey.pubkey, transaction_client)
                .await?;
        transaction.commit().await?;

        // Emit announcement notifications
        let announcements = vec![
            AnnouncementVariant::NewDataProxyId(endpoint.id.to_string()),
            AnnouncementVariant::NewPubkey(pubkey.id as i32),
        ];

        for ann in announcements {
            if let Err(err) = self.natsio_handler.register_announcement_event(ann).await {
                // Log error, rollback transaction and return
                log::error!("{err}");
                //transaction.rollback().await?;
                return Err(anyhow::anyhow!("Notification emission failed"));
            }
        }

        Ok((endpoint, pubkey))
    }

    pub async fn get_endpoint(&self, request: GetEP) -> Result<Endpoint> {
        let client = self.database.get_client().await?;
        let endpoint = match request.get_query()? {
            GetBy::ID(id) => Endpoint::get(id, client.client()).await?,
            GetBy::NAME(name) => Endpoint::get_by_name(name, client.client()).await?,
        };
        endpoint.ok_or_else(|| anyhow!("No endpoint found"))
    }

    pub async fn get_endpoints(&self) -> Result<Vec<Endpoint>> {
        let client = self.database.get_client().await?;
        let endpoints = Endpoint::all(client.client()).await?;
        Ok(endpoints)
    }

    pub async fn delete_endpoint(&self, request: DeleteEP) -> Result<()> {
        // Open transaction
        let mut db_client = self.database.get_client().await?;
        let transaction = db_client.transaction().await?;
        let transaction_client = transaction.client();

        // Remove endpoint from database
        let id = request.get_id()?;
        Endpoint::delete_by_id(&id, transaction_client.client()).await?;

        // Remove endpoint from users/resources
        User::remove_endpoint_from_users(transaction_client, &id).await?;
        Object::remove_endpoint_from_objects(transaction_client, &id).await?;

        // Commit transaction
        transaction.commit().await?;

        //ToDo Clean user/resources from endpoint

        // Emit announcement notification
        if let Err(err) = self
            .natsio_handler
            .register_announcement_event(AnnouncementVariant::RemoveDataProxyId(id.to_string()))
            .await
        {
            // Log error, rollback transaction and return
            log::error!("{err}");
            //transaction.rollback().await?;
            return Err(anyhow::anyhow!("Notification emission failed"));
        }

        Ok(())
    }
}
