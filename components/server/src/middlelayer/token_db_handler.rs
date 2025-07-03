use crate::database::dsls::user_dsl::APIToken;
use crate::database::dsls::user_dsl::User;
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::token_request_types::{CreateToken, DeleteToken};
use ahash::HashMap;
use anyhow::Result;
use aruna_rust_api::api::notification::services::v2::EventVariant;
use diesel_ulid::DieselUlid;

impl DatabaseHandler {
    pub async fn create_hook_token(
        &self,
        user_id: &DieselUlid,
        token: APIToken,
    ) -> Result<DieselUlid> {
        // Init database transaction
        let mut client = self.database.get_client().await?;
        let transaction = client.transaction().await?;
        let client = transaction.client();

        // Add token to user attributes
        let token_ulid = DieselUlid::generate();
        let mut token_map: HashMap<DieselUlid, &APIToken> = HashMap::default();
        token_map.insert(token_ulid, &token);
        let user = User::add_user_token(client, user_id, token_map).await?;
        transaction.commit().await?;

        self.cache.update_user(user_id, user.clone());
        // Try to emit user updated notification(s)
        if let Err(err) = self
            .natsio_handler
            .register_user_event(&user, EventVariant::Updated)
            .await
        {
            // Log error (rollback transaction and return)
            log::error!("{err}");
            //transaction.rollback().await?;
            return Err(anyhow::anyhow!("Notification emission failed"));
        }

        // Return token_id and token
        Ok(token_ulid)
    }
    pub async fn create_token(
        &self,
        user_id: &DieselUlid,
        pubkey_serial: i32,
        request: CreateToken,
    ) -> Result<(DieselUlid, APIToken)> {
        // Init database transaction
        let mut client = self.database.get_client().await?;
        let transaction = client.transaction().await?;
        let client = transaction.client();

        // Generate APIToken and add to user
        let token_ulid = DieselUlid::generate();
        let token = request.build_token(pubkey_serial)?;

        // Add token to user attributes in database
        let mut token_map: HashMap<DieselUlid, &APIToken> = HashMap::default();
        token_map.insert(token_ulid, &token);
        let user = User::add_user_token(client, user_id, token_map).await?;
        transaction.commit().await?;

        // Update user in cache
        self.cache.update_user(&user.id, user.clone());

        // Try to emit user updated notification(s)
        if let Err(err) = self
            .natsio_handler
            .register_user_event(&user, EventVariant::Updated)
            .await
        {
            // Log error (rollback transaction and return)
            log::error!("{err}");
            //transaction.rollback().await?;
            return Err(anyhow::anyhow!("Notification emission failed"));
        }

        // Return token_id and token
        Ok((token_ulid, token))
    }

    pub async fn delete_token(&self, user_id: DieselUlid, request: DeleteToken) -> Result<()> {
        let client = self.database.get_client().await?;
        let token_id = request.get_token_id()?;

        // Remove token from user attributes in database
        let user = User::remove_user_token(&client, &user_id, &token_id).await?;

        // Update user in cache
        self.cache.update_user(&user.id, user.clone());

        // Try to emit user updated notification(s)
        if let Err(err) = self
            .natsio_handler
            .register_user_event(&user, EventVariant::Updated)
            .await
        {
            // Log error (rollback transaction and return)
            log::error!("{err}");
            //transaction.rollback().await?;
            return Err(anyhow::anyhow!("Notification emission failed"));
        }

        Ok(())
    }

    pub async fn delete_all_tokens(&self, user_id: DieselUlid) -> Result<()> {
        let client = self.database.get_client().await?;

        // Remove all tokens from user attributes in database
        let user = User::remove_all_tokens(&client, &user_id).await?;

        // Update user in cache
        self.cache.update_user(&user.id, user.clone());

        // Try to emit user updated notification(s)
        if let Err(err) = self
            .natsio_handler
            .register_user_event(&user, EventVariant::Updated)
            .await
        {
            // Log error (rollback transaction and return)
            log::error!("{err}");
            //transaction.rollback().await?;
            return Err(anyhow::anyhow!("Notification emission failed"));
        }

        Ok(())
    }
}
