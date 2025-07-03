use std::str::FromStr;
use std::sync::Arc;

use super::db_handler::DatabaseHandler;
use super::service_account_request_types::{
    CreateServiceAccountToken, DeleteServiceAccount, DeleteServiceAccountToken,
    DeleteServiceAccountTokens, GetServiceAccountInfo, GetTokenAndServiceAccountInfo,
};
use crate::auth::permission_handler::PermissionHandler;
use crate::database::crud::CrudDb;
use crate::database::dsls::object_dsl::Object;
use crate::database::dsls::user_dsl::{User, UserAttributes};
use crate::database::enums::{ObjectMapping, ObjectType};
use crate::middlelayer::service_account_request_types::CreateServiceAccount;
use crate::middlelayer::token_request_types::CreateToken;
use crate::utils::conversions::users::convert_token_to_proto;
use anyhow::{anyhow, Result};
use aruna_rust_api::api::notification::services::v2::EventVariant;
use aruna_rust_api::api::storage::models::v2::permission::ResourceId;
use aruna_rust_api::api::storage::models::v2::{Permission, Token};
use aruna_rust_api::api::storage::services::v2::CreateApiTokenRequest;
use dashmap::DashMap;
use diesel_ulid::DieselUlid;
use postgres_types::Json;

impl DatabaseHandler {
    pub async fn create_service_account(&self, request: CreateServiceAccount) -> Result<User> {
        let user_id = DieselUlid::generate();
        let (res_id, perm) = request.get_permissions()?;
        let client = self.database.get_client().await?;
        let res = Object::get(res_id, &client)
            .await?
            .ok_or_else(|| anyhow!("Project not found"))?;
        match res.object_type {
            ObjectType::PROJECT => (),
            _ => return Err(anyhow!("Service accounts must have project permissions")),
        }
        let mut user = User {
            id: user_id,
            display_name: format!("SERVICE_ACCOUNT#{user_id}"),
            first_name: "".to_string(),
            last_name: "".to_string(),
            email: String::new(),
            attributes: Json(UserAttributes {
                global_admin: false,
                service_account: true,
                tokens: DashMap::default(),
                trusted_endpoints: DashMap::default(),
                custom_attributes: vec![],
                external_ids: vec![],
                pubkey: "".to_string(),
                permissions: DashMap::from_iter([(res_id, perm)]),
                data_proxy_attribute: Default::default(),
            }),
            active: true,
        };
        user.create(&client).await?;
        self.cache.update_user(&user_id, user.clone());
        if let Err(err) = self
            .natsio_handler
            .register_user_event(&user, EventVariant::Created)
            .await
        {
            // Log error (rollback transaction and return)
            log::error!("{err}");
            //transaction.rollback().await?;
            return Err(anyhow::anyhow!("Notification emission failed"));
        }
        Ok(user)
    }

    pub async fn create_service_account_token(
        &self,
        authorizer: Arc<PermissionHandler>,
        request: CreateServiceAccountToken,
    ) -> Result<(Option<Token>, String)> {
        let id = <DieselUlid as FromStr>::from_str(&request.0.svc_account_id)?;
        let client = self.database.get_client().await?;
        let service_account = User::get(id, &client)
            .await?
            .ok_or_else(|| anyhow!("User not found"))?;
        let (id, level) = request.get_permissions()?;
        let perms = service_account
            .attributes
            .0
            .permissions
            .iter()
            .next()
            .ok_or_else(|| anyhow!("Error retrieving service_account permission"))?;
        let (project_id, _) = perms.pair();
        if &id != project_id {
            let sub_resources = Object::fetch_subresources_by_id(project_id, &client).await?;
            if !sub_resources.iter().any(|sub_id| &id == sub_id) {
                return Err(anyhow!("Specified permission id is not a sub resource of associated service_account project"));
            }
        }
        let permission = Some(Permission {
            permission_level: level.into_inner().into(),
            resource_id: Some(match level {
                ObjectMapping::PROJECT(_) => ResourceId::ProjectId(id.to_string()),
                ObjectMapping::COLLECTION(_) => ResourceId::CollectionId(id.to_string()),
                ObjectMapping::DATASET(_) => ResourceId::DatasetId(id.to_string()),
                ObjectMapping::OBJECT(_) => ResourceId::ObjectId(id.to_string()),
            }),
        });
        let expires_at = request.0.expires_at;
        // Create token
        let (token_ulid, token) = self
            .create_token(
                &id,
                authorizer.token_handler.get_current_pubkey_serial() as i32,
                CreateToken(CreateApiTokenRequest {
                    name: request.0.name,
                    permission,
                    expires_at: expires_at.clone(),
                }),
            )
            .await?;

        // Update service account with token
        service_account
            .attributes
            .0
            .tokens
            .insert(token_ulid, token.clone());
        self.cache
            .update_user(&service_account.id, service_account.clone());

        // Sign token
        let token_secret = authorizer.token_handler.sign_user_token(
            &service_account.id,
            &token_ulid,
            expires_at,
        )?;

        if let Err(err) = self
            .natsio_handler
            .register_user_event(&service_account, EventVariant::Updated)
            .await
        {
            // Log error (rollback transaction and return)
            log::error!("{err}");
            //transaction.rollback().await?;
            return Err(anyhow::anyhow!("Notification emission failed"));
        }
        Ok((
            Some(convert_token_to_proto(&token_ulid, token)),
            token_secret,
        ))
    }

    pub async fn delete_service_account_token(
        &self,
        request: DeleteServiceAccountToken,
    ) -> Result<()> {
        let (service_account_id, token_id) = request.get_ids()?;
        let client = self.database.get_client().await?;
        let service_account =
            User::remove_user_token(&client, &service_account_id, &token_id).await?;
        self.cache
            .update_user(&service_account_id, service_account.clone());

        if let Err(err) = self
            .natsio_handler
            .register_user_event(&service_account, EventVariant::Updated)
            .await
        {
            // Log error (rollback transaction and return)
            log::error!("{err}");
            //transaction.rollback().await?;
            return Err(anyhow::anyhow!("Notification emission failed"));
        }
        Ok(())
    }

    pub async fn delete_service_account_tokens(
        &self,
        request: DeleteServiceAccountTokens,
    ) -> Result<()> {
        let service_account_id = request.get_id()?;
        let client = self.database.get_client().await?;
        let service_account = User::remove_all_tokens(&client, &service_account_id).await?;
        self.cache
            .update_user(&service_account_id, service_account.clone());

        if let Err(err) = self
            .natsio_handler
            .register_user_event(&service_account, EventVariant::Updated)
            .await
        {
            // Log error (rollback transaction and return)
            log::error!("{err}");
            //transaction.rollback().await?;
            return Err(anyhow::anyhow!("Notification emission failed"));
        }
        Ok(())
    }

    pub async fn delete_service_account(&self, request: DeleteServiceAccount) -> Result<()> {
        let service_account_id = request.get_id()?;
        let client = self.database.get_client().await?;
        let service_account = User::get(service_account_id, &client)
            .await?
            .ok_or_else(|| anyhow!("User not found"))?;
        service_account.delete(&client).await?;
        self.cache.remove_user(&service_account_id);

        if let Err(err) = self
            .natsio_handler
            .register_user_event(&service_account, EventVariant::Deleted)
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
