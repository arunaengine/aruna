use crate::auth::permission_handler::PermissionHandler;
use crate::auth::token_handler::{Action, Intent};
use crate::database::dsls::endpoint_dsl::Endpoint;
use crate::database::dsls::hook_dsl::Hook;
use crate::database::dsls::object_dsl::Object;
use crate::database::dsls::user_dsl::User;
use crate::database::dsls::workspaces_dsl::WorkspaceTemplate;
use crate::database::enums::{DataClass, ObjectMapping, ObjectType};
use crate::middlelayer::delete_request_types::DeleteRequest;
use crate::middlelayer::token_request_types::CreateToken;
use crate::middlelayer::workspace_request_types::{CreateTemplate, CreateWorkspace};
use crate::notification::handler::EventHandler;
use crate::{database::crud::CrudDb, middlelayer::db_handler::DatabaseHandler};
use anyhow::{anyhow, Ok, Result};
use aruna_rust_api::api::dataproxy::services::v2::{GetCredentialsRequest, GetCredentialsResponse};
use aruna_rust_api::api::notification::services::v2::EventVariant;
use aruna_rust_api::api::storage::models::v2::{Permission, PermissionLevel};
use aruna_rust_api::api::storage::services::v2::{
    ClaimWorkspaceRequest, CreateApiTokenRequest, DeleteProjectRequest,
};
use diesel_ulid::DieselUlid;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use tonic::metadata::{AsciiMetadataKey, AsciiMetadataValue};
use tonic::Request;

impl DatabaseHandler {
    pub async fn create_workspace_template(
        &self,
        request: CreateTemplate,
        owner: DieselUlid,
    ) -> Result<DieselUlid> {
        let client = self.database.get_client().await?;
        // Build template
        let mut template = request.get_template(owner)?;
        let hooks = template.hook_ids.0.clone();
        // Check if specified hooks exist
        Hook::exists(&hooks, &client).await?;
        // Create template
        template.create(&client).await?;
        Ok(template.id)
    }
    pub async fn create_workspace(
        &self,
        authorizer: Arc<PermissionHandler>,
        request: CreateWorkspace,
        endpoint: String,
    ) -> Result<(DieselUlid, String, String, String)> // (ProjectID, Token, AccessKey, SecretKey)
    {
        let mut client = self.database.get_client().await?;
        let id = DieselUlid::from_str(&request.0.workspace_template)?;

        let template = WorkspaceTemplate::get(id, &client)
            .await?
            .ok_or_else(|| anyhow!("WorkspaceTemplate not found"))?;

        let hooks = template.hook_ids.0.clone();

        // If no endpoint configured for template
        //      -> default endpoint
        // if provided endpoints are configured:
        //      -> ServiceAccount gets all endpoints as trusted
        //      -> Project gets all endpoints
        //      -> S3 creds are returned for first endpoint in list
        let mut endpoints = template.endpoint_ids.0.clone();
        if template.endpoint_ids.0.is_empty() {
            let default_ep = DieselUlid::from_str(&endpoint)?;
            endpoints.push(default_ep);
        }
        let default = Endpoint::get(endpoints[0], &client)
            .await?
            .ok_or_else(|| anyhow!("Default endpoint not found"))?;
        let transaction = client.transaction().await?;
        let transaction_client = transaction.client();
        let mut workspace = CreateWorkspace::make_project(template, endpoints.clone());

        workspace.create(transaction_client).await?;
        if !hooks.is_empty() {
            Hook::add_workspace_to_hook(workspace.id, &hooks, transaction_client).await?;
        }

        // Create service account
        let mut service_user = CreateWorkspace::create_service_account(endpoints, workspace.id);
        service_user.create(transaction_client).await?;
        transaction.commit().await?;

        // Add service account user to cache
        self.cache.add_user(service_user.id, service_user.clone());

        // Create token
        let (token_ulid, _) = self
            .create_token(
                &service_user.id,
                authorizer.token_handler.get_current_pubkey_serial() as i32,
                CreateToken(CreateApiTokenRequest {
                    name: service_user.display_name.clone(),
                    permission: Some(Permission {
                        permission_level: PermissionLevel::Append as i32,
                        resource_id: Some(aruna_rust_api::api::storage::models::v2::permission::ResourceId::ProjectId(workspace.id.to_string())),
                    }),
                    expires_at: None,
                }),
            )
            .await?;

        // Create events for resource and user creation
        let hierarchy = workspace.fetch_object_hierarchies(&client).await?;
        let workspace_with_relations =
            Object::get_object_with_relations(&workspace.id, &client).await?;
        // Add workspace to cache
        self.cache.add_object(workspace_with_relations.clone());
        let block_id = DieselUlid::generate();

        if let Err(err) = self
            .natsio_handler
            .register_resource_event(
                &workspace_with_relations,
                hierarchy,
                EventVariant::Created,
                Some(&block_id),
            )
            .await
        {
            log::error!("{err}");
            return Err(anyhow::anyhow!("Notification emission failed"));
        }
        if let Err(err) = self
            .natsio_handler
            .register_user_event(&service_user, EventVariant::Created)
            .await
        {
            log::error!("{err}");
            return Err(anyhow::anyhow!("Notification emission failed"));
        }

        // Update service account without explicit fetch
        // This is not needed, because create_token() updates cache
        // service_user.attributes.0.tokens.insert(token_ulid, token);
        // self.cache
        //     .update_user(&service_user.id, service_user.clone());

        // Wait for ack of default proxy, so that create credentials does not fail
        self.natsio_handler
            .wait_for_acknowledgement(&default.id.to_string())
            .await?;

        // Sign token
        let token_secret =
            authorizer
                .token_handler
                .sign_user_token(&service_user.id, &token_ulid, None)?;

        // Create creds
        let slt = authorizer.token_handler.sign_dataproxy_slt(
            &service_user.id,
            Some(token_ulid.to_string()),
            Some(Intent {
                target: default.id,
                action: Action::CreateSecrets,
            }),
        )?;

        let mut credentials_request = Request::new(GetCredentialsRequest {});
        credentials_request.metadata_mut().append(
            AsciiMetadataKey::from_bytes("Authorization".as_bytes())?,
            AsciiMetadataValue::try_from(format!("Bearer {slt}"))?,
        );
        let (
            ..,
            GetCredentialsResponse {
                access_key,
                secret_key,
            },
        ) = DatabaseHandler::get_or_create_credentials(
            authorizer.clone(),
            service_user.id,
            None,
            default,
            false,
        )
        .await?;

        Ok((workspace.id, access_key, secret_key, token_secret))
    }

    pub async fn delete_workspace(
        &self,
        workspace_id: DieselUlid,
        service_account: DieselUlid,
    ) -> Result<()> {
        let client = self.database.get_client().await?;

        // Get and delete workspace instance
        let workspace = Object::get_object_with_relations(&workspace_id, &client).await?;
        if !matches!(workspace.object.object_type, ObjectType::PROJECT)
            || !matches!(workspace.object.data_class, DataClass::WORKSPACE)
        {
            return Err(anyhow!("Not allowed to delete non workspace projects"));
        }
        let request = DeleteRequest::Project(DeleteProjectRequest {
            project_id: workspace_id.to_string(),
        });
        self.delete_resource(request).await?;

        // Get and delete service account
        let user = User::get(service_account, &client)
            .await?
            .ok_or_else(|| anyhow!("User not found"))?;
        self.cache.remove_user(&service_account);
        user.delete(&client).await?;

        // Resource events are handled by delete_resource()
        if let Err(err) = self
            .natsio_handler
            .register_user_event(&user, EventVariant::Deleted)
            .await
        {
            log::error!("{err}");
            return Err(anyhow!("Notification emission failed"));
        }

        Ok(())
    }
    pub async fn claim_workspace(
        &self,
        request: ClaimWorkspaceRequest,
        user_id: DieselUlid,
    ) -> Result<()> {
        let workspace_id = DieselUlid::from_str(&request.workspace_id)?;
        let mut client = self.database.get_client().await?;
        // All get requests:
        // - Get project
        let project = Object::get_object_with_relations(&workspace_id, &client).await?;
        // - Get all subresources
        let subresource_ids = project.object.fetch_subresources(&client).await?;
        let mut resources = Object::get_objects_with_relations(&subresource_ids, &client).await?;
        // - Append project to subresources
        resources.push(project);
        // - Affected hooks
        let hooks = Hook::list_hooks(&workspace_id, &client)
            .await?
            .iter()
            .map(|hook| hook.id)
            .collect();

        // All updates:
        let transaction = client.transaction().await?;
        let transaction_client = transaction.client();

        // - Remove all hooks
        Hook::remove_workspace_from_hooks(&workspace_id, &hooks, transaction_client).await?;
        // - Make user account project admin
        User::add_user_permission(
            transaction_client,
            &user_id,
            HashMap::from_iter([(
                workspace_id,
                ObjectMapping::PROJECT(crate::database::enums::DbPermissionLevel::ADMIN),
            )]),
        )
        .await?;

        // - Apply changes in DB
        let mut all_affected_ids = subresource_ids.clone();
        all_affected_ids.push(workspace_id);
        Object::batch_claim(&user_id, &all_affected_ids, transaction_client).await?;
        transaction.commit().await?;

        // - Sync with cache
        for res in &mut resources {
            // - Change created_by to user
            res.object.created_by = user_id;
            // - Change DataClass from workspace to confidential
            res.object.data_class = DataClass::CONFIDENTIAL;
            self.cache.upsert_object(&res.object.id, res.clone());
        }
        let user = User::get(user_id, &client)
            .await?
            .ok_or_else(|| anyhow!("User not found"))?;
        self.cache.update_user(&user.id, user.clone());

        // - Emit notifications (copied from archive project)
        let mut notifications = vec![];
        for obj in &resources {
            notifications.push((
                obj,
                obj.object.fetch_object_hierarchies(&client).await?,
                EventVariant::Updated,
                DieselUlid::generate(), // block_id for deduplication
            ))
        }
        for (object_plus, hierarchies, event_variant, block_id) in notifications {
            if let Err(err) = self
                .natsio_handler
                .register_resource_event(object_plus, hierarchies, event_variant, Some(&block_id))
                .await
            {
                log::error!("{err}");
                return Err(anyhow::anyhow!("Notification emission failed"));
            }
        }
        if let Err(err) = self
            .natsio_handler
            .register_user_event(&user, EventVariant::Updated)
            .await
        {
            log::error!("{err}");
            return Err(anyhow!("Notification emission failed"));
        }

        Ok(())
    }
    pub async fn get_ws_template(&self, ws_id: &DieselUlid) -> Result<WorkspaceTemplate> {
        let client = self.database.get_client().await?;
        let workspace = WorkspaceTemplate::get(*ws_id, &client)
            .await?
            .ok_or_else(|| anyhow!("Template not found"))?;
        Ok(workspace)
    }
    pub async fn get_owned_ws(&self, user_id: &DieselUlid) -> Result<Vec<WorkspaceTemplate>> {
        let client = self.database.get_client().await?;
        let workspaces = WorkspaceTemplate::list_owned(user_id, &client).await?;
        Ok(workspaces)
    }
    pub async fn delete_workspace_template(
        &self,
        workspace_id: String,
        user_id: &DieselUlid,
    ) -> Result<()> {
        let id = DieselUlid::from_str(&workspace_id)?;
        let client = self.database.get_client().await?;
        let workspace = WorkspaceTemplate::get(id, &client)
            .await?
            .ok_or_else(|| anyhow!("WorkspaceTemplate not found"))?;
        if workspace.owner != *user_id {
            Err(anyhow!("Unauthorized delete request"))
        } else {
            workspace.delete(&client).await?;
            Ok(())
        }
    }
}
