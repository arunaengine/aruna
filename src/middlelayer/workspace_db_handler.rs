use crate::auth::permission_handler::PermissionHandler;
use crate::auth::token_handler::{Action, Intent};
use crate::database::dsls::endpoint_dsl::Endpoint;
use crate::database::dsls::hook_dsl::Hook;
use crate::database::dsls::object_dsl::Object;
use crate::database::dsls::workspaces_dsl::WorkspaceTemplate;
use crate::middlelayer::token_request_types::CreateToken;
use crate::middlelayer::workspace_request_types::{CreateTemplate, CreateWorkspace};
use crate::{database::crud::CrudDb, middlelayer::db_handler::DatabaseHandler};
use anyhow::{anyhow, Ok, Result};
use aruna_rust_api::api::dataproxy::services::v2::{GetCredentialsRequest, GetCredentialsResponse};
use aruna_rust_api::api::storage::models::v2::{Permission, PermissionLevel};
use aruna_rust_api::api::storage::services::v2::{ClaimWorkspaceRequest, CreateApiTokenRequest};
use diesel_ulid::DieselUlid;
use std::str::FromStr;
use std::sync::Arc;
use tonic::metadata::{AsciiMetadataKey, AsciiMetadataValue};
use tonic::Request;

impl DatabaseHandler {
    pub async fn create_workspace_template(
        &self,
        request: CreateTemplate,
        owner: DieselUlid,
    ) -> Result<String> {
        let client = self.database.get_client().await?;
        let mut template = request.get_template(owner)?;
        template.create(&client).await?;
        Ok(request.0.name)
    }
    pub async fn create_workspace(
        &self,
        authorizer: Arc<PermissionHandler>,
        request: CreateWorkspace,
        endpoint: String,
    ) -> Result<(DieselUlid, String, String, String)> // (ProjectID, Token, AccessKey, SecretKey)
    {
        let mut client = self.database.get_client().await?;

        let template = WorkspaceTemplate::get_by_name(request.get_name(), &client)
            .await?
            .ok_or_else(|| anyhow!("WorkspaceTemplate not found"))?;

        // If no endpoint configured for template -> default endpoint
        // else if configured
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
        // Create service account
        let user = CreateWorkspace::create_service_account(endpoints, workspace.id);
        // Create token
        let (token_ulid, token) = self
            .create_token(
                &user.id,
                authorizer.token_handler.get_current_pubkey_serial() as i32,
                CreateToken(CreateApiTokenRequest {
                    name: user.display_name,
                    permission: Some(Permission {
                        permission_level: PermissionLevel::Append as i32,
                        resource_id: Some(aruna_rust_api::api::storage::models::v2::permission::ResourceId::ProjectId(workspace.id.to_string())),
                    }),
                    expires_at: None,
                }),
            )
            .await?;
        // Update service account
        user.attributes.0.tokens.insert(token_ulid, token);
        // Sign token
        let token_secret = authorizer
            .token_handler
            .sign_user_token(&user.id, &token_ulid, None)?;

        // Create creds
        let slt = authorizer.token_handler.sign_dataproxy_slt(
            &user.id,
            Some(token_ulid.to_string()),
            Some(Intent {
                target: default.id,
                action: Action::CreateSecrets,
            }),
        )?;
        let mut credentials_request = Request::new(GetCredentialsRequest {});
        credentials_request.metadata_mut().append(
            AsciiMetadataKey::from_bytes("Authorization".as_bytes())?,
            AsciiMetadataValue::try_from(format!("Bearer {}", slt))?,
        );
        let (
            ..,
            GetCredentialsResponse {
                access_key,
                secret_key,
            },
        ) = DatabaseHandler::get_credentials(authorizer.clone(), user.id, None, default).await?;

        Ok((workspace.id, access_key, secret_key, token_secret))
    }

    pub async fn delete_workspace(&self, workspace_id: DieselUlid) -> Result<()> {
        let client = self.database.get_client().await?;
        let workspace = Object::get(workspace_id, &client)
            .await?
            .ok_or_else(|| anyhow!("Workspace not found"))?;
        workspace.delete(&client).await?;
        Ok(())
    }
    pub async fn claim_workspace(&self, request: ClaimWorkspaceRequest) -> Result<()> {
        let workspace_id = DieselUlid::from_str(&request.workspace_id)?;
        let mut client = self.database.get_client().await?;
        // All get requests
        let project = Object::get_object_with_relations(&workspace_id, &client).await?;
        let subresources = project.object.fetch_subresources(&client).await?;
        let _subresource = Object::get_objects_with_relations(&subresources, &client).await?;
        let _hooks = Hook::get_hooks_for_projects(&vec![workspace_id], &client).await?;
        let transaction = client.transaction().await?;
        let _transaction_client = transaction.client();
        // TODO:
        // - Remove all hooks
        // - Make user account project admin
        // - Change DataClass from workspace to confidential
        Err(anyhow!("Not implemented"))
    }
}