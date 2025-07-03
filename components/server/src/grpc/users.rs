use crate::auth::permission_handler::{PermissionCheck, PermissionHandler};
use crate::auth::structs::Context;
use crate::auth::token_handler::{Action, Intent, ProcessedToken, TokenHandler};
use crate::caching::cache::Cache;
use crate::database::enums::DbPermissionLevel;
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::token_request_types::{CreateToken, DeleteToken, GetToken};
use crate::middlelayer::user_request_types::{
    ActivateUser, DeactivateUser, DeleteProxyAttributeSource, GetUser, RegisterUser,
    UpdateUserEmail, UpdateUserName,
};
use crate::utils::conversions::users::{as_api_token, convert_token_to_proto};
use crate::utils::grpc_utils::get_token_from_md;
use crate::utils::mailclient::MailClient;
use anyhow::anyhow;
use aruna_rust_api::api::storage::models::v2::context::Context as ProtoContext;
use aruna_rust_api::api::storage::services::v2::user_service_server::UserService;
use aruna_rust_api::api::storage::services::v2::{
    AcknowledgePersonalNotificationsRequest, AcknowledgePersonalNotificationsResponse,
    ActivateUserRequest, ActivateUserResponse, AddDataProxyAttributeUserRequest,
    AddDataProxyAttributeUserResponse, AddOidcProviderRequest, AddOidcProviderResponse,
    AddPubkeyUserRequest, AddPubkeyUserResponse, AddTrustedEndpointsUserRequest,
    AddTrustedEndpointsUserResponse, CreateApiTokenRequest, CreateApiTokenResponse,
    CreateS3CredentialsUserTokenRequest, CreateS3CredentialsUserTokenResponse,
    DeactivateUserRequest, DeactivateUserResponse, DeleteApiTokenRequest, DeleteApiTokenResponse,
    DeleteApiTokensRequest, DeleteApiTokensResponse, DeleteS3CredentialsUserResponse,
    DeleteS3CredentialsUserTokenRequest, GetAllUsersRequest, GetAllUsersResponse,
    GetApiTokenRequest, GetApiTokenResponse, GetApiTokensRequest, GetApiTokensResponse,
    GetDataproxyTokenUserRequest, GetDataproxyTokenUserResponse, GetNotActivatedUsersRequest,
    GetNotActivatedUsersResponse, GetPersonalNotificationsRequest,
    GetPersonalNotificationsResponse, GetS3CredentialsUserTokenRequest,
    GetS3CredentialsUserTokenResponse, GetUserRedactedRequest, GetUserRedactedResponse,
    GetUserRequest, GetUserResponse, RegisterUserRequest, RegisterUserResponse,
    RemoveDataProxyAttributeUserRequest, RemoveDataProxyAttributeUserResponse,
    RemoveOidcProviderRequest, RemoveOidcProviderResponse, RemoveTrustedEndpointsUserRequest,
    RemoveTrustedEndpointsUserResponse, UpdateUserDisplayNameRequest,
    UpdateUserDisplayNameResponse, UpdateUserEmailRequest, UpdateUserEmailResponse,
};
use diesel_ulid::DieselUlid;
use log::error;
use std::str::FromStr;
use std::sync::Arc;
use tonic::{Request, Response, Status};
crate::impl_grpc_server!(UserServiceImpl, token_handler: Arc<TokenHandler>, mailclient: Arc<Option<MailClient>>);

#[tonic::async_trait]
impl UserService for UserServiceImpl {
    //ToDo: Docs
    async fn register_user(
        &self,
        request: Request<RegisterUserRequest>,
    ) -> Result<Response<RegisterUserResponse>, Status> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );
        let request = RegisterUser(request.into_inner());

        // Validate display name and email is filled
        if request.get_display_name().is_empty() {
            return Err(Status::invalid_argument("Display name is mandatory"));
        }
        if request.get_email().is_empty() {
            return Err(Status::invalid_argument("Email is mandatory"));
        }

        let external_id = tonic_auth!(
            self.authorizer.check_unregistered_oidc(&token).await,
            "Unauthorized"
        );
        let user = tonic_internal!(
            self.database_handler
                .register_user(request, external_id)
                .await,
            "Internal register user error"
        );

        // Try to send mail to recently registered users email address
        if let Some(mailclient) = self.mailclient.as_ref() {
            if let Err(err) = mailclient.send_message(
                &user.email,
                format!("Dear {},
We are excited to inform you that your registration with Aruna has been successfully completed.\n
After your account has been activated by an administrator, you can start exploring all the features we offer. Should you need any assistance or have any questions, you can create support tickets by reaching out to us at support@aruna-storage.org.\n
Thank you for joining us, and we look forward to supporting you on your journey!\n
With kind regards,
the Aruna team", user.display_name),
                "Welcome to Aruna - Registration Successful",
            ) {
                error!("{err:#?}");
            }
        }

        return_with_log!(RegisterUserResponse {
            user_id: user.id.to_string(),
        });
    }

    //ToDo: Docs
    async fn deactivate_user(
        &self,
        request: Request<DeactivateUserRequest>,
    ) -> Result<Response<DeactivateUserResponse>, Status> {
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );
        let request = DeactivateUser(request.into_inner());
        let ctx = Context::admin();
        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        tonic_internal!(
            self.database_handler.deactivate_user(request).await,
            "Internal deactivate user error"
        );

        return_with_log!(DeactivateUserResponse {});
    }

    //ToDo: Docs
    async fn activate_user(
        &self,
        request: Request<ActivateUserRequest>,
    ) -> Result<Response<ActivateUserResponse>, Status> {
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );
        let request = ActivateUser(request.into_inner());
        let ctx = Context::admin();
        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        let user = tonic_internal!(
            self.database_handler.activate_user(request).await,
            "Internal activate user error"
        );

        // Try to send mail to recently activated users email address
        if let Some(mailclient) = self.mailclient.as_ref() {
            if let Err(err) = mailclient.send_message(
                &user.email,
                format!("Dear {},
your Aruna user account has been activated.\n
Again, if you have any questions or need assistance, feel free to contact us via our support address support@aruna-storage.org\n
Kind regards,
the Aruna team", user.display_name),
                "[ARUNA] User activated",
            ) {
                error!("{err:#?}"); 
            }
        }

        return_with_log!(ActivateUserResponse {});
    }

    //ToDo: Docs
    async fn create_api_token(
        &self,
        request: Request<CreateApiTokenRequest>,
    ) -> Result<Response<CreateApiTokenResponse>, Status> {
        log_received!(&request);

        // Consume gRPC request into its parts
        let (metadata, _, inner_request) = request.into_parts();

        // Check empty context if is registered user
        let request_token = tonic_auth!(get_token_from_md(&metadata), "Token authentication error");
        let user_id = tonic_auth!(
            self.authorizer
                .check_permissions(&request_token, vec![Context::default()])
                .await,
            "Unauthorized"
        );

        // Create token in database
        let middlelayer_request = CreateToken(inner_request);
        let (token_ulid, token) = tonic_internal!(
            self.database_handler
                .create_token(
                    &user_id,
                    self.token_handler.get_current_pubkey_serial() as i32,
                    middlelayer_request.clone(),
                )
                .await,
            "Token creation failed"
        );

        // Sign token
        let token_secret = tonic_internal!(
            self.token_handler.sign_user_token(
                &user_id,
                &token_ulid,
                middlelayer_request.0.expires_at,
            ),
            "Token signing failed"
        );

        // Create and return response
        let response = CreateApiTokenResponse {
            token: Some(convert_token_to_proto(&token_ulid, token)),
            token_secret,
        };
        return_with_log!(response);
    }

    //ToDo: Docs
    async fn get_api_token(
        &self,
        request: Request<GetApiTokenRequest>,
    ) -> Result<Response<GetApiTokenResponse>, Status> {
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );
        let request = GetToken(request.into_inner());
        let ctx = Context::self_ctx();
        let user_id = tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        let user = tonic_invalid!(
            self.cache
                .get_user(&user_id)
                .ok_or_else(|| anyhow!("Not found")),
            "User not found"
        );
        let token_id = tonic_invalid!(request.get_token_id(), "Invalid token_id");
        let token = match user.attributes.0.tokens.get(&token_id) {
            Some(token) => Some(as_api_token(token_id, token.clone())),
            None => return Err(Status::not_found("Token not found")),
        };
        let response = GetApiTokenResponse { token };

        return_with_log!(response);
    }

    //ToDo: Docs
    async fn get_api_tokens(
        &self,
        request: Request<GetApiTokensRequest>,
    ) -> Result<Response<GetApiTokensResponse>, Status> {
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );
        let ctx = Context::self_ctx();
        let user_id = tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );
        let user = tonic_invalid!(
            self.cache
                .get_user(&user_id)
                .ok_or_else(|| anyhow!("Not found")),
            "User not found"
        );
        let tokens = Vec::from_iter(
            user.attributes
                .0
                .tokens
                .into_iter()
                .map(|t| as_api_token(t.0, t.1)),
        );
        let response = GetApiTokensResponse { tokens };

        return_with_log!(response);
    }

    //ToDo: Docs
    async fn delete_api_token(
        &self,
        request: Request<DeleteApiTokenRequest>,
    ) -> Result<Response<DeleteApiTokenResponse>, Status> {
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );
        let request = DeleteToken(request.into_inner());
        let ctx = Context::self_ctx();
        let user_id = tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        tonic_internal!(
            self.database_handler.delete_token(user_id, request).await,
            "Internal database request error"
        );

        return_with_log!(DeleteApiTokenResponse {});
    }

    //ToDo: Docs
    async fn delete_api_tokens(
        &self,
        request: Request<DeleteApiTokensRequest>,
    ) -> Result<Response<DeleteApiTokensResponse>, Status> {
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );
        let ctx = Context::self_ctx();
        let user_id = tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );
        tonic_internal!(
            self.database_handler.delete_all_tokens(user_id).await,
            "Internal database request error"
        );

        return_with_log!(DeleteApiTokensResponse {});
    }

    //ToDo: Docs
    async fn get_user(
        &self,
        request: Request<GetUserRequest>,
    ) -> Result<Response<GetUserResponse>, Status> {
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );
        let request = GetUser::GetUser(request.into_inner());

        let user_id = match tonic_invalid!(request.get_user(), "Invalid user id") {
            // If admin requests users this gets a user context scope
            (Some(id), ctx) => {
                self.authorizer.check_permissions(&token, vec![ctx]).await?;
                id
            }
            // If user requests himself, this is a self context
            (None, ctx) => self.authorizer.check_permissions(&token, vec![ctx]).await?,
        };
        let user = self.cache.get_user(&user_id);
        let response = GetUserResponse {
            user: user.map(|user| user.into()),
        };
        return_with_log!(response);
    }

    //ToDo: Docs
    async fn get_user_redacted(
        &self,
        request: Request<GetUserRedactedRequest>,
    ) -> Result<Response<GetUserRedactedResponse>, Status> {
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );
        tonic_auth!(
            self.authorizer
                .check_permissions(
                    &token,
                    vec![Context::user_ctx(
                        tonic_invalid!(
                            DieselUlid::from_str(&request.get_ref().user_id),
                            "Invalid user_id"
                        ),
                        DbPermissionLevel::READ
                    )]
                )
                .await,
            "Unauthorized"
        );
        let user_ulid = tonic_invalid!(
            DieselUlid::from_str(&request.get_ref().user_id),
            "Invalid user_id"
        );
        let user = self.cache.get_user(&user_ulid);
        let response = GetUserRedactedResponse {
            user: user.map(|user| user.into_redacted()),
        };
        return_with_log!(response);
    }

    //ToDo: Docs
    async fn update_user_display_name(
        &self,
        request: Request<UpdateUserDisplayNameRequest>,
    ) -> Result<Response<UpdateUserDisplayNameResponse>, Status> {
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );
        let request = UpdateUserName(request.into_inner());
        let ctx = Context::self_ctx();
        let user_id = tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        let user = tonic_internal!(
            self.database_handler
                .update_display_name(request, user_id)
                .await,
            "Internal deactivate user error"
        );

        let response = UpdateUserDisplayNameResponse {
            user: Some(user.into()),
        };
        return_with_log!(response);
    }

    //ToDo: Docs
    async fn update_user_email(
        &self,
        request: Request<UpdateUserEmailRequest>,
    ) -> Result<Response<UpdateUserEmailResponse>, Status> {
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );
        let request = UpdateUserEmail(request.into_inner());
        let user_id = tonic_invalid!(request.get_user(), "Invalid user id");
        let ctx = Context::user_ctx(user_id, DbPermissionLevel::WRITE);
        let user_id = tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        let user = tonic_internal!(
            self.database_handler.update_email(request, user_id).await,
            "Internal deactivate user error"
        );

        let response = UpdateUserEmailResponse {
            user: Some(user.into()),
        };
        return_with_log!(response);
    }

    //ToDo: Docs
    async fn get_not_activated_users(
        &self,
        request: Request<GetNotActivatedUsersRequest>,
    ) -> Result<Response<GetNotActivatedUsersResponse>, Status> {
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );
        let ctx = Context::admin();
        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        let users = self.cache.get_all_deactivated().await;

        let response = GetNotActivatedUsersResponse { users };
        return_with_log!(response);
    }

    async fn get_all_users(
        &self,
        request: Request<GetAllUsersRequest>,
    ) -> Result<Response<GetAllUsersResponse>, Status> {
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );
        let ctx = Context::admin();
        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );
        let user = self.cache.get_all_users_proto().await;

        let response = GetAllUsersResponse { user };
        return_with_log!(response);
    }

    //ToDo: Docs
    async fn get_s3_credentials_user_token(
        &self,
        request: Request<GetS3CredentialsUserTokenRequest>,
    ) -> Result<Response<GetS3CredentialsUserTokenResponse>, Status> {
        log_received!(&request);

        // Consume gRPC request into its parts
        let (metadata, _, inner_request) = request.into_parts();

        // Check empty context if is registered user
        let token = tonic_auth!(get_token_from_md(&metadata), "Token authentication error");
        let PermissionCheck {
            user_id,
            token: maybe_token,
            ..
        } = tonic_auth!(
            self.authorizer
                .check_permissions_verbose(&token, vec![Context::default()])
                .await,
            "Unauthorized"
        );

        // Remove trusted endpoints from user
        let (s3_access_key, s3_secret_key, s3_endpoint_url) = tonic_internal!(
            self.database_handler
                .get_s3_credentials(
                    user_id,
                    maybe_token,
                    inner_request.endpoint_id,
                    &self.authorizer.token_handler
                )
                .await,
            "Failed to add endpoint to user"
        );

        // Return S3 credentials to user
        let response = GetS3CredentialsUserTokenResponse {
            s3_access_key,
            s3_secret_key,
            s3_endpoint_url,
        };
        return_with_log!(response);
    }

    //ToDo: Docs
    async fn get_dataproxy_token_user(
        &self,
        request: Request<GetDataproxyTokenUserRequest>,
    ) -> Result<Response<GetDataproxyTokenUserResponse>, Status> {
        log_received!(&request);

        // Consume gRPC request into its parts
        let (metadata, _, inner_request) = request.into_parts();

        // Check empty context if is registered user
        let token = tonic_auth!(get_token_from_md(&metadata), "Token authentication error");
        let PermissionCheck {
            user_id: user_ulid,
            token: maybe_token,
            is_proxy: proxy_request,
            ..
        } = tonic_auth!(
            self.authorizer
                .check_permissions_verbose(&token, vec![Context::self_ctx()])
                .await,
            "Unauthorized"
        );

        // Check if endpoint is in trusted_endpoints
        let endpoint_id = tonic_invalid!(
            DieselUlid::from_str(&inner_request.endpoint_id),
            "Invalid endpoint id"
        );
        if !self
            .cache
            .get_user(&user_ulid)
            .ok_or_else(|| Status::not_found("User not found"))?
            .attributes
            .0
            .trusted_endpoints
            .contains_key(&endpoint_id)
        {
            return Err(Status::invalid_argument("Endpoint is not trusted"));
        };

        // Create token based on provided context
        let response_token = if let Some(context) = inner_request.context {
            match context.context {
                Some(con) => {
                    match con {
                        ProtoContext::S3Credentials(_) => {
                            tonic_internal!(
                                self.authorizer.token_handler.sign_dataproxy_slt(
                                    &user_ulid,
                                    maybe_token.map(|token_id| token_id.to_string()), // Token_Id of user token; None if OIDC
                                    Some(Intent {
                                        target: endpoint_id,
                                        action: Action::CreateSecrets
                                    }),
                                ),
                                "Token signing failed"
                            )
                        }
                        ProtoContext::Copy(_) => {
                            unimplemented!(
                                "Dataproxy data replication token creation not yet implemented"
                            )
                        }
                    }
                }
                None => return Err(Status::invalid_argument("Missing context action")),
            }
        } else if proxy_request {
            return Err(Status::invalid_argument("No context provided"));
        } else {
            tonic_internal!(
                self.authorizer.token_handler.sign_dataproxy_slt(
                    &user_ulid,
                    maybe_token.map(|token_id| token_id.to_string()), // Token_Id of user token; None if OIDC
                    Some(Intent {
                        target: endpoint_id,
                        action: Action::All
                    }),
                ),
                "Token signing failed"
            )
        };

        // Return token to user
        let response = GetDataproxyTokenUserResponse {
            token: response_token,
        };
        return_with_log!(response);
    }

    //ToDo: Docs
    async fn get_personal_notifications(
        &self,
        request: Request<GetPersonalNotificationsRequest>,
    ) -> tonic::Result<Response<GetPersonalNotificationsResponse>> {
        log_received!(&request);

        // Consume gRPC request into its parts
        let (request_metadata, _, _) = request.into_parts();

        // Extract token from request and check permissions
        let token = tonic_auth!(
            get_token_from_md(&request_metadata),
            "Token authentication error"
        );

        let ctx = Context::self_ctx();
        let token_user_ulid = tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        // Fetch personal notifications from database
        let notifications = tonic_internal!(
            self.database_handler
                .get_persistent_notifications(token_user_ulid)
                .await,
            "Failed to fetch personal notifications"
        );

        // Return personal notifications
        let response = GetPersonalNotificationsResponse { notifications };
        return_with_log!(response);
    }

    //ToDo: Docs
    async fn acknowledge_personal_notifications(
        &self,
        request: Request<AcknowledgePersonalNotificationsRequest>,
    ) -> tonic::Result<Response<AcknowledgePersonalNotificationsResponse>> {
        log_received!(&request);

        // Consume gRPC request into its parts
        let (request_metadata, _, inner_request) = request.into_parts();

        // Extract token from request and check permissions
        let token = tonic_auth!(
            get_token_from_md(&request_metadata),
            "Token authentication error"
        );

        let ctx = Context::self_ctx();
        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        // Acknowledge personal notifications in database
        tonic_internal!(
            self.database_handler
                .acknowledge_persistent_notifications(inner_request.notification_ids)
                .await,
            "Failed to acknowledge personal notifications"
        );

        // Return empty response on success
        let response = AcknowledgePersonalNotificationsResponse {};
        return_with_log!(response);
    }

    async fn add_oidc_provider(
        &self,
        request: Request<AddOidcProviderRequest>,
    ) -> Result<Response<AddOidcProviderResponse>, Status> {
        log_received!(&request);

        // Consume gRPC request into its parts
        let (request_metadata, _, inner_request) = request.into_parts();

        // Extract token from request and check permissions
        let token = tonic_auth!(
            get_token_from_md(&request_metadata),
            "Token authentication error"
        );

        let ctx = Context::self_ctx();
        let user_id = tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        let new_mapping = tonic_internal!(
            self.authorizer
                .check_unregistered_oidc(&inner_request.new_access_token)
                .await,
            "Failed to add OIDC provider"
        );

        // Acknowledge personal notifications in database
        let user = tonic_internal!(
            self.database_handler
                .add_oidc_provider(user_id, &new_mapping)
                .await,
            "Failed to add oidc_provider to user"
        );

        // Return empty response on success
        let response = AddOidcProviderResponse {
            user: Some(user.into()),
        };
        return_with_log!(response);
    }

    async fn remove_oidc_provider(
        &self,
        request: Request<RemoveOidcProviderRequest>,
    ) -> Result<Response<RemoveOidcProviderResponse>, Status> {
        log_received!(&request);

        // Consume gRPC request into its parts
        let (request_metadata, _, inner_request) = request.into_parts();

        // Extract token from request and check permissions
        let token = tonic_auth!(
            get_token_from_md(&request_metadata),
            "Token authentication error"
        );

        let ctx = Context::self_ctx();
        let user_id = tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        // Acknowledge personal notifications in database
        let user = tonic_internal!(
            self.database_handler
                .remove_oidc_provider(user_id, &inner_request.provider_url)
                .await,
            "Failed to add oidc_provider to user"
        );

        // Return empty response on success
        let response = RemoveOidcProviderResponse {
            user: Some(user.into()),
        };
        return_with_log!(response);
    }

    async fn add_pubkey_user(
        &self,
        request: Request<AddPubkeyUserRequest>,
    ) -> Result<Response<AddPubkeyUserResponse>, Status> {
        log_received!(&request);

        // Consume gRPC request into its parts
        let (request_metadata, _, inner_request) = request.into_parts();

        // Extract token from request and check permissions
        let token = tonic_auth!(
            get_token_from_md(&request_metadata),
            "Token authentication error"
        );

        let ctx = Context::self_ctx();
        let user_id = tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        // Add trusted endpoints to user
        let user = tonic_internal!(
            self.database_handler
                .add_pubkey_to_user(inner_request.public_key, user_id)
                .await,
            "Failed to add endpoint to user"
        );

        // Return response
        let response = AddPubkeyUserResponse {
            user: Some(user.into()),
        };
        return_with_log!(response);
    }
    async fn add_trusted_endpoints_user(
        &self,
        request: Request<AddTrustedEndpointsUserRequest>,
    ) -> Result<Response<AddTrustedEndpointsUserResponse>, Status> {
        log_received!(&request);

        // Consume gRPC request into its parts
        let (request_metadata, _, inner_request) = request.into_parts();

        // Extract token from request and check permissions
        let token = tonic_auth!(
            get_token_from_md(&request_metadata),
            "Token authentication error"
        );

        let ctx = Context::self_ctx();
        let user_id = tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        // Add trusted endpoints to user
        let user = tonic_internal!(
            self.database_handler
                .add_trusted_endpoint_to_user(user_id, inner_request)
                .await,
            "Failed to add endpoint to user"
        );

        // Return response
        let response = AddTrustedEndpointsUserResponse {
            user: Some(user.into()),
        };
        return_with_log!(response);
    }
    async fn remove_trusted_endpoints_user(
        &self,
        request: Request<RemoveTrustedEndpointsUserRequest>,
    ) -> Result<Response<RemoveTrustedEndpointsUserResponse>, Status> {
        log_received!(&request);

        // Consume gRPC request into its parts
        let (request_metadata, _, inner_request) = request.into_parts();

        // Extract token from request and check permissions
        let token = tonic_auth!(
            get_token_from_md(&request_metadata),
            "Token authentication error"
        );

        let ctx = Context::self_ctx();
        let user_id = tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        // Remove trusted endpoints from user
        let user = tonic_internal!(
            self.database_handler
                .remove_trusted_endpoint_from_user(user_id, inner_request)
                .await,
            "Failed to add endpoint to user"
        );

        // Return response
        let response = RemoveTrustedEndpointsUserResponse {
            user: Some(user.into()),
        };
        return_with_log!(response);
    }
    async fn add_data_proxy_attribute_user(
        &self,
        request: Request<AddDataProxyAttributeUserRequest>,
    ) -> Result<Response<AddDataProxyAttributeUserResponse>, Status> {
        log_received!(&request);

        // Consume gRPC request into its parts
        let (request_metadata, _, inner_request) = request.into_parts();

        // Extract token from request and check permissions
        let token = tonic_auth!(
            get_token_from_md(&request_metadata),
            "Token authentication error"
        );

        let ctx = Context::proxy();
        let PermissionCheck { proxy_id, .. } = tonic_auth!(
            self.authorizer
                .check_permissions_verbose(&token, vec![ctx])
                .await,
            "Unauthorized"
        );
        let proxy_id = if let Some(id) = proxy_id {
            id
        } else {
            return Err(Status::unauthenticated("Unauthorized"));
        };

        // Remove trusted endpoints from user
        tonic_internal!(
            self.database_handler
                .add_data_proxy_attribute(inner_request, proxy_id)
                .await,
            "Failed to add endpoint to user"
        );

        // Return response
        return_with_log!(AddDataProxyAttributeUserResponse {});
    }
    async fn remove_data_proxy_attribute_user(
        &self,
        request: Request<RemoveDataProxyAttributeUserRequest>,
    ) -> Result<Response<RemoveDataProxyAttributeUserResponse>, Status> {
        log_received!(&request);

        // Consume gRPC request into its parts
        let (request_metadata, _, inner_request) = request.into_parts();

        // Extract token from request and check permissions
        let token = tonic_auth!(
            get_token_from_md(&request_metadata),
            "Token authentication error"
        );
        let ProcessedToken {
            main_id, is_proxy, ..
        } = tonic_auth!(
            self.token_handler.process_token(&token).await,
            "Unauthorized"
        );

        let id = if is_proxy {
            let ctx = Context::proxy();
            tonic_auth!(
                self.authorizer.check_permissions(&token, vec![ctx]).await,
                "Unauthorized"
            );
            DeleteProxyAttributeSource::Proxy(main_id)
        } else {
            let ctx = Context::self_ctx();
            let user_id = tonic_auth!(
                self.authorizer.check_permissions(&token, vec![ctx]).await,
                "Unauthorized"
            );
            DeleteProxyAttributeSource::User(user_id)
        };

        // Remove trusted endpoints from user
        tonic_internal!(
            self.database_handler
                .rm_data_proxy_attribute(inner_request, id)
                .await,
            "Failed to add endpoint to user"
        );

        // Return response
        return_with_log!(RemoveDataProxyAttributeUserResponse {});
    }
    async fn create_s3_credentials_user_token(
        &self,
        request: Request<CreateS3CredentialsUserTokenRequest>,
    ) -> Result<Response<CreateS3CredentialsUserTokenResponse>, Status> {
        log_received!(&request);

        // Consume gRPC request into its parts
        let (request_metadata, _, inner_request) = request.into_parts();

        // Extract token from request and check permissions
        let token = tonic_auth!(
            get_token_from_md(&request_metadata),
            "Token authentication error"
        );

        let ctx = Context::self_ctx();
        let PermissionCheck {
            user_id,
            token: token_id,
            ..
        } = tonic_auth!(
            self.authorizer
                .check_permissions_verbose(&token, vec![ctx])
                .await,
            "Unauthorized"
        );

        // Remove trusted endpoints from user
        let (s3_access_key, s3_secret_key, s3_endpoint_url) = tonic_internal!(
            self.database_handler
                .create_s3_credentials_with_user_token(
                    user_id,
                    inner_request.endpoint_id,
                    token_id,
                    &self.authorizer.token_handler
                )
                .await,
            "Failed to add endpoint to user"
        );

        // Return response
        let response = CreateS3CredentialsUserTokenResponse {
            s3_access_key,
            s3_secret_key,
            s3_endpoint_url,
        };
        return_with_log!(response);
    }
    async fn delete_s3_credentials_user_token(
        &self,
        request: Request<DeleteS3CredentialsUserTokenRequest>,
    ) -> Result<Response<DeleteS3CredentialsUserResponse>, Status> {
        log_received!(&request);

        // Consume gRPC request into its parts
        let (request_metadata, _, inner_request) = request.into_parts();

        // Extract token from request and check permissions
        let token = tonic_auth!(
            get_token_from_md(&request_metadata),
            "Token authentication error"
        );

        let ctx = Context::self_ctx();
        let PermissionCheck {
            user_id,
            token: token_id,
            ..
        } = tonic_auth!(
            self.authorizer
                .check_permissions_verbose(&token, vec![ctx])
                .await,
            "Unauthorized"
        );

        // Remove trusted endpoints from user
        tonic_internal!(
            self.database_handler
                .delete_s3_credentials_with_user_token(
                    user_id,
                    inner_request.endpoint_id,
                    token_id,
                    &self.authorizer.token_handler
                )
                .await,
            "Failed to add endpoint to user"
        );

        return_with_log!(DeleteS3CredentialsUserResponse {});
    }
}
