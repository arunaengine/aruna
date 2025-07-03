use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use crate::auth::token_handler::{Action, Intent, TokenHandler};
use crate::database::crud::CrudDb;
use crate::database::dsls::persistent_notification_dsl::{
    NotificationReference, NotificationReferences, PersistentNotification,
};
use crate::database::dsls::user_dsl::{DataProxyAttribute, OIDCMapping, User, UserAttributes};
use crate::database::enums::{
    DataProxyFeature, DbPermissionLevel, NotificationReferenceType, ObjectMapping,
    PersistentNotificationVariant,
};
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::endpoints_request_types::GetEP;
use crate::middlelayer::user_request_types::{
    ActivateUser, DeactivateUser, DeleteProxyAttributeSource, RegisterUser, UpdateUserEmail,
    UpdateUserName,
};
use crate::notification::handler::EventHandler;
use anyhow::{anyhow, bail, Result};
use aruna_rust_api::api::dataproxy::services::v2::dataproxy_user_service_client::DataproxyUserServiceClient;
use aruna_rust_api::api::dataproxy::services::v2::{
    CreateOrUpdateCredentialsRequest, CreateOrUpdateCredentialsResponse, GetCredentialsRequest,
    RevokeCredentialsRequest,
};
use aruna_rust_api::api::notification::services::v2::EventVariant;
use aruna_rust_api::api::storage::services::v2::get_endpoint_request::Endpoint;
use aruna_rust_api::api::storage::services::v2::{
    AddDataProxyAttributeUserRequest, AddTrustedEndpointsUserRequest, GetEndpointRequest,
    PersonalNotification, RemoveDataProxyAttributeUserRequest, RemoveTrustedEndpointsUserRequest,
};
use diesel_ulid::DieselUlid;
use postgres_types::Json;
use tokio::sync::Notify;
use tokio_postgres::GenericClient;
use tonic::metadata::{AsciiMetadataKey, AsciiMetadataValue};
use tonic::transport::{Channel, ClientTlsConfig};
use tonic::{Request, Status};

impl DatabaseHandler {
    pub async fn register_user(
        &self,
        request: RegisterUser,
        external_id: OIDCMapping,
    ) -> Result<User> {
        let client = self.database.get_client().await?;
        let user_id = DieselUlid::generate();
        let new_attributes = UserAttributes {
            global_admin: false,
            service_account: false,
            tokens: Default::default(),
            trusted_endpoints: Default::default(),
            custom_attributes: vec![],
            permissions: Default::default(),
            external_ids: vec![external_id],
            pubkey: "".to_string(),
            data_proxy_attribute: Default::default(),
        };
        let mut user = User {
            id: user_id,
            display_name: request.get_display_name(),
            first_name: "".to_string(),
            last_name: "".to_string(),
            email: request.get_email(),
            attributes: Json(new_attributes),
            active: false,
        };

        // Create new user in database
        user.create(client.client()).await?;

        // Add user to cache
        self.cache.add_user(user.id, user.clone());

        // Try to emit user updated notification(s)
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

    pub async fn deactivate_user(&self, request: DeactivateUser) -> Result<User> {
        let client = self.database.get_client().await?;
        let id = request.get_id()?;

        // Update user activation status in database
        let user = User::deactivate_user(&client, &id).await?;

        // Update user activaetion status in cache
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

        Ok(user)
    }

    pub async fn activate_user(&self, request: ActivateUser) -> Result<User> {
        let client = self.database.get_client().await?;
        let id = request.get_id()?;

        // Update user activation status in database
        let user = User::activate_user(&client, &id).await?;

        // Update user activation status in cache
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

        Ok(user)
    }

    pub async fn update_display_name(
        &self,
        request: UpdateUserName,
        user_id: DieselUlid,
    ) -> Result<User> {
        let client = self.database.get_client().await?;
        let name = request.get_name();

        // Update user display name in database
        let user = User::update_display_name(&client, &user_id, name).await?;

        // Update user display name in cache
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

        Ok(user)
    }

    pub async fn update_email(
        &self,
        request: UpdateUserEmail,
        user_id: DieselUlid,
    ) -> Result<User> {
        let client = self.database.get_client().await?;
        let email = request.get_email();

        // Update user email in database
        let user = User::update_email(&client, &user_id, email).await?;

        // Update user email in cache
        self.cache.update_user(&user_id, user.clone());

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

        Ok(user)
    }

    pub async fn add_endpoint_to_user(
        &self,
        user_id: DieselUlid,
        endpoint_id: DieselUlid,
        notifier: Option<&Notify>,
    ) -> Result<User> {
        let client = self.database.get_client().await?;

        // Update user endpoints in database
        let user = User::add_trusted_endpoint(&client, &user_id, &endpoint_id).await?;

        // Update user endpoints in cache
        self.cache.update_user(&user_id, user.clone());

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
        if let Some(notifier) = notifier {
            notifier.notify_one();
        }
        Ok(user)
    }

    pub async fn add_permission_to_user(
        &self,
        user_id: DieselUlid,
        resource_id: DieselUlid,
        resource_name: &str,
        perm_level: ObjectMapping<DbPermissionLevel>,
        persistent_notification: bool,
    ) -> Result<User> {
        let client = self.database.get_client().await?;

        // Update user permissions in database
        let user = User::add_user_permission(
            &client,
            &user_id,
            HashMap::from_iter([(resource_id, perm_level)]),
        )
        .await?;

        // Update user permissions in cache
        self.cache.update_user(&user.id, user.clone());

        // Create personal/persistent notification (if needed)
        if persistent_notification {
            let mut p_notification = PersistentNotification {
                id: DieselUlid::generate(),
                user_id,
                notification_variant: PersistentNotificationVariant::PERMISSION_GRANTED,
                message: format!("Permission granted for {resource_name} ({resource_id})"),
                refs: Json(NotificationReferences(vec![NotificationReference {
                    reference_type: NotificationReferenceType::Resource,
                    reference_name: resource_name.to_string(),
                    reference_value: resource_id.to_string(),
                }])),
            };
            p_notification.create(&client).await?;
        }

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

        Ok(user)
    }

    pub async fn remove_permission_from_user(
        &self,
        user_id: DieselUlid,
        resource_id: DieselUlid,
    ) -> Result<User> {
        let client = self.database.get_client().await?;

        // Fetch resource to validate it exists
        let resource = if let Some(resource) = self.cache.get_object(&resource_id) {
            resource
        } else {
            bail!("Object does not exist");
        };

        // Remove permission for specific resource from user
        let user = User::remove_user_permission(&client, &user_id, &resource_id).await?;

        // Update user in cache
        self.cache.update_user(&user.id, user.clone());

        // Create personal/persistent notification (no transaction needed)
        let mut p_notification = PersistentNotification {
            id: DieselUlid::generate(),
            user_id,
            notification_variant: PersistentNotificationVariant::PERMISSION_REVOKED,
            message: format!(
                "Permission revoked for {} ({})",
                resource.object.name, resource_id
            ),
            refs: Json(NotificationReferences(vec![NotificationReference {
                reference_type: NotificationReferenceType::Resource,
                reference_name: resource.object.name,
                reference_value: resource.object.id.to_string(),
            }])),
        };
        p_notification.create(&client).await?;

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

        Ok(user)
    }

    pub async fn update_permission_from_user(
        &self,
        user_id: DieselUlid,
        resource_id: DieselUlid,
        permission: ObjectMapping<DbPermissionLevel>,
    ) -> Result<User> {
        let client = self.database.get_client().await?;

        // Remove permission for specific resource from user
        let user =
            User::update_user_permission(&client, &user_id, &resource_id, permission).await?;

        // Update user display name in cache
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

        Ok(user)
    }

    //ToDo: Rust Doc
    pub async fn get_persistent_notifications(
        &self,
        user_id: DieselUlid,
    ) -> Result<Vec<PersonalNotification>> {
        let client = self.database.get_client().await?;

        // Fetch notifications from database and convert to protobuf representation
        let proto_notifications = PersistentNotification::get_user_notifications(&user_id, &client)
            .await?
            .into_iter()
            .map(|m| m.into())
            .collect();

        Ok(proto_notifications)
    }

    //ToDo: Rust Doc
    pub async fn acknowledge_persistent_notifications(
        &self,
        notification_ids: Vec<String>,
    ) -> Result<()> {
        let client = self.database.get_client().await?;

        // Convert provided id strings to DieselUlids
        let result: Result<Vec<_>, _> = notification_ids
            .into_iter()
            .map(|id| DieselUlid::from_str(&id))
            .collect();

        let notification_ulids = tonic_invalid!(result, "Invalid notification ids provided");

        // Acknowledge notification (delete from persistent notifications table)
        PersistentNotification::acknowledge_user_notifications(&notification_ulids, &client)
            .await?;

        Ok(())
    }

    pub async fn add_oidc_provider(
        &self,
        user_id: DieselUlid,
        mapping: &OIDCMapping,
    ) -> Result<User> {
        let client = self.database.get_client().await?;
        if self.cache.oidc_mapping_exists(mapping) {
            bail!("Oidc ID already registered");
        }
        let user = self
            .cache
            .get_user(&user_id)
            .ok_or_else(|| anyhow!("User not found"))?;

        let mut new_attributes = user.attributes.0.clone();
        new_attributes.external_ids.push(mapping.clone());
        let user = User::set_user_attributes(&client, &user_id, Json(new_attributes)).await?;
        self.cache.update_user(&user_id, user.clone());
        Ok(user)
    }

    pub async fn remove_oidc_provider(
        &self,
        user_id: DieselUlid,
        provider_name: &str,
    ) -> Result<User> {
        let client = self.database.get_client().await?;
        let user = self
            .cache
            .get_user(&user_id)
            .ok_or_else(|| anyhow!("User not found"))?;

        let mut new_attributes = user.attributes.0.clone();
        if user.attributes.0.external_ids.len() == 1 {
            bail!("Cannot remove last external id");
        }
        new_attributes
            .external_ids
            .retain(|e| e.oidc_name != provider_name);
        let user = User::set_user_attributes(&client, &user_id, Json(new_attributes)).await?;
        self.cache.update_user(&user_id, user.clone());
        Ok(user)
    }

    //ToDo: Rust Doc
    pub async fn request_resource_access(
        &self,
        request_user_ulid: DieselUlid,
        resource_ulid: DieselUlid,
    ) -> Result<()> {
        let client = self.database.get_client().await?;

        // Fetch resource and requesting user to validate they exist
        let resource = if let Some(resource) = self.cache.get_object(&resource_ulid) {
            resource
        } else {
            bail!("Object does not exist");
        };

        let request_user = if let Some(cache_user) = self.cache.get_user(&request_user_ulid) {
            cache_user
        } else {
            bail!("Requesting user does not exist");
        };

        // Create personal/persistent notification
        let mut p_notification = PersistentNotification {
            id: DieselUlid::generate(),
            user_id: resource.object.created_by,
            notification_variant: PersistentNotificationVariant::ACCESS_REQUESTED,
            message: format!(
                "{} ({}) requests access for {:?} {} ({})",
                request_user.display_name,
                request_user.id,
                resource.object.object_type,
                resource.object.name,
                resource_ulid
            ),
            refs: Json(NotificationReferences(vec![
                NotificationReference {
                    reference_type: NotificationReferenceType::User,
                    reference_name: request_user.display_name,
                    reference_value: request_user.id.to_string(),
                },
                NotificationReference {
                    reference_type: NotificationReferenceType::Resource,
                    reference_name: resource.object.name,
                    reference_value: resource.object.id.to_string(),
                },
            ])),
        };
        p_notification.create(&client).await?;

        Ok(())
    }

    pub async fn add_trusted_endpoint_to_user(
        &self,
        user_id: DieselUlid,
        request: AddTrustedEndpointsUserRequest,
    ) -> Result<User> {
        let client = self.database.get_client().await?;
        let endpoint = DieselUlid::from_str(&request.endpoint_id)?;
        let user = User::add_trusted_endpoint(&client, &user_id, &endpoint).await?;
        self.cache.update_user(&user_id, user.clone());
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
        Ok(user)
    }

    pub async fn remove_trusted_endpoint_from_user(
        &self,
        user_id: DieselUlid,
        request: RemoveTrustedEndpointsUserRequest,
    ) -> Result<User> {
        let client = self.database.get_client().await?;
        let endpoint = DieselUlid::from_str(&request.endpoint_id)?;
        let user = User::remove_trusted_endpoint(&client, &user_id, &endpoint).await?;
        self.cache.update_user(&user_id, user.clone());
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
        Ok(user)
    }

    pub async fn create_s3_credentials_with_user_token(
        &self,
        user_id: DieselUlid,
        endpoint_id: String,
        token_id: Option<DieselUlid>,
        token_handler: &TokenHandler,
    ) -> Result<(String, String, String)> {
        let endpoint_ulid = DieselUlid::from_str(&endpoint_id)?;
        let user = self
            .cache
            .get_user(&user_id)
            .ok_or_else(|| Status::not_found("User not found"))?;

        // Service accounts are not allowed to get additional trusted endpoints
        if user.attributes.0.service_account
            && !user
                .attributes
                .0
                .trusted_endpoints
                .contains_key(&endpoint_ulid)
        {
            return Err(anyhow!(
                "Service accounts are not allowed to add non-predefined endpoints",
            ));
        }
        let endpoint = self
            .get_endpoint(GetEP(GetEndpointRequest {
                endpoint: Some(Endpoint::EndpointId(endpoint_id)),
            }))
            .await?;

        // Create slt for proxy interaction
        let short_lived_token = token_handler.sign_dataproxy_slt(
            &user_id,
            token_id.map(|t| t.to_string()),
            Some(Intent {
                target: endpoint_ulid,
                action: Action::CreateSecrets,
            }),
        )?;

        let notifier = Arc::new(Notify::new());
        // Add endpoint to user
        if !user.attributes.0.service_account {
            self.add_endpoint_to_user(user_id, endpoint.id, Some(&notifier))
                .await?;
        }

        self.natsio_handler
            .wait_for_acknowledgement(&endpoint.id.to_string())
            .await?;
        //self.add_endpoint_to_user(user_id, endpoint.id).await?;
        //notifier.notify_one();

        // Collect endpoint info
        let mut endpoint_host_url: String = String::new();
        let mut endpoint_s3_url: String = String::new();
        for endpoint_config in endpoint.host_config.0 .0 {
            match endpoint_config.feature {
                DataProxyFeature::GRPC => endpoint_host_url = endpoint_config.url,
                DataProxyFeature::S3 => endpoint_s3_url = endpoint_config.url,
            }
            if !endpoint_s3_url.is_empty() && !endpoint_host_url.is_empty() {
                break;
            }
        }

        // Check if dataproxy host url is tls
        let dp_endpoint = if endpoint_host_url.starts_with("https") {
            Channel::from_shared(endpoint_host_url)
                .map_err(|_| Status::internal("Could not connect to Dataproxy"))?
                .tls_config(ClientTlsConfig::new())
                .map_err(|_| Status::internal("Could not connect to Dataproxy"))?
        } else {
            Channel::from_shared(endpoint_host_url)
                .map_err(|_| Status::internal("Could not connect to Dataproxy"))?
        };

        // Create dataproxy client
        let mut dp_conn = DataproxyUserServiceClient::connect(dp_endpoint).await?;

        // Create GetCredentialsRequest with slt in header ...
        let mut credentials_request = Request::new(CreateOrUpdateCredentialsRequest {});
        credentials_request.metadata_mut().append(
            AsciiMetadataKey::from_bytes("Authorization".as_bytes())?,
            AsciiMetadataValue::try_from(format!("Bearer {short_lived_token}"))?,
        );

        let response = tokio::spawn(async move {
            notifier.notified().await;
            Ok::<CreateOrUpdateCredentialsResponse, Status>(
                dp_conn
                    .create_or_update_credentials(credentials_request)
                    .await?
                    .into_inner(),
            )
        })
        .await??;
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
        Ok((
            response.access_key,
            response.secret_key,
            endpoint_s3_url.to_string(),
        ))
    }

    pub async fn get_s3_credentials(
        &self,
        user_id: DieselUlid,
        token_id: Option<DieselUlid>,
        endpoint_id: String,
        token_handler: &TokenHandler,
    ) -> Result<(String, String, String)> {
        // Get endpoint
        let endpoint_ulid = DieselUlid::from_str(&endpoint_id)?;
        let endpoint = self
            .get_endpoint(GetEP(GetEndpointRequest {
                endpoint: Some(Endpoint::EndpointId(endpoint_id)),
            }))
            .await?;

        // Create slt for proxy interaction
        let short_lived_token = token_handler.sign_dataproxy_slt(
            &user_id,
            token_id.map(|t| t.to_string()),
            Some(Intent {
                target: endpoint_ulid,
                action: Action::CreateSecrets,
            }),
        )?;

        // Get endpoint info
        let mut endpoint_host_url: String = String::new();
        let mut endpoint_s3_url: String = String::new();
        for endpoint_config in endpoint.host_config.0 .0 {
            match endpoint_config.feature {
                DataProxyFeature::GRPC => endpoint_host_url = endpoint_config.url,
                DataProxyFeature::S3 => endpoint_s3_url = endpoint_config.url,
            }
            if !endpoint_s3_url.is_empty() && !endpoint_host_url.is_empty() {
                break;
            }
        }
        // Check if dataproxy host url uses tls
        let dp_endpoint = if endpoint_host_url.starts_with("https") {
            Channel::from_shared(endpoint_host_url)
                .map_err(|_| Status::internal("Could not connect to Dataproxy"))?
                .tls_config(ClientTlsConfig::new())
                .map_err(|_| Status::internal("Could not connect to Dataproxy"))?
        } else {
            Channel::from_shared(endpoint_host_url)
                .map_err(|_| Status::internal("Could not connect to Dataproxy"))?
        };

        // Create dataproxy client
        let mut dp_conn = DataproxyUserServiceClient::connect(dp_endpoint).await?;

        // Create GetCredentialsRequest with slt in header ...
        let mut credentials_request = Request::new(GetCredentialsRequest {});
        credentials_request.metadata_mut().append(
            AsciiMetadataKey::from_bytes("Authorization".as_bytes())?,
            AsciiMetadataValue::try_from(format!("Bearer {short_lived_token}"))?,
        );

        // Collect results
        let response = dp_conn
            .get_credentials(credentials_request)
            .await?
            .into_inner();
        Ok((
            response.access_key,
            response.secret_key,
            endpoint_s3_url.to_string(),
        ))
    }

    pub async fn delete_s3_credentials_with_user_token(
        &self,
        user_id: DieselUlid,
        endpoint_id: String,
        token_id: Option<DieselUlid>,
        token_handler: &TokenHandler,
    ) -> Result<()> {
        let endpoint_ulid = DieselUlid::from_str(&endpoint_id)?;
        let endpoint = self
            .get_endpoint(GetEP(GetEndpointRequest {
                endpoint: Some(Endpoint::EndpointId(endpoint_id)),
            }))
            .await?;

        // Create slt for proxy interaction
        let short_lived_token = token_handler.sign_dataproxy_slt(
            &user_id,
            token_id.map(|t| t.to_string()),
            Some(Intent {
                target: endpoint_ulid,
                action: Action::CreateSecrets,
            }),
        )?;

        // Collect endpoint info
        let mut endpoint_host_url: String = String::new();
        let mut endpoint_s3_url: String = String::new();
        for endpoint_config in endpoint.host_config.0 .0 {
            match endpoint_config.feature {
                DataProxyFeature::GRPC => endpoint_host_url = endpoint_config.url,
                DataProxyFeature::S3 => endpoint_s3_url = endpoint_config.url,
            }
            if !endpoint_s3_url.is_empty() && !endpoint_host_url.is_empty() {
                break;
            }
        }

        // Check if dataproxy host url is tls
        let dp_endpoint = if endpoint_host_url.starts_with("https") {
            Channel::from_shared(endpoint_host_url)
                .map_err(|_| Status::internal("Could not connect to Dataproxy"))?
                .tls_config(ClientTlsConfig::new())
                .map_err(|_| Status::internal("Could not connect to Dataproxy"))?
        } else {
            Channel::from_shared(endpoint_host_url)
                .map_err(|_| Status::internal("Could not connect to Dataproxy"))?
        };

        // Create dataproxy client
        let mut dp_conn = DataproxyUserServiceClient::connect(dp_endpoint).await?;

        // Create GetCredentialsRequest with slt in header ...
        let mut credentials_request = Request::new(RevokeCredentialsRequest {});
        credentials_request.metadata_mut().append(
            AsciiMetadataKey::from_bytes("Authorization".as_bytes())?,
            AsciiMetadataValue::try_from(format!("Bearer {short_lived_token}"))?,
        );

        dp_conn.revoke_credentials(credentials_request).await?;
        Ok(())
    }

    pub async fn add_pubkey_to_user(&self, pubkey: String, user_id: DieselUlid) -> Result<User> {
        let client = self.database.get_client().await?;
        let user = User::add_pubkey(&pubkey, &user_id, &client).await?;
        self.cache.update_user(&user_id, user.clone());

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
        Ok(user)
    }

    pub async fn add_data_proxy_attribute(
        &self,
        request: AddDataProxyAttributeUserRequest,
        proxy_id: DieselUlid,
    ) -> Result<()> {
        let attribute: DataProxyAttribute = request
            .attribute
            .ok_or_else(|| anyhow!("No attribute provided"))?
            .try_into()?;
        let user_id = DieselUlid::from_str(&request.user_id)?;
        // Check if proxy is setting attribute for itself
        if attribute.proxy_id != proxy_id {
            return Err(anyhow!("Unauthorized"));
        }
        let client = self.database.get_client().await?;
        // Check if proxy is allowed to add attribute
        let user = User::get(user_id, &client)
            .await?
            .ok_or_else(|| anyhow!("User not found"))?;
        if !user.attributes.0.trusted_endpoints.contains_key(&proxy_id) {
            Err(anyhow!("Unauthorized"))
        } else {
            let user = User::add_data_proxy_attribute(&client, attribute, &user_id).await?;
            self.cache.update_user(&user_id, user.clone());

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
    pub async fn rm_data_proxy_attribute(
        &self,
        request: RemoveDataProxyAttributeUserRequest,
        source: DeleteProxyAttributeSource,
    ) -> Result<()> {
        let client = self.database.get_client().await?;
        let user = match source {
            DeleteProxyAttributeSource::Proxy(proxy_id) => {
                let user_id = DieselUlid::from_str(&request.user_id)?;
                let user = User::get(user_id, &client)
                    .await?
                    .ok_or_else(|| anyhow!("User not found"))?;
                if !user.attributes.0.trusted_endpoints.contains_key(&proxy_id) {
                    return Err(anyhow!("Unauthorized"));
                }
                user
            }
            DeleteProxyAttributeSource::User(user_id) => User::get(user_id, &client)
                .await?
                .ok_or_else(|| anyhow!("User not found"))?,
        };
        let attribute = user
            .attributes
            .0
            .data_proxy_attribute
            .into_iter()
            .find(|a| {
                a.proxy_id.to_string() == request.dataproxy_id
                    && a.attribute_name == request.attribute_name
            })
            .ok_or_else(|| anyhow!("Attribute not found"))?;
        let user = User::rm_data_proxy_attribute(&client, attribute, &user.id).await?;
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
