use crate::caching::structs::ObjectWrapper;
use crate::database::dsls::hook_dsl::{
    BasicTemplate, Credentials, ExternalHook, TemplateVariant, TriggerVariant,
};
use crate::database::dsls::object_dsl::KeyValueVariant::HOOK_STATUS;
use crate::database::dsls::user_dsl::APIToken;
use crate::database::enums::{ObjectMapping, ObjectStatus, ObjectType};
use crate::middlelayer::hooks_request_types::CustomTemplate;
use crate::middlelayer::presigned_url_handler::PresignedDownload;
use crate::middlelayer::relations_request_types::ModifyRelations;
use crate::notification::handler::EventHandler;
use crate::{
    auth::permission_handler::PermissionHandler,
    database::dsls::{
        hook_dsl::{HookStatusValues, HookStatusVariant, HookWithAssociatedProject},
        object_dsl::{KeyValue, ObjectWithRelations},
    },
    middlelayer::db_handler::DatabaseHandler,
};
use anyhow::anyhow;
use anyhow::Result;
use aruna_rust_api::api::dataproxy::services::v2::GetCredentialsResponse;
use aruna_rust_api::api::storage::models::v2::{
    KeyValue as APIKeyVals, KeyValueVariant as APIKeyValVariant,
};
use aruna_rust_api::api::storage::services::v2::{
    GetDownloadUrlRequest, UpdateCollectionKeyValuesRequest, UpdateObjectRequest,
    UpdateProjectKeyValuesRequest,
};
use async_channel::Receiver;
use diesel_ulid::DieselUlid;
use reqwest::header::CONTENT_TYPE;
use std::sync::Arc;

#[derive(Clone)]
pub struct HookHandler {
    pub reciever: Receiver<HookMessage>,
    pub authorizer: Arc<PermissionHandler>,
    pub database_handler: Arc<DatabaseHandler>,
}

#[derive(Debug, Clone)]
pub struct HookMessage {
    pub hook: HookWithAssociatedProject,
    pub object: ObjectWithRelations,
    pub user_id: DieselUlid,
}

impl HookHandler {
    pub async fn new(
        reciever: Receiver<HookMessage>,
        authorizer: Arc<PermissionHandler>,
        database_handler: Arc<DatabaseHandler>,
    ) -> Self {
        HookHandler {
            reciever,
            authorizer,
            database_handler,
        }
    }
    pub async fn run(&self) -> Result<()> {
        let handler = self.clone();
        let client = reqwest::Client::new();
        tokio::spawn(async move {
            while let Ok(message) = handler.reciever.recv().await {
                // TODO:
                // - queue logic
                // - deduplication
                // - retries
                if let Err(action) = handler.hook_action(message, client.clone()).await {
                    log::error!("[HookHandler] ERROR: {action:?}");
                };
            }
        });
        Ok(())
    }
    pub async fn hook_action(&self, message: HookMessage, client: reqwest::Client) -> Result<()> {
        let HookMessage {
            hook,
            object,
            user_id,
        } = message;
        let object_id = object.object.id;

        // Add running status:
        self.add_or_replace_status(&hook, &object, HookStatusVariant::RUNNING)
            .await?;

        match hook.hook.0 {
            crate::database::dsls::hook_dsl::HookVariant::Internal(ref internal_hook) => {
                match internal_hook {
                    crate::database::dsls::hook_dsl::InternalHook::AddLabel { key, value } => {
                        if let Err(e) = self
                            .add_keyvals(
                                object.clone(),
                                user_id,
                                key.to_string(),
                                value.to_string(),
                                APIKeyValVariant::Label,
                            )
                            .await
                        {
                            self.add_or_replace_status(
                                &hook,
                                &object,
                                HookStatusVariant::ERROR(e.to_string()),
                            )
                            .await?;
                        } else {
                            // Add finished status
                            self.add_or_replace_status(&hook, &object, HookStatusVariant::FINISHED)
                                .await?;
                        }
                    }
                    crate::database::dsls::hook_dsl::InternalHook::AddHook { key, value } => {
                        if let Err(e) = self
                            .add_keyvals(
                                object.clone(),
                                user_id,
                                key.to_string(),
                                value.to_string(),
                                APIKeyValVariant::Hook,
                            )
                            .await
                        {
                            self.add_or_replace_status(
                                &hook,
                                &object,
                                HookStatusVariant::ERROR(e.to_string()),
                            )
                            .await?;
                        } else {
                            // Add finished status
                            self.add_or_replace_status(&hook, &object, HookStatusVariant::FINISHED)
                                .await?;
                        }
                    }
                    crate::database::dsls::hook_dsl::InternalHook::CreateRelation { relation } => {
                        let relation = aruna_rust_api::api::storage::models::v2::Relation {
                            relation: Some(relation.clone()),
                        };
                        let request = ModifyRelations(
                            aruna_rust_api::api::storage::services::v2::ModifyRelationsRequest {
                                resource_id: object_id.to_string(),
                                add_relations: vec![relation],
                                remove_relations: vec![],
                            },
                        );
                        let (resource, labels_info) =
                            self.database_handler.get_resource(request).await?;
                        if let Err(e) = self
                            .database_handler
                            .modify_relations(
                                resource,
                                labels_info.relations_to_add,
                                labels_info.relations_to_remove,
                            )
                            .await
                        {
                            self.add_or_replace_status(
                                &hook,
                                &object,
                                HookStatusVariant::ERROR(e.to_string()),
                            )
                            .await?;
                        } else {
                            self.add_or_replace_status(&hook, &object, HookStatusVariant::FINISHED)
                                .await?;
                        }
                    }
                }
            }
            crate::database::dsls::hook_dsl::HookVariant::External(ExternalHook {
                ref url,
                ref credentials,
                ref template,
                ref method,
            }) => {
                log::info!("[HookHandler] Starting external hook");
                // This creates only presigned download urls for available objects.
                // If ObjectType is not OBJECT, only s3 credentials are generated.
                // This should allow for generic external hooks that can also be
                // triggered for other ObjectTypes than OBJECTs
                let (secret, download, pubkey_serial, upload_credentials) = self
                    .get_template_input(object.clone(), hook.clone(), user_id)
                    .await?;

                // Create & send request
                let base_request = match method {
                    crate::database::dsls::hook_dsl::Method::PUT => match credentials {
                        Some(Credentials { token }) => client.put(url).bearer_auth(token),
                        None => client.put(url),
                    },
                    crate::database::dsls::hook_dsl::Method::POST => match credentials {
                        Some(Credentials { token }) => client.post(url).bearer_auth(token),
                        None => client.post(url),
                    },
                };
                // Query rule bindings from cache and build generic object
                let object_wrapper = ObjectWrapper {
                    object_with_relations: object.clone(),
                    rules: self
                        .database_handler
                        .cache
                        .get_rule_bindings(&object.object.id)
                        .unwrap_or_default(),
                };

                // Put everything into template
                let data_request = match template {
                    TemplateVariant::Basic => {
                        let input = BasicTemplate {
                            hook_id: hook.id,
                            object: object_wrapper.into(),
                            secret,
                            download,
                            pubkey_serial: pubkey_serial.into(),
                            access_key: Some(upload_credentials.access_key),
                            secret_key: Some(upload_credentials.secret_key),
                        };
                        base_request.json(&input)
                    }
                    TemplateVariant::Custom(template) => {
                        let template = CustomTemplate::create_custom_template(
                            template.to_string(),
                            hook.id,
                            &object.object,
                            secret,
                            download,
                            upload_credentials,
                            pubkey_serial.into(),
                        )?;
                        base_request
                            .header(CONTENT_TYPE, "text/plain")
                            .body(template)
                    }
                };
                if let Err(e) = data_request.send().await {
                    log::error!("External hook error: {e}");
                    self.add_or_replace_status(
                        &hook,
                        &object,
                        HookStatusVariant::ERROR(e.to_string()),
                    )
                    .await?;
                };
            }
        };
        Ok(())
    }

    async fn add_keyvals(
        &self,
        object: ObjectWithRelations,
        user_id: DieselUlid,
        key: String,
        value: String,
        variant: APIKeyValVariant,
    ) -> Result<()> {
        match object.object.object_type {
            ObjectType::PROJECT => {
                let request = crate::middlelayer::update_request_types::KeyValueUpdate::Project(
                    UpdateProjectKeyValuesRequest {
                        project_id: object.object.id.to_string(),
                        add_key_values: vec![APIKeyVals {
                            key,
                            value,
                            variant: variant as i32,
                        }],
                        remove_key_values: Vec::new(),
                    },
                );
                self.database_handler.update_keyvals(request).await?;
            }
            ObjectType::COLLECTION => {
                let request = crate::middlelayer::update_request_types::KeyValueUpdate::Collection(
                    UpdateCollectionKeyValuesRequest {
                        collection_id: object.object.id.to_string(),
                        add_key_values: vec![APIKeyVals {
                            key,
                            value,
                            variant: variant as i32,
                        }],
                        remove_key_values: Vec::new(),
                    },
                );
                self.database_handler.update_keyvals(request).await?;
            }
            ObjectType::DATASET => {
                let request = crate::middlelayer::update_request_types::KeyValueUpdate::Collection(
                    UpdateCollectionKeyValuesRequest {
                        collection_id: object.object.id.to_string(),
                        add_key_values: vec![APIKeyVals {
                            key,
                            value,
                            variant: variant as i32,
                        }],
                        remove_key_values: Vec::new(),
                    },
                );
                self.database_handler.update_keyvals(request).await?;
            }
            ObjectType::OBJECT => {
                let request = UpdateObjectRequest {
                    object_id: object.object.id.to_string(),
                    name: None,
                    description: None,
                    add_key_values: vec![APIKeyVals {
                        key,
                        value,
                        variant: APIKeyValVariant::Label as i32,
                    }],
                    remove_key_values: Vec::new(),
                    data_class: 0,
                    hashes: Vec::new(),
                    force_revision: false,
                    metadata_license_tag: None,
                    data_license_tag: None,
                    parent: None,
                };
                let is_service_account = self
                    .database_handler
                    .cache
                    .get_user(&user_id)
                    .ok_or_else(|| anyhow!("User not found"))?
                    .attributes
                    .0
                    .service_account;
                self.database_handler
                    .update_grpc_object(request, user_id, is_service_account)
                    .await?;
            }
        }
        Ok(())
    }

    async fn add_or_replace_status(
        &self,
        hook: &HookWithAssociatedProject,
        object: &ObjectWithRelations,
        status: HookStatusVariant,
    ) -> Result<()> {
        let client = self.database_handler.database.get_client().await?;
        let mut object = object.clone();
        let status_value = HookStatusValues {
            name: hook.name.clone(),
            status,
            trigger: hook.trigger.0.clone(),
        };
        let hook_status = KeyValue {
            key: hook.id.to_string(),
            value: serde_json::to_string(&status_value)?,
            variant: HOOK_STATUS,
        };
        if object
            .object
            .key_values
            .0
             .0
            .iter_mut()
            .find_map(|kv| {
                if (kv.key == hook.name) && (kv.variant == HOOK_STATUS) {
                    *kv = hook_status.clone();
                    Some(kv)
                } else {
                    None
                }
            })
            .is_none()
        {
            object.object.key_values.0 .0.push(hook_status.clone())
        }
        object.object.update(&client).await?;
        self.database_handler
            .cache
            .upsert_object(&object.object.id, object.clone());

        // Send HookStatusChanged trigger to self
        let db_handler = DatabaseHandler {
            database: self.database_handler.database.clone(),
            natsio_handler: self.database_handler.natsio_handler.clone(),
            cache: self.database_handler.cache.clone(),
            hook_sender: self.database_handler.hook_sender.clone(),
        };
        // TODO!
        // Because we cannot define which project triggered this hooks callback,
        // we also cannot define the hook_owner.
        tokio::spawn(async move {
            let call = db_handler
                .trigger_hooks(
                    object,
                    vec![TriggerVariant::HOOK_STATUS_CHANGED],
                    Some(vec![hook_status]),
                )
                .await;
            if call.is_err() {
                log::error!("{call:?}");
            }
        });
        Ok(())
    }

    async fn get_template_input(
        &self,
        object: ObjectWithRelations,
        hook: HookWithAssociatedProject,
        user_id: DieselUlid,
    ) -> Result<(String, Option<String>, i16, GetCredentialsResponse)> {
        let object_id = object.object.id;
        // This creates only presigned download urls for available objects.
        // If ObjectType is not OBJECT, only s3 credentials are generated.
        // This should allow for generic external hooks that can also be
        // triggered for other ObjectTypes than OBJECTs
        let (secret, download, pubkey_serial, upload_credentials) = {
            let (secret, pubkey_serial) = self
                .authorizer
                .token_handler
                .sign_hook_secret(self.database_handler.cache.clone(), object_id, hook.id)
                .await?;
            // Create append only s3-credentials
            let append_only_token = APIToken {
                pub_key: pubkey_serial.into(),
                name: format!("{}-append_only", hook.id),
                created_at: chrono::Utc::now().naive_utc(),
                expires_at: hook.timeout,
                // TODO: Custom resource permissions for hooks
                object_id: Some(ObjectMapping::PROJECT(hook.project_id)),
                user_rights: crate::database::enums::DbPermissionLevel::APPEND,
            };
            let token_id = self
                .database_handler
                .create_hook_token(&user_id, append_only_token)
                .await?;

            let associated_project = hook.project_id;

            let endpoint = self
                .database_handler
                .get_fullsync_endpoint(associated_project)
                .await?;

            self.database_handler
                .natsio_handler
                .wait_for_acknowledgement(&endpoint.id.to_string())
                .await?;
            dbg!("waited for ack");

            // Create download url for response
            let request = PresignedDownload(GetDownloadUrlRequest {
                object_id: object_id.to_string(),
            });
            let (download, upload_credentials) = self
                .database_handler
                .get_presigned_download_with_credentials(
                    self.authorizer.clone(),
                    request,
                    user_id,
                    Some(token_id),
                    hook.project_id,
                    endpoint,
                )
                .await?;
            let download = match (object.object.object_type, &object.object.object_status) {
                (ObjectType::OBJECT, ObjectStatus::AVAILABLE) => Some(download),
                _ => None,
            };
            (secret, download, pubkey_serial, upload_credentials)
        };
        Ok((secret, download, pubkey_serial, upload_credentials))
    }
}
