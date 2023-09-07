use crate::auth::permission_handler::PermissionHandler;
use crate::database::crud::CrudDb;
use crate::database::dsls::hook_dsl::{
    BasicTemplate, Credentials, ExternalHook, Hook, TemplateVariant, TriggerType,
};
use crate::database::dsls::internal_relation_dsl::InternalRelation;
use crate::database::dsls::object_dsl::{ExternalRelation, KeyValue, KeyValueVariant};
use crate::database::dsls::object_dsl::{Object, ObjectWithRelations};
use crate::database::enums::{ObjectMapping, ObjectStatus, ObjectType};
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::hooks_request_types::{Callback, CreateHook};
use crate::middlelayer::presigned_url_handler::PresignedDownload;
use crate::middlelayer::token_request_types::CreateToken;
use anyhow::{anyhow, Result};
use aruna_rust_api::api::hooks::services::v2::ListHooksRequest;
use aruna_rust_api::api::storage::models::v2::{Permission, PermissionLevel};
use aruna_rust_api::api::storage::services::v2::{CreateApiTokenRequest, GetDownloadUrlRequest};
use diesel_ulid::DieselUlid;
use std::str::FromStr;
use std::sync::Arc;

impl DatabaseHandler {
    pub async fn create_hook(&self, request: CreateHook) -> Result<Hook> {
        let client = self.database.get_client().await?;
        let mut hook = request.get_hook()?;
        hook.create(&client).await?;
        Ok(hook)
    }
    pub async fn list_hook(&self, request: ListHooksRequest) -> Result<Vec<Hook>> {
        let client = self.database.get_client().await?;
        let project_id = DieselUlid::from_str(&request.project_id)?;
        let hooks = Hook::list_hooks(&project_id, &client).await?;
        Ok(hooks)
    }
    pub async fn delete_hook(&self, hook_id: DieselUlid) -> Result<()> {
        let client = self.database.get_client().await?;
        Hook::delete_by_id(&hook_id, &client).await?;
        Ok(())
    }
    pub async fn get_project_by_hook(&self, hook_id: &DieselUlid) -> Result<DieselUlid> {
        let client = self.database.get_client().await?;
        let project_id = Hook::get_project_from_hook(hook_id, &client).await?;
        Ok(project_id)
    }
    pub async fn hook_callback(&self, request: Callback) -> Result<()> {
        let mut client = self.database.get_client().await?;
        let transaction = client.transaction().await?;
        let transaction_client = transaction.client();
        let (_, object_id) = request.get_ids()?;
        let (add_kvs, rm_kvs) = request.get_keyvals()?;
        if request.0.success {
            if !add_kvs.0.is_empty() {
                for kv in add_kvs.0 {
                    Object::add_key_value(&object_id, transaction_client, kv).await?;
                }
            }

            if !rm_kvs.0.is_empty() {
                let object = Object::get(object_id, transaction_client)
                    .await?
                    .ok_or(anyhow!("Dataset does not exist."))?;
                for kv in rm_kvs.0 {
                    if !(kv.variant == KeyValueVariant::STATIC_LABEL) {
                        object.remove_key_value(transaction_client, kv).await?;
                    } else {
                        return Err(anyhow!("Cannot remove static labels."));
                    }
                }
            }
            transaction.commit().await?;
            let owr = Object::get_object_with_relations(&object_id, &client).await?;
            self.cache.update_object(&object_id, owr);
        }
        Ok(())
    }

    pub async fn trigger_on_creation(
        &self,
        authorizer: Arc<PermissionHandler>,
        object_id: DieselUlid,
        user_id: DieselUlid,
    ) -> Result<Option<ObjectWithRelations>> {
        dbg!("Trigger creation triggered");
        let client = self.database.get_client().await?;
        dbg!(object_id);
        let parents = self.cache.upstream_dfs_iterative(&object_id)?;
        dbg!("THRESHOLD");
        let mut projects: Vec<DieselUlid> = Vec::new();
        for branch in parents {
            projects.append(
                &mut branch
                    .iter()
                    .filter_map(|parent| match parent {
                        ObjectMapping::PROJECT(id) => Some(*id),
                        _ => None,
                    })
                    .collect(),
            );
        }
        dbg!("Projects = {:?}", &projects);
        let hooks: Vec<Hook> = Hook::get_hooks_for_projects(&projects, &client)
            .await?
            .into_iter()
            .filter(|h| h.trigger_type == TriggerType::OBJECT_CREATED)
            .collect();

        if hooks.is_empty() {
            Ok(None)
        } else {
            let owr = self
                .hook_action(authorizer.clone(), hooks, object_id, user_id)
                .await?;
            Ok(Some(owr))
        }
    }

    pub async fn trigger_on_append_hook(
        &self,
        authorizer: Arc<PermissionHandler>,
        user_id: DieselUlid,
        object_id: DieselUlid,
        keyvals: Vec<KeyValue>,
    ) -> Result<Option<ObjectWithRelations>> {
        dbg!("Trigger on append triggered");
        let client = self.database.get_client().await?;
        let parents = self.cache.upstream_dfs_iterative(&object_id)?;
        dbg!(&parents);
        let mut projects: Vec<DieselUlid> = Vec::new();
        for branch in parents {
            projects.append(
                &mut branch
                    .iter()
                    .filter_map(|parent| match parent {
                        ObjectMapping::PROJECT(id) => Some(*id),
                        _ => None,
                    })
                    .collect(),
            );
        }
        dbg!("Projects = {:?}", &projects);
        let keyvals: Vec<(String, String)> =
            keyvals.into_iter().map(|k| (k.key, k.value)).collect();
        dbg!("KEYVALS: {:?}", &keyvals);
        let hooks: Vec<Hook> = Hook::get_hooks_for_projects(&projects, &client)
            .await?
            .into_iter()
            .filter_map(|h| {
                if h.trigger_type == TriggerType::HOOK_ADDED {
                    if keyvals.contains(&(h.trigger_key.clone(), h.trigger_value.clone())) {
                        Some(h)
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();
        dbg!("HOOKS: {:?}", &hooks);
        if hooks.is_empty() {
            dbg!("HOOKS EMPTY");
            Ok(None)
        } else {
            dbg!("STARTING HOOKS ACTION");
            let owr = self
                .hook_action(authorizer.clone(), hooks, object_id, user_id)
                .await?;
            Ok(Some(owr))
        }
    }

    async fn hook_action(
        &self,
        authorizer: Arc<PermissionHandler>,
        hooks: Vec<Hook>,
        object_id: DieselUlid,
        user_id: DieselUlid,
    ) -> Result<ObjectWithRelations> {
        let mut client = self.database.get_client().await?;
        let transaction = client.transaction().await?;
        let transaction_client = transaction.client();
        let mut affected_parents: Vec<DieselUlid> = Vec::new();
        for hook in hooks {
            dbg!("Hook: {:?}", &hook);
            dbg!("ObjectID: {:?}", &object_id);
            let affected_parent = match hook.hook.0 {
                crate::database::dsls::hook_dsl::HookVariant::Internal(internal_hook) => {
                    match internal_hook {
                        crate::database::dsls::hook_dsl::InternalHook::AddLabel { key, value } => {
                            Object::add_key_value(
                                &object_id,
                                transaction_client,
                                KeyValue {
                                    key,
                                    value,
                                    variant: KeyValueVariant::LABEL,
                                },
                            )
                            .await?;
                            None
                        }
                        crate::database::dsls::hook_dsl::InternalHook::AddHook { key, value } => {
                            Object::add_key_value(
                                &object_id,
                                transaction_client,
                                KeyValue {
                                    key,
                                    value,
                                    variant: KeyValueVariant::HOOK,
                                },
                            )
                            .await?;
                            None
                        }
                        crate::database::dsls::hook_dsl::InternalHook::CreateRelation {
                            relation,
                        } => {
                            match relation {
                                aruna_rust_api::api::storage::models::v2::relation::Relation::External(external) => {
                                    let relation: ExternalRelation = (&external).try_into()?;
                                    Object::add_external_relations(&object_id, transaction_client, vec![relation]).await?;
                                    None
                                },
                                aruna_rust_api::api::storage::models::v2::relation::Relation::Internal(internal) => {
                                    let affected_parent = Some(DieselUlid::from_str(&internal.resource_id)?);
                                    let mut internal = InternalRelation::from_api(&internal, object_id, self.cache.clone())?;
                                    internal.create(transaction_client).await?;
                                    affected_parent
                                },
                            }
                        }
                    }
                }
                crate::database::dsls::hook_dsl::HookVariant::External(ExternalHook{ url, credentials, template, method }) => {
                    dbg!("REACHED EXTERNAL TRIGGER");
                    // Get Object for response
                    let object = self.cache.get_object(&object_id).ok_or_else(|| anyhow!("Object not found"))?;
                    if object.object.object_type != ObjectType::OBJECT || object.object.object_status == ObjectStatus::INITIALIZING {
                        continue
                    }
                    // Create secret for callback
                    let (secret, pubkey_serial) = authorizer.token_handler.sign_hook_secret(self.cache.clone(), object_id, hook.id).await?;
                    // Create download url for response
                    let request = PresignedDownload(GetDownloadUrlRequest{ object_id: object_id.to_string()});
                    let download = self.get_presigned_download(self.cache.clone(), authorizer.clone(), request, user_id).await?;
                    // Create token for upload
                    let resource_id = match object.object.object_type {
                        crate::database::enums::ObjectType::OBJECT => Some(aruna_rust_api::api::storage::models::v2::permission::ResourceId::ObjectId(object_id.to_string())),
                        _ => return Err(anyhow!("Only hooks on objects are allowed"))};
                    let expiry: prost_wkt_types::Timestamp = hook.timeout.try_into()?;
                    let token_request = CreateToken(CreateApiTokenRequest{ 
                        name: "HookToken".to_string(), 
                        permission: Some(Permission{ 
                            permission_level: PermissionLevel::Append as i32, 
                            resource_id,
                        }), 
                        expires_at: Some(expiry.clone()) ,
                        }
                    );
                    let (token_id, token) = self.create_token(&user_id, pubkey_serial, token_request).await?;
                    // Update user with new created short-lived upload token
                    let user = self.cache.get_user(&user_id).ok_or_else(|| anyhow!("User not found"))?;
                    user.attributes.0.tokens.insert(token_id, token.clone());
                    self.cache.update_user(&user_id, user);

                    // Sign token
                    let upload_token = 
                        authorizer.token_handler.sign_user_token(
                            &user_id,
                            &token_id,
                            Some(expiry),
                        )?;
                    // Put everything into template
                    let template = match template {
                        TemplateVariant::BasicTemplate => 
                            BasicTemplate { 
                                hook_id: hook.id, 
                                object: object.try_into()?, 
                                secret,
                                download, 
                                upload_token,
                                pubkey_serial,
                            }
                    };
                    dbg!("TRIGGER EXTERNAL TEMPALTE: {:?}", &template);
                    // Create & send request
                    let client = reqwest::Client::new();
                    match method {
                        crate::database::dsls::hook_dsl::Method::PUT => {
                            match credentials {
                                Some(Credentials{token}) =>  {
                                    let response = client.put(url).bearer_auth(token).json(&serde_json::to_string(&template)?).send().await?;
                                    dbg!(&response);
                                },
                                None => { let response = client.put(url).json(&serde_json::to_string(&template)?).send().await?;
                                    dbg!(&response);
                                }
                            }
                        },
                        crate::database::dsls::hook_dsl::Method::POST => {
                            match credentials {
                                Some(Credentials{token}) =>  {
                                    let response = client.post(url).bearer_auth(token).json(&serde_json::to_string(&template)?).send().await?;
                                    dbg!(&response);
                                },
                                None => {let response = client.post(url).json(&serde_json::to_string(&template)?).send().await?;
                                    dbg!(&response);
                                }
                            }
                        }
                    }
                    None
                }
            };
            if let Some(p) = affected_parent {
                affected_parents.push(p);
            }
        }
        transaction.commit().await?;
        let updated = Object::get_object_with_relations(&object_id, &client).await?;
        if !affected_parents.is_empty() {
            let affected = Object::get_objects_with_relations(&affected_parents, &client).await?;
            for object in affected {
                self.cache.update_object(&object.object.id.clone(), object);
            }
        }
        Ok(updated)
    }
}