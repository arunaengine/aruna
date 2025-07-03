use crate::database::crud::CrudDb;
use crate::database::dsls::hook_dsl::{
    Filter, Hook, HookStatusValues, HookStatusVariant, HookWithAssociatedProject, TriggerVariant,
};
use crate::database::dsls::object_dsl::{KeyValue, KeyValueVariant};
use crate::database::dsls::object_dsl::{Object, ObjectWithRelations};
use crate::database::enums::ObjectMapping;
use crate::hooks::hook_handler::HookMessage;
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::hooks_request_types::{Callback, CreateHook};
use anyhow::{anyhow, Result};

use crate::middlelayer::hooks_request_types::ListBy;
use aruna_rust_api::api::hooks::services::v2::AddProjectsToHookRequest;
use diesel_ulid::DieselUlid;
use postgres_types::Json;
use regex::Regex;
use std::str::FromStr;

impl DatabaseHandler {
    pub async fn create_hook(&self, request: CreateHook, user_id: &DieselUlid) -> Result<Hook> {
        let client = self.database.get_client().await?;
        let mut hook = request.get_hook(user_id)?;
        hook.create(&client).await?;
        Ok(hook)
    }
    pub async fn list_hook(&self, request: ListBy) -> Result<Vec<Hook>> {
        let client = self.database.get_client().await?;
        let hooks = match request {
            ListBy::PROJECT(_) => {
                let project_id = request.get_id()?;

                Hook::list_hooks(&project_id, &client).await?
            }
            ListBy::OWNER(id) => Hook::list_owned(&id, &client).await?,
        };
        Ok(hooks)
    }
    pub async fn delete_hook(&self, hook_id: DieselUlid) -> Result<()> {
        let client = self.database.get_client().await?;
        Hook::delete_by_id(&hook_id, &client).await?;
        Ok(())
    }
    pub async fn get_project_by_hook(&self, hook_id: &DieselUlid) -> Result<Vec<DieselUlid>> {
        let client = self.database.get_client().await?;
        let project_ids = Hook::get_project_from_hook(hook_id, &client).await?;
        Ok(project_ids)
    }

    pub async fn append_project_to_hook(
        &self,
        request: AddProjectsToHookRequest,
        user_id: &DieselUlid,
    ) -> Result<()> {
        let hook_id = DieselUlid::from_str(&request.hook_id)?;
        let client = self.database.get_client().await?;
        let hook = Hook::get(hook_id, &client)
            .await?
            .ok_or_else(|| anyhow!("Hook not found"))?;
        if hook.owner != *user_id {
            return Err(anyhow!("User is not allowed to add projects to hook"));
        }
        let projects = request
            .project_ids
            .iter()
            .map(|id| DieselUlid::from_str(id).map_err(|_| anyhow!("Invalid project id")))
            .collect::<Result<Vec<_>>>()?;
        Hook::add_projects_to_hook(&projects, &hook_id, &client).await?;
        Ok(())
    }
    pub async fn hook_callback(&self, request: Callback) -> Result<()> {
        // Parsing
        let (_, object_id) = request.get_ids()?;

        // Client creation
        let mut client = self.database.get_client().await?;

        let object = Object::get(object_id, &client)
            .await?
            .ok_or_else(|| anyhow!("Object not found"))?;

        let status = object
            .key_values
            .0
             .0
            .iter()
            .find(|kv| kv.key == request.0.hook_id)
            .ok_or_else(|| anyhow!("Hook status not found"))?
            .clone();
        let mut value: HookStatusValues = serde_json::from_str(&status.value)?;
        let transaction = client.transaction().await?;
        let transaction_client = transaction.client();

        match request.0.status {
            Some(
                aruna_rust_api::api::hooks::services::v2::hook_callback_request::Status::Finished(
                    req,
                ),
            ) => {
                let (add, rm) = Callback::get_keyvals(req)?;
                // Adding kvs from callback
                if !add.0.is_empty() {
                    for kv in add.0 {
                        Object::add_key_value(&object_id, transaction_client, kv).await?;
                    }
                }
                // Removing kvs from callback
                if !rm.0.is_empty() {
                    for kv in rm.0 {
                        if kv.variant != KeyValueVariant::STATIC_LABEL {
                            object.remove_key_value(transaction_client, kv).await?;
                        } else {
                            return Err(anyhow!("Cannot remove static labels."));
                        }
                    }
                }

                value.status = HookStatusVariant::FINISHED;
            }
            Some(
                aruna_rust_api::api::hooks::services::v2::hook_callback_request::Status::Error(
                    aruna_rust_api::api::hooks::services::v2::Error { error },
                ),
            ) => {
                value.status = HookStatusVariant::ERROR(error);
            }
            None => return Err(anyhow!("No status provided")),
        };
        let kvs = if let Err(e) = self
            .evaluate_rules(&vec![object_id], transaction_client)
            .await
        {
            transaction.rollback().await?;
            if let Some(hook) =
                Hook::get(DieselUlid::from_str(&request.0.hook_id)?, &client).await?
            {
                let value = HookStatusValues {
                    name: hook.name,
                    status: HookStatusVariant::ERROR(e.to_string()),
                    trigger: hook.trigger.0,
                };
                let kv = KeyValue {
                    key: request.0.hook_id,
                    value: serde_json::to_string(&value)?,
                    variant: KeyValueVariant::HOOK_STATUS,
                };
                // Update status
                Object::add_key_value(&object_id, &client, kv.clone()).await?;
                vec![kv]
            } else {
                return Err(anyhow!("Hook not found"));
            }
        } else {
            let mut owr = Object::get_object_with_relations(&object_id, transaction_client).await?;
            // Update status
            let kvs = owr
                .object
                .key_values
                .0
                 .0
                .iter()
                .map(|kv| -> Result<KeyValue> {
                    if kv.key == request.0.hook_id {
                        let value = serde_json::to_string(&value)?;
                        Ok(KeyValue {
                            key: kv.key.clone(),
                            value,
                            variant: KeyValueVariant::HOOK_STATUS,
                        })
                    } else {
                        Ok(kv.clone())
                    }
                })
                .collect::<Result<Vec<KeyValue>>>()?;
            owr.object.key_values = Json(crate::database::dsls::object_dsl::KeyValues(kvs.clone()));
            owr.object.update(transaction_client).await?;

            transaction.commit().await?;
            kvs
        };
        // Update object in cache
        let owr = Object::get_object_with_relations(&object_id, &client).await?;
        self.cache.upsert_object(&object_id, owr.clone());

        // Send HookStatusChanged trigger to hook handler
        let db_handler = DatabaseHandler {
            database: self.database.clone(),
            natsio_handler: self.natsio_handler.clone(),
            cache: self.cache.clone(),
            hook_sender: self.hook_sender.clone(),
        };
        // TODO!
        // Because we cannot define which project triggered this hooks callback,
        // we also cannot define the hook_owner.
        //let user_id = hook.owner; // This is a temporary solution
        tokio::spawn(async move {
            let call = db_handler
                .trigger_hooks(owr, vec![TriggerVariant::HOOK_STATUS_CHANGED], Some(kvs))
                .await;
            if call.is_err() {
                log::error!("{call:?}");
            }
        });

        Ok(())
    }
    fn collect_projects(parents: Vec<Vec<ObjectMapping<DieselUlid>>>) -> Vec<DieselUlid> {
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
        projects
    }
    pub async fn trigger_hooks(
        &self,
        object: ObjectWithRelations,
        //user_id: DieselUlid,
        triggers: Vec<TriggerVariant>,
        updated_labels: Option<Vec<KeyValue>>,
    ) -> Result<()> {
        let client = self.database.get_client().await?;
        let parents = self.cache.upstream_dfs_iterative(&object.object.id)?;
        let projects = DatabaseHandler::collect_projects(parents);
        let labels = if let Some(labels) = &updated_labels {
            labels
        } else {
            &object.object.key_values.0 .0
        };

        // Get hooks that are associated with triggered-object parent-projects
        let hooks: Vec<HookWithAssociatedProject> = {
            let mut hooks = Vec::new();
            // Filter through hooks
            for h in Hook::get_hooks_for_projects(&projects, &client).await? {
                // Only get hooks that are triggered
                if triggers.contains(&h.trigger.0.variant) {
                    let mut is_match = false;
                    // Only get hooks that are matched by filter
                    for filter in h.trigger.0.filter.clone() {
                        match filter {
                            Filter::Name(name) => {
                                let regex = if let Ok(regex) = Regex::new(&name) {
                                    regex
                                } else {
                                    continue;
                                };
                                if regex.is_match(&object.object.name) {
                                    is_match = true;
                                    break;
                                }
                                continue;
                            }
                            Filter::KeyValue(KeyValue {
                                key,
                                value,
                                variant,
                            }) => {
                                let key_regex = if let Ok(regex) = Regex::new(&key) {
                                    regex
                                } else {
                                    continue;
                                };
                                let value_regex = if let Ok(regex) = Regex::new(&value) {
                                    regex
                                } else {
                                    continue;
                                };
                                for label in labels {
                                    if (label.variant == variant)
                                        && (key_regex.is_match(&label.key))
                                        && (value_regex.is_match(&label.value))
                                    {
                                        is_match = true;
                                        break;
                                    }
                                    continue;
                                }
                            }
                        }
                    }
                    if is_match {
                        hooks.push(h)
                    }
                }
            }
            hooks
        };
        if hooks.is_empty() {
            Ok(())
        } else {
            for hook in hooks {
                let user_id = match self.cache.get_object(&hook.project_id) {
                    Some(project) => project.object.created_by,
                    None => return Ok(()),
                };
                let message = HookMessage {
                    hook,
                    object: object.clone(),
                    user_id,
                };
                self.hook_sender.send(message).await?;
            }
            Ok(())
        }
    }
}
