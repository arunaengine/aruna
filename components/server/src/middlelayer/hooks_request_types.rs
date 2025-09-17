use crate::auth::permission_handler::PermissionHandler;
use crate::caching::cache::Cache;
use crate::database::dsls::hook_dsl::{
    ExternalHook, Filter, Hook, InternalHook, TemplateVariant, Trigger, TriggerVariant,
};
use crate::database::dsls::object_dsl::{KeyValue, KeyValueVariant, KeyValues, Object};
use crate::database::enums::{DataClass, ObjectStatus};
use anyhow::{anyhow, Result};
use aruna_rust_api::api::dataproxy::services::v2::GetCredentialsResponse;
use aruna_rust_api::api::hooks::services::v2::{
    hook::HookType, CreateHookRequest, Hook as APIHook,
};
use aruna_rust_api::api::hooks::services::v2::{internal_hook::InternalAction, AddHook, AddLabel};
use aruna_rust_api::api::hooks::services::v2::{
    HookCallbackRequest, ListProjectHooksRequest, Method,
};
use chrono::{DateTime, NaiveDateTime};
use diesel_ulid::DieselUlid;
use postgres_types::Json;
use regex::{Regex, RegexSet};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;

pub struct CreateHook(pub CreateHookRequest);

pub enum ListBy {
    PROJECT(ListProjectHooksRequest),
    OWNER(DieselUlid),
}

impl ListBy {
    pub fn get_id(&self) -> Result<DieselUlid> {
        Ok(match self {
            Self::PROJECT(req) => DieselUlid::from_str(&req.project_id)?,
            Self::OWNER(id) => *id,
        })
    }
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct Callback(pub HookCallbackRequest);

impl CreateHook {
    fn get_trigger(&self) -> Result<Trigger> {
        match self.0.trigger.clone() {
            Some(trigger) => match trigger.trigger_type() {
                aruna_rust_api::api::hooks::services::v2::TriggerType::HookAdded => Ok(Trigger {
                    variant: TriggerVariant::HOOK_ADDED,
                    filter: trigger
                        .filters
                        .into_iter()
                        .map(|f| f.try_into())
                        .collect::<Result<Vec<Filter>>>()?,
                }),
                aruna_rust_api::api::hooks::services::v2::TriggerType::ResourceCreated => {
                    Ok(Trigger {
                        variant: TriggerVariant::RESOURCE_CREATED,
                        filter: trigger
                            .filters
                            .into_iter()
                            .map(|f| f.try_into())
                            .collect::<Result<Vec<Filter>>>()?,
                    })
                }
                aruna_rust_api::api::hooks::services::v2::TriggerType::LabelAdded => Ok(Trigger {
                    variant: TriggerVariant::LABEL_ADDED,
                    filter: trigger
                        .filters
                        .into_iter()
                        .map(|f| f.try_into())
                        .collect::<Result<Vec<Filter>>>()?,
                }),
                aruna_rust_api::api::hooks::services::v2::TriggerType::StaticLabelAdded => {
                    Ok(Trigger {
                        variant: TriggerVariant::STATIC_LABEL_ADDED,
                        filter: trigger
                            .filters
                            .into_iter()
                            .map(|f| f.try_into())
                            .collect::<Result<Vec<Filter>>>()?,
                    })
                }
                aruna_rust_api::api::hooks::services::v2::TriggerType::HookStatusChanged => {
                    Ok(Trigger {
                        variant: TriggerVariant::HOOK_STATUS_CHANGED,
                        filter: trigger
                            .filters
                            .into_iter()
                            .map(|f| f.try_into())
                            .collect::<Result<Vec<Filter>>>()?,
                    })
                }
                aruna_rust_api::api::hooks::services::v2::TriggerType::ObjectFinished => {
                    Ok(Trigger {
                        variant: TriggerVariant::OBJECT_FINISHED,
                        filter: trigger
                            .filters
                            .into_iter()
                            .map(|f| f.try_into())
                            .collect::<Result<Vec<Filter>>>()?,
                    })
                }
                _ => Err(anyhow!("Invalid trigger type")),
            },
            None => Err(anyhow!("No trigger defined")),
        }
    }
    fn get_timeout(&self) -> Result<NaiveDateTime> {
        let time = self.0.timeout;
        DateTime::from_timestamp_millis(time.try_into()?)
            .map(|e| e.naive_utc())
            .ok_or_else(|| anyhow!("Invalid timeout provided"))
    }
    pub fn get_project_ids(&self) -> Result<Vec<DieselUlid>> {
        self.0
            .project_ids
            .iter()
            .map(|id| DieselUlid::from_str(id).map_err(|_| anyhow!("Invalid id")))
            .collect::<Result<Vec<DieselUlid>>>()
    }

    pub fn get_hook(&self, user_id: &DieselUlid) -> Result<Hook> {
        match &self.0.hook {
            Some(APIHook {
                hook_type: Some(HookType::ExternalHook(external_hook)),
            }) => {
                let trigger = Json(self.get_trigger()?);
                Ok(Hook {
                    id: DieselUlid::generate(),
                    name: self.0.name.clone(),
                    description: self.0.description.clone(),
                    project_ids: self.get_project_ids()?,
                    owner: *user_id,
                    trigger,
                    timeout: self.get_timeout()?,
                    hook: Json(crate::database::dsls::hook_dsl::HookVariant::External(
                        ExternalHook {
                            url: external_hook.url.clone(),
                            credentials: external_hook.credentials.clone().map(|c| {
                                crate::database::dsls::hook_dsl::Credentials { token: c.token }
                            }),
                            template: match &external_hook.custom_template {
                                Some(t) => {
                                    if !CreateHook::verify_template(t.clone())? {
                                        return Err(anyhow!("Invalid template"));
                                    };
                                    TemplateVariant::Custom(t.clone())
                                }
                                None => TemplateVariant::Basic,
                            },
                            method: match external_hook.method() {
                                Method::Unspecified => {
                                    return Err(anyhow!("Unspecified external hook reply method"))
                                }
                                Method::Put => crate::database::dsls::hook_dsl::Method::PUT,
                                Method::Post => crate::database::dsls::hook_dsl::Method::POST,
                            },
                        },
                    )),
                })
            }
            Some(APIHook {
                hook_type: Some(HookType::InternalHook(internal_hook)),
            }) => {
                let trigger = Json(self.get_trigger()?);
                let internal_hook = match &internal_hook.internal_action {
                    Some(InternalAction::AddLabel(AddLabel { key, value })) => {
                        InternalHook::AddHook {
                            key: key.clone(),
                            value: value.clone(),
                        }
                    }
                    Some(InternalAction::AddHook(AddHook { key, value })) => {
                        InternalHook::AddLabel {
                            key: key.clone(),
                            value: value.clone(),
                        }
                    }
                    Some(InternalAction::AddRelation(relation)) => InternalHook::CreateRelation {
                        relation: relation
                            .relation
                            .clone()
                            .ok_or_else(|| anyhow!("No relation provided"))?,
                    },
                    _ => return Err(anyhow!("Invalid internal action")),
                };
                Ok(Hook {
                    id: DieselUlid::generate(),
                    name: self.0.name.clone(),
                    description: self.0.description.to_string(),
                    project_ids: self.get_project_ids()?,
                    owner: *user_id,
                    trigger,
                    timeout: self.get_timeout()?,
                    hook: Json(crate::database::dsls::hook_dsl::HookVariant::Internal(
                        internal_hook,
                    )),
                })
            }
            _ => Err(anyhow!("Invalid hook provided")),
        }
    }

    fn verify_template(input: String) -> Result<bool> {
        let re = RegexSet::new([
            r"\{\{secret\}\}",
            r"\{\{object_id\}\}",
            r"\{\{hook_id\}\}",
            r"\{\{pubkey_serial\}\}",
        ])?;

        let matches: Vec<_> = re.matches(&input).into_iter().collect();

        if matches.len() == re.len() {
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

impl Callback {
    pub fn get_keyvals(
        aruna_rust_api::api::hooks::services::v2::Finished {
            add_key_values,
            remove_key_values,
        }: aruna_rust_api::api::hooks::services::v2::Finished,
    ) -> Result<(KeyValues, KeyValues)> {
        let add = add_key_values
            .clone()
            .into_iter()
            .map(|kv| -> Result<KeyValue> {
                let variant = match kv.variant() {
                    aruna_rust_api::api::storage::models::v2::KeyValueVariant::Unspecified => {
                        return Err(anyhow!("Unspecified KeyValueVariant"))
                    }
                    aruna_rust_api::api::storage::models::v2::KeyValueVariant::Label => {
                        KeyValueVariant::LABEL
                    }
                    aruna_rust_api::api::storage::models::v2::KeyValueVariant::StaticLabel => {
                        KeyValueVariant::STATIC_LABEL
                    }
                    aruna_rust_api::api::storage::models::v2::KeyValueVariant::Hook => {
                        KeyValueVariant::HOOK
                    }
                    aruna_rust_api::api::storage::models::v2::KeyValueVariant::HookStatus => {
                        KeyValueVariant::HOOK_STATUS
                    }
                };
                Ok(KeyValue {
                    key: kv.key,
                    value: kv.value,
                    variant,
                })
            })
            .collect::<Result<Vec<KeyValue>>>()?;
        let rm = remove_key_values
            .clone()
            .into_iter()
            .map(|kv| -> Result<KeyValue> {
                let variant = match kv.variant() {
                    aruna_rust_api::api::storage::models::v2::KeyValueVariant::Unspecified => {
                        return Err(anyhow!("Unspecified KeyValueVariant"))
                    }
                    aruna_rust_api::api::storage::models::v2::KeyValueVariant::Label => {
                        KeyValueVariant::LABEL
                    }
                    aruna_rust_api::api::storage::models::v2::KeyValueVariant::StaticLabel => {
                        KeyValueVariant::STATIC_LABEL
                    }
                    aruna_rust_api::api::storage::models::v2::KeyValueVariant::Hook => {
                        KeyValueVariant::HOOK
                    }
                    aruna_rust_api::api::storage::models::v2::KeyValueVariant::HookStatus => {
                        KeyValueVariant::HOOK_STATUS
                    }
                };
                Ok(KeyValue {
                    key: kv.key,
                    value: kv.value,
                    variant,
                })
            })
            .collect::<Result<Vec<KeyValue>>>()?;
        Ok((KeyValues(add.to_owned()), KeyValues(rm.to_owned())))
    }

    pub fn verify_secret(
        &self,
        authorizer: Arc<PermissionHandler>,
        cache: Arc<Cache>,
    ) -> Result<()> {
        let (hook_id, object_id) = self.get_ids()?;
        let pubkey_serial = self.0.pubkey_serial;
        let secret = self.0.secret.clone();
        authorizer.token_handler.verify_hook_secret(
            cache.clone(),
            secret,
            object_id,
            hook_id,
            pubkey_serial.try_into()?,
        )?;
        Ok(())
    }

    pub fn get_ids(&self) -> Result<(DieselUlid, DieselUlid)> {
        Ok((
            DieselUlid::from_str(&self.0.hook_id)?,
            DieselUlid::from_str(&self.0.object_id)?,
        ))
    }
}

pub struct CustomTemplate {}

impl CustomTemplate {
    pub fn create_custom_template(
        input: String,
        hook_id: DieselUlid,
        object: &Object,
        secret: String,
        download_url: Option<String>,
        token: String,
        upload_credentials: GetCredentialsResponse,
        pubkey_serial: i32,
    ) -> Result<String> {
        let object_status = match object.object_status {
            ObjectStatus::ERROR => "ERROR".to_string(),
            ObjectStatus::INITIALIZING => "INITIALIZING".to_string(),
            ObjectStatus::VALIDATING => "VALIDATING".to_string(),
            ObjectStatus::AVAILABLE => "AVAILABLE".to_string(),
            ObjectStatus::UNAVAILABLE => "UNAVAILABLE".to_string(),
            ObjectStatus::DELETED => "DELETED".to_string(),
        };
        let data_class = match object.data_class {
            DataClass::PUBLIC => "PUBLIC".to_string(),
            DataClass::PRIVATE => "PRIVATE".to_string(),
            DataClass::WORKSPACE => "WORKSPACE".to_string(),
            DataClass::CONFIDENTIAL => "CONFIDENTIAL".to_string(),
        };
        let GetCredentialsResponse {
            access_key,
            secret_key,
        } = upload_credentials;
        let download_url = download_url.unwrap_or_default();
        let replacement_pairs = [
            (r"\{\{secret\}\}", secret),
            (r"\{\{object_id\}\}", object.id.to_string()),
            (r"\{\{hook_id\}\}", hook_id.to_string()),
            (r"\{\{pubkey_serial\}\}", pubkey_serial.to_string()),
            (r"\{\{name\}\}", object.name.clone()),
            (r"\{\{description\}\}", object.description.clone()),
            (r"\{\{size\}\}", object.content_len.to_string()),
            (
                r"\{\{key_values\}\}",
                // TODO: no json parsing here
                serde_json::to_string(&object.key_values.0)?,
            ),
            (r"\{\{status\}\}", object_status),
            (r"\{\{class\}\}", data_class),
            (
                r"\{\{endpoints\}\}",
                // TODO: Remove json parsing
                serde_json::to_string(&object.endpoints.0)?,
            ),
            (r"\{\{download_url\}\}", download_url),
            (r"\{\{token\}\}", token),
            (r"\{\{access_key\}\}", access_key),
            (r"\{\{secret_key\}\}", secret_key),
        ];

        let mut input = input.clone();
        for (regex, replacement) in replacement_pairs {
            let re = Regex::new(regex)?;
            input = re.replace(&input, replacement).to_string();
        }
        Ok(input)
    }
}
