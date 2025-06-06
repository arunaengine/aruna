use crate::caching::cache::Cache;
use crate::structs::{Object, ObjectLocation, ObjectType};
use anyhow::Result;
use aruna_rust_api::api::storage::models::v2::DataClass;
use base64::engine::general_purpose;
use base64::Engine;
use chrono::NaiveDateTime;
use diesel_ulid::DieselUlid;
use s3s::s3_error;
use std::collections::BTreeSet;
use std::sync::Arc;

#[derive(Debug, Eq, PartialEq, Hash, Clone, PartialOrd, Ord)]
pub struct Contents {
    pub key: String,
    pub etag: DieselUlid,
    pub size: i64,
    pub storage_class: DataClass,
    pub created_at: Option<NaiveDateTime>,
}
impl From<(&String, &(Object, Option<ObjectLocation>))> for Contents {
    #[tracing::instrument(level = "trace", skip(value))]
    fn from(value: (&String, &(Object, Option<ObjectLocation>))) -> Self {
        Contents {
            key: value.0.clone(),
            etag: value.1 .0.id,
            size: match &value.1 .1 {
                Some(s) => s.raw_content_len,
                None => 0,
            },
            storage_class: value.1 .0.data_class,
            created_at: value.1 .0.created_at,
        }
    }
}

#[tracing::instrument(level = "trace", skip(cache, delimiter, prefix, start_at, max_keys))]
pub async fn list_response(
    cache: &Arc<Cache>,
    delimiter: &Option<String>,
    prefix: &Option<String>,
    bucket_name: &str,
    start_at: &str,
    max_keys: usize,
    v1: bool, // If true, returns raw path marker instead of continuation token
) -> Result<(BTreeSet<Contents>, BTreeSet<String>, Option<String>)> {
    let mut keys: BTreeSet<Contents> = BTreeSet::default();
    let mut common_prefixes: BTreeSet<String> = BTreeSet::default();
    let mut new_continuation_token: Option<String> = None;

    match (delimiter.clone(), prefix.clone()) {
        (Some(delimiter), Some(prefix)) => {
            for (path, id) in cache.get_path_range(bucket_name, start_at) {
                // Breaks with next path to start at after max_keys is reached
                let num_keys = keys.len() + common_prefixes.len();
                if num_keys == max_keys {
                    new_continuation_token = if v1 {
                        Some(path)
                    } else {
                        Some(general_purpose::STANDARD_NO_PAD.encode(path))
                    };
                    break;
                }

                if let Some(stripped_path) = path.strip_prefix(&prefix) {
                    if let Some((common_prefix, _)) = stripped_path.split_once(&delimiter) {
                        common_prefixes.insert(format!(
                            "{}{}",
                            [prefix.to_string(), common_prefix.to_string()].join(""),
                            delimiter
                        ));
                    } else {
                        let object_with_location = cache
                            .get_resource_cloned(&id, false)
                            .await
                            .map_err(|_| s3_error!(NoSuchKey, "No key found for path"))?;

                        if object_with_location.0.object_type != ObjectType::Object {
                            continue;
                        }
                        keys.insert((&path, &object_with_location).into());
                    };
                } else {
                    continue;
                };
            }
        }
        (Some(delimiter), None) => {
            for (path, id) in cache.get_path_range(bucket_name, start_at) {
                // Breaks with next path to start at after max_keys is reached
                let num_keys = keys.len() + common_prefixes.len();
                if num_keys == max_keys {
                    new_continuation_token = if v1 {
                        Some(path)
                    } else {
                        Some(general_purpose::STANDARD_NO_PAD.encode(path))
                    };
                    break;
                }

                if let Some((common_prefix, _)) = path.split_once(&delimiter) {
                    // Collect common prefix with delimiter at its end
                    common_prefixes.insert(format!("{}{}", common_prefix, delimiter));
                } else {
                    // If None split -> Entry
                    let object_with_location = cache
                        .get_resource_cloned(&id, false)
                        .await
                        .map_err(|_| s3_error!(NoSuchKey, "No key found for path"))?;

                    if object_with_location.0.object_type != ObjectType::Object {
                        continue;
                    }
                    keys.insert((&path, &object_with_location).into());
                };
            }
        }
        (None, Some(prefix)) => {
            for (path, id) in cache.get_path_range(bucket_name, start_at) {
                // Breaks with next path to start at after max_keys is reached
                let num_keys = keys.len() + common_prefixes.len();
                if num_keys == max_keys {
                    new_continuation_token = if v1 {
                        Some(path)
                    } else {
                        Some(general_purpose::STANDARD_NO_PAD.encode(path))
                    };
                    break;
                }

                if path.strip_prefix(&prefix).is_some() {
                    let object_with_location = cache
                        .get_resource_cloned(&id, false)
                        .await
                        .map_err(|_| s3_error!(NoSuchKey, "No key found for path"))?;

                    if object_with_location.0.object_type != ObjectType::Object {
                        continue;
                    }
                    keys.insert((&path, &object_with_location).into());
                } else {
                    continue;
                };
            }
        }
        (None, None) => {
            for (path, id) in cache.get_path_range(bucket_name, start_at) {
                // Breaks with next path to start at after max_keys is reached
                let num_keys = keys.len() + common_prefixes.len();
                if num_keys == max_keys {
                    new_continuation_token = if v1 {
                        Some(path)
                    } else {
                        Some(general_purpose::STANDARD_NO_PAD.encode(path))
                    };
                    break;
                }

                let object_with_location = cache
                    .get_resource_cloned(&id, false)
                    .await
                    .map_err(|_| s3_error!(NoSuchKey, "No key found for path"))?;

                if object_with_location.0.object_type != ObjectType::Object {
                    continue;
                }
                keys.insert((&path, &object_with_location).into());
            }
        }
    }

    Ok((keys, common_prefixes, new_continuation_token))
}
