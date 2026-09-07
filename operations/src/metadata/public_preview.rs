use std::collections::BTreeSet;

use aruna_core::metadata::MetadataError;
use aruna_core::structs::{AuthContext, Permission, RealmId, blob_object_permission_path};
use aruna_core::types::GroupId;
use serde_json::Value as JsonValue;

use crate::blob::resolve_blob_permission_paths::ResolveBlobPermissionPathsOperation;
use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::driver::{DriverContext, drive};
use crate::jobs::export::{EntityIdentity, entity_identity};
use crate::request_policy::{
    PolicyEnforcementError, PolicyEvaluator, PolicyRequestExtras, policy_request_with,
};
use crate::s3::get_bucket_info::{GetBucketInfoError, GetBucketInfoOperation};

const FILE_TYPES: [&str; 4] = [
    "File",
    "MediaObject",
    "http://schema.org/MediaObject",
    "https://schema.org/MediaObject",
];
/// A draft past these bounds reports what was resolved rather than fanning out
/// further; the preview is an advisory warning, not an authorization decision.
const MAX_DRAFT_FILES: usize = 256;
const MAX_ENTITY_PATHS: usize = 16;

/// A draft data entity the realm's anonymous principal may not read. The
/// location fields are omitted when the caller may not read the object either.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RestrictedFile {
    pub entity_id: String,
    /// The object's permission path, so a grant can name exactly this object.
    pub permission_path: Option<String>,
    pub bucket: Option<String>,
    pub key: Option<String>,
}

struct ObjectPath {
    group_id: GroupId,
    bucket: String,
    key: String,
    path: String,
}

/// Lists the draft's Aruna data entities that anonymous READ would not reach,
/// so a dataset about to be published as public can warn about them. Resolution
/// only follows paths the caller may read; nothing about a foreign object other
/// than "not publicly readable" is disclosed.
pub async fn restricted_files(
    context: &DriverContext,
    realm_id: RealmId,
    auth: &AuthContext,
    rocrate: &JsonValue,
) -> Result<Vec<RestrictedFile>, MetadataError> {
    let anonymous = AuthContext::anonymous(realm_id);
    let mut restricted = Vec::new();
    for (entity_id, content_urls) in draft_files(rocrate) {
        let identity = entity_identity(&entity_id, &content_urls);
        let paths = entity_paths(context, realm_id, &identity).await?;
        if paths.is_empty() {
            continue;
        }
        let mut visible = None;
        let mut public = false;
        for path in paths {
            let evaluator = PolicyEvaluator::load(context, realm_id, Some(path.group_id))
                .await
                .map_err(policy_failure)?;
            if !readable(context, &evaluator, auth, &path).await? {
                continue;
            }
            if readable(context, &evaluator, &anonymous, &path).await? {
                public = true;
                break;
            }
            if visible.is_none() {
                visible = Some(path);
            }
        }
        if public {
            continue;
        }
        restricted.push(RestrictedFile {
            entity_id,
            permission_path: visible.as_ref().map(|path| path.path.clone()),
            bucket: visible.as_ref().map(|path| path.bucket.clone()),
            key: visible.as_ref().map(|path| path.key.clone()),
        });
    }
    Ok(restricted)
}

async fn entity_paths(
    context: &DriverContext,
    realm_id: RealmId,
    identity: &EntityIdentity,
) -> Result<Vec<ObjectPath>, MetadataError> {
    let mut paths = Vec::new();
    let mut seen = BTreeSet::new();
    if let Some(exact) = identity
        .exact
        .as_ref()
        .filter(|exact| exact.realm_id == realm_id)
        && let Some(group_id) = bucket_group(context, &exact.bucket).await?
    {
        let path = blob_object_permission_path(
            realm_id,
            group_id,
            exact.node_id,
            &exact.bucket,
            &exact.key,
        );
        seen.insert(path.clone());
        paths.push(ObjectPath {
            group_id,
            bucket: exact.bucket.clone(),
            key: exact.key.clone(),
            path,
        });
    }
    let hash = identity
        .hash
        .filter(|_| identity.hash_realm.is_none_or(|realm| realm == realm_id));
    let Some(hash) = hash else {
        return Ok(paths);
    };
    let aliases = drive(ResolveBlobPermissionPathsOperation::new(hash), context)
        .await
        .map_err(|error| MetadataError::Backend(error.to_string()))?;
    for alias in aliases.iter().filter(|alias| alias.realm_id == realm_id) {
        if paths.len() >= MAX_ENTITY_PATHS {
            break;
        }
        let path = alias.permission_path();
        if !seen.insert(path.clone()) {
            continue;
        }
        paths.push(ObjectPath {
            group_id: alias.group_id,
            bucket: alias.bucket.clone(),
            key: alias.key.clone(),
            path,
        });
    }
    Ok(paths)
}

async fn bucket_group(
    context: &DriverContext,
    bucket: &str,
) -> Result<Option<GroupId>, MetadataError> {
    match drive(GetBucketInfoOperation::new(bucket.to_string()), context).await {
        Ok(Some(Ok(info))) => Ok(Some(info.group_id)),
        Ok(Some(Err(GetBucketInfoError::NotFound))) | Ok(None) => Ok(None),
        Ok(Some(Err(error))) => Err(MetadataError::Backend(error.to_string())),
        Err(error) => Err(MetadataError::Backend(error.to_string())),
    }
}

async fn readable(
    context: &DriverContext,
    evaluator: &PolicyEvaluator,
    auth: &AuthContext,
    path: &ObjectPath,
) -> Result<bool, MetadataError> {
    let allowed = drive(
        CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: auth.clone(),
            path: path.path.clone(),
            required_permission: Permission::READ,
        }),
        context,
    )
    .await
    .map_err(|error| MetadataError::Backend(error.to_string()))?;
    if !allowed {
        return Ok(false);
    }
    let request = policy_request_with(
        &path.path,
        &Permission::READ,
        Some(auth),
        PolicyRequestExtras::operation("s3.GetObject"),
    );
    match evaluator.evaluate(&request) {
        Ok(()) => Ok(true),
        Err(PolicyEnforcementError::Denied { .. }) => Ok(false),
        Err(error) => Err(policy_failure(error)),
    }
}

fn policy_failure(error: PolicyEnforcementError) -> MetadataError {
    MetadataError::Backend(error.to_string())
}

fn draft_files(rocrate: &JsonValue) -> Vec<(String, Vec<String>)> {
    let Some(graph) = rocrate.get("@graph").and_then(JsonValue::as_array) else {
        return Vec::new();
    };
    graph
        .iter()
        .filter_map(|entity| {
            let entity = entity.as_object()?;
            let entity_id = entity.get("@id")?.as_str()?;
            is_file(entity.get("@type")?).then(|| {
                (
                    entity_id.to_string(),
                    entity
                        .get("contentUrl")
                        .map(text_values)
                        .unwrap_or_default(),
                )
            })
        })
        .take(MAX_DRAFT_FILES)
        .collect()
}

fn is_file(value: &JsonValue) -> bool {
    match value {
        JsonValue::String(value) => FILE_TYPES.contains(&value.as_str()),
        JsonValue::Array(values) => values.iter().any(is_file),
        _ => false,
    }
}

fn text_values(value: &JsonValue) -> Vec<String> {
    match value {
        JsonValue::String(value) => vec![value.clone()],
        JsonValue::Array(values) => values.iter().flat_map(text_values).collect(),
        JsonValue::Object(value) => value
            .get("@id")
            .and_then(JsonValue::as_str)
            .map(|id| vec![id.to_string()])
            .into_iter()
            .flatten()
            .collect(),
        _ => Vec::new(),
    }
}
