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

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{
        AUTH_KEYSPACE, GROUP_KEYSPACE, HASH_PATHS_INDEX_KEYSPACE, REALM_CONFIG_KEYSPACE,
        S3_BUCKET_KEYSPACE,
    };
    use aruna_core::structs::{
        ARUNA_DATA_PREFIX, Actor, BucketInfo, Group, GroupAuthorizationDocument, HashPathIndexKey,
        RealmAuthorizationDocument, RealmConfigDocument, RealmNodeKind, Role,
    };
    use serde_json::json;
    use std::collections::{HashMap, HashSet};
    use ulid::Ulid;

    const BUCKET: &str = "reads";
    const KEY: &str = "raw/one.csv";

    struct Fixture {
        context: DriverContext,
        realm_id: RealmId,
        owner: AuthContext,
        hash: [u8; 32],
        permission_path: String,
        _tempdir: tempfile::TempDir,
    }

    async fn fixture(anonymous_read: bool) -> Fixture {
        let staging = crate::staging::test_utils::setup_driver_context().await;
        let context = staging.driver_context;
        let realm_id = RealmId::from_bytes([61; 32]);
        let owner = UserId::local(Ulid::from_bytes([62; 16]), realm_id);
        let group_id = Ulid::from_bytes([63; 16]);
        let version_id = Ulid::from_bytes([64; 16]);
        let node_id = iroh::SecretKey::from_bytes(&[65; 32]).public();
        let hash = [66; 32];
        let actor = Actor {
            node_id,
            user_id: owner,
            realm_id,
        };
        let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        config.ensure_node(node_id, RealmNodeKind::Server);
        let mut realm_auth = RealmAuthorizationDocument::new_default_realm_doc(realm_id);
        if anonymous_read {
            let role_id = Ulid::from_bytes([67; 16]);
            realm_auth.roles.insert(
                role_id,
                Role {
                    role_id,
                    name: "everyone".to_string(),
                    permissions: HashMap::from([(
                        format!("/{realm_id}/g/{group_id}/**"),
                        Permission::READ,
                    )]),
                    assigned_users: HashSet::from([UserId::nil(realm_id)]),
                },
            );
        }
        let group_auth =
            GroupAuthorizationDocument::new_default_group_doc(owner, realm_id, group_id);
        let group = Group {
            display_name: "preview".to_string(),
            group_id,
            realm_id,
            roles: group_auth.roles.keys().copied().collect(),
            owner,
        };
        let bucket = BucketInfo {
            group_id,
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: owner,
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
        };
        let alias =
            HashPathIndexKey::new(hash, version_id, realm_id, group_id, node_id, BUCKET, KEY);
        let writes = vec![
            (
                REALM_CONFIG_KEYSPACE.to_string(),
                realm_id.as_bytes().to_vec().into(),
                config.to_bytes(&actor).unwrap().into(),
            ),
            (
                AUTH_KEYSPACE.to_string(),
                realm_id.as_bytes().to_vec().into(),
                realm_auth.to_bytes(&actor).unwrap().into(),
            ),
            (
                AUTH_KEYSPACE.to_string(),
                group_id.to_bytes().to_vec().into(),
                group_auth.to_bytes(&actor).unwrap().into(),
            ),
            (
                GROUP_KEYSPACE.to_string(),
                group_id.to_bytes().to_vec().into(),
                group.to_bytes(&actor).unwrap().into(),
            ),
            (
                S3_BUCKET_KEYSPACE.to_string(),
                BUCKET.as_bytes().to_vec().into(),
                bucket.to_bytes().unwrap().into(),
            ),
            (
                HASH_PATHS_INDEX_KEYSPACE.to_string(),
                alias.to_bytes().unwrap().into(),
                Vec::new().into(),
            ),
        ];
        assert!(matches!(
            context
                .storage_handle
                .send_storage_effect(StorageEffect::BatchWrite {
                    writes,
                    txn_id: None
                })
                .await,
            Event::Storage(StorageEvent::BatchWriteResult { .. })
        ));
        Fixture {
            context,
            realm_id,
            owner: AuthContext {
                user_id: owner,
                realm_id,
                path_restrictions: None,
                session: None,
            },
            hash,
            permission_path: blob_object_permission_path(realm_id, group_id, node_id, BUCKET, KEY),
            _tempdir: staging._tempdir,
        }
    }

    fn draft(hash: [u8; 32]) -> JsonValue {
        json!({
            "@graph": [
                {"@id": "./", "@type": "Dataset", "name": "draft"},
                {
                    "@id": format!("{ARUNA_DATA_PREFIX}{}", hex::encode(hash)),
                    "@type": "File",
                    "name": "one.csv",
                    "contentUrl": format!("s3://{BUCKET}/{KEY}")
                }
            ]
        })
    }

    #[tokio::test]
    async fn lists_private_object() {
        let fixture = fixture(false).await;

        let restricted = restricted_files(
            &fixture.context,
            fixture.realm_id,
            &fixture.owner,
            &draft(fixture.hash),
        )
        .await
        .unwrap();

        assert_eq!(restricted.len(), 1);
        assert_eq!(restricted[0].bucket.as_deref(), Some(BUCKET));
        assert_eq!(restricted[0].key.as_deref(), Some(KEY));
        assert_eq!(
            restricted[0].permission_path.as_deref(),
            Some(fixture.permission_path.as_str())
        );
    }

    #[tokio::test]
    async fn skips_public_object() {
        let fixture = fixture(true).await;

        let restricted = restricted_files(
            &fixture.context,
            fixture.realm_id,
            &fixture.owner,
            &draft(fixture.hash),
        )
        .await
        .unwrap();

        assert!(restricted.is_empty());
    }

    #[tokio::test]
    async fn hides_foreign_object() {
        let fixture = fixture(false).await;
        let stranger = AuthContext {
            user_id: UserId::local(Ulid::from_bytes([68; 16]), fixture.realm_id),
            realm_id: fixture.realm_id,
            path_restrictions: None,
            session: None,
        };

        let restricted = restricted_files(
            &fixture.context,
            fixture.realm_id,
            &stranger,
            &draft(fixture.hash),
        )
        .await
        .unwrap();

        assert_eq!(restricted.len(), 1);
        assert!(restricted[0].permission_path.is_none());
        assert!(restricted[0].bucket.is_none());
        assert!(restricted[0].key.is_none());
    }

    #[tokio::test]
    async fn ignores_unknown_object() {
        let fixture = fixture(false).await;

        let restricted = restricted_files(
            &fixture.context,
            fixture.realm_id,
            &fixture.owner,
            &draft([99; 32]),
        )
        .await
        .unwrap();

        assert!(restricted.is_empty());
    }

    #[test]
    fn selects_data_entities() {
        let document = json!({
            "@graph": [
                {"@id": "./", "@type": "Dataset", "name": "draft"},
                {"@id": "one", "@type": "File", "contentUrl": "s3://reads/one.csv"},
                {"@id": "two", "@type": ["MediaObject", "Thing"],
                 "contentUrl": ["s3://reads/two.csv", {"@id": "s3://reads/dup.csv"}]},
                {"@id": "three", "@type": "CreativeWork"}
            ]
        });

        let files = draft_files(&document);

        assert_eq!(
            files,
            vec![
                ("one".to_string(), vec!["s3://reads/one.csv".to_string()]),
                (
                    "two".to_string(),
                    vec![
                        "s3://reads/two.csv".to_string(),
                        "s3://reads/dup.csv".to_string()
                    ]
                ),
            ]
        );
    }

    #[test]
    fn ignores_missing_graph() {
        assert!(draft_files(&json!({"@id": "./"})).is_empty());
    }
}
