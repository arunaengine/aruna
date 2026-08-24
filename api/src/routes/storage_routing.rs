use crate::auth::{parse_group_id, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server_state::ServerState;
use aruna_core::structs::{
    AuthContext, BackendRef, Permission, RoutingTarget, StorageRoutingRule, target_warnings,
};
use aruna_operations::driver::{drive, node_routing};
use aruna_operations::group_routing::{
    GetGroupRoutingOperation, GroupRoutingInputsOperation, PutGroupRoutingError,
    PutGroupRoutingOperation,
};
use aruna_operations::s3::bucket_routing::{
    GetBucketRoutingError, GetBucketRoutingOperation, PutBucketRoutingError,
    PutBucketRoutingOperation,
};
use aruna_operations::s3::get_bucket_info::{GetBucketInfoError, GetBucketInfoOperation};
use axum::extract::{Path, State};
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;
use std::time::SystemTime;
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

#[derive(OpenApi)]
#[openapi(
    tags((name = "storage-routing", description = "Write routing rules for buckets and groups"))
)]
pub struct StorageRoutingApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(StorageRoutingApiDoc::openapi())
        .routes(routes!(get_bucket_routing, put_bucket_routing))
        .routes(routes!(get_group_routing, put_group_routing))
}

/// A rule target names either a group storage backend or a storage class, and
/// exactly one of the two fields must be set. Operator backend names are
/// rejected: tenants never bind node topology.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct RoutingTargetRequest {
    /// Set this or `class`, never both and never neither.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backend_id: Option<String>,
    /// Set this or `backend_id`, never both and never neither.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub class: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct StorageRoutingRuleRequest {
    #[serde(default)]
    pub key_prefix: String,
    #[serde(default)]
    pub exact: bool,
    pub target: RoutingTargetRequest,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct BucketRoutingRequest {
    pub rules: Vec<StorageRoutingRuleRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct BucketRoutingResponse {
    pub bucket: String,
    pub rules: Vec<StorageRoutingRuleRequest>,
    /// Advisory notes about targets this node cannot serve. The rules are
    /// stored regardless, because the record replicates to other nodes.
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct GroupRoutingRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_target: Option<RoutingTargetRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct GroupRoutingResponse {
    pub group_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_target: Option<RoutingTargetRequest>,
    pub warnings: Vec<String>,
}

impl TryFrom<RoutingTargetRequest> for RoutingTarget {
    type Error = ServerError;

    fn try_from(value: RoutingTargetRequest) -> Result<Self, Self::Error> {
        match (value.backend_id, value.class) {
            (Some(backend_id), None) => Ulid::from_str(&backend_id)
                .map(|id| RoutingTarget::Backend(BackendRef::Group(id)))
                .map_err(|_| ServerError::BadRequest),
            (None, Some(class)) => Ok(RoutingTarget::Class(class)),
            _ => Err(ServerError::BadRequest),
        }
    }
}

impl From<RoutingTarget> for RoutingTargetRequest {
    fn from(value: RoutingTarget) -> Self {
        match value {
            RoutingTarget::Backend(BackendRef::Group(id)) => Self {
                backend_id: Some(id.to_string()),
                class: None,
            },
            RoutingTarget::Backend(BackendRef::Node(name)) => Self {
                backend_id: Some(name),
                class: None,
            },
            RoutingTarget::Class(class) => Self {
                backend_id: None,
                class: Some(class),
            },
        }
    }
}

impl TryFrom<StorageRoutingRuleRequest> for StorageRoutingRule {
    type Error = ServerError;

    fn try_from(value: StorageRoutingRuleRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            key_prefix: value.key_prefix,
            exact: value.exact,
            target: value.target.try_into()?,
        })
    }
}

impl From<StorageRoutingRule> for StorageRoutingRuleRequest {
    fn from(value: StorageRoutingRule) -> Self {
        Self {
            key_prefix: value.key_prefix,
            exact: value.exact,
            target: value.target.into(),
        }
    }
}

fn map_group_error(error: PutGroupRoutingError) -> ServerError {
    match error {
        PutGroupRoutingError::InvalidTarget(reason) => {
            ServerError::BadRequestReason(reason.to_string())
        }
        other => ServerError::InternalError(other.to_string()),
    }
}

fn map_put_error(error: PutBucketRoutingError) -> ServerError {
    match error {
        PutBucketRoutingError::NoSuchBucket | PutBucketRoutingError::GroupMismatch => {
            ServerError::NotFound
        }
        PutBucketRoutingError::InvalidRules(reason) => {
            ServerError::BadRequestReason(reason.to_string())
        }
        other => ServerError::InternalError(other.to_string()),
    }
}

/// Advisory notes from this node's own class table, read against the backends
/// the group itself registered.
async fn warnings_for<'a>(
    state: &ServerState,
    group_id: Ulid,
    targets: impl IntoIterator<Item = &'a RoutingTarget>,
) -> Vec<String> {
    let context = state.get_ctx();
    let inputs = drive(GroupRoutingInputsOperation::new(group_id), &context)
        .await
        .unwrap_or_default();
    let catalog = node_routing(&context)
        .catalog
        .with_group_backends(inputs.backend_ids);
    target_warnings(&catalog, targets)
}

async fn group_of_bucket(state: &ServerState, bucket: &str) -> ServerResult<Ulid> {
    match drive(
        GetBucketInfoOperation::new(bucket.to_string()),
        &state.get_ctx(),
    )
    .await
    {
        Ok(Some(Ok(info))) => Ok(info.group_id),
        Ok(Some(Err(GetBucketInfoError::NotFound))) | Err(GetBucketInfoError::NotFound) => {
            Err(ServerError::NotFound)
        }
        Ok(Some(Err(err))) | Err(err) => Err(ServerError::InternalError(err.to_string())),
        Ok(None) => Err(ServerError::NotFound),
    }
}

/// Routing decides where a group's bytes physically land, so it takes group
/// admin rights, not the write rights that suffice for objects.
pub(crate) async fn ensure_group_admin(
    state: &ServerState,
    auth: &AuthContext,
    group_id: Ulid,
) -> ServerResult<()> {
    crate::auth::ensure_permission(
        state,
        auth,
        format!("/{}/g/{group_id}/admin/**", state.get_realm_id()),
        Permission::WRITE,
    )
    .await
}

#[utoipa::path(
    get,
    path = "/buckets/{bucket}/storage-routing",
    tag = "storage-routing",
    summary = "Read a bucket's write routing rules",
    description = r#"Returns the write routing rules stored for a bucket on this node, in the order they were submitted.

**Authentication**: realm bearer token with WRITE on the owning group's admin path. Routing decides
where a group's bytes physically land, so the write rights that suffice for objects are not enough.

**Behavior**
- The bucket's owning group is resolved first, so a bucket unknown to this node is not found and the
  admin check always runs against the group that owns it.
- This is a node-local read of the replicated bucket record: rules written on another node can be
  missing until they arrive here, and a bucket that never had rules returns an empty list.
- `warnings` is advisory only and is recomputed per request from the storage classes this node
  offers to tenants plus the backends the group itself registered, so the same stored rules can
  warn here and not on another node."#,
    params(("bucket" = String, Path, description = "Bucket name as used by the S3 surface, without a leading slash")),
    responses(
        (
            status = 200,
            description = "The stored rules in submission order, plus advisory warnings for targets this node cannot serve",
            body = BucketRoutingResponse,
            example = json!({
                "bucket": "research-raw",
                "rules": [
                    {
                        "key_prefix": "archive/",
                        "exact": false,
                        "target": { "class": "cold" }
                    },
                    {
                        "key_prefix": "",
                        "exact": false,
                        "target": { "backend_id": "01JBACKEND0123456789ABCDE" }
                    }
                ],
                "warnings": [
                    "storage class `cold` is not offered to tenants by this node"
                ]
            })
        ),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm, or the caller lacks WRITE on the group admin path", body = ErrorResponse),
        (status = 404, description = "No bucket of that name is known to this node", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_bucket_routing(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(bucket): Path<String>,
) -> ServerResult<Json<BucketRoutingResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let group_id = group_of_bucket(&state, &bucket).await?;
    ensure_group_admin(&state, &auth, group_id).await?;

    let rules = drive(
        GetBucketRoutingOperation::new(bucket.clone()),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| match error {
        GetBucketRoutingError::NoSuchBucket => ServerError::NotFound,
        other => ServerError::InternalError(other.to_string()),
    })?
    .transpose()
    .map_err(|error| ServerError::InternalError(error.to_string()))?
    .unwrap_or_default();

    let warnings = warnings_for(&state, group_id, rules.iter().map(|rule| &rule.target)).await;
    Ok(Json(BucketRoutingResponse {
        bucket,
        rules: rules.into_iter().map(Into::into).collect(),
        warnings,
    }))
}

#[utoipa::path(
    put,
    path = "/buckets/{bucket}/storage-routing",
    tag = "storage-routing",
    summary = "Replace a bucket's write routing rules",
    description = r#"Replaces the whole write routing rule set of a bucket with the submitted list.

**Authentication**: realm bearer token with WRITE on the owning group's admin path.

**Behavior**
- The submitted list replaces the bucket's whole rule set; sending an empty list clears it.
- At write time the most specific rule wins, in this order: an exact-key rule, then the longest
  matching `key_prefix` rule (an empty prefix is the bucket default), then the group default, then
  the operator's own node rules, then the node's default backend.
- This only steers data written after the change; objects already stored are never moved.
- Rules are stored even when this node cannot serve their target, because the bucket record
  replicates to nodes that may offer it, and the unserved targets come back in `warnings`.

**Limits** (all refused with 400)
- Each rule target sets exactly one of `backend_id` or `class`.
- A `backend_id` must be the ULID of a backend the group itself registered, so tenant rules can
  never name an operator's node backend.
- A `class` must be a valid storage-class name.
- Two rules may not share the same `key_prefix` and `exact` combination."#,
    params(("bucket" = String, Path, description = "Bucket name as used by the S3 surface, without a leading slash")),
    request_body(
        content = BucketRoutingRequest,
        description = "The complete rule set for this bucket. `key_prefix` defaults to the empty string (the bucket default) and `exact` defaults to false (prefix match).",
        example = json!({
            "rules": [
                {
                    "key_prefix": "archive/",
                    "exact": false,
                    "target": { "class": "cold" }
                },
                {
                    "key_prefix": "index/manifest.json",
                    "exact": true,
                    "target": { "backend_id": "01JBACKEND0123456789ABCDE" }
                }
            ]
        })
    ),
    responses(
        (
            status = 200,
            description = "The rule set as stored, with advisory warnings for targets this node cannot serve",
            body = BucketRoutingResponse,
            example = json!({
                "bucket": "research-raw",
                "rules": [
                    {
                        "key_prefix": "archive/",
                        "exact": false,
                        "target": { "class": "cold" }
                    },
                    {
                        "key_prefix": "index/manifest.json",
                        "exact": true,
                        "target": { "backend_id": "01JBACKEND0123456789ABCDE" }
                    }
                ],
                "warnings": [
                    "storage class `cold` is not offered to tenants by this node"
                ]
            })
        ),
        (status = 400, description = "A rule target is invalid, names a backend the group does not own, or duplicates another rule's prefix and match mode", body = ErrorResponse),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm, or the caller lacks WRITE on the group admin path", body = ErrorResponse),
        (status = 404, description = "No bucket of that name is known to this node, or it no longer belongs to the authorized group", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn put_bucket_routing(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(bucket): Path<String>,
    Json(request): Json<BucketRoutingRequest>,
) -> ServerResult<Json<BucketRoutingResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let group_id = group_of_bucket(&state, &bucket).await?;
    ensure_group_admin(&state, &auth, group_id).await?;

    let rules = request
        .rules
        .into_iter()
        .map(StorageRoutingRule::try_from)
        .collect::<Result<Vec<_>, _>>()?;

    let stored = drive(
        PutBucketRoutingOperation::new(bucket.clone(), group_id, rules),
        &state.get_ctx(),
    )
    .await
    .map_err(map_put_error)?
    .transpose()
    .map_err(map_put_error)?
    .unwrap_or_default();

    let warnings = warnings_for(&state, group_id, stored.iter().map(|rule| &rule.target)).await;
    Ok(Json(BucketRoutingResponse {
        bucket,
        rules: stored.into_iter().map(Into::into).collect(),
        warnings,
    }))
}

#[utoipa::path(
    get,
    path = "/groups/{group_id}/storage-routing",
    tag = "storage-routing",
    summary = "Read a group's default write target",
    description = r#"Returns the default write target this node holds for a group, or none when the group never set one.

**Authentication**: realm bearer token with WRITE on that group's admin path.

**Behavior**
- The group default applies to every bucket of the group and is consulted only after the bucket's
  own rules: an exact-key rule and then the longest matching key prefix on the bucket take
  precedence over it, and it in turn takes precedence over the operator's node rules and the node's
  default backend.
- This is a node-local read of the replicated group routing record; a group that has never set a
  default returns `default_target` omitted.
- `warnings` is advisory only and is recomputed per request against the classes this node offers to
  tenants and the backends the group registered, so it can differ between nodes for the same stored
  default."#,
    params(("group_id" = String, Path, description = "Group id as a 26-character ULID")),
    responses(
        (
            status = 200,
            description = "The group's default target, omitted when none is set, plus advisory warnings",
            body = GroupRoutingResponse,
            example = json!({
                "group_id": "01JABCDEF0123456789ABCDEFG",
                "default_target": { "class": "warm" },
                "warnings": []
            })
        ),
        (status = 400, description = "The path segment is not a valid group ULID", body = ErrorResponse),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm, or the caller lacks WRITE on the group admin path", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_group_routing(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(group_id): Path<String>,
) -> ServerResult<Json<GroupRoutingResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let group_id = parse_group_id(&group_id)?;
    ensure_group_admin(&state, &auth, group_id).await?;

    let record = drive(GetGroupRoutingOperation::new(group_id), &state.get_ctx())
        .await
        .map_err(|error| ServerError::InternalError(error.to_string()))?
        .transpose()
        .map_err(|error| ServerError::InternalError(error.to_string()))?
        .flatten();

    let target = record.and_then(|record| record.default_target);
    let warnings = warnings_for(&state, group_id, target.iter()).await;
    Ok(Json(GroupRoutingResponse {
        group_id: group_id.to_string(),
        default_target: target.map(Into::into),
        warnings,
    }))
}

#[utoipa::path(
    put,
    path = "/groups/{group_id}/storage-routing",
    tag = "storage-routing",
    summary = "Set or clear a group's default write target",
    description = r#"Replaces the group's default write target, or clears it when no target is submitted.

**Authentication**: realm bearer token with WRITE on that group's admin path.

**Behavior**
- The submitted value replaces the group's default outright, and omitting `default_target` or
  sending it as null clears it, which returns the group to the operator's node rules and the node
  default.
- The default is scoped to the whole group and is weaker than any matching rule on an individual
  bucket.
- It only steers data written after the change; objects already stored are never moved.
- The record is stored even when this node cannot serve the target, because it replicates to nodes
  that may, and the unserved target comes back in `warnings`.
- Each write records the caller and the time as the last decider.

**Limits** (all refused with 400)
- The target sets exactly one of `backend_id` or `class`.
- A `backend_id` must be the ULID of a backend the group itself registered, so a tenant can never
  bind an operator's node backend.
- A `class` must be a valid storage-class name."#,
    params(("group_id" = String, Path, description = "Group id as a 26-character ULID")),
    request_body(
        content = GroupRoutingRequest,
        description = "The new default target, or an empty object to clear the group default.",
        example = json!({
            "default_target": { "class": "warm" }
        })
    ),
    responses(
        (
            status = 200,
            description = "The default as stored, omitted when the request cleared it, plus advisory warnings",
            body = GroupRoutingResponse,
            example = json!({
                "group_id": "01JABCDEF0123456789ABCDEFG",
                "default_target": { "class": "warm" },
                "warnings": [
                    "storage class `warm` is not offered to tenants by this node"
                ]
            })
        ),
        (status = 400, description = "The path segment is not a valid group ULID, or the target is invalid or names a backend the group does not own", body = ErrorResponse),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm, or the caller lacks WRITE on the group admin path", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn put_group_routing(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(group_id): Path<String>,
    Json(request): Json<GroupRoutingRequest>,
) -> ServerResult<Json<GroupRoutingResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let group_id = parse_group_id(&group_id)?;
    ensure_group_admin(&state, &auth, group_id).await?;

    let target = request
        .default_target
        .map(RoutingTarget::try_from)
        .transpose()?;

    let record = drive(
        PutGroupRoutingOperation::new(group_id, target, auth.user_id, SystemTime::now()),
        &state.get_ctx(),
    )
    .await
    .map_err(map_group_error)?
    .transpose()
    .map_err(map_group_error)?;

    let target = record.and_then(|record| record.default_target);
    let warnings = warnings_for(&state, group_id, target.iter()).await;
    Ok(Json(GroupRoutingResponse {
        group_id: group_id.to_string(),
        default_target: target.map(Into::into),
        warnings,
    }))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::openapi::ApiDoc;
    use aruna_core::UserId;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{
        AUTH_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE, S3_BUCKET_KEYSPACE,
    };
    use aruna_core::structs::{
        Actor, BucketInfo, Group, GroupAuthorizationDocument, NodeCapabilities,
        RealmAuthorizationDocument, RealmConfigDocument, RealmId,
    };
    use aruna_operations::driver::DriverContext;
    use aruna_storage::storage;
    use tempfile::TempDir;

    pub(crate) struct TestState {
        _storage_dir: TempDir,
        pub(crate) auth: AuthContext,
        pub(crate) other_auth: AuthContext,
        pub(crate) group_id: Ulid,
        pub(crate) bucket: String,
        pub(crate) state: Arc<ServerState>,
    }

    fn class_rule(class: &str) -> StorageRoutingRuleRequest {
        StorageRoutingRuleRequest {
            key_prefix: "archive/".to_string(),
            exact: false,
            target: RoutingTargetRequest {
                backend_id: None,
                class: Some(class.to_string()),
            },
        }
    }

    #[tokio::test]
    async fn bucket_rules_roundtrip() {
        let test = setup_state().await;

        let Json(stored) = put_bucket_routing(
            State(test.state.clone()),
            Extension(Some(test.auth.clone())),
            Path(test.bucket.clone()),
            Json(BucketRoutingRequest {
                rules: vec![class_rule("cold")],
            }),
        )
        .await
        .unwrap();

        assert_eq!(stored.rules, vec![class_rule("cold")]);
        // The node offers no cold backend, so the rule is stored with a warning.
        assert_eq!(stored.warnings.len(), 1);

        let Json(fetched) = get_bucket_routing(
            State(test.state.clone()),
            Extension(Some(test.auth.clone())),
            Path(test.bucket.clone()),
        )
        .await
        .unwrap();

        assert_eq!(fetched.rules, stored.rules);
    }

    #[tokio::test]
    async fn rejects_operator_backend() {
        let test = setup_state().await;

        let result = put_bucket_routing(
            State(test.state.clone()),
            Extension(Some(test.auth.clone())),
            Path(test.bucket.clone()),
            Json(BucketRoutingRequest {
                rules: vec![StorageRoutingRuleRequest {
                    key_prefix: String::new(),
                    exact: false,
                    target: RoutingTargetRequest {
                        backend_id: Some("cold".to_string()),
                        class: None,
                    },
                }],
            }),
        )
        .await;

        assert!(matches!(result, Err(ServerError::BadRequest)));
    }

    #[tokio::test]
    async fn rejects_non_admin() {
        let test = setup_state().await;

        let result = put_bucket_routing(
            State(test.state.clone()),
            Extension(Some(test.other_auth.clone())),
            Path(test.bucket.clone()),
            Json(BucketRoutingRequest { rules: Vec::new() }),
        )
        .await;

        assert!(matches!(result, Err(ServerError::Forbidden)));
    }

    #[tokio::test]
    async fn group_default_roundtrip() {
        let test = setup_state().await;

        let Json(empty) = get_group_routing(
            State(test.state.clone()),
            Extension(Some(test.auth.clone())),
            Path(test.group_id.to_string()),
        )
        .await
        .unwrap();
        assert_eq!(empty.default_target, None);

        let Json(stored) = put_group_routing(
            State(test.state.clone()),
            Extension(Some(test.auth.clone())),
            Path(test.group_id.to_string()),
            Json(GroupRoutingRequest {
                default_target: Some(RoutingTargetRequest {
                    backend_id: None,
                    class: Some("cold".to_string()),
                }),
            }),
        )
        .await
        .unwrap();
        assert_eq!(
            stored
                .default_target
                .and_then(|target| target.class)
                .as_deref(),
            Some("cold")
        );

        let Json(cleared) = put_group_routing(
            State(test.state.clone()),
            Extension(Some(test.auth.clone())),
            Path(test.group_id.to_string()),
            Json(GroupRoutingRequest {
                default_target: None,
            }),
        )
        .await
        .unwrap();
        assert_eq!(cleared.default_target, None);
    }

    #[tokio::test]
    async fn rejects_invalid_class() {
        let test = setup_state().await;

        let result = put_group_routing(
            State(test.state.clone()),
            Extension(Some(test.auth.clone())),
            Path(test.group_id.to_string()),
            Json(GroupRoutingRequest {
                default_target: Some(RoutingTargetRequest {
                    backend_id: None,
                    class: Some("NOT VALID".to_string()),
                }),
            }),
        )
        .await;

        assert!(matches!(result, Err(ServerError::BadRequestReason(_))));
    }

    #[tokio::test]
    async fn rejects_foreign_backend() {
        // Nothing registered this id for the group, so the rule must not store.
        let test = setup_state().await;
        let foreign = Ulid::generate();

        let result = put_bucket_routing(
            State(test.state.clone()),
            Extension(Some(test.auth.clone())),
            Path(test.bucket.clone()),
            Json(BucketRoutingRequest {
                rules: vec![StorageRoutingRuleRequest {
                    key_prefix: String::new(),
                    exact: false,
                    target: RoutingTargetRequest {
                        backend_id: Some(foreign.to_string()),
                        class: None,
                    },
                }],
            }),
        )
        .await;

        let Err(ServerError::BadRequestReason(reason)) = result else {
            panic!("expected a 400 naming the backend, got {result:?}")
        };
        assert!(reason.contains(&foreign.to_string()), "{reason}");
    }

    #[test]
    fn openapi_lists_routes() {
        let openapi = serde_json::to_value(ApiDoc::openapi()).unwrap();

        assert!(openapi["paths"]["/buckets/{bucket}/storage-routing"]["put"].is_object());
        assert!(openapi["paths"]["/groups/{group_id}/storage-routing"]["put"].is_object());
        assert!(
            openapi["components"]["schemas"]["BucketRoutingResponse"]["properties"]
                .get("warnings")
                .is_some()
        );
    }

    pub(crate) async fn setup_state() -> TestState {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let realm_id = RealmId([3u8; 32]);
        let node_id = iroh::SecretKey::from_bytes(&[11u8; 32]).public();
        let user_id = UserId::local(Ulid::generate(), realm_id);
        let other_user_id = UserId::local(Ulid::generate(), realm_id);
        let actor = Actor {
            node_id,
            user_id,
            realm_id,
        };
        let driver_ctx = Arc::new(DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });
        let group_id = Ulid::generate();
        let group_auth =
            GroupAuthorizationDocument::new_default_group_doc(user_id, realm_id, group_id);
        let group = Group {
            display_name: "routing-group".to_string(),
            group_id,
            realm_id,
            roles: group_auth.roles.keys().copied().collect(),
            owner: user_id,
        };
        let realm_auth = RealmAuthorizationDocument::new_default_realm_doc(realm_id);
        let bucket = "routed".to_string();
        let bucket_info = BucketInfo {
            group_id,
            created_at: SystemTime::UNIX_EPOCH,
            created_by: user_id,
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
        };

        // Request-policy loading fails closed without the realm config document.
        write_doc(
            &driver_ctx,
            REALM_CONFIG_KEYSPACE,
            (*realm_id.as_bytes()).into(),
            RealmConfigDocument::default_for_realm(realm_id, Vec::new())
                .to_bytes(&actor)
                .unwrap()
                .into(),
        )
        .await;
        write_doc(
            &driver_ctx,
            AUTH_KEYSPACE,
            (*realm_id.as_bytes()).into(),
            realm_auth.to_bytes(&actor).unwrap().into(),
        )
        .await;
        write_doc(
            &driver_ctx,
            AUTH_KEYSPACE,
            group_id.to_bytes().into(),
            group_auth.to_bytes(&actor).unwrap().into(),
        )
        .await;
        write_doc(
            &driver_ctx,
            GROUP_KEYSPACE,
            group_id.to_bytes().into(),
            group.to_bytes(&actor).unwrap().into(),
        )
        .await;
        write_doc(
            &driver_ctx,
            S3_BUCKET_KEYSPACE,
            bucket.as_bytes().to_vec().into(),
            bucket_info.to_bytes().unwrap().into(),
        )
        .await;

        let state = Arc::new(
            ServerState::new(
                driver_ctx,
                realm_id,
                node_id,
                NodeCapabilities::user_node(realm_id).unwrap(),
                false,
                None,
                aruna_operations::jobs::runtime::JobsRuntime::new(),
            )
            .await,
        );

        TestState {
            _storage_dir: storage_dir,
            auth: AuthContext {
                user_id,
                realm_id,
                path_restrictions: None,
            },
            other_auth: AuthContext {
                user_id: other_user_id,
                realm_id,
                path_restrictions: None,
            },
            group_id,
            bucket,
            state,
        }
    }

    async fn write_doc(
        driver_ctx: &Arc<DriverContext>,
        key_space: &str,
        key: byteview::ByteView,
        value: byteview::ByteView,
    ) {
        let event = driver_ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: key_space.to_string(),
                key,
                value,
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }
}
