use crate::auth::{parse_group_id, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server::state::ServerState;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::{BackendRef, bucket_permission_path};
use aruna_core::structs::storage::routing::{RoutingTarget, StorageRoutingRule, target_warnings};
use aruna_operations::driver::{drive, node_routing};
use aruna_operations::groups::storage_routing::{
    GroupInputsOperation, GroupRoutingOperation, PutGroupError, PutGroupOperation,
};
use aruna_operations::s3::bucket::get::{GetBucketError, GetBucketOperation};
use aruna_operations::s3::bucket::routing::{
    GetRoutingError, GetRoutingOperation, PutRoutingError, PutRoutingOperation,
};
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
#[openapi()]
pub struct StorageRoutingDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(StorageRoutingDoc::openapi())
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
#[schema(as = StorageRoutingRuleRequest)]
pub struct RoutingRuleRequest {
    #[serde(default)]
    pub key_prefix: String,
    #[serde(default)]
    pub exact: bool,
    pub target: RoutingTargetRequest,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct BucketRoutingRequest {
    pub rules: Vec<RoutingRuleRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct BucketRoutingResponse {
    pub bucket: String,
    pub rules: Vec<RoutingRuleRequest>,
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

impl TryFrom<RoutingRuleRequest> for StorageRoutingRule {
    type Error = ServerError;

    fn try_from(value: RoutingRuleRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            key_prefix: value.key_prefix,
            exact: value.exact,
            target: value.target.try_into()?,
        })
    }
}

impl From<StorageRoutingRule> for RoutingRuleRequest {
    fn from(value: StorageRoutingRule) -> Self {
        Self {
            key_prefix: value.key_prefix,
            exact: value.exact,
            target: value.target.into(),
        }
    }
}

fn map_group_error(error: PutGroupError) -> ServerError {
    match error {
        PutGroupError::InvalidTarget(reason) => ServerError::BadRequestReason(reason.to_string()),
        other => ServerError::InternalError(other.to_string()),
    }
}

fn map_put_error(error: PutRoutingError) -> ServerError {
    match error {
        PutRoutingError::NoSuchBucket | PutRoutingError::GroupMismatch => ServerError::NotFound,
        PutRoutingError::InvalidRules(reason) => ServerError::BadRequestReason(reason.to_string()),
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
    let inputs = drive(GroupInputsOperation::new(group_id), &context)
        .await
        .unwrap_or_default();
    let catalog = node_routing(&context)
        .catalog
        .with_group_backends(inputs.backend_ids);
    target_warnings(&catalog, targets)
}

async fn group_of_bucket(state: &ServerState, bucket: &str) -> ServerResult<Ulid> {
    match drive(
        GetBucketOperation::new(bucket.to_string()),
        &state.get_ctx(),
    )
    .await
    {
        Ok(info) => Ok(info.group_id),
        Err(GetBucketError::NotFound) => Err(ServerError::NotFound),
        Err(err) => Err(ServerError::InternalError(err.to_string())),
    }
}

/// The bucket overview shows the backend a bucket writes to, so reading the
/// rules takes the same right as reading the bucket itself.
async fn ensure_bucket_read(
    state: &ServerState,
    auth: &AuthContext,
    group_id: Ulid,
    bucket: &str,
) -> ServerResult<()> {
    crate::auth::ensure_permission(
        state,
        auth,
        bucket_permission_path(state.get_realm_id(), group_id, state.get_node_id(), bucket),
        Permission::READ,
    )
    .await
}

/// Routing decides where a group's bytes physically land, so changing it takes
/// group admin rights, not the write rights that suffice for objects.
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
    path = "/data/buckets/{bucket}/storage/routing",
    tag = "data/storage",
    summary = "Read a bucket's write routing rules",
    description = r#"Returns the write routing rules that pick the storage backend for new writes to a bucket on this node, in submission order.

**Authentication**: realm bearer token with READ on the bucket. Reading where a bucket's bytes land
is part of describing the bucket; changing the rules still takes group admin write.

**Behavior**
- Node-local read of the replicated bucket record: rules written on another node can be missing
  until they arrive here.
- `warnings` is advisory and is recomputed per request from the storage classes this node offers to
  tenants plus the backends the group registered, so the same rules can warn here and not on
  another node."#,
    params(("bucket" = String, Path, description = "Bucket name as used on the S3 surface, without a leading slash")),
    responses(
        (
            status = 200,
            description = "The stored rules in submission order, with advisory warnings for targets this node cannot serve",
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
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token from another realm, or no READ on the bucket", body = ErrorResponse),
        (status = 404, description = "Bucket not found on this node", body = ErrorResponse)
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
    ensure_bucket_read(&state, &auth, group_id, &bucket).await?;

    let rules = drive(GetRoutingOperation::new(bucket.clone()), &state.get_ctx())
        .await
        .map_err(|error| match error {
            GetRoutingError::NoSuchBucket => ServerError::NotFound,
            other => ServerError::InternalError(other.to_string()),
        })?;

    let warnings = warnings_for(&state, group_id, rules.iter().map(|rule| &rule.target)).await;
    Ok(Json(BucketRoutingResponse {
        bucket,
        rules: rules.into_iter().map(Into::into).collect(),
        warnings,
    }))
}

#[utoipa::path(
    put,
    path = "/data/buckets/{bucket}/storage/routing",
    tag = "data/storage",
    summary = "Replace a bucket's write routing rules",
    description = r#"Replaces the whole write routing rule set of a bucket with the submitted list.

**Authentication**: realm bearer token with WRITE on the owning group's admin path.

**Behavior**
- The submitted list replaces the whole rule set; an empty list clears it.
- Resolution is most specific first: exact key, then the longest matching `key_prefix` (an empty
  prefix is the bucket default), then the group default, the operator's node rules, and the node's
  default backend.
- Rules steer writes made after the change; stored objects are never moved.
- A target this node cannot serve is stored anyway, because the bucket record replicates to nodes
  that may offer it, and comes back in `warnings`.

**Limits**
- Each rule target sets exactly one of `backend_id` or `class`.
- A `backend_id` must name a backend the group itself registered, so a tenant rule can never name
  an operator's node backend.
- A `class` must be a valid storage class name.
- Two rules may not share the same `key_prefix` and `exact` combination."#,
    params(("bucket" = String, Path, description = "Bucket name as used on the S3 surface, without a leading slash")),
    request_body(
        content = BucketRoutingRequest,
        description = "The complete rule set for this bucket. An empty `key_prefix` is the bucket default, and `exact` false matches by prefix.",
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
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token from another realm, or no WRITE on the group admin path", body = ErrorResponse),
        (status = 404, description = "Bucket not found on this node, or no longer owned by the authorized group", body = ErrorResponse)
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
        PutRoutingOperation::new(bucket.clone(), group_id, rules),
        &state.get_ctx(),
    )
    .await
    .map_err(map_put_error)?;

    let warnings = warnings_for(&state, group_id, stored.iter().map(|rule| &rule.target)).await;
    Ok(Json(BucketRoutingResponse {
        bucket,
        rules: stored.into_iter().map(Into::into).collect(),
        warnings,
    }))
}

#[utoipa::path(
    get,
    path = "/data/groups/{group_id}/storage/routing",
    tag = "data/storage",
    summary = "Read a group's default write target",
    description = r#"Returns the default write target this node holds for a group, if the group has set one.

**Authentication**: realm bearer token with WRITE on that group's admin path.

**Behavior**
- The default applies to every bucket of the group, ranks below that bucket's own rules and above
  the operator's node rules and the node's default backend.
- Node-local read of the replicated group routing record; `default_target` is omitted when no
  default is stored.
- `warnings` is advisory and is recomputed per request from the storage classes this node offers to
  tenants plus the backends the group registered, so it can differ between nodes for the same
  stored default."#,
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
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token from another realm, or no WRITE on the group admin path", body = ErrorResponse)
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

    let record = drive(GroupRoutingOperation::new(group_id), &state.get_ctx())
        .await
        .map_err(|error| ServerError::InternalError(error.to_string()))?;

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
    path = "/data/groups/{group_id}/storage/routing",
    tag = "data/storage",
    summary = "Set or clear a group's default write target",
    description = r#"Replaces the group's default write target, or clears it when no target is submitted.

**Authentication**: realm bearer token with WRITE on that group's admin path.

**Behavior**
- The submitted value replaces the stored default outright; omitting `default_target` or sending it
  as null clears it and returns the group to the operator's node rules and the node default.
- The default is weaker than any matching rule on an individual bucket.
- It steers writes made after the change; stored objects are never moved.
- A target this node cannot serve is stored anyway, because the record replicates to nodes that may
  offer it, and comes back in `warnings`.

**Limits**
- The target sets exactly one of `backend_id` or `class`.
- A `backend_id` must name a backend the group itself registered, so a tenant can never bind an
  operator's node backend.
- A `class` must be a valid storage class name."#,
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
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token from another realm, or no WRITE on the group admin path", body = ErrorResponse)
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
        PutGroupOperation::new(group_id, target, auth.user_id, SystemTime::now()),
        &state.get_ctx(),
    )
    .await
    .map_err(map_group_error)?;

    let target = record.default_target;
    let warnings = warnings_for(&state, group_id, target.iter()).await;
    Ok(Json(GroupRoutingResponse {
        group_id: group_id.to_string(),
        default_target: target.map(Into::into),
        warnings,
    }))
}

#[cfg(test)]
#[path = "routing_tests.rs"]
pub(crate) mod tests;
