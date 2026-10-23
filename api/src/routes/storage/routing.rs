//! Serves the bucket and group routing routes that steer writes to a backend or class.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::{parse_group_id, permission_granted, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server::state::ServerState;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::{BackendRef, bucket_permission_path};
use aruna_core::structs::storage::routing::{RoutingTarget, StorageRoutingRule, target_warnings};
use aruna_operations::driver::{drive, node_routing};
use aruna_operations::groups::backends::query::GetBackendOperation;
use aruna_operations::groups::bucket_jobs::{ActiveJob, JobKind};
use aruna_operations::groups::default_rule::PutDefaultOperation;
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
        .routes(routes!(list_group_buckets))
}

/// Listing pages one bucket listing reads past while every bucket in them is hidden.
const HIDDEN_PAGES: usize = 8;

#[derive(Debug, Clone, Deserialize, utoipa::IntoParams)]
pub struct BucketPageQuery {
    /// Buckets per page, 1 to 500; 200 when omitted.
    pub limit: Option<usize>,
    /// The `next` of the previous page.
    pub after: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct BucketStorageResponse {
    pub bucket: String,
    /// The group storage; `null` is Aruna storage on this node.
    pub backend_id: Option<String>,
    /// The S3 bucket or container the files go to, where known.
    pub provider_bucket: Option<String>,
    /// The storage folder of a whole-bucket link.
    pub folder: Option<String>,
    /// Aruna names the files in the storage's shared bucket or container.
    pub shared: bool,
    /// The rule that links the whole bucket.
    pub import_id: Option<String>,
    pub write_pattern: Option<String>,
    /// The job that runs or is paused on the bucket; absent while none does.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job: Option<BucketJob>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BucketJobKind {
    Rename,
    Move,
    Compression,
    Encryption,
    Duplicates,
    KeyMove,
    Check,
    Purge,
    Import,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BucketJobState {
    Running,
    Paused,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct BucketJob {
    pub kind: BucketJobKind,
    pub state: BucketJobState,
    /// Items finished so far, changed or left as they are.
    pub done: u64,
    pub failed: u64,
    /// The files a rename planned to rename; absent when unknown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total: Option<u64>,
}

impl From<ActiveJob> for BucketJob {
    fn from(job: ActiveJob) -> Self {
        let kind = match job.kind {
            JobKind::Rename => BucketJobKind::Rename,
            JobKind::Move => BucketJobKind::Move,
            JobKind::Compression => BucketJobKind::Compression,
            JobKind::Encryption => BucketJobKind::Encryption,
            JobKind::Duplicates => BucketJobKind::Duplicates,
            JobKind::KeyMove => BucketJobKind::KeyMove,
            JobKind::Check => BucketJobKind::Check,
            JobKind::Purge => BucketJobKind::Purge,
            JobKind::Import => BucketJobKind::Import,
        };
        let state = match job.paused {
            true => BucketJobState::Paused,
            false => BucketJobState::Running,
        };
        Self {
            kind,
            state,
            done: job.done,
            failed: job.failed,
            total: job.total,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct BucketStoragePage {
    pub buckets: Vec<BucketStorageResponse>,
    pub next: Option<String>,
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
    /// Bucket on a target storage without a fixed bucket that receives the uploads; found or
    /// created first. Omitted keeps the storage's stored default bucket.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bucket: Option<String>,
    /// Gives each new bucket its own provider bucket, like `aruna-{group}-{bucket}`, on a
    /// target storage without a fixed bucket. Empty clears it; omitted keeps the stored one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bucket_template: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct GroupRoutingResponse {
    pub group_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_target: Option<RoutingTargetRequest>,
    /// The shared bucket of the target storage, if it has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bucket: Option<String>,
    /// The provider bucket template of the target storage for new buckets, if set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bucket_template: Option<String>,
    pub warnings: Vec<String>,
}

/// The shared bucket and the bucket template of a group storage target.
async fn target_naming(
    state: &ServerState,
    group_id: Ulid,
    target: Option<&RoutingTarget>,
) -> ServerResult<(Option<String>, Option<String>)> {
    let Some(RoutingTarget::Backend(BackendRef::Group(backend_id))) = target else {
        return Ok((None, None));
    };
    let record = drive(GetBackendOperation::new(*backend_id), &state.get_ctx())
        .await
        .map_err(|error| ServerError::InternalError(error.to_string()))?;
    let record = record.filter(|record| record.group_id == group_id);
    Ok(record.map_or((None, None), |record| {
        let bucket = record.default_bucket().map(str::to_string);
        (bucket, record.bucket_template())
    }))
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
        PutGroupError::Invalid(message) => ServerError::BadRequestMessage(message),
        PutGroupError::Unusable(error) => ServerError::BadRequestMessage(error.reason()),
        PutGroupError::Conflict(message) => ServerError::Conflict(message),
        PutGroupError::GroupWrite(error) => ServerError::Conflict(error.to_string()),
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
- Node-local read of this node's bucket record. A bucket with the same name on another node is
  separate and keeps its own rules.
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
- Rules steer writes made after the change. `POST /data/buckets/{bucket}/storage/move` moves
  stored objects to where the rules send them now.
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
    let (bucket, bucket_template) = target_naming(&state, group_id, target.as_ref()).await?;
    Ok(Json(GroupRoutingResponse {
        group_id: group_id.to_string(),
        default_target: target.map(Into::into),
        bucket,
        bucket_template,
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
- It steers writes made after the change. `POST /data/buckets/{bucket}/storage/move` moves
  stored objects of a bucket to where the rules send them now.
- A target this node cannot serve is stored anyway, because the record replicates to nodes that may
  offer it, and comes back in `warnings`.
- A group storage without a fixed bucket takes Aruna's own layout in `bucket`. The bucket is found or
  created and tested with a write and a delete before anything is stored; the storage then lists it
  as `default_bucket` in its `public_config`. A later request without `bucket` keeps that bucket.
- `bucket_template`, like `aruna-{group}-{bucket}`, gives each bucket created later its own
  provider bucket on that storage, linked whole with the naming `{dir}{name}`; `{group}` is the
  group's name in lowercase. Existing buckets stay where they are. An empty template clears it;
  without it the storage keeps its stored template. With a template no shared `bucket` is
  needed, as long as every bucket the group already has on this node has its own provider
  bucket; else 409, since those buckets would have no storage left. The storage record keeps it as `bucket_template`, and both responses return it with
  the shared `bucket`. A template that gives no valid S3 bucket name for a new bucket, or a
  provider bucket another link uses, refuses the bucket's creation.
- Clearing the target, or naming a class, returns new uploads to Aruna storage; files already on
  the group's storage stay there.

**Limits**
- The target sets exactly one of `backend_id` or `class`.
- A `backend_id` must name a backend the group itself registered, so a tenant can never bind an
  operator's node backend.
- A `class` must be a valid storage class name.
- `bucket` is only accepted with a `backend_id` of a storage without a fixed bucket. It follows the
  S3 naming rules and must not be a bucket a linked folder of that storage uses.
- `bucket_template` is only accepted like `bucket`. It must contain `{bucket}` and give S3 bucket
  names."#,
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
        (status = 400, description = "The path segment is not a valid group ULID, the target is invalid or names a backend the group does not own, or the bucket is invalid or cannot be used", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token from another realm, or no WRITE on the group admin path", body = ErrorResponse),
        (status = 409, description = "A linked folder uses the bucket, a bucket of the group would have no storage without a shared bucket, or the group is being deleted", body = ErrorResponse)
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

    let now = SystemTime::now();
    let naming = request.bucket.is_some() || request.bucket_template.is_some();
    let record = match (target, request.bucket) {
        (Some(RoutingTarget::Backend(BackendRef::Group(backend_id))), bucket) => {
            let operation =
                PutDefaultOperation::new(group_id, backend_id, bucket, auth.user_id, now)
                    .with_template(request.bucket_template);
            drive(operation, &state.get_ctx()).await
        }
        _ if naming => {
            return Err(ServerError::BadRequestMessage(
                "A bucket or template needs one of the group's storages as target.".to_string(),
            ));
        }
        (target, _) => {
            let operation = PutGroupOperation::new(group_id, target, auth.user_id, now);
            drive(operation, &state.get_ctx()).await
        }
    }
    .map_err(map_group_error)?;

    let target = record.default_target;
    let warnings = warnings_for(&state, group_id, target.iter()).await;
    let (bucket, bucket_template) = target_naming(&state, group_id, target.as_ref()).await?;
    Ok(Json(GroupRoutingResponse {
        group_id: group_id.to_string(),
        default_target: target.map(Into::into),
        bucket,
        bucket_template,
        warnings,
    }))
}

#[utoipa::path(
    get,
    path = "/data/groups/{group_id}/storage/buckets",
    tag = "data/storage",
    summary = "List a group's buckets with their storage",
    description = r#"Lists one page of the group's buckets on this node in name order, each with the storage its new uploads go to.

**Authentication**: realm bearer token with WRITE on that group's admin path.

**Behavior**
- A bucket linked whole by a backend import names that rule in `import_id`, its storage folder in
  `folder` and its S3 bucket in `provider_bucket`.
- Else the bucket's own rule for all its keys, then the group default, decides: a group storage
  stores the files in its shared bucket or container (`shared`), and `backend_id` `null` means
  Aruna storage on this node.
- Rules for single folders of a bucket are not shown, nor buckets the caller may not read; `next`
  names only a returned bucket. 8 listing pages of only hidden buckets answer 409.
- `job` is the job running or paused on the bucket, absent while none is. Of several it shows the
  first of: a storage move (`rename`, `move`) or `compression` change, a `key_move` or `duplicates`
  cleanup, an `encryption` change, a storage `check`, a `purge`, an `import` run. An aborted rename
  stays `running` until its move ends.
- `done` counts items finished: versions or keys changed or left as they are, versions checked or
  removed, files imported or skipped. A purge counts no `failed`.
- `total` is the planned count of files a `rename` renames, stored at its start; absent otherwise.
  A `rename` counts in `done` only the files it gave a new name or moved into the folder.

**Limits**
- 1 to 500 buckets per page, 200 by default; pass `next` as `after` for the next page.
- A group with more than 10,000 links and mapping rules is refused rather than listed in part.
- An encrypted or compressed bucket shows the storage its uploads use, not its link."#,
    params(
        ("group_id" = String, Path, description = "Group id as a 26-character ULID"),
        BucketPageQuery
    ),
    responses(
        (status = 200, description = "One page of buckets", body = BucketStoragePage, example = json!({
            "buckets": [{
                "bucket": "genes", "backend_id": "01JBCKND0123456789ABCDEFGH",
                "provider_bucket": "genes", "folder": "genes/", "shared": false,
                "import_id": "01JMPRT0123456789ABCDEFGHJ", "write_pattern": "{dir}{name}",
                "job": {
                    "kind": "rename", "state": "paused", "done": 1200, "failed": 3, "total": 4000
                }
            }],
            "next": null
        })),
        (status = 400, description = "Invalid group id or limit", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token from another realm, or no WRITE on the group admin path", body = ErrorResponse),
        (status = 409, description = "Too many buckets in a row are hidden from the caller", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_group_buckets(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(group_id): Path<String>,
    axum::extract::Query(page): axum::extract::Query<BucketPageQuery>,
) -> ServerResult<Json<BucketStoragePage>> {
    use aruna_operations::groups::bucket_storage::list_bucket_storage;
    use aruna_operations::s3::bucket::list::bucket_cursor;
    let auth = require_realm_auth(&state, auth)?;
    let group_id = parse_group_id(&group_id)?;
    ensure_group_admin(&state, &auth, group_id).await?;
    let limit = page.limit.unwrap_or(200);
    if !(1..=500).contains(&limit) {
        return Err(ServerError::BadRequestMessage(
            "limit takes 1 to 500".to_string(),
        ));
    }
    let id = |id: Option<Ulid>| id.map(|id| id.to_string());
    // Buckets the caller may not read are left out, and `next` names only a returned bucket.
    let (mut readable, mut after, mut more) = (Vec::new(), page.after, false);
    for _ in 0..HIDDEN_PAGES {
        let wanted = (after.take(), limit - readable.len());
        let (buckets, token) = list_bucket_storage(&state.get_ctx(), group_id, wanted)
            .await
            .map_err(ServerError::InternalError)?;
        for found in buckets {
            let path = bucket_permission_path(
                state.get_realm_id(),
                group_id,
                state.get_node_id(),
                &found.bucket,
            );
            if permission_granted(&state, &auth, path, Permission::READ).await? {
                readable.push(found);
            }
        }
        (more, after) = (token.is_some(), token);
        if !more || readable.len() == limit {
            break;
        }
    }
    if more && readable.is_empty() {
        return Err(ServerError::Conflict(
            "too many buckets in a row are hidden from the caller".to_string(),
        ));
    }
    let next = match (more, readable.last()) {
        (true, Some(last)) => Some(
            bucket_cursor(&last.bucket)
                .map_err(|error| ServerError::InternalError(error.to_string()))?,
        ),
        _ => None,
    };
    let buckets = readable
        .into_iter()
        .map(|found| BucketStorageResponse {
            bucket: found.bucket,
            backend_id: id(found.backend_id),
            provider_bucket: found.provider_bucket,
            folder: found.folder,
            shared: found.shared,
            import_id: id(found.import_id),
            write_pattern: found.write_pattern,
            job: found.job.map(Into::into),
        })
        .collect();
    Ok(Json(BucketStoragePage { buckets, next }))
}

#[cfg(test)]
#[path = "routing_tests.rs"]
pub(crate) mod tests;
