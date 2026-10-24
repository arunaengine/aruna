//! Serves the distributed bucket routes: create a set on chosen member nodes, retry its
//! admissions, and read its status from this node's record.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::bucket_permission_path;
use aruna_core::structs::storage::distribution::{
    DataStrategy, DistributionSettings, MemberState, MetadataCoverage, RangeBehaviour, RouteError,
    SetPhase,
};
use aruna_operations::distribution::create::{
    CreateError, CreateInput, CreationReport, LayoutKind, admit_pending, create_set,
};
use aruna_operations::distribution::forward::PublishRefusal;
use aruna_operations::distribution::guard::GuardError;
use aruna_operations::distribution::status::{DistributionStatus, Reach, read_status};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use serde_json::json;
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use super::routing::ensure_group_admin;
use crate::auth::{ValidatedBearer, ensure_permission, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::storage::deletion::bucket_info;
use crate::server::state::ServerState;

#[derive(OpenApi)]
#[openapi()]
pub struct DistributedDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(DistributedDoc::openapi())
        .routes(routes!(create_distributed))
        .routes(routes!(get_distributed))
        .routes(routes!(retry_admission))
        .routes(routes!(get_bucket_distribution))
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LayoutName {
    /// One range; the first node coordinates, every other node keeps a full copy.
    Replica,
    /// Ranges split at `boundaries`, coordinators in turn over `nodes`.
    Sharding,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StrategyName {
    Copy,
    CopyOnUse,
    CacheOnUse,
    Stream,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BehaviourName {
    Passthrough,
    FillFirst,
    ServeAndFill,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct EdgeRequest {
    pub node_id: String,
    /// `copy_on_use`, `cache_on_use` or `stream`.
    pub strategy: StrategyName,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct SettingsRequest {
    /// Seconds between coordinated source refreshes; omitted refreshes only on request.
    #[serde(default)]
    pub refresh_secs: Option<u64>,
    /// Omitted lets each member's strategy decide.
    #[serde(default)]
    pub range_behaviour: Option<BehaviourName>,
    #[serde(default)]
    pub search_index: bool,
    #[serde(default)]
    pub public: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct CreateDistributedRequest {
    /// The bucket name every member's local bucket carries.
    pub bucket: String,
    pub layout: LayoutName,
    /// Nodes that coordinate or replicate ranges, in layout order.
    pub nodes: Vec<String>,
    /// Lower bounds of the second and later ranges, in order (sharding only).
    #[serde(default)]
    pub boundaries: Vec<String>,
    /// Further copies of each range (sharding only).
    #[serde(default)]
    pub replicas: usize,
    /// Members that hold every record and fetch or stream bytes on use.
    #[serde(default)]
    pub edges: Vec<EdgeRequest>,
    #[serde(default)]
    pub settings: SettingsRequest,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct MemberOutcomeResponse {
    pub member_id: String,
    pub node_id: String,
    pub admitted: bool,
    /// Why this attempt failed; omitted when it did not.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct AdmissionResponse {
    pub set_id: String,
    /// Every coordinator and replica is admitted and the bucket serves.
    pub active: bool,
    pub members: Vec<MemberOutcomeResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct MemberResponse {
    pub member_id: String,
    pub node_id: String,
    /// `created`, `catching_up`, `ready`, `leaving` or `failed`.
    pub state: String,
    /// `all` or `assigned`.
    pub coverage: String,
    pub strategy: StrategyName,
    pub admitted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct RangeResponse {
    pub range_id: String,
    /// Inclusive lower key bound.
    pub lower: String,
    /// Exclusive upper key bound; omitted for the end of the key space.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upper: Option<String>,
    pub coordinator: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub coordinator_node: Option<String>,
    pub epoch: u64,
    pub replicas: Vec<String>,
    /// The coordinator is unreachable, so writes to the range fail.
    pub degraded: bool,
    /// When this node last heard from the unreachable coordinator, in Unix milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unreachable_since_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct DistributedResponse {
    pub set_id: String,
    pub bucket: String,
    pub group_id: String,
    /// The node that alone signs configuration changes.
    pub publisher: String,
    pub revision: u64,
    /// `pending` or `active`.
    pub phase: String,
    /// Active, and every coordinator is ready.
    pub serving: bool,
    /// The publisher signed two revisions of one number; configuration changes stopped.
    pub conflicted: bool,
    /// The publisher is unreachable; data service continues, configuration changes wait.
    pub configuration_changes_unavailable: bool,
    /// At least one range has an unreachable coordinator.
    pub degraded: bool,
    pub members: Vec<MemberResponse>,
    pub ranges: Vec<RangeResponse>,
    /// The node glob grant that covers this bucket on every member node.
    pub recommended_grant: String,
}

fn strategy(value: StrategyName) -> DataStrategy {
    match value {
        StrategyName::Copy => DataStrategy::Copy,
        StrategyName::CopyOnUse => DataStrategy::CopyOnUse,
        StrategyName::CacheOnUse => DataStrategy::CacheOnUse,
        StrategyName::Stream => DataStrategy::Stream,
    }
}

fn strategy_name(value: DataStrategy) -> StrategyName {
    match value {
        DataStrategy::Copy => StrategyName::Copy,
        DataStrategy::CopyOnUse => StrategyName::CopyOnUse,
        DataStrategy::CacheOnUse => StrategyName::CacheOnUse,
        DataStrategy::Stream => StrategyName::Stream,
    }
}

fn node(value: &str) -> ServerResult<NodeId> {
    value
        .parse()
        .map_err(|_| ServerError::BadRequestMessage(format!("`{value}` is not a node id")))
}

fn create_input(group_id: Ulid, request: CreateDistributedRequest) -> ServerResult<CreateInput> {
    let nodes = request
        .nodes
        .iter()
        .map(|value| node(value))
        .collect::<ServerResult<Vec<_>>>()?;
    let edges = request
        .edges
        .iter()
        .map(|edge| Ok((node(&edge.node_id)?, strategy(edge.strategy))))
        .collect::<ServerResult<Vec<_>>>()?;
    let layout = match request.layout {
        LayoutName::Replica if request.boundaries.is_empty() && request.replicas == 0 => {
            LayoutKind::Replica
        }
        LayoutName::Replica => {
            return Err(ServerError::BadRequestMessage(
                "a replica layout takes no boundaries or replica count".to_string(),
            ));
        }
        LayoutName::Sharding => LayoutKind::Sharding {
            boundaries: request.boundaries,
            replicas: request.replicas,
        },
    };
    let settings = DistributionSettings {
        refresh_secs: request.settings.refresh_secs,
        range_behaviour: request.settings.range_behaviour.map(|value| match value {
            BehaviourName::Passthrough => RangeBehaviour::Passthrough,
            BehaviourName::FillFirst => RangeBehaviour::FillFirst,
            BehaviourName::ServeAndFill => RangeBehaviour::ServeAndFill,
        }),
        search_index: request.settings.search_index,
        public: request.settings.public,
    };
    Ok(CreateInput {
        bucket: request.bucket,
        owner_group: group_id,
        nodes,
        layout,
        edges,
        settings,
    })
}

fn admission_response(report: CreationReport) -> AdmissionResponse {
    AdmissionResponse {
        set_id: report.set_id.to_string(),
        active: report.active,
        members: report
            .members
            .into_iter()
            .map(|member| MemberOutcomeResponse {
                member_id: member.member_id.to_string(),
                node_id: member.node_id.to_string(),
                admitted: member.admitted,
                error: member.error,
            })
            .collect(),
    }
}

fn state_name(state: MemberState) -> &'static str {
    match state {
        MemberState::Created => "created",
        MemberState::CatchingUp => "catching_up",
        MemberState::Ready => "ready",
        MemberState::Leaving => "leaving",
        MemberState::Failed => "failed",
    }
}

pub(crate) fn status_response(status: DistributionStatus) -> DistributedResponse {
    let members = status
        .members
        .into_iter()
        .map(|member| MemberResponse {
            member_id: member.member_id.to_string(),
            node_id: member.node_id.to_string(),
            state: state_name(member.state).to_string(),
            coverage: match member.coverage {
                MetadataCoverage::All => "all".to_string(),
                MetadataCoverage::Assigned => "assigned".to_string(),
            },
            strategy: strategy_name(member.strategy),
            admitted: member.admitted,
        })
        .collect();
    let ranges = status
        .ranges
        .into_iter()
        .map(|range| RangeResponse {
            range_id: range.range_id.to_string(),
            lower: range.lower,
            upper: range.upper,
            coordinator: range.coordinator.to_string(),
            coordinator_node: range.coordinator_node.map(|node| node.to_string()),
            epoch: range.epoch,
            replicas: range.replicas.iter().map(ToString::to_string).collect(),
            degraded: range.degraded.is_some(),
            unreachable_since_ms: match range.degraded {
                Some(Reach::Unreachable { since_ms }) => since_ms,
                _ => None,
            },
        })
        .collect();
    DistributedResponse {
        set_id: status.set_id.to_string(),
        bucket: status.bucket,
        group_id: status.owner_group.to_string(),
        publisher: status.publisher.to_string(),
        revision: status.revision.number,
        phase: match status.phase {
            SetPhase::Pending => "pending".to_string(),
            SetPhase::Active => "active".to_string(),
        },
        serving: status.serving,
        conflicted: status.conflicted,
        configuration_changes_unavailable: status.configuration_unavailable,
        degraded: status.degraded,
        members,
        ranges,
        recommended_grant: status.recommended_grant,
    }
}

/// A write this member of a distributed bucket may not take now: 503 when a retry or the
/// coordinator can take it, 409 when the bucket state refuses it.
pub(crate) fn map_guard_error(error: GuardError) -> ServerError {
    match error {
        GuardError::NotCoordinator { .. }
        | GuardError::RecordMissing(_)
        | GuardError::Unreadable(_)
        | GuardError::Route(RouteError::Pending(_) | RouteError::Moving(_)) => {
            ServerError::ServiceUnavailableReason(error.to_string())
        }
        GuardError::Route(_) | GuardError::Unbound(_) => ServerError::Conflict(error.to_string()),
        GuardError::Conversion(error) => ServerError::InternalError(error.to_string()),
    }
}

pub(crate) fn map_create_error(error: CreateError) -> ServerError {
    match error {
        CreateError::Invalid(message) => ServerError::BadRequestMessage(message),
        CreateError::Distribution(error) => ServerError::BadRequestMessage(error.to_string()),
        CreateError::Publish(PublishRefusal::Unauthorized) => ServerError::Unauthorized,
        CreateError::Publish(PublishRefusal::Forbidden) => ServerError::Forbidden,
        CreateError::Publish(refusal @ PublishRefusal::Unavailable(_)) => {
            ServerError::ServiceUnavailableReason(refusal.to_string())
        }
        CreateError::Publish(refusal) => ServerError::Conflict(refusal.to_string()),
        CreateError::UnknownSet(_) => ServerError::NotFound,
        CreateError::Conflicted => ServerError::Conflict(error.to_string()),
        CreateError::Status(error) => ServerError::InternalError(error.to_string()),
    }
}

const STATUS_EXAMPLE: &str = r#"{
    "set_id": "01JMPRT0123456789ABCDEFGHJ",
    "bucket": "spire",
    "group_id": "01JABCDEF0123456789ABCDEFG",
    "publisher": "1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978",
    "revision": 4,
    "phase": "active",
    "serving": true,
    "conflicted": false,
    "configuration_changes_unavailable": false,
    "degraded": false,
    "members": [{
        "member_id": "01JMPRT1123456789ABCDEFGHJ",
        "node_id": "1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978",
        "state": "ready",
        "coverage": "all",
        "strategy": "copy",
        "admitted": true
    }],
    "ranges": [{
        "range_id": "01JMPRT2123456789ABCDEFGHJ",
        "lower": "",
        "coordinator": "01JMPRT1123456789ABCDEFGHJ",
        "coordinator_node": "1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978",
        "epoch": 1,
        "replicas": [],
        "degraded": false
    }],
    "recommended_grant": "/AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8/g/01JABCDEF0123456789ABCDEFG/data/*/spire/**"
}"#;

fn status_example() -> serde_json::Value {
    serde_json::from_str(STATUS_EXAMPLE).unwrap_or_default()
}

fn admission_example() -> serde_json::Value {
    json!({
        "set_id": "01JMPRT0123456789ABCDEFGHJ",
        "active": false,
        "members": [
            {
                "member_id": "01JMPRT1123456789ABCDEFGHJ",
                "node_id": "1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978",
                "admitted": true
            },
            {
                "member_id": "01JMPRT3123456789ABCDEFGHJ",
                "node_id": "2f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978",
                "admitted": false,
                "error": "the member node is unreachable or busy; retry"
            }
        ]
    })
}

#[utoipa::path(
    post,
    path = "/data/groups/{group_id}/distributed-buckets",
    tag = "data/storage",
    summary = "Create a distributed bucket",
    description = r#"Creates a distributed bucket of one name on the chosen member nodes, with this node as the publisher of its configuration.

**Authentication**: realm bearer token with WRITE on the group's admin path, and WRITE on the bucket path of every member node. Each member node checks the forwarded token on its own path.

**Behavior**
- This node records the pending set, then asks every member node to create its local bucket
  and admit it. A member node signs its admission with its own key.
- When every coordinator and replica is admitted, the set becomes active and serves. Writes go
  to the coordinator of the key's range; other members forward them.
- A member that fails keeps the set pending. Its error is listed; retry the admission later.
  Admitted members and their buckets are kept, nothing is deleted.
- An existing bucket of the name on a member node fails that member, unless it is this
  member's own bucket from an earlier attempt.
- Grant permissions with node globs such as `/{realm}/g/{group}/data/*/{bucket}/**`: every
  read and write is checked on the node that serves it.

**Limits**
- At most 256 members and 4096 ranges. Members must be realm infrastructure nodes."#,
    params(("group_id" = String, Path, description = "Owner group id")),
    request_body(
        content = CreateDistributedRequest,
        description = "Bucket name, layout and member nodes",
        example = json!({
            "bucket": "spire",
            "layout": "replica",
            "nodes": [
                "1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978",
                "2f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978"
            ]
        })
    ),
    responses(
        (status = 201, description = "The set and each member's admission", body = AdmissionResponse, example = admission_example),
        (status = 400, description = "Invalid layout, bucket name or node", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No WRITE on the group's admin path", body = ErrorResponse),
        (status = 409, description = "The publisher refused the change", body = ErrorResponse),
        (status = 503, description = "Configuration changes unavailable", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn create_distributed(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    Path(group_id): Path<Ulid>,
    Json(request): Json<CreateDistributedRequest>,
) -> ServerResult<(StatusCode, Json<AdmissionResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    ensure_group_admin(&state, &auth, group_id).await?;
    let input = create_input(group_id, request)?;
    let bearer = crate::metadata::forwarded_auth_token(bearer)?;
    let report = create_set(&state.get_ctx(), &auth, bearer, input)
        .await
        .map_err(map_create_error)?;
    Ok((StatusCode::CREATED, Json(admission_response(report))))
}

#[utoipa::path(
    post,
    path = "/data/groups/{group_id}/distributed-buckets/{set_id}/admission",
    tag = "data/storage",
    summary = "Retry a distributed bucket's admissions",
    description = r#"Asks every member the record has not admitted yet to create and admit its local bucket again, then activates the set when it can.

**Authentication**: realm bearer token with WRITE on the group's admin path; member nodes and the publisher check the forwarded token again.

**Behavior**
- Runs on any node that holds the set's record. Admissions and activation go to the publisher.
- A retry finds a member's own bucket from an earlier attempt and admits it once.
- If the publisher is unreachable, the answer is 503 and nothing is queued."#,
    params(
        ("group_id" = String, Path, description = "Owner group id"),
        ("set_id" = String, Path, description = "Distributed bucket set id")
    ),
    responses(
        (status = 200, description = "Each member's admission", body = AdmissionResponse, example = admission_example),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No WRITE on the group's admin path", body = ErrorResponse),
        (status = 404, description = "This node holds no record of the set", body = ErrorResponse),
        (status = 409, description = "The publisher refused the change or the record conflicts", body = ErrorResponse),
        (status = 503, description = "Configuration changes unavailable", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn retry_admission(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    Path((group_id, set_id)): Path<(Ulid, Ulid)>,
) -> ServerResult<Json<AdmissionResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    ensure_group_admin(&state, &auth, group_id).await?;
    owned_status(&state, group_id, set_id).await?;
    let bearer = crate::metadata::forwarded_auth_token(bearer)?;
    let report = admit_pending(&state.get_ctx(), &auth, bearer, set_id)
        .await
        .map_err(map_create_error)?;
    Ok(Json(admission_response(report)))
}

/// The set's status, only when it belongs to `group_id`.
async fn owned_status(
    state: &ServerState,
    group_id: Ulid,
    set_id: Ulid,
) -> ServerResult<DistributionStatus> {
    let status = read_status(&state.get_ctx(), state.get_realm_id(), set_id)
        .await
        .map_err(|error| ServerError::InternalError(error.to_string()))?
        .filter(|status| status.owner_group == group_id)
        .ok_or(ServerError::NotFound)?;
    Ok(status)
}

#[utoipa::path(
    get,
    path = "/data/groups/{group_id}/distributed-buckets/{set_id}",
    tag = "data/storage",
    summary = "Read a distributed bucket's status",
    description = r#"Returns the members, key ranges and service state of a distributed bucket from this node's copy of its record.

**Authentication**: realm bearer token with READ on the bucket path on this node, which a node glob grant covers.

**Behavior**
- `degraded` marks ranges whose coordinator this node has not heard from recently. Writes to
  them fail with a hint; reads continue from copies.
- `configuration_changes_unavailable` means the publisher is unreachable: data service
  continues, but admissions and other configuration changes wait.
- `recommended_grant` is the node glob grant that lets reads that pull from another member
  pass that member's own check."#,
    params(
        ("group_id" = String, Path, description = "Owner group id"),
        ("set_id" = String, Path, description = "Distributed bucket set id")
    ),
    responses(
        (status = 200, description = "The set's status", body = DistributedResponse, example = status_example),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No READ on the bucket path", body = ErrorResponse),
        (status = 404, description = "This node holds no record of the set", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_distributed(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((group_id, set_id)): Path<(Ulid, Ulid)>,
) -> ServerResult<Json<DistributedResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let status = owned_status(&state, group_id, set_id).await?;
    let path = bucket_permission_path(
        state.get_realm_id(),
        group_id,
        state.get_node_id(),
        &status.bucket,
    );
    ensure_permission(&state, &auth, path, Permission::READ).await?;
    Ok(Json(status_response(status)))
}

#[utoipa::path(
    get,
    path = "/data/buckets/{bucket}/distribution",
    tag = "data/storage",
    summary = "Read the distributed bucket a bucket belongs to",
    description = r#"Returns the status of the distributed bucket this node's bucket is a member of.

**Authentication**: realm bearer token with READ on the bucket.

**Behavior**
- An ordinary bucket answers 404.
- The fields are those of the distributed bucket status route."#,
    params(("bucket" = String, Path, description = "Bucket name as used on the S3 surface, without a leading slash")),
    responses(
        (status = 200, description = "The set's status", body = DistributedResponse, example = status_example),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No READ on the bucket", body = ErrorResponse),
        (status = 404, description = "Bucket not found, ordinary, or its record not here yet", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_bucket_distribution(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(bucket): Path<String>,
) -> ServerResult<Json<DistributedResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let info = bucket_info(&state, &bucket).await?;
    let path = bucket_permission_path(
        state.get_realm_id(),
        info.group_id,
        state.get_node_id(),
        &bucket,
    );
    ensure_permission(&state, &auth, path, Permission::READ).await?;
    let binding = info.distribution.ok_or(ServerError::NotFound)?;
    let status = owned_status(&state, info.group_id, binding.set_id).await?;
    Ok(Json(status_response(status)))
}

#[cfg(test)]
#[path = "distributed_tests.rs"]
mod tests;
