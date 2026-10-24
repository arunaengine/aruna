//! Serves the distributed bucket routes: read a set's status from this node's record.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::bucket_permission_path;
use aruna_core::structs::storage::distribution::{
    DataStrategy, MemberState, MetadataCoverage, SetPhase,
};
use aruna_operations::distribution::status::{DistributionStatus, Reach, read_status};
use axum::extract::{Path, State};
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::auth::{ensure_permission, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::storage::deletion::bucket_info;
use crate::server::state::ServerState;

#[derive(OpenApi)]
#[openapi()]
pub struct DistributedDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(DistributedDoc::openapi())
        .routes(routes!(get_distributed))
        .routes(routes!(get_bucket_distribution))
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StrategyName {
    Copy,
    CopyOnUse,
    CacheOnUse,
    Stream,
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

fn strategy_name(value: DataStrategy) -> StrategyName {
    match value {
        DataStrategy::Copy => StrategyName::Copy,
        DataStrategy::CopyOnUse => StrategyName::CopyOnUse,
        DataStrategy::CacheOnUse => StrategyName::CacheOnUse,
        DataStrategy::Stream => StrategyName::Stream,
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
