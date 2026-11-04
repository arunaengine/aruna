//! Routes that show group statistics to group and realm administrators without any data access.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::{ensure_permission, parse_group_id, require_unrestricted_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::access::admin_tokens::TOKEN_PAGE;
use crate::routes::access::groups::{PaginationParams, ensure_group_admin, load_group};
use crate::routes::info::{GroupQuotaStatus, distinct_buckets, load_realm_usage};
use crate::server::state::ServerState;
use aruna_core::UserId;
use aruna_core::jobs::RunningJobView;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::time::{unix_timestamp_millis, unix_timestamp_secs};
use aruna_core::types::GroupId;
use aruna_operations::driver::drive;
use aruna_operations::groups::admin_check::{AdminCheckOperation, realm_groups_path};
use aruna_operations::groups::list_groups::ListGroupOperation;
use aruna_operations::groups::tokens::list::{GroupTokensConfig, ListTokensOperation};
use aruna_operations::groups::tokens::{GroupTokensError, TokenStatus};
use aruna_operations::jobs::recent::RecentJobsOperation;
use aruna_operations::node::usage_stats::RealmUsageScope;
use aruna_operations::realm::get_config::GetConfigOperation;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::warn;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

/// Jobs that finished within this window count as recent.
const RECENT_MS: u64 = 24 * 60 * 60 * 1000;

#[derive(OpenApi)]
pub struct GroupStatsDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(GroupStatsDoc::openapi())
        .routes(routes!(get_group_statistics))
        .routes(routes!(list_group_statistics))
}

/// Counts and sizes only: no bucket, object, dataset or metadata name.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct GroupStatsResponse {
    pub group_id: String,
    pub display_name: String,
    pub members: u64,
    pub admins: u64,
    /// Bucket count with a distributed bucket counted once per member node.
    pub buckets: u64,
    /// `buckets` with each distributed bucket counted once; omitted when unknown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub distinct_buckets: Option<u64>,
    pub objects: u64,
    /// Realm-wide bytes of the objects the group stored itself.
    pub logical_bytes: u64,
    /// Realm-wide bytes of the objects the group references from elsewhere.
    pub referenced_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quota: Option<GroupQuotaStatus>,
    /// Jobs of the group this node runs now.
    pub running_jobs: u64,
    /// Jobs of the group this node finished in the last 24 hours; omitted when unknown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recent_jobs: Option<u64>,
    /// Active tokens of the group this node holds; only on the single group route.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_tokens: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct GroupStatsList {
    pub groups: Vec<GroupStatsResponse>,
}

/// Inputs shared by every group of one request.
struct StatsContext {
    config: Option<RealmConfigDocument>,
    running: Vec<RunningJobView>,
    recent: Option<HashMap<GroupId, u64>>,
}

async fn stats_context(state: &ServerState) -> StatsContext {
    let config = drive(
        GetConfigOperation::new(state.get_realm_id()),
        &state.get_ctx(),
    )
    .await
    .inspect_err(|error| warn!(error = %error, "realm config unavailable for group statistics"))
    .ok();
    let since = unix_timestamp_millis().saturating_sub(RECENT_MS);
    let recent = drive(RecentJobsOperation::new(since), &state.get_ctx())
        .await
        .inspect_err(|error| warn!(error = %error, "recent jobs unavailable for group statistics"))
        .ok();
    StatsContext {
        config,
        running: state.jobs_runtime().running_list(),
        recent,
    }
}

async fn group_stats(
    state: &ServerState,
    context: &StatsContext,
    group: Group,
    auth_doc: GroupAuthorizationDocument,
) -> ServerResult<GroupStatsResponse> {
    let realm_id = state.get_realm_id();
    let group_id = group.group_id;
    let members: HashSet<UserId> = auth_doc
        .roles
        .values()
        .flat_map(|role| role.assigned_users.iter().copied())
        .filter(|user| !user.is_nil())
        .collect();
    let admins = members
        .iter()
        .filter(|user| auth_doc.is_admin(realm_id, **user))
        .count();
    let realm = load_realm_usage(state, RealmUsageScope::Group(group_id)).await?;
    let quota = context.config.as_ref().map(|config| {
        let replicas = match config.replica_quota.counts(&group_id) {
            true => realm.replicas.logical_bytes,
            false => 0,
        };
        let used = realm.counters.logical_bytes.saturating_add(replicas);
        GroupQuotaStatus::resolve(&config.quota, &group_id, used)
    });
    let buckets = realm.counters.buckets;
    let running = context
        .running
        .iter()
        .filter(|job| job.group_id == Some(group_id))
        .count();
    Ok(GroupStatsResponse {
        group_id: group_id.to_string(),
        display_name: group.display_name,
        members: members.len() as u64,
        admins: admins as u64,
        buckets,
        distinct_buckets: distinct_buckets(state, Some(group_id), buckets).await,
        objects: realm.counters.objects,
        logical_bytes: realm.counters.logical_bytes,
        referenced_bytes: realm.counters.referenced_bytes,
        quota,
        running_jobs: running as u64,
        recent_jobs: context
            .recent
            .as_ref()
            .map(|recent| recent.get(&group_id).copied().unwrap_or(0)),
        active_tokens: None,
    })
}

#[utoipa::path(
    get,
    path = "/access/groups/{id}/statistics",
    tag = "access/groups",
    summary = "Read a group's statistics",
    description = r#"Returns counts and sizes for one group, without naming any of its contents.

**Authentication**: realm bearer token without path restrictions, with WRITE on the group's
`/{realm}/g/{group}/admin` or on the realm's `/{realm}/admin/groups`. Membership is not needed.

**Behavior**
- Members, admins, buckets, objects, bytes, quota use and job counts only. No bucket, object,
  dataset or metadata name is returned, and the route grants no read access to the group's data.
- Bucket, object and byte figures are realm-wide totals from the usage summaries the nodes
  publish, so they trail recent writes. Quota use counts like `GET /access/groups/{id}/usage`.
- `running_jobs`, `recent_jobs` and `active_tokens` count what this node holds."#,
    params(("id" = String, Path, description = "Group id as a 26-character ULID")),
    responses(
        (status = 200, description = "The group's statistics", body = GroupStatsResponse,
            example = json!({
                "group_id": "01JGRP00123456789ABCDEFGHJ",
                "display_name": "Genomics lab",
                "members": 12,
                "admins": 2,
                "buckets": 5,
                "distinct_buckets": 4,
                "objects": 2048,
                "logical_bytes": 152882105100_i64,
                "referenced_bytes": 1048576,
                "quota": {
                    "quota_bytes": 214748364800_i64,
                    "ceiling_bytes": 236223201280_i64,
                    "warn_threshold_percent": 80,
                    "warning": false
                },
                "running_jobs": 1,
                "recent_jobs": 7,
                "active_tokens": 3
            })),
        (status = 400, description = "The group id is not a ULID", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token is path-restricted or belongs to another realm, or the caller does not administer the group", body = ErrorResponse),
        (status = 404, description = "No such group on this node", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_group_statistics(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(group_id): Path<String>,
) -> ServerResult<(StatusCode, Json<GroupStatsResponse>)> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let group_id = parse_group_id(&group_id)?;
    ensure_group_admin(&state, &auth, group_id).await?;
    let allowed = drive(
        AdminCheckOperation::new(auth.clone(), group_id),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| ServerError::InternalError(error.to_string()))?;
    if !allowed {
        return Err(ServerError::Forbidden);
    }
    let (group, auth_doc) = load_group(&state, group_id).await?;
    let context = stats_context(&state).await;
    let mut response = group_stats(&state, &context, group, auth_doc).await?;
    response.active_tokens = active_tokens(&state, auth, group_id)
        .await
        .inspect_err(|error| warn!(error = %error, "group tokens unavailable for statistics"))
        .ok();
    Ok((StatusCode::OK, Json(response)))
}

/// Counts page by page, so only one page of tokens is held at a time.
async fn active_tokens(
    state: &ServerState,
    auth: AuthContext,
    group_id: GroupId,
) -> Result<u64, GroupTokensError> {
    let (now, mut cursor, mut active) = (unix_timestamp_secs(), None, 0);
    loop {
        let page = drive(
            ListTokensOperation::new(GroupTokensConfig {
                auth_context: auth.clone(),
                group_id,
                now,
                cursor,
                limit: TOKEN_PAGE.1 as usize,
            }),
            &state.get_ctx(),
        )
        .await?;
        let tokens = page.tokens.iter();
        active += tokens.filter(|t| t.status == TokenStatus::Active).count() as u64;
        match page.next {
            Some(next) => cursor = Some(next),
            None => return Ok(active),
        }
    }
}

#[utoipa::path(
    get,
    path = "/system/groups/statistics",
    tag = "access/groups",
    summary = "Read the statistics of every group",
    description = r#"Returns the statistics of every group of the realm, for realm administrators.

**Authentication**: realm bearer token without path restrictions and with WRITE on the realm's
`/{realm}/admin/groups`. Group membership is not needed and grants nothing here.

**Behavior**
- Each entry has the fields of `GET /access/groups/{id}/statistics` except `active_tokens`.
- No bucket, object, dataset or metadata name is returned, and the route grants no read access to
  any group's data.

**Limits**
- Groups are listed in group id order, a page at a time: `offset` skips groups and `limit` sets
  the page size. A page shorter than `limit` is the last one."#,
    params(
        ("limit" = Option<u32>, Query, description = "Maximum number of groups to return; defaults to 100 and is clamped to the range 1-1000"),
        ("offset" = Option<u32>, Query, description = "Number of groups to skip from the start of the directory; defaults to 0")
    ),
    responses(
        (status = 200, description = "Every group's statistics", body = GroupStatsList,
            example = json!({
                "groups": [{
                    "group_id": "01JGRP00123456789ABCDEFGHJ",
                    "display_name": "Genomics lab",
                    "members": 12,
                    "admins": 2,
                    "buckets": 5,
                    "distinct_buckets": 4,
                    "objects": 2048,
                    "logical_bytes": 152882105100_i64,
                    "referenced_bytes": 1048576,
                    "running_jobs": 1,
                    "recent_jobs": 7
                }]
            })),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token is path-restricted or belongs to another realm, or the caller is not a realm groups administrator", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_group_statistics(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Query(page): Query<PaginationParams>,
) -> ServerResult<(StatusCode, Json<GroupStatsList>)> {
    let auth = require_unrestricted_auth(&state, auth)?;
    ensure_permission(&state, &auth, realm_groups_path(&auth), Permission::WRITE).await?;
    let limit = page.limit_or(100).clamp(1, 1_000) as usize;
    let offset = page.offset_or(0) as usize;
    let groups = ListGroupOperation::with_pagination(limit, offset);
    let groups = drive(groups, &state.get_ctx())
        .await
        .map_err(|error| ServerError::InternalError(error.to_string()))?;
    let context = stats_context(&state).await;
    let mut response = GroupStatsList { groups: Vec::new() };
    for group in groups {
        let (group, auth_doc) = match load_group(&state, group.group_id).await {
            Ok(loaded) => loaded,
            Err(ServerError::NotFound) => continue,
            Err(error) => return Err(error),
        };
        response
            .groups
            .push(group_stats(&state, &context, group, auth_doc).await?);
    }
    Ok((StatusCode::OK, Json(response)))
}
