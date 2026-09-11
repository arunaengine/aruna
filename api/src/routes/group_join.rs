use crate::auth::{ensure_permission, require_unrestricted_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server_state::ServerState;
use aruna_core::join_request::{JoinDecisionKind, JoinRequestState};
use aruna_core::structs::{Actor, AuthContext, Permission};
use aruna_operations::auth::request_policy::{
    PolicyEnforcementError, PolicyRequestExtras, enforce_policies, policy_request_with,
};
use aruna_operations::driver::drive;
use aruna_operations::groups::join_request::{
    GroupJoinError, GroupJoinInput, GroupJoinOperation, JoinAction,
};
use aruna_operations::groups::list_requests::{
    ListJoinRequestsError, ListJoinRequestsInput, ListJoinRequestsOperation,
};
use aruna_operations::users::resolve_users::{ResolveUsersInput, ResolveUsersOperation};
use axum::extract::{Path, Query, State};
use axum::{Extension, Json};
use http::StatusCode;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use ulid::Ulid;
use utoipa::ToSchema;
use utoipa_axum::{router::OpenApiRouter, routes};

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new()
        .routes(routes!(submit_join, list_joins))
        .routes(routes!(withdraw_join))
        .routes(routes!(decide_join))
        .routes(routes!(own_joins))
}

#[derive(Deserialize, ToSchema)]
pub struct CreateJoinRequest {
    pub message: Option<String>,
}

#[derive(Deserialize, ToSchema)]
pub struct DecideJoinRequest {
    pub approve: bool,
    #[serde(default)]
    pub role_ids: BTreeSet<Ulid>,
    pub reason: Option<String>,
}

#[derive(Deserialize)]
pub struct JoinQuery {
    pub start_after: Option<String>,
    pub limit: Option<usize>,
    pub status: Option<String>,
}

#[derive(Serialize, ToSchema)]
pub struct JoinResponse {
    pub request_id: String,
    pub group_id: String,
    pub user_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_name: Option<String>,
    pub message: Option<String>,
    pub status: String,
    pub decided_by: Option<String>,
    pub decision_reason: Option<String>,
    pub created_at: String,
    pub decided_at: Option<String>,
}

#[derive(Serialize, ToSchema)]
pub struct JoinPage {
    pub requests: Vec<JoinResponse>,
    pub next_start_after: Option<String>,
}

#[derive(Serialize, ToSchema)]
pub struct JoinDecisionResponse {
    pub request: JoinResponse,
}

fn timestamp(value: u64) -> ServerResult<String> {
    i64::try_from(value)
        .ok()
        .and_then(chrono::DateTime::from_timestamp_millis)
        .map(|value| value.to_rfc3339())
        .ok_or_else(|| ServerError::InternalError("invalid membership request timestamp".into()))
}

fn response(entry: JoinRequestState) -> ServerResult<JoinResponse> {
    let status = match entry.decision.as_ref().map(|decision| decision.kind) {
        None => "pending",
        Some(JoinDecisionKind::Approved) => "approved",
        Some(JoinDecisionKind::Denied) => "denied",
        Some(JoinDecisionKind::Withdrawn) => "withdrawn",
    };
    Ok(JoinResponse {
        request_id: entry.request.request_id.to_string(),
        group_id: entry.request.group_id.to_string(),
        user_id: entry.request.user_id.to_string(),
        user_name: None,
        message: entry.request.message,
        status: status.into(),
        created_at: timestamp(entry.request.created_at)?,
        decided_by: entry
            .decision
            .as_ref()
            .map(|decision| decision.decided_by.to_string()),
        decision_reason: entry
            .decision
            .as_ref()
            .and_then(|decision| decision.reason.clone()),
        decided_at: entry
            .decision
            .as_ref()
            .map(|decision| timestamp(decision.decided_at))
            .transpose()?,
    })
}

fn map_error(error: GroupJoinError) -> ServerError {
    match error {
        GroupJoinError::Unauthorized => ServerError::Forbidden,
        GroupJoinError::NotFound => ServerError::NotFound,
        GroupJoinError::Conflict => ServerError::Conflict(error.to_string()),
        GroupJoinError::Storage(aruna_core::errors::StorageError::TransactionConflict)
        | GroupJoinError::PlacementFenced => {
            ServerError::Conflict("concurrent membership update; refresh and retry".into())
        }
        GroupJoinError::InvalidMessage | GroupJoinError::InvalidRoles => ServerError::BadRequest,
        other => ServerError::InternalError(other.to_string()),
    }
}

async fn authorize_join(
    state: &ServerState,
    auth: &AuthContext,
    group_id: Option<Ulid>,
    permission: Permission,
) -> ServerResult<()> {
    let path = match group_id {
        Some(group_id) => format!(
            "/{}/g/{group_id}/admin/join-requests/{}",
            auth.realm_id, auth.user_id
        ),
        None => format!("/{}/admin/u/{}/join-requests", auth.realm_id, auth.user_id),
    };
    enforce_policies(
        &state.get_ctx(),
        state.get_realm_id(),
        &policy_request_with(&path, &permission, Some(auth), PolicyRequestExtras::rest()),
    )
    .await
    .map_err(|error| match error {
        PolicyEnforcementError::Denied { .. } => ServerError::Forbidden,
        other => ServerError::InternalError(other.to_string()),
    })
}

async fn mutate(
    state: &ServerState,
    auth: AuthContext,
    group_id: Ulid,
    action: JoinAction,
) -> ServerResult<JoinResponse> {
    super::groups::refuse_group_edit(state).await?;
    response(
        drive(
            GroupJoinOperation::new(GroupJoinInput {
                actor: Actor {
                    node_id: state.get_node_id(),
                    user_id: auth.user_id,
                    realm_id: auth.realm_id,
                },
                auth,
                group_id,
                action,
                now_ms: aruna_core::util::unix_timestamp_millis(),
            }),
            &state.get_ctx(),
        )
        .await
        .map_err(map_error)?,
    )
}

async fn list(
    state: &ServerState,
    auth: AuthContext,
    group_id: Option<Ulid>,
    query: JoinQuery,
) -> ServerResult<JoinPage> {
    if query
        .status
        .as_deref()
        .is_some_and(|status| status != "pending")
    {
        return Err(ServerError::BadRequest);
    }
    let cursor = query
        .start_after
        .as_deref()
        .map(|cursor| {
            let (group, request) = cursor.split_once(':').ok_or(ServerError::BadRequest)?;
            let group = group.parse::<Ulid>().map_err(|_| ServerError::BadRequest)?;
            if group_id.is_some_and(|expected| group != expected) {
                return Err(ServerError::BadRequest);
            }
            Ok((
                group,
                request
                    .parse::<Ulid>()
                    .map_err(|_| ServerError::BadRequest)?,
            ))
        })
        .transpose()?;
    let page = drive(
        ListJoinRequestsOperation::new(ListJoinRequestsInput {
            auth,
            group_id,
            pending_only: query.status.is_some(),
            start_after: cursor,
            limit: query.limit.unwrap_or(100),
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| match error {
        ListJoinRequestsError::Unauthorized => ServerError::Forbidden,
        other => ServerError::InternalError(other.to_string()),
    })?;
    let names = drive(
        ResolveUsersOperation::new(ResolveUsersInput {
            realm_id: state.get_realm_id(),
            user_ids: page
                .requests
                .iter()
                .map(|entry| entry.request.user_id)
                .collect(),
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| ServerError::InternalError(error.to_string()))?
    .users
    .into_iter()
    .map(|user| (user.user_id.to_string(), user.name))
    .collect::<HashMap<_, _>>();
    let requests = page
        .requests
        .into_iter()
        .map(|entry| {
            let mut entry = response(entry)?;
            entry.user_name = names.get(&entry.user_id).cloned();
            Ok(entry)
        })
        .collect::<ServerResult<_>>()?;
    Ok(JoinPage {
        requests,
        next_start_after: page
            .next_start_after
            .map(|(group, request)| format!("{group}:{request}")),
    })
}

#[utoipa::path(post, path = "/access/groups/{id}/join-requests", tag = "access/groups",
    summary = "Request group membership",
    description = r#"Requests group membership for the calling user.

**Authentication**: unrestricted realm bearer token. Realm and group request policies may deny.

**Behavior**
- Returns an existing pending request unchanged; existing members receive 409.
- Commits the request and replicates it as a signed group operation.
- Only group membership administrators may approve or deny the request.
- An optional message is limited to 2000 bytes."#,
    params(("id" = Ulid, Path, description = "Group id")),
    request_body(content = CreateJoinRequest, example = json!({"message": "I would like to collaborate."})),
    responses((status = 201, body = JoinResponse, description = "Pending request", example = json!({"request_id":"01JABCDEF0123456789ABCDEFG","group_id":"01JABCDEF0123456789ABCDEFG","user_id":"01JABCDEF0123456789ABCDEFG@YXJ1bmEtZXhhbXBsZS1yZWFsbS0wMDAwMDAwMDAwMDA","message":null,"status":"pending","decided_by":null,"decision_reason":null,"created_at":"2026-09-08T00:00:00+00:00","decided_at":null})),
        (status = 400, body = ErrorResponse, description = "Invalid message or id"), (status = 401, body = ErrorResponse, description = "Authentication required"),
        (status = 403, body = ErrorResponse, description = "Restricted token or policy denial"), (status = 404, body = ErrorResponse, description = "Group not found"),
        (status = 409, body = ErrorResponse, description = "Already a member, concurrent change, or device node")), security(("bearer_auth" = [])))]
async fn submit_join(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(group_id): Path<Ulid>,
    Json(input): Json<CreateJoinRequest>,
) -> ServerResult<(StatusCode, Json<JoinResponse>)> {
    let auth = require_unrestricted_realm_auth(&state, auth)?;
    authorize_join(&state, &auth, Some(group_id), Permission::WRITE).await?;
    Ok((
        StatusCode::CREATED,
        Json(
            mutate(
                &state,
                auth,
                group_id,
                JoinAction::Request {
                    message: input.message,
                },
            )
            .await?,
        ),
    ))
}

#[utoipa::path(get, path = "/access/groups/{id}/join-requests", tag = "access/groups",
    summary = "List group membership requests",
    description = r#"Lists membership requests for a group.

**Authentication**: unrestricted realm bearer token with WRITE on the group's admin/users/** path.
Realm and group request policies may deny.

**Behavior**
- Request messages are visible only to their requester and membership administrators.
- Use status=pending for the approval inbox.
- Follow next_start_after for further pages; page size is clamped to 1 to 100."#,
    params(("id" = Ulid, Path, description = "Group id"), ("status" = Option<String>, Query, description = "Optional pending filter"), ("start_after" = Option<String>, Query, description = "Cursor from the previous page"), ("limit" = Option<usize>, Query, description = "Page size, 1 to 100")),
    responses((status = 200, body = JoinPage, description = "Membership requests", example = json!({"requests":[],"next_start_after":null})),
        (status = 400, body = ErrorResponse, description = "Invalid filter or cursor"), (status = 401, body = ErrorResponse, description = "Authentication required"), (status = 403, body = ErrorResponse, description = "Membership administration required")), security(("bearer_auth" = [])))]
async fn list_joins(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(group_id): Path<Ulid>,
    Query(query): Query<JoinQuery>,
) -> ServerResult<Json<JoinPage>> {
    let auth = require_unrestricted_realm_auth(&state, auth)?;
    ensure_permission(
        &state,
        &auth,
        format!("/{}/g/{group_id}/admin/users/**", auth.realm_id),
        Permission::WRITE,
    )
    .await?;
    Ok(Json(list(&state, auth, Some(group_id), query).await?))
}

#[utoipa::path(get, path = "/access/users/join-requests", tag = "access/users",
    summary = "List your membership requests",
    description = r#"Lists the calling user's membership requests.

**Authentication**: unrestricted realm bearer token. Realm request policies may deny.

**Behavior**
- Returns only the caller's requests, including approved and denied history.
- Withdrawn requests are omitted; status=pending selects only pending requests.
- Results cover the group state held by this node.
- Follow next_start_after until null; page size is clamped to 1 to 100."#,
    params(("status" = Option<String>, Query, description = "Optional pending filter"), ("start_after" = Option<String>, Query, description = "Cursor from the previous page"), ("limit" = Option<usize>, Query, description = "Page size, 1 to 100")),
    responses((status = 200, body = JoinPage, description = "Your membership requests", example = json!({"requests":[],"next_start_after":null})),
        (status = 400, body = ErrorResponse, description = "Invalid cursor"), (status = 401, body = ErrorResponse, description = "Authentication required"), (status = 403, body = ErrorResponse, description = "Restricted token or policy denial")), security(("bearer_auth" = [])))]
async fn own_joins(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Query(query): Query<JoinQuery>,
) -> ServerResult<Json<JoinPage>> {
    let auth = require_unrestricted_realm_auth(&state, auth)?;
    authorize_join(&state, &auth, None, Permission::READ).await?;
    Ok(Json(list(&state, auth, None, query).await?))
}

#[utoipa::path(delete, path = "/access/groups/{id}/join-requests/{request_id}", tag = "access/groups",
    summary = "Withdraw your pending membership request",
    description = r#"Withdraws the calling user's pending membership request.

**Authentication**: unrestricted realm bearer token belonging to the requester.
Realm and group request policies may deny.

**Behavior**
- A repeated withdrawal succeeds without another change.
- An approved or denied request cannot be withdrawn.
- The withdrawal is committed and replicated as a signed group operation."#,
    params(("id" = Ulid, Path, description = "Group id"), ("request_id" = Ulid, Path, description = "Membership request id")),
    responses((status = 204, description = "Withdrawn"), (status = 401, body = ErrorResponse, description = "Authentication required"), (status = 403, body = ErrorResponse, description = "Not the requester or policy denial"), (status = 404, body = ErrorResponse, description = "Request not found"), (status = 409, body = ErrorResponse, description = "Already decided, concurrent change, or device node")), security(("bearer_auth" = [])))]
async fn withdraw_join(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((group_id, request_id)): Path<(Ulid, Ulid)>,
) -> ServerResult<StatusCode> {
    let auth = require_unrestricted_realm_auth(&state, auth)?;
    authorize_join(&state, &auth, Some(group_id), Permission::WRITE).await?;
    mutate(&state, auth, group_id, JoinAction::Withdraw { request_id }).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(post, path = "/access/groups/{id}/join-requests/{request_id}/decide", tag = "access/groups",
    summary = "Approve or deny a membership request",
    description = r#"Approves or denies one pending membership request.

**Authentication**: unrestricted realm bearer token with WRITE on the group's admin/users/** path.
Realm and group request policies may deny.

**Behavior**
- Approval atomically records the decision and assigns the requested roles.
- Omitted roles default to the group's single user role; denial grants no roles.
- Repeating a decision returns its saved result; changing a terminal decision returns 409.
- Concurrent decisions retain approval if either node accepted an approval granting membership."#,
    params(("id" = Ulid, Path, description = "Group id"), ("request_id" = Ulid, Path, description = "Membership request id")),
    request_body(content = DecideJoinRequest, example = json!({"approve":true,"role_ids":[]})),
    responses((status = 200, body = JoinDecisionResponse, description = "Saved decision", example = json!({"request":{"request_id":"01JABCDEF0123456789ABCDEFG","group_id":"01JABCDEF0123456789ABCDEFG","user_id":"01JABCDEF0123456789ABCDEFG@YXJ1bmEtZXhhbXBsZS1yZWFsbS0wMDAwMDAwMDAwMDA","message":null,"status":"approved","decided_by":"01JABCDEF0123456789ABCDEFG@YXJ1bmEtZXhhbXBsZS1yZWFsbS0wMDAwMDAwMDAwMDA","decision_reason":null,"created_at":"2026-09-08T00:00:00+00:00","decided_at":"2026-09-08T01:00:00+00:00"}})),
        (status = 400, body = ErrorResponse, description = "Invalid roles or reason"), (status = 401, body = ErrorResponse, description = "Authentication required"), (status = 403, body = ErrorResponse, description = "Membership administration required"), (status = 404, body = ErrorResponse, description = "Request not found"), (status = 409, body = ErrorResponse, description = "Different terminal decision, concurrent change, or device node")), security(("bearer_auth" = [])))]
async fn decide_join(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((group_id, request_id)): Path<(Ulid, Ulid)>,
    Json(input): Json<DecideJoinRequest>,
) -> ServerResult<Json<JoinDecisionResponse>> {
    let auth = require_unrestricted_realm_auth(&state, auth)?;
    ensure_permission(
        &state,
        &auth,
        format!("/{}/g/{group_id}/admin/users/**", auth.realm_id),
        Permission::WRITE,
    )
    .await?;
    let request = mutate(
        &state,
        auth,
        group_id,
        JoinAction::Decide {
            request_id,
            approve: input.approve,
            role_ids: input.role_ids,
            reason: input.reason,
        },
    )
    .await?;
    Ok(Json(JoinDecisionResponse { request }))
}
