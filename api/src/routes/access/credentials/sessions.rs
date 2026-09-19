//! Routes that create, list, refresh, and revoke short-lived S3 session credentials.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{
    authorize_credential_issuance, build_credential_restrictions, format_node_id,
    format_system_time, serialize_restrictions,
};
use crate::auth::{ValidatedBearer, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server::state::ServerState;
use aruna_core::structs::identity::auth::{AuthContext, PathRestriction};
use aruna_core::structs::identity::s3_session::{S3Session, SESSION_ACCESS_PREFIX};
use aruna_operations::driver::drive;
use aruna_operations::groups::get_group::{GetGroupConfig, GetGroupError, GetGroupOperation};
use aruna_operations::s3::session::{
    CreateS3Config, CreateS3Operation, GetS3Operation, ListSessionsOperation, RefreshS3Config,
    RefreshS3Operation, RevokeS3Config, RevokeS3Operation, S3SessionCredentials, S3SessionError,
};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use ulid::Ulid;
use utoipa::ToSchema;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

pub(super) fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new()
        .routes(routes!(create_s3_session, list_s3_sessions))
        .routes(routes!(refresh_s3_session))
        .routes(routes!(revoke_s3_session))
}

#[derive(Clone, Deserialize, Serialize, ToSchema)]
#[schema(as = CreateS3SessionRequest)]
pub struct S3SessionRequest {
    pub group_id: String,
}

#[derive(Clone, Deserialize, Serialize, ToSchema)]
#[schema(as = S3SessionGroupResponse)]
pub struct SessionGroupResponse {
    pub id: String,
}

#[derive(Clone, Deserialize, Serialize, ToSchema)]
#[schema(as = S3SessionIssuerResponse)]
pub struct SessionIssuerResponse {
    pub node_id: String,
    pub s3_endpoint: Option<String>,
}

#[derive(Clone, Deserialize, Serialize, ToSchema)]
#[schema(as = S3SessionRestrictionResponse)]
pub struct SessionRestrictionResponse {
    pub pattern: String,
    pub permission: String,
}

#[derive(Clone, Deserialize, Serialize, ToSchema)]
#[schema(as = S3SessionResponse)]
pub struct SessionTokenResponse {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: String,
    pub expires_at: String,
    pub group: SessionGroupResponse,
    pub restrictions: Vec<SessionRestrictionResponse>,
    pub issuer_node: SessionIssuerResponse,
}

#[derive(Clone, Deserialize, Serialize, ToSchema)]
#[schema(as = S3SessionSummaryResponse)]
pub struct SessionSummaryResponse {
    pub access_key_id: String,
    pub created_at: Option<String>,
    pub expires_at: String,
    pub last_used_at: Option<String>,
    pub group: SessionGroupResponse,
    pub restrictions: Vec<SessionRestrictionResponse>,
    pub issuer_node: SessionIssuerResponse,
}

#[derive(Clone, Deserialize, Serialize, ToSchema)]
#[schema(as = ListS3SessionsResponse)]
pub struct S3SessionsResponse {
    pub sessions: Vec<SessionSummaryResponse>,
}

#[utoipa::path(
    post,
    path = "/access/s3/sessions",
    tag = "access/credentials",
    summary = "Exchange a bearer token for an S3 session",
    description = r#"Issues a short-lived, node-local S3 session for an explicitly selected group.

**Authentication**: realm bearer token, membership in the requested group, and an effective read or
write scope on that group's data path. A read-only member receives a read-only session.

**Behavior**
- The group is always taken from `group_id` and is never inferred from membership order.
- The returned access key, signing secret and session token are accepted only by the issuing node.
- Sessions are stored outside the long-lived credential list and its limit.

**Limits**
- Expiry is the earlier of one hour from issuance and the bearer token's own expiry.
- At most four sessions are kept per user and group; a further exchange evicts the oldest one."#,
    request_body(
        content = S3SessionRequest,
        description = "The explicitly selected group; group_id is required.",
        example = json!({
            "group_id": "01JGRP00123456789ABCDEFGHJ"
        })
    ),
    responses(
        (
            status = 201,
            description = "A new node-local S3 session; the secret and token are redacted in this example and returned in full to the caller",
            body = SessionTokenResponse,
            example = json!({
                "access_key_id": "ARUNASESSION0123456789AB",
                "secret_access_key": "<redacted>",
                "session_token": "<redacted>",
                "expires_at": "2026-04-09T12:00:00Z",
                "group": {
                    "id": "01JGRP00123456789ABCDEFGHJ"
                },
                "restrictions": [
                    {
                        "pattern": "/realm/group/*",
                        "permission": "WRITE"
                    }
                ],
                "issuer_node": {
                    "node_id": "f3a1b2c3d4e5f60718293a4b5c6d7e8f9091a2b3c4d5e6f708192a3b4c5d6e7f",
                    "s3_endpoint": "https://s3.example.org"
                }
            })
        ),
        (status = 400, description = "group_id is not a ULID", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token, or one with no remaining lifetime", body = ErrorResponse),
        (status = 403, description = "The caller is not a member of the group, or its effective token restrictions leave no allowed scope on the group data path", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn create_s3_session(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    Json(request): Json<S3SessionRequest>,
) -> ServerResult<(StatusCode, Json<SessionTokenResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    let bearer = bearer.ok_or(ServerError::Unauthorized)?;
    let group_id = request
        .group_id
        .parse::<Ulid>()
        .map_err(|_| ServerError::BadRequest)?;
    let restrictions = session_scope(&state, &auth, group_id).await?;
    let now = SystemTime::now();
    let expiry = session_expiry(now, bearer.expires_at_secs())?;
    let credentials = drive(
        CreateS3Operation::new(
            CreateS3Config {
                user_identity: auth.user_id,
                group_id,
                now,
                expiry,
                path_restrictions: restrictions,
                issued_by: *state.get_node_id().as_bytes(),
            },
            state.credential_encryption_key().clone(),
        ),
        &state.get_ctx(),
    )
    .await
    .map_err(map_create_error)?;
    let response = session_response(&state, credentials).await;
    Ok((StatusCode::CREATED, Json(response)))
}

#[utoipa::path(
    get,
    path = "/access/s3/sessions",
    tag = "access/credentials",
    summary = "List the caller's S3 sessions",
    description = r#"Lists the caller's own S3 sessions on the node serving the request.

**Authentication**: realm bearer token; a path-restricted token sees the same sessions, since the
listing is scoped to the caller and carries no permission of its own.

**Behavior**
- No secret is listed: neither the signing secret nor the session token is part of a summary.
- Only sessions this node issued are listed, so a user holding sessions on several nodes asks each
  of them separately.
- `created_at` is taken from the access key id, so a refresh keeps it while `expires_at` moves.
- An expired session is left out even before the periodic sweep deletes it."#,
    responses(
        (
            status = 200,
            description = "The caller's active sessions on this node",
            body = S3SessionsResponse,
            example = json!({
                "sessions": [{
                    "access_key_id": "ARUNASESSION0123456789AB",
                    "created_at": "2026-04-09T11:00:00Z",
                    "expires_at": "2026-04-09T12:00:00Z",
                    "last_used_at": "2026-04-09T11:12:00Z",
                    "group": {
                        "id": "01JGRP00123456789ABCDEFGHJ"
                    },
                    "restrictions": [
                        {
                            "pattern": "/realm/group/*",
                            "permission": "WRITE"
                        }
                    ],
                    "issuer_node": {
                        "node_id": "f3a1b2c3d4e5f60718293a4b5c6d7e8f9091a2b3c4d5e6f708192a3b4c5d6e7f",
                        "s3_endpoint": "https://s3.example.org"
                    }
                }]
            })
        ),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_s3_sessions(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<(StatusCode, Json<S3SessionsResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    let now = SystemTime::now();
    let issued_by = *state.get_node_id().as_bytes();
    let s3_endpoint = state
        .interface_state()
        .await
        .s3
        .map(|interface| interface.base_url);
    let sessions = drive(ListSessionsOperation::new(auth.user_id), &state.get_ctx())
        .await
        .map_err(|error| ServerError::InternalError(error.to_string()))?
        .into_iter()
        .filter(|session| session.issued_by == issued_by && !session.is_expired(now))
        .map(|session| session_summary(session, s3_endpoint.clone()))
        .collect();
    Ok((StatusCode::OK, Json(S3SessionsResponse { sessions })))
}

#[utoipa::path(
    post,
    path = "/access/s3/sessions/{access_key_id}/refresh",
    tag = "access/credentials",
    summary = "Rotate an active S3 session",
    description = r#"Rotates the secret and session token of an active S3 session inside its refresh window.

**Authentication**: realm bearer token of the session's owner, who must still be a member of the
session's group with READ or WRITE on some path under its data root.

**Behavior**
- The access key id is kept while the signing secret and session token are rotated in place, so the
  previous pair stops working.
- Activity is reset for the next cycle, and the new expiry is capped by the bearer token's own.

**Limits**
- Refresh is accepted at or after five minutes before expiry, and only when the session has
  completed an authenticated S3 request since its last issuance."#,
    params(("access_key_id" = String, Path, description = "Access key id returned by the session exchange")),
    responses(
        (
            status = 200,
            description = "The same access key id with a rotated secret and session token, redacted in this example",
            body = SessionTokenResponse,
            example = json!({
                "access_key_id": "ARUNASESSION0123456789AB",
                "secret_access_key": "<redacted>",
                "session_token": "<redacted>",
                "expires_at": "2026-04-09T12:00:00Z",
                "group": {
                    "id": "01JGRP00123456789ABCDEFGHJ"
                },
                "restrictions": [
                    {
                        "pattern": "/realm/group/*",
                        "permission": "WRITE"
                    }
                ],
                "issuer_node": {
                    "node_id": "f3a1b2c3d4e5f60718293a4b5c6d7e8f9091a2b3c4d5e6f708192a3b4c5d6e7f",
                    "s3_endpoint": "https://s3.example.org"
                }
            })
        ),
        (status = 401, description = "Missing or invalid bearer token, or one with no remaining lifetime", body = ErrorResponse),
        (status = 403, description = "The caller is no longer a member of the group, or lacks readable access under its data path", body = ErrorResponse),
        (status = 404, description = "Session not found on this node, or it belongs to another user", body = ErrorResponse),
        (status = 409, description = "The session is idle, expired, or not yet in its refresh window", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn refresh_s3_session(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    Path(access_key_id): Path<String>,
) -> ServerResult<(StatusCode, Json<SessionTokenResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    let bearer = bearer.ok_or(ServerError::Unauthorized)?;
    let session = drive(GetS3Operation::new(access_key_id.clone()), &state.get_ctx())
        .await
        .map_err(|error| ServerError::InternalError(error.to_string()))?
        .ok_or(ServerError::NotFound)?;
    if session.user_identity != auth.user_id || session.issued_by != *state.get_node_id().as_bytes()
    {
        return Err(ServerError::NotFound);
    }
    let restrictions = session_scope(&state, &auth, session.group_id).await?;
    let now = SystemTime::now();
    let expiry = session_expiry(now, bearer.expires_at_secs())?;
    let credentials = drive(
        RefreshS3Operation::new(
            RefreshS3Config {
                access_key: access_key_id,
                user_identity: auth.user_id,
                group_id: session.group_id,
                now,
                expiry,
                path_restrictions: restrictions,
                issued_by: *state.get_node_id().as_bytes(),
            },
            state.credential_encryption_key().clone(),
        ),
        &state.get_ctx(),
    )
    .await
    .map_err(map_refresh_error)?;
    let response = session_response(&state, credentials).await;
    Ok((StatusCode::OK, Json(response)))
}

#[utoipa::path(
    delete,
    path = "/access/s3/sessions/{access_key_id}",
    tag = "access/credentials",
    summary = "Revoke an S3 session",
    description = r#"Deletes one of the caller's own S3 sessions from the node that issued it.

**Authentication**: realm bearer token of the session's owner; group membership is not rechecked,
so a session stays revocable after the caller left its group.

**Behavior**
- The access key, its signing secret and its session token stop authenticating S3 requests as soon
  as the deletion commits.
- The revocation is node-local, like the session it ends, and does not touch long-lived
  credentials.
- A session of another user, one already revoked, and one issued by another node are all answered
  as unknown."#,
    params(("access_key_id" = String, Path, description = "Access key id returned by the session exchange")),
    responses(
        (status = 204, description = "Session deleted from this node"),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm", body = ErrorResponse),
        (status = 404, description = "Session not found on this node, or it belongs to another user", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn revoke_s3_session(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(access_key_id): Path<String>,
) -> ServerResult<StatusCode> {
    let auth = require_realm_auth(&state, auth)?;
    drive(
        RevokeS3Operation::new(RevokeS3Config {
            access_key: access_key_id,
            user_identity: auth.user_id,
            issued_by: *state.get_node_id().as_bytes(),
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_revoke_error)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn session_scope(
    state: &ServerState,
    auth: &AuthContext,
    group_id: Ulid,
) -> ServerResult<Option<Vec<PathRestriction>>> {
    ensure_membership(state, auth, group_id).await?;
    let restrictions = build_credential_restrictions(auth, state, group_id, None).await?;
    authorize_credential_issuance(auth, state, group_id, restrictions.as_deref()).await?;
    Ok(restrictions.as_deref().map(serialize_restrictions))
}

async fn ensure_membership(
    state: &ServerState,
    auth: &AuthContext,
    group_id: Ulid,
) -> ServerResult<()> {
    let (_, authorization) = drive(
        GetGroupOperation::new(GetGroupConfig { group_id }),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| match error {
        GetGroupError::GroupNotFound | GetGroupError::DocNotFound => ServerError::Forbidden,
        _ => ServerError::InternalError(error.to_string()),
    })?;
    authorization
        .roles
        .values()
        .any(|role| role.assigned_users.contains(&auth.user_id))
        .then_some(())
        .ok_or(ServerError::Forbidden)
}

fn session_expiry(now: SystemTime, bearer_expiry: u64) -> ServerResult<SystemTime> {
    let now_secs = now
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ServerError::InternalError("system clock precedes Unix epoch".to_string()))?
        .as_secs();
    let expires_at = now_secs
        .checked_add(60 * 60)
        .ok_or(ServerError::InternalError(
            "session expiry overflow".to_string(),
        ))?
        .min(bearer_expiry);
    if expires_at <= now_secs {
        return Err(ServerError::Unauthorized);
    }
    Ok(UNIX_EPOCH + Duration::from_secs(expires_at))
}

async fn session_response(
    state: &ServerState,
    credentials: S3SessionCredentials,
) -> SessionTokenResponse {
    let s3_endpoint = state
        .interface_state()
        .await
        .s3
        .map(|interface| interface.base_url);
    SessionTokenResponse {
        access_key_id: credentials.access_key_id,
        secret_access_key: credentials.secret_access_key.expose().to_string(),
        session_token: credentials.session_token.expose().to_string(),
        expires_at: format_system_time(credentials.session.expiry),
        group: SessionGroupResponse {
            id: credentials.session.group_id.to_string(),
        },
        restrictions: credentials
            .session
            .path_restrictions
            .unwrap_or_default()
            .into_iter()
            .map(|restriction| SessionRestrictionResponse {
                pattern: restriction.pattern,
                permission: restriction.permission.to_string(),
            })
            .collect(),
        issuer_node: SessionIssuerResponse {
            node_id: format_node_id(credentials.session.issued_by),
            s3_endpoint,
        },
    }
}

/// Issue time of a session, held only by the ULID inside its access key.
fn session_issued(access_key: &str) -> Option<SystemTime> {
    let key_id = access_key
        .strip_prefix(SESSION_ACCESS_PREFIX)?
        .parse::<Ulid>()
        .ok()?;
    Some(UNIX_EPOCH + Duration::from_millis(key_id.timestamp_ms()))
}

fn session_summary(session: S3Session, s3_endpoint: Option<String>) -> SessionSummaryResponse {
    let created_at = session_issued(&session.access_key).map(format_system_time);
    let restrictions = session
        .path_restrictions
        .unwrap_or_default()
        .into_iter()
        .map(|restriction| SessionRestrictionResponse {
            pattern: restriction.pattern,
            permission: restriction.permission.to_string(),
        })
        .collect();
    SessionSummaryResponse {
        access_key_id: session.access_key,
        created_at,
        expires_at: format_system_time(session.expiry),
        last_used_at: session.last_used_at.map(format_system_time),
        group: SessionGroupResponse {
            id: session.group_id.to_string(),
        },
        restrictions,
        issuer_node: SessionIssuerResponse {
            node_id: format_node_id(session.issued_by),
            s3_endpoint,
        },
    }
}

fn map_create_error(error: S3SessionError) -> ServerError {
    match error {
        S3SessionError::InvalidExpiry => ServerError::Unauthorized,
        error => ServerError::InternalError(error.to_string()),
    }
}

fn map_refresh_error(error: S3SessionError) -> ServerError {
    match error {
        S3SessionError::NotFound
        | S3SessionError::WrongIssuer
        | S3SessionError::WrongOwner
        | S3SessionError::WrongGroup
        | S3SessionError::InvalidAccessKey => ServerError::NotFound,
        S3SessionError::TooEarly => {
            ServerError::Conflict("session refresh is not available yet".to_string())
        }
        S3SessionError::Idle => ServerError::Conflict("session is idle".to_string()),
        S3SessionError::Expired => ServerError::Conflict("session has expired".to_string()),
        S3SessionError::InvalidExpiry => ServerError::Unauthorized,
        error => ServerError::InternalError(error.to_string()),
    }
}

fn map_revoke_error(error: S3SessionError) -> ServerError {
    match error {
        S3SessionError::NotFound
        | S3SessionError::WrongIssuer
        | S3SessionError::WrongOwner
        | S3SessionError::InvalidAccessKey => ServerError::NotFound,
        error => ServerError::InternalError(error.to_string()),
    }
}

#[cfg(test)]
#[path = "sessions_tests.rs"]
mod tests;
