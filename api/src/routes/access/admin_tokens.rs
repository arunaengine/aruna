//! Routes for group and realm administrators to list and revoke the tokens of others.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::{ensure_permission, parse_group_id, require_unrestricted_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::access::groups::ensure_group_admin;
use crate::routes::access::sessions::unix_rfc3339;
use crate::server::state::ServerState;
use aruna_core::UserId;
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::time::unix_timestamp_secs;
use aruna_operations::auth::revoke_token::RevokeTokenError;
use aruna_operations::driver::drive;
use aruna_operations::groups::tokens::list::{GroupTokensConfig, ListTokensOperation};
use aruna_operations::groups::tokens::revoke::{
    RevokeTarget, RevokeTokensConfig, RevokeTokensOperation,
};
use aruna_operations::groups::tokens::{GroupToken, GroupTokensError, TokenKind, TokenStatus};
use aruna_operations::users::revoke_all::{RevokeAllConfig, RevokeAllError, RevokeAllOperation};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

#[derive(OpenApi)]
pub struct AdminTokensDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(AdminTokensDoc::openapi())
        .routes(routes!(list_group_tokens))
        .routes(routes!(revoke_group_token))
        .routes(routes!(revoke_member_tokens))
        .routes(routes!(revoke_user_tokens))
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct GroupTokenResponse {
    /// `session`, `s3_credential` or `s3_session`.
    pub kind: String,
    /// Session id or access key id; never a secret.
    pub id: String,
    pub owner: String,
    pub created_at: Option<String>,
    pub expires_at: String,
    pub last_used_at: Option<String>,
    /// `active`, `expired` or `revoked`.
    pub status: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct GroupTokensResponse {
    pub tokens: Vec<GroupTokenResponse>,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct RevocationResponse {
    /// Active tokens this node held that were revoked.
    pub revoked: usize,
    /// The member is a service account of the group and lost every token on every node.
    pub all_tokens: bool,
}

fn kind_name(kind: TokenKind) -> &'static str {
    match kind {
        TokenKind::Session => "session",
        TokenKind::S3Credential => "s3_credential",
        TokenKind::S3Session => "s3_session",
    }
}

fn parse_kind(kind: &str) -> ServerResult<TokenKind> {
    match kind {
        "session" => Ok(TokenKind::Session),
        "s3_credential" => Ok(TokenKind::S3Credential),
        "s3_session" => Ok(TokenKind::S3Session),
        _ => Err(ServerError::BadRequestReason(
            "kind must be session, s3_credential or s3_session".to_string(),
        )),
    }
}

fn token_response(token: GroupToken) -> GroupTokenResponse {
    GroupTokenResponse {
        kind: kind_name(token.kind).to_string(),
        id: token.id,
        owner: token.owner.to_string(),
        created_at: token.created_at.map(unix_rfc3339),
        expires_at: unix_rfc3339(token.expires_at),
        last_used_at: token.last_used_at.map(unix_rfc3339),
        status: match token.status {
            TokenStatus::Active => "active",
            TokenStatus::Expired => "expired",
            TokenStatus::Revoked => "revoked",
        }
        .to_string(),
    }
}

fn map_revocation(message: String) -> ServerError {
    match message == RevokeTokenError::CapacityReached.to_string() {
        true => ServerError::ServiceUnavailableReason("token_revocation_capacity_reached".into()),
        false => ServerError::InternalError(message),
    }
}

fn map_tokens_error(error: GroupTokensError) -> ServerError {
    match error {
        GroupTokensError::Refused => ServerError::Forbidden,
        GroupTokensError::NotFound => ServerError::NotFound,
        GroupTokensError::Revocation(message) => map_revocation(message),
        error => ServerError::InternalError(error.to_string()),
    }
}

fn actor_for(state: &ServerState, auth: &AuthContext) -> Actor {
    Actor {
        node_id: state.get_node_id(),
        user_id: auth.user_id,
        realm_id: auth.realm_id,
    }
}

async fn run_revocation(
    state: &ServerState,
    auth: AuthContext,
    group_id: ulid::Ulid,
    target: RevokeTarget,
) -> ServerResult<RevocationResponse> {
    ensure_group_admin(state, &auth, group_id).await?;
    let revocation = drive(
        RevokeTokensOperation::new(RevokeTokensConfig {
            actor: actor_for(state, &auth),
            auth_context: auth.clone(),
            group_id,
            target,
            now: unix_timestamp_secs(),
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_tokens_error)?;
    if revocation.credentials > 0 {
        crate::routes::storage::abe::epoch_due(state, &auth, Some(group_id)).await;
    }
    Ok(RevocationResponse {
        revoked: revocation.revoked,
        all_tokens: revocation.all_tokens,
    })
}

#[utoipa::path(
    get,
    path = "/access/groups/{id}/tokens",
    tag = "access/groups",
    summary = "List tokens that grant group access",
    description = r#"Lists the tokens this node holds that grant access to the group.

**Authentication**: realm bearer token without path restrictions, with WRITE on the group's
`/{realm}/g/{group}/admin` or on the realm's `/{realm}/admin/groups`. Service accounts are refused.

**Behavior**
- `session` entries are bearer sessions of the group's service accounts. `s3_credential` and
  `s3_session` entries are S3 keys of any user that are bound to the group.
- Personal bearer sessions of members are not listed: they are bound to the user, not the group.
- Tokens issued before their owner's revocation cutoff are listed as `revoked`.
- No secret, token or token hash is ever returned.

**Limits**
- Sessions and S3 keys live on the node that issued them, so each node lists only its own.
- `last_used_at` is known only for S3 sessions."#,
    params(("id" = String, Path, description = "Group id as a 26-character ULID")),
    responses(
        (status = 200, description = "The group's tokens held by this node", body = GroupTokensResponse,
            example = json!({
                "tokens": [{
                    "kind": "s3_credential",
                    "id": "01JAKEY0123456789ABCDEFGHJ",
                    "owner": "01JUSER01ABCDEFGHJKMNPQRST@AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8",
                    "created_at": "2026-04-09T12:00:00Z",
                    "expires_at": "2027-04-09T12:00:00Z",
                    "last_used_at": null,
                    "status": "active"
                }]
            })),
        (status = 400, description = "The group id is not a ULID", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token is path-restricted or belongs to another realm, or the caller does not administer the group", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_group_tokens(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(group_id): Path<String>,
) -> ServerResult<(StatusCode, Json<GroupTokensResponse>)> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let group_id = parse_group_id(&group_id)?;
    ensure_group_admin(&state, &auth, group_id).await?;
    let tokens = drive(
        ListTokensOperation::new(GroupTokensConfig {
            auth_context: auth,
            group_id,
            now: unix_timestamp_secs(),
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_tokens_error)?;
    let tokens = tokens.into_iter().map(token_response).collect();
    Ok((StatusCode::OK, Json(GroupTokensResponse { tokens })))
}

#[utoipa::path(
    delete,
    path = "/access/groups/{id}/tokens/{kind}/{token_id}",
    tag = "access/groups",
    summary = "Revoke one token of a group",
    description = r#"Revokes one token this node holds that grants access to the group.

**Authentication**: as for `GET /access/groups/{id}/tokens`.

**Behavior**
- A session is revoked on every node through the realm revocation set; an S3 credential or S3
  session is deleted on this node, where it lives.
- A token that is already expired or revoked is left as it is, and `revoked` is 0.

**Limits**
- Only tokens that `GET /access/groups/{id}/tokens` lists on this node can be revoked here."#,
    params(
        ("id" = String, Path, description = "Group id as a 26-character ULID"),
        ("kind" = String, Path, description = "`session`, `s3_credential` or `s3_session`"),
        ("token_id" = String, Path, description = "Session id or access key id as listed")
    ),
    responses(
        (status = 200, description = "The token is revoked", body = RevocationResponse,
            example = json!({ "revoked": 1, "all_tokens": false })),
        (status = 400, description = "Invalid group id or token kind", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token is path-restricted or belongs to another realm, or the caller does not administer the group", body = ErrorResponse),
        (status = 404, description = "This node holds no such token of the group", body = ErrorResponse),
        (status = 503, description = "The revocation set is at capacity; retryable", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn revoke_group_token(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((group_id, kind, token_id)): Path<(String, String, String)>,
) -> ServerResult<(StatusCode, Json<RevocationResponse>)> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let group_id = parse_group_id(&group_id)?;
    let kind = parse_kind(&kind)?;
    let target = RevokeTarget::Token { kind, id: token_id };
    let response = run_revocation(&state, auth, group_id, target).await?;
    Ok((StatusCode::OK, Json(response)))
}

#[utoipa::path(
    delete,
    path = "/access/groups/{id}/members/{user_id}/tokens",
    tag = "access/groups",
    summary = "Revoke a member's access to a group",
    description = r#"Revokes every token this node holds that binds one member to the group.

**Authentication**: as for `GET /access/groups/{id}/tokens`.

**Behavior**
- For a person, the S3 credentials and S3 sessions bound to this group are revoked. Their own
  bearer sessions and their access to other groups stay untouched.
- For a service account of this group, every token it holds is ended on every node through its
  revocation cutoff, and `all_tokens` is true.
- Group membership is not changed; remove the member separately.

**Limits**
- S3 keys on other nodes are revoked only by calling this route there as well."#,
    params(
        ("id" = String, Path, description = "Group id as a 26-character ULID"),
        ("user_id" = String, Path, description = "User id in the form `<ulid>@<realm>`")
    ),
    responses(
        (status = 200, description = "The member's group tokens are revoked", body = RevocationResponse,
            example = json!({ "revoked": 2, "all_tokens": false })),
        (status = 400, description = "Invalid group or user id", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token is path-restricted or belongs to another realm, or the caller does not administer the group", body = ErrorResponse),
        (status = 503, description = "The revocation set is at capacity; retryable", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn revoke_member_tokens(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((group_id, user_id)): Path<(String, String)>,
) -> ServerResult<(StatusCode, Json<RevocationResponse>)> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let group_id = parse_group_id(&group_id)?;
    let member = UserId::from_string(&user_id).map_err(|_| ServerError::BadRequest)?;
    let target = RevokeTarget::Member(member);
    let response = run_revocation(&state, auth, group_id, target).await?;
    Ok((StatusCode::OK, Json(response)))
}

#[utoipa::path(
    delete,
    path = "/access/users/{id}/tokens",
    tag = "access/tokens",
    summary = "Revoke every token of a user",
    description = r#"Ends every bearer token, S3 credential and S3 session of a user on every node.

**Authentication**: realm bearer token without path restrictions and with WRITE on
`/{realm}/admin/u/{user}`, which realm administrators hold.

**Behavior**
- The user's revocation cutoff moves to now, so every token issued up to now is refused, and it
  reaches the other nodes with the realm configuration.
- The account stays active: the user can sign in again and gets new tokens.
- Revoking one's own tokens also ends the token of this request.

**Limits**
- Accounts of other realms cannot be targeted."#,
    params(("id" = String, Path, description = "User id in the form `<ulid>@<realm>`")),
    responses(
        (status = 204, description = "Every token issued so far is revoked"),
        (status = 400, description = "The id is not a user id", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token is path-restricted or belongs to another realm, the target is of another realm, or the caller lacks WRITE on the user's administration path", body = ErrorResponse),
        (status = 503, description = "The revocation set is at capacity; retryable", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn revoke_user_tokens(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(id): Path<String>,
) -> ServerResult<StatusCode> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let target = UserId::from_string(&id).map_err(|_| ServerError::BadRequest)?;
    let path = format!("/{}/admin/u/{target}", auth.realm_id);
    ensure_permission(&state, &auth, path, Permission::WRITE).await?;
    drive(
        RevokeAllOperation::new(RevokeAllConfig {
            actor: actor_for(&state, &auth),
            auth_context: auth,
            target,
            now: unix_timestamp_secs(),
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| match error {
        RevokeAllError::Refused => ServerError::Forbidden,
        RevokeAllError::Cutoff(message) => map_revocation(message),
        error => ServerError::InternalError(error.to_string()),
    })?;
    Ok(StatusCode::NO_CONTENT)
}
