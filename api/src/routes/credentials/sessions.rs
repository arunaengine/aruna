use super::{
    authorize_credential_issuance, build_credential_restrictions, format_node_id,
    format_system_time, serialize_restrictions,
};
use crate::auth::{ValidatedArunaBearerTokenCarrier, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server_state::ServerState;
use aruna_core::structs::{AuthContext, PathRestriction, blob_group_permission_path};
use aruna_operations::driver::drive;
use aruna_operations::get_group::{GetGroupConfig, GetGroupError, GetGroupOperation};
use aruna_operations::s3::session::{
    CreateS3SessionConfig, CreateS3SessionOperation, GetS3SessionOperation, RefreshS3SessionConfig,
    RefreshS3SessionOperation, S3SessionCredentials, S3SessionError,
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
        .routes(routes!(create_s3_session))
        .routes(routes!(refresh_s3_session))
}

#[derive(Clone, Deserialize, Serialize, ToSchema)]
pub struct CreateS3SessionRequest {
    pub group_id: String,
}

#[derive(Clone, Deserialize, Serialize, ToSchema)]
pub struct S3SessionGroupResponse {
    pub id: String,
}

#[derive(Clone, Deserialize, Serialize, ToSchema)]
pub struct S3SessionIssuerResponse {
    pub node_id: String,
    pub s3_endpoint: Option<String>,
}

#[derive(Clone, Deserialize, Serialize, ToSchema)]
pub struct S3SessionRestrictionResponse {
    pub pattern: String,
    pub permission: String,
}

#[derive(Clone, Deserialize, Serialize, ToSchema)]
pub struct S3SessionResponse {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: String,
    pub expires_at: String,
    pub group: S3SessionGroupResponse,
    pub restrictions: Vec<S3SessionRestrictionResponse>,
    pub issuer_node: S3SessionIssuerResponse,
}

#[utoipa::path(
    post,
    path = "/users/s3-sessions",
    tag = "credentials",
    summary = "Exchange a bearer token for a short-lived S3 session",
    description = "Requires a bearer token issued for this realm, explicit membership in the requested group, and WRITE capability under the effective bearer restrictions on that group's node-local data path. The group is always taken from group_id and is never inferred from membership order. The returned access key, signing secret, and session token are accepted only by the issuing node; expiry is the earlier of one hour from issuance and the current bearer expiry. Sessions are stored outside the long-lived credential list and limit.",
    request_body(
        content = CreateS3SessionRequest,
        description = "The explicitly selected group; group_id is required.",
        example = json!({"group_id": "01JGRP00123456789ABCDEFGHJ"})
    ),
    responses(
        (status = 201, description = "A new node-local temporary S3 session; the secret and token are shown redacted here and are returned in full to the caller", body = S3SessionResponse, example = json!({
            "access_key_id": "ARUNASESSION0123456789AB",
            "secret_access_key": "<redacted>",
            "session_token": "<redacted>",
            "expires_at": "2026-04-09T12:00:00Z",
            "group": {"id": "01JGRP00123456789ABCDEFGHJ"},
            "restrictions": [{"pattern": "/realm/group/*", "permission": "WRITE"}],
            "issuer_node": {
                "node_id": "f3a1b2c3d4e5f60718293a4b5c6d7e8f9091a2b3c4d5e6f708192a3b4c5d6e7f",
                "s3_endpoint": "https://s3.example.org"
            }
        })),
        (status = 400, description = "group_id is not a ULID", body = ErrorResponse),
        (status = 401, description = "The bearer token is absent, invalid, expired, or has no remaining lifetime", body = ErrorResponse),
        (status = 403, description = "The caller is not a group member or lacks effective WRITE capability", body = ErrorResponse),
        (status = 409, description = "The per-user/group active session bound has been reached", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn create_s3_session(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedArunaBearerTokenCarrier>>,
    Json(request): Json<CreateS3SessionRequest>,
) -> ServerResult<(StatusCode, Json<S3SessionResponse>)> {
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
        CreateS3SessionOperation::new(
            CreateS3SessionConfig {
                user_identity: auth.user_id,
                group_id,
                now,
                expiry,
                path_restrictions: restrictions,
                issued_by: *state.get_node_id().as_bytes(),
            },
            state.credential_seal_key().clone(),
        ),
        &state.get_ctx(),
    )
    .await
    .map_err(map_create_error)?;
    let response = session_response(&state, credentials).await;
    Ok((StatusCode::CREATED, Json(response)))
}

#[utoipa::path(
    post,
    path = "/users/s3-sessions/{access_key_id}/refresh",
    tag = "credentials",
    summary = "Rotate an active S3 session in its refresh window",
    params(("access_key_id" = String, Path, description = "Temporary access key returned by the session exchange")),
    description = "Requires the owning bearer identity to remain a member with effective WRITE capability. Refresh is accepted at or after five minutes before expiry only when this session has completed an authenticated S3 request since its last issuance. It keeps the access key id, rotates the signing secret and session token in place, resets activity for the next cycle, and caps the new expiry by the current bearer.",
    responses(
        (status = 200, description = "The same session id with rotated temporary secret and token, shown redacted here", body = S3SessionResponse, example = json!({
            "access_key_id": "ARUNASESSION0123456789AB",
            "secret_access_key": "<redacted>",
            "session_token": "<redacted>",
            "expires_at": "2026-04-09T12:00:00Z",
            "group": {"id": "01JGRP00123456789ABCDEFGHJ"},
            "restrictions": [{"pattern": "/realm/group/*", "permission": "WRITE"}],
            "issuer_node": {
                "node_id": "f3a1b2c3d4e5f60718293a4b5c6d7e8f9091a2b3c4d5e6f708192a3b4c5d6e7f",
                "s3_endpoint": "https://s3.example.org"
            }
        })),
        (status = 401, description = "The bearer token is absent, invalid, expired, or has no remaining lifetime", body = ErrorResponse),
        (status = 403, description = "The caller is no longer a group member or lacks effective WRITE capability", body = ErrorResponse),
        (status = 404, description = "The session is absent, purged, foreign, or belongs to another user", body = ErrorResponse),
        (status = 409, description = "The session is idle, expired, or not yet in its refresh window", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn refresh_s3_session(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedArunaBearerTokenCarrier>>,
    Path(access_key_id): Path<String>,
) -> ServerResult<(StatusCode, Json<S3SessionResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    let bearer = bearer.ok_or(ServerError::Unauthorized)?;
    let session = drive(
        GetS3SessionOperation::new(access_key_id.clone()),
        &state.get_ctx(),
    )
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
        RefreshS3SessionOperation::new(
            RefreshS3SessionConfig {
                access_key: access_key_id,
                user_identity: auth.user_id,
                group_id: session.group_id,
                now,
                expiry,
                path_restrictions: restrictions,
                issued_by: *state.get_node_id().as_bytes(),
            },
            state.credential_seal_key().clone(),
        ),
        &state.get_ctx(),
    )
    .await
    .map_err(map_refresh_error)?;
    let response = session_response(&state, credentials).await;
    Ok((StatusCode::OK, Json(response)))
}

async fn session_scope(
    state: &ServerState,
    auth: &AuthContext,
    group_id: Ulid,
) -> ServerResult<Option<Vec<PathRestriction>>> {
    ensure_membership(state, auth, group_id).await?;
    let group_root =
        blob_group_permission_path(state.get_realm_id(), group_id, state.get_node_id());
    let restrictions = build_credential_restrictions(auth, state, group_id, None).await?;
    authorize_credential_issuance(auth, state, &group_root, restrictions.as_deref()).await?;
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
        GetGroupError::GroupNotFound | GetGroupError::AuthDocNotFound => ServerError::Forbidden,
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
) -> S3SessionResponse {
    let s3_endpoint = state
        .interface_state()
        .await
        .s3
        .map(|interface| interface.base_url);
    S3SessionResponse {
        access_key_id: credentials.access_key_id,
        secret_access_key: credentials.secret_access_key.expose().to_string(),
        session_token: credentials.session_token.expose().to_string(),
        expires_at: format_system_time(credentials.session.expiry),
        group: S3SessionGroupResponse {
            id: credentials.session.group_id.to_string(),
        },
        restrictions: credentials
            .session
            .path_restrictions
            .unwrap_or_default()
            .into_iter()
            .map(|restriction| S3SessionRestrictionResponse {
                pattern: restriction.pattern,
                permission: restriction.permission.to_string(),
            })
            .collect(),
        issuer_node: S3SessionIssuerResponse {
            node_id: format_node_id(credentials.session.issued_by),
            s3_endpoint,
        },
    }
}

fn map_create_error(error: S3SessionError) -> ServerError {
    match error {
        S3SessionError::LimitReached => {
            ServerError::Conflict("active session limit reached".to_string())
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::effects::StorageEffect;
    use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE};
    use aruna_core::structs::{
        Actor, Group, GroupAuthorizationDocument, NodeCapabilities, RealmId,
    };
    use aruna_operations::driver::DriverContext;
    use aruna_operations::jobs::runtime::JobsRuntime;
    use aruna_storage::FjallStorage;

    #[test]
    fn mint_needs_group() {
        assert!(serde_json::from_value::<CreateS3SessionRequest>(serde_json::json!({})).is_err());
    }

    #[test]
    fn expiry_caps_hour() {
        let now = UNIX_EPOCH + Duration::from_secs(1_000);
        assert_eq!(
            session_expiry(now, 10_000).unwrap(),
            UNIX_EPOCH + Duration::from_secs(4_600)
        );
    }

    #[test]
    fn expiry_caps_bearer() {
        let now = UNIX_EPOCH + Duration::from_secs(1_000);
        assert_eq!(
            session_expiry(now, 2_500).unwrap(),
            UNIX_EPOCH + Duration::from_secs(2_500)
        );
    }

    #[tokio::test]
    async fn mint_checks_member() {
        let directory = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(directory.path().to_str().unwrap()).unwrap();
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let node_id = iroh::SecretKey::from_bytes(&[2u8; 32]).public();
        let state = ServerState::new(
            Arc::new(DriverContext {
                storage_handle: storage,
                net_handle: None,
                blob_handle: None,
                metadata_handle: None,
                task_handle: None,
                compute_handle: None,
            }),
            realm_id,
            node_id,
            NodeCapabilities::local_node(realm_id).unwrap(),
            false,
            None,
            JobsRuntime::new(),
        )
        .await;
        let group_id = Ulid::from_bytes([3u8; 16]);
        let member = UserId::new(Ulid::from_bytes([4u8; 16]), realm_id);
        let stranger = UserId::new(Ulid::from_bytes([5u8; 16]), realm_id);
        let actor = Actor {
            node_id,
            user_id: member,
            realm_id,
        };
        let authorization =
            GroupAuthorizationDocument::new_default_group_doc(member, realm_id, group_id);
        let group = Group {
            display_name: "session-group".to_string(),
            group_id,
            realm_id,
            roles: authorization.roles.keys().copied().collect(),
            owner: member,
        };
        for (key_space, value) in [
            (AUTH_KEYSPACE, authorization.to_bytes(&actor).unwrap()),
            (GROUP_KEYSPACE, group.to_bytes(&actor).unwrap()),
        ] {
            state
                .get_ctx()
                .storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: key_space.to_string(),
                    key: group_id.to_bytes().to_vec().into(),
                    value: value.into(),
                    txn_id: None,
                })
                .await;
        }
        let member_auth = AuthContext {
            user_id: member,
            realm_id,
            path_restrictions: None,
        };
        let stranger_auth = AuthContext {
            user_id: stranger,
            ..member_auth.clone()
        };

        assert!(
            ensure_membership(&state, &member_auth, group_id)
                .await
                .is_ok()
        );
        assert!(matches!(
            ensure_membership(&state, &stranger_auth, group_id).await,
            Err(ServerError::Forbidden)
        ));
    }
}
