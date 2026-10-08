//! Routes to create, list, and delete user bearer sessions.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::{ValidatedBearer, require_realm_auth, require_unrestricted_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::access::credentials::{
    CreatePathRestriction, build_credential_restrictions, serialize_restrictions,
};
use crate::server::state::ServerState;
use aruna_core::structs::identity::auth::{
    Actor, AuthContext, PathRestriction, Permission, SessionKind,
};
use aruna_core::structs::identity::user::session::UserSession;
use aruna_core::time::unix_timestamp_secs;
use aruna_operations::auth::create_token::CreateTokenError;
use aruna_operations::driver::drive;
use aruna_operations::session::{
    CreateSessionConfig, CreateSessionError, CreateSessionOperation, ListSessionOperation,
    RevokeSessionError, RevokeSessionOperation, bound_session_expiry,
};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

#[derive(OpenApi)]
#[openapi(tags((name = "access/sessions", description = "User bearer sessions")))]
pub struct SessionsApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(SessionsApiDoc::openapi())
        .routes(routes!(create_session, list_sessions))
        .routes(routes!(delete_session))
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct CreateSessionRequest {
    #[schema(example = "assistant")]
    pub kind: String,
    pub label: Option<String>,
    pub expires_in_seconds: Option<u64>,
    pub path_restrictions: Option<Vec<CreatePathRestriction>>,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct CreateSessionResponse {
    pub session_id: String,
    pub kind: String,
    pub label: String,
    pub token: String,
    #[schema(example = "2026-04-09T12:00:00Z")]
    pub expires_at: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct SessionSummary {
    pub session_id: String,
    pub kind: String,
    pub label: String,
    pub created_at: String,
    pub expires_at: String,
    pub revoked: bool,
    pub current: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct ListSessionsResponse {
    pub sessions: Vec<SessionSummary>,
}

/// Unix seconds as the RFC 3339 form every other route uses.
pub(crate) fn unix_rfc3339(secs: u64) -> String {
    DateTime::<Utc>::from_timestamp(i64::try_from(secs).unwrap_or(i64::MAX), 0)
        .unwrap_or_default()
        .to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn parse_session_kind(kind: &str) -> ServerResult<SessionKind> {
    match kind {
        "portal" => Ok(SessionKind::Portal),
        "assistant" => Ok(SessionKind::Assistant),
        "api" => Ok(SessionKind::Api),
        _ => Err(ServerError::BadRequestReason(
            "kind must be portal, assistant, or api".to_string(),
        )),
    }
}

pub(crate) fn map_create_error(error: CreateSessionError) -> ServerError {
    match error {
        CreateSessionError::InvalidExpiry => {
            ServerError::BadRequestReason("session expiry is invalid".to_string())
        }
        CreateSessionError::LimitReached => {
            ServerError::Conflict("active session limit reached".to_string())
        }
        CreateSessionError::Token(CreateTokenError::RestrictionsTooLarge) => {
            ServerError::BadRequestReason("path restrictions exceed the allowed size".to_string())
        }
        error => ServerError::InternalError(error.to_string()),
    }
}

/// The caller's own restrictions, narrowed to the requested scopes when any are given.
pub(crate) async fn session_restrictions(
    state: &ServerState,
    auth: &AuthContext,
    requested: Option<Vec<CreatePathRestriction>>,
) -> ServerResult<Option<Vec<PathRestriction>>> {
    if requested.is_none() {
        return Ok(auth.path_restrictions.clone());
    }
    let realm_root = format!("/{}", auth.realm_id);
    let narrowed = build_credential_restrictions(auth, state, &realm_root, requested)
        .await?
        .map(|narrowed| serialize_restrictions(&narrowed))
        .unwrap_or_default();
    if narrowed
        .iter()
        .all(|restriction| restriction.permission == Permission::DENY)
    {
        return Err(ServerError::BadRequestReason(
            "path restrictions need at least one read or write scope".to_string(),
        ));
    }
    Ok(Some(narrowed))
}

fn map_revoke_error(error: RevokeSessionError) -> ServerError {
    match error {
        RevokeSessionError::NotFound => ServerError::NotFound,
        error => ServerError::InternalError(error.to_string()),
    }
}

fn session_summary(session: UserSession, current_sid: Option<&str>) -> SessionSummary {
    SessionSummary {
        current: current_sid.is_some_and(|sid| sid == session.sid),
        session_id: session.sid,
        kind: session.kind.to_string(),
        label: session.label.unwrap_or_default(),
        created_at: unix_rfc3339(session.created_at),
        expires_at: unix_rfc3339(session.expires_at),
        revoked: session.revoked,
    }
}

#[utoipa::path(
    post,
    path = "/access/sessions",
    tag = "access/sessions",
    summary = "Create a user bearer session",
    description = r#"Creates a bearer session for the caller and returns its token once.

**Authentication**: realm bearer token.

**Behavior**
- The session carries the caller's own identity, so it grants no permission the caller does not
  already hold.
- `path_restrictions` narrows the session to the listed scopes. Patterns are absolute permission
  paths or relative to the realm root, ending in `/**` for a subtree. Every read or write scope
  must already be allowed to the caller, and deny rules are kept.
- A path-restricted caller creates a session with its own restrictions, or narrower ones.
- A bound assistant or API session can create only its own kind; a portal session and an unbound
  token may create any kind. A federated session creates none.
- The token is returned in this response only; the session itself stays listable and revocable by
  its id.
- Sessions live on the node that issued them and are not replicated to the realm's other nodes.

**Limits**
- The lifetime is capped by the caller's own remaining lifetime and by 24 hours, which is also the
  default when `expires_in_seconds` is omitted.
- A user may hold at most 256 sessions once expired and revoked ones are pruned."#,
    request_body(
        content = CreateSessionRequest,
        description = "Session kind, an optional label, the requested lifetime in seconds and optional path restrictions",
        example = json!({
            "kind": "assistant",
            "label": "Desktop assistant",
            "expires_in_seconds": 3600
        })
    ),
    responses(
        (status = 201, description = "Session created; the token is shown only here", body = CreateSessionResponse,
            example = json!({
                "session_id": "01JCNCTR0123456789ABCDEFGH",
                "kind": "assistant",
                "label": "Desktop assistant",
                "token": "EXAMPLE-SESSION-TOKEN-PLACEHOLDER",
                "expires_at": "2026-04-09T12:00:00Z"
            })),
        (status = 400, description = "Unknown session kind, a lifetime outside the allowed range, or restrictions without a read or write scope", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "A requested scope exceeds the caller's access, the user is deactivated, the caller holds a federated session, or the token belongs to another realm", body = ErrorResponse),
        (status = 409, description = "The caller already holds 256 active sessions", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn create_session(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    Json(request): Json<CreateSessionRequest>,
) -> ServerResult<(StatusCode, Json<CreateSessionResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    let bearer = bearer.ok_or(ServerError::Unauthorized)?;
    let kind = parse_session_kind(&request.kind)?;
    // A federated session can neither renew itself nor create children of any kind.
    if auth.session.as_ref().is_some_and(|parent| {
        parent.kind == SessionKind::Federated
            || (parent.kind != SessionKind::Portal && parent.kind != kind)
    }) {
        return Err(ServerError::Forbidden);
    }
    let now = unix_timestamp_secs();
    let expiry = bound_session_expiry(now, request.expires_in_seconds, bearer.expires_at_secs())
        .map_err(map_create_error)?;
    crate::routes::access::users::ensure_active(&state, auth.user_id).await?;
    let restrictions = session_restrictions(&state, &auth, request.path_restrictions).await?;
    let created = drive(
        CreateSessionOperation::new(CreateSessionConfig {
            time: now,
            expiry,
            user_id: auth.user_id,
            realm_id: auth.realm_id,
            node_capabilities: state.node_capabilities().clone(),
            kind,
            label: request.label,
            name: None,
            restrictions,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_create_error)?;
    Ok((
        StatusCode::CREATED,
        Json(CreateSessionResponse {
            session_id: created.session.sid,
            kind: created.session.kind.to_string(),
            label: created.session.label.unwrap_or_default(),
            token: created.token.expose().to_string(),
            expires_at: unix_rfc3339(created.session.expires_at),
        }),
    ))
}

#[utoipa::path(
    get,
    path = "/access/sessions",
    tag = "access/sessions",
    summary = "List user bearer sessions",
    description = r#"Lists the caller's own bearer sessions as this node stored them.

**Authentication**: realm bearer token without path restrictions.

**Behavior**
- Only sessions this node issued are listed, so a user with sessions on several nodes asks each of
  them separately.
- `current` marks the session the request itself was authenticated with, and is false for a token
  bound to no session.
- A revoked or expired session stays listed until the next session creation prunes it.
- Tokens are never listed; a token is returned only by the creating request."#,
    responses(
        (status = 200, description = "Sessions stored on this issuing node", body = ListSessionsResponse,
            example = json!({
                "sessions": [{
                    "session_id": "01JCNCTR0123456789ABCDEFGH",
                    "kind": "assistant",
                    "label": "Desktop assistant",
                    "created_at": "2026-04-09T12:00:00Z",
                    "expires_at": "2026-04-10T12:00:00Z",
                    "revoked": false,
                    "current": true
                }]
            })),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token is path-restricted, or belongs to another realm", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_sessions(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<(StatusCode, Json<ListSessionsResponse>)> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let current_sid = auth.session.as_ref().map(|session| session.sid.as_str());
    let sessions = drive(ListSessionOperation::new(auth.user_id), &state.get_ctx())
        .await
        .map_err(|error| ServerError::InternalError(error.to_string()))?
        .into_iter()
        .map(|session| session_summary(session, current_sid))
        .collect();
    Ok((StatusCode::OK, Json(ListSessionsResponse { sessions })))
}

#[utoipa::path(
    delete,
    path = "/access/sessions/{session_id}",
    tag = "access/sessions",
    summary = "Revoke a user bearer session",
    description = r#"Revokes one of the caller's own bearer sessions on the node that issued it.

**Authentication**: realm bearer token without path restrictions.

**Behavior**
- Every token bound to the session is refused from here on, including the one the request itself
  was made with.
- Revoking an already revoked session, or an id that is not a ULID, still answers 204.
- The revocation is node-local, like the session it ends."#,
    params(("session_id" = String, Path, description = "Session ULID")),
    responses(
        (status = 204, description = "Session revoked, or already revoked by an earlier call"),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token is path-restricted, or belongs to another realm", body = ErrorResponse),
        (status = 404, description = "The session belongs to another user", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn delete_session(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(session_id): Path<String>,
) -> ServerResult<StatusCode> {
    let auth = require_unrestricted_auth(&state, auth)?;
    if Ulid::from_string(&session_id).is_err() {
        return Ok(StatusCode::NO_CONTENT);
    }
    drive(
        RevokeSessionOperation::new(
            Actor {
                node_id: state.get_node_id(),
                user_id: auth.user_id,
                realm_id: auth.realm_id,
            },
            session_id,
            unix_timestamp_secs(),
        ),
        &state.get_ctx(),
    )
    .await
    .map_err(map_revoke_error)?;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::handle_token;
    use crate::error::TokenError;
    use crate::tests::routes::{test_context, test_state, test_storage};
    use aruna_core::UserId;
    use aruna_core::keys::generate_signing_key;
    use aruna_core::structs::identity::auth::{NodeCapabilities, SessionRef};
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_operations::realm::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use axum::response::IntoResponse;
    use tempfile::TempDir;

    async fn setup_state() -> (TempDir, Arc<ServerState>, AuthContext) {
        let (dir, storage) = test_storage();
        let context = Arc::new(test_context(storage));
        let signing_key = generate_signing_key();
        let realm_id = RealmId::from_bytes(signing_key.verifying_key().to_bytes());
        let user_id = UserId::local(Ulid::generate(), realm_id);
        let node_id = iroh::SecretKey::generate().public();
        drive(
            CreateRealmOperation::new(CreateRealmConfig {
                actor: Actor {
                    node_id,
                    user_id: UserId::nil(realm_id),
                    realm_id,
                },
                realm_description: "Realm".to_string(),
                oidc_providers: Vec::new(),
                node_location: None,
                node_weight: None,
                node_labels: Default::default(),
            }),
            &context,
        )
        .await
        .unwrap();
        let state = Arc::new(
            test_state(
                context,
                realm_id,
                node_id,
                NodeCapabilities::management_node(signing_key).unwrap(),
            )
            .await,
        );
        let auth = AuthContext {
            user_id,
            realm_id,
            path_restrictions: None,
            session: None,
        };
        (dir, state, auth)
    }

    #[tokio::test]
    async fn invalid_kind_rejected() {
        let (_dir, state, auth) = setup_state().await;
        let error = create_session(
            State(state),
            Extension(Some(auth)),
            Extension(Some(ValidatedBearer::new_for_test("parent"))),
            Json(CreateSessionRequest {
                kind: "unknown".to_string(),
                label: None,
                expires_in_seconds: None,
                path_restrictions: None,
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(error.into_response().status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn rejects_kind_change() {
        let (_dir, state, mut auth) = setup_state().await;
        auth.session = Some(SessionRef {
            sid: Ulid::generate().to_string(),
            kind: SessionKind::Assistant,
            name: None,
        });
        let error = create_session(
            State(state.clone()),
            Extension(Some(auth)),
            Extension(Some(ValidatedBearer::new_for_test("parent"))),
            Json(CreateSessionRequest {
                kind: "portal".to_string(),
                label: None,
                expires_in_seconds: None,
                path_restrictions: None,
            }),
        )
        .await
        .unwrap_err();

        assert_eq!(error.into_response().status(), StatusCode::FORBIDDEN);
        // The refusal must not have written a session behind the denial.
        assert!(session_rows(&state).await.is_empty());
    }

    /// State whose caller owns one group, with the group data root.
    async fn group_state() -> (TempDir, Arc<ServerState>, AuthContext, String) {
        let (dir, state, auth) = setup_state().await;
        let group_id = Ulid::generate();
        let actor = Actor {
            node_id: state.get_node_id(),
            user_id: auth.user_id,
            realm_id: auth.realm_id,
        };
        crate::tests::routes::seed_group_docs(
            &state.get_ctx(),
            auth.realm_id,
            &actor,
            group_id,
            "scoped",
            auth.user_id,
        )
        .await;
        let root = format!("/{}/g/{group_id}/data", auth.realm_id);
        (dir, state, auth, root)
    }

    fn scope(pattern: String, permission: &str) -> CreatePathRestriction {
        CreatePathRestriction {
            pattern,
            permission: permission.to_string(),
        }
    }

    async fn create_scoped(
        state: &Arc<ServerState>,
        auth: AuthContext,
        scopes: Option<Vec<CreatePathRestriction>>,
    ) -> ServerResult<CreateSessionResponse> {
        create_session(
            State(state.clone()),
            Extension(Some(auth)),
            Extension(Some(ValidatedBearer::new_for_test("parent"))),
            Json(CreateSessionRequest {
                kind: "api".to_string(),
                label: None,
                expires_in_seconds: Some(600),
                path_restrictions: scopes,
            }),
        )
        .await
        .map(|(_, Json(created))| created)
    }

    #[tokio::test]
    async fn narrows_to_scope() {
        let (_dir, state, auth, root) = group_state().await;
        let created = create_scoped(
            &state,
            auth,
            Some(vec![
                scope(format!("{root}/**"), "READ"),
                scope(format!("{root}/secret/**"), "DENY"),
            ]),
        )
        .await
        .unwrap();

        let claims = handle_token(&state, &created.token).await.unwrap();
        assert_eq!(
            claims.restrictions,
            Some(vec![
                PathRestriction {
                    pattern: format!("{root}/**"),
                    permission: Permission::READ,
                },
                PathRestriction {
                    pattern: format!("{root}/secret/**"),
                    permission: Permission::DENY,
                },
            ])
        );
    }

    #[tokio::test]
    async fn restricted_cannot_widen() {
        let (_dir, state, mut auth, root) = group_state().await;
        auth.path_restrictions = Some(vec![PathRestriction {
            pattern: format!("{root}/public/**"),
            permission: Permission::READ,
        }]);
        for requested in [
            scope(format!("{root}/**"), "READ"),
            scope(format!("{root}/public/**"), "WRITE"),
        ] {
            let error = create_scoped(&state, auth.clone(), Some(vec![requested]))
                .await
                .unwrap_err();
            assert_eq!(error.into_response().status(), StatusCode::FORBIDDEN);
        }
        assert!(session_rows(&state).await.is_empty());
    }

    #[tokio::test]
    async fn restricted_inherits_scope() {
        let (_dir, state, mut auth, root) = group_state().await;
        let restrictions = vec![PathRestriction {
            pattern: format!("{root}/public/**"),
            permission: Permission::READ,
        }];
        auth.path_restrictions = Some(restrictions.clone());

        let inherited = create_scoped(&state, auth.clone(), None).await.unwrap();
        let narrower = create_scoped(
            &state,
            auth,
            Some(vec![scope(format!("{root}/public/raw/**"), "READ")]),
        )
        .await
        .unwrap();

        let claims = handle_token(&state, &inherited.token).await.unwrap();
        assert_eq!(claims.restrictions, Some(restrictions));
        let claims = handle_token(&state, &narrower.token).await.unwrap();
        assert_eq!(
            claims.restrictions,
            Some(vec![PathRestriction {
                pattern: format!("{root}/public/raw/**"),
                permission: Permission::READ,
            }])
        );
    }

    #[tokio::test]
    async fn outside_scope_refused() {
        let (_dir, state, auth, _root) = group_state().await;
        let other = format!("/{}/g/{}/data/**", auth.realm_id, Ulid::generate());
        let error = create_scoped(&state, auth, Some(vec![scope(other, "READ")]))
            .await
            .unwrap_err();
        assert_eq!(error.into_response().status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn deny_only_rejected() {
        let (_dir, state, auth, root) = group_state().await;
        for scopes in [vec![], vec![scope(format!("{root}/**"), "DENY")]] {
            let error = create_scoped(&state, auth.clone(), Some(scopes))
                .await
                .unwrap_err();
            assert_eq!(error.into_response().status(), StatusCode::BAD_REQUEST);
        }
        assert!(session_rows(&state).await.is_empty());
    }

    /// Every persisted session row, for proving a denial wrote nothing.
    async fn session_rows(state: &ServerState) -> Vec<(byteview::ByteView, byteview::ByteView)> {
        use aruna_core::effects::{Effect, StorageEffect};
        use aruna_core::events::{Event, StorageEvent};
        use aruna_core::handle::Handle;
        use aruna_core::keyspaces::USER_SESSION_KEYSPACE;
        match state
            .get_ctx()
            .storage_handle
            .send_effect(Effect::Storage(StorageEffect::Iter {
                key_space: USER_SESSION_KEYSPACE.to_string(),
                prefix: None,
                start: None,
                limit: 16,
                txn_id: None,
            }))
            .await
        {
            Event::Storage(StorageEvent::IterResult { values, .. }) => values,
            other => panic!("unexpected session iter result: {other:?}"),
        }
    }

    #[tokio::test]
    async fn revoked_token_rejected() {
        let (_dir, state, mut auth) = setup_state().await;
        auth.session = Some(SessionRef {
            sid: Ulid::generate().to_string(),
            kind: SessionKind::Portal,
            name: None,
        });
        let (_, Json(created)) = create_session(
            State(state.clone()),
            Extension(Some(auth.clone())),
            Extension(Some(ValidatedBearer::new_for_test("parent"))),
            Json(CreateSessionRequest {
                kind: "assistant".to_string(),
                label: None,
                expires_in_seconds: Some(600),
                path_restrictions: None,
            }),
        )
        .await
        .unwrap();
        delete_session(
            State(state.clone()),
            Extension(Some(auth)),
            Path(created.session_id),
        )
        .await
        .unwrap();

        assert!(matches!(
            handle_token(&state, &created.token).await,
            Err(TokenError::TokenBlacklisted)
        ));
    }

    #[tokio::test]
    async fn foreign_session_hidden() {
        let (_dir, state, auth) = setup_state().await;
        let (_, Json(created)) = create_session(
            State(state.clone()),
            Extension(Some(auth.clone())),
            Extension(Some(ValidatedBearer::new_for_test("parent"))),
            Json(CreateSessionRequest {
                kind: "api".to_string(),
                label: None,
                expires_in_seconds: Some(600),
                path_restrictions: None,
            }),
        )
        .await
        .unwrap();
        let stranger = AuthContext {
            user_id: UserId::local(Ulid::generate(), auth.realm_id),
            ..auth
        };
        let error = delete_session(
            State(state),
            Extension(Some(stranger)),
            Path(created.session_id),
        )
        .await
        .unwrap_err();

        assert_eq!(error.into_response().status(), StatusCode::NOT_FOUND);
    }
}
