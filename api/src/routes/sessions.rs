use crate::auth::{ValidatedArunaBearerTokenCarrier, require_unrestricted_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server_state::ServerState;
use aruna_core::structs::{Actor, AuthContext, SessionKind, UserSession};
use aruna_core::util::unix_timestamp_secs;
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
#[openapi(tags((name = "sessions", description = "User bearer sessions")))]
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

fn map_create_error(error: CreateSessionError) -> ServerError {
    match error {
        CreateSessionError::InvalidExpiry => {
            ServerError::BadRequestReason("session expiry is invalid".to_string())
        }
        error => ServerError::InternalError(error.to_string()),
    }
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
    path = "/users/sessions",
    tag = "sessions",
    summary = "Create a user bearer session",
    description = "Creates a self-scoped bearer session. The lifetime is capped by the caller's remaining lifetime and 24 hours. The token is shown only once.",
    request_body = CreateSessionRequest,
    responses(
        (status = 201, description = "Session created", body = CreateSessionResponse),
        (status = 400, description = "Invalid kind or lifetime", body = ErrorResponse),
        (status = 401, description = "No usable bearer token", body = ErrorResponse),
        (status = 403, description = "Restricted or foreign bearer token", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn create_session(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedArunaBearerTokenCarrier>>,
    Json(request): Json<CreateSessionRequest>,
) -> ServerResult<(StatusCode, Json<CreateSessionResponse>)> {
    let auth = require_unrestricted_realm_auth(&state, auth)?;
    let bearer = bearer.ok_or(ServerError::Unauthorized)?;
    let kind = parse_session_kind(&request.kind)?;
    let now = unix_timestamp_secs();
    let expiry = bound_session_expiry(now, request.expires_in_seconds, bearer.expires_at_secs())
        .map_err(map_create_error)?;
    let created = drive(
        CreateSessionOperation::new(CreateSessionConfig {
            time: now,
            expiry,
            user_id: auth.user_id,
            realm_id: auth.realm_id,
            node_capabilities: state.node_capabilities().clone(),
            kind,
            label: request.label,
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
    path = "/users/sessions",
    tag = "sessions",
    summary = "List user bearer sessions",
    responses(
        (status = 200, description = "Sessions stored on this issuing node", body = ListSessionsResponse),
        (status = 401, description = "No usable bearer token", body = ErrorResponse),
        (status = 403, description = "Restricted or foreign bearer token", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_sessions(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<(StatusCode, Json<ListSessionsResponse>)> {
    let auth = require_unrestricted_realm_auth(&state, auth)?;
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
    path = "/users/sessions/{session_id}",
    tag = "sessions",
    summary = "Revoke a user bearer session",
    params(("session_id" = String, Path, description = "Session ULID")),
    responses(
        (status = 204, description = "Session revoked or already absent"),
        (status = 401, description = "No usable bearer token", body = ErrorResponse),
        (status = 403, description = "Restricted or foreign bearer token", body = ErrorResponse),
        (status = 404, description = "Session belongs to another user", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn delete_session(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(session_id): Path<String>,
) -> ServerResult<StatusCode> {
    let auth = require_unrestricted_realm_auth(&state, auth)?;
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
    use aruna_core::keys::generate_signing_key;
    use aruna_core::structs::{NodeCapabilities, RealmId};
    use aruna_core::types::UserId;
    use aruna_operations::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use aruna_operations::driver::DriverContext;
    use aruna_operations::jobs::runtime::JobsRuntime;
    use aruna_storage::storage::FjallStorage;
    use axum::response::IntoResponse;
    use tempfile::TempDir;

    async fn setup_state() -> (TempDir, Arc<ServerState>, AuthContext) {
        let dir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });
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
            ServerState::new(
                context,
                realm_id,
                node_id,
                NodeCapabilities::management_node(signing_key).unwrap(),
                false,
                None,
                JobsRuntime::new(),
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
            Extension(Some(ValidatedArunaBearerTokenCarrier::new_for_test(
                "parent",
            ))),
            Json(CreateSessionRequest {
                kind: "unknown".to_string(),
                label: None,
                expires_in_seconds: None,
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(error.into_response().status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn revoked_token_rejected() {
        let (_dir, state, auth) = setup_state().await;
        let (_, Json(created)) = create_session(
            State(state.clone()),
            Extension(Some(auth.clone())),
            Extension(Some(ValidatedArunaBearerTokenCarrier::new_for_test(
                "parent",
            ))),
            Json(CreateSessionRequest {
                kind: "assistant".to_string(),
                label: None,
                expires_in_seconds: Some(600),
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
            Extension(Some(ValidatedArunaBearerTokenCarrier::new_for_test(
                "parent",
            ))),
            Json(CreateSessionRequest {
                kind: "api".to_string(),
                label: None,
                expires_in_seconds: Some(600),
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
