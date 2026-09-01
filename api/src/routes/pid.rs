//! Automatic typed w3id lifecycle and landing resolution. Ordinary documents
//! use `https://w3id.org/aruna/{document_id}`; Profiles use only
//! `https://w3id.org/aruna/profile/{document_id}`. Every lifecycle read and
//! transition routes to the document's single PID authority.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use aruna_core::structs::{
    AuthContext, MetadataRegistryRecord, Permission, PersistentIdFailure, PersistentIdKind,
    PersistentIdMapping, PersistentIdProvider, PersistentIdStatus,
};
use aruna_core::util::unix_timestamp_millis;
use aruna_operations::get_metadata_document::load_metadata_record_by_document;
use aruna_operations::metadata::PersistentIdResolution;
use aruna_operations::metadata::api::MetadataApiError;
use aruna_operations::metadata::forward::{
    read_pid_routed, resolve_pid_routed, withdraw_pid_routed,
};

use crate::auth::{
    ValidatedArunaBearerTokenCarrier, ensure_permission, require_unrestricted_realm_auth,
};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::metadata::{forwarded_auth_token, map_metadata_api_error};
use crate::server_state::ServerState;

#[derive(OpenApi)]
#[openapi(
    tags(
        (name = "pid", description = "w3id persistent identifier landing and lifecycle"),
        (name = "metadata/pids", description = "Persistent identifiers for metadata documents")
    )
)]
pub struct PidApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(PidApiDoc::openapi())
        .routes(routes!(resolve_pid, withdraw_pid))
        .routes(routes!(resolve_profile_pid))
        .routes(routes!(list_persistent_ids))
}

fn rocrate_location(document_id: Ulid) -> String {
    format!("/api/v1/metadata/{document_id}/rocrate")
}

#[utoipa::path(
    get,
    path = "/pid/{document_id}",
    tag = "pid",
    summary = "Resolve a w3id persistent identifier",
    description = r#"Public landing route for the ordinary identity `https://w3id.org/aruna/{document_id}`.

**Authentication**: none; public route, no bearer token is read.

**Behavior**: a Profile resolves under `/profile/{document_id}`, so this path answers 404 for a
Profile and never acts as a duplicate PID."#,
    params(("document_id" = String, Path, description = "Document ULID carried by the w3id PID, for example 01JMETADATA0123456789ABCDE")),
    responses(
        (status = 302, description = "Active public identifier; the Location header points at the document's RO-Crate read route, which applies its own authorization"),
        (status = 404, description = "Unknown, malformed, private or Profile identifier, or a mapping still in `requested`, `processing` or `failed`"),
        (
            status = 410,
            description = "The identifier was withdrawn and stays withdrawn",
            body = Object,
            content_type = "application/json",
            example = json!({
                "pid": "https://w3id.org/aruna/01JMETADATA0123456789ABCDE",
                "status": "withdrawn"
            })
        ),
        (status = 503, description = "The PID authority is unreachable, which is reported instead of a false 404; retryable")
    ),
    security(())
)]
async fn resolve_pid(
    State(state): State<Arc<ServerState>>,
    Path(document_id): Path<String>,
) -> Response {
    let Ok(document_id) = Ulid::from_string(&document_id) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let ctx = state.get_ctx();
    let resolved = resolve_pid_routed(
        &ctx,
        state.get_realm_id(),
        document_id,
        MetadataRegistryRecord::graph_iri_for(document_id),
    )
    .await;
    landing_response(document_id, resolved)
}

#[utoipa::path(
    get,
    path = "/profile/{document_id}",
    tag = "pid",
    summary = "Resolve a Profile w3id persistent identifier",
    description = r#"Public landing route for the Profile identity `https://w3id.org/aruna/profile/{document_id}`.

**Authentication**: none; public route, no bearer token is read.

**Behavior**: resolves only when that exact value is the document's stored automatic primary PID.
Privacy, lifecycle and authority handling match `GET /pid/{document_id}`."#,
    params(("document_id" = String, Path, description = "Profile document ULID")),
    responses(
        (status = 302, description = "Active public Profile identifier; the Location header points at the RO-Crate read route"),
        (status = 404, description = "Unknown, private, non-Profile or non-active identifier"),
        (status = 410, description = "The Profile identifier was withdrawn and stays withdrawn"),
        (status = 503, description = "The PID authority is unreachable; retryable")
    ),
    security(())
)]
async fn resolve_profile_pid(
    State(state): State<Arc<ServerState>>,
    Path(document_id): Path<String>,
) -> Response {
    let Ok(document_id) = Ulid::from_string(&document_id) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let ctx = state.get_ctx();
    let resolved = resolve_pid_routed(
        &ctx,
        state.get_realm_id(),
        document_id,
        PersistentIdMapping::profile_pid(document_id),
    )
    .await;
    landing_response(document_id, resolved)
}

fn landing_response(
    document_id: Ulid,
    resolved: Result<PersistentIdResolution, MetadataApiError>,
) -> Response {
    match resolved {
        Ok(PersistentIdResolution::Redirect) => redirect(&rocrate_location(document_id)),
        Ok(PersistentIdResolution::Gone { pid }) => gone(&pid),
        Ok(PersistentIdResolution::Missing) => StatusCode::NOT_FOUND.into_response(),
        Err(MetadataApiError::NotFound) => StatusCode::NOT_FOUND.into_response(),
        Err(_) => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

fn redirect(location: &str) -> Response {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::FOUND;
    if let Ok(value) = HeaderValue::from_str(location) {
        response.headers_mut().insert(header::LOCATION, value);
    }
    response
}

fn gone(pid: &str) -> Response {
    (
        StatusCode::GONE,
        Json(serde_json::json!({ "pid": pid, "status": "withdrawn" })),
    )
        .into_response()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "kebab-case")]
enum PersistentIdStateView {
    Requested,
    Processing,
    Active,
    Failed,
    AdminWithdrawn,
    Tombstoned,
    Unknown,
}

#[derive(Debug, Serialize, ToSchema)]
struct PersistentIdFailureView {
    message: String,
    retryable: bool,
    recorded_at_ms: u64,
}

#[derive(Debug, Serialize, ToSchema)]
struct PersistentIdView {
    kind: String,
    provider: String,
    value: Option<String>,
    state: PersistentIdStateView,
    document_id: String,
    job_id: Option<String>,
    failure: Option<PersistentIdFailureView>,
    requested_at_ms: Option<u64>,
    minted_at_ms: Option<u64>,
    withdrawn_at_ms: Option<u64>,
}

fn status_view(mapping: &PersistentIdMapping) -> PersistentIdView {
    let state = match mapping.status {
        PersistentIdStatus::Requested => PersistentIdStateView::Requested,
        PersistentIdStatus::Processing => PersistentIdStateView::Processing,
        PersistentIdStatus::Active => PersistentIdStateView::Active,
        PersistentIdStatus::Failed => PersistentIdStateView::Failed,
        PersistentIdStatus::AdminWithdrawn => PersistentIdStateView::AdminWithdrawn,
        PersistentIdStatus::Tombstoned => PersistentIdStateView::Tombstoned,
    };
    PersistentIdView {
        kind: match mapping.kind {
            PersistentIdKind::Conceptual => "conceptual".to_string(),
        },
        provider: match mapping.provider {
            PersistentIdProvider::W3id => "w3id".to_string(),
        },
        value: Some(mapping.pid.clone()),
        state,
        document_id: mapping.target.to_string(),
        job_id: mapping.job_id.map(|job_id| job_id.to_string()),
        failure: mapping.failure.as_ref().map(
            |PersistentIdFailure {
                 message,
                 retryable,
                 recorded_at_ms,
             }| PersistentIdFailureView {
                message: message.clone(),
                retryable: *retryable,
                recorded_at_ms: *recorded_at_ms,
            },
        ),
        requested_at_ms: mapping.requested_at_ms,
        minted_at_ms: mapping.minted_at_ms,
        withdrawn_at_ms: mapping.withdrawn_at_ms,
    }
}

fn synthetic_status_view(
    document_id: Ulid,
    value: Option<String>,
    state: PersistentIdStateView,
) -> PersistentIdView {
    PersistentIdView {
        kind: "conceptual".to_string(),
        provider: "w3id".to_string(),
        value,
        state,
        document_id: document_id.to_string(),
        job_id: None,
        failure: None,
        requested_at_ms: None,
        minted_at_ms: None,
        withdrawn_at_ms: None,
    }
}

async fn require_status_visibility(
    state: &ServerState,
    auth: Option<&AuthContext>,
    mapping: Option<&PersistentIdMapping>,
    record: Option<&MetadataRegistryRecord>,
) -> ServerResult<()> {
    let public = mapping
        .and_then(|mapping| mapping.public)
        .or_else(|| record.map(|record| record.public));
    if public == Some(true) {
        return Ok(());
    }
    let Some(auth) = auth else {
        return Err(ServerError::NotFound);
    };
    let path = mapping
        .and_then(|mapping| mapping.permission_path.clone())
        .or_else(|| record.map(|record| record.permission_path.clone()))
        .unwrap_or_else(|| format!("/{}/admin/pids/**", state.get_realm_id()));
    ensure_permission(state, auth, path, Permission::READ).await
}

/// Authenticated typed status is sourced only from the durable PID mapping.
/// `requested`, `processing`, `active`, `failed`, `admin-withdrawn` and
/// `tombstoned` are its stored lifecycle states; failure and job fields come
/// from that same row, never from a possibly unreadable job endpoint.
/// `unknown` means the caller may read the document but the authority cannot
/// currently give a definitive record. Anonymous access to a private
/// mapping or registry row remains 404 and is not an existence oracle.
#[utoipa::path(
    get,
    path = "/metadata/{document_id}/pids",
    tag = "metadata/pids",
    summary = "List a document's typed persistent identifiers",
    description = r#"Returns the one stored automatic w3id record for a document as a typed list.

**Authentication**: optional bearer token; a public record may be read anonymously, a private one
needs READ on the document's frozen permission path and answers 404 anonymously.

**Behavior**
- The stored mapping is authoritative for the `requested`, `processing`, `active`, `failed`,
  `admin-withdrawn` and `tombstoned` states and for the job and failure fields, so a failed job
  read is never reported as unminted.
- A missing or unavailable authority mapping is reported as `unknown`."#,
    params(("document_id" = String, Path, description = "Metadata document ULID")),
    responses(
        (
            status = 200,
            description = "Typed list containing the automatic w3id status",
            body = [PersistentIdView],
            example = json!([
                {
                    "kind": "conceptual",
                    "provider": "w3id",
                    "value": "https://w3id.org/aruna/01JMETADATA0123456789ABCDE",
                    "state": "active",
                    "document_id": "01JMETADATA0123456789ABCDE",
                    "job_id": "01JABCDEF0123456789ABCDEFG",
                    "failure": null,
                    "requested_at_ms": 1755500000000u64,
                    "minted_at_ms": 1755500001000u64,
                    "withdrawn_at_ms": null
                }
            ])
        ),
        (status = 400, description = "Malformed document id", body = ErrorResponse),
        (status = 403, description = "Caller lacks READ on the document", body = ErrorResponse),
        (status = 404, description = "Document not found, or a private record read anonymously", body = ErrorResponse)
    ),
    security(("bearer_auth" = []), ())
)]
async fn list_persistent_ids(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(document_id): Path<String>,
) -> ServerResult<Json<Vec<PersistentIdView>>> {
    let document_id = Ulid::from_string(&document_id).map_err(|_| ServerError::BadRequest)?;
    let ctx = state.get_ctx();
    let record = load_metadata_record_by_document(&ctx, document_id)
        .await
        .map_err(|error| ServerError::InternalError(format!("{error:?}")))?;
    let routed = read_pid_routed(&ctx, state.get_realm_id(), document_id).await;
    match routed {
        Ok(Some(mapping)) => {
            require_status_visibility(&state, auth.as_ref(), Some(&mapping), record.as_ref())
                .await?;
            Ok(Json(vec![status_view(&mapping)]))
        }
        Ok(None) => {
            let record = record.as_ref().ok_or(ServerError::NotFound)?;
            require_status_visibility(&state, auth.as_ref(), None, Some(record)).await?;
            Ok(Json(vec![synthetic_status_view(
                document_id,
                None,
                PersistentIdStateView::Unknown,
            )]))
        }
        Err(_) => {
            let record = record.as_ref().ok_or(ServerError::NotFound)?;
            require_status_visibility(&state, auth.as_ref(), None, Some(record)).await?;
            Ok(Json(vec![synthetic_status_view(
                document_id,
                None,
                PersistentIdStateView::Unknown,
            )]))
        }
    }
}

#[derive(Debug, Deserialize, ToSchema)]
struct WithdrawPersistentIdRequest {
    provider: String,
    confirm_pid: String,
    reason: String,
}

fn validated_withdrawal_reason(request: &WithdrawPersistentIdRequest) -> ServerResult<String> {
    let reason = request.reason.trim();
    if request.provider != "w3id"
        || reason.is_empty()
        || reason.len() > 1_024
        || reason.chars().any(char::is_control)
    {
        return Err(ServerError::BadRequest);
    }
    Ok(reason.to_string())
}

/// Exceptional admin-only withdrawal. Normal deletion uses the distinct
/// tombstone transition; owners with only document WRITE cannot call this path.
#[utoipa::path(
    delete,
    path = "/pid/{document_id}",
    tag = "pid",
    summary = "Withdraw a document's w3id persistent identifier",
    description = r#"Exceptional administration route that terminally withdraws a document's w3id identifier.

**Authentication**: unrestricted realm bearer token with WRITE on
`/{realm}/admin/pids/{document_id}`; document WRITE alone is deliberately insufficient.

**Behavior**
- The authority stores `admin-withdrawn`, the actor and the reason, and writes a
  `WithdrawPersistentId` metadata audit row in the same transaction.
- Normal document deletion instead stores `tombstoned`.
- The transition is terminal and idempotent.

**Limits**
- `provider` must be `w3id`.
- `confirm_pid` must equal the stored PID value exactly.
- `reason` is trimmed first and must then be 1 to 1024 bytes long and free of control characters."#,
    params(("document_id" = String, Path, description = "Document ULID whose PID is withdrawn, for example 01JMETADATA0123456789ABCDE")),
    request_body(
        content = WithdrawPersistentIdRequest,
        description = "Provider, the exact stored PID as confirmation, and a non-empty reason",
        example = json!({
            "provider": "w3id",
            "confirm_pid": "https://w3id.org/aruna/01JMETADATA0123456789ABCDE",
            "reason": "published in error"
        })
    ),
    responses(
        (status = 204, description = "The withdrawal is durable on the PID authority"),
        (status = 400, description = "Malformed id, unsupported provider, wrong confirmation, or invalid reason", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Caller lacks WRITE on the realm PID administration path", body = ErrorResponse),
        (status = 404, description = "No identifier is registered for this document", body = ErrorResponse),
        (status = 503, description = "The PID authority is unreachable; retryable", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn withdraw_pid(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer_token): Extension<Option<ValidatedArunaBearerTokenCarrier>>,
    Path(document_id): Path<String>,
    Json(request): Json<WithdrawPersistentIdRequest>,
) -> ServerResult<StatusCode> {
    let auth = require_unrestricted_realm_auth(&state, auth)?;
    let document_id = Ulid::from_string(&document_id).map_err(|_| ServerError::BadRequest)?;
    let reason = validated_withdrawal_reason(&request)?;
    let ctx = state.get_ctx();
    ensure_permission(
        &state,
        &auth,
        format!("/{}/admin/pids/{document_id}", state.get_realm_id()),
        Permission::WRITE,
    )
    .await?;
    let existing = read_pid_routed(&ctx, state.get_realm_id(), document_id)
        .await
        .map_err(map_metadata_api_error)?
        .ok_or(ServerError::NotFound)?;
    if request.confirm_pid != existing.pid {
        return Err(ServerError::BadRequest);
    }
    let mapping = withdraw_pid_routed(
        &ctx,
        state.get_realm_id(),
        document_id,
        auth.user_id,
        reason,
        unix_timestamp_millis(),
        forwarded_auth_token(bearer_token)?,
    )
    .await
    .map_err(map_metadata_api_error)?;
    if mapping.status != PersistentIdStatus::AdminWithdrawn {
        return Err(ServerError::ServiceUnavailable);
    }
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;

    // the RO-Crate Location header targets the read route
    #[test]
    fn location_targets_read() {
        let id = Ulid::from_bytes([1; 16]);
        assert_eq!(
            rocrate_location(id),
            format!("/api/v1/metadata/{id}/rocrate")
        );
    }

    #[test]
    fn landing_maps_resolutions() {
        let id = Ulid::from_bytes([1; 16]);
        let status = |resolved| landing_response(id, resolved).status();

        assert_eq!(
            status(Ok(PersistentIdResolution::Redirect)),
            StatusCode::FOUND
        );
        assert_eq!(
            status(Ok(PersistentIdResolution::Gone {
                pid: "pid".to_string()
            })),
            StatusCode::GONE
        );
        assert_eq!(
            status(Ok(PersistentIdResolution::Missing)),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            status(Err(MetadataApiError::NotFound)),
            StatusCode::NOT_FOUND
        );
        // An unreachable authority is never a 404: that would turn a live PID dead.
        assert_eq!(
            status(Err(MetadataApiError::ServiceUnavailable)),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[test]
    fn stored_intent_does_not_depend_on_job_readability() {
        let id = Ulid::from_bytes([3; 16]);
        let mapping = PersistentIdMapping::requested(
            id,
            false,
            aruna_core::UserId::local(
                Ulid::from_bytes([4; 16]),
                aruna_core::structs::RealmId([5; 32]),
            ),
            aruna_core::structs::JobId::from_bytes([6; 16]),
            false,
            "/private/document".to_string(),
            aruna_core::structs::PersistentIdRevision {
                event_id: Ulid::from_bytes([7; 16]),
                actor: iroh::SecretKey::from_bytes(&[8; 32]).public(),
                occurred_at_ms: 9,
            },
        );

        let view = status_view(&mapping);
        assert_eq!(view.state, PersistentIdStateView::Requested);
        assert_eq!(view.job_id, mapping.job_id.map(|job_id| job_id.to_string()));
        assert_eq!(view.value.as_deref(), Some(mapping.pid.as_str()));
    }

    #[test]
    fn admin_withdrawal_requires_provider_confirmation_fields() {
        let valid = WithdrawPersistentIdRequest {
            provider: "w3id".to_string(),
            confirm_pid: "https://w3id.org/aruna/example".to_string(),
            reason: "  duplicate external registration  ".to_string(),
        };
        assert_eq!(
            validated_withdrawal_reason(&valid).unwrap(),
            "duplicate external registration"
        );
        assert!(
            validated_withdrawal_reason(&WithdrawPersistentIdRequest {
                provider: "doi".to_string(),
                ..valid
            })
            .is_err()
        );
    }
}
