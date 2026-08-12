//! w3id persistent-identifier landing resolution. A PID is the document graph IRI
//! `https://w3id.org/aruna/{document_id}`; every operation routes to the single
//! authority that reads and writes the mapping.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use ulid::Ulid;
use utoipa::OpenApi;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use aruna_core::structs::{
    AuthContext, DEFAULT_JOB_RETENTION_MS, MetadataRegistryRecord, MintPersistentIdSpec,
    Permission, PersistentIdStatus,
};
use aruna_core::util::unix_timestamp_millis;
use aruna_operations::get_metadata_document::load_metadata_record_by_document;
use aruna_operations::jobs::service::submit_mint_pid;
use aruna_operations::jobs::submit::SubmitJobError;
use aruna_operations::metadata::PersistentIdResolution;
use aruna_operations::metadata::api::MetadataApiError;
use aruna_operations::metadata::forward::{resolve_pid_routed, withdraw_pid_routed};

use crate::auth::{
    ValidatedArunaBearerTokenCarrier, ensure_permission, require_unrestricted_realm_auth,
};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::metadata::{forwarded_auth_token, map_metadata_api_error};
use crate::server_state::ServerState;

#[derive(OpenApi)]
#[openapi(
    tags((name = "pid", description = "w3id persistent identifier landing and lifecycle"))
)]
pub struct PidApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(PidApiDoc::openapi()).routes(routes!(
        resolve_pid,
        mint_pid,
        withdraw_pid
    ))
}

fn rocrate_location(document_id: Ulid) -> String {
    format!("/api/v1/metadata/{document_id}/rocrate")
}

#[utoipa::path(
    get,
    path = "/pid/{document_id}",
    tag = "pid",
    summary = "Resolve a w3id persistent identifier",
    description = "Public landing route behind https://w3id.org/aruna/{document_id}: no bearer token is required or read. A registered identifier answers 302 to the document's RO-Crate read route, which then applies its own authorization, so this route reveals no document content. A withdrawn identifier answers a permanent 410 tombstone and never returns to 302. An unknown or malformed identifier answers 404, which deliberately does not distinguish a never-minted identifier from a document the caller may not see. The mapping is read from the document's single PID authority; when that node is unreachable the answer is 503 rather than 404, so a live identifier never reads as dead.",
    params(("document_id" = String, Path, description = "Document ULID carried by the w3id PID, for example 01JMETADATA0123456789ABCDE")),
    responses(
        (status = 302, description = "Registered identifier; the Location header points at /api/v1/metadata/{document_id}/rocrate and the body is empty"),
        (status = 404, description = "No identifier is registered for this document, or the identifier is malformed; the body is empty"),
        (
            status = 410,
            description = "The identifier was withdrawn and stays withdrawn",
            body = Object,
            content_type = "application/json",
            example = json!({"pid": "https://w3id.org/aruna/01JMETADATA0123456789ABCDE", "status": "withdrawn"})
        ),
        (status = 503, description = "The PID authority is unreachable; retry the same request later")
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
    let resolved = resolve_pid_routed(&ctx, state.get_realm_id(), document_id).await;
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

/// The submission runs on the document's PID authority, so its failures are that
/// node's answer about the document, not an internal fault of this one.
fn mint_submit_error(error: SubmitJobError) -> ServerError {
    match error {
        SubmitJobError::DocumentMissing => ServerError::NotFound,
        SubmitJobError::AuthorityDenied => ServerError::Forbidden,
        SubmitJobError::PlacementUnavailable(_) => ServerError::ServiceUnavailable,
        error => ServerError::InternalError(error.to_string()),
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

/// Register a w3id PID for a document. Idempotent by document id; requires WRITE
/// on the document. Runs as a fenced job, so the response is 202 Accepted.
#[utoipa::path(
    post,
    path = "/pid/{document_id}",
    tag = "pid",
    summary = "Mint a w3id persistent identifier for a document",
    description = "Requires an unrestricted realm bearer token and WRITE on the document's permission path; a path-restricted delegated token is rejected. The request is submitted as a fenced job on the document's PID authority, so 202 means the mint was durably accepted there, not that the identifier already resolves; poll the returned job id for completion. Minting is idempotent per document: a repeated request returns created=false with the same identifier. Because the authority answers for the document, its verdicts are returned as they are, including 404 for an unknown document and 403 when the authority denies the write.",
    params(("document_id" = String, Path, description = "Document ULID to mint a PID for, for example 01JMETADATA0123456789ABCDE")),
    responses(
        (
            status = 202,
            description = "Mint job durably accepted on the document's PID authority; the identifier resolves once the job completes",
            body = Object,
            content_type = "application/json",
            example = json!({"pid": "https://w3id.org/aruna/01JMETADATA0123456789ABCDE", "job_id": "01JJOB0123456789ABCDEFGHJK", "created": true})
        ),
        (status = 400, description = "Malformed document id", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Caller lacks WRITE on the document", body = ErrorResponse),
        (status = 404, description = "Document not found", body = ErrorResponse),
        (status = 503, description = "PID authority unreachable, retry later", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn mint_pid(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer_token): Extension<Option<ValidatedArunaBearerTokenCarrier>>,
    Path(document_id): Path<String>,
) -> ServerResult<(StatusCode, Json<serde_json::Value>)> {
    let auth = require_unrestricted_realm_auth(&state, auth)?;
    let document_id = Ulid::from_string(&document_id).map_err(|_| ServerError::BadRequest)?;
    let ctx = state.get_ctx();
    let record = load_metadata_record_by_document(&ctx, document_id)
        .await
        .map_err(|error| ServerError::InternalError(format!("{error:?}")))?
        .ok_or(ServerError::NotFound)?;
    ensure_permission(
        &state,
        &auth,
        record.permission_path.clone(),
        Permission::WRITE,
    )
    .await?;

    let result = submit_mint_pid(
        &ctx,
        MintPersistentIdSpec {
            document_id,
            minted_by: auth.user_id,
        },
        state.get_node_id(),
        DEFAULT_JOB_RETENTION_MS,
        forwarded_auth_token(bearer_token)?,
    )
    .await
    .map_err(mint_submit_error)?;

    Ok((
        StatusCode::ACCEPTED,
        Json(serde_json::json!({
            "pid": MetadataRegistryRecord::graph_iri_for(document_id),
            "job_id": result.job_id.to_string(),
            "created": result.created,
        })),
    ))
}

/// Explicit admin withdrawal: flip a document's PID to a permanent 410 tombstone,
/// writing the tombstone even when nothing was ever minted so an accepted mint job
/// cannot land after it. Deletion withdraws automatically; this is the manual
/// path. Idempotent, and 204 only once the transition is durable on the authority.
#[utoipa::path(
    delete,
    path = "/pid/{document_id}",
    tag = "pid",
    summary = "Withdraw a document's w3id persistent identifier",
    description = "Requires an unrestricted realm bearer token and WRITE on the document's permission path. Withdrawal is terminal: the identifier answers 410 from then on and cannot be minted again, and the tombstone is written even when nothing was minted yet so an in-flight mint job cannot land after it. Deleting a document withdraws its identifier automatically; this is the manual path. The call is idempotent and answers 204 only once the transition is durable on the document's PID authority, otherwise 503.",
    params(("document_id" = String, Path, description = "Document ULID whose PID is withdrawn, for example 01JMETADATA0123456789ABCDE")),
    responses(
        (status = 204, description = "The withdrawal is durable on the PID authority; the response has no body"),
        (status = 400, description = "Malformed document id", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Caller lacks WRITE on the document", body = ErrorResponse),
        (status = 404, description = "Document not found", body = ErrorResponse),
        (status = 503, description = "PID authority unreachable, retry later", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn withdraw_pid(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer_token): Extension<Option<ValidatedArunaBearerTokenCarrier>>,
    Path(document_id): Path<String>,
) -> ServerResult<StatusCode> {
    let auth = require_unrestricted_realm_auth(&state, auth)?;
    let document_id = Ulid::from_string(&document_id).map_err(|_| ServerError::BadRequest)?;
    let ctx = state.get_ctx();
    let record = load_metadata_record_by_document(&ctx, document_id)
        .await
        .map_err(|error| ServerError::InternalError(format!("{error:?}")))?
        .ok_or(ServerError::NotFound)?;
    ensure_permission(
        &state,
        &auth,
        record.permission_path.clone(),
        Permission::WRITE,
    )
    .await?;
    let mapping = withdraw_pid_routed(
        &ctx,
        state.get_realm_id(),
        document_id,
        unix_timestamp_millis(),
        forwarded_auth_token(bearer_token)?,
    )
    .await
    .map_err(map_metadata_api_error)?;
    if mapping.status != PersistentIdStatus::Withdrawn {
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

    // The mint job is queued on the document's authority, so that node's answer
    // about the document is the client's answer, not a fault of this one.
    #[test]
    fn mint_maps_errors() {
        assert!(matches!(
            mint_submit_error(SubmitJobError::DocumentMissing),
            ServerError::NotFound
        ));
        assert!(matches!(
            mint_submit_error(SubmitJobError::AuthorityDenied),
            ServerError::Forbidden
        ));
        assert!(matches!(
            mint_submit_error(SubmitJobError::PlacementUnavailable("down".to_string())),
            ServerError::ServiceUnavailable
        ));
        assert!(matches!(
            mint_submit_error(SubmitJobError::InvalidWorkspace("bad".to_string())),
            ServerError::InternalError(_)
        ));
    }
}
