//! w3id persistent-identifier landing resolution.
//!
//! A PID is the document graph IRI `https://w3id.org/aruna/{document_id}`. Every
//! operation routes to a holder of the document's metadata placement, which is the
//! mapping's authority: resolution redirects only while the document is
//! anonymously readable, a withdrawn mapping is a permanent 410 whatever the
//! document's visibility, and everything else is indistinguishable from unminted.
//! A node that cannot reach the authority reports 503, never a local 404.
//!
//! DEPLOYMENT: the external `w3id.org/aruna/{id}` redirect (currently pointed at
//! the v2 API) should target `/api/v1/pid/{id}`; that is a deployment change, not
//! done here.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Extension, Json, Router};
use ulid::Ulid;

use aruna_core::structs::{
    AuthContext, DEFAULT_JOB_RETENTION_MS, MetadataRegistryRecord, MintPersistentIdSpec,
    Permission, PersistentIdStatus,
};
use aruna_core::util::unix_timestamp_millis;
use aruna_operations::get_metadata_document::load_metadata_record_by_document;
use aruna_operations::jobs::service::submit_mint_pid;
use aruna_operations::metadata::PersistentIdResolution;
use aruna_operations::metadata::api::MetadataApiError;
use aruna_operations::metadata::forward::{resolve_pid_routed, withdraw_pid_routed};

use crate::auth::{
    ValidatedArunaBearerTokenCarrier, ensure_permission, require_unrestricted_realm_auth,
};
use crate::error::{ServerError, ServerResult};
use crate::routes::metadata::{forwarded_auth_token, map_metadata_api_error};
use crate::server_state::ServerState;

pub fn router() -> Router<Arc<ServerState>> {
    Router::new().route(
        "/pid/{document_id}",
        get(resolve_pid).post(mint_pid).delete(withdraw_pid),
    )
}

fn rocrate_location(document_id: Ulid) -> String {
    format!("/api/v1/metadata/{document_id}/rocrate")
}

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
async fn mint_pid(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
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
    )
    .await
    .map_err(|error| ServerError::InternalError(error.to_string()))?;

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

    #[test]
    fn rocrate_location_targets_the_read_route() {
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
}
