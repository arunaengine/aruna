//! w3id persistent-identifier landing resolution.
//!
//! A PID is the document graph IRI `https://w3id.org/aruna/{document_id}`.
//! Resolution consults the mapping: Active resolves to the RO-Crate read (which
//! enforces its own visibility), Withdrawn is a permanent 410, and an unminted id
//! 404s here without affecting the normal metadata read.
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
use aruna_operations::persistent_id::{read_mapping, withdraw_persistent_id};

use crate::auth::{ensure_permission, require_unrestricted_realm_auth};
use crate::error::{ServerError, ServerResult};
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

    let mapping = match read_mapping(&ctx, document_id).await {
        Ok(Some(mapping)) => mapping,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    match mapping.status {
        PersistentIdStatus::Withdrawn => gone(&mapping.pid),
        PersistentIdStatus::Active => {
            match load_metadata_record_by_document(&ctx, document_id).await {
                Ok(Some(_)) => redirect(&rocrate_location(document_id)),
                // Safety net: a minted PID whose target is gone is a permanent 410,
                // never a 404, even if the delete-time withdrawal did not persist.
                Ok(None) => gone(&mapping.pid),
                Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            }
        }
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

/// Explicit admin withdrawal: flip a minted PID to a permanent 410 tombstone.
/// Deletion and harvest-tombstoning withdraw automatically; this is the manual
/// path. Idempotent.
async fn withdraw_pid(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
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
    withdraw_persistent_id(&ctx, document_id, unix_timestamp_millis())
        .await
        .map_err(|error| ServerError::InternalError(format!("{error:?}")))?;
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
}
