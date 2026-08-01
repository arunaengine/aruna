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

use axum::Json;
use axum::Router;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use ulid::Ulid;

use aruna_core::structs::PersistentIdStatus;
use aruna_operations::get_metadata_document::load_metadata_record_by_document;
use aruna_operations::persistent_id::read_mapping;

use crate::server_state::ServerState;

pub fn router() -> Router<Arc<ServerState>> {
    Router::new().route("/pid/{document_id}", get(resolve_pid))
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
