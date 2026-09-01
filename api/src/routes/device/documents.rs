//! The metadata documents this device keeps available offline.

use std::str::FromStr;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use utoipa::ToSchema;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server_state::ServerState;
use aruna_core::structs::AuthContext;
use aruna_operations::device::replica::{ReplicaOrigin, ReplicaRecord, list_replicas};
use aruna_operations::device::selection::{SelectionError, deselect_document, select_document};

use super::require_owner;

pub(super) fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new()
        .routes(routes!(list_documents))
        .routes(routes!(set_selection))
}

/// One document this device holds a replica of.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DeviceDocument {
    pub document_id: String,
    pub path: String,
    pub group_id: String,
    /// Whether the owner keeps this document available offline.
    pub selected: bool,
    /// `device` for a document created here, `realm` for one fetched from a
    /// holder.
    pub origin: String,
    /// Actors in the local graph's clock, and in the realm's at the last
    /// refresh. A local clock ahead of the realm one means unpublished edits.
    pub local_clock_size: usize,
    pub realm_clock_size: usize,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DeviceDocumentList {
    pub documents: Vec<DeviceDocument>,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SelectDocumentRequest {
    pub selected: bool,
}

impl From<ReplicaRecord> for DeviceDocument {
    fn from(replica: ReplicaRecord) -> Self {
        Self {
            document_id: replica.document_id.to_string(),
            path: replica.document_path,
            group_id: replica.group_id.to_string(),
            selected: replica.selected,
            origin: match replica.origin {
                ReplicaOrigin::Device => "device".to_string(),
                ReplicaOrigin::Realm => "realm".to_string(),
            },
            local_clock_size: replica.local_clock.0.len(),
            realm_clock_size: replica.realm_clock.0.len(),
        }
    }
}

fn map_selection_error(error: SelectionError) -> ServerError {
    match error {
        SelectionError::NotFound => ServerError::NotFound,
        SelectionError::PendingEdits => ServerError::Conflict(error.to_string()),
        SelectionError::TooManyReplicas { .. } => ServerError::Conflict(error.to_string()),
        SelectionError::Refused(reason) => ServerError::BadRequestReason(reason),
        SelectionError::Unavailable => ServerError::ServiceUnavailableReason(
            "the realm has not served this document to this device yet".to_string(),
        ),
    }
}

#[utoipa::path(
    get,
    path = "/device/documents",
    tag = "device",
    summary = "List the documents this device keeps offline",
    description = r#"Lists every metadata document this device holds a local replica of.

**Authentication**: unrestricted realm bearer token belonging to the user this device is enrolled
for.

**Behavior**
- A document is here because it was created on this device, selected for offline use, or edited
  here.
- Reads and edits of a listed document are answered by this device alone, so they keep working while
  the realm is unreachable."#,
    responses(
        (status = 200, description = "Every replica this device holds, selected or not", body = DeviceDocumentList,
            example = json!({
                "documents": [{
                    "documentId": "01JDOCUMENT0123456789ABCDE",
                    "path": "field-notes/2026-08",
                    "groupId": "01JGROUP0123456789ABCDEFGH",
                    "selected": true,
                    "origin": "device",
                    "localClockSize": 2,
                    "realmClockSize": 1
                }]
            })),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller is not the user this device is enrolled for", body = ErrorResponse),
        (status = 404, description = "This node is not a user node and serves no device plane", body = ErrorResponse),
        (status = 503, description = "The realm configuration has not reached this device yet", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn list_documents(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<Json<DeviceDocumentList>> {
    require_owner(&state, auth).await?;
    let replicas = list_replicas(&state.get_ctx())
        .await
        .ok_or(ServerError::ServiceUnavailable)?;
    Ok(Json(DeviceDocumentList {
        documents: replicas.into_iter().map(DeviceDocument::from).collect(),
    }))
}

#[utoipa::path(
    put,
    path = "/device/documents/{document_id}/selection",
    tag = "device",
    summary = "Keep a document offline, or stop keeping it",
    description = r#"Selects one metadata document for offline use on this device, or deselects it.

**Authentication**: unrestricted realm bearer token belonging to the user this device is enrolled
for.

**Behavior**
- Selecting fetches the document's graph from a holder once, so a first selection needs the realm.
  Selecting again only refreshes what is already here.
- A selected document is read and edited on this device alone; edits queue and publish as soon as
  the realm answers.
- Deselecting drops the ledger entry. The local graph stays, so selecting the same document again
  joins onto it rather than fetching it from nothing.
- Documents created or edited on this device select themselves."#,
    request_body(
        content = SelectDocumentRequest,
        description = "Whether the document is kept offline on this device",
        example = json!({ "selected": true })
    ),
    params(("document_id" = String, Path, description = "Metadata document ULID, for example 01JDOCUMENT0123456789ABCDE")),
    responses(
        (status = 200, description = "The document's selection on this device", body = DeviceDocument,
            example = json!({
                "documentId": "01JDOCUMENT0123456789ABCDE",
                "path": "field-notes/2026-08",
                "groupId": "01JGROUP0123456789ABCDEFGH",
                "selected": true,
                "origin": "realm",
                "localClockSize": 1,
                "realmClockSize": 1
            })),
        (status = 400, description = "The document id is not a ULID, or a holder refused the document", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller is not the user this device is enrolled for", body = ErrorResponse),
        (status = 404, description = "No such document, or this node is not a user node", body = ErrorResponse),
        (status = 409, description = "The document has unpublished edits, or the offline limit is reached", body = ErrorResponse),
        (status = 503, description = "The realm has not served this document to this device yet", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn set_selection(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(document_id): Path<String>,
    Json(request): Json<SelectDocumentRequest>,
) -> ServerResult<Json<DeviceDocument>> {
    require_owner(&state, auth).await?;
    let document_id = Ulid::from_str(&document_id).map_err(|_| ServerError::BadRequest)?;
    let context = state.get_ctx();
    let replica = match request.selected {
        true => select_document(&context, document_id).await,
        false => deselect_document(&context, document_id).await,
    }
    .map_err(map_selection_error)?;
    Ok(Json(replica.into()))
}

#[cfg(test)]
mod tests {
    use super::DeviceDocument;
    use aruna_operations::device::replica::{ReplicaOrigin, ReplicaRecord};
    use ulid::Ulid;

    #[test]
    fn maps_replica_row() {
        // The desktop reads the origin as a word, never as a flag mix.
        let replica = ReplicaRecord::new(
            Ulid::from_bytes([2u8; 16]),
            Ulid::from_bytes([1u8; 16]),
            "notes".to_string(),
            ReplicaOrigin::Device,
        );
        let row: DeviceDocument = replica.into();
        assert_eq!(row.origin, "device");
        assert!(row.selected);
        assert_eq!(row.local_clock_size, 0);
    }
}
