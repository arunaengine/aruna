//! What this device still owes the realm, and the control that makes it run.

use std::sync::Arc;

use axum::extract::State;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::error::{ErrorResponse, ServerResult};
use crate::server_state::ServerState;
use aruna_core::structs::AuthContext;
use aruna_operations::device::status::{
    DatasetRow, DocumentRow, SyncStatus, start_sync_run, sync_status,
};

use super::require_owner;

pub(super) fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new()
        .routes(routes!(get_sync_status))
        .routes(routes!(run_sync))
}

/// One selected document as the Sync view shows it.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DeviceSyncDocument {
    /// The realm document id, or the local draft id while a create has not
    /// been accepted and no realm id exists yet.
    pub document_id: String,
    pub path: String,
    pub group_id: String,
    /// One of `local_only`, `pending`, `publishing`, `invalid`, `failed` or
    /// `synced`.
    pub state: String,
    /// Edits this device has applied locally and not published yet.
    pub pending_edits: u32,
    /// Whether the document exists only on this device so far.
    pub local_only: bool,
    /// Profile findings against the merged state while it is invalid.
    pub validation_findings: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_synced_ms: Option<u64>,
}

/// One synced folder as the Sync view shows it.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DeviceSyncDataset {
    pub folder_id: String,
    /// The folder's root on this machine.
    pub label: String,
    /// One of `active`, `paused`, `deleting` or `error`.
    pub state: String,
    pub pending_uploads: usize,
    pub unsynced_files: usize,
    pub conflicts: usize,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DeviceSyncStatus {
    /// Whether the realm answered this device within the last poll window.
    pub realm_reachable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_sync_ms: Option<u64>,
    /// Documents still queued plus the files the folders have not exchanged.
    pub pending_total: usize,
    pub documents: Vec<DeviceSyncDocument>,
    pub datasets: Vec<DeviceSyncDataset>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DeviceSyncRun {
    pub started: bool,
}

impl From<DocumentRow> for DeviceSyncDocument {
    fn from(row: DocumentRow) -> Self {
        Self {
            document_id: row.document_id.to_string(),
            path: row.document_path,
            group_id: row.group_id.to_string(),
            state: row.state.as_str().to_string(),
            pending_edits: row.pending_edits,
            local_only: row.local_only,
            validation_findings: row.validation_findings,
            last_error: row.last_error,
            last_synced_ms: row.last_synced_ms,
        }
    }
}

impl From<DatasetRow> for DeviceSyncDataset {
    fn from(row: DatasetRow) -> Self {
        Self {
            folder_id: row.folder_id.to_string(),
            label: row.label,
            state: row.state,
            pending_uploads: row.pending_uploads,
            unsynced_files: row.unsynced_files,
            conflicts: row.conflicts,
        }
    }
}

impl From<SyncStatus> for DeviceSyncStatus {
    fn from(status: SyncStatus) -> Self {
        Self {
            realm_reachable: status.realm_reachable,
            last_sync_ms: status.last_sync_ms,
            pending_total: status.pending_total,
            documents: status.documents.into_iter().map(Into::into).collect(),
            datasets: status.datasets.into_iter().map(Into::into).collect(),
        }
    }
}

#[utoipa::path(
    get,
    path = "/device/sync/status",
    tag = "device",
    summary = "What this device still owes the realm",
    description = r#"Reports every selected document and synced folder with what is still unfinished.

**Authentication**: unrestricted realm bearer token belonging to the user this device is enrolled for. Any other caller is refused, and a node that is not a user node answers 404.

**Behavior**
- Everything is derived from state this device already holds, so the view answers while the realm is unreachable.
- `realmReachable` says whether the realm answered within the last poll window; when it is false the rest describes what will be exchanged once it does.
- A document's `state` is `local_only` while a create has not published, `pending` or `publishing` while an intent is queued, `failed` when the owner has to act, `invalid` when the merged document fails profile validation, and `synced` otherwise.
- A create the realm has not accepted yet is listed under its local draft id, because no realm document id exists for it.
- `pendingEdits` counts edits applied to the local replica that no holder has confirmed yet.
- A dataset's counters come from the folder's own rows: uploads a realm node has not pulled, files with no acknowledged version, and entries waiting for the owner to resolve."#,
    responses(
        (status = 200, description = "What this device still owes the realm", body = DeviceSyncStatus,
            example = json!({
                "realmReachable": true,
                "lastSyncMs": 1756000000000u64,
                "pendingTotal": 2,
                "documents": [{
                    "documentId": "01JDOCUMENT0123456789ABCDE",
                    "path": "field-notes/2026-08",
                    "groupId": "01JGROUP0123456789ABCDEFGH",
                    "state": "pending",
                    "pendingEdits": 1,
                    "localOnly": false,
                    "validationFindings": 0,
                    "lastSyncedMs": 1755999000000u64
                }],
                "datasets": [{
                    "folderId": "01JFOLDER0123456789ABCDEFG",
                    "label": "/home/ada/lab",
                    "state": "active",
                    "pendingUploads": 1,
                    "unsyncedFiles": 0,
                    "conflicts": 0
                }]
            })),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller is not the user this device is enrolled for", body = ErrorResponse),
        (status = 404, description = "This node is not a user node and serves no device plane", body = ErrorResponse),
        (status = 503, description = "The realm configuration has not reached this device yet", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn get_sync_status(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<Json<DeviceSyncStatus>> {
    require_owner(&state, auth).await?;
    Ok(Json(sync_status(&state.get_ctx()).await.into()))
}

#[utoipa::path(
    post,
    path = "/device/sync/run",
    tag = "device",
    summary = "Exchange everything with the realm now",
    description = r#"Asks this device to publish what it has queued and refresh what it keeps offline.

**Authentication**: unrestricted realm bearer token belonging to the user this device is enrolled for.

**Behavior**
- Schedules the authoring drain, the upload outbox drain and a refresh of every selected replica; the work then runs on the device's own timers.
- Idempotent while a run is in flight: the answer is the same and no second run starts.
- The route returns as soon as the work is scheduled, never when it has finished. `GET /device/sync/status` reports the progress."#,
    responses(
        (status = 202, description = "The device is exchanging with its realm", body = DeviceSyncRun,
            example = json!({"started": true})),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller is not the user this device is enrolled for", body = ErrorResponse),
        (status = 404, description = "This node is not a user node and serves no device plane", body = ErrorResponse),
        (status = 503, description = "The realm configuration has not reached this device yet", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn run_sync(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<(axum::http::StatusCode, Json<DeviceSyncRun>)> {
    require_owner(&state, auth).await?;
    start_sync_run(&state.get_ctx()).await;
    Ok((
        axum::http::StatusCode::ACCEPTED,
        Json(DeviceSyncRun { started: true }),
    ))
}
