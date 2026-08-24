//! Device-local plane of a user node. It is served only by a User-kind node
//! and only to the owner that node is bound to, so the desktop app and the
//! headless CLI share one authenticated surface.

use std::str::FromStr;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::auth::{parse_group_id, require_unrestricted_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::metadata::{ProfileValidationPreviewRequest, ProfileValidationPreviewResponse};
use crate::server_state::ServerState;
use aruna_core::structs::{AuthContext, NodeCapabilities};
use aruna_operations::device::delete_draft::{DeleteDraftError, DeleteDraftOperation};
use aruna_operations::device::enqueue_draft::{
    EnqueueDraftError, EnqueueDraftInput, EnqueueDraftOperation,
};
use aruna_operations::device::inspect_draft::{InspectDraftError, InspectDraftOperation};
use aruna_operations::device::list_drafts::ListDraftsOperation;
use aruna_operations::device::repository::{IntakeEntry, IntakeState};
use aruna_operations::device::wipe::{
    WIPED_EXIT_CODE, WipeDeviceConfig, WipeDeviceError, WipeDeviceOperation,
};
use aruna_operations::driver::drive;
use aruna_operations::get_realm_config::{GetRealmConfigError, GetRealmConfigOperation};
use aruna_operations::metadata::profile_validation::preview_submission;

#[derive(OpenApi)]
#[openapi(tags((
    name = "device",
    description = "Owner-only controls a user node serves for the machine it runs on"
)))]
pub struct DeviceApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(DeviceApiDoc::openapi())
        .routes(routes!(queue_draft, list_drafts))
        .routes(routes!(preview_draft))
        .routes(routes!(get_draft, delete_draft))
        .routes(routes!(wipe_device))
}

/// One queued create as the desktop sees it.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct DeviceDraft {
    /// Local draft id. It is never the realm document id.
    pub draft_id: String,
    pub group_id: String,
    pub path: String,
    pub public: bool,
    pub created_at_ms: u64,
    /// One of `pending`, `publishing`, `published` or `failed`.
    pub status: String,
    /// Set once the realm document id is minted, and after publishing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub document_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attempts: Option<u32>,
    /// Why the last attempt did not publish, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    /// Whether a parked entry could still succeed on a later attempt.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retryable: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct DeviceDraftList {
    pub drafts: Vec<DeviceDraft>,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct QueueDraftRequest {
    /// Group the document is created in, as a ULID.
    pub group_id: String,
    /// Document path inside the group.
    pub path: String,
    #[serde(default)]
    pub public: bool,
    /// RO-Crate JSON-LD to publish unchanged.
    #[schema(value_type = Object)]
    pub rocrate: Value,
}

impl From<IntakeEntry> for DeviceDraft {
    fn from(entry: IntakeEntry) -> Self {
        let mut draft = Self {
            draft_id: entry.draft_id.to_string(),
            group_id: entry.group_id.to_string(),
            path: entry.document_path,
            public: entry.public,
            created_at_ms: entry.created_at_ms,
            status: String::new(),
            document_id: None,
            attempts: None,
            last_error: None,
            retryable: None,
        };
        match entry.state {
            IntakeState::Pending {
                attempts,
                last_error,
                ..
            } => {
                draft.status = "pending".to_string();
                draft.attempts = Some(attempts);
                draft.last_error = last_error;
            }
            IntakeState::Publishing {
                document_id,
                attempts,
                ..
            } => {
                draft.status = "publishing".to_string();
                draft.document_id = Some(document_id.to_string());
                draft.attempts = Some(attempts);
            }
            IntakeState::Published { document_id } => {
                draft.status = "published".to_string();
                draft.document_id = Some(document_id.to_string());
            }
            IntakeState::Failed { reason, retryable } => {
                draft.status = "failed".to_string();
                draft.last_error = Some(reason);
                draft.retryable = Some(retryable);
            }
        }
        draft
    }
}

/// The device's owner, read from the replicated realm configuration.
///
/// This is the device plane's authorization boundary: the surface exists only
/// on a User-kind node, and only for the user that node is bound to. The read
/// is node-local, so the plane keeps working while the realm is unreachable.
async fn require_owner(
    state: &ServerState,
    auth: Option<AuthContext>,
) -> ServerResult<AuthContext> {
    if !matches!(state.node_capabilities(), NodeCapabilities::User { .. }) {
        return Err(ServerError::NotFound);
    }
    let auth = require_unrestricted_realm_auth(state, auth)?;
    let config = drive(
        GetRealmConfigOperation::new(state.get_realm_id()),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| match error {
        // A device that enrolled but has not received the configuration yet
        // cannot resolve its owner; that resolves on its own.
        GetRealmConfigError::DocumentNotFound => ServerError::ServiceUnavailableReason(
            "the realm configuration has not reached this device yet".to_string(),
        ),
        other => ServerError::InternalError(other.to_string()),
    })?;
    let node_id = state.get_node_id().to_string();
    let owner = config
        .nodes
        .iter()
        .find(|node| node.node_id == node_id)
        .and_then(|node| node.kind.owner())
        .ok_or(ServerError::Forbidden)?;
    if owner != auth.user_id {
        return Err(ServerError::Forbidden);
    }
    Ok(auth)
}

fn parse_draft_id(draft_id: &str) -> ServerResult<Ulid> {
    Ulid::from_str(draft_id).map_err(|_| ServerError::BadRequest)
}

fn rocrate_jsonld(rocrate: &Value) -> ServerResult<String> {
    if !rocrate.is_object() {
        return Err(ServerError::BadRequestReason(
            "rocrate must be a JSON-LD object".to_string(),
        ));
    }
    serde_json::to_string(rocrate).map_err(|error| ServerError::InternalError(error.to_string()))
}

#[utoipa::path(
    post,
    path = "/device/drafts",
    tag = "device",
    summary = "Queue a document create on this device",
    description = r#"Queues one RO-Crate create so the device publishes it as soon as the realm is reachable.

**Authentication**: unrestricted realm bearer token belonging to the user this device is enrolled for. Any other caller is refused, and a node that is not a user node answers 404.

**Behavior**
- Only creates of new documents queue. Updates and deletes of documents that already exist in the realm are never queued: they need connectivity and current authorization, and the ordinary metadata routes refuse them while the realm is unreachable.
- The entry is stored locally and stays visible to its owner through this route until it publishes or is deleted.
- A background drain forwards each entry in queue order within seconds of the realm becoming reachable, mints the realm document id before the first forward and retries with the same id, so a crash between forward and outcome cannot create a second document.
- The realm document id is unknown while the entry is pending; the local draft id is the stable reference for the desktop.

**Limits**
- The device holds at most 256 queued drafts.
- `rocrate` must be a JSON-LD object and `path` must not be blank.

**Errors**
- 409 when the local queue is full."#,
    request_body(
        content = QueueDraftRequest,
        description = "Group, path, visibility and the RO-Crate payload to publish",
        example = json!({
            "group_id": "01JGROUP0123456789ABCDEFGH",
            "path": "/field-notes/2026-08",
            "public": false,
            "rocrate": {"@context": "https://w3id.org/ro/crate/1.1/context", "@graph": []}
        })
    ),
    responses(
        (status = 201, description = "The draft is durably queued on this device", body = DeviceDraft,
            example = json!({
                "draft_id": "01JDRAFT0123456789ABCDEFGH",
                "group_id": "01JGROUP0123456789ABCDEFGH",
                "path": "/field-notes/2026-08",
                "public": false,
                "created_at_ms": 1756000000000u64,
                "status": "pending",
                "attempts": 0
            })),
        (status = 400, description = "Malformed group id, blank path or a non-object payload", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller is not the user this device is enrolled for", body = ErrorResponse),
        (status = 404, description = "This node is not a user node and serves no device plane", body = ErrorResponse),
        (status = 409, description = "The device already holds the maximum number of queued drafts", body = ErrorResponse),
        (status = 503, description = "The realm configuration has not reached this device yet", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn queue_draft(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<QueueDraftRequest>,
) -> ServerResult<(StatusCode, Json<DeviceDraft>)> {
    let auth = require_owner(&state, auth).await?;
    let group_id = parse_group_id(&request.group_id)?;
    let jsonld = rocrate_jsonld(&request.rocrate)?;
    let entry = IntakeEntry::new(
        Ulid::generate(),
        auth.user_id,
        group_id,
        request.path,
        request.public,
        jsonld,
    );
    let queued = drive(
        EnqueueDraftOperation::new(EnqueueDraftInput { entry }),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| match error {
        EnqueueDraftError::QueueFull { limit } => {
            ServerError::Conflict(format!("the authoring queue already holds {limit} drafts"))
        }
        EnqueueDraftError::MissingPath | EnqueueDraftError::MissingPayload => {
            ServerError::BadRequestReason(error.to_string())
        }
        other => ServerError::InternalError(other.to_string()),
    })?;
    Ok((StatusCode::CREATED, Json(queued.into())))
}

#[utoipa::path(
    get,
    path = "/device/drafts",
    tag = "device",
    summary = "List the drafts queued on this device",
    description = r#"Lists every authoring intent this device holds, oldest first.

**Authentication**: unrestricted realm bearer token belonging to the user this device is enrolled for.

**Behavior**
- Entries stay listed after they publish or park, so the owner can see what happened before removing them.
- The list is node-local and answers while the realm is unreachable."#,
    responses(
        (status = 200, description = "The device's authoring queue in creation order", body = DeviceDraftList,
            example = json!({
                "drafts": [{
                    "draft_id": "01JDRAFT0123456789ABCDEFGH",
                    "group_id": "01JGROUP0123456789ABCDEFGH",
                    "path": "/field-notes/2026-08",
                    "public": false,
                    "created_at_ms": 1756000000000u64,
                    "status": "published",
                    "document_id": "01JDOCUMENT0123456789ABCDE"
                }]
            })),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller is not the user this device is enrolled for", body = ErrorResponse),
        (status = 404, description = "This node is not a user node and serves no device plane", body = ErrorResponse),
        (status = 503, description = "The realm configuration has not reached this device yet", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn list_drafts(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<Json<DeviceDraftList>> {
    require_owner(&state, auth).await?;
    let entries = drive(ListDraftsOperation::new(), &state.get_ctx())
        .await
        .map_err(|error| ServerError::InternalError(error.to_string()))?;
    Ok(Json(DeviceDraftList {
        drafts: entries.into_iter().map(DeviceDraft::from).collect(),
    }))
}

#[utoipa::path(
    get,
    path = "/device/drafts/{draft_id}",
    tag = "device",
    summary = "Read one queued draft",
    description = r#"Reads the current state of one queued authoring intent.

**Authentication**: unrestricted realm bearer token belonging to the user this device is enrolled for.

**Behavior**
- A published entry carries the realm document id it created."#,
    params(("draft_id" = String, Path, description = "Local draft ULID, for example 01JDRAFT0123456789ABCDEFGH")),
    responses(
        (status = 200, description = "The queued draft and its current state", body = DeviceDraft,
            example = json!({
                "draft_id": "01JDRAFT0123456789ABCDEFGH",
                "group_id": "01JGROUP0123456789ABCDEFGH",
                "path": "/field-notes/2026-08",
                "public": false,
                "created_at_ms": 1756000000000u64,
                "status": "failed",
                "last_error": "forward to holder may have applied the write",
                "retryable": true
            })),
        (status = 400, description = "The draft id is not a ULID", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller is not the user this device is enrolled for", body = ErrorResponse),
        (status = 404, description = "No such draft, or this node is not a user node", body = ErrorResponse),
        (status = 503, description = "The realm configuration has not reached this device yet", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn get_draft(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(draft_id): Path<String>,
) -> ServerResult<Json<DeviceDraft>> {
    require_owner(&state, auth).await?;
    let draft_id = parse_draft_id(&draft_id)?;
    let entry = drive(InspectDraftOperation::new(draft_id), &state.get_ctx())
        .await
        .map_err(|error| match error {
            InspectDraftError::NotFound => ServerError::NotFound,
            other => ServerError::InternalError(other.to_string()),
        })?;
    Ok(Json(entry.into()))
}

#[utoipa::path(
    delete,
    path = "/device/drafts/{draft_id}",
    tag = "device",
    summary = "Remove a queued draft",
    description = r#"Removes one authoring intent from the device's queue.

**Authentication**: unrestricted realm bearer token belonging to the user this device is enrolled for.

**Behavior**
- Removing a published entry drops the local record only; the realm document it created stays.
- A draft whose forward is in flight is refused until its outcome is recorded, because the create may already have applied.

**Errors**
- 409 while a forward for the draft is in flight."#,
    params(("draft_id" = String, Path, description = "Local draft ULID, for example 01JDRAFT0123456789ABCDEFGH")),
    responses(
        (status = 204, description = "The draft is gone from the device queue"),
        (status = 400, description = "The draft id is not a ULID", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller is not the user this device is enrolled for", body = ErrorResponse),
        (status = 404, description = "No such draft, or this node is not a user node", body = ErrorResponse),
        (status = 409, description = "A forward for this draft is in flight", body = ErrorResponse),
        (status = 503, description = "The realm configuration has not reached this device yet", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn delete_draft(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(draft_id): Path<String>,
) -> ServerResult<StatusCode> {
    require_owner(&state, auth).await?;
    let draft_id = parse_draft_id(&draft_id)?;
    drive(DeleteDraftOperation::new(draft_id), &state.get_ctx())
        .await
        .map_err(|error| match error {
            DeleteDraftError::NotFound => ServerError::NotFound,
            DeleteDraftError::PublishInFlight => ServerError::Conflict(error.to_string()),
            other => ServerError::InternalError(other.to_string()),
        })?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    post,
    path = "/device/drafts/preview",
    tag = "device",
    summary = "Validate a draft without storing it",
    description = r#"Runs the create-time RO-Crate and Profile checks against a draft the device has not stored.

**Authentication**: unrestricted realm bearer token belonging to the user this device is enrolled for.

**Behavior**
- The same evaluation a create enforces, so the owner sees the verdict before queueing.
- Structural checks need nothing but the payload. A draft that names a registered Profile is checked against the copy this device already holds, so an unknown Profile reports as unevaluated rather than failing."#,
    request_body(
        content = ProfileValidationPreviewRequest,
        description = "The RO-Crate JSON-LD to evaluate",
        example = json!({"rocrate": {"@context": "https://w3id.org/ro/crate/1.1/context", "@graph": []}})
    ),
    responses(
        (status = 200, description = "The verdict a create would enforce for this draft", body = ProfileValidationPreviewResponse,
            example = json!({
                "accepted": true,
                "state": "valid",
                "evaluator": "craqle-shacl",
                "findings": [],
                "completeness": "complete",
                "structural_violations": []
            })),
        (status = 400, description = "The payload is not a JSON-LD object", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller is not the user this device is enrolled for", body = ErrorResponse),
        (status = 404, description = "This node is not a user node and serves no device plane", body = ErrorResponse),
        (status = 503, description = "The realm configuration has not reached this device yet", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn preview_draft(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<ProfileValidationPreviewRequest>,
) -> ServerResult<(StatusCode, Json<ProfileValidationPreviewResponse>)> {
    require_owner(&state, auth).await?;
    let jsonld = rocrate_jsonld(&request.rocrate)?;
    let preview = preview_submission(&state.get_ctx(), &jsonld)
        .await
        .map_err(|error| ServerError::InternalError(error.to_string()))?;
    Ok((StatusCode::OK, Json(preview.into())))
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct WipeDeviceRequest {
    /// This node's own id, typed back as confirmation.
    pub confirm_node_id: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct WipeDeviceResponse {
    pub node_id: String,
    /// Process exit status the supervisor sees once the wipe completes.
    pub exit_code: i32,
}

#[utoipa::path(
    post,
    path = "/device/wipe",
    tag = "device",
    summary = "Erase this device and stop the node",
    description = r#"Erases everything this device stores locally and stops the node.

**Authentication**: unrestricted realm bearer token belonging to the user this device is enrolled for. A node that is not a user node answers 404.

**Behavior**
- Realm-side eviction is a separate, earlier step. The desktop calls `DELETE /users/me/devices/{id}` on a management node first, so the realm drops the membership; this route only erases what the device holds.
- The node accepts the wipe, answers, then runs its ordinary shutdown, erases the contents of its storage roots including the persisted identity, and exits with status 79 so a supervisor can tell an erased device from a crash or an ordinary stop.
- The storage roots themselves are kept, so a mounted volume stays mounted.
- Everything local is lost: queued drafts, blobs, credentials and the node identity. Re-enrolling mints a new node id.

**Limits**
- `confirm_node_id` must equal this node's own id.

**Errors**
- 400 when the confirmation names a different node."#,
    request_body(
        content = WipeDeviceRequest,
        description = "This node's own id, typed back as confirmation",
        example = json!({"confirm_node_id": "k5r2gmr7qeqfhqxhbpcpqoa2xhpqcrmr2vpxjqx3nvxfvbxvvrga"})
    ),
    responses(
        (status = 202, description = "The wipe is accepted; the node shuts down, erases its roots and exits", body = WipeDeviceResponse,
            example = json!({
                "node_id": "k5r2gmr7qeqfhqxhbpcpqoa2xhpqcrmr2vpxjqx3nvxfvbxvvrga",
                "exit_code": 79
            })),
        (status = 400, description = "The confirmation does not name this node", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller is not the user this device is enrolled for", body = ErrorResponse),
        (status = 404, description = "This node is not a user node and serves no device plane", body = ErrorResponse),
        (status = 503, description = "The realm configuration has not reached this device yet", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn wipe_device(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<WipeDeviceRequest>,
) -> ServerResult<(StatusCode, Json<WipeDeviceResponse>)> {
    require_owner(&state, auth).await?;
    let wipe = state.device_wipe().ok_or(ServerError::NotFound)?.clone();
    let node_id = drive(
        WipeDeviceOperation::new(WipeDeviceConfig {
            node_id: state.get_node_id(),
            confirm_node_id: request.confirm_node_id,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| match error {
        WipeDeviceError::ConfirmationMismatch => ServerError::BadRequestReason(error.to_string()),
        other => ServerError::InternalError(other.to_string()),
    })?;
    wipe.arm();
    Ok((
        StatusCode::ACCEPTED,
        Json(WipeDeviceResponse {
            node_id: node_id.to_string(),
            exit_code: WIPED_EXIT_CODE,
        }),
    ))
}

#[cfg(test)]
mod tests {
    use super::DeviceDraft;
    use aruna_core::structs::RealmId;
    use aruna_core::types::UserId;
    use aruna_operations::device::repository::{IntakeEntry, IntakeState};
    use ulid::Ulid;

    fn entry() -> IntakeEntry {
        IntakeEntry::new(
            Ulid::generate(),
            UserId::local(Ulid::generate(), RealmId::from_bytes([1u8; 32])),
            Ulid::generate(),
            "/notes".to_string(),
            true,
            "{}".to_string(),
        )
    }

    #[test]
    fn maps_draft_states() {
        // The desktop reads the lifecycle from `status`, never from a flag mix.
        let document_id = Ulid::generate();
        let mut source = entry();
        let pending: DeviceDraft = source.clone().into();
        assert_eq!(pending.status, "pending");
        assert!(pending.document_id.is_none());

        source.state = IntakeState::Publishing {
            document_id,
            due_at_ms: 0,
            attempts: 2,
        };
        let publishing: DeviceDraft = source.clone().into();
        assert_eq!(publishing.status, "publishing");
        assert_eq!(
            publishing.document_id.as_deref(),
            Some(document_id.to_string().as_str())
        );

        source.state = IntakeState::Failed {
            reason: "denied".to_string(),
            retryable: false,
        };
        let failed: DeviceDraft = source.into();
        assert_eq!(failed.status, "failed");
        assert_eq!(failed.retryable, Some(false));
    }

    #[test]
    fn openapi_lists_device_routes() {
        let openapi = serde_json::to_value(crate::openapi::ApiDoc::openapi()).unwrap();
        assert!(openapi["paths"]["/device/drafts"]["post"].is_object());
        assert!(openapi["paths"]["/device/drafts"]["get"].is_object());
        assert!(openapi["paths"]["/device/drafts/preview"]["post"].is_object());
        assert!(openapi["paths"]["/device/drafts/{draft_id}"]["delete"].is_object());
    }
}
