//! The offline authoring queue this device holds for its owner.

use std::str::FromStr;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use ulid::Ulid;
use utoipa::ToSchema;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::auth::parse_group_id;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::metadata::{ProfileValidationPreviewRequest, ProfileValidationPreviewResponse};
use crate::server_state::ServerState;
use aruna_core::structs::AuthContext;
use aruna_operations::device::delete_draft::{DeleteDraftError, DeleteDraftOperation};
use aruna_operations::device::enqueue_draft::{
    EnqueueDraftError, EnqueueDraftInput, EnqueueDraftOperation,
};
use aruna_operations::device::inspect_draft::{InspectDraftError, InspectDraftOperation};
use aruna_operations::device::list_drafts::ListDraftsOperation;
use aruna_operations::device::repository::{IntakeEntry, IntakeState};
use aruna_operations::driver::drive;
use aruna_operations::metadata::profile_validation::preview_submission;

use super::require_owner;

pub(super) fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new()
        .routes(routes!(queue_draft, list_drafts))
        .routes(routes!(preview_draft))
        .routes(routes!(get_draft, delete_draft))
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
            IntakeState::Failed {
                reason,
                retryable,
                document_id,
            } => {
                draft.status = "failed".to_string();
                draft.document_id = document_id.map(|id| id.to_string());
                draft.last_error = Some(reason);
                draft.retryable = Some(retryable);
            }
        }
        draft
    }
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

**Authentication**: unrestricted realm bearer token belonging to the user this device is enrolled
for.

**Behavior**
- Only creates of new documents queue. Updates and deletes need connectivity and current
  authorization, so the ordinary metadata routes refuse them while the realm is unreachable.
- A background drain forwards each entry in queue order within seconds of the realm becoming
  reachable. It mints the realm document id before the first forward and retries with the same id,
  so a crash between forward and outcome cannot create a second document.
- The realm document id is unknown while the entry is pending; the local draft id is the stable
  reference for the desktop.

**Limits**
- The device holds at most 256 queued drafts.
- `rocrate` must be a JSON-LD object and `path` must not be blank."#,
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

**Authentication**: unrestricted realm bearer token belonging to the user this device is enrolled
for.

**Behavior**
- Entries stay listed after they publish or park, so the owner can see what happened before removing
  them.
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

**Authentication**: unrestricted realm bearer token belonging to the user this device is enrolled
for.

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

**Authentication**: unrestricted realm bearer token belonging to the user this device is enrolled
for.

**Behavior**
- Removing a published entry drops the local record only; the realm document it created stays.
- A draft whose forward is in flight is refused until its outcome is recorded, because the create
  may already have applied."#,
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

**Authentication**: unrestricted realm bearer token belonging to the user this device is enrolled
for.

**Behavior**
- The same evaluation a create enforces, so the owner sees the verdict before queueing.
- Structural checks need nothing but the payload. A draft that names a registered Profile is checked
  against the copy this device already holds, so an unknown Profile reports as unevaluated rather
  than failing.
- `group_id` names the group the draft would be saved in, and a Profile of that group is resolved
  under the device owner's own authority. Without it only public Profiles resolve."#,
    request_body(
        content = ProfileValidationPreviewRequest,
        description = "The RO-Crate JSON-LD to evaluate",
        example = json!({"group_id": "01JGROUP00000000000000000", "rocrate": {"@context": "https://w3id.org/ro/crate/1.1/context", "@graph": []}})
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
    let group_id = request.group_id.as_deref().map(parse_group_id).transpose()?;
    let jsonld = rocrate_jsonld(&request.rocrate)?;
    let preview = preview_submission(&state.get_ctx(), group_id, &jsonld)
        .await
        .map_err(|error| ServerError::InternalError(error.to_string()))?;
    Ok((StatusCode::OK, Json(preview.into())))
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
            document_id: None,
        };
        let failed: DeviceDraft = source.into();
        assert_eq!(failed.status, "failed");
        assert_eq!(failed.retryable, Some(false));
    }
}
