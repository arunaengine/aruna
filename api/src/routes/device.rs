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

use crate::auth::{ensure_permission, parse_group_id, require_unrestricted_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::metadata::{ProfileValidationPreviewRequest, ProfileValidationPreviewResponse};
use crate::server_state::ServerState;
use aruna_core::errors::StagingSourceError;
use aruna_core::structs::{
    AuthContext, NodeCapabilities, OfferedDirectory, Permission, blob_bucket_permission_path,
};
use aruna_core::types::GroupId;
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
use aruna_operations::staging::offered_directory::{
    OfferDirectoryInput, OfferedDirectoryError, WithdrawOfferInput, list_offers as read_offers,
    offer_directory as register_offer, withdraw_offer as remove_offer,
};
use std::time::UNIX_EPOCH;

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
        .routes(routes!(offer_directory, list_offers))
        .routes(routes!(withdraw_offer))
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
pub(crate) async fn require_owner(
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

/// One directory this device offers as a read-only bucket.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct DeviceOffer {
    /// Bucket the directory is served as on this device's S3 endpoint.
    pub bucket: String,
    pub group_id: String,
    /// Absolute path on this machine, exactly as the owner gave it.
    pub root_path: String,
    pub created_at_ms: u64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct DeviceOfferList {
    pub offers: Vec<DeviceOffer>,
}

/// What one offer registered.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct OfferedInventory {
    pub bucket: String,
    /// Files the directory currently holds.
    pub files: usize,
    /// Objects tombstoned because their file is gone from the directory.
    pub removed: usize,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct WithdrawnOffer {
    pub bucket: String,
    /// Objects tombstoned by the withdrawal.
    pub removed: usize,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct OfferDirectoryRequest {
    /// Bucket name to serve the directory as. It must be free on this device.
    pub bucket: String,
    /// Absolute path of the directory to offer.
    pub root_path: String,
    /// Group the bucket belongs to, as a ULID. It decides who may reference the
    /// offered objects from the realm.
    pub group_id: String,
}

impl From<OfferedDirectory> for DeviceOffer {
    fn from(record: OfferedDirectory) -> Self {
        Self {
            bucket: record.bucket,
            group_id: record.group_id.to_string(),
            root_path: record.root,
            created_at_ms: record
                .created_at
                .duration_since(UNIX_EPOCH)
                .ok()
                .and_then(|elapsed| u64::try_from(elapsed.as_millis()).ok())
                .unwrap_or_default(),
        }
    }
}

fn map_offer_error(error: OfferedDirectoryError) -> ServerError {
    match error {
        OfferedDirectoryError::BucketTaken(_) | OfferedDirectoryError::ReadOnly(_) => {
            ServerError::Conflict(error.to_string())
        }
        OfferedDirectoryError::NotOffered(_) => ServerError::NotFound,
        OfferedDirectoryError::TooManyFiles(_) => ServerError::BadRequestReason(error.to_string()),
        OfferedDirectoryError::HandleMissing => {
            ServerError::ServiceUnavailableReason(error.to_string())
        }
        OfferedDirectoryError::Source(
            source @ (StagingSourceError::NotFound
            | StagingSourceError::AccessDenied
            | StagingSourceError::CheckError(_)),
        ) => ServerError::BadRequestReason(format!("the offered directory is unusable: {source}")),
        other => ServerError::InternalError(other.to_string()),
    }
}

/// The offer's own bucket path, which the owner must be allowed to write in the
/// named group: an offered bucket is that group's data on this device.
async fn offer_permission(
    state: &ServerState,
    auth: &AuthContext,
    group_id: GroupId,
    bucket: &str,
) -> ServerResult<()> {
    ensure_permission(
        state,
        auth,
        blob_bucket_permission_path(state.get_realm_id(), group_id, state.get_node_id(), bucket),
        Permission::WRITE,
    )
    .await
}

#[utoipa::path(
    post,
    path = "/device/offers",
    tag = "device",
    summary = "Offer a local directory as a read-only bucket",
    description = r#"Serves a directory on this machine as a read-only bucket of this device's S3 endpoint.

**Authentication**: unrestricted realm bearer token belonging to the user this device is enrolled for, with WRITE on the bucket in the named group.

**Behavior**
- The directory is walked once and every file becomes one read-only object; no byte is ever copied into the node's own blob store.
- The root is stored device-locally and re-resolved on every read. A link that leaves the offered directory is refused, and only regular files are served.
- Objects are observations: size and modification time identify a file, and the content hash is computed on the first complete stable read.
- Re-offering the same bucket refreshes the inventory: an unchanged file keeps its object version, a changed file gains a successor, and a file that is gone becomes a delete marker.
- Writes addressed to the bucket are refused for as long as the offer stands.

**Limits**
- The directory is refused whole if it holds more than 100000 files; nothing is registered in that case.
- The bucket name must be free, or already this offer's.

**Errors**
- 400 when the root does not exist, cannot be read, or holds too many files.
- 409 when an ordinary bucket already owns the name."#,
    request_body(
        content = OfferDirectoryRequest,
        description = "Bucket name, directory to offer and the group it belongs to",
        example = json!({
            "bucket": "field-photos",
            "root_path": "/home/ada/photos/field",
            "group_id": "01JGROUP0123456789ABCDEFGH"
        })
    ),
    responses(
        (status = 201, description = "The directory is offered and its inventory is registered", body = OfferedInventory,
            example = json!({"bucket": "field-photos", "files": 128, "removed": 0})),
        (status = 400, description = "Malformed group id, or an unusable directory", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller is not the user this device is enrolled for, or may not write in the group", body = ErrorResponse),
        (status = 404, description = "This node is not a user node and serves no device plane", body = ErrorResponse),
        (status = 409, description = "The bucket name is taken by an ordinary bucket", body = ErrorResponse),
        (status = 503, description = "The realm configuration has not reached this device yet", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn offer_directory(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<OfferDirectoryRequest>,
) -> ServerResult<(StatusCode, Json<OfferedInventory>)> {
    let auth = require_owner(&state, auth).await?;
    let group_id = parse_group_id(&request.group_id)?;
    offer_permission(&state, &auth, group_id, &request.bucket).await?;
    let offered = register_offer(
        &state.get_ctx(),
        OfferDirectoryInput {
            bucket: request.bucket,
            root: request.root_path,
            group_id,
            realm_id: state.get_realm_id(),
            node_id: state.get_node_id(),
            user_id: auth.user_id,
        },
    )
    .await
    .map_err(map_offer_error)?;
    Ok((
        StatusCode::CREATED,
        Json(OfferedInventory {
            bucket: offered.bucket,
            files: offered.files,
            removed: offered.removed,
        }),
    ))
}

#[utoipa::path(
    get,
    path = "/device/offers",
    tag = "device",
    summary = "List the directories this device offers",
    description = r#"Lists every directory this device currently serves as a read-only bucket.

**Authentication**: unrestricted realm bearer token belonging to the user this device is enrolled for.

**Behavior**
- The list is node-local and answers while the realm is unreachable.
- A withdrawn offer disappears from the list; the objects it left behind are delete markers."#,
    responses(
        (status = 200, description = "The directories this device offers", body = DeviceOfferList,
            example = json!({
                "offers": [{
                    "bucket": "field-photos",
                    "group_id": "01JGROUP0123456789ABCDEFGH",
                    "root_path": "/home/ada/photos/field",
                    "created_at_ms": 1756000000000u64
                }]
            })),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller is not the user this device is enrolled for", body = ErrorResponse),
        (status = 404, description = "This node is not a user node and serves no device plane", body = ErrorResponse),
        (status = 503, description = "The realm configuration has not reached this device yet", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn list_offers(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<Json<DeviceOfferList>> {
    require_owner(&state, auth).await?;
    let offers = read_offers(&state.get_ctx())
        .await
        .map_err(map_offer_error)?;
    Ok(Json(DeviceOfferList {
        offers: offers.into_iter().map(DeviceOffer::from).collect(),
    }))
}

#[utoipa::path(
    delete,
    path = "/device/offers/{bucket}",
    tag = "device",
    summary = "Stop offering a directory",
    description = r#"Withdraws one offer: the registration is removed and every object it registered becomes a delete marker.

**Authentication**: unrestricted realm bearer token belonging to the user this device is enrolled for, with WRITE on the bucket in the offer's group.

**Behavior**
- Nothing on the owner's filesystem is touched; only what this device published about it is withdrawn.
- The registration is removed first, so an interrupted withdrawal still leaves the bucket unservable rather than half-offered.
- A realm node that already references an offered object can no longer read it: the registration a read resolves the root through is gone, so those references degrade to unavailable rather than to another file.
- The bucket keeps its name and its version history, and stops being read-only. Offering the same directory again uses a new bucket name."#,
    params(("bucket" = String, Path, description = "Bucket the directory is offered as")),
    responses(
        (status = 200, description = "The offer is withdrawn", body = WithdrawnOffer,
            example = json!({"bucket": "field-photos", "removed": 128})),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller is not the user this device is enrolled for, or may not write in the group", body = ErrorResponse),
        (status = 404, description = "No such offer, or this node is not a user node", body = ErrorResponse),
        (status = 503, description = "The realm configuration has not reached this device yet", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn withdraw_offer(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(bucket): Path<String>,
) -> ServerResult<Json<WithdrawnOffer>> {
    let auth = require_owner(&state, auth).await?;
    let context = state.get_ctx();
    let offer = read_offers(&context)
        .await
        .map_err(map_offer_error)?
        .into_iter()
        .find(|offer| offer.bucket == bucket)
        .ok_or(ServerError::NotFound)?;
    offer_permission(&state, &auth, offer.group_id, &bucket).await?;
    let removed = remove_offer(
        &context,
        WithdrawOfferInput {
            bucket: bucket.clone(),
            realm_id: state.get_realm_id(),
            node_id: state.get_node_id(),
            user_id: auth.user_id,
        },
    )
    .await
    .map_err(map_offer_error)?;
    Ok(Json(WithdrawnOffer { bucket, removed }))
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
    /// Process exit status the supervisor sees once the wipe completes. A wipe
    /// that leaves paths behind exits with 80 instead.
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
- Status 79 is claimed only when every root was emptied. A wipe that left paths behind logs them and exits with status 80: data may still be on disk, so the device must not be treated as erased.
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
    use super::{
        DeviceDraft, DeviceOffer, OfferedDirectory, OfferedDirectoryError, ServerError,
        StagingSourceError, UNIX_EPOCH, map_offer_error,
    };
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
    fn lists_device_routes() {
        let openapi = serde_json::to_value(crate::openapi::ApiDoc::openapi()).unwrap();
        assert!(openapi["paths"]["/device/drafts"]["post"].is_object());
        assert!(openapi["paths"]["/device/drafts"]["get"].is_object());
        assert!(openapi["paths"]["/device/drafts/preview"]["post"].is_object());
        assert!(openapi["paths"]["/device/drafts/{draft_id}"]["delete"].is_object());
        assert!(openapi["paths"]["/device/offers"]["post"].is_object());
        assert!(openapi["paths"]["/device/offers"]["get"].is_object());
        assert!(openapi["paths"]["/device/offers/{bucket}"]["delete"].is_object());
    }

    #[test]
    fn maps_offer_errors() {
        // A taken name and an unusable directory are the owner's problem, not
        // this node's: neither may read as an internal fault.
        assert!(matches!(
            map_offer_error(OfferedDirectoryError::BucketTaken("taken".to_string())),
            ServerError::Conflict(_)
        ));
        assert!(matches!(
            map_offer_error(OfferedDirectoryError::NotOffered("gone".to_string())),
            ServerError::NotFound
        ));
        assert!(matches!(
            map_offer_error(OfferedDirectoryError::TooManyFiles(100_000)),
            ServerError::BadRequestReason(_)
        ));
        assert!(matches!(
            map_offer_error(OfferedDirectoryError::Source(StagingSourceError::NotFound)),
            ServerError::BadRequestReason(_)
        ));
    }

    #[test]
    fn maps_offer_record() {
        let group_id = Ulid::generate();
        let offer: DeviceOffer = OfferedDirectory {
            bucket: "field-photos".to_string(),
            group_id,
            root: "/home/ada/photos".to_string(),
            created_at: UNIX_EPOCH + std::time::Duration::from_millis(1_500),
            created_by: UserId::local(Ulid::generate(), RealmId::from_bytes([1u8; 32])),
        }
        .into();
        assert_eq!(offer.bucket, "field-photos");
        assert_eq!(offer.group_id, group_id.to_string());
        assert_eq!(offer.root_path, "/home/ada/photos");
        assert_eq!(offer.created_at_ms, 1_500);
    }
}
