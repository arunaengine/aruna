//! The folders this device keeps in sync with a realm bucket prefix.

use std::str::FromStr;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::Deserialize;
use ulid::Ulid;
use utoipa::{IntoParams, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::auth::parse_group_id;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server_state::ServerState;
use aruna_core::errors::StagingSourceError;
use aruna_core::structs::{
    ActionKind, ActionOutcome, ActionScope, AuthContext, FolderMode, FolderState, RemoteBinding,
};
use aruna_core::types::{Key, NodeId};
use aruna_operations::device::sync::actions::{
    ActionError, ApplyActionInput, ExpectedEntry, apply_action,
};
use aruna_operations::device::sync::folders::{
    BindFolderInput, FolderError, bind_folder, folder_counters, list_actions, list_entries,
    list_folders, read_bound, read_entry, set_folder_state, unbind_folder,
};
use aruna_operations::staging::offered_directory::OfferedDirectoryError;

use super::dto::{
    ActionRecordPage, ActionScopeName, EntryAction, FolderEntryPage, FolderEntryView,
    SyncedFolderList, SyncedFolderView, action_view, entry_view, folder_view,
};
use super::require_owner;

pub(super) fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new()
        .routes(routes!(bind_synced_folder, list_synced_folders))
        .routes(routes!(get_synced_folder, unbind_synced_folder))
        .routes(routes!(pause_folder))
        .routes(routes!(resume_folder))
        .routes(routes!(sync_folder))
        .routes(routes!(list_folder_entries))
        .routes(routes!(act_on_entry))
        .routes(routes!(act_on_folder, list_folder_actions))
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct BindFolderRequest {
    /// Absolute path of the directory to sync.
    pub root: String,
    /// Group the folder's data belongs to, as a ULID.
    pub group_id: String,
    /// Realm node that holds the remote bucket.
    pub remote_node_id: String,
    pub remote_bucket: String,
    /// Key prefix inside the remote bucket. Empty binds the whole bucket.
    #[serde(default)]
    pub remote_prefix: String,
    /// `two_way` (default) or `upload_only`.
    #[serde(default)]
    pub mode: Option<super::dto::FolderModeName>,
    /// Whether a local delete becomes a realm delete marker. Default true.
    #[serde(default)]
    pub propagate_deletes: Option<bool>,
}

#[derive(Debug, serde::Serialize, Deserialize, ToSchema)]
pub struct UnboundFolder {
    pub folder_id: String,
    /// Objects the device tombstoned in its own observation bucket.
    pub removed: usize,
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct EntryQuery {
    /// Only entries in this state, by its snake_case name.
    pub state: Option<String>,
    /// Cursor from a previous page.
    pub cursor: Option<String>,
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct ActionQuery {
    pub cursor: Option<String>,
}

/// The bytes the owner was shown. A replacement applies to exactly these bytes.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ExpectedBytes {
    /// The local side's `fingerprint`, echoed back unchanged.
    pub fingerprint: String,
    /// Hex-encoded blake3 of the local file the owner decided about.
    pub blake3: String,
    #[serde(default)]
    pub remote_version: Option<String>,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct EntryActionRequest {
    pub action: EntryAction,
    /// Required for `replace_local`.
    #[serde(default)]
    pub expected: Option<ExpectedBytes>,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct FolderActionRequest {
    /// Only `replace_local` applies to a whole folder.
    pub action: EntryAction,
    /// Only `all_pending` is accepted.
    pub scope: ActionScopeName,
    /// The last path segment of the folder's root, typed back.
    pub confirm: String,
}

fn parse_folder_id(folder_id: &str) -> ServerResult<Ulid> {
    Ulid::from_str(folder_id).map_err(|_| ServerError::BadRequest)
}

fn parse_hash(hash: &str) -> ServerResult<[u8; 32]> {
    if hash.len() != 64 {
        return Err(ServerError::BadRequestReason(
            "blake3 must be 64 hex characters".to_string(),
        ));
    }
    let mut bytes = [0u8; 32];
    for (index, slot) in bytes.iter_mut().enumerate() {
        *slot = u8::from_str_radix(&hash[index * 2..index * 2 + 2], 16)
            .map_err(|_| ServerError::BadRequestReason("blake3 must be hex".to_string()))?;
    }
    Ok(bytes)
}

/// The name the owner types to confirm a folder-wide replacement: the last
/// segment of the root, whichever separator the platform writes.
fn folder_name(root: &str) -> String {
    root.trim()
        .trim_end_matches(['/', '\\'])
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or_default()
        .trim()
        .to_string()
}

fn encode_cursor(cursor: Option<Key>) -> Option<String> {
    use base64::Engine;
    cursor.map(|cursor| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(cursor.as_ref()))
}

fn decode_cursor(cursor: Option<String>) -> ServerResult<Option<Key>> {
    use base64::Engine;
    cursor
        .map(|cursor| {
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(cursor)
                .map(Key::from)
                .map_err(|_| ServerError::BadRequestReason("malformed cursor".to_string()))
        })
        .transpose()
}

pub(super) fn map_folder_error(error: FolderError) -> ServerError {
    match error {
        FolderError::NotFound => ServerError::NotFound,
        FolderError::TooManyFolders(_)
        | FolderError::RootOverlaps(_)
        | FolderError::BucketBound(_) => ServerError::Conflict(error.to_string()),
        FolderError::Unavailable => ServerError::ServiceUnavailableReason(error.to_string()),
        FolderError::Offer(offer) => map_offer_error(offer),
    }
}

/// An unusable directory is the owner's problem, not this node's.
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
        ) => ServerError::BadRequestReason(format!("the directory is unusable: {source}")),
        other => ServerError::InternalError(other.to_string()),
    }
}

fn map_action_error(error: ActionError) -> ServerError {
    match error {
        ActionError::NotFound => ServerError::NotFound,
        ActionError::NoRemoteVersion | ActionError::ExpectedMissing => {
            ServerError::BadRequestReason(error.to_string())
        }
        ActionError::RemoteUnavailable | ActionError::Unavailable => {
            ServerError::ServiceUnavailableReason(error.to_string())
        }
        ActionError::Folder(folder) => map_folder_error(folder),
        ActionError::Storage(error) => ServerError::InternalError(error.to_string()),
    }
}

/// One folder with the counters its entries currently add up to.
async fn folder_detail(
    state: &ServerState,
    folder: aruna_core::structs::SyncedFolder,
) -> ServerResult<SyncedFolderView> {
    let counters = folder_counters(&state.get_ctx(), folder.folder_id)
        .await
        .map_err(map_folder_error)?;
    Ok(folder_view(folder, counters))
}

#[utoipa::path(
    post,
    path = "/device/folders",
    tag = "device",
    summary = "Bind a local directory to a realm bucket prefix",
    description = r#"Keeps a directory on this machine in sync with one prefix of one realm bucket.

**Authentication**: unrestricted realm bearer token belonging to the user this device is enrolled for. A device holds no group authorization document, so the owner binding is the whole local authority: the realm node that serves the folder authorizes the owner's WRITE on the remote bucket every time it pulls, and refuses the pull otherwise.

**Behavior**
- The device-local bucket is derived from the folder id, so the owner never names it and it cannot collide with a bucket they already use.
- The directory is walked once and every file becomes one read-only object of that bucket; no byte is copied into this node's blob store.
- `two_way` syncs both directions. `upload_only` never writes to disk and replaces the earlier offered-directory model.
- Local data always wins locally: a file is replaced automatically only while its fingerprint and its blake3 still equal the recorded synced base. Everything else is preserved and reported, the incoming version lands beside it as a conflicted copy, and replacing or removing local bytes is an explicit action.
- A remote deletion never deletes a local file. A local deletion becomes a realm delete marker while `propagate_deletes` is set.

**Limits**
- A device binds at most 64 folders, and a folder holds at most 100000 files.
- Roots must not nest.

**Errors**
- 400 when the root does not exist, cannot be read, or holds too many files.
- 409 when the root overlaps a bound folder."#,
    request_body(
        content = BindFolderRequest,
        description = "Local directory and the realm prefix to bind it to",
        example = json!({
            "root": "/home/ada/data",
            "group_id": "01JGROUP0123456789ABCDEFGH",
            "remote_node_id": "k5r2gmr7qeqfhqxhbpcpqoa2xhpqcrmr2vpxjqx3nvxfvbxvvrga",
            "remote_bucket": "lab-data",
            "remote_prefix": "ada",
            "mode": "two_way",
            "propagate_deletes": true
        })
    ),
    responses(
        (status = 201, description = "The folder is bound and its first observation is registered", body = SyncedFolderView),
        (status = 400, description = "Malformed ids, or an unusable directory", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller is not the user this device is enrolled for", body = ErrorResponse),
        (status = 404, description = "This node is not a user node and serves no device plane", body = ErrorResponse),
        (status = 409, description = "The root overlaps a bound folder", body = ErrorResponse),
        (status = 503, description = "The realm configuration has not reached this device yet", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn bind_synced_folder(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<BindFolderRequest>,
) -> ServerResult<(StatusCode, Json<SyncedFolderView>)> {
    let auth = require_owner(&state, auth).await?;
    let group_id = parse_group_id(&request.group_id)?;
    let folder_id = Ulid::generate();
    let node_id = NodeId::from_str(&request.remote_node_id).map_err(|_| {
        ServerError::BadRequestReason("remote_node_id is not a node id".to_string())
    })?;
    let folder = bind_folder(
        &state.get_ctx(),
        BindFolderInput {
            folder_id,
            root: request.root,
            group_id,
            remote: RemoteBinding {
                node_id,
                bucket: request.remote_bucket,
                prefix: request.remote_prefix,
            },
            mode: match request.mode {
                Some(super::dto::FolderModeName::UploadOnly) => FolderMode::UploadOnly,
                _ => FolderMode::TwoWay,
            },
            propagate_deletes: request.propagate_deletes.unwrap_or(true),
            realm_id: state.get_realm_id(),
            node_id: state.get_node_id(),
            user_id: auth.user_id,
        },
    )
    .await
    .map_err(map_folder_error)?;
    Ok((
        StatusCode::CREATED,
        Json(folder_detail(&state, folder).await?),
    ))
}

#[utoipa::path(
    get,
    path = "/device/folders",
    tag = "device",
    summary = "List the folders this device syncs",
    description = r#"Lists every directory this device keeps in sync, with its counters.

**Authentication**: unrestricted realm bearer token belonging to the user this device is enrolled for.

**Behavior**
- The list is node-local and answers while the realm is unreachable."#,
    responses(
        (status = 200, description = "The folders this device binds", body = SyncedFolderList),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller is not the user this device is enrolled for", body = ErrorResponse),
        (status = 404, description = "This node is not a user node and serves no device plane", body = ErrorResponse),
        (status = 503, description = "The realm configuration has not reached this device yet", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn list_synced_folders(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<Json<SyncedFolderList>> {
    require_owner(&state, auth).await?;
    let bound = list_folders(&state.get_ctx())
        .await
        .map_err(map_folder_error)?;
    let mut folders = Vec::with_capacity(bound.len());
    for folder in bound {
        folders.push(folder_detail(&state, folder).await?);
    }
    Ok(Json(SyncedFolderList { folders }))
}

#[utoipa::path(
    get,
    path = "/device/folders/{folder_id}",
    tag = "device",
    summary = "Read one folder and its counters",
    description = r#"Reads one bound folder together with how much of it still needs attention.

**Authentication**: unrestricted realm bearer token belonging to the user this device is enrolled for.

**Behavior**
- The counters are derived from the stored entries, so they always describe what the entries route would list."#,
    params(("folder_id" = String, Path, description = "Folder ULID")),
    responses(
        (status = 200, description = "The folder and its counters", body = SyncedFolderView),
        (status = 400, description = "The folder id is not a ULID", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller is not the user this device is enrolled for", body = ErrorResponse),
        (status = 404, description = "No such folder, or this node is not a user node", body = ErrorResponse),
        (status = 503, description = "The realm configuration has not reached this device yet", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn get_synced_folder(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(folder_id): Path<String>,
) -> ServerResult<Json<SyncedFolderView>> {
    require_owner(&state, auth).await?;
    let folder_id = parse_folder_id(&folder_id)?;
    let folder = read_bound(&state.get_ctx(), folder_id)
        .await
        .map_err(map_folder_error)?;
    Ok(Json(folder_detail(&state, folder).await?))
}

#[utoipa::path(
    delete,
    path = "/device/folders/{folder_id}",
    tag = "device",
    summary = "Stop syncing a folder",
    description = r#"Unbinds one folder. Nothing on the owner's filesystem is touched.

**Authentication**: unrestricted realm bearer token belonging to the user this device is enrolled for.

**Authorization**: the owner binding alone, as for binding; the realm objects the folder published are untouched, so nothing outside this device is decided here.

**Behavior**
- The binding, its merge bases, its queued uploads and its audit log are removed from this device.
- The device's own observation bucket is emptied with delete markers; the realm objects the folder published stay.
- The files on disk, including any conflicted copies and anything in the folder's trash, are left exactly as they are."#,
    params(("folder_id" = String, Path, description = "Folder ULID")),
    responses(
        (status = 200, description = "The folder is unbound", body = UnboundFolder),
        (status = 400, description = "The folder id is not a ULID", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller is not the user this device is enrolled for", body = ErrorResponse),
        (status = 404, description = "No such folder, or this node is not a user node", body = ErrorResponse),
        (status = 503, description = "The realm configuration has not reached this device yet", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn unbind_synced_folder(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(folder_id): Path<String>,
) -> ServerResult<Json<UnboundFolder>> {
    let auth = require_owner(&state, auth).await?;
    let folder_id = parse_folder_id(&folder_id)?;
    let context = state.get_ctx();
    // An unknown folder answers 404 before anything is torn down.
    read_bound(&context, folder_id)
        .await
        .map_err(map_folder_error)?;
    let removed = unbind_folder(
        &context,
        folder_id,
        state.get_realm_id(),
        state.get_node_id(),
        auth.user_id,
    )
    .await
    .map_err(map_folder_error)?;
    Ok(Json(UnboundFolder {
        folder_id: folder_id.to_string(),
        removed,
    }))
}

#[utoipa::path(
    post,
    path = "/device/folders/{folder_id}/pause",
    tag = "device",
    summary = "Pause one folder",
    description = r#"Stops reconciling one folder until it is resumed.

**Authentication**: unrestricted realm bearer token belonging to the user this device is enrolled for.

**Behavior**
- Queued uploads stay queued; nothing on disk changes."#,
    params(("folder_id" = String, Path, description = "Folder ULID")),
    responses(
        (status = 200, description = "The folder is paused", body = SyncedFolderView),
        (status = 400, description = "The folder id is not a ULID", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller is not the user this device is enrolled for", body = ErrorResponse),
        (status = 404, description = "No such folder, or this node is not a user node", body = ErrorResponse),
        (status = 503, description = "The realm configuration has not reached this device yet", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn pause_folder(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(folder_id): Path<String>,
) -> ServerResult<Json<SyncedFolderView>> {
    require_owner(&state, auth).await?;
    let folder_id = parse_folder_id(&folder_id)?;
    let folder = set_folder_state(&state.get_ctx(), folder_id, FolderState::Paused)
        .await
        .map_err(map_folder_error)?;
    Ok(Json(folder_detail(&state, folder).await?))
}

#[utoipa::path(
    post,
    path = "/device/folders/{folder_id}/resume",
    tag = "device",
    summary = "Resume one folder",
    description = r#"Reconciles the folder again from the next pass on.

**Authentication**: unrestricted realm bearer token belonging to the user this device is enrolled for."#,
    params(("folder_id" = String, Path, description = "Folder ULID")),
    responses(
        (status = 200, description = "The folder is active again", body = SyncedFolderView),
        (status = 400, description = "The folder id is not a ULID", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller is not the user this device is enrolled for", body = ErrorResponse),
        (status = 404, description = "No such folder, or this node is not a user node", body = ErrorResponse),
        (status = 503, description = "The realm configuration has not reached this device yet", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn resume_folder(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(folder_id): Path<String>,
) -> ServerResult<Json<SyncedFolderView>> {
    require_owner(&state, auth).await?;
    let folder_id = parse_folder_id(&folder_id)?;
    let folder = set_folder_state(&state.get_ctx(), folder_id, FolderState::Active)
        .await
        .map_err(map_folder_error)?;
    Ok(Json(folder_detail(&state, folder).await?))
}

#[utoipa::path(
    post,
    path = "/device/folders/{folder_id}/sync",
    tag = "device",
    summary = "Reconcile one folder now",
    description = r#"Runs one reconciliation of the folder without waiting for the timer.

**Authentication**: unrestricted realm bearer token belonging to the user this device is enrolled for.

**Behavior**
- The pass observes the directory, lists the realm heads and applies exactly what the sync is allowed to apply.
- A paused folder is refused."#,
    params(("folder_id" = String, Path, description = "Folder ULID")),
    responses(
        (status = 200, description = "The folder was reconciled", body = SyncedFolderView),
        (status = 400, description = "The folder id is not a ULID", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller is not the user this device is enrolled for", body = ErrorResponse),
        (status = 404, description = "No such folder, or this node is not a user node", body = ErrorResponse),
        (status = 409, description = "The folder is paused", body = ErrorResponse),
        (status = 503, description = "The folder could not be reconciled right now", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn sync_folder(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(folder_id): Path<String>,
) -> ServerResult<Json<SyncedFolderView>> {
    require_owner(&state, auth).await?;
    let folder_id = parse_folder_id(&folder_id)?;
    let context = state.get_ctx();
    let folder = read_bound(&context, folder_id)
        .await
        .map_err(map_folder_error)?;
    if folder.state != FolderState::Active {
        return Err(ServerError::Conflict(
            "the folder is not active".to_string(),
        ));
    }
    aruna_operations::device::sync::reconcile_folder(&context, &folder)
        .await
        .ok_or_else(|| {
            ServerError::ServiceUnavailableReason("the folder could not be reconciled".to_string())
        })?;
    let folder = read_bound(&context, folder_id)
        .await
        .map_err(map_folder_error)?;
    Ok(Json(folder_detail(&state, folder).await?))
}

#[utoipa::path(
    get,
    path = "/device/folders/{folder_id}/entries",
    tag = "device",
    summary = "List one folder's entries",
    description = r#"Lists the reconciled paths of one folder, in key order.

**Authentication**: unrestricted realm bearer token belonging to the user this device is enrolled for.

**Behavior**
- `state` filters to one state name, for example `conflict` or `pending_replace`.
- Each entry names both sides it was last seen with. The local side's `fingerprint` and `blake3` are what an action echoes back as `expected`.
- The page carries an opaque `next_cursor` while more entries follow."#,
    params(
        ("folder_id" = String, Path, description = "Folder ULID"),
        EntryQuery
    ),
    responses(
        (status = 200, description = "One page of the folder's entries", body = FolderEntryPage),
        (status = 400, description = "Malformed folder id or cursor", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller is not the user this device is enrolled for", body = ErrorResponse),
        (status = 404, description = "No such folder, or this node is not a user node", body = ErrorResponse),
        (status = 503, description = "The realm configuration has not reached this device yet", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn list_folder_entries(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(folder_id): Path<String>,
    Query(query): Query<EntryQuery>,
) -> ServerResult<Json<FolderEntryPage>> {
    require_owner(&state, auth).await?;
    let folder_id = parse_folder_id(&folder_id)?;
    let cursor = decode_cursor(query.cursor)?;
    let (entries, next) = list_entries(&state.get_ctx(), folder_id, query.state.as_deref(), cursor)
        .await
        .map_err(map_folder_error)?;
    Ok(Json(FolderEntryPage {
        entries: entries
            .into_iter()
            .map(|(path, base)| entry_view(path, base))
            .collect(),
        next_cursor: encode_cursor(next),
    }))
}

#[utoipa::path(
    post,
    path = "/device/folders/{folder_id}/entries/{path}/actions",
    tag = "device",
    summary = "Decide one entry explicitly",
    description = r#"Applies the owner's decision to one entry. This is the only way local bytes are replaced or removed.

**Authentication**: unrestricted realm bearer token belonging to the user this device is enrolled for.

**Behavior**
- `replace_local` writes the remote version over the local file, but only while the file still carries exactly the bytes `expected` names. Anything else keeps the file and answers 412.
- `keep_local` publishes the local bytes as the next realm version and clears the pending state.
- `remove_local` moves the file into the folder's `.aruna/trash/` directory. Nothing is ever unlinked.
- `resolve` accepts the current state without touching either side.
- Every action leaves an audit row, committed with the state it changed. The reply is the entry as it now stands.

**Limits**
- The entry path is one URL-encoded path segment relative to the folder root; separators are percent-encoded.
- `expected` is required for `replace_local` and carries the entry's local `fingerprint` and `blake3`.

**Errors**
- 412 when the bytes changed since the owner saw them, or when `expected` is missing for a replacement. The file is preserved and the attempt is audited."#,
    params(
        ("folder_id" = String, Path, description = "Folder ULID"),
        ("path" = String, Path, description = "URL-encoded path relative to the folder root")
    ),
    request_body(
        content = EntryActionRequest,
        description = "The decision and the bytes it was taken on",
        example = json!({
            "action": "replace_local",
            "expected": {
                "fingerprint": "2a-18f0c1d2e3",
                "blake3": "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"
            }
        })
    ),
    responses(
        (status = 200, description = "The entry as it stands after the action", body = FolderEntryView),
        (status = 400, description = "Malformed ids or hashes", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller is not the user this device is enrolled for", body = ErrorResponse),
        (status = 404, description = "No such folder or entry, or this node is not a user node", body = ErrorResponse),
        (status = 412, description = "The bytes changed since the owner saw them; the file is preserved", body = ErrorResponse),
        (status = 503, description = "The remote version or the device store is unavailable", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn act_on_entry(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((folder_id, path)): Path<(String, String)>,
    Json(request): Json<EntryActionRequest>,
) -> ServerResult<Json<FolderEntryView>> {
    let auth = require_owner(&state, auth).await?;
    let folder_id = parse_folder_id(&folder_id)?;
    let expected = request
        .expected
        .map(|expected| {
            Ok::<_, ServerError>(ExpectedEntry {
                fingerprint: expected.fingerprint,
                blake3: parse_hash(&expected.blake3)?,
                remote_version: expected
                    .remote_version
                    .map(|version| Ulid::from_str(&version))
                    .transpose()
                    .map_err(|_| ServerError::BadRequest)?,
            })
        })
        .transpose()?;
    if request.action == EntryAction::ReplaceLocal && expected.is_none() {
        return Err(ServerError::PreconditionFailed(
            "a replacement must name the bytes the owner saw".to_string(),
        ));
    }
    let context = state.get_ctx();
    let record = apply_action(
        &context,
        ApplyActionInput {
            folder_id,
            kind: action_kind(request.action),
            scope: ActionScope::Entry {
                relative: path.clone(),
            },
            expected,
            actor: auth.user_id,
        },
    )
    .await
    .map_err(map_action_error)?;
    if record.outcome == ActionOutcome::Stale {
        return Err(ServerError::PreconditionFailed(
            "the file changed since the owner saw it".to_string(),
        ));
    }
    let base = read_entry(&context, folder_id, &path)
        .await
        .map_err(map_folder_error)?;
    Ok(Json(entry_view(path, base)))
}

fn action_kind(action: EntryAction) -> ActionKind {
    match action {
        EntryAction::ReplaceLocal => ActionKind::Replace,
        EntryAction::KeepLocal => ActionKind::KeepLocal,
        EntryAction::RemoveLocal => ActionKind::RemoveLocal,
        EntryAction::Resolve => ActionKind::Resolve,
    }
}

#[utoipa::path(
    post,
    path = "/device/folders/{folder_id}/actions",
    tag = "device",
    summary = "Decide every pending entry of a folder",
    description = r#"Replaces every entry of the folder that is waiting for a decision.

**Authentication**: unrestricted realm bearer token belonging to the user this device is enrolled for.

**Behavior**
- Only `replace_local` at scope `all_pending` is accepted, and it covers conflicts and pending replacements only. A remote deletion is a removal and stays a per-entry decision.
- Each entry is guarded by the bytes of its last observation, so a file that changed since the listing is preserved and counted as stale instead of being overwritten.
- One audit row records the scope, how many entries it applied to and whether any were stale. The reply is the folder with fresh counters.

**Limits**
- `confirm` must equal the last path segment of the folder's root."#,
    params(("folder_id" = String, Path, description = "Folder ULID")),
    request_body(
        content = FolderActionRequest,
        description = "The folder-wide decision and the typed confirmation",
        example = json!({"action": "replace_local", "scope": "all_pending", "confirm": "data"})
    ),
    responses(
        (status = 200, description = "The folder with fresh counters", body = SyncedFolderView),
        (status = 400, description = "Malformed folder id or an unsupported action", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller is not the user this device is enrolled for", body = ErrorResponse),
        (status = 404, description = "No such folder, or this node is not a user node", body = ErrorResponse),
        (status = 409, description = "The confirmation does not name this folder", body = ErrorResponse),
        (status = 503, description = "The device store is unavailable", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn act_on_folder(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(folder_id): Path<String>,
    Json(request): Json<FolderActionRequest>,
) -> ServerResult<Json<SyncedFolderView>> {
    let auth = require_owner(&state, auth).await?;
    let folder_id = parse_folder_id(&folder_id)?;
    if request.action != EntryAction::ReplaceLocal || request.scope != ActionScopeName::AllPending {
        return Err(ServerError::BadRequestReason(
            "only replace_local at scope all_pending applies to a whole folder".to_string(),
        ));
    }
    let context = state.get_ctx();
    let folder = read_bound(&context, folder_id)
        .await
        .map_err(map_folder_error)?;
    if request.confirm != folder_name(&folder.root) {
        return Err(ServerError::Conflict(
            "the confirmation must name this folder".to_string(),
        ));
    }
    apply_action(
        &context,
        ApplyActionInput {
            folder_id,
            kind: ActionKind::Replace,
            scope: ActionScope::AllPending,
            expected: None,
            actor: auth.user_id,
        },
    )
    .await
    .map_err(map_action_error)?;
    Ok(Json(folder_detail(&state, folder).await?))
}

#[utoipa::path(
    get,
    path = "/device/folders/{folder_id}/actions",
    tag = "device",
    summary = "Read one folder's audit log",
    description = r#"Lists the explicit decisions taken on this folder, oldest first.

**Authentication**: unrestricted realm bearer token belonging to the user this device is enrolled for.

**Behavior**
- Every replacement, removal and resolution is recorded, including the ones that were refused because the bytes had changed."#,
    params(
        ("folder_id" = String, Path, description = "Folder ULID"),
        ActionQuery
    ),
    responses(
        (status = 200, description = "One page of the folder's audit log", body = ActionRecordPage),
        (status = 400, description = "Malformed folder id or cursor", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The caller is not the user this device is enrolled for", body = ErrorResponse),
        (status = 404, description = "No such folder, or this node is not a user node", body = ErrorResponse),
        (status = 503, description = "The realm configuration has not reached this device yet", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
async fn list_folder_actions(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(folder_id): Path<String>,
    Query(query): Query<ActionQuery>,
) -> ServerResult<Json<ActionRecordPage>> {
    require_owner(&state, auth).await?;
    let folder_id = parse_folder_id(&folder_id)?;
    let cursor = decode_cursor(query.cursor)?;
    let (actions, next) = list_actions(&state.get_ctx(), folder_id, cursor)
        .await
        .map_err(map_folder_error)?;
    Ok(Json(ActionRecordPage {
        actions: actions.into_iter().map(action_view).collect(),
        next_cursor: encode_cursor(next),
    }))
}

#[cfg(test)]
mod tests {
    use super::{
        FolderError, ServerError, folder_name, map_action_error, map_folder_error, parse_hash,
    };
    use crate::routes::device::dto::hex_hash;
    use aruna_operations::device::sync::actions::ActionError;

    #[test]
    fn round_trips_hashes() {
        let hash = [0xabu8; 32];
        assert_eq!(parse_hash(&hex_hash(&hash)).unwrap(), hash);
        assert!(parse_hash("nothex").is_err());
        assert!(parse_hash(&"z".repeat(64)).is_err());
    }

    #[test]
    fn names_folder_root() {
        // The confirmation is the directory's own name, on either platform.
        assert_eq!(folder_name("/home/ada/data"), "data");
        assert_eq!(folder_name("/home/ada/data/"), "data");
        assert_eq!(folder_name(r"C:\Users\ada\Data"), "Data");
        assert_eq!(folder_name("data"), "data");
    }

    #[test]
    fn maps_folder_errors() {
        // A nested root is the owner's problem, never an internal fault.
        assert!(matches!(
            map_folder_error(FolderError::RootOverlaps("/home/ada".to_string())),
            ServerError::Conflict(_)
        ));
        assert!(matches!(
            map_folder_error(FolderError::NotFound),
            ServerError::NotFound
        ));
        assert!(matches!(
            map_action_error(ActionError::ExpectedMissing),
            ServerError::BadRequestReason(_)
        ));
        assert!(matches!(
            map_action_error(ActionError::RemoteUnavailable),
            ServerError::ServiceUnavailableReason(_)
        ));
    }
}
