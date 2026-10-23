//! Serves the routes that organize a linked folder's bucket: re-map keys by file name, remove
//! duplicate stored files, and pause, resume or abort the bucket's long job.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::imports::{admin_of_group, existing_rule, map_error};
use crate::auth::ValidatedBearer;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server::state::ServerState;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::backend_import::BackendImport;
use aruna_core::structs::storage::linked_job::{LinkedJob, LinkedState, LinkedTask, RemapPlan};
use aruna_operations::auth::bearer_token::token_grant;
use aruna_operations::auth::request_authorization::BucketAccess;
use aruna_operations::blob::unify;
use aruna_operations::driver::{drive, now_ms};
use aruna_operations::groups::imports::discovery::bucket_access;
use aruna_operations::groups::imports::last_run;
use aruna_operations::s3::bucket::linked::{
    BucketJob, BucketJobOperation, LinkedAction, LinkedError, read_bucket_job, rename_total,
};
use aruna_operations::s3::bucket::remap::{self, RemapScope};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

#[derive(OpenApi)]
#[openapi()]
pub struct OrganizeDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(OrganizeDoc::openapi())
        .routes(routes!(preview_move))
        .routes(routes!(start_move))
        .routes(routes!(preview_duplicates))
        .routes(routes!(start_duplicates))
        .routes(routes!(get_job))
        .routes(routes!(control_job))
        .routes(routes!(link_status))
}

/// One page of a preview: an optional name filter, the page size and the `next` cursor of the
/// page before.
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct PageRequest {
    /// Only keys that contain this text.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,
    /// Entries per page: 1 to 1000, default 100.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    /// The `next` value of the page before; omitted for the first page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct MoveRequest {
    /// File name pattern: `*`, `?`, `[0-9]`, `{4}` or `{2,8}` repeats, `{fna,fa}` either.
    pub pattern: String,
    /// The new key: `{name}` is the file name, `{dir}` the folders below `from`.
    pub target: String,
    /// Folder whose keys are selected: empty for the bucket root, else ending with `/`.
    /// Default: the rule's upload folder.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<String>,
    /// Also selects keys in folders below `from`.
    #[serde(default)]
    pub subfolders: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct MovePreviewRequest {
    #[serde(flatten)]
    pub request: MoveRequest,
    #[serde(flatten)]
    pub page: PageRequest,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct MoveItem {
    pub from: String,
    pub to: String,
    /// Why the key keeps its name, such as a taken new key; omitted when it moves.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conflict: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct MovePreview {
    /// Selected keys the caller may read; a lower bound while `complete` is false.
    pub total: u64,
    pub complete: bool,
    pub items: Vec<MoveItem>,
    /// Cursor of the next page; omitted on the last page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next: Option<String>,
    /// Why no key can move now, such as a running import; the job waits for it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocked: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct StoredFile {
    pub path: String,
    pub bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct DuplicateItem {
    pub key: String,
    /// The file the key's versions keep reading.
    pub kept: StoredFile,
    /// The files with the same content that are released.
    pub removed: Vec<StoredFile>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct DuplicatePreview {
    /// Keys with duplicate files and the bytes their release frees; lower bounds while
    /// `complete` is false.
    pub total: u64,
    pub bytes: u64,
    pub complete: bool,
    pub items: Vec<DuplicateItem>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next: Option<String>,
}

/// The long job of the bucket: a move of keys, a duplicate cleanup or a rename of files.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct OrganizeJob {
    /// `move`, `duplicates` or `rename`.
    pub kind: String,
    /// `running`, `paused`, `done` or `aborted`.
    pub state: String,
    pub done: u64,
    pub skipped: u64,
    pub failed: u64,
    /// Keys selected at the start, or the files a rename planned to rename; a lower bound while
    /// `counted` is false, and 0 for a rename whose start stored no total.
    pub total: u64,
    pub counted: bool,
    /// Why the job waits before its next step, such as a running import.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub waiting: Option<String>,
    pub started_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at_ms: Option<u64>,
    /// The job can still be paused, resumed or aborted.
    pub controls: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct LinkConflict {
    pub path: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct LinkStatus {
    /// Not counted per caller yet; always omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub files: Option<u64>,
    /// Not counted yet; always omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes: Option<u64>,
    /// Files the last sync could not take, such as a file two links claim.
    pub conflicts: Vec<LinkConflict>,
}

impl From<BucketJob> for OrganizeJob {
    fn from(job: BucketJob) -> Self {
        match job {
            BucketJob::Linked(job) => linked_job(job),
            BucketJob::Rename(record, request) => {
                let state = match (request.aborted_at_ms, record.finished_at_ms, request.paused) {
                    (Some(_), ..) => "aborted",
                    (None, Some(_), _) => "done",
                    (None, None, true) => "paused",
                    (None, None, false) => "running",
                };
                OrganizeJob {
                    kind: "rename".to_string(),
                    state: state.to_string(),
                    done: record.migrated,
                    skipped: record.skipped,
                    failed: record.failed,
                    total: 0,
                    counted: false,
                    waiting: None,
                    started_at_ms: record.started_at_ms,
                    finished_at_ms: record.finished_at_ms,
                    controls: matches!(state, "running" | "paused"),
                }
            }
        }
    }
}

fn linked_job(job: LinkedJob) -> OrganizeJob {
    let kind = match job.task {
        LinkedTask::Remap(_) => "move",
        LinkedTask::Unify { .. } => "duplicates",
    };
    let state = match job.state {
        LinkedState::Running => "running",
        LinkedState::Paused => "paused",
        LinkedState::Aborted => "aborted",
        LinkedState::Finished => "done",
    };
    OrganizeJob {
        kind: kind.to_string(),
        state: state.to_string(),
        done: job.done,
        skipped: job.skipped,
        failed: job.failed,
        total: job.total,
        counted: job.counted,
        waiting: job.waiting.clone(),
        started_at_ms: job.started_at_ms,
        finished_at_ms: job.finished_at_ms,
        controls: job.is_active(),
    }
}

fn job_error(error: LinkedError) -> ServerError {
    match error {
        LinkedError::Invalid(message) => ServerError::BadRequestMessage(message),
        LinkedError::NotFound => ServerError::NotFound,
        LinkedError::Busy(message) => {
            ServerError::Refused(StatusCode::CONFLICT, "job_running", message)
        }
        LinkedError::NoJob(message) => {
            ServerError::Refused(StatusCode::CONFLICT, "no_job", message)
        }
        LinkedError::Storage(aruna_core::errors::StorageError::TransactionConflict) => {
            ServerError::Conflict("the bucket job changed meanwhile; try again".to_string())
        }
        other => ServerError::InternalError(other.to_string()),
    }
}

/// The rule, the caller and the caller's WRITE on its bucket; restricted tokens are refused.
async fn caller(
    state: &ServerState,
    auth: Option<AuthContext>,
    (group_id, import_id): (&str, &str),
) -> ServerResult<(BackendImport, AuthContext, BucketAccess)> {
    let (group_id, auth) = admin_of_group(state, auth, group_id).await?;
    let rule = existing_rule(state, group_id, import_id).await?;
    let access = bucket_access(&state.get_ctx(), &auth, state.get_node_id(), &rule)
        .await
        .map_err(map_error)?;
    Ok((rule, auth, access))
}

fn page_of(page: &PageRequest) -> ServerResult<(Option<String>, usize)> {
    let limit = page.limit.unwrap_or(remap::DEFAULT_PAGE);
    if !(1..=remap::MAX_PAGE).contains(&limit) {
        return Err(ServerError::BadRequestMessage(format!(
            "limit takes 1 to {}",
            remap::MAX_PAGE
        )));
    }
    let after = page.after.as_deref().map(decode_cursor).transpose()?;
    Ok((after, limit))
}

/// Cursors name a key the caller may read; they are encoded so clients pass them back as is.
fn encode_cursor(key: String) -> String {
    URL_SAFE_NO_PAD.encode(key)
}

fn decode_cursor(cursor: &str) -> ServerResult<String> {
    let bytes = URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| ServerError::BadRequestMessage("`after` is not a cursor".to_string()))?;
    String::from_utf8(bytes)
        .map_err(|_| ServerError::BadRequestMessage("`after` is not a cursor".to_string()))
}

fn plan(rule: &BackendImport, request: MoveRequest) -> ServerResult<RemapPlan> {
    let plan = RemapPlan {
        from: request.from.unwrap_or_else(|| rule.key_prefix.clone()),
        pattern: request.pattern,
        template: request.target,
        subfolders: request.subfolders,
    };
    plan.check().map_err(ServerError::BadRequestMessage)?;
    Ok(plan)
}

fn scope<'a>(
    state: &ServerState,
    (rule, auth): (&'a BackendImport, &AuthContext),
    access: &'a BucketAccess,
) -> RemapScope<'a> {
    RemapScope {
        realm_id: auth.realm_id,
        group_id: rule.group_id,
        node_id: state.get_node_id(),
        bucket: &rule.bucket,
        access,
    }
}

/// Starts a job of the rule's bucket as the caller, whose token the job checks on each page.
async fn start_job(
    state: &ServerState,
    (rule, auth, bearer): (&BackendImport, AuthContext, Option<ValidatedBearer>),
    (task, total): (LinkedTask, (u64, bool)),
) -> ServerResult<(StatusCode, Json<OrganizeJob>)> {
    let bearer = bearer.ok_or(ServerError::Unauthorized)?;
    let grant = token_grant(bearer.as_str()).map_err(|_| ServerError::Unauthorized)?;
    let now = now_ms();
    let job = LinkedJob {
        id: Ulid::generate(),
        task,
        group_id: rule.group_id,
        auth,
        node_id: state.get_node_id(),
        grant,
        state: LinkedState::Running,
        cursor: None,
        total: total.0,
        counted: total.1,
        done: 0,
        skipped: 0,
        failed: 0,
        waiting: None,
        started_at_ms: now,
        updated_at_ms: now,
        finished_at_ms: None,
    };
    let action = LinkedAction::Start(Box::new(job));
    let operation = BucketJobOperation::new(rule.bucket.clone(), rule.group_id, action, now);
    let job = drive(operation, &state.get_ctx())
        .await
        .map_err(job_error)?;
    Ok((StatusCode::ACCEPTED, Json(job.into())))
}

#[utoipa::path(
    post,
    path = "/data/groups/{group_id}/storage/imports/{import_id}/organize/move/preview",
    tag = "data/storage",
    summary = "Preview a move of keys by file name",
    description = r#"Lists one page of the keys of the rule's bucket a move by file name selects, with the new key of each and the reason a key would keep its name. Only keys in Aruna change; stored files stay.

**Authentication**: realm bearer token with WRITE on the group's admin path and the rule's bucket,
including request policies. Tokens with path restrictions are refused.

**Behavior**
- A key is selected when it lies directly in `from` (or below it with `subfolders`) and its file
  name matches `pattern`. Only keys the caller may read are counted, listed or named by `next`.
- Conflicts: the new key exists or another selected key gets it, the new key matches the pattern
  as well, the caller may not write the key or its new key, an open multipart upload, or versions
  that cannot move (encrypted, referenced, unfinished). `blocked` names a reason for the whole
  bucket, such as a running import.
- Pages are in key order and stay stable while nothing changes. The move itself checks every key
  again; opening the preview is not needed to start it.
- A key already below the target's own folder, the fixed part of `target` before its first
  placeholder, is not selected when that folder lies outside `from`; so a mapping's Apply moves
  each key once.

**Limits**
- 1 to 1000 entries per page. Totals count at most 10,000 keys; `complete` is then false and
  `total` is a lower bound. A page passes over at most 10,000 keys, also with a filter, and then
  names `next`, even with fewer entries."#,
    params(
        ("group_id" = String, Path, description = "Group that owns the rule"),
        ("import_id" = String, Path, description = "Rule whose bucket is organized")
    ),
    request_body(content = MovePreviewRequest, example = json!({
        "pattern": "*.faa.gz", "target": "proteins/{name}", "limit": 100
    })),
    responses(
        (status = 200, description = "One page of the move", body = MovePreview, example = json!({
            "total": 2, "complete": true,
            "items": [
                {"from": "a.faa.gz", "to": "proteins/a.faa.gz"},
                {"from": "b.faa.gz", "to": "proteins/b.faa.gz", "conflict": "the key `proteins/b.faa.gz` already exists"}
            ]
        })),
        (status = 400, description = "Invalid pattern, target, folder, limit or cursor", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No WRITE on the group admin path or the bucket, or a restricted token", body = ErrorResponse),
        (status = 404, description = "No such rule in this group on this node", body = ErrorResponse),
        (status = 409, description = "Too many keys in a row are hidden from the caller", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn preview_move(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((group_id, import_id)): Path<(String, String)>,
    Json(request): Json<MovePreviewRequest>,
) -> ServerResult<Json<MovePreview>> {
    let (rule, auth, access) = caller(&state, auth, (&group_id, &import_id)).await?;
    let plan = plan(&rule, request.request)?;
    let (after, limit) = page_of(&request.page)?;
    let scope = scope(&state, (&rule, &auth), &access);
    let filter = request.page.filter.as_deref();
    let page = remap::preview_page(&state.get_ctx(), &plan, &scope, (filter, after, limit))
        .await
        .map_err(super::rename::map_error)?;
    Ok(Json(MovePreview {
        total: page.total,
        complete: page.complete,
        items: (page.entries.into_iter())
            .map(|entry| MoveItem {
                from: entry.from,
                to: entry.to,
                conflict: entry.conflict,
            })
            .collect(),
        next: page.next.map(encode_cursor),
        blocked: page.blocked,
    }))
}

#[utoipa::path(
    post,
    path = "/data/groups/{group_id}/storage/imports/{import_id}/organize/move",
    tag = "data/storage",
    summary = "Move keys by file name",
    description = r#"Starts this node's background job that gives the keys a move by file name selects their new key. Only keys in Aruna change; stored files stay, links by path break and DRS links stay valid.

**Authentication**: as the preview. The job runs as the caller: each key moves only while the
caller may still write it and its new key, and once the token expires, is revoked or is cut off,
the job ends as aborted.

**Behavior**
- The job checks each key again; a key with a conflict keeps its name and counts as skipped,
  never overwriting another key. Keys written after the start stay.
- Each batch of keys moves in one transaction. A running import, storage move or another reason
  for the whole bucket makes the job wait and try again.
- Progress, pause, resume and abort: `GET` and `POST .../organize/job`.

**Limits**
- One job per bucket at a time; another running job answers 409 `job_running`."#,
    params(
        ("group_id" = String, Path, description = "Group that owns the rule"),
        ("import_id" = String, Path, description = "Rule whose bucket is organized")
    ),
    request_body(content = MoveRequest, example = json!({
        "pattern": "*.faa.gz", "target": "proteins/{name}"
    })),
    responses(
        (status = 202, description = "The job as stored; it runs in the background", body = OrganizeJob, example = json!({
            "kind": "move", "state": "running", "done": 0, "skipped": 0, "failed": 0,
            "total": 300000, "counted": true, "started_at_ms": 1790000000000_u64, "controls": true
        })),
        (status = 400, description = "Invalid pattern, target or folder", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No WRITE on the group admin path or the bucket, or a restricted token", body = ErrorResponse),
        (status = 404, description = "No such rule in this group on this node", body = ErrorResponse),
        (status = 409, description = "`job_running`: another job of the bucket runs", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn start_move(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    Path((group_id, import_id)): Path<(String, String)>,
    Json(request): Json<MoveRequest>,
) -> ServerResult<(StatusCode, Json<OrganizeJob>)> {
    let (rule, auth, access) = caller(&state, auth, (&group_id, &import_id)).await?;
    let plan = plan(&rule, request)?;
    let scope = scope(&state, (&rule, &auth), &access);
    let total = remap::count_selected(&state.get_ctx(), &plan, &scope, None)
        .await
        .map_err(ServerError::InternalError)?;
    let task = LinkedTask::Remap(plan);
    start_job(&state, (&rule, auth, bearer), (task, total)).await
}

#[utoipa::path(
    post,
    path = "/data/groups/{group_id}/storage/imports/{import_id}/organize/duplicates/preview",
    tag = "data/storage",
    summary = "Preview the removal of duplicate stored files",
    description = r#"Lists one page of the keys whose versions keep the same content in several linked files, such as `ID_name` and `name`, with the kept and the removed files.

**Authentication**: as the move preview.

**Behavior**
- The kept file is the one the current name pattern gives one of its versions, else the file
  the newest version reads. Only keys the caller may read are counted, listed or named by `next`.
- A removed file is deleted only at its exact backend version and only once no key, version,
  link or copy reads it any more; others stay. Opening the preview is not needed to start.

**Limits**
- 1 to 1000 entries per page. Totals pass over at most 10,000 version rows and check at most
  5,000 keys, also with a filter; `complete` is then false and `total` and `bytes` are lower
  bounds. A page has the same bounds and then names `next`, even with fewer entries. A key with
  more than 10,000 versions is skipped and `complete` is false."#,
    params(
        ("group_id" = String, Path, description = "Group that owns the rule"),
        ("import_id" = String, Path, description = "Rule whose bucket is organized")
    ),
    request_body(content = PageRequest, example = json!({"limit": 100})),
    responses(
        (status = 200, description = "One page of keys with duplicate files", body = DuplicatePreview, example = json!({
            "total": 1, "bytes": 1048576, "complete": true,
            "items": [{
                "key": "a.faa.gz",
                "kept": {"path": "raw/a.faa.gz~01JA7Q3K2M4N5P6Q7R8S9T0V1W", "bytes": 1048576},
                "removed": [{"path": "raw/01JA7Q3K2M4N5P6Q7R8S9T0V1W_a.faa.gz", "bytes": 1048576}]
            }]
        })),
        (status = 400, description = "Invalid limit or cursor", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No WRITE on the group admin path or the bucket, or a restricted token", body = ErrorResponse),
        (status = 404, description = "No such rule in this group on this node", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn preview_duplicates(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((group_id, import_id)): Path<(String, String)>,
    Json(page): Json<PageRequest>,
) -> ServerResult<Json<DuplicatePreview>> {
    let (rule, _, access) = caller(&state, auth, (&group_id, &import_id)).await?;
    let (after, limit) = page_of(&page)?;
    // A cursor within a key names how many of its content groups were shown.
    let after = after.map(|after| match after.split_once('\0') {
        Some((key, shown)) => (key.to_string(), shown.parse().ok()),
        None => (after, None),
    });
    let filter = page.filter.as_deref();
    let found = unify::preview_page(
        &state.get_ctx(),
        (&rule.bucket, ""),
        &access,
        (filter, after, limit),
    )
    .await
    .map_err(ServerError::InternalError)?;
    let file = |location: aruna_core::structs::storage::blob::BackendLocation| StoredFile {
        path: location.backend_path,
        bytes: location.blob_size,
    };
    Ok(Json(DuplicatePreview {
        total: found.keys,
        bytes: found.bytes,
        complete: found.complete,
        items: (found.entries.into_iter())
            .map(|entry| DuplicateItem {
                key: entry.key,
                kept: file(entry.keep),
                removed: entry.copies.into_iter().map(file).collect(),
            })
            .collect(),
        next: found.next.map(|(key, shown)| match shown {
            Some(shown) => encode_cursor(format!("{key}\0{shown}")),
            None => encode_cursor(key),
        }),
    }))
}

#[utoipa::path(
    post,
    path = "/data/groups/{group_id}/storage/imports/{import_id}/organize/duplicates",
    tag = "data/storage",
    summary = "Remove duplicate stored files",
    description = r#"Starts this node's background job that lets the versions of each key with the same content read one linked file and releases the other files.

**Authentication**: as the move. The job runs as the caller and checks WRITE on each key.

**Behavior**
- Each key changes in one transaction that still reads the same versions and kept file; a key
  that changed meanwhile counts as failed and keeps its files. Keys written after the start stay.
- A released file is deleted only at its exact backend version, after the change is on disk,
  and only once nothing reads it; on storage without exact versions only its record goes.
- Progress, pause, resume and abort: `GET` and `POST .../organize/job`.

**Limits**
- One job per bucket at a time; another running job answers 409 `job_running`."#,
    params(
        ("group_id" = String, Path, description = "Group that owns the rule"),
        ("import_id" = String, Path, description = "Rule whose bucket is organized")
    ),
    responses(
        (status = 202, description = "The job as stored; it runs in the background", body = OrganizeJob, example = json!({
            "kind": "duplicates", "state": "running", "done": 0, "skipped": 0, "failed": 0,
            "total": 1200, "counted": true, "started_at_ms": 1790000000000_u64, "controls": true
        })),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No WRITE on the group admin path or the bucket, or a restricted token", body = ErrorResponse),
        (status = 404, description = "No such rule in this group on this node", body = ErrorResponse),
        (status = 409, description = "`job_running`: another job of the bucket runs", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn start_duplicates(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    Path((group_id, import_id)): Path<(String, String)>,
) -> ServerResult<(StatusCode, Json<OrganizeJob>)> {
    let (rule, auth, access) = caller(&state, auth, (&group_id, &import_id)).await?;
    let found = unify::preview_page(
        &state.get_ctx(),
        (&rule.bucket, ""),
        &access,
        (None, None, 1),
    )
    .await
    .map_err(ServerError::InternalError)?;
    let task = LinkedTask::Unify {
        prefix: String::new(),
    };
    let total = (found.keys, found.complete);
    start_job(&state, (&rule, auth, bearer), (task, total)).await
}

#[utoipa::path(
    get,
    path = "/data/groups/{group_id}/storage/imports/{import_id}/organize/job",
    tag = "data/storage",
    summary = "Show the long job of the rule's bucket",
    description = r#"Shows the running job of the rule's bucket on this node, else the one started last: a move of keys, a duplicate cleanup or a rename of linked files. `null` before any.

**Authentication**: as the previews.

**Behavior**
- A rename shows the files its start planned to rename as `total`, with `counted` true. A rename
  whose start stored no total for its move shows `total` 0 and `counted` false."#,
    params(
        ("group_id" = String, Path, description = "Group that owns the rule"),
        ("import_id" = String, Path, description = "Rule whose bucket is organized")
    ),
    responses(
        (status = 200, description = "The job, or null", body = Option<OrganizeJob>, example = json!({
            "kind": "move", "state": "running", "done": 120, "skipped": 3, "failed": 0,
            "total": 300000, "counted": true, "started_at_ms": 1790000000000_u64, "controls": true
        })),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No WRITE on the group admin path or the bucket, or a restricted token", body = ErrorResponse),
        (status = 404, description = "No such rule in this group on this node", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_job(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((group_id, import_id)): Path<(String, String)>,
) -> ServerResult<Json<Option<OrganizeJob>>> {
    let (rule, ..) = caller(&state, auth, (&group_id, &import_id)).await?;
    let context = state.get_ctx();
    let job = read_bucket_job(&context, &rule.bucket)
        .await
        .map_err(ServerError::InternalError)?;
    match job {
        Some(job) => Ok(Json(Some(shown(&context, &rule.bucket, job).await?))),
        None => Ok(Json(None)),
    }
}

/// The job as the routes show it, with the files a rename planned to rename where its start
/// stored them for its move.
async fn shown(
    context: &aruna_operations::driver::DriverContext,
    bucket: &str,
    job: BucketJob,
) -> ServerResult<OrganizeJob> {
    let total = match &job {
        BucketJob::Rename(record, _) => {
            let total = rename_total(context, bucket, record.started_at_ms).await;
            total.map_err(ServerError::InternalError)?
        }
        _ => None,
    };
    let mut shown = OrganizeJob::from(job);
    if let Some(total) = total {
        (shown.total, shown.counted) = (total, true);
    }
    Ok(shown)
}

#[utoipa::path(
    post,
    path = "/data/groups/{group_id}/storage/imports/{import_id}/organize/job/{action}",
    tag = "data/storage",
    summary = "Pause, resume or abort the bucket job",
    description = r#"Pauses, resumes or aborts the running job of the rule's bucket: a move of keys, a duplicate cleanup or a rename of linked files.

**Authentication**: as starting the job.

**Behavior**
- `pause` keeps the job's position; `resume` continues from it.
- `abort` ends the job for good, also a paused one. Each key or file it finished stays finished,
  since each change is one transaction; the step in flight completes or leaves nothing behind,
  like after a restart. The job then shows `aborted` with its counts, and a new job may start.
- The answer shows a rename's `total` as `GET` on the job does."#,
    params(
        ("group_id" = String, Path, description = "Group that owns the rule"),
        ("import_id" = String, Path, description = "Rule whose bucket is organized"),
        ("action" = String, Path, description = "`pause`, `resume` or `abort`")
    ),
    responses(
        (status = 200, description = "The job after the change", body = OrganizeJob, example = json!({
            "kind": "move", "state": "paused", "done": 120, "skipped": 0, "failed": 0,
            "total": 300000, "counted": true, "started_at_ms": 1790000000000_u64, "controls": true
        })),
        (status = 400, description = "Unknown action", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No WRITE on the group admin path or the bucket, or a restricted token", body = ErrorResponse),
        (status = 404, description = "No such rule in this group on this node", body = ErrorResponse),
        (status = 409, description = "`no_job`: no running or paused job, or the action does not fit its state", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn control_job(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((group_id, import_id, action)): Path<(String, String, String)>,
) -> ServerResult<Json<OrganizeJob>> {
    let (rule, ..) = caller(&state, auth, (&group_id, &import_id)).await?;
    let action = match action.as_str() {
        "pause" => LinkedAction::Pause,
        "resume" => LinkedAction::Resume,
        "abort" => LinkedAction::Abort,
        _ => return Err(ServerError::BadRequestMessage("unknown action".to_string())),
    };
    let operation = BucketJobOperation::new(rule.bucket.clone(), rule.group_id, action, now_ms());
    let context = state.get_ctx();
    let job = drive(operation, &context).await.map_err(job_error)?;
    Ok(Json(shown(&context, &rule.bucket, job).await?))
}

#[utoipa::path(
    get,
    path = "/data/groups/{group_id}/storage/imports/{import_id}/organize/status",
    tag = "data/storage",
    summary = "Show what the link holds",
    description = r#"Shows the files the last sync of the rule could not take, such as a file two links claim, without reading the storage again.

A file is shown only when the caller may read every key the rule may give it: its recorded key, the keys of pending writes to it, its key under the rule's folders and its discovery key.

**Authentication**: as the previews.

**Limits**
- Byte counts are not kept yet. The last sync keeps at most 100 failed files."#,
    params(
        ("group_id" = String, Path, description = "Group that owns the rule"),
        ("import_id" = String, Path, description = "Rule to show")
    ),
    responses(
        (status = 200, description = "The link's last sync", body = LinkStatus, example = json!({
            "conflicts": [{"path": "raw/a.faa.gz", "reason": "another link records the file"}]
        })),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No WRITE on the group admin path or the bucket, or a restricted token", body = ErrorResponse),
        (status = 404, description = "No such rule in this group on this node", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn link_status(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((group_id, import_id)): Path<(String, String)>,
) -> ServerResult<Json<LinkStatus>> {
    let (rule, _, access) = caller(&state, auth, (&group_id, &import_id)).await?;
    let context = state.get_ctx();
    let run = last_run(&context, &rule).await.map_err(map_error)?;
    let Some(run) = run else {
        return Ok(Json(LinkStatus {
            files: None,
            bytes: None,
            conflicts: Vec::new(),
        }));
    };
    let rules = aruna_operations::groups::imports::discovery::read_discovery(
        &context,
        rule.group_id,
        rule.import_id,
    )
    .await
    .map_err(map_error)?;
    let earlier = aruna_operations::groups::imports::read_earlier(&context, rule.import_id)
        .await
        .map_err(map_error)?;
    let mut conflicts = Vec::new();
    for failure in run.checkpoint.errors {
        let keys = file_keys(&context, (&rule, &earlier, &rules), &failure.key).await?;
        let read = |key: &String| access.allows(Some(key), &Permission::READ);
        if !keys.is_empty() && keys.iter().all(read) {
            conflicts.push(LinkConflict {
                path: failure.key,
                reason: failure.error,
            });
        }
    }
    Ok(Json(LinkStatus {
        files: None,
        bytes: None,
        conflicts,
    }))
}

/// Intents of one path a link status reads at most; more hide the file.
const INTENT_PAGE: usize = 64;

/// Every key the rule may give the file at `path`: the key its row records, the keys of its
/// pending writes, the key its folders give it, and the key discovery gives it. Empty when
/// none is known.
async fn file_keys(
    context: &aruna_operations::driver::DriverContext,
    (rule, earlier, rules): (
        &BackendImport,
        &[String],
        &[aruna_core::structs::storage::discovery::DiscoveryRule],
    ),
    path: &str,
) -> ServerResult<Vec<String>> {
    use aruna_core::structs::storage::backend_import::{ImportedObject, object_key};
    use aruna_core::structs::storage::discovery::file_identity;
    let read = aruna_core::effects::StorageEffect::Read {
        key_space: aruna_core::keyspaces::IMPORTED_OBJECT_KEYSPACE.to_string(),
        key: object_key(rule.import_id, path),
        txn_id: None,
    };
    let row = match context.storage_handle.send_storage_effect(read).await {
        aruna_core::events::Event::Storage(aruna_core::events::StorageEvent::ReadResult {
            value,
            ..
        }) => value,
        other => return Err(ServerError::InternalError(format!("{other:?}"))),
    };
    let row = row.as_deref().map(ImportedObject::from_bytes).transpose();
    let row = row.map_err(|error| ServerError::InternalError(error.to_string()))?;
    let mut keys: Vec<String> = row.and_then(|row| row.key).into_iter().collect();
    keys.extend(rule.aruna_key(path));
    // A pending write to the path names its key too.
    let intents = aruna_operations::jobs::store::iter_prefix_page(
        &context.storage_handle,
        aruna_core::keyspaces::IMPORT_INTENT_KEYSPACE,
        Some(aruna_core::structs::storage::backend_import::intent_prefix(
            rule.import_id,
            path,
        )),
        None,
        INTENT_PAGE,
        None,
    );
    let (intents, more) = intents.await.map_err(ServerError::InternalError)?;
    if more.is_some() {
        return Ok(Vec::new());
    }
    for (_, value) in intents {
        let intent = aruna_core::structs::storage::backend_import::WriteIntent::from_bytes(&value);
        let intent = intent.map_err(|error| ServerError::InternalError(error.to_string()))?;
        keys.push(intent.key);
    }
    // A pending write to the path names its key too.
    let intents = aruna_operations::jobs::store::iter_prefix_page(
        &context.storage_handle,
        aruna_core::keyspaces::IMPORT_INTENT_KEYSPACE,
        Some(aruna_core::structs::storage::backend_import::intent_prefix(
            rule.import_id,
            path,
        )),
        None,
        INTENT_PAGE,
        None,
    );
    let (intents, more) = intents.await.map_err(ServerError::InternalError)?;
    if more.is_some() {
        return Ok(Vec::new());
    }
    for (_, value) in intents {
        let intent = aruna_core::structs::storage::backend_import::WriteIntent::from_bytes(&value);
        let intent = intent.map_err(|error| ServerError::InternalError(error.to_string()))?;
        keys.push(intent.key);
    }
    match file_identity(rule, earlier, rules, path) {
        Some(Ok(identity)) => keys.push(identity.key),
        // A file of several patterns or keys names nothing a caller could be shown safely.
        Some(Err(_)) => return Ok(Vec::new()),
        None => {}
    }
    Ok(keys)
}

#[cfg(test)]
#[path = "organize_tests.rs"]
mod tests;
