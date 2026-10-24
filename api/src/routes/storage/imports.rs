//! Serves the group routes that import the objects of a storage backend in place.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::{ValidatedBearer, ensure_permission, parse_group_id, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::storage::compression::MigrationProgress;
use crate::routes::storage::moves::{BucketMoveStarted, move_refusal};
use crate::routes::storage::routing::ensure_group_admin;
use crate::server::state::ServerState;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::backend_import::{BackendImport, PLAIN_PATTERN};
use aruna_core::structs::storage::blob::bucket_permission_path;
use aruna_core::time::unix_timestamp_millis;
use aruna_operations::auth::bearer_token::token_grant;
use aruna_operations::driver::{drive, now_ms};
use aruna_operations::groups::imports::change::{ChangeImportOperation, ImportChange};
use aruna_operations::groups::imports::create::{CreateImportInput, CreateImportOperation};
use aruna_operations::groups::imports::discovery::{authorized_renames, rename_guards};
use aruna_operations::groups::imports::remove::RemoveImportOperation;
use aruna_operations::groups::imports::schedule::{next_due, start_run, wake_schedule};
use aruna_operations::groups::imports::{ImportRuleError, last_run, list_imports, read_import};
use aruna_operations::s3::bucket::compression::MigrationStatusOperation;
use aruna_operations::s3::bucket::storage_move::StartMoveOperation;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

#[derive(OpenApi)]
#[openapi()]
pub struct BackendImportsDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(BackendImportsDoc::openapi())
        .routes(routes!(create_import, list_group_imports))
        .routes(routes!(get_import, update_import, delete_import))
        .routes(routes!(run_import))
        .routes(routes!(get_renames, start_renames))
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct CreateImportRequest {
    /// A storage backend the group registered on this node.
    pub backend_id: String,
    /// Folder in the backend to import: empty, or a relative path ending with `/`.
    #[serde(default)]
    pub backend_prefix: String,
    pub bucket: String,
    /// Folder in the bucket whose uploads go to the linked folder: empty, or a path ending with
    /// `/`. Files a run finds go to the bucket root or their discovery folder instead.
    #[serde(default)]
    pub key_prefix: String,
    /// Five-field cron expression in UTC; omitted runs only on request.
    #[serde(default)]
    pub schedule: Option<String>,
    /// Another enabled backend of the group for new uploads; omitted uses `backend_id`.
    #[serde(default)]
    pub write_backend_id: Option<String>,
    /// Folder for new uploads: empty, or a relative path ending with `/`; omitted uses
    /// `backend_prefix`.
    #[serde(default)]
    pub write_prefix: Option<String>,
    /// Path of a new upload below the write folder with `{dir}`, `{name}` and `{id}`;
    /// omitted uses `{dir}{name}`.
    #[serde(default)]
    pub write_pattern: Option<String>,
    /// Also moves the bucket's stored files under `key_prefix` to the write folder.
    #[serde(default)]
    pub move_existing: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct UpdateImportRequest {
    /// Five-field cron expression in UTC; `null` or omitted stops scheduled runs.
    #[serde(default)]
    pub schedule: Option<String>,
    /// Path of later uploads below the write folder, as in the create request; omitted keeps
    /// the current pattern.
    #[serde(default)]
    pub write_pattern: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct ImportFailureResponse {
    /// Key in the backend.
    pub key: String,
    pub error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct ImportRunResponse {
    pub job_id: String,
    /// Job state such as `queued`, `running`, `succeeded` or `failed`.
    pub state: String,
    pub started_at: String,
    pub imported: u64,
    pub skipped: u64,
    pub failed: u64,
    /// The first failures of the run; `failed` counts all of them.
    pub errors: Vec<ImportFailureResponse>,
    /// Why a failed or indeterminate run stopped as a whole, as a short fixed reason such as
    /// `No access to the bucket`; never the provider's text. Omitted otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct ImportResponse {
    pub import_id: String,
    pub group_id: String,
    pub backend_id: String,
    pub backend_prefix: String,
    pub bucket: String,
    pub key_prefix: String,
    /// Omitted when new uploads go to `backend_id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_backend_id: Option<String>,
    /// Omitted when new uploads go to `backend_prefix`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_prefix: Option<String>,
    pub write_pattern: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule: Option<String>,
    /// When the schedule starts the next run; omitted without a schedule.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_run_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    /// Omitted before the first run and after the run's job record was pruned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_run: Option<ImportRunResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct ListImportsResponse {
    pub imports: Vec<ImportResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct RunImportResponse {
    pub job_id: String,
}

fn rfc3339(at_ms: u64) -> String {
    let at = UNIX_EPOCH + Duration::from_millis(at_ms);
    chrono::DateTime::<chrono::Utc>::from(at).to_rfc3339()
}

fn millis(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_millis() as u64)
}

pub(super) fn map_error(error: ImportRuleError) -> ServerError {
    match error {
        ImportRuleError::Invalid(message) => ServerError::BadRequestMessage(message),
        ImportRuleError::Conflict(message) => ServerError::Conflict(message),
        ImportRuleError::GroupWrite(error) => ServerError::Conflict(error.to_string()),
        ImportRuleError::NotFound => ServerError::NotFound,
        ImportRuleError::Forbidden => ServerError::Forbidden,
        other => ServerError::InternalError(other.to_string()),
    }
}

pub(super) async fn admin_of_group(
    state: &ServerState,
    auth: Option<AuthContext>,
    group_id: &str,
) -> ServerResult<(Ulid, AuthContext)> {
    let auth = require_realm_auth(state, auth)?;
    let group_id = parse_group_id(group_id)?;
    ensure_group_admin(state, &auth, group_id).await?;
    Ok((group_id, auth))
}

pub(super) async fn existing_rule(
    state: &ServerState,
    group_id: Ulid,
    import_id: &str,
) -> ServerResult<BackendImport> {
    let import_id = Ulid::from_str(import_id).map_err(|_| ServerError::BadRequest)?;
    read_import(&state.get_ctx(), group_id, import_id)
        .await
        .map_err(map_error)?
        .ok_or(ServerError::NotFound)
}

async fn describe(state: &ServerState, rule: BackendImport) -> ServerResult<ImportResponse> {
    let run = last_run(&state.get_ctx(), &rule).await.map_err(map_error)?;
    let since = rule.last_started_ms.unwrap_or(millis(rule.created_at));
    let now = unix_timestamp_millis();
    let next_run_at = rule
        .schedule
        .as_deref()
        .and_then(|schedule| next_due(schedule, since))
        .map(|due| rfc3339(due.max(now)));
    Ok(ImportResponse {
        import_id: rule.import_id.to_string(),
        group_id: rule.group_id.to_string(),
        backend_id: rule.backend_id.to_string(),
        backend_prefix: rule.backend_prefix,
        bucket: rule.bucket,
        key_prefix: rule.key_prefix,
        write_backend_id: rule.write_backend_id.map(|id| id.to_string()),
        write_prefix: rule.write_prefix,
        write_pattern: rule.write_pattern,
        schedule: rule.schedule,
        next_run_at,
        created_at: rfc3339(millis(rule.created_at)),
        updated_at: rfc3339(millis(rule.updated_at)),
        last_run: run.map(|run| ImportRunResponse {
            job_id: run.job_id.to_string(),
            state: run.state.name().to_string(),
            started_at: rfc3339(run.started_ms),
            imported: run.checkpoint.imported,
            skipped: run.checkpoint.skipped,
            failed: run.checkpoint.failed,
            errors: run
                .checkpoint
                .errors
                .into_iter()
                .map(|failure| ImportFailureResponse {
                    key: failure.key,
                    error: failure.error,
                })
                .collect(),
            error: run.error,
        }),
    })
}

#[utoipa::path(
    post,
    path = "/data/groups/{group_id}/storage/imports",
    tag = "data/storage",
    summary = "Import a storage backend folder in place",
    description = r#"Creates a rule that registers the objects of a group storage backend folder in a bucket of this node without copying them.

**Authentication**: realm bearer token with WRITE on the group's admin path and WRITE on the bucket.

**Behavior**
- The rule also adds a bucket routing rule that sends writes under `key_prefix` to the write
  backend (`write_backend_id`, else `backend_id`). An existing rule for the same folder must
  already name it.
- `key_prefix` is where uploads go from. A backend file found by a run, without a recorded
  mapping, goes to the folder of its discovery rule, else to its path below `backend_prefix`
  at the bucket root; it never gets `key_prefix`.
- A new upload to `key_prefix` + `d/n` is stored at the write folder (`write_prefix`, else
  `backend_prefix`) + `write_pattern`, with `{dir}` = `d/`, `{name}` = `n` and `{id}` = the
  version id, so other tools see it. Uploads under the rule's own import folder are not imported
  again; later changes to such a file are, found by its ETag or MD5 where the backend lists one.
- A pattern without `{id}` keeps the file name. No upload waits for another. A new file is
  created only if the name is still free. If the file exists and the backend keeps versions, the
  upload replaces it and older Aruna versions still read their own backend version. Without
  backend versions the upload is stored next to it as `<name>~<version id>`, and the bucket key
  stays the same. If another upload creates the same new file first, a single upload gets
  `503 ServiceUnavailable` and its retry is stored next to the file; a multipart upload is
  stored next to it directly. Backends without conditional writes (B2) always store the upload
  at the tagged name `<name>~<version id>`.
- A bucket that turns on compression or encryption later stores new uploads in Aruna's own layout.
- Nothing runs on creation. Start a run on request or give a `schedule`.
- With `move_existing` the bucket's files already stored under `key_prefix` move in the
  background to the paths new uploads get, as `POST /data/buckets/{bucket}/storage/move` with
  `prefix` = `key_prefix` does. Files already in a linked folder stay where they are. While a
  move of the bucket with another prefix is unfinished, the rule is not created (409).
- Aruna never deletes imported bytes from the backend. Deleting an object in Aruna only removes
  Aruna's record of it.
- The rule is node-local.
- On a backend without a fixed bucket the first folder of `backend_prefix` and of the write folder
  names a bucket. Each such bucket is found or created and tested with a write and a delete
  before the rule is stored; the backend then lists it as `bucket:<name>` in its `public_config`.

**Limits**
- The bucket must belong to the group, without compression and without encryption.
- All prefixes are empty or relative folders ending with `/`; `backend_prefix` and the write
  folder may not lie inside the folders where Aruna stores its own objects on that backend.
- `write_pattern` must contain `{name}` and give a relative path.
- Two rules may not import into overlapping folders of one bucket. Rules may share a backend
  folder only when each writes inside the folder it reads. In a shared folder a recorded file
  stays with its rule, and a new file is imported only by the one rule whose name pattern
  matches it; otherwise each run reports it and imports nothing.
- `schedule` is a five-field cron expression evaluated in UTC.
- On a backend without a fixed bucket both folders start with a valid bucket name other than the
  default rule's bucket.
- A folder must leave room for a file name: keys on group backends are at most 1024 bytes."#,
    params(("group_id" = String, Path, description = "Group that owns the backend and the bucket, as a 26-character ULID")),
    request_body(
        content = CreateImportRequest,
        description = "The backend folder, the bucket folder and an optional schedule",
        example = json!({
            "backend_id": "01JBCKND0123456789ABCDEFGH",
            "backend_prefix": "instrument/raw/",
            "bucket": "research-raw",
            "key_prefix": "raw/",
            "write_pattern": "{dir}{name}~{id}",
            "schedule": "0 3 * * *"
        })
    ),
    responses(
        (
            status = 201,
            description = "The stored rule",
            body = ImportResponse,
            example = json!({
                "import_id": "01JMPRT0123456789ABCDEFGHJ",
                "group_id": "01JABCDEF0123456789ABCDEFG",
                "backend_id": "01JBCKND0123456789ABCDEFGH",
                "backend_prefix": "instrument/raw/",
                "bucket": "research-raw",
                "key_prefix": "raw/",
                "write_pattern": "{dir}{name}~{id}",
                "schedule": "0 3 * * *",
                "next_run_at": "2026-10-15T03:00:00+00:00",
                "created_at": "2026-10-14T09:12:00+00:00",
                "updated_at": "2026-10-14T09:12:00+00:00"
            })
        ),
        (status = 400, description = "Invalid ids, prefixes, schedule, bucket or backend, or a backend bucket that cannot be found, created or written", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No WRITE on the group admin path or on the bucket", body = ErrorResponse),
        (status = 409, description = "A routing rule for the folder names another target, another rule imports into an overlapping folder or shares a backend folder it does not both read and write, a folder lies in the default rule's bucket, or the group is being deleted", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn create_import(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(group_id): Path<String>,
    Json(request): Json<CreateImportRequest>,
) -> ServerResult<(StatusCode, Json<ImportResponse>)> {
    let (group_id, auth) = admin_of_group(&state, auth, &group_id).await?;
    let backend_id = Ulid::from_str(&request.backend_id).map_err(|_| ServerError::BadRequest)?;
    let write_backend_id = match request.write_backend_id.as_deref() {
        None | Some("") => None,
        Some(id) => Some(Ulid::from_str(id).map_err(|_| ServerError::BadRequest)?),
    };
    let bucket_path = bucket_permission_path(
        state.get_realm_id(),
        group_id,
        state.get_node_id(),
        &request.bucket,
    );
    ensure_permission(&state, &auth, bucket_path, Permission::WRITE).await?;
    let scheduled = request.schedule.is_some();
    let rule = drive(
        CreateImportOperation::new(CreateImportInput {
            group_id,
            created_by: auth.user_id,
            backend_id,
            backend_prefix: request.backend_prefix,
            bucket: request.bucket,
            key_prefix: request.key_prefix,
            schedule: request.schedule,
            write_backend_id,
            write_prefix: request.write_prefix,
            write_pattern: request
                .write_pattern
                .unwrap_or_else(|| PLAIN_PATTERN.to_string()),
            now: SystemTime::now(),
            move_existing: request.move_existing,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_error)?;
    if scheduled {
        wake_schedule(&state.get_ctx()).await;
    }
    Ok((StatusCode::CREATED, Json(describe(&state, rule).await?)))
}

#[utoipa::path(
    get,
    path = "/data/groups/{group_id}/storage/imports",
    tag = "data/storage",
    summary = "List a group's backend imports",
    description = r#"Lists every backend import rule the group has on this node, with its last run.

**Authentication**: realm bearer token with WRITE on the group's admin path."#,
    params(("group_id" = String, Path, description = "Group whose rules are listed, as a 26-character ULID")),
    responses(
        (
            status = 200,
            description = "Every rule of the group on this node, ordered by group and rule id",
            body = ListImportsResponse,
            example = json!({
                "imports": [{
                    "import_id": "01JMPRT0123456789ABCDEFGHJ",
                    "group_id": "01JABCDEF0123456789ABCDEFG",
                    "backend_id": "01JBCKND0123456789ABCDEFGH",
                    "backend_prefix": "instrument/raw/",
                    "bucket": "research-raw",
                    "key_prefix": "raw/",
                    "write_pattern": "{dir}{name}~{id}",
                    "created_at": "2026-10-14T09:12:00+00:00",
                    "updated_at": "2026-10-14T09:12:00+00:00",
                    "last_run": {
                        "job_id": "01JJ0B0123456789ABCDEFGHJK",
                        "state": "succeeded",
                        "started_at": "2026-10-14T09:13:00+00:00",
                        "imported": 1240,
                        "skipped": 0,
                        "failed": 1,
                        "errors": [{"key": "instrument/raw/broken.bin", "error": "the object changed while it was read"}]
                    }
                }]
            })
        ),
        (status = 400, description = "The group id is not a ULID", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No WRITE on the group admin path", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_group_imports(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(group_id): Path<String>,
) -> ServerResult<Json<ListImportsResponse>> {
    let (group_id, _) = admin_of_group(&state, auth, &group_id).await?;
    let rules = list_imports(&state.get_ctx(), group_id)
        .await
        .map_err(map_error)?;
    let mut imports = Vec::with_capacity(rules.len());
    for rule in rules {
        imports.push(describe(&state, rule).await?);
    }
    Ok(Json(ListImportsResponse { imports }))
}

#[utoipa::path(
    get,
    path = "/data/groups/{group_id}/storage/imports/{import_id}",
    tag = "data/storage",
    summary = "Read one backend import",
    description = r#"Returns one backend import rule with the counts and first errors of its last run.

**Authentication**: realm bearer token with WRITE on the group's admin path."#,
    params(
        ("group_id" = String, Path, description = "Group that owns the rule, as a 26-character ULID"),
        ("import_id" = String, Path, description = "Rule to read, as a 26-character ULID")
    ),
    responses(
        (
            status = 200,
            description = "The stored rule and its last run",
            body = ImportResponse,
            example = json!({
                "import_id": "01JMPRT0123456789ABCDEFGHJ",
                "group_id": "01JABCDEF0123456789ABCDEFG",
                "backend_id": "01JBCKND0123456789ABCDEFGH",
                "backend_prefix": "",
                "bucket": "research-raw",
                "key_prefix": "",
                "write_backend_id": "01JBCKND0123456789ABCDEFGJ",
                "write_prefix": "uploads/",
                "write_pattern": "{dir}{name}",
                "created_at": "2026-10-14T09:12:00+00:00",
                "updated_at": "2026-10-14T09:12:00+00:00",
                "last_run": {
                    "job_id": "01JJ0B0123456789ABCDEFGHJK",
                    "state": "running",
                    "started_at": "2026-10-14T09:13:00+00:00",
                    "imported": 52,
                    "skipped": 10,
                    "failed": 0,
                    "errors": []
                }
            })
        ),
        (status = 400, description = "The group id or the rule id is not a ULID", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No WRITE on the group admin path", body = ErrorResponse),
        (status = 404, description = "No such rule in this group on this node", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_import(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((group_id, import_id)): Path<(String, String)>,
) -> ServerResult<Json<ImportResponse>> {
    let (group_id, _) = admin_of_group(&state, auth, &group_id).await?;
    let rule = existing_rule(&state, group_id, &import_id).await?;
    Ok(Json(describe(&state, rule).await?))
}

#[utoipa::path(
    put,
    path = "/data/groups/{group_id}/storage/imports/{import_id}",
    tag = "data/storage",
    summary = "Change a backend import's schedule or write pattern",
    description = r#"Replaces the schedule of one backend import rule and, when given, its write pattern; the rule id, the folders and the bucket stay as they are.

**Authentication**: realm bearer token with WRITE on the group's admin path.

**Behavior**
- `null` or an omitted `schedule` stops scheduled runs; runs on request keep working.
- A running import is not affected.
- A new `write_pattern` names only later uploads. Files already in the backend keep their names,
  and every stored version still reads its own file.
- A new pattern is refused with 409 while a run of the rule or a move of its bucket has not
  finished.

**Limits**
- `write_pattern` must contain `{name}` and give a relative path.
- A rule that links a whole top folder of its storage to a whole bucket with the pattern
  `{dir}{name}` is a 1:1 link: its bucket cannot be compressed or encrypted, takes no
  replicated copies and ends the link when it is deleted. A switch to `{dir}{name}` is refused
  with 409 while the bucket is compressed or encrypted.
- A 1:1 link keeps `{dir}{name}` (409) while its storage has no shared bucket, since a
  compressed or encrypted bucket would then have nowhere to write.
- A `<folder>/{name}` discovery rule needs a pattern with `{dir}` (400)."#,
    params(
        ("group_id" = String, Path, description = "Group that owns the rule, as a 26-character ULID"),
        ("import_id" = String, Path, description = "Rule to change, as a 26-character ULID")
    ),
    request_body(
        content = UpdateImportRequest,
        description = "The new schedule, or `null` to stop scheduled runs, and an optional pattern",
        example = json!({"schedule": "*/30 * * * *", "write_pattern": "{dir}{name}~{id}"})
    ),
    responses(
        (
            status = 200,
            description = "The stored rule",
            body = ImportResponse,
            example = json!({
                "import_id": "01JMPRT0123456789ABCDEFGHJ",
                "group_id": "01JABCDEF0123456789ABCDEFG",
                "backend_id": "01JBCKND0123456789ABCDEFGH",
                "backend_prefix": "instrument/raw/",
                "bucket": "research-raw",
                "key_prefix": "raw/",
                "write_pattern": "{dir}{name}~{id}",
                "schedule": "*/30 * * * *",
                "next_run_at": "2026-10-14T10:00:00+00:00",
                "created_at": "2026-10-14T09:12:00+00:00",
                "updated_at": "2026-10-14T09:40:00+00:00"
            })
        ),
        (status = 400, description = "Invalid ids, schedule or pattern", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No WRITE on the group admin path", body = ErrorResponse),
        (status = 404, description = "No such rule in this group on this node", body = ErrorResponse),
        (status = 409, description = "The group is being deleted, a run or move has not finished, or a compressed or encrypted bucket would become a 1:1 link", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn update_import(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((group_id, import_id)): Path<(String, String)>,
    Json(request): Json<UpdateImportRequest>,
) -> ServerResult<Json<ImportResponse>> {
    let (group_id, _) = admin_of_group(&state, auth, &group_id).await?;
    let import_id = Ulid::from_str(&import_id).map_err(|_| ServerError::BadRequest)?;
    let scheduled = request.schedule.is_some();
    let change = ImportChange {
        schedule: request.schedule,
        write_pattern: request.write_pattern,
    };
    let operation = ChangeImportOperation::new(group_id, import_id, change, SystemTime::now());
    let rule = drive(operation, &state.get_ctx())
        .await
        .map_err(map_error)?;
    if scheduled {
        wake_schedule(&state.get_ctx()).await;
    }
    Ok(Json(describe(&state, rule).await?))
}

#[utoipa::path(
    delete,
    path = "/data/groups/{group_id}/storage/imports/{import_id}",
    tag = "data/storage",
    summary = "Remove a backend import",
    description = r#"Removes one backend import rule; imported objects stay readable and their bytes stay in the backend.

**Authentication**: realm bearer token with WRITE on the group's admin path.

**Behavior**
- A link ends only with its bucket: while the rule's bucket holds any object version, the
  removal is refused with 409. Deleting the bucket ends a 1:1 link; empty the bucket for others.
- A 1:1 link whose storage has no shared bucket is the bucket's only storage, so its removal is
  refused with 409 while the bucket exists.
- A running import stops before its next page.
- The bucket routing rule the import added stays, so later writes keep going to the backend.
- Objects added to the backend afterwards are no longer imported."#,
    params(
        ("group_id" = String, Path, description = "Group that owns the rule, as a 26-character ULID"),
        ("import_id" = String, Path, description = "Rule to remove, as a 26-character ULID")
    ),
    responses(
        (status = 204, description = "The rule was removed"),
        (status = 400, description = "The group id or the rule id is not a ULID", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No WRITE on the group admin path", body = ErrorResponse),
        (status = 404, description = "No such rule in this group on this node", body = ErrorResponse),
        (status = 409, description = "The bucket still holds files or has no other storage, or the group is being deleted", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn delete_import(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((group_id, import_id)): Path<(String, String)>,
) -> ServerResult<StatusCode> {
    let (group_id, _) = admin_of_group(&state, auth, &group_id).await?;
    let import_id = Ulid::from_str(&import_id).map_err(|_| ServerError::BadRequest)?;
    let removed = drive(
        RemoveImportOperation::new(group_id, import_id),
        &state.get_ctx(),
    )
    .await;
    if removed.is_err() {
        // A removal that stopped after deleting the rule is finished by the schedule.
        wake_schedule(&state.get_ctx()).await;
    }
    removed.map_err(map_error)?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    post,
    path = "/data/groups/{group_id}/storage/imports/{import_id}/run",
    tag = "data/storage",
    summary = "Start a backend import run",
    description = r#"Starts one run of a backend import rule as the caller; the run is a job on this node.

**Authentication**: realm bearer token with WRITE on the group's admin path.

**Behavior**
- The run lists the backend folder and registers each object without copying it. Hashes come
  from reading each new or changed object once.
- Objects registered before are skipped by size and ETag (or modification time) without reading
  them. Aruna's own uploads compare with the ETag the backend answered, else a single-part ETag
  with their MD5. A same-size change goes unnoticed only where neither exists, such as an older
  multipart upload, and on backends that list neither ETag nor time.
- A changed object gets a new version unless the key was written or deleted in Aruna since;
  Aruna's side always wins.
- A new object goes where the rule's discovery rules put it, else to its path below
  `backend_prefix` at the bucket root, never below `key_prefix`.
  Its key is recorded, so later rule changes never move it. A new object whose key already
  holds another object, or that matches several patterns, fails and is not imported.
- Imported bytes count toward the group quota like uploaded ones.
- Progress, counts and the first errors show in `last_run` of the rule."#,
    params(
        ("group_id" = String, Path, description = "Group that owns the rule, as a 26-character ULID"),
        ("import_id" = String, Path, description = "Rule to run, as a 26-character ULID")
    ),
    responses(
        (
            status = 202,
            description = "The run was queued",
            body = RunImportResponse,
            example = json!({"job_id": "01JJ0B0123456789ABCDEFGHJK"})
        ),
        (status = 400, description = "The group id or the rule id is not a ULID", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No WRITE on the group admin path", body = ErrorResponse),
        (status = 404, description = "No such rule in this group on this node", body = ErrorResponse),
        (status = 409, description = "The last run of this rule has not finished", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn run_import(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((group_id, import_id)): Path<(String, String)>,
) -> ServerResult<(StatusCode, Json<RunImportResponse>)> {
    let (group_id, auth) = admin_of_group(&state, auth, &group_id).await?;
    let rule = existing_rule(&state, group_id, &import_id).await?;
    let retention_ms = state.rocrate_limits().artifact_retention_ms;
    let started = start_run(
        &state.get_ctx(),
        &rule,
        auth,
        state.get_node_id(),
        None,
        retention_ms,
    )
    .await
    .map_err(map_error)?;
    let Some(job_id) = started else {
        return Err(ServerError::Conflict(
            "the last run of this import has not finished".to_string(),
        ));
    };
    let job_id = job_id.to_string();
    Ok((StatusCode::ACCEPTED, Json(RunImportResponse { job_id })))
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct RenameFileResponse {
    /// Path of the file in the backend now.
    pub from: String,
    /// Path the file gets under the rule's current pattern.
    pub to: String,
    /// The caller may write the file's key, so the rename renames it.
    pub writable: bool,
}

/// One page of the files a rename renames.
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema, utoipa::IntoParams)]
pub struct RenamePageQuery {
    /// Only keys that contain this text.
    pub filter: Option<String>,
    /// Files per page: 1 to 1000, default 100.
    pub limit: Option<usize>,
    /// The `next` value of the page before.
    pub after: Option<String>,
}

/// What a rename would do, from the rule's own record of its files.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct RenamePlanResponse {
    /// Bytes of the `count` files.
    pub bytes: u64,
    /// Of them, files that pass through the node, and their bytes: all files of a storage other
    /// than S3; on S3 those without a recorded ETag that are over 1 GiB or have no stored MD5.
    pub streamed_files: u64,
    pub streamed_bytes: u64,
    /// Of them, files whose new name another file of the rule already holds; they fail.
    pub taken_files: u64,
    /// Of them, files the caller may read but not write; they keep their name.
    pub withheld_files: u64,
    /// The rule's other files, which keep their name, and their bytes.
    pub unchanged_files: u64,
    pub unchanged_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct ImportRenamesResponse {
    /// Files of the rule that an earlier pattern named and that a rename would give their
    /// current name, as far as the caller may read their keys.
    pub count: u64,
    /// Always `true`: the plan covers every file of the rule.
    pub complete: bool,
    /// The rest of the plan.
    pub plan: RenamePlanResponse,
    /// One page of them.
    pub files: Vec<RenameFileResponse>,
    /// Cursor of the next page; omitted on the last page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next: Option<String>,
    /// Progress of the last move of the rule's folder on this node; absent before any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress: Option<MigrationProgress>,
}

#[utoipa::path(
    get,
    path = "/data/groups/{group_id}/storage/imports/{import_id}/rename",
    tag = "data/storage",
    summary = "List the files a rename would rename",
    description = r#"Lists the files of a backend import rule that an earlier write pattern named, with the name the current pattern gives them, and the progress of the last rename.

**Authentication**: realm bearer token with WRITE on the group's admin path and on the rule's
bucket, including request policies. Tokens with path restrictions are refused. Only files whose
key the caller may read are counted and listed.

**Behavior**
- The plan reads only the rule's own record of its files, never the provider. A file counts
  when an earlier pattern of the rule named it for exactly the version recorded for it. Files
  a user put there, and files with their current name, count as `unchanged_files`.
- On S3 storages, files are copied inside the provider, in parts of up to 1 GiB at any size,
  while a provider version or the recorded ETag holds the source, or as one part proven by the
  stored MD5. Other storages pass every file through the node.
- `plan` splits the counted files: `streamed_files` that pass through the node (on S3 those
  none of these holds, unless the provider keeps a version); `taken_files` whose new name the rule
  records for another file; `withheld_files` the caller may not write, which make `POST`
  answer 403.
- Each listed file is also checked against its version; a file whose version changed is not
  listed, and a rename leaves it in place.
- A filter limits the plan and the pages alike.
- Changing the pattern renames nothing; only `POST` on this path does.

**Limits**
- Pages hold 1 to 1000 files, in path order; `next` resumes after the last listed file. The
  plan covers every file of the rule, read in batches, so `complete` is always true; a large
  rule takes longer to plan."#,
    params(
        ("group_id" = String, Path, description = "Group that owns the rule, as a 26-character ULID"),
        ("import_id" = String, Path, description = "Rule to check, as a 26-character ULID"),
        RenamePageQuery
    ),
    responses(
        (
            status = 200,
            description = "The files and the last rename",
            body = ImportRenamesResponse,
            example = json!({
                "count": 1,
                "complete": true,
                "plan": {
                    "bytes": 1048576,
                    "streamed_files": 0,
                    "streamed_bytes": 0,
                    "taken_files": 0,
                    "withheld_files": 0,
                    "unchanged_files": 12,
                    "unchanged_bytes": 52428800
                },
                "files": [{
                    "from": "instrument/raw/01JA7Q3K2M4N5P6Q7R8S9T0V1W_b.fastq",
                    "to": "instrument/raw/b.fastq~01JA7Q3K2M4N5P6Q7R8S9T0V1W",
                    "writable": true
                }]
            })
        ),
        (status = 400, description = "The group id or the rule id is not a ULID", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No WRITE on the group admin path or the bucket, or a restricted token", body = ErrorResponse),
        (status = 404, description = "No such rule in this group on this node", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_renames(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((group_id, import_id)): Path<(String, String)>,
    axum::extract::Query(page): axum::extract::Query<RenamePageQuery>,
) -> ServerResult<Json<ImportRenamesResponse>> {
    use aruna_operations::blob::migration::misplaced::{DEFAULT_PAGE, MAX_PAGE, rename_page};
    use base64::Engine;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let (group_id, auth) = admin_of_group(&state, auth, &group_id).await?;
    let rule = existing_rule(&state, group_id, &import_id).await?;
    let limit = page.limit.unwrap_or(DEFAULT_PAGE);
    let bad = || ServerError::BadRequestMessage("`after` is not a cursor".to_string());
    let after = match page.after.as_deref() {
        Some(after) => Some(URL_SAFE_NO_PAD.decode(after).map_err(|_| bad())?),
        None => None,
    };
    if !(1..=MAX_PAGE).contains(&limit) {
        return Err(ServerError::BadRequestMessage(format!(
            "limit takes 1 to {MAX_PAGE}"
        )));
    }
    let context = state.get_ctx();
    let access = aruna_operations::groups::imports::discovery::bucket_access(
        &context,
        &auth,
        state.get_node_id(),
        &rule,
    )
    .await
    .map_err(map_error)?;
    let readable = |key: &str| access.allows(Some(key), &Permission::READ);
    let writable = |key: &str| access.allows(Some(key), &Permission::WRITE);
    let found = rename_page(
        &context,
        &rule,
        (&readable, &writable),
        (page.filter.as_deref(), after, limit),
    )
    .await
    .map_err(ServerError::InternalError)?;
    let status = drive(
        MigrationStatusOperation::new(rule.bucket.clone()),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| ServerError::InternalError(error.to_string()))?;
    let last = status.filter(|record| record.moving && record.prefix == rule.key_prefix);
    let (files, plan) = (found.entries.into_iter(), found.plan);
    Ok(Json(ImportRenamesResponse {
        count: plan.files,
        complete: plan.complete,
        plan: RenamePlanResponse {
            bytes: plan.bytes,
            streamed_files: plan.streamed_files,
            streamed_bytes: plan.streamed_bytes,
            taken_files: plan.taken_files,
            withheld_files: plan.withheld_files,
            unchanged_files: plan.unchanged_files,
            unchanged_bytes: plan.unchanged_bytes,
        },
        files: files
            .map(|(from, to, writable)| RenameFileResponse { from, to, writable })
            .collect(),
        next: found.next.map(|key| URL_SAFE_NO_PAD.encode(key)),
        progress: last.map(Into::into),
    }))
}

#[utoipa::path(
    post,
    path = "/data/groups/{group_id}/storage/imports/{import_id}/rename",
    tag = "data/storage",
    summary = "Give a rule's files the current name",
    description = r#"Starts this node's background rename of the files `GET` on this path lists, as a storage move of the rule's folder in the bucket.

**Authentication**: realm bearer token with WRITE on the group's admin path, on the rule's
bucket and on the key of every file to rename, including request policies. Tokens with path
restrictions are refused.

**Behavior**
- Each file is copied to its new name inside the provider where it can, and created only
  while that name is free. The copy holds the old file's recorded provider version or ETag, or
  its MD5 is checked. Its version then names the new file, together with the rule's record of
  the file. The old file is deleted after that at its provider version; without one only while
  its name carries the version's id and its size and recorded ETag still match, else it stays.
- A copy an interrupted rename left at the new name is used again when its size and ETag prove
  the same bytes; no file is read to compare. Another file at the new name keeps the old file in
  place and counts as failed.
- The move also moves the folder's other versions to where a new upload of their key goes, as
  `POST /data/buckets/{bucket}/storage/move` does. Progress shows in `GET` on this path.
- The planned `count` of files to rename is stored with the move. It shows as the rename's
  `total` in `GET /data/groups/{group_id}/storage/imports/{import_id}/organize/job` and in `job`
  of `GET /data/groups/{group_id}/storage/buckets`.
- A pattern change is refused while the rename runs, and a version that changed meanwhile
  keeps its file.

**Limits**
- Only files on the rule's own folder storage are renamed. Encrypted buckets answer 409
  `bucket_encrypted`, and a move with another prefix answers 409 `move_running`.
- The keys of all files `GET` on this path plans are checked before the start. A pattern
  change between that check and the start answers 409 `rule_changed`; once the rename runs,
  pattern changes are refused.
- A storage that cannot create a file only while its name is free renames nothing: each such
  file keeps its name and counts as failed. A copy an earlier rename left at its tagged name is
  still used when its size and ETag prove the same bytes.
- The rename runs as the caller: a file is renamed only while the caller may still write its
  key when the rename reaches it, including files written after the start. Others keep their
  name. Once the caller's token expires, is revoked or is cut off, the rename renames nothing
  more."#,
    params(
        ("group_id" = String, Path, description = "Group that owns the rule, as a 26-character ULID"),
        ("import_id" = String, Path, description = "Rule whose files get their name, as a 26-character ULID")
    ),
    responses(
        (
            status = 202,
            description = "The move as stored; it runs in the background",
            body = BucketMoveStarted,
            example = json!({
                "bucket": "research-raw",
                "prefix": "raw/",
                "progress": {
                    "migrated": 0,
                    "skipped": 0,
                    "failed": 0,
                    "retries": 0,
                    "started_at_ms": 1790000000000_u64
                }
            })
        ),
        (status = 400, description = "The group id or the rule id is not a ULID", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No WRITE on the group admin path, the bucket or a key, or a restricted token", body = ErrorResponse),
        (status = 404, description = "No such rule in this group on this node", body = ErrorResponse),
        (status = 409, description = "`bucket_encrypted`, `move_running` or `rule_changed`", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn start_renames(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    Path((group_id, import_id)): Path<(String, String)>,
) -> ServerResult<(StatusCode, Json<BucketMoveStarted>)> {
    // The rename runs as the caller later, so the token itself must keep holding.
    let bearer = bearer.ok_or(ServerError::Unauthorized)?;
    let grant = token_grant(bearer.as_str()).map_err(|_| ServerError::Unauthorized)?;
    let (group_id, auth) = admin_of_group(&state, auth, &group_id).await?;
    let rule = existing_rule(&state, group_id, &import_id).await?;
    let caller = (&auth, state.get_node_id());
    // The move starts only while the rule and its patterns are as they were when checked.
    let guards = rename_guards(&state.get_ctx(), &rule)
        .await
        .map_err(map_error)?;
    let checked = authorized_renames(&state.get_ctx(), caller, &rule, Permission::WRITE)
        .await
        .map_err(map_error)?;
    if checked.withheld_files > 0 {
        return Err(ServerError::Forbidden);
    }
    // Files written after the check are authorized one by one when the rename reaches them.
    let (bucket, prefix) = (rule.bucket, rule.key_prefix);
    let operation = StartMoveOperation::new(bucket.clone(), group_id, prefix, now_ms())
        .renaming((auth.clone(), state.get_node_id()), grant, checked.files)
        .with_guards(guards);
    let record = drive(operation, &state.get_ctx())
        .await
        .map_err(move_refusal)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(BucketMoveStarted {
            bucket,
            prefix: record.prefix.clone(),
            progress: record.into(),
        }),
    ))
}

#[cfg(test)]
#[path = "imports_tests.rs"]
pub(crate) mod tests;
