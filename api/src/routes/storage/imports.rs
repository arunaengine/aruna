//! Serves the group routes that import the objects of a storage backend in place.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::{ensure_permission, parse_group_id, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::storage::routing::ensure_group_admin;
use crate::server::state::ServerState;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::backend_import::{BackendImport, DEFAULT_PATTERN};
use aruna_core::structs::storage::blob::bucket_permission_path;
use aruna_core::time::unix_timestamp_millis;
use aruna_operations::driver::drive;
use aruna_operations::groups::imports::change::{ChangeImportOperation, ImportChange};
use aruna_operations::groups::imports::create::{CreateImportInput, CreateImportOperation};
use aruna_operations::groups::imports::remove::RemoveImportOperation;
use aruna_operations::groups::imports::schedule::{next_due, start_run, wake_schedule};
use aruna_operations::groups::imports::{ImportRuleError, last_run, list_imports, read_import};
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
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct CreateImportRequest {
    /// A storage backend the group registered on this node.
    pub backend_id: String,
    /// Folder in the backend to import: empty, or a relative path ending with `/`.
    #[serde(default)]
    pub backend_prefix: String,
    pub bucket: String,
    /// Folder in the bucket that receives the objects: empty, or a path ending with `/`.
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
    /// omitted uses `{dir}{id}_{name}`.
    #[serde(default)]
    pub write_pattern: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct UpdateImportRequest {
    /// Five-field cron expression in UTC; `null` or omitted stops scheduled runs.
    #[serde(default)]
    pub schedule: Option<String>,
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

fn map_error(error: ImportRuleError) -> ServerError {
    match error {
        ImportRuleError::Invalid(message) => ServerError::BadRequestMessage(message),
        ImportRuleError::Conflict(message) => ServerError::Conflict(message),
        ImportRuleError::GroupWrite(error) => ServerError::Conflict(error.to_string()),
        ImportRuleError::NotFound => ServerError::NotFound,
        other => ServerError::InternalError(other.to_string()),
    }
}

async fn admin_of_group(
    state: &ServerState,
    auth: Option<AuthContext>,
    group_id: &str,
) -> ServerResult<(Ulid, AuthContext)> {
    let auth = require_realm_auth(state, auth)?;
    let group_id = parse_group_id(group_id)?;
    ensure_group_admin(state, &auth, group_id).await?;
    Ok((group_id, auth))
}

async fn existing_rule(
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
- A backend key `backend_prefix` + `x` becomes the bucket key `key_prefix` + `x`.
- A new upload to `key_prefix` + `d/n` is stored at the write folder (`write_prefix`, else
  `backend_prefix`) + `write_pattern`, with `{dir}` = `d/`, `{name}` = `n` and `{id}` = the
  version id, so other tools see it. Uploads under the rule's own import folder are not imported
  again; later changes to such a file are.
- A pattern without `{id}` replaces the file of the same name. Aruna first moves the bytes of the
  version stored there to `.aruna-versions/<path>/<id>`, so older versions stay readable. A second
  upload to the same file while one runs gets `409 OperationAborted`. A rejected upload does not
  bring back the replaced file.
- A bucket that turns on compression or encryption later stores new uploads in Aruna's own layout.
- Nothing runs on creation. Start a run on request or give a `schedule`.
- Aruna never deletes imported bytes from the backend. Deleting an object in Aruna only removes
  Aruna's record of it.
- The rule is node-local.

**Limits**
- The bucket must belong to the group, without compression and without encryption.
- All prefixes are empty or relative folders ending with `/`; `backend_prefix` and the write
  folder may not lie inside the folders where Aruna stores its own objects on that backend.
- `write_pattern` must contain `{name}` and give a relative path.
- Two rules may not import into overlapping folders of one bucket, and no rule may write into a
  backend folder that another rule reads or writes.
- `schedule` is a five-field cron expression evaluated in UTC."#,
    params(("group_id" = String, Path, description = "Group that owns the backend and the bucket, as a 26-character ULID")),
    request_body(
        content = CreateImportRequest,
        description = "The backend folder, the bucket folder and an optional schedule",
        example = json!({
            "backend_id": "01JBCKND0123456789ABCDEFGH",
            "backend_prefix": "instrument/raw/",
            "bucket": "research-raw",
            "key_prefix": "raw/",
            "write_pattern": "{dir}{id}_{name}",
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
                "write_pattern": "{dir}{id}_{name}",
                "schedule": "0 3 * * *",
                "next_run_at": "2026-10-15T03:00:00+00:00",
                "created_at": "2026-10-14T09:12:00+00:00",
                "updated_at": "2026-10-14T09:12:00+00:00"
            })
        ),
        (status = 400, description = "Invalid ids, prefixes, schedule, bucket or backend", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No WRITE on the group admin path or on the bucket", body = ErrorResponse),
        (status = 409, description = "A routing rule for the folder names another target, another rule imports into an overlapping folder or shares a backend folder, or the group is being deleted", body = ErrorResponse)
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
                .unwrap_or_else(|| DEFAULT_PATTERN.to_string()),
            now: SystemTime::now(),
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
                    "write_pattern": "{dir}{id}_{name}",
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
    summary = "Change a backend import's schedule",
    description = r#"Replaces the schedule of one backend import rule; the folders and the bucket stay as they are.

**Authentication**: realm bearer token with WRITE on the group's admin path.

**Behavior**
- `null` or an omitted `schedule` stops scheduled runs; runs on request keep working.
- A running import is not affected."#,
    params(
        ("group_id" = String, Path, description = "Group that owns the rule, as a 26-character ULID"),
        ("import_id" = String, Path, description = "Rule to change, as a 26-character ULID")
    ),
    request_body(
        content = UpdateImportRequest,
        description = "The new schedule, or `null` to stop scheduled runs",
        example = json!({"schedule": "*/30 * * * *"})
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
                "write_pattern": "{dir}{id}_{name}",
                "schedule": "*/30 * * * *",
                "next_run_at": "2026-10-14T10:00:00+00:00",
                "created_at": "2026-10-14T09:12:00+00:00",
                "updated_at": "2026-10-14T09:40:00+00:00"
            })
        ),
        (status = 400, description = "Invalid ids or schedule", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No WRITE on the group admin path", body = ErrorResponse),
        (status = 404, description = "No such rule in this group on this node", body = ErrorResponse),
        (status = 409, description = "The group is being deleted", body = ErrorResponse)
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
    let change = ImportChange::Schedule(request.schedule);
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
        (status = 409, description = "The group is being deleted", body = ErrorResponse)
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
    drive(
        RemoveImportOperation::new(group_id, import_id),
        &state.get_ctx(),
    )
    .await
    .map_err(map_error)?;
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
- Objects registered before are skipped. A changed object gets a new version unless the key
  was written or deleted in Aruna since; Aruna's side always wins.
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

#[cfg(test)]
#[path = "imports_tests.rs"]
mod tests;
