//! Serves the file name discovery rules of a backend import and their preview.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::{parse_group_id, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::storage::routing::ensure_group_admin;
use crate::server::state::ServerState;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::discovery::DiscoveryRule;
use aruna_operations::driver::drive;
use aruna_operations::groups::imports::discovery::{
    FileOutcome, SetDiscoveryOperation, authorized_preview, bucket_access, read_discovery,
};
use aruna_operations::groups::imports::{ImportRuleError, read_import};
use axum::extract::{Path, State};
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

#[derive(OpenApi)]
#[openapi()]
pub struct DiscoveryDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(DiscoveryDoc::openapi())
        .routes(routes!(get_discovery, put_discovery))
        .routes(routes!(preview_rules))
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct DiscoveryRuleBody {
    /// File name pattern: `*` any text, `?` one character, `[0-9]` or `[!x]` one of a set,
    /// `{4}` or `{2,8}` after a set or `?` repeats it, `{fna,fa}` either text.
    pub pattern: String,
    /// Folder of the bucket for matching files, ending with `/`.
    pub folder: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct DiscoveryRules {
    pub rules: Vec<DiscoveryRuleBody>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct DiscoveryPreviewRequest {
    /// The rules to try; they are not stored.
    pub rules: Vec<DiscoveryRuleBody>,
    /// Continues after this backend key, the `next` of the previous page.
    #[serde(default)]
    pub after: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct PreviewFileResponse {
    /// Key in the backend.
    pub path: String,
    /// Bucket key the file gets; omitted for a conflict.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    /// Version the file name or an unfinished write names; a run registers the file as it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
    /// `known` (a recorded mapping keeps it), `matched`, `root` or `conflict`.
    pub outcome: String,
    /// The patterns that all match a conflicting file.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub patterns: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct DiscoveryPreviewResponse {
    pub files: Vec<PreviewFileResponse>,
    /// Pass as `after` for the next page; omitted on the last page. It names a listed file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next: Option<String>,
}

fn map_error(error: ImportRuleError) -> ServerError {
    match error {
        ImportRuleError::Invalid(message) => ServerError::BadRequestMessage(message),
        ImportRuleError::Conflict(message) => ServerError::Conflict(message),
        ImportRuleError::GroupWrite(error) => ServerError::Conflict(error.to_string()),
        ImportRuleError::NotFound => ServerError::NotFound,
        ImportRuleError::Forbidden => ServerError::Forbidden,
        other => ServerError::InternalError(other.to_string()),
    }
}

fn to_rules(rules: Vec<DiscoveryRuleBody>) -> Vec<DiscoveryRule> {
    rules
        .into_iter()
        .map(|rule| DiscoveryRule {
            pattern: rule.pattern,
            folder: rule.folder,
        })
        .collect()
}

fn from_rules(rules: Vec<DiscoveryRule>) -> DiscoveryRules {
    let rules = rules
        .into_iter()
        .map(|rule| DiscoveryRuleBody {
            pattern: rule.pattern,
            folder: rule.folder,
        })
        .collect();
    DiscoveryRules { rules }
}

async fn admin_rule(
    state: &ServerState,
    auth: Option<AuthContext>,
    group_id: &str,
    import_id: &str,
) -> ServerResult<(AuthContext, Ulid, Ulid)> {
    let auth = require_realm_auth(state, auth)?;
    let group_id = parse_group_id(group_id)?;
    ensure_group_admin(state, &auth, group_id).await?;
    let import_id = Ulid::from_str(import_id).map_err(|_| ServerError::BadRequest)?;
    Ok((auth, group_id, import_id))
}

#[utoipa::path(
    get,
    path = "/data/groups/{group_id}/storage/imports/{import_id}/discovery",
    tag = "data/storage",
    summary = "Read a backend import's discovery rules",
    description = r#"Returns the file name rules that place files a run finds without a recorded mapping.

**Authentication**: realm bearer token with WRITE on the group's admin path."#,
    params(
        ("group_id" = String, Path, description = "Group that owns the rule, as a 26-character ULID"),
        ("import_id" = String, Path, description = "Rule to read, as a 26-character ULID")
    ),
    responses(
        (
            status = 200,
            description = "The stored rules, empty without any",
            body = DiscoveryRules,
            example = json!({"rules": [{"pattern": "*.faa.gz", "folder": "proteins/"}]})
        ),
        (status = 400, description = "The group id or the rule id is not a ULID", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No WRITE on the group admin path", body = ErrorResponse),
        (status = 404, description = "No such rule in this group on this node", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_discovery(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((group_id, import_id)): Path<(String, String)>,
) -> ServerResult<Json<DiscoveryRules>> {
    let (_, group_id, import_id) = admin_rule(&state, auth, &group_id, &import_id).await?;
    let context = state.get_ctx();
    read_import(&context, group_id, import_id)
        .await
        .map_err(map_error)?
        .ok_or(ServerError::NotFound)?;
    let rules = read_discovery(&context, group_id, import_id)
        .await
        .map_err(map_error)?;
    Ok(Json(from_rules(rules)))
}

#[utoipa::path(
    put,
    path = "/data/groups/{group_id}/storage/imports/{import_id}/discovery",
    tag = "data/storage",
    summary = "Replace a backend import's discovery rules",
    description = r#"Replaces the file name rules that place files a run finds without a recorded mapping.

**Authentication**: realm bearer token with WRITE on the group's admin path and on the rule's
bucket, including request policies. Tokens with path restrictions are refused.

**Behavior**
- A file without a recorded mapping or write intent whose name matches one pattern is imported
  at `folder` + its path below `backend_prefix`, so subfolders stay. A file that matches no
  pattern is imported at its path below `backend_prefix`, at the bucket root. The rule's
  `key_prefix` is where its uploads come from; found files never get it.
- Patterns match the file name only, without its folders: `*` any text, `?` one character,
  `[abc]`, `[0-9]` or `[!x]` one character of a set, `{4}` or `{2,8}` right after a set or `?`
  that many of it, and `{fna,fa}` either text. Every other character is literal. There are no
  regular expressions. Matching ignores the version id that the rule's
  current or an earlier `write_pattern`, or a `~<version id>` tag, adds to a name. A file
  whose name a pattern of the rule wrote is registered as that version of its new key.
- A file that already has a recorded mapping keeps its key; adding or changing rules never moves
  it. Use a rename to move such keys.
- A run reports a file that matches several patterns as failed and does not import it.
- A file whose new key already holds another object is reported as failed and not imported.
- The rules apply to the pages a run lists after the change. An empty list removes all rules.

**Limits**
- At most 32 patterns of 1 to 128 characters, without `/` or control characters.
- A `{...}` group holds 2 to 8 plain alternatives, and the groups of a pattern spell out at
  most 32 names. A count is 1 to 64, the smaller number first. Malformed patterns answer 400.
- Two patterns that can match the same name are refused (400), whatever their order.
- Each folder is a relative folder ending with `/`."#,
    params(
        ("group_id" = String, Path, description = "Group that owns the rule, as a 26-character ULID"),
        ("import_id" = String, Path, description = "Rule to change, as a 26-character ULID")
    ),
    request_body(
        content = DiscoveryRules,
        description = "The complete new list of rules",
        example = json!({"rules": [
            {"pattern": "*.faa.gz", "folder": "proteins/"},
            {"pattern": "*.fna.gz", "folder": "nucleotides/"}
        ]})
    ),
    responses(
        (
            status = 200,
            description = "The stored rules",
            body = DiscoveryRules,
            example = json!({"rules": [
                {"pattern": "*.faa.gz", "folder": "proteins/"},
                {"pattern": "*.fna.gz", "folder": "nucleotides/"}
            ]})
        ),
        (status = 400, description = "Invalid ids, patterns or folders, or patterns that overlap", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No WRITE on the group admin path or the bucket, or a restricted token", body = ErrorResponse),
        (status = 404, description = "No such rule in this group on this node", body = ErrorResponse),
        (status = 409, description = "The group is being deleted", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn put_discovery(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((group_id, import_id)): Path<(String, String)>,
    Json(request): Json<DiscoveryRules>,
) -> ServerResult<Json<DiscoveryRules>> {
    let (auth, group_id, import_id) = admin_rule(&state, auth, &group_id, &import_id).await?;
    let context = state.get_ctx();
    let rule = read_import(&context, group_id, import_id)
        .await
        .map_err(map_error)?
        .ok_or(ServerError::NotFound)?;
    bucket_access(&context, &auth, state.get_node_id(), &rule)
        .await
        .map_err(map_error)?;
    let operation = SetDiscoveryOperation::new(group_id, import_id, to_rules(request.rules));
    let rules = drive(operation, &context).await.map_err(map_error)?;
    Ok(Json(from_rules(rules)))
}

#[utoipa::path(
    post,
    path = "/data/groups/{group_id}/storage/imports/{import_id}/discovery/preview",
    tag = "data/storage",
    summary = "Preview discovery rules on a backend import's files",
    description = r#"Lists one page of the rule's backend folder and shows where a run would put each file with the given rules. Nothing is stored or imported.

**Authentication**: realm bearer token with WRITE on the group's admin path and on the rule's
bucket, including request policies. Tokens with path restrictions are refused.

**Behavior**
- Files are placed as a run places them: a recorded mapping first, then an unfinished write of
  the rule, then the file name. `version_id` is the version a run registers the file as.
- Only files whose bucket key the caller may read are listed. A conflict is checked by its
  key at the bucket root.
- `known`: a recorded mapping or an unfinished write keeps its key whatever the rules say.
- `matched`: one pattern matches; `key` is where the file goes.
- `root`: no pattern matches; the file goes to the bucket root.
- `conflict`: a run would report the file and not import it. `patterns` names the patterns
  that all match it. Empty `patterns` means the name fits several keys or versions,
  unfinished writes name different keys, or in a backend folder shared with other rules, no
  single rule names a new file and the rules would give it different keys.
- Files in the folders where Aruna stores its own objects are left out, as are files of a
  shared folder that another rule recorded, writes or alone matches. Rules that all give a new
  file the same root key of one bucket leave it to the oldest of them.
- Whether a key is already taken is checked by the run, not here.

**Limits**
- One page lists up to 200 files; pass `next` as `after` for the next one. `next` names a
  file this page shows. Pages without any file the caller may read are skipped, up to 2000
  files in a row; more answer 400.
- The rules follow the limits of `PUT` on the discovery path."#,
    params(
        ("group_id" = String, Path, description = "Group that owns the rule, as a 26-character ULID"),
        ("import_id" = String, Path, description = "Rule whose folder is listed, as a 26-character ULID")
    ),
    request_body(
        content = DiscoveryPreviewRequest,
        description = "The rules to try and an optional page start",
        example = json!({"rules": [{"pattern": "*.faa.gz", "folder": "proteins/"}]})
    ),
    responses(
        (
            status = 200,
            description = "Where each listed file would go",
            body = DiscoveryPreviewResponse,
            example = json!({
                "files": [
                    {"path": "raw/run1/sample.faa.gz", "key": "proteins/run1/sample.faa.gz", "outcome": "matched"},
                    {"path": "raw/baz", "key": "baz", "outcome": "root"},
                    {"path": "raw/bar.txt", "key": "foo/bar.txt", "outcome": "known"}
                ]
            })
        ),
        (status = 400, description = "Invalid ids, patterns or folders, or patterns that overlap", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No WRITE on the group admin path or the bucket, or a restricted token", body = ErrorResponse),
        (status = 404, description = "No such rule in this group on this node", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn preview_rules(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((group_id, import_id)): Path<(String, String)>,
    Json(request): Json<DiscoveryPreviewRequest>,
) -> ServerResult<Json<DiscoveryPreviewResponse>> {
    let (auth, group_id, import_id) = admin_rule(&state, auth, &group_id, &import_id).await?;
    let context = state.get_ctx();
    let rule = read_import(&context, group_id, import_id)
        .await
        .map_err(map_error)?
        .ok_or(ServerError::NotFound)?;
    let rules = to_rules(request.rules);
    let caller = (&auth, state.get_node_id());
    let preview = authorized_preview(&context, caller, &rule, &rules, request.after)
        .await
        .map_err(map_error)?;
    let files = preview
        .files
        .into_iter()
        .map(|file| {
            let (outcome, patterns) = match file.outcome {
                FileOutcome::Known => ("known", Vec::new()),
                FileOutcome::Matched => ("matched", Vec::new()),
                FileOutcome::Root => ("root", Vec::new()),
                FileOutcome::Conflict(patterns) => ("conflict", patterns),
            };
            PreviewFileResponse {
                path: file.path,
                key: file.key,
                version_id: file.version_id.map(|id| id.to_string()),
                outcome: outcome.to_string(),
                patterns,
            }
        })
        .collect();
    Ok(Json(DiscoveryPreviewResponse {
        files,
        next: preview.next,
    }))
}
