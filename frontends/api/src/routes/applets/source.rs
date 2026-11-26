//! Reads and commits an applet's source in its Git repository, and releases a commit's static
//! files as a new private version.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;
use std::sync::Arc;

use aruna_core::applet::manifest::MAX_SITE_BYTES;
use aruna_ops_data::git::commit::{commit_files, read_tree};
use aruna_ops_work::applets::release::{ReleaseOperation, Source};
use aruna_ops_work::applets::{AppletAccessOperation, Need};
use aruna_runner::driver::drive;
use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use super::{applet_error, applet_id, require_feature};
use crate::auth::UnrestrictedAuth;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::git::git_node;
use crate::server::state::ServerState;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new()
        .routes(routes!(source_files))
        .routes(routes!(commit_source))
        .routes(routes!(release_version))
}

#[derive(Clone, Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct SourceQuery {
    /// A branch, tag or commit; `main` when absent.
    pub revision: Option<String>,
    /// A folder of the repository to list; the whole tree when absent.
    pub root: Option<String>,
    /// Whether to return each file's content as base64.
    #[serde(default)]
    pub contents: bool,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct SourceFile {
    pub path: String,
    /// The file as base64, when `contents` was asked for.
    pub content: Option<String>,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(example = json!({"commit": "4b825dc642cb6eb9a060e54bf8d69288fbee4904",
    "files": [{"path": "index.html", "content": null}, {"path": "src/main.ts", "content": null}]}))]
pub struct SourceTree {
    pub commit: String,
    pub files: Vec<SourceFile>,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[schema(example = json!({"base": "4b825dc642cb6eb9a060e54bf8d69288fbee4904",
    "message": "Add the upload form", "files": {"src/App.vue": "PHRlbXBsYXRlPjwvdGVtcGxhdGU+", "old.js": null}}))]
pub struct CommitRequest {
    /// The main commit the change starts from; a moved main answers 412.
    pub base: String,
    pub message: String,
    /// Paths and their new contents as base64; `null` removes the path.
    pub files: BTreeMap<String, Option<String>>,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(example = json!({"commit": "9fceb02d0ae598e95dc970b74767f19372d61af8"}))]
pub struct CommitResponse {
    /// The new main commit, or the base when nothing changed.
    pub commit: String,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[schema(example = json!({"revision": "main", "root": "dist"}))]
pub struct ReleaseRequest {
    /// A branch, tag or commit of the applet repository; `main` when absent.
    pub revision: Option<String>,
    /// The folder whose files the version serves; the repository root when absent.
    pub root: Option<String>,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(example = json!({"version": 3, "commit": "9fceb02d0ae598e95dc970b74767f19372d61af8", "state": "private"}))]
pub struct ReleasedVersion {
    pub version: u32,
    pub commit: Option<String>,
    pub state: super::wire::VersionStateBody,
}

/// The repository of an applet the caller may change.
async fn repository(
    state: &ServerState,
    auth: &aruna_core::structs::identity::auth::AuthContext,
    id: &str,
    need: Need,
) -> ServerResult<ulid::Ulid> {
    let read = AppletAccessOperation::new(auth, applet_id(id)?, need);
    let applet = drive(read, &state.get_ctx()).await.map_err(applet_error)?;
    (applet.repository)
        .ok_or_else(|| ServerError::Conflict("the applet has no repository".to_string()))
}

#[utoipa::path(
    get,
    path = "/applets/{applet_id}/source",
    tag = "applets",
    summary = "List the applet's source files",
    description = r#"Lists the files of one revision of the applet's Git repository, with their contents on request.

**Authentication**: realm bearer token without path restrictions, with READ on the applet's group
data and on its repository document.

**Limits**
- With `contents`, the files together may hold at most 256 MiB."#,
    params(("applet_id" = String, Path, description = "Applet id; an unparseable id is 404"), SourceQuery),
    responses(
        (status = 200, description = "The files of the revision", body = SourceTree),
        (status = 404, description = "No such applet or revision", body = ErrorResponse),
        (status = 409, description = "The applet has no repository", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn source_files(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Path(id): Path<String>,
    Query(query): Query<SourceQuery>,
) -> ServerResult<Json<SourceTree>> {
    let repository = repository(&state, &auth, &id, Need::Read).await?;
    let revision = query.revision.as_deref().unwrap_or("main");
    let limit = query.contents.then_some(MAX_SITE_BYTES);
    let root = query.root.as_deref().unwrap_or_default();
    let read = read_tree(
        &git_node(&state),
        &auth,
        (repository, revision),
        root,
        limit,
    );
    let tree = drive(read, &state.get_ctx())
        .await
        .map_err(ServerError::from)?;
    Ok(Json(SourceTree {
        commit: tree.commit,
        files: (tree.files.into_iter())
            .map(|(path, content)| SourceFile {
                path,
                content: content.map(|bytes| STANDARD.encode(bytes)),
            })
            .collect(),
    }))
}

#[utoipa::path(
    post,
    path = "/applets/{applet_id}/source/commits",
    tag = "applets",
    summary = "Commit files to the applet's source",
    description = r#"Writes file contents to the applet repository's main branch as one commit made by the node.

**Authentication**: realm bearer token without path restrictions, with WRITE on the applet's group
data and on its repository document.

**Behavior**
- The node writes the commit for the caller, so no Git client or credential is needed.
- `base` must still be main's head, else 412; read the source again and retry.
- The metadata files `ro-crate-metadata.json` and `aruna-metadata.json` stay with the metadata
  document and cannot be committed here.
- A commit never creates a version; release or build it for that."#,
    params(("applet_id" = String, Path, description = "Applet id; an unparseable id is 404")),
    request_body = CommitRequest,
    responses(
        (status = 200, description = "The new main commit", body = CommitResponse),
        (status = 400, description = "Invalid base64, path or empty change", body = ErrorResponse),
        (status = 403, description = "No WRITE on the group's data or the repository, or applets are off", body = ErrorResponse),
        (status = 404, description = "No such applet", body = ErrorResponse),
        (status = 409, description = "The applet has no repository, or a path is locked by another user", body = ErrorResponse),
        (status = 412, description = "Main moved since `base`", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn commit_source(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Path(id): Path<String>,
    Json(body): Json<CommitRequest>,
) -> ServerResult<Json<CommitResponse>> {
    let read = AppletAccessOperation::new(&auth, applet_id(&id)?, Need::Write);
    let applet = drive(read, &state.get_ctx()).await.map_err(applet_error)?;
    require_feature(&state, applet.group_id).await?;
    let repository = (applet.repository)
        .ok_or_else(|| ServerError::Conflict("the applet has no repository".to_string()))?;
    let mut files = Vec::with_capacity(body.files.len());
    for (path, content) in body.files {
        let content = (content.as_deref())
            .map(|text| STANDARD.decode(text).map(Bytes::from))
            .transpose()
            .map_err(|_| ServerError::BadRequestReason(format!("{path} is not base64")))?;
        files.push((path, content));
    }
    let commit = commit_files(
        &git_node(&state),
        &auth,
        (repository, body.base),
        files,
        body.message,
    );
    let commit = drive(commit, &state.get_ctx())
        .await
        .map_err(ServerError::from)?;
    Ok(Json(CommitResponse { commit }))
}

#[utoipa::path(
    post,
    path = "/applets/{applet_id}/versions",
    tag = "applets",
    summary = "Release a commit as a private version",
    description = r#"Copies the static files of one revision folder into the held bucket as a new private version.

**Authentication**: realm bearer token without path restrictions, with WRITE on the group's held
applet bucket on the applet's host node and READ on its repository document.

**Behavior**
- No build runs: the folder must already hold the served HTML, CSS and JavaScript with an
  `index.html` at its root. A `package-lock.json` there is recorded, not served.
- Each file is pinned to the exact object version written, so the version never changes.
- The new version is private; only an approval puts it live.

**Limits**
- The files of a version may hold at most 256 MiB together."#,
    params(("applet_id" = String, Path, description = "Applet id; an unparseable id is 404")),
    request_body = ReleaseRequest,
    responses(
        (status = 201, description = "The new private version", body = ReleasedVersion),
        (status = 400, description = "No index.html, an invalid path, or too many bytes", body = ErrorResponse),
        (status = 403, description = "No WRITE on the held bucket", body = ErrorResponse),
        (status = 404, description = "No such applet", body = ErrorResponse),
        (status = 409, description = "The applet has no repository, is taken down or hosted elsewhere, or applets are off", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn release_version(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Path(id): Path<String>,
    Json(body): Json<ReleaseRequest>,
) -> ServerResult<(StatusCode, Json<ReleasedVersion>)> {
    let applet = applet_id(&id)?;
    let read = AppletAccessOperation::new(&auth, applet, Need::Read);
    let record = drive(read, &state.get_ctx()).await.map_err(applet_error)?;
    require_feature(&state, record.group_id).await?;
    let source = Source::Commit {
        revision: body.revision.unwrap_or_else(|| "main".to_string()),
        root: body.root.unwrap_or_default(),
    };
    let release = ReleaseOperation::new(&git_node(&state), &auth, applet, source);
    let entry = drive(release, &state.get_ctx())
        .await
        .map_err(applet_error)?;
    let state_body = match entry.state {
        aruna_core::applet::record::VersionState::Private => super::wire::VersionStateBody::Private,
        aruna_core::applet::record::VersionState::Approved => {
            super::wire::VersionStateBody::Approved
        }
        aruna_core::applet::record::VersionState::TakenDown => {
            super::wire::VersionStateBody::TakenDown
        }
    };
    Ok((
        StatusCode::CREATED,
        Json(ReleasedVersion {
            version: entry.version,
            commit: entry.commit,
            state: state_body,
        }),
    ))
}
