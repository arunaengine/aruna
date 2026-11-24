//! Preview-only edge reads: build drafts and source maps, served only to an applet's creator
//! and approvers in an authenticated preview session.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::applet::record::{Applet, PinnedObject, VersionState};
use aruna_ops_work::applets::reads::LoadAppletOperation;
use aruna_runner::driver::drive;
use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::response::Response;
use ulid::Ulid;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use super::edge::{FileQuery, edge_auth, previewer, read_manifest, serve_pinned, visitor};
use super::{applet_error, applet_id};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server::state::ServerState;

const SOURCE_MAP_TYPE: &str = "application/json";

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new()
        .routes(routes!(version_map))
        .routes(routes!(draft_file))
        .routes(routes!(draft_map))
}

/// The applet when the edge key is valid and the visitor previews it; anyone else learns
/// nothing, so a missing preview right reads as a missing applet.
async fn previewed(state: &ServerState, headers: &HeaderMap, id: &str) -> ServerResult<Applet> {
    edge_auth(state, headers)?;
    let id = applet_id(id)?;
    let load = drive(LoadAppletOperation::new(id), &state.get_ctx()).await;
    let applet = load.map_err(applet_error)?.ok_or(ServerError::NotFound)?;
    let visitor = visitor(state, headers, id).await;
    match previewer(state, &applet, visitor.as_ref()).await {
        true => Ok(applet),
        false => Err(ServerError::NotFound),
    }
}

/// The manifest of draft `build` once its build was captured.
fn draft_manifest(applet: &Applet, build: &str) -> ServerResult<PinnedObject> {
    let build = Ulid::from_string(build).map_err(|_| ServerError::NotFound)?;
    (applet.draft(build))
        .and_then(|draft| draft.manifest.clone())
        .ok_or(ServerError::NotFound)
}

async fn serve_map(
    state: &ServerState,
    (applet, headers): (&Applet, &HeaderMap),
    manifest: &PinnedObject,
) -> ServerResult<Response> {
    let manifest = read_manifest(state, applet, manifest).await?;
    let map = manifest.source_map.ok_or(ServerError::NotFound)?;
    serve_pinned(state, (applet, headers), &map, (SOURCE_MAP_TYPE, None)).await
}

#[utoipa::path(
    get,
    path = "/edge/v1/applets/{applet_id}/versions/{version}/source-map",
    tag = "applets",
    summary = "Read a version's source map in a preview",
    description = r#"Streams the builder's `aruna-source-map.json` of one version to a preview session.

**Authentication**: the configured edge key in `X-Aruna-Edge-Key`, plus the sealed token of the
applet's creator or an approver in `X-Aruna-Visitor`.

**Behavior**
- The source map is never a served file of the version; only this route reads it.
- Any other visitor, and a version without a source map, answers 404."#,
    params(
        ("applet_id" = String, Path, description = "Applet id; an unparseable id is 404"),
        ("version" = u32, Path, description = "Version number")
    ),
    responses(
        (status = 200, description = "The source map", content_type = "application/json", body = Object,
            example = json!({"version": 3, "sources": ["src/App.vue"], "mappings": "AAAA"})),
        (status = 304, description = "The edge holds this source map already"),
        (status = 401, description = "No valid edge key", body = ErrorResponse),
        (status = 404, description = "No such applet, version or source map, or no preview right", body = ErrorResponse)
    )
)]
pub async fn version_map(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Path((id, version)): Path<(String, u32)>,
) -> ServerResult<Response> {
    let applet = previewed(&state, &headers, &id).await?;
    let entry = applet.version(version).ok_or(ServerError::NotFound)?;
    if entry.state == VersionState::TakenDown {
        return Err(ServerError::NotFound);
    }
    let manifest = entry.manifest.clone();
    serve_map(&state, (&applet, &headers), &manifest).await
}

#[utoipa::path(
    get,
    path = "/edge/v1/applets/{applet_id}/drafts/{build_id}/file",
    tag = "applets",
    summary = "Read a draft file in a preview",
    description = r#"Streams one file of a captured build draft to a preview session.

**Authentication**: the configured edge key in `X-Aruna-Edge-Key`, plus the sealed token of the
applet's creator or an approver in `X-Aruna-Visitor`.

**Behavior**
- A folder serves its `index.html`. `ETag` is the file's BLAKE3 digest, and an `If-None-Match`
  that names it answers 304.
- Any other visitor, and a draft whose build was not captured, answers 404."#,
    params(
        ("applet_id" = String, Path, description = "Applet id; an unparseable id is 404"),
        ("build_id" = String, Path, description = "Build id; an unparseable id is 404"),
        FileQuery
    ),
    responses(
        (status = 200, description = "The file bytes with their content type", content_type = "application/octet-stream", body = Vec<u8>),
        (status = 304, description = "The edge holds this file already"),
        (status = 401, description = "No valid edge key", body = ErrorResponse),
        (status = 404, description = "No such applet, draft or file, or no preview right", body = ErrorResponse)
    )
)]
pub async fn draft_file(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Path((id, build)): Path<(String, String)>,
    Query(query): Query<FileQuery>,
) -> ServerResult<Response> {
    let applet = previewed(&state, &headers, &id).await?;
    let manifest = read_manifest(&state, &applet, &draft_manifest(&applet, &build)?).await?;
    let file = manifest.resolve(&query.path).ok_or(ServerError::NotFound)?;
    let pinned = file.pinned();
    let typed = (file.content_type.as_str(), Some(file.size_bytes));
    serve_pinned(&state, (&applet, &headers), &pinned, typed).await
}

#[utoipa::path(
    get,
    path = "/edge/v1/applets/{applet_id}/drafts/{build_id}/source-map",
    tag = "applets",
    summary = "Read a draft's source map in a preview",
    description = r#"Streams the builder's `aruna-source-map.json` of a captured draft to a preview session.

**Authentication**: the configured edge key in `X-Aruna-Edge-Key`, plus the sealed token of the
applet's creator or an approver in `X-Aruna-Visitor`.

**Behavior**
- Any other visitor, and a draft without a source map, answers 404."#,
    params(
        ("applet_id" = String, Path, description = "Applet id; an unparseable id is 404"),
        ("build_id" = String, Path, description = "Build id; an unparseable id is 404")
    ),
    responses(
        (status = 200, description = "The source map", content_type = "application/json", body = Object,
            example = json!({"version": 3, "sources": ["src/App.vue"], "mappings": "AAAA"})),
        (status = 304, description = "The edge holds this source map already"),
        (status = 401, description = "No valid edge key", body = ErrorResponse),
        (status = 404, description = "No such applet, draft or source map, or no preview right", body = ErrorResponse)
    )
)]
pub async fn draft_map(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Path((id, build)): Path<(String, String)>,
) -> ServerResult<Response> {
    let applet = previewed(&state, &headers, &id).await?;
    let manifest = draft_manifest(&applet, &build)?;
    serve_map(&state, (&applet, &headers), &manifest).await
}
