//! Exposes automatic ARC repository status and ISA-derived metadata for an exact Git commit.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{base_url, map_error};
use crate::auth::require_realm_auth;
use crate::error::{ServerError, ServerResult};
use crate::server::state::ServerState;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_operations::git;
use axum::extract::{Path, Query, State};
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use ulid::Ulid;
use utoipa::ToSchema;

#[derive(Serialize, ToSchema)]
pub struct RepositoryStatus {
    pub document_id: String,
    pub clone_url: String,
    pub lfs_url: String,
    pub bucket: String,
    pub revision: Option<String>,
    pub commit: Option<String>,
    pub error: Option<String>,
}

#[utoipa::path(get, path = "/metadata/{document_id}/git", tag = "metadata/git",
    security(("bearer_auth" = [])), summary = "Get the automatic ARC repository",
    description = "Requires document READ. Creates a missing repository on its fixed owner from the accepted metadata. The protected aruna branch tracks graph snapshots. An error means no new valid ARC snapshot was published.",
    params(("document_id" = String, Path, description = "Metadata document ID")),
    responses((status = 200, description = "Repository and conversion status", body = RepositoryStatus),
              (status = 401, description = "Authentication required"), (status = 403, description = "Access denied"),
              (status = 404, description = "Document missing or Git belongs to another node"),
              (status = 503, description = "Metadata or conversion runtime unavailable")))]
pub async fn repository_status(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(id): Path<Ulid>,
) -> ServerResult<Json<RepositoryStatus>> {
    let auth = require_realm_auth(&state, auth)?;
    let repository = git::snapshot::ensure(&state.get_ctx(), &auth, id, Permission::READ)
        .await
        .map_err(map_error)?;
    let status = git::snapshot::status(&state.get_ctx(), &auth, id)
        .await
        .map_err(map_error)?;
    let clone_url = base_url(&state, id).await?;
    Ok(Json(RepositoryStatus {
        document_id: id.to_string(),
        lfs_url: format!("{clone_url}/info/lfs"),
        clone_url,
        bucket: repository.bucket,
        revision: status.as_ref().map(|value| value.event_id.to_string()),
        commit: status.as_ref().and_then(|value| value.commit.clone()),
        error: status.and_then(|value| value.error),
    }))
}

#[derive(Deserialize)]
pub struct RevisionQuery {
    pub revision: String,
}

#[utoipa::path(get, path = "/metadata/{document_id}/git/rocrate", tag = "metadata/git",
    security(("bearer_auth" = [])), summary = "Export an ARC commit as RO-Crate",
    description = "Requires repository READ. Resolves a branch, tag or commit once, parses its ISA metadata using pinned ARCtrl and returns the resolved commit plus RO-Crate JSON-LD. This does not overwrite another branch or the collaborative metadata graph.",
    params(("document_id" = String, Path, description = "Metadata document ID"),
           ("revision" = String, Query, description = "Branch, tag or full commit ID")),
    responses((status = 200, description = "Exact commit and derived RO-Crate", body = Value),
              (status = 400, description = "Revision has invalid ISA metadata"),
              (status = 401, description = "Authentication required"), (status = 403, description = "Access denied"),
              (status = 503, description = "Git revision or converter unavailable")))]
pub async fn export_revision(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(id): Path<Ulid>,
    Query(query): Query<RevisionQuery>,
) -> ServerResult<Json<Value>> {
    let auth = require_realm_auth(&state, auth)?;
    let bytes = git::snapshot::export(
        &state.get_ctx(),
        state.git().ok_or(ServerError::ServiceUnavailable)?,
        &auth,
        id,
        query.revision,
    )
    .await
    .map_err(map_error)?;
    let result: Value =
        serde_json::from_slice(&bytes).map_err(|_| ServerError::ServiceUnavailable)?;
    Ok(Json(result))
}
