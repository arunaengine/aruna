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
    /// Branches, tags and any `refs/conflicts/...` refs replicated for this document.
    pub refs: std::collections::BTreeMap<String, String>,
}

#[utoipa::path(get, path = "/metadata/{document_id}/git", tag = "metadata/git",
    security(("bearer_auth" = [])), summary = "Get the automatic ARC repository",
    description = "Returns the automatic ARC repository and its conversion status.\n\n**Authentication**: realm bearer token with READ on the metadata document.\n\n**Behavior**: any current holder of the document serves the repository from replicated Git records and rebuilds a missing local copy. The protected aruna branch tracks graph snapshots, which are also merged into main. Concurrent pushes to one branch on different holders keep the first; the other is listed under refs/conflicts/. A conversion error means no new valid snapshot was published.",
    params(("document_id" = String, Path, description = "Metadata document ID")),
    responses((status = 200, description = "Repository and conversion status", body = RepositoryStatus,
               example = json!({"document_id":"01M000000000000000000000000","clone_url":"https://node.example/api/v1/git/01M000000000000000000000000.git","lfs_url":"https://node.example/api/v1/git/01M000000000000000000000000.git/info/lfs","bucket":"arc-storage","revision":"01M000000000000000000000001","commit":"1111111111111111111111111111111111111111","error":null,"refs":{"refs/heads/aruna":"1111111111111111111111111111111111111111","refs/heads/main":"1111111111111111111111111111111111111111"}})),
              (status = 401, description = "Authentication required"), (status = 403, description = "Access denied"),
              (status = 404, description = "Document missing or not held by this node"),
              (status = 503, description = "Metadata or conversion runtime unavailable")))]
pub async fn repository_status(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(id): Path<Ulid>,
) -> ServerResult<Json<RepositoryStatus>> {
    let auth = require_realm_auth(&state, auth)?;
    let (_, repository) = git::repository(&state.get_ctx(), &auth, id, Permission::READ)
        .await
        .map_err(map_error)?;
    let (status, projection) = git::snapshot::status(
        &state.get_ctx(),
        state.git().ok_or(ServerError::ServiceUnavailable)?,
        &auth,
        id,
    )
    .await
    .map_err(map_error)?;
    let clone_url = base_url(&state, id).await?;
    Ok(Json(RepositoryStatus {
        document_id: id.to_string(),
        lfs_url: format!("{clone_url}/info/lfs"),
        clone_url,
        bucket: repository.bucket,
        revision: projection
            .state
            .revision
            .map(|revision| revision.to_string()),
        commit: projection.state.refs.get("refs/heads/aruna").cloned(),
        // A conversion error matters only while no newer snapshot replaced it.
        error: status
            .filter(|value| {
                projection
                    .state
                    .revision
                    .is_none_or(|applied| value.event_id > applied)
            })
            .and_then(|value| value.error),
        refs: projection.state.refs,
    }))
}

#[derive(Deserialize)]
pub struct RevisionQuery {
    pub revision: String,
}

#[utoipa::path(get, path = "/metadata/{document_id}/git/rocrate", tag = "metadata/git",
    security(("bearer_auth" = [])), summary = "Export an ARC commit as RO-Crate",
    description = "Exports one Git revision as ISA-derived RO-Crate metadata.\n\n**Authentication**: realm bearer token with READ on the metadata document.\n\n**Behavior**: the revision resolves to one commit whose ISA metadata is parsed by pinned ARCtrl. The response names that commit; the collaborative graph and other branches remain unchanged.",
    params(("document_id" = String, Path, description = "Metadata document ID"),
           ("revision" = String, Query, description = "Branch, tag or full commit ID")),
    responses((status = 200, description = "Exact commit and derived RO-Crate", body = Value,
               example = json!({"commit":"1111111111111111111111111111111111111111","rocrate":{"@context":"https://w3id.org/ro/crate/1.2/context","@graph":[{"@id":"ro-crate-metadata.json","@type":"CreativeWork","about":{"@id":"./"},"conformsTo":{"@id":"https://w3id.org/ro/crate/1.2"}},{"@id":"./","@type":"Dataset","additionalType":"Investigation","identifier":"arc-example","name":"ARC example","description":"Example investigation","datePublished":"2026-09-22","license":{"@id":"https://creativecommons.org/licenses/by/4.0/"}}]}})),
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
