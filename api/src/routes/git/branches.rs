//! Plain JSON branch, tag, draft edit and merge endpoints of a dataset.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::versions::{
    BranchList, BranchView, MergeConflictView, MergeView, NamedView, TagList, VersionView,
    expected, failure, named_view, store, version_view,
};
use crate::auth::require_realm_auth;
use crate::error::{ServerError, ServerResult};
use crate::server::state::ServerState;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_operations::git::GitError;
use aruna_operations::git::merge::{self, Merged};
use aruna_operations::git::versions::{self, Named, WriteOptions};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;
use ulid::Ulid;
use utoipa::ToSchema;

#[derive(Deserialize, ToSchema)]
#[schema(example = json!({"name":"draft/new-assay","from":"main"}))]
pub struct CreateBranch {
    pub name: String,
    /// Branch, tag or version the new branch starts at.
    pub from: String,
}

#[derive(Deserialize, ToSchema)]
#[schema(example = json!({"name":"v1.0","version":"main"}))]
pub struct CreateTag {
    pub name: String,
    /// Branch, tag or version to name.
    pub version: String,
}

#[derive(Deserialize, ToSchema)]
#[schema(example = json!({"rocrate":{"@context":"https://w3id.org/ro/crate/1.2/context","@graph":[{"@id":"ro-crate-metadata.json","@type":"CreativeWork","about":{"@id":"./"},"conformsTo":{"@id":"https://w3id.org/ro/crate/1.2"}},{"@id":"./","@type":"Dataset","name":"Draft title","description":"Example investigation","datePublished":"2026-09-25","license":{"@id":"https://creativecommons.org/licenses/by/4.0/"}}]},"message":"Rename the investigation"}))]
pub struct EditDraft {
    pub rocrate: Value,
    pub message: Option<String>,
}

#[derive(Deserialize, ToSchema)]
#[schema(example = json!({"into":"main","message":"Merge the new assay"}))]
pub struct MergeBranch {
    /// Target branch, usually `main`.
    pub into: String,
    pub message: Option<String>,
}

fn merge_view(merged: Merged) -> MergeView {
    MergeView {
        version: merged.version,
        fast_forward: merged.fast_forward,
    }
}

#[utoipa::path(get, path = "/metadata/{document_id}/branches", tag = "metadata/versions",
    security(("bearer_auth" = [])), summary = "List dataset branches",
    description = "Lists every branch and the version it points to.\n\n**Authentication**: realm bearer token with READ on the metadata document.",
    params(("document_id" = String, Path, description = "Metadata document ID")),
    responses((status = 200, description = "Branches", body = BranchList),
              (status = 401, description = "Authentication required"), (status = 403, description = "Access denied"),
              (status = 404, description = "Document missing or not held by this node")))]
pub async fn branches(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(id): Path<Ulid>,
) -> ServerResult<Response> {
    let auth = require_realm_auth(&state, auth)?;
    let result = versions::names(&state.get_ctx(), store(&state)?, &auth, id, false).await;
    Ok(match result {
        Ok(names) => Json(BranchList {
            branches: names
                .into_iter()
                .map(|named| BranchView {
                    protected: versions::protected(&named.name),
                    name: named.name,
                    version: named.version,
                })
                .collect(),
        })
        .into_response(),
        Err(error) => failure(error),
    })
}

#[utoipa::path(post, path = "/metadata/{document_id}/branches", tag = "metadata/versions",
    security(("bearer_auth" = [])), summary = "Create a draft branch",
    description = "Creates a branch at an existing version.\n\n**Authentication**: realm bearer token with WRITE on the metadata document.\n\n**Behavior**: the branch replicates to every holder. A new branch is a draft; it changes the live metadata only when merged into main.",
    params(("document_id" = String, Path, description = "Metadata document ID")),
    request_body = CreateBranch,
    responses((status = 201, description = "Branch created", body = NamedView),
              (status = 400, description = "Invalid or reserved name"),
              (status = 401, description = "Authentication required"), (status = 403, description = "Access denied"),
              (status = 404, description = "Start version missing"),
              (status = 409, description = "The branch already exists")))]
pub async fn create_branch(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(id): Path<Ulid>,
    Json(request): Json<CreateBranch>,
) -> ServerResult<Response> {
    let auth = require_realm_auth(&state, auth)?;
    let name = format!("refs/heads/{}", request.name);
    let result = versions::change_ref(
        &state.get_ctx(),
        store(&state)?,
        &auth,
        id,
        name,
        Some(&request.from),
        None,
    )
    .await;
    Ok(created(result, &request.name))
}

fn created(result: Result<Named, GitError>, name: &str) -> Response {
    match result {
        Ok(named) => (
            StatusCode::CREATED,
            Json(NamedView {
                name: name.to_string(),
                version: named.version,
            }),
        )
            .into_response(),
        Err(error) => failure(error),
    }
}

fn deleted(result: Result<Named, GitError>) -> Response {
    match result {
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => failure(error),
    }
}

#[utoipa::path(delete, path = "/metadata/{document_id}/branches/{name}", tag = "metadata/versions",
    security(("bearer_auth" = [])), summary = "Delete a draft branch",
    description = "Deletes a draft branch; its versions stay reachable from other branches or tags that contain them.\n\n**Authentication**: realm bearer token with WRITE on the metadata document.\n\n**Behavior**: `main` and `aruna` cannot be deleted. `If-Match` with the head version refuses a moved branch with 412.",
    params(("document_id" = String, Path, description = "Metadata document ID"),
           ("name" = String, Path, description = "URL-encoded branch name"),
           ("If-Match" = Option<String>, Header, description = "Expected head version")),
    responses((status = 204, description = "Branch deleted"),
              (status = 400, description = "Protected branch"),
              (status = 401, description = "Authentication required"), (status = 403, description = "Access denied"),
              (status = 404, description = "Branch missing"),
              (status = 412, description = "The branch moved")))]
pub async fn delete_branch(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((id, name)): Path<(Ulid, String)>,
    headers: HeaderMap,
) -> ServerResult<Response> {
    let auth = require_realm_auth(&state, auth)?;
    let result = versions::change_ref(
        &state.get_ctx(),
        store(&state)?,
        &auth,
        id,
        format!("refs/heads/{name}"),
        None,
        expected(&headers).as_deref(),
    )
    .await;
    Ok(deleted(result))
}

#[utoipa::path(put, path = "/metadata/{document_id}/branches/{name}/rocrate", tag = "metadata/versions",
    security(("bearer_auth" = [])), summary = "Edit a draft branch's metadata",
    description = "Replaces a draft branch's metadata with the given RO-Crate as a new version on that branch.\n\n**Authentication**: realm bearer token with WRITE on the metadata document.\n\n**Behavior**: ISA workbooks, `ro-crate-metadata.json` and `aruna-metadata.json` are regenerated; other files stay. Equivalent workbooks keep their bytes. `main` is edited through `PUT /metadata/{document_id}/rocrate`. `If-Match` with the head version refuses a moved branch with 412.",
    params(("document_id" = String, Path, description = "Metadata document ID"),
           ("name" = String, Path, description = "URL-encoded draft branch name"),
           ("If-Match" = Option<String>, Header, description = "Expected head version")),
    request_body = EditDraft,
    responses((status = 200, description = "The branch's new head version", body = VersionView),
              (status = 400, description = "Protected branch or metadata that cannot become an ARC"),
              (status = 401, description = "Authentication required"), (status = 403, description = "Access denied"),
              (status = 404, description = "Branch missing"),
              (status = 409, description = "A changed file is locked by another user"),
              (status = 412, description = "The branch moved")))]
pub async fn edit(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((id, name)): Path<(Ulid, String)>,
    headers: HeaderMap,
    Json(request): Json<EditDraft>,
) -> ServerResult<Response> {
    let auth = require_realm_auth(&state, auth)?;
    if !request.rocrate.is_object() {
        return Err(ServerError::BadRequestMessage(
            "rocrate must be a JSON-LD object".into(),
        ));
    }
    let jsonld = serde_json::to_string(&request.rocrate).map_err(|_| ServerError::BadRequest)?;
    let result = merge::edit(
        &state.get_ctx(),
        store(&state)?,
        &auth,
        id,
        &name,
        jsonld,
        WriteOptions {
            message: request.message,
            expected: expected(&headers).as_deref(),
        },
    )
    .await;
    Ok(match result {
        Ok(version) => Json(version_view(version)).into_response(),
        Err(error) => failure(error),
    })
}

#[utoipa::path(post, path = "/metadata/{document_id}/branches/{name}/merge", tag = "metadata/versions",
    security(("bearer_auth" = [])), summary = "Merge a branch",
    description = "Merges a branch into another branch, usually `main`.\n\n**Authentication**: realm bearer token with WRITE on the metadata document.\n\n**Behavior**: a merge into `main` also updates the live metadata document, as a push to main does. When both branches changed the same metadata property to different values, or the same non-metadata file, nothing changes and 409 lists them; edit the branch to the wanted values and merge again. `If-Match` names the expected head of the target branch.",
    params(("document_id" = String, Path, description = "Metadata document ID"),
           ("name" = String, Path, description = "URL-encoded source branch name"),
           ("If-Match" = Option<String>, Header, description = "Expected head version of the target branch")),
    request_body = MergeBranch,
    responses((status = 200, description = "Merged", body = MergeView),
              (status = 400, description = "Target is protected or merged metadata cannot become an ARC"),
              (status = 401, description = "Authentication required"), (status = 403, description = "Access denied"),
              (status = 404, description = "Branch missing"),
              (status = 409, description = "Conflicting properties or files", body = MergeConflictView),
              (status = 412, description = "The target branch moved")))]
pub async fn merge(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((id, name)): Path<(Ulid, String)>,
    headers: HeaderMap,
    Json(request): Json<MergeBranch>,
) -> ServerResult<Response> {
    let auth = require_realm_auth(&state, auth)?;
    let result = merge::merge(
        &state.get_ctx(),
        store(&state)?,
        &auth,
        id,
        &format!("refs/heads/{name}"),
        &request.into,
        WriteOptions {
            message: request.message,
            expected: expected(&headers).as_deref(),
        },
    )
    .await;
    Ok(match result {
        Ok(merged) => Json(merge_view(merged)).into_response(),
        Err(error) => failure(error),
    })
}

#[utoipa::path(get, path = "/metadata/{document_id}/tags", tag = "metadata/versions",
    security(("bearer_auth" = [])), summary = "List dataset tags",
    description = "Lists every tag and the version it names.\n\n**Authentication**: realm bearer token with READ on the metadata document.",
    params(("document_id" = String, Path, description = "Metadata document ID")),
    responses((status = 200, description = "Tags", body = TagList),
              (status = 401, description = "Authentication required"), (status = 403, description = "Access denied"),
              (status = 404, description = "Document missing or not held by this node")))]
pub async fn tags(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(id): Path<Ulid>,
) -> ServerResult<Response> {
    let auth = require_realm_auth(&state, auth)?;
    let result = versions::names(&state.get_ctx(), store(&state)?, &auth, id, true).await;
    Ok(match result {
        Ok(names) => Json(TagList {
            tags: names.into_iter().map(named_view).collect(),
        })
        .into_response(),
        Err(error) => failure(error),
    })
}

#[utoipa::path(post, path = "/metadata/{document_id}/tags", tag = "metadata/versions",
    security(("bearer_auth" = [])), summary = "Create a tag",
    description = "Gives an existing version a fixed name.\n\n**Authentication**: realm bearer token with WRITE on the metadata document.\n\n**Behavior**: tags never move; delete and create again to rename.",
    params(("document_id" = String, Path, description = "Metadata document ID")),
    request_body = CreateTag,
    responses((status = 201, description = "Tag created", body = NamedView),
              (status = 400, description = "Invalid name"),
              (status = 401, description = "Authentication required"), (status = 403, description = "Access denied"),
              (status = 404, description = "Version missing"),
              (status = 409, description = "The tag already exists")))]
pub async fn create_tag(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(id): Path<Ulid>,
    Json(request): Json<CreateTag>,
) -> ServerResult<Response> {
    let auth = require_realm_auth(&state, auth)?;
    let result = versions::change_ref(
        &state.get_ctx(),
        store(&state)?,
        &auth,
        id,
        format!("refs/tags/{}", request.name),
        Some(&request.version),
        None,
    )
    .await;
    Ok(created(result, &request.name))
}

#[utoipa::path(delete, path = "/metadata/{document_id}/tags/{name}", tag = "metadata/versions",
    security(("bearer_auth" = [])), summary = "Delete a tag",
    description = "Deletes a tag; the version it named stays.\n\n**Authentication**: realm bearer token with WRITE on the metadata document.",
    params(("document_id" = String, Path, description = "Metadata document ID"),
           ("name" = String, Path, description = "URL-encoded tag name")),
    responses((status = 204, description = "Tag deleted"),
              (status = 401, description = "Authentication required"), (status = 403, description = "Access denied"),
              (status = 404, description = "Tag missing")))]
pub async fn delete_tag(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((id, name)): Path<(Ulid, String)>,
) -> ServerResult<Response> {
    let auth = require_realm_auth(&state, auth)?;
    let result = versions::change_ref(
        &state.get_ctx(),
        store(&state)?,
        &auth,
        id,
        format!("refs/tags/{name}"),
        None,
        None,
    )
    .await;
    Ok(deleted(result))
}

#[utoipa::path(post, path = "/metadata/{document_id}/conflicts/{id}/merge", tag = "metadata/versions",
    security(("bearer_auth" = [])), summary = "Merge a kept conflict",
    description = "Merges a kept conflict into the branch it lost to and removes it.\n\n**Authentication**: realm bearer token with WRITE on the metadata document.\n\n**Behavior**: works like a branch merge, including 409 for conflicting properties or files.",
    params(("document_id" = String, Path, description = "Metadata document ID"),
           ("id" = String, Path, description = "Conflict ID")),
    responses((status = 200, description = "Merged", body = MergeView),
              (status = 401, description = "Authentication required"), (status = 403, description = "Access denied"),
              (status = 404, description = "Conflict missing"),
              (status = 409, description = "Conflicting properties or files", body = MergeConflictView)))]
pub async fn merge_conflict(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((id, conflict)): Path<(Ulid, Ulid)>,
) -> ServerResult<Response> {
    let auth = require_realm_auth(&state, auth)?;
    let result =
        merge::resolve_conflict(&state.get_ctx(), store(&state)?, &auth, id, conflict).await;
    Ok(match result {
        Ok(merged) => Json(merge_view(merged)).into_response(),
        Err(error) => failure(error),
    })
}

#[utoipa::path(delete, path = "/metadata/{document_id}/conflicts/{id}", tag = "metadata/versions",
    security(("bearer_auth" = [])), summary = "Discard a kept conflict",
    description = "Removes a kept conflict without merging it.\n\n**Authentication**: realm bearer token with WRITE on the metadata document.",
    params(("document_id" = String, Path, description = "Metadata document ID"),
           ("id" = String, Path, description = "Conflict ID")),
    responses((status = 204, description = "Conflict discarded"),
              (status = 401, description = "Authentication required"), (status = 403, description = "Access denied"),
              (status = 404, description = "Conflict missing")))]
pub async fn discard_conflict(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((id, conflict)): Path<(Ulid, Ulid)>,
) -> ServerResult<Response> {
    let auth = require_realm_auth(&state, auth)?;
    let context = state.get_ctx();
    let git = store(&state)?;
    let result = match versions::conflict_ref(&context, git, &auth, id, conflict).await {
        Ok(name) => versions::change_ref(&context, git, &auth, id, name, None, None).await,
        Err(error) => Err(error),
    };
    Ok(deleted(result))
}
