//! Plain JSON views of a dataset's versions, comparisons and kept conflicts.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::map_error;
use crate::auth::require_realm_auth;
use crate::error::{ServerError, ServerResult};
use crate::server::state::ServerState;
use aruna_blob::git::GitStore;
use aruna_core::git::{CommitInfo, FileChange, FileChangeKind};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_operations::git::changes::EntityChangeKind;
use aruna_operations::git::versions::{self, Comparison, Named, Version};
use aruna_operations::git::{GitError, PropertyConflict};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use ulid::Ulid;
use utoipa::{IntoParams, ToSchema};

#[derive(Serialize, ToSchema)]
#[schema(example = json!({"name":"Ada Lovelace","email":"ada@example.org","user_id":null}))]
pub struct Author {
    pub name: String,
    pub email: String,
    /// The Aruna user of a server-side edit or merge.
    pub user_id: Option<String>,
}

#[derive(Serialize, ToSchema)]
#[schema(example = json!({"version":"9f3c2a7112345678901234567890123456789abc","parents":["c71a9e5512345678901234567890123456789abc"],"created_at":"2026-09-25T11:02:00Z","author":{"name":"Aruna","email":"git@aruna.local","user_id":"01M000000000000000000000003"},"message":"Rename the investigation","signed":true,"metadata_event_id":null,"branches":["draft/new-assay"],"tags":[]}))]
pub struct VersionView {
    /// The Git commit ID.
    pub version: String,
    pub parents: Vec<String>,
    pub created_at: String,
    pub author: Author,
    pub message: String,
    /// The commit carries a signature; this API does not verify it.
    pub signed: bool,
    /// Set when the version captures a metadata graph edit.
    pub metadata_event_id: Option<String>,
    pub branches: Vec<String>,
    pub tags: Vec<String>,
}

#[derive(Serialize, ToSchema)]
#[schema(example = json!({"path":"isa.investigation.xlsx","change":"modified"}))]
pub struct FileChangeView {
    pub path: String,
    /// `added`, `modified` or `deleted`.
    pub change: String,
}

#[derive(Serialize, ToSchema)]
#[schema(example = json!({"versions":[{"version":"9f3c2a7112345678901234567890123456789abc","parents":["c71a9e5512345678901234567890123456789abc"],"created_at":"2026-09-25T11:02:00Z","author":{"name":"Ada Lovelace","email":"ada@example.org","user_id":null},"message":"Add assay measurements","signed":true,"metadata_event_id":null,"branches":["main"],"tags":["v1.0"]}],"next_cursor":"9f3c2a7112345678901234567890123456789abc.50"}))]
pub struct VersionList {
    pub versions: Vec<VersionView>,
    /// Pass as `cursor` for the next page; `null` on the last page.
    pub next_cursor: Option<String>,
}

#[derive(Serialize, ToSchema)]
#[schema(example = json!({"version":"9f3c2a7112345678901234567890123456789abc","parents":["c71a9e5512345678901234567890123456789abc"],"created_at":"2026-09-25T11:02:00Z","author":{"name":"Ada Lovelace","email":"ada@example.org","user_id":null},"message":"Add assay measurements","signed":true,"metadata_event_id":null,"branches":["main"],"tags":[],"files":[{"path":"assays/heat/isa.assay.xlsx","change":"added"}]}))]
pub struct VersionDetail {
    #[serde(flatten)]
    pub version: VersionView,
    /// Files changed against the first parent.
    pub files: Vec<FileChangeView>,
}

#[derive(Serialize, ToSchema)]
#[schema(example = json!({"name":"name","before":["Old title"],"after":["New title"]}))]
pub struct PropertyChangeView {
    pub name: String,
    pub before: Vec<Value>,
    pub after: Vec<Value>,
}

#[derive(Serialize, ToSchema)]
#[schema(example = json!({"id":"./","change":"changed","properties":[{"name":"name","before":["Old title"],"after":["New title"]}]}))]
pub struct EntityChangeView {
    pub id: String,
    /// `added`, `removed` or `changed`.
    pub change: String,
    pub properties: Vec<PropertyChangeView>,
}

#[derive(Serialize, ToSchema)]
#[schema(example = json!({"from":"c71a9e5512345678901234567890123456789abc","to":"9f3c2a7112345678901234567890123456789abc","entities":[{"id":"./","change":"changed","properties":[{"name":"name","before":["Old title"],"after":["New title"]}]}],"files":[{"path":"isa.investigation.xlsx","change":"modified"}]}))]
pub struct ComparisonView {
    pub from: String,
    pub to: String,
    /// `null` when either side has no readable ISA metadata.
    pub entities: Option<Vec<EntityChangeView>>,
    pub files: Vec<FileChangeView>,
}

#[derive(Serialize, ToSchema)]
#[schema(example = json!({"name":"v1.0","version":"9f3c2a7112345678901234567890123456789abc"}))]
pub struct NamedView {
    pub name: String,
    pub version: String,
}

#[derive(Serialize, ToSchema)]
#[schema(example = json!({"name":"main","version":"9f3c2a7112345678901234567890123456789abc","protected":true}))]
pub struct BranchView {
    pub name: String,
    pub version: String,
    /// `main` holds the live metadata and `aruna` the graph snapshots.
    pub protected: bool,
}

#[derive(Serialize, ToSchema)]
#[schema(example = json!({"branches":[{"name":"main","version":"9f3c2a7112345678901234567890123456789abc","protected":true},{"name":"draft/new-assay","version":"c71a9e5512345678901234567890123456789abc","protected":false}]}))]
pub struct BranchList {
    pub branches: Vec<BranchView>,
}

#[derive(Serialize, ToSchema)]
#[schema(example = json!({"tags":[{"name":"v1.0","version":"9f3c2a7112345678901234567890123456789abc"}]}))]
pub struct TagList {
    pub tags: Vec<NamedView>,
}

#[derive(Serialize, ToSchema)]
#[schema(example = json!({"id":"01M000000000000000000000004","branch":"main","version":"9f3c2a7112345678901234567890123456789abc"}))]
pub struct ConflictView {
    pub id: String,
    pub branch: String,
    pub version: String,
}

#[derive(Serialize, ToSchema)]
#[schema(example = json!({"conflicts":[{"id":"01M000000000000000000000004","branch":"main","version":"9f3c2a7112345678901234567890123456789abc"}]}))]
pub struct ConflictList {
    pub conflicts: Vec<ConflictView>,
}

#[derive(Serialize, ToSchema)]
#[schema(example = json!({"version":"9f3c2a7112345678901234567890123456789abc","fast_forward":false}))]
pub struct MergeView {
    pub version: String,
    pub fast_forward: bool,
}

#[derive(Serialize, ToSchema)]
#[schema(example = json!({"entity":"./","property":"name","source":["Draft title"],"target":["Main title"]}))]
pub struct PropertyConflictView {
    pub entity: String,
    pub property: String,
    /// The value on the merged branch.
    pub source: Vec<Value>,
    /// The value on the target branch.
    pub target: Vec<Value>,
}

#[derive(Serialize, ToSchema)]
#[schema(example = json!({"files":[],"properties":[{"entity":"./","property":"name","source":["Draft title"],"target":["Main title"]}]}))]
pub struct MergeConflictView {
    pub files: Vec<String>,
    pub properties: Vec<PropertyConflictView>,
}

#[derive(Deserialize, IntoParams)]
pub struct ListQuery {
    /// Branch to list, default `main`.
    pub branch: Option<String>,
    /// Page size, 1 to 200, default 50.
    pub limit: Option<usize>,
    /// `next_cursor` of the previous page.
    pub cursor: Option<String>,
}

#[derive(Deserialize, IntoParams)]
pub struct CompareQuery {
    /// Branch, tag or version.
    pub from: String,
    /// Branch, tag or version.
    pub to: String,
}

pub(super) fn store(state: &ServerState) -> ServerResult<&GitStore> {
    state.git().ok_or(ServerError::ServiceUnavailable)
}

/// The version from `If-Match`, with or without quotes.
pub(super) fn expected(headers: &HeaderMap) -> Option<String> {
    headers
        .get(header::IF_MATCH)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.trim().trim_matches('"').to_string())
}

pub(super) fn failure(error: GitError) -> Response {
    match error {
        GitError::Stale => ServerError::PreconditionFailed(error.to_string()).into_response(),
        GitError::Exists => ServerError::Conflict(error.to_string()).into_response(),
        GitError::Refused(reason) => ServerError::BadRequestMessage(reason).into_response(),
        GitError::MergeConflict(conflict) => (
            StatusCode::CONFLICT,
            Json(MergeConflictView {
                files: conflict.files,
                properties: conflict.properties.into_iter().map(conflict_view).collect(),
            }),
        )
            .into_response(),
        other => map_error(other).into_response(),
    }
}

fn conflict_view(conflict: PropertyConflict) -> PropertyConflictView {
    PropertyConflictView {
        entity: conflict.entity,
        property: conflict.property,
        source: conflict.source,
        target: conflict.target,
    }
}

fn change_name(change: FileChangeKind) -> String {
    match change {
        FileChangeKind::Added => "added",
        FileChangeKind::Modified => "modified",
        FileChangeKind::Deleted => "deleted",
    }
    .to_string()
}

pub(super) fn files_view(files: Vec<FileChange>) -> Vec<FileChangeView> {
    files
        .into_iter()
        .map(|file| FileChangeView {
            path: file.path,
            change: change_name(file.change),
        })
        .collect()
}

pub(super) fn version_view(version: Version) -> VersionView {
    let Version {
        commit,
        user_id,
        metadata_event_id,
        branches,
        tags,
    } = version;
    let CommitInfo {
        commit,
        parents,
        author_name,
        author_email,
        authored_at_s,
        message,
        signed,
    } = commit;
    // Aruna trailers are exposed as fields, not repeated in the message.
    let message = message
        .lines()
        .filter(|line| !line.starts_with("Aruna-User:") && !line.starts_with("Aruna-Revision:"))
        .collect::<Vec<_>>()
        .join("\n")
        .trim()
        .to_string();
    VersionView {
        version: commit,
        parents,
        created_at: chrono::DateTime::from_timestamp(authored_at_s, 0)
            .unwrap_or_default()
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        author: Author {
            name: author_name,
            email: author_email,
            user_id,
        },
        message,
        signed,
        metadata_event_id: metadata_event_id.map(|id| id.to_string()),
        branches,
        tags,
    }
}

pub(super) fn named_view(named: Named) -> NamedView {
    NamedView {
        name: named.name,
        version: named.version,
    }
}

fn comparison_view(comparison: Comparison) -> ComparisonView {
    ComparisonView {
        from: comparison.from,
        to: comparison.to,
        entities: comparison.entities.map(|entities| {
            entities
                .into_iter()
                .map(|entity| EntityChangeView {
                    id: entity.id,
                    change: match entity.change {
                        EntityChangeKind::Added => "added",
                        EntityChangeKind::Removed => "removed",
                        EntityChangeKind::Changed => "changed",
                    }
                    .to_string(),
                    properties: entity
                        .properties
                        .into_iter()
                        .map(|property| PropertyChangeView {
                            name: property.name,
                            before: property.before,
                            after: property.after,
                        })
                        .collect(),
                })
                .collect()
        }),
        files: files_view(comparison.files),
    }
}

#[utoipa::path(get, path = "/metadata/{document_id}/versions", tag = "metadata/versions",
    security(("bearer_auth" = [])), summary = "List dataset versions",
    description = "Lists the versions of one branch of the dataset's ARC history, newest first.\n\n**Authentication**: realm bearer token with READ on the metadata document.\n\n**Behavior**: a version is one commit. `main` holds the live metadata; other branches are drafts. The cursor pins the branch head the first page started from.",
    params(("document_id" = String, Path, description = "Metadata document ID"), ListQuery),
    responses((status = 200, description = "One page of versions", body = VersionList),
              (status = 400, description = "Invalid branch or cursor"),
              (status = 401, description = "Authentication required"), (status = 403, description = "Access denied"),
              (status = 404, description = "Document or branch missing, or not held by this node"),
              (status = 503, description = "Git storage unavailable")))]
pub async fn list(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(id): Path<Ulid>,
    Query(query): Query<ListQuery>,
) -> ServerResult<Response> {
    let auth = require_realm_auth(&state, auth)?;
    let branch = query.branch.unwrap_or_else(|| "main".into());
    let limit = query.limit.unwrap_or(50);
    let result = versions::list(
        &state.get_ctx(),
        store(&state)?,
        &auth,
        id,
        &branch,
        query.cursor.as_deref(),
        limit,
    )
    .await;
    Ok(match result {
        Ok((list, next_cursor)) => Json(VersionList {
            versions: list.into_iter().map(version_view).collect(),
            next_cursor,
        })
        .into_response(),
        Err(error) => failure(error),
    })
}

#[utoipa::path(get, path = "/metadata/{document_id}/versions/{version}", tag = "metadata/versions",
    security(("bearer_auth" = [])), summary = "Get one dataset version",
    description = "Returns one version and the files it changed against its first parent.\n\n**Authentication**: realm bearer token with READ on the metadata document.",
    params(("document_id" = String, Path, description = "Metadata document ID"),
           ("version" = String, Path, description = "Version, branch or tag")),
    responses((status = 200, description = "The version", body = VersionDetail),
              (status = 401, description = "Authentication required"), (status = 403, description = "Access denied"),
              (status = 404, description = "Version missing"),
              (status = 503, description = "Git storage unavailable")))]
pub async fn show(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((id, revision)): Path<(Ulid, String)>,
) -> ServerResult<Response> {
    let auth = require_realm_auth(&state, auth)?;
    let result = versions::show(&state.get_ctx(), store(&state)?, &auth, id, &revision).await;
    Ok(match result {
        Ok((version, files)) => Json(VersionDetail {
            version: version_view(version),
            files: files_view(files),
        })
        .into_response(),
        Err(error) => failure(error),
    })
}

#[utoipa::path(get, path = "/metadata/{document_id}/versions/{version}/rocrate", tag = "metadata/versions",
    security(("bearer_auth" = [])), summary = "Get the RO-Crate of a version",
    description = "Returns the ISA-derived RO-Crate metadata of one version.\n\n**Authentication**: realm bearer token with READ on the metadata document.",
    params(("document_id" = String, Path, description = "Metadata document ID"),
           ("version" = String, Path, description = "Version, branch or tag")),
    responses((status = 200, description = "Exact commit and derived RO-Crate", body = Value,
               example = json!({"commit":"1111111111111111111111111111111111111111","rocrate":{"@context":"https://w3id.org/ro/crate/1.2/context","@graph":[{"@id":"ro-crate-metadata.json","@type":"CreativeWork","about":{"@id":"./"},"conformsTo":{"@id":"https://w3id.org/ro/crate/1.2"}},{"@id":"./","@type":"Dataset","additionalType":"Investigation","identifier":"arc-example","name":"ARC example","description":"Example investigation","datePublished":"2026-09-22"}]}})),
              (status = 400, description = "The version has invalid ISA metadata"),
              (status = 401, description = "Authentication required"), (status = 403, description = "Access denied"),
              (status = 503, description = "Git storage or converter unavailable")))]
pub async fn rocrate(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((id, revision)): Path<(Ulid, String)>,
) -> ServerResult<Response> {
    let auth = require_realm_auth(&state, auth)?;
    let result = aruna_operations::git::snapshot::export(
        &state.get_ctx(),
        store(&state)?,
        &auth,
        id,
        revision,
    )
    .await;
    Ok(match result {
        Ok(bytes) => {
            let value: Value =
                serde_json::from_slice(&bytes).map_err(|_| ServerError::ServiceUnavailable)?;
            Json(value).into_response()
        }
        Err(error) => failure(error),
    })
}

#[utoipa::path(get, path = "/metadata/{document_id}/compare", tag = "metadata/versions",
    security(("bearer_auth" = [])), summary = "Compare two dataset versions",
    description = "Lists the metadata entities and files that differ between two versions.\n\n**Authentication**: realm bearer token with READ on the metadata document.\n\n**Behavior**: entities are compared in the ISA-derived RO-Crate of each version; property values compare as sets.",
    params(("document_id" = String, Path, description = "Metadata document ID"), CompareQuery),
    responses((status = 200, description = "Differences", body = ComparisonView),
              (status = 401, description = "Authentication required"), (status = 403, description = "Access denied"),
              (status = 404, description = "A version is missing"),
              (status = 503, description = "Git storage unavailable")))]
pub async fn compare(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(id): Path<Ulid>,
    Query(query): Query<CompareQuery>,
) -> ServerResult<Response> {
    let auth = require_realm_auth(&state, auth)?;
    let result = versions::compare(
        &state.get_ctx(),
        store(&state)?,
        &auth,
        id,
        &query.from,
        &query.to,
    )
    .await;
    Ok(match result {
        Ok(comparison) => Json(comparison_view(comparison)).into_response(),
        Err(error) => failure(error),
    })
}

#[utoipa::path(get, path = "/metadata/{document_id}/conflicts", tag = "metadata/versions",
    security(("bearer_auth" = [])), summary = "List kept conflicts",
    description = "Lists branch updates that lost a race between holders. They are kept instead of dropped.\n\n**Authentication**: realm bearer token with READ on the metadata document.",
    params(("document_id" = String, Path, description = "Metadata document ID")),
    responses((status = 200, description = "Kept conflicts", body = ConflictList),
              (status = 401, description = "Authentication required"), (status = 403, description = "Access denied"),
              (status = 404, description = "Document missing or not held by this node")))]
pub async fn conflicts(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(id): Path<Ulid>,
) -> ServerResult<Response> {
    let auth = require_realm_auth(&state, auth)?;
    let result = versions::conflicts(&state.get_ctx(), store(&state)?, &auth, id).await;
    Ok(match result {
        Ok(conflicts) => Json(ConflictList {
            conflicts: conflicts
                .into_iter()
                .map(|conflict| ConflictView {
                    id: conflict.id.to_string(),
                    branch: conflict.branch,
                    version: conflict.version,
                })
                .collect(),
        })
        .into_response(),
        Err(error) => failure(error),
    })
}
