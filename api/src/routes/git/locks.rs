//! Git LFS file locking API backed by replicated lock claims.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{map_error, repository_id};
use crate::auth::require_realm_auth;
use crate::error::{ServerError, ServerResult};
use crate::server::state::ServerState;
use aruna_core::git::LfsLock;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_operations::git::{self, locks::LockOutcome};
use axum::extract::{Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::Arc;
use ulid::Ulid;

const MEDIA: &str = "application/vnd.git-lfs+json";

fn view(lock: &LfsLock) -> Value {
    let locked_at = chrono::DateTime::from_timestamp_millis(
        i64::try_from(lock.locked_at_ms).unwrap_or(i64::MAX),
    )
    .unwrap_or_default()
    .to_rfc3339();
    json!({"id": lock.id.to_string(), "path": lock.path, "locked_at": locked_at,
           "owner": {"name": lock.user_id.user_ulid.to_string()}})
}

fn reply(status: StatusCode, body: Value) -> Response {
    (status, [(header::CONTENT_TYPE, MEDIA)], Json(body)).into_response()
}

#[derive(Deserialize)]
pub struct CreateLock {
    pub path: String,
}

#[utoipa::path(post, path = "/git/{repository}/info/lfs/locks", tag = "metadata/git",
    security(("bearer_auth" = []), ("basic_auth" = [])),
    summary = "Lock an LFS file",
    description = "Claims a file lock as defined by the Git LFS locking API.\n\n**Authentication**: Aruna bearer token, directly or as an HTTP Basic password, with WRITE on the document.\n\n**Behavior**: the claim replicates to every holder; the earliest claim on a path wins everywhere. A claim made at the same moment on another holder can still win after it replicates.",
    params(("repository" = String, Path, description = "Document ID followed by .git")),
    request_body(content = Value, content_type = "application/vnd.git-lfs+json", example = json!({"path":"assays/assay/dataset/measurements.bin"})),
    responses((status = 201, description = "Lock created", body = Value, content_type = "application/vnd.git-lfs+json",
               example = json!({"lock":{"id":"01M000000000000000000000002","path":"assays/assay/dataset/measurements.bin","locked_at":"2026-09-25T10:00:00+00:00","owner":{"name":"01M000000000000000000000003"}}})),
              (status = 401, description = "Authentication required"), (status = 403, description = "Access denied"),
              (status = 409, description = "Path is locked by another user", body = Value, content_type = "application/vnd.git-lfs+json")))]
pub async fn create(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(repository): Path<String>,
    Json(request): Json<CreateLock>,
) -> ServerResult<Response> {
    let auth = require_realm_auth(&state, auth)?;
    let id = repository_id(&repository)?;
    Ok(
        match git::locks::create(&state.get_ctx(), &auth, id, request.path)
            .await
            .map_err(map_error)?
        {
            LockOutcome::Locked(lock) => reply(StatusCode::CREATED, json!({"lock": view(&lock)})),
            LockOutcome::Taken(lock) => reply(
                StatusCode::CONFLICT,
                json!({"lock": view(&lock), "message": "already created lock"}),
            ),
        },
    )
}

#[derive(Deserialize)]
pub struct LockFilter {
    pub path: Option<String>,
    pub id: Option<String>,
}

#[utoipa::path(get, path = "/git/{repository}/info/lfs/locks", tag = "metadata/git",
    security(("bearer_auth" = []), ("basic_auth" = [])),
    summary = "List LFS file locks",
    description = "Lists current file locks as defined by the Git LFS locking API.\n\n**Authentication**: Aruna bearer token, directly or as an HTTP Basic password, with READ on the document.\n\n**Behavior**: results come from this holder's replicated lock state in one page.",
    params(("repository" = String, Path, description = "Document ID followed by .git"),
           ("path" = Option<String>, Query, description = "Only the lock on this path"),
           ("id" = Option<String>, Query, description = "Only the lock with this ID")),
    responses((status = 200, description = "Locks", body = Value, content_type = "application/vnd.git-lfs+json",
               example = json!({"locks":[{"id":"01M000000000000000000000002","path":"assays/assay/dataset/measurements.bin","locked_at":"2026-09-25T10:00:00+00:00","owner":{"name":"01M000000000000000000000003"}}],"next_cursor":null})),
              (status = 401, description = "Authentication required"), (status = 403, description = "Access denied")))]
pub async fn list(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(repository): Path<String>,
    Query(filter): Query<LockFilter>,
) -> ServerResult<Response> {
    let auth = require_realm_auth(&state, auth)?;
    let locks = git::locks::list(&state.get_ctx(), &auth, repository_id(&repository)?)
        .await
        .map_err(map_error)?;
    let locks: Vec<_> = locks
        .iter()
        .filter(|lock| filter.path.as_ref().is_none_or(|path| *path == lock.path))
        .filter(|lock| {
            filter
                .id
                .as_ref()
                .is_none_or(|id| *id == lock.id.to_string())
        })
        .map(view)
        .collect();
    Ok(reply(
        StatusCode::OK,
        json!({"locks": locks, "next_cursor": null}),
    ))
}

#[utoipa::path(post, path = "/git/{repository}/info/lfs/locks/verify", tag = "metadata/git",
    security(("bearer_auth" = []), ("basic_auth" = [])),
    summary = "Verify LFS file locks before a push",
    description = "Splits current locks into the caller's and other users' as defined by the Git LFS locking API.\n\n**Authentication**: Aruna bearer token, directly or as an HTTP Basic password, with WRITE on the document.\n\n**Behavior**: a push that changes a file locked by another user is refused by the server as well.",
    params(("repository" = String, Path, description = "Document ID followed by .git")),
    request_body(content = Value, content_type = "application/vnd.git-lfs+json", example = json!({"ref":{"name":"refs/heads/main"}})),
    responses((status = 200, description = "Own and other locks", body = Value, content_type = "application/vnd.git-lfs+json",
               example = json!({"ours":[],"theirs":[],"next_cursor":null})),
              (status = 401, description = "Authentication required"), (status = 403, description = "Access denied")))]
pub async fn verify(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(repository): Path<String>,
) -> ServerResult<Response> {
    let auth = require_realm_auth(&state, auth)?;
    let id = repository_id(&repository)?;
    git::repository(
        &state.get_ctx(),
        &auth,
        id,
        aruna_core::structs::identity::auth::Permission::WRITE,
    )
    .await
    .map_err(map_error)?;
    let locks = git::locks::list(&state.get_ctx(), &auth, id)
        .await
        .map_err(map_error)?;
    let (ours, theirs): (Vec<_>, Vec<_>) =
        locks.iter().partition(|lock| lock.user_id == auth.user_id);
    let ours: Vec<_> = ours.into_iter().map(view).collect();
    let theirs: Vec<_> = theirs.into_iter().map(view).collect();
    Ok(reply(
        StatusCode::OK,
        json!({"ours": ours, "theirs": theirs, "next_cursor": null}),
    ))
}

#[derive(Deserialize, Default)]
pub struct Unlock {
    #[serde(default)]
    pub force: bool,
}

#[utoipa::path(post, path = "/git/{repository}/info/lfs/locks/{id}/unlock", tag = "metadata/git",
    security(("bearer_auth" = []), ("basic_auth" = [])),
    summary = "Release an LFS file lock",
    description = "Releases a file lock as defined by the Git LFS locking API.\n\n**Authentication**: Aruna bearer token, directly or as an HTTP Basic password, with WRITE on the document.\n\n**Behavior**: the owner releases a lock; another writer needs `force`.",
    params(("repository" = String, Path, description = "Document ID followed by .git"), ("id" = String, Path, description = "Lock ID")),
    request_body(content = Value, content_type = "application/vnd.git-lfs+json", example = json!({"force":false})),
    responses((status = 200, description = "Lock released", body = Value, content_type = "application/vnd.git-lfs+json"),
              (status = 401, description = "Authentication required"), (status = 403, description = "Access denied"),
              (status = 404, description = "No such lock"), (status = 409, description = "Owned by another user; use force")))]
pub async fn unlock(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((repository, lock)): Path<(String, String)>,
    body: Option<Json<Unlock>>,
) -> ServerResult<Response> {
    let auth = require_realm_auth(&state, auth)?;
    let lock: Ulid = lock.parse().map_err(|_| ServerError::BadRequest)?;
    let force = body.is_some_and(|Json(body)| body.force);
    let released = git::locks::unlock(
        &state.get_ctx(),
        &auth,
        repository_id(&repository)?,
        lock,
        force,
    )
    .await
    .map_err(map_error)?;
    Ok(reply(StatusCode::OK, json!({"lock": view(&released)})))
}
