//! Maps Git smart HTTP requests to the authorized native repository operation.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{base_url, map_error, repository_id};
use crate::auth::{ValidatedBearer, require_realm_auth};
use crate::error::{ServerError, ServerResult};
use crate::server::state::ServerState;
use aruna_core::git::{GitEvent, GitRequest, MAX_GIT_BYTES};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_operations::git;
use axum::Extension;
use axum::body::{Body, Bytes, to_bytes};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::Response;
use serde::Deserialize;
use std::sync::Arc;

#[derive(Deserialize)]
pub struct ServiceQuery {
    service: String,
}

#[utoipa::path(get, path = "/git/{repository}/info/refs", tag = "metadata/git",
    security(("bearer_auth" = []), ("basic_auth" = [])),
    summary = "Advertise native Git refs",
    description = "Advertises refs for native Git fetch or push.\n\n**Authentication**: Aruna bearer token, directly or as an HTTP Basic password; READ is required for fetch and WRITE for push.\n\n**Behavior**: a missing repository is initialized from the document's metadata on its fixed owner.",
    params(("repository" = String, Path, description = "Document ID followed by .git"),
           ("service" = String, Query, description = "git-upload-pack or git-receive-pack")),
    responses((status = 200, description = "Git ref advertisement", content_type = "application/x-git-upload-pack-advertisement"),
              (status = 401, description = "Authentication required"), (status = 403, description = "Access denied")))]
pub async fn advertise(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(token): Extension<Option<ValidatedBearer>>,
    Path(repository): Path<String>,
    Query(query): Query<ServiceQuery>,
    headers: HeaderMap,
) -> ServerResult<Response> {
    if !matches!(
        query.service.as_str(),
        "git-upload-pack" | "git-receive-pack"
    ) {
        return Err(ServerError::BadRequest);
    }
    let auth = require_realm_auth(&state, auth)?;
    git::snapshot::ensure(
        &state.get_ctx(),
        &auth,
        repository_id(&repository)?,
        if query.service == "git-receive-pack" {
            Permission::WRITE
        } else {
            Permission::READ
        },
    )
    .await
    .map_err(map_error)?;
    serve(
        state,
        Some(auth),
        token,
        repository,
        "GET",
        "info/refs".into(),
        format!("service={}", query.service),
        headers,
        Bytes::new(),
    )
    .await
}

#[utoipa::path(post, path = "/git/{repository}/{service}", tag = "metadata/git",
    security(("bearer_auth" = []), ("basic_auth" = [])),
    summary = "Execute native Git fetch or push",
    description = "Executes the requested Git smart HTTP service.\n\n**Authentication**: Aruna bearer token, directly or as an HTTP Basic password; upload-pack requires READ and receive-pack requires WRITE.\n\n**Behavior**: ARC validation and LFS availability checks precede ref publication. Atomic pushes use Git's native transaction support.",
    params(("repository" = String, Path, description = "Document ID followed by .git"),
           ("service" = String, Path, description = "git-upload-pack or git-receive-pack")),
    responses((status = 200, description = "Git protocol result", content_type = "application/x-git-upload-pack-result"),
              (status = 401, description = "Authentication required"), (status = 403, description = "Access denied")))]
pub async fn rpc(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(token): Extension<Option<ValidatedBearer>>,
    Path((repository, service)): Path<(String, String)>,
    headers: HeaderMap,
    body: Body,
) -> ServerResult<Response> {
    if !matches!(service.as_str(), "git-upload-pack" | "git-receive-pack") {
        return Err(ServerError::NotFound);
    }
    let auth = require_realm_auth(&state, auth)?;
    git::repository(
        &state.get_ctx(),
        &auth,
        repository_id(&repository)?,
        if service == "git-receive-pack" {
            Permission::WRITE
        } else {
            Permission::READ
        },
    )
    .await
    .map_err(map_error)?;
    let body = to_bytes(body, MAX_GIT_BYTES)
        .await
        .map_err(|_| ServerError::PayloadTooLarge("Git request exceeds limit".into()))?;
    serve(
        state,
        Some(auth),
        token,
        repository,
        "POST",
        service,
        String::new(),
        headers,
        body,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn serve(
    state: Arc<ServerState>,
    auth: Option<AuthContext>,
    token: Option<ValidatedBearer>,
    repository: String,
    method: &str,
    action: String,
    query: String,
    headers: HeaderMap,
    body: Bytes,
) -> ServerResult<Response> {
    let auth = require_realm_auth(&state, auth)?;
    let id = repository_id(&repository)?;
    let permission = if action == "git-receive-pack" || query == "service=git-receive-pack" {
        Permission::WRITE
    } else {
        Permission::READ
    };
    let repository = git::repository(&state.get_ctx(), &auth, id, permission)
        .await
        .map_err(map_error)?;
    let request = GitRequest {
        repository,
        method: method.into(),
        action,
        query,
        content_type: headers
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("")
            .into(),
        content_encoding: headers
            .get("content-encoding")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("")
            .into(),
        protocol: headers
            .get("git-protocol")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("")
            .into(),
        body,
        token: token.ok_or(ServerError::Unauthorized)?.as_str().to_string(),
        lfs_url: format!("{}/info/lfs/objects/batch", base_url(&state, id).await?),
    };
    let result = git::transport(
        &state.get_ctx(),
        state.git().ok_or(ServerError::ServiceUnavailable)?,
        &auth,
        request,
    )
    .await
    .map_err(map_error)?;
    let GitEvent::Response {
        status,
        headers,
        body,
    } = result
    else {
        return Err(ServerError::ServiceUnavailable);
    };
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = StatusCode::from_u16(status).map_err(|_| ServerError::BadGateway)?;
    for (key, value) in headers {
        response.headers_mut().insert(
            HeaderName::try_from(key).map_err(|_| ServerError::BadGateway)?,
            HeaderValue::try_from(value).map_err(|_| ServerError::BadGateway)?,
        );
    }
    Ok(response)
}
