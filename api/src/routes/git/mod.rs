//! Native Git repository setup and transport authentication.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod lfs;
mod snapshot;
mod transport;

use crate::auth::{map_authorize_error, require_realm_auth};
use crate::error::{ServerError, ServerResult};
use crate::server::state::ServerState;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_operations::git::{self, GitError};
use axum::extract::{Path, Request, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::middleware::Next;
use axum::response::Response;
use axum::{Extension, Json};
use base64::Engine;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use ulid::Ulid;
use utoipa::ToSchema;
use utoipa_axum::{router::OpenApiRouter, routes};

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new()
        .routes(routes!(create_repository))
        .routes(routes!(snapshot::repository_status))
        .routes(routes!(snapshot::export_revision))
        .routes(routes!(transport::advertise))
        .routes(routes!(transport::rpc))
        .routes(routes!(lfs::batch))
        .routes(routes!(lfs::upload, lfs::download))
}

pub async fn credentials(mut request: Request, next: Next) -> Response {
    let native = request.uri().path().starts_with("/git/")
        || request.uri().path().starts_with("/api/v1/git/");
    if native {
        let value = request
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Basic "))
            .and_then(|value| base64::engine::general_purpose::STANDARD.decode(value).ok())
            .and_then(|value| String::from_utf8(value).ok())
            .and_then(|value| value.split_once(':').map(|(_, token)| token.to_string()))
            .and_then(|token| HeaderValue::from_str(&format!("Bearer {token}")).ok());
        if let Some(value) = value {
            request.headers_mut().insert(header::AUTHORIZATION, value);
        }
    }
    let mut response = next.run(request).await;
    if native && response.status() == StatusCode::UNAUTHORIZED {
        response.headers_mut().insert(
            header::WWW_AUTHENTICATE,
            HeaderValue::from_static("Basic realm=\"Aruna Git\""),
        );
    }
    response
}

fn repository_id(value: &str) -> ServerResult<Ulid> {
    value
        .strip_suffix(".git")
        .ok_or(ServerError::BadRequest)?
        .parse()
        .map_err(|_| ServerError::BadRequest)
}

fn map_error(error: GitError) -> ServerError {
    match error {
        GitError::NotFound => ServerError::NotFound,
        GitError::Invalid => ServerError::BadRequest,
        GitError::Conflict => ServerError::Conflict("repository binding differs".into()),
        GitError::Authorization(error) => map_authorize_error(error),
        GitError::Unavailable => ServerError::ServiceUnavailable,
    }
}

async fn base_url(state: &ServerState, id: Ulid) -> ServerResult<String> {
    let rest = state
        .interface_state()
        .await
        .rest
        .ok_or(ServerError::ServiceUnavailable)?;
    Ok(format!("{}/git/{id}.git", rest.api_base_url))
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CreateRepository {
    pub bucket: String,
    #[serde(default = "arc_enabled")]
    pub arc: bool,
}

fn arc_enabled() -> bool {
    true
}

#[derive(Serialize, ToSchema)]
pub struct RepositoryResponse {
    pub document_id: String,
    pub clone_url: String,
    pub lfs_url: String,
    pub arc: bool,
}

#[utoipa::path(post, path = "/metadata/{document_id}/git", tag = "metadata/git",
    security(("bearer_auth" = [])),
    summary = "Bind an ARC repository to LFS storage",
    description = "Binds a repository to an explicit same-group LFS bucket.\n\n**Authentication**: realm bearer token with WRITE on the document and bucket.\n\n**Behavior**: automatic repositories already have an immutable binding. ARC validation cannot be disabled. Normal clients discover the automatic repository with GET on this route.",
    params(("document_id" = String, Path, description = "Existing crate document ID")),
    request_body(content = CreateRepository, example = serde_json::json!({"bucket":"arc-storage","arc":true})),
    responses((status = 200, description = "Repository enabled", body = RepositoryResponse,
               example = serde_json::json!({"document_id":"01M000000000000000000000000","clone_url":"https://node.example/api/v1/git/01M000000000000000000000000.git","lfs_url":"https://node.example/api/v1/git/01M000000000000000000000000.git/info/lfs","arc":true})),
              (status = 401, description = "Authentication required"), (status = 403, description = "Access denied"),
              (status = 404, description = "Document or bucket missing"), (status = 409, description = "Different binding exists")))]
pub async fn create_repository(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(id): Path<Ulid>,
    Json(request): Json<CreateRepository>,
) -> ServerResult<Json<RepositoryResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let record = git::create(
        &state.get_ctx(),
        state.git().ok_or(ServerError::ServiceUnavailable)?,
        &auth,
        state.get_node_id(),
        id,
        request.bucket,
        request.arc,
    )
    .await
    .map_err(map_error)?;
    let clone_url = base_url(&state, id).await?;
    Ok(Json(RepositoryResponse {
        document_id: id.to_string(),
        lfs_url: format!("{clone_url}/info/lfs"),
        clone_url,
        arc: record.arc,
    }))
}
