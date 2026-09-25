//! Native Git repository setup and transport authentication.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod lfs;
mod locks;
mod snapshot;
mod transport;
mod versions;

use crate::auth::{map_authorize_error, require_realm_auth};
use crate::error::{ServerError, ServerResult};
use crate::server::state::ServerState;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_operations::git::{self, GitError};
use axum::Extension;
use axum::extract::{Path, Request, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::middleware::Next;
use axum::response::Response;
use base64::Engine;
use std::sync::Arc;
use ulid::Ulid;
use utoipa_axum::{router::OpenApiRouter, routes};

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new()
        .routes(routes!(push))
        .routes(routes!(locks::create, locks::list))
        .routes(routes!(locks::verify))
        .routes(routes!(locks::unlock))
        .routes(routes!(snapshot::repository_status))
        .routes(routes!(snapshot::export_revision))
        .routes(routes!(transport::advertise))
        .routes(routes!(transport::rpc))
        .routes(routes!(lfs::batch))
        .routes(routes!(lfs::upload, lfs::download))
        .routes(routes!(versions::list))
        .routes(routes!(versions::show))
        .routes(routes!(versions::rocrate))
        .routes(routes!(versions::compare))
        .routes(routes!(versions::conflicts))
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
        GitError::Unavailable | GitError::Full => ServerError::ServiceUnavailable,
        GitError::NotHolder => ServerError::NotFound,
        GitError::Locked(path) => {
            ServerError::Conflict(format!("{path} is locked by another user"))
        }
        GitError::Stale => ServerError::PreconditionFailed(error.to_string()),
        GitError::Exists | GitError::MergeConflict(_) => ServerError::Conflict(error.to_string()),
        GitError::Refused(reason) => ServerError::BadRequestMessage(reason),
    }
}

async fn api_url(state: &ServerState) -> ServerResult<String> {
    let rest = state
        .interface_state()
        .await
        .rest
        .ok_or(ServerError::ServiceUnavailable)?;
    Ok(rest.api_base_url)
}

async fn base_url(state: &ServerState, id: Ulid) -> ServerResult<String> {
    Ok(format!("{}/git/{id}.git", api_url(state).await?))
}

#[utoipa::path(post, path = "/metadata/{document_id}/git/push", tag = "metadata/git",
    security(("bearer_auth" = [])),
    summary = "Record a validated native Git push",
    description = "Stores the objects of one push and publishes its ref updates to every holder of the document.\n\n**Authentication**: realm bearer token with WRITE on the document.\n\n**Behavior**: called by the node's own receive hook before Git moves refs. The body is a four-byte big-endian length, a JSON object with `refs`, `lfs` and `paths`, then the Git pack. Paths locked by another user or unknown LFS objects refuse the push.",
    params(("document_id" = String, Path, description = "Metadata document ID")),
    request_body(content = Vec<u8>, content_type = "application/x-aruna-git-push"),
    responses((status = 204, description = "Push recorded"), (status = 400, description = "Malformed push"),
              (status = 401, description = "Authentication required"), (status = 403, description = "Access denied"),
              (status = 404, description = "Document missing or not held by this node"),
              (status = 409, description = "A changed path is locked by another user"),
              (status = 503, description = "Records or storage unavailable")))]
pub async fn push(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(id): Path<Ulid>,
    body: axum::body::Bytes,
) -> ServerResult<StatusCode> {
    let auth = require_realm_auth(&state, auth)?;
    let (request, pack) = git::push::decode(body).map_err(map_error)?;
    git::push::accept(&state.get_ctx(), &auth, id, request, pack)
        .await
        .map_err(map_error)?;
    Ok(StatusCode::NO_CONTENT)
}
