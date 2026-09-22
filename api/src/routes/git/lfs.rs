//! Native Git LFS batch negotiation and streamed object transfers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{base_url, map_error, repository_id};
use crate::auth::require_realm_auth;
use crate::error::{ServerError, ServerResult};
use crate::server::state::ServerState;
use aruna_core::git::LfsObject;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_operations::git::{self, GitError};
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use serde_json::{Value, json};
use std::sync::Arc;

const MEDIA: &str = "application/vnd.git-lfs+json";

#[utoipa::path(post, path = "/git/{repository}/info/lfs/objects/batch", tag = "metadata/git",
    security(("bearer_auth" = []), ("basic_auth" = [])),
    summary = "Negotiate native LFS transfers",
    description = "Negotiates basic Git LFS transfers using SHA-256 identities.\n\n**Authentication**: Aruna bearer token, directly or as an HTTP Basic password, with repository and object-path READ or WRITE.\n\n**Behavior**: individual failures are reported per object.",
    params(("repository" = String, Path, description = "Document ID followed by .git")),
    request_body(content = Value, content_type = "application/vnd.git-lfs+json",
                 example = json!({"operation":"download","transfers":["basic"],"objects":[{"oid":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","size":42}]})),
    responses((status = 200, description = "LFS batch actions", body = Value, content_type = "application/vnd.git-lfs+json",
               example = json!({"transfer":"basic","objects":[{"oid":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","size":42,"error":{"code":404,"message":"object not found"}}]})),
              (status = 401, description = "Authentication required"), (status = 403, description = "Access denied")))]
pub async fn batch(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(repository): Path<String>,
    Json(body): Json<Value>,
) -> ServerResult<Response> {
    let auth = require_realm_auth(&state, auth)?;
    let id = repository_id(&repository)?;
    let operation = body["operation"].as_str().ok_or(ServerError::BadRequest)?;
    let permission = match operation {
        "upload" => Permission::WRITE,
        "download" => Permission::READ,
        _ => return Err(ServerError::BadRequest),
    };
    if body.get("hash_algo").is_some_and(|value| value != "sha256") {
        return Err(ServerError::BadRequest);
    }
    if body.get("transfers").is_some_and(|value| {
        !value
            .as_array()
            .is_some_and(|values| values.iter().any(|value| value == "basic"))
    }) {
        return Err(ServerError::BadRequest);
    }
    git::repository(&state.get_ctx(), &auth, id, permission.clone())
        .await
        .map_err(map_error)?;
    let objects: Vec<LfsObject> =
        serde_json::from_value(body["objects"].clone()).map_err(|_| ServerError::BadRequest)?;
    if objects.len() > 1000 {
        return Err(ServerError::BadRequest);
    }
    let base = base_url(&state, id).await?;
    let mut output = Vec::new();
    for object in objects {
        if !object.valid() || object.size > state.rocrate_limits().import_source_bytes {
            return Err(ServerError::BadRequest);
        }
        let mut item = json!({"oid": object.oid, "size": object.size});
        match git::lfs::inspect(
            &state.get_ctx(),
            &auth,
            state.get_node_id(),
            id,
            &object,
            permission.clone(),
        )
        .await
        {
            Ok(Some(_)) if operation == "upload" => {}
            Ok(None) if operation == "download" => {
                item["error"] = json!({"code":404,"message":"object not found"});
            }
            Ok(_) => {
                item["actions"] =
                    json!({operation: {"href": format!("{base}/info/lfs/objects/{}", object.oid)}});
            }
            Err(error) => {
                let code = match error {
                    GitError::Invalid => 422,
                    other => map_error(other).into_response().status().as_u16(),
                };
                item["error"] = json!({"code":code,"message":"object unavailable"});
            }
        }
        output.push(item);
    }
    Ok((
        [(header::CONTENT_TYPE, MEDIA)],
        Json(json!({"transfer":"basic","objects":output})),
    )
        .into_response())
}

#[utoipa::path(put, path = "/git/{repository}/info/lfs/objects/{oid}", tag = "metadata/git",
    security(("bearer_auth" = []), ("basic_auth" = [])),
    summary = "Upload a native LFS object",
    description = "Uploads and verifies one Git LFS object.\n\n**Authentication**: Aruna bearer token, directly or as an HTTP Basic password, with repository and destination-object WRITE.\n\n**Behavior**: the transfer uses Aruna quota, routing and checksum operations. SHA-256 and Content-Length must match before publication.",
    params(("repository" = String, Path, description = "Document ID followed by .git"), ("oid" = String, Path, description = "Lowercase SHA-256")),
    responses((status = 200, description = "Verified object stored"), (status = 400, description = "Invalid content"),
              (status = 401, description = "Authentication required"), (status = 403, description = "Access denied")))]
pub async fn upload(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((repository, oid)): Path<(String, String)>,
    headers: HeaderMap,
    body: Body,
) -> ServerResult<StatusCode> {
    let auth = require_realm_auth(&state, auth)?;
    let size: u64 = headers
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .ok_or(ServerError::BadRequest)?
        .parse()
        .map_err(|_| ServerError::BadRequest)?;
    if size > state.rocrate_limits().import_source_bytes {
        return Err(ServerError::PayloadTooLarge(
            "LFS object exceeds limit".into(),
        ));
    }
    git::lfs::upload(
        &state.get_ctx(),
        &auth,
        state.get_node_id(),
        repository_id(&repository)?,
        LfsObject { oid, size },
        crate::routes::rocrate_import::upload_body_stream(
            body,
            tokio::time::Instant::now() + std::time::Duration::from_secs(3600),
        ),
    )
    .await
    .map_err(map_error)?;
    Ok(StatusCode::OK)
}

#[utoipa::path(get, path = "/git/{repository}/info/lfs/objects/{oid}", tag = "metadata/git",
    security(("bearer_auth" = []), ("basic_auth" = [])),
    summary = "Download an exact native LFS version",
    description = "Downloads the exact Aruna version bound to an LFS identity.\n\n**Authentication**: Aruna bearer token, directly or as an HTTP Basic password, with repository and source-object READ.\n\n**Behavior**: changing the S3 key head does not change the recorded LFS payload.",
    params(("repository" = String, Path, description = "Document ID followed by .git"), ("oid" = String, Path, description = "Lowercase SHA-256")),
    responses((status = 200, description = "LFS bytes", content_type = "application/octet-stream"),
              (status = 401, description = "Authentication required"), (status = 403, description = "Access denied"),
              (status = 404, description = "Object missing")))]
pub async fn download(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((repository, oid)): Path<(String, String)>,
) -> ServerResult<Response> {
    let auth = require_realm_auth(&state, auth)?;
    let result = git::lfs::download(
        &state.get_ctx(),
        &auth,
        state.get_node_id(),
        repository_id(&repository)?,
        &oid,
    )
    .await
    .map_err(map_error)?;
    Response::builder()
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(header::CONTENT_LENGTH, result.info.size)
        .body(Body::from_stream(result.blob))
        .map_err(|_| ServerError::ServiceUnavailable)
}
