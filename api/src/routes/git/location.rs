//! Reads and chooses where a dataset's files are stored.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::map_error;
use crate::auth::require_realm_auth;
use crate::error::{ServerError, ServerResult};
use crate::server::state::ServerState;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::dataset_location::DatasetLocation;
use aruna_operations::git::GitError;
use aruna_operations::git::location;
use axum::extract::{Path, State};
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use ulid::Ulid;
use utoipa::ToSchema;

/// The effective storage location of a dataset.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct StorageLocation {
    pub bucket: String,
    /// Key prefix without a leading `/`, ending in `/` unless empty.
    pub prefix: String,
    /// `true` while no location was chosen: bucket `datasets-<group id>`, prefix `<document id>/`.
    pub default: bool,
}

impl StorageLocation {
    pub fn new((location, default): (DatasetLocation, bool)) -> Self {
        Self {
            bucket: location.bucket,
            prefix: location.prefix,
            default,
        }
    }
}

/// A chosen storage location. The prefix is normalized: no leading `/`, one trailing `/`.
#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct StorageLocationRequest {
    pub bucket: String,
    pub prefix: String,
}

impl StorageLocationRequest {
    /// The checked location, or 400 with the rule the input breaks.
    pub fn location(&self) -> ServerResult<DatasetLocation> {
        DatasetLocation::new(&self.bucket, &self.prefix)
            .map_err(|error| ServerError::BadRequestMessage(error.to_string()))
    }
}

#[utoipa::path(get, path = "/metadata/{document_id}/storage-location", tag = "metadata/git",
    security(("bearer_auth" = [])), summary = "Get the dataset storage location",
    description = "Returns where the dataset's files are stored.\n\n**Authentication**: realm bearer token with READ on the metadata document.\n\n**Behavior**: files pushed through Git are stored at `<prefix><repository path>` in this bucket. Without a chosen location the default is bucket `datasets-<group id in lowercase>` with prefix `<document id>/`, and `default` is `true`. The choice replicates to every holder of the document.",
    params(("document_id" = String, Path, description = "Metadata document ID")),
    responses((status = 200, description = "Effective storage location", body = StorageLocation,
               example = json!({"bucket":"datasets-01jabcdef0123456789abcdefg","prefix":"01M000000000000000000000000/","default":true})),
              (status = 401, description = "Authentication required"), (status = 403, description = "Access denied"),
              (status = 404, description = "Document missing or not held by this node"),
              (status = 503, description = "Metadata storage unavailable")))]
pub async fn get_location(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(id): Path<Ulid>,
) -> ServerResult<Json<StorageLocation>> {
    let auth = require_realm_auth(&state, auth)?;
    let effective = location::get(&state.get_ctx(), &auth, id)
        .await
        .map_err(map_error)?;
    Ok(Json(StorageLocation::new(effective)))
}

#[utoipa::path(put, path = "/metadata/{document_id}/storage-location", tag = "metadata/git",
    security(("bearer_auth" = [])), summary = "Choose the dataset storage location",
    description = "Chooses where files pushed through Git are stored.\n\n**Authentication**: realm bearer token with WRITE on the metadata document and WRITE on the bucket under the prefix.\n\n**Behavior**: the prefix is normalized (no leading `/`, a trailing `/` is added) and must not contain empty, `.` or `..` segments. The bucket must exist on this node, except the default `datasets-<group id>` bucket, which is created on first use. Objects already stored stay where they are; later pushes store new or changed files in the new location. The choice replicates to every holder of the document.",
    params(("document_id" = String, Path, description = "Metadata document ID")),
    request_body(content = StorageLocationRequest, example = json!({"bucket":"lab-data","prefix":"projects/liver/"})),
    responses((status = 200, description = "The chosen storage location", body = StorageLocation,
               example = json!({"bucket":"lab-data","prefix":"projects/liver/","default":false})),
              (status = 400, description = "Invalid bucket name or prefix"),
              (status = 401, description = "Authentication required"), (status = 403, description = "Access denied"),
              (status = 404, description = "Document or bucket missing, or the document is not held by this node"),
              (status = 503, description = "Metadata storage unavailable")))]
pub async fn put_location(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(id): Path<Ulid>,
    Json(request): Json<StorageLocationRequest>,
) -> ServerResult<Json<StorageLocation>> {
    let auth = require_realm_auth(&state, auth)?;
    let chosen = request.location()?;
    let chosen = location::set(&state.get_ctx(), &auth, id, chosen)
        .await
        .map_err(|error| match error {
            GitError::Invalid => ServerError::BadRequest,
            error => map_error(error),
        })?;
    Ok(Json(StorageLocation::new((chosen, false))))
}

#[cfg(test)]
#[path = "location_tests.rs"]
mod tests;
