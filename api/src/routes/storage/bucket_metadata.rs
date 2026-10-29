//! Serves bucket metadata: what describes a bucket, and marks it as a database (D34).
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::identifier::{Identifier, IdentifierTarget};
use aruna_core::structs::storage::blob::bucket_permission_path;
use aruna_core::structs::storage::bucket_metadata::{BucketMetadata, validate_metadata};
use aruna_operations::distribution::change::change_metadata;
use aruna_operations::driver::drive;
use aruna_operations::s3::bucket::metadata::{
    PutMetadataError, PutMetadataOperation, bucket_metadata,
};
use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, State};
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use super::deletion::bucket_info;
use super::distributed::map_create_error;
use super::routing::ensure_group_admin;
use crate::auth::{ValidatedBearer, ensure_permission, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::identifiers::IdentifierParserBody;
use crate::server::state::ServerState;

#[derive(OpenApi)]
#[openapi()]
pub struct BucketMetadataApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(BucketMetadataApiDoc::openapi())
        .routes(routes!(get_bucket_metadata, put_bucket_metadata))
}

/// One identifier of a bucket, such as its DOI.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct BucketIdentifierBody {
    pub name: String,
    pub value: String,
    /// Landing page; `http` or `https`.
    pub url: String,
}

/// What describes a bucket. `database` lets the portal show it as a database; the server treats
/// it as any bucket.
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct BucketMetadataBody {
    #[serde(default)]
    pub database: bool,
    pub title: Option<String>,
    pub description: Option<String>,
    pub version: Option<String>,
    /// A license URL or SPDX id.
    pub license: Option<String>,
    #[serde(default)]
    pub identifiers: Vec<BucketIdentifierBody>,
    /// Identifier rules applied to the bucket's keys and source URLs.
    pub parser: Option<IdentifierParserBody>,
    /// The dataset document that describes the bucket.
    pub dataset: Option<String>,
}

impl From<BucketMetadata> for BucketMetadataBody {
    fn from(metadata: BucketMetadata) -> Self {
        Self {
            database: metadata.database,
            title: metadata.title,
            description: metadata.description,
            version: metadata.version,
            license: metadata.license,
            identifiers: (metadata.identifiers.into_iter())
                .map(|identifier| BucketIdentifierBody {
                    name: identifier.name,
                    value: identifier.value,
                    url: identifier.url,
                })
                .collect(),
            parser: metadata.parser.map(Into::into),
            dataset: metadata.dataset.map(|dataset| dataset.to_string()),
        }
    }
}

impl TryFrom<BucketMetadataBody> for BucketMetadata {
    type Error = ServerError;

    fn try_from(body: BucketMetadataBody) -> Result<Self, Self::Error> {
        let dataset = body
            .dataset
            .map(|dataset| Ulid::from_string(&dataset))
            .transpose()
            .map_err(|_| ServerError::BadRequestReason("dataset is not a ULID".to_string()))?;
        let metadata = BucketMetadata {
            database: body.database,
            title: body.title,
            description: body.description,
            version: body.version,
            license: body.license,
            identifiers: (body.identifiers.into_iter())
                .map(|identifier| Identifier {
                    name: identifier.name,
                    value: identifier.value,
                    url: identifier.url,
                    target: IdentifierTarget::Key,
                })
                .collect(),
            parser: body.parser.map(Into::into),
            dataset,
        };
        validate_metadata(&metadata).map_err(ServerError::BadRequestReason)?;
        Ok(metadata)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BucketMetadataResponse {
    pub bucket: String,
    /// `null` when the bucket has none.
    pub metadata: Option<BucketMetadataBody>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct PutBucketMetadataRequest {
    /// Required; `null` clears the metadata.
    #[serde(deserialize_with = "Option::deserialize")]
    pub metadata: Option<BucketMetadataBody>,
}

#[utoipa::path(
    get,
    path = "/data/buckets/{bucket}/metadata",
    tag = "data/storage",
    summary = "Read a bucket's metadata",
    description = r#"Returns what describes a bucket: title, version, license, identifiers such as a DOI, identifier rules, and whether it is shown as a database.

**Authentication**: realm bearer token with READ on the bucket.

**Behavior**
- An ordinary bucket keeps its metadata on its own node; a distributed bucket in its set, so
  every member answers the same.
- A bucket created again under the same name starts without metadata."#,
    params(("bucket" = String, Path, description = "Bucket name on this node")),
    responses(
        (status = 200, description = "The bucket's metadata", body = BucketMetadataResponse, example = json!({
            "bucket": "bakta-db",
            "metadata": {
                "database": true,
                "title": "Bakta database",
                "version": "6.0",
                "license": "CC-BY-4.0",
                "identifiers": [{"name": "DOI", "value": "10.5281/zenodo.14916843", "url": "https://doi.org/10.5281/zenodo.14916843"}],
                "parser": null,
                "dataset": null
            }
        })),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No READ on the bucket", body = ErrorResponse),
        (status = 404, description = "Bucket not found on this node", body = ErrorResponse),
        (status = 503, description = "The metadata could not be read now", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_bucket_metadata(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(bucket): Path<String>,
) -> ServerResult<Json<BucketMetadataResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let info = bucket_info(&state, &bucket).await?;
    let path = bucket_permission_path(
        state.get_realm_id(),
        info.group_id,
        state.get_node_id(),
        &bucket,
    );
    ensure_permission(&state, &auth, path, Permission::READ).await?;
    let metadata = bucket_metadata(&state.get_ctx(), &bucket, &info)
        .await
        .map_err(ServerError::ServiceUnavailableReason)?;
    Ok(Json(BucketMetadataResponse {
        bucket,
        metadata: metadata.map(Into::into),
    }))
}

#[utoipa::path(
    put,
    path = "/data/buckets/{bucket}/metadata",
    tag = "data/storage",
    summary = "Set a bucket's metadata",
    description = r#"Replaces what describes a bucket, or clears it with `"metadata": null`.

**Authentication**: realm bearer token with WRITE on the owning group's admin path. For a
distributed bucket the set's publisher checks the forwarded token again.

**Behavior**
- `metadata` is required and is the only field, so a body without it never clears anything.
- `database: true` lets clients show the bucket as a database; it changes nothing else.
- Identifier rules must compile and their links, like every identifier link, must be `http` or
  `https`.
- An ordinary bucket's metadata is written on this node, in the same transaction that checks
  the bucket still belongs to the group. A distributed bucket's metadata changes in its set;
  a settings change of the set keeps it."#,
    params(("bucket" = String, Path, description = "Bucket name on this node")),
    request_body(content = PutBucketMetadataRequest, example = json!({
        "metadata": {
            "database": true,
            "title": "Bakta database",
            "version": "6.0",
            "identifiers": [{"name": "DOI", "value": "10.5281/zenodo.14916843", "url": "https://doi.org/10.5281/zenodo.14916843"}]
        }
    })),
    responses(
        (status = 200, description = "The metadata as stored", body = BucketMetadataResponse, example = json!({
            "bucket": "bakta-db",
            "metadata": {"database": true, "title": "Bakta database", "version": "6.0", "license": null, "identifiers": [], "parser": null, "dataset": null}
        })),
        (status = 400, description = "Missing `metadata`, an unknown field, or invalid identifier rules, identifier link or dataset id", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No WRITE on the owning group's admin path", body = ErrorResponse),
        (status = 404, description = "Bucket not found on this node, or no longer owned by the group", body = ErrorResponse),
        (status = 409, description = "The set's publisher refused the change", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn put_bucket_metadata(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    Path(bucket): Path<String>,
    request: Result<Json<PutBucketMetadataRequest>, JsonRejection>,
) -> ServerResult<Json<BucketMetadataResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let Json(request) =
        request.map_err(|error| ServerError::BadRequestReason(error.body_text()))?;
    let metadata = request.metadata.map(BucketMetadata::try_from).transpose()?;
    let info = bucket_info(&state, &bucket).await?;
    ensure_group_admin(&state, &auth, info.group_id).await?;
    match &info.distribution {
        Some(binding) => {
            let bearer = crate::metadata::forwarded_auth_token(bearer)?;
            change_metadata(
                &state.get_ctx(),
                (&auth, bearer),
                binding.set_id,
                metadata.clone(),
            )
            .await
            .map_err(map_create_error)?;
        }
        None => {
            drive(
                PutMetadataOperation::new(
                    (auth.clone(), state.get_realm_id()),
                    bucket.clone(),
                    info.group_id,
                    metadata.clone(),
                ),
                &state.get_ctx(),
            )
            .await
            .map_err(|error| match error {
                PutMetadataError::NoSuchBucket | PutMetadataError::GroupMismatch => {
                    ServerError::NotFound
                }
                PutMetadataError::Forbidden => ServerError::Forbidden,
                error => ServerError::InternalError(error.to_string()),
            })?;
        }
    }
    Ok(Json(BucketMetadataResponse {
        bucket,
        metadata: metadata.map(Into::into),
    }))
}

#[cfg(test)]
#[path = "bucket_metadata_tests.rs"]
mod tests;
