//! Serves bucket metadata: whether a bucket is a database (D34), its identifier rules, and the
//! dataset that describes it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::identifier::Identifier;
use aruna_core::structs::storage::blob::{BucketInfo, bucket_permission_path};
use aruna_core::structs::storage::bucket_metadata::{BucketMetadata, validate_metadata};
use aruna_core::structs::storage::dataset_description::DatasetDescription;
use aruna_operations::distribution::change::change_metadata;
use aruna_operations::driver::drive;
use aruna_operations::metadata::AuthToken;
use aruna_operations::s3::bucket::metadata::{
    DescriptionError, PutMetadataError, PutMetadataOperation, bucket_metadata, dataset_description,
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
use crate::metadata::forwarded_auth_token;
use crate::routes::identifiers::IdentifierParserBody;
use crate::server::state::ServerState;

#[derive(OpenApi)]
#[openapi()]
pub struct BucketMetadataDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(BucketMetadataDoc::openapi())
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

impl From<Identifier> for BucketIdentifierBody {
    fn from(identifier: Identifier) -> Self {
        Self {
            name: identifier.name,
            value: identifier.value,
            url: identifier.url,
        }
    }
}

/// What a bucket keeps itself. `database` lets the portal show it as a database; the server
/// treats it as any bucket. Title, description, license, identifiers and version live in the
/// linked dataset.
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct BucketMetadataBody {
    /// Needs `dataset`.
    #[serde(default)]
    pub database: bool,
    /// Identifier rules applied to the bucket's keys and source URLs.
    pub parser: Option<IdentifierParserBody>,
    /// The dataset document that describes the bucket.
    pub dataset: Option<String>,
}

impl From<BucketMetadata> for BucketMetadataBody {
    fn from(metadata: BucketMetadata) -> Self {
        Self {
            database: metadata.database,
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
            parser: body.parser.map(Into::into),
            dataset,
        };
        validate_metadata(&metadata).map_err(ServerError::BadRequestReason)?;
        Ok(metadata)
    }
}

/// What the linked dataset's root entity says about the bucket, read now.
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct DatasetDescriptionBody {
    pub title: Option<String>,
    pub description: Option<String>,
    pub version: Option<String>,
    /// A license URL or SPDX id.
    pub license: Option<String>,
    /// Identifiers that link to a web page, such as a DOI.
    pub identifiers: Vec<BucketIdentifierBody>,
}

impl From<DatasetDescription> for DatasetDescriptionBody {
    fn from(described: DatasetDescription) -> Self {
        Self {
            title: described.title,
            description: described.description,
            version: described.version,
            license: described.license,
            identifiers: described.identifiers.into_iter().map(Into::into).collect(),
        }
    }
}

/// The linked dataset's description read with the caller's access, or why there is none now.
pub(crate) async fn describe_bucket(
    state: &ServerState,
    metadata: Option<&BucketMetadata>,
    (auth, token): (Option<&AuthContext>, Option<AuthToken>),
) -> (Option<DatasetDescriptionBody>, Option<String>) {
    let Some(dataset) = metadata.and_then(|metadata| metadata.dataset) else {
        return (None, None);
    };
    let ctx = state.get_ctx();
    let read = dataset_description(&ctx, state.get_realm_id(), dataset, (auth, token)).await;
    match read {
        Ok(described) => (Some(described.into()), None),
        Err(error) => (None, Some(error.to_string())),
    }
}

/// Refuses a dataset link the caller cannot read or whose root is not a Dataset.
async fn check_link(
    state: &ServerState,
    metadata: Option<&BucketMetadata>,
    (auth, token): (&AuthContext, Option<AuthToken>),
) -> ServerResult<Option<DatasetDescriptionBody>> {
    let Some(dataset) = metadata.and_then(|metadata| metadata.dataset) else {
        return Ok(None);
    };
    let ctx = state.get_ctx();
    let read = dataset_description(&ctx, state.get_realm_id(), dataset, (Some(auth), token));
    match read.await {
        Ok(described) => Ok(Some(described.into())),
        Err(DescriptionError::Unreadable) => Err(ServerError::BadRequestReason(format!(
            "dataset {dataset} does not exist or you may not read it"
        ))),
        Err(DescriptionError::Invalid(reason)) => Err(ServerError::BadRequestReason(reason)),
        Err(DescriptionError::Unavailable) => Err(ServerError::ServiceUnavailableReason(format!(
            "dataset {dataset} could not be read now"
        ))),
    }
}

/// Writes a bucket's metadata: on this node for an ordinary bucket, through the set's publisher
/// for a distributed one. With `expected`, only while the current metadata is still that.
pub(crate) async fn write_metadata(
    state: &ServerState,
    (auth, bearer): (&AuthContext, Option<ValidatedBearer>),
    (bucket, info): (&str, &BucketInfo),
    (metadata, expected): (Option<BucketMetadata>, Option<BucketMetadata>),
) -> ServerResult<()> {
    match &info.distribution {
        Some(binding) => {
            let bearer = forwarded_auth_token(bearer)?;
            let change = (metadata, expected);
            change_metadata(&state.get_ctx(), (auth, bearer), binding.set_id, change)
                .await
                .map_err(map_create_error)?;
        }
        None => {
            let mut put = PutMetadataOperation::new(
                (auth.clone(), state.get_realm_id()),
                bucket.to_string(),
                info.group_id,
                metadata,
            );
            if let Some(expected) = expected {
                put = put.expecting(expected);
            }
            drive(put, &state.get_ctx())
                .await
                .map_err(|error| match error {
                    PutMetadataError::NoSuchBucket | PutMetadataError::GroupMismatch => {
                        ServerError::NotFound
                    }
                    PutMetadataError::Forbidden => ServerError::Forbidden,
                    PutMetadataError::Changed => ServerError::Conflict(error.to_string()),
                    error => ServerError::InternalError(error.to_string()),
                })?;
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BucketMetadataResponse {
    pub bucket: String,
    /// `null` when the bucket has none.
    pub metadata: Option<BucketMetadataBody>,
    /// What the linked dataset says now; `null` without a dataset or while it cannot be read.
    pub description: Option<DatasetDescriptionBody>,
    /// Why a linked dataset gives no description now.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description_unavailable: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
#[schema(as = PutBucketMetadataRequest)]
pub struct PutMetadataRequest {
    /// Required; `null` clears the metadata.
    #[serde(deserialize_with = "Option::deserialize")]
    pub metadata: Option<BucketMetadataBody>,
}

#[utoipa::path(
    get,
    path = "/data/buckets/{bucket}/metadata",
    tag = "data/storage",
    summary = "Read a bucket's metadata",
    description = r#"Returns whether a bucket is shown as a database, its identifier rules, the dataset that describes it, and that dataset's description now.

**Authentication**: realm bearer token with READ on the bucket. The description is read with the
caller's own access to the dataset.

**Behavior**
- An ordinary bucket keeps its metadata on its own node; a distributed bucket in its set, so
  every member answers the same.
- A bucket created again under the same name starts without metadata.
- Title, description, version, license and identifiers come only from the linked dataset's
  root entity. While that dataset cannot be read, `description` is `null` and
  `description_unavailable` says why."#,
    params(("bucket" = String, Path, description = "Bucket name on this node")),
    responses(
        (status = 200, description = "The bucket's metadata", body = BucketMetadataResponse, example = json!({
            "bucket": "bakta-db",
            "metadata": {
                "database": true,
                "parser": null,
                "dataset": "01JMETADATA0123456789ABCDE"
            },
            "description": {
                "title": "Bakta database",
                "description": "Annotation database for Bakta",
                "version": "6.0",
                "license": "https://spdx.org/licenses/CC-BY-4.0",
                "identifiers": [{"name": "DOI", "value": "10.5281/zenodo.14916843", "url": "https://doi.org/10.5281/zenodo.14916843"}]
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
    Extension(bearer): Extension<Option<ValidatedBearer>>,
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
    let token = forwarded_auth_token(bearer)?;
    let (description, description_unavailable) =
        describe_bucket(&state, metadata.as_ref(), (Some(&auth), token)).await;
    Ok(Json(BucketMetadataResponse {
        bucket,
        metadata: metadata.map(Into::into),
        description,
        description_unavailable,
    }))
}

#[utoipa::path(
    put,
    path = "/data/buckets/{bucket}/metadata",
    tag = "data/storage",
    summary = "Set a bucket's metadata",
    description = r#"Replaces whether a bucket is a database, its identifier rules and its dataset link, or clears them with `"metadata": null`.

**Authentication**: realm bearer token with WRITE on the owning group's admin path, and READ on
the linked dataset. For a distributed bucket the set's publisher checks the forwarded token again.

**Behavior**
- `metadata` is required and is the only field, so a body without it never clears anything.
- `database: true` lets clients show the bucket as a database and needs `dataset`.
- `dataset` must name an existing dataset the caller may read whose root entity is a Dataset
  with a name. `POST /data/buckets/{bucket}/dataset` creates one and links it in one step.
- Identifier rules must compile and their links must be `http` or `https`.
- An ordinary bucket's metadata is written on this node, in the same transaction that checks
  the bucket still belongs to the group. A distributed bucket's metadata changes in its set;
  a settings change of the set keeps it."#,
    params(("bucket" = String, Path, description = "Bucket name on this node")),
    request_body(content = PutMetadataRequest, example = json!({
        "metadata": {
            "database": true,
            "dataset": "01JMETADATA0123456789ABCDE"
        }
    })),
    responses(
        (status = 200, description = "The metadata as stored, with the dataset's description", body = BucketMetadataResponse, example = json!({
            "bucket": "bakta-db",
            "metadata": {"database": true, "parser": null, "dataset": "01JMETADATA0123456789ABCDE"},
            "description": {"title": "Bakta database", "description": "Annotation database for Bakta", "version": "6.0", "license": null, "identifiers": []}
        })),
        (status = 400, description = "Missing `metadata`, an unknown field such as a former `title`, `database` without `dataset`, invalid identifier rules, or a dataset that does not exist, cannot be read by the caller or is not a Dataset", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No WRITE on the owning group's admin path", body = ErrorResponse),
        (status = 404, description = "Bucket not found on this node, or no longer owned by the group", body = ErrorResponse),
        (status = 409, description = "The set's publisher refused the change", body = ErrorResponse),
        (status = 503, description = "The linked dataset could not be read now", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn put_bucket_metadata(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    Path(bucket): Path<String>,
    request: Result<Json<PutMetadataRequest>, JsonRejection>,
) -> ServerResult<Json<BucketMetadataResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let Json(request) =
        request.map_err(|error| ServerError::BadRequestReason(error.body_text()))?;
    let metadata = request.metadata.map(BucketMetadata::try_from).transpose()?;
    let info = bucket_info(&state, &bucket).await?;
    ensure_group_admin(&state, &auth, info.group_id).await?;
    let token = forwarded_auth_token(bearer.clone())?;
    let description = check_link(&state, metadata.as_ref(), (&auth, token)).await?;
    let change = (metadata.clone(), None);
    write_metadata(&state, (&auth, bearer), (&bucket, &info), change).await?;
    Ok(Json(BucketMetadataResponse {
        bucket,
        metadata: metadata.map(Into::into),
        description,
        description_unavailable: None,
    }))
}

#[cfg(test)]
#[path = "bucket_metadata_tests.rs"]
mod tests;
