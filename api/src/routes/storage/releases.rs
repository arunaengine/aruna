//! Publishes and lists database releases: tagged release crate versions that pin the current
//! files under a prefix.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::identifier::{Identifier, IdentifierTarget};
use aruna_operations::s3::bucket::release::{BucketScope, MembershipCaller, release_members};
use aruna_operations::s3::bucket::release_publish::{
    self, PublishError, PublishInput, ReleaseSpec,
};
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use super::bucket_metadata::BucketIdentifierBody;
use super::deletion::bucket_info;
use super::relay::{relay_json, rest_target};
use crate::auth::{ValidatedBearer, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server::state::ServerState;

#[derive(OpenApi)]
#[openapi()]
pub struct ReleasesApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(ReleasesApiDoc::openapi())
        .routes(routes!(publish_release, list_releases))
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PublishReleaseRequest {
    /// Key of the release crate in the bucket.
    pub key: String,
    /// The release label, such as `6.0`.
    pub label: String,
    /// The key prefix whose current files the release pins; empty for the whole bucket.
    #[serde(default)]
    pub prefix: String,
    /// Where the release came from, such as its DOI.
    #[serde(default)]
    pub upstream: Vec<BucketIdentifierBody>,
    /// Runs that prepared the files, by run id.
    #[serde(default)]
    pub preparation: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PublishReleaseResponse {
    pub bucket: String,
    pub key: String,
    /// The release crate's version; immutable.
    pub version_id: String,
    pub files: usize,
    /// An identical release was already the key's current version, so nothing was written.
    pub replayed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReleaseItem {
    pub key: String,
    pub version_id: String,
    pub label: String,
    pub prefix: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReleaseListResponse {
    pub bucket: String,
    pub releases: Vec<ReleaseItem>,
    /// `false` when this member does not hold every range, so releases may be missing.
    pub complete: bool,
}

#[utoipa::path(
    post,
    path = "/data/buckets/{bucket}/releases",
    tag = "data/storage",
    summary = "Publish a database release",
    description = r#"Writes a release crate that pins the current version of every file under `prefix`, as one new version of `key` tagged as a release.

**Authentication**: realm bearer token with WRITE on the bucket, and WRITE on the owning group's
admin path or on the realm's database curator path. The caller must be able to read every file
under `prefix`, or nothing is published.

**Behavior**
- The crate is an RO-Crate: the release label, the upstream identifiers, the preparation runs,
  and each file by its exact version id with its size, content id or source pin.
- The tagged version is immutable; a later version of the key is a new release or none. A copy
  of a release crate is no release.
- Repeating an identical publish answers the existing version with `replayed: true`. The write
  is guarded by the key's head, so two concurrent identical publishes make one release.
- In a distributed bucket the coordinator of the key's range writes it, and each range under the
  prefix is listed by its own coordinator with the caller's token. A range whose coordinator
  cannot list it now refuses the publish with 503; a copy that may be behind is never used.

**Limits**
- The crate is built in memory, so its size grows with the number of files."#,
    params(("bucket" = String, Path, description = "Bucket name on this node")),
    request_body(content = PublishReleaseRequest, example = json!({
        "key": "bakta/6.0/ro-crate-metadata.json",
        "label": "6.0",
        "prefix": "bakta/6.0/",
        "upstream": [{"name": "DOI", "value": "10.5281/zenodo.14916843", "url": "https://doi.org/10.5281/zenodo.14916843"}],
        "preparation": []
    })),
    responses(
        (status = 200, description = "The release crate's version", body = PublishReleaseResponse, example = json!({
            "bucket": "bakta-db",
            "key": "bakta/6.0/ro-crate-metadata.json",
            "version_id": "01K8W1B3D5F7H9KMQ1S3V5X7Z2",
            "files": 31,
            "replayed": false
        })),
        (status = 400, description = "Invalid identifier link or run id", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No WRITE on the bucket, neither admin nor curator, or a file under the prefix is not readable", body = ErrorResponse),
        (status = 404, description = "Bucket not found on this node", body = ErrorResponse),
        (status = 409, description = "The key changed while the release was written", body = ErrorResponse),
        (status = 503, description = "A range could not be listed by its coordinator now", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn publish_release(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    headers: HeaderMap,
    Path(bucket): Path<String>,
    Json(request): Json<PublishReleaseRequest>,
) -> ServerResult<Json<PublishReleaseResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let spec = release_spec(&request)?;
    let info = bucket_info(&state, &bucket).await?;
    // In a distributed bucket the coordinator of the key's range writes it.
    if let Some(forward) = rest_target(&state, &info, &request.key, &headers).await? {
        let path = format!("/data/buckets/{bucket}/releases");
        let (_, answer) = relay_json(&state, &forward, &path, bearer.as_ref(), &request).await?;
        return Ok(Json(answer));
    }
    let scope = BucketScope {
        realm_id: state.get_realm_id(),
        node_id: state.get_node_id(),
        bucket: &bucket,
        info: &info,
    };
    let input = PublishInput {
        auth,
        bearer: bearer.as_ref().map(|bearer| bearer.as_str().to_string()),
        key: request.key.clone(),
        spec,
    };
    let published = release_publish::publish_release(state.get_ctx().as_ref(), scope, input)
        .await
        .map_err(|error| match error {
            PublishError::Forbidden(_) => ServerError::Forbidden,
            PublishError::Conflict(reason) => ServerError::Conflict(reason),
            PublishError::Unavailable(reason) => ServerError::ServiceUnavailableReason(reason),
        })?;
    Ok(Json(PublishReleaseResponse {
        bucket,
        key: request.key,
        version_id: published.version_id.to_string(),
        files: published.files,
        replayed: published.replayed,
    }))
}

fn release_spec(request: &PublishReleaseRequest) -> ServerResult<ReleaseSpec> {
    let preparation = (request.preparation.iter())
        .map(|run_id| Ulid::from_string(run_id))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| ServerError::BadRequestReason("a preparation run id is no ULID".into()))?;
    let upstream: Vec<Identifier> = (request.upstream.iter())
        .map(|identifier| Identifier {
            name: identifier.name.clone(),
            value: identifier.value.clone(),
            url: identifier.url.clone(),
            target: IdentifierTarget::Key,
        })
        .collect();
    aruna_core::structs::storage::bucket_metadata::validate_identifiers(&upstream)
        .map_err(ServerError::BadRequestReason)?;
    Ok(ReleaseSpec {
        label: request.label.clone(),
        prefix: request.prefix.clone(),
        upstream,
        preparation,
    })
}

#[utoipa::path(
    get,
    path = "/data/buckets/{bucket}/releases",
    tag = "data/storage",
    summary = "List a bucket's releases",
    description = r#"Lists a bucket's releases whose release crate the caller may read.

**Authentication**: realm bearer token.

**Behavior**
- Each release is the exact version of its tagged release crate, read and checked: a version
  that is gone, for example purged or of an earlier bucket of the name, is not listed.
- In a distributed bucket each range is answered by its coordinator, asked with the caller's
  token; `complete` is `false` when one did not answer."#,
    params(("bucket" = String, Path, description = "Bucket name on this node")),
    responses(
        (status = 200, description = "The releases", body = ReleaseListResponse, example = json!({
            "bucket": "bakta-db",
            "releases": [{"key": "bakta/6.0/ro-crate-metadata.json", "version_id": "01K8W1B3D5F7H9KMQ1S3V5X7Z2", "label": "6.0", "prefix": "bakta/6.0/"}],
            "complete": true
        })),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 404, description = "Bucket not found on this node", body = ErrorResponse),
        (status = 503, description = "The releases could not be read now", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_releases(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    Path(bucket): Path<String>,
) -> ServerResult<Json<ReleaseListResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let info = bucket_info(&state, &bucket).await?;
    let scope = BucketScope {
        realm_id: state.get_realm_id(),
        node_id: state.get_node_id(),
        bucket: &bucket,
        info: &info,
    };
    let caller = MembershipCaller {
        auth: &auth,
        bearer: bearer.as_ref().map(ValidatedBearer::as_str),
    };
    let found = release_members(state.get_ctx().as_ref(), scope, caller, &[])
        .await
        .map_err(|error| ServerError::ServiceUnavailableReason(error.to_string()))?;
    let releases = (found.releases.into_iter())
        .map(|valid| ReleaseItem {
            key: valid.key,
            version_id: valid.version_id.to_string(),
            label: valid.release.label,
            prefix: valid.prefix,
        })
        .collect();
    Ok(Json(ReleaseListResponse {
        bucket,
        releases,
        complete: found.complete,
    }))
}

#[cfg(test)]
#[path = "releases_tests.rs"]
mod tests;
