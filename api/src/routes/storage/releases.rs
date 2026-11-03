//! Publishes and lists database releases: tagged release crate versions that pin the current
//! files under a prefix, and lists the files a release pins.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::identifier::{Identifier, IdentifierTarget};
use aruna_operations::s3::bucket::release::{BucketScope, MembershipCaller, release_members};
use aruna_operations::s3::bucket::release_publish::{
    self, PublishError, PublishInput, ReleaseSpec,
};
use aruna_operations::s3::bucket::release_view::{FilePage, MAX_PAGE, ViewError, release_view};
use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use utoipa::{IntoParams, OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use super::bucket_metadata::BucketIdentifierBody;
use super::deletion::bucket_info;
use super::relay::{copy_miss, query_path, read_holders, relay_json, relay_read, rest_target};
use crate::auth::{ValidatedBearer, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server::state::ServerState;

#[derive(OpenApi)]
#[openapi()]
pub struct ReleasesApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(ReleasesApiDoc::openapi())
        .routes(routes!(publish_release, list_releases))
        .routes(routes!(list_release_files))
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

#[derive(Debug, Clone, Serialize, Deserialize, IntoParams)]
pub struct ReleaseFilesParams {
    /// Key of the release crate, as the release list names it.
    pub key: String,
    /// Only files under this prefix.
    #[serde(default)]
    pub prefix: Option<String>,
    /// `/` groups the files below the next `/` after `prefix` into `folders`.
    #[serde(default)]
    pub delimiter: Option<String>,
    /// The `next` value of the page before.
    #[serde(default)]
    pub after: Option<String>,
    /// Entries per page, files and folders together: 1 to 1000, default 100.
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReleaseFileItem {
    pub key: String,
    /// The exact version the release pins; read it with this `versionId`.
    pub version_id: String,
    pub size: u64,
    /// The exact id the release crate names the version by.
    pub id: String,
    /// The content id, when the bytes had a known BLAKE3 at publication.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReleaseFilesResponse {
    pub bucket: String,
    pub key: String,
    pub version_id: String,
    pub label: String,
    /// The prefix the release pinned.
    pub prefix: String,
    pub files: Vec<ReleaseFileItem>,
    /// Folders below `prefix`, each ending with `/`, when `delimiter` is `/`.
    pub folders: Vec<String>,
    /// Pass as `after` for the next page; omitted on the last page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next: Option<String>,
}

#[utoipa::path(
    get,
    path = "/data/buckets/{bucket}/releases/{version_id}/files",
    tag = "data/storage",
    summary = "List the files of a release",
    description = r#"Lists the files one release pins, with the exact version of each, so the bucket can be browsed and read as of that release.

**Authentication**: realm bearer token with READ on the release crate.

**Behavior**
- `version_id` is the release crate's version and `key` its key, as the release list names
  them. A version that is gone or not tagged as a release answers 404.
- Files come in key order from the release crate. Each names the version id to read it by,
  for example S3 GET with `versionId`; a later version or a delete marker of the key does not
  change the release.
- With `delimiter=/` the keys below the next `/` after `prefix` are grouped into `folders`.
- In a distributed bucket the crate is read from a member that holds its range, its
  coordinator last.

**Limits**
- The crate is read in full for every page, so its size grows with the number of files."#,
    params(
        ("bucket" = String, Path, description = "Bucket name on this node"),
        ("version_id" = String, Path, description = "The release crate's version id"),
        ReleaseFilesParams
    ),
    responses(
        (status = 200, description = "One page of the release's files", body = ReleaseFilesResponse, example = json!({
            "bucket": "bakta-db",
            "key": "releases/6.0.crate.json",
            "version_id": "01K8W1B3D5F7H9KMQ1S3V5X7Z2",
            "label": "6.0",
            "prefix": "db/",
            "files": [{
                "key": "db/amrfinderplus-db/version.txt",
                "version_id": "01K8W0Q2C4E6G8J0M2P4R6T8V1",
                "size": 11,
                "id": "https://w3id.org/aruna/data/AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8/01K8W0Q2C4E6G8J0M2P4R6T8V1",
                "content_id": "https://w3id.org/aruna/data/5d41402abc4b2a76b9719d911017c5925d41402abc4b2a76b9719d911017c592"
            }],
            "folders": ["db/pfam/"],
            "next": "db/pfam/"
        })),
        (status = 400, description = "Invalid version id, delimiter or limit", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No READ on the release crate", body = ErrorResponse),
        (status = 404, description = "Bucket not found, or the version is gone or no release", body = ErrorResponse),
        (status = 503, description = "The release crate could not be read now", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_release_files(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    headers: HeaderMap,
    Path((bucket, version_id)): Path<(String, String)>,
    Query(params): Query<ReleaseFilesParams>,
) -> ServerResult<Json<ReleaseFilesResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let version: Ulid = (version_id.parse())
        .map_err(|_| ServerError::BadRequestMessage(format!("`{version_id}` is not an id")))?;
    let folders = match params.delimiter.as_deref() {
        None => false,
        Some("/") => true,
        Some(_) => {
            return Err(ServerError::BadRequestMessage(
                "only `/` is a delimiter".to_string(),
            ));
        }
    };
    let limit = params.limit.unwrap_or(100);
    if !(1..=MAX_PAGE).contains(&limit) {
        return Err(ServerError::BadRequestMessage(format!(
            "`limit` must be 1 to {MAX_PAGE}"
        )));
    }
    let info = bucket_info(&state, &bucket).await?;
    let relayed = |route: &str| {
        let mut pairs = vec![("key", params.key.as_str())];
        let optional = [
            ("prefix", params.prefix.as_deref()),
            ("delimiter", params.delimiter.as_deref()),
            ("after", params.after.as_deref()),
        ];
        pairs.extend(
            optional
                .into_iter()
                .filter_map(|(name, value)| Some((name, value?))),
        );
        let limit = limit.to_string();
        query_path(route, &[pairs, vec![("limit", limit.as_str())]].concat())
    };
    let route = format!("/data/buckets/{bucket}/releases/{version}/files");
    if let Some(holders) = read_holders(&state, &info, &params.key, &headers).await? {
        let answer = relay_read(&state, &holders, &relayed(&route), bearer.as_ref()).await?;
        return Ok(Json(answer));
    }
    let scope = BucketScope {
        realm_id: state.get_realm_id(),
        node_id: state.get_node_id(),
        bucket: &bucket,
        info: &info,
    };
    let page = FilePage {
        prefix: params.prefix.clone().unwrap_or_default(),
        folders,
        after: params.after.clone(),
        limit,
    };
    let context = state.get_ctx();
    let found = release_view(context.as_ref(), scope, &auth, (&params.key, version), page).await;
    let view = match found {
        Ok(view) => view,
        // A copy may not have received the release yet; its coordinator decides.
        Err(ViewError::Missing) => match copy_miss(&state, &info, &params.key, &headers).await? {
            Some(holders) => {
                let path = relayed(&route);
                return Ok(Json(
                    relay_read(&state, &holders, &path, bearer.as_ref()).await?,
                ));
            }
            None => return Err(ServerError::NotFound),
        },
        Err(ViewError::NoRelease) => return Err(ServerError::NotFound),
        Err(ViewError::Forbidden) => return Err(ServerError::Forbidden),
        Err(error) => return Err(ServerError::ServiceUnavailableReason(error.to_string())),
    };
    let files = (view.files.into_iter())
        .map(|file| ReleaseFileItem {
            key: file.key,
            version_id: file.version_id.to_string(),
            size: file.size,
            id: file.id,
            content_id: file.content_id,
        })
        .collect();
    Ok(Json(ReleaseFilesResponse {
        bucket,
        key: params.key,
        version_id: version.to_string(),
        label: view.label,
        prefix: view.prefix,
        files,
        folders: view.folders,
        next: view.next,
    }))
}

#[cfg(test)]
#[path = "releases_tests.rs"]
mod tests;
