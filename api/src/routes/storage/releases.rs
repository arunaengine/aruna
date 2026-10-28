//! Publishes and lists database releases: tagged release crate versions that pin the current
//! files under a prefix.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::sync::Arc;

use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::identifier::{Identifier, IdentifierTarget};
use aruna_core::structs::storage::blob::{BucketInfo, object_permission_path};
use aruna_core::structs::storage::bucket_metadata::{
    RELEASE_PREFIX_TAG, RELEASE_TAG, curator_path,
};
use aruna_operations::auth::request_authorization::authorize;
use aruna_operations::auth::request_policy::PolicyRequestExtras;
use aruna_operations::distribution::status::read_view;
use aruna_operations::s3::bucket::release::{
    BucketScope, ReleaseSpec, release_crate, release_files, release_rows,
};
use aruna_operations::s3::object::origin::read_origin;
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
use super::routing::ensure_group_admin;
use crate::auth::{ValidatedBearer, ensure_permission, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::mcp::data::{WriteObjectInput, store_text};
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

**Authentication**: realm bearer token with WRITE on `key`, and WRITE on the owning group's admin
path or on the realm's database curator path.

**Behavior**
- The crate is an RO-Crate: the release label, the upstream identifiers, the preparation runs,
  and each file by its exact version id with its size, content id or source pin.
- The tagged version is immutable; a later version of the key is a new release or none. A copy
  of a release crate is no release.
- Repeating an identical publish answers the existing version with `replayed: true`.
- In a distributed bucket the coordinator of the key's range writes it; it must hold the
  records of every range under the prefix.

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
        (status = 403, description = "No WRITE on the key, or neither admin nor curator", body = ErrorResponse),
        (status = 404, description = "Bucket not found on this node", body = ErrorResponse),
        (status = 409, description = "This member does not hold every range under the prefix", body = ErrorResponse),
        (status = 503, description = "The files could not be listed now", body = ErrorResponse)
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
    let path = object_permission_path(
        state.get_realm_id(),
        info.group_id,
        state.get_node_id(),
        &bucket,
        &request.key,
    );
    ensure_permission(&state, &auth, path, Permission::WRITE).await?;
    if ensure_group_admin(&state, &auth, info.group_id)
        .await
        .is_err()
    {
        let curator = curator_path(state.get_realm_id());
        ensure_permission(&state, &auth, curator, Permission::WRITE).await?;
    }
    // In a distributed bucket the coordinator of the key's range writes it.
    if let Some(forward) = rest_target(&state, &info, &request.key, &headers).await? {
        let path = format!("/data/buckets/{bucket}/releases");
        let (_, answer) = relay_json(&state, &forward, &path, bearer.as_ref(), &request).await?;
        return Ok(Json(answer));
    }
    holds_prefix(&state, &info, &spec.prefix).await?;
    let scope = BucketScope {
        realm_id: state.get_realm_id(),
        node_id: state.get_node_id(),
        bucket: &bucket,
        info: &info,
    };
    let context = state.get_ctx();
    let unavailable =
        |error: &dyn std::fmt::Display| ServerError::ServiceUnavailableReason(error.to_string());
    let files = release_files(context.as_ref(), scope, &spec.prefix, &request.key)
        .await
        .map_err(|error| unavailable(&error))?;
    let bytes = release_crate(&spec, &files);
    let hash = *blake3::hash(&bytes).as_bytes();
    if let Some(version_id) = current_release(&state, scope, &request.key, (&spec, hash)).await? {
        return Ok(Json(PublishReleaseResponse {
            bucket,
            key: request.key,
            version_id: version_id.to_string(),
            files: files.len(),
            replayed: true,
        }));
    }
    let text = String::from_utf8(bytes).map_err(|error| unavailable(&error))?;
    let tags = HashMap::from([
        (RELEASE_TAG.to_string(), spec.label.clone()),
        (RELEASE_PREFIX_TAG.to_string(), spec.prefix.clone()),
    ]);
    let input = WriteObjectInput {
        bucket: bucket.clone(),
        key: request.key.clone(),
        text,
        content_type: Some("application/ld+json".to_string()),
    };
    let stored = store_text(&state, &auth, &info, input, tags).await?;
    Ok(Json(PublishReleaseResponse {
        bucket,
        key: request.key,
        version_id: stored.version_id,
        files: files.len(),
        replayed: false,
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
    let check = aruna_core::structs::storage::bucket_metadata::BucketMetadata {
        identifiers: upstream.clone(),
        ..Default::default()
    };
    aruna_core::structs::storage::bucket_metadata::validate_metadata(&check)
        .map_err(ServerError::BadRequestReason)?;
    Ok(ReleaseSpec {
        label: request.label.clone(),
        prefix: request.prefix.clone(),
        upstream,
        preparation,
    })
}

/// A distributed member lists only the records it holds, so it must hold every range that
/// keys under `prefix` can fall into.
async fn holds_prefix(state: &ServerState, info: &BucketInfo, prefix: &str) -> ServerResult<()> {
    let Some(binding) = &info.distribution else {
        return Ok(());
    };
    let view = read_view(state.get_ctx().as_ref(), binding.set_id)
        .await
        .map_err(|error| ServerError::ServiceUnavailableReason(error.to_string()))?
        .ok_or(ServerError::NotFound)?;
    let snapshot = view.snapshot();
    let held = snapshot.held_ranges(binding.member_id);
    let missing = (snapshot.config.ranges.iter())
        .filter(|range| covers(range.lower.as_str(), range.upper.as_deref(), prefix))
        .any(|range| !held.iter().any(|kept| kept.range_id == range.range_id));
    match missing {
        true => Err(ServerError::Conflict(
            "this member does not hold every range under the prefix".to_string(),
        )),
        false => Ok(()),
    }
}

/// Whether a range `[lower, upper)` may hold keys that start with `prefix`.
fn covers(lower: &str, upper: Option<&str>, prefix: &str) -> bool {
    let below_upper = upper.is_none_or(|upper| prefix < upper);
    let above_lower = lower <= prefix || lower.starts_with(prefix);
    below_upper && above_lower
}

/// The key's current version, when it is already this exact release.
async fn current_release(
    state: &ServerState,
    scope: BucketScope<'_>,
    key: &str,
    (spec, hash): (&ReleaseSpec, [u8; 32]),
) -> ServerResult<Option<Ulid>> {
    let context = state.get_ctx();
    let Ok(current) = read_origin(
        context.as_ref(),
        (scope.realm_id, scope.node_id),
        scope.bucket,
        key,
        None,
    )
    .await
    else {
        return Ok(None);
    };
    if current.content_hash != Some(hash) {
        return Ok(None);
    }
    let rows = release_rows(context.as_ref(), scope.bucket)
        .await
        .map_err(|error| ServerError::ServiceUnavailableReason(error.to_string()))?;
    let same = rows.iter().any(|(version_id, pointer)| {
        *version_id == current.version_id
            && pointer.label == spec.label
            && pointer.prefix == spec.prefix
    });
    Ok(same.then_some(current.version_id))
}

#[utoipa::path(
    get,
    path = "/data/buckets/{bucket}/releases",
    tag = "data/storage",
    summary = "List a bucket's releases",
    description = r#"Lists the releases this node holds of a bucket whose release crate the caller may read.

**Authentication**: realm bearer token.

**Behavior**
- Each release is the exact version of its tagged release crate.
- A member of a distributed bucket lists the releases in the ranges it holds; `complete` is
  `false` when it does not hold every range."#,
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
    Path(bucket): Path<String>,
) -> ServerResult<Json<ReleaseListResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let info = bucket_info(&state, &bucket).await?;
    let context = state.get_ctx();
    let rows = release_rows(context.as_ref(), &bucket)
        .await
        .map_err(|error| ServerError::ServiceUnavailableReason(error.to_string()))?;
    let mut releases = Vec::new();
    for (version_id, pointer) in rows {
        let path = object_permission_path(
            state.get_realm_id(),
            info.group_id,
            state.get_node_id(),
            &bucket,
            &pointer.key,
        );
        let extras = PolicyRequestExtras::operation("s3.GetObject");
        let realm_id = state.get_realm_id();
        if authorize(&context, realm_id, &auth, &path, &Permission::READ, extras)
            .await
            .is_ok()
        {
            releases.push(ReleaseItem {
                key: pointer.key,
                version_id: version_id.to_string(),
                label: pointer.label,
                prefix: pointer.prefix,
            });
        }
    }
    let complete = match &info.distribution {
        None => true,
        Some(binding) => read_view(context.as_ref(), binding.set_id)
            .await
            .ok()
            .flatten()
            .is_some_and(|view| {
                let snapshot = view.snapshot();
                snapshot.held_ranges(binding.member_id).len() == snapshot.config.ranges.len()
            }),
    };
    Ok(Json(ReleaseListResponse {
        bucket,
        releases,
        complete,
    }))
}

#[cfg(test)]
#[path = "releases_tests.rs"]
mod tests;
