//! Serves the bucket usage route that counts objects, versions and bytes of one bucket.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::bucket_permission_path;
use aruna_core::structs::storage::usage::{UsageCounters, usage_bucket_key};
use aruna_operations::driver::drive;
use aruna_operations::s3::bucket::usage::{
    BucketUsageInput, BucketUsageOperation, BucketUsageOutput,
};
use axum::extract::{Path, Query, State};
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::auth::{ensure_permission, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::info::load_usage_counters;
use crate::routes::storage::deletion::bucket_info;
use crate::server::state::ServerState;

const MAX_USAGE_LIMIT: usize = 100_000;

#[derive(OpenApi)]
#[openapi()]
pub struct BucketUsageDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(BucketUsageDoc::openapi()).routes(routes!(get_bucket_usage))
}

#[derive(Debug, Clone, Deserialize, IntoParams)]
pub struct BucketUsageQuery {
    /// Also scans up to this many rows of each inventory for `versions`, `delete_markers` and
    /// `open_multipart_uploads`, 1 to 100000.
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BucketUsageResponse {
    pub bucket: String,
    /// Current heads that are not delete markers.
    pub objects: u64,
    /// Stored versions including noncurrent ones, excluding delete markers; only with `limit`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub versions: Option<u64>,
    /// Only with `limit`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delete_markers: Option<u64>,
    /// Only with `limit`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub open_multipart_uploads: Option<u64>,
    /// Bytes of all stored versions, the group quota basis.
    pub logical_bytes: u64,
    /// Bytes reachable only through external references.
    pub referenced_bytes: u64,
    /// False means the scan asked with `limit` hit it: the scanned numbers are lower bounds.
    pub complete: bool,
}

impl BucketUsageResponse {
    fn new(bucket: String, counters: UsageCounters, scanned: Option<BucketUsageOutput>) -> Self {
        Self {
            bucket,
            objects: counters.objects,
            versions: scanned.map(|totals| totals.versions),
            delete_markers: scanned.map(|totals| totals.delete_markers),
            open_multipart_uploads: scanned.map(|totals| totals.open_multipart_uploads),
            logical_bytes: counters.logical_bytes,
            referenced_bytes: counters.referenced_bytes,
            complete: scanned.is_none_or(|totals| totals.complete),
        }
    }
}

#[utoipa::path(
    get,
    path = "/data/buckets/{bucket}/usage",
    tag = "data/storage",
    summary = "Read one bucket's stored inventory",
    description = r#"Reports how many objects, versions, delete markers and open multipart uploads a bucket holds on this node, and how many bytes they occupy.

**Authentication**: realm bearer token with READ on the bucket, the same right that lists its
objects.

**Behavior**
- `objects`, `logical_bytes` and `referenced_bytes` come from this node's counter of the bucket,
  kept up to date with every write and delete, so they are exact and cost one read.
- `objects` counts current heads that are not delete markers, `logical_bytes` sums the bytes of
  every stored version, as the group quota does, and `referenced_bytes` the sizes of versions
  that only point to external data.
- Only with `limit` does the node also scan its rows for `versions`, every stored version
  including noncurrent ones, `delete_markers` and `open_multipart_uploads`. `complete` false means
  that scan hit `limit` and these three numbers are lower bounds; without `limit` it is true.

**Limits**
- `limit` accepts 1 to 100000."#,
    params(
        ("bucket" = String, Path, description = "Bucket name as used on the S3 surface, without a leading slash"),
        BucketUsageQuery
    ),
    responses(
        (
            status = 200,
            description = "The bucket's node-local inventory and byte total",
            body = BucketUsageResponse,
            example = json!({
                "bucket": "research-raw",
                "objects": 128,
                "logical_bytes": 84291337,
                "referenced_bytes": 0,
                "complete": true
            })
        ),
        (status = 400, description = "The limit is outside the accepted range", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token from another realm, or no READ on the bucket", body = ErrorResponse),
        (status = 404, description = "Bucket not found on this node", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_bucket_usage(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(bucket): Path<String>,
    Query(query): Query<BucketUsageQuery>,
) -> ServerResult<Json<BucketUsageResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    if query
        .limit
        .is_some_and(|limit| !(1..=MAX_USAGE_LIMIT).contains(&limit))
    {
        return Err(ServerError::BadRequest);
    }
    let info = bucket_info(&state, &bucket).await?;
    ensure_permission(
        &state,
        &auth,
        bucket_permission_path(
            state.get_realm_id(),
            info.group_id,
            state.get_node_id(),
            &bucket,
        ),
        Permission::READ,
    )
    .await?;
    let counters = load_usage_counters(&state, usage_bucket_key(&bucket)).await?;
    let scanned = match query.limit {
        Some(limit) => {
            let input = BucketUsageInput {
                bucket: bucket.clone(),
                limit,
            };
            let totals = drive(BucketUsageOperation::new(input), &state.get_ctx()).await;
            Some(totals.map_err(|error| ServerError::InternalError(error.to_string()))?)
        }
        None => None,
    };
    Ok(Json(BucketUsageResponse::new(bucket, counters, scanned)))
}
