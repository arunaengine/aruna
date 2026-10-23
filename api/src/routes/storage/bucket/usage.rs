//! Serves the bucket usage route that counts objects, versions and bytes of one bucket.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::bucket_permission_path;
use aruna_core::structs::storage::usage::{BucketCounters, usage_bucket_key};
use aruna_operations::driver::drive;
use aruna_operations::s3::multipart::uploads::{ListUploadsInput, ListUploadsOperation};
use axum::extract::{Path, State};
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::auth::{ensure_permission, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::storage::deletion::bucket_info;
use crate::server::state::ServerState;

#[derive(OpenApi)]
#[openapi()]
pub struct BucketUsageDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(BucketUsageDoc::openapi()).routes(routes!(get_bucket_usage))
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BucketUsageResponse {
    pub bucket: String,
    /// Current heads that are not delete markers.
    pub objects: u64,
    /// Stored versions including noncurrent ones, excluding delete markers.
    pub versions: u64,
    pub delete_markers: u64,
    pub open_multipart_uploads: u64,
    /// Bytes of all stored versions, the group quota basis.
    pub logical_bytes: u64,
    /// Bytes reachable only through external references.
    pub referenced_bytes: u64,
    /// Always true: every number is counted in full.
    pub complete: bool,
}

impl BucketUsageResponse {
    fn new(bucket: String, counters: BucketCounters, uploads: u64) -> Self {
        Self {
            bucket,
            objects: counters.objects,
            versions: counters.versions,
            delete_markers: counters.delete_markers,
            open_multipart_uploads: uploads,
            logical_bytes: counters.logical_bytes,
            referenced_bytes: counters.referenced_bytes,
            complete: true,
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
- Every number except `open_multipart_uploads` comes from this node's counter of the bucket,
  kept up to date in the same transaction as each write and delete, so the answer costs one read
  however large the bucket is.
- `objects` counts current heads that are not delete markers, `versions` every stored version
  including noncurrent ones, and `logical_bytes` the bytes of those versions, as the group quota
  does. `referenced_bytes` sums the sizes of versions that only point to external data.
- `open_multipart_uploads` is counted by a full pass over this node's open uploads, read in short
  batches.
- `complete` is always true."#,
    params(
        ("bucket" = String, Path, description = "Bucket name as used on the S3 surface, without a leading slash")
    ),
    responses(
        (
            status = 200,
            description = "The bucket's node-local inventory and byte total",
            body = BucketUsageResponse,
            example = json!({
                "bucket": "research-raw",
                "objects": 128,
                "versions": 194,
                "delete_markers": 3,
                "open_multipart_uploads": 1,
                "logical_bytes": 84291337,
                "referenced_bytes": 0,
                "complete": true
            })
        ),
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
) -> ServerResult<Json<BucketUsageResponse>> {
    let auth = require_realm_auth(&state, auth)?;
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
    let counters = bucket_counters(&state, &bucket).await?;
    let input = ListUploadsInput {
        bucket: bucket.clone(),
        prefix: None,
        delimiter: None,
        key_marker: None,
        upload_id_marker: None,
        max_uploads: usize::MAX,
    };
    let uploads = drive(
        ListUploadsOperation::new(input).complete_scan(),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| ServerError::InternalError(error.to_string()))?;
    let uploads = uploads.uploads.len() as u64;
    Ok(Json(BucketUsageResponse::new(bucket, counters, uploads)))
}

/// The bucket's counter row; a bucket without one holds nothing yet.
async fn bucket_counters(state: &ServerState, bucket: &str) -> ServerResult<BucketCounters> {
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::USAGE_STATS_KEYSPACE;

    let read = StorageEffect::Read {
        key_space: USAGE_STATS_KEYSPACE.to_string(),
        key: usage_bucket_key(bucket).into(),
        txn_id: None,
    };
    let value = match state
        .get_ctx()
        .storage_handle
        .send_storage_effect(read)
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
        other => {
            let error = format!("could not read the bucket counter: {other:?}");
            return Err(ServerError::InternalError(error));
        }
    };
    let counters = value.as_deref().map(BucketCounters::from_bytes).transpose();
    let counters = counters.map_err(|error| ServerError::InternalError(error.to_string()))?;
    Ok(counters.unwrap_or_default())
}
