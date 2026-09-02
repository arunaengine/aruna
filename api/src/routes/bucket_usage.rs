use std::sync::Arc;

use aruna_core::structs::{AuthContext, Permission, blob_bucket_permission_path};
use aruna_operations::driver::drive;
use aruna_operations::s3::bucket_usage::{
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
use crate::routes::storage_deletion::bucket_info;
use crate::server_state::ServerState;

const DEFAULT_USAGE_LIMIT: usize = 10_000;
const MAX_USAGE_LIMIT: usize = 100_000;

#[derive(OpenApi)]
#[openapi()]
pub struct BucketUsageApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(BucketUsageApiDoc::openapi()).routes(routes!(get_bucket_usage))
}

#[derive(Debug, Clone, Deserialize, IntoParams)]
pub struct BucketUsageQuery {
    /// Scan bound for both inventories, 1 to 100000, default 10000.
    #[serde(default)]
    pub limit: Option<usize>,
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
    /// Bytes over all stored versions counted above.
    pub logical_bytes: u64,
    /// False means the scan hit its bound and every number is a lower bound.
    pub complete: bool,
}

impl BucketUsageResponse {
    fn new(bucket: String, totals: BucketUsageOutput) -> Self {
        Self {
            bucket,
            objects: totals.objects,
            versions: totals.versions,
            delete_markers: totals.delete_markers,
            open_multipart_uploads: totals.open_multipart_uploads,
            logical_bytes: totals.logical_bytes,
            complete: totals.complete,
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
- There is no maintained per-bucket counter, so the numbers come from a bounded scan of this node's
  own rows and cost grows with the bucket.
- `complete` false means the scan hit `limit`: every number is then a lower bound.
- `objects` counts current heads that are not delete markers, `versions` counts every stored
  version including noncurrent ones, and `logical_bytes` sums the bytes of those versions.
- Bytes this node may not describe, such as a governed copy it holds no registration for, count as
  zero rather than failing the request.

**Limits**
- `limit` accepts 1 to 100000 and defaults to 10000."#,
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
                "versions": 194,
                "delete_markers": 3,
                "open_multipart_uploads": 1,
                "logical_bytes": 84291337,
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
    let limit = query.limit.unwrap_or(DEFAULT_USAGE_LIMIT);
    if !(1..=MAX_USAGE_LIMIT).contains(&limit) {
        return Err(ServerError::BadRequest);
    }
    let info = bucket_info(&state, &bucket).await?;
    ensure_permission(
        &state,
        &auth,
        blob_bucket_permission_path(
            state.get_realm_id(),
            info.group_id,
            state.get_node_id(),
            &bucket,
        ),
        Permission::READ,
    )
    .await?;
    let totals = drive(
        BucketUsageOperation::new(BucketUsageInput {
            bucket: bucket.clone(),
            limit,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| ServerError::InternalError(error.to_string()))?;
    Ok(Json(BucketUsageResponse::new(bucket, totals)))
}
