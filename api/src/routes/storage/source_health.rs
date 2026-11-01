//! Serves this node's measurements of the equal sources of one file version.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::object_permission_path;
use aruna_operations::s3::object::origin::{OriginError, SourceHealth, source_health};
use axum::extract::{Path, Query, State};
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use utoipa::{IntoParams, OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::auth::{ensure_permission, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::storage::deletion::bucket_info;
use crate::server::state::ServerState;

#[derive(OpenApi)]
#[openapi()]
pub struct SourceHealthDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(SourceHealthDoc::openapi()).routes(routes!(get_source_health))
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct SourceHealthParams {
    /// Object key in the bucket.
    pub key: String,
    /// The version; the current version when absent.
    pub version_id: Option<Ulid>,
}

/// What this node measured of one source lately.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SourceStatsBody {
    /// Decaying average time to the first answer, in milliseconds; absent before one.
    pub first_byte_ms: Option<f64>,
    /// Decaying average transfer rate in bytes per second; absent before a long enough read.
    pub bytes_per_sec: Option<f64>,
    /// Share of recent requests the server failed, from 0 to 1, fading while none fail.
    pub error_rate: f64,
    /// Reads streaming from the source now.
    pub in_flight: u32,
    /// Failed lately without answering since; it is tried after the others.
    pub failing: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SourceHealthBody {
    /// Public URL of the object at this source, when the source has one.
    pub url: Option<String>,
    pub path: String,
    /// The connector of this node the source reads through; absent for a public source.
    pub connector_id: Option<String>,
    /// Absent when this node cannot resolve the source, as it lacks its connector.
    pub stats: Option<SourceStatsBody>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SourceHealthResponse {
    /// Every source of the version, in the version's order; empty without an external source.
    pub sources: Vec<SourceHealthBody>,
}

impl From<SourceHealth> for SourceHealthBody {
    fn from(health: SourceHealth) -> Self {
        Self {
            url: health.place.url,
            path: health.place.path,
            connector_id: health.place.connector_id.map(|id| id.to_string()),
            stats: health.stats.map(|stats| SourceStatsBody {
                first_byte_ms: stats.first_byte_ms,
                bytes_per_sec: stats.bytes_per_sec,
                error_rate: stats.error_rate,
                in_flight: stats.in_flight,
                failing: stats.failing,
            }),
        }
    }
}

#[utoipa::path(
    get,
    path = "/data/buckets/{bucket}/sources/health",
    tag = "data/storage",
    summary = "Show how sources of a file version perform",
    description = r#"Lists every source of one file version with what this node measured of it lately.

**Authentication**: realm bearer token with READ on the file.

**Behavior**
- All sources of a version are equal. Each read picks one at random, weighted by its expected
  speed for that read and the connector's optional `weight` setting, and stays on it; failing
  sources are tried last. A small share of reads goes to any healthy source so all stay measured.
- The measurements are this node's own, kept in memory: they start empty after a restart and a
  source unused for ten minutes is measured afresh. Sources of one server share them.
- A source this node cannot resolve, as it lacks its connector, has no `stats`.

**Limits**
- One version per call, answered from this node's copy of the version only."#,
    params(
        ("bucket" = String, Path, description = "Bucket name on this node"),
        SourceHealthParams
    ),
    responses(
        (status = 200, description = "The version's sources", body = SourceHealthResponse, example = json!({
            "sources": [{
                "url": "https://data.example.org/runs/a.fastq",
                "path": "runs/a.fastq",
                "connector_id": "01K8W1B3D5F7H9KMQ1S3V5X7Z2",
                "stats": {"first_byte_ms": 42.5, "bytes_per_sec": 81250000.0, "error_rate": 0.0, "in_flight": 1, "failing": false}
            }, {
                "url": "https://mirror.example.org/runs/a.fastq",
                "path": "runs/a.fastq",
                "connector_id": "01K8W1B3D5F7H9KMQ1S3V5X7Z3",
                "stats": {"first_byte_ms": null, "bytes_per_sec": null, "error_rate": 0.0, "in_flight": 0, "failing": false}
            }]
        })),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "No READ on the file", body = ErrorResponse),
        (status = 404, description = "Bucket or version not found on this node", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_source_health(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(bucket): Path<String>,
    Query(params): Query<SourceHealthParams>,
) -> ServerResult<Json<SourceHealthResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let info = bucket_info(&state, &bucket).await?;
    let path = object_permission_path(
        state.get_realm_id(),
        info.group_id,
        state.get_node_id(),
        &bucket,
        &params.key,
    );
    ensure_permission(&state, &auth, path, Permission::READ).await?;
    let context = state.get_ctx();
    let sources = source_health(
        context.as_ref(),
        info.group_id,
        (&bucket, &params.key),
        params.version_id,
    )
    .await
    .map_err(|error| match error {
        OriginError::BucketMissing | OriginError::VersionMissing => ServerError::NotFound,
        OriginError::Storage(reason) => ServerError::InternalError(reason),
    })?;
    Ok(Json(SourceHealthResponse {
        sources: sources.into_iter().map(Into::into).collect(),
    }))
}
