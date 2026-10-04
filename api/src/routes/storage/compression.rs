//! Serves the bucket compression routes: read and change how new writes are stored.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::routing::ensure_group_admin;
use crate::auth::require_realm_auth;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server::state::ServerState;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::bucket_permission_path;
use aruna_core::structs::storage::format::{Compression, CompressionMigration};
use aruna_operations::driver::{drive, now_ms};
use aruna_operations::s3::bucket::compression::{
    MigrationStatusOperation, PutCompressionError, PutCompressionOperation,
};
use aruna_operations::s3::bucket::get::{GetBucketError, GetBucketOperation};
use aruna_operations::s3::key_status::bucket_settings;
use axum::extract::{Path, State};
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

/// zstd level used when a request enables zstd without naming one.
const DEFAULT_LEVEL: u8 = 3;

#[derive(OpenApi)]
#[openapi()]
pub struct StorageCompressionDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(StorageCompressionDoc::openapi())
        .routes(routes!(get_bucket_compression, put_bucket_compression))
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CompressionMode {
    Off,
    Zstd,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct BucketCompressionRequest {
    pub mode: CompressionMode,
    /// zstd level from 1 to 22; only allowed with `zstd`, which defaults to 3.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub level: Option<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct BucketCompressionResponse {
    pub bucket: String,
    pub mode: CompressionMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub level: Option<u8>,
    /// The zstd level Pithos applies in an encrypted bucket; absent for plain buckets.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_level: Option<u8>,
    /// This node's re-encoding of stored objects after the last change; absent before any change.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub migration: Option<MigrationProgress>,
}

/// Progress of re-encoding this node's stored versions to the current setting.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct MigrationProgress {
    /// Versions moved to the current setting over all passes.
    pub migrated: u64,
    /// Versions the current pass did not need to change.
    pub skipped: u64,
    /// Versions the current or last pass could not move; later passes retry them.
    pub failed: u64,
    /// Passes started again because the pass before them had failures.
    pub retries: u32,
    /// Set while a retry pass waits; it starts at this time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_at_ms: Option<u64>,
    pub started_at_ms: u64,
    /// Set once every version was visited.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at_ms: Option<u64>,
}

impl From<CompressionMigration> for MigrationProgress {
    fn from(migration: CompressionMigration) -> Self {
        Self {
            migrated: migration.migrated,
            skipped: migration.skipped,
            failed: migration.failed,
            retries: migration.retries,
            retry_at_ms: migration.retry_at_ms,
            started_at_ms: migration.started_at_ms,
            finished_at_ms: migration.finished_at_ms,
        }
    }
}

impl BucketCompressionResponse {
    fn new(
        bucket: String,
        compression: Compression,
        migration: Option<CompressionMigration>,
    ) -> Self {
        let (mode, level) = match compression {
            Compression::Off => (CompressionMode::Off, None),
            Compression::Zstd { level } => (CompressionMode::Zstd, Some(level)),
        };
        Self {
            bucket,
            mode,
            level,
            effective_level: None,
            migration: migration.map(Into::into),
        }
    }
}

impl TryFrom<BucketCompressionRequest> for Compression {
    type Error = ServerError;

    fn try_from(request: BucketCompressionRequest) -> Result<Self, Self::Error> {
        match (request.mode, request.level) {
            (CompressionMode::Off, None) => Ok(Compression::Off),
            (CompressionMode::Off, Some(_)) => Err(ServerError::BadRequestReason(
                "`level` is only allowed with `zstd`".to_string(),
            )),
            (CompressionMode::Zstd, level) => Compression::Zstd {
                level: level.unwrap_or(DEFAULT_LEVEL),
            }
            .checked()
            .map_err(|error| ServerError::BadRequestReason(error.to_string())),
        }
    }
}

#[utoipa::path(
    get,
    path = "/data/buckets/{bucket}/storage/compression",
    tag = "data/storage",
    summary = "Read a bucket's compression setting",
    description = r#"Returns the compression this node applies to new writes of a bucket.

**Authentication**: realm bearer token with READ on the bucket.

**Behavior**
- Node-local read of this node's bucket record.
- `mode` is `off` or `zstd`; `level` is present only for `zstd`.
- `migration` reports this node's re-encoding of stored objects after the last change. Buckets on
  other nodes are separate and keep their own setting.
- S3 clients see no difference: sizes, ranges and checksums always refer to the original bytes.
- `effective_level` is the zstd level Pithos applies in an encrypted bucket: the nearest of 1, 4,
  8, 11, 15, 18 and 22, the lower on a tie."#,
    params(("bucket" = String, Path, description = "Bucket name as used on the S3 surface, without a leading slash")),
    responses(
        (
            status = 200,
            description = "The stored compression setting",
            body = BucketCompressionResponse,
            example = json!({
                "bucket": "research-raw",
                "mode": "zstd",
                "level": 3,
                "effective_level": 4,
                "migration": {
                    "migrated": 120,
                    "skipped": 4,
                    "failed": 0,
                    "retries": 0,
                    "started_at_ms": 1790000000000_u64,
                    "finished_at_ms": 1790000060000_u64
                }
            })
        ),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token from another realm, or no READ on the bucket", body = ErrorResponse),
        (status = 404, description = "Bucket not found on this node", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_bucket_compression(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(bucket): Path<String>,
) -> ServerResult<Json<BucketCompressionResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let info = drive(GetBucketOperation::new(bucket.clone()), &state.get_ctx())
        .await
        .map_err(|error| match error {
            GetBucketError::NotFound => ServerError::NotFound,
            other => ServerError::InternalError(other.to_string()),
        })?;
    crate::auth::ensure_permission(
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
    let migration = drive(
        MigrationStatusOperation::new(bucket.clone()),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| ServerError::InternalError(error.to_string()))?;
    let migration = migration.filter(|migration| migration.target == info.compression);
    let encrypted = bucket_settings(&state.get_ctx(), &bucket)
        .await
        .map_err(|error| ServerError::InternalError(error.to_string()))?
        .is_encrypted();
    let mut response = BucketCompressionResponse::new(bucket, info.compression, migration);
    response.effective_level = info.compression.pithos_level().filter(|_| encrypted);
    Ok(Json(response))
}

#[utoipa::path(
    put,
    path = "/data/buckets/{bucket}/storage/compression",
    tag = "data/storage",
    summary = "Change a bucket's compression setting",
    description = r#"Sets how this node stores new writes of a bucket: uncompressed, or zstd with a level.

**Authentication**: realm bearer token with WRITE on the owning group's admin path.

**Behavior**
- Objects are stored as 1 MiB frames; a frame that saves less than 1 KiB or 5 percent stays raw.
- Writes after the change use the new setting. Existing objects on this node are re-encoded in the
  background; buckets on other nodes are not changed.
- Versions that fail are retried automatically in later passes, with a wait that doubles from
  1 minute up to 1 hour, for up to 10 passes. A migration that still has failures then finishes.
- Sending the current setting again resumes a running re-encoding, or restarts one that finished
  or waits with failed versions. The response reports this node's current progress.
- S3 behavior does not change: sizes, ranges and checksums always refer to the original bytes.
- Quotas count original bytes; backend capacity counts stored bytes.

**Limits**
- `level` is 1 to 22 and only allowed with `zstd`; `zstd` without a level uses 3."#,
    params(("bucket" = String, Path, description = "Bucket name as used on the S3 surface, without a leading slash")),
    request_body(
        content = BucketCompressionRequest,
        description = "The new setting. Use `off` to store new writes uncompressed.",
        example = json!({ "mode": "zstd", "level": 9 })
    ),
    responses(
        (
            status = 200,
            description = "The setting as stored",
            body = BucketCompressionResponse,
            example = json!({ "bucket": "research-raw", "mode": "zstd", "level": 9 })
        ),
        (status = 400, description = "Unknown mode, a level outside 1 to 22, or a level with `off`", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token from another realm, or no WRITE on the group admin path", body = ErrorResponse),
        (status = 404, description = "Bucket not found on this node, or no longer owned by the authorized group", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn put_bucket_compression(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(bucket): Path<String>,
    Json(request): Json<BucketCompressionRequest>,
) -> ServerResult<Json<BucketCompressionResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let compression = Compression::try_from(request)?;
    let group_id = drive(GetBucketOperation::new(bucket.clone()), &state.get_ctx())
        .await
        .map_err(|error| match error {
            GetBucketError::NotFound => ServerError::NotFound,
            other => ServerError::InternalError(other.to_string()),
        })?
        .group_id;
    ensure_group_admin(&state, &auth, group_id).await?;
    let migration = drive(
        PutCompressionOperation::new(bucket.clone(), group_id, compression, now_ms()),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| match error {
        PutCompressionError::NoSuchBucket | PutCompressionError::GroupMismatch => {
            ServerError::NotFound
        }
        PutCompressionError::ConversionError(error) => {
            ServerError::BadRequestReason(error.to_string())
        }
        other => ServerError::InternalError(other.to_string()),
    })?;
    Ok(Json(BucketCompressionResponse::new(
        bucket,
        compression,
        migration,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(mode: CompressionMode, level: Option<u8>) -> BucketCompressionRequest {
        BucketCompressionRequest { mode, level }
    }

    #[test]
    fn maps_request_modes() {
        let zstd = Compression::try_from(request(CompressionMode::Zstd, None));
        assert_eq!(zstd.unwrap(), Compression::Zstd { level: 3 });
        let off = Compression::try_from(request(CompressionMode::Off, None));
        assert_eq!(off.unwrap(), Compression::Off);
        assert!(Compression::try_from(request(CompressionMode::Off, Some(3))).is_err());
        assert!(Compression::try_from(request(CompressionMode::Zstd, Some(0))).is_err());
        assert!(Compression::try_from(request(CompressionMode::Zstd, Some(23))).is_err());
    }
}
