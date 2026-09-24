//! Serves the RO-Crate upload route and the route that submits an import job.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::path::{Component, Path as FsPath};
use std::sync::{Arc, Mutex};

use aruna_core::StructuredId;
use aruna_core::errors::{BlobError, SourceResolutionError, StagingSourceError};
use aruna_core::stream::BackendStream;
use aruna_core::structs::execution::job::{
    ImportMetadataTarget, ImportRoCrateSource, ImportRoCrateSpec, ImportRoCrateTarget, JobPayload,
    RoCrateMediaType, user_dedup_key,
};
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::storage::blob::{bucket_permission_path, object_permission_path};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_operations::driver::{drive, drive_until};
use aruna_operations::jobs::import::{
    CreateRoCrateConfig, CreateRoCrateError, CreateRoCrateOperation, load_rocrate_upload,
};
use aruna_operations::jobs::service::{lookup_job_dedup, read_owned_job, submit_rocrate_import};
use aruna_operations::metadata::create_document::mint_job_document;
use aruna_operations::s3::bucket::get::{GetBucketError, GetBucketOperation};
use aruna_operations::s3::object::head::{HeadObjectError, HeadObjectInput, HeadObjectOperation};
use aruna_operations::staging::head_source::{
    HeadSourceError, HeadSourceInput, HeadSourceOperation,
};
use axum::body::Body;
use axum::extract::State;
use axum::http::header::{CONTENT_LENGTH, CONTENT_TYPE};
use axum::http::{HeaderMap, StatusCode};
use axum::{Extension, Json};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures_util::{Stream, StreamExt, stream};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::time::{Instant, timeout_at};
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::auth::{ensure_permission, require_unrestricted_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::execution::jobs::{job_urls, map_submit_error};
use crate::server::state::ServerState;

const ZIP_MEDIA_TYPE: &str = "application/zip";
const ELN_MEDIA_TYPE: &str = "application/vnd.eln+zip";
const UPLOAD_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const UPLOAD_DEADLINE: Duration = Duration::from_secs(30 * 60);

#[derive(OpenApi)]
#[openapi()]
pub struct RoCrateImportDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(RoCrateImportDoc::openapi())
        .routes(routes!(upload_rocrate))
        .routes(routes!(submit_import))
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UploadRoCrateResponse {
    pub upload_id: String,
    pub blake3: String,
    pub size: u64,
    pub expires_at: String,
    pub owner_node_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ImportSourceRequest {
    Invenio {
        group_id: String,
        connector_id: String,
        record_id: String,
        #[serde(flatten)]
        options: super::invenio::InvenioOptionsRequest,
    },
    Upload {
        upload_id: String,
    },
    Object {
        bucket: String,
        key: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        version: Option<String>,
    },
    Connector {
        group_id: String,
        connector_id: String,
        path: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ImportTargetRequest {
    pub bucket: String,
    pub prefix: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ImportMetadataRequest {
    pub group_id: String,
    pub path: String,
    pub public: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SubmitImportRequest {
    pub source: ImportSourceRequest,
    pub target: ImportTargetRequest,
    pub metadata: ImportMetadataRequest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SubmitImportResponse {
    pub job_id: String,
    pub created: bool,
    pub owner_node_url: String,
    pub status_url: String,
    pub report_url: String,
}

#[utoipa::path(
    post,
    path = "/metadata/rocrate/uploads",
    tag = "metadata/rocrate",
    summary = "Upload an RO-Crate archive for a later import",
    description = r#"Stores an RO-Crate archive privately on this node so a later import can claim it.

**Authentication**: unrestricted bearer token issued by this realm; a token from another realm and
a path-restricted delegated token are both refused.

**Behavior**
- Bytes are hashed and spooled as they arrive, so the archive never has to fit in memory.
- The upload is private to the caller and to this node, whose base URL is returned as
  `owner_node_url`.
- Nothing is unpacked, validated or imported until the import route is called with the returned
  `upload_id`, and the upload is single-use.
- The upload expires at `expires_at`, one day after creation by default, after which it is swept
  and an import naming it fails.

**Limits**
- The body is the archive itself as `application/zip` or `application/vnd.eln+zip`; media-type
  parameters after a semicolon are allowed, any other type is rejected before the body is read.
- The size cap is the node's configured direct-upload limit, 8 GiB by default, enforced from
  `Content-Length` when it is present and otherwise while streaming.
- A body that delivers nothing for 30 seconds, or a transfer still running after 30 minutes, is
  aborted as a failed transfer rather than a 413."#,
    request_body(
        content(
            (String = "application/zip"),
            (String = "application/vnd.eln+zip")
        ),
        description = "The RO-Crate archive as a raw binary body; no multipart or form encoding and no wrapper JSON"
    ),
    responses(
        (
            status = 201,
            description = "The archive is stored privately for this caller on this node; its bytes are not yet unpacked or validated",
            body = UploadRoCrateResponse,
            example = json!({
                "upload_id": "01JABCDEF0123456789ABCDEFG",
                "blake3": "9f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978",
                "size": 20971520,
                "expires_at": "2026-04-10T14:23:11.123+00:00",
                "owner_node_url": "https://node.example.test/api/v1"
            })
        ),
        (status = 400, description = "Content-Type is neither application/zip nor application/vnd.eln+zip", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm or is a path-restricted delegated token", body = ErrorResponse),
        (status = 413, description = "The archive exceeds the node's direct-upload cap; the partial spool is discarded", body = ErrorResponse),
        (status = 503, description = "Upload capacity exhausted or blob storage unavailable; retryable", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn upload_rocrate(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    headers: HeaderMap,
    body: Body,
) -> ServerResult<(StatusCode, Json<UploadRoCrateResponse>)> {
    let deadline = Instant::now() + UPLOAD_DEADLINE;
    let auth = require_unrestricted_auth(&state, auth)?;
    let media_type = parse_media_type(&headers)?;
    let limit = state.rocrate_limits().direct_upload_bytes;
    if headers
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .is_some_and(|size| size > limit)
    {
        return Err(ServerError::PayloadTooLarge(format!(
            "upload exceeds limit {limit}"
        )));
    }
    let Some(upload_slot) = state.try_rocrate_slot() else {
        return Err(ServerError::ServiceUnavailableReason(
            "RO-Crate upload capacity is temporarily exhausted".to_string(),
        ));
    };
    let expires_at_ms = aruna_core::time::unix_timestamp_millis()
        .checked_add(state.rocrate_limits().upload_retention_ms)
        .ok_or_else(|| ServerError::InternalError("upload expiry overflow".to_string()))?;
    let timestamp = i64::try_from(expires_at_ms)
        .ok()
        .and_then(DateTime::<Utc>::from_timestamp_millis)
        .ok_or_else(|| ServerError::InternalError("upload expiry is invalid".to_string()))?;
    let owner_node_url = timeout_at(deadline, owner_node_url(&state))
        .await
        .map_err(|_| {
            ServerError::ServiceUnavailableReason("RO-Crate upload deadline expired".to_string())
        })??;
    let upload_id = Ulid::generate();
    let record = drive_until(
        CreateRoCrateOperation::new(CreateRoCrateConfig {
            upload_id,
            owner: auth.user_id,
            media_type,
            expires_at_ms,
            max_bytes: limit,
            deadline: Some(deadline.into_std()),
            blob: upload_body_stream(body, deadline),
        }),
        &state.get_ctx(),
        deadline,
    )
    .await
    .map_err(map_upload_error)?;
    drop(upload_slot);
    Ok((
        StatusCode::CREATED,
        Json(UploadRoCrateResponse {
            upload_id: upload_id.to_string(),
            blake3: hex::encode(record.blake3),
            size: record.size,
            expires_at: timestamp.to_rfc3339(),
            owner_node_url,
        }),
    ))
}

#[utoipa::path(
    post,
    path = "/metadata/rocrate/imports",
    tag = "metadata/rocrate",
    summary = "Submit an RO-Crate import job",
    description = r#"Accepts an RO-Crate import plan and durably records it as a job on this node.

**Authentication**: unrestricted bearer token issued by this realm; a token from another realm and
a path-restricted delegated token are both refused. The whole plan is authorized before the job is
taken: READ on the source, WRITE on the target bucket, and WRITE on the metadata document path.

**Behavior**
- An upload source is readable only when it is the caller's own, unclaimed, unexpired and within
  the import-source cap; an object or connector source needs READ on it.
- RO-Crate 1.2 and 1.3 contexts and specification IRIs are accepted, structurally validated, and
  retained in the published metadata and later exports.
- Nothing is unpacked, written or published when the job is recorded; progress is followed at
  `status_url` and the per-entry outcome at `report_url`, both on the owning node.
- Send `idempotency_key` to make retries safe: a repeat with the same key and the same plan replays
  the original job and returns `created` false.

**Limits**
- A caller holds at most 4 active import jobs.
- Bucket, prefix and metadata paths must be non-empty and within the configured key size, and a
  prefix or connector path may not contain dot segments, backslashes or control characters."#,
    request_body(
        content = SubmitImportRequest,
        description = "The import plan: where the archive comes from, which bucket and prefix its payload lands in, and which metadata document the crate is published as",
        example = json!({
            "source": {
                "kind": "upload",
                "upload_id": "01JABCDEF0123456789ABCDEFG"
            },
            "target": {
                "bucket": "lab-raw",
                "prefix": "imports/rna-seq"
            },
            "metadata": {
                "group_id": "01JGROUP0123456789ABCDEFGH",
                "path": "datasets/rna-seq",
                "public": true
            },
            "idempotency_key": "rna-seq-2026-04-09"
        })
    ),
    responses(
        (
            status = 202,
            description = "The plan passed authorization and the job is durably recorded; the import itself runs afterwards",
            body = SubmitImportResponse,
            example = json!({
                "job_id": "01JJOB0123456789ABCDEFGHIJ",
                "created": true,
                "owner_node_url": "https://node.example.test/api/v1",
                "status_url": "https://node.example.test/api/v1/compute/jobs/01JJOB0123456789ABCDEFGHIJ",
                "report_url": "https://node.example.test/api/v1/compute/jobs/01JJOB0123456789ABCDEFGHIJ/report"
            })
        ),
        (status = 400, description = "Malformed ids, an empty or oversized bucket, prefix or metadata path, an unsafe path segment, or an expired or oversized source", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm, is a path-restricted delegated token, names another user's upload, or lacks READ on the source or WRITE on the target bucket or metadata path", body = ErrorResponse),
        (status = 404, description = "The upload, source object or version, connector source, or target bucket does not exist", body = ErrorResponse),
        (status = 409, description = "Idempotency key bound to a different plan, an upload already claimed by another job, a reached active-job cap, or a standing compute quota refusal, which reports the exact scope, dimension and numbers in `quota`", body = ErrorResponse),
        (status = 502, description = "The connector source's staging backend could not be reached; retryable", body = ErrorResponse),
        (status = 503, description = "The job could not be placed right now; the unchanged request may be retried", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn submit_import(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<SubmitImportRequest>,
) -> ServerResult<(StatusCode, Json<SubmitImportResponse>)> {
    let auth = require_unrestricted_auth(&state, auth)?;
    let source = parse_import_source(request.source)?;
    let target = parse_import_target(request.target, state.rocrate_limits().key_bytes)?;
    let metadata = parse_import_metadata(request.metadata, state.rocrate_limits().key_bytes)?;
    let mut spec = ImportRoCrateSpec {
        auth_context: auth,
        source,
        target,
        metadata,
        limits: state.rocrate_limits().clone(),
        document_id: Ulid::nil(),
    };
    let replay = if let Some(idempotency_key) = request.idempotency_key.as_deref() {
        lookup_job_dedup(
            &state.get_ctx(),
            spec.auth_context.user_id,
            idempotency_key,
            JobPayload::ImportRoCrate(spec.clone()).plan_digest(),
        )
        .await
        .map_err(map_submit_error)?
    } else {
        None
    };
    let result = if let Some(result) = replay {
        result
    } else {
        let actor = Actor {
            node_id: state.get_node_id(),
            user_id: spec.auth_context.user_id,
            realm_id: state.get_realm_id(),
        };
        let document_id = mint_job_document(
            state.get_ctx().as_ref(),
            &actor,
            spec.metadata.group_id,
            &spec.metadata.path,
        )
        .await
        .map_err(crate::metadata::map_create_error)?
        .as_ulid();
        spec.document_id = document_id;
        fast_source_check(
            &state,
            &spec.auth_context,
            &spec.source,
            request.idempotency_key.as_deref(),
        )
        .await?;
        fast_target_check(&state, &spec.auth_context, &spec.target).await?;
        fast_metadata_check(&state, &spec.auth_context, &spec.metadata, document_id).await?;
        submit_rocrate_import(
            &state.get_ctx(),
            spec,
            state.get_node_id(),
            request.idempotency_key,
        )
        .await
        .map_err(map_submit_error)?
    };
    let urls = job_urls(&state, result.job_id).await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(SubmitImportResponse {
            job_id: result.job_id.to_string(),
            created: result.created,
            owner_node_url: urls.owner_node_url,
            status_url: urls.status_url,
            report_url: urls.report_url,
        }),
    ))
}

fn parse_import_source(source: ImportSourceRequest) -> ServerResult<ImportRoCrateSource> {
    match source {
        ImportSourceRequest::Invenio {
            group_id,
            connector_id,
            record_id,
            options,
        } => {
            aruna_core::invenio::validate_id(&record_id)
                .map_err(|error| ServerError::BadRequestReason(error.to_string()))?;
            Ok(ImportRoCrateSource::Invenio {
                options: options.into(),
                group_id: parse_ulid(&group_id)?,
                connector_id: parse_ulid(&connector_id)?,
                record_id,
                pull: None,
            })
        }
        ImportSourceRequest::Upload { upload_id } => Ok(ImportRoCrateSource::Upload {
            upload_id: parse_ulid(&upload_id)?,
        }),
        ImportSourceRequest::Object {
            bucket,
            key,
            version,
        } => {
            if bucket.is_empty() || key.is_empty() {
                return Err(ServerError::BadRequest);
            }
            Ok(ImportRoCrateSource::Object {
                bucket,
                key,
                version: version.as_deref().map(parse_ulid).transpose()?,
            })
        }
        ImportSourceRequest::Connector {
            group_id,
            connector_id,
            path,
        } => {
            validate_source_path(&path)?;
            Ok(ImportRoCrateSource::Connector {
                group_id: parse_ulid(&group_id)?,
                connector_id: parse_ulid(&connector_id)?,
                path,
            })
        }
    }
}

fn parse_import_target(
    target: ImportTargetRequest,
    key_limit: u64,
) -> ServerResult<ImportRoCrateTarget> {
    let prefix = target.prefix.trim_matches('/').to_string();
    if target.bucket.is_empty()
        || prefix.len() as u64 > key_limit
        || prefix.contains('\\')
        || prefix.chars().any(char::is_control)
        || (!prefix.is_empty()
            && prefix
                .split('/')
                .any(|part| part.is_empty() || part == "." || part == ".."))
    {
        return Err(ServerError::BadRequest);
    }
    Ok(ImportRoCrateTarget {
        bucket: target.bucket,
        prefix,
    })
}

fn parse_import_metadata(
    metadata: ImportMetadataRequest,
    key_limit: u64,
) -> ServerResult<ImportMetadataTarget> {
    let path = MetadataRegistryRecord::normalize_document_path(&metadata.path);
    if path.is_empty() || path.len() as u64 > key_limit {
        return Err(ServerError::BadRequest);
    }
    Ok(ImportMetadataTarget {
        group_id: parse_ulid(&metadata.group_id)?,
        path,
        public: metadata.public,
    })
}

async fn fast_source_check(
    state: &ServerState,
    auth: &AuthContext,
    source: &ImportRoCrateSource,
    idempotency_key: Option<&str>,
) -> ServerResult<()> {
    match source {
        ImportRoCrateSource::Invenio { group_id, .. } => {
            crate::metadata::ensure_metadata_scope(state, auth, *group_id, Permission::READ).await
        }
        ImportRoCrateSource::Upload { upload_id } => {
            let record = load_rocrate_upload(&state.get_ctx(), *upload_id)
                .await
                .map_err(ServerError::InternalError)?
                .ok_or(ServerError::NotFound)?;
            if record.owner != auth.user_id {
                return Err(ServerError::Forbidden);
            }
            let reclaimed = if let Some(job_id) = record.claimed_by {
                let dedup_key = idempotency_key.map(|key| user_dedup_key(auth.user_id, key));
                let claimed = read_owned_job(&state.get_ctx(), auth.user_id, job_id)
                    .await
                    .map_err(ServerError::InternalError)?;
                let same_identity = matches!(
                    (
                        claimed.as_ref().and_then(|job| job.dedup_key.as_ref()),
                        dedup_key.as_ref(),
                    ),
                    (Some(existing), Some(requested)) if existing == requested
                );
                if !same_identity {
                    return Err(ServerError::Conflict(format!(
                        "upload is already claimed by job {job_id}"
                    )));
                }
                true
            } else {
                false
            };
            if !reclaimed && record.expires_at_ms <= aruna_core::time::unix_timestamp_millis() {
                return Err(ServerError::BadRequestReason("upload expired".to_string()));
            }
            if record.size > state.rocrate_limits().import_source_bytes {
                return Err(ServerError::BadRequestReason(
                    "upload exceeds the import source cap".to_string(),
                ));
            }
            Ok(())
        }
        ImportRoCrateSource::Object {
            bucket,
            key,
            version,
        } => {
            let bucket_info = load_bucket(state, bucket).await?;
            ensure_permission(
                state,
                auth,
                object_permission_path(
                    state.get_realm_id(),
                    bucket_info.group_id,
                    state.get_node_id(),
                    bucket,
                    key,
                ),
                Permission::READ,
            )
            .await?;
            let result = match drive(
                HeadObjectOperation::new(HeadObjectInput {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    version_id: *version,
                }),
                &state.get_ctx(),
            )
            .await
            {
                Ok(result) => result,
                Err(error) => return Err(map_head_error(error)),
            };
            if result
                .location
                .as_ref()
                .map(|location| location.blob_size)
                .or_else(|| {
                    result
                        .source_metadata
                        .as_ref()
                        .map(|metadata| metadata.content_length)
                })
                .is_some_and(|size| size > state.rocrate_limits().import_source_bytes)
            {
                return Err(ServerError::BadRequestReason(
                    "object exceeds the import source cap".to_string(),
                ));
            }
            Ok(())
        }
        ImportRoCrateSource::Connector {
            group_id,
            connector_id,
            path,
        } => {
            ensure_permission(
                state,
                auth,
                source_permission_path(state, *group_id, *connector_id, path),
                Permission::READ,
            )
            .await?;
            let result = drive(
                HeadSourceOperation::new(HeadSourceInput {
                    group_id: *group_id,
                    connector_id: *connector_id,
                    source_path: path.clone(),
                }),
                &state.get_ctx(),
            )
            .await
            .map_err(map_source_error)?;
            if result.metadata.content_length > state.rocrate_limits().import_source_bytes {
                return Err(ServerError::BadRequestReason(
                    "connector source exceeds the import source cap".to_string(),
                ));
            }
            Ok(())
        }
    }
}

async fn fast_target_check(
    state: &ServerState,
    auth: &AuthContext,
    target: &ImportRoCrateTarget,
) -> ServerResult<()> {
    let bucket_info = load_bucket(state, &target.bucket).await?;
    ensure_permission(
        state,
        auth,
        bucket_permission_path(
            state.get_realm_id(),
            bucket_info.group_id,
            state.get_node_id(),
            &target.bucket,
        ),
        Permission::WRITE,
    )
    .await?;
    Ok(())
}

async fn fast_metadata_check(
    state: &ServerState,
    auth: &AuthContext,
    metadata: &ImportMetadataTarget,
    document_id: Ulid,
) -> ServerResult<()> {
    ensure_permission(
        state,
        auth,
        MetadataRegistryRecord::permission_path_for(
            &state.get_realm_id(),
            metadata.group_id,
            &metadata.path,
            document_id,
        ),
        Permission::WRITE,
    )
    .await?;
    Ok(())
}

async fn load_bucket(
    state: &ServerState,
    bucket: &str,
) -> ServerResult<aruna_core::structs::storage::blob::BucketInfo> {
    match drive(
        GetBucketOperation::new(bucket.to_string()),
        &state.get_ctx(),
    )
    .await
    {
        Ok(info) => Ok(info),
        Err(GetBucketError::NotFound) => Err(ServerError::NotFound),
        Err(error) => Err(ServerError::InternalError(error.to_string())),
    }
}

fn upload_body_stream(
    body: Body,
    deadline: Instant,
) -> BackendStream<Result<Bytes, aruna_core::stream::StreamError>> {
    let body = body.into_data_stream();
    let body = Mutex::new(Box::pin(stream::unfold(
        (body, deadline, false),
        |(mut body, deadline, finished)| async move {
            if finished {
                return None;
            }
            let idle_deadline = (Instant::now() + UPLOAD_IDLE_TIMEOUT).min(deadline);
            match timeout_at(idle_deadline, body.next()).await {
                Ok(Some(Ok(bytes))) => Some((Ok(bytes), (body, deadline, false))),
                Ok(Some(Err(error))) => Some((
                    Err(std::io::Error::other(error.to_string())),
                    (body, deadline, true),
                )),
                Ok(None) => None,
                Err(_) => Some((
                    Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "RO-Crate upload stream timed out",
                    )),
                    (body, deadline, true),
                )),
            }
        },
    )));
    BackendStream::new(stream::poll_fn(move |cx| {
        let mut body = body.lock().unwrap_or_else(|error| error.into_inner());
        body.as_mut().poll_next(cx)
    }))
}

fn map_upload_error(error: CreateRoCrateError) -> ServerError {
    match error {
        CreateRoCrateError::Blob(BlobError::SizeLimitExceeded { limit }) => {
            ServerError::PayloadTooLarge(format!("upload exceeds limit {limit}"))
        }
        CreateRoCrateError::Blob(BlobError::HandleMissing) => {
            ServerError::ServiceUnavailableReason("blob storage is unavailable".to_string())
        }
        other => ServerError::InternalError(other.to_string()),
    }
}

fn parse_media_type(headers: &HeaderMap) -> ServerResult<RoCrateMediaType> {
    let media_type = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .map(str::trim);
    match media_type {
        Some(ZIP_MEDIA_TYPE) => Ok(RoCrateMediaType::Zip),
        Some(ELN_MEDIA_TYPE) => Ok(RoCrateMediaType::Eln),
        _ => Err(ServerError::BadRequestReason(format!(
            "Content-Type must be {ZIP_MEDIA_TYPE} or {ELN_MEDIA_TYPE}"
        ))),
    }
}

fn parse_ulid(value: &str) -> ServerResult<Ulid> {
    Ulid::from_string(value).map_err(|_| ServerError::BadRequest)
}

fn validate_source_path(path: &str) -> ServerResult<()> {
    let path = path.trim();
    if path.is_empty() || path.split('/').any(|part| part == "." || part == "..") {
        return Err(ServerError::BadRequest);
    }
    if FsPath::new(path)
        .components()
        .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(ServerError::BadRequest);
    }
    Ok(())
}

fn map_head_error(error: HeadObjectError) -> ServerError {
    match error {
        HeadObjectError::NoSuchKey
        | HeadObjectError::NoSuchVersion
        | HeadObjectError::DeleteMarker => ServerError::NotFound,
        other => ServerError::InternalError(other.to_string()),
    }
}

fn map_source_error(error: HeadSourceError) -> ServerError {
    match error {
        HeadSourceError::Resolve(SourceResolutionError::NotFound)
        | HeadSourceError::Staging(StagingSourceError::NotFound) => ServerError::NotFound,
        HeadSourceError::Resolve(
            SourceResolutionError::InvalidSourcePath
            | SourceResolutionError::UnsupportedConnectorKind(_),
        ) => ServerError::BadRequest,
        HeadSourceError::Staging(_) => ServerError::BadGateway,
        other => ServerError::InternalError(other.to_string()),
    }
}

fn source_permission_path(
    state: &ServerState,
    group_id: Ulid,
    connector_id: Ulid,
    path: &str,
) -> String {
    format!(
        "/{}/g/{group_id}/data/{}/_sources/{connector_id}/{path}",
        state.get_realm_id(),
        state.get_node_id()
    )
}

async fn owner_node_url(state: &ServerState) -> ServerResult<String> {
    state
        .interface_state()
        .await
        .rest
        .map(|rest| rest.api_base_url)
        .ok_or_else(|| {
            ServerError::InternalError("REST interface public URL is unavailable".to_string())
        })
}

#[cfg(test)]
#[path = "rocrate_import_tests.rs"]
mod tests;
