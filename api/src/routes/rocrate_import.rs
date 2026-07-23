use std::io;
use std::path::{Component, Path as FsPath};
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use aruna_core::effects::BlobEffect;
use aruna_core::errors::{BlobError, SourceConnectorResolutionError, StagingSourceError};
use aruna_core::events::{BlobEvent, Event};
use aruna_core::stream::BackendStream;
use aruna_core::structs::{
    AuthContext, ImportMetadataTarget, ImportRoCrateSource, ImportRoCrateSpec, ImportRoCrateTarget,
    MetadataRegistryRecord, Permission, RoCrateMediaType, RoCrateUploadRecord,
    blob_bucket_permission_path, blob_object_permission_path, user_dedup_key,
};
use aruna_operations::blob::hidden::delete_hidden;
use aruna_operations::driver::drive;
use aruna_operations::jobs::import::{read_rocrate_upload, write_rocrate_upload};
use aruna_operations::jobs::service::{read_owned_job, submit_rocrate_import};
use aruna_operations::list_metadata_documents::ListMetadataDocumentsOperation;
use aruna_operations::s3::get_bucket_info::{GetBucketInfoError, GetBucketInfoOperation};
use aruna_operations::s3::head_object::{HeadObjectError, HeadObjectInput, HeadObjectOperation};
use aruna_operations::staging::head_source::{
    HeadStagingSourceError, HeadStagingSourceInput, HeadStagingSourceOperation,
};
use axum::body::Body;
use axum::extract::State;
use axum::http::header::{CONTENT_LENGTH, CONTENT_TYPE};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::{Extension, Json, Router};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures_util::{StreamExt, stream};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};

use super::jobs::{job_urls, map_submit_error};
use crate::auth::{ensure_permission, require_unrestricted_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server_state::ServerState;

const ZIP_MEDIA_TYPE: &str = "application/zip";
const ELN_MEDIA_TYPE: &str = "application/vnd.eln+zip";

#[derive(OpenApi)]
#[openapi(
    tags((name = "rocrate-import", description = "RO-Crate upload and import")),
    paths(upload_rocrate, submit_import)
)]
pub struct RoCrateImportApiDoc;

pub fn router() -> Router<Arc<ServerState>> {
    Router::new()
        .route("/metadata/rocrate/uploads", post(upload_rocrate))
        .route("/metadata/rocrate/imports", post(submit_import))
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
    tag = "rocrate-import",
    request_body(
        content(
            (String = "application/zip"),
            (String = "application/vnd.eln+zip")
        ),
        description = "Streamed application/zip or application/vnd.eln+zip body"
    ),
    responses(
        (status = 201, description = "Private upload created", body = UploadRoCrateResponse),
        (status = 400, description = "Unsupported or malformed upload", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 413, description = "Upload exceeds the configured cap", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn upload_rocrate(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    headers: HeaderMap,
    body: Body,
) -> ServerResult<(StatusCode, Json<UploadRoCrateResponse>)> {
    let auth = require_unrestricted_realm_auth(&state, auth)?;
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
    let expires_at_ms = aruna_core::util::unix_timestamp_millis()
        .checked_add(state.rocrate_limits().upload_retention_ms)
        .ok_or_else(|| ServerError::InternalError("upload expiry overflow".to_string()))?;
    let timestamp = i64::try_from(expires_at_ms)
        .ok()
        .and_then(DateTime::<Utc>::from_timestamp_millis)
        .ok_or_else(|| ServerError::InternalError("upload expiry is invalid".to_string()))?;
    let owner_node_url = owner_node_url(&state).await?;
    let upload_id = Ulid::generate();
    let Some(blob_handle) = state.get_ctx().blob_handle.as_ref().cloned() else {
        return Err(ServerError::ServiceUnavailableReason(
            "blob storage is unavailable".to_string(),
        ));
    };
    let event = blob_handle
        .send_blob_effect(BlobEffect::SpoolHidden {
            namespace: upload_id,
            name: "input".to_string(),
            created_by: auth.user_id,
            max_bytes: Some(limit),
            blob: upload_body_stream(body),
        })
        .await;
    let (location, blake3, size) = match event {
        Event::Blob(BlobEvent::HiddenSpooled {
            location,
            blake3,
            size,
        }) => (location, blake3, size),
        Event::Blob(BlobEvent::Error(BlobError::SizeLimitExceeded { limit })) => {
            return Err(ServerError::PayloadTooLarge(format!(
                "upload exceeds limit {limit}"
            )));
        }
        Event::Blob(BlobEvent::Error(error)) => {
            return Err(ServerError::InternalError(error.to_string()));
        }
        other => {
            return Err(ServerError::InternalError(format!(
                "unexpected hidden upload event: {other:?}"
            )));
        }
    };
    let record = RoCrateUploadRecord {
        upload_id,
        owner: auth.user_id,
        location: location.clone(),
        blake3,
        size,
        media_type,
        expires_at_ms,
        claimed_by: None,
    };
    if let Err(error) = write_rocrate_upload(&state.get_ctx().storage_handle, &record).await {
        let _ = delete_hidden(&state.get_ctx(), &location).await;
        return Err(ServerError::InternalError(error));
    }
    Ok((
        StatusCode::CREATED,
        Json(UploadRoCrateResponse {
            upload_id: upload_id.to_string(),
            blake3: hex::encode(blake3),
            size,
            expires_at: timestamp.to_rfc3339(),
            owner_node_url,
        }),
    ))
}

#[utoipa::path(
    post,
    path = "/metadata/rocrate/imports",
    tag = "rocrate-import",
    request_body = SubmitImportRequest,
    responses(
        (status = 202, description = "Import accepted", body = SubmitImportResponse),
        (status = 400, description = "Invalid import plan", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 404, description = "Source or target not found", body = ErrorResponse),
        (status = 409, description = "Idempotency conflict or active-job cap", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn submit_import(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<SubmitImportRequest>,
) -> ServerResult<(StatusCode, Json<SubmitImportResponse>)> {
    let auth = require_unrestricted_realm_auth(&state, auth)?;
    let document_id = Ulid::generate();
    let source = fast_source_check(
        &state,
        &auth,
        request.source,
        request.idempotency_key.as_deref(),
    )
    .await?;
    let target = fast_target_check(&state, &auth, request.target).await?;
    let metadata = fast_metadata_check(&state, &auth, request.metadata, document_id).await?;
    let spec = ImportRoCrateSpec {
        auth_context: auth,
        source,
        target,
        metadata,
        limits: state.rocrate_limits().clone(),
        document_id,
    };
    let result = submit_rocrate_import(
        &state.get_ctx(),
        spec,
        state.get_node_id(),
        request.idempotency_key,
    )
    .await
    .map_err(map_submit_error)?;
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

async fn fast_source_check(
    state: &ServerState,
    auth: &AuthContext,
    source: ImportSourceRequest,
    idempotency_key: Option<&str>,
) -> ServerResult<ImportRoCrateSource> {
    match source {
        ImportSourceRequest::Upload { upload_id } => {
            let upload_id = parse_ulid(&upload_id)?;
            let record = read_rocrate_upload(&state.get_ctx().storage_handle, upload_id)
                .await
                .map_err(ServerError::InternalError)?
                .ok_or(ServerError::NotFound)?;
            if record.owner != auth.user_id {
                return Err(ServerError::Forbidden);
            }
            if record.expires_at_ms <= aruna_core::util::unix_timestamp_millis() {
                return Err(ServerError::BadRequestReason("upload expired".to_string()));
            }
            if let Some(job_id) = record.claimed_by {
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
            }
            if record.size > state.rocrate_limits().import_source_bytes {
                return Err(ServerError::BadRequestReason(
                    "upload exceeds the import source cap".to_string(),
                ));
            }
            Ok(ImportRoCrateSource::Upload { upload_id })
        }
        ImportSourceRequest::Object {
            bucket,
            key,
            version,
        } => {
            if bucket.is_empty() || key.is_empty() {
                return Err(ServerError::BadRequest);
            }
            let version = version.as_deref().map(parse_ulid).transpose()?;
            let bucket_info = load_bucket(state, &bucket).await?;
            ensure_permission(
                state,
                auth,
                blob_object_permission_path(
                    state.get_realm_id(),
                    bucket_info.group_id,
                    state.get_node_id(),
                    &bucket,
                    &key,
                ),
                Permission::READ,
            )
            .await?;
            let result = match drive(
                HeadObjectOperation::new(HeadObjectInput {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    version_id: version,
                }),
                &state.get_ctx(),
            )
            .await
            {
                Ok(Some(Ok(result))) => result,
                Ok(Some(Err(error))) | Err(error) => return Err(map_head_error(error)),
                Ok(None) => return Err(ServerError::NotFound),
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
            Ok(ImportRoCrateSource::Object {
                bucket,
                key,
                version,
            })
        }
        ImportSourceRequest::Connector {
            group_id,
            connector_id,
            path,
        } => {
            let group_id = parse_ulid(&group_id)?;
            let connector_id = parse_ulid(&connector_id)?;
            validate_source_path(&path)?;
            ensure_permission(
                state,
                auth,
                source_permission_path(state, group_id, connector_id, &path),
                Permission::READ,
            )
            .await?;
            let result = drive(
                HeadStagingSourceOperation::new(HeadStagingSourceInput {
                    group_id,
                    connector_id,
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
            Ok(ImportRoCrateSource::Connector {
                group_id,
                connector_id,
                path,
            })
        }
    }
}

async fn fast_target_check(
    state: &ServerState,
    auth: &AuthContext,
    target: ImportTargetRequest,
) -> ServerResult<ImportRoCrateTarget> {
    let prefix = target.prefix.trim_matches('/').to_string();
    if target.bucket.is_empty()
        || prefix.len() as u64 > state.rocrate_limits().key_bytes
        || prefix.contains('\\')
        || prefix.chars().any(char::is_control)
        || (!prefix.is_empty()
            && prefix
                .split('/')
                .any(|part| part.is_empty() || part == "." || part == ".."))
    {
        return Err(ServerError::BadRequest);
    }
    let bucket_info = load_bucket(state, &target.bucket).await?;
    ensure_permission(
        state,
        auth,
        blob_bucket_permission_path(
            state.get_realm_id(),
            bucket_info.group_id,
            state.get_node_id(),
            &target.bucket,
        ),
        Permission::WRITE,
    )
    .await?;
    Ok(ImportRoCrateTarget {
        bucket: target.bucket,
        prefix,
    })
}

async fn fast_metadata_check(
    state: &ServerState,
    auth: &AuthContext,
    metadata: ImportMetadataRequest,
    document_id: Ulid,
) -> ServerResult<ImportMetadataTarget> {
    let group_id = parse_ulid(&metadata.group_id)?;
    let path = MetadataRegistryRecord::normalize_document_path(&metadata.path);
    if path.is_empty() || path.len() as u64 > state.rocrate_limits().key_bytes {
        return Err(ServerError::BadRequest);
    }
    ensure_permission(
        state,
        auth,
        MetadataRegistryRecord::permission_path_for(
            &state.get_realm_id(),
            group_id,
            &path,
            document_id,
        ),
        Permission::WRITE,
    )
    .await?;
    let records = drive(
        ListMetadataDocumentsOperation::new(group_id),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| ServerError::InternalError(error.to_string()))?;
    if records
        .iter()
        .any(|record| record.document_path == path && record.document_id != document_id)
    {
        return Err(ServerError::Conflict(format!(
            "metadata path `{path}` already exists"
        )));
    }
    Ok(ImportMetadataTarget {
        group_id,
        path,
        public: metadata.public,
    })
}

async fn load_bucket(
    state: &ServerState,
    bucket: &str,
) -> ServerResult<aruna_core::structs::BucketInfo> {
    match drive(
        GetBucketInfoOperation::new(bucket.to_string()),
        &state.get_ctx(),
    )
    .await
    {
        Ok(Some(Ok(info))) => Ok(info),
        Ok(Some(Err(GetBucketInfoError::NotFound))) | Ok(None) => Err(ServerError::NotFound),
        Ok(Some(Err(error))) | Err(error) => Err(ServerError::InternalError(error.to_string())),
    }
}

fn upload_body_stream(body: Body) -> BackendStream<Result<Bytes, aruna_core::stream::StreamError>> {
    let (sender, receiver) = mpsc::channel::<Result<Bytes, io::Error>>(8);
    tokio::spawn(async move {
        let mut body = body.into_data_stream();
        while let Some(chunk) = body.next().await {
            let chunk = chunk.map_err(|error| io::Error::other(error.to_string()));
            if sender.send(chunk).await.is_err() {
                break;
            }
        }
    });
    let receiver = Arc::new(Mutex::new(receiver));
    BackendStream::new(stream::poll_fn(move |cx| {
        let mut receiver = receiver.lock().expect("upload receiver mutex poisoned");
        Pin::new(&mut *receiver).poll_recv(cx)
    }))
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

fn map_source_error(error: HeadStagingSourceError) -> ServerError {
    match error {
        HeadStagingSourceError::Resolve(SourceConnectorResolutionError::NotFound)
        | HeadStagingSourceError::Staging(StagingSourceError::NotFound) => ServerError::NotFound,
        HeadStagingSourceError::Resolve(
            SourceConnectorResolutionError::InvalidSourcePath
            | SourceConnectorResolutionError::UnsupportedConnectorKind(_),
        ) => ServerError::BadRequest,
        HeadStagingSourceError::Staging(_) => ServerError::BadGateway,
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
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn upload_accepts_types() {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static(ZIP_MEDIA_TYPE));
        assert!(matches!(
            parse_media_type(&headers),
            Ok(RoCrateMediaType::Zip)
        ));
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/vnd.eln+zip; version=1"),
        );
        assert!(matches!(
            parse_media_type(&headers),
            Ok(RoCrateMediaType::Eln)
        ));
    }

    #[test]
    fn upload_openapi_types() {
        let openapi = serde_json::to_value(RoCrateImportApiDoc::openapi()).unwrap();
        let content =
            &openapi["paths"]["/metadata/rocrate/uploads"]["post"]["requestBody"]["content"];
        assert!(content.get(ZIP_MEDIA_TYPE).is_some());
        assert!(content.get(ELN_MEDIA_TYPE).is_some());
    }

    #[test]
    fn upload_rejects_type() {
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/octet-stream"),
        );
        assert!(parse_media_type(&headers).is_err());
    }

    #[test]
    fn source_path_checks() {
        assert!(validate_source_path("folder/file.zip").is_ok());
        assert!(validate_source_path("../file.zip").is_err());
        assert!(validate_source_path("/file.zip").is_err());
    }
}
