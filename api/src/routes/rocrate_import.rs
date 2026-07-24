use std::path::{Component, Path as FsPath};
use std::sync::{Arc, Mutex};

use aruna_core::errors::{BlobError, SourceConnectorResolutionError, StagingSourceError};
use aruna_core::stream::BackendStream;
use aruna_core::structs::{
    AuthContext, ImportMetadataTarget, ImportRoCrateSource, ImportRoCrateSpec, ImportRoCrateTarget,
    JobPayload, MetadataRegistryRecord, Permission, RoCrateMediaType, blob_bucket_permission_path,
    blob_object_permission_path, user_dedup_key,
};
use aruna_operations::driver::drive;
use aruna_operations::jobs::import::{
    CreateRoCrateUploadConfig, CreateRoCrateUploadError, CreateRoCrateUploadOperation,
    load_rocrate_upload,
};
use aruna_operations::jobs::service::{lookup_job_dedup, read_owned_job, submit_rocrate_import};
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
use futures_util::{Stream, stream};
use serde::{Deserialize, Serialize};
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
    let record = drive(
        CreateRoCrateUploadOperation::new(CreateRoCrateUploadConfig {
            upload_id,
            owner: auth.user_id,
            media_type,
            expires_at_ms,
            max_bytes: limit,
            blob: upload_body_stream(body),
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_upload_error)?;
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
    let source = parse_import_source(request.source)?;
    let target = parse_import_target(request.target, state.rocrate_limits().key_bytes)?;
    let metadata = parse_import_metadata(request.metadata, state.rocrate_limits().key_bytes)?;
    let spec = ImportRoCrateSpec {
        auth_context: auth,
        source,
        target,
        metadata,
        limits: state.rocrate_limits().clone(),
        document_id,
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
            if !reclaimed && record.expires_at_ms <= aruna_core::util::unix_timestamp_millis() {
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
                blob_object_permission_path(
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
                HeadStagingSourceOperation::new(HeadStagingSourceInput {
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
        blob_bucket_permission_path(
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
    let records = drive(
        ListMetadataDocumentsOperation::new(metadata.group_id),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| ServerError::InternalError(error.to_string()))?;
    if records
        .iter()
        .any(|record| record.document_path == metadata.path && record.document_id != document_id)
    {
        return Err(ServerError::Conflict(format!(
            "metadata path `{}` already exists",
            metadata.path
        )));
    }
    Ok(())
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
    let body = Mutex::new(Box::pin(body.into_data_stream()));
    BackendStream::new(stream::poll_fn(move |cx| {
        let mut body = body.lock().unwrap_or_else(|error| error.into_inner());
        body.as_mut().poll_next(cx)
    }))
}

fn map_upload_error(error: CreateRoCrateUploadError) -> ServerError {
    match error {
        CreateRoCrateUploadError::Blob(BlobError::SizeLimitExceeded { limit }) => {
            ServerError::PayloadTooLarge(format!("upload exceeds limit {limit}"))
        }
        CreateRoCrateUploadError::Blob(BlobError::HandleMissing) => {
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
    use aruna_blob::blob::BlobHandler;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE, S3_BUCKET_KEYSPACE};
    use aruna_core::structs::{
        Actor, Backend, BackendConfig, BackendLocation, BucketInfo, Group,
        GroupAuthorizationDocument, NodeCapabilities, PathRestriction, RealmAuthorizationDocument,
        RealmId, RoCrateLimits, RoCrateUploadRecord,
    };
    use aruna_core::types::UserId;
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_operations::driver::DriverContext;
    use aruna_operations::jobs::import::write_rocrate_upload;
    use aruna_operations::jobs::runtime::JobsRuntime;
    use aruna_storage::FjallStorage;
    use aruna_tasks::TaskHandle;
    use axum::http::{HeaderValue, Request};
    use byteview::ByteView;
    use std::collections::HashMap;
    use std::time::SystemTime;
    use tempfile::TempDir;
    use tower::ServiceExt;

    fn realm() -> RealmId {
        RealmId::from_bytes([1u8; 32])
    }

    fn auth(user: UserId) -> Option<AuthContext> {
        Some(AuthContext {
            user_id: user,
            realm_id: realm(),
            path_restrictions: None,
        })
    }

    fn restricted(user: UserId) -> Option<AuthContext> {
        Some(AuthContext {
            user_id: user,
            realm_id: realm(),
            path_restrictions: Some(vec![PathRestriction {
                pattern: "/**".to_string(),
                permission: Permission::READ,
            }]),
        })
    }

    fn test_limits() -> RoCrateLimits {
        RoCrateLimits {
            direct_upload_bytes: 1024,
            import_source_bytes: 1024,
            ..RoCrateLimits::default()
        }
    }

    async fn write_doc(state: &ServerState, key_space: &str, key: ByteView, value: ByteView) {
        let event = state
            .get_ctx()
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: key_space.to_string(),
                key,
                value,
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    async fn grant(state: &ServerState, user: UserId, group: Ulid) {
        let realm = realm();
        let actor = Actor {
            node_id: state.get_node_id(),
            user_id: user,
            realm_id: realm,
        };
        let group_auth = GroupAuthorizationDocument::new_default_group_doc(user, realm, group);
        let group_doc = Group {
            display_name: "import-group".to_string(),
            group_id: group,
            realm_id: realm,
            roles: group_auth.roles.keys().copied().collect(),
            owner: user,
        };
        let realm_auth = RealmAuthorizationDocument::new_default_realm_doc(realm);
        write_doc(
            state,
            AUTH_KEYSPACE,
            (*realm.as_bytes()).into(),
            realm_auth.to_bytes(&actor).unwrap().into(),
        )
        .await;
        write_doc(
            state,
            AUTH_KEYSPACE,
            group.to_bytes().into(),
            group_auth.to_bytes(&actor).unwrap().into(),
        )
        .await;
        write_doc(
            state,
            GROUP_KEYSPACE,
            group.to_bytes().into(),
            group_doc.to_bytes(&actor).unwrap().into(),
        )
        .await;
    }

    async fn seed_bucket(state: &ServerState, bucket: &str, group: Ulid, user: UserId) {
        let info = BucketInfo {
            group_id: group,
            created_at: SystemTime::UNIX_EPOCH,
            created_by: user,
            cors_configuration: None,
        };
        write_doc(
            state,
            S3_BUCKET_KEYSPACE,
            bucket.as_bytes().into(),
            info.to_bytes().unwrap().into(),
        )
        .await;
    }

    async fn seed_upload(
        state: &ServerState,
        upload_id: Ulid,
        owner: UserId,
        size: u64,
        expires_at_ms: u64,
    ) {
        let record = RoCrateUploadRecord {
            upload_id,
            owner,
            location: BackendLocation {
                root: "/data".to_string(),
                storage_bucket: "storage".to_string(),
                backend_path: format!("_jobs/{upload_id}/input"),
                ulid: Ulid::from_bytes([9u8; 16]),
                compressed: false,
                encrypted: false,
                created_by: owner,
                created_at: SystemTime::UNIX_EPOCH,
                staging: false,
                partial: false,
                blob_size: size,
                hashes: HashMap::new(),
            },
            blake3: [5u8; 32],
            size,
            media_type: RoCrateMediaType::Zip,
            expires_at_ms,
            claimed_by: None,
        };
        write_rocrate_upload(&state.get_ctx().storage_handle, &record)
            .await
            .unwrap();
    }

    fn submit_request(
        upload_id: Ulid,
        group: Ulid,
        prefix: &str,
        key: Option<&str>,
    ) -> SubmitImportRequest {
        SubmitImportRequest {
            source: ImportSourceRequest::Upload {
                upload_id: upload_id.to_string(),
            },
            target: ImportTargetRequest {
                bucket: "target".to_string(),
                prefix: prefix.to_string(),
            },
            metadata: ImportMetadataRequest {
                group_id: group.to_string(),
                path: "crate".to_string(),
                public: false,
            },
            idempotency_key: key.map(str::to_string),
        }
    }

    async fn plain_state(limits: RoCrateLimits) -> (TempDir, Arc<ServerState>, UserId) {
        let dir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let node_id = iroh::SecretKey::from_bytes(&[7u8; 32]).public();
        let user = UserId::local(Ulid::generate(), realm());
        let ctx = Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: Some(TaskHandle::new()),
            compute_handle: None,
        });
        let state = Arc::new(
            ServerState::new(
                ctx,
                realm(),
                node_id,
                NodeCapabilities::local_node(realm()).unwrap(),
                false,
                None,
                JobsRuntime::new(),
            )
            .await
            .with_rocrate_limits(limits),
        );
        (dir, state, user)
    }

    async fn submit_state() -> (TempDir, Arc<ServerState>, UserId, Ulid) {
        let (dir, state, user) = plain_state(test_limits()).await;
        let group = Ulid::generate();
        grant(&state, user, group).await;
        state
            .register_rest_interface_with_public_url(
                "127.0.0.1:3000".parse().unwrap(),
                Some("https://owner.example/"),
            )
            .await;
        (dir, state, user, group)
    }

    async fn blob_state(limits: RoCrateLimits) -> (TempDir, TempDir, Arc<ServerState>, UserId) {
        let storage_dir = tempfile::tempdir().unwrap();
        let blob_dir = tempfile::tempdir().unwrap();
        let blob_root = blob_dir.path().join("blobstore");
        std::fs::create_dir_all(&blob_root).unwrap();
        let storage = FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let net = NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().unwrap(),
                realm_id: realm(),
                discovery_method: DiscoveryMethod::None,
                relay_method: RelayMethod::None,
                ..NetConfig::default()
            },
            storage.clone(),
        )
        .await
        .unwrap();
        let blob = BlobHandler::new(
            BackendConfig {
                backend_type: Backend::FileSystem,
                root: blob_root.to_str().unwrap().to_string(),
                service_config: HashMap::new(),
                bucket_prefix: Some("aruna-upload-".to_string()),
                max_bucket_size: Some(1_000_000),
                multipart_bucket: Some("multipart".to_string()),
                timeouts: Default::default(),
            },
            storage.clone(),
            net.clone(),
        )
        .await
        .unwrap();
        let node_id = net.node_id();
        let user = UserId::local(Ulid::generate(), realm());
        let ctx = Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: Some(net),
            blob_handle: Some(blob),
            metadata_handle: None,
            task_handle: Some(TaskHandle::new()),
            compute_handle: None,
        });
        let state = Arc::new(
            ServerState::new(
                ctx,
                realm(),
                node_id,
                NodeCapabilities::local_node(realm()).unwrap(),
                false,
                None,
                JobsRuntime::new(),
            )
            .await
            .with_rocrate_limits(limits),
        );
        state
            .register_rest_interface_with_public_url(
                "127.0.0.1:3000".parse().unwrap(),
                Some("https://owner.example/"),
            )
            .await;
        (storage_dir, blob_dir, state, user)
    }

    #[tokio::test]
    async fn upload_streams_hash() {
        let (_storage, _blob, state, user) = blob_state(test_limits()).await;
        let body: &[u8] = b"hello ro-crate payload";
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static(ZIP_MEDIA_TYPE));
        let (status, Json(response)) = upload_rocrate(
            State(state),
            Extension(auth(user)),
            headers,
            Body::from(body),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(response.size, body.len() as u64);
        assert_eq!(response.blake3, hex::encode(blake3::hash(body).as_bytes()));
        assert_eq!(response.owner_node_url, "https://owner.example/api/v1");
        assert!(!response.expires_at.is_empty());
    }

    #[tokio::test]
    async fn upload_accepts_eln() {
        let (_storage, _blob, state, user) = blob_state(test_limits()).await;
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/vnd.eln+zip; version=1"),
        );
        let (status, _) = upload_rocrate(
            State(state),
            Extension(auth(user)),
            headers,
            Body::from(&b"eln bytes"[..]),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::CREATED);
    }

    #[tokio::test]
    async fn upload_rejects_media() {
        let (_dir, state, user) = plain_state(test_limits()).await;
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/octet-stream"),
        );
        let result =
            upload_rocrate(State(state), Extension(auth(user)), headers, Body::empty()).await;
        assert!(matches!(result, Err(ServerError::BadRequestReason(_))));
    }

    #[tokio::test]
    async fn upload_cap_header() {
        let (_dir, state, user) = plain_state(test_limits()).await;
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static(ZIP_MEDIA_TYPE));
        headers.insert(CONTENT_LENGTH, HeaderValue::from_static("2048"));
        let result =
            upload_rocrate(State(state), Extension(auth(user)), headers, Body::empty()).await;
        assert!(matches!(result, Err(ServerError::PayloadTooLarge(_))));
    }

    #[tokio::test]
    async fn upload_streams_capped() {
        // No Content-Length header: the cap must trip mid-stream, not from the header.
        let (_storage, _blob, state, user) = blob_state(RoCrateLimits {
            direct_upload_bytes: 4,
            ..RoCrateLimits::default()
        })
        .await;
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static(ZIP_MEDIA_TYPE));
        let result = upload_rocrate(
            State(state),
            Extension(auth(user)),
            headers,
            Body::from(&b"way past four bytes"[..]),
        )
        .await;
        assert!(matches!(result, Err(ServerError::PayloadTooLarge(_))));
    }

    #[tokio::test]
    async fn upload_requires_auth() {
        let (_dir, state, user) = plain_state(test_limits()).await;
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static(ZIP_MEDIA_TYPE));
        let anonymous = upload_rocrate(
            State(state.clone()),
            Extension(None),
            headers.clone(),
            Body::empty(),
        )
        .await;
        assert!(matches!(anonymous, Err(ServerError::Unauthorized)));
        let delegated = upload_rocrate(
            State(state),
            Extension(restricted(user)),
            headers,
            Body::empty(),
        )
        .await;
        assert!(matches!(delegated, Err(ServerError::Forbidden)));
    }

    #[tokio::test]
    async fn submit_accepts_upload() {
        let (_dir, state, user, group) = submit_state().await;
        let upload_id = Ulid::generate();
        let future = aruna_core::util::unix_timestamp_millis() + 60_000;
        seed_upload(&state, upload_id, user, 1, future).await;
        seed_bucket(&state, "target", group, user).await;
        let (status, Json(response)) = submit_import(
            State(state),
            Extension(auth(user)),
            Json(submit_request(upload_id, group, "crate", None)),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::ACCEPTED);
        assert!(response.created);
        assert!(!response.job_id.is_empty());
        assert_eq!(response.owner_node_url, "https://owner.example/api/v1");
        assert!(
            response
                .status_url
                .ends_with(&format!("/jobs/{}", response.job_id))
        );
        assert!(response.report_url.ends_with("/report"));
    }

    #[tokio::test]
    async fn submit_missing_upload() {
        let (_dir, state, user, group) = submit_state().await;
        let result = submit_import(
            State(state),
            Extension(auth(user)),
            Json(submit_request(Ulid::generate(), group, "crate", None)),
        )
        .await;
        assert!(matches!(result, Err(ServerError::NotFound)));
    }

    #[tokio::test]
    async fn submit_foreign_upload() {
        let (_dir, state, user, group) = submit_state().await;
        let upload_id = Ulid::generate();
        let other = UserId::local(Ulid::generate(), realm());
        seed_upload(
            &state,
            upload_id,
            other,
            1,
            aruna_core::util::unix_timestamp_millis() + 60_000,
        )
        .await;
        let result = submit_import(
            State(state),
            Extension(auth(user)),
            Json(submit_request(upload_id, group, "crate", None)),
        )
        .await;
        assert!(matches!(result, Err(ServerError::Forbidden)));
    }

    #[tokio::test]
    async fn submit_expired_upload() {
        // An unclaimed upload past its retention is rejected at submit time.
        let (_dir, state, user, group) = submit_state().await;
        let upload_id = Ulid::generate();
        seed_upload(&state, upload_id, user, 1, 1).await;
        seed_bucket(&state, "target", group, user).await;
        let result = submit_import(
            State(state),
            Extension(auth(user)),
            Json(submit_request(upload_id, group, "crate", None)),
        )
        .await;
        assert!(matches!(result, Err(ServerError::BadRequestReason(_))));
    }

    #[tokio::test]
    async fn submit_oversized_upload() {
        // An upload larger than the import-source cap is rejected at submit time.
        let (_dir, state, user, group) = submit_state().await;
        let upload_id = Ulid::generate();
        let future = aruna_core::util::unix_timestamp_millis() + 60_000;
        seed_upload(&state, upload_id, user, 2048, future).await;
        seed_bucket(&state, "target", group, user).await;
        let result = submit_import(
            State(state),
            Extension(auth(user)),
            Json(submit_request(upload_id, group, "crate", None)),
        )
        .await;
        assert!(matches!(result, Err(ServerError::BadRequestReason(_))));
    }

    #[tokio::test]
    async fn submit_target_forbidden() {
        // Source passes, but the target bucket belongs to a group the caller cannot write.
        let (_dir, state, user, group) = submit_state().await;
        let upload_id = Ulid::generate();
        seed_upload(
            &state,
            upload_id,
            user,
            1,
            aruna_core::util::unix_timestamp_millis() + 60_000,
        )
        .await;
        let foreign = Ulid::generate();
        grant(&state, UserId::local(Ulid::generate(), realm()), foreign).await;
        seed_bucket(&state, "target", foreign, user).await;
        let result = submit_import(
            State(state),
            Extension(auth(user)),
            Json(submit_request(upload_id, group, "crate", None)),
        )
        .await;
        assert!(matches!(result, Err(ServerError::Forbidden)));
    }

    #[tokio::test]
    async fn submit_dedups_key() {
        let (_dir, state, user, group) = submit_state().await;
        let upload_id = Ulid::generate();
        seed_upload(
            &state,
            upload_id,
            user,
            1,
            aruna_core::util::unix_timestamp_millis() + 60_000,
        )
        .await;
        seed_bucket(&state, "target", group, user).await;
        let (_, Json(first)) = submit_import(
            State(state.clone()),
            Extension(auth(user)),
            Json(submit_request(upload_id, group, "crate", Some("key"))),
        )
        .await
        .unwrap();
        assert!(first.created);
        let (_, Json(replay)) = submit_import(
            State(state.clone()),
            Extension(auth(user)),
            Json(submit_request(upload_id, group, "crate", Some("key"))),
        )
        .await
        .unwrap();
        assert!(!replay.created);
        assert_eq!(replay.job_id, first.job_id);
        let conflict = submit_import(
            State(state),
            Extension(auth(user)),
            Json(submit_request(upload_id, group, "other", Some("key"))),
        )
        .await;
        assert!(matches!(conflict, Err(ServerError::JobPlanConflict(_))));
    }

    #[tokio::test]
    async fn submit_requires_auth() {
        let (_dir, state, user, group) = submit_state().await;
        let request = submit_request(Ulid::generate(), group, "crate", None);
        let anonymous =
            submit_import(State(state.clone()), Extension(None), Json(request.clone())).await;
        assert!(matches!(anonymous, Err(ServerError::Unauthorized)));
        let delegated =
            submit_import(State(state), Extension(restricted(user)), Json(request)).await;
        assert!(matches!(delegated, Err(ServerError::Forbidden)));
    }

    #[tokio::test]
    async fn submit_object_source() {
        // Object source with a nonexistent bucket fails the fast source check.
        let (_dir, state, user, group) = submit_state().await;
        let request = SubmitImportRequest {
            source: ImportSourceRequest::Object {
                bucket: "missing".to_string(),
                key: "crate.zip".to_string(),
                version: None,
            },
            target: ImportTargetRequest {
                bucket: "target".to_string(),
                prefix: "crate".to_string(),
            },
            metadata: ImportMetadataRequest {
                group_id: group.to_string(),
                path: "crate".to_string(),
                public: false,
            },
            idempotency_key: None,
        };
        let result = submit_import(State(state), Extension(auth(user)), Json(request)).await;
        assert!(matches!(result, Err(ServerError::NotFound)));
    }

    #[tokio::test]
    async fn submit_connector_source() {
        // Connector source requires READ; a non-member group is forbidden before staging.
        let (_dir, state, user, group) = submit_state().await;
        let foreign = Ulid::generate();
        grant(&state, UserId::local(Ulid::generate(), realm()), foreign).await;
        let request = SubmitImportRequest {
            source: ImportSourceRequest::Connector {
                group_id: foreign.to_string(),
                connector_id: Ulid::generate().to_string(),
                path: "folder/crate.zip".to_string(),
            },
            target: ImportTargetRequest {
                bucket: "target".to_string(),
                prefix: "crate".to_string(),
            },
            metadata: ImportMetadataRequest {
                group_id: group.to_string(),
                path: "crate".to_string(),
                public: false,
            },
            idempotency_key: None,
        };
        let result = submit_import(State(state), Extension(auth(user)), Json(request)).await;
        assert!(matches!(result, Err(ServerError::Forbidden)));
    }

    #[test]
    fn parses_import_source() {
        let object = parse_import_source(ImportSourceRequest::Object {
            bucket: "b".to_string(),
            key: "k".to_string(),
            version: None,
        })
        .unwrap();
        assert!(matches!(object, ImportRoCrateSource::Object { .. }));
        assert!(
            parse_import_source(ImportSourceRequest::Object {
                bucket: String::new(),
                key: "k".to_string(),
                version: None,
            })
            .is_err()
        );
        let connector = parse_import_source(ImportSourceRequest::Connector {
            group_id: Ulid::generate().to_string(),
            connector_id: Ulid::generate().to_string(),
            path: "a/b".to_string(),
        })
        .unwrap();
        assert!(matches!(connector, ImportRoCrateSource::Connector { .. }));
        assert!(
            parse_import_source(ImportSourceRequest::Connector {
                group_id: "bad".to_string(),
                connector_id: Ulid::generate().to_string(),
                path: "a/b".to_string(),
            })
            .is_err()
        );
        assert!(
            parse_import_source(ImportSourceRequest::Upload {
                upload_id: "bad".to_string(),
            })
            .is_err()
        );
    }

    #[test]
    fn parses_import_target() {
        let target = parse_import_target(
            ImportTargetRequest {
                bucket: "b".to_string(),
                prefix: "/crate/".to_string(),
            },
            1024,
        )
        .unwrap();
        assert_eq!(target.prefix, "crate");
        assert!(
            parse_import_target(
                ImportTargetRequest {
                    bucket: String::new(),
                    prefix: "crate".to_string(),
                },
                1024,
            )
            .is_err()
        );
        assert!(
            parse_import_target(
                ImportTargetRequest {
                    bucket: "b".to_string(),
                    prefix: "a/../b".to_string(),
                },
                1024,
            )
            .is_err()
        );
        assert!(
            parse_import_target(
                ImportTargetRequest {
                    bucket: "b".to_string(),
                    prefix: "x".to_string(),
                },
                0,
            )
            .is_err()
        );
    }

    #[test]
    fn parses_import_metadata() {
        let metadata = parse_import_metadata(
            ImportMetadataRequest {
                group_id: Ulid::generate().to_string(),
                path: "/datasets/crate/".to_string(),
                public: true,
            },
            1024,
        )
        .unwrap();
        assert_eq!(metadata.path, "datasets/crate");
        assert!(metadata.public);
        assert!(
            parse_import_metadata(
                ImportMetadataRequest {
                    group_id: Ulid::generate().to_string(),
                    path: "/".to_string(),
                    public: false,
                },
                1024,
            )
            .is_err()
        );
        assert!(
            parse_import_metadata(
                ImportMetadataRequest {
                    group_id: "bad".to_string(),
                    path: "crate".to_string(),
                    public: false,
                },
                1024,
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn submit_rejects_body() {
        // Malformed JSON is rejected at the route with 400, never reaching the handler.
        let (_dir, state, user, _group) = submit_state().await;
        let request = Request::builder()
            .method("POST")
            .uri("/metadata/rocrate/imports")
            .header(CONTENT_TYPE, "application/json")
            .extension(auth(user))
            .body(Body::from("{ not json"))
            .unwrap();
        let response = router().with_state(state).oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

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
