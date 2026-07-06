#![allow(clippy::result_large_err)]

use crate::s3::checksum::{
    ApplyChecksums, ChecksumSelection, UploadChecksumRequest, checksum_mode_enabled,
    encode_checksums, parse_upload_checksum_request,
};
use crate::s3::cors::{bucket_cors_to_get_output, dto_to_bucket_cors};
use crate::s3::error::IntoS3Error;
use crate::s3::util::{
    convert_input, multipart_checksum_type_from_s3, parse_completed_part,
    parse_multipart_checksum_hint, parse_multipart_part_number, parse_upload_id, parse_version_id,
    s3_checksum_type_from_multipart, validate_object_key,
};
use aruna_core::NodeId;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::checksum::HASH_MD5;
use aruna_core::structs::{
    AuthContext, BucketInfo, Permission, RealmId, UserAccess, WatchEvent, WatchEventDetail,
    WatchEventKind, blob_bucket_permission_path, data_watch_resource_path,
};
use aruna_core::types::UserId;
use aruna_core::util::unix_timestamp_millis;
use aruna_operations::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::get_realm_config::GetRealmConfigOperation;
use aruna_operations::notifications::watch::emit::emit_resource_watch_event;
use aruna_operations::replication::queue::{
    QueueLiveVersionReplicationInput, QueueLiveVersionReplicationOperation,
};
use aruna_operations::s3::abort_multipart_upload::{
    AbortMultipartUploadInput as AMUI, AbortMultipartUploadOperation,
};
use aruna_operations::s3::bucket_cors::{
    DeleteBucketCorsOperation, GetBucketCorsOperation, PutBucketCorsOperation,
};
use aruna_operations::s3::complete_multipart_upload::{
    CompleteMultipartUploadInput as CMUI, CompleteMultipartUploadOperation,
    CompleteMultipartUploadResult,
};
use aruna_operations::s3::create_bucket::CreateBucketOperation;
use aruna_operations::s3::create_multipart_upload::{
    CreateMultipartUploadInput as CMPI, CreateMultipartUploadOperation,
};
use aruna_operations::s3::delete_bucket::DeleteBucketOperation;
use aruna_operations::s3::delete_object::{
    DeleteObjectInput as DOI, DeleteObjectOperation, DeleteObjectResult,
};
use aruna_operations::s3::get_bucket_info::GetBucketInfoOperation;
use aruna_operations::s3::get_object::{
    GetObjectInput as GOI, GetObjectOperation, GetObjectResult, ObjectRangeRequest,
};
use aruna_operations::s3::head_object::{HeadObjectInput as HOI, HeadObjectOperation};
use aruna_operations::s3::list_buckets::{ListBucketsInput as LBI, ListBucketsOperation};
use aruna_operations::s3::list_objects_v2::{
    ListObjectsV2ContinuationToken, ListObjectsV2Input as LOV2I, ListObjectsV2Operation,
};
use aruna_operations::s3::put_bucket_replication::{
    DeleteBucketReplicationOperation, GetBucketReplicationOperation, PutBucketReplicationOperation,
};
use aruna_operations::s3::put_object::{PutObjectConfig, PutObjectOperation, PutObjectResult};
use aruna_operations::s3::refresh_reference_metadata::{
    QueueReferenceMetadataRefreshOperation, ReferenceMetadataRefresh,
};
use aruna_operations::s3::upload_part::{UploadPartInput as UPI, UploadPartOperation};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use s3s::dto::{
    AbortMultipartUploadInput, AbortMultipartUploadOutput, Bucket, BucketVersioningStatus,
    CommonPrefix, CompleteMultipartUploadInput, CompleteMultipartUploadOutput, CreateBucketInput,
    CreateBucketOutput, CreateMultipartUploadInput, CreateMultipartUploadOutput,
    DeleteBucketCorsInput, DeleteBucketCorsOutput, DeleteBucketInput, DeleteBucketOutput,
    DeleteBucketReplicationInput, DeleteBucketReplicationOutput, DeleteMarkerReplication,
    DeleteMarkerReplicationStatus, DeleteObjectInput, DeleteObjectOutput, Destination, ETag,
    EncodingType, GetBucketCorsInput, GetBucketCorsOutput, GetBucketReplicationInput,
    GetBucketReplicationOutput, GetBucketVersioningInput, GetBucketVersioningOutput,
    GetObjectAttributesInput, GetObjectAttributesOutput, GetObjectInput, GetObjectOutput,
    HeadBucketInput, HeadBucketOutput, HeadObjectInput, HeadObjectOutput, LastModified,
    ListBucketsInput, ListBucketsOutput, ListObjectsV2Input, ListObjectsV2Output, Object, Owner,
    PutBucketCorsInput, PutBucketCorsOutput, PutBucketReplicationInput, PutBucketReplicationOutput,
    PutBucketVersioningInput, PutBucketVersioningOutput, PutObjectInput, PutObjectOutput,
    ReplicationConfiguration, ReplicationRule, ReplicationRuleStatus, StreamingBlob,
    UploadPartInput, UploadPartOutput,
};
use s3s::{S3, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::Arc;
use std::time::SystemTime;
use tracing::{debug, error, warn};

#[derive(Debug)]
struct ObjectResponseFields {
    content_length: Option<i64>,
    content_type: Option<String>,
    e_tag: Option<ETag>,
    last_modified: Option<LastModified>,
    metadata: Option<std::collections::HashMap<String, String>>,
}

const S3_URL_ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

fn object_range_request(range: s3s::dto::Range) -> ObjectRangeRequest {
    match range {
        s3s::dto::Range::Int { first, last } => match last {
            Some(end) => ObjectRangeRequest::StartEnd { start: first, end },
            None => ObjectRangeRequest::Start { start: first },
        },
        s3s::dto::Range::Suffix { length } => ObjectRangeRequest::Suffix { length },
    }
}

#[derive(Clone)]
pub struct ArunaS3Service {
    state: Arc<DriverContext>,
    realm_id: RealmId,
    node_id: NodeId,
}

impl Debug for ArunaS3Service {
    #[tracing::instrument(level = "trace", skip(self, f))]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArunaS3Service").finish()
    }
}

impl ArunaS3Service {
    #[tracing::instrument(level = "trace", skip(driver_ctx))]
    pub async fn new(driver_ctx: Arc<DriverContext>, realm_id: RealmId, node_id: NodeId) -> Self {
        ArunaS3Service {
            state: driver_ctx,
            realm_id,
            node_id,
        }
    }

    async fn can_access_bucket(
        &self,
        user_access: &UserAccess,
        bucket: &str,
        bucket_info: &BucketInfo,
    ) -> S3Result<bool> {
        drive(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context: AuthContext {
                    user_id: user_access.user_identity,
                    realm_id: user_access.user_identity.realm_id,
                    path_restrictions: None,
                },
                path: blob_bucket_permission_path(
                    self.realm_id,
                    bucket_info.group_id,
                    self.node_id,
                    bucket,
                ),
                required_permission: Permission::READ,
            }),
            &self.state,
        )
        .await
        .map_err(|err| s3_error!(InternalError, "{}", err.to_string()))
    }

    /// Resolves the hard byte ceiling for a group's realm-wide `logical_bytes`
    /// from the realm quota config, mirroring the create_group pattern of reading
    /// realm config at the request surface. `None` means the group is unlimited.
    async fn resolve_quota_ceiling(
        &self,
        group_id: aruna_core::types::GroupId,
    ) -> S3Result<Option<u64>> {
        let realm_config = drive(GetRealmConfigOperation::new(self.realm_id), &self.state)
            .await
            .map_err(|err| {
                error!(error = %err, "Failed to load realm config for quota enforcement");
                s3_error!(InternalError, "Failed to load realm quota configuration")
            })?;
        Ok(realm_config.quota.effective_group_ceiling(&group_id))
    }

    fn parse_replication_targets(
        &self,
        bucket: &str,
        configuration: &ReplicationConfiguration,
    ) -> S3Result<Vec<aruna_core::structs::BucketReplicationTarget>> {
        let mut targets = Vec::new();

        for rule in &configuration.rules {
            if rule.status.as_str() != ReplicationRuleStatus::ENABLED {
                continue;
            }
            let arn = aruna_core::structs::ArunaArn::parse(&rule.destination.bucket)
                .map_err(|err| s3_error!(InvalidArgument, "{}", err.to_string()))?;
            if arn.resource_type != aruna_core::structs::ArunaArnType::S3 {
                return Err(s3_error!(
                    InvalidArgument,
                    "Replication target ARN must use s3 type"
                ));
            }
            if arn.realm_id != self.realm_id {
                return Err(s3_error!(
                    InvalidArgument,
                    "Replication target must be in same realm"
                ));
            }
            if arn.path != bucket {
                return Err(s3_error!(
                    InvalidArgument,
                    "Replication target bucket must equal source bucket"
                ));
            }
            let replicate_delete_markers = rule
                .delete_marker_replication
                .as_ref()
                .and_then(|replication| replication.status.as_ref())
                .is_some_and(|status| status.as_str() == DeleteMarkerReplicationStatus::ENABLED);
            targets.push(aruna_core::structs::BucketReplicationTarget {
                node_id: arn.node_id,
                realm_id: arn.realm_id,
                bucket: bucket.to_string(),
                arn: arn.to_string(),
                replicate_delete_markers,
            });
        }

        if targets.is_empty() {
            return Err(s3_error!(
                InvalidArgument,
                "Replication requires at least one enabled target"
            ));
        }

        Ok(targets)
    }

    fn build_replication_configuration(
        &self,
        config: &aruna_core::structs::BucketReplicationConfig,
    ) -> ReplicationConfiguration {
        let rules = config
            .targets
            .iter()
            .enumerate()
            .map(|(index, target)| ReplicationRule {
                delete_marker_replication: Some(DeleteMarkerReplication {
                    status: Some(DeleteMarkerReplicationStatus::from_static(
                        if target.replicate_delete_markers {
                            DeleteMarkerReplicationStatus::ENABLED
                        } else {
                            DeleteMarkerReplicationStatus::DISABLED
                        },
                    )),
                }),
                destination: Destination {
                    access_control_translation: None,
                    account: None,
                    bucket: aruna_core::structs::ArunaArn::s3_bucket(
                        target.realm_id,
                        target.node_id,
                        target.bucket.clone(),
                    )
                    .expect("bucket replication targets have valid bucket names")
                    .to_string(),
                    encryption_configuration: None,
                    metrics: None,
                    replication_time: None,
                    storage_class: None,
                },
                existing_object_replication: None,
                filter: None,
                id: Some(format!("aruna-target-{}", index + 1)),
                prefix: None,
                priority: Some((index + 1) as i32),
                source_selection_criteria: None,
                status: ReplicationRuleStatus::from_static(ReplicationRuleStatus::ENABLED),
            })
            .collect();

        ReplicationConfiguration {
            role: "arn:aruna:replication-role".to_string(),
            rules,
        }
    }

    async fn queue_live_version_replication(
        &self,
        auth_context: AuthContext,
        bucket: String,
        key: String,
        version_id: ulid::Ulid,
        delete_marker: bool,
    ) {
        let result = match drive(
            QueueLiveVersionReplicationOperation::new(QueueLiveVersionReplicationInput {
                local_node_id: self.node_id,
                auth_context,
                bucket: bucket.clone(),
                key: key.clone(),
                version_id,
                delete_marker,
            }),
            &self.state,
        )
        .await
        {
            Ok(result) => result,
            Err(error) => {
                warn!(
                    error = %error,
                    bucket,
                    key,
                    version_id = %version_id,
                    delete_marker,
                    "Failed to queue live replication after committed write; durable obligation remains for repair"
                );
                return;
            }
        };

        if result.queued > 0 && !result.scheduled {
            warn!(bucket, key, version_id = %version_id, queued = result.queued, "Live replication jobs persisted but drain scheduling was not acknowledged");
        }
    }

    /// Post-commit, best-effort resource-watch emission for a committed object
    /// write. A failed emission only warns and never affects the already-successful
    /// upload.
    async fn emit_data_uploaded_watch(
        &self,
        actor: UserId,
        group_id: ulid::Ulid,
        bucket: String,
        key: String,
        size_bytes: u64,
    ) {
        let path = data_watch_resource_path(group_id, self.node_id, &bucket, &key);
        let event = WatchEvent {
            event_id: ulid::Ulid::r#gen(),
            realm_id: self.realm_id,
            kind: WatchEventKind::DataUploaded,
            path,
            actor,
            occurred_at_ms: unix_timestamp_millis(),
            detail: WatchEventDetail::DataUploaded {
                group_id,
                node_id: self.node_id,
                bucket,
                key,
                size_bytes,
            },
        };
        emit_resource_watch_event(self.state.as_ref(), event).await;
    }

    async fn complete_multipart_upload_response(
        &self,
        group_id: ulid::Ulid,
        bucket: String,
        key: String,
        checksum_request: &UploadChecksumRequest,
        replication_auth: AuthContext,
        result: CompleteMultipartUploadResult,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let replication_bucket = bucket.clone();
        let replication_key = key.clone();
        let mut output = CompleteMultipartUploadOutput {
            bucket: Some(bucket),
            key: Some(key),
            e_tag: result
                .location
                .hashes
                .get(HASH_MD5)
                .map(|value| ETag::Strong(hex::encode(value))),
            version_id: Some(result.version_id.to_string()),
            ..Default::default()
        };

        output.apply_checksums(encode_checksums(
            &result.response_hashes,
            ChecksumSelection::Requested(checksum_request.response_algorithm),
            s3_checksum_type_from_multipart(result.checksum_type),
        ));

        let watch_actor = replication_auth.user_id;
        let watch_bucket = replication_bucket.clone();
        let watch_key = replication_key.clone();
        let watch_size = result.location.blob_size;
        self.queue_live_version_replication(
            replication_auth,
            replication_bucket,
            replication_key,
            result.version_id,
            false,
        )
        .await;
        self.emit_data_uploaded_watch(watch_actor, group_id, watch_bucket, watch_key, watch_size)
            .await;

        Ok(S3Response::new(output))
    }

    async fn put_object_response(
        &self,
        checksum_request: &UploadChecksumRequest,
        replication_auth: AuthContext,
        group_id: ulid::Ulid,
        replication_bucket: String,
        replication_key: String,
        result: PutObjectResult,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        let mut output = PutObjectOutput {
            e_tag: Some(ETag::Strong(hex::encode(
                result.location.hashes.get(HASH_MD5).ok_or_else(|| {
                    error!(error = "Missing MD5 hash");
                    s3_error!(InternalError, "Missing MD5 hash")
                })?,
            ))),
            size: Some(result.location.blob_size as i64),
            version_id: Some(result.version_id.to_string()),
            ..Default::default()
        };
        output.apply_checksums(encode_checksums(
            &result.location.hashes,
            ChecksumSelection::Requested(checksum_request.response_algorithm),
            checksum_request.checksum_type.clone(),
        ));
        let watch_actor = replication_auth.user_id;
        let watch_bucket = replication_bucket.clone();
        let watch_key = replication_key.clone();
        let watch_size = result.location.blob_size;
        self.queue_live_version_replication(
            replication_auth,
            replication_bucket,
            replication_key,
            result.version_id,
            false,
        )
        .await;
        self.emit_data_uploaded_watch(watch_actor, group_id, watch_bucket, watch_key, watch_size)
            .await;

        Ok(S3Response::new(output))
    }

    async fn delete_object_response(
        &self,
        replication_auth: AuthContext,
        replication_bucket: String,
        replication_key: String,
        replicate_latest_delete: bool,
        result: DeleteObjectResult,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        if replicate_latest_delete {
            self.queue_live_version_replication(
                replication_auth,
                replication_bucket,
                replication_key,
                result.version_id,
                result.delete_marker,
            )
            .await;
        }

        Ok(S3Response::new(DeleteObjectOutput {
            delete_marker: Some(result.delete_marker),
            version_id: Some(result.version_id.to_string()),
            ..Default::default()
        }))
    }

    fn source_metadata_headers(
        &self,
        metadata: &aruna_core::structs::SourceMetadata,
        last_refresh: Option<SystemTime>,
    ) -> Option<std::collections::HashMap<String, String>> {
        let mut headers = std::collections::HashMap::new();

        if let Some(content_type) = &metadata.content_type {
            headers.insert(
                "aruna-source-content-type".to_string(),
                content_type.clone(),
            );
        }
        if let Some(etag) = &metadata.etag {
            headers.insert("aruna-source-etag".to_string(), etag.clone());
        }
        if let Some(last_modified) = metadata.last_modified {
            headers.insert(
                "aruna-source-last-modified".to_string(),
                chrono::DateTime::<chrono::Utc>::from(last_modified).to_rfc3339(),
            );
        }
        if let Some(last_refresh) = last_refresh {
            headers.insert(
                "aruna-last-refresh".to_string(),
                chrono::DateTime::<chrono::Utc>::from(last_refresh).to_rfc3339(),
            );
        }

        (!headers.is_empty()).then_some(headers)
    }

    fn build_object_response_fields(
        &self,
        location: Option<&aruna_core::structs::BackendLocation>,
        source_metadata: Option<&aruna_core::structs::SourceMetadata>,
        last_refresh: Option<SystemTime>,
    ) -> ObjectResponseFields {
        ObjectResponseFields {
            content_length: location
                .map(|location| location.blob_size as i64)
                .or_else(|| source_metadata.map(|metadata| metadata.content_length as i64)),
            content_type: source_metadata.and_then(|metadata| metadata.content_type.clone()),
            e_tag: location
                .and_then(|location| {
                    location
                        .hashes
                        .get(HASH_MD5)
                        .map(|value| ETag::Strong(hex::encode(value)))
                })
                .or_else(|| {
                    source_metadata.and_then(|metadata| {
                        metadata
                            .etag
                            .as_deref()
                            .and_then(|etag| ETag::from_str(etag).ok())
                    })
                }),
            last_modified: location
                .map(|location| location.created_at.into())
                .or_else(|| {
                    source_metadata.and_then(|metadata| metadata.last_modified.map(Into::into))
                }),
            metadata: source_metadata
                .and_then(|metadata| self.source_metadata_headers(metadata, last_refresh)),
        }
    }

    fn decode_list_objects_v2_continuation_token(
        token: Option<&str>,
    ) -> S3Result<Option<ListObjectsV2ContinuationToken>> {
        token
            .map(|token| {
                let decoded = STANDARD
                    .decode(token)
                    .map_err(|_| s3_error!(InvalidArgument, "Invalid continuation token"))?;
                ListObjectsV2ContinuationToken::from_bytes(&decoded)
                    .map_err(|_| s3_error!(InvalidArgument, "Invalid continuation token"))
            })
            .transpose()
    }

    fn encode_list_objects_v2_continuation_token(
        token: Option<&ListObjectsV2ContinuationToken>,
    ) -> S3Result<Option<String>> {
        token
            .map(|token| {
                token
                    .to_bytes()
                    .map(|bytes| STANDARD.encode(bytes))
                    .map_err(|err| s3_error!(InternalError, "{}", err.to_string()))
            })
            .transpose()
    }
}

fn reference_metadata_refresh(
    bucket: String,
    key: String,
    result: &GetObjectResult,
) -> Option<ReferenceMetadataRefresh> {
    if result.location.is_some() {
        return None;
    }

    Some(ReferenceMetadataRefresh {
        bucket,
        key,
        version_id: result.resolved_version_id.or(result.version_id)?,
        metadata: result.source_metadata.clone()?,
        refreshed_at: result.last_refresh?,
    })
}

fn attach_reference_metadata_refresh<T: 'static>(
    blob: BackendStream<Result<T, StreamError>>,
    context: Arc<DriverContext>,
    refresh: ReferenceMetadataRefresh,
) -> BackendStream<Result<T, StreamError>> {
    let refresh_bucket = refresh.bucket.clone();
    let refresh_key = refresh.key.clone();
    let refresh_version_id = refresh.version_id;

    blob.on_success_async(move || async move {
        match drive(
            QueueReferenceMetadataRefreshOperation::new(refresh),
            context.as_ref(),
        )
        .await
        {
            Ok(queue_result) => {
                if queue_result.queued && !queue_result.scheduled {
                    warn!(
                        bucket = %refresh_bucket,
                        key = %refresh_key,
                        version_id = %refresh_version_id,
                        "Reference metadata refresh job persisted but drain scheduling was not acknowledged"
                    );
                }
            }
            Err(error) => {
                warn!(
                    error = %error,
                    bucket = %refresh_bucket,
                    key = %refresh_key,
                    version_id = %refresh_version_id,
                    "Failed to queue reference metadata refresh after successful stream; refresh is best effort"
                );
            }
        }
        Ok(())
    })
}

#[async_trait::async_trait]
impl S3 for ArunaS3Service {
    #[tracing::instrument(err, skip(self, req))]
    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        debug!("Received CREATE BUCKET Request: {:#?}", req);

        let user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;

        let operation = CreateBucketOperation::new(
            req.input.bucket.clone(),
            BucketInfo {
                group_id: user_access.group_id,
                created_at: SystemTime::now(),
                created_by: user_access.user_identity,
                cors_configuration: None,
            },
        );

        drive(operation, &self.state)
            .await
            .and_then(|result| result.transpose())
            .map_err(IntoS3Error::into_s3_error)?
            .ok_or_else(|| s3_error!(InternalError, "Failed to create bucket"))?;

        Ok(S3Response::new(CreateBucketOutput::default()))
    }

    #[tracing::instrument(err, skip(self, req))]
    async fn head_bucket(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        debug!("Received HEAD BUCKET Request: {:#?}", req);

        let _user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;

        drive(GetBucketInfoOperation::new(req.input.bucket), &self.state)
            .await
            .and_then(|result| result.transpose())
            .map_err(IntoS3Error::into_s3_error)?
            .ok_or_else(|| s3_error!(InternalError, "Failed to head bucket"))?;

        Ok(S3Response::new(HeadBucketOutput::default()))
    }

    #[tracing::instrument(err, skip(self, req))]
    async fn get_bucket_versioning(
        &self,
        req: S3Request<GetBucketVersioningInput>,
    ) -> S3Result<S3Response<GetBucketVersioningOutput>> {
        debug!(bucket = %req.input.bucket, "Received GET BUCKET VERSIONING Request");

        let _user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;

        Ok(S3Response::new(GetBucketVersioningOutput {
            status: Some(BucketVersioningStatus::from_static(
                BucketVersioningStatus::ENABLED,
            )),
            mfa_delete: None,
        }))
    }

    #[tracing::instrument(err, skip(self, req))]
    async fn put_bucket_versioning(
        &self,
        req: S3Request<PutBucketVersioningInput>,
    ) -> S3Result<S3Response<PutBucketVersioningOutput>> {
        debug!(bucket = %req.input.bucket, "Received PUT BUCKET VERSIONING Request");

        let _user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;

        match req
            .input
            .versioning_configuration
            .status
            .as_ref()
            .map(BucketVersioningStatus::as_str)
        {
            Some(BucketVersioningStatus::ENABLED) => {
                Ok(S3Response::new(PutBucketVersioningOutput::default()))
            }
            _ => Err(s3_error!(NotImplemented, "Versioning cannot be suspended")),
        }
    }

    #[tracing::instrument(err, skip(self, req))]
    async fn list_buckets(
        &self,
        req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        debug!("Received LIST BUCKETS Request: {:#?}", req);

        let user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;

        let result = drive(
            ListBucketsOperation::new(LBI {
                group_id: user_access.group_id,
                prefix: req.input.prefix.clone(),
                continuation_token: req.input.continuation_token.clone(),
                max_buckets: req
                    .input
                    .max_buckets
                    .and_then(|max_buckets| usize::try_from(max_buckets).ok()),
            }),
            &self.state,
        )
        .await
        .and_then(|result| result.transpose())
        .map_err(IntoS3Error::into_s3_error)?
        .ok_or_else(|| s3_error!(InternalError, "Failed to list buckets"))?;

        let mut buckets = Vec::new();
        for (bucket, bucket_info) in result.buckets {
            if self
                .can_access_bucket(&user_access, &bucket, &bucket_info)
                .await?
            {
                buckets.push(Bucket {
                    bucket_region: None,
                    creation_date: Some(bucket_info.created_at.into()),
                    name: Some(bucket),
                });
            }
        }

        Ok(S3Response::new(ListBucketsOutput {
            buckets: Some(buckets),
            continuation_token: result.continuation_token,
            owner: None,
            prefix: req.input.prefix,
        }))
    }

    #[tracing::instrument(err, skip(self, req))]
    async fn put_bucket_cors(
        &self,
        req: S3Request<PutBucketCorsInput>,
    ) -> S3Result<S3Response<PutBucketCorsOutput>> {
        debug!(bucket = %req.input.bucket, "Received PUT BUCKET CORS Request");

        let _user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;
        let config = dto_to_bucket_cors(req.input.cors_configuration.clone())?;

        drive(
            PutBucketCorsOperation::new(req.input.bucket.clone(), config),
            &self.state,
        )
        .await
        .and_then(|result| result.transpose())
        .map_err(IntoS3Error::into_s3_error)?
        .ok_or_else(|| s3_error!(InternalError, "Failed to put bucket CORS configuration"))?;

        Ok(S3Response::new(PutBucketCorsOutput::default()))
    }

    #[tracing::instrument(err, skip(self, req))]
    async fn get_bucket_cors(
        &self,
        req: S3Request<GetBucketCorsInput>,
    ) -> S3Result<S3Response<GetBucketCorsOutput>> {
        debug!(bucket = %req.input.bucket, "Received GET BUCKET CORS Request");

        let _user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;
        let config = drive(
            GetBucketCorsOperation::new(req.input.bucket.clone()),
            &self.state,
        )
        .await
        .and_then(|result| result.transpose())
        .map_err(IntoS3Error::into_s3_error)?
        .ok_or_else(|| s3_error!(InternalError, "Failed to get bucket CORS configuration"))?;

        Ok(S3Response::new(bucket_cors_to_get_output(config)))
    }

    #[tracing::instrument(err, skip(self, req))]
    async fn delete_bucket_cors(
        &self,
        req: S3Request<DeleteBucketCorsInput>,
    ) -> S3Result<S3Response<DeleteBucketCorsOutput>> {
        debug!(bucket = %req.input.bucket, "Received DELETE BUCKET CORS Request");

        let _user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;

        drive(
            DeleteBucketCorsOperation::new(req.input.bucket.clone()),
            &self.state,
        )
        .await
        .and_then(|result| result.transpose())
        .map_err(IntoS3Error::into_s3_error)?
        .ok_or_else(|| s3_error!(InternalError, "Failed to delete bucket CORS configuration"))?;

        Ok(S3Response::new(DeleteBucketCorsOutput::default()))
    }

    #[tracing::instrument(err, skip(self, req))]
    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        debug!("Received LIST OBJECTS V2 Request: {:#?}", req);

        let user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;
        let bucket_info = req.extensions.get::<BucketInfo>().cloned();
        let requested_continuation_token = req.input.continuation_token.clone();
        let continuation_token = Self::decode_list_objects_v2_continuation_token(
            requested_continuation_token.as_deref(),
        )?;
        let max_keys = match req.input.max_keys {
            None => ListObjectsV2Operation::DEFAULT_MAX_KEYS,
            Some(max_keys) => usize::try_from(max_keys)
                .map_err(|_| s3_error!(InvalidArgument, "max-keys must be non-negative"))?
                .min(ListObjectsV2Operation::DEFAULT_MAX_KEYS),
        };
        let bucket = req.input.bucket.clone();
        let prefix = req.input.prefix.clone();
        let delimiter = req.input.delimiter.clone();
        let start_after = req.input.start_after.clone();

        let group_id = bucket_info
            .as_ref()
            .map(|bucket_info| bucket_info.group_id)
            .unwrap_or(user_access.group_id);

        let result = drive(
            ListObjectsV2Operation::new(LOV2I {
                bucket: bucket.clone(),
                group_id,
                continuation_token,
                max_keys: Some(max_keys),
                prefix: prefix.clone(),
                delimiter: delimiter.clone(),
                start_after: start_after.clone(),
            }),
            &self.state,
        )
        .await
        .and_then(|result| result.transpose())
        .map_err(IntoS3Error::into_s3_error)?
        .ok_or_else(|| s3_error!(InternalError, "Failed to list objects"))?;

        let owner = req.input.fetch_owner.unwrap_or(false).then(|| Owner {
            display_name: None,
            id: Some(group_id.to_string()),
        });
        let url_encoded = req
            .input
            .encoding_type
            .as_ref()
            .is_some_and(|encoding_type| encoding_type.as_str() == EncodingType::URL);
        let encode_field = |value: String| -> String {
            if url_encoded {
                utf8_percent_encode(&value, S3_URL_ENCODE_SET).to_string()
            } else {
                value
            }
        };

        let contents: Vec<Object> = result
            .objects
            .into_iter()
            .map(|object| {
                let response_fields = self.build_object_response_fields(
                    object.location.as_ref(),
                    object.source_metadata.as_ref(),
                    object.last_refresh,
                );
                Object {
                    e_tag: response_fields.e_tag,
                    key: Some(encode_field(object.head.key)),
                    last_modified: response_fields.last_modified,
                    owner: owner.clone(),
                    size: response_fields.content_length,
                    ..Default::default()
                }
            })
            .collect();
        let common_prefixes: Vec<CommonPrefix> = result
            .common_prefixes
            .into_iter()
            .map(|prefix| CommonPrefix {
                prefix: Some(encode_field(prefix)),
            })
            .collect();
        let key_count = contents.len() + common_prefixes.len();
        let next_continuation_token =
            Self::encode_list_objects_v2_continuation_token(result.continuation_token.as_ref())?;
        let is_truncated = next_continuation_token.is_some();

        Ok(S3Response::new(ListObjectsV2Output {
            name: Some(bucket),
            prefix: prefix.map(&encode_field),
            max_keys: Some(i32::try_from(max_keys).unwrap_or(i32::MAX)),
            key_count: Some(i32::try_from(key_count).unwrap_or(i32::MAX)),
            continuation_token: requested_continuation_token,
            is_truncated: Some(is_truncated),
            next_continuation_token,
            contents: Some(contents),
            common_prefixes: Some(common_prefixes),
            delimiter: delimiter.map(&encode_field),
            encoding_type: req.input.encoding_type,
            start_after: start_after.map(&encode_field),
            ..Default::default()
        }))
    }

    #[tracing::instrument(err, skip(self, req))]
    #[allow(clippy::blocks_in_conditions)]
    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        debug!("Received PUT Request: {:#?}", req);

        // Extract access check result
        let user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;
        validate_object_key(&req.input.key)?;
        let bucket_info = req.extensions.get::<BucketInfo>().cloned();
        let checksum_request = parse_upload_checksum_request(&req.headers)?;
        let replication_auth = AuthContext {
            user_id: user_access.user_identity,
            realm_id: user_access.user_identity.realm_id,
            path_restrictions: user_access.path_restrictions.clone(),
        };
        let replication_bucket = req.input.bucket.clone();
        let replication_key = req.input.key.clone();

        let group_id = bucket_info
            .as_ref()
            .map(|bucket_info| bucket_info.group_id)
            .unwrap_or(user_access.group_id);
        let quota_ceiling = self.resolve_quota_ceiling(group_id).await?;
        let input = convert_input(req.input)?;
        let config = PutObjectConfig {
            user_id: user_access.user_identity,
            group_id,
            realm_id: self.realm_id,
            node_id: self.node_id,
            request: input,
            expected_checksums: checksum_request.expected.clone(),
            checksum_type: Some(checksum_request.checksum_type.as_str().to_string()),
            version_source: None,
            exists: false,
            quota_ceiling,
        };
        let operation = PutObjectOperation::new(config);

        let result = drive(operation, &self.state)
            .await
            .and_then(|result| result.transpose())
            .map_err(IntoS3Error::into_s3_error)?
            .ok_or_else(|| s3_error!(InternalError, "Failed to process PUT request"))?;

        self.put_object_response(
            &checksum_request,
            replication_auth,
            group_id,
            replication_bucket,
            replication_key,
            result,
        )
        .await
    }

    #[tracing::instrument(err, skip(self, req))]
    #[allow(clippy::blocks_in_conditions)]
    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        debug!("Received CREATE MULTIPART Request: {:#?}", req);

        let user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;
        validate_object_key(&req.input.key)?;
        let bucket_info = req.extensions.get::<BucketInfo>().cloned();
        let checksum_hint = parse_multipart_checksum_hint(&req.input)?;

        let operation = CreateMultipartUploadOperation::new(CMPI {
            bucket: req.input.bucket.clone(),
            key: req.input.key.clone(),
            group_id: bucket_info
                .as_ref()
                .map(|bucket_info| bucket_info.group_id)
                .unwrap_or(user_access.group_id),
            created_by: user_access.user_identity,
            checksum_hint: checksum_hint.clone(),
        });

        let result = drive(operation, &self.state)
            .await
            .and_then(|result| result.transpose())
            .map_err(IntoS3Error::into_s3_error)?
            .ok_or_else(|| s3_error!(InternalError, "Failed to create multipart upload"))?;

        Ok(S3Response::new(CreateMultipartUploadOutput {
            bucket: Some(req.input.bucket),
            key: Some(req.input.key),
            upload_id: Some(result.record.upload_id.to_string()),
            checksum_algorithm: req.input.checksum_algorithm,
            checksum_type: checksum_hint
                .map(|hint| s3_checksum_type_from_multipart(hint.checksum_type)),
            ..Default::default()
        }))
    }

    #[tracing::instrument(err, skip(self, req))]
    #[allow(clippy::blocks_in_conditions)]
    async fn upload_part(
        &self,
        req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        debug!("Received UPLOAD PART Request: {:#?}", req);

        let user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;
        validate_object_key(&req.input.key)?;
        let checksum_request = parse_upload_checksum_request(&req.headers)?;
        let upload_id = parse_upload_id(&req.input.upload_id)?;
        let body = req
            .input
            .body
            .map(BackendStream::new_from_boxed)
            .ok_or_else(|| s3_error!(InvalidRequest, "Missing body"))?;

        let operation = UploadPartOperation::new(UPI {
            bucket: req.input.bucket,
            key: req.input.key,
            upload_id,
            part_number: parse_multipart_part_number(
                req.input.part_number,
                S3ErrorCode::InvalidArgument,
            )?,
            content_length: req.input.content_length.map(|length| length as u64),
            body: Some(body),
            created_by: user_access.user_identity,
            compressed: false,
            encrypted: req.input.sse_customer_algorithm.is_some()
                || req.input.sse_customer_key.is_some()
                || req.input.sse_customer_key_md5.is_some(),
            expected_checksums: checksum_request.expected.clone(),
        });

        let result = drive(operation, &self.state)
            .await
            .and_then(|result| result.transpose())
            .map_err(IntoS3Error::into_s3_error)?
            .ok_or_else(|| s3_error!(InternalError, "Failed to upload part"))?;

        let mut output = UploadPartOutput {
            e_tag: result
                .location
                .hashes
                .get(HASH_MD5)
                .map(|value| ETag::Strong(hex::encode(value))),
            ..Default::default()
        };
        output.apply_checksums(encode_checksums(
            &result.location.hashes,
            ChecksumSelection::Requested(checksum_request.response_algorithm),
            checksum_request.checksum_type,
        ));

        Ok(S3Response::new(output))
    }

    #[tracing::instrument(err, skip(self, req))]
    #[allow(clippy::blocks_in_conditions)]
    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        debug!("Received COMPLETE MULTIPART Request: {:#?}", req);

        let user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;
        validate_object_key(&req.input.key)?;
        let bucket_info = req.extensions.get::<BucketInfo>().cloned();
        let group_id = bucket_info
            .as_ref()
            .map(|bucket_info| bucket_info.group_id)
            .unwrap_or(user_access.group_id);
        let quota_ceiling = self.resolve_quota_ceiling(group_id).await?;
        let checksum_request = parse_upload_checksum_request(&req.headers)?;
        let upload_id = parse_upload_id(&req.input.upload_id)?;
        let replication_auth = AuthContext {
            user_id: user_access.user_identity,
            realm_id: user_access.user_identity.realm_id,
            path_restrictions: user_access.path_restrictions.clone(),
        };
        let completed_parts = req
            .input
            .multipart_upload
            .as_ref()
            .and_then(|multipart| multipart.parts.as_ref())
            .map(|parts| {
                parts
                    .iter()
                    .map(parse_completed_part)
                    .collect::<S3Result<Vec<_>>>()
            })
            .transpose()?
            .unwrap_or_default();

        let operation = CompleteMultipartUploadOperation::new(CMUI {
            bucket: req.input.bucket.clone(),
            key: req.input.key.clone(),
            upload_id,
            realm_id: self.realm_id,
            node_id: self.node_id,
            completed_parts,
            expected_checksums: checksum_request.expected.clone(),
            checksum_type: multipart_checksum_type_from_s3(&checksum_request.checksum_type),
            object_size: req.input.mpu_object_size.map(|size| size as u64),
            created_by: user_access.user_identity,
            quota_ceiling,
        });

        let result = drive(operation, &self.state)
            .await
            .and_then(|result| result.transpose())
            .map_err(IntoS3Error::into_s3_error)?
            .ok_or_else(|| s3_error!(InternalError, "Failed to complete multipart upload"))?;

        self.complete_multipart_upload_response(
            group_id,
            req.input.bucket,
            req.input.key,
            &checksum_request,
            replication_auth,
            result,
        )
        .await
    }

    #[tracing::instrument(err, skip(self, req))]
    #[allow(clippy::blocks_in_conditions)]
    async fn abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        debug!("Received ABORT MULTIPART Request: {:#?}", req);

        let _user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;
        let upload_id = parse_upload_id(&req.input.upload_id)?;
        let operation = AbortMultipartUploadOperation::new(AMUI {
            bucket: req.input.bucket,
            key: req.input.key,
            upload_id,
        });

        drive(operation, &self.state)
            .await
            .and_then(|result| result.transpose())
            .map_err(IntoS3Error::into_s3_error)?
            .ok_or_else(|| s3_error!(InternalError, "Failed to abort multipart upload"))?;

        Ok(S3Response::new(AbortMultipartUploadOutput::default()))
    }

    #[tracing::instrument(err, skip(self, req))]
    #[allow(clippy::blocks_in_conditions)]
    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        debug!("Received GET Request: {:#?}", req);

        // Extract access check result
        let user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;
        let bucket_info = req.extensions.get::<BucketInfo>().cloned();
        let requested_range = req.input.range;
        let version_id = parse_version_id(req.input.version_id)?;
        let bucket = req.input.bucket;
        let key = req.input.key;
        let response_bucket = bucket.clone();
        let response_key = key.clone();

        let range_request = requested_range.map(object_range_request);

        let operation = GetObjectOperation::new(GOI {
            bucket,
            key,
            version_id,
            range: range_request,
            group_id: bucket_info
                .as_ref()
                .map(|bucket_info| bucket_info.group_id)
                .unwrap_or(user_access.group_id),
            user_identity: user_access.user_identity,
        });

        let result = drive(operation, &self.state)
            .await
            .and_then(|result| result.transpose())
            .map_err(IntoS3Error::into_s3_error)?
            .ok_or_else(|| s3_error!(InternalError, "Failed to process GET request"))?;

        let version_id = result.version_id;
        let resolved_range = result.resolved_range.clone();
        let reference_refresh = reference_metadata_refresh(response_bucket, response_key, &result);
        let response_fields = self.build_object_response_fields(
            result.location.as_ref(),
            result.source_metadata.as_ref(),
            result.last_refresh,
        );
        let blob = if let Some(refresh) = reference_refresh {
            attach_reference_metadata_refresh(result.blob, self.state.clone(), refresh)
        } else {
            result.blob
        };
        let content = StreamingBlob::wrap(blob);
        let mut output = GetObjectOutput {
            body: Some(content),
            accept_ranges: resolved_range.as_ref().map(|_| "bytes".to_string()),
            content_length: resolved_range
                .as_ref()
                .map(|range| range.content_length)
                .or(response_fields.content_length),
            content_range: resolved_range
                .as_ref()
                .map(|range| range.content_range.clone()),
            content_type: response_fields.content_type,
            e_tag: response_fields.e_tag,
            last_modified: response_fields.last_modified,
            metadata: response_fields.metadata,
            version_id: version_id.map(|version_id| version_id.to_string()),
            ..Default::default()
        };
        if checksum_mode_enabled(&req.headers)
            && let Some(location) = result.location.as_ref()
        {
            output.apply_checksums(encode_checksums(
                &location.hashes,
                ChecksumSelection::AllStored,
                s3_checksum_type_from_multipart(result.checksum_type),
            ));
        }

        Ok(if resolved_range.is_some() {
            S3Response::with_status(output, http::StatusCode::PARTIAL_CONTENT)
        } else {
            S3Response::new(output)
        })
    }

    #[tracing::instrument(err, skip(self, _req))]
    #[allow(clippy::blocks_in_conditions)]
    async fn get_object_attributes(
        &self,
        _req: S3Request<GetObjectAttributesInput>,
    ) -> S3Result<S3Response<GetObjectAttributesOutput>> {
        Err(s3_error!(
            NotImplemented,
            "GetObjectAttributes is not implemented"
        ))
    }

    #[tracing::instrument(err, skip(self, req))]
    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        debug!("Received HEAD Request: {:#?}", req);

        let _user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;
        let version_id = parse_version_id(req.input.version_id)?;
        let operation = HeadObjectOperation::new(HOI {
            bucket: req.input.bucket,
            key: req.input.key,
            version_id,
        });

        let result = drive(operation, &self.state)
            .await
            .and_then(|result| result.transpose())
            .map_err(IntoS3Error::into_s3_error)?
            .ok_or_else(|| s3_error!(InternalError, "Failed to process HEAD request"))?;

        let response_fields = self.build_object_response_fields(
            result.location.as_ref(),
            result.source_metadata.as_ref(),
            result.last_refresh,
        );
        let mut output = HeadObjectOutput {
            content_length: response_fields.content_length,
            content_type: response_fields.content_type,
            e_tag: response_fields.e_tag,
            version_id: result.version_id.map(|version_id| version_id.to_string()),
            last_modified: response_fields.last_modified,
            metadata: response_fields.metadata,
            ..Default::default()
        };

        if checksum_mode_enabled(&req.headers)
            && let Some(location) = result.location.as_ref()
        {
            output.apply_checksums(encode_checksums(
                &location.hashes,
                ChecksumSelection::AllStored,
                s3_checksum_type_from_multipart(result.checksum_type),
            ));
        }

        Ok(S3Response::new(output))
    }

    #[tracing::instrument(err)]
    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        debug!("Received DELETE Request: {:#?}", req);

        let user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;
        let version_id = parse_version_id(req.input.version_id)?;
        let replication_auth = AuthContext {
            user_id: user_access.user_identity,
            realm_id: user_access.user_identity.realm_id,
            path_restrictions: user_access.path_restrictions.clone(),
        };
        let replication_bucket = req.input.bucket.clone();
        let replication_key = req.input.key.clone();
        let replicate_latest_delete = version_id.is_none();

        let operation = DeleteObjectOperation::new(DOI {
            bucket: req.input.bucket,
            key: req.input.key,
            version_id,
            group_id: user_access.group_id,
            realm_id: self.realm_id,
            node_id: self.node_id,
            deleted_by: user_access.user_identity,
        });

        let result = drive(operation, &self.state)
            .await
            .and_then(|result| result.transpose())
            .map_err(IntoS3Error::into_s3_error)?
            .ok_or_else(|| s3_error!(InternalError, "Failed to process DELETE request"))?;

        self.delete_object_response(
            replication_auth,
            replication_bucket,
            replication_key,
            replicate_latest_delete,
            result,
        )
        .await
    }

    #[tracing::instrument(err)]
    async fn delete_bucket(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        debug!("Received DELETE BUCKET Request: {:#?}", req);

        drive(DeleteBucketOperation::new(req.input.bucket), &self.state)
            .await
            .and_then(|result| result.transpose())
            .map_err(IntoS3Error::into_s3_error)?
            .ok_or_else(|| s3_error!(InternalError, "Failed to delete bucket"))?;

        Ok(S3Response::new(DeleteBucketOutput::default()))
    }

    #[tracing::instrument(err, skip(self, req))]
    async fn put_bucket_replication(
        &self,
        req: S3Request<PutBucketReplicationInput>,
    ) -> S3Result<S3Response<PutBucketReplicationOutput>> {
        let _user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;
        let targets = self
            .parse_replication_targets(&req.input.bucket, &req.input.replication_configuration)?;

        drive(
            PutBucketReplicationOperation::new(req.input.bucket, targets),
            &self.state,
        )
        .await
        .and_then(|result| result.transpose())
        .map_err(IntoS3Error::into_s3_error)?
        .ok_or_else(|| s3_error!(InternalError, "Failed to store bucket replication config"))?;

        Ok(S3Response::new(PutBucketReplicationOutput::default()))
    }

    #[tracing::instrument(err, skip(self, req))]
    async fn get_bucket_replication(
        &self,
        req: S3Request<GetBucketReplicationInput>,
    ) -> S3Result<S3Response<GetBucketReplicationOutput>> {
        let _user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;

        let config = drive(
            GetBucketReplicationOperation::new(req.input.bucket),
            &self.state,
        )
        .await
        .and_then(|result| result.transpose())
        .map_err(IntoS3Error::into_s3_error)?
        .ok_or_else(|| s3_error!(InternalError, "Failed to load bucket replication config"))?;

        Ok(S3Response::new(GetBucketReplicationOutput {
            replication_configuration: Some(self.build_replication_configuration(&config)),
        }))
    }

    #[tracing::instrument(err, skip(self, req))]
    async fn delete_bucket_replication(
        &self,
        req: S3Request<DeleteBucketReplicationInput>,
    ) -> S3Result<S3Response<DeleteBucketReplicationOutput>> {
        let _user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;

        drive(
            DeleteBucketReplicationOperation::new(req.input.bucket),
            &self.state,
        )
        .await
        .and_then(|result| result.transpose())
        .map_err(IntoS3Error::into_s3_error)?
        .ok_or_else(|| s3_error!(InternalError, "Failed to delete bucket replication config"))?;

        Ok(S3Response::new(DeleteBucketReplicationOutput::default()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{
        AUTH_KEYSPACE, BLOB_HEAD_KEYSPACE, BLOB_LIVE_REPLICATION_OBLIGATION_KEYSPACE,
        BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, NOTIFICATION_INBOX_KEYSPACE,
        REALM_CONFIG_KEYSPACE, S3_BUCKET_REPLICATION_KEYSPACE,
    };
    use aruna_core::structs::{
        Actor, BackendLocation, BlobHeadKey, BlobVersion, BlobVersionState, CurrentVersionPointer,
        GroupAuthorizationDocument, NotificationClass, NotificationKind, NotificationRecord,
        PortableSourceDescriptor, RealmAuthorizationDocument, RealmConfigDocument, RealmNodeKind,
        SourceConnectorKind, SourceMetadata, StagingStrategy, VersionKey, VersionSourceBinding,
        WatchEventMask, WatchInterestEntry, WatchInterestTable,
    };
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_operations::driver::{DriverContext, drive};
    use aruna_operations::notifications::watch::subscriptions::create_watch_subscription;
    use aruna_operations::replication::queue::{
        LiveReplicationObligationRecord, live_replication_obligation_key,
    };
    use aruna_operations::s3::refresh_reference_metadata::refresh_reference_metadata;
    use aruna_storage::storage;
    use futures_util::{StreamExt, stream};
    use http::Extensions;
    use hyper::{HeaderMap, Method, Uri, body::Bytes};
    use s3s::dto::ChecksumType;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::{Duration, UNIX_EPOCH};
    use tempfile::TempDir;
    use ulid::Ulid;

    struct TestState {
        _storage_dir: TempDir,
        context: Arc<DriverContext>,
        bucket: String,
        key: String,
        version_id: Ulid,
        created_by: UserId,
    }

    #[tokio::test]
    async fn put_response_ignores_post_commit_live_replication_queue_failure() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let context = Arc::new(DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        });
        let realm_id = RealmId([40u8; 32]);
        let node_id = iroh::SecretKey::from_bytes(&[4u8; 32]).public();
        let service = ArunaS3Service::new(context, realm_id, node_id).await;
        let bucket = "bucket".to_string();
        let key = "object".to_string();
        let version_id = Ulid::r#gen();
        let user_id = UserId::local(Ulid::r#gen(), realm_id);
        let auth = AuthContext {
            user_id,
            realm_id,
            path_restrictions: None,
        };

        write_storage_value(
            &storage_handle,
            S3_BUCKET_REPLICATION_KEYSPACE,
            bucket.as_bytes().to_vec(),
            b"not a bucket replication config".to_vec(),
        )
        .await;
        let obligation = LiveReplicationObligationRecord::new(
            node_id,
            auth.clone(),
            bucket.clone(),
            key.clone(),
            version_id,
            false,
        );
        let obligation_key = live_replication_obligation_key(&obligation).unwrap();
        write_storage_value(
            &storage_handle,
            BLOB_LIVE_REPLICATION_OBLIGATION_KEYSPACE,
            obligation_key.as_ref().to_vec(),
            postcard::to_allocvec(&obligation).unwrap(),
        )
        .await;

        let checksum_request = UploadChecksumRequest {
            expected: Vec::new(),
            response_algorithm: None,
            checksum_type: ChecksumType::from_static(ChecksumType::FULL_OBJECT),
        };
        let response = service
            .put_object_response(
                &checksum_request,
                auth,
                Ulid::r#gen(),
                bucket.clone(),
                key,
                PutObjectResult {
                    location: response_location(user_id),
                    version_id,
                },
            )
            .await
            .expect("committed PUT response should not fail on queue kick error");

        assert_eq!(response.output.version_id, Some(version_id.to_string()));
        assert!(
            read_storage_value(
                &storage_handle,
                BLOB_LIVE_REPLICATION_OBLIGATION_KEYSPACE,
                obligation_key.as_ref().to_vec(),
            )
            .await
            .is_some(),
            "durable obligation should remain repairable when queue kick fails"
        );
    }

    async fn build_watch_context(
        realm_id: RealmId,
        secret: [u8; 32],
    ) -> (TempDir, Arc<DriverContext>, NetHandle) {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let net = NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().unwrap(),
                secret_key: Some(iroh::SecretKey::from_bytes(&secret)),
                realm_id,
                discovery_method: DiscoveryMethod::None,
                relay_method: RelayMethod::None,
                ..NetConfig::default()
            },
            storage_handle.clone(),
        )
        .await
        .unwrap();
        let mut realm_config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        realm_config.ensure_node(net.node_id(), RealmNodeKind::Server);
        let actor = Actor {
            node_id: net.node_id(),
            user_id: UserId::nil(realm_id),
            realm_id,
        };
        write_storage_value(
            &storage_handle,
            REALM_CONFIG_KEYSPACE,
            realm_id.as_bytes().to_vec(),
            realm_config.to_bytes(&actor).unwrap(),
        )
        .await;
        let context = Arc::new(DriverContext {
            storage_handle,
            net_handle: Some(net.clone()),
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        });
        (storage_dir, context, net)
    }

    fn data_uploaded_interest(
        realm_id: RealmId,
        holder: NodeId,
        path_prefix: String,
    ) -> WatchInterestTable {
        let mut table = WatchInterestTable::default();
        table.insert(
            realm_id,
            holder,
            vec![WatchInterestEntry {
                path_prefix,
                event_mask: WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
            }],
        );
        table
    }

    async fn install_watch_authorization(
        context: &DriverContext,
        realm_id: RealmId,
        node_id: NodeId,
        group_id: Ulid,
        watcher: UserId,
    ) {
        let actor = Actor {
            node_id,
            user_id: watcher,
            realm_id,
        };
        let realm_auth = RealmAuthorizationDocument::new_default_realm_doc(realm_id);
        let group_auth =
            GroupAuthorizationDocument::new_default_group_doc(watcher, realm_id, group_id);
        write_storage_value(
            &context.storage_handle,
            AUTH_KEYSPACE,
            realm_id.as_bytes().to_vec(),
            realm_auth.to_bytes(&actor).unwrap(),
        )
        .await;
        write_storage_value(
            &context.storage_handle,
            AUTH_KEYSPACE,
            group_id.to_bytes().to_vec(),
            group_auth.to_bytes(&actor).unwrap(),
        )
        .await;
    }

    async fn read_watch_inbox_rows(context: &DriverContext) -> Vec<NotificationRecord> {
        match context
            .storage_handle
            .send_storage_effect(StorageEffect::Iter {
                key_space: NOTIFICATION_INBOX_KEYSPACE.to_string(),
                prefix: None,
                start: None,
                limit: 1024,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::IterResult { values, .. }) => values
                .into_iter()
                .map(|(_, value)| NotificationRecord::from_bytes(&value).unwrap())
                .collect(),
            other => panic!("unexpected inbox iter event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn put_object_immediately_expands_watch_event_for_local_holder() {
        let realm_id = RealmId([41u8; 32]);
        let (_storage_dir, context, net) = build_watch_context(realm_id, [41u8; 32]).await;
        let holder = net.node_id();

        let service = ArunaS3Service::new(context.clone(), realm_id, net.node_id()).await;
        let user_id = UserId::local(Ulid::r#gen(), realm_id);
        let watcher = UserId::local(Ulid::r#gen(), realm_id);
        let group_id = Ulid::r#gen();
        let watch_prefix = data_watch_resource_path(group_id, net.node_id(), "bucket", "");
        net.replace_watch_interest(data_uploaded_interest(
            realm_id,
            holder,
            watch_prefix.clone(),
        ));
        install_watch_authorization(&context, realm_id, net.node_id(), group_id, watcher).await;
        create_watch_subscription(
            &context.storage_handle,
            watcher,
            watch_prefix,
            WatchEventMask::from_kinds([WatchEventKind::DataUploaded]),
            0,
        )
        .await
        .expect("watch subscription creates");
        let auth = AuthContext {
            user_id,
            realm_id,
            path_restrictions: None,
        };
        let checksum_request = UploadChecksumRequest {
            expected: Vec::new(),
            response_algorithm: None,
            checksum_type: ChecksumType::from_static(ChecksumType::FULL_OBJECT),
        };

        service
            .put_object_response(
                &checksum_request,
                auth,
                group_id,
                "bucket".to_string(),
                "object".to_string(),
                PutObjectResult {
                    location: response_location(user_id),
                    version_id: Ulid::r#gen(),
                },
            )
            .await
            .expect("committed PUT response should succeed");

        let rows = read_watch_inbox_rows(context.as_ref()).await;
        assert_eq!(rows.len(), 1, "the local holder expands immediately");
        let record = &rows[0];
        assert_eq!(record.recipient, watcher);
        assert_eq!(record.class, NotificationClass::Transient);
        match &record.kind {
            NotificationKind::DataUploaded {
                path,
                group_id: event_group_id,
                node_id: event_node_id,
                bucket,
                key,
                size_bytes,
                actor_user_id,
            } => {
                assert_eq!(
                    path,
                    &data_watch_resource_path(group_id, net.node_id(), "bucket", "object")
                );
                assert_eq!(*event_group_id, group_id);
                assert_eq!(*event_node_id, net.node_id());
                assert_eq!(bucket, "bucket");
                assert_eq!(key, "object");
                // response_location reports a 2-byte blob.
                assert_eq!(*size_bytes, 2);
                assert_eq!(*actor_user_id, user_id);
            }
            other => panic!("unexpected notification kind: {other:?}"),
        }

        net.shutdown().await;
    }

    #[tokio::test]
    async fn put_object_with_anonymous_actor_emits_no_watch_event() {
        let realm_id = RealmId([42u8; 32]);
        let (_storage_dir, context, net) = build_watch_context(realm_id, [42u8; 32]).await;
        let holder = net.node_id();
        net.replace_watch_interest(data_uploaded_interest(
            realm_id,
            holder,
            data_watch_resource_path(Ulid::r#gen(), net.node_id(), "bucket", ""),
        ));

        let service = ArunaS3Service::new(context.clone(), realm_id, net.node_id()).await;
        let anonymous = UserId::nil(realm_id);
        let group_id = Ulid::r#gen();
        let auth = AuthContext {
            user_id: anonymous,
            realm_id,
            path_restrictions: None,
        };
        let checksum_request = UploadChecksumRequest {
            expected: Vec::new(),
            response_algorithm: None,
            checksum_type: ChecksumType::from_static(ChecksumType::FULL_OBJECT),
        };

        service
            .put_object_response(
                &checksum_request,
                auth,
                group_id,
                "bucket".to_string(),
                "object".to_string(),
                PutObjectResult {
                    location: response_location(anonymous),
                    version_id: Ulid::r#gen(),
                },
            )
            .await
            .expect("committed PUT response should succeed");

        assert!(
            read_watch_inbox_rows(context.as_ref()).await.is_empty(),
            "an anonymous actor must not emit a watch event"
        );

        net.shutdown().await;
    }

    #[tokio::test]
    async fn reference_refresh_queue_failure_does_not_surface_as_stream_error() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let context = Arc::new(DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        });
        let refresh = ReferenceMetadataRefresh {
            bucket: "bucket".to_string(),
            key: "reference".to_string(),
            version_id: Ulid::r#gen(),
            metadata: source_metadata(2, "etag"),
            refreshed_at: UNIX_EPOCH.checked_sub(Duration::from_secs(1)).unwrap(),
        };

        let queue_result = drive(
            QueueReferenceMetadataRefreshOperation::new(refresh.clone()),
            context.as_ref(),
        )
        .await;
        assert!(
            queue_result.is_err(),
            "test refresh must fail queueing to exercise the callback error path"
        );

        let mut blob = attach_reference_metadata_refresh(
            BackendStream::new(stream::iter(vec![Ok::<_, std::io::Error>(
                Bytes::from_static(b"ok"),
            )])),
            context,
            refresh,
        );

        let mut body = Vec::new();
        let mut errors = Vec::new();
        while let Some(result) = blob.next().await {
            match result {
                Ok(bytes) => body.extend_from_slice(&bytes),
                Err(error) => errors.push(error.to_string()),
            }
        }

        assert_eq!(body, b"ok");
        assert!(errors.is_empty(), "unexpected stream errors: {errors:?}");
    }

    #[tokio::test]
    async fn stale_reference_metadata_refresh_does_not_overwrite_newer_cache() {
        let test = setup_state();
        let last_refresh = UNIX_EPOCH + Duration::from_secs(20);
        let original_metadata = source_metadata(10, "original");
        write_reference_version(&test, original_metadata.clone(), last_refresh).await;

        refresh_reference_metadata(
            test.context.clone(),
            refresh(
                &test,
                source_metadata(20, "older"),
                UNIX_EPOCH + Duration::from_secs(10),
            ),
        )
        .await
        .unwrap();
        assert_reference_state(&test, &original_metadata, last_refresh).await;

        refresh_reference_metadata(
            test.context.clone(),
            refresh(&test, source_metadata(30, "equal"), last_refresh),
        )
        .await
        .unwrap();

        assert_reference_state(&test, &original_metadata, last_refresh).await;
    }

    #[tokio::test]
    async fn newer_reference_metadata_refresh_updates_cache() {
        let test = setup_state();
        let last_refresh = UNIX_EPOCH + Duration::from_secs(20);
        let refreshed_at = UNIX_EPOCH + Duration::from_secs(30);
        let new_metadata = source_metadata(20, "newer");
        write_reference_version(&test, source_metadata(10, "original"), last_refresh).await;

        refresh_reference_metadata(
            test.context.clone(),
            refresh(&test, new_metadata.clone(), refreshed_at),
        )
        .await
        .unwrap();

        assert_reference_state(&test, &new_metadata, refreshed_at).await;
    }

    fn setup_state() -> TestState {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let context = Arc::new(DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        });
        let realm_id = aruna_core::structs::RealmId([9u8; 32]);

        TestState {
            _storage_dir: storage_dir,
            context,
            bucket: "bucket".to_string(),
            key: "key".to_string(),
            version_id: Ulid::r#gen(),
            created_by: UserId::local(Ulid::r#gen(), realm_id),
        }
    }

    fn response_location(created_by: UserId) -> BackendLocation {
        let mut hashes = HashMap::new();
        hashes.insert(HASH_MD5.to_string(), vec![1u8; 16]);

        BackendLocation {
            root: "/tmp".to_string(),
            storage_bucket: "objects".to_string(),
            backend_path: "bucket/object".to_string(),
            ulid: Ulid::r#gen(),
            compressed: false,
            encrypted: false,
            created_by,
            created_at: UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 2,
            hashes,
        }
    }

    async fn write_storage_value(
        storage: &storage::StorageHandle,
        keyspace: &str,
        key: Vec<u8>,
        value: Vec<u8>,
    ) {
        let event = storage
            .send_storage_effect(StorageEffect::Write {
                key_space: keyspace.to_string(),
                key: key.into(),
                value: value.into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    async fn read_storage_value(
        storage: &storage::StorageHandle,
        keyspace: &str,
        key: Vec<u8>,
    ) -> Option<byteview::ByteView> {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = storage
            .send_storage_effect(StorageEffect::Read {
                key_space: keyspace.to_string(),
                key: key.into(),
                txn_id: None,
            })
            .await
        else {
            panic!("unexpected storage read event")
        };

        value
    }

    async fn write_reference_version(
        test: &TestState,
        cached_metadata: SourceMetadata,
        last_refresh: SystemTime,
    ) {
        let version = BlobVersion::reference(
            source_binding(),
            cached_metadata,
            UNIX_EPOCH,
            test.created_by,
            last_refresh,
        );
        let event = test
            .context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: version_key(test).into(),
                value: version.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    async fn assert_reference_state(
        test: &TestState,
        expected_metadata: &SourceMetadata,
        expected_last_refresh: SystemTime,
    ) {
        let event = test
            .context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: version_key(test).into(),
                txn_id: None,
            })
            .await;
        let Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) = event
        else {
            panic!("unexpected version read event: {event:?}");
        };
        let version = BlobVersion::from_bytes(value.as_ref()).unwrap();
        let BlobVersionState::Reference {
            cached_metadata,
            last_refresh,
            ..
        } = version.state
        else {
            panic!("version was not a reference");
        };
        assert_eq!(cached_metadata, *expected_metadata);
        assert_eq!(last_refresh, expected_last_refresh);
    }

    fn refresh(
        test: &TestState,
        metadata: SourceMetadata,
        refreshed_at: SystemTime,
    ) -> ReferenceMetadataRefresh {
        ReferenceMetadataRefresh {
            bucket: test.bucket.clone(),
            key: test.key.clone(),
            version_id: test.version_id,
            metadata,
            refreshed_at,
        }
    }

    fn version_key(test: &TestState) -> Vec<u8> {
        VersionKey::new(&test.bucket, &test.key, test.version_id)
            .to_bytes()
            .unwrap()
    }

    fn source_metadata(content_length: u64, etag: &str) -> SourceMetadata {
        SourceMetadata {
            content_length,
            content_type: Some("application/octet-stream".to_string()),
            etag: Some(etag.to_string()),
            last_modified: Some(UNIX_EPOCH + Duration::from_secs(content_length)),
            source_version: None,
        }
    }

    fn source_binding() -> VersionSourceBinding {
        VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: PortableSourceDescriptor {
                kind: SourceConnectorKind::Http,
                public_config: HashMap::new(),
                source_path: "source/path".to_string(),
                version_selector: None,
                capabilities: Vec::new(),
                origin_node_id: None,
            },
            connector_id: None,
        }
    }

    // ------------------------------------------------------------------
    // ListObjectsV2 API-level tests
    // ------------------------------------------------------------------

    async fn write_head(
        storage: &storage::StorageHandle,
        bucket: &str,
        key: &str,
        version_id: Ulid,
    ) {
        let _ = storage
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_HEAD_KEYSPACE.to_string(),
                key: BlobHeadKey::new(bucket, key).to_bytes().unwrap().into(),
                value: CurrentVersionPointer::new(version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                txn_id: None,
            })
            .await;
    }

    #[allow(clippy::too_many_arguments)]
    async fn write_materialized_version(
        storage: &storage::StorageHandle,
        bucket: &str,
        key: &str,
        version_id: Ulid,
        hash: [u8; 32],
        created_by: UserId,
        created_at: SystemTime,
        blob_size: u64,
    ) {
        let version = BlobVersion::materialized(hash, created_at, created_by, None);
        let _ = storage
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: VersionKey::new(bucket, key, version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                value: version.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;

        let location = BackendLocation {
            root: "/tmp".to_string(),
            storage_bucket: "objects".to_string(),
            backend_path: format!("path/{key}"),
            ulid: Ulid::r#gen(),
            compressed: false,
            encrypted: false,
            created_by,
            created_at,
            staging: false,
            partial: false,
            blob_size,
            hashes: HashMap::new(),
        };
        let _ = storage
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_LOCATIONS_KEYSPACE.to_string(),
                key: hash.to_vec().into(),
                value: location.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;
    }

    #[allow(clippy::too_many_arguments)]
    async fn write_reference_version_with_metadata(
        storage: &storage::StorageHandle,
        bucket: &str,
        key: &str,
        version_id: Ulid,
        metadata: SourceMetadata,
        created_at: SystemTime,
        created_by: UserId,
        last_refresh: SystemTime,
    ) {
        let version = BlobVersion::reference(
            source_binding(),
            metadata,
            created_at,
            created_by,
            last_refresh,
        );
        let _ = storage
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: VersionKey::new(bucket, key, version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                value: version.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;

        let _ = storage
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_HEAD_KEYSPACE.to_string(),
                key: BlobHeadKey::new(bucket, key).to_bytes().unwrap().into(),
                value: CurrentVersionPointer::new(version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                txn_id: None,
            })
            .await;
    }

    fn test_user_access(group_id: Ulid, realm_id: RealmId) -> UserAccess {
        UserAccess {
            access_key: "test-key".to_string(),
            user_identity: UserId::local(Ulid::r#gen(), realm_id),
            group_id,
            secret: "secret".to_string(),
            expiry: SystemTime::now() + Duration::from_secs(3600),
            path_restrictions: None,
            issued_by: [0u8; 32],
            revoked_at: None,
        }
    }

    fn test_bucket_info(group_id: Ulid, created_by: UserId) -> BucketInfo {
        BucketInfo {
            group_id,
            created_at: UNIX_EPOCH,
            created_by,
            cors_configuration: None,
        }
    }

    async fn seed_materialized_keys(
        storage_handle: &storage::StorageHandle,
        bucket: &str,
        keys: &[&str],
        created_by: UserId,
        created_at: SystemTime,
    ) {
        for key in keys {
            let version_id = Ulid::r#gen();
            let hash = [key.len() as u8; 32];
            write_head(storage_handle, bucket, key, version_id).await;
            write_materialized_version(
                storage_handle,
                bucket,
                key,
                version_id,
                hash,
                created_by,
                created_at,
                42,
            )
            .await;
        }
    }

    fn test_list_objects_v2_request(
        extensions: Extensions,
        input: ListObjectsV2Input,
    ) -> S3Request<ListObjectsV2Input> {
        S3Request {
            input,
            method: Method::GET,
            uri: Uri::from_static("/"),
            headers: HeaderMap::new(),
            extensions,
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    #[tokio::test]
    async fn test_list_objects_v2_delimiter_grouping() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let context = Arc::new(DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        });
        let realm_id = RealmId([2u8; 32]);
        let group_id = Ulid::r#gen();
        let created_by = UserId::local(Ulid::r#gen(), realm_id);
        let created_at = UNIX_EPOCH;

        let service = ArunaS3Service::new(
            context.clone(),
            realm_id,
            NodeId::from_bytes(&[0u8; 32]).unwrap(),
        )
        .await;

        seed_materialized_keys(
            &storage_handle,
            "bucket",
            &["dir-a/1", "dir-a/2", "dir-b/1", "root.txt"],
            created_by,
            created_at,
        )
        .await;

        let mut extensions = Extensions::new();
        extensions.insert(test_user_access(group_id, realm_id));
        extensions.insert(test_bucket_info(group_id, created_by));

        let req = test_list_objects_v2_request(
            extensions,
            ListObjectsV2Input {
                bucket: "bucket".to_string(),
                continuation_token: None,
                delimiter: Some("/".to_string()),
                encoding_type: None,
                expected_bucket_owner: None,
                fetch_owner: None,
                max_keys: Some(10),
                optional_object_attributes: None,
                prefix: None,
                request_payer: None,
                start_after: None,
            },
        );

        let response = service.list_objects_v2(req).await.unwrap();
        let output = response.output;

        let mut common_prefixes: Vec<_> = output
            .common_prefixes
            .unwrap_or_default()
            .into_iter()
            .filter_map(|cp| cp.prefix)
            .collect();
        common_prefixes.sort();
        assert_eq!(common_prefixes, vec!["dir-a/", "dir-b/"]);

        let mut contents: Vec<_> = output
            .contents
            .unwrap_or_default()
            .into_iter()
            .filter_map(|obj| obj.key)
            .collect();
        contents.sort();
        assert_eq!(contents, vec!["root.txt"]);

        assert_eq!(output.is_truncated, Some(false));
    }

    #[tokio::test]
    async fn test_list_objects_v2_pagination_with_delimiter() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let context = Arc::new(DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        });
        let realm_id = RealmId([3u8; 32]);
        let group_id = Ulid::r#gen();
        let created_by = UserId::local(Ulid::r#gen(), realm_id);
        let created_at = UNIX_EPOCH;

        let service = ArunaS3Service::new(
            context.clone(),
            realm_id,
            NodeId::from_bytes(&[0u8; 32]).unwrap(),
        )
        .await;

        seed_materialized_keys(
            &storage_handle,
            "bucket",
            &["a/1", "a/2", "b.txt"],
            created_by,
            created_at,
        )
        .await;

        let mut continuation_token = None;
        let mut all_keys = Vec::new();
        let mut all_prefixes = Vec::new();
        let mut total_pages = 0;

        loop {
            let mut extensions = Extensions::new();
            extensions.insert(test_user_access(group_id, realm_id));
            extensions.insert(test_bucket_info(group_id, created_by));

            let req = test_list_objects_v2_request(
                extensions,
                ListObjectsV2Input {
                    bucket: "bucket".to_string(),
                    continuation_token,
                    delimiter: Some("/".to_string()),
                    encoding_type: None,
                    expected_bucket_owner: None,
                    fetch_owner: None,
                    max_keys: Some(1),
                    optional_object_attributes: None,
                    prefix: None,
                    request_payer: None,
                    start_after: None,
                },
            );

            let response = service.list_objects_v2(req).await.unwrap();
            let output = response.output;

            total_pages += 1;
            for obj in output.contents.unwrap_or_default() {
                if let Some(key) = obj.key {
                    all_keys.push(key);
                }
            }
            for cp in output.common_prefixes.unwrap_or_default() {
                if let Some(prefix) = cp.prefix {
                    all_prefixes.push(prefix);
                }
            }

            continuation_token = output.next_continuation_token;
            if continuation_token.is_none() {
                break;
            }
        }

        assert_eq!(all_prefixes, vec!["a/"]);
        assert_eq!(all_keys, vec!["b.txt"]);
        assert_eq!(total_pages, 2);
    }

    #[tokio::test]
    async fn test_list_objects_v2_prefix_only_page_is_not_truncated() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let context = Arc::new(DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        });
        let realm_id = RealmId([33u8; 32]);
        let group_id = Ulid::r#gen();
        let created_by = UserId::local(Ulid::r#gen(), realm_id);

        let service =
            ArunaS3Service::new(context, realm_id, NodeId::from_bytes(&[0u8; 32]).unwrap()).await;

        seed_materialized_keys(
            &storage_handle,
            "bucket",
            &["a/1", "a/2"],
            created_by,
            UNIX_EPOCH,
        )
        .await;

        let mut extensions = Extensions::new();
        extensions.insert(test_user_access(group_id, realm_id));
        extensions.insert(test_bucket_info(group_id, created_by));

        let req = test_list_objects_v2_request(
            extensions,
            ListObjectsV2Input {
                bucket: "bucket".to_string(),
                continuation_token: None,
                delimiter: Some("/".to_string()),
                encoding_type: None,
                expected_bucket_owner: None,
                fetch_owner: None,
                max_keys: Some(1),
                optional_object_attributes: None,
                prefix: None,
                request_payer: None,
                start_after: None,
            },
        );

        let response = service.list_objects_v2(req).await.unwrap();
        let output = response.output;

        let prefixes: Vec<_> = output
            .common_prefixes
            .unwrap_or_default()
            .into_iter()
            .filter_map(|prefix| prefix.prefix)
            .collect();

        assert_eq!(prefixes, vec!["a/"]);
        assert_eq!(output.contents.unwrap_or_default().len(), 0);
        assert_eq!(output.key_count, Some(1));
        assert_eq!(output.is_truncated, Some(false));
        assert!(output.next_continuation_token.is_none());
    }

    #[tokio::test]
    async fn test_list_objects_v2_counts_prefixes_and_contents_in_order() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let context = Arc::new(DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        });
        let realm_id = RealmId([34u8; 32]);
        let group_id = Ulid::r#gen();
        let created_by = UserId::local(Ulid::r#gen(), realm_id);

        let service = ArunaS3Service::new(
            context.clone(),
            realm_id,
            NodeId::from_bytes(&[0u8; 32]).unwrap(),
        )
        .await;

        seed_materialized_keys(
            &storage_handle,
            "bucket",
            &["a.txt", "b/1", "c.txt"],
            created_by,
            UNIX_EPOCH,
        )
        .await;

        let mut extensions = Extensions::new();
        extensions.insert(test_user_access(group_id, realm_id));
        extensions.insert(test_bucket_info(group_id, created_by));

        let first_response = service
            .list_objects_v2(test_list_objects_v2_request(
                extensions,
                ListObjectsV2Input {
                    bucket: "bucket".to_string(),
                    continuation_token: None,
                    delimiter: Some("/".to_string()),
                    encoding_type: None,
                    expected_bucket_owner: None,
                    fetch_owner: None,
                    max_keys: Some(2),
                    optional_object_attributes: None,
                    prefix: None,
                    request_payer: None,
                    start_after: None,
                },
            ))
            .await
            .unwrap()
            .output;

        let first_keys: Vec<_> = first_response
            .contents
            .clone()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|object| object.key)
            .collect();
        let first_prefixes: Vec<_> = first_response
            .common_prefixes
            .clone()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|prefix| prefix.prefix)
            .collect();

        assert_eq!(first_keys, vec!["a.txt"]);
        assert_eq!(first_prefixes, vec!["b/"]);
        assert_eq!(first_response.key_count, Some(2));
        assert_eq!(first_response.is_truncated, Some(true));

        let mut extensions = Extensions::new();
        extensions.insert(test_user_access(group_id, realm_id));
        extensions.insert(test_bucket_info(group_id, created_by));

        let second_response = service
            .list_objects_v2(test_list_objects_v2_request(
                extensions,
                ListObjectsV2Input {
                    bucket: "bucket".to_string(),
                    continuation_token: first_response.next_continuation_token,
                    delimiter: Some("/".to_string()),
                    encoding_type: None,
                    expected_bucket_owner: None,
                    fetch_owner: None,
                    max_keys: Some(2),
                    optional_object_attributes: None,
                    prefix: None,
                    request_payer: None,
                    start_after: None,
                },
            ))
            .await
            .unwrap()
            .output;

        let second_keys: Vec<_> = second_response
            .contents
            .unwrap_or_default()
            .into_iter()
            .filter_map(|object| object.key)
            .collect();
        let second_prefixes: Vec<_> = second_response
            .common_prefixes
            .unwrap_or_default()
            .into_iter()
            .filter_map(|prefix| prefix.prefix)
            .collect();

        assert_eq!(second_keys, vec!["c.txt"]);
        assert!(second_prefixes.is_empty());
        assert_eq!(second_response.key_count, Some(1));
        assert_eq!(second_response.is_truncated, Some(false));
    }

    #[tokio::test]
    async fn test_list_objects_v2_large_group_collapses_to_single_page() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let context = Arc::new(DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        });
        let realm_id = RealmId([4u8; 32]);
        let group_id = Ulid::r#gen();
        let created_by = UserId::local(Ulid::r#gen(), realm_id);
        let created_at = UNIX_EPOCH + Duration::from_secs(1);

        let service = ArunaS3Service::new(
            context.clone(),
            realm_id,
            NodeId::from_bytes(&[0u8; 32]).unwrap(),
        )
        .await;

        // Seed 305 keys under "dir/" so delimiter collapses them into one
        // common prefix; the scan seeks past the group instead of paging
        // through it.
        for i in 0..305 {
            let key = format!("dir/key_{:04}", i);
            let version_id = Ulid::r#gen();
            let hash = [i as u8; 32];
            write_head(&storage_handle, "bucket", &key, version_id).await;
            write_materialized_version(
                &storage_handle,
                "bucket",
                &key,
                version_id,
                hash,
                created_by,
                created_at,
                1,
            )
            .await;
        }

        let mut extensions = Extensions::new();
        extensions.insert(test_user_access(group_id, realm_id));
        extensions.insert(test_bucket_info(group_id, created_by));

        let req = test_list_objects_v2_request(
            extensions,
            ListObjectsV2Input {
                bucket: "bucket".to_string(),
                continuation_token: None,
                delimiter: Some("/".to_string()),
                encoding_type: None,
                expected_bucket_owner: None,
                fetch_owner: None,
                max_keys: Some(2),
                optional_object_attributes: None,
                prefix: None,
                request_payer: None,
                start_after: None,
            },
        );

        let response = service.list_objects_v2(req).await.unwrap();
        let output = response.output;

        assert_eq!(output.is_truncated, Some(false));
        assert!(
            output.next_continuation_token.is_none(),
            "single visible entry must not be truncated"
        );
        assert_eq!(output.key_count, Some(1));

        let common_prefixes: Vec<_> = output
            .common_prefixes
            .unwrap_or_default()
            .into_iter()
            .filter_map(|cp| cp.prefix)
            .collect();
        assert_eq!(common_prefixes, vec!["dir/"]);
    }

    #[tokio::test]
    async fn test_list_objects_v2_reference_object_returns_cached_metadata() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let context = Arc::new(DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        });
        let realm_id = RealmId([5u8; 32]);
        let group_id = Ulid::r#gen();
        let created_by = UserId::local(Ulid::r#gen(), realm_id);
        let created_at = UNIX_EPOCH + Duration::from_secs(5);
        let last_refresh = UNIX_EPOCH + Duration::from_secs(20);

        let service = ArunaS3Service::new(
            context.clone(),
            realm_id,
            NodeId::from_bytes(&[0u8; 32]).unwrap(),
        )
        .await;

        let metadata = SourceMetadata {
            content_length: 100,
            content_type: Some("text/csv".to_string()),
            etag: Some("ref-etag-value".to_string()),
            last_modified: Some(UNIX_EPOCH + Duration::from_secs(10)),
            source_version: None,
        };

        let version_id = Ulid::r#gen();
        write_reference_version_with_metadata(
            &storage_handle,
            "bucket",
            "ref-object",
            version_id,
            metadata.clone(),
            created_at,
            created_by,
            last_refresh,
        )
        .await;

        let mut extensions = Extensions::new();
        extensions.insert(test_user_access(group_id, realm_id));
        extensions.insert(test_bucket_info(group_id, created_by));

        let req = S3Request {
            input: ListObjectsV2Input {
                bucket: "bucket".to_string(),
                continuation_token: None,
                delimiter: None,
                encoding_type: None,
                expected_bucket_owner: None,
                fetch_owner: None,
                max_keys: Some(10),
                optional_object_attributes: None,
                prefix: None,
                request_payer: None,
                start_after: None,
            },
            method: Method::GET,
            uri: Uri::from_static("/"),
            headers: HeaderMap::new(),
            extensions,
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        };

        let response = service.list_objects_v2(req).await.unwrap();
        let output = response.output;

        let objects: Vec<_> = output.contents.unwrap_or_default();
        assert_eq!(objects.len(), 1);

        let obj = &objects[0];
        assert_eq!(obj.key.as_deref(), Some("ref-object"));
        assert_eq!(obj.size, Some(100));

        // ETag from source_metadata.etag
        let expected_etag = Some(ETag::Strong("ref-etag-value".to_string()));
        assert_eq!(obj.e_tag, expected_etag);

        // last_modified from source_metadata.last_modified
        assert_eq!(
            obj.last_modified,
            Some((UNIX_EPOCH + Duration::from_secs(10)).into())
        );

        assert_eq!(output.is_truncated, Some(false));
    }

    #[tokio::test]
    async fn test_list_objects_v2_honors_explicit_zero_max_keys() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let context = Arc::new(DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        });
        let realm_id = RealmId([6u8; 32]);
        let group_id = Ulid::r#gen();
        let created_by = UserId::local(Ulid::r#gen(), realm_id);
        let created_at = UNIX_EPOCH + Duration::from_secs(5);

        let service = ArunaS3Service::new(
            context.clone(),
            realm_id,
            NodeId::from_bytes(&[0u8; 32]).unwrap(),
        )
        .await;

        seed_materialized_keys(
            &storage_handle,
            "bucket",
            &["alpha"],
            created_by,
            created_at,
        )
        .await;

        let mut extensions = Extensions::new();
        extensions.insert(test_user_access(group_id, realm_id));
        extensions.insert(test_bucket_info(group_id, created_by));

        let req = test_list_objects_v2_request(
            extensions,
            ListObjectsV2Input {
                bucket: "bucket".to_string(),
                continuation_token: None,
                delimiter: None,
                encoding_type: None,
                expected_bucket_owner: None,
                fetch_owner: None,
                max_keys: Some(0),
                optional_object_attributes: None,
                prefix: None,
                request_payer: None,
                start_after: None,
            },
        );

        let response = service.list_objects_v2(req).await.unwrap();
        let output = response.output;

        assert_eq!(output.max_keys, Some(0));
        assert_eq!(output.key_count, Some(0));
        assert_eq!(output.is_truncated, Some(false));
        assert_eq!(output.contents.unwrap_or_default().len(), 0);
        assert_eq!(output.common_prefixes.unwrap_or_default().len(), 0);
        assert!(output.next_continuation_token.is_none());
    }

    #[tokio::test]
    async fn test_list_objects_v2_start_after_includes_later_common_prefixes() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let context = Arc::new(DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        });
        let realm_id = RealmId([7u8; 32]);
        let group_id = Ulid::r#gen();
        let created_by = UserId::local(Ulid::r#gen(), realm_id);
        let created_at = UNIX_EPOCH;

        let service = ArunaS3Service::new(
            context.clone(),
            realm_id,
            NodeId::from_bytes(&[0u8; 32]).unwrap(),
        )
        .await;

        seed_materialized_keys(
            &storage_handle,
            "bucket",
            &["dir-a/1", "dir-b/1", "root.txt"],
            created_by,
            created_at,
        )
        .await;

        let mut extensions = Extensions::new();
        extensions.insert(test_user_access(group_id, realm_id));
        extensions.insert(test_bucket_info(group_id, created_by));

        let req = test_list_objects_v2_request(
            extensions,
            ListObjectsV2Input {
                bucket: "bucket".to_string(),
                continuation_token: None,
                delimiter: Some("/".to_string()),
                encoding_type: None,
                expected_bucket_owner: None,
                fetch_owner: None,
                max_keys: Some(10),
                optional_object_attributes: None,
                prefix: None,
                request_payer: None,
                start_after: Some("dir-b/".to_string()),
            },
        );

        let response = service.list_objects_v2(req).await.unwrap();
        let output = response.output;

        let common_prefixes: Vec<_> = output
            .common_prefixes
            .unwrap_or_default()
            .into_iter()
            .filter_map(|cp| cp.prefix)
            .collect();
        let contents: Vec<_> = output
            .contents
            .unwrap_or_default()
            .into_iter()
            .filter_map(|obj| obj.key)
            .collect();

        assert_eq!(common_prefixes, vec!["dir-b/"]);
        assert_eq!(contents, vec!["root.txt"]);
        assert_eq!(output.key_count, Some(2));
    }

    #[tokio::test]
    async fn test_list_objects_v2_requires_user_access_extension() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let context = Arc::new(DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        });
        let realm_id = RealmId([8u8; 32]);
        let service =
            ArunaS3Service::new(context, realm_id, NodeId::from_bytes(&[0u8; 32]).unwrap()).await;

        let req = test_list_objects_v2_request(
            Extensions::new(),
            ListObjectsV2Input {
                bucket: "bucket".to_string(),
                continuation_token: None,
                delimiter: None,
                encoding_type: None,
                expected_bucket_owner: None,
                fetch_owner: None,
                max_keys: Some(10),
                optional_object_attributes: None,
                prefix: None,
                request_payer: None,
                start_after: None,
            },
        );

        let err = service.list_objects_v2(req).await.unwrap_err();
        assert_eq!(*err.code(), S3ErrorCode::UnexpectedContent);
    }

    #[tokio::test]
    async fn test_list_objects_v2_clamps_and_validates_max_keys() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let context = Arc::new(DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        });
        let realm_id = RealmId([35u8; 32]);
        let group_id = Ulid::r#gen();
        let created_by = UserId::local(Ulid::r#gen(), realm_id);

        let service =
            ArunaS3Service::new(context, realm_id, NodeId::from_bytes(&[0u8; 32]).unwrap()).await;

        seed_materialized_keys(
            &storage_handle,
            "bucket",
            &["a", "b"],
            created_by,
            UNIX_EPOCH,
        )
        .await;

        let mut extensions = Extensions::new();
        extensions.insert(test_user_access(group_id, realm_id));
        extensions.insert(test_bucket_info(group_id, created_by));

        let input = ListObjectsV2Input {
            bucket: "bucket".to_string(),
            continuation_token: None,
            delimiter: None,
            encoding_type: None,
            expected_bucket_owner: None,
            fetch_owner: None,
            max_keys: Some(5000),
            optional_object_attributes: None,
            prefix: None,
            request_payer: None,
            start_after: None,
        };
        let req = test_list_objects_v2_request(extensions.clone(), input.clone());
        let output = service.list_objects_v2(req).await.unwrap().output;
        assert_eq!(output.max_keys, Some(1000));
        assert_eq!(output.key_count, Some(2));

        let req = test_list_objects_v2_request(
            extensions,
            ListObjectsV2Input {
                max_keys: Some(-1),
                ..input
            },
        );
        let err = service.list_objects_v2(req).await.unwrap_err();
        assert_eq!(*err.code(), S3ErrorCode::InvalidArgument);
    }

    #[tokio::test]
    async fn test_list_objects_v2_applies_url_encoding_type() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let context = Arc::new(DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        });
        let realm_id = RealmId([36u8; 32]);
        let group_id = Ulid::r#gen();
        let created_by = UserId::local(Ulid::r#gen(), realm_id);

        let service =
            ArunaS3Service::new(context, realm_id, NodeId::from_bytes(&[0u8; 32]).unwrap()).await;

        seed_materialized_keys(
            &storage_handle,
            "bucket",
            &["a b+c.txt", "d e/f.txt"],
            created_by,
            UNIX_EPOCH,
        )
        .await;

        let mut extensions = Extensions::new();
        extensions.insert(test_user_access(group_id, realm_id));
        extensions.insert(test_bucket_info(group_id, created_by));

        let req = test_list_objects_v2_request(
            extensions,
            ListObjectsV2Input {
                bucket: "bucket".to_string(),
                continuation_token: None,
                delimiter: Some("/".to_string()),
                encoding_type: Some(EncodingType::from_static(EncodingType::URL)),
                expected_bucket_owner: None,
                fetch_owner: None,
                max_keys: Some(10),
                optional_object_attributes: None,
                prefix: None,
                request_payer: None,
                start_after: Some("a".to_string()),
            },
        );
        let output = service.list_objects_v2(req).await.unwrap().output;

        let keys: Vec<_> = output
            .contents
            .unwrap_or_default()
            .into_iter()
            .filter_map(|object| object.key)
            .collect();
        let prefixes: Vec<_> = output
            .common_prefixes
            .unwrap_or_default()
            .into_iter()
            .filter_map(|prefix| prefix.prefix)
            .collect();

        assert_eq!(keys, vec!["a%20b%2Bc.txt"]);
        assert_eq!(prefixes, vec!["d%20e%2F"]);
        assert_eq!(output.delimiter.as_deref(), Some("%2F"));
        assert_eq!(output.start_after.as_deref(), Some("a"));
        assert_eq!(
            output
                .encoding_type
                .map(|encoding| encoding.as_str().to_string()),
            Some("url".to_string())
        );
    }

    #[tokio::test]
    async fn test_list_objects_v2_fetch_owner_includes_group() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let context = Arc::new(DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        });
        let realm_id = RealmId([37u8; 32]);
        let group_id = Ulid::r#gen();
        let created_by = UserId::local(Ulid::r#gen(), realm_id);

        let service =
            ArunaS3Service::new(context, realm_id, NodeId::from_bytes(&[0u8; 32]).unwrap()).await;

        seed_materialized_keys(&storage_handle, "bucket", &["a"], created_by, UNIX_EPOCH).await;

        let mut extensions = Extensions::new();
        extensions.insert(test_user_access(group_id, realm_id));
        extensions.insert(test_bucket_info(group_id, created_by));

        let req = test_list_objects_v2_request(
            extensions,
            ListObjectsV2Input {
                bucket: "bucket".to_string(),
                continuation_token: None,
                delimiter: None,
                encoding_type: None,
                expected_bucket_owner: None,
                fetch_owner: Some(true),
                max_keys: Some(10),
                optional_object_attributes: None,
                prefix: None,
                request_payer: None,
                start_after: None,
            },
        );
        let output = service.list_objects_v2(req).await.unwrap().output;

        let owners: Vec<_> = output
            .contents
            .unwrap_or_default()
            .into_iter()
            .filter_map(|object| object.owner.and_then(|owner| owner.id))
            .collect();
        assert_eq!(owners, vec![group_id.to_string()]);
    }
}
