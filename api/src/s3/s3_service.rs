#![allow(clippy::result_large_err)]

use crate::s3::checksum::{
    ApplyChecksums, ChecksumSelection, UploadChecksumRequest, checksum_mode_enabled,
    encode_checksums, parse_upload_checksum_request,
};
use crate::s3::error::IntoS3Error;
use crate::s3::replication::spawn_version_replication;
use crate::s3::util::{
    convert_input, multipart_checksum_type_from_s3, parse_completed_part,
    parse_multipart_checksum_hint, parse_multipart_part_number, parse_upload_id, parse_version_id,
    s3_checksum_type_from_multipart, to_base64,
};
use aruna_core::NodeId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::BLOB_VERSIONS_KEYSPACE;
use aruna_core::stream::BackendStream;
use aruna_core::structs::checksum::HASH_MD5;
use aruna_core::structs::{
    AuthContext, BlobVersion, BlobVersionState, BucketInfo, Permission, RealmId, SourceMetadata,
    UserAccess, VersionKey, blob_bucket_permission_path,
};
use aruna_operations::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::s3::abort_multipart_upload::{
    AbortMultipartUploadInput as AMUI, AbortMultipartUploadOperation,
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
use aruna_operations::s3::get_object::{
    GetObjectInput as GOI, GetObjectOperation, GetObjectResult, ObjectRangeRequest,
};
use aruna_operations::s3::head_object::{HeadObjectInput as HOI, HeadObjectOperation};
use aruna_operations::s3::list_buckets::{ListBucketsInput as LBI, ListBucketsOperation};
use aruna_operations::s3::list_objects_v2::{ListObjectsV2Input as LOV2I, ListObjectsV2Operation};
use aruna_operations::s3::put_bucket_replication::{
    DeleteBucketReplicationOperation, GetBucketReplicationOperation, PutBucketReplicationOperation,
};
use aruna_operations::s3::put_object::{PutObjectConfig, PutObjectOperation, PutObjectResult};
use aruna_operations::s3::upload_part::{UploadPartInput as UPI, UploadPartOperation};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use s3s::dto::{
    AbortMultipartUploadInput, AbortMultipartUploadOutput, Bucket, CommonPrefix,
    CompleteMultipartUploadInput, CompleteMultipartUploadOutput, CreateBucketInput,
    CreateBucketOutput, CreateMultipartUploadInput, CreateMultipartUploadOutput, DeleteBucketInput,
    DeleteBucketOutput, DeleteBucketReplicationInput, DeleteBucketReplicationOutput,
    DeleteMarkerReplication, DeleteMarkerReplicationStatus, DeleteObjectInput, DeleteObjectOutput,
    Destination, ETag, GetBucketReplicationInput, GetBucketReplicationOutput,
    GetObjectAttributesInput, GetObjectAttributesOutput, GetObjectInput, GetObjectOutput,
    HeadObjectInput, HeadObjectOutput, LastModified, ListBucketsInput, ListBucketsOutput,
    ListObjectsV2Input, ListObjectsV2Output, Object, PutBucketReplicationInput,
    PutBucketReplicationOutput, PutObjectInput, PutObjectOutput, ReplicationConfiguration,
    ReplicationRule, ReplicationRuleStatus, StreamingBlob, UploadPartInput, UploadPartOutput,
};
use s3s::{S3, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use std::collections::BTreeSet;
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

#[derive(Debug)]
struct ReferenceMetadataRefresh {
    bucket: String,
    key: String,
    version_id: ulid::Ulid,
    metadata: SourceMetadata,
    refreshed_at: SystemTime,
}

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
    const LIST_OBJECTS_V2_MAX_PAGES: usize = 100;

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

    fn complete_multipart_upload_response(
        &self,
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
                .map(|value| ETag::Strong(to_base64(value))),
            version_id: Some(result.version_id.to_string()),
            ..Default::default()
        };

        output.apply_checksums(encode_checksums(
            &result.response_hashes,
            ChecksumSelection::Requested(checksum_request.response_algorithm),
            s3_checksum_type_from_multipart(result.checksum_type),
        ));

        spawn_version_replication(
            self.state.clone(),
            self.realm_id,
            self.node_id,
            replication_auth,
            replication_bucket,
            replication_key,
            result.version_id,
            false,
        );

        Ok(S3Response::new(output))
    }

    fn put_object_response(
        &self,
        checksum_request: &UploadChecksumRequest,
        replication_auth: AuthContext,
        replication_bucket: String,
        replication_key: String,
        result: PutObjectResult,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        let mut output = PutObjectOutput {
            e_tag: Some(ETag::Strong(to_base64(
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
        spawn_version_replication(
            self.state.clone(),
            self.realm_id,
            self.node_id,
            replication_auth,
            replication_bucket,
            replication_key,
            result.version_id,
            false,
        );

        Ok(S3Response::new(output))
    }

    fn delete_object_response(
        &self,
        replication_auth: AuthContext,
        replication_bucket: String,
        replication_key: String,
        replicate_latest_delete: bool,
        result: DeleteObjectResult,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        if replicate_latest_delete {
            spawn_version_replication(
                self.state.clone(),
                self.realm_id,
                self.node_id,
                replication_auth,
                replication_bucket,
                replication_key,
                result.version_id,
                result.delete_marker,
            );
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
                        .map(|value| ETag::Strong(to_base64(value)))
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

    fn decode_list_objects_v2_continuation_token(token: Option<&str>) -> S3Result<Option<Vec<u8>>> {
        token
            .map(|token| {
                STANDARD
                    .decode(token)
                    .map_err(|_| s3_error!(InvalidArgument, "Invalid continuation token"))
            })
            .transpose()
    }

    fn encode_list_objects_v2_continuation_token(token: Option<&[u8]>) -> Option<String> {
        token.map(|token| STANDARD.encode(token))
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

async fn refresh_reference_metadata(
    context: Arc<DriverContext>,
    refresh: ReferenceMetadataRefresh,
) -> Result<(), String> {
    let version_key = VersionKey::new(&refresh.bucket, &refresh.key, refresh.version_id)
        .to_bytes()
        .map_err(|err| err.to_string())?;
    let txn_id = match context
        .storage_handle
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        Event::Storage(StorageEvent::Error { error }) => return Err(error.to_string()),
        other => return Err(format!("unexpected start transaction event: {other:?}")),
    };

    let refreshed_value = match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: version_key.clone().into(),
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => {
            let version = match BlobVersion::from_bytes(value.as_ref()) {
                Ok(version) => version,
                Err(err) => {
                    abort_reference_refresh(&context, txn_id).await;
                    return Err(err.to_string());
                }
            };
            let BlobVersion {
                created_at,
                created_by,
                state,
            } = version;
            let BlobVersionState::Reference {
                source,
                last_refresh,
                ..
            } = state
            else {
                abort_reference_refresh(&context, txn_id).await;
                return Ok(());
            };
            if refresh.refreshed_at <= last_refresh {
                None
            } else {
                Some(
                    match BlobVersion::reference(
                        source,
                        refresh.metadata,
                        created_at,
                        created_by,
                        refresh.refreshed_at,
                    )
                    .to_bytes()
                    {
                        Ok(value) => value,
                        Err(err) => {
                            abort_reference_refresh(&context, txn_id).await;
                            return Err(err.to_string());
                        }
                    },
                )
            }
        }
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => {
            abort_reference_refresh(&context, txn_id).await;
            return Ok(());
        }
        Event::Storage(StorageEvent::Error { error }) => {
            abort_reference_refresh(&context, txn_id).await;
            return Err(error.to_string());
        }
        other => {
            abort_reference_refresh(&context, txn_id).await;
            return Err(format!("unexpected version read event: {other:?}"));
        }
    };

    if let Some(refreshed_value) = refreshed_value {
        match context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: version_key.into(),
                value: refreshed_value.into(),
                txn_id: Some(txn_id),
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            Event::Storage(StorageEvent::Error { error }) => {
                abort_reference_refresh(&context, txn_id).await;
                return Err(error.to_string());
            }
            other => {
                abort_reference_refresh(&context, txn_id).await;
                return Err(format!("unexpected version write event: {other:?}"));
            }
        }
    }

    match context
        .storage_handle
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => {
            abort_reference_refresh(&context, txn_id).await;
            Err(error.to_string())
        }
        other => {
            abort_reference_refresh(&context, txn_id).await;
            Err(format!("unexpected commit event: {other:?}"))
        }
    }
}

async fn abort_reference_refresh(context: &DriverContext, txn_id: ulid::Ulid) {
    let _ = context
        .storage_handle
        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
        .await;
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
        let mut continuation_token = Self::decode_list_objects_v2_continuation_token(
            requested_continuation_token.as_deref(),
        )?;
        let max_keys = req
            .input
            .max_keys
            .and_then(|max_keys| usize::try_from(max_keys).ok())
            .unwrap_or(1_000);
        let bucket = req.input.bucket.clone();
        let prefix = req.input.prefix.clone();
        let delimiter = req.input.delimiter.clone();
        let start_after = req.input.start_after.clone();

        let mut contents = Vec::new();
        let mut common_prefixes = BTreeSet::new();

        if req.input.max_keys == Some(0) {
            return Ok(S3Response::new(ListObjectsV2Output {
                name: Some(bucket),
                prefix,
                max_keys: Some(0),
                key_count: Some(0),
                continuation_token: requested_continuation_token,
                is_truncated: Some(false),
                next_continuation_token: None,
                contents: Some(contents),
                common_prefixes: Some(Vec::new()),
                delimiter,
                encoding_type: req.input.encoding_type,
                start_after,
                ..Default::default()
            }));
        }

        let group_id = bucket_info
            .as_ref()
            .map(|bucket_info| bucket_info.group_id)
            .unwrap_or(user_access.group_id);
        let mut next_continuation_token = None;
        let mut backend_pages_scanned = 0usize;

        loop {
            let result = drive(
                ListObjectsV2Operation::new(LOV2I {
                    bucket: bucket.clone(),
                    group_id,
                    continuation_token: continuation_token.clone(),
                    max_keys: Some(max_keys),
                    prefix: prefix.clone(),
                }),
                &self.state,
            )
            .await
            .and_then(|result| result.transpose())
            .map_err(IntoS3Error::into_s3_error)?
            .ok_or_else(|| s3_error!(InternalError, "Failed to list objects"))?;

            let page_next_continuation_token = result.continuation_token.clone();
            let page_len = result.objects.len();
            backend_pages_scanned += 1;

            for (index, object) in result.objects.into_iter().enumerate() {
                let head_bytes = object
                    .head
                    .to_bytes()
                    .map_err(|err| s3_error!(InternalError, "{}", err.to_string()))?;
                let key = object.head.key;

                if continuation_token.is_none()
                    && start_after
                        .as_ref()
                        .is_some_and(|start_after| key.as_str() <= start_after.as_str())
                {
                    continue;
                }

                if let Some(delimiter) = delimiter.as_ref() {
                    let prefix_start = prefix.as_ref().map(|prefix| prefix.len()).unwrap_or(0);
                    if let Some(relative_match) = key[prefix_start..].find(delimiter) {
                        let group_end = prefix_start + relative_match + delimiter.len();
                        let candidate_prefix = key[..group_end].to_string();
                        if continuation_token.is_none()
                            && start_after.as_ref().is_some_and(|start_after| {
                                candidate_prefix.as_str() <= start_after.as_str()
                            })
                        {
                            continue;
                        }
                        common_prefixes.insert(candidate_prefix);
                        if contents.len() + common_prefixes.len() >= max_keys {
                            next_continuation_token = if index + 1 < page_len {
                                Some(head_bytes)
                            } else {
                                page_next_continuation_token.clone()
                            };
                            break;
                        }
                        continue;
                    }
                }

                let response_fields = self.build_object_response_fields(
                    object.location.as_ref(),
                    object.source_metadata.as_ref(),
                    object.last_refresh,
                );
                contents.push(Object {
                    e_tag: response_fields.e_tag,
                    key: Some(key),
                    last_modified: response_fields.last_modified,
                    owner: None,
                    size: response_fields.content_length,
                    ..Default::default()
                });
                if contents.len() + common_prefixes.len() >= max_keys {
                    next_continuation_token = if index + 1 < page_len {
                        Some(head_bytes)
                    } else {
                        page_next_continuation_token.clone()
                    };
                    break;
                }
            }

            if contents.len() + common_prefixes.len() >= max_keys {
                break;
            }

            if backend_pages_scanned >= Self::LIST_OBJECTS_V2_MAX_PAGES
                && page_next_continuation_token.is_some()
            {
                warn!(
                    bucket = %bucket,
                    prefix = ?prefix,
                    delimiter = ?delimiter,
                    max_keys = %max_keys,
                    visible_count = contents.len() + common_prefixes.len(),
                    pages = backend_pages_scanned,
                    "ListObjectsV2 pagination guard triggered; returning truncated response"
                );
                next_continuation_token = page_next_continuation_token.clone();
                break;
            }

            next_continuation_token = page_next_continuation_token.clone();
            if next_continuation_token.is_none() {
                break;
            }

            continuation_token = next_continuation_token.clone();
        }

        let is_truncated = next_continuation_token.is_some();
        if contents.len() > max_keys {
            contents.truncate(max_keys);
        }
        let remaining_common_prefixes = max_keys.saturating_sub(contents.len());
        let common_prefixes: Vec<_> = common_prefixes
            .into_iter()
            .take(remaining_common_prefixes)
            .collect();
        let key_count = contents.len() + common_prefixes.len();
        let next_continuation_token =
            Self::encode_list_objects_v2_continuation_token(next_continuation_token.as_deref());

        Ok(S3Response::new(ListObjectsV2Output {
            name: Some(bucket),
            prefix,
            max_keys: Some(i32::try_from(max_keys).unwrap_or(i32::MAX)),
            key_count: Some(i32::try_from(key_count).unwrap_or(i32::MAX)),
            continuation_token: requested_continuation_token,
            is_truncated: Some(is_truncated),
            next_continuation_token,
            contents: Some(contents),
            common_prefixes: Some(
                common_prefixes
                    .into_iter()
                    .map(|prefix| CommonPrefix {
                        prefix: Some(prefix),
                    })
                    .collect(),
            ),
            delimiter,
            encoding_type: req.input.encoding_type,
            start_after,
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
        let bucket_info = req.extensions.get::<BucketInfo>().cloned();
        let checksum_request = parse_upload_checksum_request(&req.headers)?;
        let replication_auth = AuthContext {
            user_id: user_access.user_identity,
            realm_id: user_access.user_identity.realm_id,
            path_restrictions: user_access.path_restrictions.clone(),
        };
        let replication_bucket = req.input.bucket.clone();
        let replication_key = req.input.key.clone();

        let input = convert_input(req.input)?;
        let config = PutObjectConfig {
            user_id: user_access.user_identity,
            group_id: bucket_info
                .as_ref()
                .map(|bucket_info| bucket_info.group_id)
                .unwrap_or(user_access.group_id),
            realm_id: self.realm_id,
            node_id: self.node_id,
            request: input,
            expected_checksums: checksum_request.expected.clone(),
            checksum_type: Some(checksum_request.checksum_type.as_str().to_string()),
            version_source: None,
            exists: false,
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
            replication_bucket,
            replication_key,
            result,
        )
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
                .map(|value| ETag::Strong(to_base64(value))),
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
        });

        let result = drive(operation, &self.state)
            .await
            .and_then(|result| result.transpose())
            .map_err(IntoS3Error::into_s3_error)?
            .ok_or_else(|| s3_error!(InternalError, "Failed to complete multipart upload"))?;

        self.complete_multipart_upload_response(
            req.input.bucket,
            req.input.key,
            &checksum_request,
            replication_auth,
            result,
        )
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
            let context = self.state.clone();
            result.blob.on_success(move || {
                tokio::spawn(async move {
                    if let Err(err) = refresh_reference_metadata(context, refresh).await {
                        warn!(error = %err, "Failed to refresh reference metadata after read");
                    }
                });
            })
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
        unimplemented!()
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
    use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE};
    use aruna_core::structs::{
        BackendLocation, BlobHeadKey, CurrentVersionPointer, PortableSourceDescriptor,
        SourceConnectorKind, StagingStrategy, VersionSourceBinding,
    };
    use aruna_operations::driver::DriverContext;
    use aruna_storage::storage;
    use http::Extensions;
    use hyper::{HeaderMap, Method, Uri};
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
            automerge_handle: None,
            metadata_handle: None,
            task_handle: None,
        });
        let realm_id = aruna_core::structs::RealmId([9u8; 32]);

        TestState {
            _storage_dir: storage_dir,
            context,
            bucket: "bucket".to_string(),
            key: "key".to_string(),
            version_id: Ulid::new(),
            created_by: UserId::local(Ulid::new(), realm_id),
        }
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
            ulid: Ulid::new(),
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
            user_identity: UserId::local(Ulid::new(), realm_id),
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
            let version_id = Ulid::new();
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
            automerge_handle: None,
            metadata_handle: None,
            task_handle: None,
        });
        let realm_id = RealmId([2u8; 32]);
        let group_id = Ulid::new();
        let created_by = UserId::local(Ulid::new(), realm_id);
        let created_at = UNIX_EPOCH;

        let service = ArunaS3Service::new(
            context.clone(),
            realm_id,
            NodeId::from_bytes(&[0u8; 32]).unwrap(),
        )
        .await;

        // Seed keys: dir-a/1, dir-a/2, dir-b/1, root.txt
        let keys = ["dir-a/1", "dir-a/2", "dir-b/1", "root.txt"];
        for key in &keys {
            let version_id = Ulid::new();
            let hash = [key.len() as u8; 32];
            write_head(&storage_handle, "bucket", key, version_id).await;
            write_materialized_version(
                &storage_handle,
                "bucket",
                key,
                version_id,
                hash,
                created_by,
                created_at,
                42,
            )
            .await;
        }

        let mut extensions = Extensions::new();
        extensions.insert(test_user_access(group_id, realm_id));
        extensions.insert(test_bucket_info(group_id, created_by));

        let req = S3Request {
            input: ListObjectsV2Input {
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
            automerge_handle: None,
            metadata_handle: None,
            task_handle: None,
        });
        let realm_id = RealmId([3u8; 32]);
        let group_id = Ulid::new();
        let created_by = UserId::local(Ulid::new(), realm_id);
        let created_at = UNIX_EPOCH;

        let service = ArunaS3Service::new(
            context.clone(),
            realm_id,
            NodeId::from_bytes(&[0u8; 32]).unwrap(),
        )
        .await;

        // Seed keys: a/1, a/2, b.txt
        let keys = ["a/1", "a/2", "b.txt"];
        for key in &keys {
            let version_id = Ulid::new();
            let hash = [key.len() as u8; 32];
            write_head(&storage_handle, "bucket", key, version_id).await;
            write_materialized_version(
                &storage_handle,
                "bucket",
                key,
                version_id,
                hash,
                created_by,
                created_at,
                42,
            )
            .await;
        }

        // Paginate with max_keys=1, delimiter="/" and collect all results
        let mut continuation_token = None;
        let mut all_keys = Vec::new();
        let mut all_prefixes = Vec::new();
        let mut total_pages = 0;

        loop {
            let mut extensions = Extensions::new();
            extensions.insert(test_user_access(group_id, realm_id));
            extensions.insert(test_bucket_info(group_id, created_by));

            let req = S3Request {
                input: ListObjectsV2Input {
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

        // First page emitted the "a/" common prefix (from a/1)
        // Second page re-emitted "a/" (from a/2, now correctly on page 2)
        // Third page returned b.txt as a content key
        // Common prefixes are repeated per page because the API
        // accumulates them from scratch on each request.
        assert!(!all_prefixes.is_empty());
        assert!(all_prefixes.iter().all(|p| p == "a/"));
        assert_eq!(all_keys, vec!["b.txt"]);
        assert!(total_pages >= 2);
    }

    #[tokio::test]
    async fn test_list_objects_v2_pagination_guard_returns_truncated() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let context = Arc::new(DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            automerge_handle: None,
            metadata_handle: None,
            task_handle: None,
        });
        let realm_id = RealmId([4u8; 32]);
        let group_id = Ulid::new();
        let created_by = UserId::local(Ulid::new(), realm_id);
        let created_at = UNIX_EPOCH + Duration::from_secs(1);

        let service = ArunaS3Service::new(
            context.clone(),
            realm_id,
            NodeId::from_bytes(&[0u8; 32]).unwrap(),
        )
        .await;

        // Seed 305 keys under "dir/" so delimiter collapses them into one
        // common prefix, forcing many backend pages.
        for i in 0..305 {
            let key = format!("dir/key_{:04}", i);
            let version_id = Ulid::new();
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

        let req = S3Request {
            input: ListObjectsV2Input {
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

        assert_eq!(output.is_truncated, Some(true));
        assert!(
            output.next_continuation_token.is_some(),
            "guard should set next_continuation_token"
        );

        // Verify we got a common_prefix
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
            automerge_handle: None,
            metadata_handle: None,
            task_handle: None,
        });
        let realm_id = RealmId([5u8; 32]);
        let group_id = Ulid::new();
        let created_by = UserId::local(Ulid::new(), realm_id);
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

        let version_id = Ulid::new();
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
            automerge_handle: None,
            metadata_handle: None,
            task_handle: None,
        });
        let realm_id = RealmId([6u8; 32]);
        let group_id = Ulid::new();
        let created_by = UserId::local(Ulid::new(), realm_id);
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
    async fn test_list_objects_v2_start_after_filters_common_prefixes() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let context = Arc::new(DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            automerge_handle: None,
            metadata_handle: None,
            task_handle: None,
        });
        let realm_id = RealmId([7u8; 32]);
        let group_id = Ulid::new();
        let created_by = UserId::local(Ulid::new(), realm_id);
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

        assert!(common_prefixes.is_empty());
        assert_eq!(contents, vec!["root.txt"]);
        assert_eq!(output.key_count, Some(1));
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
            automerge_handle: None,
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
}
