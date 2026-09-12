#![allow(clippy::result_large_err)]

mod bucket;
mod multipart;
mod object;
mod response;

use self::response::{attach_reference_refresh, object_metadata, reference_metadata_refresh};

use self::multipart::parse_upload_marker;
use self::object::{
    ObjectListingPage, copy_source_conditions, marker_continuation_token, next_marker_for,
    object_range_request,
};

use crate::s3::auth::map_authorize_error;
use crate::s3::checksum::{
    ApplyChecksums, ChecksumSelection, checksum_mode_enabled, encode_checksums,
    parse_completion_checksum, parse_upload_checksum, validate_delete_checksum,
    validate_part_count, validate_trailing_checksum, verify_trailer_stream,
};
use crate::s3::cors::{map_bucket_cors, parse_bucket_cors};
use crate::s3::error::{IntoS3Error, gate_context_error, routing_inputs_error};
use crate::s3::multipart_join::{
    CompletionFailure, CompletionRegistry, await_completion, completion_registry,
};
use crate::s3::scope::SubpathScope;
use crate::s3::server::DeleteObjectsBody;
use crate::s3::util::{
    checked_size, checksum_response_hashes, convert_input, declared_trailer_algorithm,
    map_checksum_algorithm, map_checksum_type, parse_checksum_hint, parse_checksum_type,
    parse_completed_part, parse_copy_source, parse_part_number, parse_source_range,
    parse_upload_id, parse_version_id, reject_sse, validate_object_key,
};
use aruna_compute::session::TouchedObject;
use aruna_core::NodeId;
use aruna_core::stream::BackendStream;
use aruna_core::structs::checksum::HASH_MD5;
use aruna_core::structs::{
    ArunaArn, AuthContext, BucketInfo, COMPLETION_DEADLINE_MS, Permission, RealmId, RoCrateLimits,
    SyncMode, SyncRelationship, SyncState, SyncStatusSnapshot, UserAccess, credential_job_id,
    object_permission_path,
};
use aruna_operations::auth::check_permissions::{
    CheckPermissionsConfig, CheckPermissionsOperation,
};
use aruna_operations::auth::request_authorization::authorize;
use aruna_operations::auth::request_policy::PolicyRequestExtras;
use aruna_operations::driver::{
    DriverContext, bucket_snapshot, drive, drive_until, gate_context, now_ms, routing_snapshot,
};
use aruna_operations::s3::abort_upload::{
    AbortMultipartUploadInput as AMUI, AbortMultipartUploadOperation,
};
use aruna_operations::s3::bucket_cors::{
    DeleteBucketCorsOperation, GetBucketCorsOperation, PutBucketCorsOperation,
};
use aruna_operations::s3::complete_upload::{
    CompleteMultipartUploadInput as CMUI, CompleteMultipartUploadOperation,
};
use aruna_operations::s3::copy_object::{CopyObjectInput as CopyObjectData, copy_object};
use aruna_operations::s3::copy_part::{
    UploadPartCopyInput as UploadPartCopyData, upload_part_copy,
};
use aruna_operations::s3::create_bucket::CreateBucketOperation;
use aruna_operations::s3::create_upload::{
    CreateMultipartUploadInput as CMPI, CreateMultipartUploadOperation,
};
use aruna_operations::s3::delete_bucket::DeleteBucketOperation;
use aruna_operations::s3::delete_object::{
    DeleteObjectError, DeleteObjectInput as DOI, DeleteObjectOperation,
};
use aruna_operations::s3::delete_objects::{
    DeleteObjectsEntry, DeleteObjectsInput as DOSI, delete_objects,
};
use aruna_operations::s3::get_attributes::{
    GetObjectAttributesInput as GOAI, GetObjectAttributesOperation,
};
use aruna_operations::s3::get_bucket::GetBucketInfoOperation;
use aruna_operations::s3::get_object::{GetObjectInput as GOI, get_object_info, get_object_routed};
use aruna_operations::s3::head_object::{HeadObjectInput as HOI, HeadObjectOperation};
use aruna_operations::s3::list_buckets::{ListBucketsInput as LBI, ListBucketsOperation};
use aruna_operations::s3::list_objects::{ListObjectsV2Input as LOV2I, ListObjectsV2Operation};
use aruna_operations::s3::list_parts::{ListPartsInput as LPI, ListPartsOperation};
use aruna_operations::s3::list_uploads::{
    ListMultipartUploadsInput as LMUI, ListMultipartUploadsOperation,
};
use aruna_operations::s3::list_versions::{
    ListObjectVersionsInput as LOVI, ListObjectVersionsItem, ListObjectVersionsOperation,
};
use aruna_operations::s3::put_object::{PutObjectConfig, PutObjectOperation};
use aruna_operations::s3::upload_part::{UploadPartInput as UPI, UploadPartOperation};
use aruna_operations::sync::mirror_repair::{
    SyncMirrorRepairIntent, kick_mirror_repair, stage_mirror_delete, stage_mirror_reconcile,
};
use aruna_operations::sync::sync_relationship::SyncRelationshipDirection;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use s3s::dto::{
    AbortMultipartUploadInput, AbortMultipartUploadOutput, Bucket, BucketVersioningStatus,
    Checksum, ChecksumType, CommonPrefix, CompleteMultipartUploadInput,
    CompleteMultipartUploadOutput, CopyObjectInput, CopyObjectOutput, CopyObjectResult,
    CopyPartResult, CreateBucketInput, CreateBucketOutput, CreateMultipartUploadInput,
    CreateMultipartUploadOutput, DeleteBucketCorsInput, DeleteBucketCorsOutput, DeleteBucketInput,
    DeleteBucketOutput, DeleteBucketReplicationInput, DeleteBucketReplicationOutput,
    DeleteMarkerEntry, DeleteObjectInput, DeleteObjectOutput, DeleteObjectsInput,
    DeleteObjectsOutput, DeletedObject, ETag, EncodingType, Error as S3DeleteError,
    GetBucketCorsInput, GetBucketCorsOutput, GetBucketLocationInput, GetBucketLocationOutput,
    GetBucketReplicationInput, GetBucketReplicationOutput, GetBucketVersioningInput,
    GetBucketVersioningOutput, GetObjectAttributesInput, GetObjectAttributesOutput,
    GetObjectAttributesParts, GetObjectInput, GetObjectOutput, HeadBucketInput, HeadBucketOutput,
    HeadObjectInput, HeadObjectOutput, Initiator, ListBucketsInput, ListBucketsOutput,
    ListMultipartUploadsInput, ListMultipartUploadsOutput, ListObjectVersionsInput,
    ListObjectVersionsOutput, ListObjectsInput, ListObjectsOutput, ListObjectsV2Input,
    ListObjectsV2Output, ListPartsInput, ListPartsOutput, MetadataDirective,
    MultipartUpload as S3MultipartUpload, ObjectAttributes, ObjectPart, ObjectVersion,
    ObjectVersionStorageClass, Owner, Part, PutBucketCorsInput, PutBucketCorsOutput,
    PutBucketReplicationInput, PutBucketReplicationOutput, PutBucketVersioningInput,
    PutBucketVersioningOutput, PutObjectInput, PutObjectOutput, StorageClass, StreamingBlob,
    UploadPartCopyInput, UploadPartCopyOutput, UploadPartInput, UploadPartOutput,
};
use s3s::{S3, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use std::fmt::Debug;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tracing::{Instrument, debug, error, warn};

const S3_URL_ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

/// On expiry the record reopens and the composed blob is rolled back. The
/// cleanup sweep waits out the same deadline, so it never aborts a live
/// completion.
const COMPLETION_DEADLINE: Duration = Duration::from_millis(COMPLETION_DEADLINE_MS);

#[derive(Clone)]
pub struct ArunaS3Service {
    state: Arc<DriverContext>,
    realm_id: RealmId,
    node_id: NodeId,
    rocrate_limits: RoCrateLimits,
    /// Detached CompleteMultipartUpload tasks, shared by every clone of the
    /// service so a retry on another connection joins the running completion.
    completions: Arc<CompletionRegistry>,
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
            rocrate_limits: RoCrateLimits::default(),
            completions: Arc::new(completion_registry()),
        }
    }

    pub fn with_rocrate_limits(mut self, limits: RoCrateLimits) -> Self {
        self.rocrate_limits = limits;
        self
    }

    /// Attributes one authorized request made with a session's own credential
    /// to its job. Every other credential is ignored.
    fn record_touch(&self, access_key: &str, bucket: &str, key: &str, operation: &str) {
        let Some(job_id) = credential_job_id(access_key) else {
            return;
        };
        let Some(session) = self
            .state
            .compute_handle
            .as_ref()
            .and_then(|registry| registry.sessions().get(&job_id.to_string()))
        else {
            return;
        };
        session.record_touched(TouchedObject {
            bucket: bucket.to_string(),
            key: key.to_string(),
            operation: operation.to_string(),
        });
    }
}

#[async_trait::async_trait]
impl S3 for ArunaS3Service {
    #[tracing::instrument(err, skip(self, req))]
    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        debug!(bucket = %req.input.bucket, "Received CREATE BUCKET Request");

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
                storage_routing: Vec::new(),
                placement_policies: Vec::new(),
                placement_policy_generation: 0,
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
        debug!(bucket = %req.input.bucket, "Received HEAD BUCKET Request");

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
    async fn get_bucket_location(
        &self,
        req: S3Request<GetBucketLocationInput>,
    ) -> S3Result<S3Response<GetBucketLocationOutput>> {
        debug!(bucket = %req.input.bucket, "Received GET BUCKET LOCATION Request");

        let _user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;

        drive(GetBucketInfoOperation::new(req.input.bucket), &self.state)
            .await
            .and_then(|result| result.transpose())
            .map_err(IntoS3Error::into_s3_error)?
            .ok_or_else(|| s3_error!(InternalError, "Failed to get bucket location"))?;

        // No region is configured for the node, so report the default location
        // constraint (an empty constraint denotes the us-east-1 default region).
        Ok(S3Response::new(GetBucketLocationOutput {
            location_constraint: None,
        }))
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
        debug!("Received LIST BUCKETS Request");

        let user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;
        let extras = req
            .extensions
            .get::<PolicyRequestExtras>()
            .cloned()
            .ok_or_else(|| {
                error!(error = "Missing policy context");
                s3_error!(InternalError, "Missing policy context")
            })?;
        let scope = req.extensions.get::<SubpathScope>().cloned();

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
                .can_access_bucket(&user_access, &bucket, &bucket_info, &extras, scope.as_ref())
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
        let config = parse_bucket_cors(req.input.cors_configuration.clone())?;

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

        Ok(S3Response::new(map_bucket_cors(config)))
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
        debug!(
            bucket = %req.input.bucket,
            prefix = ?req.input.prefix,
            max_keys = ?req.input.max_keys,
            "Received LIST OBJECTS V2 Request"
        );

        let user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;
        let bucket_info = req.extensions.get::<BucketInfo>().cloned();
        let requested_continuation_token = req.input.continuation_token.clone();
        let continuation_token = Self::decode_list_token(requested_continuation_token.as_deref())?;
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
        let scope = req.extensions.get::<SubpathScope>().cloned();
        if let Some(scope) = &scope {
            let requested = object_permission_path(
                self.realm_id,
                group_id,
                self.node_id,
                &bucket,
                prefix.as_deref().unwrap_or_default(),
            );
            if !scope.overlaps(&requested) {
                return Err(s3_error!(AccessDenied, "Permission denied"));
            }
        }

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

        let page = self
            .run_object_listing(
                LOV2I {
                    bucket: bucket.clone(),
                    group_id,
                    continuation_token,
                    max_keys: Some(max_keys),
                    prefix: prefix.clone(),
                    delimiter: delimiter.clone(),
                    start_after: start_after.clone(),
                },
                owner,
                url_encoded,
                scope.as_ref(),
            )
            .await?;

        let ObjectListingPage {
            contents,
            common_prefixes,
            continuation_token: result_token,
        } = page;
        let key_count = contents.len() + common_prefixes.len();
        let next_continuation_token = Self::encode_list_token(result_token.as_ref())?;
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
    async fn list_objects(
        &self,
        req: S3Request<ListObjectsInput>,
    ) -> S3Result<S3Response<ListObjectsOutput>> {
        debug!(
            bucket = %req.input.bucket,
            prefix = ?req.input.prefix,
            max_keys = ?req.input.max_keys,
            "Received LIST OBJECTS Request"
        );

        let user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;
        let bucket_info = req.extensions.get::<BucketInfo>().cloned();
        let max_keys = match req.input.max_keys {
            None => ListObjectsV2Operation::DEFAULT_MAX_KEYS,
            Some(max_keys) => usize::try_from(max_keys)
                .map_err(|_| s3_error!(InvalidArgument, "max-keys must be non-negative"))?
                .min(ListObjectsV2Operation::DEFAULT_MAX_KEYS),
        };
        let bucket = req.input.bucket.clone();
        let prefix = req.input.prefix.clone();
        let delimiter = req.input.delimiter.clone();
        let marker = req.input.marker.clone();

        let group_id = bucket_info
            .as_ref()
            .map(|bucket_info| bucket_info.group_id)
            .unwrap_or(user_access.group_id);
        let scope = req.extensions.get::<SubpathScope>().cloned();
        if let Some(scope) = &scope {
            let requested = object_permission_path(
                self.realm_id,
                group_id,
                self.node_id,
                &bucket,
                prefix.as_deref().unwrap_or_default(),
            );
            if !scope.overlaps(&requested) {
                return Err(s3_error!(AccessDenied, "Permission denied"));
            }
        }

        let continuation_token = marker_continuation_token(
            &bucket,
            marker.as_deref(),
            prefix.as_deref(),
            delimiter.as_deref(),
        )?;
        let start_after = continuation_token
            .is_none()
            .then(|| marker.clone())
            .flatten();

        let owner = Some(Owner {
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

        let page = self
            .run_object_listing(
                LOV2I {
                    bucket: bucket.clone(),
                    group_id,
                    continuation_token,
                    max_keys: Some(max_keys),
                    prefix: prefix.clone(),
                    delimiter: delimiter.clone(),
                    start_after,
                },
                owner,
                url_encoded,
                scope.as_ref(),
            )
            .await?;

        let ObjectListingPage {
            contents,
            common_prefixes,
            continuation_token: result_token,
        } = page;
        let is_truncated = result_token.is_some();
        let next_marker = next_marker_for(
            delimiter.as_deref(),
            result_token.as_ref(),
            contents.is_empty(),
        )
        .map(&encode_field);

        Ok(S3Response::new(ListObjectsOutput {
            name: Some(bucket),
            prefix: prefix.map(&encode_field),
            marker: marker.map(&encode_field),
            max_keys: Some(i32::try_from(max_keys).unwrap_or(i32::MAX)),
            is_truncated: Some(is_truncated),
            next_marker,
            contents: Some(contents),
            common_prefixes: Some(common_prefixes),
            delimiter: delimiter.map(&encode_field),
            encoding_type: req.input.encoding_type,
            ..Default::default()
        }))
    }

    #[tracing::instrument(err, skip(self, req))]
    #[allow(clippy::blocks_in_conditions)]
    async fn put_object(
        &self,
        mut req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        debug!(
            bucket = %req.input.bucket,
            key = %req.input.key,
            content_length = ?req.input.content_length,
            "Received PUT Request"
        );

        // Extract access check result
        let user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;
        reject_sse(
            req.input.server_side_encryption.is_some()
                || req.input.ssekms_key_id.is_some()
                || req.input.ssekms_encryption_context.is_some()
                || req.input.bucket_key_enabled.is_some()
                || req.input.sse_customer_algorithm.is_some()
                || req.input.sse_customer_key.is_some()
                || req.input.sse_customer_key_md5.is_some(),
        )?;
        validate_object_key(&req.input.key)?;
        let bucket_info = req.extensions.get::<BucketInfo>().cloned();
        let trailer_algorithm = declared_trailer_algorithm(
            &req.headers,
            req.trailing_headers.is_some(),
            req.input.checksum_algorithm.as_ref(),
        )?;
        let checksum_request = parse_upload_checksum(&req.headers, trailer_algorithm)?;
        let trailing_headers = req.trailing_headers.clone();
        let replication_auth = AuthContext {
            user_id: user_access.user_identity,
            realm_id: user_access.user_identity.realm_id,
            path_restrictions: user_access.path_restrictions.clone(),
            session: None,
        };
        let replication_bucket = req.input.bucket.clone();
        let replication_key = req.input.key.clone();

        let group_id = bucket_info
            .as_ref()
            .map(|bucket_info| bucket_info.group_id)
            .unwrap_or(user_access.group_id);
        let quota_ceiling = self.resolve_quota_ceiling(group_id).await?;
        if let (Some(algorithm), Some(headers)) = (trailer_algorithm, trailing_headers.clone())
            && let Some(body) = req.input.body.take()
        {
            req.input.body = Some(verify_trailer_stream(
                body,
                algorithm,
                checksum_request.checksum_type.as_str() == ChecksumType::COMPOSITE,
                move || headers.read(Clone::clone),
            ));
        }
        let metadata = object_metadata(
            req.input.metadata.clone().unwrap_or_default(),
            req.input.content_type.as_deref(),
        );
        let routing = match bucket_info.as_ref() {
            Some(info) => bucket_snapshot(&self.state, info).await,
            None => routing_snapshot(&self.state, group_id, &replication_bucket).await,
        }
        .map_err(routing_inputs_error)?;
        let input = convert_input(req.input)?;
        let gate = gate_context(&self.state, self.realm_id, now_ms())
            .await
            .map_err(gate_context_error)?;
        let mut operation = PutObjectOperation::new(PutObjectConfig {
            user_id: user_access.user_identity,
            group_id,
            realm_id: self.realm_id,
            node_id: self.node_id,
            request: input,
            expected_checksums: checksum_request.expected.clone(),
            checksum_type: Some(checksum_request.checksum_type.as_str().to_string()),
            version_source: None,
            preassigned_version_id: None,
            exists: false,
            quota_ceiling,
            routing,
        })
        .with_rocrate_limits(self.rocrate_limits.clone())
        .with_metadata(metadata)
        .with_restrictions(replication_auth.path_restrictions.clone());
        if let Some(gate) = gate {
            operation = operation.with_gate(gate);
        }

        let result = drive(operation, &self.state)
            .await
            .and_then(|result| result.transpose())
            .map_err(IntoS3Error::into_s3_error)?
            .ok_or_else(|| s3_error!(InternalError, "Failed to process PUT request"))?;
        validate_trailing_checksum(
            trailer_algorithm,
            &checksum_request.checksum_type,
            trailing_headers.as_ref(),
            &result.location.hashes,
        )?;
        self.record_touch(
            &user_access.access_key,
            &replication_bucket,
            &replication_key,
            "write",
        );

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
    async fn copy_object(
        &self,
        req: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        debug!(
            bucket = %req.input.bucket,
            key = %req.input.key,
            "Received COPY OBJECT Request"
        );

        let user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;
        reject_sse(
            req.input.server_side_encryption.is_some()
                || req.input.ssekms_key_id.is_some()
                || req.input.ssekms_encryption_context.is_some()
                || req.input.bucket_key_enabled.is_some()
                || req.input.sse_customer_algorithm.is_some()
                || req.input.sse_customer_key.is_some()
                || req.input.sse_customer_key_md5.is_some()
                || req.input.copy_source_sse_customer_algorithm.is_some()
                || req.input.copy_source_sse_customer_key.is_some()
                || req.input.copy_source_sse_customer_key_md5.is_some(),
        )?;
        validate_object_key(&req.input.key)?;
        let dest_bucket_info = req.extensions.get::<BucketInfo>().cloned();

        let (source_bucket, source_key, source_version_id) =
            parse_copy_source(&req.input.copy_source)?;

        // The auth layer only authorized the destination path; authorize the source here.
        let source_bucket_info = drive(
            GetBucketInfoOperation::new(source_bucket.clone()),
            &self.state,
        )
        .await
        .and_then(|result| result.transpose())
        .map_err(IntoS3Error::into_s3_error)?
        .ok_or_else(|| s3_error!(NoSuchBucket, "The specified bucket does not exist."))?;

        let source_auth_context = if source_bucket_info.group_id == user_access.group_id {
            AuthContext {
                user_id: user_access.user_identity,
                realm_id: user_access.user_identity.realm_id,
                path_restrictions: user_access.path_restrictions.clone(),
                session: None,
            }
        } else {
            AuthContext::anonymous(self.realm_id)
        };
        let source_extras = req
            .extensions
            .get::<PolicyRequestExtras>()
            .cloned()
            .ok_or_else(|| {
                error!(error = "Missing policy context");
                s3_error!(InternalError, "Missing policy context")
            })?;
        authorize(
            &self.state,
            self.realm_id,
            &source_auth_context,
            &object_permission_path(
                self.realm_id,
                source_bucket_info.group_id,
                self.node_id,
                &source_bucket,
                &source_key,
            ),
            &Permission::READ,
            source_extras,
        )
        .await
        .map_err(map_authorize_error)?;

        let dest_bucket = req.input.bucket.clone();
        let dest_key = req.input.key.clone();

        let metadata_replace = req
            .input
            .metadata_directive
            .as_ref()
            .map(MetadataDirective::as_str)
            == Some(MetadataDirective::REPLACE);
        if source_bucket == dest_bucket
            && source_key == dest_key
            && source_version_id.is_none()
            && !metadata_replace
        {
            return Err(s3_error!(
                InvalidRequest,
                "This copy request is illegal because it is trying to copy an object to itself without changing the object's metadata, storage class, website redirect location or encryption attributes."
            ));
        }

        let dest_group_id = dest_bucket_info
            .as_ref()
            .map(|bucket_info| bucket_info.group_id)
            .unwrap_or(user_access.group_id);
        let quota_ceiling = self.resolve_quota_ceiling(dest_group_id).await?;
        let replication_auth = AuthContext {
            user_id: user_access.user_identity,
            realm_id: user_access.user_identity.realm_id,
            path_restrictions: user_access.path_restrictions.clone(),
            session: None,
        };
        let conditions = copy_source_conditions(
            req.input.copy_source_if_match.as_ref(),
            req.input.copy_source_if_none_match.as_ref(),
            req.input.copy_source_if_modified_since.as_ref(),
            req.input.copy_source_if_unmodified_since.as_ref(),
        )?;

        let result = copy_object(
            &self.state,
            CopyObjectData {
                source_bucket,
                source_key,
                source_version_id,
                source_group_id: source_bucket_info.group_id,
                dest_bucket: dest_bucket.clone(),
                dest_key: dest_key.clone(),
                user_id: user_access.user_identity,
                group_id: dest_group_id,
                realm_id: self.realm_id,
                node_id: self.node_id,
                quota_ceiling,
                conditions,
                metadata: metadata_replace.then(|| {
                    object_metadata(
                        req.input.metadata.clone().unwrap_or_default(),
                        req.input.content_type.as_deref(),
                    )
                }),
                source_auth_context,
                restrictions: replication_auth.path_restrictions.clone(),
            },
        )
        .await
        .map_err(IntoS3Error::into_s3_error)?;

        self.queue_live_replication(
            replication_auth,
            dest_bucket,
            dest_key,
            result.version_id,
            false,
        )
        .await;

        let mut copy_object_result = CopyObjectResult {
            e_tag: result
                .location
                .hashes
                .get(HASH_MD5)
                .map(|value| ETag::Strong(hex::encode(value))),
            last_modified: Some(result.created_at.into()),
            ..Default::default()
        };
        copy_object_result.apply_checksums(encode_checksums(
            &result.location.hashes,
            ChecksumSelection::AllStored,
            ChecksumType::from_static(ChecksumType::FULL_OBJECT),
            None,
        ));

        Ok(S3Response::new(CopyObjectOutput {
            copy_object_result: Some(copy_object_result),
            version_id: Some(result.version_id.to_string()),
            copy_source_version_id: result
                .source_version_id
                .map(|version_id| version_id.to_string()),
            ..Default::default()
        }))
    }

    #[tracing::instrument(err, skip(self, req))]
    #[allow(clippy::blocks_in_conditions)]
    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        debug!(
            bucket = %req.input.bucket,
            key = %req.input.key,
            "Received CREATE MULTIPART Request"
        );

        let user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;
        reject_sse(
            req.input.server_side_encryption.is_some()
                || req.input.ssekms_key_id.is_some()
                || req.input.ssekms_encryption_context.is_some()
                || req.input.bucket_key_enabled.is_some()
                || req.input.sse_customer_algorithm.is_some()
                || req.input.sse_customer_key.is_some()
                || req.input.sse_customer_key_md5.is_some(),
        )?;
        validate_object_key(&req.input.key)?;
        let bucket_info = req.extensions.get::<BucketInfo>().cloned();
        let checksum_hint = parse_checksum_hint(&req.input)?;

        let group_id = bucket_info
            .as_ref()
            .map(|bucket_info| bucket_info.group_id)
            .unwrap_or(user_access.group_id);
        let routing = match bucket_info.as_ref() {
            Some(info) => bucket_snapshot(&self.state, info).await,
            None => routing_snapshot(&self.state, group_id, &req.input.bucket).await,
        }
        .map_err(routing_inputs_error)?;
        let gate = gate_context(&self.state, self.realm_id, now_ms())
            .await
            .map_err(gate_context_error)?;
        let mut operation = CreateMultipartUploadOperation::new(CMPI {
            bucket: req.input.bucket.clone(),
            key: req.input.key.clone(),
            group_id,
            created_by: user_access.user_identity,
            checksum_hint: checksum_hint.clone(),
            routing,
        })
        .with_metadata(object_metadata(
            req.input.metadata.clone().unwrap_or_default(),
            req.input.content_type.as_deref(),
        ));
        if let Some(gate) = gate {
            operation = operation.with_gate(gate);
        }

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
            checksum_type: checksum_hint.map(|hint| map_checksum_type(hint.checksum_type)),
            ..Default::default()
        }))
    }

    #[tracing::instrument(err, skip(self, req))]
    #[allow(clippy::blocks_in_conditions)]
    async fn upload_part(
        &self,
        mut req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        debug!(
            bucket = %req.input.bucket,
            key = %req.input.key,
            part_number = req.input.part_number,
            content_length = ?req.input.content_length,
            "Received UPLOAD PART Request"
        );

        let user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;
        reject_sse(
            req.input.sse_customer_algorithm.is_some()
                || req.input.sse_customer_key.is_some()
                || req.input.sse_customer_key_md5.is_some(),
        )?;
        validate_object_key(&req.input.key)?;
        let trailer_algorithm = declared_trailer_algorithm(
            &req.headers,
            req.trailing_headers.is_some(),
            req.input.checksum_algorithm.as_ref(),
        )?;
        let checksum_request = parse_upload_checksum(&req.headers, trailer_algorithm)?;
        let trailing_headers = req.trailing_headers.clone();
        let upload_id = parse_upload_id(&req.input.upload_id)?;
        let body = req
            .input
            .body
            .take()
            .map(|body| match (trailer_algorithm, trailing_headers.clone()) {
                (Some(algorithm), Some(headers)) => verify_trailer_stream(
                    body,
                    algorithm,
                    checksum_request.checksum_type.as_str() == ChecksumType::COMPOSITE,
                    move || headers.read(Clone::clone),
                ),
                _ => body,
            })
            .map(BackendStream::new_from_boxed)
            .ok_or_else(|| s3_error!(InvalidRequest, "Missing body"))?;

        let operation = UploadPartOperation::new(UPI {
            bucket: req.input.bucket,
            key: req.input.key,
            upload_id,
            part_number: parse_part_number(req.input.part_number, S3ErrorCode::InvalidArgument)?,
            content_length: req.input.content_length.map(checked_size).transpose()?,
            body: Some(body),
            created_by: user_access.user_identity,
            compressed: false,
            encrypted: false,
            expected_checksums: checksum_request.expected.clone(),
        });

        let result = drive(operation, &self.state)
            .await
            .and_then(|result| result.transpose())
            .map_err(IntoS3Error::into_s3_error)?
            .ok_or_else(|| s3_error!(InternalError, "Failed to upload part"))?;
        validate_trailing_checksum(
            trailer_algorithm,
            &checksum_request.checksum_type,
            trailing_headers.as_ref(),
            &result.location.hashes,
        )?;

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
            None,
        ));

        Ok(S3Response::new(output))
    }

    #[tracing::instrument(err, skip(self, req))]
    #[allow(clippy::blocks_in_conditions)]
    async fn upload_part_copy(
        &self,
        req: S3Request<UploadPartCopyInput>,
    ) -> S3Result<S3Response<UploadPartCopyOutput>> {
        debug!(
            bucket = %req.input.bucket,
            key = %req.input.key,
            part_number = req.input.part_number,
            "Received UPLOAD PART COPY Request"
        );

        let user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;
        reject_sse(
            req.input.sse_customer_algorithm.is_some()
                || req.input.sse_customer_key.is_some()
                || req.input.sse_customer_key_md5.is_some()
                || req.input.copy_source_sse_customer_algorithm.is_some()
                || req.input.copy_source_sse_customer_key.is_some()
                || req.input.copy_source_sse_customer_key_md5.is_some(),
        )?;
        validate_object_key(&req.input.key)?;

        let upload_id = parse_upload_id(&req.input.upload_id)?;
        let part_number = parse_part_number(req.input.part_number, S3ErrorCode::InvalidArgument)?;
        let range = parse_source_range(req.input.copy_source_range.as_deref())?;
        let conditions = copy_source_conditions(
            req.input.copy_source_if_match.as_ref(),
            req.input.copy_source_if_none_match.as_ref(),
            req.input.copy_source_if_modified_since.as_ref(),
            req.input.copy_source_if_unmodified_since.as_ref(),
        )?;

        let (source_bucket, source_key, source_version_id) =
            parse_copy_source(&req.input.copy_source)?;

        // The auth layer only authorized the destination path; authorize the source here.
        let source_bucket_info = drive(
            GetBucketInfoOperation::new(source_bucket.clone()),
            &self.state,
        )
        .await
        .and_then(|result| result.transpose())
        .map_err(IntoS3Error::into_s3_error)?
        .ok_or_else(|| s3_error!(NoSuchBucket, "The specified bucket does not exist."))?;

        let source_auth_context = if source_bucket_info.group_id == user_access.group_id {
            AuthContext {
                user_id: user_access.user_identity,
                realm_id: user_access.user_identity.realm_id,
                path_restrictions: user_access.path_restrictions.clone(),
                session: None,
            }
        } else {
            AuthContext::anonymous(self.realm_id)
        };
        let source_extras = req
            .extensions
            .get::<PolicyRequestExtras>()
            .cloned()
            .ok_or_else(|| {
                error!(error = "Missing policy context");
                s3_error!(InternalError, "Missing policy context")
            })?;
        authorize(
            &self.state,
            self.realm_id,
            &source_auth_context,
            &object_permission_path(
                self.realm_id,
                source_bucket_info.group_id,
                self.node_id,
                &source_bucket,
                &source_key,
            ),
            &Permission::READ,
            source_extras,
        )
        .await
        .map_err(map_authorize_error)?;

        let result = upload_part_copy(
            &self.state,
            UploadPartCopyData {
                source_bucket,
                source_key,
                source_version_id,
                source_group_id: source_bucket_info.group_id,
                dest_bucket: req.input.bucket,
                dest_key: req.input.key,
                upload_id,
                part_number,
                range,
                user_id: user_access.user_identity,
                node_id: self.node_id,
                conditions,
                source_auth_context,
            },
        )
        .await
        .map_err(IntoS3Error::into_s3_error)?;

        let mut copy_part_result = CopyPartResult {
            e_tag: result
                .part_location
                .hashes
                .get(HASH_MD5)
                .map(|value| ETag::Strong(hex::encode(value))),
            last_modified: Some(result.part_location.created_at.into()),
            ..Default::default()
        };
        copy_part_result.apply_checksums(encode_checksums(
            &result.part_location.hashes,
            ChecksumSelection::AllStored,
            ChecksumType::from_static(ChecksumType::FULL_OBJECT),
            None,
        ));

        Ok(S3Response::new(UploadPartCopyOutput {
            copy_part_result: Some(copy_part_result),
            copy_source_version_id: result
                .source_version_id
                .map(|version_id| version_id.to_string()),
            ..Default::default()
        }))
    }

    #[tracing::instrument(err, skip(self, req))]
    #[allow(clippy::blocks_in_conditions)]
    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        debug!(
            bucket = %req.input.bucket,
            key = %req.input.key,
            "Received COMPLETE MULTIPART Request"
        );

        let user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;
        reject_sse(
            req.input.sse_customer_algorithm.is_some()
                || req.input.sse_customer_key.is_some()
                || req.input.sse_customer_key_md5.is_some(),
        )?;
        validate_object_key(&req.input.key)?;
        let bucket_info = req.extensions.get::<BucketInfo>().cloned();
        let group_id = bucket_info
            .as_ref()
            .map(|bucket_info| bucket_info.group_id)
            .unwrap_or(user_access.group_id);
        let quota_ceiling = self.resolve_quota_ceiling(group_id).await?;
        let checksum_request = parse_completion_checksum(&req.headers)?;
        let upload_id = parse_upload_id(&req.input.upload_id)?;
        let replication_auth = AuthContext {
            user_id: user_access.user_identity,
            realm_id: user_access.user_identity.realm_id,
            path_restrictions: user_access.path_restrictions.clone(),
            session: None,
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
        validate_part_count(&checksum_request, completed_parts.len())?;

        let gate = gate_context(&self.state, self.realm_id, now_ms())
            .await
            .map_err(gate_context_error)?;
        let mut operation = CompleteMultipartUploadOperation::new(CMUI {
            bucket: req.input.bucket.clone(),
            key: req.input.key.clone(),
            upload_id,
            realm_id: self.realm_id,
            node_id: self.node_id,
            completed_parts,
            expected_checksums: checksum_request.expected.clone(),
            checksum_algorithm: checksum_request.response_algorithm,
            checksum_type: parse_checksum_type(&checksum_request.checksum_type),
            checksum_type_explicit: checksum_request.checksum_type_declared,
            object_size: req.input.mpu_object_size.map(checked_size).transpose()?,
            created_by: user_access.user_identity,
            quota_ceiling,
            now_ms: now_ms(),
        })
        .with_rocrate_limits(self.rocrate_limits.clone())
        .with_restrictions(replication_auth.path_restrictions.clone());
        if let Some(gate) = gate {
            operation = operation.with_gate(gate);
        }

        // The completion outlives its request: a dropped connection must not
        // cancel it, and a retry joins it instead of racing a second one.
        let bucket = req.input.bucket.clone();
        let key = req.input.key.clone();
        let service = self.clone();
        let deadline = tokio::time::Instant::now() + COMPLETION_DEADLINE;
        let work = async move {
            let outcome = match drive_until(operation, &service.state, deadline)
                .await
                .and_then(|result| result.transpose())
            {
                Ok(Some(result)) => {
                    service
                        .complete_put(
                            replication_auth,
                            group_id,
                            bucket,
                            key,
                            result.version_id,
                            result.location.blob_size,
                        )
                        .await;
                    Ok(result)
                }
                Ok(None) => Err(CompletionFailure::new(&s3_error!(
                    InternalError,
                    "Failed to complete multipart upload"
                ))),
                Err(error) => Err(CompletionFailure::new(&error.into_s3_error())),
            };
            Arc::new(outcome)
        };
        let joined = self.completions.join(
            (req.input.bucket.clone(), req.input.key.clone(), upload_id),
            work.instrument(tracing::Span::current()),
        );

        match await_completion(joined).await.as_ref() {
            Ok(result) => Ok(self.complete_upload_response(
                req.input.bucket,
                req.input.key,
                &checksum_request,
                result.clone(),
            )),
            Err(failure) => Err(failure.to_s3_error()),
        }
    }

    #[tracing::instrument(err, skip(self, req))]
    #[allow(clippy::blocks_in_conditions)]
    async fn abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        debug!(
            bucket = %req.input.bucket,
            key = %req.input.key,
            "Received ABORT MULTIPART Request"
        );

        let _user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;
        let upload_id = parse_upload_id(&req.input.upload_id)?;
        let operation = AbortMultipartUploadOperation::new(AMUI {
            bucket: req.input.bucket,
            key: req.input.key,
            upload_id,
            now_ms: now_ms(),
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
        debug!(
            bucket = %req.input.bucket,
            key = %req.input.key,
            "Received GET Request"
        );

        // Extract access check result
        let user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;
        reject_sse(
            req.input.sse_customer_algorithm.is_some()
                || req.input.sse_customer_key.is_some()
                || req.input.sse_customer_key_md5.is_some(),
        )?;
        let bucket_info = req.extensions.get::<BucketInfo>().cloned();
        let requested_range = req.input.range;
        let version_id = parse_version_id(req.input.version_id)?;
        let bucket = req.input.bucket;
        let key = req.input.key;
        let response_bucket = bucket.clone();
        let response_key = key.clone();

        let range_request = requested_range.map(object_range_request);

        let input = GOI {
            bucket,
            key,
            version_id,
            range: range_request,
            group_id: bucket_info
                .as_ref()
                .map(|bucket_info| bucket_info.group_id)
                .unwrap_or(user_access.group_id),
            user_identity: user_access.user_identity,
            node_id: self.node_id,
        };

        // A device holds version records without their bytes, so a local miss
        // continues against the realm's holders instead of failing here.
        let result = get_object_routed(&self.state, input, user_access.path_restrictions.clone())
            .await
            .and_then(|result| result.transpose())
            .map_err(IntoS3Error::into_s3_error)?
            .ok_or_else(|| s3_error!(InternalError, "Failed to process GET request"))?;
        self.record_touch(
            &user_access.access_key,
            &response_bucket,
            &response_key,
            "read",
        );

        let version_id = result.version_id;
        let resolved_range = result.resolved_range.clone();
        let reference_refresh = reference_metadata_refresh(response_bucket, response_key, &result);
        let response_fields = self.build_response_fields(
            result.location.as_ref(),
            Some(&result.info),
            Some(&result.metadata),
            result.source_metadata.as_ref(),
            result.last_refresh,
            None,
        );
        let blob = if let Some(refresh) = reference_refresh {
            attach_reference_refresh(result.blob, self.state.clone(), refresh)
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
        if checksum_mode_enabled(&req.headers) {
            output.apply_checksums(encode_checksums(
                checksum_response_hashes(
                    result.info.checksum_type,
                    &result.info.hashes,
                    &result.info.composite_hashes,
                ),
                ChecksumSelection::AllStored,
                map_checksum_type(result.info.checksum_type),
                result.info.part_count,
            ));
        }

        Ok(if resolved_range.is_some() {
            S3Response::with_status(output, http::StatusCode::PARTIAL_CONTENT)
        } else {
            S3Response::new(output)
        })
    }

    #[tracing::instrument(err, skip(self, req))]
    #[allow(clippy::blocks_in_conditions)]
    async fn get_object_attributes(
        &self,
        req: S3Request<GetObjectAttributesInput>,
    ) -> S3Result<S3Response<GetObjectAttributesOutput>> {
        debug!(
            bucket = %req.input.bucket,
            key = %req.input.key,
            "Received GET OBJECT ATTRIBUTES Request"
        );

        let user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;
        reject_sse(
            req.input.sse_customer_algorithm.is_some()
                || req.input.sse_customer_key.is_some()
                || req.input.sse_customer_key_md5.is_some(),
        )?;

        let mut want_etag = false;
        let mut want_checksum = false;
        let mut want_object_parts = false;
        let mut want_object_size = false;
        let mut want_storage_class = false;
        for attribute in req.input.object_attributes.iter() {
            match attribute.as_str() {
                ObjectAttributes::ETAG => want_etag = true,
                ObjectAttributes::CHECKSUM => want_checksum = true,
                ObjectAttributes::OBJECT_PARTS => want_object_parts = true,
                ObjectAttributes::OBJECT_SIZE => want_object_size = true,
                ObjectAttributes::STORAGE_CLASS => want_storage_class = true,
                _ => {}
            }
        }
        if !(want_etag
            || want_checksum
            || want_object_parts
            || want_object_size
            || want_storage_class)
        {
            return Err(s3_error!(
                InvalidArgument,
                "At least one object attribute must be specified"
            ));
        }

        let requested_part_number_marker = req.input.part_number_marker;
        let part_number_marker = match requested_part_number_marker {
            None => None,
            Some(marker) if marker < 0 => {
                return Err(s3_error!(InvalidArgument, "Invalid part-number-marker"));
            }
            Some(marker) => Some(u16::try_from(marker).unwrap_or(u16::MAX)),
        };
        let max_parts = match req.input.max_parts {
            None => ListPartsOperation::DEFAULT_MAX_PARTS,
            Some(max_parts) => usize::try_from(max_parts)
                .map_err(|_| s3_error!(InvalidArgument, "max-parts must be non-negative"))?
                .min(ListPartsOperation::DEFAULT_MAX_PARTS),
        };
        let version_id = parse_version_id(req.input.version_id)?;
        let bucket = req.input.bucket.clone();
        let key = req.input.key.clone();
        let group_id = req
            .extensions
            .get::<BucketInfo>()
            .map(|info| info.group_id)
            .unwrap_or(user_access.group_id);

        let result = drive(
            GetObjectAttributesOperation::new(GOAI {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id,
                include_parts: want_object_parts,
            }),
            &self.state,
        )
        .await
        .and_then(|result| result.transpose())
        .map_err(IntoS3Error::into_s3_error)?
        .ok_or_else(|| s3_error!(InternalError, "Failed to get object attributes"))?;

        let remote_info = if result.location.is_none() {
            Some(
                get_object_info(
                    &self.state,
                    GOI {
                        bucket,
                        key,
                        version_id,
                        range: None,
                        group_id,
                        user_identity: user_access.user_identity,
                        node_id: self.node_id,
                    },
                    user_access.path_restrictions.clone(),
                )
                .await
                .map_err(IntoS3Error::into_s3_error)?,
            )
        } else {
            None
        };

        let response_fields = self.build_response_fields(
            result.location.as_ref(),
            remote_info.as_ref(),
            None,
            result.source_metadata.as_ref(),
            None,
            result.version_created_at,
        );

        let composite_hashes = result
            .summary
            .as_ref()
            .map(|summary| summary.composite_hashes.clone())
            .unwrap_or_default();
        let checksum = if want_checksum {
            remote_info
                .as_ref()
                .map(|info| {
                    let encoded = encode_checksums(
                        checksum_response_hashes(
                            info.checksum_type,
                            &info.hashes,
                            &info.composite_hashes,
                        ),
                        ChecksumSelection::AllStored,
                        map_checksum_type(info.checksum_type),
                        info.part_count,
                    );
                    Checksum {
                        checksum_crc32: encoded.checksum_crc32,
                        checksum_crc32c: encoded.checksum_crc32c,
                        checksum_crc64nvme: encoded.checksum_crc64nvme,
                        checksum_md5: None,
                        checksum_sha1: encoded.checksum_sha1,
                        checksum_sha256: encoded.checksum_sha256,
                        checksum_sha512: None,
                        checksum_type: encoded.checksum_type,
                        checksum_xxhash128: None,
                        checksum_xxhash3: None,
                        checksum_xxhash64: None,
                    }
                })
                .or_else(|| {
                    result.location.as_ref().map(|location| {
                        let encoded = encode_checksums(
                            checksum_response_hashes(
                                result.checksum_type,
                                &location.hashes,
                                &composite_hashes,
                            ),
                            ChecksumSelection::AllStored,
                            map_checksum_type(result.checksum_type),
                            result.summary.as_ref().map(|summary| summary.part_count),
                        );
                        Checksum {
                            checksum_crc32: encoded.checksum_crc32,
                            checksum_crc32c: encoded.checksum_crc32c,
                            checksum_crc64nvme: encoded.checksum_crc64nvme,
                            checksum_md5: None,
                            checksum_sha1: encoded.checksum_sha1,
                            checksum_sha256: encoded.checksum_sha256,
                            checksum_sha512: None,
                            checksum_type: encoded.checksum_type,
                            checksum_xxhash128: None,
                            checksum_xxhash3: None,
                            checksum_xxhash64: None,
                        }
                    })
                })
        } else {
            None
        };

        let object_parts = if want_object_parts {
            result.summary.as_ref().map(|summary| {
                let mut parts: Vec<&aruna_core::structs::MultipartObjectPart> =
                    result.parts.iter().collect();
                if let Some(marker) = part_number_marker {
                    parts.retain(|part| part.part_number > marker);
                }
                let is_truncated = parts.len() > max_parts;
                parts.truncate(max_parts);
                // With max_parts=0 the truncation empties `parts`, so fall back to
                // the marker preceding the first unreturned part (request marker/0).
                let next_part_number_marker = is_truncated.then(|| {
                    parts
                        .last()
                        .map(|part| part.part_number)
                        .unwrap_or(part_number_marker.unwrap_or(0))
                });
                let object_part_list: Vec<ObjectPart> = parts
                    .into_iter()
                    .map(|part| {
                        let checksums = encode_checksums(
                            &part.hashes,
                            ChecksumSelection::AllStored,
                            ChecksumType::from_static(ChecksumType::FULL_OBJECT),
                            None,
                        );
                        ObjectPart {
                            part_number: Some(i32::from(part.part_number)),
                            size: Some(part.size as i64),
                            checksum_crc32: checksums.checksum_crc32,
                            checksum_crc32c: checksums.checksum_crc32c,
                            checksum_crc64nvme: checksums.checksum_crc64nvme,
                            checksum_md5: None,
                            checksum_sha1: checksums.checksum_sha1,
                            checksum_sha256: checksums.checksum_sha256,
                            checksum_sha512: None,
                            checksum_xxhash128: None,
                            checksum_xxhash3: None,
                            checksum_xxhash64: None,
                        }
                    })
                    .collect();
                GetObjectAttributesParts {
                    total_parts_count: Some(i32::try_from(summary.part_count).unwrap_or(i32::MAX)),
                    is_truncated: Some(is_truncated),
                    max_parts: Some(i32::try_from(max_parts).unwrap_or(i32::MAX)),
                    part_number_marker: requested_part_number_marker,
                    next_part_number_marker: next_part_number_marker.map(i32::from),
                    parts: Some(object_part_list),
                }
            })
        } else {
            None
        };

        let output = GetObjectAttributesOutput {
            e_tag: want_etag.then(|| response_fields.e_tag.clone()).flatten(),
            last_modified: response_fields.last_modified,
            object_size: want_object_size
                .then_some(response_fields.content_length)
                .flatten(),
            storage_class: want_storage_class
                .then(|| StorageClass::from_static(StorageClass::STANDARD)),
            version_id: result.version_id.map(|version_id| version_id.to_string()),
            checksum,
            object_parts,
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }

    #[tracing::instrument(err, skip(self, req))]
    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        debug!(
            bucket = %req.input.bucket,
            key = %req.input.key,
            "Received HEAD Request"
        );

        let user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;
        reject_sse(
            req.input.sse_customer_algorithm.is_some()
                || req.input.sse_customer_key.is_some()
                || req.input.sse_customer_key_md5.is_some(),
        )?;
        let version_id = parse_version_id(req.input.version_id)?;
        let bucket = req.input.bucket.clone();
        let key = req.input.key.clone();
        let group_id = req
            .extensions
            .get::<BucketInfo>()
            .map(|info| info.group_id)
            .unwrap_or(user_access.group_id);
        let operation = HeadObjectOperation::new(HOI {
            bucket: bucket.clone(),
            key: key.clone(),
            version_id,
        });

        let result = drive(operation, &self.state)
            .await
            .and_then(|result| result.transpose())
            .map_err(IntoS3Error::into_s3_error)?
            .ok_or_else(|| s3_error!(InternalError, "Failed to process HEAD request"))?;

        let remote_info = if result.location.is_none() {
            Some(
                get_object_info(
                    &self.state,
                    GOI {
                        bucket,
                        key,
                        version_id,
                        range: None,
                        group_id,
                        user_identity: user_access.user_identity,
                        node_id: self.node_id,
                    },
                    user_access.path_restrictions.clone(),
                )
                .await
                .map_err(IntoS3Error::into_s3_error)?,
            )
        } else {
            None
        };

        let response_fields = self.build_response_fields(
            result.location.as_ref(),
            remote_info.as_ref(),
            Some(&result.metadata),
            result.source_metadata.as_ref(),
            result.last_refresh,
            result.version_created_at,
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
            && let Some(info) = remote_info.as_ref()
        {
            output.apply_checksums(encode_checksums(
                checksum_response_hashes(info.checksum_type, &info.hashes, &info.composite_hashes),
                ChecksumSelection::AllStored,
                map_checksum_type(info.checksum_type),
                info.part_count,
            ));
        } else if checksum_mode_enabled(&req.headers)
            && let Some(location) = result.location.as_ref()
        {
            output.apply_checksums(encode_checksums(
                checksum_response_hashes(
                    result.checksum_type,
                    &location.hashes,
                    &result.composite_hashes,
                ),
                ChecksumSelection::AllStored,
                map_checksum_type(result.checksum_type),
                result.part_count,
            ));
        }

        Ok(S3Response::new(output))
    }

    #[tracing::instrument(err, skip(self, req))]
    async fn list_parts(
        &self,
        req: S3Request<ListPartsInput>,
    ) -> S3Result<S3Response<ListPartsOutput>> {
        debug!(
            bucket = %req.input.bucket,
            key = %req.input.key,
            max_parts = ?req.input.max_parts,
            "Received LIST PARTS Request"
        );

        let _user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;
        reject_sse(
            req.input.sse_customer_algorithm.is_some()
                || req.input.sse_customer_key.is_some()
                || req.input.sse_customer_key_md5.is_some(),
        )?;
        let upload_id = parse_upload_id(&req.input.upload_id)?;
        let part_number_marker = match req.input.part_number_marker {
            None => None,
            Some(marker) if marker < 0 => {
                return Err(s3_error!(InvalidArgument, "Invalid part-number-marker"));
            }
            Some(marker) => Some(u16::try_from(marker).unwrap_or(u16::MAX)),
        };
        let max_parts = match req.input.max_parts {
            None => ListPartsOperation::DEFAULT_MAX_PARTS,
            Some(max_parts) => usize::try_from(max_parts)
                .map_err(|_| s3_error!(InvalidArgument, "max-parts must be non-negative"))?
                .min(ListPartsOperation::DEFAULT_MAX_PARTS),
        };

        let result = drive(
            ListPartsOperation::new(LPI {
                bucket: req.input.bucket.clone(),
                key: req.input.key.clone(),
                upload_id,
                part_number_marker,
                max_parts,
            }),
            &self.state,
        )
        .await
        .and_then(|result| result.transpose())
        .map_err(IntoS3Error::into_s3_error)?
        .ok_or_else(|| s3_error!(InternalError, "Failed to list parts"))?;

        let checksum_algorithm = result
            .upload
            .checksum_hint
            .as_ref()
            .and_then(|hint| hint.algorithm)
            .and_then(map_checksum_algorithm);
        let checksum_type = result
            .upload
            .checksum_hint
            .as_ref()
            .map(|hint| map_checksum_type(hint.checksum_type));
        let initiator = Some(Initiator {
            display_name: None,
            id: Some(result.upload.created_by.to_string()),
        });
        let owner = Some(Owner {
            display_name: None,
            id: Some(result.upload.group_id.to_string()),
        });

        let parts = result
            .parts
            .into_iter()
            .map(|part| {
                let checksums = encode_checksums(
                    &part.location.hashes,
                    ChecksumSelection::AllStored,
                    ChecksumType::from_static(ChecksumType::FULL_OBJECT),
                    None,
                );
                Part {
                    part_number: Some(i32::from(part.part_number)),
                    size: Some(part.location.blob_size as i64),
                    last_modified: Some(part.created_at.into()),
                    e_tag: part
                        .location
                        .hashes
                        .get(HASH_MD5)
                        .map(|value| ETag::Strong(hex::encode(value))),
                    checksum_crc32: checksums.checksum_crc32,
                    checksum_crc32c: checksums.checksum_crc32c,
                    checksum_crc64nvme: checksums.checksum_crc64nvme,
                    checksum_md5: None,
                    checksum_sha1: checksums.checksum_sha1,
                    checksum_sha256: checksums.checksum_sha256,
                    checksum_sha512: None,
                    checksum_xxhash128: None,
                    checksum_xxhash3: None,
                    checksum_xxhash64: None,
                }
            })
            .collect();

        Ok(S3Response::new(ListPartsOutput {
            bucket: Some(req.input.bucket),
            key: Some(req.input.key),
            upload_id: Some(req.input.upload_id),
            part_number_marker: req.input.part_number_marker,
            max_parts: Some(i32::try_from(max_parts).unwrap_or(i32::MAX)),
            is_truncated: Some(result.is_truncated),
            next_part_number_marker: result.next_part_number_marker.map(i32::from),
            parts: Some(parts),
            initiator,
            owner,
            storage_class: Some(StorageClass::from_static(StorageClass::STANDARD)),
            checksum_algorithm,
            checksum_type,
            ..Default::default()
        }))
    }

    #[tracing::instrument(err, skip(self, req))]
    async fn list_multipart_uploads(
        &self,
        req: S3Request<ListMultipartUploadsInput>,
    ) -> S3Result<S3Response<ListMultipartUploadsOutput>> {
        debug!(
            bucket = %req.input.bucket,
            prefix = ?req.input.prefix,
            "Received LIST MULTIPART UPLOADS Request"
        );

        let _user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;
        let bucket = req.input.bucket.clone();
        let prefix = req.input.prefix.clone();
        let delimiter = req.input.delimiter.clone();
        let key_marker = req.input.key_marker.clone();
        let requested_upload_id_marker = req.input.upload_id_marker.clone();
        let upload_id_marker =
            parse_upload_marker(key_marker.as_deref(), requested_upload_id_marker.as_deref())?;
        let max_uploads = match req.input.max_uploads {
            None => ListMultipartUploadsOperation::DEFAULT_MAX_UPLOADS,
            Some(max_uploads) => usize::try_from(max_uploads)
                .map_err(|_| s3_error!(InvalidArgument, "max-uploads must be non-negative"))?
                .min(ListMultipartUploadsOperation::DEFAULT_MAX_UPLOADS),
        };

        let result = drive(
            ListMultipartUploadsOperation::new(LMUI {
                bucket: bucket.clone(),
                prefix: prefix.clone(),
                delimiter: delimiter.clone(),
                key_marker: key_marker.clone(),
                upload_id_marker,
                max_uploads,
            }),
            &self.state,
        )
        .await
        .and_then(|result| result.transpose())
        .map_err(IntoS3Error::into_s3_error)?
        .ok_or_else(|| s3_error!(InternalError, "Failed to list multipart uploads"))?;

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

        let uploads: Vec<S3MultipartUpload> = result
            .uploads
            .into_iter()
            .map(|record| S3MultipartUpload {
                key: Some(encode_field(record.key)),
                upload_id: Some(record.upload_id.to_string()),
                initiated: Some(record.created_at.into()),
                initiator: Some(Initiator {
                    display_name: None,
                    id: Some(record.created_by.to_string()),
                }),
                owner: Some(Owner {
                    display_name: None,
                    id: Some(record.group_id.to_string()),
                }),
                storage_class: Some(StorageClass::from_static(StorageClass::STANDARD)),
                checksum_algorithm: record
                    .checksum_hint
                    .as_ref()
                    .and_then(|hint| hint.algorithm)
                    .and_then(map_checksum_algorithm),
                checksum_type: record
                    .checksum_hint
                    .as_ref()
                    .map(|hint| map_checksum_type(hint.checksum_type)),
            })
            .collect();
        let common_prefixes: Vec<CommonPrefix> = result
            .common_prefixes
            .into_iter()
            .map(|prefix| CommonPrefix {
                prefix: Some(encode_field(prefix)),
            })
            .collect();

        Ok(S3Response::new(ListMultipartUploadsOutput {
            bucket: Some(bucket),
            prefix: prefix.map(&encode_field),
            delimiter: delimiter.map(&encode_field),
            key_marker: key_marker.map(&encode_field),
            upload_id_marker: requested_upload_id_marker,
            max_uploads: Some(i32::try_from(max_uploads).unwrap_or(i32::MAX)),
            is_truncated: Some(result.is_truncated),
            next_key_marker: result.next_key_marker.map(&encode_field),
            next_upload_id_marker: result
                .next_upload_id_marker
                .map(|upload_id| upload_id.to_string()),
            uploads: Some(uploads),
            common_prefixes: Some(common_prefixes),
            encoding_type: req.input.encoding_type,
            ..Default::default()
        }))
    }

    #[tracing::instrument(err, skip(self, req))]
    async fn list_object_versions(
        &self,
        req: S3Request<ListObjectVersionsInput>,
    ) -> S3Result<S3Response<ListObjectVersionsOutput>> {
        debug!(
            bucket = %req.input.bucket,
            prefix = ?req.input.prefix,
            "Received LIST OBJECT VERSIONS Request"
        );

        let user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;
        let bucket_info = req.extensions.get::<BucketInfo>().cloned();
        let bucket = req.input.bucket.clone();
        let prefix = req.input.prefix.clone();
        let delimiter = req.input.delimiter.clone();
        let key_marker = req.input.key_marker.clone();
        let requested_version_id_marker = req.input.version_id_marker.clone();
        let version_id_marker = match requested_version_id_marker.as_deref() {
            None => None,
            Some(marker) => Some(
                ulid::Ulid::from_string(marker)
                    .map_err(|_| s3_error!(InvalidArgument, "Invalid version-id-marker"))?,
            ),
        };
        let max_keys = match req.input.max_keys {
            None => ListObjectVersionsOperation::DEFAULT_MAX_KEYS,
            Some(max_keys) => usize::try_from(max_keys)
                .map_err(|_| s3_error!(InvalidArgument, "max-keys must be non-negative"))?
                .min(ListObjectVersionsOperation::DEFAULT_MAX_KEYS),
        };
        let group_id = bucket_info
            .as_ref()
            .map(|bucket_info| bucket_info.group_id)
            .unwrap_or(user_access.group_id);

        let result = drive(
            ListObjectVersionsOperation::new(LOVI {
                bucket: bucket.clone(),
                prefix: prefix.clone(),
                delimiter: delimiter.clone(),
                key_marker: key_marker.clone(),
                version_id_marker,
                max_keys: Some(max_keys),
            }),
            &self.state,
        )
        .await
        .and_then(|result| result.transpose())
        .map_err(IntoS3Error::into_s3_error)?
        .ok_or_else(|| s3_error!(InternalError, "Failed to list object versions"))?;

        let owner = Some(Owner {
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

        let mut versions = Vec::new();
        let mut delete_markers = Vec::new();
        for item in result.items {
            match item {
                ListObjectVersionsItem::Version {
                    key,
                    version_id,
                    is_latest,
                    location,
                    source_metadata,
                    created_at,
                } => {
                    let response_fields = self.build_response_fields(
                        location.as_ref(),
                        None,
                        None,
                        source_metadata.as_ref(),
                        None,
                        Some(created_at),
                    );
                    versions.push(ObjectVersion {
                        key: Some(encode_field(key)),
                        version_id: Some(version_id.to_string()),
                        is_latest: Some(is_latest),
                        last_modified: Some(created_at.into()),
                        e_tag: response_fields.e_tag,
                        size: response_fields.content_length,
                        owner: owner.clone(),
                        storage_class: Some(ObjectVersionStorageClass::from_static(
                            ObjectVersionStorageClass::STANDARD,
                        )),
                        ..Default::default()
                    });
                }
                ListObjectVersionsItem::DeleteMarker {
                    key,
                    version_id,
                    is_latest,
                    created_at,
                } => {
                    delete_markers.push(DeleteMarkerEntry {
                        key: Some(encode_field(key)),
                        version_id: Some(version_id.to_string()),
                        is_latest: Some(is_latest),
                        last_modified: Some(created_at.into()),
                        owner: owner.clone(),
                    });
                }
            }
        }
        let common_prefixes: Vec<CommonPrefix> = result
            .common_prefixes
            .into_iter()
            .map(|prefix| CommonPrefix {
                prefix: Some(encode_field(prefix)),
            })
            .collect();

        Ok(S3Response::new(ListObjectVersionsOutput {
            name: Some(bucket),
            prefix: prefix.map(&encode_field),
            delimiter: delimiter.map(&encode_field),
            key_marker: key_marker.map(&encode_field),
            version_id_marker: requested_version_id_marker,
            max_keys: Some(i32::try_from(max_keys).unwrap_or(i32::MAX)),
            is_truncated: Some(result.is_truncated),
            next_key_marker: result.next_key_marker.map(&encode_field),
            next_version_id_marker: result
                .next_version_id_marker
                .map(|version_id| version_id.to_string()),
            versions: Some(versions),
            delete_markers: Some(delete_markers),
            common_prefixes: Some(common_prefixes),
            encoding_type: req.input.encoding_type,
            ..Default::default()
        }))
    }

    #[tracing::instrument(err, skip(self, req))]
    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        debug!(
            bucket = %req.input.bucket,
            key = %req.input.key,
            "Received DELETE Request"
        );

        let user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;
        let version_id = parse_version_id(req.input.version_id)?;
        let replication_auth = AuthContext {
            user_id: user_access.user_identity,
            realm_id: user_access.user_identity.realm_id,
            path_restrictions: user_access.path_restrictions.clone(),
            session: None,
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
        })
        .with_restrictions(replication_auth.path_restrictions.clone());

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

    #[tracing::instrument(err, skip(self, req))]
    async fn delete_objects(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        debug!(
            bucket = %req.input.bucket,
            objects = req.input.delete.objects.len(),
            "Received DELETE OBJECTS Request"
        );

        let body = req
            .extensions
            .get::<DeleteObjectsBody>()
            .ok_or_else(|| s3_error!(InternalError, "Missing DeleteObjects request body"))?;
        if body.exceeded() {
            return Err(s3_error!(
                MaxMessageLengthExceeded,
                "DeleteObjects request body exceeds 2 MiB"
            ));
        }
        let body = body.take_bytes();
        validate_delete_checksum(&req.headers, &body)?;

        let user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;
        // Deferred per-object policy uses the stashed request context, so
        // each entry is decided against the real query and headers.
        let extras = req
            .extensions
            .get::<PolicyRequestExtras>()
            .cloned()
            .ok_or_else(|| {
                error!(error = "Missing policy context");
                s3_error!(InternalError, "Missing policy context")
            })?;

        if req.input.delete.objects.len() > 1000 {
            return Err(s3_error!(
                MalformedXML,
                "The number of keys in a delete request must not exceed 1000."
            ));
        }

        let quiet = req.input.delete.quiet.unwrap_or(false);
        let replication_auth = AuthContext {
            user_id: user_access.user_identity,
            realm_id: user_access.user_identity.realm_id,
            path_restrictions: user_access.path_restrictions.clone(),
            session: None,
        };
        let bucket = req.input.bucket;

        // DeleteObjects has no object key in its path: load the realm and
        // group policy sets once and decide every entry against them.
        let policy_evaluator = aruna_operations::auth::request_policy::PolicyEvaluator::load(
            &self.state,
            self.realm_id,
            Some(user_access.group_id),
        )
        .await
        .map_err(|_| s3_error!(AccessDenied, "Request denied by policy"))?;

        let mut entries = Vec::with_capacity(req.input.delete.objects.len());
        let mut errors: Vec<S3DeleteError> = Vec::new();
        for object in req.input.delete.objects {
            let version_id = match parse_version_id(object.version_id.clone()) {
                Ok(version_id) => version_id,
                Err(_) => {
                    errors.push(S3DeleteError {
                        code: Some("NoSuchVersion".to_string()),
                        key: Some(object.key),
                        version_id: object.version_id,
                        message: Some("The specified version does not exist.".to_string()),
                    });
                    continue;
                }
            };

            let object_path = object_permission_path(
                self.realm_id,
                user_access.group_id,
                self.node_id,
                &bucket,
                &object.key,
            );
            let allowed = drive(
                CheckPermissionsOperation::new(CheckPermissionsConfig {
                    auth_context: replication_auth.clone(),
                    path: object_path.clone(),
                    required_permission: Permission::WRITE,
                }),
                &self.state,
            )
            .await
            .map_err(|err| s3_error!(InternalError, "{}", err.to_string()))?;
            let policy_request = aruna_operations::auth::request_policy::policy_request_with(
                &object_path,
                &Permission::WRITE,
                Some(&replication_auth),
                extras.clone(),
            );
            if !allowed || policy_evaluator.evaluate(&policy_request).is_err() {
                errors.push(S3DeleteError {
                    code: Some("AccessDenied".to_string()),
                    key: Some(object.key),
                    version_id: object.version_id,
                    message: Some("Access Denied".to_string()),
                });
                continue;
            }

            entries.push(DeleteObjectsEntry {
                key: object.key,
                version_id,
            });
        }

        let outcomes = delete_objects(
            &self.state,
            DOSI {
                bucket: bucket.clone(),
                entries,
                group_id: user_access.group_id,
                realm_id: self.realm_id,
                node_id: self.node_id,
                deleted_by: user_access.user_identity,
                restrictions: replication_auth.path_restrictions.clone(),
            },
        )
        .await;

        let mut deleted = Vec::new();
        for outcome in outcomes {
            match outcome.result {
                Ok(result) => {
                    if outcome.requested_version_id.is_none() {
                        self.queue_live_replication(
                            replication_auth.clone(),
                            bucket.clone(),
                            outcome.key.clone(),
                            result.version_id,
                            result.delete_marker,
                        )
                        .await;
                    }
                    let deleted_object = if outcome.requested_version_id.is_none() {
                        DeletedObject {
                            key: Some(outcome.key),
                            delete_marker: Some(result.delete_marker),
                            delete_marker_version_id: Some(result.version_id.to_string()),
                            ..Default::default()
                        }
                    } else {
                        DeletedObject {
                            key: Some(outcome.key),
                            version_id: Some(result.version_id.to_string()),
                            delete_marker: Some(result.delete_marker),
                            delete_marker_version_id: result
                                .delete_marker
                                .then(|| result.version_id.to_string()),
                        }
                    };
                    deleted.push(deleted_object);
                }
                Err(DeleteObjectError::NoSuchVersion) => errors.push(S3DeleteError {
                    code: Some("NoSuchVersion".to_string()),
                    key: Some(outcome.key),
                    version_id: outcome.requested_version_id.map(|id| id.to_string()),
                    message: Some("The specified version does not exist.".to_string()),
                }),
                Err(err) => {
                    warn!(error = %err, key = %outcome.key, "DeleteObjects entry failed");
                    errors.push(S3DeleteError {
                        code: Some("InternalError".to_string()),
                        key: Some(outcome.key),
                        version_id: outcome.requested_version_id.map(|id| id.to_string()),
                        message: Some(
                            "We encountered an internal error. Please try again.".to_string(),
                        ),
                    });
                }
            }
        }

        Ok(S3Response::new(DeleteObjectsOutput {
            deleted: (!quiet).then_some(deleted),
            errors: (!errors.is_empty()).then_some(errors),
            ..Default::default()
        }))
    }

    #[tracing::instrument(err, skip(self, req))]
    async fn delete_bucket(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        debug!(bucket = %req.input.bucket, "Received DELETE BUCKET Request");

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
        let user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;
        let extras = req
            .extensions
            .get::<PolicyRequestExtras>()
            .cloned()
            .ok_or_else(|| {
                error!(error = "Missing policy context");
                s3_error!(InternalError, "Missing policy context")
            })?;
        let bucket = req.input.bucket;
        let targets =
            self.parse_replication_targets(&bucket, &req.input.replication_configuration)?;
        let old_relationships = self.list_xml_relationships(&bucket).await?;
        let source = ArunaArn::s3_bucket(self.realm_id, self.node_id, bucket.clone())
            .map_err(|error| s3_error!(InvalidArgument, "{}", error.to_string()))?;
        let created_at = SystemTime::now();
        let relationships = targets
            .into_iter()
            .map(|(target, replicate_deletes)| SyncRelationship {
                id: ulid::Ulid::generate(),
                source: source.clone(),
                target,
                mode: SyncMode::Continuous,
                reference_handling: Default::default(),
                reference_serving: false,
                replicate_deletes,
                created_by: user_access.user_identity,
                created_at,
                state: SyncState::Enabled,
                status: SyncStatusSnapshot::default(),
            })
            .collect::<Vec<_>>();

        let mut stored = Vec::new();
        for relationship in &relationships {
            stage_mirror_reconcile(&self.state, relationship)
                .await
                .map_err(|error| s3_error!(InternalError, "{}", error))?;
            if let Err(error) = self
                .create_sync_mirror(&user_access, relationship, &extras)
                .await
            {
                kick_mirror_repair(&self.state).await;
                for created in &stored {
                    if stage_mirror_delete(&self.state, created).await.is_ok() {
                        let _ = self
                            .delete_sync_relationship(
                                created.clone(),
                                SyncRelationshipDirection::Outgoing,
                            )
                            .await;
                        kick_mirror_repair(&self.state).await;
                        if self.remove_sync_mirror(created).await {
                            self.clear_mirror_repair(created, SyncMirrorRepairIntent::Delete)
                                .await;
                        }
                    }
                }
                return Err(error);
            }
            if let Err(error) = self
                .store_sync_relationship(relationship.clone(), SyncRelationshipDirection::Outgoing)
                .await
            {
                let mut rollback = stored.clone();
                rollback.push(relationship.clone());
                for created in &rollback {
                    if stage_mirror_delete(&self.state, created).await.is_ok() {
                        let _ = self
                            .delete_sync_relationship(
                                created.clone(),
                                SyncRelationshipDirection::Outgoing,
                            )
                            .await;
                        kick_mirror_repair(&self.state).await;
                        if self.remove_sync_mirror(created).await {
                            self.clear_mirror_repair(created, SyncMirrorRepairIntent::Delete)
                                .await;
                        }
                    }
                }
                return Err(error);
            }
            self.clear_mirror_repair(relationship, SyncMirrorRepairIntent::Reconcile)
                .await;
            stored.push(relationship.clone());
        }

        for relationship in old_relationships {
            stage_mirror_delete(&self.state, &relationship)
                .await
                .map_err(|error| s3_error!(InternalError, "{}", error))?;
            self.delete_sync_relationship(
                relationship.clone(),
                SyncRelationshipDirection::Outgoing,
            )
            .await?;
            kick_mirror_repair(&self.state).await;
            if self.remove_sync_mirror(&relationship).await {
                self.clear_mirror_repair(&relationship, SyncMirrorRepairIntent::Delete)
                    .await;
            }
        }

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

        let relationships = self.list_xml_relationships(&req.input.bucket).await?;
        if relationships.is_empty() {
            return Err(s3_error!(
                ReplicationConfigurationNotFoundError,
                "Replication configuration not found"
            ));
        }

        Ok(S3Response::new(GetBucketReplicationOutput {
            replication_configuration: Some(self.build_replication_configuration(&relationships)),
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
        let relationships = self.list_xml_relationships(&req.input.bucket).await?;

        for relationship in relationships {
            stage_mirror_delete(&self.state, &relationship)
                .await
                .map_err(|error| s3_error!(InternalError, "{}", error))?;
            self.delete_sync_relationship(
                relationship.clone(),
                SyncRelationshipDirection::Outgoing,
            )
            .await?;
            kick_mirror_repair(&self.state).await;
            if self.remove_sync_mirror(&relationship).await {
                self.clear_mirror_repair(&relationship, SyncMirrorRepairIntent::Delete)
                    .await;
            }
        }

        Ok(S3Response::new(DeleteBucketReplicationOutput::default()))
    }
}

#[cfg(test)]
mod tests {
    use super::bucket::MAX_REPLICATION_TARGETS;
    use super::object::next_marker_of;
    use super::*;
    use crate::s3::checksum::UploadChecksumRequest;
    use crate::s3::scope::resolve_scope;
    use aruna_core::UserId;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{
        AUTH_KEYSPACE, BLOB_HEAD_KEYSPACE, BLOB_LIVE_REPLICATION_OBLIGATION_KEYSPACE,
        BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, NOTIFICATION_INBOX_KEYSPACE,
        REALM_CONFIG_KEYSPACE, S3_BUCKET_KEYSPACE,
    };
    use aruna_core::structs::{
        Actor, BackendLocation, BackendRef, BlobHeadKey, BlobLocationKey, BlobVersion,
        BlobVersionState, CurrentVersionPointer, GroupAuthorizationDocument, MultipartChecksumType,
        NotificationClass, NotificationKind, NotificationRecord, PathRestriction,
        PortableSourceDescriptor, RealmAuthorizationDocument, RealmConfigDocument, RealmNodeKind,
        SourceConnectorKind, SourceMetadata, StagingStrategy, VersionKey, VersionSourceBinding,
        WatchEventKind, WatchEventMask, WatchInterestEntry, WatchInterestTable,
        bucket_permission_path, watch_resource_path,
    };
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_operations::driver::{DriverContext, drive};
    use aruna_operations::notifications::watch::subscriptions::create_local_watch;
    use aruna_operations::replication::queue::{
        LiveReplicationObligationRecord, live_obligation_key,
    };
    use aruna_operations::s3::get_object::ObjectInfo;
    use aruna_operations::s3::list_objects::ListObjectsV2ContinuationToken;
    use aruna_operations::s3::put_object::PutObjectResult;
    use aruna_operations::s3::refresh_metadata::{
        QueueReferenceMetadataRefreshOperation, ReferenceMetadataRefresh,
        refresh_reference_metadata,
    };
    use aruna_storage::storage;
    use futures_util::{StreamExt, stream};
    use http::Extensions;
    use hyper::{HeaderMap, Method, Uri, body::Bytes};
    use s3s::dto::{
        ChecksumType, DeleteMarkerReplication, DeleteMarkerReplicationStatus, Destination,
        ReplicationConfiguration, ReplicationRule, ReplicationRuleStatus,
    };
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

    fn parser_service(realm_id: RealmId, node_id: NodeId) -> (TempDir, ArunaS3Service) {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let state = Arc::new(DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });
        (
            storage_dir,
            ArunaS3Service {
                state,
                realm_id,
                node_id,
                rocrate_limits: RoCrateLimits::default(),
                completions: Arc::new(completion_registry()),
            },
        )
    }

    #[test]
    fn tracks_content_type() {
        let realm_id = RealmId([1u8; 32]);
        let node_id = iroh::SecretKey::from_bytes(&[2u8; 32]).public();
        let (_dir, service) = parser_service(realm_id, node_id);
        let metadata = object_metadata(
            HashMap::from([("user".to_string(), "value".to_string())]),
            Some("application/vnd.eln+zip"),
        );

        let fields = service.build_response_fields(None, None, Some(&metadata), None, None, None);

        assert_eq!(
            fields.content_type.as_deref(),
            Some("application/vnd.eln+zip")
        );
        assert_eq!(
            fields.metadata,
            Some(HashMap::from([("user".to_string(), "value".to_string())]))
        );
    }

    // A holder-backed read has no location, so ObjectInfo supplies its fields.
    #[test]
    fn info_supplies_fields() {
        let realm_id = RealmId([4u8; 32]);
        let node_id = iroh::SecretKey::from_bytes(&[5u8; 32]).public();
        let (_dir, service) = parser_service(realm_id, node_id);
        let info = ObjectInfo {
            size: 42,
            version_created_at: Some(UNIX_EPOCH),
            etag: Some("0123456789abcdef0123456789abcdef".to_string()),
            checksum_type: MultipartChecksumType::FullObject,
            hashes: HashMap::new(),
            composite_hashes: HashMap::new(),
            part_count: None,
        };

        let fields = service.build_response_fields(
            None,
            Some(&info),
            Some(&HashMap::new()),
            None,
            None,
            None,
        );

        assert_eq!(fields.content_length, Some(42));
        assert_eq!(fields.last_modified, Some(UNIX_EPOCH.into()));
        assert_eq!(
            fields.e_tag,
            Some(ETag::Strong("0123456789abcdef0123456789abcdef".to_string()))
        );
    }

    #[test]
    fn info_prevents_fallbacks() {
        let realm_id = RealmId([6u8; 32]);
        let node_id = iroh::SecretKey::from_bytes(&[7u8; 32]).public();
        let (_dir, service) = parser_service(realm_id, node_id);
        let location = response_location(UserId::local(Ulid::generate(), realm_id));
        let info = ObjectInfo {
            size: 0,
            version_created_at: None,
            etag: None,
            checksum_type: MultipartChecksumType::FullObject,
            hashes: HashMap::new(),
            composite_hashes: HashMap::new(),
            part_count: None,
        };

        let fields =
            service.build_response_fields(Some(&location), Some(&info), None, None, None, None);

        assert_eq!(fields.content_length, Some(0));
        assert_eq!(fields.e_tag, None);
        assert_eq!(fields.last_modified, None);
    }

    fn replication_config(bucket: String) -> ReplicationConfiguration {
        ReplicationConfiguration {
            role: "arn:aruna:replication-role".to_string(),
            rules: vec![ReplicationRule {
                delete_marker_replication: None,
                destination: Destination {
                    access_control_translation: None,
                    account: None,
                    bucket,
                    encryption_configuration: None,
                    metrics: None,
                    replication_time: None,
                    storage_class: None,
                },
                existing_object_replication: None,
                filter: None,
                id: None,
                prefix: None,
                priority: None,
                source_selection_criteria: None,
                status: ReplicationRuleStatus::from_static(ReplicationRuleStatus::ENABLED),
            }],
        }
    }

    #[test]
    fn accepts_different_bucket() {
        let realm_id = RealmId([71u8; 32]);
        let source_node = iroh::SecretKey::from_bytes(&[72u8; 32]).public();
        let target_node = iroh::SecretKey::from_bytes(&[73u8; 32]).public();
        let (_storage_dir, service) = parser_service(realm_id, source_node);
        let target = ArunaArn::s3_bucket(realm_id, target_node, "target-bucket").unwrap();

        let parsed = service
            .parse_replication_targets("source-bucket", &replication_config(target.to_string()))
            .unwrap();

        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].0.bucket(), Some("target-bucket"));
        assert_eq!(parsed[0].0.to_string(), target.to_string());
    }

    #[test]
    fn deduplicates_targets() {
        let realm_id = RealmId([77u8; 32]);
        let source_node = iroh::SecretKey::from_bytes(&[78u8; 32]).public();
        let target_node = iroh::SecretKey::from_bytes(&[79u8; 32]).public();
        let (_storage_dir, service) = parser_service(realm_id, source_node);
        let target = ArunaArn::s3_bucket(realm_id, target_node, "target-bucket").unwrap();
        let mut configuration = replication_config(target.to_string());
        let rule = configuration.rules[0].clone();
        configuration.rules.push(rule);

        let parsed = service
            .parse_replication_targets("source-bucket", &configuration)
            .unwrap();

        assert_eq!(parsed.len(), 1);
    }

    #[test]
    fn retains_delete_modes() {
        let realm_id = RealmId([80u8; 32]);
        let source_node = iroh::SecretKey::from_bytes(&[81u8; 32]).public();
        let target_node = iroh::SecretKey::from_bytes(&[82u8; 32]).public();
        let (_storage_dir, service) = parser_service(realm_id, source_node);
        let target = ArunaArn::s3_bucket(realm_id, target_node, "target-bucket").unwrap();
        let mut configuration = replication_config(target.to_string());
        let rule = configuration.rules[0].clone();
        configuration.rules.push(ReplicationRule {
            delete_marker_replication: Some(DeleteMarkerReplication {
                status: Some(DeleteMarkerReplicationStatus::from_static(
                    DeleteMarkerReplicationStatus::ENABLED,
                )),
            }),
            ..rule
        });

        let parsed = service
            .parse_replication_targets("source-bucket", &configuration)
            .unwrap();

        assert_eq!(parsed.len(), 2);
        assert!(!parsed[0].1);
        assert!(parsed[1].1);
    }

    #[test]
    fn rejects_target_cap() {
        let realm_id = RealmId([83u8; 32]);
        let source_node = iroh::SecretKey::from_bytes(&[84u8; 32]).public();
        let target_node = iroh::SecretKey::from_bytes(&[85u8; 32]).public();
        let (_storage_dir, service) = parser_service(realm_id, source_node);
        let target = ArunaArn::s3_bucket(realm_id, target_node, "target-bucket").unwrap();
        let mut configuration = replication_config(target.to_string());
        let rule = configuration.rules[0].clone();
        configuration
            .rules
            .extend((0..MAX_REPLICATION_TARGETS).map(|_| rule.clone()));

        assert!(
            service
                .parse_replication_targets("source-bucket", &configuration)
                .is_err()
        );
    }

    #[test]
    fn rejects_prefix_target() {
        let realm_id = RealmId([74u8; 32]);
        let source_node = iroh::SecretKey::from_bytes(&[75u8; 32]).public();
        let target_node = iroh::SecretKey::from_bytes(&[76u8; 32]).public();
        let (_storage_dir, service) = parser_service(realm_id, source_node);
        let target =
            ArunaArn::s3_object_prefix(realm_id, target_node, "target-bucket", "prefix").unwrap();

        assert!(
            service
                .parse_replication_targets(
                    "source-bucket",
                    &replication_config(target.to_string()),
                )
                .is_err()
        );
    }

    #[test]
    fn upload_marker_ignored() {
        assert_eq!(parse_upload_marker(None, Some("not-a-ulid")).unwrap(), None);
        assert_eq!(
            parse_upload_marker(Some(""), Some("not-a-ulid")).unwrap(),
            None
        );

        let marker = Ulid::generate();
        assert_eq!(
            parse_upload_marker(Some("key"), Some(&marker.to_string())).unwrap(),
            Some(marker)
        );
        assert!(parse_upload_marker(Some("key"), Some("not-a-ulid")).is_err());
    }

    #[tokio::test]
    async fn put_survives_queue() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let context = Arc::new(DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });
        let realm_id = RealmId([40u8; 32]);
        let node_id = iroh::SecretKey::from_bytes(&[4u8; 32]).public();
        let service = ArunaS3Service::new(context, realm_id, node_id).await;
        let bucket = "bucket".to_string();
        let key = "object".to_string();
        let version_id = Ulid::generate();
        let user_id = UserId::local(Ulid::generate(), realm_id);
        let auth = AuthContext {
            user_id,
            realm_id,
            path_restrictions: None,
            session: None,
        };

        write_storage_value(
            &storage_handle,
            S3_BUCKET_KEYSPACE,
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
        let obligation_key = live_obligation_key(&obligation).unwrap();
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
            checksum_type_declared: false,
            composite_part_count: None,
        };
        let response = service
            .put_object_response(
                &checksum_request,
                auth,
                Ulid::generate(),
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
            compute_handle: None,
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
        let realm_auth = RealmAuthorizationDocument::default_realm_doc(realm_id);
        let group_auth = GroupAuthorizationDocument::default_group_doc(watcher, realm_id, group_id);
        let group = aruna_core::structs::Group {
            display_name: "watched".to_string(),
            group_id,
            realm_id,
            owner: watcher,
            roles: group_auth.roles.keys().copied().collect(),
        };
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
        write_storage_value(
            &context.storage_handle,
            aruna_core::keyspaces::GROUP_KEYSPACE,
            group_id.to_bytes().to_vec(),
            group.to_bytes(&actor).unwrap(),
        )
        .await;
    }

    async fn read_watch_rows(context: &DriverContext) -> Vec<NotificationRecord> {
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
    async fn put_expands_watch() {
        let realm_id = RealmId([41u8; 32]);
        let (_storage_dir, context, net) = build_watch_context(realm_id, [41u8; 32]).await;
        let holder = net.node_id();

        let service = ArunaS3Service::new(context.clone(), realm_id, net.node_id()).await;
        let user_id = UserId::local(Ulid::generate(), realm_id);
        let watcher = UserId::local(Ulid::generate(), realm_id);
        let group_id = Ulid::generate();
        let watch_prefix = watch_resource_path(group_id, net.node_id(), "bucket", "");
        net.replace_watch_interest(data_uploaded_interest(
            realm_id,
            holder,
            watch_prefix.clone(),
        ));
        install_watch_authorization(&context, realm_id, net.node_id(), group_id, watcher).await;
        create_local_watch(
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
            session: None,
        };
        let checksum_request = UploadChecksumRequest {
            expected: Vec::new(),
            response_algorithm: None,
            checksum_type: ChecksumType::from_static(ChecksumType::FULL_OBJECT),
            checksum_type_declared: false,
            composite_part_count: None,
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
                    version_id: Ulid::generate(),
                },
            )
            .await
            .expect("committed PUT response should succeed");

        let rows = read_watch_rows(context.as_ref()).await;
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
                    &watch_resource_path(group_id, net.node_id(), "bucket", "object")
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
    async fn anonymous_put_silent() {
        let realm_id = RealmId([42u8; 32]);
        let (_storage_dir, context, net) = build_watch_context(realm_id, [42u8; 32]).await;
        let holder = net.node_id();
        net.replace_watch_interest(data_uploaded_interest(
            realm_id,
            holder,
            watch_resource_path(Ulid::generate(), net.node_id(), "bucket", ""),
        ));

        let service = ArunaS3Service::new(context.clone(), realm_id, net.node_id()).await;
        let anonymous = UserId::nil(realm_id);
        let group_id = Ulid::generate();
        let auth = AuthContext {
            user_id: anonymous,
            realm_id,
            path_restrictions: None,
            session: None,
        };
        let checksum_request = UploadChecksumRequest {
            expected: Vec::new(),
            response_algorithm: None,
            checksum_type: ChecksumType::from_static(ChecksumType::FULL_OBJECT),
            checksum_type_declared: false,
            composite_part_count: None,
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
                    version_id: Ulid::generate(),
                },
            )
            .await
            .expect("committed PUT response should succeed");

        assert!(
            read_watch_rows(context.as_ref()).await.is_empty(),
            "an anonymous actor must not emit a watch event"
        );

        net.shutdown().await;
    }

    #[tokio::test]
    async fn refresh_failure_hidden() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let context = Arc::new(DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });
        let refresh = ReferenceMetadataRefresh {
            bucket: "bucket".to_string(),
            key: "reference".to_string(),
            version_id: Ulid::generate(),
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

        let mut blob = attach_reference_refresh(
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
    async fn stale_refresh_ignored() {
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
    async fn newer_refresh_updates() {
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
            compute_handle: None,
        });
        let realm_id = aruna_core::structs::RealmId([9u8; 32]);

        TestState {
            _storage_dir: storage_dir,
            context,
            bucket: "bucket".to_string(),
            key: "key".to_string(),
            version_id: Ulid::generate(),
            created_by: UserId::local(Ulid::generate(), realm_id),
        }
    }

    fn response_location(created_by: UserId) -> BackendLocation {
        let mut hashes = HashMap::new();
        hashes.insert(HASH_MD5.to_string(), vec![1u8; 16]);

        BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/tmp".to_string(),
            storage_bucket: "objects".to_string(),
            backend_path: "bucket/object".to_string(),
            ulid: Ulid::generate(),
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

    /// Policy loading fails closed without the realm config document.
    async fn write_realm_config(
        storage: &storage::StorageHandle,
        realm_id: RealmId,
        actor: &Actor,
    ) {
        write_storage_value(
            storage,
            REALM_CONFIG_KEYSPACE,
            realm_id.as_bytes().to_vec(),
            RealmConfigDocument::default_for_realm(realm_id, Vec::new())
                .to_bytes(actor)
                .unwrap(),
        )
        .await;
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
        let version = BlobVersion::materialized(
            hash,
            BackendRef::node_default(),
            created_at,
            created_by,
            None,
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

        let location = BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/tmp".to_string(),
            storage_bucket: "objects".to_string(),
            backend_path: format!("path/{key}"),
            ulid: Ulid::generate(),
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
                key: BlobLocationKey::new(hash, location.backend.clone())
                    .to_bytes()
                    .into(),
                value: location.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;
    }

    #[allow(clippy::too_many_arguments)]
    async fn write_reference_metadata(
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
            user_identity: UserId::local(Ulid::generate(), realm_id),
            group_id,
            secret: aruna_core::credential_encryption::EncryptedS3Secret::empty(),
            expiry: SystemTime::now() + Duration::from_secs(3600),
            path_restrictions: None,
            issued_by: [0u8; 32],
            revoked_at: None,
        }
    }

    /// Names a credential restricted to the `allowed` bucket path plus `scope`
    /// sees; the group also owns a `hidden` bucket outside that restriction.
    async fn visible_buckets(scope: &str) -> Vec<String> {
        let realm_id = RealmId([43u8; 32]);
        let node_id = iroh::SecretKey::from_bytes(&[43u8; 32]).public();
        let (_storage_dir, service) = parser_service(realm_id, node_id);
        let group_id = Ulid::generate();
        let mut user_access = test_user_access(group_id, realm_id);
        let actor = Actor {
            node_id,
            user_id: user_access.user_identity,
            realm_id,
        };
        let realm_auth = RealmAuthorizationDocument::default_realm_doc(realm_id);
        let group_auth = GroupAuthorizationDocument::default_group_doc(
            user_access.user_identity,
            realm_id,
            group_id,
        );
        let group = aruna_core::structs::Group {
            display_name: "listing".to_string(),
            group_id,
            realm_id,
            owner: user_access.user_identity,
            roles: group_auth.roles.keys().copied().collect(),
        };
        write_realm_config(&service.state.storage_handle, realm_id, &actor).await;
        write_storage_value(
            &service.state.storage_handle,
            AUTH_KEYSPACE,
            realm_id.as_bytes().to_vec(),
            realm_auth.to_bytes(&actor).unwrap(),
        )
        .await;
        write_storage_value(
            &service.state.storage_handle,
            AUTH_KEYSPACE,
            group_id.to_bytes().to_vec(),
            group_auth.to_bytes(&actor).unwrap(),
        )
        .await;
        write_storage_value(
            &service.state.storage_handle,
            aruna_core::keyspaces::GROUP_KEYSPACE,
            group_id.to_bytes().to_vec(),
            group.to_bytes(&actor).unwrap(),
        )
        .await;
        for bucket in ["allowed", "hidden"] {
            write_storage_value(
                &service.state.storage_handle,
                S3_BUCKET_KEYSPACE,
                bucket.as_bytes().to_vec(),
                test_bucket_info(group_id, user_access.user_identity)
                    .to_bytes()
                    .unwrap(),
            )
            .await;
        }
        user_access.path_restrictions = Some(vec![PathRestriction {
            pattern: format!(
                "{}{scope}",
                bucket_permission_path(realm_id, group_id, node_id, "allowed")
            ),
            permission: Permission::READ,
        }]);

        let mut extensions = Extensions::new();
        extensions.insert(user_access);
        extensions.insert(PolicyRequestExtras::operation("s3.ListBuckets"));
        let request = S3Request {
            input: ListBucketsInput::default(),
            method: Method::GET,
            uri: Uri::from_static("/"),
            headers: HeaderMap::new(),
            extensions,
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        };
        let response = service.list_buckets(request).await.unwrap();
        response
            .output
            .buckets
            .unwrap_or_default()
            .into_iter()
            .filter_map(|bucket| bucket.name)
            .collect()
    }

    /// A node whose caller holds one role granting read on `study/imaging`
    /// only, with a second bucket and sibling keys the role never reaches.
    async fn subpath_node() -> (TempDir, ArunaS3Service, UserAccess, Ulid) {
        use std::collections::{HashMap, HashSet};
        let realm_id = RealmId([51u8; 32]);
        let node_id = iroh::SecretKey::from_bytes(&[51u8; 32]).public();
        let (storage_dir, service) = parser_service(realm_id, node_id);
        let group_id = Ulid::generate();
        let user_access = test_user_access(group_id, realm_id);
        let actor = Actor {
            node_id,
            user_id: user_access.user_identity,
            realm_id,
        };
        let role_id = Ulid::generate();
        let group_auth = GroupAuthorizationDocument {
            group_id,
            roles: HashMap::from([(
                role_id,
                aruna_core::structs::Role {
                    role_id,
                    name: "imaging-reader".to_string(),
                    permissions: HashMap::from([(
                        format!(
                            "{}/imaging/**",
                            bucket_permission_path(realm_id, group_id, node_id, "study")
                        ),
                        Permission::READ,
                    )]),
                    assigned_users: HashSet::from([user_access.user_identity]),
                },
            )]),
            policies: Vec::new(),
        };
        let group = aruna_core::structs::Group {
            display_name: "imaging".to_string(),
            group_id,
            realm_id,
            owner: user_access.user_identity,
            roles: group_auth.roles.keys().copied().collect(),
        };
        write_realm_config(&service.state.storage_handle, realm_id, &actor).await;
        write_storage_value(
            &service.state.storage_handle,
            AUTH_KEYSPACE,
            realm_id.as_bytes().to_vec(),
            RealmAuthorizationDocument::default_realm_doc(realm_id)
                .to_bytes(&actor)
                .unwrap(),
        )
        .await;
        write_storage_value(
            &service.state.storage_handle,
            AUTH_KEYSPACE,
            group_id.to_bytes().to_vec(),
            group_auth.to_bytes(&actor).unwrap(),
        )
        .await;
        write_storage_value(
            &service.state.storage_handle,
            aruna_core::keyspaces::GROUP_KEYSPACE,
            group_id.to_bytes().to_vec(),
            group.to_bytes(&actor).unwrap(),
        )
        .await;
        for bucket in ["study", "other"] {
            write_storage_value(
                &service.state.storage_handle,
                S3_BUCKET_KEYSPACE,
                bucket.as_bytes().to_vec(),
                test_bucket_info(group_id, user_access.user_identity)
                    .to_bytes()
                    .unwrap(),
            )
            .await;
        }
        seed_materialized_keys(
            &service.state.storage_handle,
            "study",
            &[
                "imaging/scan-a",
                "imaging/scan-b",
                "sequencing/reads",
                "notes.txt",
            ],
            user_access.user_identity,
            UNIX_EPOCH,
        )
        .await;

        (storage_dir, service, user_access, group_id)
    }

    async fn subpath_request(
        service: &ArunaS3Service,
        user_access: &UserAccess,
        group_id: Ulid,
        prefix: Option<&str>,
    ) -> S3Request<ListObjectsV2Input> {
        let scope = resolve_scope(
            &service.state,
            user_access,
            &bucket_permission_path(service.realm_id, group_id, service.node_id, "study"),
        )
        .await
        .unwrap();
        assert!(!scope.is_empty());
        let mut extensions = Extensions::new();
        extensions.insert(user_access.clone());
        extensions.insert(test_bucket_info(group_id, user_access.user_identity));
        extensions.insert(scope);
        list_request(
            extensions,
            ListObjectsV2Input {
                bucket: "study".to_string(),
                delimiter: Some("/".to_string()),
                max_keys: Some(10),
                prefix: prefix.map(str::to_string),
                ..Default::default()
            },
        )
    }

    #[tokio::test]
    async fn subpath_sees_bucket() {
        // Only the bucket holding the granted folder may appear.
        let (_storage_dir, service, user_access, _group_id) = subpath_node().await;
        let mut extensions = Extensions::new();
        extensions.insert(user_access);
        extensions.insert(PolicyRequestExtras::operation("s3.ListBuckets"));
        let request = S3Request {
            input: ListBucketsInput::default(),
            method: Method::GET,
            uri: Uri::from_static("/"),
            headers: HeaderMap::new(),
            extensions,
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        };

        let response = service.list_buckets(request).await.unwrap();
        let buckets: Vec<String> = response
            .output
            .buckets
            .unwrap_or_default()
            .into_iter()
            .filter_map(|bucket| bucket.name)
            .collect();
        assert_eq!(buckets, vec!["study"]);
    }

    /// Adds a realm role that denies the caller the whole group data subtree.
    async fn write_realm_deny(service: &ArunaS3Service, user_access: &UserAccess) {
        use std::collections::{HashMap, HashSet};
        let realm_id = service.realm_id;
        let actor = Actor {
            node_id: service.node_id,
            user_id: user_access.user_identity,
            realm_id,
        };
        let mut realm_auth = RealmAuthorizationDocument::default_realm_doc(realm_id);
        let role_id = Ulid::generate();
        realm_auth.roles.insert(
            role_id,
            aruna_core::structs::Role {
                role_id,
                name: "data-deny".to_string(),
                permissions: HashMap::from([(format!("/{realm_id}/g/**"), Permission::DENY)]),
                assigned_users: HashSet::from([user_access.user_identity]),
            },
        );
        write_storage_value(
            &service.state.storage_handle,
            AUTH_KEYSPACE,
            realm_id.as_bytes().to_vec(),
            realm_auth.to_bytes(&actor).unwrap(),
        )
        .await;
    }

    async fn write_deny_policy(service: &ArunaS3Service, user_access: &UserAccess) {
        let realm_id = service.realm_id;
        let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        config
            .request_policies
            .push(aruna_core::request_policy::RequestPolicy {
                policy_id: Ulid::generate(),
                name: "no-reads".to_string(),
                kind: aruna_core::request_policy::PolicyKind::Deny,
                when: None,
                expression: "permission == 'read'".to_string(),
                enabled: true,
            });
        let actor = Actor {
            node_id: service.node_id,
            user_id: user_access.user_identity,
            realm_id,
        };
        write_storage_value(
            &service.state.storage_handle,
            REALM_CONFIG_KEYSPACE,
            realm_id.as_bytes().to_vec(),
            config.to_bytes(&actor).unwrap(),
        )
        .await;
    }

    async fn listed_buckets(service: &ArunaS3Service, user_access: &UserAccess) -> Vec<String> {
        let mut extensions = Extensions::new();
        extensions.insert(user_access.clone());
        extensions.insert(PolicyRequestExtras::operation("s3.ListBuckets"));
        let request = S3Request {
            input: ListBucketsInput::default(),
            method: Method::GET,
            uri: Uri::from_static("/"),
            headers: HeaderMap::new(),
            extensions,
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        };
        service
            .list_buckets(request)
            .await
            .unwrap()
            .output
            .buckets
            .unwrap_or_default()
            .into_iter()
            .filter_map(|bucket| bucket.name)
            .collect()
    }

    #[tokio::test]
    async fn deny_hides_subpath() {
        // A realm deny outranks the group role that grants the folder, so no
        // listing, head or bucket visibility survives it.
        let (_storage_dir, service, user_access, group_id) = subpath_node().await;
        write_realm_deny(&service, &user_access).await;

        let scope = resolve_scope(
            &service.state,
            &user_access,
            &bucket_permission_path(service.realm_id, group_id, service.node_id, "study"),
        )
        .await
        .unwrap();
        assert!(scope.is_empty());
        assert!(listed_buckets(&service, &user_access).await.is_empty());
    }

    /// Replaces the seeded folder role with the owner's default roles, so the
    /// caller reads the whole bucket and only a policy can refuse it.
    async fn grant_group_owner(service: &ArunaS3Service, user_access: &UserAccess, group_id: Ulid) {
        let actor = Actor {
            node_id: service.node_id,
            user_id: user_access.user_identity,
            realm_id: service.realm_id,
        };
        write_storage_value(
            &service.state.storage_handle,
            AUTH_KEYSPACE,
            group_id.to_bytes().to_vec(),
            GroupAuthorizationDocument::default_group_doc(
                user_access.user_identity,
                service.realm_id,
                group_id,
            )
            .to_bytes(&actor)
            .unwrap(),
        )
        .await;
    }

    #[tokio::test]
    async fn policy_hides_bucket() {
        // A policy denial is the verdict itself, never a reason to fall back on
        // the caller's role subtrees.
        let (_storage_dir, service, user_access, group_id) = subpath_node().await;
        grant_group_owner(&service, &user_access, group_id).await;
        assert!(!listed_buckets(&service, &user_access).await.is_empty());

        write_deny_policy(&service, &user_access).await;
        assert!(listed_buckets(&service, &user_access).await.is_empty());
    }

    #[tokio::test]
    async fn subpath_hides_siblings() {
        let (_storage_dir, service, user_access, group_id) = subpath_node().await;

        let request = subpath_request(&service, &user_access, group_id, None).await;
        let output = service.list_objects_v2(request).await.unwrap().output;
        let prefixes: Vec<String> = output
            .common_prefixes
            .unwrap_or_default()
            .into_iter()
            .filter_map(|prefix| prefix.prefix)
            .collect();
        assert_eq!(prefixes, vec!["imaging/"]);
        assert!(output.contents.unwrap_or_default().is_empty());
        assert_eq!(output.is_truncated, Some(false));

        let request = subpath_request(&service, &user_access, group_id, Some("imaging/")).await;
        let output = service.list_objects_v2(request).await.unwrap().output;
        let keys: Vec<String> = output
            .contents
            .unwrap_or_default()
            .into_iter()
            .filter_map(|object| object.key)
            .collect();
        assert_eq!(keys, vec!["imaging/scan-a", "imaging/scan-b"]);
    }

    async fn paged_request(
        service: &ArunaS3Service,
        user_access: &UserAccess,
        group_id: Ulid,
        token: Option<String>,
    ) -> S3Request<ListObjectsV2Input> {
        let scope = resolve_scope(
            &service.state,
            user_access,
            &bucket_permission_path(service.realm_id, group_id, service.node_id, "study"),
        )
        .await
        .unwrap();
        let mut extensions = Extensions::new();
        extensions.insert(user_access.clone());
        extensions.insert(test_bucket_info(group_id, user_access.user_identity));
        extensions.insert(scope);
        list_request(
            extensions,
            ListObjectsV2Input {
                bucket: "study".to_string(),
                max_keys: Some(1),
                continuation_token: token,
                ..Default::default()
            },
        )
    }

    #[tokio::test]
    async fn marker_stays_scoped() {
        // Paging one key at a time crosses keys the caller may not see, so no
        // token may name one and the last page must admit it is the last.
        let (_storage_dir, service, user_access, group_id) = subpath_node().await;
        let mut token = None;
        let mut keys: Vec<String> = Vec::new();

        for _ in 0..4 {
            let request = paged_request(&service, &user_access, group_id, token.clone()).await;
            let output = service.list_objects_v2(request).await.unwrap().output;
            keys.extend(
                output
                    .contents
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|object| object.key),
            );
            token = output.next_continuation_token;
            assert_eq!(output.is_truncated, Some(token.is_some()));
            let Some(encoded) = token.clone() else {
                break;
            };
            let decoded = ArunaS3Service::decode_list_token(Some(&encoded))
                .unwrap()
                .unwrap();
            let head = BlobHeadKey::from_bytes(&decoded.last_key).unwrap();
            assert!(head.key.starts_with("imaging/"), "leaked {}", head.key);
        }

        assert_eq!(keys, vec!["imaging/scan-a", "imaging/scan-b"]);
        assert!(token.is_none());
    }

    #[tokio::test]
    async fn exact_listing_scope() {
        let (_storage_dir, service, mut user_access, group_id) = subpath_node().await;
        grant_group_owner(&service, &user_access, group_id).await;
        seed_materialized_keys(
            &service.state.storage_handle,
            "study",
            &["imaging", "imaging/private/key"],
            user_access.user_identity,
            UNIX_EPOCH,
        )
        .await;
        let root = bucket_permission_path(service.realm_id, group_id, service.node_id, "study");
        for (suffix, expected) in [("/imaging", vec!["imaging"]), ("", vec![])] {
            user_access.path_restrictions = Some(vec![PathRestriction {
                pattern: format!("{root}{suffix}"),
                permission: Permission::READ,
            }]);
            for delimiter in [None, Some("/".to_string())] {
                let mut request = subpath_request(&service, &user_access, group_id, None).await;
                request.input.delimiter = delimiter;
                let output = service.list_objects_v2(request).await.unwrap().output;
                let keys: Vec<_> = output
                    .contents
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|object| object.key)
                    .collect();
                assert_eq!(keys, expected);
                assert!(output.common_prefixes.unwrap_or_default().is_empty());
            }
        }
    }

    #[tokio::test]
    async fn denied_listing_keys() {
        let (_storage_dir, service, mut user_access, group_id) = subpath_node().await;
        seed_materialized_keys(
            &service.state.storage_handle,
            "study",
            &[
                "imaging/private/key",
                "imaging/public/secret.txt",
                "imaging/public/open.txt",
                "imaging/hidden/secret.txt",
            ],
            user_access.user_identity,
            UNIX_EPOCH,
        )
        .await;
        let root = bucket_permission_path(service.realm_id, group_id, service.node_id, "study");
        user_access.path_restrictions = Some(
            [
                ("imaging/**", Permission::READ),
                ("imaging/private/**", Permission::DENY),
                ("imaging/*/secret*", Permission::DENY),
            ]
            .into_iter()
            .map(|(key, permission)| PathRestriction {
                pattern: format!("{root}/{key}"),
                permission,
            })
            .collect(),
        );
        let mut request = subpath_request(&service, &user_access, group_id, None).await;
        request.input.delimiter = None;
        let output = service.list_objects_v2(request).await.unwrap().output;
        let keys: Vec<_> = output
            .contents
            .unwrap_or_default()
            .into_iter()
            .filter_map(|object| object.key)
            .collect();
        assert_eq!(
            keys,
            vec![
                "imaging/public/open.txt",
                "imaging/scan-a",
                "imaging/scan-b"
            ]
        );
        let request = subpath_request(&service, &user_access, group_id, Some("imaging/")).await;
        let output = service.list_objects_v2(request).await.unwrap().output;
        let prefixes: Vec<_> = output
            .common_prefixes
            .unwrap_or_default()
            .into_iter()
            .filter_map(|prefix| prefix.prefix)
            .collect();
        assert_eq!(prefixes, vec!["imaging/public/"]);
    }

    #[tokio::test]
    async fn scoped_scan_bounded() {
        let (_storage_dir, service, user_access, group_id) = subpath_node().await;
        let keys: Vec<_> = (0..101).map(|index| format!("hidden/{index:03}")).collect();
        seed_materialized_keys(
            &service.state.storage_handle,
            "study",
            &keys.iter().map(String::as_str).collect::<Vec<_>>(),
            user_access.user_identity,
            UNIX_EPOCH,
        )
        .await;
        let request = paged_request(&service, &user_access, group_id, None).await;
        let error = service.list_objects_v2(request).await.unwrap_err();
        assert_eq!(error.code(), &s3s::S3ErrorCode::SlowDown);
    }

    #[tokio::test]
    async fn prefix_pages_progress() {
        let (_storage_dir, service, user_access, group_id) = subpath_node().await;
        let keys: Vec<_> = (0..101)
            .map(|index| format!("imaging/{index:03}/key"))
            .collect();
        seed_materialized_keys(
            &service.state.storage_handle,
            "study",
            &keys.iter().map(String::as_str).collect::<Vec<_>>(),
            user_access.user_identity,
            UNIX_EPOCH,
        )
        .await;
        let mut request = subpath_request(&service, &user_access, group_id, Some("imaging/")).await;
        request.input.max_keys = Some(0);
        let output = service.list_objects_v2(request).await.unwrap().output;
        assert!(output.contents.unwrap_or_default().is_empty());
        assert!(output.common_prefixes.unwrap_or_default().is_empty());
        assert!(output.next_continuation_token.is_none());
        let mut token = None;
        let mut prefixes = Vec::new();
        for _ in 0..3 {
            let mut request =
                subpath_request(&service, &user_access, group_id, Some("imaging/")).await;
            request.input.max_keys = None;
            request.input.continuation_token = token.take();
            let output = service.list_objects_v2(request).await.unwrap().output;
            prefixes.extend(
                output
                    .common_prefixes
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|prefix| prefix.prefix),
            );
            token = output.next_continuation_token;
            if token.is_none() {
                break;
            }
        }
        assert!(token.is_none());
        assert_eq!(
            prefixes,
            (0..101)
                .map(|index| format!("imaging/{index:03}/"))
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn prefix_token_precedence() {
        let (_storage_dir, service, user_access, group_id) = subpath_node().await;
        seed_materialized_keys(
            &service.state.storage_handle,
            "study",
            &["imaging/a/key", "imaging/b/key"],
            user_access.user_identity,
            UNIX_EPOCH,
        )
        .await;
        let mut request = subpath_request(&service, &user_access, group_id, Some("imaging/")).await;
        request.input.start_after = Some("imaging/z".to_string());
        let token =
            crate::s3::service::object::scoped_marker("study", Some("imaging/0"), None).unwrap();
        request.input.continuation_token =
            ArunaS3Service::encode_list_token(token.as_ref()).unwrap();
        let output = service.list_objects_v2(request).await.unwrap().output;
        let prefixes: Vec<_> = output
            .common_prefixes
            .unwrap_or_default()
            .into_iter()
            .filter_map(|prefix| prefix.prefix)
            .collect();
        assert_eq!(prefixes, vec!["imaging/a/", "imaging/b/"]);
    }

    #[tokio::test]
    async fn subpath_refuses_sibling() {
        let (_storage_dir, service, user_access, group_id) = subpath_node().await;

        let request = subpath_request(&service, &user_access, group_id, Some("sequencing/")).await;
        let error = service.list_objects_v2(request).await.unwrap_err();
        assert_eq!(error.code(), &s3s::S3ErrorCode::AccessDenied);
    }

    #[tokio::test]
    async fn object_path_decides() {
        // Object reads stay authorized at their own path, inside and outside
        // the granted folder.
        let (_storage_dir, service, user_access, group_id) = subpath_node().await;
        let auth_context = AuthContext {
            user_id: user_access.user_identity,
            realm_id: service.realm_id,
            path_restrictions: None,
            session: None,
        };
        let object_path = |key: &str| {
            aruna_core::structs::object_permission_path(
                service.realm_id,
                group_id,
                service.node_id,
                "study",
                key,
            )
        };

        assert!(
            authorize(
                &service.state,
                service.realm_id,
                &auth_context,
                &object_path("imaging/scan-a"),
                &Permission::READ,
                PolicyRequestExtras::operation("s3.GetObject"),
            )
            .await
            .is_ok()
        );
        assert!(
            authorize(
                &service.state,
                service.realm_id,
                &auth_context,
                &object_path("sequencing/reads"),
                &Permission::READ,
                PolicyRequestExtras::operation("s3.GetObject"),
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn filters_bucket_scope() {
        assert_eq!(visible_buckets("").await, vec!["allowed"]);
    }

    #[tokio::test]
    async fn lists_prefix_scope() {
        // A credential scoped to a prefix inside a bucket must still see that
        // bucket, and only that one, in the listing.
        assert_eq!(visible_buckets("/logs/**").await, vec!["allowed"]);
    }

    fn test_bucket_info(group_id: Ulid, created_by: UserId) -> BucketInfo {
        BucketInfo {
            group_id,
            created_at: UNIX_EPOCH,
            created_by,
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
        }
    }

    async fn setup_copy_authorization(
        same_group: bool,
        public: bool,
    ) -> (TempDir, ArunaS3Service, UserAccess, BucketInfo) {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let context = Arc::new(DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });
        let realm_id = RealmId([36u8; 32]);
        let node_id = NodeId::from_bytes(&[0u8; 32]).unwrap();
        let credential_group_id = Ulid::generate();
        let source_group_id = if same_group {
            credential_group_id
        } else {
            Ulid::generate()
        };
        let user_access = test_user_access(credential_group_id, realm_id);
        let actor = Actor {
            node_id,
            user_id: user_access.user_identity,
            realm_id,
        };
        let mut source_auth = GroupAuthorizationDocument::default_group_doc(
            user_access.user_identity,
            realm_id,
            source_group_id,
        );
        if public {
            source_auth
                .roles
                .values_mut()
                .find(|role| role.name == "viewer")
                .unwrap()
                .assigned_users
                .insert(UserId::nil(realm_id));
        }

        let source_group = aruna_core::structs::Group {
            display_name: "source".to_string(),
            group_id: source_group_id,
            realm_id,
            owner: user_access.user_identity,
            roles: source_auth.roles.keys().copied().collect(),
        };

        write_realm_config(&storage_handle, realm_id, &actor).await;
        write_storage_value(
            &storage_handle,
            AUTH_KEYSPACE,
            realm_id.as_bytes().to_vec(),
            RealmAuthorizationDocument::default_realm_doc(realm_id)
                .to_bytes(&actor)
                .unwrap(),
        )
        .await;
        write_storage_value(
            &storage_handle,
            AUTH_KEYSPACE,
            source_group_id.to_bytes().to_vec(),
            source_auth.to_bytes(&actor).unwrap(),
        )
        .await;
        write_storage_value(
            &storage_handle,
            aruna_core::keyspaces::GROUP_KEYSPACE,
            source_group_id.to_bytes().to_vec(),
            source_group.to_bytes(&actor).unwrap(),
        )
        .await;
        write_storage_value(
            &storage_handle,
            S3_BUCKET_KEYSPACE,
            b"source".to_vec(),
            test_bucket_info(source_group_id, user_access.user_identity)
                .to_bytes()
                .unwrap(),
        )
        .await;

        let destination_info = test_bucket_info(credential_group_id, user_access.user_identity);
        (
            storage_dir,
            ArunaS3Service::new(context, realm_id, node_id).await,
            user_access,
            destination_info,
        )
    }

    fn test_copy_request<T>(
        input: T,
        user_access: UserAccess,
        bucket_info: BucketInfo,
    ) -> S3Request<T> {
        let mut extensions = Extensions::new();
        extensions.insert(user_access);
        extensions.insert(bucket_info);
        extensions.insert(PolicyRequestExtras::rest());
        S3Request {
            input,
            method: Method::PUT,
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
    async fn copy_scopes_authorization() {
        for (same_group, public, allowed) in [
            (true, false, true),
            (false, true, true),
            (false, false, false),
        ] {
            let (_storage_dir, service, user_access, bucket_info) =
                setup_copy_authorization(same_group, public).await;
            let input = CopyObjectInput::builder()
                .bucket("destination".to_string())
                .key("copied".to_string())
                .copy_source(s3s::dto::CopySource::parse("source/object").unwrap())
                .metadata_directive(Some(MetadataDirective::from_static(
                    MetadataDirective::REPLACE,
                )))
                .build()
                .unwrap();
            let error = service
                .copy_object(test_copy_request(input, user_access, bucket_info))
                .await
                .unwrap_err();
            // Passing source authorization reaches the copy itself, which fails
            // on the absent source object.
            let expected = if allowed {
                S3ErrorCode::NoSuchKey
            } else {
                S3ErrorCode::AccessDenied
            };
            assert_eq!(
                *error.code(),
                expected,
                "same_group={same_group}, public={public}"
            );
        }
    }

    #[tokio::test]
    async fn part_copy_scopes() {
        for (same_group, public, allowed) in [
            (true, false, true),
            (false, true, true),
            (false, false, false),
        ] {
            let (_storage_dir, service, user_access, bucket_info) =
                setup_copy_authorization(same_group, public).await;
            let input = UploadPartCopyInput::builder()
                .bucket("destination".to_string())
                .key("copied".to_string())
                .copy_source(s3s::dto::CopySource::parse("source/object").unwrap())
                .upload_id(Ulid::generate().to_string())
                .part_number(1)
                .build()
                .unwrap();
            let error = service
                .upload_part_copy(test_copy_request(input, user_access, bucket_info))
                .await
                .unwrap_err();
            let expected = if allowed {
                S3ErrorCode::NoSuchUpload
            } else {
                S3ErrorCode::AccessDenied
            };
            assert_eq!(
                *error.code(),
                expected,
                "same_group={same_group}, public={public}"
            );
        }
    }

    #[tokio::test]
    async fn source_policy_denied() {
        // A group deny policy on the source path blocks the copy even when RBAC
        // and destination write are allowed.
        let storage_dir = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let context = Arc::new(DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });
        let realm_id = RealmId([37u8; 32]);
        let node_id = NodeId::from_bytes(&[0u8; 32]).unwrap();
        let group_id = Ulid::generate();
        let user_access = test_user_access(group_id, realm_id);
        let actor = Actor {
            node_id,
            user_id: user_access.user_identity,
            realm_id,
        };
        let mut source_auth = GroupAuthorizationDocument::default_group_doc(
            user_access.user_identity,
            realm_id,
            group_id,
        );
        source_auth.policies = vec![aruna_core::request_policy::RequestPolicy {
            policy_id: Ulid::generate(),
            name: "no-reads".to_string(),
            kind: aruna_core::request_policy::PolicyKind::Deny,
            when: None,
            expression: "permission == 'read'".to_string(),
            enabled: true,
        }];
        let source_group = aruna_core::structs::Group {
            display_name: "src".to_string(),
            group_id,
            realm_id,
            owner: user_access.user_identity,
            roles: source_auth.roles.keys().copied().collect(),
        };
        write_realm_config(&storage_handle, realm_id, &actor).await;
        write_storage_value(
            &storage_handle,
            AUTH_KEYSPACE,
            realm_id.as_bytes().to_vec(),
            RealmAuthorizationDocument::default_realm_doc(realm_id)
                .to_bytes(&actor)
                .unwrap(),
        )
        .await;
        write_storage_value(
            &storage_handle,
            AUTH_KEYSPACE,
            group_id.to_bytes().to_vec(),
            source_auth.to_bytes(&actor).unwrap(),
        )
        .await;
        write_storage_value(
            &storage_handle,
            aruna_core::keyspaces::GROUP_KEYSPACE,
            group_id.to_bytes().to_vec(),
            source_group.to_bytes(&actor).unwrap(),
        )
        .await;
        write_storage_value(
            &storage_handle,
            S3_BUCKET_KEYSPACE,
            b"source".to_vec(),
            test_bucket_info(group_id, user_access.user_identity)
                .to_bytes()
                .unwrap(),
        )
        .await;

        let service = ArunaS3Service::new(context, realm_id, node_id).await;
        let input = CopyObjectInput::builder()
            .bucket("destination".to_string())
            .key("copied".to_string())
            .copy_source(s3s::dto::CopySource::parse("source/object").unwrap())
            .metadata_directive(Some(MetadataDirective::from_static(
                MetadataDirective::REPLACE,
            )))
            .build()
            .unwrap();
        let bucket_info = test_bucket_info(group_id, user_access.user_identity);
        let error = service
            .copy_object(test_copy_request(input, user_access, bucket_info))
            .await
            .unwrap_err();
        assert_eq!(*error.code(), S3ErrorCode::AccessDenied);
    }

    #[tokio::test]
    async fn delete_uses_context() {
        // The per-object policy must see the real query parameters and allowlisted
        // headers the access hook captured, not an empty operation-only context.
        let storage_dir = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let context = Arc::new(DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });
        let realm_id = RealmId([38u8; 32]);
        let node_id = NodeId::from_bytes(&[0u8; 32]).unwrap();
        let group_id = Ulid::generate();
        let user_access = test_user_access(group_id, realm_id);
        let actor = Actor {
            node_id,
            user_id: user_access.user_identity,
            realm_id,
        };
        let mut auth_doc = GroupAuthorizationDocument::default_group_doc(
            user_access.user_identity,
            realm_id,
            group_id,
        );
        auth_doc.policies = vec![aruna_core::request_policy::RequestPolicy {
            policy_id: Ulid::generate(),
            name: "context-deny".to_string(),
            kind: aruna_core::request_policy::PolicyKind::Deny,
            when: None,
            expression: "('mode' in params && params['mode'] == 'purge') \
                || ('x-amz-meta-env' in headers && headers['x-amz-meta-env'] == 'prod')"
                .to_string(),
            enabled: true,
        }];
        let group = aruna_core::structs::Group {
            display_name: "g".to_string(),
            group_id,
            realm_id,
            owner: user_access.user_identity,
            roles: auth_doc.roles.keys().copied().collect(),
        };
        write_realm_config(&storage_handle, realm_id, &actor).await;
        write_storage_value(
            &storage_handle,
            AUTH_KEYSPACE,
            realm_id.as_bytes().to_vec(),
            RealmAuthorizationDocument::default_realm_doc(realm_id)
                .to_bytes(&actor)
                .unwrap(),
        )
        .await;
        write_storage_value(
            &storage_handle,
            AUTH_KEYSPACE,
            group_id.to_bytes().to_vec(),
            auth_doc.to_bytes(&actor).unwrap(),
        )
        .await;
        write_storage_value(
            &storage_handle,
            aruna_core::keyspaces::GROUP_KEYSPACE,
            group_id.to_bytes().to_vec(),
            group.to_bytes(&actor).unwrap(),
        )
        .await;

        let service = ArunaS3Service::new(context, realm_id, node_id).await;
        let param_extras = PolicyRequestExtras {
            operation: "s3.DeleteObjects".to_string(),
            params: std::collections::BTreeMap::from([("mode".to_string(), "purge".to_string())]),
            headers: std::collections::BTreeMap::new(),
            body: None,
        };
        let header_extras = PolicyRequestExtras {
            operation: "s3.DeleteObjects".to_string(),
            params: std::collections::BTreeMap::new(),
            headers: std::collections::BTreeMap::from([(
                "x-amz-meta-env".to_string(),
                "prod".to_string(),
            )]),
            body: None,
        };

        for extras in [param_extras, header_extras] {
            let delete = s3s::dto::Delete {
                objects: vec![
                    s3s::dto::ObjectIdentifier {
                        key: "a".to_string(),
                        ..Default::default()
                    },
                    s3s::dto::ObjectIdentifier {
                        key: "b".to_string(),
                        ..Default::default()
                    },
                ],
                quiet: None,
            };
            let input = DeleteObjectsInput::builder()
                .bucket("bucket".to_string())
                .delete(delete)
                .build()
                .unwrap();
            let mut extensions = Extensions::new();
            extensions.insert(user_access.clone());
            extensions.insert(DeleteObjectsBody::default());
            extensions.insert(extras);
            let mut headers = HeaderMap::new();
            headers.insert("content-md5", "1B2M2Y8AsgTpgAmY7PhCfg==".parse().unwrap());
            let request = S3Request {
                input,
                method: Method::POST,
                uri: Uri::from_static("/"),
                headers,
                extensions,
                credentials: None,
                region: None,
                service: None,
                trailing_headers: None,
            };
            let output = service.delete_objects(request).await.unwrap();
            let errors = output.output.errors.unwrap_or_default();
            assert_eq!(errors.len(), 2);
            assert!(
                errors
                    .iter()
                    .all(|error| error.code.as_deref() == Some("AccessDenied"))
            );
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
            let version_id = Ulid::generate();
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

    fn list_request(
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
    async fn delimiter_groups_results() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let context = Arc::new(DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });
        let realm_id = RealmId([2u8; 32]);
        let group_id = Ulid::generate();
        let created_by = UserId::local(Ulid::generate(), realm_id);
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

        let req = list_request(
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
    async fn delimiter_paginates() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let context = Arc::new(DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });
        let realm_id = RealmId([3u8; 32]);
        let group_id = Ulid::generate();
        let created_by = UserId::local(Ulid::generate(), realm_id);
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

            let req = list_request(
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

    #[test]
    fn marker_skips_group() {
        // A delimited marker names an already returned common prefix, so the
        // next page must skip that whole group instead of re-listing it.
        let token = marker_continuation_token("bucket", Some("a/"), None, Some("/"))
            .unwrap()
            .expect("delimited marker must resume past its group");
        assert_eq!(token.last_common_prefix.as_deref(), Some("a/"));
        assert_eq!(
            BlobHeadKey::from_bytes(&token.last_key).unwrap().key,
            "a/".to_string()
        );
    }

    #[test]
    fn plain_marker_preserved() {
        assert!(
            marker_continuation_token("bucket", Some("a/b.txt"), None, None)
                .unwrap()
                .is_none()
        );
        assert!(
            marker_continuation_token("bucket", None, None, Some("/"))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn next_marker_group() {
        let token = ListObjectsV2ContinuationToken {
            last_key: BlobHeadKey::object_prefix("bucket", "a/z.txt").unwrap(),
            last_common_prefix: Some("a/".to_string()),
        };
        assert_eq!(next_marker_of(&token).as_deref(), Some("a/"));

        let token = ListObjectsV2ContinuationToken {
            last_key: BlobHeadKey::object_prefix("bucket", "b.txt").unwrap(),
            last_common_prefix: None,
        };
        assert_eq!(next_marker_of(&token).as_deref(), Some("b.txt"));
    }

    #[test]
    fn marker_rescues_page() {
        // An undelimited page that filtered every key it scanned is truncated with
        // no `<Key>` to resume from, so it must carry the token-derived marker.
        let token = ListObjectsV2ContinuationToken {
            last_key: BlobHeadKey::object_prefix("bucket", "b.txt").unwrap(),
            last_common_prefix: None,
        };

        assert_eq!(
            next_marker_for(None, Some(&token), true).as_deref(),
            Some("b.txt")
        );
        // A page with contents resumes from its last key: S3 sends no NextMarker.
        assert_eq!(next_marker_for(None, Some(&token), false), None);
        // A delimited page may end on a common prefix, so it always reports one.
        assert_eq!(
            next_marker_for(Some("/"), Some(&token), false).as_deref(),
            Some("b.txt")
        );
        // A complete listing has nothing to resume.
        assert_eq!(next_marker_for(None, None, true), None);
    }

    #[tokio::test]
    async fn prefix_page_complete() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let context = Arc::new(DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });
        let realm_id = RealmId([33u8; 32]);
        let group_id = Ulid::generate();
        let created_by = UserId::local(Ulid::generate(), realm_id);

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

        let req = list_request(
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
    async fn listing_counts_entries() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let context = Arc::new(DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });
        let realm_id = RealmId([34u8; 32]);
        let group_id = Ulid::generate();
        let created_by = UserId::local(Ulid::generate(), realm_id);

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
            .list_objects_v2(list_request(
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
            .list_objects_v2(list_request(
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
    async fn large_group_collapses() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let context = Arc::new(DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });
        let realm_id = RealmId([4u8; 32]);
        let group_id = Ulid::generate();
        let created_by = UserId::local(Ulid::generate(), realm_id);
        let created_at = UNIX_EPOCH + Duration::from_secs(1);

        let service = ArunaS3Service::new(
            context.clone(),
            realm_id,
            NodeId::from_bytes(&[0u8; 32]).unwrap(),
        )
        .await;

        // Seed 305 keys under "dir/" so the delimiter collapses them into
        // one prefix; the scan seeks past the group instead of paging it.
        for i in 0..305 {
            let key = format!("dir/key_{:04}", i);
            let version_id = Ulid::generate();
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

        let req = list_request(
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
    async fn reference_returns_metadata() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let context = Arc::new(DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });
        let realm_id = RealmId([5u8; 32]);
        let group_id = Ulid::generate();
        let created_by = UserId::local(Ulid::generate(), realm_id);
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

        let version_id = Ulid::generate();
        write_reference_metadata(
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
    async fn zero_max_honored() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let context = Arc::new(DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });
        let realm_id = RealmId([6u8; 32]);
        let group_id = Ulid::generate();
        let created_by = UserId::local(Ulid::generate(), realm_id);
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

        let req = list_request(
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
    async fn start_after_prefixes() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let context = Arc::new(DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });
        let realm_id = RealmId([7u8; 32]);
        let group_id = Ulid::generate();
        let created_by = UserId::local(Ulid::generate(), realm_id);
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

        let req = list_request(
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
    async fn listing_requires_access() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let context = Arc::new(DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });
        let realm_id = RealmId([8u8; 32]);
        let service =
            ArunaS3Service::new(context, realm_id, NodeId::from_bytes(&[0u8; 32]).unwrap()).await;

        let req = list_request(
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
    async fn max_keys_validated() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let context = Arc::new(DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });
        let realm_id = RealmId([35u8; 32]);
        let group_id = Ulid::generate();
        let created_by = UserId::local(Ulid::generate(), realm_id);

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
        let req = list_request(extensions.clone(), input.clone());
        let output = service.list_objects_v2(req).await.unwrap().output;
        assert_eq!(output.max_keys, Some(1000));
        assert_eq!(output.key_count, Some(2));

        let req = list_request(
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
    async fn listing_applies_encoding() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let context = Arc::new(DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });
        let realm_id = RealmId([36u8; 32]);
        let group_id = Ulid::generate();
        let created_by = UserId::local(Ulid::generate(), realm_id);

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

        let req = list_request(
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
    async fn fetch_owner_group() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let context = Arc::new(DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });
        let realm_id = RealmId([37u8; 32]);
        let group_id = Ulid::generate();
        let created_by = UserId::local(Ulid::generate(), realm_id);

        let service =
            ArunaS3Service::new(context, realm_id, NodeId::from_bytes(&[0u8; 32]).unwrap()).await;

        seed_materialized_keys(&storage_handle, "bucket", &["a"], created_by, UNIX_EPOCH).await;

        let mut extensions = Extensions::new();
        extensions.insert(test_user_access(group_id, realm_id));
        extensions.insert(test_bucket_info(group_id, created_by));

        let req = list_request(
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
