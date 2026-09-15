#![allow(clippy::result_large_err)]

mod attributes;
mod bucket;
mod copy;
mod listing;
mod multipart;
mod object;
mod response;

use self::attributes::{
    RequestedAttributes, attributes_checksum, attributes_output, attributes_parts, parse_max_parts,
    parse_part_marker,
};
use self::response::{attach_reference_refresh, object_metadata, reference_metadata_refresh};

use self::copy::{copy_object_response, copy_part_response, copy_source_conditions};
use self::listing::{
    ObjectListingPage, list_parts_output, list_uploads_output, marker_continuation_token,
    next_marker_for,
};
use self::multipart::parse_upload_marker;
use self::object::object_range_request;

use crate::s3::auth::map_authorize_error;
use crate::s3::browse::{IndexEntry, render_index, wants_index};
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
    map_checksum_type, parse_checksum_hint, parse_checksum_type, parse_completed_part,
    parse_copy_source, parse_part_number, parse_source_range, parse_upload_id, parse_version_id,
    reject_sse, validate_object_key,
};
use aruna_compute::session::TouchedObject;
use aruna_core::NodeId;
use aruna_core::stream::BackendStream;
use aruna_core::structs::checksum::HASH_MD5;
use aruna_core::structs::storage::replication::ArunaArn;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::{BucketInfo, UserAccess, object_permission_path};
use aruna_core::structs::storage::multipart::COMPLETION_DEADLINE_MS;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::execution::job::{RoCrateLimits, credential_job_id};
use aruna_core::structs::{SyncMode, SyncRelationship, SyncState, SyncStatusSnapshot};
use aruna_operations::auth::check_permissions::{
    CheckPermissionsConfig, CheckPermissionsOperation,
};
use aruna_operations::auth::request_authorization::{AuthorizeError, authorize};
use aruna_operations::auth::request_policy::PolicyRequestExtras;
use aruna_operations::driver::{
    DriverContext, bucket_snapshot, drive, drive_until, gate_context, now_ms, routing_snapshot,
};
use aruna_operations::s3::bucket::cors::{DeleteCorsOperation, GetCorsOperation, PutCorsOperation};
use aruna_operations::s3::bucket::create::CreateBucketOperation;
use aruna_operations::s3::bucket::delete::DeleteBucketOperation;
use aruna_operations::s3::bucket::get::GetBucketOperation;
use aruna_operations::s3::bucket::list::{ListBucketsInput as LBI, ListBucketsOperation};
use aruna_operations::s3::multipart::abort::{AbortUploadInput as AMUI, AbortUploadOperation};
use aruna_operations::s3::multipart::complete::{
    CompleteUploadInput as CMUI, CompleteUploadOperation,
};
use aruna_operations::s3::multipart::create::{
    CreateMultipartInput as CMPI, CreateMultipartOperation,
};
use aruna_operations::s3::multipart::part_copy::{
    PartCopyInput as UploadPartCopyData, upload_part_copy,
};
use aruna_operations::s3::multipart::part_upload::{UploadPartInput as UPI, UploadPartOperation};
use aruna_operations::s3::multipart::parts::{ListPartsInput as LPI, ListPartsOperation};
use aruna_operations::s3::multipart::uploads::{ListUploadsInput as LMUI, ListUploadsOperation};
use aruna_operations::s3::object::attributes::{
    GetAttributesInput as GOAI, GetAttributesOperation,
};
use aruna_operations::s3::object::copy::{
    CopyObjectInput as CopyObjectData, CopyReferences, copy_object,
};
use aruna_operations::s3::object::delete::{DeleteObjectInput as DOI, DeleteObjectOperation};
use aruna_operations::s3::object::delete_bulk::{
    BulkDeleteEntry, BulkDeleteInput as DOSI, delete_objects,
};
use aruna_operations::s3::object::get::{
    GetObjectInput as GOI, get_object_info, get_object_routed,
};
use aruna_operations::s3::object::head::{HeadObjectInput as HOI, HeadObjectOperation};
use aruna_operations::s3::object::list::{ListBucketInput as LOV2I, ListBucketOperation};
use aruna_operations::s3::object::put::{PutObjectConfig, PutObjectOperation};
use aruna_operations::s3::object::versions::{ListVersionsInput as LOVI, ListVersionsOperation};
use aruna_operations::sync::mirror_repair::{
    SyncMirrorIntent, kick_mirror_repair, stage_mirror_delete, stage_mirror_reconcile,
};
use aruna_operations::sync::sync_relationship::SyncRelationshipDirection;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use s3s::dto::{
    AbortMultipartUploadInput, AbortMultipartUploadOutput, Bucket, BucketVersioningStatus,
    ChecksumType, CompleteMultipartUploadInput, CompleteMultipartUploadOutput, CopyObjectInput,
    CopyObjectOutput, CreateBucketInput, CreateBucketOutput, CreateMultipartUploadInput,
    CreateMultipartUploadOutput, DeleteBucketCorsInput, DeleteBucketCorsOutput, DeleteBucketInput,
    DeleteBucketOutput, DeleteBucketReplicationInput, DeleteBucketReplicationOutput,
    DeleteObjectInput, DeleteObjectOutput, DeleteObjectsInput, DeleteObjectsOutput, ETag,
    EncodingType, Error as S3DeleteError, GetBucketCorsInput, GetBucketCorsOutput,
    GetBucketLocationInput, GetBucketLocationOutput, GetBucketReplicationInput,
    GetBucketReplicationOutput, GetBucketVersioningInput, GetBucketVersioningOutput,
    GetObjectAttributesInput, GetObjectAttributesOutput, GetObjectInput, GetObjectOutput,
    HeadBucketInput, HeadBucketOutput, HeadObjectInput, HeadObjectOutput, ListBucketsInput,
    ListBucketsOutput, ListMultipartUploadsInput, ListMultipartUploadsOutput,
    ListObjectVersionsInput, ListObjectVersionsOutput, ListObjectsInput, ListObjectsOutput,
    ListObjectsV2Input, ListObjectsV2Output, ListPartsInput, ListPartsOutput, MetadataDirective,
    Owner, PutBucketCorsInput, PutBucketCorsOutput, PutBucketReplicationInput,
    PutBucketReplicationOutput, PutBucketVersioningInput, PutBucketVersioningOutput,
    PutObjectInput, PutObjectOutput, StreamingBlob, UploadPartCopyInput, UploadPartCopyOutput,
    UploadPartInput, UploadPartOutput,
};
use s3s::{S3, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tracing::{Instrument, debug, error, warn};

const URL_ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

/// The ETag a reference answers with: the source's when it gave a usable one,
/// otherwise one derived from the observation, so listings and reads agree and
/// clients that require an ETag, such as mountpoint, keep working.
pub(super) fn reference_etag(metadata: &aruna_core::structs::execution::source_access::SourceMetadata) -> ETag {
    metadata
        .etag
        .as_deref()
        .and_then(|etag| ETag::from_str(etag).ok())
        .unwrap_or_else(|| ETag::Strong(hex::encode(&metadata.observation_fingerprint()[..16])))
}

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
impl ArunaS3Service {
    /// One page directly below a public folder for an unsigned browser request.
    /// The access layer authorized the folder; each entry is checked again for
    /// the Everyone principal, so a narrower rule never lists a private name.
    async fn folder_index(
        &self,
        extensions: &http::Extensions,
        group_id: aruna_core::types::GroupId,
        bucket: &str,
        prefix: &str,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let extras = extensions
            .get::<PolicyRequestExtras>()
            .cloned()
            .ok_or_else(|| s3_error!(InternalError, "Missing policy context"))?;
        let page = self
            .run_object_listing(
                LOV2I {
                    bucket: bucket.to_owned(),
                    group_id,
                    continuation_token: None,
                    max_keys: Some(ListObjectsV2Operation::DEFAULT_MAX_KEYS),
                    prefix: Some(prefix.to_owned()),
                    delimiter: Some("/".to_owned()),
                    start_after: None,
                },
                None,
                false,
                None,
            )
            .await?;
        let truncated = page.continuation_token.is_some();
        let folders = page
            .common_prefixes
            .into_iter()
            .filter_map(|folder| folder.prefix)
            .map(|key| (key, None, None));
        let objects = page.contents.into_iter().filter_map(|object| {
            object
                .key
                .map(|key| (key, object.size, object.last_modified))
        });
        let anonymous = AuthContext::anonymous(self.realm_id);
        let mut entries = Vec::new();
        for (key, size, modified) in folders.chain(objects) {
            let Some(name) = key.strip_prefix(prefix).filter(|name| !name.is_empty()) else {
                continue;
            };
            let path = aruna_core::structs::blob_object_permission_path(
                self.realm_id,
                group_id,
                self.node_id,
                bucket,
                &key,
            );
            match authorize(
                &self.state,
                self.realm_id,
                &anonymous,
                &path,
                &Permission::READ,
                extras.clone(),
            )
            .await
            {
                Ok(_) => entries.push(IndexEntry {
                    name: name.to_owned(),
                    size,
                    modified,
                }),
                Err(AuthorizeError::PermissionDenied | AuthorizeError::Policy(_)) => {}
                Err(error) => return Err(map_authorize_error(error)),
            }
        }

        let html = render_index(bucket, prefix, &entries, truncated);
        let output = GetObjectOutput {
            content_length: i64::try_from(html.len()).ok(),
            content_type: Some("text/html; charset=utf-8".to_owned()),
            body: Some(StreamingBlob::from(bytes::Bytes::from(html))),
            ..Default::default()
        };
        let mut response = S3Response::new(output);
        response
            .headers
            .insert(http::header::VARY, http::HeaderValue::from_static("Accept"));
        response.headers.insert(
            http::header::X_CONTENT_TYPE_OPTIONS,
            http::HeaderValue::from_static("nosniff"),
        );
        response.headers.insert(
            http::header::CONTENT_SECURITY_POLICY,
            http::HeaderValue::from_static("default-src 'none'; style-src 'unsafe-inline'"),
        );
        Ok(response)
    }
}

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
            .map_err(IntoS3Error::into_s3_error)?;

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

        drive(GetBucketOperation::new(req.input.bucket), &self.state)
            .await
            .map_err(IntoS3Error::into_s3_error)?;

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

        drive(GetBucketOperation::new(req.input.bucket), &self.state)
            .await
            .map_err(IntoS3Error::into_s3_error)?;

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
        .map_err(IntoS3Error::into_s3_error)?;

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
            PutCorsOperation::new(req.input.bucket.clone(), config),
            &self.state,
        )
        .await
        .map_err(IntoS3Error::into_s3_error)?;

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
        let config = drive(GetCorsOperation::new(req.input.bucket.clone()), &self.state)
            .await
            .map_err(IntoS3Error::into_s3_error)?;

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
            DeleteCorsOperation::new(req.input.bucket.clone()),
            &self.state,
        )
        .await
        .map_err(IntoS3Error::into_s3_error)?;

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
            None => ListBucketOperation::DEFAULT_MAX_KEYS,
            Some(max_keys) => usize::try_from(max_keys)
                .map_err(|_| s3_error!(InvalidArgument, "max-keys must be non-negative"))?
                .min(ListBucketOperation::DEFAULT_MAX_KEYS),
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
                utf8_percent_encode(&value, URL_ENCODE_SET).to_string()
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
            None => ListBucketOperation::DEFAULT_MAX_KEYS,
            Some(max_keys) => usize::try_from(max_keys)
                .map_err(|_| s3_error!(InvalidArgument, "max-keys must be non-negative"))?
                .min(ListBucketOperation::DEFAULT_MAX_KEYS),
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
                utf8_percent_encode(&value, URL_ENCODE_SET).to_string()
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
            .map_err(IntoS3Error::into_s3_error)?;
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
        let source_extras = req
            .extensions
            .get::<PolicyRequestExtras>()
            .cloned()
            .ok_or_else(|| {
                error!(error = "Missing policy context");
                s3_error!(InternalError, "Missing policy context")
            })?;
        let (source_bucket_info, source_auth_context) = self
            .authorize_copy_source(
                &user_access,
                source_bucket.clone(),
                &source_key,
                source_extras,
            )
            .await?;

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
                references: CopyReferences::Preserve,
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

        Ok(copy_object_response(result))
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
        let mut operation = CreateMultipartOperation::new(CMPI {
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
            .map_err(IntoS3Error::into_s3_error)?;

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
            .map_err(IntoS3Error::into_s3_error)?;
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
        let source_extras = req
            .extensions
            .get::<PolicyRequestExtras>()
            .cloned()
            .ok_or_else(|| {
                error!(error = "Missing policy context");
                s3_error!(InternalError, "Missing policy context")
            })?;
        let (source_bucket_info, source_auth_context) = self
            .authorize_copy_source(
                &user_access,
                source_bucket.clone(),
                &source_key,
                source_extras,
            )
            .await?;

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

        Ok(copy_part_response(result))
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
        let mut operation = CompleteUploadOperation::new(CMUI {
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
            let outcome = match drive_until(operation, &service.state, deadline).await {
                Ok(result) => {
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
        let operation = AbortUploadOperation::new(AMUI {
            bucket: req.input.bucket,
            key: req.input.key,
            upload_id,
            now_ms: now_ms(),
        });

        drive(operation, &self.state)
            .await
            .map_err(IntoS3Error::into_s3_error)?;

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
        if user_access.user_identity.is_nil() && wants_index(&req.headers, &req.input.key) {
            let group_id = bucket_info
                .as_ref()
                .map(|bucket_info| bucket_info.group_id)
                .unwrap_or(user_access.group_id);
            return self
                .folder_index(&req.extensions, group_id, &req.input.bucket, &req.input.key)
                .await;
        }
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
            .map_err(IntoS3Error::into_s3_error)?;
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

        let requested = RequestedAttributes::from_request(&req.input.object_attributes)?;
        let requested_part_number_marker = req.input.part_number_marker;
        let part_number_marker = parse_part_marker(requested_part_number_marker)?;
        let max_parts = parse_max_parts(req.input.max_parts)?;
        let version_id = parse_version_id(req.input.version_id)?;
        let bucket = req.input.bucket.clone();
        let key = req.input.key.clone();
        let group_id = req
            .extensions
            .get::<BucketInfo>()
            .map(|info| info.group_id)
            .unwrap_or(user_access.group_id);

        let result = drive(
            GetAttributesOperation::new(GOAI {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id,
                include_parts: requested.object_parts,
            }),
            &self.state,
        )
        .await
        .map_err(IntoS3Error::into_s3_error)?;

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

        let checksum = requested
            .checksum
            .then(|| attributes_checksum(remote_info.as_ref(), &result))
            .flatten();

        let object_parts = requested
            .object_parts
            .then(|| {
                attributes_parts(
                    &result,
                    requested_part_number_marker,
                    part_number_marker,
                    max_parts,
                )
            })
            .flatten();

        Ok(S3Response::new(attributes_output(
            requested,
            response_fields,
            &result,
            checksum,
            object_parts,
        )))
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
            .map_err(IntoS3Error::into_s3_error)?;

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
        .map_err(IntoS3Error::into_s3_error)?;

        Ok(list_parts_output(req.input, max_parts, result))
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
            None => ListUploadsOperation::DEFAULT_MAX_UPLOADS,
            Some(max_uploads) => usize::try_from(max_uploads)
                .map_err(|_| s3_error!(InvalidArgument, "max-uploads must be non-negative"))?
                .min(ListUploadsOperation::DEFAULT_MAX_UPLOADS),
        };

        let result = drive(
            ListUploadsOperation::new(LMUI {
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
        .map_err(IntoS3Error::into_s3_error)?;

        Ok(list_uploads_output(req.input, max_uploads, result))
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
            None => ListVersionsOperation::DEFAULT_MAX_KEYS,
            Some(max_keys) => usize::try_from(max_keys)
                .map_err(|_| s3_error!(InvalidArgument, "max-keys must be non-negative"))?
                .min(ListVersionsOperation::DEFAULT_MAX_KEYS),
        };
        let group_id = bucket_info
            .as_ref()
            .map(|bucket_info| bucket_info.group_id)
            .unwrap_or(user_access.group_id);

        let result = drive(
            ListVersionsOperation::new(LOVI {
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
        .map_err(IntoS3Error::into_s3_error)?;

        Ok(self.list_versions_output(req.input, group_id, max_keys, result))
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
            .map_err(IntoS3Error::into_s3_error)?;

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

            entries.push(BulkDeleteEntry {
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

        Ok(self
            .delete_objects_response(quiet, bucket, replication_auth, errors, outcomes)
            .await)
    }

    #[tracing::instrument(err, skip(self, req))]
    async fn delete_bucket(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        debug!(bucket = %req.input.bucket, "Received DELETE BUCKET Request");

        drive(DeleteBucketOperation::new(req.input.bucket), &self.state)
            .await
            .map_err(IntoS3Error::into_s3_error)?;

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
                            self.clear_mirror_repair(created, SyncMirrorIntent::Delete)
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
                            self.clear_mirror_repair(created, SyncMirrorIntent::Delete)
                                .await;
                        }
                    }
                }
                return Err(error);
            }
            self.clear_mirror_repair(relationship, SyncMirrorIntent::Reconcile)
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
                self.clear_mirror_repair(&relationship, SyncMirrorIntent::Delete)
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
                self.clear_mirror_repair(&relationship, SyncMirrorIntent::Delete)
                    .await;
            }
        }

        Ok(S3Response::new(DeleteBucketReplicationOutput::default()))
    }
}

#[cfg(test)]
mod tests;
