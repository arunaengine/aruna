use crate::s3::checksum::{
    ApplyChecksums, ChecksumSelection, checksum_mismatch_error, checksum_mode_enabled,
    encode_checksums, parse_upload_checksum_request,
};
use crate::s3::util::{
    convert_input, multipart_checksum_type_from_s3, parse_completed_part,
    parse_multipart_checksum_hint, parse_multipart_part_number, parse_upload_id, parse_version_id,
    s3_checksum_type_from_multipart, to_base64,
};
use aruna_core::NodeId;
use aruna_core::stream::BackendStream;
use aruna_core::structs::checksum::HASH_MD5;
use aruna_core::structs::{AuthContext, BucketInfo, Permission, RealmId, UserAccess};
use aruna_operations::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::s3::abort_multipart_upload::{
    AbortMultipartUploadError, AbortMultipartUploadInput as AMUI, AbortMultipartUploadOperation,
};
use aruna_operations::s3::complete_multipart_upload::{
    CompleteMultipartUploadError, CompleteMultipartUploadInput as CMUI,
    CompleteMultipartUploadOperation,
};
use aruna_operations::s3::create_bucket::{CreateBucketError, CreateBucketOperation};
use aruna_operations::s3::create_multipart_upload::{
    CreateMultipartUploadInput as CMPI, CreateMultipartUploadOperation,
};
use aruna_operations::s3::delete_bucket::{DeleteBucketError, DeleteBucketOperation};
use aruna_operations::s3::delete_object::{
    DeleteObjectError, DeleteObjectInput as DOI, DeleteObjectOperation,
};
use aruna_operations::s3::get_object::{GetObjectError, GetObjectInput as GOI, GetObjectOperation};
use aruna_operations::s3::head_object::{
    HeadObjectError, HeadObjectInput as HOI, HeadObjectOperation,
};
use aruna_operations::s3::list_buckets::{ListBucketsInput as LBI, ListBucketsOperation};
use aruna_operations::s3::put_object::{PutObjectConfig, PutObjectError, PutObjectOperation};
use aruna_operations::s3::upload_part::{
    UploadPartError, UploadPartInput as UPI, UploadPartOperation,
};
use s3s::dto::{
    AbortMultipartUploadInput, AbortMultipartUploadOutput, Bucket, CompleteMultipartUploadInput,
    CompleteMultipartUploadOutput, CreateBucketInput, CreateBucketOutput,
    CreateMultipartUploadInput, CreateMultipartUploadOutput, DeleteBucketInput, DeleteBucketOutput,
    DeleteObjectInput, DeleteObjectOutput, ETag, GetObjectAttributesInput,
    GetObjectAttributesOutput, GetObjectInput, GetObjectOutput, HeadObjectInput, HeadObjectOutput,
    ListBucketsInput, ListBucketsOutput, PutObjectInput, PutObjectOutput, StreamingBlob,
    UploadPartInput, UploadPartOutput,
};
use s3s::{S3, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use std::fmt::Debug;
use std::sync::Arc;
use std::time::SystemTime;
use tracing::{debug, error, warn};

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
                    user_id: user_access.user_identity.user_id,
                    realm_id: user_access.user_identity.user_id.realm_id,
                    path_restrictions: None,
                },
                path: format!(
                    "/{}/g/{}/data/{}/{bucket}",
                    self.realm_id, bucket_info.group_id, self.node_id
                ),
                required_permission: Permission::READ,
            }),
            &self.state,
        )
        .await
        .map_err(|err| s3_error!(InternalError, "{}", err.to_string()))
    }
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
                created_by: user_access.user_identity.user_id,
            },
        );

        match drive(operation, &self.state).await {
            Ok(Some(Ok(_))) => Ok(S3Response::new(CreateBucketOutput::default())),
            Ok(Some(Err(CreateBucketError::BucketAlreadyExists)))
            | Err(CreateBucketError::BucketAlreadyExists) => {
                Err(s3_error!(BucketAlreadyOwnedByYou, "Bucket already exists"))
            }
            Ok(Some(Err(err))) | Err(err) => Err(s3_error!(InternalError, "{}", err.to_string())),
            Ok(None) => Err(s3_error!(InternalError, "Failed to create bucket")),
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
        .map_err(|err| s3_error!(InternalError, "{}", err.to_string()))?;

        if let Some(Ok(result)) = result {
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
        } else {
            Err(s3_error!(InternalError, "Failed to list buckets"))
        }
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

        let input = convert_input(req.input)?;
        let config = PutObjectConfig {
            user_id: user_access.user_identity.user_id,
            group_id: bucket_info
                .as_ref()
                .map(|bucket_info| bucket_info.group_id)
                .unwrap_or(user_access.group_id),
            realm_id: self.realm_id,
            node_id: self.node_id,
            request: input,
            expected_checksums: checksum_request.expected,
            checksum_type: Some(checksum_request.checksum_type.as_str().to_string()),
            exists: false,
        };
        let operation = PutObjectOperation::new(config);

        match drive(operation, &self.state).await {
            Ok(Some(Ok(result))) => {
                debug!(?result);
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
                    checksum_request.checksum_type,
                ));
                Ok(S3Response::new(output))
            }
            Ok(Some(Err(PutObjectError::ChecksumMismatch(algorithm))))
            | Err(PutObjectError::ChecksumMismatch(algorithm)) => {
                warn!(algorithm, "Checksum mismatch during PutObject");
                Err(checksum_mismatch_error())
            }
            Ok(Some(Err(PutObjectError::MissingExpectedChecksum(algorithm))))
            | Err(PutObjectError::MissingExpectedChecksum(algorithm)) => {
                warn!(algorithm, "Missing checksum during PutObject");
                Err(s3_error!(InternalError, "Missing stored checksum"))
            }
            Ok(Some(Err(err))) | Err(err) => Err(s3_error!(InternalError, "{}", err.to_string())),
            Ok(None) => Err(s3_error!(InternalError, "Failed to process PUT request")),
        }
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
            created_by: user_access.user_identity.user_id,
            checksum_hint: checksum_hint.clone(),
        });

        match drive(operation, &self.state).await {
            Ok(Some(Ok(result))) => Ok(S3Response::new(CreateMultipartUploadOutput {
                bucket: Some(req.input.bucket),
                key: Some(req.input.key),
                upload_id: Some(result.record.upload_id.to_string()),
                checksum_algorithm: req.input.checksum_algorithm,
                checksum_type: checksum_hint
                    .map(|hint| s3_checksum_type_from_multipart(hint.checksum_type)),
                ..Default::default()
            })),
            Ok(Some(Err(err))) | Err(err) => Err(s3_error!(InternalError, "{}", err.to_string())),
            Ok(None) => Err(s3_error!(
                InternalError,
                "Failed to create multipart upload"
            )),
        }
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
            created_by: user_access.user_identity.user_id,
            compressed: false,
            encrypted: req.input.sse_customer_algorithm.is_some()
                || req.input.sse_customer_key.is_some()
                || req.input.sse_customer_key_md5.is_some(),
            expected_checksums: checksum_request.expected.clone(),
        });

        match drive(operation, &self.state).await {
            Ok(Some(Ok(result))) => {
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
            Ok(Some(Err(UploadPartError::NoSuchUpload))) | Err(UploadPartError::NoSuchUpload) => {
                Err(s3_error!(
                    NoSuchUpload,
                    "The specified upload does not exist."
                ))
            }
            Ok(Some(Err(UploadPartError::UploadTargetMismatch)))
            | Err(UploadPartError::UploadTargetMismatch) => Err(s3_error!(
                NoSuchUpload,
                "The specified upload does not exist."
            )),
            Ok(Some(Err(UploadPartError::ChecksumMismatch(algorithm))))
            | Err(UploadPartError::ChecksumMismatch(algorithm)) => {
                warn!(algorithm, "Checksum mismatch during UploadPart");
                Err(checksum_mismatch_error())
            }
            Ok(Some(Err(err))) | Err(err) => Err(s3_error!(InternalError, "{}", err.to_string())),
            Ok(None) => Err(s3_error!(InternalError, "Failed to upload part")),
        }
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
            created_by: user_access.user_identity.user_id,
        });

        match drive(operation, &self.state).await {
            Ok(Some(Ok(result))) => {
                let mut output = CompleteMultipartUploadOutput {
                    bucket: Some(req.input.bucket),
                    key: Some(req.input.key),
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
                Ok(S3Response::new(output))
            }
            Ok(Some(Err(CompleteMultipartUploadError::NoSuchUpload)))
            | Err(CompleteMultipartUploadError::NoSuchUpload)
            | Ok(Some(Err(CompleteMultipartUploadError::UploadTargetMismatch)))
            | Err(CompleteMultipartUploadError::UploadTargetMismatch) => Err(s3_error!(
                NoSuchUpload,
                "The specified upload does not exist."
            )),
            Ok(Some(Err(CompleteMultipartUploadError::InvalidPart)))
            | Err(CompleteMultipartUploadError::InvalidPart) => Err(s3_error!(
                InvalidPart,
                "One or more of the specified parts could not be found."
            )),
            Ok(Some(Err(CompleteMultipartUploadError::InvalidPartOrder)))
            | Err(CompleteMultipartUploadError::InvalidPartOrder) => Err(s3_error!(
                InvalidPartOrder,
                "The list of parts was not in ascending order."
            )),
            Ok(Some(Err(CompleteMultipartUploadError::ChecksumMismatch(algorithm))))
            | Err(CompleteMultipartUploadError::ChecksumMismatch(algorithm)) => {
                warn!(
                    algorithm,
                    "Checksum mismatch during CompleteMultipartUpload"
                );
                Err(checksum_mismatch_error())
            }
            Ok(Some(Err(CompleteMultipartUploadError::PartEtagMismatch)))
            | Err(CompleteMultipartUploadError::PartEtagMismatch) => Err(s3_error!(
                InvalidPart,
                "The part ETag did not match the uploaded part."
            )),
            Ok(Some(Err(err))) | Err(err) => Err(s3_error!(InternalError, "{}", err.to_string())),
            Ok(None) => Err(s3_error!(
                InternalError,
                "Failed to complete multipart upload"
            )),
        }
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

        match drive(operation, &self.state).await {
            Ok(Some(Ok(()))) => Ok(S3Response::new(AbortMultipartUploadOutput::default())),
            Ok(Some(Err(AbortMultipartUploadError::NoSuchUpload)))
            | Err(AbortMultipartUploadError::NoSuchUpload)
            | Ok(Some(Err(AbortMultipartUploadError::UploadTargetMismatch)))
            | Err(AbortMultipartUploadError::UploadTargetMismatch) => Err(s3_error!(
                NoSuchUpload,
                "The specified upload does not exist."
            )),
            Ok(Some(Err(err))) | Err(err) => Err(s3_error!(InternalError, "{}", err.to_string())),
            Ok(None) => Err(s3_error!(InternalError, "Failed to abort multipart upload")),
        }
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
        let version_id = parse_version_id(req.input.version_id)?;

        let operation = GetObjectOperation::new(GOI {
            bucket: req.input.bucket,
            key: req.input.key,
            version_id,
            range: None,
            group_id: bucket_info
                .as_ref()
                .map(|bucket_info| bucket_info.group_id)
                .unwrap_or(user_access.group_id),
            user_identity: user_access.user_identity,
        });

        match drive(operation, &self.state).await {
            Ok(Some(Ok(result))) => {
                let version_id = result.version_id;
                let hashes = result.location.hashes.clone();
                let content = StreamingBlob::wrap(result.blob);
                let mut output = GetObjectOutput {
                    body: Some(content),
                    version_id: version_id.map(|version_id| version_id.to_string()),
                    ..Default::default()
                };
                if checksum_mode_enabled(&req.headers) {
                    output.apply_checksums(encode_checksums(
                        &hashes,
                        ChecksumSelection::AllStored,
                        s3_checksum_type_from_multipart(result.checksum_type),
                    ));
                }

                Ok(S3Response::new(output))
            }
            Ok(Some(Err(GetObjectError::NoSuchVersion))) | Err(GetObjectError::NoSuchVersion) => {
                Err(s3_error!(
                    NoSuchVersion,
                    "The specified version does not exist."
                ))
            }
            Ok(Some(Err(GetObjectError::DeleteMarker))) | Err(GetObjectError::DeleteMarker) => {
                Err(s3_error!(
                    MethodNotAllowed,
                    "The specified version is a delete marker."
                ))
            }
            Ok(Some(Err(GetObjectError::NoSuchKey))) | Err(GetObjectError::NoSuchKey) => {
                Err(s3_error!(NoSuchKey, "The specified key does not exist."))
            }
            Ok(Some(Err(err))) | Err(err) => Err(s3_error!(InternalError, "{}", err)),
            Ok(None) => Err(s3_error!(InternalError, "Failed to process GET request")),
        }
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

        match drive(operation, &self.state).await {
            Ok(Some(Ok(result))) => {
                let mut output = HeadObjectOutput {
                    content_length: Some(result.location.blob_size as i64),
                    e_tag: result
                        .location
                        .hashes
                        .get(HASH_MD5)
                        .map(|value| ETag::Strong(to_base64(value))),
                    version_id: result.version_id.map(|version_id| version_id.to_string()),
                    last_modified: Some(result.location.created_at.into()),
                    ..Default::default()
                };

                if checksum_mode_enabled(&req.headers) {
                    output.apply_checksums(encode_checksums(
                        &result.location.hashes,
                        ChecksumSelection::AllStored,
                        s3_checksum_type_from_multipart(result.checksum_type),
                    ));
                }

                Ok(S3Response::new(output))
            }
            Err(HeadObjectError::NoSuchVersion) => Err(s3_error!(
                NoSuchVersion,
                "The specified version does not exist."
            )),
            Err(HeadObjectError::DeleteMarker) => Err(s3_error!(
                MethodNotAllowed,
                "The specified version is a delete marker."
            )),
            Err(HeadObjectError::NoSuchKey) => {
                Err(s3_error!(NoSuchKey, "The specified key does not exist."))
            }
            Ok(Some(Err(err))) | Err(err) => Err(s3_error!(InternalError, "{}", err)),
            Ok(None) => Err(s3_error!(InternalError, "Failed to process HEAD request")),
        }
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

        let operation = DeleteObjectOperation::new(DOI {
            bucket: req.input.bucket,
            key: req.input.key,
            version_id,
            deleted_by: user_access.user_identity.user_id,
        });

        match drive(operation, &self.state)
            .await
            .map_err(|err| s3_error!(InternalError, "{}", err.to_string()))?
        {
            Some(Ok(result)) => Ok(S3Response::new(DeleteObjectOutput {
                delete_marker: Some(result.delete_marker),
                version_id: Some(result.version_id.to_string()),
                ..Default::default()
            })),
            Some(Err(err @ DeleteObjectError::NoSuchVersion)) => {
                Err(s3_error!(NoSuchVersion, "{}", err))
            }
            Some(Err(err)) => Err(s3_error!(InternalError, "{}", err.to_string())),
            None => Err(s3_error!(InternalError, "Failed to process DELETE request")),
        }
    }

    #[tracing::instrument(err)]
    async fn delete_bucket(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        debug!("Received DELETE BUCKET Request: {:#?}", req);

        match drive(DeleteBucketOperation::new(req.input.bucket), &self.state).await {
            Ok(Some(Ok(()))) => Ok(S3Response::new(DeleteBucketOutput::default())),
            Ok(Some(Err(DeleteBucketError::NotFound))) | Err(DeleteBucketError::NotFound) => Err(
                s3_error!(NoSuchBucket, "The specified bucket does not exist."),
            ),
            Ok(Some(Err(DeleteBucketError::NotEmpty))) | Err(DeleteBucketError::NotEmpty) => {
                Err(s3_error!(
                    BucketNotEmpty,
                    "The bucket you tried to delete is not empty."
                ))
            }
            Ok(Some(Err(err))) | Err(err) => Err(s3_error!(InternalError, "{}", err)),
            Ok(None) => Err(s3_error!(InternalError, "Failed to delete bucket")),
        }
    }
}
