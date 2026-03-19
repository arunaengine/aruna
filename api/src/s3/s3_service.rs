use crate::s3::util::{convert_input, to_base64};
use aruna_core::NodeId;
use aruna_core::structs::{AuthContext, BucketInfo, Permission, RealmId, UserAccess};
use aruna_operations::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::s3::create_bucket::{CreateBucketError, CreateBucketOperation};
use aruna_operations::s3::delete_bucket::{DeleteBucketError, DeleteBucketOperation};
use aruna_operations::s3::delete_object::{DeleteObjectInput as DOI, DeleteObjectOperation};
use aruna_operations::s3::get_object::{GetObjectInput as GOI, GetObjectOperation};
use aruna_operations::s3::list_buckets::{ListBucketsInput as LBI, ListBucketsOperation};
use aruna_operations::s3::put_object::{PutObjectConfig, PutObjectOperation};
use s3s::dto::{
    Bucket, CreateBucketInput, CreateBucketOutput, DeleteBucketInput, DeleteBucketOutput,
    DeleteObjectInput, DeleteObjectOutput, ETag, GetObjectInput, GetObjectOutput, ListBucketsInput,
    ListBucketsOutput, PutObjectInput, PutObjectOutput, StreamingBlob,
};
use s3s::{S3, S3Request, S3Response, S3Result, s3_error};
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
                    realm_id: user_access.user_identity.realm_key.clone(),
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

        let input = convert_input(req.input)?;
        let config = PutObjectConfig {
            user_id: user_access.user_identity.user_id,
            group_id: bucket_info
                .as_ref()
                .map(|bucket_info| bucket_info.group_id)
                .unwrap_or(user_access.group_id),
            request: input,
            exists: false,
        };
        let operation = PutObjectOperation::new(config);

        if let Some(Ok(location)) = drive(operation, &self.state)
            .await
            .map_err(|err| s3_error!(InternalError, "{}", err.to_string()))?
        {
            debug!(?location);
            Ok(S3Response::new(PutObjectOutput {
                e_tag: Some(ETag::Strong(to_base64(
                    location.hashes.get("md5").ok_or_else(|| {
                        error!(error = "Missing MD5 hash");
                        s3_error!(InternalError, "Missing MD5 hash")
                    })?,
                ))),
                size: Some(location.blob_size as i64),
                ..Default::default()
            }))
        } else {
            Err(s3_error!(InternalError, "Failed to process PUT request"))
        }
    }

    #[tracing::instrument(err)]
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

        let operation = GetObjectOperation::new(GOI {
            bucket: req.input.bucket,
            key: req.input.key,
            range: None,
            group_id: bucket_info
                .as_ref()
                .map(|bucket_info| bucket_info.group_id)
                .unwrap_or(user_access.group_id),
            user_identity: user_access.user_identity,
        });

        if let Ok(Some(Ok(blob_stream))) = drive(operation, &self.state).await {
            let wut = StreamingBlob::wrap(blob_stream);

            Ok(S3Response::new(GetObjectOutput {
                body: Some(wut),
                ..Default::default()
            }))
        } else {
            //TODO: Better error handling
            Err(s3_error!(InternalError, "Failed to process GET request"))
        }
    }

    #[tracing::instrument(err)]
    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        debug!("Received DELETE Request: {:#?}", req);

        if req.input.version_id.is_some() {
            return Err(s3_error!(
                NotImplemented,
                "Deleting explicit object versions is not implemented yet"
            ));
        }

        let user_access = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;

        let operation = DeleteObjectOperation::new(DOI {
            bucket: req.input.bucket,
            key: req.input.key,
            deleted_by: user_access.user_identity.user_id,
        });

        match drive(operation, &self.state)
            .await
            .map_err(|err| s3_error!(InternalError, "{}", err.to_string()))?
        {
            Some(Ok(result)) => Ok(S3Response::new(DeleteObjectOutput {
                delete_marker: Some(true),
                version_id: Some(result.version_id.to_string()),
                ..Default::default()
            })),
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
