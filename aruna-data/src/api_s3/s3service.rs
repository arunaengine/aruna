use crate::api_s3::auth::UserAccess;
use crate::controller::controller::Controller;
use crate::error::ArunaDataError;
use crate::io::io_handler::ObjectInfo;
use crate::io::io_handler::tables::{LOCATION_DB_NAME, PATH_LOCATION_DB_NAME};
use crate::util::opendal::create_paths;
use crate::util::s3::{Destination, ReplicationTask};
use crate::IOHandler;
use aruna_storage::storage::lmdb::LmdbStore;
use aruna_storage::storage::store::Store;
use futures_util::TryStreamExt;
use s3s::dto::{
    CreateBucketInput, CreateBucketOutput, DeleteBucketReplicationInput,
    DeleteBucketReplicationOutput, GetBucketReplicationInput, GetBucketReplicationOutput,
    GetObjectInput, GetObjectOutput, PutBucketReplicationInput, PutBucketReplicationOutput,
    PutObjectInput, PutObjectOutput, StreamingBlob,
};
use s3s::{S3, S3Request, S3Response, S3Result, s3_error};
use std::fmt::Debug;
use std::path::Path;
use tracing::{debug, error, warn};

//TODO: Multipart --> Store parts, concatenate on finish

pub struct ArunaS3Service<St>
where
    for<'a> St: Store<'a> + 'static,
{
    controller: Controller<St>,
}

impl<St> Debug for ArunaS3Service<St>
where
    for<'a> St: Store<'a>,
{
    #[tracing::instrument(level = "trace", skip(self, f))]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArunaS3Service").finish()
    }
}

impl<St> ArunaS3Service<St>
where
    for<'a> St: Store<'a> + 'static,
{
    #[tracing::instrument(level = "trace", skip(controller))]
    pub async fn new(controller: Controller<St>) -> Self {
        ArunaS3Service { controller }
    }
}

#[async_trait::async_trait]
impl<St> S3 for ArunaS3Service<St>
where
    for<'a> St: Store<'a> + 'static,
{
    #[tracing::instrument(err)]
    #[allow(clippy::blocks_in_conditions)]
    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        debug!("Received GET Request: {:#?}", req);
        // Extract access check result
        let UserAccess { group_id, .. } =
            req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
                error!(error = "Missing user context");
                s3_error!(UnexpectedContent, "Missing user context")
            })?;

        // Extend provided path
        let frontend_path = format!("{}/{}/{}", group_id, req.input.bucket, req.input.key);

        // Fetch object info
        let store_clone = self.controller.io_handler.store.clone();
        let info = tokio::task::spawn_blocking(move || {
            let read_txn = store_clone.create_txn(false)?;
            let hash = store_clone
                .get(&read_txn, PATH_LOCATION_DB_NAME, frontend_path.as_bytes())?
                .ok_or_else(|| ArunaDataError::NotFound("No such key".to_string()))?;
            let info_raw = store_clone
                .get(&read_txn, LOCATION_DB_NAME, &hash)?
                .ok_or_else(|| ArunaDataError::NotFound("No such key".to_string()))?;
            let info: ObjectInfo = postcard::from_bytes(&*info_raw)?;

            Ok::<ObjectInfo, ArunaDataError>(info)
        })
        .await
        .map_err(|e| s3_error!(InternalError, "{}", e))??;

        // Create backend storage operator
        let operator = self
            .controller
            .io_handler
            .get_operator(&info.storage_root)
            .await?;
        //.map_err(|e| s3_error!(InternalError, "{}", e))?;

        // Fetch reader stream
        let filename = Path::new(&info.storage_path)
            .file_name()
            .ok_or_else(|| s3_error!(InvalidKeyPath, "Path is not a file"))?
            .to_str()
            .ok_or_else(|| s3_error!(InternalError, "String conversion failed"))?;

        let stream = IOHandler::<LmdbStore>::read_data(&operator, &info.storage_path)
            .await
            .map_err(|e| s3_error!(InternalError, "{}", e))?;

        let body = Some(StreamingBlob::wrap(stream.map_err(|e| {
            error!("Unable to wrap reader stream: {}", e);
            s3_error!(InternalError, "Internal processing error: {e}")
        })));

        //TODO: Set more response fields (?)
        let output = GetObjectOutput {
            body,
            content_disposition: Some(format!(r#"attachment;filename="{}""#, filename)),
            content_length: Some(info.file_size as i64),
            last_modified: None,
            version_id: None,
            checksum_sha256: Some(info.file_hashes.sha256),
            e_tag: Some(info.file_hashes.md5),
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }

    #[tracing::instrument(err, skip(self, req))]
    #[allow(clippy::blocks_in_conditions)]
    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        debug!("Received PUT Request: {:#?}", req);
        // Extract access check result
        let UserAccess {
            user_id, group_id, ..
        } = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;

        // Create paths
        let origin_path = format!("{}/{}", req.input.bucket, req.input.key);
        let (maybe_frontend_path, backend_path) =
            create_paths(&origin_path, None, &group_id, false)
                .map_err(|e| s3_error!(InternalError, "{}", e))?;
        let frontend_path = maybe_frontend_path
            .ok_or_else(|| s3_error!(InternalError, "Frontend path creation failed"))?;

        // Check if frontend path is already occupied
        let store_clone = self.controller.io_handler.store.clone();
        let frontend_path_clone = frontend_path.clone();
        let exists = tokio::task::spawn_blocking(move || {
            let mut read_txn = store_clone.create_txn(false)?;

            // Try fetch content hash associated with frontend path
            let res = store_clone.get(
                &mut read_txn,
                PATH_LOCATION_DB_NAME,
                frontend_path_clone.as_bytes(),
            )?;

            Ok::<bool, anyhow::Error>(res.is_some())
        })
        .await
        .map_err(|e| s3_error!(InternalError, "{}", e))?
        .map_err(|e| s3_error!(InternalError, "{}", e))?;

        if exists {
            warn!(
                "Frontend path: {} is already occupied and will be overwritten.",
                frontend_path
            )
        }

        // Stream data into backend
        let object_info = match req.input.body {
            None => {
                error!("Empty body is not allowed");
                return Err(s3_error!(InvalidRequest, "Empty body is not allowed"));
            }
            Some(data) => {
                let (hash, info) = self
                    .controller
                    .io_handler
                    .handle_incoming_data_stream(
                        user_id,
                        self.controller.network.get_realm_key(),
                        group_id,
                        frontend_path,
                        backend_path,
                        data,
                    )
                    .await
                    .map_err(|e| s3_error!(InternalError, "{}", e))?;
                self.controller
                    .network
                    .store(hash)
                    .await
                    .map_err(|e| s3_error!(InternalError, "{}", e))?;

                info
            }
        };

        if exists {
            //TODO: Update metadata version field?
        }

        //TODO: Set other response fields?
        // - Version ?
        let inner_response = PutObjectOutput {
            e_tag: Some(object_info.file_hashes.md5),
            size: Some(object_info.file_size as i64),
            checksum_sha256: Some(object_info.file_hashes.sha256),
            ..Default::default()
        };
        Ok(S3Response::new(inner_response))
    }

    #[tracing::instrument(err)]
    #[allow(clippy::blocks_in_conditions)]
    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        todo!("Add bucket path to permission handler");
        todo!("Add bucket mapping to database?");
    }

    #[allow(clippy::blocks_in_conditions)]
    async fn put_bucket_replication(
        &self,
        req: S3Request<PutBucketReplicationInput>,
    ) -> S3Result<S3Response<PutBucketReplicationOutput>> {
        let UserAccess { group_id, .. } =
            req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
                error!(error = "Missing user context");
                s3_error!(UnexpectedContent, "Missing user context")
            })?;
        let replication_policy = req.input.replication_configuration;
        for rule in replication_policy.rules {
            let distribution_strategy = aruna_task::DistributionStrategy::LimitedRealm(2);
            let retry_strategy = aruna_task::RetryStrategy::Forever;
            let destination = Destination::from_arn(rule.destination.bucket)?;

            let bucket_path = format!("{}/{}", group_id, req.input.bucket);

            todo!("Check if path/bucket exists?");

            let replication_task = ReplicationTask {
                source_bucket: todo!("Only bucket replication"),
                source_filter: todo!("skip filters for now"),
                target: todo!(),
                existing_object_replication: rule
                    .existing_object_replication
                    .map(|status| match status.status.as_str() {
                        "Enabled" => true,
                        "Disabled" => false,
                        _ => false,
                    })
                    .unwrap_or(false),
            };
            let payload = postcard::to_allocvec(&replication_task).unwrap();
            self.controller
                .task_handler
                .register_task(
                    distribution_strategy,
                    retry_strategy,
                    todo!("self.executor_idx"),
                    payload,
                    None,
                )
                .await
                .map_err(|e| {
                    error!("{e}");
                    s3_error!(InternalError, "Could not register replication task")
                })?;
            //self.controller.task_handler.register_task(rule);
        }
        todo!()
    }

    #[tracing::instrument(err)]
    #[allow(clippy::blocks_in_conditions)]
    async fn get_bucket_replication(
        &self,
        req: S3Request<GetBucketReplicationInput>,
    ) -> S3Result<S3Response<GetBucketReplicationOutput>> {
        todo!()
    }

    #[tracing::instrument(err)]
    #[allow(clippy::blocks_in_conditions)]
    async fn delete_bucket_replication(
        &self,
        req: S3Request<DeleteBucketReplicationInput>,
    ) -> S3Result<S3Response<DeleteBucketReplicationOutput>> {
        Err(s3_error!(
            NotImplemented,
            "Deleting bucket replications is not implemented"
        ))
    }
}
