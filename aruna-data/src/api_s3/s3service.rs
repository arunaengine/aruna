use crate::IOHandler;
use crate::api_s3::auth::{ModifyAccess, UserAccess};
use crate::controller::controller::Controller;
use crate::error::ArunaDataError;
use crate::io::io_handler::tables::{
    BUCKET_STATE_DB_NAME, LOCATION_DB_NAME, PATH_LOCATION_DB_NAME,
};
use crate::io::io_handler::{BucketState, MultipartUpload, ObjectInfo, SPECIAL_BUCKET};
use crate::util::opendal::create_paths;
use crate::util::s3::{Destination, ReplicationRule};
use aruna_permission::paths::PathBuilder;
use aruna_storage::storage::lmdb::LmdbStore;
use aruna_storage::storage::store::Store;
use futures_util::TryStreamExt;
use s3s::dto::{
    CompleteMultipartUploadInput, CompleteMultipartUploadOutput, CreateBucketInput,
    CreateBucketOutput, CreateMultipartUploadInput, CreateMultipartUploadOutput,
    DeleteBucketReplicationInput, DeleteBucketReplicationOutput, GetBucketReplicationInput,
    GetBucketReplicationOutput, GetObjectInput, GetObjectOutput, PutBucketPolicyInput,
    PutBucketPolicyOutput, PutBucketReplicationInput, PutBucketReplicationOutput, PutObjectInput,
    PutObjectOutput, StreamingBlob, UploadPartInput, UploadPartOutput,
};
use s3s::{S3, S3Error, S3Request, S3Response, S3Result, s3_error};
use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap};
use std::fmt::Debug;
use std::path::Path;
use std::str::FromStr;
use std::time::SystemTime;
use tracing::{debug, error, warn};
use ulid::Ulid;

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

        // Fetch object info
        let store_clone = self.controller.io_handler.store.clone();
        let bucket = format!("{}/{}", group_id, req.input.bucket);
        let key = req.input.key;
        let info = tokio::task::spawn_blocking(move || {
            let read_txn = store_clone.create_txn(false)?;
            let frontend_path = format!("{}/{}", bucket, key);

            let hash = if &bucket != SPECIAL_BUCKET {
                store_clone
                    .get(&read_txn, PATH_LOCATION_DB_NAME, frontend_path.as_bytes())?
                    .ok_or_else(|| ArunaDataError::NotFound("No such key".to_string()))?
            } else {
                let hash =
                    blake3::Hash::from_str(&key).map_err(|_e| ArunaDataError::ConversionError {
                        from: "String".to_string(),
                        to: "blake3::Hash".to_string(),
                    })?;
                Cow::from(hash.as_bytes().to_vec())
            };

            // Extend provided path
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
            e_tag: Some(info.file_hashes.blake3.to_string()),
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
        let (maybe_frontend_path, backend_path) =
            create_paths(Some(&req.input.key), &req.input.bucket, &group_id, false)
                .map_err(|e| s3_error!(InternalError, "{}", e))?;
        let frontend_path = maybe_frontend_path
            .ok_or_else(|| s3_error!(InvalidKeyPath, "Invalid path provided"))?;

        // Check if frontend path is already occupied
        let (bucket_path, _) = create_paths(None, &req.input.bucket, &group_id, false)
            .map_err(|e| s3_error!(InternalError, "{}", e))?;
        let bucket_path =
            bucket_path.ok_or_else(|| s3_error!(InvalidBucketName, "Invalid bucket"))?;
        let store_clone = self.controller.io_handler.store.clone();
        let frontend_path_clone = frontend_path.clone();
        let exists = tokio::task::spawn_blocking(move || {
            let read_txn = store_clone.create_txn(false)?;

            // Try fetch content hash associated with frontend bucket
            let res = store_clone.get(&read_txn, BUCKET_STATE_DB_NAME, bucket_path.as_bytes())?;

            if !res.is_some() {
                return Err(ArunaDataError::InvalidParameter {
                    name: "bucket".to_string(),
                    error: "Bucket does not exist".to_string(),
                });
            };

            // Try fetch content hash associated with frontend bucket+key
            let res = store_clone.get(
                &read_txn,
                PATH_LOCATION_DB_NAME,
                frontend_path_clone.as_bytes(),
            )?;

            let exists = res.is_some();
            store_clone.commit(read_txn)?;

            Ok::<bool, ArunaDataError>(exists)
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
            e_tag: Some(object_info.file_hashes.blake3.to_string()),
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
        debug!("Received CreateBucket Request: {:#?}", req);
        // Extract access check result
        let UserAccess {
            user_id, group_id, ..
        } = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;

        // Create paths
        let (maybe_frontend_path, backend_path) =
            create_paths(None, &req.input.bucket, &group_id, false)
                .map_err(|e| s3_error!(InternalError, "{}", e))?;
        let frontend_path = maybe_frontend_path
            .ok_or_else(|| s3_error!(InternalError, "Frontend path creation failed"))?;

        // Check if frontend path is already occupied
        let self_clone = self.controller.clone();
        let frontend_path_clone = frontend_path.clone();
        let backend_bucket = self
            .controller
            .io_handler
            .eval_suitable_bucket()
            .await?
            .clone();
        let backend_bucket_clone = backend_bucket.clone();
        let backend_path_clone = backend_path.clone();
        let bucket = req.input.bucket.clone();
        let state = tokio::task::spawn_blocking(move || {
            let mut write_txn = self_clone
                .io_handler
                .store
                .create_txn(true)
                .map_err(|e| s3_error!(InternalError, "{}", e))?;
            // Try fetch content hash associated with frontend path
            let res = self_clone
                .io_handler
                .store
                .get(
                    &write_txn,
                    BUCKET_STATE_DB_NAME,
                    frontend_path_clone.as_bytes(),
                )
                .map_err(|e| s3_error!(InternalError, "{}", e))?;
            if res.is_some() {
                return Err(s3_error!(
                    BucketAlreadyExists,
                    "Bucket {} already exists",
                    bucket
                ));
            };

            let permission_path = PathBuilder::new()
                .realm_id(self_clone.network.get_realm_key())
                .group_data_bucket(group_id, req.input.bucket.clone())
                .build()
                .map_err(|e| s3_error!(InternalError, "{}", e.to_string()))?;

            // TODO: This currently just randomly creates folders in s3
            // - [ ] Only create backend folder if one bucket config is set or
            //       only create dir when used with filesystem backend
            let info = BucketState {
                created_by: user_id,
                created_at: SystemTime::now(),
                id: Ulid::new(),
                name: req.input.bucket,
                merkle_tree: BTreeSet::new(),
                root_hash: None,
                backend_bucket: Some(backend_bucket_clone.clone()),
                backend_path: Some(backend_path_clone.clone()),
            };

            self_clone.io_handler.store_bucket(
                info.clone(),
                frontend_path,
                permission_path,
                &mut write_txn,
            )?;

            // todo!("Add bucket path to permission handler");
            // todo!("Add bucket mapping to database?");

            self_clone
                .io_handler
                .store
                .commit(write_txn)
                .map_err(|e| s3_error!(InternalError, "{}", e))?;

            Ok::<BucketState, S3Error>(info)
        })
        .await
        .map_err(|e| s3_error!(InternalError, "{}", e))?
        .map_err(|e| s3_error!(InternalError, "{}", e))?;

        let operator = self
            .controller
            .io_handler
            .get_operator(&backend_bucket)
            .await?;
        let location = format!("{}/{}/", backend_bucket, backend_path);
        operator
            .create_dir(&location)
            .await
            .map_err(|e| s3_error!(InternalError, "{}", e.to_string()))?;

        self.controller
            .network
            .store(blake3::hash(state.id.to_bytes().as_slice()))
            .await
            .map_err(|e| s3_error!(InternalError, "{}", e))?;

        let inner_response = CreateBucketOutput {
            // TODO:
            // - [] Location should be ignored when multi bucket config is set
            location: Some(location),
        };
        Ok(S3Response::new(inner_response))
    }

    #[allow(clippy::blocks_in_conditions)]
    async fn put_bucket_replication(
        &self,
        req: S3Request<PutBucketReplicationInput>,
    ) -> S3Result<S3Response<PutBucketReplicationOutput>> {
        let UserAccess {
            group_id, user_id, ..
        } = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;

        // Check if bucket exists
        let (bucket_path, _) = create_paths(None, &req.input.bucket, &group_id, false)
            .map_err(|e| s3_error!(InternalError, "{}", e))?;
        let bucket_path =
            bucket_path.ok_or_else(|| s3_error!(InvalidBucketName, "Invalid bucket"))?;
        let store_clone = self.controller.io_handler.store.clone();
        let bucket_clone = bucket_path.clone();
        tokio::task::spawn_blocking(move || {
            let read_txn = store_clone.create_txn(false)?;

            // Try fetch content hash associated with frontend bucket
            let res = store_clone.get(&read_txn, BUCKET_STATE_DB_NAME, bucket_clone.as_bytes())?;

            if !res.is_some() {
                return Err(ArunaDataError::InvalidParameter {
                    name: "bucket".to_string(),
                    error: "Bucket does not exist".to_string(),
                });
            };

            store_clone.commit(read_txn)?;

            Ok::<(), ArunaDataError>(())
        })
        .await
        .map_err(|e| s3_error!(InternalError, "{}", e))?
        .map_err(|e| s3_error!(InternalError, "{}", e))?;

        // Parse policies and rules
        let replication_policy = req.input.replication_configuration;

        // TODO:
        // - [ ] Start replication task for each rule
        for rule in replication_policy.rules {
            let distribution_strategy = aruna_task::DistributionStrategy::LimitedRealm(2);
            let retry_strategy = aruna_task::RetryStrategy::Forever;
            let destination = Destination::from_arn(rule.destination.bucket)?;
            let target_node = if let Some(endpoint_id) = &destination.endpoint_id {
                let nodes = self.controller.network.get_realm_nodes().await?;
                nodes
                    .into_iter()
                    .find(|nodes| &nodes.node_id.to_string() == endpoint_id)
                    .ok_or_else(|| {
                        s3_error!(
                            EndpointNotFound,
                            "Endpoint with id {} not found",
                            endpoint_id
                        )
                    })?
            } else {
                return Err(s3_error!(
                    NotImplemented,
                    "Same node replication is not implemented"
                ));
            };

            let replication_rule = ReplicationRule {
                user_id: user_id.clone(),
                group_id,
                source_bucket: bucket_path.clone(),
                source_filter: match rule.filter {
                    Some(r) => Some(r.try_into()?),
                    None => None,
                },
                target: destination,
                existing_object_replication: rule
                    .existing_object_replication
                    .map(|status| match status.status.as_str() {
                        "Enabled" => true,
                        "Disabled" => false,
                        _ => false,
                    })
                    .unwrap_or(false),
            };

            let replication_tasks = self
                .controller
                .io_handler
                .store_replication_rule(
                    target_node,
                    self.controller.network.get_realm_key(),
                    replication_rule,
                )
                .await?;

            // Register each object individually
            for task in &replication_tasks {
                let payload = postcard::to_allocvec(task).unwrap();
                self.controller
                    .task_handler
                    .register_task(
                        distribution_strategy.clone(),
                        retry_strategy.clone(),
                        self.controller.executor_idx,
                        payload,
                        None, // No backchannel needed here
                    )
                    .await
                    .map_err(|e| {
                        error!("{e}");
                        s3_error!(InternalError, "Could not register replication task")
                    })?;
            }
        }

        Ok(S3Response::new(PutBucketReplicationOutput {}))
    }

    #[tracing::instrument(err)]
    #[allow(clippy::blocks_in_conditions)]
    async fn get_bucket_replication(
        &self,
        _req: S3Request<GetBucketReplicationInput>,
    ) -> S3Result<S3Response<GetBucketReplicationOutput>> {
        // TODO:
        Err(s3_error!(
            NotImplemented,
            "Bucket replications are not implemented"
        ))
    }

    #[tracing::instrument(err)]
    #[allow(clippy::blocks_in_conditions)]
    async fn delete_bucket_replication(
        &self,
        _req: S3Request<DeleteBucketReplicationInput>,
    ) -> S3Result<S3Response<DeleteBucketReplicationOutput>> {
        // TODO:
        Err(s3_error!(
            NotImplemented,
            "Deleting bucket replications is not implemented"
        ))
    }

    #[allow(clippy::blocks_in_conditions)]
    async fn put_bucket_policy(
        &self,
        req: S3Request<PutBucketPolicyInput>,
    ) -> S3Result<S3Response<PutBucketPolicyOutput>> {
        return Err(s3_error!(
            NotImplemented,
            "Deleting bucket replications is not implemented"
        ));

        let UserAccess {
            group_id, user_id, ..
        } = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;

        // Check if bucket exists
        let (bucket_path, _) = create_paths(None, &req.input.bucket, &group_id, false)
            .map_err(|e| s3_error!(InternalError, "{}", e))?;
        let bucket_path =
            bucket_path.ok_or_else(|| s3_error!(InvalidBucketName, "Invalid bucket"))?;
        let store_clone = self.controller.io_handler.store.clone();
        let bucket_clone = bucket_path.clone();
        tokio::task::spawn_blocking(move || {
            let read_txn = store_clone.create_txn(false)?;

            // Try fetch content hash associated with frontend bucket
            let res = store_clone.get(&read_txn, BUCKET_STATE_DB_NAME, bucket_clone.as_bytes())?;

            if !res.is_some() {
                return Err(ArunaDataError::InvalidParameter {
                    name: "bucket".to_string(),
                    error: "Bucket does not exist".to_string(),
                });
            };

            store_clone.commit(read_txn)?;

            Ok::<(), ArunaDataError>(())
        })
        .await
        .map_err(|e| s3_error!(InternalError, "{}", e))?
        .map_err(|e| s3_error!(InternalError, "{}", e))?;

        // Parse policies and rules
        let bucket_policy: ModifyAccess = serde_json::from_str(&req.input.policy).map_err(|e| {
            error!("{}", e.to_string());
            s3_error!(InvalidPolicyDocument, "Invalid BucketPolicy provided")
        })?;

        let store_clone = self.controller.io_handler.store.clone();
        let bucket_clone = bucket_path.clone();
        tokio::task::spawn_blocking(move || {
            let mut wtxn = store_clone.create_txn(true)?;

            todo!("Mutate group permissions");

            store_clone.commit(wtxn)?;

            Ok::<(), ArunaDataError>(())
        })
        .await
        .map_err(|e| s3_error!(InternalError, "{}", e))?
        .map_err(|e| s3_error!(InternalError, "{}", e))?;

        todo!()
    }

    #[tracing::instrument(err, skip(self, req))]
    #[allow(clippy::blocks_in_conditions)]
    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        debug!("Received PUT Request: {:#?}", req);
        // Extract access check result
        let UserAccess {
            user_id, group_id, ..
        } = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;

        // Create paths
        let (maybe_frontend_path, backend_path) =
            create_paths(Some(&req.input.key), &req.input.bucket, &group_id, false)
                .map_err(|e| s3_error!(InternalError, "{}", e))?;
        let frontend_path = maybe_frontend_path
            .ok_or_else(|| s3_error!(InvalidKeyPath, "Invalid path provided"))?;

        // Check if frontend path is already occupied
        let (bucket_path, _) = create_paths(None, &req.input.bucket, &group_id, false)
            .map_err(|e| s3_error!(InternalError, "{}", e))?;
        let bucket_path =
            bucket_path.ok_or_else(|| s3_error!(InvalidBucketName, "Invalid bucket"))?;
        let bucket_path_clone = bucket_path.clone();
        let store_clone = self.controller.io_handler.store.clone();
        let frontend_path_clone = frontend_path.clone();
        let exists = tokio::task::spawn_blocking(move || {
            let read_txn = store_clone.create_txn(false)?;

            // Try fetch content hash associated with frontend bucket
            let res = store_clone.get(
                &read_txn,
                BUCKET_STATE_DB_NAME,
                bucket_path_clone.as_bytes(),
            )?;

            if !res.is_some() {
                return Err(ArunaDataError::InvalidParameter {
                    name: "bucket".to_string(),
                    error: "Bucket does not exist".to_string(),
                });
            };

            // Try fetch content hash associated with frontend bucket+key
            let res = store_clone.get(
                &read_txn,
                PATH_LOCATION_DB_NAME,
                frontend_path_clone.as_bytes(),
            )?;

            let exists = res.is_some();
            store_clone.commit(read_txn)?;

            Ok::<bool, ArunaDataError>(exists)
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

        let backend_bucket = self.controller.io_handler.eval_suitable_bucket().await?;
        let multipart = MultipartUpload {
            id: Ulid::new(),
            bucket: bucket_path,
            key: req.input.key,
            created_by: user_id,
            created_at: SystemTime::now(),
            staging: true,
            compressed: false,
            encrypted: false,
            partial: false,
            storage_root: backend_bucket,
            storage_path: backend_path,
            parts: HashMap::new(),
            finished: false,
            frontend_path: Some(frontend_path),
        };

        self.controller
            .io_handler
            .create_multipart_upload(multipart.clone())
            .await?;

        //TODO: Set other response fields?
        // - Version ?
        let inner_response = CreateMultipartUploadOutput {
            bucket: Some(multipart.bucket),
            key: Some(multipart.key),
            upload_id: Some(multipart.id.to_string()),
            ..Default::default()
        };
        Ok(S3Response::new(inner_response))
    }

    #[tracing::instrument(err, skip(self, req))]
    #[allow(clippy::blocks_in_conditions)]
    async fn upload_part(
        &self,
        req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        let UserAccess { .. } = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;

        let hash = match req.input.body {
            None => {
                error!("Empty body is not allowed");
                return Err(s3_error!(InvalidRequest, "Empty body is not allowed"));
            }
            Some(data) => {
                self.controller
                    .io_handler
                    .add_part(
                        Ulid::from_string(&req.input.upload_id)
                            .map_err(|_e| s3_error!(NoSuchUpload, "Invalid upload id provided"))?,
                        req.input.part_number as usize,
                        data,
                    )
                    .await?
            }
        };
        let inner_response = UploadPartOutput {
            e_tag: Some(hash.to_string()),
            ..Default::default()
        };
        Ok(S3Response::new(inner_response))
    }

    #[tracing::instrument(err, skip(self, req))]
    #[allow(clippy::blocks_in_conditions)]
    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let UserAccess {
            group_id, ..
        } = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;

        let parts = req
            .input
            .multipart_upload
            .ok_or_else(|| s3_error!(InvalidRequest, "No multipart filed specified"))?
            .parts
            .ok_or_else(|| s3_error!(InvalidRequest, "No parts specified"))?
            .into_iter()
            .map(|p| -> Result<(usize, String), ArunaDataError> {
                Ok((
                    p.part_number.ok_or_else(|| {
                        ArunaDataError::NotFound("Part number not found".to_string())
                    })? as usize,
                    p.e_tag
                        .ok_or_else(|| ArunaDataError::NotFound("Etag not found".to_string()))?,
                ))
            })
            .collect::<Result<Vec<(usize, String)>, ArunaDataError>>()?;

        let (bucket, key, etag) = self
            .controller
            .io_handler
            .finish_multipart_upload(
                self.controller.network.get_realm_key(),
                group_id,
                Ulid::from_string(&req.input.upload_id)
                    .map_err(|_e| s3_error!(InvalidRequest, "Invalid upload id provided"))?,
                parts,
            )
            .await?;

        self.controller
            .network
            .store(etag)
            .await
            .map_err(|e| s3_error!(InternalError, "{}", e))?;

        let inner_response = CompleteMultipartUploadOutput {
            bucket: Some(bucket),
            e_tag: Some(etag.to_string()),
            key: Some(key),
            ..Default::default()
        };
        Ok(S3Response::new(inner_response))
    }
}
