use crate::IOHandler;
use crate::api_s3::auth::UserAccess;
use crate::io::io_handler::{LOCATION_DB_NAME, ObjectInfo, PATH_LOCATION_DB_NAME};
use anyhow::Result;
use aruna_storage::storage::lmdb::LmdbStore;
use aruna_storage::storage::store::Store;
use futures_util::TryStreamExt;
use iroh::NodeId;
use s3s::dto::{GetObjectInput, GetObjectOutput, PutObjectInput, PutObjectOutput, StreamingBlob};
use s3s::{S3, S3Request, S3Response, S3Result, s3_error};
use std::fmt::Debug;
use std::path::Path;
use std::sync::Arc;
use tracing::{debug, error, warn};
use ulid::Ulid;

pub struct ArunaS3Service<St>
where
    for<'a> St: Store<'a> + 'static,
{
    backend: Arc<IOHandler<St>>,
    node_id: NodeId,
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
    #[tracing::instrument(level = "trace", skip(backend))]
    pub async fn new(backend: Arc<IOHandler<St>>, node_id: NodeId) -> Result<Self> {
        Ok(ArunaS3Service { backend, node_id })
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
        let store_clone = self.backend.store.clone();
        let info = tokio::task::spawn_blocking(move || {
            let read_txn = store_clone
                .create_txn(false)
                .map_err(|e| s3_error!(InternalError, "{}", e))?;
            let hash = store_clone
                .get(&read_txn, PATH_LOCATION_DB_NAME, frontend_path.as_bytes())
                .map_err(|e| s3_error!(InternalError, "{}", e))?
                .ok_or_else(|| s3_error!(NoSuchKey, "No such key"))?;
            let info_raw = store_clone
                .get(&read_txn, LOCATION_DB_NAME, &hash)
                .map_err(|e| s3_error!(InternalError, "{}", e))?
                .ok_or_else(|| s3_error!(NoSuchKey, "No such key"))?;
            let (info, _): (ObjectInfo, usize) =
                bincode::serde::decode_from_slice(&*info_raw, bincode::config::standard())
                    .map_err(|e| s3_error!(InternalError, "{}", e))?;

            Ok::<ObjectInfo, anyhow::Error>(info)
        })
        .await
        .map_err(|e| s3_error!(InternalError, "{}", e))?
        .map_err(|e| s3_error!(InternalError, "{}", e))?;

        // Fetch reader stream
        let filename = Path::new(&info.file_path)
            .file_name()
            .ok_or_else(|| s3_error!(InvalidKeyPath, "Path is not a file"))?
            .to_str()
            .ok_or_else(|| s3_error!(InternalError, "String conversion failed"))?;

        let stream = IOHandler::<LmdbStore>::read_data(&self.backend.operator, &info.file_path)
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
        let UserAccess { group_id, .. } =
            req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
                error!(error = "Missing user context");
                s3_error!(UnexpectedContent, "Missing user context")
            })?;

        // Create paths
        let path_ulid = Ulid::new();
        let backend_path = format!(
            "{}/{}/{}/{}/{}",
            self.node_id, group_id, path_ulid, req.input.bucket, req.input.key
        );
        let extended_frontend_path = format!("{}/{}/{}", group_id, req.input.bucket, req.input.key);

        // Check if frontend path is already occupied
        let store_clone = self.backend.store.clone();
        let frontend_path_clone = extended_frontend_path.clone();
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
                extended_frontend_path
            )
        }

        // Stream data into backend
        let object_info = match req.input.body {
            None => {
                error!("Empty body is not allowed");
                return Err(s3_error!(InvalidRequest, "Empty body is not allowed"));
            }
            Some(data) => self
                .backend
                .handle_incoming_data_stream(extended_frontend_path, backend_path, data)
                .await
                .map_err(|e| s3_error!(InternalError, "{}", e))?,
        };

        if exists {
            //TODO: Update metadata version field?
        }

        //TODO: Set response fields
        // - Version ?
        let inner_response = PutObjectOutput {
            e_tag: Some(object_info.file_hashes.md5),
            size: Some(object_info.file_size as i64),
            checksum_sha256: Some(object_info.file_hashes.sha256),
            ..Default::default()
        };
        Ok(S3Response::new(inner_response))
    }
}
