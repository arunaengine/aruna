use crate::IOHandler;
use crate::api_s3::auth::UserAccess;
use crate::api_s3::util::extract_access_key;
use crate::io::io_handler::{ACCESS_DB_NAME, LOCATION_DB_NAME, ObjectInfo, PATH_LOCATION_DB_NAME};
use anyhow::{Result, anyhow};
use aruna_storage::storage::lmdb::LmdbStore;
use aruna_storage::storage::store::Store;
use futures_util::TryStreamExt;
use iroh::NodeId;
use s3s::dto::{GetObjectInput, GetObjectOutput, PutObjectInput, PutObjectOutput, StreamingBlob};
use s3s::{S3, S3Request, S3Response, S3Result, s3_error};
use std::fmt::Debug;
use std::path::Path;
use std::sync::Arc;
use tracing::{debug, error};
use ulid::Ulid;

pub struct ArunaS3Service {
    backend: Arc<IOHandler<LmdbStore>>,
    node_id: NodeId,
}

impl Debug for ArunaS3Service {
    #[tracing::instrument(level = "trace", skip(self, f))]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArunaS3Service").finish()
    }
}

impl ArunaS3Service {
    #[tracing::instrument(level = "trace", skip(backend))]
    pub async fn new(backend: Arc<IOHandler<LmdbStore>>, node_id: NodeId) -> Result<Self> {
        Ok(ArunaS3Service { backend, node_id })
    }
}

#[async_trait::async_trait]
impl S3 for ArunaS3Service {
    #[tracing::instrument(err)]
    #[allow(clippy::blocks_in_conditions)]
    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        // Extract access key id from provided credentials
        let access_key = extract_access_key(&req)?;

        //
        let store_clone = self.backend.store.clone();
        let info = tokio::task::spawn_blocking(move || {
            let mut read_txn = store_clone
                .create_txn(false)
                .map_err(|e| s3_error!(InternalError, "{}", e))?;

            // Fetch access info for user
            let user_access: UserAccess = if let Some(info_raw) =
                store_clone.get(&mut read_txn, ACCESS_DB_NAME, access_key.as_bytes())?
            {
                bincode::serde::decode_from_slice(&*info_raw, bincode::config::standard())?.0
            } else {
                return Err(anyhow!("No access info found"));
            };

            //TODO: Check permissions

            let full_path = format!(
                "{}/{}/{}",
                user_access.group_id, req.input.bucket, req.input.key
            );

            let hash = store_clone
                .get(&read_txn, PATH_LOCATION_DB_NAME, full_path.as_bytes())
                .map_err(|e| s3_error!(InternalError, "{}", e))?
                .ok_or_else(|| s3_error!(NoSuchKey, "No such key"))?;
            let info_raw = store_clone
                .get(&read_txn, LOCATION_DB_NAME, &hash)
                .map_err(|e| s3_error!(InternalError, "{}", e))?
                .ok_or_else(|| s3_error!(NoSuchKey, "No such key"))?;
            let info: (ObjectInfo, usize) =
                bincode::serde::decode_from_slice(&*info_raw, bincode::config::standard())
                    .map_err(|e| s3_error!(InternalError, "{}", e))?;

            Ok::<ObjectInfo, anyhow::Error>(info.0)
        })
        .await
        .map_err(|e| s3_error!(InternalError, "{}", e))?
        .map_err(|e| s3_error!(InternalError, "{}", e))?;

        //TODO: Create operator for backend (if necessary)
        // - How to handle different backends dynamically?

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

        //TODO: Set response fields
        let output = GetObjectOutput {
            body,
            content_disposition: Some(format!(r#"attachment;filename="{}""#, filename)),
            content_length: Some(info.file_size as i64),
            last_modified: None,
            version_id: None,
            ..Default::default()
        };
        debug!(?output);

        Ok(S3Response::new(output))
    }

    #[tracing::instrument(err, skip(self, req))]
    #[allow(clippy::blocks_in_conditions)]
    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        debug!("Received PUT Request: {:#?}", req);

        // Extract access key id from provided credentials
        let access_key = extract_access_key(&req)?;

        // Fetch user access info
        let store_clone = self.backend.store.clone();
        let info = tokio::task::spawn_blocking(move || {
            let mut read_txn = store_clone.create_txn(false)?;

            // Fetch access info for user
            let info = if let Some(info_raw) =
                store_clone.get(&mut read_txn, ACCESS_DB_NAME, access_key.as_bytes())?
            {
                bincode::serde::decode_from_slice(&*info_raw, bincode::config::standard())?.0
            } else {
                return Err(anyhow!("No access info found"));
            };

            //TODO: Check permissions ?

            Ok::<UserAccess, anyhow::Error>(info)
        })
        .await
        .map_err(|e| s3_error!(InternalError, "{}", e))?
        .map_err(|e| s3_error!(InternalError, "{}", e))?;

        //TODO: Path concept :/
        let path_ulid = Ulid::new();
        let backend_path = format!(
            "{}/{}/{}/{}/{}/{}",
            "aruna", self.node_id, info.group_id, path_ulid, req.input.bucket, req.input.key
        );
        let extended_frontend_path =
            format!("{}/{}/{}", info.group_id, req.input.bucket, req.input.key);

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
