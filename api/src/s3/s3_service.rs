use crate::s3::util::{convert_input, to_base64};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::s3::put_object::{PutObjectConfig, PutObjectOperation};
use s3s::dto::{ETag, PutObjectInput, PutObjectOutput};
use s3s::{S3, S3Request, S3Response, S3Result, s3_error};
use std::fmt::Debug;
use std::sync::Arc;
use tracing::{debug, error, warn};
use ulid::Ulid;

#[derive(Clone)]
pub struct ArunaS3Service {
    state: Arc<DriverContext>,
}

impl Debug for ArunaS3Service {
    #[tracing::instrument(level = "trace", skip(self, f))]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArunaS3Service").finish()
    }
}

impl ArunaS3Service {
    #[tracing::instrument(level = "trace", skip(driver_ctx))]
    pub async fn new(driver_ctx: Arc<DriverContext>) -> Self {
        ArunaS3Service { state: driver_ctx }
    }
}

#[async_trait::async_trait]
impl S3 for ArunaS3Service {
    #[tracing::instrument(err, skip(self, req))]
    #[allow(clippy::blocks_in_conditions)]
    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        debug!("Received PUT Request: {:#?}", req);

        // Extract access check result
        /*
        let UserAccess {
            user_id, group_id, ..
        } = req.extensions.get::<UserAccess>().cloned().ok_or_else(|| {
            error!(error = "Missing user context");
            s3_error!(UnexpectedContent, "Missing user context")
        })?;
        */
        let user_id = Ulid::new();
        let group_id = Ulid::new();

        let input = convert_input(req.input)?;
        let config = PutObjectConfig {
            user_id,
            group_id,
            request: input,
            exists: false,
        };
        let operation = PutObjectOperation::new(config);

        if let Some(Ok(blob_info)) = drive(operation, &self.state)
            .await
            .map_err(|err| s3_error!(InternalError, "{}", err.to_string()))?
        {
            Ok(S3Response::new(PutObjectOutput {
                e_tag: Some(ETag::Strong(to_base64(
                    blob_info.hashes.get("md5").ok_or_else(|| {
                        error!(error = "Missing MD5 hash");
                        s3_error!(InternalError, "Missing MD5 hash")
                    })?,
                ))),
                size: Some(blob_info.blob_size as i64),
                ..Default::default()
            }))
        } else {
            Err(s3_error!(InternalError, "Failed to process PUT request"))
        }
    }
}
