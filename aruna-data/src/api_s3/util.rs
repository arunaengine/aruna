use s3s::{S3Error, S3Request, s3_error};

pub fn extract_access_key<T>(req: &S3Request<T>) -> Result<String, S3Error> {
    if let Some(access_key) = &req.credentials {
        Ok(access_key.access_key.clone())
    } else {
        Err(s3_error!(InvalidRequest, ""))
    }
}
