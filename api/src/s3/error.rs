use s3s::S3Error;

pub(crate) trait IntoS3Error {
    fn into_s3_error(self) -> S3Error;
}
