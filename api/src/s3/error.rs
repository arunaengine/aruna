use crate::s3::checksum::checksum_mismatch_error;
use s3s::{s3_error, S3Error};
use std::fmt::Display;
use tracing::warn;

fn internal_error<E: Display>(err: E) -> S3Error {
    s3_error!(InternalError, "{}", err)
}

fn no_such_upload_error() -> S3Error {
    s3_error!(NoSuchUpload, "The specified upload does not exist.")
}

fn no_such_key_error() -> S3Error {
    s3_error!(NoSuchKey, "The specified key does not exist.")
}

fn no_such_version_error() -> S3Error {
    s3_error!(NoSuchVersion, "The specified version does not exist.")
}

fn delete_marker_error() -> S3Error {
    s3_error!(
        MethodNotAllowed,
        "The specified version is a delete marker."
    )
}

fn bucket_not_found_error() -> S3Error {
    s3_error!(NoSuchBucket, "The specified bucket does not exist.")
}

fn bucket_already_exists_error() -> S3Error {
    s3_error!(BucketAlreadyExists, "Bucket already exists")
}

fn bucket_not_empty_error() -> S3Error {
    s3_error!(
        BucketNotEmpty,
        "The bucket you tried to delete is not empty."
    )
}

fn replication_configuration_not_found_error() -> S3Error {
    s3_error!(
        ReplicationConfigurationNotFoundError,
        "Replication configuration not found"
    )
}

fn checksum_mismatch_s3_error(algorithm: &'static str, operation: &'static str) -> S3Error {
    warn!(algorithm, "Checksum mismatch during {}", operation);
    checksum_mismatch_error()
}

fn missing_expected_checksum_s3_error(algorithm: &'static str, operation: &'static str) -> S3Error {
    warn!(algorithm, "Missing checksum during {}", operation);
    s3_error!(InternalError, "Missing stored checksum")
}

pub(crate) trait IntoS3Error {
    fn into_s3_error(self) -> S3Error;
}
