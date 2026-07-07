use crate::s3::checksum::checksum_mismatch_error;
use aruna_core::errors::{SourceConnectorResolutionError, StagingSourceError};
use aruna_operations::s3::abort_multipart_upload::AbortMultipartUploadError;
use aruna_operations::s3::bucket_cors::{
    DeleteBucketCorsError, GetBucketCorsError, PutBucketCorsError,
};
use aruna_operations::s3::complete_multipart_upload::CompleteMultipartUploadError;
use aruna_operations::s3::copy_object::CopyObjectError;
use aruna_operations::s3::create_bucket::CreateBucketError;
use aruna_operations::s3::create_multipart_upload::CreateMultipartUploadError;
use aruna_operations::s3::delete_bucket::DeleteBucketError;
use aruna_operations::s3::delete_object::DeleteObjectError;
use aruna_operations::s3::get_bucket_info::GetBucketInfoError;
use aruna_operations::s3::get_object::GetObjectError;
use aruna_operations::s3::head_object::HeadObjectError;
use aruna_operations::s3::list_buckets::ListBucketsError;
use aruna_operations::s3::list_multipart_uploads::ListMultipartUploadsError;
use aruna_operations::s3::list_object_versions::ListObjectVersionsError;
use aruna_operations::s3::list_objects_v2::ListObjectsV2Error;
use aruna_operations::s3::list_parts::ListPartsError;
use aruna_operations::s3::put_bucket_replication::{
    DeleteBucketReplicationError, GetBucketReplicationError, PutBucketReplicationError,
};
use aruna_operations::s3::put_object::PutObjectError;
use aruna_operations::s3::upload_part::UploadPartError;
use aruna_operations::s3::upload_part_copy::UploadPartCopyError;
use s3s::{S3Error, S3ErrorCode, s3_error};
use std::fmt::Display;
use tracing::warn;

fn internal_error<E: Display>(err: E) -> S3Error {
    s3_error!(InternalError, "{}", err)
}

/// A group storage quota rejection. S3 has no standard quota code, so we return a
/// custom `QuotaExceeded` code with an explicit 403 status, matching the
/// convention used by S3-compatible object stores.
fn quota_exceeded_error(limit: u64, usage: u64) -> S3Error {
    let mut error = S3Error::with_message(
        S3ErrorCode::Custom("QuotaExceeded".into()),
        format!("Group storage quota exceeded: {usage} bytes would exceed limit of {limit} bytes"),
    );
    error.set_status_code(http::StatusCode::FORBIDDEN);
    error
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

fn cors_configuration_not_found_error() -> S3Error {
    s3_error!(NoSuchCORSConfiguration, "CORS configuration not found")
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

impl IntoS3Error for CreateBucketError {
    fn into_s3_error(self) -> S3Error {
        match self {
            CreateBucketError::BucketAlreadyExists => bucket_already_exists_error(),
            err => internal_error(err),
        }
    }
}

impl IntoS3Error for ListBucketsError {
    fn into_s3_error(self) -> S3Error {
        internal_error(self)
    }
}

impl IntoS3Error for ListObjectsV2Error {
    fn into_s3_error(self) -> S3Error {
        internal_error(self)
    }
}

impl IntoS3Error for ListPartsError {
    fn into_s3_error(self) -> S3Error {
        match self {
            ListPartsError::NoSuchUpload
            | ListPartsError::UploadTargetMismatch
            | ListPartsError::UploadNotOpen => no_such_upload_error(),
            err => internal_error(err),
        }
    }
}

impl IntoS3Error for ListMultipartUploadsError {
    fn into_s3_error(self) -> S3Error {
        internal_error(self)
    }
}

impl IntoS3Error for ListObjectVersionsError {
    fn into_s3_error(self) -> S3Error {
        internal_error(self)
    }
}

impl IntoS3Error for PutObjectError {
    fn into_s3_error(self) -> S3Error {
        match self {
            PutObjectError::ChecksumMismatch(algorithm) => {
                checksum_mismatch_s3_error(algorithm, "PutObject")
            }
            PutObjectError::MissingExpectedChecksum(algorithm) => {
                missing_expected_checksum_s3_error(algorithm, "PutObject")
            }
            PutObjectError::QuotaExceeded { limit, usage } => quota_exceeded_error(limit, usage),
            err => internal_error(err),
        }
    }
}

impl IntoS3Error for CreateMultipartUploadError {
    fn into_s3_error(self) -> S3Error {
        internal_error(self)
    }
}

impl IntoS3Error for UploadPartError {
    fn into_s3_error(self) -> S3Error {
        match self {
            UploadPartError::NoSuchUpload | UploadPartError::UploadTargetMismatch => {
                no_such_upload_error()
            }
            UploadPartError::ChecksumMismatch(algorithm) => {
                checksum_mismatch_s3_error(algorithm, "UploadPart")
            }
            err => internal_error(err),
        }
    }
}

impl IntoS3Error for UploadPartCopyError {
    fn into_s3_error(self) -> S3Error {
        match self {
            UploadPartCopyError::Get(err) => err.into_s3_error(),
            UploadPartCopyError::UploadPart(err) => err.into_s3_error(),
            UploadPartCopyError::PreconditionFailed => s3_error!(
                PreconditionFailed,
                "At least one of the preconditions you specified did not hold."
            ),
        }
    }
}

impl IntoS3Error for CompleteMultipartUploadError {
    fn into_s3_error(self) -> S3Error {
        match self {
            CompleteMultipartUploadError::NoSuchUpload
            | CompleteMultipartUploadError::UploadTargetMismatch => no_such_upload_error(),
            CompleteMultipartUploadError::InvalidPart => {
                s3_error!(
                    InvalidPart,
                    "One or more of the specified parts could not be found."
                )
            }
            CompleteMultipartUploadError::InvalidPartOrder => {
                s3_error!(
                    InvalidPartOrder,
                    "The list of parts was not in ascending order."
                )
            }
            CompleteMultipartUploadError::ChecksumMismatch(algorithm) => {
                checksum_mismatch_s3_error(algorithm, "CompleteMultipartUpload")
            }
            CompleteMultipartUploadError::PartEtagMismatch => {
                s3_error!(
                    InvalidPart,
                    "The part ETag did not match the uploaded part."
                )
            }
            CompleteMultipartUploadError::QuotaExceeded { limit, usage } => {
                quota_exceeded_error(limit, usage)
            }
            err => internal_error(err),
        }
    }
}

impl IntoS3Error for AbortMultipartUploadError {
    fn into_s3_error(self) -> S3Error {
        match self {
            AbortMultipartUploadError::NoSuchUpload
            | AbortMultipartUploadError::UploadTargetMismatch => no_such_upload_error(),
            err => internal_error(err),
        }
    }
}

impl IntoS3Error for GetObjectError {
    fn into_s3_error(self) -> S3Error {
        match self {
            GetObjectError::NoSuchVersion => no_such_version_error(),
            GetObjectError::DeleteMarker => delete_marker_error(),
            GetObjectError::NoSuchKey => no_such_key_error(),
            GetObjectError::InvalidRange => {
                s3_error!(InvalidRange, "The requested range is not satisfiable.")
            }
            GetObjectError::ResolveReferenceError(error) => match error {
                SourceConnectorResolutionError::ResolveFailed
                | SourceConnectorResolutionError::NotFound => {
                    s3_error!(
                        ServiceUnavailable,
                        "Reference source is currently unavailable"
                    )
                }
                err => internal_error(err),
            },
            GetObjectError::StagingSourceError(error) => match error {
                StagingSourceError::NotFound => {
                    s3_error!(NoSuchKey, "The referenced source object does not exist.")
                }
                err => s3_error!(ServiceUnavailable, "{}", err),
            },
            err => internal_error(err),
        }
    }
}

impl IntoS3Error for CopyObjectError {
    fn into_s3_error(self) -> S3Error {
        match self {
            CopyObjectError::Get(err) => err.into_s3_error(),
            CopyObjectError::Put(err) => err.into_s3_error(),
            CopyObjectError::PreconditionFailed => s3_error!(
                PreconditionFailed,
                "At least one of the preconditions you specified did not hold."
            ),
        }
    }
}

impl IntoS3Error for HeadObjectError {
    fn into_s3_error(self) -> S3Error {
        match self {
            HeadObjectError::NoSuchVersion => no_such_version_error(),
            HeadObjectError::DeleteMarker => delete_marker_error(),
            HeadObjectError::NoSuchKey => no_such_key_error(),
            err => internal_error(err),
        }
    }
}

impl IntoS3Error for DeleteObjectError {
    fn into_s3_error(self) -> S3Error {
        match self {
            DeleteObjectError::NoSuchVersion => no_such_version_error(),
            err => internal_error(err),
        }
    }
}

impl IntoS3Error for GetBucketInfoError {
    fn into_s3_error(self) -> S3Error {
        match self {
            GetBucketInfoError::NotFound => bucket_not_found_error(),
            err => internal_error(err),
        }
    }
}

impl IntoS3Error for DeleteBucketError {
    fn into_s3_error(self) -> S3Error {
        match self {
            DeleteBucketError::NotFound => bucket_not_found_error(),
            DeleteBucketError::NotEmpty => bucket_not_empty_error(),
            err => internal_error(err),
        }
    }
}

impl IntoS3Error for PutBucketReplicationError {
    fn into_s3_error(self) -> S3Error {
        internal_error(self)
    }
}

impl IntoS3Error for PutBucketCorsError {
    fn into_s3_error(self) -> S3Error {
        match self {
            PutBucketCorsError::NotFound => bucket_not_found_error(),
            err => internal_error(err),
        }
    }
}

impl IntoS3Error for GetBucketCorsError {
    fn into_s3_error(self) -> S3Error {
        match self {
            GetBucketCorsError::BucketNotFound => bucket_not_found_error(),
            GetBucketCorsError::CorsNotFound => cors_configuration_not_found_error(),
            err => internal_error(err),
        }
    }
}

impl IntoS3Error for DeleteBucketCorsError {
    fn into_s3_error(self) -> S3Error {
        match self {
            DeleteBucketCorsError::NotFound => bucket_not_found_error(),
            err => internal_error(err),
        }
    }
}

impl IntoS3Error for GetBucketReplicationError {
    fn into_s3_error(self) -> S3Error {
        match self {
            GetBucketReplicationError::NotFound => replication_configuration_not_found_error(),
            err => internal_error(err),
        }
    }
}

impl IntoS3Error for DeleteBucketReplicationError {
    fn into_s3_error(self) -> S3Error {
        internal_error(self)
    }
}
