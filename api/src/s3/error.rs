use crate::s3::checksum::checksum_mismatch_error;
use aruna_core::errors::{SourceConnectorResolutionError, StagingSourceError};
use aruna_core::structs::RoutingError;
use aruna_operations::blob::managed_copy::ManagedCopyError;
use aruna_operations::driver::{GateContextError, RoutingInputsError};
use aruna_operations::placement_policy::PolicyGateError;
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
use aruna_operations::s3::get_object_attributes::GetObjectAttributesError;
use aruna_operations::s3::head_object::HeadObjectError;
use aruna_operations::s3::list_buckets::ListBucketsError;
use aruna_operations::s3::list_multipart_uploads::ListMultipartUploadsError;
use aruna_operations::s3::list_object_versions::ListObjectVersionsError;
use aruna_operations::s3::list_objects_v2::ListObjectsV2Error;
use aruna_operations::s3::list_parts::ListPartsError;
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

/// A reference binding at its automatic advance cap. S3 has no standard code
/// for it, so we return a custom code with an explicit 409 and the remedy.
fn reference_exhausted_error() -> S3Error {
    let mut error = S3Error::with_message(
        S3ErrorCode::Custom("ReferenceAdvanceExhausted".into()),
        "The reference binding reached its automatic advance limit; rebind it with an explicit write.".to_string(),
    );
    error.set_status_code(http::StatusCode::CONFLICT);
    error
}

/// A placement-policy refusal. The response never names a policy, a ref, or a
/// node: a public caller must not learn the residency rule from a refusal.
fn placement_denied_error(action: &str) -> S3Error {
    let mut error = S3Error::with_message(
        S3ErrorCode::Custom("PlacementPolicyDenied".into()),
        format!("{action} is not permitted for this object on this node."),
    );
    error.set_status_code(http::StatusCode::FORBIDDEN);
    error
}

/// A governed copy this node cannot currently answer for. Retryable and equally
/// non-disclosing: an unregistered, quarantined and blocked copy look alike.
fn placement_unavailable_error() -> S3Error {
    let mut error = S3Error::with_message(
        S3ErrorCode::Custom("PlacementUnavailable".into()),
        "The requested object is not currently available from this node.".to_string(),
    );
    error.set_status_code(http::StatusCode::SERVICE_UNAVAILABLE);
    error
}

/// One stable mapping for every managed-copy outcome, so a caller cannot tell
/// an absent registration from a quarantined or blocked one.
fn managed_copy_error(error: &ManagedCopyError) -> S3Error {
    match error {
        ManagedCopyError::Unregistered
        | ManagedCopyError::NotServeable(_)
        | ManagedCopyError::Mismatched
        | ManagedCopyError::NoSubject
        | ManagedCopyError::ServingBlocked => placement_unavailable_error(),
        other => internal_error(other),
    }
}

/// A denial and an unresolved rule map apart only by retryability; neither
/// discloses which policy decided.
fn policy_gate_error(error: &PolicyGateError, action: &str) -> S3Error {
    match error {
        PolicyGateError::Denied { .. } | PolicyGateError::NoSubject => {
            placement_denied_error(action)
        }
        PolicyGateError::Unavailable { .. }
        | PolicyGateError::Required { .. }
        | PolicyGateError::Drift
        | PolicyGateError::AdmissionStopped
        | PolicyGateError::Read(_) => placement_unavailable_error(),
        PolicyGateError::Invalid | PolicyGateError::Policy(_) => placement_denied_error(action),
        PolicyGateError::InvalidEvent | PolicyGateError::Conversion(_) => internal_error(error),
    }
}

/// A named backend that has reached its operator quota. Refusing loudly beats
/// hiding exhaustion by writing somewhere the rule did not name.
fn backend_full_error(backend: &str) -> S3Error {
    let mut error = S3Error::with_message(
        S3ErrorCode::Custom("QuotaExceeded".into()),
        format!("Storage backend {backend} has reached its quota"),
    );
    error.set_status_code(http::StatusCode::FORBIDDEN);
    error
}

/// A write whose routing inputs could not be read is refused: landing it on the
/// node default would permanently record the wrong backend.
pub(crate) fn routing_inputs_error(error: RoutingInputsError) -> S3Error {
    warn!(error = %error, "Refusing write with unreadable routing inputs");
    s3_error!(InternalError, "Storage routing inputs are unavailable")
}

/// A node mid-transition admits nothing governed. The refusal is retryable and
/// says nothing about which rule or subject is being revalidated.
pub(crate) fn gate_context_error(error: GateContextError) -> S3Error {
    match error {
        GateContextError::Routing(error) => routing_inputs_error(error),
        GateContextError::AdmissionStopped => placement_unavailable_error(),
    }
}

fn no_such_upload_error() -> S3Error {
    s3_error!(NoSuchUpload, "The specified upload does not exist.")
}

fn incomplete_body_error() -> S3Error {
    s3_error!(
        IncompleteBody,
        "You did not provide the number of bytes specified by the Content-Length HTTP header."
    )
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

/// A rejected body stream (trailer checksum mismatch or interrupted upload)
/// must surface as a client error so SDKs do not retry it as a server fault.
fn write_failed_error(message: &str, operation: &'static str) -> S3Error {
    warn!(message, "Blob write failed during {}", operation);
    checksum_mismatch_error()
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
            PutObjectError::RoutingFailed(RoutingError::BackendFull(backend)) => {
                backend_full_error(&backend.to_string())
            }
            PutObjectError::IncompleteBody => incomplete_body_error(),
            PutObjectError::WriteFailed(message) => write_failed_error(&message, "PutObject"),
            PutObjectError::PolicyGate(ref error) => policy_gate_error(error, "PutObject"),
            PutObjectError::ManagedCopyError(ref error) => managed_copy_error(error),
            err => internal_error(err),
        }
    }
}

impl IntoS3Error for CreateMultipartUploadError {
    fn into_s3_error(self) -> S3Error {
        match self {
            CreateMultipartUploadError::RoutingFailed(RoutingError::BackendFull(backend)) => {
                backend_full_error(&backend.to_string())
            }
            err => internal_error(err),
        }
    }
}

impl IntoS3Error for UploadPartError {
    fn into_s3_error(self) -> S3Error {
        match self {
            UploadPartError::NoSuchUpload
            | UploadPartError::UploadTargetMismatch
            | UploadPartError::UploadNotOpen => no_such_upload_error(),
            UploadPartError::ChecksumMismatch(algorithm) => {
                checksum_mismatch_s3_error(algorithm, "UploadPart")
            }
            UploadPartError::IncompleteBody => incomplete_body_error(),
            UploadPartError::WriteFailed(message) => write_failed_error(&message, "UploadPart"),
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
            UploadPartCopyError::Policy(err) => internal_error(err),
        }
    }
}

impl IntoS3Error for CompleteMultipartUploadError {
    fn into_s3_error(self) -> S3Error {
        match self {
            CompleteMultipartUploadError::NoSuchUpload
            | CompleteMultipartUploadError::UploadTargetMismatch
            | CompleteMultipartUploadError::UploadNotOpen => no_such_upload_error(),
            CompleteMultipartUploadError::MissingParts => {
                s3_error!(InvalidRequest, "You must specify at least one part.")
            }
            CompleteMultipartUploadError::InvalidObjectSize => s3_error!(
                InvalidRequest,
                "The provided object size does not match the uploaded parts."
            ),
            CompleteMultipartUploadError::EntityTooSmall => s3_error!(
                EntityTooSmall,
                "Your proposed upload is smaller than the minimum allowed object size."
            ),
            CompleteMultipartUploadError::MissingPartEtag => {
                s3_error!(InvalidPart, "The part ETag could not be validated.")
            }
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
            CompleteMultipartUploadError::ChecksumContractMismatch => s3_error!(
                InvalidRequest,
                "CompleteMultipartUpload checksum headers do not match the multipart upload initiation."
            ),
            CompleteMultipartUploadError::PartEtagMismatch => {
                s3_error!(
                    InvalidPart,
                    "The part ETag did not match the uploaded part."
                )
            }
            CompleteMultipartUploadError::QuotaExceeded { limit, usage } => {
                quota_exceeded_error(limit, usage)
            }
            CompleteMultipartUploadError::PolicyGate(ref error) => {
                policy_gate_error(error, "CompleteMultipartUpload")
            }
            CompleteMultipartUploadError::ManagedCopyError(ref error) => managed_copy_error(error),
            err => internal_error(err),
        }
    }
}

impl IntoS3Error for AbortMultipartUploadError {
    fn into_s3_error(self) -> S3Error {
        match self {
            AbortMultipartUploadError::NoSuchUpload
            | AbortMultipartUploadError::UploadTargetMismatch
            | AbortMultipartUploadError::UploadNotOpen => no_such_upload_error(),
            err => internal_error(err),
        }
    }
}

impl IntoS3Error for GetObjectError {
    fn into_s3_error(self) -> S3Error {
        match self {
            GetObjectError::ManagedCopyError(ref error) => managed_copy_error(error),
            GetObjectError::NoSuchVersion => no_such_version_error(),
            GetObjectError::HistoricalReferenceUnavailable => {
                s3_error!(
                    NoSuchVersion,
                    "The requested reference version is no longer available."
                )
            }
            GetObjectError::ReferenceSourceChanged => {
                s3_error!(
                    ServiceUnavailable,
                    "The reference source is changing; retry the request."
                )
            }
            GetObjectError::ReferenceAdvanceExhausted => reference_exhausted_error(),
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
                StagingSourceError::AccessDenied => {
                    s3_error!(AccessDenied, "Access to the referenced source was denied.")
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
            CopyObjectError::Routing(err) => routing_inputs_error(err),
            CopyObjectError::Gate(err) => gate_context_error(err),
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
            HeadObjectError::ManagedCopyError(ref error) => managed_copy_error(error),
            HeadObjectError::NoSuchVersion => no_such_version_error(),
            HeadObjectError::DeleteMarker => delete_marker_error(),
            HeadObjectError::NoSuchKey => no_such_key_error(),
            HeadObjectError::ResolveReferenceError(error) => match error {
                SourceConnectorResolutionError::ResolveFailed
                | SourceConnectorResolutionError::NotFound => {
                    s3_error!(
                        ServiceUnavailable,
                        "Reference source is currently unavailable"
                    )
                }
                err => internal_error(err),
            },
            HeadObjectError::StagingSourceError(error) => match error {
                StagingSourceError::NotFound => {
                    s3_error!(NoSuchKey, "The referenced source object does not exist.")
                }
                StagingSourceError::AccessDenied => {
                    s3_error!(AccessDenied, "Access to the referenced source was denied.")
                }
                err => s3_error!(ServiceUnavailable, "{}", err),
            },
            err => internal_error(err),
        }
    }
}

impl IntoS3Error for GetObjectAttributesError {
    fn into_s3_error(self) -> S3Error {
        match self {
            GetObjectAttributesError::ManagedCopyError(ref error) => managed_copy_error(error),
            GetObjectAttributesError::NoSuchVersion => no_such_version_error(),
            GetObjectAttributesError::DeleteMarker => delete_marker_error(),
            GetObjectAttributesError::NoSuchKey => no_such_key_error(),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_incomplete_body() {
        assert_eq!(
            *PutObjectError::IncompleteBody.into_s3_error().code(),
            S3ErrorCode::IncompleteBody
        );
        assert_eq!(
            *UploadPartError::IncompleteBody.into_s3_error().code(),
            S3ErrorCode::IncompleteBody
        );
    }

    // A failed blob write maps to BadDigest (400) so SDKs do not retry it.
    #[test]
    fn maps_write_failed() {
        assert_eq!(
            *PutObjectError::WriteFailed("mismatch".to_string())
                .into_s3_error()
                .code(),
            S3ErrorCode::BadDigest
        );
        assert_eq!(
            *UploadPartError::WriteFailed("mismatch".to_string())
                .into_s3_error()
                .code(),
            S3ErrorCode::BadDigest
        );
    }

    // A server-side write fault (full or flapping disk) must stay a retryable
    // InternalError; reporting BadDigest would tell SDKs the data was corrupt.
    #[test]
    fn maps_backend_write() {
        for error in [
            PutObjectError::BlobWriteFailed("No space left on device".to_string()).into_s3_error(),
            UploadPartError::BlobWriteFailed("No space left on device".to_string()).into_s3_error(),
        ] {
            assert_eq!(*error.code(), S3ErrorCode::InternalError);
        }
    }

    // The three reference failures are distinct to a client: gone (404), retry
    // later (503), and rebind required (409). Copy must forward them unchanged.
    #[test]
    fn maps_reference_errors() {
        let historical = GetObjectError::HistoricalReferenceUnavailable.into_s3_error();
        assert_eq!(*historical.code(), S3ErrorCode::NoSuchVersion);

        let changed = GetObjectError::ReferenceSourceChanged.into_s3_error();
        assert_eq!(*changed.code(), S3ErrorCode::ServiceUnavailable);

        let exhausted = GetObjectError::ReferenceAdvanceExhausted.into_s3_error();
        assert_eq!(
            *exhausted.code(),
            S3ErrorCode::Custom("ReferenceAdvanceExhausted".into())
        );
        assert_eq!(exhausted.status_code(), Some(http::StatusCode::CONFLICT));

        let copied =
            CopyObjectError::Get(GetObjectError::ReferenceAdvanceExhausted).into_s3_error();
        assert_eq!(*copied.code(), *exhausted.code());
        assert_eq!(copied.status_code(), exhausted.status_code());
    }

    // UploadNotOpen from upload/complete/abort maps to NoSuchUpload (404).
    #[test]
    fn maps_not_open() {
        for error in [
            UploadPartError::UploadNotOpen.into_s3_error(),
            CompleteMultipartUploadError::UploadNotOpen.into_s3_error(),
            AbortMultipartUploadError::UploadNotOpen.into_s3_error(),
        ] {
            assert_eq!(*error.code(), S3ErrorCode::NoSuchUpload);
        }
    }

    #[test]
    fn maps_complete_errors() {
        assert_eq!(
            *CompleteMultipartUploadError::MissingParts
                .into_s3_error()
                .code(),
            S3ErrorCode::InvalidRequest
        );
        assert_eq!(
            *CompleteMultipartUploadError::InvalidObjectSize
                .into_s3_error()
                .code(),
            S3ErrorCode::InvalidRequest
        );
        assert_eq!(
            *CompleteMultipartUploadError::MissingPartEtag
                .into_s3_error()
                .code(),
            S3ErrorCode::InvalidPart
        );
        let entity_too_small = CompleteMultipartUploadError::EntityTooSmall.into_s3_error();
        assert_eq!(*entity_too_small.code(), S3ErrorCode::EntityTooSmall);
        assert_eq!(
            entity_too_small.status_code(),
            Some(http::StatusCode::BAD_REQUEST)
        );
    }

    #[test]
    fn maps_head_denial() {
        let error =
            HeadObjectError::StagingSourceError(StagingSourceError::AccessDenied).into_s3_error();

        assert_eq!(*error.code(), S3ErrorCode::AccessDenied);
    }

    #[test]
    fn hides_policy_ids() {
        // A public caller may learn that a write was refused, never which rule
        // refused it or which node the rule names.
        let policy_id = ulid::Ulid::from_bytes([7u8; 16]);
        let denied = PutObjectError::PolicyGate(PolicyGateError::Denied {
            policy_ids: vec![policy_id],
        })
        .into_s3_error();

        assert_eq!(
            *denied.code(),
            S3ErrorCode::Custom("PlacementPolicyDenied".into())
        );
        assert_eq!(denied.status_code(), Some(http::StatusCode::FORBIDDEN));
        let rendered = format!("{denied:?}");
        assert!(!rendered.contains(&policy_id.to_string()));
    }

    #[test]
    fn hides_copy_state() {
        // Unregistered, quarantined and blocked must be indistinguishable, so a
        // caller cannot probe which copies a node holds.
        let codes: Vec<S3ErrorCode> = [
            ManagedCopyError::Unregistered,
            ManagedCopyError::NotServeable(aruna_core::structs::ManagedCopyState::Quarantined(
                aruna_core::structs::ManagedCopyQuarantine::Rejoin,
            )),
            ManagedCopyError::Mismatched,
            ManagedCopyError::NoSubject,
            ManagedCopyError::ServingBlocked,
        ]
        .into_iter()
        .map(|error| {
            GetObjectError::ManagedCopyError(error)
                .into_s3_error()
                .code()
                .clone()
        })
        .collect();

        assert!(
            codes
                .iter()
                .all(|code| *code == S3ErrorCode::Custom("PlacementUnavailable".into()))
        );
    }
}
