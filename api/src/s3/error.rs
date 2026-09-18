//! Maps operation errors onto S3 error codes, messages and HTTP statuses.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::s3::checksum::checksum_mismatch_error;
use aruna_core::errors::{SourceResolutionError, StagingSourceError};
use aruna_core::structs::storage::routing::RoutingError;
use aruna_operations::blob::managed_copy::ManagedCopyError;
use aruna_operations::driver::{GateContextError, RoutingInputsError};
use aruna_operations::placement::policy::PolicyGateError;
use aruna_operations::s3::bucket::cors::{DeleteCorsError, GetCorsError, PutCorsError};
use aruna_operations::s3::bucket::create::CreateBucketError;
use aruna_operations::s3::bucket::delete::DeleteBucketError;
use aruna_operations::s3::bucket::get::GetBucketError;
use aruna_operations::s3::bucket::list::ListBucketsError;
use aruna_operations::s3::multipart::abort::AbortUploadError;
use aruna_operations::s3::multipart::complete::CompleteUploadError;
use aruna_operations::s3::multipart::create::CreateMultipartError;
use aruna_operations::s3::multipart::part_copy::PartCopyError;
use aruna_operations::s3::multipart::part_upload::UploadPartError;
use aruna_operations::s3::multipart::parts::ListPartsError;
use aruna_operations::s3::multipart::uploads::ListUploadsError;
use aruna_operations::s3::object::attributes::GetAttributesError;
use aruna_operations::s3::object::copy::CopyObjectError;
use aruna_operations::s3::object::delete::DeleteObjectError;
use aruna_operations::s3::object::get::GetObjectError;
use aruna_operations::s3::object::head::HeadObjectError;
use aruna_operations::s3::object::list::ListBucketError;
use aruna_operations::s3::object::put::PutObjectError;
use aruna_operations::s3::object::versions::ListVersionsError;
use aruna_operations::s3::purge_fence::PurgeFenceError;
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

fn purge_progress_error() -> S3Error {
    let mut error = S3Error::with_message(
        S3ErrorCode::Custom("PurgeInProgress".into()),
        "Writes to this object scope are temporarily suspended while a permanent purge is in progress; retry later."
            .to_string(),
    );
    error.set_status_code(http::StatusCode::SERVICE_UNAVAILABLE);
    error
}

/// A usage accounting failure. Decrements saturate, so only a real accounting
/// fault reaches here; the caller gets one stable code instead of a Rust error.
fn usage_accounting_error() -> S3Error {
    let mut error = S3Error::with_message(
        S3ErrorCode::Custom("UsageAccountingFailed".into()),
        "The request was not applied because this node could not update its usage counters."
            .to_string(),
    );
    error.set_status_code(http::StatusCode::INTERNAL_SERVER_ERROR);
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
        PolicyGateError::Denied { .. }
        | PolicyGateError::ForeignPolicy { .. }
        | PolicyGateError::NoSubject => placement_denied_error(action),
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

/// A destination gate that could not be built at all. An admission stop is not
/// one: `policy_gate_error` reports that, and only for a write carrying refs.
pub(crate) fn gate_context_error(error: GateContextError) -> S3Error {
    match error {
        GateContextError::Routing(error) => routing_inputs_error(error),
    }
}

fn missing_upload_error() -> S3Error {
    s3_error!(NoSuchUpload, "The specified upload does not exist.")
}

/// A completion already owns the upload. It must never read as `NoSuchUpload`:
/// the upload exists and the same request succeeds once the holder is done.
fn completion_lease_error() -> S3Error {
    let mut error = S3Error::with_message(
        S3ErrorCode::OperationAborted,
        "The upload is being completed, retry shortly.".to_string(),
    );
    error.set_status_code(http::StatusCode::CONFLICT);
    error
}

fn incomplete_body_error() -> S3Error {
    s3_error!(
        IncompleteBody,
        "You did not provide the number of bytes specified by the Content-Length HTTP header."
    )
}

fn missing_key_error() -> S3Error {
    s3_error!(NoSuchKey, "The specified key does not exist.")
}

fn missing_version_error() -> S3Error {
    s3_error!(NoSuchVersion, "The specified version does not exist.")
}

fn delete_marker_error() -> S3Error {
    s3_error!(
        MethodNotAllowed,
        "The specified version is a delete marker."
    )
}

fn missing_bucket_error() -> S3Error {
    s3_error!(NoSuchBucket, "The specified bucket does not exist.")
}

fn existing_bucket_error() -> S3Error {
    s3_error!(BucketAlreadyExists, "Bucket already exists")
}

fn nonempty_bucket_error() -> S3Error {
    s3_error!(
        BucketNotEmpty,
        "The bucket you tried to delete is not empty."
    )
}

fn missing_cors_error() -> S3Error {
    s3_error!(NoSuchCORSConfiguration, "CORS configuration not found")
}

fn checksum_mismatch_logged(algorithm: &'static str, operation: &'static str) -> S3Error {
    warn!(algorithm, "Checksum mismatch during {}", operation);
    checksum_mismatch_error()
}

fn missing_checksum_error(algorithm: &'static str, operation: &'static str) -> S3Error {
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
            CreateBucketError::BucketAlreadyExists => existing_bucket_error(),
            err => internal_error(err),
        }
    }
}

impl IntoS3Error for ListBucketsError {
    fn into_s3_error(self) -> S3Error {
        internal_error(self)
    }
}

impl IntoS3Error for ListBucketError {
    fn into_s3_error(self) -> S3Error {
        internal_error(self)
    }
}

impl IntoS3Error for ListPartsError {
    fn into_s3_error(self) -> S3Error {
        match self {
            ListPartsError::NoSuchUpload
            | ListPartsError::UploadTargetMismatch
            | ListPartsError::UploadNotOpen => missing_upload_error(),
            err => internal_error(err),
        }
    }
}

impl IntoS3Error for ListUploadsError {
    fn into_s3_error(self) -> S3Error {
        internal_error(self)
    }
}

impl IntoS3Error for ListVersionsError {
    fn into_s3_error(self) -> S3Error {
        internal_error(self)
    }
}

impl IntoS3Error for PutObjectError {
    fn into_s3_error(self) -> S3Error {
        match self {
            PutObjectError::ChecksumMismatch(algorithm) => {
                checksum_mismatch_logged(algorithm, "PutObject")
            }
            PutObjectError::MissingExpectedChecksum(algorithm) => {
                missing_checksum_error(algorithm, "PutObject")
            }
            PutObjectError::QuotaExceeded { limit, usage } => quota_exceeded_error(limit, usage),
            PutObjectError::RoutingFailed(RoutingError::BackendFull(backend)) => {
                backend_full_error(&backend.to_string())
            }
            PutObjectError::IncompleteBody => incomplete_body_error(),
            PutObjectError::WriteFailed(message) => write_failed_error(&message, "PutObject"),
            PutObjectError::PolicyGate(ref error) => policy_gate_error(error, "PutObject"),
            PutObjectError::ManagedCopyError(ref error) => managed_copy_error(error),
            PutObjectError::PurgeFence(PurgeFenceError::Suspended) => purge_progress_error(),
            PutObjectError::UsageUpdateError(_) => usage_accounting_error(),
            err => internal_error(err),
        }
    }
}

impl IntoS3Error for CreateMultipartError {
    fn into_s3_error(self) -> S3Error {
        match self {
            CreateMultipartError::RoutingFailed(RoutingError::BackendFull(backend)) => {
                backend_full_error(&backend.to_string())
            }
            CreateMultipartError::PurgeFence(PurgeFenceError::Suspended) => purge_progress_error(),
            err => internal_error(err),
        }
    }
}

impl IntoS3Error for UploadPartError {
    fn into_s3_error(self) -> S3Error {
        match self {
            UploadPartError::NoSuchUpload
            | UploadPartError::UploadTargetMismatch
            | UploadPartError::UploadNotOpen => missing_upload_error(),
            UploadPartError::ChecksumMismatch(algorithm) => {
                checksum_mismatch_logged(algorithm, "UploadPart")
            }
            UploadPartError::IncompleteBody => incomplete_body_error(),
            UploadPartError::WriteFailed(message) => write_failed_error(&message, "UploadPart"),
            UploadPartError::PolicyGateError(ref error) => policy_gate_error(error, "UploadPart"),
            UploadPartError::PurgeFence(PurgeFenceError::Suspended) => purge_progress_error(),
            err => internal_error(err),
        }
    }
}

impl IntoS3Error for PartCopyError {
    fn into_s3_error(self) -> S3Error {
        match self {
            PartCopyError::Get(err) => err.into_s3_error(),
            PartCopyError::UploadPart(err) => err.into_s3_error(),
            PartCopyError::PreconditionFailed => s3_error!(
                PreconditionFailed,
                "At least one of the preconditions you specified did not hold."
            ),
            // A policy error names the ids it conflicts on, which a client must
            // never learn: the refusal is reported without them.
            PartCopyError::Policy(_) => placement_denied_error("UploadPartCopy"),
            PartCopyError::Gate(err) => gate_context_error(err),
        }
    }
}

impl IntoS3Error for CompleteUploadError {
    fn into_s3_error(self) -> S3Error {
        match self {
            CompleteUploadError::NoSuchUpload
            | CompleteUploadError::UploadTargetMismatch
            | CompleteUploadError::UploadNotOpen => missing_upload_error(),
            CompleteUploadError::CompletionInProgress => completion_lease_error(),
            CompleteUploadError::MissingParts => {
                s3_error!(InvalidRequest, "You must specify at least one part.")
            }
            CompleteUploadError::InvalidObjectSize => s3_error!(
                InvalidRequest,
                "The provided object size does not match the uploaded parts."
            ),
            CompleteUploadError::EntityTooSmall => s3_error!(
                EntityTooSmall,
                "Your proposed upload is smaller than the minimum allowed object size."
            ),
            CompleteUploadError::MissingPartEtag => {
                s3_error!(InvalidPart, "The part ETag could not be validated.")
            }
            CompleteUploadError::InvalidPart => {
                s3_error!(
                    InvalidPart,
                    "One or more of the specified parts could not be found."
                )
            }
            CompleteUploadError::InvalidPartOrder => {
                s3_error!(
                    InvalidPartOrder,
                    "The list of parts was not in ascending order."
                )
            }
            CompleteUploadError::ChecksumMismatch(algorithm) => {
                checksum_mismatch_logged(algorithm, "CompleteMultipartUpload")
            }
            CompleteUploadError::ChecksumContractMismatch => s3_error!(
                InvalidRequest,
                "CompleteMultipartUpload checksum headers do not match the multipart upload initiation."
            ),
            CompleteUploadError::PartEtagMismatch => {
                s3_error!(
                    InvalidPart,
                    "The part ETag did not match the uploaded part."
                )
            }
            CompleteUploadError::QuotaExceeded { limit, usage } => {
                quota_exceeded_error(limit, usage)
            }
            CompleteUploadError::PolicyGate(ref error) => {
                policy_gate_error(error, "CompleteMultipartUpload")
            }
            CompleteUploadError::ManagedCopyError(ref error) => managed_copy_error(error),
            CompleteUploadError::PurgeFence(PurgeFenceError::Suspended) => purge_progress_error(),
            err => internal_error(err),
        }
    }
}

impl IntoS3Error for AbortUploadError {
    fn into_s3_error(self) -> S3Error {
        match self {
            AbortUploadError::NoSuchUpload
            | AbortUploadError::UploadTargetMismatch
            | AbortUploadError::UploadNotOpen => missing_upload_error(),
            AbortUploadError::CompletionInProgress => completion_lease_error(),
            err => internal_error(err),
        }
    }
}

impl IntoS3Error for GetObjectError {
    fn into_s3_error(self) -> S3Error {
        match self {
            GetObjectError::ManagedCopyError(ref error) => managed_copy_error(error),
            GetObjectError::NoSuchVersion => missing_version_error(),
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
            // A device is never a legal destination for governed data, so this
            // is a refusal (403), not a fault: retrying cannot change it.
            GetObjectError::GovernedUnavailable => {
                s3_error!(
                    AccessDenied,
                    "Governed content is not served on a user node."
                )
            }
            GetObjectError::HolderAccessDenied => {
                s3_error!(
                    AccessDenied,
                    "Access to the object was denied by its holder."
                )
            }
            // No holder served and at least one failed: a fault a retry may
            // clear, never object absence.
            GetObjectError::HoldersUnavailable => {
                s3_error!(
                    ServiceUnavailable,
                    "The object bytes are currently unavailable from every holder."
                )
            }
            GetObjectError::HolderIntegrityFailure => {
                s3_error!(
                    InternalError,
                    "A holder returned object bytes that failed integrity verification."
                )
            }
            GetObjectError::DeleteMarker => delete_marker_error(),
            GetObjectError::NoSuchKey => missing_key_error(),
            GetObjectError::InvalidRange => {
                s3_error!(InvalidRange, "The requested range is not satisfiable.")
            }
            GetObjectError::ResolveReferenceError(error) => match error {
                SourceResolutionError::ResolveFailed | SourceResolutionError::NotFound => {
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
            CopyObjectError::Reference(err) => {
                S3Error::with_message(S3ErrorCode::InternalError, err.to_string())
            }
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
            HeadObjectError::NoSuchVersion => missing_version_error(),
            HeadObjectError::DeleteMarker => delete_marker_error(),
            HeadObjectError::NoSuchKey => missing_key_error(),
            HeadObjectError::ResolveReferenceError(error) => match error {
                SourceResolutionError::ResolveFailed | SourceResolutionError::NotFound => {
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

impl IntoS3Error for GetAttributesError {
    fn into_s3_error(self) -> S3Error {
        match self {
            GetAttributesError::ManagedCopyError(ref error) => managed_copy_error(error),
            GetAttributesError::NoSuchVersion => missing_version_error(),
            GetAttributesError::DeleteMarker => delete_marker_error(),
            GetAttributesError::NoSuchKey => missing_key_error(),
            err => internal_error(err),
        }
    }
}

impl IntoS3Error for DeleteObjectError {
    fn into_s3_error(self) -> S3Error {
        match self {
            DeleteObjectError::NoSuchVersion => missing_version_error(),
            DeleteObjectError::PurgeFence(PurgeFenceError::Suspended) => purge_progress_error(),
            DeleteObjectError::UsageUpdateError(_) => usage_accounting_error(),
            err => internal_error(err),
        }
    }
}

impl IntoS3Error for GetBucketError {
    fn into_s3_error(self) -> S3Error {
        match self {
            GetBucketError::NotFound => missing_bucket_error(),
            err => internal_error(err),
        }
    }
}

impl IntoS3Error for DeleteBucketError {
    fn into_s3_error(self) -> S3Error {
        match self {
            DeleteBucketError::NotFound => missing_bucket_error(),
            DeleteBucketError::NotEmpty => nonempty_bucket_error(),
            err => internal_error(err),
        }
    }
}

impl IntoS3Error for PutCorsError {
    fn into_s3_error(self) -> S3Error {
        match self {
            PutCorsError::NotFound => missing_bucket_error(),
            err => internal_error(err),
        }
    }
}

impl IntoS3Error for GetCorsError {
    fn into_s3_error(self) -> S3Error {
        match self {
            GetCorsError::BucketNotFound => missing_bucket_error(),
            GetCorsError::CorsNotFound => missing_cors_error(),
            err => internal_error(err),
        }
    }
}

impl IntoS3Error for DeleteCorsError {
    fn into_s3_error(self) -> S3Error {
        match self {
            DeleteCorsError::NotFound => missing_bucket_error(),
            err => internal_error(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::errors::BlobError;

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
            PutObjectError::BlobWriteFailed(BlobError::WriteError(
                "No space left on device".to_string(),
            ))
            .into_s3_error(),
            UploadPartError::BlobWriteFailed("No space left on device".to_string()).into_s3_error(),
        ] {
            assert_eq!(*error.code(), S3ErrorCode::InternalError);
        }
    }

    #[test]
    fn purge_fence_retryable() {
        for error in [
            PutObjectError::PurgeFence(PurgeFenceError::Suspended).into_s3_error(),
            CompleteUploadError::PurgeFence(PurgeFenceError::Suspended).into_s3_error(),
            DeleteObjectError::PurgeFence(PurgeFenceError::Suspended).into_s3_error(),
        ] {
            assert_eq!(*error.code(), S3ErrorCode::Custom("PurgeInProgress".into()));
            assert_eq!(
                error.status_code(),
                Some(http::StatusCode::SERVICE_UNAVAILABLE)
            );
        }
    }

    // Governed content on a device is refused, not faulted: an honest 403 tells
    // the client the read will never succeed here.
    #[test]
    fn refuses_governed_read() {
        let refused = GetObjectError::GovernedUnavailable.into_s3_error();
        assert_eq!(*refused.code(), S3ErrorCode::AccessDenied);
        assert_eq!(refused.status_code(), Some(http::StatusCode::FORBIDDEN));
    }

    #[test]
    fn maps_holder_failures() {
        let denied = GetObjectError::HolderAccessDenied.into_s3_error();
        assert_eq!(*denied.code(), S3ErrorCode::AccessDenied);

        let unavailable = GetObjectError::HoldersUnavailable.into_s3_error();
        assert_eq!(*unavailable.code(), S3ErrorCode::ServiceUnavailable);

        let integrity = GetObjectError::HolderIntegrityFailure.into_s3_error();
        assert_eq!(*integrity.code(), S3ErrorCode::InternalError);
        assert_eq!(
            integrity.message(),
            Some("A holder returned object bytes that failed integrity verification.")
        );
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

    // A live completion lease is a retryable 409, never a 404: the upload exists
    // and the same request succeeds once the holder releases it.
    #[test]
    fn maps_completion_lease() {
        for error in [
            CompleteUploadError::CompletionInProgress.into_s3_error(),
            AbortUploadError::CompletionInProgress.into_s3_error(),
        ] {
            assert_eq!(*error.code(), S3ErrorCode::OperationAborted);
            assert_eq!(error.status_code(), Some(http::StatusCode::CONFLICT));
            assert_eq!(
                error.message(),
                Some("The upload is being completed, retry shortly.")
            );
        }
    }

    // UploadNotOpen from upload/complete/abort maps to NoSuchUpload (404).
    #[test]
    fn maps_not_open() {
        for error in [
            UploadPartError::UploadNotOpen.into_s3_error(),
            CompleteUploadError::UploadNotOpen.into_s3_error(),
            AbortUploadError::UploadNotOpen.into_s3_error(),
        ] {
            assert_eq!(*error.code(), S3ErrorCode::NoSuchUpload);
        }
    }

    #[test]
    fn maps_complete_errors() {
        assert_eq!(
            *CompleteUploadError::MissingParts.into_s3_error().code(),
            S3ErrorCode::InvalidRequest
        );
        assert_eq!(
            *CompleteUploadError::InvalidObjectSize
                .into_s3_error()
                .code(),
            S3ErrorCode::InvalidRequest
        );
        assert_eq!(
            *CompleteUploadError::MissingPartEtag.into_s3_error().code(),
            S3ErrorCode::InvalidPart
        );
        let entity_too_small = CompleteUploadError::EntityTooSmall.into_s3_error();
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
    fn maps_usage_failure() {
        // The counter fault must not leak a Rust error string to an S3 client.
        let error = DeleteObjectError::UsageUpdateError(
            aruna_operations::node::usage_stats::UsageUpdateError::UnexpectedEvent(
                aruna_core::events::Event::Blob(aruna_core::events::BlobEvent::DeleteFinished),
            ),
        )
        .into_s3_error();

        assert_eq!(
            *error.code(),
            S3ErrorCode::Custom("UsageAccountingFailed".into())
        );
        assert_eq!(
            error.status_code(),
            Some(http::StatusCode::INTERNAL_SERVER_ERROR)
        );
        assert!(!format!("{error:?}").contains("UnexpectedEvent"));
    }

    #[test]
    fn hides_copy_state() {
        // Unregistered, quarantined and blocked must be indistinguishable, so a
        // caller cannot probe which copies a node holds.
        let codes: Vec<S3ErrorCode> = [
            ManagedCopyError::Unregistered,
            ManagedCopyError::NotServeable(
                aruna_core::structs::storage::blob::ManagedCopyState::Quarantined(
                    aruna_core::structs::storage::blob::ManagedCopyQuarantine::Rejoin,
                ),
            ),
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
