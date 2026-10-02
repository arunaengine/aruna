//! Joins concurrent complete multipart requests for one upload onto one shared run.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::time::Duration;

use aruna_core::structs::checksum::ExpectedChecksum;
use aruna_core::structs::storage::multipart::MultipartChecksumType;
use aruna_operations::s3::multipart::complete::{CompleteMultipartPart, CompleteUploadResult};
use aruna_tasks::join_registry::{JoinRegistry, JoinWatch, await_joined};
use s3s::{S3Error, S3ErrorCode, s3_error};
use ulid::Ulid;

/// Retention lets a retry recover the completed object's ETag and version.
const COMPLETION_RETENTION: Duration = Duration::from_secs(600);

/// Bucket, object key and upload id: an upload id alone would let a request
/// for another key join this completion and skip its own target validation.
pub type CompletionKey = (String, String, Ulid);
pub type CompletionOutcome = Arc<Result<CompleteUploadResult, CompletionFailure>>;
pub type CompletionRegistry = JoinRegistry<CompletionKey, CompletionOutcome, CompletionRequest>;

/// What one CompleteMultipartUpload asks for. Only the same request may join a running
/// completion or replay its result; ETags compare without their quotes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompletionRequest {
    parts: Vec<CompleteMultipartPart>,
    expected_checksums: Vec<ExpectedChecksum>,
    checksum_type: MultipartChecksumType,
    checksum_type_explicit: bool,
    object_size: Option<u64>,
}

impl CompletionRequest {
    pub fn new(
        parts: &[CompleteMultipartPart],
        expected_checksums: &[ExpectedChecksum],
        checksum_type: MultipartChecksumType,
        checksum_type_explicit: bool,
        object_size: Option<u64>,
    ) -> Self {
        let parts = parts
            .iter()
            .map(|part| CompleteMultipartPart {
                etag: part
                    .etag
                    .as_deref()
                    .map(|etag| etag.trim_matches('"').to_string()),
                ..part.clone()
            })
            .collect();
        Self {
            parts,
            expected_checksums: expected_checksums.to_vec(),
            checksum_type,
            checksum_type_explicit,
            object_size,
        }
    }
}

/// Another request with other parts, ETags, checksums or size already completes this upload.
pub fn conflicting_completion() -> S3Error {
    s3_error!(
        InvalidRequest,
        "The request conflicts with another completion of this upload."
    )
}

pub fn completion_registry() -> CompletionRegistry {
    // Only a successful completion is worth replaying; a corrected retry must
    // not be served the earlier failure.
    JoinRegistry::new(COMPLETION_RETENTION).retain_if(|outcome: &CompletionOutcome| outcome.is_ok())
}

/// An S3 error kept in a shareable form, so every joined request can be given
/// the same refusal.
#[derive(Clone, Debug)]
pub struct CompletionFailure {
    code: S3ErrorCode,
    message: String,
    status: Option<http::StatusCode>,
}

impl CompletionFailure {
    pub fn new(error: &S3Error) -> Self {
        Self {
            code: error.code().clone(),
            message: error.message().unwrap_or_default().to_string(),
            status: error.status_code(),
        }
    }

    pub fn to_s3_error(&self) -> S3Error {
        let mut error = S3Error::with_message(self.code.clone(), self.message.clone());
        if let Some(status) = self.status {
            error.set_status_code(status);
        }
        error
    }
}

/// Waits for the shared completion. A run that vanished without an answer is
/// an internal failure, not a lost upload.
pub async fn await_completion(watch: JoinWatch<CompletionOutcome>) -> CompletionOutcome {
    match await_joined(watch).await {
        Some(outcome) => outcome,
        None => Arc::new(Err(CompletionFailure::new(&s3_error!(
            InternalError,
            "The multipart completion did not produce a result."
        )))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn refused(code: S3ErrorCode) -> CompletionOutcome {
        Arc::new(Err(CompletionFailure::new(&S3Error::new(code))))
    }

    fn failure_code(outcome: &CompletionOutcome) -> Option<S3ErrorCode> {
        outcome
            .as_ref()
            .as_ref()
            .err()
            .map(|failure| failure.code.clone())
    }

    fn part(etag: &str) -> CompleteMultipartPart {
        CompleteMultipartPart {
            part_number: 1,
            etag: Some(etag.to_string()),
            expected_checksums: Vec::new(),
        }
    }

    fn request() -> CompletionRequest {
        CompletionRequest::new(
            &[part("abc")],
            &[],
            MultipartChecksumType::FullObject,
            false,
            None,
        )
    }

    // Another part selection, ETag, assertion or size must neither join nor replay a run.
    #[tokio::test]
    async fn refuses_other_request() {
        let registry = completion_registry();
        let key: CompletionKey = ("bucket".to_string(), "key".to_string(), Ulid::nil());
        let done = registry
            .join_as(key.clone(), request(), async {
                Arc::new(Err(CompletionFailure::new(&S3Error::new(
                    S3ErrorCode::InternalError,
                ))))
            })
            .unwrap();
        let _ = await_completion(done).await;
        let quoted = CompletionRequest::new(
            &[part("\"abc\"")],
            &[],
            MultipartChecksumType::FullObject,
            false,
            None,
        );
        assert_eq!(quoted, request());

        let (gate, blocked) = tokio::sync::oneshot::channel::<()>();
        let running = registry
            .join_as(key.clone(), request(), async move {
                let _ = blocked.await;
                refused(S3ErrorCode::InvalidPart)
            })
            .unwrap();
        for other in [
            CompletionRequest::new(
                &[part("def")],
                &[],
                MultipartChecksumType::FullObject,
                false,
                None,
            ),
            CompletionRequest::new(
                &[part("abc")],
                &[],
                MultipartChecksumType::FullObject,
                false,
                Some(9),
            ),
            CompletionRequest::new(
                &[part("abc")],
                &[],
                MultipartChecksumType::Composite,
                true,
                None,
            ),
        ] {
            assert!(
                registry
                    .join_as(key.clone(), other, async {
                        refused(S3ErrorCode::InvalidPart)
                    })
                    .is_err()
            );
        }
        assert!(
            registry
                .join_as(key, quoted, async { refused(S3ErrorCode::InvalidPart) })
                .is_ok()
        );
        let _ = gate.send(());
        assert_eq!(
            failure_code(&await_completion(running).await),
            Some(S3ErrorCode::InvalidPart)
        );
    }

    // A refused or vanished completion must not answer a retry of the same upload.
    #[tokio::test]
    async fn retry_runs_again() {
        let registry = completion_registry();
        let key: CompletionKey = ("bucket".to_string(), "key".to_string(), Ulid::nil());

        let refusal = registry.join_as(key.clone(), request(), async {
            refused(S3ErrorCode::NoSuchUpload)
        });
        let outcome = await_completion(refusal.unwrap()).await;
        assert_eq!(failure_code(&outcome), Some(S3ErrorCode::NoSuchUpload));

        let vanished = registry.join_as(key.clone(), request(), async {
            panic!("completion failed")
        });
        let outcome = await_completion(vanished.unwrap()).await;
        assert_eq!(failure_code(&outcome), Some(S3ErrorCode::InternalError));

        let retried = registry.join_as(key, request(), async { refused(S3ErrorCode::InvalidPart) });
        let outcome = await_completion(retried.unwrap()).await;
        assert_eq!(failure_code(&outcome), Some(S3ErrorCode::InvalidPart));
    }
}
