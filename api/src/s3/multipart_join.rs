//! Node-local join point for detached CompleteMultipartUpload work.
//! Concurrent requests for one upload join the same run and answer.

use std::sync::Arc;
use std::time::Duration;

use aruna_operations::s3::complete_upload::CompleteMultipartUploadResult;
use aruna_tasks::join_registry::{JoinRegistry, JoinWatch, await_joined};
use s3s::{S3Error, S3ErrorCode, s3_error};
use ulid::Ulid;

/// Retention lets a retry recover the completed object's ETag and version.
const COMPLETION_RETENTION: Duration = Duration::from_secs(600);

/// Bucket, object key and upload id: an upload id alone would let a request
/// for another key join this completion and skip its own target validation.
pub type CompletionKey = (String, String, Ulid);
pub type CompletionOutcome = Arc<Result<CompleteMultipartUploadResult, CompletionFailure>>;
pub type CompletionRegistry = JoinRegistry<CompletionKey, CompletionOutcome>;

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
