//! Node-local join point for CompleteMultipartUpload.
//!
//! Completion is long and expensive, and the request future that starts it may
//! be dropped by a client or an intermediary at any moment. The completion
//! therefore runs detached under its upload key; a concurrent or later request
//! joins the same run and receives the same answer.

use std::sync::Arc;
use std::time::Duration;

use aruna_operations::s3::complete_multipart_upload::CompleteMultipartUploadResult;
use aruna_tasks::join_registry::{JoinRegistry, JoinWatch, await_joined};
use s3s::{S3Error, S3ErrorCode, s3_error};
use ulid::Ulid;

/// How long a finished completion stays joinable. A retry that arrives after
/// the connection was cut still sees the ETag and version of the object that
/// was created, instead of a `NoSuchUpload` for an upload that is already gone.
const COMPLETION_RETENTION: Duration = Duration::from_secs(600);

pub type CompletionKey = (String, Ulid);
pub type CompletionOutcome = Arc<Result<CompleteMultipartUploadResult, CompletionFailure>>;
pub type CompletionRegistry = JoinRegistry<CompletionKey, CompletionOutcome>;

pub fn completion_registry() -> CompletionRegistry {
    JoinRegistry::new(COMPLETION_RETENTION)
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
