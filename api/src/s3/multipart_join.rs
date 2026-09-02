//! Node-local join point for CompleteMultipartUpload.
//!
//! Completion is long and expensive, and the request future that starts it may
//! be dropped by a client or an intermediary at any moment. The completion
//! therefore runs as a detached task keyed by upload; a concurrent or later
//! request joins the same task and receives the same answer.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use aruna_operations::s3::complete_multipart_upload::CompleteMultipartUploadResult;
use s3s::{S3Error, S3ErrorCode, s3_error};
use tokio::sync::watch;
use tokio::time::Instant;
use ulid::Ulid;

/// How long a finished completion stays joinable. A retry that arrives after
/// the connection was cut still sees the ETag and version of the object that
/// was created, instead of a `NoSuchUpload` for an upload that is already gone.
const COMPLETION_RETENTION: Duration = Duration::from_secs(600);

type CompletionKey = (String, Ulid);
type CompletionOutcome = Arc<Result<CompleteMultipartUploadResult, CompletionFailure>>;
type CompletionWatch = watch::Receiver<Option<CompletionRecord>>;

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

#[derive(Clone, Debug)]
pub struct CompletionRecord {
    finished: Instant,
    outcome: CompletionOutcome,
}

/// The completions this node currently owns, plus the recently finished ones.
#[derive(Debug, Default)]
pub struct CompletionRegistry {
    entries: Mutex<HashMap<CompletionKey, CompletionWatch>>,
}

impl CompletionRegistry {
    /// Joins the completion already running for `key`, or spawns `work` for it.
    /// The work is detached, so dropping the returned receiver never cancels it.
    pub fn join<F>(&self, key: CompletionKey, work: F) -> CompletionWatch
    where
        F: Future<Output = CompletionOutcome> + Send + 'static,
    {
        let mut entries = self.entries.lock().unwrap_or_else(PoisonError::into_inner);
        entries.retain(|_, watch| {
            watch
                .borrow()
                .as_ref()
                .is_none_or(|record| record.finished.elapsed() < COMPLETION_RETENTION)
        });
        if let Some(watch) = entries.get(&key) {
            return watch.clone();
        }
        let (sender, receiver) = watch::channel(None);
        entries.insert(key, receiver.clone());
        tokio::spawn(async move {
            let outcome = work.await;
            let _ = sender.send(Some(CompletionRecord {
                finished: Instant::now(),
                outcome,
            }));
        });
        receiver
    }
}

/// Waits for the shared completion. A sender that vanished without an answer
/// means the detached task died, which is an internal failure, not a lost upload.
pub async fn await_completion(mut watch: CompletionWatch) -> CompletionOutcome {
    loop {
        if let Some(record) = watch.borrow_and_update().clone() {
            return record.outcome;
        }
        if watch.changed().await.is_err() {
            return Arc::new(Err(CompletionFailure::new(&s3_error!(
                InternalError,
                "The multipart completion did not produce a result."
            ))));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn failure(message: &str) -> CompletionOutcome {
        Arc::new(Err(CompletionFailure::new(&s3_error!(
            InternalError,
            "{}",
            message
        ))))
    }

    // Two requests for one upload must run the completion once and share it.
    #[tokio::test(start_paused = true)]
    async fn joins_running_completion() {
        let registry = CompletionRegistry::default();
        let key = ("bucket".to_string(), Ulid::from_bytes([1u8; 16]));
        let runs = Arc::new(AtomicUsize::new(0));
        let (gate, blocked) = tokio::sync::oneshot::channel();

        let first = registry.join(key.clone(), {
            let runs = runs.clone();
            async move {
                let _ = blocked.await;
                runs.fetch_add(1, Ordering::SeqCst);
                failure("first")
            }
        });
        let second = registry.join(key, {
            let runs = runs.clone();
            async move {
                runs.fetch_add(1, Ordering::SeqCst);
                failure("second")
            }
        });

        let _ = gate.send(());
        let first = await_completion(first).await;
        let second = await_completion(second).await;

        assert_eq!(runs.load(Ordering::SeqCst), 1);
        for outcome in [first, second] {
            let Err(failure) = outcome.as_ref() else {
                panic!("expected the shared failure")
            };
            assert_eq!(failure.message, "first");
        }
    }

    // A retry inside the retention window answers from the finished record; one
    // after it starts fresh work.
    #[tokio::test(start_paused = true)]
    async fn retains_finished_result() {
        let registry = CompletionRegistry::default();
        let key = ("bucket".to_string(), Ulid::from_bytes([2u8; 16]));

        let first = await_completion(registry.join(key.clone(), async { failure("first") })).await;
        assert!(first.is_err());

        tokio::time::advance(COMPLETION_RETENTION / 2).await;
        let retry = await_completion(registry.join(key.clone(), async { failure("second") })).await;
        let Err(retry) = retry.as_ref() else {
            panic!("expected the retained failure")
        };
        assert_eq!(retry.message, "first");

        tokio::time::advance(COMPLETION_RETENTION).await;
        let expired = await_completion(registry.join(key, async { failure("second") })).await;
        let Err(expired) = expired.as_ref() else {
            panic!("expected fresh work")
        };
        assert_eq!(expired.message, "second");
    }
}
