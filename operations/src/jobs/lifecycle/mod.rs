//! The decentralized submission-to-completion lifecycle.
//!
//! Every round here is local authority over replicated immutable records: no
//! leader, no quorum, no global scheduler. A submission is admitted by one
//! family holder, planned independently by every witness, accepted by the
//! target that signs its own receipt, and reduced by whoever is asked.
//!
//! * [`ingress`] normalizes and authorizes a request, derives its identity, and
//!   either admits it here or forwards it one hop to an observed holder.
//! * [`admit`] commits the claim and the immutable spec in one transaction.
//! * [`outbox`] replicates locally published records to the other holders.
//! * [`witness`] ranks holders, seals budgets, plans, and offers launches.
//! * [`target`] reserves exact local capacity and signs the receipt.
//! * [`stage`] moves the sealed input versions to the target.
//! * [`updates`] publishes the monotonic execution chain and its outputs.
//! * [`cancel`] publishes the append-only cancellation intent.
//! * [`routing`] answers external reads from the family projection.

use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::structs::{JobFamilyError, JobId, JobRecordError};
use thiserror::Error;

use super::records::RecordStoreError;

pub mod admit;
pub mod ids;
pub mod ingress;
pub mod outbox;
pub mod plan;
pub mod witness;

pub use admit::{AdmitSubmissionConfig, AdmitSubmissionOperation, AdmittedSubmission};
pub use ids::{RequestIdentity, SubmissionRequest};
pub use ingress::{AcceptedSubmission, submit_external_job};

/// Records of one submission an admission decision scans before it commits.
pub const MAX_SUBMISSION_SCAN: usize = 512;

/// Why one lifecycle round did not complete. An availability failure is never
/// reported as a refusal of the request itself.
#[derive(Debug, PartialEq, Error)]
pub enum LifecycleError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Record(#[from] JobRecordError),
    #[error(transparent)]
    Family(#[from] JobFamilyError),
    #[error(transparent)]
    Store(#[from] RecordStoreError),
    #[error("realm config document missing")]
    RealmConfigMissing,
    /// This node does not hold the submission family, so it may not admit it.
    #[error("this node does not hold the submission family placement")]
    NotHolder,
    /// The same idempotency key is already bound to a different request.
    #[error("idempotency key already bound to job {existing_job_id}")]
    IdempotencyConflict { existing_job_id: JobId },
    #[error("operation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}
