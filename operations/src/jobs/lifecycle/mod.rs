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

use std::time::Duration;

use aruna_core::compute_quota::QuotaDenied;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::handle::Handle;
use aruna_core::structs::{JobFamilyError, JobId, JobRecordError};
use aruna_core::task::TaskEvent;
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;
use thiserror::Error;
use tracing::warn;

use super::records::RecordStoreError;

pub mod admit;
pub mod cancel;
pub mod ids;
pub mod ingress;
pub mod outbox;
pub mod plan;
pub mod report;
pub mod reservation;
pub mod routing;
pub mod stage;
pub mod target;
pub mod updates;
pub mod witness;

pub use admit::{AdmitSubmissionConfig, AdmitSubmissionOperation, AdmittedSubmission};
pub use ids::{RequestIdentity, SubmissionRequest};
pub use ingress::{AcceptedSubmission, submit_external_job};
pub use report::{
    AuditPaging, AuditRange, FamilyReport, MAX_AUDIT_PAGE, PagingError, PlanEstimate, family_audit,
    family_report,
};

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
    /// Exact local admission found no capacity for the sealed resources.
    #[error("no local capacity for the sealed execution resources")]
    Capacity,
    /// The same idempotency key is already bound to a different request.
    #[error("idempotency key already bound to job {existing_job_id}")]
    IdempotencyConflict { existing_job_id: JobId },
    /// Standing quota refused this NEW admission; replays are never refused.
    #[error("standing compute quota refused the admission: {0}")]
    QuotaDenied(#[from] QuotaDenied),
    #[error("operation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

#[cfg(test)]
mod tests;

/// Re-arms the replication and witness queues after a restart. Both queues are
/// durable rows, so the timers only have to be brought back, never the work.
pub async fn restore_lifecycle_timers(storage: &StorageHandle, task_handle: &TaskHandle) {
    let pending = crate::jobs::store::iter_prefix_page(
        storage,
        aruna_core::keyspaces::JOB_WITNESS_DEADLINE_KEYSPACE,
        None,
        None,
        1,
        None,
    )
    .await
    .map(|(rows, _)| !rows.is_empty())
    .unwrap_or(false);
    let mut timers = vec![outbox::schedule_outbox_drain(outbox::OUTBOX_RETRY_AFTER)];
    if pending {
        timers.push(witness::schedule_witness_drain(Duration::from_secs(1)));
    }
    for timer in timers {
        if let aruna_core::events::Event::Task(TaskEvent::Error { message, .. }) =
            task_handle.send_effect(timer).await
        {
            warn!(message = %message, "Failed to restore a job lifecycle timer");
        }
    }
}
