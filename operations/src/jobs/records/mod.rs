//! The append-only job-record store.
//!
//! Cross-node truth about an external job lives here: immutable signed records,
//! keyed once and never rewritten. The mutable [`aruna_core::structs::JobRecord`]
//! row remains a local execution and projection cache during the transition;
//! reduced projections are written into it where the existing status surfaces
//! read it, but it is no longer the sole cross-node truth.
//!
//! * [`append`] admits one record after verifying it against this node's own
//!   view and the evidence it already stored.
//! * [`reduce`] turns an authentic record set into one projection per family,
//!   identically under any order, replay, duplication, or partition merge.
//! * [`project`] caches that projection with a bounded revision.
//! * [`audit`] pages the immutable log with bounded cursors.
//! * [`transport`] carries records and launch offers between nodes.

use aruna_core::effects::FrameBoundsError;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::structs::{JobFamilyError, JobRecordError};
use thiserror::Error;

pub mod admit;
pub mod append;
pub mod audit;
pub mod keys;
pub mod project;
pub mod reduce;
pub mod rows;
pub mod transport;
pub mod verify;

pub use append::{AppendOutcome, AppendRecordConfig, AppendRecordOperation, RecordOrigin};
pub use audit::{AuditPage, AuditScope, FamilyAuditConfig, FamilyAuditOperation};
pub use project::{FamilyRef, ProjectFamilyConfig, ProjectFamilyOperation, ProjectedFamily};
pub use transport::{dispatch_offer, dispatch_record, serve_job_record, serve_launch_offer};

/// Evidence-bearing records one bounded family scan loads before an append.
pub const MAX_FAMILY_EVIDENCE: usize = 256;
/// Records one bounded page of the immutable log returns.
pub const RECORD_PAGE_SIZE: usize = 64;
/// Records one projection may reduce. A family beyond this bound is projected
/// from its first records only, and the truncation is reported, never hidden.
pub const MAX_PROJECTION_RECORDS: usize = 4096;
/// Conflict rows one audit page reports.
pub const MAX_CONFLICT_ROWS: usize = 32;

/// Why an append, projection, or audit did not complete. A local availability
/// failure is never reported as a denial of the record.
#[derive(Debug, PartialEq, Error)]
pub enum RecordStoreError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Record(#[from] JobRecordError),
    #[error(transparent)]
    Bounds(#[from] FrameBoundsError),
    #[error(transparent)]
    Family(#[from] JobFamilyError),
    #[error("realm config document missing")]
    RealmConfigMissing,
    /// This node does not hold the family placement, so it is not an append or
    /// serve authority for it.
    #[error("this node does not hold the job family placement")]
    NotHolder,
    #[error("no job family is bound to that alias")]
    UnknownAlias,
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
