//! Owns the decentralized submission lifecycle: no leader, no quorum, no global scheduler.
//! One family holder admits a submission, every witness plans it, and its target receipts it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::IoError;
use aruna_core::compute::quota::QuotaDenied;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::structs::execution::job::{JobFamilyId, JobId, JobRecordError};
use aruna_core::structs::identity::realm::{JobFamilyError, RealmConfigDocument, RealmId};
use aruna_core::types::Value;
use thiserror::Error;

use super::records::RecordStoreError;
use super::records::verify::FamilyView;

pub mod admit;
pub mod admit_here;
pub mod alias;
pub mod cancel;
pub mod chain;
mod fetch;
pub mod held;
pub mod ids;
pub mod ingress;
pub(crate) mod launch_checks;
pub mod offer;
pub mod outbox;
pub mod plan;
mod policies;
pub mod publish;
pub mod report;
pub mod reservation;
pub mod routing;
pub mod stage;
mod submission_rules;
pub mod target;
pub mod updates;
pub mod witness;
pub mod witness_arm;

/// Why one lifecycle round did not complete. An availability failure is never
/// reported as a refusal of the request itself.
#[derive(Debug, PartialEq, Error)]
pub enum LifecycleError {
    #[error(transparent)]
    GroupWrite(#[from] aruna_core::structs::identity::group_delete::GroupWriteError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("a row did not encode or decode: {0}")]
    Codec(String),
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
    /// Exact local admission found no capacity for the stored resources.
    #[error("no local capacity for the stored execution resources")]
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
        got: &'static str,
    },
}

impl From<IoError> for LifecycleError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::Storage(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::Storage(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "call".to_string(),
                expected,
                got,
            },
            IoError::Aborted | IoError::Hung { .. } | IoError::Unfinished => Self::NotFinished,
            IoError::Codec(error) => Self::Codec(error),
        }
    }
}

impl LifecycleError {
    /// An event the operation's current state does not accept.
    pub(crate) fn unexpected(
        state: &impl std::fmt::Debug,
        expected: &'static str,
        got: &'static str,
    ) -> Self {
        Self::UnexpectedEvent {
            state: format!("{state:?}"),
            expected,
            got,
        }
    }
}

/// The family view a realm config row gives; a missing row is `RealmConfigMissing`.
fn stored_view(
    value: Option<Value>,
    realm_id: RealmId,
    family: JobFamilyId,
) -> Result<Option<FamilyView>, LifecycleError> {
    let value = value.ok_or(LifecycleError::RealmConfigMissing)?;
    let config = RealmConfigDocument::from_bytes(&value)?;
    Ok(FamilyView::resolve(&config, realm_id, family))
}

#[cfg(test)]
mod tests;
