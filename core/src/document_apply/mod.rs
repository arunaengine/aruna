//! Decides how replicated document events change stored rows, as plans over those rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod admin;
pub mod create;
pub mod distribution;
pub mod events;
pub mod links;
pub mod quarantine;
pub mod reads;
pub mod registry;
pub mod validate;

use thiserror::Error;

use crate::calls::IoError;
use crate::deferred_topics::DocumentSyncDependency;
use crate::errors::StorageError;
use crate::row_plan::PlanStop;

/// Why applying a replicated document failed.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ApplyError {
    /// The event can never apply: it is malformed or unsupported.
    #[error("{0}")]
    Invalid(String),
    /// The event waits for state that has not replicated yet.
    #[error("{0}")]
    Deferred(String),
    #[error(transparent)]
    Storage(StorageError),
    #[error("{0}")]
    Failed(String),
}

impl From<IoError> for ApplyError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::Storage(error),
            IoError::Conflict => Self::Storage(StorageError::TransactionConflict),
            other => Self::Failed(other.to_string()),
        }
    }
}

impl From<ApplyError> for PlanStop<ApplyError> {
    fn from(error: ApplyError) -> Self {
        Self::Failed(error)
    }
}

/// A step of a plan: its value, or why the plan stopped.
pub type Step<T> = Result<T, PlanStop<ApplyError>>;

/// A failure that names malformed stored or replicated bytes.
pub fn invalid(error: impl ToString) -> ApplyError {
    ApplyError::Invalid(error.to_string())
}

/// How a replicated metadata row fits the placement it was stamped with.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlacementOutcome<T> {
    Accepted(T),
    Deferred(DocumentSyncDependency),
    Rejected,
}
