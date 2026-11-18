//! Decides how replicated document events change stored rows, as plans over those rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod admin;
pub mod batch;
pub mod create;
pub mod distribution;
pub mod events;
#[cfg(test)]
mod fixtures;
pub mod links;
pub mod overflow;
#[cfg(test)]
mod plan_tests;
pub mod quarantine;
pub mod reads;
pub mod reconcile;
pub mod registry;
pub mod validate;

use byteview::ByteView;
use thiserror::Error;

use crate::calls::{ChildOutcome, IoCall, IoError};
use crate::deferred_topics::DocumentSyncDependency;
use crate::errors::StorageError;
use crate::events::Event;
use crate::operation::boxed_retry;
use crate::row_plan::{PlanOperation, PlanStop, RowPlan};
use crate::types::Key;

/// The row of a topic's applied cursor in the applied-ops key space.
pub fn topic_cursor_key(topic_id: irokle::TopicId) -> Key {
    let mut key = b"topic-cursor/".to_vec();
    key.extend_from_slice(topic_id.as_bytes());
    ByteView::from(key)
}

/// The row of the deferred-topic registry; the name is kept so stored registries decode.
pub fn deferred_topics_key() -> Key {
    ByteView::from(b"deferred-admin-topics".to_vec())
}

/// Runs a plan as a child that reruns after each lost conflict it allows.
pub fn plan_call<P>(plan: P) -> IoCall
where
    P: RowPlan<Error = ApplyError> + Clone + 'static,
    P::Output: PartialEq + 'static,
{
    IoCall::Child(boxed_retry(
        move || PlanOperation::new(plan.clone()),
        b"document-apply",
        |result| Event::Child(ChildOutcome::new(result)),
    ))
}

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
