//! Prunes job families whose index row is due: tombstone first, then the bodies page by page.
//! This part holds the shared rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::calls::IoError;
use aruna_core::keyspaces::{
    FAMILY_CONFLICT_KEYSPACE, FAMILY_PENDING_KEYSPACE, FAMILY_PRUNE_KEYSPACE,
    FAMILY_RECORD_KEYSPACE, LAUNCH_CLOSURE_KEYSPACE, PLAN_EXPLAIN_KEYSPACE,
};
use aruna_core::structs::execution::job::family::JobFamilyId;
use aruna_core::structs::execution::job::record_keys::{prune_due_key, prune_slot_key};
use aruna_core::structs::execution::job::rows::JobDeletes;
use aruna_core::types::Value;
use thiserror::Error;

mod commit;
mod family;
pub mod pages;
pub mod pass;

/// Due index rows one pass reads, and body rows one page transaction deletes.
pub const FAMILY_PRUNE_PAGE: usize = 256;

/// The keyspaces that hold a family's bodies under its prefix, deleted page by page. Input pages
/// are keyed by submission and go first, through their own keys.
pub(super) const BODY_SPACES: [&str; 5] = [
    FAMILY_RECORD_KEYSPACE,
    FAMILY_CONFLICT_KEYSPACE,
    FAMILY_PENDING_KEYSPACE,
    PLAN_EXPLAIN_KEYSPACE,
    LAUNCH_CLOSURE_KEYSPACE,
];

#[derive(Debug, Error, PartialEq)]
pub enum PruneError {
    #[error(transparent)]
    Io(#[from] IoError),
    #[error("family prune read failed: {0}")]
    Read(String),
}

impl From<aruna_core::errors::ConversionError> for PruneError {
    fn from(error: aruna_core::errors::ConversionError) -> Self {
        Self::Read(error.to_string())
    }
}

/// What one pass over the due rows did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FamilyPruneOutcome {
    /// Families whose last body page this pass deleted.
    pub pruned: usize,
    pub has_more: bool,
    pub next_due_after: Option<Duration>,
}

/// What one due row led to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PruneStep {
    /// The family is gone here: the last page or the last pending row went.
    Pruned,
    /// A body page went; more follow.
    Paged,
    /// The row moved to a later due time, or was stale.
    Moved,
}

pub(super) fn index_rows(family: &JobFamilyId, due_ms: u64) -> JobDeletes {
    vec![
        (
            FAMILY_PRUNE_KEYSPACE.to_string(),
            prune_due_key(due_ms, family),
        ),
        (FAMILY_PRUNE_KEYSPACE.to_string(), prune_slot_key(family)),
    ]
}

pub(super) fn slot_of(value: Option<Value>) -> Option<u64> {
    value.and_then(|value| {
        <[u8; 8]>::try_from(value.as_ref())
            .ok()
            .map(u64::from_be_bytes)
    })
}

#[cfg(test)]
mod tests;
