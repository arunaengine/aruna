//! Prunes job families whose index row is due: tombstone first, then the bodies page by page.
//! This part holds the shared rows and the guarded commit.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::{
    FAMILY_CONFLICT_KEYSPACE, FAMILY_PENDING_KEYSPACE, FAMILY_PRUNE_KEYSPACE,
    FAMILY_RECORD_KEYSPACE, INPUT_PAGE_KEYSPACE, LAUNCH_CLOSURE_KEYSPACE, PLAN_EXPLAIN_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::execution::job::record_keys::{prune_due_key, prune_slot_key};
use aruna_core::structs::execution::job::{JobDeletes, JobFamilyId, JobWrites};
use aruna_core::types::Value;
use thiserror::Error;

pub use super::prune_family::PruneFamilyOperation;
pub use super::prune_pass::PruneFamiliesOperation;

/// Due index rows one pass reads, and body rows one page transaction deletes.
pub const FAMILY_PRUNE_PAGE: usize = 256;

/// The keyspaces that hold a family's bodies under its prefix, deleted page by page.
pub(super) const BODY_SPACES: [&str; 6] = [
    FAMILY_RECORD_KEYSPACE,
    FAMILY_CONFLICT_KEYSPACE,
    FAMILY_PENDING_KEYSPACE,
    INPUT_PAGE_KEYSPACE,
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
#[derive(Debug, Default, PartialEq, Eq)]
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

pub(super) fn slot_rows(family: &JobFamilyId, due_ms: u64) -> JobWrites {
    vec![
        (
            FAMILY_PRUNE_KEYSPACE.to_string(),
            prune_due_key(due_ms, family),
            Value::from(&[][..]),
        ),
        (
            FAMILY_PRUNE_KEYSPACE.to_string(),
            prune_slot_key(family),
            Value::from(due_ms.to_be_bytes().as_slice()),
        ),
    ]
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

#[derive(Debug, PartialEq)]
enum CommitStep {
    Guard,
    Delete,
    Write,
}

/// Deletes and writes rows in one transaction. With a guard, the family's slot is read inside it,
/// so a writer that indexed the family meanwhile makes the commit refuse and the pass decides again.
#[derive(Debug, PartialEq)]
pub struct PruneCommitOperation {
    guard: Option<(JobFamilyId, u64)>,
    deletes: JobDeletes,
    writes: JobWrites,
    step: CommitStep,
}

impl PruneCommitOperation {
    pub fn new(guard: Option<(JobFamilyId, u64)>, deletes: JobDeletes, writes: JobWrites) -> Self {
        Self {
            guard,
            deletes,
            writes,
            step: CommitStep::Guard,
        }
    }

    fn apply(&mut self) -> Next<bool> {
        if !self.deletes.is_empty() {
            self.step = CommitStep::Delete;
            return Next::Call(IoCall::Delete(std::mem::take(&mut self.deletes)));
        }
        if !self.writes.is_empty() {
            self.step = CommitStep::Write;
            return Next::Call(IoCall::Write(std::mem::take(&mut self.writes)));
        }
        Next::Done(true)
    }
}

impl Operation for PruneCommitOperation {
    type Output = bool;
    type Error = IoError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<bool>, IoError> {
        match self.guard {
            Some((family, _)) => {
                let slot = (FAMILY_PRUNE_KEYSPACE.to_string(), prune_slot_key(&family));
                Ok(Next::Call(IoCall::Read(vec![slot])))
            }
            None => Ok(self.apply()),
        }
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, IoError> {
        match self.step {
            CommitStep::Guard => {
                let due_ms = self.guard.map(|(_, due_ms)| due_ms);
                if slot_of(result.value()?) != due_ms {
                    return Ok(Next::Done(false));
                }
                Ok(self.apply())
            }
            CommitStep::Delete | CommitStep::Write => {
                result.done()?;
                Ok(self.apply())
            }
        }
    }
}

#[cfg(test)]
#[path = "prune_tests.rs"]
mod tests;
