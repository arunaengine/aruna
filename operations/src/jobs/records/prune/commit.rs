//! Commits the rows of one family prune step in one transaction, guarded by the family's slot.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::FAMILY_PRUNE_KEYSPACE;
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::execution::job::family::JobFamilyId;
use aruna_core::structs::execution::job::record_keys::prune_slot_key;
use aruna_core::structs::execution::job::rows::{JobDeletes, JobWrites};

use super::slot_of;

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
