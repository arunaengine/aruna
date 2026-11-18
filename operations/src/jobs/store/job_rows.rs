//! Writes the rows a running job owns, fenced by its claim, each in one write transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{done, values};

use super::mutation::{read_record, record_read};
use super::*;

#[derive(Debug, PartialEq)]
enum RowsStep {
    Fence,
    Write,
    Delete,
}

/// Writes and deletes a running job's own rows. The claim row, not the record a progress flush
/// rewrites, fences the write: a token other than `token` refuses it before any write.
#[derive(Debug, PartialEq)]
pub(crate) struct PutRowsOperation {
    job_id: JobId,
    token: Ulid,
    writes: JobWrites,
    deletes: JobDeletes,
    step: RowsStep,
}

impl PutRowsOperation {
    pub(crate) fn new(job_id: JobId, token: Ulid, writes: JobWrites, deletes: JobDeletes) -> Self {
        Self {
            job_id,
            token,
            writes,
            deletes,
            step: RowsStep::Fence,
        }
    }
}

impl Operation for PutRowsOperation {
    type Output = ();
    type Error = JobMutationError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<()>, JobMutationError> {
        let row = (JOB_CLAIM_KEYSPACE.to_string(), job_record_key(self.job_id));
        Ok(Next::Call(IoCall::Read(vec![row])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, JobMutationError> {
        match self.step {
            RowsStep::Fence => {
                let [claim] = values::<1>(result)?;
                let claim = claim
                    .map(|value| ClaimGuard::from_bytes(value.as_ref()))
                    .transpose()
                    .map_err(|error| JobMutationError::Storage(error.to_string()))?;
                if claim.is_none_or(|claim| claim.claim_token != self.token) {
                    return Err(JobMutationError::TokenMismatch);
                }
                self.step = RowsStep::Write;
                Ok(Next::Call(IoCall::Write(std::mem::take(&mut self.writes))))
            }
            RowsStep::Write => {
                done(result)?;
                self.step = RowsStep::Delete;
                Ok(Next::Call(IoCall::Delete(std::mem::take(
                    &mut self.deletes,
                ))))
            }
            RowsStep::Delete => {
                done(result)?;
                Ok(Next::Done(()))
            }
        }
    }
}

/// Writes one report entry of a running job. A settled job's report is frozen, and only the
/// holder of the claim token `token` may add to it.
#[derive(Debug, PartialEq)]
pub(crate) struct PutEntryOperation {
    job_id: JobId,
    token: Ulid,
    entry: Option<(Key, Value)>,
}

impl PutEntryOperation {
    pub(crate) fn new(job_id: JobId, token: Ulid, entry_key: &[u8], value: Value) -> Self {
        Self {
            job_id,
            token,
            entry: Some((job_entry_key(job_id, entry_key), value)),
        }
    }
}

impl Operation for PutEntryOperation {
    type Output = ();
    type Error = JobMutationError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<()>, JobMutationError> {
        Ok(Next::Call(record_read(self.job_id)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, JobMutationError> {
        let Some((key, value)) = self.entry.take() else {
            done(result)?;
            return Ok(Next::Done(()));
        };
        let [row] = values::<1>(result)?;
        let record = read_record(self.job_id, row).ok_or(JobMutationError::NotFound)?;
        if record.is_settled() {
            return Err(JobMutationError::ReportFrozen);
        }
        guard_token(&record, self.token)?;
        let row = (JOB_ENTRY_KEYSPACE.to_string(), key, value);
        Ok(Next::Call(IoCall::Write(vec![row])))
    }
}
