//! Removes a finished run of one user from this node in one write transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{PrefixPages, done, values};

use super::mutation::{read_record, record_read};
use super::*;

#[derive(Debug, PartialEq)]
enum DeleteStep {
    Record,
    Cleanup,
    Entries,
    Listed,
    Hide,
    Delete,
}

/// Removes a finished run of `user_id`: its row, list index and every local row pruning removes.
/// An execution run goes only once its deletion was `recorded` in its replicated family, whose
/// records stay. Any other settled job leaves the list and becomes due for pruning, which removes
/// its rows page by page, as staging rows can be too many for one transaction.
#[derive(Debug, PartialEq)]
pub(crate) struct DeleteRunOperation {
    user_id: UserId,
    job_id: JobId,
    recorded: bool,
    record: Option<JobRecord>,
    deletes: JobDeletes,
    entries: PrefixPages,
    step: DeleteStep,
}

type DeleteNext = Result<Next<RunDelete>, JobMutationError>;

impl DeleteRunOperation {
    pub(crate) fn new(user_id: UserId, job_id: JobId, recorded: bool) -> Self {
        Self {
            user_id,
            job_id,
            recorded,
            record: None,
            deletes: Vec::new(),
            entries: PrefixPages::new(JOB_ENTRY_KEYSPACE, job_entry_prefix(job_id), JOB_PRUNE_PAGE),
            step: DeleteStep::Record,
        }
    }

    fn judge(&mut self, record: Option<JobRecord>) -> DeleteNext {
        let Some(record) = record else {
            return Ok(Next::Done(RunDelete::NotFound));
        };
        if record.created_by != self.user_id || record.payload.is_internal() {
            return Ok(Next::Done(RunDelete::NotFound));
        }
        let execution = matches!(record.payload, JobPayload::Execution(_));
        let finished = match execution {
            true => record.state.is_terminal(),
            false => record.is_settled(),
        };
        if !finished {
            return Ok(Next::Done(RunDelete::Unfinished(record.state)));
        }
        let call = match execution {
            // A queued cleanup still needs this row to fence and remove the container.
            true => {
                self.step = DeleteStep::Cleanup;
                record_read(cleanup_job_id(self.job_id))
            }
            false => {
                self.step = DeleteStep::Listed;
                let listed =
                    owner_index_key(record.created_by, record.created_at_ms, record.job_id);
                IoCall::Read(vec![(JOB_INDEX_KEYSPACE.to_string(), listed)])
            }
        };
        self.record = Some(record);
        Ok(Next::Call(call))
    }

    fn cleanup(&mut self, cleanup: Option<JobRecord>) -> DeleteNext {
        if cleanup.is_some_and(|cleanup| !cleanup.state.is_terminal()) {
            return Ok(Next::Done(RunDelete::CleanupPending));
        }
        // A run became deletable after its eligibility was read, so no deletion was recorded yet.
        if !self.recorded {
            return Ok(Next::Done(RunDelete::Unrecorded(
                "the run settled while it was deleted; retry".to_string(),
            )));
        }
        self.deletes = prune_delete_entries(self.record()?);
        self.step = DeleteStep::Entries;
        Ok(Next::Call(self.entries.call()))
    }

    fn entries(&mut self, result: IoResult) -> DeleteNext {
        let rows = self.entries.take(result)?;
        let rows = rows
            .into_iter()
            .map(|(key, _)| (JOB_ENTRY_KEYSPACE.to_string(), key));
        self.deletes.extend(rows);
        if !self.entries.done() {
            return Ok(Next::Call(self.entries.call()));
        }
        self.step = DeleteStep::Delete;
        Ok(Next::Call(IoCall::Delete(std::mem::take(
            &mut self.deletes,
        ))))
    }

    fn hide(&mut self, listed: Option<Value>) -> DeleteNext {
        if listed.is_none() {
            return Ok(Next::Done(RunDelete::NotFound));
        }
        let record = self.record()?;
        let mut hidden = record.clone();
        hidden.retention_ms = 0;
        let (writes, mut deletes) = index_deltas(record, &hidden)
            .map_err(|error| JobMutationError::Storage(error.to_string()))?;
        let listed = owner_index_key(record.created_by, record.created_at_ms, record.job_id);
        deletes.push((JOB_INDEX_KEYSPACE.to_string(), listed));
        self.deletes = deletes;
        self.step = DeleteStep::Hide;
        Ok(Next::Call(IoCall::Write(writes)))
    }

    fn record(&self) -> Result<&JobRecord, JobMutationError> {
        self.record
            .as_ref()
            .ok_or_else(|| JobMutationError::Storage("run delete lost its record".into()))
    }
}

impl Operation for DeleteRunOperation {
    type Output = RunDelete;
    type Error = JobMutationError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> DeleteNext {
        Ok(Next::Call(record_read(self.job_id)))
    }

    fn next(&mut self, result: IoResult) -> DeleteNext {
        match self.step {
            DeleteStep::Record => {
                let [value] = values::<1>(result)?;
                let record = read_record(self.job_id, value);
                self.judge(record)
            }
            DeleteStep::Cleanup => {
                let [value] = values::<1>(result)?;
                self.cleanup(read_record(cleanup_job_id(self.job_id), value))
            }
            DeleteStep::Entries => self.entries(result),
            DeleteStep::Listed => {
                let [listed] = values::<1>(result)?;
                self.hide(listed)
            }
            DeleteStep::Hide => {
                done(result)?;
                self.step = DeleteStep::Delete;
                Ok(Next::Call(IoCall::Delete(std::mem::take(
                    &mut self.deletes,
                ))))
            }
            DeleteStep::Delete => {
                done(result)?;
                Ok(Next::Done(RunDelete::Deleted))
            }
        }
    }
}
