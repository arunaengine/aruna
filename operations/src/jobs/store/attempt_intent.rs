//! Records the write-ahead intent of a job's next attempt with its attempt control row, in one
//! write transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{done, values};

use super::mutation::{read_record, record_read};
use super::*;

/// The job and the control row of the attempt its intent is recorded for.
#[derive(Debug, PartialEq)]
pub struct AttemptCommit {
    pub record: JobRecord,
    pub control: AttemptControl,
}

#[derive(Debug, PartialEq)]
enum IntentStep {
    Record,
    Control,
    Write,
    Delete,
}

/// Records the intent `draft` under the next attempt epoch, bound to the claim token `token`,
/// with a fresh control row for `execution_id` and the `launch` facts of that epoch. An intent
/// already recorded for the same attempt answers with its rows; a different one, or a
/// committed cancellation, refuses.
#[derive(Debug, PartialEq)]
pub(crate) struct RecordIntentOperation {
    job_id: JobId,
    token: Ulid,
    draft: AttemptIntent,
    execution_id: Ulid,
    launch: Option<LaunchRecord>,
    now_ms: u64,
    record: Option<JobRecord>,
    commit: Option<(JobRecord, AttemptControl)>,
    deletes: JobDeletes,
    step: IntentStep,
}

type IntentNext = Result<Next<AttemptCommit>, JobMutationError>;

impl RecordIntentOperation {
    pub(crate) fn new(
        (job_id, token): (JobId, Ulid),
        draft: AttemptIntent,
        execution_id: Ulid,
        launch: Option<LaunchRecord>,
        now_ms: u64,
    ) -> Self {
        Self {
            job_id,
            token,
            draft,
            execution_id,
            launch,
            now_ms,
            record: None,
            commit: None,
            deletes: Vec::new(),
            step: IntentStep::Record,
        }
    }

    fn judge(&mut self, record: JobRecord) -> IntentNext {
        guard_token(&record, self.token)?;
        if let Some(intent) = &record.attempt_intent {
            let draft = &self.draft;
            if intent.attempt_no != draft.attempt_no
                || intent.external_name != draft.external_name
                || intent.executor_kind != draft.executor_kind
                || intent.pinned_image != draft.pinned_image
            {
                return Err(JobMutationError::IntentConflict);
            }
            let key = ByteView::from(attempt_control_key(self.job_id, intent.attempt_epoch));
            self.record = Some(record);
            self.step = IntentStep::Control;
            let row = (ATTEMPT_CONTROL_KEYSPACE.to_string(), key);
            return Ok(Next::Call(IoCall::Read(vec![row])));
        }
        if record.cancel_requested {
            return Err(JobMutationError::IntentConflict);
        }
        self.record_new(record)
    }

    fn record_new(&mut self, mut record: JobRecord) -> IntentNext {
        let epoch = record.next_attempt_epoch;
        record.next_attempt_epoch = epoch
            .checked_add(1)
            .ok_or(JobMutationError::EpochExhausted)?;
        let control = AttemptControl {
            attempt_epoch: epoch,
            execution_id: self.execution_id,
            controller_generation: 1,
            bound_token: Some(self.token),
            tombstone_ref: None,
            output_commits: Vec::new(),
            output_record: None,
        };
        let old = record.clone();
        record.attempt_intent = Some(AttemptIntent {
            attempt_epoch: epoch,
            ..self.draft.clone()
        });
        record.updated_at_ms = self.now_ms;
        let (mut writes, deletes) = index_deltas(&old, &record).map_err(storage)?;
        let key = ByteView::from(attempt_control_key(self.job_id, epoch));
        let value = ByteView::from(control.to_bytes().map_err(storage)?);
        writes.push((ATTEMPT_CONTROL_KEYSPACE.to_string(), key.clone(), value));
        // The launch facts belong to exactly the epoch this guarded commit assigns.
        if let Some(launch) = &self.launch {
            let value = ByteView::from(launch.to_bytes().map_err(storage)?);
            writes.push((LAUNCH_RECORD_KEYSPACE.to_string(), key, value));
        }
        self.deletes = deletes;
        self.commit = Some((record, control));
        self.step = IntentStep::Write;
        Ok(Next::Call(IoCall::Write(writes)))
    }
}

fn storage(error: impl std::fmt::Display) -> JobMutationError {
    JobMutationError::Storage(error.to_string())
}

impl Operation for RecordIntentOperation {
    type Output = AttemptCommit;
    type Error = JobMutationError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> IntentNext {
        Ok(Next::Call(record_read(self.job_id)))
    }

    fn next(&mut self, result: IoResult) -> IntentNext {
        match self.step {
            IntentStep::Record => {
                let [value] = values::<1>(result)?;
                let record = read_record(self.job_id, value).ok_or(JobMutationError::NotFound)?;
                self.judge(record)
            }
            IntentStep::Control => {
                let [value] = values::<1>(result)?;
                let bytes = value.ok_or(JobMutationError::MissingControl)?;
                let control = AttemptControl::from_bytes(bytes.as_ref()).map_err(storage)?;
                let record = self
                    .record
                    .take()
                    .ok_or_else(|| storage("intent lost its job"))?;
                Ok(Next::Done(AttemptCommit { record, control }))
            }
            IntentStep::Write => {
                done(result)?;
                self.step = IntentStep::Delete;
                Ok(Next::Call(IoCall::Delete(std::mem::take(
                    &mut self.deletes,
                ))))
            }
            IntentStep::Delete => {
                done(result)?;
                let (record, control) = self
                    .commit
                    .take()
                    .ok_or_else(|| storage("intent lost its rows"))?;
                Ok(Next::Done(AttemptCommit { record, control }))
            }
        }
    }
}
