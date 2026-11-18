//! Changes one job together with the attempt control row of its current attempt, in one write
//! transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{done, values};

use super::mutation::{read_record, record_read};
use super::*;

#[derive(Debug, PartialEq)]
enum ControlStep {
    Record,
    Control,
    Write,
    Delete,
}

/// Changes a job and the control row of its current attempt with `mutate`, plus `extra` writes
/// that must land with the control row. A job without an attempt intent or control row refuses
/// the change; a skipped change writes nothing and answers the rows as read.
pub(crate) struct MutateControlOperation<F> {
    job_id: JobId,
    extra: JobWrites,
    mutate: F,
    record: Option<JobRecord>,
    epoch: u64,
    changed: Option<ControlChange>,
    deletes: JobDeletes,
    step: ControlStep,
}

impl<F> std::fmt::Debug for MutateControlOperation<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MutateControlOperation")
            .field("job_id", &self.job_id)
            .field("step", &self.step)
            .finish_non_exhaustive()
    }
}

/// Two changes are equal when they stand at the same place; the closure is not compared.
impl<F> PartialEq for MutateControlOperation<F> {
    fn eq(&self, other: &Self) -> bool {
        self.job_id == other.job_id
            && self.extra == other.extra
            && self.record == other.record
            && self.epoch == other.epoch
            && self.changed == other.changed
            && self.deletes == other.deletes
            && self.step == other.step
    }
}

/// The job and control rows after a change, and whether the change was written.
#[derive(Debug, PartialEq)]
pub(crate) struct ControlChange {
    pub(crate) record: JobRecord,
    pub(crate) control: AttemptControl,
    pub(crate) persisted: bool,
}

type ControlNext = Result<Next<ControlChange>, JobMutationError>;

impl<F> MutateControlOperation<F>
where
    F: FnMut(&mut JobRecord, &mut AttemptControl) -> Result<JobMutation, JobMutationError> + Send,
{
    pub(crate) fn new(job_id: JobId, extra: JobWrites, mutate: F) -> Self {
        Self {
            job_id,
            extra,
            mutate,
            record: None,
            epoch: 0,
            changed: None,
            deletes: Vec::new(),
            step: ControlStep::Record,
        }
    }

    fn control_read(&mut self, value: Option<Value>) -> ControlNext {
        let record = read_record(self.job_id, value).ok_or(JobMutationError::NotFound)?;
        self.epoch = record
            .attempt_intent
            .as_ref()
            .map(|intent| intent.attempt_epoch)
            .ok_or(JobMutationError::MissingControl)?;
        self.record = Some(record);
        self.step = ControlStep::Control;
        let key = ByteView::from(attempt_control_key(self.job_id, self.epoch));
        Ok(Next::Call(IoCall::Read(vec![(
            ATTEMPT_CONTROL_KEYSPACE.to_string(),
            key,
        )])))
    }

    fn change(&mut self, value: Option<Value>) -> ControlNext {
        let bytes = value.ok_or(JobMutationError::MissingControl)?;
        let mut control = AttemptControl::from_bytes(bytes.as_ref())
            .map_err(|error| JobMutationError::Storage(error.to_string()))?;
        if control.attempt_epoch != self.epoch {
            return Err(JobMutationError::EpochMismatch);
        }
        let mut record = self
            .record
            .take()
            .ok_or_else(|| JobMutationError::Storage("control change lost its job".into()))?;
        let old = record.clone();
        if matches!((self.mutate)(&mut record, &mut control)?, JobMutation::Skip) {
            return Ok(Next::Done(ControlChange {
                record: old,
                control,
                persisted: false,
            }));
        }
        if old.state != record.state {
            validate_transition(old.execution_class, old.state, record.state)?;
        }
        let (mut writes, deletes) = index_deltas(&old, &record)
            .map_err(|error| JobMutationError::Storage(error.to_string()))?;
        let value = control
            .to_bytes()
            .map_err(|error| JobMutationError::Storage(error.to_string()))?;
        writes.push((
            ATTEMPT_CONTROL_KEYSPACE.to_string(),
            ByteView::from(attempt_control_key(self.job_id, self.epoch)),
            ByteView::from(value),
        ));
        writes.extend(std::mem::take(&mut self.extra));
        self.deletes = deletes;
        self.changed = Some(ControlChange {
            record,
            control,
            persisted: true,
        });
        self.step = ControlStep::Write;
        Ok(Next::Call(IoCall::Write(writes)))
    }
}

impl<F> Operation for MutateControlOperation<F>
where
    F: FnMut(&mut JobRecord, &mut AttemptControl) -> Result<JobMutation, JobMutationError> + Send,
{
    type Output = ControlChange;
    type Error = JobMutationError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> ControlNext {
        Ok(Next::Call(record_read(self.job_id)))
    }

    fn next(&mut self, result: IoResult) -> ControlNext {
        match self.step {
            ControlStep::Record => {
                let [value] = values::<1>(result)?;
                self.control_read(value)
            }
            ControlStep::Control => {
                let [value] = values::<1>(result)?;
                self.change(value)
            }
            ControlStep::Write => {
                done(result)?;
                self.step = ControlStep::Delete;
                Ok(Next::Call(IoCall::Delete(std::mem::take(
                    &mut self.deletes,
                ))))
            }
            ControlStep::Delete => {
                done(result)?;
                self.changed
                    .take()
                    .map(Next::Done)
                    .ok_or_else(|| JobMutationError::Storage("control change lost its rows".into()))
            }
        }
    }
}
