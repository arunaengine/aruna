//! Changes a job and marks the launch record of its current attempt as executed, in one
//! write transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{done, values};

use super::mutation::{MutateJobCalls, NoGuard, forward};
use super::*;

#[derive(Debug, PartialEq)]
enum StartStep {
    Mutate,
    ReadLaunch,
    WriteLaunch,
}

/// A job mutation that also marks the launch record of the current attempt as executed: its
/// guard passed, so the backend accepted this epoch, even when a cancellation keeps the job
/// from running. Both commit in one write transaction.
pub(crate) struct MarkStartOperation<F> {
    calls: MutateJobCalls<F, NoGuard>,
    started_at_ms: u64,
    record: Option<JobRecord>,
    step: StartStep,
}

impl<F> std::fmt::Debug for MarkStartOperation<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MarkStartOperation")
            .field("calls", &self.calls)
            .finish_non_exhaustive()
    }
}

impl<F> PartialEq for MarkStartOperation<F> {
    fn eq(&self, other: &Self) -> bool {
        self.calls == other.calls
            && self.started_at_ms == other.started_at_ms
            && self.record == other.record
            && self.step == other.step
    }
}

impl<F> MarkStartOperation<F>
where
    F: FnMut(&mut JobRecord) -> Result<JobMutation, JobMutationError> + Send,
{
    pub(crate) fn new(job_id: JobId, mutate: F, started_at_ms: u64) -> Self {
        Self {
            calls: MutateJobCalls::new(job_id, mutate, None),
            started_at_ms,
            record: None,
            step: StartStep::Mutate,
        }
    }

    fn launch_key(&self) -> Option<Key> {
        let record = self.record.as_ref()?;
        let intent = record.attempt_intent.as_ref()?;
        Some(ByteView::from(attempt_control_key(
            record.job_id,
            intent.attempt_epoch,
        )))
    }

    fn finish(&mut self) -> Result<Next<JobRecord>, JobMutationError> {
        self.record
            .take()
            .map(Next::Done)
            .ok_or_else(|| JobMutationError::Storage("launch mark lost its record".into()))
    }
}

impl<F> Operation for MarkStartOperation<F>
where
    F: FnMut(&mut JobRecord) -> Result<JobMutation, JobMutationError> + Send,
{
    type Output = JobRecord;
    type Error = JobMutationError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<JobRecord>, JobMutationError> {
        forward(self.calls.first()?)
    }

    fn next(&mut self, result: IoResult) -> Result<Next<JobRecord>, JobMutationError> {
        match self.step {
            StartStep::Mutate => {
                let record = match self.calls.next(result)? {
                    Next::Done(record) => record,
                    other => return forward(other),
                };
                self.record = Some(record);
                let Some(key) = self.launch_key() else {
                    return self.finish();
                };
                self.step = StartStep::ReadLaunch;
                let row = (LAUNCH_RECORD_KEYSPACE.to_string(), key);
                Ok(Next::Call(IoCall::Read(vec![row])))
            }
            StartStep::ReadLaunch => {
                let [value] = values::<1>(result)?;
                let (Some(value), Some(key)) = (value, self.launch_key()) else {
                    return self.finish();
                };
                let mut launch = LaunchRecord::from_bytes(value.as_ref())
                    .map_err(|error| JobMutationError::Storage(error.to_string()))?;
                if launch.started_at_ms.is_some() {
                    return self.finish();
                }
                launch.started_at_ms = Some(self.started_at_ms);
                let value = launch
                    .to_bytes()
                    .map_err(|error| JobMutationError::Storage(error.to_string()))?;
                self.step = StartStep::WriteLaunch;
                let row = (
                    LAUNCH_RECORD_KEYSPACE.to_string(),
                    key,
                    ByteView::from(value),
                );
                Ok(Next::Call(IoCall::Write(vec![row])))
            }
            StartStep::WriteLaunch => {
                done(result)?;
                self.finish()
            }
        }
    }
}
