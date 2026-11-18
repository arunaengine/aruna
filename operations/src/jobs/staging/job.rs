//! Runs one staging job from its checkpoint: items first, then prefix lanes, the table and the
//! remaining listings, each step committed before the next begins.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::jobs::{JobEffect, JobEvent, JobReport, JobStops};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{
    JobError, JobErrorKind, JobResultPayload, StagingJobCheckpoint, StagingJobPhase,
};
use aruna_core::structs::execution::staging::StagingStrategy;

use super::item::RunItemOperation;
use super::lanes::{self, LanesEnd, LanesOperation};
use super::manifest::ManifestChunkOperation;
use super::table::{self, GzipStream, TableChunkOperation, TableStepEnd};
use super::walk::{
    Ahead, WalkPageOperation, WalkPlace, WalkedPage, parse_queued, queue_read, start_prefix_walk,
    start_queued_walk,
};
use super::{
    ClearQueueOperation, PageFailure, StagingRun, StagingStoreError, committed, decode_main,
    initial_checkpoint, main_read,
};
use crate::connectors::get_connector::{
    GetSourceError, GetSourceInput, GetSourceOperation, GetSourceResult,
};
use crate::jobs::executor::JobRunOutcome;

#[derive(Debug, PartialEq)]
enum Step {
    Read,
    Connector,
    Seed,
    Stops,
    Item,
    ItemCommit,
    Progress,
    Lanes,
    Table,
    Queue,
    Manifest,
    Walk,
    Finish,
    Finished,
    Clear(Box<Ending>),
    StopCheck(Box<JobRunOutcome>),
}

/// What follows a cleared queue.
#[derive(Debug, PartialEq)]
enum Ending {
    /// The outcome is settled.
    Settled(JobRunOutcome),
    /// A failure, which a stop of a table import still overrides.
    Failed(JobRunOutcome),
}

/// Walks a staging job to its end. A cancel or shutdown is checked before every step, and a
/// failure of a table import while the job stops is that stop's outcome and spends no attempt.
#[derive(Debug, PartialEq)]
pub struct StagingJobOperation {
    run: StagingRun,
    checkpoint: Option<StagingJobCheckpoint>,
    ahead: Option<Ahead>,
    gzip: Option<GzipStream>,
    step: Step,
}

impl StagingJobOperation {
    pub fn new(run: StagingRun) -> Self {
        Self {
            run,
            checkpoint: None,
            ahead: None,
            gzip: None,
            step: Step::Read,
        }
    }

    fn checkpoint(&self) -> Result<&StagingJobCheckpoint, StagingStoreError> {
        (self.checkpoint.as_ref())
            .ok_or_else(|| StagingStoreError("the job has no checkpoint".to_string()))
    }

    fn take_checkpoint(&mut self) -> Result<StagingJobCheckpoint, StagingStoreError> {
        (self.checkpoint.take())
            .ok_or_else(|| StagingStoreError("the job has no checkpoint".to_string()))
    }

    fn commit(&mut self, step: Step) -> Turn {
        let checkpoint = self.checkpoint()?;
        match self
            .run
            .commit_call(None, checkpoint, Vec::new(), Vec::new())
        {
            Ok(call) => {
                self.step = step;
                Ok(Next::Call(call))
            }
            Err(error) => self.end(retryable(error)),
        }
    }

    fn turn(&mut self) -> Next<JobRunOutcome> {
        self.step = Step::Stops;
        Next::Call(job_call(JobEffect::Stops))
    }

    /// Takes the next step of the job; `lanes` is false once the lanes are done this turn.
    fn decide(&mut self, lanes_allowed: bool) -> Turn {
        let spec = self.run.spec.clone();
        let checkpoint = self.checkpoint()?.clone();
        let checkpoint = &checkpoint;
        let item = usize::try_from(checkpoint.items_done)
            .ok()
            .and_then(|done| spec.items.get(done));
        if let Some(item) = item {
            let run = self.run.clone();
            let item = RunItemOperation::new(run, item.clone(), checkpoint.clone());
            self.step = Step::Item;
            return Ok(Next::Call(IoCall::child(item)));
        }
        if lanes_allowed && lanes::applies(&spec, checkpoint) {
            let operation = LanesOperation::new(self.run.clone(), checkpoint.clone());
            self.step = Step::Lanes;
            return Ok(Next::Call(IoCall::child(operation)));
        }
        if table::reads_table(checkpoint) {
            // The stream kept from the last step goes on with this one.
            let kept = self.gzip.take();
            let chunk = TableChunkOperation::new(self.run.clone(), checkpoint, kept);
            self.step = Step::Table;
            return Ok(Next::Call(IoCall::child(chunk)));
        }
        if checkpoint.manifest.is_none() && checkpoint.walk.is_none() {
            return self.start_walk();
        }
        self.page()
    }

    /// Starts the next listing: the next queued directory, else the next spec prefix.
    fn start_walk(&mut self) -> Turn {
        let (head, tail) = {
            let checkpoint = self.checkpoint()?;
            (checkpoint.queue_head, checkpoint.queue_tail)
        };
        if head < tail {
            self.step = Step::Queue;
            return Ok(Next::Call(queue_read(self.run.job_id, head)));
        }
        let spec = self.run.spec.clone();
        let checkpoint = (self.checkpoint.as_mut())
            .ok_or_else(|| StagingStoreError("the job has no checkpoint".to_string()))?;
        match start_prefix_walk(&spec, checkpoint) {
            true => Ok(self.turn()),
            false => self.complete(),
        }
    }

    fn page(&mut self) -> Turn {
        let checkpoint = self.checkpoint()?;
        if checkpoint.manifest.is_some() {
            let chunk = ManifestChunkOperation::new(self.run.clone(), checkpoint);
            self.step = Step::Manifest;
            return Ok(Next::Call(IoCall::child(chunk)));
        }
        let checkpoint = checkpoint.clone();
        let ahead = self.ahead.take();
        let walk =
            WalkPageOperation::new(self.run.clone(), WalkPlace::default(), checkpoint, ahead);
        self.step = Step::Walk;
        Ok(Next::Call(IoCall::child(walk)))
    }

    fn complete(&mut self) -> Turn {
        let snapshot = self.run.spec.strategy == StagingStrategy::Snapshot;
        let mut checkpoint = self.take_checkpoint()?;
        checkpoint.phase = StagingJobPhase::Completed;
        checkpoint.current_path = None;
        checkpoint.items_total = Some(checkpoint.items_current);
        if snapshot {
            checkpoint.bytes_total = Some(checkpoint.bytes_current);
        }
        self.checkpoint = Some(checkpoint);
        self.commit(Step::Finish)
    }

    /// Shows the committed checkpoint's progress before the job goes on.
    fn progress(&mut self, committed: bool, step: Step) -> Turn {
        let checkpoint = self.checkpoint()?;
        let report = JobReport {
            current: Some(checkpoint.items_current),
            total: checkpoint.items_total,
            advance: 0,
            checkpoint: committed,
        };
        self.step = step;
        Ok(Next::Call(job_call(JobEffect::Report(report))))
    }

    fn page_done(&mut self, checkpoint: StagingJobCheckpoint) -> Turn {
        self.checkpoint = Some(checkpoint);
        self.progress(true, Step::Progress)
    }

    fn failure(&mut self, failure: PageFailure) -> Turn {
        match failure {
            PageFailure::Retry(message) => self.end(retryable(message)),
            PageFailure::Conflict(message) => {
                self.end(JobRunOutcome::Deferred(JobError::retryable(message)))
            }
            PageFailure::Permanent(message) => self.clear(Ending::Failed(permanent(message))),
        }
    }

    fn clear(&mut self, ending: Ending) -> Turn {
        self.step = Step::Clear(Box::new(ending));
        let clear = ClearQueueOperation::new(self.run.job_id);
        Ok(Next::Call(IoCall::child(clear)))
    }

    /// Ends the job; a failed or deferred table import asks first whether the job was stopped.
    fn end(&mut self, outcome: JobRunOutcome) -> Turn {
        let failed = matches!(
            outcome,
            JobRunOutcome::Failed(_) | JobRunOutcome::Deferred(_)
        );
        if failed && self.run.spec.table.is_some() {
            self.step = Step::StopCheck(Box::new(outcome));
            return Ok(Next::Call(job_call(JobEffect::Stops)));
        }
        Ok(Next::Done(outcome))
    }

    fn succeeded(&self) -> Result<JobRunOutcome, StagingStoreError> {
        let checkpoint = self.checkpoint()?;
        Ok(JobRunOutcome::Succeeded(JobResultPayload::Staging {
            completed_items: checkpoint.items_succeeded,
            failed_items: checkpoint.items_failed,
        }))
    }
}

type Turn = Result<Next<JobRunOutcome>, StagingStoreError>;

fn job_call(effect: JobEffect) -> IoCall {
    IoCall::Effect(Box::new(Effect::Job(effect)))
}

fn retryable(message: impl Into<String>) -> JobRunOutcome {
    JobRunOutcome::Failed(JobError::retryable(message.into()))
}

fn permanent(message: impl Into<String>) -> JobRunOutcome {
    JobRunOutcome::Failed(JobError::permanent(message.into()))
}

/// The outcome a stop gives a failed run: a permanent failure already cleared the job's rows, so
/// it stays terminal through a shutdown.
fn stopped_outcome(stops: JobStops, outcome: JobRunOutcome) -> Result<JobRunOutcome, Ending> {
    if stops.cancelled {
        return Err(Ending::Settled(JobRunOutcome::Cancelled));
    }
    let lasting = matches!(
        &outcome,
        JobRunOutcome::Failed(error) if error.kind == JobErrorKind::Permanent
    );
    Ok(match stops.shutdown && !lasting {
        true => JobRunOutcome::Interrupted,
        false => outcome,
    })
}

fn stops_of(result: IoResult) -> Result<JobStops, IoError> {
    let event = result.event()?;
    match event {
        Event::Job(JobEvent::Stops(stops)) => Ok(stops),
        other => Err(IoError::Unexpected {
            expected: "the job's stop state",
            got: other.label(),
        }),
    }
}

fn reported(result: IoResult) -> Result<(), IoError> {
    let event = result.event()?;
    match event {
        Event::Job(JobEvent::Reported) => Ok(()),
        other => Err(IoError::Unexpected {
            expected: "the recorded progress",
            got: other.label(),
        }),
    }
}

impl Operation for StagingJobOperation {
    type Output = JobRunOutcome;
    type Error = StagingStoreError;

    fn first(&mut self) -> Turn {
        if self.run.spec.strategy == StagingStrategy::Sync {
            return self.end(permanent("sync is not a staging job strategy"));
        }
        Ok(Next::Call(main_read(self.run.job_id)))
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match std::mem::replace(&mut self.step, Step::Stops) {
            Step::Read => {
                let stored = match decode_main(result) {
                    Ok(stored) => stored,
                    Err(error) => return self.end(retryable(error.0)),
                };
                let checkpoint = stored.unwrap_or_else(|| initial_checkpoint(&self.run.spec));
                let flat_known = checkpoint.flat.is_some();
                self.checkpoint = Some(checkpoint);
                if flat_known {
                    return self.commit(Step::Seed);
                }
                self.step = Step::Connector;
                let spec = &self.run.spec;
                Ok(Next::Call(IoCall::child(GetSourceOperation::new(
                    GetSourceInput {
                        group_id: spec.group_id,
                        connector_id: spec.connector_id,
                    },
                ))))
            }
            Step::Connector => match result.child::<GetSourceResult, GetSourceError>()? {
                Ok(source) => {
                    let mut checkpoint = self.take_checkpoint()?;
                    checkpoint.flat = Some(source.connector.kind.lists_flat());
                    self.checkpoint = Some(checkpoint);
                    self.commit(Step::Seed)
                }
                Err(error) => self.clear(Ending::Failed(permanent(error.to_string()))),
            },
            Step::Seed => match committed(result) {
                Ok(()) => Ok(self.turn()),
                Err(error) => self.end(retryable(error)),
            },
            Step::Stops => {
                let stops = stops_of(result)?;
                if stops.cancelled {
                    return self.clear(Ending::Settled(JobRunOutcome::Cancelled));
                }
                if stops.shutdown {
                    return Ok(Next::Done(JobRunOutcome::Interrupted));
                }
                self.decide(true)
            }
            Step::Item => match result.child::<StagingJobCheckpoint, PageFailure>()? {
                Ok(checkpoint) => {
                    self.checkpoint = Some(checkpoint);
                    self.commit(Step::ItemCommit)
                }
                Err(failure) => self.failure(failure),
            },
            Step::ItemCommit => match committed(result) {
                Ok(()) => self.progress(true, Step::Progress),
                Err(error) => self.end(retryable(error)),
            },
            Step::Progress => {
                reported(result)?;
                Ok(self.turn())
            }
            Step::Lanes => match result.child::<LanesEnd, PageFailure>()? {
                Ok(end) => {
                    self.checkpoint = Some(end.main);
                    match end.finished {
                        // Every prefix is walked, so the walk below finds nothing left.
                        true => self.decide(false),
                        false => Ok(self.turn()),
                    }
                }
                Err(failure) => self.failure(failure),
            },
            Step::Table => match result.child::<TableStepEnd, PageFailure>()? {
                Ok(end) => {
                    self.gzip = end.kept;
                    self.page_done(end.checkpoint)
                }
                Err(failure) => self.failure(failure),
            },
            Step::Queue => {
                let directory = match parse_queued(result) {
                    Ok(directory) => directory,
                    Err(error) => return self.end(retryable(error)),
                };
                let mut checkpoint = self.take_checkpoint()?;
                start_queued_walk(&mut checkpoint, directory);
                self.checkpoint = Some(checkpoint);
                Ok(self.turn())
            }
            Step::Manifest => match result.child::<StagingJobCheckpoint, PageFailure>()? {
                Ok(checkpoint) => self.page_done(checkpoint),
                Err(failure) => self.failure(failure),
            },
            Step::Walk => match result.child::<WalkedPage, PageFailure>()? {
                Ok(walked) => {
                    self.ahead = walked.ahead;
                    self.page_done(walked.checkpoint)
                }
                Err(failure) => self.failure(failure),
            },
            Step::Finish => match committed(result) {
                Ok(()) => self.progress(false, Step::Finished),
                Err(error) => self.end(retryable(error)),
            },
            Step::Finished => {
                reported(result)?;
                self.clear(Ending::Settled(self.succeeded()?))
            }
            Step::Clear(ending) => {
                // A failed cleanup is logged by the clear; it never changes the outcome.
                (result.child::<(), IoError>()?).map_err(StagingStoreError::from)?;
                match *ending {
                    Ending::Settled(outcome) => Ok(Next::Done(outcome)),
                    Ending::Failed(outcome) => self.end(outcome),
                }
            }
            Step::StopCheck(outcome) => match stopped_outcome(stops_of(result)?, *outcome) {
                Ok(outcome) => Ok(Next::Done(outcome)),
                Err(ending) => self.clear(ending),
            },
        }
    }
}

#[cfg(test)]
#[path = "job_tests.rs"]
mod tests;
