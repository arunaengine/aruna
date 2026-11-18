//! Runs an RO-Crate import through its phases, saving the checkpoint after every step, so a
//! later attempt resumes where this one stopped.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::jobs::{JobEffect, JobReport, JobStops};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{JobError, rocrate_plan_key};

use super::acquire::{AcquireSourceOperation, AcquiredSource};
use super::finish::{
    CleanupOperation, CreateDatasetOperation, RewriteCrateOperation, UpdateDatasetOperation,
};
use super::plan::{
    ValidateSourceOperation, Validated, federated_call, federated_result, inspect_call, inspected,
};
use super::reports::{
    MarkEntriesOperation, SystemRowsOperation, Unreached, phase_row, validation_rows,
};
use super::write::WriteEntryOperation;
use super::{
    ImportCheckpoint, ImportFailure, ImportPhase, ImportPlan, ImportRun, SOURCE_TIMEOUT,
    failure_message, import_outcome, job_call, reported, retryable_error, stops_of, updates_link,
    validation_message,
};
use crate::federation::import::{ConfirmSourceOperation, ImportError};
use crate::jobs::executor::JobRunOutcome;
use crate::jobs::run_rows::{
    ReadStateOperation, SettledRowsOperation, StateReadError, job_state_key, state_row,
};
use crate::jobs::store::mutation_error::JobMutationError;

/// A runner failure of the import job; its checkpoint stays for the next attempt.
#[derive(Debug, PartialEq, thiserror::Error)]
#[error("{0}")]
pub struct ImportJobError(String);

impl From<IoError> for ImportJobError {
    fn from(error: IoError) -> Self {
        Self(error.to_string())
    }
}

#[derive(Debug, PartialEq)]
enum Step {
    Checkpoint,
    Plan {
        required: bool,
    },
    Stops,
    Recheck,
    Confirm,
    Phase,
    /// The failure rows; a failed write then marks the entries it did not reach.
    Rows(Option<String>),
    Marked,
    Report,
    Saved {
        resumed: bool,
    },
}

/// Imports an RO-Crate archive: acquires and validates it, writes its entries one by one, creates
/// or updates the dataset and cleans up. A failure rolls the written objects back.
#[derive(Debug, PartialEq)]
pub struct ImportJobOperation {
    run: ImportRun,
    checkpoint: ImportCheckpoint,
    plan: Option<ImportPlan>,
    /// Each run, a start or a resume, first needs the source's confirmation of the grant.
    confirmed: bool,
    now_ms: u64,
    step: Step,
}

type Turn = Result<Next<JobRunOutcome>, ImportJobError>;

fn give_up(message: impl Into<String>) -> Turn {
    Ok(Next::Done(retryable_error(message)))
}

fn missing_plan() -> ImportFailure {
    ImportFailure::Permanent("import plan is missing".to_string())
}

impl ImportJobOperation {
    pub fn new(run: ImportRun) -> Self {
        Self {
            run,
            checkpoint: ImportCheckpoint::default(),
            plan: None,
            confirmed: false,
            now_ms: 0,
            step: Step::Checkpoint,
        }
    }

    fn save(&mut self, resumed: bool) -> Turn {
        let (job_id, token) = (self.run.job_id, self.run.claim_token);
        let row = match state_row(job_state_key(job_id), &self.checkpoint) {
            Ok(row) => row,
            Err(error) => return give_up(error),
        };
        self.step = Step::Saved { resumed };
        let rows = SettledRowsOperation::new(job_id, token, vec![row], Vec::new());
        Ok(Next::Call(IoCall::child(rows)))
    }

    /// Reads the plan the checkpoint's phase needs. A resumed cleanup rolls back with the plan;
    /// a failure before planning simply has none.
    fn resume(&mut self) -> Turn {
        let required = match self.checkpoint.phase {
            ImportPhase::Write | ImportPhase::Rewrite | ImportPhase::Create => true,
            ImportPhase::Cleanup => false,
            ImportPhase::Acquire
            | ImportPhase::Inspect
            | ImportPhase::Validate
            | ImportPhase::Done => return self.turn(),
        };
        self.step = Step::Plan { required };
        let key = rocrate_plan_key(self.run.job_id);
        let read = ReadStateOperation::<ImportPlan>::at(key);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn turn(&mut self) -> Turn {
        self.step = Step::Stops;
        Ok(Next::Call(job_call(JobEffect::Stops)))
    }

    fn stopped(&mut self, stops: JobStops) -> Turn {
        if stops.shutdown {
            return Ok(Next::Done(JobRunOutcome::Interrupted));
        }
        let open = !matches!(
            self.checkpoint.phase,
            ImportPhase::Cleanup | ImportPhase::Done
        );
        if stops.cancelled && open {
            return self.cancel();
        }
        self.dispatch()
    }

    fn cancel(&mut self) -> Turn {
        self.checkpoint.cancelled = true;
        self.mark(Unreached::Cancelled)
    }

    fn mark(&mut self, unreached: Unreached) -> Turn {
        let mark = MarkEntriesOperation::new(
            self.run.clone(),
            self.plan.as_ref(),
            self.checkpoint.next_entry,
            unreached,
        );
        self.step = Step::Marked;
        Ok(Next::Call(IoCall::child(mark)))
    }

    fn clean_up(&mut self) -> Turn {
        self.checkpoint.phase = ImportPhase::Cleanup;
        self.save(false)
    }

    fn dispatch(&mut self) -> Turn {
        let phase = self.checkpoint.phase;
        if phase == ImportPhase::Done {
            let outcome = import_outcome(&self.checkpoint, &self.run.spec);
            return Ok(Next::Done(outcome));
        }
        if !self.confirmed && phase != ImportPhase::Cleanup {
            self.confirmed = true;
            self.step = Step::Recheck;
            return Ok(Next::Call(federated_call(&self.run, self.now_ms)));
        }
        match self.phase_call(phase) {
            Ok(call) => {
                self.step = Step::Phase;
                Ok(Next::Call(call))
            }
            Err(failure) => self.failed(failure),
        }
    }

    fn phase_call(&self, phase: ImportPhase) -> Result<IoCall, ImportFailure> {
        let run = self.run.clone();
        let checkpoint = self.checkpoint.clone();
        let plan = self.plan.as_ref().ok_or_else(missing_plan);
        Ok(match phase {
            ImportPhase::Acquire => IoCall::child(AcquireSourceOperation::new(run)),
            ImportPhase::Inspect => inspect_call(&run, &checkpoint)?,
            ImportPhase::Validate => IoCall::child(ValidateSourceOperation::new(run, checkpoint)),
            ImportPhase::Write => IoCall::child(WriteEntryOperation::new(run, checkpoint, plan?)),
            ImportPhase::Rewrite => {
                IoCall::child(RewriteCrateOperation::new(run, checkpoint, plan?.clone()))
            }
            ImportPhase::Create if updates_link(&run.spec) => {
                plan?;
                IoCall::child(UpdateDatasetOperation::new(run, checkpoint))
            }
            ImportPhase::Create => IoCall::child(CreateDatasetOperation::new(run, checkpoint)),
            ImportPhase::Cleanup | ImportPhase::Done => {
                let plan = self.plan.clone();
                IoCall::child(CleanupOperation::new(run, checkpoint, plan))
            }
        })
    }

    /// Takes the phase's answer into the checkpoint.
    fn phase_done(&mut self, result: IoResult) -> Result<(), ImportFailure> {
        let checkpoint = &mut self.checkpoint;
        match checkpoint.phase {
            ImportPhase::Acquire => {
                let acquired = result.child::<AcquiredSource, ImportFailure>()??;
                checkpoint.pull = acquired.pull;
                checkpoint.identifiers = acquired.identifiers;
                checkpoint.refs.hidden_locations = vec![acquired.input.location.clone()];
                checkpoint.input = Some(acquired.input);
                checkpoint.phase = ImportPhase::Inspect;
            }
            ImportPhase::Inspect => {
                let (inspection, metadata_json) = inspected(result)?;
                checkpoint.inspection = Some(inspection);
                checkpoint.metadata_json = Some(metadata_json);
                checkpoint.phase = ImportPhase::Validate;
            }
            ImportPhase::Validate => {
                let validated = result.child::<Validated, ImportFailure>()??;
                *checkpoint = validated.checkpoint;
                self.plan = Some(validated.plan);
            }
            _ => *checkpoint = result.child::<ImportCheckpoint, ImportFailure>()??,
        }
        Ok(())
    }

    /// A finished step shows in the job's progress and is saved.
    fn advanced(&mut self) -> Turn {
        let checkpoint = &self.checkpoint;
        let total = checkpoint.entries_total;
        self.step = Step::Report;
        Ok(Next::Call(job_call(JobEffect::Report(JobReport {
            current: Some(checkpoint.next_entry as u64),
            total: (total > 0).then_some(total),
            advance: 0,
            checkpoint: false,
        }))))
    }

    fn settled(&mut self, outcome: Result<(), ImportFailure>) -> Turn {
        match outcome {
            Ok(()) => self.advanced(),
            Err(failure) => self.failed(failure),
        }
    }

    fn failed(&mut self, failure: ImportFailure) -> Turn {
        let phase = self.checkpoint.phase;
        let (rows, unreached) = match failure {
            ImportFailure::Retryable(error) => return give_up(error),
            ImportFailure::Cancelled => return self.cancel(),
            ImportFailure::Interrupted => return Ok(Next::Done(JobRunOutcome::Interrupted)),
            ImportFailure::Deferred(error) => {
                let deferred = JobRunOutcome::Deferred(JobError::retryable(error));
                return Ok(Next::Done(deferred));
            }
            ImportFailure::Validation(violations) => {
                self.checkpoint.failure = Some(validation_message(&violations));
                (validation_rows(&violations), None)
            }
            ImportFailure::Permanent(error) => {
                let rows = vec![phase_row(phase, &error)];
                self.checkpoint.failure = Some(error.clone());
                (rows, (phase == ImportPhase::Write).then_some(error))
            }
        };
        self.step = Step::Rows(unreached);
        let rows = SystemRowsOperation::new(self.run.clone(), rows);
        Ok(Next::Call(IoCall::child(rows)))
    }
}

impl Operation for ImportJobOperation {
    type Output = JobRunOutcome;
    type Error = ImportJobError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Turn {
        let read = ReadStateOperation::<ImportCheckpoint>::new(self.run.job_id);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match std::mem::replace(&mut self.step, Step::Report) {
            Step::Checkpoint => {
                match result.child::<Option<ImportCheckpoint>, StateReadError>()? {
                    Ok(stored) => self.checkpoint = stored.unwrap_or_default(),
                    Err(error) => return give_up(error.0),
                }
                self.save(true)
            }
            Step::Plan { required } => {
                match result.child::<Option<ImportPlan>, StateReadError>()? {
                    Ok(None) if required => {
                        let error = JobError::permanent("import plan is missing");
                        Ok(Next::Done(JobRunOutcome::Failed(error)))
                    }
                    Ok(plan) => {
                        self.plan = plan;
                        self.turn()
                    }
                    Err(error) => give_up(error.0),
                }
            }
            Step::Stops => {
                let stops = stops_of(result)?;
                self.stopped(stops)
            }
            Step::Recheck => match federated_result(result) {
                Ok(()) => {
                    self.step = Step::Confirm;
                    let confirm = ConfirmSourceOperation::new(&self.run.spec, SOURCE_TIMEOUT);
                    Ok(Next::Call(IoCall::child(confirm)))
                }
                Err(failure) => self.failed(failure),
            },
            // A refusal fails the job; an unreachable source waits for the next pull or push.
            Step::Confirm => {
                let confirmed = (result.child::<(), ImportError>()?).map_err(|error| match error {
                    ImportError::Storage(error) => ImportFailure::Retryable(error),
                    ImportError::Unreachable(_) => ImportFailure::Deferred(error.to_string()),
                    error => ImportFailure::Permanent(error.to_string()),
                });
                self.settled(confirmed)
            }
            Step::Phase => {
                let outcome = self.phase_done(result);
                self.settled(outcome)
            }
            Step::Rows(unreached) => match result.child::<(), ImportFailure>()? {
                Ok(()) => match unreached {
                    Some(message) => self.mark(Unreached::WriteFailed(message)),
                    None => self.clean_up(),
                },
                Err(failure) => give_up(failure_message(failure)),
            },
            Step::Marked => match result.child::<(), ImportFailure>()? {
                Ok(()) => self.clean_up(),
                Err(failure) => give_up(failure_message(failure)),
            },
            Step::Report => {
                reported(result)?;
                self.save(false)
            }
            Step::Saved { resumed } => match result.child::<(), JobMutationError>()? {
                Ok(()) if resumed => self.resume(),
                Ok(()) => self.turn(),
                Err(error) => give_up(error.to_string()),
            },
        }
    }
}

#[cfg(test)]
#[path = "job_tests.rs"]
mod tests;
