//! Runs an RO-Crate export through its phases and records the end of a push on its link.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::jobs::{JobEffect, JobReport, JobStops};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{
    ArtifactRef, ExportRoCrateResult, JobResultPayload, KeyWait,
};
use aruna_core::types::GroupId;

use super::archive::{
    failure_outcome, permanent, plan_export, report_counts, retryable, validation_rows,
};
use super::assemble::AssembleOperation;
use super::probe::{CandidateFailures, ProbeSourcesOperation, Probed};
use super::publish::{RepositoryStepOperation, WriteRowsOperation};
use super::snapshot::SnapshotOperation;
use super::sources::{AliasState, ResolveEntriesOperation, Resolved, policy_call};
use super::{ExportCheckpoint, ExportFailure, ExportPhase, ExportScope, ProbedEntry};
use crate::jobs::executor::JobRunOutcome;
use crate::jobs::import::{hidden_delete, job_call, reported, stops_of};
use crate::jobs::key_locks::AskWaitsOperation;
use crate::jobs::repository::links::LinkError;
use crate::jobs::repository::push::{ExportEnd, SettlePushOperation, settled_outcome};
use crate::jobs::run_rows::{
    ReadStateOperation, SettledRowsOperation, StateReadError, job_state_key, state_row,
};
use crate::jobs::store::mutation_error::JobMutationError;

/// A runner failure of the export job; its checkpoint stays for the next attempt.
#[derive(Debug, PartialEq, thiserror::Error)]
#[error("{0}")]
pub struct ExportJobError(String);

impl From<IoError> for ExportJobError {
    fn from(error: IoError) -> Self {
        Self(error.to_string())
    }
}

/// What ends the job once its artifact is discarded.
#[derive(Debug, PartialEq)]
enum After {
    Outcome(JobRunOutcome),
    Failure(ExportFailure),
}

#[derive(Debug, PartialEq)]
enum Step {
    Checkpoint,
    Resumed,
    Stops,
    Policies(BTreeSet<GroupId>),
    Snapshot,
    SnapshotReported,
    Resolve,
    Probe,
    Assemble,
    Repository,
    Rows(ArtifactRef),
    Saved,
    Discarded { persist: bool, after: After },
    DiscardSaved(After),
    Waits(ExportFailure),
    Validation(ExportFailure),
}

type Turn = Result<Next<JobRunOutcome>, ExportJobError>;

fn ended(outcome: JobRunOutcome) -> Turn {
    Ok(Next::Done(outcome))
}

/// Runs the export's phases, saving the checkpoint after each. A candidate that fails while the
/// archive is written is set aside and the sources are probed again.
#[derive(Debug, PartialEq)]
struct ExportRunOperation {
    run: ExportScope,
    checkpoint: ExportCheckpoint,
    probed: Option<Vec<ProbedEntry>>,
    failures: CandidateFailures,
    aliases: AliasState,
    step: Step,
}

impl ExportRunOperation {
    fn new(run: ExportScope) -> Self {
        Self {
            run,
            checkpoint: ExportCheckpoint::default(),
            probed: None,
            failures: BTreeMap::new(),
            aliases: AliasState::default(),
            step: Step::Checkpoint,
        }
    }

    fn turn(&mut self) -> Turn {
        self.step = Step::Stops;
        Ok(Next::Call(job_call(JobEffect::Stops)))
    }

    fn report(&mut self, current: Option<u64>, step: Step) -> Turn {
        self.step = step;
        Ok(Next::Call(job_call(JobEffect::Report(JobReport {
            current,
            total: Some(self.checkpoint.entities.len() as u64),
            advance: 0,
            checkpoint: false,
        }))))
    }

    fn resumed(&mut self, stored: Option<ExportCheckpoint>) -> Turn {
        self.checkpoint = stored.unwrap_or_default();
        self.aliases = AliasState::of(&self.checkpoint.entities);
        if self.checkpoint.entities.is_empty() {
            return self.turn();
        }
        let current = match self.checkpoint.phase {
            ExportPhase::Snapshot | ExportPhase::Resolve => 0,
            ExportPhase::Plan | ExportPhase::Assemble | ExportPhase::Publish => {
                self.checkpoint.entities.len() as u64
            }
        };
        self.report(Some(current), Step::Resumed)
    }

    fn stopped(&mut self, stops: JobStops) -> Turn {
        if stops.cancelled {
            return self.discard(true, After::Outcome(JobRunOutcome::Cancelled));
        }
        if stops.shutdown {
            return ended(JobRunOutcome::Interrupted);
        }
        if self.aliases.over_limit() {
            let limit = ExportFailure::Retryable("export alias limit exceeded".to_string());
            return self.fail(limit);
        }
        let pending = self.aliases.candidate_groups(&self.checkpoint.entities);
        if pending.is_empty() {
            return self.dispatch();
        }
        let call = policy_call(self.run.spec.auth_context.realm_id, &pending);
        self.step = Step::Policies(pending);
        Ok(Next::Call(call))
    }

    fn dispatch(&mut self) -> Turn {
        let run = self.run.clone();
        match self.checkpoint.phase {
            ExportPhase::Snapshot => {
                let snapshot = SnapshotOperation::new(
                    run.spec.clone(),
                    run.local_node,
                    self.checkpoint.clone(),
                );
                self.step = Step::Snapshot;
                Ok(Next::Call(IoCall::child(snapshot)))
            }
            ExportPhase::Resolve => {
                let resolve = ResolveEntriesOperation::new(
                    run.sources(),
                    self.checkpoint.entities.clone(),
                    std::mem::take(&mut self.aliases),
                );
                self.step = Step::Resolve;
                Ok(Next::Call(IoCall::child(resolve)))
            }
            ExportPhase::Plan => self.probe(),
            ExportPhase::Assemble if self.probed.is_none() => self.probe(),
            ExportPhase::Assemble => self.assemble(),
            ExportPhase::Publish => match run.spec.destination.clone() {
                Some(destination) => {
                    let step =
                        RepositoryStepOperation::new(run, destination, self.checkpoint.clone());
                    self.step = Step::Repository;
                    Ok(Next::Call(IoCall::child(step)))
                }
                None => self.publish(),
            },
        }
    }

    fn probe(&mut self) -> Turn {
        let probe = ProbeSourcesOperation::new(
            self.run.sources(),
            self.checkpoint.entities.clone(),
            self.failures.clone(),
            std::mem::take(&mut self.aliases),
        );
        self.step = Step::Probe;
        Ok(Next::Call(IoCall::child(probe)))
    }

    fn probed(&mut self, probed: Probed) -> Turn {
        self.checkpoint.entities = probed.entities;
        self.aliases = probed.aliases;
        let assembling = self.checkpoint.phase == ExportPhase::Assemble;
        if let Err(error) = plan_export(&self.run.spec, &mut self.checkpoint, &probed.entries) {
            return self.fail(error);
        }
        self.probed = Some(probed.entries);
        match assembling {
            true => self.assemble(),
            false => self.save(),
        }
    }

    fn assemble(&mut self) -> Turn {
        let Some(entries) = self.probed.take() else {
            return ended(permanent("planned export sources are missing"));
        };
        match AssembleOperation::new(self.run.clone(), &self.checkpoint, entries) {
            Ok(assemble) => {
                self.step = Step::Assemble;
                Ok(Next::Call(IoCall::child(assemble)))
            }
            Err(error) => self.fail(error),
        }
    }

    fn assembled(&mut self, result: Result<ArtifactRef, ExportFailure>) -> Turn {
        match result {
            Ok(artifact) => {
                self.checkpoint.refs.hidden_locations = vec![artifact.location.clone()];
                self.checkpoint.artifact = Some(artifact);
                self.checkpoint.phase = ExportPhase::Publish;
                self.save()
            }
            Err(ExportFailure::Candidate {
                entity_index,
                candidate_index,
                status,
                ..
            }) => {
                let failed = self.failures.entry(entity_index).or_default();
                failed.insert(candidate_index, status);
                self.turn()
            }
            Err(error) => self.fail(error),
        }
    }

    fn publish(&mut self) -> Turn {
        let Some(artifact) = self.checkpoint.artifact.clone() else {
            return ended(permanent("export artifact is missing"));
        };
        let rows = self.checkpoint.report.clone();
        let write = WriteRowsOperation::new(self.run.clone(), rows, true);
        self.step = Step::Rows(artifact);
        Ok(Next::Call(IoCall::child(write)))
    }

    fn published(&mut self, artifact: ArtifactRef) -> Turn {
        let (included, omitted) = report_counts(&self.checkpoint.report);
        ended(JobRunOutcome::Succeeded(JobResultPayload::ExportRoCrate(
            ExportRoCrateResult {
                repository: self.checkpoint.repository.clone(),
                artifact: Some(artifact),
                included,
                omitted,
                report_digest: [0; 32],
            },
        )))
    }

    fn state_call(&self) -> Result<IoCall, String> {
        let job_id = self.run.job_id;
        let row = state_row(job_state_key(job_id), &self.checkpoint)?;
        let token = self.run.claim_token;
        let rows = SettledRowsOperation::new(job_id, token, vec![row], Vec::new());
        Ok(IoCall::child(rows))
    }

    fn save(&mut self) -> Turn {
        match self.state_call() {
            Ok(call) => {
                self.step = Step::Saved;
                Ok(Next::Call(call))
            }
            Err(error) => self.discard(false, After::Outcome(retryable(error))),
        }
    }

    fn fail(&mut self, error: ExportFailure) -> Turn {
        self.discard(false, After::Failure(error))
    }

    /// Deletes the artifact; the delete is best effort, and what follows decides the outcome.
    fn discard(&mut self, persist: bool, after: After) -> Turn {
        let delete = (self.checkpoint.artifact.as_ref())
            .and_then(|artifact| hidden_delete(&artifact.location).ok());
        match delete {
            Some(delete) => {
                self.step = Step::Discarded { persist, after };
                Ok(Next::Call(delete))
            }
            None => self.discarded(persist, after),
        }
    }

    fn discarded(&mut self, persist: bool, after: After) -> Turn {
        self.checkpoint.artifact = None;
        self.checkpoint.refs.hidden_locations.clear();
        if persist && let Ok(call) = self.state_call() {
            self.step = Step::DiscardSaved(after);
            return Ok(Next::Call(call));
        }
        self.conclude(after)
    }

    fn conclude(&mut self, after: After) -> Turn {
        let error = match after {
            After::Outcome(outcome) => return ended(outcome),
            After::Failure(error) => error,
        };
        match &error {
            // A remote holder with a locked key wakes this job once it registers there.
            ExportFailure::RemoteLocked {
                node_id,
                target,
                auth,
            } => {
                let ask = AskWaitsOperation::new(
                    auth,
                    self.run.job_id,
                    *node_id,
                    std::slice::from_ref(target),
                    self.run.scope.node,
                );
                self.step = Step::Waits(error);
                Ok(Next::Call(IoCall::child(ask)))
            }
            ExportFailure::Validation(violations) => {
                let rows = validation_rows(violations);
                let write = WriteRowsOperation::new(self.run.clone(), rows, false);
                self.step = Step::Validation(error);
                Ok(Next::Call(IoCall::child(write)))
            }
            _ => ended(failure_outcome(error)),
        }
    }
}

impl Operation for ExportRunOperation {
    type Output = JobRunOutcome;
    type Error = ExportJobError;

    fn first(&mut self) -> Turn {
        let read = ReadStateOperation::<ExportCheckpoint>::new(self.run.job_id);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match std::mem::replace(&mut self.step, Step::Saved) {
            Step::Checkpoint => {
                match result.child::<Option<ExportCheckpoint>, StateReadError>()? {
                    Ok(stored) => self.resumed(stored),
                    Err(error) => ended(retryable(error.0)),
                }
            }
            Step::Resumed => {
                reported(result)?;
                self.turn()
            }
            Step::Stops => {
                let stops = stops_of(result)?;
                self.stopped(stops)
            }
            Step::Policies(pending) => {
                let realm_id = self.run.spec.auth_context.realm_id;
                match self.aliases.keep_policies(realm_id, &pending, result) {
                    Ok(()) => self.dispatch(),
                    Err(error) => self.fail(error),
                }
            }
            Step::Snapshot => match result.child::<ExportCheckpoint, ExportFailure>()? {
                Ok(checkpoint) => {
                    self.checkpoint = checkpoint;
                    self.report(None, Step::SnapshotReported)
                }
                Err(error) => self.fail(error),
            },
            Step::SnapshotReported => {
                reported(result)?;
                self.save()
            }
            Step::Resolve => match result.child::<Resolved, ExportFailure>()? {
                Ok(resolved) => {
                    self.checkpoint.entities = resolved.entities;
                    self.aliases = resolved.aliases;
                    self.checkpoint.phase = ExportPhase::Plan;
                    self.save()
                }
                Err(error) => self.fail(error),
            },
            Step::Probe => match result.child::<Probed, ExportFailure>()? {
                Ok(probed) => self.probed(probed),
                Err(error) => self.fail(error),
            },
            Step::Assemble => {
                let assembled = result.child::<ArtifactRef, ExportFailure>()?;
                self.assembled(assembled)
            }
            Step::Repository => match result.child::<ExportCheckpoint, ExportFailure>()? {
                Ok(checkpoint) => {
                    self.checkpoint = checkpoint;
                    self.publish()
                }
                Err(error) => ended(failure_outcome(error)),
            },
            Step::Rows(artifact) => match result.child::<(), ExportFailure>()? {
                Ok(()) => self.published(artifact),
                Err(ExportFailure::Cancelled) => {
                    self.discard(true, After::Outcome(JobRunOutcome::Cancelled))
                }
                Err(error) => ended(failure_outcome(error)),
            },
            Step::Saved => match result.child::<(), JobMutationError>()? {
                Ok(()) => self.turn(),
                Err(error) => {
                    let outcome = retryable(error.to_string());
                    self.discard(false, After::Outcome(outcome))
                }
            },
            Step::Discarded { persist, after } => self.discarded(persist, after),
            Step::DiscardSaved(after) => self.conclude(after),
            Step::Waits(error) => {
                let waits = result.child::<Vec<KeyWait>, IoError>()?.unwrap_or_default();
                match waits.is_empty() {
                    true => ended(failure_outcome(error)),
                    false => ended(JobRunOutcome::AwaitingKey(waits)),
                }
            }
            Step::Validation(error) => match result.child::<(), ExportFailure>()? {
                Ok(()) => ended(failure_outcome(error)),
                Err(ExportFailure::Retryable(message)) => ended(retryable(message)),
                Err(other) => ended(failure_outcome(other)),
            },
        }
    }
}

/// Runs an export and, for a push of a link, records how it ended.
#[derive(Debug, PartialEq)]
pub struct ExportJobOperation {
    run: ExportScope,
    outcome: Option<JobRunOutcome>,
}

impl ExportJobOperation {
    pub fn new(run: ExportScope) -> Self {
        Self { run, outcome: None }
    }
}

impl Operation for ExportJobOperation {
    type Output = JobRunOutcome;
    type Error = ExportJobError;

    fn first(&mut self) -> Turn {
        let run = ExportRunOperation::new(self.run.clone());
        Ok(Next::Call(IoCall::child(run)))
    }

    fn next(&mut self, result: IoResult) -> Turn {
        if let Some(outcome) = self.outcome.take() {
            let settled = result.child::<Option<String>, LinkError>()?;
            return ended(settled_outcome(outcome, settled));
        }
        let outcome = result.child::<JobRunOutcome, ExportJobError>()??;
        let spec = &self.run.spec;
        let link = (spec.destination.as_ref()).and_then(|target| target.link.as_ref());
        let Some(link) = link else {
            return ended(outcome);
        };
        let settle = SettlePushOperation::new(
            self.run.scope.clone(),
            self.run.job_id,
            spec.document_id,
            link.clone(),
            ExportEnd::of(&outcome),
        );
        self.outcome = Some(outcome);
        Ok(Next::Call(IoCall::child(settle)))
    }
}

#[cfg(test)]
#[path = "job_tests.rs"]
mod tests;
