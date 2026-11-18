//! Owns the execution job workflow and runs one execution job from prepare to finish.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod attempt;
pub mod cleanup;
pub mod compute;
pub mod credential;
pub mod finalize;
pub mod input_view;
pub mod inputs;
pub mod inventory;
pub mod outputs;
pub mod prepare;
pub mod purge;
pub mod reconcile;
pub mod recovery;
pub mod run_crate;
pub mod session;
pub mod settle;
mod storage_audit;
pub mod supervise;
pub mod workspace;

use std::sync::Arc;
use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::effect::{BackendFacts, ComputeAnswer, ComputeEffect, ComputeRequest};
use aruna_core::compute::quota::JobReservationRecord;
use aruna_core::compute::{
    AttemptRef, AttemptStatus, BackendError, ExecutorKind, FenceContext, GrantedResources, TaskSpec,
};
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{
    AttemptIntent, ExecutionSpec, JobError, JobId, JobPayload, JobRecord, LaunchRecord,
    PhysicalExecutionState, recorded_env,
};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::node_subject::NodeSubjectRecord;
use tracing::warn;
use ulid::Ulid;

use attempt::{AttemptScope, RunWiring};
use compute::{compute_answer, compute_call};
use finalize::{FinalizeGoal, FinalizeOperation};
use prepare::{PrepareAttemptOperation, PreparedTask, build_task_spec, job_bucket};
use recovery::RecoverSubmitOperation;
use settle::{
    AnnounceTerminalOperation, EndRunOperation, RunEnding, SettleScope, SubmitFailureOperation,
    execution_result,
};
use supervise::SuperviseOperation;

use super::key_locks::ParkLockedOperation;
use super::lifecycle::LifecycleError;
use super::lifecycle::held::HeldReservations;
use super::lifecycle::updates::PublishProgressOperation;
use super::store::JobMutationError;
use super::store::attempt_intent::AttemptCommit;
use super::store::job_calls::{
    begin_external_running, cancel_running_job, record_attempt_intent, transition_to_preparing,
    transition_to_ready,
};
use super::store::row_calls::read_job_record;
use crate::placement::policy::{LocalSubjectOperation, SubjectScanError};

/// Fallback walltime when a spec declares none. Enforcement, reconcile
/// validation, and credential expiry must all agree on this value.
pub(crate) const DEFAULT_WALLTIME: Duration = Duration::from_secs(24 * 60 * 60);

/// The backend a run was placed on and the execution its receipt names.
#[derive(Debug, PartialEq)]
pub struct ResolvedBackend {
    pub facts: BackendFacts,
    pub receipted: Option<Ulid>,
}

#[derive(Debug, PartialEq)]
enum ResolveStep {
    Select,
    Held(Box<BackendFacts>),
    Subject(Box<BackendFacts>),
    Fenced,
}

type ResolveNext = Result<Next<ResolvedBackend>, JobError>;

/// Resolves the backend for a run, or an error when none is eligible. A receipted execution is
/// fenced to the receipt's execution site, so subject drift refuses the start; a local job
/// without a receipt keeps the unfenced selection.
#[derive(Debug, PartialEq)]
pub struct ResolveBackendOperation {
    constraint: Option<ExecutorKind>,
    job_id: JobId,
    receipted: Option<Ulid>,
    stored: (u64, [u8; 32]),
    current_generation: u64,
    step: ResolveStep,
}

impl ResolveBackendOperation {
    pub fn new(spec: &ExecutionSpec, job_id: JobId) -> Self {
        Self {
            constraint: (spec.executor_constraint.as_deref()).map(ExecutorKind::from_wire),
            job_id,
            receipted: None,
            stored: (0, [0; 32]),
            current_generation: 0,
            step: ResolveStep::Select,
        }
    }

    fn resolved(&self, facts: BackendFacts) -> ResolveNext {
        Ok(Next::Done(ResolvedBackend {
            facts,
            receipted: self.receipted,
        }))
    }
}

impl Operation for ResolveBackendOperation {
    type Output = ResolvedBackend;
    type Error = JobError;

    fn first(&mut self) -> ResolveNext {
        let constraint = self.constraint.clone();
        Ok(Next::Call(compute_call(ComputeEffect::Select {
            constraint,
        })))
    }

    fn next(&mut self, result: IoResult) -> ResolveNext {
        match std::mem::replace(&mut self.step, ResolveStep::Fenced) {
            // A node without compute is a local gap, never a verdict on the job.
            ResolveStep::Select => match compute_answer(result)? {
                ComputeAnswer::Selected(Some(facts)) => {
                    self.step = ResolveStep::Held(Box::new(facts));
                    let held = HeldReservations::of_job(self.job_id);
                    Ok(Next::Call(IoCall::child(held)))
                }
                ComputeAnswer::Selected(None) => {
                    Err(JobError::permanent("no eligible executor for job"))
                }
                _ => Err(JobError::retryable("no compute backend configured")),
            },
            ResolveStep::Held(facts) => {
                let held = result
                    .child::<Vec<JobReservationRecord>, LifecycleError>()?
                    .map_err(|error| JobError::retryable(error.to_string()))?;
                let Some(reservation) = held.into_iter().next() else {
                    return self.resolved(*facts);
                };
                self.receipted = Some(reservation.execution_id);
                if reservation.subject_generation == 0 {
                    return self.resolved(*facts);
                }
                self.stored = (reservation.subject_generation, reservation.subject_digest);
                self.step = ResolveStep::Subject(facts);
                Ok(Next::Call(IoCall::child(LocalSubjectOperation::new())))
            }
            ResolveStep::Subject(facts) => {
                let subject = result.child::<Option<NodeSubjectRecord>, SubjectScanError>()?;
                let Some(subject) = subject.ok().flatten().map(|record| record.subject) else {
                    return Err(JobError::retryable(
                        "receipted execution cannot start without a local placement subject",
                    ));
                };
                self.current_generation = subject.generation;
                let (generation, digest) = self.stored;
                Ok(Next::Call(compute_call(ComputeEffect::Fenced {
                    kind: facts.kind,
                    subject: Box::new(subject),
                    generation,
                    digest,
                })))
            }
            ResolveStep::Fenced => match compute_answer(result)? {
                ComputeAnswer::Fenced(Ok(facts)) => self.resolved(facts),
                ComputeAnswer::Fenced(Err(BackendError::Fenced)) => {
                    let (stored, current) = (self.stored.0, self.current_generation);
                    warn!(
                        job_id = %self.job_id,
                        stored_generation = stored,
                        current_generation = current,
                        "Receipted execution refused: the execution site drifted from its receipt"
                    );
                    Err(JobError::retryable(format!(
                        "execution site drifted from its receipt: stored subject generation {stored}, current {current}"
                    )))
                }
                ComputeAnswer::Fenced(Err(error)) => Err(JobError::permanent(format!(
                    "no eligible executor for job: {error}"
                ))),
                _ => Err(JobError::retryable("no compute backend configured")),
            },
        }
    }
}

#[derive(Debug, PartialEq)]
enum RunStep {
    FreshCancel,
    Resolve,
    Preparing,
    Published,
    Parked,
    Prepare,
    ParkedOnError(Box<JobError>),
    Ready(Box<PreparedTask>),
    Image(Box<PreparedTask>),
    Granted(Box<String>),
    LaunchSite(Box<(TaskSpec, String, GrantedResources)>),
    Intent(Box<(TaskSpec, String)>),
    IntentIntent,
    CancelWrite,
    Fence(Box<TaskSpec>),
    Submit,
    Running,
    Progress(bool),
    Recover,
    /// A child that ends the run.
    Last,
}

type RunNext = Result<Next<()>, IoError>;

/// Drives a claimed execution job through prepare, submit, supervision and finalization.
/// External attempts never share the generic in-process supervisor because a lost lease must
/// not requeue a container. The caller keeps the lease alive and owns the job's stop signal.
#[derive(Debug, PartialEq)]
pub struct ExecutionRunOperation {
    record: Arc<JobRecord>,
    spec: Option<Arc<ExecutionSpec>>,
    token: Option<Ulid>,
    bucket: String,
    wiring: RunWiring,
    facts: Option<BackendFacts>,
    receipted: Option<Ulid>,
    scope: Option<Arc<AttemptScope>>,
    now_ms: u64,
    step: RunStep,
}

impl ExecutionRunOperation {
    pub fn new(record: JobRecord, wiring: RunWiring) -> Self {
        let bucket = job_bucket(&record);
        let spec = match record.payload.clone() {
            JobPayload::Execution(mut spec) => {
                if !bucket.is_empty() {
                    spec.resolve_outputs(&bucket, record.owner_node_id);
                }
                Some(Arc::new(spec))
            }
            _ => None,
        };
        Self {
            token: record.claim.as_ref().map(|claim| claim.claim_token),
            record: Arc::new(record),
            spec,
            bucket,
            wiring,
            facts: None,
            receipted: None,
            scope: None,
            now_ms: 0,
            step: RunStep::Last,
        }
    }

    fn call(&mut self, step: RunStep, call: IoCall) -> RunNext {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn job_id(&self) -> JobId {
        self.record.job_id
    }

    fn claim(&self) -> Result<Ulid, IoError> {
        self.token.ok_or(IoError::Unfinished)
    }

    fn execution(&self) -> Result<Arc<ExecutionSpec>, IoError> {
        self.spec.clone().ok_or(IoError::Unfinished)
    }

    fn serving(&self) -> Result<(NodeId, RealmId), IoError> {
        self.wiring.serving.ok_or(IoError::Unfinished)
    }

    fn selected(&self) -> Result<&BackendFacts, IoError> {
        self.facts.as_ref().ok_or(IoError::Unfinished)
    }

    fn settle(&self) -> SettleScope {
        SettleScope {
            serving: self.wiring.serving,
            now_ms: self.now_ms,
        }
    }

    /// A failure before any attempt was submitted.
    fn pre_submit(&mut self, error: JobError, logged: bool) -> RunNext {
        let token = self.claim()?;
        let settle = self.settle();
        let failure = SubmitFailureOperation::new(&self.record, token, error, logged, settle);
        self.call(RunStep::Last, IoCall::child(failure))
    }

    /// No container exists yet, so a cancel settles the job right here.
    fn cancel_write(&mut self, step: RunStep) -> RunNext {
        let write = cancel_running_job(self.job_id(), self.claim()?, self.now_ms);
        self.call(step, IoCall::child(write))
    }

    fn cancelled(&mut self, result: IoResult, label: &str) -> RunNext {
        match result.child::<JobRecord, JobMutationError>()? {
            Ok(record) => {
                let announce = AnnounceTerminalOperation::new(record, self.settle());
                self.call(RunStep::Last, IoCall::child(announce))
            }
            Err(error) => {
                warn!(job_id = %self.job_id(), error = %error, "{label}");
                Ok(Next::Done(()))
            }
        }
    }

    fn scope(&self) -> Result<Arc<AttemptScope>, IoError> {
        self.scope.clone().ok_or(IoError::Unfinished)
    }

    fn finalize(&mut self, goal: FinalizeGoal) -> RunNext {
        let finalize = FinalizeOperation::new(self.scope()?, goal);
        self.call(RunStep::Last, IoCall::child(finalize))
    }

    fn supervise(&mut self) -> RunNext {
        let supervise = SuperviseOperation::new(self.scope()?);
        self.call(RunStep::Last, IoCall::child(supervise))
    }

    fn backend_ready(&mut self, facts: BackendFacts) -> RunNext {
        self.facts = Some(facts);
        let write = transition_to_preparing(self.job_id(), self.claim()?, self.now_ms);
        self.call(RunStep::Preparing, IoCall::child(write))
    }

    fn prepare(&mut self) -> RunNext {
        let (node_id, realm_id) = self.serving()?;
        let spec = self.execution()?;
        let run = credential::RunIdentity::new(&spec, &self.record, realm_id, node_id);
        let prepare = PrepareAttemptOperation::new(
            run,
            (spec, self.record.clone()),
            &self.bucket,
            self.wiring.encryption_key.clone(),
            true,
        );
        self.call(RunStep::Prepare, IoCall::child(prepare))
    }

    fn attempt(&self) -> AttemptRef {
        // Lowercased: attempt ids must be injective under the backend name mapping.
        AttemptRef::new(
            self.job_id().to_string().to_lowercase(),
            self.record.attempts,
        )
    }

    fn resolve_image(&mut self, prepared: PreparedTask) -> RunNext {
        let spec = self.execution()?;
        let kind = self.selected()?.kind.clone();
        let image = spec.image.clone();
        let call = compute_call(ComputeEffect::ResolveImage { kind, image });
        self.call(RunStep::Image(Box::new(prepared)), call)
    }

    fn image_failed(&mut self, error: BackendError) -> RunNext {
        let spec = self.execution()?;
        warn!(job_id = %self.job_id(), image = %spec.image, %error, "image resolution failed");
        let message = format!("image resolution failed: {error}");
        let job_error = match error.retryable() {
            true => JobError::retryable(message),
            false => JobError::permanent(message),
        };
        self.pre_submit(job_error, true)
    }

    fn granted_call(&mut self, prepared: PreparedTask, image: String) -> RunNext {
        let spec = self.execution()?;
        let facts = self.selected()?;
        let (kind, run_as) = (facts.kind.clone(), facts.run_as);
        let task = build_task_spec(&spec, &self.attempt(), &image, prepared, run_as);
        let call = compute_call(ComputeEffect::Granted {
            kind,
            spec: Box::new(task),
        });
        self.call(RunStep::Granted(Box::new(image)), call)
    }

    /// Where and with what this attempt is launched; planned until the backend accepts it.
    fn launch_site(&mut self, task: TaskSpec, image: String, granted: GrantedResources) -> RunNext {
        let facts = self.selected()?;
        if facts.local_site {
            let read = LocalSubjectOperation::new();
            let held = Box::new((task, image, granted));
            return self.call(RunStep::LaunchSite(held), IoCall::child(read));
        }
        let location = facts.worker_location.clone().unwrap_or_default();
        self.write_intent(task, image, granted, location)
    }

    /// Writes the attempt intent ahead of the submit, so a lost attempt is adoptable.
    fn write_intent(
        &mut self,
        task: TaskSpec,
        image: String,
        resources: GrantedResources,
        location: String,
    ) -> RunNext {
        let spec = self.execution()?;
        let (node_id, _) = self.serving()?;
        let facts = self.selected()?;
        let kind = facts.kind.as_wire();
        let launch = LaunchRecord {
            node_id,
            executor_kind: kind.clone(),
            location,
            resources,
            workdir: spec.workdir.clone(),
            env: recorded_env(&spec.env),
            started_at_ms: None,
        };
        let attempt = self.attempt();
        let intent = AttemptIntent {
            attempt_no: attempt.attempt,
            external_name: attempt.external_name(),
            executor_kind: kind,
            pinned_image: image.clone(),
            attempt_epoch: 0,
        };
        let write = record_attempt_intent(
            self.job_id(),
            self.claim()?,
            intent,
            self.receipted,
            Some(launch),
            self.now_ms,
        );
        self.call(
            RunStep::Intent(Box::new((task, image))),
            IoCall::child(write),
        )
    }

    fn intent_written(&mut self, commit: AttemptCommit, task: TaskSpec) -> RunNext {
        if commit.record.cancel_requested {
            return self.cancel_write(RunStep::CancelWrite);
        }
        let spec = self.execution()?;
        let facts = self.selected()?;
        let scope = AttemptScope {
            job_id: self.job_id(),
            token: self.claim()?,
            kind: facts.kind.clone(),
            fence: FenceContext {
                attempt: self.attempt(),
                attempt_epoch: commit.control.attempt_epoch,
                controller_generation: commit.control.controller_generation,
            },
            spec,
            bucket: self.bucket.clone(),
            run_as: facts.run_as,
            wiring: self.wiring.clone(),
        };
        let call = scope.backend(ComputeRequest::Fence);
        self.scope = Some(Arc::new(scope));
        self.call(RunStep::Fence(Box::new(task)), call)
    }

    fn submitted(&mut self, status: AttemptStatus) -> RunNext {
        // Running only after the backend accepted the fenced attempt.
        let write = begin_external_running(
            self.job_id(),
            self.claim()?,
            status.started_at_ms,
            self.now_ms,
        );
        self.call(RunStep::Running, IoCall::child(write))
    }

    fn publish(&mut self, state: PhysicalExecutionState, step: RunStep) -> RunNext {
        let publish =
            PublishProgressOperation::new(self.job_id(), state, self.wiring.serving, self.now_ms);
        self.call(step, IoCall::child(publish))
    }
}

impl Operation for ExecutionRunOperation {
    type Output = ();
    type Error = IoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> RunNext {
        if self.token.is_none() {
            warn!(job_id = %self.job_id(), "Execution job has no claim token; skipping");
            return Ok(Next::Done(()));
        }
        let spec = match &self.spec {
            Some(spec) => spec.clone(),
            None => return Ok(Next::Done(())),
        };
        // A fresh cancel before any attempt: no container exists, so the job ends directly.
        if self.record.cancel_requested && self.record.attempt_intent.is_none() {
            return self.cancel_write(RunStep::FreshCancel);
        }
        if self.wiring.serving.is_none() {
            let error = JobError::permanent("execution needs a net handle");
            let result = execution_result(&self.bucket, None, Vec::new(), Default::default());
            let ending = RunEnding::Fail { error, result };
            let settle = self.settle();
            let end = EndRunOperation::new(self.job_id(), self.claim()?, ending, settle);
            return self.call(RunStep::Last, IoCall::child(end));
        }
        let resolve = ResolveBackendOperation::new(&spec, self.job_id());
        self.call(RunStep::Resolve, IoCall::child(resolve))
    }

    fn next(&mut self, result: IoResult) -> RunNext {
        let job_id = self.job_id();
        match std::mem::replace(&mut self.step, RunStep::Last) {
            RunStep::FreshCancel => self.cancelled(result, "Fresh cancellation write failed"),
            // A fenced refusal is retryable: the site may come back or another target may
            // take the job.
            RunStep::Resolve => match result.child::<ResolvedBackend, JobError>()? {
                Ok(resolved) => {
                    self.receipted = resolved.receipted;
                    self.backend_ready(resolved.facts)
                }
                Err(error) => self.pre_submit(error, false),
            },
            RunStep::Preparing => {
                if result.child::<JobRecord, JobMutationError>()?.is_err() {
                    warn!(job_id = %job_id, "Lost claim before preparing; aborting");
                    return Ok(Next::Done(()));
                }
                self.publish(PhysicalExecutionState::Preparing, RunStep::Published)
            }
            // Staging reads input plaintext, so a locked input parks before any attempt intent.
            RunStep::Published => {
                if let Err(error) = result.child::<(), LifecycleError>()? {
                    warn!(error = %error, "Execution publication failed");
                }
                let (node_id, _) = self.serving()?;
                let node = (node_id, self.wiring.serving);
                let park = ParkLockedOperation::new(&self.record, self.claim()?, node, self.now_ms);
                self.call(RunStep::Parked, IoCall::child(park))
            }
            RunStep::Parked => match result.child::<bool, IoError>()? {
                Ok(true) => Ok(Next::Done(())),
                _ => self.prepare(),
            },
            RunStep::Prepare => match result.child::<PreparedTask, JobError>()? {
                Ok(prepared) => {
                    let write = transition_to_ready(job_id, self.claim()?, self.now_ms);
                    self.call(RunStep::Ready(Box::new(prepared)), IoCall::child(write))
                }
                Err(error) => {
                    let (node_id, _) = self.serving()?;
                    let node = (node_id, self.wiring.serving);
                    let park = ParkLockedOperation::on_error(
                        &self.record,
                        self.claim()?,
                        node,
                        &error,
                        self.now_ms,
                    );
                    let step = RunStep::ParkedOnError(Box::new(error));
                    self.call(step, IoCall::child(park))
                }
            },
            RunStep::ParkedOnError(error) => match result.child::<bool, IoError>()? {
                Ok(true) => Ok(Next::Done(())),
                _ => self.pre_submit(*error, false),
            },
            RunStep::Ready(prepared) => match result.child::<JobRecord, JobMutationError>()? {
                Ok(_) => self.resolve_image(*prepared),
                Err(_) => Ok(Next::Done(())),
            },
            RunStep::Image(prepared) => match compute_answer(result)? {
                ComputeAnswer::Image(Ok(image)) => self.granted_call(*prepared, image),
                ComputeAnswer::Image(Err(error)) => self.image_failed(error),
                _ => self.image_failed(BackendError::Unavailable("no backend".to_string())),
            },
            RunStep::Granted(image) => match compute_answer(result)? {
                ComputeAnswer::Granted { spec, resources } => match resources {
                    Ok(granted) => self.launch_site(*spec, *image, granted),
                    Err(error) => {
                        let message = format!("submit failed: {error}");
                        let job_error = match error.retryable() {
                            true => JobError::retryable(message),
                            false => JobError::permanent(message),
                        };
                        self.pre_submit(job_error, true)
                    }
                },
                _ => self.pre_submit(JobError::retryable("submit failed: no backend"), true),
            },
            RunStep::LaunchSite(held) => {
                let (task, image, granted) = *held;
                let subject = result.child::<Option<NodeSubjectRecord>, SubjectScanError>()?;
                let location = subject
                    .ok()
                    .flatten()
                    .map(|record| record.subject.location)
                    .unwrap_or_default();
                self.write_intent(task, image, granted, location)
            }
            RunStep::Intent(held) => {
                let (task, _) = *held;
                match result.child::<AttemptCommit, JobMutationError>()? {
                    Ok(commit) => self.intent_written(commit, task),
                    Err(JobMutationError::IntentConflict) => {
                        let read = read_job_record(job_id);
                        self.call(RunStep::IntentIntent, IoCall::child(read))
                    }
                    Err(_) => Ok(Next::Done(())),
                }
            }
            RunStep::IntentIntent => {
                let record = result.child::<Option<JobRecord>, JobMutationError>()?;
                match record
                    .ok()
                    .flatten()
                    .is_some_and(|record| record.cancel_requested)
                {
                    true => self.cancel_write(RunStep::CancelWrite),
                    false => Ok(Next::Done(())),
                }
            }
            RunStep::CancelWrite => self.cancelled(result, "Pre-submit cancellation write failed"),
            RunStep::Fence(task) => match compute_answer(result)? {
                ComputeAnswer::Done(Ok(())) => {
                    let call = self.scope()?.backend(ComputeRequest::Submit(task));
                    self.call(RunStep::Submit, call)
                }
                _ => Ok(Next::Done(())),
            },
            RunStep::Submit => match compute_answer(result)? {
                ComputeAnswer::Submitted(Ok(status)) => self.submitted(status),
                ComputeAnswer::Submitted(Err(BackendError::Cancelled)) => {
                    self.finalize(FinalizeGoal::Cancel)
                }
                // The submit outcome is unknown: reconciliation decides adoption.
                ComputeAnswer::Submitted(Err(error)) => {
                    let recover = RecoverSubmitOperation::new(self.scope()?, error);
                    self.call(RunStep::Recover, IoCall::child(recover))
                }
                _ => {
                    let error = BackendError::Unavailable("no backend".to_string());
                    let recover = RecoverSubmitOperation::new(self.scope()?, error);
                    self.call(RunStep::Recover, IoCall::child(recover))
                }
            },
            RunStep::Running => match result.child::<JobRecord, JobMutationError>()? {
                Ok(running) => {
                    let step = RunStep::Progress(running.cancel_requested);
                    self.publish(PhysicalExecutionState::Running, step)
                }
                Err(_) => Ok(Next::Done(())),
            },
            RunStep::Progress(cancel_requested) => {
                if let Err(error) = result.child::<(), LifecycleError>()? {
                    warn!(error = %error, "Execution publication failed");
                }
                match cancel_requested {
                    true => self.finalize(FinalizeGoal::Cancel),
                    false => self.supervise(),
                }
            }
            RunStep::Recover => match result.child::<bool, IoError>()? {
                Ok(true) => self.supervise(),
                _ => Ok(Next::Done(())),
            },
            RunStep::Last => {
                if let Err(error) = result.child::<(), IoError>()? {
                    warn!(job_id = %job_id, %error, "Execution run failed to settle");
                }
                Ok(Next::Done(()))
            }
        }
    }
}

#[cfg(test)]
mod fixtures;
#[cfg(test)]
mod run_tests;
#[cfg(test)]
mod tests;
