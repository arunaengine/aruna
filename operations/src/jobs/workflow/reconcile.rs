//! Adopts a lost external attempt by name and resolves it from backend evidence. It never
//! submits a new attempt name, so a container is never run twice.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::effect::{ComputeAnswer, ComputeEffect, ComputeRequest};
use aruna_core::compute::{
    AttemptPhase, AttemptRef, AttemptStatus, BackendError, ExecutorKind, FenceContext,
    ReconcileEvidence,
};
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{
    AttemptControl, AttemptIntent, ExecutionSpec, JobError, JobErrorKind, JobId, JobPayload,
    JobRecord, JobState,
};
use tracing::{info, warn};
use ulid::Ulid;

use super::DEFAULT_WALLTIME;
use super::attempt::{AttemptScope, RunWiring};
use super::compute::{compute_answer, compute_call};
use super::finalize::{FinalizeGoal, FinalizeOperation};
use super::prepare::{PrepareAttemptOperation, PreparedTask, build_task_spec, job_bucket};
use super::settle::{
    EndRunOperation, ParkAttemptOperation, RunEnding, SubmitFailureOperation, execution_result,
};
use super::supervise::SuperviseOperation;
use crate::jobs::store::JobMutationError;
use crate::jobs::store::job_calls::{
    AdoptOutcome, ReleaseOutcome, adopt_external_attempt, begin_external_running,
    handoff_external_attempt, record_attempt_started, record_attempt_tombstone, release_job,
};
use crate::jobs::store::row_calls::read_job_record;

/// What taking over a lost attempt left to do.
#[derive(Debug, PartialEq)]
pub enum Adoption {
    /// Nothing runs here: the attempt is held elsewhere, terminal, or was handed back.
    Done,
    Adopted {
        scope: Arc<AttemptScope>,
        record: Arc<JobRecord>,
    },
}

#[derive(Debug, PartialEq)]
enum AdoptStep {
    Adopt,
    Facts(Box<(JobRecord, AttemptControl, AttemptIntent, Ulid)>),
    Released,
    HandedBack,
}

type AdoptNext = Result<Next<Adoption>, IoError>;

/// Takes over a lost attempt with a fresh claim token, but only if the old holder is really
/// gone, and finds the executor its intent names.
#[derive(Debug, PartialEq)]
pub struct AdoptAttemptOperation {
    job_id: JobId,
    holder: NodeId,
    wiring: RunWiring,
    now_ms: u64,
    step: AdoptStep,
}

impl AdoptAttemptOperation {
    pub fn new(job_id: JobId, holder: NodeId, wiring: RunWiring) -> Self {
        Self {
            job_id,
            holder,
            wiring,
            now_ms: 0,
            step: AdoptStep::Adopt,
        }
    }

    fn call(&mut self, step: AdoptStep, call: IoCall) -> AdoptNext {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn adopted(&mut self, record: JobRecord, control: AttemptControl) -> AdoptNext {
        let Some(token) = record.claim.as_ref().map(|claim| claim.claim_token) else {
            return Ok(Next::Done(Adoption::Done));
        };
        // No durable attempt intent means the attempt was never submitted; the adopted lease
        // goes back without charging an attempt.
        let Some(intent) = record.attempt_intent.clone() else {
            let release = release_job(self.job_id, token, self.now_ms);
            return self.call(AdoptStep::Released, IoCall::child(release));
        };
        // The executor persisted in the intent: a fresh selection could ask the wrong backend
        // and park a live attempt as absent.
        let kind = ExecutorKind::from_wire(&intent.executor_kind);
        let call = compute_call(ComputeEffect::Facts { kind });
        let held = Box::new((record, control, intent, token));
        self.call(AdoptStep::Facts(held), call)
    }

    fn scope_of(
        &self,
        (record, control, intent, token): (JobRecord, AttemptControl, AttemptIntent, Ulid),
        run_as: aruna_core::compute::UserSpec,
    ) -> Next<Adoption> {
        let JobPayload::Execution(mut spec) = record.payload.clone() else {
            return Next::Done(Adoption::Done);
        };
        let bucket = job_bucket(&record);
        if !bucket.is_empty() {
            spec.resolve_outputs(&bucket, record.owner_node_id);
        }
        let attempt = AttemptRef::new(self.job_id.to_string().to_lowercase(), intent.attempt_no);
        let scope = AttemptScope {
            job_id: self.job_id,
            token,
            kind: ExecutorKind::from_wire(&intent.executor_kind),
            fence: FenceContext {
                attempt,
                attempt_epoch: intent.attempt_epoch,
                controller_generation: control.controller_generation,
            },
            spec: Arc::new(spec),
            bucket,
            run_as,
            wiring: self.wiring.clone(),
        };
        Next::Done(Adoption::Adopted {
            scope: Arc::new(scope),
            record: Arc::new(record),
        })
    }
}

impl Operation for AdoptAttemptOperation {
    type Output = Adoption;
    type Error = IoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> AdoptNext {
        let adopt = adopt_external_attempt(self.job_id, self.holder, self.now_ms);
        Ok(Next::Call(IoCall::child(adopt)))
    }

    fn next(&mut self, result: IoResult) -> AdoptNext {
        let job_id = self.job_id;
        match std::mem::replace(&mut self.step, AdoptStep::Released) {
            AdoptStep::Adopt => match result.child::<AdoptOutcome, JobMutationError>()? {
                Ok(AdoptOutcome::Adopted(record, control)) => self.adopted(record, control),
                Ok(AdoptOutcome::Skipped) => {
                    info!(job_id = %job_id, "Attempt is terminal or still held; not adopting");
                    Ok(Next::Done(Adoption::Done))
                }
                Err(error) => {
                    warn!(job_id = %job_id, error = %error, "Adoption failed");
                    Ok(Next::Done(Adoption::Done))
                }
            },
            AdoptStep::Facts(held) => match compute_answer(result)? {
                ComputeAnswer::Facts(facts) => Ok(self.scope_of(*held, facts.run_as)),
                // A node without this backend cannot observe the attempt, so charging would
                // terminalize a healthy container: it goes back with an expired lease.
                _ => {
                    let (_, _, intent, token) = *held;
                    warn!(job_id = %job_id, kind = %intent.executor_kind, "Reconcile backend unavailable; handing back");
                    let handoff = handoff_external_attempt(job_id, token, self.now_ms);
                    self.call(AdoptStep::HandedBack, IoCall::child(handoff))
                }
            },
            AdoptStep::Released => {
                let _ = result.child::<ReleaseOutcome, JobMutationError>()?;
                Ok(Next::Done(Adoption::Done))
            }
            AdoptStep::HandedBack => {
                if let Err(error) = result.child::<ReleaseOutcome, JobMutationError>()? {
                    warn!(job_id = %job_id, error = %error, "Failed to hand back an unreconciled attempt");
                }
                Ok(Next::Done(Adoption::Done))
            }
        }
    }
}

/// What the evidence about an adopted attempt asks for next.
#[derive(Debug, PartialEq)]
pub enum AdoptedEnd {
    Settled,
    /// The attempt lives on, or must be submitted again under its name: a supervisor takes it.
    Resume(AttemptPhase),
}

#[derive(Debug, PartialEq)]
enum SettleStep {
    Fence,
    Reconcile,
    Started(Box<ReconcileEvidence>),
    Running(Box<ReconcileEvidence>),
    StopThenPark,
    Tombstone,
    Last,
}

type SettleNext = Result<Next<AdoptedEnd>, IoError>;

/// Resolves an adopted attempt from what its backend says. Every unresolved exit parks: the
/// sweep that routed it here spends no attempt, so the park bounds what never resolves.
#[derive(Debug, PartialEq)]
pub struct SettleAdoptedOperation {
    scope: Arc<AttemptScope>,
    record: Arc<JobRecord>,
    now_ms: u64,
    step: SettleStep,
}

impl SettleAdoptedOperation {
    pub fn new(scope: Arc<AttemptScope>, record: Arc<JobRecord>) -> Self {
        Self {
            scope,
            record,
            now_ms: 0,
            step: SettleStep::Fence,
        }
    }

    fn call(&mut self, step: SettleStep, call: IoCall) -> SettleNext {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn park(&mut self, error: JobError) -> SettleNext {
        let scope = &self.scope;
        let settle = scope.settle(self.now_ms);
        let park = ParkAttemptOperation::new(scope.job_id, scope.token, error, settle);
        self.call(SettleStep::Last, IoCall::child(park))
    }

    fn finalize(&mut self, goal: FinalizeGoal) -> SettleNext {
        let finalize = FinalizeOperation::new(self.scope.clone(), goal);
        self.call(SettleStep::Last, IoCall::child(finalize))
    }

    fn start_known(&mut self, evidence: ReconcileEvidence) -> SettleNext {
        let scope = self.scope.clone();
        let started = match &evidence {
            ReconcileEvidence::Adoptable(evidence) => evidence.status.started_at_ms,
            _ => None,
        };
        if let Some(started_at_ms) = started {
            let write = record_attempt_started(scope.job_id, scope.token, started_at_ms);
            return self.call(
                SettleStep::Started(Box::new(evidence)),
                IoCall::child(write),
            );
        }
        self.mark_running(evidence)
    }

    fn mark_running(&mut self, evidence: ReconcileEvidence) -> SettleNext {
        let scope = self.scope.clone();
        let (adoptable, started) = match &evidence {
            ReconcileEvidence::Adoptable(evidence) => (
                !matches!(evidence.status.phase, AttemptPhase::Submitted),
                evidence.status.started_at_ms,
            ),
            _ => (false, None),
        };
        if adoptable && matches!(self.record.state, JobState::Indeterminate | JobState::Ready) {
            let write = begin_external_running(scope.job_id, scope.token, started, self.now_ms);
            return self.call(
                SettleStep::Running(Box::new(evidence)),
                IoCall::child(write),
            );
        }
        self.resolve(evidence)
    }

    fn resolve(&mut self, evidence: ReconcileEvidence) -> SettleNext {
        let scope = self.scope.clone();
        match evidence {
            ReconcileEvidence::Adoptable(evidence) if !evidence.status.is_terminal() => {
                Ok(Next::Done(AdoptedEnd::Resume(evidence.status.phase)))
            }
            // Terminal evidence is committed now, which correlates the cancel intent.
            ReconcileEvidence::Adoptable(evidence) => {
                let status = over_walltime(evidence.status, &scope.spec);
                self.finalize(FinalizeGoal::Attempt(Ok(status)))
            }
            // Absence after a submit retries the same attempt name.
            ReconcileEvidence::Absent => {
                Ok(Next::Done(AdoptedEnd::Resume(AttemptPhase::Submitted)))
            }
            ReconcileEvidence::Unavailable(error) => {
                self.park(JobError::retryable(format!("backend unavailable: {error}")))
            }
            ReconcileEvidence::Unadoptable(artifact) if artifact.exact_identity => {
                let call = scope.backend(ComputeRequest::Cancel);
                self.call(SettleStep::StopThenPark, call)
            }
            ReconcileEvidence::Unadoptable(_) => {
                self.park(JobError::retryable("backend artifact is not adoptable"))
            }
            ReconcileEvidence::Tombstoned(tombstone) => {
                let write = record_attempt_tombstone(
                    scope.job_id,
                    scope.token,
                    scope.fence.attempt_epoch,
                    tombstone.backend_ref,
                );
                self.call(SettleStep::Tombstone, IoCall::child(write))
            }
        }
    }
}

/// An attempt that ran past its walltime while nobody watched it counts as failed.
fn over_walltime(mut status: AttemptStatus, spec: &ExecutionSpec) -> AttemptStatus {
    let max_walltime_ms =
        (spec.resources.max_walltime_ms).unwrap_or(DEFAULT_WALLTIME.as_millis() as u64);
    let overran = (status.started_at_ms.zip(status.finished_at_ms))
        .is_some_and(|(started, finished)| finished.saturating_sub(started) > max_walltime_ms);
    if overran {
        status.phase = AttemptPhase::Failed {
            reason: "walltime limit exceeded while unobserved".to_string(),
        };
    }
    status
}

impl Operation for SettleAdoptedOperation {
    type Output = AdoptedEnd;
    type Error = IoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> SettleNext {
        Ok(Next::Call(self.scope.backend(ComputeRequest::Fence)))
    }

    fn next(&mut self, result: IoResult) -> SettleNext {
        let scope = self.scope.clone();
        match std::mem::replace(&mut self.step, SettleStep::Last) {
            SettleStep::Fence => match compute_answer(result)? {
                ComputeAnswer::Done(Ok(())) => {
                    if self.record.cancel_requested {
                        return self.finalize(FinalizeGoal::Cancel);
                    }
                    let call = scope.backend(ComputeRequest::Reconcile);
                    self.call(SettleStep::Reconcile, call)
                }
                answer => {
                    let error = match answer {
                        ComputeAnswer::Done(Err(error)) => error,
                        _ => BackendError::Unavailable("no backend".to_string()),
                    };
                    warn!(job_id = %scope.job_id, error = %error, "Backend fence failed");
                    let message = format!("reconcile fence failed: {error}");
                    self.park(JobError::retryable(message))
                }
            },
            SettleStep::Reconcile => match compute_answer(result)? {
                ComputeAnswer::Reconciled(evidence) => self.start_known(evidence),
                _ => self.park(JobError::retryable("backend unavailable: no backend")),
            },
            SettleStep::Started(evidence) => match result.child::<JobRecord, JobMutationError>()? {
                Ok(_) => self.mark_running(*evidence),
                Err(error) => {
                    warn!(job_id = %scope.job_id, error = %error, "Attempt start evidence write failed during adoption");
                    let message = format!("start evidence write failed: {error}");
                    self.park(JobError::retryable(message))
                }
            },
            SettleStep::Running(evidence) => match result.child::<JobRecord, JobMutationError>()? {
                Ok(running) if running.cancel_requested => self.finalize(FinalizeGoal::Cancel),
                Ok(_) => self.resolve(*evidence),
                Err(error) => {
                    let message = format!("adopted attempt cannot run: {error}");
                    self.park(JobError::retryable(message))
                }
            },
            SettleStep::StopThenPark => {
                let _ = compute_answer(result)?;
                self.park(JobError::retryable("backend artifact is not adoptable"))
            }
            SettleStep::Tombstone => {
                if result.child::<AttemptControl, JobMutationError>()?.is_err() {
                    return Ok(Next::Done(AdoptedEnd::Settled));
                }
                let error = JobError::retryable(format!(
                    "submit failed: {}",
                    BackendError::Unavailable("attempt tombstoned".to_string())
                ));
                let failure = SubmitFailureOperation::new(
                    &self.record,
                    scope.token,
                    error,
                    false,
                    scope.settle(self.now_ms),
                );
                self.call(SettleStep::Last, IoCall::child(failure))
            }
            SettleStep::Last => {
                if let Err(error) = result.child::<(), IoError>()? {
                    warn!(job_id = %scope.job_id, %error, "Adopted attempt failed to settle");
                }
                Ok(Next::Done(AdoptedEnd::Settled))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum ResumeStep {
    Current,
    Prepare,
    Submit,
    Started(Box<AttemptStatus>),
    Running,
    Supervise,
    Last,
}

type ResumeNext = Result<Next<()>, IoError>;

/// Supervises an adopted attempt, submitting it again under its name when it never ran.
#[derive(Debug, PartialEq)]
pub struct ResumeAttemptOperation {
    scope: Arc<AttemptScope>,
    record: Arc<JobRecord>,
    phase: AttemptPhase,
    now_ms: u64,
    step: ResumeStep,
}

impl ResumeAttemptOperation {
    pub fn new(scope: Arc<AttemptScope>, record: Arc<JobRecord>, phase: AttemptPhase) -> Self {
        Self {
            scope,
            record,
            phase,
            now_ms: 0,
            step: ResumeStep::Current,
        }
    }

    fn call(&mut self, step: ResumeStep, call: IoCall) -> ResumeNext {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn supervise(&mut self) -> ResumeNext {
        let supervise = SuperviseOperation::new(self.scope.clone());
        self.call(ResumeStep::Supervise, IoCall::child(supervise))
    }

    fn cancel(&mut self) -> ResumeNext {
        let finalize = FinalizeOperation::new(self.scope.clone(), FinalizeGoal::Cancel);
        self.call(ResumeStep::Last, IoCall::child(finalize))
    }

    fn park(&mut self, error: JobError) -> ResumeNext {
        let scope = &self.scope;
        let settle = scope.settle(self.now_ms);
        let park = ParkAttemptOperation::new(scope.job_id, scope.token, error, settle);
        self.call(ResumeStep::Last, IoCall::child(park))
    }

    fn fail_or_park(&mut self, error: JobError) -> ResumeNext {
        if error.kind != JobErrorKind::Permanent {
            return self.park(error);
        }
        let scope = &self.scope;
        let bucket = job_bucket(&self.record);
        let result = execution_result(&bucket, None, Vec::new(), Default::default());
        let ending = RunEnding::Fail { error, result };
        let settle = scope.settle(self.now_ms);
        let end = EndRunOperation::new(scope.job_id, scope.token, ending, settle);
        self.call(ResumeStep::Last, IoCall::child(end))
    }

    fn prepare(&mut self) -> ResumeNext {
        let scope = self.scope.clone();
        let Some(run) = scope.run(&self.record) else {
            return self.fail_or_park(JobError::permanent("execution needs a net handle"));
        };
        let prepare = PrepareAttemptOperation::new(
            run,
            (scope.spec.clone(), self.record.clone()),
            &scope.bucket,
            scope.wiring.encryption_key.clone(),
            false,
        );
        self.call(ResumeStep::Prepare, IoCall::child(prepare))
    }

    fn submit(&mut self, prepared: PreparedTask) -> ResumeNext {
        let scope = self.scope.clone();
        let pinned = (self.record.attempt_intent.as_ref())
            .filter(|intent| intent.attempt_epoch == scope.fence.attempt_epoch)
            .map(|intent| intent.pinned_image.clone());
        let Some(pinned) = pinned else {
            return self.park(JobError::retryable("attempt intent mismatch"));
        };
        let task = build_task_spec(
            &scope.spec,
            &scope.fence.attempt,
            &pinned,
            prepared,
            scope.run_as,
        );
        let call = scope.backend(ComputeRequest::Submit(Box::new(task)));
        self.call(ResumeStep::Submit, call)
    }

    fn submitted(&mut self, status: AttemptStatus) -> ResumeNext {
        let scope = self.scope.clone();
        if let Some(started_at_ms) = status.started_at_ms {
            let write = record_attempt_started(scope.job_id, scope.token, started_at_ms);
            let step = ResumeStep::Started(Box::new(status));
            return self.call(step, IoCall::child(write));
        }
        self.running(&status)
    }

    fn running(&mut self, status: &AttemptStatus) -> ResumeNext {
        let scope = &self.scope;
        let write =
            begin_external_running(scope.job_id, scope.token, status.started_at_ms, self.now_ms);
        self.call(ResumeStep::Running, IoCall::child(write))
    }
}

impl Operation for ResumeAttemptOperation {
    type Output = ();
    type Error = IoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> ResumeNext {
        let read = read_job_record(self.scope.job_id);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> ResumeNext {
        let job_id = self.scope.job_id;
        match std::mem::replace(&mut self.step, ResumeStep::Last) {
            ResumeStep::Current => match result.child::<Option<JobRecord>, JobMutationError>()? {
                Ok(Some(current)) if current.cancel_requested => self.cancel(),
                Ok(Some(_)) if matches!(self.phase, AttemptPhase::Submitted) => self.prepare(),
                Ok(Some(_)) => self.supervise(),
                Ok(None) => Ok(Next::Done(())),
                Err(error) => {
                    warn!(job_id = %job_id, error = %error, "Recovered job lookup failed");
                    Ok(Next::Done(()))
                }
            },
            ResumeStep::Prepare => match result.child::<PreparedTask, JobError>()? {
                Ok(prepared) => self.submit(prepared),
                Err(error) => self.fail_or_park(error),
            },
            ResumeStep::Submit => match compute_answer(result)? {
                ComputeAnswer::Submitted(Ok(status)) => self.submitted(status),
                ComputeAnswer::Submitted(Err(BackendError::Cancelled)) => self.cancel(),
                ComputeAnswer::Submitted(Err(error)) => {
                    let message = format!("resubmit failed: {error}");
                    self.fail_or_park(match error.retryable() {
                        true => JobError::retryable(message),
                        false => JobError::permanent(message),
                    })
                }
                _ => self.park(JobError::retryable("resubmit failed: no backend")),
            },
            ResumeStep::Started(status) => match result.child::<JobRecord, JobMutationError>()? {
                Ok(_) => self.running(&status),
                Err(_) => Ok(Next::Done(())),
            },
            ResumeStep::Running => match result.child::<JobRecord, JobMutationError>()? {
                Ok(running) if running.cancel_requested => self.cancel(),
                Ok(_) => self.supervise(),
                Err(_) => Ok(Next::Done(())),
            },
            ResumeStep::Supervise | ResumeStep::Last => {
                if let Err(error) = result.child::<(), IoError>()? {
                    warn!(job_id = %job_id, %error, "Adopted attempt failed to settle");
                }
                Ok(Next::Done(()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::JOB_MAX_ATTEMPTS;
    use crate::jobs::store::query::insert_job;
    use crate::jobs::store::state::record_attempt_intent;
    use crate::tests::workflow::{execution_spec, node_id};
    use aruna_compute::ExecutorRegistry;
    use aruna_core::UserId;
    use aruna_core::structs::execution::job::{AttemptIntent, JobClaim, JobId};
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::placement::record::FIRST_GRANTABLE_HANDLE;
    use aruna_core::structured_id::{BucketId, PlacementHandle};
    use aruna_storage::FjallStorage;
    use aruna_tasks::TaskHandle;
    use tempfile::tempdir;
    use ulid::Ulid;

    fn job_id() -> JobId {
        crate::jobs::submit::mint_job_id(
            PlacementHandle::new(FIRST_GRANTABLE_HANDLE).unwrap(),
            BucketId::new(0).unwrap(),
        )
        .unwrap()
    }

    // A node whose registry lacks the executor cannot observe the container, so no
    // number of sweeps by it may spend an attempt or terminalize the job.
    #[tokio::test]
    async fn missing_backend_spares() {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = Arc::new(DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: Some(TaskHandle::new()),
            compute_handle: Some(Arc::new(ExecutorRegistry::new())),
            outbound_handle: None,
        });
        let job_id = job_id();
        let token = Ulid::generate();
        let mut record = JobRecord::new(
            job_id,
            JobPayload::Execution(execution_spec()),
            UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32])),
            node_id(7),
            1,
            1,
            None,
        );
        record.state = JobState::Running;
        record.claim = Some(JobClaim {
            holder_node_id: node_id(7),
            claim_token: token,
            lease_expires_ms: 1,
        });
        insert_job(&context, &record).await.unwrap();
        record_attempt_intent(
            &context,
            job_id,
            token,
            AttemptIntent {
                attempt_no: 1,
                external_name: job_id.to_string().to_lowercase(),
                executor_kind: "kubernetes".to_string(),
                pinned_image: "alpine@sha256:digest".to_string(),
                attempt_epoch: 0,
            },
            None,
            None,
            unix_timestamp_millis(),
        )
        .await
        .unwrap();

        let runtime = JobsRuntime::new();
        let reconciler = ComputeReconciler::new(context.clone(), Arc::downgrade(&runtime));
        for sweep in 0..(JOB_MAX_ATTEMPTS + 2) {
            let lost = read_job_record(&context, job_id, None)
                .await
                .unwrap()
                .unwrap();
            reconciler.reconcile_lost_attempt(&context, lost).await;
            let after = read_job_record(&context, job_id, None)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(after.state, JobState::Running, "sweep {sweep}");
            assert_eq!(after.attempts, 0, "sweep {sweep}");
            assert!(after.attempt_intent.is_some(), "sweep {sweep}");
        }
    }
}
