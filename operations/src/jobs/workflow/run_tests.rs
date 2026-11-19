//! Tests how an execution run is driven, by the calls and results of its operations.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::compute::{AttemptPhase, NOBODY};
use aruna_core::structs::execution::job::EffectiveResources;

use super::fixtures::{
    child, claimed, compute, control, facts, is_child, job, node, prepared, realm, record,
    scope_of, spec, status, token,
};
use super::*;

const NOW: u64 = 13;
const IMAGE: &str = "alpine@sha256:2";

fn lost() -> BackendError {
    BackendError::Unavailable("lost".to_string())
}

fn wiring() -> RunWiring {
    scope_of(spec()).wiring.clone()
}

fn resolve() -> ResolveBackendOperation {
    let mut resolve = ResolveBackendOperation::new(&spec(), job());
    let select = ComputeEffect::Select { constraint: None };
    assert_eq!(resolve.first(), Ok(Next::Call(compute_call(select))));
    resolve
}

fn selected() -> IoResult {
    compute(ComputeAnswer::Selected(Some(facts())))
}

fn reservation(subject_generation: u64) -> JobReservationRecord {
    JobReservationRecord {
        execution_id: control().execution_id,
        job_id: job(),
        logical_job_id: job(),
        resources: EffectiveResources {
            cpu_cores: 1,
            ram_bytes: 1,
            disk_bytes: 0,
            max_walltime_ms: 1,
            preemptible: false,
        },
        created_at_ms: 1,
        subject_generation,
        subject_digest: [4; 32],
    }
}

fn held(reservations: Vec<JobReservationRecord>) -> IoResult {
    child(Ok::<_, LifecycleError>(reservations))
}

/// A resolution that found the backend and asked for the job's reservations.
fn reading_held() -> ResolveBackendOperation {
    let mut resolve = resolve();
    let read = HeldReservations::of_job(job());
    assert_eq!(
        resolve.next(selected()),
        Ok(Next::Call(IoCall::child(read)))
    );
    resolve
}

// A node without compute may retry; a node with no eligible backend refuses the job.
#[test]
fn selection_failures_differ() {
    assert_eq!(
        resolve().next(compute(ComputeAnswer::NoCompute)),
        Err(JobError::retryable("no compute backend configured"))
    );
    assert_eq!(
        resolve().next(compute(ComputeAnswer::Selected(None))),
        Err(JobError::permanent("no eligible executor for job"))
    );
}

// A job without a receipt runs on the selection as it is.
#[test]
fn unreceipted_job_resolved() {
    let expected = ResolvedBackend {
        facts: facts(),
        receipted: None,
    };
    assert_eq!(
        reading_held().next(held(Vec::new())),
        Ok(Next::Done(expected))
    );
}

// A receipt without a subject names the execution and needs no fence.
#[test]
fn unfenced_receipt_named() {
    let expected = ResolvedBackend {
        facts: facts(),
        receipted: Some(control().execution_id),
    };
    assert_eq!(
        reading_held().next(held(vec![reservation(0)])),
        Ok(Next::Done(expected))
    );
}

// A receipted run without a local subject must not start anywhere else.
#[test]
fn receipt_needs_subject() {
    let mut resolve = reading_held();
    let read = LocalSubjectOperation::new();
    assert_eq!(
        resolve.next(held(vec![reservation(2)])),
        Ok(Next::Call(IoCall::child(read)))
    );
    let none = child(Ok::<Option<NodeSubjectRecord>, SubjectScanError>(None));
    assert_eq!(
        resolve.next(none),
        Err(JobError::retryable(
            "receipted execution cannot start without a local placement subject"
        ))
    );
}

fn fenced_at() -> ResolveBackendOperation {
    let mut resolve = ResolveBackendOperation::new(&spec(), job());
    resolve.receipted = Some(control().execution_id);
    resolve.stored = (2, [4; 32]);
    resolve.current_generation = 3;
    resolve.step = ResolveStep::Fenced;
    resolve
}

// The fence keeps the receipt, and a drifted site may come back later.
#[test]
fn fence_answers_judged() {
    let expected = ResolvedBackend {
        facts: facts(),
        receipted: Some(control().execution_id),
    };
    let fits = compute(ComputeAnswer::Fenced(Ok(facts())));
    assert_eq!(fenced_at().next(fits), Ok(Next::Done(expected)));
    let drifted = compute(ComputeAnswer::Fenced(Err(BackendError::Fenced)));
    assert_eq!(
        fenced_at().next(drifted),
        Err(JobError::retryable(
            "execution site drifted from its receipt: stored subject generation 2, current 3"
        ))
    );
    let refused = compute(ComputeAnswer::Fenced(Err(lost())));
    let message = format!("no eligible executor for job: {}", lost());
    assert_eq!(fenced_at().next(refused), Err(JobError::permanent(message)));
}

fn settle() -> SettleScope {
    SettleScope {
        serving: Some((node(), realm())),
        now_ms: NOW,
    }
}

fn run_of(job: JobRecord) -> ExecutionRunOperation {
    let mut run = ExecutionRunOperation::new(job, wiring());
    run.stamp(NOW);
    run
}

fn attempt() -> AttemptRef {
    AttemptRef::new(job().to_string().to_lowercase(), 0)
}

fn task() -> TaskSpec {
    build_task_spec(&spec(), &attempt(), IMAGE, prepared(), NOBODY)
}

fn stored() -> IoResult {
    child(Ok::<_, JobMutationError>(claimed()))
}

fn settled() -> IoResult {
    child(Ok::<(), IoError>(()))
}

fn published() -> IoResult {
    child(Ok::<(), LifecycleError>(()))
}

fn publish(state: PhysicalExecutionState) -> Next<()> {
    let publish = PublishProgressOperation::new(job(), state, settle().serving, NOW);
    Next::Call(IoCall::child(publish))
}

fn failure(error: JobError, logged: bool) -> Next<()> {
    let failure = SubmitFailureOperation::new(&claimed(), token(), error, logged, settle());
    Next::Call(IoCall::child(failure))
}

fn cancel_write() -> Next<()> {
    Next::Call(IoCall::child(cancel_running_job(job(), token(), NOW)))
}

fn announce() -> Next<()> {
    let announce = AnnounceTerminalOperation::new(claimed(), settle());
    Next::Call(IoCall::child(announce))
}

/// The attempt scope the run builds once its intent is written.
fn run_scope() -> Arc<AttemptScope> {
    let mut scope = AttemptScope::clone(&scope_of(spec()));
    scope.fence.attempt = attempt();
    scope.bucket = String::new();
    Arc::new(scope)
}

/// A run that resolved its backend and is about to be marked preparing.
fn resolved() -> ExecutionRunOperation {
    let mut run = run_of(claimed());
    let resolve = ResolveBackendOperation::new(&spec(), job());
    assert_eq!(run.first(), Ok(Next::Call(IoCall::child(resolve))));
    let found = ResolvedBackend {
        facts: facts(),
        receipted: None,
    };
    let write = transition_to_preparing(job(), token(), NOW);
    assert_eq!(
        run.next(child(Ok::<_, JobError>(found))),
        Ok(Next::Call(IoCall::child(write)))
    );
    run
}

/// A run waiting for its attempt preparation.
fn preparing() -> ExecutionRunOperation {
    let mut run = resolved();
    assert_eq!(
        run.next(stored()),
        Ok(publish(PhysicalExecutionState::Preparing))
    );
    assert!(is_child(&run.next(published())));
    assert!(is_child(&run.next(child(Ok::<_, IoError>(false)))));
    run
}

/// A run that asked for the pinned image of its attempt.
fn resolving_image() -> ExecutionRunOperation {
    let mut run = preparing();
    let write = transition_to_ready(job(), token(), NOW);
    assert_eq!(
        run.next(child(Ok::<_, JobError>(prepared()))),
        Ok(Next::Call(IoCall::child(write)))
    );
    let image = ComputeEffect::ResolveImage {
        kind: facts().kind,
        image: spec().image,
    };
    assert_eq!(run.next(stored()), Ok(Next::Call(compute_call(image))));
    run
}

/// A run that asked what its task is granted.
fn granting() -> ExecutionRunOperation {
    let mut run = resolving_image();
    let granted = ComputeEffect::Granted {
        kind: facts().kind,
        spec: Box::new(task()),
    };
    assert_eq!(
        run.next(compute(ComputeAnswer::Image(Ok(IMAGE.to_string())))),
        Ok(Next::Call(compute_call(granted)))
    );
    run
}

fn grant() -> IoResult {
    compute(ComputeAnswer::Granted {
        spec: Box::new(task()),
        resources: Ok(GrantedResources::default()),
    })
}

/// A run whose attempt intent is being written.
fn writing_intent() -> ExecutionRunOperation {
    let mut run = granting();
    assert!(is_child(&run.next(grant())));
    run
}

fn commit(job: JobRecord) -> IoResult {
    child(Ok::<_, JobMutationError>(AttemptCommit {
        record: job,
        control: control(),
    }))
}

/// A run that told the backend to submit its fenced attempt.
fn submitting() -> ExecutionRunOperation {
    let mut run = writing_intent();
    let fence = run_scope().backend(ComputeRequest::Fence);
    assert_eq!(run.next(commit(claimed())), Ok(Next::Call(fence)));
    let submit = ComputeRequest::Submit(Box::new(task()));
    assert_eq!(
        run.next(compute(ComputeAnswer::Done(Ok(())))),
        Ok(Next::Call(run_scope().backend(submit)))
    );
    run
}

// A job without a claim or without an execution spec is not run at all.
#[test]
fn unrunnable_job_skipped() {
    assert_eq!(run_of(record()).first(), Ok(Next::Done(())));
    let mut other = claimed();
    other.payload = JobPayload::WriteRunCrate { for_job: job() };
    assert_eq!(run_of(other).first(), Ok(Next::Done(())));
}

// A cancel before any attempt ends the job directly, since no container exists.
#[test]
fn fresh_cancel_ends() {
    let mut cancelled = claimed();
    cancelled.cancel_requested = true;
    let mut run = run_of(cancelled.clone());
    assert_eq!(run.first(), Ok(cancel_write()));
    assert_eq!(run.next(stored()), Ok(announce()));
    assert_eq!(run.next(settled()), Ok(Next::Done(())));
    let mut lost_write = run_of(cancelled);
    lost_write.first().unwrap();
    let failed = child(Err::<JobRecord, _>(JobMutationError::TokenMismatch));
    assert_eq!(lost_write.next(failed), Ok(Next::Done(())));
}

// A node without an identity cannot run anything, so the job fails for good.
#[test]
fn unserved_run_fails() {
    let mut unserved = wiring();
    unserved.serving = None;
    let mut run = ExecutionRunOperation::new(claimed(), unserved);
    run.stamp(NOW);
    let ending = RunEnding::Fail {
        error: JobError::permanent("execution needs a net handle"),
        result: execution_result("", None, Vec::new(), Default::default()),
    };
    let scope = SettleScope {
        serving: None,
        now_ms: NOW,
    };
    let end = EndRunOperation::new(job(), token(), ending, scope);
    assert_eq!(run.first(), Ok(Next::Call(IoCall::child(end))));
}

// A backend that cannot be resolved settles as a failure before submit.
#[test]
fn unresolved_backend_settles() {
    let mut run = run_of(claimed());
    run.first().unwrap();
    let error = JobError::retryable("no backend");
    let reply = child(Err::<ResolvedBackend, _>(error.clone()));
    assert_eq!(run.next(reply), Ok(failure(error, false)));
}

// A lost claim stops the run before it prepares anything.
#[test]
fn lost_claim_aborts() {
    let lost_claim = child(Err::<JobRecord, _>(JobMutationError::TokenMismatch));
    assert_eq!(resolved().next(lost_claim), Ok(Next::Done(())));
}

// A run parked on a locked key is finished for now.
#[test]
fn locked_input_parks() {
    let mut run = resolved();
    run.next(stored()).unwrap();
    run.next(published()).unwrap();
    assert_eq!(run.next(child(Ok::<_, IoError>(true))), Ok(Next::Done(())));
}

// A failed preparation parks on a locked key, and otherwise fails before submit.
#[test]
fn failed_prepare_settles() {
    let error = JobError::permanent("denied");
    let parked = |locked: bool| {
        let mut run = preparing();
        let reply = child(Err::<PreparedTask, _>(error.clone()));
        assert!(is_child(&run.next(reply)));
        run.next(child(Ok::<_, IoError>(locked)))
    };
    assert_eq!(parked(true), Ok(Next::Done(())));
    assert_eq!(parked(false), Ok(failure(error.clone(), false)));
}

// An image that cannot be resolved fails by the kind of its error.
#[test]
fn failed_image_settles() {
    let cases = [
        (
            ComputeAnswer::Image(Err(lost())),
            JobError::retryable(format!("image resolution failed: {}", lost())),
        ),
        (
            ComputeAnswer::Image(Err(BackendError::ImageNotFound("x".to_string()))),
            JobError::permanent(format!(
                "image resolution failed: {}",
                BackendError::ImageNotFound("x".to_string())
            )),
        ),
        (
            ComputeAnswer::NoBackend,
            JobError::retryable(format!(
                "image resolution failed: {}",
                BackendError::Unavailable("no backend".to_string())
            )),
        ),
    ];
    for (answer, error) in cases {
        let mut run = resolving_image();
        assert_eq!(run.next(compute(answer)), Ok(failure(error, true)));
    }
}

// A task the backend cannot grant fails before any intent is written.
#[test]
fn refused_grant_settles() {
    let refused = ComputeAnswer::Granted {
        spec: Box::new(task()),
        resources: Err(lost()),
    };
    let error = JobError::retryable(format!("submit failed: {}", lost()));
    assert_eq!(granting().next(compute(refused)), Ok(failure(error, true)));
    let error = JobError::retryable("submit failed: no backend");
    assert_eq!(
        granting().next(compute(ComputeAnswer::NoBackend)),
        Ok(failure(error, true))
    );
}

// A backend on this host launches at this node's own site.
#[test]
fn local_site_read() {
    let mut run = granting();
    run.facts = Some(BackendFacts {
        local_site: true,
        ..facts()
    });
    let read = LocalSubjectOperation::new();
    assert_eq!(run.next(grant()), Ok(Next::Call(IoCall::child(read))));
    let none = child(Ok::<Option<NodeSubjectRecord>, SubjectScanError>(None));
    assert!(is_child(&run.next(none)));
}

// A cancel found while the intent is written settles the job without a submit.
#[test]
fn intent_sees_cancel() {
    let mut cancelled = claimed();
    cancelled.cancel_requested = true;
    let mut run = writing_intent();
    assert_eq!(run.next(commit(cancelled)), Ok(cancel_write()));
    assert_eq!(run.next(stored()), Ok(announce()));
}

// An intent conflict only matters when a cancel explains it.
#[test]
fn intent_conflict_checked() {
    let conflict = || child(Err::<AttemptCommit, _>(JobMutationError::IntentConflict));
    let read = Next::Call(IoCall::child(read_job_record(job())));
    let mut plain = writing_intent();
    assert_eq!(plain.next(conflict()), Ok(read));
    let current = child(Ok::<_, JobMutationError>(Some(claimed())));
    assert_eq!(plain.next(current), Ok(Next::Done(())));
    let mut cancelled = writing_intent();
    cancelled.next(conflict()).unwrap();
    let mut job = claimed();
    job.cancel_requested = true;
    let current = child(Ok::<_, JobMutationError>(Some(job)));
    assert_eq!(cancelled.next(current), Ok(cancel_write()));
}

// A fence the backend refuses ends the run: another owner holds the attempt.
#[test]
fn refused_fence_ends() {
    let mut run = writing_intent();
    run.next(commit(claimed())).unwrap();
    let refused = compute(ComputeAnswer::Done(Err(BackendError::Fenced)));
    assert_eq!(run.next(refused), Ok(Next::Done(())));
}

// An accepted attempt is marked running, published and supervised.
#[test]
fn accepted_attempt_supervised() {
    let mut run = submitting();
    let accepted = ComputeAnswer::Submitted(Ok(status(AttemptPhase::Submitted)));
    let write = begin_external_running(job(), token(), None, NOW);
    assert_eq!(
        run.next(compute(accepted)),
        Ok(Next::Call(IoCall::child(write)))
    );
    assert_eq!(
        run.next(stored()),
        Ok(publish(PhysicalExecutionState::Running))
    );
    let supervise = SuperviseOperation::new(run_scope());
    assert_eq!(
        run.next(published()),
        Ok(Next::Call(IoCall::child(supervise)))
    );
    assert_eq!(run.next(settled()), Ok(Next::Done(())));
}

// A cancel that arrived while the attempt started is finalized instead of supervised.
#[test]
fn running_sees_cancel() {
    let mut run = submitting();
    let accepted = ComputeAnswer::Submitted(Ok(status(AttemptPhase::Submitted)));
    run.next(compute(accepted)).unwrap();
    let mut cancelled = claimed();
    cancelled.cancel_requested = true;
    run.next(child(Ok::<_, JobMutationError>(cancelled)))
        .unwrap();
    let finalize = FinalizeOperation::new(run_scope(), FinalizeGoal::Cancel);
    assert_eq!(
        run.next(published()),
        Ok(Next::Call(IoCall::child(finalize)))
    );
}

// A cancelled submit is finalized, and any other failure is recovered.
#[test]
fn failed_submit_recovers() {
    let cancelled = ComputeAnswer::Submitted(Err(BackendError::Cancelled));
    let finalize = FinalizeOperation::new(run_scope(), FinalizeGoal::Cancel);
    assert_eq!(
        submitting().next(compute(cancelled)),
        Ok(Next::Call(IoCall::child(finalize)))
    );
    let none = BackendError::Unavailable("no backend".to_string());
    let cases = [
        (ComputeAnswer::Submitted(Err(lost())), lost()),
        (ComputeAnswer::NoBackend, none),
    ];
    for (answer, error) in cases {
        let recover = RecoverSubmitOperation::new(run_scope(), error);
        assert_eq!(
            submitting().next(compute(answer)),
            Ok(Next::Call(IoCall::child(recover)))
        );
    }
}

// Only a recovery that found a live attempt goes on to supervise it.
#[test]
fn recovery_decides_supervision() {
    let recovering = || {
        let mut run = submitting();
        run.next(compute(ComputeAnswer::Submitted(Err(lost()))))
            .unwrap();
        run
    };
    let supervise = SuperviseOperation::new(run_scope());
    assert_eq!(
        recovering().next(child(Ok::<_, IoError>(true))),
        Ok(Next::Call(IoCall::child(supervise)))
    );
    assert_eq!(
        recovering().next(child(Ok::<_, IoError>(false))),
        Ok(Next::Done(()))
    );
}
