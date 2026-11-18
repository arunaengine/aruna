//! Tests the session plan and report by their calls and results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::calls::ChildOutcome;
use aruna_core::structs::execution::job::JobPayload;

use super::*;

fn job() -> JobId {
    JobId::from_bytes([1; 16])
}

fn node() -> NodeId {
    iroh::SecretKey::from_bytes(&[4; 32]).public()
}

fn spec() -> ExecutionSpec {
    ExecutionSpec {
        group_id: Ulid::from_bytes([2; 16]),
        name: None,
        description: None,
        tags: Default::default(),
        image: "alpine".to_string(),
        entrypoint: None,
        command: Vec::new(),
        workdir: None,
        env: Default::default(),
        resources: Default::default(),
        executor_constraint: None,
        inputs: Vec::new(),
        file_outputs: Vec::new(),
        workspace_outputs: Vec::new(),
        output_prefixes: Vec::new(),
        collision_policy: Default::default(),
    }
}

fn record() -> JobRecord {
    let user = UserId::local(Ulid::from_bytes([5; 16]), RealmId([1; 32]));
    let payload = JobPayload::Execution(spec());
    JobRecord::new(job(), payload, user, node(), 1, 1, None)
}

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

fn is_child<T>(next: &Result<Next<T>, IoError>) -> bool {
    matches!(next, Ok(Next::Call(IoCall::Child(_))))
}

fn plan(asked_idle_ms: Option<u64>) -> SessionPlanOperation {
    let inputs = SessionInputs {
        realm_id: RealmId([1; 32]),
        node_id: node(),
        mounts: false,
        encryption_key: None,
        now: SystemTime::UNIX_EPOCH,
    };
    let mut plan = SessionPlanOperation::new(job(), &spec(), "work", asked_idle_ms, inputs);
    assert!(is_child(&plan.first()));
    plan
}

fn no_reservations() -> IoResult {
    child(Ok::<_, LifecycleError>(Vec::<JobReservationRecord>::new()))
}

fn realm(session_idle_ms: u64) -> IoResult {
    let mut config = RealmConfigDocument::default_for_realm(RealmId([1; 32]), Vec::new());
    config.compute.session_idle_ms = session_idle_ms;
    child(Ok::<_, GetConfigError>(config))
}

/// A plan that found no job record and waits for the realm config.
fn at_realm(asked_idle_ms: Option<u64>) -> SessionPlanOperation {
    let mut plan = plan(asked_idle_ms);
    assert!(is_child(&plan.next(no_reservations())));
    let missing = child(Ok::<_, JobMutationError>(None::<JobRecord>));
    assert!(is_child(&plan.next(missing)));
    plan
}

fn planned(idle_after_ms: u64, credential_expires_ms: u64) -> Next<Option<SessionPlan>> {
    Next::Done(Some(SessionPlan {
        public_job_id: job(),
        credential_expires_ms,
        idle_after_ms,
    }))
}

// Without readable reservations no session opens.
#[test]
fn unreadable_reservations_stop() {
    let mut plan = plan(None);
    let failed = child(Err::<Vec<JobReservationRecord>, _>(LifecycleError::from(
        IoError::Unfinished,
    )));
    assert_eq!(plan.next(failed), Ok(Next::Done(None)));
}

// The request may shorten the realm's idle wait and never lengthen it.
#[test]
fn request_shortens_idle() {
    assert_eq!(at_realm(Some(10)).next(realm(50)), Ok(planned(10, 0)));
    assert_eq!(at_realm(Some(90)).next(realm(50)), Ok(planned(50, 0)));
    assert_eq!(at_realm(None).next(realm(50)), Ok(planned(50, 0)));
}

// A realm without a usable value falls back to the default wait.
#[test]
fn realm_falls_back() {
    assert_eq!(at_realm(None).next(realm(0)), Ok(planned(IDLE_AFTER_MS, 0)));
    let failed = child(Err::<RealmConfigDocument, _>(GetConfigError::from(
        IoError::Unfinished,
    )));
    assert_eq!(at_realm(None).next(failed), Ok(planned(IDLE_AFTER_MS, 0)));
}

// The client sees when the run's credential really expires; a failed mint shows none.
#[test]
fn credential_expiry_reported() {
    let minted = WorkspaceCredential {
        access_key: "key".to_string(),
        secret: "secret".to_string(),
        expires_at_ms: 77,
    };
    let cases = [
        (child(Ok::<_, JobError>(minted)), 77),
        (
            child(Err::<WorkspaceCredential, _>(JobError::retryable("no"))),
            0,
        ),
    ];
    for (reply, expires) in cases {
        let mut plan = plan(None);
        plan.next(no_reservations()).unwrap();
        let found = child(Ok::<_, JobMutationError>(Some(record())));
        assert!(is_child(&plan.next(found)));
        assert!(is_child(&plan.next(reply)));
        assert_eq!(plan.next(realm(50)), Ok(planned(50, expires)));
    }
}

fn staged(dest_key: &str) -> StagedInput {
    StagedInput {
        dest_key: dest_key.to_string(),
        bytes: 1,
        blake3: "hash".to_string(),
        source_node_id: "node".to_string(),
        version_id: "version".to_string(),
    }
}

fn pending(job_id: &str) -> PendingInput {
    PendingInput {
        dest_key: "copied".to_string(),
        job_id: job_id.to_string(),
        source_node_id: "node".to_string(),
    }
}

fn report(ledger: SessionLedger) -> SessionReportOperation {
    let token = Ulid::from_bytes([2; 16]);
    let mut report = SessionReportOperation::new(job(), token, false, EndReason::Ended);
    report.take_ledger(ledger);
    report
}

fn written() -> IoResult {
    child(Ok::<_, JobMutationError>(()))
}

fn keys(report: &SessionReportOperation) -> Vec<String> {
    (report.rows.iter())
        .map(|row| row.entry_key.clone())
        .collect()
}

// Without a ledger the report still says why the session ended.
#[test]
fn empty_report_ends() {
    let mut report = report(SessionLedger::default());
    assert!(is_child(&report.first()));
    assert!(report.rows.is_empty());
    assert_eq!(report.next(written()), Ok(Next::Done(())));
}

// Inputs, then touched objects, then the end, one write each.
#[test]
fn report_rows_ordered() {
    let touched = TouchedObject {
        bucket: "work".to_string(),
        key: "a".to_string(),
        operation: "read".to_string(),
    };
    let mut report = report(SessionLedger {
        inputs: vec![staged("a"), staged("b")],
        pending: Vec::new(),
        touched: vec![touched],
    });
    assert!(is_child(&report.first()));
    assert_eq!(keys(&report), ["input/0001", "touched/0000", "end"]);
    for _ in 0..3 {
        assert!(is_child(&report.next(written())));
    }
    assert_eq!(report.next(written()), Ok(Next::Done(())));
}

// A queued copy that finished counts as an input; one still running is left out.
#[test]
fn finished_copy_counts() {
    let mut done = record();
    done.result = Some(JobResultPayload::CopyObject {
        version_id: "v".to_string(),
        bytes: 3,
        blake3: "h".to_string(),
    });
    let copy = job().to_string();
    let mut report = report(SessionLedger {
        inputs: vec![staged("a")],
        pending: vec![pending(&copy), pending(&copy)],
        touched: Vec::new(),
    });
    assert!(is_child(&report.first()));
    let finished = child(Ok::<_, JobMutationError>(Some(done)));
    assert!(is_child(&report.next(finished)));
    let running = child(Ok::<_, JobMutationError>(Some(record())));
    assert!(is_child(&report.next(running)));
    assert_eq!(keys(&report), ["input/0001", "end"]);
}

// A copy whose id does not parse is skipped without a read.
#[test]
fn bad_copy_skipped() {
    let mut report = report(SessionLedger {
        inputs: Vec::new(),
        pending: vec![pending("not-a-job")],
        touched: Vec::new(),
    });
    assert!(is_child(&report.first()));
    assert_eq!(report.step, ReportStep::Write);
}

// A failed write ends the report.
#[test]
fn failed_write_stops() {
    let mut report = report(SessionLedger {
        inputs: vec![staged("a")],
        ..Default::default()
    });
    report.first().unwrap();
    let failed = child(Err::<(), _>(JobMutationError::NotFound));
    assert_eq!(report.next(failed), Ok(Next::Done(())));
}

// A live session is asked for its ledger first; one that is gone reports only the end.
#[test]
fn reads_live_ledger() {
    let token = Ulid::from_bytes([2; 16]);
    let mut report = SessionReportOperation::new(job(), token, true, EndReason::Idle);
    assert!(is_child(&report.first()));
    assert_eq!(report.step, ReportStep::Ledger);
    let missing = child(Ok::<_, IoError>(SessionAnswer::Missing));
    assert!(is_child(&report.next(missing)));
    assert_eq!(keys(&report), Vec::<String>::new());
}
