//! Call and result tests of the job report reads.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::execution::job::{
    ImportMetadataTarget, ImportRoCrateSource, ImportRoCrateSpec, ImportRoCrateTarget,
    RoCrateLimits,
};
use ulid::Ulid;

use super::*;
use crate::jobs::job_reads::tests::{
    auth_of, child, is_child, job_id, node, owner, probe, realm, record_of, stranger,
};

const DIGEST: [u8; 32] = [7u8; 32];

fn query() -> ReportQuery {
    ReportQuery {
        expected_digest: None,
        last_key: None,
        limit: 10,
    }
}

fn import_record() -> JobRecord {
    let mut record = record_of(JobPayload::ImportRoCrate(ImportRoCrateSpec {
        auth_context: auth_of(owner()),
        source: ImportRoCrateSource::Upload {
            upload_id: Ulid::from_bytes([4u8; 16]),
        },
        target: ImportRoCrateTarget {
            bucket: "target".to_string(),
            prefix: String::new(),
        },
        metadata: ImportMetadataTarget {
            group_id: Ulid::from_bytes([5u8; 16]),
            path: "crate".to_string(),
            public: false,
        },
        limits: RoCrateLimits::default(),
        document_id: Ulid::from_bytes([6u8; 16]),
    }));
    record.state = JobState::Succeeded;
    record.finished_at_ms = Some(1);
    record.retention_ms = 100;
    record.report_digest = Some(DIGEST);
    record
}

fn found(record: Option<JobRecord>) -> IoResult {
    child(Ok::<_, JobReadError>(record))
}

fn read(query: ReportQuery, record: JobRecord, now_ms: u64) -> ReportNext {
    let mut report = JobReportOperation::owned(owner(), job_id(), query, now_ms);
    report.next(found(Some(record)))
}

fn view() -> JobReportView {
    JobReportView {
        job_id: job_id(),
        created_by: owner(),
        kind: JobKind::ImportRoCrate,
        report_digest: DIGEST,
    }
}

fn route() -> ReportRouteOperation {
    let serving = Some((node(1), realm()));
    ReportRouteOperation::new(&auth_of(owner()), job_id(), query(), None, serving, 5)
}

fn remote(response: JobResponse) -> IoResult {
    child(Ok::<_, JobRouteError>(JobRouteOutcome::Remote(response)))
}

// A finished job reads one page and answers it with its report view.
#[test]
fn ready_page_read() {
    let mut report = JobReportOperation::owned(owner(), job_id(), query(), 5);
    assert!(is_child(&report.first().unwrap()));
    assert!(is_child(
        &report.next(found(Some(import_record()))).unwrap()
    ));
    let page: EntryPage = (vec![(vec![1u8], Value::from(vec![2u8]))], None);
    let done = report.next(child(Ok::<_, JobMutationError>(page))).unwrap();
    assert!(matches!(
        done,
        Next::Done(JobReportLookup::Ready { job, rows, next_key: None })
            if job == view() && rows.len() == 1
    ));
}

// A missing job and a job without a report are both absent.
#[test]
fn absent_reports_hidden() {
    let mut report = JobReportOperation::owned(owner(), job_id(), query(), 5);
    assert!(matches!(
        report.next(found(None)),
        Ok(Next::Done(JobReportLookup::NotFound))
    ));
    assert!(matches!(
        read(query(), record_of(probe()), 5),
        Ok(Next::Done(JobReportLookup::NotFound))
    ));
}

// A report past its retention is gone, whatever its rows still hold.
#[test]
fn expired_report_hidden() {
    assert!(matches!(
        read(query(), import_record(), 101),
        Ok(Next::Done(JobReportLookup::NotFound))
    ));
}

// A running job is pending, and a stale digest or oversized cursor conflicts.
#[test]
fn page_guards_apply() {
    let mut running = import_record();
    running.state = JobState::Running;
    assert!(matches!(
        read(query(), running, 5),
        Ok(Next::Done(JobReportLookup::Pending(JobState::Running)))
    ));
    let stale = ReportQuery {
        expected_digest: Some([9u8; 32]),
        ..query()
    };
    assert!(matches!(
        read(stale, import_record(), 5),
        Ok(Next::Done(JobReportLookup::CursorConflict))
    ));
    let mut limited = import_record();
    if let JobPayload::ImportRoCrate(spec) = &mut limited.payload {
        spec.limits.key_bytes = 1;
    }
    let long = ReportQuery {
        last_key: Some(vec![0u8; 2]),
        ..query()
    };
    assert!(matches!(
        read(long, limited, 5),
        Ok(Next::Done(JobReportLookup::CursorConflict))
    ));
}

// A terminal job without its digest is a broken record.
#[test]
fn missing_digest_fails() {
    let mut record = import_record();
    record.report_digest = None;
    assert!(read(query(), record, 5).is_err());
}

// The last end row names the reason, and a running session has none yet.
#[test]
fn session_reason_read() {
    let mut reason = SessionReasonOperation::new(owner(), job_id());
    assert!(is_child(&reason.first().unwrap()));
    let mut running = record_of(probe());
    running.state = JobState::Running;
    assert!(matches!(
        reason.next(found(Some(running))),
        Ok(Next::Done(None))
    ));

    let mut finished = record_of(probe());
    finished.state = JobState::Succeeded;
    let mut reason = SessionReasonOperation::new(owner(), job_id());
    assert!(is_child(&reason.next(found(Some(finished))).unwrap()));
    let page: EntryPage = (Vec::new(), None);
    let none = reason.next(child(Ok::<_, JobMutationError>(page))).unwrap();
    assert!(matches!(none, Next::Done(None)));
}

// Only the expected job and digest are accepted from a remote owner.
#[test]
fn remote_report_checked() {
    let job = view();
    assert!(report_job_matches(&job, owner(), job_id(), Some(DIGEST)));
    assert!(!report_job_matches(
        &job,
        owner(),
        job_id(),
        Some([9u8; 32])
    ));
    assert!(!report_job_matches(&job, stranger(), job_id(), None));

    let mut routed = route();
    routed.step = RouteStep::Route;
    let answer = remote(JobResponse::ReportReady {
        job,
        rows: vec![(vec![1u8], vec![2u8])],
        next_key: None,
    });
    assert!(matches!(
        routed.next(answer),
        Ok(Next::Done(JobReportLookup::Ready { .. }))
    ));
    let mut routed = route();
    routed.step = RouteStep::Route;
    let refused = remote(JobResponse::Forbidden);
    assert_eq!(routed.next(refused).unwrap_err(), JobRouteError::Forbidden);
}

// A plain job routes at once, and a session alias reads its physical job.
#[test]
fn alias_steps() {
    let mut routed = route();
    assert!(is_child(&routed.first().unwrap()));
    let plain = child(Ok::<Option<JobFamilyId>, JobRouteError>(None));
    assert!(is_child(&routed.next(plain).unwrap()));
    assert_eq!(routed.step, RouteStep::Route);

    let physical = JobId::from_bytes([8u8; 16]);
    let mut routed = route();
    routed.step = RouteStep::Session;
    let session = child(Ok::<_, JobRouteError>((record_of(probe()), Some(physical))));
    assert!(is_child(&routed.next(session).unwrap()));
    assert_eq!(routed.job_id, physical);
}

// A session without a physical job is pending, and an unknown one is absent.
#[test]
fn session_outcomes() {
    let mut routed = route();
    routed.step = RouteStep::Session;
    let waiting = child(Ok::<_, JobRouteError>((record_of(probe()), None::<JobId>)));
    assert!(matches!(
        routed.next(waiting),
        Ok(Next::Done(JobReportLookup::Pending(JobState::Queued)))
    ));
    let mut routed = route();
    routed.step = RouteStep::Session;
    let unknown = child(Err::<(JobRecord, Option<JobId>), _>(
        JobRouteError::NotFound,
    ));
    assert!(matches!(
        routed.next(unknown),
        Ok(Next::Done(JobReportLookup::NotFound))
    ));
}
