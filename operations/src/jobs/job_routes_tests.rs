//! Call and result tests of the routed job reads.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::jobs::JobKind;
use aruna_core::structs::execution::job::JobRecord;

use super::*;
use crate::jobs::job_reads::tests::{
    auth_of, child, is_child, job_id, node, owner, probe, realm, record_of, stranger,
};

fn serving() -> ServingNode {
    Some((node(1), realm()))
}

fn remote(response: JobResponse) -> IoResult {
    child(Ok::<_, JobRouteError>(JobRouteOutcome::Remote(response)))
}

fn record_route() -> RecordRouteOperation {
    RecordRouteOperation::new(owner(), job_id(), None, serving(), 5)
}

fn status_route() -> StatusRouteOperation {
    StatusRouteOperation::new(&auth_of(owner()), job_id(), None, serving(), 5)
}

fn record_answer(record: JobRecord) -> JobResponse {
    JobResponse::Record {
        record: Box::new(record),
        checkpoint: None,
    }
}

// The route first asks for the family's responder and then routes through it.
#[test]
fn family_route_steps() {
    let mut route = FamilyRouteOperation::new(job_id(), None, (node(1), realm()), 5);
    assert!(is_child(&route.first().unwrap()));
    let responder = child(Ok::<Option<NodeId>, JobRouteError>(Some(node(2))));
    assert!(is_child(&route.next(responder).unwrap()));
    let local = child(Ok::<_, JobRouteError>(JobRouteOutcome::Local));
    assert!(matches!(
        route.next(local),
        Ok(Next::Done(JobRouteOutcome::Local))
    ));
}

// Refusals keep their meaning and an unknown answer is unavailable.
#[test]
fn refusals_mapped() {
    assert_eq!(
        refused(JobResponse::Unauthorized, "x"),
        JobRouteError::Unauthorized
    );
    assert_eq!(
        refused(JobResponse::Forbidden, "x"),
        JobRouteError::Forbidden
    );
    assert_eq!(
        refused(JobResponse::Unavailable("gone".into()), "x"),
        JobRouteError::Unavailable("gone".into())
    );
    assert!(matches!(
        refused(JobResponse::KeyWakeAcked, "x"),
        JobRouteError::Unavailable(message) if message.contains("unexpected x response")
    ));
}

// Without a network the record is read here.
#[test]
fn record_without_network() {
    let mut route = RecordRouteOperation::new(owner(), job_id(), None, None, 5);
    assert!(is_child(&route.first().unwrap()));
    assert_eq!(route.step, RecordStep::Local);
    let data = child(Ok::<Option<RecordData>, JobReadError>(None));
    assert!(matches!(route.next(data), Ok(Next::Done(None))));
}

// Only the caller's own record is accepted from the owner, which alone answers absence.
#[test]
fn record_answers_checked() {
    let mut route = record_route();
    let own = remote(record_answer(record_of(probe())));
    assert!(matches!(route.next(own), Ok(Next::Done(Some(_)))));

    let mut foreign = record_of(probe());
    foreign.created_by = stranger();
    let mut route = record_route();
    let answer = remote(record_answer(foreign));
    assert_eq!(route.next(answer).unwrap_err(), JobRouteError::NotFound);

    let mut route = record_route();
    let absent = remote(JobResponse::NotFound);
    assert!(matches!(route.next(absent), Ok(Next::Done(None))));
}

// A family projection answers at once; without one a local owner reads its own store.
#[test]
fn status_family_first() {
    let mut route = status_route();
    assert!(is_child(&route.first().unwrap()));
    let view = JobStatusView::from(&record_of(probe()));
    let status = RoutedJobStatus {
        job: view,
        run_crate: None,
    };
    let family = child(Ok::<_, JobRouteError>(Some(status)));
    assert!(matches!(route.next(family), Ok(Next::Done(_))));

    let mut route = status_route();
    let none = child(Ok::<Option<RoutedJobStatus>, JobRouteError>(None));
    assert!(is_child(&route.next(none).unwrap()));
    assert_eq!(route.step, StatusStep::Route);
    let local = child(Ok::<_, JobRouteError>(JobRouteOutcome::Local));
    assert!(is_child(&route.next(local).unwrap()));
    assert_eq!(route.step, StatusStep::Local);
}

// A remote status must describe the caller's visible job.
#[test]
fn status_answers_checked() {
    let view = JobStatusView::from(&record_of(probe()));
    assert!(routed_job_matches(&view, owner(), job_id()));
    assert!(!routed_job_matches(&view, stranger(), job_id()));
    assert!(!routed_job_matches(
        &view,
        owner(),
        JobId::from_bytes([5u8; 16])
    ));
    let mut internal = JobStatusView::from(&record_of(probe()));
    internal.kind = JobKind::WriteRunCrate;
    assert!(!routed_job_matches(&internal, owner(), job_id()));

    let mut route = status_route();
    route.step = StatusStep::Route;
    let answer = remote(JobResponse::Status {
        job: view,
        run_crate: Some("{}".to_string()),
    });
    let done = route.next(answer).unwrap();
    assert!(matches!(done, Next::Done(status) if status.run_crate.is_some()));

    let mut route = status_route();
    route.step = StatusStep::Route;
    let absent = remote(JobResponse::NotFound);
    assert_eq!(route.next(absent).unwrap_err(), JobRouteError::NotFound);
}
