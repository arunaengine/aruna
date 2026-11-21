//! Probes export candidates through their calls and answers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;

use aruna_core::events::Event;
use aruna_core::jobs::{JobEvent, JobStops};

use super::super::tests::{plain_entity, source_scope, stored_candidate};
use super::*;
use crate::jobs::repository::test_support::child;

fn entity() -> ExportEntity {
    let mut entity = plain_entity("a");
    entity.candidates = vec![stored_candidate()];
    entity
}

fn probe(failures: CandidateFailures) -> ProbeSourcesOperation {
    ProbeSourcesOperation::new(
        source_scope(),
        vec![entity()],
        failures,
        AliasState::default(),
    )
}

fn running() -> IoResult {
    let stops = JobStops::default();
    IoResult::Event(Box::new(Event::Job(JobEvent::Stops(stops))))
}

fn answer(open: CandidateOpen) -> IoResult {
    child(Ok::<_, ExportFailure>(open))
}

fn opened_to_status(status: OpenStatus) -> Result<Next<Probed>, ExportFailure> {
    let mut probe = probe(BTreeMap::new());
    probe.first().unwrap();
    probe.next(running()).unwrap();
    probe.next(answer(CandidateOpen::Status(status)))
}

fn left_out(turn: Result<Next<Probed>, ExportFailure>) -> (Option<ReasonCode>, Option<String>) {
    let Ok(Next::Done(probed)) = turn else {
        panic!("expected the finished probe");
    };
    assert!(probed.entries.is_empty());
    let entity = &probed.entities[0];
    (entity.omission, entity.message.clone())
}

// Entities already left out are not probed.
#[test]
fn omitted_skipped() {
    let mut entity = entity();
    entity.omission = Some(ReasonCode::External);
    let mut probe = ProbeSourcesOperation::new(
        source_scope(),
        vec![entity],
        BTreeMap::new(),
        AliasState::default(),
    );
    assert!(matches!(probe.first(), Ok(Next::Done(_))));
}

// The first candidate that opens is chosen with its size and hash.
#[test]
fn opened_candidate_selected() {
    let mut probe = probe(BTreeMap::new());
    assert_eq!(probe.first(), Ok(Next::Call(job_call(JobEffect::Stops))));
    assert!(matches!(
        probe.next(running()),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let opened = CandidateOpen::Opened(BaoReadOutput::Metadata {
        size: 4,
        blake3: [1; 32],
        etag: None,
        hashes: HashMap::new(),
    });
    let Ok(Next::Done(probed)) = probe.next(answer(opened)) else {
        panic!("expected the finished probe");
    };
    assert_eq!(probed.entries.len(), 1);
    assert_eq!(
        (probed.entries[0].size, probed.entries[0].hash),
        (4, [1; 32])
    );
}

// A denied candidate leaves the entity out with that reason.
#[test]
fn denied_left_out() {
    assert_eq!(
        left_out(opened_to_status(OpenStatus::Denied)),
        (
            Some(ReasonCode::Denied),
            Some("all payload candidates denied READ".to_string())
        )
    );
}

// An offline candidate is reported as offline, a missing one as missing.
#[test]
fn offline_left_out() {
    let (code, _) = left_out(opened_to_status(OpenStatus::Offline));
    assert_eq!(code, Some(ReasonCode::Offline));
    let (code, _) = left_out(opened_to_status(OpenStatus::Missing));
    assert_eq!(code, Some(ReasonCode::Missing));
}

// A corrupt copy fails the attempt, so a later one can find a good copy.
#[test]
fn corrupt_retries() {
    assert_eq!(
        opened_to_status(OpenStatus::Corrupt),
        Err(ExportFailure::Retryable(
            "payload integrity check failed".to_string()
        ))
    );
}

// A copy that needs a bucket key the caller lacks fails the export.
#[test]
fn unheld_fails() {
    assert!(matches!(
        opened_to_status(OpenStatus::NotHolder),
        Err(ExportFailure::Permanent(_))
    ));
}

// A candidate that failed in the archive is not opened again.
#[test]
fn failed_candidate_skipped() {
    let failures = BTreeMap::from([(0, BTreeMap::from([(0, OpenStatus::Offline)]))]);
    let mut probe = probe(failures);
    let (code, _) = left_out(probe.first());
    assert_eq!(code, Some(ReasonCode::Offline));
}

// A stop ends the probe.
#[test]
fn stop_ends_probe() {
    let mut probe = probe(BTreeMap::new());
    probe.first().unwrap();
    let stops = JobStops {
        cancelled: true,
        shutdown: false,
    };
    let stopped = IoResult::Event(Box::new(Event::Job(JobEvent::Stops(stops))));
    assert_eq!(probe.next(stopped), Err(ExportFailure::Cancelled));
}
