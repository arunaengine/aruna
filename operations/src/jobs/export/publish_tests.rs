//! Publishes exports through the repository and report row calls and answers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::jobs::JobStops;
use aruna_core::structs::execution::job::{ExportReportDetail, ReasonCode};

use super::super::tests::{destination, export_scope, op_spec};
use super::*;
use crate::jobs::repository::test_support::{artifact, child};

fn repository(checkpoint: ExportCheckpoint) -> RepositoryStepOperation {
    RepositoryStepOperation::new(export_scope(), destination(), checkpoint)
}

fn row(code: ReasonCode) -> ExportReportRow {
    ExportReportRow {
        entry_key: "a".to_string(),
        code,
        message: None,
        detail: ExportReportDetail {
            entity_id: "a".to_string(),
            zip_path: None,
            source: None,
            resolved_version: None,
            validation: None,
        },
    }
}

fn stops(cancelled: bool) -> IoResult {
    let stops = JobStops {
        cancelled,
        shutdown: !cancelled,
    };
    IoResult::Event(Box::new(Event::Job(JobEvent::Stops(stops))))
}

// A crate with omitted files never reaches a repository.
#[test]
fn omissions_refused() {
    let checkpoint = ExportCheckpoint {
        report: vec![row(ReasonCode::Missing)],
        ..Default::default()
    };
    assert!(matches!(
        repository(checkpoint).first(),
        Err(ExportFailure::Permanent(_))
    ));
}

// A publish starts at its connector, and a finished one has nothing left to do.
#[test]
fn publish_starts_connector() {
    let mut open = repository(ExportCheckpoint::default());
    assert!(matches!(open.first(), Ok(Next::Call(IoCall::Child(_)))));
    let finished = ExportCheckpoint {
        repository_complete: true,
        ..Default::default()
    };
    let mut done = repository(finished.clone());
    assert_eq!(done.first(), Ok(Next::Done(finished)));
}

// A refusal is saved in the checkpoint before it fails the job.
#[test]
fn refusal_saved() {
    let mut step = repository(ExportCheckpoint::default());
    let failure = LinkFailure::SourceUnavailable;
    let refused = TransferError::Refused(failure.clone());
    let message = refused.to_string();
    let answer = child(Err::<ConnectorView, _>(refused));
    assert!(matches!(
        step.next(answer),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(step.checkpoint.link_failure, Some(failure));
    let saved = child(Ok::<_, JobMutationError>(()));
    assert_eq!(step.next(saved), Err(ExportFailure::Permanent(message)));
}

// Stops of the transfer keep their meaning.
#[test]
fn transfer_stops_map() {
    let mut step = repository(ExportCheckpoint::default());
    let stopped = child(Err::<ConnectorView, _>(TransferError::Cancelled));
    assert_eq!(step.next(stopped), Err(ExportFailure::Cancelled));
}

// The inspection runs until the job stops, and then ends with that stop.
#[test]
fn inspection_stops() {
    let mut inspect = InspectArtifactOperation::new(&op_spec(), &artifact());
    assert!(matches!(inspect.first(), Ok(Next::Call(IoCall::Effect(_)))));
    let stopped = IoResult::Event(Box::new(Event::Job(JobEvent::Stopped)));
    assert_eq!(
        inspect.next(stopped),
        Ok(Next::Call(job_call(JobEffect::Stops)))
    );
    assert_eq!(inspect.next(stops(true)), Err(TransferError::Cancelled));
}

// An export artifact may hold its metadata and report next to the entry limit.
#[test]
fn inspection_widens_limits() {
    let spec = op_spec();
    let inspect = InspectArtifactOperation::new(&spec, &artifact());
    let limits = inspect.read.unwrap().limits;
    assert_eq!(limits.max_entries, spec.limits.max_entries + 2);
    assert_eq!(
        limits.import_source_bytes,
        spec.limits.export_artifact_bytes
    );
}

// Each report row is written after a look at the job's stops.
#[test]
fn rows_check_stops() {
    let rows = vec![row(ReasonCode::Included)];
    let mut write = WriteRowsOperation::new(export_scope(), rows, true);
    assert_eq!(write.first(), Ok(Next::Call(job_call(JobEffect::Stops))));
    assert_eq!(write.next(stops(false)), Err(ExportFailure::Interrupted));
}

// Validation rows are written whatever the job's state.
#[test]
fn rows_write_directly() {
    let rows = vec![row(ReasonCode::Failed)];
    let mut write = WriteRowsOperation::new(export_scope(), rows, false);
    assert!(matches!(write.first(), Ok(Next::Call(IoCall::Child(_)))));
    let saved = child(Ok::<_, JobMutationError>(()));
    assert_eq!(write.next(saved), Ok(Next::Done(())));
}
