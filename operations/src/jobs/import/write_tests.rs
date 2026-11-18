//! Writes import entries through their calls and answers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::super::reports::report_read;
use super::super::tests::{bucket, plan, run, stops};
use super::*;
use crate::jobs::repository::test_support::child;
use crate::jobs::store::JobMutationError;

fn writing(step: Step) -> WriteEntryOperation {
    let mut write = WriteEntryOperation::new(run(), ImportCheckpoint::default(), &plan(&["a"]));
    write.row = Some(entry_report(write.entry.as_ref().unwrap()));
    write.bucket = Some(bucket());
    write.step = step;
    write
}

fn saved() -> IoResult {
    child(Ok::<_, JobMutationError>(()))
}

fn federated() -> IoResult {
    child(Ok::<_, crate::federation::import::ImportError>(()))
}

// Past the last entry the import moves on to its rewrite.
#[test]
fn last_entry_rewrites() {
    let checkpoint = ImportCheckpoint {
        next_entry: 1,
        ..Default::default()
    };
    let mut write = WriteEntryOperation::new(run(), checkpoint, &plan(&["a"]));
    write.first().unwrap();
    let Ok(Next::Done(done)) = write.next(federated()) else {
        panic!("expected the finished phase");
    };
    assert_eq!(done.phase, ImportPhase::Rewrite);
}

// An open entry starts at its bucket and then reads its report row.
#[test]
fn entry_reads_report() {
    let mut write = writing(Step::Federated);
    assert_eq!(write.next(federated()), Ok(Next::Call(bucket_call("data"))));
    write.step = Step::Allowed;
    let allowed = child(Ok::<_, aruna_core::errors::AuthorizationError>(true));
    assert_eq!(
        write.next(allowed),
        Ok(Next::Call(report_read(&run(), "a")))
    );
}

// The planned version is recorded before the object is written.
#[test]
fn version_recorded_first() {
    let mut write = writing(Step::Report);
    assert!(matches!(
        write.next(IoResult::Values(vec![None])),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(write.step, Step::Version);
    let version = write.entry.as_ref().unwrap().version_id;
    assert_eq!(write.row.unwrap().detail.version_id, Some(version));
}

// An entry that cannot be opened fails the import.
#[test]
fn open_failure_fails() {
    let mut write = writing(Step::Open);
    let event = Event::Job(JobEvent::ArchiveEntry(Err("broken".to_string())));
    assert_eq!(
        write.next(IoResult::Event(Box::new(event))),
        Err(ImportFailure::Permanent("broken".to_string()))
    );
}

// A write error asks for the job's stops, and a stop wins over the error.
#[test]
fn put_checks_stops() {
    let mut write = writing(Step::Put);
    let failed = child(Err::<PutObjectResult, _>(PutObjectError::Unmapped));
    assert_eq!(
        write.next(failed),
        Ok(Next::Call(job_call(JobEffect::Stops)))
    );
    assert_eq!(
        write.next(stops(true, false)),
        Err(ImportFailure::Cancelled)
    );
}

// A version other than the planned one fails after its report row was written.
#[test]
fn divergent_version_fails() {
    let mut write = writing(Step::Divergent);
    assert!(matches!(
        write.next(saved()),
        Err(ImportFailure::Permanent(_))
    ));
}

// A finished entry counts by its code and queues its replication.
#[test]
fn final_report_counts() {
    let mut write = writing(Step::FinalReport);
    assert!(matches!(
        write.next(saved()),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(write.checkpoint.imported, 1);
    assert_eq!(write.step, Step::FinalQueue);
}

// A failed queue write does not stop the import.
#[test]
fn queue_failure_continues() {
    let mut write = writing(Step::FinalQueue);
    let failed = child(Err::<QueueBlobResult, _>(BlobQueueError::Conversion(
        aruna_core::errors::ConversionError::InvalidLength("broken".to_string()),
    )));
    let Ok(Next::Done(done)) = write.next(failed) else {
        panic!("expected the advanced checkpoint");
    };
    assert_eq!(done.next_entry, 1);
}

// A reference needs its queue write, unlike a stored object.
#[test]
fn reference_queue_retries() {
    let mut write = writing(Step::ReferenceQueue);
    let failed = child(Err::<QueueBlobResult, _>(BlobQueueError::Conversion(
        aruna_core::errors::ConversionError::InvalidLength("broken".to_string()),
    )));
    assert!(matches!(
        write.next(failed),
        Err(ImportFailure::Retryable(_))
    ));
}
