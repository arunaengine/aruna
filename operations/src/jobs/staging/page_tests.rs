//! Tests of the calls and results of recording a page.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use super::*;
use crate::jobs::staging::initial_checkpoint;
use crate::jobs::staging::pure_tests::{child_answer, test_bucket, test_connector, test_run};
use crate::jobs::store::mutation_error::JobMutationError;
use crate::s3::bucket::get::GetBucketError;
use aruna_core::errors::AuthorizationError;

fn file(observed: bool) -> StagedFile {
    StagedFile {
        source_path: "src/a".to_string(),
        target_key: "copy/a".to_string(),
        size: Some(1),
        observation: observed.then(|| SourceMetadata {
            content_length: 1,
            content_type: None,
            etag: None,
            last_modified: None,
            source_version: None,
            digests: Default::default(),
        }),
        manifest: Vec::new(),
        metadata: Default::default(),
        modified: None,
        mirrors: Vec::new(),
    }
}

fn import(files: Vec<StagedFile>, source: Option<PageSource>) -> ImportPageOperation {
    let run = test_run();
    let next = initial_checkpoint(&run.spec);
    let rows = (files, Vec::new(), Vec::new());
    ImportPageOperation::new(run, WalkPlace::default(), next, rows, source)
}

fn bucket_answer() -> IoResult {
    child_answer(Ok::<_, GetBucketError>(test_bucket(&test_run().spec)))
}

fn rules_answer() -> IoResult {
    child_answer(Ok::<_, AuthorizationError>(PermissionRules::default()))
}

// A file the submitter may not reach fails alone; a page left empty only writes its checkpoint.
#[test]
fn denied_file_counted() {
    let mut page = import(vec![file(true)], Some((test_connector(), None)));
    assert!(matches!(page.first(), Ok(Next::Call(IoCall::Child(_)))));
    page.next(bucket_answer()).unwrap();
    assert_eq!(page.step, ImportStep::Rules);
    assert!(matches!(
        page.next(rules_answer()),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(page.step, ImportStep::Mirrors);
    assert_eq!(page.next.items_failed, 1);
    let mirrored = MirroredPage {
        next: page.next.clone(),
        entries: Vec::new(),
        connectors: Vec::new(),
    };
    let answer = child_answer(Ok::<_, PageFailure>(mirrored));
    assert!(matches!(
        page.next(answer),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(page.step, ImportStep::Prepare);
}

// Without a known connector and without entries there is nothing to resolve.
#[test]
fn empty_page_commits() {
    let mut page = import(Vec::new(), None);
    page.first().unwrap();
    page.next(bucket_answer()).unwrap();
    page.next(rules_answer()).unwrap();
    let mirrored = MirroredPage {
        next: page.next.clone(),
        entries: Vec::new(),
        connectors: Vec::new(),
    };
    page.next(child_answer(Ok::<_, PageFailure>(mirrored)))
        .unwrap();
    assert_eq!(page.step, ImportStep::Commit);
    let written = child_answer(Ok::<(), JobMutationError>(()));
    assert!(matches!(page.next(written), Ok(Next::Done(_))));
}

// The page's commit errors decide the job: a changed bucket lasts, a conflict defers.
#[test]
fn commit_errors_map() {
    let failed = |error| {
        let mut page = import(Vec::new(), None);
        page.step = ImportStep::Turns;
        page.next(child_answer(Ok::<_, IoError>(Err::<PageImportResult, _>(
            error,
        ))))
    };
    assert!(matches!(
        failed(PageImportError::BucketChanged),
        Err(PageFailure::Permanent(_))
    ));
    assert!(matches!(failed(conflict()), Err(PageFailure::Conflict(_))));
    assert!(matches!(
        failed(PageImportError::ClaimLost),
        Err(PageFailure::Retry(_))
    ));
}

fn input() -> PageImportInput {
    let run = test_run();
    let spec = &run.spec;
    PageImportInput {
        job_id: run.job_id,
        claim_token: run.claim_token,
        realm_id: spec.auth_context.realm_id,
        group_id: spec.group_id,
        user_id: spec.auth_context.user_id,
        auth_context: spec.auth_context.clone(),
        node_id: spec.node_id,
        bucket: spec.bucket.clone(),
        expected_bucket: test_bucket(spec),
        connector: test_connector(),
        secret_fingerprint: None,
        entries: Vec::new(),
        checkpoint: initial_checkpoint(spec),
        checkpoint_key: checkpoint_key(run.job_id, None),
        writes: Vec::new(),
        deletes: Vec::new(),
        now: SystemTime::UNIX_EPOCH,
        ranges: None,
        mirror_connectors: Vec::new(),
    }
}

fn attempts(attempts: u32) -> PageAttemptsOperation {
    PageAttemptsOperation {
        input: input(),
        prepared: None,
        attempts,
        pausing: false,
        last: None,
    }
}

fn job_event(event: JobEvent) -> IoResult {
    IoResult::Event(Box::new(Event::Job(event)))
}

// A conflicting commit pauses and runs a new transaction; the pause grows with the attempts.
#[test]
fn conflict_runs_again() {
    let mut page = attempts(2);
    assert!(matches!(page.first(), Ok(Next::Call(IoCall::Child(_)))));
    let lost = child_answer(Err::<PageImportResult, _>(conflict()));
    let pause = Effect::Job(JobEffect::Pause(CONFLICT_PAUSE * 2));
    assert_eq!(
        page.next(lost),
        Ok(Next::Call(IoCall::Effect(Box::new(pause))))
    );
    let woke = job_event(JobEvent::Paused { stopped: false });
    assert!(matches!(page.next(woke), Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(page.attempts, 3);
}

// A stop during the pause ends the page with its conflict.
#[test]
fn stop_ends_pause() {
    let mut page = attempts(1);
    page.first().unwrap();
    page.next(child_answer(Err::<PageImportResult, _>(conflict())))
        .unwrap();
    let stopped = job_event(JobEvent::Paused { stopped: true });
    let Ok(Next::Done(end)) = page.next(stopped) else {
        panic!("expected the end of the attempts");
    };
    assert_eq!(end.result, Err(conflict()));
}

// The last attempt returns its conflict without another pause.
#[test]
fn attempts_are_bounded() {
    let mut page = attempts(PAGE_ATTEMPTS);
    page.first().unwrap();
    let lost = child_answer(Err::<PageImportResult, _>(conflict()));
    let Ok(Next::Done(end)) = page.next(lost) else {
        panic!("expected the end of the attempts");
    };
    assert_eq!(end.result, Err(conflict()));
}

fn turns_end(end: AttemptsEnd) -> IoResult {
    let child = Event::Child(aruna_core::calls::ChildOutcome::new(Ok::<_, IoError>(end)));
    job_event(JobEvent::Finished(Box::new(child)))
}

// The page holds its ranges' turns and the job's lane turn while it runs.
#[test]
fn page_takes_turns() {
    let (set, range) = (Ulid::from_bytes([3; 16]), Ulid::from_bytes([4; 16]));
    let lane = lane_turn(test_run().job_id);
    let mut page = PageTurnsOperation::new(input(), Turns::from([(set, range)]), Some(lane), None);
    let Ok(Next::Call(IoCall::Effect(effect))) = page.first() else {
        panic!("expected the turns");
    };
    let Effect::Job(JobEffect::InTurns { turns, .. }) = *effect else {
        panic!("expected the turns");
    };
    assert_eq!(turns, Turns::from([(set, range), lane]));
    assert_eq!(page.input.ranges, Some(BTreeSet::from([range])));
}

// Ranges that moved send the page into the turns of the new ones, counting the attempt.
#[test]
fn moved_ranges_retake() {
    let (set, old, new) = (
        Ulid::from_bytes([3; 16]),
        Ulid::from_bytes([4; 16]),
        Ulid::from_bytes([5; 16]),
    );
    let mut page = PageTurnsOperation::new(input(), Turns::from([(set, old)]), None, None);
    page.first().unwrap();
    let moved = turns_end(AttemptsEnd {
        attempts: 1,
        result: Err(PageImportError::RangesMoved(BTreeSet::from([new]))),
    });
    let Ok(Next::Call(IoCall::Effect(effect))) = page.next(moved) else {
        panic!("expected the new turns");
    };
    let Effect::Job(JobEffect::InTurns { turns, .. }) = *effect else {
        panic!("expected the new turns");
    };
    assert_eq!(turns, Turns::from([(set, new)]));
    assert_eq!(page.attempts, 2);
    assert_eq!(page.input.ranges, Some(BTreeSet::from([new])));
}

fn snapshot(files: Vec<StagedFile>) -> SnapshotPageOperation {
    let run = test_run();
    let checkpoint = initial_checkpoint(&run.spec);
    let next = checkpoint.clone();
    SnapshotPageOperation::new(run, checkpoint, next, (files, Vec::new(), Vec::new()))
}

fn stops(cancelled: bool) -> IoResult {
    job_event(JobEvent::Stops(aruna_core::jobs::JobStops {
        cancelled,
        shutdown: false,
    }))
}

// A stop before a file leaves the page open, so a later run copies it again.
#[test]
fn stop_leaves_page() {
    let mut page = snapshot(vec![file(true)]);
    page.first().unwrap();
    assert!(matches!(
        page.next(bucket_answer()),
        Ok(Next::Call(IoCall::Effect(_)))
    ));
    let Ok(Next::Done(checkpoint)) = page.next(stops(true)) else {
        panic!("expected the page to stop");
    };
    assert_eq!(checkpoint, page.checkpoint);
}

// A copied file counts with its size, shows in the progress, and the page then commits.
#[test]
fn copied_file_counts() {
    let mut page = snapshot(vec![file(true)]);
    page.first().unwrap();
    page.next(bucket_answer()).unwrap();
    assert!(matches!(
        page.next(stops(false)),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let report = JobReport {
        current: Some(1),
        ..JobReport::default()
    };
    let report = Effect::Job(JobEffect::Report(report));
    assert_eq!(
        page.next(child_answer(Ok::<u64, ItemFailure>(5))),
        Ok(Next::Call(IoCall::Effect(Box::new(report))))
    );
    assert_eq!(page.next.bytes_current, 5);
    assert!(matches!(
        page.next(job_event(JobEvent::Reported)),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(page.step, SnapshotStep::Commit);
}
