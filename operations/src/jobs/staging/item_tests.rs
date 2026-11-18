//! Tests of the calls and results of staging one item.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::jobs::staging::pure_tests::{child_answer, test_bucket, test_run};
use crate::s3::bucket::get::GetBucketError;

fn paths() -> (String, String) {
    ("src/a".to_string(), "copy/a".to_string())
}

fn bucket_answer() -> IoResult {
    let run = test_run();
    child_answer(Ok::<_, GetBucketError>(test_bucket(&run.spec)))
}

fn item() -> StagingJobItem {
    StagingJobItem {
        source_path: "src/a".to_string(),
        target_key: "copy/a".to_string(),
    }
}

fn item_run() -> RunItemOperation {
    let run = test_run();
    let checkpoint = crate::jobs::staging::initial_checkpoint(&run.spec);
    RunItemOperation::new(run, item(), checkpoint)
}

// The submitter's rules decide before anything is written: no rule, no access.
#[test]
fn stage_checks_access() {
    let mut stage = StageItemOperation::new(test_run(), paths(), Vec::new(), 0);
    assert!(matches!(stage.first(), Ok(Next::Call(IoCall::Child(_)))));
    assert!(matches!(
        stage.next(bucket_answer()),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let rules = child_answer(Ok::<_, AuthorizationError>(PermissionRules::default()));
    assert_eq!(
        stage.next(rules),
        Err(ItemFailure::Denied("permission denied".to_string()))
    );
}

// A destination replaced since the submission ends the job, not only the item.
#[test]
fn replaced_bucket_lasts() {
    let run = test_run();
    let mut bucket = test_bucket(&run.spec);
    bucket.group_id = ulid::Ulid::from_bytes([9; 16]);
    let mut stage = StageItemOperation::new(run, paths(), Vec::new(), 0);
    stage.first().unwrap();
    let answer = child_answer(Ok::<_, GetBucketError>(bucket));
    assert!(matches!(stage.next(answer), Err(ItemFailure::Lasting(_))));
}

// A version without its replication seed is no success, whatever the write did.
#[test]
fn seed_failure_fails() {
    let mut stage = StageItemOperation::new(test_run(), paths(), Vec::new(), 0);
    stage.step = StageStep::Replicate(4);
    let failed = child_answer(Err::<QueueBlobResult, _>(BlobQueueError::UnexpectedEvent(
        "full".to_string(),
    )));
    assert!(matches!(stage.next(failed), Err(ItemFailure::Stage(_))));
}

// An ordinary bucket has one writer, so nothing is imported elsewhere.
#[test]
fn ordinary_bucket_here() {
    let mut elsewhere = ElsewhereOperation::new(test_run(), "copy/a".to_string());
    elsewhere.first().unwrap();
    assert_eq!(elsewhere.next(bucket_answer()), Ok(Next::Done(false)));
}

// An item another coordinator imports counts as done and fails nothing.
#[test]
fn item_elsewhere_skipped() {
    let mut run = item_run();
    run.first().unwrap();
    let Ok(Next::Done(checkpoint)) = run.next(child_answer(Ok::<_, ItemFailure>(true))) else {
        panic!("the item is skipped");
    };
    assert_eq!(checkpoint.items_done, 1);
    assert_eq!(checkpoint.items_current, 0);
}

// A failed item is recorded unless its key moved to another coordinator meanwhile.
#[test]
fn failed_item_recorded() {
    let failed = || child_answer(Err::<u64, _>(ItemFailure::Stage("no".to_string())));
    let mut run = item_run();
    run.step = RunStep::Stage;
    assert!(matches!(
        run.next(failed()),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let Ok(Next::Done(checkpoint)) = run.next(child_answer(Ok::<_, ItemFailure>(false))) else {
        panic!("the item ends");
    };
    assert_eq!((checkpoint.items_done, checkpoint.items_failed), (1, 1));

    let mut moved = item_run();
    moved.step = RunStep::Stage;
    moved.next(failed()).unwrap();
    let Ok(Next::Done(checkpoint)) = moved.next(child_answer(Ok::<_, ItemFailure>(true))) else {
        panic!("the item ends");
    };
    assert_eq!((checkpoint.items_done, checkpoint.items_failed), (1, 0));
}

// A system failure retries the job, a lasting one ends it.
#[test]
fn item_failures_map() {
    let mut run = item_run();
    run.step = RunStep::Stage;
    let system = child_answer(Err::<u64, _>(ItemFailure::System("busy".to_string())));
    assert_eq!(
        run.next(system),
        Err(PageFailure::Retry("busy".to_string()))
    );
    let mut run = item_run();
    run.step = RunStep::Stage;
    let lasting = child_answer(Err::<u64, _>(ItemFailure::Lasting("gone".to_string())));
    assert_eq!(
        run.next(lasting),
        Err(PageFailure::Permanent("gone".to_string()))
    );
}
