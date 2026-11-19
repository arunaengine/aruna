//! Tests of the calls and results of the staging job loop.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::structs::execution::job::StagingJobItem;
use byteview::ByteView;

use super::*;
use crate::jobs::staging::pure_tests::{child_answer, test_connector, test_run};
use crate::jobs::store::mutation_error::JobMutationError;
use crate::tests::staging::test_spec;

fn job_event(event: JobEvent) -> IoResult {
    IoResult::Event(Box::new(Event::Job(event)))
}

fn stops(cancelled: bool, shutdown: bool) -> IoResult {
    job_event(JobEvent::Stops(JobStops {
        cancelled,
        shutdown,
    }))
}

fn written() -> IoResult {
    child_answer(Ok::<(), JobMutationError>(()))
}

fn stored(flat: Option<bool>) -> IoResult {
    let mut checkpoint = initial_checkpoint(&test_spec());
    checkpoint.flat = flat;
    let value = ByteView::from(postcard::to_allocvec(&checkpoint).unwrap());
    IoResult::Values(vec![Some(value)])
}

/// A job that read its checkpoint, wrote it again and waits for the stop state.
fn started() -> StagingJobOperation {
    let mut job = StagingJobOperation::new(test_run());
    assert_eq!(job.first(), Ok(Next::Call(main_read(job.run.job_id))));
    assert!(matches!(
        job.next(stored(Some(false))),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(
        job.next(written()),
        Ok(Next::Call(job_call(JobEffect::Stops)))
    );
    job
}

fn with_table(run: StagingRun) -> StagingRun {
    let mut spec = (*run.spec).clone();
    let rows = serde_json::json!({ "kind": "file", "source": "{a}", "target": "{a}" });
    let table = serde_json::json!({ "source": { "connector": "list.csv" }, "rows": rows });
    spec.table = Some(serde_json::from_value(table).unwrap());
    StagingRun {
        spec: Arc::new(spec),
        ..run
    }
}

// Sync is not a job strategy: it fails for good before anything is read.
#[test]
fn sync_strategy_refused() {
    let mut spec = test_spec();
    spec.strategy = StagingStrategy::Sync;
    let run = StagingRun {
        spec: Arc::new(spec),
        ..test_run()
    };
    let Ok(Next::Done(JobRunOutcome::Failed(error))) = StagingJobOperation::new(run).first() else {
        panic!("expected the refusal");
    };
    assert_eq!(error.kind, JobErrorKind::Permanent);
}

// A new job learns from its connector whether listings are flat, then records that.
#[test]
fn job_asks_connector() {
    let mut job = StagingJobOperation::new(test_run());
    job.first().unwrap();
    let missing = IoResult::Values(vec![None]);
    assert!(matches!(
        job.next(missing),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(job.step, Step::Connector);
    let source = GetSourceResult {
        connector: test_connector(),
        has_secret_config: false,
    };
    let answer = child_answer(Ok::<_, GetSourceError>(source));
    assert!(matches!(job.next(answer), Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(job.step, Step::Seed);
    assert_eq!(job.checkpoint().unwrap().flat, Some(true));
}

// A connector that cannot be read fails the job for good after its rows are cleared.
#[test]
fn missing_connector_fails() {
    let mut job = StagingJobOperation::new(test_run());
    job.first().unwrap();
    job.next(IoResult::Values(vec![None])).unwrap();
    let failed = child_answer(Err::<GetSourceResult, _>(GetSourceError::NotFound));
    assert!(matches!(job.next(failed), Ok(Next::Call(IoCall::Child(_)))));
    let cleared = child_answer(Ok::<(), IoError>(()));
    let Ok(Next::Done(JobRunOutcome::Failed(error))) = job.next(cleared) else {
        panic!("expected the failure");
    };
    assert_eq!(error.kind, JobErrorKind::Permanent);
}

// A cancel clears the job's rows first; a shutdown just hands the job back.
#[test]
fn stops_end_job() {
    let mut cancelled = started();
    assert!(matches!(
        cancelled.next(stops(true, false)),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let cleared = child_answer(Ok::<(), IoError>(()));
    assert_eq!(
        cancelled.next(cleared),
        Ok(Next::Done(JobRunOutcome::Cancelled))
    );

    let mut interrupted = started();
    assert_eq!(
        interrupted.next(stops(false, true)),
        Ok(Next::Done(JobRunOutcome::Interrupted))
    );
}

// Items run first; each is committed and shown before the next stop check.
#[test]
fn items_run_first() {
    let mut spec = test_spec();
    spec.items = vec![StagingJobItem {
        source_path: "a".to_string(),
        target_key: "b".to_string(),
    }];
    let run = StagingRun {
        spec: Arc::new(spec),
        ..test_run()
    };
    let mut job = StagingJobOperation::new(run);
    job.first().unwrap();
    job.next(stored(Some(false))).unwrap();
    job.next(written()).unwrap();
    job.next(stops(false, false)).unwrap();
    assert_eq!(job.step, Step::Item);
    let mut checkpoint = job.checkpoint().unwrap().clone();
    checkpoint.items_done = 1;
    checkpoint.items_current = 1;
    let done = child_answer(Ok::<_, PageFailure>(checkpoint));
    assert!(matches!(job.next(done), Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(job.step, Step::ItemCommit);
    let report = JobReport {
        current: Some(1),
        total: None,
        advance: 0,
        checkpoint: true,
    };
    assert_eq!(
        job.next(written()),
        Ok(Next::Call(job_call(JobEffect::Report(report))))
    );
    assert_eq!(
        job.next(job_event(JobEvent::Reported)),
        Ok(Next::Call(job_call(JobEffect::Stops)))
    );
}

// With nothing left to walk, the job records its totals, clears its rows and succeeds.
#[test]
fn finished_job_succeeds() {
    let mut spec = test_spec();
    spec.prefixes.clear();
    let run = StagingRun {
        spec: Arc::new(spec),
        ..test_run()
    };
    let mut job = StagingJobOperation::new(run);
    job.first().unwrap();
    job.next(stored(Some(false))).unwrap();
    job.next(written()).unwrap();
    assert!(matches!(
        job.next(stops(false, false)),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(job.step, Step::Finish);
    assert_eq!(job.checkpoint().unwrap().phase, StagingJobPhase::Completed);
    assert!(matches!(
        job.next(written()),
        Ok(Next::Call(IoCall::Effect(_)))
    ));
    assert!(matches!(
        job.next(job_event(JobEvent::Reported)),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let cleared = child_answer(Ok::<(), IoError>(()));
    assert!(matches!(
        job.next(cleared),
        Ok(Next::Done(JobRunOutcome::Succeeded(_)))
    ));
}

// A walk failure maps to the job's outcome: retry, defer or a cleaned up lasting failure.
#[test]
fn page_failures_map() {
    let failed = |failure| {
        let mut job = started();
        job.step = Step::Walk;
        let answer = child_answer(Err::<WalkedPage, _>(failure));
        (job.next(answer), job)
    };
    let (retry, _) = failed(PageFailure::Retry("again".to_string()));
    assert!(matches!(retry, Ok(Next::Done(JobRunOutcome::Failed(_)))));
    let (conflict, _) = failed(PageFailure::Conflict("busy".to_string()));
    assert!(matches!(
        conflict,
        Ok(Next::Done(JobRunOutcome::Deferred(_)))
    ));
    let (lasting, job) = failed(PageFailure::Permanent("gone".to_string()));
    assert!(matches!(lasting, Ok(Next::Call(IoCall::Child(_)))));
    assert!(matches!(job.step, Step::Clear(_)));
}

// A table import that fails while it is stopped ends as that stop and spends no attempt.
#[test]
fn stopped_table_failure() {
    let run = with_table(test_run());
    let failing = || {
        let mut job = StagingJobOperation::new(run.clone());
        job.checkpoint = Some(initial_checkpoint(&run.spec));
        job.step = Step::Walk;
        let failure = PageFailure::Retry("paced wait ended".to_string());
        assert_eq!(
            job.next(child_answer(Err::<WalkedPage, _>(failure))),
            Ok(Next::Call(job_call(JobEffect::Stops)))
        );
        job
    };
    let mut shutdown = failing();
    assert_eq!(
        shutdown.next(stops(false, true)),
        Ok(Next::Done(JobRunOutcome::Interrupted))
    );
    let mut cancelled = failing();
    assert!(matches!(
        cancelled.next(stops(true, false)),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let cleared = child_answer(Ok::<(), IoError>(()));
    assert_eq!(
        cancelled.next(cleared),
        Ok(Next::Done(JobRunOutcome::Cancelled))
    );
    let mut running = failing();
    assert!(matches!(
        running.next(stops(false, false)),
        Ok(Next::Done(JobRunOutcome::Failed(_)))
    ));
}

// A permanent failure stays terminal through a shutdown, as its rows are already cleared.
#[test]
fn permanent_survives_shutdown() {
    let shutdown = JobStops {
        cancelled: false,
        shutdown: true,
    };
    assert_eq!(
        stopped_outcome(shutdown, permanent("gone")),
        Ok(permanent("gone"))
    );
    assert_eq!(
        stopped_outcome(shutdown, retryable("again")),
        Ok(JobRunOutcome::Interrupted)
    );
}
