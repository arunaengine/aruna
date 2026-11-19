//! Drives the materialization queue operations through their calls and answers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::tests::{create_event, node};
use super::*;
use aruna_core::storage_entries::create_event_entry;

type Error = MetadataMaterializationError;

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

fn ids() -> (Ulid, Ulid) {
    (Ulid::from_bytes([31u8; 16]), Ulid::from_parts(31, 1))
}

fn event() -> MetadataEventRecord {
    let (document_id, event_id) = ids();
    create_event(document_id, event_id, "calls")
}

fn job(due_at_ms: u64) -> MetadataMaterializationRecord {
    let (document_id, event_id) = ids();
    MetadataMaterializationRecord {
        document_id,
        event_id,
        due_at_ms,
        attempts: 0,
        failures: 0,
        parks: 0,
    }
}

fn event_row() -> Value {
    create_event_entry(&event()).unwrap().2
}

fn sidecar_row(job: &MetadataMaterializationRecord) -> Value {
    document_job_entry(job).unwrap().2
}

fn job_key() -> Vec<u8> {
    materialization_job_key(&job(1)).to_vec()
}

fn sidecar_key() -> Vec<u8> {
    let (document_id, event_id) = ids();
    document_job_key(document_id, event_id).to_vec()
}

fn is_child<T>(next: &MaterializationTurn<T>) -> bool {
    matches!(next, Ok(Next::Call(IoCall::Child(_))))
}

fn page(space: &str, limit: usize) -> MaterializationTurn<()> {
    Ok(Next::Call(IoCall::Page {
        space: space.to_string(),
        prefix: Key::from(Vec::new()),
        after: None,
        limit,
    }))
}

fn same_call<T: std::fmt::Debug>(next: MaterializationTurn<T>, call: MaterializationTurn<()>) {
    match (next, call) {
        (Ok(Next::Call(next)), Ok(Next::Call(call))) => assert_eq!(next, call),
        (next, call) => panic!("expected {call:?}, got {next:?}"),
    }
}

#[test]
fn timer_waits_due() {
    let mut timer = NextTimerOperation::new();
    timer.stamp(1_000);
    assert!(is_child(&timer.first()));
    let due: DueJobs = (Vec::new(), false, Some(4_000));
    let after = timer.next(child(Ok::<_, Error>(due)));
    assert_eq!(after, Ok(Next::Done(Some(Duration::from_millis(3_000)))));
}

#[test]
fn timer_fires_now() {
    let mut timer = NextTimerOperation::new();
    timer.first().unwrap();
    let due: DueJobs = (vec![(job_key(), job(1))], false, None);
    let after = timer.next(child(Ok::<_, Error>(due)));
    assert_eq!(after, Ok(Next::Done(Some(Duration::ZERO))));
}

#[test]
fn scan_finds_due() {
    let (document_id, event_id) = ids();
    let mut scan = DueJobsOperation::new(10, 4);
    let first = scan.first();
    same_call(
        first,
        page(MATERIALIZATION_JOB_KEYSPACE, MATERIALIZATION_PAGE_SIZE),
    );
    let rows = vec![(Key::from(job_key()), Value::from(Vec::new()))];
    let sidecars = scan.next(IoResult::Page { rows, next: None });
    let read = vec![sidecar_read(document_id, event_id)];
    same_call(sidecars, Ok(Next::Call(IoCall::Read(read))));
    let live = scan.next(IoResult::Values(vec![Some(sidecar_row(&job(1)))]));
    let reads = vec![status_read(document_id), event_read(document_id, event_id)];
    same_call(live, Ok(Next::Call(IoCall::Read(reads))));
    let done = scan.next(IoResult::Values(vec![None, Some(event_row())]));
    assert_eq!(
        done,
        Ok(Next::Done((vec![(job_key(), job(1))], false, None)))
    );
}

#[test]
fn scan_stops_future() {
    let mut scan = DueJobsOperation::new(10, 4);
    scan.first().unwrap();
    let future = materialization_job_key(&job(50));
    let rows = vec![(future, Value::from(Vec::new()))];
    let done = scan.next(IoResult::Page { rows, next: None });
    assert_eq!(done, Ok(Next::Done((Vec::new(), false, Some(50)))));
}

#[test]
fn scan_prunes_orphan() {
    let mut scan = DueJobsOperation::new(10, 4);
    scan.first().unwrap();
    let rows = vec![(Key::from(job_key()), Value::from(Vec::new()))];
    scan.next(IoResult::Page { rows, next: None }).unwrap();
    let prune = scan.next(IoResult::Values(vec![None]));
    assert!(is_child(&prune));
    let done = scan.next(child(Ok::<(), Error>(())));
    assert_eq!(done, Ok(Next::Done((Vec::new(), false, None))));
}

#[test]
fn prune_drops_rows() {
    let (document_id, event_id) = ids();
    let stale = StaleJob {
        index_key: job_key(),
        seen: Some(job(1)),
        drop_sidecar: true,
    };
    let mut prune = PruneJobsOperation::new(vec![stale]);
    assert_eq!(prune.transaction(), TransactionMode::Write);
    let read = vec![sidecar_read(document_id, event_id)];
    assert_eq!(prune.first(), Ok(Next::Call(IoCall::Read(read))));
    let sidecar = Key::from(sidecar_key());
    let deletes = vec![
        global_row(job_key()),
        (DOCUMENT_JOB_KEYSPACE.to_string(), sidecar.clone()),
        (MATERIALIZATION_CAPTURE_KEYSPACE.to_string(), sidecar),
    ];
    let deleted = prune.next(IoResult::Values(vec![Some(sidecar_row(&job(1)))]));
    assert_eq!(deleted, Ok(Next::Call(IoCall::Delete(deletes))));
    assert_eq!(prune.next(IoResult::Done), Ok(Next::Done(())));
}

#[test]
fn prune_spares_retry() {
    let stale = StaleJob {
        index_key: job_key(),
        seen: Some(job(1)),
        drop_sidecar: true,
    };
    let mut prune = PruneJobsOperation::new(vec![stale]);
    prune.first().unwrap();
    let retry = MetadataMaterializationRecord {
        attempts: 1,
        ..job(1)
    };
    let kept = prune.next(IoResult::Values(vec![Some(sidecar_row(&retry))]));
    assert_eq!(kept, Ok(Next::Done(())));
}

#[test]
fn load_drops_malformed() {
    let (document_id, _) = ids();
    let mut load = GroupJobsOperation::new(document_id);
    let read = vec![status_read(document_id)];
    assert_eq!(load.first(), Ok(Next::Call(IoCall::Read(read))));
    let page = load.next(IoResult::Values(vec![None]));
    let prefix = Key::from(document_job_prefix(document_id));
    let call = IoCall::Page {
        space: DOCUMENT_JOB_KEYSPACE.to_string(),
        prefix,
        after: None,
        limit: MATERIALIZATION_PAGE_SIZE,
    };
    assert_eq!(page, Ok(Next::Call(call)));
    let key = Key::from(sidecar_key());
    let rows = vec![(key.clone(), Value::from(vec![1, 2, 3]))];
    let deleted = load.next(IoResult::Page { rows, next: None });
    let delete = vec![(DOCUMENT_JOB_KEYSPACE.to_string(), key)];
    assert_eq!(deleted, Ok(Next::Call(IoCall::Delete(delete))));
    assert_eq!(
        load.next(IoResult::Done),
        Ok(Next::Done(GroupJobs::default()))
    );
}

#[test]
fn older_event_blocks() {
    let (document_id, event_id) = ids();
    let mut older = OlderJobOperation::new(vec![job(1)]);
    let read = vec![event_read(document_id, event_id)];
    assert_eq!(older.first(), Ok(Next::Call(IoCall::Read(read))));
    let blocked = older.next(IoResult::Values(vec![Some(event_row())]));
    assert_eq!(blocked, Ok(Next::Done(true)));
}

#[test]
fn older_orphan_pruned() {
    let mut older = OlderJobOperation::new(vec![job(1)]);
    older.first().unwrap();
    assert!(is_child(&older.next(IoResult::Values(vec![None]))));
    let done = older.next(child(Ok::<(), Error>(())));
    assert_eq!(done, Ok(Next::Done(false)));
}

fn job_run(status: Option<MaterializationStatusRecord>) -> MaterializationJobOperation {
    let mut run = MaterializationJobOperation::new(
        (job_key(), job(1)),
        status,
        RawStateCache::default(),
        GitNode::default(),
    );
    run.stamp(5);
    run
}

fn processed(
    turn: MaterializationTurn<ProcessedMaterializationJob>,
) -> ProcessedMaterializationJob {
    match turn {
        Ok(Next::Done(processed)) => processed,
        other => panic!("expected a processed job, got {other:?}"),
    }
}

#[test]
fn missing_event_settles() {
    let (document_id, event_id) = ids();
    let mut run = job_run(None);
    let read = vec![event_read(document_id, event_id)];
    assert_eq!(run.first(), Ok(Next::Call(IoCall::Read(read))));
    let processed = processed(run.next(IoResult::Values(vec![None])));
    let settled = CompletedMaterializationJob::without_rows(job_key(), sidecar_key(), None);
    let finished = FinishedMaterializationJob::Completed(settled);
    assert_eq!(processed.finished, Some(finished));
    assert!(processed.attempted);
    assert!(!processed.stop_group);
}

#[test]
fn deleted_graph_fails() {
    let mut run = job_run(None);
    run.first().unwrap();
    assert!(is_child(
        &run.next(IoResult::Values(vec![Some(event_row())]))
    ));
    let processed = processed(run.next(child(Ok::<bool, GraphDeletedError>(true))));
    let Some(FinishedMaterializationJob::Completed(completed)) = processed.finished else {
        panic!("expected a completed job");
    };
    let status = completed.status.expect("failed status");
    assert_eq!(status.state, MaterializationState::Failed);
    assert_eq!(status.updated_at_ms, 5);
}

#[test]
fn prepare_failure_reschedules() {
    let mut run = job_run(None);
    run.first().unwrap();
    run.next(IoResult::Values(vec![Some(event_row())])).unwrap();
    let prepare = run.next(child(Ok::<bool, GraphDeletedError>(false)));
    assert!(is_child(&prepare));
    let failure = RawReadError::UnexpectedEvent("raw".to_string());
    let processed = processed(run.next(child(Err::<RawEventPlan, _>(failure))));
    assert!(matches!(
        processed.finished,
        Some(FinishedMaterializationJob::Rescheduled { .. })
    ));
    assert!(processed.stop_group);
}

#[test]
fn advanced_retry_removed() {
    let status = MaterializationStatusRecord {
        attempts: 1,
        ..MaterializationStatusRecord::pending(&event(), 1)
    };
    let mut run = job_run(Some(status));
    run.first().unwrap();
    let removed = run.next(IoResult::Values(vec![Some(event_row())]));
    let delete = vec![global_row(job_key())];
    assert_eq!(removed, Ok(Next::Call(IoCall::Delete(delete))));
    let processed = processed(run.next(IoResult::Done));
    assert_eq!(processed.finished, None);
    assert!(!processed.attempted);
}

#[test]
fn group_counts_jobs() {
    let mut group = JobGroupOperation::new(vec![(job_key(), job(1))], GitNode::default());
    assert!(is_child(&group.first()));
    assert!(is_child(
        &group.next(child(Ok::<_, Error>(GroupJobs::default())))
    ));
    let settled = CompletedMaterializationJob::without_rows(job_key(), sidecar_key(), None);
    let run = ProcessedMaterializationJob::completed(settled, Duration::from_millis(3));
    let Ok(Next::Done(outcome)) = group.next(child(Ok::<_, Error>(run))) else {
        panic!("expected the group outcome");
    };
    assert_eq!(outcome.processed, 1);
    assert_eq!(outcome.finished.len(), 1);
    assert_eq!(outcome.craqle_elapsed, Duration::from_millis(3));
    assert_eq!(outcome.error, None);
}

#[test]
fn group_keeps_failure() {
    let mut group = JobGroupOperation::new(vec![(job_key(), job(1))], GitNode::default());
    group.first().unwrap();
    let failure = || Error::Storage(StorageError::Timeout);
    let done = group.next(child(Err::<GroupJobs, _>(failure())));
    let Ok(Next::Done(outcome)) = done else {
        panic!("expected the group outcome");
    };
    assert_eq!(outcome.error, Some(failure()));
}

#[test]
fn plan_parks_job() {
    let (document_id, event_id) = ids();
    let status = materialization_failure_status(&job(1), &event(), ("boom".into(), 10), true, 5);
    let parked = FinishedMaterializationJob::Parked {
        job_key: job_key(),
        job: job(1),
        status,
    };
    let mut plan = PlanFinishOperation::new(vec![parked]);
    plan.stamp(5);
    let dead_letter = (
        DEAD_LETTER_KEYSPACE.to_string(),
        dead_letter_key(document_id, event_id),
    );
    let reads = vec![status_read(document_id), dead_letter];
    assert_eq!(plan.first(), Ok(Next::Call(IoCall::Read(reads))));
    assert!(is_child(&plan.next(IoResult::Values(vec![None, None]))));
    let queued = child(Ok::<Vec<(String, Key, Value)>, LinkError>(Vec::new()));
    let Ok(Next::Done(rows)) = plan.next(queued) else {
        panic!("expected the finish plan");
    };
    let parks = (rows.writes.iter()).any(|(space, _, _)| space == DEAD_LETTER_KEYSPACE);
    assert!(parks);
    assert!(rows.deletes.contains(&global_row(job_key())));
}

#[test]
fn commit_writes_first() {
    let row = (
        MATERIALIZATION_STATUS_KEYSPACE.to_string(),
        Key::from(vec![1]),
        Value::from(vec![2]),
    );
    let delete = global_row(job_key());
    let plan = FinishPlan {
        writes: vec![row.clone()],
        deletes: vec![delete.clone()],
    };
    let mut commit = CommitFinishOperation::new(plan);
    assert_eq!(commit.transaction(), TransactionMode::Write);
    assert_eq!(commit.first(), Ok(Next::Call(IoCall::Write(vec![row]))));
    let deleted = commit.next(IoResult::Done);
    assert_eq!(deleted, Ok(Next::Call(IoCall::Delete(vec![delete]))));
    assert_eq!(commit.next(IoResult::Done), Ok(Next::Done(())));
}

fn drain_result(turn: MaterializationTurn<MetadataDrainResult>) -> MetadataDrainResult {
    match turn {
        Ok(Next::Done(result)) => result,
        other => panic!("expected the drain result, got {other:?}"),
    }
}

#[test]
fn batch_empty_queue() {
    let mut batch = MaterializationBatchOperation::new(GitNode::default(), 2, true);
    batch.stamp(100);
    assert!(is_child(&batch.first()));
    let scanned: DueJobs = (Vec::new(), false, Some(400));
    let result = drain_result(batch.next(child(Ok::<_, Error>(scanned))));
    assert_eq!(result.processed, 0);
    assert!(!result.has_more_due);
    assert_eq!(result.next_due_after, Some(Duration::from_millis(300)));
}

fn group_answer(outcome: MaterializationGroupOutcome) -> IoResult {
    let outcome = Event::Child(ChildOutcome::new(Ok::<_, Error>(outcome)));
    IoResult::Group(GroupEvent::Finished {
        results: vec![GroupResult::Done(Box::new(outcome))],
    })
}

#[test]
fn batch_syncs_graphs() {
    let graph_iri = event().record.graph_iri;
    let mut batch = MaterializationBatchOperation::new(GitNode::default(), 2, true);
    batch.stamp(100);
    batch.first().unwrap();
    let scanned: DueJobs = (vec![(job_key(), job(1))], false, None);
    let groups = batch.next(child(Ok::<_, Error>(scanned)));
    let Ok(Next::Call(IoCall::Group(group))) = groups else {
        panic!("expected the job groups, got {groups:?}");
    };
    assert_eq!((group.members.len(), group.limit), (1, 2));
    let mut completed = CompletedMaterializationJob::without_rows(job_key(), sidecar_key(), None);
    completed.sync = Some(CompletedMaterializationSync {
        graph_iri: graph_iri.clone(),
        peers: vec![node(2)],
    });
    let outcome = MaterializationGroupOutcome {
        finished: vec![FinishedMaterializationJob::Completed(completed)],
        processed: 1,
        ..MaterializationGroupOutcome::default()
    };
    assert!(is_child(&batch.next(group_answer(outcome))));
    let plan = FinishPlan {
        writes: Vec::new(),
        deletes: vec![global_row(job_key())],
    };
    assert!(is_child(&batch.next(child(Ok::<_, Error>(plan)))));
    let sync = MetadataEffect::SyncBestEffort {
        graph_iri: graph_iri.clone(),
        peers: vec![node(2)],
    };
    let call = IoCall::Effect(Box::new(Effect::Metadata(sync)));
    same_call(batch.next(child(Ok::<(), Error>(()))), Ok(Next::Call(call)));
    let scheduled = MetadataEvent::GraphSyncScheduled {
        graph_iri,
        peers: vec![node(2)],
    };
    let timer = batch.next(IoResult::Event(Box::new(Event::Metadata(scheduled))));
    assert!(is_child(&timer));
    let result = drain_result(batch.next(child(Ok::<(), LinkError>(()))));
    assert_eq!(result.processed, 1);
    assert_eq!(result.next_due_after, None);
}

#[test]
fn batch_reports_failure() {
    let mut batch = MaterializationBatchOperation::new(GitNode::default(), 2, true);
    batch.first().unwrap();
    let scanned: DueJobs = (vec![(job_key(), job(1))], false, None);
    batch.next(child(Ok::<_, Error>(scanned))).unwrap();
    let failure = || Error::Storage(StorageError::Timeout);
    let outcome = MaterializationGroupOutcome {
        error: Some(failure()),
        ..MaterializationGroupOutcome::default()
    };
    let failed = batch.next(group_answer(outcome));
    assert_eq!(failed.map(|_| ()), Err(failure()));
}

fn dead_letter(requeue_at_ms: u64) -> DeadLetterRecord {
    DeadLetterRecord {
        job: job(1),
        last_error: "boom".to_string(),
        parked_at_ms: 1,
        parks: 1,
        requeue_at_ms,
    }
}

fn swept(requeue_at_ms: u64) -> (RequeueParkedOperation, MaterializationTurn<usize>) {
    let mut sweep = RequeueParkedOperation::new();
    sweep.stamp(10);
    same_call(sweep.first(), page(DEAD_LETTER_KEYSPACE, REQUEUE_PAGE_SIZE));
    let (_, key, value) = dead_letter_entry(&dead_letter(requeue_at_ms)).unwrap();
    let turn = sweep.next(IoResult::Page {
        rows: vec![(key, value)],
        next: None,
    });
    (sweep, turn)
}

#[test]
fn sweep_requeues_due() {
    let (mut sweep, requeue) = swept(1);
    assert!(is_child(&requeue));
    let done = sweep.next(child(Ok::<bool, Error>(true)));
    assert_eq!(done, Ok(Next::Done(1)));
}

#[test]
fn sweep_skips_contended() {
    let (mut sweep, _) = swept(1);
    let conflict = Error::Storage(StorageError::TransactionConflict);
    let done = sweep.next(child(Err::<bool, _>(conflict)));
    assert_eq!(done, Ok(Next::Done(0)));
}

#[test]
fn sweep_waits_future() {
    let (_, done) = swept(50);
    assert_eq!(done, Ok(Next::Done(0)));
}

#[test]
fn sweep_drops_malformed() {
    let mut sweep = RequeueParkedOperation::new();
    sweep.first().unwrap();
    let key = Key::from(vec![7]);
    let rows = vec![(key.clone(), Value::from(vec![1, 2, 3]))];
    let deleted = sweep.next(IoResult::Page { rows, next: None });
    let delete = vec![(DEAD_LETTER_KEYSPACE.to_string(), key)];
    assert_eq!(deleted, Ok(Next::Call(IoCall::Delete(delete))));
    assert_eq!(sweep.next(IoResult::Done), Ok(Next::Done(0)));
}

#[test]
fn requeue_drops_orphan() {
    let (document_id, event_id) = ids();
    let mut requeue = RequeueJobOperation::new(dead_letter(1));
    requeue.first().unwrap();
    let dropped = requeue.next(IoResult::Values(vec![None, None]));
    let delete = vec![(
        DEAD_LETTER_KEYSPACE.to_string(),
        dead_letter_key(document_id, event_id),
    )];
    assert_eq!(dropped, Ok(Next::Call(IoCall::Delete(delete))));
    assert_eq!(requeue.next(IoResult::Done), Ok(Next::Done(false)));
}

#[test]
fn exists_finds_live() {
    let (document_id, event_id) = ids();
    let mut probe = JobsExistOperation::new();
    same_call(probe.first(), page(MATERIALIZATION_JOB_KEYSPACE, 1));
    let rows = vec![(Key::from(job_key()), Value::from(Vec::new()))];
    let sidecar = probe.next(IoResult::Page { rows, next: None });
    let read = vec![sidecar_read(document_id, event_id)];
    assert_eq!(sidecar, Ok(Next::Call(IoCall::Read(read))));
    let live = probe.next(IoResult::Values(vec![Some(sidecar_row(&job(1)))]));
    let reads = vec![event_read(document_id, event_id), status_read(document_id)];
    assert_eq!(live, Ok(Next::Call(IoCall::Read(reads))));
    let found = probe.next(IoResult::Values(vec![Some(event_row()), None]));
    assert_eq!(found, Ok(Next::Done(true)));
}

#[test]
fn exists_drops_malformed() {
    let mut probe = JobsExistOperation::new();
    probe.first().unwrap();
    let rows = vec![(Key::from(vec![0]), Value::from(Vec::new()))];
    let removed = probe.next(IoResult::Page { rows, next: None });
    let delete = vec![global_row(vec![0])];
    assert_eq!(removed, Ok(Next::Call(IoCall::Delete(delete))));
    assert_eq!(probe.next(IoResult::Done), Ok(Next::Done(false)));
}
