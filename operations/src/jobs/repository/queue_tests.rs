//! Checks the calls and results of the link queue, its drain and the checks it runs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::driver::drive;
use crate::jobs::repository::test_support::{
    NOW_MS, child, job_id, link, link_bytes, local_scope, realm, registry_record, scope,
};
use aruna_core::effects::StorageEffect;
use aruna_core::events::StorageEvent;
use aruna_core::repository::{LinkFailure, LinkPush};
use aruna_core::rows::read_row;
use aruna_storage::FjallStorage;

fn entry(due_at_ms: u64) -> LinkQueueEntry {
    LinkQueueEntry {
        document_id: link().document_id,
        due_at_ms,
        first_at_ms: due_at_ms,
        settle_only: false,
    }
}

fn queued(due_at_ms: u64) -> Value {
    ByteView::from(postcard::to_allocvec(&entry(due_at_ms)).unwrap())
}

fn no_record() -> IoResult {
    crate::jobs::repository::test_support::event(Event::Storage(StorageEvent::ReadResult {
        key: ByteView::from(Vec::new()),
        value: None,
    }))
}

fn registry_answer() -> IoResult {
    crate::jobs::repository::test_support::event(Event::Storage(StorageEvent::ReadResult {
        key: ByteView::from(Vec::new()),
        value: Some(ByteView::from(
            postcard::to_allocvec(&registry_record()).unwrap(),
        )),
    }))
}

fn check() -> CheckLinkOperation {
    let mut operation = CheckLinkOperation::new(local_scope(), link().link_id, entry(1), queued(1));
    operation.stamp(NOW_MS);
    operation
}

fn drop_call() -> IoCall {
    settle_call(link().link_id, &queued(1), None)
}

/// Runs a check up to the answer of its holder step.
fn checked_holder(stored: &RepositoryLink) -> CheckLinkOperation {
    let mut operation = check();
    operation.first().unwrap();
    operation
        .next(IoResult::Values(vec![Some(link_bytes(stored))]))
        .unwrap();
    operation.next(registry_answer()).unwrap();
    operation
}

#[test]
fn deleted_document_queues() {
    let mut operation = QueueRowsOperation::new([link().document_id]);
    operation.stamp(NOW_MS);
    assert_eq!(
        operation.first(),
        Ok(Next::Call(IoCall::child(DocumentLinksOperation::new(
            link().document_id
        ))))
    );
    let mut paused = link();
    paused.status = LinkStatus::Paused;
    let call = operation
        .next(child::<_, LinkError>(Ok(vec![paused])))
        .unwrap();
    assert_eq!(call, Next::Call(registry_read(link().document_id)));
    let read = operation.next(no_record()).unwrap();
    let row = (LINK_QUEUE_KEYSPACE.to_string(), id_key(link().link_id));
    assert_eq!(read, Next::Call(IoCall::Read(vec![row])));
    let Ok(Next::Done(rows)) = operation.next(IoResult::Values(vec![None])) else {
        panic!("the rows are ready");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1, id_key(link().link_id));
}

#[test]
fn linkless_document_skipped() {
    let mut operation = QueueRowsOperation::new([link().document_id]);
    operation.first().unwrap();
    let answer = child::<Vec<RepositoryLink>, LinkError>(Ok(Vec::new()));
    assert_eq!(operation.next(answer), Ok(Next::Done(Vec::new())));
}

fn queue_page(rows: Vec<(Key, Value)>) -> IoResult {
    IoResult::Page { rows, next: None }
}

fn drain() -> DrainLinksOperation {
    let mut operation = DrainLinksOperation::new(local_scope());
    operation.stamp(NOW_MS);
    operation.first().unwrap();
    operation
}

#[test]
fn drain_removes_malformed() {
    let mut operation = drain();
    let key = ByteView::from(vec![1, 2]);
    let row = (LINK_QUEUE_KEYSPACE.to_string(), key.clone());
    let answer = queue_page(vec![(key, ByteView::from(vec![0]))]);
    assert_eq!(
        operation.next(answer),
        Ok(Next::Call(IoCall::Delete(vec![row])))
    );
    assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(None)));
}

#[test]
fn drain_waits_later() {
    let mut operation = drain();
    let due = NOW_MS + 5_000;
    let answer = queue_page(vec![(id_key(link().link_id), queued(due))]);
    assert_eq!(
        operation.next(answer),
        Ok(Next::Done(Some(Duration::from_millis(5_000))))
    );
}

#[test]
fn drain_moves_failure() {
    let mut operation = drain();
    let value = queued(1);
    let answer = queue_page(vec![(id_key(link().link_id), value.clone())]);
    let expected = CheckLinkOperation::new(local_scope(), link().link_id, entry(1), value.clone());
    assert_eq!(
        operation.next(answer),
        Ok(Next::Call(IoCall::child(expected)))
    );
    let retry_at = NOW_MS + ERROR_RETRY_MS;
    let failed = child::<Option<u64>, LinkError>(Err(LinkError::NoRevision));
    assert_eq!(
        operation.next(failed),
        Ok(Next::Call(settle_call(
            link().link_id,
            &value,
            Some(retry_at)
        )))
    );
    let conflict =
        child::<(), LinkError>(Err(LinkError::Storage(StorageError::TransactionConflict)));
    assert_eq!(
        operation.next(conflict),
        Ok(Next::Done(Some(Duration::from_millis(ERROR_RETRY_MS))))
    );
}

#[test]
fn missing_link_dropped() {
    let mut operation = check();
    let row = link_row(link().document_id, link().link_id);
    assert_eq!(operation.first(), Ok(Next::Call(IoCall::Read(vec![row]))));
    assert_eq!(
        operation.next(IoResult::Values(vec![None])),
        Ok(Next::Call(drop_call()))
    );
    assert_eq!(
        operation.next(child::<(), LinkError>(Ok(()))),
        Ok(Next::Done(None))
    );
}

#[test]
fn deleted_dataset_unlinks() {
    let mut operation = check();
    operation.first().unwrap();
    operation
        .next(IoResult::Values(vec![Some(link_bytes(&link()))]))
        .unwrap();
    assert_eq!(
        operation.next(no_record()),
        Ok(Next::Call(IoCall::child(ApplyChangeOperation::new(
            local_scope(),
            link(),
            LinkChange::Delete
        ))))
    );
    let deleted = child::<Option<RepositoryLink>, LinkError>(Ok(None));
    assert_eq!(operation.next(deleted), Ok(Next::Done(None)));
}

#[test]
fn lost_holder_dropped() {
    let mut operation = checked_holder(&link());
    let refused = child::<(), LinkError>(Err(LinkError::NotHolder));
    assert_eq!(operation.next(refused), Ok(Next::Call(drop_call())));
}

#[test]
fn running_push_waits() {
    let mut running = link();
    running.active_job = Some(job_id(4));
    let mut operation = checked_holder(&running);
    assert_eq!(
        operation.next(child::<(), LinkError>(Ok(()))),
        Ok(Next::Call(IoCall::child(ReadJobOperation::new(job_id(4)))))
    );
    let mut record = queued_job(job_id(4));
    record.state = JobState::Running;
    let answer = child::<_, aruna_core::calls::IoError>(Ok(Some(record)));
    assert_eq!(
        operation.next(answer),
        Ok(Next::Done(Some(NOW_MS + ACTIVE_RETRY_MS)))
    );
}

#[test]
fn unchanged_revision_dropped() {
    let event_id = Ulid::from_bytes([8; 16]);
    let mut stored = link();
    stored.last_push = Some(LinkPush {
        event_id,
        dataset_digest: None,
        job_id: job_id(2),
        pushed_at: std::time::SystemTime::UNIX_EPOCH,
    });
    let mut operation = checked_holder(&stored);
    let review = RefreshReviewOperation::new(local_scope(), stored.clone());
    assert_eq!(
        operation.next(child::<(), LinkError>(Ok(()))),
        Ok(Next::Call(IoCall::child(review)))
    );
    assert_eq!(
        operation.next(child::<_, LinkError>(Ok(stored.clone()))),
        Ok(Next::Call(IoCall::child(PushRevisionOperation::new(
            stored.document_id
        ))))
    );
    let revision = child::<Option<Revision>, LinkError>(Ok(Some((event_id, None))));
    assert_eq!(operation.next(revision), Ok(Next::Call(drop_call())));
}

#[test]
fn limited_push_retries() {
    let stored = link();
    let mut operation = checked_holder(&stored);
    operation.next(child::<(), LinkError>(Ok(()))).unwrap();
    operation
        .next(child::<_, LinkError>(Ok(stored.clone())))
        .unwrap();
    let event_id = Ulid::from_bytes([8; 16]);
    let revision = child::<Option<Revision>, LinkError>(Ok(Some((event_id, None))));
    assert_eq!(
        operation.next(revision),
        Ok(Next::Call(IoCall::child(StartPushOperation::new(
            local_scope(),
            stored,
            event_id,
            false
        ))))
    );
    let limited = child::<JobId, LinkError>(Err(LinkError::JobLimit(2)));
    assert_eq!(
        operation.next(limited),
        Ok(Next::Done(Some(NOW_MS + ACTIVE_RETRY_MS)))
    );
}

fn queued_job(job_id: JobId) -> JobRecord {
    JobRecord::new(
        job_id,
        aruna_core::structs::execution::job::JobPayload::Probe {
            steps: 1,
            step_sleep_ms: 0,
            fail_at: None,
            panic_at: None,
            cleanup_marker: None,
        },
        link().created_by,
        link().owner_node,
        NOW_MS,
        NOW_MS,
        None,
    )
}

fn stale(record: Option<JobRecord>) -> SettleStaleOperation {
    let mut operation =
        SettleStaleOperation::new(local_scope(), link(), job_id(4), record, queued(1));
    operation.stamp(NOW_MS);
    operation
}

#[test]
fn missing_job_fails() {
    let mut operation = stale(None);
    assert_eq!(
        operation.first(),
        Ok(Next::Call(read_checkpoint(job_id(4))))
    );
    let change = LinkChange::Finish {
        job_id: job_id(4),
        outcome: Box::new(PushOutcome::Failed(LinkFailure::Other(
            "push job missing".into(),
        ))),
        requeue: false,
    };
    assert_eq!(
        operation.next(IoResult::Values(vec![None])),
        Ok(Next::Call(IoCall::child(ApplyChangeOperation::new(
            local_scope(),
            link(),
            change
        ))))
    );
    let stored = child::<Option<RepositoryLink>, LinkError>(Ok(None));
    assert_eq!(operation.next(stored), Ok(Next::Done(Some(NOW_MS))));
}

#[test]
fn settled_review_kept() {
    let mut operation = RefreshReviewOperation::new(local_scope(), link());
    assert_eq!(operation.first(), Ok(Next::Done(link())));
}

fn in_review() -> RepositoryLink {
    let mut stored = link();
    stored.remote.review = LinkReview::Pending;
    stored
}

#[test]
fn open_review_kept() {
    let mut operation = RefreshReviewOperation::new(local_scope(), in_review());
    assert_eq!(
        operation.first(),
        Ok(Next::Call(IoCall::child(ReviewStateOperation::new(
            local_scope(),
            in_review()
        ))))
    );
    let open = child::<Option<RemoteState>, TransferError>(Ok(None));
    assert_eq!(operation.next(open), Ok(Next::Done(in_review())));
}

#[test]
fn refused_review_fails() {
    let mut operation = RefreshReviewOperation::new(local_scope(), in_review());
    operation.first().unwrap();
    let refused = child::<Option<RemoteState>, TransferError>(Err(TransferError::Refused(
        LinkFailure::RemoteChanged,
    )));
    assert_eq!(
        operation.next(refused),
        Ok(Next::Call(IoCall::child(ApplyChangeOperation::new(
            local_scope(),
            in_review(),
            LinkChange::Fail(LinkFailure::RemoteChanged)
        ))))
    );
    let gone = child::<Option<RepositoryLink>, LinkError>(Ok(None));
    assert_eq!(operation.next(gone), Err(LinkError::NotFound));
}

#[test]
fn push_submits_begins() {
    let event_id = Ulid::from_bytes([8; 16]);
    let mut operation = StartPushOperation::new(local_scope(), link(), event_id, true);
    assert_eq!(
        operation.first(),
        Ok(Next::Call(IoCall::child(EnsureHolderOperation::new(
            local_scope(),
            link()
        ))))
    );
    let submit = operation.next(child::<(), LinkError>(Ok(()))).unwrap();
    assert!(matches!(submit, Next::Call(IoCall::Child(_))));
    let submitted = SubmitJobResult {
        job_id: job_id(4),
        created: true,
    };
    assert_eq!(
        operation.next(child::<_, SubmitJobError>(Ok(submitted))),
        Ok(Next::Call(IoCall::child(ApplyChangeOperation::new(
            local_scope(),
            link(),
            LinkChange::Begin(job_id(4))
        ))))
    );
    let begun = child::<Option<RepositoryLink>, LinkError>(Ok(Some(link())));
    assert_eq!(operation.next(begun), Ok(Next::Done(job_id(4))));
}

#[test]
fn failed_link_lineage() {
    let mut failed = link();
    failed.status = LinkStatus::Failed {
        reason: LinkFailure::RemoteChanged,
    };
    let mut operation = StartPushOperation::new(local_scope(), failed.clone(), Ulid::nil(), false);
    operation.first().unwrap();
    assert_eq!(
        operation.next(child::<(), LinkError>(Ok(()))),
        Ok(Next::Call(IoCall::child(EnsureLineageOperation::new(
            failed
        ))))
    );
}

#[test]
fn job_limit_separate() {
    assert_eq!(
        submit_failure(SubmitJobError::ActiveJobLimit { limit: 3 }),
        LinkError::JobLimit(3)
    );
}

#[test]
fn unknown_placement_holds() {
    let mut operation = OwnerHoldsOperation::new(scope(), &link());
    assert_eq!(
        operation.first(),
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(realm()))))
    );
    let missing =
        child::<RealmConfigDocument, GetConfigError>(Err(GetConfigError::DocumentNotFound));
    assert_eq!(operation.next(missing), Ok(Next::Done(true)));
}

#[test]
fn lost_dataset_fails() {
    let mut operation = EnsureHolderOperation::new(local_scope(), link());
    operation.stamp(NOW_MS);
    operation.first().unwrap();
    let expected = ChangeLinkOperation::new(
        link().document_id,
        link().link_id,
        LinkChange::Fail(LinkFailure::OwnerNotHolder),
        time_of(NOW_MS),
    );
    assert_eq!(
        operation.next(child::<_, LinkError>(Ok(false))),
        Ok(Next::Call(IoCall::child(expected)))
    );
    let stored = child::<Option<RepositoryLink>, LinkError>(Ok(None));
    assert_eq!(operation.next(stored), Err(LinkError::NotHolder));
}

#[test]
fn held_dataset_passes() {
    let mut operation = EnsureHolderOperation::new(local_scope(), link());
    operation.first().unwrap();
    assert_eq!(
        operation.next(child::<_, LinkError>(Ok(true))),
        Ok(Next::Done(()))
    );
}

#[test]
fn scaffold_uses_registry() {
    let mut operation = PushRevisionOperation::new(link().document_id);
    operation.first().unwrap();
    let raw = child::<RawViewOutput, RawReadError>(Ok((None, None)));
    assert_eq!(
        operation.next(raw),
        Ok(Next::Call(registry_read(link().document_id)))
    );
    assert_eq!(operation.next(no_record()), Ok(Next::Done(None)));
    assert_eq!(revision_event(None), Err(LinkError::NoRevision));
}

#[test]
fn foreign_iri_ignored() {
    let mut operation = QueueDeletedOperation::new(scope(), "urn:other".into());
    assert_eq!(operation.first(), Ok(Next::Done(())));
}

#[test]
fn deletion_arms_timer() {
    let document = link().document_id;
    let iri = MetadataRegistryRecord::graph_iri_for(document);
    let mut operation = QueueDeletedOperation::new(scope(), iri);
    assert_eq!(
        operation.first(),
        Ok(Next::Call(IoCall::child(QueueRowsOperation::new([
            document
        ]))))
    );
    let row = queue_row(link().link_id, &entry(1)).unwrap();
    let write = operation
        .next(child::<_, LinkError>(Ok(vec![row.clone()])))
        .unwrap();
    assert_eq!(write, Next::Call(IoCall::Write(vec![row])));
    assert_eq!(
        operation.next(IoResult::Done),
        Ok(Next::Call(IoCall::child(LinkTimerOperation::new())))
    );
    assert_eq!(
        operation.next(child::<(), LinkError>(Ok(()))),
        Ok(Next::Done(()))
    );
}

#[test]
fn timer_arms_earliest() {
    let mut operation = LinkTimerOperation::new();
    operation.stamp(NOW_MS);
    operation.first().unwrap();
    let rows = vec![
        (id_key(link().link_id), queued(NOW_MS + 9_000)),
        (id_key(Ulid::nil()), queued(NOW_MS + 2_000)),
    ];
    let expected: IoCall = schedule_drain(Duration::from_millis(2_000)).into();
    assert_eq!(operation.next(queue_page(rows)), Ok(Next::Call(expected)));
    let armed = crate::jobs::repository::test_support::event(Event::Task(TaskEvent::Error {
        key: None,
        message: "late".into(),
    }));
    assert_eq!(operation.next(armed), Ok(Next::Done(())));
}

#[test]
fn empty_queue_idle() {
    let mut operation = LinkTimerOperation::new();
    operation.first().unwrap();
    assert_eq!(operation.next(queue_page(Vec::new())), Ok(Next::Done(())));
}

async fn stored(context: &crate::driver::DriverContext, link_id: Ulid) -> Option<LinkQueueEntry> {
    drive(StoredEntry(link_id), context).await.unwrap()
}

/// Reads one queue row back for the storage test below.
#[derive(Debug, PartialEq)]
struct StoredEntry(Ulid);

impl Operation for StoredEntry {
    type Output = Option<LinkQueueEntry>;
    type Error = LinkError;

    fn first(&mut self) -> Turn<Self::Output> {
        Ok(Next::Call(read_row(LINK_QUEUE_KEYSPACE, id_key(self.0))))
    }

    fn next(&mut self, result: IoResult) -> Turn<Self::Output> {
        let value = aruna_core::rows::value(result)?;
        Ok(Next::Done(
            value.map(|bytes| postcard::from_bytes(&bytes).unwrap()),
        ))
    }
}

async fn write_row(context: &crate::driver::DriverContext, row: (String, Key, Value)) {
    let effect = StorageEffect::Write {
        key_space: row.0,
        key: row.1,
        value: row.2,
        txn_id: None,
    };
    let answer = context.storage_handle.send_storage_effect(effect).await;
    assert!(matches!(
        answer,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
}

#[tokio::test]
async fn retry_keeps_newer() {
    let dir = tempfile::tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let context = crate::testing::test_context(storage.clone());
    let (link_id, document_id) = (Ulid::from_parts(1, 1), Ulid::from_parts(1, 2));
    let entry = |due_at_ms, first_at_ms| LinkQueueEntry {
        document_id,
        due_at_ms,
        first_at_ms,
        settle_only: false,
    };
    let settle =
        |queued: &Value, retry_at| SettleCheckOperation::new(link_id, queued.clone(), retry_at);
    let queued = queue_row(link_id, &entry(100, 90)).unwrap();
    write_row(&context, queued.clone()).await;

    drive(settle(&queued.2, Some(500)), &context).await.unwrap();
    assert_eq!(stored(&context, link_id).await, Some(entry(500, 90)));

    // A change queued while the check ran keeps its earlier due time.
    let newer = queue_row(link_id, &entry(200, 150)).unwrap();
    write_row(&context, newer).await;
    drive(settle(&queued.2, Some(500)), &context).await.unwrap();
    assert_eq!(stored(&context, link_id).await, Some(entry(200, 150)));
    drive(settle(&queued.2, None), &context).await.unwrap();
    assert_eq!(stored(&context, link_id).await, Some(entry(200, 150)));

    // A check removed meanwhile, for example by a started push, stays removed.
    let delete = StorageEffect::Delete {
        key_space: LINK_QUEUE_KEYSPACE.to_string(),
        key: id_key(link_id),
        txn_id: None,
    };
    context.storage_handle.send_storage_effect(delete).await;
    drive(settle(&queued.2, Some(500)), &context).await.unwrap();
    assert_eq!(stored(&context, link_id).await, None);
}
