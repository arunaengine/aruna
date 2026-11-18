//! Tests which graph a refresh may snapshot while a materialization job waits or captures.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::super::testing::{
    self, answered, asks_child, asks_store, auth, child, dataset_of, git_node, node, projection,
    ran, record,
};
use super::*;
use crate::driver::{DriverContext, drive};
use aruna_core::effects::StorageEffect;
use aruna_core::events::StorageEvent;
use aruna_core::git::{GitAnswer, GitChange, GitRepository};
use aruna_core::metadata::{
    MetadataEventPayload, MetadataEventRecord, MetadataMaterializationRecord,
};
use aruna_core::storage_entries::{
    create_event_entry, document_job_entry, document_job_key, materialization_status_entry,
};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_storage::FjallStorage;

const OLDER: Ulid = Ulid::from_parts(1, 10);

async fn read_current(
    context: &DriverContext,
    document: &MetadataRegistryRecord,
    revision: Option<&MetadataRawRevision>,
    materializing: bool,
) -> Result<Source, GitError> {
    drive(super::current(document, revision, materializing), context).await
}

/// The stored crate event "older" and the registry row of the newer event `last`.
fn events(last: Ulid) -> (MetadataEventRecord, MetadataRegistryRecord) {
    let realm_id = RealmId::from_bytes([1; 32]);
    let group_id = Ulid::from_parts(1, 1);
    let document_id = Ulid::from_parts(1, 2);
    let node_id = iroh::SecretKey::from_bytes(&[2; 32]).public();
    let record = MetadataRegistryRecord {
        realm_id,
        group_id,
        document_id,
        document_path: "datasets/raw".to_string(),
        graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
        public: true,
        permission_path: MetadataRegistryRecord::permission_path_for(
            &realm_id,
            group_id,
            "datasets/raw",
            document_id,
        ),
        placement: PlacementRef::NIL,
        holder_node_ids: vec![node_id],
        created_at_ms: 1,
        updated_at_ms: 1,
        establishing_event_id: OLDER,
        last_event_id: OLDER,
    };
    let jsonld = serde_json::json!({
        "@context": "https://w3id.org/ro/crate/1.2/context",
        "@graph": [{"@id": "./", "@type": "Dataset", "name": "older"}]
    });
    let event = MetadataEventRecord {
        event_id: OLDER,
        record: record.clone(),
        user_id: aruna_core::UserId::local(Ulid::from_parts(1, 3), realm_id),
        node_id,
        payload: MetadataEventPayload::RoCrate {
            jsonld: jsonld.to_string(),
        },
        occurred_at_ms: 1,
    };
    let document = MetadataRegistryRecord {
        last_event_id: last,
        ..record
    };
    (event, document)
}

fn status(
    event: &MetadataEventRecord,
    id: Ulid,
    state: MaterializationState,
) -> MaterializationStatusRecord {
    let mut status = MaterializationStatusRecord::pending(event, 2);
    status.event_id = id;
    status.state = state;
    status
}

/// Stores the older event, the status and the job row of `queued`, if any, which `captured`
/// marks as a job that published its graph.
async fn stored(
    event: &MetadataEventRecord,
    status: &MaterializationStatusRecord,
    queued: Option<Ulid>,
    captured: bool,
) -> (tempfile::TempDir, DriverContext) {
    let directory = tempfile::tempdir().unwrap();
    let storage = FjallStorage::open(directory.path().to_str().unwrap()).unwrap();
    let mut writes = vec![
        create_event_entry(event).unwrap(),
        materialization_status_entry(status).unwrap(),
    ];
    if let Some(event_id) = queued {
        let job = MetadataMaterializationRecord {
            document_id: event.record.document_id,
            event_id,
            due_at_ms: 1,
            attempts: 0,
            failures: 0,
            parks: 0,
        };
        writes.push(document_job_entry(&job).unwrap());
        if captured {
            let key = document_job_key(job.document_id, event_id);
            let mark = (
                MATERIALIZATION_CAPTURE_KEYSPACE.to_string(),
                key,
                Vec::new().into(),
            );
            writes.push(mark);
        }
    }
    let written = storage
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })
        .await;
    assert!(matches!(
        written,
        Event::Storage(StorageEvent::BatchWriteResult { .. })
    ));
    let context = DriverContext {
        storage_handle: storage,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    };
    (directory, context)
}

// The newer event's job already published its snapshot; the older raw view must not replace it.
#[tokio::test]
async fn waits_for_materialization() {
    let pending = Ulid::from_parts(2, 10);
    let (event, document) = events(pending);
    let status = status(&event, pending, MaterializationState::Pending);
    let (_directory, context) = stored(&event, &status, Some(pending), true).await;

    let refreshed = read_current(&context, &document, None, false)
        .await
        .unwrap();
    assert_eq!(refreshed, None, "a refresh snapshots the older graph");
    let stored = read_current(&context, &document, None, true).await.unwrap();
    assert_eq!(stored.map(|(id, _)| id), Some(OLDER));
    let supplied = MetadataRawRevision {
        jsonld: "{\"name\":\"newer\"}".to_string(),
        winning_event_id: pending,
        context_digest: [0; 32],
        dataset_digest: None,
        merged: None,
    };
    let captured = read_current(&context, &document, Some(&supplied), true).await;
    assert_eq!(
        captured.unwrap(),
        Some((pending, supplied.jsonld.clone())),
        "a capture snapshots the graph its job made"
    );
    let refreshed = read_current(&context, &document, Some(&supplied), false).await;
    assert_eq!(
        refreshed.unwrap(),
        None,
        "a refresh passes the captured job"
    );
}

// A batch waiting for a predecessor on an offline node holds back no completed revision, which
// this holder must still publish, for example after holder failover.
#[tokio::test]
async fn waiting_job_publishes() {
    let waiting = Ulid::from_parts(2, 10);
    let (event, document) = events(OLDER);
    let status = status(&event, OLDER, MaterializationState::Materialized);
    let (_directory, context) = stored(&event, &status, Some(waiting), false).await;

    let refreshed = read_current(&context, &document, None, false)
        .await
        .unwrap();
    assert_eq!(refreshed.map(|(id, _)| id), Some(OLDER));
}

// A late merge leaves the newer status materialized; its job still holds refreshes back.
#[tokio::test]
async fn waits_for_merge() {
    let newer = Ulid::from_parts(3, 10);
    let late = Ulid::from_parts(2, 10);
    let (event, document) = events(newer);
    let status = status(&event, newer, MaterializationState::Materialized);
    let (_directory, context) = stored(&event, &status, Some(late), true).await;

    let refreshed = read_current(&context, &document, None, false)
        .await
        .unwrap();
    assert_eq!(
        refreshed, None,
        "a refresh snapshots the graph before the merge"
    );

    // Completion removes the mark in the batch that stores the merged graph.
    let removed = context
        .storage_handle
        .send_storage_effect(StorageEffect::Delete {
            key_space: MATERIALIZATION_CAPTURE_KEYSPACE.to_string(),
            key: document_job_key(document.document_id, late),
            txn_id: None,
        })
        .await;
    assert!(matches!(
        removed,
        Event::Storage(StorageEvent::DeleteResult { .. })
    ));
    let finished = read_current(&context, &document, None, false)
        .await
        .unwrap();
    assert_eq!(finished.map(|(id, _)| id), Some(OLDER));
}

#[tokio::test]
async fn snapshots_materialized_graph() {
    let (event, document) = events(OLDER);
    let status = status(&event, OLDER, MaterializationState::Materialized);
    let (_directory, context) = stored(&event, &status, None, false).await;

    let refreshed = read_current(&context, &document, None, false)
        .await
        .unwrap();
    let (id, jsonld) = refreshed.expect("a materialized graph is ready");
    assert_eq!(id, OLDER);
    assert!(jsonld.contains("older"));
}

fn no_rows() -> IoResult {
    child(Ok::<PendingRows, GitError>(Vec::new()))
}

fn one_row() -> IoResult {
    let merge = PendingMerge {
        user_id: auth().user_id,
        old: String::new(),
        new: "a".to_string(),
    };
    child(Ok::<PendingRows, GitError>(vec![(vec![1], merge)]))
}

fn projected(value: Projection) -> IoResult {
    child(Ok::<_, GitError>(value))
}

fn unknown() -> IoResult {
    child(Ok::<Option<Projection>, GitError>(None))
}

/// An update stepped to the answer of its graph read.
fn updating(known: Projection) -> UpdateOperation {
    let mut run = update(&git_node(), &dataset_of(40), None, false);
    assert!(asks_child(&run.first()));
    assert!(asks_child(&run.next(projected(known))));
    assert!(asks_child(&run.next(child(Ok::<(), GitError>(())))));
    run
}

// Without a ready graph an update ends with what it projected.
#[test]
fn update_without_graph() {
    let known = projection(&[]);
    let answer = updating(known.clone()).next(child(Ok::<Source, GitError>(None)));
    assert_eq!(answer, Ok(Next::Done(known)));
}

// Pushed metadata that cannot apply yet does not fail the update.
#[test]
fn waiting_metadata_tolerated() {
    let mut run = update(&git_node(), &dataset_of(41), None, false);
    run.first().unwrap();
    run.next(projected(projection(&[]))).unwrap();
    let failed = child(Err::<(), _>(GitError::Unavailable));
    assert!(asks_child(&run.next(failed)));
}

// A due checkpoint that cannot be written never fails the request.
#[test]
fn checkpoint_failure_tolerated() {
    let mut known = projection(&[]);
    let unlock = |seed| record(seed, GitChange::Unlock { id: Ulid::from(9) });
    let many: Vec<_> = (1..=CHECKPOINT_AFTER as u128 + 1).map(unlock).collect();
    known.records = std::sync::Arc::new(many);
    let mut run = updating(known.clone());
    assert!(asks_child(&run.next(child(Ok::<Source, GitError>(None)))));
    assert_eq!(run.step, UpdateStep::Checkpoint);
    let failed = child(Err::<GitRecord, _>(GitError::Unavailable));
    assert_eq!(run.next(failed), Ok(Next::Done(known)));
}

// Another holder leaves a fresh revision to the first holder.
#[test]
fn follower_waits() {
    let mut known = projection(&[]);
    known.holders = vec![node(2), node(1)];
    let graph = serde_json::json!({"@context": {}, "@graph": []}).to_string();
    let source = Some((OLDER, graph));
    let answer = updating(known.clone()).next(child(Ok::<Source, GitError>(source)));
    assert_eq!(answer, Ok(Next::Done(known)));
}

#[test]
fn overdue_after_failover() {
    let document = dataset_of(42);
    assert!(!overdue(document.updated_at_ms + FAILOVER_MS, &document));
    assert!(overdue(document.updated_at_ms + FAILOVER_MS + 1, &document));
}

// An unchanged repository is answered from the last refresh.
#[test]
fn refresh_reuses_recent() {
    let known = projection(&[]);
    let mut run = refresh(&git_node(), &dataset_of(43));
    assert!(asks_child(&run.first()));
    let answer = run.next(child(Ok::<_, GitError>(Some(known.clone()))));
    assert_eq!(answer, Ok(Next::Done(known)));
}

// A refresh is remembered only when no pushed metadata still waits.
#[test]
fn refresh_remembers_settled() {
    let refreshing = |document: &Document, rows| {
        let mut run = refresh(&git_node(), document);
        run.first().unwrap();
        assert!(asks_child(&run.next(unknown())));
        assert!(asks_child(&run.next(projected(projection(&[])))));
        run.next(rows).unwrap();
    };
    let settled = dataset_of(44);
    refreshing(&settled, no_rows());
    assert!(last(settled.document_id).is_some());
    let waiting = dataset_of(45);
    refreshing(&waiting, one_row());
    assert!(last(waiting.document_id).is_none());
}

// A held refresh keeps the repository and only logs a failure.
#[test]
fn held_refresh_logs() {
    let document = dataset_of(46);
    let mut run = held_refresh(&git_node(), &document);
    let scope = HoldScope::GitRepository(document.document_id);
    assert_eq!(run.hold_scope(), Some(scope));
    assert!(asks_child(&run.first()));
    let failed = child(Err::<Projection, _>(GitError::Unavailable));
    assert_eq!(run.next(failed), Ok(Next::Done(())));
}

/// A view stepped to the answer of its pending merges.
fn viewing(document: &Document, known: Projection) -> ViewOperation {
    let mut run = view(&git_node(), document);
    run.first().unwrap();
    assert!(asks_child(&run.next(unknown())));
    assert!(asks_child(&run.next(projected(known))));
    run
}

fn current_projection(document: &Document) -> Projection {
    let mut known = projection(&[]);
    known.state.revision = Some(document.last_event_id);
    known
}

// A current repository is served as it is and remembered.
#[test]
fn view_serves_current() {
    let document = dataset_of(47);
    let known = current_projection(&document);
    let answer = viewing(&document, known.clone()).next(no_rows());
    assert_eq!(answer, Ok(Next::Done((known, false))));
    assert!(last(document.document_id).is_some());
}

// Waiting work starts one refresh beside the read, which is answered as pending.
#[test]
fn view_starts_refresh() {
    let document = dataset_of(48);
    let known = current_projection(&document);
    let mut run = viewing(&document, known.clone());
    let Ok(Next::Call(IoCall::Effect(effect))) = run.next(one_row()) else {
        panic!("expected the background start");
    };
    assert!(matches!(*effect, Effect::Background(_)));
    assert_eq!(run.next(IoResult::Done), Ok(Next::Done((known, true))));
}

// A refresh already running is not started again.
#[test]
fn running_refresh_kept() {
    let document = dataset_of(49);
    let _running = RefreshClaim::take(document.document_id).unwrap();
    let known = current_projection(&document);
    let answer = viewing(&document, known.clone()).next(one_row());
    assert_eq!(answer, Ok(Next::Done((known, true))));
}

// While another run holds the repository, the last projection is served as pending.
#[test]
fn busy_serves_last() {
    let document = dataset_of(50);
    let known = projection(&[]);
    remember(Some(node(1)), &document, false, &known);
    let mut run = read_view(&git_node(), &document);
    assert!(asks_store(&run.first()));
    let busy = answered(GitAnswer::Busy(true));
    assert_eq!(run.next(busy), Ok(Next::Done((known, true))));
}

// A free repository, or one never projected here, is viewed under its hold.
#[test]
fn idle_read_views() {
    let mut free = read_view(&git_node(), &dataset_of(51));
    free.first().unwrap();
    assert!(asks_child(&free.next(answered(GitAnswer::Busy(false)))));
    let mut first_time = read_view(&git_node(), &dataset_of(52));
    first_time.first().unwrap();
    assert!(asks_child(
        &first_time.next(answered(GitAnswer::Busy(true)))
    ));
    let viewed = (projection(&[]), false);
    let answer = first_time.next(child(Ok::<_, GitError>(viewed.clone())));
    assert_eq!(answer, Ok(Next::Done(viewed)));
}

// A node without a Git store has nothing to capture.
#[test]
fn capture_needs_store() {
    let mut run = capture(&git_node(), &dataset_of(53), None);
    assert!(asks_store(&run.first()));
    let none = answered(GitAnswer::Failed("no store".to_string()));
    assert_eq!(run.next(none), Ok(Next::Done(())));
}

// A node that does not hold the document is done; other failures defer the job.
#[test]
fn capture_outcomes() {
    let finish = |error: GitError| {
        let mut run = capture(&git_node(), &dataset_of(54), None);
        run.first().unwrap();
        assert!(asks_child(&run.next(answered(GitAnswer::Configured(true)))));
        run.next(child(Err::<Projection, _>(error)))
    };
    assert_eq!(finish(GitError::NotHolder), Ok(Next::Done(())));
    assert_eq!(finish(GitError::Unavailable), Err(GitError::Unavailable));
}

fn opened(document: &Document) -> IoResult {
    let repository = GitRepository {
        document_id: document.document_id,
        group_id: document.group_id,
        bucket: "bucket".to_string(),
    };
    child(Ok::<Repository, GitError>((document.clone(), repository)))
}

// A busy repository never projected here reports an empty pending state.
#[test]
fn busy_status_pending() {
    let document = dataset_of(55);
    let mut run = super::status(&git_node(), &auth(), document.document_id);
    assert!(asks_child(&run.first()));
    assert!(asks_store(&run.next(opened(&document))));
    assert!(asks_child(&run.next(answered(GitAnswer::Busy(true)))));
    let answer = run.next(child(Ok::<Option<GitStatus>, GitError>(None)));
    let Ok(Next::Done((None, seen, None, _, true))) = answer else {
        panic!("expected the pending status");
    };
    assert!(seen.state.refs.is_empty());
}

// The layout of main is part of the status.
#[test]
fn status_reads_layout() {
    let document = dataset_of(56);
    let mut run = super::status(&git_node(), &auth(), document.document_id);
    run.first().unwrap();
    run.next(opened(&document)).unwrap();
    assert!(asks_child(&run.next(answered(GitAnswer::Busy(false)))));
    let viewed = (projection(&[("refs/heads/main", "a")]), false);
    assert!(asks_child(&run.next(child(Ok::<_, GitError>(viewed)))));
    let none = child(Ok::<Option<GitStatus>, GitError>(None));
    assert!(asks_store(&run.next(none)));
    let answer = run.next(ran(GitEvent::Layout(Some(Layout::Arc))));
    assert!(matches!(
        answer,
        Ok(Next::Done((_, _, Some(Layout::Arc), _, false)))
    ));
}

/// An export stepped to the answer of the export itself.
fn exporting() -> ExportOperation {
    let document = dataset_of(57);
    let mut run = export(&git_node(), &auth(), document.document_id, "a".to_string());
    assert!(asks_child(&run.first()));
    assert!(asks_child(&run.next(child(Ok::<_, GitError>(document)))));
    let viewed = (testing::projection(&[]), false);
    assert!(asks_store(&run.next(child(Ok::<_, GitError>(viewed)))));
    let known = ran(GitEvent::Resolved(Some("a".to_string())));
    assert!(asks_store(&run.next(known)));
    run
}

// A revision the converter cannot read is the caller's error.
#[test]
fn export_reports_invalid() {
    let bytes = Bytes::from_static(br#"{"error":"no ISA"}"#);
    let answer = exporting().next(ran(GitEvent::Exported(bytes)));
    assert_eq!(answer, Err(GitError::Invalid));
}

#[test]
fn export_returns_bytes() {
    let bytes = Bytes::from_static(br#"{"rocrate":{}}"#);
    let answer = exporting().next(ran(GitEvent::Exported(bytes.clone())));
    assert_eq!(answer, Ok(Next::Done(bytes)));
}

// An unknown revision is not found.
#[test]
fn export_needs_revision() {
    let document = dataset_of(58);
    let mut run = export(&git_node(), &auth(), document.document_id, "a".to_string());
    run.step = ExportStep::Resolve;
    assert_eq!(
        run.next(ran(GitEvent::Resolved(None))),
        Err(GitError::NotFound)
    );
}
