//! Drives link changes through their transaction without storage I/O.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::UserId;
use aruna_core::credential_encryption::CredentialEncryptionKey;
use aruna_core::invenio::{InvenioRecord, LinkFailure, LinkRemote};
use aruna_core::keyspaces::{
    SHARD_MANIFEST_KEYSPACE, SYNC_OUTBOX_KEYSPACE, SYNC_REVISION_KEYSPACE, WRITE_FENCE_KEYSPACE,
};
use aruna_core::structs::execution::job::RoCrateLimits;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::record::FIRST_GRANTABLE_HANDLE;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::structured_id::{BucketId, PlacementHandle};
use aruna_core::task::TaskEvent;

const TXN: Ulid = Ulid::from_bytes([7; 16]);
const NOW_MS: u64 = 1_700_000_000_000;

fn now() -> SystemTime {
    SystemTime::UNIX_EPOCH + std::time::Duration::from_millis(NOW_MS)
}

fn job(nonce: u64) -> JobId {
    JobId::from_parts(
        1_700_000_000_000,
        PlacementHandle::new(FIRST_GRANTABLE_HANDLE).unwrap(),
        BucketId::new(0).unwrap(),
        nonce,
    )
    .unwrap()
}

fn link() -> InvenioLink {
    InvenioLink {
        link_id: Ulid::from_bytes([1; 16]),
        document_id: Ulid::from_bytes([2; 16]),
        group_id: Ulid::from_bytes([4; 16]),
        connector_id: Ulid::from_bytes([5; 16]),
        endpoint: "https://zenodo.org/api/".into(),
        owner_node: iroh::SecretKey::from_bytes(&[7; 32]).public(),
        owner_node_url: "https://node.example/api/v1".into(),
        created_by: UserId::local(Ulid::from_bytes([6; 16]), RealmId::from_bytes([3; 32])),
        status: LinkStatus::Enabled,
        auto_publish: false,
        public_files: false,
        metadata_json: "{}".into(),
        remote: LinkRemote::default(),
        last_push: None,
        active_job: None,
        sequence: 0,
        limits: RoCrateLimits::default(),
        created_at: SystemTime::UNIX_EPOCH,
        updated_at: SystemTime::UNIX_EPOCH,
        generation: 0,
        warning: None,
        direction: aruna_core::invenio::LinkDirection::Push,
    }
}

fn secret(link_id: Ulid) -> InvenioCredential {
    let link = link();
    InvenioCredential::seal_link(
        &CredentialEncryptionKey::derive(&[1; 32]),
        link.created_by,
        link.group_id,
        link.connector_id,
        Some(link_id),
        link.endpoint,
        "link-token",
    )
    .unwrap()
}

fn operation(change: LinkChange) -> ChangeLinkOperation {
    let link = link();
    ChangeLinkOperation::new(link.document_id, link.link_id, change, now())
}

/// Runs the operation up to the effects that follow reading the stored link.
fn read(op: &mut ChangeLinkOperation, stored: Option<&InvenioLink>) -> Effects {
    read_queued(op, stored, None)
}

/// Like `read`, with a push check already queued for the link.
fn read_queued(
    op: &mut ChangeLinkOperation,
    stored: Option<&InvenioLink>,
    queued: Option<&LinkQueueEntry>,
) -> Effects {
    assert!(matches!(
        op.start()[..],
        [Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    ));
    let effects = op.step(Event::Storage(StorageEvent::TransactionStarted {
        txn_id: TXN,
    }));
    let [
        Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: Some(TXN),
        }),
    ] = &effects[..]
    else {
        panic!("expected the link and queue read, got {effects:?}");
    };
    let read = reads.iter().map(|row| row.0.as_str()).collect::<Vec<_>>();
    assert_eq!(read, [INVENIO_LINK_KEYSPACE, LINK_QUEUE_KEYSPACE]);
    op.step(Event::Storage(StorageEvent::BatchReadResult {
        values: vec![
            (
                ByteView::from(vec![]),
                stored.map(|link| ByteView::from(link.to_bytes().unwrap())),
            ),
            (
                ByteView::from(vec![]),
                queued.map(|entry| ByteView::from(postcard::to_allocvec(entry).unwrap())),
            ),
        ],
    }))
}

fn queued_entry(effects: &Effects) -> Option<LinkQueueEntry> {
    written(effects)
        .into_iter()
        .find(|row| row.0 == LINK_QUEUE_KEYSPACE)
        .map(|row| postcard::from_bytes(&row.2).unwrap())
}

fn keyspaces<T>(rows: &[(String, Key, T)]) -> Vec<&str> {
    rows.iter().map(|row| row.0.as_str()).collect()
}

fn written(effects: &Effects) -> Vec<(String, Key, Value)> {
    match &effects[..] {
        [Effect::Storage(StorageEffect::BatchWrite { writes, txn_id })] => {
            assert_eq!(*txn_id, Some(TXN));
            writes.clone()
        }
        other => panic!("expected one batch write, got {other:?}"),
    }
}

fn commit(op: &mut ChangeLinkOperation, effects: Effects) -> Effects {
    let effects = match &effects[..] {
        [Effect::Storage(StorageEffect::BatchWrite { .. })] => {
            op.step(Event::Storage(StorageEvent::BatchWriteResult {
                entries: vec![],
            }))
        }
        _ => effects,
    };
    let effects = match &effects[..] {
        [Effect::Storage(StorageEffect::BatchDelete { .. })] => {
            op.step(Event::Storage(StorageEvent::BatchDeleteResult {
                entries: vec![],
            }))
        }
        _ => effects,
    };
    assert!(matches!(
        effects[..],
        [Effect::Storage(StorageEffect::CommitTransaction {
            txn_id: TXN
        })]
    ));
    op.step(Event::Storage(StorageEvent::TransactionCommitted {
        txn_id: TXN,
    }))
}

fn schedules(effects: &Effects) -> bool {
    matches!(
        &effects[..],
        [Effect::Task(TaskEffect::ResetTimer { key: TaskKey::DrainLinkQueue, after })]
            if after.is_zero()
    )
}

#[test]
fn create_writes_rows() {
    let link = link();
    let mut op = operation(LinkChange::Create {
        link: Box::new(link.clone()),
        secret: Some(secret(link.link_id)),
    });
    let effects = read(&mut op, None);
    let rows = written(&effects);
    assert_eq!(
        keyspaces(&rows),
        [
            LINK_SECRET_KEYSPACE,
            LINK_CONNECTOR_KEYSPACE,
            INVENIO_LINK_KEYSPACE,
            LINK_QUEUE_KEYSPACE
        ]
    );
    assert!(
        rows.iter()
            .all(|row| !String::from_utf8_lossy(&row.2).contains("link-token"))
    );
    let effects = commit(&mut op, effects);
    assert!(schedules(&effects));
    assert!(!op.is_complete());
    op.step(Event::Task(TaskEvent::Error {
        key: None,
        message: "no task handle".into(),
    }));
    assert!(op.is_complete());
    let created = op.finalize().unwrap().unwrap();
    assert!(
        created.generation > 0,
        "the stored row carries a sync generation"
    );
    assert_eq!(
        InvenioLink {
            generation: 0,
            ..created
        },
        link
    );
}

#[test]
fn create_refuses_duplicates() {
    let link = link();
    let mut op = operation(LinkChange::Create {
        link: Box::new(link.clone()),
        secret: Some(secret(link.link_id)),
    });
    let effects = read(&mut op, Some(&link));
    assert!(matches!(
        effects[..],
        [Effect::Storage(StorageEffect::AbortTransaction {
            txn_id: TXN
        })]
    ));
    op.step(Event::Storage(StorageEvent::TransactionAborted {
        txn_id: TXN,
    }));
    assert_eq!(op.finalize(), Err(LinkError::Exists));

    let mut foreign = operation(LinkChange::Create {
        link: Box::new(link),
        secret: Some(secret(Ulid::from_bytes([9; 16]))),
    });
    read(&mut foreign, None);
    foreign.step(Event::Storage(StorageEvent::TransactionAborted {
        txn_id: TXN,
    }));
    assert_eq!(foreign.finalize(), Err(LinkError::ForeignToken));
}

#[test]
fn delete_removes_secret() {
    let link = link();
    let mut op = operation(LinkChange::Delete);
    let effects = read(&mut op, Some(&link));
    let [Effect::Storage(StorageEffect::BatchDelete { deletes, .. })] = &effects[..] else {
        panic!("expected one batch delete, got {effects:?}");
    };
    let spaces = deletes.iter().map(|row| row.0.as_str()).collect::<Vec<_>>();
    assert_eq!(
        spaces,
        [
            INVENIO_LINK_KEYSPACE,
            LINK_QUEUE_KEYSPACE,
            LINK_SECRET_KEYSPACE,
            LINK_CONNECTOR_KEYSPACE
        ]
    );
    let effects = commit(&mut op, effects);
    assert!(effects.is_empty() && op.is_complete());
    assert_eq!(op.finalize(), Ok(None));

    let mut missing = operation(LinkChange::Delete);
    read(&mut missing, None);
    missing.step(Event::Storage(StorageEvent::TransactionAborted {
        txn_id: TXN,
    }));
    assert_eq!(missing.finalize(), Err(LinkError::NotFound));
}

#[test]
fn rejects_unexpected_events() {
    let mut op = operation(LinkChange::Delete);
    op.start();
    let effects = op.step(Event::Storage(StorageEvent::BatchWriteResult {
        entries: vec![],
    }));
    assert!(effects.is_empty() && op.is_complete());
    assert!(matches!(op.finalize(), Err(LinkError::Unexpected(_))));

    let mut op = operation(LinkChange::Delete);
    op.start();
    op.step(Event::Storage(StorageEvent::TransactionStarted {
        txn_id: TXN,
    }));
    let effects = op.step(Event::Storage(StorageEvent::BatchDeleteResult {
        entries: vec![],
    }));
    assert!(matches!(
        effects[..],
        [Effect::Storage(StorageEffect::AbortTransaction {
            txn_id: TXN
        })]
    ));
    op.step(Event::Storage(StorageEvent::TransactionAborted {
        txn_id: TXN,
    }));
    assert!(matches!(op.finalize(), Err(LinkError::Unexpected(_))));
}

#[test]
fn begin_takes_queue() {
    let mut stored = link();
    let mut op = operation(LinkChange::Begin(job(1)));
    let effects = read(&mut op, Some(&stored));
    assert_eq!(keyspaces(&written(&effects)), [INVENIO_LINK_KEYSPACE]);
    let effects = op.step(Event::Storage(StorageEvent::BatchWriteResult {
        entries: vec![],
    }));
    let [Effect::Storage(StorageEffect::BatchDelete { deletes, .. })] = &effects[..] else {
        panic!("expected the queue entry delete, got {effects:?}");
    };
    assert_eq!(deletes[0].0, LINK_QUEUE_KEYSPACE);

    stored.active_job = Some(job(1));
    let mut busy = operation(LinkChange::Begin(job(2)));
    read(&mut busy, Some(&stored));
    busy.step(Event::Storage(StorageEvent::TransactionAborted {
        txn_id: TXN,
    }));
    assert_eq!(busy.finalize(), Err(LinkError::Busy(LinkBusy)));
}

#[test]
fn finish_requeues_enabled() {
    let record = InvenioRecord {
        id: "draft-1".into(),
        url: "https://zenodo.org/api/records/draft-1/draft".into(),
        published: false,
        parent_id: "parent-1".into(),
        revision_id: 2,
        doi: None,
        html_url: None,
        concept_doi: None,
        in_review: false,
        warning: None,
    };
    for (paused, requeue, queued) in [
        (false, true, true),
        (true, true, false),
        (false, false, false),
    ] {
        let mut stored = link();
        stored.active_job = Some(job(1));
        if paused {
            stored.status = LinkStatus::Paused;
        }
        let mut op = operation(LinkChange::Finish {
            job_id: job(1),
            outcome: Box::new(PushOutcome::Pushed {
                record: Box::new(record.clone()),
                event_id: Ulid::from_bytes([9; 16]),
                dataset_digest: None,
                files: Vec::new(),
            }),
            requeue,
        });
        let effects = read(&mut op, Some(&stored));
        let rows = written(&effects);
        assert_eq!(rows.iter().any(|row| row.0 == LINK_QUEUE_KEYSPACE), queued);
        let effects = commit(&mut op, effects);
        assert_eq!(schedules(&effects), queued);
        if queued {
            op.step(Event::Task(TaskEvent::Error {
                key: None,
                message: String::new(),
            }));
        }
        let link = op.finalize().unwrap().unwrap();
        assert_eq!(link.remote.draft_id.as_deref(), Some("draft-1"));
        assert_eq!(link.active_job, None);
    }
}

#[test]
fn rotate_resumes_rejected() {
    let mut stored = link();
    stored.status = LinkStatus::Failed {
        reason: LinkFailure::TokenRejected,
    };
    let mut op = operation(LinkChange::Rotate(secret(stored.link_id)));
    let effects = read(&mut op, Some(&stored));
    assert_eq!(
        keyspaces(&written(&effects)),
        [
            LINK_SECRET_KEYSPACE,
            INVENIO_LINK_KEYSPACE,
            LINK_QUEUE_KEYSPACE
        ]
    );
    let mut foreign = operation(LinkChange::Rotate(secret(Ulid::from_bytes([9; 16]))));
    read(&mut foreign, Some(&stored));
    foreign.step(Event::Storage(StorageEvent::TransactionAborted {
        txn_id: TXN,
    }));
    assert_eq!(foreign.finalize(), Err(LinkError::ForeignToken));
}

#[test]
fn patch_pauses_quietly() {
    let stored = link();
    let mut op = operation(LinkChange::Patch(LinkPatch {
        paused: Some(true),
        ..LinkPatch::default()
    }));
    let effects = read(&mut op, Some(&stored));
    assert_eq!(keyspaces(&written(&effects)), [INVENIO_LINK_KEYSPACE]);
    let effects = commit(&mut op, effects);
    assert!(effects.is_empty());
    assert_eq!(op.finalize().unwrap().unwrap().status, LinkStatus::Paused);
}

fn route(generation: u64) -> MappingRoute {
    MappingRoute {
        realm_id: RealmId::from_bytes([3; 32]),
        placement: PlacementRef {
            strategy_id: Ulid::from_bytes([8; 16]),
            shard: 2,
        },
        peers: vec![iroh::SecretKey::from_bytes(&[9; 32]).public()],
        actor: link().owner_node,
        generation,
    }
}

/// Answers the fence read of a routed change with the stored fence value.
fn fenced(
    op: &mut ChangeLinkOperation,
    stored: Option<&InvenioLink>,
    fence: Option<u64>,
) -> Effects {
    let effects = read(op, stored);
    assert!(matches!(
        &effects[..],
        [Effect::Storage(StorageEffect::Read { key_space, txn_id: Some(TXN), .. })]
            if key_space == WRITE_FENCE_KEYSPACE
    ));
    op.step(Event::Storage(StorageEvent::ReadResult {
        key: ByteView::from(vec![]),
        value: fence.map(|closed| ByteView::from(closed.to_be_bytes().to_vec())),
    }))
}

fn outbox_event(rows: &[(String, Key, Value)]) -> DocumentOutboxEvent {
    let row = rows
        .iter()
        .find(|row| row.0 == SYNC_OUTBOX_KEYSPACE)
        .expect("outbox row");
    postcard::from_bytes::<aruna_core::document::DocumentOutboxRecord>(&row.2)
        .unwrap()
        .event
}

#[test]
fn routed_changes_replicate() {
    let link = link();
    let route = route(3);
    let mut op = operation(LinkChange::Create {
        link: Box::new(link.clone()),
        secret: Some(secret(link.link_id)),
    })
    .routed(Some(route.clone()));
    let effects = fenced(&mut op, None, Some(2));
    let rows = written(&effects);
    for keyspace in [
        SYNC_REVISION_KEYSPACE,
        SHARD_MANIFEST_KEYSPACE,
        SYNC_OUTBOX_KEYSPACE,
    ] {
        assert!(
            keyspaces(&rows).contains(&keyspace),
            "{keyspace} row missing"
        );
    }
    let effects = commit(&mut op, effects);
    assert_eq!(effects.len(), 2, "link and outbox drains are scheduled");
    for _ in 0..2 {
        op.step(Event::Task(TaskEvent::Error {
            key: None,
            message: "no task handle".into(),
        }));
    }
    let created = op.finalize().unwrap().unwrap();
    assert!(created.generation > 0);
    let DocumentOutboxEvent::Upsert { bytes, change } = outbox_event(&rows) else {
        panic!("create publishes an upsert");
    };
    assert_eq!(InvenioLink::from_bytes(&bytes).unwrap(), created);
    assert_eq!(change, created.sync_change(route.placement));

    // A delete keeps a tombstone that orders after the last stored change.
    let mut op = operation(LinkChange::Delete).routed(Some(route.clone()));
    let effects = fenced(&mut op, Some(&created), None);
    let rows = written(&effects);
    let DocumentOutboxEvent::Delete { change } = outbox_event(&rows) else {
        panic!("delete publishes a delete");
    };
    assert_eq!(change, created.delete_change(route.placement));
    assert!(keyspaces(&rows).contains(&SHARD_MANIFEST_KEYSPACE));
    assert!(!keyspaces(&rows).contains(&INVENIO_LINK_KEYSPACE));
}

#[test]
fn closed_fence_refuses() {
    let link = link();
    let mut op = operation(LinkChange::Patch(LinkPatch::default())).routed(Some(route(3)));
    let effects = fenced(&mut op, Some(&link), Some(3));
    assert!(matches!(
        effects[..],
        [Effect::Storage(StorageEffect::AbortTransaction {
            txn_id: TXN
        })]
    ));
    op.step(Event::Storage(StorageEvent::TransactionAborted {
        txn_id: TXN,
    }));
    assert_eq!(op.finalize(), Err(LinkError::Fenced));
}

#[test]
fn aborting_rejects_events() {
    let mut op = operation(LinkChange::Delete);
    read(&mut op, None);
    let effects = op.step(Event::Storage(StorageEvent::BatchWriteResult {
        entries: vec![],
    }));
    assert!(effects.is_empty() && op.is_complete());
    assert!(matches!(op.finalize(), Err(LinkError::Unexpected(_))));
}

#[test]
fn finish_schedules_follow_ups() {
    let mut record = InvenioRecord {
        id: "draft-1".into(),
        url: "https://zenodo.org/api/records/draft-1/draft".into(),
        published: false,
        parent_id: "parent-1".into(),
        revision_id: 2,
        doi: Some("10.5281/zenodo.2".into()),
        html_url: None,
        concept_doi: None,
        in_review: true,
        warning: None,
    };
    let finish = |record: &InvenioRecord| LinkChange::Finish {
        job_id: job(1),
        outcome: Box::new(PushOutcome::Pushed {
            record: Box::new(record.clone()),
            event_id: Ulid::from_bytes([9; 16]),
            dataset_digest: None,
            files: vec!["data.txt".into()],
        }),
        requeue: false,
    };
    let mut stored = link();
    stored.active_job = Some(job(1));
    let effects = read(&mut operation(finish(&record)), Some(&stored));
    let entry = queued_entry(&effects).expect("a pending review is polled");
    assert_eq!(entry.due_at_ms, NOW_MS + REVIEW_POLL_MS);

    record.in_review = false;
    stored.auto_publish = true;
    let effects = read(&mut operation(finish(&record)), Some(&stored));
    let entry = queued_entry(&effects).expect("auto_publish waits for a quiet draft");
    assert_eq!(
        entry.due_at_ms,
        NOW_MS + aruna_core::invenio::AUTO_PUBLISH_QUIET_MS
    );

    // A change queued during the push keeps its earlier check.
    let earlier = LinkQueueEntry {
        document_id: stored.document_id,
        due_at_ms: NOW_MS + 5,
        first_at_ms: NOW_MS - 5,
    };
    let mut op = operation(finish(&record));
    let effects = read_queued(&mut op, Some(&stored), Some(&earlier));
    assert_eq!(queued_entry(&effects), Some(earlier));
}

#[test]
fn draft_needs_running_push() {
    let record = InvenioRecord {
        id: "draft-1".into(),
        url: "https://zenodo.org/api/records/draft-1/draft".into(),
        published: false,
        parent_id: "parent-1".into(),
        revision_id: 4,
        doi: None,
        html_url: None,
        concept_doi: None,
        in_review: false,
        warning: None,
    };
    let change = || LinkChange::Draft {
        job_id: job(1),
        record: Box::new(record.clone()),
    };
    let mut op = operation(change());
    read(&mut op, Some(&link()));
    op.step(Event::Storage(StorageEvent::TransactionAborted {
        txn_id: TXN,
    }));
    assert_eq!(op.finalize(), Err(LinkError::Busy(LinkBusy)));

    let mut stored = link();
    stored.active_job = Some(job(1));
    let mut op = operation(change());
    let effects = read(&mut op, Some(&stored));
    assert_eq!(keyspaces(&written(&effects)), [INVENIO_LINK_KEYSPACE]);
    commit(&mut op, effects);
    let link = op.finalize().unwrap().unwrap();
    assert_eq!(link.remote.draft_id.as_deref(), Some("draft-1"));
    assert_eq!(link.remote.revision_id, Some(4));
}

fn pulling() -> InvenioLink {
    let mut link = link();
    link.direction =
        aruna_core::invenio::LinkDirection::Pull(Box::new(aruna_core::invenio::LinkPull {
            auto_update: false,
            options: Default::default(),
            target: aruna_core::structs::execution::job::ImportRoCrateTarget {
                bucket: "research".into(),
                prefix: "zenodo".into(),
            },
            latest_remote_id: None,
            latest_revision: None,
            last_checked_at: None,
            next_check_ms: u64::MAX,
            failures: 0,
            revision: None,
            local_changed: false,
        }));
    link
}

#[test]
fn pull_links_never_push() {
    let mut link = pulling();
    link.pull_mut().unwrap().next_check_ms = 5_000;
    let mut op = operation(LinkChange::Create {
        link: Box::new(link.clone()),
        secret: None,
    });
    let effects = read(&mut op, None);
    let rows = written(&effects);
    assert_eq!(
        keyspaces(&rows),
        [
            LINK_CONNECTOR_KEYSPACE,
            INVENIO_LINK_KEYSPACE,
            LINK_QUEUE_KEYSPACE
        ]
    );
    // The queue row holds the first repository check, not a push.
    let entry: LinkQueueEntry = postcard::from_bytes(&rows[2].2).unwrap();
    assert_eq!(entry.due_at_ms, 5_000);
    assert!(schedules(&commit(&mut op, effects)));
    // Resuming a pull link checks the repository now instead of queueing a push.
    let mut paused = link.clone();
    paused.status = LinkStatus::Paused;
    let mut op = operation(LinkChange::Patch(LinkPatch {
        paused: Some(false),
        ..LinkPatch::default()
    }));
    let effects = read(&mut op, Some(&paused));
    let rows = written(&effects);
    assert_eq!(
        keyspaces(&rows),
        [INVENIO_LINK_KEYSPACE, LINK_QUEUE_KEYSPACE]
    );
    let now_ms = now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let stored = InvenioLink::from_bytes(&rows[0].2).unwrap();
    assert_eq!(stored.pull().unwrap().next_check_ms, 5_000.min(now_ms));
    assert!(schedules(&commit(&mut op, effects)));
}

#[test]
fn pull_check_replaces_row() {
    // A finished check moves the queued check to its next due time, even when it is later.
    let mut op = operation(LinkChange::Checked(PullCheck::Unavailable));
    let queued = LinkQueueEntry {
        document_id: pulling().document_id,
        due_at_ms: 1_000,
        first_at_ms: 1_000,
    };
    let effects = read_queued(&mut op, Some(&pulling()), Some(&queued));
    let rows = written(&effects);
    assert_eq!(
        keyspaces(&rows),
        [INVENIO_LINK_KEYSPACE, LINK_QUEUE_KEYSPACE]
    );
    let stored = InvenioLink::from_bytes(&rows[0].2).unwrap();
    let entry: LinkQueueEntry = postcard::from_bytes(&rows[1].2).unwrap();
    assert_eq!(entry.due_at_ms, stored.pull().unwrap().next_check_ms);
    assert!(entry.due_at_ms > 1_000);

    // A paused pull link has no queued check.
    let mut paused = pulling();
    paused.status = LinkStatus::Paused;
    let mut op = operation(LinkChange::Checked(PullCheck::Unavailable));
    let effects = read_queued(&mut op, Some(&paused), Some(&queued));
    assert_eq!(keyspaces(&written(&effects)), [INVENIO_LINK_KEYSPACE]);
    let effects = op.step(Event::Storage(StorageEvent::BatchWriteResult {
        entries: vec![],
    }));
    assert!(matches!(
        &effects[..],
        [Effect::Storage(StorageEffect::BatchDelete { deletes, .. })]
            if deletes.iter().map(|row| row.0.as_str()).eq([LINK_QUEUE_KEYSPACE])
    ));
}
