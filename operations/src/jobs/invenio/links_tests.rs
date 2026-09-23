//! Drives link changes through their transaction without storage I/O.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::UserId;
use aruna_core::credential_encryption::CredentialEncryptionKey;
use aruna_core::invenio::{InvenioRecord, LinkFailure, LinkRemote};
use aruna_core::structs::execution::job::RoCrateLimits;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::record::FIRST_GRANTABLE_HANDLE;
use aruna_core::structured_id::{BucketId, PlacementHandle};
use aruna_core::task::TaskEvent;

const TXN: Ulid = Ulid::from_bytes([7; 16]);

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
    ChangeLinkOperation::new(link.document_id, link.link_id, change)
}

/// Runs the operation up to the effects that follow reading the stored link.
fn read(op: &mut ChangeLinkOperation, stored: Option<&InvenioLink>) -> Effects {
    assert!(matches!(
        op.start()[..],
        [Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    ));
    let effects = op.step(Event::Storage(StorageEvent::TransactionStarted {
        txn_id: TXN,
    }));
    assert!(matches!(
        &effects[..],
        [Effect::Storage(StorageEffect::Read { key_space, txn_id: Some(TXN), .. })]
            if key_space == INVENIO_LINK_KEYSPACE
    ));
    op.step(Event::Storage(StorageEvent::ReadResult {
        key: ByteView::from(vec![]),
        value: stored.map(|link| ByteView::from(link.to_bytes().unwrap())),
    }))
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
        secret: secret(link.link_id),
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
    assert_eq!(op.finalize(), Ok(Some(link)));
}

#[test]
fn create_refuses_duplicates() {
    let link = link();
    let mut op = operation(LinkChange::Create {
        link: Box::new(link.clone()),
        secret: secret(link.link_id),
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
        secret: secret(Ulid::from_bytes([9; 16])),
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
                record: record.clone(),
                event_id: Ulid::from_bytes([9; 16]),
                dataset_digest: None,
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
