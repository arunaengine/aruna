//! Tests that the realm config change count moves only after commits that may change it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::tests::{poll_queued, worker};
use super::{FjallStorage, SlowQueueAggregator, StorageHandle};
use aruna_core::effects::StorageEffect;
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use tempfile::tempdir;
use ulid::Ulid;

fn write(key_space: &str, txn_id: Option<Ulid>) -> StorageEffect {
    StorageEffect::Write {
        key_space: key_space.to_string(),
        key: b"realm".to_vec().into(),
        value: b"config".to_vec().into(),
        txn_id,
    }
}

fn start(storage: &mut FjallStorage) -> Ulid {
    match storage.process_effect(StorageEffect::StartTransaction { read: false }) {
        StorageEvent::TransactionStarted { txn_id } => txn_id,
        other => panic!("unexpected transaction start: {other:?}"),
    }
}

fn commit(storage: &mut FjallStorage, txn_id: Ulid) -> StorageEvent {
    storage.process_effect(StorageEffect::CommitTransaction { txn_id })
}

#[test]
fn commits_count_changes() {
    let dir = tempdir().unwrap();
    let (handle, _receivers) = StorageHandle::new();
    let mut storage = worker(&dir, &handle);

    let unrelated = start(&mut storage);
    storage.process_effect(write("other", Some(unrelated)));
    commit(&mut storage, unrelated);
    assert_eq!(handle.config_changes(), 0);

    let changed = start(&mut storage);
    storage.process_effect(write(REALM_CONFIG_KEYSPACE, Some(changed)));
    assert!(storage.config_txns.contains(&changed));
    assert_eq!(handle.config_changes(), 0, "not before the commit");
    commit(&mut storage, changed);
    assert_eq!(handle.config_changes(), 1);

    let aborted = start(&mut storage);
    storage.process_effect(StorageEffect::BatchDelete {
        deletes: vec![
            ("other".to_string(), b"a".to_vec().into()),
            (REALM_CONFIG_KEYSPACE.to_string(), b"realm".to_vec().into()),
        ],
        txn_id: Some(aborted),
    });
    storage.process_effect(StorageEffect::AbortTransaction { txn_id: aborted });
    assert_eq!(handle.config_changes(), 1);

    let fenced = start(&mut storage);
    storage.process_effect(write(REALM_CONFIG_KEYSPACE, Some(fenced)));
    storage.retire_fenced_txn(fenced);
    assert_eq!(handle.config_changes(), 1);

    storage.process_effect(write(REALM_CONFIG_KEYSPACE, None));
    storage.process_effect(StorageEffect::BatchWrite {
        writes: vec![
            (
                "other".to_string(),
                b"a".to_vec().into(),
                b"v".to_vec().into(),
            ),
            (
                REALM_CONFIG_KEYSPACE.to_string(),
                b"realm".to_vec().into(),
                b"v".to_vec().into(),
            ),
        ],
        txn_id: None,
    });
    storage.process_effect(StorageEffect::Delete {
        key_space: REALM_CONFIG_KEYSPACE.to_string(),
        key: b"realm".to_vec().into(),
        txn_id: None,
    });
    assert_eq!(handle.config_changes(), 4);
    storage.process_effect(write("other", None));
    assert_eq!(handle.config_changes(), 4);
    assert!(storage.config_txns.is_empty());
}

#[test]
fn conflict_keeps_count() {
    let dir = tempdir().unwrap();
    let (handle, _receivers) = StorageHandle::new();
    let mut storage = worker(&dir, &handle);
    let read = |txn_id| StorageEffect::Read {
        key_space: REALM_CONFIG_KEYSPACE.to_string(),
        key: b"realm".to_vec().into(),
        txn_id: Some(txn_id),
    };
    let first = start(&mut storage);
    let second = start(&mut storage);
    storage.process_effect(read(first));
    storage.process_effect(read(second));
    storage.process_effect(write(REALM_CONFIG_KEYSPACE, Some(first)));
    storage.process_effect(write(REALM_CONFIG_KEYSPACE, Some(second)));
    commit(&mut storage, first);
    assert_eq!(handle.config_changes(), 1);

    assert!(matches!(
        commit(&mut storage, second),
        StorageEvent::Error {
            error: StorageError::TransactionConflict
        }
    ));
    assert_eq!(handle.config_changes(), 1);
    assert!(storage.config_txns.is_empty());
}

#[tokio::test]
async fn grouped_write_counts() {
    let dir = tempdir().unwrap();
    let (handle, receivers) = StorageHandle::new();
    let mut storage = worker(&dir, &handle);
    let mut config = Box::pin(handle.send_storage_effect(write(REALM_CONFIG_KEYSPACE, None)));
    poll_queued(&mut config).await;
    let mut other = Box::pin(handle.send_storage_effect(write("other", None)));
    poll_queued(&mut other).await;
    let mut group = vec![
        receivers.foreground.recv().unwrap(),
        receivers.foreground.recv().unwrap(),
    ];

    storage.flush_write_group(&mut group, &mut SlowQueueAggregator::default());

    for event in [config.await, other.await] {
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }
    assert_eq!(handle.config_changes(), 1);
}
