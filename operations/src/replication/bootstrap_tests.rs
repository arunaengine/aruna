//! Tests bootstrap pages against real storage: bounded pages, continuation inside one key,
//! waiting for the delivery of a page, completion and a restart for a newer intent.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::distribution::test_store::{apply, context, get, install, open, range, write};
use crate::driver::drive;
use aruna_storage::storage::StorageHandle as Handle;

fn set() -> Ulid {
    Ulid::from_bytes([9; 16])
}

/// Rows a test page reads, small enough for a key with more versions than one page.
const ROWS: usize = 64;

fn config(intent: u8) -> PageConfig {
    PageConfig {
        realm_id: RealmId::from_bytes([9; 32]),
        bucket: "spire".to_string(),
        set_id: set(),
        member_id: Ulid::from_bytes([2; 16]),
        node_id: iroh::SecretKey::from_bytes(&[2; 32]).public(),
        intent: [intent; 32],
        range: range(1, "", None),
        now_ms: 7,
        rows: ROWS,
    }
}

async fn page(handle: &Handle, intent: u8) -> PageProgress {
    drive(BootstrapPage::new(config(intent)), &context(handle))
        .await
        .expect("page runs")
}

async fn cursor(handle: &Handle) -> BootstrapCursor {
    let config = config(1);
    let key = bootstrap_key(set(), config.member_id, config.range.range_id);
    let value = get(handle, BOOTSTRAP_KEYSPACE, key)
        .await
        .expect("cursor stored");
    BootstrapCursor::from_bytes(&value).expect("cursor decodes")
}

/// Delivers the queued page: the drain deletes each job once it succeeded.
async fn deliver(handle: &Handle) {
    let pending = cursor(handle).await.pending;
    let deletes = (pending.into_iter())
        .map(|key| (REPLICATION_JOB_KEYSPACE.to_string(), Key::from(key)))
        .collect();
    let event = handle
        .send_storage_effect(StorageEffect::BatchDelete {
            deletes,
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::BatchDeleteResult { .. })
    ));
}

#[tokio::test]
async fn walks_then_completes() {
    let (_dir, handle) = open();
    install(&handle, set(), &[range(1, "", None)]).await;
    for (key, seed) in [("a", 1), ("b", 2)] {
        assert!(apply(&handle, write(set(), key, seed, 3)).await.unwrap());
    }
    assert_eq!(
        page(&handle, 1).await,
        PageProgress::Queued(2),
        "heads queue nothing"
    );
    let walked = cursor(&handle).await;
    assert!(walked.walked && !walked.done);
    for key in &walked.pending {
        assert!(
            get(&handle, REPLICATION_JOB_KEYSPACE, key.clone())
                .await
                .is_some()
        );
    }
    assert_eq!(page(&handle, 1).await, PageProgress::Waiting);
    deliver(&handle).await;
    assert_eq!(page(&handle, 1).await, PageProgress::Done { watermark: 7 });
    assert!(cursor(&handle).await.done);
    assert_eq!(page(&handle, 1).await, PageProgress::Done { watermark: 7 });
}

#[tokio::test]
async fn continues_inside_key() {
    // A key with more versions than one page resumes after the last queued version.
    let (_dir, handle) = open();
    install(&handle, set(), &[range(1, "", None)]).await;
    let count = ROWS as u8 + 6;
    for seed in 1..=count {
        assert!(apply(&handle, write(set(), "k", seed, 3)).await.unwrap());
    }
    // The key's head row takes one place of the first page; it queues nothing.
    assert_eq!(page(&handle, 1).await, PageProgress::Queued(ROWS - 1));
    let first = cursor(&handle).await;
    assert!(!first.walked);
    deliver(&handle).await;
    assert_eq!(page(&handle, 1).await, PageProgress::Queued(7));
    let second = cursor(&handle).await;
    assert!(second.walked);
    assert!(
        second
            .pending
            .iter()
            .all(|key| !first.pending.contains(key))
    );
}

#[tokio::test]
async fn newer_intent_restarts() {
    let (_dir, handle) = open();
    install(&handle, set(), &[range(1, "", None)]).await;
    assert!(apply(&handle, write(set(), "a", 1, 3)).await.unwrap());
    page(&handle, 1).await;
    deliver(&handle).await;
    assert_eq!(page(&handle, 1).await, PageProgress::Done { watermark: 7 });
    assert_eq!(
        page(&handle, 2).await,
        PageProgress::Queued(1),
        "a later join catches up again"
    );
}

fn cursor_row(value: Option<Vec<u8>>) -> Event {
    Event::Storage(StorageEvent::ReadResult {
        key: b"cursor".to_vec().into(),
        value: value.map(Into::into),
    })
}

#[test]
fn reads_stored_cursor() {
    let stored = BootstrapCursor::start([3; 32], 7);
    let start = Effect::Storage(StorageEffect::Read {
        key_space: BOOTSTRAP_KEYSPACE.to_string(),
        key: b"cursor".to_vec().into(),
        txn_id: None,
    });
    let mut operation = ReadCursorOperation::new(b"cursor".to_vec());
    let steps = vec![(cursor_row(Some(stored.to_bytes().unwrap())), vec![])];
    aruna_core::operation_check::check_trace(&mut operation, &[start], steps);
    assert_eq!(operation.finalize(), Ok(Some(stored)));
}

#[test]
fn cursor_read_outcomes() {
    let read = |event: Event| {
        let mut operation = ReadCursorOperation::new(b"cursor".to_vec());
        operation.start();
        operation.step(event);
        operation.finalize()
    };
    assert_eq!(read(cursor_row(None)), Ok(None));
    assert!(matches!(
        read(cursor_row(Some(vec![0xff]))),
        Err(BootstrapError::Conversion(_))
    ));
    let failed = Event::Storage(StorageEvent::Error {
        error: StorageError::Timeout,
    });
    assert_eq!(
        read(failed),
        Err(BootstrapError::Storage(StorageError::Timeout))
    );
}

#[test]
fn cursor_read_contract() {
    use aruna_core::operation_check::{
        check_abort, rejects_events, rejects_finalize, sample_events,
    };
    let make = || {
        let mut operation = ReadCursorOperation::new(b"cursor".to_vec());
        operation.start();
        operation
    };
    let accepted = |event: &Event| matches!(event, Event::Storage(StorageEvent::ReadResult { .. }));
    let refused = rejects_events(make, sample_events, accepted);
    assert_eq!(refused.len(), sample_events().len() - 1);
    let mut fresh = ReadCursorOperation::new(b"cursor".to_vec());
    check_abort(&mut fresh, &[]);
    rejects_finalize(fresh);
}

/// A cursor row of `member` for the set of [`set`].
fn cursor_key(member: u8) -> Key {
    bootstrap_key(
        set(),
        Ulid::from_bytes([member; 16]),
        Ulid::from_bytes([1; 16]),
    )
    .into()
}

fn cursor_page(members: &[u8]) -> Event {
    Event::Storage(StorageEvent::IterResult {
        values: (members.iter())
            .map(|member| (cursor_key(*member), Vec::<u8>::new().into()))
            .collect(),
        next_start_after: None,
    })
}

#[test]
fn clears_set_cursors() {
    let scan = Effect::Storage(StorageEffect::Iter {
        key_space: BOOTSTRAP_KEYSPACE.to_string(),
        prefix: Some(bootstrap_prefix(set()).into()),
        start: None,
        limit: 2,
        txn_id: None,
    });
    let delete = Effect::Storage(StorageEffect::BatchDelete {
        deletes: vec![(BOOTSTRAP_KEYSPACE.to_string(), cursor_key(4))],
        txn_id: None,
    });
    let deleted = Event::Storage(StorageEvent::BatchDeleteResult {
        entries: Vec::new(),
    });
    let mut operation = ClearCursorsOperation::new(set(), None, 2);
    let steps = vec![(cursor_page(&[4]), vec![delete]), (deleted, vec![])];
    aruna_core::operation_check::check_trace(&mut operation, &[scan], steps);
    assert_eq!(operation.finalize(), Ok(true));
}

#[test]
fn keeps_walked_cursors() {
    // Only the cursors of members no longer walked go; a full page is not all of them.
    let walked = vec![Ulid::from_bytes([4; 16])];
    let mut operation = ClearCursorsOperation::new(set(), Some(walked.clone()), 2);
    operation.start();
    let delete = Effect::Storage(StorageEffect::BatchDelete {
        deletes: vec![(BOOTSTRAP_KEYSPACE.to_string(), cursor_key(5))],
        txn_id: None,
    });
    assert_eq!(operation.step(cursor_page(&[4, 5])).as_slice(), [delete]);
    let mut kept = ClearCursorsOperation::new(set(), Some(walked), 2);
    kept.start();
    assert!(kept.step(cursor_page(&[4])).is_empty());
    assert_eq!(kept.finalize(), Ok(true));
    let mut full = ClearCursorsOperation::new(set(), Some(vec![Ulid::from_bytes([5; 16])]), 1);
    full.start();
    assert!(full.step(cursor_page(&[5])).is_empty());
    assert_eq!(full.finalize(), Ok(false));
}

#[test]
fn cursor_clear_contract() {
    use aruna_core::operation_check::{
        check_abort, rejects_events, rejects_finalize, sample_events,
    };
    let make = || {
        let mut operation = ClearCursorsOperation::new(set(), None, 2);
        operation.start();
        operation
    };
    let accepted = |event: &Event| matches!(event, Event::Storage(StorageEvent::IterResult { .. }));
    let refused = rejects_events(make, sample_events, accepted);
    assert_eq!(refused.len(), sample_events().len() - 1);
    let deleting = || {
        let mut operation = ClearCursorsOperation::new(set(), None, 2);
        operation.start();
        operation.step(cursor_page(&[4]));
        operation
    };
    let accepted = |event: &Event| {
        matches!(
            event,
            Event::Storage(StorageEvent::BatchDeleteResult { .. })
        )
    };
    let refused = rejects_events(deleting, sample_events, accepted);
    assert_eq!(refused.len(), sample_events().len() - 1);
    let failed = Event::Storage(StorageEvent::Error {
        error: StorageError::Timeout,
    });
    let mut scanning = make();
    scanning.step(failed);
    assert_eq!(
        scanning.finalize(),
        Err(BootstrapError::Storage(StorageError::Timeout))
    );
    let mut fresh = ClearCursorsOperation::new(set(), None, 2);
    check_abort(&mut fresh, &[]);
    rejects_finalize(fresh);
}
