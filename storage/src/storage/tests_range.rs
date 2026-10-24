//! Tests bounded range reads: their bounds, budgets and the interval they add to a read set.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::tests::{commit_transaction, start_write_transaction};
use super::{FjallStorage, StorageHandle};
use aruna_core::effects::{IterEnd, IterStart, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use byteview::ByteView;
use tempfile::tempdir;
use ulid::Ulid;

const SPACE: &str = "range_reads";

fn key(text: &str) -> ByteView {
    ByteView::from(text.as_bytes())
}

async fn put(handle: &StorageHandle, keys: &[&str], txn_id: Option<Ulid>) {
    let writes = keys
        .iter()
        .map(|name| (SPACE.to_string(), key(name), key("value")))
        .collect();
    let event = handle
        .send_storage_effect(StorageEffect::BatchWrite { writes, txn_id })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::BatchWriteResult { .. })
    ));
}

async fn read(
    handle: &StorageHandle,
    start: IterStart,
    end: IterEnd,
    budget: (usize, usize),
    txn_id: Option<Ulid>,
) -> (Vec<String>, Option<String>) {
    let event = handle
        .send_storage_effect(StorageEffect::IterRange {
            key_space: SPACE.to_string(),
            start,
            end,
            limit: budget.0,
            max_bytes: budget.1,
            txn_id,
        })
        .await;
    let Event::Storage(StorageEvent::IterResult {
        values,
        next_start_after,
    }) = event
    else {
        panic!("unexpected storage event: {event:?}");
    };
    let text = |bytes: &ByteView| String::from_utf8(bytes.to_vec()).unwrap();
    let keys = values.iter().map(|(key, _)| text(key)).collect();
    (keys, next_start_after.as_ref().map(text))
}

#[tokio::test]
async fn honours_both_bounds() {
    let dir = tempdir().unwrap();
    let handle = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    put(&handle, &["a", "b", "c", "d", "e"], None).await;
    let all = (100, usize::MAX);

    let (keys, next) = read(
        &handle,
        IterStart::At(key("b")),
        IterEnd::Through(key("d")),
        all,
        None,
    )
    .await;
    assert_eq!(
        (keys, next),
        (vec!["b".into(), "c".into(), "d".into()], None)
    );
    let (keys, _) = read(
        &handle,
        IterStart::After(key("b")),
        IterEnd::Before(key("d")),
        all,
        None,
    )
    .await;
    assert_eq!(keys, vec!["c".to_string()]);
    let (keys, _) = read(
        &handle,
        IterStart::At(key("d")),
        IterEnd::Before(key("d")),
        all,
        None,
    )
    .await;
    assert!(keys.is_empty(), "an empty interval reads nothing");
    let (keys, _) = read(
        &handle,
        IterStart::After(key("e")),
        IterEnd::Through(key("a")),
        all,
        None,
    )
    .await;
    assert!(keys.is_empty(), "an inverted interval reads nothing");
}

#[tokio::test]
async fn stops_at_budgets() {
    let dir = tempdir().unwrap();
    let handle = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    put(&handle, &["a", "b", "c", "d"], None).await;
    let end = IterEnd::Before(key("z"));

    let (keys, next) = read(
        &handle,
        IterStart::At(key("a")),
        end.clone(),
        (2, usize::MAX),
        None,
    )
    .await;
    assert_eq!(
        (keys, next),
        (vec!["a".into(), "b".into()], Some("b".into()))
    );
    // Each row is 6 bytes: a 12-byte budget returns two rows, a 1-byte budget still one.
    let (keys, next) = read(
        &handle,
        IterStart::At(key("a")),
        end.clone(),
        (100, 13),
        None,
    )
    .await;
    assert_eq!(
        (keys, next),
        (vec!["a".into(), "b".into()], Some("b".into()))
    );
    let (keys, next) = read(
        &handle,
        IterStart::At(key("a")),
        end.clone(),
        (100, 1),
        None,
    )
    .await;
    assert_eq!((keys, next), (vec!["a".into()], Some("a".into())));
    let (keys, next) = read(
        &handle,
        IterStart::After(key("b")),
        end,
        (2, usize::MAX),
        None,
    )
    .await;
    assert_eq!(
        (keys, next),
        (vec!["c".into(), "d".into()], None),
        "the last page names no cursor"
    );
}

/// A bounded read conflicts only with writes inside its interval, so a page walk is not
/// aborted by unrelated writes behind its page end.
#[tokio::test]
async fn tracks_only_interval() {
    let dir = tempdir().unwrap();
    let handle = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    put(&handle, &["a", "b", "c"], None).await;

    let page = start_write_transaction(&handle).await;
    read(
        &handle,
        IterStart::At(key("a")),
        IterEnd::Through(key("b")),
        (10, usize::MAX),
        Some(page),
    )
    .await;
    put(&handle, &["page"], Some(page)).await;
    let tail = start_write_transaction(&handle).await;
    put(&handle, &["c2"], Some(tail)).await;
    commit_transaction(&handle, tail).await;
    commit_transaction(&handle, page).await;

    let page = start_write_transaction(&handle).await;
    read(
        &handle,
        IterStart::At(key("a")),
        IterEnd::Through(key("b")),
        (10, usize::MAX),
        Some(page),
    )
    .await;
    put(&handle, &["page"], Some(page)).await;
    let inside = start_write_transaction(&handle).await;
    put(&handle, &["a2"], Some(inside)).await;
    commit_transaction(&handle, inside).await;
    let event = handle
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id: page })
        .await;
    assert_eq!(
        event,
        Event::Storage(StorageEvent::Error {
            error: StorageError::TransactionConflict
        })
    );
}
