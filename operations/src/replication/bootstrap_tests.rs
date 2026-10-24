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
    let count = PAGE_ROWS as u8 + 6;
    for seed in 1..=count {
        assert!(apply(&handle, write(set(), "k", seed, 3)).await.unwrap());
    }
    // The key's head row takes one place of the first page; it queues nothing.
    assert_eq!(page(&handle, 1).await, PageProgress::Queued(PAGE_ROWS - 1));
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
