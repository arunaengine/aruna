//! Multipart sealing and composition within the node's Pithos working set.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::setup_two_backends;
use crate::blob::pithos::{Share, WORKING_SET, budget_permits, working_set};
use crate::blob::pithos_parts::{PART_BLOCK, PartBatch, blocking};
use aruna_core::errors::BlobError;
use aruna_core::events::BlobEvent;
use aruna_core::structs::storage::multipart::MAX_PART_SIZE;
use bytes::Bytes;
use futures::FutureExt;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Semaphore;

const MIB: usize = 1 << 20;

#[test]
fn oversized_part_refused() {
    // A declared or single large chunk above the cap is refused before any block is encoded.
    let mut batch = PartBatch::new(10 * MIB as u64);
    let refused = batch.push(Bytes::from(vec![1; 11 * MIB]));
    assert!(matches!(
        refused,
        Err(BlobError::SizeLimitExceeded { limit }) if limit == 10 * MIB as u64
    ));
    assert!(batch.rest().is_empty());
}

#[test]
fn unsized_part_refused() {
    // An unsized part is counted as it arrives and stops at the cap.
    let mut batch = PartBatch::new(10 * MIB as u64);
    let mut blocks = 0;
    for _ in 0..10 {
        blocks += batch.push(Bytes::from(vec![1; MIB])).unwrap().len();
    }
    assert_eq!(blocks, 2);
    assert!(batch.push(Bytes::from(vec![1; 1])).is_err());
}

#[test]
fn large_chunk_sliced() {
    // A large chunk is encoded in block slices, never as one buffer.
    let mut batch = PartBatch::new(MAX_PART_SIZE);
    batch.push(Bytes::from(vec![1; MIB])).unwrap();
    let blocks = batch.push(Bytes::from(vec![2; 9 * MIB])).unwrap();
    assert_eq!(blocks.len(), 2);
    assert!(blocks.iter().all(|block| block.len() == PART_BLOCK));
    assert_eq!(batch.rest().len(), 2 * MIB);
}

#[tokio::test]
async fn sealing_keeps_share() {
    // A cancelled part does not free its share while the detached sealing task still runs.
    let budget = Arc::new(Semaphore::new(budget_permits()));
    let permit = Arc::clone(&budget).acquire_many_owned(64).await.unwrap();
    let share: Share = Arc::new(Mutex::new(permit));
    let (release, wait) = std::sync::mpsc::channel::<()>();
    let (started, running) = tokio::sync::oneshot::channel();
    let mut task = Box::pin(blocking(&share, move || {
        let _ = started.send(());
        let _ = wait.recv();
        Ok(())
    }));
    assert!((&mut task).now_or_never().is_none());
    running.await.unwrap();
    drop(task);
    drop(share);
    assert_eq!(budget.available_permits(), budget_permits() - 64);
    release.send(()).unwrap();
    // A generous cap that only a hang reaches.
    let freed = async {
        while budget.available_permits() != budget_permits() {
            tokio::task::yield_now().await;
        }
    };
    tokio::time::timeout(Duration::from_secs(60), freed)
        .await
        .expect("the share must be freed once the sealing task ends");
}

#[tokio::test]
async fn composition_waits_budget() {
    // A saturated budget holds a completion back before it loads any piece record.
    let context = setup_two_backends().await;
    let handler = context.blob_handle.handler.clone();
    let held = handler
        .pithos_budget
        .clone()
        .acquire_many_owned(budget_permits() as u32)
        .await
        .unwrap();
    let content = 3 * MAX_PART_SIZE;
    let mut waiting = Box::pin(handler.reserve_compose(content));
    assert!((&mut waiting).now_or_never().is_none());
    drop(held);
    let BlobEvent::ComposeReserved { share } = waiting.await else {
        panic!("the reservation must succeed once the budget has room")
    };
    assert_eq!(share.bytes, working_set(content).min(WORKING_SET));
    assert!(handler.pithos_budget.available_permits() < budget_permits());
    drop(share);
    assert_eq!(handler.pithos_budget.available_permits(), budget_permits());
}
