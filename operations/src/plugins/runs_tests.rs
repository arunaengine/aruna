//! Tests run records: admission within the queue bound, pages in creation order, and a run
//! that ends leaving the queue.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::plugin_runs::{RunState, RunTrigger};
use aruna_storage::FjallStorage;
use tempfile::{TempDir, tempdir};

/// A node context on its own empty storage.
pub(crate) fn opened() -> (TempDir, DriverContext) {
    let dir = tempdir().unwrap();
    let storage_handle = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let ctx = DriverContext {
        storage_handle,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    };
    (dir, ctx)
}

/// A queued run of `install_id`; ids grow with `seq`.
pub(crate) fn queued(install_id: Ulid, seq: u64) -> PluginRun {
    PluginRun {
        run_id: Ulid::from_parts(seq, 1),
        install_id,
        revision: 1,
        digest: format!("sha256:{}", "ab".repeat(32)),
        handler: "on_manual".into(),
        settings: "{}".into(),
        trigger: RunTrigger::Timer { occurrence_ms: seq },
        state: RunState::Queued,
        attempts: 0,
        due_ms: seq,
        created_ms: seq,
        finished_ms: None,
        message: None,
        output: None,
        log: Vec::new(),
    }
}

#[tokio::test]
async fn bounded_queue() {
    let (_dir, ctx) = opened();
    let (install, other) = (Ulid::from_parts(1, 1), Ulid::from_parts(1, 2));
    for seq in 0..RUNS_QUEUED as u64 {
        admit(&ctx, &queued(install, seq + 10)).await.unwrap();
    }
    let refused = admit(&ctx, &queued(install, 500)).await;
    assert_eq!(refused, Err(RunError::Full(RUNS_QUEUED)));
    assert_eq!(
        read_run(&ctx, install, Ulid::from_parts(500, 1)).await,
        Err(RunError::NotFound)
    );
    admit(&ctx, &queued(other, 600)).await.unwrap();

    // A run that ended frees its place, and stays readable.
    let mut done = read_run(&ctx, install, Ulid::from_parts(10, 1))
        .await
        .unwrap();
    done.finish(RunState::Succeeded, None, 700);
    save_run(&ctx, &done).await.unwrap();
    assert_eq!(read_run(&ctx, install, done.run_id).await.unwrap(), done);
    assert_eq!(unfinished(&ctx).await.unwrap().len(), RUNS_QUEUED);
    admit(&ctx, &queued(install, 501)).await.unwrap();
}

#[tokio::test]
async fn paged_runs() {
    let (_dir, ctx) = opened();
    let install = Ulid::from_parts(1, 1);
    for seq in [30, 10, 20] {
        admit(&ctx, &queued(install, seq)).await.unwrap();
    }
    admit(&ctx, &queued(Ulid::from_parts(1, 2), 15))
        .await
        .unwrap();
    let (first, next) = list_runs(&ctx, install, None, 2).await.unwrap();
    let ids: Vec<_> = first.iter().map(|run| run.due_ms).collect();
    assert_eq!(ids, [10, 20]);
    let (rest, last) = list_runs(&ctx, install, next, 2).await.unwrap();
    assert_eq!(rest.iter().map(|run| run.due_ms).collect::<Vec<_>>(), [30]);
    assert_eq!(last, None);
}
