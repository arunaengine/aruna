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

#[tokio::test]
async fn pruned_runs() {
    use crate::plugins::intents::{begin, open_intent, settle_in};
    use aruna_core::plugin_runs::RUN_RETENTION_MS;
    use aruna_plugins::broker::Intent;
    let (_dir, ctx) = opened();
    let install = Ulid::from_parts(1, 1);
    let mut ended = Vec::new();
    for (seq, state) in [
        (1, RunState::Succeeded),
        (2, RunState::NeedsAttention),
        (3, RunState::Failed),
    ] {
        let mut run = queued(install, seq);
        admit(&ctx, &run).await.unwrap();
        let finished = if seq == 3 { 50 * RUN_RETENTION_MS } else { 10 };
        run.finish(state, None, finished);
        save_run(&ctx, &run).await.unwrap();
        let intent = Intent::new(0, None, &[b"x"]);
        begin(&ctx, run.run_id, &intent).await.unwrap();
        let settle = async |txn: &Txn<'_>| settle_in(txn, run.run_id, &intent, "r".into()).await;
        transact(&ctx, settle).await.unwrap();
        ended.push(run);
    }
    let waiting = queued(install, 4);
    admit(&ctx, &waiting).await.unwrap();
    let now = 10 + RUN_RETENTION_MS;
    assert_eq!(prune_runs(&ctx, now).await.unwrap(), 1);
    assert_eq!(
        read_run(&ctx, install, ended[0].run_id).await,
        Err(RunError::NotFound)
    );
    assert_eq!(open_intent(&ctx, ended[0].run_id).await.unwrap(), None);
    for kept in [&ended[1], &ended[2], &waiting] {
        assert!(read_run(&ctx, install, kept.run_id).await.is_ok());
    }
}

#[tokio::test]
async fn resolved_runs() {
    use aruna_core::UserId;
    use aruna_core::structs::identity::admin_audit::{AdminAuditAction, AdminAuditRecord};
    use aruna_core::structs::identity::realm::RealmId;
    let (_dir, ctx) = opened();
    let install = Ulid::from_parts(1, 1);
    let mut run = queued(install, 1);
    admit(&ctx, &run).await.unwrap();
    let admin = UserId::local(Ulid::from_parts(3, 3), RealmId::from_bytes([1u8; 32]));
    let audit = AdminAuditRecord::new(AdminAuditAction::PluginRunResolved, admin, admin);
    let early = resolve_run(&ctx, install, run.run_id, true, &audit).await;
    assert_eq!(early, Err(RunError::NotWaiting("queued")));
    run.finish(
        RunState::NeedsAttention,
        Some("call 0 is unknown".into()),
        5,
    );
    save_run(&ctx, &run).await.unwrap();
    let resolved = resolve_run(&ctx, install, run.run_id, true, &audit)
        .await
        .unwrap();
    assert_eq!(resolved.state, RunState::Succeeded);
    assert!(resolved.message.unwrap_or_default().ends_with("as done"));
    let again = resolve_run(&ctx, install, run.run_id, false, &audit).await;
    assert_eq!(again, Err(RunError::NotWaiting("succeeded")));
}

#[tokio::test]
async fn keeps_open_calls() {
    use crate::plugins::intents::mark_unknown;
    use aruna_core::plugin_runs::RUN_RETENTION_MS;
    use aruna_plugins::broker::Intent;
    let (_dir, ctx) = opened();
    let install = Ulid::from_parts(1, 1);
    let mut run = queued(install, 1);
    admit(&ctx, &run).await.unwrap();
    run.finish(RunState::Failed, None, 10);
    save_run(&ctx, &run).await.unwrap();
    mark_unknown(&ctx, run.run_id, &Intent::new(0, None, &[b"x"]), "lost")
        .await
        .unwrap();
    assert_eq!(prune_runs(&ctx, 10 + RUN_RETENTION_MS).await.unwrap(), 0);
    assert!(read_run(&ctx, install, run.run_id).await.is_ok());
}
