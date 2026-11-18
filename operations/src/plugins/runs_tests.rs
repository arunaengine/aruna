//! Tests run records: admission within the queue bound, pages in creation order, and a run
//! that ends leaving the queue.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::driver::{DriverContext, drive};
use crate::plugins::host_calls::call_intent;
use crate::plugins::rows::tests::child_result;
use crate::plugins::rows::tests::{page_result, values_result};
use crate::plugins::worker::prune_runs;
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
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    };
    (dir, ctx)
}

/// A queued run of `install_id`; ids grow with `seq`.
pub(crate) fn queued(install_id: Ulid, seq: u64) -> PluginRun {
    PluginRun {
        run_id: Ulid::from_parts(seq, 1),
        install_id,
        group_id: Ulid::from_parts(1, 9),
        plugin: "announcer".into(),
        version: "0.1.0".into(),
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
        drive(admit(queued(install, seq + 10)), &ctx).await.unwrap();
    }
    let refused = drive(admit(queued(install, 500)), &ctx).await;
    assert_eq!(refused, Err(RunError::Full(RUNS_QUEUED)));
    assert_eq!(
        drive(read_run(install, Ulid::from_parts(500, 1)), &ctx).await,
        Err(RunError::NotFound)
    );
    drive(admit(queued(other, 600)), &ctx).await.unwrap();

    // A run that ended frees its place, and stays readable.
    let mut done = drive(read_run(install, Ulid::from_parts(10, 1)), &ctx)
        .await
        .unwrap();
    done.finish(RunState::Succeeded, None, 700);
    drive(save_run(&done).unwrap(), &ctx).await.unwrap();
    assert_eq!(
        drive(read_run(install, done.run_id), &ctx).await.unwrap(),
        done
    );
    assert_eq!(drive(unfinished(), &ctx).await.unwrap().len(), RUNS_QUEUED);
    drive(admit(queued(install, 501)), &ctx).await.unwrap();
}

#[tokio::test]
async fn paged_runs() {
    let (_dir, ctx) = opened();
    let install = Ulid::from_parts(1, 1);
    for seq in [30, 10, 20] {
        drive(admit(queued(install, seq)), &ctx).await.unwrap();
    }
    drive(admit(queued(Ulid::from_parts(1, 2), 15)), &ctx)
        .await
        .unwrap();
    let (first, next) = drive(list_runs(install, None, 2), &ctx).await.unwrap();
    let ids: Vec<_> = first.iter().map(|run| run.due_ms).collect();
    assert_eq!(ids, [10, 20]);
    let (rest, last) = drive(list_runs(install, next, 2), &ctx).await.unwrap();
    assert_eq!(rest.iter().map(|run| run.due_ms).collect::<Vec<_>>(), [30]);
    assert_eq!(last, None);
}

#[tokio::test]
async fn pruned_runs() {
    use crate::plugins::intents::{begin, open_intent, settled_row};
    use crate::plugins::rows::write_rows;
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
        drive(admit(run.clone()), &ctx).await.unwrap();
        let finished = if seq == 3 { 50 * RUN_RETENTION_MS } else { 10 };
        run.finish(state, None, finished);
        drive(save_run(&run).unwrap(), &ctx).await.unwrap();
        let intent = Intent::new(0, &[b"x"]);
        drive(begin(run.run_id, call_intent(&intent)), &ctx)
            .await
            .unwrap();
        let settled = settled_row(run.run_id, &call_intent(&intent), "r".into()).unwrap();
        drive(write_rows(vec![settled]), &ctx).await.unwrap();
        ended.push(run);
    }
    let waiting = queued(install, 4);
    drive(admit(waiting.clone()), &ctx).await.unwrap();
    let now = 10 + RUN_RETENTION_MS;
    assert_eq!(prune_runs(&ctx, now).await.unwrap(), 1);
    assert_eq!(
        drive(read_run(install, ended[0].run_id), &ctx).await,
        Err(RunError::NotFound)
    );
    assert_eq!(
        drive(open_intent(ended[0].run_id), &ctx).await.unwrap(),
        None
    );
    for kept in [&ended[1], &ended[2], &waiting] {
        assert!(drive(read_run(install, kept.run_id), &ctx).await.is_ok());
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
    drive(admit(run.clone()), &ctx).await.unwrap();
    let admin = UserId::local(Ulid::from_parts(3, 3), RealmId::from_bytes([1u8; 32]));
    let audit = AdminAuditRecord::new(AdminAuditAction::PluginRunResolved, admin, admin);
    let early = drive(
        resolve_run(
            (install, run.run_id),
            true,
            &audit,
            aruna_core::time::unix_timestamp_millis(),
        )
        .unwrap(),
        &ctx,
    )
    .await;
    assert_eq!(early, Err(RunError::NotWaiting("queued")));
    run.finish(
        RunState::NeedsAttention,
        Some("call 0 is unknown".into()),
        5,
    );
    drive(save_run(&run).unwrap(), &ctx).await.unwrap();
    let resolved = drive(
        resolve_run(
            (install, run.run_id),
            true,
            &audit,
            aruna_core::time::unix_timestamp_millis(),
        )
        .unwrap(),
        &ctx,
    )
    .await
    .unwrap();
    assert_eq!(resolved.state, RunState::Succeeded);
    assert!(resolved.message.unwrap_or_default().ends_with("as done"));
    let again = drive(
        resolve_run(
            (install, run.run_id),
            false,
            &audit,
            aruna_core::time::unix_timestamp_millis(),
        )
        .unwrap(),
        &ctx,
    )
    .await;
    assert_eq!(again, Err(RunError::NotWaiting("succeeded")));
}

#[tokio::test]
async fn keeps_open_calls() {
    use crate::plugins::intents::unknown_row;
    use crate::plugins::rows::write_rows;
    use aruna_core::plugin_runs::RUN_RETENTION_MS;
    use aruna_plugins::broker::Intent;
    let (_dir, ctx) = opened();
    let install = Ulid::from_parts(1, 1);
    let mut run = queued(install, 1);
    drive(admit(run.clone()), &ctx).await.unwrap();
    run.finish(RunState::Failed, None, 10);
    drive(save_run(&run).unwrap(), &ctx).await.unwrap();
    let unknown = unknown_row(run.run_id, &call_intent(&Intent::new(0, &[b"x"])), "lost").unwrap();
    drive(write_rows(vec![unknown]), &ctx).await.unwrap();
    assert_eq!(prune_runs(&ctx, 10 + RUN_RETENTION_MS).await.unwrap(), 0);
    assert!(drive(read_run(install, run.run_id), &ctx).await.is_ok());
}

#[tokio::test]
async fn unresolved_count() {
    // A run that needs attention keeps its place until a person resolves it.
    use aruna_core::UserId;
    use aruna_core::structs::identity::admin_audit::{AdminAuditAction, AdminAuditRecord};
    use aruna_core::structs::identity::realm::RealmId;
    let (_dir, ctx) = opened();
    let install = Ulid::from_parts(1, 1);
    let mut waiting = queued(install, 1);
    drive(admit(waiting.clone()), &ctx).await.unwrap();
    waiting.finish(
        RunState::NeedsAttention,
        Some("call 0 is unknown".into()),
        5,
    );
    drive(save_run(&waiting).unwrap(), &ctx).await.unwrap();
    for seq in 2..=RUNS_QUEUED as u64 {
        drive(admit(queued(install, seq)), &ctx).await.unwrap();
    }
    let refused = drive(admit(queued(install, 500)), &ctx).await;
    assert_eq!(refused, Err(RunError::Full(RUNS_QUEUED)));
    let running = drive(unfinished(), &ctx).await.unwrap();
    assert_eq!(running.len(), RUNS_QUEUED - 1);
    assert!(!running.contains(&(install, waiting.run_id)));
    let admin = UserId::local(Ulid::from_parts(3, 3), RealmId::from_bytes([1u8; 32]));
    let audit = AdminAuditRecord::new(AdminAuditAction::PluginRunResolved, admin, admin);
    drive(
        resolve_run(
            (install, waiting.run_id),
            false,
            &audit,
            aruna_core::time::unix_timestamp_millis(),
        )
        .unwrap(),
        &ctx,
    )
    .await
    .unwrap();
    drive(admit(queued(install, 500)), &ctx).await.unwrap();
}

#[tokio::test]
async fn resolved_expire() {
    // A resolved run and its intents leave with the retention like any finished run.
    use crate::plugins::intents::{open_intent, unknown_row, written};
    use crate::plugins::rows::write_rows;
    use aruna_core::UserId;
    use aruna_core::plugin_runs::RUN_RETENTION_MS;
    use aruna_core::structs::identity::admin_audit::{AdminAuditAction, AdminAuditRecord};
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_plugins::broker::Intent;
    let (_dir, ctx) = opened();
    let install = Ulid::from_parts(1, 1);
    let mut run = queued(install, 1);
    drive(admit(run.clone()), &ctx).await.unwrap();
    let intent = Intent::new(0, &[b"x"]);
    let unknown = unknown_row(run.run_id, &call_intent(&intent), "lost").unwrap();
    drive(write_rows(vec![unknown]), &ctx).await.unwrap();
    run.finish(
        RunState::NeedsAttention,
        Some("call 0 is unknown".into()),
        5,
    );
    drive(save_run(&run).unwrap(), &ctx).await.unwrap();
    let admin = UserId::local(Ulid::from_parts(3, 3), RealmId::from_bytes([1u8; 32]));
    let audit = AdminAuditRecord::new(AdminAuditAction::PluginRunResolved, admin, admin);
    let resolved = drive(
        resolve_run(
            (install, run.run_id),
            true,
            &audit,
            aruna_core::time::unix_timestamp_millis(),
        )
        .unwrap(),
        &ctx,
    )
    .await
    .unwrap();
    assert_eq!(drive(open_intent(run.run_id), &ctx).await.unwrap(), None);
    let later = resolved.finished_ms.unwrap() + RUN_RETENTION_MS;
    assert_eq!(prune_runs(&ctx, later).await.unwrap(), 1);
    assert_eq!(
        drive(read_run(install, run.run_id), &ctx).await,
        Err(RunError::NotFound)
    );
    assert!(
        !drive(written(run.run_id, &call_intent(&intent)), &ctx)
            .await
            .unwrap()
    );
}

/// A page answer of `count` queue rows.
fn queue_page(count: usize) -> IoResult {
    let rows = (0..count)
        .map(|seq| (vec![seq as u8], Vec::new()))
        .collect();
    page_result(rows, None)
}

#[test]
fn admission_bound() {
    let install = Ulid::from_parts(1, 1);
    let run = queued(install, 1);
    let mut admission = Admission::new(run.clone());
    let IoCall::Page { space, prefix, .. } = admission.ask() else {
        panic!("an admission counts the install's queue");
    };
    assert_eq!(
        (space.as_str(), prefix.to_vec()),
        (PLUGIN_QUEUE_KEYSPACE, install.to_bytes().to_vec())
    );
    let rows = admission
        .take(queue_page(RUNS_QUEUED - 1))
        .unwrap()
        .unwrap();
    let keys: Vec<_> = rows
        .iter()
        .map(|(space, key, _)| (space.clone(), key.to_vec()))
        .collect();
    assert_eq!(
        keys,
        [
            (
                PLUGIN_QUEUE_KEYSPACE.to_string(),
                queue_key(install, run.run_id)
            ),
            (
                PLUGIN_RUNS_KEYSPACE.to_string(),
                run_key(install, run.run_id)
            ),
        ]
    );
    let mut full = Admission::new(run);
    assert_eq!(
        full.take(queue_page(RUNS_QUEUED)),
        Err(RunError::Full(RUNS_QUEUED))
    );
    assert!(full.take(IoResult::Done).is_err());
}

#[test]
fn resolve_decisions() {
    use aruna_core::UserId;
    use aruna_core::structs::identity::realm::RealmId;
    let install = Ulid::from_parts(1, 1);
    let mut waiting = queued(install, 1);
    waiting.state = RunState::NeedsAttention;
    let audit = AdminAuditRecord::new(
        aruna_core::structs::identity::admin_audit::AdminAuditAction::PluginRunResolved,
        UserId::local(Ulid::from_parts(2, 2), RealmId::from_bytes([1; 32])),
        UserId::local(Ulid::from_parts(2, 3), RealmId::from_bytes([1; 32])),
    );
    let ids = (install, waiting.run_id);
    let mut resolve = resolve_run(ids, false, &audit, 50).unwrap();
    assert!(matches!(
        resolve.first().unwrap(),
        Next::Call(IoCall::Read(_))
    ));
    let mut plan = ResolveRun {
        key: run_key(install, waiting.run_id),
        done: false,
        message: "a resolved it as not done".into(),
        now_ms: 50,
        audit: Vec::new(),
        phase: ResolvePhase::Reading,
    };
    // A run that does not wait for a person is refused.
    let running = values_result(vec![Some(encode(&queued(install, 1)).unwrap())]);
    assert_eq!(plan.next(running), Err(RunError::NotWaiting("queued")));
    plan.phase = ResolvePhase::Reading;
    assert_eq!(
        plan.next(values_result(vec![None])),
        Err(RunError::NotFound)
    );
    plan.phase = ResolvePhase::Reading;
    let stored = values_result(vec![Some(encode(&waiting).unwrap())]);
    assert!(matches!(
        plan.next(stored).unwrap(),
        Next::Call(IoCall::Page { .. })
    ));
    let Next::Call(IoCall::Write(rows)) = plan.next(page_result(Vec::new(), None)).unwrap() else {
        panic!("the resolved run is written");
    };
    let saved: PluginRun = decode(&rows[0].2).unwrap();
    assert_eq!(
        (saved.state, saved.finished_ms),
        (RunState::Failed, Some(50))
    );
    let Next::Call(IoCall::Delete(keys)) = plan.next(IoResult::Done).unwrap() else {
        panic!("an ended run leaves the queue");
    };
    let keys: Vec<_> = keys
        .iter()
        .map(|(space, key)| (space.as_str(), key.to_vec()))
        .collect();
    assert_eq!(
        keys,
        [(PLUGIN_QUEUE_KEYSPACE, queue_key(install, waiting.run_id))]
    );
    assert_eq!(plan.next(IoResult::Done).unwrap(), Next::Done(saved));
}

#[test]
fn drop_deletes_intents() {
    let install = Ulid::from_parts(1, 1);
    let run = queued(install, 1);
    let mut plan = DropRun {
        key: run_key(install, run.run_id),
        intents: intent_scan(run.run_id),
        phase: DropPhase::Run,
    };
    let first = plan.first().unwrap();
    assert_eq!(
        first,
        Next::Call(delete_call(vec![(
            PLUGIN_RUNS_KEYSPACE,
            run_key(install, run.run_id)
        )]))
    );
    assert!(matches!(
        plan.next(IoResult::Done).unwrap(),
        Next::Call(IoCall::Page { .. })
    ));
    let intents = vec![(b"i1".to_vec(), Vec::new())];
    let Next::Call(IoCall::Delete(keys)) = plan.next(page_result(intents, None)).unwrap() else {
        panic!("the run's intents are deleted");
    };
    assert_eq!(keys.len(), 1);
    assert_eq!(plan.next(IoResult::Done).unwrap(), Next::Done(()));
}

#[test]
fn expired_runs_listed() {
    let install = Ulid::from_parts(1, 1);
    let mut ended = queued(install, 1);
    ended.finish(RunState::Succeeded, None, 1);
    let mut waiting = queued(install, 2);
    waiting.finish(RunState::NeedsAttention, None, 1);
    let mut plan = ExpiredRuns {
        now_ms: 1 + RUN_RETENTION_MS,
        phase: ExpiredPhase::Runs(Scan::new(PLUGIN_RUNS_KEYSPACE, vec![RUN_ROW])),
        prunable: Vec::new(),
    };
    plan.first().unwrap();
    let rows = vec![
        (b"r1".to_vec(), encode(&ended).unwrap()),
        (b"r2".to_vec(), encode(&waiting).unwrap()),
    ];
    // The work rows hold no completion, so the ended run's intents are read next.
    assert!(matches!(
        plan.next(page_result(rows, None)).unwrap(),
        Next::Call(IoCall::Page { .. })
    ));
    assert!(matches!(
        plan.next(page_result(Vec::new(), None)).unwrap(),
        Next::Call(IoCall::Page { .. })
    ));
    let done = plan.next(page_result(Vec::new(), None)).unwrap();
    assert_eq!(done, Next::Done(vec![(install, ended.run_id)]));
}

#[test]
fn prunes_each_expired() {
    let ids = (Ulid::from_parts(1, 1), Ulid::from_parts(2, 2));
    let mut prune = PruneRuns::new(9);
    assert!(matches!(prune.first(), Ok(Next::Call(IoCall::Child(_)))));
    let listed = child_result(Ok::<_, RunError>(vec![ids]));
    assert!(matches!(
        prune.next(listed),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let dropped = child_result(Ok::<(), StoreError>(()));
    assert_eq!(prune.next(dropped), Ok(Next::Done(1)));
    let mut empty = PruneRuns::new(9);
    let none = child_result(Ok::<Vec<(Ulid, Ulid)>, RunError>(Vec::new()));
    assert_eq!(empty.next(none), Ok(Next::Done(0)));
}
