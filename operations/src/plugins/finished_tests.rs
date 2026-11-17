//! Tests delivering finished jobs: one `job_finished` run after the starting run succeeded, a held
//! end with its rows kept, no run without the trigger, and an admin's retry or drop.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::driver::drive;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::plugin_runs::{RUN_RETENTION_MS, RunTrigger};
use aruna_core::structs::identity::admin_audit::AdminAuditAction;
use serde_json::json;

use super::*;
use crate::plugins::broker::call_intent;
use crate::plugins::install::read_config;
use crate::plugins::install::tests::Fixture;
use crate::plugins::runs::{list_runs, save_run};
use crate::plugins::submit::start_copy;
use crate::plugins::submit::tests::{path, ready, request, scope};
use crate::plugins::work::tests::edit_work;
use crate::plugins::work::{FinalState, prune_work, read_work, summary_of};
use crate::plugins::worker::{notify_work, prune_runs};
use crate::s3::object::copy::test::seed_bucket;

/// An install whose version copies and has a `job_finished` trigger, with buckets `in` and `out`.
async fn finisher(fixture: &Fixture) -> PluginInstall {
    let manifest = json!({ "permissions": { "required": ["objects.copy"] },
        "triggers": { "job_finished": {} } });
    let mut request = fixture.request(None);
    request.version = Some(fixture.publish("0.3.0", "ce", manifest).await);
    let data = format!("/{}/g/{}/data/**", fixture.actor.realm_id, fixture.group_id);
    let grants = [("read", data.clone()), ("write", data)];
    request.grants = (grants.into_iter())
        .map(|(grant, pattern)| (grant.to_string(), vec![pattern]))
        .collect();
    let install = fixture.install(&request).await.unwrap();
    for bucket in ["in", "out"] {
        seed_bucket(
            &fixture.ctx,
            bucket,
            install.group_id,
            install.account,
            Vec::new(),
        )
        .await;
    }
    install
}

/// A copy of `install` whose job succeeded, started by a run that ended as `ended`.
async fn settled(fixture: &Fixture, install: &PluginInstall, ended: RunState) -> Ulid {
    let scope = scope(fixture, install);
    let copy = request(fixture, install, "out/a.txt");
    let work_id = start_copy(&fixture.ctx, &scope, copy).await.unwrap();
    let mut run = scope.run.clone();
    run.finish(ended, None, 1);
    drive(save_run(&run).unwrap(), &fixture.ctx).await.unwrap();
    let settle = edit_work(&fixture.ctx, (install.install_id, work_id), |row| {
        let mut summary = summary_of(None).unwrap();
        (summary.state, summary.message) = (FinalState::Succeeded, None);
        (row.state, row.summary, row.settled_ms) = (WorkState::Settled, Some(summary), Some(1));
    });
    settle.await.unwrap();
    work_id
}

async fn notify(fixture: &Fixture) -> usize {
    let config = read_config(&fixture.ctx, fixture.actor.realm_id)
        .await
        .unwrap();
    notify_work(&fixture.ctx, &config, fixture.actor.node_id, 5)
        .await
        .unwrap()
}

/// The `job_finished` runs of `install`, as the job id and how it ended.
async fn finish_runs(fixture: &Fixture, install: &PluginInstall) -> Vec<(String, String)> {
    let (runs, _) = drive(list_runs(install.install_id, None, 10), &fixture.ctx)
        .await
        .unwrap();
    (runs.into_iter())
        .filter_map(|run| match run.trigger {
            RunTrigger::Event(event) if event.kind == SourceKind::JobFinished => {
                Some((event.resource, event.revision))
            }
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn delivers_after_success() {
    let fixture = Fixture::new().await;
    let install = finisher(&fixture).await;
    let work_id = settled(&fixture, &install, RunState::Succeeded).await;
    assert_eq!(notify(&fixture).await, 1);
    let expected = vec![(work_id.to_string(), "succeeded".to_string())];
    assert_eq!(finish_runs(&fixture, &install).await, expected);
    let (runs, _) = drive(list_runs(install.install_id, None, 10), &fixture.ctx)
        .await
        .unwrap();
    let RunTrigger::Event(event) = &runs.last().unwrap().trigger else {
        panic!("a job_finished run gets an event");
    };
    assert_eq!(event.path, path(&fixture, &install, "out/a.txt"));
    // Delivered once: a later pass admits nothing more.
    assert_eq!(notify(&fixture).await, 0);
    assert_eq!(finish_runs(&fixture, &install).await, expected);
}

#[tokio::test]
async fn holds_failed_runs() {
    // A failed starting run may not have written its mapping: the completion waits, and neither
    // the work row nor the run is pruned while it does.
    let fixture = Fixture::new().await;
    let install = finisher(&fixture).await;
    let work_id = settled(&fixture, &install, RunState::Failed).await;
    assert_eq!(notify(&fixture).await, 0);
    assert!(finish_runs(&fixture, &install).await.is_empty());
    let late = RUN_RETENTION_MS * 3;
    assert_eq!(prune_runs(&fixture.ctx, late).await.unwrap(), 0);
    assert_eq!(drive(prune_work(late), &fixture.ctx).await.unwrap(), 0);
    let row = drive(read_work(install.install_id, work_id), &fixture.ctx)
        .await
        .unwrap();
    assert!(row.is_some_and(|row| !row.notified));
}

#[tokio::test]
async fn notifies_without_trigger() {
    let fixture = Fixture::new().await;
    let install = ready(&fixture).await;
    let work_id = settled(&fixture, &install, RunState::Succeeded).await;
    assert_eq!(notify(&fixture).await, 1);
    assert!(finish_runs(&fixture, &install).await.is_empty());
    let row = drive(read_work(install.install_id, work_id), &fixture.ctx)
        .await
        .unwrap();
    assert!(row.is_some_and(|row| row.notified));
}

fn audit(fixture: &Fixture, action: AdminAuditAction) -> AdminAuditRecord {
    let actor = fixture.actor.user_id;
    AdminAuditRecord::new(action, actor, actor)
}

#[tokio::test]
async fn retry_releases_held() {
    // A retry queues the failed run again with its calls kept; once it succeeds, the held end
    // is delivered.
    let fixture = Fixture::new().await;
    let install = finisher(&fixture).await;
    let work_id = settled(&fixture, &install, RunState::Failed).await;
    let row = drive(read_work(install.install_id, work_id), &fixture.ctx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(waits_for(&row, Some(RunState::Failed)), Some(row.run_id));
    let retried = audit(&fixture, AdminAuditAction::PluginRunRetried);
    let ids = (install.install_id, row.run_id);
    let mut run = drive(retry_run(ids, &retried, 9).unwrap(), &fixture.ctx)
        .await
        .unwrap();
    assert_eq!(
        (run.state, run.attempts, run.due_ms),
        (RunState::Queued, 0, 9)
    );
    let again = drive(retry_run(ids, &retried, 9).unwrap(), &fixture.ctx).await;
    assert_eq!(again.map(drop), Err(RunError::NotRetried("queued")));
    assert_eq!(notify(&fixture).await, 0);
    run.finish(RunState::Succeeded, None, 10);
    drive(save_run(&run).unwrap(), &fixture.ctx).await.unwrap();
    assert_eq!(notify(&fixture).await, 1);
    assert_eq!(finish_runs(&fixture, &install).await.len(), 1);
}

#[tokio::test]
async fn drop_delivers_none() {
    let fixture = Fixture::new().await;
    let install = finisher(&fixture).await;
    let work_id = settled(&fixture, &install, RunState::Cancelled).await;
    let dropped = audit(&fixture, AdminAuditAction::PluginCompletionDropped);
    let ids = (install.install_id, work_id);
    let row = drive(drop_completion(ids, &dropped).unwrap(), &fixture.ctx)
        .await
        .unwrap();
    assert_eq!(waits_for(&row, Some(RunState::Cancelled)), None);
    let twice = drive(drop_completion(ids, &dropped).unwrap(), &fixture.ctx).await;
    assert_eq!(twice.map(drop), Err(RunError::NotHeld));
    assert_eq!(notify(&fixture).await, 0);
    assert!(finish_runs(&fixture, &install).await.is_empty());
    let missing = drop_completion((install.install_id, Ulid::nil()), &dropped).unwrap();
    let missing = drive(missing, &fixture.ctx).await;
    assert_eq!(missing.map(drop), Err(RunError::NotFound));
}

#[tokio::test]
async fn retry_redoes_undone() {
    // A call a person resolved as not done is made again by the retry; a settled one answers its
    // reply again.
    use aruna_plugins::broker::Intent;

    use crate::plugins::intents::{
        Begun, begin, intent_scan, resolved_rows, settled_row, unknown_row,
    };
    use crate::plugins::rows::{scan_rows, values_result, write_rows};
    let fixture = Fixture::new().await;
    let install = finisher(&fixture).await;
    let mut run = scope(&fixture, &install).run;
    let (settled, undone) = (Intent::new(1, &[b"put"]), Intent::new(2, &[b"copy"]));
    let ctx = &fixture.ctx;
    drive(begin(run.run_id, call_intent(&settled)), ctx)
        .await
        .unwrap();
    let reply = settled_row(run.run_id, &call_intent(&settled), "7".into()).unwrap();
    drive(write_rows(vec![reply]), ctx).await.unwrap();
    let unknown = unknown_row(run.run_id, &call_intent(&undone), "timed out").unwrap();
    drive(write_rows(vec![unknown]), ctx).await.unwrap();
    let intents = drive(scan_rows(intent_scan(run.run_id)), ctx)
        .await
        .unwrap();
    let resolved = resolved_rows(intents, false).unwrap();
    drive(write_rows(resolved), ctx).await.unwrap();
    run.finish(RunState::Failed, None, 1);
    drive(save_run(&run).unwrap(), ctx).await.unwrap();
    let retried = audit(&fixture, AdminAuditAction::PluginRunRetried);
    drive(
        retry_run((install.install_id, run.run_id), &retried, 9).unwrap(),
        ctx,
    )
    .await
    .unwrap();
    assert_eq!(
        drive(begin(run.run_id, call_intent(&undone)), ctx)
            .await
            .unwrap(),
        Begun::Fresh
    );
    assert_eq!(
        drive(begin(run.run_id, call_intent(&settled)), ctx)
            .await
            .unwrap(),
        Begun::Settled("7".into())
    );
}

/// A settled work row of a fixed install, and its two lookup answers.
fn settled_work() -> (WorkRow, [IoResult; 2]) {
    let install = crate::plugins::work::tests::sample_install(Ulid::from_parts(1, 1));
    let mut row = WorkRow::reserved(&install, WorkKind::Copy, Ulid::nil(), 1);
    row.state = WorkState::Settled;
    let answers = [
        values_result(vec![Some(b"key".to_vec())]),
        values_result(vec![Some(encode(&row).unwrap())]),
    ];
    (row, answers)
}

#[test]
fn notify_marks_once() {
    let (row, [by_index, by_key]) = settled_work();
    let mut plan = NotifyWork::new(Ulid::from_parts(1, 1), row.work_id, None);
    plan.first().unwrap();
    plan.next(by_index).unwrap();
    let Next::Call(IoCall::Write(rows)) = plan.next(by_key).unwrap() else {
        panic!("a manifest without the trigger marks the work notified");
    };
    let written: WorkRow = decode(&rows[0].2).unwrap();
    assert!(written.notified);
    assert_eq!(plan.next(IoResult::Done).unwrap(), Next::Done(true));
    // A notified work is left alone.
    let (mut done, _) = settled_work();
    done.notified = true;
    let mut again = NotifyWork::new(Ulid::from_parts(1, 1), done.work_id, None);
    again
        .next(values_result(vec![Some(b"key".to_vec())]))
        .unwrap();
    let stored = values_result(vec![Some(encode(&done).unwrap())]);
    assert_eq!(again.next(stored).unwrap(), Next::Done(false));
}

#[test]
fn drop_needs_held() {
    let (row, [by_index, by_key]) = settled_work();
    let mut plan = DropCompletion {
        lookup: WorkLookup::new(Ulid::from_parts(1, 1), row.work_id),
        audit: Vec::new(),
        dropped: None,
    };
    plan.next(by_index).unwrap();
    assert!(matches!(
        plan.next(by_key).unwrap(),
        Next::Call(IoCall::Write(_))
    ));
    let Next::Done(dropped) = plan.next(IoResult::Done).unwrap() else {
        panic!("the drop answers the row");
    };
    assert!(dropped.notified);
    let mut gone = DropCompletion {
        lookup: WorkLookup::new(Ulid::from_parts(1, 1), row.work_id),
        audit: Vec::new(),
        dropped: None,
    };
    assert_eq!(
        gone.next(values_result(vec![None])),
        Err(RunError::NotFound)
    );
}
