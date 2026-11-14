//! Tests delivering finished jobs: after the starting run succeeded one `job_finished` run, a
//! failed starting run holds the completion and its rows, and no trigger means no run.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::plugin_runs::{RUN_RETENTION_MS, RunTrigger};
use serde_json::json;

use super::*;
use crate::plugins::install::read_config;
use crate::plugins::install::tests::Fixture;
use crate::plugins::runs::{list_runs, prune_runs, save_run};
use crate::plugins::submit::start_copy;
use crate::plugins::submit::tests::{path, ready, request, scope};
use crate::plugins::work::{FinalState, prune_work, read_work, summary_of, update_work};
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
    save_run(&fixture.ctx, &run).await.unwrap();
    let settle = update_work(&fixture.ctx, (install.install_id, work_id), |row| {
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
    let (runs, _) = list_runs(&fixture.ctx, install.install_id, None, 10)
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
    let (runs, _) = list_runs(&fixture.ctx, install.install_id, None, 10)
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
    assert_eq!(prune_work(&fixture.ctx, late).await.unwrap(), 0);
    let row = read_work(&fixture.ctx, install.install_id, work_id)
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
    let row = read_work(&fixture.ctx, install.install_id, work_id)
        .await
        .unwrap();
    assert!(row.is_some_and(|row| row.notified));
}
