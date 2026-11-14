//! Tests the checks of every plugin work attempt: a missing or rebound work ends, a paused install
//! waits, and a grant that no longer covers a path refuses before any byte moves.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::execution::job::{JobErrorKind, JobId, JobProgress};
use serde_json::json;
use tokio_util::sync::CancellationToken;
use ulid::Ulid;

use super::*;
use crate::driver::DriverContext;
use crate::jobs::executor::ProgressReporter;
use crate::plugins::bound::tests::edit_config;
use crate::plugins::install::tests::Fixture;
use crate::plugins::work::{Identity, WorkKind, WorkRow, reserve};

pub(crate) fn job_context(ctx: &DriverContext, fixture: &Fixture) -> JobContext {
    JobContext {
        driver: Arc::new(ctx.clone()),
        job_id: JobId::from_bytes([7u8; 16]),
        owner_node_id: fixture.actor.node_id,
        claim_token: Ulid::generate(),
        final_attempt: false,
        cancel: CancellationToken::new(),
        shutdown: CancellationToken::new(),
        progress: ProgressReporter::from_progress(&JobProgress::new("bytes")),
        range_turns: Default::default(),
        mail: Default::default(),
    }
}

/// An active install of a version that requests `objects.copy`, granted on the whole group.
pub(crate) async fn copier(fixture: &Fixture) -> PluginInstall {
    let copy = json!({ "permissions": { "required": ["objects.copy"] } });
    let digest = fixture.publish("0.2.0", "cd", copy).await;
    let mut request = fixture.request(None);
    request.version = Some(digest);
    fixture.install(&request).await.unwrap()
}

/// A recorded copy of `install` between two object paths of its group on this node.
pub(crate) async fn recorded(
    fixture: &Fixture,
    install: &PluginInstall,
    (source, target): (&str, &str),
) -> PluginWorkSpec {
    let mut row = WorkRow::reserved(install, WorkKind::Copy, Ulid::generate(), 1);
    row.version_id = Some(Ulid::generate());
    let identity = Identity::Key(format!("{source}>{target}"));
    let (row, _) = reserve(&fixture.ctx, (install, &identity), row)
        .await
        .unwrap();
    let data = format!("/{}/g/{}/data", fixture.actor.realm_id, install.group_id);
    let node = fixture.actor.node_id;
    PluginWorkSpec {
        realm_id: fixture.actor.realm_id,
        install_id: install.install_id,
        work_id: row.work_id,
        group_id: install.group_id,
        action: PluginWorkAction::Copy {
            source: format!("{data}/{node}/{source}"),
            source_version: None,
            target: format!("{data}/{node}/{target}"),
            version_id: row.version_id.unwrap(),
        },
    }
}

fn failure(outcome: JobRunOutcome) -> (JobErrorKind, String) {
    match outcome {
        JobRunOutcome::Failed(error) => (error.kind, error.message),
        other => panic!("expected a failure, got {}", outcome_name(&other)),
    }
}

fn outcome_name(outcome: &JobRunOutcome) -> &'static str {
    match outcome {
        JobRunOutcome::Succeeded(_) => "succeeded",
        JobRunOutcome::Failed(_) => "failed",
        JobRunOutcome::Deferred(_) => "deferred",
        JobRunOutcome::DeferredFor(..) => "deferred for",
        JobRunOutcome::Cancelled => "cancelled",
        JobRunOutcome::Interrupted => "interrupted",
        JobRunOutcome::AwaitingKey(_) => "awaiting key",
    }
}

#[tokio::test]
async fn refuses_changed_work() {
    let fixture = Fixture::new().await;
    let install = copier(&fixture).await;
    let ctx = job_context(&fixture.ctx, &fixture);
    let spec = recorded(&fixture, &install, ("in/a", "out/a")).await;
    let mut unknown = spec.clone();
    unknown.work_id = Ulid::generate();
    let (kind, message) = failure(run_plugin_work(&ctx, &unknown).await);
    assert_eq!(kind, JobErrorKind::Permanent);
    assert!(message.contains("no longer recorded"), "{message}");
    // The install moved to another artifact after the work was recorded.
    edit_config(&fixture, |config| {
        config.installs[0].digest = "sha256:00".into()
    })
    .await;
    let (kind, message) = failure(run_plugin_work(&ctx, &spec).await);
    assert_eq!(kind, JobErrorKind::Permanent);
    assert!(message.contains("another version"), "{message}");
}

#[tokio::test]
async fn paused_install_waits() {
    let fixture = Fixture::new().await;
    let install = copier(&fixture).await;
    let ctx = job_context(&fixture.ctx, &fixture);
    let spec = recorded(&fixture, &install, ("in/a", "out/a")).await;
    edit_config(&fixture, |config| config.installs[0].paused = true).await;
    let outcome = run_plugin_work(&ctx, &spec).await;
    assert!(
        matches!(outcome, JobRunOutcome::DeferredFor(_, wait) if wait == PAUSED_RETRY),
        "{}",
        outcome_name(&outcome)
    );
}

#[tokio::test]
async fn narrowed_grant_refuses() {
    let fixture = Fixture::new().await;
    let install = copier(&fixture).await;
    let ctx = job_context(&fixture.ctx, &fixture);
    let spec = recorded(&fixture, &install, ("in/a", "out/a")).await;
    let PluginWorkAction::Copy { source, .. } = &spec.action;
    let only_source = vec![source.clone()];
    edit_config(&fixture, |config| {
        let grants = &mut config.installs[0].grants;
        grants.insert("objects.copy".into(), only_source);
    })
    .await;
    let (kind, message) = failure(run_plugin_work(&ctx, &spec).await);
    assert_eq!(kind, JobErrorKind::Permanent);
    assert!(message.contains("is not granted"), "{message}");
}
