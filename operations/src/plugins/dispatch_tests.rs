//! Tests starting runs: a manual run with its saved invocation and the refusals before it, and
//! timer occurrences queued once each, counted from activation and skipped at the bound.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::plugins::install::tests::Fixture;
use crate::plugins::runs::{list_runs, read_run};
use crate::plugins::worker::tests::installed;
use aruna_core::plugin_runs::RUNS_QUEUED;

const HOUR: u64 = 3600 * 1000;

#[tokio::test]
async fn manual_runs() {
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let (ctx, realm, local) = (&fixture.ctx, fixture.actor.realm_id, fixture.actor.node_id);
    let start = |input: &'static str| {
        let auth = fixture.auth();
        async move { start_run(ctx, &auth, realm, local, install.install_id, input, 5).await }
    };
    let run = start(r#"{"dataset":"d1"}"#).await.unwrap();
    let stored = read_run(ctx, install.install_id, run.run_id).await.unwrap();
    assert_eq!(stored, run);
    assert_eq!(
        (run.handler.as_str(), run.revision),
        ("probe", install.revision)
    );
    assert_eq!(run.settings, install.settings);
    let by = fixture.actor.user_id;
    let input = r#"{"dataset":"d1"}"#.to_string();
    assert_eq!(run.trigger, RunTrigger::Manual { by, input });
    assert!(matches!(start("not json").await, Err(StartError::Input)));

    let other = iroh::SecretKey::from_bytes(&[9u8; 32]).public();
    let auth = fixture.auth();
    let elsewhere = start_run(ctx, &auth, realm, other, install.install_id, "{}", 5).await;
    assert!(matches!(elsewhere, Err(StartError::Elsewhere(node)) if node == local));
    let plain = fixture.install(&fixture.request(None)).await.unwrap();
    let none = start_run(ctx, &auth, realm, local, plain.install_id, "{}", 5).await;
    assert!(matches!(none, Err(StartError::NoTrigger("manual"))));
}

#[tokio::test]
async fn timer_occurrences() {
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let ctx = &fixture.ctx;
    let config = read_config(ctx, fixture.actor.realm_id).await.unwrap();
    let local = fixture.actor.node_id;
    let active = install.active_since_ms.unwrap();
    assert_eq!(
        enqueue_timers(ctx, &config, local, active + HOUR - 1)
            .await
            .unwrap(),
        0
    );
    // Three missed hours collapse into the latest occurrence, queued once.
    assert_eq!(
        enqueue_timers(ctx, &config, local, active + 3 * HOUR + 7)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        enqueue_timers(ctx, &config, local, active + 3 * HOUR + 9)
            .await
            .unwrap(),
        0
    );
    let (runs, _) = list_runs(ctx, install.install_id, None, 10).await.unwrap();
    let occurrence_ms = active + 3 * HOUR;
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].trigger, RunTrigger::Timer { occurrence_ms });
    assert_eq!(runs[0].handler, "chatty");
    let other = iroh::SecretKey::from_bytes(&[9u8; 32]).public();
    assert_eq!(
        enqueue_timers(ctx, &config, other, active + 9 * HOUR)
            .await
            .unwrap(),
        0
    );

    // At the queue bound an occurrence is skipped and counted, and the timer moves on.
    for n in 1..RUNS_QUEUED as u64 {
        let auth = fixture.auth();
        let realm = fixture.actor.realm_id;
        start_run(ctx, &auth, realm, local, install.install_id, "{}", n)
            .await
            .unwrap();
    }
    assert_eq!(
        enqueue_timers(ctx, &config, local, active + 4 * HOUR)
            .await
            .unwrap(),
        0
    );
    let row = read_timer(ctx, install.install_id).await.unwrap().unwrap();
    assert_eq!(
        row,
        TimerRow {
            last_due_ms: active + 4 * HOUR,
            skipped: 1
        }
    );
}
