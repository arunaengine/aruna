//! Tests starting runs: a manual run, its saved invocation and refusals, timer occurrences queued
//! once each and skipped at the bound, and the stored trigger kind indexes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::driver::drive;
use crate::plugins::install::tests::Fixture;
use crate::plugins::rows::{decode, page_result, values_result};
use crate::plugins::runs::{TimerRow, list_runs, read_run, read_timer};
use crate::plugins::worker::tests::installed;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::plugin_runs::RUNS_QUEUED;
use aruna_core::plugin_runs::SourceKind;

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
    // The fixture's manual trigger has no form, so only empty input fits it.
    let refused = start(r#"{"dataset":"d1"}"#).await;
    let unknown = SettingError::Unknown("dataset".into());
    assert!(matches!(refused, Err(StartError::Form(error)) if error == unknown));
    for input in ["[]", "1", "{"] {
        assert!(
            matches!(start(input).await, Err(StartError::Input)),
            "{input}"
        );
    }
    let run = start("{}").await.unwrap();
    let stored = drive(read_run(install.install_id, run.run_id), ctx)
        .await
        .unwrap();
    assert_eq!(stored, run);
    assert_eq!(
        (run.handler.as_str(), run.revision),
        ("manual", install.revision)
    );
    assert_eq!(run.settings, install.settings);
    let by = fixture.actor.user_id;
    let input = "{}".to_string();
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
    let (runs, _) = drive(list_runs(install.install_id, None, 10), ctx)
        .await
        .unwrap();
    let occurrence_ms = active + 3 * HOUR;
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].trigger, RunTrigger::Timer { occurrence_ms });
    assert_eq!(runs[0].handler, "timer");
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
    let row = drive(read_timer(install.install_id), ctx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        row,
        TimerRow {
            last_due_ms: active + 4 * HOUR,
            skipped: 1
        }
    );
}

// Coverage rows store trigger kinds by index; the update kind was appended, so a row written
// before it keeps its meaning, and every event kind starts the trigger of its own name.
#[test]
fn trigger_indexes_kept() {
    let index = |kind: TriggerKind| postcard::to_allocvec(&kind).unwrap();
    assert_eq!(index(TriggerKind::SyncFailed), [6]);
    assert_eq!(index(TriggerKind::DatasetUpdated), [7]);
    for kind in [
        SourceKind::DatasetCreated,
        SourceKind::DataUploaded,
        SourceKind::DatasetUpdated,
    ] {
        assert_eq!(event_trigger(kind).as_str(), kind.name());
    }
}

#[test]
fn timer_occurrences() {
    let install = crate::plugins::work::tests::sample_install(Ulid::from_parts(1, 1));
    let plan = |now_ms| enqueue_timer(&install, "every".into(), 10, now_ms);
    // Before the first interval since activation nothing is due.
    let mut early = plan(14);
    assert!(matches!(
        early.first().unwrap(),
        Next::Call(IoCall::Read(_))
    ));
    assert_eq!(
        early.next(values_result(vec![None])).unwrap(),
        Next::Done(false)
    );
    // Missed occurrences collapse into the latest; a full queue skips it.
    let mut due = plan(37);
    due.first().unwrap();
    assert!(matches!(
        due.next(values_result(vec![None])).unwrap(),
        Next::Call(IoCall::Page { .. })
    ));
    let full = (0..RUNS_QUEUED)
        .map(|seq| (vec![seq as u8], Vec::new()))
        .collect();
    let Next::Call(IoCall::Write(rows)) = due.next(page_result(full, None)).unwrap() else {
        panic!("the timer row is written");
    };
    let row: TimerRow = decode(&rows[0].2).unwrap();
    assert_eq!((row.last_due_ms, row.skipped), (35, 1));
    assert_eq!(due.next(IoResult::Done).unwrap(), Next::Done(false));
    let mut queued = plan(37);
    queued.first().unwrap();
    queued.next(values_result(vec![None])).unwrap();
    let Next::Call(IoCall::Write(rows)) = queued.next(page_result(Vec::new(), None)).unwrap()
    else {
        panic!("the run and the timer row are written");
    };
    assert_eq!(rows.len(), 3);
    assert_eq!(queued.next(IoResult::Done).unwrap(), Next::Done(true));
}
