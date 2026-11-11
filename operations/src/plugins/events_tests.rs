//! Tests event delivery across its stages: a restart with rows at every stage ends with one run
//! per event, a narrowed grant stops delivery, and pause and removal deny or drop pending rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::keyspaces::{DOCUMENT_STATE_KEYSPACE, PLUGIN_OUTBOX_KEYSPACE};
use aruna_core::plugin_installs::{InstallState, PluginInstall};
use aruna_core::plugin_runs::{ForwardRow, RunTrigger, SourceEvent};
use aruna_core::storage_entries::reducer_state_key;
use aruna_core::time::unix_timestamp_millis;

use crate::plugins::decide::decide_batch;
use crate::plugins::forward::{Backoff, Decisions, EventBatch, Outcome, send_outbox};
use crate::plugins::install::tests::Fixture;
use crate::plugins::lifecycle::{pause_install, set_grants};
use crate::plugins::matcher::match_pending;
use crate::plugins::matcher::tests::{config, forwarded, journal};
use crate::plugins::replay::plan_replays;
use crate::plugins::runs::list_runs;
use crate::plugins::store::Txn;
use crate::plugins::worker::tests::{created, installed, worker};

/// A created dataset journaled at `at`, with its origin anchor there too.
fn dataset(install: &PluginInstall, resource: &str, at: u64) -> SourceEvent {
    let mut event = created(install, resource).0;
    (event.journaled_ms, event.anchor_ms) = (at, at);
    event
}

/// The resources of the install's runs, sorted.
async fn ran(fixture: &Fixture, install: &PluginInstall) -> Vec<String> {
    let runs = list_runs(&fixture.ctx, install.install_id, None, 100).await;
    let mut resources: Vec<String> = (runs.unwrap().0.into_iter())
        .filter_map(|run| match run.trigger {
            RunTrigger::Event(event) => Some(event.resource),
            _ => None,
        })
        .collect();
    resources.sort();
    resources
}

/// Decides `rows` on this node as their receiver.
async fn decide(fixture: &Fixture, rows: Vec<ForwardRow>, now: u64) -> Decisions {
    let nodes = (fixture.actor.node_id, fixture.actor.node_id);
    let batch = EventBatch {
        events: rows,
        gaps: Vec::new(),
    };
    let config = config(fixture).await;
    decide_batch(&fixture.ctx, &config, nodes, batch, now)
        .await
        .unwrap()
}

#[tokio::test]
async fn restart_runs_once() {
    // Rows wait in the replay, the journal, the outbox and the waiting set when the node stops.
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let (local, now) = (fixture.actor.node_id, unix_timestamp_millis());
    let account = reducer_state_key(&install.prerequisites[0].target).to_vec();
    let txn = Txn::direct(&fixture.ctx);
    let state = txn.read(DOCUMENT_STATE_KEYSPACE, &account).await.unwrap();
    txn.delete(DOCUMENT_STATE_KEYSPACE, vec![account.clone()])
        .await
        .unwrap();
    let config = config(&fixture).await;
    let mut bare = config.clone();
    bare.installs.clear();
    journal(&fixture, &dataset(&install, "replayed", now - 4)).await;
    match_pending(&fixture.ctx, &bare, local, now)
        .await
        .unwrap();
    let no_clock = |_| async { None };
    plan_replays(&fixture.ctx, &config, local, now, no_clock)
        .await
        .unwrap();
    journal(&fixture, &dataset(&install, "outbox", now - 3)).await;
    match_pending(&fixture.ctx, &config, local, now)
        .await
        .unwrap();
    journal(&fixture, &dataset(&install, "journal", now - 2)).await;
    let waiting = ForwardRow {
        install_id: install.install_id,
        revision: install.revision,
        event: dataset(&install, "waiting", now - 1),
        queued_ms: now,
    };
    let decided = decide(&fixture, vec![waiting], now).await;
    assert_eq!(decided.events[0].2, Outcome::Waiting);

    // The restarted node delivers everything; once the account's writes arrive, all run.
    worker(&fixture).claim(now).await.unwrap();
    assert!(ran(&fixture, &install).await.is_empty());
    let state = vec![(account, state.unwrap())];
    txn.write(DOCUMENT_STATE_KEYSPACE, state).await.unwrap();
    for _ in 0..2 {
        let restarted = worker(&fixture);
        restarted.claim(now).await.unwrap();
        restarted.settle().await;
    }
    let expected = ["journal", "outbox", "replayed", "waiting"];
    assert_eq!(ran(&fixture, &install).await, expected);
    assert!(forwarded(&fixture, &install).await.is_empty());
}

#[tokio::test]
async fn narrowed_grant_stops() {
    // A row matched before the grant shrank is denied; a later event is not matched at all.
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let (local, now) = (fixture.actor.node_id, unix_timestamp_millis());
    journal(&fixture, &dataset(&install, "B/one", now - 1)).await;
    match_pending(&fixture.ctx, &config(&fixture).await, local, now)
        .await
        .unwrap();
    let base = created(&install, "").1;
    let narrowed = BTreeMap::from([("metadata.read".into(), vec![format!("{base}/A/**")])]);
    let (auth, id) = (fixture.auth(), install.install_id);
    set_grants(&fixture.ctx, &fixture.actor, &auth, id, &narrowed)
        .await
        .unwrap();
    journal(&fixture, &dataset(&install, "B/two", now)).await;
    worker(&fixture).claim(now).await.unwrap();
    assert!(forwarded(&fixture, &install).await.is_empty());
    assert!(ran(&fixture, &install).await.is_empty());
}

#[tokio::test]
async fn pause_and_removal() {
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let (local, now) = (fixture.actor.node_id, unix_timestamp_millis());
    for (resource, at) in [("D1", now - 1), ("D2", now)] {
        journal(&fixture, &dataset(&install, resource, at)).await;
    }
    match_pending(&fixture.ctx, &config(&fixture).await, local, now)
        .await
        .unwrap();
    let (auth, id) = (fixture.auth(), install.install_id);
    pause_install(&fixture.ctx, &fixture.actor, &auth, id)
        .await
        .unwrap();
    let paused = ForwardRow {
        install_id: id,
        revision: install.revision,
        event: dataset(&install, "D1", now - 1),
        queued_ms: now,
    };
    let decided = decide(&fixture, vec![paused], now).await;
    assert!(matches!(&decided.events[0].2, Outcome::Denied(reason) if reason.contains("paused")));
    // Once the install is removing, its senders drop the rows they still hold.
    let mut removing = config(&fixture).await;
    removing.installs[0].state = InstallState::Removing;
    let unsent = |_, _| std::future::ready(Err::<Decisions, _>("not called".to_string()));
    let mut backoff = Backoff::default();
    send_outbox(&fixture.ctx, &removing, now, &mut backoff, unsent)
        .await
        .unwrap();
    let txn = Txn::direct(&fixture.ctx);
    assert!(
        txn.scan(PLUGIN_OUTBOX_KEYSPACE, &[])
            .await
            .unwrap()
            .is_empty()
    );
    assert!(ran(&fixture, &install).await.is_empty());
}
