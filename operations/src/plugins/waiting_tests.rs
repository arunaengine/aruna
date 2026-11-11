//! Tests the waiting set: an event queues in its own slot once its account arrives, and one
//! that waits too long ends `expired` with a gap.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::keyspaces::{DOCUMENT_STATE_KEYSPACE, PLUGIN_GAPS_KEYSPACE, PLUGIN_SEEN_KEYSPACE};
use aruna_core::plugin_installs::PluginInstall;
use aruna_core::plugin_runs::{ForwardRow, GapRow, RECEIVER_GAP, RunTrigger};
use aruna_core::storage_entries::reducer_state_key;
use aruna_core::time::unix_timestamp_millis;

use super::*;
use crate::plugins::decide::{SeenRow, decide_batch, seen_key};
use crate::plugins::forward::EventBatch;
use crate::plugins::install::tests::Fixture;
use crate::plugins::matcher::tests::config;
use crate::plugins::runs::list_runs;
use crate::plugins::worker::tests::{created, installed};

/// An event decided as waiting because the account's writes are gone; returns the event and
/// the account's state row, which arrives again later.
async fn waiting(fixture: &Fixture, install: &PluginInstall, now: u64) -> (ForwardRow, Rows) {
    let mut event = created(install, "D1").0;
    (event.journaled_ms, event.anchor_ms) = (now, now);
    let row = ForwardRow {
        install_id: install.install_id,
        revision: install.revision,
        event,
        queued_ms: now,
    };
    let key = reducer_state_key(&install.prerequisites[0].target).to_vec();
    let txn = Txn::direct(&fixture.ctx);
    let state = txn
        .read(DOCUMENT_STATE_KEYSPACE, &key)
        .await
        .unwrap()
        .unwrap();
    txn.delete(DOCUMENT_STATE_KEYSPACE, vec![key.clone()])
        .await
        .unwrap();
    assert_eq!(decide(fixture, &row, now).await, Outcome::Waiting);
    (row, vec![(key, state)])
}

type Rows = Vec<(Vec<u8>, Vec<u8>)>;

async fn decide(fixture: &Fixture, row: &ForwardRow, now: u64) -> Outcome {
    let batch = EventBatch {
        events: vec![row.clone()],
        gaps: Vec::new(),
    };
    let nodes = (fixture.actor.node_id, fixture.actor.node_id);
    let config = config(fixture).await;
    let decided = decide_batch(&fixture.ctx, &config, nodes, batch, now).await;
    decided.unwrap().events.remove(0).2
}

async fn recheck(fixture: &Fixture, now: u64) -> usize {
    let config = config(fixture).await;
    let local = fixture.actor.node_id;
    recheck_waiting(&fixture.ctx, &config, local, now)
        .await
        .unwrap()
}

async fn slots(fixture: &Fixture, install: &PluginInstall) -> usize {
    let prefix = install.install_id.to_bytes();
    let txn = Txn::direct(&fixture.ctx);
    txn.scan(PLUGIN_QUEUE_KEYSPACE, &prefix)
        .await
        .unwrap()
        .len()
}

async fn seen(fixture: &Fixture, row: &ForwardRow) -> Outcome {
    let key = seen_key(row.install_id, row.event.kind, &row.event.source);
    let txn = Txn::direct(&fixture.ctx);
    let bytes = txn.read(PLUGIN_SEEN_KEYSPACE, &key).await.unwrap().unwrap();
    decode::<SeenRow>(&bytes).unwrap().outcome
}

#[tokio::test]
async fn queues_when_ready() {
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let now = unix_timestamp_millis();
    let (row, account) = waiting(&fixture, &install, now).await;
    assert_eq!(recheck(&fixture, now).await, 0);
    let txn = Txn::direct(&fixture.ctx);
    txn.write(DOCUMENT_STATE_KEYSPACE, account).await.unwrap();
    assert_eq!(recheck(&fixture, now).await, 1);
    // The run took the waiting event's slot; a resend gives no second run.
    assert_eq!(slots(&fixture, &install).await, 1);
    assert_eq!(seen(&fixture, &row).await, Outcome::Queued);
    assert_eq!(decide(&fixture, &row, now).await, Outcome::Queued);
    let runs = list_runs(&fixture.ctx, install.install_id, None, 10).await;
    let runs = runs.unwrap().0;
    assert_eq!(runs.len(), 1);
    assert!(matches!(runs[0].trigger, RunTrigger::Event(_)));
    assert_eq!(recheck(&fixture, now).await, 0);
}

#[tokio::test]
async fn expires_with_gap() {
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let now = unix_timestamp_millis();
    let (row, _) = waiting(&fixture, &install, now).await;
    let later = now + WAITING_EXPIRY_MS + 1;
    assert_eq!(recheck(&fixture, later).await, 1);
    assert_eq!(slots(&fixture, &install).await, 0);
    assert_eq!(decide(&fixture, &row, later).await, Outcome::Expired);
    let txn = Txn::direct(&fixture.ctx);
    let gaps = txn
        .scan(PLUGIN_GAPS_KEYSPACE, &[RECEIVER_GAP])
        .await
        .unwrap();
    let gap: GapRow = decode(&gaps[0].1).unwrap();
    assert_eq!((gap.reason, gap.count), (GapReason::Waited, 1));
}
