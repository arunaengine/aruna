//! Tests a whole event from journal to guest, through the plugin host.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::plugin_host::worker::tests::worker;
use aruna_core::keyspaces::PLUGIN_OUTBOX_KEYSPACE;
use aruna_core::plugin_runs::RunState;
use aruna_core::plugin_runs::RunTrigger;
use aruna_core::time::unix_timestamp_millis;
use aruna_operations::plugins::flow_support::decide::event;
use aruna_operations::plugins::flow_support::decide::runs;
use aruna_operations::plugins::flow_support::matcher::journal;
use aruna_operations::plugins::store::Txn;
use aruna_operations::plugins::test_support::Fixture;
use aruna_operations::plugins::test_support::installed;

#[tokio::test]
async fn end_to_end() {
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let now = unix_timestamp_millis();
    journal(&fixture, &event(&install, "D1", now).0).await;
    let worker = worker(&fixture);
    worker.claim(now).await.unwrap();
    worker.settle().await;
    let runs = runs(&fixture, &install).await;
    assert_eq!(runs.len(), 1);
    assert!(matches!(&runs[0].trigger, RunTrigger::Event(event) if event.resource == "D1"));
    assert_eq!(runs[0].state, RunState::Succeeded, "{:?}", runs[0].message);
    let txn = Txn::direct(&fixture.ctx);
    assert!(
        txn.scan(PLUGIN_OUTBOX_KEYSPACE, &[])
            .await
            .unwrap()
            .is_empty()
    );
}
