//! Tests a removed install stops its running guests, through the plugin host.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::plugin_host::worker::tests::worker;
use aruna_clock::unix_timestamp_millis;
use aruna_core::keyspaces::DOCUMENT_STATE_KEYSPACE;
use aruna_core::keyspaces::PLUGIN_OUTBOX_KEYSPACE;
use aruna_core::keyspaces::PLUGIN_QUEUE_KEYSPACE;
use aruna_core::keyspaces::PLUGIN_REPLAYS_KEYSPACE;
use aruna_core::keyspaces::PLUGIN_WAITING_KEYSPACE;
use aruna_core::plugin_installs::InstallState;
use aruna_core::storage_entries::reducer_state_key;
use aruna_ops_work::plugins::cleanup::sweep_events;
use aruna_runner::driver::drive;
use aruna_test_harness::plugins::Fixture;
use aruna_test_harness::plugins::artifacts::installed;
use aruna_test_harness::plugins::created;
use aruna_test_harness::plugins::flows::cleanup::count;
use aruna_test_harness::plugins::flows::matcher::config;
use aruna_test_harness::plugins::flows::matcher::journal;
use aruna_test_harness::plugins::rows::Txn;

#[tokio::test]
async fn removed_install_rows() {
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let now = unix_timestamp_millis();
    let mut event = created(&install, "D1").0;
    (event.journaled_ms, event.anchor_ms) = (now, now);
    journal(&fixture, &event).await;
    // Without the account's writes the forwarded event waits on the receiver.
    let account = reducer_state_key(&install.prerequisites[0].target).to_vec();
    let txn = Txn::direct(&fixture.ctx);
    txn.delete(DOCUMENT_STATE_KEYSPACE, vec![account])
        .await
        .unwrap();
    worker(&fixture).claim(now).await.unwrap();
    assert_eq!(count(&fixture, PLUGIN_WAITING_KEYSPACE).await, 1);
    assert_eq!(count(&fixture, PLUGIN_OUTBOX_KEYSPACE).await, 0);
    assert!(count(&fixture, PLUGIN_REPLAYS_KEYSPACE).await > 0);
    let mut config = config(&fixture).await;
    config.installs[0].state = InstallState::Removing;
    assert!(
        drive(sweep_events(&config, now), &fixture.ctx)
            .await
            .unwrap()
            >= 2
    );
    for space in [
        PLUGIN_WAITING_KEYSPACE,
        PLUGIN_QUEUE_KEYSPACE,
        PLUGIN_REPLAYS_KEYSPACE,
    ] {
        assert_eq!(count(&fixture, space).await, 0, "{space}");
    }
}
