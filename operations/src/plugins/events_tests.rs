//! Tests event delivery: a restart with rows at every stage runs each event once, also sync ones;
//! grants, pause and removal stop delivery; sync jobs and remote sync rows run once.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::keyspaces::PLUGIN_OUTBOX_KEYSPACE;
use aruna_core::plugin_installs::InstallState;
use aruna_core::plugin_runs::EVENTS_QUEUED;
use aruna_core::plugin_runs::ForwardRow;
use aruna_core::time::unix_timestamp_millis;

use crate::plugins::calls::*;
use crate::plugins::flow_support::events::decide;
use crate::plugins::forward::Backoff;
use aruna_core::plugin_runs::Decisions;
use aruna_core::plugin_runs::Outcome;
use aruna_test_support::plugins::Fixture;
use aruna_test_support::plugins::artifacts::installed;
use aruna_test_support::plugins::flows::events::{dataset, ran};
use aruna_test_support::plugins::flows::matcher::config;
use aruna_test_support::plugins::flows::matcher::journal;
use aruna_test_support::plugins::rows::Txn;

#[tokio::test]
async fn pause_and_removal() {
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let (local, now) = (fixture.actor.node_id, unix_timestamp_millis());
    for (resource, at) in [("D1", now - 1), ("D2", now)] {
        journal(&fixture, &dataset(&install, resource, at)).await;
    }
    match_journal(
        &fixture.ctx,
        &config(&fixture).await,
        local,
        (now, EVENTS_QUEUED),
    )
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
