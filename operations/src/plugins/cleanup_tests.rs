//! Tests the sweep: expired gap counts go on both sides, a removed install's replay rows go,
//! and a removing install's waiting events free their queue slots.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::keyspaces::{DOCUMENT_STATE_KEYSPACE, PLUGIN_OUTBOX_KEYSPACE};
use aruna_core::plugin_runs::{GapReason, SourceKind, gap_key, reported_key};
use aruna_core::storage_entries::reducer_state_key;
use aruna_core::time::unix_timestamp_millis;

use super::*;
use crate::plugins::install::tests::Fixture;
use crate::plugins::matcher::tests::{config, journal};
use crate::plugins::store::encode;
use crate::plugins::worker::tests::{created, installed, worker};

async fn count(fixture: &Fixture, space: &str) -> usize {
    let txn = Txn::direct(&fixture.ctx);
    txn.scan(space, &[]).await.unwrap().len()
}

#[tokio::test]
async fn expired_gaps() {
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let config = config(&fixture).await;
    let now = unix_timestamp_millis();
    let gap = |latest_ms| {
        let mut row = GapRow::new(SourceKind::DataUploaded, GapReason::Full);
        row.add(latest_ms);
        encode(&row).unwrap()
    };
    let (kind, reason) = (SourceKind::DataUploaded, GapReason::Full);
    let sender = iroh::SecretKey::from_bytes(&[7u8; 32]).public();
    let rows = vec![
        (gap_key(install.install_id, kind, reason), gap(now)),
        (
            reported_key(sender, install.install_id, kind, reason),
            gap(now),
        ),
    ];
    let txn = Txn::direct(&fixture.ctx);
    txn.write(PLUGIN_GAPS_KEYSPACE, rows).await.unwrap();
    assert_eq!(sweep_events(&fixture.ctx, &config, now).await, Ok(0));
    let later = now + DEDUP_RETENTION_MS + 1;
    assert_eq!(sweep_events(&fixture.ctx, &config, later).await, Ok(2));
    assert_eq!(count(&fixture, PLUGIN_GAPS_KEYSPACE).await, 0);
}

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
    assert!(sweep_events(&fixture.ctx, &config, now).await.unwrap() >= 2);
    for space in [
        PLUGIN_WAITING_KEYSPACE,
        PLUGIN_QUEUE_KEYSPACE,
        PLUGIN_REPLAYS_KEYSPACE,
    ] {
        assert_eq!(count(&fixture, space).await, 0, "{space}");
    }
}
