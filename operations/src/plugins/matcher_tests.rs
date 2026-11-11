//! Tests matching: grants narrow the match, pages commit with their rows, the bound and stale
//! rows count gaps, sync triggers match nothing, and a user node keeps no journal.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::plugin_installs::PluginInstall;
use aruna_core::plugin_runs::{SourceKind, journal_entries};

use super::*;
use crate::plugins::install::read_config;
use crate::plugins::install::tests::Fixture;
use crate::plugins::lifecycle::set_grants;
use crate::plugins::worker::tests::{created, installed};

/// Journals `event` on this node as a committing transaction would.
pub(crate) async fn journal(fixture: &Fixture, event: &SourceEvent) {
    let rows = (journal_entries(event).unwrap().into_iter())
        .map(|(_, key, value)| (key.to_vec(), value.to_vec()))
        .collect();
    (Txn::direct(&fixture.ctx))
        .write(EVENT_JOURNAL_KEYSPACE, rows)
        .await
        .unwrap();
}

/// The forward rows of `install` on this node, in journal order.
pub(crate) async fn forwarded(fixture: &Fixture, install: &PluginInstall) -> Vec<String> {
    let txn = Txn::direct(&fixture.ctx);
    let prefix = install.install_id.to_bytes();
    let rows = txn.scan(PLUGIN_OUTBOX_KEYSPACE, &prefix).await.unwrap();
    (rows.into_iter())
        .map(|(_, value)| decode::<ForwardRow>(&value).unwrap().event.resource)
        .collect()
}

pub(crate) async fn gap(
    fixture: &Fixture,
    install: &PluginInstall,
    reason: GapReason,
) -> Option<GapRow> {
    let key = gap_key(install.install_id, SourceKind::DatasetCreated, reason);
    let txn = Txn::direct(&fixture.ctx);
    let row = txn.read(PLUGIN_GAPS_KEYSPACE, &key).await.unwrap();
    row.map(|bytes| decode(&bytes).unwrap())
}

async fn pending(fixture: &Fixture) -> usize {
    let txn = Txn::direct(&fixture.ctx);
    txn.scan(EVENT_JOURNAL_KEYSPACE, &[PENDING_ROW])
        .await
        .unwrap()
        .len()
}

pub(crate) async fn config(fixture: &Fixture) -> RealmConfigDocument {
    read_config(&fixture.ctx, fixture.actor.realm_id)
        .await
        .unwrap()
}

#[tokio::test]
async fn narrowed_grants() {
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let (_, base) = created(&install, "A");
    let (inside, outside) = (created(&install, "A/one").0, created(&install, "B/two").0);
    let narrowed = BTreeMap::from([("metadata.read".into(), vec![format!("{base}/A/**")])]);
    let auth = fixture.auth();
    let install = set_grants(
        &fixture.ctx,
        &fixture.actor,
        &auth,
        install.install_id,
        &narrowed,
    )
    .await
    .unwrap();
    journal(&fixture, &inside).await;
    journal(&fixture, &outside).await;
    let local = fixture.actor.node_id;
    let read = match_pending(&fixture.ctx, &config(&fixture).await, local, 10).await;
    assert_eq!(read.unwrap(), 2);
    assert_eq!(forwarded(&fixture, &install).await, ["A/one"]);
    assert_eq!(pending(&fixture).await, 0);
}

#[tokio::test]
async fn rows_commit_once() {
    // A matched page leaves no pending row, so a restart matches each event once.
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let (local, config) = (fixture.actor.node_id, config(&fixture).await);
    journal(&fixture, &created(&install, "D1").0).await;
    match_pending(&fixture.ctx, &config, local, 10)
        .await
        .unwrap();
    journal(&fixture, &created(&install, "D2").0).await;
    assert_eq!(match_pending(&fixture.ctx, &config, local, 10).await, Ok(1));
    assert_eq!(match_pending(&fixture.ctx, &config, local, 10).await, Ok(0));
    assert_eq!(forwarded(&fixture, &install).await, ["D1", "D2"]);
}

#[tokio::test]
async fn bound_and_stale() {
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let (local, config) = (fixture.actor.node_id, config(&fixture).await);
    let now = JOURNAL_RETENTION_MS + 100;
    for (resource, journaled_ms) in [("D1", now - 2), ("D2", now - 1), ("D3", now), ("D0", 50)] {
        let mut event = created(&install, resource).0;
        (event.journaled_ms, event.anchor_ms) = (journaled_ms, journaled_ms);
        journal(&fixture, &event).await;
    }
    let read = match_bounded(&fixture.ctx, &config, local, now, 2).await;
    assert_eq!(read.unwrap(), 4);
    assert_eq!(forwarded(&fixture, &install).await, ["D1", "D2"]);
    let full = gap(&fixture, &install, GapReason::Full).await.unwrap();
    assert_eq!((full.count, full.sequence, full.latest_ms), (1, 1, now));
    let stale = gap(&fixture, &install, GapReason::Pruned).await.unwrap();
    assert_eq!((stale.count, stale.earliest_ms), (1, 50));
}

#[tokio::test]
async fn user_node_clears() {
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let mut config = config(&fixture).await;
    let local = fixture.actor.node_id.to_string();
    let node = config.nodes.iter_mut().find(|node| node.node_id == local);
    node.unwrap().kind = RealmNodeKind::User {
        owner: fixture.actor.user_id,
    };
    journal(&fixture, &created(&install, "D1").0).await;
    let read = match_pending(&fixture.ctx, &config, fixture.actor.node_id, 10).await;
    assert_eq!(read.unwrap(), 1);
    assert!(forwarded(&fixture, &install).await.is_empty());
    let txn = Txn::direct(&fixture.ctx);
    let kept = txn.scan(EVENT_JOURNAL_KEYSPACE, &[]).await.unwrap();
    assert!(kept.is_empty());
}

#[test]
fn sync_matches_nothing() {
    let group_id = Ulid::from_parts(1, 1);
    let target = Target {
        install_id: Ulid::from_parts(2, 2),
        group_id,
        revision: 1,
        coverage: BTreeMap::from([(TriggerKind::SyncCompleted, BTreeSet::from(["/**".into()]))]),
    };
    for kind in [SourceKind::DatasetCreated, SourceKind::DataUploaded] {
        let event = SourceEvent {
            kind,
            group_id,
            path: "/r/g/x/data/n/b/k".into(),
            resource: "k".into(),
            revision: "v".into(),
            source: "s".into(),
            anchor_ms: 1,
            journaled_ms: 1,
        };
        assert!(!target.matches(&event));
    }
}
