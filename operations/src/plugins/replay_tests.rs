//! Tests replays: a late install and a widened grant replay once, skipped revisions and resumes
//! replay from the activation, a cut replay continues, and clocks and pruning are bounded.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::plugins::lifecycle::set_grants;
use crate::plugins::matcher::match_pending;
use crate::plugins::matcher::tests::{config, forwarded, gap, journal};
use crate::plugins::store::decode;
use crate::plugins::worker::tests::{created, failing_sync, install_artifact, installed, synced};
use aruna_core::keyspaces::PLUGIN_GAPS_KEYSPACE;
use aruna_core::plugin_installs::Widening;
use aruna_core::plugin_runs::{GapRow, gap_key};
use aruna_core::time::unix_timestamp_millis;

const MINUTE: u64 = 60 * 1000;

async fn no_clock(_: NodeId) -> Option<ClockReading> {
    None
}

/// A created dataset journaled here at `at_ms`.
fn dataset(install: &PluginInstall, resource: &str, at_ms: u64) -> SourceEvent {
    let mut event = created(install, resource).0;
    (event.journaled_ms, event.anchor_ms) = (at_ms, at_ms);
    event
}

/// Matches the pending rows as a node that did not know any install yet.
async fn unmatched(fixture: &crate::plugins::install::tests::Fixture, now: u64) {
    let mut bare = config(fixture).await;
    bare.installs.clear();
    let local = fixture.actor.node_id;
    match_pending(&fixture.ctx, &bare, local, now)
        .await
        .unwrap();
}

#[tokio::test]
async fn late_install() {
    // Active elsewhere, committed here before this node had the install: one row, once.
    let fixture = crate::plugins::install::tests::Fixture::new().await;
    let install = installed(&fixture).await;
    let (local, now) = (fixture.actor.node_id, unix_timestamp_millis());
    journal(&fixture, &dataset(&install, "D1", now)).await;
    unmatched(&fixture, now).await;
    let config = config(&fixture).await;
    assert_eq!(
        plan_replays(&fixture.ctx, &config, local, now, no_clock).await,
        Ok(1)
    );
    assert_eq!(run_replays(&fixture.ctx, now).await, Ok(1));
    assert_eq!(
        plan_replays(&fixture.ctx, &config, local, now, no_clock).await,
        Ok(0)
    );
    assert_eq!(run_replays(&fixture.ctx, now).await, Ok(0));
    assert_eq!(forwarded(&fixture, &install).await, ["D1"]);
}

#[tokio::test]
async fn widened_grant() {
    let fixture = crate::plugins::install::tests::Fixture::new().await;
    let install = installed(&fixture).await;
    let (local, now, auth) = (
        fixture.actor.node_id,
        unix_timestamp_millis(),
        fixture.auth(),
    );
    let (whole, base) = (
        install.grants["metadata.read"].clone(),
        created(&install, "").1,
    );
    let (inside, outside) = (
        dataset(&install, "A/one", now - 1),
        dataset(&install, "B/two", now),
    );
    let grant = |patterns| BTreeMap::from([("metadata.read".to_string(), patterns)]);
    let id = install.install_id;
    let narrow = grant(vec![format!("{base}/A/**")]);
    set_grants(&fixture.ctx, &fixture.actor, &auth, id, &narrow)
        .await
        .unwrap();
    let narrowed = config(&fixture).await;
    plan_replays(&fixture.ctx, &narrowed, local, now, no_clock)
        .await
        .unwrap();
    run_replays(&fixture.ctx, now).await.unwrap();
    journal(&fixture, &inside).await;
    journal(&fixture, &outside).await;
    match_pending(&fixture.ctx, &narrowed, local, now)
        .await
        .unwrap();
    assert_eq!(forwarded(&fixture, &install).await, ["A/one"]);

    let wide = grant(whole);
    let widened = set_grants(&fixture.ctx, &fixture.actor, &auth, id, &wide);
    assert!(widened.await.unwrap().widened.is_some());
    let config = config(&fixture).await;
    assert_eq!(
        plan_replays(&fixture.ctx, &config, local, now, no_clock).await,
        Ok(1)
    );
    // Only the newly covered event is read into a forward row.
    run_replays(&fixture.ctx, now).await.unwrap();
    assert_eq!(forwarded(&fixture, &install).await, ["A/one", "B/two"]);
    assert_eq!(
        plan_replays(&fixture.ctx, &config, local, now, no_clock).await,
        Ok(0)
    );
}

#[tokio::test]
async fn widened_sync() {
    // A sync row journaled while the grant left its path out is read once the grant widens.
    let fixture = crate::plugins::install::tests::Fixture::new().await;
    let install = installed(&fixture).await;
    let (local, now, auth) = (
        fixture.actor.node_id,
        unix_timestamp_millis(),
        fixture.auth(),
    );
    let whole = install.grants["objects.read"].clone();
    let base = whole[0].strip_suffix("/**").unwrap().to_string();
    let (inside, outside) = (Ulid::from_parts(1, 1), Ulid::from_parts(2, 2));
    let grant = |patterns| BTreeMap::from([("objects.read".to_string(), patterns)]);
    let id = install.install_id;
    let narrow = grant(vec![format!("{base}/raw/**")]);
    (set_grants(&fixture.ctx, &fixture.actor, &auth, id, &narrow).await).unwrap();
    let narrowed = config(&fixture).await;
    plan_replays(&fixture.ctx, &narrowed, local, now, no_clock)
        .await
        .unwrap();
    run_replays(&fixture.ctx, now).await.unwrap();
    for (prefix, relationship, at) in [("raw/", inside, now - 1), ("other/", outside, now)] {
        let mut event = synced(&install, prefix, relationship);
        (event.journaled_ms, event.anchor_ms) = (at, at);
        journal(&fixture, &event).await;
    }
    match_pending(&fixture.ctx, &narrowed, local, now)
        .await
        .unwrap();
    assert_eq!(forwarded(&fixture, &install).await, [inside.to_string()]);

    let wide = grant(whole);
    let widened = set_grants(&fixture.ctx, &fixture.actor, &auth, id, &wide);
    assert!(widened.await.unwrap().widened.is_some());
    let config = config(&fixture).await;
    let planned = plan_replays(&fixture.ctx, &config, local, now, no_clock).await;
    assert_eq!(planned, Ok(1));
    run_replays(&fixture.ctx, now).await.unwrap();
    let both = [inside.to_string(), outside.to_string()];
    assert_eq!(forwarded(&fixture, &install).await, both);
}

#[tokio::test]
async fn growth_rules() {
    let fixture = crate::plugins::install::tests::Fixture::new().await;
    let mut install = installed(&fixture).await;
    let since = install.active_since_ms.unwrap();
    let wide = Coverage::from([(
        TriggerKind::DatasetCreated,
        BTreeSet::from(["/a/**".into()]),
    )]);
    let narrow = Coverage::from([(TriggerKind::DatasetCreated, BTreeSet::from(["/a/b".into()]))]);
    let other = iroh::SecretKey::from_bytes(&[7u8; 32]).public();
    let row = |revision, coverage: &Coverage| CoverageRow {
        revision,
        active_since_ms: Some(since),
        coverage: coverage.clone(),
        note: None,
    };
    install.revision = 3;
    install.widened = Some(Widening {
        at_ms: since + 60 * MINUTE,
        by: other,
        extends: 2,
    });
    let first = growth(&install, None, &wide).unwrap();
    assert_eq!((first.0, first.1), (Coverage::new(), since));
    assert_eq!(growth(&install, Some(&row(3, &narrow)), &wide), None);
    assert_eq!(growth(&install, Some(&row(2, &wide)), &wide), None);
    let extended = growth(&install, Some(&row(2, &narrow)), &wide).unwrap();
    assert_eq!((extended.1, extended.2), (since + 60 * MINUTE, Some(other)));
    // Revision 2 was skipped here, so the grown part replays from the activation.
    let skipped = growth(&install, Some(&row(1, &narrow)), &wide).unwrap();
    assert_eq!((skipped.0, skipped.1), (narrow.clone(), since));
    let mut resumed = row(3, &wide);
    resumed.active_since_ms = Some(since - 1);
    assert_eq!(
        growth(&install, Some(&resumed), &wide).unwrap().0,
        Coverage::new()
    );
}

#[tokio::test]
async fn cut_replay_continues() {
    let fixture = crate::plugins::install::tests::Fixture::new().await;
    let install = installed(&fixture).await;
    let (local, now) = (fixture.actor.node_id, unix_timestamp_millis());
    for (n, resource) in ["D1", "D2", "D3"].into_iter().enumerate() {
        journal(&fixture, &dataset(&install, resource, now - 3 + n as u64)).await;
    }
    unmatched(&fixture, now).await;
    let config = config(&fixture).await;
    plan_replays(&fixture.ctx, &config, local, now, no_clock)
        .await
        .unwrap();
    assert_eq!(replay_pages(&fixture.ctx, now, 1, 1).await, Ok(1));
    // After a restart the replay reads on from its last row.
    assert_eq!(run_replays(&fixture.ctx, now).await, Ok(2));
    assert_eq!(forwarded(&fixture, &install).await, ["D1", "D2", "D3"]);
}

#[tokio::test]
async fn unreadable_clock() {
    // Six minutes off with two minutes of round trip: the replay starts at the earliest row.
    let fixture = crate::plugins::install::tests::Fixture::new().await;
    let install = installed(&fixture).await;
    let (local, now) = (fixture.actor.node_id, unix_timestamp_millis());
    journal(&fixture, &dataset(&install, "D0", now - 24 * 60 * MINUTE)).await;
    unmatched(&fixture, now).await;
    let other = iroh::SecretKey::from_bytes(&[7u8; 32]).public();
    let mut config = config(&fixture).await;
    config.installs[0].activated_by = Some(other);
    let slow = async |_| {
        Some(ClockReading {
            sent_ms: now,
            remote_ms: now - 6 * MINUTE,
            received_ms: now + 2 * MINUTE,
        })
    };
    plan_replays(&fixture.ctx, &config, local, now, slow)
        .await
        .unwrap();
    run_replays(&fixture.ctx, now).await.unwrap();
    assert_eq!(forwarded(&fixture, &install).await, ["D0"]);
    let key = row_key(COVERAGE_ROW, install.install_id);
    let row: CoverageRow = stored(&Txn::direct(&fixture.ctx), &key)
        .await
        .unwrap()
        .unwrap();
    assert!(row.note.unwrap().contains(&other.to_string()));
}

#[tokio::test]
async fn pruned_journal() {
    let fixture = crate::plugins::install::tests::Fixture::new().await;
    let install = install_artifact(&fixture, failing_sync()).await;
    let local = fixture.actor.node_id;
    let now = install.active_since_ms.unwrap() + JOURNAL_RETENTION_MS + 60 * MINUTE;
    let config = config(&fixture).await;
    plan_replays(&fixture.ctx, &config, local, now, no_clock)
        .await
        .unwrap();
    // The pruned part held an unknown number of events.
    let pruned = gap(&fixture, &install, GapReason::Pruned).await.unwrap();
    assert_eq!((pruned.count, pruned.unknown), (0, true));
    let since = install.active_since_ms.unwrap();
    assert!(pruned.earliest_ms < since && pruned.latest_ms == now - JOURNAL_RETENTION_MS);
    // Each grown sync kind counts its own pruned gap.
    for kind in [SourceKind::SyncCompleted, SourceKind::SyncFailed] {
        let key = gap_key(install.install_id, kind, GapReason::Pruned);
        let row = Txn::direct(&fixture.ctx)
            .read(PLUGIN_GAPS_KEYSPACE, &key)
            .await;
        let row: GapRow = decode(&row.unwrap().expect("a pruned gap")).unwrap();
        assert_eq!((row.count, row.unknown), (0, true), "{kind:?}");
    }
}
