//! Tests matching: grants narrow the match, also for sync events, own writes start nothing, pages
//! commit with their rows, the bound and stale rows count gaps, and a user node keeps no journal.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::plugin_installs::PluginInstall;
use aruna_core::plugin_runs::{SourceKind, journal_entries};
use aruna_core::structs::identity::realm::RealmId;

use super::*;
use crate::plugins::install::read_config;
use crate::plugins::install::tests::Fixture;
use crate::plugins::lifecycle::set_grants;
use crate::plugins::rows::{page_result, values_result};
use crate::plugins::store::Txn;
use crate::plugins::worker::match_journal;
use crate::plugins::worker::tests::{created, installed, scoped, synced};
use aruna_core::plugin_runs::EVENTS_QUEUED;
use aruna_core::plugin_runs::journal_key;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmNodeKind};

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
    let narrowed = scoped(vec![format!("{base}/A/**")]);
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
    let read = match_journal(
        &fixture.ctx,
        &config(&fixture).await,
        local,
        (10, EVENTS_QUEUED),
    )
    .await;
    assert_eq!(read.unwrap(), 2);
    assert_eq!(forwarded(&fixture, &install).await, ["A/one"]);
    assert_eq!(pending(&fixture).await, 0);
}

#[tokio::test]
async fn narrowed_sync() {
    // A sync event under the granted prefix is matched; one under another prefix is not.
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let base = install.grants["read"][0].strip_suffix("/**").unwrap();
    let narrowed = scoped(vec![format!("{base}/data/raw/**")]);
    let (inside, outside) = (Ulid::from_parts(1, 1), Ulid::from_parts(2, 2));
    let events = [
        synced(&install, "raw/runs/", inside),
        synced(&install, "other/", outside),
    ];
    let auth = fixture.auth();
    let id = install.install_id;
    let install = (set_grants(&fixture.ctx, &fixture.actor, &auth, id, &narrowed).await).unwrap();
    for event in &events {
        journal(&fixture, event).await;
    }
    let local = fixture.actor.node_id;
    let read = match_journal(
        &fixture.ctx,
        &config(&fixture).await,
        local,
        (10, EVENTS_QUEUED),
    )
    .await;
    assert_eq!(read.unwrap(), 2);
    assert_eq!(forwarded(&fixture, &install).await, [inside.to_string()]);
}

#[tokio::test]
async fn rows_commit_once() {
    // A matched page leaves no pending row, so a restart matches each event once.
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let (local, config) = (fixture.actor.node_id, config(&fixture).await);
    journal(&fixture, &created(&install, "D1").0).await;
    match_journal(&fixture.ctx, &config, local, (10, EVENTS_QUEUED))
        .await
        .unwrap();
    journal(&fixture, &created(&install, "D2").0).await;
    assert_eq!(
        match_journal(&fixture.ctx, &config, local, (10, EVENTS_QUEUED)).await,
        Ok(1)
    );
    assert_eq!(
        match_journal(&fixture.ctx, &config, local, (10, EVENTS_QUEUED)).await,
        Ok(0)
    );
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
    let read = match_journal(&fixture.ctx, &config, local, (now, 2)).await;
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
    let read = match_journal(
        &fixture.ctx,
        &config,
        fixture.actor.node_id,
        (10, EVENTS_QUEUED),
    )
    .await;
    assert_eq!(read.unwrap(), 1);
    assert!(forwarded(&fixture, &install).await.is_empty());
    let txn = Txn::direct(&fixture.ctx);
    let kept = txn.scan(EVENT_JOURNAL_KEYSPACE, &[]).await.unwrap();
    assert!(kept.is_empty());
}

#[test]
fn sync_matches_kind() {
    // A sync trigger matches its own kind on a covered path, and no other kind.
    let group_id = Ulid::from_parts(1, 1);
    let target = Target {
        install_id: Ulid::from_parts(2, 2),
        group_id,
        revision: 1,
        coverage: BTreeMap::from([(TriggerKind::SyncCompleted, BTreeSet::from(["/**".into()]))]),
        account: UserId::local(Ulid::from_parts(2, 2), RealmId::from_bytes([1u8; 32])),
    };
    for (kind, matched) in [
        (SourceKind::DatasetCreated, false),
        (SourceKind::DataUploaded, false),
        (SourceKind::SyncFailed, false),
        (SourceKind::SyncCompleted, true),
    ] {
        let event = SourceEvent {
            kind,
            group_id,
            path: "/r/g/x/data/n/b/k".into(),
            resource: "k".into(),
            revision: "v".into(),
            source: "s".into(),
            anchor_ms: 1,
            journaled_ms: 1,
            actor: None,
        };
        assert_eq!(target.matches(&event), matched, "{kind:?}");
    }
}

#[tokio::test]
async fn own_changes_skipped() {
    // A dataset write of the install's own account starts no run of it; one by anyone else does.
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let (mut own, _) = created(&install, "A/own");
    let (mut other, _) = created(&install, "A/other");
    for event in [&mut own, &mut other] {
        event.kind = SourceKind::DatasetUpdated;
        event.source = format!("dataset_updated/{}", event.resource);
    }
    (own.actor, other.actor) = (Some(install.account), Some(fixture.actor.user_id));
    journal(&fixture, &own).await;
    journal(&fixture, &other).await;
    let local = fixture.actor.node_id;
    let read = match_journal(
        &fixture.ctx,
        &config(&fixture).await,
        local,
        (10, EVENTS_QUEUED),
    )
    .await;
    assert_eq!(read.unwrap(), 2);
    assert_eq!(forwarded(&fixture, &install).await, ["A/other"]);
}

/// A created dataset event of a fixed group at `anchor_ms`.
fn plain_event(anchor_ms: u64) -> SourceEvent {
    SourceEvent {
        kind: SourceKind::DatasetCreated,
        group_id: Ulid::from_parts(1, 9),
        path: "/r/g/x/meta/a@d".into(),
        resource: "d".into(),
        revision: "e".into(),
        source: format!("dataset_created/{anchor_ms}"),
        actor: None,
        anchor_ms,
        journaled_ms: anchor_ms,
    }
}

#[test]
fn outbox_counts_gaps() {
    let install = Ulid::from_parts(1, 1);
    let mut outbox = Outbox::new(9, BTreeMap::from([(install, 1)]));
    outbox
        .offer((install, 1), b"j1", &plain_event(5), false, 2)
        .unwrap();
    // The bound is reached, then a stale row counts too.
    outbox
        .offer((install, 1), b"j2", &plain_event(6), false, 2)
        .unwrap();
    outbox
        .offer((install, 1), b"j3", &plain_event(4), true, 2)
        .unwrap();
    outbox.lost_range(
        install,
        (SourceKind::DataUploaded, GapReason::Pruned),
        (1, 3),
    );
    let IoCall::Read(reads) = outbox.gap_ask() else {
        panic!("gap rows are read once each");
    };
    assert_eq!(reads.len(), 3);
    let full = gap_key(install, SourceKind::DatasetCreated, GapReason::Full);
    let mut kept = GapRow::new(SourceKind::DatasetCreated, GapReason::Full);
    kept.add(1);
    let values: Vec<_> = (reads.iter())
        .map(|(_, key)| (**key == *full).then(|| encode(&kept).unwrap()))
        .collect();
    let rows = outbox.rows(values).unwrap();
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0].0, PLUGIN_OUTBOX_KEYSPACE);
    let counted: GapRow = (rows.iter())
        .find(|(_, key, _)| key.to_vec() == full)
        .map(|(_, _, value)| decode(value).unwrap())
        .unwrap();
    assert_eq!(
        (counted.count, counted.earliest_ms, counted.latest_ms),
        (2, 1, 6)
    );
    let short = Outbox::new(9, BTreeMap::new());
    assert!(Outbox::new(9, BTreeMap::new()).rows(vec![None]).is_err());
    assert_eq!(short.rows(Vec::new()).unwrap(), Vec::new());
}

#[test]
fn counting_pages() {
    let (first, second) = (Ulid::from_parts(1, 1), Ulid::from_parts(1, 2));
    let mut counting = Counting::new(BTreeSet::from([first, second]), 5);
    let Some(IoCall::Page { prefix, limit, .. }) = counting.ask() else {
        panic!("each install's queue is counted");
    };
    assert_eq!((prefix.to_vec(), limit), (first.to_bytes().to_vec(), 5));
    counting
        .take(page_result(vec![(vec![1], Vec::new())], None))
        .unwrap();
    counting.take(page_result(Vec::new(), None)).unwrap();
    assert_eq!(counting.ask(), None);
    assert!(counting.take(page_result(Vec::new(), None)).is_err());
    assert_eq!(
        counting.counted(),
        BTreeMap::from([(first, 1), (second, 0)])
    );
}

#[test]
fn user_pending_dropped() {
    let mut plan = MatchPage {
        targets: Vec::new(),
        keep: false,
        now_ms: 9,
        bound: 2,
        phase: MatchPhase::Reading,
    };
    assert!(matches!(
        plan.first().unwrap(),
        Next::Call(IoCall::Page { .. })
    ));
    let event = plain_event(5);
    let pending = journal_key(PENDING_ROW, 5, Ulid::from_parts(3, 3));
    let rows = vec![(pending.clone(), event.to_bytes().unwrap())];
    // A user node keeps no journal: nothing to count, no gap to read.
    assert_eq!(
        plan.next(page_result(rows, None)).unwrap(),
        Next::Call(read_call(Vec::new()))
    );
    let Next::Call(IoCall::Write(written)) = plan.next(values_result(Vec::new())).unwrap() else {
        panic!("the page writes no forward row");
    };
    assert!(written.is_empty());
    let Next::Call(IoCall::Delete(keys)) = plan.next(IoResult::Done).unwrap() else {
        panic!("the pending, kept and source rows go");
    };
    assert_eq!(keys.len(), 3);
    assert_eq!(
        keys[0],
        (EVENT_JOURNAL_KEYSPACE.to_string(), pending.into())
    );
    assert_eq!(plan.next(IoResult::Done).unwrap(), Next::Done(1));
    // An empty page reads nothing more.
    let mut empty = MatchPage {
        targets: Vec::new(),
        keep: true,
        now_ms: 9,
        bound: 2,
        phase: MatchPhase::Reading,
    };
    assert_eq!(
        empty.next(page_result(Vec::new(), None)).unwrap(),
        Next::Done(0)
    );
}
