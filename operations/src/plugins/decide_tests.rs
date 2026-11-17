//! Tests the receiver: an event runs end to end once, waits for its record or account, is
//! denied once, shares the queue bound with other runs, and keeps the newest gap reports.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::driver::drive;
use aruna_core::keyspaces::{DOCUMENT_STATE_KEYSPACE, PLUGIN_OUTBOX_KEYSPACE};
use aruna_core::plugin_runs::{RECEIVER_GAP, RunState, RunTrigger, SourceEvent};
use aruna_core::storage_entries::reducer_state_key;

use super::*;
use crate::plugins::install::tests::Fixture;
use crate::plugins::lifecycle::set_grants;
use crate::plugins::matcher::tests::{config, journal};
use crate::plugins::runs::tests::queued;
use crate::plugins::runs::{admit, list_runs, save_run, unfinished};
use crate::plugins::store::Txn;
use crate::plugins::worker::decide_batch;
use crate::plugins::worker::prune_runs;
use crate::plugins::worker::tests::{created, installed, scoped, worker};
use aruna_core::operation::Operation;

/// A created dataset journaled now, and its forward row for `install`.
fn event(install: &PluginInstall, resource: &str, now: u64) -> (SourceEvent, ForwardRow) {
    let mut event = created(install, resource).0;
    (event.journaled_ms, event.anchor_ms) = (now, now);
    let row = ForwardRow {
        install_id: install.install_id,
        revision: install.revision,
        event: event.clone(),
        queued_ms: now,
    };
    (event, row)
}

/// Decides `rows` as one batch from this node.
async fn decided(fixture: &Fixture, rows: &[ForwardRow]) -> Vec<Outcome> {
    let now = unix_timestamp_millis();
    decided_at(fixture, rows, fixture.actor.node_id, now).await
}

/// Decides `rows` as one batch from `sender` at `now`.
async fn decided_at(
    fixture: &Fixture,
    rows: &[ForwardRow],
    sender: NodeId,
    now: u64,
) -> Vec<Outcome> {
    let batch = EventBatch {
        events: rows.to_vec(),
        gaps: Vec::new(),
    };
    let nodes = (fixture.actor.node_id, sender);
    let config = config(fixture).await;
    let decisions = decide_batch(&fixture.ctx, &config, nodes, batch, now).await;
    (decisions.unwrap().events.into_iter())
        .map(|(.., outcome)| outcome)
        .collect()
}

async fn runs(fixture: &Fixture, install: &PluginInstall) -> Vec<PluginRun> {
    let listed = drive(list_runs(install.install_id, None, 1_000), &fixture.ctx).await;
    listed.unwrap().0
}

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

#[tokio::test]
async fn waits_for_record() {
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let mut newer = event(&install, "D1", unix_timestamp_millis()).1;
    newer.revision += 1;
    // The account's writes have not arrived either; the event waits and is never denied.
    let account = &install.prerequisites[0].target;
    let key = reducer_state_key(account).to_vec();
    let txn = Txn::direct(&fixture.ctx);
    txn.delete(DOCUMENT_STATE_KEYSPACE, vec![key])
        .await
        .unwrap();
    let current = event(&install, "D2", unix_timestamp_millis()).1;
    let outcomes = decided(&fixture, &[newer.clone(), current]).await;
    assert_eq!(outcomes, [Outcome::Waiting, Outcome::Waiting]);
    assert_eq!(decided(&fixture, &[newer]).await, [Outcome::Waiting]);
    let prefix = install.install_id.to_bytes();
    let slots = txn.scan(PLUGIN_QUEUE_KEYSPACE, &prefix).await.unwrap();
    assert_eq!(slots.len(), 2);
    assert!(drive(unfinished(), &fixture.ctx).await.unwrap().is_empty());
}

#[tokio::test]
async fn denied_once() {
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let (_, outside) = event(&install, "B/one", unix_timestamp_millis());
    let base = created(&install, "").1;
    let narrowed = scoped(vec![format!("{base}/A/**")]);
    let auth = fixture.auth();
    let id = install.install_id;
    set_grants(&fixture.ctx, &fixture.actor, &auth, id, &narrowed)
        .await
        .unwrap();
    for _ in 0..2 {
        let outcomes = decided(&fixture, std::slice::from_ref(&outside)).await;
        assert!(matches!(&outcomes[0], Outcome::Denied(reason) if reason.contains("not granted")));
    }
    assert!(runs(&fixture, &install).await.is_empty());
}

#[tokio::test]
async fn shared_bound() {
    // Timer runs, a waiting event and an event run share one bound; a resend gives no second run.
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    for seq in 0..RUNS_QUEUED as u64 - 2 {
        drive(admit(queued(install.install_id, seq + 10)), &fixture.ctx)
            .await
            .unwrap();
    }
    let now = unix_timestamp_millis();
    let mut waiting = event(&install, "D0", now).1;
    waiting.revision += 1;
    let (first, second) = (event(&install, "D1", now).1, event(&install, "D2", now).1);
    let outcomes = decided(&fixture, &[waiting, first.clone(), first, second.clone()]).await;
    let expected = [
        Outcome::Waiting,
        Outcome::Queued,
        Outcome::Queued,
        Outcome::Busy,
    ];
    assert_eq!(outcomes, expected);
    assert_eq!(decided(&fixture, &[second]).await, [Outcome::Busy]);
    assert_eq!(runs(&fixture, &install).await.len(), RUNS_QUEUED - 1);
}

#[tokio::test]
async fn newest_reports() {
    let fixture = Fixture::new().await;
    let install_id = Ulid::from_parts(1, 1);
    let report = |sequence, count| {
        let mut row = GapRow::new(SourceKind::DataUploaded, GapReason::Full);
        (row.sequence, row.count) = (sequence, count);
        (install_id, row)
    };
    let senders = [1u8, 2].map(|seed| iroh::SecretKey::from_bytes(&[seed; 32]).public());
    let sent = [
        (0, report(2, 5)),
        (0, report(2, 5)),
        (0, report(1, 1)),
        (1, report(1, 3)),
    ];
    let mut acked = Vec::new();
    for (sender, report) in sent {
        let kept = drive(keep_reports(senders[sender], vec![report]), &fixture.ctx).await;
        acked.push(kept.unwrap()[0].3);
    }
    assert_eq!(acked, [2, 2, 2, 1]);
    let txn = Txn::direct(&fixture.ctx);
    let rows = txn
        .scan(PLUGIN_GAPS_KEYSPACE, &[RECEIVER_GAP])
        .await
        .unwrap();
    let counts: Vec<u64> = (rows.iter())
        .map(|(_, value)| decode::<GapRow>(value).unwrap().count)
        .collect();
    assert_eq!(counts.iter().sum::<u64>(), 8);
}

const DAY: u64 = 24 * 3600 * 1000;

#[tokio::test]
async fn dedup_retention() {
    // Replayed on day 7, resent on day 10 after its run was pruned: the record still answers.
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let (anchor, local) = (unix_timestamp_millis(), fixture.actor.node_id);
    let row = event(&install, "D1", anchor).1;
    let first = decided_at(
        &fixture,
        std::slice::from_ref(&row),
        local,
        anchor + 7 * DAY,
    );
    assert_eq!(first.await, [Outcome::Queued]);
    let mut run = runs(&fixture, &install).await.remove(0);
    run.finish(RunState::Succeeded, None, anchor + 7 * DAY);
    drive(save_run(&run).unwrap(), &fixture.ctx).await.unwrap();
    prune_runs(&fixture.ctx, u64::MAX / 2).await.unwrap();
    assert_eq!(
        drive(prune_seen(anchor + 10 * DAY), &fixture.ctx).await,
        Ok(0)
    );
    let resent = decided_at(
        &fixture,
        std::slice::from_ref(&row),
        local,
        anchor + 10 * DAY,
    );
    assert_eq!(resent.await, [Outcome::Queued]);
    assert!(runs(&fixture, &install).await.is_empty());
    let expired = anchor + DEDUP_RETENTION_MS + 1;
    assert_eq!(drive(prune_seen(expired), &fixture.ctx).await, Ok(1));
}

#[tokio::test]
async fn delayed_creation_runs() {
    // A creation journaled eight days after its origin is still within the dedup horizon.
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let now = unix_timestamp_millis();
    let row = event(&install, "D1", now - 8 * DAY).1;
    let outcome = decided_at(&fixture, &[row], fixture.actor.node_id, now).await;
    assert_eq!(outcome, [Outcome::Queued]);
}

#[tokio::test]
async fn late_rows_refused() {
    // Rows past their anchor's retention, also another holder's late row, never run.
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let anchor = unix_timestamp_millis();
    let late = anchor + DEDUP_RETENTION_MS + 1;
    let holder = iroh::SecretKey::from_bytes(&[7u8; 32]).public();
    for (resource, sender) in [("D1", fixture.actor.node_id), ("D2", holder)] {
        let row = event(&install, resource, anchor).1;
        let outcome = decided_at(&fixture, &[row], sender, late).await;
        assert_eq!(outcome, [Outcome::Expired]);
    }
    assert!(runs(&fixture, &install).await.is_empty());
    let txn = Txn::direct(&fixture.ctx);
    let gaps = txn
        .scan(PLUGIN_GAPS_KEYSPACE, &[RECEIVER_GAP])
        .await
        .unwrap();
    let gap: GapRow = decode(&gaps[0].1).unwrap();
    assert_eq!((gaps.len(), gap.reason, gap.count), (1, GapReason::Late, 2));
}

#[tokio::test]
async fn delivery_totals() {
    // Gap reports from two senders add up; a waiting event is counted with its age.
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    for (seed, count) in [(1u8, 2), (2, 3)] {
        let mut row = GapRow::new(SourceKind::DataUploaded, GapReason::Full);
        (row.sequence, row.count, row.latest_ms) = (1, count, u64::from(seed));
        let sender = iroh::SecretKey::from_bytes(&[seed; 32]).public();
        let kept = drive(
            keep_reports(sender, vec![(install.install_id, row)]),
            &fixture.ctx,
        );
        kept.await.unwrap();
    }
    let mut waiting = event(&install, "D0", unix_timestamp_millis()).1;
    waiting.revision += 1;
    decided(&fixture, &[waiting]).await;
    let delivery = drive(event_delivery(install.install_id), &fixture.ctx)
        .await
        .unwrap();
    assert_eq!((delivery.waiting, delivery.gaps.len()), (1, 1));
    assert_eq!((delivery.gaps[0].count, delivery.gaps[0].latest_ms), (5, 2));
    assert!(delivery.oldest_ms.is_some());
}

/// A forward row of a fixed install, with no install record behind it.
fn plain_row(source: &str) -> ForwardRow {
    ForwardRow {
        install_id: Ulid::from_parts(1, 1),
        revision: 1,
        event: SourceEvent {
            kind: SourceKind::DatasetCreated,
            group_id: Ulid::from_parts(1, 9),
            path: "/r/g/x/meta/a@d".into(),
            resource: "d".into(),
            revision: "e".into(),
            source: source.into(),
            actor: None,
            anchor_ms: 5,
            journaled_ms: 5,
        },
        queued_ms: 5,
    }
}

#[test]
fn verdict_decisions() {
    let row = plain_row("s1");
    // An unavailable check is busy and needs no storage.
    let mut busy = decide_event(Verdict::Unavailable, &row, 9).unwrap();
    assert!(busy.start().is_empty());
    assert_eq!(busy.finalize(), Ok(Outcome::Busy));
    // A denial records its outcome at once.
    let mut plan = DecideEvent {
        row: Box::new(row.clone()),
        now_ms: 9,
        phase: DecidePhase::Denying("no".into()),
    };
    let Turn::Ask(Ask::Write(rows)) = plan.first().unwrap() else {
        panic!("a denial writes its dedup record");
    };
    assert_eq!(
        rows,
        [seen_row(&row, &Outcome::Denied("no".into()), 9).unwrap()]
    );
    let denied = Outcome::Denied("no".into());
    assert_eq!(plan.next(Answer::Done).unwrap(), Turn::Finish(denied));
    // An unready event holds a queue slot unless the queue is full.
    let mut hold = DecideEvent {
        row: Box::new(row.clone()),
        now_ms: 9,
        phase: DecidePhase::Holding(Hold::new(&row, 9).unwrap()),
    };
    hold.first().unwrap();
    let full = (0..RUNS_QUEUED)
        .map(|seq| (vec![seq as u8], Vec::new()))
        .collect();
    assert_eq!(
        hold.next(Answer::Page(full, None)).unwrap(),
        Turn::Finish(Outcome::Busy)
    );
    let mut slot = DecideEvent {
        row: Box::new(row.clone()),
        now_ms: 9,
        phase: DecidePhase::Holding(Hold::new(&row, 9).unwrap()),
    };
    slot.first().unwrap();
    let Turn::Ask(Ask::Write(rows)) = slot.next(Answer::Page(Vec::new(), None)).unwrap() else {
        panic!("a free slot is taken");
    };
    assert_eq!(rows.len(), 3);
    assert_eq!(
        slot.next(Answer::Done).unwrap(),
        Turn::Finish(Outcome::Waiting)
    );
    assert!(slot.next(Answer::Done).is_err());
}

#[test]
fn reports_keep_newest() {
    let sender = iroh::SecretKey::from_bytes(&[4; 32]).public();
    let install = Ulid::from_parts(1, 1);
    let report = |sequence| {
        let mut row = GapRow::new(SourceKind::DatasetCreated, GapReason::Full);
        row.sequence = sequence;
        (install, row)
    };
    let mut plan = KeepReports {
        sender,
        reports: vec![report(3)],
        acked: None,
    };
    let Turn::Ask(Ask::Read(reads)) = plan.first().unwrap() else {
        panic!("the kept reports are read first");
    };
    assert_eq!(reads.len(), 1);
    // A newer kept report wins and nothing is written.
    let kept = Answer::Values(vec![Some(encode(&report(5).1).unwrap())]);
    assert_eq!(plan.next(kept).unwrap(), Turn::Ask(Ask::Write(Vec::new())));
    let acked = vec![(install, SourceKind::DatasetCreated, GapReason::Full, 5)];
    assert_eq!(plan.next(Answer::Done).unwrap(), Turn::Finish(acked));
}

#[test]
fn gap_counts_once() {
    let local = iroh::SecretKey::from_bytes(&[4; 32]).public();
    let mut gap = GapCount::new(local, &plain_row("s1"), GapReason::Late);
    let Some(counted) = gap.take(Answer::Values(vec![None])).unwrap() else {
        panic!("the first answer counts the gap");
    };
    let row: GapRow = decode(&counted.2).unwrap();
    assert_eq!((row.count, row.earliest_ms), (1, 5));
    assert_eq!(gap.take(Answer::Done).unwrap(), None);
}

#[test]
fn seen_records_expire() {
    let mut plan = PruneSeen {
        now_ms: DEDUP_RETENTION_MS + 10,
        after: None,
        deleting: None,
        pruned: 0,
    };
    assert!(matches!(plan.first().unwrap(), Turn::Ask(Ask::Page { .. })));
    let seen = |anchor_ms| {
        let record = SeenRow {
            outcome: Outcome::Queued,
            anchor_ms,
            decided_ms: anchor_ms,
        };
        encode(&record).unwrap()
    };
    let page = vec![(b"old".to_vec(), seen(1)), (b"new".to_vec(), seen(20))];
    let next = plan
        .next(Answer::Page(page, Some(b"new".to_vec())))
        .unwrap();
    let deleted = Ask::Delete(vec![(PLUGIN_SEEN_KEYSPACE, b"old".to_vec())]);
    assert_eq!(next, Turn::Ask(deleted));
    let Turn::Ask(Ask::Page { after, .. }) = plan.next(Answer::Done).unwrap() else {
        panic!("the next page follows");
    };
    assert_eq!(after, Some(b"new".to_vec()));
    assert_eq!(
        plan.next(Answer::Page(Vec::new(), None)).unwrap(),
        Turn::Ask(Ask::Delete(Vec::new()))
    );
    assert_eq!(plan.next(Answer::Done).unwrap(), Turn::Finish(1));
}
