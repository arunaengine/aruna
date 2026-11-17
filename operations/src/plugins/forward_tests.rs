//! Tests forwarding against a fake receiver: decided rows leave, busy and unanswered rows stay,
//! expired rows become gaps that are resent until acknowledged, metadata waits, removal drops.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::{Arc, Mutex};

use aruna_core::plugin_installs::PluginInstall;
use aruna_core::plugin_runs::{SourceEvent, SourceKind};
use aruna_core::time::unix_timestamp_millis;

use super::*;
use crate::plugins::install::tests::Fixture;
use crate::plugins::matcher::tests::{config, forwarded, gap, journal};
use crate::plugins::store::Txn;
use crate::plugins::worker::match_journal;
use crate::plugins::worker::tests::{created, installed};
use crate::plugins::worker::{send_bounded, send_outbox, unreadable};
use aruna_core::operation::Operation;
use aruna_core::plugin_runs::EVENTS_QUEUED;

/// The batches a fake receiver got; it answers each event with `decide` and acknowledges gaps
/// when `ack` is set.
#[derive(Clone, Default)]
struct Receiver {
    got: Arc<Mutex<Vec<EventBatch>>>,
    ack: Arc<Mutex<bool>>,
}

impl Receiver {
    fn send(
        &self,
        decide: fn(&str) -> Option<Outcome>,
    ) -> impl Fn(NodeId, EventBatch) -> std::future::Ready<Result<Decisions, String>> {
        move |_, batch: EventBatch| {
            let events = (batch.events.iter())
                .filter_map(|row| {
                    let outcome = decide(&row.event.resource)?;
                    Some((row.install_id, row.event.source.clone(), outcome))
                })
                .collect();
            let gaps = match *self.ack.lock().unwrap() {
                true => (batch.gaps.iter())
                    .map(|(id, row)| (*id, row.kind, row.reason, row.sequence))
                    .collect(),
                false => Vec::new(),
            };
            self.got.lock().unwrap().push(batch);
            std::future::ready(Ok(Decisions { events, gaps }))
        }
    }

    fn batches(&self) -> usize {
        self.got.lock().unwrap().len()
    }
}

/// Journals and matches created datasets at successive times from `at_ms`.
async fn queued(fixture: &Fixture, install: &PluginInstall, names: &[&str], at_ms: u64) {
    for (n, name) in names.iter().enumerate() {
        let mut event = created(install, name).0;
        event.journaled_ms = at_ms + n as u64;
        journal(fixture, &event).await;
    }
    let local = fixture.actor.node_id;
    let config = config(fixture).await;
    match_journal(&fixture.ctx, &config, local, (at_ms, EVENTS_QUEUED))
        .await
        .unwrap();
}

fn queued_or_busy(resource: &str) -> Option<Outcome> {
    match resource {
        "D1" => Some(Outcome::Queued),
        "D2" => Some(Outcome::Busy),
        _ => None,
    }
}

#[tokio::test]
async fn decided_rows() {
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let now = unix_timestamp_millis();
    queued(&fixture, &install, &["D1", "D2", "D3"], now).await;
    let (config, receiver) = (config(&fixture).await, Receiver::default());
    let mut backoff = Backoff::default();
    let send = receiver.send(queued_or_busy);
    let decided = send_outbox(&fixture.ctx, &config, now, &mut backoff, &send).await;
    assert_eq!(decided, Ok(1));
    assert_eq!(forwarded(&fixture, &install).await, ["D2", "D3"]);
    // A busy receiver waits before the next send.
    send_outbox(&fixture.ctx, &config, now + 1, &mut backoff, &send)
        .await
        .unwrap();
    assert_eq!(receiver.batches(), 1);
    let failing = |_, _| std::future::ready(Err::<Decisions, _>("down".to_string()));
    let later = now + MAX_BACKOFF_MS;
    send_outbox(&fixture.ctx, &config, later, &mut backoff, failing)
        .await
        .unwrap();
    assert_eq!(forwarded(&fixture, &install).await, ["D2", "D3"]);
}

#[tokio::test]
async fn expired_gaps() {
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let at = unix_timestamp_millis();
    queued(&fixture, &install, &["D1"], at).await;
    let (config, receiver) = (config(&fixture).await, Receiver::default());
    let (mut backoff, send) = (Backoff::default(), receiver.send(|_| None));
    let now = at + SEND_EXPIRY_MS + 1;
    send_outbox(&fixture.ctx, &config, now, &mut backoff, &send)
        .await
        .unwrap();
    assert!(forwarded(&fixture, &install).await.is_empty());
    let sent = receiver.got.lock().unwrap()[0].clone();
    assert_eq!((sent.events.len(), sent.gaps[0].1.sequence), (0, 1));
    // Resent until acknowledged, then quiet.
    send_outbox(&fixture.ctx, &config, now, &mut backoff, &send)
        .await
        .unwrap();
    *receiver.ack.lock().unwrap() = true;
    send_outbox(&fixture.ctx, &config, now, &mut backoff, &send)
        .await
        .unwrap();
    send_outbox(&fixture.ctx, &config, now, &mut backoff, &send)
        .await
        .unwrap();
    assert_eq!(receiver.batches(), 3);
    // A new gap raises the sequence of the same row.
    queued(&fixture, &install, &["D2"], at).await;
    send_outbox(&fixture.ctx, &config, now, &mut backoff, &send)
        .await
        .unwrap();
    let row = gap(&fixture, &install, GapReason::Expired).await.unwrap();
    assert_eq!((row.count, row.sequence, row.acked), (2, 2, 2));
    assert_eq!(receiver.got.lock().unwrap()[3].gaps.len(), 1);
}

/// Journals and matches the creation of a fresh document; returns its document and event ids.
async fn creation(fixture: &Fixture, install: &PluginInstall, now: u64) -> (Ulid, Ulid) {
    let (document, event_id) = (Ulid::generate(), Ulid::generate());
    let event = SourceEvent {
        resource: document.to_string(),
        revision: event_id.to_string(),
        journaled_ms: now,
        actor: None,
        ..created(install, &document.to_string()).0
    };
    journal(fixture, &event).await;
    let (local, config) = (fixture.actor.node_id, config(fixture).await);
    match_journal(&fixture.ctx, &config, local, (now, EVENTS_QUEUED))
        .await
        .unwrap();
    (document, event_id)
}

/// Stores the document's materialization status in `state` at `event_id`.
async fn materialization(fixture: &Fixture, (document, event_id): (Ulid, Ulid), state: State) {
    let record = MaterializationStatusRecord {
        document_id: document,
        event_id,
        graph_iri: MetadataRegistryRecord::graph_iri_for(document),
        context_digest: None,
        dataset_digest: None,
        state,
        attempts: 1,
        failures: 1,
        last_error: None,
        updated_at_ms: 1,
    };
    let row = (
        materialization_status_key(document).to_vec(),
        encode(&record).unwrap(),
    );
    let txn = Txn::direct(&fixture.ctx);
    txn.write(MATERIALIZATION_STATUS_KEYSPACE, vec![row])
        .await
        .unwrap();
}

type State = MaterializationState;

#[tokio::test]
async fn metadata_waits() {
    // A parked job holds the creation until its send expiry counts a gap; it is never sent.
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let now = unix_timestamp_millis();
    let parked = creation(&fixture, &install, now).await;
    let config = config(&fixture).await;
    let receiver = Receiver::default();
    let (mut backoff, send) = (Backoff::default(), receiver.send(|_| Some(Outcome::Queued)));
    send_outbox(&fixture.ctx, &config, now, &mut backoff, &send)
        .await
        .unwrap();
    materialization(&fixture, parked, State::Failed).await;
    send_outbox(&fixture.ctx, &config, now, &mut backoff, &send)
        .await
        .unwrap();
    assert_eq!(receiver.batches(), 0);
    let expired = now + SEND_EXPIRY_MS + 1;
    send_outbox(&fixture.ctx, &config, expired, &mut backoff, &send)
        .await
        .unwrap();
    assert!(receiver.got.lock().unwrap()[0].events.is_empty());
    let gap = gap(&fixture, &install, GapReason::Expired).await.unwrap();
    assert_eq!(gap.count, 1);

    // A materialized creation and the creation of a deleted document are sent.
    let done = creation(&fixture, &install, now).await;
    materialization(&fixture, done, State::Materialized).await;
    let deleted = creation(&fixture, &install, now).await;
    let graph = MetadataRegistryRecord::graph_iri_for(deleted.0);
    let realm_id = fixture.actor.realm_id;
    let record =
        GraphLifecycleRecord::deleted(graph.clone(), realm_id, fixture.group_id, deleted.0, 1);
    let row = (
        graph_lifecycle_key(&graph).to_vec(),
        encode(&record).unwrap(),
    );
    let txn = Txn::direct(&fixture.ctx);
    txn.write(GRAPH_LIFECYCLE_KEYSPACE, vec![row])
        .await
        .unwrap();
    let decided = send_outbox(&fixture.ctx, &config, now, &mut backoff, &send).await;
    assert_eq!(decided, Ok(2));
}

#[tokio::test]
async fn removed_install_drops() {
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let now = unix_timestamp_millis();
    queued(&fixture, &install, &["D1"], now).await;
    let mut config = config(&fixture).await;
    config.installs.clear();
    let receiver = Receiver::default();
    let (mut backoff, send) = (Backoff::default(), receiver.send(|_| None));
    send_outbox(&fixture.ctx, &config, now, &mut backoff, &send)
        .await
        .unwrap();
    assert!(forwarded(&fixture, &install).await.is_empty());
    assert_eq!(gap(&fixture, &install, GapReason::Expired).await, None);
    assert_eq!(receiver.batches(), 0);
}

#[tokio::test]
async fn bounded_requests() {
    // Requests to one receiver are split by encoded size, gap reports included.
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let now = unix_timestamp_millis();
    queued(&fixture, &install, &["D1", "D2", "D3", "D4", "D5"], now).await;
    let mut report = GapRow::new(SourceKind::DataUploaded, GapReason::Full);
    report.add(now);
    let report_key = gap_key(
        install.install_id,
        SourceKind::DataUploaded,
        GapReason::Full,
    );
    let txn = Txn::direct(&fixture.ctx);
    txn.write(
        PLUGIN_GAPS_KEYSPACE,
        vec![(report_key, encode(&report).unwrap())],
    )
    .await
    .unwrap();
    let rows = txn.scan(PLUGIN_OUTBOX_KEYSPACE, &[]).await.unwrap();
    let bound = 2 * rows[0].1.len();
    let (config, receiver) = (config(&fixture).await, Receiver::default());
    let (mut backoff, send) = (Backoff::default(), receiver.send(|_| Some(Outcome::Queued)));
    let decided = send_bounded(&fixture.ctx, &config, (now, bound), &mut backoff, &send).await;
    assert_eq!(decided, Ok(5));
    let got = receiver.got.lock().unwrap().clone();
    assert!(got.len() >= 3, "{} requests", got.len());
    for batch in &got {
        let events = (batch.events.iter()).map(|row| encode(row).unwrap().len());
        let gaps = (batch.gaps.iter()).map(|(_, row)| encode(row).unwrap().len() + 16);
        assert!(events.chain(gaps).sum::<usize>() <= bound);
    }
    assert_eq!(got.iter().map(|batch| batch.gaps.len()).sum::<usize>(), 1);
    assert!(forwarded(&fixture, &install).await.is_empty());
}

// A change waits for evidence of its own event: a logged event and a status naming it. A newer
// status alone does not release it, a deleted document does.
#[tokio::test]
async fn update_waits() {
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let (document, event_id) = (Ulid::generate(), Ulid::from_parts(10, 1));
    let event = SourceEvent {
        kind: SourceKind::DatasetUpdated,
        resource: document.to_string(),
        revision: event_id.to_string(),
        ..created(&install, &document.to_string()).0
    };
    let row = ForwardRow {
        install_id: install.install_id,
        revision: 1,
        event,
        queued_ms: 1,
    };
    let waits = async || unreadable(&fixture.ctx, &row).await;
    let newer = (document, Ulid::from_parts(20, 1));
    materialization(&fixture, newer, State::Materialized).await;
    assert_eq!(waits().await, Ok(true));

    let realm_id = fixture.actor.realm_id;
    let graph_iri = MetadataRegistryRecord::graph_iri_for(document);
    let record = MetadataRegistryRecord {
        realm_id,
        group_id: fixture.group_id,
        document_id: document,
        document_path: "datasets/a".into(),
        graph_iri: graph_iri.clone(),
        public: false,
        permission_path: row.event.path.clone(),
        placement: aruna_core::structs::placement::record::PlacementRef::NIL,
        holder_node_ids: Vec::new(),
        created_at_ms: 1,
        updated_at_ms: 1,
        establishing_event_id: event_id,
        last_event_id: event_id,
    };
    let logged = MetadataEventRecord {
        event_id,
        record,
        user_id: fixture.actor.user_id,
        node_id: fixture.actor.node_id,
        payload: MetadataEventPayload::UpsertDataEntity {
            jsonld: "{}".into(),
        },
        occurred_at_ms: 1,
    };
    let entry = (
        event_log_key(document, event_id).to_vec(),
        encode(&logged).unwrap(),
    );
    let txn = Txn::direct(&fixture.ctx);
    txn.write(EVENT_LOG_KEYSPACE, vec![entry]).await.unwrap();
    assert_eq!(waits().await, Ok(true));
    materialization(&fixture, (document, event_id), State::Materialized).await;
    assert_eq!(waits().await, Ok(false));

    materialization(&fixture, newer, State::Materialized).await;
    let deleted =
        GraphLifecycleRecord::deleted(graph_iri.clone(), realm_id, fixture.group_id, document, 1);
    let lifecycle = (
        graph_lifecycle_key(&graph_iri).to_vec(),
        encode(&deleted).unwrap(),
    );
    txn.write(GRAPH_LIFECYCLE_KEYSPACE, vec![lifecycle])
        .await
        .unwrap();
    assert_eq!(waits().await, Ok(false));
}

#[test]
fn apply_decisions() {
    let install = Ulid::from_parts(1, 1);
    let keys = BTreeMap::from([
        ((install, "s1".to_string()), b"k1".to_vec()),
        ((install, "s2".to_string()), b"k2".to_vec()),
    ]);
    let decisions = Decisions {
        events: vec![
            (install, "s1".into(), Outcome::Queued),
            (install, "s2".into(), Outcome::Busy),
        ],
        gaps: vec![(install, SourceKind::DatasetCreated, GapReason::Full, 4)],
    };
    let mut plan = Apply {
        count: 1,
        busy: true,
        decided: Some(vec![(PLUGIN_OUTBOX_KEYSPACE, b"k1".to_vec())]),
        acked: vec![(
            gap_key(install, SourceKind::DatasetCreated, GapReason::Full),
            4,
        )],
        phase: ApplyPhase::Deleting,
    };
    assert_eq!(apply(&keys, decisions).start().len(), 1);
    // A busy event keeps its row; the acknowledged sequence stops at the counted one.
    let decided = vec![(PLUGIN_OUTBOX_KEYSPACE, b"k1".to_vec())];
    assert_eq!(plan.first().unwrap(), Turn::Ask(Ask::Delete(decided)));
    assert!(matches!(
        plan.next(Answer::Done).unwrap(),
        Turn::Ask(Ask::Read(_))
    ));
    let mut row = GapRow::new(SourceKind::DatasetCreated, GapReason::Full);
    row.sequence = 3;
    let read = Answer::Values(vec![Some(encode(&row).unwrap())]);
    let Turn::Ask(Ask::Write(rows)) = plan.next(read).unwrap() else {
        panic!("the acknowledgement is written");
    };
    let written: GapRow = decode(&rows[0].2).unwrap();
    assert_eq!(written.acked, 3);
    assert_eq!(plan.next(Answer::Done).unwrap(), Turn::Finish((1, true)));
}

#[test]
fn expired_rows_counted() {
    let install = Ulid::from_parts(1, 1);
    let row = |queued_ms| ForwardRow {
        install_id: install,
        revision: 1,
        event: SourceEvent {
            kind: SourceKind::DatasetCreated,
            group_id: Ulid::from_parts(1, 9),
            path: "/r/g/x/meta/a@d".into(),
            resource: "d".into(),
            revision: "e".into(),
            source: format!("s{queued_ms}"),
            actor: None,
            anchor_ms: queued_ms,
            journaled_ms: queued_ms,
        },
        queued_ms,
    };
    let now = SEND_EXPIRY_MS + 10;
    let mut plan = ExpireRows {
        install_id: install,
        now_ms: now,
        phase: ExpirePhase::Reading,
    };
    assert!(matches!(plan.first().unwrap(), Turn::Ask(Ask::Page { .. })));
    let (old, kept) = (encode(&row(1)).unwrap(), encode(&row(now)).unwrap());
    let page = vec![(b"o".to_vec(), old), (b"k".to_vec(), kept.clone())];
    let Turn::Ask(Ask::Read(reads)) = plan.next(Answer::Page(page, None)).unwrap() else {
        panic!("the expired row's gap is read");
    };
    assert_eq!(reads.len(), 1);
    let Turn::Ask(Ask::Write(rows)) = plan.next(Answer::Values(vec![None])).unwrap() else {
        panic!("the gap is written");
    };
    assert_eq!(rows.len(), 1);
    let deleted = Ask::Delete(vec![(PLUGIN_OUTBOX_KEYSPACE, b"o".to_vec())]);
    assert_eq!(plan.next(Answer::Done).unwrap(), Turn::Ask(deleted));
    let rest = vec![(b"k".to_vec(), kept)];
    assert_eq!(plan.next(Answer::Done).unwrap(), Turn::Finish(rest));
}

#[test]
fn next_after_install() {
    let id = Ulid::from_parts(3, 3);
    let start = [&id.to_bytes()[..], &[0xff; 24]].concat();
    let mut plan = NextInstall(Some(start.clone()));
    let Turn::Ask(Ask::Page { after, limit, .. }) = plan.first().unwrap() else {
        panic!("the next install reads one row");
    };
    assert_eq!((after, limit), (Some(start), 1));
    let key = [&id.to_bytes()[..], &[0; 24]].concat();
    let found = plan.next(Answer::Page(vec![(key, Vec::new())], None));
    assert_eq!(found.unwrap(), Turn::Finish(Some(id)));
    let mut empty = NextInstall(None);
    let none = empty.next(Answer::Page(Vec::new(), None)).unwrap();
    assert_eq!(none, Turn::Finish(None));
    assert!(empty.next(Answer::Done).is_err());
}

#[test]
fn drops_every_page() {
    let space = PLUGIN_OUTBOX_KEYSPACE;
    let mut plan = DropRows {
        space,
        prefix: b"p".to_vec(),
        deleting: false,
    };
    let first = plan.first().unwrap();
    assert!(matches!(first, Turn::Ask(Ask::Page { limit: 1_000, .. })));
    let rows = vec![(b"p1".to_vec(), b"x".to_vec())];
    let asked = plan.next(Answer::Page(rows, None)).unwrap();
    assert_eq!(asked, Turn::Ask(Ask::Delete(vec![(space, b"p1".to_vec())])));
    let again = plan.next(Answer::Done).unwrap();
    assert!(matches!(again, Turn::Ask(Ask::Page { .. })));
    let last = plan.next(Answer::Page(Vec::new(), None)).unwrap();
    assert_eq!(last, Turn::Finish(()));
}
