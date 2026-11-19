//! Tests forwarding against a fake receiver: decided rows leave, busy and unanswered rows stay,
//! expired rows become gaps that are resent until acknowledged, metadata waits, removal drops.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::{Arc, Mutex};

use aruna_core::plugin_installs::PluginInstall;
use aruna_core::plugin_runs::{SourceEvent, SourceKind};
use aruna_core::time::unix_timestamp_millis;

use super::*;
use crate::driver::drive;
use crate::plugins::calls::*;
use crate::plugins::install::tests::Fixture;
use crate::plugins::matcher::tests::{config, forwarded, gap, journal};
use crate::plugins::rows::tests::child_result;
use crate::plugins::rows::tests::{page_result, values_result};
use crate::plugins::store::Txn;
use crate::plugins::test_support::{created, installed};
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
    let later = now + u64::try_from(MAX_BACKOFF.as_millis()).unwrap();
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
    let metadata = fixture.ctx.metadata_handle.is_some();
    let waits = async || drive(Unreadable::new(&row, metadata), &fixture.ctx).await;
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
    let first = apply(&keys, decisions).first().unwrap();
    assert!(matches!(first, Next::Call(IoCall::Delete(_))));
    // A busy event keeps its row; the acknowledged sequence stops at the counted one.
    let decided = vec![(PLUGIN_OUTBOX_KEYSPACE, b"k1".to_vec())];
    assert_eq!(plan.first().unwrap(), Next::Call(delete_call(decided)));
    assert!(matches!(
        plan.next(IoResult::Done).unwrap(),
        Next::Call(IoCall::Read(_))
    ));
    let mut row = GapRow::new(SourceKind::DatasetCreated, GapReason::Full);
    row.sequence = 3;
    let read = values_result(vec![Some(encode(&row).unwrap())]);
    let Next::Call(IoCall::Write(rows)) = plan.next(read).unwrap() else {
        panic!("the acknowledgement is written");
    };
    let written: GapRow = decode(&rows[0].2).unwrap();
    assert_eq!(written.acked, 3);
    assert_eq!(plan.next(IoResult::Done).unwrap(), Next::Done((1, true)));
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
    assert!(matches!(
        plan.first().unwrap(),
        Next::Call(IoCall::Page { .. })
    ));
    let (old, kept) = (encode(&row(1)).unwrap(), encode(&row(now)).unwrap());
    let page = vec![(b"o".to_vec(), old), (b"k".to_vec(), kept.clone())];
    let Next::Call(IoCall::Read(reads)) = plan.next(page_result(page, None)).unwrap() else {
        panic!("the expired row's gap is read");
    };
    assert_eq!(reads.len(), 1);
    let Next::Call(IoCall::Write(rows)) = plan.next(values_result(vec![None])).unwrap() else {
        panic!("the gap is written");
    };
    assert_eq!(rows.len(), 1);
    let deleted = delete_call(vec![(PLUGIN_OUTBOX_KEYSPACE, b"o".to_vec())]);
    assert_eq!(plan.next(IoResult::Done).unwrap(), Next::Call(deleted));
    let rest = vec![(b"k".to_vec(), kept)];
    assert_eq!(plan.next(IoResult::Done).unwrap(), Next::Done(rest));
}

#[test]
fn next_after_install() {
    let id = Ulid::from_parts(3, 3);
    let start = [&id.to_bytes()[..], &[0xff; 24]].concat();
    let mut plan = NextInstall(Some(start.clone()));
    let Next::Call(IoCall::Page { after, limit, .. }) = plan.first().unwrap() else {
        panic!("the next install reads one row");
    };
    assert_eq!((after.map(|key| key.to_vec()), limit), (Some(start), 1));
    let key = [&id.to_bytes()[..], &[0; 24]].concat();
    let found = plan.next(page_result(vec![(key, Vec::new())], None));
    assert_eq!(found.unwrap(), Next::Done(Some(id)));
    let mut empty = NextInstall(None);
    let none = empty.next(page_result(Vec::new(), None)).unwrap();
    assert_eq!(none, Next::Done(None));
    assert!(empty.next(IoResult::Done).is_err());
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
    assert!(matches!(
        first,
        Next::Call(IoCall::Page { limit: 1_000, .. })
    ));
    let rows = vec![(b"p1".to_vec(), b"x".to_vec())];
    let asked = plan.next(page_result(rows, None)).unwrap();
    assert_eq!(
        asked,
        Next::Call(delete_call(vec![(space, b"p1".to_vec())]))
    );
    let again = plan.next(IoResult::Done).unwrap();
    assert!(matches!(again, Next::Call(IoCall::Page { .. })));
    let last = plan.next(page_result(Vec::new(), None)).unwrap();
    assert_eq!(last, Next::Done(()));
}

fn forward_row(kind: SourceKind, resource: String, revision: String) -> ForwardRow {
    ForwardRow {
        install_id: Ulid::from_parts(1, 1),
        revision: 1,
        event: SourceEvent {
            kind,
            group_id: Ulid::from_parts(1, 9),
            path: "/r/g/x/meta/a@d".into(),
            resource,
            revision,
            source: "s".into(),
            actor: None,
            anchor_ms: 5,
            journaled_ms: 5,
        },
        queued_ms: 5,
    }
}

fn dataset_row(kind: SourceKind) -> ForwardRow {
    let (document, event) = (Ulid::from_parts(5, 5), Ulid::from_parts(6, 6));
    forward_row(kind, document.to_string(), event.to_string())
}

#[test]
fn other_events_readable() {
    let upload = dataset_row(SourceKind::DataUploaded);
    assert_eq!(
        Unreadable::new(&upload, true).first(),
        Ok(Next::Done(false))
    );
    let named = forward_row(SourceKind::DatasetCreated, "d".into(), "e".into());
    assert_eq!(Unreadable::new(&named, true).first(), Ok(Next::Done(false)));
}

#[test]
fn unmaterialized_update_unreadable() {
    let row = dataset_row(SourceKind::DatasetUpdated);
    let mut check = Unreadable::new(&row, true);
    assert!(matches!(check.first(), Ok(Next::Call(IoCall::Child(_)))));
    let facts = child_result(Ok::<_, StoreError>(Facts::default()));
    assert_eq!(check.next(facts), Ok(Next::Done(true)));
    let mut deleted = Unreadable::new(&row, true);
    let gone = Facts {
        deleted: true,
        ..Facts::default()
    };
    let answer = deleted.next(child_result(Ok::<_, StoreError>(gone)));
    assert_eq!(answer, Ok(Next::Done(false)));
}

fn collecting(config: RealmConfigDocument) -> CollectOutbox {
    let open = BTreeSet::from([iroh::SecretKey::from_bytes(&[3u8; 32]).public()]);
    CollectOutbox::new(Arc::new(config), open, (9, 1 << 20), true)
}

fn bare_config() -> RealmConfigDocument {
    let realm = aruna_core::structs::identity::realm::RealmId::from_bytes([1u8; 32]);
    RealmConfigDocument::new(realm, Vec::new(), 3)
}

#[test]
fn collects_nothing_empty() {
    let mut collect = collecting(bare_config());
    assert!(matches!(collect.first(), Ok(Next::Call(IoCall::Child(_)))));
    let none = child_result(Ok::<Option<Ulid>, StoreError>(None));
    let scan = collect.next(none);
    assert!(matches!(scan, Ok(Next::Call(IoCall::Page { .. }))));
    assert_eq!(
        collect.next(page_result(Vec::new(), None)),
        Ok(Next::Done(BTreeMap::new()))
    );
}

#[test]
fn drops_removed_install() {
    let mut collect = collecting(bare_config());
    let found = child_result(Ok::<_, StoreError>(Some(Ulid::from_parts(7, 7))));
    assert!(matches!(
        collect.next(found),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(collect.phase, CollectPhase::Dropping);
    let dropped = child_result(Ok::<(), StoreError>(()));
    assert!(matches!(
        collect.next(dropped),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(collect.phase, CollectPhase::NextInstall);
}

#[test]
fn readable_row_joins() {
    let mut config = bare_config();
    let install = crate::plugins::work::tests::sample_install(Ulid::from_parts(1, 1));
    let receiver = install.receiver;
    config.installs.push(install.clone());
    let mut collect = collecting(config);
    let found = child_result(Ok::<_, StoreError>(Some(install.install_id)));
    assert!(matches!(
        collect.next(found),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let row = forward_row(SourceKind::DataUploaded, "d".into(), "e".into());
    let kept: Rows = vec![(b"k".to_vec(), encode(&row).unwrap())];
    let check = collect.next(child_result(Ok::<_, StoreError>(kept)));
    assert!(matches!(check, Ok(Next::Call(IoCall::Child(_)))));
    let next = collect.next(child_result(Ok::<bool, StoreError>(false)));
    assert!(matches!(next, Ok(Next::Call(IoCall::Child(_)))));
    let none = child_result(Ok::<Option<Ulid>, StoreError>(None));
    collect.next(none).unwrap();
    let Next::Done(batches) = collect.next(page_result(Vec::new(), None)).unwrap() else {
        panic!("the gap scan ends the pass");
    };
    assert_eq!(batches[&receiver].open[0].0.0.events, [row]);
}

mod operations {
    use super::*;
    use crate::plugins::fixtures::{is_effect, node, peer_says, peer_silent};

    #[test]
    fn events_reach_receiver() {
        let mut send = SendEvents::new(node(1), EventBatch::default());
        assert!(is_effect(&send.first()));
        let result = Ok(Decisions::default());
        let said = peer_says(node(1), &MetadataTransportMessage::EventsDecided { result });
        assert_eq!(send.next(said), Ok(Next::Done(Decisions::default())));
        let silent = send.next(peer_silent(node(1)));
        assert_eq!(silent, Err(MetadataReadError::Unavailable));
    }
}

mod outbox_pass {
    use serde_json::json;

    use super::*;
    use crate::plugins::fixtures::{self, is_child, is_effect, node};

    fn pass(route: OutboxRoute) -> SendOutbox {
        let config = Arc::new(fixtures::config(&fixtures::install(), json!({})));
        SendOutbox::new(config, (route, node(3)), (50, true), Backoff::default())
    }

    fn collected(peer: NodeId, count: usize) -> IoResult {
        let pending = || ((EventBatch::default(), BTreeMap::new()), 0);
        let requests = Requests {
            bound: REQUEST_BYTES,
            open: (0..count).map(|_| pending()).collect(),
        };
        child_result(Ok::<_, StoreError>(BTreeMap::from([(peer, requests)])))
    }

    fn decided() -> IoResult {
        child_result(Ok::<_, MetadataReadError>(Decisions::default()))
    }

    fn done(next: Result<Next<OutboxPass>, StoreError>) -> OutboxPass {
        let Ok(Next::Done(pass)) = next else {
            panic!("the pass should be over");
        };
        pass
    }

    #[test]
    fn outbox_holds_nothing() {
        assert_eq!(pass(OutboxRoute::Local).hold_scope(), None);
        assert_eq!(pass(OutboxRoute::Remote).hold_scope(), None);
    }

    #[test]
    fn remote_applies_decisions() {
        let mut send = pass(OutboxRoute::Remote);
        assert!(is_child(&send.first()));
        assert!(is_child(&send.next(collected(node(1), 1))));
        assert!(is_child(&send.next(decided())));
        let applied = child_result(Ok::<_, StoreError>((2usize, false)));
        let over = done(send.next(applied));
        assert_eq!((over.decided, over.failure), (2, None));
        assert!(over.backoff.ready(node(1), 50));
    }

    #[test]
    fn failed_receiver_waits() {
        let mut send = pass(OutboxRoute::Remote);
        send.first().unwrap();
        send.next(collected(node(1), 2)).unwrap();
        let failed = Err::<Decisions, _>(MetadataReadError::Unavailable);
        let over = done(send.next(child_result(failed)));
        assert!(!over.backoff.ready(node(1), 50));
    }

    #[test]
    fn busy_receiver_waits() {
        let mut send = pass(OutboxRoute::Remote);
        send.first().unwrap();
        send.next(collected(node(1), 1)).unwrap();
        send.next(decided()).unwrap();
        let busy = child_result(Ok::<_, StoreError>((1usize, true)));
        let over = done(send.next(busy));
        assert_eq!(over.decided, 1);
        assert!(!over.backoff.ready(node(1), 50));
    }

    #[test]
    fn local_decides_here() {
        let mut send = pass(OutboxRoute::Local);
        send.first().unwrap();
        let asked = send.next(collected(node(3), 1));
        assert!(is_child(&asked) && !is_effect(&asked));
        let answer = child_result(Ok::<_, StoreError>(Decisions::default()));
        assert!(is_child(&send.next(answer)));
    }

    #[test]
    fn failure_keeps_backoff() {
        let mut send = pass(OutboxRoute::Remote);
        send.backoff.failed(node(2), 50);
        send.first().unwrap();
        let failed = StoreError("no".to_string());
        let answer = child_result(Err::<BTreeMap<NodeId, Requests>, _>(failed.clone()));
        let over = done(send.next(answer));
        assert_eq!(over.failure, Some(failed));
        assert!(!over.backoff.ready(node(2), 50));
    }
}
