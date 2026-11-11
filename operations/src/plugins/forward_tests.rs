//! Tests forwarding against a fake receiver: decided rows leave, busy and unanswered rows stay,
//! expired rows become gaps that are resent until acknowledged, metadata waits, removal drops.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::{Arc, Mutex};

use aruna_core::plugin_installs::PluginInstall;
use aruna_core::plugin_runs::SourceEvent;
use aruna_core::time::unix_timestamp_millis;

use super::*;
use crate::plugins::install::tests::Fixture;
use crate::plugins::matcher::match_pending;
use crate::plugins::matcher::tests::{config, forwarded, gap, journal};
use crate::plugins::worker::tests::{created, installed};

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
    match_pending(&fixture.ctx, &config, local, at_ms)
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

#[tokio::test]
async fn metadata_waits() {
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let now = unix_timestamp_millis();
    let (document, event_id) = (Ulid::from_parts(now, 1), Ulid::from_parts(now, 2));
    let event = SourceEvent {
        resource: document.to_string(),
        revision: event_id.to_string(),
        journaled_ms: now,
        ..created(&install, &document.to_string()).0
    };
    journal(&fixture, &event).await;
    let (local, config) = (fixture.actor.node_id, config(&fixture).await);
    match_pending(&fixture.ctx, &config, local, now)
        .await
        .unwrap();
    let job = (
        document_job_key(document, event_id).to_vec(),
        b"job".to_vec(),
    );
    let txn = Txn::direct(&fixture.ctx);
    txn.write(DOCUMENT_JOB_KEYSPACE, vec![job.clone()])
        .await
        .unwrap();
    let receiver = Receiver::default();
    let (mut backoff, send) = (Backoff::default(), receiver.send(|_| Some(Outcome::Queued)));
    send_outbox(&fixture.ctx, &config, now, &mut backoff, &send)
        .await
        .unwrap();
    assert_eq!(receiver.batches(), 0);
    txn.delete(DOCUMENT_JOB_KEYSPACE, vec![job.0])
        .await
        .unwrap();
    let decided = send_outbox(&fixture.ctx, &config, now, &mut backoff, &send).await;
    assert_eq!(decided, Ok(1));
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
