//! Splits drained outbox records into those that publish now, wait for their shard topic, or can
//! never publish from this node, keeping each admin origin's order.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{HashMap, HashSet};

use aruna_core::document::{DocumentOutboxEvent, DocumentOutboxRecord};
use aruna_core::id::NodeId;
use aruna_core::placement::{holds_placement, is_draining_holder, retained_departing_holder};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use irokle::TopicId;
use tracing::debug;

/// One drained outbox record: its storage key, the record and the topic it publishes on.
pub type DrainRecord = (Vec<u8>, DocumentOutboxRecord, TopicId);

/// Per-invocation defer state. The block sets are seeded from the open rotation;
/// `topic_exists` and `topic_held` deliberately are not, so holdership and
/// genesis presence are re-read every invocation.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DrainDeferState {
    pub topic_exists: HashMap<TopicId, bool>,
    pub topic_held: HashMap<TopicId, bool>,
    pub deferred_topics: HashSet<TopicId>,
    pub blocked_origins: HashSet<NodeId>,
    pub undeliverable_topics: HashSet<TopicId>,
    pub sync_waits: SyncWaits,
}

/// Published records that must sync again, by admin origin (`None` for other records), with the
/// topics they ride and the lineage each had. A later record on such a topic goes out only on that
/// lineage, and a later record of such an origin only on a topic that holds all its waits.
pub type SyncWaits = HashMap<Option<NodeId>, HashSet<(TopicId, [u8; 32])>>;

/// The admin origin of a published record that must sync again, its topic and the topic's lineage.
pub type SyncWait = (Option<NodeId>, TopicId, [u8; 32]);

/// Adds the wait of each record whose sync must retry to `waits`.
pub fn add_waits(waits: &mut SyncWaits, pairs: &[SyncWait]) {
    for (origin, topic, lineage) in pairs {
        waits.entry(*origin).or_default().insert((*topic, *lineage));
    }
}

/// Whether a shard-classed record can ever publish from this node.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeferOutcome {
    /// The local node holds the bucket, so it may publish onto the bucket's topic
    /// once the genesis is there (rank-0 creates it, every other holder pulls it).
    Retry,
    /// The local node holds none of the record's bucket. Topic membership is the
    /// holder set, so it may neither mint the genesis nor join the topic: this
    /// record can never publish from here, however long it waits.
    Undeliverable,
}

/// Whether a record whose shard topic is missing locally can ever publish from `node_id`.
/// Holdership comes from the live realm config, never a local copy (rebalances leave
/// stale copies). Without a readable config nothing is decided and the record retries.
pub fn classify_deferred_record(
    config: Option<&RealmConfigDocument>,
    node_id: NodeId,
    record: &DocumentOutboxRecord,
) -> DeferOutcome {
    let Some(config) = config else {
        return DeferOutcome::Retry;
    };
    // Draining former holders may publish until flushed; true non-holders remain undeliverable.
    // The receiver history cutoff bounds a departing holder.
    if holds_placement(config, &record.placement, node_id)
        || is_draining_holder(config, &record.placement, node_id)
        || retained_departing_holder(config, &record.placement, node_id)
    {
        DeferOutcome::Retry
    } else {
        DeferOutcome::Undeliverable
    }
}

/// Splits FIFO-ordered drain records into publish-now, deferred (no local shard genesis
/// yet) and undeliverable. Holdership decides publishability, not local topic presence,
/// and a topic never straddles the defer/publish boundary; `publishes_shared` gates devices.
pub fn partition_drain_records(
    records: Vec<DrainRecord>,
    defer: &mut DrainDeferState,
    publishes_shared: bool,
    mut topic_available: impl FnMut(TopicId) -> bool,
    mut classify_defer: impl FnMut(&DocumentOutboxRecord) -> DeferOutcome,
) -> (Vec<DrainRecord>, Vec<DrainRecord>, Vec<DrainRecord>) {
    let mut to_publish = Vec::with_capacity(records.len());
    let mut deferred = Vec::new();
    let mut undeliverable = Vec::new();
    for (record_key, record, topic) in records {
        // Origin order is the contract, whatever topic the later records ride; a held record
        // holds its topic too.
        if admin_origin(&record).is_some_and(|origin| defer.blocked_origins.contains(&origin)) {
            defer.deferred_topics.insert(topic);
            deferred.push((record_key, record, topic));
            continue;
        }
        // A record of an origin waiting on another topic's sync waits too, and holds its origin,
        // so no later record of the origin overtakes it on the waiting topic.
        if let Some(origin) = admin_origin(&record)
            && defer
                .sync_waits
                .get(&Some(origin))
                .is_some_and(|topics| topics.iter().any(|(waiting, _)| *waiting != topic))
        {
            defer.blocked_origins.insert(origin);
            defer.deferred_topics.insert(topic);
            deferred.push((record_key, record, topic));
            continue;
        }
        // A topic a failure blocked holds every later record, shared or shard, and the origin of
        // a held admin record with it.
        if defer.deferred_topics.contains(&topic) {
            if let Some(origin) = admin_origin(&record) {
                defer.blocked_origins.insert(origin);
            }
            deferred.push((record_key, record, topic));
            continue;
        }
        if !record.target.uses_shard_topic() {
            match publishes_shared {
                true => to_publish.push((record_key, record, topic)),
                false => undeliverable.push((record_key, record, topic)),
            }
            continue;
        }
        if defer.undeliverable_topics.contains(&topic) {
            if let Some(origin) = admin_origin(&record) {
                defer.blocked_origins.insert(origin);
            }
            undeliverable.push((record_key, record, topic));
            continue;
        }
        let held = *defer
            .topic_held
            .entry(topic)
            .or_insert_with(|| classify_defer(&record) == DeferOutcome::Retry);
        if !held {
            defer.undeliverable_topics.insert(topic);
            if let Some(origin) = admin_origin(&record) {
                defer.blocked_origins.insert(origin);
            }
            undeliverable.push((record_key, record, topic));
            continue;
        }
        let already_deferred = defer.deferred_topics.contains(&topic);
        let available = !already_deferred
            && *defer
                .topic_exists
                .entry(topic)
                .or_insert_with(|| topic_available(topic));
        if available {
            to_publish.push((record_key, record, topic));
            continue;
        }
        defer.deferred_topics.insert(topic);
        if let Some(origin) = admin_origin(&record) {
            defer.blocked_origins.insert(origin);
        }
        debug!(
            event = "pipeline.drain.deferred",
            target = ?record.target,
            %topic,
            "Deferring outbox record: shard topic genesis not yet known"
        );
        deferred.push((record_key, record, topic));
    }
    (to_publish, deferred, undeliverable)
}

/// Holds back each relay that follows a record of its admin origin this page still publishes, and
/// each later relay to a held one's shard topic, so a holder never takes a later record first.
/// Answers the relays to run now and the held ones; keys order a page as it was read.
pub fn hold_relays(
    to_publish: &[DrainRecord],
    undeliverable: Vec<DrainRecord>,
    defer: &mut DrainDeferState,
) -> (Vec<DrainRecord>, Vec<DrainRecord>) {
    let mut first_publish: HashMap<NodeId, &[u8]> = HashMap::new();
    for (key, record, _) in to_publish {
        if let Some(origin) = admin_origin(record) {
            let first = first_publish.entry(origin).or_insert(key);
            *first = (*first).min(key.as_slice());
        }
    }
    let (mut now, mut held) = (Vec::new(), Vec::new());
    let mut held_topics = HashSet::new();
    for relay in undeliverable {
        let (key, record, topic) = &relay;
        let Some(origin) = admin_origin(record) else {
            now.push(relay);
            continue;
        };
        let after_publish = first_publish
            .get(&origin)
            .is_some_and(|first| key.as_slice() > *first);
        if !after_publish && !held_topics.contains(topic) {
            now.push(relay);
            continue;
        }
        // A held relay holds its destination topic too, here and on the next pages, so no relay
        // of another origin reaches that shard first.
        held_topics.insert(*topic);
        defer.deferred_topics.insert(*topic);
        defer.blocked_origins.insert(origin);
        held.push(relay);
    }
    (now, held)
}

/// The admin origin stream a record belongs to. Origin sequence is ordered
/// within one origin node.
pub fn admin_origin(record: &DocumentOutboxRecord) -> Option<NodeId> {
    match &record.event {
        DocumentOutboxEvent::AdminOperation { event, .. } => Some(event.origin_node_id),
        _ => None,
    }
}

#[cfg(test)]
#[path = "defer_tests.rs"]
mod tests;
