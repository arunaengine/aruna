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
#[derive(Default)]
pub struct DrainDeferState {
    pub topic_exists: HashMap<TopicId, bool>,
    pub topic_held: HashMap<TopicId, bool>,
    pub deferred_topics: HashSet<TopicId>,
    pub blocked_origins: HashSet<NodeId>,
    pub undeliverable_topics: HashSet<TopicId>,
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
        // Origin order is the contract, whatever topic the later records ride.
        if admin_origin(&record).is_some_and(|origin| defer.blocked_origins.contains(&origin)) {
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
