//! The sub-batches one outbox drain invocation publishes and syncs, and the counts it keeps:
//! records grouped by peers, each sub-batch with the ordering domains it blocks when it fails.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use aruna_core::document::{DocumentOutboxEvent, DocumentSyncPublish, DocumentTarget};
use aruna_core::id::NodeId;
use aruna_core::placement::target_placement_ref;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::telemetry::duration_ms;
use irokle::TopicId;
use tracing::info;
use ulid::Ulid;

use super::defer::{DrainDeferState, DrainRecord, SyncWait, add_waits, admin_origin};
use super::record::OUTBOX_DRAIN_SIZE;
use super::rotation::OutboxRotation;
use crate::sync::shard_placement::{DEFER_RETRY_AFTER, sort_node_ids};

/// Records one publish or sync sub-batch holds at most.
pub const DRAIN_SUBBATCH_RECORDS: usize = 512;
/// Pages one drain invocation may examine, so a large or blocked queue cannot
/// monopolise a task invocation.
pub const OUTBOX_INVOCATION_PAGES: usize = 2;
pub const OUTBOX_INVOCATION_RECORDS: usize = OUTBOX_INVOCATION_PAGES * OUTBOX_DRAIN_SIZE;
/// Consecutive continuations before a rotation yields through the timer, so an
/// append-heavy or wholly blocked queue cannot keep the task continuously hot.
pub const OUTBOX_CONTINUATION_STREAK: u32 = 8;
const _: () = assert!(OUTBOX_INVOCATION_PAGES > 0 && OUTBOX_CONTINUATION_STREAK > 0);
/// When a rotation with records left goes on after an invocation.
pub const OUTBOX_CONTINUATION_AFTER: Duration = Duration::from_millis(50);

/// The pages and records one invocation may examine, and the continuations before a rotation
/// yields through the timer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OutboxLimits {
    pub pages: usize,
    pub records: usize,
    pub continuation_streak: u32,
}

impl Default for OutboxLimits {
    fn default() -> Self {
        Self {
            pages: OUTBOX_INVOCATION_PAGES,
            records: OUTBOX_INVOCATION_RECORDS,
            continuation_streak: OUTBOX_CONTINUATION_STREAK,
        }
    }
}

/// What a record's publication is bound to: its topic's lineage and the digest of the row the
/// drain read, each `None` where it could not be learned.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RowStamp {
    pub lineage: Option<[u8; 32]>,
    pub digest: Option<[u8; 32]>,
}

/// Records sent to the same peers together: the documents to publish, empty for records this
/// process already published, and per record its topic, admin origin, target and key.
#[derive(Debug, PartialEq)]
pub struct DrainSubBatch {
    pub peers: Vec<NodeId>,
    pub documents: Vec<DocumentSyncPublish>,
    pub topics: Vec<TopicId>,
    /// Admin origin of each entry, so a blocked publish also blocks the rest of
    /// that origin's sequence.
    pub origins: Vec<Option<NodeId>>,
    pub targets: Vec<DocumentTarget>,
    pub record_keys: Vec<Vec<u8>>,
    /// What each record's publication is bound to; a missing stamp binds nothing.
    pub stamps: Vec<RowStamp>,
}

impl DrainSubBatch {
    /// The entries at `indices`, to sync without their documents; `None` for an index outside.
    pub fn sync_subset(&self, indices: &[usize]) -> Option<Self> {
        let mut topics = Vec::with_capacity(indices.len());
        let mut origins = Vec::with_capacity(indices.len());
        let mut targets = Vec::with_capacity(indices.len());
        let mut record_keys = Vec::with_capacity(indices.len());
        let mut stamps = Vec::with_capacity(indices.len());
        for &index in indices {
            topics.push(*self.topics.get(index)?);
            origins.push(*self.origins.get(index)?);
            targets.push(self.targets.get(index)?.clone());
            record_keys.push(self.record_keys.get(index)?.clone());
            stamps.push(self.stamp(index));
        }
        Some(Self {
            peers: self.peers.clone(),
            documents: Vec::new(),
            topics,
            origins,
            targets,
            record_keys,
            stamps,
        })
    }

    /// What the publication of the record at `index` is bound to.
    pub fn stamp(&self, index: usize) -> RowStamp {
        self.stamps.get(index).copied().unwrap_or_default()
    }

    /// The entries at `indices` with their documents, to publish; `None` for an index outside.
    pub fn select(&self, indices: &[usize]) -> Option<Self> {
        let mut selected = self.sync_subset(indices)?;
        if self.documents.len() == self.record_keys.len() {
            let documents = indices
                .iter()
                .map(|&index| self.documents.get(index).cloned());
            selected.documents = documents.collect::<Option<_>>()?;
        }
        Some(selected)
    }

    /// Ordering domains blocked together when a publish or sync leaves records
    /// behind.
    pub fn ordering_domains(&self) -> (Vec<TopicId>, Vec<NodeId>) {
        (
            self.topics.clone(),
            self.origins.iter().flatten().copied().collect(),
        )
    }
}

/// Groups records by peers into sub-batches and answers how many groups formed; a record this
/// process published already only syncs. No record joins a sub-batch before one that holds an
/// earlier record of its topic or admin origin, so each of them keeps the scan order.
pub fn build_drain_batches(
    records: Vec<DrainRecord>,
    published: &BTreeSet<Vec<u8>>,
) -> (usize, Vec<DrainSubBatch>) {
    let mut subbatches: Vec<DrainSubBatch> = Vec::new();
    // The open sub-batch of each group, keyed by whether it only syncs and by its sorted peers.
    let mut open: BTreeMap<(bool, Vec<NodeId>), usize> = BTreeMap::new();
    let mut last_topic: BTreeMap<TopicId, usize> = BTreeMap::new();
    let mut last_origin: BTreeMap<NodeId, usize> = BTreeMap::new();
    for (record_key, record, topic) in records {
        let origin = admin_origin(&record);
        let sync_only = published.contains(&record_key);
        let document = (!sync_only).then(|| {
            publish_from_outbox(
                record.outbox_id,
                record.target.clone(),
                record.event,
                record.placement,
                record.allow_genesis,
            )
        });
        let mut peer_key = record.peers.clone();
        sort_node_ids(&mut peer_key);
        let origin_floor = origin.and_then(|origin| last_origin.get(&origin).copied());
        let floor = last_topic.get(&topic).copied().max(origin_floor);
        let group = (sync_only, peer_key);
        let joins = open.get(&group).copied().filter(|&index| {
            let full = subbatches[index].record_keys.len() >= DRAIN_SUBBATCH_RECORDS;
            !full && floor.is_none_or(|floor| index >= floor)
        });
        let index = match joins {
            Some(index) => index,
            None => {
                subbatches.push(DrainSubBatch {
                    peers: record.peers.clone(),
                    documents: Vec::new(),
                    topics: Vec::new(),
                    origins: Vec::new(),
                    targets: Vec::new(),
                    record_keys: Vec::new(),
                    stamps: Vec::new(),
                });
                let index = subbatches.len() - 1;
                open.insert(group, index);
                index
            }
        };
        let subbatch = &mut subbatches[index];
        subbatch.documents.extend(document);
        subbatch.topics.push(topic);
        subbatch.origins.push(origin);
        subbatch.targets.push(record.target);
        subbatch.record_keys.push(record_key);
        last_topic.insert(topic, index);
        if let Some(origin) = origin {
            last_origin.insert(origin, index);
        }
    }
    (open.len(), subbatches)
}

/// The document sync publication of one outbox record.
pub fn publish_from_outbox(
    event_id: Ulid,
    target: DocumentTarget,
    event: DocumentOutboxEvent,
    placement: PlacementRef,
    allow_genesis: bool,
) -> DocumentSyncPublish {
    match event {
        DocumentOutboxEvent::Upsert { bytes, change } => DocumentSyncPublish::Upsert {
            event_id,
            target,
            bytes,
            change,
            allow_genesis,
        },
        DocumentOutboxEvent::Delete { change } => DocumentSyncPublish::Delete {
            event_id,
            target,
            change,
            allow_genesis,
        },
        DocumentOutboxEvent::AdminOperation {
            event,
            origin_signature,
        } => DocumentSyncPublish::AdminOperation {
            target,
            event,
            placement,
            allow_genesis,
            origin_signature,
        },
    }
}

/// Resolves the shard placement a drained record publishes under: a real ref is
/// kept, a NIL ref (from admin-operation emitters) is resolved from the realm
/// config. Shared realm targets ignore placement, so resolving is harmless.
pub fn resolve_publish_placement(
    config: Option<&RealmConfigDocument>,
    target: &DocumentTarget,
    current: PlacementRef,
) -> PlacementRef {
    if current != PlacementRef::NIL {
        return current;
    }
    match config {
        Some(config) => target_placement_ref(config, target, Default::default()),
        None => PlacementRef::NIL,
    }
}

/// What publishing and syncing sub-batches left: the time spent, the records deleted and the
/// ordering domains to block for the rest of the rotation.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DrainSyncOutcome {
    pub sync_elapsed: Duration,
    pub project_elapsed: Duration,
    pub delete_elapsed: Duration,
    pub retry_needed: bool,
    pub deleted: usize,
    /// Ordering domains to block for the rest of the rotation.
    pub blocked_topics: Vec<TopicId>,
    pub blocked_origins: Vec<NodeId>,
    /// The wait of each published record whose sync must retry.
    pub sync_waits: Vec<SyncWait>,
}

impl DrainSyncOutcome {
    pub fn merge(&mut self, other: DrainSyncOutcome) {
        self.sync_elapsed += other.sync_elapsed;
        self.project_elapsed += other.project_elapsed;
        self.delete_elapsed += other.delete_elapsed;
        self.retry_needed |= other.retry_needed;
        self.deleted += other.deleted;
        self.blocked_topics.extend(other.blocked_topics);
        self.blocked_origins.extend(other.blocked_origins);
        self.sync_waits.extend(other.sync_waits);
    }
}

/// The oldest stuck record's age, target, topic and placement, logged as representative.
pub type StuckRecord = (u64, DocumentTarget, TopicId, PlacementRef);

/// One bounded invocation of an open rotation: where it reads, what it blocks and what it
/// counted.
#[derive(Clone, Debug, PartialEq)]
pub struct DrainInvocation {
    pub outcome: DrainSyncOutcome,
    pub defer: DrainDeferState,
    pub cursor: Option<Vec<u8>>,
    pub reached_end: bool,
    pub scan_elapsed: Duration,
    pub publish_elapsed: Duration,
    pub records: usize,
    pub deferred: usize,
    pub stuck: usize,
    pub oldest_stuck: Option<StuckRecord>,
    pub undeliverable: usize,
    pub groups: usize,
    pub subbatches: usize,
    pub pages: usize,
    /// The rows this invocation's pages read, admitted or not.
    pub scanned: usize,
    pub oldest_record_ms: Option<u64>,
    pub read_failed: bool,
    pub config_drained: bool,
}

impl DrainInvocation {
    /// An invocation that resumes `rotation` with its blocks.
    pub fn new(rotation: &OutboxRotation) -> Self {
        Self {
            outcome: DrainSyncOutcome::default(),
            defer: DrainDeferState {
                deferred_topics: rotation.blocked_topics.clone(),
                blocked_origins: rotation.blocked_origins.clone(),
                undeliverable_topics: rotation.undeliverable_topics.clone(),
                sync_waits: rotation.sync_waits.clone(),
                ..DrainDeferState::default()
            },
            cursor: rotation.cursor.clone(),
            reached_end: false,
            scan_elapsed: Duration::ZERO,
            publish_elapsed: Duration::ZERO,
            records: 0,
            deferred: 0,
            stuck: 0,
            oldest_stuck: None,
            undeliverable: 0,
            groups: 0,
            subbatches: 0,
            pages: 0,
            scanned: 0,
            oldest_record_ms: None,
            read_failed: false,
            config_drained: false,
        }
    }

    /// Takes in what publishing left. Its blocks hold for the rest of this invocation at once, so
    /// a later page cannot publish or relay past a record that has to wait.
    pub fn merge_outcome(&mut self, outcome: DrainSyncOutcome) {
        let defer = &mut self.defer;
        defer
            .deferred_topics
            .extend(outcome.blocked_topics.iter().copied());
        defer
            .blocked_origins
            .extend(outcome.blocked_origins.iter().copied());
        add_waits(&mut defer.sync_waits, &outcome.sync_waits);
        self.outcome.merge(outcome);
    }

    /// Whether records past this invocation's cursor wait for a later one.
    pub fn has_unvisited(&self) -> bool {
        !self.reached_end && !self.read_failed
    }

    /// Takes this finished invocation into `rotation`, logs its summary and answers when the
    /// drain runs again: on the retry ladder, as a continuation, after the defer delay once the
    /// streak is spent, or as the closed rotation plans. `elapsed` is the invocation's time.
    pub fn finish(
        mut self,
        rotation: &mut OutboxRotation,
        limits: &OutboxLimits,
        (now_ms, elapsed): (u64, Duration),
    ) -> Option<Duration> {
        let outcome = &self.outcome;
        let defer = &mut self.defer;
        defer
            .deferred_topics
            .extend(outcome.blocked_topics.iter().copied());
        defer
            .blocked_origins
            .extend(outcome.blocked_origins.iter().copied());
        add_waits(&mut defer.sync_waits, &outcome.sync_waits);
        let unvisited = self.has_unvisited();
        let retry = self.outcome.retry_needed || self.read_failed;
        rotation.cursor = match self.reached_end {
            true => None,
            false => self.cursor.take(),
        };
        rotation.blocked_topics = std::mem::take(&mut self.defer.deferred_topics);
        rotation.blocked_origins = std::mem::take(&mut self.defer.blocked_origins);
        rotation.undeliverable_topics = std::mem::take(&mut self.defer.undeliverable_topics);
        rotation.sync_waits = std::mem::take(&mut self.defer.sync_waits);
        let totals = &mut rotation.totals;
        totals.examined += self.records;
        totals.deleted += self.outcome.deleted;
        totals.deferred += self.deferred;
        totals.undeliverable += self.undeliverable;
        totals.retry_invocations += usize::from(retry);
        totals.invocations = totals.invocations.saturating_add(1);
        if self.records > 0 {
            self.summary(
                rotation.continuations,
                (unvisited, retry),
                (now_ms, elapsed),
            );
        }
        if retry {
            return rotation.finish_retry(self.reached_end);
        }
        if !unvisited {
            return rotation.close_logged();
        }
        if rotation.continuations < limits.continuation_streak {
            rotation.continuations = rotation.continuations.saturating_add(1);
            return Some(OUTBOX_CONTINUATION_AFTER);
        }
        rotation.continuations = 0;
        Some(DEFER_RETRY_AFTER)
    }

    fn summary(&self, continuation: u32, (unvisited, retry): (bool, bool), at: (u64, Duration)) {
        let (now_ms, elapsed) = at;
        let oldest_age_ms = (self.oldest_record_ms)
            .map(|record_ms| now_ms.saturating_sub(record_ms))
            .unwrap_or(0);
        info!(
            event = "pipeline.drain.summary",
            records = self.records,
            examined = self.records,
            deleted = self.outcome.deleted,
            deferred = self.deferred,
            undeliverable = self.undeliverable,
            retry_scheduled = retry,
            has_unvisited = unvisited,
            continuation = continuation,
            rotation_complete = self.reached_end,
            groups = self.groups,
            subbatches = self.subbatches,
            pages = self.pages,
            scan_ms = duration_ms(self.scan_elapsed),
            publish_ms = duration_ms(self.publish_elapsed),
            sync_ms = duration_ms(self.outcome.sync_elapsed),
            project_ms = duration_ms(self.outcome.project_elapsed),
            delete_ms = duration_ms(self.outcome.delete_elapsed),
            total_ms = duration_ms(elapsed),
            oldest_age_ms,
            retry = retry,
            "Document sync outbox drain summary"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::document::{DocumentChange, DocumentChangeKind, DocumentSyncRevision};

    use crate::sync::outbox::record::new_identified_record;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn record(seq: u8, peer: NodeId, topic: TopicId) -> DrainRecord {
        let event_id = Ulid::from_parts(1, u128::from(seq));
        let current = DocumentSyncRevision {
            generation: 1,
            event_id,
            actor: node(9),
            updated_at_ms: 1,
        };
        let change = DocumentChange {
            base: None,
            current,
            kind: DocumentChangeKind::Upsert,
            placement: PlacementRef::NIL,
        };
        let event = DocumentOutboxEvent::Upsert {
            bytes: vec![seq],
            change,
        };
        let target = DocumentTarget::Group {
            group_id: Ulid::from_parts(7, u128::from(seq)),
        };
        let (peers, placement) = (vec![peer], PlacementRef::NIL);
        let record =
            new_identified_record(event_id, node(9), target, peers, event, placement, false);
        (vec![seq], record, topic)
    }

    // A later record of a topic never publishes before an earlier one, whatever its peers; an
    // unrelated record still joins the open sub-batch of its peers.
    #[test]
    fn topic_order_kept() {
        let mut peers = vec![node(1), node(2)];
        sort_node_ids(&mut peers);
        let (first, second) = (peers[0], peers[1]);
        let (topic, other) = (TopicId::hash(b"topic"), TopicId::hash(b"other"));
        let records = vec![
            record(1, second, topic),
            record(2, first, topic),
            record(3, second, other),
        ];
        let (groups, batches) = build_drain_batches(records, &BTreeSet::new());
        assert_eq!(groups, 2);
        let keys: Vec<_> = batches
            .iter()
            .map(|batch| batch.record_keys.clone())
            .collect();
        assert_eq!(keys, vec![vec![vec![1], vec![3]], vec![vec![2]]]);
    }

    // A failed publish blocks its domains for the rest of the invocation at once.
    #[test]
    fn blocks_reach_invocation() {
        let mut invocation = DrainInvocation::new(&OutboxRotation::default());
        let topic = TopicId::hash(b"topic");
        let outcome = DrainSyncOutcome {
            retry_needed: true,
            blocked_topics: vec![topic],
            blocked_origins: vec![node(3)],
            ..DrainSyncOutcome::default()
        };
        invocation.merge_outcome(outcome);
        assert!(invocation.defer.deferred_topics.contains(&topic));
        assert!(invocation.defer.blocked_origins.contains(&node(3)));
        assert!(invocation.outcome.retry_needed);
    }
}
