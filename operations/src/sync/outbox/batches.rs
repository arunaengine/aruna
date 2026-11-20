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
use irokle::TopicId;
use ulid::Ulid;

use super::defer::{DrainDeferState, DrainRecord, admin_origin};
use super::rotation::OutboxRotation;
use crate::sync::shard_placement::sort_node_ids;

/// Records one publish or sync sub-batch holds at most.
pub const DRAIN_SUBBATCH_RECORDS: usize = 512;

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
}

impl DrainSubBatch {
    /// The entries at `indices`, to sync without their documents; `None` for an index outside.
    pub fn sync_subset(&self, indices: &[usize]) -> Option<Self> {
        let mut topics = Vec::with_capacity(indices.len());
        let mut origins = Vec::with_capacity(indices.len());
        let mut targets = Vec::with_capacity(indices.len());
        let mut record_keys = Vec::with_capacity(indices.len());
        for &index in indices {
            topics.push(*self.topics.get(index)?);
            origins.push(*self.origins.get(index)?);
            targets.push(self.targets.get(index)?.clone());
            record_keys.push(self.record_keys.get(index)?.clone());
        }
        Some(Self {
            peers: self.peers.clone(),
            documents: Vec::new(),
            topics,
            origins,
            targets,
            record_keys,
        })
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

/// Sub-batches keyed by whether their records only sync and by their sorted peers.
type PublishGroups = BTreeMap<(bool, Vec<NodeId>), (Vec<NodeId>, Vec<DrainSubBatch>)>;

/// Groups records by peers into sub-batches and answers how many groups they formed. A record
/// already published by this process goes into a sub-batch without documents, which only syncs.
pub fn build_drain_batches(
    records: Vec<DrainRecord>,
    published: &BTreeSet<Vec<u8>>,
) -> (usize, Vec<DrainSubBatch>) {
    let mut publish_groups: PublishGroups = BTreeMap::new();
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
        let (peers, subbatches) = publish_groups
            .entry((sync_only, peer_key))
            .or_insert_with(|| (record.peers.clone(), Vec::new()));
        if subbatches
            .last()
            .is_none_or(|subbatch| subbatch.record_keys.len() >= DRAIN_SUBBATCH_RECORDS)
        {
            subbatches.push(DrainSubBatch {
                peers: peers.clone(),
                documents: Vec::new(),
                topics: Vec::new(),
                origins: Vec::new(),
                targets: Vec::new(),
                record_keys: Vec::new(),
            });
        }
        let Some(subbatch) = subbatches.last_mut() else {
            continue;
        };
        subbatch.documents.extend(document);
        subbatch.topics.push(topic);
        subbatch.origins.push(origin);
        subbatch.targets.push(record.target);
        subbatch.record_keys.push(record_key);
    }
    let groups = publish_groups.len();
    let subbatches = publish_groups
        .into_values()
        .flat_map(|(_, subbatches)| subbatches)
        .collect();
    (groups, subbatches)
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
            oldest_record_ms: None,
            read_failed: false,
            config_drained: false,
        }
    }

    /// Whether records past this invocation's cursor wait for a later one.
    pub fn has_unvisited(&self) -> bool {
        !self.reached_end && !self.read_failed
    }
}
