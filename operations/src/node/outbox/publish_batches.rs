//! Publishes one drain invocation's sub-batches in order and syncs each after its publish: a
//! sub-batch publishes while the one before it syncs, and a failed publish blocks its domains.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::time::Duration;

use aruna_core::calls::{ChildOutcome, IoCall, IoError, IoResult, Next};
use aruna_core::document::{DocumentEffect, DocumentNetEvent};
use aruna_core::effects::{Effect, EffectGroup, GroupForm, NetEffect};
use aruna_core::events::{Event, GroupEvent, GroupResult, NetEvent};
use aruna_core::id::NodeId;
use aruna_core::id_mint::IdMint;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::task::TaskKey;
use irokle::TopicId;
use tracing::warn;

use crate::node::outbox::finish_batch::FinishedBatch;
use crate::node::outbox::sync_batch::SyncBatchOperation;
use crate::sync::outbox::batches::{DrainSubBatch, DrainSyncOutcome, RowStamp};
use crate::sync::outbox::cleanup::{Publication, lineage_call, lineages};

/// What publishing the sub-batches left: the sync outcome, the time the publishes took, the
/// records this run published with the lineage their topic had, and those it deleted.
#[derive(Debug, Default, PartialEq)]
pub struct PublishedBatches {
    pub outcome: DrainSyncOutcome,
    pub publish_elapsed: Duration,
    pub published: Vec<(Vec<u8>, Publication)>,
    pub deleted: Vec<Vec<u8>>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum PublishStep {
    Sync,
    Publish,
    Window,
    Lineage,
}

/// A sub-batch whose sync runs, by the ordering domains its failure blocks.
#[derive(Debug, PartialEq)]
struct Syncing {
    domains: (Vec<TopicId>, Vec<NodeId>),
}

/// The publications of `batch` a later drain may rely on: each record with the lineage its
/// topic had and its row's digest. A record missing either publishes again on a retry.
fn evidence(batch: &DrainSubBatch) -> Vec<(Vec<u8>, Publication)> {
    let publication = |index: usize| {
        let stamp = batch.stamp(index);
        let (lineage, digest) = (stamp.lineage?, stamp.digest?);
        Some(Publication { lineage, digest })
    };
    (batch.record_keys.iter().enumerate())
        .filter_map(|(index, key)| Some((key.clone(), publication(index)?)))
        .collect()
}

/// Whether a record of `batch` lacks the lineage of its topic.
fn lacks_lineage(batch: &DrainSubBatch) -> bool {
    (0..batch.record_keys.len()).any(|index| batch.stamp(index).lineage.is_none())
}

/// Whether two sub-batches hold records of one topic or one admin origin, so their order counts.
fn shares_domain(first: &DrainSubBatch, second: &DrainSubBatch) -> bool {
    let (topics, origins) = first.ordering_domains();
    let (later_topics, later_origins) = second.ordering_domains();
    topics.iter().any(|topic| later_topics.contains(topic))
        || origins.iter().any(|origin| later_origins.contains(origin))
}

/// The entries of `batch` at `published` whose topic or admin origin no entry at `retried` before
/// them shares; the others wait unsynced, so no peer receives them ahead of a retried entry.
fn ready_entries(batch: &DrainSubBatch, published: &[usize], retried: &[usize]) -> Vec<usize> {
    let shares = |first: usize, later: usize| {
        let topic = |index: usize| batch.topics.get(index);
        let origin = |index: usize| batch.origins.get(index).copied().flatten();
        topic(first) == topic(later) || origin(first).is_some_and(|o| origin(later) == Some(o))
    };
    let waits = |later: usize| {
        retried
            .iter()
            .any(|&first| first < later && shares(first, later))
    };
    published
        .iter()
        .copied()
        .filter(|&index| !waits(index))
        .collect()
}

type PublishTurn = Result<Next<PublishedBatches>, IoError>;

/// The sub-batches of one drain invocation, published and synced in order.
#[derive(Debug, PartialEq)]
pub struct PublishBatchesOperation {
    serving: (NodeId, RealmId),
    batches: VecDeque<DrainSubBatch>,
    awaiting: Option<DrainSubBatch>,
    publishing: Option<DrainSubBatch>,
    syncing: Option<Syncing>,
    window_publish: Option<Option<Event>>,
    window_sync: Option<Option<Result<FinishedBatch, IoError>>>,
    /// Whether the awaiting batch already asked for the lineages it lacked.
    lineage_asked: bool,
    topics: Vec<TopicId>,
    out: PublishedBatches,
    step: PublishStep,
    ids: IdMint,
    started_ms: u64,
    now_ms: u64,
}

impl PublishBatchesOperation {
    pub fn new(serving: (NodeId, RealmId), batches: Vec<DrainSubBatch>) -> Self {
        Self {
            serving,
            batches: batches.into(),
            awaiting: None,
            publishing: None,
            syncing: None,
            window_publish: None,
            window_sync: None,
            lineage_asked: false,
            topics: Vec::new(),
            out: PublishedBatches::default(),
            step: PublishStep::Sync,
            ids: IdMint::default(),
            started_ms: 0,
            now_ms: 0,
        }
    }

    /// Starts the next publish, sync or both; a sub-batch without documents only syncs, after
    /// the one before it.
    fn advance(&mut self) -> PublishTurn {
        loop {
            // A publication onto a topic without a genesis before it has a lineage only now.
            if !self.lineage_asked && self.awaiting.as_ref().is_some_and(lacks_lineage) {
                return self.ask_lineage();
            }
            let Some(batch) = self.batches.pop_front() else {
                return match self.awaiting.take() {
                    Some(last) => self.sync_alone(last),
                    None => Ok(Next::Done(std::mem::take(&mut self.out))),
                };
            };
            // A batch sharing a topic or admin origin with the one awaiting its sync waits for that
            // sync, so a failure there holds the later batch before it goes out.
            if let Some(before) = self
                .awaiting
                .take_if(|before| shares_domain(before, &batch))
            {
                self.batches.push_front(batch);
                return self.sync_alone(before);
            }
            if batch.documents.is_empty() {
                self.lineage_asked = false;
                match self.awaiting.replace(batch) {
                    Some(before) => return self.sync_alone(before),
                    None => continue,
                }
            }
            let Some(mut batch) = self.without_blocked(batch) else {
                continue;
            };
            let documents = std::mem::take(&mut batch.documents);
            let peers = batch.peers.clone();
            let publish = DocumentEffect::PublishDocuments { documents, peers };
            let publish = Effect::Net(NetEffect::DocumentSync(publish));
            self.publishing = Some(batch);
            self.started_ms = self.now_ms;
            let before = self.awaiting.take();
            let Some(before) = before.and_then(|before| self.without_blocked(before)) else {
                self.step = PublishStep::Publish;
                return Ok(Next::Call(IoCall::from(publish)));
            };
            let sync = boxed_suboperation(self.sync_batch(before), |result| {
                Event::Child(ChildOutcome::new(result))
            });
            self.step = PublishStep::Window;
            return Ok(Next::Call(IoCall::Group(EffectGroup {
                members: vec![publish, Effect::SubOperation(sync)],
                limit: 2,
                form: GroupForm::Window,
            })));
        }
    }

    /// Leaves out the entries whose topic or admin origin an earlier failure of this run
    /// blocked, or whose origin waits for a sync on another topic: they stay in the outbox for a
    /// retry and keep their own domains blocked.
    fn without_blocked(&mut self, batch: DrainSubBatch) -> Option<DrainSubBatch> {
        let mut kept = Vec::with_capacity(batch.record_keys.len());
        for (index, &topic) in batch.topics.iter().enumerate() {
            let origin = batch.origins.get(index).copied().flatten();
            let outcome = &self.out.outcome;
            // A successor rides only a topic that holds every wait of its origin.
            let waits_elsewhere = |origin: NodeId| {
                (outcome.sync_waits.iter()).any(|(waiting, on)| *waiting == origin && *on != topic)
            };
            let held = outcome.blocked_topics.contains(&topic)
                || origin.is_some_and(|origin| outcome.blocked_origins.contains(&origin))
                || origin.is_some_and(waits_elsewhere);
            if !held {
                kept.push(index);
                continue;
            }
            // A held row blocks its domains before the next row is examined.
            self.block((vec![topic], origin.into_iter().collect()));
        }
        if kept.len() == batch.record_keys.len() {
            return Some(batch);
        }
        (!kept.is_empty()).then(|| batch.select(&kept)).flatten()
    }

    /// Asks for the lineages the awaiting batch lacks before it syncs, so its finish can match
    /// them and a later drain can rely on its publication.
    fn ask_lineage(&mut self) -> PublishTurn {
        self.lineage_asked = true;
        let Some(batch) = self.awaiting.as_ref() else {
            return self.advance();
        };
        let mut topics: Vec<TopicId> = (0..batch.record_keys.len())
            .filter(|&index| batch.stamp(index).lineage.is_none())
            .filter_map(|index| batch.topics.get(index).copied())
            .collect();
        topics.sort();
        topics.dedup();
        let Some(call) = lineage_call(&topics) else {
            return self.advance();
        };
        self.topics = topics;
        self.step = PublishStep::Lineage;
        Ok(Next::Call(call))
    }

    /// Fills the lineages the awaiting batch lacked and keeps the publications they complete.
    fn lineage_known(&mut self, result: IoResult) -> PublishTurn {
        let current = lineages(&self.topics, result)?;
        let Some(batch) = self.awaiting.as_mut() else {
            return self.advance();
        };
        let entries = batch.record_keys.len();
        batch.stamps.resize(entries, RowStamp::default());
        // A lineage sampled after the append may postdate a tie-break that dropped it, so it
        // guards the finish but is no evidence a later drain may rely on.
        for (index, stamp) in batch.stamps.iter_mut().enumerate() {
            let topic = batch.topics.get(index);
            if stamp.lineage.is_none() {
                stamp.lineage = topic.and_then(|topic| current.get(topic).copied().flatten());
            }
        }
        self.advance()
    }

    fn sync_batch(&mut self, batch: DrainSubBatch) -> SyncBatchOperation {
        self.syncing = Some(Syncing {
            domains: batch.ordering_domains(),
        });
        let generation = self.ids.mint(self.now_ms);
        SyncBatchOperation::new(self.serving, generation, batch)
    }

    /// Syncs `batch` by itself, leaving out entries a failure has blocked since it was queued.
    fn sync_alone(&mut self, batch: DrainSubBatch) -> PublishTurn {
        let Some(batch) = self.without_blocked(batch) else {
            return self.advance();
        };
        let sync = self.sync_batch(batch);
        self.step = PublishStep::Sync;
        Ok(Next::Call(IoCall::child(sync)))
    }

    /// Takes in what a sub-batch's sync left; a sync that broke off retries and blocks its
    /// domains.
    fn synced(&mut self, result: Option<Result<FinishedBatch, IoError>>) {
        let Some(syncing) = self.syncing.take() else {
            return;
        };
        match result {
            Some(Ok(finished)) => {
                self.out.deleted.extend(finished.deleted);
                self.out.outcome.merge(finished.outcome);
            }
            _ => {
                let task_id = TaskKey::DrainSyncOutbox;
                warn!(task_id = ?task_id, "Syncing a document sync batch broke off");
                self.block(syncing.domains);
            }
        }
    }

    fn block(&mut self, (topics, origins): (Vec<TopicId>, Vec<NodeId>)) {
        let outcome = &mut self.out.outcome;
        outcome.retry_needed = true;
        outcome.blocked_topics.extend(topics);
        outcome.blocked_origins.extend(origins);
    }

    /// Takes in a publish answer: published records sync next, the rest retry and block their
    /// ordering domains.
    fn published(&mut self, event: Option<Event>) {
        let Some(batch) = self.publishing.take() else {
            return;
        };
        let task_id = TaskKey::DrainSyncOutbox;
        match event {
            Some(Event::Net(NetEvent::DocumentSync(DocumentNetEvent::DocumentsPublished {
                ..
            }))) => {
                self.out.published.extend(evidence(&batch));
                self.awaiting = Some(batch);
                self.lineage_asked = false;
            }
            Some(Event::Net(NetEvent::DocumentSync(
                DocumentNetEvent::DocumentsPartiallyPublished {
                    published_indices,
                    retry_indices,
                    error,
                },
            ))) => {
                warn!(
                    task_id = ?task_id,
                    published = published_indices.len(),
                    retry = retry_indices.len(),
                    error = %error,
                    "Partially created local document sync batch"
                );
                match batch.sync_subset(&retry_indices) {
                    Some(retried) => self.block(retried.ordering_domains()),
                    None => self.block(batch.ordering_domains()),
                }
                let Some(published) = batch.sync_subset(&published_indices) else {
                    warn!(task_id = ?task_id, "Invalid partial document publish indices");
                    return;
                };
                // Every published entry is in the oplog, but only those no retried entry before
                // them shares a topic or origin with sync now.
                self.out.published.extend(evidence(&published));
                let ready = ready_entries(&batch, &published_indices, &retry_indices);
                let waiting: Vec<usize> = (published_indices.iter().copied())
                    .filter(|index| !ready.contains(index))
                    .collect();
                if let Some(waiting) = batch.sync_subset(&waiting) {
                    self.block(waiting.ordering_domains());
                }
                if let Some(ready) = batch.sync_subset(&ready)
                    && !ready.record_keys.is_empty()
                {
                    self.awaiting = Some(ready);
                    self.lineage_asked = false;
                }
            }
            Some(Event::Net(NetEvent::DocumentSync(DocumentNetEvent::Error { error, .. }))) => {
                warn!(
                    task_id = ?task_id,
                    error = %error,
                    "Failed to create local document sync batch"
                );
                self.block(batch.ordering_domains());
            }
            Some(Event::Net(NetEvent::Error(error))) => {
                warn!(
                    task_id = ?task_id,
                    error = ?error,
                    "Failed to create local document sync batch"
                );
                self.block(batch.ordering_domains());
            }
            other => {
                warn!(
                    task_id = ?task_id,
                    event = ?other,
                    "Unexpected local document sync batch result"
                );
                self.block(batch.ordering_domains());
            }
        }
    }

    fn publish_time(&mut self) {
        let elapsed = self.now_ms.saturating_sub(self.started_ms);
        self.out.publish_elapsed += Duration::from_millis(elapsed);
    }

    /// One member of a publish window; the closed window takes in the sync, then the publish.
    fn window(&mut self, event: GroupEvent) -> PublishTurn {
        match event {
            GroupEvent::Member { index: 0, result } => {
                self.publish_time();
                let event = match result {
                    GroupResult::Done(event) => Some(*event),
                    _ => None,
                };
                self.window_publish = Some(event);
                Ok(Next::Wait)
            }
            GroupEvent::Member { result, .. } => {
                let synced = result.child_outcome::<FinishedBatch, IoError>();
                self.window_sync = Some(synced);
                Ok(Next::Wait)
            }
            GroupEvent::Closed => {
                let synced = self.window_sync.take().flatten();
                self.synced(synced);
                let published = self.window_publish.take().flatten();
                self.published(published);
                self.advance()
            }
            GroupEvent::Finished { .. } => Err(IoError::Unexpected {
                expected: "a publish window member",
                got: "a finished group",
            }),
        }
    }
}

impl Operation for PublishBatchesOperation {
    type Output = PublishedBatches;
    type Error = IoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn seed(&mut self, entropy: u128) {
        self.ids = IdMint::new(entropy);
    }

    fn first(&mut self) -> PublishTurn {
        self.advance()
    }

    fn next(&mut self, result: IoResult) -> PublishTurn {
        match (self.step, result) {
            (PublishStep::Window, IoResult::Group(event)) => self.window(event),
            (PublishStep::Window, _) => Err(IoError::Unexpected {
                expected: "a publish window event",
                got: "another result",
            }),
            (PublishStep::Publish, result) => {
                self.publish_time();
                self.published(result.event().ok());
                self.advance()
            }
            (PublishStep::Sync, result) => {
                self.synced(result.child::<FinishedBatch, IoError>().ok());
                self.advance()
            }
            (PublishStep::Lineage, result) => self.lineage_known(result),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::document::{
        DocumentChange, DocumentChangeKind, DocumentSyncPublish, DocumentSyncRevision,
        DocumentTarget,
    };
    use aruna_core::events::ShardNetEvent;
    use aruna_core::structs::placement::record::PlacementRef;

    fn serving() -> (NodeId, RealmId) {
        let node = iroh::SecretKey::from_bytes(&[1; 32]).public();
        (node, RealmId::from_bytes([3; 32]))
    }

    fn batch(name: &[u8]) -> DrainSubBatch {
        let peer = iroh::SecretKey::from_bytes(&[2; 32]).public();
        let target = DocumentTarget::RealmConfig {
            realm_id: RealmId::from_bytes([3; 32]),
        };
        let event_id = ulid::Ulid::from_parts(1, 1);
        let current = DocumentSyncRevision {
            generation: 1,
            event_id,
            actor: peer,
            updated_at_ms: 1,
        };
        let change = DocumentChange {
            base: None,
            current,
            kind: DocumentChangeKind::Delete,
            placement: PlacementRef::NIL,
        };
        let document = DocumentSyncPublish::Delete {
            event_id,
            target: target.clone(),
            change,
            allow_genesis: false,
        };
        DrainSubBatch {
            peers: vec![peer],
            documents: vec![document],
            topics: vec![TopicId::hash(name)],
            origins: vec![None],
            targets: vec![target],
            record_keys: vec![name.to_vec()],
            stamps: vec![RowStamp {
                lineage: Some(lineage(name)),
                digest: Some(lineage(name)),
            }],
        }
    }

    fn lineage(name: &[u8]) -> [u8; 32] {
        *blake3::hash(name).as_bytes()
    }

    /// What the run keeps of the publication of the record `name` from [`batch`].
    fn kept(key: &[u8], name: &[u8]) -> (Vec<u8>, Publication) {
        let (lineage, digest) = (lineage(name), lineage(name));
        (key.to_vec(), Publication { lineage, digest })
    }

    fn net(event: DocumentNetEvent) -> IoResult {
        IoResult::Event(Box::new(Event::Net(NetEvent::DocumentSync(event))))
    }

    // A published batch syncs after its publish, and its records count as published and deleted.
    #[test]
    fn published_batch_synced() {
        let mut run = PublishBatchesOperation::new(serving(), vec![batch(b"one")]);
        assert!(matches!(run.first(), Ok(Next::Call(IoCall::Effect(_)))));
        let published = net(DocumentNetEvent::DocumentsPublished {
            targets: Vec::new(),
        });
        assert!(matches!(
            run.next(published),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let finished = FinishedBatch {
            deleted: vec![b"one".to_vec()],
            ..FinishedBatch::default()
        };
        let synced = IoResult::Child(ChildOutcome::new(Ok::<_, IoError>(finished)));
        let Ok(Next::Done(done)) = run.next(synced) else {
            panic!("the batches finish");
        };
        assert_eq!(done.published, vec![kept(b"one", b"one")]);
        assert_eq!(done.deleted, vec![b"one".to_vec()]);
    }

    // A record published onto a topic its publish created has no lineage before the publish; the
    // run learns it before the sync for the finish, but keeps no evidence a retry relies on.
    #[test]
    fn missing_lineage_filled() {
        let mut first = batch(b"one");
        first.stamps[0].lineage = None;
        let mut run = PublishBatchesOperation::new(serving(), vec![first]);
        run.first().unwrap();
        let published = net(DocumentNetEvent::DocumentsPublished {
            targets: Vec::new(),
        });
        let query = run.next(published);
        assert!(matches!(query, Ok(Next::Call(IoCall::Group(_)))));
        let root = ShardNetEvent::Root(Ok(lineage(b"one")));
        let root = Event::Net(NetEvent::Shard(Box::new(root)));
        let results = vec![GroupResult::Done(Box::new(root))];
        let sync = run.next(IoResult::Group(GroupEvent::Finished { results }));
        assert!(matches!(sync, Ok(Next::Call(IoCall::Child(_)))));
        let synced = Ok::<_, IoError>(FinishedBatch::default());
        let Ok(Next::Done(done)) = run.next(IoResult::Child(ChildOutcome::new(synced))) else {
            panic!("the batch finishes");
        };
        assert!(done.published.is_empty());
    }

    // A batch sharing an admin origin with the one before it waits for that sync instead of
    // publishing beside it, and a failed sync there holds it before it publishes.
    #[test]
    fn shared_origin_waits() {
        let origin = iroh::SecretKey::from_bytes(&[4; 32]).public();
        let (mut first, mut second) = (batch(b"one"), batch(b"two"));
        (first.origins, second.origins) = (vec![Some(origin)], vec![Some(origin)]);
        let mut run = PublishBatchesOperation::new(serving(), vec![first, second]);
        run.first().unwrap();
        let published = net(DocumentNetEvent::DocumentsPublished {
            targets: Vec::new(),
        });
        let sync = run.next(published);
        assert!(matches!(sync, Ok(Next::Call(IoCall::Child(_)))));
        let mut finished = FinishedBatch::default();
        finished.outcome.retry_needed = true;
        finished
            .outcome
            .sync_waits
            .push((origin, TopicId::hash(b"one")));
        let synced = IoResult::Child(ChildOutcome::new(Ok::<_, IoError>(finished)));
        let Ok(Next::Done(done)) = run.next(synced) else {
            panic!("the later batch is held");
        };
        assert_eq!(done.published, vec![kept(b"one", b"one")]);
        assert!(done.outcome.retry_needed);
    }

    // A successor on one waiting topic is held while its origin also waits on another topic.
    #[test]
    fn split_waits_held() {
        let origin = iroh::SecretKey::from_bytes(&[4; 32]).public();
        let (mut first, mut second) = (batch(b"one"), batch(b"one"));
        (first.origins, second.origins) = (vec![Some(origin)], vec![Some(origin)]);
        second.record_keys = vec![b"later".to_vec()];
        let mut run = PublishBatchesOperation::new(serving(), vec![first, second]);
        run.first().unwrap();
        let published = net(DocumentNetEvent::DocumentsPublished {
            targets: Vec::new(),
        });
        run.next(published).unwrap();
        let mut finished = FinishedBatch::default();
        finished.outcome.retry_needed = true;
        let waits = [TopicId::hash(b"one"), TopicId::hash(b"two")];
        finished.outcome.sync_waits = waits.map(|topic| (origin, topic)).to_vec();
        let synced = IoResult::Child(ChildOutcome::new(Ok::<_, IoError>(finished)));
        let Ok(Next::Done(done)) = run.next(synced) else {
            panic!("the successor is held");
        };
        assert!(done.outcome.retry_needed);
    }

    // A row held by its origin's wait on another topic blocks that origin for the later rows
    // of the same batch, so a same-topic successor of that origin stays behind it.
    #[test]
    fn held_row_blocks() {
        let origin = iroh::SecretKey::from_bytes(&[4; 32]).public();
        let mut first = batch(b"one");
        first.origins = vec![Some(origin)];
        let (mut second, mut later) = (batch(b"two"), batch(b"one"));
        later.record_keys = vec![b"later".to_vec()];
        second.documents.append(&mut later.documents);
        second.topics.append(&mut later.topics);
        second.origins = vec![Some(origin), Some(origin)];
        second.targets.append(&mut later.targets);
        second.record_keys.append(&mut later.record_keys);
        second.stamps.append(&mut later.stamps);
        let mut run = PublishBatchesOperation::new(serving(), vec![first, second]);
        run.first().unwrap();
        let published = net(DocumentNetEvent::DocumentsPublished {
            targets: Vec::new(),
        });
        run.next(published).unwrap();
        let mut finished = FinishedBatch::default();
        finished.outcome.retry_needed = true;
        finished.outcome.sync_waits = vec![(origin, TopicId::hash(b"one"))];
        let synced = IoResult::Child(ChildOutcome::new(Ok::<_, IoError>(finished)));
        let Ok(Next::Done(done)) = run.next(synced) else {
            panic!("both rows of the second batch are held");
        };
        assert!(done.outcome.blocked_topics.contains(&TopicId::hash(b"two")));
        assert!(done.outcome.blocked_origins.contains(&origin));
    }

    // A failed sync holds no later record on its own topic: the record is in the oplog, and the
    // later record's sync carries it in order, also past a peer that stays unreachable.
    #[test]
    fn same_topic_proceeds() {
        let origin = iroh::SecretKey::from_bytes(&[4; 32]).public();
        let (mut first, mut second) = (batch(b"one"), batch(b"one"));
        (first.origins, second.origins) = (vec![Some(origin)], vec![Some(origin)]);
        second.record_keys = vec![b"later".to_vec()];
        let mut run = PublishBatchesOperation::new(serving(), vec![first, second]);
        run.first().unwrap();
        let published = net(DocumentNetEvent::DocumentsPublished {
            targets: Vec::new(),
        });
        let sync = run.next(published);
        assert!(matches!(sync, Ok(Next::Call(IoCall::Child(_)))));
        let mut finished = FinishedBatch::default();
        finished.outcome.retry_needed = true;
        finished
            .outcome
            .sync_waits
            .push((origin, TopicId::hash(b"one")));
        let synced = IoResult::Child(ChildOutcome::new(Ok::<_, IoError>(finished)));
        let publish = run.next(synced);
        assert!(matches!(publish, Ok(Next::Call(IoCall::Effect(_)))));
    }

    // An entry the adapter published after a retried entry of its topic stays in the oplog but
    // is not synced, so no peer receives it first.
    #[test]
    fn retried_holds_later() {
        let mut joined = batch(b"one");
        let later = batch(b"one");
        joined.documents.extend(later.documents);
        joined.topics.extend(later.topics);
        joined.origins.extend(later.origins);
        joined.targets.extend(later.targets);
        joined.record_keys.push(b"later".to_vec());
        joined.stamps.extend(later.stamps);
        let mut run = PublishBatchesOperation::new(serving(), vec![joined]);
        run.first().unwrap();
        let partial = net(DocumentNetEvent::DocumentsPartiallyPublished {
            published_indices: vec![1],
            retry_indices: vec![0],
            error: "busy".to_string(),
        });
        let Ok(Next::Done(done)) = run.next(partial) else {
            panic!("nothing is ready to sync");
        };
        assert_eq!(done.published, vec![kept(b"later", b"one")]);
        assert!(done.outcome.retry_needed);
    }

    // After a failed publish, a later sub-batch leaves out the entries of the blocked topic and
    // publishes the rest.
    #[test]
    fn blocked_entries_skipped() {
        let mut second = batch(b"one");
        let other = batch(b"two");
        second.documents.extend(other.documents);
        second.topics.extend(other.topics);
        second.origins.extend(other.origins);
        second.targets.extend(other.targets);
        second.record_keys.extend(other.record_keys);
        second.stamps.extend(other.stamps);
        let mut run = PublishBatchesOperation::new(serving(), vec![batch(b"one"), second]);
        run.first().unwrap();
        let failed = net(DocumentNetEvent::Error {
            target: None,
            error: "down".to_string(),
        });
        let Ok(Next::Call(IoCall::Effect(effect))) = run.next(failed) else {
            panic!("the second sub-batch publishes what is not blocked");
        };
        let documents = match *effect {
            Effect::Net(NetEffect::DocumentSync(DocumentEffect::PublishDocuments {
                documents,
                ..
            })) => documents,
            _ => panic!("expected a publish"),
        };
        assert_eq!(documents.len(), 1);
    }

    // The second publish runs together with the first batch's sync; a failed publish blocks its
    // topic and leaves its records for a retry.
    #[test]
    fn failed_publish_blocks() {
        let batches = vec![batch(b"one"), batch(b"two")];
        let mut run = PublishBatchesOperation::new(serving(), batches);
        run.first().unwrap();
        let published = net(DocumentNetEvent::DocumentsPublished {
            targets: Vec::new(),
        });
        let Ok(Next::Call(IoCall::Group(group))) = run.next(published) else {
            panic!("the second publish runs with the first sync");
        };
        assert_eq!(group.members.len(), 2);
        let failed = DocumentNetEvent::Error {
            target: None,
            error: "down".to_string(),
        };
        let failed = Event::Net(NetEvent::DocumentSync(failed));
        let index = 0;
        let result = GroupResult::Done(Box::new(failed));
        let member = IoResult::Group(GroupEvent::Member { index, result });
        assert_eq!(run.next(member), Ok(Next::Wait));
        let finished = Ok::<_, IoError>(FinishedBatch::default());
        let finished = Event::Child(ChildOutcome::new(finished));
        let result = GroupResult::Done(Box::new(finished));
        let member = IoResult::Group(GroupEvent::Member { index: 1, result });
        assert_eq!(run.next(member), Ok(Next::Wait));
        let Ok(Next::Done(done)) = run.next(IoResult::Group(GroupEvent::Closed)) else {
            panic!("a failed publish leaves nothing to sync");
        };
        assert!(done.outcome.retry_needed);
        assert_eq!(done.outcome.blocked_topics, vec![TopicId::hash(b"two")]);
        assert_eq!(done.published, vec![kept(b"one", b"one")]);
    }
}
