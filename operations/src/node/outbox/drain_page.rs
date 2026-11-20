//! Drains one page of the open outbox rotation: reads it, pulls missing shard geneses, splits
//! the records, relays those this node cannot publish and publishes the rest.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use aruna_core::admin_documents::AdminDocumentOperation;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::{
    DocumentNetEvent, DocumentOutboxEvent, DocumentOutboxRecord, DocumentReconcileResult,
    DocumentTarget,
};
use aruna_core::document_apply::ApplyError;
use aruna_core::effects::{Effect, NetEffect, ShardNetEffect};
use aruna_core::events::{Event, NetEvent, ShardNetEvent};
use aruna_core::id::NodeId;
use aruna_core::id_mint::IdMint;
use aruna_core::operation::Operation;
use aruna_core::placement::{
    bucket_membership, holds_placement, is_draining_holder, resolve_shard_holders,
};
use aruna_core::row_plan::PlanOperation;
use aruna_core::structs::identity::group_delete::GroupDeletionError;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::task::TaskKey;
use irokle::TopicId;
use tracing::warn;

use crate::groups::deletion::coordinator::SyncDecisionOperation;
use crate::node::outbox::finish_batch::{FinishBatchOperation, FinishedBatch};
use crate::node::outbox::publish_batches::{PublishBatchesOperation, PublishedBatches};
use crate::node::outbox::relay_records::{RelayRecordsOperation, RelayedRecords};
use crate::sync::document_outbox::{OutboxPagePlan, OutboxReadBatch};
use crate::sync::outbox::batches::{
    DrainInvocation, DrainSyncOutcome, build_drain_batches, resolve_publish_placement,
};
use crate::sync::outbox::defer::{DrainRecord, classify_deferred_record, partition_drain_records};
use crate::sync::outbox::rotation::OutboxRotation;
use crate::sync::reconcile::SyncTopicsOperation;
use crate::sync::shard_placement::sort_node_ids;

/// How long a record may wait for its shard topic's genesis before the drain stops treating the
/// wait as normal and says so at error level.
pub const OUTBOX_STUCK_AFTER: Duration = Duration::from_secs(300);

/// How a page ended, which decides whether the invocation reads on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PageEnd {
    /// Records were processed; the rotation ends with them at the boundary or the tail.
    Records {
        has_more: bool,
        boundary_reached: bool,
    },
    /// Nothing on the page belonged to the rotation and more follows.
    Skip,
    /// Reading stops for this invocation.
    Stop,
}

/// One drained page: the invocation it updated, how it ended, and the records this run
/// published and deleted, which the drain remembers and forgets.
#[derive(Debug, PartialEq)]
pub struct DrainedPage {
    pub invocation: DrainInvocation,
    pub end: PageEnd,
    pub published: Vec<Vec<u8>>,
    pub deleted: Vec<Vec<u8>>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum PageStep {
    Read,
    Decision,
    Existing,
    Missing,
    MissingFinish,
    Available,
    Relay,
    Publish,
}

type PageTurn = Result<Next<DrainedPage>, IoError>;

/// Missing shard topics keyed by their sorted genesis peers, with the peers in record order.
type MissingTopics = BTreeMap<Vec<NodeId>, (Vec<NodeId>, BTreeSet<TopicId>)>;

/// Drains one page of the rotation into the invocation it is given.
#[derive(Debug, PartialEq)]
pub struct DrainPageOperation {
    serving: (NodeId, RealmId),
    config: Option<Arc<RealmConfigDocument>>,
    rotation: OutboxRotation,
    limit: usize,
    published_before: BTreeSet<Vec<u8>>,
    page: Option<DrainedPage>,
    batch: Option<OutboxReadBatch>,
    records: Vec<DrainRecord>,
    to_publish: Vec<DrainRecord>,
    missing: VecDeque<(Vec<NodeId>, Vec<TopicId>)>,
    blocked: DrainSyncOutcome,
    step: PageStep,
    ids: IdMint,
    started_ms: u64,
    now_ms: u64,
}

impl DrainPageOperation {
    /// Drains at most `limit` records after the invocation's cursor. `published_before` holds the
    /// records this process published already, which only sync.
    pub fn new(
        serving: (NodeId, RealmId),
        config: Option<Arc<RealmConfigDocument>>,
        rotation: OutboxRotation,
        invocation: DrainInvocation,
        limit: usize,
        published_before: BTreeSet<Vec<u8>>,
    ) -> Self {
        let page = DrainedPage {
            invocation,
            end: PageEnd::Stop,
            published: Vec::new(),
            deleted: Vec::new(),
        };
        Self {
            serving,
            config,
            rotation,
            limit,
            published_before,
            page: Some(page),
            batch: None,
            records: Vec::new(),
            to_publish: Vec::new(),
            missing: VecDeque::new(),
            blocked: DrainSyncOutcome::default(),
            step: PageStep::Read,
            ids: IdMint::default(),
            started_ms: 0,
            now_ms: 0,
        }
    }

    fn invocation(&mut self) -> Result<&mut DrainInvocation, IoError> {
        let page = self.page.as_mut().ok_or(IoError::Unfinished)?;
        Ok(&mut page.invocation)
    }

    fn done(&mut self, end: PageEnd) -> PageTurn {
        let mut page = self.page.take().ok_or(IoError::Unfinished)?;
        page.end = end;
        Ok(Next::Done(page))
    }

    fn ask<O>(&mut self, step: PageStep, operation: O) -> PageTurn
    where
        O: Operation + 'static,
        O::Output: PartialEq + 'static,
        O::Error: PartialEq + 'static,
    {
        self.step = step;
        Ok(Next::Call(IoCall::child(operation)))
    }

    fn existing(&mut self, step: PageStep, topics: Vec<TopicId>) -> PageTurn {
        self.step = step;
        let query = Effect::Net(NetEffect::Shard(Box::new(ShardNetEffect::Existing(topics))));
        Ok(Next::Call(IoCall::from(query)))
    }

    /// Takes in the page read: the rotation's records go on, an empty page skips or stops.
    fn scanned(&mut self, mut batch: OutboxReadBatch) -> PageTurn {
        let elapsed = Duration::from_millis(self.now_ms.saturating_sub(self.started_ms));
        let invocation = self.invocation()?;
        invocation.scan_elapsed += elapsed;
        let has_more = batch.has_more;
        invocation.cursor = batch.next_start_after;
        let cursor = invocation.cursor.clone();
        let boundary_reached = self.rotation.at_end(cursor.as_deref());
        let rotation = &self.rotation;
        batch.records.retain(|(key, _)| rotation.admits(key));
        if !batch.records.is_empty() {
            return self.prepare(batch.records, has_more, boundary_reached);
        }
        let invocation = self.invocation()?;
        if boundary_reached || !has_more {
            invocation.reached_end = true;
            self.done(PageEnd::Stop)
        } else if invocation.cursor.is_some() {
            self.done(PageEnd::Skip)
        } else {
            invocation.reached_end = true;
            self.done(PageEnd::Stop)
        }
    }

    /// Counts the page, resolves each record's placement and topic, and asks which shard
    /// topics this node may pull a missing genesis for already exist.
    fn prepare(
        &mut self,
        records: Vec<(Vec<u8>, DocumentOutboxRecord)>,
        has_more: bool,
        boundary_reached: bool,
    ) -> PageTurn {
        let (config, realm_id) = (self.config.clone(), self.serving.1);
        let invocation = self.invocation()?;
        invocation.pages += 1;
        invocation.records += records.len();
        let page_oldest = (records.iter())
            .map(|(_, record)| record.outbox_id.timestamp_ms())
            .min();
        if let Some(page_oldest) = page_oldest {
            let current = invocation.oldest_record_ms.unwrap_or(page_oldest);
            invocation.oldest_record_ms = Some(current.min(page_oldest));
        }
        let mut prepared = Vec::with_capacity(records.len());
        for (record_key, mut record) in records {
            invocation.config_drained |=
                matches!(record.target, DocumentTarget::RealmConfig { .. });
            record.placement =
                resolve_publish_placement(config.as_deref(), &record.target, record.placement);
            let topic = record.target.sync_topic_id(realm_id, &record.placement);
            prepared.push((record_key, record, topic));
        }
        if let Some(page) = self.page.as_mut() {
            page.end = PageEnd::Records {
                has_more,
                boundary_reached,
            };
        }
        let topics: BTreeSet<TopicId> = (prepared.iter())
            .filter(|(_, record, topic)| self.may_pull(record, topic))
            .map(|(_, _, topic)| *topic)
            .collect();
        self.records = prepared;
        if topics.is_empty() {
            return self.after_missing();
        }
        self.existing(PageStep::Existing, topics.into_iter().collect())
    }

    /// Whether this node may pull a missing genesis for `record`: a shard record not deferred
    /// for the rotation, whose bucket this node holds or drains.
    fn may_pull(&self, record: &DocumentOutboxRecord, topic: &TopicId) -> bool {
        let deferred = (self.page.as_ref())
            .is_some_and(|page| page.invocation.defer.deferred_topics.contains(topic));
        let local = self.serving.0;
        record.target.uses_shard_topic()
            && !deferred
            && self.config.as_deref().is_none_or(|config| {
                holds_placement(config, &record.placement, local)
                    || is_draining_holder(config, &record.placement, local)
            })
    }

    /// Groups the topics without a local genesis by the peers that may hold it: the record's
    /// stamped peers and the live holders.
    fn missing(&mut self, existing: &BTreeSet<TopicId>) -> PageTurn {
        let local = self.serving.0;
        let mut missing = MissingTopics::new();
        for (_, record, topic) in &self.records {
            if !self.may_pull(record, topic) || existing.contains(topic) {
                continue;
            }
            let mut peers = record.peers.clone();
            if let Some(config) = self.config.as_deref() {
                for holder in resolve_shard_holders(config, &record.placement) {
                    if !peers.contains(&holder) {
                        peers.push(holder);
                    }
                }
            }
            peers.retain(|peer| *peer != local);
            if peers.is_empty() {
                continue;
            }
            let mut peer_key = peers.clone();
            sort_node_ids(&mut peer_key);
            let entry = missing.entry(peer_key);
            entry.or_insert_with(|| (peers, BTreeSet::new())).1.insert(*topic);
        }
        self.missing = (missing.into_values())
            .map(|(peers, topics)| (peers, topics.into_iter().collect()))
            .collect();
        self.next_missing()
    }

    fn next_missing(&mut self) -> PageTurn {
        let Some((peers, topics)) = self.missing.pop_front() else {
            return self.after_missing();
        };
        let (node_id, realm_id) = self.serving;
        let sync = SyncTopicsOperation::new(realm_id, topics, peers).queuing_metadata(node_id);
        self.ask(PageStep::Missing, sync)
    }

    /// Publishes shard records to their bucket's sync members, keeping the stamped peers as
    /// genesis sources above, then asks which of the page's topics exist now.
    fn after_missing(&mut self) -> PageTurn {
        if let Some(config) = self.config.clone() {
            for (_, record, _) in &mut self.records {
                if !record.target.uses_shard_topic() {
                    continue;
                }
                let members = bucket_membership(&config, &record.placement, self.now_ms).members;
                if !members.is_empty() {
                    record.peers = members;
                }
            }
        }
        let topics: BTreeSet<TopicId> = self.records.iter().map(|(_, _, topic)| *topic).collect();
        self.existing(PageStep::Available, topics.into_iter().collect())
    }

    /// Splits the page into records to publish, deferred ones and undeliverable ones, counts the
    /// stuck ones and relays what this node cannot publish.
    fn partition(&mut self, existing: BTreeSet<TopicId>) -> PageTurn {
        let (local, now_ms) = (self.serving.0, self.now_ms);
        let config = self.config.clone();
        let device = (config.as_deref())
            .and_then(|config| config.node_kind(&local))
            .is_some_and(|kind| !kind.is_sync_eligible());
        let records = std::mem::take(&mut self.records);
        let invocation = self.invocation()?;
        let (to_publish, deferred, undeliverable) = partition_drain_records(
            records,
            &mut invocation.defer,
            !device,
            |topic| existing.contains(&topic),
            |record| classify_deferred_record(config.as_deref(), local, record),
        );
        invocation.deferred += deferred.len();
        let stuck_after = OUTBOX_STUCK_AFTER.as_millis() as u64;
        for (_, record, topic) in &deferred {
            let record_ms = record.outbox_id.timestamp_ms();
            if now_ms.saturating_sub(record_ms) < stuck_after {
                continue;
            }
            invocation.stuck += 1;
            let older = (invocation.oldest_stuck.as_ref())
                .is_none_or(|(oldest_ms, ..)| record_ms < *oldest_ms);
            if older {
                let stuck = (record_ms, record.target.clone(), *topic, record.placement);
                invocation.oldest_stuck = Some(stuck);
            }
        }
        self.to_publish = to_publish;
        if undeliverable.is_empty() {
            return self.publish();
        }
        let relay = RelayRecordsOperation::new(local, config, undeliverable);
        self.ask(PageStep::Relay, relay)
    }

    /// Groups the records to publish into sub-batches and publishes them.
    fn publish(&mut self) -> PageTurn {
        let to_publish = std::mem::take(&mut self.to_publish);
        let (groups, subbatches) = build_drain_batches(to_publish, &self.published_before);
        let invocation = self.invocation()?;
        invocation.groups += groups;
        invocation.subbatches += subbatches.len();
        if subbatches.is_empty() {
            return self.finished();
        }
        // A run that breaks off retries every sub-batch and blocks its ordering domains.
        self.blocked = DrainSyncOutcome {
            retry_needed: true,
            ..DrainSyncOutcome::default()
        };
        for subbatch in &subbatches {
            let (topics, origins) = subbatch.ordering_domains();
            self.blocked.blocked_topics.extend(topics);
            self.blocked.blocked_origins.extend(origins);
        }
        self.started_ms = self.now_ms;
        let publish = PublishBatchesOperation::new(self.serving, subbatches);
        self.ask(PageStep::Publish, publish)
    }

    fn finished(&mut self) -> PageTurn {
        let end = self.page.as_ref().map_or(PageEnd::Stop, |page| page.end);
        self.done(end)
    }
}

/// The topics a shard topic query found with a local genesis; none without a network.
fn existing_topics(result: IoResult) -> Result<BTreeSet<TopicId>, IoError> {
    match result.event()? {
        Event::Net(NetEvent::Shard(answer)) => match *answer {
            ShardNetEvent::Existing(topics) => Ok(topics),
            _ => Ok(BTreeSet::new()),
        },
        _ => Ok(BTreeSet::new()),
    }
}

/// Whether a page holds a group deletion, whose decision must be durable before it publishes.
fn deletes_group(batch: &OutboxReadBatch) -> bool {
    batch.records.iter().any(|(_, record)| match &record.event {
        DocumentOutboxEvent::AdminOperation { event, .. } => {
            matches!(event.op, AdminDocumentOperation::GroupDeleted { .. })
        }
        _ => false,
    })
}

impl Operation for DrainPageOperation {
    type Output = DrainedPage;
    type Error = IoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn seed(&mut self, entropy: u128) {
        self.ids = IdMint::new(entropy);
    }

    fn first(&mut self) -> PageTurn {
        self.started_ms = self.now_ms;
        let read = OutboxPagePlan {
            prefix: Vec::new(),
            start_after: self.invocation()?.cursor.clone(),
            limit: self.limit,
        };
        self.ask(PageStep::Read, PlanOperation::new(read))
    }

    fn next(&mut self, result: IoResult) -> PageTurn {
        let task_id = TaskKey::DrainSyncOutbox;
        match self.step {
            PageStep::Read => match result.child::<OutboxReadBatch, ApplyError>()? {
                Ok(batch) if deletes_group(&batch) => {
                    self.batch = Some(batch);
                    self.ask(PageStep::Decision, SyncDecisionOperation)
                }
                Ok(batch) => self.scanned(batch),
                Err(error) => {
                    warn!(
                        task_id = ?task_id,
                        error = %error,
                        "Failed to read document sync outbox record"
                    );
                    self.invocation()?.read_failed = true;
                    self.done(PageEnd::Stop)
                }
            },
            PageStep::Decision => {
                if let Err(error) = result.child::<(), GroupDeletionError>()? {
                    warn!(
                        error = %error,
                        "Group deletion decision is not durable; retaining outbox"
                    );
                    let invocation = self.invocation()?;
                    invocation.read_failed = true;
                    invocation.outcome.retry_needed = true;
                    return self.done(PageEnd::Stop);
                }
                let batch = self.batch.take().ok_or(IoError::Unfinished)?;
                self.scanned(batch)
            }
            PageStep::Existing => {
                let existing = existing_topics(result)?;
                self.missing(&existing)
            }
            PageStep::Missing => {
                let synced = result.child::<DocumentReconcileResult, ApplyError>()?;
                let synced = DocumentNetEvent::reconciled(synced);
                let event = Event::Net(NetEvent::DocumentSync(synced));
                let generation = self.ids.mint(self.now_ms);
                let batch = (Vec::new(), Vec::new());
                let outcome = DrainSyncOutcome::default();
                let serving = Some(self.serving);
                let finish = FinishBatchOperation::new(serving, generation, batch, event, outcome);
                self.ask(PageStep::MissingFinish, finish)
            }
            PageStep::MissingFinish => {
                let outcome = match result.child::<FinishedBatch, IoError>()? {
                    Ok(finished) => finished.outcome,
                    Err(error) => {
                        warn!(
                            task_id = ?task_id,
                            %error,
                            "Finishing a document sync batch broke off"
                        );
                        DrainSyncOutcome {
                            retry_needed: true,
                            ..DrainSyncOutcome::default()
                        }
                    }
                };
                self.invocation()?.outcome.merge(outcome);
                self.next_missing()
            }
            PageStep::Available => {
                let existing = existing_topics(result)?;
                self.partition(existing)
            }
            PageStep::Relay => {
                match result.child::<RelayedRecords, IoError>()? {
                    Ok(relayed) => self.invocation()?.undeliverable += relayed.left,
                    Err(error) => warn!(%error, "Relaying undeliverable outbox records broke off"),
                }
                self.publish()
            }
            PageStep::Publish => {
                let elapsed = Duration::from_millis(self.now_ms.saturating_sub(self.started_ms));
                let blocked = std::mem::take(&mut self.blocked);
                let published = result.child::<PublishedBatches, IoError>()?;
                let page = self.page.as_mut().ok_or(IoError::Unfinished)?;
                match published {
                    Ok(done) => {
                        page.invocation.publish_elapsed += done.publish_elapsed;
                        page.invocation.outcome.merge(done.outcome);
                        page.published.extend(done.published);
                        page.deleted.extend(done.deleted);
                    }
                    Err(error) => {
                        warn!(
                            task_id = ?task_id,
                            %error,
                            "Publishing document sync batches broke off"
                        );
                        page.invocation.publish_elapsed += elapsed;
                        page.invocation.outcome.merge(blocked);
                    }
                }
                self.finished()
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::calls::ChildOutcome;

    fn page() -> DrainPageOperation {
        let local = iroh::SecretKey::from_bytes(&[1; 32]).public();
        let serving = (local, RealmId::from_bytes([3; 32]));
        let rotation = OutboxRotation::default();
        let invocation = DrainInvocation::new(&rotation);
        DrainPageOperation::new(serving, None, rotation, invocation, 8, BTreeSet::new())
    }

    fn read(result: Result<OutboxReadBatch, ApplyError>) -> IoResult {
        IoResult::Child(ChildOutcome::new(result))
    }

    // A failed read stops the invocation and marks it for a retry.
    #[test]
    fn failed_read_stops() {
        let mut run = page();
        assert!(matches!(run.first(), Ok(Next::Call(IoCall::Child(_)))));
        let failed = read(Err(ApplyError::Failed("down".to_string())));
        let Ok(Next::Done(page)) = run.next(failed) else {
            panic!("a failed read ends the page");
        };
        assert_eq!(page.end, PageEnd::Stop);
        assert!(page.invocation.read_failed);
    }

    // An empty page at the tail ends the rotation.
    #[test]
    fn empty_tail_ends() {
        let mut run = page();
        run.first().unwrap();
        let empty = OutboxReadBatch {
            records: Vec::new(),
            has_more: false,
            next_start_after: None,
        };
        let Ok(Next::Done(page)) = run.next(read(Ok(empty))) else {
            panic!("an empty tail ends the page");
        };
        assert_eq!(page.end, PageEnd::Stop);
        assert!(page.invocation.reached_end);
    }
}
