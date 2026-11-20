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
use crate::sync::outbox::batches::{DrainSubBatch, DrainSyncOutcome};

/// What publishing the sub-batches left: the sync outcome, the time the publishes took, the
/// records this run published and those it deleted after their sync.
#[derive(Debug, Default, PartialEq)]
pub struct PublishedBatches {
    pub outcome: DrainSyncOutcome,
    pub publish_elapsed: Duration,
    pub published: Vec<Vec<u8>>,
    pub deleted: Vec<Vec<u8>>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum PublishStep {
    Sync,
    Publish,
    Window,
}

/// A sub-batch whose sync runs: its record keys and the ordering domains its failure blocks.
#[derive(Debug, PartialEq)]
struct Syncing {
    record_keys: Vec<Vec<u8>>,
    domains: (Vec<TopicId>, Vec<NodeId>),
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
            let Some(mut batch) = self.batches.pop_front() else {
                return match self.awaiting.take() {
                    Some(last) => self.sync_alone(last),
                    None => Ok(Next::Done(std::mem::take(&mut self.out))),
                };
            };
            if batch.documents.is_empty() {
                match self.awaiting.replace(batch) {
                    Some(before) => return self.sync_alone(before),
                    None => continue,
                }
            }
            let documents = std::mem::take(&mut batch.documents);
            let peers = batch.peers.clone();
            let publish = DocumentEffect::PublishDocuments { documents, peers };
            let publish = Effect::Net(NetEffect::DocumentSync(publish));
            self.publishing = Some(batch);
            self.started_ms = self.now_ms;
            let Some(before) = self.awaiting.take() else {
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

    fn sync_batch(&mut self, batch: DrainSubBatch) -> SyncBatchOperation {
        self.syncing = Some(Syncing {
            record_keys: batch.record_keys.clone(),
            domains: batch.ordering_domains(),
        });
        let generation = self.ids.mint(self.now_ms);
        SyncBatchOperation::new(self.serving, generation, batch)
    }

    fn sync_alone(&mut self, batch: DrainSubBatch) -> PublishTurn {
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
                if finished.deleted {
                    self.out.deleted.extend(syncing.record_keys);
                }
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
                self.out.published.extend(batch.record_keys.iter().cloned());
                self.awaiting = Some(batch);
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
                match batch.sync_subset(&published_indices) {
                    Some(published) if !published.record_keys.is_empty() => {
                        self.out
                            .published
                            .extend(published.record_keys.iter().cloned());
                        self.awaiting = Some(published);
                    }
                    Some(_) => {}
                    None => {
                        warn!(task_id = ?task_id, "Invalid partial document publish indices");
                    }
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
        }
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
            deleted: true,
            ..FinishedBatch::default()
        };
        let synced = IoResult::Child(ChildOutcome::new(Ok::<_, IoError>(finished)));
        let Ok(Next::Done(done)) = run.next(synced) else {
            panic!("the batches finish");
        };
        assert_eq!(done.published, vec![b"one".to_vec()]);
        assert_eq!(done.deleted, vec![b"one".to_vec()]);
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
        assert_eq!(done.published, vec![b"one".to_vec()]);
    }
}
