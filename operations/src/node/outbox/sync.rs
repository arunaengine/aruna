//! Syncs one published outbox sub-batch with its peers and finishes it; a batch that must retry
//! blocks its ordering domains for the rest of the rotation.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::{DocumentNetEvent, DocumentReconcileResult};
use aruna_core::document_apply::ApplyError;
use aruna_core::events::{Event, NetEvent};
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use irokle::TopicId;
use ulid::Ulid;

use crate::node::outbox::finish::{FinishBatchOperation, FinishedBatch};
use crate::sync::outbox::batches::{DrainSubBatch, DrainSyncOutcome};
use crate::sync::reconcile::SyncTopicsOperation;

type SyncTurn = Result<Next<FinishedBatch>, IoError>;

/// One sub-batch synced and then finished. The usage refresh of its finish records under
/// `generation`.
#[derive(Debug, PartialEq)]
pub struct SyncBatchOperation {
    serving: (NodeId, RealmId),
    generation: Ulid,
    batch: Option<DrainSubBatch>,
    domains: (Vec<TopicId>, Vec<NodeId>),
    finishing: bool,
    started_ms: u64,
    now_ms: u64,
}

impl SyncBatchOperation {
    pub fn new(serving: (NodeId, RealmId), generation: Ulid, batch: DrainSubBatch) -> Self {
        Self {
            serving,
            generation,
            domains: batch.ordering_domains(),
            batch: Some(batch),
            finishing: false,
            started_ms: 0,
            now_ms: 0,
        }
    }
}

impl Operation for SyncBatchOperation {
    type Output = FinishedBatch;
    type Error = IoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> SyncTurn {
        let Some(batch) = self.batch.as_mut() else {
            return Err(IoError::Unfinished);
        };
        let (node_id, realm_id) = self.serving;
        let (topics, peers) = (std::mem::take(&mut batch.topics), batch.peers.clone());
        let sync = SyncTopicsOperation::new(realm_id, topics, peers).queuing_metadata(node_id);
        self.started_ms = self.now_ms;
        Ok(Next::Call(IoCall::child(sync)))
    }

    fn next(&mut self, result: IoResult) -> SyncTurn {
        if self.finishing {
            let mut finished = result.child::<FinishedBatch, IoError>()??;
            if finished.outcome.retry_needed {
                let (topics, origins) = std::mem::take(&mut self.domains);
                finished.outcome.blocked_topics.extend(topics);
                finished.outcome.blocked_origins.extend(origins);
            }
            return Ok(Next::Done(finished));
        }
        let Some(batch) = self.batch.take() else {
            return Err(IoError::Unfinished);
        };
        let synced = result.child::<DocumentReconcileResult, ApplyError>()?;
        let event = Event::Net(NetEvent::DocumentSync(DocumentNetEvent::reconciled(synced)));
        let elapsed = self.now_ms.saturating_sub(self.started_ms);
        let outcome = DrainSyncOutcome {
            sync_elapsed: Duration::from_millis(elapsed),
            ..DrainSyncOutcome::default()
        };
        let finished = (batch.record_keys, batch.targets);
        let finish = FinishBatchOperation::new(
            Some(self.serving),
            self.generation,
            finished,
            event,
            outcome,
        );
        self.finishing = true;
        Ok(Next::Call(IoCall::child(finish)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::calls::ChildOutcome;

    fn batch() -> DrainSubBatch {
        let peer = iroh::SecretKey::from_bytes(&[2; 32]).public();
        DrainSubBatch {
            peers: vec![peer],
            documents: Vec::new(),
            topics: vec![TopicId::hash(b"topic")],
            origins: vec![Some(peer)],
            targets: Vec::new(),
            record_keys: vec![b"record".to_vec()],
        }
    }

    // A finished batch that must retry blocks its topics and admin origins.
    #[test]
    fn retried_batch_blocks() {
        let local = iroh::SecretKey::from_bytes(&[1; 32]).public();
        let serving = (local, RealmId::from_bytes([3; 32]));
        let mut run = SyncBatchOperation::new(serving, Ulid::from_parts(1, 1), batch());
        assert!(matches!(run.first(), Ok(Next::Call(IoCall::Child(_)))));
        let failed = Err::<DocumentReconcileResult, _>(ApplyError::Failed("down".to_string()));
        let finish = run.next(IoResult::Child(ChildOutcome::new(failed)));
        assert!(matches!(finish, Ok(Next::Call(IoCall::Child(_)))));
        let mut finished = FinishedBatch::default();
        finished.outcome.retry_needed = true;
        let finished = Ok::<_, IoError>(finished);
        let Ok(Next::Done(done)) = run.next(IoResult::Child(ChildOutcome::new(finished))) else {
            panic!("the batch finishes");
        };
        assert_eq!(done.outcome.blocked_topics, vec![TopicId::hash(b"topic")]);
        assert_eq!(done.outcome.blocked_origins, batch().ordering_domains().1);
    }
}
