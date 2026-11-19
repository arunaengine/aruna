//! Pulls shard topics from peers and applies what arrived to this node's local views.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::{
    AppliedStep, AppliedSync, DocumentNetEvent, DocumentReconcileResult, applied_sync_entry,
    schedule_applied_drain,
};
use aruna_core::document_apply::ApplyError;
use aruna_core::effects::ShardNetEffect;
use aruna_core::events::ShardNetEvent;
use aruna_core::id::NodeId;
use aruna_core::id_mint::IdMint;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use irokle::TopicId;
use tracing::warn;

use crate::shard::{shard_answer, shard_call};
use crate::sync::reconcile::SyncTopicsOperation;

#[derive(Debug, PartialEq)]
enum PullStage {
    Fetch,
    Reconcile(NodeId, RealmId),
    Apply,
}

/// Fetches `topics` from `peers`, reconciles them and answers whether what arrived was applied.
#[derive(Debug, PartialEq)]
pub(crate) struct PullTopicsOperation {
    topics: Vec<TopicId>,
    peers: Vec<NodeId>,
    stage: PullStage,
}

impl PullTopicsOperation {
    pub(crate) fn new(topics: Vec<TopicId>, peers: Vec<NodeId>) -> Self {
        Self {
            topics,
            peers,
            stage: PullStage::Fetch,
        }
    }
}

impl Operation for PullTopicsOperation {
    type Output = bool;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<bool>, IoError> {
        Ok(Next::Call(shard_call(ShardNetEffect::Pull {
            topics: std::mem::take(&mut self.topics),
            peers: std::mem::take(&mut self.peers),
        })))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, IoError> {
        match std::mem::replace(&mut self.stage, PullStage::Apply) {
            PullStage::Fetch => match shard_answer(result)? {
                ShardNetEvent::Pulled {
                    node_id,
                    realm_id,
                    sync,
                } => {
                    self.stage = PullStage::Reconcile(node_id, realm_id);
                    let reconcile = SyncTopicsOperation::after_fetch(realm_id, *sync);
                    Ok(Next::Call(IoCall::child(reconcile)))
                }
                _ => Ok(Next::Done(false)),
            },
            PullStage::Reconcile(node_id, realm_id) => {
                let reconciled = result.child::<DocumentReconcileResult, ApplyError>()?;
                let sync = DocumentNetEvent::reconciled(reconciled);
                let apply = ApplyPulledOperation::new(node_id, Some(realm_id), sync);
                Ok(Next::Call(IoCall::child(apply)))
            }
            PullStage::Apply => Ok(Next::Done(result.child::<bool, IoError>()??)),
        }
    }
}

type ApplyTurn = Result<Next<bool>, IoError>;

/// Queues the usage, watch interest, mail settings and metadata follow-ups of a document
/// reconcile for the node drain. Answers false when the reconcile itself failed. A follow-up
/// that cannot be queued only warns: each has its own retry, and the applied documents stay.
#[derive(Debug, PartialEq)]
pub(crate) struct ApplyPulledOperation {
    node_id: NodeId,
    /// The realm this node serves, when it runs a net transport.
    realm_id: Option<RealmId>,
    sync: Option<DocumentNetEvent>,
    now_ms: u64,
    ids: IdMint,
}

impl ApplyPulledOperation {
    pub(crate) fn new(node_id: NodeId, realm_id: Option<RealmId>, sync: DocumentNetEvent) -> Self {
        Self {
            node_id,
            realm_id,
            sync: Some(sync),
            now_ms: 0,
            ids: IdMint::default(),
        }
    }
}

impl Operation for ApplyPulledOperation {
    type Output = bool;
    type Error = IoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn seed(&mut self, entropy: u128) {
        self.ids = IdMint::new(entropy);
    }

    fn first(&mut self) -> ApplyTurn {
        match self.sync.take().ok_or(IoError::Unfinished)? {
            DocumentNetEvent::DocumentsReconciled {
                applied,
                targets,
                metadata_create_events,
                metadata_graph_tombstones,
            } => {
                if applied == 0 {
                    return Ok(Next::Done(true));
                }
                let applied = AppliedSync {
                    id: self.ids.mint(self.now_ms),
                    node_id: self.node_id,
                    realm_id: self.realm_id,
                    result: DocumentReconcileResult {
                        targets,
                        metadata_create_events,
                        metadata_graph_tombstones,
                    },
                    next: AppliedStep::Usage,
                };
                match applied_sync_entry(&applied) {
                    Ok(entry) => Ok(Next::Then {
                        output: true,
                        calls: vec![
                            IoCall::Write(vec![entry]),
                            IoCall::from(schedule_applied_drain()),
                        ],
                    }),
                    Err(error) => {
                        warn!(%error, "The shard restore follow-ups could not be queued");
                        Ok(Next::Done(true))
                    }
                }
            }
            DocumentNetEvent::Error { error, .. } => {
                warn!(error = %error, "Failed to sync held shard topics on restart");
                Ok(Next::Done(false))
            }
            other => {
                warn!(event = ?other, "Unexpected restart shard sync result");
                Ok(Next::Done(false))
            }
        }
    }

    fn next(&mut self, _result: IoResult) -> ApplyTurn {
        Err(IoError::Unfinished)
    }
}

#[cfg(test)]
mod tests {
    use aruna_core::calls::ChildOutcome;
    use aruna_core::document::DocumentTarget;
    use aruna_core::events::{Event, NetEvent};

    use super::*;

    fn node() -> NodeId {
        iroh::SecretKey::from_bytes(&[1; 32]).public()
    }

    fn realm() -> RealmId {
        RealmId::from_bytes([7; 32])
    }

    fn topic() -> TopicId {
        TopicId::from_bytes([3; 32])
    }

    fn reconciled(applied: usize) -> DocumentNetEvent {
        DocumentNetEvent::DocumentsReconciled {
            applied,
            targets: vec![DocumentTarget::RealmConfig { realm_id: realm() }],
            metadata_create_events: Vec::new(),
            metadata_graph_tombstones: Vec::new(),
        }
    }

    fn answer(event: ShardNetEvent) -> IoResult {
        let event = Event::Net(NetEvent::Shard(Box::new(event)));
        IoResult::Event(Box::new(event))
    }

    fn child<T, E>(outcome: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(ChildOutcome::new(outcome))
    }

    #[test]
    fn pull_applies_answer() {
        let mut pull = PullTopicsOperation::new(vec![topic()], vec![node()]);
        let call = shard_call(ShardNetEffect::Pull {
            topics: vec![topic()],
            peers: vec![node()],
        });
        assert_eq!(pull.first(), Ok(Next::Call(call)));
        let fetched = DocumentNetEvent::DocumentsSynced {
            topics: vec![topic()],
        };
        let turn = pull.next(answer(ShardNetEvent::Pulled {
            node_id: node(),
            realm_id: realm(),
            sync: Box::new(fetched.clone()),
        }));
        let reconcile = SyncTopicsOperation::after_fetch(realm(), fetched);
        assert_eq!(turn, Ok(Next::Call(IoCall::child(reconcile))));
        let reconciled = DocumentReconcileResult::default();
        let turn = pull.next(child::<_, ApplyError>(Ok(reconciled)));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
        let done = pull.next(child::<bool, IoError>(Ok(true)));
        assert_eq!(done, Ok(Next::Done(true)));
    }

    #[test]
    fn failed_reconcile_unapplied() {
        let mut pull = PullTopicsOperation::new(vec![topic()], vec![node()]);
        pull.stage = PullStage::Reconcile(node(), realm());
        let failed = ApplyError::Failed("down".to_string());
        let turn = pull.next(child::<DocumentReconcileResult, _>(Err(failed)));
        let sync = DocumentNetEvent::Error {
            target: None,
            error: "down".to_string(),
        };
        let Ok(Next::Call(IoCall::Child(_))) = turn else {
            panic!("a failed reconcile still reaches the apply step: {sync:?}");
        };
        let mut apply = ApplyPulledOperation::new(node(), Some(realm()), sync);
        assert_eq!(apply.first(), Ok(Next::Done(false)));
    }

    #[test]
    fn no_network_unapplied() {
        let mut pull = PullTopicsOperation::new(vec![topic()], vec![node()]);
        pull.first().unwrap();
        let done = pull.next(answer(ShardNetEvent::NoNetwork));
        assert_eq!(done, Ok(Next::Done(false)));
    }

    #[test]
    fn nothing_applied_succeeds() {
        let mut apply = ApplyPulledOperation::new(node(), Some(realm()), reconciled(0));
        assert_eq!(apply.first(), Ok(Next::Done(true)));
    }

    #[test]
    fn failed_sync_unapplied() {
        let failed = DocumentNetEvent::Error {
            target: None,
            error: "down".to_string(),
        };
        let mut apply = ApplyPulledOperation::new(node(), Some(realm()), failed);
        assert_eq!(apply.first(), Ok(Next::Done(false)));
    }

    #[test]
    fn applied_queues_followups() {
        let mut apply = ApplyPulledOperation::new(node(), Some(realm()), reconciled(1));
        apply.stamp(1_000);
        apply.seed(7);
        let applied = AppliedSync {
            id: ulid::Ulid::from_parts(1_000, 7),
            node_id: node(),
            realm_id: Some(realm()),
            result: DocumentReconcileResult {
                targets: vec![DocumentTarget::RealmConfig { realm_id: realm() }],
                ..DocumentReconcileResult::default()
            },
            next: AppliedStep::Usage,
        };
        let entry = applied_sync_entry(&applied).unwrap();
        let calls = vec![
            IoCall::Write(vec![entry]),
            IoCall::from(schedule_applied_drain()),
        ];
        let queued = Next::Then {
            output: true,
            calls,
        };
        assert_eq!(apply.first(), Ok(queued));
    }
}
