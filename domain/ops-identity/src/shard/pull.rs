//! Pulls shard topics from peers and applies what arrived to this node's local views.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::{DocumentNetEvent, DocumentReconcileResult, schedule_applied_drain};
use aruna_core::document_apply::ApplyError;
use aruna_core::effects::ShardNetEffect;
use aruna_core::events::ShardNetEvent;
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use irokle::TopicId;
use tracing::warn;

use crate::shard::{shard_answer, shard_call};
use crate::sync::reconcile::SyncTopicsOperation;

#[derive(Debug, PartialEq)]
enum PullStage {
    Fetch,
    Reconcile,
    Apply,
}

/// Fetches `topics` from `peers`, reconciles them and answers whether what arrived was applied.
#[derive(Debug, PartialEq)]
pub struct PullTopicsOperation {
    topics: Vec<TopicId>,
    peers: Vec<NodeId>,
    stage: PullStage,
}

impl PullTopicsOperation {
    pub fn new(topics: Vec<TopicId>, peers: Vec<NodeId>) -> Self {
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
                    self.stage = PullStage::Reconcile;
                    let reconcile = SyncTopicsOperation::after_fetch(realm_id, *sync, node_id);
                    Ok(Next::Call(IoCall::child(reconcile)))
                }
                _ => Ok(Next::Done(false)),
            },
            PullStage::Reconcile => {
                let reconciled = result.child::<DocumentReconcileResult, ApplyError>()?;
                let sync = DocumentNetEvent::reconciled(reconciled);
                let apply = ApplyPulledOperation::new(sync);
                Ok(Next::Call(IoCall::child(apply)))
            }
            PullStage::Apply => Ok(Next::Done(result.child::<bool, IoError>()??)),
        }
    }
}

type ApplyTurn = Result<Next<bool>, IoError>;

/// Arms the node drain of the usage, watch interest, mail settings and metadata follow-ups that
/// the reconcile stored with what it applied, and answers false when the reconcile failed. A
/// failed reconcile may have stored follow-ups before it stopped, so it arms the drain too.
#[derive(Debug, PartialEq)]
pub struct ApplyPulledOperation {
    sync: Option<DocumentNetEvent>,
}

impl ApplyPulledOperation {
    pub fn new(sync: DocumentNetEvent) -> Self {
        Self { sync: Some(sync) }
    }
}

impl Operation for ApplyPulledOperation {
    type Output = bool;
    type Error = IoError;

    fn first(&mut self) -> ApplyTurn {
        let applied = match self.sync.take().ok_or(IoError::Unfinished)? {
            DocumentNetEvent::DocumentsReconciled { .. } => true,
            DocumentNetEvent::Error { error, .. } => {
                warn!(error = %error, "Failed to sync held shard topics on restart");
                false
            }
            other => {
                warn!(event = ?other, "Unexpected restart shard sync result");
                false
            }
        };
        Ok(Next::Then {
            output: applied,
            calls: vec![IoCall::from(schedule_applied_drain())],
        })
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
    use aruna_core::structs::identity::realm::RealmId;

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
        let reconcile = SyncTopicsOperation::after_fetch(realm(), fetched, node());
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
        pull.stage = PullStage::Reconcile;
        let failed = ApplyError::Failed("down".to_string());
        let turn = pull.next(child::<DocumentReconcileResult, _>(Err(failed)));
        let sync = DocumentNetEvent::Error {
            target: None,
            error: "down".to_string(),
        };
        let Ok(Next::Call(IoCall::Child(_))) = turn else {
            panic!("a failed reconcile still reaches the apply step: {sync:?}");
        };
        let mut apply = ApplyPulledOperation::new(sync);
        assert_eq!(apply.first(), Ok(armed(false)));
    }

    #[test]
    fn no_network_unapplied() {
        let mut pull = PullTopicsOperation::new(vec![topic()], vec![node()]);
        pull.first().unwrap();
        let done = pull.next(answer(ShardNetEvent::NoNetwork));
        assert_eq!(done, Ok(Next::Done(false)));
    }

    // The reconcile stored its follow-ups with what it applied, so the pull only arms the drain,
    // after a failed reconcile too.
    fn armed(applied: bool) -> Next<bool> {
        Next::Then {
            output: applied,
            calls: vec![IoCall::from(schedule_applied_drain())],
        }
    }

    #[test]
    fn nothing_applied_succeeds() {
        let mut apply = ApplyPulledOperation::new(reconciled(0));
        assert_eq!(apply.first(), Ok(armed(true)));
    }

    #[test]
    fn failed_sync_unapplied() {
        let failed = DocumentNetEvent::Error {
            target: None,
            error: "down".to_string(),
        };
        let mut apply = ApplyPulledOperation::new(failed);
        assert_eq!(apply.first(), Ok(armed(false)));
    }

    #[test]
    fn applied_arms_drain() {
        let mut apply = ApplyPulledOperation::new(reconciled(1));
        assert_eq!(apply.first(), Ok(armed(true)));
    }
}
