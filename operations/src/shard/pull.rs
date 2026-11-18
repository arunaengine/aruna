//! Pulls shard topics from peers and applies what arrived to this node's local views.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::{DocumentNetEvent, DocumentReconcileResult};
use aruna_core::effects::ShardNetEffect;
use aruna_core::events::ShardNetEvent;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use irokle::TopicId;
use tracing::warn;
use ulid::Ulid;

use crate::metadata::after_sync::{AfterSyncOperation, MetadataTimings};
use crate::node::summary_refresh::{RefreshUsageError, RefreshUsageOperation};
use crate::notifications::email::RefreshMailOperation;
use crate::notifications::watch::interest::RefreshInterestOperation;
use crate::shard::{shard_answer, shard_call};

/// Pulls `topics` from `peers` and answers whether what arrived was applied.
#[derive(Debug, PartialEq)]
pub(crate) struct PullTopicsOperation {
    topics: Vec<TopicId>,
    peers: Vec<NodeId>,
    applying: bool,
}

impl PullTopicsOperation {
    pub(crate) fn new(topics: Vec<TopicId>, peers: Vec<NodeId>) -> Self {
        Self {
            topics,
            peers,
            applying: false,
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
        if self.applying {
            return Ok(Next::Done(result.child::<bool, IoError>()??));
        }
        match shard_answer(result)? {
            ShardNetEvent::Pulled {
                node_id,
                realm_id,
                sync,
            } => {
                self.applying = true;
                let apply = ApplyPulledOperation::new(node_id, Some(realm_id), *sync);
                Ok(Next::Call(IoCall::child(apply)))
            }
            _ => Ok(Next::Done(false)),
        }
    }
}

#[derive(Debug, PartialEq)]
enum ApplyStage {
    Start,
    Usage,
    Interest,
    Mail,
    Metadata,
}

type ApplyTurn = Result<Next<bool>, IoError>;

/// Brings usage, watch interest, mail settings and metadata in line with a document reconcile.
/// Answers false when the reconcile itself failed. A failed follow-up only warns: each has its
/// own retry, and the applied documents stay applied.
#[derive(Debug, PartialEq)]
pub(crate) struct ApplyPulledOperation {
    node_id: NodeId,
    /// The realm this node serves, when it runs a net transport.
    realm_id: Option<RealmId>,
    sync: Option<DocumentNetEvent>,
    generation: Ulid,
    result: DocumentReconcileResult,
    stage: ApplyStage,
}

impl ApplyPulledOperation {
    pub(crate) fn new(node_id: NodeId, realm_id: Option<RealmId>, sync: DocumentNetEvent) -> Self {
        Self {
            node_id,
            realm_id,
            sync: Some(sync),
            generation: Ulid::generate(),
            result: DocumentReconcileResult::default(),
            stage: ApplyStage::Start,
        }
    }

    fn mail(&mut self) -> ApplyTurn {
        self.stage = ApplyStage::Mail;
        let refresh = RefreshMailOperation::applied(&self.result.targets);
        Ok(Next::Call(IoCall::child(refresh)))
    }
}

impl Operation for ApplyPulledOperation {
    type Output = bool;
    type Error = IoError;

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
                self.result = DocumentReconcileResult {
                    targets,
                    metadata_create_events,
                    metadata_graph_tombstones,
                };
                self.stage = ApplyStage::Usage;
                let refresh = RefreshUsageOperation::new(
                    self.node_id,
                    self.realm_id,
                    &self.result.targets,
                    self.generation,
                );
                Ok(Next::Call(IoCall::child(refresh)))
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

    fn next(&mut self, result: IoResult) -> ApplyTurn {
        match self.stage {
            ApplyStage::Start => Err(IoError::Unfinished),
            ApplyStage::Usage => {
                if let Err(error) = result.child::<(), RefreshUsageError>()? {
                    warn!(%error, "Realm usage refresh stopped after the shard restore");
                }
                let Some(realm_id) = self.realm_id else {
                    return self.mail();
                };
                self.stage = ApplyStage::Interest;
                let refresh = RefreshInterestOperation::new(&self.result.targets, realm_id);
                Ok(Next::Call(IoCall::child(refresh)))
            }
            ApplyStage::Interest => {
                if let Err(error) = result.child::<(), IoError>()? {
                    warn!(%error, "Watch interest refresh stopped after the shard restore");
                }
                self.mail()
            }
            ApplyStage::Mail => {
                if let Err(error) = result.child::<(), IoError>()? {
                    warn!(%error, "Mail settings refresh stopped after the shard restore");
                }
                self.stage = ApplyStage::Metadata;
                let applied = std::mem::take(&mut self.result);
                let after = AfterSyncOperation::new(Some(self.node_id), applied, false);
                Ok(Next::Call(IoCall::child(after)))
            }
            ApplyStage::Metadata => {
                if let Err(error) = result.child::<MetadataTimings, IoError>()? {
                    warn!(%error, "Metadata follow-up stopped after the shard restore");
                }
                Ok(Next::Done(true))
            }
        }
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
        let turn = pull.next(answer(ShardNetEvent::Pulled {
            node_id: node(),
            realm_id: realm(),
            sync: Box::new(reconciled(0)),
        }));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
        let done = pull.next(child::<bool, IoError>(Ok(true)));
        assert_eq!(done, Ok(Next::Done(true)));
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
    fn followups_run_ordered() {
        let mut apply = ApplyPulledOperation::new(node(), Some(realm()), reconciled(1));
        let targets = vec![DocumentTarget::RealmConfig { realm_id: realm() }];
        let usage = RefreshUsageOperation::new(node(), Some(realm()), &targets, apply.generation);
        assert_eq!(apply.first(), Ok(Next::Call(IoCall::child(usage))));
        // A failed refresh does not stop the ones after it.
        let failed = RefreshUsageError::Io(IoError::Aborted);
        let interest = RefreshInterestOperation::new(&targets, realm());
        let turn = apply.next(child::<(), _>(Err(failed)));
        assert_eq!(turn, Ok(Next::Call(IoCall::child(interest))));
        let mail = RefreshMailOperation::applied(&targets);
        let turn = apply.next(child::<(), IoError>(Ok(())));
        assert_eq!(turn, Ok(Next::Call(IoCall::child(mail))));
        let result = DocumentReconcileResult {
            targets,
            ..DocumentReconcileResult::default()
        };
        let after = AfterSyncOperation::new(Some(node()), result, false);
        let turn = apply.next(child::<(), IoError>(Ok(())));
        assert_eq!(turn, Ok(Next::Call(IoCall::child(after))));
        let timings = MetadataTimings::default();
        let done = apply.next(child::<_, IoError>(Ok(timings)));
        assert_eq!(done, Ok(Next::Done(true)));
    }

    #[test]
    fn realmless_skips_interest() {
        let mut apply = ApplyPulledOperation::new(node(), None, reconciled(1));
        apply.first().unwrap();
        let targets = vec![DocumentTarget::RealmConfig { realm_id: realm() }];
        let mail = RefreshMailOperation::applied(&targets);
        let turn = apply.next(child::<(), RefreshUsageError>(Ok(())));
        assert_eq!(turn, Ok(Next::Call(IoCall::child(mail))));
    }
}
