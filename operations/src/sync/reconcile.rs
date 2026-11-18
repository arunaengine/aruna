//! Syncs document topics with their peers and reconciles what arrived.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::{DocumentEffect, DocumentNetEvent, DocumentReconcileResult};
use aruna_core::document_apply::ApplyError;
use aruna_core::document_apply::reconcile::ReconcileTopicsOperation;
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::events::{Event, NetEvent};
use aruna_core::operation::Operation;
use aruna_core::rows::event;
use aruna_core::structs::identity::realm::RealmId;

/// Pulls `topics` from `peers` through net, then runs one reconcile pass over them.
#[derive(Debug, PartialEq)]
pub struct SyncTopicsOperation {
    realm_id: RealmId,
    effect: Option<DocumentEffect>,
    reconciling: bool,
}

impl SyncTopicsOperation {
    /// Syncs the topics this node holds or can join; an unknown one without a peer is skipped.
    pub fn new(realm_id: RealmId, topics: Vec<irokle::TopicId>, peers: Vec<NodeId>) -> Self {
        Self::with(realm_id, DocumentEffect::SyncDocuments { topics, peers })
    }

    /// Syncs one topic and fails when it cannot be fetched.
    pub fn single(realm_id: RealmId, topic: irokle::TopicId, peers: Vec<NodeId>) -> Self {
        Self::with(realm_id, DocumentEffect::SyncDocument { topic, peers })
    }

    fn with(realm_id: RealmId, effect: DocumentEffect) -> Self {
        Self {
            realm_id,
            effect: Some(effect),
            reconciling: false,
        }
    }
}

impl Operation for SyncTopicsOperation {
    type Output = DocumentReconcileResult;
    type Error = ApplyError;

    fn first(&mut self) -> Result<Next<DocumentReconcileResult>, ApplyError> {
        let effect = self.effect.take().ok_or(IoError::Unfinished)?;
        let effect = Effect::Net(NetEffect::DocumentSync(effect));
        Ok(Next::Call(IoCall::Effect(Box::new(effect))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<DocumentReconcileResult>, ApplyError> {
        if self.reconciling {
            let reconciled = result.child::<DocumentReconcileResult, ApplyError>()??;
            return Ok(Next::Done(reconciled));
        }
        match event(result)? {
            Event::Net(NetEvent::DocumentSync(DocumentNetEvent::DocumentsSynced { topics })) => {
                self.reconciling = true;
                let reconcile = ReconcileTopicsOperation::new(self.realm_id, topics);
                Ok(Next::Call(IoCall::child(reconcile)))
            }
            Event::Net(NetEvent::DocumentSync(DocumentNetEvent::Error { error, .. })) => {
                Err(ApplyError::Failed(error))
            }
            Event::Net(NetEvent::Error(error)) => Err(ApplyError::Failed(format!("{error:?}"))),
            other => Err(IoError::Unexpected {
                expected: "DocumentNetEvent::DocumentsSynced",
                got: other.label(),
            }
            .into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::calls::ChildOutcome;

    fn realm() -> RealmId {
        RealmId::from_bytes([1; 32])
    }

    fn topic() -> irokle::TopicId {
        irokle::TopicId::from_bytes([3; 32])
    }

    fn net(event: DocumentNetEvent) -> IoResult {
        IoResult::Event(Box::new(Event::Net(NetEvent::DocumentSync(event))))
    }

    #[test]
    fn syncs_then_reconciles() {
        let mut operation = SyncTopicsOperation::new(realm(), vec![topic()], Vec::new());
        let effect = Effect::Net(NetEffect::DocumentSync(DocumentEffect::SyncDocuments {
            topics: vec![topic()],
            peers: Vec::new(),
        }));
        assert_eq!(
            operation.first(),
            Ok(Next::Call(IoCall::Effect(Box::new(effect))))
        );
        let synced = net(DocumentNetEvent::DocumentsSynced {
            topics: vec![topic()],
        });
        assert!(matches!(
            operation.next(synced),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let done = DocumentReconcileResult::default();
        let answer = IoResult::Child(ChildOutcome::new(Ok::<_, ApplyError>(done.clone())));
        assert_eq!(operation.next(answer), Ok(Next::Done(done)));
    }

    #[test]
    fn reports_sync_failure() {
        let mut operation = SyncTopicsOperation::single(realm(), topic(), Vec::new());
        operation.first().unwrap();
        let failed = net(DocumentNetEvent::Error {
            target: None,
            error: "no peer".to_string(),
        });
        assert_eq!(
            operation.next(failed),
            Err(ApplyError::Failed("no peer".to_string()))
        );
    }

    #[test]
    fn refuses_other_results() {
        let mut operation = SyncTopicsOperation::single(realm(), topic(), Vec::new());
        operation.first().unwrap();
        assert!(matches!(
            operation.next(IoResult::Done),
            Err(ApplyError::Failed(_))
        ));
    }
}
