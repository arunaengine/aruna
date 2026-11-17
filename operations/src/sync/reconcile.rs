//! Applies the stored events of inbound document sync topics and returns what changed.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::{DocumentEffect, DocumentNetEvent, DocumentReconcileResult};
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::events::{Event, NetEvent};
use aruna_core::operation::Operation;
use thiserror::Error;

#[derive(Debug, PartialEq, Error)]
pub enum ReconcileError {
    #[error("document reconcile failed: {0}")]
    Net(String),
    #[error(transparent)]
    Io(#[from] IoError),
}

/// Reconciles the topics a peer stream touched. Net applies the events it holds; the
/// result names the changed targets for the follow-up work of the peer trigger.
#[derive(Debug, PartialEq)]
pub struct ReconcileTopicsOperation {
    topics: Vec<irokle::TopicId>,
}

impl ReconcileTopicsOperation {
    pub fn new(topics: Vec<irokle::TopicId>) -> Self {
        Self { topics }
    }
}

impl Operation for ReconcileTopicsOperation {
    type Output = DocumentReconcileResult;
    type Error = ReconcileError;

    fn first(&mut self) -> Result<Next<DocumentReconcileResult>, ReconcileError> {
        let topics = std::mem::take(&mut self.topics);
        let effect = Effect::Net(NetEffect::DocumentSync(DocumentEffect::ReconcileTopics {
            topics,
        }));
        Ok(Next::Call(IoCall::Effect(Box::new(effect))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<DocumentReconcileResult>, ReconcileError> {
        let IoResult::Event(event) = result else {
            return Err(unexpected(result.label()));
        };
        match *event {
            Event::Net(NetEvent::DocumentSync(DocumentNetEvent::DocumentsReconciled {
                targets,
                metadata_create_events,
                metadata_graph_tombstones,
                ..
            })) => Ok(Next::Done(DocumentReconcileResult {
                targets,
                metadata_create_events,
                metadata_graph_tombstones,
            })),
            Event::Net(NetEvent::DocumentSync(DocumentNetEvent::Error { error, .. })) => {
                Err(ReconcileError::Net(error))
            }
            Event::Net(NetEvent::Error(error)) => Err(ReconcileError::Net(format!("{error:?}"))),
            other => Err(unexpected(other.label())),
        }
    }
}

fn unexpected(got: &'static str) -> ReconcileError {
    ReconcileError::Io(IoError::Unexpected {
        expected: "DocumentNetEvent::DocumentsReconciled",
        got,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::document::DocumentTarget;
    use aruna_core::events::NetError;
    use aruna_core::structs::identity::realm::RealmId;

    fn topics() -> Vec<irokle::TopicId> {
        vec![irokle::TopicId::from_bytes([3; 32])]
    }

    fn net(event: DocumentNetEvent) -> IoResult {
        IoResult::Event(Box::new(Event::Net(NetEvent::DocumentSync(event))))
    }

    fn reconciled(targets: Vec<DocumentTarget>) -> IoResult {
        net(DocumentNetEvent::DocumentsReconciled {
            applied: targets.len(),
            targets,
            metadata_create_events: Vec::new(),
            metadata_graph_tombstones: Vec::new(),
        })
    }

    #[test]
    fn returns_reconciled_targets() {
        let target = DocumentTarget::RealmConfig {
            realm_id: RealmId::from_bytes([1; 32]),
        };
        let mut operation = ReconcileTopicsOperation::new(topics());
        let effect = Effect::Net(NetEffect::DocumentSync(DocumentEffect::ReconcileTopics {
            topics: topics(),
        }));
        assert_eq!(
            operation.first(),
            Ok(Next::Call(IoCall::Effect(Box::new(effect))))
        );
        let Ok(Next::Done(result)) = operation.next(reconciled(vec![target.clone()])) else {
            panic!("reconcile finishes");
        };
        assert_eq!(result.targets, vec![target]);
        assert_eq!(result.applied(), 1);
    }

    #[test]
    fn net_errors_fail() {
        let mut operation = ReconcileTopicsOperation::new(topics());
        let failed = net(DocumentNetEvent::Error {
            target: None,
            error: "topic missing".to_string(),
        });
        assert_eq!(
            operation.next(failed),
            Err(ReconcileError::Net("topic missing".to_string()))
        );
        let closed = IoResult::Event(Box::new(Event::Net(NetEvent::Error(
            NetError::ChannelClosed,
        ))));
        assert!(matches!(
            operation.next(closed),
            Err(ReconcileError::Net(_))
        ));
    }

    #[test]
    fn refuses_other_results() {
        let mut operation = ReconcileTopicsOperation::new(topics());
        assert!(matches!(
            operation.next(IoResult::Done),
            Err(ReconcileError::Io(IoError::Unexpected { .. }))
        ));
        let other = IoResult::Event(Box::new(Event::Delayed));
        assert!(matches!(
            operation.next(other),
            Err(ReconcileError::Io(IoError::Unexpected { .. }))
        ));
    }
}
