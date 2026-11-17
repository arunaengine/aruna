//! Applies the stored events of inbound document sync topics and returns what changed.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::document::{DocumentEffect, DocumentNetEvent, DocumentReconcileResult};
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::events::{Event, NetEvent};
use aruna_core::operation::EffectOperation;
use aruna_core::types::Effects;
use smallvec::smallvec;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ReconcileError {
    #[error("document reconcile failed: {0}")]
    Net(String),
    #[error("unexpected event while reconciling document topics: {0}")]
    UnexpectedEvent(String),
    #[error("document reconcile did not finish")]
    NotFinished,
    #[error("document reconcile was aborted")]
    Aborted,
}

#[derive(Debug, PartialEq)]
enum ReconcileState {
    Ready(Vec<irokle::TopicId>),
    Reconciling,
    Done(Result<DocumentReconcileResult, ReconcileError>),
}

/// Reconciles the topics a peer stream touched. Net applies the events it holds; the
/// result names the changed targets for the follow-up work of the peer trigger.
#[derive(Debug, PartialEq)]
pub struct ReconcileTopicsOperation {
    state: ReconcileState,
}

impl ReconcileTopicsOperation {
    pub fn new(topics: Vec<irokle::TopicId>) -> Self {
        Self {
            state: ReconcileState::Ready(topics),
        }
    }

    fn finish(&mut self, result: Result<DocumentReconcileResult, ReconcileError>) -> Effects {
        self.state = ReconcileState::Done(result);
        Effects::new()
    }
}

impl EffectOperation for ReconcileTopicsOperation {
    type Output = DocumentReconcileResult;
    type Error = ReconcileError;

    fn start(&mut self) -> Effects {
        let ReconcileState::Ready(topics) = &mut self.state else {
            return Effects::new();
        };
        let topics = std::mem::take(topics);
        self.state = ReconcileState::Reconciling;
        smallvec![Effect::Net(NetEffect::DocumentSync(
            DocumentEffect::ReconcileTopics { topics }
        ))]
    }

    fn step(&mut self, event: Event) -> Effects {
        if !matches!(self.state, ReconcileState::Reconciling) {
            return self.finish(Err(ReconcileError::UnexpectedEvent(format!("{event:?}"))));
        }
        match event {
            Event::Net(NetEvent::DocumentSync(DocumentNetEvent::DocumentsReconciled {
                targets,
                metadata_create_events,
                metadata_graph_tombstones,
                ..
            })) => self.finish(Ok(DocumentReconcileResult {
                targets,
                metadata_create_events,
                metadata_graph_tombstones,
            })),
            Event::Net(NetEvent::DocumentSync(DocumentNetEvent::Error { error, .. })) => {
                self.finish(Err(ReconcileError::Net(error)))
            }
            Event::Net(NetEvent::Error(error)) => {
                self.finish(Err(ReconcileError::Net(format!("{error:?}"))))
            }
            other => self.finish(Err(ReconcileError::UnexpectedEvent(format!("{other:?}")))),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, ReconcileState::Done(_))
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.state {
            ReconcileState::Done(result) => result,
            _ => Err(ReconcileError::NotFinished),
        }
    }

    fn abort(&mut self) -> Effects {
        // Net applies each topic durably on its own, so nothing needs undoing.
        self.finish(Err(ReconcileError::Aborted))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::document::DocumentTarget;
    use aruna_core::events::NetError;
    use aruna_core::operation_check::{
        check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
    };
    use aruna_core::structs::identity::realm::RealmId;

    fn topics() -> Vec<irokle::TopicId> {
        vec![irokle::TopicId::from_bytes([3; 32])]
    }

    fn running() -> ReconcileTopicsOperation {
        let mut operation = ReconcileTopicsOperation::new(topics());
        operation.start();
        operation
    }

    fn reconciled(targets: Vec<DocumentTarget>) -> Event {
        Event::Net(NetEvent::DocumentSync(
            DocumentNetEvent::DocumentsReconciled {
                applied: targets.len(),
                targets,
                metadata_create_events: Vec::new(),
                metadata_graph_tombstones: Vec::new(),
            },
        ))
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
        check_trace(
            &mut operation,
            &[effect],
            vec![(reconciled(vec![target.clone()]), vec![])],
        );
        let result = operation.finalize().expect("reconcile finishes");
        assert_eq!(result.targets, vec![target]);
        assert_eq!(result.applied(), 1);
    }

    #[test]
    fn net_errors_fail() {
        let mut operation = running();
        operation.step(Event::Net(NetEvent::DocumentSync(
            DocumentNetEvent::Error {
                target: None,
                error: "topic missing".to_string(),
            },
        )));
        assert_eq!(
            operation.finalize(),
            Err(ReconcileError::Net("topic missing".to_string()))
        );
        let mut closed = running();
        closed.step(Event::Net(NetEvent::Error(NetError::ChannelClosed)));
        assert!(matches!(closed.finalize(), Err(ReconcileError::Net(_))));
    }

    #[test]
    fn rejects_foreign_events() {
        let refusals = rejects_events(running, sample_events, |_| false);
        assert_eq!(refusals.len(), sample_events().len());
        for (label, effects, error) in refusals {
            assert!(effects.is_empty(), "{label} emitted effects");
            assert!(
                matches!(error, ReconcileError::UnexpectedEvent(_)),
                "{label}"
            );
        }
        let mut early = ReconcileTopicsOperation::new(topics());
        early.step(reconciled(Vec::new()));
        assert!(matches!(
            early.finalize(),
            Err(ReconcileError::UnexpectedEvent(_))
        ));
        let mut late = running();
        late.step(reconciled(Vec::new()));
        late.step(reconciled(Vec::new()));
        assert!(matches!(
            late.finalize(),
            Err(ReconcileError::UnexpectedEvent(_))
        ));
    }

    #[test]
    fn abort_and_finalize() {
        rejects_finalize(ReconcileTopicsOperation::new(topics()));
        rejects_finalize(running());
        let mut operation = running();
        check_abort(&mut operation, &[]);
        assert_eq!(operation.finalize(), Err(ReconcileError::Aborted));
    }
}
