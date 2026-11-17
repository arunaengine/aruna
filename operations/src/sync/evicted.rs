//! Writes documents a genesis tie-break evicted back to the sync outbox and arms its drain.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;

use aruna_core::NodeId;
use aruna_core::document::DocumentEvictedDocument;
use aruna_core::effects::Effect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::task::{TaskEvent, TaskKey};
use aruna_core::types::Effects;
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;

use crate::sync::outbox_record::{
    new_identified_record, schedule_drain_effect, write_outbox_effect,
};

/// What a re-emit wrote. `complete` is false when a record could not be encoded or written,
/// so the caller keeps its eviction journal; a failed drain schedule leaves it true.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReemitOutcome {
    pub written: usize,
    pub complete: bool,
    pub drain_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ReemitError {
    #[error("unexpected event while re-emitting evicted documents: {0}")]
    UnexpectedEvent(String),
    #[error("re-emit of evicted documents did not finish")]
    NotFinished,
    #[error("re-emit of evicted documents was aborted")]
    Aborted,
}

#[derive(Debug, PartialEq)]
enum ReemitState {
    Ready,
    Writing,
    Scheduling,
    Done(Result<(), ReemitError>),
}

/// Writes one outbox record per evicted document under its evicted event id, so a replay
/// lands once and mints no genesis, then arms the outbox drain when anything was written.
#[derive(Debug, PartialEq)]
pub struct ReemitEvictedOperation {
    writes: VecDeque<Effect>,
    written: usize,
    complete: bool,
    drain_error: Option<String>,
    state: ReemitState,
}

impl ReemitEvictedOperation {
    pub fn new(node_id: NodeId, documents: Vec<DocumentEvictedDocument>) -> Self {
        let mut writes = VecDeque::with_capacity(documents.len());
        let mut complete = true;
        for document in documents {
            let record = new_identified_record(
                document.event_id,
                node_id,
                document.target,
                Vec::new(),
                document.event,
                document.placement,
                document.allow_genesis,
            );
            match write_outbox_effect(&record) {
                Ok(effect) => writes.push_back(effect),
                Err(error) => {
                    warn!(task_id = ?TaskKey::DrainSyncOutbox, error = %error, "Failed to encode re-emitted eviction outbox record");
                    complete = false;
                }
            }
        }
        Self {
            writes,
            written: 0,
            complete,
            drain_error: None,
            state: ReemitState::Ready,
        }
    }

    fn next_write(&mut self) -> Effects {
        if let Some(effect) = self.writes.pop_front() {
            self.state = ReemitState::Writing;
            return smallvec![effect];
        }
        if self.written == 0 {
            self.state = ReemitState::Done(Ok(()));
            return Effects::new();
        }
        self.state = ReemitState::Scheduling;
        smallvec![schedule_drain_effect()]
    }

    fn reject(&mut self, event: &Event) -> Effects {
        self.state = ReemitState::Done(Err(ReemitError::UnexpectedEvent(format!("{event:?}"))));
        Effects::new()
    }
}

impl Operation for ReemitEvictedOperation {
    type Output = ReemitOutcome;
    type Error = ReemitError;

    fn start(&mut self) -> Effects {
        match self.state {
            ReemitState::Ready => self.next_write(),
            _ => Effects::new(),
        }
    }

    fn step(&mut self, event: Event) -> Effects {
        match (&self.state, &event) {
            (ReemitState::Writing, Event::Storage(StorageEvent::WriteResult { .. })) => {
                self.written += 1;
                self.next_write()
            }
            (ReemitState::Writing, Event::Storage(StorageEvent::Error { error })) => {
                warn!(task_id = ?TaskKey::DrainSyncOutbox, error = %error, "Failed to write re-emitted eviction outbox record");
                self.complete = false;
                self.next_write()
            }
            (ReemitState::Scheduling, Event::Task(TaskEvent::TimerScheduled { .. })) => {
                self.state = ReemitState::Done(Ok(()));
                Effects::new()
            }
            (ReemitState::Scheduling, Event::Task(TaskEvent::Error { message, .. })) => {
                self.drain_error = Some(message.clone());
                self.state = ReemitState::Done(Ok(()));
                Effects::new()
            }
            _ => self.reject(&event),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, ReemitState::Done(_))
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.state {
            ReemitState::Done(Ok(())) => Ok(ReemitOutcome {
                written: self.written,
                complete: self.complete,
                drain_error: self.drain_error,
            }),
            ReemitState::Done(Err(error)) => Err(error),
            _ => Err(ReemitError::NotFinished),
        }
    }

    fn abort(&mut self) -> Effects {
        // Each write stands alone, so nothing needs undoing.
        self.state = ReemitState::Done(Err(ReemitError::Aborted));
        Effects::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::document::{
        DocumentChange, DocumentChangeKind, DocumentOutboxEvent, DocumentSyncRevision,
        DocumentTarget,
    };
    use aruna_core::errors::StorageError;
    use aruna_core::operation_check::{
        check_abort, rejects_events, rejects_finalize, sample_events,
    };
    use aruna_core::structs::placement::record::PlacementRef;
    use std::time::Duration;
    use ulid::Ulid;

    fn node() -> NodeId {
        iroh::SecretKey::from_bytes(&[9; 32]).public()
    }

    fn evicted(seed: u8) -> DocumentEvictedDocument {
        let event_id = Ulid::from_parts(u64::from(seed), u128::from(seed));
        let change = DocumentChange {
            base: None,
            current: DocumentSyncRevision {
                generation: 1,
                event_id,
                actor: node(),
                updated_at_ms: u64::from(seed),
            },
            kind: DocumentChangeKind::Delete,
            placement: PlacementRef::NIL,
        };
        DocumentEvictedDocument {
            event_id,
            target: DocumentTarget::MetadataDocumentLifecycle {
                document_id: event_id,
            },
            event: DocumentOutboxEvent::Delete { change },
            placement: PlacementRef::NIL,
            allow_genesis: false,
        }
    }

    /// The outbox record of the one unbatched write in `effects`.
    fn written_record(effects: &[Effect]) -> aruna_core::document::DocumentOutboxRecord {
        let [
            Effect::Storage(aruna_core::effects::StorageEffect::Write {
                value,
                txn_id: None,
                ..
            }),
        ] = effects
        else {
            panic!("expected one outbox write, got {effects:?}");
        };
        postcard::from_bytes(value).expect("outbox record decodes")
    }

    fn written() -> Event {
        Event::Storage(StorageEvent::WriteResult {
            key: Vec::<u8>::new().into(),
        })
    }

    fn scheduled() -> Event {
        Event::Task(TaskEvent::TimerScheduled {
            key: TaskKey::DrainSyncOutbox,
            after: Duration::ZERO,
        })
    }

    fn writing() -> ReemitEvictedOperation {
        let mut operation = ReemitEvictedOperation::new(node(), vec![evicted(1)]);
        operation.start();
        operation
    }

    #[test]
    fn writes_then_schedules() {
        let mut operation = ReemitEvictedOperation::new(node(), vec![evicted(1), evicted(2)]);
        let first = written_record(&operation.start());
        assert_eq!(first.outbox_id, evicted(1).event_id);
        let second = written_record(&operation.step(written()));
        assert_eq!(second.outbox_id, evicted(2).event_id);
        assert_eq!(
            operation.step(written()).as_slice(),
            &[schedule_drain_effect()]
        );
        assert!(operation.step(scheduled()).is_empty());
        let outcome = operation.finalize().expect("re-emit finishes");
        assert_eq!(
            outcome,
            ReemitOutcome {
                written: 2,
                complete: true,
                drain_error: None,
            }
        );
    }

    #[test]
    fn records_keep_ids() {
        let mut operation = ReemitEvictedOperation::new(node(), vec![evicted(3)]);
        let record = written_record(&operation.start());
        assert_eq!(record.outbox_id, evicted(3).event_id);
        assert_eq!(record.node_id, node());
    }

    #[test]
    fn failed_write_incomplete() {
        let mut operation = ReemitEvictedOperation::new(node(), vec![evicted(1), evicted(2)]);
        let failed = Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        });
        operation.start();
        let second = written_record(&operation.step(failed));
        assert_eq!(second.outbox_id, evicted(2).event_id);
        assert_eq!(
            operation.step(written()).as_slice(),
            &[schedule_drain_effect()]
        );
        operation.step(scheduled());
        let outcome = operation.finalize().expect("re-emit finishes");
        assert_eq!((outcome.written, outcome.complete), (1, false));
    }

    #[test]
    fn empty_skips_drain() {
        let mut operation = writing();
        let failed = Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        });
        assert!(operation.step(failed).is_empty());
        assert!(operation.is_complete());
        let outcome = operation.finalize().expect("re-emit finishes");
        assert_eq!((outcome.written, outcome.complete), (0, false));

        let mut empty = ReemitEvictedOperation::new(node(), Vec::new());
        assert!(empty.start().is_empty());
        assert_eq!(empty.finalize().expect("empty re-emit").written, 0);
    }

    #[test]
    fn drain_error_reported() {
        let mut operation = writing();
        operation.step(written());
        operation.step(Event::Task(TaskEvent::Error {
            key: Some(TaskKey::DrainSyncOutbox),
            message: "task handle unavailable".to_string(),
        }));
        let outcome = operation.finalize().expect("re-emit finishes");
        assert!(outcome.complete);
        assert_eq!(
            outcome.drain_error.as_deref(),
            Some("task handle unavailable")
        );
    }

    #[test]
    fn rejects_foreign_events() {
        let refusals = rejects_events(writing, sample_events, |event| {
            matches!(
                event,
                Event::Storage(StorageEvent::WriteResult { .. } | StorageEvent::Error { .. })
            )
        });
        assert!(!refusals.is_empty());
        for (label, effects, error) in refusals {
            assert!(effects.is_empty(), "{label} emitted effects");
            assert!(matches!(error, ReemitError::UnexpectedEvent(_)), "{label}");
        }
        let scheduling = || {
            let mut operation = writing();
            operation.step(written());
            operation
        };
        let refusals = rejects_events(scheduling, sample_events, |event| {
            matches!(event, Event::Task(TaskEvent::Error { .. }))
        });
        assert!(!refusals.is_empty());
        let mut early = ReemitEvictedOperation::new(node(), vec![evicted(1)]);
        early.step(written());
        assert!(matches!(
            early.finalize(),
            Err(ReemitError::UnexpectedEvent(_))
        ));
    }

    #[test]
    fn abort_and_finalize() {
        rejects_finalize(writing());
        let mut operation = writing();
        check_abort(&mut operation, &[]);
        assert_eq!(operation.finalize(), Err(ReemitError::Aborted));
        let mut scheduling = writing();
        scheduling.step(written());
        check_abort(&mut scheduling, &[]);
        assert_eq!(scheduling.finalize(), Err(ReemitError::Aborted));
    }
}
