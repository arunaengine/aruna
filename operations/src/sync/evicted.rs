//! Writes documents a genesis tie-break evicted back to the sync outbox and arms its drain.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::DocumentEvictedDocument;
use aruna_core::effects::Effect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::task::{TaskEvent, TaskKey};
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

/// Writes one outbox record per evicted document under its evicted event id, so a replay
/// lands once and mints no genesis, then arms the outbox drain when anything was written.
/// A failed write only marks the outcome incomplete.
#[derive(Debug, PartialEq)]
pub struct ReemitEvictedOperation {
    writes: VecDeque<Effect>,
    outcome: ReemitOutcome,
    scheduling: bool,
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
            outcome: ReemitOutcome {
                written: 0,
                complete,
                drain_error: None,
            },
            scheduling: false,
        }
    }

    fn advance(&mut self) -> Next<ReemitOutcome> {
        if let Some(effect) = self.writes.pop_front() {
            return Next::Call(IoCall::Effect(Box::new(effect)));
        }
        if self.outcome.written == 0 {
            return Next::Done(self.outcome.clone());
        }
        self.scheduling = true;
        Next::Call(IoCall::Effect(Box::new(schedule_drain_effect())))
    }
}

impl Operation for ReemitEvictedOperation {
    type Output = ReemitOutcome;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<ReemitOutcome>, IoError> {
        Ok(self.advance())
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ReemitOutcome>, IoError> {
        let IoResult::Event(event) = result else {
            return Err(unexpected(result.label()));
        };
        match (self.scheduling, *event) {
            (false, Event::Storage(StorageEvent::WriteResult { .. })) => {
                self.outcome.written += 1;
                Ok(self.advance())
            }
            (false, Event::Storage(StorageEvent::Error { error })) => {
                warn!(task_id = ?TaskKey::DrainSyncOutbox, error = %error, "Failed to write re-emitted eviction outbox record");
                self.outcome.complete = false;
                Ok(self.advance())
            }
            (true, Event::Task(TaskEvent::TimerScheduled { .. })) => {
                Ok(Next::Done(self.outcome.clone()))
            }
            (true, Event::Task(TaskEvent::Error { message, .. })) => {
                self.outcome.drain_error = Some(message);
                Ok(Next::Done(self.outcome.clone()))
            }
            (_, other) => Err(unexpected(other.label())),
        }
    }
}

fn unexpected(got: &'static str) -> IoError {
    IoError::Unexpected {
        expected: "an outbox write or drain schedule result",
        got,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::document::{
        DocumentChange, DocumentChangeKind, DocumentOutboxEvent, DocumentOutboxRecord,
        DocumentSyncRevision, DocumentTarget,
    };
    use aruna_core::effects::StorageEffect;
    use aruna_core::errors::StorageError;
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

    /// The outbox record of the one unbatched write the operation asked for.
    fn written_record(next: Result<Next<ReemitOutcome>, IoError>) -> DocumentOutboxRecord {
        let Ok(Next::Call(IoCall::Effect(effect))) = next else {
            panic!("expected one outbox write");
        };
        let Effect::Storage(StorageEffect::Write {
            value,
            txn_id: None,
            ..
        }) = *effect
        else {
            panic!("expected one outbox write");
        };
        postcard::from_bytes(&value).expect("outbox record decodes")
    }

    fn event(event: Event) -> IoResult {
        IoResult::Event(Box::new(event))
    }

    fn written() -> IoResult {
        event(Event::Storage(StorageEvent::WriteResult {
            key: Vec::<u8>::new().into(),
        }))
    }

    fn failed() -> IoResult {
        event(Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        }))
    }

    fn scheduled() -> IoResult {
        event(Event::Task(TaskEvent::TimerScheduled {
            key: TaskKey::DrainSyncOutbox,
            after: Duration::ZERO,
        }))
    }

    fn schedule() -> Next<ReemitOutcome> {
        Next::Call(IoCall::Effect(Box::new(schedule_drain_effect())))
    }

    #[test]
    fn writes_then_schedules() {
        let mut operation = ReemitEvictedOperation::new(node(), vec![evicted(1), evicted(2)]);
        let first = written_record(operation.first());
        assert_eq!(first.outbox_id, evicted(1).event_id);
        assert_eq!(first.node_id, node());
        let second = written_record(operation.next(written()));
        assert_eq!(second.outbox_id, evicted(2).event_id);
        assert_eq!(operation.next(written()), Ok(schedule()));
        let outcome = ReemitOutcome {
            written: 2,
            complete: true,
            drain_error: None,
        };
        assert_eq!(operation.next(scheduled()), Ok(Next::Done(outcome)));
    }

    #[test]
    fn failed_write_incomplete() {
        let mut operation = ReemitEvictedOperation::new(node(), vec![evicted(1), evicted(2)]);
        operation.first().unwrap();
        let second = written_record(operation.next(failed()));
        assert_eq!(second.outbox_id, evicted(2).event_id);
        assert_eq!(operation.next(written()), Ok(schedule()));
        let Ok(Next::Done(outcome)) = operation.next(scheduled()) else {
            panic!("re-emit finishes");
        };
        assert_eq!((outcome.written, outcome.complete), (1, false));
    }

    #[test]
    fn empty_skips_drain() {
        let mut operation = ReemitEvictedOperation::new(node(), vec![evicted(1)]);
        operation.first().unwrap();
        let Ok(Next::Done(outcome)) = operation.next(failed()) else {
            panic!("re-emit finishes");
        };
        assert_eq!((outcome.written, outcome.complete), (0, false));
        let mut empty = ReemitEvictedOperation::new(node(), Vec::new());
        let Ok(Next::Done(outcome)) = empty.first() else {
            panic!("empty re-emit finishes");
        };
        assert_eq!(outcome.written, 0);
    }

    #[test]
    fn drain_error_reported() {
        let mut operation = ReemitEvictedOperation::new(node(), vec![evicted(1)]);
        operation.first().unwrap();
        operation.next(written()).unwrap();
        let refused = event(Event::Task(TaskEvent::Error {
            key: Some(TaskKey::DrainSyncOutbox),
            message: "task handle unavailable".to_string(),
        }));
        let Ok(Next::Done(outcome)) = operation.next(refused) else {
            panic!("re-emit finishes");
        };
        assert!(outcome.complete);
        assert_eq!(
            outcome.drain_error.as_deref(),
            Some("task handle unavailable")
        );
    }

    #[test]
    fn refuses_other_results() {
        let mut operation = ReemitEvictedOperation::new(node(), vec![evicted(1)]);
        operation.first().unwrap();
        assert!(matches!(
            operation.next(scheduled()),
            Err(IoError::Unexpected { .. })
        ));
        assert!(matches!(
            operation.next(IoResult::Done),
            Err(IoError::Unexpected { .. })
        ));
        operation.next(written()).unwrap();
        assert!(matches!(
            operation.next(written()),
            Err(IoError::Unexpected { .. })
        ));
    }
}
