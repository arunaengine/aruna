//! Emits the watch events producers queued as rows, oldest first, and deletes each row once its
//! emission settled; an emission that may pass later keeps its row for a rerun.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::WATCH_OUTBOX_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::notification_watch::WatchEvent;
use aruna_core::types::Key;
use tracing::warn;

use super::origin::EmitEventOperation;

/// Rows one page of the drain reads at most.
pub(crate) const WATCH_DRAIN_PAGE: usize = 64;
/// When the drain runs again after an emission that did not settle; its row waits until then.
pub const WATCH_RETRY_AFTER: Duration = Duration::from_secs(30);

type Turn = Result<Next<Option<Duration>>, IoError>;

#[derive(Clone, Debug, PartialEq)]
enum DrainStep {
    Page,
    Emit,
    Delete,
}

/// One drain run over every queued event, page by page: a settled emission deletes its row, one
/// that may pass later keeps it for a rerun with the same event id, and an unreadable row goes.
#[derive(Clone, Debug, PartialEq)]
pub struct WatchDrainOperation {
    step: DrainStep,
    pending: VecDeque<(Key, Option<WatchEvent>)>,
    current: Option<Key>,
    after: Option<Key>,
    unsettled: bool,
}

impl Default for WatchDrainOperation {
    fn default() -> Self {
        Self::new()
    }
}

impl WatchDrainOperation {
    pub fn new() -> Self {
        Self {
            step: DrainStep::Page,
            pending: VecDeque::new(),
            current: None,
            after: None,
            unsettled: false,
        }
    }

    fn read_page(&mut self) -> Turn {
        self.step = DrainStep::Page;
        Ok(Next::Call(IoCall::Page {
            space: WATCH_OUTBOX_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after: self.after.clone(),
            limit: WATCH_DRAIN_PAGE,
        }))
    }

    fn next_row(&mut self) -> Turn {
        let Some((key, event)) = self.pending.pop_front() else {
            return match self.after {
                Some(_) => self.read_page(),
                None => Ok(Next::Done(self.unsettled.then_some(WATCH_RETRY_AFTER))),
            };
        };
        self.current = Some(key);
        match event {
            Some(event) => {
                self.step = DrainStep::Emit;
                Ok(Next::Call(IoCall::child(EmitEventOperation::new(event))))
            }
            None => self.delete(),
        }
    }

    fn delete(&mut self) -> Turn {
        let key = self.current.take().ok_or(IoError::Unfinished)?;
        self.step = DrainStep::Delete;
        let row = (WATCH_OUTBOX_KEYSPACE.to_string(), key);
        Ok(Next::Call(IoCall::Delete(vec![row])))
    }
}

impl Operation for WatchDrainOperation {
    type Output = Option<Duration>;
    type Error = IoError;

    fn first(&mut self) -> Turn {
        self.read_page()
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match self.step {
            DrainStep::Page => {
                let (rows, next) = result.page()?;
                // Rows kept for a rerun are passed: the next page starts after them.
                self.after = next;
                for (key, value) in rows {
                    let event = WatchEvent::from_bytes(value.as_ref())
                        .inspect_err(|error| warn!(%error, "Deleting an unreadable watch row"))
                        .ok();
                    self.pending.push_back((key, event));
                }
                self.next_row()
            }
            DrainStep::Emit => match result.child::<bool, IoError>()? {
                Ok(true) => self.delete(),
                outcome => {
                    if let Err(error) = outcome {
                        warn!(%error, "A queued watch event could not be emitted");
                    }
                    self.unsettled = true;
                    self.current = None;
                    self.next_row()
                }
            },
            DrainStep::Delete => {
                result.done()?;
                self.next_row()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::execution::notification_watch::watch_event_entry;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::types::Value;
    use aruna_core::user::id::UserId;
    use ulid::Ulid;

    fn event() -> WatchEvent {
        let realm = RealmId::from_bytes([3; 32]);
        let user = UserId::local(Ulid::from_bytes([4; 16]), realm);
        let group = Ulid::from_bytes([5; 16]);
        let document = Ulid::from_bytes([6; 16]);
        WatchEvent::metadata_created(realm, user, group, document, "d", Ulid::nil())
    }

    fn page(rows: Vec<(Key, Value)>, next: Option<Key>) -> IoResult {
        IoResult::Page { rows, next }
    }

    // Each queued event is emitted, then its row goes; an empty keyspace ends the run.
    #[test]
    fn emits_then_deletes() {
        let (_, key, value) = watch_event_entry(&event()).unwrap();
        let mut drain = WatchDrainOperation::new();
        assert!(matches!(drain.first(), Ok(Next::Call(IoCall::Page { .. }))));
        let emit = drain.next(page(vec![(key.clone(), value)], None));
        assert!(matches!(emit, Ok(Next::Call(IoCall::Child(_)))));
        let emitted = IoResult::Child(ChildOutcome::new(Ok::<bool, IoError>(true)));
        let row = (WATCH_OUTBOX_KEYSPACE.to_string(), key);
        let delete = Ok(Next::Call(IoCall::Delete(vec![row])));
        assert_eq!(drain.next(emitted), delete);
        assert_eq!(drain.next(IoResult::Done), Ok(Next::Done(None)));
    }

    // An emission that did not settle, or failed, keeps its row; the next page starts after it,
    // and the run asks to run again.
    #[test]
    fn unsettled_keeps_row() {
        let (_, key, value) = watch_event_entry(&event()).unwrap();
        let mut drain = WatchDrainOperation::new();
        drain.first().unwrap();
        let after = Key::from(vec![1]);
        drain
            .next(page(vec![(key.clone(), value.clone())], Some(after.clone())))
            .unwrap();
        let open = IoResult::Child(ChildOutcome::new(Ok::<bool, IoError>(false)));
        let next_page = IoCall::Page {
            space: WATCH_OUTBOX_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after: Some(after),
            limit: WATCH_DRAIN_PAGE,
        };
        assert_eq!(drain.next(open), Ok(Next::Call(next_page)));
        drain.next(page(vec![(key, value)], None)).unwrap();
        let failed = Err::<bool, IoError>(IoError::Unfinished);
        let failed = IoResult::Child(ChildOutcome::new(failed));
        assert_eq!(drain.next(failed), Ok(Next::Done(Some(WATCH_RETRY_AFTER))));
    }

    // An unreadable row is deleted without an emission.
    #[test]
    fn unreadable_row_deleted() {
        let mut drain = WatchDrainOperation::new();
        drain.first().unwrap();
        let rows = vec![(Key::from(vec![9]), Value::from(vec![0xff]))];
        let delete = drain.next(page(rows, None));
        assert!(matches!(delete, Ok(Next::Call(IoCall::Delete(_)))));
    }
}
