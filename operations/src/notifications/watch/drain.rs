//! Emits the watch events producers queued as rows, oldest first, and deletes each row once its
//! emission ran; a failed emission only warns, as it did when producers emitted themselves.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::WATCH_OUTBOX_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::notification_watch::WatchEvent;
use aruna_core::types::Key;
use tracing::warn;

use super::origin::EmitEventOperation;

/// Rows one page of the drain reads at most.
pub(crate) const WATCH_DRAIN_PAGE: usize = 64;

type Turn = Result<Next<()>, IoError>;

#[derive(Clone, Debug, PartialEq)]
enum DrainStep {
    Page,
    Emit,
    Delete,
}

/// One drain run: every queued event is emitted and its row deleted, page by page, until the
/// keyspace is empty. A row that does not decode is deleted without an emission.
#[derive(Clone, Debug, PartialEq)]
pub struct WatchDrainOperation {
    step: DrainStep,
    pending: VecDeque<(Key, Option<WatchEvent>)>,
    current: Option<Key>,
    more: bool,
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
            more: false,
        }
    }

    fn read_page(&mut self) -> Turn {
        self.step = DrainStep::Page;
        Ok(Next::Call(IoCall::Page {
            space: WATCH_OUTBOX_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after: None,
            limit: WATCH_DRAIN_PAGE,
        }))
    }

    fn next_row(&mut self) -> Turn {
        let Some((key, event)) = self.pending.pop_front() else {
            return match self.more {
                true => self.read_page(),
                false => Ok(Next::Done(())),
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
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Turn {
        self.read_page()
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match self.step {
            DrainStep::Page => {
                let (rows, next) = result.page()?;
                self.more = next.is_some();
                for (key, value) in rows {
                    let event = WatchEvent::from_bytes(value.as_ref())
                        .inspect_err(|error| warn!(%error, "Deleting an unreadable watch row"))
                        .ok();
                    self.pending.push_back((key, event));
                }
                self.next_row()
            }
            DrainStep::Emit => {
                if let Err(error) = result.child::<(), IoError>()? {
                    warn!(%error, "A queued watch event could not be emitted");
                }
                self.delete()
            }
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
    use aruna_core::UserId;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::execution::notification_watch::watch_event_entry;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::types::Value;
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
        let emitted = IoResult::Child(ChildOutcome::new(Ok::<(), IoError>(())));
        let row = (WATCH_OUTBOX_KEYSPACE.to_string(), key);
        let delete = Ok(Next::Call(IoCall::Delete(vec![row])));
        assert_eq!(drain.next(emitted), delete);
        assert_eq!(drain.next(IoResult::Done), Ok(Next::Done(())));
    }

    // A failed emission still deletes its row, and a full page reads the next one.
    #[test]
    fn failure_still_deletes() {
        let (_, key, value) = watch_event_entry(&event()).unwrap();
        let mut drain = WatchDrainOperation::new();
        drain.first().unwrap();
        drain
            .next(page(vec![(key, value)], Some(Key::from(vec![1]))))
            .unwrap();
        let failed = Err::<(), IoError>(IoError::Unfinished);
        let failed = IoResult::Child(ChildOutcome::new(failed));
        assert!(matches!(
            drain.next(failed),
            Ok(Next::Call(IoCall::Delete(_)))
        ));
        let more = drain.next(IoResult::Done);
        assert!(matches!(more, Ok(Next::Call(IoCall::Page { .. }))));
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
