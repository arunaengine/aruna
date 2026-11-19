//! Emits the watch events producers queued as rows, oldest first, and deletes each row once its
//! emission settled; an open one keeps its row until its next attempt time, for its open holders.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::time::Duration;

use aruna_core::backoff::capped_backoff;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::WATCH_OUTBOX_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::notification_watch::{QueuedWatchEvent, WatchRetry};
use aruna_core::types::Key;
use tracing::warn;

use super::origin::EmitEventOperation;
use crate::queue_backoff::due_after;

/// Rows one page of the drain reads at most.
pub(crate) const WATCH_DRAIN_PAGE: usize = 64;
/// The wait before the first retry of an event that did not settle; it doubles per attempt.
pub const WATCH_RETRY_AFTER: Duration = Duration::from_secs(30);
/// The longest wait between two retries of one event.
const WATCH_RETRY_CAP: Duration = Duration::from_secs(3_600);

type Turn = Result<Next<Option<Duration>>, IoError>;

#[derive(Clone, Debug, PartialEq)]
enum DrainStep {
    Page,
    Emit,
    Retry,
    Delete,
}

/// One drain run over every queued event, page by page: a settled emission deletes its row; an
/// open one stores its next attempt time and open holders in the row, and rows not yet due wait.
/// The run answers when the earliest waiting row is due. An unreadable row goes.
#[derive(Clone, Debug, PartialEq)]
pub struct WatchDrainOperation {
    step: DrainStep,
    pending: VecDeque<(Key, Option<QueuedWatchEvent>)>,
    current: Option<(Key, QueuedWatchEvent)>,
    after: Option<Key>,
    next_due: Option<u64>,
    now_ms: u64,
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
            next_due: None,
            now_ms: 0,
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

    fn waits_until(&mut self, due_at_ms: u64) {
        self.next_due = Some(self.next_due.map_or(due_at_ms, |due| due.min(due_at_ms)));
    }

    fn next_row(&mut self) -> Turn {
        loop {
            let Some((key, queued)) = self.pending.pop_front() else {
                return match self.after {
                    Some(_) => self.read_page(),
                    None => Ok(Next::Done(self.next_due.map(|due| due_after(self.now_ms, due)))),
                };
            };
            let Some(queued) = queued else {
                return self.delete(key);
            };
            let waiting = queued.retry.as_ref().map(|retry| retry.due_at_ms);
            if let Some(due_at_ms) = waiting.filter(|due| *due > self.now_ms) {
                self.waits_until(due_at_ms);
                continue;
            }
            let holders = queued.retry.as_ref().and_then(|retry| retry.holders.clone());
            let emit = EmitEventOperation::retrying(queued.event.clone(), holders);
            self.current = Some((key, queued));
            self.step = DrainStep::Emit;
            return Ok(Next::Call(IoCall::child(emit)));
        }
    }

    /// Stores when the open event is tried again and for which holders; a failed emission that
    /// named none keeps the holders the row already had.
    fn retry(&mut self, open: Option<Vec<NodeId>>) -> Turn {
        let (_, mut queued) = self.current.take().ok_or(IoError::Unfinished)?;
        let previous = queued.retry.take();
        let attempts = previous.as_ref().map_or(0, |retry| retry.attempts);
        let wait = capped_backoff(WATCH_RETRY_AFTER, WATCH_RETRY_CAP, attempts, 0);
        let wait_ms = u64::try_from(wait.as_millis()).unwrap_or(u64::MAX);
        let due_at_ms = self.now_ms.saturating_add(wait_ms);
        let holders = open.or_else(|| previous.and_then(|retry| retry.holders));
        queued.retry = Some(WatchRetry {
            due_at_ms,
            attempts: attempts.saturating_add(1),
            holders,
        });
        self.waits_until(due_at_ms);
        let row = queued.entry().map_err(|error| IoError::Codec(error.to_string()))?;
        self.step = DrainStep::Retry;
        Ok(Next::Call(IoCall::Write(vec![row])))
    }

    fn delete(&mut self, key: Key) -> Turn {
        self.step = DrainStep::Delete;
        let row = (WATCH_OUTBOX_KEYSPACE.to_string(), key);
        Ok(Next::Call(IoCall::Delete(vec![row])))
    }
}

impl Operation for WatchDrainOperation {
    type Output = Option<Duration>;
    type Error = IoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Turn {
        self.read_page()
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match self.step {
            DrainStep::Page => {
                let (rows, next) = result.page()?;
                // Rows kept for a later attempt are passed: the next page starts after them.
                self.after = next;
                for (key, value) in rows {
                    let queued = QueuedWatchEvent::from_bytes(value.as_ref())
                        .inspect_err(|error| warn!(%error, "Deleting an unreadable watch row"))
                        .ok();
                    self.pending.push_back((key, queued));
                }
                self.next_row()
            }
            DrainStep::Emit => match result.child::<Vec<NodeId>, IoError>()? {
                Ok(open) if open.is_empty() => {
                    let (key, _) = self.current.take().ok_or(IoError::Unfinished)?;
                    self.delete(key)
                }
                Ok(open) => self.retry(Some(open)),
                Err(error) => {
                    warn!(%error, "A queued watch event could not be emitted");
                    self.retry(None)
                }
            },
            DrainStep::Retry | DrainStep::Delete => {
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
    use aruna_core::structs::execution::notification_watch::{WatchEvent, watch_event_entry};
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::types::Value;
    use aruna_core::user::id::UserId;
    use ulid::Ulid;

    const NOW_MS: u64 = 100_000;

    fn event(seed: u8) -> WatchEvent {
        let realm = RealmId::from_bytes([3; 32]);
        let user = UserId::local(Ulid::from_bytes([4; 16]), realm);
        let group = Ulid::from_bytes([5; 16]);
        let document = Ulid::from_bytes([6; 16]);
        let event_id = Ulid::from_parts(1, u128::from(seed));
        WatchEvent::metadata_created(realm, user, group, document, "d", event_id)
    }

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn queued(seed: u8, retry: Option<WatchRetry>) -> QueuedWatchEvent {
        QueuedWatchEvent {
            event: event(seed),
            retry,
        }
    }

    fn row(queued: &QueuedWatchEvent) -> (Key, Value) {
        let (_, key, value) = queued.entry().unwrap();
        (key, value)
    }

    fn page(rows: Vec<(Key, Value)>, next: Option<Key>) -> IoResult {
        IoResult::Page { rows, next }
    }

    fn emitted(outcome: Result<Vec<NodeId>, IoError>) -> IoResult {
        IoResult::Child(ChildOutcome::new(outcome))
    }

    fn drain() -> WatchDrainOperation {
        let mut drain = WatchDrainOperation::new();
        drain.stamp(NOW_MS);
        assert!(matches!(drain.first(), Ok(Next::Call(IoCall::Page { .. }))));
        drain
    }

    // Each queued event is emitted, then its row goes; an empty keyspace ends the run.
    #[test]
    fn emits_then_deletes() {
        let (_, key, value) = watch_event_entry(&event(1)).unwrap();
        let mut drain = drain();
        let emit = drain.next(page(vec![(key.clone(), value)], None));
        assert!(matches!(emit, Ok(Next::Call(IoCall::Child(_)))));
        let row = (WATCH_OUTBOX_KEYSPACE.to_string(), key);
        let delete = Ok(Next::Call(IoCall::Delete(vec![row])));
        assert_eq!(drain.next(emitted(Ok(Vec::new()))), delete);
        assert_eq!(drain.next(IoResult::Done), Ok(Next::Done(None)));
    }

    // An open emission stores its next attempt time and open holders in its row, and the run
    // answers when that attempt is due.
    #[test]
    fn open_row_waits() {
        let mut drain = drain();
        drain.next(page(vec![row(&queued(1, None))], None)).unwrap();
        let retry = WatchRetry {
            due_at_ms: NOW_MS + 30_000,
            attempts: 1,
            holders: Some(vec![node(2)]),
        };
        let (_, key, value) = queued(1, Some(retry)).entry().unwrap();
        let stored = Ok(Next::Call(IoCall::Write(vec![(
            WATCH_OUTBOX_KEYSPACE.to_string(),
            key,
            value,
        )])));
        assert_eq!(drain.next(emitted(Ok(vec![node(2)]))), stored);
        assert_eq!(drain.next(IoResult::Done), Ok(Next::Done(Some(WATCH_RETRY_AFTER))));
    }

    // A failed row waits for its attempt time even when a new event arms the drain at once;
    // the new event leaves now, and a due row goes only to its open holders.
    #[test]
    fn waiting_row_skipped() {
        let later = WatchRetry {
            due_at_ms: NOW_MS + 10_000,
            attempts: 2,
            holders: None,
        };
        let due = WatchRetry {
            due_at_ms: NOW_MS,
            attempts: 1,
            holders: Some(vec![node(3)]),
        };
        let rows = vec![
            row(&queued(1, Some(later))),
            row(&queued(2, Some(due))),
            row(&queued(3, None)),
        ];
        let mut drain = drain();
        let only = EmitEventOperation::retrying(event(2), Some(vec![node(3)]));
        assert_eq!(drain.next(page(rows, None)), Ok(Next::Call(IoCall::child(only))));
        assert!(matches!(drain.next(emitted(Ok(Vec::new()))), Ok(Next::Call(IoCall::Delete(_)))));
        let fresh = EmitEventOperation::retrying(event(3), None);
        assert_eq!(drain.next(IoResult::Done), Ok(Next::Call(IoCall::child(fresh))));
        assert!(matches!(drain.next(emitted(Ok(Vec::new()))), Ok(Next::Call(IoCall::Delete(_)))));
        let wait = Some(Duration::from_millis(10_000));
        assert_eq!(drain.next(IoResult::Done), Ok(Next::Done(wait)));
    }

    // A failed emission names no holders, so the row keeps the ones it had and waits longer.
    #[test]
    fn failure_keeps_holders() {
        let due = WatchRetry {
            due_at_ms: NOW_MS,
            attempts: 1,
            holders: Some(vec![node(3)]),
        };
        let mut drain = drain();
        drain.next(page(vec![row(&queued(2, Some(due)))], None)).unwrap();
        let retry = WatchRetry {
            due_at_ms: NOW_MS + 60_000,
            attempts: 2,
            holders: Some(vec![node(3)]),
        };
        let (_, key, value) = queued(2, Some(retry)).entry().unwrap();
        let stored = Ok(Next::Call(IoCall::Write(vec![(
            WATCH_OUTBOX_KEYSPACE.to_string(),
            key,
            value,
        )])));
        assert_eq!(drain.next(emitted(Err(IoError::Unfinished))), stored);
    }

    // An unreadable row is deleted without an emission.
    #[test]
    fn unreadable_row_deleted() {
        let mut drain = drain();
        let rows = vec![(Key::from(vec![9]), Value::from(vec![0xff]))];
        let delete = drain.next(page(rows, None));
        assert!(matches!(delete, Ok(Next::Call(IoCall::Delete(_)))));
    }
}
