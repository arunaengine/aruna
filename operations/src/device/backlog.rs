//! Drains the two device queues page by page, forwarding the due entries side by side.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::fmt::Debug;

use aruna_core::calls::{ChildOutcome, IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, EffectGroup, GroupForm};
use aruna_core::events::{Event, GroupEvent, GroupResult};
use aruna_core::operation::{Operation, Runnable, boxed_suboperation};
use aruna_core::types::{Key, Value};
use tracing::warn;

use super::drain::DrainOutcome;
use super::retry::RetryView;
use crate::distribution::inventory::{StagedCall, outside_answer};

/// What forwarding one due queue entry asks of the drain.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ForwardOutcome {
    /// The entry may need another pass; keep the drain awake.
    Recheck,
    /// This pass is done with the entry.
    Settled,
}

/// One forward as a member of the drain's window.
pub(crate) fn forward_member<O, K>(forward: O) -> Effect
where
    O: Runnable<K, Output = ForwardOutcome> + 'static,
    O::Error: PartialEq + 'static,
    K: 'static,
{
    Effect::SubOperation(boxed_suboperation(forward, |result| {
        Event::Child(ChildOutcome::new(result))
    }))
}

/// One device queue as the page drain runs it.
pub(crate) trait DrainQueue: Send + Debug + PartialEq {
    type Entry: RetryView;

    /// The keyspace of the queue and how many rows one page reads.
    fn page(&self) -> (&'static str, usize);
    /// The entry a row holds; a row that does not decode is skipped.
    fn entry(&self, value: &Value) -> Option<Self::Entry>;
    /// The forward of one due entry, which reports a [`ForwardOutcome`] or an error of `E`.
    fn forward(&self, entry: Self::Entry) -> Effect;
    /// Whether a finished forward asks for another pass.
    fn rechecks(&self, outcome: ChildOutcome) -> bool;
}

/// Reads whether a forward's outcome asks for another pass; a failed forward does.
pub(crate) fn outcome_rechecks<E: 'static>(outcome: ChildOutcome) -> bool {
    !matches!(
        outcome.take::<ForwardOutcome, E>(),
        Some(Ok(ForwardOutcome::Settled))
    )
}

/// Drains one queue page by page: entries that are not due are skipped, the rest are forwarded,
/// at most `in_flight` at once. Each forward reports whether it asks for another pass, so each
/// queue keeps its own claim ordering.
#[derive(Debug, PartialEq)]
pub(crate) struct DrainOperation<Q> {
    queue: Q,
    now_ms: u64,
    in_flight: usize,
    cursor: Option<Key>,
    next: Option<Key>,
    recheck: bool,
    forwarding: bool,
}

type Draining = Result<Next<DrainOutcome>, IoError>;

impl<Q: DrainQueue> DrainOperation<Q> {
    pub(crate) fn new(queue: Q, now_ms: u64, in_flight: usize) -> Self {
        Self {
            queue,
            now_ms,
            in_flight,
            cursor: None,
            next: None,
            recheck: false,
            forwarding: false,
        }
    }

    fn read_page(&mut self) -> Draining {
        let (space, limit) = self.queue.page();
        let page = IoCall::Page {
            space: space.to_string(),
            prefix: Key::from(Vec::new()),
            after: self.cursor.take(),
            limit,
        };
        self.forwarding = false;
        Ok(Next::Call(StagedCall::Outside(page).into_call()))
    }

    fn page_done(&mut self) -> Draining {
        match self.next.take() {
            Some(next) => {
                self.cursor = Some(next);
                self.read_page()
            }
            None => Ok(Next::Done(match self.recheck {
                true => DrainOutcome::Recheck,
                false => DrainOutcome::Idle,
            })),
        }
    }

    fn page_read(&mut self, result: IoResult) -> Draining {
        let (rows, next) = match outside_answer(result).and_then(IoResult::page) {
            Ok(page) => page,
            Err(error) => {
                warn!(%error, "Failed to scan a device queue");
                return Ok(Next::Done(DrainOutcome::Deferred));
            }
        };
        self.next = next;
        let members: Vec<Effect> = (rows.iter())
            .filter_map(|(_, value)| self.queue.entry(value))
            .filter(|entry| entry.is_due(self.now_ms))
            .map(|entry| self.queue.forward(entry))
            .collect();
        if members.is_empty() {
            return self.page_done();
        }
        self.forwarding = true;
        Ok(Next::Call(IoCall::Group(EffectGroup {
            members,
            limit: self.in_flight.max(1),
            form: GroupForm::Window,
        })))
    }
}

impl<Q: DrainQueue> Operation for DrainOperation<Q> {
    type Output = DrainOutcome;
    type Error = IoError;

    fn first(&mut self) -> Draining {
        self.read_page()
    }

    fn next(&mut self, result: IoResult) -> Draining {
        if !self.forwarding {
            return self.page_read(result);
        }
        match result {
            IoResult::Group(GroupEvent::Member { result, .. }) => {
                self.recheck |= match result {
                    GroupResult::Done(event) => match *event {
                        Event::Child(outcome) => self.queue.rechecks(outcome),
                        _ => true,
                    },
                    _ => true,
                };
                Ok(Next::Wait)
            }
            IoResult::Group(GroupEvent::Closed) => self.page_done(),
            other => Err(IoError::Unexpected {
                expected: "IoResult::Group",
                got: other.label(),
            }),
        }
    }
}

#[cfg(test)]
#[path = "backlog_tests.rs"]
mod tests;
