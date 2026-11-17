//! Bounds a node's watch interest digest, marks it dirty and requests its publish.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::WATCH_INTEREST_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::notification_watch::{
    INTEREST_BYTES_CAP, INTEREST_ENTRY_CAP, WATCH_DIRTY_PREFIX, WatchEventKind, WatchEventMask,
    WatchInterestDigest, WatchInterestEntry, interest_dirty_key, interest_node_key,
};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::types::{Effects, Key, KeySpace, Value};
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

/// Debounce window for the coalesced watch-interest publisher: `ShortenTimer`
/// fires this long after the first dirty write, collapsing a run of watch CRUD
/// into one publish with bounded latency.
pub(crate) const WATCH_PUBLISH_DEBOUNCE: Duration = Duration::from_secs(2);

/// Schedules (or shortens toward) the debounced watch-interest publish task.
pub(crate) fn schedule_publish_effect() -> Effect {
    Effect::Task(TaskEffect::ShortenTimer {
        key: TaskKey::PublishWatchInterest,
        after: WATCH_PUBLISH_DEBOUNCE,
    })
}

/// Dirty marker written in the same transaction as a subscription row change,
/// so the debounced publisher knows which realm's digest to rebuild. The value
/// is a generation id; a marker is only cleared if its generation still matches.
pub(crate) fn dirty_marker_write(realm_id: RealmId) -> (KeySpace, Key, Value) {
    let generation = ByteView::from(Ulid::generate().to_bytes().to_vec());
    (
        WATCH_INTEREST_KEYSPACE.to_string(),
        ByteView::from(interest_dirty_key(realm_id)),
        generation,
    )
}

pub(crate) fn digest_over(digest: &WatchInterestDigest) -> Result<bool, String> {
    if digest.entries.len() > INTEREST_ENTRY_CAP {
        return Ok(true);
    }
    Ok(digest.to_bytes().map_err(|error| error.to_string())?.len() > INTEREST_BYTES_CAP)
}

pub(crate) fn catchall_digest(node_id: NodeId) -> WatchInterestDigest {
    WatchInterestDigest {
        node_id,
        entries: vec![WatchInterestEntry {
            path_prefix: String::new(),
            event_mask: WatchEventMask::from_kinds([
                WatchEventKind::MetadataCreated,
                WatchEventKind::DataUploaded,
                WatchEventKind::SyncCompleted,
                WatchEventKind::SyncFailed,
            ]),
        }],
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum InterestError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("{0}")]
    Encode(String),
    #[error("unexpected event in the watch interest step: {0}")]
    UnexpectedEvent(String),
    #[error("the watch interest step did not finish")]
    NotFinished,
}

fn unexpected(event: Event) -> InterestError {
    match event {
        Event::Storage(StorageEvent::Error { error }) => InterestError::Storage(error),
        other => InterestError::UnexpectedEvent(format!("{other:?}")),
    }
}

#[derive(Debug, Default, PartialEq)]
enum InterestState {
    #[default]
    Init,
    Mark,
    Scan,
    Read,
    Write,
    Schedule(bool),
    Done(bool),
    Failed(InterestError),
}

/// The step on this node's watch interest a caller asks for.
#[derive(Debug, PartialEq)]
enum InterestStep {
    /// Marks the realm's digest dirty, then requests the debounced publish.
    Mark(RealmId),
    /// Requests the debounced publish.
    Publish,
    /// Requests the publish when a dirty marker is left over, as after a restart.
    Restore,
    /// Writes an empty digest for this node unless one exists; existing ones may hold live
    /// watches and are never overwritten.
    Ensure(RealmId, NodeId),
}

/// One small step on this node's watch interest. Its output is whether it changed anything:
/// a stored marker, a requested publish, or a created digest. A failed publish request only
/// waits for the periodic re-arm, as the durable marker survives.
#[derive(Debug, PartialEq)]
pub struct InterestOperation {
    step: InterestStep,
    state: InterestState,
}

impl InterestOperation {
    pub fn mark(realm_id: RealmId) -> Self {
        Self::new(InterestStep::Mark(realm_id))
    }

    pub fn publish() -> Self {
        Self::new(InterestStep::Publish)
    }

    pub fn restore() -> Self {
        Self::new(InterestStep::Restore)
    }

    pub fn ensure(realm_id: RealmId, node_id: NodeId) -> Self {
        Self::new(InterestStep::Ensure(realm_id, node_id))
    }

    fn new(step: InterestStep) -> Self {
        Self {
            step,
            state: InterestState::Init,
        }
    }

    fn schedule(&mut self, changed: bool) -> Effects {
        self.state = InterestState::Schedule(changed);
        smallvec![schedule_publish_effect()]
    }

    fn done(&mut self, changed: bool) -> Effects {
        self.state = InterestState::Done(changed);
        smallvec![]
    }
}

impl Operation for InterestOperation {
    type Output = bool;
    type Error = InterestError;

    fn start(&mut self) -> Effects {
        match self.step {
            InterestStep::Mark(realm_id) => {
                let (key_space, key, value) = dirty_marker_write(realm_id);
                self.state = InterestState::Mark;
                smallvec![Effect::Storage(StorageEffect::Write {
                    key_space,
                    key,
                    value,
                    txn_id: None,
                })]
            }
            InterestStep::Publish => self.schedule(true),
            InterestStep::Restore => {
                self.state = InterestState::Scan;
                smallvec![Effect::Storage(StorageEffect::Iter {
                    key_space: WATCH_INTEREST_KEYSPACE.to_string(),
                    prefix: Some(Key::from(WATCH_DIRTY_PREFIX.to_vec())),
                    start: None,
                    limit: 1,
                    txn_id: None,
                })]
            }
            InterestStep::Ensure(realm_id, node_id) => {
                self.state = InterestState::Read;
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: WATCH_INTEREST_KEYSPACE.to_string(),
                    key: Key::from(interest_node_key(realm_id, node_id)),
                    txn_id: None,
                })]
            }
        }
    }

    fn step(&mut self, event: Event) -> Effects {
        match (std::mem::take(&mut self.state), event) {
            (InterestState::Mark, Event::Storage(StorageEvent::WriteResult { .. })) => {
                self.schedule(true)
            }
            (InterestState::Scan, Event::Storage(StorageEvent::IterResult { values, .. })) => {
                match values.is_empty() {
                    true => self.done(false),
                    false => self.schedule(true),
                }
            }
            (InterestState::Read, Event::Storage(StorageEvent::ReadResult { key, value })) => {
                let InterestStep::Ensure(_, node_id) = self.step else {
                    return self.done(false);
                };
                if value.is_some() {
                    return self.done(false);
                }
                let digest = WatchInterestDigest {
                    node_id,
                    entries: Vec::new(),
                };
                match digest.to_bytes() {
                    Ok(bytes) => {
                        self.state = InterestState::Write;
                        smallvec![Effect::Storage(StorageEffect::Write {
                            key_space: WATCH_INTEREST_KEYSPACE.to_string(),
                            key,
                            value: Value::from(bytes),
                            txn_id: None,
                        })]
                    }
                    Err(error) => {
                        self.state =
                            InterestState::Failed(InterestError::Encode(error.to_string()));
                        smallvec![]
                    }
                }
            }
            (InterestState::Write, Event::Storage(StorageEvent::WriteResult { .. })) => {
                self.done(true)
            }
            (InterestState::Schedule(changed), Event::Task(task)) => {
                if let TaskEvent::Error { message, .. } = task {
                    warn!(%message, "Failed to schedule watch interest publish");
                }
                self.done(changed)
            }
            (_, event) => {
                self.state = InterestState::Failed(unexpected(event));
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            InterestState::Done(_) | InterestState::Failed(_)
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.state {
            InterestState::Done(changed) => Ok(changed),
            InterestState::Failed(error) => Err(error),
            _ => Err(InterestError::NotFinished),
        }
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod state_machine_tests {
    use super::*;
    use crate::tests::locations::node_id;
    use aruna_core::operation_check::{
        check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
    };

    const REALM: RealmId = RealmId([3; 32]);

    fn scheduled() -> Event {
        Event::Task(TaskEvent::TimerScheduled {
            key: TaskKey::PublishWatchInterest,
            after: WATCH_PUBLISH_DEBOUNCE,
        })
    }

    #[test]
    fn ensure_writes_once() {
        let key = Key::from(interest_node_key(REALM, node_id(1)));
        let mut ensure = InterestOperation::ensure(REALM, node_id(1));
        let digest = WatchInterestDigest {
            node_id: node_id(1),
            entries: Vec::new(),
        };
        check_trace(
            &mut ensure,
            &[Effect::Storage(StorageEffect::Read {
                key_space: WATCH_INTEREST_KEYSPACE.to_string(),
                key: key.clone(),
                txn_id: None,
            })],
            vec![
                (
                    Event::Storage(StorageEvent::ReadResult {
                        key: key.clone(),
                        value: None,
                    }),
                    vec![Effect::Storage(StorageEffect::Write {
                        key_space: WATCH_INTEREST_KEYSPACE.to_string(),
                        key: key.clone(),
                        value: Value::from(digest.to_bytes().unwrap()),
                        txn_id: None,
                    })],
                ),
                (Event::Storage(StorageEvent::WriteResult { key }), vec![]),
            ],
        );
        assert_eq!(ensure.finalize(), Ok(true));
    }

    #[test]
    fn existing_digest_kept() {
        let mut ensure = InterestOperation::ensure(REALM, node_id(1));
        ensure.start();
        let kept = ensure.step(Event::Storage(StorageEvent::ReadResult {
            key: Key::from(vec![1]),
            value: Some(Value::from(vec![1])),
        }));
        assert!(kept.is_empty());
        assert_eq!(ensure.finalize(), Ok(false));
    }

    #[test]
    fn mark_then_publish() {
        let mut mark = InterestOperation::mark(REALM);
        assert!(matches!(
            mark.start().as_slice(),
            [Effect::Storage(StorageEffect::Write { .. })]
        ));
        let publish = mark.step(Event::Storage(StorageEvent::WriteResult {
            key: Key::from(vec![1]),
        }));
        assert_eq!(publish.as_slice(), [schedule_publish_effect()]);
        mark.step(scheduled());
        assert_eq!(mark.finalize(), Ok(true));
    }

    #[test]
    fn restore_needs_marker() {
        let mut restore = InterestOperation::restore();
        restore.start();
        restore.step(Event::Storage(StorageEvent::IterResult {
            values: Vec::new(),
            next_start_after: None,
        }));
        assert_eq!(restore.finalize(), Ok(false));
        let mut failed = InterestOperation::publish();
        failed.start();
        failed.step(Event::Task(TaskEvent::Error {
            key: None,
            message: "closed".to_string(),
        }));
        assert_eq!(failed.finalize(), Ok(true));
    }

    #[test]
    fn unexpected_events_fail() {
        let make = || {
            let mut mark = InterestOperation::mark(REALM);
            mark.start();
            mark
        };
        let expected =
            |event: &Event| matches!(event, Event::Storage(StorageEvent::WriteResult { .. }));
        assert!(!rejects_events(make, sample_events, expected).is_empty());
    }

    #[test]
    fn abort_and_finalize() {
        let mut mark = InterestOperation::mark(REALM);
        mark.start();
        check_abort(&mut mark, &[]);
        rejects_finalize(mark);
    }
}
