//! Bounds a node's watch interest digest, marks it dirty and requests its publish.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::keyspaces::WATCH_INTEREST_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::notification_watch::{
    INTEREST_BYTES_CAP, INTEREST_ENTRY_CAP, WATCH_DIRTY_PREFIX, WatchEventKind, WatchEventMask,
    WatchInterestDigest, WatchInterestEntry, interest_dirty_key, interest_node_key,
};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::types::{Key, KeySpace, Value};
use byteview::ByteView;
use thiserror::Error;
use ulid::Ulid;

use crate::notifications::unexpected;

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
    #[error("{0}")]
    Encode(String),
    #[error(transparent)]
    Io(#[from] IoError),
}

#[derive(Debug, Default, PartialEq)]
enum InterestState {
    #[default]
    Mark,
    Scan,
    Read,
    Write,
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
            state: InterestState::Mark,
        }
    }

    /// Requests the debounced publish once the step is stored.
    fn schedule(&mut self, changed: bool) -> Next<bool> {
        Next::Then {
            output: changed,
            calls: vec![IoCall::from(schedule_publish_effect())],
        }
    }
}

impl Operation for InterestOperation {
    type Output = bool;
    type Error = InterestError;

    fn first(&mut self) -> Result<Next<Self::Output>, Self::Error> {
        let call = match self.step {
            InterestStep::Mark(realm_id) => {
                self.state = InterestState::Mark;
                IoCall::Write(vec![dirty_marker_write(realm_id)])
            }
            InterestStep::Publish => return Ok(self.schedule(true)),
            InterestStep::Restore => {
                self.state = InterestState::Scan;
                IoCall::Page {
                    space: WATCH_INTEREST_KEYSPACE.to_string(),
                    prefix: Key::from(WATCH_DIRTY_PREFIX.to_vec()),
                    after: None,
                    limit: 1,
                }
            }
            InterestStep::Ensure(realm_id, node_id) => {
                self.state = InterestState::Read;
                let key = Key::from(interest_node_key(realm_id, node_id));
                IoCall::Read(vec![(WATCH_INTEREST_KEYSPACE.to_string(), key)])
            }
        };
        Ok(Next::Call(call))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, Self::Error> {
        match (std::mem::take(&mut self.state), result) {
            (InterestState::Mark, IoResult::Done) => Ok(self.schedule(true)),
            (InterestState::Scan, IoResult::Page { rows, .. }) => match rows.is_empty() {
                true => Ok(Next::Done(false)),
                false => Ok(self.schedule(true)),
            },
            (InterestState::Read, IoResult::Values(values)) => {
                let InterestStep::Ensure(realm_id, node_id) = self.step else {
                    return Ok(Next::Done(false));
                };
                if values.into_iter().flatten().next().is_some() {
                    return Ok(Next::Done(false));
                }
                let digest = WatchInterestDigest {
                    node_id,
                    entries: Vec::new(),
                };
                let bytes = digest
                    .to_bytes()
                    .map_err(|error| InterestError::Encode(error.to_string()))?;
                self.state = InterestState::Write;
                let key = Key::from(interest_node_key(realm_id, node_id));
                let row = (WATCH_INTEREST_KEYSPACE.to_string(), key, Value::from(bytes));
                Ok(Next::Call(IoCall::Write(vec![row])))
            }
            (InterestState::Write, IoResult::Done) => Ok(Next::Done(true)),
            (_, other) => Err(unexpected("a watch interest result", &other).into()),
        }
    }
}

#[cfg(test)]
mod call_tests {
    use super::*;
    use crate::tests::locations::node_id;

    const REALM: RealmId = RealmId([3; 32]);

    fn scheduled() -> Next<bool> {
        Next::Then {
            output: true,
            calls: vec![IoCall::from(schedule_publish_effect())],
        }
    }

    #[test]
    fn ensure_writes_once() {
        let key = Key::from(interest_node_key(REALM, node_id(1)));
        let space = WATCH_INTEREST_KEYSPACE.to_string();
        let mut ensure = InterestOperation::ensure(REALM, node_id(1));
        assert_eq!(
            ensure.first(),
            Ok(Next::Call(IoCall::Read(vec![(space.clone(), key.clone())])))
        );
        let digest = WatchInterestDigest {
            node_id: node_id(1),
            entries: Vec::new(),
        };
        let row = (space, key, Value::from(digest.to_bytes().unwrap()));
        assert_eq!(
            ensure.next(IoResult::Values(vec![None])),
            Ok(Next::Call(IoCall::Write(vec![row])))
        );
        assert_eq!(ensure.next(IoResult::Done), Ok(Next::Done(true)));
    }

    #[test]
    fn existing_digest_kept() {
        let mut ensure = InterestOperation::ensure(REALM, node_id(1));
        ensure.first().unwrap();
        let kept = ensure.next(IoResult::Values(vec![Some(Value::from(vec![1]))]));
        assert_eq!(kept, Ok(Next::Done(false)));
    }

    #[test]
    fn mark_then_publish() {
        let mut mark = InterestOperation::mark(REALM);
        assert!(matches!(mark.first(), Ok(Next::Call(IoCall::Write(rows))) if rows.len() == 1));
        assert_eq!(mark.next(IoResult::Done), Ok(scheduled()));
    }

    #[test]
    fn restore_needs_marker() {
        let mut restore = InterestOperation::restore();
        restore.first().unwrap();
        let empty = IoResult::Page {
            rows: Vec::new(),
            next: None,
        };
        assert_eq!(restore.next(empty), Ok(Next::Done(false)));
        assert_eq!(InterestOperation::publish().first(), Ok(scheduled()));
    }

    #[test]
    fn other_results_fail() {
        let mut mark = InterestOperation::mark(REALM);
        mark.first().unwrap();
        assert!(matches!(
            mark.next(IoResult::Woke),
            Err(InterestError::Io(_))
        ));
    }
}
