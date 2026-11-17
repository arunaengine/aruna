//! Runs one bounded pass over the job family outbox and offers each queued record to its holders.
//! The pass reports whether entries remain, so the timer re-arms instead of spinning.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;

use aruna_core::effects::{Effect, IterStart, JobRecordFrame, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{FAMILY_OUTBOX_KEYSPACE, FAMILY_RECORD_KEYSPACE, NODE_STATE_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{JobRecordEnvelope, JobRecordKey};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::types::{Effects, Key};
use smallvec::smallvec;
use tracing::warn;

use super::LifecycleError;
use super::outbox::{OUTBOX_DRAIN_BATCH, PublishRecordConfig, PublishRecordOperation};
use crate::jobs::records::rows::from_bytes;
use crate::placement::resolve_shard_holders;
use crate::realm::get_config::GetConfigOperation;

const OUTBOX_CURSOR_KEY: &[u8] = b"job_family_outbox_cursor";

/// One drain pass. Every local failure ends the pass with "entries remain", so
/// the timer retries later; only an event that fits no phase is an error.
#[derive(Debug)]
pub struct DrainOutboxOperation {
    realm_id: RealmId,
    local: NodeId,
    realm: Option<Box<RealmConfigDocument>>,
    entries: VecDeque<Key>,
    next_cursor: Option<Key>,
    phase: Phase,
    outcome: Option<Result<bool, LifecycleError>>,
}

#[derive(Debug)]
enum Phase {
    Init,
    Config(GetConfigOperation),
    Cursor,
    Scan,
    Record(Key),
    Clear,
    Publish(Box<PublishRecordOperation>),
    WriteCursor,
    Rescan,
    Done,
}

impl DrainOutboxOperation {
    /// One pass of `local`, a node of realm `realm_id`.
    pub fn new(realm_id: RealmId, local: NodeId) -> Self {
        Self {
            realm_id,
            local,
            realm: None,
            entries: VecDeque::new(),
            next_cursor: None,
            phase: Phase::Init,
            outcome: None,
        }
    }

    fn finish(&mut self, remaining: bool) -> Effects {
        self.outcome = Some(Ok(remaining));
        self.phase = Phase::Done;
        smallvec![]
    }

    /// Ends a pass that cannot go on; the entries stay queued for the next one.
    fn retry_later(&mut self, what: &str, error: impl std::fmt::Display) -> Effects {
        warn!(error = %error, "Job family outbox {what} failed");
        self.finish(true)
    }

    fn unexpected(&mut self, phase: &'static str, event: &Event) -> Effects {
        self.outcome = Some(Err(LifecycleError::UnexpectedEvent {
            state: phase.to_string(),
            expected: "event of the current drain phase",
            got: format!("{event:?}"),
        }));
        self.phase = Phase::Done;
        smallvec![]
    }

    fn cursor_key() -> Key {
        Key::from(OUTBOX_CURSOR_KEY.to_vec())
    }

    fn scan(&mut self, start_after: Option<Key>, limit: usize) -> Effects {
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: FAMILY_OUTBOX_KEYSPACE.to_string(),
            prefix: None,
            start: start_after.map(IterStart::After),
            limit,
            txn_id: None,
        })]
    }

    /// Reads the record of the next entry with a valid key, or writes the cursor at the end.
    fn next_entry(&mut self) -> Effects {
        while let Some(key) = self.entries.pop_front() {
            if JobRecordKey::from_bytes(&key).is_err() {
                continue;
            }
            self.phase = Phase::Record(key.clone());
            return smallvec![Effect::Storage(StorageEffect::Read {
                key_space: FAMILY_RECORD_KEYSPACE.to_string(),
                key,
                txn_id: None,
            })];
        }
        self.phase = Phase::WriteCursor;
        let key_space = NODE_STATE_KEYSPACE.to_string();
        let key = Self::cursor_key();
        smallvec![Effect::Storage(match self.next_cursor.take() {
            Some(value) => StorageEffect::Write {
                key_space,
                key,
                value,
                txn_id: None,
            },
            None => StorageEffect::Delete {
                key_space,
                key,
                txn_id: None,
            },
        })]
    }

    /// Offers one stored record to the holders other than this node, or clears
    /// its entry when no other holder exists.
    fn offer(&mut self, key: Key, frame: JobRecordFrame) -> Effects {
        let (Ok(record_key), Some(realm)) = (JobRecordKey::from_bytes(&key), self.realm.as_ref())
        else {
            return self.next_entry();
        };
        let placement = match realm.family_placement(record_key.family.submission_id) {
            Ok(placement) => placement,
            Err(error) => {
                warn!(error = %error, "Queued record has no derivable family placement");
                return self.next_entry();
            }
        };
        let local = self.local;
        let holders: Vec<NodeId> = resolve_shard_holders(realm, &placement)
            .into_iter()
            .filter(|holder| *holder != local)
            .collect();
        if holders.is_empty() {
            self.phase = Phase::Clear;
            return smallvec![Effect::Storage(StorageEffect::Delete {
                key_space: FAMILY_OUTBOX_KEYSPACE.to_string(),
                key,
                txn_id: None,
            })];
        }
        let mut publish = PublishRecordOperation::new(PublishRecordConfig {
            realm_id: self.realm_id,
            placement,
            holders,
            record: Box::new(frame),
            key: record_key,
        });
        let effects = publish.start();
        self.phase = Phase::Publish(Box::new(publish));
        self.after_publish(effects)
    }

    /// Hands the publish's effects on, or moves to the next entry once it ended.
    fn after_publish(&mut self, effects: Effects) -> Effects {
        if !effects.is_empty() {
            return effects;
        }
        match std::mem::replace(&mut self.phase, Phase::Done) {
            Phase::Publish(publish) if publish.is_complete() => {
                if let Err(error) = publish.finalize() {
                    warn!(error = %error, "Job record replication failed");
                }
                self.next_entry()
            }
            Phase::Publish(_) => self.finish_stalled(),
            phase => {
                self.phase = phase;
                effects
            }
        }
    }

    fn finish_stalled(&mut self) -> Effects {
        self.outcome = Some(Err(LifecycleError::NotFinished));
        self.phase = Phase::Done;
        smallvec![]
    }
}

/// The stored record of one outbox entry as a frame, or `None` when it cannot be offered.
fn stored_frame(value: Option<&[u8]>) -> Option<JobRecordFrame> {
    let envelope = from_bytes::<JobRecordEnvelope>(value?).ok()?;
    JobRecordFrame::new(envelope).ok()
}

impl Operation for DrainOutboxOperation {
    type Output = bool;
    type Error = LifecycleError;

    fn start(&mut self) -> Effects {
        let mut config = GetConfigOperation::new(self.realm_id);
        let effects = config.start();
        self.phase = Phase::Config(config);
        effects
    }

    fn step(&mut self, event: Event) -> Effects {
        match std::mem::replace(&mut self.phase, Phase::Done) {
            Phase::Config(mut config) => {
                let effects = config.step(event);
                if !config.is_complete() {
                    self.phase = Phase::Config(config);
                    return effects;
                }
                match config.finalize() {
                    Ok(realm) => {
                        self.realm = Some(Box::new(realm));
                        self.phase = Phase::Cursor;
                        smallvec![Effect::Storage(StorageEffect::Read {
                            key_space: NODE_STATE_KEYSPACE.to_string(),
                            key: Self::cursor_key(),
                            txn_id: None,
                        })]
                    }
                    Err(error) => self.retry_later("realm config read", error),
                }
            }
            Phase::Cursor => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    self.phase = Phase::Scan;
                    self.scan(value, OUTBOX_DRAIN_BATCH)
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.retry_later("cursor read", error)
                }
                other => self.unexpected("cursor", &other),
            },
            Phase::Scan => match event {
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }) => {
                    self.entries = values.into_iter().map(|(key, _)| key).collect();
                    self.next_cursor = next_start_after;
                    self.next_entry()
                }
                Event::Storage(StorageEvent::Error { error }) => self.retry_later("scan", error),
                other => self.unexpected("scan", &other),
            },
            Phase::Record(key) => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    match stored_frame(value.as_deref()) {
                        Some(frame) => self.offer(key, frame),
                        None => self.next_entry(),
                    }
                }
                Event::Storage(StorageEvent::Error { .. }) => self.next_entry(),
                other => self.unexpected("record", &other),
            },
            Phase::Clear => match event {
                Event::Storage(StorageEvent::DeleteResult { .. }) => self.next_entry(),
                // A failed clear ends the pass before the cursor moves past the entry.
                Event::Storage(StorageEvent::Error { .. }) => self.finish(true),
                other => self.unexpected("clear", &other),
            },
            Phase::Publish(mut publish) => {
                let effects = publish.step(event);
                self.phase = Phase::Publish(publish);
                self.after_publish(effects)
            }
            Phase::WriteCursor => match event {
                Event::Storage(
                    StorageEvent::WriteResult { .. } | StorageEvent::DeleteResult { .. },
                ) => {
                    self.phase = Phase::Rescan;
                    self.scan(None, 1)
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    self.retry_later("cursor write", error)
                }
                other => self.unexpected("write cursor", &other),
            },
            Phase::Rescan => match event {
                Event::Storage(StorageEvent::IterResult { values, .. }) => {
                    self.finish(!values.is_empty())
                }
                Event::Storage(StorageEvent::Error { error }) => self.retry_later("rescan", error),
                other => self.unexpected("rescan", &other),
            },
            Phase::Init => self.unexpected("init", &event),
            Phase::Done => self.unexpected("done", &event),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.phase, Phase::Done)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.outcome.unwrap_or(Err(LifecycleError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
#[path = "outbox_drain_tests.rs"]
mod state_machine_tests;
