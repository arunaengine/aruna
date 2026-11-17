//! Runs one bounded pass over the job family outbox and offers each queued record to its holders.
//! The pass reports whether entries remain, so the timer re-arms instead of spinning.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::document::DocumentTarget;
use aruna_core::effects::JobRecordFrame;
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{FAMILY_OUTBOX_KEYSPACE, FAMILY_RECORD_KEYSPACE, NODE_STATE_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::placement::resolve_shard_holders;
use aruna_core::structs::execution::job::record_rows::from_bytes;
use aruna_core::structs::execution::job::{JobRecordEnvelope, JobRecordKey};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::types::Key;
use tracing::warn;

use super::{OUTBOX_DRAIN_BATCH, PublishRecordConfig, PublishRecordOperation};
use crate::jobs::lifecycle::LifecycleError;

const OUTBOX_CURSOR_KEY: &[u8] = b"job_family_outbox_cursor";

/// One drain pass. A missing realm config ends the pass with "entries remain", so the timer
/// retries later; a failed storage call fails the pass, which the timer also retries.
#[derive(Debug, PartialEq)]
pub struct DrainOutboxOperation {
    realm_id: RealmId,
    local: NodeId,
    realm: Option<Box<RealmConfigDocument>>,
    entries: VecDeque<Key>,
    next_cursor: Option<Key>,
    phase: Phase,
}

/// The call the pass waits on.
#[derive(Debug, PartialEq)]
enum Phase {
    Config,
    Cursor,
    Scan,
    Record(Key),
    Clear,
    Publish(Box<PublishRecordOperation>),
    WriteCursor,
    Rescan,
}

type Step = Result<Next<bool>, LifecycleError>;

impl DrainOutboxOperation {
    /// One pass of `local`, a node of realm `realm_id`.
    pub fn new(realm_id: RealmId, local: NodeId) -> Self {
        Self {
            realm_id,
            local,
            realm: None,
            entries: VecDeque::new(),
            next_cursor: None,
            phase: Phase::Config,
        }
    }

    fn cursor_key() -> Key {
        Key::from(OUTBOX_CURSOR_KEY.to_vec())
    }

    fn scan(&mut self, after: Option<Key>, limit: usize) -> Step {
        Ok(Next::Call(IoCall::Page {
            space: FAMILY_OUTBOX_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after,
            limit,
        }))
    }

    /// Reads the record of the next entry with a valid key, or writes the cursor at the end.
    fn next_entry(&mut self) -> Step {
        while let Some(key) = self.entries.pop_front() {
            if JobRecordKey::from_bytes(&key).is_err() {
                continue;
            }
            self.phase = Phase::Record(key.clone());
            return Ok(Next::Call(IoCall::Read(vec![(
                FAMILY_RECORD_KEYSPACE.to_string(),
                key,
            )])));
        }
        self.phase = Phase::WriteCursor;
        let row = (NODE_STATE_KEYSPACE.to_string(), Self::cursor_key());
        Ok(Next::Call(match self.next_cursor.take() {
            Some(value) => IoCall::Write(vec![(row.0, row.1, value)]),
            None => IoCall::Delete(vec![row]),
        }))
    }

    /// Offers one stored record to the holders other than this node, or clears
    /// its entry when no other holder exists.
    fn offer(&mut self, key: Key, frame: JobRecordFrame) -> Step {
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
            return Ok(Next::Call(IoCall::Delete(vec![(
                FAMILY_OUTBOX_KEYSPACE.to_string(),
                key,
            )])));
        }
        let mut publish = PublishRecordOperation::new(PublishRecordConfig {
            placement,
            holders,
            record: Box::new(frame),
            key: record_key,
        });
        let step = publish.first();
        self.phase = Phase::Publish(Box::new(publish));
        self.after_publish(step)
    }

    /// Hands the publish's calls on, or moves to the next entry once it ended.
    fn after_publish(&mut self, step: Result<Next<bool>, LifecycleError>) -> Step {
        match step {
            Ok(Next::Done(_)) => self.next_entry(),
            Ok(next) => Ok(next),
            Err(error) => {
                warn!(error = %error, "Job record replication failed");
                self.next_entry()
            }
        }
    }
}

/// The stored record of one outbox entry as a frame, or `None` when it cannot be offered.
fn stored_frame(value: Option<&[u8]>) -> Option<JobRecordFrame> {
    let envelope = from_bytes::<JobRecordEnvelope>(value?).ok()?;
    JobRecordFrame::new(envelope).ok()
}

/// The single value a one-row read answered.
fn single(result: IoResult) -> Result<Option<aruna_core::types::Value>, LifecycleError> {
    match result {
        IoResult::Values(values) => Ok(values.into_iter().next().flatten()),
        _ => Err(LifecycleError::UnexpectedEvent {
            state: "drain".to_string(),
            expected: "one row",
            got: "another result",
        }),
    }
}

impl Operation for DrainOutboxOperation {
    type Output = bool;
    type Error = LifecycleError;

    fn first(&mut self) -> Step {
        let config = DocumentTarget::RealmConfig {
            realm_id: self.realm_id,
        };
        Ok(Next::Call(IoCall::Read(vec![(
            config.storage_keyspace().to_string(),
            config.storage_key(),
        )])))
    }

    fn next(&mut self, result: IoResult) -> Step {
        match std::mem::replace(&mut self.phase, Phase::WriteCursor) {
            Phase::Config => {
                let realm = single(result)?.map(|value| RealmConfigDocument::from_bytes(&value));
                match realm {
                    Some(Ok(realm)) => {
                        self.realm = Some(Box::new(realm));
                        self.phase = Phase::Cursor;
                        Ok(Next::Call(IoCall::Read(vec![(
                            NODE_STATE_KEYSPACE.to_string(),
                            Self::cursor_key(),
                        )])))
                    }
                    Some(Err(error)) => {
                        warn!(error = %error, "Job family outbox realm config read failed");
                        Ok(Next::Done(true))
                    }
                    None => {
                        warn!("Job family outbox realm config is missing");
                        Ok(Next::Done(true))
                    }
                }
            }
            Phase::Cursor => {
                let cursor = single(result)?;
                self.phase = Phase::Scan;
                self.scan(cursor, OUTBOX_DRAIN_BATCH)
            }
            Phase::Scan => {
                let IoResult::Page { rows, next } = result else {
                    return Err(unfit("outbox page"));
                };
                self.entries = rows.into_iter().map(|(key, _)| key).collect();
                self.next_cursor = next;
                self.next_entry()
            }
            Phase::Record(key) => match stored_frame(single(result)?.as_deref()) {
                Some(frame) => self.offer(key, frame),
                None => self.next_entry(),
            },
            Phase::Clear => match result {
                IoResult::Done => self.next_entry(),
                _ => Err(unfit("outbox delete")),
            },
            Phase::Publish(mut publish) => {
                let step = publish.next(result);
                self.phase = Phase::Publish(publish);
                self.after_publish(step)
            }
            Phase::WriteCursor => match result {
                IoResult::Done => {
                    self.phase = Phase::Rescan;
                    self.scan(None, 1)
                }
                _ => Err(unfit("cursor write")),
            },
            Phase::Rescan => match result {
                IoResult::Page { rows, .. } => Ok(Next::Done(!rows.is_empty())),
                _ => Err(unfit("outbox rescan")),
            },
        }
    }
}

fn unfit(expected: &'static str) -> LifecycleError {
    LifecycleError::UnexpectedEvent {
        state: "drain".to_string(),
        expected,
        got: "another result",
    }
}

#[cfg(test)]
#[path = "drain_tests.rs"]
mod state_machine_tests;
