//! Locks a bucket: every unlocked generation stops admitting reads at once, while admitted
//! reads finish. The lock applies before its audit, so a failed audit write never undoes it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::s3::bucket::key_rows::{SettingsError, authority_read, parse_authority};
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{BUCKET_AUDIT_KEYSPACE, BUCKET_HOLDER_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::encryption::{BucketHolder, HolderOrigin, KeyTicket};
use aruna_core::structs::storage::key_audit::{AuditAction, AuditOutcome, BucketAuditRecord};
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::types::{Effects, GroupId, Key, Value};
use aruna_core::{NodeId, UserId};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LockStep {
    Init,
    ReadBucket,
    ReadGrant,
    LockKeys,
    WriteAudit,
    CancelTimers,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum LockError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Settings(#[from] SettingsError),
    #[error(transparent)]
    Blob(#[from] BlobError),
    #[error("the bucket has no keys")]
    NotEncrypted,
    #[error("the caller is neither a key holder nor a group admin")]
    NotHolder,
    #[error("unexpected event in state {state}: expected {expected}, got {received:?}")]
    InvalidStateEvent {
        state: String,
        expected: &'static str,
        received: Event,
    },
    #[error("the lock did not finish")]
    NotFinished,
}

/// A lock by a holder or group admin. Without a caller the node locks one timed `session`.
#[derive(Debug, PartialEq)]
pub struct LockInput {
    pub bucket: String,
    pub group_id: GroupId,
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub caller: Option<UserId>,
    pub session: Option<KeyTicket>,
    pub now_ms: u64,
}

#[derive(Debug, PartialEq)]
pub struct LockResult {
    /// The sessions this lock ended; an already locked bucket reports none.
    pub locked: Vec<KeyTicket>,
    pub audited: bool,
}

#[derive(Debug, PartialEq)]
pub struct LockBucketOperation {
    input: LockInput,
    step: LockStep,
    bucket_id: Option<Ulid>,
    locked: Vec<KeyTicket>,
    audited: bool,
    timers: usize,
    output: Option<Result<LockResult, LockError>>,
}

impl LockBucketOperation {
    pub fn new(input: LockInput) -> Self {
        Self {
            input,
            step: LockStep::Init,
            bucket_id: None,
            locked: Vec::new(),
            audited: false,
            timers: 0,
            output: None,
        }
    }

    /// The node's own lock of one timed session, after its deadline passed.
    pub fn timed(session: KeyTicket, node_id: NodeId) -> Self {
        let mut operation = Self::new(LockInput {
            bucket: String::new(),
            group_id: Ulid::nil(),
            realm_id: RealmId::from_bytes([0; 32]),
            node_id,
            caller: None,
            session: Some(session),
            now_ms: aruna_core::time::unix_timestamp_millis(),
        });
        operation.bucket_id = Some(session.key.bucket_id);
        operation
    }

    fn fail(&mut self, error: impl Into<LockError>) -> Effects {
        self.output = Some(Err(error.into()));
        self.step = LockStep::Error;
        smallvec![]
    }

    fn authorize(&mut self, values: Vec<(Key, Option<Value>)>) -> Effects {
        let (realm_id, group_id) = (self.input.realm_id, self.input.group_id);
        let state = match parse_authority(values, realm_id, group_id) {
            Ok(state) => state,
            Err(error) => return self.fail(error),
        };
        let settings = state.settings;
        let Some(bucket_id) = settings.bucket_id else {
            return self.fail(LockError::NotEncrypted);
        };
        self.bucket_id = Some(bucket_id);
        let Some(caller) = self.input.caller else {
            return self.lock();
        };
        if state.info.created_by == caller || state.admins.contains(&caller) {
            return self.lock();
        }
        self.step = LockStep::ReadGrant;
        let grant = [&bucket_id.to_bytes()[..], &caller.to_storage_key()].concat();
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BUCKET_HOLDER_KEYSPACE.to_string(),
            key: grant.into(),
            txn_id: None,
        })]
    }

    fn lock(&mut self) -> Effects {
        let Some(bucket_id) = self.bucket_id else {
            return self.fail(LockError::NotFinished);
        };
        self.step = LockStep::LockKeys;
        smallvec![Effect::Blob(BlobEffect::LockKey {
            bucket_id,
            session: self.input.session,
        })]
    }

    fn audit(&mut self, locked: Vec<KeyTicket>) -> Effects {
        let foreign = |ticket: &KeyTicket| Some(ticket.key.bucket_id) != self.bucket_id;
        if locked.iter().any(foreign) {
            return self.fail(LockError::NotFinished);
        }
        self.locked = locked;
        let action = match self.input.caller {
            Some(_) => AuditAction::Lock,
            None => AuditAction::TimedLock,
        };
        let mut writes = Vec::new();
        for ticket in &self.locked {
            let record = BucketAuditRecord {
                event_id: Ulid::generate(),
                bucket_id: ticket.key.bucket_id,
                at_ms: self.input.now_ms,
                action,
                actor: self.input.caller,
                node_id: self.input.node_id,
                generation: Some(ticket.key.generation),
                deadline_ms: None,
                reason: None,
                outcome: AuditOutcome::Applied,
            };
            match record.to_bytes() {
                Ok(value) => writes.push((
                    BUCKET_AUDIT_KEYSPACE.to_string(),
                    record.key().into(),
                    value.into(),
                )),
                Err(error) => return self.fail(error),
            }
        }
        if writes.is_empty() {
            return self.finish(true);
        }
        self.step = LockStep::WriteAudit;
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })]
    }

    /// A manual lock ends the timers of the sessions it locked; a timed lock is its own timer.
    fn finish(&mut self, audited: bool) -> Effects {
        self.audited = audited;
        let cancels: Effects = match self.input.caller {
            Some(_) => self
                .locked
                .iter()
                .map(|ticket| {
                    Effect::Task(TaskEffect::CancelTimer {
                        key: lock_timer(ticket),
                    })
                })
                .collect(),
            None => Effects::new(),
        };
        if !cancels.is_empty() {
            self.timers = cancels.len();
            self.step = LockStep::CancelTimers;
            return cancels;
        }
        self.complete()
    }

    fn complete(&mut self) -> Effects {
        let locked = std::mem::take(&mut self.locked);
        self.output = Some(Ok(LockResult {
            locked,
            audited: self.audited,
        }));
        self.step = LockStep::Finish;
        smallvec![]
    }
}

impl Operation for LockBucketOperation {
    type Output = LockResult;
    type Error = LockError;

    fn start(&mut self) -> Effects {
        if self.input.caller.is_none() && self.bucket_id.is_some() {
            return self.lock();
        }
        self.step = LockStep::ReadBucket;
        let (realm_id, group_id) = (self.input.realm_id, self.input.group_id);
        smallvec![authority_read(&self.input.bucket, realm_id, group_id, None)]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (self.step, event) {
            // The lock already applied; it stays even when its audit cannot be written.
            (LockStep::WriteAudit, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                self.finish(true)
            }
            (LockStep::WriteAudit, Event::Storage(StorageEvent::Error { .. })) => {
                self.finish(false)
            }
            (_, Event::Storage(StorageEvent::Error { error })) => self.fail(error),
            (LockStep::ReadBucket, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                self.authorize(values)
            }
            (LockStep::ReadGrant, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                let grant = value
                    .map(|value| BucketHolder::from_bytes(&value))
                    .transpose();
                match grant {
                    Ok(Some(grant)) if grant.origin == HolderOrigin::Explicit => self.lock(),
                    Ok(_) => self.fail(LockError::NotHolder),
                    Err(error) => self.fail(error),
                }
            }
            (LockStep::LockKeys, Event::Blob(BlobEvent::KeyLocked { locked })) => {
                self.audit(locked)
            }
            // A timer that cannot be cancelled fires later and finds its session gone.
            (LockStep::CancelTimers, Event::Task(event))
                if self
                    .locked
                    .iter()
                    .any(|ticket| answers_timer(&event, &lock_timer(ticket))) =>
            {
                self.timers = self.timers.saturating_sub(1);
                match self.timers {
                    0 => self.complete(),
                    _ => smallvec![],
                }
            }
            (LockStep::Finish | LockStep::Error, _) => smallvec![],
            (_, Event::Blob(BlobEvent::Error(error))) => self.fail(error),
            (state, received) => self.fail(LockError::InvalidStateEvent {
                state: format!("{state:?}"),
                expected: "the event of the current step",
                received,
            }),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.step, LockStep::Finish | LockStep::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(LockError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

/// Whether a task event answers an effect on the timer `key`; a failure counts as an answer.
pub(crate) fn answers_timer(event: &TaskEvent, key: &TaskKey) -> bool {
    match event {
        TaskEvent::TimerScheduled { key: answered, .. }
        | TaskEvent::TimerCancelled { key: answered } => answered == key,
        TaskEvent::Error { key: answered, .. } => {
            answered.as_ref().is_none_or(|answered| answered == key)
        }
        TaskEvent::RunningHandlersAborted { .. } => false,
    }
}

/// The in-memory timer of one unlock session.
pub fn lock_timer(ticket: &KeyTicket) -> TaskKey {
    TaskKey::LockBucket {
        bucket_id: ticket.key.bucket_id,
        generation: ticket.key.generation,
        session_id: ticket.session_id,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::bucket::key_rows::authority_rows;
    use aruna_core::structs::storage::blob::BucketInfo;
    use aruna_core::structs::storage::encryption::{
        BucketEncryption, BucketKeyRef, EncryptionMode,
    };
    use aruna_core::structs::storage::format::Compression;
    use std::time::SystemTime;

    const BUCKET_ID: Ulid = Ulid::from_bytes([4; 16]);

    fn user(seed: u8) -> UserId {
        UserId::new(Ulid::from_bytes([seed; 16]), RealmId::from_bytes([1; 32]))
    }

    fn read(caller: Option<UserId>, session: Option<KeyTicket>) -> (LockBucketOperation, Effects) {
        let mut operation = LockBucketOperation::new(LockInput {
            bucket: "bucket".to_string(),
            group_id: Ulid::from_bytes([3; 16]),
            node_id: iroh::SecretKey::from_bytes(&[2; 32]).public(),
            caller,
            session,
            realm_id: RealmId::from_bytes([1; 32]),
            now_ms: 5,
        });
        operation.start();
        let info = BucketInfo {
            group_id: Ulid::from_bytes([3; 16]),
            created_at: SystemTime::UNIX_EPOCH,
            created_by: user(1),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
        };
        // Mode off with retained keys: a decrypting change may still hold source generations.
        let settings = BucketEncryption {
            mode: EncryptionMode::Off,
            bucket_id: Some(BUCKET_ID),
            key_generation: 2,
            ..Default::default()
        };
        let values = authority_rows(&info, Some(&settings), &[]);
        let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult { values }));
        (operation, effects)
    }

    fn ticket(generation: u64) -> KeyTicket {
        KeyTicket {
            key: BucketKeyRef::new(BUCKET_ID, generation),
            session_id: Ulid::from_bytes([generation as u8; 16]),
        }
    }

    #[test]
    fn locks_every_generation() {
        let (mut operation, effects) = read(Some(user(1)), None);
        let lock = BlobEffect::LockKey {
            bucket_id: BUCKET_ID,
            session: None,
        };
        assert_eq!(effects.as_slice(), [Effect::Blob(lock)]);
        let locked = vec![ticket(1), ticket(2)];
        let effects = operation.step(Event::Blob(BlobEvent::KeyLocked {
            locked: locked.clone(),
        }));
        let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
            panic!("expected the audit records, got {effects:?}");
        };
        let records: Vec<_> = writes
            .iter()
            .map(|(_, _, value)| BucketAuditRecord::from_bytes(value).unwrap())
            .collect();
        assert_eq!(
            records
                .iter()
                .map(|record| record.generation)
                .collect::<Vec<_>>(),
            [Some(1), Some(2)]
        );
        assert!(
            records
                .iter()
                .all(|record| record.action == AuditAction::Lock)
        );
        // A failed audit write leaves the lock in place; the sessions' timers end.
        let effects = operation.step(Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        }));
        let cancels: Vec<_> = locked
            .iter()
            .map(|ticket| {
                Effect::Task(TaskEffect::CancelTimer {
                    key: lock_timer(ticket),
                })
            })
            .collect();
        assert_eq!(effects.into_vec(), cancels);
        for ticket in &locked {
            let key = lock_timer(ticket);
            operation.step(Event::Task(aruna_core::task::TaskEvent::TimerCancelled {
                key,
            }));
        }
        assert_eq!(
            operation.finalize(),
            Ok(LockResult {
                locked,
                audited: false
            })
        );
    }

    #[test]
    fn locks_named_session() {
        let node = iroh::SecretKey::from_bytes(&[2; 32]).public();
        let mut operation = LockBucketOperation::timed(ticket(2), node);
        // The node locks by session alone, without reading the bucket.
        let effects = operation.start();
        let lock = BlobEffect::LockKey {
            bucket_id: BUCKET_ID,
            session: Some(ticket(2)),
        };
        assert_eq!(effects.as_slice(), [Effect::Blob(lock)]);
        // A stale timer locks nothing and records nothing.
        operation.step(Event::Blob(BlobEvent::KeyLocked { locked: Vec::new() }));
        assert_eq!(
            operation.finalize(),
            Ok(LockResult {
                locked: Vec::new(),
                audited: true
            })
        );
    }

    #[test]
    fn stranger_refused() {
        let (mut operation, effects) = read(Some(user(9)), None);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { .. })]
        ));
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Key::from(Vec::new()),
            value: None,
        }));
        assert_eq!(operation.finalize(), Err(LockError::NotHolder));
    }
}
