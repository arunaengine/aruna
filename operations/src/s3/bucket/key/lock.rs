//! Locks a bucket: every unlocked generation stops admitting reads at once, while admitted
//! reads finish. The lock applies before its audit, so a failed audit write never undoes it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::s3::bucket::audit_retry::{AUDIT_ATTEMPTS, AuditWrite, retry_effect, retry_timer};
use crate::s3::bucket::key::rows::{SettingsError, authority_keys, parse_authority};
use crate::s3::object::put::abe::keyed;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event};
use aruna_core::keyspaces::BUCKET_HOLDER_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::rows::read_row;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::encryption::{
    BucketHolder, HolderOrigin, KeyTicket, UnlockStatus,
};
use aruna_core::structs::storage::key_audit::{
    AuditAction, AuditOutcome, BucketAuditRecord, next_event_id,
};
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::types::{GroupId, Value};
use aruna_core::{NodeId, UserId};
use std::collections::VecDeque;
use thiserror::Error;
use ulid::Ulid;

/// The record of a lock of `ticket`, timed when there is no actor.
fn lock_record(
    ticket: KeyTicket,
    actor: Option<UserId>,
    node_id: NodeId,
    at_ms: u64,
) -> BucketAuditRecord {
    BucketAuditRecord {
        event_id: next_event_id(at_ms),
        bucket_id: ticket.key.bucket_id,
        at_ms,
        action: match actor {
            Some(_) => AuditAction::Lock,
            None => AuditAction::TimedLock,
        },
        actor,
        node_id,
        generation: Some(ticket.key.generation),
        session_id: Some(ticket.session_id),
        intent_id: None,
        sequence: None,
        deadline_ms: None,
        reason: None,
        outcome: AuditOutcome::Applied,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LockStep {
    ReadBucket,
    ReadGrant,
    LockKeys,
    RearmTimer,
    WriteAudit,
    ArmRetries,
    CancelTimers,
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
    #[error("unexpected event in state {state}: expected {expected}, got {received}")]
    InvalidStateEvent {
        state: String,
        expected: &'static str,
        received: &'static str,
    },
    #[error("the lock did not finish")]
    NotFinished,
}

impl From<IoError> for LockError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.into())
    }
}

type LockTurn = Result<Next<LockResult>, LockError>;

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
    /// The records of the audit batch, kept for the retry timers if the batch is not stored.
    records: Vec<BucketAuditRecord>,
    attempts: u32,
    /// Timer effects still to send, one at a time, and the timer the running one names.
    timers: VecDeque<(Effect, TaskKey)>,
    waiting: Option<TaskKey>,
}

impl LockBucketOperation {
    pub fn new(input: LockInput) -> Self {
        Self {
            input,
            step: LockStep::ReadBucket,
            bucket_id: None,
            locked: Vec::new(),
            audited: false,
            records: Vec::new(),
            attempts: 0,
            timers: VecDeque::new(),
            waiting: None,
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

    fn call(&mut self, step: LockStep, call: IoCall) -> LockTurn {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn refused(&self, received: Event) -> LockTurn {
        match received {
            Event::Blob(BlobEvent::Error(error)) => Err(error.into()),
            other => Err(LockError::InvalidStateEvent {
                state: format!("{:?}", self.step),
                expected: "the event of the current step",
                received: other.label(),
            }),
        }
    }

    fn authorize(&mut self, values: Vec<Option<Value>>) -> LockTurn {
        let (realm_id, group_id) = (self.input.realm_id, self.input.group_id);
        let state = parse_authority(keyed(&values), realm_id, group_id)?;
        let bucket_id = state.settings.bucket_id.ok_or(LockError::NotEncrypted)?;
        self.bucket_id = Some(bucket_id);
        let Some(caller) = self.input.caller else {
            return self.lock();
        };
        if state.info.created_by == caller || state.admins.contains(&caller) {
            return self.lock();
        }
        let grant = [&bucket_id.to_bytes()[..], &caller.to_storage_key()].concat();
        let read = read_row(BUCKET_HOLDER_KEYSPACE, grant);
        self.call(LockStep::ReadGrant, read)
    }

    fn grant_read(&mut self, value: Option<Value>) -> LockTurn {
        let grant = value
            .map(|value| BucketHolder::from_bytes(&value))
            .transpose()?;
        match grant {
            Some(grant) if grant.origin == HolderOrigin::Explicit => self.lock(),
            _ => Err(LockError::NotHolder),
        }
    }

    fn lock(&mut self) -> LockTurn {
        let bucket_id = self.bucket_id.ok_or(LockError::NotFinished)?;
        let lock = Effect::Blob(BlobEffect::LockKey {
            bucket_id,
            session: self.input.session,
        });
        self.call(LockStep::LockKeys, lock.into())
    }

    fn audit(
        &mut self,
        locked: Vec<KeyTicket>,
        sequence: Ulid,
        live: Option<UnlockStatus>,
    ) -> LockTurn {
        let foreign = |ticket: &KeyTicket| Some(ticket.key.bucket_id) != self.bucket_id;
        if locked.iter().any(foreign) {
            return Err(LockError::NotFinished);
        }
        self.locked = locked;
        if self.locked.is_empty() && self.input.session.is_some() {
            return self.rearm(live);
        }
        let (actor, node_id, now_ms) = (self.input.caller, self.input.node_id, self.input.now_ms);
        self.records = (self.locked.iter())
            .map(|ticket| {
                let mut record = lock_record(*ticket, actor, node_id, now_ms);
                record.sequence = Some(sequence);
                record
            })
            .collect();
        if self.records.is_empty() {
            return self.finish(true);
        }
        self.write_audit()
    }

    fn rearm(&mut self, live: Option<UnlockStatus>) -> LockTurn {
        let ticket = self.input.session.ok_or(LockError::NotFinished)?;
        let live = live.filter(|status| {
            status.active && status.key == ticket.key && status.session_id == ticket.session_id
        });
        let Some(after) = live.and_then(|status| status.remaining) else {
            return self.finish(true);
        };
        let key = lock_timer(&ticket);
        self.waiting = Some(key.clone());
        let arm = Effect::Task(TaskEffect::ShortenTimer { key, after });
        self.call(LockStep::RearmTimer, arm.into())
    }

    /// The audit records stay with the operation until written, so a failed write is retried.
    fn write_audit(&mut self) -> LockTurn {
        let write = AuditWrite::batch(&self.records, false)?;
        self.attempts += 1;
        self.call(LockStep::WriteAudit, IoCall::child(write))
    }

    /// The lock already applied; it stays even when its audit cannot be written.
    fn audit_written(&mut self, written: Result<(), StorageError>) -> LockTurn {
        if written.is_ok() {
            return self.finish(true);
        }
        if self.attempts < AUDIT_ATTEMPTS {
            return self.write_audit();
        }
        self.arm_retries()
    }

    /// Records still not stored move to retry timers that write them until storage answers.
    fn arm_retries(&mut self) -> LockTurn {
        self.timers = (self.records.iter())
            .map(|record| (retry_effect(record), retry_timer(record)))
            .collect();
        self.step = LockStep::ArmRetries;
        self.next_timer()
    }

    /// Sends the next timer effect, or moves on once all of them were answered.
    fn next_timer(&mut self) -> LockTurn {
        if let Some((effect, key)) = self.timers.pop_front() {
            self.waiting = Some(key);
            return Ok(Next::Call(effect.into()));
        }
        self.waiting = None;
        match self.step {
            LockStep::ArmRetries => self.finish(false),
            _ => self.complete(),
        }
    }

    /// A manual lock ends the timers of the sessions it locked; a timed lock is its own timer.
    fn finish(&mut self, audited: bool) -> LockTurn {
        self.audited = audited;
        if self.input.caller.is_none() {
            return self.complete();
        }
        self.timers = (self.locked.iter())
            .map(|ticket| {
                let key = lock_timer(ticket);
                let cancel = TaskEffect::CancelTimer { key: key.clone() };
                (Effect::Task(cancel), key)
            })
            .collect();
        self.step = LockStep::CancelTimers;
        self.next_timer()
    }

    fn complete(&mut self) -> LockTurn {
        Ok(Next::Done(LockResult {
            locked: std::mem::take(&mut self.locked),
            audited: self.audited,
        }))
    }

    fn answers_waiting(&self, event: &TaskEvent) -> bool {
        (self.waiting.as_ref()).is_some_and(|key| answers_timer(event, key))
    }

    fn event_answered(&mut self, event: Event) -> LockTurn {
        match (self.step, event) {
            (
                LockStep::LockKeys,
                Event::Blob(BlobEvent::KeyLocked {
                    locked,
                    sequence,
                    live,
                }),
            ) => self.audit(locked, sequence, live.map(|status| *status)),
            (LockStep::RearmTimer, Event::Task(event)) if self.answers_waiting(&event) => {
                self.finish(true)
            }
            // A timer that cannot be cancelled fires later and finds its session gone.
            (LockStep::ArmRetries | LockStep::CancelTimers, Event::Task(event))
                if self.answers_waiting(&event) =>
            {
                self.next_timer()
            }
            (_, received) => self.refused(received),
        }
    }
}

impl Operation for LockBucketOperation {
    type Output = LockResult;
    type Error = LockError;

    fn first(&mut self) -> LockTurn {
        if self.input.caller.is_none() && self.bucket_id.is_some() {
            return self.lock();
        }
        let (realm_id, group_id) = (self.input.realm_id, self.input.group_id);
        let reads = authority_keys(&self.input.bucket, realm_id, group_id);
        self.call(LockStep::ReadBucket, IoCall::Read(reads))
    }

    fn next(&mut self, result: IoResult) -> LockTurn {
        match self.step {
            LockStep::ReadBucket => self.authorize(result.values()?),
            LockStep::ReadGrant => self.grant_read(result.value()?),
            LockStep::WriteAudit => self.audit_written(result.child::<(), StorageError>()?),
            _ => self.event_answered(result.event()?),
        }
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
    use crate::s3::bucket::audit_retry::AUDIT_RETRY;
    use crate::s3::bucket::key::rows::authority_rows;
    use aruna_core::calls::ChildOutcome;
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

    fn event(event: Event) -> IoResult {
        IoResult::Event(Box::new(event))
    }

    fn asked(turn: &LockTurn) -> &Effect {
        match turn {
            Ok(Next::Call(IoCall::Effect(effect))) => effect,
            other => panic!("expected an effect call, got {other:?}"),
        }
    }

    fn write_done(result: Result<(), StorageError>) -> IoResult {
        IoResult::Child(ChildOutcome::new(result))
    }

    fn read(caller: Option<UserId>, session: Option<KeyTicket>) -> (LockBucketOperation, LockTurn) {
        let mut operation = LockBucketOperation::new(LockInput {
            bucket: "bucket".to_string(),
            group_id: Ulid::from_bytes([3; 16]),
            node_id: iroh::SecretKey::from_bytes(&[2; 32]).public(),
            caller,
            session,
            realm_id: RealmId::from_bytes([1; 32]),
            now_ms: 5,
        });
        operation.first().unwrap();
        let info = BucketInfo {
            group_id: Ulid::from_bytes([3; 16]),
            created_at: SystemTime::UNIX_EPOCH,
            created_by: user(1),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: None,
        };
        // Mode off with retained keys: a decrypting change may still hold source generations.
        let settings = BucketEncryption {
            mode: EncryptionMode::Off,
            bucket_id: Some(BUCKET_ID),
            key_generation: 2,
            ..Default::default()
        };
        let values = authority_rows(&info, Some(&settings), &[]);
        let values = values.into_iter().map(|(_, value)| value).collect();
        let turn = operation.next(IoResult::Values(values));
        (operation, turn)
    }

    fn ticket(generation: u64) -> KeyTicket {
        KeyTicket {
            key: BucketKeyRef::new(BUCKET_ID, generation),
            session_id: Ulid::from_bytes([generation as u8; 16]),
        }
    }

    #[test]
    fn locks_every_generation() {
        let (mut operation, turn) = read(Some(user(1)), None);
        let lock = BlobEffect::LockKey {
            bucket_id: BUCKET_ID,
            session: None,
        };
        assert_eq!(asked(&turn), &Effect::Blob(lock));
        let locked = vec![ticket(1), ticket(2)];
        let turn = operation.next(event(Event::Blob(BlobEvent::KeyLocked {
            locked: locked.clone(),
            sequence: Ulid::from_parts(1, 1),
            live: None,
        })));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
        let records = operation.records.clone();
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
        // A failed audit write is retried, and a lasting failure leaves the lock in place.
        for _ in 1..AUDIT_ATTEMPTS {
            let retry = operation.next(write_done(Err(StorageError::Timeout)));
            assert!(matches!(retry, Ok(Next::Call(IoCall::Child(_)))));
        }
        // The records move to retry timers that keep writing them; the lock stays in place.
        let mut turn = operation.next(write_done(Err(StorageError::Timeout)));
        let mut kept = Vec::new();
        for _ in &records {
            let Effect::Task(TaskEffect::ResetTimer { key, after }) = asked(&turn).clone() else {
                panic!("expected a retry timer, got {turn:?}");
            };
            assert_eq!(after, AUDIT_RETRY);
            let TaskKey::RecordAudit { record } = &key else {
                panic!("expected an audit record timer, got {key:?}");
            };
            // Each timer keeps the very record that was not stored.
            kept.push((**record).clone());
            turn = operation.next(event(Event::Task(TaskEvent::TimerScheduled { key, after })));
        }
        assert_eq!(kept, records);
        // The sessions' timers end once the records are handed to their retry timers.
        for ticket in &locked {
            let key = lock_timer(ticket);
            assert_eq!(
                asked(&turn),
                &Effect::Task(TaskEffect::CancelTimer { key: key.clone() })
            );
            turn = operation.next(event(Event::Task(TaskEvent::TimerCancelled { key })));
        }
        assert_eq!(
            turn,
            Ok(Next::Done(LockResult {
                locked,
                audited: false
            }))
        );
    }

    #[test]
    fn locks_named_session() {
        let node = iroh::SecretKey::from_bytes(&[2; 32]).public();
        let mut operation = LockBucketOperation::timed(ticket(2), node);
        // The node locks by session alone, without reading the bucket.
        let turn = operation.first();
        let lock = BlobEffect::LockKey {
            bucket_id: BUCKET_ID,
            session: Some(ticket(2)),
        };
        assert_eq!(asked(&turn), &Effect::Blob(lock));
        // A stale timer locks nothing and records nothing.
        let turn = operation.next(event(Event::Blob(BlobEvent::KeyLocked {
            locked: Vec::new(),
            sequence: Ulid::nil(),
            live: None,
        })));
        assert_eq!(
            turn,
            Ok(Next::Done(LockResult {
                locked: Vec::new(),
                audited: true
            }))
        );
    }

    #[test]
    fn stranger_refused() {
        let (mut operation, turn) = read(Some(user(9)), None);
        assert!(matches!(turn, Ok(Next::Call(IoCall::Read(_)))));
        assert_eq!(
            operation.next(IoResult::Values(vec![None])),
            Err(LockError::NotHolder)
        );
    }
}
