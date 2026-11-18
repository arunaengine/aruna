//! Extends a running unlock session of one key generation: the new deadline counts from now and
//! never passes the maximum counted from the session start. The session timer moves with it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::s3::bucket::audit_retry::{AuditError, AuditWrite, KeepAudit};
use crate::s3::bucket::key::lock::{answers_timer, lock_timer};
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
    BucketHolder, BucketKeyError, BucketKeyRef, HolderOrigin, KeyTicket, UnlockStatus,
    deadline_after,
};
use aruna_core::structs::storage::key_audit::{
    AuditAction, AuditOutcome, BucketAuditRecord, next_event_id,
};
use aruna_core::task::TaskEffect;
use aruna_core::types::{GroupId, Value};
use aruna_core::{NodeId, UserId};
use std::time::Duration;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExtendStep {
    ReadBucket,
    ReadGrant,
    CheckSession,
    WriteIntent,
    ExtendKey,
    MoveTimer,
    WriteAudit,
}

#[derive(Debug, Error, PartialEq)]
pub enum ExtendError {
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
    #[error("the caller holds no key of this bucket")]
    NotHolder,
    #[error("unexpected event in state {state}: expected {expected}, got {received}")]
    InvalidStateEvent {
        state: String,
        expected: &'static str,
        received: &'static str,
    },
    #[error("the extension did not finish")]
    NotFinished,
}

impl From<IoError> for ExtendError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.into())
    }
}

impl From<AuditError> for ExtendError {
    fn from(error: AuditError) -> Self {
        match error {
            AuditError::Conversion(error) => Self::Conversion(error),
            AuditError::Storage(error) => Self::Storage(error),
            AuditError::NotAuditTimer => Self::NotFinished,
            AuditError::Unexpected(received) => Self::InvalidStateEvent {
                state: "WriteAudit".to_string(),
                expected: "the audit retry timer's answer",
                received,
            },
        }
    }
}

type ExtendTurn = Result<Next<UnlockStatus>, ExtendError>;

/// An extension of `session_id` in `generation`; implicit authority is read right before it.
#[derive(Debug, PartialEq)]
pub struct ExtendInput {
    pub bucket: String,
    pub group_id: GroupId,
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub caller: UserId,
    pub generation: u64,
    pub session_id: Ulid,
    pub duration: Option<Duration>,
    pub now_ms: u64,
}

#[derive(Debug, PartialEq)]
pub struct ExtendBucketOperation {
    input: ExtendInput,
    step: ExtendStep,
    key: Option<BucketKeyRef>,
    status: Option<UnlockStatus>,
    outcome: Option<BucketAuditRecord>,
    /// Why the registry refused the extension, reported once its failed outcome is recorded.
    failure: Option<ExtendError>,
    /// The event id of the stored intent, which the outcome names.
    intent_id: Option<Ulid>,
}

impl ExtendBucketOperation {
    pub fn new(input: ExtendInput) -> Self {
        Self {
            input,
            step: ExtendStep::ReadBucket,
            key: None,
            status: None,
            outcome: None,
            failure: None,
            intent_id: None,
        }
    }

    fn call(&mut self, step: ExtendStep, call: IoCall) -> ExtendTurn {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn refused(&self, received: Event) -> ExtendTurn {
        match received {
            Event::Blob(BlobEvent::Error(error)) => Err(error.into()),
            other => Err(ExtendError::InvalidStateEvent {
                state: format!("{:?}", self.step),
                expected: "the event of the current step",
                received: other.label(),
            }),
        }
    }

    fn authorize(&mut self, values: Vec<Option<Value>>) -> ExtendTurn {
        let (realm_id, group_id) = (self.input.realm_id, self.input.group_id);
        let state = parse_authority(keyed(&values), realm_id, group_id)?;
        let bucket_id = state.settings.bucket_id.ok_or(ExtendError::NotEncrypted)?;
        self.key = Some(BucketKeyRef::new(bucket_id, self.input.generation));
        let caller = self.input.caller;
        if state.info.created_by == caller || state.admins.contains(&caller) {
            return self.check_session();
        }
        let grant = [&bucket_id.to_bytes()[..], &caller.to_storage_key()].concat();
        let read = read_row(BUCKET_HOLDER_KEYSPACE, grant);
        self.call(ExtendStep::ReadGrant, read)
    }

    fn grant_read(&mut self, value: Option<Value>) -> ExtendTurn {
        let grant = value
            .map(|value| BucketHolder::from_bytes(&value))
            .transpose()?;
        match grant {
            Some(grant) if grant.origin == HolderOrigin::Explicit => self.check_session(),
            _ => Err(ExtendError::NotHolder),
        }
    }

    /// Asks the registry first, so an extension it would refuse writes no intent at all.
    fn check_session(&mut self) -> ExtendTurn {
        let key = self.key.ok_or(ExtendError::NotFinished)?;
        let status = Effect::Blob(BlobEffect::ReadKeyStatus {
            bucket_id: key.bucket_id,
        });
        self.call(ExtendStep::CheckSession, status.into())
    }

    fn session_checked(&mut self, generations: &[UnlockStatus]) -> ExtendTurn {
        let (key, session_id) = (self.key, self.input.session_id);
        let current = generations.iter().find(|status| {
            Some(status.key) == key && status.session_id == session_id && status.active
        });
        let Some(current) = current else {
            return Err(BlobError::BucketKey(BucketKeyError::SessionMismatch).into());
        };
        let past_max = matches!(
            (self.input.duration, current.max_remaining),
            (Some(asked), Some(max)) if asked > max
        );
        if past_max {
            return Err(BlobError::BucketKey(BucketKeyError::InvalidDuration).into());
        }
        self.write_intent()
    }

    /// A synced intent records the extension before the registry applies it.
    fn write_intent(&mut self) -> ExtendTurn {
        let key = self.key.ok_or(ExtendError::NotFinished)?;
        let now_ms = self.input.now_ms;
        let deadline = self
            .input
            .duration
            .and_then(|left| deadline_after(now_ms, left));
        let intent = self.record(key, AuditOutcome::Intent, deadline);
        self.intent_id = Some(intent.event_id);
        let write = AuditWrite::new(&intent, true)?;
        self.call(ExtendStep::WriteIntent, IoCall::child(write))
    }

    fn record(
        &self,
        key: BucketKeyRef,
        outcome: AuditOutcome,
        deadline_ms: Option<u64>,
    ) -> BucketAuditRecord {
        BucketAuditRecord {
            event_id: next_event_id(self.input.now_ms),
            bucket_id: key.bucket_id,
            at_ms: self.input.now_ms,
            action: AuditAction::Extend,
            actor: Some(self.input.caller),
            node_id: self.input.node_id,
            generation: Some(key.generation),
            session_id: Some(self.input.session_id),
            intent_id: match outcome {
                AuditOutcome::Intent => None,
                AuditOutcome::Applied | AuditOutcome::Failed => self.intent_id,
            },
            sequence: None,
            deadline_ms,
            reason: None,
            outcome,
        }
    }

    /// The registry refuses a session of another generation and any bound past the maximum.
    fn extend(&mut self) -> ExtendTurn {
        let key = self.key.ok_or(ExtendError::NotFinished)?;
        let extend = Effect::Blob(BlobEffect::ExtendKey {
            key,
            session_id: self.input.session_id,
            duration: self.input.duration,
        });
        self.call(ExtendStep::ExtendKey, extend.into())
    }

    fn move_timer(&mut self, status: UnlockStatus) -> ExtendTurn {
        let foreign = Some(status.key) != self.key || status.session_id != self.input.session_id;
        if foreign {
            return self.reject(BlobError::BucketKey(BucketKeyError::SessionMismatch).into());
        }
        let ticket = KeyTicket {
            key: status.key,
            session_id: status.session_id,
        };
        let key = lock_timer(&ticket);
        let remaining = status.remaining;
        self.status = Some(status);
        let Some(after) = remaining else {
            return self.audit();
        };
        let shorten = Effect::Task(TaskEffect::ShortenTimer { key, after });
        self.call(ExtendStep::MoveTimer, shorten.into())
    }

    fn audit(&mut self) -> ExtendTurn {
        let status = self.status.as_ref().ok_or(ExtendError::NotFinished)?;
        let mut record = self.record(status.key, AuditOutcome::Applied, status.deadline_ms);
        record.sequence = Some(status.sequence);
        self.keep_outcome(record)
    }

    /// A refused extension records its failed outcome, so the synced intent never reads as
    /// applied at restart.
    fn reject(&mut self, error: ExtendError) -> ExtendTurn {
        let Some(key) = self.key else {
            return Err(error);
        };
        self.failure = Some(error);
        let record = self.record(key, AuditOutcome::Failed, None);
        self.keep_outcome(record)
    }

    fn keep_outcome(&mut self, record: BucketAuditRecord) -> ExtendTurn {
        self.outcome = Some(record.clone());
        let keep = KeepAudit::new(vec![record]);
        self.call(ExtendStep::WriteAudit, IoCall::child(keep))
    }

    /// The new deadline already applies; a lost audit write does not undo it.
    fn finish(&mut self, kept: Result<bool, AuditError>) -> ExtendTurn {
        kept?;
        if let Some(error) = self.failure.take() {
            return Err(error);
        }
        let status = self.status.take().ok_or(ExtendError::NotFinished)?;
        Ok(Next::Done(status))
    }

    fn event_answered(&mut self, event: Event) -> ExtendTurn {
        match (self.step, event) {
            (ExtendStep::CheckSession, Event::Blob(BlobEvent::KeyStatus { generations })) => {
                self.session_checked(&generations)
            }
            (ExtendStep::ExtendKey, Event::Blob(BlobEvent::KeyExtended { status })) => {
                self.move_timer(status)
            }
            (ExtendStep::ExtendKey, Event::Blob(BlobEvent::Error(error))) => {
                self.reject(error.into())
            }
            // A timer that did not move only records the lock late; admission ends on time.
            (ExtendStep::MoveTimer, Event::Task(event))
                if self.status.as_ref().is_some_and(|status| {
                    let ticket = KeyTicket {
                        key: status.key,
                        session_id: status.session_id,
                    };
                    answers_timer(&event, &lock_timer(&ticket))
                }) =>
            {
                self.audit()
            }
            (_, received) => self.refused(received),
        }
    }
}

impl Operation for ExtendBucketOperation {
    type Output = UnlockStatus;
    type Error = ExtendError;

    fn first(&mut self) -> ExtendTurn {
        let (realm_id, group_id) = (self.input.realm_id, self.input.group_id);
        let reads = authority_keys(&self.input.bucket, realm_id, group_id);
        self.call(ExtendStep::ReadBucket, IoCall::Read(reads))
    }

    fn next(&mut self, result: IoResult) -> ExtendTurn {
        match self.step {
            ExtendStep::ReadBucket => self.authorize(result.values()?),
            ExtendStep::ReadGrant => self.grant_read(result.value()?),
            // The intent must survive a crash before the registry moves the deadline.
            ExtendStep::WriteIntent => {
                result.child::<(), StorageError>()??;
                self.extend()
            }
            ExtendStep::WriteAudit => self.finish(result.child::<bool, AuditError>()?),
            _ => self.event_answered(result.event()?),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::bucket::key::lock::LockBucketOperation;
    use crate::s3::bucket::key::rows::authority_rows;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::effects::StorageEffect;
    use aruna_core::handle::Handle;
    use aruna_core::structs::storage::blob::BucketInfo;
    use aruna_core::structs::storage::encryption::{BucketEncryption, EncryptionMode};
    use aruna_core::structs::storage::format::Compression;
    use aruna_core::task::TaskEvent;
    use std::time::SystemTime;

    const BUCKET_ID: Ulid = Ulid::from_bytes([4; 16]);
    const SESSION: Ulid = Ulid::from_bytes([8; 16]);

    fn user(seed: u8) -> UserId {
        UserId::new(Ulid::from_bytes([seed; 16]), RealmId::from_bytes([1; 32]))
    }

    fn event(event: Event) -> IoResult {
        IoResult::Event(Box::new(event))
    }

    fn blob(blob: BlobEvent) -> IoResult {
        event(Event::Blob(blob))
    }

    fn intent_done(result: Result<(), StorageError>) -> IoResult {
        IoResult::Child(ChildOutcome::new(result))
    }

    fn kept(stored: bool) -> IoResult {
        IoResult::Child(ChildOutcome::new(Ok::<_, AuditError>(stored)))
    }

    fn asked(turn: &ExtendTurn) -> &Effect {
        match turn {
            Ok(Next::Call(IoCall::Effect(effect))) => effect,
            other => panic!("expected an effect call, got {other:?}"),
        }
    }

    /// The record the audit write child of `turn` stores.
    fn intent_of(turn: ExtendTurn) -> BucketAuditRecord {
        let Ok(Next::Call(IoCall::Child(mut child))) = turn else {
            panic!("expected the intent, got {turn:?}");
        };
        let effects = child.start();
        let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
            panic!("expected the intent, got {effects:?}");
        };
        BucketAuditRecord::from_bytes(&writes[0].2).unwrap()
    }

    fn read(caller: UserId) -> (ExtendBucketOperation, ExtendTurn) {
        let mut operation = ExtendBucketOperation::new(ExtendInput {
            bucket: "bucket".to_string(),
            group_id: Ulid::from_bytes([3; 16]),
            node_id: iroh::SecretKey::from_bytes(&[2; 32]).public(),
            caller,
            generation: 2,
            session_id: SESSION,
            duration: Some(Duration::from_secs(30)),
            realm_id: RealmId::from_bytes([1; 32]),
            now_ms: 1_000,
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
        let settings = BucketEncryption {
            mode: EncryptionMode::VaultLocked,
            bucket_id: Some(BUCKET_ID),
            key_generation: 3,
            ..Default::default()
        };
        let rows = authority_rows(&info, Some(&settings), &[]);
        let values = rows.into_iter().map(|(_, value)| value).collect();
        let turn = operation.next(IoResult::Values(values));
        (operation, turn)
    }

    /// Answers the registry check before the intent with the running session.
    fn checked(operation: &mut ExtendBucketOperation, turn: &ExtendTurn) -> ExtendTurn {
        let check = BlobEffect::ReadKeyStatus {
            bucket_id: BUCKET_ID,
        };
        assert_eq!(asked(turn), &Effect::Blob(check));
        let generations = vec![status(Some(Duration::from_secs(60)))];
        operation.next(blob(BlobEvent::KeyStatus { generations }))
    }

    /// Answers the synced intent of an authorized extension.
    fn synced(operation: &mut ExtendBucketOperation, turn: &ExtendTurn) -> ExtendTurn {
        let intent = intent_of(checked(operation, turn));
        assert_eq!(
            (intent.action, intent.outcome, intent.deadline_ms),
            (AuditAction::Extend, AuditOutcome::Intent, Some(31_000))
        );
        operation.next(intent_done(Ok(())))
    }

    fn status(remaining: Option<Duration>) -> UnlockStatus {
        UnlockStatus {
            key: BucketKeyRef::new(BUCKET_ID, 2),
            session_id: SESSION,
            sequence: ulid::Ulid::from_parts(1, 1),
            deadline_ms: remaining.and_then(|left| deadline_after(1_000, left)),
            active: true,
            unlocked_at: SystemTime::UNIX_EPOCH,
            remaining,
            max_remaining: Some(Duration::from_secs(90)),
        }
    }

    fn session_ticket() -> KeyTicket {
        KeyTicket {
            key: BucketKeyRef::new(BUCKET_ID, 2),
            session_id: SESSION,
        }
    }

    /// An extension whose intent is synced, waiting for the registry.
    fn extending(seconds: u64) -> ExtendBucketOperation {
        let (mut operation, turn) = read(user(1));
        operation.input.duration = Some(Duration::from_secs(seconds));
        checked(&mut operation, &turn).unwrap();
        operation.next(intent_done(Ok(()))).unwrap();
        operation
    }

    async fn on_scheduler<T: std::fmt::Debug, E: std::fmt::Debug>(
        scheduler: &aruna_tasks::TaskHandle,
        turn: Result<Next<T>, E>,
    ) -> IoResult {
        let Ok(Next::Call(IoCall::Effect(effect))) = turn else {
            panic!("expected a timer effect, got {turn:?}");
        };
        IoResult::Event(Box::new(scheduler.send_effect(*effect).await))
    }

    #[test]
    fn delayed_extension_recorded() {
        let (mut operation, turn) = read(user(1));
        operation.input.duration = Some(Duration::from_secs(60));
        let intent = intent_of(checked(&mut operation, &turn));
        assert_eq!(intent.deadline_ms, Some(61_000));
        operation.next(intent_done(Ok(()))).unwrap();
        let mut current = status(Some(Duration::from_secs(60)));
        current.deadline_ms = Some(91_000);
        operation
            .next(blob(BlobEvent::KeyExtended {
                status: current.clone(),
            }))
            .unwrap();
        let armed = TaskEvent::TimerScheduled {
            key: lock_timer(&session_ticket()),
            after: Duration::from_secs(60),
        };
        operation.next(event(Event::Task(armed))).unwrap();
        let record = operation.outcome.clone().unwrap();
        assert_eq!(
            (record.deadline_ms, record.sequence),
            (Some(91_000), Some(current.sequence))
        );
        let mut unlocked = intent.clone();
        unlocked.event_id = Ulid::from_parts(0, 1);
        unlocked.action = AuditAction::Unlock;
        unlocked.outcome = AuditOutcome::Applied;
        unlocked.sequence = Some(Ulid::from_parts(0, 1));
        assert_eq!(
            crate::s3::bucket::key::restart::replay(&[unlocked.clone(), intent.clone()], 76_000),
            [2]
        );
        assert_eq!(
            crate::s3::bucket::key::restart::replay(&[unlocked, intent, record], 76_000),
            [2]
        );
    }

    struct TimerSender(tokio::sync::mpsc::Sender<aruna_core::task::TaskKey>);

    #[async_trait::async_trait]
    impl aruna_tasks::InboundTaskHandler for TimerSender {
        async fn handle_timer(&self, key: aruna_core::task::TaskKey) {
            self.0.send(key).await.unwrap();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn reversed_extensions_shorten() {
        use aruna_core::handle::Handle;
        for remaining in [None, Some(Duration::from_secs(90))] {
            let (mut first, _) = read(user(1));
            let (mut second, _) = read(user(1));
            first.step = ExtendStep::ExtendKey;
            second.step = ExtendStep::ExtendKey;
            let mut older = first.step(Event::Blob(BlobEvent::KeyExtended {
                status: status(remaining),
            }));
            let mut newer = second.step(Event::Blob(BlobEvent::KeyExtended {
                status: status(Some(Duration::from_secs(30))),
            }));
            let scheduler = aruna_tasks::TaskHandle::new();
            let (sender, mut fired) = tokio::sync::mpsc::channel(2);
            scheduler
                .set_inbound_handler(std::sync::Arc::new(TimerSender(sender)))
                .await;
            let start = tokio::time::Instant::now();
            second.step(scheduler.send_effect(newer.pop().unwrap()).await);
            if remaining.is_some() {
                first.step(scheduler.send_effect(older.pop().unwrap()).await);
            } else {
                assert!(matches!(
                    older.as_slice(),
                    [Effect::Storage(StorageEffect::Write { .. })]
                ));
            }
            let ticket = KeyTicket {
                key: status(None).key,
                session_id: SESSION,
            };
            assert_eq!(fired.recv().await.unwrap(), lock_timer(&ticket));
            assert_eq!(tokio::time::Instant::now() - start, Duration::from_secs(30));
            scheduler.shutdown(Duration::ZERO).await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn reversed_rearms_shorten() {
        use crate::s3::bucket::key::lock::LockBucketOperation;
        use aruna_core::handle::Handle;
        let ticket = KeyTicket {
            key: status(None).key,
            session_id: SESSION,
        };
        let mut callback =
            LockBucketOperation::timed(ticket, iroh::SecretKey::from_bytes(&[2; 32]).public());
        callback.start();
        let mut stale = callback.step(Event::Blob(BlobEvent::KeyLocked {
            locked: Vec::new(),
            sequence: Ulid::nil(),
            live: Some(Box::new(status(Some(Duration::from_secs(90))))),
        }));
        let (mut extension, _) = read(user(1));
        extension.step = ExtendStep::ExtendKey;
        let mut newer = extension.step(Event::Blob(BlobEvent::KeyExtended {
            status: status(Some(Duration::from_secs(30))),
        }));
        let scheduler = aruna_tasks::TaskHandle::new();
        let (sender, mut fired) = tokio::sync::mpsc::channel(2);
        scheduler
            .set_inbound_handler(std::sync::Arc::new(TimerSender(sender)))
            .await;
        let start = tokio::time::Instant::now();
        extension.step(scheduler.send_effect(newer.pop().unwrap()).await);
        callback.step(scheduler.send_effect(stale.pop().unwrap()).await);
        assert_eq!(fired.recv().await.unwrap(), lock_timer(&ticket));
        assert_eq!(tokio::time::Instant::now() - start, Duration::from_secs(30));
        scheduler.shutdown(Duration::ZERO).await;
    }

    #[tokio::test(start_paused = true)]
    async fn reversed_timers_rearm() {
        use crate::s3::bucket::key::lock::LockBucketOperation;
        use aruna_core::handle::Handle;
        let ready = |seconds| {
            let (mut operation, effects) = read(user(1));
            operation.input.duration = Some(Duration::from_secs(seconds));
            checked(&mut operation, &effects);
            operation.step(Event::Storage(StorageEvent::WriteResult {
                key: Vec::new().into(),
            }));
            operation.step(Event::Storage(StorageEvent::SyncAllFinished));
            operation
        };
        let (mut first, mut second) = (ready(30), ready(90));
        let mut short = status(Some(Duration::from_secs(30)));
        short.sequence = Ulid::from_parts(1, 1);
        let mut long = status(Some(Duration::from_secs(90)));
        long.sequence = Ulid::from_parts(1, 2);
        let first_reset = first.step(Event::Blob(BlobEvent::KeyExtended { status: short }));
        let second_reset = second.step(Event::Blob(BlobEvent::KeyExtended {
            status: long.clone(),
        }));
        let scheduler = aruna_tasks::TaskHandle::new();
        let (sender, mut fired) = tokio::sync::mpsc::channel(2);
        scheduler
            .set_inbound_handler(std::sync::Arc::new(TimerSender(sender)))
            .await;
        let start = tokio::time::Instant::now();
        for (operation, mut effects) in [(&mut second, second_reset), (&mut first, first_reset)] {
            let event = scheduler.send_effect(effects.pop().unwrap()).await;
            operation.step(event);
        }
        let ticket = KeyTicket {
            key: long.key,
            session_id: long.session_id,
        };
        assert_eq!(fired.recv().await.unwrap(), lock_timer(&ticket));
        assert_eq!(tokio::time::Instant::now() - start, Duration::from_secs(30));
        let mut callback =
            LockBucketOperation::timed(ticket, iroh::SecretKey::from_bytes(&[2; 32]).public());
        callback.start();
        long.remaining = Some(Duration::from_secs(60));
        let mut effects = callback.step(Event::Blob(BlobEvent::KeyLocked {
            locked: Vec::new(),
            sequence: Ulid::nil(),
            live: Some(Box::new(long)),
        }));
        let event = scheduler.send_effect(effects.pop().unwrap()).await;
        callback.step(event);
        assert!(callback.finalize().unwrap().locked.is_empty());
        assert_eq!(fired.recv().await.unwrap(), lock_timer(&ticket));
        assert_eq!(tokio::time::Instant::now() - start, Duration::from_secs(90));
        let mut callback =
            LockBucketOperation::timed(ticket, iroh::SecretKey::from_bytes(&[2; 32]).public());
        callback.start();
        let effects = callback.step(Event::Blob(BlobEvent::KeyLocked {
            locked: vec![ticket],
            sequence: Ulid::from_parts(1, 3),
            live: None,
        }));
        let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
            panic!("timed audit missing")
        };
        let record = BucketAuditRecord::from_bytes(&writes[0].2).unwrap();
        assert_eq!(record.action, AuditAction::TimedLock);
        scheduler.shutdown(Duration::ZERO).await;
    }

    #[test]
    fn extends_source_generation() {
        // Generation 2 is a source generation while 3 is active; its session is extended.
        let (mut operation, effects) = read(user(1));
        // The deadline moves only after its intent reached the disk.
        let effects = synced(&mut operation, &effects);
        let extend = BlobEffect::ExtendKey {
            key: BucketKeyRef::new(BUCKET_ID, 2),
            session_id: SESSION,
            duration: Some(Duration::from_secs(30)),
        };
        assert_eq!(effects.as_slice(), [Effect::Blob(extend)]);
        let remaining = Some(Duration::from_secs(30));
        let effects = operation.step(Event::Blob(BlobEvent::KeyExtended {
            status: status(remaining),
        }));
        let ticket = KeyTicket {
            key: BucketKeyRef::new(BUCKET_ID, 2),
            session_id: SESSION,
        };
        let reset = TaskEffect::ShortenTimer {
            key: lock_timer(&ticket),
            after: Duration::from_secs(30),
        };
        assert_eq!(effects.as_slice(), [Effect::Task(reset)]);
        let effects = operation.step(Event::Task(aruna_core::task::TaskEvent::TimerScheduled {
            key: lock_timer(&ticket),
            after: Duration::from_secs(30),
        }));
        let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
            panic!("expected the audit record, got {effects:?}");
        };
        let record = BucketAuditRecord::from_bytes(value).unwrap();
        assert_eq!(
            (record.action, record.outcome, record.deadline_ms),
            (AuditAction::Extend, AuditOutcome::Applied, Some(31_000))
        );
        // A short outage on the outcome write is retried with the same record.
        let error = StorageError::Timeout;
        let effects = operation.step(Event::Storage(StorageEvent::Error { error }));
        let [Effect::Storage(StorageEffect::Write { value: retried, .. })] = effects.as_slice()
        else {
            panic!("expected the retried record, got {effects:?}");
        };
        assert_eq!(BucketAuditRecord::from_bytes(retried).unwrap(), record);
        operation.step(Event::Storage(StorageEvent::WriteResult {
            key: Key::from(Vec::new()),
        }));
        assert_eq!(operation.finalize(), Ok(status(remaining)));
    }

    #[test]
    fn refuses_bad_extensions() {
        let (mut operation, effects) = read(user(1));
        synced(&mut operation, &effects);
        let mismatch = || BlobError::BucketKey(BucketKeyError::SessionMismatch);
        let effects = operation.step(Event::Blob(BlobEvent::Error(mismatch())));
        // The refused extension records a failed outcome before the error is reported.
        let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
            panic!("expected the failed outcome, got {effects:?}");
        };
        let record = BucketAuditRecord::from_bytes(value).unwrap();
        assert_eq!(
            (record.action, record.outcome, record.session_id),
            (AuditAction::Extend, AuditOutcome::Failed, Some(SESSION))
        );
        // Only the write's own answer completes it.
        let stray = Event::Blob(BlobEvent::KeyDiscarded {
            ticket: KeyTicket {
                key: BucketKeyRef::new(BUCKET_ID, 2),
                session_id: SESSION,
            },
        });
        let (mut wrong, effects) = read(user(1));
        let effects_wrong = synced(&mut wrong, &effects);
        assert!(!effects_wrong.is_empty());
        wrong.step(Event::Blob(BlobEvent::Error(mismatch())));
        wrong.step(stray);
        assert!(matches!(
            wrong.finalize(),
            Err(ExtendError::InvalidStateEvent { .. })
        ));
        operation.step(Event::Storage(StorageEvent::WriteResult {
            key: Key::from(Vec::new()),
        }));
        assert_eq!(operation.finalize(), Err(ExtendError::Blob(mismatch())));

        let (mut operation, effects) = read(user(9));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { .. })]
        ));
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Key::from(Vec::new()),
            value: None,
        }));
        assert_eq!(operation.finalize(), Err(ExtendError::NotHolder));

        // An intent that cannot be stored leaves the deadline as it was.
        let (mut operation, effects) = read(user(1));
        checked(&mut operation, &effects);
        let error = StorageError::Timeout;
        let effects = operation.step(Event::Storage(StorageEvent::Error { error }));
        assert!(effects.is_empty());
        assert_eq!(
            operation.finalize(),
            Err(ExtendError::Storage(StorageError::Timeout))
        );
    }

    #[test]
    fn registry_refusal_unrecorded() {
        // A session the registry does not hold, or a bound past its maximum, writes no intent.
        let other = UnlockStatus {
            session_id: Ulid::from_bytes([9; 16]),
            ..status(None)
        };
        let short = UnlockStatus {
            max_remaining: Some(Duration::from_secs(10)),
            ..status(None)
        };
        let refusals = [
            (other, BucketKeyError::SessionMismatch),
            (short, BucketKeyError::InvalidDuration),
        ];
        for (generation, error) in refusals {
            let (mut operation, _) = read(user(1));
            let generations = vec![generation];
            let effects = operation.step(Event::Blob(BlobEvent::KeyStatus { generations }));
            assert!(effects.is_empty(), "{effects:?}");
            let refused = ExtendError::Blob(BlobError::BucketKey(error));
            assert_eq!(operation.finalize(), Err(refused));
        }
    }

    #[test]
    fn kept_failure_reconciles() {
        // The registry refuses after the intent; every write of the failed outcome fails.
        let (mut operation, effects) = read(user(1));
        let intent = match checked(&mut operation, &effects).as_slice() {
            [Effect::Storage(StorageEffect::Write { value, .. })] => {
                BucketAuditRecord::from_bytes(value).unwrap()
            }
            other => panic!("expected the intent, got {other:?}"),
        };
        operation.step(Event::Storage(StorageEvent::WriteResult {
            key: Key::from(Vec::new()),
        }));
        operation.step(Event::Storage(StorageEvent::SyncAllFinished));
        let mismatch = || BlobError::BucketKey(BucketKeyError::SessionMismatch);
        let mut effects = operation.step(Event::Blob(BlobEvent::Error(mismatch())));
        for _ in 0..AUDIT_ATTEMPTS {
            let error = StorageError::Timeout;
            effects = operation.step(Event::Storage(StorageEvent::Error { error }));
        }
        // The failed outcome moves to its own retry timer instead of being dropped.
        let [Effect::Task(TaskEffect::ResetTimer { key, .. })] = effects.as_slice() else {
            panic!("expected the retry timer, got {effects:?}");
        };
        let aruna_core::task::TaskKey::RecordAudit { record } = key.clone() else {
            panic!("expected an audit record timer, got {key:?}");
        };
        assert_eq!(
            (record.outcome, record.intent_id),
            (AuditOutcome::Failed, Some(intent.event_id))
        );
        let after = crate::s3::bucket::audit_retry::AUDIT_RETRY;
        let scheduled = aruna_core::task::TaskEvent::TimerScheduled {
            key: key.clone(),
            after,
        };
        operation.step(Event::Task(scheduled));
        assert_eq!(operation.finalize(), Err(ExtendError::Blob(mismatch())));

        // After a restart the kept record is stored; the session that had ended stays ended.
        let unlock = BucketAuditRecord {
            event_id: Ulid::from_parts(1, 0),
            action: AuditAction::Unlock,
            outcome: AuditOutcome::Applied,
            sequence: None,
            deadline_ms: Some(1_000),
            intent_id: None,
            ..intent.clone()
        };
        let mut trail = vec![unlock, intent, *record];
        trail.sort_by_key(BucketAuditRecord::key);
        let open = crate::s3::bucket::key::restart::replay(&trail, 2_000);
        assert!(open.is_empty(), "{open:?}");
    }
}
