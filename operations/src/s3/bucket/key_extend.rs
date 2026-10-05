//! Extends a running unlock session of one key generation: the new deadline counts from now and
//! never passes the maximum counted from the session start. The session timer moves with it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::s3::bucket::key_lock::{AUDIT_ATTEMPTS, answers_timer, lock_timer};
use crate::s3::bucket::key_rows::{SettingsError, authority_read, parse_authority};
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{BUCKET_AUDIT_KEYSPACE, BUCKET_HOLDER_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::encryption::{
    BucketHolder, BucketKeyError, BucketKeyRef, HolderOrigin, KeyTicket, UnlockStatus,
    deadline_after,
};
use aruna_core::structs::storage::key_audit::{AuditAction, AuditOutcome, BucketAuditRecord};
use aruna_core::task::TaskEffect;
use aruna_core::types::{Effects, GroupId, Key, Value};
use aruna_core::{NodeId, UserId};
use smallvec::smallvec;
use std::time::Duration;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExtendStep {
    Init,
    ReadBucket,
    ReadGrant,
    WriteIntent,
    SyncIntent,
    ExtendKey,
    MoveTimer,
    WriteAudit,
    Finish,
    Error,
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
    #[error("unexpected event in state {state}: expected {expected}, got {received:?}")]
    InvalidStateEvent {
        state: String,
        expected: &'static str,
        received: Event,
    },
    #[error("the extension did not finish")]
    NotFinished,
}

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
    /// The outcome record until it is written, and the writes tried so far.
    outcome: Option<BucketAuditRecord>,
    /// Why the registry refused the extension, reported once its failed outcome is recorded.
    failure: Option<ExtendError>,
    attempts: u32,
    output: Option<Result<UnlockStatus, ExtendError>>,
}

impl ExtendBucketOperation {
    pub fn new(input: ExtendInput) -> Self {
        Self {
            input,
            step: ExtendStep::Init,
            key: None,
            status: None,
            outcome: None,
            failure: None,
            attempts: 0,
            output: None,
        }
    }

    fn fail(&mut self, error: impl Into<ExtendError>) -> Effects {
        self.output = Some(Err(error.into()));
        self.step = ExtendStep::Error;
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
            return self.fail(ExtendError::NotEncrypted);
        };
        self.key = Some(BucketKeyRef::new(bucket_id, self.input.generation));
        let caller = self.input.caller;
        if state.info.created_by == caller || state.admins.contains(&caller) {
            return self.write_intent();
        }
        self.step = ExtendStep::ReadGrant;
        let grant = [&bucket_id.to_bytes()[..], &caller.to_storage_key()].concat();
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BUCKET_HOLDER_KEYSPACE.to_string(),
            key: grant.into(),
            txn_id: None,
        })]
    }

    /// A synced intent records the extension before the registry applies it.
    fn write_intent(&mut self) -> Effects {
        let Some(key) = self.key else {
            return self.fail(ExtendError::NotFinished);
        };
        let now_ms = self.input.now_ms;
        let deadline = self
            .input
            .duration
            .and_then(|left| deadline_after(now_ms, left));
        let intent = self.record(key, AuditOutcome::Intent, deadline);
        self.step = ExtendStep::WriteIntent;
        self.write_record(&intent)
    }

    fn record(
        &self,
        key: BucketKeyRef,
        outcome: AuditOutcome,
        deadline_ms: Option<u64>,
    ) -> BucketAuditRecord {
        BucketAuditRecord {
            event_id: Ulid::generate(),
            bucket_id: key.bucket_id,
            at_ms: self.input.now_ms,
            action: AuditAction::Extend,
            actor: Some(self.input.caller),
            node_id: self.input.node_id,
            generation: Some(key.generation),
            session_id: Some(self.input.session_id),
            deadline_ms,
            reason: None,
            outcome,
        }
    }

    fn write_record(&mut self, record: &BucketAuditRecord) -> Effects {
        match record.to_bytes() {
            Ok(value) => smallvec![Effect::Storage(StorageEffect::Write {
                key_space: BUCKET_AUDIT_KEYSPACE.to_string(),
                key: record.key().into(),
                value: value.into(),
                txn_id: None,
            })],
            Err(error) => self.fail(error),
        }
    }

    /// The registry refuses a session of another generation and any bound past the maximum.
    fn extend(&mut self) -> Effects {
        let Some(key) = self.key else {
            return self.fail(ExtendError::NotFinished);
        };
        self.step = ExtendStep::ExtendKey;
        smallvec![Effect::Blob(BlobEffect::ExtendKey {
            key,
            session_id: self.input.session_id,
            duration: self.input.duration,
        })]
    }

    fn move_timer(&mut self, status: UnlockStatus) -> Effects {
        let foreign = Some(status.key) != self.key || status.session_id != self.input.session_id;
        if foreign {
            return self.reject(BlobError::BucketKey(BucketKeyError::SessionMismatch).into());
        }
        let ticket = KeyTicket {
            key: status.key,
            session_id: status.session_id,
        };
        let key = lock_timer(&ticket);
        let effect = match status.remaining {
            Some(after) => TaskEffect::ResetTimer { key, after },
            // An unlock without a maximum lasts until lock or restart, so no timer runs.
            None => TaskEffect::CancelTimer { key },
        };
        self.status = Some(status);
        self.step = ExtendStep::MoveTimer;
        smallvec![Effect::Task(effect)]
    }

    fn audit(&mut self) -> Effects {
        let Some(status) = self.status.as_ref() else {
            return self.fail(ExtendError::NotFinished);
        };
        let now_ms = self.input.now_ms;
        let deadline = status
            .remaining
            .and_then(|left| deadline_after(now_ms, left));
        self.outcome = Some(self.record(status.key, AuditOutcome::Applied, deadline));
        self.retry_audit()
    }

    /// A refused extension records its failed outcome, so the synced intent never reads as
    /// applied at restart.
    fn reject(&mut self, error: ExtendError) -> Effects {
        let Some(key) = self.key else {
            return self.fail(error);
        };
        self.failure = Some(error);
        self.outcome = Some(self.record(key, AuditOutcome::Failed, None));
        self.retry_audit()
    }

    fn retry_audit(&mut self) -> Effects {
        let Some(record) = self.outcome.clone() else {
            return self.fail(ExtendError::NotFinished);
        };
        self.attempts += 1;
        self.step = ExtendStep::WriteAudit;
        self.write_record(&record)
    }

    /// The new deadline already applies; a lost audit write does not undo it.
    fn finish(&mut self) -> Effects {
        self.output = match self.failure.take() {
            Some(error) => Some(Err(error)),
            None => self.status.take().map(Ok),
        };
        self.step = ExtendStep::Finish;
        smallvec![]
    }
}

impl Operation for ExtendBucketOperation {
    type Output = UnlockStatus;
    type Error = ExtendError;

    fn start(&mut self) -> Effects {
        self.step = ExtendStep::ReadBucket;
        let (realm_id, group_id) = (self.input.realm_id, self.input.group_id);
        smallvec![authority_read(&self.input.bucket, realm_id, group_id, None)]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (self.step, event) {
            (ExtendStep::WriteAudit, Event::Storage(StorageEvent::Error { .. }))
                if self.attempts < AUDIT_ATTEMPTS =>
            {
                self.retry_audit()
            }
            // A lasting outage leaves the synced intent; only the write's own answers count.
            (
                ExtendStep::WriteAudit,
                Event::Storage(StorageEvent::WriteResult { .. } | StorageEvent::Error { .. }),
            ) => self.finish(),
            (ExtendStep::WriteIntent, Event::Storage(StorageEvent::WriteResult { .. })) => {
                self.step = ExtendStep::SyncIntent;
                smallvec![Effect::Storage(StorageEffect::SyncAll)]
            }
            // The intent must survive a crash before the registry moves the deadline.
            (ExtendStep::SyncIntent, Event::Storage(StorageEvent::SyncAllFinished)) => {
                self.extend()
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
            (_, Event::Storage(StorageEvent::Error { error })) => self.fail(error),
            (ExtendStep::ReadBucket, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                self.authorize(values)
            }
            (ExtendStep::ReadGrant, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                let grant = value
                    .map(|value| BucketHolder::from_bytes(&value))
                    .transpose();
                match grant {
                    Ok(Some(grant)) if grant.origin == HolderOrigin::Explicit => {
                        self.write_intent()
                    }
                    Ok(_) => self.fail(ExtendError::NotHolder),
                    Err(error) => self.fail(error),
                }
            }
            (ExtendStep::ExtendKey, Event::Blob(BlobEvent::KeyExtended { status })) => {
                self.move_timer(status)
            }
            (ExtendStep::ExtendKey, Event::Blob(BlobEvent::Error(error))) => {
                self.reject(error.into())
            }
            (ExtendStep::Finish | ExtendStep::Error, _) => smallvec![],
            (_, Event::Blob(BlobEvent::Error(error))) => self.fail(error),
            (state, received) => self.fail(ExtendError::InvalidStateEvent {
                state: format!("{state:?}"),
                expected: "the event of the current step",
                received,
            }),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.step, ExtendStep::Finish | ExtendStep::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(ExtendError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::bucket::key_rows::authority_rows;
    use aruna_core::structs::storage::blob::BucketInfo;
    use aruna_core::structs::storage::encryption::{BucketEncryption, EncryptionMode};
    use aruna_core::structs::storage::format::Compression;
    use std::time::SystemTime;

    const BUCKET_ID: Ulid = Ulid::from_bytes([4; 16]);
    const SESSION: Ulid = Ulid::from_bytes([8; 16]);

    fn user(seed: u8) -> UserId {
        UserId::new(Ulid::from_bytes([seed; 16]), RealmId::from_bytes([1; 32]))
    }

    fn read(caller: UserId) -> (ExtendBucketOperation, Effects) {
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
        let settings = BucketEncryption {
            mode: EncryptionMode::VaultLocked,
            bucket_id: Some(BUCKET_ID),
            key_generation: 3,
            ..Default::default()
        };
        let values = authority_rows(&info, Some(&settings), &[]);
        let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult { values }));
        (operation, effects)
    }

    /// Answers the synced intent of an authorized extension.
    fn synced(operation: &mut ExtendBucketOperation, effects: &Effects) -> Effects {
        let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
            panic!("expected the intent, got {effects:?}");
        };
        let intent = BucketAuditRecord::from_bytes(value).unwrap();
        assert_eq!(
            (intent.action, intent.outcome, intent.deadline_ms),
            (AuditAction::Extend, AuditOutcome::Intent, Some(31_000))
        );
        let effects = operation.step(Event::Storage(StorageEvent::WriteResult {
            key: Key::from(Vec::new()),
        }));
        assert_eq!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::SyncAll)]
        );
        operation.step(Event::Storage(StorageEvent::SyncAllFinished))
    }

    fn status(remaining: Option<Duration>) -> UnlockStatus {
        UnlockStatus {
            key: BucketKeyRef::new(BUCKET_ID, 2),
            session_id: SESSION,
            active: true,
            unlocked_at: SystemTime::UNIX_EPOCH,
            remaining,
            max_remaining: Some(Duration::from_secs(90)),
        }
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
        let reset = TaskEffect::ResetTimer {
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
        let (mut operation, _) = read(user(1));
        let error = StorageError::Timeout;
        let effects = operation.step(Event::Storage(StorageEvent::Error { error }));
        assert!(effects.is_empty());
        assert_eq!(
            operation.finalize(),
            Err(ExtendError::Storage(StorageError::Timeout))
        );
    }
}
