//! Unlocks one key generation of a bucket with a key a holder opened in their client. The key is
//! checked and prepared, an audit intent commits and syncs, and only then do reads see the key.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::s3::bucket::audit_retry::{AuditError, KeepAudit};
use crate::s3::bucket::key::lock::{answers_timer, lock_timer};
use crate::s3::bucket::key::rows::{SettingsError, authority_keys, parse_authority};
use crate::s3::object::put::abe::keyed;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::SharedSecret;
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{
    BUCKET_AUDIT_KEYSPACE, BUCKET_HOLDER_KEYSPACE, BUCKET_KEY_KEYSPACE, KEY_COPY_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::values;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::encryption::{
    BucketHolder, BucketKeyError, BucketKeyRecord, BucketKeyRef, HolderOrigin, KeyState, KeyTicket,
    SealedCopy, UnlockStatus, deadline_after,
};
use aruna_core::structs::storage::key_audit::{
    AuditAction, AuditOutcome, BucketAuditRecord, next_event_id,
};
use aruna_core::task::TaskEffect;
use aruna_core::types::{GroupId, Key, Value};
use aruna_core::{NodeId, UserId};
use std::time::Duration;
use thiserror::Error;
use ulid::Ulid;

#[derive(Debug, Error, PartialEq)]
pub enum UnlockError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Settings(#[from] SettingsError),
    #[error(transparent)]
    Key(#[from] BucketKeyError),
    #[error(transparent)]
    Blob(#[from] BlobError),
    /// The caller is neither creator, current admin nor an explicit holder (D30).
    #[error("the caller holds no key of this bucket")]
    NotHolder,
    /// The caller is a holder, but no copy of this generation was sealed to them yet.
    #[error("the caller holds no sealed copy of this key generation")]
    NoCopy,
    #[error("unexpected event in state {state}: expected {expected}, got {received}")]
    InvalidStateEvent {
        state: String,
        expected: &'static str,
        received: &'static str,
    },
    #[error("the unlock did not finish")]
    NotFinished,
}

impl From<IoError> for UnlockError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.into())
    }
}

impl From<AuditError> for UnlockError {
    fn from(error: AuditError) -> Self {
        match error {
            AuditError::Conversion(error) => Self::Conversion(error),
            AuditError::Storage(error) => Self::Storage(error),
            AuditError::NotAuditTimer => Self::NotFinished,
            AuditError::Unexpected(received) => Self::InvalidStateEvent {
                state: "WriteOutcome".to_string(),
                expected: "the audit retry timer's answer",
                received,
            },
        }
    }
}

/// One unlock request; the caller's implicit authority is read in the unlock's transaction.
#[derive(Clone, Debug, PartialEq)]
pub struct UnlockInput {
    pub bucket: String,
    pub group_id: GroupId,
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub caller: UserId,
    pub key: BucketKeyRef,
    pub duration: Option<Duration>,
    pub now_ms: u64,
}

/// The audit record of an unlock of `input`'s key.
fn audit_record(
    input: &UnlockInput,
    max: Option<Duration>,
    session: Option<Ulid>,
    outcome: AuditOutcome,
) -> BucketAuditRecord {
    let deadline = input.duration.or(max);
    BucketAuditRecord {
        event_id: next_event_id(input.now_ms),
        bucket_id: input.key.bucket_id,
        at_ms: input.now_ms,
        action: AuditAction::Unlock,
        actor: Some(input.caller),
        node_id: input.node_id,
        generation: Some(input.key.generation),
        session_id: session,
        intent_id: None,
        sequence: None,
        deadline_ms: deadline.and_then(|deadline| deadline_after(input.now_ms, deadline)),
        reason: None,
        outcome,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IntentStep {
    ReadBucket,
    ReadKey,
    ReadCopy,
    PrepareKey,
    WriteIntent,
}

/// What the committed intent leaves the unlock with.
#[derive(Debug, PartialEq)]
struct PreparedUnlock {
    ticket: KeyTicket,
    intent: BucketAuditRecord,
    max: Option<Duration>,
}

type IntentTurn = Result<Next<PreparedUnlock>, UnlockError>;

/// Checks the caller, prepares the key and stores the audit intent in one transaction. A run that
/// does not commit discards the prepared key.
#[derive(Debug, PartialEq)]
struct UnlockIntent {
    input: UnlockInput,
    step: IntentStep,
    implicit: bool,
    max: Option<Duration>,
    private_key: Option<SharedSecret>,
    record: Option<BucketKeyRecord>,
    ticket: Option<KeyTicket>,
    intent: Option<BucketAuditRecord>,
}

impl UnlockIntent {
    fn call(&mut self, step: IntentStep, call: IoCall) -> IntentTurn {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn read_key(&mut self, read: Vec<Option<Value>>) -> IntentTurn {
        let (realm_id, group_id) = (self.input.realm_id, self.input.group_id);
        let state = parse_authority(keyed(&read), realm_id, group_id)?;
        let caller = self.input.caller;
        self.implicit = state.info.created_by == caller || state.admins.contains(&caller);
        let settings = state.settings;
        let key = self.input.key;
        if settings.bucket_id != Some(key.bucket_id) {
            return Err(BucketKeyError::StaleGeneration {
                requested: key.generation,
                current: settings.key_generation,
            }
            .into());
        }
        self.max = settings.unlock_limit().map(Duration::from_millis);
        if matches!((self.input.duration, self.max), (Some(asked), Some(max)) if asked > max) {
            return Err(BucketKeyError::InvalidDuration.into());
        }
        let asked = self.input.duration.or(self.max);
        if asked.is_some_and(|asked| deadline_after(self.input.now_ms, asked).is_none()) {
            return Err(BucketKeyError::InvalidDuration.into());
        }
        let grant = [&key.bucket_id.to_bytes()[..], &caller.to_storage_key()].concat();
        let reads = vec![
            (BUCKET_KEY_KEYSPACE.to_string(), key.key().into()),
            (BUCKET_HOLDER_KEYSPACE.to_string(), grant.into()),
        ];
        self.call(IntentStep::ReadKey, IoCall::Read(reads))
    }

    /// The generation must still be needed, and the caller must hold it now.
    fn check_holder(&mut self, [record, grant]: [Option<Value>; 2]) -> IntentTurn {
        let record = record
            .map(|value| BucketKeyRecord::from_bytes(&value))
            .transpose()?
            .filter(|record| record.state != KeyState::Retired)
            .ok_or(BucketKeyError::StaleGeneration {
                requested: self.input.key.generation,
                current: 0,
            })?;
        let grant = grant
            .map(|value| BucketHolder::from_bytes(&value))
            .transpose()?;
        let explicit = grant.is_some_and(|grant| grant.origin == HolderOrigin::Explicit);
        if !(explicit || self.implicit) {
            return Err(UnlockError::NotHolder);
        }
        let prefix = SealedCopy::user_prefix(record.key, self.input.caller);
        self.record = Some(record);
        let page = IoCall::Page {
            space: KEY_COPY_KEYSPACE.to_string(),
            prefix: prefix.into(),
            after: None,
            limit: 1,
        };
        self.call(IntentStep::ReadCopy, page)
    }

    /// Only a holder with a copy of this generation can have opened its key.
    fn copy_read(&mut self, rows: Vec<(Key, Value)>) -> IntentTurn {
        if rows.is_empty() {
            return Err(UnlockError::NoCopy);
        }
        let record = self.record.take().ok_or(UnlockError::NotFinished)?;
        let private_key = self.private_key.take().ok_or(UnlockError::NotFinished)?;
        let prepare = Effect::Blob(BlobEffect::PrepareKey {
            key: record.key,
            public_key: record.public_key,
            private_key,
            duration: self.input.duration,
            max: self.max,
        });
        self.call(IntentStep::PrepareKey, prepare.into())
    }

    fn prepared(&mut self, event: Event) -> IntentTurn {
        let ticket = match event {
            Event::Blob(BlobEvent::KeyPrepared { ticket }) if ticket.key == self.input.key => {
                ticket
            }
            other => return refused("PrepareKey", other),
        };
        self.ticket = Some(ticket);
        let session = Some(ticket.session_id);
        let intent = audit_record(&self.input, self.max, session, AuditOutcome::Intent);
        let row = (
            BUCKET_AUDIT_KEYSPACE.to_string(),
            intent.key().into(),
            intent.to_bytes()?.into(),
        );
        self.intent = Some(intent);
        self.call(IntentStep::WriteIntent, IoCall::Write(vec![row]))
    }
}

/// A registry error as itself; any other answer is refused.
fn refused<T>(state: &str, received: Event) -> Result<T, UnlockError> {
    match received {
        Event::Blob(BlobEvent::Error(error)) => Err(error.into()),
        other => Err(UnlockError::InvalidStateEvent {
            state: state.to_string(),
            expected: "the event of the current step",
            received: other.label(),
        }),
    }
}

impl Operation for UnlockIntent {
    type Output = PreparedUnlock;
    type Error = UnlockError;

    fn first(&mut self) -> IntentTurn {
        let (realm_id, group_id) = (self.input.realm_id, self.input.group_id);
        let reads = authority_keys(&self.input.bucket, realm_id, group_id);
        self.call(IntentStep::ReadBucket, IoCall::Read(reads))
    }

    fn next(&mut self, result: IoResult) -> IntentTurn {
        match self.step {
            IntentStep::ReadBucket => self.read_key(result.values()?),
            IntentStep::ReadKey => self.check_holder(values::<2>(result)?),
            IntentStep::ReadCopy => self.copy_read(result.page()?.0),
            IntentStep::PrepareKey => self.prepared(result.event()?),
            IntentStep::WriteIntent => {
                result.done()?;
                let ticket = self.ticket.ok_or(UnlockError::NotFinished)?;
                let intent = self.intent.take().ok_or(UnlockError::NotFinished)?;
                let max = self.max;
                Ok(Next::Done(PreparedUnlock {
                    ticket,
                    intent,
                    max,
                }))
            }
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    /// A prepared key whose intent did not commit is discarded.
    fn abort(&mut self) -> Vec<IoCall> {
        self.private_key = None;
        let discard = |ticket| Effect::Blob(BlobEffect::DiscardKey { ticket }).into();
        self.ticket.take().map(discard).into_iter().collect()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum UnlockStep {
    Intent,
    SyncIntent,
    ActivateKey,
    ArmTimer,
    DiscardKey,
    WriteOutcome,
}

type UnlockTurn = Result<Next<UnlockStatus>, UnlockError>;

/// Unlocks one generation: the intent commits and syncs first, then reads may see the key.
#[derive(Debug, PartialEq)]
pub struct UnlockBucketOperation {
    input: UnlockInput,
    step: UnlockStep,
    private_key: Option<SharedSecret>,
    max: Option<Duration>,
    ticket: Option<KeyTicket>,
    /// The session the prepared key opens, named in the audit records of this unlock.
    session: Option<Ulid>,
    intent: Option<BucketAuditRecord>,
    failure: Option<UnlockError>,
    activated: Option<UnlockStatus>,
    outcome: Option<BucketAuditRecord>,
    finished: Option<Result<UnlockStatus, UnlockError>>,
}

impl UnlockBucketOperation {
    /// The operation hands `private_key` to the registry and keeps no other copy.
    pub fn new(input: UnlockInput, private_key: SharedSecret) -> Self {
        Self {
            input,
            step: UnlockStep::Intent,
            private_key: Some(private_key),
            max: None,
            ticket: None,
            session: None,
            intent: None,
            failure: None,
            activated: None,
            outcome: None,
            finished: None,
        }
    }

    fn call(&mut self, step: UnlockStep, call: IoCall) -> UnlockTurn {
        self.step = step;
        Ok(Next::Call(call))
    }

    /// The intent committed; it must survive a crash before any read can use the key.
    fn intent_committed(&mut self, prepared: PreparedUnlock) -> UnlockTurn {
        self.ticket = Some(prepared.ticket);
        self.session = Some(prepared.ticket.session_id);
        self.max = prepared.max;
        self.intent = Some(prepared.intent);
        let sync = Effect::Storage(StorageEffect::SyncAll);
        self.call(UnlockStep::SyncIntent, sync.into())
    }

    fn activate(&mut self) -> UnlockTurn {
        let ticket = self.ticket.ok_or(UnlockError::NotFinished)?;
        let activate = Effect::Blob(BlobEffect::ActivateKey { ticket });
        self.call(UnlockStep::ActivateKey, activate.into())
    }

    fn activated(&mut self, status: UnlockStatus) -> UnlockTurn {
        let ticket = self.ticket.take().ok_or(UnlockError::NotFinished)?;
        let Some(after) = status.remaining else {
            return self.write_outcome(Ok(status));
        };
        // The registry closes admission at the deadline; the timer records the lock.
        self.activated = Some(status);
        let arm = Effect::Task(TaskEffect::ShortenTimer {
            key: lock_timer(&ticket),
            after,
        });
        self.call(UnlockStep::ArmTimer, arm.into())
    }

    /// A failed activation discards the prepared key; the intent stays unconfirmed.
    fn activation_failed(&mut self, error: BlobError) -> UnlockTurn {
        let Some(ticket) = self.ticket.take() else {
            return Err(error.into());
        };
        self.failure = Some(error.into());
        let discard = Effect::Blob(BlobEffect::DiscardKey { ticket });
        self.call(UnlockStep::DiscardKey, discard.into())
    }

    /// The outcome is a separate record, so an intent without one never reads as success.
    fn write_outcome(&mut self, result: Result<UnlockStatus, UnlockError>) -> UnlockTurn {
        let outcome = match result {
            Ok(_) => AuditOutcome::Applied,
            Err(_) => AuditOutcome::Failed,
        };
        let mut record = audit_record(&self.input, self.max, self.session, outcome);
        // An applied unlock records the deadline of the session as it was activated.
        record.deadline_ms = match &result {
            Ok(status) => status.deadline_ms,
            Err(_) => self.intent.as_ref().and_then(|intent| intent.deadline_ms),
        };
        record.intent_id = self.intent.as_ref().map(|intent| intent.event_id);
        record.sequence = result.as_ref().ok().map(|status| status.sequence);
        self.finished = Some(result);
        self.outcome = Some(record.clone());
        let keep = KeepAudit::new(vec![record]);
        self.call(UnlockStep::WriteOutcome, IoCall::child(keep))
    }

    /// The key state is settled, so the run ends with what the activation decided.
    fn finish(&mut self, kept: Result<bool, AuditError>) -> UnlockTurn {
        kept?;
        self.outcome = None;
        let finished = self.finished.take().ok_or(UnlockError::NotFinished)?;
        finished.map(Next::Done)
    }

    fn event_answered(&mut self, event: Event) -> UnlockTurn {
        match (self.step, event) {
            (UnlockStep::SyncIntent, Event::Storage(StorageEvent::SyncAllFinished)) => {
                self.activate()
            }
            (UnlockStep::ActivateKey, Event::Blob(BlobEvent::KeyActivated { status }))
                if self.ticket.is_some_and(|ticket| {
                    (ticket.key, ticket.session_id) == (status.key, status.session_id)
                }) =>
            {
                self.activated(status)
            }
            (UnlockStep::ActivateKey, Event::Blob(BlobEvent::Error(error))) => {
                self.activation_failed(error)
            }
            (UnlockStep::ArmTimer, Event::Task(event))
                if self.activated.as_ref().is_some_and(|status| {
                    let ticket = KeyTicket {
                        key: status.key,
                        session_id: status.session_id,
                    };
                    answers_timer(&event, &lock_timer(&ticket))
                }) =>
            {
                let status = self.activated.take().ok_or(UnlockError::NotFinished)?;
                self.write_outcome(Ok(status))
            }
            (
                UnlockStep::DiscardKey,
                Event::Blob(BlobEvent::KeyDiscarded { .. } | BlobEvent::Error(_)),
            ) => {
                let error = self.failure.take().unwrap_or(UnlockError::NotFinished);
                self.write_outcome(Err(error))
            }
            (step, received) => refused(&format!("{step:?}"), received),
        }
    }
}

impl Operation for UnlockBucketOperation {
    type Output = UnlockStatus;
    type Error = UnlockError;

    fn first(&mut self) -> UnlockTurn {
        let private_key = self.private_key.take().ok_or(UnlockError::NotFinished)?;
        let intent = UnlockIntent {
            input: self.input.clone(),
            step: IntentStep::ReadBucket,
            implicit: false,
            max: None,
            private_key: Some(private_key),
            record: None,
            ticket: None,
            intent: None,
        };
        self.call(UnlockStep::Intent, IoCall::child(intent))
    }

    fn next(&mut self, result: IoResult) -> UnlockTurn {
        match self.step {
            UnlockStep::Intent => {
                let prepared = result.child::<PreparedUnlock, UnlockError>()??;
                self.intent_committed(prepared)
            }
            UnlockStep::WriteOutcome => self.finish(result.child::<bool, AuditError>()?),
            _ => self.event_answered(result.event()?),
        }
    }

    /// A prepared key that never activated is discarded.
    fn abort(&mut self) -> Vec<IoCall> {
        self.private_key = None;
        let discard = |ticket| Effect::Blob(BlobEffect::DiscardKey { ticket }).into();
        self.ticket.take().map(discard).into_iter().collect()
    }
}

#[cfg(test)]
#[path = "unlock_tests.rs"]
mod tests;
