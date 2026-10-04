//! Unlocks one key generation of a bucket with a key a holder opened in their client. The key is
//! checked and prepared, an audit intent commits, and only then do reads see the key.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::s3::bucket::key_lock::{answers_timer, lock_timer};
use crate::s3::bucket::key_rows::{SettingsError, authority_read, parse_authority};
use aruna_core::compute::SharedSecret;
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{BUCKET_AUDIT_KEYSPACE, BUCKET_HOLDER_KEYSPACE, BUCKET_KEY_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::encryption::{
    BucketHolder, BucketKeyError, BucketKeyRecord, BucketKeyRef, HolderOrigin, KeyState, KeyTicket,
    UnlockStatus, deadline_after,
};
use aruna_core::structs::storage::key_audit::{AuditAction, AuditOutcome, BucketAuditRecord};
use aruna_core::task::TaskEffect;
use aruna_core::types::{Effects, GroupId, Key, TxnId, Value};
use aruna_core::{NodeId, UserId};
use smallvec::smallvec;
use std::time::Duration;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum UnlockStep {
    Init,
    StartTransaction,
    ReadBucket,
    ReadKey,
    PrepareKey,
    WriteIntent,
    CommitIntent,
    ActivateKey,
    ArmTimer,
    DiscardKey,
    WriteOutcome,
    Finish,
    Error,
}

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
    #[error("unexpected event in state {state}: expected {expected}, got {received:?}")]
    InvalidStateEvent {
        state: String,
        expected: &'static str,
        received: Event,
    },
    #[error("the unlock did not finish")]
    NotFinished,
}

/// One unlock request; the caller's implicit authority is read in the unlock's transaction.
#[derive(Debug, PartialEq)]
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

#[derive(Debug, PartialEq)]
pub struct UnlockBucketOperation {
    input: UnlockInput,
    step: UnlockStep,
    txn_id: Option<TxnId>,
    implicit: bool,
    max: Option<Duration>,
    private_key: Option<SharedSecret>,
    ticket: Option<KeyTicket>,
    intent: Option<BucketAuditRecord>,
    failure: Option<UnlockError>,
    activated: Option<UnlockStatus>,
    output: Option<Result<UnlockStatus, UnlockError>>,
}

impl UnlockBucketOperation {
    /// The operation hands `private_key` to the registry and keeps no other copy.
    pub fn new(input: UnlockInput, private_key: SharedSecret) -> Self {
        Self {
            input,
            step: UnlockStep::Init,
            txn_id: None,
            implicit: false,
            max: None,
            private_key: Some(private_key),
            ticket: None,
            intent: None,
            failure: None,
            activated: None,
            output: None,
        }
    }

    fn fail(&mut self, error: impl Into<UnlockError>) -> Effects {
        self.output = Some(Err(error.into()));
        let effects = self.abort();
        self.step = UnlockStep::Error;
        effects
    }

    fn read_key(&mut self, values: Vec<(Key, Option<Value>)>) -> Effects {
        let (realm_id, group_id) = (self.input.realm_id, self.input.group_id);
        let settings = match parse_authority(values, realm_id, group_id) {
            Ok(state) => {
                let caller = self.input.caller;
                self.implicit = state.info.created_by == caller || state.admins.contains(&caller);
                state.settings
            }
            Err(error) => return self.fail(error),
        };
        let key = self.input.key;
        if settings.bucket_id != Some(key.bucket_id) {
            return self.fail(BucketKeyError::StaleGeneration {
                requested: key.generation,
                current: settings.key_generation,
            });
        }
        self.max = settings.max_unlock_ms.map(Duration::from_millis);
        if matches!((self.input.duration, self.max), (Some(asked), Some(max)) if asked > max) {
            return self.fail(BucketKeyError::InvalidDuration);
        }
        let asked = self.input.duration.or(self.max);
        if asked.is_some_and(|asked| deadline_after(self.input.now_ms, asked).is_none()) {
            return self.fail(BucketKeyError::InvalidDuration);
        }
        let grant = [
            &key.bucket_id.to_bytes()[..],
            &self.input.caller.to_storage_key(),
        ]
        .concat();
        self.step = UnlockStep::ReadKey;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (BUCKET_KEY_KEYSPACE.to_string(), key.key().into()),
                (BUCKET_HOLDER_KEYSPACE.to_string(), grant.into()),
            ],
            txn_id: self.txn_id,
        })]
    }

    /// The generation must still be needed, and the caller must hold it now.
    fn prepare(&mut self, values: Vec<(Key, Option<Value>)>) -> Effects {
        let mut values = values.into_iter().map(|(_, value)| value);
        let (Some(record), Some(grant)) = (values.next(), values.next()) else {
            return self.fail(UnlockError::NotFinished);
        };
        let record = match record
            .map(|value| BucketKeyRecord::from_bytes(&value))
            .transpose()
        {
            Ok(Some(record)) if record.state != KeyState::Retired => record,
            Ok(_) => {
                return self.fail(BucketKeyError::StaleGeneration {
                    requested: self.input.key.generation,
                    current: 0,
                });
            }
            Err(error) => return self.fail(error),
        };
        let explicit = match grant
            .map(|value| BucketHolder::from_bytes(&value))
            .transpose()
        {
            Ok(grant) => grant.is_some_and(|grant| grant.origin == HolderOrigin::Explicit),
            Err(error) => return self.fail(error),
        };
        if !(explicit || self.implicit) {
            return self.fail(UnlockError::NotHolder);
        }
        let Some(private_key) = self.private_key.take() else {
            return self.fail(UnlockError::NotFinished);
        };
        self.step = UnlockStep::PrepareKey;
        smallvec![Effect::Blob(BlobEffect::PrepareKey {
            key: record.key,
            public_key: record.public_key,
            private_key,
            duration: self.input.duration,
            max: self.max,
        })]
    }

    fn record(&self, outcome: AuditOutcome, at_ms: u64) -> BucketAuditRecord {
        let deadline = self.input.duration.or(self.max);
        BucketAuditRecord {
            event_id: Ulid::generate(),
            bucket_id: self.input.key.bucket_id,
            at_ms,
            action: AuditAction::Unlock,
            actor: Some(self.input.caller),
            node_id: self.input.node_id,
            generation: Some(self.input.key.generation),
            deadline_ms: deadline.and_then(|deadline| deadline_after(at_ms, deadline)),
            reason: None,
            outcome,
        }
    }

    fn write_record(&mut self, record: &BucketAuditRecord, txn_id: Option<TxnId>) -> Effects {
        match record.to_bytes() {
            Ok(value) => smallvec![Effect::Storage(StorageEffect::Write {
                key_space: BUCKET_AUDIT_KEYSPACE.to_string(),
                key: record.key().into(),
                value: value.into(),
                txn_id,
            })],
            Err(error) => self.fail(error),
        }
    }

    /// The outcome is a separate record, so an intent without one never reads as success.
    fn write_outcome(&mut self, result: Result<UnlockStatus, UnlockError>) -> Effects {
        let outcome = match result {
            Ok(_) => AuditOutcome::Applied,
            Err(_) => AuditOutcome::Failed,
        };
        let mut record = self.record(outcome, self.input.now_ms);
        // An applied unlock records the deadline of the session as it was activated.
        record.deadline_ms = match &result {
            Ok(status) => status.remaining.and_then(|left| {
                let since = status
                    .unlocked_at
                    .duration_since(std::time::UNIX_EPOCH)
                    .ok()?;
                deadline_after(u64::try_from(since.as_millis()).ok()?, left)
            }),
            Err(_) => self.intent.as_ref().and_then(|intent| intent.deadline_ms),
        };
        self.output = Some(result);
        self.step = UnlockStep::WriteOutcome;
        self.write_record(&record, None)
    }
}

impl Operation for UnlockBucketOperation {
    type Output = UnlockStatus;
    type Error = UnlockError;

    fn start(&mut self) -> Effects {
        self.step = UnlockStep::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = &event
            && self.step != UnlockStep::WriteOutcome
        {
            return self.fail(error.clone());
        }
        match (self.step, event) {
            (
                UnlockStep::StartTransaction,
                Event::Storage(StorageEvent::TransactionStarted { txn_id }),
            ) => {
                self.txn_id = Some(txn_id);
                self.step = UnlockStep::ReadBucket;
                let (realm_id, group_id) = (self.input.realm_id, self.input.group_id);
                smallvec![authority_read(
                    &self.input.bucket,
                    realm_id,
                    group_id,
                    Some(txn_id)
                )]
            }
            (UnlockStep::ReadBucket, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                self.read_key(values)
            }
            (UnlockStep::ReadKey, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                self.prepare(values)
            }
            (UnlockStep::PrepareKey, Event::Blob(BlobEvent::KeyPrepared { ticket }))
                if ticket.key == self.input.key =>
            {
                self.ticket = Some(ticket);
                let intent = self.record(AuditOutcome::Intent, self.input.now_ms);
                self.step = UnlockStep::WriteIntent;
                let effects = self.write_record(&intent, self.txn_id);
                self.intent = Some(intent);
                effects
            }
            (UnlockStep::WriteIntent, Event::Storage(StorageEvent::WriteResult { .. })) => {
                let Some(txn_id) = self.txn_id else {
                    return self.fail(UnlockError::NotFinished);
                };
                self.step = UnlockStep::CommitIntent;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            (
                UnlockStep::CommitIntent,
                Event::Storage(StorageEvent::TransactionCommitted { txn_id }),
            ) if Some(txn_id) == self.txn_id => {
                self.txn_id = None;
                let Some(ticket) = self.ticket else {
                    return self.fail(UnlockError::NotFinished);
                };
                self.step = UnlockStep::ActivateKey;
                smallvec![Effect::Blob(BlobEffect::ActivateKey { ticket })]
            }
            (UnlockStep::ActivateKey, Event::Blob(BlobEvent::KeyActivated { status }))
                if self.ticket.is_some_and(|ticket| {
                    (ticket.key, ticket.session_id) == (status.key, status.session_id)
                }) =>
            {
                let Some(ticket) = self.ticket.take() else {
                    return self.fail(UnlockError::NotFinished);
                };
                let Some(after) = status.remaining else {
                    return self.write_outcome(Ok(status));
                };
                // The registry closes admission at the deadline; the timer records the lock.
                self.activated = Some(status);
                self.step = UnlockStep::ArmTimer;
                smallvec![Effect::Task(TaskEffect::ResetTimer {
                    key: lock_timer(&ticket),
                    after,
                })]
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
                match self.activated.take() {
                    Some(status) => self.write_outcome(Ok(status)),
                    None => self.fail(UnlockError::NotFinished),
                }
            }
            // A failed activation discards the prepared key; the intent stays unconfirmed.
            (UnlockStep::ActivateKey, Event::Blob(BlobEvent::Error(error))) => {
                let Some(ticket) = self.ticket.take() else {
                    return self.fail(error);
                };
                self.failure = Some(error.into());
                self.step = UnlockStep::DiscardKey;
                smallvec![Effect::Blob(BlobEffect::DiscardKey { ticket })]
            }
            (
                UnlockStep::DiscardKey,
                Event::Blob(BlobEvent::KeyDiscarded { .. } | BlobEvent::Error(_)),
            ) => {
                let error = self.failure.take().unwrap_or(UnlockError::NotFinished);
                self.write_outcome(Err(error))
            }
            // The key state is settled; a lost outcome record leaves only the unconfirmed intent.
            (
                UnlockStep::WriteOutcome,
                Event::Storage(StorageEvent::WriteResult { .. } | StorageEvent::Error { .. }),
            ) => {
                self.step = UnlockStep::Finish;
                smallvec![]
            }
            (UnlockStep::Finish | UnlockStep::Error, _) => smallvec![],
            (_, Event::Blob(BlobEvent::Error(error))) => self.fail(error),
            (state, received) => self.fail(UnlockError::InvalidStateEvent {
                state: format!("{state:?}"),
                expected: "the event of the current step",
                received,
            }),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.step, UnlockStep::Finish | UnlockStep::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.step {
            UnlockStep::Finish | UnlockStep::Error => {
                self.output.unwrap_or(Err(UnlockError::NotFinished))
            }
            _ => Err(UnlockError::NotFinished),
        }
    }

    fn abort(&mut self) -> Effects {
        self.private_key = None;
        let mut effects = Effects::new();
        if let Some(txn_id) = self.txn_id.take() {
            effects.push(Effect::Storage(StorageEffect::AbortTransaction { txn_id }));
        }
        // A prepared key that never activated is discarded.
        if let Some(ticket) = self.ticket.take() {
            effects.push(Effect::Blob(BlobEffect::DiscardKey { ticket }));
        }
        effects
    }
}

#[cfg(test)]
#[path = "key_unlock_tests.rs"]
mod tests;
