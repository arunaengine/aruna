//! Locks a bucket: every unlocked generation stops admitting reads at once, while admitted
//! reads finish. The lock applies before its audit, so a failed audit write never undoes it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::s3::bucket::key_rows::{SettingsError, parse_settings, settings_read};
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{BUCKET_AUDIT_KEYSPACE, BUCKET_HOLDER_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::encryption::{BucketHolder, HolderOrigin, KeyTicket};
use aruna_core::structs::storage::key_audit::{AuditAction, AuditOutcome, BucketAuditRecord};
use aruna_core::types::{Effects, GroupId, Key, Value};
use aruna_core::{NodeId, UserId};
use smallvec::smallvec;
use std::collections::BTreeSet;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LockStep {
    Init,
    ReadBucket,
    ReadGrant,
    LockKeys,
    WriteAudit,
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
    pub node_id: NodeId,
    pub caller: Option<UserId>,
    pub session: Option<KeyTicket>,
    pub admins: BTreeSet<UserId>,
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
    output: Option<Result<LockResult, LockError>>,
}

impl LockBucketOperation {
    pub fn new(input: LockInput) -> Self {
        Self {
            input,
            step: LockStep::Init,
            bucket_id: None,
            locked: Vec::new(),
            output: None,
        }
    }

    fn fail(&mut self, error: impl Into<LockError>) -> Effects {
        self.output = Some(Err(error.into()));
        self.step = LockStep::Error;
        smallvec![]
    }

    fn authorize(&mut self, values: Vec<(Key, Option<Value>)>) -> Effects {
        let (info, settings) = match parse_settings(values, self.input.group_id) {
            Ok(read) => read,
            Err(error) => return self.fail(error),
        };
        let Some(bucket_id) = settings.bucket_id else {
            return self.fail(LockError::NotEncrypted);
        };
        self.bucket_id = Some(bucket_id);
        let Some(caller) = self.input.caller else {
            return self.lock();
        };
        if info.created_by == caller || self.input.admins.contains(&caller) {
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

    fn finish(&mut self, audited: bool) -> Effects {
        let locked = std::mem::take(&mut self.locked);
        self.output = Some(Ok(LockResult { locked, audited }));
        self.step = LockStep::Finish;
        smallvec![]
    }
}

impl Operation for LockBucketOperation {
    type Output = LockResult;
    type Error = LockError;

    fn start(&mut self) -> Effects {
        self.step = LockStep::ReadBucket;
        smallvec![settings_read(&self.input.bucket, None)]
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

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::identity::realm::RealmId;
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
            admins: BTreeSet::new(),
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
        let row = |value: Vec<u8>| (Key::from(Vec::new()), Some(Value::from(value)));
        let values = vec![
            row(info.to_bytes().unwrap()),
            row(settings.to_bytes().unwrap()),
        ];
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
        // A failed audit write leaves the lock in place.
        operation.step(Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        }));
        assert_eq!(
            operation.finalize(),
            Ok(LockResult {
                locked,
                audited: false
            })
        );
    }

    #[test]
    fn timed_lock_names_session() {
        let (mut operation, effects) = read(None, Some(ticket(2)));
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
