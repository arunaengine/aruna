//! Reads what the bucket encryption status reports: settings, key generations, grants, sealed
//! copies and the unlock state of each generation, and pages of the bucket's key audit.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::abe::rekey::RekeyProgress;
use crate::driver::DriverContext;
use crate::s3::bucket::key::restart::Replay;
use crate::s3::bucket::key::rows::{SettingsError, authority_read, parse_authority};
use aruna_core::UserId;
use aruna_core::effects::{BlobEffect, Effect, IterStart, StorageEffect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{
    ABE_DUE_KEYSPACE, ABE_EPOCH_KEYSPACE, ABE_REKEY_KEYSPACE, BUCKET_AUDIT_KEYSPACE,
    BUCKET_ENCRYPTION_KEYSPACE, BUCKET_HOLDER_KEYSPACE, BUCKET_KEY_KEYSPACE, KEY_COPY_KEYSPACE,
    TRANSITION_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::encryption::{
    BucketEncryption, BucketHolder, BucketKeyRecord, SealedCopy, UnlockStatus,
};
use aruna_core::structs::storage::key_audit::{AuditAction, BucketAuditRecord};
use aruna_core::structs::storage::transition::EncryptionTransition;
use aruna_core::types::{Effects, GroupId, Key, Value};
use smallvec::smallvec;
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;
use ulid::Ulid;

const SCAN_PAGE: usize = 1_000;

#[derive(Debug, Error, PartialEq)]
pub enum KeyStatusError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Settings(#[from] SettingsError),
    #[error(transparent)]
    Blob(#[from] BlobError),
    #[error("unexpected event in state {state}: expected {expected}, got {received:?}")]
    InvalidStateEvent {
        state: String,
        expected: &'static str,
        received: Event,
    },
    #[error("the status read did not finish")]
    NotFinished,
}

/// One bucket's key state as stored on this node, read without a transaction.
#[derive(Debug, Default, PartialEq)]
pub struct KeySnapshot {
    pub info: Option<BucketInfo>,
    pub settings: BucketEncryption,
    pub admins: BTreeSet<UserId>,
    pub records: Vec<BucketKeyRecord>,
    pub grants: Vec<BucketHolder>,
    pub copies: Vec<SealedCopy>,
    /// Unlocked generations; a generation without an entry is locked.
    pub unlocks: Vec<UnlockStatus>,
    /// The latest applied lock of each generation not unlocked since, with its time.
    pub locks: BTreeMap<u64, (AuditAction, u64)>,
    /// This node's mode change or rotation of the bucket's copies, if one was started.
    pub transition: Option<EncryptionTransition>,
    /// The ABE epoch state; none without an epoch row.
    pub abe: Option<AbeSnapshot>,
}

/// A bucket's ABE epoch, whether a raise is due and its unfinished re-key pass.
#[derive(Debug, PartialEq)]
pub struct AbeSnapshot {
    pub epoch: u64,
    pub due: bool,
    pub rekey: Option<RekeyProgress>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StatusStep {
    Init,
    ReadBucket,
    Transition,
    Records,
    Grants,
    Copies,
    Audit,
    KeyStatus,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct KeyStatusOperation {
    bucket: String,
    realm_id: RealmId,
    group_id: GroupId,
    step: StatusStep,
    snapshot: KeySnapshot,
    /// The audit trail read the same way as at restart, for the lock reasons.
    replay: Replay,
    output: Option<Result<KeySnapshot, KeyStatusError>>,
}

impl KeyStatusOperation {
    pub fn new(bucket: String, realm_id: RealmId, group_id: GroupId) -> Self {
        Self {
            bucket,
            realm_id,
            group_id,
            step: StatusStep::Init,
            snapshot: KeySnapshot::default(),
            replay: Replay::default(),
            output: None,
        }
    }

    fn fail(&mut self, error: impl Into<KeyStatusError>) -> Effects {
        self.output = Some(Err(error.into()));
        self.step = StatusStep::Error;
        smallvec![]
    }

    fn bucket_id(&self) -> Option<Ulid> {
        self.snapshot.settings.bucket_id
    }

    fn scan(&mut self, step: StatusStep, start: Option<Key>) -> Effects {
        let Some(bucket_id) = self.bucket_id() else {
            return self.fail(KeyStatusError::NotFinished);
        };
        let key_space = match step {
            StatusStep::Records => BUCKET_KEY_KEYSPACE,
            StatusStep::Grants => BUCKET_HOLDER_KEYSPACE,
            StatusStep::Audit => BUCKET_AUDIT_KEYSPACE,
            _ => KEY_COPY_KEYSPACE,
        };
        self.step = step;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: key_space.to_string(),
            prefix: Some(bucket_id.to_bytes().to_vec().into()),
            start: start.map(IterStart::After),
            limit: SCAN_PAGE,
            txn_id: None,
        })]
    }

    fn read_bucket(&mut self, values: Vec<(Key, Option<Value>)>) -> Effects {
        let state = match parse_authority(values, self.realm_id, self.group_id) {
            Ok(state) => state,
            Err(error) => return self.fail(error),
        };
        self.snapshot.info = Some(state.info);
        self.snapshot.settings = state.settings;
        self.snapshot.admins = state.admins;
        self.step = StatusStep::Transition;
        let mut reads = vec![(
            TRANSITION_KEYSPACE.to_string(),
            self.bucket.as_bytes().to_vec().into(),
        )];
        if let Some(bucket_id) = self.bucket_id() {
            let id: Key = bucket_id.to_bytes().to_vec().into();
            reads.extend(
                [ABE_EPOCH_KEYSPACE, ABE_DUE_KEYSPACE, ABE_REKEY_KEYSPACE]
                    .map(|key_space| (key_space.to_string(), id.clone())),
            );
        }
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: None
        })]
    }

    fn read_transition(&mut self, values: Vec<(Key, Option<Value>)>) -> Effects {
        let mut values = values.into_iter().map(|(_, value)| value);
        match values
            .next()
            .flatten()
            .map(|value| EncryptionTransition::from_bytes(&value))
            .transpose()
        {
            Ok(transition) => self.snapshot.transition = transition,
            Err(error) => return self.fail(error),
        }
        match parse_abe(
            values.next().flatten(),
            values.next().flatten(),
            values.next().flatten(),
        ) {
            Ok(abe) => self.snapshot.abe = abe,
            Err(error) => return self.fail(error),
        }
        match self.bucket_id() {
            Some(_) => self.scan(StatusStep::Records, None),
            None => self.finish(),
        }
    }

    fn take_page(&mut self, values: Vec<(Key, Value)>, next: Option<Key>) -> Effects {
        for (key, value) in values {
            let parsed =
                match self.step {
                    StatusStep::Records => BucketKeyRecord::from_bytes(&value)
                        .map(|record| self.snapshot.records.push(record)),
                    StatusStep::Grants => BucketHolder::from_bytes(&value)
                        .map(|grant| self.snapshot.grants.push(grant)),
                    StatusStep::Audit => BucketAuditRecord::from_bytes(&value)
                        .map(|record| self.replay.apply(&record)),
                    // Only user copies; other copy kinds are not part of this stage.
                    _ if SealedCopy::parse_key(&key).is_err() => Ok(()),
                    _ => SealedCopy::from_bytes(&value).map(|copy| self.snapshot.copies.push(copy)),
                };
            if let Err(error) = parsed {
                return self.fail(error);
            }
        }
        if next.is_some() {
            return self.scan(self.step, next);
        }
        match self.step {
            StatusStep::Records => self.scan(StatusStep::Grants, None),
            StatusStep::Grants => self.scan(StatusStep::Copies, None),
            StatusStep::Copies => self.scan(StatusStep::Audit, None),
            _ => self.read_unlocks(),
        }
    }

    /// Outcomes pair with their intents, so a failed unlock keeps the lock reason it replaced.
    fn read_unlocks(&mut self) -> Effects {
        self.snapshot.locks = self.replay.locks();
        let Some(bucket_id) = self.bucket_id() else {
            return self.fail(KeyStatusError::NotFinished);
        };
        self.step = StatusStep::KeyStatus;
        smallvec![Effect::Blob(BlobEffect::ReadKeyStatus { bucket_id })]
    }

    fn finish(&mut self) -> Effects {
        self.output = Some(Ok(std::mem::take(&mut self.snapshot)));
        self.step = StatusStep::Finish;
        smallvec![]
    }
}

impl Operation for KeyStatusOperation {
    type Output = KeySnapshot;
    type Error = KeyStatusError;

    fn start(&mut self) -> Effects {
        self.step = StatusStep::ReadBucket;
        smallvec![authority_read(
            &self.bucket,
            self.realm_id,
            self.group_id,
            None
        )]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (self.step, event) {
            (StatusStep::Finish | StatusStep::Error, _) => smallvec![],
            (_, Event::Storage(StorageEvent::Error { error })) => self.fail(error),
            (_, Event::Blob(BlobEvent::Error(error))) => self.fail(error),
            (StatusStep::ReadBucket, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                self.read_bucket(values)
            }
            (StatusStep::Transition, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                self.read_transition(values)
            }
            (
                StatusStep::Records | StatusStep::Grants | StatusStep::Copies | StatusStep::Audit,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => self.take_page(values, next_start_after),
            (StatusStep::KeyStatus, Event::Blob(BlobEvent::KeyStatus { generations })) => {
                self.snapshot.unlocks = generations;
                self.finish()
            }
            (state, received) => self.fail(KeyStatusError::InvalidStateEvent {
                state: format!("{state:?}"),
                expected: "the event of the current step",
                received,
            }),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.step, StatusStep::Finish | StatusStep::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(KeyStatusError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }

    fn expected_error(error: &Self::Error) -> bool {
        matches!(
            error,
            KeyStatusError::Settings(SettingsError::NoSuchBucket | SettingsError::GroupMismatch)
        )
    }
}

fn parse_abe(
    epoch: Option<Value>,
    due: Option<Value>,
    rekey: Option<Value>,
) -> Result<Option<AbeSnapshot>, ConversionError> {
    let Some(epoch) = epoch else {
        return Ok(None);
    };
    let epoch = <[u8; 8]>::try_from(epoch.as_ref())
        .map_err(|_| ConversionError::InvalidLength("abe epoch".to_string()))?;
    Ok(Some(AbeSnapshot {
        epoch: u64::from_be_bytes(epoch),
        due: due.is_some(),
        rekey: rekey
            .map(|value| postcard::from_bytes(&value))
            .transpose()?,
    }))
}

/// The encryption settings of `bucket`; a bucket without a settings row is plain.
pub async fn bucket_settings(
    context: &DriverContext,
    bucket: &str,
) -> Result<BucketEncryption, KeyStatusError> {
    let read = StorageEffect::Read {
        key_space: BUCKET_ENCRYPTION_KEYSPACE.to_string(),
        key: bucket.as_bytes().to_vec().into(),
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => {
            Ok(BucketEncryption::from_row(value.as_deref())?)
        }
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        _ => Err(StorageError::ReadError("unexpected settings read".to_string()).into()),
    }
}

/// One page of a bucket's key audit in time order, after the event `cursor`.
#[derive(Debug, PartialEq)]
pub struct AuditPageOperation {
    bucket_id: Ulid,
    cursor: Option<Ulid>,
    limit: usize,
    done: bool,
    output: Option<Result<AuditPage, KeyStatusError>>,
}

impl AuditPageOperation {
    pub fn new(bucket_id: Ulid, cursor: Option<Ulid>, limit: usize) -> Self {
        Self {
            bucket_id,
            cursor,
            limit: limit.max(1),
            done: false,
            output: None,
        }
    }

    fn page(&self, values: Vec<(Key, Value)>, more: bool) -> Result<AuditPage, KeyStatusError> {
        let events = values
            .into_iter()
            .map(|(_, value)| BucketAuditRecord::from_bytes(&value))
            .collect::<Result<Vec<_>, _>>()?;
        let next = match more && events.len() == self.limit {
            true => events.last().map(|event| event.event_id),
            false => None,
        };
        Ok((events, next))
    }
}

type AuditPage = (Vec<BucketAuditRecord>, Option<Ulid>);

impl Operation for AuditPageOperation {
    type Output = AuditPage;
    type Error = KeyStatusError;

    fn start(&mut self) -> Effects {
        let prefix = self.bucket_id.to_bytes().to_vec();
        let start = self
            .cursor
            .map(|cursor| IterStart::After([&prefix[..], &cursor.to_bytes()].concat().into()));
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: BUCKET_AUDIT_KEYSPACE.to_string(),
            prefix: Some(prefix.into()),
            start,
            limit: self.limit,
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if self.done {
            return smallvec![];
        }
        self.done = true;
        self.output = Some(match event {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => self.page(values, next_start_after.is_some()),
            Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
            received => Err(KeyStatusError::InvalidStateEvent {
                state: "ReadAudit".to_string(),
                expected: "IterResult",
                received,
            }),
        });
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        self.done
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(KeyStatusError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::bucket::key::rows::authority_rows;
    use aruna_core::structs::storage::encryption::{BucketKeyRef, EncryptionMode};
    use aruna_core::structs::storage::format::Compression;
    use aruna_core::structs::storage::key_audit::AuditOutcome;
    use aruna_core::structs::storage::transition::{TransitionKind, TransitionTarget};
    use std::time::SystemTime;

    const BUCKET_ID: Ulid = Ulid::from_bytes([4; 16]);

    fn user(seed: u8) -> UserId {
        UserId::new(Ulid::from_bytes([seed; 16]), RealmId::from_bytes([1; 32]))
    }

    fn info() -> BucketInfo {
        BucketInfo {
            group_id: Ulid::from_bytes([3; 16]),
            created_at: SystemTime::UNIX_EPOCH,
            created_by: user(1),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
        }
    }

    fn operation() -> KeyStatusOperation {
        KeyStatusOperation::new(
            "bucket".to_string(),
            RealmId::from_bytes([1; 32]),
            info().group_id,
        )
    }

    fn iter(values: Vec<Vec<u8>>, next: Option<&[u8]>) -> Event {
        Event::Storage(StorageEvent::IterResult {
            values: values
                .into_iter()
                .map(|value| (Key::from(Vec::new()), Value::from(value)))
                .collect(),
            next_start_after: next.map(|key| Key::from(key.to_vec())),
        })
    }

    fn batch(values: Vec<Option<Vec<u8>>>) -> Event {
        let values = values
            .into_iter()
            .map(|value| (Key::from(Vec::new()), value.map(Value::from)))
            .collect();
        Event::Storage(StorageEvent::BatchReadResult { values })
    }

    #[test]
    fn plain_bucket_stops() {
        let mut operation = operation();
        operation.start();
        let rows = authority_rows(&info(), None, &[user(2)]);
        let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: rows,
        }));
        assert!(matches!(
            &effects[..],
            [Effect::Storage(StorageEffect::BatchRead { reads, .. })]
                if reads.len() == 1 && reads[0].0 == TRANSITION_KEYSPACE
        ));
        // A decrypt that finished its mode change still reports its transition.
        let transition = EncryptionTransition::new(
            TransitionKind::Decrypt,
            Some(BucketKeyRef::new(BUCKET_ID, 1)),
            TransitionTarget {
                compression: Compression::Off,
                plan: None,
            },
            2,
            7,
        );
        let effects = operation.step(batch(vec![Some(transition.to_bytes().unwrap())]));
        assert!(effects.is_empty());
        let snapshot = operation.finalize().unwrap();
        assert_eq!(snapshot.transition, Some(transition));
        assert_eq!(snapshot.settings.mode, EncryptionMode::Off);
        assert_eq!(snapshot.admins, BTreeSet::from([user(2)]));
        assert!(snapshot.records.is_empty());
        assert_eq!(snapshot.abe, None);
    }

    #[test]
    fn reads_every_page() {
        let mut operation = operation();
        operation.start();
        let settings = BucketEncryption {
            mode: EncryptionMode::NodeManaged,
            bucket_id: Some(BUCKET_ID),
            key_generation: 1,
            ..Default::default()
        };
        let rows = authority_rows(&info(), Some(&settings), &[]);
        let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: rows,
        }));
        assert!(matches!(
            &effects[..],
            [Effect::Storage(StorageEffect::BatchRead { reads, .. })] if reads.len() == 4
        ));
        let progress = RekeyProgress {
            prefix: "raw/".to_string(),
            epoch: 3,
            cursor: b"raw/a".to_vec(),
            rekeyed: 5,
        };
        let effects = operation.step(batch(vec![
            None,
            Some(3_u64.to_be_bytes().to_vec()),
            Some(vec![1]),
            Some(postcard::to_allocvec(&progress).unwrap()),
        ]));
        assert!(matches!(
            &effects[..],
            [Effect::Storage(StorageEffect::Iter { key_space, start: None, .. })]
                if key_space == BUCKET_KEY_KEYSPACE
        ));
        let record = BucketKeyRecord::new(BucketKeyRef::new(BUCKET_ID, 1), BUCKET_ID, [7; 32], 5);
        let bytes = record.to_bytes().unwrap();
        let effects = operation.step(iter(vec![bytes.clone()], Some(b"after")));
        assert!(matches!(
            &effects[..],
            [Effect::Storage(StorageEffect::Iter { start: Some(IterStart::After(key)), .. })]
                if key.as_ref() == b"after"
        ));
        let effects = operation.step(iter(vec![bytes], None));
        assert!(matches!(
            &effects[..],
            [Effect::Storage(StorageEffect::Iter { key_space, .. })]
                if key_space == BUCKET_HOLDER_KEYSPACE
        ));
        operation.step(iter(Vec::new(), None));
        let effects = operation.step(iter(Vec::new(), None));
        assert!(matches!(
            &effects[..],
            [Effect::Storage(StorageEffect::Iter { key_space, .. })]
                if key_space == BUCKET_AUDIT_KEYSPACE
        ));
        let event = |at_ms: u64, action, generation| BucketAuditRecord {
            event_id: Ulid::from_parts(at_ms, 1),
            bucket_id: BUCKET_ID,
            at_ms,
            action,
            actor: None,
            node_id: iroh::SecretKey::from_bytes(&[2; 32]).public(),
            generation: Some(generation),
            session_id: None,
            intent_id: None,
            sequence: None,
            deadline_ms: None,
            reason: None,
            outcome: AuditOutcome::Applied,
        };
        let trail = [
            event(1, AuditAction::RestartLock, 1),
            event(2, AuditAction::Lock, 2),
            event(3, AuditAction::Unlock, 2),
            event(4, AuditAction::TimedLock, 3),
        ];
        let rows = trail
            .iter()
            .map(|record| record.to_bytes().unwrap())
            .collect();
        let effects = operation.step(iter(rows, None));
        assert_eq!(
            effects[..],
            [Effect::Blob(BlobEffect::ReadKeyStatus {
                bucket_id: BUCKET_ID
            })]
        );
        operation.step(Event::Blob(BlobEvent::KeyStatus {
            generations: Vec::new(),
        }));
        let snapshot = operation.finalize().unwrap();
        assert_eq!(snapshot.records, vec![record.clone(), record]);
        let locks = BTreeMap::from([
            (1, (AuditAction::RestartLock, 1)),
            (3, (AuditAction::TimedLock, 4)),
        ]);
        assert_eq!(snapshot.locks, locks);
        let abe = AbeSnapshot {
            epoch: 3,
            due: true,
            rekey: Some(progress),
        };
        assert_eq!(snapshot.abe, Some(abe));
    }

    #[test]
    fn failures_keep_reason() {
        let mut operation = operation();
        operation.start();
        let settings = BucketEncryption {
            mode: EncryptionMode::VaultLocked,
            bucket_id: Some(BUCKET_ID),
            key_generation: 1,
            ..Default::default()
        };
        let rows = authority_rows(&info(), Some(&settings), &[]);
        operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: rows,
        }));
        operation.step(batch(vec![None; 4]));
        for _ in 0..3 {
            operation.step(iter(Vec::new(), None));
        }
        // Every record shares one millisecond; storage returns them sorted by key.
        let mut trail = Vec::new();
        let mut add = |action, session: u8, outcome, intent_id| {
            let record = BucketAuditRecord {
                event_id: Ulid::from_parts(4, (1 << 64) + trail.len() as u128),
                bucket_id: BUCKET_ID,
                at_ms: 4,
                action,
                actor: None,
                node_id: iroh::SecretKey::from_bytes(&[2; 32]).public(),
                generation: Some(1),
                session_id: Some(Ulid::from_bytes([session; 16])),
                intent_id,
                sequence: None,
                deadline_ms: None,
                reason: None,
                outcome,
            };
            trail.push(record.clone());
            record.event_id
        };
        add(AuditAction::Unlock, 1, AuditOutcome::Applied, None);
        add(AuditAction::TimedLock, 1, AuditOutcome::Applied, None);
        // A later unlock and an extension both fail: the timed lock stays the reason.
        let unlock = add(AuditAction::Unlock, 2, AuditOutcome::Intent, None);
        add(AuditAction::Unlock, 2, AuditOutcome::Failed, Some(unlock));
        let extend = add(AuditAction::Extend, 1, AuditOutcome::Intent, None);
        add(AuditAction::Extend, 1, AuditOutcome::Failed, Some(extend));
        trail.sort_by_key(BucketAuditRecord::key);
        let rows = trail
            .iter()
            .map(|record| record.to_bytes().unwrap())
            .collect();
        operation.step(iter(rows, None));
        operation.step(Event::Blob(BlobEvent::KeyStatus {
            generations: Vec::new(),
        }));
        let snapshot = operation.finalize().unwrap();
        let locks = BTreeMap::from([(1, (AuditAction::TimedLock, 4))]);
        assert_eq!(snapshot.locks, locks);
    }

    #[test]
    fn rejects_foreign_event() {
        let mut operation = operation();
        operation.start();
        operation.step(iter(Vec::new(), None));
        assert!(matches!(
            operation.finalize(),
            Err(KeyStatusError::InvalidStateEvent { .. })
        ));
    }

    #[test]
    fn audit_page_cursor() {
        let record = |event: u64| BucketAuditRecord {
            event_id: Ulid::from_parts(event, 1),
            bucket_id: BUCKET_ID,
            at_ms: event,
            action: AuditAction::Lock,
            actor: None,
            node_id: iroh::SecretKey::from_bytes(&[2; 32]).public(),
            generation: Some(1),
            session_id: None,
            intent_id: None,
            sequence: None,
            deadline_ms: None,
            reason: None,
            outcome: AuditOutcome::Applied,
        };
        let cursor = Ulid::from_parts(1, 1);
        let mut full = AuditPageOperation::new(BUCKET_ID, Some(cursor), 2);
        let effects = full.start();
        let expected = [&BUCKET_ID.to_bytes()[..], &cursor.to_bytes()].concat();
        assert!(matches!(
            &effects[..],
            [Effect::Storage(StorageEffect::Iter { start: Some(IterStart::After(key)), limit: 2, .. })]
                if key.as_ref() == expected
        ));
        let rows = vec![record(2).to_bytes().unwrap(), record(3).to_bytes().unwrap()];
        full.step(iter(rows.clone(), Some(b"more")));
        let (events, next) = full.finalize().unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(next, Some(Ulid::from_parts(3, 1)));
        let mut last = AuditPageOperation::new(BUCKET_ID, None, 5);
        last.start();
        last.step(iter(rows, None));
        assert_eq!(last.finalize().unwrap().1, None);
    }
}
