//! Reads what the bucket encryption status reports: settings, key generations, grants, sealed
//! copies and the unlock state of each generation, and pages of the bucket's key audit.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::driver::DriverContext;
use crate::s3::bucket::key_rows::{SettingsError, authority_read, parse_authority};
use aruna_core::UserId;
use aruna_core::effects::{BlobEffect, Effect, IterStart, StorageEffect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{
    BUCKET_AUDIT_KEYSPACE, BUCKET_ENCRYPTION_KEYSPACE, BUCKET_HOLDER_KEYSPACE, BUCKET_KEY_KEYSPACE,
    KEY_COPY_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::encryption::{
    BucketEncryption, BucketHolder, BucketKeyRecord, SealedCopy, UnlockStatus,
};
use aruna_core::structs::storage::key_audit::BucketAuditRecord;
use aruna_core::types::{Effects, GroupId, Key, Value};
use smallvec::smallvec;
use std::collections::BTreeSet;
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
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StatusStep {
    Init,
    ReadBucket,
    Records,
    Grants,
    Copies,
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
        match self.bucket_id() {
            Some(_) => self.scan(StatusStep::Records, None),
            None => self.finish(),
        }
    }

    fn take_page(&mut self, values: Vec<(Key, Value)>, next: Option<Key>) -> Effects {
        for (key, value) in values {
            let parsed = match self.step {
                StatusStep::Records => BucketKeyRecord::from_bytes(&value)
                    .map(|record| self.snapshot.records.push(record)),
                StatusStep::Grants => {
                    BucketHolder::from_bytes(&value).map(|grant| self.snapshot.grants.push(grant))
                }
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
            _ => self.read_unlocks(),
        }
    }

    fn read_unlocks(&mut self) -> Effects {
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
            (
                StatusStep::Records | StatusStep::Grants | StatusStep::Copies,
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
    output: Option<Result<(Vec<BucketAuditRecord>, Option<Ulid>), KeyStatusError>>,
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
    use crate::s3::bucket::key_rows::authority_rows;
    use aruna_core::structs::storage::encryption::{BucketKeyRef, EncryptionMode};
    use aruna_core::structs::storage::format::Compression;
    use aruna_core::structs::storage::key_audit::{AuditAction, AuditOutcome};
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

    #[test]
    fn plain_bucket_stops() {
        let mut operation = operation();
        operation.start();
        let rows = authority_rows(&info(), None, &[user(2)]);
        let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: rows,
        }));
        assert!(effects.is_empty());
        let snapshot = operation.finalize().unwrap();
        assert_eq!(snapshot.settings.mode, EncryptionMode::Off);
        assert_eq!(snapshot.admins, BTreeSet::from([user(2)]));
        assert!(snapshot.records.is_empty());
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
