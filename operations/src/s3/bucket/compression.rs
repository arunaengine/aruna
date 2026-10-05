//! Changes a bucket's compression setting and starts this node's migration of its stored copies;
//! the same setting again resumes an unfinished migration or retries one with failed versions.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{
    BUCKET_ENCRYPTION_KEYSPACE, BUCKET_KEY_KEYSPACE, COMPRESSION_MIGRATION_KEYSPACE,
    COMPRESSION_QUEUE_KEYSPACE, S3_BUCKET_KEYSPACE, TRANSITION_KEYSPACE, TRANSITION_QUEUE_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::encryption::{
    BucketEncryption, BucketKeyError, BucketKeyRecord, SealPlan, UnlockStatus,
};
use aruna_core::structs::storage::format::{Compression, CompressionMigration};
use aruna_core::structs::storage::transition::{
    EncryptionTransition, TransitionKind, TransitionTarget,
};
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::types::{Effects, GroupId, Key, TxnId, Value};
use smallvec::smallvec;
use std::time::Duration;
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PutCompressionState {
    Init,
    StartTransaction,
    ReadBucket,
    ReadMigration,
    CheckUnlocked,
    ReadKey,
    WriteBucket,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum PutCompressionError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("The specified bucket does not exist.")]
    NoSuchBucket,
    #[error("The bucket changed owner while the setting was written")]
    GroupMismatch,
    #[error(transparent)]
    Key(#[from] BucketKeyError),
    /// Replacing an unfinished encryption transition would strand its source key.
    #[error("the bucket's stored copies are still moving to a new encryption")]
    TransitionRunning,
    #[error("PutBucketCompression did not finish")]
    NotFinished,
    #[error("Unexpected event in state {state:?}: expected {expected}, got {received:?}")]
    InvalidStateEvent {
        state: &'static str,
        expected: &'static str,
        received: Event,
    },
}

/// Stores a compression setting; returns this node's migration for it, if one exists.
#[derive(Debug, PartialEq)]
pub struct PutCompressionOperation {
    bucket: String,
    group_id: GroupId,
    compression: Compression,
    now_ms: u64,
    info: Option<BucketInfo>,
    /// The encryption settings of an encrypted bucket, read with the migration record.
    settings: Option<BucketEncryption>,
    /// Set when the commit must wake the migration task.
    wake: bool,
    state: PutCompressionState,
    txn_id: Option<TxnId>,
    output: Option<Result<Option<CompressionMigration>, PutCompressionError>>,
}

impl PutCompressionOperation {
    pub fn new(bucket: String, group_id: GroupId, compression: Compression, now_ms: u64) -> Self {
        Self {
            bucket,
            group_id,
            compression,
            now_ms,
            info: None,
            settings: None,
            wake: false,
            state: PutCompressionState::Init,
            txn_id: None,
            output: None,
        }
    }

    fn fail(&mut self, error: PutCompressionError) -> Effects {
        self.state = PutCompressionState::Error;
        self.output = Some(Err(error));
        self.abort()
    }

    fn key(&self) -> Key {
        self.bucket.as_bytes().to_vec().into()
    }

    fn unexpected(&mut self, expected: &'static str, received: Event) -> Effects {
        let state = match self.state {
            PutCompressionState::Init => "Init",
            PutCompressionState::StartTransaction => "StartTransaction",
            PutCompressionState::ReadBucket => "ReadBucket",
            PutCompressionState::ReadMigration => "ReadMigration",
            PutCompressionState::CheckUnlocked => "CheckUnlocked",
            PutCompressionState::ReadKey => "ReadKey",
            PutCompressionState::WriteBucket => "WriteBucket",
            PutCompressionState::CommitTransaction => "CommitTransaction",
            PutCompressionState::Finish => "Finish",
            PutCompressionState::Error => "Error",
        };
        self.fail(PutCompressionError::InvalidStateEvent {
            state,
            expected,
            received,
        })
    }

    fn read_migration(&mut self, value: Option<Vec<u8>>) -> Effects {
        let Some(value) = value else {
            return self.fail(PutCompressionError::NoSuchBucket);
        };
        let info = match BucketInfo::from_bytes(&value) {
            Ok(info) => info,
            Err(error) => return self.fail(error.into()),
        };
        if info.group_id != self.group_id {
            return self.fail(PutCompressionError::GroupMismatch);
        }
        self.info = Some(info);
        self.state = PutCompressionState::ReadMigration;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (COMPRESSION_MIGRATION_KEYSPACE.to_string(), self.key()),
                (BUCKET_ENCRYPTION_KEYSPACE.to_string(), self.key()),
                (TRANSITION_KEYSPACE.to_string(), self.key()),
            ],
            txn_id: self.txn_id,
        })]
    }

    /// An encrypted bucket re-encodes its archives through a transition instead of a migration.
    fn read_rows(&mut self, values: Vec<(Key, Option<Value>)>) -> Effects {
        let mut rows = values.into_iter().map(|(_, value)| value);
        let (Some(migration), Some(settings), Some(transition), None) =
            (rows.next(), rows.next(), rows.next(), rows.next())
        else {
            return self.fail(PutCompressionError::NotFinished);
        };
        let transition = transition.map(|row| EncryptionTransition::from_bytes(&row));
        match transition.transpose() {
            Ok(Some(transition)) if transition.finished_at_ms.is_none() => {
                return self.fail(PutCompressionError::TransitionRunning);
            }
            Ok(_) => {}
            Err(error) => return self.fail(error.into()),
        }
        let settings = match BucketEncryption::from_row(settings.as_deref()) {
            Ok(settings) => settings,
            Err(error) => return self.fail(error.into()),
        };
        let changed = self.info.as_ref().map(|info| info.compression) != Some(self.compression);
        let Some(active) = settings.active_key().filter(|_| changed) else {
            return self.write_bucket(migration.map(|value| value.to_vec()));
        };
        self.settings = Some(settings);
        // Re-encoding opens every archive, so the active key must be unlocked before anything commits.
        self.state = PutCompressionState::CheckUnlocked;
        smallvec![Effect::Blob(BlobEffect::ReadKeyStatus {
            bucket_id: active.bucket_id
        })]
    }

    fn check_unlocked(&mut self, generations: &[UnlockStatus]) -> Effects {
        let Some(active) = self
            .settings
            .as_ref()
            .and_then(BucketEncryption::active_key)
        else {
            return self.fail(PutCompressionError::NotFinished);
        };
        if !generations
            .iter()
            .any(|status| status.key == active && status.active)
        {
            return self.fail(BucketKeyError::Locked(active.bucket_id).into());
        }
        self.state = PutCompressionState::ReadKey;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BUCKET_KEY_KEYSPACE.to_string(),
            key: active.key().into(),
            txn_id: self.txn_id,
        })]
    }

    /// Writes the setting, the next storage generation and a re-encode of every archive.
    fn write_sealed(&mut self, value: Option<Vec<u8>>) -> Effects {
        match self.sealed_rows(value) {
            Ok(writes) => {
                self.wake = true;
                self.output = Some(Ok(None));
                self.state = PutCompressionState::WriteBucket;
                smallvec![Effect::Storage(StorageEffect::BatchWrite {
                    writes,
                    txn_id: self.txn_id,
                })]
            }
            Err(error) => self.fail(error),
        }
    }

    fn sealed_rows(
        &mut self,
        value: Option<Vec<u8>>,
    ) -> Result<Vec<(String, Key, Value)>, PutCompressionError> {
        let record = BucketKeyRecord::from_bytes(&value.ok_or(PutCompressionError::NotFinished)?)?;
        let (Some(mut info), Some(mut settings)) = (self.info.take(), self.settings.take()) else {
            return Err(PutCompressionError::NotFinished);
        };
        info.compression = self.compression;
        settings.storage_generation += 1;
        let plan = SealPlan::capture(&settings, &record)?;
        let target = TransitionTarget {
            compression: self.compression,
            plan,
        };
        let (kind, generation) = (TransitionKind::Reencode, settings.storage_generation);
        let transition =
            EncryptionTransition::new(kind, Some(record.key), target, generation, self.now_ms);
        Ok(vec![
            (
                S3_BUCKET_KEYSPACE.to_string(),
                self.key(),
                info.to_bytes()?.into(),
            ),
            (
                BUCKET_ENCRYPTION_KEYSPACE.to_string(),
                self.key(),
                settings.to_bytes()?.into(),
            ),
            (
                TRANSITION_KEYSPACE.to_string(),
                self.key(),
                transition.to_bytes()?.into(),
            ),
            (
                TRANSITION_QUEUE_KEYSPACE.to_string(),
                self.key(),
                Vec::new().into(),
            ),
        ])
    }

    fn write_bucket(&mut self, value: Option<Vec<u8>>) -> Effects {
        let stored = match value.map(|value| CompressionMigration::from_bytes(&value)) {
            Some(Ok(record)) => Some(record).filter(|record| record.target == self.compression),
            Some(Err(error)) => return self.fail(error.into()),
            None => None,
        };
        let Some(mut info) = self.info.take() else {
            return self.fail(PutCompressionError::NotFinished);
        };
        let previous = std::mem::replace(&mut info.compression, self.compression);
        let mut writes = Vec::new();
        match info.to_bytes() {
            Ok(value) => writes.push((S3_BUCKET_KEYSPACE.to_string(), self.key(), value.into())),
            Err(error) => return self.fail(error.into()),
        }
        // A change, or a run that finished or waits with failed versions, starts a fresh run;
        // the record is replaced atomically with the setting. A running pass is only woken.
        let retry = stored.as_ref().is_some_and(|record| {
            record.failed > 0 && (record.finished_at_ms.is_some() || record.retry_at_ms.is_some())
        });
        let migration = match previous != self.compression || retry {
            true => {
                let record = CompressionMigration::new(self.compression, self.now_ms);
                match record.to_bytes() {
                    Ok(value) => writes.push((
                        COMPRESSION_MIGRATION_KEYSPACE.to_string(),
                        self.key(),
                        value.into(),
                    )),
                    Err(error) => return self.fail(error.into()),
                }
                Some(record)
            }
            false => stored,
        };
        self.wake = migration
            .as_ref()
            .is_some_and(|record| record.finished_at_ms.is_none());
        if self.wake {
            writes.push((
                COMPRESSION_QUEUE_KEYSPACE.to_string(),
                self.key(),
                Vec::<u8>::new().into(),
            ));
        }
        self.output = Some(Ok(migration));
        self.state = PutCompressionState::WriteBucket;
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: self.txn_id,
        })]
    }
}

impl Operation for PutCompressionOperation {
    type Output = Option<CompressionMigration>;
    type Error = PutCompressionError;

    fn start(&mut self) -> Effects {
        if let Err(error) = self.compression.checked() {
            return self.fail(error.into());
        }
        self.state = PutCompressionState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = &event {
            return self.fail(error.clone().into());
        }
        match self.state {
            PutCompressionState::Init => self.start(),
            PutCompressionState::StartTransaction => {
                let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
                    return self.unexpected("TransactionStarted", event);
                };
                self.txn_id = Some(txn_id);
                self.state = PutCompressionState::ReadBucket;
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: S3_BUCKET_KEYSPACE.to_string(),
                    key: self.key(),
                    txn_id: Some(txn_id),
                })]
            }
            PutCompressionState::ReadBucket => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.unexpected("ReadResult", event);
                };
                self.read_migration(value.map(|value| value.to_vec()))
            }
            PutCompressionState::ReadMigration => {
                let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
                    return self.unexpected("BatchReadResult", event);
                };
                self.read_rows(values)
            }
            PutCompressionState::CheckUnlocked => {
                let Event::Blob(BlobEvent::KeyStatus { generations }) = event else {
                    return self.unexpected("KeyStatus", event);
                };
                self.check_unlocked(&generations)
            }
            PutCompressionState::ReadKey => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.unexpected("ReadResult", event);
                };
                self.write_sealed(value.map(|value| value.to_vec()))
            }
            PutCompressionState::WriteBucket => {
                let Event::Storage(StorageEvent::BatchWriteResult { .. }) = event else {
                    return self.unexpected("BatchWriteResult", event);
                };
                let Some(txn_id) = self.txn_id else {
                    return self.unexpected("an open transaction", event);
                };
                self.state = PutCompressionState::CommitTransaction;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            PutCompressionState::CommitTransaction => {
                let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
                    return self.unexpected("TransactionCommitted", event);
                };
                self.txn_id = None;
                self.state = PutCompressionState::Finish;
                match self.wake {
                    true => smallvec![Effect::Task(TaskEffect::ShortenTimer {
                        key: TaskKey::MigrateCompression,
                        after: Duration::ZERO,
                    })],
                    false => smallvec![],
                }
            }
            PutCompressionState::Finish => smallvec![],
            PutCompressionState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            PutCompressionState::Finish | PutCompressionState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.state {
            PutCompressionState::Finish | PutCompressionState::Error => {
                self.output.unwrap_or(Err(PutCompressionError::NotFinished))
            }
            _ => Err(PutCompressionError::NotFinished),
        }
    }

    fn abort(&mut self) -> Effects {
        self.txn_id
            .take()
            .map_or_else(smallvec::SmallVec::new, |txn_id| {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            })
    }
}

/// Reads this node's migration record of one bucket, if a setting change created one.
#[derive(Debug, PartialEq)]
pub struct MigrationStatusOperation {
    bucket: String,
    output: Option<Result<Option<CompressionMigration>, PutCompressionError>>,
}

impl MigrationStatusOperation {
    pub fn new(bucket: String) -> Self {
        Self {
            bucket,
            output: None,
        }
    }
}

impl Operation for MigrationStatusOperation {
    type Output = Option<CompressionMigration>;
    type Error = PutCompressionError;

    fn start(&mut self) -> Effects {
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: COMPRESSION_MIGRATION_KEYSPACE.to_string(),
            key: self.bucket.as_bytes().to_vec().into(),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        self.output = Some(match event {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => value
                .map(|value| CompressionMigration::from_bytes(value.as_ref()))
                .transpose()
                .map_err(Into::into),
            Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
            received => Err(PutCompressionError::InvalidStateEvent {
                state: "ReadMigration",
                expected: "ReadResult",
                received,
            }),
        });
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(PutCompressionError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;
    use ulid::Ulid;

    fn group() -> Ulid {
        Ulid::from_bytes([3u8; 16])
    }

    fn bucket(group_id: Ulid) -> BucketInfo {
        BucketInfo {
            group_id,
            created_at: SystemTime::UNIX_EPOCH,
            created_by: aruna_core::UserId::default(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
        }
    }

    fn read(operation: &mut PutCompressionOperation, info: Option<BucketInfo>) -> Effects {
        read_with(operation, info, None)
    }

    /// Answers the bucket read and, once it passed, the migration read.
    fn read_with(
        operation: &mut PutCompressionOperation,
        info: Option<BucketInfo>,
        migration: Option<CompressionMigration>,
    ) -> Effects {
        operation.start();
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TxnId::default(),
        }));
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: b"b".to_vec().into(),
            value: info.map(|info| info.to_bytes().unwrap().into()),
        }));
        if operation.state != PutCompressionState::ReadMigration {
            return effects;
        }
        let migration = migration.map(|record| record.to_bytes().unwrap().into());
        operation.step(rows(migration, None, None))
    }

    /// The answer to the read of migration, encryption settings and transition rows.
    fn rows(migration: Option<Value>, settings: Option<Value>, transition: Option<Value>) -> Event {
        let values = [migration, settings, transition];
        Event::Storage(StorageEvent::BatchReadResult {
            values: values
                .into_iter()
                .map(|value| (b"b".to_vec().into(), value))
                .collect(),
        })
    }

    fn encrypted() -> (BucketEncryption, BucketKeyRecord) {
        use aruna_core::structs::storage::encryption::{BucketKeyRef, EncryptionMode};
        let settings = BucketEncryption {
            mode: EncryptionMode::NodeManaged,
            bucket_id: Some(Ulid::from_bytes([6; 16])),
            key_generation: 2,
            storage_generation: 7,
            ..BucketEncryption::default()
        };
        let key = BucketKeyRef::new(Ulid::from_bytes([6; 16]), 2);
        (
            settings,
            BucketKeyRecord::new(key, Ulid::from_bytes([8; 16]), [9; 32], 1),
        )
    }

    /// The registry's answer: the active generation unlocked, or nothing unlocked.
    fn status(record: &BucketKeyRecord, unlocked: bool) -> Event {
        let session = UnlockStatus {
            key: record.key,
            session_id: Ulid::from_bytes([5; 16]),
            active: true,
            unlocked_at: SystemTime::UNIX_EPOCH,
            remaining: None,
            max_remaining: None,
        };
        let generations = if unlocked { vec![session] } else { Vec::new() };
        Event::Blob(BlobEvent::KeyStatus { generations })
    }

    #[test]
    fn locked_key_refuses() {
        // Nothing is written while the archives' key is locked on this node.
        let zstd = Compression::Zstd { level: 7 };
        let mut operation = PutCompressionOperation::new("b".to_string(), group(), zstd, 5);
        operation.start();
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TxnId::default(),
        }));
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: b"b".to_vec().into(),
            value: Some(bucket(group()).to_bytes().unwrap().into()),
        }));
        let (settings, record) = encrypted();
        operation.step(rows(None, Some(settings.to_bytes().unwrap().into()), None));

        let effects = operation.step(status(&record, false));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { .. })]
        ));
        let locked = BucketKeyError::Locked(record.key.bucket_id);
        assert_eq!(operation.finalize(), Err(PutCompressionError::Key(locked)));
    }

    #[test]
    fn encrypted_change_reencodes() {
        // Archives re-encode through a transition; no plain migration would read them.
        let zstd = Compression::Zstd { level: 7 };
        let mut operation = PutCompressionOperation::new("b".to_string(), group(), zstd, 5);
        operation.start();
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TxnId::default(),
        }));
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: b"b".to_vec().into(),
            value: Some(bucket(group()).to_bytes().unwrap().into()),
        }));
        let (settings, record) = encrypted();
        let effects = operation.step(rows(None, Some(settings.to_bytes().unwrap().into()), None));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::ReadKeyStatus { .. })]
        ));
        let effects = operation.step(status(&record, true));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { key_space, .. })] if key_space == BUCKET_KEY_KEYSPACE
        ));

        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: b"b".to_vec().into(),
            value: Some(record.to_bytes().unwrap().into()),
        }));

        let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
            panic!("expected one batch write, got {effects:?}")
        };
        let spaces: Vec<&str> = writes.iter().map(|(space, _, _)| space.as_str()).collect();
        assert_eq!(
            spaces,
            [
                S3_BUCKET_KEYSPACE,
                BUCKET_ENCRYPTION_KEYSPACE,
                TRANSITION_KEYSPACE,
                TRANSITION_QUEUE_KEYSPACE
            ]
        );
        let stored = BucketEncryption::from_bytes(&writes[1].2).unwrap();
        assert_eq!(stored.storage_generation, 8);
        let transition = EncryptionTransition::from_bytes(&writes[2].2).unwrap();
        assert_eq!(transition.kind, TransitionKind::Reencode);
        assert_eq!(transition.source, Some(record.key));
        assert_eq!(transition.target.compression, zstd);
        assert_eq!(transition.target.plan.unwrap().storage_generation, 8);
        assert!(matches!(
            commit(&mut operation).as_slice(),
            [Effect::Task(TaskEffect::ShortenTimer { .. })]
        ));
        assert_eq!(operation.finalize(), Ok(None));
    }

    #[test]
    fn running_transition_conflicts() {
        let zstd = Compression::Zstd { level: 7 };
        let mut operation = PutCompressionOperation::new("b".to_string(), group(), zstd, 5);
        operation.start();
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TxnId::default(),
        }));
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: b"b".to_vec().into(),
            value: Some(bucket(group()).to_bytes().unwrap().into()),
        }));
        let target = TransitionTarget {
            compression: Compression::Off,
            plan: None,
        };
        let running = EncryptionTransition::new(TransitionKind::Decrypt, None, target, 3, 1);

        let effects = operation.step(rows(None, None, Some(running.to_bytes().unwrap().into())));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { .. })]
        ));
        assert_eq!(
            operation.finalize(),
            Err(PutCompressionError::TransitionRunning)
        );
    }

    fn commit(operation: &mut PutCompressionOperation) -> Effects {
        let commit = operation.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));
        assert!(matches!(
            commit.as_slice(),
            [Effect::Storage(StorageEffect::CommitTransaction { .. })]
        ));
        operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id: TxnId::default(),
        }))
    }

    #[test]
    fn change_starts_migration() {
        // The setting and a fresh migration record commit together, then the task runs.
        let zstd = Compression::Zstd { level: 7 };
        let mut operation = PutCompressionOperation::new("b".to_string(), group(), zstd, 5);

        let effects = read(&mut operation, Some(bucket(group())));

        let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
            panic!("expected one batch write, got {effects:?}")
        };
        let [(_, _, info), (space, _, record), (queue, _, _)] = writes.as_slice() else {
            panic!("expected bucket, migration and queue rows, got {writes:?}")
        };
        assert_eq!(queue, COMPRESSION_QUEUE_KEYSPACE);
        assert_eq!(BucketInfo::from_bytes(info).unwrap().compression, zstd);
        assert_eq!(space, COMPRESSION_MIGRATION_KEYSPACE);
        assert_eq!(
            CompressionMigration::from_bytes(record).unwrap(),
            CompressionMigration::new(zstd, 5)
        );
        let scheduled = commit(&mut operation);
        assert!(matches!(
            scheduled.as_slice(),
            [Effect::Task(TaskEffect::ShortenTimer {
                key: TaskKey::MigrateCompression,
                ..
            })]
        ));
        assert_eq!(
            operation.finalize(),
            Ok(Some(CompressionMigration::new(zstd, 5)))
        );
    }

    #[test]
    fn unchanged_stays_idle() {
        let mut operation =
            PutCompressionOperation::new("b".to_string(), group(), Compression::Off, 5);

        let effects = read(&mut operation, Some(bucket(group())));

        let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
            panic!("expected one batch write, got {effects:?}")
        };
        assert_eq!(writes.len(), 1);
        assert!(commit(&mut operation).is_empty());
        assert_eq!(operation.finalize(), Ok(None));
    }

    #[test]
    fn same_setting_retries() {
        // A finished run with failures restarts; an unfinished run is woken and reported.
        let zstd = Compression::Zstd { level: 3 };
        let mut info = bucket(group());
        info.compression = zstd;
        let mut failed = CompressionMigration::new(zstd, 1);
        failed.failed = 2;
        failed.finished_at_ms = Some(4);
        let mut operation = PutCompressionOperation::new("b".to_string(), group(), zstd, 5);

        let effects = read_with(&mut operation, Some(info.clone()), Some(failed));

        let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
            panic!("expected one batch write, got {effects:?}")
        };
        assert_eq!(writes.len(), 3);
        assert_eq!(commit(&mut operation).len(), 1);
        let fresh = CompressionMigration::new(zstd, 5);
        assert_eq!(operation.finalize(), Ok(Some(fresh)));

        let running = CompressionMigration::new(zstd, 1);
        let mut operation = PutCompressionOperation::new("b".to_string(), group(), zstd, 5);
        let effects = read_with(&mut operation, Some(info), Some(running.clone()));
        let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
            panic!("expected one batch write, got {effects:?}")
        };
        // The bucket row and the queue entry; the running record stays as it is.
        assert_eq!(writes.len(), 2);
        assert!(matches!(
            commit(&mut operation).as_slice(),
            [Effect::Task(TaskEffect::ShortenTimer { .. })]
        ));
        assert_eq!(operation.finalize(), Ok(Some(running)));
    }

    #[test]
    fn refuses_bad_input() {
        let mut level = PutCompressionOperation::new(
            "b".to_string(),
            group(),
            Compression::Zstd { level: 40 },
            5,
        );
        assert!(level.start().is_empty());
        assert!(matches!(
            level.finalize(),
            Err(PutCompressionError::ConversionError(_))
        ));

        let mut missing =
            PutCompressionOperation::new("b".to_string(), group(), Compression::Off, 5);
        let effects = read(&mut missing, None);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { .. })]
        ));
        assert_eq!(missing.finalize(), Err(PutCompressionError::NoSuchBucket));

        let mut foreign =
            PutCompressionOperation::new("b".to_string(), group(), Compression::Off, 5);
        read(&mut foreign, Some(bucket(Ulid::from_bytes([9u8; 16]))));
        assert_eq!(foreign.finalize(), Err(PutCompressionError::GroupMismatch));
    }

    #[test]
    fn rejects_wrong_event() {
        let mut operation =
            PutCompressionOperation::new("b".to_string(), group(), Compression::Off, 5);
        operation.start();

        operation.step(Event::Storage(StorageEvent::WriteResult {
            key: b"b".to_vec().into(),
        }));

        assert!(matches!(
            operation.finalize(),
            Err(PutCompressionError::InvalidStateEvent { .. })
        ));
    }
}
