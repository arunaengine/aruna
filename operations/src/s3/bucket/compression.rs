//! Changes the compression setting a bucket record carries for later writes. A change also
//! starts this node's migration of the bucket's stored copies; the same setting again resumes
//! an unfinished migration or restarts one that left failed versions.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{COMPRESSION_MIGRATION_KEYSPACE, S3_BUCKET_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::format::{Compression, CompressionMigration};
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::types::{Effects, GroupId, Key, TxnId};
use smallvec::smallvec;
use std::time::Duration;
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PutCompressionState {
    Init,
    StartTransaction,
    ReadBucket,
    ReadMigration,
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
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: COMPRESSION_MIGRATION_KEYSPACE.to_string(),
            key: self.key(),
            txn_id: self.txn_id,
        })]
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
        // A change, or a finished run that left failed versions, starts a fresh run; the record
        // is replaced atomically with the setting. An unfinished run is only woken.
        let retry = stored
            .as_ref()
            .is_some_and(|record| record.finished_at_ms.is_some() && record.failed > 0);
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
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.unexpected("ReadResult", event);
                };
                self.write_bucket(value.map(|value| value.to_vec()))
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
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: b"b".to_vec().into(),
            value: migration.map(|record| record.to_bytes().unwrap().into()),
        }))
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
        let [(_, _, info), (space, _, record)] = writes.as_slice() else {
            panic!("expected bucket and migration rows, got {writes:?}")
        };
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
        assert_eq!(writes.len(), 2);
        assert_eq!(commit(&mut operation).len(), 1);
        let fresh = CompressionMigration::new(zstd, 5);
        assert_eq!(operation.finalize(), Ok(Some(fresh)));

        let running = CompressionMigration::new(zstd, 1);
        let mut operation = PutCompressionOperation::new("b".to_string(), group(), zstd, 5);
        let effects = read_with(&mut operation, Some(info), Some(running.clone()));
        let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
            panic!("expected one batch write, got {effects:?}")
        };
        assert_eq!(writes.len(), 1);
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
