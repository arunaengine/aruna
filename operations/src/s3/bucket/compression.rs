//! Changes the compression setting a bucket record carries for later writes. A change also
//! starts this node's migration of the bucket's stored copies.
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

/// Stores a new compression setting; returns the setting it replaced.
#[derive(Debug, PartialEq)]
pub struct PutCompressionOperation {
    bucket: String,
    group_id: GroupId,
    compression: Compression,
    now_ms: u64,
    changed: bool,
    state: PutCompressionState,
    txn_id: Option<TxnId>,
    output: Option<Result<Compression, PutCompressionError>>,
}

impl PutCompressionOperation {
    pub fn new(bucket: String, group_id: GroupId, compression: Compression, now_ms: u64) -> Self {
        Self {
            bucket,
            group_id,
            compression,
            now_ms,
            changed: false,
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

    fn write_bucket(&mut self, value: Option<Vec<u8>>) -> Effects {
        let Some(value) = value else {
            return self.fail(PutCompressionError::NoSuchBucket);
        };
        let mut info = match BucketInfo::from_bytes(&value) {
            Ok(info) => info,
            Err(error) => return self.fail(error.into()),
        };
        if info.group_id != self.group_id {
            return self.fail(PutCompressionError::GroupMismatch);
        }
        let previous = std::mem::replace(&mut info.compression, self.compression);
        self.changed = previous != self.compression;
        let mut writes = Vec::new();
        match info.to_bytes() {
            Ok(value) => writes.push((S3_BUCKET_KEYSPACE.to_string(), self.key(), value.into())),
            Err(error) => return self.fail(error.into()),
        }
        // A change restarts this node's migration; the record is replaced atomically with it.
        if self.changed {
            match CompressionMigration::new(self.compression, self.now_ms).to_bytes() {
                Ok(value) => writes.push((
                    COMPRESSION_MIGRATION_KEYSPACE.to_string(),
                    self.key(),
                    value.into(),
                )),
                Err(error) => return self.fail(error.into()),
            }
        }
        self.output = Some(Ok(previous));
        self.state = PutCompressionState::WriteBucket;
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: self.txn_id,
        })]
    }
}

impl Operation for PutCompressionOperation {
    type Output = Compression;
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
                match self.changed {
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
        operation.start();
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TxnId::default(),
        }));
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: b"b".to_vec().into(),
            value: info.map(|info| info.to_bytes().unwrap().into()),
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
        assert_eq!(operation.finalize(), Ok(Compression::Off));
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
