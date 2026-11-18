//! Changes a bucket's compression setting and starts this node's migration of its stored copies;
//! the same setting again resumes an unfinished migration or retries one with failed versions.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::s3::imports::LinkScan;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event};
use aruna_core::keyspaces::{
    BUCKET_ENCRYPTION_KEYSPACE, BUCKET_KEY_KEYSPACE, COMPRESSION_MIGRATION_KEYSPACE,
    COMPRESSION_QUEUE_KEYSPACE, S3_BUCKET_KEYSPACE, TRANSITION_KEYSPACE, TRANSITION_QUEUE_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{read_row, values};
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::encryption::{
    BucketEncryption, BucketKeyError, BucketKeyRecord, SealPlan, UnlockStatus,
};
use aruna_core::structs::storage::format::{Compression, CompressionMigration};
use aruna_core::structs::storage::transition::{
    EncryptionTransition, TransitionKind, TransitionTarget,
};
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::types::{GroupId, Key, Value};
use std::time::Duration;
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PutCompressionState {
    ReadBucket,
    CheckLinks,
    ReadMigration,
    CheckUnlocked,
    ReadKey,
    WriteBucket,
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
    #[error("{}", crate::s3::imports::MAPPED_REASON)]
    Mapped,
    #[error("PutBucketCompression lost the rows it read")]
    NotFinished,
    #[error("Unexpected event in state {state:?}: expected {expected}, got {received}")]
    InvalidStateEvent {
        state: &'static str,
        expected: &'static str,
        received: &'static str,
    },
}

impl From<IoError> for PutCompressionError {
    fn from(error: IoError) -> Self {
        Self::StorageError(error.into())
    }
}

type CompressionTurn = Result<Next<Option<CompressionMigration>>, PutCompressionError>;

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
    links: Option<LinkScan>,
    migration: Option<CompressionMigration>,
    state: PutCompressionState,
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
            links: None,
            migration: None,
            state: PutCompressionState::ReadBucket,
        }
    }

    fn key(&self) -> Key {
        self.bucket.as_bytes().to_vec().into()
    }

    fn call(&mut self, state: PutCompressionState, call: IoCall) -> CompressionTurn {
        self.state = state;
        Ok(Next::Call(call))
    }

    fn read_migration(&mut self, value: Option<Value>) -> CompressionTurn {
        let value = value.ok_or(PutCompressionError::NoSuchBucket)?;
        let info = BucketInfo::from_bytes(&value)?;
        if info.group_id != self.group_id {
            return Err(PutCompressionError::GroupMismatch);
        }
        self.info = Some(info);
        if self.compression == Compression::Off {
            return self.read_settings();
        }
        let links = LinkScan::new(self.group_id, &self.bucket);
        let page = links.page(None);
        self.links = Some(links);
        self.call(PutCompressionState::CheckLinks, page)
    }

    /// A bucket linked whole to a provider bucket keeps its bytes raw.
    fn links_scanned(&mut self, values: Vec<(Key, Value)>, next: Option<Key>) -> CompressionTurn {
        let links = self
            .links
            .as_mut()
            .ok_or(PutCompressionError::NotFinished)?;
        match links.scanned(values, next)? {
            Some(page) => Ok(Next::Call(page)),
            None if links.linked => Err(PutCompressionError::Mapped),
            None => self.read_settings(),
        }
    }

    fn read_settings(&mut self) -> CompressionTurn {
        let reads = vec![
            (COMPRESSION_MIGRATION_KEYSPACE.to_string(), self.key()),
            (BUCKET_ENCRYPTION_KEYSPACE.to_string(), self.key()),
            (TRANSITION_KEYSPACE.to_string(), self.key()),
        ];
        self.call(PutCompressionState::ReadMigration, IoCall::Read(reads))
    }

    /// An encrypted bucket re-encodes its archives through a transition instead of a migration.
    fn read_rows(
        &mut self,
        [migration, settings, transition]: [Option<Value>; 3],
    ) -> CompressionTurn {
        let transition = transition
            .map(|row| EncryptionTransition::from_bytes(&row))
            .transpose()?;
        if transition.is_some_and(|transition| transition.finished_at_ms.is_none()) {
            return Err(PutCompressionError::TransitionRunning);
        }
        let settings = BucketEncryption::from_row(settings.as_deref())?;
        let changed = self.info.as_ref().map(|info| info.compression) != Some(self.compression);
        let Some(active) = settings.active_key().filter(|_| changed) else {
            return self.write_bucket(migration);
        };
        self.settings = Some(settings);
        // Re-encoding opens every archive, so the active key must be unlocked before anything commits.
        let status = BlobEffect::ReadKeyStatus {
            bucket_id: active.bucket_id,
        };
        self.call(
            PutCompressionState::CheckUnlocked,
            Effect::Blob(status).into(),
        )
    }

    fn check_unlocked(&mut self, generations: &[UnlockStatus]) -> CompressionTurn {
        let active = (self.settings.as_ref())
            .and_then(BucketEncryption::active_key)
            .ok_or(PutCompressionError::NotFinished)?;
        if !generations
            .iter()
            .any(|status| status.key == active && status.active)
        {
            return Err(BucketKeyError::Locked(active.bucket_id).into());
        }
        let read = read_row(BUCKET_KEY_KEYSPACE, active.key());
        self.call(PutCompressionState::ReadKey, read)
    }

    /// Writes the setting, the next storage generation and a re-encode of every archive.
    fn write_sealed(&mut self, value: Option<Value>) -> CompressionTurn {
        let writes = self.sealed_rows(value)?;
        self.wake = true;
        self.migration = None;
        self.call(PutCompressionState::WriteBucket, IoCall::Write(writes))
    }

    fn sealed_rows(
        &mut self,
        value: Option<Value>,
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

    fn write_bucket(&mut self, value: Option<Value>) -> CompressionTurn {
        let stored = value
            .map(|value| CompressionMigration::from_bytes(&value))
            .transpose()?;
        // An unfinished move continues with the new setting.
        let moving = stored
            .as_ref()
            .filter(|record| record.moving && record.finished_at_ms.is_none())
            .map(|record| record.prefix.clone());
        let stored = stored.filter(|record| record.target == self.compression);
        let mut info = self.info.take().ok_or(PutCompressionError::NotFinished)?;
        let previous = std::mem::replace(&mut info.compression, self.compression);
        let mut writes = vec![(
            S3_BUCKET_KEYSPACE.to_string(),
            self.key(),
            info.to_bytes()?.into(),
        )];
        // A change, or a run that finished or waits with failed versions, starts a fresh run;
        // the record is replaced atomically with the setting. A running pass is only woken.
        let retry = stored.as_ref().is_some_and(|record| {
            record.failed > 0 && (record.finished_at_ms.is_some() || record.retry_at_ms.is_some())
        });
        let migration = match previous != self.compression || retry {
            true => {
                let mut record = CompressionMigration::new(self.compression, self.now_ms);
                if let Some(prefix) = moving {
                    (record.moving, record.prefix) = (true, prefix);
                }
                writes.push((
                    COMPRESSION_MIGRATION_KEYSPACE.to_string(),
                    self.key(),
                    record.to_bytes()?.into(),
                ));
                Some(record)
            }
            false => stored,
        };
        self.wake = (migration.as_ref()).is_some_and(|record| record.finished_at_ms.is_none());
        if self.wake {
            writes.push((
                COMPRESSION_QUEUE_KEYSPACE.to_string(),
                self.key(),
                Vec::<u8>::new().into(),
            ));
        }
        self.migration = migration;
        self.call(PutCompressionState::WriteBucket, IoCall::Write(writes))
    }

    /// The outcome; a pass to run is woken once the setting committed.
    fn written(&mut self) -> CompressionTurn {
        let output = self.migration.take();
        if !self.wake {
            return Ok(Next::Done(output));
        }
        let wake = Effect::Task(TaskEffect::ShortenTimer {
            key: TaskKey::MigrateCompression,
            after: Duration::ZERO,
        });
        Ok(Next::Then {
            output,
            calls: vec![wake.into()],
        })
    }
}

impl Operation for PutCompressionOperation {
    type Output = Option<CompressionMigration>;
    type Error = PutCompressionError;

    fn first(&mut self) -> CompressionTurn {
        self.compression.checked()?;
        let read = read_row(S3_BUCKET_KEYSPACE, self.key());
        self.call(PutCompressionState::ReadBucket, read)
    }

    fn next(&mut self, result: IoResult) -> CompressionTurn {
        match self.state {
            PutCompressionState::ReadBucket => self.read_migration(result.value()?),
            PutCompressionState::CheckLinks => {
                let (rows, next) = result.page()?;
                self.links_scanned(rows, next)
            }
            PutCompressionState::ReadMigration => self.read_rows(values::<3>(result)?),
            PutCompressionState::CheckUnlocked => match result.event()? {
                Event::Blob(BlobEvent::KeyStatus { generations }) => {
                    self.check_unlocked(&generations)
                }
                other => Err(PutCompressionError::InvalidStateEvent {
                    state: "CheckUnlocked",
                    expected: "KeyStatus",
                    received: other.label(),
                }),
            },
            PutCompressionState::ReadKey => self.write_sealed(result.value()?),
            PutCompressionState::WriteBucket => {
                result.done()?;
                self.written()
            }
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

/// Reads this node's migration record of one bucket, if a setting change created one.
#[derive(Debug, PartialEq)]
pub struct MigrationStatusOperation {
    bucket: String,
}

impl MigrationStatusOperation {
    pub fn new(bucket: String) -> Self {
        Self { bucket }
    }
}

impl Operation for MigrationStatusOperation {
    type Output = Option<CompressionMigration>;
    type Error = PutCompressionError;

    fn first(&mut self) -> CompressionTurn {
        let key = self.bucket.as_bytes().to_vec();
        Ok(Next::Call(read_row(COMPRESSION_MIGRATION_KEYSPACE, key)))
    }

    fn next(&mut self, result: IoResult) -> CompressionTurn {
        let record = result
            .value()?
            .map(|value| CompressionMigration::from_bytes(value.as_ref()))
            .transpose()?;
        Ok(Next::Done(record))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;
    use ulid::Ulid;

    type Migration = Option<CompressionMigration>;

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
            distribution: None,
        }
    }

    fn row(value: Option<Vec<u8>>) -> IoResult {
        IoResult::Values(vec![value.map(Into::into)])
    }

    fn no_rules() -> IoResult {
        IoResult::Page {
            rows: Vec::new(),
            next: None,
        }
    }

    fn put(compression: Compression) -> PutCompressionOperation {
        PutCompressionOperation::new("b".to_string(), group(), compression, 5)
    }

    /// Runs up to the migration read of a bucket of this node's group.
    fn at_rows(operation: &mut PutCompressionOperation, info: &BucketInfo) {
        assert_eq!(operation.transaction(), TransactionMode::Write);
        operation.first().unwrap();
        operation.next(row(Some(info.to_bytes().unwrap()))).unwrap();
        if operation.state == PutCompressionState::CheckLinks {
            operation.next(no_rules()).unwrap();
        }
        assert_eq!(operation.state, PutCompressionState::ReadMigration);
    }

    /// The answer to the read of migration, encryption settings and transition rows.
    fn rows(
        migration: Option<Value>,
        settings: Option<Value>,
        transition: Option<Value>,
    ) -> IoResult {
        IoResult::Values(vec![migration, settings, transition])
    }

    fn written(turn: &CompressionTurn) -> &Vec<(String, Key, Value)> {
        match turn {
            Ok(Next::Call(IoCall::Write(writes))) => writes,
            other => panic!("expected one write, got {other:?}"),
        }
    }

    fn woke(turn: CompressionTurn) -> Migration {
        match turn {
            Ok(Next::Then { output, calls }) => {
                assert_eq!(calls.len(), 1);
                assert!(matches!(
                    &calls[0],
                    IoCall::Effect(effect) if matches!(
                        effect.as_ref(),
                        Effect::Task(TaskEffect::ShortenTimer {
                            key: TaskKey::MigrateCompression,
                            ..
                        })
                    )
                ));
                output
            }
            other => panic!("expected the migration to be woken, got {other:?}"),
        }
    }

    #[test]
    fn mapped_refuses_compression() {
        use aruna_core::structs::storage::backend_import::{BackendImport, PLAIN_PATTERN};
        let mut operation = put(Compression::Zstd { level: 3 });
        operation.first().unwrap();
        let turn = operation.next(row(Some(bucket(group()).to_bytes().unwrap())));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Page { .. }))));
        let rule = BackendImport {
            import_id: Ulid::from_bytes([5; 16]),
            group_id: group(),
            backend_id: Ulid::from_bytes([4; 16]),
            backend_prefix: "team-a-b/".to_string(),
            bucket: "b".to_string(),
            key_prefix: String::new(),
            schedule: None,
            created_by: Default::default(),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            updated_at: std::time::SystemTime::UNIX_EPOCH,
            last_job: None,
            last_started_ms: None,
            write_backend_id: None,
            write_prefix: None,
            write_pattern: PLAIN_PATTERN.to_string(),
        };
        let page = IoResult::Page {
            rows: vec![(rule.key(), rule.to_bytes().unwrap().into())],
            next: None,
        };
        assert_eq!(operation.next(page), Err(PutCompressionError::Mapped));
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
    fn status(record: &BucketKeyRecord, unlocked: bool) -> IoResult {
        let session = UnlockStatus {
            key: record.key,
            session_id: Ulid::from_bytes([5; 16]),
            sequence: ulid::Ulid::from_parts(1, 1),
            deadline_ms: None,
            active: true,
            unlocked_at: SystemTime::UNIX_EPOCH,
            remaining: None,
            max_remaining: None,
        };
        let generations = if unlocked { vec![session] } else { Vec::new() };
        IoResult::Event(Box::new(Event::Blob(BlobEvent::KeyStatus { generations })))
    }

    #[test]
    fn locked_key_refuses() {
        // Nothing is written while the archives' key is locked on this node.
        let mut operation = put(Compression::Zstd { level: 7 });
        at_rows(&mut operation, &bucket(group()));
        let (settings, record) = encrypted();
        operation
            .next(rows(None, Some(settings.to_bytes().unwrap().into()), None))
            .unwrap();
        let locked = BucketKeyError::Locked(record.key.bucket_id);
        assert_eq!(
            operation.next(status(&record, false)),
            Err(PutCompressionError::Key(locked))
        );
    }

    #[test]
    fn encrypted_change_reencodes() {
        // Archives re-encode through a transition; no plain migration would read them.
        let zstd = Compression::Zstd { level: 7 };
        let mut operation = put(zstd);
        at_rows(&mut operation, &bucket(group()));
        let (settings, record) = encrypted();
        let turn = operation.next(rows(None, Some(settings.to_bytes().unwrap().into()), None));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Effect(_)))));
        let turn = operation.next(status(&record, true));
        assert_eq!(
            turn,
            Ok(Next::Call(read_row(BUCKET_KEY_KEYSPACE, record.key.key())))
        );

        let turn = operation.next(row(Some(record.to_bytes().unwrap())));
        let writes = written(&turn);
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
        assert_eq!(woke(operation.next(IoResult::Done)), None);
    }

    #[test]
    fn running_transition_conflicts() {
        let mut operation = put(Compression::Zstd { level: 7 });
        at_rows(&mut operation, &bucket(group()));
        let target = TransitionTarget {
            compression: Compression::Off,
            plan: None,
        };
        let running = EncryptionTransition::new(TransitionKind::Decrypt, None, target, 3, 1);
        assert_eq!(
            operation.next(rows(None, None, Some(running.to_bytes().unwrap().into()))),
            Err(PutCompressionError::TransitionRunning)
        );
    }

    #[test]
    fn change_starts_migration() {
        // The setting and a fresh migration record commit together, then the task runs.
        let zstd = Compression::Zstd { level: 7 };
        let mut operation = put(zstd);
        at_rows(&mut operation, &bucket(group()));

        let turn = operation.next(rows(None, None, None));

        let [(_, _, info), (space, _, record), (queue, _, _)] = written(&turn).as_slice() else {
            panic!("expected bucket, migration and queue rows, got {turn:?}")
        };
        assert_eq!(queue, COMPRESSION_QUEUE_KEYSPACE);
        assert_eq!(BucketInfo::from_bytes(info).unwrap().compression, zstd);
        assert_eq!(space, COMPRESSION_MIGRATION_KEYSPACE);
        assert_eq!(
            CompressionMigration::from_bytes(record).unwrap(),
            CompressionMigration::new(zstd, 5)
        );
        assert_eq!(
            woke(operation.next(IoResult::Done)),
            Some(CompressionMigration::new(zstd, 5))
        );
    }

    #[test]
    fn unchanged_stays_idle() {
        let mut operation = put(Compression::Off);
        at_rows(&mut operation, &bucket(group()));
        let turn = operation.next(rows(None, None, None));
        assert_eq!(written(&turn).len(), 1);
        assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(None)));
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
        let mut operation = put(zstd);
        at_rows(&mut operation, &info);
        let turn = operation.next(rows(Some(failed.to_bytes().unwrap().into()), None, None));
        assert_eq!(written(&turn).len(), 3);
        assert_eq!(
            woke(operation.next(IoResult::Done)),
            Some(CompressionMigration::new(zstd, 5))
        );

        let running = CompressionMigration::new(zstd, 1);
        let mut operation = put(zstd);
        at_rows(&mut operation, &info);
        let turn = operation.next(rows(Some(running.to_bytes().unwrap().into()), None, None));
        // The bucket row and the queue entry; the running record stays as it is.
        assert_eq!(written(&turn).len(), 2);
        assert_eq!(woke(operation.next(IoResult::Done)), Some(running));
    }

    #[test]
    fn refuses_bad_input() {
        let mut level = put(Compression::Zstd { level: 40 });
        assert!(matches!(
            level.first(),
            Err(PutCompressionError::ConversionError(_))
        ));

        let mut missing = put(Compression::Off);
        missing.first().unwrap();
        assert_eq!(
            missing.next(row(None)),
            Err(PutCompressionError::NoSuchBucket)
        );

        let mut foreign = put(Compression::Off);
        foreign.first().unwrap();
        let other = bucket(Ulid::from_bytes([9u8; 16])).to_bytes().unwrap();
        assert_eq!(
            foreign.next(row(Some(other))),
            Err(PutCompressionError::GroupMismatch)
        );
    }

    #[test]
    fn rejects_wrong_result() {
        let mut operation = put(Compression::Off);
        operation.first().unwrap();
        assert!(operation.next(IoResult::Done).is_err());
    }

    #[test]
    fn reads_migration_status() {
        let record = CompressionMigration::new(Compression::Off, 1);
        let mut status = MigrationStatusOperation::new("b".to_string());
        assert_eq!(
            status.first(),
            Ok(Next::Call(read_row(
                COMPRESSION_MIGRATION_KEYSPACE,
                b"b".to_vec()
            )))
        );
        assert_eq!(
            status.next(row(Some(record.to_bytes().unwrap()))),
            Ok(Next::Done(Some(record)))
        );
    }
}
