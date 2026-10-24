//! Starts this node's move of a bucket's stored copies to the storage its rules choose now.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BUCKET_ENCRYPTION_KEYSPACE, COMPRESSION_MIGRATION_KEYSPACE, COMPRESSION_QUEUE_KEYSPACE,
    LINKED_JOB_KEYSPACE, RENAME_MOVE_KEYSPACE, RENAME_TOTAL_KEYSPACE, S3_BUCKET_KEYSPACE,
    TRANSITION_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::backend_import::{RenameRequest, RenameTotal, TokenGrant};
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::encryption::BucketEncryption;
use aruna_core::structs::storage::format::{Compression, CompressionMigration};
use aruna_core::structs::storage::linked_job::LinkedJob;
use aruna_core::structs::storage::transition::EncryptionTransition;
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::types::{Effects, GroupId, Key, TxnId, Value};
use smallvec::smallvec;
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum StartMoveError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("The specified bucket does not exist.")]
    NoSuchBucket,
    #[error("an encrypted bucket keeps its stored copies where they are")]
    Encrypted,
    #[error("a move of the keys under `{0}` is still running")]
    MoveRunning(String),
    #[error("the storage move did not finish")]
    NotFinished,
    #[error("the import rule changed since its files were checked")]
    RuleChanged,
    #[error("a re-mapping or duplicate cleanup of the bucket runs")]
    JobRunning,
    #[error("unexpected event: {0:?}")]
    Unexpected(Box<Event>),
}

/// The move record that starts or resumes a move of the keys under `prefix`. An unfinished
/// move with the same prefix resumes, or restarts once it waits with failures; an unfinished
/// move with another prefix, also one waiting for a retry, refuses the new one.
pub fn move_record(
    stored: Option<CompressionMigration>,
    compression: Compression,
    prefix: &str,
    now_ms: u64,
) -> Result<CompressionMigration, StartMoveError> {
    let running = stored.filter(|record| record.moving && record.finished_at_ms.is_none());
    if let Some(record) = running {
        if record.prefix != prefix {
            return Err(StartMoveError::MoveRunning(record.prefix));
        }
        let waiting = record.retry_at_ms.is_some() || record.failed > 0;
        if record.target == compression && !waiting {
            return Ok(record);
        }
    }
    let mut record = CompressionMigration::new(compression, now_ms);
    (record.moving, record.prefix) = (true, prefix.to_string());
    Ok(record)
}

/// The record and queue rows that hand a move to the migration task.
pub fn move_rows(
    bucket: &str,
    record: &CompressionMigration,
) -> Result<Vec<(String, Key, Value)>, ConversionError> {
    let key: Key = bucket.as_bytes().to_vec().into();
    Ok(vec![
        (
            COMPRESSION_MIGRATION_KEYSPACE.to_string(),
            key.clone(),
            record.to_bytes()?.into(),
        ),
        (
            COMPRESSION_QUEUE_KEYSPACE.to_string(),
            key,
            Vec::<u8>::new().into(),
        ),
    ])
}

/// The rows a move start reads in its transaction; the bucket record comes first.
pub fn move_reads(bucket: &str) -> Vec<(String, Key)> {
    let key: Key = bucket.as_bytes().to_vec().into();
    vec![
        (S3_BUCKET_KEYSPACE.to_string(), key.clone()),
        (BUCKET_ENCRYPTION_KEYSPACE.to_string(), key.clone()),
        (TRANSITION_KEYSPACE.to_string(), key.clone()),
        (COMPRESSION_MIGRATION_KEYSPACE.to_string(), key),
    ]
}

/// Refuses an encrypted or encrypting bucket and answers the stored move record.
pub fn check_movable(
    settings: Option<&[u8]>,
    transition: Option<&[u8]>,
    stored: Option<&[u8]>,
) -> Result<Option<CompressionMigration>, StartMoveError> {
    let transition = transition
        .map(EncryptionTransition::from_bytes)
        .transpose()?;
    let running = transition.is_some_and(|transition| transition.finished_at_ms.is_none());
    if running || BucketEncryption::from_row(settings)?.is_encrypted() {
        return Err(StartMoveError::Encrypted);
    }
    Ok(stored.map(CompressionMigration::from_bytes).transpose()?)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StartMoveState {
    Init,
    Start,
    Read,
    Write,
    Commit,
    Finish,
}

/// Writes a move record for the keys of `bucket` under `prefix` and wakes the migration task.
#[derive(Debug, PartialEq)]
pub struct StartMoveOperation {
    bucket: String,
    group_id: GroupId,
    prefix: String,
    now_ms: u64,
    /// Also gives linked files of earlier patterns their current name, for this caller, with
    /// the files the rename plans to rename.
    renaming: Option<(AuthContext, NodeId, TokenGrant, u64)>,
    /// Values read when the caller's access to the renamed files was checked; the move
    /// starts only while they are unchanged.
    guards: Vec<(String, Key, Option<Value>)>,
    state: StartMoveState,
    txn_id: Option<TxnId>,
    output: Option<Result<CompressionMigration, StartMoveError>>,
}

impl StartMoveOperation {
    pub fn new(bucket: String, group_id: GroupId, prefix: String, now_ms: u64) -> Self {
        Self {
            bucket,
            group_id,
            prefix,
            now_ms,
            renaming: None,
            guards: Vec::new(),
            state: StartMoveState::Init,
            txn_id: None,
            output: None,
        }
    }

    /// Asks the move to rename linked files of earlier patterns, each only while `auth` may
    /// write its key; an ordinary move never renames. `total` is the files the rename plans to
    /// rename, stored with the request.
    pub fn renaming(
        mut self,
        (auth, node_id): (AuthContext, NodeId),
        grant: TokenGrant,
        total: u64,
    ) -> Self {
        self.renaming = Some((auth, node_id, grant, total));
        self
    }

    /// Starts only while `guards` are stored as they were read.
    pub fn with_guards(mut self, guards: Vec<(String, Key, Option<Value>)>) -> Self {
        self.guards = guards;
        self
    }

    fn fail(&mut self, error: StartMoveError) -> Effects {
        self.state = StartMoveState::Finish;
        self.output = Some(Err(error));
        self.abort()
    }

    fn write(&mut self, mut values: Vec<(Key, Option<Value>)>) -> Effects {
        let seen = values.split_off(values.len().saturating_sub(self.guards.len()));
        let expected = self.guards.iter().map(|(.., value)| value);
        if seen.len() != self.guards.len() || !seen.iter().map(|(_, value)| value).eq(expected) {
            return self.fail(StartMoveError::RuleChanged);
        }
        let total = values.pop().and_then(|(_, value)| value);
        let total = match total.as_deref().map(RenameTotal::from_bytes).transpose() {
            Ok(total) => total,
            Err(error) => return self.fail(error.into()),
        };
        // A re-mapping or duplicate cleanup of the bucket holds it until it ends.
        let linked = values.pop().and_then(|(_, value)| value);
        match linked.as_deref().map(LinkedJob::from_bytes).transpose() {
            Ok(Some(job)) if job.is_active() => return self.fail(StartMoveError::JobRunning),
            Ok(_) => {}
            Err(error) => return self.fail(error.into()),
        }
        match self.rows(values, total) {
            Ok(writes) => {
                self.state = StartMoveState::Write;
                smallvec![Effect::Storage(StorageEffect::BatchWrite {
                    writes,
                    txn_id: self.txn_id,
                })]
            }
            Err(error) => self.fail(error),
        }
    }

    /// The rows of the move; `planned` is the total an earlier start of a rename stored.
    fn rows(
        &mut self,
        values: Vec<(Key, Option<Value>)>,
        planned: Option<RenameTotal>,
    ) -> Result<Vec<(String, Key, Value)>, StartMoveError> {
        let Ok([info, settings, transition, stored]) =
            <[_; 4]>::try_from(values).map(|values| values.map(|(_, value)| value))
        else {
            return Err(StartMoveError::NotFinished);
        };
        let info = info.ok_or(StartMoveError::NoSuchBucket)?;
        let info = BucketInfo::from_bytes(info.as_ref())?;
        if info.group_id != self.group_id {
            return Err(StartMoveError::NoSuchBucket);
        }
        let stored = check_movable(
            settings.as_deref(),
            transition.as_deref(),
            stored.as_deref(),
        )?;
        let record = move_record(stored, info.compression, &self.prefix, self.now_ms)?;
        let mut writes = move_rows(&self.bucket, &record)?;
        if let Some((auth, node_id, grant, total)) = self.renaming.clone() {
            let started_at_ms = record.started_at_ms;
            let request = RenameRequest {
                started_at_ms,
                auth,
                node_id,
                grant,
                paused: false,
                aborted_at_ms: None,
            };
            let key: Key = self.bucket.as_bytes().to_vec().into();
            let value = request.to_bytes()?.into();
            writes.push((RENAME_MOVE_KEYSPACE.to_string(), key.clone(), value));
            // A start repeated for the same move keeps the total its first start planned.
            let kept = planned.filter(|planned| planned.started_at_ms == started_at_ms);
            let total = kept.unwrap_or(RenameTotal {
                started_at_ms,
                total,
            });
            writes.push((
                RENAME_TOTAL_KEYSPACE.to_string(),
                key,
                total.to_bytes()?.into(),
            ));
        }
        self.output = Some(Ok(record));
        Ok(writes)
    }
}

impl Operation for StartMoveOperation {
    type Output = CompressionMigration;
    type Error = StartMoveError;

    fn start(&mut self) -> Effects {
        self.state = StartMoveState::Start;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }
        match (self.state, event) {
            (
                StartMoveState::Start,
                Event::Storage(StorageEvent::TransactionStarted { txn_id }),
            ) => {
                self.txn_id = Some(txn_id);
                self.state = StartMoveState::Read;
                let guards = self
                    .guards
                    .iter()
                    .map(|(space, key, _)| (space.clone(), key.clone()));
                let mut reads = move_reads(&self.bucket);
                let key: Key = self.bucket.as_bytes().to_vec().into();
                reads.push((LINKED_JOB_KEYSPACE.to_string(), key.clone()));
                reads.push((RENAME_TOTAL_KEYSPACE.to_string(), key));
                reads.extend(guards);
                smallvec![Effect::Storage(StorageEffect::BatchRead {
                    reads,
                    txn_id: Some(txn_id),
                })]
            }
            (StartMoveState::Read, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                self.write(values)
            }
            (StartMoveState::Write, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                let Some(txn_id) = self.txn_id else {
                    return self.fail(StartMoveError::NotFinished);
                };
                self.state = StartMoveState::Commit;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            (StartMoveState::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.txn_id = None;
                self.state = StartMoveState::Finish;
                smallvec![Effect::Task(TaskEffect::ShortenTimer {
                    key: TaskKey::MigrateCompression,
                    after: Duration::ZERO,
                })]
            }
            (_, event) => self.fail(StartMoveError::Unexpected(Box::new(event))),
        }
    }

    fn is_complete(&self) -> bool {
        self.state == StartMoveState::Finish
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.state {
            StartMoveState::Finish => self.output.unwrap_or(Err(StartMoveError::NotFinished)),
            _ => Err(StartMoveError::NotFinished),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn asker() -> AuthContext {
        AuthContext {
            user_id: Default::default(),
            realm_id: aruna_core::structs::identity::realm::RealmId::from_bytes([1; 32]),
            path_restrictions: None,
            session: None,
        }
    }

    fn grant() -> TokenGrant {
        TokenGrant {
            token_hash: "hash".to_string(),
            issuer: asker().realm_id,
            issued_at_secs: 1,
            expires_at_secs: 2,
            via: None,
        }
    }

    fn node() -> NodeId {
        iroh::SecretKey::from_bytes(&[3; 32]).public()
    }

    fn running(prefix: &str) -> CompressionMigration {
        let mut record = CompressionMigration::new(Compression::Off, 1);
        (record.moving, record.prefix) = (true, prefix.to_string());
        record
    }

    #[test]
    fn resumes_same_move() {
        let mut stored = running("in/");
        stored.migrated = 4;
        let record = move_record(Some(stored.clone()), Compression::Off, "in/", 9);
        assert_eq!(record, Ok(stored));
    }

    #[test]
    fn refuses_other_prefix() {
        // An unfinished move keeps its scope, also while it waits for a retry pass.
        let mut waiting = running("in/a/");
        (waiting.failed, waiting.retry_at_ms) = (2, Some(50));
        let refused = move_record(Some(waiting.clone()), Compression::Off, "in/b/", 9);
        assert_eq!(
            refused,
            Err(StartMoveError::MoveRunning("in/a/".to_string()))
        );
        let restarted = move_record(Some(waiting), Compression::Off, "in/a/", 9).unwrap();
        assert_eq!(
            (restarted.prefix.as_str(), restarted.started_at_ms),
            ("in/a/", 9)
        );
        let compression = Some(CompressionMigration::new(Compression::Off, 1));
        let fresh = move_record(compression, Compression::Off, "x/", 9).unwrap();
        assert!(fresh.moving && fresh.prefix == "x/");
    }

    #[test]
    fn records_asked_rename() {
        // Only a move started to rename writes the request, bound to its record.
        let group_id = ulid::Ulid::from_parts(1, 1);
        let info = BucketInfo {
            group_id,
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
        };
        for renaming in [false, true] {
            let mut operation = StartMoveOperation::new("b".into(), group_id, "in/".into(), 7);
            if renaming {
                operation = operation.renaming((asker(), node()), grant(), 3);
            }
            let info = Some(info.to_bytes().unwrap().into());
            let values = vec![(Key::from(vec![]), info), (vec![].into(), None)];
            let values = [values, vec![(vec![].into(), None), (vec![].into(), None)]].concat();
            let writes = operation.rows(values, None).unwrap();
            let asked = writes
                .iter()
                .find(|(space, ..)| space == RENAME_MOVE_KEYSPACE);
            // The request names the caller, whose access the move checks for each file.
            let asked = asked.map(|(_, _, value)| RenameRequest::from_bytes(value).unwrap());
            let expected = RenameRequest {
                started_at_ms: 7,
                auth: asker(),
                node_id: node(),
                grant: grant(),
                paused: false,
                aborted_at_ms: None,
            };
            assert_eq!(asked, renaming.then_some(expected));
            // The planned total belongs to the same move.
            let total = writes
                .iter()
                .find(|(space, ..)| space == RENAME_TOTAL_KEYSPACE)
                .map(|(_, _, value)| RenameTotal::from_bytes(value).unwrap());
            let planned = RenameTotal {
                started_at_ms: 7,
                total: 3,
            };
            assert_eq!(total, renaming.then_some(planned));
        }
        // A start repeated for the running move keeps its first total; an older one's goes.
        let mut running = CompressionMigration::new(Compression::Off, 5);
        (running.moving, running.prefix) = (true, "in/".to_string());
        for (started_at_ms, kept) in [(5, 10), (4, 3)] {
            let operation = StartMoveOperation::new("b".into(), group_id, "in/".into(), 7);
            let mut operation = operation.renaming((asker(), node()), grant(), 3);
            let info = Some(info.to_bytes().unwrap().into());
            let stored = Some(running.to_bytes().unwrap().into());
            let values = vec![(vec![].into(), info), (vec![].into(), None)];
            let values = [values, vec![(vec![].into(), None), (vec![].into(), stored)]].concat();
            let total = RenameTotal {
                started_at_ms,
                total: 10,
            };
            let writes = operation.rows(values, Some(total)).unwrap();
            let total = writes
                .iter()
                .find(|(space, ..)| space == RENAME_TOTAL_KEYSPACE)
                .map(|(_, _, value)| RenameTotal::from_bytes(value).unwrap().total);
            assert_eq!(total, Some(kept));
        }
    }

    #[test]
    fn refuses_changed_rule() {
        // The rename starts only while the rule reads as it did when its files were checked.
        let group_id = ulid::Ulid::from_parts(1, 1);
        let guard = (
            "rules".to_string(),
            Key::from(vec![1]),
            Some(Value::from(vec![2])),
        );
        let mut operation = StartMoveOperation::new("b".into(), group_id, "in/".into(), 7)
            .renaming((asker(), node()), grant(), 0)
            .with_guards(vec![guard]);
        operation.start();
        let effects = operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TxnId::from(1),
        }));
        let [Effect::Storage(StorageEffect::BatchRead { reads, .. })] = effects.as_slice() else {
            panic!("expected the reads, got {effects:?}")
        };
        assert_eq!(reads.last().map(|(space, _)| space.as_str()), Some("rules"));
        let mut values: Vec<(Key, Option<Value>)> = vec![(vec![].into(), None); 6];
        values.push((vec![1].into(), Some(vec![3].into())));
        operation.step(Event::Storage(StorageEvent::BatchReadResult { values }));
        assert!(matches!(
            operation.output,
            Some(Err(StartMoveError::RuleChanged))
        ));
    }

    #[test]
    fn refuses_encrypted_bucket() {
        use aruna_core::structs::storage::encryption::EncryptionMode;
        let settings = BucketEncryption {
            mode: EncryptionMode::NodeManaged,
            bucket_id: Some(ulid::Ulid::from_bytes([6; 16])),
            key_generation: 2,
            ..BucketEncryption::default()
        };
        let row = settings.to_bytes().unwrap();
        assert_eq!(
            check_movable(Some(&row), None, None),
            Err(StartMoveError::Encrypted)
        );
        assert_eq!(check_movable(None, None, None), Ok(None));
    }
}
