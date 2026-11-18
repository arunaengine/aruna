//! Removes a backend import rule and its object rows; versions and bytes stay. A mark that
//! names a provider bucket also deletes that bucket while it is empty.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::ImportRuleError;
use crate::groups::backends::{backend_key, record_writes};
use crate::groups::fence::group_fence_key;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::events::{BlobEvent, Event};
use aruna_core::keyspaces::{
    BACKEND_IMPORT_KEYSPACE, BLOB_CLEANUP_KEYSPACE, BLOB_VERSIONS_KEYSPACE,
    IMPORT_DISCOVERY_KEYSPACE, IMPORT_INTENT_KEYSPACE, IMPORT_PATTERN_KEYSPACE,
    IMPORT_REMOVAL_KEYSPACE, IMPORTED_OBJECT_KEYSPACE, RENAMED_ROW_KEYSPACE, S3_BUCKET_KEYSPACE,
    STORAGE_BACKEND_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{read_row, values};
use aruna_core::structs::identity::group_delete::check_group_write;
use aruna_core::structs::storage::backend_import::{
    BackendImport, ProviderCleanup, RemovalMark, WriteIntent, import_key, object_prefix,
};
use aruna_core::structs::storage::blob::{BackendRef, BlobCleanupWork, VersionKey};
use aruna_core::structs::storage::group_backend::{BUCKET_DELETING, BUCKET_STATE, GroupStorage};
use aruna_core::types::{GroupId, Key, Value};
use ulid::Ulid;

const DELETE_PAGE: usize = 512;

/// Why the 1:1 link of a bucket whose storage has no shared bucket stays.
pub const ONLY_STORAGE: &str =
    "the bucket has no other storage, as its storage has no shared bucket; delete the bucket";

/// Why a link of a bucket that still holds files stays.
pub const LINKED_FILES: &str =
    "the bucket still holds files; delete or empty the bucket to end the link";

fn mark_key(import_id: Ulid) -> Key {
    import_id.to_bytes().to_vec().into()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DropStep {
    Rule,
    Storage,
    Files,
    Delete,
    Mark,
}

type DropTurn = Result<Next<String>, ImportRuleError>;

/// Deletes the rule and stores its removal mark in one transaction. Outputs the rule's bucket.
#[derive(Debug, PartialEq)]
struct DropRule {
    group_id: GroupId,
    import_id: Ulid,
    step: DropStep,
    bucket: String,
}

impl DropRule {
    fn new(group_id: GroupId, import_id: Ulid) -> Self {
        Self {
            group_id,
            import_id,
            step: DropStep::Rule,
            bucket: String::new(),
        }
    }

    fn call(&mut self, step: DropStep, call: IoCall) -> DropTurn {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn rule_read(&mut self, [rule, fence]: [Option<Value>; 2]) -> DropTurn {
        check_group_write(fence.as_deref())?;
        let rule = rule.ok_or(ImportRuleError::NotFound)?;
        let rule = BackendImport::from_bytes(&rule)?;
        self.bucket = rule.bucket.clone();
        if rule.mapped_bucket().is_none() {
            return self.check_files();
        }
        // A 1:1 link may be the only storage its bucket has.
        let reads = vec![
            (
                STORAGE_BACKEND_KEYSPACE.to_string(),
                backend_key(rule.backend_id),
            ),
            (
                S3_BUCKET_KEYSPACE.to_string(),
                rule.bucket.as_bytes().to_vec().into(),
            ),
        ];
        self.call(DropStep::Storage, IoCall::Read(reads))
    }

    fn storage_read(&mut self, [storage, bucket]: [Option<Value>; 2]) -> DropTurn {
        let storage = storage
            .as_deref()
            .map(GroupStorage::from_bytes)
            .transpose()?;
        let shared =
            storage.is_none_or(|storage| !storage.splits() || storage.default_bucket().is_some());
        if bucket.is_some() && !shared {
            return Err(ImportRuleError::Conflict(ONLY_STORAGE.to_string()));
        }
        self.check_files()
    }

    /// A link ends with its bucket: one version row is enough to refuse.
    fn check_files(&mut self) -> DropTurn {
        let page = IoCall::Page {
            space: BLOB_VERSIONS_KEYSPACE.to_string(),
            prefix: VersionKey::bucket_prefix(&self.bucket)?.into(),
            after: None,
            limit: 1,
        };
        self.call(DropStep::Files, page)
    }

    fn files_read(&mut self, rows: Vec<(Key, Value)>) -> DropTurn {
        if !rows.is_empty() {
            return Err(ImportRuleError::Conflict(LINKED_FILES.to_string()));
        }
        let key = import_key(self.group_id, self.import_id);
        // The discovery rules go with the rule.
        let deletes = [BACKEND_IMPORT_KEYSPACE, IMPORT_DISCOVERY_KEYSPACE]
            .map(|key_space| (key_space.to_string(), key.clone()))
            .to_vec();
        self.call(DropStep::Delete, IoCall::Delete(deletes))
    }

    fn write_mark(&mut self) -> DropTurn {
        let mark = RemovalMark {
            bucket: self.bucket.clone(),
            cleanup: None,
        };
        let row = (
            IMPORT_REMOVAL_KEYSPACE.to_string(),
            mark_key(self.import_id),
            mark.to_bytes()?.into(),
        );
        self.call(DropStep::Mark, IoCall::Write(vec![row]))
    }
}

impl Operation for DropRule {
    type Output = String;
    type Error = ImportRuleError;

    fn first(&mut self) -> DropTurn {
        let key = import_key(self.group_id, self.import_id);
        let reads = vec![
            (BACKEND_IMPORT_KEYSPACE.to_string(), key),
            group_fence_key(self.group_id),
        ];
        self.call(DropStep::Rule, IoCall::Read(reads))
    }

    fn next(&mut self, result: IoResult) -> DropTurn {
        match self.step {
            DropStep::Rule => self.rule_read(values::<2>(result)?),
            DropStep::Storage => self.storage_read(values::<2>(result)?),
            DropStep::Files => self.files_read(result.page()?.0),
            DropStep::Delete => {
                result.done()?;
                self.write_mark()
            }
            DropStep::Mark => {
                result.done()?;
                Ok(Next::Done(std::mem::take(&mut self.bucket)))
            }
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SettleStep {
    Record,
    Write,
    Unmark,
}

type SettleTurn = Result<Next<()>, ImportRuleError>;

/// Settles the storage record after its provider bucket was handled and drops the mark, in one
/// transaction. A deleted bucket leaves the record; one that still holds objects is `created`
/// again.
#[derive(Debug, PartialEq)]
struct SettleProvider {
    import_id: Ulid,
    cleanup: ProviderCleanup,
    deleted: bool,
    step: SettleStep,
}

impl SettleProvider {
    fn call(&mut self, step: SettleStep, call: IoCall) -> SettleTurn {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn delete_mark(&mut self) -> SettleTurn {
        let mark = (
            IMPORT_REMOVAL_KEYSPACE.to_string(),
            mark_key(self.import_id),
        );
        self.call(SettleStep::Unmark, IoCall::Delete(vec![mark]))
    }

    fn record_read(&mut self, value: Option<Value>) -> SettleTurn {
        let record = value
            .map(|value| GroupStorage::from_bytes(&value))
            .transpose()?;
        let bucket = &self.cleanup.bucket;
        let Some(mut record) =
            record.filter(|record| record.bucket_state(bucket) == Some(BUCKET_DELETING))
        else {
            return self.delete_mark();
        };
        let state = format!("{BUCKET_STATE}{bucket}");
        match self.deleted {
            true => record.public_config.remove(&state),
            false => record.public_config.insert(state, "created".to_string()),
        };
        let writes = record_writes(&record)?;
        self.call(SettleStep::Write, IoCall::Write(writes))
    }
}

impl Operation for SettleProvider {
    type Output = ();
    type Error = ImportRuleError;

    fn first(&mut self) -> SettleTurn {
        let key = backend_key(self.cleanup.backend_id);
        self.call(SettleStep::Record, read_row(STORAGE_BACKEND_KEYSPACE, key))
    }

    fn next(&mut self, result: IoResult) -> SettleTurn {
        match self.step {
            SettleStep::Record => self.record_read(result.value()?),
            SettleStep::Write => {
                result.done()?;
                self.delete_mark()
            }
            SettleStep::Unmark => {
                result.done()?;
                Ok(Next::Done(()))
            }
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RemoveState {
    DropRule,
    ReadMark,
    Scan,
    Purge,
    QueueMoves,
    QueueClaims,
    Unmark,
    DropProvider,
    Settle,
}

type RemoveTurn = Result<Next<()>, ImportRuleError>;

/// The rule goes in one transaction with a removal mark; its object rows follow in pages and
/// the mark goes last, so an interrupted removal resumes from the mark. Imported versions
/// remain readable, and the routing rule for the folder stays in place.
#[derive(Debug, PartialEq)]
pub struct RemoveImportOperation {
    /// `None` when resuming the rows of a rule that is already gone.
    group_id: Option<GroupId>,
    import_id: Ulid,
    state: RemoveState,
    /// The provider bucket the mark deletes.
    cleanup: Option<ProviderCleanup>,
    /// The rule's bucket, which the settlement of its unfinished moves names.
    bucket: Option<String>,
    /// The rows go first, then the write intents.
    space: &'static str,
    /// The intent rows that go once their settlements are queued.
    pending: Vec<(String, Key)>,
    claims_queued: bool,
}

impl RemoveImportOperation {
    pub fn new(group_id: GroupId, import_id: Ulid) -> Self {
        Self::with_group(Some(group_id), import_id)
    }

    /// Deletes the remaining object rows of a removal that was interrupted.
    pub fn resume(import_id: Ulid) -> Self {
        Self::with_group(None, import_id)
    }

    fn with_group(group_id: Option<GroupId>, import_id: Ulid) -> Self {
        Self {
            group_id,
            import_id,
            state: RemoveState::ReadMark,
            cleanup: None,
            bucket: None,
            space: IMPORTED_OBJECT_KEYSPACE,
            pending: Vec::new(),
            claims_queued: false,
        }
    }

    fn call(&mut self, state: RemoveState, call: IoCall) -> RemoveTurn {
        self.state = state;
        Ok(Next::Call(call))
    }

    fn scan(&mut self) -> RemoveTurn {
        let page = IoCall::Page {
            space: self.space.to_string(),
            prefix: object_prefix(self.import_id),
            after: None,
            limit: DELETE_PAGE,
        };
        self.call(RemoveState::Scan, page)
    }

    fn mark_read(&mut self, value: Option<Value>) -> RemoveTurn {
        if let Some(mark) = value.as_deref().map(RemovalMark::from_bytes).transpose()? {
            self.cleanup = mark.cleanup;
            self.bucket = Some(mark.bucket);
        }
        self.scan()
    }

    /// The rule's delete claims and moved-row records go with it, through the cleanup queue.
    fn queue_claims(&mut self) -> RemoveTurn {
        let id = self.import_id.to_bytes().to_vec();
        let works = [
            BlobCleanupWork::DropClaims {
                import_id: self.import_id,
            },
            BlobCleanupWork::DropRows {
                key_space: RENAMED_ROW_KEYSPACE.to_string(),
                prefix: id.clone(),
            },
        ];
        let mut writes = Vec::new();
        for (tag, work) in works.into_iter().enumerate() {
            let key = [&id[..], &[tag as u8]].concat().into();
            writes.push((
                BLOB_CLEANUP_KEYSPACE.to_string(),
                key,
                work.to_bytes()?.into(),
            ));
        }
        self.call(RemoveState::QueueClaims, IoCall::Write(writes))
    }

    /// All rows are gone: the provider bucket is next, or the mark when there is none.
    fn rows_gone(&mut self) -> RemoveTurn {
        if let Some(cleanup) = self.cleanup.as_ref() {
            let drop = Effect::Blob(BlobEffect::DeleteEmptyBucket {
                backend: BackendRef::Group(cleanup.backend_id),
                bucket: cleanup.bucket.clone(),
            });
            return self.call(RemoveState::DropProvider, drop.into());
        }
        let mark = (
            IMPORT_REMOVAL_KEYSPACE.to_string(),
            mark_key(self.import_id),
        );
        self.call(RemoveState::Unmark, IoCall::Delete(vec![mark]))
    }

    fn purge(&mut self, rows: Vec<(Key, Value)>) -> RemoveTurn {
        if rows.is_empty() {
            return match self.space {
                IMPORTED_OBJECT_KEYSPACE => {
                    self.space = IMPORT_INTENT_KEYSPACE;
                    self.scan()
                }
                IMPORT_INTENT_KEYSPACE => {
                    self.space = IMPORT_PATTERN_KEYSPACE;
                    self.scan()
                }
                _ if !self.claims_queued => self.queue_claims(),
                _ => self.rows_gone(),
            };
        }
        // A move's own intents are settled through the cleanup queue, which keeps each one.
        let settle = match self.space == IMPORT_INTENT_KEYSPACE {
            true => self.settlements(&rows)?,
            false => Vec::new(),
        };
        let deletes: Vec<_> = rows
            .into_iter()
            .map(|(key, _)| (self.space.to_string(), key))
            .collect();
        if settle.is_empty() {
            return self.purge_rows(deletes);
        }
        self.pending = deletes;
        self.call(RemoveState::QueueMoves, IoCall::Write(settle))
    }

    fn purge_rows(&mut self, deletes: Vec<(String, Key)>) -> RemoveTurn {
        self.call(RemoveState::Purge, IoCall::Delete(deletes))
    }

    /// One `SettleMove` work per intent a move owns, keyed by the intent like a hand-over.
    fn settlements(
        &self,
        rows: &[(Key, Value)],
    ) -> Result<Vec<(String, Key, Value)>, ImportRuleError> {
        let mut writes = Vec::new();
        for (key, value) in rows {
            let intent = WriteIntent::from_bytes(value)?;
            let Some(bucket) = self.bucket.as_deref().filter(|_| intent.mover.is_some()) else {
                continue;
            };
            let work = BlobCleanupWork::SettleMove {
                version: VersionKey::new(bucket, &intent.key, intent.version_id),
                intents: vec![(key.to_vec(), intent)],
            };
            writes.push((
                BLOB_CLEANUP_KEYSPACE.to_string(),
                key.clone(),
                work.to_bytes()?.into(),
            ));
        }
        Ok(writes)
    }

    fn provider_dropped(&mut self, event: Event) -> RemoveTurn {
        let deleted = match event {
            Event::Blob(BlobEvent::BucketDeleted { deleted }) => deleted,
            // The mark stays, so the schedule tries the provider bucket again.
            Event::Blob(BlobEvent::Error(error)) => {
                return Err(ImportRuleError::Read(error.reason()));
            }
            _ => return Err(ImportRuleError::Unexpected("DropProvider")),
        };
        let cleanup = self.cleanup.clone();
        let cleanup = cleanup.ok_or(ImportRuleError::Unexpected("DropProvider"))?;
        let settle = SettleProvider {
            import_id: self.import_id,
            cleanup,
            deleted,
            step: SettleStep::Record,
        };
        self.call(RemoveState::Settle, IoCall::child(settle))
    }
}

impl Operation for RemoveImportOperation {
    type Output = ();
    type Error = ImportRuleError;

    fn first(&mut self) -> RemoveTurn {
        match self.group_id {
            Some(group_id) => {
                let drop = DropRule::new(group_id, self.import_id);
                self.call(RemoveState::DropRule, IoCall::child(drop))
            }
            None => {
                let mark = read_row(IMPORT_REMOVAL_KEYSPACE, mark_key(self.import_id));
                self.call(RemoveState::ReadMark, mark)
            }
        }
    }

    fn next(&mut self, result: IoResult) -> RemoveTurn {
        match self.state {
            RemoveState::DropRule => {
                self.bucket = Some(result.child::<String, ImportRuleError>()??);
                self.scan()
            }
            RemoveState::ReadMark => self.mark_read(result.value()?),
            RemoveState::Scan => self.purge(result.page()?.0),
            RemoveState::Purge => {
                result.done()?;
                self.scan()
            }
            RemoveState::QueueMoves => {
                result.done()?;
                let deletes = std::mem::take(&mut self.pending);
                self.purge_rows(deletes)
            }
            RemoveState::QueueClaims => {
                result.done()?;
                self.claims_queued = true;
                self.rows_gone()
            }
            RemoveState::DropProvider => self.provider_dropped(result.event()?),
            RemoveState::Settle => {
                result.child::<(), ImportRuleError>()??;
                Ok(Next::Done(()))
            }
            RemoveState::Unmark => {
                result.done()?;
                Ok(Next::Done(()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::bucket::rename::tests::rule;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::StorageEvent;

    fn page(rows: usize) -> IoResult {
        IoResult::Page {
            rows: (0..rows)
                .map(|row| (Key::from(vec![row as u8]), Value::from(Vec::new())))
                .collect(),
            next: None,
        }
    }

    fn child<T, E>(result: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(ChildOutcome::new(result))
    }

    /// Answers scans until no space is left, checks the queued claims and returns the next turn.
    fn drain(operation: &mut RemoveImportOperation) -> RemoveTurn {
        loop {
            match operation.next(page(0)) {
                Ok(Next::Call(IoCall::Page { space, .. })) => {
                    assert!(
                        [IMPORT_INTENT_KEYSPACE, IMPORT_PATTERN_KEYSPACE].contains(&space.as_str())
                    );
                }
                Ok(Next::Call(IoCall::Write(writes))) => {
                    let works: Vec<_> = (writes.iter())
                        .map(|(_, _, value)| BlobCleanupWork::from_bytes(value).unwrap())
                        .collect();
                    assert!(matches!(
                        works.as_slice(),
                        [
                            BlobCleanupWork::DropClaims { .. },
                            BlobCleanupWork::DropRows { .. }
                        ]
                    ));
                    return operation.next(IoResult::Done);
                }
                other => return other,
            }
        }
    }

    fn unmarked(operation: &mut RemoveImportOperation, turn: RemoveTurn) {
        let Ok(Next::Call(IoCall::Delete(deletes))) = turn else {
            panic!("expected the mark delete, got {turn:?}")
        };
        assert_eq!(deletes[0].0, IMPORT_REMOVAL_KEYSPACE);
        assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(())));
    }

    fn dropping() -> DropRule {
        let mut drop = DropRule::new(Ulid::from_parts(1, 1), Ulid::from_parts(2, 2));
        assert_eq!(drop.transaction(), TransactionMode::Write);
        assert!(matches!(drop.first(), Ok(Next::Call(IoCall::Read(_)))));
        drop
    }

    fn rule_rows(rule: &BackendImport) -> IoResult {
        IoResult::Values(vec![Some(rule.to_bytes().unwrap().into()), None])
    }

    #[test]
    fn missing_rule_refused() {
        let turn = dropping().next(IoResult::Values(vec![None, None]));
        assert_eq!(turn, Err(ImportRuleError::NotFound));
    }

    #[test]
    fn keeps_filled_link() {
        let mut drop = dropping();
        let turn = drop.next(rule_rows(&rule()));
        assert!(matches!(
            turn,
            Ok(Next::Call(IoCall::Page { limit: 1, .. }))
        ));
        assert_eq!(
            drop.next(page(1)),
            Err(ImportRuleError::Conflict(LINKED_FILES.to_string()))
        );
    }

    #[test]
    fn keeps_only_storage() {
        // A 1:1 link on a storage without a shared bucket is all the bucket has.
        let mut rule = rule();
        rule.backend_prefix = "lab/".to_string();
        rule.write_pattern =
            aruna_core::structs::storage::backend_import::PLAIN_PATTERN.to_string();
        assert!(rule.mapped_bucket().is_some());
        let record = GroupStorage {
            backend_id: rule.backend_id,
            group_id: rule.group_id,
            name: "lab".to_string(),
            kind: aruna_core::structs::storage::group_backend::GroupBackendKind::S3,
            public_config: Default::default(),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            updated_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            disabled: false,
            cleanup: Default::default(),
        };
        let mut operation = RemoveImportOperation::new(rule.group_id, rule.import_id);
        operation.start();
        operation.step(storage(StorageEvent::TransactionStarted {
            txn_id: TxnId::from(7),
        }));
        operation.step(storage(StorageEvent::BatchReadResult {
            values: vec![
                (Key::from(Vec::new()), Some(rule.to_bytes().unwrap().into())),
                (Key::from(Vec::new()), None),
            ],
        }));
        operation.step(storage(StorageEvent::BatchReadResult {
            values: vec![
                (
                    Key::from(Vec::new()),
                    Some(record.to_bytes().unwrap().into()),
                ),
                (Key::from(Vec::new()), Some(Value::from(vec![1]))),
            ],
        }));
        assert_eq!(
            operation.finalize(),
            Err(ImportRuleError::Conflict(ONLY_STORAGE.to_string()))
        );
    }

    #[test]
    fn marks_removed_rule() {
        let (group_id, import_id) = (Ulid::from_parts(1, 1), Ulid::from_parts(2, 2));
        let mut operation = RemoveImportOperation::new(group_id, import_id);
        let rule = crate::s3::bucket::rename::tests::rule();
        operation.start();
        let txn_id = TxnId::from(7);
        operation.step(storage(StorageEvent::TransactionStarted { txn_id }));
        let effects = operation.step(storage(StorageEvent::BatchReadResult {
            values: vec![
                (Key::from(Vec::new()), Some(rule.to_bytes().unwrap().into())),
                (Key::from(Vec::new()), None),
            ],
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Iter { key_space, limit: 1, .. })]
                if key_space == BLOB_VERSIONS_KEYSPACE
        ));
        let effects = operation.step(storage(StorageEvent::IterResult {
            values: Vec::new(),
            next_start_after: None,
        }));
        let [
            Effect::Storage(StorageEffect::BatchDelete {
                deletes,
                txn_id: Some(_),
            }),
        ] = effects.as_slice()
        else {
            panic!("expected the rule delete, got {effects:?}")
        };
        let spaces: Vec<_> = deletes.iter().map(|(space, _)| space.as_str()).collect();
        assert_eq!(spaces, [BACKEND_IMPORT_KEYSPACE, IMPORT_DISCOVERY_KEYSPACE]);

        // The mark commits with the rule deletion, before any row goes.
        let effects = operation.step(storage(StorageEvent::BatchDeleteResult {
            entries: Vec::new(),
        }));
        let [
            Effect::Storage(StorageEffect::Write {
                key_space,
                key,
                txn_id: mark_txn,
                ..
            }),
        ] = effects.as_slice()
        else {
            panic!("expected the removal mark, got {effects:?}")
        };
        assert_eq!(key_space, IMPORT_REMOVAL_KEYSPACE);
        assert_eq!(key.as_ref(), import_id.to_bytes().as_slice());
        assert_eq!(*mark_txn, Some(txn_id));
        let effects = operation.step(storage(StorageEvent::WriteResult {
            key: Vec::new().into(),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::CommitTransaction { .. })]
        ));
        operation.step(storage(StorageEvent::TransactionCommitted { txn_id }));

        drain(&mut operation, 2);
        operation.step(storage(StorageEvent::BatchDeleteResult {
            entries: Vec::new(),
        }));
        let effects = drain(&mut operation, 0);
        unmarked(&mut operation, effects);
        assert_eq!(operation.finalize(), Ok(()));
    }

    /// Answers the read of the removal mark with `value`.
    fn mark(operation: &mut RemoveImportOperation, value: Option<Vec<u8>>) -> Effects {
        let effects = operation.start();
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { key_space, .. })]
                if key_space == IMPORT_REMOVAL_KEYSPACE
        ));
        operation.step(storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: value.map(Into::into),
        }))
    }

    fn marked(cleanup: Option<ProviderCleanup>) -> Vec<u8> {
        let bucket = "bucket".to_string();
        RemovalMark { bucket, cleanup }.to_bytes().unwrap()
    }

    fn cleanup() -> ProviderCleanup {
        ProviderCleanup {
            backend_id: Ulid::from_parts(4, 4),
            bucket: "team-a-x".to_string(),
        }
    }

    fn deleting() -> GroupStorage {
        use aruna_core::structs::storage::cleanup::CleanupStrategy;
        use aruna_core::structs::storage::group_backend::GroupBackendKind;
        GroupStorage {
            backend_id: cleanup().backend_id,
            group_id: Ulid::from_parts(1, 1),
            name: "tenant".to_string(),
            kind: GroupBackendKind::S3,
            public_config: [("bucket:team-a-x", "deleting"), ("bucket:other", "found")]
                .into_iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            updated_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            disabled: false,
            cleanup: CleanupStrategy::Retain,
        }
    }

    /// Runs a provider mark through its rows and the bucket delete answered with `answer`;
    /// answers the record written in the closing transaction.
    fn settled(answer: BlobEvent) -> GroupStorage {
        let mut operation = RemoveImportOperation::resume(Ulid::from_parts(2, 2));
        mark(&mut operation, Some(marked(Some(cleanup()))));
        let effects = drain(&mut operation, 0);
        assert_eq!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::DeleteEmptyBucket {
                backend: BackendRef::Group(cleanup().backend_id),
                bucket: "team-a-x".to_string(),
            })]
        );
        operation.step(Event::Blob(answer));
        let txn_id = TxnId::from(7);
        operation.step(storage(StorageEvent::TransactionStarted { txn_id }));
        let effects = operation.step(storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: Some(deleting().to_bytes().unwrap().into()),
        }));
        let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
            panic!("expected the record, got {effects:?}")
        };
        let record = GroupStorage::from_bytes(&writes[0].2).unwrap();
        // The mark goes in the same transaction as the record.
        let effects = operation.step(storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Delete { key_space, txn_id: Some(_), .. })]
                if key_space == IMPORT_REMOVAL_KEYSPACE
        ));
        operation.step(storage(StorageEvent::DeleteResult {
            key: Vec::new().into(),
        }));
        operation.step(storage(StorageEvent::TransactionCommitted { txn_id }));
        assert_eq!(operation.finalize(), Ok(()));
        record
    }

    #[test]
    fn settles_provider_bucket() {
        let deleted = settled(BlobEvent::BucketDeleted { deleted: true });
        assert_eq!(deleted.bucket_state("team-a-x"), None);
        assert_eq!(deleted.bucket_state("other"), Some("found"));
        // A bucket with objects stays, and Aruna still knows it created it.
        let kept = settled(BlobEvent::BucketDeleted { deleted: false });
        assert_eq!(kept.bucket_state("team-a-x"), Some("created"));
    }

    #[test]
    fn failed_cleanup_retries() {
        let mut operation = RemoveImportOperation::resume(Ulid::from_parts(2, 2));
        mark(&mut operation, Some(marked(Some(cleanup()))));
        drain(&mut operation, 0);
        let failure = aruna_core::errors::BlobError::WriteError("unreachable".to_string());
        let effects = operation.step(Event::Blob(BlobEvent::Error(failure)));
        // Nothing is written, so the mark and the `deleting` fence stay for the next run.
        assert!(effects.is_empty());
        assert!(operation.finalize().is_err());
    }

    #[tokio::test]
    async fn keeps_mark_stored() {
        use crate::driver::{DriverContext, drive};
        use aruna_core::handle::Handle as _;
        let dir = tempfile::tempdir().unwrap();
        let storage_handle =
            aruna_storage::storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };
        let import_id = Ulid::from_parts(2, 2);
        let mut writes = record_writes(&deleting()).unwrap();
        let mark_key: Key = import_id.to_bytes().to_vec().into();
        let mark_value: Value = marked(Some(cleanup())).into();
        writes.push((
            IMPORT_REMOVAL_KEYSPACE.to_string(),
            mark_key.clone(),
            mark_value,
        ));
        let write = StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        };
        storage_handle.send_effect(Effect::Storage(write)).await;
        // Without a reachable storage the bucket stays; the next run finds mark and fence.
        let result = drive(RemoveImportOperation::resume(import_id), &context).await;
        assert!(result.is_err());
        let read = |key_space: &str, key: Key| {
            let effect = StorageEffect::Read {
                key_space: key_space.to_string(),
                key,
                txn_id: None,
            };
            storage_handle.send_effect(Effect::Storage(effect))
        };
        let Event::Storage(StorageEvent::ReadResult { value, .. }) =
            read(IMPORT_REMOVAL_KEYSPACE, mark_key).await
        else {
            panic!("mark read failed")
        };
        let cleanup = RemovalMark::from_bytes(&value.unwrap()).unwrap().cleanup;
        assert_eq!(
            cleanup.map(|cleanup| cleanup.bucket).as_deref(),
            Some("team-a-x")
        );
        let key = backend_key(deleting().backend_id);
        let Event::Storage(StorageEvent::ReadResult { value, .. }) =
            read(STORAGE_BACKEND_KEYSPACE, key).await
        else {
            panic!("record read failed")
        };
        let record = GroupStorage::from_bytes(&value.unwrap()).unwrap();
        assert_eq!(record.bucket_state("team-a-x"), Some(BUCKET_DELETING));
    }

    #[tokio::test]
    async fn settles_move_intents() {
        // A removed rule's move intents are queued for settlement, with their data, before
        // they go; an upload's intent just goes.
        use crate::driver::{DriverContext, drive};
        use aruna_core::handle::Handle as _;
        use aruna_core::structs::storage::backend_import::intent_key;
        let dir = tempfile::tempdir().unwrap();
        let storage_handle =
            aruna_storage::storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };
        let import_id = Ulid::from_parts(2, 2);
        let intent = |mover| WriteIntent {
            key: "a.csv".to_string(),
            version_id: Ulid::from_parts(3, 3),
            backend: BackendRef::node_default(),
            created_by: Default::default(),
            written: None,
            mover,
        };
        let (moved, uploaded) = (intent(Some(7)), intent(None));
        let moved_key = intent_key(import_id, "raw/a.csv~X", moved.version_id);
        let uploaded_key = intent_key(import_id, "raw/a.csv", uploaded.version_id);
        let writes = vec![
            (
                IMPORT_REMOVAL_KEYSPACE.to_string(),
                import_id.to_bytes().to_vec().into(),
                marked(None).into(),
            ),
            (
                IMPORT_INTENT_KEYSPACE.to_string(),
                moved_key.clone(),
                moved.to_bytes().unwrap().into(),
            ),
            (
                IMPORT_INTENT_KEYSPACE.to_string(),
                uploaded_key.clone(),
                uploaded.to_bytes().unwrap().into(),
            ),
        ];
        let write = StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        };
        storage_handle.send_effect(Effect::Storage(write)).await;
        drive(RemoveImportOperation::resume(import_id), &context)
            .await
            .unwrap();
        let read = |key_space: &str, key: Key| {
            let effect = StorageEffect::Read {
                key_space: key_space.to_string(),
                key,
                txn_id: None,
            };
            storage_handle.send_effect(Effect::Storage(effect))
        };
        for key in [moved_key.clone(), uploaded_key.clone()] {
            let Event::Storage(StorageEvent::ReadResult { value, .. }) =
                read(IMPORT_INTENT_KEYSPACE, key).await
            else {
                panic!("intent read failed")
            };
            assert!(value.is_none());
        }
        let Event::Storage(StorageEvent::ReadResult { value, .. }) =
            read(BLOB_CLEANUP_KEYSPACE, moved_key.clone()).await
        else {
            panic!("work read failed")
        };
        let work = BlobCleanupWork::from_bytes(&value.unwrap()).unwrap();
        let expected = BlobCleanupWork::SettleMove {
            version: VersionKey::new("bucket", "a.csv", moved.version_id),
            intents: vec![(moved_key.to_vec(), moved)],
        };
        assert_eq!(work, expected);
        let Event::Storage(StorageEvent::ReadResult { value, .. }) =
            read(BLOB_CLEANUP_KEYSPACE, uploaded_key).await
        else {
            panic!("work read failed")
        };
        assert!(value.is_none());
    }

    #[test]
    fn resumes_row_removal() {
        let mut operation = RemoveImportOperation::resume(Ulid::from_parts(2, 2));
        let effects = mark(&mut operation, Some(marked(None)));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Iter { .. })]
        ));
        let effects = drain(&mut operation, 0);
        unmarked(&mut operation, effects);
        assert_eq!(operation.finalize(), Ok(()));

        // A resumed removal never touches a rule.
        let mut operation = RemoveImportOperation::resume(Ulid::from_parts(2, 2));
        mark(&mut operation, None);
        operation.step(storage(StorageEvent::TransactionStarted {
            txn_id: TxnId::from(7),
        }));
        assert_eq!(
            operation.finalize(),
            Err(ImportRuleError::Unexpected("RemoveImport"))
        );
    }
}
