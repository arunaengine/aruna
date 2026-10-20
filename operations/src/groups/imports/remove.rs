//! Removes a backend import rule and its object rows; versions and bytes stay. A mark that
//! names a provider bucket also deletes that bucket while it is empty.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::ImportRuleError;
use crate::groups::backends::{backend_key, record_writes};
use crate::groups::fence::{parse_group_record, read_group_record};
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{
    BACKEND_IMPORT_KEYSPACE, IMPORT_DISCOVERY_KEYSPACE, IMPORT_INTENT_KEYSPACE,
    IMPORT_PATTERN_KEYSPACE, IMPORT_REMOVAL_KEYSPACE, IMPORTED_OBJECT_KEYSPACE,
    STORAGE_BACKEND_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::backend_import::{ProviderCleanup, import_key, object_prefix};
use aruna_core::structs::storage::blob::BackendRef;
use aruna_core::structs::storage::group_backend::{BUCKET_DELETING, BUCKET_STATE, GroupStorage};
use aruna_core::types::{Effects, GroupId, Key, TxnId, Value};
use smallvec::smallvec;
use ulid::Ulid;

const DELETE_PAGE: usize = 512;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RemoveState {
    Init,
    Start,
    Read,
    Delete,
    Mark,
    Commit,
    ReadMark,
    Scan,
    Purge,
    Unmark,
    DropProvider,
    StartRecord,
    ReadRecord,
    WriteRecord,
    DeleteMark,
    CommitRecord,
    Finish,
}

/// The rule goes in one transaction with a removal mark; its object rows follow in pages and
/// the mark goes last, so an interrupted removal resumes from the mark. Imported versions
/// remain readable, and the routing rule for the folder stays in place.
#[derive(Debug, PartialEq)]
pub struct RemoveImportOperation {
    /// `None` when resuming the rows of a rule that is already gone.
    group_id: Option<GroupId>,
    import_id: Ulid,
    state: RemoveState,
    txn_id: Option<TxnId>,
    /// The provider bucket the mark deletes, and whether the storage deleted it.
    cleanup: Option<ProviderCleanup>,
    deleted: bool,
    /// The rows go first, then the write intents.
    space: &'static str,
    output: Option<Result<(), ImportRuleError>>,
}

impl RemoveImportOperation {
    pub fn new(group_id: GroupId, import_id: Ulid) -> Self {
        Self {
            group_id: Some(group_id),
            import_id,
            state: RemoveState::Init,
            txn_id: None,
            cleanup: None,
            deleted: false,
            space: IMPORTED_OBJECT_KEYSPACE,
            output: None,
        }
    }

    /// Deletes the remaining object rows of a removal that was interrupted.
    pub fn resume(import_id: Ulid) -> Self {
        Self {
            group_id: None,
            import_id,
            state: RemoveState::Init,
            txn_id: None,
            cleanup: None,
            deleted: false,
            space: IMPORTED_OBJECT_KEYSPACE,
            output: None,
        }
    }

    fn mark_key(&self) -> Key {
        self.import_id.to_bytes().to_vec().into()
    }

    fn fail(&mut self, error: ImportRuleError) -> Effects {
        self.state = RemoveState::Finish;
        self.output = Some(Err(error));
        self.abort()
    }

    fn scan(&mut self) -> Effects {
        self.state = RemoveState::Scan;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: self.space.to_string(),
            prefix: Some(object_prefix(self.import_id)),
            start: None,
            limit: DELETE_PAGE,
            txn_id: None,
        })]
    }

    fn purge(&mut self, values: Vec<(Key, Value)>) -> Effects {
        if values.is_empty() && self.space == IMPORTED_OBJECT_KEYSPACE {
            self.space = IMPORT_INTENT_KEYSPACE;
            return self.scan();
        }
        if values.is_empty() && self.space == IMPORT_INTENT_KEYSPACE {
            self.space = IMPORT_PATTERN_KEYSPACE;
            return self.scan();
        }
        if let (true, Some(cleanup)) = (values.is_empty(), self.cleanup.as_ref()) {
            self.state = RemoveState::DropProvider;
            return smallvec![Effect::Blob(BlobEffect::DeleteEmptyBucket {
                backend: BackendRef::Group(cleanup.backend_id),
                bucket: cleanup.bucket.clone(),
            })];
        }
        if values.is_empty() {
            self.state = RemoveState::Unmark;
            return smallvec![Effect::Storage(StorageEffect::Delete {
                key_space: IMPORT_REMOVAL_KEYSPACE.to_string(),
                key: self.mark_key(),
                txn_id: None,
            })];
        }
        self.state = RemoveState::Purge;
        let deletes = values
            .into_iter()
            .map(|(key, _)| (self.space.to_string(), key))
            .collect();
        smallvec![Effect::Storage(StorageEffect::BatchDelete {
            deletes,
            txn_id: None,
        })]
    }
}

impl RemoveImportOperation {
    /// A deleted bucket leaves the record; one that still holds objects is `created` again.
    fn settle(&mut self, value: Option<Value>) -> Effects {
        let Some(cleanup) = self.cleanup.as_ref() else {
            return self.fail(ImportRuleError::Unexpected("ReadRecord"));
        };
        let record = value
            .map(|value| GroupStorage::from_bytes(&value))
            .transpose();
        let mut record = match record {
            Ok(Some(record)) if record.bucket_state(&cleanup.bucket) == Some(BUCKET_DELETING) => {
                record
            }
            Ok(_) => return self.delete_mark(),
            Err(error) => return self.fail(error.into()),
        };
        let state = format!("{BUCKET_STATE}{}", cleanup.bucket);
        match self.deleted {
            true => record.public_config.remove(&state),
            false => record.public_config.insert(state, "created".to_string()),
        };
        let writes = match record_writes(&record) {
            Ok(writes) => writes,
            Err(error) => return self.fail(error.into()),
        };
        self.state = RemoveState::WriteRecord;
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: self.txn_id,
        })]
    }

    fn delete_mark(&mut self) -> Effects {
        self.state = RemoveState::DeleteMark;
        smallvec![Effect::Storage(StorageEffect::Delete {
            key_space: IMPORT_REMOVAL_KEYSPACE.to_string(),
            key: self.mark_key(),
            txn_id: self.txn_id,
        })]
    }
}

impl Operation for RemoveImportOperation {
    type Output = ();
    type Error = ImportRuleError;

    fn start(&mut self) -> Effects {
        if self.group_id.is_none() {
            self.state = RemoveState::ReadMark;
            return smallvec![Effect::Storage(StorageEffect::Read {
                key_space: IMPORT_REMOVAL_KEYSPACE.to_string(),
                key: self.mark_key(),
                txn_id: None,
            })];
        }
        self.state = RemoveState::Start;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (self.state, self.group_id, event) {
            (_, _, Event::Storage(StorageEvent::Error { error })) => self.fail(error.into()),
            (
                RemoveState::Start,
                Some(group_id),
                Event::Storage(StorageEvent::TransactionStarted { txn_id }),
            ) => {
                self.txn_id = Some(txn_id);
                self.state = RemoveState::Read;
                let key = import_key(group_id, self.import_id);
                smallvec![read_group_record(
                    group_id,
                    BACKEND_IMPORT_KEYSPACE,
                    key,
                    txn_id
                )]
            }
            (RemoveState::Read, Some(group_id), event) => match parse_group_record(event) {
                Ok(Some(_)) => {
                    self.state = RemoveState::Delete;
                    let key = import_key(group_id, self.import_id);
                    // The discovery rules go with the rule.
                    let deletes = [BACKEND_IMPORT_KEYSPACE, IMPORT_DISCOVERY_KEYSPACE]
                        .map(|key_space| (key_space.to_string(), key.clone()))
                        .to_vec();
                    smallvec![Effect::Storage(StorageEffect::BatchDelete {
                        deletes,
                        txn_id: self.txn_id,
                    })]
                }
                Ok(None) => self.fail(ImportRuleError::NotFound),
                Err(error) => self.fail(error.into()),
            },
            (RemoveState::Delete, _, Event::Storage(StorageEvent::BatchDeleteResult { .. })) => {
                self.state = RemoveState::Mark;
                smallvec![Effect::Storage(StorageEffect::Write {
                    key_space: IMPORT_REMOVAL_KEYSPACE.to_string(),
                    key: self.mark_key(),
                    value: Vec::new().into(),
                    txn_id: self.txn_id,
                })]
            }
            (RemoveState::Mark, _, Event::Storage(StorageEvent::WriteResult { .. })) => {
                let Some(txn_id) = self.txn_id else {
                    return self.fail(ImportRuleError::Unexpected("Mark"));
                };
                self.state = RemoveState::Commit;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            (RemoveState::Commit, _, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.txn_id = None;
                self.scan()
            }
            (RemoveState::Scan, _, Event::Storage(StorageEvent::IterResult { values, .. })) => {
                self.purge(values)
            }
            (RemoveState::Purge, _, Event::Storage(StorageEvent::BatchDeleteResult { .. })) => {
                self.scan()
            }
            (RemoveState::ReadMark, _, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                let mark = value.as_deref().map(ProviderCleanup::from_mark).transpose();
                match mark {
                    Ok(cleanup) => self.cleanup = cleanup.flatten(),
                    Err(error) => return self.fail(error.into()),
                }
                self.scan()
            }
            (RemoveState::DropProvider, _, Event::Blob(BlobEvent::BucketDeleted { deleted })) => {
                self.deleted = deleted;
                self.state = RemoveState::StartRecord;
                smallvec![Effect::Storage(StorageEffect::StartTransaction {
                    read: false
                })]
            }
            // The mark stays, so the schedule tries the provider bucket again.
            (RemoveState::DropProvider, _, Event::Blob(BlobEvent::Error(error))) => {
                self.fail(ImportRuleError::Read(error.reason()))
            }
            (
                RemoveState::StartRecord,
                _,
                Event::Storage(StorageEvent::TransactionStarted { txn_id }),
            ) => {
                let Some(cleanup) = self.cleanup.as_ref() else {
                    return self.fail(ImportRuleError::Unexpected("StartRecord"));
                };
                self.txn_id = Some(txn_id);
                self.state = RemoveState::ReadRecord;
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: STORAGE_BACKEND_KEYSPACE.to_string(),
                    key: backend_key(cleanup.backend_id),
                    txn_id: Some(txn_id),
                })]
            }
            (
                RemoveState::ReadRecord,
                _,
                Event::Storage(StorageEvent::ReadResult { value, .. }),
            ) => self.settle(value),
            (
                RemoveState::WriteRecord,
                _,
                Event::Storage(StorageEvent::BatchWriteResult { .. }),
            ) => self.delete_mark(),
            (RemoveState::DeleteMark, _, Event::Storage(StorageEvent::DeleteResult { .. })) => {
                let Some(txn_id) = self.txn_id else {
                    return self.fail(ImportRuleError::Unexpected("DeleteMark"));
                };
                self.state = RemoveState::CommitRecord;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            (
                RemoveState::CommitRecord,
                _,
                Event::Storage(StorageEvent::TransactionCommitted { .. }),
            ) => {
                self.txn_id = None;
                self.state = RemoveState::Finish;
                self.output = Some(Ok(()));
                smallvec![]
            }
            (RemoveState::Unmark, _, Event::Storage(StorageEvent::DeleteResult { .. })) => {
                self.state = RemoveState::Finish;
                self.output = Some(Ok(()));
                smallvec![]
            }
            _ => self.fail(ImportRuleError::Unexpected("RemoveImport")),
        }
    }

    fn is_complete(&self) -> bool {
        self.state == RemoveState::Finish
    }

    fn finalize(self) -> Result<(), ImportRuleError> {
        self.output
            .unwrap_or(Err(ImportRuleError::Unexpected("Finish")))
    }

    fn abort(&mut self) -> Effects {
        self.txn_id
            .take()
            .map(|txn_id| smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })])
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn storage(event: StorageEvent) -> Event {
        Event::Storage(event)
    }

    /// Answers a page scan with `rows` object rows and checks that their delete follows.
    fn drain(operation: &mut RemoveImportOperation, rows: usize) -> Effects {
        let values = (0..rows)
            .map(|row| (Key::from(vec![row as u8]), Value::from(Vec::new())))
            .collect();
        let effects = operation.step(storage(StorageEvent::IterResult {
            values,
            next_start_after: None,
        }));
        if rows > 0 {
            assert!(matches!(
                effects.as_slice(),
                [Effect::Storage(StorageEffect::BatchDelete { .. })]
            ));
        }
        // The write intents and earlier patterns of the rule go after its rows.
        if let [Effect::Storage(StorageEffect::Iter { key_space, .. })] = effects.as_slice() {
            assert!(
                [IMPORT_INTENT_KEYSPACE, IMPORT_PATTERN_KEYSPACE].contains(&key_space.as_str())
            );
            return drain(operation, 0);
        }
        effects
    }

    fn unmarked(operation: &mut RemoveImportOperation, effects: Effects) {
        let [
            Effect::Storage(StorageEffect::Delete {
                key_space, txn_id, ..
            }),
        ] = effects.as_slice()
        else {
            panic!("expected the mark delete, got {effects:?}")
        };
        assert_eq!(
            (key_space.as_str(), *txn_id),
            (IMPORT_REMOVAL_KEYSPACE, None)
        );
        operation.step(storage(StorageEvent::DeleteResult {
            key: Vec::new().into(),
        }));
        assert!(operation.is_complete());
    }

    #[test]
    fn marks_removed_rule() {
        let (group_id, import_id) = (Ulid::from_parts(1, 1), Ulid::from_parts(2, 2));
        let mut operation = RemoveImportOperation::new(group_id, import_id);
        operation.start();
        let txn_id = TxnId::from(7);
        operation.step(storage(StorageEvent::TransactionStarted { txn_id }));
        let effects = operation.step(storage(StorageEvent::BatchReadResult {
            values: vec![
                (Key::from(Vec::new()), Some(vec![1u8].into())),
                (Key::from(Vec::new()), None),
            ],
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
        mark(&mut operation, Some(cleanup().to_bytes().unwrap()));
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
        mark(&mut operation, Some(cleanup().to_bytes().unwrap()));
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
            task_handle: None,
            compute_handle: None,
        };
        let import_id = Ulid::from_parts(2, 2);
        let mut writes = record_writes(&deleting()).unwrap();
        let mark_key: Key = import_id.to_bytes().to_vec().into();
        let mark_value: Value = cleanup().to_bytes().unwrap().into();
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
        let cleanup = ProviderCleanup::from_mark(&value.unwrap()).unwrap();
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

    #[test]
    fn resumes_row_removal() {
        let mut operation = RemoveImportOperation::resume(Ulid::from_parts(2, 2));
        let effects = mark(&mut operation, Some(Vec::new()));
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
