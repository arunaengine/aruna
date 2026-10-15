//! Creates a backend import rule together with the bucket routing rule for its folder.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::schedule::parse_schedule;
use super::{ImportRuleError, folders_overlap, validate_folder};
use crate::groups::backends::{backend_key, record_writes};
use crate::groups::fence::group_fence_key;
use aruna_core::UserId;
use aruna_core::effects::{BlobEffect, Effect, IterStart, StorageEffect};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{
    BACKEND_IMPORT_KEYSPACE, BUCKET_ENCRYPTION_KEYSPACE, S3_BUCKET_KEYSPACE,
    STORAGE_BACKEND_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::group_delete::check_group_write;
use aruna_core::structs::storage::backend_import::{BackendImport, valid_pattern};
use aruna_core::structs::storage::blob::{BackendRef, BucketInfo};
use aruna_core::structs::storage::encryption::BucketEncryption;
use aruna_core::structs::storage::format::Compression;
use aruna_core::structs::storage::group_backend::{
    BUCKET_STATE, GroupStorage, MAX_KEY_LEN, path_bucket, valid_bucket,
};
use aruna_core::structs::storage::routing::{RoutingTarget, StorageRoutingRule};
use aruna_core::types::{Effects, GroupId, Key, TxnId, Value};
use smallvec::smallvec;
use std::time::SystemTime;
use ulid::Ulid;

const SCAN_PAGE: usize = 256;

/// Room a file needs below its folder: an id, its separator and one character.
const NAME_ROOM: usize = 28;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateImportInput {
    pub group_id: GroupId,
    pub created_by: UserId,
    pub backend_id: Ulid,
    pub backend_prefix: String,
    pub bucket: String,
    pub key_prefix: String,
    pub schedule: Option<String>,
    pub write_backend_id: Option<Ulid>,
    pub write_prefix: Option<String>,
    pub write_pattern: String,
    pub now: SystemTime,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CreateState {
    Init,
    Backends,
    Ensure,
    Start,
    Read,
    Scan,
    Write,
    Commit,
    Finish,
}

/// Checks the bucket, both backends and the group's other rules in one transaction, then
/// writes the rule and, when it is missing, the routing rule that sends the folder's
/// writes to the write backend.
#[derive(Debug, PartialEq)]
pub struct CreateImportOperation {
    input: CreateImportInput,
    state: CreateState,
    txn_id: Option<TxnId>,
    /// The bucket record with the added routing rule; `None` keeps the stored one.
    routed: Option<BucketInfo>,
    /// Rule buckets on storages without a bucket, still to check.
    pending: Vec<(Ulid, String)>,
    /// Checked rule buckets and whether Aruna created them.
    checked: Vec<(Ulid, String, bool)>,
    /// Both storage records as the transaction read them.
    backends: Vec<GroupStorage>,
    output: Option<Result<BackendImport, ImportRuleError>>,
}

impl CreateImportOperation {
    pub fn new(input: CreateImportInput) -> Self {
        Self {
            input,
            state: CreateState::Init,
            txn_id: None,
            routed: None,
            pending: Vec::new(),
            checked: Vec::new(),
            backends: Vec::new(),
            output: None,
        }
    }

    fn fail(&mut self, error: ImportRuleError) -> Effects {
        self.state = CreateState::Finish;
        self.output = Some(Err(error));
        self.abort()
    }

    fn invalid(&mut self, reason: &str) -> Effects {
        self.fail(ImportRuleError::Invalid(reason.to_string()))
    }

    fn validate(&self) -> Result<(), ImportRuleError> {
        validate_folder("backend_prefix", &self.input.backend_prefix)?;
        validate_folder("key_prefix", &self.input.key_prefix)?;
        if let Some(prefix) = self.input.write_prefix.as_deref() {
            validate_folder("write_prefix", prefix)?;
        }
        if !valid_pattern(&self.input.write_pattern) {
            return Err(ImportRuleError::Invalid(
                "write_pattern needs {name} and may only use {dir}, {name} and {id}".to_string(),
            ));
        }
        if self.input.bucket.is_empty() {
            return Err(ImportRuleError::Invalid("bucket is required".to_string()));
        }
        if let Some(schedule) = self.input.schedule.as_deref() {
            parse_schedule(schedule)?;
        }
        Ok(())
    }

    fn record(&self) -> BackendImport {
        BackendImport {
            import_id: Ulid::generate(),
            group_id: self.input.group_id,
            backend_id: self.input.backend_id,
            backend_prefix: self.input.backend_prefix.clone(),
            bucket: self.input.bucket.clone(),
            key_prefix: self.input.key_prefix.clone(),
            schedule: self.input.schedule.clone(),
            created_by: self.input.created_by,
            created_at: self.input.now,
            updated_at: self.input.now,
            last_job: None,
            last_started_ms: None,
            write_backend_id: self.input.write_backend_id,
            write_prefix: self.input.write_prefix.clone(),
            write_pattern: self.input.write_pattern.clone(),
        }
    }

    fn read_backends(&mut self) -> Effects {
        self.state = CreateState::Backends;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (
                    STORAGE_BACKEND_KEYSPACE.to_string(),
                    backend_key(self.input.backend_id)
                ),
                (
                    STORAGE_BACKEND_KEYSPACE.to_string(),
                    backend_key(self.write_backend())
                ),
            ],
            txn_id: None,
        })]
    }

    /// Checks both folders before any bucket is touched, then queues the rule buckets of
    /// storages without a bucket.
    fn backends_read(&mut self, values: Vec<(Key, Option<Value>)>) -> Effects {
        let [backend, write_backend] = match <[_; 2]>::try_from(values) {
            Ok(values) => values.map(|(_, value)| value),
            Err(_) => return self.fail(ImportRuleError::Unexpected("Backends")),
        };
        let (backend, write_backend) = match (self.owned(backend), self.owned(write_backend)) {
            (Ok(backend), Ok(write_backend)) => (backend, write_backend),
            (Err(error), _) | (_, Err(error)) => return self.fail(error),
        };
        let folders = [
            (backend, self.input.backend_prefix.clone()),
            (write_backend, self.write_folder().to_string()),
        ];
        for (storage, folder) in folders {
            match check_folder(&storage, &folder) {
                Ok(Some(bucket))
                    if !self.pending.contains(&(storage.backend_id, bucket.clone())) =>
                {
                    self.pending.push((storage.backend_id, bucket));
                }
                Ok(_) => {}
                Err(error) => return self.fail(error),
            }
        }
        self.ensure_next()
    }

    fn ensure_next(&mut self) -> Effects {
        let Some((backend_id, bucket)) = self.pending.first().cloned() else {
            self.state = CreateState::Start;
            return smallvec![Effect::Storage(StorageEffect::StartTransaction {
                read: false
            })];
        };
        self.state = CreateState::Ensure;
        smallvec![Effect::Blob(BlobEffect::EnsureBucket {
            backend: BackendRef::Group(backend_id),
            bucket,
        })]
    }

    fn ensured(&mut self, event: Event) -> Effects {
        match event {
            Event::Blob(BlobEvent::BucketReady { created }) if !self.pending.is_empty() => {
                let (backend_id, bucket) = self.pending.remove(0);
                self.checked.push((backend_id, bucket, created));
                self.ensure_next()
            }
            Event::Blob(BlobEvent::Error(error)) => {
                self.fail(ImportRuleError::Invalid(error.reason()))
            }
            _ => self.fail(ImportRuleError::Unexpected("Ensure")),
        }
    }

    fn read(&mut self, txn_id: TxnId) -> Effects {
        self.txn_id = Some(txn_id);
        self.state = CreateState::Read;
        let bucket: Key = self.input.bucket.as_bytes().to_vec().into();
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (S3_BUCKET_KEYSPACE.to_string(), bucket.clone()),
                (BUCKET_ENCRYPTION_KEYSPACE.to_string(), bucket),
                (
                    STORAGE_BACKEND_KEYSPACE.to_string(),
                    backend_key(self.input.backend_id)
                ),
                group_fence_key(self.input.group_id),
                (
                    STORAGE_BACKEND_KEYSPACE.to_string(),
                    backend_key(self.write_backend())
                ),
            ],
            txn_id: Some(txn_id),
        })]
    }

    fn checked(&mut self, values: Vec<(Key, Option<Value>)>) -> Effects {
        let [bucket, encryption, backend, fence, write_backend] = match <[_; 5]>::try_from(values) {
            Ok(values) => values.map(|(_, value)| value),
            Err(_) => return self.fail(ImportRuleError::Unexpected("Read")),
        };
        if let Err(error) = check_group_write(fence.as_deref()) {
            return self.fail(error.into());
        }
        let bucket = match bucket
            .map(|value| BucketInfo::from_bytes(&value))
            .transpose()
        {
            Ok(Some(bucket)) if bucket.group_id == self.input.group_id => bucket,
            Ok(_) => return self.invalid("the bucket does not exist in this group on this node"),
            Err(error) => return self.fail(error.into()),
        };
        if bucket.compression != Compression::Off {
            return self.invalid("a bucket with compression cannot import objects in place");
        }
        match BucketEncryption::from_row(encryption.as_deref()) {
            Ok(settings) if settings.is_encrypted() => {
                return self.invalid("an encrypted bucket cannot import objects in place");
            }
            Ok(_) => {}
            Err(error) => return self.fail(error.into()),
        }
        let (backend, write_backend) = match (self.owned(backend), self.owned(write_backend)) {
            (Ok(backend), Ok(write_backend)) => (backend, write_backend),
            (Err(error), _) | (_, Err(error)) => return self.fail(error),
        };
        let checks = [
            check_folder(&backend, &self.input.backend_prefix),
            check_folder(&write_backend, self.write_folder()),
        ];
        if let Some(Err(error)) = checks.into_iter().find(Result::is_err) {
            return self.fail(error);
        }
        if backend.is_reserved(&self.input.backend_prefix) {
            return self.invalid("backend_prefix lies inside the folder Aruna writes to");
        }
        if write_backend.is_reserved(self.write_folder()) {
            return self.invalid("write_prefix lies inside the folder Aruna writes to");
        }
        self.backends = vec![backend, write_backend];
        if let Err(error) = self.route(bucket) {
            return self.fail(error);
        }
        self.scan(None)
    }

    fn write_backend(&self) -> Ulid {
        self.input.write_backend_id.unwrap_or(self.input.backend_id)
    }

    fn write_folder(&self) -> &str {
        let prefix = self.input.write_prefix.as_deref();
        prefix.unwrap_or(&self.input.backend_prefix)
    }

    /// An enabled backend of this group.
    fn owned(&self, record: Option<Value>) -> Result<GroupStorage, ImportRuleError> {
        let invalid = |reason: &str| ImportRuleError::Invalid(reason.to_string());
        match record
            .map(|value| GroupStorage::from_bytes(&value))
            .transpose()?
        {
            Some(backend) if backend.group_id != self.input.group_id => {
                Err(invalid("the storage backend does not belong to this group"))
            }
            Some(backend) if backend.disabled => Err(invalid("the storage backend is disabled")),
            Some(backend) => Ok(backend),
            None => Err(invalid("the storage backend does not belong to this group")),
        }
    }

    /// Another rule must neither write where this one reads or writes, nor read where it writes.
    fn shares_folder(&self, rule: &BackendImport) -> bool {
        let (read, write) = (self.input.backend_id, self.write_backend());
        let prefix = self.input.backend_prefix.as_str();
        (rule.write_backend() == write && folders_overlap(rule.write_folder(), self.write_folder()))
            || (rule.backend_id == write
                && folders_overlap(&rule.backend_prefix, self.write_folder()))
            || (rule.write_backend() == read && folders_overlap(rule.write_folder(), prefix))
    }

    /// Adds the folder's routing rule; an existing rule for the same folder must agree.
    fn route(&mut self, mut bucket: BucketInfo) -> Result<(), ImportRuleError> {
        let target = RoutingTarget::Backend(BackendRef::Group(self.write_backend()));
        let existing = bucket
            .storage_routing
            .iter()
            .find(|rule| !rule.exact && rule.key_prefix == self.input.key_prefix);
        match existing {
            Some(rule) if rule.target == target => Ok(()),
            Some(_) => Err(ImportRuleError::Conflict(
                "a routing rule for this folder names another target".to_string(),
            )),
            None => {
                bucket.storage_routing.push(StorageRoutingRule {
                    key_prefix: self.input.key_prefix.clone(),
                    exact: false,
                    target,
                });
                self.routed = Some(bucket);
                Ok(())
            }
        }
    }

    fn scan(&mut self, start_after: Option<Key>) -> Effects {
        self.state = CreateState::Scan;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: BACKEND_IMPORT_KEYSPACE.to_string(),
            prefix: Some(self.input.group_id.to_bytes().to_vec().into()),
            start: start_after.map(IterStart::After),
            limit: SCAN_PAGE,
            txn_id: self.txn_id,
        })]
    }

    fn scanned(&mut self, values: Vec<(Key, Value)>, next: Option<Key>) -> Effects {
        for (_, value) in values {
            let rule = match BackendImport::from_bytes(&value) {
                Ok(rule) => rule,
                Err(error) => return self.fail(error.into()),
            };
            if rule.bucket == self.input.bucket
                && folders_overlap(&rule.key_prefix, &self.input.key_prefix)
            {
                return self.fail(ImportRuleError::Conflict(format!(
                    "import {} already imports into this folder",
                    rule.import_id
                )));
            }
            if self.shares_folder(&rule) {
                return self.fail(ImportRuleError::Conflict(format!(
                    "import {} reads or writes the same backend folder",
                    rule.import_id
                )));
            }
        }
        if next.is_some() {
            return self.scan(next);
        }
        self.write()
    }

    fn write(&mut self) -> Effects {
        let record = self.record();
        let mut writes = Vec::new();
        match record.to_bytes() {
            Ok(value) => writes.push((
                BACKEND_IMPORT_KEYSPACE.to_string(),
                record.key(),
                value.into(),
            )),
            Err(error) => return self.fail(error.into()),
        }
        if let Some(bucket) = self.routed.take() {
            match bucket.to_bytes() {
                Ok(value) => writes.push((
                    S3_BUCKET_KEYSPACE.to_string(),
                    self.input.bucket.as_bytes().to_vec().into(),
                    value.into(),
                )),
                Err(error) => return self.fail(error.into()),
            }
        }
        match self.noted_backends() {
            Ok(records) => writes.extend(records),
            Err(error) => return self.fail(error),
        }
        self.output = Some(Ok(record));
        self.state = CreateState::Write;
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: self.txn_id,
        })]
    }

    /// The storage records with the state of each checked bucket, for the bucket icons.
    fn noted_backends(&mut self) -> Result<Vec<(String, Key, Value)>, ImportRuleError> {
        let mut backends = std::mem::take(&mut self.backends);
        backends.dedup_by_key(|record| record.backend_id);
        let mut writes = Vec::new();
        for mut record in backends {
            let states = self
                .checked
                .iter()
                .filter(|(backend_id, ..)| *backend_id == record.backend_id)
                .map(|(_, bucket, created)| {
                    let state = if *created { "created" } else { "found" };
                    (format!("{BUCKET_STATE}{bucket}"), state.to_string())
                })
                .collect::<Vec<_>>();
            if states.is_empty() {
                continue;
            }
            record.public_config.extend(states);
            record.updated_at = self.input.now;
            writes.extend(record_writes(&record)?);
        }
        Ok(writes)
    }

    fn commit(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(ImportRuleError::Unexpected("Write"));
        };
        self.state = CreateState::Commit;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }
}

/// A folder on a storage without a bucket starts with a bucket that never held the default
/// rule; any folder leaves room for a file below it. Answers the bucket to check.
fn check_folder(storage: &GroupStorage, folder: &str) -> Result<Option<String>, ImportRuleError> {
    let (bucket, rest) = match storage.splits() {
        true => match path_bucket(folder).filter(|bucket| valid_bucket(bucket)) {
            Some(bucket) => (Some(bucket), &folder[bucket.len() + 1..]),
            None => {
                return Err(ImportRuleError::Invalid(format!(
                    "On {} the folder starts with a bucket, like `lab-raw/`.",
                    storage.name
                )));
            }
        },
        false => (None, folder),
    };
    // A former default bucket may still hold Aruna's own files.
    if let Some(bucket) = bucket.filter(|bucket| storage.layout_buckets().contains(bucket)) {
        return Err(ImportRuleError::Conflict(format!(
            "Bucket `{bucket}` holds Aruna's files of a default rule. Pick another bucket."
        )));
    }
    if rest.len() + NAME_ROOM > MAX_KEY_LEN {
        return Err(ImportRuleError::Invalid(
            "The folder is too long: paths on this storage are at most 1024 bytes.".to_string(),
        ));
    }
    Ok(bucket.map(str::to_string))
}

impl Operation for CreateImportOperation {
    type Output = BackendImport;
    type Error = ImportRuleError;

    fn start(&mut self) -> Effects {
        if let Err(error) = self.validate() {
            return self.fail(error);
        }
        self.read_backends()
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }
        match (self.state, event) {
            (CreateState::Backends, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                self.backends_read(values)
            }
            (CreateState::Ensure, event) => self.ensured(event),
            (CreateState::Start, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.read(txn_id)
            }
            (CreateState::Read, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                self.checked(values)
            }
            (
                CreateState::Scan,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => self.scanned(values, next_start_after),
            (CreateState::Write, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                self.commit()
            }
            (CreateState::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.txn_id = None;
                self.state = CreateState::Finish;
                smallvec![]
            }
            _ => self.fail(ImportRuleError::Unexpected("CreateImport")),
        }
    }

    fn is_complete(&self) -> bool {
        self.state == CreateState::Finish
    }

    fn finalize(self) -> Result<BackendImport, ImportRuleError> {
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
    use aruna_core::errors::BlobError;
    use aruna_core::structs::storage::backend_import::DEFAULT_PATTERN;
    use aruna_core::structs::storage::cleanup::CleanupStrategy;
    use aruna_core::structs::storage::group_backend::GroupBackendKind;
    use std::collections::HashMap;

    fn group() -> Ulid {
        Ulid::from_bytes([1u8; 16])
    }

    fn backend_id() -> Ulid {
        Ulid::from_bytes([2u8; 16])
    }

    fn input(backend_prefix: &str, key_prefix: &str) -> CreateImportInput {
        CreateImportInput {
            group_id: group(),
            created_by: UserId::default(),
            backend_id: backend_id(),
            backend_prefix: backend_prefix.to_string(),
            bucket: "bucket".to_string(),
            key_prefix: key_prefix.to_string(),
            schedule: Some("0 3 * * *".to_string()),
            write_backend_id: None,
            write_prefix: None,
            write_pattern: DEFAULT_PATTERN.to_string(),
            now: SystemTime::UNIX_EPOCH,
        }
    }

    fn bucket(routing: Vec<StorageRoutingRule>, compression: Compression) -> Option<Value> {
        let bucket = BucketInfo {
            group_id: group(),
            created_at: SystemTime::UNIX_EPOCH,
            created_by: UserId::default(),
            cors_configuration: None,
            storage_routing: routing,
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression,
        };
        Some(bucket.to_bytes().unwrap().into())
    }

    fn storage(public: &[(&str, &str)]) -> GroupStorage {
        GroupStorage {
            backend_id: backend_id(),
            group_id: group(),
            name: "tenant".to_string(),
            kind: GroupBackendKind::S3,
            public_config: public
                .iter()
                .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                .collect::<HashMap<_, _>>(),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            created_by: UserId::default(),
            disabled: false,
            cleanup: CleanupStrategy::Retain,
        }
    }

    fn backend() -> Option<Value> {
        Some(storage(&[("bucket", "data")]).to_bytes().unwrap().into())
    }

    fn values(values: Vec<Option<Value>>) -> Event {
        let values = values
            .into_iter()
            .map(|value| (Key::from(Vec::new()), value))
            .collect();
        Event::Storage(StorageEvent::BatchReadResult { values })
    }

    /// Runs the operation up to its reads and answers them with `bucket`.
    fn after_reads(
        input: CreateImportInput,
        bucket: Option<Value>,
    ) -> (CreateImportOperation, Effects) {
        let mut operation = CreateImportOperation::new(input);
        operation.start();
        let effects = operation.step(values(vec![backend(), backend()]));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::StartTransaction { .. })]
        ));
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TxnId::default(),
        }));
        let effects = operation.step(values(vec![bucket, None, backend(), None, backend()]));
        (operation, effects)
    }

    /// Answers the first read of a rule on a storage without a bucket.
    fn on_split(
        input: CreateImportInput,
        record: &GroupStorage,
    ) -> (CreateImportOperation, Effects) {
        let mut operation = CreateImportOperation::new(input);
        operation.start();
        let record: Option<Value> = Some(record.to_bytes().unwrap().into());
        let effects = operation.step(values(vec![record.clone(), record]));
        (operation, effects)
    }

    #[test]
    fn notes_rule_bucket() {
        let record = storage(&[("endpoint", "https://s3.lab")]);
        let (mut operation, effects) = on_split(input("lab-raw/2024/", "imported/"), &record);
        let [Effect::Blob(BlobEffect::EnsureBucket { bucket, .. })] = effects.as_slice() else {
            panic!("expected one bucket check, got {effects:?}");
        };
        assert_eq!(bucket, "lab-raw");

        let effects = operation.step(Event::Blob(BlobEvent::BucketReady { created: false }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::StartTransaction { .. })]
        ));
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TxnId::default(),
        }));
        let stored: Option<Value> = Some(record.to_bytes().unwrap().into());
        operation.step(values(vec![
            bucket_row(),
            None,
            stored.clone(),
            None,
            stored,
        ]));
        let effects = scan_page(&mut operation, &[]);
        let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
            panic!("expected one batch write, got {effects:?}")
        };
        // The rule, the bucket routing and both copies of the storage record.
        assert_eq!(writes.len(), 4);
        let noted = GroupStorage::from_bytes(&writes[2].2).unwrap();
        assert_eq!(
            noted
                .public_config
                .get("bucket:lab-raw")
                .map(String::as_str),
            Some("found")
        );
    }

    fn bucket_row() -> Option<Value> {
        bucket(Vec::new(), Compression::Off)
    }

    #[test]
    fn refuses_split_folders() {
        let record = storage(&[
            ("endpoint", "https://s3.lab"),
            ("default_bucket", "lab-default"),
            ("layout:lab-old", "true"),
        ]);
        let long = format!("lab-raw/{}/", "a".repeat(1000));
        for (folder, conflict) in [
            ("", false),
            ("Raw/", false),
            ("lab-default/x/", true),
            ("lab-old/", true),
            (long.as_str(), false),
        ] {
            let (operation, effects) = on_split(input(folder, "imported/"), &record);
            assert!(effects.is_empty(), "{folder}: {effects:?}");
            let error = failure(operation);
            assert_eq!(
                matches!(error, ImportRuleError::Conflict(_)),
                conflict,
                "{folder}: {error:?}"
            );
        }
    }

    #[test]
    fn reports_bucket_error() {
        let record = storage(&[("endpoint", "https://s3.lab")]);
        let (mut operation, _) = on_split(input("lab-raw/", "imported/"), &record);
        let denied = BlobError::WriteError("No access to bucket `lab-raw`.".to_string());
        assert!(
            operation
                .step(Event::Blob(BlobEvent::Error(denied)))
                .is_empty()
        );
        assert_eq!(
            failure(operation),
            ImportRuleError::Invalid("No access to bucket `lab-raw`.".to_string())
        );
    }

    fn scan_page(operation: &mut CreateImportOperation, rules: &[BackendImport]) -> Effects {
        let values = rules
            .iter()
            .map(|rule| (rule.key(), rule.to_bytes().unwrap().into()))
            .collect();
        operation.step(Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after: None,
        }))
    }

    fn failure(operation: CreateImportOperation) -> ImportRuleError {
        assert!(operation.is_complete());
        operation.finalize().unwrap_err()
    }

    #[test]
    fn writes_routing_rule() {
        let (mut operation, _) = after_reads(
            input("raw/", "imported/"),
            bucket(Vec::new(), Compression::Off),
        );
        let effects = scan_page(&mut operation, &[]);

        let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
            panic!("expected one batch write, got {effects:?}")
        };
        let [(rule_space, _, rule), (bucket_space, _, bucket)] = writes.as_slice() else {
            panic!("expected the rule and the bucket record")
        };
        assert_eq!(rule_space, BACKEND_IMPORT_KEYSPACE);
        assert_eq!(bucket_space, S3_BUCKET_KEYSPACE);
        let rule = BackendImport::from_bytes(rule).unwrap();
        assert_eq!(
            (rule.backend_prefix.as_str(), rule.key_prefix.as_str()),
            ("raw/", "imported/")
        );
        let routing = BucketInfo::from_bytes(bucket).unwrap().storage_routing;
        assert_eq!(
            routing,
            vec![StorageRoutingRule {
                key_prefix: "imported/".to_string(),
                exact: false,
                target: RoutingTarget::Backend(BackendRef::Group(backend_id())),
            }]
        );

        operation.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));
        operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id: TxnId::default(),
        }));
        assert_eq!(operation.finalize().unwrap(), rule);
    }

    #[test]
    fn keeps_matching_route() {
        let routing = vec![StorageRoutingRule {
            key_prefix: "imported/".to_string(),
            exact: false,
            target: RoutingTarget::Backend(BackendRef::Group(backend_id())),
        }];
        let (mut operation, _) =
            after_reads(input("", "imported/"), bucket(routing, Compression::Off));
        let effects = scan_page(&mut operation, &[]);

        let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
            panic!("expected one batch write")
        };
        assert_eq!(writes.len(), 1, "the bucket record stays as it is");
    }

    #[test]
    fn refuses_other_route() {
        let routing = vec![StorageRoutingRule {
            key_prefix: "imported/".to_string(),
            exact: false,
            target: RoutingTarget::Class("cold".to_string()),
        }];
        let (operation, effects) =
            after_reads(input("", "imported/"), bucket(routing, Compression::Off));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { .. })]
        ));
        assert!(matches!(failure(operation), ImportRuleError::Conflict(_)));
    }

    #[test]
    fn refuses_overlapping_import() {
        let (mut operation, _) = after_reads(
            input("", "imported/x/"),
            bucket(Vec::new(), Compression::Off),
        );
        let mut other = operation.record();
        other.key_prefix = "imported/".to_string();
        scan_page(&mut operation, &[other]);

        assert!(matches!(failure(operation), ImportRuleError::Conflict(_)));
    }

    #[test]
    fn refuses_unsuitable_inputs() {
        let compressed = Compression::Zstd { level: 3 };
        let (operation, _) = after_reads(input("", ""), bucket(Vec::new(), compressed));
        assert!(matches!(failure(operation), ImportRuleError::Invalid(_)));

        let (operation, _) = after_reads(input("data/", ""), bucket(Vec::new(), Compression::Off));
        assert!(matches!(failure(operation), ImportRuleError::Invalid(_)));

        let (operation, _) = after_reads(input("", ""), None);
        assert!(matches!(failure(operation), ImportRuleError::Invalid(_)));

        let mut bad = input("raw", "");
        bad.schedule = Some("hourly please".to_string());
        let mut operation = CreateImportOperation::new(bad);
        assert!(operation.start().is_empty());
        assert!(matches!(failure(operation), ImportRuleError::Invalid(_)));
    }

    #[test]
    fn routes_write_backend() {
        let other = Ulid::from_bytes([9u8; 16]);
        let mut moved = input("raw/", "imported/");
        moved.write_backend_id = Some(other);
        moved.write_pattern = "{dir}{name}".to_string();
        let (mut operation, _) = after_reads(moved, bucket(Vec::new(), Compression::Off));
        let effects = scan_page(&mut operation, &[]);

        let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
            panic!("expected one batch write")
        };
        let rule = BackendImport::from_bytes(&writes[0].2).unwrap();
        assert_eq!(rule.write_backend(), other);
        assert!(rule.same_name());
        let routing = BucketInfo::from_bytes(&writes[1].2)
            .unwrap()
            .storage_routing;
        assert_eq!(
            routing[0].target,
            RoutingTarget::Backend(BackendRef::Group(other))
        );
    }

    #[test]
    fn refuses_shared_folder() {
        let (mut operation, _) = after_reads(
            input("raw/x/", "imported/"),
            bucket(Vec::new(), Compression::Off),
        );
        let mut other = operation.record();
        other.key_prefix = "elsewhere/".to_string();
        other.backend_prefix = "raw/".to_string();
        scan_page(&mut operation, &[other]);
        assert!(matches!(failure(operation), ImportRuleError::Conflict(_)));

        let mut bad = input("raw/", "imported/");
        bad.write_pattern = "{dir}{id}".to_string();
        let mut operation = CreateImportOperation::new(bad);
        assert!(operation.start().is_empty());
        assert!(matches!(failure(operation), ImportRuleError::Invalid(_)));
    }
}
