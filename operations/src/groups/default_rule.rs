//! Makes one group storage the default for new uploads, with its bucket found or created.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::groups::backends::{backend_key, parse_read, record_writes};
use crate::groups::fence::{parse_group_record, read_group_record};
use crate::groups::storage_routing::{PutGroupError, load_group_inputs, routing_key};
use aruna_core::UserId;
use aruna_core::effects::{BlobEffect, Effect, IterStart, StorageEffect};
use aruna_core::events::{BlobEvent, Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::{
    BACKEND_IMPORT_KEYSPACE, STORAGE_BACKEND_KEYSPACE, STORAGE_ROUTING_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::backend_import::BackendImport;
use aruna_core::structs::storage::blob::BackendRef;
use aruna_core::structs::storage::group_backend::{
    BUCKET_STATE, DEFAULT_BUCKET, GroupStorage, LAYOUT_BUCKET, valid_bucket,
};
use aruna_core::structs::storage::routing::{GroupStorageRouting, RoutingError, RoutingTarget};
use aruna_core::types::{Effects, GroupId, Key, TxnId, Value};
use smallvec::smallvec;
use std::time::SystemTime;
use ulid::Ulid;

const SCAN_PAGE: usize = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DefaultState {
    Init,
    LoadInputs,
    ReadBackend,
    EnsureBucket,
    Start,
    Verify,
    Scan,
    Write,
    Commit,
    Finish,
}

/// The default rule of a group: all buckets go to `backend_id`. A storage without a bucket
/// takes Aruna's layout in `bucket`, which is found or created before anything is written.
#[derive(Debug, PartialEq)]
pub struct PutDefaultOperation {
    group_id: GroupId,
    backend_id: Ulid,
    bucket: Option<String>,
    updated_by: UserId,
    updated_at: SystemTime,
    imports: Vec<BackendImport>,
    /// The bucket the record gets, and whether Aruna created it.
    checked: Option<(String, Option<bool>)>,
    /// The storage record as the write transaction read it.
    verified: Option<GroupStorage>,
    state: DefaultState,
    txn_id: Option<TxnId>,
    output: Option<Result<GroupStorageRouting, PutGroupError>>,
}

impl PutDefaultOperation {
    pub fn new(
        group_id: GroupId,
        backend_id: Ulid,
        bucket: Option<String>,
        updated_by: UserId,
        updated_at: SystemTime,
    ) -> Self {
        Self {
            group_id,
            backend_id,
            bucket,
            updated_by,
            updated_at,
            imports: Vec::new(),
            checked: None,
            verified: None,
            state: DefaultState::Init,
            txn_id: None,
            output: None,
        }
    }

    fn fail(&mut self, error: PutGroupError) -> Effects {
        self.state = DefaultState::Finish;
        self.output = Some(Err(error));
        self.abort()
    }

    fn owned(&self, record: Option<GroupStorage>) -> Result<GroupStorage, PutGroupError> {
        let record = record
            .filter(|record| record.group_id == self.group_id)
            .ok_or(RoutingError::ForeignBackend(self.backend_id))?;
        if record.disabled {
            return Err(PutGroupError::Invalid(
                "The storage is disabled.".to_string(),
            ));
        }
        Ok(record)
    }

    /// The bucket of a storage without a bucket: the requested one or the stored one.
    fn choose(&mut self, record: &GroupStorage) -> Result<Option<String>, PutGroupError> {
        let invalid = |reason: &str| Err(PutGroupError::Invalid(reason.to_string()));
        if !record.splits() {
            return match self.bucket {
                Some(_) => invalid("This storage has a fixed bucket."),
                None => Ok(None),
            };
        }
        let Some(bucket) = self
            .bucket
            .clone()
            .or(record.default_bucket().map(str::to_string))
        else {
            return invalid("Name a bucket for the default rule.");
        };
        if !valid_bucket(&bucket) {
            return invalid(
                "A bucket name has 3 to 63 lowercase letters, digits, dots or hyphens.",
            );
        }
        if let Some(rule) = self
            .imports
            .iter()
            .find(|rule| rule.uses_bucket(self.backend_id, &bucket))
        {
            return Err(linked_error(&bucket, rule));
        }
        Ok(Some(bucket))
    }

    fn read_backend(&mut self, event: Event) -> Effects {
        let record = match parse_read(event, GroupStorage::from_bytes)
            .map_err(|error| PutGroupError::InputsUnavailable(error.to_string()))
            .and_then(|record| self.owned(record))
        {
            Ok(record) => record,
            Err(error) => return self.fail(error),
        };
        let bucket = match self.choose(&record) {
            Ok(bucket) => bucket,
            Err(error) => return self.fail(error),
        };
        match (bucket, self.bucket.is_some()) {
            // A named bucket is checked again: it may be new, or gone since.
            (Some(bucket), true) => {
                self.state = DefaultState::EnsureBucket;
                let effect = BlobEffect::EnsureBucket {
                    backend: BackendRef::Group(self.backend_id),
                    bucket: bucket.clone(),
                };
                self.checked = Some((bucket, None));
                smallvec![Effect::Blob(effect)]
            }
            (bucket, _) => {
                self.checked = bucket.map(|bucket| (bucket, None));
                self.begin()
            }
        }
    }

    fn ensured(&mut self, event: Event) -> Effects {
        match event {
            Event::Blob(BlobEvent::BucketReady { created }) => {
                if let Some((_, state)) = self.checked.as_mut() {
                    *state = Some(created);
                }
                self.begin()
            }
            Event::Blob(BlobEvent::Error(error)) => self.fail(PutGroupError::Unusable(error)),
            _ => self.fail(PutGroupError::Unexpected("EnsureBucket")),
        }
    }

    fn begin(&mut self) -> Effects {
        self.state = DefaultState::Start;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    /// The record is read again with the group fence, so a disable or a deletion that
    /// committed meanwhile wins. The linked rules are read again in the same transaction.
    fn verify(&mut self, event: Event) -> Effects {
        let record = match parse_group_record(event)
            .map_err(PutGroupError::from)
            .and_then(|value| {
                Ok(value
                    .map(|value| GroupStorage::from_bytes(&value))
                    .transpose()?)
            })
            .and_then(|record| self.owned(record))
        {
            Ok(record) => record,
            Err(error) => return self.fail(error),
        };
        if record.splits() != self.checked.is_some() {
            return self.fail(PutGroupError::Unexpected("Verify"));
        }
        self.verified = Some(record);
        match self.checked.is_some() {
            true => self.scan(None),
            false => self.write(),
        }
    }

    fn scan(&mut self, start_after: Option<Key>) -> Effects {
        self.state = DefaultState::Scan;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: BACKEND_IMPORT_KEYSPACE.to_string(),
            prefix: Some(self.group_id.to_bytes().to_vec().into()),
            start: start_after.map(IterStart::After),
            limit: SCAN_PAGE,
            txn_id: self.txn_id,
        })]
    }

    fn scanned(&mut self, values: Vec<(Key, Value)>, next: Option<Key>) -> Effects {
        let Some((bucket, _)) = self.checked.clone() else {
            return self.fail(PutGroupError::Unexpected("Scan"));
        };
        for (_, value) in values {
            match BackendImport::from_bytes(&value) {
                Ok(rule) if rule.uses_bucket(self.backend_id, &bucket) => {
                    return self.fail(linked_error(&bucket, &rule));
                }
                Ok(_) => {}
                Err(error) => return self.fail(error.into()),
            }
        }
        match next {
            Some(next) => self.scan(Some(next)),
            None => self.write(),
        }
    }

    fn write(&mut self) -> Effects {
        let Some(mut record) = self.verified.take() else {
            return self.fail(PutGroupError::Unexpected("Write"));
        };
        let routing = GroupStorageRouting {
            group_id: self.group_id,
            default_target: Some(RoutingTarget::Backend(BackendRef::Group(self.backend_id))),
            updated_at: self.updated_at,
            updated_by: self.updated_by,
        };
        let mut writes = match routing.to_bytes() {
            Ok(value) => vec![(
                STORAGE_ROUTING_KEYSPACE.to_string(),
                routing_key(self.group_id),
                value.into(),
            )],
            Err(error) => return self.fail(error.into()),
        };
        if let Some((bucket, created)) = self.checked.clone() {
            let config = &mut record.public_config;
            config.insert(DEFAULT_BUCKET.to_string(), bucket.clone());
            // Aruna's files stay in a former default bucket, so it stays reserved.
            config.insert(format!("{LAYOUT_BUCKET}{bucket}"), "true".to_string());
            if let Some(created) = created {
                let state = if created { "created" } else { "found" };
                config.insert(format!("{BUCKET_STATE}{bucket}"), state.to_string());
            }
            record.updated_at = self.updated_at;
            match record_writes(&record) {
                Ok(records) => writes.extend(records),
                Err(error) => return self.fail(error.into()),
            }
        }
        self.output = Some(Ok(routing));
        self.state = DefaultState::Write;
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: self.txn_id,
        })]
    }
}

fn linked_error(bucket: &str, rule: &BackendImport) -> PutGroupError {
    PutGroupError::Conflict(format!(
        "Bucket `{bucket}` is linked to the Aruna bucket `{}`. Pick another bucket.",
        rule.bucket
    ))
}

impl Operation for PutDefaultOperation {
    type Output = GroupStorageRouting;
    type Error = PutGroupError;

    fn start(&mut self) -> Effects {
        self.state = DefaultState::LoadInputs;
        smallvec![load_group_inputs(self.group_id)]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }
        match (self.state, event) {
            (
                DefaultState::LoadInputs,
                Event::SubOperation(SubOperationEvent::GroupRoutingLoaded { result }),
            ) => match result {
                Ok(inputs) if inputs.backend_ids.contains(&self.backend_id) => {
                    self.imports = inputs.imports;
                    self.state = DefaultState::ReadBackend;
                    smallvec![Effect::Storage(StorageEffect::Read {
                        key_space: STORAGE_BACKEND_KEYSPACE.to_string(),
                        key: backend_key(self.backend_id),
                        txn_id: None,
                    })]
                }
                Ok(_) => self.fail(RoutingError::ForeignBackend(self.backend_id).into()),
                Err(error) => self.fail(PutGroupError::InputsUnavailable(error)),
            },
            (DefaultState::ReadBackend, event) => self.read_backend(event),
            (DefaultState::EnsureBucket, event) => self.ensured(event),
            (DefaultState::Start, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.txn_id = Some(txn_id);
                self.state = DefaultState::Verify;
                smallvec![read_group_record(
                    self.group_id,
                    STORAGE_BACKEND_KEYSPACE,
                    backend_key(self.backend_id),
                    txn_id,
                )]
            }
            (DefaultState::Verify, event) => self.verify(event),
            (
                DefaultState::Scan,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => self.scanned(values, next_start_after),
            (DefaultState::Write, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                let Some(txn_id) = self.txn_id else {
                    return self.fail(PutGroupError::Unexpected("Write"));
                };
                self.state = DefaultState::Commit;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            (DefaultState::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.txn_id = None;
                self.state = DefaultState::Finish;
                smallvec![]
            }
            _ => self.fail(PutGroupError::Unexpected("PutDefault")),
        }
    }

    fn is_complete(&self) -> bool {
        self.state == DefaultState::Finish
    }

    fn finalize(self) -> Result<GroupStorageRouting, PutGroupError> {
        self.output.unwrap_or(Err(PutGroupError::NotFinished))
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
    use aruna_core::structs::storage::backend_import::DEFAULT_PATTERN;
    use aruna_core::structs::storage::cleanup::CleanupStrategy;
    use aruna_core::structs::storage::group_backend::GroupBackendKind;
    use aruna_core::structs::storage::routing::GroupRoutingInputs;
    use std::collections::{BTreeSet, HashMap};

    fn group() -> Ulid {
        Ulid::from_bytes([1u8; 16])
    }

    fn backend() -> Ulid {
        Ulid::from_bytes([2u8; 16])
    }

    fn storage(public: &[(&str, &str)]) -> GroupStorage {
        GroupStorage {
            backend_id: backend(),
            group_id: group(),
            name: "lab".to_string(),
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

    fn linked(prefix: &str) -> BackendImport {
        BackendImport {
            import_id: Ulid::nil(),
            group_id: group(),
            backend_id: backend(),
            backend_prefix: prefix.to_string(),
            bucket: "seq".to_string(),
            key_prefix: String::new(),
            schedule: None,
            created_by: UserId::default(),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            last_job: None,
            last_started_ms: None,
            write_backend_id: None,
            write_prefix: None,
            write_pattern: DEFAULT_PATTERN.to_string(),
        }
    }

    /// Runs the operation up to the read of the storage record and answers it.
    fn read(
        bucket: Option<&str>,
        record: GroupStorage,
        imports: Vec<BackendImport>,
    ) -> (PutDefaultOperation, Effects) {
        let mut operation = PutDefaultOperation::new(
            group(),
            backend(),
            bucket.map(str::to_string),
            UserId::default(),
            SystemTime::UNIX_EPOCH,
        );
        operation.start();
        let inputs = GroupRoutingInputs {
            backend_ids: BTreeSet::from([backend()]),
            imports,
            ..GroupRoutingInputs::default()
        };
        let effects = operation.step(Event::SubOperation(SubOperationEvent::GroupRoutingLoaded {
            result: Ok(inputs),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { .. })]
        ));
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: backend_key(backend()),
            value: Some(record.to_bytes().unwrap().into()),
        }));
        (operation, effects)
    }

    #[test]
    fn writes_default_bucket() {
        let record = storage(&[("endpoint", "https://s3.lab")]);
        let (mut operation, effects) = read(Some("lab-default"), record.clone(), Vec::new());
        let [Effect::Blob(BlobEffect::EnsureBucket { bucket, .. })] = effects.as_slice() else {
            panic!("expected a bucket check, got {effects:?}");
        };
        assert_eq!(bucket, "lab-default");

        let effects = operation.step(Event::Blob(BlobEvent::BucketReady { created: true }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::StartTransaction { .. })]
        ));
        let txn_id = TxnId::from(3);
        let effects = verified(&mut operation, &record, txn_id);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Iter {
                txn_id: Some(_),
                ..
            })]
        ));
        let effects = operation.step(Event::Storage(StorageEvent::IterResult {
            values: Vec::new(),
            next_start_after: None,
        }));
        let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
            panic!("expected one write, got {effects:?}");
        };
        assert_eq!(writes.len(), 3);
        let stored = GroupStorage::from_bytes(&writes[1].2).unwrap();
        assert_eq!(stored.default_bucket(), Some("lab-default"));
        assert_eq!(stored.layout_buckets(), vec!["lab-default"]);
        assert_eq!(
            stored
                .public_config
                .get("bucket:lab-default")
                .map(String::as_str),
            Some("created")
        );
        let routing = GroupStorageRouting::from_bytes(&writes[0].2).unwrap();
        assert_eq!(
            routing.default_target,
            Some(RoutingTarget::Backend(BackendRef::Group(backend())))
        );

        operation.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));
        operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id,
        }));
        assert!(operation.is_complete());
        assert!(operation.finalize().is_ok());
    }

    /// Starts the write transaction and answers its read of the storage record.
    fn verified(
        operation: &mut PutDefaultOperation,
        record: &GroupStorage,
        txn_id: TxnId,
    ) -> Effects {
        operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![
                (
                    backend_key(backend()),
                    Some(record.to_bytes().unwrap().into()),
                ),
                (routing_key(group()), None),
            ],
        }))
    }

    #[test]
    fn rechecks_links_late() {
        // A link saved while the bucket was checked stops the default before it is written.
        let record = storage(&[("endpoint", "https://s3.lab")]);
        let (mut operation, _) = read(Some("lab-raw"), record.clone(), Vec::new());
        operation.step(Event::Blob(BlobEvent::BucketReady { created: false }));
        verified(&mut operation, &record, TxnId::from(4));
        let rule = linked("lab-raw/");
        let effects = operation.step(Event::Storage(StorageEvent::IterResult {
            values: vec![(rule.key(), rule.to_bytes().unwrap().into())],
            next_start_after: None,
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { .. })]
        ));
        assert!(matches!(
            operation.finalize(),
            Err(PutGroupError::Conflict(_))
        ));
    }

    #[test]
    fn refuses_linked_bucket() {
        let record = storage(&[("endpoint", "https://s3.lab")]);
        let (operation, effects) = read(Some("lab-raw"), record, vec![linked("lab-raw/2024/")]);
        assert!(effects.is_empty(), "{effects:?}");
        assert!(matches!(
            operation.finalize(),
            Err(PutGroupError::Conflict(_))
        ));
    }

    #[test]
    fn checks_bucket_input() {
        let split = storage(&[("endpoint", "https://s3.lab")]);
        let fixed = storage(&[("endpoint", "https://s3.lab"), ("bucket", "data")]);
        for (bucket, record) in [
            (None, split.clone()),
            (Some("Bad_Name"), split),
            (Some("x-y-z"), fixed),
        ] {
            let (operation, effects) = read(bucket, record, Vec::new());
            assert!(effects.is_empty(), "{effects:?}");
            assert!(matches!(
                operation.finalize(),
                Err(PutGroupError::Invalid(_))
            ));
        }
    }

    #[test]
    fn keeps_stored_bucket() {
        // Choosing the storage again needs no new check of its known bucket.
        let record = storage(&[
            ("endpoint", "https://s3.lab"),
            ("default_bucket", "lab-default"),
        ]);
        let (_, effects) = read(None, record, Vec::new());
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::StartTransaction { .. })]
        ));
    }

    #[test]
    fn rejects_stray_event() {
        let record = storage(&[("endpoint", "https://s3.lab")]);
        let (mut operation, _) = read(Some("lab-default"), record, Vec::new());
        let effects = operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TxnId::from(1),
        }));
        assert!(effects.is_empty());
        assert!(matches!(
            operation.finalize(),
            Err(PutGroupError::Unexpected(_))
        ));
    }
}
