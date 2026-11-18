//! Makes one group storage the default for new uploads, with its bucket found or created.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::groups::backends::{backend_key, record_writes};
use crate::groups::fence::group_fence_key;
use crate::groups::storage_routing::{GroupInputsOperation, PutGroupError, routing_key};
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::events::{BlobEvent, Event};
use aruna_core::keyspaces::{
    BACKEND_IMPORT_KEYSPACE, S3_BUCKET_KEYSPACE, STORAGE_BACKEND_KEYSPACE, STORAGE_ROUTING_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{PrefixPages, done, values};
use aruna_core::structs::identity::group_delete::check_group_write;
use aruna_core::structs::storage::backend_import::BackendImport;
use aruna_core::structs::storage::blob::{BackendRef, BucketInfo};
use aruna_core::structs::storage::group_backend::{
    BUCKET_PREFIX, BUCKET_STATE, BUCKET_TEMPLATE, DEFAULT_BUCKET, GroupBackendKind, GroupStorage,
    LAYOUT_BUCKET, template_bucket, valid_bucket,
};
use aruna_core::structs::storage::routing::{
    GroupRoutingInputs, GroupStorageRouting, RoutingError, RoutingTarget,
};
use aruna_core::types::{GroupId, Key, Value};
use std::time::SystemTime;
use ulid::Ulid;

use crate::groups::storage_routing::GroupInputsError;

const SCAN_PAGE: usize = 256;
/// Bucket rows an own-bucket default without a shared bucket checks at most.
const BUCKET_SCAN: usize = 10_000;

/// What the default rule waits for before its write transaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DefaultState {
    LoadInputs,
    ReadBackend,
    EnsureBucket,
    Write,
}

type DefaultTurn = Result<Next<GroupStorageRouting>, PutGroupError>;

/// The default rule of a group: all buckets go to `backend_id`. A storage without a bucket
/// takes Aruna's layout in `bucket`, which is found or created before anything is written.
#[derive(Debug, PartialEq)]
pub struct PutDefaultOperation {
    group_id: GroupId,
    backend_id: Ulid,
    bucket: Option<String>,
    /// The new `bucket_template`; empty clears it, `None` keeps the stored one.
    template: Option<String>,
    updated_by: UserId,
    updated_at: SystemTime,
    imports: Vec<BackendImport>,
    /// The bucket the record gets, and whether Aruna created it.
    checked: Option<(String, Option<bool>)>,
    state: DefaultState,
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
            template: None,
            updated_by,
            updated_at,
            imports: Vec::new(),
            checked: None,
            state: DefaultState::LoadInputs,
        }
    }

    /// Gives each new bucket its own provider bucket from `template`, or clears it when empty.
    pub fn with_template(mut self, template: Option<String>) -> Self {
        self.template = template;
        self
    }

    /// The bucket of a storage without a bucket: the requested one or the stored one.
    fn choose(&mut self, record: &GroupStorage) -> Result<Option<String>, PutGroupError> {
        let invalid = |reason: &str| Err(PutGroupError::Invalid(reason.to_string()));
        if !record.splits() {
            return match (&self.bucket, &self.template) {
                (Some(_), _) | (_, Some(_)) => invalid("This storage has a fixed bucket."),
                (None, None) => Ok(None),
            };
        }
        let sample = |template: &str| template_bucket(template, "data", "group");
        if let Some(template) = self.template.as_deref().filter(|t| !t.is_empty())
            && (!template.contains("{bucket}") || !valid_bucket(&sample(template)))
        {
            return invalid(
                "A bucket template needs {bucket} and gives S3 bucket names, like aruna-{bucket}.",
            );
        }
        let owned = self.template.as_deref().is_some_and(|t| !t.is_empty());
        let Some(bucket) = self
            .bucket
            .clone()
            .or(record.default_bucket().map(str::to_string))
        else {
            // Each new bucket gets its own provider bucket, so no shared bucket is needed.
            let kept = self.template.is_none() && record.bucket_template().is_some();
            return match owned || kept {
                true => Ok(None),
                false => invalid("Name a bucket for the default rule."),
            };
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

    fn read_backend(&mut self, value: Option<Value>) -> DefaultTurn {
        let record = value
            .map(|value| GroupStorage::from_bytes(&value))
            .transpose()
            .map_err(|error| PutGroupError::InputsUnavailable(error.to_string()))?;
        let record = owned(self.group_id, self.backend_id, record)?;
        match (self.choose(&record)?, self.bucket.is_some()) {
            // A named bucket is checked again: it may be new, or gone since.
            (Some(bucket), true) => {
                self.state = DefaultState::EnsureBucket;
                let effect = BlobEffect::EnsureBucket {
                    backend: BackendRef::Group(self.backend_id),
                    bucket: bucket.clone(),
                };
                self.checked = Some((bucket, None));
                Ok(Next::Call(IoCall::Effect(Box::new(Effect::Blob(effect)))))
            }
            (bucket, _) => {
                self.checked = bucket.map(|bucket| (bucket, None));
                Ok(self.begin())
            }
        }
    }

    fn ensured(&mut self, event: Event) -> DefaultTurn {
        match event {
            Event::Blob(BlobEvent::BucketReady { created }) => {
                if let Some((_, state)) = self.checked.as_mut() {
                    *state = Some(created);
                }
                Ok(self.begin())
            }
            Event::Blob(BlobEvent::Error(error)) => Err(PutGroupError::Unusable(error)),
            _ => Err(PutGroupError::Unexpected("EnsureBucket")),
        }
    }

    fn begin(&mut self) -> Next<GroupStorageRouting> {
        self.state = DefaultState::Write;
        Next::Call(IoCall::child(self.write()))
    }

    /// The write transaction of the checked bucket and template.
    fn write(&self) -> DefaultWrite {
        DefaultWrite {
            group_id: self.group_id,
            backend_id: self.backend_id,
            template: self.template.clone(),
            updated_by: self.updated_by,
            updated_at: self.updated_at,
            imports: self.imports.clone(),
            checked: self.checked.clone(),
            stage: WriteStage::Verify,
            verified: None,
            pages: None,
            scanned_buckets: 0,
        }
    }
}

/// The stored record of `backend_id` when the group owns it and it can take the default.
fn owned(
    group_id: GroupId,
    backend_id: Ulid,
    record: Option<GroupStorage>,
) -> Result<GroupStorage, PutGroupError> {
    let record = record
        .filter(|record| record.group_id == group_id)
        .ok_or(RoutingError::ForeignBackend(backend_id))?;
    if record.disabled {
        return Err(PutGroupError::Invalid(
            "The storage is disabled.".to_string(),
        ));
    }
    if record.kind == GroupBackendKind::Fs {
        return Err(RoutingError::FolderTarget(backend_id).into());
    }
    Ok(record)
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

    fn first(&mut self) -> DefaultTurn {
        let load = GroupInputsOperation::new(self.group_id);
        Ok(Next::Call(IoCall::child(load)))
    }

    fn next(&mut self, result: IoResult) -> DefaultTurn {
        match self.state {
            DefaultState::LoadInputs => {
                let inputs = result
                    .child::<GroupRoutingInputs, GroupInputsError>()?
                    .map_err(|error| PutGroupError::InputsUnavailable(error.to_string()))?;
                if !inputs.backend_ids.contains(&self.backend_id) {
                    return Err(RoutingError::ForeignBackend(self.backend_id).into());
                }
                self.imports = inputs.imports;
                self.state = DefaultState::ReadBackend;
                Ok(Next::Call(IoCall::Read(vec![(
                    STORAGE_BACKEND_KEYSPACE.to_string(),
                    backend_key(self.backend_id),
                )])))
            }
            DefaultState::ReadBackend => {
                let [value] = values::<1>(result)?;
                self.read_backend(value)
            }
            DefaultState::EnsureBucket => self.ensured(result.event()?),
            DefaultState::Write => result
                .child::<GroupStorageRouting, PutGroupError>()?
                .map(Next::Done),
        }
    }
}

/// What the default write transaction waits for.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WriteStage {
    Verify,
    Scan,
    ScanBuckets,
    Write,
}

/// Reads the storage record again with the group fence, re-checks the links or the group's
/// buckets, and writes the default and the record in one write transaction.
#[derive(Debug, PartialEq)]
struct DefaultWrite {
    group_id: GroupId,
    backend_id: Ulid,
    template: Option<String>,
    updated_by: UserId,
    updated_at: SystemTime,
    imports: Vec<BackendImport>,
    checked: Option<(String, Option<bool>)>,
    stage: WriteStage,
    /// The storage record as the write transaction read it.
    verified: Option<GroupStorage>,
    pages: Option<PrefixPages>,
    scanned_buckets: usize,
}

impl DefaultWrite {
    /// The record is read again with the group fence, so a disable or a deletion that
    /// committed meanwhile wins. The linked rules are read again in the same transaction.
    fn verify(&mut self, record: Option<Value>, fence: Option<Value>) -> DefaultTurn {
        check_group_write(fence.as_deref())?;
        let record = record
            .map(|value| GroupStorage::from_bytes(&value))
            .transpose()?;
        let record = owned(self.group_id, self.backend_id, record)?;
        if !record.splits() && self.checked.is_some() {
            return Err(PutGroupError::Unexpected("Verify"));
        }
        let own_only = record.splits() && self.checked.is_none();
        self.verified = Some(record);
        match (self.checked.is_some(), own_only) {
            (true, _) => {
                let prefix = Key::from(self.group_id.to_bytes().to_vec());
                self.scan(WriteStage::Scan, BACKEND_IMPORT_KEYSPACE, prefix)
            }
            // Without a shared bucket the group's buckets need their own provider bucket, since
            // the default would be their storage too. The scan of this node's buckets is bounded.
            (false, true) => {
                let prefix = Key::from(Vec::new());
                self.scan(WriteStage::ScanBuckets, S3_BUCKET_KEYSPACE, prefix)
            }
            (false, false) => self.write(),
        }
    }

    fn scan(&mut self, stage: WriteStage, space: &str, prefix: Key) -> DefaultTurn {
        let pages = PrefixPages::new(space, prefix, SCAN_PAGE);
        let call = pages.call();
        self.pages = Some(pages);
        self.stage = stage;
        Ok(Next::Call(call))
    }

    fn pages(&mut self) -> Result<&mut PrefixPages, PutGroupError> {
        self.pages.as_mut().ok_or(PutGroupError::Unexpected("Scan"))
    }

    fn buckets_scanned(&mut self, rows: Vec<(Key, Value)>) -> DefaultTurn {
        self.scanned_buckets += rows.len();
        for (key, value) in rows {
            let info = BucketInfo::from_bytes(&value)?;
            let name = String::from_utf8_lossy(&key);
            // Only a 1:1 link keeps plain files, which never need the shared bucket.
            let linked = (self.imports.iter())
                .any(|rule| rule.bucket == name && rule.mapped_bucket().is_some());
            if info.group_id == self.group_id && !linked {
                return Err(PutGroupError::Conflict(format!(
                    "Bucket `{name}` has no own provider bucket and would lose its storage. \
                     Name a shared bucket too."
                )));
            }
        }
        let pages = self.pages()?;
        match (pages.done(), self.scanned_buckets >= BUCKET_SCAN) {
            (true, _) => self.write(),
            (false, true) => Err(PutGroupError::Conflict(
                "Too many buckets to check. Name a shared bucket too.".to_string(),
            )),
            (false, false) => Ok(Next::Call(self.pages()?.call())),
        }
    }

    fn scanned(&mut self, rows: Vec<(Key, Value)>) -> DefaultTurn {
        let (bucket, _) = self
            .checked
            .clone()
            .ok_or(PutGroupError::Unexpected("Scan"))?;
        for (_, value) in rows {
            let rule = BackendImport::from_bytes(&value)?;
            if rule.uses_bucket(self.backend_id, &bucket) {
                return Err(linked_error(&bucket, &rule));
            }
        }
        match self.pages()?.done() {
            true => self.write(),
            false => Ok(Next::Call(self.pages()?.call())),
        }
    }

    fn write(&mut self) -> DefaultTurn {
        let mut record = self
            .verified
            .take()
            .ok_or(PutGroupError::Unexpected("Write"))?;
        let mut writes = vec![(
            STORAGE_ROUTING_KEYSPACE.to_string(),
            routing_key(self.group_id),
            self.routing().to_bytes()?.into(),
        )];
        let template = self.template.clone().filter(|_| record.splits());
        if let Some((bucket, created)) = self.checked.clone() {
            let config = &mut record.public_config;
            config.insert(DEFAULT_BUCKET.to_string(), bucket.clone());
            // Aruna's files stay in a former default bucket, so it stays reserved.
            config.insert(format!("{LAYOUT_BUCKET}{bucket}"), "true".to_string());
            if let Some(created) = created {
                let state = if created { "created" } else { "found" };
                config.insert(format!("{BUCKET_STATE}{bucket}"), state.to_string());
            }
        }
        if let Some(template) = template {
            let config = &mut record.public_config;
            config.remove(BUCKET_PREFIX);
            match template.is_empty() {
                true => config.remove(BUCKET_TEMPLATE),
                false => config.insert(BUCKET_TEMPLATE.to_string(), template),
            };
        }
        if self.checked.is_some() || self.template.is_some() {
            record.updated_at = self.updated_at;
            writes.extend(record_writes(&record)?);
        }
        self.stage = WriteStage::Write;
        Ok(Next::Call(IoCall::Write(writes)))
    }

    fn routing(&self) -> GroupStorageRouting {
        GroupStorageRouting {
            group_id: self.group_id,
            default_target: Some(RoutingTarget::Backend(BackendRef::Group(self.backend_id))),
            updated_at: self.updated_at,
            updated_by: self.updated_by,
        }
    }
}

impl Operation for DefaultWrite {
    type Output = GroupStorageRouting;
    type Error = PutGroupError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> DefaultTurn {
        Ok(Next::Call(IoCall::Read(vec![
            (
                STORAGE_BACKEND_KEYSPACE.to_string(),
                backend_key(self.backend_id),
            ),
            group_fence_key(self.group_id),
        ])))
    }

    fn next(&mut self, result: IoResult) -> DefaultTurn {
        match self.stage {
            WriteStage::Verify => {
                let [record, fence] = values::<2>(result)?;
                self.verify(record, fence)
            }
            WriteStage::Scan => {
                let rows = self.pages()?.take(result)?;
                self.scanned(rows)
            }
            WriteStage::ScanBuckets => {
                let rows = self.pages()?.take(result)?;
                self.buckets_scanned(rows)
            }
            WriteStage::Write => {
                done(result)?;
                Ok(Next::Done(self.routing()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::events::StorageEvent;
    use aruna_core::structs::storage::backend_import::DEFAULT_PATTERN;
    use aruna_core::structs::storage::cleanup::CleanupStrategy;
    use aruna_core::structs::storage::group_backend::GroupBackendKind;
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
    ) -> (PutDefaultOperation, DefaultTurn) {
        read_with((bucket, None), record, imports)
    }

    fn read_with(
        (bucket, template): (Option<&str>, Option<&str>),
        record: GroupStorage,
        imports: Vec<BackendImport>,
    ) -> (PutDefaultOperation, DefaultTurn) {
        let mut operation = PutDefaultOperation::new(
            group(),
            backend(),
            bucket.map(str::to_string),
            UserId::default(),
            SystemTime::UNIX_EPOCH,
        )
        .with_template(template.map(str::to_string));
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let inputs = GroupRoutingInputs {
            backend_ids: BTreeSet::from([backend()]),
            imports,
            ..GroupRoutingInputs::default()
        };
        let loaded = IoResult::Child(ChildOutcome::new(Ok::<_, GroupInputsError>(inputs)));
        assert!(matches!(
            operation.next(loaded),
            Ok(Next::Call(IoCall::Read(_)))
        ));
        let turn = operation.next(IoResult::Values(vec![Some(
            record.to_bytes().unwrap().into(),
        )]));
        (operation, turn)
    }

    fn event(event: Event) -> IoResult {
        IoResult::Event(Box::new(event))
    }

    /// Starts the write transaction and answers its read of the storage record.
    fn verified(
        operation: &PutDefaultOperation,
        record: &GroupStorage,
    ) -> (DefaultWrite, DefaultTurn) {
        let mut write = operation.write();
        assert_eq!(write.transaction(), TransactionMode::Write);
        write.first().unwrap();
        let read = IoResult::Values(vec![Some(record.to_bytes().unwrap().into()), None]);
        let turn = write.next(read);
        (write, turn)
    }

    fn page(rows: Vec<(Key, Value)>) -> IoResult {
        IoResult::Page { rows, next: None }
    }

    /// The writes of a turn, as one write call.
    fn written(turn: DefaultTurn) -> Vec<(String, Key, Value)> {
        match turn {
            Ok(Next::Call(IoCall::Write(writes))) => writes,
            _ => panic!("expected one write"),
        }
    }

    #[test]
    fn refuses_local_folder() {
        // A local folder serves imports only, never Aruna's own layout.
        let mut record = storage(&[("root", "projects")]);
        record.kind = GroupBackendKind::Fs;

        let (_, turn) = read(None, record, Vec::new());

        assert_eq!(
            turn.err(),
            Some(PutGroupError::InvalidTarget(RoutingError::FolderTarget(
                backend()
            )))
        );
    }

    #[test]
    fn writes_default_bucket() {
        let record = storage(&[("endpoint", "https://s3.lab")]);
        let (mut operation, turn) = read(Some("lab-default"), record.clone(), Vec::new());
        let Ok(Next::Call(IoCall::Effect(ensure))) = turn else {
            panic!("expected a bucket check");
        };
        let Effect::Blob(BlobEffect::EnsureBucket { bucket, .. }) = *ensure else {
            panic!("expected a bucket check");
        };
        assert_eq!(bucket, "lab-default");

        let write = operation.next(event(Event::Blob(BlobEvent::BucketReady { created: true })));
        assert!(matches!(write, Ok(Next::Call(IoCall::Child(_)))));
        let (mut write, turn) = verified(&operation, &record);
        assert!(matches!(turn, Ok(Next::Call(IoCall::Page { .. }))));
        let writes = written(write.next(page(Vec::new())));
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
        assert_eq!(write.next(IoResult::Done), Ok(Next::Done(routing)));
    }

    #[test]
    fn rechecks_links_late() {
        // A link saved while the bucket was checked stops the default before it is written.
        let record = storage(&[("endpoint", "https://s3.lab")]);
        let (mut operation, _) = read(Some("lab-raw"), record.clone(), Vec::new());
        operation
            .next(event(Event::Blob(BlobEvent::BucketReady {
                created: false,
            })))
            .unwrap();
        let (mut write, _) = verified(&operation, &record);
        let rule = linked("lab-raw/");
        let rows = vec![(rule.key(), rule.to_bytes().unwrap().into())];
        assert!(matches!(
            write.next(page(rows)),
            Err(PutGroupError::Conflict(_))
        ));
    }

    #[test]
    fn refuses_linked_bucket() {
        let record = storage(&[("endpoint", "https://s3.lab")]);
        let (_, turn) = read(Some("lab-raw"), record, vec![linked("lab-raw/2024/")]);
        assert!(matches!(turn, Err(PutGroupError::Conflict(_))));
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
            let (_, turn) = read(bucket, record, Vec::new());
            assert!(matches!(turn, Err(PutGroupError::Invalid(_))));
        }
    }

    #[test]
    fn stores_bucket_template() {
        // The template replaces a former prefix; the shared bucket stays.
        let record = storage(&[
            ("endpoint", "https://s3.lab"),
            ("default_bucket", "lab-default"),
            ("bucket_prefix", "lab-"),
        ]);
        let template = Some("aruna-{group}-{bucket}");
        let (operation, turn) = read_with((None, template), record.clone(), Vec::new());
        assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
        let (mut write, _) = verified(&operation, &record);
        let writes = written(write.next(page(Vec::new())));
        let stored = GroupStorage::from_bytes(&writes[1].2).unwrap();
        assert_eq!(stored.bucket_template().as_deref(), template);
        assert_eq!(stored.default_bucket(), Some("lab-default"));
        assert!(!stored.public_config.contains_key("bucket_prefix"));

        // An empty template clears it; a bad one is refused before anything is written.
        let (operation, _) = read_with((None, Some("")), stored.clone(), Vec::new());
        let (mut write, _) = verified(&operation, &stored);
        let writes = written(write.next(page(Vec::new())));
        assert_eq!(
            GroupStorage::from_bytes(&writes[1].2)
                .unwrap()
                .bucket_template(),
            None
        );
        for bad in ["aruna-data", "Aruna_{bucket}"] {
            let (_, turn) = read_with((None, Some(bad)), stored.clone(), Vec::new());
            assert!(matches!(turn, Err(PutGroupError::Invalid(_))));
        }
    }

    #[test]
    fn skips_shared_bucket() {
        // Own provider buckets work on a new storage without a shared bucket.
        let record = storage(&[("endpoint", "https://s3.lab")]);
        let template = Some("aruna-{bucket}");
        let bucket = |group_id: Ulid| {
            let info = BucketInfo {
                group_id,
                created_at: SystemTime::UNIX_EPOCH,
                created_by: UserId::default(),
                cors_configuration: None,
                storage_routing: Vec::new(),
                placement_policies: Vec::new(),
                placement_policy_generation: 0,
                compression: Default::default(),
                distribution: None,
            };
            info.to_bytes().unwrap().into()
        };
        let rows = |names: &[(&str, Ulid)]| {
            let rows = names
                .iter()
                .map(|(name, group_id)| (name.as_bytes().to_vec().into(), bucket(*group_id)))
                .collect();
            page(rows)
        };
        // A bucket of the group without its own provider bucket would lose its storage.
        let mut seq = linked("seq/");
        seq.write_pattern = aruna_core::structs::storage::backend_import::PLAIN_PATTERN.to_string();
        let imports = vec![seq];
        let (operation, _) = read_with((None, template), record.clone(), imports.clone());
        let (mut write, turn) = verified(&operation, &record);
        assert!(matches!(
            turn,
            Ok(Next::Call(IoCall::Page { space, .. })) if space == S3_BUCKET_KEYSPACE
        ));
        assert!(matches!(
            write.next(rows(&[("old", group()), ("seq", group())])),
            Err(PutGroupError::Conflict(_))
        ));
        let (operation, turn) = read_with((None, template), record.clone(), imports);
        assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
        let (mut write, _) = verified(&operation, &record);
        let writes = written(write.next(rows(&[("other", Ulid::nil()), ("seq", group())])));
        let stored = GroupStorage::from_bytes(&writes[1].2).unwrap();
        assert_eq!(stored.bucket_template().as_deref(), template);
        assert_eq!(stored.default_bucket(), None);
        // Clearing the template then needs a shared bucket.
        let (_, turn) = read_with((None, Some("")), stored, Vec::new());
        assert!(matches!(turn, Err(PutGroupError::Invalid(_))));
    }

    #[test]
    fn keeps_stored_bucket() {
        // Choosing the storage again needs no new check of its known bucket.
        let record = storage(&[
            ("endpoint", "https://s3.lab"),
            ("default_bucket", "lab-default"),
        ]);
        let (_, turn) = read(None, record, Vec::new());
        assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    }

    #[test]
    fn rejects_stray_event() {
        let record = storage(&[("endpoint", "https://s3.lab")]);
        let (mut operation, _) = read(Some("lab-default"), record, Vec::new());
        let stray = event(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: aruna_core::types::TxnId::from(1),
        }));
        assert!(matches!(
            operation.next(stray),
            Err(PutGroupError::Unexpected(_))
        ));
    }
}
