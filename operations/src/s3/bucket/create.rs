//! Creates a bucket record and counts the new bucket in the node usage stats. A mapped create
//! also links the bucket to its own provider bucket on the group default storage.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::groups::backends::backend_key;
use crate::groups::fence::group_fence_key;
use crate::groups::storage_routing::routing_key;
use crate::s3::imports::ImportRuleError;
use crate::s3::imports::create::{CreateImportInput, CreateImportOperation};
use crate::s3::usage::{
    RowChange, UsageCounterUpdate, UsageUpdateError, schedule_snapshot_publish,
};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::{
    BACKEND_IMPORT_KEYSPACE, BUCKET_CREATION_KEYSPACE, GROUP_KEYSPACE, S3_BUCKET_KEYSPACE,
    STORAGE_BACKEND_KEYSPACE, STORAGE_ROUTING_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::read_row;
use aruna_core::structs::identity::group::Group;
use aruna_core::structs::identity::group_delete::check_group_write;
use aruna_core::structs::storage::backend_import::BackendImport;
use aruna_core::structs::storage::backend_import::PLAIN_PATTERN;
use aruna_core::structs::storage::blob::{BackendRef, BucketInfo};
use aruna_core::structs::storage::group_backend::{GroupStorage, template_bucket, valid_bucket};
use aruna_core::structs::storage::routing::{GroupStorageRouting, RoutingTarget};
use aruna_core::structs::storage::usage::UsageDelta;
use aruna_core::types::{Key, Value};
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CreateBucketState {
    CheckExists,
    ReadRouting,
    ReadStorage,
    ScanLinks,
    CreateBucket,
    UpdateUsage,
    Link,
}

#[derive(Debug, Error, PartialEq)]
pub enum CreateBucketError {
    #[error(transparent)]
    GroupWrite(#[from] aruna_core::structs::identity::group_delete::GroupWriteError),
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("Bucket already exists")]
    BucketAlreadyExists,
    #[error("the provider bucket could not be linked: {0}")]
    ProviderBucket(ImportRuleError),
    #[error(transparent)]
    UsageUpdateError(#[from] UsageUpdateError),
    #[error("CreateBucket failed")]
    CreateBucketFailed,
}

impl From<IoError> for CreateBucketError {
    fn from(error: IoError) -> Self {
        Self::StorageError(error.into())
    }
}

type Turn = Result<Next<BucketInfo>, CreateBucketError>;

#[derive(Debug, PartialEq)]
pub struct CreateBucketOperation {
    bucket: String,
    bucket_info: BucketInfo,
    state: CreateBucketState,
    usage_update: Option<UsageCounterUpdate>,
    /// Looks for a provider bucket to link; only user creates opt in, never mirrors.
    mapped: bool,
    /// The automatic link, whose calls join this transaction.
    link: Option<CreateImportOperation>,
    /// The group storage and provider bucket of the automatic link.
    provider: Option<(ulid::Ulid, String)>,
}

/// Rules one scan page of the automatic link check reads.
const LINK_PAGE: usize = 256;

impl CreateBucketOperation {
    pub fn new(bucket: String, bucket_info: BucketInfo) -> Self {
        Self {
            bucket,
            bucket_info,
            state: CreateBucketState::CheckExists,
            usage_update: None,
            mapped: false,
            link: None,
            provider: None,
        }
    }

    /// Links a new bucket to its own provider bucket when the group default storage sets a
    /// `bucket_template`.
    pub fn mapped(mut self) -> Self {
        self.mapped = true;
        self
    }

    fn handle_bucket_checked(&mut self, values: Vec<Option<Value>>) -> Turn {
        let [value, fence] = <[_; 2]>::try_from(values).map_err(|_| {
            aruna_core::structs::identity::group_delete::GroupWriteError::Unexpected
        })?;
        check_group_write(fence.as_deref())?;
        if value.is_some() {
            return Err(CreateBucketError::BucketAlreadyExists);
        }
        if self.mapped {
            self.state = CreateBucketState::ReadRouting;
            let key = routing_key(self.bucket_info.group_id);
            return Ok(Next::Call(read_row(STORAGE_ROUTING_KEYSPACE, key)));
        }
        self.write_bucket()
    }

    fn routing_read(&mut self, value: Option<Value>) -> Turn {
        let routing = value
            .map(|value| GroupStorageRouting::from_bytes(&value))
            .transpose()?;
        match routing.and_then(|routing| routing.default_target) {
            Some(RoutingTarget::Backend(BackendRef::Group(backend_id))) => {
                self.state = CreateBucketState::ReadStorage;
                let group = self.bucket_info.group_id.to_bytes().to_vec().into();
                Ok(Next::Call(IoCall::Read(vec![
                    (
                        STORAGE_BACKEND_KEYSPACE.to_string(),
                        backend_key(backend_id),
                    ),
                    (GROUP_KEYSPACE.to_string(), group),
                ])))
            }
            _ => self.write_bucket(),
        }
    }

    fn storage_read(&mut self, values: Vec<Option<Value>>) -> Turn {
        let mut values = values.into_iter();
        let (storage, group) = (values.next().flatten(), values.next().flatten());
        let storage = storage.map(|value| GroupStorage::from_bytes(&value));
        let group = group.map(|value| Group::from_bytes(&value));
        let (Some(storage), group) = (storage.transpose()?, group.transpose()?) else {
            return self.write_bucket();
        };
        let name = group.map(|group| group.display_name).unwrap_or_default();
        if let (Some(template), false, true) = (
            storage.bucket_template(),
            storage.disabled,
            storage.group_id == self.bucket_info.group_id,
        ) {
            let provider = template_bucket(&template, &self.bucket, &name);
            // The whole name must be one S3 bucket; a group name with `/` or `_` is refused.
            if !valid_bucket(&provider) {
                let reason = format!("the bucket template gives `{provider}`, no S3 bucket name");
                return Err(CreateBucketError::ProviderBucket(ImportRuleError::Invalid(
                    reason,
                )));
            }
            let input = CreateImportInput {
                group_id: storage.group_id,
                created_by: self.bucket_info.created_by,
                backend_id: storage.backend_id,
                backend_prefix: format!("{provider}/"),
                bucket: self.bucket.clone(),
                key_prefix: String::new(),
                schedule: None,
                write_backend_id: None,
                write_prefix: None,
                write_pattern: PLAIN_PATTERN.to_string(),
                now: self.bucket_info.created_at,
                move_existing: false,
                import_roots: Default::default(),
            };
            self.link = Some(CreateImportOperation::within(input));
            self.provider = Some((storage.backend_id, provider));
            return self.scan_links(None);
        }
        self.write_bucket()
    }

    /// Reads the group's rules, so no automatic link takes a provider bucket a rule uses.
    fn scan_links(&mut self, start: Option<Key>) -> Turn {
        self.state = CreateBucketState::ScanLinks;
        Ok(Next::Call(IoCall::Page {
            space: BACKEND_IMPORT_KEYSPACE.to_string(),
            prefix: self.bucket_info.group_id.to_bytes().to_vec().into(),
            after: start,
            limit: LINK_PAGE,
        }))
    }

    fn links_scanned(&mut self, values: Vec<(Key, Value)>, next: Option<Key>) -> Turn {
        let (backend_id, provider) = self
            .provider
            .clone()
            .ok_or(CreateBucketError::CreateBucketFailed)?;
        for (_, value) in values {
            let rule = BackendImport::from_bytes(&value)?;
            if rule.uses_bucket(backend_id, &provider) {
                let reason = format!(
                    "provider bucket `{provider}` is linked to `{}`",
                    rule.bucket
                );
                return Err(CreateBucketError::ProviderBucket(
                    ImportRuleError::Conflict(reason),
                ));
            }
        }
        match next {
            Some(next) => self.scan_links(Some(next)),
            None => self.write_bucket(),
        }
    }

    fn write_bucket(&mut self) -> Turn {
        // Every creation, a replicated one too, gets a fresh id for its delete trail.
        let creation = ulid::Ulid::generate().to_bytes();
        if let Some(binding) = self.bucket_info.distribution.as_mut() {
            // A member binds this exact incarnation, so a later bucket of the name never passes.
            binding.incarnation = u128::from_be_bytes(creation);
        }
        let bytes = self.bucket_info.to_bytes()?;
        self.state = CreateBucketState::CreateBucket;
        let key: Key = self.bucket.as_bytes().into();
        let creation = creation.to_vec();
        Ok(Next::Call(IoCall::Write(vec![
            (S3_BUCKET_KEYSPACE.to_string(), key.clone(), bytes.into()),
            (BUCKET_CREATION_KEYSPACE.to_string(), key, creation.into()),
        ])))
    }

    fn handle_bucket_created(&mut self) -> Turn {
        let mut update = UsageCounterUpdate::for_group(
            self.bucket_info.group_id,
            UsageDelta {
                buckets: 1,
                ..Default::default()
            },
        )
        .in_bucket(&self.bucket, RowChange::Kept);
        self.state = CreateBucketState::UpdateUsage;
        let call = update.start_calls().into_call();
        self.usage_update = Some(update);
        Ok(Next::Call(call))
    }

    fn handle_usage_update(&mut self, result: IoResult) -> Turn {
        let update = self.usage_update.as_mut();
        let update = update.ok_or(CreateBucketError::CreateBucketFailed)?;
        if let Some(call) = update.step_calls(result)? {
            return Ok(Next::Call(call.into_call()));
        }
        match self.link.as_mut() {
            Some(link) => {
                self.state = CreateBucketState::Link;
                let turn = link.first();
                self.linked(turn)
            }
            None => self.created(Vec::new()),
        }
    }

    /// Makes the link's calls in this transaction until it is done; its failure aborts the
    /// whole create.
    fn linked(&mut self, turn: Result<Next<BackendImport>, ImportRuleError>) -> Turn {
        match turn.map_err(CreateBucketError::ProviderBucket)? {
            Next::Call(call) => Ok(Next::Call(call)),
            Next::Wait => Ok(Next::Wait),
            Next::Done(_) => self.created(Vec::new()),
            Next::Then { calls, .. } => self.created(calls),
        }
    }

    fn created(&mut self, mut calls: Vec<IoCall>) -> Turn {
        calls.push(schedule_snapshot_publish().into());
        Ok(Next::Then {
            output: self.bucket_info.clone(),
            calls,
        })
    }
}

impl Operation for CreateBucketOperation {
    type Output = BucketInfo;
    type Error = CreateBucketError;

    fn first(&mut self) -> Turn {
        self.state = CreateBucketState::CheckExists;
        let key: Key = self.bucket.as_bytes().into();
        Ok(Next::Call(IoCall::Read(vec![
            (S3_BUCKET_KEYSPACE.to_string(), key),
            group_fence_key(self.bucket_info.group_id),
        ])))
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match self.state {
            CreateBucketState::CheckExists => self.handle_bucket_checked(result.values()?),
            CreateBucketState::ReadRouting => self.routing_read(result.value()?),
            CreateBucketState::ReadStorage => self.storage_read(result.values()?),
            CreateBucketState::ScanLinks => {
                let (rows, next) = result.page()?;
                self.links_scanned(rows, next)
            }
            CreateBucketState::CreateBucket => {
                result.done()?;
                self.handle_bucket_created()
            }
            CreateBucketState::UpdateUsage => self.handle_usage_update(result),
            CreateBucketState::Link => {
                let link = self.link.as_mut();
                let link = link.ok_or(CreateBucketError::CreateBucketFailed)?;
                let turn = link.next(result);
                self.linked(turn)
            }
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

#[cfg(test)]
mod mapped_tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::effects::BlobEffect;
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::errors::BlobError;
    use aruna_core::events::{BlobEvent, Event, StorageEvent};
    use aruna_core::keyspaces::BACKEND_IMPORT_KEYSPACE;
    use aruna_core::structs::storage::backend_import::BackendImport;
    use aruna_core::structs::storage::cleanup::CleanupStrategy;
    use aruna_core::structs::storage::format::Compression;
    use aruna_core::structs::storage::group_backend::GroupBackendKind;
    use aruna_core::task::{TaskEvent, TaskKey};
    use aruna_core::types::{Effects, TxnId};
    use std::collections::HashMap;
    use std::time::SystemTime;
    use ulid::Ulid;

    type Stepper = crate::stepper::Stepper<BucketInfo, CreateBucketError>;

    const GROUP: Ulid = Ulid::from_parts(1, 1);
    const BACKEND: Ulid = Ulid::from_parts(2, 2);

    fn info() -> BucketInfo {
        BucketInfo {
            group_id: GROUP,
            created_at: SystemTime::UNIX_EPOCH,
            created_by: UserId::default(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: None,
        }
    }

    /// A storage without a bucket; a former `bucket_prefix` still names the provider bucket.
    fn storage() -> Value {
        storage_with(("bucket_prefix", "team-a-"))
    }

    fn storage_with(naming: (&str, &str)) -> Value {
        let public = [("endpoint", "https://s3.example.test"), naming];
        let record = GroupStorage {
            backend_id: BACKEND,
            group_id: GROUP,
            name: "tenant".to_string(),
            kind: GroupBackendKind::S3,
            public_config: public
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect::<HashMap<_, _>>(),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            created_by: UserId::default(),
            disabled: false,
            cleanup: CleanupStrategy::Retain,
        };
        record.to_bytes().unwrap().into()
    }

    fn routing() -> Value {
        let routing = GroupStorageRouting {
            group_id: GROUP,
            default_target: Some(RoutingTarget::Backend(BackendRef::Group(BACKEND))),
            updated_at: SystemTime::UNIX_EPOCH,
            updated_by: UserId::default(),
        };
        routing.to_bytes().unwrap().into()
    }

    fn batch(values: Vec<Option<Value>>) -> Event {
        let values = values
            .into_iter()
            .map(|v| (Key::from(Vec::new()), v))
            .collect();
        Event::Storage(StorageEvent::BatchReadResult { values })
    }

    fn written() -> Event {
        Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        })
    }

    fn group() -> Value {
        let group = Group {
            display_name: "Spire".to_string(),
            group_id: GROUP,
            realm_id: aruna_core::structs::identity::realm::RealmId([1u8; 32]),
            roles: Default::default(),
            owner: UserId::default(),
        };
        postcard::to_allocvec(&group).unwrap().into()
    }

    /// Runs a mapped create up to the ensure of the provider bucket.
    fn until_ensure(txn_id: TxnId) -> (Stepper, Effects) {
        until_ensure_on(txn_id, storage())
    }

    fn until_ensure_on(txn_id: TxnId, storage: Value) -> (Stepper, Effects) {
        let (mut operation, _) =
            Stepper::new(CreateBucketOperation::new("x".to_string(), info()).mapped());
        operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        operation.step(batch(vec![None, None]));
        operation.step(batch(vec![Some(routing())]));
        operation.step(batch(vec![Some(storage.clone()), Some(group())]));
        operation.step(Event::Storage(StorageEvent::IterResult {
            values: Vec::new(),
            next_start_after: None,
        }));
        operation.step(written());
        operation.step(written());
        operation.step(written());
        let bucket: Value = info().to_bytes().unwrap().into();
        let reads = vec![
            Some(bucket),
            None,
            Some(storage.clone()),
            None,
            Some(storage),
        ];
        operation.step(batch(reads));
        let effects = operation.step(Event::Storage(StorageEvent::IterResult {
            values: Vec::new(),
            next_start_after: None,
        }));
        (operation, effects)
    }

    #[test]
    fn links_provider_bucket() {
        let txn_id = TxnId::default();
        let (mut operation, effects) = until_ensure(txn_id);
        assert_eq!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::EnsureBucket {
                backend: BackendRef::Group(BACKEND),
                bucket: "team-a-x".to_string(),
            })]
        );
        operation.step(Event::Blob(BlobEvent::BucketReady { created: true }));
        let bucket: Value = info().to_bytes().unwrap().into();
        operation.step(batch(vec![
            Some(bucket),
            None,
            Some(storage()),
            None,
            Some(storage()),
        ]));
        let effects = operation.step(Event::Storage(StorageEvent::IterResult {
            values: Vec::new(),
            next_start_after: None,
        }));
        let [
            Effect::Storage(StorageEffect::BatchWrite {
                writes,
                txn_id: Some(_),
            }),
        ] = effects.as_slice()
        else {
            panic!("expected the link writes, got {effects:?}");
        };
        let rule = writes
            .iter()
            .find(|(space, ..)| space == BACKEND_IMPORT_KEYSPACE)
            .map(|(_, _, value)| BackendImport::from_bytes(value).unwrap())
            .unwrap();
        assert_eq!(rule.mapped_bucket(), Some("team-a-x"));
        assert_eq!((rule.bucket.as_str(), rule.schedule), ("x", None));
        let record = writes
            .iter()
            .find(|(space, ..)| space == STORAGE_BACKEND_KEYSPACE)
            .map(|(_, _, value)| GroupStorage::from_bytes(value).unwrap())
            .unwrap();
        assert_eq!(
            record
                .public_config
                .get("bucket:team-a-x")
                .map(String::as_str),
            Some("created")
        );
        // The link writes join the bucket's own transaction, which commits last.
        let effects = operation.step(written());
        assert_eq!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
        );
        let effects = operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id,
        }));
        assert_eq!(effects.as_slice(), [schedule_snapshot_publish()]);
        operation.step(Event::Task(TaskEvent::TimerScheduled {
            key: TaskKey::PublishUsageSnapshots,
            after: std::time::Duration::ZERO,
        }));
        assert_eq!(operation.finish(), Ok(info()));
    }

    #[test]
    fn fills_bucket_template() {
        let storage = storage_with(("bucket_template", "aruna-{group}-{bucket}"));
        let (_, effects) = until_ensure_on(TxnId::default(), storage);
        assert_eq!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::EnsureBucket {
                backend: BackendRef::Group(BACKEND),
                bucket: "aruna-spire-x".to_string(),
            })]
        );
    }

    #[test]
    fn refuses_bad_providers() {
        // A group name that breaks the bucket name is refused, not split into a folder.
        let template = storage_with(("bucket_template", "aruna-{group}-{bucket}"));
        let mut odd = Group::from_bytes(&group()).unwrap();
        odd.display_name = "lab/sub".to_string();
        let (mut operation, _) =
            Stepper::new(CreateBucketOperation::new("x".to_string(), info()).mapped());
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TxnId::default(),
        }));
        operation.step(batch(vec![None, None]));
        operation.step(batch(vec![Some(routing())]));
        let odd = postcard::to_allocvec(&odd).unwrap().into();
        operation.step(batch(vec![Some(template.clone()), Some(odd)]));
        assert!(matches!(
            operation.finish(),
            Err(CreateBucketError::ProviderBucket(ImportRuleError::Invalid(
                _
            )))
        ));

        // A provider bucket another rule uses is never linked again.
        let (mut operation, _) =
            Stepper::new(CreateBucketOperation::new("x".to_string(), info()).mapped());
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TxnId::default(),
        }));
        operation.step(batch(vec![None, None]));
        operation.step(batch(vec![Some(routing())]));
        operation.step(batch(vec![Some(template), Some(group())]));
        let rule = BackendImport {
            import_id: Ulid::from_parts(5, 5),
            group_id: GROUP,
            backend_id: BACKEND,
            backend_prefix: "aruna-spire-x/".to_string(),
            bucket: "other".to_string(),
            key_prefix: String::new(),
            schedule: None,
            created_by: UserId::default(),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            last_job: None,
            last_started_ms: None,
            write_backend_id: None,
            write_prefix: None,
            write_pattern: PLAIN_PATTERN.to_string(),
        };
        operation.step(Event::Storage(StorageEvent::IterResult {
            values: vec![(rule.key(), rule.to_bytes().unwrap().into())],
            next_start_after: None,
        }));
        assert!(matches!(
            operation.finish(),
            Err(CreateBucketError::ProviderBucket(
                ImportRuleError::Conflict(_)
            ))
        ));
    }

    #[test]
    fn ensure_failure_aborts() {
        let txn_id = TxnId::default();
        let (mut operation, _) = until_ensure(txn_id);
        let refused = BlobError::WriteError("No access to bucket `team-a-x`.".to_string());
        let effects = operation.step(Event::Blob(BlobEvent::Error(refused)));
        assert_eq!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
        );
        assert!(operation.is_complete());
        assert!(matches!(
            operation.finish(),
            Err(CreateBucketError::ProviderBucket(ImportRuleError::Invalid(
                _
            )))
        ));
    }

    #[test]
    fn mirrors_skip_mapping() {
        let (mut operation, _) = Stepper::new(CreateBucketOperation::new("x".to_string(), info()));
        let txn_id = TxnId::default();
        operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        let effects = operation.step(batch(vec![None, None]));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::BatchWrite { writes, .. })]
                if writes[0].0 == S3_BUCKET_KEYSPACE
        ));
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::structs::storage::format::Compression;
    use aruna_storage::storage;
    use std::time::SystemTime;
    use tempfile::tempdir;
    use ulid::Ulid;

    #[tokio::test]
    async fn test_create_bucket() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };

        let bucket_info = BucketInfo {
            group_id: Ulid::generate(),
            created_at: SystemTime::now(),
            created_by: Default::default(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: None,
        };

        let result = drive(
            CreateBucketOperation::new("bucket-a".to_string(), bucket_info.clone()),
            &driver_ctx,
        )
        .await
        .unwrap();

        assert_eq!(result, bucket_info);
    }

    #[tokio::test]
    async fn duplicate_bucket_rejected() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };

        let bucket_info = BucketInfo {
            group_id: Ulid::generate(),
            created_at: SystemTime::now(),
            created_by: Default::default(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: None,
        };

        drive(
            CreateBucketOperation::new("bucket-a".to_string(), bucket_info.clone()),
            &driver_ctx,
        )
        .await
        .unwrap();

        let result = drive(
            CreateBucketOperation::new("bucket-a".to_string(), bucket_info),
            &driver_ctx,
        )
        .await;

        assert!(matches!(
            result,
            Err(CreateBucketError::BucketAlreadyExists)
        ));
    }

    #[tokio::test]
    async fn recreation_gets_incarnation() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };
        // A replicated recreation carries the same record, creation time included.
        let bucket_info = BucketInfo {
            group_id: Ulid::generate(),
            created_at: SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: None,
        };
        let storage = &driver_ctx.storage_handle;
        let mut seen = Vec::new();
        for _ in 0..2 {
            let create = CreateBucketOperation::new("bucket-a".to_string(), bucket_info.clone());
            drive(create, &driver_ctx).await.unwrap();
            let read = StorageEffect::Read {
                key_space: BUCKET_CREATION_KEYSPACE.to_string(),
                key: b"bucket-a".to_vec().into(),
                txn_id: None,
            };
            let Event::Storage(StorageEvent::ReadResult { value, .. }) =
                storage.send_storage_effect(read).await
            else {
                panic!("expected the creation row")
            };
            let value = value.expect("creation row written");
            seen.push(aruna_core::structs::storage::blob::incarnation(Some(
                &value,
            )));
            let delete = StorageEffect::Delete {
                key_space: S3_BUCKET_KEYSPACE.to_string(),
                key: b"bucket-a".to_vec().into(),
                txn_id: None,
            };
            storage.send_storage_effect(delete).await;
        }
        assert_ne!(seen[0], seen[1]);
        assert_ne!(seen[0], 0);
    }

    #[tokio::test]
    async fn member_binds_incarnation() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };
        let binding = aruna_core::structs::storage::distribution::DistributionBinding {
            set_id: Ulid::generate(),
            member_id: Ulid::generate(),
            incarnation: 0,
            revision: Default::default(),
        };
        let bucket_info = BucketInfo {
            group_id: Ulid::generate(),
            created_at: SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: Some(binding),
        };
        let create = CreateBucketOperation::new("bucket-a".to_string(), bucket_info);
        let created = drive(create, &driver_ctx).await.unwrap();
        let read = StorageEffect::Read {
            key_space: BUCKET_CREATION_KEYSPACE.to_string(),
            key: b"bucket-a".to_vec().into(),
            txn_id: None,
        };
        let Event::Storage(StorageEvent::ReadResult { value, .. }) =
            driver_ctx.storage_handle.send_storage_effect(read).await
        else {
            panic!("expected the creation row")
        };
        let local = aruna_core::structs::storage::blob::incarnation(value.as_deref());
        let bound = created.distribution.expect("binding kept").incarnation;
        assert_eq!(bound, local);
        assert_ne!(bound, 0);
    }
}
