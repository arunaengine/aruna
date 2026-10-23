//! Creates a bucket record and counts the new bucket in the node usage stats. A mapped create
//! also links the bucket to its own provider bucket on the group default storage.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::groups::backends::backend_key;
use crate::groups::imports::ImportRuleError;
use crate::groups::imports::create::{CreateImportInput, CreateImportOperation};
use crate::groups::storage_routing::routing_key;
use crate::node::usage_stats::{
    RowChange, UsageCounterUpdate, UsageUpdateError, schedule_snapshot_publish,
};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BACKEND_IMPORT_KEYSPACE, BUCKET_CREATION_KEYSPACE, GROUP_KEYSPACE, S3_BUCKET_KEYSPACE,
    STORAGE_BACKEND_KEYSPACE, STORAGE_ROUTING_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::group::Group;
use aruna_core::structs::storage::backend_import::BackendImport;
use aruna_core::structs::storage::backend_import::PLAIN_PATTERN;
use aruna_core::structs::storage::blob::{BackendRef, BucketInfo};
use aruna_core::structs::storage::group_backend::{GroupStorage, template_bucket, valid_bucket};
use aruna_core::structs::storage::routing::{GroupStorageRouting, RoutingTarget};
use aruna_core::structs::storage::usage::UsageDelta;
use aruna_core::types::{Effects, Value};
use smallvec::smallvec;
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CreateBucketState {
    Init,
    StartTransaction,
    CheckExists,
    ReadRouting,
    ReadStorage,
    ScanLinks,
    CreateBucket,
    UpdateUsage,
    Link,
    CommitTransaction,
    Finish,
    Error,
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
    #[error("No transaction found")]
    NoTransactionFound,
    #[error("Invalid state [{current:?}] - expected [{expected:?}]")]
    InvalidState {
        current: CreateBucketState,
        expected: CreateBucketState,
    },
    #[error("State [{state:?}] invalid: expected [{expected:?}] - received [{received:?}]")]
    InvalidStateEvent {
        state: CreateBucketState,
        expected: &'static str,
        received: Event,
    },
    #[error(transparent)]
    UsageUpdateError(#[from] UsageUpdateError),
    #[error("CreateBucket failed")]
    CreateBucketFailed,
    #[error("operation did not finish")]
    NotFinished,
}

#[derive(Debug, PartialEq)]
pub struct CreateBucketOperation {
    bucket: String,
    bucket_info: BucketInfo,
    state: CreateBucketState,
    txn_id: Option<ulid::Ulid>,
    usage_update: Option<UsageCounterUpdate>,
    /// Looks for a provider bucket to link; only user creates opt in, never mirrors.
    mapped: bool,
    link: Option<CreateImportOperation>,
    /// The group storage and provider bucket of the automatic link.
    provider: Option<(ulid::Ulid, String)>,
    output: Option<Result<BucketInfo, CreateBucketError>>,
}

/// Rules one scan page of the automatic link check reads.
const LINK_PAGE: usize = 256;

impl CreateBucketOperation {
    pub fn new(bucket: String, bucket_info: BucketInfo) -> Self {
        Self {
            bucket,
            bucket_info,
            state: CreateBucketState::Init,
            txn_id: None,
            usage_update: None,
            mapped: false,
            link: None,
            provider: None,
            output: None,
        }
    }

    /// Links a new bucket to its own provider bucket when the group default storage sets a
    /// `bucket_template`.
    pub fn mapped(mut self) -> Self {
        self.mapped = true;
        self
    }

    fn emit_error(&mut self, error: CreateBucketError) -> Effects {
        self.state = CreateBucketState::Error;
        self.output = Some(Err(error));
        self.abort()
    }

    fn handle_init(&mut self) -> Effects {
        if self.state != CreateBucketState::Init {
            return self.emit_error(CreateBucketError::InvalidState {
                current: self.state.clone(),
                expected: CreateBucketState::Init,
            });
        }

        self.state = CreateBucketState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn handle_transaction_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.emit_error(CreateBucketError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionStarted)",
                received: event,
            });
        };

        self.txn_id = Some(txn_id);
        self.state = CreateBucketState::CheckExists;
        smallvec![crate::groups::fence::read_group_record(
            self.bucket_info.group_id,
            S3_BUCKET_KEYSPACE,
            self.bucket.as_bytes().into(),
            txn_id,
        )]
    }

    fn handle_bucket_checked(&mut self, event: Event) -> Effects {
        let value = match crate::groups::fence::parse_group_record(event) {
            Ok(value) => value,
            Err(error) => return self.emit_error(error.into()),
        };

        if value.is_some() {
            return self.emit_error(CreateBucketError::BucketAlreadyExists);
        }
        if self.mapped {
            self.state = CreateBucketState::ReadRouting;
            let key = routing_key(self.bucket_info.group_id);
            return self.read(STORAGE_ROUTING_KEYSPACE, key);
        }
        self.write_bucket()
    }

    fn read(&self, key_space: &str, key: aruna_core::types::Key) -> Effects {
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: key_space.to_string(),
            key,
            txn_id: self.txn_id,
        })]
    }

    fn read_value(&mut self, event: Event) -> Result<Option<Value>, CreateBucketError> {
        match event {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value),
            received => Err(CreateBucketError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received,
            }),
        }
    }

    fn routing_read(&mut self, event: Event) -> Effects {
        let routing = self.read_value(event).and_then(|value| {
            let routing = value.map(|value| GroupStorageRouting::from_bytes(&value));
            Ok(routing.transpose()?)
        });
        match routing.map(|routing| routing.and_then(|routing| routing.default_target)) {
            Ok(Some(RoutingTarget::Backend(BackendRef::Group(backend_id)))) => {
                self.state = CreateBucketState::ReadStorage;
                let group = self.bucket_info.group_id.to_bytes().to_vec().into();
                smallvec![Effect::Storage(StorageEffect::BatchRead {
                    reads: vec![
                        (
                            STORAGE_BACKEND_KEYSPACE.to_string(),
                            backend_key(backend_id)
                        ),
                        (GROUP_KEYSPACE.to_string(), group),
                    ],
                    txn_id: self.txn_id,
                })]
            }
            Ok(_) => self.write_bucket(),
            Err(error) => self.emit_error(error),
        }
    }

    fn storage_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.emit_error(CreateBucketError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::BatchReadResult)",
                received: event,
            });
        };
        let mut values = values.into_iter().map(|(_, value)| value);
        let (storage, group) = (values.next().flatten(), values.next().flatten());
        let storage = storage.map(|value| GroupStorage::from_bytes(&value));
        let group = group.map(|value| Group::from_bytes(&value));
        let (storage, group) = match (storage.transpose(), group.transpose()) {
            (Ok(storage), Ok(group)) => (storage, group),
            (Err(error), _) | (_, Err(error)) => return self.emit_error(error.into()),
        };
        let (Some(storage), Some(txn_id)) = (storage, self.txn_id) else {
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
                return self.emit_error(CreateBucketError::ProviderBucket(
                    ImportRuleError::Invalid(reason),
                ));
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
            };
            self.link = Some(CreateImportOperation::within(input, txn_id));
            self.provider = Some((storage.backend_id, provider));
            return self.scan_links(None);
        }
        self.write_bucket()
    }

    /// Reads the group's rules, so no automatic link takes a provider bucket a rule uses.
    fn scan_links(&mut self, start: Option<aruna_core::types::Key>) -> Effects {
        self.state = CreateBucketState::ScanLinks;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: BACKEND_IMPORT_KEYSPACE.to_string(),
            prefix: Some(self.bucket_info.group_id.to_bytes().to_vec().into()),
            start: start.map(aruna_core::effects::IterStart::After),
            limit: LINK_PAGE,
            txn_id: self.txn_id,
        })]
    }

    fn links_scanned(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) = event
        else {
            return self.emit_error(CreateBucketError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::IterResult)",
                received: event,
            });
        };
        let Some((backend_id, provider)) = self.provider.clone() else {
            return self.emit_error(CreateBucketError::CreateBucketFailed);
        };
        for (_, value) in values {
            match BackendImport::from_bytes(&value) {
                Ok(rule) if rule.uses_bucket(backend_id, &provider) => {
                    let reason = format!(
                        "provider bucket `{provider}` is linked to `{}`",
                        rule.bucket
                    );
                    return self.emit_error(CreateBucketError::ProviderBucket(
                        ImportRuleError::Conflict(reason),
                    ));
                }
                Ok(_) => {}
                Err(error) => return self.emit_error(error.into()),
            }
        }
        match next_start_after {
            Some(next) => self.scan_links(Some(next)),
            None => self.write_bucket(),
        }
    }

    fn write_bucket(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(CreateBucketError::NoTransactionFound);
        };
        let bytes = match self.bucket_info.to_bytes() {
            Ok(bytes) => bytes,
            Err(err) => return self.emit_error(err.into()),
        };

        self.state = CreateBucketState::CreateBucket;
        let key: aruna_core::types::Key = self.bucket.as_bytes().into();
        // Every creation, a replicated one too, gets a fresh id for its delete trail.
        let creation = ulid::Ulid::generate().to_bytes().to_vec();
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes: vec![
                (S3_BUCKET_KEYSPACE.to_string(), key.clone(), bytes.into()),
                (BUCKET_CREATION_KEYSPACE.to_string(), key, creation.into()),
            ],
            txn_id: Some(txn_id),
        })]
    }

    fn handle_bucket_created(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchWriteResult { .. }) = event else {
            return self.emit_error(CreateBucketError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::BatchWriteResult)",
                received: event,
            });
        };

        let Some(txn_id) = self.txn_id else {
            return self.emit_error(CreateBucketError::NoTransactionFound);
        };
        let mut update = UsageCounterUpdate::for_group(
            self.bucket_info.group_id,
            UsageDelta {
                buckets: 1,
                ..Default::default()
            },
        )
        .in_bucket(&self.bucket, RowChange::Kept);
        self.state = CreateBucketState::UpdateUsage;
        let effects = update.start(txn_id);
        self.usage_update = Some(update);
        effects
    }

    fn handle_usage_update(&mut self, event: Event) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(CreateBucketError::NoTransactionFound);
        };
        let Some(update) = self.usage_update.as_mut() else {
            return self.emit_error(CreateBucketError::CreateBucketFailed);
        };
        match update.step(event, txn_id) {
            Ok(Some(effects)) => effects,
            Ok(None) => match self.link.as_mut() {
                Some(link) => {
                    self.state = CreateBucketState::Link;
                    let effects = link.start();
                    self.linked(effects)
                }
                None => self.commit(txn_id),
            },
            Err(err) => self.emit_error(err.into()),
        }
    }

    fn commit(&mut self, txn_id: ulid::Ulid) -> Effects {
        self.state = CreateBucketState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    /// Passes the link's effects on until it is done; its failure aborts the whole create.
    fn linked(&mut self, effects: Effects) -> Effects {
        let (Some(link), Some(txn_id)) =
            (self.link.take_if(|link| link.is_complete()), self.txn_id)
        else {
            return effects;
        };
        match link.finalize() {
            Ok(_) => self.commit(txn_id),
            Err(error) => self.emit_error(CreateBucketError::ProviderBucket(error)),
        }
    }

    fn handle_transaction_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.emit_error(CreateBucketError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionCommitted)",
                received: event,
            });
        };

        self.txn_id = None;
        self.state = CreateBucketState::Finish;
        self.output = Some(Ok(self.bucket_info.clone()));
        smallvec![schedule_snapshot_publish()]
    }
}

impl Operation for CreateBucketOperation {
    type Output = BucketInfo;
    type Error = CreateBucketError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = &event {
            return self.emit_error(error.clone().into());
        }
        match self.state {
            CreateBucketState::Init => self.handle_init(),
            CreateBucketState::StartTransaction => self.handle_transaction_started(event),
            CreateBucketState::CheckExists => self.handle_bucket_checked(event),
            CreateBucketState::ReadRouting => self.routing_read(event),
            CreateBucketState::ReadStorage => self.storage_read(event),
            CreateBucketState::ScanLinks => self.links_scanned(event),
            CreateBucketState::CreateBucket => self.handle_bucket_created(event),
            CreateBucketState::UpdateUsage => self.handle_usage_update(event),
            CreateBucketState::Link => match self.link.as_mut() {
                Some(link) => {
                    let effects = link.step(event);
                    self.linked(effects)
                }
                None => self.emit_error(CreateBucketError::CreateBucketFailed),
            },
            CreateBucketState::CommitTransaction => self.handle_transaction_committed(event),
            CreateBucketState::Finish => smallvec![],
            CreateBucketState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            CreateBucketState::Finish | CreateBucketState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.output {
            Some(Ok(value)) => Ok(value),
            Some(Err(error)) => Err(error),
            None => Err(CreateBucketError::NotFinished),
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
mod mapped_tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::effects::BlobEffect;
    use aruna_core::errors::BlobError;
    use aruna_core::events::BlobEvent;
    use aruna_core::keyspaces::BACKEND_IMPORT_KEYSPACE;
    use aruna_core::structs::storage::backend_import::BackendImport;
    use aruna_core::structs::storage::cleanup::CleanupStrategy;
    use aruna_core::structs::storage::format::Compression;
    use aruna_core::structs::storage::group_backend::GroupBackendKind;
    use aruna_core::types::{Key, TxnId};
    use std::collections::HashMap;
    use std::time::SystemTime;
    use ulid::Ulid;

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

    fn read(value: Option<Value>) -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: Key::from(Vec::new()),
            value,
        })
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
    fn until_ensure(txn_id: TxnId) -> (CreateBucketOperation, Effects) {
        until_ensure_on(txn_id, storage())
    }

    fn until_ensure_on(txn_id: TxnId, storage: Value) -> (CreateBucketOperation, Effects) {
        let mut operation = CreateBucketOperation::new("x".to_string(), info()).mapped();
        operation.start();
        operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        operation.step(batch(vec![None, None]));
        operation.step(read(Some(routing())));
        operation.step(batch(vec![Some(storage.clone()), Some(group())]));
        operation.step(Event::Storage(StorageEvent::IterResult {
            values: Vec::new(),
            next_start_after: None,
        }));
        operation.step(written());
        operation.step(written());
        operation.step(written());
        let effects = operation.step(batch(vec![Some(storage.clone()), Some(storage)]));
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
        operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id,
        }));
        assert_eq!(operation.finalize(), Ok(info()));
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
        let mut operation = CreateBucketOperation::new("x".to_string(), info()).mapped();
        operation.start();
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TxnId::default(),
        }));
        operation.step(batch(vec![None, None]));
        operation.step(read(Some(routing())));
        let odd = postcard::to_allocvec(&odd).unwrap().into();
        operation.step(batch(vec![Some(template.clone()), Some(odd)]));
        assert!(matches!(
            operation.finalize(),
            Err(CreateBucketError::ProviderBucket(ImportRuleError::Invalid(
                _
            )))
        ));

        // A provider bucket another rule uses is never linked again.
        let mut operation = CreateBucketOperation::new("x".to_string(), info()).mapped();
        operation.start();
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: TxnId::default(),
        }));
        operation.step(batch(vec![None, None]));
        operation.step(read(Some(routing())));
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
            operation.finalize(),
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
            operation.finalize(),
            Err(CreateBucketError::ProviderBucket(ImportRuleError::Invalid(
                _
            )))
        ));
    }

    #[test]
    fn mirrors_skip_mapping() {
        let mut operation = CreateBucketOperation::new("x".to_string(), info());
        operation.start();
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
            task_handle: None,
            compute_handle: None,
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
            task_handle: None,
            compute_handle: None,
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
            task_handle: None,
            compute_handle: None,
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
}
