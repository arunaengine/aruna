//! Tells where the buckets of a group store new uploads, one page of buckets at a time.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::groups::backends::query::{GetBackendOperation, GroupQueryError};
use crate::groups::storage_routing::{GroupInputsError, GroupInputsOperation};
use crate::s3::bucket::bucket_jobs::{ActiveJob, ActiveJobError, ActiveJobOperation};
use crate::s3::bucket::list::{
    ListBucketsError, ListBucketsInput, ListBucketsOperation, ListBucketsResult,
};
use crate::s3::imports::{ImportRuleError, ListImportsOperation};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::ConversionError;
use aruna_core::keyspaces::BUCKET_ENCRYPTION_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::storage::backend_import::BackendImport;
use aruna_core::structs::storage::blob::{BackendRef, BucketInfo};
use aruna_core::structs::storage::encryption::BucketEncryption;
use aruna_core::structs::storage::format::Compression;
use aruna_core::structs::storage::group_backend::{GroupStorage, path_bucket};
use aruna_core::structs::storage::routing::{
    GroupRoutingInputs, NodeRouting, RoutingSnapshot, resolve_backend,
};
use aruna_core::types::{GroupId, Value};
use std::collections::{HashMap, VecDeque};
use thiserror::Error;
use ulid::Ulid;

/// Where one bucket stores new uploads, and the job that runs or is paused on it.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BucketStorage {
    pub bucket: String,
    /// The group storage; `None` is Aruna storage on this node.
    pub backend_id: Option<Ulid>,
    pub provider_bucket: Option<String>,
    /// The storage folder of a whole-bucket link.
    pub folder: Option<String>,
    /// Aruna names the files, in the storage's shared bucket or fixed container.
    pub shared: bool,
    pub import_id: Option<Ulid>,
    pub write_pattern: Option<String>,
    pub job: Option<ActiveJob>,
}

/// Links and mapping rows of a group the listing reads at most.
const LINK_CAP: usize = 10_000;

#[derive(Debug, Error, PartialEq)]
pub enum BucketStorageError {
    #[error(transparent)]
    Io(#[from] IoError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Buckets(#[from] ListBucketsError),
    #[error(transparent)]
    Inputs(#[from] GroupInputsError),
    #[error(transparent)]
    Imports(#[from] ImportRuleError),
    #[error(transparent)]
    Backend(#[from] GroupQueryError),
    #[error(transparent)]
    Job(#[from] ActiveJobError),
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum StorageStep {
    Buckets,
    Inputs,
    Encryption,
    Imports,
    Backend(Ulid),
    Job,
}

/// One bucket of the page, on its way through the steps.
#[derive(Debug, PartialEq)]
struct PageBucket {
    name: String,
    info: BucketInfo,
    encrypted: bool,
}

/// One page of buckets and the cursor of the next.
pub type BucketStoragePage = (Vec<BucketStorage>, Option<String>);

type StorageTurn = Result<Next<BucketStoragePage>, BucketStorageError>;

/// One page of the group's buckets with their storage and job, and the next page's cursor. The
/// group's routing inputs load once, as for one upload; each bucket resolves like an upload.
#[derive(Debug, PartialEq)]
pub struct ListBucketStorageOperation {
    group_id: GroupId,
    after: Option<String>,
    limit: usize,
    routing: NodeRouting,
    step: StorageStep,
    names: Vec<(String, BucketInfo)>,
    token: Option<String>,
    inputs: Option<GroupRoutingInputs>,
    snapshot: Option<RoutingSnapshot>,
    rules: Vec<BackendImport>,
    buckets: VecDeque<PageBucket>,
    /// The bucket whose job is being read, with where it stores uploads.
    current: Option<BucketStorage>,
    storages: HashMap<Ulid, Option<GroupStorage>>,
    found: Vec<BucketStorage>,
}

impl ListBucketStorageOperation {
    pub fn new(
        routing: NodeRouting,
        group_id: GroupId,
        (after, limit): (Option<String>, usize),
    ) -> Self {
        Self {
            group_id,
            after,
            limit,
            routing,
            step: StorageStep::Buckets,
            names: Vec::new(),
            token: None,
            inputs: None,
            snapshot: None,
            rules: Vec::new(),
            buckets: VecDeque::new(),
            current: None,
            storages: HashMap::new(),
            found: Vec::new(),
        }
    }

    fn call(&mut self, step: StorageStep, call: IoCall) -> StorageTurn {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn listed(&mut self, page: ListBucketsResult) -> StorageTurn {
        self.names = page.buckets;
        self.token = page.continuation_token;
        let inputs = GroupInputsOperation::new(self.group_id).capped(LINK_CAP);
        self.call(StorageStep::Inputs, IoCall::child(inputs))
    }

    /// Whether each bucket of the page encrypts its new uploads.
    fn read_encryption(&mut self) -> StorageTurn {
        if self.names.is_empty() {
            return self.encryption_read(Vec::new());
        }
        let reads = (self.names.iter())
            .map(|(bucket, _)| {
                let key = bucket.as_bytes().to_vec().into();
                (BUCKET_ENCRYPTION_KEYSPACE.to_string(), key)
            })
            .collect();
        self.call(StorageStep::Encryption, IoCall::Read(reads))
    }

    fn encryption_read(&mut self, rows: Vec<Option<Value>>) -> StorageTurn {
        let names = std::mem::take(&mut self.names);
        for ((name, info), row) in names.into_iter().zip(rows) {
            let encrypted = BucketEncryption::from_row(row.as_deref())?.is_encrypted();
            self.buckets.push_back(PageBucket {
                name,
                info,
                encrypted,
            });
        }
        // Jobs show for every rule, also one whose write storage routing leaves out.
        let imports = ListImportsOperation::new(self.group_id);
        self.call(StorageStep::Imports, IoCall::child(imports))
    }

    fn imports_read(&mut self, rules: Vec<BackendImport>) -> StorageTurn {
        self.rules = rules;
        let inputs = self.inputs.take().unwrap_or_default();
        let snapshot = self.routing.snapshot(self.group_id);
        self.snapshot = Some(snapshot.with_group_inputs(inputs));
        self.next_bucket()
    }

    fn next_bucket(&mut self) -> StorageTurn {
        let Some(bucket) = self.buckets.pop_front() else {
            let found = std::mem::take(&mut self.found);
            return Ok(Next::Done((found, self.token.take())));
        };
        let snapshot = self.snapshot.as_mut().ok_or(IoError::Unexpected {
            expected: "the routing snapshot",
            got: "none",
        })?;
        snapshot.bucket_rules = bucket.info.storage_routing;
        snapshot.compression = bucket.info.compression;
        let encoded = bucket.encrypted || bucket.info.compression != Compression::Off;
        let target = destination(snapshot, &bucket.name, encoded);
        let unknown = (target.backend_id).filter(|id| !self.storages.contains_key(id));
        self.current = Some(target);
        match unknown {
            Some(id) => {
                let backend = GetBackendOperation::new(id);
                self.call(StorageStep::Backend(id), IoCall::child(backend))
            }
            None => self.read_job(),
        }
    }

    fn read_job(&mut self) -> StorageTurn {
        let current = self.current.as_ref().ok_or(IoError::Unexpected {
            expected: "the current bucket",
            got: "none",
        })?;
        let job = ActiveJobOperation::new(&current.bucket, &self.rules);
        self.call(StorageStep::Job, IoCall::child(job))
    }

    fn job_read(&mut self, job: Option<ActiveJob>) -> StorageTurn {
        if let Some(current) = self.current.take() {
            let mut found = named(current, &self.storages);
            found.job = job;
            self.found.push(found);
        }
        self.next_bucket()
    }
}

impl Operation for ListBucketStorageOperation {
    type Output = BucketStoragePage;
    type Error = BucketStorageError;

    fn first(&mut self) -> StorageTurn {
        let list = ListBucketsOperation::new(ListBucketsInput {
            group_id: self.group_id,
            prefix: None,
            continuation_token: self.after.take(),
            max_buckets: Some(self.limit),
        });
        self.call(StorageStep::Buckets, IoCall::child(list))
    }

    fn next(&mut self, result: IoResult) -> StorageTurn {
        match self.step {
            StorageStep::Buckets => {
                self.listed(result.child::<ListBucketsResult, ListBucketsError>()??)
            }
            StorageStep::Inputs => {
                self.inputs = Some(result.child::<GroupRoutingInputs, GroupInputsError>()??);
                self.read_encryption()
            }
            StorageStep::Encryption => self.encryption_read(result.values()?),
            StorageStep::Imports => {
                self.imports_read(result.child::<Vec<BackendImport>, ImportRuleError>()??)
            }
            StorageStep::Backend(id) => {
                let record = result.child::<Option<GroupStorage>, GroupQueryError>()??;
                let group_id = self.group_id;
                let record = record.filter(|record| record.group_id == group_id);
                self.storages.insert(id, record);
                self.read_job()
            }
            StorageStep::Job => {
                self.job_read(result.child::<Option<ActiveJob>, ActiveJobError>()??)
            }
        }
    }
}

/// Where uploads of `bucket` go: a whole-bucket link's write folder, else the backend an
/// upload resolves to. Encoded uploads keep Aruna's layout and skip the link.
fn destination(snapshot: &RoutingSnapshot, bucket: &str, encoded: bool) -> BucketStorage {
    let link =
        (snapshot.imports.iter()).find(|rule| rule.bucket == bucket && rule.key_prefix.is_empty());
    if let Some(rule) = link.filter(|_| !encoded) {
        return BucketStorage {
            bucket: bucket.to_string(),
            backend_id: Some(rule.write_backend()),
            folder: Some(rule.write_folder().to_string()),
            import_id: Some(rule.import_id),
            write_pattern: Some(rule.write_pattern.clone()),
            ..BucketStorage::default()
        };
    }
    let backend = resolve_backend(snapshot, bucket, "").map(|resolved| resolved.backend);
    BucketStorage {
        bucket: bucket.to_string(),
        backend_id: match backend {
            Ok(BackendRef::Group(id)) => Some(id),
            _ => None,
        },
        shared: matches!(backend, Ok(BackendRef::Group(_))),
        import_id: link.map(|rule| rule.import_id),
        write_pattern: link.map(|rule| rule.write_pattern.clone()),
        ..BucketStorage::default()
    }
}

/// Adds the S3 bucket or container the files go to, where the storage record tells it.
fn named(
    mut found: BucketStorage,
    storages: &HashMap<Ulid, Option<GroupStorage>>,
) -> BucketStorage {
    let record = found
        .backend_id
        .and_then(|id| storages.get(&id))
        .and_then(Option::as_ref);
    found.provider_bucket = record
        .and_then(|record| match (record.splits(), found.folder.as_deref()) {
            (true, Some(folder)) => path_bucket(folder),
            (true, None) => record.default_bucket(),
            (false, _) => record.container(),
        })
        .map(str::to_string);
    found
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::storage::backend_import::{BackendImport, PLAIN_PATTERN};
    use aruna_core::structs::storage::cleanup::CleanupStrategy;
    use aruna_core::structs::storage::group_backend::GroupBackendKind;
    use aruna_core::structs::storage::routing::{
        BackendCatalog, GroupRoutingInputs, RoutingTarget, StorageRoutingRule,
    };
    use std::time::SystemTime;

    const BACKEND: Ulid = Ulid::from_parts(2, 2);
    const WRITES: Ulid = Ulid::from_parts(4, 4);

    fn storage(id: Ulid) -> GroupStorage {
        GroupStorage {
            backend_id: id,
            group_id: Ulid::from_parts(1, 1),
            name: "spire".to_string(),
            kind: GroupBackendKind::S3,
            public_config: [("default_bucket".to_string(), "aruna-spire".to_string())].into(),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            disabled: false,
            cleanup: CleanupStrategy::Retain,
        }
    }

    fn link(bucket: &str, folder: &str, key_prefix: &str) -> BackendImport {
        BackendImport {
            import_id: Ulid::from_parts(3, 3),
            group_id: Ulid::from_parts(1, 1),
            backend_id: BACKEND,
            backend_prefix: folder.to_string(),
            bucket: bucket.to_string(),
            key_prefix: key_prefix.to_string(),
            schedule: None,
            created_by: Default::default(),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            last_job: None,
            last_started_ms: None,
            write_backend_id: Some(WRITES),
            write_prefix: Some("out/".to_string()),
            write_pattern: PLAIN_PATTERN.to_string(),
        }
    }

    #[test]
    fn shows_write_destinations() {
        let storages = HashMap::from([(WRITES, Some(storage(WRITES)))]);
        let catalog = BackendCatalog::new("default").with_backend("default", None);
        let inputs = GroupRoutingInputs {
            default_target: Some(RoutingTarget::Backend(BackendRef::Group(WRITES))),
            backend_ids: [BACKEND, WRITES].into(),
            imports: vec![link("genes", "genes/", ""), link("lab", "raw/", "in/")],
            ..GroupRoutingInputs::default()
        };
        let snapshot =
            RoutingSnapshot::new(Ulid::from_parts(1, 1), catalog).with_group_inputs(inputs);
        // A redirected link shows where its uploads go, not where it reads.
        let found = named(destination(&snapshot, "genes", false), &storages);
        assert_eq!(
            (
                found.backend_id,
                found.folder.as_deref(),
                found.provider_bucket.as_deref()
            ),
            (Some(WRITES), Some("out/"), Some("out"))
        );
        assert_eq!(found.import_id, Some(Ulid::from_parts(3, 3)));
        // An encoded bucket writes Aruna's layout to the shared bucket, not to its link.
        let found = named(destination(&snapshot, "genes", true), &storages);
        assert_eq!(
            (found.folder, found.shared, found.provider_bucket.as_deref()),
            (None, true, Some("aruna-spire"))
        );
        // A bucket rule naming a class this node lacks falls back to the group storage.
        let rule = StorageRoutingRule {
            key_prefix: String::new(),
            exact: false,
            target: RoutingTarget::Class("cold".to_string()),
        };
        let lab = snapshot.with_bucket_rules(vec![rule]);
        let found = named(destination(&lab, "lab", false), &storages);
        assert_eq!(
            (
                found.backend_id,
                found.shared,
                found.provider_bucket.as_deref()
            ),
            (Some(WRITES), true, Some("aruna-spire"))
        );
    }

    #[test]
    fn empty_page_ends() {
        use aruna_core::calls::ChildOutcome;
        let group = Ulid::from_parts(1, 1);
        let mut operation =
            ListBucketStorageOperation::new(NodeRouting::default(), group, (None, 5));
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let page = Ok::<_, ListBucketsError>(ListBucketsResult {
            buckets: Vec::new(),
            continuation_token: None,
        });
        operation
            .next(IoResult::Child(ChildOutcome::new(page)))
            .unwrap();
        let inputs = Ok::<_, GroupInputsError>(GroupRoutingInputs::default());
        let turn = operation.next(IoResult::Child(ChildOutcome::new(inputs)));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
        let imports = Ok::<_, ImportRuleError>(Vec::<BackendImport>::new());
        let turn = operation.next(IoResult::Child(ChildOutcome::new(imports)));
        assert_eq!(turn, Ok(Next::Done((Vec::new(), None))));
    }
}
