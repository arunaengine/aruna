//! Renames the keys under one folder of a bucket to another folder in one transaction. Version
//! ids, history, delete markers and provider files stay; only the bucket keys change.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::s3::imports::cron::is_running;
use crate::s3::imports::validate_folder;
use crate::s3::purge_fence::fence_key;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{
    BACKEND_IMPORT_KEYSPACE, BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE,
    BROKEN_VERSION_KEYSPACE, BUCKET_ENCRYPTION_KEYSPACE, COMPRESSION_MIGRATION_KEYSPACE,
    IMPORTED_OBJECT_KEYSPACE, JOB_KEYSPACE, LINKED_JOB_KEYSPACE, MANAGED_COPY_KEYSPACE,
    PATHS_INDEX_KEYSPACE, PURGE_FENCE_KEYSPACE, RELATIONSHIP_IN_KEYSPACE,
    RELATIONSHIP_OUT_KEYSPACE, RENAMED_ROW_KEYSPACE, S3_BUCKET_KEYSPACE, UPLOAD_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::values;
use aruna_core::structs::execution::job::JobRecord;
use aruna_core::structs::execution::job::job_record_key;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::backend_import::{BackendImport, ImportedObject, object_key};
use aruna_core::structs::storage::blob::{
    BackendLocation, BackendRef, BlobHeadKey, BlobVersion, BlobVersionState, BucketInfo,
    CurrentVersionPointer, HashIndex, ManagedCopyRecord, VersionKey, ensure_confined_path,
};
use aruna_core::structs::storage::format::{CompressionMigration, EncodingClass};
use aruna_core::structs::storage::group_backend::MAX_KEY_LEN;
use aruna_core::structs::storage::multipart::MultipartUpload;
use aruna_core::structs::storage::replication_queue::queued_prefixes;
use aruna_core::structs::sync_relationship_prefix;
use aruna_core::types::{GroupId, Key, Value};
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::Path;
use thiserror::Error;
use ulid::Ulid;

/// Why a rename of a bucket job's page refuses: the job was paused, aborted or replaced.
pub const JOB_CHANGED: &str = "the bucket job changed";

/// Keys one rename takes; a larger folder is renamed in parts.
pub const MAX_RENAME_KEYS: usize = 1000;

const PAGE: usize = 256;

/// Rows deleted under the old keys and rows written under the new ones.
type RowMoves = (Vec<(String, Key)>, Vec<(String, Key, Value)>);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RenameInput {
    pub realm_id: RealmId,
    pub node_id: NodeId,
    /// The group the caller was authorized for; the bucket must still belong to it.
    pub group_id: GroupId,
    pub bucket: String,
    pub from: String,
    pub to: String,
    /// The keys the preview showed. `None` selects the current keys under `from`, for a preview.
    pub keys: Option<Vec<String>>,
    /// `false` only plans and changes nothing.
    pub apply: bool,
    /// Explicit new keys by old key; when not empty, `from`, `to` and `keys` are not used.
    pub pairs: Vec<(String, String)>,
    /// A key whose current version is newer than this stays.
    pub cutoff_ms: Option<u64>,
    /// The bucket's job row as the caller read it and as the rename leaves it; another stored
    /// row, as after a pause or an abort, refuses the rename.
    pub job: Option<(Value, Value)>,
}

/// One planned key move.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RenameItem {
    pub from: String,
    pub to: String,
    /// Every stored version of the key, current or not, which keep their ids.
    pub versions: Vec<Ulid>,
    pub current: Option<Ulid>,
    /// The current version is a delete marker.
    pub deleted: bool,
    /// Provider files whose recorded key changes; the files themselves stay.
    pub files: Vec<String>,
    /// Why the key cannot move; a rename with any conflict changes nothing.
    pub conflict: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RenamePlan {
    pub items: Vec<RenameItem>,
    /// More keys lie under `from` than one rename takes.
    pub truncated: bool,
}

#[derive(Debug, Error, PartialEq)]
pub enum RenameError {
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    Conflict(String),
    #[error("bucket not found")]
    NotFound,
    #[error("permission denied")]
    Forbidden,
    #[error("authorization check failed: {0}")]
    Authorization(String),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("unexpected answer in state {0}")]
    Unexpected(&'static str),
}

impl From<IoError> for RenameError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.into())
    }
}

type RenameTurn = Result<Next<RenamePlan>, RenameError>;

/// Why `to` cannot be a key, if it cannot.
pub fn check_target(to: &str) -> Result<(), String> {
    if to.is_empty() {
        return Err("the new key would be empty".to_string());
    }
    if to.len() > MAX_KEY_LEN {
        return Err(format!("the new key `{to}` is too long"));
    }
    ensure_confined_path(Path::new(to)).map_err(|error| error.to_string())
}

/// Where `key` goes when the folder `from` becomes `to`; `None` outside `from`.
pub fn renamed_key(from: &str, to: &str, key: &str) -> Option<String> {
    key.strip_prefix(from).map(|rest| format!("{to}{rest}"))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ScanKind {
    Relations,
    Queued,
    Rules,
    Uploads,
    Select,
    Versions(usize),
    Target(usize),
    Copies(usize),
    Broken(usize),
    Renamed(usize),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Scan {
    kind: ScanKind,
    space: &'static str,
    prefix: Vec<u8>,
    limit: usize,
    start: Option<Key>,
}

impl Scan {
    fn new(kind: ScanKind, space: &'static str, prefix: Vec<u8>, limit: usize) -> Self {
        Self {
            kind,
            space,
            prefix,
            limit,
            start: None,
        }
    }
}

/// The rows of one key the transaction read.
#[derive(Debug, Default, PartialEq)]
struct Found {
    head: Option<Value>,
    target_head: bool,
    target_versions: bool,
    versions: Vec<(VersionKey, Value)>,
    copies: Vec<(Key, ManagedCopyRecord)>,
    broken: Vec<(VersionKey, Value)>,
    /// Path index rows that exist for the key's versions.
    indexed: Vec<HashIndex>,
    rows: Vec<(Key, ImportedObject)>,
    /// Rows an earlier rename or a second copy put at this key: index row, own key, value.
    renamed: Vec<(Key, Vec<u8>, Value)>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RenameState {
    Bucket,
    Scan,
    Runs,
    Heads,
    Locations,
    Rows,
    Index,
    Delete,
    Write,
}

/// Reads every row of the selected keys in one write transaction, checks for collisions and
/// moves the rows, so a concurrent write, import start or rule change makes it fail whole.
#[derive(Debug, PartialEq)]
pub struct RenameKeysOperation {
    input: RenameInput,
    state: RenameState,
    scans: VecDeque<Scan>,
    scan: Option<Scan>,
    rules: Vec<BackendImport>,
    /// The new key of each old key of `pairs`.
    targets: HashMap<String, String>,
    /// Keys of the bucket with an open multipart upload, which would write the old key.
    uploads: HashSet<String>,
    items: Vec<RenameItem>,
    found: Vec<Found>,
    /// The rows the transaction reads for the linked files of the keys: item, rule and path.
    row_reads: Option<Vec<(usize, usize, String)>>,
    truncated: bool,
    writes: Vec<(String, Key, Value)>,
    plan: Option<RenamePlan>,
}

impl RenameKeysOperation {
    pub fn new(input: RenameInput) -> Self {
        let targets = input.pairs.iter().cloned().collect();
        Self {
            input,
            targets,
            state: RenameState::Bucket,
            scans: VecDeque::new(),
            scan: None,
            rules: Vec::new(),
            uploads: HashSet::new(),
            items: Vec::new(),
            found: Vec::new(),
            row_reads: None,
            truncated: false,
            writes: Vec::new(),
            plan: None,
        }
    }

    fn call(&mut self, state: RenameState, call: IoCall) -> RenameTurn {
        self.state = state;
        Ok(Next::Call(call))
    }

    fn validate(&self) -> Result<Vec<RenameItem>, RenameError> {
        let input = &self.input;
        let invalid = |reason: String| RenameError::Invalid(reason);
        if !input.pairs.is_empty() {
            let pairs = &input.pairs;
            if input.keys.is_some() || pairs.len() > MAX_RENAME_KEYS {
                return Err(invalid(format!(
                    "a rename takes 1 to {MAX_RENAME_KEYS} keys"
                )));
            }
            let sources: HashSet<_> = pairs.iter().map(|(from, _)| from).collect();
            let targets: HashSet<_> = pairs.iter().map(|(_, to)| to).collect();
            if sources.len() != pairs.len() || targets.len() != pairs.len() {
                return Err(invalid("keys and new keys must not repeat".to_string()));
            }
            return pairs.iter().map(|(from, _)| self.item(from)).collect();
        }
        validate_folder("from", &input.from).map_err(|error| invalid(error.to_string()))?;
        validate_folder("to", &input.to).map_err(|error| invalid(error.to_string()))?;
        if input.from == input.to {
            return Err(invalid("from and to must differ".to_string()));
        }
        let Some(keys) = input.keys.as_ref() else {
            return match input.apply {
                true => Err(invalid(
                    "a rename needs the keys of its preview".to_string(),
                )),
                false => Ok(Vec::new()),
            };
        };
        if keys.is_empty() || keys.len() > MAX_RENAME_KEYS {
            return Err(invalid(format!(
                "a rename takes 1 to {MAX_RENAME_KEYS} keys"
            )));
        }
        if keys.iter().collect::<HashSet<_>>().len() != keys.len() {
            return Err(invalid("keys must not repeat".to_string()));
        }
        keys.iter().map(|key| self.item(key)).collect()
    }

    fn item(&self, key: &str) -> Result<RenameItem, RenameError> {
        let input = &self.input;
        let invalid = |reason: String| RenameError::Invalid(reason);
        let to = match input.pairs.is_empty() {
            true => renamed_key(&input.from, &input.to, key),
            false => self.targets.get(key).cloned(),
        };
        let to = to
            .filter(|_| !key.is_empty())
            .ok_or_else(|| invalid(format!("key `{key}` does not lie under `{}`", input.from)))?;
        if to.is_empty() {
            return Err(invalid(format!("the new key of `{key}` would be empty")));
        }
        if to.len() > MAX_KEY_LEN {
            return Err(invalid(format!("the new key of `{key}` is too long")));
        }
        ensure_confined_path(Path::new(&to)).map_err(|error| invalid(error.to_string()))?;
        Ok(RenameItem {
            from: key.to_string(),
            to,
            ..RenameItem::default()
        })
    }

    fn read_bucket(&mut self) -> RenameTurn {
        let name: Key = self.input.bucket.as_bytes().to_vec().into();
        let reads = vec![
            (S3_BUCKET_KEYSPACE.to_string(), name.clone()),
            (BUCKET_ENCRYPTION_KEYSPACE.to_string(), name.clone()),
            (COMPRESSION_MIGRATION_KEYSPACE.to_string(), name.clone()),
            (
                PURGE_FENCE_KEYSPACE.to_string(),
                fence_key(&self.input.bucket),
            ),
            (LINKED_JOB_KEYSPACE.to_string(), name),
        ];
        self.call(RenameState::Bucket, IoCall::Read(reads))
    }

    /// Only a plain bucket of the caller's group without replication, a purge or a running
    /// compression or storage move is renamed; a move would miss the moved versions.
    fn bucket_read(
        &mut self,
        [bucket, encryption, migration, fence, job]: [Option<Value>; 5],
    ) -> RenameTurn {
        if let Some((expected, _)) = &self.input.job
            && job.as_ref() != Some(expected)
        {
            return self.conflict(JOB_CHANGED);
        }
        let info = bucket.as_deref().map(BucketInfo::from_bytes).transpose()?;
        let info = info.ok_or(RenameError::NotFound)?;
        if info.group_id != self.input.group_id {
            return Err(RenameError::NotFound);
        }
        // Its inventory and replicas name every key, so keys move only as new writes.
        if info.distribution.is_some() {
            return self.conflict("keys of a distributed bucket cannot be renamed in place");
        }
        if encryption.is_some() {
            return self.conflict("an encrypted bucket cannot be renamed");
        }
        if fence.is_some() {
            return self.conflict("a permanent purge of this bucket is running");
        }
        let migration = migration.as_deref().map(CompressionMigration::from_bytes);
        if migration
            .transpose()?
            .is_some_and(|run| run.finished_at_ms.is_none())
        {
            return self.conflict("a compression or storage move of this bucket is running");
        }
        let bucket = self.input.bucket.as_str();
        let group = self.input.group_id.to_bytes().to_vec();
        let mut scans = VecDeque::from([
            Scan::new(
                ScanKind::Relations,
                RELATIONSHIP_OUT_KEYSPACE,
                sync_relationship_prefix(bucket),
                1,
            ),
            Scan::new(
                ScanKind::Relations,
                RELATIONSHIP_IN_KEYSPACE,
                sync_relationship_prefix(bucket),
                1,
            ),
            Scan::new(ScanKind::Rules, BACKEND_IMPORT_KEYSPACE, group, PAGE),
            Scan::new(ScanKind::Uploads, UPLOAD_KEYSPACE, Vec::new(), PAGE),
        ]);
        let queued = queued_prefixes(bucket)?;
        scans.extend(queued.map(|(space, prefix)| Scan::new(ScanKind::Queued, space, prefix, 1)));
        if self.input.keys.is_none() && self.input.pairs.is_empty() {
            let prefix = BlobHeadKey::object_prefix(bucket, &self.input.from)?;
            scans.push_back(Scan::new(
                ScanKind::Select,
                BLOB_HEAD_KEYSPACE,
                prefix,
                PAGE,
            ));
        }
        self.scans = scans;
        self.next_scan()
    }

    fn conflict(&self, reason: &str) -> RenameTurn {
        Err(RenameError::Conflict(reason.to_string()))
    }

    fn next_scan(&mut self) -> RenameTurn {
        let Some(scan) = self.scans.pop_front() else {
            self.scan = None;
            return self.scans_done();
        };
        let page = IoCall::Page {
            space: scan.space.to_string(),
            prefix: scan.prefix.clone().into(),
            after: scan.start.clone(),
            limit: scan.limit,
        };
        self.scan = Some(scan);
        self.call(RenameState::Scan, page)
    }

    fn scanned(&mut self, rows: Vec<(Key, Value)>, next: Option<Key>) -> RenameTurn {
        let mut scan = self.scan.take().ok_or(RenameError::Unexpected("Scan"))?;
        let more = self.take_page(scan.kind, rows)?;
        if let (true, Some(next)) = (more, next) {
            scan.start = Some(next);
            self.scans.push_front(scan);
        }
        self.next_scan()
    }

    /// Records one page; answers whether the scan wants the next page.
    fn take_page(
        &mut self,
        kind: ScanKind,
        values: Vec<(Key, Value)>,
    ) -> Result<bool, RenameError> {
        match kind {
            ScanKind::Relations if !values.is_empty() => Err(RenameError::Conflict(
                "a bucket that replicates cannot be renamed".to_string(),
            )),
            ScanKind::Queued if !values.is_empty() => Err(RenameError::Conflict(
                "a copy of this bucket to another node is still queued".to_string(),
            )),
            ScanKind::Relations | ScanKind::Queued => Ok(false),
            ScanKind::Rules => {
                for (_, value) in values {
                    let rule = BackendImport::from_bytes(&value)?;
                    if rule.bucket == self.input.bucket {
                        self.rules.push(rule);
                    }
                }
                Ok(true)
            }
            ScanKind::Uploads => {
                for (_, value) in values {
                    let upload = MultipartUpload::from_bytes(&value)?;
                    if upload.bucket == self.input.bucket {
                        self.uploads.insert(upload.key);
                    }
                }
                Ok(true)
            }
            ScanKind::Select => {
                for (key, _) in values {
                    if self.items.len() == MAX_RENAME_KEYS {
                        self.truncated = true;
                        return Ok(false);
                    }
                    let head = BlobHeadKey::from_bytes(&key)?;
                    // Keys are shown only after their permissions are checked.
                    let item = self.item(&head.key).map_err(|_| {
                        RenameError::Invalid(
                            "a key under `from` cannot get a name under `to`".into(),
                        )
                    })?;
                    self.items.push(item);
                }
                Ok(true)
            }
            ScanKind::Versions(index) => {
                for (key, value) in values {
                    self.found[index]
                        .versions
                        .push((VersionKey::from_bytes(&key)?, value));
                }
                Ok(true)
            }
            ScanKind::Target(index) => {
                self.found[index].target_versions = !values.is_empty();
                Ok(false)
            }
            ScanKind::Copies(index) => {
                for (key, value) in values {
                    let record = ManagedCopyRecord::from_bytes(&value)?;
                    self.found[index].copies.push((key, record));
                }
                Ok(true)
            }
            ScanKind::Broken(index) => {
                for (key, value) in values {
                    self.found[index]
                        .broken
                        .push((VersionKey::from_bytes(&key)?, value));
                }
                Ok(true)
            }
            ScanKind::Renamed(index) => {
                let from = &self.items[index].from;
                let prefix = VersionKey::object_prefix(&self.input.bucket, from)?;
                for (key, value) in values {
                    // The index key is the rule, the key's prefix, then the row's path.
                    let import = key.get(..16).unwrap_or_default();
                    let path = key.get(16 + prefix.len()..).unwrap_or_default();
                    let row = [import, path].concat();
                    self.found[index].renamed.push((key, row, value));
                }
                Ok(true)
            }
        }
    }

    /// The location rows of the keys' materialized versions, which name their linked files.
    fn read_locations(&mut self) -> RenameTurn {
        let mut reads = Vec::new();
        for found in &self.found {
            for (_, value) in &found.versions {
                if let Some(key) = BlobVersion::from_bytes(value)?.location_key() {
                    reads.push((BLOB_LOCATIONS_KEYSPACE.to_string(), key.to_bytes().into()));
                }
            }
        }
        self.row_reads = Some(Vec::new());
        if self.rules.is_empty() {
            return self.read_index();
        }
        if reads.is_empty() {
            return self.locations_read(Vec::new());
        }
        self.call(RenameState::Locations, IoCall::Read(reads))
    }

    /// Reads each rule's record of every linked file a version of a selected key names.
    fn locations_read(&mut self, values: Vec<Option<Value>>) -> RenameTurn {
        let mut owners = Vec::new();
        for (index, found) in self.found.iter().enumerate() {
            let count = found.versions.iter().filter(|(_, value)| {
                BlobVersion::from_bytes(value).is_ok_and(|version| version.location_key().is_some())
            });
            owners.extend(std::iter::repeat_n(index, count.count()));
        }
        if owners.len() != values.len() {
            return Err(RenameError::Unexpected("Locations"));
        }
        let mut reads = Vec::new();
        let mut seen = HashSet::new();
        for (index, value) in owners.into_iter().zip(values) {
            let Some(value) = value else {
                continue;
            };
            let location = BackendLocation::from_bytes(&value)?;
            if !location.is_external() {
                continue;
            }
            for (rule, import) in self.rules.iter().enumerate() {
                let on = |id: Ulid| BackendRef::Group(id) == location.backend;
                let path = location.backend_path.clone();
                if (on(import.backend_id) || on(import.write_backend()))
                    && seen.insert((index, rule, path.clone()))
                {
                    reads.push((index, rule, path));
                }
            }
        }
        // Rows an earlier rename moved to the key are read where they are.
        for (index, found) in self.found.iter().enumerate() {
            for (_, row, _) in &found.renamed {
                let (import, path) = row.split_at(row.len().min(16));
                let path = String::from_utf8_lossy(path).into_owned();
                let rule = (self.rules.iter()).position(|rule| rule.import_id.to_bytes() == import);
                if let Some(rule) = rule
                    && seen.insert((index, rule, path.clone()))
                {
                    reads.push((index, rule, path));
                }
            }
        }
        // A file whose version reads another file's location, as identical imports do, has
        // its row at the path its rule gives the key.
        for (index, item) in self.items.iter().enumerate() {
            let versions = &self.found[index].versions;
            for (rule, import) in self.rules.iter().enumerate() {
                let rest = item.from.strip_prefix(&import.key_prefix);
                let named = [Some(item.from.as_str()), rest]
                    .into_iter()
                    .flatten()
                    .map(|rest| format!("{}{rest}", import.backend_prefix));
                let written = versions
                    .iter()
                    .filter_map(|(key, _)| import.plain_path(&item.from, key.version_id));
                for path in named.chain(written) {
                    if seen.insert((index, rule, path.clone())) {
                        reads.push((index, rule, path));
                    }
                }
            }
        }
        if reads.is_empty() {
            return self.read_index();
        }
        let rows = reads
            .iter()
            .map(|(_, rule, path)| {
                let key = object_key(self.rules[*rule].import_id, path);
                (IMPORTED_OBJECT_KEYSPACE.to_string(), key)
            })
            .collect();
        self.row_reads = Some(reads);
        self.call(RenameState::Rows, IoCall::Read(rows))
    }

    /// Notes each recorded file that maps to its selected key.
    fn rows_read(&mut self, values: Vec<Option<Value>>) -> RenameTurn {
        let reads = self.row_reads.clone().unwrap_or_default();
        if reads.len() != values.len() {
            return Err(RenameError::Unexpected("Rows"));
        }
        for ((index, rule, path), value) in reads.into_iter().zip(values) {
            let Some(value) = value else {
                continue;
            };
            let row = ImportedObject::from_bytes(&value)?;
            let import = &self.rules[rule];
            let mapped = row.key.clone().or_else(|| import.aruna_key(&path));
            if mapped.as_deref() == Some(self.items[index].from.as_str()) {
                let key = object_key(import.import_id, &path);
                self.found[index].rows.push((key, row));
                self.items[index].files.push(path);
            }
        }
        self.read_index()
    }

    /// The rules are known; a running import of the bucket would act on stale rows.
    fn scans_done(&mut self) -> RenameTurn {
        match (self.found.is_empty(), self.row_reads.is_some()) {
            (true, _) => self.read_runs(),
            (false, false) => self.read_locations(),
            (false, true) => self.read_index(),
        }
    }

    fn read_runs(&mut self) -> RenameTurn {
        // A later part would select the keys this part moved, so `to` inside `from` needs one part.
        if self.truncated && self.input.to.starts_with(&self.input.from) {
            return self.conflict("too many keys to rename into a folder inside `from` at once");
        }
        if self.items.is_empty() {
            return self.finish_plan();
        }
        let reads: Vec<_> = self
            .rules
            .iter()
            .filter_map(|rule| rule.last_job)
            .map(|job_id| (JOB_KEYSPACE.to_string(), job_record_key(job_id)))
            .collect();
        if reads.is_empty() {
            return self.read_heads();
        }
        self.call(RenameState::Runs, IoCall::Read(reads))
    }

    fn runs_read(&mut self, values: Vec<Option<Value>>) -> RenameTurn {
        for value in values {
            let record = value.as_deref().map(JobRecord::from_bytes).transpose()?;
            if record.is_some_and(|record| is_running(record.state)) {
                return self.conflict("an import of this bucket is running");
            }
        }
        self.read_heads()
    }

    fn read_heads(&mut self) -> RenameTurn {
        let bucket = &self.input.bucket;
        let reads = self
            .items
            .iter()
            .flat_map(|item| [&item.from, &item.to])
            .map(|key| BlobHeadKey::new(bucket.clone(), key.clone()).to_bytes())
            .map(|head| head.map(|head| (BLOB_HEAD_KEYSPACE.to_string(), Key::from(head))))
            .collect::<Result<Vec<_>, _>>()?;
        self.call(RenameState::Heads, IoCall::Read(reads))
    }

    fn heads_read(&mut self, values: Vec<Option<Value>>) -> RenameTurn {
        if values.len() != self.items.len() * 2 {
            return Err(RenameError::Unexpected("Heads"));
        }
        let mut values = values.into_iter();
        while let (Some(head), Some(target)) = (values.next(), values.next()) {
            self.found.push(Found {
                head,
                target_head: target.is_some(),
                ..Found::default()
            });
        }
        let scans = self.key_scans()?;
        self.scans.extend(scans);
        self.next_scan()
    }

    /// Per key its versions, the new key's versions, copies and broken rows; then the files.
    fn key_scans(&self) -> Result<Vec<Scan>, RenameError> {
        let bucket = &self.input.bucket;
        let mut scans = Vec::new();
        for (index, item) in self.items.iter().enumerate() {
            let source = VersionKey::object_prefix(bucket, &item.from)?;
            let target = VersionKey::object_prefix(bucket, &item.to)?;
            scans.extend([
                Scan::new(
                    ScanKind::Versions(index),
                    BLOB_VERSIONS_KEYSPACE,
                    source.clone(),
                    PAGE,
                ),
                Scan::new(ScanKind::Target(index), BLOB_VERSIONS_KEYSPACE, target, 1),
                Scan::new(
                    ScanKind::Copies(index),
                    MANAGED_COPY_KEYSPACE,
                    source.clone(),
                    PAGE,
                ),
                Scan::new(
                    ScanKind::Broken(index),
                    BROKEN_VERSION_KEYSPACE,
                    source.clone(),
                    PAGE,
                ),
            ]);
            for rule in &self.rules {
                let prefix = [&rule.import_id.to_bytes()[..], &source[..]].concat();
                let kind = ScanKind::Renamed(index);
                scans.push(Scan::new(kind, RENAMED_ROW_KEYSPACE, prefix, PAGE));
            }
        }
        Ok(scans)
    }

    /// The path index row each materialized version may have under its old key.
    fn index_rows(&self) -> Vec<(usize, HashIndex)> {
        let input = &self.input;
        let mut rows = Vec::new();
        for (index, found) in self.found.iter().enumerate() {
            for (key, value) in &found.versions {
                let Ok(version) = BlobVersion::from_bytes(value) else {
                    continue;
                };
                if let Some(hash) = version.blob_hash() {
                    let row = HashIndex::new(
                        *hash,
                        key.version_id,
                        input.realm_id,
                        input.group_id,
                        input.node_id,
                        input.bucket.clone(),
                        key.key.clone(),
                    );
                    rows.push((index, row));
                }
            }
        }
        rows
    }

    fn read_index(&mut self) -> RenameTurn {
        let rows = self.index_rows();
        let mut reads = Vec::with_capacity(rows.len());
        for (_, row) in &rows {
            reads.push((PATHS_INDEX_KEYSPACE.to_string(), row.to_bytes()?.into()));
        }
        if reads.is_empty() {
            return self.finish_plan();
        }
        self.call(RenameState::Index, IoCall::Read(reads))
    }

    fn index_read(&mut self, values: Vec<Option<Value>>) -> RenameTurn {
        let rows = self.index_rows();
        if values.len() != rows.len() {
            return Err(RenameError::Unexpected("Index"));
        }
        for ((index, row), value) in rows.into_iter().zip(values) {
            if value.is_some() {
                self.found[index].indexed.push(row);
            }
        }
        self.finish_plan()
    }

    /// Fills each item and its conflict; a preview ends here, a rename moves the rows.
    fn finish_plan(&mut self) -> RenameTurn {
        let sources: HashSet<String> = self.items.iter().map(|item| item.from.clone()).collect();
        for (item, found) in self.items.iter_mut().zip(&self.found) {
            describe(item, found, (&sources, &self.uploads), self.input.cutoff_ms)?;
        }
        let plan = RenamePlan {
            items: self.items.clone(),
            truncated: self.truncated,
        };
        if !self.input.apply {
            return Ok(Next::Done(plan));
        }
        let conflicts: Vec<String> = plan
            .items
            .iter()
            .filter_map(|item| {
                let conflict = item.conflict.as_ref()?;
                Some(format!("`{}`: {conflict}", item.from))
            })
            .take(5)
            .collect();
        if !conflicts.is_empty() {
            return Err(RenameError::Conflict(conflicts.join("; ")));
        }
        self.plan = Some(plan);
        let (deletes, writes) = self.moves()?;
        self.writes = writes;
        self.call(RenameState::Delete, IoCall::Delete(deletes))
    }

    /// The rows to delete under the old keys and to write under the new ones.
    fn moves(&self) -> Result<RowMoves, RenameError> {
        let bucket = &self.input.bucket;
        let (mut deletes, mut writes) = (Vec::new(), Vec::new());
        for (item, found) in self.items.iter().zip(&self.found) {
            if let Some(head) = &found.head {
                let old = BlobHeadKey::new(bucket.clone(), item.from.clone()).to_bytes()?;
                let new = BlobHeadKey::new(bucket.clone(), item.to.clone()).to_bytes()?;
                deletes.push((BLOB_HEAD_KEYSPACE.to_string(), old.into()));
                writes.push((BLOB_HEAD_KEYSPACE.to_string(), new.into(), head.clone()));
            }
            let moved =
                |key: &VersionKey| VersionKey::new(bucket.clone(), &item.to, key.version_id);
            for (key, value) in &found.versions {
                deletes.push((BLOB_VERSIONS_KEYSPACE.to_string(), key.to_bytes()?.into()));
                let new = moved(key).to_bytes()?;
                writes.push((
                    BLOB_VERSIONS_KEYSPACE.to_string(),
                    new.into(),
                    value.clone(),
                ));
            }
            for (key, record) in &found.copies {
                deletes.push((MANAGED_COPY_KEYSPACE.to_string(), key.clone()));
                let record = ManagedCopyRecord {
                    version: moved(&record.version),
                    ..record.clone()
                };
                let new = record.key().to_bytes()?.into();
                writes.push((
                    MANAGED_COPY_KEYSPACE.to_string(),
                    new,
                    record.to_bytes()?.into(),
                ));
            }
            for (key, value) in &found.broken {
                deletes.push((BROKEN_VERSION_KEYSPACE.to_string(), key.to_bytes()?.into()));
                let new = moved(key).to_bytes()?;
                writes.push((
                    BROKEN_VERSION_KEYSPACE.to_string(),
                    new.into(),
                    value.clone(),
                ));
            }
            for row in &found.indexed {
                deletes.push((PATHS_INDEX_KEYSPACE.to_string(), row.to_bytes()?.into()));
                let new = HashIndex {
                    key: item.to.clone(),
                    ..row.clone()
                };
                let empty = Value::from(Vec::new());
                writes.push((
                    PATHS_INDEX_KEYSPACE.to_string(),
                    new.to_bytes()?.into(),
                    empty,
                ));
            }
            for (key, ..) in &found.renamed {
                deletes.push((RENAMED_ROW_KEYSPACE.to_string(), key.clone()));
            }
            let moved_to = VersionKey::object_prefix(bucket, &item.to)?;
            for (key, row) in &found.rows {
                let row = ImportedObject {
                    key: Some(item.to.clone()),
                    ..row.clone()
                };
                writes.push((
                    IMPORTED_OBJECT_KEYSPACE.to_string(),
                    key.clone(),
                    row.to_bytes()?.into(),
                ));
                // The row is found again by its new key, also when no version names its file.
                let (import, path) = key.split_at(key.len().min(16));
                let index = [import, &moved_to[..], path].concat();
                // A second copy keeps the file it names.
                let value = (found.renamed.iter())
                    .find(|(_, row, _)| row.as_slice() == key.as_ref())
                    .map_or_else(|| Value::from(Vec::new()), |(.., value)| value.clone());
                writes.push((RENAMED_ROW_KEYSPACE.to_string(), index.into(), value));
            }
        }
        if let Some((_, next)) = &self.input.job {
            let name = bucket.as_bytes().to_vec().into();
            writes.push((LINKED_JOB_KEYSPACE.to_string(), name, next.clone()));
        }
        Ok((deletes, writes))
    }
}

/// Fills the versions of one item and the reason it cannot move, if any.
fn describe(
    item: &mut RenameItem,
    found: &Found,
    (sources, uploads): (&HashSet<String>, &HashSet<String>),
    cutoff_ms: Option<u64>,
) -> Result<(), RenameError> {
    let head = found
        .head
        .as_deref()
        .map(CurrentVersionPointer::from_bytes)
        .transpose()?;
    item.current = head.map(|head| head.version_id);
    item.versions = found
        .versions
        .iter()
        .map(|(key, _)| key.version_id)
        .collect();
    let mut movable = true;
    for (key, value) in &found.versions {
        let version = BlobVersion::from_bytes(value)?;
        if Some(key.version_id) == item.current {
            item.deleted = version.is_deleted();
        }
        movable &= match &version.state {
            BlobVersionState::Materialized { encoding, .. } => {
                !matches!(encoding, EncodingClass::Pithos { .. })
            }
            BlobVersionState::Deleted => true,
            BlobVersionState::Reference { .. } | BlobVersionState::PendingContent { .. } => false,
        };
    }
    let newer = item.current.zip(cutoff_ms);
    item.conflict = if found.head.is_none() && found.versions.is_empty() {
        Some("the key does not exist".to_string())
    } else if newer.is_some_and(|(current, cutoff)| current.timestamp_ms() > cutoff) {
        Some("the key changed after the job started".to_string())
    } else if found.target_head || found.target_versions {
        Some(format!("the key `{}` already exists", item.to))
    } else if sources.contains(&item.to) {
        Some(format!("the key `{}` is renamed as well", item.to))
    } else if uploads.contains(&item.from) || uploads.contains(&item.to) {
        Some("a multipart upload to the key is open".to_string())
    } else if !movable {
        Some("the key holds encrypted, referenced or unfinished versions".to_string())
    } else {
        None
    };
    Ok(())
}

impl Operation for RenameKeysOperation {
    type Output = RenamePlan;
    type Error = RenameError;

    fn first(&mut self) -> RenameTurn {
        self.items = self.validate()?;
        self.read_bucket()
    }

    fn next(&mut self, result: IoResult) -> RenameTurn {
        match self.state {
            RenameState::Bucket => self.bucket_read(values::<5>(result)?),
            RenameState::Scan => {
                let (rows, next) = result.page()?;
                self.scanned(rows, next)
            }
            RenameState::Runs => self.runs_read(result.values()?),
            RenameState::Heads => self.heads_read(result.values()?),
            RenameState::Locations => self.locations_read(result.values()?),
            RenameState::Rows => self.rows_read(result.values()?),
            RenameState::Index => self.index_read(result.values()?),
            RenameState::Delete => {
                result.done()?;
                let writes = std::mem::take(&mut self.writes);
                self.call(RenameState::Write, IoCall::Write(writes))
            }
            RenameState::Write => {
                result.done()?;
                let plan = self.plan.take();
                plan.map(Next::Done).ok_or(RenameError::Unexpected("Write"))
            }
        }
    }

    /// A preview only reads, from one snapshot; a rename moves the rows it read.
    fn transaction(&self) -> TransactionMode {
        match self.input.apply {
            true => TransactionMode::Write,
            false => TransactionMode::ReadSnapshot,
        }
    }
}

#[cfg(test)]
#[path = "rename_tests.rs"]
pub(crate) mod tests;
