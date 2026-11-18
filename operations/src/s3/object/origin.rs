//! Reads where one file version came from: its origin record, its sync hop, the external
//! source it is bound to, and what a dataset may say about it. One point read per version.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::connectors::root_record::{hydrated, named_root, parse_root, read_root};
use std::collections::{BTreeSet, VecDeque};
use std::time::SystemTime;

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, StagingSourceEffect};
use aruna_core::events::{Event, StagingSourceEvent};
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, SOURCE_INDEX_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::rows::read_row;
use aruna_core::structs::checksum::ChecksumAlgorithm;
use aruna_core::structs::execution::source_access::{
    ResolvedSourceAccess, SourceDigest, SourceStats,
};
use aruna_core::structs::execution::source_connector::SourceConnector;
use aruna_core::structs::execution::staging::VersionSourceBinding;
use aruna_core::structs::identity::identifier::{CompiledParser, Identifier};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{
    BackendLocation, BlobHeadKey, BlobVersion, BlobVersionState, BucketInfo, CurrentVersionPointer,
    VersionKey,
};
use aruna_core::structs::storage::bucket_metadata::BucketMetadata;
use aruna_core::structs::storage::global_id::GlobalId;
use aruna_core::structs::storage::origin::{
    OriginProjection, SyncHop, VersionOrigin, project, public_source_url, publishable,
};
use aruna_core::types::{GroupId, Key};
use thiserror::Error;
use ulid::Ulid;

use crate::blob::migration::misplaced::KeyRule;
use crate::connectors::repository::source_connector_key;
use crate::connectors::resolver::{
    ResolveBindingInput, ResolveBindingOperation, SourceResolutionError,
};
use crate::s3::bucket::dataset::{BucketMetadataError, BucketMetadataOperation};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};

#[derive(Debug, Error, PartialEq)]
pub enum OriginError {
    #[error("bucket not found")]
    BucketMissing,
    #[error("object version not found")]
    VersionMissing,
    #[error("{0}")]
    Storage(String),
}

impl From<IoError> for OriginError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.to_string())
    }
}

/// The external source a version is bound to, as readers of the file see it.
#[derive(Clone, Debug, PartialEq)]
pub struct SourceView {
    pub url: Option<String>,
    pub path: String,
    /// The source's own version the binding names, when it names one.
    pub version: Option<String>,
    pub etag: Option<String>,
    pub digests: Vec<(ChecksumAlgorithm, SourceDigest)>,
    /// Every source of the version, equal ones; the first is the one above.
    pub sources: Vec<SourcePlace>,
}

/// Where one source of a version reads the bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourcePlace {
    pub url: Option<String>,
    pub path: String,
    /// This node's connector the source reads through; `None` for a public or native source.
    pub connector_id: Option<Ulid>,
    /// The connector's display name, read when the origin is read; never its settings.
    pub connector_name: Option<String>,
}

fn places(source: &VersionSourceBinding) -> Vec<SourcePlace> {
    (0..source.source_count())
        .filter_map(|slot| source.source_at(slot))
        .map(|single| place(&single))
        .collect()
}

fn place(single: &VersionSourceBinding) -> SourcePlace {
    SourcePlace {
        url: public_source_url(&single.descriptor),
        path: single.descriptor.source_path.clone(),
        connector_id: single.connector_id,
        connector_name: None,
    }
}

/// One source of a version with this node's measurements of it.
#[derive(Clone, Debug, PartialEq)]
pub struct SourceHealth {
    pub place: SourcePlace,
    /// `None` when this node cannot resolve the source, as it lacks its connector.
    pub stats: Option<SourceStats>,
}

type OriginTurn<T> = Result<Next<T>, OriginError>;

fn storage(error: impl std::fmt::Display) -> OriginError {
    OriginError::Storage(error.to_string())
}

#[derive(Debug, PartialEq)]
enum VersionStep {
    Head,
    Version(Ulid),
    Root(Ulid, Box<BlobVersion>),
}

/// Reads `version_id`, or the current version, of a local bucket's key, with its source root.
#[derive(Debug, PartialEq)]
struct ReadVersionOperation {
    bucket: String,
    key: String,
    version_id: Option<Ulid>,
    step: VersionStep,
}

type VersionTurn = OriginTurn<(Ulid, BlobVersion)>;

impl ReadVersionOperation {
    fn new(bucket: &str, key: &str, version_id: Option<Ulid>) -> Self {
        Self {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id,
            step: VersionStep::Head,
        }
    }

    fn read_version(&mut self, version_id: Ulid) -> VersionTurn {
        let row = VersionKey::new(&self.bucket, &self.key, version_id)
            .to_bytes()
            .map_err(storage)?;
        self.step = VersionStep::Version(version_id);
        Ok(Next::Call(read_row(BLOB_VERSIONS_KEYSPACE, row)))
    }
}

impl Operation for ReadVersionOperation {
    type Output = (Ulid, BlobVersion);
    type Error = OriginError;

    fn first(&mut self) -> VersionTurn {
        if let Some(version_id) = self.version_id {
            return self.read_version(version_id);
        }
        let head = BlobHeadKey::new(&self.bucket, &self.key)
            .to_bytes()
            .map_err(storage)?;
        Ok(Next::Call(read_row(BLOB_HEAD_KEYSPACE, head)))
    }

    fn next(&mut self, result: IoResult) -> VersionTurn {
        match std::mem::replace(&mut self.step, VersionStep::Head) {
            VersionStep::Head => {
                let value = result.value()?.ok_or(OriginError::VersionMissing)?;
                let pointer = CurrentVersionPointer::from_bytes(&value).map_err(storage)?;
                self.read_version(pointer.version_id)
            }
            VersionStep::Version(version_id) => {
                let value = result.value()?.ok_or(OriginError::VersionMissing)?;
                let version = BlobVersion::from_bytes(&value).map_err(storage)?;
                // The source's public address comes from the root this node keeps once.
                let Some(digest) = named_root(&version) else {
                    return Ok(Next::Done((version_id, version)));
                };
                self.step = VersionStep::Root(version_id, Box::new(version));
                Ok(Next::Call(Effect::Storage(read_root(digest, None)).into()))
            }
            VersionStep::Root(version_id, version) => {
                let root = parse_root(result.event()?).map_err(storage)?;
                let root = root.ok_or_else(|| storage("the source root record is missing"))?;
                let version = hydrated(*version, &root).map_err(OriginError::Storage)?;
                Ok(Next::Done((version_id, version)))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum HealthStep {
    Version,
    Resolve,
    Measure,
}

/// This node's measurements of every source of `version_id`, or of the current version, in the
/// binding's order; empty for a version without an external source.
#[derive(Debug, PartialEq)]
pub struct SourceHealthOperation {
    group_id: GroupId,
    bucket: String,
    key: String,
    version_id: Option<Ulid>,
    pending: VecDeque<VersionSourceBinding>,
    sources: Vec<(SourcePlace, Option<ResolvedSourceAccess>)>,
    step: HealthStep,
}

type HealthTurn = OriginTurn<Vec<SourceHealth>>;

impl SourceHealthOperation {
    pub fn new(group_id: GroupId, (bucket, key): (&str, &str), version_id: Option<Ulid>) -> Self {
        Self {
            group_id,
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id,
            pending: VecDeque::new(),
            sources: Vec::new(),
            step: HealthStep::Version,
        }
    }

    fn resolve_next(&mut self) -> HealthTurn {
        let Some(single) = self.pending.front() else {
            return self.measure();
        };
        let resolve = ResolveBindingOperation::new(ResolveBindingInput {
            source: single.clone(),
            group_id: Some(self.group_id),
        });
        self.step = HealthStep::Resolve;
        Ok(Next::Call(IoCall::child(resolve)))
    }

    fn measure(&mut self) -> HealthTurn {
        let accesses: Vec<_> = (self.sources.iter())
            .filter_map(|(_, access)| access.clone())
            .collect();
        if accesses.is_empty() {
            return self.finish(Vec::new());
        }
        self.step = HealthStep::Measure;
        let health = StagingSourceEffect::Health { accesses };
        Ok(Next::Call(Effect::StagingSource(health).into()))
    }

    fn finish(&mut self, stats: Vec<SourceStats>) -> HealthTurn {
        let mut stats = stats.into_iter();
        let sources = std::mem::take(&mut self.sources);
        Ok(Next::Done(
            sources
                .into_iter()
                .map(|(place, access)| SourceHealth {
                    place,
                    stats: access.and_then(|_| stats.next()),
                })
                .collect(),
        ))
    }
}

impl Operation for SourceHealthOperation {
    type Output = Vec<SourceHealth>;
    type Error = OriginError;

    fn first(&mut self) -> HealthTurn {
        let read = ReadVersionOperation::new(&self.bucket, &self.key, self.version_id);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> HealthTurn {
        match self.step {
            HealthStep::Version => {
                let (_, version) = result.child::<(Ulid, BlobVersion), OriginError>()??;
                let binding = match version.state {
                    BlobVersionState::Reference { source, .. }
                    | BlobVersionState::Materialized {
                        source: Some(source),
                        ..
                    }
                    | BlobVersionState::PendingContent {
                        source: Some(source),
                        ..
                    } => source,
                    _ => return Ok(Next::Done(Vec::new())),
                };
                self.pending = (0..binding.source_count())
                    .filter_map(|slot| binding.source_at(slot))
                    .collect();
                self.resolve_next()
            }
            HealthStep::Resolve => {
                let access = result.child::<ResolvedSourceAccess, SourceResolutionError>()?;
                let single = self.pending.pop_front().ok_or(IoError::Unfinished)?;
                self.sources.push((place(&single), access.ok()));
                self.resolve_next()
            }
            HealthStep::Measure => match result.event()? {
                Event::StagingSource(StagingSourceEvent::HealthResult { stats, .. }) => {
                    self.finish(stats)
                }
                _ => Err(storage("no source health answer")),
            },
        }
    }
}

/// Everything file readers see about one version's origin.
#[derive(Clone, Debug, PartialEq)]
pub struct OriginView {
    pub version_id: Ulid,
    /// The global id, for a version of a distributed bucket.
    pub global: Option<GlobalId>,
    pub created_by: UserId,
    pub created_at: SystemTime,
    pub origin: Option<VersionOrigin>,
    pub hop: Option<SyncHop>,
    pub source: Option<SourceView>,
    /// What a dataset may add about the origin: never users, devices or storage paths.
    pub projection: Vec<OriginProjection>,
    /// The bucket's metadata now; it may have changed since the version was made.
    pub bucket_metadata: Option<BucketMetadata>,
    /// What the metadata's identifier rules find in the key and source URL.
    pub identifiers: Vec<Identifier>,
    /// The BLAKE3 of the bytes, for a materialized version only; never a reference identity.
    pub content_hash: Option<[u8; 32]>,
    /// The size of exactly this version: its stored blob, or a reference's observed length.
    pub size: Option<u64>,
}

#[derive(Debug, PartialEq)]
enum OriginStep {
    Bucket,
    Version,
    Metadata,
    Size,
    Connectors(Vec<usize>),
}

/// Reads the origin of `version_id`, or of the current version, of a local bucket's key.
#[derive(Debug, PartialEq)]
pub struct ReadOriginOperation {
    realm_id: RealmId,
    bucket: String,
    key: String,
    version_id: Option<Ulid>,
    info: Option<BucketInfo>,
    version: Option<(Ulid, BlobVersion)>,
    metadata: Option<BucketMetadata>,
    view: Option<OriginView>,
    step: OriginStep,
}

type ReadTurn = OriginTurn<OriginView>;

impl ReadOriginOperation {
    pub fn new(realm_id: RealmId, bucket: &str, key: &str, version_id: Option<Ulid>) -> Self {
        Self {
            realm_id,
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id,
            info: None,
            version: None,
            metadata: None,
            view: None,
            step: OriginStep::Bucket,
        }
    }

    /// The size of one version: its blob location row, or a reference's cached observation.
    fn read_size(&mut self) -> ReadTurn {
        let (_, version) = self.version.as_ref().ok_or(IoError::Unfinished)?;
        if let BlobVersionState::Reference {
            cached_metadata, ..
        } = &version.state
        {
            let size = cached_metadata.content_length;
            return self.sized(Some(size));
        }
        let Some(location) = version.location_key() else {
            return self.sized(None);
        };
        self.step = OriginStep::Size;
        Ok(Next::Call(read_row(
            BLOB_LOCATIONS_KEYSPACE,
            location.to_bytes(),
        )))
    }

    /// Asks for the display name of every connector of the bucket's group that is here.
    fn sized(&mut self, size: Option<u64>) -> ReadTurn {
        let (version_id, version) = self.version.take().ok_or(IoError::Unfinished)?;
        let global = (self.info.as_ref())
            .and_then(|info| info.distribution.as_ref())
            .map(|_| GlobalId::Version {
                realm_id: self.realm_id,
                version_id,
            });
        let mut view = view(version_id, global, version);
        view.size = size;
        let group_id = self.info.as_ref().ok_or(IoError::Unfinished)?.group_id;
        let (mut slots, mut reads) = (Vec::new(), Vec::new());
        let places = view.source.iter().flat_map(|source| source.sources.iter());
        for (slot, place) in places.enumerate() {
            if let Some(connector_id) = place.connector_id {
                let key = source_connector_key(group_id, connector_id).to_vec();
                reads.push((SOURCE_INDEX_KEYSPACE.to_string(), Key::from(key)));
                slots.push(slot);
            }
        }
        self.view = Some(view);
        if reads.is_empty() {
            return self.finish();
        }
        self.step = OriginStep::Connectors(slots);
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn finish(&mut self) -> ReadTurn {
        let mut view = self.view.take().ok_or(IoError::Unfinished)?;
        let metadata = self.metadata.take();
        let parser = (metadata.as_ref())
            .and_then(|metadata| metadata.parser.as_ref())
            .and_then(|parser| CompiledParser::compile(parser).ok());
        let source_url = (view.source.as_ref()).and_then(|source| source.url.as_deref());
        view.identifiers =
            parser.map_or_else(Vec::new, |parser| parser.find(&self.key, source_url));
        view.projection.extend(
            view.identifiers
                .iter()
                .map(|found| OriginProjection::Identifier {
                    name: found.name.clone(),
                    value: found.value.clone(),
                    url: found.url.clone(),
                }),
        );
        view.bucket_metadata = metadata;
        Ok(Next::Done(view))
    }
}

impl Operation for ReadOriginOperation {
    type Output = OriginView;
    type Error = OriginError;

    fn first(&mut self) -> ReadTurn {
        let bucket = GetBucketOperation::new(self.bucket.clone());
        Ok(Next::Call(IoCall::child(bucket)))
    }

    fn next(&mut self, result: IoResult) -> ReadTurn {
        match std::mem::replace(&mut self.step, OriginStep::Version) {
            OriginStep::Bucket => {
                let info =
                    (result.child::<BucketInfo, GetBucketError>()?).map_err(
                        |error| match error {
                            GetBucketError::NotFound => OriginError::BucketMissing,
                            other => storage(other),
                        },
                    )?;
                self.info = Some(info);
                let read = ReadVersionOperation::new(&self.bucket, &self.key, self.version_id);
                Ok(Next::Call(IoCall::child(read)))
            }
            OriginStep::Version => {
                self.version = Some(result.child::<(Ulid, BlobVersion), OriginError>()??);
                let info = self.info.as_ref().ok_or(IoError::Unfinished)?;
                let metadata = BucketMetadataOperation::new(&self.bucket, info);
                self.step = OriginStep::Metadata;
                Ok(Next::Call(IoCall::child(metadata)))
            }
            OriginStep::Metadata => {
                let metadata = result.child::<Option<BucketMetadata>, BucketMetadataError>()?;
                self.metadata = metadata.map_err(storage)?;
                self.read_size()
            }
            OriginStep::Size => {
                let size = match result.value()? {
                    Some(value) => {
                        let location = BackendLocation::from_bytes(&value).map_err(storage)?;
                        Some(location.blob_size)
                    }
                    None => None,
                };
                self.sized(size)
            }
            OriginStep::Connectors(slots) => {
                let view = self.view.as_mut().ok_or(IoError::Unfinished)?;
                let source = view.source.as_mut().ok_or(IoError::Unfinished)?;
                for (slot, value) in slots.into_iter().zip(result.values()?) {
                    let Some(value) = value else {
                        continue;
                    };
                    let connector = SourceConnector::from_bytes(&value).map_err(storage)?;
                    if let Some(place) = source.sources.get_mut(slot) {
                        place.connector_name = Some(connector.name);
                    }
                }
                self.finish()
            }
        }
    }
}

fn view(version_id: Ulid, global: Option<GlobalId>, version: BlobVersion) -> OriginView {
    let descriptor = match &version.state {
        BlobVersionState::Reference { source, .. }
        | BlobVersionState::Materialized {
            source: Some(source),
            ..
        }
        | BlobVersionState::PendingContent {
            source: Some(source),
            ..
        } => Some(&source.descriptor),
        _ => None,
    };
    // File readers see the source location; datasets only a source anyone may read.
    let published = descriptor.is_some_and(publishable);
    let source = match &version.state {
        BlobVersionState::Reference {
            source,
            cached_metadata,
            ..
        } => Some(SourceView {
            url: public_source_url(&source.descriptor),
            path: source.descriptor.source_path.clone(),
            version: source.descriptor.version_selector.clone(),
            etag: cached_metadata.etag.clone(),
            digests: cached_metadata
                .digests
                .iter()
                .map(|(algorithm, digest)| (*algorithm, digest.clone()))
                .collect(),
            sources: places(source),
        }),
        BlobVersionState::Materialized {
            source: Some(source),
            ..
        }
        | BlobVersionState::PendingContent {
            source: Some(source),
            ..
        } => Some(SourceView {
            url: public_source_url(&source.descriptor),
            path: source.descriptor.source_path.clone(),
            version: source.descriptor.version_selector.clone(),
            etag: None,
            digests: Vec::new(),
            sources: places(source),
        }),
        _ => None,
    };
    let projection = project(
        version.origin.as_ref(),
        version.hop.as_ref(),
        (source.as_ref().filter(|_| published)).and_then(|source| source.url.clone()),
    );
    let content_hash = version.blob_hash().copied();
    OriginView {
        content_hash,
        version_id,
        global,
        created_by: version.created_by,
        created_at: version.created_at,
        origin: version.origin,
        hop: version.hop,
        source,
        projection,
        bucket_metadata: None,
        identifiers: Vec::new(),
        size: None,
    }
}

/// Current files one runs lookup reads at most; a larger scope answers incomplete.
const RUN_SCAN_LIMIT: usize = 5000;
/// Runs one lookup names at most.
const RUN_LIMIT: usize = 50;

/// The runs whose results are current files of a bucket under a prefix, newest first.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BucketRuns {
    pub run_ids: Vec<Ulid>,
    /// Every current file under the prefix was read and no run was left out.
    pub complete: bool,
}

/// The runs that wrote the current files of a bucket under a prefix that `rule` lets the caller
/// read, from their origins.
#[derive(Debug, PartialEq)]
pub struct BucketRunsOperation<R> {
    bucket: String,
    scope: Vec<u8>,
    rule: R,
    runs: BTreeSet<Ulid>,
    after: Option<Key>,
    next: Option<Key>,
    scanned: usize,
    reading: bool,
}

impl<R: KeyRule> BucketRunsOperation<R> {
    pub fn new(bucket: &str, prefix: &str, rule: R) -> Result<Self, OriginError> {
        Ok(Self {
            bucket: bucket.to_string(),
            scope: BlobHeadKey::object_prefix(bucket, prefix).map_err(storage)?,
            rule,
            runs: BTreeSet::new(),
            after: None,
            next: None,
            scanned: 0,
            reading: false,
        })
    }

    fn page(&mut self) -> OriginTurn<BucketRuns> {
        self.reading = false;
        Ok(Next::Call(IoCall::Page {
            space: BLOB_HEAD_KEYSPACE.to_string(),
            prefix: Key::from(self.scope.clone()),
            after: self.after.take(),
            limit: RUN_SCAN_LIMIT.saturating_sub(self.scanned).min(500),
        }))
    }

    fn page_done(&mut self) -> OriginTurn<BucketRuns> {
        let next = self.next.take();
        let complete = match next {
            None => true,
            Some(_) if self.scanned >= RUN_SCAN_LIMIT => false,
            next => {
                self.after = next;
                return self.page();
            }
        };
        let runs = std::mem::take(&mut self.runs);
        Ok(Next::Done(BucketRuns {
            complete: complete && runs.len() <= RUN_LIMIT,
            run_ids: runs.into_iter().rev().take(RUN_LIMIT).collect(),
        }))
    }
}

impl<R: KeyRule> Operation for BucketRunsOperation<R> {
    type Output = BucketRuns;
    type Error = OriginError;

    fn first(&mut self) -> OriginTurn<BucketRuns> {
        self.page()
    }

    fn next(&mut self, result: IoResult) -> OriginTurn<BucketRuns> {
        if self.reading {
            for value in result.values()?.into_iter().flatten() {
                let version = BlobVersion::from_bytes(&value).map_err(storage)?;
                if let Some(VersionOrigin::Job { run_id, .. }) = version.origin {
                    self.runs.insert(run_id);
                }
            }
            return self.page_done();
        }
        let (heads, next) = result.page()?;
        self.scanned += heads.len();
        self.next = next;
        let mut reads = Vec::new();
        for (head, pointer) in heads {
            let key = BlobHeadKey::from_bytes(&head).map_err(storage)?;
            if !self.rule.allows(&key.key) {
                continue;
            }
            let pointer = CurrentVersionPointer::from_bytes(&pointer).map_err(storage)?;
            let row = VersionKey::new(&self.bucket, &key.key, pointer.version_id)
                .to_bytes()
                .map_err(storage)?;
            reads.push((BLOB_VERSIONS_KEYSPACE.to_string(), Key::from(row)));
        }
        if reads.is_empty() {
            return self.page_done();
        }
        self.reading = true;
        Ok(Next::Call(IoCall::Read(reads)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob::migration::misplaced::AnyKey;
    use aruna_core::calls::ChildOutcome;

    #[test]
    fn missing_bucket_reported() {
        let realm_id = RealmId::from_bytes([1; 32]);
        let mut read = ReadOriginOperation::new(realm_id, "b", "k", None);
        read.first().unwrap();
        let missing: Result<BucketInfo, GetBucketError> = Err(GetBucketError::NotFound);
        let answer = read.next(IoResult::Child(ChildOutcome::new(missing)));
        assert_eq!(answer, Err(OriginError::BucketMissing));
    }

    #[test]
    fn absent_head_reported() {
        let mut read = ReadVersionOperation::new("b", "k", None);
        assert!(matches!(read.first(), Ok(Next::Call(IoCall::Read(_)))));
        let answer = read.next(IoResult::Values(vec![None]));
        assert_eq!(answer, Err(OriginError::VersionMissing));
    }

    #[test]
    fn wrong_health_answer() {
        let group_id = Ulid::from_bytes([2; 16]);
        let mut health = SourceHealthOperation::new(group_id, ("b", "k"), None);
        health.first().unwrap();
        health.step = HealthStep::Measure;
        let wrong = Event::Delayed;
        let answer = health.next(IoResult::Event(Box::new(wrong)));
        assert_eq!(answer, Err(storage("no source health answer")));
    }

    #[test]
    fn empty_scope_completes() {
        let mut runs = BucketRunsOperation::new("b", "", AnyKey).unwrap();
        assert!(matches!(runs.first(), Ok(Next::Call(IoCall::Page { .. }))));
        let page = IoResult::Page {
            rows: Vec::new(),
            next: None,
        };
        let expected = BucketRuns {
            run_ids: Vec::new(),
            complete: true,
        };
        assert_eq!(runs.next(page), Ok(Next::Done(expected)));
    }
}
