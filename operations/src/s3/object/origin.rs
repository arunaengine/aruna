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
    let source_url = view
        .source
        .as_ref()
        .and_then(|source| source.url.as_deref());
    view.identifiers = parser.map_or_else(Vec::new, |parser| parser.find(key, source_url));
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
    Ok(view)
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

/// Reads `version_id`, or the current version, of a local bucket's key, with its source root.
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

/// The runs that wrote the current files of `bucket` under `prefix` that are `readable` to the
/// caller, from their origins.
pub async fn bucket_runs(
    context: &DriverContext,
    bucket: &str,
    prefix: &str,
    readable: impl Fn(&str) -> bool,
) -> Result<BucketRuns, OriginError> {
    let storage =
        |error: aruna_core::errors::ConversionError| OriginError::Storage(error.to_string());
    let scope = BlobHeadKey::object_prefix(bucket, prefix).map_err(storage)?;
    let (mut runs, mut after, mut scanned) = (std::collections::BTreeSet::new(), None, 0);
    let complete = loop {
        let page = RUN_SCAN_LIMIT.saturating_sub(scanned).min(500);
        let iter = StorageEffect::Iter {
            key_space: BLOB_HEAD_KEYSPACE.to_string(),
            prefix: Some(scope.clone().into()),
            start: after.take().map(aruna_core::effects::IterStart::After),
            limit: page,
            txn_id: None,
        };
        let (heads, next) = match context.storage_handle.send_storage_effect(iter).await {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => (values, next_start_after),
            other => return Err(OriginError::Storage(other.label().to_string())),
        };
        scanned += heads.len();
        for (head, pointer) in heads {
            let key = BlobHeadKey::from_bytes(&head).map_err(storage)?;
            if !readable(&key.key) {
                continue;
            }
            let pointer = CurrentVersionPointer::from_bytes(&pointer).map_err(storage)?;
            let row = VersionKey::new(bucket, &key.key, pointer.version_id).to_bytes();
            let Some(value) = read(context, BLOB_VERSIONS_KEYSPACE, row.map_err(storage)?).await?
            else {
                continue;
            };
            let version = BlobVersion::from_bytes(&value).map_err(storage)?;
            if let Some(VersionOrigin::Job { run_id, .. }) = version.origin {
                runs.insert(run_id);
            }
        }
        match next {
            None => break true,
            Some(_) if scanned >= RUN_SCAN_LIMIT => break false,
            next => after = next,
        }
    };
    let complete = complete && runs.len() <= RUN_LIMIT;
    let run_ids = runs.into_iter().rev().take(RUN_LIMIT).collect();
    Ok(BucketRuns { run_ids, complete })
}

async fn read_version(
    context: &DriverContext,
    bucket: &str,
    key: &str,
    version_id: Option<Ulid>,
) -> Result<(Ulid, BlobVersion), OriginError> {
    let version_id = match version_id {
        Some(version_id) => version_id,
        None => {
            let head = BlobHeadKey::new(bucket, key)
                .to_bytes()
                .map_err(|error| OriginError::Storage(error.to_string()))?;
            let value = read(context, BLOB_HEAD_KEYSPACE, head)
                .await?
                .ok_or(OriginError::VersionMissing)?;
            CurrentVersionPointer::from_bytes(&value)
                .map_err(|error| OriginError::Storage(error.to_string()))?
                .version_id
        }
    };
    let row = VersionKey::new(bucket, key, version_id)
        .to_bytes()
        .map_err(|error| OriginError::Storage(error.to_string()))?;
    let value = read(context, BLOB_VERSIONS_KEYSPACE, row)
        .await?
        .ok_or(OriginError::VersionMissing)?;
    let version =
        BlobVersion::from_bytes(&value).map_err(|error| OriginError::Storage(error.to_string()))?;
    // The source's public address comes from the root this node keeps once.
    let version = hydrate_version(context, version)
        .await
        .map_err(OriginError::Storage)?;
    Ok((version_id, version))
}

/// The size of one version: its blob location row, or a reference's cached observation.
async fn version_size(
    context: &DriverContext,
    version: &BlobVersion,
) -> Result<Option<u64>, OriginError> {
    if let BlobVersionState::Reference {
        cached_metadata, ..
    } = &version.state
    {
        return Ok(Some(cached_metadata.content_length));
    }
    let Some(location) = version.location_key() else {
        return Ok(None);
    };
    let Some(value) = read(context, BLOB_LOCATIONS_KEYSPACE, location.to_bytes()).await? else {
        return Ok(None);
    };
    let location = BackendLocation::from_bytes(&value)
        .map_err(|error| OriginError::Storage(error.to_string()))?;
    Ok(Some(location.blob_size))
}

/// The display name of a connector of the bucket's group on this node, when it is here.
async fn connector_name(
    context: &DriverContext,
    group_id: GroupId,
    connector_id: Option<Ulid>,
) -> Result<Option<String>, OriginError> {
    let Some(connector_id) = connector_id else {
        return Ok(None);
    };
    let key = source_connector_key(group_id, connector_id).to_vec();
    let Some(value) = read(context, SOURCE_INDEX_KEYSPACE, key).await? else {
        return Ok(None);
    };
    SourceConnector::from_bytes(&value)
        .map(|connector| Some(connector.name))
        .map_err(|error| OriginError::Storage(error.to_string()))
}

async fn read(
    context: &DriverContext,
    key_space: &str,
    key: Vec<u8>,
) -> Result<Option<Vec<u8>>, OriginError> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: key_space.to_string(),
            key: key.into(),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => {
            Ok(value.map(|value| value.to_vec()))
        }
        Event::Storage(StorageEvent::Error { error }) => {
            Err(OriginError::Storage(error.to_string()))
        }
        other => Err(OriginError::Storage(other.label().to_string())),
    }
}

/// Puts the root's settings back into a version read with a compact binding.
async fn hydrate_version(
    context: &DriverContext,
    version: BlobVersion,
) -> Result<BlobVersion, String> {
    let Some(digest) = named_root(&version) else {
        return Ok(version);
    };
    let read = read_root(digest, None);
    let event = context.storage_handle.send_storage_effect(read).await;
    let root = parse_root(event).map_err(|error| error.to_string())?;
    let root = root.ok_or_else(|| "the source root record is missing".to_string())?;
    hydrated(version, &root)
}

#[cfg(test)]
#[path = "origin_tests.rs"]
mod tests;
