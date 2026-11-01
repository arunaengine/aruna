//! Reads where one file version came from: its origin record, its sync hop, the external
//! source it is bound to, and what a dataset may say about it. One point read per version.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::SystemTime;

use aruna_core::effects::{Effect, StagingSourceEffect, StorageEffect};
use aruna_core::events::{Event, StagingSourceEvent, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, SOURCE_INDEX_KEYSPACE,
};
use aruna_core::structs::checksum::ChecksumAlgorithm;
use aruna_core::structs::execution::source_access::{SourceDigest, SourceStats};
use aruna_core::structs::execution::source_connector::SourceConnector;
use aruna_core::structs::execution::staging::VersionSourceBinding;
use aruna_core::structs::identity::identifier::{CompiledParser, Identifier};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{
    BackendLocation, BlobHeadKey, BlobVersion, BlobVersionState, CurrentVersionPointer, VersionKey,
};
use aruna_core::structs::storage::bucket_metadata::BucketMetadata;
use aruna_core::structs::storage::global_id::GlobalId;
use aruna_core::structs::storage::origin::{
    OriginProjection, SyncHop, VersionOrigin, project, public_source_url, publishable,
};
use aruna_core::types::GroupId;
use aruna_core::{NodeId, UserId};
use thiserror::Error;
use ulid::Ulid;

use crate::connectors::repository::source_connector_key;
use crate::connectors::resolver::{ResolveBindingInput, ResolveBindingOperation};
use crate::driver::{DriverContext, drive};
use crate::effect_adapters::dispatch_effect;
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use crate::s3::bucket::metadata::bucket_metadata;

#[derive(Debug, Error)]
pub enum OriginError {
    #[error("bucket not found")]
    BucketMissing,
    #[error("object version not found")]
    VersionMissing,
    #[error("{0}")]
    Storage(String),
}

/// The external source a version is bound to, as readers of the file see it.
#[derive(Clone, Debug)]
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
#[derive(Clone, Debug)]
pub struct SourceHealth {
    pub place: SourcePlace,
    /// `None` when this node cannot resolve the source, as it lacks its connector.
    pub stats: Option<SourceStats>,
}

/// This node's measurements of every source of `version_id`, or of the current version, in
/// the binding's order; empty for a version without an external source.
pub async fn source_health(
    context: &DriverContext,
    group_id: GroupId,
    (bucket, key): (&str, &str),
    version_id: Option<Ulid>,
) -> Result<Vec<SourceHealth>, OriginError> {
    let (_, version) = read_version(context, bucket, key, version_id).await?;
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
        _ => return Ok(Vec::new()),
    };
    let mut sources = Vec::new();
    for single in (0..binding.source_count()).filter_map(|slot| binding.source_at(slot)) {
        let input = ResolveBindingInput {
            source: single.clone(),
            group_id: Some(group_id),
        };
        let access = drive(ResolveBindingOperation::new(input), context)
            .await
            .ok();
        sources.push((place(&single), access));
    }
    let accesses: Vec<_> = sources
        .iter()
        .filter_map(|(_, access)| access.clone())
        .collect();
    let mut stats = match accesses.is_empty() {
        true => Vec::new(),
        false => {
            let health = Effect::StagingSource(StagingSourceEffect::Health { accesses });
            match dispatch_effect(health, context, 0).await {
                Event::StagingSource(StagingSourceEvent::HealthResult { stats, .. }) => stats,
                _ => return Err(OriginError::Storage("no source health answer".to_string())),
            }
        }
    }
    .into_iter();
    Ok(sources
        .into_iter()
        .map(|(place, access)| SourceHealth {
            place,
            stats: access.and_then(|_| stats.next()),
        })
        .collect())
}

/// Everything file readers see about one version's origin.
#[derive(Clone, Debug)]
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

/// Reads the origin of `version_id`, or of the current version, of a local bucket's key.
pub async fn read_origin(
    context: &DriverContext,
    (realm_id, _node_id): (RealmId, NodeId),
    bucket: &str,
    key: &str,
    version_id: Option<Ulid>,
) -> Result<OriginView, OriginError> {
    let info = drive(GetBucketOperation::new(bucket.to_string()), context)
        .await
        .map_err(|error| match error {
            GetBucketError::NotFound => OriginError::BucketMissing,
            other => OriginError::Storage(other.to_string()),
        })?;
    let (version_id, version) = read_version(context, bucket, key, version_id).await?;
    let global = info.distribution.as_ref().map(|_| GlobalId::Version {
        realm_id,
        version_id,
    });
    let metadata = bucket_metadata(context, bucket, &info)
        .await
        .map_err(OriginError::Storage)?;
    let parser = (metadata.as_ref())
        .and_then(|metadata| metadata.parser.as_ref())
        .and_then(|parser| CompiledParser::compile(parser).ok());
    let size = version_size(context, &version).await?;
    let mut view = view(version_id, global, version);
    view.size = size;
    for place in view
        .source
        .iter_mut()
        .flat_map(|source| source.sources.iter_mut())
    {
        place.connector_name = connector_name(context, info.group_id, place.connector_id).await?;
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
pub const RUN_SCAN_LIMIT: usize = 5000;
/// Runs one lookup names at most.
pub const RUN_LIMIT: usize = 50;

/// The runs whose results are current files of a bucket under a prefix, newest first.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BucketRuns {
    pub run_ids: Vec<Ulid>,
    /// Every current file under the prefix was read and no run was left out.
    pub complete: bool,
}

/// The runs that wrote the current files of `bucket` under `prefix`, from their origins.
pub async fn bucket_runs(
    context: &DriverContext,
    bucket: &str,
    prefix: &str,
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
            other => return Err(OriginError::Storage(format!("{other:?}"))),
        };
        scanned += heads.len();
        for (head, pointer) in heads {
            let key = BlobHeadKey::from_bytes(&head).map_err(storage)?;
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
    let version = crate::staging::root_record::hydrate_version(context, version)
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
        other => Err(OriginError::Storage(format!("{other:?}"))),
    }
}

#[cfg(test)]
#[path = "origin_tests.rs"]
mod tests;
