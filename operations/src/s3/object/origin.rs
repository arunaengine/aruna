//! Reads where one file version came from: its origin record, its sync hop, the external
//! source it is bound to, and what a dataset may say about it. One point read per version.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::SystemTime;

use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE};
use aruna_core::structs::checksum::ChecksumAlgorithm;
use aruna_core::structs::execution::source_access::SourceDigest;
use aruna_core::structs::identity::database::DatabaseTarget;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{
    BlobHeadKey, BlobVersion, BlobVersionState, CurrentVersionPointer, VersionKey,
};
use aruna_core::structs::storage::global_id::GlobalId;
use aruna_core::structs::storage::origin::{
    OriginProjection, SyncHop, VersionOrigin, project, public_source_url,
};
use aruna_core::{NodeId, UserId};
use thiserror::Error;
use ulid::Ulid;

use crate::driver::{DriverContext, drive};
use crate::realm::get_config::GetConfigOperation;
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};

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
    pub etag: Option<String>,
    pub digests: Vec<(ChecksumAlgorithm, SourceDigest)>,
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
}

/// Reads the origin of `version_id`, or of the current version, of a local bucket's key.
pub async fn read_origin(
    context: &DriverContext,
    (realm_id, node_id): (RealmId, NodeId),
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
    let global = info.distribution.as_ref().map(|_| GlobalId::Version {
        realm_id,
        version_id,
    });
    let mut view = view(version_id, global, version);
    // A source origin names the bucket's database entry when the bucket is listed.
    if let Some(VersionOrigin::Source { database }) = view.origin.as_mut()
        && database.is_none()
        && let Ok(config) = drive(GetConfigOperation::new(realm_id), context).await
    {
        *database = config
            .databases
            .iter()
            .find(|entry| match &entry.target {
                DatabaseTarget::Distributed { set_id } => info
                    .distribution
                    .as_ref()
                    .is_some_and(|binding| binding.set_id == *set_id),
                DatabaseTarget::Ordinary {
                    node_id: entry_node,
                    bucket: name,
                } => *entry_node == node_id && name == bucket,
            })
            .map(|entry| entry.entry_id);
    }
    Ok(view)
}

fn view(version_id: Ulid, global: Option<GlobalId>, version: BlobVersion) -> OriginView {
    let source = match &version.state {
        BlobVersionState::Reference {
            source,
            cached_metadata,
            ..
        } => Some(SourceView {
            url: public_source_url(&source.descriptor),
            path: source.descriptor.source_path.clone(),
            etag: cached_metadata.etag.clone(),
            digests: cached_metadata
                .digests
                .iter()
                .map(|(algorithm, digest)| (*algorithm, digest.clone()))
                .collect(),
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
            etag: None,
            digests: Vec::new(),
        }),
        _ => None,
    };
    let projection = project(
        version.origin.as_ref(),
        version.hop.as_ref(),
        source.as_ref().and_then(|source| source.url.clone()),
    );
    OriginView {
        version_id,
        global,
        created_by: version.created_by,
        created_at: version.created_at,
        origin: version.origin,
        hop: version.hop,
        source,
        projection,
    }
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
