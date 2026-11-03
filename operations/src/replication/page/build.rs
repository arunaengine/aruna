//! Builds one reference page on the coordinator from queued member jobs: every version, head
//! and source root is read in batches, never one exchange per record.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, ROOT_RECORD_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::blob::{
    BlobHeadKey, BlobVersion, BlobVersionState, BucketInfo, CurrentVersionPointer, VersionKey,
};
use aruna_core::structs::storage::distribution::Publication;
use aruna_core::structs::storage::distribution::source_root::SourceRoot;
use aruna_core::structs::storage::replication::ReplicationItemKind;
use aruna_core::types::{Effects, GroupId, Key, KeySpace, Value};
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;

use super::{PAGE_BYTES, PAGE_RECORDS};
use crate::replication::protocol::VersionReplicationManifest;
use crate::replication::queue::BlobJobRecord;
use crate::replication::version_replication::ReplicateScopeTarget;
use crate::staging::root_record::{hydrated, named_root};

/// What a page does with one queued job.
#[derive(Clone, Debug, PartialEq)]
pub enum Built {
    /// The job's record, in the page.
    Record(Box<VersionReplicationManifest>),
    /// The version is gone, so nothing is owed, as for a single delivery.
    Gone,
    /// Not a reference record; single delivery sends it.
    Single,
    /// Another record of the key or the page bounds hold it back for a later page.
    Later,
}

#[derive(Debug, Error, PartialEq)]
pub enum BuildError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("the source bucket is missing")]
    BucketMissing,
    #[error("unexpected event while building a page: {0}")]
    UnexpectedEvent(String),
    #[error("operation did not finish")]
    NotFinished,
}

/// Queued member jobs of one bucket, member and publication.
#[derive(Clone, Debug, PartialEq)]
pub struct BuildInput {
    pub bucket: String,
    pub publication: Publication,
    pub jobs: Vec<BlobJobRecord>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BuildState {
    Init,
    ReadRows,
    ReadRoots,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct PageBuildOperation {
    input: BuildInput,
    state: BuildState,
    group_id: GroupId,
    versions: Vec<Option<BlobVersion>>,
    heads: Vec<Option<CurrentVersionPointer>>,
    output: Option<Result<Vec<Built>, BuildError>>,
}

impl PageBuildOperation {
    pub fn new(input: BuildInput) -> Self {
        Self {
            input,
            state: BuildState::Init,
            group_id: GroupId::nil(),
            versions: Vec::new(),
            heads: Vec::new(),
            output: None,
        }
    }

    fn rows(&self) -> Result<Vec<(KeySpace, Key)>, ConversionError> {
        let bucket = &self.input.bucket;
        let mut reads = vec![(
            S3_BUCKET_KEYSPACE.to_string(),
            ByteView::from(bucket.as_bytes().to_vec()),
        )];
        for job in &self.input.jobs {
            let (key, version_id) = match &job.input.target {
                ReplicateScopeTarget::Version { key, version_id } => (key.as_str(), *version_id),
                _ => ("", ulid::Ulid::nil()),
            };
            let version = VersionKey::new(bucket, key, version_id).to_bytes()?;
            let head = BlobHeadKey::new(bucket, key).to_bytes()?;
            reads.push((BLOB_VERSIONS_KEYSPACE.to_string(), version.into()));
            reads.push((BLOB_HEAD_KEYSPACE.to_string(), head.into()));
        }
        Ok(reads)
    }

    fn accept_rows(&mut self, values: Vec<(Key, Option<Value>)>) -> Result<Effects, BuildError> {
        if values.len() != 1 + 2 * self.input.jobs.len() {
            return Err(StorageError::ReadError("short page build read".to_string()).into());
        }
        let mut values = values.into_iter().map(|(_, value)| value);
        let bucket = values.next().flatten().ok_or(BuildError::BucketMissing)?;
        self.group_id = BucketInfo::from_bytes(&bucket)?.group_id;
        for _ in 0..self.input.jobs.len() {
            let version = values.next().flatten();
            self.versions
                .push(version.map(|v| BlobVersion::from_bytes(&v)).transpose()?);
            let head = values.next().flatten();
            self.heads.push(
                head.map(|v| CurrentVersionPointer::from_bytes(&v))
                    .transpose()?,
            );
        }
        let digests: BTreeSet<[u8; 32]> = self
            .versions
            .iter()
            .flatten()
            .filter_map(named_root)
            .collect();
        if digests.is_empty() {
            return self.finish(BTreeMap::new());
        }
        self.state = BuildState::ReadRoots;
        let reads = (digests.into_iter())
            .map(|digest| (ROOT_RECORD_KEYSPACE.to_string(), digest.to_vec().into()))
            .collect();
        Ok(smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        })])
    }

    fn accept_roots(&mut self, values: Vec<(Key, Option<Value>)>) -> Result<Effects, BuildError> {
        let mut roots = BTreeMap::new();
        for (key, value) in values {
            let (Ok(digest), Some(value)) = (<[u8; 32]>::try_from(key.as_ref()), value) else {
                continue;
            };
            roots.insert(digest, SourceRoot::from_bytes(&value)?);
        }
        self.finish(roots)
    }

    fn finish(&mut self, roots: BTreeMap<[u8; 32], SourceRoot>) -> Result<Effects, BuildError> {
        let mut keys = BTreeSet::new();
        let mut bytes = 0usize;
        let mut built = Vec::with_capacity(self.input.jobs.len());
        let rows = self.input.jobs.iter().zip(&self.versions).zip(&self.heads);
        for ((job, version), head) in rows {
            let Some(version) = version.clone() else {
                built.push(Built::Gone);
                continue;
            };
            // A compact version needs its root back; without it the single path retries.
            let version = match named_root(&version) {
                Some(digest) => match roots.get(&digest).map(|root| hydrated(version, root)) {
                    Some(Ok(version)) => version,
                    _ => {
                        built.push(Built::Single);
                        continue;
                    }
                },
                None => version,
            };
            let publication = &self.input.publication;
            let Some(manifest) =
                reference_manifest(job, version, head.as_ref(), self.group_id, publication)
            else {
                built.push(Built::Single);
                continue;
            };
            let size = postcard::to_allocvec(&manifest)
                .map_err(ConversionError::from)?
                .len();
            let full = keys.len() == PAGE_RECORDS || bytes.saturating_add(size) > PAGE_BYTES;
            if full || !keys.insert(manifest.key.clone()) {
                built.push(Built::Later);
                continue;
            }
            bytes += size;
            built.push(Built::Record(Box::new(manifest)));
        }
        self.state = BuildState::Finish;
        self.output = Some(Ok(built));
        Ok(smallvec![])
    }
}

/// The manifest a single delivery of `job` sends for a reference version, publishing it to a
/// member under `publication`; `None` for anything else.
pub(crate) fn reference_manifest(
    job: &BlobJobRecord,
    version: BlobVersion,
    head: Option<&CurrentVersionPointer>,
    group_id: GroupId,
    publication: &Publication,
) -> Option<VersionReplicationManifest> {
    let ReplicateScopeTarget::Version { key, version_id } = &job.input.target else {
        return None;
    };
    let BlobVersionState::Reference {
        source,
        cached_metadata,
        advance_count,
        ..
    } = version.state
    else {
        return None;
    };
    let head = head.filter(|head| head.version_id == *version_id);
    let bucket = job.input.bucket.clone();
    Some(VersionReplicationManifest {
        bucket: bucket.clone(),
        key: key.clone(),
        version_id: *version_id,
        group_id,
        kind: ReplicationItemKind::Materialized,
        created_at: version.created_at,
        created_by: version.created_by,
        current_version: head.is_some(),
        current_version_generation: head.map(|head| head.generation),
        auth_context: job.input.auth_context.clone(),
        blob: None,
        // Members of a set read the source themselves; connector ids are node-local.
        source: Some(
            aruna_core::structs::execution::staging::VersionSourceBinding {
                connector_id: None,
                ..source
            },
        ),
        multipart: None,
        reference_intent: true,
        origin: None,
        upstream_sources: Vec::new(),
        writer_auth_context: None,
        reference_metadata: Some(cached_metadata),
        metadata: version.metadata,
        reference_advance: None,
        reference_advance_count: Some(advance_count),
        placement_policies: version.placement_policies,
        original_epoch: version.original_epoch,
        version_generation: version.generation,
        logical_identity: version.identity,
        distribution: Some(publication.clone()),
        version_origin: version.origin,
        version_hop: version.hop,
        sync_source: Some((bucket, key.clone())),
    })
}

impl Operation for PageBuildOperation {
    type Output = Vec<Built>;
    type Error = BuildError;

    fn start(&mut self) -> Effects {
        match self.rows() {
            Ok(reads) => {
                self.state = BuildState::ReadRows;
                smallvec![Effect::Storage(StorageEffect::BatchRead {
                    reads,
                    txn_id: None,
                })]
            }
            Err(error) => {
                self.state = BuildState::Error;
                self.output = Some(Err(error.into()));
                smallvec![]
            }
        }
    }

    fn step(&mut self, event: Event) -> Effects {
        let result = match (self.state, event) {
            (BuildState::ReadRows, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                self.accept_rows(values)
            }
            (BuildState::ReadRoots, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                self.accept_roots(values)
            }
            (BuildState::Finish | BuildState::Error, _) => return smallvec![],
            (_, Event::Storage(StorageEvent::Error { error })) => Err(error.into()),
            (_, event) => Err(BuildError::UnexpectedEvent(format!("{event:?}"))),
        };
        result.unwrap_or_else(|error| {
            self.state = BuildState::Error;
            self.output = Some(Err(error));
            smallvec![]
        })
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, BuildState::Finish | BuildState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(BuildError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
