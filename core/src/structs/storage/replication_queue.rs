//! Stored blob replication jobs and live copy obligations: rows, keys, the scope a job copies
//! and the jobs a sync relationship derives. The queue rows and the doctor share these formats.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::NodeId;
use crate::effects::{Effect, StorageEffect};
use crate::errors::ConversionError;
use crate::keyspaces::{REPLICATION_JOB_KEYSPACE, REPLICATION_OBLIGATION_KEYSPACE};
use crate::structs::identity::auth::AuthContext;
use crate::structs::storage::replication::ArunaArn;
use crate::structs::{SyncMode, SyncRelationship, SyncState, sync_relationship_key};
use crate::time::unix_timestamp_millis;
use crate::types::Key;
use byteview::ByteView;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ReplicationMode {
    Live,
    OnDemand,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReferenceAdvance {
    pub generation: u64,
    pub predecessor: Ulid,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SyncOrigin {
    pub relationship_id: Ulid,
    pub hop_count: u8,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ReplicateScopeTarget {
    Bucket,
    Prefix(String),
    Object { key: String },
    Version { key: String, version_id: Ulid },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicateScopeInput {
    pub bucket: String,
    pub target: ReplicateScopeTarget,
    pub target_node_id: NodeId,
    pub auth_context: AuthContext,
    pub replicate_delete_markers: bool,
    pub mode: ReplicationMode,
}

/// The member of a distributed bucket a queued job publishes to.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct MemberTarget {
    pub set_id: Ulid,
    pub member_id: Ulid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobJobRecord {
    pub input: ReplicateScopeInput,
    pub source_delete_marker: Option<bool>,
    pub due_at_ms: u64,
    pub attempts: u32,
    pub last_error: Option<String>,
    pub relationship_id: Option<Ulid>,
    pub enqueued_at_ms: u64,
    pub origin: Option<SyncOrigin>,
    pub upstream_sources: Vec<ArunaArn>,
    pub writer_auth_context: Option<AuthContext>,
    pub reference_advance: Option<ReferenceAdvance>,
    /// A coordinator's publication to one member of a distributed bucket.
    pub distribution: Option<MemberTarget>,
    /// Names this queued job until it retires: kept by retries and parking, new when an enqueue
    /// writes the row again. Not part of the job key.
    pub job_id: Ulid,
}

#[derive(Serialize)]
struct BlobJobIdentity<'a> {
    mode: ReplicationMode,
    bucket: &'a str,
    target: &'a ReplicateScopeTarget,
    target_node_id: &'a NodeId,
    source_delete_marker: Option<bool>,
    relationship_id: Option<Ulid>,
    origin: Option<&'a SyncOrigin>,
    upstream_sources: &'a [ArunaArn],
    member: Option<&'a MemberTarget>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveReplicationContinuation {
    pub relationship_after: Option<Vec<u8>>,
    pub relationships_complete: bool,
    /// The publications to the members of a distributed bucket are queued.
    pub members_done: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveObligationRecord {
    pub local_node_id: NodeId,
    pub auth_context: AuthContext,
    pub bucket: String,
    pub key: String,
    pub version_id: Ulid,
    pub delete_marker: bool,
    pub origin: Option<SyncOrigin>,
    pub upstream_sources: Vec<ArunaArn>,
    pub continuation: Option<LiveReplicationContinuation>,
    pub reference_advance: Option<ReferenceAdvance>,
}

#[derive(Serialize)]
struct LiveObligationIdentity<'a> {
    bucket: &'a str,
    key: &'a str,
    version_id: Ulid,
}

impl BlobJobRecord {
    pub fn new(
        input: ReplicateScopeInput,
        source_delete_marker: Option<bool>,
        due_at_ms: u64,
    ) -> Self {
        let writer_auth_context = Some(input.auth_context.clone());
        Self {
            input,
            source_delete_marker,
            due_at_ms,
            attempts: 0,
            last_error: None,
            relationship_id: None,
            enqueued_at_ms: due_at_ms,
            origin: None,
            upstream_sources: Vec::new(),
            writer_auth_context,
            reference_advance: None,
            distribution: None,
            job_id: Ulid::generate(),
        }
    }

    pub fn new_relationship(
        input: ReplicateScopeInput,
        source_delete_marker: Option<bool>,
        relationship_id: Ulid,
        due_at_ms: u64,
    ) -> Self {
        Self {
            relationship_id: Some(relationship_id),
            ..Self::new(input, source_delete_marker, due_at_ms)
        }
    }

    pub fn with_origin(mut self, origin: Option<SyncOrigin>) -> Self {
        self.origin = origin;
        self
    }

    pub fn with_sources(mut self, upstream_sources: Vec<ArunaArn>) -> Self {
        self.upstream_sources = upstream_sources;
        self
    }

    pub fn with_writer_auth(mut self, auth_context: AuthContext) -> Self {
        self.writer_auth_context = Some(auth_context);
        self
    }

    pub fn with_reference_advance(mut self, advance: ReferenceAdvance) -> Self {
        self.reference_advance = Some(advance);
        self
    }

    pub fn with_member(mut self, target: MemberTarget) -> Self {
        self.distribution = Some(target);
        self
    }

    /// When the version this job delivers was written, which dates its obligation; the
    /// enqueue time for other jobs.
    pub fn owed_since_ms(&self) -> u64 {
        match &self.input.target {
            ReplicateScopeTarget::Version { version_id, .. } => version_id.timestamp_ms(),
            _ => self.enqueued_at_ms,
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        let record: Self = match postcard::take_from_bytes(bytes) {
            Ok((record, [])) => record,
            Ok(_) => return Err(postcard::Error::DeserializeBadEncoding.into()),
            Err(error) => return Err(error.into()),
        };
        if record.writer_auth_context.is_none() && record.reference_advance.is_none() {
            return Err(ConversionError::FromStrError(
                "replication job is missing its source writer".to_string(),
            ));
        }
        Ok(record)
    }
}

impl LiveObligationRecord {
    pub fn new(
        local_node_id: NodeId,
        auth_context: AuthContext,
        bucket: String,
        key: String,
        version_id: Ulid,
        delete_marker: bool,
    ) -> Self {
        Self {
            local_node_id,
            auth_context,
            bucket,
            key,
            version_id,
            delete_marker,
            origin: None,
            upstream_sources: Vec::new(),
            continuation: None,
            reference_advance: None,
        }
    }

    pub fn with_origin(mut self, origin: Option<SyncOrigin>) -> Self {
        self.origin = origin;
        self
    }

    pub fn with_sources(mut self, upstream_sources: Vec<ArunaArn>) -> Self {
        self.upstream_sources = upstream_sources;
        self
    }

    pub fn with_reference_advance(mut self, advance: ReferenceAdvance) -> Self {
        self.reference_advance = Some(advance);
        self
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        match postcard::take_from_bytes(bytes) {
            Ok((record, [])) => Ok(record),
            Ok(_) => Err(postcard::Error::DeserializeBadEncoding.into()),
            Err(error) => Err(error.into()),
        }
    }
}

pub fn blob_job_key(record: &BlobJobRecord) -> Result<Key, ConversionError> {
    let identity = BlobJobIdentity {
        mode: record.input.mode,
        bucket: &record.input.bucket,
        target: &record.input.target,
        target_node_id: &record.input.target_node_id,
        source_delete_marker: record.source_delete_marker,
        relationship_id: record.relationship_id,
        origin: record.origin.as_ref(),
        upstream_sources: &record.upstream_sources,
        member: record.distribution.as_ref(),
    };
    let mut key = b"v1".to_vec();
    key.extend(postcard::to_allocvec(&identity)?);
    Ok(ByteView::from(key))
}

/// Key prefixes of the jobs and live obligations queued for `bucket`.
pub fn queued_prefixes(bucket: &str) -> Result<[(&'static str, Vec<u8>); 3], ConversionError> {
    let key = |identity: Vec<u8>| [b"v1".to_vec(), identity].concat();
    Ok([
        (
            REPLICATION_JOB_KEYSPACE,
            key(postcard::to_allocvec(&(ReplicationMode::Live, bucket))?),
        ),
        (
            REPLICATION_JOB_KEYSPACE,
            key(postcard::to_allocvec(&(ReplicationMode::OnDemand, bucket))?),
        ),
        (
            REPLICATION_OBLIGATION_KEYSPACE,
            key(postcard::to_allocvec(&bucket)?),
        ),
    ])
}

pub fn blob_job_entry(record: &BlobJobRecord) -> Result<(String, Key, ByteView), ConversionError> {
    Ok((
        REPLICATION_JOB_KEYSPACE.to_string(),
        blob_job_key(record)?,
        ByteView::from(record.to_bytes()?),
    ))
}

pub fn blob_job_preferred(candidate: &BlobJobRecord, current: &BlobJobRecord) -> bool {
    (candidate.attempts, candidate.due_at_ms) > (current.attempts, current.due_at_ms)
}

pub fn live_obligation_key(record: &LiveObligationRecord) -> Result<Key, ConversionError> {
    let identity = LiveObligationIdentity {
        bucket: &record.bucket,
        key: &record.key,
        version_id: record.version_id,
    };
    let mut key = b"v1".to_vec();
    key.extend(postcard::to_allocvec(&identity)?);
    Ok(ByteView::from(key))
}

pub fn live_obligation_entry(
    record: &LiveObligationRecord,
) -> Result<(String, Key, ByteView), ConversionError> {
    Ok((
        REPLICATION_OBLIGATION_KEYSPACE.to_string(),
        live_obligation_key(record)?,
        ByteView::from(record.to_bytes()?),
    ))
}

pub fn live_obligation_effect(
    record: LiveObligationRecord,
    txn_id: Option<Ulid>,
) -> Result<Effect, ConversionError> {
    let (key_space, key, value) = live_obligation_entry(&record)?;
    Ok(Effect::Storage(StorageEffect::Write {
        key_space,
        key,
        value,
        txn_id,
    }))
}

pub fn build_live_obligation(
    local_node_id: NodeId,
    auth_context: AuthContext,
    bucket: String,
    key: String,
    version_id: Ulid,
    delete_marker: bool,
    txn_id: Option<Ulid>,
) -> Result<Effect, ConversionError> {
    let record = LiveObligationRecord::new(
        local_node_id,
        auth_context,
        bucket,
        key,
        version_id,
        delete_marker,
    );
    live_obligation_effect(record, txn_id)
}

/// Identifies the object version a replication job is being derived for.
pub struct RelationshipJobTarget<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
    pub version_id: Ulid,
    pub delete_marker: bool,
}

pub fn relationship_job(
    local_node_id: NodeId,
    target: RelationshipJobTarget<'_>,
    relationship: SyncRelationship,
    inbound_origin: Option<&SyncOrigin>,
    upstream_sources: &[ArunaArn],
) -> Option<BlobJobRecord> {
    let RelationshipJobTarget {
        bucket,
        key,
        version_id,
        delete_marker,
    } = target;
    if !matches!(
        relationship.mode,
        SyncMode::Continuous | SyncMode::Reference
    ) || relationship.state != SyncState::Enabled
        || relationship.source.node_id != local_node_id
        || relationship.source.bucket() != Some(bucket)
        || relationship
            .source
            .key_prefix()
            .is_some_and(|prefix| !key.starts_with(prefix))
        || (delete_marker && !relationship.replicate_deletes)
        || inbound_origin.is_some_and(|origin| origin.hop_count >= 4)
        || upstream_sources
            .iter()
            .any(|source| reverse_target(&relationship, source))
    {
        return None;
    }

    let now_ms = unix_timestamp_millis();
    let origin = Some(match inbound_origin {
        Some(origin) => SyncOrigin {
            relationship_id: relationship.id,
            hop_count: origin.hop_count.saturating_add(1),
        },
        None => SyncOrigin {
            relationship_id: relationship.id,
            hop_count: 0,
        },
    });
    let mut next_sources = upstream_sources.to_vec();
    if !next_sources
        .iter()
        .any(|source| same_endpoint(source, &relationship.source))
    {
        next_sources.push(relationship.source.clone());
    }
    Some(
        BlobJobRecord::new_relationship(
            ReplicateScopeInput {
                bucket: bucket.to_string(),
                target: ReplicateScopeTarget::Version {
                    key: key.to_string(),
                    version_id,
                },
                target_node_id: relationship.target.node_id,
                auth_context: AuthContext {
                    user_id: relationship.created_by,
                    realm_id: relationship.source.realm_id,
                    path_restrictions: None,
                    session: None,
                },
                replicate_delete_markers: relationship.replicate_deletes,
                mode: ReplicationMode::Live,
            },
            Some(delete_marker),
            relationship.id,
            now_ms,
        )
        .with_origin(origin)
        .with_sources(next_sources),
    )
}

fn reverse_target(relationship: &SyncRelationship, source: &ArunaArn) -> bool {
    same_endpoint(&relationship.target, source)
}

pub fn same_endpoint(left: &ArunaArn, right: &ArunaArn) -> bool {
    left == right
}

pub fn validate_sync_key(
    bucket: &str,
    key: &Key,
    relationship: &SyncRelationship,
) -> Result<(), ConversionError> {
    if key.as_ref() != sync_relationship_key(bucket, relationship.id).as_slice() {
        return Err(ConversionError::FromStrError(
            "sync relationship key does not match payload".to_string(),
        ));
    }
    Ok(())
}

pub fn continuation_newer(
    candidate: &LiveReplicationContinuation,
    current: &LiveReplicationContinuation,
) -> bool {
    if candidate.relationships_complete != current.relationships_complete {
        return candidate.relationships_complete;
    }
    if candidate.relationships_complete {
        return false;
    }
    match (&candidate.relationship_after, &current.relationship_after) {
        (Some(candidate), Some(current)) => candidate > current,
        (Some(_), None) => true,
        (None, Some(_)) | (None, None) => false,
    }
}

pub fn merge_due_job(
    jobs: &mut Vec<(Vec<u8>, BlobJobRecord)>,
    key: Vec<u8>,
    job: BlobJobRecord,
    (repaired, limit): (bool, usize),
) -> bool {
    let existing = match repaired {
        true => (jobs.iter_mut()).find(|(existing_key, _)| *existing_key == key),
        false => None,
    };
    if let Some((_, existing)) = existing {
        if blob_job_preferred(&job, existing) {
            *existing = job;
        }
        false
    } else {
        jobs.push((key, job));
        jobs.len() >= limit
    }
}
