//! Reads the bytes of a distributed version this member keeps the record of but no copy: from a
//! holder of the version's range, verified by bao against its BLAKE3 and read under the
//! caller's authority on that holder, then kept as the member's data strategy says.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::ops::Range;
use std::time::SystemTime;

use aruna_core::NodeId;
use aruna_core::effects::{BlobEffect, StorageEffect};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{
    ATTACHMENT_KEYSPACE, BUCKET_ENCRYPTION_KEYSPACE, CACHE_ENTRY_KEYSPACE, DISTRIBUTION_KEYSPACE,
    S3_BUCKET_KEYSPACE,
};
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::checksum::HASH_BLAKE3;
use aruna_core::structs::execution::source_connector::SourceConnectorKind;
use aruna_core::structs::execution::staging::VersionSourceBinding;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::placement::policy::PlacementPolicyRef;
use aruna_core::structs::storage::blob::{BackendLocation, BucketInfo};
use aruna_core::structs::storage::distribution::on_use::{
    Attachment, CacheEntry, PullPlan, attachment_key,
};
use aruna_core::structs::storage::distribution::{
    DataStrategy, DistributionView, MemberState, RangeBehaviour,
};
use aruna_core::structs::storage::encryption::BucketEncryption;
use aruna_core::structs::storage::multipart::MultipartChecksumType;
use aruna_core::types::{Key, KeySpace, Value};
use bytes::Bytes;
use ulid::Ulid;

use crate::connectors::resolver::NATIVE_SET_ID;
use crate::driver::DriverContext;
use crate::replication::bao_read::{BaoReadOutput, managed_read};
use crate::replication::protocol::{BaoReadRequest, BaoReadTarget};
use crate::s3::object::get::{
    GetObjectError, GetObjectResult, HolderFailures, ObjectInfo, ObjectRangeRequest,
};

type Blob = BackendStream<Result<Bytes, StreamError>>;

/// A distributed version whose record this member holds without its bytes.
#[derive(Clone, Debug, PartialEq)]
pub struct HeldElsewhere {
    pub set_id: Ulid,
    pub version_id: Ulid,
    pub blake3: [u8; 32],
    pub size: u64,
    pub metadata: HashMap<String, String>,
    pub version_created_at: Option<SystemTime>,
    pub source_policies: Vec<PlacementPolicyRef>,
}

impl HeldElsewhere {
    /// The set and BLAKE3 of a record-only version a member of a distributed bucket received.
    pub fn of(
        source: &VersionSourceBinding,
        identity: Option<[u8; 32]>,
    ) -> Option<(Ulid, [u8; 32])> {
        if source.descriptor.kind != SourceConnectorKind::ArunaNative {
            return None;
        }
        let set_id = source.descriptor.public_config.get(NATIVE_SET_ID)?;
        Some((Ulid::from_string(set_id).ok()?, identity?))
    }

    /// The whole-object facts the record gives without any byte.
    pub fn info(&self) -> ObjectInfo {
        ObjectInfo {
            size: self.size,
            version_created_at: self.version_created_at,
            etag: None,
            checksum_type: MultipartChecksumType::FullObject,
            hashes: HashMap::from([(HASH_BLAKE3.to_string(), self.blake3.to_vec())]),
            composite_hashes: HashMap::new(),
            part_count: None,
        }
    }
}

/// The reads of the copies this member may keep of `held`: its attachment, its cache entry.
pub fn kept_reads(held: &HeldElsewhere) -> Vec<(KeySpace, Key)> {
    vec![
        (
            ATTACHMENT_KEYSPACE.to_string(),
            attachment_key(held.set_id, held.version_id).into(),
        ),
        (
            CACHE_ENTRY_KEYSPACE.to_string(),
            held.blake3.to_vec().into(),
        ),
    ]
}

/// The kept copy a read uses: one whose BLAKE3 and size are the version's.
pub fn kept_location(
    held: &HeldElsewhere,
    values: &[(Key, Option<Value>)],
) -> Option<BackendLocation> {
    let fits = |location: &BackendLocation, blake3: [u8; 32]| {
        blake3 == held.blake3 && location.blob_size == held.size
    };
    let attached = values
        .first()
        .and_then(|(_, value)| Attachment::from_bytes(value.as_deref()?).ok())
        .filter(|attached| fits(&attached.location, attached.blake3))
        .map(|attached| attached.location);
    attached.or_else(|| {
        values
            .get(1)
            .and_then(|(_, value)| CacheEntry::from_bytes(value.as_deref()?).ok())
            .filter(|entry| fits(&entry.location, entry.blake3))
            .map(|entry| entry.location)
    })
}

/// One read through the holders, with the caller it is read for.
#[derive(Clone, Debug, PartialEq)]
pub struct PullRequest {
    pub bucket: String,
    pub key: String,
    pub range: Option<ObjectRangeRequest>,
    /// The caller; every holder checks it on its own path.
    pub auth: AuthContext,
    pub held: HeldElsewhere,
}

/// How this member answers reads of the version's range.
#[derive(Clone, Debug, PartialEq)]
struct Member {
    strategy: DataStrategy,
    behaviour: RangeBehaviour,
    /// An encrypting bucket keeps nothing in plain text, so it only streams.
    keeping: bool,
    holders: Vec<NodeId>,
}

/// Reads the version through a holder per this member's strategy and range behaviour. No
/// reachable holder is an unavailable error, never absence.
pub async fn pull_through(
    context: &DriverContext,
    request: PullRequest,
) -> Result<GetObjectResult, GetObjectError> {
    let member = member_of(context, &request).await?;
    let resolved = request
        .range
        .as_ref()
        .map(|range| range.resolve(request.held.size))
        .transpose()?;
    let range = resolved.as_ref().map(|resolved| resolved.range.clone());
    let plan = PullPlan::choose(
        member.strategy,
        member.behaviour,
        range.is_some(),
        member.keeping,
    );
    let blob = match plan {
        PullPlan::Stream => from_holders(context, &request, &member.holders, range).await?,
        PullPlan::SpoolFirst => {
            let whole = from_holders(context, &request, &member.holders, None).await?;
            spool_range(context, &request.held, whole, range).await?
        }
        // Keeping follows with the fill of kept copies; until then the bytes stream.
        PullPlan::FillFirst(_) | PullPlan::ServeAndFill(_) => {
            from_holders(context, &request, &member.holders, range).await?
        }
    };
    let held = request.held;
    Ok(GetObjectResult {
        blob,
        location: None,
        info: held.info(),
        metadata: held.metadata,
        source_metadata: None,
        source_binding: None,
        last_refresh: None,
        version_id: Some(held.version_id),
        resolved_version_id: Some(held.version_id),
        resolved_range: resolved,
        source_policies: held.source_policies,
    })
}

async fn read_row(
    context: &DriverContext,
    key_space: &str,
    key: Vec<u8>,
) -> Result<Option<Value>, GetObjectError> {
    let read = StorageEffect::Read {
        key_space: key_space.to_string(),
        key: key.into(),
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        _ => Err(GetObjectError::GetObjectFailed),
    }
}

/// This member's strategy for the key's range and the holders of its bytes: the replicas that
/// copy it, then its coordinator.
async fn member_of(
    context: &DriverContext,
    request: &PullRequest,
) -> Result<Member, GetObjectError> {
    let bucket = read_row(
        context,
        S3_BUCKET_KEYSPACE,
        request.bucket.as_bytes().to_vec(),
    )
    .await?
    .ok_or(GetObjectError::NoSuchKey)?;
    let binding = BucketInfo::from_bytes(&bucket)?
        .distribution
        .filter(|binding| binding.set_id == request.held.set_id)
        .ok_or(GetObjectError::HoldersUnavailable)?;
    let record = read_row(
        context,
        DISTRIBUTION_KEYSPACE,
        binding.set_id.to_bytes().to_vec(),
    )
    .await?
    .ok_or(GetObjectError::HoldersUnavailable)?;
    let view = DistributionView::from_bytes(&record)?;
    let snapshot = view.snapshot();
    let member = snapshot
        .member(binding.member_id)
        .ok_or(GetObjectError::HoldersUnavailable)?;
    let range = snapshot
        .range_for(&request.key)
        .ok_or(GetObjectError::HoldersUnavailable)?;
    let copies = range.replicas.iter().filter(|replica| {
        snapshot.member(**replica).is_some_and(|replica| {
            replica.strategy == DataStrategy::Copy && replica.state == MemberState::Ready
        })
    });
    let mut holders: Vec<NodeId> = copies
        .chain(std::iter::once(&range.coordinator))
        .filter(|holder| **holder != member.member_id)
        .filter_map(|holder| snapshot.member(*holder).map(|holder| holder.node_id))
        .collect();
    // Replicas share the load; the coordinator stays the last resort.
    if let Some(replicas) = holders
        .len()
        .checked_sub(1)
        .filter(|replicas| *replicas > 1)
    {
        let spread = (Ulid::generate().random() % replicas as u128) as usize;
        holders[..replicas].rotate_left(spread);
    }
    let settings = read_row(
        context,
        BUCKET_ENCRYPTION_KEYSPACE,
        request.bucket.as_bytes().to_vec(),
    )
    .await?;
    let keeping = BucketEncryption::from_row(settings.as_deref())?
        .active_key()
        .is_none();
    Ok(Member {
        strategy: member.strategy,
        behaviour: snapshot.config.range_behaviour(member),
        keeping,
        holders,
    })
}

/// Streams the version, or only `range` of it, from the first holder that serves it.
async fn from_holders(
    context: &DriverContext,
    request: &PullRequest,
    holders: &[NodeId],
    range: Option<Range<u64>>,
) -> Result<Blob, GetObjectError> {
    let held = &request.held;
    let mut failures = HolderFailures::default();
    for holder in holders {
        let read = BaoReadRequest {
            auth_context: request.auth.clone(),
            realm_id: request.auth.realm_id,
            target: BaoReadTarget::Version {
                set_id: held.set_id,
                version_id: held.version_id,
            },
            expected_blake3: Some(held.blake3),
            metadata_only: false,
            destination: None,
            known_refs: Vec::new(),
            range: range.clone(),
        };
        if let Some(BaoReadOutput::Stream { blob, .. }) =
            failures.consider(managed_read(context, *holder, read).await)
        {
            return Ok(blob);
        }
    }
    Err(failures.into_error())
}

/// Reads the whole version into a local spool and serves `range` of it once all bytes arrived.
async fn spool_range(
    context: &DriverContext,
    held: &HeldElsewhere,
    whole: Blob,
    range: Option<Range<u64>>,
) -> Result<Blob, GetObjectError> {
    let blob_handle = context
        .blob_handle
        .as_ref()
        .ok_or(GetObjectError::GetObjectFailed)?;
    let spool = BlobEffect::SpoolLocal {
        size: held.size,
        deadline: None,
        range,
        blob: whole,
    };
    match blob_handle.send_blob_effect(spool).await {
        Event::Blob(BlobEvent::LocalSpooled { hashes, blob, .. })
            if hashes.get(HASH_BLAKE3).map(Vec::as_slice) == Some(held.blake3.as_slice()) =>
        {
            Ok(blob)
        }
        Event::Blob(BlobEvent::LocalSpooled { .. }) => Err(GetObjectError::HolderIntegrityFailure),
        Event::Blob(BlobEvent::Error(aruna_core::errors::BlobError::SpoolBudgetExhausted {
            ..
        })) => Err(GetObjectError::SpoolBudgetExhausted),
        _ => Err(GetObjectError::HoldersUnavailable),
    }
}

#[cfg(test)]
#[path = "pull_tests.rs"]
mod tests;
