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
    FILL_CLAIM_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::checksum::HASH_BLAKE3;
use aruna_core::structs::execution::source_connector::SourceConnectorKind;
use aruna_core::structs::execution::staging::VersionSourceBinding;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::placement::policy::PlacementPolicyRef;
use aruna_core::structs::storage::blob::{BackendLocation, BucketInfo, HiddenBlobKey};
use aruna_core::structs::storage::distribution::on_use::{
    Attachment, CACHE_CAPACITY_BYTES, CacheEntry, FillClaim, KeptKind, PullPlan, attachment_key,
    claim_key,
};
use aruna_core::structs::storage::distribution::{
    DataStrategy, DistributionView, MemberState, RangeBehaviour,
};
use aruna_core::structs::storage::encryption::BucketEncryption;
use aruna_core::structs::storage::multipart::MultipartChecksumType;
use aruna_core::types::{Key, KeySpace, Value};
use bytes::Bytes;
use tracing::warn;
use ulid::Ulid;

use super::keep::{ClaimFillOperation, Claimed, KeptCopy, PublishKeptOperation, Published};
use crate::connectors::resolver::NATIVE_SET_ID;
use crate::driver::{DriverContext, drive, now_ms};
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
        PullPlan::FillFirst(kind) => match fill(context, &request, kind, &member.holders).await {
            Some(location) => read_kept(context, location, range).await?,
            None => from_holders(context, &request, &member.holders, range).await?,
        },
        PullPlan::ServeAndFill(kind) => {
            let (background, filled) = (context.clone(), request.clone());
            let holders = member.holders.clone();
            tokio::spawn(async move { fill(&background, &filled, kind, &holders).await });
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

/// Reads `range` of a kept copy.
async fn read_kept(
    context: &DriverContext,
    location: BackendLocation,
    range: Option<Range<u64>>,
) -> Result<Blob, GetObjectError> {
    let blob_handle = context
        .blob_handle
        .as_ref()
        .ok_or(GetObjectError::GetObjectFailed)?;
    let read = match range {
        Some(range) => BlobEffect::ReadRange { location, range },
        None => BlobEffect::Read { location },
    };
    match blob_handle.send_blob_effect(read).await {
        Event::Blob(BlobEvent::ReadFinished { blob, .. }) => Ok(blob),
        _ => Err(GetObjectError::GetObjectFailed),
    }
}

/// Keeps a verified copy of the whole version once: claims the fill, reads the version through
/// a holder, checks its BLAKE3 and size, then publishes it. `None` when another fill runs or
/// keeping failed; the read then streams instead.
async fn fill(
    context: &DriverContext,
    request: &PullRequest,
    kind: KeptKind,
    holders: &[NodeId],
) -> Option<BackendLocation> {
    let held = &request.held;
    let claim = claim_key(kind, held.set_id, held.version_id, held.blake3);
    let claimed = FillClaim {
        claim_id: Ulid::generate(),
        claimed_at_ms: now_ms(),
    };
    match drive(ClaimFillOperation::new(claim.clone(), claimed), context).await {
        Ok(Claimed::Claimed) => {}
        Ok(Claimed::Busy) => return None,
        Err(error) => {
            warn!(%error, "A kept copy was not claimed");
            return None;
        }
    }
    let location = match write_copy(context, request, kind, holders).await {
        Ok(location) => location,
        Err(reason) => {
            warn!(reason, set_id = %held.set_id, version_id = %held.version_id, "A kept copy was not filled");
            release(context, claim).await;
            return None;
        }
    };
    let copy = KeptCopy {
        kind,
        set_id: held.set_id,
        version_id: held.version_id,
        blake3: held.blake3,
        size: held.size,
        location: location.clone(),
        claim_key: claim.clone(),
        capacity: CACHE_CAPACITY_BYTES,
        order: Ulid::generate(),
    };
    match drive(PublishKeptOperation::new(copy), context).await {
        Ok(Published::Kept { evicted }) => {
            for evicted in evicted {
                drop_copy(context, &evicted).await;
            }
            Some(location)
        }
        Ok(Published::Unkept) => {
            drop_copy(context, &location).await;
            None
        }
        Err(error) => {
            warn!(%error, "A kept copy was not published");
            drop_copy(context, &location).await;
            release(context, claim).await;
            None
        }
    }
}

/// Writes the whole version to a hidden copy and checks it against the record.
async fn write_copy(
    context: &DriverContext,
    request: &PullRequest,
    kind: KeptKind,
    holders: &[NodeId],
) -> Result<BackendLocation, String> {
    let held = &request.held;
    let blob_handle = context.blob_handle.as_ref().ok_or("no blob handle")?;
    let whole = from_holders(context, request, holders, None)
        .await
        .map_err(|error| error.to_string())?;
    let name = match kind {
        KeptKind::Attachment => format!("{}-{}", held.set_id, held.version_id),
        KeptKind::Cache => hex::encode(held.blake3),
    };
    let spool = BlobEffect::SpoolHidden {
        namespace: kind.namespace(),
        name,
        created_by: request.auth.user_id,
        max_bytes: Some(held.size),
        deadline: None,
        blob: whole,
    };
    match blob_handle.send_blob_effect(spool).await {
        Event::Blob(BlobEvent::HiddenSpooled {
            location,
            blake3,
            size,
        }) if blake3 == held.blake3 && size == held.size => Ok(location),
        Event::Blob(BlobEvent::HiddenSpooled { location, .. }) => {
            drop_copy(context, &location).await;
            Err("the copy does not match the version".to_string())
        }
        Event::Blob(BlobEvent::Error(error)) => Err(error.to_string()),
        _ => Err("unexpected answer to the copy write".to_string()),
    }
}

async fn drop_copy(context: &DriverContext, location: &BackendLocation) {
    let (Some(blob_handle), Ok(key)) = (
        context.blob_handle.as_ref(),
        HiddenBlobKey::try_from(location),
    ) else {
        return;
    };
    if let Event::Blob(BlobEvent::Error(error)) = blob_handle
        .send_blob_effect(BlobEffect::DeleteHidden { key })
        .await
    {
        warn!(%error, "A dropped kept copy is left to the hidden sweep");
    }
}

/// Releases a fill claim, so the next read may fill again.
async fn release(context: &DriverContext, claim: Vec<u8>) {
    let delete = StorageEffect::Delete {
        key_space: FILL_CLAIM_KEYSPACE.to_string(),
        key: claim.into(),
        txn_id: None,
    };
    if let Event::Storage(StorageEvent::Error { error }) =
        context.storage_handle.send_storage_effect(delete).await
    {
        warn!(%error, "A fill claim stays until it goes stale");
    }
}

#[cfg(test)]
#[path = "pull_tests.rs"]
mod tests;
