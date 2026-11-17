//! Deletes an object version, moves the head pointer and updates usage and cleanup work.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::blob::records::HeadAliasContext;
use crate::distribution::guard::{GuardError, authority_reads, write_epoch};
use crate::distribution::inventory::{
    AddedVersion, HeadChange, InventoryChange, stamp_head, stamp_version, version_written,
};
use crate::s3::purge_fence::{PurgeFenceError, check_fence_row, fence_key};
use crate::s3::row_fence::{FenceError, RowFence};
use crate::s3::usage::{
    RowChange, UsageCounterUpdate, UsageUpdateError, schedule_snapshot_publish,
};
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_RECLAIM_KEYSPACE, BLOB_VERSIONS_KEYSPACE,
    BUCKET_CREATION_KEYSPACE, COPY_OWNER_KEYSPACE, DELETE_AUDIT_KEYSPACE, DELETE_TRAIL_KEYSPACE,
    MANAGED_COPY_KEYSPACE, OBJECT_METADATA_KEYSPACE, PATHS_INDEX_KEYSPACE,
    PENDING_LOCATION_KEYSPACE, PURGE_FENCE_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::managed_copy::{COPY_PAGE_LIMIT, ManagedCopyError, version_scope};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{PrefixPages, read_row};
use aruna_core::structs::identity::auth::{AuthContext, PathRestriction};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{
    ArchiveKey, BackendLocation, BlobHeadKey, BlobLocationKey, BlobVersion, BlobVersionState,
    BucketInfo, CopyOwner, CurrentVersionPointer, VersionKey, incarnation,
};
use aruna_core::structs::storage::cleanup::{ReclaimCandidate, ReclaimCandidateKey};
use aruna_core::structs::storage::delete_audit::{
    BlobAuditKind, BlobAuditRecord, delete_audit_key, trail_key,
};
use aruna_core::structs::storage::distribution::hint::{SetHint, mint_version};
use aruna_core::structs::storage::distribution::inventory::VersionEntry;
use aruna_core::structs::storage::distribution::{DistributionBinding, OriginalEpoch};
use aruna_core::structs::storage::format::StoredLayout;
use aruna_core::structs::storage::multipart::MultipartObjectKey;
use aruna_core::structs::storage::replication_queue::{
    LiveObligationRecord, live_obligation_entry,
};
use aruna_core::structs::storage::usage::UsageDelta;
use aruna_core::types::{GroupId, Key, Value};
use std::collections::HashMap;
use std::time::SystemTime;
use thiserror::Error;
use ulid::Ulid;

mod abe;
pub mod bulk;
pub mod pinned;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeleteObjectState {
    CheckPurgeFence,
    /// Reads the bucket record for the set hint a new delete marker carries.
    ReadBucketBinding,
    /// Reads a distributed bucket's record: only the key's coordinator adds a delete marker.
    ReadRecord,
    /// Reads the bucket record before a single version is deleted.
    ReadVersionBucket,
    ReadTargetVersion,
    ReadTargetLocation,
    ReadAllVersions,
    ReadCurrentLookup,
    ReadLivenessVersion,
    /// Writes or deletes the head pointer, with the new head's path index.
    WriteHead,
    DeletePathIndex,
    DeleteTargetVersion,
    ReadEnvelopeVersion,
    ReadEnvelopeRows,
    DeleteEnvelopeRows,
    /// Pages the version's local placement registrations.
    ScanManagedCopies,
    DeleteManagedCopies,
    DeleteMultipartSummary,
    ReadMultipartParts,
    DeleteMultipartParts,
    ReleaseOwner,
    ScanOwners,
    WriteReclaimCandidate,
    WriteBlobVersion,
    WriteReplicationObligation,
    UpdateUsage,
    /// Reads the bucket's creation row for the incarnation its delete trail belongs to.
    ReadAuditBucket,
    WriteDeleteAudit,
    /// Reads the row fence's guards or writes its receipts in the delete transaction.
    RowFence,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HeadTransitionContinuation {
    DeleteTargetVersion,
    WriteDeletedVersion,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct VersionSummary {
    version_id: Ulid,
    materialized_hash: Option<[u8; 32]>,
    logical_size: Option<u64>,
    referenced: bool,
    /// Offered directory observations are charged only while they are the head.
    offered: bool,
    deleted: bool,
}

impl VersionSummary {
    fn from_blob_version(key: &VersionKey, version: &BlobVersion) -> Self {
        let version_id = key.version_id;
        let (materialized_hash, logical_size, referenced) = match &version.state {
            BlobVersionState::Materialized { blob_hash, .. } => (Some(*blob_hash), None, false),
            BlobVersionState::Reference {
                cached_metadata, ..
            } => (None, Some(cached_metadata.content_length), true),
            BlobVersionState::Deleted | BlobVersionState::PendingContent { .. } => {
                (None, None, false)
            }
        };
        Self {
            version_id,
            materialized_hash,
            logical_size,
            referenced,
            offered: version.offered_size(&key.bucket, &key.key).is_some(),
            deleted: version.is_deleted(),
        }
    }

    fn is_deleted(&self) -> bool {
        self.deleted
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum DeleteObjectError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("The specified version does not exist.")]
    NoSuchVersion,
    #[error("Invalid operation state")]
    InvalidOperationState,
    #[error(transparent)]
    UsageUpdateError(#[from] UsageUpdateError),
    #[error(transparent)]
    ManagedCopyError(#[from] ManagedCopyError),
    #[error(transparent)]
    PurgeFence(#[from] PurgeFenceError),
    #[error(transparent)]
    Distribution(#[from] GuardError),
    #[error(
        "single versions of a distributed bucket cannot be deleted; delete the key for a delete marker"
    )]
    DistributedVersion,
    #[error("DeleteObject failed")]
    DeleteObjectFailed,
    #[error("The version changed since it was read.")]
    VersionChanged,
    #[error("a guarded row changed before the delete committed")]
    FenceTaken,
}

impl From<IoError> for DeleteObjectError {
    fn from(error: IoError) -> Self {
        Self::StorageError(error.into())
    }
}

type Turn = Result<Next<DeleteObjectResult>, DeleteObjectError>;

#[derive(Debug, PartialEq)]
pub struct DeleteObjectInput {
    pub bucket: String,
    pub key: String,
    pub version_id: Option<Ulid>,
    pub group_id: GroupId,
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub deleted_by: UserId,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteObjectResult {
    pub version_id: Ulid,
    pub delete_marker: bool,
}

#[derive(Debug, PartialEq)]
pub struct DeleteObjectOperation {
    input: DeleteObjectInput,
    state: DeleteObjectState,
    version_id: Option<Ulid>,
    target_version: Option<VersionSummary>,
    latest_remaining: Option<VersionSummary>,
    existing_pointer: Option<CurrentVersionPointer>,
    pending_version_id: Option<Ulid>,
    pending_new_pointer: Option<CurrentVersionPointer>,
    pending_hash: Option<[u8; 32]>,
    head_transition_next: Option<HeadTransitionContinuation>,
    deleted_version_created: Option<SystemTime>,
    target_size: Option<u64>,
    envelope_bytes: u64,
    /// The envelope rows read last, in order, which the delete then removes.
    envelope_keys: Vec<Key>,
    target_location: Option<BlobLocationKey>,
    /// The Pithos archive the deleted version used, with its stored location.
    target_archive: Option<(ArchiveKey, Option<BackendLocation>)>,
    live_before_marker: bool,
    /// Offered directory bytes the head stops charging under a delete marker.
    released_bytes: u64,
    usage_update: Option<UsageCounterUpdate>,
    /// The pages of the version's local placement registrations.
    copies: Option<PrefixPages>,
    restrictions: Option<Vec<PathRestriction>>,
    metadata: HashMap<String, String>,
    /// The record the deleted version must still have inside the delete's transaction.
    expected: Option<BlobVersion>,
    /// The head a delete marker may only cover while it is still current.
    expected_head: Option<Ulid>,
    /// The bucket incarnation the delete is made in, read in its transaction for the trail.
    incarnation: Option<u128>,
    row_fence: Option<RowFence>,
    /// The set hint of a distributed bucket; ordinary buckets mint plain ULIDs.
    set_hint: Option<SetHint>,
    /// The bucket's distribution binding, read with the marker write for its inventory.
    binding: Option<DistributionBinding>,
    /// The marker's head and version epoch, for the inventory.
    marker_head: Option<CurrentVersionPointer>,
    marker_epoch: OriginalEpoch,
    /// The member binding a set deletion or range reclaim purges under; only then may a
    /// version of a distributed bucket be deleted.
    purge: Option<DistributionBinding>,
    /// A range purge also takes the version out of the set's inventory and range counters.
    inventory: bool,
}

impl DeleteObjectOperation {
    pub fn new(input: DeleteObjectInput) -> Self {
        Self {
            input,
            state: DeleteObjectState::CheckPurgeFence,
            version_id: None,
            target_version: None,
            latest_remaining: None,
            existing_pointer: None,
            pending_version_id: None,
            pending_new_pointer: None,
            pending_hash: None,
            head_transition_next: None,
            deleted_version_created: None,
            target_size: None,
            envelope_bytes: 0,
            envelope_keys: Vec::new(),
            target_location: None,
            target_archive: None,
            live_before_marker: false,
            released_bytes: 0,
            usage_update: None,
            copies: None,
            restrictions: None,
            metadata: HashMap::new(),
            expected: None,
            expected_head: None,
            incarnation: None,
            row_fence: None,
            set_hint: None,
            binding: None,
            marker_head: None,
            marker_epoch: OriginalEpoch::default(),
            purge: None,
            inventory: false,
        }
    }

    /// Lets a purge delete a version of the member bucket bound by `binding`; the bucket is
    /// read in the delete transaction and must still carry that set, member and incarnation.
    pub fn purging(mut self, binding: DistributionBinding) -> Self {
        self.purge = Some(binding);
        self
    }

    /// Also takes the purged version, and the key's head with it, out of the set's inventory and
    /// range counters in the delete's transaction, so both counters move together.
    pub fn in_inventory(mut self) -> Self {
        self.inventory = true;
        self
    }

    /// Writes the delete marker only while `version_id` is the head, read in its transaction.
    pub fn with_head(mut self, version_id: Ulid) -> Self {
        self.expected_head = Some(version_id);
        self
    }

    /// Metadata the delete marker carries. A forwarded sync delete tags the
    /// device version it came from here, so a replay is recognized instead of
    /// minting a second marker.
    pub fn with_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.metadata = metadata;
        self
    }

    /// Refuses the delete with `FenceTaken` unless its transaction still reads each guard as
    /// given; each receipt key then records the delete marker's id in that transaction.
    pub fn with_fence(
        mut self,
        guards: Vec<(String, Key, Option<Value>)>,
        receipts: Vec<(String, Key)>,
    ) -> Self {
        self.row_fence = Some(RowFence::new(guards, receipts));
        self
    }

    /// Deletes the version only while its record still equals `version`.
    pub fn with_expected(mut self, version: BlobVersion) -> Self {
        self.expected = Some(version);
        self
    }

    /// The deleter's credential restrictions. They are persisted on the durable
    /// replication obligation, so a scoped delete cannot escalate to unscoped
    /// when the obligation repair path enqueues replication instead.
    pub fn with_restrictions(mut self, restrictions: Option<Vec<PathRestriction>>) -> Self {
        self.restrictions = restrictions;
        self
    }

    fn alias_context(&self) -> HeadAliasContext {
        HeadAliasContext::new(
            self.input.realm_id,
            self.input.group_id,
            self.input.node_id,
            self.input.bucket.clone(),
            self.input.key.clone(),
        )
    }

    fn target_id(&self) -> Result<Ulid, DeleteObjectError> {
        self.input
            .version_id
            .ok_or(DeleteObjectError::InvalidOperationState)
    }

    fn version_key(&self, version_id: Ulid) -> VersionKey {
        VersionKey::new(&self.input.bucket, &self.input.key, version_id)
    }

    fn call(&mut self, state: DeleteObjectState, call: IoCall) -> Turn {
        self.state = state;
        Ok(Next::Call(call))
    }

    fn read_bucket(&mut self, state: DeleteObjectState) -> Turn {
        let bucket = self.input.bucket.as_bytes().to_vec();
        self.call(state, read_row(S3_BUCKET_KEYSPACE, bucket))
    }

    /// Writes the new head with its path index, or deletes the head when no version is left.
    fn prepare_head_transition(&mut self, next: HeadTransitionContinuation) -> Turn {
        let context = self.alias_context();
        let head = context.head_key().to_bytes()?;
        self.head_transition_next = Some(next);
        let hash = self.pending_hash.take();
        let Some(pointer) = self.pending_new_pointer.take() else {
            let delete = vec![(BLOB_HEAD_KEYSPACE.to_string(), head.into())];
            return self.call(DeleteObjectState::WriteHead, IoCall::Delete(delete));
        };
        let mut writes = vec![(
            BLOB_HEAD_KEYSPACE.to_string(),
            head.into(),
            pointer.to_bytes()?.into(),
        )];
        if let Some(hash) = hash {
            let index = context
                .path_index_key(hash, pointer.version_id)
                .to_bytes()?;
            writes.push((
                PATHS_INDEX_KEYSPACE.to_string(),
                index.into(),
                Value::from(Vec::new()),
            ));
        }
        self.call(DeleteObjectState::WriteHead, IoCall::Write(writes))
    }

    fn head_written(&mut self) -> Turn {
        match self.head_transition_next.take() {
            Some(HeadTransitionContinuation::DeleteTargetVersion) => self.delete_target_version(),
            Some(HeadTransitionContinuation::WriteDeletedVersion) => self.write_deleted_version(),
            None => Err(DeleteObjectError::DeleteObjectFailed),
        }
    }

    fn fence_checked(&mut self, value: Option<Value>) -> Turn {
        check_fence_row(value.as_deref(), &self.input.bucket, &self.input.key)?;
        self.read_bucket(DeleteObjectState::ReadBucketBinding)
    }

    fn binding_read(&mut self, value: Option<Value>) -> Turn {
        let bucket = value
            .map(|value| BucketInfo::from_bytes(value.as_ref()))
            .transpose()?;
        self.binding = bucket.and_then(|bucket| bucket.distribution);
        self.set_hint = self.binding.as_ref().map(DistributionBinding::hint);
        match self.binding.as_ref() {
            Some(binding) => {
                // Read in the marker transaction, so a fence or record set meanwhile conflicts.
                let reads = authority_reads(binding.set_id);
                self.call(DeleteObjectState::ReadRecord, IoCall::Read(reads))
            }
            None => self.write_tombstone(),
        }
    }

    fn record_checked(&mut self, values: Vec<Option<Value>>) -> Turn {
        let binding = self.binding.as_ref();
        let binding = binding.ok_or(DeleteObjectError::InvalidOperationState)?;
        let [record, fence] =
            <[_; 2]>::try_from(values).map_err(|_| DeleteObjectError::InvalidOperationState)?;
        let key = &self.input.key;
        self.marker_epoch = write_epoch(binding, key, record.as_deref(), fence.as_deref())?;
        self.write_tombstone()
    }

    /// A single version may hold the current head on one member only, so a distributed
    /// bucket refuses to delete it.
    fn version_bucket_read(&mut self, value: Option<Value>) -> Turn {
        let bucket = value
            .map(|value| BucketInfo::from_bytes(value.as_ref()))
            .transpose()?;
        let binding = bucket.and_then(|bucket| bucket.distribution);
        let purged = match (&binding, &self.purge) {
            (Some(bound), Some(purge)) => same_member(bound, purge),
            _ => false,
        };
        if self.purge.is_some() && !purged {
            // The member bucket the purge was for is gone or replaced; nothing is deleted.
            return Err(DeleteObjectError::NoSuchVersion);
        }
        if binding.is_some() && !purged {
            return Err(DeleteObjectError::DistributedVersion);
        }
        let key = self.version_key(self.target_id()?).to_bytes()?;
        self.call(
            DeleteObjectState::ReadTargetVersion,
            read_row(BLOB_VERSIONS_KEYSPACE, key),
        )
    }

    fn target_version_read(&mut self, value: Option<Value>) -> Turn {
        let value = value.ok_or(DeleteObjectError::NoSuchVersion)?;
        let version_id = self.target_id()?;
        let version = BlobVersion::from_bytes(value.as_ref())?;
        if (self.expected.as_ref()).is_some_and(|expected| *expected != version) {
            return Err(DeleteObjectError::VersionChanged);
        }
        let location_key = version.location_key();
        let summary = VersionSummary::from_blob_version(&self.version_key(version_id), &version);
        self.target_size = summary.logical_size;
        self.target_version = Some(summary);
        self.target_location = location_key.clone();

        if let Some(archive) = version.state.pending_archive() {
            self.target_archive = Some((archive.clone(), None));
            let read = read_row(PENDING_LOCATION_KEYSPACE, archive.to_bytes());
            return self.call(DeleteObjectState::ReadTargetLocation, read);
        }
        if let Some(key) = location_key {
            let read = read_row(BLOB_LOCATIONS_KEYSPACE, key.to_bytes());
            return self.call(DeleteObjectState::ReadTargetLocation, read);
        }
        self.read_all_versions()
    }

    fn target_location_read(&mut self, value: Option<Value>) -> Turn {
        let location = value.and_then(|value| BackendLocation::from_bytes(value.as_ref()).ok());
        self.target_size = location.as_ref().map(|location| location.blob_size);
        if let Some(location) =
            location.filter(|location| matches!(location.format.layout, StoredLayout::Pithos(_)))
        {
            self.target_archive = Some((ArchiveKey::of(&location), Some(location)));
        }
        self.read_all_versions()
    }

    fn read_all_versions(&mut self) -> Turn {
        // A purge removes the whole key, so its head needs no older replacement.
        if self.purge.is_some() {
            return self.read_current_lookup(self.target_id()?);
        }
        let prefix = VersionKey::object_prefix(&self.input.bucket, &self.input.key)?;
        let page = IoCall::Page {
            space: BLOB_VERSIONS_KEYSPACE.to_string(),
            prefix: prefix.into(),
            after: None,
            limit: usize::MAX,
        };
        self.call(DeleteObjectState::ReadAllVersions, page)
    }

    fn versions_read(&mut self, values: Vec<(Key, Value)>) -> Turn {
        let target_version_id = self.target_id()?;
        self.latest_remaining = values
            .into_iter()
            .filter_map(|(key, value)| {
                let version_key = VersionKey::from_bytes(key.as_ref()).ok()?;
                if version_key.version_id == target_version_id {
                    return None;
                }
                let version = BlobVersion::from_bytes(value.as_ref()).ok()?;
                Some(VersionSummary::from_blob_version(&version_key, &version))
            })
            .max_by_key(|summary| summary.version_id);
        let replacement_version_id = self
            .latest_remaining
            .as_ref()
            .map(|metadata| metadata.version_id)
            .unwrap_or(target_version_id);
        self.read_current_lookup(replacement_version_id)
    }

    fn read_current_lookup(&mut self, version_id: Ulid) -> Turn {
        self.pending_version_id = Some(version_id);
        let key = BlobHeadKey::new(&self.input.bucket, &self.input.key).to_bytes()?;
        self.call(
            DeleteObjectState::ReadCurrentLookup,
            read_row(BLOB_HEAD_KEYSPACE, key),
        )
    }

    fn current_lookup_read(&mut self, value: Option<Value>) -> Turn {
        let existing = value
            .as_ref()
            .map(|value| CurrentVersionPointer::from_bytes(value.as_ref()))
            .transpose()?;
        self.existing_pointer = existing.clone();
        let head = existing.as_ref().map(|pointer| pointer.version_id);
        if self.expected_head.is_some() && head != self.expected_head {
            return Err(DeleteObjectError::VersionChanged);
        }
        let version_id = self.pending_version_id.take();
        let version_id = version_id.ok_or(DeleteObjectError::InvalidOperationState)?;
        if let Some(target_version_id) = self.input.version_id {
            return self.delete_lookup_read(target_version_id, existing.as_ref());
        }
        self.pending_hash = None;
        if let Some(pointer) = existing.as_ref() {
            let key = self.version_key(pointer.version_id).to_bytes()?;
            return self.call(
                DeleteObjectState::ReadLivenessVersion,
                read_row(BLOB_VERSIONS_KEYSPACE, key),
            );
        }
        self.live_before_marker = false;
        self.write_tombstone_pointer(version_id, existing.as_ref())
    }

    fn liveness_read(&mut self, value: Option<Value>) -> Turn {
        let head = value.and_then(|value| BlobVersion::from_bytes(value.as_ref()).ok());
        self.live_before_marker = head.as_ref().is_some_and(|version| !version.is_deleted());
        self.released_bytes = head
            .and_then(|version| version.offered_size(&self.input.bucket, &self.input.key))
            .unwrap_or(0);
        let version_id = self.version_id;
        let version_id = version_id.ok_or(DeleteObjectError::InvalidOperationState)?;
        let existing = self.existing_pointer.clone();
        self.write_tombstone_pointer(version_id, existing.as_ref())
    }

    fn delete_lookup_read(
        &mut self,
        target_version_id: Ulid,
        existing: Option<&CurrentVersionPointer>,
    ) -> Turn {
        if existing.is_none_or(|pointer| pointer.version_id != target_version_id) {
            return self.delete_target_version();
        }
        match self
            .latest_remaining
            .as_ref()
            .map(|metadata| (metadata.version_id, metadata.materialized_hash))
        {
            Some((version_id, new_hash)) => {
                self.pending_hash = new_hash;
                self.pending_new_pointer =
                    Some(CurrentVersionPointer::next_for(existing, version_id)?);
            }
            None => {
                self.pending_new_pointer = None;
                self.pending_hash = None;
            }
        }
        self.prepare_head_transition(HeadTransitionContinuation::DeleteTargetVersion)
    }

    fn delete_target_version(&mut self) -> Turn {
        let version_id = self.target_id()?;
        if let Some(target_version) = self.target_version.as_ref()
            && let Some(materialized_hash) = target_version.materialized_hash
        {
            let context = self.alias_context();
            let index = context
                .path_index_key(materialized_hash, version_id)
                .to_bytes()?;
            let delete = vec![(PATHS_INDEX_KEYSPACE.to_string(), index.into())];
            return self.call(DeleteObjectState::DeletePathIndex, IoCall::Delete(delete));
        }
        self.delete_version_record()
    }

    fn delete_version_record(&mut self) -> Turn {
        let key = self.version_key(self.target_id()?).to_bytes()?;
        let delete = vec![(BLOB_VERSIONS_KEYSPACE.to_string(), key.into())];
        self.call(
            DeleteObjectState::DeleteTargetVersion,
            IoCall::Delete(delete),
        )
    }

    /// Joins the delete transaction, so no local registration outlives the
    /// logical version it made serveable.
    fn remove_managed_copies(&mut self) -> Turn {
        let scope = version_scope(&self.version_key(self.target_id()?))?;
        let pages = PrefixPages::new(MANAGED_COPY_KEYSPACE, scope, COPY_PAGE_LIMIT);
        let call = pages.call();
        self.copies = Some(pages);
        self.call(DeleteObjectState::ScanManagedCopies, call)
    }

    fn copies_scanned(&mut self, result: IoResult) -> Turn {
        let pages = self.copies.as_mut();
        let rows = pages
            .ok_or(DeleteObjectError::InvalidOperationState)?
            .take(result)?;
        if rows.is_empty() {
            return self.delete_multipart_summary();
        }
        let deletes = rows.into_iter();
        let deletes = deletes.map(|(key, _)| (MANAGED_COPY_KEYSPACE.to_string(), key));
        self.call(
            DeleteObjectState::DeleteManagedCopies,
            IoCall::Delete(deletes.collect()),
        )
    }

    /// A full page resumes the scan strictly after its last key, so removal always progresses.
    fn copies_deleted(&mut self) -> Turn {
        let pages = self.copies.as_ref();
        let pages = pages.ok_or(DeleteObjectError::InvalidOperationState)?;
        if pages.done() {
            self.copies = None;
            return self.delete_multipart_summary();
        }
        let call = pages.call();
        self.call(DeleteObjectState::ScanManagedCopies, call)
    }

    fn delete_multipart_summary(&mut self) -> Turn {
        let key = MultipartObjectKey::summary(self.target_id()?).to_bytes()?;
        let delete = vec![(OBJECT_METADATA_KEYSPACE.to_string(), key.into())];
        self.call(
            DeleteObjectState::DeleteMultipartSummary,
            IoCall::Delete(delete),
        )
    }

    fn summary_deleted(&mut self) -> Turn {
        let prefix = MultipartObjectKey::part_prefix(self.target_id()?)?;
        let page = IoCall::Page {
            space: OBJECT_METADATA_KEYSPACE.to_string(),
            prefix: prefix.into(),
            after: None,
            limit: 10_000,
        };
        self.call(DeleteObjectState::ReadMultipartParts, page)
    }

    fn parts_read(&mut self, values: Vec<(Key, Value)>) -> Turn {
        if values.is_empty() {
            return self.write_reclaim_candidate();
        }
        let deletes = values.into_iter();
        let deletes = deletes.map(|(key, _)| (OBJECT_METADATA_KEYSPACE.to_string(), key));
        self.call(
            DeleteObjectState::DeleteMultipartParts,
            IoCall::Delete(deletes.collect()),
        )
    }

    /// Queues the copy the deleted version named. Blind and idempotent: two
    /// aliases of one hash just refresh the row, and the sweep owns the recount.
    fn write_reclaim_candidate(&mut self) -> Turn {
        let Some((archive, _)) = self.target_archive.as_ref() else {
            return self.write_candidate();
        };
        // The deleted version no longer uses its archive. A known copy is then freed by the
        // reclaim sweep; a pending archive is freed here once no other owner remains.
        let owner = CopyOwner::new(archive.clone(), self.version_key(self.target_id()?));
        let delete = vec![(COPY_OWNER_KEYSPACE.to_string(), owner.key()?.into())];
        self.call(DeleteObjectState::ReleaseOwner, IoCall::Delete(delete))
    }

    fn owner_released(&mut self) -> Turn {
        match (&self.target_location, &self.target_archive) {
            (None, Some((archive, _))) => {
                let page = IoCall::Page {
                    space: COPY_OWNER_KEYSPACE.to_string(),
                    prefix: CopyOwner::prefix(archive).into(),
                    after: None,
                    limit: 2,
                };
                self.call(DeleteObjectState::ScanOwners, page)
            }
            _ => self.write_candidate(),
        }
    }

    fn owners_scanned(&mut self, values: Vec<(Key, Value)>) -> Turn {
        for (key, _) in &values {
            // An unreadable owner proves nothing, so the delete stops at it.
            CopyOwner::from_key(key.as_ref())?;
        }
        // Even the last owner leaves the archive to its backend's cleanup strategy: Retain keeps
        // it and a reclaim grace must pass first, so a delete never frees it at once.
        self.start_usage_update()
    }

    fn write_candidate(&mut self) -> Turn {
        let Some(location) = self.target_location.clone() else {
            return self.start_usage_update();
        };
        let candidate = ReclaimCandidate {
            enqueued_at: SystemTime::now(),
        };
        let key =
            ReclaimCandidateKey::new(location.backend, location.encoding, location.blake3_hash);
        let row = (
            BLOB_RECLAIM_KEYSPACE.to_string(),
            key.to_bytes().into(),
            candidate.to_bytes()?.into(),
        );
        self.call(
            DeleteObjectState::WriteReclaimCandidate,
            IoCall::Write(vec![row]),
        )
    }

    fn usage_delta(&self) -> UsageDelta {
        if let Some(target) = self.target_version.as_ref() {
            let pointed_at_target = self
                .existing_pointer
                .as_ref()
                .is_some_and(|pointer| pointer.version_id == target.version_id);
            let objects = if pointed_at_target {
                let live_before = !target.is_deleted();
                let live_after = self
                    .latest_remaining
                    .as_ref()
                    .is_some_and(|latest| !latest.is_deleted());
                i128::from(u8::from(live_after)) - i128::from(u8::from(live_before))
            } else {
                0
            };
            let bytes = self.target_size.map_or(0, |size| -i128::from(size));
            let envelope = -i128::from(self.envelope_bytes);
            // An offered directory observation is charged only while it is the head.
            let charged = target.referenced && (pointed_at_target || !target.offered);
            let promoted = self
                .latest_remaining
                .as_ref()
                .filter(|latest| pointed_at_target && latest.offered)
                .and_then(|latest| latest.logical_size)
                .map_or(0, i128::from);
            UsageDelta {
                objects,
                logical_bytes: if target.referenced {
                    envelope
                } else {
                    bytes + envelope
                },
                referenced_bytes: if charged { bytes } else { 0 } + promoted,
                ..Default::default()
            }
        } else {
            UsageDelta {
                objects: if self.live_before_marker { -1 } else { 0 },
                referenced_bytes: -i128::from(self.released_bytes),
                ..Default::default()
            }
        }
    }

    fn start_usage_update(&mut self) -> Turn {
        let delta = self.usage_delta();
        // A version delete removes its row; a delete without a version writes a marker.
        let row = match (self.input.version_id, self.target_version.as_ref()) {
            (None, _) => RowChange::AddedMarker,
            (Some(_), Some(target)) if target.is_deleted() => RowChange::RemovedMarker,
            (Some(_), Some(_)) => RowChange::RemovedVersion,
            (Some(_), None) => RowChange::Kept,
        };
        let inventory = self.marker_inventory().or_else(|| self.purged_inventory());
        let mut update = UsageCounterUpdate::for_group(self.input.group_id, delta)
            .in_bucket(&self.input.bucket, row)
            .with_inventory(inventory);
        if update.is_noop() {
            return self.write_delete_audit();
        }
        let call = update.start_calls().into_call();
        self.usage_update = Some(update);
        self.call(DeleteObjectState::UpdateUsage, call)
    }

    /// A delete marker of a distributed bucket becomes a version row and the key's head.
    fn marker_inventory(&self) -> Option<InventoryChange> {
        let (None, Some(version_id), Some(head)) = (
            self.input.version_id,
            self.version_id,
            self.marker_head.as_ref(),
        ) else {
            return None;
        };
        let added = AddedVersion {
            version_id,
            epoch: self.marker_epoch,
            generation: head.generation,
            entry: VersionEntry {
                marker: true,
                size: 0,
                referenced: false,
                identity: None,
                pending: false,
                searchable: false,
            },
        };
        version_written(self.binding.as_ref(), &self.input.key, added, true)
    }

    /// A purged version leaves the inventory; the key's head goes with the version it names.
    fn purged_inventory(&self) -> Option<InventoryChange> {
        let binding = self.purge.as_ref().filter(|_| self.inventory)?;
        let target = self.target_version.as_ref()?;
        let mut change = InventoryChange::new(binding.set_id, self.input.key.clone());
        change.removed.push(target.version_id);
        let pointer = self.existing_pointer.as_ref();
        if pointer.is_some_and(|pointer| pointer.version_id == target.version_id) {
            change.head = HeadChange::Remove;
        }
        // The purged range's search index is dropped with it.
        change.feed = false;
        Some(change)
    }

    fn usage_updated(&mut self, result: IoResult) -> Turn {
        let update = self.usage_update.as_mut();
        let update = update.ok_or(DeleteObjectError::DeleteObjectFailed)?;
        match update.step_calls(result)? {
            Some(call) => Ok(Next::Call(call.into_call())),
            None => self.write_delete_audit(),
        }
    }

    /// The delete trail is written inside the same transaction, so a committed
    /// deletion can never lack its record and an aborted one never leaves one.
    fn write_delete_audit(&mut self) -> Turn {
        // The trail belongs to this incarnation of the bucket, read in the transaction.
        let Some(incarnation) = self.incarnation else {
            let bucket = self.input.bucket.as_bytes().to_vec();
            let read = read_row(BUCKET_CREATION_KEYSPACE, bucket);
            return self.call(DeleteObjectState::ReadAuditBucket, read);
        };
        let (kind, version_id) = match self.version_id {
            Some(marker) => (BlobAuditKind::DeleteMarker, Some(marker)),
            None => (BlobAuditKind::DeleteVersion, self.input.version_id),
        };
        let record = BlobAuditRecord {
            realm_id: self.input.realm_id,
            group_id: self.input.group_id,
            node_id: self.input.node_id,
            user_id: self.input.deleted_by,
            kind,
            bucket: self.input.bucket.clone(),
            key: self.input.key.clone(),
            version_id,
            occurred_at_ms: aruna_core::time::unix_timestamp_millis(),
        };
        let value: Value = record.to_bytes()?.into();
        let audit_id = Ulid::generate();
        let trail = trail_key(&record, incarnation, audit_id).into();
        let writes = vec![
            (
                DELETE_AUDIT_KEYSPACE.to_string(),
                delete_audit_key(self.input.group_id, audit_id).into(),
                value.clone(),
            ),
            (DELETE_TRAIL_KEYSPACE.to_string(), trail, value),
        ];
        self.call(DeleteObjectState::WriteDeleteAudit, IoCall::Write(writes))
    }

    fn audit_bucket_read(&mut self, value: Option<Value>) -> Turn {
        self.incarnation = Some(incarnation(value.as_deref()));
        self.write_delete_audit()
    }

    /// Commits once the row fence, if any, has read its guards and written its receipts.
    fn commit_fenced(&mut self) -> Turn {
        // A permanent delete mints no marker; its receipts name the deleted version.
        let version_id = self.version_id.or(self.input.version_id);
        let next = match (self.row_fence.as_mut(), version_id) {
            (Some(fence), Some(version_id)) => fence.next_call(version_id),
            (Some(_), None) => return Err(DeleteObjectError::InvalidOperationState),
            (None, _) => None,
        };
        if let Some(call) = next {
            return self.call(DeleteObjectState::RowFence, call);
        }
        self.deleted()
    }

    fn fence_answered(&mut self, result: IoResult) -> Turn {
        match self
            .row_fence
            .as_mut()
            .map(|fence| fence.answer_call(result))
        {
            Some(Ok(())) => self.commit_fenced(),
            Some(Err(FenceError::Taken)) => Err(DeleteObjectError::FenceTaken),
            Some(Err(FenceError::Storage(error))) => Err(error.into()),
            Some(Err(FenceError::Unexpected)) | None => {
                Err(DeleteObjectError::InvalidOperationState)
            }
        }
    }

    fn write_tombstone(&mut self) -> Turn {
        let version_id = mint_version(self.set_hint);
        self.version_id = Some(version_id);
        self.deleted_version_created = Some(SystemTime::now());
        self.read_current_lookup(version_id)
    }

    fn write_tombstone_pointer(
        &mut self,
        version_id: Ulid,
        existing: Option<&CurrentVersionPointer>,
    ) -> Turn {
        let pointer = CurrentVersionPointer::next_for(existing, version_id)?;
        let pointer = stamp_head(pointer, self.binding.as_ref(), self.marker_epoch);
        self.marker_head = Some(pointer.clone());
        self.pending_new_pointer = Some(pointer);
        self.prepare_head_transition(HeadTransitionContinuation::WriteDeletedVersion)
    }

    fn write_deleted_version(&mut self) -> Turn {
        let version_id = self.version_id;
        let version_id = version_id.ok_or(DeleteObjectError::InvalidOperationState)?;
        let created_at = self
            .deleted_version_created
            .get_or_insert_with(SystemTime::now)
            .to_owned();
        let mut version = BlobVersion::deleted(created_at, self.input.deleted_by);
        version.metadata = self.metadata.clone();
        let generation = self.marker_head.as_ref().map_or(0, |head| head.generation);
        let version = stamp_version(
            version,
            self.binding.as_ref(),
            generation,
            self.marker_epoch,
        );
        self.marker_epoch = version.original_epoch;
        let row = (
            BLOB_VERSIONS_KEYSPACE.to_string(),
            self.version_key(version_id).to_bytes()?.into(),
            version.to_bytes()?.into(),
        );
        self.call(
            DeleteObjectState::WriteBlobVersion,
            IoCall::Write(vec![row]),
        )
    }

    fn write_obligation(&mut self) -> Turn {
        let version_id = self.version_id;
        let version_id = version_id.ok_or(DeleteObjectError::InvalidOperationState)?;
        let record = LiveObligationRecord::new(
            self.input.node_id,
            AuthContext {
                user_id: self.input.deleted_by,
                realm_id: self.input.realm_id,
                path_restrictions: self.restrictions.clone(),
                session: None,
            },
            self.input.bucket.clone(),
            self.input.key.clone(),
            version_id,
            true,
        );
        let row = live_obligation_entry(&record)?;
        self.call(
            DeleteObjectState::WriteReplicationObligation,
            IoCall::Write(vec![row]),
        )
    }

    /// The outcome once the transaction commits; the sweep owns reclaim cadence, so deletes
    /// do not re-arm its timer.
    fn deleted(&mut self) -> Turn {
        let output = match (self.version_id, self.input.version_id) {
            (Some(version_id), _) => DeleteObjectResult {
                version_id,
                delete_marker: true,
            },
            (None, Some(version_id)) => DeleteObjectResult {
                version_id,
                delete_marker: (self.target_version.as_ref())
                    .is_some_and(VersionSummary::is_deleted),
            },
            (None, None) => return Err(DeleteObjectError::InvalidOperationState),
        };
        Ok(Next::Then {
            output,
            calls: vec![schedule_snapshot_publish().into()],
        })
    }
}

impl Operation for DeleteObjectOperation {
    type Output = DeleteObjectResult;
    type Error = DeleteObjectError;

    fn first(&mut self) -> Turn {
        if self.input.version_id.is_some() {
            return self.read_bucket(DeleteObjectState::ReadVersionBucket);
        }
        let read = read_row(PURGE_FENCE_KEYSPACE, fence_key(&self.input.bucket));
        self.call(DeleteObjectState::CheckPurgeFence, read)
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match self.state {
            DeleteObjectState::CheckPurgeFence => self.fence_checked(result.value()?),
            DeleteObjectState::ReadBucketBinding => self.binding_read(result.value()?),
            DeleteObjectState::ReadRecord => self.record_checked(result.values()?),
            DeleteObjectState::ReadVersionBucket => self.version_bucket_read(result.value()?),
            DeleteObjectState::ReadTargetVersion => self.target_version_read(result.value()?),
            DeleteObjectState::ReadTargetLocation => self.target_location_read(result.value()?),
            DeleteObjectState::ReadAllVersions => self.versions_read(result.page()?.0),
            DeleteObjectState::ReadCurrentLookup => self.current_lookup_read(result.value()?),
            DeleteObjectState::ReadLivenessVersion => self.liveness_read(result.value()?),
            DeleteObjectState::WriteHead => {
                result.done()?;
                self.head_written()
            }
            DeleteObjectState::DeletePathIndex => {
                result.done()?;
                self.delete_version_record()
            }
            DeleteObjectState::DeleteTargetVersion => {
                result.done()?;
                self.read_envelope_version()
            }
            DeleteObjectState::ReadEnvelopeVersion => self.envelope_version_read(result.values()?),
            DeleteObjectState::ReadEnvelopeRows => self.envelope_rows_read(result.values()?),
            DeleteObjectState::DeleteEnvelopeRows => {
                result.done()?;
                self.remove_managed_copies()
            }
            DeleteObjectState::ScanManagedCopies => self.copies_scanned(result),
            DeleteObjectState::DeleteManagedCopies => {
                result.done()?;
                self.copies_deleted()
            }
            DeleteObjectState::DeleteMultipartSummary => {
                result.done()?;
                self.summary_deleted()
            }
            DeleteObjectState::ReadMultipartParts => self.parts_read(result.page()?.0),
            DeleteObjectState::DeleteMultipartParts => {
                result.done()?;
                self.write_reclaim_candidate()
            }
            DeleteObjectState::ReleaseOwner => {
                result.done()?;
                self.owner_released()
            }
            DeleteObjectState::ScanOwners => self.owners_scanned(result.page()?.0),
            DeleteObjectState::WriteReclaimCandidate => {
                result.done()?;
                self.start_usage_update()
            }
            DeleteObjectState::WriteBlobVersion => {
                result.done()?;
                self.write_obligation()
            }
            DeleteObjectState::WriteReplicationObligation => {
                result.done()?;
                self.start_usage_update()
            }
            DeleteObjectState::UpdateUsage => self.usage_updated(result),
            DeleteObjectState::ReadAuditBucket => self.audit_bucket_read(result.value()?),
            DeleteObjectState::WriteDeleteAudit => {
                result.done()?;
                self.commit_fenced()
            }
            DeleteObjectState::RowFence => self.fence_answered(result),
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

/// Whether two bindings name the same member bucket: set, member and incarnation.
fn same_member(left: &DistributionBinding, right: &DistributionBinding) -> bool {
    (left.set_id, left.member_id, left.incarnation)
        == (right.set_id, right.member_id, right.incarnation)
}

#[cfg(test)]
#[path = "tests.rs"]
mod test;
