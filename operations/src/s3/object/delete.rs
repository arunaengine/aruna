use crate::blob::managed_copy::{ManagedCopyError, ManagedCopyRemoval};
use crate::blob::records::{
    HeadAliasContext, blob_location_read, build_transition_effects, delete_index_effect,
    delete_version_effect, write_version_effect,
};
use crate::node::usage_stats::{UsageCounterUpdate, UsageUpdateError, schedule_snapshot_publish};
use crate::replication::queue::build_live_obligation;
use crate::s3::purge_fence::{PurgeFenceError, check_write_fence, write_fence_read};
use aruna_core::UserId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_RECLAIM_KEYSPACE, BLOB_VERSIONS_KEYSPACE, DELETE_AUDIT_KEYSPACE,
    OBJECT_METADATA_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::{AuthContext, PathRestriction};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{
    BackendLocation, BlobHeadKey, BlobLocationKey, BlobVersion, BlobVersionState,
    CurrentVersionPointer, VersionKey,
};
use aruna_core::structs::storage::cleanup::{ReclaimCandidate, ReclaimCandidateKey};
use aruna_core::structs::storage::delete_audit::{
    BlobAuditKind, BlobAuditRecord, delete_audit_key,
};
use aruna_core::structs::storage::multipart::MultipartObjectKey;
use aruna_core::structs::storage::usage::UsageDelta;
use aruna_core::types::{Effects, GroupId, Key};
use smallvec::smallvec;
use std::collections::{HashMap, VecDeque};
use std::time::SystemTime;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeleteObjectState {
    Init,
    StartTransaction,
    CheckPurgeFence,
    ReadTargetVersion,
    ReadTargetLocation,
    ReadAllVersions,
    ReadCurrentLookup,
    ReadLivenessVersion,
    ApplyHeadTransition,
    DeletePathIndex,
    DeleteTargetVersion,
    RemoveManagedCopies,
    DeleteMultipartSummary,
    ReadMultipartParts,
    DeleteMultipartPart,
    WriteReclaimCandidate,
    WriteBlobVersion,
    WriteReplicationObligation,
    UpdateUsage,
    WriteDeleteAudit,
    CommitTransaction,
    Finish,
    Error,
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
    deleted: bool,
}

impl VersionSummary {
    fn from_blob_version(version_id: Ulid, version: &BlobVersion) -> Self {
        let (materialized_hash, logical_size, referenced) = match &version.state {
            BlobVersionState::Materialized { blob_hash, .. } => (Some(*blob_hash), None, false),
            BlobVersionState::Reference {
                cached_metadata, ..
            } => (None, Some(cached_metadata.content_length), true),
            BlobVersionState::Deleted => (None, None, false),
        };
        Self {
            version_id,
            materialized_hash,
            logical_size,
            referenced,
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
    #[error("No transaction found")]
    NoTransactionFound,
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
    #[error("DeleteObject failed")]
    DeleteObjectFailed,
    #[error("operation did not finish")]
    NotFinished,
}

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
    txn_id: Option<Ulid>,
    version_id: Option<Ulid>,
    target_version: Option<VersionSummary>,
    latest_remaining: Option<VersionSummary>,
    existing_pointer: Option<CurrentVersionPointer>,
    pending_version_id: Option<Ulid>,
    pending_new_pointer: Option<CurrentVersionPointer>,
    pending_hash: Option<[u8; 32]>,
    head_transition_effects: VecDeque<Effect>,
    head_transition_next: Option<HeadTransitionContinuation>,
    deleted_version_created: Option<SystemTime>,
    multipart_part_keys: Vec<Key>,
    multipart_delete_index: usize,
    target_size: Option<u64>,
    target_location: Option<BlobLocationKey>,
    live_before_marker: bool,
    usage_update: Option<UsageCounterUpdate>,
    copy_removal: Option<ManagedCopyRemoval>,
    output: Option<Result<DeleteObjectResult, DeleteObjectError>>,
    restrictions: Option<Vec<PathRestriction>>,
    metadata: HashMap<String, String>,
}

impl DeleteObjectOperation {
    pub fn new(input: DeleteObjectInput) -> Self {
        Self {
            input,
            state: DeleteObjectState::Init,
            txn_id: None,
            version_id: None,
            target_version: None,
            latest_remaining: None,
            existing_pointer: None,
            pending_version_id: None,
            pending_new_pointer: None,
            pending_hash: None,
            head_transition_effects: VecDeque::new(),
            head_transition_next: None,
            deleted_version_created: None,
            multipart_part_keys: Vec::new(),
            multipart_delete_index: 0,
            target_size: None,
            target_location: None,
            live_before_marker: false,
            usage_update: None,
            copy_removal: None,
            output: None,
            restrictions: None,
            metadata: HashMap::new(),
        }
    }

    /// Metadata the delete marker carries. A forwarded sync delete tags the
    /// device version it came from here, so a replay is recognized instead of
    /// minting a second marker.
    pub fn with_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.metadata = metadata;
        self
    }

    /// The deleter's credential restrictions. They are persisted on the durable
    /// replication obligation, so a scoped delete cannot escalate to unscoped
    /// when the obligation repair path enqueues replication instead.
    pub fn with_restrictions(mut self, restrictions: Option<Vec<PathRestriction>>) -> Self {
        self.restrictions = restrictions;
        self
    }

    fn emit_error(&mut self, error: DeleteObjectError) -> Effects {
        self.state = DeleteObjectState::Error;
        self.output = Some(Err(error));
        self.abort()
    }

    fn handle_init(&mut self) -> Effects {
        self.state = DeleteObjectState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
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

    fn prepare_head_transition(&mut self, next: HeadTransitionContinuation) -> Effects {
        let effects = match build_transition_effects(
            &self.alias_context(),
            self.pending_new_pointer.take(),
            self.pending_hash.take(),
            self.txn_id,
        ) {
            Ok(effects) => effects,
            Err(err) => return self.emit_error(err.into()),
        };

        self.head_transition_effects = effects.into_iter().collect();
        self.head_transition_next = Some(next);
        self.state = DeleteObjectState::ApplyHeadTransition;
        self.emit_head_transition()
    }

    fn emit_head_transition(&mut self) -> Effects {
        if let Some(effect) = self.head_transition_effects.pop_front() {
            return smallvec![effect];
        }

        match self.head_transition_next.take() {
            Some(HeadTransitionContinuation::DeleteTargetVersion) => self.delete_target_version(),
            Some(HeadTransitionContinuation::WriteDeletedVersion) => self.write_deleted_version(),
            None => self.emit_error(DeleteObjectError::DeleteObjectFailed),
        }
    }

    fn head_transition_applied(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::WriteResult { .. })
            | Event::Storage(StorageEvent::DeleteResult { .. }) => self.emit_head_transition(),
            _ => self.emit_error(DeleteObjectError::InvalidOperationState),
        }
    }

    fn handle_transaction_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };

        self.txn_id = Some(txn_id);
        if let Some(version_id) = self.input.version_id {
            self.read_target_version(version_id)
        } else {
            self.state = DeleteObjectState::CheckPurgeFence;
            smallvec![write_fence_read(&self.input.bucket, self.txn_id)]
        }
    }

    fn fence_checked(&mut self, event: Event) -> Effects {
        match check_write_fence(event, &self.input.bucket, &self.input.key) {
            Ok(()) => self.write_tombstone(),
            Err(error) => self.emit_error(error.into()),
        }
    }

    fn read_target_version(&mut self, version_id: Ulid) -> Effects {
        self.state = DeleteObjectState::ReadTargetVersion;
        let key = match VersionKey::new(&self.input.bucket, &self.input.key, version_id).to_bytes()
        {
            Ok(key) => key.into(),
            Err(err) => return self.emit_error(err.into()),
        };

        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key,
            txn_id: self.txn_id,
        })]
    }

    fn target_version_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };

        let Some(value) = value else {
            return self.emit_error(DeleteObjectError::NoSuchVersion);
        };
        let Some(version_id) = self.input.version_id else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };
        let version = match BlobVersion::from_bytes(value.as_ref()) {
            Ok(version) => version,
            Err(err) => return self.emit_error(err.into()),
        };
        let location_key = version.location_key();
        let summary = VersionSummary::from_blob_version(version_id, &version);
        self.target_size = summary.logical_size;
        self.target_version = Some(summary);
        self.target_location = location_key.clone();

        if let Some(key) = location_key {
            self.state = DeleteObjectState::ReadTargetLocation;
            return smallvec![blob_location_read(&key, self.txn_id)];
        }

        self.read_all_versions()
    }

    fn target_location_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };

        self.target_size = value
            .and_then(|value| BackendLocation::from_bytes(value.as_ref()).ok())
            .map(|location| location.blob_size);

        self.read_all_versions()
    }

    fn read_all_versions(&mut self) -> Effects {
        self.state = DeleteObjectState::ReadAllVersions;
        let prefix = match VersionKey::object_prefix(&self.input.bucket, &self.input.key) {
            Ok(prefix) => prefix.into(),
            Err(err) => return self.emit_error(err.into()),
        };

        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            prefix: Some(prefix),
            start: None,
            limit: u64::MAX as usize,
            txn_id: self.txn_id,
        })]
    }

    fn versions_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult { values, .. }) = event else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };

        let Some(target_version_id) = self.input.version_id else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };

        self.latest_remaining = values
            .into_iter()
            .filter_map(|(key, value)| {
                let version_key = VersionKey::from_bytes(key.as_ref()).ok()?;
                if version_key.version_id == target_version_id {
                    return None;
                }
                let version = BlobVersion::from_bytes(value.as_ref()).ok()?;
                Some(VersionSummary::from_blob_version(
                    version_key.version_id,
                    &version,
                ))
            })
            .max_by_key(|summary| summary.version_id);

        let replacement_version_id = self
            .latest_remaining
            .as_ref()
            .map(|metadata| metadata.version_id)
            .unwrap_or(target_version_id);

        self.read_current_lookup(replacement_version_id)
    }

    fn read_current_lookup(&mut self, version_id: Ulid) -> Effects {
        self.pending_version_id = Some(version_id);
        self.state = DeleteObjectState::ReadCurrentLookup;
        let key = match BlobHeadKey::new(&self.input.bucket, &self.input.key).to_bytes() {
            Ok(key) => key.into(),
            Err(err) => return self.emit_error(err.into()),
        };

        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BLOB_HEAD_KEYSPACE.to_string(),
            key,
            txn_id: self.txn_id,
        })]
    }

    fn current_lookup_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };
        let existing = match value
            .as_ref()
            .map(|value| CurrentVersionPointer::from_bytes(value.as_ref()))
            .transpose()
        {
            Ok(existing) => existing,
            Err(err) => return self.emit_error(err.into()),
        };
        self.existing_pointer = existing.clone();
        let Some(version_id) = self.pending_version_id.take() else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };
        if let Some(target_version_id) = self.input.version_id {
            self.delete_lookup_read(target_version_id, existing.as_ref())
        } else {
            self.pending_hash = None;
            if let Some(pointer) = existing.as_ref() {
                let key =
                    match VersionKey::new(&self.input.bucket, &self.input.key, pointer.version_id)
                        .to_bytes()
                    {
                        Ok(key) => key.into(),
                        Err(err) => return self.emit_error(err.into()),
                    };
                self.state = DeleteObjectState::ReadLivenessVersion;
                return smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                    key,
                    txn_id: self.txn_id,
                })];
            }
            self.live_before_marker = false;
            self.write_tombstone_pointer(version_id, existing.as_ref())
        }
    }

    fn liveness_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };

        self.live_before_marker = value
            .and_then(|value| BlobVersion::from_bytes(value.as_ref()).ok())
            .is_some_and(|version| !version.is_deleted());

        let Some(version_id) = self.version_id else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };
        let existing = self.existing_pointer.clone();
        self.write_tombstone_pointer(version_id, existing.as_ref())
    }

    fn delete_lookup_read(
        &mut self,
        target_version_id: Ulid,
        existing: Option<&CurrentVersionPointer>,
    ) -> Effects {
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
                self.write_current_lookup(version_id, existing)
            }
            None => {
                self.pending_new_pointer = None;
                self.pending_hash = None;
                self.delete_current_lookup()
            }
        }
    }

    fn write_current_lookup(
        &mut self,
        version_id: Ulid,
        existing: Option<&CurrentVersionPointer>,
    ) -> Effects {
        match CurrentVersionPointer::next_for(existing, version_id) {
            Ok(pointer) => self.pending_new_pointer = Some(pointer),
            Err(err) => return self.emit_error(err.into()),
        }
        self.prepare_head_transition(HeadTransitionContinuation::DeleteTargetVersion)
    }

    fn delete_current_lookup(&mut self) -> Effects {
        self.pending_new_pointer = None;
        self.prepare_head_transition(HeadTransitionContinuation::DeleteTargetVersion)
    }

    fn delete_target_version(&mut self) -> Effects {
        let Some(version_id) = self.input.version_id else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };

        if let Some(target_version) = self.target_version.as_ref()
            && let Some(materialized_hash) = target_version.materialized_hash
        {
            self.state = DeleteObjectState::DeletePathIndex;
            let effect = match delete_index_effect(
                &self.alias_context(),
                materialized_hash,
                version_id,
                self.txn_id,
            ) {
                Ok(effect) => effect,
                Err(err) => return self.emit_error(err.into()),
            };

            return smallvec![effect];
        }

        self.delete_version_record(version_id)
    }

    fn delete_version_record(&mut self, version_id: Ulid) -> Effects {
        self.state = DeleteObjectState::DeleteTargetVersion;
        let effect = match delete_version_effect(
            &VersionKey::new(&self.input.bucket, &self.input.key, version_id),
            self.txn_id,
        ) {
            Ok(effect) => effect,
            Err(err) => return self.emit_error(err.into()),
        };

        smallvec![effect]
    }

    fn target_path_deleted(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::DeleteResult { .. }) = event else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };

        let Some(version_id) = self.input.version_id else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };

        self.delete_version_record(version_id)
    }

    fn target_version_deleted(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::DeleteResult { .. }) = event else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };

        self.remove_managed_copies()
    }

    /// Joins the delete transaction, so no local registration outlives the
    /// logical version it made serveable.
    fn remove_managed_copies(&mut self) -> Effects {
        let Some(version_id) = self.input.version_id else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };
        let mut removal = match ManagedCopyRemoval::for_version(&VersionKey::new(
            &self.input.bucket,
            &self.input.key,
            version_id,
        )) {
            Ok(removal) => removal,
            Err(err) => return self.emit_error(err.into()),
        };
        let effects = removal.start(self.txn_id);
        self.copy_removal = Some(removal);
        self.state = DeleteObjectState::RemoveManagedCopies;
        effects
    }

    fn handle_copies_removed(&mut self, event: Event) -> Effects {
        let Some(removal) = self.copy_removal.as_mut() else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };
        match removal.step(event, self.txn_id) {
            Ok(Some(effects)) => effects,
            Ok(None) => {
                self.copy_removal = None;
                self.delete_multipart_summary()
            }
            Err(err) => self.emit_error(err.into()),
        }
    }

    fn delete_multipart_summary(&mut self) -> Effects {
        let Some(version_id) = self.input.version_id else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };
        self.state = DeleteObjectState::DeleteMultipartSummary;
        let key = match MultipartObjectKey::summary(version_id).to_bytes() {
            Ok(key) => key.into(),
            Err(err) => return self.emit_error(err.into()),
        };

        smallvec![Effect::Storage(StorageEffect::Delete {
            key_space: OBJECT_METADATA_KEYSPACE.to_string(),
            key,
            txn_id: self.txn_id,
        })]
    }

    fn summary_deleted(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::DeleteResult { .. }) = event else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };

        let Some(version_id) = self.input.version_id else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };
        let prefix = match MultipartObjectKey::part_prefix(version_id) {
            Ok(prefix) => prefix.into(),
            Err(err) => return self.emit_error(err.into()),
        };

        self.state = DeleteObjectState::ReadMultipartParts;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: OBJECT_METADATA_KEYSPACE.to_string(),
            prefix: Some(prefix),
            start: None,
            limit: 10_000,
            txn_id: self.txn_id,
        })]
    }

    fn parts_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult { values, .. }) = event else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };

        self.multipart_part_keys = values.into_iter().map(|(key, _)| key).collect();
        self.multipart_delete_index = 0;
        self.delete_next_part()
    }

    /// Queues the copy the deleted version named. Blind and idempotent: two
    /// aliases of one hash just refresh the row, and the sweep owns the recount.
    fn write_reclaim_candidate(&mut self) -> Effects {
        let Some(location) = self.target_location.clone() else {
            return self.start_usage_update();
        };
        let candidate = ReclaimCandidate {
            enqueued_at: SystemTime::now(),
        };
        let value = match candidate.to_bytes() {
            Ok(value) => value,
            Err(err) => return self.emit_error(err.into()),
        };
        self.state = DeleteObjectState::WriteReclaimCandidate;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: BLOB_RECLAIM_KEYSPACE.to_string(),
            key: ReclaimCandidateKey::new(location.backend, location.blake3_hash)
                .to_bytes()
                .into(),
            value: value.into(),
            txn_id: self.txn_id,
        })]
    }

    fn handle_candidate_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };

        self.start_usage_update()
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
            UsageDelta {
                objects,
                logical_bytes: if target.referenced { 0 } else { bytes },
                referenced_bytes: if target.referenced { bytes } else { 0 },
                ..Default::default()
            }
        } else {
            UsageDelta {
                objects: if self.live_before_marker { -1 } else { 0 },
                ..Default::default()
            }
        }
    }

    fn start_usage_update(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(DeleteObjectError::NoTransactionFound);
        };
        let delta = self.usage_delta();
        let mut update = UsageCounterUpdate::for_group(self.input.group_id, delta);
        if update.is_noop() {
            return self.write_delete_audit();
        }
        self.state = DeleteObjectState::UpdateUsage;
        let effects = update.start(txn_id);
        self.usage_update = Some(update);
        effects
    }

    fn handle_usage_update(&mut self, event: Event) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(DeleteObjectError::NoTransactionFound);
        };
        let Some(update) = self.usage_update.as_mut() else {
            return self.emit_error(DeleteObjectError::DeleteObjectFailed);
        };
        match update.step(event, txn_id) {
            Ok(Some(effects)) => effects,
            Ok(None) => self.write_delete_audit(),
            Err(err) => self.emit_error(err.into()),
        }
    }

    /// The delete trail is written inside the same transaction, so a committed
    /// deletion can never lack its record and an aborted one never leaves one.
    fn write_delete_audit(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(DeleteObjectError::NoTransactionFound);
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
        let value = match record.to_bytes() {
            Ok(value) => value,
            Err(error) => return self.emit_error(error.into()),
        };
        self.state = DeleteObjectState::WriteDeleteAudit;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: DELETE_AUDIT_KEYSPACE.to_string(),
            key: delete_audit_key(self.input.group_id, Ulid::generate()).into(),
            value: value.into(),
            txn_id: Some(txn_id),
        })]
    }

    fn handle_delete_audit(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(DeleteObjectError::NoTransactionFound);
        };
        self.state = DeleteObjectState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn delete_next_part(&mut self) -> Effects {
        let Some(key) = self
            .multipart_part_keys
            .get(self.multipart_delete_index)
            .cloned()
        else {
            return self.write_reclaim_candidate();
        };

        self.state = DeleteObjectState::DeleteMultipartPart;
        smallvec![Effect::Storage(StorageEffect::Delete {
            key_space: OBJECT_METADATA_KEYSPACE.to_string(),
            key,
            txn_id: self.txn_id,
        })]
    }

    fn part_deleted(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::DeleteResult { .. }) = event else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };

        self.multipart_delete_index += 1;
        self.delete_next_part()
    }

    fn write_tombstone(&mut self) -> Effects {
        let version_id = Ulid::generate();
        self.version_id = Some(version_id);
        self.deleted_version_created = Some(SystemTime::now());
        self.read_current_lookup(version_id)
    }

    fn write_tombstone_pointer(
        &mut self,
        version_id: Ulid,
        existing: Option<&CurrentVersionPointer>,
    ) -> Effects {
        match CurrentVersionPointer::next_for(existing, version_id) {
            Ok(pointer) => self.pending_new_pointer = Some(pointer),
            Err(err) => return self.emit_error(err.into()),
        }
        self.prepare_head_transition(HeadTransitionContinuation::WriteDeletedVersion)
    }

    fn write_deleted_version(&mut self) -> Effects {
        let Some(version_id) = self.version_id else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };
        let created_at = self
            .deleted_version_created
            .get_or_insert_with(SystemTime::now)
            .to_owned();
        let mut version = BlobVersion::deleted(created_at, self.input.deleted_by);
        version.metadata = self.metadata.clone();
        let version_key = VersionKey::new(&self.input.bucket, &self.input.key, version_id);
        let effect = match write_version_effect(&version_key, &version, self.txn_id) {
            Ok(effect) => effect,
            Err(err) => return self.emit_error(err.into()),
        };

        self.state = DeleteObjectState::WriteBlobVersion;
        smallvec![effect]
    }

    fn version_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };
        self.write_obligation()
    }

    fn write_obligation(&mut self) -> Effects {
        let Some(version_id) = self.version_id else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };
        let effect = match build_live_obligation(
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
            self.txn_id,
        ) {
            Ok(effect) => effect,
            Err(err) => return self.emit_error(err.into()),
        };
        self.state = DeleteObjectState::WriteReplicationObligation;
        smallvec![effect]
    }

    fn obligation_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };

        self.start_usage_update()
    }

    fn handle_transaction_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };

        let Some(version_id) = self.version_id else {
            if let Some(version_id) = self.input.version_id {
                let delete_marker = self
                    .target_version
                    .as_ref()
                    .is_some_and(VersionSummary::is_deleted);
                self.txn_id = None;
                self.state = DeleteObjectState::Finish;
                self.output = Some(Ok(DeleteObjectResult {
                    version_id,
                    delete_marker,
                }));
                // The sweep owns reclaim cadence, so deletes do not re-arm its timer.
                return smallvec![schedule_snapshot_publish()];
            }
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };
        self.txn_id = None;
        self.state = DeleteObjectState::Finish;
        self.output = Some(Ok(DeleteObjectResult {
            version_id,
            delete_marker: true,
        }));
        smallvec![schedule_snapshot_publish()]
    }
}

impl Operation for DeleteObjectOperation {
    type Output = DeleteObjectResult;
    type Error = DeleteObjectError;

    fn start(&mut self) -> Effects {
        if self.state != DeleteObjectState::Init {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        }
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = &event {
            return self.emit_error(error.clone().into());
        }
        match self.state {
            DeleteObjectState::Init => self.handle_init(),
            DeleteObjectState::StartTransaction => self.handle_transaction_started(event),
            DeleteObjectState::CheckPurgeFence => self.fence_checked(event),
            DeleteObjectState::ReadTargetVersion => self.target_version_read(event),
            DeleteObjectState::ReadTargetLocation => self.target_location_read(event),
            DeleteObjectState::ReadAllVersions => self.versions_read(event),
            DeleteObjectState::ReadCurrentLookup => self.current_lookup_read(event),
            DeleteObjectState::ReadLivenessVersion => self.liveness_read(event),
            DeleteObjectState::ApplyHeadTransition => self.head_transition_applied(event),
            DeleteObjectState::DeletePathIndex => self.target_path_deleted(event),
            DeleteObjectState::DeleteTargetVersion => self.target_version_deleted(event),
            DeleteObjectState::RemoveManagedCopies => self.handle_copies_removed(event),
            DeleteObjectState::DeleteMultipartSummary => self.summary_deleted(event),
            DeleteObjectState::ReadMultipartParts => self.parts_read(event),
            DeleteObjectState::DeleteMultipartPart => self.part_deleted(event),
            DeleteObjectState::WriteReclaimCandidate => self.handle_candidate_written(event),
            DeleteObjectState::WriteBlobVersion => self.version_written(event),
            DeleteObjectState::WriteReplicationObligation => self.obligation_written(event),
            DeleteObjectState::UpdateUsage => self.handle_usage_update(event),
            DeleteObjectState::WriteDeleteAudit => self.handle_delete_audit(event),
            DeleteObjectState::CommitTransaction => self.handle_transaction_committed(event),
            DeleteObjectState::Finish => smallvec![],
            DeleteObjectState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            DeleteObjectState::Finish | DeleteObjectState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.output {
            Some(Ok(value)) => Ok(value),
            Some(Err(error)) => Err(error),
            None => Err(DeleteObjectError::NotFinished),
        }
    }

    fn abort(&mut self) -> Effects {
        self.txn_id
            .take()
            .map_or_else(smallvec::SmallVec::new, |txn_id| {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            })
    }
}

#[cfg(test)]
#[path = "delete_tests.rs"]
mod test;
