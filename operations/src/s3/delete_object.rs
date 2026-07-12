use crate::blob::blob_keyspace_helper::{
    HeadAliasContext, build_head_transition_effects, delete_blob_version_effect,
    delete_hash_path_index_effect, write_blob_version_effect,
};
use crate::replication::queue::write_live_replication_obligation_effect;
use crate::usage_stats::{
    UsageCounterUpdate, UsageUpdateError, schedule_usage_snapshot_publish_effect,
};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE,
    S3_MULTIPART_OBJECT_METADATA_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::{
    AuthContext, BackendLocation, BlobHeadKey, BlobVersion, BlobVersionState,
    CurrentVersionPointer, MultipartObjectMetadataKey, RealmId, UsageDelta, VersionKey,
};
use aruna_core::types::{Effects, GroupId, Key, NodeId, UserId};
use smallvec::smallvec;
use std::collections::VecDeque;
use std::time::SystemTime;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeleteObjectState {
    Init,
    StartTransaction,
    ReadTargetVersion,
    ReadTargetLocation,
    ReadAllVersions,
    ReadCurrentLookup,
    ReadLivenessVersion,
    ApplyHeadTransition,
    DeleteTargetHashPathIndex,
    DeleteTargetVersion,
    DeleteMultipartSummary,
    ReadMultipartParts,
    DeleteMultipartPart,
    WriteBlobVersion,
    WriteLiveReplicationObligation,
    UpdateUsage,
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
    deleted: bool,
}

impl VersionSummary {
    fn from_blob_version(version_id: Ulid, version: &BlobVersion) -> Self {
        let (materialized_hash, logical_size) = match &version.state {
            BlobVersionState::Materialized { blob_hash, .. } => (Some(*blob_hash), None),
            BlobVersionState::Reference {
                cached_metadata, ..
            } => (None, Some(cached_metadata.content_length)),
            BlobVersionState::Deleted => (None, None),
        };
        Self {
            version_id,
            materialized_hash,
            logical_size,
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
    #[error("DeleteObject failed")]
    DeleteObjectFailed,
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
    pending_current_version_id: Option<Ulid>,
    pending_new_pointer: Option<CurrentVersionPointer>,
    pending_new_current_hash: Option<[u8; 32]>,
    pending_head_transition_effects: VecDeque<Effect>,
    pending_head_transition_next: Option<HeadTransitionContinuation>,
    deleted_version_created_at: Option<SystemTime>,
    multipart_part_keys: Vec<Key>,
    multipart_delete_index: usize,
    target_size: Option<u64>,
    live_before_marker: bool,
    usage_update: Option<UsageCounterUpdate>,
    output: Option<Result<DeleteObjectResult, DeleteObjectError>>,
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
            pending_current_version_id: None,
            pending_new_pointer: None,
            pending_new_current_hash: None,
            pending_head_transition_effects: VecDeque::new(),
            pending_head_transition_next: None,
            deleted_version_created_at: None,
            multipart_part_keys: Vec::new(),
            multipart_delete_index: 0,
            target_size: None,
            live_before_marker: false,
            usage_update: None,
            output: None,
        }
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
        let effects = match build_head_transition_effects(
            &self.alias_context(),
            self.pending_new_pointer.take(),
            self.pending_new_current_hash.take(),
            self.txn_id,
        ) {
            Ok(effects) => effects,
            Err(err) => return self.emit_error(err.into()),
        };

        self.pending_head_transition_effects = effects.into_iter().collect();
        self.pending_head_transition_next = Some(next);
        self.state = DeleteObjectState::ApplyHeadTransition;
        self.emit_next_head_transition_effect_or_continue()
    }

    fn emit_next_head_transition_effect_or_continue(&mut self) -> Effects {
        if let Some(effect) = self.pending_head_transition_effects.pop_front() {
            return smallvec![effect];
        }

        match self.pending_head_transition_next.take() {
            Some(HeadTransitionContinuation::DeleteTargetVersion) => self.delete_target_version(),
            Some(HeadTransitionContinuation::WriteDeletedVersion) => self.write_deleted_version(),
            None => self.emit_error(DeleteObjectError::DeleteObjectFailed),
        }
    }

    fn handle_head_transition_applied(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::WriteResult { .. })
            | Event::Storage(StorageEvent::DeleteResult { .. }) => {
                self.emit_next_head_transition_effect_or_continue()
            }
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
            self.write_tombstone()
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

    fn handle_target_version_read(&mut self, event: Event) -> Effects {
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
        let summary = VersionSummary::from_blob_version(version_id, &version);
        let materialized_hash = summary.materialized_hash;
        self.target_size = summary.logical_size;
        self.target_version = Some(summary);

        if let Some(hash) = materialized_hash {
            self.state = DeleteObjectState::ReadTargetLocation;
            return smallvec![Effect::Storage(StorageEffect::Read {
                key_space: BLOB_LOCATIONS_KEYSPACE.to_string(),
                key: hash.to_vec().into(),
                txn_id: self.txn_id,
            })];
        }

        self.read_all_versions()
    }

    fn handle_target_location_read(&mut self, event: Event) -> Effects {
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

    fn handle_all_versions_read(&mut self, event: Event) -> Effects {
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
        self.pending_current_version_id = Some(version_id);
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

    fn handle_current_lookup_read(&mut self, event: Event) -> Effects {
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
        let Some(version_id) = self.pending_current_version_id.take() else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };
        if let Some(target_version_id) = self.input.version_id {
            self.handle_version_delete_current_lookup(target_version_id, existing.as_ref())
        } else {
            self.pending_new_current_hash = None;
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
            self.write_tombstone_current_lookup(version_id, existing.as_ref())
        }
    }

    fn handle_liveness_version_read(&mut self, event: Event) -> Effects {
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
        self.write_tombstone_current_lookup(version_id, existing.as_ref())
    }

    fn handle_version_delete_current_lookup(
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
                self.pending_new_pointer =
                    Some(CurrentVersionPointer::next_for(existing, version_id));
                self.pending_new_current_hash = new_hash;
                self.write_current_lookup(version_id, existing)
            }
            None => {
                self.pending_new_pointer = None;
                self.pending_new_current_hash = None;
                self.delete_current_lookup()
            }
        }
    }

    fn write_current_lookup(
        &mut self,
        version_id: Ulid,
        existing: Option<&CurrentVersionPointer>,
    ) -> Effects {
        self.pending_new_pointer = Some(CurrentVersionPointer::next_for(existing, version_id));
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
            self.state = DeleteObjectState::DeleteTargetHashPathIndex;
            let effect = match delete_hash_path_index_effect(
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

        self.delete_target_version_record(version_id)
    }

    fn delete_target_version_record(&mut self, version_id: Ulid) -> Effects {
        self.state = DeleteObjectState::DeleteTargetVersion;
        let effect = match delete_blob_version_effect(
            &VersionKey::new(&self.input.bucket, &self.input.key, version_id),
            self.txn_id,
        ) {
            Ok(effect) => effect,
            Err(err) => return self.emit_error(err.into()),
        };

        smallvec![effect]
    }

    fn handle_target_hash_path_deleted(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::DeleteResult { .. }) = event else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };

        let Some(version_id) = self.input.version_id else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };

        self.delete_target_version_record(version_id)
    }

    fn handle_target_version_deleted(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::DeleteResult { .. }) = event else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };

        let Some(version_id) = self.input.version_id else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };
        self.state = DeleteObjectState::DeleteMultipartSummary;
        let key = match MultipartObjectMetadataKey::summary(version_id).to_bytes() {
            Ok(key) => key.into(),
            Err(err) => return self.emit_error(err.into()),
        };

        smallvec![Effect::Storage(StorageEffect::Delete {
            key_space: S3_MULTIPART_OBJECT_METADATA_KEYSPACE.to_string(),
            key,
            txn_id: self.txn_id,
        })]
    }

    fn handle_multipart_summary_deleted(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::DeleteResult { .. }) = event else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };

        let Some(version_id) = self.input.version_id else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };
        let prefix = match MultipartObjectMetadataKey::part_prefix(version_id) {
            Ok(prefix) => prefix.into(),
            Err(err) => return self.emit_error(err.into()),
        };

        self.state = DeleteObjectState::ReadMultipartParts;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: S3_MULTIPART_OBJECT_METADATA_KEYSPACE.to_string(),
            prefix: Some(prefix),
            start: None,
            limit: 10_000,
            txn_id: self.txn_id,
        })]
    }

    fn handle_multipart_parts_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult { values, .. }) = event else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };

        self.multipart_part_keys = values.into_iter().map(|(key, _)| key).collect();
        self.multipart_delete_index = 0;
        self.delete_next_multipart_part()
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
            let logical_bytes = self.target_size.map_or(0, |size| -i128::from(size));
            UsageDelta {
                objects,
                logical_bytes,
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
            self.state = DeleteObjectState::CommitTransaction;
            return smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })];
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
            Ok(None) => {
                self.state = DeleteObjectState::CommitTransaction;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            Err(err) => self.emit_error(err.into()),
        }
    }

    fn delete_next_multipart_part(&mut self) -> Effects {
        let Some(key) = self
            .multipart_part_keys
            .get(self.multipart_delete_index)
            .cloned()
        else {
            return self.start_usage_update();
        };

        self.state = DeleteObjectState::DeleteMultipartPart;
        smallvec![Effect::Storage(StorageEffect::Delete {
            key_space: S3_MULTIPART_OBJECT_METADATA_KEYSPACE.to_string(),
            key,
            txn_id: self.txn_id,
        })]
    }

    fn handle_multipart_part_deleted(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::DeleteResult { .. }) = event else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };

        self.multipart_delete_index += 1;
        self.delete_next_multipart_part()
    }

    fn write_tombstone(&mut self) -> Effects {
        let version_id = Ulid::r#gen();
        self.version_id = Some(version_id);
        self.deleted_version_created_at = Some(SystemTime::now());
        self.read_current_lookup(version_id)
    }

    fn write_tombstone_current_lookup(
        &mut self,
        version_id: Ulid,
        existing: Option<&CurrentVersionPointer>,
    ) -> Effects {
        self.pending_new_pointer = Some(CurrentVersionPointer::next_for(existing, version_id));
        self.prepare_head_transition(HeadTransitionContinuation::WriteDeletedVersion)
    }

    fn write_deleted_version(&mut self) -> Effects {
        let Some(version_id) = self.version_id else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };
        let created_at = self
            .deleted_version_created_at
            .get_or_insert_with(SystemTime::now)
            .to_owned();
        let version = BlobVersion::deleted(created_at, self.input.deleted_by);
        let version_key = VersionKey::new(&self.input.bucket, &self.input.key, version_id);
        let effect = match write_blob_version_effect(&version_key, &version, self.txn_id) {
            Ok(effect) => effect,
            Err(err) => return self.emit_error(err.into()),
        };

        self.state = DeleteObjectState::WriteBlobVersion;
        smallvec![effect]
    }

    fn handle_blob_version_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };
        self.write_live_replication_obligation()
    }

    fn write_live_replication_obligation(&mut self) -> Effects {
        let Some(version_id) = self.version_id else {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };
        let effect = match write_live_replication_obligation_effect(
            self.input.node_id,
            AuthContext {
                user_id: self.input.deleted_by,
                realm_id: self.input.realm_id,
                path_restrictions: None,
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
        self.state = DeleteObjectState::WriteLiveReplicationObligation;
        smallvec![effect]
    }

    fn handle_live_replication_obligation_written(&mut self, event: Event) -> Effects {
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
                return smallvec![schedule_usage_snapshot_publish_effect()];
            }
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        };
        self.txn_id = None;
        self.state = DeleteObjectState::Finish;
        self.output = Some(Ok(DeleteObjectResult {
            version_id,
            delete_marker: true,
        }));
        smallvec![schedule_usage_snapshot_publish_effect()]
    }
}

impl Operation for DeleteObjectOperation {
    type Output = Option<Result<DeleteObjectResult, DeleteObjectError>>;
    type Error = DeleteObjectError;

    fn start(&mut self) -> Effects {
        if self.state != DeleteObjectState::Init {
            return self.emit_error(DeleteObjectError::InvalidOperationState);
        }
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            DeleteObjectState::Init => self.handle_init(),
            DeleteObjectState::StartTransaction => self.handle_transaction_started(event),
            DeleteObjectState::ReadTargetVersion => self.handle_target_version_read(event),
            DeleteObjectState::ReadTargetLocation => self.handle_target_location_read(event),
            DeleteObjectState::ReadAllVersions => self.handle_all_versions_read(event),
            DeleteObjectState::ReadCurrentLookup => self.handle_current_lookup_read(event),
            DeleteObjectState::ReadLivenessVersion => self.handle_liveness_version_read(event),
            DeleteObjectState::ApplyHeadTransition => self.handle_head_transition_applied(event),
            DeleteObjectState::DeleteTargetHashPathIndex => {
                self.handle_target_hash_path_deleted(event)
            }
            DeleteObjectState::DeleteTargetVersion => self.handle_target_version_deleted(event),
            DeleteObjectState::DeleteMultipartSummary => {
                self.handle_multipart_summary_deleted(event)
            }
            DeleteObjectState::ReadMultipartParts => self.handle_multipart_parts_read(event),
            DeleteObjectState::DeleteMultipartPart => self.handle_multipart_part_deleted(event),
            DeleteObjectState::WriteBlobVersion => self.handle_blob_version_written(event),
            DeleteObjectState::WriteLiveReplicationObligation => {
                self.handle_live_replication_obligation_written(event)
            }
            DeleteObjectState::UpdateUsage => self.handle_usage_update(event),
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
        if self.state == DeleteObjectState::Error {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(DeleteObjectError::DeleteObjectFailed);
        }
        Ok(self.output)
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
mod test {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use crate::s3::get_object::{GetObjectError, GetObjectInput, GetObjectOperation};
    use crate::s3::put_object::{PutObjectConfig, PutObjectInput, PutObjectOperation};
    use aruna_blob::blob::BlobHandler;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{
        BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, HASH_PATHS_INDEX_KEYSPACE,
    };
    use aruna_core::stream::BackendStream;
    use aruna_core::structs::{
        Backend, BackendConfig, BlobHeadKey, BlobVersion, CurrentVersionPointer, HashPathIndexKey,
        RealmId, VersionKey,
    };
    use aruna_net::{NetConfig, NetHandle};
    use aruna_storage::storage;
    use futures_util::StreamExt;
    use std::collections::HashMap;
    use std::fs::exists;
    use tempfile::tempdir;
    use ulid::Ulid;

    fn test_user_id() -> aruna_core::UserId {
        aruna_core::UserId::local(Ulid::r#gen(), RealmId::from_bytes([1u8; 32]))
    }

    fn deleted_version_value(user_id: aruna_core::UserId) -> aruna_core::types::Value {
        BlobVersion::deleted(SystemTime::UNIX_EPOCH, user_id)
            .to_bytes()
            .unwrap()
            .into()
    }

    fn deleted_version_entry(
        bucket: &str,
        key: &str,
        version_id: Ulid,
        user_id: aruna_core::UserId,
    ) -> (aruna_core::types::Key, aruna_core::types::Value) {
        (
            VersionKey::new(bucket, key, version_id)
                .to_bytes()
                .unwrap()
                .into(),
            deleted_version_value(user_id),
        )
    }

    async fn read_value(
        context: &DriverContext,
        key_space: &str,
        key: Vec<u8>,
    ) -> Option<aruna_core::types::Value> {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: key_space.to_string(),
                key: key.into(),
                txn_id: None,
            })
            .await
        else {
            panic!("unexpected storage event");
        };

        value
    }

    fn version_delete_until_current_lookup(
        target_version_id: Ulid,
        all_version_ids: Vec<Ulid>,
        user_id: aruna_core::UserId,
    ) -> DeleteObjectOperation {
        let mut op = DeleteObjectOperation::new(DeleteObjectInput {
            bucket: "bucket".to_string(),
            key: "key".to_string(),
            version_id: Some(target_version_id),
            group_id: Ulid::r#gen(),
            realm_id: RealmId::from_bytes([1u8; 32]),
            node_id: iroh::SecretKey::generate().public(),
            deleted_by: user_id,
        });

        let effects = op.start();
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::StartTransaction {
                read: false
            })]
        ));

        let effects = op.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: Ulid::r#gen(),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { key_space, .. })]
                if key_space == BLOB_VERSIONS_KEYSPACE
        ));

        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![1u8].into(),
            value: Some(deleted_version_value(user_id)),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Iter { key_space, .. })]
                if key_space == BLOB_VERSIONS_KEYSPACE
        ));

        let effects = op.step(Event::Storage(StorageEvent::IterResult {
            values: all_version_ids
                .into_iter()
                .map(|version_id| deleted_version_entry("bucket", "key", version_id, user_id))
                .collect(),
            next_start_after: None,
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { key_space, .. })]
                if key_space == BLOB_HEAD_KEYSPACE
        ));

        op
    }

    #[test]
    fn version_delete_preserves_current_pointer_when_target_is_not_current() {
        let user_id = test_user_id();
        let current_version_id = Ulid::from_bytes([1u8; 16]);
        let target_version_id = Ulid::from_bytes([2u8; 16]);
        let newer_version_id = Ulid::from_bytes([9u8; 16]);
        let mut op = version_delete_until_current_lookup(
            target_version_id,
            vec![current_version_id, target_version_id, newer_version_id],
            user_id,
        );
        let current_pointer = CurrentVersionPointer::new_with_generation(current_version_id, 20);

        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![2u8].into(),
            value: Some(current_pointer.to_bytes().unwrap().into()),
        }));

        assert_eq!(op.state, DeleteObjectState::DeleteTargetVersion);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Delete { key_space, .. })]
                if key_space == BLOB_VERSIONS_KEYSPACE
        ));
    }

    #[test]
    fn version_delete_rewrites_current_pointer_when_target_is_current() {
        let user_id = test_user_id();
        let target_version_id = Ulid::from_bytes([1u8; 16]);
        let fallback_version_id = Ulid::from_bytes([9u8; 16]);
        let mut op = version_delete_until_current_lookup(
            target_version_id,
            vec![target_version_id, fallback_version_id],
            user_id,
        );
        let current_pointer = CurrentVersionPointer::new_with_generation(target_version_id, 20);

        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![2u8].into(),
            value: Some(current_pointer.to_bytes().unwrap().into()),
        }));

        let [
            Effect::Storage(StorageEffect::Write {
                key_space, value, ..
            }),
        ] = effects.as_slice()
        else {
            panic!("expected blob head write");
        };
        assert_eq!(key_space, BLOB_HEAD_KEYSPACE);
        assert_eq!(
            CurrentVersionPointer::from_bytes(value.as_ref()).unwrap(),
            CurrentVersionPointer::new_with_generation(fallback_version_id, 21)
        );
    }

    #[test]
    fn version_delete_removes_current_pointer_when_current_has_no_fallback() {
        let user_id = test_user_id();
        let target_version_id = Ulid::from_bytes([1u8; 16]);
        let mut op = version_delete_until_current_lookup(
            target_version_id,
            vec![target_version_id],
            user_id,
        );
        let current_pointer = CurrentVersionPointer::new_with_generation(target_version_id, 20);

        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![2u8].into(),
            value: Some(current_pointer.to_bytes().unwrap().into()),
        }));

        assert_eq!(op.state, DeleteObjectState::ApplyHeadTransition);
        assert_eq!(effects.len(), 1);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Delete { key_space, .. })]
                if key_space == BLOB_HEAD_KEYSPACE
        ));
    }

    #[tokio::test]
    async fn drive_version_delete_missing_version_returns_no_such_version() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        };

        let result = drive(
            DeleteObjectOperation::new(DeleteObjectInput {
                bucket: "mybucket".to_string(),
                key: "missing.txt".to_string(),
                version_id: Some(Ulid::r#gen()),
                group_id: Ulid::r#gen(),
                realm_id: RealmId::from_bytes([1u8; 32]),
                node_id: iroh::SecretKey::generate().public(),
                deleted_by: test_user_id(),
            }),
            &context,
        )
        .await;

        assert!(matches!(result, Err(DeleteObjectError::NoSuchVersion)));
    }

    #[tokio::test]
    async fn test_delete_object_tombstone() {
        let temp_handle = tempdir().unwrap();
        let temp_root = temp_handle.path().to_str().unwrap();
        let blob_root = format!("{temp_root}/blobstore");
        std::fs::create_dir_all(&blob_root).unwrap();

        let storage_handle = storage::FjallStorage::open(temp_root).unwrap();
        let net_handle = NetHandle::new(NetConfig::default(), storage_handle.clone())
            .await
            .unwrap();
        let blob_handle = BlobHandler::new(
            BackendConfig {
                backend_type: Backend::FileSystem,
                bucket_prefix: Some("aruna_".to_string()),
                max_bucket_size: Some(100000),
                multipart_bucket: Some("multipart".to_string()),
                root: blob_root,
                service_config: HashMap::new(),
                timeouts: Default::default(),
            },
            storage_handle.clone(),
            net_handle.clone(),
        )
        .await
        .unwrap();

        let context = DriverContext {
            storage_handle,
            net_handle: Some(net_handle),
            blob_handle: Some(blob_handle),
            metadata_handle: None,
            task_handle: None,
        };

        let user_id = aruna_core::UserId::local(Ulid::r#gen(), RealmId::from_bytes([1u8; 32]));
        let group_id = Ulid::r#gen();
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let node_id = context.net_handle.as_ref().unwrap().node_id();
        let put_result = drive(
            PutObjectOperation::new(PutObjectConfig {
                user_id,
                group_id,
                realm_id,
                node_id,
                request: PutObjectInput {
                    bucket: "mybucket".to_string(),
                    key: "to-delete.txt".to_string(),
                    content_length: Some(5),
                    body: Some(BackendStream::new(tokio_util::io::ReaderStream::new(
                        &b"hello"[..],
                    ))),
                },
                expected_checksums: vec![],
                checksum_type: None,
                exists: false,
                version_source: None,
                quota_ceiling: None,
            }),
            &context,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        let delete_result = drive(
            DeleteObjectOperation::new(DeleteObjectInput {
                bucket: "mybucket".to_string(),
                key: "to-delete.txt".to_string(),
                version_id: None,
                group_id,
                realm_id,
                node_id,
                deleted_by: user_id,
            }),
            &context,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        assert!(exists(put_result.location.get_full_path().unwrap()).unwrap());

        let blob_head = read_value(
            &context,
            BLOB_HEAD_KEYSPACE,
            BlobHeadKey::new("mybucket", "to-delete.txt")
                .to_bytes()
                .unwrap(),
        )
        .await
        .expect("missing blob head entry");
        assert_eq!(
            CurrentVersionPointer::from_bytes(blob_head.as_ref()).unwrap(),
            CurrentVersionPointer::new_with_generation(delete_result.version_id, 2)
        );

        let blob_tombstone = read_value(
            &context,
            BLOB_VERSIONS_KEYSPACE,
            VersionKey::new("mybucket", "to-delete.txt", delete_result.version_id)
                .to_bytes()
                .unwrap(),
        )
        .await
        .expect("missing blob tombstone version");
        let blob_tombstone = BlobVersion::from_bytes(blob_tombstone.as_ref()).unwrap();
        assert!(blob_tombstone.is_deleted());
        assert_eq!(blob_tombstone.created_by, user_id);

        let historical_hash_path = read_value(
            &context,
            HASH_PATHS_INDEX_KEYSPACE,
            HashPathIndexKey::new(
                put_result
                    .location
                    .get_blake3()
                    .unwrap()
                    .try_into()
                    .unwrap(),
                put_result.version_id,
                realm_id,
                group_id,
                node_id,
                "mybucket",
                "to-delete.txt",
            )
            .to_bytes()
            .unwrap(),
        )
        .await
        .expect("missing historical materialized hash path entry");
        assert!(historical_hash_path.is_empty());

        let get_result = drive(
            GetObjectOperation::new(GetObjectInput {
                bucket: "mybucket".to_string(),
                key: "to-delete.txt".to_string(),
                version_id: None,
                range: None,
                group_id: Ulid::r#gen(),
                user_identity: user_id,
            }),
            &context,
        )
        .await;
        assert!(matches!(get_result, Err(GetObjectError::NoSuchKey)));
    }

    #[tokio::test]
    async fn test_delete_object_version() {
        let temp_handle = tempdir().unwrap();
        let temp_root = temp_handle.path().to_str().unwrap();
        let blob_root = format!("{temp_root}/blobstore");
        std::fs::create_dir_all(&blob_root).unwrap();

        let storage_handle = storage::FjallStorage::open(temp_root).unwrap();
        let net_handle = NetHandle::new(NetConfig::default(), storage_handle.clone())
            .await
            .unwrap();
        let blob_handle = BlobHandler::new(
            BackendConfig {
                backend_type: Backend::FileSystem,
                bucket_prefix: Some("aruna_".to_string()),
                max_bucket_size: Some(100000),
                multipart_bucket: Some("multipart".to_string()),
                root: blob_root,
                service_config: HashMap::new(),
                timeouts: Default::default(),
            },
            storage_handle.clone(),
            net_handle.clone(),
        )
        .await
        .unwrap();

        let context = DriverContext {
            storage_handle,
            net_handle: Some(net_handle),
            blob_handle: Some(blob_handle),
            metadata_handle: None,
            task_handle: None,
        };

        let user_id = aruna_core::UserId::local(Ulid::r#gen(), RealmId::from_bytes([1u8; 32]));
        let group_id = Ulid::r#gen();
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let node_id = context.net_handle.as_ref().unwrap().node_id();
        let put_result = drive(
            PutObjectOperation::new(PutObjectConfig {
                user_id,
                group_id,
                realm_id,
                node_id,
                request: PutObjectInput {
                    bucket: "mybucket".to_string(),
                    key: "versioned.txt".to_string(),
                    content_length: Some(5),
                    body: Some(BackendStream::new(tokio_util::io::ReaderStream::new(
                        &b"hello"[..],
                    ))),
                },
                expected_checksums: vec![],
                checksum_type: None,
                exists: false,
                version_source: None,
                quota_ceiling: None,
            }),
            &context,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        let tombstone = drive(
            DeleteObjectOperation::new(DeleteObjectInput {
                bucket: "mybucket".to_string(),
                key: "versioned.txt".to_string(),
                version_id: None,
                group_id,
                realm_id,
                node_id,
                deleted_by: user_id,
            }),
            &context,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        assert!(tombstone.delete_marker);

        let delete_marker_result = drive(
            DeleteObjectOperation::new(DeleteObjectInput {
                bucket: "mybucket".to_string(),
                key: "versioned.txt".to_string(),
                version_id: Some(tombstone.version_id),
                group_id,
                realm_id,
                node_id,
                deleted_by: user_id,
            }),
            &context,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        assert!(delete_marker_result.delete_marker);

        let blob_head = read_value(
            &context,
            BLOB_HEAD_KEYSPACE,
            BlobHeadKey::new("mybucket", "versioned.txt")
                .to_bytes()
                .unwrap(),
        )
        .await
        .expect("missing blob head entry after deleting marker");
        assert_eq!(
            CurrentVersionPointer::from_bytes(blob_head.as_ref()).unwrap(),
            CurrentVersionPointer::new_with_generation(put_result.version_id, 3)
        );

        let restored_hash_path = read_value(
            &context,
            HASH_PATHS_INDEX_KEYSPACE,
            HashPathIndexKey::new(
                put_result
                    .location
                    .get_blake3()
                    .unwrap()
                    .try_into()
                    .unwrap(),
                put_result.version_id,
                realm_id,
                group_id,
                node_id,
                "mybucket",
                "versioned.txt",
            )
            .to_bytes()
            .unwrap(),
        )
        .await
        .expect("missing restored hash path index");
        assert!(restored_hash_path.is_empty());

        assert!(
            read_value(
                &context,
                BLOB_VERSIONS_KEYSPACE,
                VersionKey::new("mybucket", "versioned.txt", tombstone.version_id)
                    .to_bytes()
                    .unwrap(),
            )
            .await
            .is_none()
        );

        let mut restored_blob = drive(
            GetObjectOperation::new(GetObjectInput {
                bucket: "mybucket".to_string(),
                key: "versioned.txt".to_string(),
                version_id: None,
                range: None,
                group_id: Ulid::r#gen(),
                user_identity: user_id,
            }),
            &context,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .blob;
        let restored = restored_blob.next().await.unwrap().unwrap();
        assert_eq!(restored.as_ref(), b"hello");

        let removed_object_version = drive(
            DeleteObjectOperation::new(DeleteObjectInput {
                bucket: "mybucket".to_string(),
                key: "versioned.txt".to_string(),
                version_id: Some(put_result.version_id),
                group_id,
                realm_id,
                node_id,
                deleted_by: user_id,
            }),
            &context,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        assert!(!removed_object_version.delete_marker);

        assert!(
            read_value(
                &context,
                BLOB_VERSIONS_KEYSPACE,
                VersionKey::new("mybucket", "versioned.txt", put_result.version_id)
                    .to_bytes()
                    .unwrap(),
            )
            .await
            .is_none()
        );

        let get_result = drive(
            GetObjectOperation::new(GetObjectInput {
                bucket: "mybucket".to_string(),
                key: "versioned.txt".to_string(),
                version_id: None,
                range: None,
                group_id: Ulid::r#gen(),
                user_identity: user_id,
            }),
            &context,
        )
        .await;
        assert!(matches!(get_result, Err(GetObjectError::NoSuchKey)));

        assert!(
            read_value(
                &context,
                BLOB_HEAD_KEYSPACE,
                BlobHeadKey::new("mybucket", "versioned.txt")
                    .to_bytes()
                    .unwrap(),
            )
            .await
            .is_none()
        );

        assert!(
            read_value(
                &context,
                HASH_PATHS_INDEX_KEYSPACE,
                HashPathIndexKey::new(
                    put_result
                        .location
                        .get_blake3()
                        .unwrap()
                        .try_into()
                        .unwrap(),
                    put_result.version_id,
                    realm_id,
                    group_id,
                    node_id,
                    "mybucket",
                    "versioned.txt",
                )
                .to_bytes()
                .unwrap(),
            )
            .await
            .is_none()
        );

        assert!(
            read_value(
                &context,
                HASH_PATHS_INDEX_KEYSPACE,
                HashPathIndexKey::new(
                    put_result
                        .location
                        .get_blake3()
                        .unwrap()
                        .try_into()
                        .unwrap(),
                    put_result.version_id,
                    realm_id,
                    group_id,
                    node_id,
                    "mybucket",
                    "versioned.txt",
                )
                .to_bytes()
                .unwrap(),
            )
            .await
            .is_none()
        );
    }
}
