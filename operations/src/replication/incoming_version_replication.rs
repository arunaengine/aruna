use crate::blob::blob_keyspace_helper::{
    HeadAliasContext, build_head_transition_effects, write_blob_location_effect,
    write_blob_version_effect,
};
use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::replication::error::ReplicationError;
use crate::replication::protocol::{VersionReplicationManifest, VersionReplicationMessage};
use crate::replication::util::dht_registration_effect;
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, DhtEvent, Event, NetEvent, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, S3_BUCKET_KEYSPACE,
    S3_MULTIPART_OBJECT_METADATA_KEYSPACE,
};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::{
    AuthContext, BackendLocation, BlobHeadKey, BlobVersion, BucketInfo, CurrentVersionPointer,
    MultipartObjectMetadataKey, Permission, RealmId, ReplicationItemKind,
    ReplicationNegotiationResult, VersionKey, blob_bucket_permission_path,
    blob_object_permission_path,
};
use aruna_core::types::{Effects, GroupId, NodeId};
use smallvec::smallvec;
use std::collections::VecDeque;
use thiserror::Error;
use tracing::debug;
use ulid::Ulid;

#[derive(Clone, Debug, Eq, PartialEq)]
enum IncomingVersionReplicationState {
    Init,
    ReadDestinationBucket,
    CheckPermissions,
    ReadExistingVersion,
    ReadExistingBlob,
    SendNegotiation,
    ReceiveBlob,
    StartTransaction,
    WriteBlobLocation,
    ReadObjectLookup,
    ApplyHeadTransition,
    WriteBlobVersion,
    WriteMultipartMetadata,
    CommitTransaction,
    SendApplyRejected,
    AbortTransaction,
    CleanupReceivedBlob,
    RegisterBlobInDht,
    SendApplyComplete,
    CloseConnection,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum IncomingVersionReplicationError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    AuthorizationError(#[from] AuthorizationError),
    #[error(transparent)]
    ReplicationError(#[from] ReplicationError),
    #[error("Replication is only allowed within the same realm")]
    RealmMismatch,
    #[error("Destination bucket not found")]
    DestinationBucketNotFound,
    #[error("Replication requires WRITE permission on the destination path")]
    WritePermissionDenied,
    #[error("Current version manifest is missing current pointer generation")]
    MissingCurrentVersionGeneration,
    #[error("Materialized replication manifest is missing blob info")]
    MissingBlobInfo,
    #[error("Materialized replication manifest is missing local blob location")]
    MissingBlobLocation,
    #[error("Replicated blob hash does not match manifest")]
    BlobHashMismatch,
    #[error("Replicated blob size does not match manifest")]
    BlobSizeMismatch,
    #[error("Replicated blob storage flags do not match manifest")]
    BlobStorageFlagsMismatch,
    #[error("Unexpected event in state {state}: expected {expected}, got {received:?}")]
    InvalidStateEvent {
        state: &'static str,
        expected: &'static str,
        received: Event,
    },
}

#[derive(Debug, PartialEq)]
pub struct IncomingVersionReplicationOperation {
    state: IncomingVersionReplicationState,
    stream_id: Ulid,
    local_node_id: NodeId,
    local_realm_id: RealmId,
    manifest: VersionReplicationManifest,
    txn_id: Option<Ulid>,
    destination_group_id: Option<GroupId>,
    negotiation_result: Option<ReplicationNegotiationResult>,
    existing_blob_location: Option<BackendLocation>,
    received_blob_location: Option<BackendLocation>,
    existing_current_pointer: Option<CurrentVersionPointer>,
    pending_new_pointer: Option<CurrentVersionPointer>,
    pending_new_current_hash: Option<[u8; 32]>,
    pending_head_transition_effects: VecDeque<Effect>,
    cleanup_blob_location: Option<BackendLocation>,
    apply_committed: bool,
    output: Option<Result<(), IncomingVersionReplicationError>>,
}

impl IncomingVersionReplicationOperation {
    pub fn new(
        stream_id: Ulid,
        local_node_id: NodeId,
        local_realm_id: RealmId,
        manifest: VersionReplicationManifest,
    ) -> Self {
        Self {
            state: IncomingVersionReplicationState::Init,
            stream_id,
            local_node_id,
            local_realm_id,
            manifest,
            txn_id: None,
            destination_group_id: None,
            negotiation_result: None,
            existing_blob_location: None,
            received_blob_location: None,
            existing_current_pointer: None,
            pending_new_pointer: None,
            pending_new_current_hash: None,
            pending_head_transition_effects: VecDeque::new(),
            cleanup_blob_location: None,
            apply_committed: false,
            output: None,
        }
    }

    fn state_name(&self) -> &'static str {
        match self.state {
            IncomingVersionReplicationState::Init => "Init",
            IncomingVersionReplicationState::ReadDestinationBucket => "ReadDestinationBucket",
            IncomingVersionReplicationState::CheckPermissions => "CheckPermissions",
            IncomingVersionReplicationState::ReadExistingVersion => "ReadExistingVersion",
            IncomingVersionReplicationState::ReadExistingBlob => "ReadExistingBlob",
            IncomingVersionReplicationState::SendNegotiation => "SendNegotiation",
            IncomingVersionReplicationState::ReceiveBlob => "ReceiveBlob",
            IncomingVersionReplicationState::StartTransaction => "StartTransaction",
            IncomingVersionReplicationState::WriteBlobLocation => "WriteBlobLocation",
            IncomingVersionReplicationState::ReadObjectLookup => "ReadObjectLookup",
            IncomingVersionReplicationState::ApplyHeadTransition => "ApplyHeadTransition",
            IncomingVersionReplicationState::WriteBlobVersion => "WriteBlobVersion",
            IncomingVersionReplicationState::WriteMultipartMetadata => "WriteMultipartMetadata",
            IncomingVersionReplicationState::CommitTransaction => "CommitTransaction",
            IncomingVersionReplicationState::SendApplyRejected => "SendApplyRejected",
            IncomingVersionReplicationState::AbortTransaction => "AbortTransaction",
            IncomingVersionReplicationState::CleanupReceivedBlob => "CleanupReceivedBlob",
            IncomingVersionReplicationState::RegisterBlobInDht => "RegisterBlobInDht",
            IncomingVersionReplicationState::SendApplyComplete => "SendApplyComplete",
            IncomingVersionReplicationState::CloseConnection => "CloseConnection",
            IncomingVersionReplicationState::Finish => "Finish",
            IncomingVersionReplicationState::Error => "Error",
        }
    }

    fn reject_negotiation(&mut self, err: IncomingVersionReplicationError) -> Effects {
        debug!(
            bucket = %self.manifest.bucket,
            key = %self.manifest.key,
            version_id = %self.manifest.version_id,
            stream_id = %self.stream_id,
            reason = %err,
            "Rejecting incoming version replication negotiation"
        );
        let reason = err.to_string();
        self.output = Some(Ok(()));
        self.send_negotiation(ReplicationNegotiationResult::Rejected(reason))
    }

    fn fail(&mut self, err: IncomingVersionReplicationError) -> Effects {
        debug!(
            bucket = %self.manifest.bucket,
            key = %self.manifest.key,
            version_id = %self.manifest.version_id,
            stream_id = %self.stream_id,
            state = %self.state_name(),
            error = %err,
            "Incoming version replication failed"
        );
        let should_reject = matches!(
            self.negotiation_result,
            Some(
                ReplicationNegotiationResult::NeedVersionOnly
                    | ReplicationNegotiationResult::NeedBlobAndVersion
            )
        ) && !self.apply_committed
            && !matches!(
                self.state,
                IncomingVersionReplicationState::SendApplyRejected
                    | IncomingVersionReplicationState::AbortTransaction
                    | IncomingVersionReplicationState::CleanupReceivedBlob
                    | IncomingVersionReplicationState::CloseConnection
                    | IncomingVersionReplicationState::Error
            );
        self.output = Some(Err(err));
        if should_reject {
            self.send_apply_rejected()
        } else {
            self.state = IncomingVersionReplicationState::Error;
            self.abort()
        }
    }

    fn version_key_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        VersionKey::new(
            &self.manifest.bucket,
            &self.manifest.key,
            self.manifest.version_id,
        )
        .to_bytes()
    }

    fn auth_context(&self) -> AuthContext {
        self.manifest.auth_context.clone()
    }

    fn target_authorization_path(&self, group_id: Ulid) -> String {
        if self.manifest.key.is_empty() {
            blob_bucket_permission_path(
                self.local_realm_id,
                group_id,
                self.local_node_id,
                &self.manifest.bucket,
            )
        } else {
            blob_object_permission_path(
                self.local_realm_id,
                group_id,
                self.local_node_id,
                &self.manifest.bucket,
                &self.manifest.key,
            )
        }
    }

    fn alias_context(&self) -> Result<HeadAliasContext, IncomingVersionReplicationError> {
        let Some(group_id) = self.destination_group_id else {
            return Err(IncomingVersionReplicationError::DestinationBucketNotFound);
        };

        Ok(HeadAliasContext::new(
            self.local_realm_id,
            group_id,
            self.local_node_id,
            self.manifest.bucket.clone(),
            self.manifest.key.clone(),
        ))
    }

    fn current_materialized_hash_from_manifest(&self) -> Option<[u8; 32]> {
        if !self.manifest.current_version || self.manifest.kind != ReplicationItemKind::Materialized
        {
            return None;
        }

        self.manifest.blob.as_ref().map(|blob| blob.hash)
    }

    fn prepare_head_transition(&mut self) -> Effects {
        let context = match self.alias_context() {
            Ok(context) => context,
            Err(err) => return self.fail(err),
        };
        let effects = match build_head_transition_effects(
            &context,
            self.pending_new_pointer.take(),
            self.pending_new_current_hash.take(),
            self.txn_id,
        ) {
            Ok(effects) => effects,
            Err(err) => return self.fail(err.into()),
        };

        self.pending_head_transition_effects = effects.into_iter().collect();
        self.state = IncomingVersionReplicationState::ApplyHeadTransition;
        self.emit_next_head_transition_effect_or_continue()
    }

    fn emit_next_head_transition_effect_or_continue(&mut self) -> Effects {
        if let Some(effect) = self.pending_head_transition_effects.pop_front() {
            return smallvec![effect];
        }

        self.write_version()
    }

    fn read_destination_bucket(&mut self) -> Effects {
        self.state = IncomingVersionReplicationState::ReadDestinationBucket;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: self.manifest.bucket.as_bytes().to_vec().into(),
            txn_id: None,
        })]
    }

    fn check_write_permission(&mut self, group_id: Ulid) -> Effects {
        self.state = IncomingVersionReplicationState::CheckPermissions;
        smallvec![Effect::SubOperation(boxed_suboperation(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context: self.auth_context(),
                path: self.target_authorization_path(group_id),
                required_permission: Permission::WRITE,
            }),
            |result| Event::SubOperation(SubOperationEvent::AuthorizationResult {
                allowed: result
            }),
        ))]
    }

    fn read_existing_version(&mut self) -> Effects {
        self.state = IncomingVersionReplicationState::ReadExistingVersion;
        let key = match self.version_key_bytes() {
            Ok(key) => key,
            Err(err) => return self.fail(err.into()),
        };
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: key.into(),
            txn_id: None,
        })]
    }

    fn read_existing_blob(&mut self) -> Effects {
        let Some(blob) = self.manifest.blob.as_ref() else {
            return self.fail(IncomingVersionReplicationError::MissingBlobInfo);
        };
        self.state = IncomingVersionReplicationState::ReadExistingBlob;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BLOB_LOCATIONS_KEYSPACE.to_string(),
            key: blob.hash.to_vec().into(),
            txn_id: None,
        })]
    }

    fn send_negotiation(&mut self, result: ReplicationNegotiationResult) -> Effects {
        self.negotiation_result = Some(result.clone());
        self.state = IncomingVersionReplicationState::SendNegotiation;
        let payload = match VersionReplicationMessage::VersionNegotiationResponse(result).to_bytes()
        {
            Ok(payload) => payload,
            Err(err) => return self.fail(err.into()),
        };
        smallvec![Effect::Blob(BlobEffect::SendMessage {
            stream_id: self.stream_id,
            payload,
        })]
    }

    fn receive_blob(&mut self) -> Effects {
        self.state = IncomingVersionReplicationState::ReceiveBlob;
        smallvec![Effect::Blob(BlobEffect::HandleReplication {
            replication_id: None,
            stream_id: self.stream_id,
            keep_alive: true,
        })]
    }

    fn start_transaction(&mut self) -> Effects {
        self.state = IncomingVersionReplicationState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn effective_materialized_location(
        &self,
    ) -> Result<BackendLocation, IncomingVersionReplicationError> {
        self.received_blob_location
            .clone()
            .or_else(|| self.existing_blob_location.clone())
            .ok_or(IncomingVersionReplicationError::MissingBlobLocation)
    }

    fn validate_materialized_location(
        &self,
        location: &BackendLocation,
    ) -> Result<(), IncomingVersionReplicationError> {
        let blob = self
            .manifest
            .blob
            .as_ref()
            .ok_or(IncomingVersionReplicationError::MissingBlobInfo)?;
        let blake3 = location
            .get_blake3()
            .ok_or(IncomingVersionReplicationError::MissingBlobLocation)?;

        if blake3 != blob.hash {
            return Err(IncomingVersionReplicationError::BlobHashMismatch);
        }
        if location.blob_size != blob.size {
            return Err(IncomingVersionReplicationError::BlobSizeMismatch);
        }
        if location.compressed != blob.compressed || location.encrypted != blob.encrypted {
            return Err(IncomingVersionReplicationError::BlobStorageFlagsMismatch);
        }

        Ok(())
    }

    fn write_hash_lookup_or_continue(&mut self) -> Effects {
        if let Some(location) = self.received_blob_location.as_ref()
            && let Err(err) = self.validate_materialized_location(location)
        {
            return self.fail(err);
        }

        if self.received_blob_location.is_none() && self.existing_blob_location.is_none() {
            return self.write_object_lookup_or_continue();
        }

        self.write_blob_location_or_continue()
    }

    fn write_blob_location_or_continue(&mut self) -> Effects {
        let Ok(location) = self.effective_materialized_location() else {
            return self.write_object_lookup_or_continue();
        };
        let Some(blake3_hash) = location.get_blake3() else {
            return self.write_object_lookup_or_continue();
        };

        self.state = IncomingVersionReplicationState::WriteBlobLocation;
        let effect = match write_blob_location_effect(
            match blake3_hash.try_into() {
                Ok(hash) => hash,
                Err(err) => return self.fail(ConversionError::from(err).into()),
            },
            location,
            self.txn_id,
        ) {
            Ok(effect) => effect,
            Err(err) => return self.fail(err.into()),
        };
        smallvec![effect]
    }

    fn write_object_lookup_or_continue(&mut self) -> Effects {
        if !self.manifest.current_version {
            return self.write_version();
        }
        if self.manifest.current_version_generation.is_none() {
            return self.fail(IncomingVersionReplicationError::MissingCurrentVersionGeneration);
        }

        self.state = IncomingVersionReplicationState::ReadObjectLookup;
        let key = match BlobHeadKey::new(&self.manifest.bucket, &self.manifest.key).to_bytes() {
            Ok(key) => key,
            Err(err) => return self.fail(err.into()),
        };

        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BLOB_HEAD_KEYSPACE.to_string(),
            key: key.into(),
            txn_id: self.txn_id,
        })]
    }

    fn write_object_lookup_after_compare(&mut self, existing: Option<&[u8]>) -> Effects {
        let Some(incoming_generation) = self.manifest.current_version_generation else {
            return self.write_version();
        };

        let existing_pointer = match existing.map(CurrentVersionPointer::from_bytes).transpose() {
            Ok(pointer) => pointer,
            Err(err) => return self.fail(err.into()),
        };
        let should_write = match existing_pointer.as_ref() {
            Some(pointer)
                if (incoming_generation, self.manifest.version_id)
                    > (pointer.generation, pointer.version_id) =>
            {
                true
            }
            Some(pointer)
                if (incoming_generation, self.manifest.version_id)
                    == (pointer.generation, pointer.version_id) =>
            {
                true
            }
            Some(_) => false,
            None => true,
        };

        self.existing_current_pointer = existing_pointer.clone();

        if !should_write {
            self.pending_new_pointer = None;
            self.pending_new_current_hash = None;
            return self.write_version();
        }

        self.pending_new_pointer = Some(CurrentVersionPointer::new_with_generation(
            self.manifest.version_id,
            incoming_generation,
        ));
        self.pending_new_current_hash = self.current_materialized_hash_from_manifest();

        self.prepare_head_transition()
    }

    fn write_version(&mut self) -> Effects {
        self.write_blob_version()
    }

    fn write_blob_version(&mut self) -> Effects {
        self.state = IncomingVersionReplicationState::WriteBlobVersion;
        let version_key = VersionKey::new(
            &self.manifest.bucket,
            &self.manifest.key,
            self.manifest.version_id,
        );
        let version = match self.manifest.kind {
            ReplicationItemKind::Materialized => {
                let Ok(location) = self.effective_materialized_location() else {
                    return self.fail(IncomingVersionReplicationError::MissingBlobLocation);
                };
                let Some(blake3_hash) = location.get_blake3() else {
                    return self.fail(IncomingVersionReplicationError::MissingBlobLocation);
                };
                BlobVersion::materialized(
                    match blake3_hash.try_into() {
                        Ok(hash) => hash,
                        Err(err) => return self.fail(ConversionError::from(err).into()),
                    },
                    self.manifest.created_at,
                    self.manifest.created_by,
                    self.manifest.source.clone(),
                )
            }
            ReplicationItemKind::DeleteMarker => {
                BlobVersion::deleted(self.manifest.created_at, self.manifest.created_by)
            }
        };

        let effect = match write_blob_version_effect(&version_key, &version, self.txn_id) {
            Ok(effect) => effect,
            Err(err) => return self.fail(err.into()),
        };
        smallvec![effect]
    }

    fn write_multipart_metadata_or_continue(&mut self) -> Effects {
        let Some(multipart) = self.manifest.multipart.as_ref() else {
            return self.commit_transaction();
        };

        self.state = IncomingVersionReplicationState::WriteMultipartMetadata;
        let mut writes = Vec::with_capacity(multipart.parts.len() + 1);

        let summary_key =
            match MultipartObjectMetadataKey::summary(self.manifest.version_id).to_bytes() {
                Ok(key) => key,
                Err(err) => return self.fail(err.into()),
            };
        let summary_value = match multipart.summary.to_bytes() {
            Ok(value) => value,
            Err(err) => return self.fail(err.into()),
        };
        writes.push((
            S3_MULTIPART_OBJECT_METADATA_KEYSPACE.to_string(),
            summary_key.into(),
            summary_value.into(),
        ));

        for part in &multipart.parts {
            let key =
                match MultipartObjectMetadataKey::part(self.manifest.version_id, part.part_number)
                    .to_bytes()
                {
                    Ok(key) => key,
                    Err(err) => return self.fail(err.into()),
                };
            let value = match part.to_bytes() {
                Ok(value) => value,
                Err(err) => return self.fail(err.into()),
            };
            writes.push((
                S3_MULTIPART_OBJECT_METADATA_KEYSPACE.to_string(),
                key.into(),
                value.into(),
            ));
        }

        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: self.txn_id,
        })]
    }

    fn commit_transaction(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(IncomingVersionReplicationError::StorageError(
                StorageError::TransactionNotFound,
            ));
        };
        self.state = IncomingVersionReplicationState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn register_blob_in_dht_or_continue(&mut self) -> Effects {
        let Ok(location) = self.effective_materialized_location() else {
            return self.send_apply_complete();
        };
        let Some(blake3_hash) = location.get_blake3() else {
            return self.send_apply_complete();
        };

        self.state = IncomingVersionReplicationState::RegisterBlobInDht;
        let effect =
            match dht_registration_effect(blake3_hash, self.local_realm_id, self.local_node_id) {
                Ok(effect) => effect,
                Err(_) => return self.send_apply_complete(),
            };
        smallvec![effect]
    }

    fn send_apply_complete(&mut self) -> Effects {
        self.state = IncomingVersionReplicationState::SendApplyComplete;
        let payload = match VersionReplicationMessage::VersionApplyComplete.to_bytes() {
            Ok(payload) => payload,
            Err(err) => return self.fail(err.into()),
        };
        smallvec![Effect::Blob(BlobEffect::SendMessage {
            stream_id: self.stream_id,
            payload,
        })]
    }

    fn send_apply_rejected(&mut self) -> Effects {
        self.state = IncomingVersionReplicationState::SendApplyRejected;
        let reason = self
            .output
            .as_ref()
            .and_then(|result| result.as_ref().err())
            .map(ToString::to_string)
            .unwrap_or_else(|| "version replication apply failed".to_string());
        let payload = match VersionReplicationMessage::VersionApplyRejected(reason).to_bytes() {
            Ok(payload) => payload,
            Err(_) => {
                self.state = IncomingVersionReplicationState::Error;
                return self.abort();
            }
        };
        smallvec![Effect::Blob(BlobEffect::SendMessage {
            stream_id: self.stream_id,
            payload,
        })]
    }

    fn abort_transaction_or_close(&mut self) -> Effects {
        if let Some(txn_id) = self.txn_id.take() {
            self.state = IncomingVersionReplicationState::AbortTransaction;
            smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
        } else {
            self.cleanup_received_blob_or_close()
        }
    }

    fn cleanup_received_blob_or_close(&mut self) -> Effects {
        if let Some(location) = self.cleanup_blob_location.take() {
            self.state = IncomingVersionReplicationState::CleanupReceivedBlob;
            smallvec![Effect::Blob(BlobEffect::Delete { location })]
        } else {
            self.state = IncomingVersionReplicationState::Error;
            self.close_connection()
        }
    }

    fn close_connection(&mut self) -> Effects {
        self.state = IncomingVersionReplicationState::CloseConnection;
        smallvec![Effect::Blob(BlobEffect::CloseConnection {
            stream_id: self.stream_id,
        })]
    }
}

impl Operation for IncomingVersionReplicationOperation {
    type Output = Result<(), IncomingVersionReplicationError>;
    type Error = IncomingVersionReplicationError;

    fn start(&mut self) -> Effects {
        if self.manifest.auth_context.realm_id != self.local_realm_id {
            return self.reject_negotiation(IncomingVersionReplicationError::RealmMismatch);
        }

        self.read_destination_bucket()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            IncomingVersionReplicationState::Init => self.start(),
            IncomingVersionReplicationState::ReadDestinationBucket => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::ReadResult)",
                        received: event,
                    });
                };

                let Some(value) = value else {
                    return self.reject_negotiation(
                        IncomingVersionReplicationError::DestinationBucketNotFound,
                    );
                };
                let bucket_info = match BucketInfo::from_bytes(value.as_ref()) {
                    Ok(bucket_info) => bucket_info,
                    Err(err) => return self.fail(err.into()),
                };

                debug!(
                    bucket = %self.manifest.bucket,
                    key = %self.manifest.key,
                    version_id = %self.manifest.version_id,
                    stream_id = %self.stream_id,
                    group_id = %bucket_info.group_id,
                    kind = ?self.manifest.kind,
                    current_version = self.manifest.current_version,
                    current_version_generation = ?self.manifest.current_version_generation,
                    "Loaded destination bucket for incoming replication"
                );

                self.destination_group_id = Some(bucket_info.group_id);
                self.check_write_permission(bucket_info.group_id)
            }
            IncomingVersionReplicationState::CheckPermissions => {
                let Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) = event
                else {
                    return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::SubOperation(SubOperationEvent::AuthorizationResult)",
                        received: event,
                    });
                };

                match allowed {
                    Ok(true) => {
                        debug!(
                            bucket = %self.manifest.bucket,
                            key = %self.manifest.key,
                            version_id = %self.manifest.version_id,
                            stream_id = %self.stream_id,
                            "Incoming replication write permission granted"
                        );
                        self.read_existing_version()
                    }
                    Ok(false) => self
                        .reject_negotiation(IncomingVersionReplicationError::WritePermissionDenied),
                    Err(err) => self.reject_negotiation(err.into()),
                }
            }
            IncomingVersionReplicationState::ReadExistingVersion => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::ReadResult)",
                        received: event,
                    });
                };

                debug!(
                    bucket = %self.manifest.bucket,
                    key = %self.manifest.key,
                    version_id = %self.manifest.version_id,
                    stream_id = %self.stream_id,
                    existing_version_present = value.is_some(),
                    kind = ?self.manifest.kind,
                    "Loaded existing destination version metadata"
                );

                match self.manifest.kind {
                    ReplicationItemKind::DeleteMarker => {
                        self.send_negotiation(ReplicationNegotiationResult::NeedVersionOnly)
                    }
                    ReplicationItemKind::Materialized => self.read_existing_blob(),
                }
            }
            IncomingVersionReplicationState::ReadExistingBlob => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::ReadResult)",
                        received: event,
                    });
                };

                if let Some(value) = value {
                    match BackendLocation::from_bytes(value.as_ref()) {
                        Ok(location) => {
                            if self.validate_materialized_location(&location).is_err() {
                                debug!(
                                    bucket = %self.manifest.bucket,
                                    key = %self.manifest.key,
                                    version_id = %self.manifest.version_id,
                                    stream_id = %self.stream_id,
                                    existing_blob_size = location.blob_size,
                                    "Existing destination blob differs; requesting blob and version"
                                );
                                return self.send_negotiation(
                                    ReplicationNegotiationResult::NeedBlobAndVersion,
                                );
                            }
                            self.existing_blob_location = Some(location);
                            debug!(
                                bucket = %self.manifest.bucket,
                                key = %self.manifest.key,
                                version_id = %self.manifest.version_id,
                                stream_id = %self.stream_id,
                                "Existing destination blob matches manifest; requesting version only"
                            );
                            self.send_negotiation(ReplicationNegotiationResult::NeedVersionOnly)
                        }
                        Err(_) => {
                            debug!(
                                bucket = %self.manifest.bucket,
                                key = %self.manifest.key,
                                version_id = %self.manifest.version_id,
                                stream_id = %self.stream_id,
                                "Destination blob missing or invalid; requesting blob and version"
                            );
                            self.send_negotiation(ReplicationNegotiationResult::NeedBlobAndVersion)
                        }
                    }
                } else {
                    debug!(
                        bucket = %self.manifest.bucket,
                        key = %self.manifest.key,
                        version_id = %self.manifest.version_id,
                        stream_id = %self.stream_id,
                        "Destination blob absent; requesting blob and version"
                    );
                    self.send_negotiation(ReplicationNegotiationResult::NeedBlobAndVersion)
                }
            }
            IncomingVersionReplicationState::SendNegotiation => {
                let Event::Blob(BlobEvent::MessageSent { .. }) = event else {
                    return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Blob(BlobEvent::MessageSent)",
                        received: event,
                    });
                };

                match self.negotiation_result.clone() {
                    Some(ReplicationNegotiationResult::AlreadyReplicatedVersion)
                    | Some(ReplicationNegotiationResult::Rejected(_)) => self.close_connection(),
                    Some(ReplicationNegotiationResult::NeedVersionOnly) => {
                        debug!(
                            bucket = %self.manifest.bucket,
                            key = %self.manifest.key,
                            version_id = %self.manifest.version_id,
                            stream_id = %self.stream_id,
                            decision = ?self.negotiation_result,
                            "Negotiation sent; awaiting version apply"
                        );
                        self.start_transaction()
                    }
                    Some(ReplicationNegotiationResult::NeedBlobAndVersion) => {
                        debug!(
                            bucket = %self.manifest.bucket,
                            key = %self.manifest.key,
                            version_id = %self.manifest.version_id,
                            stream_id = %self.stream_id,
                            decision = ?self.negotiation_result,
                            "Negotiation sent; awaiting blob transfer"
                        );
                        self.receive_blob()
                    }
                    None => self.fail(IncomingVersionReplicationError::ReplicationError(
                        ReplicationError::ReplicationFailed,
                    )),
                }
            }
            IncomingVersionReplicationState::ReceiveBlob => {
                let Event::Blob(BlobEvent::ReplicationFinished { location }) = event else {
                    return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Blob(BlobEvent::ReplicationFinished)",
                        received: event,
                    });
                };
                if let Err(err) = self.validate_materialized_location(&location) {
                    self.received_blob_location = Some(location.clone());
                    self.cleanup_blob_location = Some(location);
                    return self.fail(err);
                }
                debug!(
                    bucket = %self.manifest.bucket,
                    key = %self.manifest.key,
                    version_id = %self.manifest.version_id,
                    stream_id = %self.stream_id,
                    blob_size = location.blob_size,
                    backend_path = %location.backend_path,
                    "Received and validated replicated blob"
                );
                self.received_blob_location = Some(location.clone());
                self.cleanup_blob_location = Some(location);
                self.start_transaction()
            }
            IncomingVersionReplicationState::StartTransaction => {
                let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
                    return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::TransactionStarted)",
                        received: event,
                    });
                };
                self.txn_id = Some(txn_id);
                debug!(
                    bucket = %self.manifest.bucket,
                    key = %self.manifest.key,
                    version_id = %self.manifest.version_id,
                    stream_id = %self.stream_id,
                    txn_id = %txn_id,
                    "Started incoming replication transaction"
                );
                self.write_hash_lookup_or_continue()
            }
            IncomingVersionReplicationState::WriteBlobLocation => {
                let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
                    return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::WriteResult)",
                        received: event,
                    });
                };
                self.write_object_lookup_or_continue()
            }
            IncomingVersionReplicationState::ReadObjectLookup => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::ReadResult)",
                        received: event,
                    });
                };
                let existing_pointer = value
                    .as_ref()
                    .and_then(|value| CurrentVersionPointer::from_bytes(value.as_ref()).ok());
                let incoming_generation = self.manifest.current_version_generation;
                let pointer_will_update = match (incoming_generation, existing_pointer.as_ref()) {
                    (Some(incoming_generation), Some(pointer)) => {
                        (incoming_generation, self.manifest.version_id)
                            >= (pointer.generation, pointer.version_id)
                    }
                    (Some(_), None) => true,
                    (None, _) => false,
                };
                debug!(
                    bucket = %self.manifest.bucket,
                    key = %self.manifest.key,
                    version_id = %self.manifest.version_id,
                    stream_id = %self.stream_id,
                    existing_generation = existing_pointer.as_ref().map(|pointer| pointer.generation),
                    existing_version_id = ?existing_pointer.as_ref().map(|pointer| pointer.version_id),
                    incoming_generation = ?incoming_generation,
                    pointer_will_update,
                    "Compared destination current version pointer"
                );
                self.write_object_lookup_after_compare(value.as_deref())
            }
            IncomingVersionReplicationState::ApplyHeadTransition => match event {
                Event::Storage(StorageEvent::WriteResult { .. })
                | Event::Storage(StorageEvent::DeleteResult { .. }) => {
                    self.emit_next_head_transition_effect_or_continue()
                }
                _ => self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                    state: self.state_name(),
                    expected: "Event::Storage(StorageEvent::{WriteResult|DeleteResult})",
                    received: event,
                }),
            },
            IncomingVersionReplicationState::WriteBlobVersion => {
                let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
                    return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::WriteResult)",
                        received: event,
                    });
                };
                self.write_multipart_metadata_or_continue()
            }
            IncomingVersionReplicationState::WriteMultipartMetadata => {
                let Event::Storage(StorageEvent::BatchWriteResult { .. }) = event else {
                    return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::BatchWriteResult)",
                        received: event,
                    });
                };
                debug!(
                    bucket = %self.manifest.bucket,
                    key = %self.manifest.key,
                    version_id = %self.manifest.version_id,
                    stream_id = %self.stream_id,
                    multipart_parts = self.manifest.multipart.as_ref().map(|m| m.parts.len()).unwrap_or(0),
                    "Wrote multipart replication metadata"
                );
                self.commit_transaction()
            }
            IncomingVersionReplicationState::CommitTransaction => {
                let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
                    return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::TransactionCommitted)",
                        received: event,
                    });
                };
                self.txn_id = None;
                self.cleanup_blob_location = None;
                self.apply_committed = true;
                debug!(
                    bucket = %self.manifest.bucket,
                    key = %self.manifest.key,
                    version_id = %self.manifest.version_id,
                    stream_id = %self.stream_id,
                    kind = ?self.manifest.kind,
                    "Committed incoming replication transaction"
                );
                match self.manifest.kind {
                    ReplicationItemKind::Materialized => self.register_blob_in_dht_or_continue(),
                    ReplicationItemKind::DeleteMarker => self.send_apply_complete(),
                }
            }
            IncomingVersionReplicationState::SendApplyRejected => {
                let Event::Blob(BlobEvent::MessageSent { .. }) = event else {
                    return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Blob(BlobEvent::MessageSent)",
                        received: event,
                    });
                };
                self.abort_transaction_or_close()
            }
            IncomingVersionReplicationState::AbortTransaction => {
                let Event::Storage(StorageEvent::TransactionAborted { .. }) = event else {
                    return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::TransactionAborted)",
                        received: event,
                    });
                };
                self.cleanup_received_blob_or_close()
            }
            IncomingVersionReplicationState::CleanupReceivedBlob => match event {
                Event::Blob(BlobEvent::DeleteFinished) | Event::Blob(BlobEvent::Error(_)) => {
                    self.state = IncomingVersionReplicationState::Error;
                    self.close_connection()
                }
                _ => {
                    self.state = IncomingVersionReplicationState::Error;
                    self.close_connection()
                }
            },
            IncomingVersionReplicationState::RegisterBlobInDht => match event {
                Event::Net(NetEvent::Dht(DhtEvent::PutComplete { .. }))
                | Event::Net(NetEvent::Dht(DhtEvent::Error { .. }))
                | Event::Net(NetEvent::Error(_)) => self.send_apply_complete(),
                _ => self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                    state: self.state_name(),
                    expected: "Event::Net(NetEvent::Dht(DhtEvent::*))",
                    received: event,
                }),
            },
            IncomingVersionReplicationState::SendApplyComplete => {
                let Event::Blob(BlobEvent::MessageSent { .. }) = event else {
                    return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Blob(BlobEvent::MessageSent)",
                        received: event,
                    });
                };
                debug!(
                    bucket = %self.manifest.bucket,
                    key = %self.manifest.key,
                    version_id = %self.manifest.version_id,
                    stream_id = %self.stream_id,
                    "Sent incoming replication apply-complete acknowledgement"
                );
                self.close_connection()
            }
            IncomingVersionReplicationState::CloseConnection => {
                let Event::Blob(BlobEvent::ConnectionClosed { .. }) = event else {
                    return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Blob(BlobEvent::ConnectionClosed)",
                        received: event,
                    });
                };
                if self.output.as_ref().is_some_and(Result::is_err) {
                    self.state = IncomingVersionReplicationState::Error;
                } else {
                    self.state = IncomingVersionReplicationState::Finish;
                }
                if self.output.is_none() {
                    self.output = Some(Ok(()));
                }
                debug!(
                    bucket = %self.manifest.bucket,
                    key = %self.manifest.key,
                    version_id = %self.manifest.version_id,
                    stream_id = %self.stream_id,
                    state = %self.state_name(),
                    "Closed incoming replication connection"
                );
                smallvec![]
            }
            IncomingVersionReplicationState::Finish => smallvec![],
            IncomingVersionReplicationState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            IncomingVersionReplicationState::Finish | IncomingVersionReplicationState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == IncomingVersionReplicationState::Error
            && let Some(Err(err)) = self.output
        {
            return Err(err);
        }
        Ok(self.output.unwrap_or(Ok(())))
    }

    fn abort(&mut self) -> Effects {
        let mut effects = smallvec![];

        if let Some(location) = self.cleanup_blob_location.take() {
            effects.push(Effect::Blob(BlobEffect::Delete { location }));
        }
        if let Some(txn_id) = self.txn_id.take() {
            effects.push(Effect::Storage(StorageEffect::AbortTransaction { txn_id }));
        }
        effects.push(Effect::Blob(BlobEffect::CloseConnection {
            stream_id: self.stream_id,
        }));

        effects
    }
}

#[cfg(test)]
mod tests {
    use super::{
        IncomingVersionReplicationError, IncomingVersionReplicationOperation,
        IncomingVersionReplicationState,
    };
    use crate::replication::protocol::{
        MaterializedBlobInfo, VersionReplicationManifest, VersionReplicationMessage,
    };
    use aruna_core::UserId;
    use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
    use aruna_core::errors::AuthorizationError;
    use aruna_core::events::{BlobEvent, Event, StorageEvent, SubOperationEvent};
    use aruna_core::keyspaces::{
        BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE,
    };
    use aruna_core::operation::Operation;
    use aruna_core::structs::{
        AuthContext, BackendLocation, BlobVersion, BucketInfo, CurrentVersionPointer, RealmId,
        ReplicationItemKind, ReplicationNegotiationResult, SourceConnectorKind, StagingStrategy,
        VersionSourceBinding,
    };
    use std::collections::HashMap;
    use std::time::SystemTime;
    use ulid::Ulid;

    fn test_realm_id() -> RealmId {
        RealmId::from_bytes([7u8; 32])
    }

    fn test_user_id() -> UserId {
        UserId::nil(test_realm_id())
    }

    fn make_location() -> BackendLocation {
        let mut hashes = HashMap::new();
        hashes.insert("blake3".to_string(), vec![1u8; 32]);
        BackendLocation {
            root: "/tmp".to_string(),
            storage_bucket: "blob-bucket".to_string(),
            backend_path: format!("bucket/key_{}", Ulid::new()),
            ulid: Ulid::new(),
            compressed: false,
            encrypted: false,
            created_by: test_user_id(),
            created_at: SystemTime::now(),
            staging: false,
            partial: false,
            blob_size: 42,
            hashes,
        }
    }

    fn make_bucket_info(group_id: Ulid) -> BucketInfo {
        BucketInfo {
            group_id,
            created_at: SystemTime::now(),
            created_by: test_user_id(),
        }
    }

    fn make_manifest(kind: ReplicationItemKind) -> VersionReplicationManifest {
        let blob = match kind {
            ReplicationItemKind::Materialized => {
                let location = make_location();
                Some(MaterializedBlobInfo {
                    hash: [1u8; 32],
                    size: location.blob_size,
                    compressed: location.compressed,
                    encrypted: location.encrypted,
                    location,
                })
            }
            ReplicationItemKind::DeleteMarker => None,
        };

        VersionReplicationManifest {
            bucket: "bucket".to_string(),
            key: "dir/file.txt".to_string(),
            version_id: Ulid::new(),
            kind,
            created_at: SystemTime::now(),
            created_by: test_user_id(),
            current_version: true,
            current_version_generation: Some(1),
            auth_context: AuthContext {
                user_id: test_user_id(),
                realm_id: test_realm_id(),
                path_restrictions: None,
            },
            blob,
            source: None,
            multipart: None,
        }
    }

    fn make_source_binding() -> VersionSourceBinding {
        VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: aruna_core::structs::PortableSourceDescriptor {
                kind: SourceConnectorKind::Http,
                public_config: HashMap::from([(
                    "endpoint".to_string(),
                    "https://example.org".to_string(),
                )]),
                source_path: "dir/file.txt".to_string(),
                version_selector: None,
                capabilities: Vec::new(),
                origin_node_id: None,
            },
            connector_id: Some(Ulid::new()),
        }
    }

    fn message_from_effect(effect: &Effect) -> VersionReplicationMessage {
        let Effect::Blob(BlobEffect::SendMessage { payload, .. }) = effect else {
            panic!("expected blob send message effect")
        };
        VersionReplicationMessage::from_bytes(payload).unwrap()
    }

    fn expect_rejected_negotiation(effect: &Effect, expected_reason: &str) {
        match message_from_effect(effect) {
            VersionReplicationMessage::VersionNegotiationResponse(
                ReplicationNegotiationResult::Rejected(reason),
            ) => assert_eq!(reason, expected_reason),
            other => panic!("expected rejected negotiation response, got {other:?}"),
        }
    }

    fn advance_to_version_lookup(
        op: &mut IncomingVersionReplicationOperation,
        group_id: Ulid,
    ) -> Effect {
        let effects = op.start();
        assert_eq!(
            op.state,
            IncomingVersionReplicationState::ReadDestinationBucket
        );
        assert!(matches!(
            effects[0],
            Effect::Storage(StorageEffect::Read { .. })
        ));

        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: b"bucket".to_vec().into(),
            value: Some(make_bucket_info(group_id).to_bytes().unwrap().into()),
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::CheckPermissions);
        assert!(matches!(effects[0], Effect::SubOperation(_)));

        let mut effects = op.step(Event::SubOperation(
            SubOperationEvent::AuthorizationResult { allowed: Ok(true) },
        ));
        assert_eq!(
            op.state,
            IncomingVersionReplicationState::ReadExistingVersion
        );
        assert_eq!(effects.len(), 1);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { key_space, .. })]
                if key_space == BLOB_VERSIONS_KEYSPACE
        ));
        effects.remove(0)
    }

    fn start_apply_transaction(op: &mut IncomingVersionReplicationOperation) -> Ulid {
        let txn_id = Ulid::new();
        op.state = IncomingVersionReplicationState::StartTransaction;
        op.negotiation_result = Some(ReplicationNegotiationResult::NeedVersionOnly);
        op.destination_group_id = Some(Ulid::new());

        let effects = op.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        assert_eq!(op.state, IncomingVersionReplicationState::ReadObjectLookup);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { key_space, txn_id: read_txn_id, .. })]
                if key_space == BLOB_HEAD_KEYSPACE && *read_txn_id == Some(txn_id)
        ));
        txn_id
    }

    #[test]
    fn existing_version_does_not_short_circuit_apply() {
        let manifest = make_manifest(ReplicationItemKind::Materialized);
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::new(),
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest.clone(),
        );

        let _effects = advance_to_version_lookup(&mut op, Ulid::new());

        let version = BlobVersion::materialized(
            manifest.blob.as_ref().unwrap().hash,
            manifest.created_at,
            manifest.created_by,
            None,
        );
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: Some(version.to_bytes().unwrap().into()),
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::ReadExistingBlob);
        assert_eq!(effects.len(), 1);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { key_space, .. })]
                if key_space == BLOB_LOCATIONS_KEYSPACE
        ));
    }

    #[test]
    fn stale_current_pointer_update_skips_current_pointer_overwrite() {
        let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        manifest.current_version_generation = Some(10);
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::new(),
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest.clone(),
        );
        let txn_id = start_apply_transaction(&mut op);
        let existing_pointer = CurrentVersionPointer::new_with_generation(Ulid::new(), 20);

        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: Some(existing_pointer.to_bytes().unwrap().into()),
        }));

        assert_eq!(op.state, IncomingVersionReplicationState::WriteBlobVersion);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Write { key_space, txn_id: write_txn_id, .. })]
                if key_space == BLOB_VERSIONS_KEYSPACE && *write_txn_id == Some(txn_id)
        ));
    }

    #[test]
    fn current_manifest_without_pointer_generation_rejects_apply() {
        let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        manifest.current_version_generation = None;
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::new(),
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );
        let txn_id = Ulid::new();
        op.state = IncomingVersionReplicationState::StartTransaction;
        op.negotiation_result = Some(ReplicationNegotiationResult::NeedVersionOnly);

        let effects = op.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));

        assert_eq!(op.state, IncomingVersionReplicationState::SendApplyRejected);
        assert!(matches!(
            &op.output,
            Some(Err(
                IncomingVersionReplicationError::MissingCurrentVersionGeneration
            ))
        ));
        assert!(matches!(
            message_from_effect(&effects[0]),
            VersionReplicationMessage::VersionApplyRejected(_)
        ));
    }

    #[test]
    fn unparsable_existing_current_pointer_rejects_apply() {
        let manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::new(),
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );
        start_apply_transaction(&mut op);

        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: Some(vec![255, 255, 255].into()),
        }));

        assert_eq!(op.state, IncomingVersionReplicationState::SendApplyRejected);
        assert!(matches!(
            &op.output,
            Some(Err(IncomingVersionReplicationError::ConversionError(_)))
        ));
        assert!(matches!(
            message_from_effect(&effects[0]),
            VersionReplicationMessage::VersionApplyRejected(_)
        ));
    }

    #[test]
    fn stale_current_pointer_update_still_writes_version_metadata() {
        let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        manifest.current_version_generation = Some(1);
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::new(),
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest.clone(),
        );
        start_apply_transaction(&mut op);
        let existing_pointer = CurrentVersionPointer::new_with_generation(Ulid::new(), 2);

        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: Some(existing_pointer.to_bytes().unwrap().into()),
        }));

        let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
            panic!("expected blob version write")
        };
        let version = BlobVersion::from_bytes(value.as_ref()).unwrap();
        assert!(version.is_deleted());
        assert_eq!(version.created_by, manifest.created_by);
    }

    #[test]
    fn write_version_preserves_manifest_source_binding() {
        let source = make_source_binding();
        let mut manifest = make_manifest(ReplicationItemKind::Materialized);
        manifest.source = Some(source.clone());
        manifest.current_version = false;
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::new(),
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );
        op.txn_id = Some(Ulid::new());
        op.existing_blob_location = Some(make_location());

        let effects = op.write_version();

        let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
            panic!("expected blob version write")
        };
        let version = BlobVersion::from_bytes(value.as_ref()).unwrap();
        assert_eq!(version.source_binding(), Some(&source));
    }

    #[test]
    fn newer_current_pointer_generation_allows_rollback_to_older_version_id() {
        let existing_version_id = Ulid::from_bytes([9u8; 16]);
        let incoming_version_id = Ulid::from_bytes([1u8; 16]);
        let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        manifest.version_id = incoming_version_id;
        manifest.current_version_generation = Some(20);
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::new(),
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest.clone(),
        );
        let txn_id = start_apply_transaction(&mut op);
        let existing_pointer = CurrentVersionPointer::new_with_generation(existing_version_id, 10);

        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: Some(existing_pointer.to_bytes().unwrap().into()),
        }));

        let [
            Effect::Storage(StorageEffect::Write {
                key_space,
                value,
                txn_id: write_txn_id,
                ..
            }),
        ] = effects.as_slice()
        else {
            panic!("expected blob head write")
        };
        assert_eq!(key_space, BLOB_HEAD_KEYSPACE);
        assert_eq!(*write_txn_id, Some(txn_id));
        assert_eq!(
            CurrentVersionPointer::from_bytes(value.as_ref()).unwrap(),
            CurrentVersionPointer::new_with_generation(
                incoming_version_id,
                manifest.current_version_generation.unwrap()
            )
        );
    }

    #[test]
    fn same_generation_higher_ulid_overwrites_current_pointer() {
        let existing_version_id = Ulid::from_bytes([1u8; 16]);
        let incoming_version_id = Ulid::from_bytes([9u8; 16]);
        let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        manifest.version_id = incoming_version_id;
        manifest.current_version_generation = Some(7);
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::new(),
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest.clone(),
        );
        start_apply_transaction(&mut op);
        let existing_pointer = CurrentVersionPointer::new_with_generation(existing_version_id, 7);

        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: Some(existing_pointer.to_bytes().unwrap().into()),
        }));

        let [
            Effect::Storage(StorageEffect::Write {
                key_space, value, ..
            }),
        ] = effects.as_slice()
        else {
            panic!("expected blob head write")
        };
        assert_eq!(key_space, BLOB_HEAD_KEYSPACE);
        assert_eq!(
            CurrentVersionPointer::from_bytes(value.as_ref()).unwrap(),
            CurrentVersionPointer::new_with_generation(incoming_version_id, 7)
        );
    }

    #[test]
    fn same_generation_lower_ulid_skips_current_pointer_overwrite() {
        let existing_version_id = Ulid::from_bytes([9u8; 16]);
        let incoming_version_id = Ulid::from_bytes([1u8; 16]);
        let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        manifest.version_id = incoming_version_id;
        manifest.current_version_generation = Some(7);
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::new(),
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );
        start_apply_transaction(&mut op);
        let existing_pointer = CurrentVersionPointer::new_with_generation(existing_version_id, 7);

        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: Some(existing_pointer.to_bytes().unwrap().into()),
        }));

        assert_eq!(op.state, IncomingVersionReplicationState::WriteBlobVersion);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Write { key_space, .. })]
                if key_space == BLOB_VERSIONS_KEYSPACE
        ));
    }

    #[test]
    fn target_authorization_path_uses_canonical_blob_path_format() {
        let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        manifest.bucket = "bucket-a".to_string();
        manifest.key = "nested/file.txt".to_string();
        let local_node_id = iroh::SecretKey::generate().public();
        let local_realm_id = RealmId::from_bytes([7u8; 32]);
        let op = IncomingVersionReplicationOperation::new(
            Ulid::new(),
            local_node_id,
            local_realm_id,
            manifest,
        );
        let group_id = Ulid::from_bytes([4u8; 16]);

        assert_eq!(
            op.target_authorization_path(group_id),
            aruna_core::structs::blob_object_permission_path(
                local_realm_id,
                group_id,
                local_node_id,
                "bucket-a",
                "nested/file.txt",
            )
        );
    }

    #[test]
    fn existing_blob_with_manifest_mismatch_requests_blob_transfer() {
        let manifest = make_manifest(ReplicationItemKind::Materialized);
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::new(),
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );

        let _effects = advance_to_version_lookup(&mut op, Ulid::new());
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: None,
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::ReadExistingBlob);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { key_space, .. })]
                if key_space == BLOB_LOCATIONS_KEYSPACE
        ));

        let mut mismatched_location = make_location();
        mismatched_location.blob_size += 1;
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: Some(mismatched_location.to_bytes().unwrap().into()),
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::SendNegotiation);
        assert!(matches!(
            message_from_effect(&effects[0]),
            VersionReplicationMessage::VersionNegotiationResponse(
                aruna_core::structs::ReplicationNegotiationResult::NeedBlobAndVersion
            )
        ));
    }

    #[test]
    fn missing_blob_requests_location_lookup_in_new_keyspace() {
        let manifest = make_manifest(ReplicationItemKind::Materialized);
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::new(),
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );

        let _effects = advance_to_version_lookup(&mut op, Ulid::new());
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: None,
        }));

        assert_eq!(op.state, IncomingVersionReplicationState::ReadExistingBlob);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { key_space, .. })]
                if key_space == BLOB_LOCATIONS_KEYSPACE
        ));
    }

    #[test]
    fn received_blob_manifest_mismatch_is_rejected_and_cleaned_up() {
        let manifest = make_manifest(ReplicationItemKind::Materialized);
        let stream_id = Ulid::new();
        let mut op = IncomingVersionReplicationOperation::new(
            stream_id,
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );
        let mut mismatched_location = make_location();
        mismatched_location.blob_size += 1;

        op.negotiation_result = Some(ReplicationNegotiationResult::NeedBlobAndVersion);
        op.state = IncomingVersionReplicationState::ReceiveBlob;

        let effects = op.step(Event::Blob(BlobEvent::ReplicationFinished {
            location: mismatched_location.clone(),
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::SendApplyRejected);
        assert!(matches!(
            message_from_effect(&effects[0]),
            VersionReplicationMessage::VersionApplyRejected(_)
        ));

        let effects = op.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
        assert_eq!(
            op.state,
            IncomingVersionReplicationState::CleanupReceivedBlob
        );
        assert_eq!(
            effects[0],
            Effect::Blob(BlobEffect::Delete {
                location: mismatched_location
            })
        );
    }

    #[test]
    fn missing_destination_bucket_is_rejected_during_negotiation() {
        let manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        let stream_id = Ulid::new();
        let mut op = IncomingVersionReplicationOperation::new(
            stream_id,
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );

        let effects = op.start();
        assert_eq!(
            op.state,
            IncomingVersionReplicationState::ReadDestinationBucket
        );
        assert!(matches!(
            effects[0],
            Effect::Storage(StorageEffect::Read { .. })
        ));

        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: b"bucket".to_vec().into(),
            value: None,
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::SendNegotiation);
        expect_rejected_negotiation(
            &effects[0],
            IncomingVersionReplicationError::DestinationBucketNotFound
                .to_string()
                .as_str(),
        );

        let effects = op.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
        assert_eq!(op.state, IncomingVersionReplicationState::CloseConnection);
        assert!(matches!(
            effects[0],
            Effect::Blob(BlobEffect::CloseConnection { .. })
        ));
    }

    #[test]
    fn denied_write_permission_is_rejected_during_negotiation() {
        let manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        let stream_id = Ulid::new();
        let mut op = IncomingVersionReplicationOperation::new(
            stream_id,
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );

        op.start();
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: b"bucket".to_vec().into(),
            value: Some(make_bucket_info(Ulid::new()).to_bytes().unwrap().into()),
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::CheckPermissions);
        assert!(matches!(effects[0], Effect::SubOperation(_)));

        let effects = op.step(Event::SubOperation(
            SubOperationEvent::AuthorizationResult { allowed: Ok(false) },
        ));
        assert_eq!(op.state, IncomingVersionReplicationState::SendNegotiation);
        expect_rejected_negotiation(
            &effects[0],
            IncomingVersionReplicationError::WritePermissionDenied
                .to_string()
                .as_str(),
        );

        let effects = op.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
        assert_eq!(op.state, IncomingVersionReplicationState::CloseConnection);
        assert!(matches!(
            effects[0],
            Effect::Blob(BlobEffect::CloseConnection { .. })
        ));
    }

    #[test]
    fn authorization_errors_are_rejected_during_negotiation() {
        let manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::new(),
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );

        op.start();
        op.step(Event::Storage(StorageEvent::ReadResult {
            key: b"bucket".to_vec().into(),
            value: Some(make_bucket_info(Ulid::new()).to_bytes().unwrap().into()),
        }));

        let effects = op.step(Event::SubOperation(
            SubOperationEvent::AuthorizationResult {
                allowed: Err(AuthorizationError::AuthDocNotFound),
            },
        ));
        assert_eq!(op.state, IncomingVersionReplicationState::SendNegotiation);
        expect_rejected_negotiation(
            &effects[0],
            AuthorizationError::AuthDocNotFound.to_string().as_str(),
        );
    }

    #[test]
    fn delete_marker_requests_version_only() {
        let manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::new(),
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );

        let _effects = advance_to_version_lookup(&mut op, Ulid::new());
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: None,
        }));

        assert_eq!(op.state, IncomingVersionReplicationState::SendNegotiation);
        assert!(matches!(
            message_from_effect(&effects[0]),
            VersionReplicationMessage::VersionNegotiationResponse(
                aruna_core::structs::ReplicationNegotiationResult::NeedVersionOnly
            )
        ));
    }

    #[test]
    fn missing_blob_requests_blob_transfer() {
        let manifest = make_manifest(ReplicationItemKind::Materialized);
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::new(),
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );

        let _effects = advance_to_version_lookup(&mut op, Ulid::new());
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: None,
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::ReadExistingBlob);
        assert!(matches!(
            effects[0],
            Effect::Storage(StorageEffect::Read { .. })
        ));

        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: None,
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::SendNegotiation);
        assert!(matches!(
            message_from_effect(&effects[0]),
            VersionReplicationMessage::VersionNegotiationResponse(
                aruna_core::structs::ReplicationNegotiationResult::NeedBlobAndVersion
            )
        ));
    }

    #[test]
    fn apply_failures_send_explicit_rejection_before_abort() {
        let manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        let stream_id = Ulid::new();
        let txn_id = Ulid::new();
        let mut op = IncomingVersionReplicationOperation::new(
            stream_id,
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );

        op.negotiation_result = Some(ReplicationNegotiationResult::NeedVersionOnly);
        op.state = IncomingVersionReplicationState::ApplyHeadTransition;
        op.txn_id = Some(txn_id);

        let effects = op.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
        assert_eq!(op.state, IncomingVersionReplicationState::SendApplyRejected);
        assert!(matches!(
            message_from_effect(&effects[0]),
            VersionReplicationMessage::VersionApplyRejected(_)
        ));

        let effects = op.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
        assert_eq!(op.state, IncomingVersionReplicationState::AbortTransaction);
        assert_eq!(
            effects[0],
            Effect::Storage(StorageEffect::AbortTransaction { txn_id })
        );

        let effects = op.step(Event::Storage(StorageEvent::TransactionAborted { txn_id }));
        assert_eq!(op.state, IncomingVersionReplicationState::CloseConnection);
        assert!(matches!(
            effects[0],
            Effect::Blob(BlobEffect::CloseConnection { .. })
        ));
    }

    #[test]
    fn received_blobs_are_deleted_after_apply_failure_before_commit() {
        let manifest = make_manifest(ReplicationItemKind::Materialized);
        let stream_id = Ulid::new();
        let received = make_location();
        let txn_id = Ulid::new();
        let mut op = IncomingVersionReplicationOperation::new(
            stream_id,
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );
        op.negotiation_result =
            Some(aruna_core::structs::ReplicationNegotiationResult::NeedBlobAndVersion);
        op.state = IncomingVersionReplicationState::WriteBlobLocation;
        op.txn_id = Some(txn_id);
        op.received_blob_location = Some(received.clone());
        op.cleanup_blob_location = Some(received.clone());

        let effects = op.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
        assert_eq!(op.state, IncomingVersionReplicationState::SendApplyRejected);
        assert!(matches!(
            message_from_effect(&effects[0]),
            VersionReplicationMessage::VersionApplyRejected(_)
        ));

        let effects = op.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
        assert_eq!(op.state, IncomingVersionReplicationState::AbortTransaction);
        assert_eq!(
            effects[0],
            Effect::Storage(StorageEffect::AbortTransaction { txn_id })
        );

        let effects = op.step(Event::Storage(StorageEvent::TransactionAborted { txn_id }));
        assert_eq!(
            op.state,
            IncomingVersionReplicationState::CleanupReceivedBlob
        );
        assert_eq!(
            effects[0],
            Effect::Blob(BlobEffect::Delete { location: received })
        );

        let effects = op.step(Event::Blob(BlobEvent::DeleteFinished));
        assert_eq!(op.state, IncomingVersionReplicationState::CloseConnection);
        assert!(matches!(
            effects[0],
            Effect::Blob(BlobEffect::CloseConnection { .. })
        ));
    }

    #[test]
    fn failures_without_received_blob_close_without_delete() {
        let manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        let stream_id = Ulid::new();
        let txn_id = Ulid::new();
        let mut op = IncomingVersionReplicationOperation::new(
            stream_id,
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );
        op.negotiation_result =
            Some(aruna_core::structs::ReplicationNegotiationResult::NeedVersionOnly);
        op.state = IncomingVersionReplicationState::ApplyHeadTransition;
        op.txn_id = Some(txn_id);

        let effects = op.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
        assert_eq!(op.state, IncomingVersionReplicationState::SendApplyRejected);
        assert!(matches!(
            message_from_effect(&effects[0]),
            VersionReplicationMessage::VersionApplyRejected(_)
        ));

        let effects = op.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
        assert_eq!(op.state, IncomingVersionReplicationState::AbortTransaction);
        assert_eq!(
            effects[0],
            Effect::Storage(StorageEffect::AbortTransaction { txn_id })
        );

        let effects = op.step(Event::Storage(StorageEvent::TransactionAborted { txn_id }));
        assert_eq!(op.state, IncomingVersionReplicationState::CloseConnection);
        assert!(matches!(
            effects[0],
            Effect::Blob(BlobEffect::CloseConnection { .. })
        ));
    }

    #[test]
    fn committed_replication_does_not_delete_received_blob_on_late_failure() {
        let manifest = make_manifest(ReplicationItemKind::Materialized);
        let stream_id = Ulid::new();
        let received = make_location();
        let mut op = IncomingVersionReplicationOperation::new(
            stream_id,
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );
        op.negotiation_result =
            Some(aruna_core::structs::ReplicationNegotiationResult::NeedBlobAndVersion);
        op.state = IncomingVersionReplicationState::RegisterBlobInDht;
        op.received_blob_location = Some(received);
        op.apply_committed = true;

        let effects = op.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: Ulid::new(),
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::Error);
        assert_eq!(effects.len(), 1);
        assert!(matches!(
            effects[0],
            Effect::Blob(BlobEffect::CloseConnection { .. })
        ));
    }
}
