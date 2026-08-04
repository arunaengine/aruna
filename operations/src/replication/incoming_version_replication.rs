use crate::blob::blob_keyspace_helper::{
    HeadAliasContext, add_hash_path_index_effect, blob_location_read,
    build_head_transition_effects, write_blob_location_effect, write_blob_version_effect,
};
use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::group_backends::{BackendFenceError, check_fence, fence_backend};
use crate::group_routing::load_group_inputs;
use crate::replication::error::ReplicationError;
use crate::replication::protocol::{VersionReplicationManifest, VersionReplicationMessage};
use crate::replication::queue::{
    LiveReplicationObligationRecord, live_obligation_effect, schedule_blob_replication_drain_effect,
};
use crate::replication::util::dht_registration_effect;
use crate::s3::create_bucket::CreateBucketOperation;
use crate::usage_stats::{
    QuotaGate, QuotaGateError, StoredDelta, UsageCounterUpdate, UsageUpdateError,
    schedule_usage_snapshot_publish_effect,
};
use aruna_core::document::DocumentSyncTarget;
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{AuthorizationError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, DhtEvent, Event, NetEvent, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_RECLAIM_KEYSPACE, BLOB_VERSIONS_KEYSPACE, HASH_PATHS_INDEX_KEYSPACE,
    S3_BUCKET_KEYSPACE, S3_MULTIPART_OBJECT_METADATA_KEYSPACE,
};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::{
    BackendLocation, BlobHeadKey, BlobLocationKey, BlobVersion, BlobVersionState, BucketInfo,
    CurrentVersionPointer, GroupRoutingInputs, MultipartObjectMetadataKey, NodeRouting, Permission,
    RealmConfigDocument, RealmId, ReclaimCandidate, ReclaimCandidateKey, ReplicationItemKind,
    ReplicationNegotiationResult, ResolvedBackend, RoCrateLimits, RoutingError, StorageRoutingRule,
    UsageDelta, VersionKey, blob_bucket_permission_path, blob_object_permission_path,
    resolve_backend,
};
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, GroupId, NodeId};
use smallvec::smallvec;
use std::collections::VecDeque;
use std::time::SystemTime;
use thiserror::Error;
use tracing::{debug, warn};
use ulid::Ulid;

#[derive(Clone, Debug, Eq, PartialEq)]
enum IncomingVersionReplicationState {
    Init,
    ReadDestinationBucket,
    CreateDestinationBucket,
    LoadDestinationRouting,
    CheckWriterPermissions,
    ReadExistingVersion,
    ReadReplacedBlob,
    ReadQuotaConfig,
    StartQuotaCheck,
    EnforceQuota,
    FinishQuotaCheck,
    ReadExistingBlob,
    SendNegotiation,
    ReceiveBlob,
    StartTransaction,
    VerifyReplaced,
    ReadReplacedMetadata,
    DeleteReplacedMetadata,
    WriteReclaimCandidate,
    FenceBackend,
    VerifyExistingBlob,
    WriteBlobLocation,
    ReadObjectLookup,
    ReadCurrentVersion,
    ApplyHeadTransition,
    WriteBlobVersion,
    WriteMultipartMetadata,
    WriteLiveObligation,
    CheckCommitQuota,
    UpdateUsage,
    CommitTransaction,
    ScheduleUsage,
    ScheduleLiveDrain,
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
    RoutingFailed(#[from] RoutingError),
    #[error(transparent)]
    BackendFenceError(#[from] BackendFenceError),
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
    #[error("could not load the destination group's routing inputs: {0}")]
    RoutingInputsFailed(String),
    #[error("writer_access_denied")]
    WriterPermissionDenied,
    #[error("Replication hop limit exceeded")]
    HopLimitExceeded,
    #[error("Reference replication manifest is missing source metadata")]
    MissingReferenceMetadata,
    #[error("Reference replication manifest is missing source binding")]
    MissingReferenceSource,
    #[error(transparent)]
    QuotaGateError(#[from] QuotaGateError),
    #[error(transparent)]
    UsageUpdateError(#[from] UsageUpdateError),
    #[error("quota")]
    QuotaExceeded,
    #[error("Current version manifest is missing current pointer generation")]
    MissingCurrentVersionGeneration,
    #[error("Destination current version not found")]
    CurrentVersionNotFound,
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
    #[error("Existing blob copy changed before the version committed")]
    ExistingBlobChanged,
    #[error("Replaced multipart metadata exceeds the supported part limit")]
    MultipartMetadataOverflow,
    #[error("Unexpected event in state {state}: expected {expected}, got {received:?}")]
    InvalidStateEvent {
        state: &'static str,
        expected: &'static str,
        received: Event,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IncomingVersionReplicationResult {
    pub applied: bool,
    pub group_id: Option<GroupId>,
}

#[derive(Debug, PartialEq)]
pub struct IncomingVersionReplicationOperation {
    state: IncomingVersionReplicationState,
    stream_id: Ulid,
    local_node_id: NodeId,
    /// The authenticated remote peer that pushed this stream, proven at Bao
    /// ingress. It is the accountable publisher, not the forgeable manifest.
    publisher_node_id: NodeId,
    local_realm_id: RealmId,
    manifest: VersionReplicationManifest,
    txn_id: Option<Ulid>,
    destination_group_id: Option<GroupId>,
    /// The destination bucket's own rules, so this receiver routes its replica
    /// with the tenant's rules and its own class table.
    destination_rules: Vec<StorageRoutingRule>,
    destination_inputs: GroupRoutingInputs,
    create_attempted: bool,
    negotiation_result: Option<ReplicationNegotiationResult>,
    quota_ceiling: Option<u64>,
    quota_gate: Option<QuotaGate>,
    usage_update: Option<UsageCounterUpdate>,
    existing_blob_location: Option<BackendLocation>,
    replaced_version: Option<BlobVersion>,
    received_blob_location: Option<BackendLocation>,
    existing_current_pointer: Option<CurrentVersionPointer>,
    object_delta: i128,
    replaced_logical_bytes: u64,
    replaced_reference_bytes: u64,
    pending_new_pointer: Option<CurrentVersionPointer>,
    pending_new_current_hash: Option<[u8; 32]>,
    pending_head_transition_effects: VecDeque<Effect>,
    pending_version_effects: VecDeque<Effect>,
    cleanup_blob_location: Option<BackendLocation>,
    apply_committed: bool,
    output: Option<Result<IncomingVersionReplicationResult, IncomingVersionReplicationError>>,
    rocrate_limits: RoCrateLimits,
    routing: NodeRouting,
    /// Set when the destination backend is over its cap, which only refuses a
    /// negotiation that asks for the bytes.
    destination_full: Option<RoutingError>,
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
            // Defaults to the local node; the ingress handler overrides it with
            // the authenticated remote peer via `with_publisher_node`.
            publisher_node_id: local_node_id,
            local_realm_id,
            manifest,
            txn_id: None,
            destination_group_id: None,
            destination_rules: Vec::new(),
            destination_inputs: GroupRoutingInputs::default(),
            create_attempted: false,
            negotiation_result: None,
            quota_ceiling: None,
            quota_gate: None,
            usage_update: None,
            existing_blob_location: None,
            replaced_version: None,
            received_blob_location: None,
            existing_current_pointer: None,
            object_delta: 0,
            replaced_logical_bytes: 0,
            replaced_reference_bytes: 0,
            pending_new_pointer: None,
            pending_new_current_hash: None,
            pending_head_transition_effects: VecDeque::new(),
            pending_version_effects: VecDeque::new(),
            cleanup_blob_location: None,
            apply_committed: false,
            output: None,
            rocrate_limits: RoCrateLimits::default(),
            routing: NodeRouting::default(),
            destination_full: None,
        }
    }

    /// Node-local routing, so this receiver picks its own backend.
    pub fn with_routing(mut self, routing: NodeRouting) -> Self {
        self.routing = routing;
        self
    }

    pub fn with_rocrate_limits(mut self, limits: RoCrateLimits) -> Self {
        self.rocrate_limits = limits;
        self
    }

    /// Binds the authenticated remote peer proven at Bao ingress as the
    /// accountable publisher of every version this stream writes.
    pub fn with_publisher_node(mut self, publisher_node_id: NodeId) -> Self {
        self.publisher_node_id = publisher_node_id;
        self
    }

    fn state_name(&self) -> &'static str {
        match self.state {
            IncomingVersionReplicationState::Init => "Init",
            IncomingVersionReplicationState::ReadDestinationBucket => "ReadDestinationBucket",
            IncomingVersionReplicationState::CreateDestinationBucket => "CreateDestinationBucket",
            IncomingVersionReplicationState::LoadDestinationRouting => "LoadDestinationRouting",
            IncomingVersionReplicationState::CheckWriterPermissions => "CheckWriterPermissions",
            IncomingVersionReplicationState::ReadExistingVersion => "ReadExistingVersion",
            IncomingVersionReplicationState::ReadReplacedBlob => "ReadReplacedBlob",
            IncomingVersionReplicationState::ReadQuotaConfig => "ReadQuotaConfig",
            IncomingVersionReplicationState::StartQuotaCheck => "StartQuotaCheck",
            IncomingVersionReplicationState::EnforceQuota => "EnforceQuota",
            IncomingVersionReplicationState::FinishQuotaCheck => "FinishQuotaCheck",
            IncomingVersionReplicationState::ReadExistingBlob => "ReadExistingBlob",
            IncomingVersionReplicationState::SendNegotiation => "SendNegotiation",
            IncomingVersionReplicationState::ReceiveBlob => "ReceiveBlob",
            IncomingVersionReplicationState::StartTransaction => "StartTransaction",
            IncomingVersionReplicationState::VerifyReplaced => "VerifyReplaced",
            IncomingVersionReplicationState::ReadReplacedMetadata => "ReadReplacedMetadata",
            IncomingVersionReplicationState::DeleteReplacedMetadata => "DeleteReplacedMetadata",
            IncomingVersionReplicationState::WriteReclaimCandidate => "WriteReclaimCandidate",
            IncomingVersionReplicationState::FenceBackend => "FenceBackend",
            IncomingVersionReplicationState::VerifyExistingBlob => "VerifyExistingBlob",
            IncomingVersionReplicationState::WriteBlobLocation => "WriteBlobLocation",
            IncomingVersionReplicationState::ReadObjectLookup => "ReadObjectLookup",
            IncomingVersionReplicationState::ReadCurrentVersion => "ReadCurrentVersion",
            IncomingVersionReplicationState::ApplyHeadTransition => "ApplyHeadTransition",
            IncomingVersionReplicationState::WriteBlobVersion => "WriteBlobVersion",
            IncomingVersionReplicationState::WriteMultipartMetadata => "WriteMultipartMetadata",
            IncomingVersionReplicationState::WriteLiveObligation => "WriteLiveObligation",
            IncomingVersionReplicationState::CheckCommitQuota => "CheckCommitQuota",
            IncomingVersionReplicationState::UpdateUsage => "UpdateUsage",
            IncomingVersionReplicationState::CommitTransaction => "CommitTransaction",
            IncomingVersionReplicationState::ScheduleUsage => "ScheduleUsage",
            IncomingVersionReplicationState::ScheduleLiveDrain => "ScheduleLiveDrain",
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
        self.output = Some(Ok(self.result(false)));
        self.send_negotiation(ReplicationNegotiationResult::Rejected(reason))
    }

    fn result(&self, applied: bool) -> IncomingVersionReplicationResult {
        IncomingVersionReplicationResult {
            applied,
            group_id: self.destination_group_id,
        }
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
        if !self.manifest.current_version
            || self.manifest.kind != ReplicationItemKind::Materialized
            || self.manifest.reference_intent
        {
            return None;
        }

        self.manifest.blob.as_ref().map(|blob| blob.hash)
    }

    fn is_reference_item(&self) -> bool {
        self.manifest.reference_intent && self.manifest.kind == ReplicationItemKind::Materialized
    }

    fn reference_version(&self) -> Result<BlobVersion, IncomingVersionReplicationError> {
        let source = self
            .manifest
            .source
            .clone()
            .ok_or(IncomingVersionReplicationError::MissingReferenceSource)?;
        let metadata = self
            .manifest
            .reference_metadata
            .clone()
            .ok_or(IncomingVersionReplicationError::MissingReferenceMetadata)?;
        Ok(BlobVersion::reference(
            source,
            metadata,
            self.manifest.created_at,
            self.manifest.created_by,
            self.manifest.created_at,
        )
        .with_metadata(self.manifest.metadata.clone())
        .with_publisher(self.publisher_node_id))
    }

    fn incoming_logical_bytes(&self) -> Result<u64, IncomingVersionReplicationError> {
        if self.is_reference_item() {
            return self
                .manifest
                .reference_metadata
                .as_ref()
                .map(|metadata| metadata.content_length)
                .ok_or(IncomingVersionReplicationError::MissingReferenceMetadata);
        }

        self.manifest
            .blob
            .as_ref()
            .map(|blob| blob.size)
            .ok_or(IncomingVersionReplicationError::MissingBlobInfo)
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

    fn destination_bucket_info(&self) -> BucketInfo {
        BucketInfo {
            group_id: self.manifest.group_id,
            created_at: self.manifest.created_at,
            created_by: self.manifest.created_by,
            cors_configuration: None,
            replication: None,
            storage_routing: Vec::new(),
        }
    }

    fn create_destination_bucket(&mut self) -> Effects {
        self.create_attempted = true;
        self.state = IncomingVersionReplicationState::CreateDestinationBucket;
        smallvec![Effect::SubOperation(boxed_suboperation(
            CreateBucketOperation::new(
                self.manifest.bucket.clone(),
                self.destination_bucket_info()
            ),
            |result| Event::SubOperation(SubOperationEvent::BucketCreated {
                result: match result {
                    Ok(Some(Ok(_))) => Ok(()),
                    Ok(Some(Err(err))) => Err(err.to_string()),
                    Ok(None) => Err("bucket creation returned no result".to_string()),
                    Err(err) => Err(err.to_string()),
                },
            }),
        ))]
    }

    /// The receiver's own group default and registered backend ids, loaded once
    /// the destination group is known so that the existing-copy probe and the
    /// transfer resolve the destination from identical inputs.
    fn load_destination_routing(&mut self) -> Effects {
        self.state = IncomingVersionReplicationState::LoadDestinationRouting;
        smallvec![load_group_inputs(
            self.destination_group_id.unwrap_or(self.manifest.group_id)
        )]
    }

    /// #332: the pushing peer authorizes by its authenticated node identity at
    /// the ingress gate, never by the forgeable manifest auth context. The
    /// original writer's context can only narrow, so it stays as a deny check.
    fn check_writer_permission(&mut self, group_id: Ulid) -> Effects {
        let Some(auth_context) = self.manifest.writer_auth_context.clone() else {
            return self.read_existing_version();
        };
        self.state = IncomingVersionReplicationState::CheckWriterPermissions;
        smallvec![Effect::SubOperation(boxed_suboperation(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context,
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

    fn read_replaced_blob(&mut self, key: BlobLocationKey) -> Effects {
        self.state = IncomingVersionReplicationState::ReadReplacedBlob;
        smallvec![blob_location_read(&key, None)]
    }

    fn read_quota_config(&mut self) -> Effects {
        self.state = IncomingVersionReplicationState::ReadQuotaConfig;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: DocumentSyncTarget::RealmConfig {
                realm_id: self.local_realm_id,
            }
            .storage_keyspace()
            .to_string(),
            key: DocumentSyncTarget::RealmConfig {
                realm_id: self.local_realm_id,
            }
            .storage_key(),
            txn_id: None,
        })]
    }

    fn start_quota_check(&mut self, ceiling: u64) -> Effects {
        let Some(group_id) = self.destination_group_id else {
            return self.fail(IncomingVersionReplicationError::DestinationBucketNotFound);
        };
        let logical_bytes = match self.incoming_logical_bytes() {
            Ok(logical_bytes) => logical_bytes,
            Err(error) => return self.fail(error),
        }
        .saturating_sub(self.replaced_logical_bytes);
        self.quota_ceiling = Some(ceiling);
        self.quota_gate = Some(QuotaGate::new_for_realm(
            ceiling,
            logical_bytes,
            group_id,
            self.local_node_id,
            self.local_realm_id,
        ));
        self.state = IncomingVersionReplicationState::StartQuotaCheck;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: true,
        })]
    }

    fn finish_quota_check(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(IncomingVersionReplicationError::StorageError(
                StorageError::TransactionNotFound,
            ));
        };
        self.state = IncomingVersionReplicationState::FinishQuotaCheck;
        smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
    }

    /// Asks only about the backend this node would route the blob to: a copy on
    /// any other backend cannot satisfy the destination placement.
    fn read_existing_blob(&mut self) -> Effects {
        let Some(blob) = self.manifest.blob.as_ref() else {
            return self.fail(IncomingVersionReplicationError::MissingBlobInfo);
        };
        let hash = blob.hash;
        // A full destination still probes, because a copy it already holds
        // costs it nothing; the cap only refuses the transfer itself.
        let backend = match self.resolve_destination() {
            Ok(resolved) => resolved.backend,
            Err(IncomingVersionReplicationError::RoutingFailed(RoutingError::BackendFull(
                backend,
            ))) => {
                self.destination_full = Some(RoutingError::BackendFull(backend.clone()));
                backend
            }
            // Still before the negotiation reply, so a node that cannot place
            // the blob owes the sender a reason rather than a dropped stream.
            Err(error) => return self.reject_negotiation(error),
        };
        self.state = IncomingVersionReplicationState::ReadExistingBlob;
        smallvec![blob_location_read(
            &BlobLocationKey::new(hash, backend),
            None
        )]
    }

    /// The only negotiation result that stores bytes, so the destination's cap
    /// is answered here rather than at the probe that keys the deduplication.
    fn request_blob_version(&mut self) -> Effects {
        match self.destination_full.take() {
            Some(error) => {
                self.reject_negotiation(IncomingVersionReplicationError::RoutingFailed(error))
            }
            None => self.send_negotiation(ReplicationNegotiationResult::NeedBlobAndVersion),
        }
    }

    fn resolve_destination(&self) -> Result<ResolvedBackend, IncomingVersionReplicationError> {
        let snapshot = self
            .routing
            .snapshot(self.destination_group_id.unwrap_or(self.manifest.group_id))
            .with_group_inputs(self.destination_inputs.clone())
            .with_bucket_rules(self.destination_rules.clone());
        resolve_backend(&snapshot, &self.manifest.bucket, &self.manifest.key)
            .map_err(IncomingVersionReplicationError::RoutingFailed)
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
        // The receiver routes with its own snapshot; the sender's stamped
        // backend crossed the wire but is ignored.
        let resolved = match self.resolve_destination() {
            Ok(resolved) => resolved,
            Err(error) => return self.fail(error),
        };
        self.state = IncomingVersionReplicationState::ReceiveBlob;
        smallvec![Effect::Blob(BlobEffect::HandleReplication {
            replication_id: None,
            stream_id: self.stream_id,
            resolved,
            keep_alive: true,
        })]
    }

    fn start_transaction(&mut self) -> Effects {
        self.state = IncomingVersionReplicationState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn read_replaced_metadata(&mut self) -> Effects {
        if self.replaced_version.is_none() {
            return self.write_hash_lookup_or_continue();
        }
        let prefix = match MultipartObjectMetadataKey::part_prefix(self.manifest.version_id) {
            Ok(prefix) => prefix.into(),
            Err(error) => return self.fail(error.into()),
        };
        self.state = IncomingVersionReplicationState::ReadReplacedMetadata;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: S3_MULTIPART_OBJECT_METADATA_KEYSPACE.to_string(),
            prefix: Some(prefix),
            start: None,
            limit: 10_000,
            txn_id: self.txn_id,
        })]
    }

    fn verify_replaced(&mut self) -> Effects {
        let key = match self.version_key_bytes() {
            Ok(key) => key,
            Err(error) => return self.fail(error.into()),
        };
        self.state = IncomingVersionReplicationState::VerifyReplaced;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: key.into(),
            txn_id: self.txn_id,
        })]
    }

    fn delete_replaced_metadata(
        &mut self,
        values: Vec<(aruna_core::types::Key, aruna_core::types::Value)>,
    ) -> Effects {
        let mut deletes = Vec::with_capacity(values.len() + 2);
        let summary_key =
            match MultipartObjectMetadataKey::summary(self.manifest.version_id).to_bytes() {
                Ok(key) => key.into(),
                Err(error) => return self.fail(error.into()),
            };
        deletes.push((
            S3_MULTIPART_OBJECT_METADATA_KEYSPACE.to_string(),
            summary_key,
        ));
        deletes.extend(
            values
                .into_iter()
                .map(|(key, _)| (S3_MULTIPART_OBJECT_METADATA_KEYSPACE.to_string(), key)),
        );
        if let Some(hash) = self
            .replaced_version
            .as_ref()
            .and_then(BlobVersion::blob_hash)
        {
            let context = match self.alias_context() {
                Ok(context) => context,
                Err(error) => return self.fail(error),
            };
            let key = match context
                .hash_path_index_key(*hash, self.manifest.version_id)
                .to_bytes()
            {
                Ok(key) => key.into(),
                Err(error) => return self.fail(error.into()),
            };
            deletes.push((HASH_PATHS_INDEX_KEYSPACE.to_string(), key));
        }
        self.state = IncomingVersionReplicationState::DeleteReplacedMetadata;
        smallvec![Effect::Storage(StorageEffect::BatchDelete {
            deletes,
            txn_id: self.txn_id,
        })]
    }

    /// The copy the replaced version named, once the replacement stops naming
    /// it. Nothing else drops that reference, so without this the bytes stay
    /// charged to the backend forever.
    fn replaced_reclaim_key(&self) -> Option<ReclaimCandidateKey> {
        let replaced = self.replaced_version.as_ref()?.location_key()?;
        let replacement = self.effective_materialized_location().ok().and_then(|it| {
            let hash: [u8; 32] = it.get_blake3()?.try_into().ok()?;
            Some(BlobLocationKey::new(hash, it.backend))
        });
        (replacement.as_ref() != Some(&replaced))
            .then(|| ReclaimCandidateKey::new(replaced.backend, replaced.blake3_hash))
    }

    fn write_replaced_candidate(&mut self, key: ReclaimCandidateKey) -> Effects {
        let candidate = ReclaimCandidate {
            enqueued_at: SystemTime::now(),
        };
        let value = match candidate.to_bytes() {
            Ok(value) => value,
            Err(error) => return self.fail(error.into()),
        };
        self.state = IncomingVersionReplicationState::WriteReclaimCandidate;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: BLOB_RECLAIM_KEYSPACE.to_string(),
            key: key.to_bytes().into(),
            value: value.into(),
            txn_id: self.txn_id,
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
        if self.is_reference_item() {
            return self.write_object_lookup_or_continue();
        }
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
        if let Some(effect) = fence_backend(&location.backend, self.txn_id) {
            self.state = IncomingVersionReplicationState::FenceBackend;
            return smallvec![effect];
        }
        self.verify_existing_blob()
    }

    /// A negotiation that adopted an existing copy read it outside the
    /// transaction. Re-reading it inside makes the commit fail rather than
    /// leave the version naming bytes another writer has since removed.
    fn verify_existing_blob(&mut self) -> Effects {
        if self.received_blob_location.is_some() {
            return self.write_blob_location();
        }
        let Some(location) = self.existing_blob_location.clone() else {
            return self.write_blob_location();
        };
        let Some(blake3_hash) = location.get_blake3() else {
            return self.write_blob_location();
        };
        let hash: [u8; 32] = match blake3_hash.try_into() {
            Ok(hash) => hash,
            Err(err) => return self.fail(ConversionError::from(err).into()),
        };
        self.state = IncomingVersionReplicationState::VerifyExistingBlob;
        smallvec![blob_location_read(
            &BlobLocationKey::new(hash, location.backend),
            self.txn_id
        )]
    }

    fn write_blob_location(&mut self) -> Effects {
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

    fn read_current(&mut self, version_id: Ulid) -> Effects {
        self.state = IncomingVersionReplicationState::ReadCurrentVersion;
        let key = match VersionKey::new(&self.manifest.bucket, &self.manifest.key, version_id)
            .to_bytes()
        {
            Ok(key) => key,
            Err(err) => return self.fail(err.into()),
        };
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: key.into(),
            txn_id: self.txn_id,
        })]
    }

    fn apply_liveness(&mut self, previous_live: bool) -> Effects {
        let next_live = self.manifest.kind == ReplicationItemKind::Materialized;
        self.object_delta = i128::from(u8::from(next_live)) - i128::from(u8::from(previous_live));
        self.prepare_head_transition()
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

        match existing_pointer {
            Some(pointer) => self.read_current(pointer.version_id),
            None => self.apply_liveness(false),
        }
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
        let (version, materialized_hash) = match self.manifest.kind {
            ReplicationItemKind::Materialized => {
                if self.is_reference_item() {
                    let version = match self.reference_version() {
                        Ok(version) => version,
                        Err(error) => return self.fail(error),
                    };
                    (version, None)
                } else {
                    let Ok(location) = self.effective_materialized_location() else {
                        return self.fail(IncomingVersionReplicationError::MissingBlobLocation);
                    };
                    let Some(blake3_hash) = location.get_blake3() else {
                        return self.fail(IncomingVersionReplicationError::MissingBlobLocation);
                    };
                    let hash: [u8; 32] = match blake3_hash.try_into() {
                        Ok(hash) => hash,
                        Err(err) => return self.fail(ConversionError::from(err).into()),
                    };
                    (
                        BlobVersion::materialized(
                            hash,
                            location.backend.clone(),
                            self.manifest.created_at,
                            self.manifest.created_by,
                            self.manifest.source.clone(),
                        )
                        .with_metadata(self.manifest.metadata.clone())
                        .with_publisher(self.publisher_node_id),
                        Some(hash),
                    )
                }
            }
            ReplicationItemKind::DeleteMarker => (
                BlobVersion::deleted(self.manifest.created_at, self.manifest.created_by)
                    .with_publisher(self.publisher_node_id),
                None,
            ),
        };

        let effect = match write_blob_version_effect(&version_key, &version, self.txn_id) {
            Ok(effect) => effect,
            Err(err) => return self.fail(err.into()),
        };
        if let Some(hash) = materialized_hash {
            let context = match self.alias_context() {
                Ok(context) => context,
                Err(err) => return self.fail(err),
            };
            match add_hash_path_index_effect(&context, hash, self.manifest.version_id, self.txn_id)
            {
                Ok(index_effect) => self.pending_version_effects.push_back(index_effect),
                Err(err) => return self.fail(err.into()),
            }
        }
        smallvec![effect]
    }

    fn write_multipart_metadata_or_continue(&mut self) -> Effects {
        let Some(multipart) = self.manifest.multipart.as_ref() else {
            return self.write_live_obligation();
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

    fn write_live_obligation(&mut self) -> Effects {
        self.state = IncomingVersionReplicationState::WriteLiveObligation;
        let record = LiveReplicationObligationRecord::new(
            self.local_node_id,
            self.manifest
                .writer_auth_context
                .clone()
                .unwrap_or_else(|| self.manifest.auth_context.clone()),
            self.manifest.bucket.clone(),
            self.manifest.key.clone(),
            self.manifest.version_id,
            self.manifest.kind == ReplicationItemKind::DeleteMarker,
        )
        .with_origin(self.manifest.origin.clone())
        .with_sources(self.manifest.upstream_sources.clone());
        match live_obligation_effect(record, self.txn_id) {
            Ok(effect) => smallvec![effect],
            Err(error) => self.fail(error.into()),
        }
    }

    fn usage_delta(&self) -> Result<UsageDelta, IncomingVersionReplicationError> {
        let bytes = match self.manifest.kind {
            ReplicationItemKind::Materialized => i128::from(self.incoming_logical_bytes()?),
            ReplicationItemKind::DeleteMarker => 0,
        };
        Ok(UsageDelta {
            objects: self.object_delta,
            logical_bytes: if self.is_reference_item() { 0 } else { bytes }
                - i128::from(self.replaced_logical_bytes),
            referenced_bytes: if self.is_reference_item() { bytes } else { 0 }
                - i128::from(self.replaced_reference_bytes),
            ..Default::default()
        })
    }

    fn start_commit_quota(&mut self) -> Effects {
        let Some(group_id) = self.destination_group_id else {
            return self.fail(IncomingVersionReplicationError::DestinationBucketNotFound);
        };
        let group_delta = match self.usage_delta() {
            Ok(delta) => delta,
            Err(error) => return self.fail(error),
        };
        if self.manifest.kind == ReplicationItemKind::DeleteMarker {
            let update = UsageCounterUpdate::for_group(group_id, group_delta);
            if update.is_noop() {
                return self.commit_transaction();
            }
            self.usage_update = Some(update);
            return self.start_usage_update();
        }
        if self.is_reference_item() {
            self.usage_update = Some(UsageCounterUpdate::for_group(group_id, group_delta));
            return self.start_usage_update();
        }
        let Some(blob) = self.manifest.blob.as_ref() else {
            return self.fail(IncomingVersionReplicationError::MissingBlobInfo);
        };
        self.usage_update = Some(match self.received_blob_location.as_ref() {
            None => UsageCounterUpdate::for_group(group_id, group_delta),
            Some(location) => match StoredDelta::for_location(location, true) {
                Some(stored) => UsageCounterUpdate::with_stored(group_id, group_delta, stored),
                None => return self.fail(IncomingVersionReplicationError::MissingBlobInfo),
            },
        });
        let Some(txn_id) = self.txn_id else {
            return self.fail(IncomingVersionReplicationError::StorageError(
                StorageError::TransactionNotFound,
            ));
        };
        let quota_bytes = blob.size.saturating_sub(self.replaced_logical_bytes);
        if let Some(ceiling) = self.quota_ceiling
            && quota_bytes > 0
        {
            let mut gate = QuotaGate::new_for_realm(
                ceiling,
                quota_bytes,
                group_id,
                self.local_node_id,
                self.local_realm_id,
            );
            self.state = IncomingVersionReplicationState::CheckCommitQuota;
            let effects = gate.start(txn_id);
            self.quota_gate = Some(gate);
            effects
        } else {
            self.start_usage_update()
        }
    }

    fn start_usage_update(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(IncomingVersionReplicationError::StorageError(
                StorageError::TransactionNotFound,
            ));
        };
        self.state = IncomingVersionReplicationState::UpdateUsage;
        match self.usage_update.as_mut() {
            Some(update) => update.start(txn_id),
            None => self.fail(IncomingVersionReplicationError::ReplicationError(
                ReplicationError::ReplicationFailed,
            )),
        }
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
        if self.is_reference_item() {
            return self.send_apply_complete();
        }
        let Ok(location) = self.effective_materialized_location() else {
            return self.send_apply_complete();
        };
        let Some(blake3_hash) = location.get_blake3() else {
            return self.send_apply_complete();
        };

        self.state = IncomingVersionReplicationState::RegisterBlobInDht;
        let effect = match dht_registration_effect(
            blake3_hash,
            self.local_realm_id,
            self.local_node_id,
            &self.rocrate_limits,
        ) {
            Ok(effect) => effect,
            Err(_) => return self.send_apply_complete(),
        };
        smallvec![effect]
    }

    fn finish_live_drain(&mut self) -> Effects {
        match self.manifest.kind {
            ReplicationItemKind::Materialized => self.register_blob_in_dht_or_continue(),
            ReplicationItemKind::DeleteMarker => self.send_apply_complete(),
        }
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
    type Output = Result<IncomingVersionReplicationResult, IncomingVersionReplicationError>;
    type Error = IncomingVersionReplicationError;

    fn start(&mut self) -> Effects {
        if self.is_reference_item()
            && let Err(error) = self.reference_version()
        {
            return self.reject_negotiation(error);
        }
        if self
            .manifest
            .origin
            .as_ref()
            .is_some_and(|origin| origin.hop_count > 4)
        {
            return self.reject_negotiation(IncomingVersionReplicationError::HopLimitExceeded);
        }
        if self.manifest.auth_context.realm_id != self.local_realm_id {
            return self.reject_negotiation(IncomingVersionReplicationError::RealmMismatch);
        }
        if self
            .manifest
            .writer_auth_context
            .as_ref()
            .is_some_and(|auth| auth.realm_id != self.local_realm_id)
        {
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
                    if self.create_attempted {
                        return self.reject_negotiation(
                            IncomingVersionReplicationError::DestinationBucketNotFound,
                        );
                    }
                    return self.create_destination_bucket();
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
                self.destination_rules = bucket_info.storage_routing;
                self.load_destination_routing()
            }
            IncomingVersionReplicationState::LoadDestinationRouting => {
                let Event::SubOperation(SubOperationEvent::GroupRoutingLoaded { result }) = event
                else {
                    return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::SubOperation(SubOperationEvent::GroupRoutingLoaded)",
                        received: event,
                    });
                };
                match result {
                    Ok(inputs) => self.destination_inputs = inputs,
                    Err(error) => {
                        return self
                            .fail(IncomingVersionReplicationError::RoutingInputsFailed(error));
                    }
                }
                self.check_writer_permission(
                    self.destination_group_id.unwrap_or(self.manifest.group_id),
                )
            }
            IncomingVersionReplicationState::CreateDestinationBucket => {
                let Event::SubOperation(SubOperationEvent::BucketCreated { result }) = event else {
                    return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::SubOperation(SubOperationEvent::BucketCreated)",
                        received: event,
                    });
                };
                if let Err(reason) = result {
                    debug!(
                        bucket = %self.manifest.bucket,
                        stream_id = %self.stream_id,
                        reason = %reason,
                        "Destination bucket auto-create did not create; re-reading"
                    );
                }
                self.read_destination_bucket()
            }
            IncomingVersionReplicationState::CheckWriterPermissions => {
                let Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) = event
                else {
                    return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::SubOperation(SubOperationEvent::AuthorizationResult)",
                        received: event,
                    });
                };

                match allowed {
                    Ok(true) => self.read_existing_version(),
                    Ok(false) => self.reject_negotiation(
                        IncomingVersionReplicationError::WriterPermissionDenied,
                    ),
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

                if let Some(value) = value {
                    let existing = match BlobVersion::from_bytes(value.as_ref()) {
                        Ok(existing) => existing,
                        Err(error) => return self.fail(error.into()),
                    };
                    self.replaced_version = Some(existing.clone());
                    match &existing.state {
                        BlobVersionState::Reference {
                            cached_metadata, ..
                        } => {
                            self.replaced_reference_bytes = cached_metadata.content_length;
                            if self.is_reference_item() {
                                let incoming = match self.reference_version() {
                                    Ok(incoming) => incoming,
                                    Err(error) => return self.fail(error),
                                };
                                if existing == incoming {
                                    return self.send_negotiation(
                                        ReplicationNegotiationResult::AlreadyReplicatedVersion,
                                    );
                                }
                                return self.send_negotiation(
                                    ReplicationNegotiationResult::NeedVersionOnly,
                                );
                            }
                        }
                        BlobVersionState::Materialized { blob_hash, .. } => {
                            if !self.is_reference_item()
                                && self
                                    .manifest
                                    .blob
                                    .as_ref()
                                    .is_some_and(|blob| blob.hash == *blob_hash)
                            {
                                return self.send_negotiation(
                                    ReplicationNegotiationResult::AlreadyReplicatedVersion,
                                );
                            }
                            let Some(key) = existing.location_key() else {
                                return self
                                    .fail(IncomingVersionReplicationError::MissingBlobLocation);
                            };
                            return self.read_replaced_blob(key);
                        }
                        BlobVersionState::Deleted
                            if self.manifest.kind == ReplicationItemKind::DeleteMarker =>
                        {
                            return self.send_negotiation(
                                ReplicationNegotiationResult::AlreadyReplicatedVersion,
                            );
                        }
                        BlobVersionState::Deleted => {}
                    }
                }

                match self.manifest.kind {
                    ReplicationItemKind::DeleteMarker => {
                        self.send_negotiation(ReplicationNegotiationResult::NeedVersionOnly)
                    }
                    ReplicationItemKind::Materialized => self.read_quota_config(),
                }
            }
            IncomingVersionReplicationState::ReadReplacedBlob => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::ReadResult)",
                        received: event,
                    });
                };
                self.replaced_logical_bytes = match value
                    .as_ref()
                    .map(|value| BackendLocation::from_bytes(value.as_ref()))
                    .transpose()
                {
                    Ok(location) => location.map_or(0, |location| location.blob_size),
                    Err(error) => return self.fail(error.into()),
                };
                if self.is_reference_item() {
                    self.send_negotiation(ReplicationNegotiationResult::NeedVersionOnly)
                } else {
                    self.read_quota_config()
                }
            }
            IncomingVersionReplicationState::ReadQuotaConfig => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::ReadResult)",
                        received: event,
                    });
                };
                let Some(group_id) = self.destination_group_id else {
                    return self.fail(IncomingVersionReplicationError::DestinationBucketNotFound);
                };
                let ceiling = match value
                    .map(|value| RealmConfigDocument::from_bytes(value.as_ref()))
                    .transpose()
                {
                    Ok(Some(config)) => config.quota.effective_group_ceiling(&group_id),
                    Ok(None) => None,
                    Err(err) => return self.fail(err.into()),
                };

                match ceiling {
                    _ if self.is_reference_item() => {
                        self.send_negotiation(ReplicationNegotiationResult::NeedVersionOnly)
                    }
                    Some(ceiling) => self.start_quota_check(ceiling),
                    None => self.read_existing_blob(),
                }
            }
            IncomingVersionReplicationState::StartQuotaCheck => {
                let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
                    return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::TransactionStarted)",
                        received: event,
                    });
                };
                self.txn_id = Some(txn_id);
                let Some(gate) = self.quota_gate.as_mut() else {
                    return self.fail(IncomingVersionReplicationError::ReplicationError(
                        ReplicationError::ReplicationFailed,
                    ));
                };
                self.state = IncomingVersionReplicationState::EnforceQuota;
                gate.start(txn_id)
            }
            IncomingVersionReplicationState::EnforceQuota => {
                let Some(txn_id) = self.txn_id else {
                    return self.fail(IncomingVersionReplicationError::StorageError(
                        StorageError::TransactionNotFound,
                    ));
                };
                let Some(gate) = self.quota_gate.as_mut() else {
                    return self.fail(IncomingVersionReplicationError::ReplicationError(
                        ReplicationError::ReplicationFailed,
                    ));
                };
                match gate.step(event, txn_id) {
                    Ok(Some(effects)) => effects,
                    Ok(None) => self.finish_quota_check(),
                    Err(err) => self.fail(err.into()),
                }
            }
            IncomingVersionReplicationState::FinishQuotaCheck => {
                let Event::Storage(StorageEvent::TransactionAborted { .. }) = event else {
                    return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::TransactionAborted)",
                        received: event,
                    });
                };
                self.txn_id = None;
                if self.quota_gate.as_ref().is_some_and(QuotaGate::is_exceeded) {
                    self.output = Some(Ok(self.result(false)));
                    self.send_negotiation(ReplicationNegotiationResult::Rejected(
                        "quota".to_string(),
                    ))
                } else if self.is_reference_item() {
                    self.send_negotiation(ReplicationNegotiationResult::NeedVersionOnly)
                } else {
                    self.read_existing_blob()
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
                                return self.request_blob_version();
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
                            self.request_blob_version()
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
                    self.request_blob_version()
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
                self.verify_replaced()
            }
            IncomingVersionReplicationState::VerifyReplaced => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::ReadResult)",
                        received: event,
                    });
                };
                let current = match value
                    .as_ref()
                    .map(|value| BlobVersion::from_bytes(value.as_ref()))
                    .transpose()
                {
                    Ok(current) => current,
                    Err(error) => return self.fail(error.into()),
                };
                if current != self.replaced_version {
                    return self.fail(IncomingVersionReplicationError::StorageError(
                        StorageError::TransactionConflict,
                    ));
                }
                self.read_replaced_metadata()
            }
            IncomingVersionReplicationState::ReadReplacedMetadata => {
                let Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }) = event
                else {
                    return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::IterResult)",
                        received: event,
                    });
                };
                if next_start_after.is_some() {
                    return self.fail(IncomingVersionReplicationError::MultipartMetadataOverflow);
                }
                self.delete_replaced_metadata(values)
            }
            IncomingVersionReplicationState::DeleteReplacedMetadata => {
                let Event::Storage(StorageEvent::BatchDeleteResult { .. }) = event else {
                    return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::BatchDeleteResult)",
                        received: event,
                    });
                };
                match self.replaced_reclaim_key() {
                    Some(key) => self.write_replaced_candidate(key),
                    None => {
                        self.replaced_version = None;
                        self.write_hash_lookup_or_continue()
                    }
                }
            }
            IncomingVersionReplicationState::WriteReclaimCandidate => {
                let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
                    return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::WriteResult)",
                        received: event,
                    });
                };
                self.replaced_version = None;
                self.write_hash_lookup_or_continue()
            }
            IncomingVersionReplicationState::FenceBackend => match check_fence(event) {
                Ok(()) => self.verify_existing_blob(),
                Err(error) => self.fail(error.into()),
            },
            IncomingVersionReplicationState::VerifyExistingBlob => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::ReadResult)",
                        received: event,
                    });
                };
                let stored = match value
                    .map(|value| BackendLocation::from_bytes(value.as_ref()))
                    .transpose()
                {
                    Ok(stored) => stored,
                    Err(error) => return self.fail(error.into()),
                };
                if stored != self.existing_blob_location {
                    return self.fail(IncomingVersionReplicationError::ExistingBlobChanged);
                }
                self.write_blob_location()
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
            IncomingVersionReplicationState::ReadCurrentVersion => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::ReadResult)",
                        received: event,
                    });
                };
                let Some(value) = value else {
                    return self.fail(IncomingVersionReplicationError::CurrentVersionNotFound);
                };
                let version = match BlobVersion::from_bytes(value.as_ref()) {
                    Ok(version) => version,
                    Err(error) => return self.fail(error.into()),
                };
                self.apply_liveness(!version.is_deleted())
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
                if let Some(effect) = self.pending_version_effects.pop_front() {
                    return smallvec![effect];
                }
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
                self.write_live_obligation()
            }
            IncomingVersionReplicationState::WriteLiveObligation => {
                let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
                    return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::WriteResult)",
                        received: event,
                    });
                };
                self.start_commit_quota()
            }
            IncomingVersionReplicationState::CheckCommitQuota => {
                let Some(txn_id) = self.txn_id else {
                    return self.fail(IncomingVersionReplicationError::StorageError(
                        StorageError::TransactionNotFound,
                    ));
                };
                let Some(gate) = self.quota_gate.as_mut() else {
                    return self.fail(IncomingVersionReplicationError::ReplicationError(
                        ReplicationError::ReplicationFailed,
                    ));
                };
                match gate.step(event, txn_id) {
                    Ok(Some(effects)) => effects,
                    Ok(None) if gate.is_exceeded() => {
                        self.fail(IncomingVersionReplicationError::QuotaExceeded)
                    }
                    Ok(None) => self.start_usage_update(),
                    Err(error) => self.fail(error.into()),
                }
            }
            IncomingVersionReplicationState::UpdateUsage => {
                let Some(txn_id) = self.txn_id else {
                    return self.fail(IncomingVersionReplicationError::StorageError(
                        StorageError::TransactionNotFound,
                    ));
                };
                let Some(update) = self.usage_update.as_mut() else {
                    return self.fail(IncomingVersionReplicationError::ReplicationError(
                        ReplicationError::ReplicationFailed,
                    ));
                };
                match update.step(event, txn_id) {
                    Ok(Some(effects)) => effects,
                    Ok(None) => self.commit_transaction(),
                    Err(error) => self.fail(error.into()),
                }
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
                self.state = IncomingVersionReplicationState::ScheduleUsage;
                smallvec![schedule_usage_snapshot_publish_effect()]
            }
            IncomingVersionReplicationState::ScheduleUsage => match event {
                Event::Task(TaskEvent::TimerScheduled { .. })
                | Event::Task(TaskEvent::Error { .. }) => {
                    self.state = IncomingVersionReplicationState::ScheduleLiveDrain;
                    smallvec![schedule_blob_replication_drain_effect()]
                }
                other => {
                    warn!(event = ?other, "Incoming replication committed but usage scheduling returned an unexpected event");
                    self.state = IncomingVersionReplicationState::ScheduleLiveDrain;
                    smallvec![schedule_blob_replication_drain_effect()]
                }
            },
            IncomingVersionReplicationState::ScheduleLiveDrain => match event {
                Event::Task(TaskEvent::TimerScheduled { .. })
                | Event::Task(TaskEvent::Error { .. }) => self.finish_live_drain(),
                other => {
                    warn!(event = ?other, "Incoming replication committed but drain scheduling returned an unexpected event");
                    self.finish_live_drain()
                }
            },
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
                    self.output = Some(Ok(self.result(self.apply_committed)));
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
        let default = self.result(self.apply_committed);
        let output = self.output.unwrap_or(Ok(default));
        match (self.state, output) {
            (IncomingVersionReplicationState::Error, Err(error)) => Err(error),
            (_, output) => Ok(output),
        }
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
        MaterializedBlobInfo, SyncOrigin, VersionReplicationManifest, VersionReplicationMessage,
    };
    use crate::replication::queue::LiveReplicationObligationRecord;
    use aruna_core::UserId;
    use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
    use aruna_core::errors::{AuthorizationError, StorageError};
    use aruna_core::events::{BlobEvent, Event, StorageEvent, SubOperationEvent};
    use aruna_core::keyspaces::{
        BLOB_HEAD_KEYSPACE, BLOB_LIVE_REPLICATION_OBLIGATION_KEYSPACE, BLOB_LOCATIONS_KEYSPACE,
        BLOB_RECLAIM_KEYSPACE, BLOB_VERSIONS_KEYSPACE, HASH_PATHS_INDEX_KEYSPACE,
        S3_BUCKET_KEYSPACE, S3_MULTIPART_OBJECT_METADATA_KEYSPACE,
    };
    use aruna_core::operation::Operation;
    use aruna_core::structs::{
        AuthContext, BackendLocation, BackendRef, BlobLocationKey, BlobVersion, BlobVersionState,
        BucketInfo, CurrentVersionPointer, GroupRoutingInputs, HashPathIndexKey,
        MultipartObjectMetadataKey, NodeRouting, QuotaConfig, RealmConfigDocument, RealmId,
        ReclaimCandidateKey, ReplicationItemKind, ReplicationNegotiationResult, RoutingTarget,
        SourceConnectorKind, SourceMetadata, StagingStrategy, StorageRoutingRule,
        VersionSourceBinding,
    };
    use std::collections::{BTreeSet, HashMap};
    use std::time::SystemTime;
    use ulid::Ulid;

    fn test_realm_id() -> RealmId {
        RealmId::from_bytes([7u8; 32])
    }

    fn test_user_id() -> UserId {
        UserId::nil(test_realm_id())
    }

    fn test_group_id() -> Ulid {
        Ulid::from_parts(7, 7)
    }

    fn make_location() -> BackendLocation {
        let mut hashes = HashMap::new();
        hashes.insert("blake3".to_string(), vec![1u8; 32]);
        BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/tmp".to_string(),
            storage_bucket: "blob-bucket".to_string(),
            backend_path: format!("bucket/key_{}", Ulid::generate()),
            ulid: Ulid::generate(),
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
            cors_configuration: None,
            replication: None,
            storage_routing: Vec::new(),
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
            version_id: Ulid::generate(),
            group_id: test_group_id(),
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
            reference_intent: false,
            origin: None,
            upstream_sources: Vec::new(),
            writer_auth_context: None,
            reference_metadata: None,
            metadata: HashMap::new(),
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
            connector_id: Some(Ulid::generate()),
        }
    }

    fn make_reference_manifest() -> VersionReplicationManifest {
        let mut manifest = make_manifest(ReplicationItemKind::Materialized);
        let mut source = make_source_binding();
        source.descriptor.kind = SourceConnectorKind::ArunaNative;
        source.descriptor.origin_node_id = Some(iroh::SecretKey::from_bytes(&[8u8; 32]).public());
        source.connector_id = None;
        manifest.blob = None;
        manifest.source = Some(source);
        manifest.reference_intent = true;
        manifest.reference_metadata = Some(SourceMetadata {
            content_length: 1_000_000,
            content_type: Some("application/octet-stream".to_string()),
            etag: None,
            last_modified: Some(manifest.created_at),
            source_version: None,
        });
        manifest
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

    /// Answers the routing load that follows every destination bucket read.
    fn load_routing(
        op: &mut IncomingVersionReplicationOperation,
        inputs: GroupRoutingInputs,
    ) -> aruna_core::types::Effects {
        assert_eq!(
            op.state,
            IncomingVersionReplicationState::LoadDestinationRouting
        );
        op.step(Event::SubOperation(SubOperationEvent::GroupRoutingLoaded {
            result: Ok(inputs),
        }))
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

        op.step(Event::Storage(StorageEvent::ReadResult {
            key: b"bucket".to_vec().into(),
            value: Some(make_bucket_info(group_id).to_bytes().unwrap().into()),
        }));
        let mut effects = load_routing(op, GroupRoutingInputs::default());
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

    fn advance_blob_lookup(
        op: &mut IncomingVersionReplicationOperation,
    ) -> aruna_core::types::Effects {
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: None,
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::ReadQuotaConfig);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { .. })]
        ));

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
        effects
    }

    fn start_apply_transaction(op: &mut IncomingVersionReplicationOperation) -> Ulid {
        let txn_id = Ulid::generate();
        op.state = IncomingVersionReplicationState::StartTransaction;
        op.negotiation_result = Some(ReplicationNegotiationResult::NeedVersionOnly);
        op.destination_group_id = Some(Ulid::generate());

        let effects = op.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        assert_eq!(op.state, IncomingVersionReplicationState::VerifyReplaced);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { key_space, txn_id: read_txn_id, .. })]
                if key_space == BLOB_VERSIONS_KEYSPACE && *read_txn_id == Some(txn_id)
        ));
        let value = op
            .replaced_version
            .as_ref()
            .map(|version| version.to_bytes().unwrap().into());
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value,
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::ReadObjectLookup);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { key_space, txn_id: read_txn_id, .. })]
                if key_space == BLOB_HEAD_KEYSPACE && *read_txn_id == Some(txn_id)
        ));
        txn_id
    }

    #[test]
    fn existing_version_skips() {
        let manifest = make_manifest(ReplicationItemKind::Materialized);
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest.clone(),
        );

        let _effects = advance_to_version_lookup(&mut op, Ulid::generate());

        let version = BlobVersion::materialized(
            manifest.blob.as_ref().unwrap().hash,
            BackendRef::node_default(),
            manifest.created_at,
            manifest.created_by,
            None,
        );
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: Some(version.to_bytes().unwrap().into()),
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::SendNegotiation);
        assert_eq!(effects.len(), 1);
        assert!(matches!(
            message_from_effect(&effects[0]),
            VersionReplicationMessage::VersionNegotiationResponse(
                ReplicationNegotiationResult::AlreadyReplicatedVersion
            )
        ));
    }

    #[test]
    fn existing_delete_skips() {
        let manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            test_realm_id(),
            manifest.clone(),
        );

        let _effects = advance_to_version_lookup(&mut op, test_group_id());
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: Some(
                BlobVersion::deleted(manifest.created_at, manifest.created_by)
                    .to_bytes()
                    .unwrap()
                    .into(),
            ),
        }));

        assert_eq!(op.state, IncomingVersionReplicationState::SendNegotiation);
        assert!(matches!(
            message_from_effect(&effects[0]),
            VersionReplicationMessage::VersionNegotiationResponse(
                ReplicationNegotiationResult::AlreadyReplicatedVersion
            )
        ));
    }

    #[test]
    fn reference_requests_metadata() {
        let manifest = make_reference_manifest();
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            test_realm_id(),
            manifest,
        );

        let _effects = advance_to_version_lookup(&mut op, test_group_id());
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: None,
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::ReadQuotaConfig);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { .. })]
        ));

        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: None,
        }));

        assert_eq!(op.state, IncomingVersionReplicationState::SendNegotiation);
        assert!(matches!(
            message_from_effect(&effects[0]),
            VersionReplicationMessage::VersionNegotiationResponse(
                ReplicationNegotiationResult::NeedVersionOnly
            )
        ));
    }

    #[test]
    fn reference_writes_version() {
        let manifest = make_reference_manifest();
        let expected_source = manifest.source.clone().unwrap();
        let expected_metadata = manifest.reference_metadata.clone().unwrap();
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            test_realm_id(),
            manifest,
        );
        op.txn_id = Some(Ulid::generate());

        let effects = op.write_blob_version();
        let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
            panic!("expected reference version write")
        };
        let version = BlobVersion::from_bytes(value.as_ref()).unwrap();

        assert!(matches!(
            version.state,
            BlobVersionState::Reference {
                source,
                cached_metadata,
                ..
            } if source == expected_source && cached_metadata == expected_metadata
        ));
        let usage = op.usage_delta().unwrap();
        assert_eq!(usage.logical_bytes, 0);
        assert_eq!(usage.referenced_bytes, 1_000_000);
    }

    #[test]
    fn version_binds_publisher_node() {
        // A forged manifest cannot forge attribution: the persisted version is
        // bound to the authenticated publisher, never to its self-asserted user.
        let publisher = iroh::SecretKey::from_bytes(&[42u8; 32]).public();
        let forged = UserId::local(Ulid::from_bytes([9u8; 16]), test_realm_id());
        let mut manifest = make_reference_manifest();
        manifest.created_by = forged;
        manifest.auth_context.user_id = forged;
        manifest.writer_auth_context = None;
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            test_realm_id(),
            manifest,
        )
        .with_publisher_node(publisher);
        op.txn_id = Some(Ulid::generate());

        let effects = op.write_blob_version();
        let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
            panic!("expected reference version write")
        };
        let version = BlobVersion::from_bytes(value.as_ref()).unwrap();
        assert_eq!(version.published_by, Some(publisher));
        assert_eq!(version.created_by, forged);
    }

    #[test]
    fn replacement_cleans_metadata() {
        let manifest = make_manifest(ReplicationItemKind::Materialized);
        let version_id = manifest.version_id;
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            test_realm_id(),
            manifest,
        );
        let txn_id = Ulid::generate();
        op.txn_id = Some(txn_id);
        op.destination_group_id = Some(test_group_id());
        op.replaced_version = Some(BlobVersion::materialized(
            [9u8; 32],
            BackendRef::node_default(),
            SystemTime::now(),
            test_user_id(),
            None,
        ));

        let effects = op.read_replaced_metadata();
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Iter { key_space, txn_id: effect_txn, .. })]
                if key_space == S3_MULTIPART_OBJECT_METADATA_KEYSPACE
                    && *effect_txn == Some(txn_id)
        ));

        let part_key = MultipartObjectMetadataKey::part(version_id, 3)
            .to_bytes()
            .unwrap();
        let mut effects = op.step(Event::Storage(StorageEvent::IterResult {
            values: vec![(part_key.clone().into(), vec![1u8].into())],
            next_start_after: None,
        }));
        let Effect::Storage(StorageEffect::BatchDelete {
            deletes,
            txn_id: effect_txn,
        }) = effects.remove(0)
        else {
            panic!("expected replacement metadata batch delete")
        };
        assert_eq!(effect_txn, Some(txn_id));
        let summary_key = MultipartObjectMetadataKey::summary(version_id)
            .to_bytes()
            .unwrap();
        assert!(deletes.iter().any(|(key_space, key)| {
            key_space == S3_MULTIPART_OBJECT_METADATA_KEYSPACE && key.as_ref() == summary_key
        }));
        assert!(deletes.iter().any(|(key_space, key)| {
            key_space == S3_MULTIPART_OBJECT_METADATA_KEYSPACE && key.as_ref() == part_key
        }));
        assert!(deletes.iter().any(|(key_space, key)| {
            key_space == HASH_PATHS_INDEX_KEYSPACE
                && HashPathIndexKey::from_bytes(key.as_ref())
                    .is_ok_and(|index| index.blake3_hash == [9u8; 32])
        }));

        let effects = op.step(Event::Storage(StorageEvent::BatchDeleteResult {
            entries: deletes,
        }));
        assert_eq!(
            op.state,
            IncomingVersionReplicationState::WriteReclaimCandidate
        );
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Write { key_space, .. })]
                if key_space == BLOB_RECLAIM_KEYSPACE
        ));

        let effects = op.step(Event::Storage(StorageEvent::WriteResult {
            key: vec![0u8; 4].into(),
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::ReadObjectLookup);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { key_space, .. })]
                if key_space == BLOB_HEAD_KEYSPACE
        ));
    }

    #[test]
    fn replacement_queues_reclaim() {
        // The copy a replaced materialized version named is unreferenced once
        // the replacement names a different one, and only this enqueue frees it.
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            test_realm_id(),
            make_manifest(ReplicationItemKind::Materialized),
        );
        op.txn_id = Some(Ulid::generate());
        op.replaced_version = Some(BlobVersion::materialized(
            [9u8; 32],
            BackendRef::node_default(),
            SystemTime::now(),
            test_user_id(),
            None,
        ));

        assert_eq!(
            op.replaced_reclaim_key(),
            Some(ReclaimCandidateKey::new(
                BackendRef::node_default(),
                [9u8; 32]
            ))
        );

        // The replacement adopting the very same copy must not queue it.
        let mut adopted = make_location();
        adopted.backend = BackendRef::node_default();
        adopted
            .hashes
            .insert("blake3".to_string(), [9u8; 32].to_vec());
        op.received_blob_location = Some(adopted);
        assert_eq!(op.replaced_reclaim_key(), None);
    }

    #[test]
    fn replaced_version_fenced() {
        let manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest.clone(),
        );
        let prior = BlobVersion::deleted(manifest.created_at, manifest.created_by);
        op.replaced_version = Some(prior);
        op.state = IncomingVersionReplicationState::StartTransaction;
        op.negotiation_result = Some(ReplicationNegotiationResult::NeedVersionOnly);
        op.destination_group_id = Some(Ulid::generate());

        let effects = op.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: Ulid::generate(),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { key_space, .. })]
                if key_space == BLOB_VERSIONS_KEYSPACE
        ));
        let current = BlobVersion::materialized(
            [9u8; 32],
            BackendRef::node_default(),
            manifest.created_at,
            manifest.created_by,
            None,
        );
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: Some(current.to_bytes().unwrap().into()),
        }));

        assert_eq!(op.state, IncomingVersionReplicationState::SendApplyRejected);
        assert!(matches!(
            op.output,
            Some(Err(IncomingVersionReplicationError::StorageError(
                StorageError::TransactionConflict
            )))
        ));
        assert!(matches!(
            message_from_effect(&effects[0]),
            VersionReplicationMessage::VersionApplyRejected(_)
        ));
    }

    #[test]
    fn changed_reference_updates() {
        let manifest = make_reference_manifest();
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            test_realm_id(),
            manifest.clone(),
        );
        let _effects = advance_to_version_lookup(&mut op, test_group_id());
        let mut metadata = manifest.reference_metadata.clone().unwrap();
        metadata.etag = Some("old-etag".to_string());
        let existing = BlobVersion::reference(
            manifest.source.clone().unwrap(),
            metadata,
            manifest.created_at,
            manifest.created_by,
            manifest.created_at,
        );

        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: Some(existing.to_bytes().unwrap().into()),
        }));

        assert_eq!(op.state, IncomingVersionReplicationState::SendNegotiation);
        assert!(matches!(
            message_from_effect(&effects[0]),
            VersionReplicationMessage::VersionNegotiationResponse(
                ReplicationNegotiationResult::NeedVersionOnly
            )
        ));
    }

    #[test]
    fn hop_limit_rejects() {
        let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        manifest.origin = Some(SyncOrigin {
            relationship_id: Ulid::generate(),
            hop_count: 5,
        });
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            test_realm_id(),
            manifest,
        );

        let effects = op.start();

        assert_eq!(op.state, IncomingVersionReplicationState::SendNegotiation);
        expect_rejected_negotiation(
            &effects[0],
            IncomingVersionReplicationError::HopLimitExceeded
                .to_string()
                .as_str(),
        );
    }

    #[test]
    fn obligation_keeps_origin() {
        let origin = SyncOrigin {
            relationship_id: Ulid::generate(),
            hop_count: 2,
        };
        let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        manifest.origin = Some(origin.clone());
        manifest.upstream_sources.push(
            aruna_core::structs::ArunaArn::s3_bucket(
                test_realm_id(),
                iroh::SecretKey::from_bytes(&[8u8; 32]).public(),
                "source",
            )
            .unwrap(),
        );
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            test_realm_id(),
            manifest,
        );

        let effects = op.write_live_obligation();

        let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
            panic!("expected live replication obligation write")
        };
        let obligation = LiveReplicationObligationRecord::from_bytes(value).unwrap();
        assert_eq!(obligation.origin, Some(origin));
        assert_eq!(obligation.upstream_sources, op.manifest.upstream_sources);
    }

    #[test]
    fn quota_excess_rejects() {
        let manifest = make_manifest(ReplicationItemKind::Materialized);
        let group_id = test_group_id();
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            test_realm_id(),
            manifest,
        );
        let _effects = advance_to_version_lookup(&mut op, group_id);
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: None,
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::ReadQuotaConfig);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { .. })]
        ));

        let mut config = RealmConfigDocument::default_for_realm(test_realm_id(), Vec::new());
        config.quota = QuotaConfig {
            default_group_quota_bytes: Some(1),
            grace_factor_percent: 100,
            ..QuotaConfig::default()
        };
        let config_bytes = postcard::to_allocvec(&config).unwrap();
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: Some(config_bytes.clone().into()),
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::StartQuotaCheck);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::StartTransaction {
                read: true
            })]
        ));

        let txn_id = Ulid::generate();
        let effects = op.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        assert_eq!(op.state, IncomingVersionReplicationState::EnforceQuota);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { txn_id: read_txn_id, .. })]
                if *read_txn_id == Some(txn_id)
        ));
        op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: Some(config_bytes.into()),
        }));
        op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: None,
        }));
        let effects = op.step(Event::Storage(StorageEvent::IterResult {
            values: Vec::new(),
            next_start_after: None,
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::FinishQuotaCheck);
        assert_eq!(
            effects[0],
            Effect::Storage(StorageEffect::AbortTransaction { txn_id })
        );

        let effects = op.step(Event::Storage(StorageEvent::TransactionAborted { txn_id }));
        assert_eq!(op.state, IncomingVersionReplicationState::SendNegotiation);
        match message_from_effect(&effects[0]) {
            VersionReplicationMessage::VersionNegotiationResponse(
                ReplicationNegotiationResult::Rejected(reason),
            ) => assert_eq!(reason, "quota"),
            other => panic!("expected quota rejection, got {other:?}"),
        }
    }

    #[test]
    fn full_backend_rejects() {
        // Replication now routes through the quota-marked catalog, so a full
        // destination backend owes the sender a reason before any transfer.
        let manifest = make_manifest(ReplicationItemKind::Materialized);
        let group_id = test_group_id();
        let mut routing = NodeRouting::default();
        routing.catalog = routing.catalog.mark_full(BackendRef::DEFAULT_NODE_NAME);
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            test_realm_id(),
            manifest,
        )
        .with_routing(routing);

        let _effects = advance_to_version_lookup(&mut op, group_id);
        let effects = advance_blob_lookup(&mut op);
        assert_eq!(op.state, IncomingVersionReplicationState::ReadExistingBlob);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { .. })]
        ));
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: None,
        }));

        assert_eq!(op.state, IncomingVersionReplicationState::SendNegotiation);
        match message_from_effect(&effects[0]) {
            VersionReplicationMessage::VersionNegotiationResponse(
                ReplicationNegotiationResult::Rejected(reason),
            ) => assert!(reason.contains("quota"), "unexpected reason: {reason}"),
            other => panic!("expected a rejected negotiation, got {other:?}"),
        }
    }

    #[test]
    fn full_backend_dedupes() {
        // A blob the destination already holds stores no bytes, so its cap has
        // nothing left to protect.
        let manifest = make_manifest(ReplicationItemKind::Materialized);
        let existing = manifest
            .blob
            .as_ref()
            .map(|blob| blob.location.clone())
            .unwrap();
        let group_id = test_group_id();
        let mut routing = NodeRouting::default();
        routing.catalog = routing.catalog.mark_full(BackendRef::DEFAULT_NODE_NAME);
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            test_realm_id(),
            manifest,
        )
        .with_routing(routing);

        let _effects = advance_to_version_lookup(&mut op, group_id);
        advance_blob_lookup(&mut op);
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: Some(existing.to_bytes().unwrap().into()),
        }));

        assert_eq!(op.state, IncomingVersionReplicationState::SendNegotiation);
        assert!(matches!(
            message_from_effect(&effects[0]),
            VersionReplicationMessage::VersionNegotiationResponse(
                ReplicationNegotiationResult::NeedVersionOnly
            )
        ));
    }

    #[test]
    fn marker_ignores_quota() {
        // A delete marker stores no bytes, so a full destination must still let
        // the tombstone converge.
        let group_id = test_group_id();
        let mut routing = NodeRouting::default();
        routing.catalog = routing.catalog.mark_full(BackendRef::DEFAULT_NODE_NAME);
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            test_realm_id(),
            make_manifest(ReplicationItemKind::DeleteMarker),
        )
        .with_routing(routing);

        let _effects = advance_to_version_lookup(&mut op, group_id);
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: None,
        }));

        assert_eq!(op.state, IncomingVersionReplicationState::SendNegotiation);
        assert!(matches!(
            message_from_effect(&effects[0]),
            VersionReplicationMessage::VersionNegotiationResponse(
                ReplicationNegotiationResult::NeedVersionOnly
            )
        ));
    }

    #[test]
    fn stale_current_pointer_update_skips_current_pointer_overwrite() {
        let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        manifest.current_version_generation = Some(10);
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest.clone(),
        );
        let txn_id = start_apply_transaction(&mut op);
        let existing_pointer = CurrentVersionPointer::new_with_generation(Ulid::generate(), 20);

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
        assert_eq!(op.object_delta, 0);
        assert_eq!(op.usage_delta().unwrap().objects, 0);
    }

    #[test]
    fn current_manifest_without_pointer_generation_rejects_apply() {
        let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        manifest.current_version_generation = None;
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );
        let txn_id = Ulid::generate();
        op.state = IncomingVersionReplicationState::StartTransaction;
        op.negotiation_result = Some(ReplicationNegotiationResult::NeedVersionOnly);

        let effects = op.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { key_space, .. })]
                if key_space == BLOB_VERSIONS_KEYSPACE
        ));
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: None,
        }));

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
            Ulid::generate(),
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
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest.clone(),
        );
        start_apply_transaction(&mut op);
        let existing_pointer = CurrentVersionPointer::new_with_generation(Ulid::generate(), 2);

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
        manifest
            .metadata
            .insert("mtime".to_string(), "1753272000.123456789".to_string());
        let expected_metadata = manifest.metadata.clone();
        manifest.current_version = false;
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );
        op.txn_id = Some(Ulid::generate());
        op.destination_group_id = Some(Ulid::generate());
        op.existing_blob_location = Some(make_location());

        let effects = op.write_version();

        let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
            panic!("expected blob version write")
        };
        let version = BlobVersion::from_bytes(value.as_ref()).unwrap();
        assert_eq!(version.source_binding(), Some(&source));
        assert_eq!(version.metadata, expected_metadata);
    }

    #[test]
    fn write_version_indexes_non_current_materialized_version_by_content_hash() {
        let mut manifest = make_manifest(ReplicationItemKind::Materialized);
        manifest.current_version = false;
        let group_id = Ulid::generate();
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest.clone(),
        );
        op.txn_id = Some(Ulid::generate());
        op.destination_group_id = Some(group_id);
        op.existing_blob_location = Some(make_location());

        let effects = op.write_version();
        let [Effect::Storage(StorageEffect::Write { key_space, .. })] = effects.as_slice() else {
            panic!("expected blob version write")
        };
        assert_eq!(key_space, BLOB_VERSIONS_KEYSPACE);

        let effects = op.step(Event::Storage(StorageEvent::WriteResult {
            key: vec![0u8; 4].into(),
        }));
        let [Effect::Storage(StorageEffect::Write { key_space, key, .. })] = effects.as_slice()
        else {
            panic!("expected hash path index write")
        };
        assert_eq!(key_space, HASH_PATHS_INDEX_KEYSPACE);
        let index_key = HashPathIndexKey::from_bytes(key.as_ref()).unwrap();
        assert_eq!(index_key.blake3_hash, [1u8; 32]);
        assert_eq!(index_key.version_id, manifest.version_id);
        assert_eq!(index_key.group_id, group_id);
        assert_eq!(index_key.bucket, manifest.bucket);
        assert_eq!(index_key.key, manifest.key);

        let effects = op.step(Event::Storage(StorageEvent::WriteResult {
            key: vec![0u8; 4].into(),
        }));
        assert_eq!(
            op.state,
            IncomingVersionReplicationState::WriteLiveObligation
        );
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Write { key_space, .. })]
                if key_space == BLOB_LIVE_REPLICATION_OBLIGATION_KEYSPACE
        ));

        let effects = op.step(Event::Storage(StorageEvent::WriteResult {
            key: vec![0u8; 4].into(),
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::UpdateUsage);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::BatchRead { .. })]
        ));

        let effects = op.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![(vec![0].into(), None), (vec![1].into(), None)],
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::BatchWrite { .. })]
        ));
        let effects = op.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::CommitTransaction);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::CommitTransaction { .. })]
        ));
    }

    #[test]
    fn newer_current_pointer_generation_allows_rollback_to_older_version_id() {
        let existing_version_id = Ulid::from_bytes([9u8; 16]);
        let incoming_version_id = Ulid::from_bytes([1u8; 16]);
        let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        manifest.version_id = incoming_version_id;
        manifest.current_version_generation = Some(20);
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
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

        assert_eq!(
            op.state,
            IncomingVersionReplicationState::ReadCurrentVersion
        );
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { key_space, .. })]
                if key_space == BLOB_VERSIONS_KEYSPACE
        ));
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: Some(
                BlobVersion::materialized(
                    [2u8; 32],
                    BackendRef::node_default(),
                    SystemTime::now(),
                    test_user_id(),
                    None,
                )
                .to_bytes()
                .unwrap()
                .into(),
            ),
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
        assert_eq!(op.object_delta, -1);
        assert_eq!(op.usage_delta().unwrap().objects, -1);
    }

    #[test]
    fn materialized_restores_object() {
        let mut manifest = make_manifest(ReplicationItemKind::Materialized);
        manifest.current_version_generation = Some(2);
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );
        start_apply_transaction(&mut op);
        let existing_pointer = CurrentVersionPointer::new_with_generation(Ulid::generate(), 1);

        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: Some(existing_pointer.to_bytes().unwrap().into()),
        }));
        assert_eq!(
            op.state,
            IncomingVersionReplicationState::ReadCurrentVersion
        );
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { key_space, .. })]
                if key_space == BLOB_VERSIONS_KEYSPACE
        ));

        op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: Some(
                BlobVersion::deleted(SystemTime::now(), test_user_id())
                    .to_bytes()
                    .unwrap()
                    .into(),
            ),
        }));

        let delta = op.usage_delta().unwrap();
        assert_eq!(delta.objects, 1);
        assert_eq!(delta.logical_bytes, 42);
    }

    #[test]
    fn same_generation_higher_ulid_overwrites_current_pointer() {
        let existing_version_id = Ulid::from_bytes([1u8; 16]);
        let incoming_version_id = Ulid::from_bytes([9u8; 16]);
        let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        manifest.version_id = incoming_version_id;
        manifest.current_version_generation = Some(7);
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
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

        assert_eq!(
            op.state,
            IncomingVersionReplicationState::ReadCurrentVersion
        );
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { key_space, .. })]
                if key_space == BLOB_VERSIONS_KEYSPACE
        ));
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: Some(
                BlobVersion::materialized(
                    [2u8; 32],
                    BackendRef::node_default(),
                    SystemTime::now(),
                    test_user_id(),
                    None,
                )
                .to_bytes()
                .unwrap()
                .into(),
            ),
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
            Ulid::generate(),
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
            Ulid::generate(),
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
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );

        let _effects = advance_to_version_lookup(&mut op, Ulid::generate());
        let effects = advance_blob_lookup(&mut op);
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
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );

        let _effects = advance_to_version_lookup(&mut op, Ulid::generate());
        let effects = advance_blob_lookup(&mut op);

        assert_eq!(op.state, IncomingVersionReplicationState::ReadExistingBlob);
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { key_space, .. })]
                if key_space == BLOB_LOCATIONS_KEYSPACE
        ));
    }

    /// Drives an incoming materialized version to the existing-copy probe under
    /// the given bucket rules and group inputs.
    fn probe_backend(
        rules: Vec<StorageRoutingRule>,
        inputs: GroupRoutingInputs,
    ) -> (
        IncomingVersionReplicationOperation,
        aruna_core::types::Effects,
    ) {
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            test_realm_id(),
            make_manifest(ReplicationItemKind::Materialized),
        );
        let mut bucket_info = make_bucket_info(test_group_id());
        bucket_info.storage_routing = rules;

        op.start();
        op.step(Event::Storage(StorageEvent::ReadResult {
            key: b"bucket".to_vec().into(),
            value: Some(bucket_info.to_bytes().unwrap().into()),
        }));
        load_routing(&mut op, inputs);
        let effects = advance_blob_lookup(&mut op);
        (op, effects)
    }

    fn group_backend_key(backend_id: Ulid) -> Vec<u8> {
        BlobLocationKey::new([1u8; 32], BackendRef::Group(backend_id)).to_bytes()
    }

    fn probed_key(effects: &aruna_core::types::Effects) -> Vec<u8> {
        let [Effect::Storage(StorageEffect::Read { key, .. })] = effects.as_slice() else {
            panic!("expected one location read, got {effects:?}")
        };
        key.to_vec()
    }

    #[test]
    fn refuses_vanished_copy() {
        // The adopted copy is re-read in the transaction, so a sweep that
        // removed it in between must fail the apply instead of committing.
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            test_realm_id(),
            make_manifest(ReplicationItemKind::Materialized),
        );
        let txn_id = Ulid::generate();
        op.txn_id = Some(txn_id);
        op.destination_group_id = Some(test_group_id());
        op.existing_blob_location = Some(make_location());

        let effects = op.write_blob_location_or_continue();
        assert_eq!(
            op.state,
            IncomingVersionReplicationState::VerifyExistingBlob
        );
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { key_space, txn_id: read_txn, .. })]
                if key_space == BLOB_LOCATIONS_KEYSPACE && *read_txn == Some(txn_id)
        ));

        op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: None,
        }));

        assert_eq!(
            op.output,
            Some(Err(IncomingVersionReplicationError::ExistingBlobChanged))
        );
    }

    #[test]
    fn probes_rule_backend() {
        // The probe must ask about the backend the bucket rule names.
        let backend_id = Ulid::from_bytes([4u8; 16]);
        let (op, effects) = probe_backend(
            vec![StorageRoutingRule {
                key_prefix: String::new(),
                exact: false,
                target: RoutingTarget::Backend(BackendRef::Group(backend_id)),
            }],
            GroupRoutingInputs {
                default_target: None,
                backend_ids: BTreeSet::from([backend_id]),
            },
        );

        assert_eq!(
            op.resolve_destination().unwrap().backend,
            BackendRef::Group(backend_id)
        );
        assert_eq!(probed_key(&effects), group_backend_key(backend_id));
    }

    #[test]
    fn probes_group_default() {
        let backend_id = Ulid::from_bytes([5u8; 16]);
        let (_op, effects) = probe_backend(
            Vec::new(),
            GroupRoutingInputs {
                default_target: Some(RoutingTarget::Backend(BackendRef::Group(backend_id))),
                backend_ids: BTreeSet::from([backend_id]),
            },
        );

        assert_eq!(probed_key(&effects), group_backend_key(backend_id));
    }

    #[test]
    fn keeps_loaded_inputs() {
        // The version-only path resolves from the same inputs the probe used.
        let backend_id = Ulid::from_bytes([6u8; 16]);
        let (mut op, _effects) = probe_backend(
            Vec::new(),
            GroupRoutingInputs {
                default_target: Some(RoutingTarget::Backend(BackendRef::Group(backend_id))),
                backend_ids: BTreeSet::from([backend_id]),
            },
        );
        let mut location = make_location();
        location.backend = BackendRef::Group(backend_id);

        op.step(Event::Storage(StorageEvent::ReadResult {
            key: group_backend_key(backend_id).into(),
            value: Some(location.to_bytes().unwrap().into()),
        }));

        assert_eq!(
            op.negotiation_result,
            Some(ReplicationNegotiationResult::NeedVersionOnly)
        );
        assert_eq!(
            op.resolve_destination().unwrap().backend,
            BackendRef::Group(backend_id)
        );
    }

    #[test]
    fn received_blob_manifest_mismatch_is_rejected_and_cleaned_up() {
        let manifest = make_manifest(ReplicationItemKind::Materialized);
        let stream_id = Ulid::generate();
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
    fn unbuildable_bucket_rejects() {
        // One create attempt, still missing, then reject and close the stream.
        let manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        let stream_id = Ulid::generate();
        let mut op = IncomingVersionReplicationOperation::new(
            stream_id,
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );

        op.start();
        op.step(Event::Storage(StorageEvent::ReadResult {
            key: b"bucket".to_vec().into(),
            value: None,
        }));
        assert_eq!(
            op.state,
            IncomingVersionReplicationState::CreateDestinationBucket
        );
        op.step(Event::SubOperation(SubOperationEvent::BucketCreated {
            result: Err("boom".to_string()),
        }));

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
    fn rejects_denied_writer() {
        // A replica whose original writer lacks WRITE on the destination path
        // is refused during negotiation.
        let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        manifest.writer_auth_context = Some(manifest.auth_context.clone());
        let stream_id = Ulid::generate();
        let mut op = IncomingVersionReplicationOperation::new(
            stream_id,
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );

        op.start();
        op.step(Event::Storage(StorageEvent::ReadResult {
            key: b"bucket".to_vec().into(),
            value: Some(
                make_bucket_info(Ulid::generate())
                    .to_bytes()
                    .unwrap()
                    .into(),
            ),
        }));
        let effects = load_routing(&mut op, GroupRoutingInputs::default());
        assert_eq!(
            op.state,
            IncomingVersionReplicationState::CheckWriterPermissions
        );
        assert!(matches!(effects[0], Effect::SubOperation(_)));

        let effects = op.step(Event::SubOperation(
            SubOperationEvent::AuthorizationResult { allowed: Ok(false) },
        ));
        assert_eq!(op.state, IncomingVersionReplicationState::SendNegotiation);
        expect_rejected_negotiation(
            &effects[0],
            IncomingVersionReplicationError::WriterPermissionDenied
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
    fn rejects_authorization_errors() {
        let mut manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        manifest.writer_auth_context = Some(manifest.auth_context.clone());
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );

        op.start();
        op.step(Event::Storage(StorageEvent::ReadResult {
            key: b"bucket".to_vec().into(),
            value: Some(
                make_bucket_info(Ulid::generate())
                    .to_bytes()
                    .unwrap()
                    .into(),
            ),
        }));
        load_routing(&mut op, GroupRoutingInputs::default());

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
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );

        let _effects = advance_to_version_lookup(&mut op, Ulid::generate());
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
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );

        let _effects = advance_to_version_lookup(&mut op, Ulid::generate());
        let effects = advance_blob_lookup(&mut op);
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
        let stream_id = Ulid::generate();
        let txn_id = Ulid::generate();
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
        let stream_id = Ulid::generate();
        let received = make_location();
        let txn_id = Ulid::generate();
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
        let stream_id = Ulid::generate();
        let txn_id = Ulid::generate();
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
        let stream_id = Ulid::generate();
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
            txn_id: Ulid::generate(),
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::Error);
        assert_eq!(effects.len(), 1);
        assert!(matches!(
            effects[0],
            Effect::Blob(BlobEffect::CloseConnection { .. })
        ));
    }

    fn missing_bucket_op() -> IncomingVersionReplicationOperation {
        let manifest = make_manifest(ReplicationItemKind::Materialized);
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::generate(),
            iroh::SecretKey::generate().public(),
            test_realm_id(),
            manifest,
        );
        op.start();
        op.step(Event::Storage(StorageEvent::ReadResult {
            key: b"bucket".to_vec().into(),
            value: None,
        }));
        op
    }

    #[test]
    fn missing_bucket_autocreates() {
        let op = missing_bucket_op();
        assert_eq!(
            op.state,
            IncomingVersionReplicationState::CreateDestinationBucket
        );
        assert!(op.create_attempted);
        let info = op.destination_bucket_info();
        assert_eq!(info.group_id, test_group_id());
        assert_eq!(info.created_by, test_user_id());
        assert!(info.cors_configuration.is_none());
    }

    #[test]
    fn autocreate_rereads_bucket() {
        let mut op = missing_bucket_op();
        let effects = op.step(Event::SubOperation(SubOperationEvent::BucketCreated {
            result: Ok(()),
        }));
        assert_eq!(
            op.state,
            IncomingVersionReplicationState::ReadDestinationBucket
        );
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { key_space, .. })]
                if key_space == S3_BUCKET_KEYSPACE
        ));
    }

    #[test]
    fn create_invalid_event() {
        let mut op = missing_bucket_op();
        op.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: Ulid::generate(),
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::Error);
    }
}
