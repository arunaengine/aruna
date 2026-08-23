use crate::blob::blob_keyspace_helper::blob_location_read;
use crate::blob::managed_copy::{
    CopyRequest, serve_reads, split_serve_reads, validate_registration,
};
use crate::connectors::resolver::ARUNA_NATIVE_RELATIONSHIP_ID;
use crate::connectors::{
    ResolveVersionSourceBindingInput, resolve_version_source_binding_suboperation,
};
use crate::driver::{DriverContext, drive};
use crate::group_backends::{RecordReadError, parse_read};
use crate::group_routing::load_group_inputs;
use crate::permission_rules::{PermissionRules, PermissionRulesConfig, PermissionRulesOperation};
use crate::placement_policy::{
    GateContext, PolicyGateError, PolicyGateOperation, gate_decision, write_gate,
};
use crate::replication::error::ReplicationError;
use crate::replication::protocol::{
    MaterializedBlobInfo, MultipartObjectReplicationMetadata, ReferenceAdvance, ReplicationMode,
    SyncOrigin, VersionReplicationManifest, VersionReplicationMessage, VersionReplicationRequest,
};
use crate::request_policy::{PolicyEvaluator, PolicyRequestExtras, policy_request_with};
use aruna_core::effects::{BlobEffect, Effect, IterStart, StagingSourceEffect, StorageEffect};
use aruna_core::errors::{AuthorizationError, BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event, StagingSourceEvent, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, S3_BUCKET_KEYSPACE,
    S3_MULTIPART_OBJECT_METADATA_KEYSPACE, SYNC_REFERENCE_STATE_KEYSPACE,
};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::{
    ArunaArn, AuthContext, BackendLocation, BlobHeadKey, BlobLocationKey, BlobVersion,
    BlobVersionState, BucketInfo, CurrentVersionPointer, GroupRoutingInputs, ManagedCopyKey,
    MultipartObjectMetadataKey, MultipartObjectPart, MultipartObjectSummary, Permission,
    PlacementPolicyRef, PortableSourceDescriptor, ReferenceHandling, ReplicationItemKind,
    ReplicationNegotiationResult, ReplicationSuboperationResult, ResolvedSourceAccess,
    RoutingError, SourceConnectorKind, SourceMetadata, StagingStrategy, SyncMode, SyncRelationship,
    VersionKey, VersionSourceBinding, blob_object_permission_path, sync_state_key,
};
use aruna_core::structs::{NodeRouting, StorageRoutingRule, resolve_backend};
use aruna_core::types::{Effects, GroupId, Key, NodeId};
use serde::{Deserialize, Serialize};
use smallvec::smallvec;
use std::collections::{BTreeSet, HashMap};
use std::time::SystemTime;
use thiserror::Error;
use tracing::debug;
use ulid::Ulid;

const ITER_PAGE_SIZE: usize = 512;
const MAX_SCOPE_VERSIONS: usize = 1024;

#[derive(Debug, Error, PartialEq)]
pub(crate) enum SourceAuthorizationError {
    #[error("source access denied")]
    Denied,
    #[error("source authorization unavailable: {0}")]
    Unavailable(String),
}

pub(crate) struct SourceAuthorization {
    group_id: GroupId,
    source_node_id: NodeId,
    auth_context: AuthContext,
    permissions: PermissionRules,
    policies: PolicyEvaluator,
}

// Compiled rules and evaluators are derived state; identity fields decide equality.
impl PartialEq for SourceAuthorization {
    fn eq(&self, other: &Self) -> bool {
        self.group_id == other.group_id
            && self.source_node_id == other.source_node_id
            && self.auth_context == other.auth_context
    }
}

impl std::fmt::Debug for SourceAuthorization {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SourceAuthorization")
            .field("group_id", &self.group_id)
            .field("source_node_id", &self.source_node_id)
            .field("auth_context", &self.auth_context)
            .finish_non_exhaustive()
    }
}

impl SourceAuthorization {
    pub(crate) async fn load(
        context: &DriverContext,
        auth_context: AuthContext,
        group_id: GroupId,
        source_node_id: NodeId,
    ) -> Result<Self, SourceAuthorizationError> {
        let permissions = drive(
            PermissionRulesOperation::new(PermissionRulesConfig {
                auth_context: auth_context.clone(),
                path: format!("/{}/g/{group_id}", auth_context.realm_id),
            }),
            context,
        )
        .await
        .map_err(permission_error)?;
        let policies = PolicyEvaluator::load(context, auth_context.realm_id, Some(group_id))
            .await
            .map_err(|error| SourceAuthorizationError::Unavailable(error.to_string()))?;
        Ok(Self {
            group_id,
            source_node_id,
            auth_context,
            permissions,
            policies,
        })
    }

    pub(crate) fn group_id(&self) -> GroupId {
        self.group_id
    }

    fn allows(&self, bucket: &str, key: &str) -> Result<(), SourceAuthorizationError> {
        let path = blob_object_permission_path(
            self.auth_context.realm_id,
            self.group_id,
            self.source_node_id,
            bucket,
            key,
        );
        if !self.permissions.allows(&path, &Permission::READ) {
            return Err(SourceAuthorizationError::Denied);
        }
        let request = policy_request_with(
            &path,
            &Permission::READ,
            Some(&self.auth_context.user_id),
            PolicyRequestExtras::operation("s3.GetObject"),
        );
        self.policies
            .evaluate(&request)
            .map_err(|_| SourceAuthorizationError::Denied)
    }
}

fn permission_error(error: AuthorizationError) -> SourceAuthorizationError {
    match error {
        AuthorizationError::AuthDocNotFound
        | AuthorizationError::GroupNotFound
        | AuthorizationError::InvalidGroupId
        | AuthorizationError::InvalidRealmId => SourceAuthorizationError::Denied,
        other => SourceAuthorizationError::Unavailable(other.to_string()),
    }
}

#[derive(Serialize)]
struct ReferenceFingerprint<'a> {
    metadata: ReferenceMetadata<'a>,
    handling: ReferenceHandling,
}

#[derive(Serialize)]
struct ReferenceMetadata<'a> {
    content_length: u64,
    content_type: &'a Option<String>,
    etag: &'a Option<String>,
    last_modified: &'a Option<SystemTime>,
    source_version: &'a Option<String>,
}

fn reference_fingerprint(
    metadata: &SourceMetadata,
    handling: ReferenceHandling,
) -> Result<[u8; 32], ConversionError> {
    let value = ReferenceFingerprint {
        metadata: ReferenceMetadata {
            content_length: metadata.content_length,
            content_type: &metadata.content_type,
            etag: &metadata.etag,
            last_modified: &metadata.last_modified,
            source_version: &metadata.source_version,
        },
        handling,
    };
    Ok(*blake3::hash(&postcard::to_allocvec(&value)?).as_bytes())
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ReplicationVersion {
    Materialized {
        created_at: SystemTime,
        created_by: aruna_core::user_id::UserId,
        location: BackendLocation,
        source: Option<VersionSourceBinding>,
        metadata: HashMap<String, String>,
    },
    Reference {
        created_at: SystemTime,
        created_by: aruna_core::user_id::UserId,
        source: VersionSourceBinding,
        cached_metadata: SourceMetadata,
        last_refresh: SystemTime,
        metadata: HashMap<String, String>,
        advance_count: u16,
    },
    Deleted {
        created_at: SystemTime,
        created_by: aruna_core::user_id::UserId,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PendingMaterializedReplicationVersion {
    created_at: SystemTime,
    created_by: aruna_core::user_id::UserId,
    blob_hash: [u8; 32],
    source: Option<VersionSourceBinding>,
    metadata: HashMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SyncTransferContext {
    target_bucket: String,
    source_prefix: Option<String>,
    target_prefix: Option<String>,
    source_node_id: NodeId,
    relationship_id: Ulid,
    reference_intent: bool,
    reference_handling: ReferenceHandling,
    origin: Option<SyncOrigin>,
    upstream_sources: Vec<ArunaArn>,
    writer_auth_context: Option<AuthContext>,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicateScopeResult {
    pub replicated: u64,
    pub replicated_bytes: u64,
    pub skipped: u64,
    pub failed: u64,
    pub last_error: Option<String>,
}

#[derive(Debug, Error, PartialEq)]
pub enum ReplicateScopeError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    ReplicationError(#[from] ReplicationError),
    #[error(transparent)]
    ReplicateObjectVersionError(#[from] ReplicateObjectVersionError),
    #[error("Source bucket not found")]
    BucketNotFound,
    #[error("replication scope exceeds {limit} versions")]
    ScopeLimit { limit: usize },
    #[error("Unexpected event in state {state}: expected {expected}, got {received:?}")]
    InvalidStateEvent {
        state: &'static str,
        expected: &'static str,
        received: Event,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ReplicateScopeState {
    Init,
    ReadBucket,
    ResolveObjectTarget,
    ReadSingleVersion,
    IterateVersions,
    RunVersionReplication,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct ReplicateScopeOperation {
    input: ReplicateScopeInput,
    state: ReplicateScopeState,
    exact_object_exists: bool,
    iteration_prefix: Option<String>,
    next_start_after: Option<Key>,
    source_group_id: Option<GroupId>,
    sync: Option<SyncTransferContext>,
    pending_versions: Vec<VersionReplicationRequest>,
    pending_keys: BTreeSet<(String, String, Ulid)>,
    examined_versions: usize,
    source_authorization: Option<SourceAuthorization>,
    writer_auth_context: Option<AuthContext>,
    reference_advance: Option<ReferenceAdvance>,
    routing: NodeRouting,
    /// Destination facts of this node, passed to every version sub-operation
    /// that may materialize reference bytes here.
    gate_context: Option<GateContext>,
    result: ReplicateScopeResult,
    output: Option<Result<ReplicateScopeResult, ReplicateScopeError>>,
}

impl ReplicateScopeOperation {
    pub fn new(input: ReplicateScopeInput) -> Self {
        Self {
            input,
            state: ReplicateScopeState::Init,
            exact_object_exists: false,
            iteration_prefix: None,
            next_start_after: None,
            source_group_id: None,
            sync: None,
            pending_versions: Vec::new(),
            pending_keys: BTreeSet::new(),
            examined_versions: 0,
            source_authorization: None,
            writer_auth_context: None,
            reference_advance: None,
            routing: NodeRouting::default(),
            gate_context: None,
            result: ReplicateScopeResult {
                replicated: 0,
                replicated_bytes: 0,
                skipped: 0,
                failed: 0,
                last_error: None,
            },
            output: None,
        }
    }

    /// Node-local routing, forwarded to every version sub-operation.
    pub fn with_routing(mut self, routing: NodeRouting) -> Self {
        self.routing = routing;
        self
    }

    /// The destination this node materializes reference bytes against. Omitting
    /// it fails every governed reference materialization closed.
    pub fn with_gate(mut self, context: GateContext) -> Self {
        self.gate_context = Some(context);
        self
    }

    pub(crate) fn with_source_authorization(mut self, authorization: SourceAuthorization) -> Self {
        self.source_authorization = Some(authorization);
        self
    }

    pub(super) fn with_writer_auth(mut self, auth_context: AuthContext) -> Self {
        self.writer_auth_context = Some(auth_context);
        self
    }

    pub fn with_reference_advance(mut self, advance: ReferenceAdvance) -> Self {
        self.reference_advance = Some(advance);
        self
    }

    pub fn with_relationship(
        mut self,
        relationship: SyncRelationship,
        origin: Option<SyncOrigin>,
        mut upstream_sources: Vec<ArunaArn>,
        writer_auth_context: Option<AuthContext>,
    ) -> Self {
        self.writer_auth_context = writer_auth_context.clone();
        if !upstream_sources
            .iter()
            .any(|source| source == &relationship.source)
        {
            upstream_sources.push(relationship.source.clone());
        }
        self.sync = Some(SyncTransferContext {
            target_bucket: relationship.target.bucket().unwrap_or_default().to_string(),
            source_prefix: relationship.source.key_prefix().map(ToOwned::to_owned),
            target_prefix: relationship.target.key_prefix().map(ToOwned::to_owned),
            source_node_id: relationship.source.node_id,
            relationship_id: relationship.id,
            reference_intent: relationship.mode == SyncMode::Reference,
            reference_handling: relationship.reference_handling,
            origin: Some(origin.unwrap_or(SyncOrigin {
                relationship_id: relationship.id,
                hop_count: 0,
            })),
            upstream_sources,
            writer_auth_context,
        });
        self
    }

    /// Maps one exact source version to an unrelated node-local destination.
    pub(crate) fn with_destination(
        mut self,
        target_bucket: String,
        target_key: String,
        source_node_id: NodeId,
        writer_auth_context: AuthContext,
    ) -> Self {
        let (source_prefix, relationship_id) = match &self.input.target {
            ReplicateScopeTarget::Version { key, version_id } => (Some(key.clone()), *version_id),
            _ => (None, Ulid::nil()),
        };
        self.writer_auth_context = Some(writer_auth_context.clone());
        self.sync = Some(SyncTransferContext {
            target_bucket,
            source_prefix,
            target_prefix: Some(target_key),
            source_node_id,
            relationship_id,
            reference_intent: false,
            reference_handling: ReferenceHandling::Materialize,
            origin: None,
            upstream_sources: Vec::new(),
            writer_auth_context: Some(writer_auth_context),
        });
        self
    }

    fn state_name(&self) -> &'static str {
        match self.state {
            ReplicateScopeState::Init => "Init",
            ReplicateScopeState::ReadBucket => "ReadBucket",
            ReplicateScopeState::ResolveObjectTarget => "ResolveObjectTarget",
            ReplicateScopeState::ReadSingleVersion => "ReadSingleVersion",
            ReplicateScopeState::IterateVersions => "IterateVersions",
            ReplicateScopeState::RunVersionReplication => "RunVersionReplication",
            ReplicateScopeState::Finish => "Finish",
            ReplicateScopeState::Error => "Error",
        }
    }

    fn fail(&mut self, err: ReplicateScopeError) -> Effects {
        debug!(
            bucket = %self.input.bucket,
            target = ?self.input.target,
            target_node = %self.input.target_node_id,
            state = %self.state_name(),
            error = %err,
            "Scope replication failed"
        );
        self.state = ReplicateScopeState::Error;
        self.output = Some(Err(err));
        smallvec![]
    }

    fn read_bucket(&mut self) -> Effects {
        debug!(
            bucket = %self.input.bucket,
            target = ?self.input.target,
            target_node = %self.input.target_node_id,
            mode = ?self.input.mode,
            "Entering ReadBucket state"
        );
        self.state = ReplicateScopeState::ReadBucket;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: self.input.bucket.as_bytes().to_vec().into(),
            txn_id: None,
        })]
    }

    fn resolve_target(&mut self) -> Effects {
        match self.input.target.clone() {
            ReplicateScopeTarget::Bucket => self.start_iteration(None, false),
            ReplicateScopeTarget::Prefix(prefix) => self.start_iteration(Some(prefix), false),
            ReplicateScopeTarget::Object { key } => {
                self.state = ReplicateScopeState::ResolveObjectTarget;
                let lookup = match BlobHeadKey::new(&self.input.bucket, &key).to_bytes() {
                    Ok(lookup) => lookup,
                    Err(err) => return self.fail(err.into()),
                };
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: BLOB_HEAD_KEYSPACE.to_string(),
                    key: lookup.into(),
                    txn_id: None,
                })]
            }
            ReplicateScopeTarget::Version { key, version_id } => {
                self.read_single_version(&key, version_id)
            }
        }
    }

    fn start_iteration(
        &mut self,
        prefix_filter: Option<String>,
        exact_object_exists: bool,
    ) -> Effects {
        debug!(
            bucket = %self.input.bucket,
            target = ?self.input.target,
            target_node = %self.input.target_node_id,
            prefix_filter = ?prefix_filter,
            exact_object_exists,
            mode = ?self.input.mode,
            "Starting scope iteration"
        );
        self.state = ReplicateScopeState::IterateVersions;
        self.iteration_prefix = prefix_filter;
        self.exact_object_exists = exact_object_exists;
        self.next_start_after = None;
        self.request_iteration_page()
    }

    fn request_iteration_page(&mut self) -> Effects {
        let prefix = match VersionKey::bucket_prefix(&self.input.bucket) {
            Ok(prefix) => prefix,
            Err(err) => return self.fail(err.into()),
        };
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            prefix: Some(prefix.into()),
            start: self.next_start_after.clone().map(IterStart::After),
            limit: ITER_PAGE_SIZE,
            txn_id: None,
        })]
    }

    fn read_single_version(&mut self, key: &str, version_id: Ulid) -> Effects {
        self.state = ReplicateScopeState::ReadSingleVersion;
        let version_key = match VersionKey::new(&self.input.bucket, key, version_id).to_bytes() {
            Ok(key) => key,
            Err(err) => return self.fail(err.into()),
        };
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: version_key.into(),
            txn_id: None,
        })]
    }

    fn enqueue_version_request(
        &mut self,
        version_key: VersionKey,
    ) -> Result<(), ReplicateScopeError> {
        if self.sync.as_ref().is_some_and(|sync| {
            map_sync_key(
                &version_key.key,
                sync.source_prefix.as_deref(),
                sync.target_prefix.as_deref(),
            )
            .is_none()
        }) {
            return Ok(());
        }
        let identity = (
            version_key.bucket.clone(),
            version_key.key.clone(),
            version_key.version_id,
        );
        if self.pending_keys.contains(&identity) {
            debug!(
                bucket = %version_key.bucket,
                key = %version_key.key,
                version_id = %version_key.version_id,
                target_node = %self.input.target_node_id,
                "Skipping duplicate replication queue entry"
            );
            return Ok(());
        }

        if let Some(authorization) = self.source_authorization.as_ref()
            && let Err(error) = authorization.allows(&version_key.bucket, &version_key.key)
        {
            self.result.failed = self.result.failed.saturating_add(1);
            self.result.last_error = Some(error.to_string());
            return Ok(());
        }

        if self.pending_versions.len() >= MAX_SCOPE_VERSIONS {
            return Err(ReplicateScopeError::ScopeLimit {
                limit: MAX_SCOPE_VERSIONS,
            });
        }

        self.pending_keys.insert(identity);
        debug!(
            bucket = %version_key.bucket,
            key = %version_key.key,
            version_id = %version_key.version_id,
            target_node = %self.input.target_node_id,
            mode = ?self.input.mode,
            "Enqueuing version for replication"
        );
        self.pending_versions.push(VersionReplicationRequest {
            bucket: version_key.bucket,
            key: version_key.key,
            version_id: version_key.version_id,
            source_group_id: self.source_group_id.unwrap_or_default(),
            target_node_id: self.input.target_node_id,
            auth_context: self.input.auth_context.clone(),
            mode: self.input.mode,
        });
        Ok(())
    }

    fn should_enqueue_version(&self, is_deleted: bool) -> bool {
        self.input.replicate_delete_markers || !is_deleted
    }

    fn run_next_replication(&mut self) -> Effects {
        if self.reference_advance.is_some() && self.pending_versions.len() > 1 {
            return self.fail(ReplicateObjectVersionError::InvalidReferenceAdvance.into());
        }
        let Some(request) = self.pending_versions.pop() else {
            debug!(
                bucket = %self.input.bucket,
                target = ?self.input.target,
                target_node = %self.input.target_node_id,
                replicated = self.result.replicated,
                replicated_bytes = self.result.replicated_bytes,
                skipped = self.result.skipped,
                failed = self.result.failed,
                "Scope replication finished"
            );
            self.state = ReplicateScopeState::Finish;
            self.output = Some(Ok(self.result.clone()));
            return smallvec![];
        };

        debug!(
            bucket = %request.bucket,
            key = %request.key,
            version_id = %request.version_id,
            target_node = %request.target_node_id,
            remaining_pending = self.pending_versions.len(),
            mode = ?request.mode,
            "Starting version replication suboperation"
        );
        self.state = ReplicateScopeState::RunVersionReplication;
        let writer_auth_context = self.writer_auth_context.clone();
        let operation = match self.sync.clone() {
            Some(mut sync) => {
                let Some(target_key) = map_sync_key(
                    &request.key,
                    sync.source_prefix.as_deref(),
                    sync.target_prefix.as_deref(),
                ) else {
                    self.result.failed = self.result.failed.saturating_add(1);
                    return self.run_next_replication();
                };
                sync.target_prefix = Some(target_key);
                ReplicateObjectVersionOperation::new(request)
                    .with_routing(self.routing.clone())
                    .with_sync(sync)
            }
            None => {
                ReplicateObjectVersionOperation::new(request).with_routing(self.routing.clone())
            }
        };
        let operation = match self.gate_context.clone() {
            Some(context) => operation.with_gate(context),
            None => operation,
        };
        let operation = match writer_auth_context {
            Some(auth_context) => operation.with_writer_auth(auth_context),
            None => operation,
        };
        let operation = match self.reference_advance.take() {
            Some(advance) => operation.with_reference_advance(advance),
            None => operation,
        };
        smallvec![Effect::SubOperation(boxed_suboperation(
            operation,
            |result| Event::SubOperation(SubOperationEvent::ReplicationItemResult {
                result: result
                    .map_err(|err| err.to_string())
                    .and_then(|inner| inner.map_err(|err| err.to_string())),
            }),
        ))]
    }
}

pub(crate) fn map_sync_key(
    source_key: &str,
    source_prefix: Option<&str>,
    target_prefix: Option<&str>,
) -> Option<String> {
    let suffix = match source_prefix {
        Some(prefix) => source_key.strip_prefix(prefix)?,
        None => source_key,
    };
    Some(match target_prefix {
        Some(prefix) if prefix.ends_with('/') && suffix.starts_with('/') => {
            format!("{}{}", prefix, &suffix[1..])
        }
        Some(prefix)
            if !prefix.ends_with('/') && !suffix.is_empty() && !suffix.starts_with('/') =>
        {
            format!("{prefix}/{suffix}")
        }
        Some(prefix) => format!("{prefix}{suffix}"),
        None => suffix.trim_start_matches('/').to_string(),
    })
}

impl Operation for ReplicateScopeOperation {
    type Output = Option<Result<ReplicateScopeResult, ReplicateScopeError>>;
    type Error = ReplicateScopeError;

    fn start(&mut self) -> Effects {
        self.read_bucket()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            ReplicateScopeState::Init => self.read_bucket(),
            ReplicateScopeState::ReadBucket => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.fail(ReplicateScopeError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::ReadResult)",
                        received: event,
                    });
                };
                let Some(value) = value else {
                    return self.fail(ReplicateScopeError::BucketNotFound);
                };
                let bucket_info = match BucketInfo::from_bytes(value.as_ref()) {
                    Ok(bucket_info) => bucket_info,
                    Err(err) => return self.fail(err.into()),
                };
                debug!(
                    bucket = %self.input.bucket,
                    target = ?self.input.target,
                    target_node = %self.input.target_node_id,
                    group_id = %bucket_info.group_id,
                    mode = ?self.input.mode,
                    "Loaded source bucket for replication"
                );
                self.source_group_id = Some(bucket_info.group_id);
                self.resolve_target()
            }
            ReplicateScopeState::ResolveObjectTarget => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = &event else {
                    return self.fail(ReplicateScopeError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::ReadResult)",
                        received: event,
                    });
                };
                let ReplicateScopeTarget::Object { key } = &self.input.target else {
                    return self.fail(ReplicateScopeError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "ReplicateScopeTarget::Object",
                        received: event,
                    });
                };
                debug!(
                    bucket = %self.input.bucket,
                    key = %key,
                    target_node = %self.input.target_node_id,
                    current_version_exists = value.is_some(),
                    "Resolved exact object replication target"
                );
                self.start_iteration(Some(key.clone()), true)
            }
            ReplicateScopeState::ReadSingleVersion => {
                let Event::Storage(StorageEvent::ReadResult { key, value }) = event else {
                    return self.fail(ReplicateScopeError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::ReadResult)",
                        received: event,
                    });
                };

                if let Some(value) = value {
                    let version_key = match VersionKey::from_bytes(key.as_ref()) {
                        Ok(version_key) => version_key,
                        Err(err) => return self.fail(err.into()),
                    };
                    let version = match BlobVersion::from_bytes(value.as_ref()) {
                        Ok(version) => version,
                        Err(err) => return self.fail(err.into()),
                    };
                    debug!(
                        bucket = %version_key.bucket,
                        key = %version_key.key,
                        version_id = %version_key.version_id,
                        target_node = %self.input.target_node_id,
                        is_materialized = version.is_materialized(),
                        is_deleted = version.is_deleted(),
                        has_source_binding = version.source_binding().is_some(),
                        "Loaded single version for replication"
                    );
                    if self.should_enqueue_version(version.is_deleted()) {
                        if let Err(error) = self.enqueue_version_request(version_key) {
                            return self.fail(error);
                        }
                    } else {
                        debug!(
                            bucket = %version_key.bucket,
                            key = %version_key.key,
                            version_id = %version_key.version_id,
                            target_node = %self.input.target_node_id,
                            "Filtered single version from replication"
                        );
                    }
                }

                self.run_next_replication()
            }
            ReplicateScopeState::IterateVersions => {
                let Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }) = event
                else {
                    return self.fail(ReplicateScopeError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::IterResult)",
                        received: event,
                    });
                };

                let page_len = values.len();
                let examined = self.examined_versions.saturating_add(page_len);
                if examined > MAX_SCOPE_VERSIONS {
                    return self.fail(ReplicateScopeError::ScopeLimit {
                        limit: MAX_SCOPE_VERSIONS,
                    });
                }
                self.examined_versions = examined;
                let pending_before = self.pending_versions.len();

                for (key, value) in values {
                    let Ok(version_key) = VersionKey::from_bytes(key.as_ref()) else {
                        continue;
                    };
                    let Ok(version) = BlobVersion::from_bytes(value.as_ref()) else {
                        continue;
                    };
                    if version_key.bucket != self.input.bucket {
                        continue;
                    }

                    if let Some(prefix) = self.iteration_prefix.as_ref() {
                        let matches = if self.exact_object_exists {
                            version_key.key == *prefix
                        } else {
                            version_key.key.starts_with(prefix)
                        };
                        if !matches {
                            continue;
                        }
                    }

                    if !self.should_enqueue_version(version.is_deleted()) {
                        continue;
                    }

                    if let Err(error) = self.enqueue_version_request(version_key) {
                        return self.fail(error);
                    }
                }

                debug!(
                    bucket = %self.input.bucket,
                    target = ?self.input.target,
                    target_node = %self.input.target_node_id,
                    page_len,
                    enqueued_in_page = self.pending_versions.len().saturating_sub(pending_before),
                    next_page = next_start_after.is_some(),
                    "Processed replication iteration page"
                );

                if let Some(cursor) = next_start_after {
                    self.next_start_after = Some(cursor);
                    self.request_iteration_page()
                } else {
                    self.run_next_replication()
                }
            }
            ReplicateScopeState::RunVersionReplication => {
                let Event::SubOperation(SubOperationEvent::ReplicationItemResult { result }) =
                    event
                else {
                    return self.fail(ReplicateScopeError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::SubOperation(SubOperationEvent::ReplicationItemResult)",
                        received: event,
                    });
                };

                match &result {
                    Ok(ReplicationSuboperationResult::Replicated) => self.result.replicated += 1,
                    Ok(ReplicationSuboperationResult::Skipped) => self.result.skipped += 1,
                    Ok(ReplicationSuboperationResult::ReplicatedBytes(bytes)) => {
                        self.result.replicated = self.result.replicated.saturating_add(1);
                        self.result.replicated_bytes =
                            self.result.replicated_bytes.saturating_add(*bytes);
                    }
                    Err(error) => {
                        self.result.failed += 1;
                        self.result.last_error = Some(error.clone());
                    }
                }

                debug!(
                    bucket = %self.input.bucket,
                    target = ?self.input.target,
                    target_node = %self.input.target_node_id,
                    result = ?result,
                    replicated = self.result.replicated,
                    replicated_bytes = self.result.replicated_bytes,
                    skipped = self.result.skipped,
                    failed = self.result.failed,
                    "Completed version replication suboperation"
                );

                self.run_next_replication()
            }
            ReplicateScopeState::Finish => smallvec![],
            ReplicateScopeState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            ReplicateScopeState::Finish | ReplicateScopeState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == ReplicateScopeState::Error
            && let Some(Err(err)) = self.output
        {
            return Err(err);
        }
        Ok(self.output)
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum ReplicateObjectVersionError {
    #[error(transparent)]
    ManagedCopy(#[from] crate::blob::managed_copy::ManagedCopyError),
    #[error(transparent)]
    RoutingFailed(#[from] RoutingError),
    #[error("could not load the group's routing inputs: {0}")]
    RoutingInputsFailed(String),
    #[error("could not read the bucket's routing rules: {0}")]
    BucketRulesFailed(#[from] RecordReadError),
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    ReplicationError(#[from] ReplicationError),
    #[error(transparent)]
    PolicyGateError(#[from] PolicyGateError),
    #[error("Version not found")]
    VersionNotFound,
    #[error("Reference version must be materialized before manifest creation")]
    UnresolvedReferenceVersion,
    #[error("Reference advance requires a preserved reference version")]
    InvalidReferenceAdvance,
    #[error("Missing blob hash")]
    MissingBlobHash,
    #[error("Multipart metadata incomplete: expected {expected} parts, found {actual}")]
    MultipartPartCountMismatch { expected: usize, actual: usize },
    #[error("Unexpected event in state {state}: expected {expected}, got {received:?}")]
    InvalidStateEvent {
        state: &'static str,
        expected: &'static str,
        received: Event,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ReplicateObjectVersionState {
    Init,
    ReadVersion,
    ReadBlobLocation,
    CheckManagedCopy,
    ResolveReferenceAccess,
    HeadReferenceSource,
    ReadReferenceState,
    LoadRouting,
    ReadBucketRules,
    ReadReferenceSource,
    ReferencePolicyGate,
    WriteReferenceBlob,
    CleanupReferenceBlob,
    ReadMultipartSummary,
    ReadMultipartParts,
    ReadCurrentLookup,
    OpenConnection,
    SendManifest,
    AwaitNegotiation,
    TransferBlob,
    AwaitApplyComplete,
    WriteReferenceState,
    CloseConnection,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct ReplicateObjectVersionOperation {
    request: VersionReplicationRequest,
    state: ReplicateObjectVersionState,
    pending_materialized_version: Option<PendingMaterializedReplicationVersion>,
    replication_version: Option<ReplicationVersion>,
    multipart_summary: Option<MultipartObjectSummary>,
    multipart_parts: Vec<MultipartObjectPart>,
    multipart_parts_next_start_after: Option<Key>,
    stream_id: Option<Ulid>,
    manifest: Option<VersionReplicationManifest>,
    blob_replication_id: Option<Ulid>,
    cleanup_reference_blob: Option<BackendLocation>,
    preserve_reference: bool,
    reference_access: Option<ResolvedSourceAccess>,
    reference_metadata: Option<SourceMetadata>,
    group_inputs: GroupRoutingInputs,
    bucket_rules: Vec<StorageRoutingRule>,
    sync: Option<SyncTransferContext>,
    writer_auth_context: Option<AuthContext>,
    reference_advance: Option<ReferenceAdvance>,
    /// Refs read from the stored version, carried onto the manifest unchanged.
    version_policies: Vec<PlacementPolicyRef>,
    pending_copy: Option<ManagedCopyKey>,
    /// Destination facts of this node, evaluated before reference bytes are
    /// materialized locally.
    gate_context: Option<GateContext>,
    gate: Option<PolicyGateOperation>,
    routing: NodeRouting,
    result: Result<ReplicationSuboperationResult, ReplicateObjectVersionError>,
}

impl ReplicateObjectVersionOperation {
    pub fn new(request: VersionReplicationRequest) -> Self {
        Self {
            request,
            state: ReplicateObjectVersionState::Init,
            pending_materialized_version: None,
            replication_version: None,
            multipart_summary: None,
            multipart_parts: Vec::new(),
            multipart_parts_next_start_after: None,
            stream_id: None,
            manifest: None,
            blob_replication_id: None,
            cleanup_reference_blob: None,
            preserve_reference: false,
            reference_access: None,
            reference_metadata: None,
            group_inputs: GroupRoutingInputs::default(),
            bucket_rules: Vec::new(),
            sync: None,
            writer_auth_context: None,
            reference_advance: None,
            version_policies: Vec::new(),
            pending_copy: None,
            gate_context: None,
            gate: None,
            routing: NodeRouting::default(),
            result: Ok(ReplicationSuboperationResult::Replicated),
        }
    }

    pub fn with_routing(mut self, routing: NodeRouting) -> Self {
        self.routing = routing;
        self
    }

    pub fn with_gate(mut self, context: GateContext) -> Self {
        self.gate_context = Some(context);
        self
    }

    fn with_sync(mut self, sync: SyncTransferContext) -> Self {
        self.sync = Some(sync);
        self
    }

    fn with_writer_auth(mut self, auth_context: AuthContext) -> Self {
        self.writer_auth_context = Some(auth_context);
        self
    }

    fn with_reference_advance(mut self, advance: ReferenceAdvance) -> Self {
        self.reference_advance = Some(advance);
        self
    }

    fn state_name(&self) -> &'static str {
        match self.state {
            ReplicateObjectVersionState::Init => "Init",
            ReplicateObjectVersionState::ReadVersion => "ReadVersion",
            ReplicateObjectVersionState::ReadBlobLocation => "ReadBlobLocation",
            ReplicateObjectVersionState::CheckManagedCopy => "CheckManagedCopy",
            ReplicateObjectVersionState::ResolveReferenceAccess => "ResolveReferenceAccess",
            ReplicateObjectVersionState::HeadReferenceSource => "HeadReferenceSource",
            ReplicateObjectVersionState::ReadReferenceState => "ReadReferenceState",
            ReplicateObjectVersionState::LoadRouting => "LoadRouting",
            ReplicateObjectVersionState::ReadBucketRules => "ReadBucketRules",
            ReplicateObjectVersionState::ReadReferenceSource => "ReadReferenceSource",
            ReplicateObjectVersionState::ReferencePolicyGate => "ReferencePolicyGate",
            ReplicateObjectVersionState::WriteReferenceBlob => "WriteReferenceBlob",
            ReplicateObjectVersionState::CleanupReferenceBlob => "CleanupReferenceBlob",
            ReplicateObjectVersionState::ReadMultipartSummary => "ReadMultipartSummary",
            ReplicateObjectVersionState::ReadMultipartParts => "ReadMultipartParts",
            ReplicateObjectVersionState::ReadCurrentLookup => "ReadCurrentLookup",
            ReplicateObjectVersionState::OpenConnection => "OpenConnection",
            ReplicateObjectVersionState::SendManifest => "SendManifest",
            ReplicateObjectVersionState::AwaitNegotiation => "AwaitNegotiation",
            ReplicateObjectVersionState::TransferBlob => "TransferBlob",
            ReplicateObjectVersionState::AwaitApplyComplete => "AwaitApplyComplete",
            ReplicateObjectVersionState::WriteReferenceState => "WriteReferenceState",
            ReplicateObjectVersionState::CloseConnection => "CloseConnection",
            ReplicateObjectVersionState::Finish => "Finish",
            ReplicateObjectVersionState::Error => "Error",
        }
    }

    fn fail(&mut self, err: ReplicateObjectVersionError) -> Effects {
        debug!(
            bucket = %self.request.bucket,
            key = %self.request.key,
            version_id = %self.request.version_id,
            target_node = %self.request.target_node_id,
            state = %self.state_name(),
            error = %err,
            "Version replication failed"
        );
        self.state = ReplicateObjectVersionState::Error;
        self.result = Err(err);
        self.abort()
    }

    fn read_version(&mut self) -> Effects {
        self.state = ReplicateObjectVersionState::ReadVersion;
        let key = match VersionKey::new(
            &self.request.bucket,
            &self.request.key,
            self.request.version_id,
        )
        .to_bytes()
        {
            Ok(key) => key,
            Err(err) => return self.fail(err.into()),
        };

        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: key.into(),
            txn_id: None,
        })]
    }

    fn read_blob_location(&mut self, key: BlobLocationKey) -> Effects {
        self.state = ReplicateObjectVersionState::ReadBlobLocation;
        smallvec![blob_location_read(&key, None)]
    }

    fn read_multipart_summary(&mut self) -> Effects {
        self.state = ReplicateObjectVersionState::ReadMultipartSummary;
        let key = match MultipartObjectMetadataKey::summary(self.request.version_id).to_bytes() {
            Ok(key) => key,
            Err(err) => return self.fail(err.into()),
        };

        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_MULTIPART_OBJECT_METADATA_KEYSPACE.to_string(),
            key: key.into(),
            txn_id: None,
        })]
    }

    fn read_multipart_parts(&mut self) -> Effects {
        self.state = ReplicateObjectVersionState::ReadMultipartParts;
        let prefix = match MultipartObjectMetadataKey::part_prefix(self.request.version_id) {
            Ok(prefix) => prefix,
            Err(err) => return self.fail(err.into()),
        };

        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: S3_MULTIPART_OBJECT_METADATA_KEYSPACE.to_string(),
            prefix: Some(prefix.into()),
            start: self
                .multipart_parts_next_start_after
                .clone()
                .map(IterStart::After),
            limit: ITER_PAGE_SIZE,
            txn_id: None,
        })]
    }

    fn validate_multipart_parts_complete(&self) -> Result<(), ReplicateObjectVersionError> {
        let Some(summary) = self.multipart_summary.as_ref() else {
            return Ok(());
        };

        let actual = self.multipart_parts.len();
        if actual != summary.part_count {
            return Err(ReplicateObjectVersionError::MultipartPartCountMismatch {
                expected: summary.part_count,
                actual,
            });
        }

        Ok(())
    }

    fn read_current_lookup(&mut self) -> Effects {
        self.state = ReplicateObjectVersionState::ReadCurrentLookup;
        let key = match BlobHeadKey::new(&self.request.bucket, &self.request.key).to_bytes() {
            Ok(key) => key,
            Err(err) => return self.fail(err.into()),
        };

        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BLOB_HEAD_KEYSPACE.to_string(),
            key: key.into(),
            txn_id: None,
        })]
    }

    fn skip_version(&mut self) -> Effects {
        debug!(
            bucket = %self.request.bucket,
            key = %self.request.key,
            version_id = %self.request.version_id,
            target_node = %self.request.target_node_id,
            state = %self.state_name(),
            mode = ?self.request.mode,
            "Skipping version replication"
        );
        self.result = Ok(ReplicationSuboperationResult::Skipped);
        self.state = ReplicateObjectVersionState::Finish;
        smallvec![]
    }

    fn reference_matches(&self, metadata: &SourceMetadata) -> Option<bool> {
        let ReplicationVersion::Reference {
            cached_metadata, ..
        } = self.replication_version.as_ref()?
        else {
            return None;
        };
        Some(cached_metadata.observation_fingerprint() == metadata.observation_fingerprint())
    }

    fn resolve_reference_or_skip(&mut self, version: ReplicationVersion) -> Effects {
        if self.sync.is_none() && self.request.mode != ReplicationMode::OnDemand {
            return self.skip_version();
        }
        let top_level_reference = self.sync.as_ref().is_some_and(|sync| sync.reference_intent);
        let handling = self
            .sync
            .as_ref()
            .map(|sync| sync.reference_handling)
            .unwrap_or(ReferenceHandling::Materialize);
        if !top_level_reference && handling == ReferenceHandling::Skip {
            return self.skip_version();
        }
        self.preserve_reference = top_level_reference || handling == ReferenceHandling::Preserve;

        let ReplicationVersion::Reference { source, .. } = &version else {
            return self.fail(ReplicateObjectVersionError::VersionNotFound);
        };
        if self.preserve_reference {
            self.replication_version = Some(version);
            return self.read_current_lookup();
        }
        let source = source.clone();

        debug!(
            bucket = %self.request.bucket,
            key = %self.request.key,
            version_id = %self.request.version_id,
            target_node = %self.request.target_node_id,
            strategy = ?source.strategy,
            source_path = %source.descriptor.source_path,
            source_kind = %source.descriptor.kind,
            "Resolving on-demand reference source access"
        );

        self.replication_version = Some(version);
        self.state = ReplicateObjectVersionState::ResolveReferenceAccess;
        smallvec![resolve_version_source_binding_suboperation(
            ResolveVersionSourceBindingInput { source },
        )]
    }

    fn handle_reference_access_resolved(&mut self, event: Event) -> Effects {
        match event {
            Event::SubOperation(SubOperationEvent::VersionSourceAccessResolved {
                result: Ok(access),
            }) => {
                let (source_kind, source_path, source_version) = match &access {
                    aruna_core::structs::ResolvedSourceAccess::OpenDal {
                        kind,
                        path,
                        version,
                        ..
                    } => (*kind, path.clone(), version.clone()),
                };
                debug!(
                    bucket = %self.request.bucket,
                    key = %self.request.key,
                    version_id = %self.request.version_id,
                    target_node = %self.request.target_node_id,
                    source_kind = %source_kind,
                    source_path = %source_path,
                    source_version = ?source_version,
                    "Resolved on-demand reference access"
                );
                if self.sync.is_none() {
                    return self.load_routing(access);
                }
                self.reference_access = Some(access.clone());
                self.state = ReplicateObjectVersionState::HeadReferenceSource;
                smallvec![Effect::StagingSource(StagingSourceEffect::Head { access })]
            }
            Event::SubOperation(SubOperationEvent::VersionSourceAccessResolved {
                result: Err(_),
            }) => {
                debug!(
                    bucket = %self.request.bucket,
                    key = %self.request.key,
                    version_id = %self.request.version_id,
                    target_node = %self.request.target_node_id,
                    "Failed to resolve on-demand reference access; skipping version"
                );
                if self.sync.is_some() {
                    self.fail(ReplicationError::ReplicationFailed.into())
                } else {
                    self.skip_version()
                }
            }
            other => self.fail(ReplicateObjectVersionError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::SubOperation(SubOperationEvent::VersionSourceAccessResolved)",
                received: other,
            }),
        }
    }

    fn reference_state_key(&self) -> Result<Vec<u8>, ReplicateObjectVersionError> {
        let relationship_id = self
            .sync
            .as_ref()
            .map(|sync| sync.relationship_id)
            .ok_or(ReplicateObjectVersionError::UnresolvedReferenceVersion)?;
        Ok(sync_state_key(
            relationship_id,
            &self.request.bucket,
            &self.request.key,
            self.request.version_id,
        )?)
    }

    fn handle_reference_head(&mut self, event: Event) -> Effects {
        match event {
            Event::StagingSource(StagingSourceEvent::HeadResult { metadata }) => {
                match self.reference_matches(&metadata) {
                    Some(true) => {}
                    Some(false) => return self.skip_version(),
                    None => return self.fail(ReplicateObjectVersionError::VersionNotFound),
                }
                self.reference_metadata = Some(metadata);
                let key = match self.reference_state_key() {
                    Ok(key) => key,
                    Err(error) => return self.fail(error),
                };
                self.state = ReplicateObjectVersionState::ReadReferenceState;
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: SYNC_REFERENCE_STATE_KEYSPACE.to_string(),
                    key: key.into(),
                    txn_id: None,
                })]
            }
            Event::StagingSource(StagingSourceEvent::Error { .. }) => {
                self.fail(ReplicationError::ReplicationFailed.into())
            }
            other => self.fail(ReplicateObjectVersionError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::StagingSource(StagingSourceEvent::HeadResult)",
                received: other,
            }),
        }
    }

    fn handle_reference_state(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.fail(ReplicateObjectVersionError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received: event,
            });
        };
        if self.reference_advance.is_some() {
            return self.read_current_lookup();
        }
        let current = self.reference_metadata.clone();
        let handling = if self.preserve_reference {
            ReferenceHandling::Preserve
        } else {
            ReferenceHandling::Materialize
        };
        let fingerprint = match current
            .as_ref()
            .map(|metadata| reference_fingerprint(metadata, handling))
            .transpose()
        {
            Ok(fingerprint) => fingerprint,
            Err(error) => return self.fail(error.into()),
        };
        let unchanged = value
            .as_ref()
            .zip(fingerprint.as_ref())
            .is_some_and(|(value, fingerprint)| value.as_ref() == fingerprint);
        if unchanged {
            return self.skip_version();
        }
        if self.preserve_reference {
            return self.read_current_lookup();
        }
        let Some(access) = self.reference_access.take() else {
            return self.fail(ReplicateObjectVersionError::UnresolvedReferenceVersion);
        };
        self.load_routing(access)
    }

    /// This node materializes the reference locally, so it routes with its own
    /// snapshot: the group default and bucket rules load before the read.
    fn load_routing(&mut self, access: ResolvedSourceAccess) -> Effects {
        self.reference_access = Some(access);
        self.state = ReplicateObjectVersionState::LoadRouting;
        smallvec![load_group_inputs(self.request.source_group_id)]
    }

    fn read_bucket_rules(&mut self) -> Effects {
        self.state = ReplicateObjectVersionState::ReadBucketRules;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: self.request.bucket.as_bytes().to_vec().into(),
            txn_id: None,
        })]
    }

    fn handle_routing_loaded(&mut self, event: Event) -> Effects {
        let Event::SubOperation(SubOperationEvent::GroupRoutingLoaded { result }) = event else {
            return self.fail(ReplicateObjectVersionError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::SubOperation(SubOperationEvent::GroupRoutingLoaded)",
                received: event,
            });
        };
        match result {
            Ok(inputs) => self.group_inputs = inputs,
            Err(error) => {
                return self.fail(ReplicateObjectVersionError::RoutingInputsFailed(error));
            }
        }
        self.read_bucket_rules()
    }

    /// A bucket without a record simply has no rules; an unreadable or
    /// undecodable one fails the write instead of rerouting it, matching the
    /// snapshot the local write surface assembles.
    fn handle_bucket_rules(&mut self, event: Event) -> Effects {
        if !matches!(
            event,
            Event::Storage(StorageEvent::ReadResult { .. } | StorageEvent::Error { .. })
        ) {
            return self.fail(ReplicateObjectVersionError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received: event,
            });
        }
        match parse_read(event, BucketInfo::from_bytes) {
            Ok(record) => {
                self.bucket_rules = record.map(|info| info.storage_routing).unwrap_or_default();
                self.read_reference_source()
            }
            Err(error) => self.fail(error.into()),
        }
    }

    /// A reference materializes real bytes on this node, so it passes the same
    /// destination gate an ordinary write does, before the source is read.
    fn read_reference_source(&mut self) -> Effects {
        match write_gate(self.gate_context.as_ref(), &self.version_policies) {
            Ok(None) => self.open_reference_source(),
            Ok(Some(mut gate)) => {
                let effects = gate.start();
                let complete = gate.is_complete();
                self.gate = Some(gate);
                self.state = ReplicateObjectVersionState::ReferencePolicyGate;
                match complete {
                    true => self.finish_reference_gate(),
                    false => effects,
                }
            }
            Err(error) => self.fail(error.into()),
        }
    }

    fn handle_reference_gate(&mut self, event: Event) -> Effects {
        let Some(gate) = self.gate.as_mut() else {
            return self.fail(ReplicateObjectVersionError::UnresolvedReferenceVersion);
        };
        let effects = gate.step(event);
        match gate.is_complete() {
            true => self.finish_reference_gate(),
            false => effects,
        }
    }

    fn finish_reference_gate(&mut self) -> Effects {
        let Some(gate) = self.gate.take() else {
            return self.fail(ReplicateObjectVersionError::UnresolvedReferenceVersion);
        };
        let decision = gate
            .finalize()
            .map_err(PolicyGateError::from)
            .and_then(|outcome| gate_decision(outcome.decision));
        match decision {
            Ok(()) => self.open_reference_source(),
            Err(error) => self.fail(error.into()),
        }
    }

    fn open_reference_source(&mut self) -> Effects {
        let Some(access) = self.reference_access.take() else {
            return self.fail(ReplicateObjectVersionError::UnresolvedReferenceVersion);
        };
        self.state = ReplicateObjectVersionState::ReadReferenceSource;
        smallvec![Effect::StagingSource(StagingSourceEffect::Read {
            access,
            range: None,
        })]
    }

    fn handle_reference_source_read(&mut self, event: Event) -> Effects {
        match event {
            Event::StagingSource(StagingSourceEvent::ReadResult {
                metadata: source_metadata,
                stream,
            }) => {
                match self.reference_matches(&source_metadata) {
                    Some(true) => {}
                    Some(false) => return self.skip_version(),
                    None => return self.fail(ReplicateObjectVersionError::VersionNotFound),
                }
                let Some(ReplicationVersion::Reference { created_by, .. }) =
                    self.replication_version.as_ref()
                else {
                    return self.fail(ReplicateObjectVersionError::VersionNotFound);
                };

                debug!(
                    bucket = %self.request.bucket,
                    key = %self.request.key,
                    version_id = %self.request.version_id,
                    target_node = %self.request.target_node_id,
                    content_length = source_metadata.content_length,
                    content_type = ?source_metadata.content_type,
                    source_version = ?source_metadata.source_version,
                    "Read on-demand reference source content"
                );

                let created_by = *created_by;
                // Routed with this node's own snapshot, never the peer's, but
                // with the full specificity ladder the local write would use.
                let snapshot = self
                    .routing
                    .snapshot(self.request.source_group_id)
                    .with_group_inputs(self.group_inputs.clone())
                    .with_bucket_rules(self.bucket_rules.clone());
                let resolved =
                    match resolve_backend(&snapshot, &self.request.bucket, &self.request.key) {
                        Ok(resolved) => resolved,
                        Err(error) => {
                            return self.fail(ReplicateObjectVersionError::RoutingFailed(error));
                        }
                    };
                self.reference_metadata = Some(source_metadata);
                self.state = ReplicateObjectVersionState::WriteReferenceBlob;
                smallvec![Effect::Blob(BlobEffect::Write {
                    bucket: self.request.bucket.clone(),
                    key: self.request.key.clone(),
                    resolved,
                    created_by,
                    blob: stream,
                })]
            }
            Event::StagingSource(StagingSourceEvent::Error { error }) => {
                debug!(
                    bucket = %self.request.bucket,
                    key = %self.request.key,
                    version_id = %self.request.version_id,
                    target_node = %self.request.target_node_id,
                    error = %error,
                    "Reading on-demand reference source failed; skipping version"
                );
                if self.sync.is_some() {
                    self.fail(ReplicationError::ReplicationFailed.into())
                } else {
                    self.skip_version()
                }
            }
            other => self.fail(ReplicateObjectVersionError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::StagingSource(StagingSourceEvent::ReadResult)",
                received: other,
            }),
        }
    }

    fn handle_reference_blob_written(&mut self, event: Event) -> Effects {
        match event {
            Event::Blob(BlobEvent::WriteFinished { location }) => {
                let Some(version) = self.replication_version.take() else {
                    return self.fail(ReplicateObjectVersionError::VersionNotFound);
                };
                let ReplicationVersion::Reference {
                    created_at,
                    created_by,
                    source,
                    metadata,
                    ..
                } = version
                else {
                    return self.fail(ReplicateObjectVersionError::VersionNotFound);
                };
                debug!(
                    bucket = %self.request.bucket,
                    key = %self.request.key,
                    version_id = %self.request.version_id,
                    target_node = %self.request.target_node_id,
                    blob_size = location.blob_size,
                    temporary_path = %location.backend_path,
                    "Materialized temporary blob for on-demand reference replication"
                );
                self.cleanup_reference_blob = Some(location.clone());
                self.replication_version = Some(ReplicationVersion::Materialized {
                    created_at,
                    created_by,
                    location,
                    source: Some(source),
                    metadata,
                });
                self.read_current_lookup()
            }
            Event::Blob(BlobEvent::Error(BlobError::WriteCleanup { location, .. })) => {
                self.cleanup_reference_blob = Some(location);
                self.fail(ReplicationError::ReplicationFailed.into())
            }
            Event::Blob(BlobEvent::Error(_)) => {
                self.fail(ReplicationError::ReplicationFailed.into())
            }
            other => self.fail(ReplicateObjectVersionError::InvalidStateEvent {
                state: self.state_name(),
                expected: "Event::Blob(BlobEvent::WriteFinished)",
                received: other,
            }),
        }
    }

    fn build_manifest(
        &mut self,
        current_lookup: Option<CurrentVersionPointer>,
    ) -> Result<(), ReplicateObjectVersionError> {
        if self.reference_advance.is_none() {
            self.validate_multipart_parts_complete()?;
        }

        let version = self
            .replication_version
            .clone()
            .ok_or(ReplicateObjectVersionError::VersionNotFound)?;
        let reference_intent =
            self.sync.as_ref().is_some_and(|sync| sync.reference_intent) || self.preserve_reference;
        if self.reference_advance.is_some()
            && (!self.preserve_reference
                || !reference_intent
                || self.multipart_summary.is_some()
                || !matches!(&version, ReplicationVersion::Reference { .. }))
        {
            return Err(ReplicateObjectVersionError::InvalidReferenceAdvance);
        }
        let current_version_pointer = current_lookup
            .as_ref()
            .filter(|pointer| pointer.version_id == self.request.version_id);
        let (current_version, current_version_generation) = match self.reference_advance.as_ref() {
            Some(advance) => (true, Some(advance.generation)),
            None => (
                current_version_pointer.is_some(),
                current_version_pointer.map(|pointer| pointer.generation),
            ),
        };
        // A materialized version exported as a reference starts a fresh binding at
        // the target; a reference carries its own count so the cap survives repair.
        let reference_advance_count = match &version {
            ReplicationVersion::Reference { advance_count, .. } => Some(*advance_count),
            ReplicationVersion::Materialized { .. } if reference_intent => Some(0),
            _ => None,
        };
        let (kind, created_at, created_by, blob, source, reference, metadata) = match version {
            ReplicationVersion::Materialized {
                created_at,
                created_by,
                location,
                source,
                metadata,
            } => {
                if reference_intent {
                    let sync = self
                        .sync
                        .as_ref()
                        .ok_or(ReplicateObjectVersionError::UnresolvedReferenceVersion)?;
                    // Must equal the live head of this native source, or the target's first read
                    // sees drift and forks a successor. The head reads the version, not the shared
                    // content-addressed, deduplicated location row.
                    let reference = SourceMetadata {
                        content_length: location.blob_size,
                        content_type: None,
                        etag: None,
                        last_modified: Some(created_at),
                        source_version: None,
                    };
                    (
                        ReplicationItemKind::Materialized,
                        created_at,
                        created_by,
                        None,
                        Some(self.reference_binding(sync)),
                        Some(reference),
                        metadata,
                    )
                } else {
                    let hash = location
                        .get_blake3()
                        .ok_or(ReplicateObjectVersionError::MissingBlobHash)?
                        .try_into()
                        .map_err(|_| ReplicateObjectVersionError::MissingBlobHash)?;
                    (
                        ReplicationItemKind::Materialized,
                        created_at,
                        created_by,
                        Some(MaterializedBlobInfo {
                            hash,
                            size: location.blob_size,
                            compressed: location.compressed,
                            encrypted: location.encrypted,
                            location,
                        }),
                        source,
                        None,
                        metadata,
                    )
                }
            }
            ReplicationVersion::Deleted {
                created_at,
                created_by,
            } => (
                ReplicationItemKind::DeleteMarker,
                created_at,
                created_by,
                None,
                None,
                None,
                HashMap::new(),
            ),
            ReplicationVersion::Reference {
                created_at,
                created_by,
                cached_metadata,
                metadata,
                ..
            } => {
                if !reference_intent {
                    return Err(ReplicateObjectVersionError::UnresolvedReferenceVersion);
                }
                let sync = self
                    .sync
                    .as_ref()
                    .ok_or(ReplicateObjectVersionError::UnresolvedReferenceVersion)?;
                (
                    ReplicationItemKind::Materialized,
                    created_at,
                    created_by,
                    None,
                    Some(self.reference_binding(sync)),
                    Some(cached_metadata),
                    metadata,
                )
            }
        };

        let multipart =
            self.multipart_summary
                .clone()
                .map(|summary| MultipartObjectReplicationMetadata {
                    checksum_type: summary.checksum_type,
                    summary,
                    parts: self.multipart_parts.clone(),
                });

        self.manifest = Some(VersionReplicationManifest {
            bucket: self.sync.as_ref().map_or_else(
                || self.request.bucket.clone(),
                |sync| sync.target_bucket.clone(),
            ),
            key: self.sync.as_ref().map_or_else(
                || self.request.key.clone(),
                |sync| sync.target_prefix.clone().unwrap_or_default(),
            ),
            version_id: self.request.version_id,
            group_id: self.request.source_group_id,
            kind,
            created_at,
            created_by,
            current_version,
            current_version_generation,
            auth_context: self.request.auth_context.clone(),
            blob,
            source,
            multipart,
            reference_intent,
            origin: self.sync.as_ref().and_then(|sync| sync.origin.clone()),
            upstream_sources: self
                .sync
                .as_ref()
                .map_or_else(Vec::new, |sync| sync.upstream_sources.clone()),
            writer_auth_context: if self.reference_advance.is_some() {
                None
            } else {
                self.writer_auth_context.clone().or_else(|| {
                    self.sync
                        .as_ref()
                        .and_then(|sync| sync.writer_auth_context.clone())
                })
            },
            reference_metadata: reference,
            metadata,
            reference_advance: self.reference_advance,
            reference_advance_count,
            placement_policies: self.version_policies.clone(),
        });
        if let Some(manifest) = self.manifest.as_ref() {
            debug!(
                bucket = %manifest.bucket,
                key = %manifest.key,
                version_id = %manifest.version_id,
                target_node = %self.request.target_node_id,
                kind = ?manifest.kind,
                has_blob = manifest.blob.is_some(),
                blob_size = manifest.blob.as_ref().map(|blob| blob.size),
                multipart_parts = manifest.multipart.as_ref().map(|m| m.parts.len()).unwrap_or(0),
                current_version = manifest.current_version,
                current_version_generation = ?manifest.current_version_generation,
                "Built version replication manifest"
            );
        }
        Ok(())
    }

    fn reference_binding(&self, sync: &SyncTransferContext) -> VersionSourceBinding {
        VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: PortableSourceDescriptor {
                kind: SourceConnectorKind::ArunaNative,
                public_config: std::collections::HashMap::from([(
                    ARUNA_NATIVE_RELATIONSHIP_ID.to_string(),
                    sync.relationship_id.to_string(),
                )]),
                source_path: format!("{}/{}", self.request.bucket, self.request.key),
                version_selector: Some(format!("version:{}", self.request.version_id)),
                capabilities: Vec::new(),
                origin_node_id: Some(sync.source_node_id),
            },
            connector_id: None,
        }
    }

    fn send_manifest(&mut self) -> Effects {
        self.state = ReplicateObjectVersionState::SendManifest;
        let Some(stream_id) = self.stream_id else {
            return self.fail(ReplicationError::ConnectionMissing.into());
        };
        let manifest = self.manifest.clone().expect("manifest available");
        let message = match manifest.reference_advance {
            Some(advance) => VersionReplicationMessage::ReferenceAdvance { manifest, advance },
            None => VersionReplicationMessage::VersionManifest(manifest),
        };
        let payload = match message.to_bytes() {
            Ok(payload) => payload,
            Err(err) => return self.fail(err.into()),
        };

        smallvec![Effect::Blob(BlobEffect::SendMessage { stream_id, payload })]
    }

    fn await_negotiation(&mut self) -> Effects {
        self.state = ReplicateObjectVersionState::AwaitNegotiation;
        let Some(stream_id) = self.stream_id else {
            return self.fail(ReplicationError::ConnectionMissing.into());
        };
        smallvec![Effect::Blob(BlobEffect::ReadMessage { stream_id })]
    }

    fn await_apply_complete(&mut self) -> Effects {
        self.state = ReplicateObjectVersionState::AwaitApplyComplete;
        let Some(stream_id) = self.stream_id else {
            return self.fail(ReplicationError::ConnectionMissing.into());
        };
        smallvec![Effect::Blob(BlobEffect::ReadMessage { stream_id })]
    }

    fn close_connection(&mut self) -> Effects {
        self.state = ReplicateObjectVersionState::CloseConnection;
        let Some(stream_id) = self.stream_id else {
            return self.fail(ReplicationError::ConnectionMissing.into());
        };
        smallvec![Effect::Blob(BlobEffect::CloseConnection { stream_id })]
    }

    fn cleanup_reference_blob_or_close(&mut self) -> Effects {
        if let Some(location) = self.cleanup_reference_blob.take() {
            self.state = ReplicateObjectVersionState::CleanupReferenceBlob;
            smallvec![Effect::Blob(BlobEffect::Delete { location })]
        } else {
            self.close_connection()
        }
    }

    fn write_reference_state(&mut self) -> Effects {
        if self.sync.is_none() {
            return self.cleanup_reference_blob_or_close();
        }
        let Some(metadata) = self.reference_metadata.as_ref() else {
            return self.cleanup_reference_blob_or_close();
        };
        let key = match self.reference_state_key() {
            Ok(key) => key,
            Err(error) => return self.fail(error),
        };
        let handling = if self.preserve_reference {
            ReferenceHandling::Preserve
        } else {
            ReferenceHandling::Materialize
        };
        let value = match reference_fingerprint(metadata, handling) {
            Ok(value) => value.to_vec(),
            Err(error) => return self.fail(error.into()),
        };
        self.state = ReplicateObjectVersionState::WriteReferenceState;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: SYNC_REFERENCE_STATE_KEYSPACE.to_string(),
            key: key.into(),
            value: value.into(),
            txn_id: None,
        })]
    }
}

impl Operation for ReplicateObjectVersionOperation {
    type Output = Result<ReplicationSuboperationResult, ReplicateObjectVersionError>;
    type Error = ReplicateObjectVersionError;

    fn start(&mut self) -> Effects {
        self.read_version()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            ReplicateObjectVersionState::Init => self.read_version(),
            ReplicateObjectVersionState::ReadVersion => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.fail(ReplicateObjectVersionError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::ReadResult)",
                        received: event,
                    });
                };
                let Some(value) = value else {
                    return self.fail(ReplicateObjectVersionError::VersionNotFound);
                };
                self.pending_materialized_version = None;
                self.replication_version = None;
                let version = match BlobVersion::from_bytes(value.as_ref()) {
                    Ok(version) => version,
                    Err(err) => return self.fail(err.into()),
                };
                let is_materialized = version.is_materialized();
                let is_deleted = version.is_deleted();
                let has_source_binding = version.source_binding().is_some();
                debug!(
                    bucket = %self.request.bucket,
                    key = %self.request.key,
                    version_id = %self.request.version_id,
                    target_node = %self.request.target_node_id,
                    is_materialized,
                    is_deleted,
                    has_source_binding,
                    "Loaded blob version for replication"
                );
                let BlobVersion {
                    created_at,
                    created_by,
                    state,
                    metadata,
                    published_by: _,
                    placement_policies,
                } = version;
                self.version_policies = placement_policies;

                match state {
                    BlobVersionState::Materialized {
                        blob_hash,
                        backend,
                        source,
                    } => {
                        self.pending_materialized_version =
                            Some(PendingMaterializedReplicationVersion {
                                created_at,
                                created_by,
                                blob_hash,
                                source,
                                metadata,
                            });
                        self.read_blob_location(BlobLocationKey::new(blob_hash, backend))
                    }
                    BlobVersionState::Deleted => {
                        self.pending_materialized_version = None;
                        self.replication_version = Some(ReplicationVersion::Deleted {
                            created_at,
                            created_by,
                        });
                        self.read_current_lookup()
                    }
                    BlobVersionState::Reference {
                        source,
                        cached_metadata,
                        last_refresh,
                        advance_count,
                    } => {
                        self.pending_materialized_version = None;
                        self.resolve_reference_or_skip(ReplicationVersion::Reference {
                            created_at,
                            created_by,
                            source,
                            cached_metadata,
                            last_refresh,
                            metadata,
                            advance_count,
                        })
                    }
                }
            }
            ReplicateObjectVersionState::ReadBlobLocation => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.fail(ReplicateObjectVersionError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::ReadResult)",
                        received: event,
                    });
                };
                let Some(value) = value else {
                    return self.fail(ReplicateObjectVersionError::VersionNotFound);
                };
                let location = match BackendLocation::from_bytes(value.as_ref()) {
                    Ok(location) => location,
                    Err(err) => return self.fail(err.into()),
                };
                let Some(PendingMaterializedReplicationVersion {
                    created_at,
                    created_by,
                    blob_hash,
                    source,
                    metadata,
                }) = self.pending_materialized_version.take()
                else {
                    return self.fail(ReplicateObjectVersionError::VersionNotFound);
                };
                if location.get_blake3() != Some(blob_hash.as_slice()) {
                    return self.fail(ReplicateObjectVersionError::VersionNotFound);
                }
                self.replication_version = Some(ReplicationVersion::Materialized {
                    created_at,
                    created_by,
                    location: location.clone(),
                    source,
                    metadata,
                });
                // A governed copy is only pushed to a peer when this node may
                // still serve it; the destination gates itself on arrival.
                if self.version_policies.is_empty() {
                    return self.read_multipart_summary();
                }
                let key = ManagedCopyKey::new(
                    VersionKey::new(
                        &self.request.bucket,
                        &self.request.key,
                        self.request.version_id,
                    ),
                    location.backend.clone(),
                );
                let effect = match serve_reads(&key, None) {
                    Ok(effect) => effect,
                    Err(error) => return self.fail(error.into()),
                };
                self.pending_copy = Some(key);
                self.state = ReplicateObjectVersionState::CheckManagedCopy;
                smallvec![effect]
            }
            ReplicateObjectVersionState::CheckManagedCopy => {
                let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
                    return self.fail(ReplicateObjectVersionError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::BatchReadResult)",
                        received: event,
                    });
                };
                let (copy, subject) = match split_serve_reads(values) {
                    Ok(split) => split,
                    Err(error) => return self.fail(error.into()),
                };
                let Some(key) = self.pending_copy.take() else {
                    return self.fail(ReplicateObjectVersionError::VersionNotFound);
                };
                if let Err(error) = validate_registration(
                    copy.as_deref(),
                    &CopyRequest {
                        key: &key,
                        node_id: None,
                        blake3: None,
                        refs: &self.version_policies,
                        subject_generation: Some(subject.subject.generation),
                    },
                ) {
                    return self.fail(error.into());
                }
                self.read_multipart_summary()
            }
            ReplicateObjectVersionState::ResolveReferenceAccess => {
                self.handle_reference_access_resolved(event)
            }
            ReplicateObjectVersionState::HeadReferenceSource => self.handle_reference_head(event),
            ReplicateObjectVersionState::ReadReferenceState => self.handle_reference_state(event),
            ReplicateObjectVersionState::LoadRouting => self.handle_routing_loaded(event),
            ReplicateObjectVersionState::ReadBucketRules => self.handle_bucket_rules(event),
            ReplicateObjectVersionState::ReadReferenceSource => {
                self.handle_reference_source_read(event)
            }
            ReplicateObjectVersionState::ReferencePolicyGate => self.handle_reference_gate(event),
            ReplicateObjectVersionState::WriteReferenceBlob => {
                self.handle_reference_blob_written(event)
            }
            ReplicateObjectVersionState::ReadMultipartSummary => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.fail(ReplicateObjectVersionError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::ReadResult)",
                        received: event,
                    });
                };
                self.multipart_summary = value
                    .as_ref()
                    .and_then(|value| MultipartObjectSummary::from_bytes(value.as_ref()).ok());
                if self.multipart_summary.is_some() {
                    self.multipart_parts.clear();
                    self.multipart_parts_next_start_after = None;
                    self.read_multipart_parts()
                } else {
                    self.read_current_lookup()
                }
            }
            ReplicateObjectVersionState::ReadMultipartParts => {
                let Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }) = event
                else {
                    return self.fail(ReplicateObjectVersionError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::IterResult)",
                        received: event,
                    });
                };

                for (_, value) in values {
                    let part = match MultipartObjectPart::from_bytes(value.as_ref()) {
                        Ok(part) => part,
                        Err(err) => return self.fail(err.into()),
                    };
                    self.multipart_parts.push(part);
                }

                if let Some(cursor) = next_start_after {
                    self.multipart_parts_next_start_after = Some(cursor);
                    self.read_multipart_parts()
                } else {
                    self.multipart_parts_next_start_after = None;
                    self.multipart_parts
                        .sort_unstable_by_key(|part| part.part_number);
                    if let Err(err) = self.validate_multipart_parts_complete() {
                        return self.fail(err);
                    }
                    self.read_current_lookup()
                }
            }
            ReplicateObjectVersionState::ReadCurrentLookup => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.fail(ReplicateObjectVersionError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::ReadResult)",
                        received: event,
                    });
                };
                let current_lookup = value
                    .as_ref()
                    .and_then(|value| CurrentVersionPointer::from_bytes(value.as_ref()).ok());
                debug!(
                    bucket = %self.request.bucket,
                    key = %self.request.key,
                    version_id = %self.request.version_id,
                    target_node = %self.request.target_node_id,
                    current_version_generation = current_lookup.as_ref().map(|pointer| pointer.generation),
                    current_version_matches = current_lookup
                        .as_ref()
                        .map(|pointer| pointer.version_id == self.request.version_id)
                        .unwrap_or(false),
                    "Loaded current version pointer before manifest creation"
                );
                if let Err(err) = self.build_manifest(current_lookup) {
                    return self.fail(err);
                }
                self.state = ReplicateObjectVersionState::OpenConnection;
                smallvec![Effect::Blob(BlobEffect::OpenConnection {
                    node_id: self.request.target_node_id,
                })]
            }
            ReplicateObjectVersionState::OpenConnection => {
                let Event::Blob(BlobEvent::ConnectionEstablished { stream_id }) = event else {
                    return self.fail(ReplicateObjectVersionError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Blob(BlobEvent::ConnectionEstablished)",
                        received: event,
                    });
                };
                self.stream_id = Some(stream_id);
                debug!(
                    bucket = %self.request.bucket,
                    key = %self.request.key,
                    version_id = %self.request.version_id,
                    target_node = %self.request.target_node_id,
                    stream_id = %stream_id,
                    "Opened replication connection to target node"
                );
                self.send_manifest()
            }
            ReplicateObjectVersionState::SendManifest => {
                let Event::Blob(BlobEvent::MessageSent { .. }) = event else {
                    return self.fail(ReplicateObjectVersionError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Blob(BlobEvent::MessageSent)",
                        received: event,
                    });
                };
                debug!(
                    bucket = %self.request.bucket,
                    key = %self.request.key,
                    version_id = %self.request.version_id,
                    target_node = %self.request.target_node_id,
                    stream_id = ?self.stream_id,
                    "Sent version replication manifest"
                );
                self.await_negotiation()
            }
            ReplicateObjectVersionState::AwaitNegotiation => {
                let Event::Blob(BlobEvent::MessageReceived { payload, .. }) = event else {
                    return self.fail(ReplicateObjectVersionError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Blob(BlobEvent::MessageReceived)",
                        received: event,
                    });
                };

                let message = match VersionReplicationMessage::from_bytes(&payload) {
                    Ok(message) => message,
                    Err(err) => return self.fail(err.into()),
                };
                let VersionReplicationMessage::VersionNegotiationResponse(result) = message else {
                    return self.fail(ReplicateObjectVersionError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "VersionReplicationMessage::VersionNegotiationResponse",
                        received: Event::Blob(BlobEvent::MessageReceived {
                            stream_id: self.stream_id.expect("stream id available"),
                            payload,
                        }),
                    });
                };

                match result {
                    ReplicationNegotiationResult::AlreadyReplicatedVersion => {
                        debug!(
                            bucket = %self.request.bucket,
                            key = %self.request.key,
                            version_id = %self.request.version_id,
                            target_node = %self.request.target_node_id,
                            decision = ?result,
                            "Target reported version already replicated"
                        );
                        self.result = Ok(ReplicationSuboperationResult::Skipped);
                        self.write_reference_state()
                    }
                    ReplicationNegotiationResult::NeedVersionOnly => {
                        debug!(
                            bucket = %self.request.bucket,
                            key = %self.request.key,
                            version_id = %self.request.version_id,
                            target_node = %self.request.target_node_id,
                            decision = ?result,
                            "Target requested version metadata only"
                        );
                        self.await_apply_complete()
                    }
                    ReplicationNegotiationResult::NeedBlobAndVersion => {
                        let Some(blob) = self
                            .manifest
                            .as_ref()
                            .and_then(|manifest| manifest.blob.as_ref())
                        else {
                            return self.fail(ReplicateObjectVersionError::MissingBlobHash);
                        };
                        self.state = ReplicateObjectVersionState::TransferBlob;
                        let replication_id = Ulid::generate();
                        self.blob_replication_id = Some(replication_id);
                        debug!(
                            bucket = %self.request.bucket,
                            key = %self.request.key,
                            version_id = %self.request.version_id,
                            target_node = %self.request.target_node_id,
                            decision = ?result,
                            replication_id = %replication_id,
                            blob_size = blob.size,
                            "Target requested blob transfer"
                        );
                        smallvec![Effect::Blob(BlobEffect::Replicate {
                            replication_id,
                            stream_id: self.stream_id.expect("stream id available"),
                            location: blob.location.clone(),
                            keep_alive: true,
                        })]
                    }
                    ReplicationNegotiationResult::Rejected(reason) => {
                        debug!(
                            bucket = %self.request.bucket,
                            key = %self.request.key,
                            version_id = %self.request.version_id,
                            target_node = %self.request.target_node_id,
                            reason = %reason,
                            "Target rejected version replication"
                        );
                        self.fail(ReplicationError::ReplicationRejected(reason).into())
                    }
                }
            }
            ReplicateObjectVersionState::TransferBlob => {
                let Event::Blob(BlobEvent::ReplicationFinished { .. }) = event else {
                    return self.fail(ReplicateObjectVersionError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Blob(BlobEvent::ReplicationFinished)",
                        received: event,
                    });
                };
                debug!(
                    bucket = %self.request.bucket,
                    key = %self.request.key,
                    version_id = %self.request.version_id,
                    target_node = %self.request.target_node_id,
                    replication_id = ?self.blob_replication_id,
                    "Finished blob transfer to target node"
                );
                self.await_apply_complete()
            }
            ReplicateObjectVersionState::AwaitApplyComplete => {
                let Event::Blob(BlobEvent::MessageReceived { payload, .. }) = event else {
                    return self.fail(ReplicateObjectVersionError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Blob(BlobEvent::MessageReceived)",
                        received: event,
                    });
                };

                match VersionReplicationMessage::from_bytes(&payload) {
                    Ok(VersionReplicationMessage::VersionApplyComplete) => {
                        debug!(
                            bucket = %self.request.bucket,
                            key = %self.request.key,
                            version_id = %self.request.version_id,
                            target_node = %self.request.target_node_id,
                            "Target completed version apply"
                        );
                        let replicated_bytes = if self.blob_replication_id.is_some() {
                            self.manifest
                                .as_ref()
                                .and_then(|manifest| manifest.blob.as_ref())
                                .map_or(0, |blob| blob.size)
                        } else {
                            0
                        };
                        self.result = Ok(ReplicationSuboperationResult::ReplicatedBytes(
                            replicated_bytes,
                        ));
                        self.write_reference_state()
                    }
                    Ok(VersionReplicationMessage::VersionApplyRejected(reason)) => {
                        self.fail(ReplicationError::ReplicationRejected(reason).into())
                    }
                    Ok(_) => self.fail(ReplicateObjectVersionError::InvalidStateEvent {
                        state: self.state_name(),
                        expected:
                            "VersionReplicationMessage::VersionApplyComplete|VersionApplyRejected",
                        received: Event::Blob(BlobEvent::MessageReceived {
                            stream_id: self.stream_id.expect("stream id available"),
                            payload,
                        }),
                    }),
                    Err(err) => self.fail(err.into()),
                }
            }
            ReplicateObjectVersionState::CleanupReferenceBlob => match event {
                Event::Blob(BlobEvent::DeleteFinished) | Event::Blob(BlobEvent::Error(_)) => {
                    self.close_connection()
                }
                other => self.fail(ReplicateObjectVersionError::InvalidStateEvent {
                    state: self.state_name(),
                    expected: "Event::Blob(BlobEvent::DeleteFinished|Error)",
                    received: other,
                }),
            },
            ReplicateObjectVersionState::WriteReferenceState => match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => {
                    self.cleanup_reference_blob_or_close()
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.fail(ReplicateObjectVersionError::InvalidStateEvent {
                    state: self.state_name(),
                    expected: "Event::Storage(StorageEvent::WriteResult)",
                    received: other,
                }),
            },
            ReplicateObjectVersionState::CloseConnection => {
                let Event::Blob(BlobEvent::ConnectionClosed { .. }) = event else {
                    return self.fail(ReplicateObjectVersionError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Blob(BlobEvent::ConnectionClosed)",
                        received: event,
                    });
                };
                self.state = ReplicateObjectVersionState::Finish;
                smallvec![]
            }
            ReplicateObjectVersionState::Finish => smallvec![],
            ReplicateObjectVersionState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            ReplicateObjectVersionState::Finish | ReplicateObjectVersionState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == ReplicateObjectVersionState::Error {
            return match self.result {
                Ok(_) => Err(ReplicateObjectVersionError::VersionNotFound),
                Err(err) => Err(err),
            };
        }
        Ok(self.result)
    }

    fn abort(&mut self) -> Effects {
        let mut effects = smallvec![];
        if let Some(location) = self.cleanup_reference_blob.take() {
            effects.push(Effect::Blob(BlobEffect::Delete { location }));
        }
        if let Some(stream_id) = self.stream_id {
            effects.push(Effect::Blob(BlobEffect::CloseConnection { stream_id }));
        }
        effects
    }
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_SCOPE_VERSIONS, ReplicateObjectVersionError, ReplicateObjectVersionOperation,
        ReplicateScopeError, ReplicateScopeInput, ReplicateScopeOperation, ReplicateScopeTarget,
        ReplicationVersion, SourceAuthorization, SyncTransferContext,
    };
    use crate::driver::DriverContext;
    use crate::replication::protocol::{
        ReferenceAdvance, ReplicationMode, SyncOrigin, VersionReplicationMessage,
        VersionReplicationRequest,
    };
    use aruna_core::UserId;
    use aruna_core::effects::{BlobEffect, Effect, IterStart, StagingSourceEffect, StorageEffect};
    use aruna_core::errors::BlobError;
    use aruna_core::events::{
        BlobEvent, Event, StagingSourceEvent, StorageEvent, SubOperationEvent,
    };
    use aruna_core::keyspaces::{
        AUTH_KEYSPACE, BLOB_HEAD_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE,
    };
    use aruna_core::operation::Operation;
    use aruna_core::stream::BackendStream;
    use aruna_core::structs::{
        Actor, AuthContext, BackendLocation, BackendRef, BlobVersion, BucketInfo,
        CurrentVersionPointer, Group, GroupAuthorizationDocument, GroupRoutingInputs,
        MultipartChecksumType, MultipartObjectMetadataKey, MultipartObjectPart,
        MultipartObjectSummary, PathRestriction, Permission, PortableSourceDescriptor,
        RealmAuthorizationDocument, RealmConfigDocument, RealmId, ReferenceHandling,
        ReplicationItemKind, ReplicationNegotiationResult, ReplicationSuboperationResult,
        ResolvedSourceAccess, SourceConnectorKind, SourceMetadata, StagingStrategy, VersionKey,
        VersionSourceBinding,
    };
    use aruna_core::types::Effects;
    use aruna_storage::FjallStorage;
    use bytes::Bytes;
    use futures_util::stream;
    use std::collections::HashMap;
    use std::time::SystemTime;
    use tempfile::TempDir;
    use ulid::Ulid;

    /// Replays the routing loader and bucket-rules read the reference path adds.
    fn load_routing(op: &mut ReplicateObjectVersionOperation) -> Effects {
        op.step(Event::SubOperation(SubOperationEvent::GroupRoutingLoaded {
            result: Ok(GroupRoutingInputs::default()),
        }));
        op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8].into(),
            value: None,
        }))
    }

    #[test]
    fn maps_sync_prefix() {
        assert_eq!(
            super::map_sync_key("photos/2026/a.jpg", Some("photos/"), Some("archive/")),
            Some("archive/2026/a.jpg".to_string())
        );
        assert_eq!(
            super::map_sync_key("photos/a.jpg", Some("photos/"), None),
            Some("a.jpg".to_string())
        );
        assert_eq!(
            super::map_sync_key("other/a.jpg", Some("photos/"), Some("archive/")),
            None
        );
    }

    fn test_realm_id() -> RealmId {
        RealmId::from_bytes([7u8; 32])
    }

    fn test_user_id() -> UserId {
        UserId::nil(test_realm_id())
    }

    fn bucket_info() -> BucketInfo {
        BucketInfo {
            group_id: Ulid::generate(),
            created_at: SystemTime::now(),
            created_by: test_user_id(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
        }
    }

    fn auth_context() -> AuthContext {
        AuthContext {
            user_id: test_user_id(),
            realm_id: test_realm_id(),
            path_restrictions: None,
        }
    }

    fn scope_input(target: ReplicateScopeTarget) -> ReplicateScopeInput {
        ReplicateScopeInput {
            bucket: "bucket".to_string(),
            target,
            target_node_id: iroh::SecretKey::generate().public(),
            auth_context: auth_context(),
            replicate_delete_markers: true,
            mode: ReplicationMode::Live,
        }
    }

    async fn source_auth(
        input: &ReplicateScopeInput,
        allow: bool,
    ) -> (TempDir, SourceAuthorization) {
        let directory = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(directory.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let realm_id = input.auth_context.realm_id;
        let group_id = Ulid::from_bytes([3u8; 16]);
        let actor = Actor {
            node_id: input.target_node_id,
            user_id: input.auth_context.user_id,
            realm_id,
        };
        let realm = RealmAuthorizationDocument {
            realm_id,
            roles: HashMap::new(),
            operation_restrictions: HashMap::new(),
        };
        let group = if allow {
            GroupAuthorizationDocument::new_default_group_doc(actor.user_id, realm_id, group_id)
        } else {
            GroupAuthorizationDocument {
                group_id,
                roles: HashMap::new(),
                policies: Vec::new(),
            }
        };
        let event = storage
            .send_storage_effect(StorageEffect::BatchWrite {
                writes: vec![
                    // Policy loading fails closed without the realm config document.
                    (
                        REALM_CONFIG_KEYSPACE.to_string(),
                        realm_id.as_bytes().to_vec().into(),
                        RealmConfigDocument::default_for_realm(realm_id, Vec::new())
                            .to_bytes(&actor)
                            .unwrap()
                            .into(),
                    ),
                    (
                        AUTH_KEYSPACE.to_string(),
                        realm_id.as_bytes().to_vec().into(),
                        realm.to_bytes(&actor).unwrap().into(),
                    ),
                    (
                        AUTH_KEYSPACE.to_string(),
                        group_id.to_bytes().into(),
                        group.to_bytes(&actor).unwrap().into(),
                    ),
                    // Policy loading resolves the group record before group policies apply.
                    (
                        GROUP_KEYSPACE.to_string(),
                        group_id.to_bytes().into(),
                        Group {
                            display_name: "replication-group".to_string(),
                            group_id,
                            realm_id,
                            roles: group.roles.keys().copied().collect(),
                            owner: actor.user_id,
                        }
                        .to_bytes(&actor)
                        .unwrap()
                        .into(),
                    ),
                ],
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::BatchWriteResult { .. })
        ));
        let authorization = SourceAuthorization::load(
            &context,
            input.auth_context.clone(),
            group_id,
            input.target_node_id,
        )
        .await
        .unwrap();
        (directory, authorization)
    }

    fn version_entry(
        key: &str,
        version_id: Ulid,
    ) -> (aruna_core::types::Key, aruna_core::types::Value) {
        let key_bytes = VersionKey::new("bucket", key, version_id)
            .to_bytes()
            .unwrap();
        let value_bytes = BlobVersion::deleted(SystemTime::now(), test_user_id())
            .to_bytes()
            .unwrap();
        (key_bytes.into(), value_bytes.into())
    }

    fn materialized_blob_version(
        location: &BackendLocation,
        source: Option<VersionSourceBinding>,
    ) -> BlobVersion {
        BlobVersion::materialized(
            location.get_blake3().unwrap().try_into().unwrap(),
            BackendRef::node_default(),
            location.created_at,
            location.created_by,
            source,
        )
    }

    fn reference_source_binding() -> VersionSourceBinding {
        VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: PortableSourceDescriptor {
                kind: SourceConnectorKind::Http,
                public_config: HashMap::from([(
                    "endpoint".to_string(),
                    "https://example.org".to_string(),
                )]),
                source_path: "ref/file.txt".to_string(),
                version_selector: None,
                capabilities: Vec::new(),
                origin_node_id: None,
            },
            connector_id: Some(Ulid::from_bytes([9u8; 16])),
        }
    }

    fn reference_cached_metadata() -> SourceMetadata {
        SourceMetadata {
            content_length: 42,
            content_type: Some("text/plain".to_string()),
            etag: Some("etag-1".to_string()),
            last_modified: Some(SystemTime::UNIX_EPOCH),
            source_version: None,
        }
    }

    fn reference_blob_version() -> BlobVersion {
        BlobVersion::reference(
            reference_source_binding(),
            reference_cached_metadata(),
            SystemTime::now(),
            test_user_id(),
            SystemTime::now(),
        )
    }

    fn materialized_location() -> BackendLocation {
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

    fn version_request_with_mode(
        version_id: Ulid,
        mode: ReplicationMode,
    ) -> VersionReplicationRequest {
        VersionReplicationRequest {
            bucket: "bucket".to_string(),
            key: "dir/file.txt".to_string(),
            version_id,
            source_group_id: Ulid::generate(),
            target_node_id: iroh::SecretKey::generate().public(),
            auth_context: auth_context(),
            mode,
        }
    }

    fn version_request(version_id: Ulid) -> VersionReplicationRequest {
        version_request_with_mode(version_id, ReplicationMode::Live)
    }

    fn reference_sync() -> SyncTransferContext {
        SyncTransferContext {
            target_bucket: "target-bucket".to_string(),
            source_prefix: None,
            target_prefix: Some("copied/file.txt".to_string()),
            source_node_id: iroh::SecretKey::generate().public(),
            relationship_id: Ulid::generate(),
            reference_intent: true,
            reference_handling: ReferenceHandling::Materialize,
            origin: None,
            upstream_sources: Vec::new(),
            writer_auth_context: None,
        }
    }

    #[test]
    fn destination_maps_exact() {
        let version_id = Ulid::generate();
        let source_node_id = iroh::SecretKey::generate().public();
        let writer = auth_context();
        let operation = ReplicateScopeOperation::new(scope_input(ReplicateScopeTarget::Version {
            key: "stage-version".to_string(),
            version_id,
        }))
        .with_destination(
            "final-bucket".to_string(),
            "results/output.txt".to_string(),
            source_node_id,
            writer.clone(),
        );
        let sync = operation.sync.expect("destination mapping");

        assert_eq!(sync.target_bucket, "final-bucket");
        assert_eq!(sync.source_prefix.as_deref(), Some("stage-version"));
        assert_eq!(sync.target_prefix.as_deref(), Some("results/output.txt"));
        assert_eq!(sync.source_node_id, source_node_id);
        assert_eq!(sync.relationship_id, version_id);
        assert_eq!(sync.origin, None);
        assert_eq!(sync.writer_auth_context, Some(writer));
    }

    fn multipart_part_entry(
        version_id: Ulid,
        part_number: u16,
    ) -> (aruna_core::types::Key, aruna_core::types::Value) {
        let key = MultipartObjectMetadataKey::part(version_id, part_number)
            .to_bytes()
            .unwrap();
        let value = MultipartObjectPart {
            part_number,
            size: u64::from(part_number),
            hashes: HashMap::new(),
        }
        .to_bytes()
        .unwrap();
        (key.into(), value.into())
    }

    #[test]
    fn exact_object_hit_iterates_only_matching_object_versions() {
        let mut op = ReplicateScopeOperation::new(scope_input(ReplicateScopeTarget::Object {
            key: "dir/file.txt".to_string(),
        }));

        op.start();
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: b"bucket".to_vec().into(),
            value: Some(bucket_info().to_bytes().unwrap().into()),
        }));
        assert!(matches!(
            effects[0],
            Effect::Storage(StorageEffect::Read { .. })
        ));

        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![1u8].into(),
            value: Some(vec![2u8].into()),
        }));
        let Effect::Storage(StorageEffect::Iter { .. }) = &effects[0] else {
            panic!("expected iteration after exact object lookup")
        };

        let matching_version = Ulid::generate();
        let effects = op.step(Event::Storage(StorageEvent::IterResult {
            values: vec![
                version_entry("dir/file.txt", matching_version),
                version_entry("dir/file.txt.bak", Ulid::generate()),
                version_entry("dir/sub/file.txt", Ulid::generate()),
            ],
            next_start_after: None,
        }));

        let Effect::SubOperation(_) = &effects[0] else {
            panic!("expected only matching object version to be enqueued")
        };
        assert_eq!(op.pending_versions.len(), 0);
    }

    #[test]
    fn scope_dedups_versions() {
        let mut op = ReplicateScopeOperation::new(scope_input(ReplicateScopeTarget::Bucket));
        op.state = super::ReplicateScopeState::IterateVersions;
        let version_id = Ulid::from_bytes([1u8; 16]);
        let cursor: aruna_core::types::Key = vec![9u8].into();

        let effects = op.step(Event::Storage(StorageEvent::IterResult {
            values: vec![version_entry("dir/file.txt", version_id); 2],
            next_start_after: Some(cursor),
        }));

        assert!(matches!(
            effects[0],
            Effect::Storage(StorageEffect::Iter { .. })
        ));
        assert_eq!(op.pending_versions.len(), 1);
        assert_eq!(op.pending_keys.len(), 1);
    }

    #[test]
    fn scope_rejects_overflow() {
        // A page pushing the examined total past the budget is rejected before
        // any of its versions are enqueued.
        let mut op = ReplicateScopeOperation::new(scope_input(ReplicateScopeTarget::Bucket));
        op.state = super::ReplicateScopeState::IterateVersions;
        let values = (0..=MAX_SCOPE_VERSIONS)
            .map(|index| {
                version_entry(
                    &format!("key-{index}"),
                    Ulid::from_bytes((index as u128 + 1).to_be_bytes()),
                )
            })
            .collect();

        let effects = op.step(Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after: None,
        }));

        assert!(effects.is_empty());
        assert_eq!(op.state, super::ReplicateScopeState::Error);
        assert!(op.pending_versions.is_empty());
        assert_eq!(
            op.output,
            Some(Err(ReplicateScopeError::ScopeLimit {
                limit: MAX_SCOPE_VERSIONS,
            }))
        );
    }

    #[test]
    fn scope_paginates_cursor() {
        let mut op = ReplicateScopeOperation::new(scope_input(ReplicateScopeTarget::Bucket));
        op.state = super::ReplicateScopeState::IterateVersions;
        let cursor: aruna_core::types::Key = vec![9u8].into();

        let effects = op.step(Event::Storage(StorageEvent::IterResult {
            values: vec![version_entry("dir/first", Ulid::from_bytes([1u8; 16]))],
            next_start_after: Some(cursor.clone()),
        }));
        let Effect::Storage(StorageEffect::Iter { start, .. }) = &effects[0] else {
            panic!("expected next scope page")
        };
        assert_eq!(start, &Some(IterStart::After(cursor)));

        let effects = op.step(Event::Storage(StorageEvent::IterResult {
            values: vec![version_entry("dir/second", Ulid::from_bytes([2u8; 16]))],
            next_start_after: None,
        }));
        assert!(matches!(effects[0], Effect::SubOperation(_)));
        assert_eq!(op.pending_versions.len(), 1);
    }

    #[tokio::test]
    async fn scope_caps_denied() {
        // Authorization-denied pages must consume the same finite scan budget.
        let input = scope_input(ReplicateScopeTarget::Bucket);
        let (_directory, authorization) = source_auth(&input, false).await;
        let mut op = ReplicateScopeOperation::new(input).with_source_authorization(authorization);
        op.state = super::ReplicateScopeState::IterateVersions;

        let first = (0..512)
            .map(|index| {
                version_entry(
                    &format!("key-{index}"),
                    Ulid::from_bytes((index as u128 + 1).to_be_bytes()),
                )
            })
            .collect();
        let cursor = vec![1u8].into();
        let effects = op.step(Event::Storage(StorageEvent::IterResult {
            values: first,
            next_start_after: Some(cursor),
        }));
        assert!(matches!(
            effects[0],
            Effect::Storage(StorageEffect::Iter { .. })
        ));

        let second = (512..1024)
            .map(|index| {
                version_entry(
                    &format!("key-{index}"),
                    Ulid::from_bytes((index as u128 + 1).to_be_bytes()),
                )
            })
            .collect();
        let cursor = vec![2u8].into();
        let effects = op.step(Event::Storage(StorageEvent::IterResult {
            values: second,
            next_start_after: Some(cursor),
        }));
        assert!(matches!(
            effects[0],
            Effect::Storage(StorageEffect::Iter { .. })
        ));
        assert_eq!(op.examined_versions, MAX_SCOPE_VERSIONS);
        assert!(op.pending_versions.is_empty());

        let effects = op.step(Event::Storage(StorageEvent::IterResult {
            values: vec![version_entry(
                "key-1024",
                Ulid::from_bytes((MAX_SCOPE_VERSIONS as u128 + 1).to_be_bytes()),
            )],
            next_start_after: None,
        }));
        assert!(effects.is_empty());
        assert_eq!(op.state, super::ReplicateScopeState::Error);
        assert_eq!(
            op.output,
            Some(Err(ReplicateScopeError::ScopeLimit {
                limit: MAX_SCOPE_VERSIONS,
            }))
        );
    }

    #[tokio::test]
    async fn scope_admits_permitted() {
        // The source gate must admit a permitted read; a gate that denied every
        // version would silently stop all replication. Roles bind to a real
        // user, so the nil principal of the default context cannot be used.
        let mut input = scope_input(ReplicateScopeTarget::Bucket);
        input.auth_context.user_id = UserId::local(Ulid::from_bytes([4u8; 16]), test_realm_id());
        let (_directory, authorization) = source_auth(&input, true).await;
        let mut op = ReplicateScopeOperation::new(input).with_source_authorization(authorization);
        op.state = super::ReplicateScopeState::IterateVersions;

        let effects = op.step(Event::Storage(StorageEvent::IterResult {
            values: vec![version_entry("dir/file.txt", Ulid::from_bytes([1u8; 16]))],
            next_start_after: None,
        }));

        assert_eq!(op.result.last_error, None);
        assert_eq!(op.result.failed, 0);
        assert!(matches!(effects[0], Effect::SubOperation(_)));
    }

    #[test]
    fn object_miss_does_not_fall_back_to_prefix_iteration() {
        let mut op = ReplicateScopeOperation::new(scope_input(ReplicateScopeTarget::Object {
            key: "dir/file".to_string(),
        }));

        op.start();
        op.step(Event::Storage(StorageEvent::ReadResult {
            key: b"bucket".to_vec().into(),
            value: Some(bucket_info().to_bytes().unwrap().into()),
        }));
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![1u8].into(),
            value: None,
        }));
        let Effect::Storage(StorageEffect::Iter { .. }) = &effects[0] else {
            panic!("expected exact iteration after object miss")
        };

        let effects = op.step(Event::Storage(StorageEvent::IterResult {
            values: vec![
                version_entry("dir/file-a", Ulid::generate()),
                version_entry("dir/file/b", Ulid::generate()),
                version_entry("dir/other", Ulid::generate()),
            ],
            next_start_after: None,
        }));

        assert!(effects.is_empty());
        assert_eq!(op.pending_versions.len(), 0);
        assert_eq!(op.result.replicated, 0);
        assert_eq!(op.result.skipped, 0);
        assert_eq!(op.result.failed, 0);
        assert_eq!(op.state, super::ReplicateScopeState::Finish);
    }

    #[test]
    fn scope_counts_bytes() {
        let mut op = ReplicateScopeOperation::new(scope_input(ReplicateScopeTarget::Bucket));
        op.state = super::ReplicateScopeState::RunVersionReplication;

        op.step(Event::SubOperation(
            SubOperationEvent::ReplicationItemResult {
                result: Ok(ReplicationSuboperationResult::ReplicatedBytes(42)),
            },
        ));

        assert_eq!(op.result.replicated, 1);
        assert_eq!(op.result.replicated_bytes, 42);
        assert_eq!(op.state, super::ReplicateScopeState::Finish);
    }

    #[test]
    fn advance_queue_cardinality() {
        let advance = ReferenceAdvance {
            generation: 2,
            predecessor: Ulid::generate(),
        };
        let mut denied = ReplicateScopeOperation::new(scope_input(ReplicateScopeTarget::Bucket))
            .with_reference_advance(advance);
        denied.result.failed = 1;
        denied.result.last_error = Some("source access denied".to_string());

        let effects = denied.run_next_replication();

        assert!(effects.is_empty());
        assert_eq!(denied.state, super::ReplicateScopeState::Finish);
        assert_eq!(denied.result.failed, 1);
        assert_eq!(
            denied.result.last_error.as_deref(),
            Some("source access denied")
        );
        assert_eq!(denied.output, Some(Ok(denied.result.clone())));

        let mut op = ReplicateScopeOperation::new(scope_input(ReplicateScopeTarget::Bucket))
            .with_reference_advance(advance);
        op.pending_versions = vec![
            version_request(Ulid::generate()),
            version_request(Ulid::generate()),
        ];

        let effects = op.run_next_replication();

        assert!(effects.is_empty());
        assert_eq!(op.state, super::ReplicateScopeState::Error);
        assert_eq!(
            op.output,
            Some(Err(ReplicateScopeError::ReplicateObjectVersionError(
                ReplicateObjectVersionError::InvalidReferenceAdvance,
            )))
        );
    }

    #[test]
    fn multipart_metadata_paginates_across_multiple_iter_pages() {
        let version_id = Ulid::generate();
        let mut op = ReplicateObjectVersionOperation::new(version_request(version_id));
        let location = materialized_location();

        let effects = op.start();
        assert!(matches!(
            effects[0],
            Effect::Storage(StorageEffect::Read { .. })
        ));

        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![1u8].into(),
            value: Some(
                materialized_blob_version(&location, None)
                    .to_bytes()
                    .unwrap()
                    .into(),
            ),
        }));
        assert!(matches!(
            effects[0],
            Effect::Storage(StorageEffect::Read { .. })
        ));

        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![2u8].into(),
            value: Some(location.to_bytes().unwrap().into()),
        }));
        assert!(matches!(
            effects[0],
            Effect::Storage(StorageEffect::Read { .. })
        ));

        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![3u8].into(),
            value: Some(
                MultipartObjectSummary {
                    checksum_type: MultipartChecksumType::Composite,
                    part_count: 3,
                    composite_hashes: Default::default(),
                }
                .to_bytes()
                .unwrap()
                .into(),
            ),
        }));
        let Effect::Storage(StorageEffect::Iter { start, .. }) = &effects[0] else {
            panic!("expected multipart iter request")
        };
        assert!(start.is_none());

        let next_cursor: aruna_core::types::Key = vec![9u8].into();
        let effects = op.step(Event::Storage(StorageEvent::IterResult {
            values: vec![
                multipart_part_entry(version_id, 2),
                multipart_part_entry(version_id, 1),
            ],
            next_start_after: Some(next_cursor.clone()),
        }));
        let Effect::Storage(StorageEffect::Iter { start, .. }) = &effects[0] else {
            panic!("expected paginated multipart iter request")
        };
        assert_eq!(start, &Some(IterStart::After(next_cursor.clone())));

        let effects = op.step(Event::Storage(StorageEvent::IterResult {
            values: vec![multipart_part_entry(version_id, 3)],
            next_start_after: None,
        }));
        assert!(matches!(
            effects[0],
            Effect::Storage(StorageEffect::Read { .. })
        ));
        assert_eq!(op.multipart_parts.len(), 3);
        assert_eq!(op.multipart_parts[0].part_number, 1);
        assert_eq!(op.multipart_parts[1].part_number, 2);
        assert_eq!(op.multipart_parts[2].part_number, 3);
    }

    #[test]
    fn multipart_metadata_rejects_incomplete_part_set() {
        let version_id = Ulid::generate();
        let mut op = ReplicateObjectVersionOperation::new(version_request(version_id));
        let location = materialized_location();

        op.start();
        op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![1u8].into(),
            value: Some(
                materialized_blob_version(&location, None)
                    .to_bytes()
                    .unwrap()
                    .into(),
            ),
        }));
        op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![2u8].into(),
            value: Some(location.to_bytes().unwrap().into()),
        }));
        op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![3u8].into(),
            value: Some(
                MultipartObjectSummary {
                    checksum_type: MultipartChecksumType::Composite,
                    part_count: 2,
                    composite_hashes: Default::default(),
                }
                .to_bytes()
                .unwrap()
                .into(),
            ),
        }));

        let effects = op.step(Event::Storage(StorageEvent::IterResult {
            values: vec![multipart_part_entry(version_id, 1)],
            next_start_after: None,
        }));
        assert!(effects.is_empty());
        assert_eq!(op.state, super::ReplicateObjectVersionState::Error);
        assert_eq!(
            op.result,
            Err(ReplicateObjectVersionError::MultipartPartCountMismatch {
                expected: 2,
                actual: 1,
            })
        );
    }

    #[test]
    fn manifest_includes_sender_current_pointer_generation() {
        let version_id = Ulid::generate();
        let generation = 42;
        let mut op = ReplicateObjectVersionOperation::new(version_request(version_id));
        op.replication_version = Some(ReplicationVersion::Deleted {
            created_at: SystemTime::now(),
            created_by: test_user_id(),
        });

        op.build_manifest(Some(CurrentVersionPointer::new_with_generation(
            version_id, generation,
        )))
        .unwrap();

        let manifest = op.manifest.expect("manifest built");
        assert!(manifest.current_version);
        assert_eq!(manifest.current_version_generation, Some(generation));
    }

    #[test]
    fn advance_scope_lineage() {
        // A later head must not relabel the durable successor obligation.
        let version_id = Ulid::generate();
        let advance = ReferenceAdvance {
            generation: 42,
            predecessor: Ulid::generate(),
        };
        let mut input = scope_input(ReplicateScopeTarget::Version {
            key: "dir/file.txt".to_string(),
            version_id,
        });
        input.auth_context.path_restrictions = Some(vec![PathRestriction {
            pattern: "/realm/g/group/bucket/dir/file.txt".to_string(),
            permission: Permission::READ,
        }]);
        let scoped_auth = input.auth_context.clone();
        let persisted = reference_cached_metadata();
        let mut scope = ReplicateScopeOperation::new(input).with_reference_advance(advance);
        scope.source_group_id = Some(Ulid::generate());
        let mut sync = reference_sync();
        sync.origin = Some(SyncOrigin {
            relationship_id: sync.relationship_id,
            hop_count: 0,
        });
        scope.sync = Some(sync);
        scope
            .enqueue_version_request(VersionKey::new("bucket", "dir/file.txt", version_id))
            .unwrap();

        let mut effects = scope.run_next_replication();
        let Effect::SubOperation(mut operation) = effects.pop().expect("version suboperation")
        else {
            panic!("expected version suboperation")
        };
        assert!(scope.reference_advance.is_none());

        operation.start();
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![1u8].into(),
            value: Some(reference_blob_version().to_bytes().unwrap().into()),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { key_space, .. })]
                if key_space == BLOB_HEAD_KEYSPACE
        ));
        let newer =
            CurrentVersionPointer::new_with_generation(Ulid::generate(), advance.generation + 1);
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![2u8].into(),
            value: Some(newer.to_bytes().unwrap().into()),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::OpenConnection { .. })]
        ));

        let effects = operation.step(Event::Blob(BlobEvent::ConnectionEstablished {
            stream_id: Ulid::generate(),
        }));
        let [Effect::Blob(BlobEffect::SendMessage { payload, .. })] = effects.as_slice() else {
            panic!("expected manifest message")
        };
        let VersionReplicationMessage::ReferenceAdvance {
            manifest,
            advance: wire_advance,
        } = VersionReplicationMessage::from_bytes(payload).unwrap()
        else {
            panic!("expected reference advance")
        };

        assert!(manifest.current_version);
        assert_eq!(
            manifest.current_version_generation,
            Some(advance.generation)
        );
        assert_eq!(manifest.auth_context, scoped_auth);
        assert_eq!(manifest.writer_auth_context, None);
        assert_eq!(manifest.reference_metadata, Some(persisted));
        assert_eq!(wire_advance, advance);
        assert_eq!(manifest.reference_advance, Some(advance));
    }

    // Normal reference replication carries the stored cap, so repair and snapshot
    // transfers cannot hand the target a fresh advance budget.
    #[test]
    fn manifest_carries_count() {
        let mut op = ReplicateObjectVersionOperation::new(version_request(Ulid::generate()))
            .with_sync(reference_sync());
        op.preserve_reference = true;
        op.replication_version = Some(ReplicationVersion::Reference {
            created_at: SystemTime::now(),
            created_by: test_user_id(),
            source: reference_source_binding(),
            cached_metadata: reference_cached_metadata(),
            last_refresh: SystemTime::now(),
            metadata: HashMap::new(),
            advance_count: 5,
        });

        op.build_manifest(None).unwrap();

        assert_eq!(op.manifest.unwrap().reference_advance_count, Some(5));
    }

    #[test]
    fn advance_rejects_nonreference() {
        let advance = ReferenceAdvance {
            generation: 2,
            predecessor: Ulid::generate(),
        };
        let mut materialized =
            ReplicateObjectVersionOperation::new(version_request(Ulid::generate()))
                .with_sync(reference_sync())
                .with_reference_advance(advance);
        materialized.preserve_reference = true;
        materialized.replication_version = Some(ReplicationVersion::Materialized {
            created_at: SystemTime::now(),
            created_by: test_user_id(),
            location: materialized_location(),
            source: None,
            metadata: HashMap::new(),
        });
        assert_eq!(
            materialized.build_manifest(None),
            Err(ReplicateObjectVersionError::InvalidReferenceAdvance)
        );

        materialized.multipart_summary = Some(MultipartObjectSummary {
            checksum_type: MultipartChecksumType::Composite,
            part_count: 0,
            composite_hashes: HashMap::new(),
        });
        assert_eq!(
            materialized.build_manifest(None),
            Err(ReplicateObjectVersionError::InvalidReferenceAdvance)
        );

        let mut deleted = ReplicateObjectVersionOperation::new(version_request(Ulid::generate()))
            .with_sync(reference_sync())
            .with_reference_advance(advance);
        deleted.preserve_reference = true;
        deleted.replication_version = Some(ReplicationVersion::Deleted {
            created_at: SystemTime::now(),
            created_by: test_user_id(),
        });
        assert_eq!(
            deleted.build_manifest(None),
            Err(ReplicateObjectVersionError::InvalidReferenceAdvance)
        );
    }

    #[test]
    fn manifest_writer_auth() {
        let version_id = Ulid::generate();
        let writer = auth_context();
        let mut op = ReplicateObjectVersionOperation::new(version_request(version_id))
            .with_writer_auth(writer.clone());
        op.replication_version = Some(ReplicationVersion::Deleted {
            created_at: SystemTime::now(),
            created_by: test_user_id(),
        });

        op.build_manifest(None).unwrap();

        assert_eq!(op.manifest.unwrap().writer_auth_context, Some(writer));
    }

    #[test]
    fn manifest_includes_source_binding_for_materialized_version() {
        let version_id = Ulid::generate();
        let source = reference_source_binding();
        let metadata = HashMap::from([("mtime".to_string(), "1753272000.123456789".to_string())]);
        let mut op = ReplicateObjectVersionOperation::new(version_request(version_id));
        op.replication_version = Some(ReplicationVersion::Materialized {
            created_at: SystemTime::now(),
            created_by: test_user_id(),
            location: materialized_location(),
            source: Some(source.clone()),
            metadata: metadata.clone(),
        });

        op.build_manifest(None).unwrap();

        let manifest = op.manifest.expect("manifest built");
        assert_eq!(
            manifest.kind,
            aruna_core::structs::ReplicationItemKind::Materialized
        );
        assert_eq!(manifest.source, Some(source));
        assert_eq!(manifest.metadata, metadata);
    }

    #[test]
    fn builds_reference_manifest() {
        let version_id = Ulid::generate();
        let sync = reference_sync();
        let location = materialized_location();
        let source_node_id = sync.source_node_id;
        let relationship_id = sync.relationship_id;
        let created_at = SystemTime::now();
        let mut op =
            ReplicateObjectVersionOperation::new(version_request(version_id)).with_sync(sync);
        op.replication_version = Some(ReplicationVersion::Materialized {
            created_at,
            created_by: test_user_id(),
            location: location.clone(),
            source: None,
            metadata: HashMap::new(),
        });

        op.build_manifest(None).unwrap();

        let manifest = op.manifest.expect("manifest built");
        assert!(manifest.reference_intent);
        assert!(manifest.blob.is_none());
        assert_eq!(manifest.bucket, "target-bucket");
        assert_eq!(manifest.key, "copied/file.txt");
        assert_eq!(
            manifest.reference_metadata,
            Some(SourceMetadata {
                content_length: location.blob_size,
                content_type: None,
                etag: None,
                last_modified: Some(created_at),
                source_version: None,
            })
        );
        // A native version exported as a reference is a fresh binding at the target.
        assert_eq!(manifest.reference_advance_count, Some(0));
        let source = manifest.source.expect("reference binding");
        assert_eq!(source.strategy, StagingStrategy::Reference);
        assert_eq!(source.connector_id, None);
        assert_eq!(source.descriptor.kind, SourceConnectorKind::ArunaNative);
        assert_eq!(source.descriptor.origin_node_id, Some(source_node_id));
        assert_eq!(source.descriptor.source_path, "bucket/dir/file.txt");
        assert_eq!(
            source.descriptor.version_selector.as_deref(),
            Some(format!("version:{version_id}").as_str())
        );
        assert_eq!(
            source.descriptor.public_config.get("relationship_id"),
            Some(&relationship_id.to_string())
        );
    }

    // The target heads this native source on its first read and compares
    // fingerprints, so the replicated observation must be the one that head
    // returns: derived from the version, not from the shared location row.
    #[test]
    fn observation_matches_head() {
        let version_id = Ulid::generate();
        let location = materialized_location();
        let created_at = SystemTime::now();
        let mut op = ReplicateObjectVersionOperation::new(version_request(version_id))
            .with_sync(reference_sync());
        op.replication_version = Some(ReplicationVersion::Materialized {
            created_at,
            created_by: test_user_id(),
            location: location.clone(),
            source: None,
            metadata: HashMap::new(),
        });

        op.build_manifest(None).unwrap();

        // What the origin's native head reports for a materialized version;
        // source_version is transient and excluded from the fingerprint.
        let head = SourceMetadata {
            content_length: location.blob_size,
            content_type: None,
            etag: None,
            last_modified: Some(created_at),
            source_version: Some(version_id.to_string()),
        };
        let replicated = op
            .manifest
            .expect("manifest built")
            .reference_metadata
            .expect("reference observation");
        assert_eq!(
            replicated.observation_fingerprint(),
            head.observation_fingerprint()
        );
        assert_ne!(location.created_at, created_at);
    }

    #[test]
    fn manifest_omits_source_binding_for_delete_marker() {
        let version_id = Ulid::generate();
        let mut op = ReplicateObjectVersionOperation::new(version_request(version_id));
        op.replication_version = Some(ReplicationVersion::Deleted {
            created_at: SystemTime::now(),
            created_by: test_user_id(),
        });

        op.build_manifest(None).unwrap();

        let manifest = op.manifest.expect("manifest built");
        assert_eq!(
            manifest.kind,
            aruna_core::structs::ReplicationItemKind::DeleteMarker
        );
        assert_eq!(manifest.source, None);
    }

    #[test]
    fn keeps_reference_deletes() {
        let version_id = Ulid::generate();
        let mut op = ReplicateObjectVersionOperation::new(version_request(version_id))
            .with_sync(reference_sync());
        op.replication_version = Some(ReplicationVersion::Deleted {
            created_at: SystemTime::now(),
            created_by: test_user_id(),
        });

        op.build_manifest(None).unwrap();

        let manifest = op.manifest.expect("manifest built");
        assert_eq!(manifest.kind, ReplicationItemKind::DeleteMarker);
        assert!(manifest.blob.is_none());
        assert!(manifest.source.is_none());
        assert!(manifest.reference_metadata.is_none());
    }

    #[test]
    fn manifest_rejects_unresolved_reference_version() {
        let version_id = Ulid::generate();
        let source = reference_source_binding();
        let cached_metadata = reference_cached_metadata();
        let created_at = SystemTime::now();
        let created_by = test_user_id();
        let last_refresh = SystemTime::now();
        let mut op = ReplicateObjectVersionOperation::new(version_request(version_id));
        op.replication_version = Some(ReplicationVersion::Reference {
            created_at,
            created_by,
            source,
            cached_metadata,
            last_refresh,
            metadata: HashMap::new(),
            advance_count: 0,
        });

        assert_eq!(
            op.build_manifest(None),
            Err(ReplicateObjectVersionError::UnresolvedReferenceVersion)
        );
    }

    #[test]
    fn preserves_reference_source() {
        let version_id = Ulid::generate();
        let cached_metadata = reference_cached_metadata();
        let mut op = ReplicateObjectVersionOperation::new(version_request_with_mode(
            version_id,
            ReplicationMode::OnDemand,
        ))
        .with_sync(reference_sync());

        op.start();
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![1u8].into(),
            value: Some(reference_blob_version().to_bytes().unwrap().into()),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { key_space, .. })]
                if key_space == BLOB_HEAD_KEYSPACE
        ));
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![2u8].into(),
            value: None,
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::OpenConnection { .. })]
        ));
        assert!(matches!(
            op.replication_version,
            Some(ReplicationVersion::Reference { .. })
        ));

        let manifest = op.manifest.expect("manifest built");
        assert!(manifest.blob.is_none());
        assert_eq!(manifest.reference_metadata, Some(cached_metadata));
        assert_eq!(
            manifest.source.expect("reference binding").descriptor.kind,
            SourceConnectorKind::ArunaNative
        );
    }

    #[test]
    fn stale_reference_skips() {
        let version_id = Ulid::generate();
        let mut sync = reference_sync();
        sync.reference_intent = false;
        let mut op = ReplicateObjectVersionOperation::new(version_request_with_mode(
            version_id,
            ReplicationMode::OnDemand,
        ))
        .with_sync(sync);

        op.start();
        op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![1u8].into(),
            value: Some(reference_blob_version().to_bytes().unwrap().into()),
        }));
        let access = ResolvedSourceAccess::OpenDal {
            kind: SourceConnectorKind::Http,
            config: HashMap::new(),
            path: "ref/file.txt".to_string(),
            version: None,
        };
        op.step(Event::SubOperation(
            SubOperationEvent::VersionSourceAccessResolved { result: Ok(access) },
        ));
        let mut stale = reference_cached_metadata();
        stale.etag = Some("etag-2".to_string());
        let effects = op.step(Event::StagingSource(StagingSourceEvent::HeadResult {
            metadata: stale,
        }));

        assert!(effects.is_empty());
        assert_eq!(op.state, super::ReplicateObjectVersionState::Finish);
        assert_eq!(
            op.finalize(),
            Ok(Ok(ReplicationSuboperationResult::Skipped))
        );
    }

    #[test]
    fn read_drift_skips() {
        let version_id = Ulid::generate();
        let mut sync = reference_sync();
        sync.reference_intent = false;
        let mut op = ReplicateObjectVersionOperation::new(version_request_with_mode(
            version_id,
            ReplicationMode::OnDemand,
        ))
        .with_sync(sync);

        op.start();
        op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![1u8].into(),
            value: Some(reference_blob_version().to_bytes().unwrap().into()),
        }));
        let access = ResolvedSourceAccess::OpenDal {
            kind: SourceConnectorKind::Http,
            config: HashMap::new(),
            path: "ref/file.txt".to_string(),
            version: None,
        };
        op.step(Event::SubOperation(
            SubOperationEvent::VersionSourceAccessResolved { result: Ok(access) },
        ));
        let metadata = reference_cached_metadata();
        op.step(Event::StagingSource(StagingSourceEvent::HeadResult {
            metadata: metadata.clone(),
        }));
        op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![2u8].into(),
            value: None,
        }));
        load_routing(&mut op);

        let mut stale = metadata;
        stale.etag = Some("etag-2".to_string());
        let effects = op.step(Event::StagingSource(StagingSourceEvent::ReadResult {
            metadata: stale,
            stream: BackendStream::new(stream::empty::<Result<Bytes, std::io::Error>>()),
        }));

        assert!(effects.is_empty());
        assert_eq!(op.state, super::ReplicateObjectVersionState::Finish);
        assert_eq!(
            op.finalize(),
            Ok(Ok(ReplicationSuboperationResult::Skipped))
        );
    }

    #[test]
    fn reference_versions_are_skipped_without_replication_manifest() {
        let version_id = Ulid::generate();
        let mut op = ReplicateObjectVersionOperation::new(version_request(version_id));

        op.start();
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![1u8].into(),
            value: Some(reference_blob_version().to_bytes().unwrap().into()),
        }));

        assert!(effects.is_empty());
        assert_eq!(op.state, super::ReplicateObjectVersionState::Finish);
        assert_eq!(
            op.finalize(),
            Ok(Ok(ReplicationSuboperationResult::Skipped))
        );
    }

    #[test]
    fn fails_unreadable_rules() {
        // A storage failure reading the bucket record must fail the write, not
        // reroute it to the node default.
        let mut op = ReplicateObjectVersionOperation::new(version_request_with_mode(
            Ulid::generate(),
            ReplicationMode::OnDemand,
        ));
        op.start();
        op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![1u8].into(),
            value: Some(reference_blob_version().to_bytes().unwrap().into()),
        }));
        op.step(Event::SubOperation(
            SubOperationEvent::VersionSourceAccessResolved {
                result: Ok(ResolvedSourceAccess::OpenDal {
                    kind: SourceConnectorKind::Http,
                    config: HashMap::new(),
                    path: "ref/file.txt".to_string(),
                    version: None,
                }),
            },
        ));
        op.step(Event::SubOperation(SubOperationEvent::GroupRoutingLoaded {
            result: Ok(GroupRoutingInputs::default()),
        }));

        op.step(Event::Storage(StorageEvent::Error {
            error: aruna_core::errors::StorageError::ReadError("boom".to_string()),
        }));

        assert!(matches!(
            op.finalize(),
            Err(ReplicateObjectVersionError::BucketRulesFailed(_))
        ));
    }

    #[test]
    fn on_demand_reference_replication_materializes_before_manifest() {
        let version_id = Ulid::generate();
        let original_source = Some(reference_source_binding());
        let mut op = ReplicateObjectVersionOperation::new(version_request_with_mode(
            version_id,
            ReplicationMode::OnDemand,
        ));

        op.start();
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![1u8].into(),
            value: Some(reference_blob_version().to_bytes().unwrap().into()),
        }));
        assert!(matches!(effects.as_slice(), [Effect::SubOperation(_)]));

        let access = ResolvedSourceAccess::OpenDal {
            kind: SourceConnectorKind::Http,
            config: HashMap::from([("endpoint".to_string(), "https://example.org".to_string())]),
            path: "ref/file.txt".to_string(),
            version: None,
        };
        let effects = op.step(Event::SubOperation(
            SubOperationEvent::VersionSourceAccessResolved {
                result: Ok(access.clone()),
            },
        ));
        assert!(matches!(effects.as_slice(), [Effect::SubOperation(_)]));
        let effects = load_routing(&mut op);
        assert!(matches!(
            effects.as_slice(),
            [Effect::StagingSource(StagingSourceEffect::Read { access: emitted, range })]
                if emitted == &access && range.is_none()
        ));

        let effects = op.step(Event::StagingSource(StagingSourceEvent::ReadResult {
            metadata: reference_cached_metadata(),
            stream: BackendStream::new(stream::iter(vec![Ok::<Bytes, std::io::Error>(
                Bytes::from_static(b"abc"),
            )])),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::Write { bucket, key, .. })]
                if bucket == "bucket" && key == "dir/file.txt"
        ));

        let effects = op.step(Event::Blob(BlobEvent::WriteFinished {
            location: materialized_location(),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { key_space, .. })]
                if key_space == BLOB_HEAD_KEYSPACE
        ));
        assert!(matches!(
            op.replication_version.as_ref(),
            Some(ReplicationVersion::Materialized { .. })
        ));
        assert!(matches!(
            op.replication_version.as_ref(),
            Some(ReplicationVersion::Materialized { source, .. }) if source.as_ref() == original_source.as_ref()
        ));

        op.build_manifest(None).unwrap();
        assert_eq!(op.manifest.as_ref().unwrap().source, original_source);
    }

    #[test]
    fn cleanup_deletes_blob() {
        // A materialization that fails after the backend wrote data must still
        // surrender that location, or the partial blob leaks.
        let mut op = ReplicateObjectVersionOperation::new(version_request_with_mode(
            Ulid::generate(),
            ReplicationMode::OnDemand,
        ));

        op.start();
        op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![1u8].into(),
            value: Some(reference_blob_version().to_bytes().unwrap().into()),
        }));
        op.step(Event::SubOperation(
            SubOperationEvent::VersionSourceAccessResolved {
                result: Ok(ResolvedSourceAccess::OpenDal {
                    kind: SourceConnectorKind::Http,
                    config: HashMap::from([(
                        "endpoint".to_string(),
                        "https://example.org".to_string(),
                    )]),
                    path: "ref/file.txt".to_string(),
                    version: None,
                }),
            },
        ));
        load_routing(&mut op);
        op.step(Event::StagingSource(StagingSourceEvent::ReadResult {
            metadata: reference_cached_metadata(),
            stream: BackendStream::new(stream::iter(vec![Ok::<Bytes, std::io::Error>(
                Bytes::from_static(b"abc"),
            )])),
        }));

        let temp_location = materialized_location();
        let effects = op.step(Event::Blob(BlobEvent::Error(BlobError::WriteCleanup {
            location: temp_location.clone(),
            message: "backend writer failed".to_string(),
        })));

        assert_eq!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::Delete {
                location: temp_location
            })]
        );
        assert!(op.is_complete());
        assert!(op.finalize().is_err());
    }

    #[test]
    fn on_demand_reference_replication_cleans_up_temporary_blob_after_apply() {
        let version_id = Ulid::generate();
        let mut op = ReplicateObjectVersionOperation::new(version_request_with_mode(
            version_id,
            ReplicationMode::OnDemand,
        ));

        op.start();
        op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![1u8].into(),
            value: Some(reference_blob_version().to_bytes().unwrap().into()),
        }));

        let access = ResolvedSourceAccess::OpenDal {
            kind: SourceConnectorKind::Http,
            config: HashMap::from([("endpoint".to_string(), "https://example.org".to_string())]),
            path: "ref/file.txt".to_string(),
            version: None,
        };
        op.step(Event::SubOperation(
            SubOperationEvent::VersionSourceAccessResolved { result: Ok(access) },
        ));
        load_routing(&mut op);
        op.step(Event::StagingSource(StagingSourceEvent::ReadResult {
            metadata: reference_cached_metadata(),
            stream: BackendStream::new(stream::iter(vec![Ok::<Bytes, std::io::Error>(
                Bytes::from_static(b"abc"),
            )])),
        }));

        let temp_location = materialized_location();
        op.step(Event::Blob(BlobEvent::WriteFinished {
            location: temp_location.clone(),
        }));
        op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![2u8].into(),
            value: None,
        }));
        op.step(Event::Blob(BlobEvent::ConnectionEstablished {
            stream_id: Ulid::generate(),
        }));
        op.step(Event::Blob(BlobEvent::MessageSent {
            stream_id: op.stream_id.expect("stream id available"),
        }));

        let negotiation = VersionReplicationMessage::VersionNegotiationResponse(
            ReplicationNegotiationResult::NeedBlobAndVersion,
        )
        .to_bytes()
        .unwrap();
        op.step(Event::Blob(BlobEvent::MessageReceived {
            stream_id: op.stream_id.expect("stream id available"),
            payload: negotiation,
        }));
        op.step(Event::Blob(BlobEvent::ReplicationFinished {
            location: temp_location.clone(),
        }));

        let apply_complete = VersionReplicationMessage::VersionApplyComplete
            .to_bytes()
            .unwrap();
        let effects = op.step(Event::Blob(BlobEvent::MessageReceived {
            stream_id: op.stream_id.expect("stream id available"),
            payload: apply_complete,
        }));

        assert_eq!(
            op.state,
            super::ReplicateObjectVersionState::CleanupReferenceBlob
        );
        assert_eq!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::Delete {
                location: temp_location
            })]
        );
        assert_eq!(
            op.result,
            Ok(ReplicationSuboperationResult::ReplicatedBytes(42))
        );
    }
}
