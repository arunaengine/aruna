//! Pushes object versions to target nodes and picks the scope of objects to replicate.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::connectors::resolver::{NATIVE_SET_ID, ResolveBindingInput, resolve_binding_effect};
use crate::connectors::root_record::{hydrated, named_root, parse_root, read_root};
use crate::replication::error::ReplicationError;
use crate::s3::object::put::abe::keyed;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect, StagingSourceEffect, StorageEffect};
use aruna_core::errors::StorageReadError;
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event, StagingSourceEvent, SubOperationEvent};
use aruna_core::id::NodeId;
use aruna_core::id_mint::IdMint;
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE,
    BUCKET_ENCRYPTION_KEYSPACE, OBJECT_METADATA_KEYSPACE, PENDING_LOCATION_KEYSPACE,
    S3_BUCKET_KEYSPACE, SYNC_REFERENCE_KEYSPACE,
};
use aruna_core::managed_copy::{
    CopyRequest, serve_reads, split_serve_reads, validate_registration,
};
use aruna_core::native_reference::NATIVE_RELATIONSHIP_ID;
use aruna_core::operation::Operation;
use aruna_core::replication_protocol::{
    MaterializedBlobInfo, MultipartObjectMetadata, VersionReplicationManifest,
    VersionReplicationMessage, VersionReplicationRequest,
};
use aruna_core::request_policy::{PolicyRequestExtras, policy_request_with};
use aruna_core::rows::read_row;
use aruna_core::structs::checksum::unmatched_checksum;
use aruna_core::structs::execution::source_access::{
    ObservationMatch, ResolvedSourceAccess, SourceMetadata,
};
use aruna_core::structs::execution::source_connector::SourceConnectorKind;
use aruna_core::structs::execution::staging::{
    PortableSourceDescriptor, StagingStrategy, VersionSourceBinding,
};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::placement::policy::PlacementPolicyRef;
use aruna_core::structs::storage::blob::{
    ArchiveKey, BackendLocation, BlobHeadKey, BlobLocationKey, BlobVersion, BlobVersionState,
    BucketInfo, CurrentVersionPointer, ManagedCopyKey, VersionKey, object_permission_path,
};
use aruna_core::structs::storage::bucket_metadata::strip_release_keys;
use aruna_core::structs::storage::distribution::OriginalEpoch;
use aruna_core::structs::storage::distribution::delivery::Publication;
use aruna_core::structs::storage::encryption::{BucketEncryption, BucketKeyError, BucketKeyRef};
use aruna_core::structs::storage::format::Compression;
use aruna_core::structs::storage::multipart::{
    MultipartObjectKey, MultipartObjectPart, MultipartObjectSummary,
};
use aruna_core::structs::storage::origin::{SyncHop, VersionOrigin};
use aruna_core::structs::storage::replication::{
    ArunaArn, ReplicationFailure, ReplicationItemError, ReplicationItemKind,
    ReplicationNegotiationResult, ReplicationSuboperationResult,
};
use aruna_core::structs::storage::replication_queue::{
    ReferenceAdvance, ReplicationMode, SyncOrigin,
};
use aruna_core::structs::storage::replication_queue::{ReplicateScopeInput, ReplicateScopeTarget};
use aruna_core::structs::storage::routing::{GroupRoutingInputs, RoutingError};
use aruna_core::structs::storage::routing::{NodeRouting, StorageRoutingRule, resolve_backend};
use aruna_core::structs::sync_relationship::{
    ReferenceHandling, SyncMode, SyncRelationship, sync_state_key,
};
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::types::{GroupId, Key, Value};
use aruna_core::user::id::UserId;
use aruna_ops_identity::auth::enforcement::PolicyEvaluator;
use aruna_ops_identity::auth::permission_rules::PermissionRules;
use aruna_ops_identity::groups::backends::RecordReadError;
use aruna_ops_identity::groups::storage_routing::load_group_inputs;
use aruna_ops_identity::placement::policy::gate::{
    GateContext, PolicyGateError, PolicyGateOutcome, gate_decision, write_gate,
};
use aruna_ops_identity::placement::policy::read::ReadPolicyError;
use serde::Serialize;
use std::collections::{BTreeSet, HashMap};
use std::time::SystemTime;
use thiserror::Error;
use tracing::debug;
use ulid::Ulid;

const ITER_PAGE_SIZE: usize = 512;
const MAX_SCOPE_VERSIONS: usize = 1024;

#[derive(Debug, Error, PartialEq)]
pub enum SourceAuthorizationError {
    #[error("source access denied")]
    Denied,
    #[error("source authorization unavailable: {0}")]
    Unavailable(String),
}

pub struct SourceAuthorization {
    pub(super) group_id: GroupId,
    pub(super) source_node_id: NodeId,
    pub(super) auth_context: AuthContext,
    pub(super) permissions: PermissionRules,
    pub(super) policies: PolicyEvaluator,
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
    pub fn group_id(&self) -> GroupId {
        self.group_id
    }

    fn allows(&self, bucket: &str, key: &str) -> Result<(), SourceAuthorizationError> {
        let path = object_permission_path(
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
            Some(&self.auth_context),
            PolicyRequestExtras::operation("s3.GetObject"),
        );
        self.policies
            .evaluate(&request)
            .map_err(|_| SourceAuthorizationError::Denied)
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
        created_by: aruna_core::user::id::UserId,
        location: BackendLocation,
        source: Option<VersionSourceBinding>,
        metadata: HashMap<String, String>,
    },
    Reference {
        created_at: SystemTime,
        created_by: aruna_core::user::id::UserId,
        source: VersionSourceBinding,
        cached_metadata: SourceMetadata,
        last_refresh: SystemTime,
        metadata: HashMap<String, String>,
        advance_count: u16,
    },
    Deleted {
        created_at: SystemTime,
        created_by: aruna_core::user::id::UserId,
        /// The marker's tags, such as the device version a sync delete came from.
        metadata: HashMap<String, String>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PendingMaterializedVersion {
    created_at: SystemTime,
    created_by: aruna_core::user::id::UserId,
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
    /// A coordinator's publication to one member of a distributed bucket.
    distribution: Option<Publication>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicateScopeResult {
    pub replicated: u64,
    pub replicated_bytes: u64,
    pub skipped: u64,
    pub failed: u64,
    /// Human-readable reason of the last failed item, for the external boundary.
    pub last_error: Option<String>,
    /// Stable category of the last failed item, for retry/terminal policy.
    pub failure: Option<ReplicationFailure>,
    /// Source keys locked here that items wait for; they run again after an unlock.
    pub awaiting: Vec<BucketKeyRef>,
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
    ReplicateObjectError(#[from] ReplicateObjectError),
    #[error("Source bucket not found")]
    BucketNotFound,
    #[error("replication scope exceeds {limit} versions")]
    ScopeLimit { limit: usize },
    #[error("operation did not finish")]
    NotFinished,
    #[error("Unexpected event in state {state}: expected {expected}, got {received}")]
    InvalidStateEvent {
        state: &'static str,
        expected: &'static str,
        received: &'static str,
    },
}

impl ReplicateScopeError {
    /// The stable failure category of a scope-level error. Only a peer
    /// rejection carries one through; every other scope error is retryable.
    pub fn failure(&self) -> ReplicationFailure {
        match self {
            Self::ReplicateObjectError(error) => error.failure_category(),
            Self::ReplicationError(ReplicationError::ReplicationRejected(reason)) => {
                ReplicationItemError::from_peer_reason(reason).failure
            }
            _ => ReplicationFailure::Other,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ReplicateScopeState {
    Init,
    ReadBucket,
    ResolveObjectTarget,
    ReadSingleVersion,
    IterateVersions,
    RunVersionReplication,
}

type ScopeTurn = Result<Next<ReplicateScopeResult>, ReplicateScopeError>;
type ObjectTurn = Result<Next<ReplicationSuboperationResult>, ReplicateObjectError>;

fn blob_next<T>(effect: BlobEffect) -> Next<T> {
    Next::Call(Effect::blob(effect).into())
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
    /// Destination details of this node, passed to every version sub-operation
    /// that may materialize reference bytes here.
    gate_context: Option<GateContext>,
    result: ReplicateScopeResult,
    /// A holder's permitted plaintext request, checked by the caller before each run.
    plaintext: bool,
}

impl From<IoError> for ReplicateScopeError {
    fn from(error: IoError) -> Self {
        Self::StorageError(error.into())
    }
}

impl Operation for ReplicateScopeOperation {
    type Output = ReplicateScopeResult;
    type Error = ReplicateScopeError;

    fn first(&mut self) -> ScopeTurn {
        self.read_bucket()
    }

    fn next(&mut self, result: IoResult) -> ScopeTurn {
        match self.state {
            ReplicateScopeState::Init => self.read_bucket(),
            ReplicateScopeState::ReadBucket => self.accept_bucket(result.value()?),
            ReplicateScopeState::ResolveObjectTarget => self.accept_object(result.value()?),
            ReplicateScopeState::ReadSingleVersion => self.accept_single(result.value()?),
            ReplicateScopeState::IterateVersions => self.accept_page(result.page()?),
            ReplicateScopeState::RunVersionReplication => {
                let item = result.child::<ReplicationSuboperationResult, ReplicateObjectError>()?;
                self.accept_item(item.map_err(|error| ReplicationItemError {
                    failure: error.failure_category(),
                    message: error.to_string(),
                }))
            }
        }
    }
}

impl ReplicateScopeOperation {
    fn accept_bucket(&mut self, value: Option<Value>) -> ScopeTurn {
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

    fn accept_object(&mut self, value: Option<Value>) -> ScopeTurn {
        let ReplicateScopeTarget::Object { key } = &self.input.target else {
            return self.fail(ReplicateScopeError::NotFinished);
        };
        let key = key.clone();
        debug!(
            bucket = %self.input.bucket,
            key = %key,
            target_node = %self.input.target_node_id,
            current_version_exists = value.is_some(),
            "Resolved exact object replication target"
        );
        self.start_iteration(Some(key), true)
    }

    fn accept_single(&mut self, value: Option<Value>) -> ScopeTurn {
        let ReplicateScopeTarget::Version { key, version_id } = &self.input.target else {
            return self.fail(ReplicateScopeError::NotFinished);
        };
        let version_key = VersionKey::new(&self.input.bucket, key, *version_id);
        if let Some(value) = value {
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

    fn accept_page(&mut self, (values, next): (Vec<(Key, Value)>, Option<Key>)) -> ScopeTurn {
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
            next_page = next.is_some(),
            "Processed replication iteration page"
        );

        if let Some(cursor) = next {
            self.next_start_after = Some(cursor);
            self.request_iteration_page()
        } else {
            self.run_next_replication()
        }
    }

    fn accept_item(
        &mut self,
        result: Result<ReplicationSuboperationResult, ReplicationItemError>,
    ) -> ScopeTurn {
        match &result {
            Ok(ReplicationSuboperationResult::Replicated) => self.result.replicated += 1,
            Ok(ReplicationSuboperationResult::Skipped) => self.result.skipped += 1,
            Ok(ReplicationSuboperationResult::AwaitingKey(key)) => {
                if !self.result.awaiting.contains(key) {
                    self.result.awaiting.push(*key);
                }
            }
            Ok(ReplicationSuboperationResult::ReplicatedBytes(bytes)) => {
                self.result.replicated = self.result.replicated.saturating_add(1);
                self.result.replicated_bytes = self.result.replicated_bytes.saturating_add(*bytes);
            }
            Err(error) => {
                self.result.failed += 1;
                self.result.failure = Some(error.failure);
                self.result.last_error = Some(error.message.clone());
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
}

#[derive(Debug, Error, PartialEq)]
pub enum ReplicateObjectError {
    #[error(transparent)]
    ManagedCopy(#[from] aruna_core::managed_copy::ManagedCopyError),
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
    #[error("source bytes do not match the recorded {0} digest")]
    DigestMismatch(&'static str),
    #[error("encrypted versions are not replicated")]
    EncryptedVersion,
    #[error("Multipart metadata incomplete: expected {expected} parts, found {actual}")]
    PartCountMismatch { expected: usize, actual: usize },
    #[error("operation did not finish")]
    NotFinished,
    #[error("Unexpected event in state {state}: expected {expected}, got {received}")]
    InvalidStateEvent {
        state: &'static str,
        expected: &'static str,
        received: &'static str,
    },
    #[error("plaintext_required")]
    PlaintextRefused,
    #[error("the source version waits for its content hash")]
    PendingSource,
    #[error(transparent)]
    Blob(#[from] BlobError),
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
                failure: None,
                awaiting: Vec::new(),
            },
            plaintext: false,
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

    pub fn with_source_authorization(mut self, authorization: SourceAuthorization) -> Self {
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

    /// Lets encrypted source items reach a plain target as plaintext. Only for a requester
    /// who is a current key holder of the source bucket.
    pub fn with_plaintext(mut self, plaintext: bool) -> Self {
        self.plaintext = plaintext;
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
            distribution: None,
        });
        self
    }

    /// Maps one exact source version to an unrelated node-local destination.
    pub fn with_destination(
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
            distribution: None,
        });
        self
    }

    /// Publishes the source version to one member of a distributed bucket under the same
    /// bucket name and key: bytes only for a `copy` replica, its record otherwise, delete
    /// markers always, and references with their own source.
    pub fn with_distribution(mut self, publication: Publication, local: NodeId) -> Self {
        self.input.replicate_delete_markers = true;
        self.sync = Some(SyncTransferContext {
            target_bucket: self.input.bucket.clone(),
            source_prefix: None,
            target_prefix: None,
            source_node_id: local,
            relationship_id: Ulid::nil(),
            reference_intent: !publication.bytes,
            reference_handling: ReferenceHandling::Preserve,
            origin: None,
            upstream_sources: Vec::new(),
            writer_auth_context: None,
            distribution: Some(publication),
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
        }
    }

    fn fail(&self, err: ReplicateScopeError) -> ScopeTurn {
        debug!(
            bucket = %self.input.bucket,
            target = ?self.input.target,
            target_node = %self.input.target_node_id,
            state = %self.state_name(),
            error = %err,
            "Scope replication failed"
        );
        Err(err)
    }

    fn read_bucket(&mut self) -> ScopeTurn {
        debug!(
            bucket = %self.input.bucket,
            target = ?self.input.target,
            target_node = %self.input.target_node_id,
            mode = ?self.input.mode,
            "Entering ReadBucket state"
        );
        self.state = ReplicateScopeState::ReadBucket;
        let key = self.input.bucket.as_bytes().to_vec();
        Ok(Next::Call(read_row(S3_BUCKET_KEYSPACE, key)))
    }

    fn resolve_target(&mut self) -> ScopeTurn {
        match self.input.target.clone() {
            ReplicateScopeTarget::Bucket => self.start_iteration(None, false),
            ReplicateScopeTarget::Prefix(prefix) => self.start_iteration(Some(prefix), false),
            ReplicateScopeTarget::Object { key } => {
                self.state = ReplicateScopeState::ResolveObjectTarget;
                let lookup = match BlobHeadKey::new(&self.input.bucket, &key).to_bytes() {
                    Ok(lookup) => lookup,
                    Err(err) => return self.fail(err.into()),
                };
                Ok(Next::Call(read_row(BLOB_HEAD_KEYSPACE, lookup)))
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
    ) -> ScopeTurn {
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

    fn request_iteration_page(&mut self) -> ScopeTurn {
        let prefix = match VersionKey::bucket_prefix(&self.input.bucket) {
            Ok(prefix) => prefix,
            Err(err) => return self.fail(err.into()),
        };
        Ok(Next::Call(IoCall::Page {
            space: BLOB_VERSIONS_KEYSPACE.to_string(),
            prefix: prefix.into(),
            after: self.next_start_after.clone(),
            limit: ITER_PAGE_SIZE,
        }))
    }

    fn read_single_version(&mut self, key: &str, version_id: Ulid) -> ScopeTurn {
        self.state = ReplicateScopeState::ReadSingleVersion;
        let version_key = match VersionKey::new(&self.input.bucket, key, version_id).to_bytes() {
            Ok(key) => key,
            Err(err) => return self.fail(err.into()),
        };
        Ok(Next::Call(read_row(BLOB_VERSIONS_KEYSPACE, version_key)))
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
            self.result.failure = Some(ReplicationFailure::AccessDenied);
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

    fn run_next_replication(&mut self) -> ScopeTurn {
        if self.reference_advance.is_some() && self.pending_versions.len() > 1 {
            return self.fail(ReplicateObjectError::InvalidReferenceAdvance.into());
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
            return Ok(Next::Done(self.result.clone()));
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
                let Some(target_key) = target_key(&sync, &request.key) else {
                    self.result.failed = self.result.failed.saturating_add(1);
                    return self.run_next_replication();
                };
                sync.target_prefix = Some(target_key);
                ReplicateObjectOperation::new(request)
                    .with_routing(self.routing.clone())
                    .with_sync(sync)
            }
            None => ReplicateObjectOperation::new(request).with_routing(self.routing.clone()),
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
        let operation = operation.with_plaintext(self.plaintext);
        Ok(Next::Call(IoCall::child(operation)))
    }
}

/// The key a version lands under at the target; a member of a set keeps the key exactly,
/// leading slashes included.
fn target_key(sync: &SyncTransferContext, key: &str) -> Option<String> {
    match sync.distribution {
        Some(_) => Some(key.to_string()),
        None => map_sync_key(
            key,
            sync.source_prefix.as_deref(),
            sync.target_prefix.as_deref(),
        ),
    }
}

pub fn map_sync_key(
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

impl ReplicateObjectError {
    /// The stable failure category this item reports. A peer rejection is
    /// classified from its published reason at this boundary; every local
    /// failure is `Other`, so nothing local can fake a permission outcome.
    pub fn failure_category(&self) -> ReplicationFailure {
        match self {
            Self::ReplicationError(ReplicationError::ReplicationRejected(reason)) => {
                ReplicationItemError::from_peer_reason(reason).failure
            }
            Self::PlaintextRefused => ReplicationFailure::PlaintextRefused,
            _ => ReplicationFailure::Other,
        }
    }
}

/// One outbound version push per target node: every accepted event advances
/// exactly one state, and `step` dispatches each state to the handler named after
/// its protocol phase (prepare manifest, connect, negotiate, transfer, close).
#[derive(Clone, Debug, Eq, PartialEq)]
enum ReplicateObjectState {
    Init,
    ReadVersion,
    /// Reads the root record a stored compact binding names: a version leaves with its root's
    /// settings, since another node reads the source through its own connector of the root.
    ReadSourceRoot,
    ReadBlobLocation,
    CheckManagedCopy,
    ResolveReferenceAccess,
    /// Reads the source bucket's encryption before reference content is read or materialized.
    CheckReferenceEncryption,
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
    /// Reads the location of a pending source version, then admits its key.
    ReadPendingSource,
    AdmitPendingSource,
    /// An unlocked pending source is promoted first; this item fails and retries.
    SchedulePromotion,
    OpenConnection,
    SendManifest,
    AwaitNegotiation,
    /// Rereads the source bucket's encryption before any content leaves this node.
    CheckSourceEncryption,
    /// Admits the source key; the lease covers the transfer.
    AdmitSourceKey,
    TransferBlob,
    SendApproval,
    AwaitApplyComplete,
    WriteReferenceState,
    CloseConnection,
}

#[derive(Debug, PartialEq)]
pub struct ReplicateObjectOperation {
    now_ms: u64,
    ids: IdMint,
    request: VersionReplicationRequest,
    state: ReplicateObjectState,
    pending_materialized_version: Option<PendingMaterializedVersion>,
    /// A version read with a compact binding, while its root record is read.
    compact_version: Option<BlobVersion>,
    replication_version: Option<ReplicationVersion>,
    multipart_summary: Option<MultipartObjectSummary>,
    multipart_parts: Vec<MultipartObjectPart>,
    parts_next_start: Option<Key>,
    stream_id: Option<Ulid>,
    manifest: Option<VersionReplicationManifest>,
    blob_replication_id: Option<Ulid>,
    /// Resolved reference access, held until the source bucket's encryption is checked.
    pending_access: Option<ResolvedSourceAccess>,
    cleanup_reference_blob: Option<BackendLocation>,
    preserve_reference: bool,
    reference_access: Option<ResolvedSourceAccess>,
    reference_metadata: Option<SourceMetadata>,
    group_inputs: GroupRoutingInputs,
    bucket_rules: Vec<StorageRoutingRule>,
    bucket_compression: Compression,
    /// Group owning the destination bucket, read with its routing rules.
    bucket_group: Option<GroupId>,
    sync: Option<SyncTransferContext>,
    writer_auth_context: Option<AuthContext>,
    reference_advance: Option<ReferenceAdvance>,
    /// Refs read from the stored version, carried onto the manifest unchanged.
    version_policies: Vec<PlacementPolicyRef>,
    /// The stored version's original epoch, carried onto the manifest unchanged.
    version_epoch: OriginalEpoch,
    version_generation: u64,
    version_identity: Option<[u8; 32]>,
    version_origin: Option<VersionOrigin>,
    version_hop: Option<SyncHop>,
    pending_copy: Option<ManagedCopyKey>,
    /// Destination details of this node, evaluated before reference bytes are
    /// materialized locally.
    gate_context: Option<GateContext>,
    routing: NodeRouting,
    /// What the item reports once its stream is closed.
    result: ReplicationSuboperationResult,
    /// A permitted plaintext request: encrypted items may reach a plain target.
    plaintext: bool,
    /// What the target asked for; it decides how an encrypted source sends.
    negotiated: Option<ReplicationNegotiationResult>,
    /// The archive of a pending source version, or the key a sending source admits.
    pending_archive: Option<ArchiveKey>,
    admit_key: Option<BucketKeyRef>,
}

struct ManifestVersionParts {
    kind: ReplicationItemKind,
    created_at: SystemTime,
    created_by: UserId,
    blob: Option<MaterializedBlobInfo>,
    source: Option<VersionSourceBinding>,
    reference: Option<SourceMetadata>,
    metadata: HashMap<String, String>,
}

impl ReplicateObjectOperation {
    pub fn new(request: VersionReplicationRequest) -> Self {
        Self {
            now_ms: 0,
            ids: IdMint::default(),
            request,
            state: ReplicateObjectState::Init,
            pending_materialized_version: None,
            compact_version: None,
            replication_version: None,
            multipart_summary: None,
            multipart_parts: Vec::new(),
            parts_next_start: None,
            stream_id: None,
            manifest: None,
            blob_replication_id: None,
            pending_access: None,
            cleanup_reference_blob: None,
            preserve_reference: false,
            reference_access: None,
            reference_metadata: None,
            group_inputs: GroupRoutingInputs::default(),
            bucket_rules: Vec::new(),
            bucket_compression: Compression::Off,
            bucket_group: None,
            sync: None,
            writer_auth_context: None,
            reference_advance: None,
            version_policies: Vec::new(),
            version_epoch: OriginalEpoch::default(),
            version_generation: 0,
            version_identity: None,
            version_origin: None,
            version_hop: None,
            pending_copy: None,
            gate_context: None,
            routing: NodeRouting::default(),
            result: ReplicationSuboperationResult::Replicated,
            plaintext: false,
            negotiated: None,
            pending_archive: None,
            admit_key: None,
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

    fn with_plaintext(mut self, plaintext: bool) -> Self {
        self.plaintext = plaintext;
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
            ReplicateObjectState::Init => "Init",
            ReplicateObjectState::ReadVersion => "ReadVersion",
            ReplicateObjectState::ReadSourceRoot => "ReadSourceRoot",
            ReplicateObjectState::ReadBlobLocation => "ReadBlobLocation",
            ReplicateObjectState::CheckManagedCopy => "CheckManagedCopy",
            ReplicateObjectState::ResolveReferenceAccess => "ResolveReferenceAccess",
            ReplicateObjectState::CheckReferenceEncryption => "CheckReferenceEncryption",
            ReplicateObjectState::HeadReferenceSource => "HeadReferenceSource",
            ReplicateObjectState::ReadReferenceState => "ReadReferenceState",
            ReplicateObjectState::LoadRouting => "LoadRouting",
            ReplicateObjectState::ReadBucketRules => "ReadBucketRules",
            ReplicateObjectState::ReadReferenceSource => "ReadReferenceSource",
            ReplicateObjectState::ReferencePolicyGate => "ReferencePolicyGate",
            ReplicateObjectState::WriteReferenceBlob => "WriteReferenceBlob",
            ReplicateObjectState::CleanupReferenceBlob => "CleanupReferenceBlob",
            ReplicateObjectState::ReadMultipartSummary => "ReadMultipartSummary",
            ReplicateObjectState::ReadMultipartParts => "ReadMultipartParts",
            ReplicateObjectState::ReadCurrentLookup => "ReadCurrentLookup",
            ReplicateObjectState::ReadPendingSource => "ReadPendingSource",
            ReplicateObjectState::AdmitPendingSource => "AdmitPendingSource",
            ReplicateObjectState::SchedulePromotion => "SchedulePromotion",
            ReplicateObjectState::AdmitSourceKey => "AdmitSourceKey",
            ReplicateObjectState::OpenConnection => "OpenConnection",
            ReplicateObjectState::SendManifest => "SendManifest",
            ReplicateObjectState::AwaitNegotiation => "AwaitNegotiation",
            ReplicateObjectState::CheckSourceEncryption => "CheckSourceEncryption",
            ReplicateObjectState::TransferBlob => "TransferBlob",
            ReplicateObjectState::SendApproval => "SendApproval",
            ReplicateObjectState::AwaitApplyComplete => "AwaitApplyComplete",
            ReplicateObjectState::WriteReferenceState => "WriteReferenceState",
            ReplicateObjectState::CloseConnection => "CloseConnection",
        }
    }

    fn invalid(&self, expected: &'static str, received: &'static str) -> ObjectTurn {
        self.fail(ReplicateObjectError::InvalidStateEvent {
            state: self.state_name(),
            expected,
            received,
        })
    }

    fn read(
        &mut self,
        state: ReplicateObjectState,
        space: &str,
        key: impl Into<Key>,
    ) -> ObjectTurn {
        self.state = state;
        Ok(Next::Call(read_row(space, key)))
    }

    fn fail(&self, err: ReplicateObjectError) -> ObjectTurn {
        debug!(
            bucket = %self.request.bucket,
            key = %self.request.key,
            version_id = %self.request.version_id,
            target_node = %self.request.target_node_id,
            state = %self.state_name(),
            error = %err,
            "Version replication failed"
        );
        Err(err)
    }

    fn read_version(&mut self) -> ObjectTurn {
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

        self.read(
            ReplicateObjectState::ReadVersion,
            BLOB_VERSIONS_KEYSPACE,
            key,
        )
    }

    fn read_blob_location(&mut self, key: BlobLocationKey) -> ObjectTurn {
        self.read(
            ReplicateObjectState::ReadBlobLocation,
            BLOB_LOCATIONS_KEYSPACE,
            key.to_bytes(),
        )
    }

    fn read_multipart_summary(&mut self) -> ObjectTurn {
        let key = match MultipartObjectKey::summary(self.request.version_id).to_bytes() {
            Ok(key) => key,
            Err(err) => return self.fail(err.into()),
        };

        self.read(
            ReplicateObjectState::ReadMultipartSummary,
            OBJECT_METADATA_KEYSPACE,
            key,
        )
    }

    fn read_multipart_parts(&mut self) -> ObjectTurn {
        self.state = ReplicateObjectState::ReadMultipartParts;
        let prefix = match MultipartObjectKey::part_prefix(self.request.version_id) {
            Ok(prefix) => prefix,
            Err(err) => return self.fail(err.into()),
        };

        Ok(Next::Call(IoCall::Page {
            space: OBJECT_METADATA_KEYSPACE.to_string(),
            prefix: prefix.into(),
            after: self.parts_next_start.clone(),
            limit: ITER_PAGE_SIZE,
        }))
    }

    fn validate_multipart_parts(&self) -> Result<(), ReplicateObjectError> {
        let Some(summary) = self.multipart_summary.as_ref() else {
            return Ok(());
        };

        let actual = self.multipart_parts.len();
        if actual != summary.part_count {
            return Err(ReplicateObjectError::PartCountMismatch {
                expected: summary.part_count,
                actual,
            });
        }

        Ok(())
    }

    fn read_current_lookup(&mut self) -> ObjectTurn {
        let key = match BlobHeadKey::new(&self.request.bucket, &self.request.key).to_bytes() {
            Ok(key) => key,
            Err(err) => return self.fail(err.into()),
        };

        self.read(
            ReplicateObjectState::ReadCurrentLookup,
            BLOB_HEAD_KEYSPACE,
            key,
        )
    }

    fn skip_version(&mut self) -> ObjectTurn {
        debug!(
            bucket = %self.request.bucket,
            key = %self.request.key,
            version_id = %self.request.version_id,
            target_node = %self.request.target_node_id,
            state = %self.state_name(),
            mode = ?self.request.mode,
            "Skipping version replication"
        );
        Ok(Next::Done(ReplicationSuboperationResult::Skipped))
    }

    fn reference_matches(&self, metadata: &SourceMetadata) -> Option<bool> {
        let ReplicationVersion::Reference {
            cached_metadata, ..
        } = self.replication_version.as_ref()?
        else {
            return None;
        };
        // An unconfirmed observation may still be kept: the write checks the recorded digests.
        Some(match cached_metadata.compare(metadata) {
            ObservationMatch::Same => true,
            ObservationMatch::Unknown => !cached_metadata.enforced_checksums().is_empty(),
            ObservationMatch::Changed => false,
        })
    }

    fn resolve_reference(&mut self, version: ReplicationVersion) -> ObjectTurn {
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
            return self.fail(ReplicateObjectError::VersionNotFound);
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
        self.state = ReplicateObjectState::ResolveReferenceAccess;
        let resolve = resolve_binding_effect(ResolveBindingInput {
            source,
            group_id: None,
        });
        Ok(Next::Call(resolve.into()))
    }

    fn accept_reference_encryption(&mut self, value: Option<Value>) -> ObjectTurn {
        let settings = match BucketEncryption::from_row(value.as_deref()) {
            Ok(settings) => settings,
            Err(err) => return self.fail(err.into()),
        };
        if settings.is_encrypted() {
            return self.fail(ReplicateObjectError::EncryptedVersion);
        }
        let Some(access) = self.pending_access.take() else {
            return self.fail(ReplicateObjectError::VersionNotFound);
        };
        if self.sync.is_none() {
            return self.load_routing(access);
        }
        self.reference_access = Some(access.clone());
        self.state = ReplicateObjectState::HeadReferenceSource;
        Ok(Next::Call(
            Effect::staging(StagingSourceEffect::Head { access }).into(),
        ))
    }

    fn accept_reference_access(&mut self, event: Event) -> ObjectTurn {
        let Event::SubOperation(SubOperationEvent::VersionAccessResolved { result }) = event else {
            return self.invalid(
                "Event::SubOperation(SubOperationEvent::VersionSourceAccessResolved)",
                event.label(),
            );
        };
        match *result {
            Ok(access) => {
                let (source_kind, source_path, source_version) = match &access {
                    aruna_core::structs::execution::source_access::ResolvedSourceAccess::OpenDal {
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
                // Reference content of an encrypting bucket is never read or materialized here.
                self.pending_access = Some(access);
                self.read(
                    ReplicateObjectState::CheckReferenceEncryption,
                    BUCKET_ENCRYPTION_KEYSPACE,
                    self.request.bucket.as_bytes().to_vec(),
                )
            }
            Err(_) => {
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
        }
    }

    fn reference_state_key(&self) -> Result<Vec<u8>, ReplicateObjectError> {
        let relationship_id = self
            .sync
            .as_ref()
            .map(|sync| sync.relationship_id)
            .ok_or(ReplicateObjectError::UnresolvedReferenceVersion)?;
        Ok(sync_state_key(
            relationship_id,
            &self.request.bucket,
            &self.request.key,
            self.request.version_id,
        )?)
    }

    fn accept_reference_head(&mut self, event: Event) -> ObjectTurn {
        match event.into_staging() {
            Ok(StagingSourceEvent::HeadResult { metadata }) => {
                match self.reference_matches(&metadata) {
                    Some(true) => {}
                    Some(false) => return self.skip_version(),
                    None => return self.fail(ReplicateObjectError::VersionNotFound),
                }
                self.reference_metadata = Some(metadata);
                let key = match self.reference_state_key() {
                    Ok(key) => key,
                    Err(error) => return self.fail(error),
                };
                self.read(
                    ReplicateObjectState::ReadReferenceState,
                    SYNC_REFERENCE_KEYSPACE,
                    key,
                )
            }
            Ok(StagingSourceEvent::Error { .. }) => {
                self.fail(ReplicationError::ReplicationFailed.into())
            }
            Ok(other) => self.invalid(
                "Event::StagingSource(StagingSourceEvent::HeadResult)",
                other.label(),
            ),
            Err(other) => self.invalid(
                "Event::StagingSource(StagingSourceEvent::HeadResult)",
                other.label(),
            ),
        }
    }

    fn accept_reference_state(&mut self, value: Option<Value>) -> ObjectTurn {
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
            return self.fail(ReplicateObjectError::UnresolvedReferenceVersion);
        };
        self.load_routing(access)
    }

    /// This node materializes the reference locally, so it routes with its own
    /// snapshot: the group default and bucket rules load before the read.
    fn load_routing(&mut self, access: ResolvedSourceAccess) -> ObjectTurn {
        self.reference_access = Some(access);
        self.state = ReplicateObjectState::LoadRouting;
        Ok(Next::Call(
            load_group_inputs(self.request.source_group_id).into(),
        ))
    }

    fn read_bucket_rules(&mut self) -> ObjectTurn {
        self.read(
            ReplicateObjectState::ReadBucketRules,
            S3_BUCKET_KEYSPACE,
            self.request.bucket.as_bytes().to_vec(),
        )
    }

    fn accept_routing_loaded(&mut self, event: Event) -> ObjectTurn {
        let Event::SubOperation(SubOperationEvent::GroupRoutingLoaded { result }) = event else {
            return self.invalid(
                "Event::SubOperation(SubOperationEvent::GroupRoutingLoaded)",
                event.label(),
            );
        };
        match *result {
            Ok(inputs) => self.group_inputs = inputs,
            Err(error) => {
                return self.fail(ReplicateObjectError::RoutingInputsFailed(error));
            }
        }
        self.read_bucket_rules()
    }

    /// A bucket without a record simply has no rules; an unreadable or
    /// undecodable one fails the write instead of rerouting it, matching the
    /// snapshot the local write surface assembles.
    fn accept_bucket_rules(&mut self, value: Option<Value>) -> ObjectTurn {
        let record = value.as_deref().map(BucketInfo::from_bytes).transpose();
        match record.map_err(RecordReadError::Conversion) {
            Ok(record) => {
                self.bucket_group = record.as_ref().map(|info| info.group_id);
                let info = record.map(|info| (info.storage_routing, info.compression));
                (self.bucket_rules, self.bucket_compression) = info.unwrap_or_default();
                self.read_reference_source()
            }
            Err(error) => self.fail(error.into()),
        }
    }

    /// A reference materializes real bytes on this node, so it passes the same
    /// destination gate an ordinary write does, before the source is read.
    fn read_reference_source(&mut self) -> ObjectTurn {
        match write_gate(
            self.gate_context.as_ref(),
            &self.version_policies,
            self.bucket_group,
        ) {
            Ok(None) => self.open_reference_source(),
            Ok(Some(gate)) => {
                self.state = ReplicateObjectState::ReferencePolicyGate;
                Ok(Next::Call(IoCall::child(gate)))
            }
            Err(error) => self.fail(error.into()),
        }
    }

    fn accept_reference_gate(
        &mut self,
        outcome: Result<PolicyGateOutcome, ReadPolicyError>,
    ) -> ObjectTurn {
        let decision = outcome
            .map_err(PolicyGateError::from)
            .and_then(gate_decision);
        match decision {
            Ok(()) => self.open_reference_source(),
            Err(error) => self.fail(error.into()),
        }
    }

    fn open_reference_source(&mut self) -> ObjectTurn {
        let Some(access) = self.reference_access.take() else {
            return self.fail(ReplicateObjectError::UnresolvedReferenceVersion);
        };
        let expected = match self.replication_version.as_ref() {
            Some(ReplicationVersion::Reference {
                cached_metadata, ..
            }) => Some(cached_metadata.clone()),
            _ => None,
        };
        self.state = ReplicateObjectState::ReadReferenceSource;
        let read = StagingSourceEffect::Read {
            access,
            range: None,
            expected,
        };
        Ok(Next::Call(Effect::staging(read).into()))
    }

    fn accept_reference_read(&mut self, event: Event) -> ObjectTurn {
        match event.into_staging() {
            Ok(StagingSourceEvent::ReadResult {
                metadata: source_metadata,
                stream,
            }) => {
                match self.reference_matches(&source_metadata) {
                    Some(true) => {}
                    Some(false) => return self.skip_version(),
                    None => return self.fail(ReplicateObjectError::VersionNotFound),
                }
                let Some(ReplicationVersion::Reference { created_by, .. }) =
                    self.replication_version.as_ref()
                else {
                    return self.fail(ReplicateObjectError::VersionNotFound);
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
                    .with_bucket_rules(self.bucket_rules.clone())
                    .with_compression(self.bucket_compression);
                let resolved =
                    match resolve_backend(&snapshot, &self.request.bucket, &self.request.key) {
                        Ok(resolved) => resolved,
                        Err(error) => {
                            return self.fail(ReplicateObjectError::RoutingFailed(error));
                        }
                    };
                self.reference_metadata = Some(source_metadata);
                self.state = ReplicateObjectState::WriteReferenceBlob;
                Ok(blob_next(BlobEffect::Write {
                    bucket: self.request.bucket.clone(),
                    key: self.request.key.clone(),
                    resolved,
                    created_by,
                    blob: stream,
                    size: None,
                }))
            }
            Ok(StagingSourceEvent::Error { error }) => {
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
            Ok(other) => self.invalid(
                "Event::StagingSource(StagingSourceEvent::ReadResult)",
                other.label(),
            ),
            Err(other) => self.invalid(
                "Event::StagingSource(StagingSourceEvent::ReadResult)",
                other.label(),
            ),
        }
    }

    fn accept_reference_write(&mut self, event: Event) -> ObjectTurn {
        match event.into_blob() {
            Ok(BlobEvent::WriteFinished { location }) => {
                let Some(version) = self.replication_version.take() else {
                    return self.fail(ReplicateObjectError::VersionNotFound);
                };
                let ReplicationVersion::Reference {
                    created_at,
                    created_by,
                    source,
                    metadata,
                    cached_metadata,
                    ..
                } = version
                else {
                    return self.fail(ReplicateObjectError::VersionNotFound);
                };
                let enforced = cached_metadata.enforced_checksums();
                if let Some(failed) = unmatched_checksum(&enforced, &location.hashes) {
                    self.cleanup_reference_blob = Some(location);
                    return self.fail(ReplicateObjectError::DigestMismatch(
                        failed.algorithm.hash_key(),
                    ));
                }
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
            Ok(BlobEvent::Error(BlobError::WriteCleanup { location, .. })) => {
                self.cleanup_reference_blob = Some(location);
                self.fail(ReplicationError::ReplicationFailed.into())
            }
            Ok(BlobEvent::Error(_)) => self.fail(ReplicationError::ReplicationFailed.into()),
            Ok(other) => self.invalid("Event::Blob(BlobEvent::WriteFinished)", other.label()),
            Err(other) => self.invalid("Event::Blob(BlobEvent::WriteFinished)", other.label()),
        }
    }

    fn manifest_version(
        &self,
        version: ReplicationVersion,
        reference_intent: bool,
    ) -> Result<ManifestVersionParts, ReplicateObjectError> {
        match version {
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
                        .ok_or(ReplicateObjectError::UnresolvedReferenceVersion)?;
                    // Bind references to the live version head; location metadata can already drift.
                    let reference = SourceMetadata {
                        content_length: location.blob_size,
                        content_type: None,
                        etag: None,
                        last_modified: Some(created_at),
                        source_version: None,
                        digests: Default::default(),
                    };
                    Ok(ManifestVersionParts {
                        kind: ReplicationItemKind::Materialized,
                        created_at,
                        created_by,
                        blob: None,
                        source: Some(self.reference_binding(sync)),
                        reference: Some(reference),
                        metadata,
                    })
                } else {
                    let hash = location
                        .get_blake3()
                        .ok_or(ReplicateObjectError::MissingBlobHash)?
                        .try_into()
                        .map_err(|_| ReplicateObjectError::MissingBlobHash)?;
                    Ok(ManifestVersionParts {
                        kind: ReplicationItemKind::Materialized,
                        created_at,
                        created_by,
                        blob: Some(MaterializedBlobInfo {
                            hash,
                            size: location.blob_size,
                            location,
                        }),
                        source,
                        reference: None,
                        metadata,
                    })
                }
            }
            ReplicationVersion::Deleted {
                created_at,
                created_by,
                metadata,
            } => Ok(ManifestVersionParts {
                kind: ReplicationItemKind::DeleteMarker,
                created_at,
                created_by,
                blob: None,
                source: None,
                reference: None,
                metadata,
            }),
            ReplicationVersion::Reference {
                created_at,
                created_by,
                source,
                cached_metadata,
                metadata,
                ..
            } => {
                if !reference_intent {
                    return Err(ReplicateObjectError::UnresolvedReferenceVersion);
                }
                let sync = self
                    .sync
                    .as_ref()
                    .ok_or(ReplicateObjectError::UnresolvedReferenceVersion)?;
                // Members of a set read the source themselves; connector ids are node-local.
                let source = match sync.distribution {
                    Some(_) => source.without_connectors(),
                    None => self.reference_binding(sync),
                };
                Ok(ManifestVersionParts {
                    kind: ReplicationItemKind::Materialized,
                    created_at,
                    created_by,
                    blob: None,
                    source: Some(source),
                    reference: Some(cached_metadata),
                    metadata,
                })
            }
        }
    }

    fn build_manifest(
        &mut self,
        current_lookup: Option<CurrentVersionPointer>,
    ) -> Result<(), ReplicateObjectError> {
        if self.reference_advance.is_none() {
            self.validate_multipart_parts()?;
        }

        let version = self
            .replication_version
            .clone()
            .ok_or(ReplicateObjectError::VersionNotFound)?;
        let reference_intent =
            self.sync.as_ref().is_some_and(|sync| sync.reference_intent) || self.preserve_reference;
        if self.reference_advance.is_some()
            && (!self.preserve_reference
                || !reference_intent
                || self.multipart_summary.is_some()
                || !matches!(&version, ReplicationVersion::Reference { .. }))
        {
            return Err(ReplicateObjectError::InvalidReferenceAdvance);
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
        let ManifestVersionParts {
            kind,
            created_at,
            created_by,
            blob,
            source,
            reference,
            mut metadata,
        } = self.manifest_version(version, reference_intent)?;
        // Release tags travel only with a coordinator's publication inside its own set.
        if (self.sync.as_ref()).is_none_or(|sync| sync.distribution.is_none()) {
            strip_release_keys(&mut metadata);
        }

        let multipart = self
            .multipart_summary
            .clone()
            .map(|summary| MultipartObjectMetadata {
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
            original_epoch: self.version_epoch,
            version_generation: self.version_generation,
            logical_identity: self.version_identity,
            distribution: self
                .sync
                .as_ref()
                .and_then(|sync| sync.distribution.clone()),
            version_origin: self.version_origin.clone(),
            version_hop: self.version_hop.clone(),
            sync_source: (self.sync.as_ref())
                .map(|_| (self.request.bucket.clone(), self.request.key.clone())),
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

    /// Names the copy a record-only target reads later: the sync relationship, or in a
    /// distributed bucket the set whose holders keep the bytes.
    fn reference_binding(&self, sync: &SyncTransferContext) -> VersionSourceBinding {
        let config = match &sync.distribution {
            Some(publication) => (NATIVE_SET_ID, publication.set_id),
            None => (NATIVE_RELATIONSHIP_ID, sync.relationship_id),
        };
        VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: PortableSourceDescriptor {
                kind: SourceConnectorKind::ArunaNative,
                public_config: std::collections::HashMap::from([(
                    config.0.to_string(),
                    config.1.to_string(),
                )]),
                source_path: format!("{}/{}", self.request.bucket, self.request.key),
                version_selector: Some(format!("version:{}", self.request.version_id)),
                capabilities: Vec::new(),
                origin_node_id: Some(sync.source_node_id),
                namespace: None,
                root: None,
            },
            connector_id: None,
            mirrors: Vec::new(),
        }
    }

    fn send_manifest(&mut self) -> ObjectTurn {
        self.state = ReplicateObjectState::SendManifest;
        let Some(stream_id) = self.stream_id else {
            return self.fail(ReplicationError::ConnectionMissing.into());
        };
        let Some(manifest) = self.manifest.clone() else {
            return self.fail(ReplicateObjectError::NotFinished);
        };
        let message = match manifest.reference_advance {
            Some(advance) => VersionReplicationMessage::ReferenceAdvance { manifest, advance },
            None => VersionReplicationMessage::VersionManifest(manifest),
        };
        let payload = match message.to_bytes() {
            Ok(payload) => payload,
            Err(err) => return self.fail(err.into()),
        };

        Ok(blob_next(BlobEffect::SendMessage { stream_id, payload }))
    }

    fn await_negotiation(&mut self) -> ObjectTurn {
        self.state = ReplicateObjectState::AwaitNegotiation;
        let Some(stream_id) = self.stream_id else {
            return self.fail(ReplicationError::ConnectionMissing.into());
        };
        Ok(blob_next(BlobEffect::ReadMessage { stream_id }))
    }

    fn await_apply_complete(&mut self) -> ObjectTurn {
        self.state = ReplicateObjectState::AwaitApplyComplete;
        let Some(stream_id) = self.stream_id else {
            return self.fail(ReplicationError::ConnectionMissing.into());
        };
        Ok(blob_next(BlobEffect::ReadMessage { stream_id }))
    }

    fn approve_apply(&mut self) -> ObjectTurn {
        let Some(stream_id) = self.stream_id else {
            return self.fail(ReplicationError::ConnectionMissing.into());
        };
        let payload = match VersionReplicationMessage::VersionApplyApproved.to_bytes() {
            Ok(payload) => payload,
            Err(error) => return self.fail(error.into()),
        };
        self.state = ReplicateObjectState::SendApproval;
        Ok(blob_next(BlobEffect::SendMessage { stream_id, payload }))
    }

    fn close_connection(&mut self) -> ObjectTurn {
        self.state = ReplicateObjectState::CloseConnection;
        let Some(stream_id) = self.stream_id else {
            return self.fail(ReplicationError::ConnectionMissing.into());
        };
        Ok(blob_next(BlobEffect::CloseConnection { stream_id }))
    }

    fn cleanup_reference_blob(&mut self) -> ObjectTurn {
        if let Some(location) = self.cleanup_reference_blob.take() {
            self.state = ReplicateObjectState::CleanupReferenceBlob;
            Ok(blob_next(BlobEffect::Delete { location }))
        } else {
            self.close_connection()
        }
    }

    fn write_reference_state(&mut self) -> ObjectTurn {
        if self.sync.is_none() {
            return self.cleanup_reference_blob();
        }
        let Some(metadata) = self.reference_metadata.as_ref() else {
            return self.cleanup_reference_blob();
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
        self.state = ReplicateObjectState::WriteReferenceState;
        let row = (
            SYNC_REFERENCE_KEYSPACE.to_string(),
            key.into(),
            value.into(),
        );
        Ok(Next::Call(IoCall::Write(vec![row])))
    }
}

impl From<IoError> for ReplicateObjectError {
    fn from(error: IoError) -> Self {
        Self::StorageError(error.into())
    }
}

// The phase order is documented on [`ReplicateObjectState`].
impl Operation for ReplicateObjectOperation {
    type Output = ReplicationSuboperationResult;
    type Error = ReplicateObjectError;

    fn first(&mut self) -> ObjectTurn {
        self.read_version()
    }

    fn next(&mut self, result: IoResult) -> ObjectTurn {
        use ReplicateObjectState as State;
        match self.state {
            // Manifest preparation: read the version and materialize references.
            State::Init => self.read_version(),
            State::ReadVersion => self.accept_version_read(result.value()?),
            State::ReadSourceRoot => self.accept_root_read(result.event()?),
            State::ReadBlobLocation => self.accept_blob_location(result.value()?),
            State::CheckManagedCopy => self.accept_managed_copy(result.values()?),
            State::ResolveReferenceAccess => self.accept_reference_access(result.event()?),
            State::CheckReferenceEncryption => self.accept_reference_encryption(result.value()?),
            State::HeadReferenceSource => self.accept_reference_head(result.event()?),
            State::ReadReferenceState => self.accept_reference_state(result.value()?),
            State::LoadRouting => self.accept_routing_loaded(result.event()?),
            State::ReadBucketRules => self.accept_bucket_rules(result.value()?),
            State::ReadReferenceSource => self.accept_reference_read(result.event()?),
            State::ReferencePolicyGate => {
                self.accept_reference_gate(result.child::<PolicyGateOutcome, ReadPolicyError>()?)
            }
            State::WriteReferenceBlob => self.accept_reference_write(result.event()?),
            State::ReadMultipartSummary => self.accept_multipart_summary(result.value()?),
            State::ReadMultipartParts => self.accept_multipart_parts(result.page()?),
            State::ReadCurrentLookup => self.accept_current_lookup(result.value()?),
            State::ReadPendingSource => self.accept_pending_source(result.value()?),
            State::AdmitPendingSource => self.accept_pending_admission(result.event()?),
            State::SchedulePromotion => self.accept_promotion_scheduled(result.event()?),
            State::AdmitSourceKey => self.accept_source_admission(result.event()?),
            // Connection: open the stream to the target node.
            State::OpenConnection => self.accept_connection_established(result.event()?),
            // Negotiation: send the manifest and accept the target's decision.
            State::SendManifest => self.accept_manifest_sent(result.event()?),
            State::AwaitNegotiation => self.accept_negotiation_response(result.event()?),
            // Transfer: push the blob the target requested.
            State::CheckSourceEncryption => self.accept_source_encryption(result.value()?),
            State::TransferBlob => self.accept_blob_transfer(result.event()?),
            State::SendApproval => self.accept_approval_sent(result.event()?),
            // Apply acknowledgement: the target reports the replica applied.
            State::AwaitApplyComplete => self.accept_apply_response(result.event()?),
            // Close/cleanup: drop local reference bytes and close the stream.
            State::CleanupReferenceBlob => self.accept_blob_cleanup(result.event()?),
            State::WriteReferenceState => {
                result.done()?;
                self.cleanup_reference_blob()
            }
            State::CloseConnection => self.accept_connection_closed(result.event()?),
        }
    }

    /// A failed push drops its local reference bytes and closes its stream.
    fn abort(&mut self) -> Vec<IoCall> {
        let mut calls = Vec::new();
        if let Some(location) = self.cleanup_reference_blob.take() {
            calls.push(Effect::blob(BlobEffect::Delete { location }).into());
        }
        if let Some(stream_id) = self.stream_id {
            calls.push(Effect::blob(BlobEffect::CloseConnection { stream_id }).into());
        }
        calls
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn seed(&mut self, entropy: u128) {
        self.ids = IdMint::new(entropy);
    }
}

impl ReplicateObjectOperation {
    fn accept_version_read(&mut self, value: Option<Value>) -> ObjectTurn {
        let Some(value) = value else {
            return self.fail(ReplicateObjectError::VersionNotFound);
        };
        self.pending_materialized_version = None;
        self.replication_version = None;
        let version = match BlobVersion::from_bytes(value.as_ref()) {
            Ok(version) => version,
            Err(err) => return self.fail(err.into()),
        };
        if let Some(digest) = named_root(&version) {
            self.compact_version = Some(version);
            self.state = ReplicateObjectState::ReadSourceRoot;
            return Ok(Next::Call(Effect::Storage(read_root(digest, None)).into()));
        }
        self.take_version(version)
    }

    fn accept_root_read(&mut self, event: Event) -> ObjectTurn {
        let Some(version) = self.compact_version.take() else {
            return self.fail(ReplicateObjectError::VersionNotFound);
        };
        let root = match parse_root(event) {
            Ok(Some(root)) => root,
            Ok(None) => return self.fail(ReplicateObjectError::UnresolvedReferenceVersion),
            Err(StorageReadError::Storage(error)) => return self.fail(error.into()),
            Err(StorageReadError::Conversion(error)) => return self.fail(error.into()),
        };
        match hydrated(version, &root) {
            Ok(version) => self.take_version(version),
            Err(_) => self.fail(ReplicateObjectError::UnresolvedReferenceVersion),
        }
    }

    fn take_version(&mut self, version: BlobVersion) -> ObjectTurn {
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
            original_epoch,
            generation,
            identity,
            origin,
            hop,
        } = version;
        self.version_origin = origin;
        self.version_hop = hop;
        self.version_policies = placement_policies;
        self.version_epoch = original_epoch;
        self.version_generation = generation;
        self.version_identity = identity;

        match state {
            BlobVersionState::Materialized {
                blob_hash,
                backend,
                encoding,
                source,
            } => {
                self.pending_materialized_version = Some(PendingMaterializedVersion {
                    created_at,
                    created_by,
                    blob_hash,
                    source,
                    metadata,
                });
                self.read_blob_location(BlobLocationKey::new(blob_hash, encoding, backend))
            }
            BlobVersionState::Deleted => {
                self.pending_materialized_version = None;
                self.replication_version = Some(ReplicationVersion::Deleted {
                    created_at,
                    created_by,
                    metadata,
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
                self.resolve_reference(ReplicationVersion::Reference {
                    created_at,
                    created_by,
                    source,
                    cached_metadata,
                    last_refresh,
                    metadata,
                    advance_count,
                })
            }
            // Its content hash is known only after promotion with its key.
            BlobVersionState::PendingContent { archive, .. } => {
                self.pending_archive = Some(archive.clone());
                self.read(
                    ReplicateObjectState::ReadPendingSource,
                    PENDING_LOCATION_KEYSPACE,
                    archive.to_bytes(),
                )
            }
        }
    }

    fn accept_blob_location(&mut self, value: Option<Value>) -> ObjectTurn {
        let Some(value) = value else {
            return self.fail(ReplicateObjectError::VersionNotFound);
        };
        let location = match BackendLocation::from_bytes(value.as_ref()) {
            Ok(location) => location,
            Err(err) => return self.fail(err.into()),
        };
        let Some(PendingMaterializedVersion {
            created_at,
            created_by,
            blob_hash,
            source,
            metadata,
        }) = self.pending_materialized_version.take()
        else {
            return self.fail(ReplicateObjectError::VersionNotFound);
        };
        if location.get_blake3() != Some(blob_hash.as_slice()) {
            return self.fail(ReplicateObjectError::VersionNotFound);
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
        let reads = match serve_reads(&key, None) {
            Ok(Effect::Storage(StorageEffect::BatchRead { reads, .. })) => reads,
            Ok(_) => return self.fail(ReplicateObjectError::NotFinished),
            Err(error) => return self.fail(error.into()),
        };
        self.pending_copy = Some(key);
        self.state = ReplicateObjectState::CheckManagedCopy;
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn accept_managed_copy(&mut self, values: Vec<Option<Value>>) -> ObjectTurn {
        let (copy, subject) = match split_serve_reads(keyed(&values)) {
            Ok(split) => split,
            Err(error) => return self.fail(error.into()),
        };
        let Some(key) = self.pending_copy.take() else {
            return self.fail(ReplicateObjectError::VersionNotFound);
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

    fn accept_multipart_summary(&mut self, value: Option<Value>) -> ObjectTurn {
        self.multipart_summary = value
            .as_ref()
            .and_then(|value| MultipartObjectSummary::from_bytes(value.as_ref()).ok());
        if self.multipart_summary.is_some() {
            self.multipart_parts.clear();
            self.parts_next_start = None;
            self.read_multipart_parts()
        } else {
            self.read_current_lookup()
        }
    }

    fn accept_multipart_parts(
        &mut self,
        (values, next_start_after): (Vec<(Key, Value)>, Option<Key>),
    ) -> ObjectTurn {
        for (_, value) in values {
            let part = match MultipartObjectPart::from_bytes(value.as_ref()) {
                Ok(part) => part,
                Err(err) => return self.fail(err.into()),
            };
            self.multipart_parts.push(part);
        }

        if let Some(cursor) = next_start_after {
            self.parts_next_start = Some(cursor);
            self.read_multipart_parts()
        } else {
            self.parts_next_start = None;
            self.multipart_parts
                .sort_unstable_by_key(|part| part.part_number);
            if let Err(err) = self.validate_multipart_parts() {
                return self.fail(err);
            }
            self.read_current_lookup()
        }
    }

    fn accept_current_lookup(&mut self, value: Option<Value>) -> ObjectTurn {
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
        self.state = ReplicateObjectState::OpenConnection;
        Ok(blob_next(BlobEffect::OpenConnection {
            node_id: self.request.target_node_id,
        }))
    }
}

// Phase: connection
// Opening the outbound stream to the target node.
impl ReplicateObjectOperation {
    fn accept_connection_established(&mut self, event: Event) -> ObjectTurn {
        let received = event.label();
        let Ok(BlobEvent::ConnectionEstablished { stream_id }) = event.into_blob() else {
            return self.invalid("Event::Blob(BlobEvent::ConnectionEstablished)", received);
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
}

// Phase: negotiation
// Sending the manifest and accepting one of the target's negotiation
// decisions.
impl ReplicateObjectOperation {
    fn accept_manifest_sent(&mut self, event: Event) -> ObjectTurn {
        let received = event.label();
        let Ok(BlobEvent::MessageSent { .. }) = event.into_blob() else {
            return self.invalid("Event::Blob(BlobEvent::MessageSent)", received);
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

    fn accept_negotiation_response(&mut self, event: Event) -> ObjectTurn {
        let received = event.label();
        let Ok(BlobEvent::MessageReceived { payload, .. }) = event.into_blob() else {
            return self.invalid("Event::Blob(BlobEvent::MessageReceived)", received);
        };

        let message = match VersionReplicationMessage::from_bytes(&payload) {
            Ok(message) => message,
            Err(err) => return self.fail(err.into()),
        };
        let VersionReplicationMessage::VersionNegotiationResponse(result) = message else {
            return self.invalid(
                "VersionReplicationMessage::VersionNegotiationResponse",
                received,
            );
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
                self.result = ReplicationSuboperationResult::Skipped;
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
                // A plain target linking its own copy still needs the plaintext rule.
                if self.manifest_blob().is_none() {
                    return self.await_apply_complete();
                }
                self.negotiated = Some(result);
                self.read_source_encryption()
            }
            ReplicationNegotiationResult::NeedBlobVersion
            | ReplicationNegotiationResult::NeedSealedBlob(_)
            | ReplicationNegotiationResult::NeedAbeBlob(..) => {
                let Some(blob) = self.manifest_blob() else {
                    return self.fail(ReplicateObjectError::MissingBlobHash);
                };
                let blob_size = blob.size;
                self.negotiated = Some(result.clone());
                let replication_id = self.ids.mint(self.now_ms);
                self.blob_replication_id = Some(replication_id);
                debug!(
                    bucket = %self.request.bucket,
                    key = %self.request.key,
                    version_id = %self.request.version_id,
                    target_node = %self.request.target_node_id,
                    decision = ?result,
                    replication_id = %replication_id,
                    blob_size,
                    "Target requested blob transfer"
                );
                self.read_source_encryption()
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
}

// Phase: transfer
// Pushing the blob the target accepted the version for.
impl ReplicateObjectOperation {
    fn manifest_blob(&self) -> Option<&MaterializedBlobInfo> {
        self.manifest
            .as_ref()
            .and_then(|manifest| manifest.blob.as_ref())
    }

    /// Rereads the source bucket's encryption before any content leaves this node.
    fn read_source_encryption(&mut self) -> ObjectTurn {
        self.read(
            ReplicateObjectState::CheckSourceEncryption,
            BUCKET_ENCRYPTION_KEYSPACE,
            self.request.bucket.as_bytes().to_vec(),
        )
    }

    /// Any copy of an encrypting bucket, and every sealed copy, needs a key admission. A plain
    /// target takes it only as a permitted plaintext copy; an encrypting target gets a sealed
    /// copy granted to its key, or plaintext it seals itself.
    fn accept_source_encryption(&mut self, value: Option<Value>) -> ObjectTurn {
        let settings = match BucketEncryption::from_row(value.as_deref()) {
            Ok(settings) => settings,
            Err(err) => return self.fail(err.into()),
        };
        let (Some(location), Some(negotiated)) = (
            self.manifest_blob().map(|blob| blob.location.clone()),
            self.negotiated.clone(),
        ) else {
            return self.fail(ReplicateObjectError::MissingBlobHash);
        };
        let Some(key) = location.format.bucket_key().or(settings.active_key()) else {
            return match negotiated {
                ReplicationNegotiationResult::NeedVersionOnly => self.approve_apply(),
                _ => self.replicate_plain(location),
            };
        };
        let sealed_target = matches!(
            negotiated,
            ReplicationNegotiationResult::NeedSealedBlob(_)
                | ReplicationNegotiationResult::NeedAbeBlob(..)
        );
        if !sealed_target && !self.plaintext {
            return self.fail(ReplicateObjectError::PlaintextRefused);
        }
        self.admit_key = Some(key);
        self.state = ReplicateObjectState::AdmitSourceKey;
        Ok(blob_next(BlobEffect::AdmitRead {
            key,
            archive: ArchiveKey::of(&location),
        }))
    }

    fn replicate_plain(&mut self, location: BackendLocation) -> ObjectTurn {
        let (Some(replication_id), Some(stream_id)) = (self.blob_replication_id, self.stream_id)
        else {
            return self.fail(ReplicateObjectError::MissingBlobHash);
        };
        self.state = ReplicateObjectState::TransferBlob;
        Ok(blob_next(BlobEffect::Replicate {
            replication_id,
            stream_id,
            location,
            keep_alive: true,
        }))
    }

    /// A sealed copy goes to an encrypting target granted to its key; anything else is sent as
    /// plaintext. A locked key ends this item until the next unlock.
    fn accept_source_admission(&mut self, event: Event) -> ObjectTurn {
        let key = self.admit_key.take();
        let lease = match event.into_blob() {
            Ok(BlobEvent::ReadAdmitted { lease }) if Some(lease.key) == key => lease,
            Ok(BlobEvent::Error(BlobError::BucketKey(BucketKeyError::Locked(_)))) => {
                return match key {
                    Some(key) => self.await_key(key),
                    None => self.fail(ReplicateObjectError::MissingBlobHash),
                };
            }
            Ok(BlobEvent::Error(error)) => {
                return self.fail(ReplicateObjectError::Blob(error));
            }
            Ok(other) => {
                return self.invalid("Event::Blob(BlobEvent::ReadAdmitted)", other.label());
            }

            Err(other) => {
                return self.invalid("Event::Blob(BlobEvent::ReadAdmitted)", other.label());
            }
        };
        if self.negotiated == Some(ReplicationNegotiationResult::NeedVersionOnly) {
            return self.approve_apply();
        }
        let (Some(replication_id), Some(stream_id), Some(location)) = (
            self.blob_replication_id,
            self.stream_id,
            self.manifest_blob().map(|blob| blob.location.clone()),
        ) else {
            return self.fail(ReplicateObjectError::MissingBlobHash);
        };
        // Never the source object key: only the target's bucket key and its own object key.
        let (regrant, object) = match (&self.negotiated, location.format.bucket_key()) {
            (Some(ReplicationNegotiationResult::NeedSealedBlob(plan)), Some(_)) => {
                (Some(Box::new(*plan)), None)
            }
            (Some(ReplicationNegotiationResult::NeedAbeBlob(plan, object)), Some(_)) => {
                (Some(Box::new(*plan)), Some(*object))
            }
            _ => (None, None),
        };
        self.state = ReplicateObjectState::TransferBlob;
        Ok(blob_next(BlobEffect::ReplicateLeased {
            replication_id,
            stream_id,
            location,
            lease: Box::new(lease),
            regrant,
            object,
        }))
    }

    /// Ends the item without an error; its job waits for the next unlock of `key`.
    fn await_key(&mut self, key: BucketKeyRef) -> ObjectTurn {
        debug!(
            bucket = %self.request.bucket,
            key = %self.request.key,
            version_id = %self.request.version_id,
            bucket_id = %key.bucket_id,
            generation = key.generation,
            "Replication waits for a locked source key"
        );
        self.result = ReplicationSuboperationResult::AwaitingKey(key);
        match self.stream_id {
            Some(_) => self.close_connection(),
            None => Ok(Next::Done(self.result.clone())),
        }
    }

    fn accept_approval_sent(&mut self, event: Event) -> ObjectTurn {
        match event.into_blob() {
            Ok(BlobEvent::MessageSent { .. }) => self.await_apply_complete(),
            Ok(other) => self.invalid("Event::Blob(BlobEvent::MessageSent)", other.label()),
            Err(other) => self.invalid("Event::Blob(BlobEvent::MessageSent)", other.label()),
        }
    }

    fn accept_pending_source(&mut self, value: Option<Value>) -> ObjectTurn {
        let location = match value.map(|value| BackendLocation::from_bytes(&value)) {
            Some(Ok(location)) => location,
            Some(Err(error)) => return self.fail(error.into()),
            None => return self.fail(ReplicateObjectError::PendingSource),
        };
        let (Some(key), Some(archive)) =
            (location.format.bucket_key(), self.pending_archive.clone())
        else {
            return self.fail(ReplicateObjectError::EncryptedVersion);
        };
        self.admit_key = Some(key);
        self.state = ReplicateObjectState::AdmitPendingSource;
        Ok(blob_next(BlobEffect::AdmitRead { key, archive }))
    }

    /// A locked pending source waits for the unlock, which promotes it first. An unlocked one is
    /// promoted now and this item retries afterwards.
    fn accept_pending_admission(&mut self, event: Event) -> ObjectTurn {
        let Some(key) = self.admit_key.take() else {
            return self.fail(ReplicateObjectError::PendingSource);
        };
        match event.into_blob() {
            Ok(BlobEvent::ReadAdmitted { .. }) => {
                self.state = ReplicateObjectState::SchedulePromotion;
                let wake = TaskEffect::ShortenTimer {
                    key: TaskKey::PromotePending {
                        bucket_id: key.bucket_id,
                        generation: key.generation,
                    },
                    after: std::time::Duration::ZERO,
                };
                Ok(Next::Call(Effect::Task(wake).into()))
            }
            Ok(BlobEvent::Error(BlobError::BucketKey(BucketKeyError::Locked(_)))) => {
                self.await_key(key)
            }
            Ok(BlobEvent::Error(error)) => self.fail(ReplicateObjectError::Blob(error)),
            Ok(other) => self.invalid("Event::Blob(BlobEvent::ReadAdmitted)", other.label()),
            Err(other) => self.invalid("Event::Blob(BlobEvent::ReadAdmitted)", other.label()),
        }
    }

    fn accept_promotion_scheduled(&mut self, event: Event) -> ObjectTurn {
        match event {
            Event::Task(TaskEvent::TimerScheduled { .. } | TaskEvent::Error { .. }) => {
                self.fail(ReplicateObjectError::PendingSource)
            }
            other => self.invalid("Event::Task(TaskEvent::TimerScheduled)", other.label()),
        }
    }

    fn accept_blob_transfer(&mut self, event: Event) -> ObjectTurn {
        let received = event.label();
        let Ok(BlobEvent::ReplicationFinished { .. }) = event.into_blob() else {
            return self.invalid("Event::Blob(BlobEvent::ReplicationFinished)", received);
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
}

// Phase: apply acknowledgement
// Waiting for the target to report the replica applied or rejected.
impl ReplicateObjectOperation {
    fn accept_apply_response(&mut self, event: Event) -> ObjectTurn {
        let received = event.label();
        let Ok(BlobEvent::MessageReceived { payload, .. }) = event.into_blob() else {
            return self.invalid("Event::Blob(BlobEvent::MessageReceived)", received);
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
                self.result = ReplicationSuboperationResult::ReplicatedBytes(replicated_bytes);
                self.write_reference_state()
            }
            Ok(VersionReplicationMessage::VersionApplyRejected(reason)) => {
                self.fail(ReplicationError::ReplicationRejected(reason).into())
            }
            Ok(_) => self.invalid(
                "VersionReplicationMessage::VersionApplyComplete|VersionApplyRejected",
                received,
            ),
            Err(err) => self.fail(err.into()),
        }
    }
}

// Phase: close/cleanup
// Dropping locally materialized reference bytes, recording the reference
// fingerprint and closing the stream.
impl ReplicateObjectOperation {
    fn accept_blob_cleanup(&mut self, event: Event) -> ObjectTurn {
        match event.into_blob() {
            Ok(BlobEvent::DeleteFinished) | Ok(BlobEvent::Error(_)) => self.close_connection(),
            Ok(other) => self.invalid(
                "Event::Blob(BlobEvent::DeleteFinished|Error)",
                other.label(),
            ),
            Err(other) => self.invalid(
                "Event::Blob(BlobEvent::DeleteFinished|Error)",
                other.label(),
            ),
        }
    }

    fn accept_connection_closed(&mut self, event: Event) -> ObjectTurn {
        let received = event.label();
        let Ok(BlobEvent::ConnectionClosed { .. }) = event.into_blob() else {
            return self.invalid("Event::Blob(BlobEvent::ConnectionClosed)", received);
        };
        Ok(Next::Done(self.result.clone()))
    }
}

#[cfg(test)]
#[path = "version_replication_tests.rs"]
mod tests;
