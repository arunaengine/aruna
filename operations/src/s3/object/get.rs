//! Runs the S3 GetObject state machine, including range reads and reference drift successors.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::connectors::repository::{connector_reference_write, source_connector_key};
use crate::connectors::{ResolveBindingInput, resolve_binding_effect};
use crate::distribution::held::{
    HeldElsewhere, kept_location, kept_reads, kept_source_location, kept_source_reads,
};
use crate::distribution::inventory::{StagedCall, outside_answer};
use crate::driver::{DriverContext, drive};
use crate::replication::bao_read::{BaoReadError, BaoReadOutput};
use crate::replication::protocol::BaoReadRefusal;
use crate::replication::queue::{LiveVersionInput, LiveVersionOperation};
use crate::s3::object::lookup::{
    ExpectedNode, LookupError, check_copy_values, copy_check_call, location_from_value,
    summary_call, summary_from_value,
};
use crate::s3::object::metadata::{QueueRefreshOperation, ReferenceRefresh};
use crate::s3::object::put::abe::keyed;
use crate::s3::object::source::choice::{Candidate, choose_source};
use crate::s3::usage::{RowChange, UsageCounterUpdate, UsageUpdateError};
use futures_util::StreamExt;
use std::collections::{HashMap, VecDeque};
use std::ops::Range;
use std::time::{Duration, SystemTime};

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::SharedSecret;
use aruna_core::effects::{BlobEffect, Effect, StagingSourceEffect};
use aruna_core::errors::{
    BlobError, ConversionError, SourceResolutionError, StagingSourceError, StorageError,
};
use aruna_core::events::{BlobEvent, Event, StagingSourceEvent, SubOperationEvent};
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE,
    BUCKET_ENCRYPTION_KEYSPACE, S3_BUCKET_KEYSPACE, SOURCE_INDEX_KEYSPACE,
};
use aruna_core::managed_copy::ManagedCopyError;
use aruna_core::operation::{Operation, TransactionMode, boxed_suboperation};
use aruna_core::rows::read_row;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::checksum::{ExpectedChecksum, HASH_MD5, unmatched_checksum};
use aruna_core::structs::execution::source_access::{
    ObservationMatch, ResolvedSourceAccess, SourceMetadata, SourceStats, source_weight,
};
use aruna_core::structs::execution::source_connector::{SourceConnector, SourceConnectorKind};
use aruna_core::structs::execution::staging::VersionSourceBinding;
use aruna_core::structs::identity::auth::{AuthContext, PathRestriction};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::policy::{PlacementPolicyError, PlacementPolicyRef};
use aruna_core::structs::storage::abe::{AbeEffect, AbeError};
use aruna_core::structs::storage::blob::{
    ArchiveKey, BackendLocation, BackendRef, BlobHeadKey, BlobLocationKey, BlobVersion,
    BlobVersionState, BucketInfo, CurrentVersionPointer, ManagedCopyKey, VersionKey,
};
use aruna_core::structs::storage::distribution::source_root::RootEntry;
use aruna_core::structs::storage::encryption::{
    BucketEncryption, BucketKeyError, BucketKeyRef, ReadLease,
};
use aruna_core::structs::storage::multipart::MultipartChecksumType;
use aruna_core::structs::storage::replication_queue::LiveObligationRecord;
use aruna_core::structs::storage::replication_queue::ReferenceAdvance;
use aruna_core::structs::storage::replication_queue::live_obligation_entry;
use aruna_core::structs::storage::usage::UsageDelta;
use aruna_core::types::{Key, Value};
use aruna_core::{NodeId, UserId};
use bytes::Bytes;
use thiserror::Error;
use tracing::{debug, warn};
use ulid::Ulid;

/// Maximum successors attempted before a still-changing current read fails.
const MAX_DRIFT_ATTEMPTS: u8 = 3;

/// Minimum age of the current reference version before a read may mint another
/// successor. Rate-limits a READ-only caller pointing at a source they control.
pub const MIN_ADVANCE_INTERVAL: Duration = Duration::from_secs(60);

/// Hard bound on automatic successors per explicit binding: the interval only
/// slows growth, this stops it. Only a WRITE or rebind starts a fresh count.
pub const MAX_AUTO_ADVANCES: u16 = 100;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GetObjectState {
    GetVersion,
    CheckManagedCopy,
    GetBlobLocation,
    GetCurrentVersion,
    /// A distributed version kept elsewhere first looks for this member's kept copy.
    ReadKept,
    /// A reference of a distributed bucket reads its bucket, then this member's kept copy.
    ReadSourceBucket,
    ReadSourceKept,
    ResolveReferenceAccess,
    /// Reads the connectors of several sources for their current weight hint.
    ReadSourceWeights,
    /// Asks how several sources performed lately, to pick the one this read uses.
    RankSources,
    ReadMultipartSummary,
    HeadReferenceSource,
    /// Records a same-binding successor of a drifted reference in its own transaction.
    AdvanceReference,
    QueueSuccessorReplication,
    GetBlob,
    ReadReferenceSource,
    /// The bound read failed its condition: a refresh is queued to observe the source.
    QueueChangedRefresh,
    /// Unconfirmed bytes are spooled locally and checked against the recorded digests first.
    SpoolReference,
    /// A sealed copy waits for a read lease of its bucket key.
    AdmitRead,
    /// A plain copy of an encrypting bucket reads its settings, then waits for a bucket lease.
    ReadContentSettings,
    AdmitPlain,
    /// A reference version of an encrypting bucket needs its bucket unlocked.
    ReadReferenceSettings,
    CheckReferenceKey,
    Init,
}

#[derive(Debug, Error, PartialEq)]
pub enum GetObjectError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("Invalid state [{current:?}] - expected [{expected:?}]")]
    InvalidState {
        current: GetObjectState,
        expected: GetObjectState,
    },
    #[error("State [{state:?}] invalid: expected [{expected:?}] - received [{received}]")]
    InvalidStateEvent {
        state: GetObjectState,
        expected: &'static str,
        received: &'static str,
    },
    #[error("No transaction found")]
    NoTransactionFound,
    #[error("The specified key does not exist.")]
    NoSuchKey,
    #[error("The specified version does not exist.")]
    NoSuchVersion,
    #[error("The specified version is a delete marker.")]
    DeleteMarker,
    #[error("The requested range is not satisfiable.")]
    InvalidRange,
    #[error("Reference source metadata changed during access.")]
    ReferenceSourceChanged,
    #[error("The reference source no longer matches its recorded checksum.")]
    ReferenceChecksumMismatch,
    #[error("The historical reference version is no longer available.")]
    HistoricalReferenceUnavailable,
    #[error(
        "The reference binding reached its automatic advance limit; rebind it with an explicit write."
    )]
    ReferenceAdvanceExhausted,
    #[error(transparent)]
    UsageError(#[from] UsageUpdateError),
    #[error(transparent)]
    ResolveReferenceError(#[from] SourceResolutionError),
    #[error(transparent)]
    StagingSourceError(#[from] StagingSourceError),
    #[error(transparent)]
    ManagedCopyError(#[from] ManagedCopyError),
    #[error(transparent)]
    PolicyError(#[from] PlacementPolicyError),
    /// The version is known here but its bytes are not. On an infrastructure
    /// node that is a fault; on a device it is the ordinary case, and the read
    /// continues against the realm's holders.
    #[error("The object bytes are not stored on this node.")]
    BlobNotLocal {
        blake3: [u8; 32],
        version_id: Option<Ulid>,
        metadata: HashMap<String, String>,
        version_created_at: Option<SystemTime>,
        source_policies: Vec<PlacementPolicyRef>,
    },
    /// No holder served the bytes and at least one answered with an
    /// infrastructure failure, so this is a fault, never object absence.
    #[error("The object bytes are unavailable from every holder.")]
    HoldersUnavailable,
    /// This member keeps the version's record but not its bytes; they are read through a
    /// holder of its range, per the member's data strategy.
    #[error("The object bytes are kept by other members of the distributed bucket.")]
    HeldElsewhere(Box<HeldElsewhere>),
    /// At least one holder returned bytes that failed integrity verification.
    #[error("Object bytes from a holder failed integrity verification.")]
    HolderIntegrityFailure,
    /// A holder refused this caller access to the object.
    #[error("Access to the object was denied by its holder.")]
    HolderAccessDenied,
    /// Policy-covered content the holders will not serve to a device, which is
    /// never a legal destination for governed data. Terminal: the S3 answer is
    /// an honest 403, not a fault a retry could clear.
    #[error("Governed content is not available on a user node.")]
    GovernedUnavailable,
    /// Unverified reference reads already hold the node's spool budget; a retry may fit.
    #[error("Too many unverified reference reads are in progress.")]
    SpoolBudgetExhausted,
    /// The bytes cannot be verified: a spool that large never fits the node's spool budget.
    #[error("Reference reads above {limit} bytes need a source that enforces ETag conditions.")]
    ReferenceTooLarge { limit: u64 },
    #[error("GetObject failed (miserably)")]
    GetObjectFailed,
    #[error("operation did not finish")]
    NotFinished,
    /// The version waits for its content hash; a token credential or object key promotes it first.
    #[error("The object content waits for its bucket key.")]
    PendingContent(ArchiveKey),
    /// The version is a link: the caller reads its target under its own rights there.
    #[error("The version links to another object.")]
    Link(Box<(VersionSourceBinding, SourceMetadata)>),
    #[error(transparent)]
    Abe(#[from] aruna_core::structs::storage::abe::AbeError),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ObjectRangeRequest {
    StartEnd { start: u64, end: u64 },
    Start { start: u64 },
    Suffix { length: u64 },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedObjectRange {
    pub range: Range<u64>,
    pub content_length: i64,
    pub content_range: String,
}

impl ObjectRangeRequest {
    pub fn resolve(&self, full_length: u64) -> Result<ResolvedObjectRange, GetObjectError> {
        if full_length == 0 {
            return Err(GetObjectError::InvalidRange);
        }

        let range = match self {
            ObjectRangeRequest::StartEnd { start, end } => {
                if start > end || *start >= full_length {
                    return Err(GetObjectError::InvalidRange);
                }
                *start..((*end).min(full_length - 1) + 1)
            }
            ObjectRangeRequest::Start { start } => {
                if *start >= full_length {
                    return Err(GetObjectError::InvalidRange);
                }
                *start..full_length
            }
            ObjectRangeRequest::Suffix { length } => {
                if *length == 0 {
                    return Err(GetObjectError::InvalidRange);
                }
                full_length.saturating_sub(*length)..full_length
            }
        };

        Ok(ResolvedObjectRange {
            content_range: format!("bytes {}-{}/{}", range.start, range.end - 1, full_length),
            content_length: (range.end - range.start) as i64,
            range,
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct GetObjectInput {
    pub bucket: String,
    pub key: String,
    pub version_id: Option<Ulid>,
    pub range: Option<ObjectRangeRequest>,
    pub group_id: Ulid,
    pub user_identity: UserId,
    /// Realm serving the read; the user may belong to another realm.
    pub realm_id: RealmId,
    /// Local node recorded on the replication obligation of a drift successor.
    pub node_id: NodeId,
}

/// Authoritative whole-object facts of one read, carried independently of any
/// physical location. `size` is the whole-object size; a ranged read reports its
/// response length in `GetObjectResult::resolved_range` instead.
#[derive(Clone, Debug, PartialEq)]
pub struct ObjectInfo {
    pub size: u64,
    pub version_created_at: Option<SystemTime>,
    pub etag: Option<String>,
    pub checksum_type: MultipartChecksumType,
    pub hashes: HashMap<String, Vec<u8>>,
    pub composite_hashes: HashMap<String, Vec<u8>>,
    pub part_count: Option<usize>,
}

#[derive(Debug, PartialEq)]
pub struct GetObjectResult {
    pub blob: BackendStream<Result<Bytes, StreamError>>,
    pub location: Option<BackendLocation>,
    pub metadata: HashMap<String, String>,
    pub info: ObjectInfo,
    pub source_metadata: Option<SourceMetadata>,
    pub source_binding: Option<VersionSourceBinding>,
    pub last_refresh: Option<SystemTime>,
    pub version_id: Option<Ulid>,
    pub resolved_version_id: Option<Ulid>,
    pub resolved_range: Option<ResolvedObjectRange>,
    /// Refs stored on the version that was read. A copy unions them with its
    /// destination default, so a copy is never less constrained than its source.
    pub source_policies: Vec<PlacementPolicyRef>,
    /// The one source of a reference that delivered the bytes.
    pub served_source: Option<VersionSourceBinding>,
}

impl From<IoError> for GetObjectError {
    fn from(error: IoError) -> Self {
        Self::StorageError(error.into())
    }
}

type Turn = Result<Next<GetObjectResult>, GetObjectError>;

#[derive(Debug, PartialEq)]
pub struct GetObjectOperation {
    input: GetObjectInput,
    state: GetObjectState,
    /// Still reading the object's records, before the bytes or the source are read.
    reading: bool,
    /// A reference advance lost to another writer: the records are read again, each on its own.
    restarted: bool,
    location: Option<BackendLocation>,
    reference_access: Option<ResolvedSourceAccess>,
    /// Stored observation of the reference version being read, the drift baseline.
    reference_cached: Option<SourceMetadata>,
    /// `last_refresh` of the stored reference version; served when undrifted.
    reference_last_refresh: Option<SystemTime>,
    /// Recorded digests a complete read of an unconfirmed reference must match.
    verify_digests: Vec<ExpectedChecksum>,
    /// Whether the caller pinned an explicit version (a historical read).
    reference_explicit: bool,
    /// Fresh observation to record in the successor and then serve.
    advance_observation: Option<SourceMetadata>,
    /// Advance attempts so a still-drifting source cannot spin forever.
    drift_attempts: u8,
    /// Reader's scoped credential, carried onto the successor's obligation.
    restrictions: Option<Vec<PathRestriction>>,
    /// Original creator retained as successor attribution.
    reference_creator: Option<UserId>,
    /// Publisher-attested predecessor lineage for the successor.
    reference_advance: Option<ReferenceAdvance>,
    /// Injected wall clock for the advance interval check; tests set it.
    now_override: Option<SystemTime>,
    metadata: HashMap<String, String>,
    source_metadata: Option<SourceMetadata>,
    source_binding: Option<VersionSourceBinding>,
    last_refresh: Option<SystemTime>,
    version_created_at: Option<SystemTime>,
    resolved_version_id: Option<Ulid>,
    checksum_type: MultipartChecksumType,
    composite_hashes: HashMap<String, Vec<u8>>,
    part_count: Option<usize>,
    resolved_range: Option<ResolvedObjectRange>,
    /// Hash of the location this read is waiting on, so a local miss can name
    /// the blob a routed read would have to fetch.
    missing_blake3: Option<[u8; 32]>,
    /// Held while a governed version's local registration is verified.
    pending_location: Option<BlobLocationKey>,
    pending_copy: Option<ManagedCopyKey>,
    /// Refs of the version being read, carried to copy and advance writes.
    source_policies: Vec<PlacementPolicyRef>,
    /// A distributed version this member keeps no bytes of, unless a kept copy turns up.
    held: Option<HeldElsewhere>,
    /// The logical identity of a reference in a distributed bucket, its kept copy's key.
    source_identity: Option<[u8; 32]>,
    /// The distributed bucket of such a reference, whose members may keep its bytes.
    source_set: Option<Ulid>,
    /// Resolved sources of the reference not tried yet, by slot, in the order they are tried.
    source_queue: VecDeque<(usize, ResolvedSourceAccess)>,
    /// The slot of the source being resolved, then of the source being read.
    source_slot: usize,
    /// The source this read reads; a restarted read keeps it, so one read uses one source.
    chosen_source: Option<VersionSourceBinding>,
    /// The operator's weight of each queued source, read from its connector at ranking.
    source_weights: Vec<f64>,
    /// Why the last source failed, reported when no other source serves.
    source_failure: Option<GetObjectError>,
    /// Active key of an encrypting bucket whose reference or unsealed copy is read.
    reference_key: Option<BucketKeyRef>,
    /// Admitted plaintext read of that bucket; the served stream keeps it until it ends.
    reference_lease: Option<ReadLease>,
    /// The request has a token, so a pending version answers `PendingContent`.
    token: bool,
    object: Option<(
        aruna_core::structs::storage::abe::ObjectEnvelope,
        aruna_core::structs::storage::abe::EnvelopeArchive,
        aruna_core::compute::SharedSecret,
    )>,
}

impl GetObjectOperation {
    pub fn new(input: GetObjectInput) -> Self {
        GetObjectOperation {
            input,
            state: GetObjectState::Init,
            reading: true,
            restarted: false,
            location: None,
            reference_access: None,
            reference_cached: None,
            reference_last_refresh: None,
            verify_digests: Vec::new(),
            reference_explicit: false,
            advance_observation: None,
            drift_attempts: 0,
            restrictions: None,
            reference_creator: None,
            reference_advance: None,
            now_override: None,
            metadata: HashMap::new(),
            source_metadata: None,
            source_binding: None,
            last_refresh: None,
            version_created_at: None,
            resolved_version_id: None,
            checksum_type: MultipartChecksumType::FullObject,
            composite_hashes: HashMap::new(),
            part_count: None,
            resolved_range: None,
            missing_blake3: None,
            pending_location: None,
            pending_copy: None,
            source_policies: Vec::new(),
            held: None,
            source_identity: None,
            source_set: None,
            source_queue: VecDeque::new(),
            source_slot: 0,
            chosen_source: None,
            source_weights: Vec::new(),
            source_failure: None,
            reference_key: None,
            reference_lease: None,
            token: false,
            object: None,
        }
    }

    pub fn with_restrictions(mut self, restrictions: Option<Vec<PathRestriction>>) -> Self {
        self.restrictions = restrictions;
        self
    }

    /// Marks a request with a token, which can open the object key of a pending version.
    pub fn with_token(mut self, token: bool) -> Self {
        self.token = token;
        self
    }

    pub fn with_object(
        mut self,
        envelope: aruna_core::structs::storage::abe::ObjectEnvelope,
        archive: aruna_core::structs::storage::abe::EnvelopeArchive,
        private: aruna_core::compute::SharedSecret,
    ) -> Self {
        self.object = Some((envelope, archive, private));
        self
    }

    fn auth_context(&self) -> AuthContext {
        AuthContext {
            user_id: self.input.user_identity,
            realm_id: self.input.realm_id,
            path_restrictions: self.restrictions.clone(),
            session: None,
        }
    }

    fn call(&mut self, state: GetObjectState, call: IoCall) -> Turn {
        self.state = state;
        Ok(Next::Call(call))
    }

    fn effect(&mut self, state: GetObjectState, effect: Effect) -> Turn {
        self.call(state, effect.into())
    }

    /// A read of the object's records: in the read snapshot, or on its own after a restart.
    fn read(&mut self, state: GetObjectState, call: IoCall) -> Turn {
        let call = match self.restarted {
            true => StagedCall::Outside(call).into_call(),
            false => call,
        };
        self.call(state, call)
    }

    fn unexpected(&self, expected: &'static str, event: Event) -> GetObjectError {
        GetObjectError::InvalidStateEvent {
            state: self.state.clone(),
            expected,
            received: event.label(),
        }
    }

    fn lookup_error(&self, expected: &'static str, error: LookupError) -> GetObjectError {
        match error {
            LookupError::Conversion(err) => GetObjectError::ConversionError(err),
            LookupError::Managed(err) => GetObjectError::ManagedCopyError(err),
            LookupError::InvalidEvent(received) => GetObjectError::InvalidStateEvent {
                state: self.state.clone(),
                expected,
                received: received.label(),
            },
            LookupError::Missing => GetObjectError::GetObjectFailed,
        }
    }

    /// The version the caller named, or the head pointer of the key.
    fn read_first(&mut self) -> Turn {
        if let Some(version_id) = self.input.version_id {
            let key =
                VersionKey::new(&self.input.bucket, &self.input.key, version_id).to_bytes()?;
            return self.read(
                GetObjectState::GetVersion,
                read_row(BLOB_VERSIONS_KEYSPACE, key),
            );
        }
        let key = BlobHeadKey::new(&self.input.bucket, &self.input.key).to_bytes()?;
        self.read(
            GetObjectState::GetCurrentVersion,
            read_row(BLOB_HEAD_KEYSPACE, key),
        )
    }

    fn handle_received_version(&mut self, value: Option<Value>) -> Turn {
        let Some(val) = value else {
            return Err(match self.input.version_id.is_some() {
                true => GetObjectError::NoSuchVersion,
                false => GetObjectError::NoSuchKey,
            });
        };
        let version = BlobVersion::from_bytes(val.as_ref())?;
        let version_id = self.resolved_version_id.or(self.input.version_id);
        let version_id = version_id.ok_or(GetObjectError::GetObjectFailed)?;
        self.read_version(version_id, version, self.input.version_id.is_some())
    }

    fn current_version_received(&mut self, value: Option<Value>) -> Turn {
        let val = value.ok_or(GetObjectError::NoSuchKey)?;
        let pointer = CurrentVersionPointer::from_bytes(val.as_ref())?;
        let key = VersionKey::new(&self.input.bucket, &self.input.key, pointer.version_id);
        self.resolved_version_id = Some(pointer.version_id);
        self.read(
            GetObjectState::GetVersion,
            read_row(BLOB_VERSIONS_KEYSPACE, key.to_bytes()?),
        )
    }

    fn read_version(
        &mut self,
        version_id: Ulid,
        version: BlobVersion,
        explicit_version_request: bool,
    ) -> Turn {
        self.resolved_version_id = Some(version_id);
        self.metadata = version.metadata.clone();
        self.source_policies = version.placement_policies.clone();

        match version.state {
            BlobVersionState::Materialized {
                blob_hash,
                backend,
                encoding,
                source,
            } => {
                let location_key = BlobLocationKey::new(blob_hash, encoding, backend);
                self.source_binding = source;
                self.version_created_at = Some(version.created_at);
                if version.placement_policies.is_empty() {
                    return self.read_blob_location(location_key);
                }
                self.check_managed_copy(version_id, location_key)
            }
            BlobVersionState::Deleted => Err(match explicit_version_request {
                true => GetObjectError::DeleteMarker,
                false => GetObjectError::NoSuchKey,
            }),
            BlobVersionState::Reference {
                source,
                cached_metadata,
                ..
            } if HeldElsewhere::of(&source, version.identity).is_some() => {
                let Some((set_id, identity)) = HeldElsewhere::of(&source, version.identity) else {
                    return Err(GetObjectError::GetObjectFailed);
                };
                let held = HeldElsewhere {
                    set_id,
                    version_id,
                    identity,
                    blake3: Some(identity),
                    size: cached_metadata.content_length,
                    metadata: version.metadata,
                    version_created_at: Some(version.created_at),
                    source_policies: version.placement_policies,
                };
                self.version_created_at = held.version_created_at;
                // A governed version is never kept, so only its holders serve it.
                if !held.source_policies.is_empty() {
                    return Err(GetObjectError::HeldElsewhere(Box::new(held)));
                }
                let reads = kept_reads(&held);
                self.held = Some(held);
                self.read(GetObjectState::ReadKept, IoCall::Read(reads))
            }
            BlobVersionState::Reference {
                source,
                cached_metadata,
                last_refresh,
                ..
            } => {
                if source.descriptor.kind == SourceConnectorKind::ArunaLink {
                    let linked = Box::new((source, cached_metadata));
                    return Err(GetObjectError::Link(linked));
                }
                // Access-driven successor-on-drift (#256); verified cache,
                // origin relay and sync polling stay deferred (#375/#380/#314).
                self.source_binding = Some(source.clone());
                self.reference_cached = Some(cached_metadata);
                self.reference_last_refresh = Some(last_refresh);
                self.reference_explicit = explicit_version_request;
                self.reference_creator = Some(version.created_by);
                self.location = None;
                self.reference_access = None;
                self.source_metadata = None;
                self.last_refresh = None;
                self.version_created_at = None;
                // Only a distributed bucket fixes an identity; its member may keep the bytes.
                self.source_identity = version.identity;
                if version.identity.is_some() {
                    let read = read_row(S3_BUCKET_KEYSPACE, self.input.bucket.as_bytes().to_vec());
                    return self.read(GetObjectState::ReadSourceBucket, read);
                }
                self.resolve_reference()
            }
            BlobVersionState::PendingContent { archive, .. }
                if self.token || self.object.is_some() =>
            {
                Err(GetObjectError::PendingContent(archive))
            }
            BlobVersionState::PendingContent { .. } => {
                Err(ConversionError::BucketKey(BucketKeyError::Unsupported).into())
            }
        }
    }

    fn resolve_reference(&mut self) -> Turn {
        self.source_queue.clear();
        self.source_slot = 0;
        self.source_failure = None;
        self.resolve_source()
    }

    /// Resolves the source in `source_slot`; the sources of a binding resolve one by one.
    fn resolve_source(&mut self) -> Turn {
        let source = self.source_binding.as_ref();
        let source = source.and_then(|binding| binding.source_at(self.source_slot));
        let source = source.ok_or(GetObjectError::GetObjectFailed)?;
        let group_id = Some(self.input.group_id);
        let resolve = resolve_binding_effect(ResolveBindingInput { source, group_id });
        self.effect(GetObjectState::ResolveReferenceAccess, resolve)
    }

    /// Orders the resolved sources: one picked by measured speed, then the others in their
    /// order, failing ones last. A restarted read keeps its source while it is not failing.
    fn sources_ranked(&mut self, event: Event) -> Turn {
        let (stats, draw) = match event {
            Event::StagingSource(StagingSourceEvent::HealthResult { stats, draw })
                if stats.len() == self.source_queue.len() =>
            {
                (stats, draw)
            }
            other => {
                return Err(self.unexpected(
                    "Event::StagingSource(StagingSourceEvent::HealthResult)",
                    other,
                ));
            }
        };
        let weights = self.source_weights.iter().copied();
        let candidates: Vec<Candidate> = (self.source_queue.iter().zip(stats).zip(weights))
            .map(|(((slot, access), stats), weight)| self.candidate(*slot, access, stats, weight))
            .collect();
        let choice = choose_source(&candidates, draw);
        let mut order = choice.order;
        let binding = self.source_binding.as_ref();
        let chosen = self.chosen_source.as_ref();
        let kept = (order.iter()).position(|&index| {
            let source = binding.and_then(|binding| binding.source_at(self.source_queue[index].0));
            let same = chosen
                .zip(source.as_ref())
                .is_some_and(|(a, b)| RootEntry::same_source(a, b));
            same && !candidates[index].stats.failing
        });
        if let Some(position) = kept {
            let index = order.remove(position);
            order.insert(0, index);
        }
        let mut queue: Vec<_> = self.source_queue.drain(..).map(Some).collect();
        self.source_queue = order
            .iter()
            .filter_map(|&index| queue[index].take())
            .collect();
        debug!(
            slot = self.source_queue.front().map(|(slot, _)| *slot),
            sources = candidates.len(),
            share = choice.share,
            kept = kept.is_some(),
            "Picked a reference source"
        );
        self.next_source()
    }

    /// One source as a choice for this read. The first source binds the recorded ETag; another
    /// one serves bytes the recorded checksums prove, read whole until its HEAD shows an equal
    /// strong ETag it enforces, or only bound to that ETag when there are no checksums.
    fn candidate(
        &self,
        slot: usize,
        access: &ResolvedSourceAccess,
        stats: SourceStats,
        weight: f64,
    ) -> Candidate {
        let ResolvedSourceAccess::OpenDal { kind, .. } = access;
        let cached = self.reference_cached.as_ref();
        let proven = cached.is_some_and(|cached| !cached.enforced_checksums().is_empty());
        let binds = kind.enforces_if_match()
            && cached.is_some_and(|cached| strong_validator(cached.etag.as_deref()));
        let length = cached.map_or(0, |cached| cached.content_length);
        let range = (self.input.range.as_ref()).and_then(|range| range.resolve(length).ok());
        let ranged = match slot {
            0 => binds || !proven,
            _ => !proven,
        };
        let bytes = match ranged {
            true => range.map_or(length, |range| range.range.end - range.range.start),
            false => length,
        };
        Candidate {
            stats,
            weight,
            bytes,
            eligible: slot == 0 || proven || binds,
        }
    }

    /// Reads the next source: the first opens the read, a later one only heads its source.
    fn next_source(&mut self) -> Turn {
        let Some((slot, access)) = self.source_queue.pop_front() else {
            let error = self.source_failure.take();
            return self.source_failed(error.unwrap_or(GetObjectError::GetObjectFailed));
        };
        self.source_slot = slot;
        self.chosen_source = self.source_binding.as_ref().and_then(|b| b.source_at(slot));
        self.reference_access = Some(access.clone());
        self.verify_digests.clear();
        if self.reading {
            return self.read_reference();
        }
        let head = Effect::StagingSource(StagingSourceEffect::Head { access });
        self.effect(GetObjectState::HeadReferenceSource, head)
    }

    /// A member of a distributed bucket looks for its kept copy of the reference first.
    fn source_bucket_read(&mut self, value: Option<Value>) -> Turn {
        let binding = value
            .as_deref()
            .and_then(|value| BucketInfo::from_bytes(value).ok())
            .and_then(|bucket| bucket.distribution);
        let (Some(binding), Some(identity), Some(version_id)) =
            (binding, self.source_identity, self.resolved_version_id)
        else {
            return self.resolve_reference();
        };
        self.source_set = Some(binding.set_id);
        if !self.source_policies.is_empty() {
            return self.resolve_reference();
        }
        let reads = kept_source_reads(binding.set_id, version_id, identity);
        self.read(GetObjectState::ReadSourceKept, IoCall::Read(reads))
    }

    /// Serves the kept copy of a reference without asking its source, or reads the source.
    fn source_kept_read(&mut self, values: Vec<(Key, Option<Value>)>) -> Turn {
        let size = (self.reference_cached.as_ref()).map(|cached| cached.content_length);
        match size.and_then(|size| kept_source_location(&values, size)) {
            Some(location) => self.read_multipart_summary(location, self.resolved_version_id),
            None => self.resolve_reference(),
        }
    }

    /// A distributed member whose source failed reads the bytes from a member keeping a copy,
    /// so data stays readable without its origin; elsewhere the failure stands.
    fn source_failed(&mut self, error: GetObjectError) -> Turn {
        // A source that lacks the object never hides another source's outage or refusal.
        let absent = GetObjectError::StagingSourceError(StagingSourceError::NotFound);
        let error = match self.source_failure.take() {
            Some(earlier) if error == absent => earlier,
            _ => error,
        };
        if !self.source_queue.is_empty() {
            debug!(slot = self.source_slot, error = %error, "Reference source failed, trying another");
            self.source_failure = Some(error);
            return self.next_source();
        }
        let held = (self.source_set.zip(self.source_identity))
            .zip(self.resolved_version_id.zip(self.reference_cached.as_ref()))
            .map(|((set_id, identity), (version_id, cached))| HeldElsewhere {
                set_id,
                version_id,
                identity,
                blake3: None,
                size: cached.content_length,
                metadata: self.metadata.clone(),
                version_created_at: None,
                source_policies: self.source_policies.clone(),
            });
        match held {
            Some(held) => Err(GetObjectError::HeldElsewhere(Box::new(held))),
            None => Err(error),
        }
    }

    /// Reads this member's kept copy, or hands the read to the holders of the version.
    fn kept_read(&mut self, values: Vec<(Key, Option<Value>)>) -> Turn {
        let held = self.held.take().ok_or(GetObjectError::GetObjectFailed)?;
        match kept_location(&held, &values) {
            Some(location) => self.read_multipart_summary(location, Some(held.version_id)),
            None => Err(GetObjectError::HeldElsewhere(Box::new(held))),
        }
    }

    fn read_blob_location(&mut self, key: BlobLocationKey) -> Turn {
        self.missing_blake3 = Some(key.blake3_hash);
        let read = read_row(BLOB_LOCATIONS_KEYSPACE, key.to_bytes());
        self.read(GetObjectState::GetBlobLocation, read)
    }

    /// A governed version is only serveable from a registered local copy, so an
    /// unregistered or quarantined copy fails closed before any byte moves.
    fn check_managed_copy(&mut self, version_id: Ulid, location_key: BlobLocationKey) -> Turn {
        let version = VersionKey::new(&self.input.bucket, &self.input.key, version_id);
        let copy_key = ManagedCopyKey::new(version, location_key.backend.clone());
        let call = copy_check_call(&copy_key)?;
        self.pending_copy = Some(copy_key);
        self.pending_location = Some(location_key);
        self.read(GetObjectState::CheckManagedCopy, call)
    }

    fn handle_managed_copy(&mut self, values: Vec<Option<Value>>) -> Turn {
        let key = check_copy_values(
            values,
            &mut self.pending_copy,
            &mut self.pending_location,
            &self.source_policies,
            ExpectedNode::Exact(self.input.node_id),
        )
        .map_err(|error| self.lookup_error("IoResult::Values", error))?;
        self.read_blob_location(key)
    }

    fn location_read(&mut self, value: Option<Value>) -> Turn {
        let location = location_from_value(value)
            .map_err(|error| self.lookup_error("IoResult::Values", error))?;
        let Some(location) = location else {
            return Err(match self.missing_blake3 {
                Some(blake3) => GetObjectError::BlobNotLocal {
                    blake3,
                    version_id: self.resolved_version_id,
                    metadata: self.metadata.clone(),
                    version_created_at: self.version_created_at,
                    source_policies: self.source_policies.clone(),
                },
                None => GetObjectError::GetObjectFailed,
            });
        };
        self.read_multipart_summary(location, self.resolved_version_id)
    }

    fn reference_access_resolved(&mut self, event: Event) -> Turn {
        let result = match event {
            Event::SubOperation(SubOperationEvent::VersionAccessResolved { result }) => result,
            other => {
                return Err(self.unexpected(
                    "Event::SubOperation(SubOperationEvent::VersionSourceAccessResolved)",
                    other,
                ));
            }
        };
        let count = (self.source_binding.as_ref()).map_or(1, VersionSourceBinding::source_count);
        match result {
            Ok(access) => self.source_queue.push_back((self.source_slot, access)),
            Err(error) => self.source_failure = Some(error.into()),
        }
        self.source_slot += 1;
        if self.source_slot < count {
            return self.resolve_source();
        }
        if self.source_queue.len() < 2 {
            return self.next_source();
        }
        self.read_weights()
    }

    /// Reads the connectors of the queued sources, whose weight hint may change any time.
    fn read_weights(&mut self) -> Turn {
        let binding = self.source_binding.as_ref();
        let group_id = self.input.group_id;
        let reads: Vec<_> = (self.source_queue.iter())
            .filter_map(|(slot, _)| binding?.source_at(*slot)?.connector_id)
            .map(|id| {
                let key = source_connector_key(group_id, id);
                (SOURCE_INDEX_KEYSPACE.to_string(), key)
            })
            .collect();
        if reads.is_empty() {
            return self.ask_health(HashMap::new());
        }
        self.read(GetObjectState::ReadSourceWeights, IoCall::Read(reads))
    }

    fn weights_read(&mut self, values: Vec<Option<Value>>) -> Turn {
        let connectors = values.iter().filter_map(|value| value.as_deref());
        let weights = connectors
            .filter_map(|value| SourceConnector::from_bytes(value).ok())
            .filter_map(|connector| {
                Some((
                    connector.connector_id,
                    source_weight(&connector.public_config)?,
                ))
            })
            .collect();
        self.ask_health(weights)
    }

    fn ask_health(&mut self, weights: HashMap<Ulid, f64>) -> Turn {
        let binding = self.source_binding.as_ref();
        let weight = |slot: usize| {
            let connector_id = binding.and_then(|binding| binding.source_at(slot)?.connector_id);
            connector_id.and_then(|id| weights.get(&id).copied())
        };
        // A source without a connector id reads through the connector its resolution matched.
        let weights = (self.source_queue.iter()).map(|(slot, access)| {
            let ResolvedSourceAccess::OpenDal { config, .. } = access;
            weight(*slot)
                .or_else(|| source_weight(config))
                .unwrap_or(1.0)
        });
        self.source_weights = weights.collect();
        let accesses = self.source_queue.iter().map(|(_, access)| access.clone());
        let health = Effect::StagingSource(StagingSourceEffect::Health {
            accesses: accesses.collect(),
        });
        self.effect(GetObjectState::RankSources, health)
    }

    fn read_multipart_summary(
        &mut self,
        location: BackendLocation,
        resolved_version_id: Option<Ulid>,
    ) -> Turn {
        self.location = Some(location);
        self.resolved_version_id = resolved_version_id;
        let Some(version_id) = resolved_version_id else {
            return self.read_blob();
        };
        let call = summary_call(version_id)?;
        self.read(GetObjectState::ReadMultipartSummary, call)
    }

    fn summary_read(&mut self, value: Option<Value>) -> Turn {
        let summary = summary_from_value(value)
            .map_err(|error| self.lookup_error("IoResult::Values", error))?;
        if let Some(summary) = summary {
            self.checksum_type = summary.checksum_type;
            self.composite_hashes = summary.composite_hashes;
            self.part_count = Some(summary.part_count);
        }
        self.read_blob()
    }

    fn read_blob(&mut self) -> Turn {
        let location = self.location.clone();
        let location = location.ok_or(GetObjectError::GetObjectFailed)?;
        self.resolved_range = match self.input.range.as_ref() {
            Some(range) => Some(range.resolve(location.blob_size)?),
            None => None,
        };
        // A sealed copy is read only under a lease of its key, admitted after the record reads.
        if let Some(key) = location.format.bucket_key() {
            let archive = ArchiveKey::of(&location);
            let admit = if let Some((envelope, mapping, private)) = self.object.take() {
                if mapping.archive != archive
                    || location.location_key().ok().map(|k| k.to_bytes())
                        != Some(mapping.location_key)
                    || envelope.context.parameters.key != key
                    || envelope.context.object_key != self.input.key
                {
                    return Err(GetObjectError::Abe(AbeError::Context));
                }
                BlobEffect::Abe(Box::new(AbeEffect::Admit {
                    envelope,
                    archive,
                    private,
                }))
            } else {
                BlobEffect::AdmitRead { key, archive }
            };
            self.reading = false;
            return self.effect(GetObjectState::AdmitRead, Effect::Blob(admit));
        }
        // A plain copy of an encrypting bucket, not converted yet, needs the bucket unlocked too.
        let read = read_row(
            BUCKET_ENCRYPTION_KEYSPACE,
            self.input.bucket.as_bytes().to_vec(),
        );
        self.read(GetObjectState::ReadContentSettings, read)
    }

    /// The read of a plain copy over the resolved range.
    fn plain_read(&self) -> Option<BlobEffect> {
        let location = self.location.clone()?;
        Some(match self.resolved_range.as_ref() {
            Some(range) => BlobEffect::ReadRange {
                location,
                range: range.range.clone(),
            },
            None => BlobEffect::Read { location },
        })
    }

    fn content_settings_read(&mut self, value: Option<Value>) -> Turn {
        let settings = BucketEncryption::from_row(value.as_deref())?;
        let (Some(read_effect), Some(location)) = (self.plain_read(), self.location.as_ref())
        else {
            return Err(GetObjectError::GetObjectFailed);
        };
        // The lease pins the physical copy, so conversion and cleanup wait for this read.
        let archive = ArchiveKey::of(location);
        self.reference_key = settings.active_key();
        self.reading = false;
        match self.reference_key {
            Some(key) => {
                let admit = BlobEffect::AdmitRead { key, archive };
                self.effect(GetObjectState::AdmitPlain, Effect::Blob(admit))
            }
            None => self.effect(GetObjectState::GetBlob, Effect::Blob(read_effect)),
        }
    }

    /// Reads a plain copy of an encrypting bucket once its bucket lease is admitted.
    fn plain_admitted(&mut self, event: Event) -> Turn {
        let (Some(key), Some(read_effect), Some(location)) = (
            self.reference_key,
            self.plain_read(),
            self.location.as_ref(),
        ) else {
            return Err(GetObjectError::GetObjectFailed);
        };
        let archive = ArchiveKey::of(location);
        match event {
            Event::Blob(BlobEvent::ReadAdmitted { lease })
                if lease.key == key && lease.archive == archive =>
            {
                self.reference_lease = Some(lease);
                self.effect(GetObjectState::GetBlob, Effect::Blob(read_effect))
            }
            Event::Blob(BlobEvent::Error(BlobError::BucketKey(error))) => Err(locked(error)),
            Event::Blob(BlobEvent::Error(_)) => Err(GetObjectError::GetObjectFailed),
            other => Err(self.unexpected("Event::Blob(BlobEvent::ReadAdmitted)", other)),
        }
    }

    /// Reads the sealed copy with the admitted lease; a locked key ends the read typed.
    fn read_admitted(&mut self, event: Event) -> Turn {
        let location = self.location.clone();
        let location = location.ok_or(GetObjectError::GetObjectFailed)?;
        let lease = match event {
            Event::Blob(BlobEvent::ReadAdmitted { lease })
                if Some(lease.key) == location.format.bucket_key()
                    && lease.archive == ArchiveKey::of(&location) =>
            {
                lease
            }
            // A locked key stays typed: `BucketKeyError::Locked` names the bucket.
            Event::Blob(BlobEvent::Error(BlobError::Abe(error))) => {
                return Err(GetObjectError::Abe(error));
            }
            Event::Blob(BlobEvent::Error(BlobError::BucketKey(error))) => {
                return Err(locked(error));
            }
            Event::Blob(BlobEvent::Error(_)) => return Err(GetObjectError::GetObjectFailed),
            other => return Err(self.unexpected("Event::Blob(BlobEvent::ReadAdmitted)", other)),
        };
        let range = (self.resolved_range.as_ref()).map(|range| range.range.clone());
        let read = BlobEffect::ReadSealed {
            location,
            range,
            lease: Box::new(lease),
        };
        self.effect(GetObjectState::GetBlob, Effect::Blob(read))
    }

    /// An encrypting bucket gates plaintext of every version, not only of sealed copies.
    fn read_reference(&mut self) -> Turn {
        if self.reference_access.is_none() {
            return Err(GetObjectError::GetObjectFailed);
        }
        let read = read_row(
            BUCKET_ENCRYPTION_KEYSPACE,
            self.input.bucket.as_bytes().to_vec(),
        );
        self.read(GetObjectState::ReadReferenceSettings, read)
    }

    /// The record reads are done; HEAD the source: the fresh observation decides whether
    /// this read serves, advances the binding, or 404s. A locked bucket refuses first.
    fn reference_settings_read(&mut self, value: Option<Value>) -> Turn {
        let settings = BucketEncryption::from_row(value.as_deref())?;
        self.reference_key = settings.active_key();
        self.reading = false;
        if let Some(key) = self.reference_key {
            let archive = reference_archive(key);
            let admit = Effect::Blob(BlobEffect::AdmitRead { key, archive });
            return self.effect(GetObjectState::CheckReferenceKey, admit);
        }
        let access = self.reference_access.clone();
        let access = access.ok_or(GetObjectError::GetObjectFailed)?;
        let head = Effect::StagingSource(StagingSourceEffect::Head { access });
        self.effect(GetObjectState::HeadReferenceSource, head)
    }

    fn reference_key_checked(&mut self, event: Event) -> Turn {
        let (Some(key), Some(access)) = (self.reference_key, self.reference_access.clone()) else {
            return Err(GetObjectError::GetObjectFailed);
        };
        match event {
            Event::Blob(BlobEvent::ReadAdmitted { lease })
                if lease.key == key && lease.archive == reference_archive(key) =>
            {
                self.reference_lease = Some(lease);
                let head = Effect::StagingSource(StagingSourceEffect::Head { access });
                self.effect(GetObjectState::HeadReferenceSource, head)
            }
            Event::Blob(BlobEvent::Error(BlobError::BucketKey(error))) => Err(locked(error)),
            Event::Blob(BlobEvent::Error(_)) => Err(GetObjectError::GetObjectFailed),
            other => Err(self.unexpected("Event::Blob(BlobEvent::ReadAdmitted)", other)),
        }
    }
    fn reference_head_received(&mut self, event: Event) -> Turn {
        match event {
            Event::StagingSource(StagingSourceEvent::HeadResult { metadata }) => {
                let stored = self.reference_cached.clone();
                // Only the first source's validators are recorded; others must match the bytes.
                let mirror = self.source_slot > 0;
                let comparison =
                    stored
                        .as_ref()
                        .map_or(ObservationMatch::Changed, |stored| match mirror {
                            true => stored.compare_mirror(&metadata),
                            false => stored.compare(&metadata),
                        });
                if let (ObservationMatch::Same, Some(stored)) = (comparison, stored) {
                    // Undrifted: serve the recorded observation, bound to its ETag. Without a
                    // source that enforces that binding, recorded digests are checked on the
                    // whole bytes, and the read response must still match the observation.
                    self.last_refresh = self.reference_last_refresh;
                    // Another source is read under its own validators and the version's digests;
                    // the version's own content type applies to the output only.
                    let served = match mirror {
                        false => SourceMetadata {
                            content_type: stored.content_type.clone().or(metadata.content_type),
                            ..stored
                        },
                        true => SourceMetadata {
                            digests: stored.digests,
                            ..metadata
                        },
                    };
                    let enforces = self.reference_access.as_ref().is_some_and(|access| {
                        let ResolvedSourceAccess::OpenDal { kind, .. } = access;
                        kind.enforces_if_match()
                    });
                    let bound = enforces && strong_validator(served.etag.as_deref());
                    if !bound {
                        self.verify_digests = served.enforced_checksums();
                    }
                    // A source that does not enforce its ETag may send other bytes than its HEAD
                    // promised, so another source serves only bytes the checksums prove.
                    if mirror && !bound && self.verify_digests.is_empty() {
                        let failure = self.source_failure.take();
                        return self.source_failed(
                            failure.unwrap_or(GetObjectError::ReferenceSourceChanged),
                        );
                    }
                    return self.serve_reference_source(served, bound);
                }
                // Recorded digests decide over a changed time or ETag of the same length.
                let digests_decide = !mirror
                    && self.reference_cached.as_ref().is_some_and(|stored| {
                        !stored.enforced_checksums().is_empty()
                            && stored.content_length == metadata.content_length
                    });
                if comparison == ObservationMatch::Unknown || digests_decide {
                    // Nothing proves the bytes unchanged. The whole bytes are checked against the
                    // recorded digests before any is served; without digests a pinned version is
                    // refused, and the current one is served fresh without counting as refreshed.
                    let digests = self
                        .reference_cached
                        .as_ref()
                        .map(|stored| stored.digests.clone())
                        .unwrap_or_default();
                    let served = SourceMetadata {
                        digests,
                        ..metadata
                    };
                    let enforced = served.enforced_checksums();
                    // Nothing proves that another source holds the version's bytes.
                    if mirror && enforced.is_empty() {
                        let failure = self.source_failure.take();
                        return self.source_failed(
                            failure.unwrap_or(GetObjectError::ReferenceSourceChanged),
                        );
                    }
                    if self.reference_explicit && enforced.is_empty() {
                        return self.proof_failed(GetObjectError::HistoricalReferenceUnavailable);
                    }
                    self.verify_digests = enforced;
                    return self.serve_reference_source(served, false);
                }
                // Another source that disagrees gives way to the remaining ones first.
                if mirror && self.others_remain() {
                    return self.source_failed(GetObjectError::ReferenceSourceChanged);
                }
                // A pinned historical version cannot serve drifted bytes:
                // its bytes were never cached (#375 deferred).
                if self.reference_explicit {
                    return self.proof_failed(GetObjectError::HistoricalReferenceUnavailable);
                }
                // Recorded digests bind the bytes, so a drifted source never replaces them.
                let bound = self.reference_cached.as_ref();
                if bound.is_some_and(|stored| !stored.enforced_checksums().is_empty()) {
                    return self.proof_failed(GetObjectError::ReferenceChecksumMismatch);
                }
                // Current drift records a same-binding successor, but repeated
                // change fails rather than serving mismatched bytes.
                if self.drift_attempts < MAX_DRIFT_ATTEMPTS {
                    // The successor records the source that changed first, with its observation.
                    if mirror {
                        let binding = self.source_binding.as_ref();
                        self.source_binding =
                            binding.and_then(|binding| binding.promoted(self.source_slot));
                        self.source_slot = 0;
                    }
                    return self.begin_reference_advance(metadata);
                }
                Err(GetObjectError::ReferenceSourceChanged)
            }
            Event::StagingSource(StagingSourceEvent::Error { error }) => {
                self.source_failed(error.into())
            }
            other => Err(self.unexpected(
                "Event::StagingSource(StagingSourceEvent::HeadResult)",
                other,
            )),
        }
    }

    /// Issues the source read for the observation `metadata`, resolving the requested range
    /// against its content length; a `bound` read stays on that observation's ETag.
    fn serve_reference_source(&mut self, metadata: SourceMetadata, bound: bool) -> Turn {
        let access = self.reference_access.clone();
        let access = access.ok_or(GetObjectError::GetObjectFailed)?;
        let range = match self.input.range.as_ref() {
            Some(range_request) => {
                let resolved = range_request.resolve(metadata.content_length)?;
                self.resolved_range = Some(resolved.clone());
                Some(resolved.range)
            }
            None => None,
        };
        let expected = bound.then(|| metadata.clone());
        // Verified bytes are read whole and spooled; the range is cut from the spool.
        let range = range.filter(|_| self.verify_digests.is_empty());
        self.source_metadata = Some(metadata);
        let read = Effect::StagingSource(StagingSourceEffect::Read {
            access,
            range,
            expected,
        });
        self.effect(GetObjectState::ReadReferenceSource, read)
    }

    /// Records a same-binding successor for a drifted current-version read, in its own
    /// write transaction.
    fn begin_reference_advance(&mut self, observation: SourceMetadata) -> Turn {
        // The successor is read from the source it records, never from another one.
        self.source_queue.clear();
        self.drift_attempts = self.drift_attempts.saturating_add(1);
        self.advance_observation = Some(observation);
        let advance = self.advance()?;
        self.call(GetObjectState::AdvanceReference, IoCall::child(advance))
    }

    /// The advance of the drifted reference this read headed.
    fn advance(&self) -> Result<AdvanceReference, GetObjectError> {
        let (Some(observation), Some(binding), Some(creator), Some(headed)) = (
            self.advance_observation.clone(),
            self.source_binding.clone(),
            self.reference_creator,
            self.resolved_version_id,
        ) else {
            return Err(GetObjectError::GetObjectFailed);
        };
        Ok(AdvanceReference {
            input: self.input.clone(),
            auth_context: self.auth_context(),
            headed,
            observation,
            binding,
            creator,
            metadata: self.metadata.clone(),
            now: self.now_override.unwrap_or_else(SystemTime::now),
            step: AdvanceStep::ReadBucket,
            pointer: None,
            released_bytes: 0,
            usage: None,
            advanced: None,
        })
    }

    fn reference_advanced(&mut self, result: IoResult) -> Turn {
        match result.child::<AdvanceEnd, GetObjectError>()? {
            Ok(AdvanceEnd::Committed {
                version_id,
                advance,
            }) => {
                self.resolved_version_id = Some(version_id);
                self.reference_advance = Some(advance);
                self.queue_successor_replication()
            }
            Ok(AdvanceEnd::Distributed) => self.queue_changed_refresh(),
            // Another writer moved the head: the read starts again against the winner.
            Ok(AdvanceEnd::Conflict)
            | Err(GetObjectError::StorageError(StorageError::TransactionConflict)) => {
                self.restart()
            }
            Err(GetObjectError::StorageError(_)) => Err(GetObjectError::GetObjectFailed),
            Err(error) => Err(error),
        }
    }

    /// Enqueues the committed successor for live replication, the same way the
    /// normal write path does after its commit.
    fn queue_successor_replication(&mut self) -> Turn {
        let version_id = self.resolved_version_id;
        let version_id = version_id.ok_or(GetObjectError::GetObjectFailed)?;
        let reference_advance = self.reference_advance;
        let reference_advance = reference_advance.ok_or(GetObjectError::GetObjectFailed)?;
        let live = LiveVersionOperation::new(LiveVersionInput {
            local_node_id: self.input.node_id,
            auth_context: self.auth_context(),
            bucket: self.input.bucket.clone(),
            key: self.input.key.clone(),
            version_id,
            delete_marker: false,
        })
        .with_reference_advance(reference_advance);
        let queue = boxed_suboperation(live, |result| {
            Event::SubOperation(SubOperationEvent::LiveReplicationQueued {
                result: result.map(|_| ()).map_err(|error| error.to_string()),
            })
        });
        self.call(
            GetObjectState::QueueSuccessorReplication,
            IoCall::Child(queue),
        )
    }

    fn handle_successor_queued(&mut self, event: Event) -> Turn {
        let Event::SubOperation(SubOperationEvent::LiveReplicationQueued { result }) = event else {
            return Err(self.unexpected(
                "Event::SubOperation(SubOperationEvent::LiveReplicationQueued)",
                event,
            ));
        };
        // The committed obligation is what repair replays, so a failed enqueue
        // must not fail the read.
        if let Err(error) = result {
            warn!(
                error = %error,
                "Reference successor obligation committed but live replication was not queued"
            );
        }
        // Kept until the read returns: it is the fingerprint the committed
        // successor promises, so the served bytes are checked against it.
        let observation = self.advance_observation.clone();
        let observation = observation.ok_or(GetObjectError::GetObjectFailed)?;
        self.last_refresh = Some(SystemTime::now());
        self.serve_reference_source(observation, true)
    }

    /// Reads the records again after a lost advance. The preserved advance counter makes
    /// repeated source drift terminate.
    fn restart(&mut self) -> Turn {
        self.reference_access = None;
        self.source_queue.clear();
        self.reference_cached = None;
        self.reference_last_refresh = None;
        self.verify_digests = Vec::new();
        self.reference_creator = None;
        self.reference_advance = None;
        self.source_metadata = None;
        self.advance_observation = None;
        self.resolved_version_id = None;
        self.reading = true;
        self.restarted = true;
        self.read_first()
    }

    /// Authoritative facts of the object this read serves. A local location
    /// supplies size and MD5; a reference source supplies its observed size and
    /// ETag. Neither is invented when absent.
    fn object_info(
        &self,
        location: Option<&BackendLocation>,
        source: Option<&SourceMetadata>,
    ) -> Option<ObjectInfo> {
        Some(ObjectInfo {
            size: location
                .map(|location| location.blob_size)
                .or_else(|| source.map(|metadata| metadata.content_length))?,
            version_created_at: self
                .version_created_at
                .or_else(|| source.and_then(|metadata| metadata.last_modified)),
            etag: location
                .and_then(|location| location.hashes.get(HASH_MD5))
                .map(hex::encode)
                .or_else(|| source.and_then(|metadata| metadata.etag.clone())),
            checksum_type: self.checksum_type,
            hashes: location
                .map(|location| location.hashes.clone())
                .unwrap_or_default(),
            composite_hashes: self.composite_hashes.clone(),
            part_count: self.part_count,
        })
    }

    fn handle_received_blob(&mut self, event: Event) -> Turn {
        let Event::Blob(BlobEvent::ReadFinished { mut blob, .. }) = event else {
            return Err(self.unexpected("Event::Blob(BlobEvent::ReadFinished)", event));
        };
        if let Some(lease) = self.reference_lease.take() {
            blob = leased(blob, lease);
        }
        let location = self.location.clone();
        let location = location.ok_or(GetObjectError::GetObjectFailed)?;
        let info = self.object_info(Some(&location), None);
        let info = info.ok_or(GetObjectError::GetObjectFailed)?;
        Ok(Next::Done(GetObjectResult {
            blob,
            location: Some(location),
            metadata: self.metadata.clone(),
            info,
            source_metadata: None,
            source_binding: self.source_binding.clone(),
            last_refresh: None,
            version_id: self.resolved_version_id.or(self.input.version_id),
            resolved_version_id: self.resolved_version_id,
            resolved_range: self.resolved_range.clone(),
            source_policies: self.source_policies.clone(),
            served_source: None,
        }))
    }

    fn reference_source_received(&mut self, event: Event) -> Turn {
        match event {
            Event::StagingSource(StagingSourceEvent::ReadResult { metadata, stream }) => {
                let head_metadata = self.source_metadata.as_ref();
                let head_metadata = head_metadata.ok_or(GetObjectError::GetObjectFailed)?;
                let comparison = head_metadata.compare(&metadata);
                if comparison == ObservationMatch::Changed {
                    if self.source_slot > 0 && self.others_remain() {
                        return self.source_failed(GetObjectError::ReferenceSourceChanged);
                    }
                    if self.reference_explicit {
                        return self.proof_failed(GetObjectError::HistoricalReferenceUnavailable);
                    }
                    return self.restart();
                }
                // Without digests, only the read's own validators prove a pinned version.
                if comparison == ObservationMatch::Unknown
                    && self.reference_explicit
                    && self.verify_digests.is_empty()
                {
                    return self.proof_failed(GetObjectError::HistoricalReferenceUnavailable);
                }
                self.advance_observation = None;
                // `last_refresh` was set by the head handler from the drift check.
                let digests = head_metadata.digests.clone();
                let size = metadata.content_length;
                self.source_metadata = Some(SourceMetadata {
                    digests,
                    ..metadata
                });
                if !self.verify_digests.is_empty() {
                    return self.spool_reference(stream, size);
                }
                self.finish_reference_output(stream)
            }
            Event::StagingSource(StagingSourceEvent::Error {
                error: StagingSourceError::ConditionFailed,
            }) if self.source_queue.is_empty() => self.queue_changed_refresh(),
            Event::StagingSource(StagingSourceEvent::Error { error }) => {
                self.source_failed(error.into())
            }
            other => Err(self.unexpected(
                "Event::StagingSource(StagingSourceEvent::ReadResult)",
                other,
            )),
        }
    }

    /// Queues a request to observe a source whose bound read failed, so the refresh worker
    /// records the change and retries the observation until it succeeds; the read fails.
    fn queue_changed_refresh(&mut self) -> Turn {
        let version_id = self.resolved_version_id.or(self.input.version_id);
        let version_id = version_id.ok_or(GetObjectError::ReferenceSourceChanged)?;
        let refresh = ReferenceRefresh {
            bucket: self.input.bucket.clone(),
            key: self.input.key.clone(),
            version_id,
            metadata: None,
            refreshed_at: self.now_override.unwrap_or_else(SystemTime::now),
            node_id: self.input.node_id,
            auth_context: self.auth_context(),
        };
        let queue = boxed_suboperation(QueueRefreshOperation::new(refresh), |result| {
            Event::SubOperation(SubOperationEvent::RefreshQueued {
                result: result.map(|_| ()).map_err(|error| error.to_string()),
            })
        });
        self.call(GetObjectState::QueueChangedRefresh, IoCall::Child(queue))
    }

    fn handle_refresh_queued(&mut self, event: Event) -> Turn {
        if let Event::SubOperation(SubOperationEvent::RefreshQueued { result: Err(error) }) = &event
        {
            warn!(error = %error, "Changed reference source refresh was not queued");
        }
        Err(GetObjectError::ReferenceSourceChanged)
    }

    /// Spools the whole source bytes to a local unnamed file, so nothing is served before they
    /// match the recorded digests; the served stream owns the file until it ends or drops.
    fn spool_reference(
        &mut self,
        stream: BackendStream<Result<Bytes, StreamError>>,
        size: u64,
    ) -> Turn {
        let range = (self.resolved_range.as_ref()).map(|resolved| resolved.range.clone());
        let spool = Effect::Blob(BlobEffect::SpoolLocal {
            size,
            deadline: None,
            range,
            blob: stream,
        });
        self.effect(GetObjectState::SpoolReference, spool)
    }

    fn handle_reference_spooled(&mut self, event: Event) -> Turn {
        let (hashes, blob) = match event {
            Event::Blob(BlobEvent::LocalSpooled { hashes, blob, .. }) => (hashes, blob),
            Event::Blob(BlobEvent::Error(BlobError::SpoolBudgetExhausted { .. })) => {
                return Err(GetObjectError::SpoolBudgetExhausted);
            }
            Event::Blob(BlobEvent::Error(BlobError::SpoolTooLarge { limit })) => {
                return Err(GetObjectError::ReferenceTooLarge { limit });
            }
            // A source that sent other bytes than it announced changed behind the version.
            Event::Blob(BlobEvent::Error(BlobError::IntegrityCheckFailed(_))) => {
                return self.proof_failed(GetObjectError::ReferenceSourceChanged);
            }
            // Nothing was served yet, so a broken transfer tries the other sources.
            Event::Blob(BlobEvent::Error(error)) => {
                debug!(error = %error, "Spooling a reference source failed");
                return self.source_failed(GetObjectError::GetObjectFailed);
            }
            _ => return Err(GetObjectError::GetObjectFailed),
        };
        let expected = std::mem::take(&mut self.verify_digests);
        // Dropping the stream of a spool that missed the digests removes the spool.
        if unmatched_checksum(&expected, &hashes).is_some() {
            return self.proof_failed(GetObjectError::ReferenceChecksumMismatch);
        }
        self.finish_reference_output(blob)
    }

    /// Whether further sources remain to try.
    fn others_remain(&self) -> bool {
        !self.source_queue.is_empty()
    }

    /// Bytes that fail their proof were not served, so any remaining source is tried; the
    /// failure stands once none is left.
    fn proof_failed(&mut self, error: GetObjectError) -> Turn {
        match self.others_remain() {
            true => self.source_failed(error),
            false => Err(error),
        }
    }

    fn finish_reference_output(
        &mut self,
        mut blob: BackendStream<Result<Bytes, StreamError>>,
    ) -> Turn {
        if let Some(lease) = self.reference_lease.take() {
            blob = leased(blob, lease);
        }
        let source_metadata = self.source_metadata.clone();
        let mut source_metadata = source_metadata.ok_or(GetObjectError::GetObjectFailed)?;
        // Bytes from another source answer with the version's own validators.
        if let Some(cached) = self
            .reference_cached
            .as_ref()
            .filter(|_| self.source_slot > 0)
        {
            source_metadata.etag = cached.etag.clone();
            source_metadata.last_modified = cached.last_modified;
            let content_type = cached.content_type.clone();
            source_metadata.content_type = content_type.or(source_metadata.content_type);
        }
        let info = self.object_info(None, Some(&source_metadata));
        let info = info.ok_or(GetObjectError::GetObjectFailed)?;
        Ok(Next::Done(GetObjectResult {
            blob,
            location: None,
            metadata: self.metadata.clone(),
            info,
            source_metadata: Some(source_metadata),
            source_binding: self.source_binding.clone(),
            last_refresh: self.last_refresh,
            version_id: self.resolved_version_id.or(self.input.version_id),
            resolved_version_id: self.resolved_version_id,
            resolved_range: self.resolved_range.clone(),
            source_policies: self.source_policies.clone(),
            served_source: (self.source_binding.as_ref())
                .and_then(|binding| binding.source_at(self.source_slot)),
        }))
    }
}

impl Operation for GetObjectOperation {
    type Output = GetObjectResult;
    type Error = GetObjectError;

    fn first(&mut self) -> Turn {
        self.read_first()
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match self.state {
            GetObjectState::GetVersion => {
                self.handle_received_version(outside_answer(result)?.value()?)
            }
            GetObjectState::CheckManagedCopy => {
                self.handle_managed_copy(outside_answer(result)?.values()?)
            }
            GetObjectState::GetBlobLocation => self.location_read(outside_answer(result)?.value()?),
            GetObjectState::GetCurrentVersion => {
                self.current_version_received(outside_answer(result)?.value()?)
            }
            GetObjectState::ReadKept => self.kept_read(keyed(&outside_answer(result)?.values()?)),
            GetObjectState::ReadSourceBucket => {
                self.source_bucket_read(outside_answer(result)?.value()?)
            }
            GetObjectState::ReadSourceKept => {
                self.source_kept_read(keyed(&outside_answer(result)?.values()?))
            }
            GetObjectState::ResolveReferenceAccess => {
                self.reference_access_resolved(result.event()?)
            }
            GetObjectState::ReadSourceWeights => {
                self.weights_read(outside_answer(result)?.values()?)
            }
            GetObjectState::RankSources => self.sources_ranked(result.event()?),
            GetObjectState::ReadMultipartSummary => {
                self.summary_read(outside_answer(result)?.value()?)
            }
            GetObjectState::HeadReferenceSource => self.reference_head_received(result.event()?),
            GetObjectState::AdvanceReference => self.reference_advanced(result),
            GetObjectState::QueueSuccessorReplication => {
                self.handle_successor_queued(result.event()?)
            }
            GetObjectState::GetBlob => self.handle_received_blob(result.event()?),
            GetObjectState::ReadReferenceSource => self.reference_source_received(result.event()?),
            GetObjectState::QueueChangedRefresh => self.handle_refresh_queued(result.event()?),
            GetObjectState::SpoolReference => self.handle_reference_spooled(result.event()?),
            GetObjectState::AdmitRead => self.read_admitted(result.event()?),
            GetObjectState::ReadContentSettings => {
                self.content_settings_read(outside_answer(result)?.value()?)
            }
            GetObjectState::AdmitPlain => self.plain_admitted(result.event()?),
            GetObjectState::ReadReferenceSettings => {
                self.reference_settings_read(outside_answer(result)?.value()?)
            }
            GetObjectState::CheckReferenceKey => self.reference_key_checked(result.event()?),
            GetObjectState::Init => Err(GetObjectError::InvalidState {
                current: GetObjectState::Init,
                expected: GetObjectState::GetVersion,
            }),
        }
    }

    /// The record reads see one snapshot of the bucket.
    fn transaction(&self) -> TransactionMode {
        TransactionMode::ReadSnapshot
    }
}

/// How a reference advance ended.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AdvanceEnd {
    /// The successor and its obligation committed.
    Committed {
        version_id: Ulid,
        advance: ReferenceAdvance,
    },
    /// A distributed bucket advances only through its coordinated refresh.
    Distributed,
    /// The head moved meanwhile; the read starts again against the winner.
    Conflict,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AdvanceStep {
    ReadBucket,
    ReadHead,
    ReadVersion,
    WriteSuccessor,
    UpdateUsage,
}

/// Writes the same-binding successor of a drifted reference and moves the head to it, in one
/// write transaction that rechecks the head and the durable advance bounds.
#[derive(Debug, PartialEq)]
struct AdvanceReference {
    input: GetObjectInput,
    auth_context: AuthContext,
    /// The version the drifted read headed; the head must still name it.
    headed: Ulid,
    observation: SourceMetadata,
    binding: VersionSourceBinding,
    creator: UserId,
    metadata: HashMap<String, String>,
    now: SystemTime,
    step: AdvanceStep,
    pointer: Option<CurrentVersionPointer>,
    /// Offered directory bytes the superseded head stops charging.
    released_bytes: u64,
    usage: Option<UsageCounterUpdate>,
    advanced: Option<(Ulid, ReferenceAdvance)>,
}

type AdvanceTurn = Result<Next<AdvanceEnd>, GetObjectError>;

impl AdvanceReference {
    fn read(&mut self, step: AdvanceStep, call: IoCall) -> AdvanceTurn {
        self.step = step;
        Ok(Next::Call(call))
    }

    /// Source changes of a distributed bucket become successors only through the coordinated
    /// refresh, so a read there queues one durably and fails instead of advancing the binding.
    fn bucket_read(&mut self, value: Option<Value>) -> AdvanceTurn {
        let bucket = value
            .map(|value| BucketInfo::from_bytes(&value))
            .transpose()?;
        if bucket.is_some_and(|bucket| bucket.distribution.is_some()) {
            return Ok(Next::Done(AdvanceEnd::Distributed));
        }
        let key = BlobHeadKey::new(&self.input.bucket, &self.input.key).to_bytes()?;
        self.read(AdvanceStep::ReadHead, read_row(BLOB_HEAD_KEYSPACE, key))
    }

    fn head_read(&mut self, value: Option<Value>) -> AdvanceTurn {
        let Some(value) = value else {
            return Ok(Next::Done(AdvanceEnd::Conflict));
        };
        let pointer = CurrentVersionPointer::from_bytes(value.as_ref())?;
        // CAS: only advance while the head still names the version we headed;
        // otherwise a concurrent writer won and we serve its successor instead.
        if pointer.version_id != self.headed {
            return Ok(Next::Done(AdvanceEnd::Conflict));
        }
        let key = VersionKey::new(&self.input.bucket, &self.input.key, pointer.version_id);
        self.pointer = Some(pointer);
        let read = read_row(BLOB_VERSIONS_KEYSPACE, key.to_bytes()?);
        self.read(AdvanceStep::ReadVersion, read)
    }

    /// Enforces the durable advance bounds against the reread current version and
    /// writes the successor in the same transaction.
    fn version_read(&mut self, value: Option<Value>) -> AdvanceTurn {
        let Some(value) = value else {
            return Ok(Next::Done(AdvanceEnd::Conflict));
        };
        let current = BlobVersion::from_bytes(value.as_ref())?;
        let Some(advance_count) = current.advance_count() else {
            return Ok(Next::Done(AdvanceEnd::Conflict));
        };
        self.released_bytes = current
            .offered_size(&self.input.bucket, &self.input.key)
            .unwrap_or(0);
        if advance_count >= MAX_AUTO_ADVANCES {
            return Err(GetObjectError::ReferenceAdvanceExhausted);
        }
        let now = self.now;
        // A backwards clock makes `duration_since` fail, which fails the advance
        // closed rather than granting an unbounded budget.
        if !now
            .duration_since(current.created_at)
            .is_ok_and(|elapsed| elapsed >= MIN_ADVANCE_INTERVAL)
        {
            return Err(GetObjectError::ReferenceSourceChanged);
        }
        let successor_count = advance_count.checked_add(1);
        let successor_count = successor_count.ok_or(GetObjectError::ReferenceAdvanceExhausted)?;
        let pointer = self
            .pointer
            .clone()
            .ok_or(GetObjectError::GetObjectFailed)?;

        let new_version_id = Ulid::generate();
        // The successor of a governed reference keeps its predecessor's refs:
        // an automatic advance must never relax an attachment.
        let successor = BlobVersion::reference(
            self.binding.clone(),
            self.observation.clone(),
            now,
            self.creator,
            now,
        )
        .with_metadata(self.metadata.clone())
        .with_advance_count(successor_count)
        .with_policies(current.placement_policies.clone())?;
        let successor_key = VersionKey::new(&self.input.bucket, &self.input.key, new_version_id);
        let next_pointer = CurrentVersionPointer::next_for(Some(&pointer), new_version_id)?;
        let reference_advance = ReferenceAdvance {
            generation: next_pointer.generation,
            predecessor: pointer.version_id,
        };
        // The durable obligation rides the same transaction as the successor, so
        // a lost enqueue is still discoverable by the repair scanner.
        let obligation = LiveObligationRecord::new(
            self.input.node_id,
            self.auth_context.clone(),
            self.input.bucket.clone(),
            self.input.key.clone(),
            new_version_id,
            false,
        )
        .with_reference_advance(reference_advance);
        let head_key = BlobHeadKey::new(&self.input.bucket, &self.input.key).to_bytes()?;
        let mut writes = vec![
            (
                BLOB_VERSIONS_KEYSPACE.to_string(),
                successor_key.to_bytes()?.into(),
                successor.to_bytes()?.into(),
            ),
            (
                BLOB_HEAD_KEYSPACE.to_string(),
                head_key.into(),
                next_pointer.to_bytes()?.into(),
            ),
            live_obligation_entry(&obligation)?,
        ];
        writes.extend(connector_reference_write(&successor, &successor_key)?);
        self.advanced = Some((new_version_id, reference_advance));
        self.read(AdvanceStep::WriteSuccessor, IoCall::Write(writes))
    }

    /// The successor adds its own size. A superseded reference stays stored and charged,
    /// except an offered directory observation, which is released.
    fn successor_written(&mut self) -> AdvanceTurn {
        let referenced_bytes =
            i128::from(self.observation.content_length) - i128::from(self.released_bytes);
        let delta = UsageDelta {
            referenced_bytes,
            ..Default::default()
        };
        let mut update = UsageCounterUpdate::for_group(self.input.group_id, delta)
            .in_bucket(&self.input.bucket, RowChange::AddedVersion);
        if update.is_noop() {
            return self.committed();
        }
        let call = update.start_calls().into_call();
        self.usage = Some(update);
        self.read(AdvanceStep::UpdateUsage, call)
    }

    fn usage_updated(&mut self, result: IoResult) -> AdvanceTurn {
        let update = self.usage.as_mut().ok_or(GetObjectError::GetObjectFailed)?;
        match update.step_calls(result)? {
            Some(call) => Ok(Next::Call(call.into_call())),
            None => self.committed(),
        }
    }

    fn committed(&mut self) -> AdvanceTurn {
        let advanced = self.advanced.take();
        let (version_id, advance) = advanced.ok_or(GetObjectError::GetObjectFailed)?;
        Ok(Next::Done(AdvanceEnd::Committed {
            version_id,
            advance,
        }))
    }
}

impl Operation for AdvanceReference {
    type Output = AdvanceEnd;
    type Error = GetObjectError;

    fn first(&mut self) -> AdvanceTurn {
        let read = read_row(S3_BUCKET_KEYSPACE, self.input.bucket.as_bytes().to_vec());
        self.read(AdvanceStep::ReadBucket, read)
    }

    fn next(&mut self, result: IoResult) -> AdvanceTurn {
        match self.step {
            AdvanceStep::ReadBucket => self.bucket_read(result.value()?),
            AdvanceStep::ReadHead => self.head_read(result.value()?),
            AdvanceStep::ReadVersion => self.version_read(result.value()?),
            AdvanceStep::WriteSuccessor => {
                result.done()?;
                self.successor_written()
            }
            AdvanceStep::UpdateUsage => self.usage_updated(result),
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

/// Keeps `lease` with the stream until the stream ends.
pub(crate) fn leased(
    blob: BackendStream<Result<Bytes, StreamError>>,
    lease: ReadLease,
) -> BackendStream<Result<Bytes, StreamError>> {
    BackendStream(Box::pin(blob.0.map(move |chunk| {
        let _lease = &lease;
        chunk
    })))
}

/// A reference version has no archive, so its lease names the bucket id, which no archive uses.
pub(crate) fn reference_archive(key: BucketKeyRef) -> ArchiveKey {
    ArchiveKey::new(key.bucket_id, BackendRef::node_default())
}

pub(super) fn locked(error: BucketKeyError) -> GetObjectError {
    GetObjectError::ConversionError(ConversionError::BucketKey(error))
}

#[cfg(test)]
#[path = "get_tests.rs"]
mod test;

/// Whether an ETag can bind a read through `If-Match`: present and not weak.
pub(crate) fn strong_validator(etag: Option<&str>) -> bool {
    etag.map(str::trim)
        .is_some_and(|etag| !etag.is_empty() && !etag.starts_with("W/"))
}
