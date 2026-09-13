use crate::blob::holders::GetBlobHoldersOperation;
use crate::blob::managed_copy::ManagedCopyError;
use crate::blob::records::blob_location_read;
use crate::connectors::{ResolveVersionSourceBindingInput, resolve_binding_effect};
use crate::driver::{DriverContext, drive};
use crate::node::usage_stats::{UsageCounterUpdate, UsageUpdateError};
use crate::replication::bao_read::{BaoReadError, BaoReadOutput, local_is_user, managed_read};
use crate::replication::protocol::{
    BaoReadRefusal, BaoReadRequest, BaoReadTarget, ReferenceAdvance,
};
use crate::replication::queue::{
    LiveReplicationObligationRecord, QueueLiveVersionReplicationInput,
    QueueLiveVersionReplicationOperation, live_obligation_entry,
};
use crate::s3::object_lookup::{
    ExpectedNode, LookupError, begin_copy_check, finish_copy_check, location_from_read,
    multipart_summary_read, summary_from_read,
};
use aruna_core::effects::{BlobEffect, Effect, StagingSourceEffect, StorageEffect};
use aruna_core::errors::{
    ConversionError, SourceConnectorResolutionError, StagingSourceError, StorageError,
};
use aruna_core::events::{BlobEvent, Event, StagingSourceEvent, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, S3_MULTIPART_OBJECT_METADATA_KEYSPACE,
};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::checksum::HASH_MD5;
use aruna_core::structs::{
    AuthContext, BackendLocation, BackendRef, BlobHeadKey, BlobLocationKey, BlobVersion,
    BlobVersionState, CurrentVersionPointer, ManagedCopyKey, MultipartChecksumType,
    MultipartObjectMetadataKey, MultipartObjectSummary, PathRestriction, PlacementPolicyError,
    PlacementPolicyRef, ResolvedSourceAccess, SourceMetadata, UsageDelta, VersionKey,
    VersionSourceBinding,
};
use aruna_core::types::Effects;
use aruna_core::{NodeId, UserId};
use bytes::Bytes;
use smallvec::{SmallVec, smallvec};
use std::collections::HashMap;
use std::ops::Range;
use std::time::{Duration, SystemTime};
use thiserror::Error;
use tracing::warn;

/// Maximum successors attempted before a still-changing current read fails.
const MAX_DRIFT_ADVANCE_ATTEMPTS: u8 = 3;
/// Minimum age of the current reference version before a read may mint another
/// successor. Rate-limits a READ-only caller pointing at a source they control.
pub const MIN_ADVANCE_INTERVAL: Duration = Duration::from_secs(60);
/// Hard bound on automatic successors per explicit binding: the interval only
/// slows growth, this stops it. Only a WRITE or rebind starts a fresh count.
pub const MAX_AUTO_ADVANCES: u16 = 100;
use ulid::Ulid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GetObjectState {
    Init,
    StartTransaction,
    GetVersion,
    CheckManagedCopy,
    GetBlobLocation,
    GetCurrentVersion,
    ResolveReferenceAccess,
    ReadMultipartSummary,
    CommitTransaction,
    HeadReferenceSource,
    StartAdvanceTransaction,
    ReadHeadForAdvance,
    ReadCurrentForAdvance,
    WriteSuccessor,
    UpdateReferenceUsage,
    CommitAdvance,
    QueueSuccessorReplication,
    RestartReference,
    GetBlob,
    ReadReferenceSource,
    Finish,
    Error,
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
    #[error("State [{state:?}] invalid: expected [{expected:?}] - received [{received:?}]")]
    InvalidStateEvent {
        state: GetObjectState,
        expected: &'static str,
        received: Event,
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
    #[error("The historical reference version is no longer available.")]
    HistoricalReferenceUnavailable,
    #[error(
        "The reference binding reached its automatic advance limit; rebind it with an explicit write."
    )]
    ReferenceAdvanceExhausted,
    #[error(transparent)]
    UsageError(#[from] UsageUpdateError),
    #[error(transparent)]
    ResolveReferenceError(#[from] SourceConnectorResolutionError),
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
    #[error("GetObject failed (miserably)")]
    GetObjectFailed,
    #[error("operation did not finish")]
    NotFinished,
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

#[derive(Debug, PartialEq)]
pub struct GetObjectInput {
    pub bucket: String,
    pub key: String,
    pub version_id: Option<Ulid>,
    pub range: Option<ObjectRangeRequest>,
    pub group_id: Ulid,
    pub user_identity: UserId,
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
}

#[derive(Debug, PartialEq)]
pub struct GetObjectOperation {
    input: GetObjectInput,
    state: GetObjectState,
    txn_id: Option<Ulid>,
    location: Option<BackendLocation>,
    reference_access: Option<ResolvedSourceAccess>,
    reference_stream: Option<BackendStream<Result<Bytes, StreamError>>>,
    /// Stored observation of the reference version being read, the drift baseline.
    reference_cached: Option<SourceMetadata>,
    /// `last_refresh` of the stored reference version; served when undrifted.
    reference_last_refresh: Option<SystemTime>,
    /// Whether the caller pinned an explicit version (a historical read).
    reference_explicit: bool,
    /// Fresh observation to record in the successor and then serve.
    advance_observation: Option<SourceMetadata>,
    /// Embedded `referenced_bytes` counter update for the successor.
    usage_update: Option<UsageCounterUpdate>,
    /// Advance attempts so a still-drifting source cannot spin forever.
    drift_attempts: u8,
    /// Reader's scoped credential, carried onto the successor's obligation.
    restrictions: Option<Vec<PathRestriction>>,
    /// Original creator retained as successor attribution.
    reference_creator: Option<UserId>,
    /// Publisher-attested predecessor lineage for the successor.
    reference_advance: Option<ReferenceAdvance>,
    /// Head pointer revalidated inside the advance transaction.
    advance_pointer: Option<CurrentVersionPointer>,
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
    output: Option<Result<GetObjectResult, GetObjectError>>,
}

impl GetObjectOperation {
    pub fn new(input: GetObjectInput) -> Self {
        GetObjectOperation {
            input,
            state: GetObjectState::Init,
            txn_id: None,
            location: None,
            reference_access: None,
            reference_stream: None,
            reference_cached: None,
            reference_last_refresh: None,
            reference_explicit: false,
            advance_observation: None,
            usage_update: None,
            drift_attempts: 0,
            restrictions: None,
            reference_creator: None,
            reference_advance: None,
            advance_pointer: None,
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
            output: None,
        }
    }

    pub fn with_restrictions(mut self, restrictions: Option<Vec<PathRestriction>>) -> Self {
        self.restrictions = restrictions;
        self
    }

    fn auth_context(&self) -> AuthContext {
        AuthContext {
            user_id: self.input.user_identity,
            realm_id: self.input.user_identity.realm_id,
            path_restrictions: self.restrictions.clone(),
            session: None,
        }
    }

    pub fn emit_error(&mut self, error: GetObjectError) -> Effects {
        self.state = GetObjectState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn lookup_error(&self, expected: &'static str, error: LookupError) -> GetObjectError {
        match error {
            LookupError::Conversion(err) => GetObjectError::ConversionError(err),
            LookupError::Managed(err) => GetObjectError::ManagedCopyError(err),
            LookupError::InvalidEvent(received) => GetObjectError::InvalidStateEvent {
                state: self.state.clone(),
                expected,
                received,
            },
            LookupError::Missing => GetObjectError::GetObjectFailed,
        }
    }

    /// Fails the read and releases the advance transaction it holds, so a policy
    /// rejection never leaves a write transaction open.
    fn abort_with_error(&mut self, error: GetObjectError) -> Effects {
        let effects = self.txn_id.take().map_or_else(SmallVec::new, |txn_id| {
            smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
        });
        self.state = GetObjectState::Error;
        self.output = Some(Err(error));
        effects
    }

    pub fn handle_init(&mut self) -> Effects {
        if self.state != GetObjectState::Init {
            self.emit_error(GetObjectError::InvalidState {
                current: self.state.clone(),
                expected: GetObjectState::Init,
            })
        } else {
            self.state = GetObjectState::StartTransaction;
            smallvec![Effect::Storage(StorageEffect::StartTransaction {
                read: true
            })]
        }
    }

    pub fn handle_transaction_started(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event {
            self.txn_id = Some(txn_id);
            if let Some(version_id) = self.input.version_id {
                self.state = GetObjectState::GetVersion;
                let key = match VersionKey::new(&self.input.bucket, &self.input.key, version_id)
                    .to_bytes()
                {
                    Ok(key) => key.into(),
                    Err(err) => return self.emit_error(GetObjectError::ConversionError(err)),
                };
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                    key,
                    txn_id: self.txn_id,
                })]
            } else {
                self.state = GetObjectState::GetCurrentVersion;

                let key = match BlobHeadKey::new(&self.input.bucket, &self.input.key).to_bytes() {
                    Ok(key) => key.into(),
                    Err(err) => return self.emit_error(GetObjectError::ConversionError(err)),
                };
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: BLOB_HEAD_KEYSPACE.to_string(),
                    key,
                    txn_id: self.txn_id,
                })]
            }
        } else {
            self.emit_error(GetObjectError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionStarted)",
                received: event,
            })
        }
    }

    pub fn handle_received_version(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(GetObjectError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received: event,
            });
        };

        let Some(val) = value else {
            return self.emit_error(if self.input.version_id.is_some() {
                GetObjectError::NoSuchVersion
            } else {
                GetObjectError::NoSuchKey
            });
        };

        let version = match BlobVersion::from_bytes(val.as_ref()) {
            Ok(version) => version,
            Err(err) => return self.emit_error(GetObjectError::ConversionError(err)),
        };

        let Some(version_id) = self.resolved_version_id.or(self.input.version_id) else {
            return self.emit_error(GetObjectError::GetObjectFailed);
        };

        self.read_version(version_id, version, self.input.version_id.is_some())
    }

    fn current_version_received(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(GetObjectError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received: event,
            });
        };

        let Some(val) = value else {
            return self.emit_error(GetObjectError::NoSuchKey);
        };

        let pointer = match CurrentVersionPointer::from_bytes(val.as_ref()) {
            Ok(pointer) => pointer,
            Err(err) => return self.emit_error(GetObjectError::ConversionError(err)),
        };

        let key = match VersionKey::new(&self.input.bucket, &self.input.key, pointer.version_id)
            .to_bytes()
        {
            Ok(key) => key.into(),
            Err(err) => return self.emit_error(GetObjectError::ConversionError(err)),
        };

        self.resolved_version_id = Some(pointer.version_id);
        self.state = GetObjectState::GetVersion;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key,
            txn_id: self.txn_id,
        })]
    }

    fn read_version(
        &mut self,
        version_id: Ulid,
        version: BlobVersion,
        explicit_version_request: bool,
    ) -> Effects {
        self.resolved_version_id = Some(version_id);
        self.metadata = version.metadata.clone();
        self.source_policies = version.placement_policies.clone();

        match version.state {
            BlobVersionState::Materialized {
                blob_hash,
                backend,
                source,
            } => {
                self.source_binding = source;
                self.version_created_at = Some(version.created_at);
                if version.placement_policies.is_empty() {
                    return self.read_blob_location(BlobLocationKey::new(blob_hash, backend));
                }
                self.check_managed_copy(version_id, blob_hash, backend)
            }
            BlobVersionState::Deleted => self.emit_error(if explicit_version_request {
                GetObjectError::DeleteMarker
            } else {
                GetObjectError::NoSuchKey
            }),
            BlobVersionState::Reference {
                source,
                cached_metadata,
                last_refresh,
                ..
            } => {
                // Access-driven successor-on-drift (#256); verified cache,
                // origin relay and sync polling stay deferred (#375/#380/#314).
                self.source_binding = Some(source.clone());
                self.reference_cached = Some(cached_metadata);
                self.reference_last_refresh = Some(last_refresh);
                self.reference_explicit = explicit_version_request;
                self.reference_creator = Some(version.created_by);
                self.location = None;
                self.reference_access = None;
                self.reference_stream = None;
                self.source_metadata = None;
                self.last_refresh = None;
                self.version_created_at = None;
                self.state = GetObjectState::ResolveReferenceAccess;
                smallvec![resolve_binding_effect(ResolveVersionSourceBindingInput {
                    source
                },)]
            }
        }
    }

    fn read_blob_location(&mut self, key: BlobLocationKey) -> Effects {
        self.missing_blake3 = Some(key.blake3_hash);
        self.state = GetObjectState::GetBlobLocation;
        smallvec![blob_location_read(&key, self.txn_id)]
    }

    /// A governed version is only serveable from a registered local copy, so an
    /// unregistered or quarantined copy fails closed before any byte moves.
    fn check_managed_copy(
        &mut self,
        version_id: Ulid,
        blob_hash: [u8; 32],
        backend: BackendRef,
    ) -> Effects {
        let check = match begin_copy_check(
            &self.input.bucket,
            &self.input.key,
            version_id,
            blob_hash,
            backend,
            self.txn_id,
        ) {
            Ok(check) => check,
            Err(err) => return self.emit_error(err.into()),
        };
        self.pending_copy = Some(check.copy_key);
        self.pending_location = Some(check.location_key);
        self.state = GetObjectState::CheckManagedCopy;
        smallvec![check.effect]
    }

    fn handle_managed_copy(&mut self, event: Event) -> Effects {
        let key = match finish_copy_check(
            event,
            &mut self.pending_copy,
            &mut self.pending_location,
            &self.source_policies,
            ExpectedNode::Exact(self.input.node_id),
        ) {
            Ok(key) => key,
            Err(err) => {
                let error = self.lookup_error("Event::Storage(StorageEvent::BatchReadResult)", err);
                return self.emit_error(error);
            }
        };
        self.read_blob_location(key)
    }

    fn location_read(&mut self, event: Event) -> Effects {
        let location = match location_from_read(event) {
            Ok(Some(location)) => location,
            Ok(None) => {
                return match self.missing_blake3 {
                    Some(blake3) => self.emit_error(GetObjectError::BlobNotLocal {
                        blake3,
                        version_id: self.resolved_version_id,
                        metadata: self.metadata.clone(),
                        version_created_at: self.version_created_at,
                        source_policies: self.source_policies.clone(),
                    }),
                    None => self.emit_error(GetObjectError::GetObjectFailed),
                };
            }
            Err(err) => {
                let error = self.lookup_error("Event::Storage(StorageEvent::ReadResult)", err);
                return self.emit_error(error);
            }
        };

        self.read_multipart_summary(location, self.resolved_version_id)
    }

    fn reference_access_resolved(&mut self, event: Event) -> Effects {
        match event {
            Event::SubOperation(SubOperationEvent::VersionSourceAccessResolved {
                result: Ok(access),
            }) => {
                self.reference_access = Some(access);
                self.read_reference()
            }
            Event::SubOperation(SubOperationEvent::VersionSourceAccessResolved {
                result: Err(error),
            }) => self.emit_error(error.into()),
            other => self.emit_error(GetObjectError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::SubOperation(SubOperationEvent::VersionSourceAccessResolved)",
                received: other,
            }),
        }
    }

    fn read_multipart_summary(
        &mut self,
        location: BackendLocation,
        resolved_version_id: Option<Ulid>,
    ) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(GetObjectError::NoTransactionFound);
        };

        self.location = Some(location);
        self.resolved_version_id = resolved_version_id;

        let Some(version_id) = resolved_version_id else {
            return self.read_blob();
        };

        let effect = match multipart_summary_read(version_id, Some(txn_id)) {
            Ok(effect) => effect,
            Err(err) => return self.emit_error(GetObjectError::ConversionError(err)),
        };

        self.state = GetObjectState::ReadMultipartSummary;
        smallvec![effect]
    }

    pub fn summary_read(&mut self, event: Event) -> Effects {
        let summary = match summary_from_read(event) {
            Ok(summary) => summary,
            Err(err) => {
                let error = self.lookup_error("Event::Storage(StorageEvent::ReadResult)", err);
                return self.emit_error(error);
            }
        };

        if let Some(summary) = summary {
            self.checksum_type = summary.checksum_type;
            self.composite_hashes = summary.composite_hashes;
            self.part_count = Some(summary.part_count);
        }

        self.read_blob()
    }

    fn read_blob(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(GetObjectError::NoTransactionFound);
        };
        let Some(location) = self.location.clone() else {
            return self.emit_error(GetObjectError::GetObjectFailed);
        };

        let resolved_range = match self.input.range.as_ref() {
            Some(range) => match range.resolve(location.blob_size) {
                Ok(range) => Some(range),
                Err(err) => return self.emit_error(err),
            },
            None => None,
        };
        self.resolved_range = resolved_range.clone();

        let read_effect = match resolved_range {
            Some(range) => BlobEffect::ReadRange {
                location,
                range: range.range,
            },
            None => BlobEffect::Read { location },
        };

        self.state = GetObjectState::CommitTransaction;
        smallvec![
            Effect::Storage(StorageEffect::CommitTransaction { txn_id }),
            Effect::Blob(read_effect)
        ]
    }

    fn read_reference(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(GetObjectError::NoTransactionFound);
        };
        if self.reference_access.is_none() {
            return self.emit_error(GetObjectError::GetObjectFailed);
        }

        // Release the read snapshot, then HEAD the source: the fresh observation
        // decides whether this read serves, advances the binding, or 404s.
        self.state = GetObjectState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    pub fn handle_transaction_committed(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event {
            self.txn_id = None;
            if let Some(access) = self.reference_access.clone() {
                self.state = GetObjectState::HeadReferenceSource;
                return smallvec![Effect::StagingSource(StagingSourceEffect::Head { access })];
            }
            self.state = GetObjectState::GetBlob;
            smallvec![]
        } else {
            self.emit_error(GetObjectError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionCommitted)",
                received: event,
            })
        }
    }

    pub fn reference_head_received(&mut self, event: Event) -> Effects {
        match event {
            Event::StagingSource(StagingSourceEvent::HeadResult { metadata }) => {
                let baseline = self
                    .reference_cached
                    .as_ref()
                    .map(SourceMetadata::observation_fingerprint);
                let drifted = baseline != Some(metadata.observation_fingerprint());

                if drifted {
                    // A pinned historical version cannot serve drifted bytes:
                    // its bytes were never cached (#375 deferred).
                    if self.reference_explicit {
                        return self.emit_error(GetObjectError::HistoricalReferenceUnavailable);
                    }
                    // Current drift records a same-binding successor, but repeated
                    // change fails rather than serving mismatched bytes.
                    if self.drift_attempts < MAX_DRIFT_ADVANCE_ATTEMPTS {
                        return self.begin_reference_advance(metadata);
                    }
                    return self.emit_error(GetObjectError::ReferenceSourceChanged);
                }

                // Undrifted: serve the recorded observation unchanged.
                self.last_refresh = self.reference_last_refresh;
                self.serve_reference_source(metadata)
            }
            Event::StagingSource(StagingSourceEvent::Error { error }) => {
                self.emit_error(error.into())
            }
            other => self.emit_error(GetObjectError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::StagingSource(StagingSourceEvent::HeadResult)",
                received: other,
            }),
        }
    }

    /// Issues the source read for the observation `metadata`, resolving the
    /// requested range against its live content length.
    fn serve_reference_source(&mut self, metadata: SourceMetadata) -> Effects {
        let Some(access) = self.reference_access.clone() else {
            return self.emit_error(GetObjectError::GetObjectFailed);
        };
        let range = match self.input.range.as_ref() {
            Some(range_request) => match range_request.resolve(metadata.content_length) {
                Ok(resolved) => {
                    self.resolved_range = Some(resolved.clone());
                    Some(resolved.range)
                }
                Err(err) => return self.emit_error(err),
            },
            None => None,
        };
        self.source_metadata = Some(metadata);
        self.state = GetObjectState::ReadReferenceSource;
        smallvec![Effect::StagingSource(StagingSourceEffect::Read {
            access,
            range
        })]
    }

    /// Opens the write transaction that records a same-binding successor for a
    /// drifted current-version read.
    fn begin_reference_advance(&mut self, observation: SourceMetadata) -> Effects {
        self.drift_attempts = self.drift_attempts.saturating_add(1);
        self.advance_observation = Some(observation);
        self.state = GetObjectState::StartAdvanceTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn handle_advance_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.emit_error(GetObjectError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionStarted)",
                received: event,
            });
        };
        self.txn_id = Some(txn_id);
        let key = match BlobHeadKey::new(&self.input.bucket, &self.input.key).to_bytes() {
            Ok(key) => key.into(),
            Err(err) => return self.emit_error(GetObjectError::ConversionError(err)),
        };
        self.state = GetObjectState::ReadHeadForAdvance;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BLOB_HEAD_KEYSPACE.to_string(),
            key,
            txn_id: self.txn_id,
        })]
    }

    fn handle_advance_head(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(GetObjectError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received: event,
            });
        };
        let Some(value) = value else {
            return self.restart_after_conflict();
        };
        let pointer = match CurrentVersionPointer::from_bytes(value.as_ref()) {
            Ok(pointer) => pointer,
            Err(err) => return self.emit_error(GetObjectError::ConversionError(err)),
        };
        // CAS: only advance while the head still names the version we headed;
        // otherwise a concurrent writer won and we serve its successor instead.
        if Some(pointer.version_id) != self.resolved_version_id {
            return self.restart_after_conflict();
        }
        let key = match VersionKey::new(&self.input.bucket, &self.input.key, pointer.version_id)
            .to_bytes()
        {
            Ok(key) => key.into(),
            Err(err) => return self.emit_error(GetObjectError::ConversionError(err)),
        };
        self.advance_pointer = Some(pointer);
        self.state = GetObjectState::ReadCurrentForAdvance;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key,
            txn_id: self.txn_id,
        })]
    }

    /// Enforces the durable advance bounds against the reread current version and
    /// writes the successor in the same transaction.
    fn handle_advance_version(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(GetObjectError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received: event,
            });
        };
        let Some(value) = value else {
            return self.restart_after_conflict();
        };
        let current = match BlobVersion::from_bytes(value.as_ref()) {
            Ok(current) => current,
            Err(err) => return self.emit_error(GetObjectError::ConversionError(err)),
        };
        let Some(advance_count) = current.advance_count() else {
            return self.restart_after_conflict();
        };
        if advance_count >= MAX_AUTO_ADVANCES {
            return self.abort_with_error(GetObjectError::ReferenceAdvanceExhausted);
        }
        let now = self.now_override.unwrap_or_else(SystemTime::now);
        // A backwards clock makes `duration_since` fail, which fails the advance
        // closed rather than granting an unbounded budget.
        if !now
            .duration_since(current.created_at)
            .is_ok_and(|elapsed| elapsed >= MIN_ADVANCE_INTERVAL)
        {
            return self.abort_with_error(GetObjectError::ReferenceSourceChanged);
        }
        let Some(successor_count) = advance_count.checked_add(1) else {
            return self.abort_with_error(GetObjectError::ReferenceAdvanceExhausted);
        };

        let (Some(observation), Some(source_binding), Some(creator), Some(txn_id), Some(pointer)) = (
            self.advance_observation.clone(),
            self.source_binding.clone(),
            self.reference_creator,
            self.txn_id,
            self.advance_pointer.clone(),
        ) else {
            return self.emit_error(GetObjectError::GetObjectFailed);
        };

        let new_version_id = Ulid::generate();
        // The successor of a governed reference keeps its predecessor's refs:
        // an automatic advance must never relax an attachment.
        let successor = match BlobVersion::reference(source_binding, observation, now, creator, now)
            .with_metadata(self.metadata.clone())
            .with_advance_count(successor_count)
            .with_policies(current.placement_policies.clone())
        {
            Ok(successor) => successor,
            Err(err) => return self.emit_error(err.into()),
        };
        let version_key =
            match VersionKey::new(&self.input.bucket, &self.input.key, new_version_id).to_bytes() {
                Ok(key) => key.into(),
                Err(err) => return self.emit_error(GetObjectError::ConversionError(err)),
            };
        let version_value = match successor.to_bytes() {
            Ok(value) => value.into(),
            Err(err) => return self.emit_error(GetObjectError::ConversionError(err)),
        };
        let head_key = match BlobHeadKey::new(&self.input.bucket, &self.input.key).to_bytes() {
            Ok(key) => key.into(),
            Err(err) => return self.emit_error(GetObjectError::ConversionError(err)),
        };
        let next_pointer = match CurrentVersionPointer::next_for(Some(&pointer), new_version_id) {
            Ok(next_pointer) => next_pointer,
            Err(err) => return self.emit_error(GetObjectError::ConversionError(err)),
        };
        let head_value = match next_pointer.to_bytes() {
            Ok(value) => value.into(),
            Err(err) => return self.emit_error(GetObjectError::ConversionError(err)),
        };
        let reference_advance = ReferenceAdvance {
            generation: next_pointer.generation,
            predecessor: pointer.version_id,
        };

        // The durable obligation rides the same transaction as the successor, so
        // a lost enqueue is still discoverable by the repair scanner.
        let obligation = LiveReplicationObligationRecord::new(
            self.input.node_id,
            self.auth_context(),
            self.input.bucket.clone(),
            self.input.key.clone(),
            new_version_id,
            false,
        )
        .with_reference_advance(reference_advance);
        let obligation_entry = match live_obligation_entry(&obligation) {
            Ok(entry) => entry,
            Err(err) => return self.emit_error(GetObjectError::ConversionError(err)),
        };

        self.resolved_version_id = Some(new_version_id);
        self.reference_advance = Some(reference_advance);
        self.state = GetObjectState::WriteSuccessor;
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes: vec![
                (
                    BLOB_VERSIONS_KEYSPACE.to_string(),
                    version_key,
                    version_value
                ),
                (BLOB_HEAD_KEYSPACE.to_string(), head_key, head_value),
                obligation_entry,
            ],
            txn_id: Some(txn_id),
        })]
    }

    fn handle_successor_written(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchWriteResult { .. }) = event else {
            return self.emit_error(GetObjectError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::BatchWriteResult)",
                received: event,
            });
        };
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(GetObjectError::NoTransactionFound);
        };
        let Some(observation) = self.advance_observation.as_ref() else {
            return self.emit_error(GetObjectError::GetObjectFailed);
        };
        // Every stored reference version is charged its own full size, and the
        // superseded version stays stored, so the successor adds its own size.
        let referenced_bytes = i128::from(observation.content_length);
        let mut update = UsageCounterUpdate::for_group(
            self.input.group_id,
            UsageDelta {
                referenced_bytes,
                ..Default::default()
            },
        );
        if update.is_noop() {
            self.state = GetObjectState::CommitAdvance;
            return smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })];
        }
        self.state = GetObjectState::UpdateReferenceUsage;
        let effects = update.start(txn_id);
        self.usage_update = Some(update);
        effects
    }

    fn handle_advance_usage(&mut self, event: Event) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(GetObjectError::NoTransactionFound);
        };
        let Some(update) = self.usage_update.as_mut() else {
            return self.emit_error(GetObjectError::GetObjectFailed);
        };
        match update.step(event, txn_id) {
            Ok(Some(effects)) => effects,
            Ok(None) => {
                self.state = GetObjectState::CommitAdvance;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            Err(err) => self.emit_error(err.into()),
        }
    }

    fn handle_advance_committed(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                self.txn_id = None;
                self.queue_successor_replication()
            }
            Event::Storage(StorageEvent::Error {
                error: StorageError::TransactionConflict,
            }) => {
                self.txn_id = None;
                self.restart_after_conflict()
            }
            Event::Storage(StorageEvent::Error { .. }) => {
                self.emit_error(GetObjectError::GetObjectFailed)
            }
            other => self.emit_error(GetObjectError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionCommitted)",
                received: other,
            }),
        }
    }

    /// Enqueues the committed successor for live replication, the same way the
    /// normal write path does after its commit.
    fn queue_successor_replication(&mut self) -> Effects {
        let Some(version_id) = self.resolved_version_id else {
            return self.emit_error(GetObjectError::GetObjectFailed);
        };
        let Some(reference_advance) = self.reference_advance else {
            return self.emit_error(GetObjectError::GetObjectFailed);
        };
        self.state = GetObjectState::QueueSuccessorReplication;
        smallvec![Effect::SubOperation(boxed_suboperation(
            QueueLiveVersionReplicationOperation::new(QueueLiveVersionReplicationInput {
                local_node_id: self.input.node_id,
                auth_context: self.auth_context(),
                bucket: self.input.bucket.clone(),
                key: self.input.key.clone(),
                version_id,
                delete_marker: false,
            })
            .with_reference_advance(reference_advance),
            |result| Event::SubOperation(SubOperationEvent::LiveReplicationQueued {
                result: result.map(|_| ()).map_err(|error| error.to_string()),
            }),
        ))]
    }

    fn handle_successor_queued(&mut self, event: Event) -> Effects {
        let Event::SubOperation(SubOperationEvent::LiveReplicationQueued { result }) = event else {
            return self.emit_error(GetObjectError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::SubOperation(SubOperationEvent::LiveReplicationQueued)",
                received: event,
            });
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
        let Some(observation) = self.advance_observation.clone() else {
            return self.emit_error(GetObjectError::GetObjectFailed);
        };
        self.last_refresh = Some(SystemTime::now());
        self.serve_reference_source(observation)
    }

    /// Aborts an in-flight advance transaction and re-reads against the winner.
    fn restart_after_conflict(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => {
                self.state = GetObjectState::RestartReference;
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            None => self.restart_reference_read(),
        }
    }

    fn handle_restart_reference(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::TransactionAborted { .. }) => {
                self.restart_reference_read()
            }
            Event::Storage(StorageEvent::Error { error }) => self.emit_error(error.into()),
            other => self.emit_error(GetObjectError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionAborted)",
                received: other,
            }),
        }
    }

    /// Re-reads the current head after conflict. The preserved advance counter
    /// makes repeated source drift terminate.
    fn restart_reference_read(&mut self) -> Effects {
        self.txn_id = None;
        self.reference_access = None;
        self.reference_cached = None;
        self.reference_last_refresh = None;
        self.reference_creator = None;
        self.reference_advance = None;
        self.advance_pointer = None;
        self.source_metadata = None;
        self.advance_observation = None;
        self.usage_update = None;
        self.resolved_version_id = None;
        self.state = GetObjectState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: true
        })]
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

    pub fn handle_received_blob(&mut self, event: Event) -> Effects {
        if let Event::Blob(BlobEvent::ReadFinished { blob, .. }) = event {
            let Some(location) = self.location.clone() else {
                return self.emit_error(GetObjectError::GetObjectFailed);
            };
            let Some(info) = self.object_info(Some(&location), None) else {
                return self.emit_error(GetObjectError::GetObjectFailed);
            };
            self.state = GetObjectState::Finish;
            self.output = Some(Ok(GetObjectResult {
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
            }));
            smallvec![]
        } else {
            self.emit_error(GetObjectError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Blob(BlobEvent::ReadFinished)",
                received: event,
            })
        }
    }

    pub fn reference_source_received(&mut self, event: Event) -> Effects {
        match event {
            Event::StagingSource(StagingSourceEvent::ReadResult { metadata, stream }) => {
                let Some(head_metadata) = self.source_metadata.as_ref() else {
                    return self.emit_error(GetObjectError::GetObjectFailed);
                };
                if head_metadata.observation_fingerprint() != metadata.observation_fingerprint() {
                    if self.reference_explicit {
                        return self.emit_error(GetObjectError::HistoricalReferenceUnavailable);
                    }
                    return self.restart_after_conflict();
                }
                self.advance_observation = None;
                // `last_refresh` was set by the head handler from the drift check.
                self.source_metadata = Some(metadata);
                self.reference_stream = Some(stream);
                self.finish_reference_output()
            }
            Event::StagingSource(StagingSourceEvent::Error { error }) => {
                self.emit_error(error.into())
            }
            other => self.emit_error(GetObjectError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::StagingSource(StagingSourceEvent::ReadResult)",
                received: other,
            }),
        }
    }

    fn finish_reference_output(&mut self) -> Effects {
        let Some(blob) = self.reference_stream.take() else {
            return self.emit_error(GetObjectError::GetObjectFailed);
        };
        let Some(source_metadata) = self.source_metadata.clone() else {
            return self.emit_error(GetObjectError::GetObjectFailed);
        };
        let Some(info) = self.object_info(None, Some(&source_metadata)) else {
            return self.emit_error(GetObjectError::GetObjectFailed);
        };

        self.state = GetObjectState::Finish;
        self.output = Some(Ok(GetObjectResult {
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
        }));
        smallvec![]
    }
}

impl Operation for GetObjectOperation {
    type Output = GetObjectResult;
    type Error = GetObjectError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        match &self.state {
            GetObjectState::Init => self.handle_init(),
            GetObjectState::StartTransaction => self.handle_transaction_started(event),
            GetObjectState::GetVersion => self.handle_received_version(event),
            GetObjectState::CheckManagedCopy => self.handle_managed_copy(event),
            GetObjectState::GetBlobLocation => self.location_read(event),
            GetObjectState::GetCurrentVersion => self.current_version_received(event),
            GetObjectState::ResolveReferenceAccess => self.reference_access_resolved(event),
            GetObjectState::ReadMultipartSummary => self.summary_read(event),
            GetObjectState::CommitTransaction => self.handle_transaction_committed(event),
            GetObjectState::HeadReferenceSource => self.reference_head_received(event),
            GetObjectState::StartAdvanceTransaction => self.handle_advance_started(event),
            GetObjectState::ReadHeadForAdvance => self.handle_advance_head(event),
            GetObjectState::ReadCurrentForAdvance => self.handle_advance_version(event),
            GetObjectState::WriteSuccessor => self.handle_successor_written(event),
            GetObjectState::UpdateReferenceUsage => self.handle_advance_usage(event),
            GetObjectState::CommitAdvance => self.handle_advance_committed(event),
            GetObjectState::QueueSuccessorReplication => self.handle_successor_queued(event),
            GetObjectState::RestartReference => self.handle_restart_reference(event),
            GetObjectState::GetBlob => self.handle_received_blob(event),
            GetObjectState::ReadReferenceSource => self.reference_source_received(event),
            GetObjectState::Finish => smallvec![],
            GetObjectState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, GetObjectState::Finish | GetObjectState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.output {
            Some(Ok(value)) => Ok(value),
            Some(Err(error)) => Err(error),
            None => Err(GetObjectError::NotFinished),
        }
    }

    fn abort(&mut self) -> Effects {
        self.txn_id.take().map_or_else(SmallVec::new, |txn_id| {
            smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
        })
    }
}

/// Reads an object, continuing against the realm's holders when this node holds
/// the version record but not its bytes. Only User nodes route, since elsewhere a
/// missing local blob is a fault; ranged requests are not routed (bao serves whole blobs).
pub async fn get_object_routed(
    context: &DriverContext,
    input: GetObjectInput,
    restrictions: Option<Vec<PathRestriction>>,
) -> Result<GetObjectResult, GetObjectError> {
    let ranged = input.range.is_some();
    let user_id = input.user_identity;
    let operation = GetObjectOperation::new(input).with_restrictions(restrictions.clone());
    let result = drive(operation, context).await;
    let Err(GetObjectError::BlobNotLocal {
        blake3,
        version_id,
        metadata,
        version_created_at,
        source_policies,
    }) = result
    else {
        return result;
    };
    if ranged || !local_is_user(context, user_id.realm_id).await {
        return Err(GetObjectError::GetObjectFailed);
    }
    let read = RoutedRead {
        user_id,
        blake3,
        version_id,
        metadata,
        version_created_at,
        source_policies,
        restrictions,
    };
    routed_blob(context, read).await
}

/// Resolves complete object facts without transferring holder bytes.
pub async fn get_object_info(
    context: &DriverContext,
    input: GetObjectInput,
    restrictions: Option<Vec<PathRestriction>>,
) -> Result<ObjectInfo, GetObjectError> {
    if input.range.is_some() {
        return Err(GetObjectError::InvalidRange);
    }
    let user_id = input.user_identity;
    let operation = GetObjectOperation::new(input).with_restrictions(restrictions.clone());
    match drive(operation, context).await {
        Ok(result) => Ok(result.info),
        Err(GetObjectError::BlobNotLocal {
            blake3,
            version_id,
            metadata,
            version_created_at,
            source_policies,
        }) if local_is_user(context, user_id.realm_id).await => {
            let read = RoutedRead {
                user_id,
                blake3,
                version_id,
                metadata,
                version_created_at,
                source_policies,
                restrictions,
            };
            routed_metadata(context, read).await
        }
        Err(error) => Err(error),
    }
}

/// One holder-backed read: the local version facts plus the caller's scoped
/// credential, so the routed result is never less constrained than the record.
struct RoutedRead {
    user_id: UserId,
    blake3: [u8; 32],
    version_id: Option<Ulid>,
    metadata: HashMap<String, String>,
    version_created_at: Option<SystemTime>,
    source_policies: Vec<PlacementPolicyRef>,
    restrictions: Option<Vec<PathRestriction>>,
}

/// How the consulted holders failed, folded so the final answer keeps the most
/// informative cause. A read where no holder served is absence only when every
/// holder confirmed absence.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct HolderFailures {
    /// Whether at least one holder answered at all, so zero holders stay
    /// distinguishable from every holder confirming absence.
    contacted: bool,
    governed: bool,
    denied: bool,
    integrity: bool,
    unavailable: bool,
    metadata_only: bool,
}

impl HolderFailures {
    fn record(&mut self, error: BaoReadError) {
        self.contacted = true;
        match error {
            BaoReadError::Refused(BaoReadRefusal::NotFound) => {}
            BaoReadError::Refused(BaoReadRefusal::ReadDenied) => self.denied = true,
            BaoReadError::Refused(BaoReadRefusal::HashMismatch) => self.integrity = true,
            BaoReadError::Refused(
                BaoReadRefusal::BackendFailure
                | BaoReadRefusal::RealmPeerDenied
                | BaoReadRefusal::InvalidTarget,
            ) => self.unavailable = true,
            BaoReadError::GovernedUnavailable
            | BaoReadError::PolicyRequired { .. }
            | BaoReadError::PolicyDenied { .. }
            | BaoReadError::Gate(_)
            | BaoReadError::NoDestination => self.governed = true,
            BaoReadError::Blob(_)
            | BaoReadError::Conversion(_)
            | BaoReadError::Unexpected { .. }
            | BaoReadError::NotFinished
            | BaoReadError::ManagedCopy(_) => self.unavailable = true,
        }
    }

    fn into_error(self) -> GetObjectError {
        if self.governed {
            GetObjectError::GovernedUnavailable
        } else if self.integrity {
            GetObjectError::HolderIntegrityFailure
        } else if self.denied {
            GetObjectError::HolderAccessDenied
        } else if self.unavailable || self.metadata_only || !self.contacted {
            GetObjectError::HoldersUnavailable
        } else {
            GetObjectError::NoSuchKey
        }
    }

    fn consider(&mut self, result: Result<BaoReadOutput, BaoReadError>) -> Option<BaoReadOutput> {
        match result {
            Ok(output @ BaoReadOutput::Stream { .. }) => Some(output),
            Ok(BaoReadOutput::Metadata { .. }) => {
                self.contacted = true;
                self.metadata_only = true;
                None
            }
            Err(error) => {
                self.record(error);
                None
            }
        }
    }
}

async fn routed_blob(
    context: &DriverContext,
    read: RoutedRead,
) -> Result<GetObjectResult, GetObjectError> {
    let net_handle = context
        .net_handle
        .as_ref()
        .ok_or(GetObjectError::GetObjectFailed)?;
    let realm_id = read.user_id.realm_id;
    let holders = drive(
        GetBlobHoldersOperation::new(read.blake3, realm_id, net_handle.node_id()),
        context,
    )
    .await
    .map_err(|_| GetObjectError::GetObjectFailed)?;
    let summary = local_multipart_summary(context, read.version_id).await?;

    let mut failures = HolderFailures::default();
    for holder in holders {
        let request = BaoReadRequest {
            auth_context: AuthContext {
                user_id: read.user_id,
                realm_id,
                path_restrictions: read.restrictions.clone(),
                session: None,
            },
            realm_id,
            target: BaoReadTarget::Blake3(read.blake3),
            expected_blake3: Some(read.blake3),
            metadata_only: false,
            destination: None,
            known_refs: Vec::new(),
        };
        match failures.consider(managed_read(context, holder, request).await) {
            Some(BaoReadOutput::Stream {
                blob,
                size,
                etag,
                hashes,
                ..
            }) => {
                return Ok(routed_result(read, blob, size, etag, hashes, summary));
            }
            None => {}
            Some(BaoReadOutput::Metadata { .. }) => unreachable!("metadata does not serve bytes"),
        }
    }
    Err(failures.into_error())
}

async fn routed_metadata(
    context: &DriverContext,
    read: RoutedRead,
) -> Result<ObjectInfo, GetObjectError> {
    let net_handle = context
        .net_handle
        .as_ref()
        .ok_or(GetObjectError::GetObjectFailed)?;
    let realm_id = read.user_id.realm_id;
    let holders = drive(
        GetBlobHoldersOperation::new(read.blake3, realm_id, net_handle.node_id()),
        context,
    )
    .await
    .map_err(|_| GetObjectError::GetObjectFailed)?;
    let summary = local_multipart_summary(context, read.version_id).await?;
    let mut failures = HolderFailures::default();

    for holder in holders {
        let request = BaoReadRequest {
            auth_context: AuthContext {
                user_id: read.user_id,
                realm_id,
                path_restrictions: read.restrictions.clone(),
                session: None,
            },
            realm_id,
            target: BaoReadTarget::Blake3(read.blake3),
            expected_blake3: Some(read.blake3),
            metadata_only: true,
            destination: None,
            known_refs: Vec::new(),
        };
        match managed_read(context, holder, request).await {
            Ok(BaoReadOutput::Metadata {
                size, etag, hashes, ..
            }) => {
                return Ok(routed_info(&read, size, etag, hashes, summary));
            }
            Ok(BaoReadOutput::Stream { .. }) => failures.unavailable = true,
            Err(error) => failures.record(error),
        }
    }
    Err(failures.into_error())
}

/// Holder hashes describe the bytes; the local version describes logical multipart facts.
fn routed_result(
    read: RoutedRead,
    blob: BackendStream<Result<Bytes, StreamError>>,
    size: u64,
    etag: Option<String>,
    hashes: HashMap<String, Vec<u8>>,
    summary: Option<MultipartObjectSummary>,
) -> GetObjectResult {
    let info = routed_info(&read, size, etag, hashes, summary);
    GetObjectResult {
        blob,
        location: None,
        metadata: read.metadata,
        info,
        source_metadata: None,
        source_binding: None,
        last_refresh: None,
        version_id: read.version_id,
        resolved_version_id: read.version_id,
        resolved_range: None,
        source_policies: read.source_policies,
    }
}

fn routed_info(
    read: &RoutedRead,
    size: u64,
    etag: Option<String>,
    hashes: HashMap<String, Vec<u8>>,
    summary: Option<MultipartObjectSummary>,
) -> ObjectInfo {
    let (checksum_type, composite_hashes, part_count) = match summary {
        Some(summary) => (
            summary.checksum_type,
            summary.composite_hashes,
            Some(summary.part_count),
        ),
        None => (MultipartChecksumType::FullObject, HashMap::new(), None),
    };
    ObjectInfo {
        size,
        version_created_at: read.version_created_at,
        etag,
        checksum_type,
        hashes,
        composite_hashes,
        part_count,
    }
}

/// The local version owns multipart facts even when another holder supplies bytes.
async fn local_multipart_summary(
    context: &DriverContext,
    version_id: Option<Ulid>,
) -> Result<Option<MultipartObjectSummary>, GetObjectError> {
    let Some(version_id) = version_id else {
        return Ok(None);
    };
    let key = MultipartObjectMetadataKey::summary(version_id)
        .to_bytes()
        .map_err(GetObjectError::ConversionError)?;
    let event = context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: S3_MULTIPART_OBJECT_METADATA_KEYSPACE.to_string(),
            key: key.into(),
            txn_id: None,
        })
        .await;
    match event {
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        event => summary_from_read(event).map_err(|error| match error {
            LookupError::Conversion(error) => GetObjectError::ConversionError(error),
            _ => GetObjectError::GetObjectFailed,
        }),
    }
}

#[cfg(test)]
mod test;
