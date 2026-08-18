use crate::blob::blob_keyspace_helper::blob_location_read;
use crate::blob::managed_copy::{
    CopyRequest, ManagedCopyError, serve_reads, split_serve_reads, validate_registration,
};
use crate::connectors::{
    ResolveVersionSourceBindingInput, resolve_version_source_binding_suboperation,
};
use crate::replication::protocol::ReferenceAdvance;
use crate::replication::queue::{
    LiveReplicationObligationRecord, QueueLiveVersionReplicationInput,
    QueueLiveVersionReplicationOperation, live_obligation_entry,
};
use crate::usage_stats::{UsageCounterUpdate, UsageUpdateError};
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
    #[error("GetObject failed (miserably)")]
    GetObjectFailed,
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

#[derive(Debug, PartialEq)]
pub struct GetObjectResult {
    pub blob: BackendStream<Result<Bytes, StreamError>>,
    pub location: Option<BackendLocation>,
    pub metadata: HashMap<String, String>,
    pub source_metadata: Option<SourceMetadata>,
    pub source_binding: Option<VersionSourceBinding>,
    pub last_refresh: Option<SystemTime>,
    pub version_created_at: Option<SystemTime>,
    pub version_id: Option<Ulid>,
    pub resolved_version_id: Option<Ulid>,
    pub checksum_type: MultipartChecksumType,
    pub composite_hashes: HashMap<String, Vec<u8>>,
    pub part_count: Option<usize>,
    pub resolved_range: Option<ResolvedObjectRange>,
    /// Refs sealed on the version that was read. A copy unions them with its
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
        }
    }

    pub fn emit_error(&mut self, error: GetObjectError) -> Effects {
        self.state = GetObjectState::Error;
        self.output = Some(Err(error));
        smallvec![]
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

    fn handle_received_current_version(&mut self, event: Event) -> Effects {
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
                // The access-driven successor-on-drift core (#256) lands on this
                // path. Deferred as enhancements: verified cache + singleflight
                // (#375), general one-hop origin relay (#380), sync poller (#314).
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
                smallvec![resolve_version_source_binding_suboperation(
                    ResolveVersionSourceBindingInput { source },
                )]
            }
        }
    }

    fn read_blob_location(&mut self, key: BlobLocationKey) -> Effects {
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
        let key = ManagedCopyKey::new(
            VersionKey::new(&self.input.bucket, &self.input.key, version_id),
            backend.clone(),
        );
        let effect = match serve_reads(&key, self.txn_id) {
            Ok(effect) => effect,
            Err(err) => return self.emit_error(err.into()),
        };
        self.pending_copy = Some(key);
        self.pending_location = Some(BlobLocationKey::new(blob_hash, backend));
        self.state = GetObjectState::CheckManagedCopy;
        smallvec![effect]
    }

    fn handle_managed_copy(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.emit_error(GetObjectError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::BatchReadResult)",
                received: event,
            });
        };
        let (copy, subject) = match split_serve_reads(values) {
            Ok(split) => split,
            Err(err) => return self.emit_error(err.into()),
        };
        let (Some(copy_key), Some(key)) = (self.pending_copy.take(), self.pending_location.take())
        else {
            return self.emit_error(GetObjectError::GetObjectFailed);
        };
        if let Err(err) = validate_registration(
            copy.as_deref(),
            &CopyRequest {
                key: &copy_key,
                node_id: Some(self.input.node_id),
                blake3: Some(key.blake3_hash),
                refs: &self.source_policies,
                subject_generation: Some(subject.subject.generation),
            },
        ) {
            return self.emit_error(err.into());
        }
        self.read_blob_location(key)
    }

    fn handle_blob_location_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(GetObjectError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received: event,
            });
        };

        let Some(value) = value else {
            return self.emit_error(GetObjectError::GetObjectFailed);
        };

        let location = match BackendLocation::from_bytes(value.as_ref()) {
            Ok(location) => location,
            Err(err) => return self.emit_error(GetObjectError::ConversionError(err)),
        };

        self.read_multipart_summary(location, self.resolved_version_id)
    }

    fn handle_resolved_reference_access(&mut self, event: Event) -> Effects {
        match event {
            Event::SubOperation(SubOperationEvent::VersionSourceAccessResolved {
                result: Ok(access),
            }) => {
                self.reference_access = Some(access);
                self.commit_and_read_reference()
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
            return self.commit_and_read_blob();
        };

        let key = match MultipartObjectMetadataKey::summary(version_id).to_bytes() {
            Ok(key) => key.into(),
            Err(err) => return self.emit_error(GetObjectError::ConversionError(err)),
        };

        self.state = GetObjectState::ReadMultipartSummary;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_MULTIPART_OBJECT_METADATA_KEYSPACE.to_string(),
            key,
            txn_id: Some(txn_id),
        })]
    }

    pub fn handle_multipart_summary_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.emit_error(GetObjectError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::ReadResult)",
                received: event,
            });
        };

        if let Some(summary) =
            value.and_then(|value| MultipartObjectSummary::from_bytes(value.as_ref()).ok())
        {
            self.checksum_type = summary.checksum_type;
            self.composite_hashes = summary.composite_hashes;
            self.part_count = Some(summary.part_count);
        }

        self.commit_and_read_blob()
    }

    fn commit_and_read_blob(&mut self) -> Effects {
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

    fn commit_and_read_reference(&mut self) -> Effects {
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

    pub fn handle_reference_source_head(&mut self, event: Event) -> Effects {
        match event {
            Event::StagingSource(StagingSourceEvent::HeadResult { metadata }) => {
                let baseline = self
                    .reference_cached
                    .as_ref()
                    .map(SourceMetadata::observation_fingerprint);
                let drifted = baseline != Some(metadata.observation_fingerprint());

                if drifted {
                    // A pinned historical version whose live observation drifted
                    // cannot serve current bytes: its bytes were never cached
                    // (#375 deferred).
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

    pub fn handle_received_blob(&mut self, event: Event) -> Effects {
        if let Event::Blob(BlobEvent::ReadFinished { blob, .. }) = event {
            let Some(location) = self.location.clone() else {
                return self.emit_error(GetObjectError::GetObjectFailed);
            };
            self.state = GetObjectState::Finish;
            self.output = Some(Ok(GetObjectResult {
                blob,
                location: Some(location),
                metadata: self.metadata.clone(),
                source_metadata: None,
                source_binding: self.source_binding.clone(),
                last_refresh: None,
                version_created_at: self.version_created_at,
                version_id: self.resolved_version_id.or(self.input.version_id),
                resolved_version_id: self.resolved_version_id,
                checksum_type: self.checksum_type,
                composite_hashes: self.composite_hashes.clone(),
                part_count: self.part_count,
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

    pub fn handle_received_reference_source(&mut self, event: Event) -> Effects {
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

        self.state = GetObjectState::Finish;
        self.output = Some(Ok(GetObjectResult {
            blob,
            location: None,
            metadata: self.metadata.clone(),
            source_metadata: Some(source_metadata),
            source_binding: self.source_binding.clone(),
            last_refresh: self.last_refresh,
            version_created_at: self.version_created_at,
            version_id: self.resolved_version_id.or(self.input.version_id),
            resolved_version_id: self.resolved_version_id,
            checksum_type: self.checksum_type,
            composite_hashes: self.composite_hashes.clone(),
            part_count: self.part_count,
            resolved_range: self.resolved_range.clone(),
            source_policies: self.source_policies.clone(),
        }));
        smallvec![]
    }
}

impl Operation for GetObjectOperation {
    type Output = Option<Result<GetObjectResult, GetObjectError>>;
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
            GetObjectState::GetBlobLocation => self.handle_blob_location_read(event),
            GetObjectState::GetCurrentVersion => self.handle_received_current_version(event),
            GetObjectState::ResolveReferenceAccess => self.handle_resolved_reference_access(event),
            GetObjectState::ReadMultipartSummary => self.handle_multipart_summary_read(event),
            GetObjectState::CommitTransaction => self.handle_transaction_committed(event),
            GetObjectState::HeadReferenceSource => self.handle_reference_source_head(event),
            GetObjectState::StartAdvanceTransaction => self.handle_advance_started(event),
            GetObjectState::ReadHeadForAdvance => self.handle_advance_head(event),
            GetObjectState::ReadCurrentForAdvance => self.handle_advance_version(event),
            GetObjectState::WriteSuccessor => self.handle_successor_written(event),
            GetObjectState::UpdateReferenceUsage => self.handle_advance_usage(event),
            GetObjectState::CommitAdvance => self.handle_advance_committed(event),
            GetObjectState::QueueSuccessorReplication => self.handle_successor_queued(event),
            GetObjectState::RestartReference => self.handle_restart_reference(event),
            GetObjectState::GetBlob => self.handle_received_blob(event),
            GetObjectState::ReadReferenceSource => self.handle_received_reference_source(event),
            GetObjectState::Finish => smallvec![],
            GetObjectState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, GetObjectState::Finish | GetObjectState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if GetObjectState::Error == self.state {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(GetObjectError::GetObjectFailed);
        }
        Ok(self.output)
    }

    fn abort(&mut self) -> Effects {
        self.txn_id.take().map_or_else(SmallVec::new, |txn_id| {
            smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
        })
    }
}

#[cfg(test)]
mod test {
    use crate::driver::{DriverContext, drive};
    use crate::replication::protocol::ReferenceAdvance;
    use crate::replication::queue::LiveReplicationObligationRecord;
    use crate::s3::get_object::{
        GetObjectError, GetObjectInput, GetObjectOperation, GetObjectState, MAX_AUTO_ADVANCES,
        MAX_DRIFT_ADVANCE_ATTEMPTS, MIN_ADVANCE_INTERVAL, ObjectRangeRequest,
    };
    use crate::usage_stats::UsageCounterUpdate;
    use aruna_blob::blob::BlobHandler;
    use aruna_blob::hash::Hasher;
    use aruna_core::UserId;
    use aruna_core::effects::{BlobEffect, Effect, StagingSourceEffect, StorageEffect};
    use aruna_core::egress::EgressPolicy;
    use aruna_core::events::SubOperationEvent;
    use aruna_core::events::{Event, StagingSourceEvent, StorageEvent};
    use aruna_core::keyspaces::{
        BLOB_HEAD_KEYSPACE, BLOB_LIVE_REPLICATION_OBLIGATION_KEYSPACE, BLOB_LOCATIONS_KEYSPACE,
        BLOB_VERSIONS_KEYSPACE,
    };
    use aruna_core::operation::Operation;
    use aruna_core::structs::{
        Backend, BackendConfig, BackendLocation, BackendRef, BlobHeadKey, BlobLocationKey,
        BlobVersion, BlobVersionState, CurrentVersionPointer, MultipartChecksumType,
        PathRestriction, Permission, PortableSourceDescriptor, RealmId, ResolvedSourceAccess,
        SourceConnectorKind, SourceMetadata, StagingStrategy, UsageDelta, VersionKey,
        VersionSourceBinding, usage_group_key,
    };
    use aruna_net::{NetConfig, NetHandle};
    use aruna_storage::storage;
    use axum::{Router, routing::get};
    use bytes::Bytes;
    use futures_util::{StreamExt, stream};
    use std::collections::HashMap;
    use std::path::Path;
    use std::time::{Duration, SystemTime};
    use tempfile::tempdir;
    use tokio::net::TcpListener;
    use ulid::Ulid;

    fn test_node_id() -> aruna_core::NodeId {
        iroh::SecretKey::from_bytes(&[7; 32]).public()
    }

    async fn spawn_reference_server(body: &'static [u8]) -> String {
        let app =
            Router::new().route(
                "/folder/file.txt",
                get(move || async move {
                    ([("content-type", "text/plain"), ("etag", "etag-123")], body)
                }),
            );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{}", addr)
    }

    #[test]
    fn resolves_explicit_object_range() {
        let resolved = ObjectRangeRequest::StartEnd { start: 2, end: 5 }
            .resolve(10)
            .unwrap();

        assert_eq!(resolved.range, 2..6);
        assert_eq!(resolved.content_length, 4);
        assert_eq!(resolved.content_range, "bytes 2-5/10");
    }

    #[test]
    fn resolves_suffix_object_range() {
        let resolved = ObjectRangeRequest::Suffix { length: 3 }
            .resolve(10)
            .unwrap();

        assert_eq!(resolved.range, 7..10);
        assert_eq!(resolved.content_length, 3);
        assert_eq!(resolved.content_range, "bytes 7-9/10");
    }

    #[test]
    fn resolves_open_ended_object_range() {
        let resolved = ObjectRangeRequest::Start { start: 4 }.resolve(10).unwrap();

        assert_eq!(resolved.range, 4..10);
        assert_eq!(resolved.content_length, 6);
        assert_eq!(resolved.content_range, "bytes 4-9/10");
    }

    #[test]
    fn rejects_invalid_object_ranges() {
        assert_eq!(
            ObjectRangeRequest::Suffix { length: 1 }.resolve(0),
            Err(GetObjectError::InvalidRange)
        );
        assert_eq!(
            ObjectRangeRequest::Start { start: 10 }.resolve(10),
            Err(GetObjectError::InvalidRange)
        );
        assert_eq!(
            ObjectRangeRequest::StartEnd { start: 6, end: 5 }.resolve(10),
            Err(GetObjectError::InvalidRange)
        );
    }

    #[test]
    fn materialized_range_read_emits_blob_read_range() {
        let mut operation = GetObjectOperation::new(GetObjectInput {
            bucket: "s3test".to_string(),
            key: "range.txt".to_string(),
            version_id: None,
            range: Some(ObjectRangeRequest::StartEnd { start: 2, end: 4 }),
            group_id: Ulid::generate(),
            user_identity: UserId::local(Ulid::generate(), RealmId([0u8; 32])),
            node_id: test_node_id(),
        });
        let txn_id = Ulid::generate();
        let location = BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/tmp".to_string(),
            storage_bucket: "aruna_test".to_string(),
            backend_path: "s3test/range.txt".to_string(),
            ulid: Ulid::generate(),
            compressed: false,
            encrypted: false,
            created_by: Default::default(),
            created_at: SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 10,
            hashes: HashMap::new(),
        };
        operation.txn_id = Some(txn_id);
        operation.location = Some(location.clone());

        let effects = operation.commit_and_read_blob();

        assert!(matches!(
            effects.as_slice(),
            [
                Effect::Storage(StorageEffect::CommitTransaction { txn_id: committed_txn_id }),
                Effect::Blob(BlobEffect::ReadRange { location: emitted_location, range })
            ] if *committed_txn_id == txn_id && emitted_location == &location && range == &(2..5)
        ));
    }

    #[test]
    fn reference_range_read_heads_then_reads_resolved_range() {
        let mut operation = GetObjectOperation::new(GetObjectInput {
            bucket: "s3test".to_string(),
            key: "range.txt".to_string(),
            version_id: None,
            range: Some(ObjectRangeRequest::Suffix { length: 4 }),
            group_id: Ulid::generate(),
            user_identity: UserId::local(Ulid::generate(), RealmId([0u8; 32])),
            node_id: test_node_id(),
        });
        let txn_id = Ulid::generate();
        let access = ResolvedSourceAccess::OpenDal {
            kind: SourceConnectorKind::Http,
            config: HashMap::new(),
            path: "folder/file.txt".to_string(),
            version: None,
        };
        operation.txn_id = Some(txn_id);
        operation.reference_access = Some(access.clone());

        let effects = operation.commit_and_read_reference();
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::CommitTransaction { txn_id: committed_txn_id })]
                if *committed_txn_id == txn_id
        ));

        let effects = operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id,
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::StagingSource(StagingSourceEffect::Head { access: emitted_access })]
                if emitted_access == &access
        ));

        let metadata = SourceMetadata {
            content_length: 10,
            content_type: Some("text/plain".to_string()),
            etag: None,
            last_modified: None,
            source_version: None,
        };
        // Baseline matches the live head, so this undrifted read serves directly.
        operation.reference_cached = Some(metadata.clone());
        let effects = operation.step(Event::StagingSource(StagingSourceEvent::HeadResult {
            metadata: metadata.clone(),
        }));

        assert!(matches!(
            effects.as_slice(),
            [Effect::StagingSource(StagingSourceEffect::Read { access: emitted_access, range })]
                if emitted_access == &access && range == &Some(6..10)
        ));

        let effects = operation.step(Event::StagingSource(StagingSourceEvent::ReadResult {
            metadata,
            stream: aruna_core::stream::BackendStream::new(stream::iter(vec![Ok::<
                _,
                std::io::Error,
            >(
                Bytes::from_static(b"ence"),
            )])),
        }));
        assert!(effects.is_empty());
        let result = operation.finalize().unwrap().unwrap().unwrap();
        let resolved_range = result.resolved_range.unwrap();
        assert_eq!(resolved_range.range, 6..10);
        assert_eq!(resolved_range.content_length, 4);
        assert_eq!(resolved_range.content_range, "bytes 6-9/10");
    }

    #[test]
    fn range_drift_restarts() {
        let mut operation = GetObjectOperation::new(GetObjectInput {
            bucket: "s3test".to_string(),
            key: "range.txt".to_string(),
            version_id: None,
            range: Some(ObjectRangeRequest::StartEnd { start: 1, end: 3 }),
            group_id: Ulid::generate(),
            user_identity: UserId::local(Ulid::generate(), RealmId([0u8; 32])),
            node_id: test_node_id(),
        });
        operation.source_metadata = Some(SourceMetadata {
            content_length: 10,
            content_type: None,
            etag: None,
            last_modified: None,
            source_version: None,
        });
        operation.resolved_range = Some(
            ObjectRangeRequest::StartEnd { start: 1, end: 3 }
                .resolve(10)
                .unwrap(),
        );
        operation.state = GetObjectState::ReadReferenceSource;

        let effects = operation.step(Event::StagingSource(StagingSourceEvent::ReadResult {
            metadata: SourceMetadata {
                content_length: 11,
                content_type: None,
                etag: None,
                last_modified: None,
                source_version: None,
            },
            stream: aruna_core::stream::BackendStream::new(stream::iter(vec![Ok::<
                _,
                std::io::Error,
            >(
                Bytes::from_static(b"bad"),
            )])),
        }));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::StartTransaction {
                read: true
            })]
        ));
        assert_eq!(operation.state, GetObjectState::StartTransaction);
    }

    #[tokio::test]
    pub async fn test_get_object() {
        let temp_handle = tempdir().unwrap();
        let temp_root = temp_handle.path().to_str().unwrap();
        let storage_handle = storage::FjallStorage::open(temp_root).unwrap();
        let net_handle = NetHandle::new(NetConfig::default(), storage_handle.clone())
            .await
            .unwrap();
        let blob_handle = BlobHandler::with_egress(
            BackendConfig {
                backend_type: Backend::FileSystem,
                bucket_prefix: Some("aruna_".to_string()),
                max_bucket_size: Some(100000),
                multipart_bucket: Some("multipart".to_string()),
                root: temp_root.to_string(),
                service_config: HashMap::new(),
                timeouts: Default::default(),
            },
            storage_handle.clone(),
            net_handle.clone(),
            EgressPolicy::loopback(),
        )
        .await
        .unwrap();
        let content = "Hello, World!";
        let hasher = Hasher::new_with_bytes(content.as_bytes());
        let hashes = hasher.finalize();
        let blake3_hash: [u8; 32] = hashes.blake3.into();

        let bucket = "s3test".to_string();
        let key = "test.txt".to_string();
        let blob_ulid = Ulid::generate();
        let location = BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: temp_root.to_string(),
            storage_bucket: format!("aruna_{}", Ulid::generate()),
            backend_path: format!("{bucket}/{key}_{blob_ulid}"),
            ulid: blob_ulid,
            compressed: false,
            encrypted: false,
            created_by: Default::default(),
            created_at: SystemTime::now(),
            staging: false,
            partial: false,
            blob_size: content.len() as u64,
            hashes: hasher.to_map(),
        };

        // Write file + db entries
        std::fs::create_dir_all(
            Path::new(&location.get_full_path().unwrap())
                .parent()
                .unwrap(),
        )
        .unwrap();
        std::fs::write(location.get_full_path().unwrap(), content).unwrap();

        if let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = storage_handle
            .send_storage_effect(StorageEffect::StartTransaction { read: false })
            .await
        {
            let _ = storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: BLOB_LOCATIONS_KEYSPACE.to_string(),
                    key: BlobLocationKey::new(blake3_hash, location.backend.clone())
                        .to_bytes()
                        .into(),
                    value: location.clone().to_bytes().unwrap().into(),
                    txn_id: Some(txn_id),
                })
                .await;

            let version_id = Ulid::generate();
            let _ = storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: BLOB_HEAD_KEYSPACE.to_string(),
                    key: BlobHeadKey::new(&bucket, &key).to_bytes().unwrap().into(),
                    value: CurrentVersionPointer::new(version_id)
                        .to_bytes()
                        .unwrap()
                        .into(),
                    txn_id: Some(txn_id),
                })
                .await;

            let _ = storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                    key: VersionKey::new(&bucket, &key, version_id)
                        .to_bytes()
                        .unwrap()
                        .into(),
                    value: BlobVersion::materialized(
                        blake3_hash,
                        BackendRef::node_default(),
                        location.created_at,
                        location.created_by,
                        None,
                    )
                    .to_bytes()
                    .unwrap()
                    .into(),
                    txn_id: Some(txn_id),
                })
                .await;

            let _ = storage_handle
                .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
                .await;
        } else {
            panic!("Failed to start transaction");
        }

        // Read file with operation
        let driver_ctx = DriverContext {
            storage_handle,
            net_handle: Some(net_handle),
            blob_handle: Some(blob_handle),
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let operation = GetObjectOperation::new(GetObjectInput {
            bucket,
            key,
            version_id: None,
            range: None,
            group_id: Ulid::generate(),
            user_identity: Default::default(),
            node_id: test_node_id(),
        });

        let blob_result = drive(operation, &driver_ctx)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            blob_result.location.as_ref().unwrap().hashes,
            location.hashes
        );
        assert!(blob_result.source_metadata.is_none());
        assert!(blob_result.source_binding.is_none());
        assert!(blob_result.last_refresh.is_none());
        assert_eq!(blob_result.checksum_type, MultipartChecksumType::FullObject);
        let mut blob_stream = blob_result.blob;
        let mut read_buffer = Vec::new();
        while let Some(Ok(bytes)) = blob_stream.next().await {
            read_buffer.extend_from_slice(&bytes);
        }
        assert_eq!(read_buffer, content.as_bytes());
    }

    #[tokio::test]
    pub async fn test_get_object_hash_mismatch() {
        let temp_handle = tempdir().unwrap();
        let temp_root = temp_handle.path().to_str().unwrap();
        let storage_handle = storage::FjallStorage::open(temp_root).unwrap();
        let net_handle = NetHandle::new(NetConfig::default(), storage_handle.clone())
            .await
            .unwrap();
        let blob_handle = BlobHandler::with_egress(
            BackendConfig {
                backend_type: Backend::FileSystem,
                bucket_prefix: Some("aruna_".to_string()),
                max_bucket_size: Some(100000),
                multipart_bucket: Some("multipart".to_string()),
                root: temp_root.to_string(),
                service_config: HashMap::new(),
                timeouts: Default::default(),
            },
            storage_handle.clone(),
            net_handle.clone(),
            EgressPolicy::loopback(),
        )
        .await
        .unwrap();
        let content = "Hello, World!";
        let tampered = "Hallo, World!";
        let hasher = Hasher::new_with_bytes(content.as_bytes());
        let hashes = hasher.finalize();
        let blake3_hash: [u8; 32] = hashes.blake3.into();

        let bucket = "s3test".to_string();
        let key = "test.txt".to_string();
        let blob_ulid = Ulid::generate();
        let location = BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: temp_root.to_string(),
            storage_bucket: format!("aruna_{}", Ulid::generate()),
            backend_path: format!("{bucket}/{key}_{blob_ulid}"),
            ulid: blob_ulid,
            compressed: false,
            encrypted: false,
            created_by: Default::default(),
            created_at: SystemTime::now(),
            staging: false,
            partial: false,
            blob_size: content.len() as u64,
            hashes: hasher.to_map(),
        };

        std::fs::create_dir_all(
            Path::new(&location.get_full_path().unwrap())
                .parent()
                .unwrap(),
        )
        .unwrap();
        std::fs::write(location.get_full_path().unwrap(), tampered).unwrap();

        if let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = storage_handle
            .send_storage_effect(StorageEffect::StartTransaction { read: false })
            .await
        {
            let _ = storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: BLOB_LOCATIONS_KEYSPACE.to_string(),
                    key: BlobLocationKey::new(blake3_hash, location.backend.clone())
                        .to_bytes()
                        .into(),
                    value: location.clone().to_bytes().unwrap().into(),
                    txn_id: Some(txn_id),
                })
                .await;

            let version_id = Ulid::generate();
            let _ = storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: BLOB_HEAD_KEYSPACE.to_string(),
                    key: BlobHeadKey::new(&bucket, &key).to_bytes().unwrap().into(),
                    value: CurrentVersionPointer::new(version_id)
                        .to_bytes()
                        .unwrap()
                        .into(),
                    txn_id: Some(txn_id),
                })
                .await;

            let _ = storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                    key: VersionKey::new(&bucket, &key, version_id)
                        .to_bytes()
                        .unwrap()
                        .into(),
                    value: BlobVersion::materialized(
                        blake3_hash,
                        BackendRef::node_default(),
                        location.created_at,
                        location.created_by,
                        None,
                    )
                    .to_bytes()
                    .unwrap()
                    .into(),
                    txn_id: Some(txn_id),
                })
                .await;

            let _ = storage_handle
                .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
                .await;
        } else {
            panic!("Failed to start transaction");
        }

        let driver_ctx = DriverContext {
            storage_handle,
            net_handle: Some(net_handle),
            blob_handle: Some(blob_handle),
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let operation = GetObjectOperation::new(GetObjectInput {
            bucket,
            key,
            version_id: None,
            range: None,
            group_id: Ulid::generate(),
            user_identity: Default::default(),
            node_id: test_node_id(),
        });

        let mut blob_stream = drive(operation, &driver_ctx)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .blob;
        let mut read_buffer = Vec::new();
        let mut read_error = None;
        while let Some(result) = blob_stream.next().await {
            match result {
                Ok(bytes) => read_buffer.extend_from_slice(&bytes),
                Err(err) => {
                    read_error = Some(err.to_string());
                    break;
                }
            }
        }

        assert_eq!(read_buffer, tampered.as_bytes());
        assert!(read_error.is_some());
        assert!(read_error.unwrap().contains("Integrity check failed"));
    }

    #[tokio::test]
    async fn test_get_reference_object_uses_exact_bound_connector() {
        let endpoint = spawn_reference_server(b"hello reference").await;
        let temp_handle = tempdir().unwrap();
        let temp_root = temp_handle.path().to_str().unwrap();
        let storage_handle = storage::FjallStorage::open(temp_root).unwrap();
        let net_handle = NetHandle::new(NetConfig::default(), storage_handle.clone())
            .await
            .unwrap();
        let blob_handle = BlobHandler::with_egress(
            BackendConfig {
                backend_type: Backend::FileSystem,
                bucket_prefix: Some("aruna_".to_string()),
                max_bucket_size: Some(100000),
                multipart_bucket: Some("multipart".to_string()),
                root: temp_root.to_string(),
                service_config: HashMap::new(),
                timeouts: Default::default(),
            },
            storage_handle.clone(),
            net_handle.clone(),
            EgressPolicy::loopback(),
        )
        .await
        .unwrap();

        let driver_ctx = DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: Some(net_handle),
            blob_handle: Some(blob_handle),
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let bucket = "s3test".to_string();
        let key = "test.txt".to_string();
        let version_id = Ulid::generate();
        let connector_id = Ulid::generate();
        let cached_metadata = SourceMetadata {
            content_length: 15,
            content_type: Some("text/plain".to_string()),
            etag: Some("etag-123".to_string()),
            last_modified: Some(SystemTime::UNIX_EPOCH),
            source_version: None,
        };
        let source = VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: PortableSourceDescriptor {
                kind: SourceConnectorKind::Http,
                public_config: HashMap::from([("endpoint".to_string(), endpoint)]),
                source_path: "folder/file.txt".to_string(),
                version_selector: None,
                capabilities: Vec::new(),
                origin_node_id: None,
            },
            connector_id: Some(connector_id),
        };

        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = storage_handle
            .send_storage_effect(StorageEffect::StartTransaction { read: false })
            .await
        else {
            panic!("Failed to start transaction");
        };

        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_HEAD_KEYSPACE.to_string(),
                key: BlobHeadKey::new(&bucket, &key).to_bytes().unwrap().into(),
                value: CurrentVersionPointer::new(version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                txn_id: Some(txn_id),
            })
            .await;

        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: VersionKey::new(&bucket, &key, version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                value: BlobVersion::reference(
                    source,
                    cached_metadata,
                    SystemTime::UNIX_EPOCH,
                    Default::default(),
                    SystemTime::UNIX_EPOCH,
                )
                .to_bytes()
                .unwrap()
                .into(),
                txn_id: Some(txn_id),
            })
            .await;

        let _ = storage_handle
            .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
            .await;

        let result = drive(
            GetObjectOperation::new(GetObjectInput {
                bucket,
                key,
                version_id: None,
                range: None,
                group_id: Ulid::generate(),
                user_identity: UserId::local(Ulid::generate(), RealmId([0u8; 32])),
                node_id: test_node_id(),
            }),
            &driver_ctx,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        assert!(result.location.is_none());
        assert_eq!(
            result
                .source_metadata
                .as_ref()
                .and_then(|m| m.content_type.clone()),
            Some("text/plain".to_string())
        );
        assert_eq!(
            result
                .source_binding
                .as_ref()
                .map(|binding| binding.strategy.clone()),
            Some(StagingStrategy::Reference)
        );
        assert!(result.last_refresh.is_some());
        let mut stream = result.blob;
        let mut read_buffer = Vec::new();
        while let Some(Ok(bytes)) = stream.next().await {
            read_buffer.extend_from_slice(&bytes);
        }
        assert_eq!(read_buffer, b"hello reference");

        let Event::Storage(StorageEvent::ReadResult { value, .. }) = driver_ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: VersionKey::new("s3test", "test.txt", version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                txn_id: None,
            })
            .await
        else {
            panic!("missing version metadata");
        };
        let metadata = BlobVersion::from_bytes(value.unwrap().as_ref()).unwrap();
        let BlobVersionState::Reference {
            cached_metadata,
            last_refresh,
            ..
        } = metadata.state
        else {
            panic!("expected reference metadata");
        };
        assert_eq!(cached_metadata.content_type.as_deref(), Some("text/plain"));
        assert_eq!(last_refresh, SystemTime::UNIX_EPOCH);
    }

    // A drifted current-version reference read records a same-binding successor
    // (spec REQ-S3-DATA-MODEL-001) rather than silently floating, and the prior
    // version stays immutable.
    #[tokio::test]
    async fn drift_creates_successor() {
        let endpoint = spawn_reference_server(b"hello reference").await;
        let temp_handle = tempdir().unwrap();
        let temp_root = temp_handle.path().to_str().unwrap();
        let storage_handle = storage::FjallStorage::open(temp_root).unwrap();
        let net_handle = NetHandle::new(NetConfig::default(), storage_handle.clone())
            .await
            .unwrap();
        let blob_handle = BlobHandler::with_egress(
            BackendConfig {
                backend_type: Backend::FileSystem,
                bucket_prefix: Some("aruna_".to_string()),
                max_bucket_size: Some(100000),
                multipart_bucket: Some("multipart".to_string()),
                root: temp_root.to_string(),
                service_config: HashMap::new(),
                timeouts: Default::default(),
            },
            storage_handle.clone(),
            net_handle.clone(),
            EgressPolicy::loopback(),
        )
        .await
        .unwrap();

        let driver_ctx = DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: Some(net_handle),
            blob_handle: Some(blob_handle),
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let version_id = Ulid::generate();
        let source = VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: PortableSourceDescriptor {
                kind: SourceConnectorKind::Http,
                public_config: HashMap::from([("endpoint".to_string(), endpoint)]),
                source_path: "folder/file.txt".to_string(),
                version_selector: None,
                capabilities: Vec::new(),
                origin_node_id: None,
            },
            connector_id: Some(Ulid::generate()),
        };

        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = storage_handle
            .send_storage_effect(StorageEffect::StartTransaction { read: false })
            .await
        else {
            panic!("Failed to start transaction");
        };

        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_HEAD_KEYSPACE.to_string(),
                key: BlobHeadKey::new("s3test", "refresh.txt")
                    .to_bytes()
                    .unwrap()
                    .into(),
                value: CurrentVersionPointer::new(version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                txn_id: Some(txn_id),
            })
            .await;
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: VersionKey::new("s3test", "refresh.txt", version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                value: BlobVersion::reference(
                    source,
                    SourceMetadata {
                        content_length: 1,
                        content_type: Some("application/octet-stream".to_string()),
                        etag: Some("stale-etag".to_string()),
                        last_modified: None,
                        source_version: None,
                    },
                    SystemTime::UNIX_EPOCH,
                    Default::default(),
                    SystemTime::UNIX_EPOCH,
                )
                .to_bytes()
                .unwrap()
                .into(),
                txn_id: Some(txn_id),
            })
            .await;
        let _ = storage_handle
            .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
            .await;

        let group_id = Ulid::generate();
        let result = drive(
            GetObjectOperation::new(GetObjectInput {
                bucket: "s3test".to_string(),
                key: "refresh.txt".to_string(),
                version_id: None,
                range: None,
                group_id,
                user_identity: UserId::local(Ulid::generate(), RealmId([0u8; 32])),
                node_id: test_node_id(),
            }),
            &driver_ctx,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        let mut stream = result.blob;
        assert!(result.last_refresh.is_some());
        assert_eq!(
            result
                .source_metadata
                .as_ref()
                .map(|metadata| metadata.content_length),
            Some(15)
        );
        assert_eq!(
            result
                .source_metadata
                .as_ref()
                .and_then(|metadata| metadata.content_type.as_deref()),
            Some("text/plain")
        );
        while let Some(Ok(_)) = stream.next().await {}

        // The head now points at a fresh successor, not the drifted version.
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = driver_ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: BLOB_HEAD_KEYSPACE.to_string(),
                key: BlobHeadKey::new("s3test", "refresh.txt")
                    .to_bytes()
                    .unwrap()
                    .into(),
                txn_id: None,
            })
            .await
        else {
            panic!("missing head pointer");
        };
        let successor_id = CurrentVersionPointer::from_bytes(value.unwrap().as_ref())
            .unwrap()
            .version_id;
        assert_ne!(
            successor_id, version_id,
            "a successor must have been created"
        );

        let successor = read_reference_version(&driver_ctx, successor_id).await;
        assert_eq!(successor.content_length, 15);
        assert_eq!(successor.etag.as_deref(), Some("etag-123"));
        assert_eq!(successor.content_type.as_deref(), Some("text/plain"));

        // The superseded version is untouched: successors, never mutation.
        let original = read_reference_version(&driver_ctx, version_id).await;
        assert_eq!(original.content_length, 1);
        assert_eq!(original.etag.as_deref(), Some("stale-etag"));

        // Both versions stay stored, so the successor is charged its full size.
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = driver_ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: aruna_core::keyspaces::USAGE_STATS_KEYSPACE.to_string(),
                key: usage_group_key(group_id).into(),
                txn_id: None,
            })
            .await
        else {
            panic!("missing usage counters");
        };
        let counters =
            aruna_core::structs::UsageCounters::from_bytes(value.unwrap().as_ref()).unwrap();
        assert_eq!(counters.referenced_bytes, 15);

        // The successor's obligation is committed with it, so replication and
        // the repair scanner can both find it.
        let Event::Storage(StorageEvent::IterResult { values, .. }) = driver_ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Iter {
                key_space: BLOB_LIVE_REPLICATION_OBLIGATION_KEYSPACE.to_string(),
                prefix: None,
                start: None,
                limit: 16,
                txn_id: None,
            })
            .await
        else {
            panic!("missing obligation listing");
        };
        let obligations: Vec<_> = values
            .iter()
            .map(|(_, value)| LiveReplicationObligationRecord::from_bytes(value.as_ref()).unwrap())
            .collect();
        assert_eq!(obligations.len(), 1);
        assert_eq!(obligations[0].version_id, successor_id);
        assert_eq!(obligations[0].bucket, "s3test");
    }

    fn observation(content_length: u64) -> SourceMetadata {
        SourceMetadata {
            content_length,
            content_type: None,
            etag: Some(format!("etag-{content_length}")),
            last_modified: None,
            source_version: None,
        }
    }

    fn reference_source() -> VersionSourceBinding {
        VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: PortableSourceDescriptor {
                kind: SourceConnectorKind::Http,
                public_config: HashMap::new(),
                source_path: "folder/file.txt".to_string(),
                version_selector: None,
                capabilities: Vec::new(),
                origin_node_id: None,
            },
            connector_id: Some(Ulid::generate()),
        }
    }

    /// A current-version read whose head observation already drifted.
    fn drifted_operation() -> GetObjectOperation {
        let mut operation = GetObjectOperation::new(GetObjectInput {
            bucket: "s3test".to_string(),
            key: "range.txt".to_string(),
            version_id: None,
            range: None,
            group_id: Ulid::generate(),
            user_identity: UserId::local(Ulid::from_parts(2, 2), RealmId([0u8; 32])),
            node_id: test_node_id(),
        });
        let version = BlobVersion::reference(
            reference_source(),
            observation(4),
            SystemTime::UNIX_EPOCH,
            UserId::local(Ulid::from_parts(1, 1), RealmId([0u8; 32])),
            SystemTime::UNIX_EPOCH,
        )
        .with_metadata(HashMap::from([(
            "label".to_string(),
            "preserved".to_string(),
        )]));
        let _ = operation.read_version(Ulid::generate(), version, false);
        operation.advance_observation = Some(observation(9));
        operation.reference_access = Some(ResolvedSourceAccess::OpenDal {
            kind: SourceConnectorKind::Http,
            config: HashMap::new(),
            path: "folder/file.txt".to_string(),
            version: None,
        });
        operation
    }

    fn head_read(value: Ulid) -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: BlobHeadKey::new("s3test", "range.txt")
                .to_bytes()
                .unwrap()
                .into(),
            value: Some(CurrentVersionPointer::new(value).to_bytes().unwrap().into()),
        })
    }

    /// The stored current reference the advance transaction rereads.
    fn current_version(created_at: SystemTime, advance_count: u16) -> Event {
        let version = BlobVersion::reference(
            reference_source(),
            observation(4),
            created_at,
            UserId::local(Ulid::from_parts(1, 1), RealmId([0u8; 32])),
            created_at,
        )
        .with_metadata(HashMap::from([(
            "label".to_string(),
            "preserved".to_string(),
        )]))
        .with_advance_count(advance_count);
        Event::Storage(StorageEvent::ReadResult {
            key: b"version".to_vec().into(),
            value: Some(version.to_bytes().unwrap().into()),
        })
    }

    /// Drives a drifted read through the head CAS up to the version reread.
    fn advance_to_reread(operation: &mut GetObjectOperation, headed: Ulid) -> Ulid {
        let txn_id = Ulid::generate();
        operation.resolved_version_id = Some(headed);
        operation.txn_id = Some(txn_id);
        operation.state = GetObjectState::ReadHeadForAdvance;
        let effects = operation.step(head_read(headed));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Read { .. })]
        ));
        assert_eq!(operation.state, GetObjectState::ReadCurrentForAdvance);
        txn_id
    }

    fn written_successor(effects: &[Effect]) -> BlobVersion {
        let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects else {
            panic!("expected a single batch write, got {effects:?}")
        };
        BlobVersion::from_bytes(writes[0].2.as_ref()).unwrap()
    }

    fn source_read(metadata: SourceMetadata) -> Event {
        Event::StagingSource(StagingSourceEvent::ReadResult {
            metadata,
            stream: aruna_core::stream::BackendStream::new(stream::iter(vec![Ok::<
                _,
                std::io::Error,
            >(
                Bytes::from_static(b"body"),
            )])),
        })
    }

    // CAS guard: if the head advanced under a drifted read (a concurrent reader
    // already wrote the successor), this read must not write a second one — it
    // aborts and restarts against the winner.
    #[test]
    fn advance_conflict_restarts() {
        let mut operation = drifted_operation();
        let txn_id = Ulid::generate();
        operation.resolved_version_id = Some(Ulid::generate());
        operation.txn_id = Some(txn_id);
        operation.state = GetObjectState::ReadHeadForAdvance;

        // The head already names a different (winning) successor.
        let effects = operation.step(head_read(Ulid::generate()));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { txn_id: aborted })]
                if *aborted == txn_id
        ));
        assert_eq!(operation.state, GetObjectState::RestartReference);
    }

    // The successor's replication obligation must be committed with it, or peers
    // never learn of it and the repair scanner cannot discover it.
    #[test]
    fn advance_writes_obligation() {
        let restrictions = vec![PathRestriction {
            pattern: "/realm/g/group/data/node/s3test/range.txt".to_string(),
            permission: Permission::READ,
        }];
        let mut operation = drifted_operation().with_restrictions(Some(restrictions));
        let creator = operation.reference_creator.unwrap();
        let reader_auth = operation.auth_context();
        let headed = Ulid::generate();
        advance_to_reread(&mut operation, headed);

        let effects = operation.step(current_version(SystemTime::UNIX_EPOCH, 0));

        let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = effects.as_slice() else {
            panic!("expected a single batch write, got {effects:?}")
        };
        let [
            (_, _, version_value),
            (_, _, head_value),
            (key_space, _, obligation_value),
        ] = writes.as_slice()
        else {
            panic!("expected version, head and obligation writes, got {writes:?}")
        };
        let successor = BlobVersion::from_bytes(version_value.as_ref()).unwrap();
        assert_eq!(successor.created_by, creator);
        assert_ne!(successor.created_by, operation.input.user_identity);
        assert_eq!(successor.metadata, operation.metadata);
        let next_pointer = CurrentVersionPointer::from_bytes(head_value.as_ref()).unwrap();
        assert_eq!(key_space, BLOB_LIVE_REPLICATION_OBLIGATION_KEYSPACE);
        let record =
            LiveReplicationObligationRecord::from_bytes(obligation_value.as_ref()).unwrap();
        assert_eq!(Some(record.version_id), operation.resolved_version_id);
        assert_ne!(record.version_id, headed);
        assert_eq!(next_pointer.version_id, record.version_id);
        assert_eq!(next_pointer.generation, 2);
        assert_eq!(record.local_node_id, test_node_id());
        assert!(!record.delete_marker);
        assert_eq!(record.auth_context, reader_auth);
        assert_eq!(
            record.reference_advance,
            Some(ReferenceAdvance {
                generation: next_pointer.generation,
                predecessor: headed,
            })
        );
    }

    // The cooldown is measured against the current version's immutable
    // `created_at`, and the exact interval is already old enough to advance.
    #[test]
    fn advance_at_boundary() {
        let created_at = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        let mut operation = drifted_operation();
        operation.now_override = Some(created_at + MIN_ADVANCE_INTERVAL);
        advance_to_reread(&mut operation, Ulid::generate());

        let effects = operation.step(current_version(created_at, 7));

        let successor = written_successor(effects.as_slice());
        assert_eq!(successor.created_at, created_at + MIN_ADVANCE_INTERVAL);
        assert_eq!(successor.advance_count(), Some(8));
    }

    // One second inside the cooldown fails closed and releases the transaction
    // instead of minting another durable successor.
    #[test]
    fn advance_rejects_early() {
        let created_at = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        let mut operation = drifted_operation();
        operation.now_override = Some(created_at + MIN_ADVANCE_INTERVAL - Duration::from_secs(1));
        let txn_id = advance_to_reread(&mut operation, Ulid::generate());

        let effects = operation.step(current_version(created_at, 0));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { txn_id: aborted })]
                if *aborted == txn_id
        ));
        assert_eq!(
            operation.finalize(),
            Err(GetObjectError::ReferenceSourceChanged)
        );
    }

    // A backwards clock must not grant an unbounded advance budget.
    #[test]
    fn advance_rejects_rollback() {
        let created_at = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
        let mut operation = drifted_operation();
        operation.now_override = Some(created_at - Duration::from_secs(3_600));
        advance_to_reread(&mut operation, Ulid::generate());

        let effects = operation.step(current_version(created_at, 0));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { .. })]
        ));
        assert_eq!(
            operation.finalize(),
            Err(GetObjectError::ReferenceSourceChanged)
        );
    }

    // Count 99 may still mint the hundredth successor.
    #[test]
    fn advance_at_cap() {
        let created_at = SystemTime::UNIX_EPOCH;
        let mut operation = drifted_operation();
        operation.now_override = Some(created_at + MIN_ADVANCE_INTERVAL);
        advance_to_reread(&mut operation, Ulid::generate());

        let effects = operation.step(current_version(created_at, MAX_AUTO_ADVANCES - 1));

        assert_eq!(
            written_successor(effects.as_slice()).advance_count(),
            Some(MAX_AUTO_ADVANCES)
        );
    }

    // At the cap the read fails immediately with its own variant: no retry of the
    // per-request drift attempts, and the cooldown is never consulted.
    #[test]
    fn advance_rejects_exhausted() {
        let created_at = SystemTime::UNIX_EPOCH;
        let mut operation = drifted_operation();
        operation.now_override = Some(created_at + Duration::from_secs(86_400));
        let txn_id = advance_to_reread(&mut operation, Ulid::generate());
        let attempts = operation.drift_attempts;

        let effects = operation.step(current_version(created_at, MAX_AUTO_ADVANCES));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { txn_id: aborted })]
                if *aborted == txn_id
        ));
        assert_eq!(operation.drift_attempts, attempts);
        assert_eq!(
            operation.finalize(),
            Err(GetObjectError::ReferenceAdvanceExhausted)
        );
    }

    // A committed successor is enqueued for live replication before its bytes
    // are served, the same way the normal write path enqueues after its commit.
    #[test]
    fn commit_queues_replication() {
        let mut operation = drifted_operation();
        let txn_id = Ulid::generate();
        operation.resolved_version_id = Some(Ulid::generate());
        operation.reference_advance = Some(ReferenceAdvance {
            generation: 2,
            predecessor: Ulid::generate(),
        });
        operation.txn_id = Some(txn_id);
        operation.state = GetObjectState::CommitAdvance;

        let effects = operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id,
        }));

        assert!(matches!(effects.as_slice(), [Effect::SubOperation(_)]));
        assert_eq!(operation.state, GetObjectState::QueueSuccessorReplication);

        // A failed enqueue leaves the durable obligation for repair and serves.
        let effects = operation.step(Event::SubOperation(
            SubOperationEvent::LiveReplicationQueued {
                result: Err("queue unavailable".to_string()),
            },
        ));

        assert!(matches!(
            effects.as_slice(),
            [Effect::StagingSource(StagingSourceEffect::Read { .. })]
        ));
    }

    // The superseded version stays stored and stays charged, so the successor
    // adds its own full size rather than the size difference.
    #[test]
    fn successor_charges_size() {
        let mut operation = drifted_operation();
        let txn_id = Ulid::generate();
        operation.reference_cached = Some(observation(4));
        operation.txn_id = Some(txn_id);
        operation.state = GetObjectState::WriteSuccessor;

        operation.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));

        assert_eq!(operation.state, GetObjectState::UpdateReferenceUsage);
        let mut expected = UsageCounterUpdate::for_group(
            operation.input.group_id,
            UsageDelta {
                referenced_bytes: 9,
                ..Default::default()
            },
        );
        let _ = expected.start(txn_id);
        assert_eq!(operation.usage_update, Some(expected));
    }

    #[test]
    fn drift_limit_fails() {
        let mut operation = drifted_operation();
        operation.drift_attempts = MAX_DRIFT_ADVANCE_ATTEMPTS;
        operation.state = GetObjectState::HeadReferenceSource;

        let effects = operation.step(Event::StagingSource(StagingSourceEvent::HeadResult {
            metadata: observation(9),
        }));

        assert!(effects.is_empty());
        assert_eq!(
            operation.finalize(),
            Err(GetObjectError::ReferenceSourceChanged)
        );
    }

    // The source can drift again between the head and the read; those bytes must
    // never be served under the VersionId that promised the head observation.
    #[test]
    fn read_drift_restarts() {
        let mut operation = drifted_operation();
        let headed = observation(9);
        let mut read = headed.clone();
        read.etag = Some("changed-etag".to_string());
        operation.advance_observation = None;
        operation.source_metadata = Some(headed);
        operation.state = GetObjectState::ReadReferenceSource;

        let effects = operation.step(source_read(read));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::StartTransaction {
                read: true
            })]
        ));
        assert_eq!(operation.state, GetObjectState::StartTransaction);
        assert!(operation.advance_observation.is_none());
    }

    #[test]
    fn historical_read_fails() {
        let mut operation = drifted_operation();
        let headed = observation(9);
        let mut read = headed.clone();
        read.content_type = Some("changed/type".to_string());
        operation.advance_observation = None;
        operation.reference_explicit = true;
        operation.source_metadata = Some(headed);
        operation.state = GetObjectState::ReadReferenceSource;

        let effects = operation.step(source_read(read));

        assert!(effects.is_empty());
        assert_eq!(
            operation.finalize(),
            Err(GetObjectError::HistoricalReferenceUnavailable)
        );
    }

    // A matching read is served, and the stale promise is cleared with it.
    #[test]
    fn read_match_serves() {
        let mut operation = drifted_operation();
        operation.source_metadata = Some(observation(9));
        operation.state = GetObjectState::ReadReferenceSource;

        let effects = operation.step(source_read(observation(9)));

        assert!(effects.is_empty());
        assert_eq!(operation.state, GetObjectState::Finish);
        assert!(operation.advance_observation.is_none());
    }

    // The restart must observe its own abort before re-reading the head.
    #[test]
    fn restart_needs_abort() {
        let mut operation = drifted_operation();
        operation.state = GetObjectState::RestartReference;

        let effects = operation.step(Event::Storage(StorageEvent::WriteResult {
            key: b"unrelated".to_vec().into(),
        }));

        assert!(effects.is_empty());
        assert!(matches!(
            operation.finalize(),
            Err(GetObjectError::InvalidStateEvent { .. })
        ));

        let mut operation = drifted_operation();
        operation.state = GetObjectState::RestartReference;
        let effects = operation.step(Event::Storage(StorageEvent::TransactionAborted {
            txn_id: Ulid::generate(),
        }));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::StartTransaction {
                read: true
            })]
        ));
    }

    async fn read_reference_version(ctx: &DriverContext, version_id: Ulid) -> SourceMetadata {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: VersionKey::new("s3test", "refresh.txt", version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                txn_id: None,
            })
            .await
        else {
            panic!("missing version metadata");
        };
        let version = BlobVersion::from_bytes(value.unwrap().as_ref()).unwrap();
        let BlobVersionState::Reference {
            cached_metadata, ..
        } = version.state
        else {
            panic!("expected reference metadata");
        };
        cached_metadata
    }

    // A pinned historical reference version whose live source has drifted from
    // its recorded observation cannot serve current bytes: #375 caching is
    // deferred, so there are no historical bytes to return.
    #[tokio::test]
    async fn historical_drift_fails() {
        let endpoint = spawn_reference_server(b"hello reference").await;
        let temp_handle = tempdir().unwrap();
        let temp_root = temp_handle.path().to_str().unwrap();
        let storage_handle = storage::FjallStorage::open(temp_root).unwrap();
        let net_handle = NetHandle::new(NetConfig::default(), storage_handle.clone())
            .await
            .unwrap();
        let blob_handle = BlobHandler::with_egress(
            BackendConfig {
                backend_type: Backend::FileSystem,
                bucket_prefix: Some("aruna_".to_string()),
                max_bucket_size: Some(100000),
                multipart_bucket: Some("multipart".to_string()),
                root: temp_root.to_string(),
                service_config: HashMap::new(),
                timeouts: Default::default(),
            },
            storage_handle.clone(),
            net_handle.clone(),
            EgressPolicy::loopback(),
        )
        .await
        .unwrap();

        let driver_ctx = DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: Some(net_handle),
            blob_handle: Some(blob_handle),
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let version_id = Ulid::generate();
        let source = VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: PortableSourceDescriptor {
                kind: SourceConnectorKind::Http,
                public_config: HashMap::from([("endpoint".to_string(), endpoint)]),
                source_path: "folder/file.txt".to_string(),
                version_selector: None,
                capabilities: Vec::new(),
                origin_node_id: None,
            },
            connector_id: Some(Ulid::generate()),
        };

        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = storage_handle
            .send_storage_effect(StorageEffect::StartTransaction { read: false })
            .await
        else {
            panic!("Failed to start transaction");
        };
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: VersionKey::new("s3test", "refresh.txt", version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                value: BlobVersion::reference(
                    source,
                    SourceMetadata {
                        content_length: 1,
                        content_type: Some("application/octet-stream".to_string()),
                        etag: Some("stale-etag".to_string()),
                        last_modified: None,
                        source_version: None,
                    },
                    SystemTime::UNIX_EPOCH,
                    Default::default(),
                    SystemTime::UNIX_EPOCH,
                )
                .to_bytes()
                .unwrap()
                .into(),
                txn_id: Some(txn_id),
            })
            .await;
        let _ = storage_handle
            .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
            .await;

        let error = drive(
            GetObjectOperation::new(GetObjectInput {
                bucket: "s3test".to_string(),
                key: "refresh.txt".to_string(),
                version_id: Some(version_id),
                range: None,
                group_id: Ulid::generate(),
                user_identity: UserId::local(Ulid::generate(), RealmId([0u8; 32])),
                node_id: test_node_id(),
            }),
            &driver_ctx,
        )
        .await
        .unwrap_err();

        assert_eq!(error, GetObjectError::HistoricalReferenceUnavailable);
    }
}
