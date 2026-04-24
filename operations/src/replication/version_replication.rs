use crate::replication::error::ReplicationError;
use crate::replication::protocol::{
    LiveReplicationRequest, MaterializedBlobInfo, MultipartObjectReplicationMetadata,
    VersionReplicationManifest, VersionReplicationMessage,
};
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::{
    S3_BUCKET_KEYSPACE, S3_LOOKUP_KEYSPACE, S3_MULTIPART_OBJECT_METADATA_KEYSPACE,
    S3_VERSION_KEYSPACE,
};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::{
    AuthContext, BucketInfo, Location, LookupKey, MultipartObjectMetadataKey, MultipartObjectPart,
    MultipartObjectSummary, ReplicationItemKind, ReplicationNegotiationResult,
    ReplicationSuboperationResult, VersionKey, VersionMetadata,
};
use aruna_core::types::{Effects, Key, NodeId};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

const ITER_PAGE_SIZE: usize = 512;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplicateScopeTarget {
    Bucket,
    Prefix(String),
    Object { key: String },
    Version { key: String, version_id: Ulid },
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReplicateScopeInput {
    pub bucket: String,
    pub target: ReplicateScopeTarget,
    pub target_node_id: NodeId,
    pub auth_context: AuthContext,
    pub replicate_delete_markers: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicateScopeResult {
    pub replicated: u64,
    pub skipped: u64,
    pub failed: u64,
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
    pending_versions: Vec<LiveReplicationRequest>,
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
            pending_versions: Vec::new(),
            result: ReplicateScopeResult {
                replicated: 0,
                skipped: 0,
                failed: 0,
            },
            output: None,
        }
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
        self.state = ReplicateScopeState::Error;
        self.output = Some(Err(err));
        smallvec![]
    }

    fn read_bucket(&mut self) -> Effects {
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
                let lookup = match LookupKey::object(&self.input.bucket, &key).to_bytes() {
                    Ok(lookup) => lookup,
                    Err(err) => return self.fail(err.into()),
                };
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: S3_LOOKUP_KEYSPACE.to_string(),
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
            key_space: S3_VERSION_KEYSPACE.to_string(),
            prefix: Some(prefix.into()),
            start_after: self.next_start_after.clone(),
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
            key_space: S3_VERSION_KEYSPACE.to_string(),
            key: version_key.into(),
            txn_id: None,
        })]
    }

    fn enqueue_version_request(&mut self, version_key: VersionKey) {
        if self.pending_versions.iter().any(|request| {
            request.bucket == version_key.bucket
                && request.key == version_key.key
                && request.version_id == version_key.version_id
        }) {
            return;
        }

        self.pending_versions.push(LiveReplicationRequest {
            bucket: version_key.bucket,
            key: version_key.key,
            version_id: version_key.version_id,
            target_node_id: self.input.target_node_id,
            auth_context: self.input.auth_context.clone(),
        });
    }

    fn should_enqueue_version(&self, metadata: &VersionMetadata) -> bool {
        self.input.replicate_delete_markers || !metadata.is_deleted()
    }

    fn run_next_replication(&mut self) -> Effects {
        let Some(request) = self.pending_versions.pop() else {
            self.state = ReplicateScopeState::Finish;
            self.output = Some(Ok(self.result.clone()));
            return smallvec![];
        };

        self.state = ReplicateScopeState::RunVersionReplication;
        smallvec![Effect::SubOperation(boxed_suboperation(
            ReplicateObjectVersionOperation::new(request),
            |result| Event::SubOperation(SubOperationEvent::ReplicationItemResult {
                result: result
                    .map_err(|err| err.to_string())
                    .and_then(|inner| inner.map_err(|err| err.to_string())),
            }),
        ))]
    }
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
                let _bucket_info = bucket_info;
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
                self.start_iteration(Some(key.clone()), value.is_some())
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
                    let metadata = match VersionMetadata::from_bytes(value.as_ref()) {
                        Ok(metadata) => metadata,
                        Err(err) => return self.fail(err.into()),
                    };
                    if self.should_enqueue_version(&metadata) {
                        self.enqueue_version_request(version_key);
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

                for (key, value) in values {
                    let Ok(version_key) = VersionKey::from_bytes(key.as_ref()) else {
                        continue;
                    };
                    let Ok(metadata) = VersionMetadata::from_bytes(value.as_ref()) else {
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

                    if !self.should_enqueue_version(&metadata) {
                        continue;
                    }

                    self.enqueue_version_request(version_key);
                }

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

                match result {
                    Ok(ReplicationSuboperationResult::Replicated) => self.result.replicated += 1,
                    Ok(ReplicationSuboperationResult::Skipped) => self.result.skipped += 1,
                    Err(_) => self.result.failed += 1,
                }

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
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    ReplicationError(#[from] ReplicationError),
    #[error("Version not found")]
    VersionNotFound,
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
    ReadMultipartSummary,
    ReadMultipartParts,
    ReadCurrentLookup,
    OpenConnection,
    SendManifest,
    AwaitNegotiation,
    TransferBlob,
    AwaitApplyComplete,
    CloseConnection,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct ReplicateObjectVersionOperation {
    request: LiveReplicationRequest,
    state: ReplicateObjectVersionState,
    version_metadata: Option<VersionMetadata>,
    multipart_summary: Option<MultipartObjectSummary>,
    multipart_parts: Vec<MultipartObjectPart>,
    multipart_parts_next_start_after: Option<Key>,
    stream_id: Option<Ulid>,
    manifest: Option<VersionReplicationManifest>,
    blob_replication_id: Option<Ulid>,
    result: Result<ReplicationSuboperationResult, ReplicateObjectVersionError>,
}

impl ReplicateObjectVersionOperation {
    pub fn new(request: LiveReplicationRequest) -> Self {
        Self {
            request,
            state: ReplicateObjectVersionState::Init,
            version_metadata: None,
            multipart_summary: None,
            multipart_parts: Vec::new(),
            multipart_parts_next_start_after: None,
            stream_id: None,
            manifest: None,
            blob_replication_id: None,
            result: Ok(ReplicationSuboperationResult::Replicated),
        }
    }

    fn state_name(&self) -> &'static str {
        match self.state {
            ReplicateObjectVersionState::Init => "Init",
            ReplicateObjectVersionState::ReadVersion => "ReadVersion",
            ReplicateObjectVersionState::ReadMultipartSummary => "ReadMultipartSummary",
            ReplicateObjectVersionState::ReadMultipartParts => "ReadMultipartParts",
            ReplicateObjectVersionState::ReadCurrentLookup => "ReadCurrentLookup",
            ReplicateObjectVersionState::OpenConnection => "OpenConnection",
            ReplicateObjectVersionState::SendManifest => "SendManifest",
            ReplicateObjectVersionState::AwaitNegotiation => "AwaitNegotiation",
            ReplicateObjectVersionState::TransferBlob => "TransferBlob",
            ReplicateObjectVersionState::AwaitApplyComplete => "AwaitApplyComplete",
            ReplicateObjectVersionState::CloseConnection => "CloseConnection",
            ReplicateObjectVersionState::Finish => "Finish",
            ReplicateObjectVersionState::Error => "Error",
        }
    }

    fn fail(&mut self, err: ReplicateObjectVersionError) -> Effects {
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
            key_space: S3_VERSION_KEYSPACE.to_string(),
            key: key.into(),
            txn_id: None,
        })]
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
            start_after: self.multipart_parts_next_start_after.clone(),
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
        let key = match LookupKey::object(&self.request.bucket, &self.request.key).to_bytes() {
            Ok(key) => key,
            Err(err) => return self.fail(err.into()),
        };

        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_LOOKUP_KEYSPACE.to_string(),
            key: key.into(),
            txn_id: None,
        })]
    }

    fn build_manifest(
        &mut self,
        current_lookup: Option<Location>,
    ) -> Result<(), ReplicateObjectVersionError> {
        self.validate_multipart_parts_complete()?;

        let metadata = self
            .version_metadata
            .clone()
            .ok_or(ReplicateObjectVersionError::VersionNotFound)?;
        let current_version = current_lookup == metadata.lookup_location();
        let blob = if let Some(location) = metadata.materialized_location().cloned() {
            let hash = location
                .get_blake3()
                .ok_or(ReplicateObjectVersionError::MissingBlobHash)?
                .try_into()
                .map_err(|_| ReplicateObjectVersionError::MissingBlobHash)?;
            Some(MaterializedBlobInfo {
                hash,
                size: location.blob_size,
                compressed: location.compressed,
                encrypted: location.encrypted,
                location,
            })
        } else {
            None
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
            bucket: self.request.bucket.clone(),
            key: self.request.key.clone(),
            version_id: self.request.version_id,
            kind: if blob.is_some() {
                ReplicationItemKind::Materialized
            } else {
                ReplicationItemKind::DeleteMarker
            },
            created_at: metadata.created_at,
            created_by: metadata.created_by,
            current_version,
            auth_context: self.request.auth_context.clone(),
            blob,
            multipart,
        });
        Ok(())
    }

    fn send_manifest(&mut self) -> Effects {
        self.state = ReplicateObjectVersionState::SendManifest;
        let Some(stream_id) = self.stream_id else {
            return self.fail(ReplicationError::ConnectionMissing.into());
        };
        let payload = match VersionReplicationMessage::VersionManifest(
            self.manifest.clone().expect("manifest available"),
        )
        .to_bytes()
        {
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
                let metadata = match VersionMetadata::from_bytes(value.as_ref()) {
                    Ok(metadata) => metadata,
                    Err(err) => return self.fail(err.into()),
                };
                let materialized = metadata.is_materialized();
                self.version_metadata = Some(metadata);
                if materialized {
                    self.read_multipart_summary()
                } else {
                    self.read_current_lookup()
                }
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
                    .and_then(|value| Location::from_bytes(value.as_ref()).ok());
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
                        self.result = Ok(ReplicationSuboperationResult::Skipped);
                        self.close_connection()
                    }
                    ReplicationNegotiationResult::NeedVersionOnly => self.await_apply_complete(),
                    ReplicationNegotiationResult::NeedBlobAndVersion => {
                        let Some(blob) = self
                            .manifest
                            .as_ref()
                            .and_then(|manifest| manifest.blob.as_ref())
                        else {
                            return self.fail(ReplicateObjectVersionError::MissingBlobHash);
                        };
                        self.state = ReplicateObjectVersionState::TransferBlob;
                        let replication_id = Ulid::new();
                        self.blob_replication_id = Some(replication_id);
                        smallvec![Effect::Blob(BlobEffect::Replicate {
                            replication_id,
                            stream_id: self.stream_id.expect("stream id available"),
                            location: blob.location.clone(),
                            keep_alive: true,
                        })]
                    }
                    ReplicationNegotiationResult::Rejected(reason) => {
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
                        self.result = Ok(ReplicationSuboperationResult::Replicated);
                        self.close_connection()
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
        self.stream_id
            .map_or_else(smallvec::SmallVec::new, |stream_id| {
                smallvec![Effect::Blob(BlobEffect::CloseConnection { stream_id })]
            })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ReplicateObjectVersionError, ReplicateObjectVersionOperation, ReplicateScopeInput,
        ReplicateScopeOperation, ReplicateScopeTarget,
    };
    use crate::replication::protocol::LiveReplicationRequest;
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::operation::Operation;
    use aruna_core::structs::{
        AuthContext, BackendLocation, BucketInfo, Location, MultipartChecksumType,
        MultipartObjectMetadataKey, MultipartObjectPart, MultipartObjectSummary, RealmId,
        VersionKey, VersionMetadata,
    };
    use std::collections::HashMap;
    use std::time::SystemTime;
    use ulid::Ulid;

    fn bucket_info() -> BucketInfo {
        BucketInfo {
            group_id: Ulid::new(),
            created_at: SystemTime::now(),
            created_by: Ulid::new(),
        }
    }

    fn auth_context() -> AuthContext {
        AuthContext {
            user_id: Ulid::new(),
            realm_id: RealmId::from_bytes([7u8; 32]),
            path_restrictions: None,
        }
    }

    fn scope_input(target: ReplicateScopeTarget) -> ReplicateScopeInput {
        ReplicateScopeInput {
            bucket: "bucket".to_string(),
            target,
            target_node_id: iroh::SecretKey::generate(&mut rand::rng()).public(),
            auth_context: auth_context(),
            replicate_delete_markers: true,
        }
    }

    fn version_entry(
        key: &str,
        version_id: Ulid,
    ) -> (aruna_core::types::Key, aruna_core::types::Value) {
        let key_bytes = VersionKey::new("bucket", key, version_id)
            .to_bytes()
            .unwrap();
        let value_bytes = VersionMetadata::deleted(version_id, SystemTime::now(), Ulid::new())
            .to_bytes()
            .unwrap();
        (key_bytes.into(), value_bytes.into())
    }

    fn materialized_location() -> BackendLocation {
        let mut hashes = HashMap::new();
        hashes.insert("blake3".to_string(), vec![1u8; 32]);
        BackendLocation {
            root: "/tmp".to_string(),
            storage_bucket: "blob-bucket".to_string(),
            backend_path: format!("bucket/key_{}", Ulid::new()),
            ulid: Ulid::new(),
            compressed: false,
            encrypted: false,
            created_by: Ulid::new(),
            created_at: SystemTime::now(),
            staging: false,
            partial: false,
            blob_size: 42,
            hashes,
        }
    }

    fn version_request(version_id: Ulid) -> LiveReplicationRequest {
        LiveReplicationRequest {
            bucket: "bucket".to_string(),
            key: "dir/file.txt".to_string(),
            version_id,
            target_node_id: iroh::SecretKey::generate(&mut rand::rng()).public(),
            auth_context: auth_context(),
        }
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

        let matching_version = Ulid::new();
        let effects = op.step(Event::Storage(StorageEvent::IterResult {
            values: vec![
                version_entry("dir/file.txt", matching_version),
                version_entry("dir/file.txt.bak", Ulid::new()),
                version_entry("dir/sub/file.txt", Ulid::new()),
            ],
            next_start_after: None,
        }));

        let Effect::SubOperation(_) = &effects[0] else {
            panic!("expected only matching object version to be enqueued")
        };
        assert_eq!(op.pending_versions.len(), 0);
    }

    #[test]
    fn object_miss_falls_back_to_prefix_iteration() {
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
            panic!("expected prefix iteration after object miss")
        };

        let effects = op.step(Event::Storage(StorageEvent::IterResult {
            values: vec![
                version_entry("dir/file-a", Ulid::new()),
                version_entry("dir/file/b", Ulid::new()),
                version_entry("dir/other", Ulid::new()),
            ],
            next_start_after: None,
        }));

        let Effect::SubOperation(_) = &effects[0] else {
            panic!("expected matching prefix version to be enqueued")
        };
        assert_eq!(op.pending_versions.len(), 1);
    }

    #[test]
    fn multipart_metadata_paginates_across_multiple_iter_pages() {
        let version_id = Ulid::new();
        let mut op = ReplicateObjectVersionOperation::new(version_request(version_id));

        let effects = op.start();
        assert!(matches!(
            effects[0],
            Effect::Storage(StorageEffect::Read { .. })
        ));

        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![1u8].into(),
            value: Some(
                VersionMetadata::materialized(
                    version_id,
                    materialized_location(),
                    SystemTime::now(),
                    Ulid::new(),
                    None,
                )
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
            value: Some(
                MultipartObjectSummary {
                    checksum_type: MultipartChecksumType::Composite,
                    part_count: 3,
                }
                .to_bytes()
                .unwrap()
                .into(),
            ),
        }));
        let Effect::Storage(StorageEffect::Iter { start_after, .. }) = &effects[0] else {
            panic!("expected multipart iter request")
        };
        assert!(start_after.is_none());

        let next_cursor: aruna_core::types::Key = vec![9u8].into();
        let effects = op.step(Event::Storage(StorageEvent::IterResult {
            values: vec![
                multipart_part_entry(version_id, 2),
                multipart_part_entry(version_id, 1),
            ],
            next_start_after: Some(next_cursor.clone()),
        }));
        let Effect::Storage(StorageEffect::Iter { start_after, .. }) = &effects[0] else {
            panic!("expected paginated multipart iter request")
        };
        assert_eq!(start_after.as_ref(), Some(&next_cursor));

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
        let version_id = Ulid::new();
        let mut op = ReplicateObjectVersionOperation::new(version_request(version_id));

        op.start();
        op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![1u8].into(),
            value: Some(
                VersionMetadata::materialized(
                    version_id,
                    materialized_location(),
                    SystemTime::now(),
                    Ulid::new(),
                    None,
                )
                .to_bytes()
                .unwrap()
                .into(),
            ),
        }));
        op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![2u8].into(),
            value: Some(
                MultipartObjectSummary {
                    checksum_type: MultipartChecksumType::Composite,
                    part_count: 2,
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
}
