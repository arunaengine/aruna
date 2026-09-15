use crate::s3::listing::PrefixTracker;
use crate::s3::object::versions::served_copy;
use aruna_core::NodeId;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::prefix_upper_bound;
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, MANAGED_COPY_KEYSPACE,
    NODE_SUBJECT_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::source_access::SourceMetadata;
use aruna_core::structs::execution::source_connector::SourceConnectorKind;
use aruna_core::structs::placement::node_subject::{NODE_SUBJECT_KEY, NodeSubjectRecord};
use aruna_core::structs::placement::placement_policy::PlacementPolicyRef;
use aruna_core::structs::storage::blob::{
    BackendLocation, BlobHeadKey, BlobLocationKey, BlobVersion, BlobVersionState,
    CurrentVersionPointer, ManagedCopyKey, VersionKey,
};
use aruna_core::types::{Effects, GroupId, Key, Value};
use serde::{Deserialize, Serialize};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ListBucketState {
    Init,
    StartTransaction,
    ReadHeads,
    ReadVersions,
    ReadBlobLocations,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum ListBucketError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("State [{state:?}] invalid: expected [{expected:?}] - received [{received:?}]")]
    InvalidStateEvent {
        state: ListBucketState,
        expected: &'static str,
        received: Event,
    },
    #[error("No transaction found")]
    NoTransactionFound,
    #[error("ListObjectsV2 failed")]
    ListObjectsFailed,
    #[error("operation did not finish")]
    NotFinished,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ListBucketInput {
    pub bucket: String,
    pub group_id: GroupId,
    pub continuation_token: Option<ListContinuationToken>,
    pub max_keys: Option<usize>,
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    pub start_after: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ListContinuationToken {
    pub last_key: Vec<u8>,
    pub last_common_prefix: Option<String>,
}

impl ListContinuationToken {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ListedObject {
    pub head: BlobHeadKey,
    pub location: Option<BackendLocation>,
    pub source_metadata: Option<SourceMetadata>,
    pub referenced: bool,
    pub kind: Option<SourceConnectorKind>,
    pub source_path: Option<String>,
    pub connector_id: Option<Ulid>,
    pub origin_node_id: Option<NodeId>,
    pub last_refresh: Option<std::time::SystemTime>,
    pub version_created_at: Option<std::time::SystemTime>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ListBucketResult {
    pub objects: Vec<ListedObject>,
    pub common_prefixes: Vec<String>,
    pub continuation_token: Option<ListContinuationToken>,
}

#[derive(Debug, PartialEq)]
enum ResolvedEntry {
    Object(ListedObject),
    AwaitingLocation {
        head: BlobHeadKey,
        version_created_at: std::time::SystemTime,
        /// Registration this node must hold before the governed location may be
        /// described. `None` leaves an ungoverned head unchanged.
        governed: Option<(ManagedCopyKey, Vec<PlacementPolicyRef>)>,
    },
}

#[derive(Debug, PartialEq)]
pub struct ListBucketOperation {
    input: ListBucketInput,
    state: ListBucketState,
    txn_id: Option<Ulid>,
    round_candidates: Vec<(BlobHeadKey, Ulid, Vec<u8>)>,
    resolved: Vec<ResolvedEntry>,
    location_reads: Vec<(String, Key)>,
    objects: Vec<ListedObject>,
    prefixes: PrefixTracker,
    continuation_token: Option<ListContinuationToken>,
    scan_prefix: Vec<u8>,
    scan_limit: usize,
    scan_rounds: usize,
    round_exhausted: bool,
    cursor_group: Option<String>,
    cursor_group_prefix: Option<Vec<u8>>,
    last_consumed_key: Option<Vec<u8>>,
    output: Option<Result<ListBucketResult, ListBucketError>>,
}

impl ListBucketOperation {
    pub const DEFAULT_MAX_KEYS: usize = 1_000;
    const MAX_SCAN_ROUNDS: usize = 100;

    pub fn new(input: ListBucketInput) -> Self {
        Self {
            input,
            state: ListBucketState::Init,
            txn_id: None,
            round_candidates: Vec::new(),
            resolved: Vec::new(),
            location_reads: Vec::new(),
            objects: Vec::new(),
            prefixes: PrefixTracker::default(),
            continuation_token: None,
            scan_prefix: Vec::new(),
            scan_limit: 0,
            scan_rounds: 0,
            round_exhausted: false,
            cursor_group: None,
            cursor_group_prefix: None,
            last_consumed_key: None,
            output: None,
        }
    }

    fn emit_error(&mut self, error: ListBucketError) -> Effects {
        self.state = ListBucketState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn max_keys(&self) -> usize {
        self.input.max_keys.unwrap_or(Self::DEFAULT_MAX_KEYS)
    }

    /// Number of result slots already committed on this page: one per emitted
    /// common prefix plus one per resolved object. Delete-markered keys never
    /// reach `resolved`, so they do not consume a slot.
    fn emitted(&self) -> usize {
        self.prefixes.count() + self.resolved.len()
    }

    /// Record that the scan cursor now sits inside `group`, so the next round
    /// can seek past the whole group instead of re-reading its keys.
    fn enter_cursor_group(&mut self, group: &str, key_bytes: &[u8]) -> Result<(), ListBucketError> {
        let group_prefix = BlobHeadKey::object_prefix(&self.input.bucket, group)?;
        self.cursor_group = Some(group.to_string());
        self.cursor_group_prefix = Some(group_prefix);
        self.last_consumed_key = Some(key_bytes.to_vec());
        Ok(())
    }

    fn handle_init(&mut self) -> Effects {
        if self.max_keys() == 0 {
            self.state = ListBucketState::Finish;
            self.output = Some(Ok(ListBucketResult {
                objects: Vec::new(),
                common_prefixes: Vec::new(),
                continuation_token: None,
            }));
            return smallvec![];
        }

        self.state = ListBucketState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: true
        })]
    }

    fn handle_transaction_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.emit_error(ListBucketError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionStarted)",
                received: event,
            });
        };

        self.txn_id = Some(txn_id);
        let prefix = match self
            .input
            .prefix
            .as_ref()
            .filter(|prefix| !prefix.is_empty())
        {
            Some(key_prefix) => BlobHeadKey::object_prefix(&self.input.bucket, key_prefix),
            None => BlobHeadKey::bucket_prefix(&self.input.bucket),
        };
        let prefix = match prefix {
            Ok(prefix) => prefix,
            Err(err) => return self.emit_error(err.into()),
        };
        let iter_start_key = if let Some(token) = self.input.continuation_token.clone() {
            if let Err(error) = self.resume_scan_state(&token) {
                return self.emit_error(error);
            }
            Some(token.last_key)
        } else {
            match self
                .input
                .start_after
                .as_deref()
                .filter(|start_after| !start_after.is_empty())
            {
                Some(start_after) => {
                    match BlobHeadKey::object_prefix(&self.input.bucket, start_after) {
                        Ok(key) => Some(key),
                        Err(err) => return self.emit_error(err.into()),
                    }
                }
                None => None,
            }
        };
        if let Some(start) = iter_start_key.as_ref()
            && start.as_slice() > prefix.as_slice()
            && !start.starts_with(&prefix)
        {
            return self.finish_scan();
        }

        self.scan_prefix = prefix;
        self.last_consumed_key = iter_start_key;
        self.issue_scan_round()
    }

    /// Restore the group cursor from a continuation token so the first scan
    /// round can seek past a fully emitted common-prefix group instead of
    /// re-reading its keys one round at a time.
    fn resume_scan_state(&mut self, token: &ListContinuationToken) -> Result<(), ListBucketError> {
        self.prefixes.set_resume(token.last_common_prefix.clone());
        let Some(group) = token.last_common_prefix.as_deref() else {
            return Ok(());
        };
        let Ok(head) = BlobHeadKey::from_bytes(&token.last_key) else {
            return Ok(());
        };
        if head.bucket != self.input.bucket
            || self.common_prefix_of(&head.key).as_deref() != Some(group)
        {
            return Ok(());
        }

        let group_prefix = BlobHeadKey::object_prefix(&self.input.bucket, group)?;
        self.cursor_group = Some(group.to_string());
        self.cursor_group_prefix = Some(group_prefix);
        Ok(())
    }

    fn issue_scan_round(&mut self) -> Effects {
        let visible = self.emitted();
        self.scan_limit = self.max_keys().saturating_sub(visible).saturating_add(1);
        self.scan_rounds += 1;

        // While the cursor sits inside an emitted group, seek inclusively to
        // the first key past the group instead of resuming behind the cursor.
        let start = match self
            .cursor_group_prefix
            .as_deref()
            .and_then(prefix_upper_bound)
        {
            Some(seek) => Some(IterStart::At(seek.into())),
            None => self
                .last_consumed_key
                .clone()
                .map(|key| IterStart::After(key.into())),
        };

        self.state = ListBucketState::ReadHeads;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: BLOB_HEAD_KEYSPACE.to_string(),
            prefix: Some(self.scan_prefix.clone().into()),
            start,
            limit: self.scan_limit,
            txn_id: self.txn_id,
        })]
    }

    fn truncate_scan(&mut self) -> Effects {
        self.continuation_token =
            self.last_consumed_key
                .clone()
                .map(|last_key| ListContinuationToken {
                    last_key,
                    last_common_prefix: self.cursor_group.clone(),
                });
        self.finish_scan()
    }

    fn finish_scan(&mut self) -> Effects {
        if self.location_reads.is_empty() {
            return self.finish_hydration(Vec::new());
        }

        let mut reads = std::mem::take(&mut self.location_reads);
        // The subject leads the batch so hydration can decide every governed
        // entry as it walks the answers in order.
        if self.pending_subject() {
            reads.insert(
                0,
                (
                    NODE_SUBJECT_KEYSPACE.to_string(),
                    Key::from(NODE_SUBJECT_KEY.to_vec()),
                ),
            );
        }
        self.state = ListBucketState::ReadBlobLocations;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: self.txn_id,
        })]
    }

    /// Whether this page carries a governed entry, so the batch leads with the
    /// local subject row.
    fn pending_subject(&self) -> bool {
        self.resolved.iter().any(|entry| {
            matches!(
                entry,
                ResolvedEntry::AwaitingLocation {
                    governed: Some(_),
                    ..
                }
            )
        })
    }

    fn commit(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(ListBucketError::NoTransactionFound);
        };

        self.state = ListBucketState::CommitTransaction;
        self.output = Some(Ok(ListBucketResult {
            objects: std::mem::take(&mut self.objects),
            common_prefixes: self.prefixes.take(),
            continuation_token: self.continuation_token.clone(),
        }));
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn common_prefix_of(&self, key: &str) -> Option<String> {
        crate::s3::listing::common_prefix_of(
            key,
            self.input.prefix.as_deref(),
            self.input.delimiter.as_deref(),
        )
    }

    fn handle_heads_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult { values, .. }) = event else {
            return self.emit_error(ListBucketError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::IterResult)",
                received: event,
            });
        };

        let round_len = values.len();
        self.round_exhausted = round_len < self.scan_limit;

        // Each candidate head's current version decides delete-marker status,
        // Contents membership and prefix roll-up; keys in emitted groups skip.
        let mut candidates: Vec<(BlobHeadKey, Ulid, Vec<u8>)> = Vec::new();
        for (key, value) in values.into_iter() {
            if let Some(group_prefix) = self.cursor_group_prefix.as_deref()
                && key.as_ref().starts_with(group_prefix)
            {
                self.last_consumed_key = Some(key.to_vec());
                continue;
            }

            let head = match BlobHeadKey::from_bytes(key.as_ref()) {
                Ok(head) => head,
                Err(err) => return self.emit_error(err.into()),
            };
            let pointer = match CurrentVersionPointer::from_bytes(value.as_ref()) {
                Ok(pointer) => pointer,
                Err(err) => return self.emit_error(err.into()),
            };
            candidates.push((head, pointer.version_id, key.to_vec()));
        }

        if candidates.is_empty() {
            if self.round_exhausted {
                return self.finish_scan();
            }
            if self.scan_rounds >= Self::MAX_SCAN_ROUNDS {
                return self.truncate_scan();
            }
            return self.issue_scan_round();
        }

        let reads = candidates
            .iter()
            .map(|(head, version_id, _)| {
                VersionKey::new(&head.bucket, &head.key, *version_id)
                    .to_bytes()
                    .map(|key| (BLOB_VERSIONS_KEYSPACE.to_string(), key.into()))
            })
            .collect::<Result<Vec<_>, _>>();
        let reads = match reads {
            Ok(reads) => reads,
            Err(err) => return self.emit_error(err.into()),
        };

        self.round_candidates = candidates;
        self.state = ListBucketState::ReadVersions;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: self.txn_id,
        })]
    }

    fn round_versions_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.emit_error(ListBucketError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::BatchReadResult)",
                received: event,
            });
        };

        let candidates = std::mem::take(&mut self.round_candidates);
        if values.len() != candidates.len() {
            return self.emit_error(ListBucketError::ListObjectsFailed);
        }

        let max_keys = self.max_keys();
        for ((head, version_id, key_bytes), (_key, value)) in candidates.into_iter().zip(values) {
            let version = match value {
                Some(value) => match BlobVersion::from_bytes(value.as_ref()) {
                    Ok(version) => Some(version),
                    Err(err) => return self.emit_error(err.into()),
                },
                None => None,
            };
            // A missing version is an inconsistency; treat it like a delete
            // marker so it hides the key from Contents and prefix roll-up.
            let live = version
                .as_ref()
                .map(|version| !version.is_deleted())
                .unwrap_or(false);

            match self.common_prefix_of(&head.key) {
                Some(group) => {
                    let already_emitted = self.prefixes.already_emitted(&group);
                    if already_emitted {
                        // The group is already represented; advance past this key.
                        self.prefixes.set_resume(None);
                        if let Err(err) = self.enter_cursor_group(&group, &key_bytes) {
                            return self.emit_error(err);
                        }
                    } else if live {
                        if self.emitted() >= max_keys {
                            return self.truncate_scan();
                        }
                        self.prefixes.set_resume(None);
                        self.prefixes.push(group.clone());
                        if let Err(err) = self.enter_cursor_group(&group, &key_bytes) {
                            return self.emit_error(err);
                        }
                    } else {
                        // No live sibling yet: emit nothing and do not seek
                        // past, so a later live key can surface the prefix.
                        self.cursor_group = None;
                        self.cursor_group_prefix = None;
                        self.last_consumed_key = Some(key_bytes);
                    }
                }
                None => {
                    if !live {
                        // Hidden from Contents; advance past it since the
                        // preceding group is fully emitted.
                        self.cursor_group = None;
                        self.cursor_group_prefix = None;
                        self.last_consumed_key = Some(key_bytes);
                        continue;
                    }
                    // Truncate before clearing the cursor so the token still
                    // records the finished group for the next page.
                    if self.emitted() >= max_keys {
                        return self.truncate_scan();
                    }
                    self.prefixes.set_resume(None);
                    self.cursor_group = None;
                    self.cursor_group_prefix = None;
                    self.last_consumed_key = Some(key_bytes);
                    let Some(version) = version else {
                        continue;
                    };
                    match version.state {
                        BlobVersionState::Deleted => {}
                        BlobVersionState::Reference {
                            source,
                            cached_metadata,
                            last_refresh,
                            ..
                        } => {
                            let descriptor = source.descriptor;
                            self.resolved.push(ResolvedEntry::Object(ListedObject {
                                head,
                                location: None,
                                source_metadata: Some(cached_metadata),
                                referenced: true,
                                kind: Some(descriptor.kind),
                                source_path: Some(descriptor.source_path),
                                connector_id: source.connector_id,
                                origin_node_id: descriptor.origin_node_id,
                                last_refresh: Some(last_refresh),
                                version_created_at: None,
                            }));
                        }
                        BlobVersionState::Materialized {
                            blob_hash, backend, ..
                        } => {
                            self.location_reads.push((
                                BLOB_LOCATIONS_KEYSPACE.to_string(),
                                BlobLocationKey::new(blob_hash, backend.clone())
                                    .to_bytes()
                                    .into(),
                            ));
                            let governed = match version.placement_policies.is_empty() {
                                true => None,
                                false => {
                                    let copy_key = ManagedCopyKey::new(
                                        VersionKey::new(&head.bucket, &head.key, version_id),
                                        backend,
                                    );
                                    let bytes = match copy_key.to_bytes() {
                                        Ok(bytes) => bytes,
                                        Err(error) => return self.emit_error(error.into()),
                                    };
                                    self.location_reads
                                        .push((MANAGED_COPY_KEYSPACE.to_string(), bytes.into()));
                                    Some((copy_key, version.placement_policies.clone()))
                                }
                            };
                            self.resolved.push(ResolvedEntry::AwaitingLocation {
                                head,
                                version_created_at: version.created_at,
                                governed,
                            });
                        }
                    }
                }
            }
        }

        if self.round_exhausted {
            return self.finish_scan();
        }
        if self.scan_rounds >= Self::MAX_SCAN_ROUNDS {
            return self.truncate_scan();
        }

        self.issue_scan_round()
    }

    fn handle_locations_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.emit_error(ListBucketError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::BatchReadResult)",
                received: event,
            });
        };

        self.finish_hydration(values)
    }

    fn finish_hydration(&mut self, locations: Vec<(Key, Option<Value>)>) -> Effects {
        let mut locations = locations.into_iter();
        let subject = match self.pending_subject() {
            true => locations
                .next()
                .and_then(|(_, value)| value)
                .and_then(|value| NodeSubjectRecord::from_bytes(value.as_ref()).ok()),
            false => None,
        };
        for entry in std::mem::take(&mut self.resolved) {
            match entry {
                ResolvedEntry::Object(object) => self.objects.push(object),
                ResolvedEntry::AwaitingLocation {
                    head,
                    version_created_at,
                    governed,
                } => {
                    let Some((_key, value)) = locations.next() else {
                        return self.emit_error(ListBucketError::ListObjectsFailed);
                    };
                    let registration = match governed.as_ref() {
                        Some(_) => match locations.next() {
                            Some((_, value)) => value,
                            None => {
                                return self.emit_error(ListBucketError::ListObjectsFailed);
                            }
                        },
                        None => None,
                    };
                    // Objects without a stored backend location stay hidden.
                    let Some(value) = value else {
                        continue;
                    };
                    let location = match BackendLocation::from_bytes(value.as_ref()) {
                        Ok(location) => location,
                        Err(err) => return self.emit_error(err.into()),
                    };
                    // A governed head this node cannot answer for is still
                    // listed, but without describing bytes it may not serve.
                    let described = governed.is_none_or(|(copy_key, refs)| {
                        served_copy(registration.as_deref(), &copy_key, &refs, subject.as_ref())
                    });
                    self.objects.push(ListedObject {
                        head,
                        location: described.then_some(location),
                        source_metadata: None,
                        referenced: false,
                        kind: None,
                        source_path: None,
                        connector_id: None,
                        origin_node_id: None,
                        last_refresh: None,
                        version_created_at: Some(version_created_at),
                    });
                }
            }
        }

        self.commit()
    }

    fn handle_transaction_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.emit_error(ListBucketError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionCommitted)",
                received: event,
            });
        };

        self.state = ListBucketState::Finish;
        smallvec![]
    }
}

impl Operation for ListBucketOperation {
    type Output = ListBucketResult;
    type Error = ListBucketError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.emit_error(ListBucketError::StorageError(error));
        }

        match self.state {
            ListBucketState::Init => self.handle_init(),
            ListBucketState::StartTransaction => self.handle_transaction_started(event),
            ListBucketState::ReadHeads => self.handle_heads_read(event),
            ListBucketState::ReadVersions => self.round_versions_read(event),
            ListBucketState::ReadBlobLocations => self.handle_locations_read(event),
            ListBucketState::CommitTransaction => self.handle_transaction_committed(event),
            ListBucketState::Finish | ListBucketState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, ListBucketState::Finish | ListBucketState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.output {
            Some(Ok(value)) => Ok(value),
            Some(Err(error)) => Err(error),
            None => Err(ListBucketError::NotFinished),
        }
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

#[cfg(test)]
#[path = "list_tests.rs"]
mod test;
