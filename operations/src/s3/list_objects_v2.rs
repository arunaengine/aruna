use aruna_core::NodeId;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::{
    BackendLocation, BlobHeadKey, BlobVersion, BlobVersionState, CurrentVersionPointer,
    SourceConnectorKind, SourceMetadata, VersionKey,
};
use aruna_core::types::{Effects, GroupId, Key, Value};
use aruna_core::util::prefix_upper_bound;
use serde::{Deserialize, Serialize};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ListObjectsV2State {
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
pub enum ListObjectsV2Error {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("State [{state:?}] invalid: expected [{expected:?}] - received [{received:?}]")]
    InvalidStateEvent {
        state: ListObjectsV2State,
        expected: &'static str,
        received: Event,
    },
    #[error("No transaction found")]
    NoTransactionFound,
    #[error("ListObjectsV2 failed")]
    ListObjectsV2Failed,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ListObjectsV2Input {
    pub bucket: String,
    pub group_id: GroupId,
    pub continuation_token: Option<ListObjectsV2ContinuationToken>,
    pub max_keys: Option<usize>,
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    pub start_after: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ListObjectsV2ContinuationToken {
    pub last_key: Vec<u8>,
    pub last_common_prefix: Option<String>,
}

impl ListObjectsV2ContinuationToken {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ListObjectsV2Object {
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
pub struct ListObjectsV2Result {
    pub objects: Vec<ListObjectsV2Object>,
    pub common_prefixes: Vec<String>,
    pub continuation_token: Option<ListObjectsV2ContinuationToken>,
}

#[derive(Debug, PartialEq)]
enum ResolvedEntry {
    Object(ListObjectsV2Object),
    AwaitingLocation {
        head: BlobHeadKey,
        version_created_at: std::time::SystemTime,
    },
}

#[derive(Debug, PartialEq)]
pub struct ListObjectsV2Operation {
    input: ListObjectsV2Input,
    state: ListObjectsV2State,
    txn_id: Option<Ulid>,
    round_candidates: Vec<(BlobHeadKey, Ulid, Vec<u8>)>,
    resolved: Vec<ResolvedEntry>,
    location_reads: Vec<(String, Key)>,
    objects: Vec<ListObjectsV2Object>,
    common_prefixes: Vec<String>,
    continuation_token: Option<ListObjectsV2ContinuationToken>,
    scan_prefix: Vec<u8>,
    scan_limit: usize,
    scan_rounds: usize,
    round_exhausted: bool,
    resume_common_prefix: Option<String>,
    cursor_group: Option<String>,
    cursor_group_prefix: Option<Vec<u8>>,
    last_consumed_key: Option<Vec<u8>>,
    output: Option<Result<ListObjectsV2Result, ListObjectsV2Error>>,
}

impl ListObjectsV2Operation {
    pub const DEFAULT_MAX_KEYS: usize = 1_000;
    const MAX_SCAN_ROUNDS: usize = 100;

    pub fn new(input: ListObjectsV2Input) -> Self {
        Self {
            input,
            state: ListObjectsV2State::Init,
            txn_id: None,
            round_candidates: Vec::new(),
            resolved: Vec::new(),
            location_reads: Vec::new(),
            objects: Vec::new(),
            common_prefixes: Vec::new(),
            continuation_token: None,
            scan_prefix: Vec::new(),
            scan_limit: 0,
            scan_rounds: 0,
            round_exhausted: false,
            resume_common_prefix: None,
            cursor_group: None,
            cursor_group_prefix: None,
            last_consumed_key: None,
            output: None,
        }
    }

    fn emit_error(&mut self, error: ListObjectsV2Error) -> Effects {
        self.state = ListObjectsV2State::Error;
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
        self.common_prefixes.len() + self.resolved.len()
    }

    /// Record that the scan cursor now sits inside `group`, so the next round
    /// can seek past the whole group instead of re-reading its keys.
    fn enter_cursor_group(
        &mut self,
        group: &str,
        key_bytes: &[u8],
    ) -> Result<(), ListObjectsV2Error> {
        let group_prefix = BlobHeadKey::object_prefix(&self.input.bucket, group)?;
        self.cursor_group = Some(group.to_string());
        self.cursor_group_prefix = Some(group_prefix);
        self.last_consumed_key = Some(key_bytes.to_vec());
        Ok(())
    }

    fn handle_init(&mut self) -> Effects {
        if self.max_keys() == 0 {
            self.state = ListObjectsV2State::Finish;
            self.output = Some(Ok(ListObjectsV2Result {
                objects: Vec::new(),
                common_prefixes: Vec::new(),
                continuation_token: None,
            }));
            return smallvec![];
        }

        self.state = ListObjectsV2State::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: true
        })]
    }

    fn handle_transaction_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.emit_error(ListObjectsV2Error::InvalidStateEvent {
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
    fn resume_scan_state(
        &mut self,
        token: &ListObjectsV2ContinuationToken,
    ) -> Result<(), ListObjectsV2Error> {
        self.resume_common_prefix = token.last_common_prefix.clone();
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

        self.state = ListObjectsV2State::ReadHeads;
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
                .map(|last_key| ListObjectsV2ContinuationToken {
                    last_key,
                    last_common_prefix: self.cursor_group.clone(),
                });
        self.finish_scan()
    }

    fn finish_scan(&mut self) -> Effects {
        if self.location_reads.is_empty() {
            return self.finish_hydration(Vec::new());
        }

        let reads = std::mem::take(&mut self.location_reads);
        self.state = ListObjectsV2State::ReadBlobLocations;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: self.txn_id,
        })]
    }

    fn commit(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(ListObjectsV2Error::NoTransactionFound);
        };

        self.state = ListObjectsV2State::CommitTransaction;
        self.output = Some(Ok(ListObjectsV2Result {
            objects: std::mem::take(&mut self.objects),
            common_prefixes: std::mem::take(&mut self.common_prefixes),
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
            return self.emit_error(ListObjectsV2Error::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::IterResult)",
                received: event,
            });
        };

        let round_len = values.len();
        self.round_exhausted = round_len < self.scan_limit;

        // Collect the candidate heads for this round. Their current version has
        // to be read before we know whether the latest version is a delete
        // marker, which decides both Contents membership and common-prefix
        // roll-up. Keys inside an already-emitted group are skipped here: the
        // group is represented, only the cursor has to advance past them.
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
        self.state = ListObjectsV2State::ReadVersions;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: self.txn_id,
        })]
    }

    fn handle_round_versions_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.emit_error(ListObjectsV2Error::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::BatchReadResult)",
                received: event,
            });
        };

        let candidates = std::mem::take(&mut self.round_candidates);
        if values.len() != candidates.len() {
            return self.emit_error(ListObjectsV2Error::ListObjectsV2Failed);
        }

        let max_keys = self.max_keys();
        for ((head, _version_id, key_bytes), (_key, value)) in candidates.into_iter().zip(values) {
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
                    let already_emitted = self.resume_common_prefix.as_deref()
                        == Some(group.as_str())
                        || self.common_prefixes.last().map(String::as_str) == Some(group.as_str());
                    if already_emitted {
                        // The group is already represented; advance past this key.
                        self.resume_common_prefix = None;
                        if let Err(err) = self.enter_cursor_group(&group, &key_bytes) {
                            return self.emit_error(err);
                        }
                    } else if live {
                        if self.emitted() >= max_keys {
                            return self.truncate_scan();
                        }
                        self.resume_common_prefix = None;
                        self.common_prefixes.push(group.clone());
                        if let Err(err) = self.enter_cursor_group(&group, &key_bytes) {
                            return self.emit_error(err);
                        }
                    } else {
                        // Delete-markered key in a group with no live key yet:
                        // do not emit and do not seek past, so a later live
                        // sibling can still surface the prefix.
                        self.cursor_group = None;
                        self.cursor_group_prefix = None;
                        self.last_consumed_key = Some(key_bytes);
                    }
                }
                None => {
                    if !live {
                        // Delete-markered key hidden from Contents; advance past
                        // it. Any preceding group is fully emitted, so the
                        // cursor no longer sits inside one.
                        self.cursor_group = None;
                        self.cursor_group_prefix = None;
                        self.last_consumed_key = Some(key_bytes);
                        continue;
                    }
                    // Truncate before clearing the cursor so the token still
                    // records the group just finished, letting the next page
                    // seek past it instead of re-emitting the prefix.
                    if self.emitted() >= max_keys {
                        return self.truncate_scan();
                    }
                    self.resume_common_prefix = None;
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
                        } => {
                            let descriptor = source.descriptor;
                            self.resolved
                                .push(ResolvedEntry::Object(ListObjectsV2Object {
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
                        BlobVersionState::Materialized { blob_hash, .. } => {
                            self.location_reads.push((
                                BLOB_LOCATIONS_KEYSPACE.to_string(),
                                blob_hash.to_vec().into(),
                            ));
                            self.resolved.push(ResolvedEntry::AwaitingLocation {
                                head,
                                version_created_at: version.created_at,
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
            return self.emit_error(ListObjectsV2Error::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::BatchReadResult)",
                received: event,
            });
        };

        let awaiting = self
            .resolved
            .iter()
            .filter(|entry| matches!(entry, ResolvedEntry::AwaitingLocation { .. }))
            .count();
        if values.len() != awaiting {
            return self.emit_error(ListObjectsV2Error::ListObjectsV2Failed);
        }

        self.finish_hydration(values)
    }

    fn finish_hydration(&mut self, locations: Vec<(Key, Option<Value>)>) -> Effects {
        let mut locations = locations.into_iter();
        for entry in std::mem::take(&mut self.resolved) {
            match entry {
                ResolvedEntry::Object(object) => self.objects.push(object),
                ResolvedEntry::AwaitingLocation {
                    head,
                    version_created_at,
                } => {
                    let Some((_key, value)) = locations.next() else {
                        return self.emit_error(ListObjectsV2Error::ListObjectsV2Failed);
                    };
                    // Objects without a stored backend location stay hidden.
                    let Some(value) = value else {
                        continue;
                    };
                    let location = match BackendLocation::from_bytes(value.as_ref()) {
                        Ok(location) => location,
                        Err(err) => return self.emit_error(err.into()),
                    };
                    self.objects.push(ListObjectsV2Object {
                        head,
                        location: Some(location),
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
            return self.emit_error(ListObjectsV2Error::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionCommitted)",
                received: event,
            });
        };

        self.state = ListObjectsV2State::Finish;
        smallvec![]
    }
}

impl Operation for ListObjectsV2Operation {
    type Output = Option<Result<ListObjectsV2Result, ListObjectsV2Error>>;
    type Error = ListObjectsV2Error;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.emit_error(ListObjectsV2Error::StorageError(error));
        }

        match self.state {
            ListObjectsV2State::Init => self.handle_init(),
            ListObjectsV2State::StartTransaction => self.handle_transaction_started(event),
            ListObjectsV2State::ReadHeads => self.handle_heads_read(event),
            ListObjectsV2State::ReadVersions => self.handle_round_versions_read(event),
            ListObjectsV2State::ReadBlobLocations => self.handle_locations_read(event),
            ListObjectsV2State::CommitTransaction => self.handle_transaction_committed(event),
            ListObjectsV2State::Finish | ListObjectsV2State::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            ListObjectsV2State::Finish | ListObjectsV2State::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == ListObjectsV2State::Error {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(ListObjectsV2Error::ListObjectsV2Failed);
        }
        Ok(self.output)
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use aruna_core::UserId;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{
        BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE,
    };
    use aruna_core::structs::{
        BlobVersion, CurrentVersionPointer, PortableSourceDescriptor, RealmId, SourceConnectorKind,
        StagingStrategy, VersionSourceBinding,
    };
    use aruna_storage::storage;
    use std::collections::HashMap;
    use std::time::{Duration, UNIX_EPOCH};
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_list_objects_v2_skips_deleted_versions() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let group_id = Ulid::generate();
        let realm_id = RealmId([7u8; 32]);
        let created_by = UserId::local(Ulid::generate(), realm_id);
        let live_version_id = Ulid::generate();
        let deleted_version_id = Ulid::generate();
        let live_hash = [3u8; 32];
        let created_at = UNIX_EPOCH + Duration::from_secs(5);

        for (key, version_id, version) in [
            (
                "alpha",
                live_version_id,
                BlobVersion::materialized(live_hash, created_at, created_by, None),
            ),
            (
                "beta",
                deleted_version_id,
                BlobVersion::deleted(created_at, created_by),
            ),
        ] {
            let _ = storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: BLOB_HEAD_KEYSPACE.to_string(),
                    key: BlobHeadKey::new("bucket", key).to_bytes().unwrap().into(),
                    value: CurrentVersionPointer::new(version_id)
                        .to_bytes()
                        .unwrap()
                        .into(),
                    txn_id: None,
                })
                .await;
            let _ = storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                    key: VersionKey::new("bucket", key, version_id)
                        .to_bytes()
                        .unwrap()
                        .into(),
                    value: version.to_bytes().unwrap().into(),
                    txn_id: None,
                })
                .await;
        }

        let location = BackendLocation {
            root: "/tmp".to_string(),
            storage_bucket: "objects".to_string(),
            backend_path: "path".to_string(),
            ulid: Ulid::generate(),
            compressed: false,
            encrypted: false,
            created_by,
            created_at,
            staging: false,
            partial: false,
            blob_size: 42,
            hashes: HashMap::new(),
        };
        let event = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_LOCATIONS_KEYSPACE.to_string(),
                key: live_hash.to_vec().into(),
                value: location.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));

        let result = drive(
            ListObjectsV2Operation::new(ListObjectsV2Input {
                bucket: "bucket".to_string(),
                group_id,
                continuation_token: None,
                max_keys: Some(10),
                prefix: None,
                delimiter: None,
                start_after: None,
            }),
            &driver_ctx,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        assert_eq!(result.objects.len(), 1);
        assert_eq!(result.objects[0].head.key, "alpha");
        assert_eq!(result.objects[0].location, Some(location));
        assert_eq!(result.continuation_token, None);
    }

    #[tokio::test]
    async fn test_list_objects_v2_with_prefix() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());

        let group_id = Ulid::generate();
        let created_by = UserId::local(Ulid::generate(), RealmId([7u8; 32]));

        seed_materialized_keys(
            &storage_handle,
            "bucket",
            &["common/a", "common/b", "rare/1", "rare/2", "rare/3"],
            created_by,
        )
        .await;

        let mut continuation_token = None;
        let mut all_keys = Vec::new();

        loop {
            let result = drive(
                ListObjectsV2Operation::new(ListObjectsV2Input {
                    bucket: "bucket".to_string(),
                    group_id,
                    continuation_token,
                    max_keys: Some(2),
                    prefix: Some("rare/".to_string()),
                    delimiter: None,
                    start_after: None,
                }),
                &driver_ctx,
            )
            .await
            .unwrap()
            .unwrap()
            .unwrap();

            for obj in result.objects {
                all_keys.push(obj.head.key);
            }

            continuation_token = result.continuation_token;
            if continuation_token.is_none() {
                break;
            }
        }

        // Verify prefix filtered correctly — all returned keys start with "rare/"
        assert!(all_keys.iter().all(|k| k.starts_with("rare/")));
        // And we got the right ones
        let mut sorted = all_keys.clone();
        sorted.sort();
        assert_eq!(sorted, vec!["rare/1", "rare/2", "rare/3"]);
    }

    #[tokio::test]
    async fn test_list_objects_v2_prefix_scan_does_not_stop_on_prefix_miss() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());

        let group_id = Ulid::generate();
        let created_by = UserId::local(Ulid::generate(), RealmId([7u8; 32]));

        seed_materialized_keys(
            &storage_handle,
            "bucket",
            &[
                "common/01",
                "common/02",
                "common/03",
                "common/04",
                "common/05",
                "rare/01",
            ],
            created_by,
        )
        .await;

        let result = drive(
            ListObjectsV2Operation::new(ListObjectsV2Input {
                bucket: "bucket".to_string(),
                group_id,
                continuation_token: None,
                max_keys: Some(1),
                prefix: Some("rare/".to_string()),
                delimiter: None,
                start_after: None,
            }),
            &driver_ctx,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        let keys: Vec<_> = result
            .objects
            .into_iter()
            .map(|object| object.head.key)
            .collect();
        assert_eq!(keys, vec!["rare/01"]);
        assert!(result.continuation_token.is_none());
    }

    #[tokio::test]
    async fn test_list_objects_v2_honors_explicit_zero_max_keys() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let group_id = Ulid::generate();
        let realm_id = RealmId([7u8; 32]);
        let created_by = UserId::local(Ulid::generate(), realm_id);
        let created_at = UNIX_EPOCH + Duration::from_secs(5);
        let version_id = Ulid::generate();
        let hash = [3u8; 32];

        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_HEAD_KEYSPACE.to_string(),
                key: BlobHeadKey::new("bucket", "alpha")
                    .to_bytes()
                    .unwrap()
                    .into(),
                value: CurrentVersionPointer::new(version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                txn_id: None,
            })
            .await;
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: VersionKey::new("bucket", "alpha", version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                value: BlobVersion::materialized(hash, created_at, created_by, None)
                    .to_bytes()
                    .unwrap()
                    .into(),
                txn_id: None,
            })
            .await;

        let result = drive(
            ListObjectsV2Operation::new(ListObjectsV2Input {
                bucket: "bucket".to_string(),
                group_id,
                continuation_token: None,
                max_keys: Some(0),
                prefix: None,
                delimiter: None,
                start_after: None,
            }),
            &driver_ctx,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        assert!(result.objects.is_empty());
        assert!(result.continuation_token.is_none());
    }

    #[tokio::test]
    async fn test_list_objects_v2_pagination_without_prefix() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());

        let group_id = Ulid::generate();
        let created_by = UserId::local(Ulid::generate(), RealmId([7u8; 32]));

        seed_materialized_keys(
            &storage_handle,
            "bucket",
            &["alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta"],
            created_by,
        )
        .await;

        let mut continuation_token = None;
        let mut all_keys = Vec::new();

        loop {
            let result = drive(
                ListObjectsV2Operation::new(ListObjectsV2Input {
                    bucket: "bucket".to_string(),
                    group_id,
                    continuation_token,
                    max_keys: Some(3),
                    prefix: None,
                    delimiter: None,
                    start_after: None,
                }),
                &driver_ctx,
            )
            .await
            .unwrap()
            .unwrap()
            .unwrap();

            for obj in result.objects {
                all_keys.push(obj.head.key);
            }

            continuation_token = result.continuation_token;
            if continuation_token.is_none() {
                break;
            }
        }

        // Verify all 7 keys were returned
        let mut sorted = all_keys.clone();
        sorted.sort();
        assert_eq!(
            sorted,
            vec!["alpha", "beta", "delta", "epsilon", "eta", "gamma", "zeta"]
        );
    }

    #[tokio::test]
    async fn test_list_objects_v2_empty_bucket() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let group_id = Ulid::generate();

        let result = drive(
            ListObjectsV2Operation::new(ListObjectsV2Input {
                bucket: "empty-bucket".to_string(),
                group_id,
                continuation_token: None,
                max_keys: Some(10),
                prefix: None,
                delimiter: None,
                start_after: None,
            }),
            &driver_ctx,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        assert!(result.objects.is_empty());
        assert!(result.continuation_token.is_none());
    }

    #[tokio::test]
    async fn test_list_objects_v2_reference_object() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = DriverContext {
            storage_handle: storage_handle.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let group_id = Ulid::generate();
        let realm_id = RealmId([7u8; 32]);
        let created_by = UserId::local(Ulid::generate(), realm_id);
        let version_id = Ulid::generate();
        let created_at = UNIX_EPOCH + Duration::from_secs(5);
        let last_refresh = UNIX_EPOCH + Duration::from_secs(20);

        let source_metadata = SourceMetadata {
            content_length: 42,
            content_type: Some("text/plain".to_string()),
            etag: Some("ref-etag-1".to_string()),
            last_modified: Some(UNIX_EPOCH + Duration::from_secs(10)),
            source_version: None,
        };

        let version = BlobVersion::reference(
            VersionSourceBinding {
                strategy: StagingStrategy::Reference,
                descriptor: PortableSourceDescriptor {
                    kind: SourceConnectorKind::Http,
                    public_config: HashMap::new(),
                    source_path: "source/path".to_string(),
                    version_selector: None,
                    capabilities: Vec::new(),
                    origin_node_id: None,
                },
                connector_id: None,
            },
            source_metadata.clone(),
            created_at,
            created_by,
            last_refresh,
        );

        // Write head entry
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_HEAD_KEYSPACE.to_string(),
                key: BlobHeadKey::new("bucket", "ref-object")
                    .to_bytes()
                    .unwrap()
                    .into(),
                value: CurrentVersionPointer::new(version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                txn_id: None,
            })
            .await;

        // Write version entry (reference — no location)
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: VersionKey::new("bucket", "ref-object", version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                value: version.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;

        let result = drive(
            ListObjectsV2Operation::new(ListObjectsV2Input {
                bucket: "bucket".to_string(),
                group_id,
                continuation_token: None,
                max_keys: Some(10),
                prefix: None,
                delimiter: None,
                start_after: None,
            }),
            &driver_ctx,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        assert_eq!(result.objects.len(), 1);
        assert_eq!(result.objects[0].head.key, "ref-object");
        assert_eq!(result.objects[0].location, None);
        assert_eq!(result.objects[0].source_metadata, Some(source_metadata));
        assert!(result.objects[0].referenced);
        assert_eq!(result.objects[0].kind, Some(SourceConnectorKind::Http));
        assert_eq!(
            result.objects[0].source_path.as_deref(),
            Some("source/path")
        );
        assert_eq!(result.objects[0].connector_id, None);
        assert_eq!(result.objects[0].origin_node_id, None);
        assert_eq!(result.objects[0].last_refresh, Some(last_refresh));
        assert!(result.continuation_token.is_none());
    }

    fn driver_context(storage_handle: storage::StorageHandle) -> DriverContext {
        DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        }
    }

    async fn seed_materialized_keys(
        storage_handle: &storage::StorageHandle,
        bucket: &str,
        keys: &[&str],
        created_by: UserId,
    ) {
        let created_at = UNIX_EPOCH + Duration::from_secs(5);
        for (index, key) in keys.iter().enumerate() {
            let version_id = Ulid::generate();
            let hash = [index as u8 + 1; 32];
            let version = BlobVersion::materialized(hash, created_at, created_by, None);
            let _ = storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: BLOB_HEAD_KEYSPACE.to_string(),
                    key: BlobHeadKey::new(bucket, *key).to_bytes().unwrap().into(),
                    value: CurrentVersionPointer::new(version_id)
                        .to_bytes()
                        .unwrap()
                        .into(),
                    txn_id: None,
                })
                .await;
            let _ = storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                    key: VersionKey::new(bucket, *key, version_id)
                        .to_bytes()
                        .unwrap()
                        .into(),
                    value: version.to_bytes().unwrap().into(),
                    txn_id: None,
                })
                .await;
            let _ = storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: BLOB_LOCATIONS_KEYSPACE.to_string(),
                    key: hash.to_vec().into(),
                    value: BackendLocation {
                        root: "/tmp".to_string(),
                        storage_bucket: "objects".to_string(),
                        backend_path: format!("path/{key}"),
                        ulid: Ulid::generate(),
                        compressed: false,
                        encrypted: false,
                        created_by,
                        created_at,
                        staging: false,
                        partial: false,
                        blob_size: 42,
                        hashes: HashMap::new(),
                    }
                    .to_bytes()
                    .unwrap()
                    .into(),
                    txn_id: None,
                })
                .await;
        }
    }

    async fn seed_deleted_keys(
        storage_handle: &storage::StorageHandle,
        bucket: &str,
        keys: &[&str],
        created_by: UserId,
    ) {
        let created_at = UNIX_EPOCH + Duration::from_secs(5);
        for key in keys.iter() {
            let version_id = Ulid::generate();
            let version = BlobVersion::deleted(created_at, created_by);
            let _ = storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: BLOB_HEAD_KEYSPACE.to_string(),
                    key: BlobHeadKey::new(bucket, *key).to_bytes().unwrap().into(),
                    value: CurrentVersionPointer::new(version_id)
                        .to_bytes()
                        .unwrap()
                        .into(),
                    txn_id: None,
                })
                .await;
            let _ = storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                    key: VersionKey::new(bucket, *key, version_id)
                        .to_bytes()
                        .unwrap()
                        .into(),
                    value: version.to_bytes().unwrap().into(),
                    txn_id: None,
                })
                .await;
        }
    }

    async fn list_keys(
        driver_ctx: &DriverContext,
        bucket: &str,
        prefix: Option<&str>,
        start_after: Option<&str>,
    ) -> Vec<String> {
        let result = drive(
            ListObjectsV2Operation::new(ListObjectsV2Input {
                bucket: bucket.to_string(),
                group_id: Ulid::generate(),
                continuation_token: None,
                max_keys: Some(100),
                prefix: prefix.map(str::to_string),
                delimiter: None,
                start_after: start_after.map(str::to_string),
            }),
            driver_ctx,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        result
            .objects
            .into_iter()
            .map(|object| object.head.key)
            .collect()
    }

    #[tokio::test]
    async fn test_list_objects_v2_prefix_includes_key_equal_to_prefix() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());
        let created_by = UserId::local(Ulid::generate(), RealmId([7u8; 32]));

        seed_materialized_keys(
            &storage_handle,
            "bucket",
            &["docs/", "docs/readme.md"],
            created_by,
        )
        .await;

        let keys = list_keys(&driver_ctx, "bucket", Some("docs/"), None).await;
        assert_eq!(keys, vec!["docs/", "docs/readme.md"]);

        let keys = list_keys(&driver_ctx, "bucket", Some("docs/readme.md"), None).await;
        assert_eq!(keys, vec!["docs/readme.md"]);
    }

    #[tokio::test]
    async fn test_list_objects_v2_prefix_scan_with_interleaved_shorter_key() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());
        let created_by = UserId::local(Ulid::generate(), RealmId([7u8; 32]));

        seed_materialized_keys(&storage_handle, "bucket", &["rare0", "rare/1"], created_by).await;

        let keys = list_keys(&driver_ctx, "bucket", Some("rare/"), None).await;
        assert_eq!(keys, vec!["rare/1"]);
    }

    #[tokio::test]
    async fn test_list_objects_v2_returns_keys_in_lexicographic_order() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());
        let created_by = UserId::local(Ulid::generate(), RealmId([7u8; 32]));

        seed_materialized_keys(&storage_handle, "bucket", &["b", "aa", "a/1"], created_by).await;

        let keys = list_keys(&driver_ctx, "bucket", None, None).await;
        assert_eq!(keys, vec!["a/1", "aa", "b"]);
    }

    #[tokio::test]
    async fn test_list_objects_v2_start_after_skips_preceding_keys() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());
        let created_by = UserId::local(Ulid::generate(), RealmId([7u8; 32]));

        seed_materialized_keys(&storage_handle, "bucket", &["a", "b", "c"], created_by).await;

        let keys = list_keys(&driver_ctx, "bucket", None, Some("a")).await;
        assert_eq!(keys, vec!["b", "c"]);

        let keys = list_keys(&driver_ctx, "bucket", None, Some("b")).await;
        assert_eq!(keys, vec!["c"]);

        let keys = list_keys(&driver_ctx, "bucket", None, Some("c")).await;
        assert!(keys.is_empty());
    }

    #[tokio::test]
    async fn test_list_objects_v2_start_after_beyond_prefix_range_returns_empty() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());
        let created_by = UserId::local(Ulid::generate(), RealmId([7u8; 32]));

        seed_materialized_keys(&storage_handle, "bucket", &["docs/1", "docs/2"], created_by).await;

        let keys = list_keys(&driver_ctx, "bucket", Some("docs/"), Some("zzz")).await;
        assert!(keys.is_empty());

        let keys = list_keys(&driver_ctx, "bucket", Some("docs/"), Some("a")).await;
        assert_eq!(keys, vec!["docs/1", "docs/2"]);
    }

    async fn list_page(
        driver_ctx: &DriverContext,
        bucket: &str,
        delimiter: Option<&str>,
        max_keys: usize,
        continuation_token: Option<ListObjectsV2ContinuationToken>,
    ) -> ListObjectsV2Result {
        drive(
            ListObjectsV2Operation::new(ListObjectsV2Input {
                bucket: bucket.to_string(),
                group_id: Ulid::generate(),
                continuation_token,
                max_keys: Some(max_keys),
                prefix: None,
                delimiter: delimiter.map(str::to_string),
                start_after: None,
            }),
            driver_ctx,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap()
    }

    #[tokio::test]
    async fn test_list_objects_v2_delimiter_groups_keys() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());
        let created_by = UserId::local(Ulid::generate(), RealmId([7u8; 32]));

        seed_materialized_keys(
            &storage_handle,
            "bucket",
            &["a.txt", "dir/1", "dir/2", "z.txt"],
            created_by,
        )
        .await;

        let result = list_page(&driver_ctx, "bucket", Some("/"), 100, None).await;

        let keys: Vec<_> = result
            .objects
            .into_iter()
            .map(|object| object.head.key)
            .collect();
        assert_eq!(keys, vec!["a.txt", "z.txt"]);
        assert_eq!(result.common_prefixes, vec!["dir/"]);
        assert!(result.continuation_token.is_none());
    }

    #[tokio::test]
    async fn test_list_objects_v2_delimiter_pagination_never_repeats_prefixes() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());
        let created_by = UserId::local(Ulid::generate(), RealmId([7u8; 32]));

        seed_materialized_keys(
            &storage_handle,
            "bucket",
            &["a", "dir/1", "dir/2", "dir/3", "z"],
            created_by,
        )
        .await;

        let mut continuation_token = None;
        let mut all_keys = Vec::new();
        let mut all_prefixes = Vec::new();
        let mut pages = 0;

        loop {
            let result = list_page(
                &driver_ctx,
                "bucket",
                Some("/"),
                1,
                continuation_token.take(),
            )
            .await;
            all_keys.extend(result.objects.into_iter().map(|object| object.head.key));
            all_prefixes.extend(result.common_prefixes);
            pages += 1;
            assert!(pages < 10);

            continuation_token = result.continuation_token;
            if continuation_token.is_none() {
                break;
            }
        }

        assert_eq!(all_keys, vec!["a", "z"]);
        assert_eq!(all_prefixes, vec!["dir/"]);
    }

    #[tokio::test]
    async fn test_list_objects_v2_batched_hydration_preserves_key_order() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());

        let created_by = UserId::local(Ulid::generate(), RealmId([7u8; 32]));
        let created_at = UNIX_EPOCH + Duration::from_secs(5);
        let last_refresh = UNIX_EPOCH + Duration::from_secs(20);

        seed_materialized_keys(&storage_handle, "bucket", &["alpha", "delta"], created_by).await;

        let source_metadata = SourceMetadata {
            content_length: 42,
            content_type: Some("text/plain".to_string()),
            etag: Some("ref-etag-1".to_string()),
            last_modified: Some(UNIX_EPOCH + Duration::from_secs(10)),
            source_version: None,
        };
        let reference = BlobVersion::reference(
            VersionSourceBinding {
                strategy: StagingStrategy::Reference,
                descriptor: PortableSourceDescriptor {
                    kind: SourceConnectorKind::Http,
                    public_config: HashMap::new(),
                    source_path: "source/path".to_string(),
                    version_selector: None,
                    capabilities: Vec::new(),
                    origin_node_id: None,
                },
                connector_id: None,
            },
            source_metadata.clone(),
            created_at,
            created_by,
            last_refresh,
        );
        let deleted = BlobVersion::deleted(created_at, created_by);

        for (key, version) in [("beta", reference), ("gamma", deleted)] {
            let version_id = Ulid::generate();
            let _ = storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: BLOB_HEAD_KEYSPACE.to_string(),
                    key: BlobHeadKey::new("bucket", key).to_bytes().unwrap().into(),
                    value: CurrentVersionPointer::new(version_id)
                        .to_bytes()
                        .unwrap()
                        .into(),
                    txn_id: None,
                })
                .await;
            let _ = storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                    key: VersionKey::new("bucket", key, version_id)
                        .to_bytes()
                        .unwrap()
                        .into(),
                    value: version.to_bytes().unwrap().into(),
                    txn_id: None,
                })
                .await;
        }

        let result = drive(
            ListObjectsV2Operation::new(ListObjectsV2Input {
                bucket: "bucket".to_string(),
                group_id: Ulid::generate(),
                continuation_token: None,
                max_keys: Some(10),
                prefix: None,
                delimiter: None,
                start_after: None,
            }),
            &driver_ctx,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        let keys: Vec<_> = result
            .objects
            .iter()
            .map(|object| object.head.key.as_str())
            .collect();
        assert_eq!(keys, vec!["alpha", "beta", "delta"]);
        assert!(result.objects[0].location.is_some());
        assert_eq!(result.objects[1].source_metadata, Some(source_metadata));
        assert!(result.objects[2].location.is_some());
    }

    fn delimiter_input(
        max_keys: usize,
        continuation_token: Option<ListObjectsV2ContinuationToken>,
        delimiter: Option<&str>,
    ) -> ListObjectsV2Input {
        ListObjectsV2Input {
            bucket: "bucket".to_string(),
            group_id: Ulid::generate(),
            continuation_token,
            max_keys: Some(max_keys),
            prefix: None,
            delimiter: delimiter.map(str::to_string),
            start_after: None,
        }
    }

    fn step_transaction_started(operation: &mut ListObjectsV2Operation) -> Effects {
        let effects = operation.start();
        assert!(matches!(
            effects[0],
            Effect::Storage(StorageEffect::StartTransaction { .. })
        ));
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: Ulid::generate(),
        }))
    }

    #[test]
    fn scan_round_ending_inside_group_seeks_past_group() {
        let mut operation = ListObjectsV2Operation::new(delimiter_input(1, None, Some("/")));

        let effects = step_transaction_started(&mut operation);
        let Effect::Storage(StorageEffect::Iter {
            start: None, limit, ..
        }) = &effects[0]
        else {
            panic!("expected initial scan round: {:?}", effects[0]);
        };

        let pointer: aruna_core::types::Value = CurrentVersionPointer::new(Ulid::generate())
            .to_bytes()
            .unwrap()
            .into();
        let values = (0..*limit)
            .map(|index| {
                (
                    BlobHeadKey::new("bucket", format!("dir/{index}"))
                        .to_bytes()
                        .unwrap()
                        .into(),
                    pointer.clone(),
                )
            })
            .collect();
        let effects = operation.step(Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after: None,
        }));

        // The heads now need a version read before the group can be emitted.
        let Effect::Storage(StorageEffect::BatchRead { reads, .. }) = &effects[0] else {
            panic!("expected version read for grouped heads: {:?}", effects[0]);
        };
        let created_by = UserId::local(Ulid::generate(), RealmId([7u8; 32]));
        let created_at = UNIX_EPOCH + Duration::from_secs(5);
        let version: aruna_core::types::Value =
            BlobVersion::materialized([1u8; 32], created_at, created_by, None)
                .to_bytes()
                .unwrap()
                .into();
        let version_values = reads
            .iter()
            .map(|(_, key)| (key.clone(), Some(version.clone())))
            .collect();
        let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: version_values,
        }));

        let Effect::Storage(StorageEffect::Iter { start, .. }) = &effects[0] else {
            panic!("expected follow-up scan round: {:?}", effects[0]);
        };
        assert_eq!(start, &Some(IterStart::At(b"bucket/dir0".to_vec().into())));
    }

    #[test]
    fn resume_inside_group_seeks_past_group() {
        let token = ListObjectsV2ContinuationToken {
            last_key: BlobHeadKey::new("bucket", "dir/5").to_bytes().unwrap(),
            last_common_prefix: Some("dir/".to_string()),
        };

        let mut operation =
            ListObjectsV2Operation::new(delimiter_input(10, Some(token.clone()), Some("/")));
        let effects = step_transaction_started(&mut operation);
        let Effect::Storage(StorageEffect::Iter { start, .. }) = &effects[0] else {
            panic!("expected resumed scan round: {:?}", effects[0]);
        };
        assert_eq!(start, &Some(IterStart::At(b"bucket/dir0".to_vec().into())));

        // Without the delimiter the group no longer applies: resume behind
        // the exclusive cursor instead of seeking.
        let mut operation =
            ListObjectsV2Operation::new(delimiter_input(10, Some(token.clone()), None));
        let effects = step_transaction_started(&mut operation);
        let Effect::Storage(StorageEffect::Iter { start, .. }) = &effects[0] else {
            panic!("expected resumed scan round: {:?}", effects[0]);
        };
        assert_eq!(
            start,
            &Some(IterStart::After(token.last_key.clone().into()))
        );
    }

    #[tokio::test]
    async fn test_list_objects_v2_paginates_past_large_group() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());
        let created_by = UserId::local(Ulid::generate(), RealmId([7u8; 32]));

        let group_keys: Vec<String> = (0..30).map(|index| format!("dir/{index:02}")).collect();
        let mut keys: Vec<&str> = vec!["a"];
        keys.extend(group_keys.iter().map(String::as_str));
        keys.push("z");
        seed_materialized_keys(&storage_handle, "bucket", &keys, created_by).await;

        let mut continuation_token = None;
        let mut all_keys = Vec::new();
        let mut all_prefixes = Vec::new();
        let mut pages = 0;

        loop {
            let result = list_page(
                &driver_ctx,
                "bucket",
                Some("/"),
                2,
                continuation_token.take(),
            )
            .await;
            all_keys.extend(result.objects.into_iter().map(|object| object.head.key));
            all_prefixes.extend(result.common_prefixes);
            pages += 1;
            assert!(pages <= 3);

            continuation_token = result.continuation_token;
            if continuation_token.is_none() {
                break;
            }
        }

        assert_eq!(all_keys, vec!["a", "z"]);
        assert_eq!(all_prefixes, vec!["dir/"]);
    }

    // (a) A key whose latest version is a delete marker is absent from Contents,
    // including the zero-byte folder marker key equal to the listing prefix.
    #[tokio::test]
    async fn test_list_objects_v2_delete_marker_absent_from_contents() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());
        let created_by = UserId::local(Ulid::generate(), RealmId([7u8; 32]));

        seed_materialized_keys(&storage_handle, "bucket", &["docs/readme.md"], created_by).await;
        seed_deleted_keys(
            &storage_handle,
            "bucket",
            &["docs/", "docs/old.md"],
            created_by,
        )
        .await;

        // Listing inside the folder returns only the live child; neither the
        // delete-markered marker key "docs/" nor the deleted child appear.
        let keys = list_keys(&driver_ctx, "bucket", Some("docs/"), None).await;
        assert_eq!(keys, vec!["docs/readme.md"]);
    }

    // (b) A prefix whose every key is delete-markered produces NO common prefix
    // and lists nothing, even though the head keys still physically exist.
    #[tokio::test]
    async fn test_list_objects_v2_delimiter_hides_fully_deleted_prefix() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());
        let created_by = UserId::local(Ulid::generate(), RealmId([7u8; 32]));

        seed_materialized_keys(&storage_handle, "bucket", &["a.txt"], created_by).await;
        seed_deleted_keys(
            &storage_handle,
            "bucket",
            &["dir/", "dir/1", "dir/2"],
            created_by,
        )
        .await;

        let result = list_page(&driver_ctx, "bucket", Some("/"), 100, None).await;

        let keys: Vec<_> = result
            .objects
            .into_iter()
            .map(|object| object.head.key)
            .collect();
        assert_eq!(keys, vec!["a.txt"]);
        assert!(result.common_prefixes.is_empty());
        assert!(result.continuation_token.is_none());
    }

    // (c) A prefix mixing live and delete-markered keys still produces the
    // common prefix; the leading key of the group is a delete marker to prove
    // the scan keeps looking for a live sibling before dropping the prefix.
    #[tokio::test]
    async fn test_list_objects_v2_delimiter_keeps_mixed_prefix() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());
        let created_by = UserId::local(Ulid::generate(), RealmId([7u8; 32]));

        seed_materialized_keys(
            &storage_handle,
            "bucket",
            &["a.txt", "dir/live"],
            created_by,
        )
        .await;
        seed_deleted_keys(&storage_handle, "bucket", &["dir/dead"], created_by).await;

        let result = list_page(&driver_ctx, "bucket", Some("/"), 100, None).await;

        let keys: Vec<_> = result
            .objects
            .into_iter()
            .map(|object| object.head.key)
            .collect();
        assert_eq!(keys, vec!["a.txt"]);
        assert_eq!(result.common_prefixes, vec!["dir/"]);

        // Listing inside the folder lists only the live key.
        let inside = list_keys(&driver_ctx, "bucket", Some("dir/"), None).await;
        assert_eq!(inside, vec!["dir/live"]);
    }

    // (d) Pagination across delete-markered keys keeps KeyCount and IsTruncated
    // correct: each page carries max_keys live results and the marker keys never
    // inflate a page or short-count it.
    #[tokio::test]
    async fn test_list_objects_v2_pagination_skips_delete_markers() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());
        let created_by = UserId::local(Ulid::generate(), RealmId([7u8; 32]));

        seed_materialized_keys(&storage_handle, "bucket", &["a", "c", "e"], created_by).await;
        seed_deleted_keys(&storage_handle, "bucket", &["b", "d"], created_by).await;

        let mut continuation_token = None;
        let mut pages: Vec<Vec<String>> = Vec::new();

        loop {
            let result = list_page(&driver_ctx, "bucket", None, 2, continuation_token.take()).await;
            let truncated = result.continuation_token.is_some();
            let keys: Vec<String> = result
                .objects
                .into_iter()
                .map(|object| object.head.key)
                .collect();
            // A truncated page must be full; only the final page may be short.
            if truncated {
                assert_eq!(keys.len(), 2);
            }
            pages.push(keys);
            continuation_token = result.continuation_token;
            assert!(pages.len() <= 3);
            if continuation_token.is_none() {
                break;
            }
        }

        assert_eq!(pages, vec![vec!["a", "c"], vec!["e"]]);
    }
}
