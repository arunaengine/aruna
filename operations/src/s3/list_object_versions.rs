use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::{
    BackendLocation, BlobHeadKey, BlobVersion, BlobVersionState, CurrentVersionPointer,
    SourceMetadata, VersionKey,
};
use aruna_core::types::{Effects, Key, Value};
use aruna_core::util::prefix_upper_bound;
use smallvec::smallvec;
use std::collections::VecDeque;
use std::time::SystemTime;
use thiserror::Error;
use ulid::Ulid;

use crate::s3::listing::common_prefix_of;

// A single key may accumulate more versions than one scan page holds. The
// storage layer only iterates forward (oldest -> newest), so the version prefix
// is scanned in full and a rolling window retains the newest VERSION_SCAN_LIMIT
// entries, discarding older ones as the scan advances.
const VERSION_SCAN_LIMIT: usize = 10_000;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ListObjectVersionsState {
    Init,
    StartTransaction,
    ReadHeads,
    ReadVersionsForKey,
    ReadBlobLocations,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum ListObjectVersionsError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("State [{state:?}] invalid: expected [{expected:?}] - received [{received:?}]")]
    InvalidStateEvent {
        state: ListObjectVersionsState,
        expected: &'static str,
        received: Event,
    },
    #[error("No transaction found")]
    NoTransactionFound,
    #[error("ListObjectVersions failed")]
    ListObjectVersionsFailed,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ListObjectVersionsInput {
    pub bucket: String,
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    pub key_marker: Option<String>,
    pub version_id_marker: Option<Ulid>,
    pub max_keys: Option<usize>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ListObjectVersionsItem {
    Version {
        key: String,
        version_id: Ulid,
        is_latest: bool,
        location: Option<BackendLocation>,
        source_metadata: Option<SourceMetadata>,
        created_at: SystemTime,
    },
    DeleteMarker {
        key: String,
        version_id: Ulid,
        is_latest: bool,
        created_at: SystemTime,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct ListObjectVersionsResult {
    pub items: Vec<ListObjectVersionsItem>,
    pub common_prefixes: Vec<String>,
    pub is_truncated: bool,
    pub next_key_marker: Option<String>,
    pub next_version_id_marker: Option<Ulid>,
}

#[derive(Debug, PartialEq)]
enum PendingItem {
    Ready(ListObjectVersionsItem),
    AwaitingLocation {
        key: String,
        version_id: Ulid,
        is_latest: bool,
        created_at: SystemTime,
    },
}

#[derive(Debug, PartialEq)]
pub struct ListObjectVersionsOperation {
    input: ListObjectVersionsInput,
    state: ListObjectVersionsState,
    txn_id: Option<Ulid>,
    scan_prefix: Vec<u8>,
    pending_start: Option<IterStart>,
    last_head_key: Option<Vec<u8>>,
    cursor_group_prefix: Option<Vec<u8>>,
    head_round_limit: usize,
    head_exhausted: bool,
    scan_rounds: usize,
    max_scan_rounds: usize,
    resume_common_prefix: Option<String>,
    last_emitted_group: Option<String>,
    pending_heads: VecDeque<(String, Ulid)>,
    current_key: Option<(String, Ulid)>,
    version_scan_limit: usize,
    version_scan_cursor: Option<Key>,
    version_window: VecDeque<(Ulid, BlobVersion)>,
    current_pending: Vec<PendingItem>,
    items: Vec<ListObjectVersionsItem>,
    common_prefixes: Vec<String>,
    is_truncated: bool,
    last_marker: Option<(String, Option<Ulid>)>,
    next_key_marker: Option<String>,
    next_version_id_marker: Option<Ulid>,
    output: Option<Result<ListObjectVersionsResult, ListObjectVersionsError>>,
}

impl ListObjectVersionsOperation {
    pub const DEFAULT_MAX_KEYS: usize = 1_000;
    const MAX_SCAN_ROUNDS: usize = 100;

    pub fn new(input: ListObjectVersionsInput) -> Self {
        Self {
            input,
            state: ListObjectVersionsState::Init,
            txn_id: None,
            scan_prefix: Vec::new(),
            pending_start: None,
            last_head_key: None,
            cursor_group_prefix: None,
            head_round_limit: 0,
            head_exhausted: false,
            scan_rounds: 0,
            max_scan_rounds: Self::MAX_SCAN_ROUNDS,
            resume_common_prefix: None,
            last_emitted_group: None,
            pending_heads: VecDeque::new(),
            current_key: None,
            version_scan_limit: VERSION_SCAN_LIMIT,
            version_scan_cursor: None,
            version_window: VecDeque::new(),
            current_pending: Vec::new(),
            items: Vec::new(),
            common_prefixes: Vec::new(),
            is_truncated: false,
            last_marker: None,
            next_key_marker: None,
            next_version_id_marker: None,
            output: None,
        }
    }

    #[cfg(test)]
    fn with_max_scan_rounds(mut self, max_scan_rounds: usize) -> Self {
        self.max_scan_rounds = max_scan_rounds;
        self
    }

    #[cfg(test)]
    fn with_version_scan_limit(mut self, version_scan_limit: usize) -> Self {
        self.version_scan_limit = version_scan_limit;
        self
    }

    fn emit_error(&mut self, error: ListObjectVersionsError) -> Effects {
        self.state = ListObjectVersionsState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn max_keys(&self) -> usize {
        self.input.max_keys.unwrap_or(Self::DEFAULT_MAX_KEYS)
    }

    fn emit_count(&self) -> usize {
        self.items.len() + self.common_prefixes.len()
    }

    fn prefix_str(&self) -> Option<&str> {
        self.input
            .prefix
            .as_deref()
            .filter(|prefix| !prefix.is_empty())
    }

    fn handle_init(&mut self) -> Effects {
        if self.max_keys() == 0 {
            self.state = ListObjectVersionsState::Finish;
            self.output = Some(Ok(ListObjectVersionsResult {
                items: Vec::new(),
                common_prefixes: Vec::new(),
                is_truncated: false,
                next_key_marker: None,
                next_version_id_marker: None,
            }));
            return smallvec![];
        }

        self.state = ListObjectVersionsState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: true
        })]
    }

    fn handle_transaction_started(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.emit_error(ListObjectVersionsError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionStarted)",
                received: event,
            });
        };

        self.txn_id = Some(txn_id);
        let prefix = match self.prefix_str() {
            Some(prefix) => BlobHeadKey::object_prefix(&self.input.bucket, prefix),
            None => BlobHeadKey::bucket_prefix(&self.input.bucket),
        };
        let prefix = match prefix {
            Ok(prefix) => prefix,
            Err(err) => return self.emit_error(err.into()),
        };

        let delimiter = self
            .input
            .delimiter
            .as_deref()
            .filter(|delimiter| !delimiter.is_empty());
        let key_marker = self
            .input
            .key_marker
            .as_deref()
            .filter(|key_marker| !key_marker.is_empty());

        if let (Some(delimiter), Some(key_marker)) = (delimiter, key_marker)
            && key_marker.ends_with(delimiter)
        {
            self.resume_common_prefix = Some(key_marker.to_string());
        }

        let start = match key_marker {
            Some(key_marker) => {
                let start_key = match BlobHeadKey::object_prefix(&self.input.bucket, key_marker) {
                    Ok(start_key) => start_key,
                    Err(err) => return self.emit_error(err.into()),
                };
                if start_key.as_slice() > prefix.as_slice() && !start_key.starts_with(&prefix) {
                    self.scan_prefix = prefix;
                    return self.commit();
                }
                // Include the marker key itself when we still owe its older
                // versions (version_id_marker) or need to skip its group.
                if self.input.version_id_marker.is_some() || self.resume_common_prefix.is_some() {
                    Some(IterStart::At(start_key.into()))
                } else {
                    Some(IterStart::After(start_key.into()))
                }
            }
            None => None,
        };

        self.scan_prefix = prefix;
        self.pending_start = start;
        self.issue_head_round()
    }

    fn issue_head_round(&mut self) -> Effects {
        self.scan_rounds += 1;
        self.head_round_limit = self
            .max_keys()
            .saturating_sub(self.emit_count())
            .saturating_add(1);

        let start = if let Some(start) = self.pending_start.take() {
            Some(start)
        } else if let Some(seek) = self
            .cursor_group_prefix
            .as_deref()
            .and_then(prefix_upper_bound)
        {
            Some(IterStart::At(seek.into()))
        } else {
            self.last_head_key
                .clone()
                .map(|key| IterStart::After(key.into()))
        };

        self.state = ListObjectVersionsState::ReadHeads;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: BLOB_HEAD_KEYSPACE.to_string(),
            prefix: Some(self.scan_prefix.clone().into()),
            start,
            limit: self.head_round_limit,
            txn_id: self.txn_id,
        })]
    }

    fn handle_heads_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult { values, .. }) = event else {
            return self.emit_error(ListObjectVersionsError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::IterResult)",
                received: event,
            });
        };

        let round_len = values.len();
        for (key, value) in values {
            let head = match BlobHeadKey::from_bytes(key.as_ref()) {
                Ok(head) => head,
                Err(err) => return self.emit_error(err.into()),
            };
            let pointer = match CurrentVersionPointer::from_bytes(value.as_ref()) {
                Ok(pointer) => pointer,
                Err(err) => return self.emit_error(err.into()),
            };
            self.pending_heads.push_back((head.key, pointer.version_id));
        }

        if round_len < self.head_round_limit {
            self.head_exhausted = true;
        }

        self.advance()
    }

    fn advance(&mut self) -> Effects {
        loop {
            if self.is_truncated {
                return self.commit();
            }

            let Some((key, head_version_id)) = self.pending_heads.pop_front() else {
                if self.head_exhausted {
                    return self.commit();
                }
                if self.scan_rounds >= self.max_scan_rounds {
                    self.truncate();
                    return self.commit();
                }
                return self.issue_head_round();
            };

            self.last_head_key = match BlobHeadKey::object_prefix(&self.input.bucket, &key) {
                Ok(bytes) => Some(bytes),
                Err(err) => return self.emit_error(err.into()),
            };

            match common_prefix_of(&key, self.prefix_str(), self.input.delimiter.as_deref()) {
                Some(group) => {
                    let already_emitted = self.last_emitted_group.as_deref()
                        == Some(group.as_str())
                        || self.resume_common_prefix.as_deref() == Some(group.as_str());
                    self.cursor_group_prefix =
                        match BlobHeadKey::object_prefix(&self.input.bucket, &group) {
                            Ok(bytes) => Some(bytes),
                            Err(err) => return self.emit_error(err.into()),
                        };
                    if already_emitted {
                        continue;
                    }
                    self.resume_common_prefix = None;
                    if self.try_emit_common_prefix(group) {
                        return self.commit();
                    }
                    continue;
                }
                None => {
                    self.last_emitted_group = None;
                    self.resume_common_prefix = None;
                    self.cursor_group_prefix = None;
                    self.current_key = Some((key.clone(), head_version_id));
                    self.version_window.clear();
                    self.version_scan_cursor = None;
                    return self.issue_version_iter(&key);
                }
            }
        }
    }

    fn issue_version_iter(&mut self, key: &str) -> Effects {
        let prefix = match VersionKey::object_prefix(&self.input.bucket, key) {
            Ok(prefix) => prefix,
            Err(err) => return self.emit_error(err.into()),
        };
        let start = self.version_scan_cursor.take().map(IterStart::After);
        self.state = ListObjectVersionsState::ReadVersionsForKey;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            prefix: Some(prefix.into()),
            start,
            limit: self.version_scan_limit,
            txn_id: self.txn_id,
        })]
    }

    fn handle_versions_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) = event
        else {
            return self.emit_error(ListObjectVersionsError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::IterResult)",
                received: event,
            });
        };

        let Some((key, head_version_id)) = self.current_key.clone() else {
            return self.emit_error(ListObjectVersionsError::ListObjectVersionsFailed);
        };

        for (version_key, value) in values {
            let version_key = match VersionKey::from_bytes(version_key.as_ref()) {
                Ok(version_key) => version_key,
                Err(err) => return self.emit_error(err.into()),
            };
            let version = match BlobVersion::from_bytes(value.as_ref()) {
                Ok(version) => version,
                Err(err) => return self.emit_error(err.into()),
            };
            self.version_window
                .push_back((version_key.version_id, version));
        }
        while self.version_window.len() > self.version_scan_limit {
            self.version_window.pop_front();
        }

        // Iteration yields oldest -> newest; keep scanning the prefix until it
        // is exhausted so the rolling window settles on the newest versions.
        if let Some(cursor) = next_start_after {
            self.version_scan_cursor = Some(cursor);
            return self.issue_version_iter(&key);
        }

        // Window holds oldest -> newest; reverse for newest-first output.
        let mut versions: Vec<(Ulid, BlobVersion)> = self.version_window.drain(..).collect();
        versions.reverse();

        let apply_marker = self.input.key_marker.as_deref() == Some(key.as_str());
        let version_marker = self.input.version_id_marker;

        let mut location_reads = Vec::new();
        let mut pending = Vec::new();
        for (version_id, version) in versions {
            if apply_marker
                && let Some(marker) = version_marker
                && version_id >= marker
            {
                continue;
            }
            let is_latest = version_id == head_version_id;
            match version.state {
                BlobVersionState::Deleted => {
                    pending.push(PendingItem::Ready(ListObjectVersionsItem::DeleteMarker {
                        key: key.clone(),
                        version_id,
                        is_latest,
                        created_at: version.created_at,
                    }));
                }
                BlobVersionState::Reference {
                    cached_metadata, ..
                } => {
                    pending.push(PendingItem::Ready(ListObjectVersionsItem::Version {
                        key: key.clone(),
                        version_id,
                        is_latest,
                        location: None,
                        source_metadata: Some(cached_metadata),
                        created_at: version.created_at,
                    }));
                }
                BlobVersionState::Materialized { blob_hash, .. } => {
                    location_reads.push((
                        BLOB_LOCATIONS_KEYSPACE.to_string(),
                        blob_hash.to_vec().into(),
                    ));
                    pending.push(PendingItem::AwaitingLocation {
                        key: key.clone(),
                        version_id,
                        is_latest,
                        created_at: version.created_at,
                    });
                }
            }
        }

        self.current_pending = pending;
        if location_reads.is_empty() {
            return self.finish_current_key(Vec::new());
        }

        self.state = ListObjectVersionsState::ReadBlobLocations;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: location_reads,
            txn_id: self.txn_id,
        })]
    }

    fn handle_locations_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.emit_error(ListObjectVersionsError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::BatchReadResult)",
                received: event,
            });
        };

        self.finish_current_key(values)
    }

    fn finish_current_key(
        &mut self,
        locations: Vec<(aruna_core::types::Key, Option<Value>)>,
    ) -> Effects {
        let mut locations = locations.into_iter();
        for item in std::mem::take(&mut self.current_pending) {
            let entry = match item {
                PendingItem::Ready(entry) => entry,
                PendingItem::AwaitingLocation {
                    key,
                    version_id,
                    is_latest,
                    created_at,
                } => {
                    let Some((_key, value)) = locations.next() else {
                        return self.emit_error(ListObjectVersionsError::ListObjectVersionsFailed);
                    };
                    // A materialized version without a stored location is a data
                    // inconsistency; skip it rather than emitting a partial entry.
                    let Some(value) = value else {
                        continue;
                    };
                    let location = match BackendLocation::from_bytes(value.as_ref()) {
                        Ok(location) => location,
                        Err(err) => return self.emit_error(err.into()),
                    };
                    ListObjectVersionsItem::Version {
                        key,
                        version_id,
                        is_latest,
                        location: Some(location),
                        source_metadata: None,
                        created_at,
                    }
                }
            };
            if self.try_emit(entry) {
                return self.commit();
            }
        }

        self.current_key = None;
        self.advance()
    }

    fn try_emit(&mut self, entry: ListObjectVersionsItem) -> bool {
        if self.emit_count() >= self.max_keys() {
            self.truncate();
            return true;
        }
        let marker = match &entry {
            ListObjectVersionsItem::Version {
                key, version_id, ..
            }
            | ListObjectVersionsItem::DeleteMarker {
                key, version_id, ..
            } => (key.clone(), Some(*version_id)),
        };
        self.last_marker = Some(marker);
        self.items.push(entry);
        false
    }

    fn try_emit_common_prefix(&mut self, group: String) -> bool {
        if self.emit_count() >= self.max_keys() {
            self.truncate();
            return true;
        }
        self.last_emitted_group = Some(group.clone());
        self.last_marker = Some((group.clone(), None));
        self.common_prefixes.push(group);
        false
    }

    fn truncate(&mut self) {
        self.is_truncated = true;
        if let Some((key, version_id)) = self.last_marker.clone() {
            self.next_key_marker = Some(key);
            self.next_version_id_marker = version_id;
        }
    }

    fn commit(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(ListObjectVersionsError::NoTransactionFound);
        };

        self.state = ListObjectVersionsState::CommitTransaction;
        self.output = Some(Ok(ListObjectVersionsResult {
            items: std::mem::take(&mut self.items),
            common_prefixes: std::mem::take(&mut self.common_prefixes),
            is_truncated: self.is_truncated,
            next_key_marker: self.next_key_marker.take(),
            next_version_id_marker: self.next_version_id_marker.take(),
        }));
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_transaction_committed(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.emit_error(ListObjectVersionsError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionCommitted)",
                received: event,
            });
        };

        self.state = ListObjectVersionsState::Finish;
        smallvec![]
    }
}

impl Operation for ListObjectVersionsOperation {
    type Output = Option<Result<ListObjectVersionsResult, ListObjectVersionsError>>;
    type Error = ListObjectVersionsError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.emit_error(ListObjectVersionsError::StorageError(error));
        }

        match self.state {
            ListObjectVersionsState::Init => self.handle_init(),
            ListObjectVersionsState::StartTransaction => self.handle_transaction_started(event),
            ListObjectVersionsState::ReadHeads => self.handle_heads_read(event),
            ListObjectVersionsState::ReadVersionsForKey => self.handle_versions_read(event),
            ListObjectVersionsState::ReadBlobLocations => self.handle_locations_read(event),
            ListObjectVersionsState::CommitTransaction => self.handle_transaction_committed(event),
            ListObjectVersionsState::Finish | ListObjectVersionsState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            ListObjectVersionsState::Finish | ListObjectVersionsState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == ListObjectVersionsState::Error {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(ListObjectVersionsError::ListObjectVersionsFailed);
        }
        Ok(self.output)
    }

    fn abort(&mut self) -> Effects {
        self.txn_id
            .take()
            .map_or_else(smallvec::SmallVec::new, |txn_id| {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use aruna_core::UserId;
    use aruna_core::effects::StorageEffect;
    use aruna_core::structs::checksum::HASH_MD5;
    use aruna_core::structs::{
        PortableSourceDescriptor, RealmId, SourceConnectorKind, StagingStrategy,
        VersionSourceBinding,
    };
    use aruna_storage::storage;
    use std::collections::HashMap;
    use std::time::{Duration, UNIX_EPOCH};
    use tempfile::tempdir;

    fn driver_context(storage_handle: storage::StorageHandle) -> DriverContext {
        DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
        }
    }

    fn created_by() -> UserId {
        UserId::local(Ulid::new(), RealmId::from_bytes([1u8; 32]))
    }

    fn ordered_ulids(count: usize) -> Vec<Ulid> {
        let mut ulids: Vec<Ulid> = (0..count).map(|_| Ulid::new()).collect();
        ulids.sort();
        ulids
    }

    fn location(hash: [u8; 32]) -> BackendLocation {
        let mut hashes = HashMap::new();
        hashes.insert(HASH_MD5.to_string(), hash[..16].to_vec());
        BackendLocation {
            root: "/tmp".to_string(),
            storage_bucket: "objects".to_string(),
            backend_path: "path".to_string(),
            ulid: Ulid::new(),
            compressed: false,
            encrypted: false,
            created_by: created_by(),
            created_at: UNIX_EPOCH + Duration::from_secs(5),
            staging: false,
            partial: false,
            blob_size: 42,
            hashes,
        }
    }

    async fn seed_head(storage_handle: &storage::StorageHandle, key: &str, version_id: Ulid) {
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
    }

    async fn seed_version(
        storage_handle: &storage::StorageHandle,
        key: &str,
        version_id: Ulid,
        version: BlobVersion,
    ) {
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

    async fn seed_materialized(
        storage_handle: &storage::StorageHandle,
        key: &str,
        version_id: Ulid,
        hash: [u8; 32],
    ) {
        seed_version(
            storage_handle,
            key,
            version_id,
            BlobVersion::materialized(
                hash,
                UNIX_EPOCH + Duration::from_secs(5),
                created_by(),
                None,
            ),
        )
        .await;
        let _ = storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_LOCATIONS_KEYSPACE.to_string(),
                key: hash.to_vec().into(),
                value: location(hash).to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;
    }

    fn input(max_keys: usize) -> ListObjectVersionsInput {
        ListObjectVersionsInput {
            bucket: "bucket".to_string(),
            prefix: None,
            delimiter: None,
            key_marker: None,
            version_id_marker: None,
            max_keys: Some(max_keys),
        }
    }

    #[tokio::test]
    async fn list_object_versions_returns_newest_first_with_is_latest() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());

        let versions = ordered_ulids(2);
        let (older, newer) = (versions[0], versions[1]);
        seed_materialized(&storage_handle, "obj", older, [1u8; 32]).await;
        seed_materialized(&storage_handle, "obj", newer, [2u8; 32]).await;
        seed_head(&storage_handle, "obj", newer).await;

        let result = drive(
            ListObjectVersionsOperation::new(input(ListObjectVersionsOperation::DEFAULT_MAX_KEYS)),
            &driver_ctx,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        assert_eq!(result.items.len(), 2);
        match &result.items[0] {
            ListObjectVersionsItem::Version {
                version_id,
                is_latest,
                location,
                ..
            } => {
                assert_eq!(*version_id, newer);
                assert!(*is_latest);
                assert!(location.is_some());
            }
            other => panic!("expected version, got {other:?}"),
        }
        match &result.items[1] {
            ListObjectVersionsItem::Version {
                version_id,
                is_latest,
                ..
            } => {
                assert_eq!(*version_id, older);
                assert!(!*is_latest);
            }
            other => panic!("expected version, got {other:?}"),
        }
        assert!(!result.is_truncated);
    }

    #[tokio::test]
    async fn list_object_versions_includes_delete_markers() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());

        let versions = ordered_ulids(2);
        let (older, newer) = (versions[0], versions[1]);
        seed_materialized(&storage_handle, "obj", older, [3u8; 32]).await;
        seed_version(
            &storage_handle,
            "obj",
            newer,
            BlobVersion::deleted(UNIX_EPOCH + Duration::from_secs(9), created_by()),
        )
        .await;
        seed_head(&storage_handle, "obj", newer).await;

        let result = drive(
            ListObjectVersionsOperation::new(input(ListObjectVersionsOperation::DEFAULT_MAX_KEYS)),
            &driver_ctx,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        assert_eq!(result.items.len(), 2);
        assert!(matches!(
            &result.items[0],
            ListObjectVersionsItem::DeleteMarker {
                version_id,
                is_latest: true,
                ..
            } if *version_id == newer
        ));
        assert!(matches!(
            &result.items[1],
            ListObjectVersionsItem::Version {
                version_id,
                is_latest: false,
                ..
            } if *version_id == older
        ));
    }

    #[tokio::test]
    async fn list_object_versions_resumes_with_markers() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());

        // Two keys, "a" with two versions and "b" with one version.
        let a_versions = ordered_ulids(2);
        seed_materialized(&storage_handle, "a", a_versions[0], [10u8; 32]).await;
        seed_materialized(&storage_handle, "a", a_versions[1], [11u8; 32]).await;
        seed_head(&storage_handle, "a", a_versions[1]).await;
        let b_version = Ulid::new();
        seed_materialized(&storage_handle, "b", b_version, [12u8; 32]).await;
        seed_head(&storage_handle, "b", b_version).await;

        let mut key_marker = None;
        let mut version_id_marker = None;
        let mut collected = Vec::new();
        loop {
            let result = drive(
                ListObjectVersionsOperation::new(ListObjectVersionsInput {
                    bucket: "bucket".to_string(),
                    prefix: None,
                    delimiter: None,
                    key_marker: key_marker.clone(),
                    version_id_marker,
                    max_keys: Some(1),
                }),
                &driver_ctx,
            )
            .await
            .unwrap()
            .unwrap()
            .unwrap();

            for item in &result.items {
                if let ListObjectVersionsItem::Version {
                    key, version_id, ..
                } = item
                {
                    collected.push((key.clone(), *version_id));
                }
            }

            if result.is_truncated {
                key_marker = result.next_key_marker.clone();
                version_id_marker = result.next_version_id_marker;
                assert!(key_marker.is_some());
            } else {
                break;
            }
        }

        assert_eq!(
            collected,
            vec![
                ("a".to_string(), a_versions[1]),
                ("a".to_string(), a_versions[0]),
                ("b".to_string(), b_version),
            ]
        );
    }

    #[tokio::test]
    async fn list_object_versions_groups_by_delimiter() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());

        for (index, key) in ["a.txt", "dir/1", "dir/2", "z.txt"].iter().enumerate() {
            let version_id = Ulid::new();
            seed_materialized(&storage_handle, key, version_id, [index as u8 + 20; 32]).await;
            seed_head(&storage_handle, key, version_id).await;
        }

        let result = drive(
            ListObjectVersionsOperation::new(ListObjectVersionsInput {
                bucket: "bucket".to_string(),
                prefix: None,
                delimiter: Some("/".to_string()),
                key_marker: None,
                version_id_marker: None,
                max_keys: Some(ListObjectVersionsOperation::DEFAULT_MAX_KEYS),
            }),
            &driver_ctx,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        let keys: Vec<&str> = result
            .items
            .iter()
            .map(|item| match item {
                ListObjectVersionsItem::Version { key, .. }
                | ListObjectVersionsItem::DeleteMarker { key, .. } => key.as_str(),
            })
            .collect();
        assert_eq!(keys, vec!["a.txt", "z.txt"]);
        assert_eq!(result.common_prefixes, vec!["dir/"]);
        assert!(!result.is_truncated);
    }

    #[tokio::test]
    async fn list_object_versions_returns_reference_metadata() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());

        let version_id = Ulid::new();
        let source_metadata = SourceMetadata {
            content_length: 42,
            content_type: Some("text/plain".to_string()),
            etag: Some("ref-etag".to_string()),
            last_modified: Some(UNIX_EPOCH + Duration::from_secs(10)),
            source_version: None,
        };
        seed_version(
            &storage_handle,
            "ref",
            version_id,
            BlobVersion::reference(
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
                UNIX_EPOCH + Duration::from_secs(5),
                created_by(),
                UNIX_EPOCH + Duration::from_secs(20),
            ),
        )
        .await;
        seed_head(&storage_handle, "ref", version_id).await;

        let result = drive(
            ListObjectVersionsOperation::new(input(ListObjectVersionsOperation::DEFAULT_MAX_KEYS)),
            &driver_ctx,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        assert_eq!(result.items.len(), 1);
        match &result.items[0] {
            ListObjectVersionsItem::Version {
                location,
                source_metadata: metadata,
                is_latest,
                ..
            } => {
                assert!(location.is_none());
                assert_eq!(metadata.as_ref(), Some(&source_metadata));
                assert!(*is_latest);
            }
            other => panic!("expected version, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn list_object_versions_orders_keys_lexicographically() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());

        for (index, key) in ["b", "aa", "a/1"].iter().enumerate() {
            let version_id = Ulid::new();
            seed_materialized(&storage_handle, key, version_id, [index as u8 + 40; 32]).await;
            seed_head(&storage_handle, key, version_id).await;
        }

        let result = drive(
            ListObjectVersionsOperation::new(input(ListObjectVersionsOperation::DEFAULT_MAX_KEYS)),
            &driver_ctx,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        let keys: Vec<&str> = result
            .items
            .iter()
            .map(|item| match item {
                ListObjectVersionsItem::Version { key, .. }
                | ListObjectVersionsItem::DeleteMarker { key, .. } => key.as_str(),
            })
            .collect();
        assert_eq!(keys, vec!["a/1", "aa", "b"]);
    }

    #[tokio::test]
    async fn list_object_versions_truncates_at_scan_round_cap() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());

        // Three delimiter groups; each head round makes bounded progress so the
        // low scan-round cap trips before the "c/" group is reached.
        for (group, count) in [("a/", 4usize), ("b/", 3), ("c/", 1)] {
            for index in 0..count {
                let key = format!("{group}{index}");
                let version_id = Ulid::new();
                seed_materialized(&storage_handle, &key, version_id, [index as u8 + 60; 32]).await;
                seed_head(&storage_handle, &key, version_id).await;
            }
        }

        let first = drive(
            ListObjectVersionsOperation::new(ListObjectVersionsInput {
                bucket: "bucket".to_string(),
                prefix: None,
                delimiter: Some("/".to_string()),
                key_marker: None,
                version_id_marker: None,
                max_keys: Some(3),
            })
            .with_max_scan_rounds(2),
            &driver_ctx,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        assert!(first.is_truncated);
        assert_eq!(first.common_prefixes, vec!["a/", "b/"]);
        assert_eq!(first.next_key_marker.as_deref(), Some("b/"));

        let second = drive(
            ListObjectVersionsOperation::new(ListObjectVersionsInput {
                bucket: "bucket".to_string(),
                prefix: None,
                delimiter: Some("/".to_string()),
                key_marker: first.next_key_marker.clone(),
                version_id_marker: first.next_version_id_marker,
                max_keys: Some(3),
            })
            .with_max_scan_rounds(2),
            &driver_ctx,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        assert!(!second.is_truncated);
        assert_eq!(second.common_prefixes, vec!["c/"]);
    }

    #[tokio::test]
    async fn list_object_versions_keeps_newest_versions_past_scan_limit() {
        let temp_handle = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(temp_handle.path().to_str().unwrap()).unwrap();
        let driver_ctx = driver_context(storage_handle.clone());

        // Seed more versions than the scan limit; the rolling window must keep
        // the newest ones (including the is_latest head), not the oldest page.
        let versions = ordered_ulids(5);
        for (index, version_id) in versions.iter().enumerate() {
            seed_materialized(&storage_handle, "obj", *version_id, [index as u8 + 80; 32]).await;
        }
        seed_head(&storage_handle, "obj", versions[4]).await;

        let result = drive(
            ListObjectVersionsOperation::new(input(ListObjectVersionsOperation::DEFAULT_MAX_KEYS))
                .with_version_scan_limit(3),
            &driver_ctx,
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        let observed: Vec<(Ulid, bool)> = result
            .items
            .iter()
            .map(|item| match item {
                ListObjectVersionsItem::Version {
                    version_id,
                    is_latest,
                    ..
                }
                | ListObjectVersionsItem::DeleteMarker {
                    version_id,
                    is_latest,
                    ..
                } => (*version_id, *is_latest),
            })
            .collect();
        assert_eq!(
            observed,
            vec![
                (versions[4], true),
                (versions[3], false),
                (versions[2], false),
            ]
        );
    }
}
