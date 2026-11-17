//! Stores the local craqle replicas of the metadata documents this device keeps.
//! The ledger holds sync progress, registry records and renders so reads work offline.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::OnceLock;

use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{DEVICE_GRAPH_KEYSPACE, DEVICE_REPLICA_KEYSPACE};
use aruna_core::operation::EffectOperation;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::types::{Effects, GroupId, Key, Value};
use byteview::ByteView;
use craqle::VectorClock;
use serde::{Deserialize, Serialize};
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use super::publish_queue::{PublishEntry, PublishState};

/// Bytes of replica records and graphs past which a device keeps no further document it only
/// read, unless the owner set another budget.
pub const DEFAULT_CACHE_BYTES: u64 = 2_000_000_000;

static CACHE_BUDGET: OnceLock<u64> = OnceLock::new();

/// Rows one scan reads at a time.
const REPLICA_PAGE_SIZE: usize = 128;

/// How the document came to be on this device.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaOrigin {
    /// Created here, so the realm learned about it from this device.
    Device,
    /// Fetched from a holder because the owner selected or edited it.
    Realm,
}

/// What the last refresh or forward left the replica in. Pending and failed
/// edits are not stored here: they are the publish queue rows joined onto the record.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaState {
    /// Created on this device and not published yet.
    LocalOnly,
    /// The replica agrees with what the realm last served.
    Synced,
    /// The merged state fails profile validation; the last valid render is
    /// still what this device displays.
    Invalid,
    /// The last refresh did not complete. The replica keeps serving.
    Failed,
}

/// One selected document as this device holds it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaRecord {
    pub document_id: Ulid,
    pub group_id: GroupId,
    pub document_path: String,
    pub graph_iri: String,
    pub origin: ReplicaOrigin,
    /// Whether the owner wants this document available offline. A deselected
    /// record is kept only while it still has edits the realm has not seen.
    pub selected: bool,
    /// The local graph's clock after the last local edit or install.
    pub local_clock: VectorClock,
    /// The holder's clock at the last snapshot this device installed.
    pub realm_clock: VectorClock,
    pub last_synced_ms: Option<u64>,
    pub pending_edits: u32,
    pub state: ReplicaState,
    pub last_error: Option<String>,
    /// The registry record the last refresh carried, so a local read answers
    /// the same shape a holder would.
    pub record: Option<Box<MetadataRegistryRecord>>,
    /// The last valid render of the merged graph, which is what this device
    /// displays and exports.
    pub displayed_jsonld: String,
    pub dataset_digest: Option<[u8; 32]>,
    /// Profile findings the last refresh reported for the merged state.
    pub findings: u32,
}

impl ReplicaRecord {
    /// A replica the owner selected but that has not been refreshed yet.
    pub fn new(
        document_id: Ulid,
        group_id: GroupId,
        document_path: String,
        origin: ReplicaOrigin,
    ) -> Self {
        Self {
            document_id,
            group_id,
            document_path,
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            origin,
            selected: true,
            local_clock: VectorClock::default(),
            realm_clock: VectorClock::default(),
            last_synced_ms: None,
            pending_edits: 0,
            state: match origin {
                ReplicaOrigin::Device => ReplicaState::LocalOnly,
                ReplicaOrigin::Realm => ReplicaState::Synced,
            },
            last_error: None,
            record: None,
            displayed_jsonld: String::new(),
            dataset_digest: None,
            findings: 0,
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }

    /// Whether a local read may be answered from this replica alone.
    pub fn serves_reads(&self) -> bool {
        self.record.is_some() && !self.displayed_jsonld.is_empty()
    }
}

/// What the owner sees for one document in the Sync view.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DocumentState {
    LocalOnly,
    Pending,
    Publishing,
    Invalid,
    Failed,
    Synced,
}

impl DocumentState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::LocalOnly => "local_only",
            Self::Pending => "pending",
            Self::Publishing => "publishing",
            Self::Invalid => "invalid",
            Self::Failed => "failed",
            Self::Synced => "synced",
        }
    }
}

/// Joins one replica with the publish entries that go onto it. A failure
/// the owner has to answer outranks work still in flight, which outranks the
/// state the last refresh left.
pub fn document_state(replica: &ReplicaRecord, entries: &[&PublishEntry]) -> DocumentState {
    let mut queued = None;
    for entry in entries {
        match &entry.state {
            PublishState::Failed { .. } => return DocumentState::Failed,
            PublishState::Pending { .. } => queued = Some(DocumentState::Pending),
            PublishState::Publishing { .. } => {
                queued.get_or_insert(DocumentState::Publishing);
            }
            PublishState::Published { .. } => {}
        }
    }
    if let Some(queued) = queued {
        return queued;
    }
    match replica.state {
        ReplicaState::LocalOnly => DocumentState::LocalOnly,
        ReplicaState::Invalid => DocumentState::Invalid,
        ReplicaState::Failed => DocumentState::Failed,
        ReplicaState::Synced => DocumentState::Synced,
    }
}

pub(crate) fn replica_key(document_id: Ulid) -> Key {
    ByteView::from(document_id.to_bytes().to_vec())
}

pub(crate) fn replica_entry(
    replica: &ReplicaRecord,
) -> Result<(String, Key, Value), ConversionError> {
    Ok((
        DEVICE_REPLICA_KEYSPACE.to_string(),
        replica_key(replica.document_id),
        ByteView::from(replica.to_bytes()?),
    ))
}

pub(crate) fn replica_read(document_id: Ulid) -> StorageEffect {
    StorageEffect::Read {
        key_space: DEVICE_REPLICA_KEYSPACE.to_string(),
        key: replica_key(document_id),
        txn_id: None,
    }
}

pub(crate) fn replica_delete(document_id: Ulid) -> StorageEffect {
    StorageEffect::Delete {
        key_space: DEVICE_REPLICA_KEYSPACE.to_string(),
        key: replica_key(document_id),
        txn_id: None,
    }
}

/// The replica a read answer holds; an absent, unreadable or undecodable row is none.
pub(crate) fn read_answer(event: &Event) -> Option<ReplicaRecord> {
    match event {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(bytes), ..
        }) => ReplicaRecord::from_bytes(bytes).ok(),
        _ => None,
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum SelectionError {
    #[error("the realm does not know this document")]
    NotFound,
    #[error("{0}")]
    Refused(String),
    #[error("the document has edits this device has not published yet")]
    PendingEdits,
    #[error("the device store is unavailable")]
    Unavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RowState {
    Init,
    Read,
    Write,
    Finish,
}

/// Stops keeping one document offline by dropping its replica row. The local graph is left in
/// place: it is an OR-Set replica, so selecting again joins onto it instead of resurrecting
/// anything, and deleting it would tombstone the graph on this device.
#[derive(Debug, PartialEq)]
pub struct DeselectDocumentOperation {
    document_id: Ulid,
    state: RowState,
    replica: Option<ReplicaRecord>,
    output: Option<Result<ReplicaRecord, SelectionError>>,
}

impl DeselectDocumentOperation {
    pub fn new(document_id: Ulid) -> Self {
        Self {
            document_id,
            state: RowState::Init,
            replica: None,
            output: None,
        }
    }

    fn finish(&mut self, output: Result<ReplicaRecord, SelectionError>) -> Effects {
        self.state = RowState::Finish;
        self.output = Some(output);
        smallvec![]
    }
}

impl EffectOperation for DeselectDocumentOperation {
    type Output = ReplicaRecord;
    type Error = SelectionError;

    fn start(&mut self) -> Effects {
        self.state = RowState::Read;
        smallvec![Effect::Storage(replica_read(self.document_id))]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (self.state, &event) {
            (
                RowState::Read,
                Event::Storage(StorageEvent::ReadResult { .. } | StorageEvent::Error { .. }),
            ) => match read_answer(&event) {
                None => self.finish(Err(SelectionError::NotFound)),
                Some(replica) if replica.pending_edits > 0 => {
                    self.finish(Err(SelectionError::PendingEdits))
                }
                Some(replica) => {
                    self.replica = Some(replica);
                    self.state = RowState::Write;
                    smallvec![Effect::Storage(replica_delete(self.document_id))]
                }
            },
            (RowState::Write, Event::Storage(StorageEvent::DeleteResult { .. })) => {
                let replica = self.replica.take().ok_or(SelectionError::Unavailable);
                self.finish(replica.map(|replica| ReplicaRecord {
                    selected: false,
                    ..replica
                }))
            }
            _ => self.finish(Err(SelectionError::Unavailable)),
        }
    }

    fn is_complete(&self) -> bool {
        self.state == RowState::Finish
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(SelectionError::Unavailable))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

/// Selects a document this device created, once the realm has accepted it, unless this device
/// already holds a replica of it. A failed write is ignored and leaves the document untracked.
#[derive(Debug, PartialEq)]
pub struct TrackCreatedOperation {
    replica: ReplicaRecord,
    state: RowState,
    output: Option<Result<(), SelectionError>>,
}

impl TrackCreatedOperation {
    pub fn new(document_id: Ulid, group_id: GroupId, document_path: String) -> Self {
        let mut replica =
            ReplicaRecord::new(document_id, group_id, document_path, ReplicaOrigin::Device);
        replica.state = ReplicaState::LocalOnly;
        Self {
            replica,
            state: RowState::Init,
            output: None,
        }
    }

    fn finish(&mut self, output: Result<(), SelectionError>) -> Effects {
        self.state = RowState::Finish;
        self.output = Some(output);
        smallvec![]
    }
}

impl EffectOperation for TrackCreatedOperation {
    type Output = ();
    type Error = SelectionError;

    fn start(&mut self) -> Effects {
        self.state = RowState::Read;
        smallvec![Effect::Storage(replica_read(self.replica.document_id))]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (self.state, &event) {
            (
                RowState::Read,
                Event::Storage(StorageEvent::ReadResult { .. } | StorageEvent::Error { .. }),
            ) => {
                if read_answer(&event).is_some() {
                    return self.finish(Ok(()));
                }
                let Ok((key_space, key, value)) = replica_entry(&self.replica) else {
                    warn!(document_id = %self.replica.document_id, "Failed to encode a device replica");
                    return self.finish(Ok(()));
                };
                self.state = RowState::Write;
                smallvec![Effect::Storage(StorageEffect::Write {
                    key_space,
                    key,
                    value,
                    txn_id: None,
                })]
            }
            (
                RowState::Write,
                Event::Storage(StorageEvent::WriteResult { .. } | StorageEvent::Error { .. }),
            ) => self.finish(Ok(())),
            _ => self.finish(Err(SelectionError::Unavailable)),
        }
    }

    fn is_complete(&self) -> bool {
        self.state == RowState::Finish
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(SelectionError::Unavailable))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum ReplicaScanError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("unexpected {0} while scanning the device replicas")]
    UnexpectedEvent(&'static str),
    #[error("scanning the device replicas did not finish")]
    NotFinished,
}

fn scan_page(key_space: &str, start_after: Option<Key>) -> Effects {
    smallvec![Effect::Storage(StorageEffect::Iter {
        key_space: key_space.to_string(),
        prefix: None,
        start: start_after.map(IterStart::After),
        limit: REPLICA_PAGE_SIZE,
        txn_id: None,
    })]
}

/// The rows and the cursor of one scanned page.
fn scanned_page(event: Event) -> Result<(Vec<(Key, Value)>, Option<Key>), ReplicaScanError> {
    match event {
        Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) => Ok((values, next_start_after)),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        event => Err(ReplicaScanError::UnexpectedEvent(event.label())),
    }
}

/// Lists every replica this device holds, in document order. An undecodable row is skipped,
/// as a document this device does not hold.
#[derive(Debug, Default, PartialEq)]
pub struct ListReplicasOperation {
    started: bool,
    replicas: Vec<ReplicaRecord>,
    output: Option<Result<Vec<ReplicaRecord>, ReplicaScanError>>,
}

impl ListReplicasOperation {
    pub fn new() -> Self {
        Self::default()
    }
}

impl EffectOperation for ListReplicasOperation {
    type Output = Vec<ReplicaRecord>;
    type Error = ReplicaScanError;

    fn start(&mut self) -> Effects {
        self.started = true;
        scan_page(DEVICE_REPLICA_KEYSPACE, None)
    }

    fn step(&mut self, event: Event) -> Effects {
        let page = match self.started {
            true => scanned_page(event),
            false => Err(ReplicaScanError::UnexpectedEvent(event.label())),
        };
        match page {
            Ok((values, next)) => {
                let decoded = values
                    .into_iter()
                    .filter_map(|(_, bytes)| ReplicaRecord::from_bytes(&bytes).ok());
                self.replicas.extend(decoded);
                match next {
                    Some(next) => scan_page(DEVICE_REPLICA_KEYSPACE, Some(next)),
                    None => {
                        self.output = Some(Ok(std::mem::take(&mut self.replicas)));
                        smallvec![]
                    }
                }
            }
            Err(error) => {
                self.output = Some(Err(error));
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(ReplicaScanError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

/// Measures the bytes this device's replica rows and installed graphs take. A graph whose
/// replica row is gone still counts: nothing removes it locally yet.
#[derive(Debug, Default, PartialEq)]
pub struct CacheUsageOperation {
    replicas: ListReplicasOperation,
    graphs: Option<u64>,
    output: Option<Result<u64, ReplicaScanError>>,
}

impl CacheUsageOperation {
    pub fn new() -> Self {
        Self::default()
    }

    fn finish(&mut self, output: Result<u64, ReplicaScanError>) -> Effects {
        self.output = Some(output);
        smallvec![]
    }
}

impl EffectOperation for CacheUsageOperation {
    type Output = u64;
    type Error = ReplicaScanError;

    fn start(&mut self) -> Effects {
        self.replicas.start()
    }

    fn step(&mut self, event: Event) -> Effects {
        let Some(graphs) = self.graphs else {
            let effects = self.replicas.step(event);
            if !self.replicas.is_complete() {
                return effects;
            }
            return match std::mem::take(&mut self.replicas).finalize() {
                Ok(replicas) => {
                    self.graphs = Some(replicas.iter().map(replica_size).sum());
                    scan_page(DEVICE_GRAPH_KEYSPACE, None)
                }
                Err(error) => self.finish(Err(error)),
            };
        };
        match scanned_page(event) {
            Ok((values, next)) => {
                let sizes = values
                    .iter()
                    .filter_map(|(_, value)| <[u8; 8]>::try_from(value.as_ref()).ok())
                    .map(u64::from_be_bytes);
                let total = sizes.fold(graphs, u64::saturating_add);
                self.graphs = Some(total);
                match next {
                    Some(next) => scan_page(DEVICE_GRAPH_KEYSPACE, Some(next)),
                    None => self.finish(Ok(total)),
                }
            }
            Err(error) => self.finish(Err(error)),
        }
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(ReplicaScanError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

/// Sets the owner's cache budget once at startup.
pub fn set_cache_budget(bytes: u64) {
    let _ = CACHE_BUDGET.set(bytes);
}

pub fn cache_budget() -> u64 {
    CACHE_BUDGET.get().copied().unwrap_or(DEFAULT_CACHE_BYTES)
}

/// The bytes one replica record takes, its render included.
pub(crate) fn replica_size(replica: &ReplicaRecord) -> u64 {
    replica
        .to_bytes()
        .map(|bytes| bytes.len() as u64)
        .unwrap_or(0)
}

/// Records that this device now holds an edit the realm has not seen.
pub fn mark_edited(replica: &mut ReplicaRecord, local_clock: VectorClock) {
    replica.local_clock = local_clock;
    replica.pending_edits = replica.pending_edits.saturating_add(1);
    replica.selected = true;
}

#[cfg(test)]
mod pure_tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::operation_check::{
        check_trace, rejects_events, rejects_finalize, sample_events,
    };
    use aruna_core::structs::identity::realm::RealmId;

    fn replica() -> ReplicaRecord {
        ReplicaRecord::new(
            Ulid::from_bytes([2u8; 16]),
            Ulid::from_bytes([1u8; 16]),
            "notes".to_string(),
            ReplicaOrigin::Realm,
        )
    }

    fn entry(state: PublishState) -> PublishEntry {
        let mut entry = PublishEntry::new(
            Ulid::from_parts(1, 1),
            UserId::local(Ulid::from_parts(2, 2), RealmId::from_bytes([8u8; 32])),
            Ulid::from_bytes([1u8; 16]),
            "notes".to_string(),
            false,
            "{}".to_string(),
        );
        entry.state = state;
        entry
    }

    #[test]
    fn round_trips_replica() {
        let mut replica = replica();
        replica.displayed_jsonld = r#"{"@graph":[]}"#.to_string();
        replica.dataset_digest = Some([4u8; 32]);
        replica.last_synced_ms = Some(17);
        let decoded = ReplicaRecord::from_bytes(&replica.to_bytes().unwrap()).unwrap();
        assert_eq!(decoded, replica);
        assert_eq!(
            replica_key(replica.document_id).as_ref(),
            replica.document_id.to_bytes().as_slice()
        );
    }

    #[test]
    fn reports_queued_work() {
        // What the owner still has to act on outranks work in flight, which
        // outranks the state the last refresh left.
        let replica = replica();
        let pending = entry(PublishState::Pending {
            due_at_ms: 0,
            attempts: 1,
            last_error: None,
        });
        let publishing = entry(PublishState::Publishing {
            document_id: replica.document_id,
            due_at_ms: 0,
            attempts: 1,
        });
        let failed = entry(PublishState::Failed {
            reason: "denied".to_string(),
            retryable: false,
            document_id: Some(replica.document_id),
        });

        assert_eq!(document_state(&replica, &[]), DocumentState::Synced);
        assert_eq!(
            document_state(&replica, &[&publishing]),
            DocumentState::Publishing
        );
        assert_eq!(
            document_state(&replica, &[&publishing, &pending]),
            DocumentState::Pending
        );
        assert_eq!(
            document_state(&replica, &[&pending, &failed]),
            DocumentState::Failed
        );
    }

    #[test]
    fn reports_replica_state() {
        // A published entry is history: the record's own state answers again.
        let mut replica = replica();
        let published = entry(PublishState::Published {
            document_id: replica.document_id,
        });
        replica.state = ReplicaState::Invalid;
        assert_eq!(
            document_state(&replica, &[&published]),
            DocumentState::Invalid
        );
        replica.state = ReplicaState::LocalOnly;
        assert_eq!(document_state(&replica, &[]), DocumentState::LocalOnly);
    }

    fn page(values: Vec<(Key, Value)>, next: Option<&[u8]>) -> Event {
        Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after: next.map(|key| key.to_vec().into()),
        })
    }

    fn row(replica: &ReplicaRecord) -> (Key, Value) {
        let (_, key, value) = replica_entry(replica).unwrap();
        (key, value)
    }

    // Pages are read until the cursor ends; an undecodable row is skipped.
    #[test]
    fn lists_every_page() {
        let held = replica();
        let mut list = ListReplicasOperation::new();
        let next = scan_page(DEVICE_REPLICA_KEYSPACE, Some(b"a".to_vec().into()));
        check_trace(
            &mut list,
            &scan_page(DEVICE_REPLICA_KEYSPACE, None),
            vec![
                (page(vec![row(&held)], Some(b"a")), next.into_vec()),
                (
                    page(vec![(b"b".to_vec().into(), Vec::new().into())], None),
                    Vec::new(),
                ),
            ],
        );
        assert_eq!(list.finalize(), Ok(vec![held]));
    }

    // Replica rows count by their size and graphs by their stored size, over all pages.
    #[test]
    fn sums_cache_usage() {
        let held = replica();
        let graph = |size: u64| -> (Key, Value) {
            (b"g".to_vec().into(), size.to_be_bytes().to_vec().into())
        };
        let graphs = scan_page(DEVICE_GRAPH_KEYSPACE, None);
        let next = scan_page(DEVICE_GRAPH_KEYSPACE, Some(b"g".to_vec().into()));
        let mut usage = CacheUsageOperation::new();
        check_trace(
            &mut usage,
            &scan_page(DEVICE_REPLICA_KEYSPACE, None),
            vec![
                (page(vec![row(&held)], None), graphs.into_vec()),
                (page(vec![graph(4_000)], Some(b"g")), next.into_vec()),
                (page(vec![graph(6)], None), Vec::new()),
            ],
        );
        assert_eq!(usage.finalize(), Ok(replica_size(&held) + 4_006));
    }

    // A failed scan fails both operations; other events are refused and nothing finishes early.
    #[test]
    fn scan_failures_fail() {
        let error = || {
            Event::Storage(StorageEvent::Error {
                error: StorageError::Timeout,
            })
        };
        let mut list = ListReplicasOperation::new();
        list.start();
        list.step(error());
        assert_eq!(list.finalize(), Err(StorageError::Timeout.into()));
        let mut usage = CacheUsageOperation::new();
        usage.start();
        usage.step(page(Vec::new(), None));
        usage.step(error());
        assert_eq!(usage.finalize(), Err(StorageError::Timeout.into()));

        let all = sample_events().len();
        let lists = rejects_events(ListReplicasOperation::new, sample_events, |_| false);
        let usages = rejects_events(CacheUsageOperation::new, sample_events, |_| false);
        assert_eq!((lists.len(), usages.len()), (all, all));
        rejects_finalize(ListReplicasOperation::new());
        rejects_finalize(CacheUsageOperation::new());
        assert!(ListReplicasOperation::new().abort().is_empty());
        assert!(CacheUsageOperation::new().abort().is_empty());
    }

    fn stored(replica: Option<&ReplicaRecord>) -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: replica.map(|replica| replica.to_bytes().unwrap().into()),
        })
    }

    // A deselected replica row is deleted and answered unselected; pending edits keep it.
    #[test]
    fn deselect_drops_row() {
        let held = replica();
        let id = held.document_id;
        let mut deselect = DeselectDocumentOperation::new(id);
        let deleted = Event::Storage(StorageEvent::DeleteResult {
            key: replica_key(id),
        });
        check_trace(
            &mut deselect,
            &[Effect::Storage(replica_read(id))],
            vec![
                (
                    stored(Some(&held)),
                    vec![Effect::Storage(replica_delete(id))],
                ),
                (deleted, Vec::new()),
            ],
        );
        let unselected = ReplicaRecord {
            selected: false,
            ..held.clone()
        };
        assert_eq!(deselect.finalize(), Ok(unselected));

        let edited = ReplicaRecord {
            pending_edits: 1,
            ..held
        };
        let mut deselect = DeselectDocumentOperation::new(id);
        deselect.start();
        assert!(deselect.step(stored(Some(&edited))).is_empty());
        assert_eq!(deselect.finalize(), Err(SelectionError::PendingEdits));
        let mut deselect = DeselectDocumentOperation::new(id);
        deselect.start();
        deselect.step(stored(None));
        assert_eq!(deselect.finalize(), Err(SelectionError::NotFound));
    }

    // A created document gets a local-only row, unless a replica row is already there.
    #[test]
    fn tracks_created_once() {
        let group_id = Ulid::from_bytes([1u8; 16]);
        let make =
            || TrackCreatedOperation::new(Ulid::from_bytes([2u8; 16]), group_id, "notes".into());
        let mut expected = replica();
        expected.origin = ReplicaOrigin::Device;
        expected.state = ReplicaState::LocalOnly;
        let (key_space, key, value) = replica_entry(&expected).unwrap();
        let write = Effect::Storage(StorageEffect::Write {
            key_space,
            key: key.clone(),
            value,
            txn_id: None,
        });
        let mut track = make();
        let read = [Effect::Storage(replica_read(expected.document_id))];
        let written = Event::Storage(StorageEvent::WriteResult { key });
        check_trace(
            &mut track,
            &read,
            vec![(stored(None), vec![write]), (written, Vec::new())],
        );
        assert_eq!(track.finalize(), Ok(()));
        let mut track = make();
        check_trace(
            &mut track,
            &read,
            vec![(stored(Some(&expected)), Vec::new())],
        );
        assert_eq!(track.finalize(), Ok(()));
    }

    // Other events fail both operations with an unavailable store; nothing finishes early.
    #[test]
    fn selection_rejects_unexpected() {
        let id = Ulid::from_bytes([2u8; 16]);
        let deselect = || DeselectDocumentOperation::new(id);
        let track = || TrackCreatedOperation::new(id, Ulid::from_bytes([1u8; 16]), "notes".into());
        let all = sample_events().len();
        let refused = rejects_events(deselect, sample_events, |_| false);
        assert!(
            refused
                .iter()
                .all(|(_, _, error)| *error == SelectionError::Unavailable)
        );
        assert_eq!(refused.len(), all);
        assert_eq!(rejects_events(track, sample_events, |_| false).len(), all);
        rejects_finalize(deselect());
        rejects_finalize(track());
        assert!(deselect().abort().is_empty());
        assert!(track().abort().is_empty());
    }

    #[test]
    fn counts_local_edits() {
        let mut replica = replica();
        replica.selected = false;
        mark_edited(&mut replica, VectorClock::default());
        mark_edited(&mut replica, VectorClock::default());
        assert_eq!(replica.pending_edits, 2);
        assert!(replica.selected, "an edited document is selected again");
    }
}
