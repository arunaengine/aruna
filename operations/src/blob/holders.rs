//! Republishes the blob locations of this node to the DHT and looks up blob holders.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{DhtEffect, DhtGetOptions, Effect, NetEffect};
use aruna_core::errors::{ConversionError, DhtError, StorageError};
use aruna_core::events::{DhtEntry, DhtEvent, Event, NetEvent};
use aruna_core::id::DhtKeyId;
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{
    BLOB_LOCATIONS_KEYSPACE, NODE_STATE_KEYSPACE, PATHS_INDEX_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::rows::read_row;
use aruna_core::structs::execution::job::RoCrateLimits;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{BlobLocationKey, BucketInfo, HashIndex};
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::types::{Key, Value};
use thiserror::Error;

const REFRESH_PAGE_SIZE: usize = 256;
/// Owner rows of one hash read to tell whether only distributed buckets hold it.
const OWNER_PROBE: usize = 8;
/// Bucket answers remembered by one refresh run before the memory is cleared.
const BUCKET_MEMORY: usize = 1024;
/// The node state row that keeps the refresh position across restarts.
const REFRESH_CURSOR_KEY: &[u8] = b"holder_refresh_cursor";

#[derive(Debug, Clone, PartialEq, Eq)]
enum RefreshState {
    Schedule,
    ReadCursor,
    ReadPage,
    ReadOwners,
    ReadBuckets,
    Publish,
    SaveCursor,
}

#[derive(Debug, Error, PartialEq)]
pub enum RefreshHoldersError {
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("failed to schedule blob holder refresh: {0}")]
    Schedule(String),
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

impl From<IoError> for RefreshHoldersError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.into())
    }
}

type RefreshTurn = Result<Next<usize>, RefreshHoldersError>;

/// Republishes the hashes of this node's blob locations page by page from a durable cursor.
/// A hash whose owners all sit in distributed buckets is left out: those are found through
/// their content indexes.
#[derive(Debug, PartialEq)]
pub struct RefreshHoldersOperation {
    realm_id: RealmId,
    limits: RoCrateLimits,
    state: RefreshState,
    pending: VecDeque<DhtKeyId>,
    next_start: Option<Key>,
    last_published: Option<DhtKeyId>,
    /// The hash whose owners are being read, and their bucket names.
    current: Option<(DhtKeyId, Vec<String>)>,
    /// The buckets of the running read, in the order asked.
    asked_buckets: Vec<String>,
    /// Whether a bucket of this node is distributed.
    distributed: HashMap<String, bool>,
    refreshed: usize,
}

impl RefreshHoldersOperation {
    pub fn new(realm_id: RealmId, limits: RoCrateLimits) -> Self {
        Self {
            realm_id,
            limits,
            state: RefreshState::Schedule,
            pending: VecDeque::new(),
            next_start: None,
            last_published: None,
            current: None,
            asked_buckets: Vec::new(),
            distributed: HashMap::new(),
            refreshed: 0,
        }
    }

    fn call(&mut self, state: RefreshState, call: IoCall) -> RefreshTurn {
        self.state = state;
        Ok(Next::Call(call))
    }

    fn read_page(&mut self, after: Option<Key>) -> RefreshTurn {
        let page = IoCall::Page {
            space: BLOB_LOCATIONS_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after,
            limit: REFRESH_PAGE_SIZE,
        };
        self.call(RefreshState::ReadPage, page)
    }

    /// Reads the owner rows of the next hash, or saves the cursor after a page.
    fn next_call(&mut self) -> RefreshTurn {
        if let Some(key) = self.pending.pop_front() {
            let prefix = HashIndex::hash_prefix(key.as_bytes())?;
            self.current = Some((key, Vec::new()));
            let page = IoCall::Page {
                space: PATHS_INDEX_KEYSPACE.to_string(),
                prefix: prefix.into(),
                after: None,
                limit: OWNER_PROBE,
            };
            return self.call(RefreshState::ReadOwners, page);
        }
        let key = Key::from(REFRESH_CURSOR_KEY.to_vec());
        let space = NODE_STATE_KEYSPACE.to_string();
        let save = match self.next_start.clone() {
            Some(start) => IoCall::Write(vec![(space, key, start.to_vec().into())]),
            None => IoCall::Delete(vec![(space, key)]),
        };
        self.call(RefreshState::SaveCursor, save)
    }

    fn page_read(&mut self, values: Vec<(Key, Value)>, next: Option<Key>) -> RefreshTurn {
        // Copies of one hash sort together, so one publish per hash
        // is enough even though each backend has its own entry.
        for (key, _) in values {
            let Ok(location) = BlobLocationKey::from_bytes(key.as_ref()) else {
                continue;
            };
            let dht_key = DhtKeyId::from_bytes(location.blake3_hash);
            if self.last_published.replace(dht_key) != Some(dht_key) {
                self.pending.push_back(dht_key);
            }
        }
        self.next_start = next;
        self.next_call()
    }

    fn owners_read(&mut self, values: Vec<(Key, Value)>, more: bool) -> RefreshTurn {
        // Owners beyond the probe are unknown, so the hash stays announced.
        if more {
            return self.decide();
        }
        let mut buckets: Vec<String> = values
            .iter()
            .filter_map(|(key, _)| HashIndex::from_bytes(key.as_ref()).ok())
            .map(|row| row.bucket)
            .collect();
        buckets.sort();
        buckets.dedup();
        let unknown: Vec<String> = buckets
            .iter()
            .filter(|bucket| !self.distributed.contains_key(*bucket))
            .cloned()
            .collect();
        if let Some((_, owners)) = self.current.as_mut() {
            *owners = buckets;
        }
        if unknown.is_empty() {
            return self.decide();
        }
        if self.distributed.len() >= BUCKET_MEMORY {
            self.distributed.clear();
        }
        let reads = unknown
            .iter()
            .map(|bucket| {
                (
                    S3_BUCKET_KEYSPACE.to_string(),
                    bucket.clone().into_bytes().into(),
                )
            })
            .collect();
        self.asked_buckets = unknown;
        self.call(RefreshState::ReadBuckets, IoCall::Read(reads))
    }

    fn buckets_read(&mut self, values: Vec<Option<Value>>) -> RefreshTurn {
        let asked = std::mem::take(&mut self.asked_buckets);
        for (bucket, value) in asked.into_iter().zip(values) {
            // A bucket that is gone or unreadable counts as ordinary, so its hash stays announced.
            let distributed = value
                .and_then(|value| BucketInfo::from_bytes(value.as_ref()).ok())
                .is_some_and(|info| info.distribution.is_some());
            self.distributed.insert(bucket, distributed);
        }
        self.decide()
    }

    /// Publishes the current hash unless every owner row names a distributed bucket.
    fn decide(&mut self) -> RefreshTurn {
        let Some((key, owners)) = self.current.take() else {
            return self.next_call();
        };
        let only_distributed = !owners.is_empty()
            && owners
                .iter()
                .all(|bucket| self.distributed.get(bucket).copied().unwrap_or(false));
        if only_distributed {
            return self.next_call();
        }
        self.refreshed = self.refreshed.saturating_add(1);
        let put = dht_registration_effect(key.as_bytes(), self.realm_id, &self.limits)?;
        self.call(RefreshState::Publish, put.into())
    }

    fn unexpected(&self, expected: &'static str, event: Event) -> RefreshTurn {
        Err(RefreshHoldersError::UnexpectedEvent {
            state: format!("{:?}", self.state),
            expected,
            got: event.label().to_string(),
        })
    }

    fn timer_set(&mut self, event: Event) -> RefreshTurn {
        match event {
            Event::Task(TaskEvent::TimerScheduled {
                key: TaskKey::RefreshBlobHolders,
                ..
            }) => {
                let read = read_row(NODE_STATE_KEYSPACE, REFRESH_CURSOR_KEY.to_vec());
                self.call(RefreshState::ReadCursor, read)
            }
            Event::Task(TaskEvent::Error { message, .. }) => {
                Err(RefreshHoldersError::Schedule(message))
            }
            other => self.unexpected("blob holder timer result", other),
        }
    }

    fn published(&mut self, event: Event) -> RefreshTurn {
        match event {
            Event::Net(NetEvent::Dht(DhtEvent::PutComplete { .. }))
            | Event::Net(NetEvent::Dht(DhtEvent::Error { .. }))
            | Event::Net(NetEvent::Error(_)) => self.next_call(),
            other => self.unexpected("DHT put result", other),
        }
    }
}

impl Operation for RefreshHoldersOperation {
    type Output = usize;
    type Error = RefreshHoldersError;

    fn first(&mut self) -> RefreshTurn {
        let reset = Effect::Task(TaskEffect::ResetTimer {
            key: TaskKey::RefreshBlobHolders,
            after: Duration::from_millis(self.limits.holder_refresh_ms),
        });
        self.call(RefreshState::Schedule, reset.into())
    }

    fn next(&mut self, result: IoResult) -> RefreshTurn {
        match self.state {
            RefreshState::Schedule => self.timer_set(result.event()?),
            RefreshState::ReadCursor => {
                let cursor = result.value()?;
                self.read_page(cursor.map(|value| Key::from(value.to_vec())))
            }
            RefreshState::ReadPage => {
                let (values, next) = result.page()?;
                self.page_read(values, next)
            }
            RefreshState::ReadOwners => {
                let (values, next) = result.page()?;
                self.owners_read(values, next.is_some())
            }
            RefreshState::ReadBuckets => self.buckets_read(result.values()?),
            RefreshState::Publish => self.published(result.event()?),
            RefreshState::SaveCursor => {
                result.done()?;
                match self.next_start.take() {
                    Some(start) => self.read_page(Some(start)),
                    None => Ok(Next::Done(self.refreshed)),
                }
            }
        }
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum GetHoldersError {
    #[error(transparent)]
    Dht(#[from] DhtError),
    #[error("blob holder lookup did not complete")]
    Aborted,
    #[error("unexpected event while looking up blob holders: expected {expected}, got {got}")]
    UnexpectedEvent { expected: &'static str, got: String },
    #[error("blob holder lookup failed: {0}")]
    Io(IoError),
}

/// A deadline must not look like an empty holder set: the caller reports the gap instead of
/// claiming it enumerated every copy.
impl From<IoError> for GetHoldersError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Aborted | IoError::Hung { .. } => Self::Aborted,
            other => Self::Io(other),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct GetHoldersOperation {
    key: DhtKeyId,
    realm_id: RealmId,
    self_node_id: NodeId,
}

impl GetHoldersOperation {
    pub fn new(blake3: [u8; 32], realm_id: RealmId, self_node_id: NodeId) -> Self {
        Self {
            key: DhtKeyId::from_bytes(blake3),
            realm_id,
            self_node_id,
        }
    }

    fn holders(&self, values: Vec<DhtEntry>) -> Vec<NodeId> {
        let mut holders = values
            .into_iter()
            .map(|entry| entry.node_id)
            .filter(|node_id| *node_id != self.self_node_id)
            .collect::<Vec<_>>();
        holders.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        holders.dedup();
        holders
    }
}

impl Operation for GetHoldersOperation {
    type Output = Vec<NodeId>;
    type Error = GetHoldersError;

    fn first(&mut self) -> Result<Next<Vec<NodeId>>, GetHoldersError> {
        let get = Effect::Net(NetEffect::Dht(DhtEffect::Get {
            key: self.key,
            realm_filter: Some(self.realm_id),
            // Holder discovery must see every publisher, so it stays exhaustive
            // under the default read budget.
            options: DhtGetOptions::default(),
        }));
        Ok(Next::Call(get.into()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Vec<NodeId>>, GetHoldersError> {
        match result.event()? {
            Event::Net(NetEvent::Dht(DhtEvent::GetResult { key, values, .. }))
                if key == self.key =>
            {
                Ok(Next::Done(self.holders(values)))
            }
            Event::Net(NetEvent::Dht(DhtEvent::Error { error })) => Err(error.into()),
            other => Err(GetHoldersError::UnexpectedEvent {
                expected: "DHT get result",
                got: other.label().to_string(),
            }),
        }
    }
}

/// The DHT put that announces this node as a holder of the blob with hash `blake3`.
pub(crate) fn dht_registration_effect(
    blake3: &[u8],
    local_realm_id: RealmId,
    limits: &RoCrateLimits,
) -> Result<Effect, ConversionError> {
    let key = DhtKeyId::from_bytes(blake3.try_into()?);
    Ok(Effect::Net(NetEffect::Dht(DhtEffect::Put {
        key,
        realm_id: local_realm_id,
        value: Vec::new(),
        ttl: Duration::from_millis(limits.holder_ttl_ms),
    })))
}

#[cfg(test)]
mod pure_tests {
    use aruna_core::events::DhtEntry;
    use aruna_core::structs::storage::format::EncodingClass;

    use super::*;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    use aruna_core::structs::storage::blob::BackendRef;

    fn location_key(seed: u8) -> Vec<u8> {
        BlobLocationKey::new([seed; 32], EncodingClass::Raw, BackendRef::node_default()).to_bytes()
    }

    /// A second copy of the same hash, so the scan must publish it only once.
    fn cold_key(seed: u8) -> Vec<u8> {
        BlobLocationKey::new(
            [seed; 32],
            EncodingClass::Raw,
            BackendRef::Node("cold".to_string()),
        )
        .to_bytes()
    }

    fn entry(node_id: NodeId, realm_id: RealmId) -> DhtEntry {
        DhtEntry {
            node_id,
            realm_id,
            value: Vec::new(),
            expires_at: 100,
        }
    }

    #[test]
    fn registration_uses_ttl() {
        let realm_id = RealmId::from_bytes([1; 32]);
        let limits = RoCrateLimits {
            holder_ttl_ms: 90_000,
            holder_refresh_ms: 30_000,
            ..RoCrateLimits::default()
        };
        let effect = dht_registration_effect(&[2; 32], realm_id, &limits).unwrap();

        assert!(matches!(
            effect,
            Effect::Net(NetEffect::Dht(DhtEffect::Put {
                ttl,
                value,
                ..
            })) if ttl == Duration::from_secs(90) && value.is_empty()
        ));
    }

    fn owner_row(bucket: &str, seed: u8) -> Key {
        let realm_id = RealmId::from_bytes([1; 32]);
        let row = HashIndex::new(
            [seed; 32],
            ulid::Ulid::from_bytes([seed; 16]),
            realm_id,
            ulid::Ulid::from_bytes([3; 16]),
            node(5),
            bucket,
            "key",
        );
        row.to_bytes().unwrap().into()
    }

    #[test]
    fn refreshes_blob_pages() {
        let realm_id = RealmId::from_bytes([1; 32]);
        let limits = RoCrateLimits {
            holder_ttl_ms: 90_000,
            holder_refresh_ms: 30_000,
            ..RoCrateLimits::default()
        };
        let mut operation = RefreshHoldersOperation::new(realm_id, limits);

        let schedule = operation.start();
        assert_eq!(
            schedule.as_slice(),
            [Effect::Task(TaskEffect::ResetTimer {
                key: TaskKey::RefreshBlobHolders,
                after: Duration::from_secs(30),
            })]
        );
        let cursor_read = operation.step(Event::Task(TaskEvent::TimerScheduled {
            key: TaskKey::RefreshBlobHolders,
            after: Duration::from_secs(30),
        }));
        assert!(matches!(
            cursor_read.as_slice(),
            [Effect::Storage(StorageEffect::Read { key_space, .. })]
                if key_space == NODE_STATE_KEYSPACE
        ));
        let first = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: REFRESH_CURSOR_KEY.to_vec().into(),
            value: None,
        }));
        assert!(matches!(
            first.as_slice(),
            [Effect::Storage(StorageEffect::Iter {
                key_space,
                limit: REFRESH_PAGE_SIZE,
                start: None,
                ..
            })] if key_space == BLOB_LOCATIONS_KEYSPACE
        ));

        let cursor: Key = location_key(1).into();
        let owners = operation.step(Event::Storage(StorageEvent::IterResult {
            values: vec![
                (location_key(1).into(), Vec::<u8>::new().into()),
                (cold_key(1).into(), Vec::<u8>::new().into()),
            ],
            next_start_after: Some(cursor.clone()),
        }));
        assert!(matches!(
            owners.as_slice(),
            [Effect::Storage(StorageEffect::Iter { key_space, .. })]
                if key_space == PATHS_INDEX_KEYSPACE
        ));
        // A hash without owner rows is announced, as before.
        let publish = operation.step(Event::Storage(StorageEvent::IterResult {
            values: Vec::new(),
            next_start_after: None,
        }));
        assert!(matches!(
            publish.as_slice(),
            [Effect::Net(NetEffect::Dht(DhtEffect::Put {
                ttl,
                ..
            }))] if *ttl == Duration::from_secs(90)
        ));

        let save = operation.step(Event::Net(NetEvent::Dht(DhtEvent::PutComplete {
            key: DhtKeyId::from_bytes([1; 32]),
            remote_attempt_count: 1,
            remote_store_count: 1,
        })));
        assert!(matches!(
            save.as_slice(),
            [Effect::Storage(StorageEffect::Write { key_space, value, .. })]
                if key_space == NODE_STATE_KEYSPACE && value.as_ref() == cursor.as_ref()
        ));
        let next_page = operation.step(Event::Storage(StorageEvent::WriteResult {
            key: REFRESH_CURSOR_KEY.to_vec().into(),
        }));
        assert!(matches!(
            next_page.as_slice(),
            [Effect::Storage(StorageEffect::Iter {
                start: Some(IterStart::After(start)),
                ..
            })] if start == &cursor
        ));

        operation.step(Event::Storage(StorageEvent::IterResult {
            values: vec![(location_key(2).into(), Vec::<u8>::new().into())],
            next_start_after: None,
        }));
        operation.distributed.insert("shared".to_string(), true);
        let clear = operation.step(Event::Storage(StorageEvent::IterResult {
            values: vec![(owner_row("shared", 2), Vec::<u8>::new().into())],
            next_start_after: None,
        }));
        assert!(
            matches!(
                clear.as_slice(),
                [Effect::Storage(StorageEffect::Delete { key_space, .. })]
                    if key_space == NODE_STATE_KEYSPACE
            ),
            "a hash held only by distributed buckets is not announced"
        );
        let finish = operation.step(Event::Storage(StorageEvent::DeleteResult {
            key: REFRESH_CURSOR_KEY.to_vec().into(),
        }));
        assert!(finish.is_empty());
        assert_eq!(operation.finalize(), Ok(1));
    }

    // Owners past the probe may be ordinary buckets, so a truncated probe keeps the hash.
    #[test]
    fn truncated_owners_announce() {
        let realm_id = RealmId::from_bytes([1; 32]);
        let mut operation = RefreshHoldersOperation::new(realm_id, RoCrateLimits::default());
        operation.distributed.insert("shared".to_string(), true);
        operation.pending.push_back(DhtKeyId::from_bytes([4; 32]));
        operation.next_effect();
        let publish = operation.step(Event::Storage(StorageEvent::IterResult {
            values: vec![(owner_row("shared", 4), Vec::<u8>::new().into())],
            next_start_after: Some(owner_row("shared", 4)),
        }));
        assert!(matches!(
            publish.as_slice(),
            [Effect::Net(NetEffect::Dht(DhtEffect::Put { .. }))]
        ));
    }

    #[test]
    fn unknown_bucket_announces() {
        let realm_id = RealmId::from_bytes([1; 32]);
        let mut operation = RefreshHoldersOperation::new(realm_id, RoCrateLimits::default());
        operation.pending.push_back(DhtKeyId::from_bytes([4; 32]));
        operation.next_effect();
        let read = operation.step(Event::Storage(StorageEvent::IterResult {
            values: vec![(owner_row("gone", 4), Vec::<u8>::new().into())],
            next_start_after: None,
        }));
        assert!(matches!(
            read.as_slice(),
            [Effect::Storage(StorageEffect::BatchRead { .. })]
        ));
        let publish = operation.step(Event::Storage(StorageEvent::BatchReadResult {
            values: vec![(b"gone".to_vec().into(), None)],
        }));
        assert!(
            matches!(
                publish.as_slice(),
                [Effect::Net(NetEffect::Dht(DhtEffect::Put { .. }))]
            ),
            "a bucket that cannot be read keeps its hash announced"
        );
    }

    #[test]
    fn holders_are_sorted() {
        let realm_id = RealmId::from_bytes([1; 32]);
        let self_node = node(9);
        let mut expected = vec![node(3), node(1), node(2)];
        expected.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        let mut operation = GetHoldersOperation::new([4; 32], realm_id, self_node);
        operation.start();
        operation.step(Event::Net(NetEvent::Dht(DhtEvent::GetResult {
            key: DhtKeyId::from_bytes([4; 32]),
            stale: false,
            values: vec![
                entry(node(3), realm_id),
                entry(node(1), realm_id),
                entry(node(2), realm_id),
            ],
        })));

        assert_eq!(operation.finalize(), Ok(expected));
    }

    #[test]
    fn holders_exclude_self() {
        let realm_id = RealmId::from_bytes([1; 32]);
        let self_node = node(9);
        let other = node(2);
        let mut operation = GetHoldersOperation::new([4; 32], realm_id, self_node);
        operation.start();
        operation.step(Event::Net(NetEvent::Dht(DhtEvent::GetResult {
            key: DhtKeyId::from_bytes([4; 32]),
            stale: false,
            values: vec![
                entry(self_node, realm_id),
                entry(other, realm_id),
                entry(other, realm_id),
            ],
        })));

        assert_eq!(operation.finalize(), Ok(vec![other]));
    }

    // A state that expects no event must reject one instead of ignoring it.
    #[test]
    fn terminal_rejects_event() {
        let realm_id = RealmId::from_bytes([1; 32]);
        let refresh = || RefreshHoldersOperation::new(realm_id, RoCrateLimits::default());
        let put = || {
            Event::Net(NetEvent::Dht(DhtEvent::PutComplete {
                key: DhtKeyId::from_bytes([1; 32]),
                remote_attempt_count: 1,
                remote_store_count: 1,
            }))
        };
        for (_, effects, error) in rejects_events(
            refresh,
            || {
                let mut events = sample_events();
                events.push(put());
                events
            },
            |_| false,
        ) {
            assert!(effects.is_empty());
            assert!(
                matches!(
                    error,
                    RefreshHoldersError::UnexpectedEvent { .. } | RefreshHoldersError::Storage(_)
                ),
                "{error:?}"
            );
        }

        let get = || GetHoldersOperation::new([4; 32], realm_id, node(9));
        let offline = || {
            Event::Net(NetEvent::Dht(DhtEvent::Error {
                error: DhtError::Other("offline".to_string()),
            }))
        };
        for (_, effects, error) in rejects_events(
            get,
            || {
                let mut events = sample_events();
                events.push(offline());
                events
            },
            |_| false,
        ) {
            assert!(effects.is_empty());
            assert!(
                matches!(error, GetHoldersError::UnexpectedEvent { .. }),
                "{error:?}"
            );
        }
    }
}
