//! Republishes the blob locations of this node to the DHT and looks up blob holders.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use aruna_core::effects::{DhtEffect, DhtGetOptions, Effect, IterStart, NetEffect, StorageEffect};
use aruna_core::errors::{ConversionError, DhtError, StorageError};
use aruna_core::events::{DhtEntry, DhtEvent, Event, NetEvent, StorageEvent};
use aruna_core::id::DhtKeyId;
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{
    BLOB_LOCATIONS_KEYSPACE, NODE_STATE_KEYSPACE, PATHS_INDEX_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::RoCrateLimits;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{BlobLocationKey, BucketInfo, HashIndex};
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::types::{Effects, Key};
use smallvec::smallvec;
use thiserror::Error;

use crate::replication::dht_registration::dht_registration_effect;

const REFRESH_PAGE_SIZE: usize = 256;
/// Owner rows of one hash read to tell whether only distributed buckets hold it.
const OWNER_PROBE: usize = 8;
/// Bucket answers remembered by one refresh run before the memory is cleared.
const BUCKET_MEMORY: usize = 1024;
/// The node state row that keeps the refresh position across restarts.
const REFRESH_CURSOR_KEY: &[u8] = b"holder_refresh_cursor";

#[derive(Debug, Clone, PartialEq, Eq)]
enum RefreshState {
    Init,
    ReadCursor,
    ReadPage,
    ReadOwners,
    ReadBuckets,
    Publish,
    SaveCursor,
    Schedule,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum RefreshHoldersError {
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("failed to schedule blob holder refresh: {0}")]
    Schedule(String),
    #[error("operation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

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
    /// Whether a bucket of this node is distributed.
    distributed: HashMap<String, bool>,
    refreshed: usize,
    output: Option<Result<usize, RefreshHoldersError>>,
}

impl RefreshHoldersOperation {
    pub fn new(realm_id: RealmId, limits: RoCrateLimits) -> Self {
        Self {
            realm_id,
            limits,
            state: RefreshState::Init,
            pending: VecDeque::new(),
            next_start: None,
            last_published: None,
            current: None,
            distributed: HashMap::new(),
            refreshed: 0,
            output: None,
        }
    }

    fn read_page(&mut self, start: Option<Key>) -> Effects {
        self.state = RefreshState::ReadPage;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: BLOB_LOCATIONS_KEYSPACE.to_string(),
            prefix: None,
            start: start.map(IterStart::After),
            limit: REFRESH_PAGE_SIZE,
            txn_id: None,
        })]
    }

    /// Reads the owner rows of the next hash, saves the cursor after a page, or ends the run.
    fn next_effect(&mut self) -> Effects {
        if let Some(key) = self.pending.pop_front() {
            let prefix = match HashIndex::hash_prefix(key.as_bytes()) {
                Ok(prefix) => prefix,
                Err(error) => return self.fail(error.into()),
            };
            self.current = Some((key, Vec::new()));
            self.state = RefreshState::ReadOwners;
            return smallvec![Effect::Storage(StorageEffect::Iter {
                key_space: PATHS_INDEX_KEYSPACE.to_string(),
                prefix: Some(prefix.into()),
                start: None,
                limit: OWNER_PROBE,
                txn_id: None,
            })];
        }
        self.state = RefreshState::SaveCursor;
        let key = Key::from(REFRESH_CURSOR_KEY.to_vec());
        let key_space = NODE_STATE_KEYSPACE.to_string();
        match self.next_start.clone() {
            Some(start) => smallvec![Effect::Storage(StorageEffect::Write {
                key_space,
                key,
                value: start.to_vec().into(),
                txn_id: None,
            })],
            None => smallvec![Effect::Storage(StorageEffect::Delete {
                key_space,
                key,
                txn_id: None,
            })],
        }
    }

    fn owners_read(&mut self, values: Vec<(Key, aruna_core::types::Value)>) -> Effects {
        let mut buckets: Vec<String> = values
            .iter()
            .filter_map(|(key, _)| HashIndex::from_bytes(key.as_ref()).ok())
            .map(|row| row.bucket)
            .collect();
        buckets.sort();
        buckets.dedup();
        let unknown: Vec<(String, Key)> = buckets
            .iter()
            .filter(|bucket| !self.distributed.contains_key(*bucket))
            .map(|bucket| {
                (
                    S3_BUCKET_KEYSPACE.to_string(),
                    bucket.clone().into_bytes().into(),
                )
            })
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
        self.state = RefreshState::ReadBuckets;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: unknown,
            txn_id: None,
        })]
    }

    fn buckets_read(&mut self, values: Vec<(Key, Option<aruna_core::types::Value>)>) -> Effects {
        for (key, value) in values {
            let Ok(bucket) = String::from_utf8(key.to_vec()) else {
                continue;
            };
            // A bucket that is gone or unreadable counts as ordinary, so its hash stays announced.
            let distributed = value
                .and_then(|value| BucketInfo::from_bytes(value.as_ref()).ok())
                .is_some_and(|info| info.distribution.is_some());
            self.distributed.insert(bucket, distributed);
        }
        self.decide()
    }

    /// Publishes the current hash unless every owner row names a distributed bucket.
    fn decide(&mut self) -> Effects {
        let Some((key, owners)) = self.current.take() else {
            return self.next_effect();
        };
        let only_distributed = !owners.is_empty()
            && owners
                .iter()
                .all(|bucket| self.distributed.get(bucket).copied().unwrap_or(false));
        if only_distributed {
            return self.next_effect();
        }
        self.state = RefreshState::Publish;
        self.refreshed = self.refreshed.saturating_add(1);
        match dht_registration_effect(key.as_bytes(), self.realm_id, &self.limits) {
            Ok(effect) => smallvec![effect],
            Err(error) => self.fail(error.into()),
        }
    }

    fn fail(&mut self, error: RefreshHoldersError) -> Effects {
        self.state = RefreshState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn unexpected(&mut self, expected: &'static str, event: Event) -> Effects {
        let state = format!("{:?}", self.state);
        self.fail(RefreshHoldersError::UnexpectedEvent {
            state,
            expected,
            got: format!("{event:?}"),
        })
    }
}

impl Operation for RefreshHoldersOperation {
    type Output = usize;
    type Error = RefreshHoldersError;

    fn start(&mut self) -> Effects {
        self.state = RefreshState::Schedule;
        smallvec![Effect::Task(TaskEffect::ResetTimer {
            key: TaskKey::RefreshBlobHolders,
            after: Duration::from_millis(self.limits.holder_refresh_ms),
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }
        match self.state {
            RefreshState::ReadCursor => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    self.read_page(value.map(|value| Key::from(value.to_vec())))
                }
                other => self.unexpected("refresh cursor", other),
            },
            RefreshState::ReadPage => match event {
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }) => {
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
                    self.next_start = next_start_after;
                    self.next_effect()
                }
                other => self.unexpected("blob location page", other),
            },
            RefreshState::ReadOwners => match event {
                Event::Storage(StorageEvent::IterResult { values, .. }) => self.owners_read(values),
                other => self.unexpected("hash owner rows", other),
            },
            RefreshState::ReadBuckets => match event {
                Event::Storage(StorageEvent::BatchReadResult { values }) => {
                    self.buckets_read(values)
                }
                other => self.unexpected("owner buckets", other),
            },
            RefreshState::Publish => match event {
                Event::Net(NetEvent::Dht(DhtEvent::PutComplete { .. }))
                | Event::Net(NetEvent::Dht(DhtEvent::Error { .. }))
                | Event::Net(NetEvent::Error(_)) => self.next_effect(),
                other => self.unexpected("DHT put result", other),
            },
            RefreshState::SaveCursor => match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => {
                    let start = self.next_start.take();
                    self.read_page(start)
                }
                Event::Storage(StorageEvent::DeleteResult { .. }) => {
                    self.state = RefreshState::Finish;
                    self.output = Some(Ok(self.refreshed));
                    smallvec![]
                }
                other => self.unexpected("refresh cursor write", other),
            },
            RefreshState::Schedule => match event {
                Event::Task(TaskEvent::TimerScheduled {
                    key: TaskKey::RefreshBlobHolders,
                    ..
                }) => {
                    self.state = RefreshState::ReadCursor;
                    smallvec![Effect::Storage(StorageEffect::Read {
                        key_space: NODE_STATE_KEYSPACE.to_string(),
                        key: REFRESH_CURSOR_KEY.to_vec().into(),
                        txn_id: None,
                    })]
                }
                Event::Task(TaskEvent::Error { message, .. }) => {
                    self.fail(RefreshHoldersError::Schedule(message))
                }
                other => self.unexpected("blob holder timer result", other),
            },
            RefreshState::Init | RefreshState::Finish | RefreshState::Error => {
                self.unexpected("no event in a terminal state", event)
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, RefreshState::Finish | RefreshState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(RefreshHoldersError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum GetState {
    Init,
    Read,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum GetHoldersError {
    #[error(transparent)]
    Dht(#[from] DhtError),
    #[error("blob holder lookup did not complete")]
    Aborted,
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

#[derive(Debug, PartialEq)]
pub struct GetHoldersOperation {
    key: DhtKeyId,
    realm_id: RealmId,
    self_node_id: NodeId,
    state: GetState,
    output: Option<Result<Vec<NodeId>, GetHoldersError>>,
}

impl GetHoldersOperation {
    pub fn new(blake3: [u8; 32], realm_id: RealmId, self_node_id: NodeId) -> Self {
        Self {
            key: DhtKeyId::from_bytes(blake3),
            realm_id,
            self_node_id,
            state: GetState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: GetHoldersError) -> Effects {
        self.state = GetState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn unexpected(&mut self, expected: &'static str, event: Event) -> Effects {
        let state = format!("{:?}", self.state);
        self.fail(GetHoldersError::UnexpectedEvent {
            state,
            expected,
            got: format!("{event:?}"),
        })
    }

    fn finish(&mut self, values: Vec<DhtEntry>) -> Effects {
        let mut holders = values
            .into_iter()
            .map(|entry| entry.node_id)
            .filter(|node_id| *node_id != self.self_node_id)
            .collect::<Vec<_>>();
        holders.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        holders.dedup();
        self.state = GetState::Finish;
        self.output = Some(Ok(holders));
        smallvec![]
    }
}

impl Operation for GetHoldersOperation {
    type Output = Vec<NodeId>;
    type Error = GetHoldersError;

    fn start(&mut self) -> Effects {
        self.state = GetState::Read;
        smallvec![Effect::Net(NetEffect::Dht(DhtEffect::Get {
            key: self.key,
            realm_filter: Some(self.realm_id),
            // Holder discovery must see every publisher, so it stays exhaustive
            // under the default read budget.
            options: DhtGetOptions::default(),
        }))]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            GetState::Read => match event {
                Event::Net(NetEvent::Dht(DhtEvent::GetResult { key, values, .. }))
                    if key == self.key =>
                {
                    self.finish(values)
                }
                Event::Net(NetEvent::Dht(DhtEvent::Error { error })) => self.fail(error.into()),
                other => self.unexpected("DHT get result", other),
            },
            GetState::Init | GetState::Finish | GetState::Error => {
                self.unexpected("no event in a terminal state", event)
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, GetState::Finish | GetState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(GetHoldersError::Aborted))
    }

    /// A deadline must not look like an empty holder set: the caller reports the
    /// gap instead of claiming it enumerated every copy.
    fn abort(&mut self) -> Effects {
        self.state = GetState::Error;
        self.output.get_or_insert(Err(GetHoldersError::Aborted));
        smallvec![]
    }
}

#[cfg(test)]
mod pure_tests {
    use aruna_core::events::DhtEntry;
    use aruna_core::operation::Operation;
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
        let mut refresh = RefreshHoldersOperation::new(realm_id, RoCrateLimits::default());

        let effects = refresh.step(Event::Net(NetEvent::Dht(DhtEvent::PutComplete {
            key: DhtKeyId::from_bytes([1; 32]),
            remote_attempt_count: 1,
            remote_store_count: 1,
        })));

        assert!(effects.is_empty());
        assert!(matches!(
            refresh.finalize(),
            Err(RefreshHoldersError::UnexpectedEvent { .. })
        ));

        let mut get = GetHoldersOperation::new([4; 32], realm_id, node(9));

        let effects = get.step(Event::Net(NetEvent::Dht(DhtEvent::Error {
            error: DhtError::Other("offline".to_string()),
        })));

        assert!(effects.is_empty());
        assert!(matches!(
            get.finalize(),
            Err(GetHoldersError::UnexpectedEvent { .. })
        ));
    }
}
