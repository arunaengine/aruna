use std::collections::VecDeque;
use std::time::Duration;

use aruna_core::effects::{DhtEffect, Effect, IterStart, NetEffect, StorageEffect};
use aruna_core::errors::{ConversionError, DhtError, StorageError};
use aruna_core::events::{DhtEntry, DhtEvent, Event, NetEvent, StorageEvent};
use aruna_core::id::DhtKeyId;
use aruna_core::keyspaces::BLOB_LOCATIONS_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::{RealmId, RoCrateLimits};
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::types::{Effects, Key, NodeId};
use smallvec::smallvec;
use thiserror::Error;

use crate::replication::util::dht_registration_effect;

const HOLDER_REFRESH_PAGE_SIZE: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq)]
enum RefreshState {
    Init,
    ReadPage,
    Publish,
    Schedule,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum RefreshBlobHoldersError {
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

#[derive(Debug, PartialEq)]
pub struct RefreshBlobHoldersOperation {
    realm_id: RealmId,
    node_id: NodeId,
    limits: RoCrateLimits,
    state: RefreshState,
    pending: VecDeque<DhtKeyId>,
    next_start: Option<Key>,
    refreshed: usize,
    output: Option<Result<usize, RefreshBlobHoldersError>>,
}

impl RefreshBlobHoldersOperation {
    pub fn new(realm_id: RealmId, node_id: NodeId, limits: RoCrateLimits) -> Self {
        Self {
            realm_id,
            node_id,
            limits,
            state: RefreshState::Init,
            pending: VecDeque::new(),
            next_start: None,
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
            limit: HOLDER_REFRESH_PAGE_SIZE,
            txn_id: None,
        })]
    }

    fn next_effect(&mut self) -> Effects {
        if let Some(key) = self.pending.pop_front() {
            self.state = RefreshState::Publish;
            self.refreshed = self.refreshed.saturating_add(1);
            return match dht_registration_effect(
                key.as_bytes(),
                self.realm_id,
                self.node_id,
                &self.limits,
            ) {
                Ok(effect) => smallvec![effect],
                Err(error) => self.fail(error.into()),
            };
        }
        if let Some(start) = self.next_start.take() {
            return self.read_page(Some(start));
        }

        self.state = RefreshState::Finish;
        self.output = Some(Ok(self.refreshed));
        smallvec![]
    }

    fn fail(&mut self, error: RefreshBlobHoldersError) -> Effects {
        self.state = RefreshState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn unexpected(&mut self, expected: &'static str, event: Event) -> Effects {
        let state = format!("{:?}", self.state);
        self.fail(RefreshBlobHoldersError::UnexpectedEvent {
            state,
            expected,
            got: format!("{event:?}"),
        })
    }
}

impl Operation for RefreshBlobHoldersOperation {
    type Output = usize;
    type Error = RefreshBlobHoldersError;

    fn start(&mut self) -> Effects {
        self.state = RefreshState::Schedule;
        smallvec![Effect::Task(TaskEffect::ResetTimer {
            key: TaskKey::RefreshBlobHolders,
            after: Duration::from_millis(self.limits.holder_refresh_ms),
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            RefreshState::ReadPage => match event {
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }) => {
                    self.pending
                        .extend(values.into_iter().filter_map(|(key, _)| {
                            <[u8; 32]>::try_from(key.as_ref())
                                .ok()
                                .map(DhtKeyId::from_bytes)
                        }));
                    self.next_start = next_start_after;
                    self.next_effect()
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected("blob location page", other),
            },
            RefreshState::Publish => match event {
                Event::Net(NetEvent::Dht(DhtEvent::PutComplete { .. }))
                | Event::Net(NetEvent::Dht(DhtEvent::Error { .. }))
                | Event::Net(NetEvent::Error(_)) => self.next_effect(),
                other => self.unexpected("DHT put result", other),
            },
            RefreshState::Schedule => match event {
                Event::Task(TaskEvent::TimerScheduled {
                    key: TaskKey::RefreshBlobHolders,
                    ..
                }) => self.read_page(None),
                Event::Task(TaskEvent::Error { message, .. }) => {
                    self.fail(RefreshBlobHoldersError::Schedule(message))
                }
                other => self.unexpected("blob holder timer result", other),
            },
            RefreshState::Init | RefreshState::Finish | RefreshState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, RefreshState::Finish | RefreshState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .expect("blob holder refresh operation must set output")
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
pub enum GetBlobHoldersError {
    #[error(transparent)]
    Dht(#[from] DhtError),
    #[error("unexpected event in state {state:?}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: String,
    },
}

#[derive(Debug, PartialEq)]
pub struct GetBlobHoldersOperation {
    key: DhtKeyId,
    realm_id: RealmId,
    self_node_id: NodeId,
    state: GetState,
    output: Option<Result<Vec<NodeId>, GetBlobHoldersError>>,
}

impl GetBlobHoldersOperation {
    pub fn new(blake3: [u8; 32], realm_id: RealmId, self_node_id: NodeId) -> Self {
        Self {
            key: DhtKeyId::from_bytes(blake3),
            realm_id,
            self_node_id,
            state: GetState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: GetBlobHoldersError) -> Effects {
        self.state = GetState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn unexpected(&mut self, expected: &'static str, event: Event) -> Effects {
        let state = format!("{:?}", self.state);
        self.fail(GetBlobHoldersError::UnexpectedEvent {
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

impl Operation for GetBlobHoldersOperation {
    type Output = Vec<NodeId>;
    type Error = GetBlobHoldersError;

    fn start(&mut self) -> Effects {
        self.state = GetState::Read;
        smallvec![Effect::Net(NetEffect::Dht(DhtEffect::Get {
            key: self.key,
            realm_filter: Some(self.realm_id),
        }))]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            GetState::Read => match event {
                Event::Net(NetEvent::Dht(DhtEvent::GetResult { key, values }))
                    if key == self.key =>
                {
                    self.finish(values)
                }
                Event::Net(NetEvent::Dht(DhtEvent::Error { error })) => self.fail(error.into()),
                other => self.unexpected("DHT get result", other),
            },
            GetState::Init | GetState::Finish | GetState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, GetState::Finish | GetState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .expect("blob holder get operation must set output")
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use aruna_core::events::DhtEntry;
    use aruna_core::operation::Operation;

    use super::*;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
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
        let effect = dht_registration_effect(&[2; 32], realm_id, node(3), &limits).unwrap();

        assert!(matches!(
            effect,
            Effect::Net(NetEffect::Dht(DhtEffect::Put {
                ttl,
                value,
                ..
            })) if ttl == Duration::from_secs(90) && value.is_empty()
        ));
    }

    #[test]
    fn refreshes_blob_pages() {
        let realm_id = RealmId::from_bytes([1; 32]);
        let node_id = node(2);
        let limits = RoCrateLimits {
            holder_ttl_ms: 90_000,
            holder_refresh_ms: 30_000,
            ..RoCrateLimits::default()
        };
        let mut operation = RefreshBlobHoldersOperation::new(realm_id, node_id, limits);

        let schedule = operation.start();
        assert_eq!(
            schedule.as_slice(),
            [Effect::Task(TaskEffect::ResetTimer {
                key: TaskKey::RefreshBlobHolders,
                after: Duration::from_secs(30),
            })]
        );
        let first = operation.step(Event::Task(TaskEvent::TimerScheduled {
            key: TaskKey::RefreshBlobHolders,
            after: Duration::from_secs(30),
        }));
        assert!(matches!(
            first.as_slice(),
            [Effect::Storage(StorageEffect::Iter {
                key_space,
                limit: HOLDER_REFRESH_PAGE_SIZE,
                start: None,
                ..
            })] if key_space == BLOB_LOCATIONS_KEYSPACE
        ));

        let cursor: Key = vec![1; 32].into();
        let publish = operation.step(Event::Storage(StorageEvent::IterResult {
            values: vec![(vec![1; 32].into(), Vec::<u8>::new().into())],
            next_start_after: Some(cursor.clone()),
        }));
        assert!(matches!(
            publish.as_slice(),
            [Effect::Net(NetEffect::Dht(DhtEffect::Put {
                ttl,
                ..
            }))] if *ttl == Duration::from_secs(90)
        ));

        let next_page = operation.step(Event::Net(NetEvent::Dht(DhtEvent::PutComplete {
            key: DhtKeyId::from_bytes([1; 32]),
            remote_attempt_count: 1,
            remote_store_count: 1,
        })));
        assert!(matches!(
            next_page.as_slice(),
            [Effect::Storage(StorageEffect::Iter {
                start: Some(IterStart::After(start)),
                ..
            })] if start == &cursor
        ));

        let publish = operation.step(Event::Storage(StorageEvent::IterResult {
            values: vec![(vec![2; 32].into(), Vec::<u8>::new().into())],
            next_start_after: None,
        }));
        assert_eq!(publish.len(), 1);
        let finish = operation.step(Event::Net(NetEvent::Dht(DhtEvent::Error {
            error: DhtError::Other("offline".to_string()),
        })));
        assert!(finish.is_empty());
        assert_eq!(operation.finalize(), Ok(2));
    }

    #[test]
    fn holders_are_sorted() {
        let realm_id = RealmId::from_bytes([1; 32]);
        let self_node = node(9);
        let mut expected = vec![node(3), node(1), node(2)];
        expected.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        let mut operation = GetBlobHoldersOperation::new([4; 32], realm_id, self_node);
        operation.start();
        operation.step(Event::Net(NetEvent::Dht(DhtEvent::GetResult {
            key: DhtKeyId::from_bytes([4; 32]),
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
        let mut operation = GetBlobHoldersOperation::new([4; 32], realm_id, self_node);
        operation.start();
        operation.step(Event::Net(NetEvent::Dht(DhtEvent::GetResult {
            key: DhtKeyId::from_bytes([4; 32]),
            values: vec![
                entry(self_node, realm_id),
                entry(other, realm_id),
                entry(other, realm_id),
            ],
        })));

        assert_eq!(operation.finalize(), Ok(vec![other]));
    }
}
