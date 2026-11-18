//! Reads one user's vault heads or public key records, locally on a holder or from the holders.
//! A miss on every reached holder is an empty answer; no answer at all is unavailable.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::DocumentTarget;
use aruna_core::effects::{HolderList, MAX_FETCH_HOLDERS, VaultFetchEffect, VaultQuery};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, NetEvent, VaultFetchEvent};
use aruna_core::keyspaces::{USER_KEY_KEYSPACE, VAULT_REVISION_KEYSPACE};
use aruna_core::operation::{Operation, SubOperation, boxed_suboperation};
use aruna_core::rows::{event, page, values};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::identity::user::vault::{
    MAX_KEY_RECORDS, MAX_VAULT_HEADS, UserKeyRecord, VaultRecords, VaultRevision,
    retain_newest_keys, user_record_prefix,
};
use aruna_core::types::{Key, Value};
use aruna_core::{NodeId, UserId};
use thiserror::Error;
use ulid::Ulid;

use aruna_core::placement::{
    PlacementResolveError, holds_placement, plan_target_placement, read_holder_sets,
};

use crate::forward::fetch::{Answer, Budget, Fetched, HolderFetchOperation, HolderRecord};
use crate::metadata::protocol::{MetadataReadError, MetadataTransportMessage};

#[derive(Debug, Clone, PartialEq)]
pub struct ReadVaultConfig {
    pub node_id: NodeId,
    /// The realm that stores the vault: the serving realm, also for a user of another realm.
    pub realm_id: RealmId,
    pub user_id: UserId,
    pub query: VaultQuery,
    pub deadline: Duration,
}

#[derive(Debug, Error, PartialEq)]
pub enum ReadVaultError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    PlacementResolve(#[from] PlacementResolveError),
    #[error("realm config document missing")]
    RealmConfigMissing,
    #[error("no placement strategy governs user vaults")]
    PlacementUnavailable,
    /// A holder did not accept the forwarded caller.
    #[error("a vault holder refused the caller")]
    Denied,
    /// No holder answered; this never means the records are absent.
    #[error("no vault holder answered: {0}")]
    Unavailable(String),
    #[error("a vault holder answered with records of another user or kind")]
    ForeignRecords,
    #[error("operation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: expected {expected}, got {got}")]
    UnexpectedEvent {
        state: String,
        expected: &'static str,
        got: &'static str,
    },
}

impl From<IoError> for ReadVaultError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::Storage(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::Storage(error),
            IoError::Unexpected { expected, got } => Self::UnexpectedEvent {
                state: "ReadVault".to_string(),
                expected,
                got,
            },
            _ => Self::NotFinished,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stage {
    Config,
    Local,
    Fetch,
}

type ReadTurn = Result<Next<VaultRecords>, ReadVaultError>;

/// Reads the user's records from this node when it holds the vault placement,
/// otherwise from the resolved holders through one fetch child.
#[derive(Debug, PartialEq)]
pub struct ReadVaultOperation {
    config: ReadVaultConfig,
    stage: Stage,
    keys: Vec<UserKeyRecord>,
}

impl ReadVaultOperation {
    pub fn new(config: ReadVaultConfig) -> Self {
        Self {
            config,
            stage: Stage::Config,
            keys: Vec::new(),
        }
    }

    fn reads_heads(&self) -> bool {
        matches!(self.config.query, VaultQuery::Heads { .. })
    }

    fn route(&mut self, value: Option<Value>) -> ReadTurn {
        let config =
            RealmConfigDocument::from_bytes(&value.ok_or(ReadVaultError::RealmConfigMissing)?)?;
        let ReadVaultConfig {
            realm_id, user_id, ..
        } = self.config;
        let target = if self.reads_heads() {
            DocumentTarget::VaultRevision {
                realm_id,
                user_id,
                revision_id: Ulid::nil(),
            }
        } else {
            DocumentTarget::UserKey {
                realm_id,
                user_id,
                record_id: Ulid::nil(),
            }
        };
        let plan = plan_target_placement(&config, &target, Default::default())?
            .ok_or(ReadVaultError::PlacementUnavailable)?;
        if holds_placement(&config, &plan.placement, self.config.node_id) {
            self.stage = Stage::Local;
            return Ok(Next::Call(page_call(user_id, self.reads_heads(), None)));
        }
        let mut holders = read_holder_sets(&config, &plan.placement)?;
        holders.retain(|holder| *holder != self.config.node_id);
        holders.truncate(MAX_FETCH_HOLDERS);
        let holders = HolderList::new(holders)
            .map_err(|_| ReadVaultError::Unavailable("the vault has no holder".to_string()))?;
        self.stage = Stage::Fetch;
        Ok(Next::Call(IoCall::Child(fetch_child(&VaultFetchEffect {
            holders,
            user_id,
            query: self.config.query.clone(),
            deadline: self.config.deadline,
        }))))
    }

    /// Keeps the newest key records over every page; heads are bounded by one page.
    fn local(&mut self, result: IoResult) -> ReadTurn {
        let (rows, next) = page(result)?;
        let keys = match decode_page(self.reads_heads(), rows)? {
            VaultRecords::Keys(keys) => keys,
            heads => return Ok(Next::Done(heads)),
        };
        self.keys.extend(keys);
        retain_newest_keys(&mut self.keys);
        Ok(match next {
            Some(after) => Next::Call(page_call(self.config.user_id, false, Some(after))),
            None => Next::Done(VaultRecords::Keys(std::mem::take(&mut self.keys))),
        })
    }

    /// A fetched answer is accepted only for this user, realm and query, signed by its origin.
    fn fetched(&self, event: VaultFetchEvent) -> Result<VaultRecords, ReadVaultError> {
        let ReadVaultConfig {
            realm_id, user_id, ..
        } = self.config;
        match event {
            VaultFetchEvent::Fetched { records, .. } => {
                let fits = match (&records, self.reads_heads()) {
                    (VaultRecords::Heads(heads), true) => heads.iter().all(|head| {
                        head.user_id == user_id
                            && head.realm_id == realm_id
                            && head.validate().is_ok()
                            && head.origin_signed()
                    }),
                    (VaultRecords::Keys(keys), false) => keys.iter().all(|key| {
                        key.user_id == user_id
                            && key.realm_id == realm_id
                            && key.validate().is_ok()
                            && key.origin_signed()
                    }),
                    _ => false,
                };
                if !fits || records.len() > MAX_KEY_RECORDS.max(MAX_VAULT_HEADS) {
                    return Err(ReadVaultError::ForeignRecords);
                }
                Ok(records)
            }
            VaultFetchEvent::NotFound if self.reads_heads() => Ok(VaultRecords::Heads(Vec::new())),
            VaultFetchEvent::NotFound => Ok(VaultRecords::Keys(Vec::new())),
            VaultFetchEvent::Denied => Err(ReadVaultError::Denied),
            VaultFetchEvent::Unavailable(reason) => Err(ReadVaultError::Unavailable(reason)),
        }
    }
}

impl Operation for ReadVaultOperation {
    type Output = VaultRecords;
    type Error = ReadVaultError;

    fn first(&mut self) -> ReadTurn {
        let target = DocumentTarget::RealmConfig {
            realm_id: self.config.realm_id,
        };
        Ok(Next::Call(IoCall::Read(vec![(
            target.storage_keyspace().to_string(),
            target.storage_key(),
        )])))
    }

    fn next(&mut self, result: IoResult) -> ReadTurn {
        match self.stage {
            Stage::Config => {
                let [value] = values::<1>(result)?;
                self.route(value)
            }
            Stage::Local => self.local(result),
            Stage::Fetch => match event(result)? {
                Event::Net(NetEvent::VaultFetch(fetched)) => Ok(Next::Done(self.fetched(fetched)?)),
                other => Err(ReadVaultError::UnexpectedEvent {
                    state: "fetch".to_string(),
                    expected: "Event::Net(NetEvent::VaultFetch)",
                    got: other.label(),
                }),
            },
        }
    }
}

/// Reads one user's vault heads, or all its newest key records page by page, on this holder.
#[derive(Debug, PartialEq)]
pub(crate) struct LocalRecordsOperation {
    user_id: UserId,
    heads: bool,
    keys: Vec<UserKeyRecord>,
}

impl LocalRecordsOperation {
    pub(crate) fn new(user_id: UserId, heads: bool) -> Self {
        Self {
            user_id,
            heads,
            keys: Vec::new(),
        }
    }

    fn page(&self, after: Option<Key>) -> Next<VaultRecords> {
        Next::Call(page_call(self.user_id, self.heads, after))
    }
}

impl Operation for LocalRecordsOperation {
    type Output = VaultRecords;
    type Error = MetadataReadError;

    fn first(&mut self) -> Result<Next<VaultRecords>, MetadataReadError> {
        Ok(self.page(None))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<VaultRecords>, MetadataReadError> {
        let IoResult::Page { rows, next } = result else {
            return Err(MetadataReadError::Unavailable);
        };
        match decode_page(self.heads, rows) {
            Ok(VaultRecords::Keys(keys)) => {
                self.keys.extend(keys);
                retain_newest_keys(&mut self.keys);
                Ok(match next {
                    Some(after) => self.page(Some(after)),
                    None => Next::Done(VaultRecords::Keys(std::mem::take(&mut self.keys))),
                })
            }
            Ok(heads) => Ok(Next::Done(heads)),
            Err(_) => Err(MetadataReadError::Unavailable),
        }
    }

    /// A runner failure is answered as unavailable, never retried as a conflict.
    fn is_conflict(_error: &MetadataReadError) -> bool {
        false
    }
}

/// The first or a following page of one user's vault heads or key records on this node.
fn page_call(user_id: UserId, heads: bool, after: Option<Key>) -> IoCall {
    let (space, limit) = match heads {
        true => (VAULT_REVISION_KEYSPACE, MAX_VAULT_HEADS),
        false => (USER_KEY_KEYSPACE, MAX_KEY_RECORDS),
    };
    IoCall::Page {
        space: space.to_string(),
        prefix: user_record_prefix(user_id),
        after,
        limit,
    }
}

fn decode_page(heads: bool, rows: Vec<(Key, Value)>) -> Result<VaultRecords, ConversionError> {
    let values = rows.iter().map(|(_, value)| value.as_ref());
    Ok(match heads {
        true => VaultRecords::Heads(
            values
                .map(VaultRevision::from_bytes)
                .collect::<Result<_, _>>()?,
        ),
        false => VaultRecords::Keys(
            values
                .map(UserKeyRecord::from_bytes)
                .collect::<Result<_, _>>()?,
        ),
    })
}

impl HolderRecord for VaultRecords {
    const LABEL: &'static str = "vault";

    fn read(message: MetadataTransportMessage) -> Answer<Self> {
        match message {
            MetadataTransportMessage::FetchedVaultRecords {
                result: Ok(records),
            } if records.is_empty() => Answer::Missing,
            MetadataTransportMessage::FetchedVaultRecords {
                result: Ok(records),
            } => Answer::Found(records),
            MetadataTransportMessage::FetchedVaultRecords {
                result: Err(MetadataReadError::Unauthorized | MetadataReadError::Forbidden),
            } => Answer::Denied,
            _ => Answer::Skip,
        }
    }
}

/// Asks the holders in rank order, each with its share of the deadline; the answer arrives as a
/// vault fetch event.
fn fetch_child(fetch: &VaultFetchEffect) -> Box<dyn SubOperation> {
    let request = MetadataTransportMessage::FetchVaultRecords {
        user_id: fetch.user_id,
        query: fetch.query.clone(),
    };
    let holders = fetch.holders.as_slice();
    let operation =
        HolderFetchOperation::<VaultRecords>::new(holders, &request, fetch.deadline, Budget::Split);
    boxed_suboperation(operation, |result| {
        Event::Net(NetEvent::VaultFetch(match result {
            Ok(Fetched::Found { holder, value }) => VaultFetchEvent::Fetched {
                holder,
                records: value,
            },
            Ok(Fetched::NotFound) => VaultFetchEvent::NotFound,
            Ok(Fetched::Denied) => VaultFetchEvent::Denied,
            Ok(Fetched::Unavailable(reason)) => VaultFetchEvent::Unavailable(reason),
            Err(error) => VaultFetchEvent::Unavailable(error.to_string()),
        }))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::effects::Effect;
    use aruna_core::structs::identity::auth::Actor;
    use aruna_core::structs::identity::realm::{RealmId, RealmNodeKind};
    use aruna_core::structs::placement::record::PlacementRef;
    use aruna_core::types::Effects;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn user(seed: u8) -> UserId {
        UserId::local(Ulid::from_bytes([seed; 16]), RealmId::from_bytes([3; 32]))
    }

    /// Four servers and two vault replicas, so the realm has holders and non-holders.
    fn realm() -> (Value, Vec<NodeId>) {
        let mut config = RealmConfigDocument::new(user(7).realm_id, Vec::new(), 2);
        config.seed_default_placement();
        for seed in 1..=4 {
            config.ensure_node(node(seed), RealmNodeKind::Server);
        }
        let target = DocumentTarget::UserKey {
            realm_id: user(7).realm_id,
            user_id: user(7),
            record_id: Ulid::nil(),
        };
        let holders = plan_target_placement(&config, &target, Default::default())
            .unwrap()
            .unwrap()
            .holders;
        let actor = Actor {
            node_id: node(1),
            user_id: user(7),
            realm_id: user(7).realm_id,
        };
        (config.to_bytes(&actor).unwrap().into(), holders)
    }

    /// Starts a read on a holder or, with `holds` false, on a node outside the holder set.
    fn started(holds: bool, query: VaultQuery) -> (ReadVaultOperation, ReadTurn, Vec<NodeId>) {
        let (config, holders) = realm();
        let node_id = if holds {
            holders[0]
        } else {
            (1..=4)
                .map(node)
                .find(|node| !holders.contains(node))
                .unwrap()
        };
        let mut operation = ReadVaultOperation::new(ReadVaultConfig {
            node_id,
            realm_id: user(7).realm_id,
            user_id: user(7),
            query,
            deadline: Duration::from_secs(10),
        });
        assert!(matches!(operation.first(), Ok(Next::Call(IoCall::Read(_)))));
        let turn = operation.next(IoResult::Values(vec![Some(config)]));
        (operation, turn, holders)
    }

    fn fetched(operation: &mut ReadVaultOperation, fetched: VaultFetchEvent) -> ReadTurn {
        let event = Event::Net(NetEvent::VaultFetch(fetched));
        operation.next(IoResult::Event(Box::new(event)))
    }

    #[test]
    fn fetches_from_holders() {
        let (mut operation, turn, holders) = started(false, VaultQuery::Keys);
        let Ok(Next::Call(IoCall::Child(child))) = turn else {
            panic!("expected a holder fetch child");
        };
        let effects: Effects = [Effect::SubOperation(child)].into_iter().collect();
        let (asked, request) = crate::tests::policy::fetch_requests(effects);
        assert_eq!(asked, holders);
        assert_eq!(
            request,
            MetadataTransportMessage::FetchVaultRecords {
                user_id: user(7),
                query: VaultQuery::Keys,
            }
        );
        // Every reached holder answered without records: the user has none.
        assert_eq!(
            fetched(&mut operation, VaultFetchEvent::NotFound),
            Ok(Next::Done(VaultRecords::Keys(Vec::new())))
        );
    }

    #[test]
    fn reads_holder_locally() {
        let (_, turn, _) = started(true, VaultQuery::Keys);
        assert!(matches!(
            turn,
            Ok(Next::Call(IoCall::Page { prefix, .. })) if prefix == user_record_prefix(user(7))
        ));
    }

    /// 65 key records of user 7; records 1 and 65 are the newest.
    fn key_rows() -> Vec<(Key, Value)> {
        (1..=65)
            .map(|id| {
                let record = UserKeyRecord {
                    realm_id: user(7).realm_id,
                    user_id: user(7),
                    record_id: Ulid::from_bytes([id; 16]),
                    key_id: format!("key-{id}"),
                    public_key: [1; 32],
                    fingerprint: aruna_core::vault_format::key_fingerprint(&[1; 32]),
                    has_recovery: false,
                    node_id: node(1),
                    placement: PlacementRef::NIL,
                    created_at_ms: if id == 1 || id == 65 { 2 } else { 1 },
                    signature: None,
                };
                (
                    record.target().storage_key(),
                    record.to_bytes().unwrap().into(),
                )
            })
            .collect()
    }

    #[test]
    fn reads_newest_keys() {
        let (mut operation, _, _) = started(true, VaultQuery::Keys);
        let rows = key_rows();
        let cursor = rows[63].0.clone();
        let turn = operation.next(IoResult::Page {
            rows: rows[..64].to_vec(),
            next: Some(cursor.clone()),
        });
        assert!(matches!(turn,
            Ok(Next::Call(IoCall::Page {
                after: Some(after), limit: MAX_KEY_RECORDS, ..
            })) if after == cursor));
        let turn = operation.next(IoResult::Page {
            rows: rows[64..].to_vec(),
            next: None,
        });
        let Ok(Next::Done(VaultRecords::Keys(keys))) = turn else {
            panic!("expected keys");
        };
        assert_eq!(keys.len(), MAX_KEY_RECORDS);
        assert_eq!(keys[0].record_id, Ulid::from_bytes([65; 16]));
        assert_eq!(keys[1].record_id, Ulid::from_bytes([1; 16]));
        assert!(
            !keys
                .iter()
                .any(|key| key.record_id == Ulid::from_bytes([2; 16]))
        );
    }

    #[test]
    fn unavailable_stays_unavailable() {
        // No answer is never reported as an absent vault.
        let query = VaultQuery::Heads { auth_token: None };
        let (mut operation, _, _) = started(false, query);
        assert_eq!(
            fetched(
                &mut operation,
                VaultFetchEvent::Unavailable("down".to_string()),
            ),
            Err(ReadVaultError::Unavailable("down".to_string()))
        );
        let query = VaultQuery::Heads { auth_token: None };
        let (mut operation, _, _) = started(false, query);
        assert_eq!(
            fetched(&mut operation, VaultFetchEvent::Denied),
            Err(ReadVaultError::Denied)
        );
    }

    #[test]
    fn rejects_foreign_records() {
        let foreign = VaultRevision {
            realm_id: user(8).realm_id,
            user_id: user(8),
            revision_id: Ulid::from_bytes([1; 16]),
            predecessors: Vec::new(),
            payload: Some("sealed".to_string()),
            node_id: node(1),
            placement: PlacementRef::NIL,
            created_at_ms: 1,
            signature: None,
        };
        let query = VaultQuery::Heads { auth_token: None };
        let (mut operation, _, _) = started(false, query);
        let answer = VaultFetchEvent::Fetched {
            holder: node(1),
            records: VaultRecords::Heads(vec![foreign]),
        };
        assert_eq!(
            fetched(&mut operation, answer),
            Err(ReadVaultError::ForeignRecords)
        );
        // An answer of the other kind is refused too.
        let query = VaultQuery::Heads { auth_token: None };
        let (mut operation, _, _) = started(false, query);
        let answer = VaultFetchEvent::Fetched {
            holder: node(1),
            records: VaultRecords::Keys(Vec::new()),
        };
        assert_eq!(
            fetched(&mut operation, answer),
            Err(ReadVaultError::ForeignRecords)
        );
    }

    fn key_record(node_id: NodeId) -> UserKeyRecord {
        UserKeyRecord {
            realm_id: user(7).realm_id,
            user_id: user(7),
            record_id: Ulid::from_bytes([1; 16]),
            key_id: "key-1".to_string(),
            public_key: [1; 32],
            fingerprint: aruna_core::vault_format::key_fingerprint(&[1; 32]),
            has_recovery: false,
            node_id,
            placement: PlacementRef::NIL,
            created_at_ms: 1,
            signature: None,
        }
    }

    fn vault_fetch() -> (Box<dyn SubOperation>, Vec<NodeId>) {
        let holders = vec![node(1), node(2)];
        let fetch = fetch_child(&VaultFetchEffect {
            holders: HolderList::new(holders.clone()).unwrap(),
            user_id: user(7),
            query: VaultQuery::Keys,
            deadline: Duration::from_secs(10),
        });
        (fetch, holders)
    }

    fn answer(
        peer: NodeId,
        seconds: u64,
        result: Result<VaultRecords, MetadataReadError>,
    ) -> Event {
        let message = MetadataTransportMessage::FetchedVaultRecords { result };
        let frame = crate::metadata::protocol::encode_message(&message).unwrap();
        reply(peer, seconds, Ok(frame))
    }

    fn reply(
        peer: NodeId,
        seconds: u64,
        result: Result<Vec<u8>, aruna_core::events::PeerRequestError>,
    ) -> Event {
        Event::Net(NetEvent::MetadataAnswer(Box::new(
            aruna_core::events::MetadataAnswer {
                peer,
                elapsed: Duration::from_secs(seconds),
                result,
            },
        )))
    }

    fn timed_out(peer: NodeId) -> Event {
        reply(peer, 5, Err(aruna_core::events::PeerRequestError::TimedOut))
    }

    fn timeout_of(effects: &Effects) -> Duration {
        let Some(Effect::Net(aruna_core::effects::NetEffect::MetadataRequest(request))) =
            effects.first()
        else {
            panic!("expected a holder request, got {effects:?}");
        };
        request.timeout
    }

    #[test]
    fn tries_later_holder() {
        // A silent holder spends only its share of the deadline; the next one still answers.
        let (mut fetch, holders) = vault_fetch();
        assert_eq!(timeout_of(&fetch.start()), Duration::from_secs(5));
        let next = fetch.step(timed_out(holders[0]));
        assert_eq!(timeout_of(&next), Duration::from_secs(5));
        let keys = VaultRecords::Keys(vec![key_record(node(2))]);
        assert!(
            fetch
                .step(answer(holders[1], 1, Ok(keys.clone())))
                .is_empty()
        );
        assert_eq!(
            fetch.finalize(),
            Event::Net(NetEvent::VaultFetch(VaultFetchEvent::Fetched {
                holder: holders[1],
                records: keys,
            }))
        );
    }

    #[test]
    fn preserves_fetch_results() {
        // Silence is unavailable, an empty answer is a miss, and a refusal ends the fetch.
        let empty = || Ok(VaultRecords::Keys(Vec::new()));
        let forbidden = || Err(MetadataReadError::Forbidden);
        let cases: [(Vec<Option<Event>>, VaultFetchEvent); 4] = [
            (
                vec![Some(timed_out(node(1))), Some(timed_out(node(2)))],
                VaultFetchEvent::Unavailable("no vault holder answered".to_string()),
            ),
            (
                vec![Some(answer(node(1), 1, empty())), Some(timed_out(node(2)))],
                VaultFetchEvent::NotFound,
            ),
            (
                vec![
                    Some(timed_out(node(1))),
                    Some(answer(node(2), 1, forbidden())),
                ],
                VaultFetchEvent::Denied,
            ),
            (
                vec![Some(answer(node(1), 1, forbidden())), None],
                VaultFetchEvent::Denied,
            ),
        ];
        for (events, expected) in cases {
            let (mut fetch, _) = vault_fetch();
            fetch.start();
            for event in events.into_iter().flatten() {
                fetch.step(event);
            }
            assert!(fetch.is_complete());
            assert_eq!(fetch.finalize(), Event::Net(NetEvent::VaultFetch(expected)));
        }
    }

    fn local_page(heads: bool, after: Option<Key>) -> Next<VaultRecords> {
        LocalRecordsOperation::new(user(7), heads).page(after)
    }

    #[test]
    fn serves_newest_keys() {
        // A holder serves the newest key records over every page.
        let rows = key_rows();
        let cursor = rows[63].0.clone();
        let mut operation = LocalRecordsOperation::new(user(7), false);
        assert_eq!(operation.first(), Ok(local_page(false, None)));
        let answer = operation.next(IoResult::Page {
            rows: rows[..64].to_vec(),
            next: Some(cursor.clone()),
        });
        assert_eq!(answer, Ok(local_page(false, Some(cursor))));
        let answer = operation.next(IoResult::Page {
            rows: rows[64..].to_vec(),
            next: None,
        });
        let Ok(Next::Done(VaultRecords::Keys(keys))) = answer else {
            panic!("expected keys");
        };
        assert_eq!(keys.len(), MAX_KEY_RECORDS);
        assert_eq!(keys[0].record_id, Ulid::from_bytes([65; 16]));
        assert_eq!(keys[1].record_id, Ulid::from_bytes([1; 16]));
    }

    #[test]
    fn serves_heads_once() {
        // Heads are bounded by one page; a cursor after it is not followed.
        let mut operation = LocalRecordsOperation::new(user(7), true);
        assert_eq!(operation.first(), Ok(local_page(true, None)));
        let answer = operation.next(IoResult::Page {
            rows: Vec::new(),
            next: Some(Key::from(vec![1u8])),
        });
        assert_eq!(answer, Ok(Next::Done(VaultRecords::Heads(Vec::new()))));
    }

    #[test]
    fn local_read_failures() {
        // An undecodable row or an answer that is not a page answers unavailable.
        let mut operation = LocalRecordsOperation::new(user(7), false);
        operation.first().unwrap();
        let broken = vec![(Key::from(vec![1u8]), Value::from(vec![0xff; 3]))];
        let answer = operation.next(IoResult::Page {
            rows: broken,
            next: None,
        });
        assert_eq!(answer, Err(MetadataReadError::Unavailable));
        let mut operation = LocalRecordsOperation::new(user(7), false);
        operation.first().unwrap();
        assert_eq!(
            operation.next(IoResult::Done),
            Err(MetadataReadError::Unavailable)
        );
    }
}
