//! Rebuilds the in-memory watch interest table from the replicated digests in storage.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{HashMap, HashSet, VecDeque};

use aruna_core::NodeId;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{REALM_CONFIG_KEYSPACE, WATCH_INTEREST_KEYSPACE};
use aruna_core::operation::EffectOperation;
use aruna_core::structs::execution::notification_watch::{
    WatchInterestDigest, WatchInterestTable, interest_node_id, interest_node_prefix,
    interest_realm_id,
};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::types::{Effects, Key, Value};
use smallvec::smallvec;
use tracing::warn;

use crate::notifications::watch::digest::InterestError;

/// Digest rows read per page.
const TABLE_PAGE: usize = 1_000;

#[derive(Debug, Default, PartialEq)]
enum TableState {
    #[default]
    Init,
    Scan,
    /// Reads the realm config that decides which digest nodes count.
    Config(RealmId),
    Done,
    Failed(InterestError),
}

/// Rebuilds the full watch interest table. A digest whose embedded node id disagrees with its
/// key, or whose node is not sync-eligible in the realm's current config, is skipped; empty
/// digests contribute nothing. The output is the table.
#[derive(Debug, PartialEq)]
pub struct RebuildTableOperation {
    table: WatchInterestTable,
    eligible: HashMap<RealmId, Option<HashSet<NodeId>>>,
    rows: VecDeque<(Key, Value)>,
    next: Option<Key>,
    state: TableState,
}

impl Default for RebuildTableOperation {
    fn default() -> Self {
        Self {
            table: WatchInterestTable::default(),
            eligible: HashMap::new(),
            rows: VecDeque::new(),
            next: None,
            state: TableState::Init,
        }
    }
}

impl RebuildTableOperation {
    fn scan(&mut self, start: Option<Key>) -> Effects {
        self.state = TableState::Scan;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: WATCH_INTEREST_KEYSPACE.to_string(),
            prefix: Some(Key::from(interest_node_prefix())),
            start: start.map(IterStart::After),
            limit: TABLE_PAGE,
            txn_id: None,
        })]
    }

    /// Adds queued digests until one needs a realm config not read yet.
    fn apply(&mut self) -> Effects {
        while let Some((key, value)) = self.rows.pop_front() {
            let Some(realm_id) = interest_realm_id(key.as_ref()) else {
                continue;
            };
            let digest = match WatchInterestDigest::from_bytes(value.as_ref()) {
                Ok(digest) => digest,
                Err(error) => {
                    warn!(%error, "Failed to decode watch interest digest");
                    continue;
                }
            };
            // Never trust a digest whose embedded node id disagrees with its key.
            if interest_node_id(key.as_ref()) != Some(digest.node_id) {
                continue;
            }
            let Some(eligible) = self.eligible.get(&realm_id) else {
                self.rows.push_front((key, value));
                self.state = TableState::Config(realm_id);
                return smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: REALM_CONFIG_KEYSPACE.to_string(),
                    key: Key::from(realm_id.as_bytes().to_vec()),
                    txn_id: None,
                })];
            };
            if eligible
                .as_ref()
                .is_some_and(|nodes| nodes.contains(&digest.node_id))
            {
                self.table.insert(realm_id, digest.node_id, digest.entries);
            }
        }
        match self.next.take() {
            Some(next) => self.scan(Some(next)),
            None => {
                self.state = TableState::Done;
                smallvec![]
            }
        }
    }
}

/// The sync-eligible nodes of a stored realm config.
fn eligible_nodes(realm_id: RealmId, value: Option<&[u8]>) -> Result<HashSet<NodeId>, String> {
    let value = value.ok_or_else(|| format!("realm config {realm_id} not found"))?;
    RealmConfigDocument::from_bytes(value)
        .map_err(|error| error.to_string())?
        .sync_eligible_nodes()
        .map(|nodes| nodes.into_iter().collect())
        .map_err(|error| error.to_string())
}

impl EffectOperation for RebuildTableOperation {
    type Output = WatchInterestTable;
    type Error = InterestError;

    fn start(&mut self) -> Effects {
        self.scan(None)
    }

    fn step(&mut self, event: Event) -> Effects {
        match (std::mem::take(&mut self.state), event) {
            (
                TableState::Scan,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => {
                self.rows.extend(values);
                self.next = next_start_after;
                self.apply()
            }
            (TableState::Config(realm_id), event) => {
                let eligible = match event {
                    Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                        eligible_nodes(realm_id, value.as_deref())
                    }
                    Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
                    other => {
                        let got = format!("{other:?}");
                        self.state = TableState::Failed(InterestError::UnexpectedEvent(got));
                        return smallvec![];
                    }
                };
                let eligible = match eligible {
                    Ok(nodes) => Some(nodes),
                    Err(error) => {
                        warn!(%realm_id, %error, "Ignoring watch interest without a current realm config");
                        None
                    }
                };
                self.eligible.insert(realm_id, eligible);
                self.apply()
            }
            (_, Event::Storage(StorageEvent::Error { error })) => {
                self.state = TableState::Failed(InterestError::Storage(error));
                smallvec![]
            }
            (_, other) => {
                let got = format!("{other:?}");
                self.state = TableState::Failed(InterestError::UnexpectedEvent(got));
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, TableState::Done | TableState::Failed(_))
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.state {
            TableState::Done => Ok(self.table),
            TableState::Failed(error) => Err(error),
            _ => Err(InterestError::NotFinished),
        }
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod state_machine_tests {
    use super::*;
    use crate::tests::locations::node_id;
    use aruna_core::errors::StorageError;
    use aruna_core::operation_check::{
        check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
    };
    use aruna_core::structs::execution::notification_watch::interest_node_key;

    const REALM: RealmId = RealmId([4; 32]);

    fn digest_row(node: NodeId) -> (Key, Value) {
        let digest = WatchInterestDigest {
            node_id: node,
            entries: Vec::new(),
        };
        let key = Key::from(interest_node_key(REALM, node));
        (key, Value::from(digest.to_bytes().unwrap()))
    }

    fn scan(start: Option<Key>) -> Effect {
        Effect::Storage(StorageEffect::Iter {
            key_space: WATCH_INTEREST_KEYSPACE.to_string(),
            prefix: Some(Key::from(interest_node_prefix())),
            start: start.map(IterStart::After),
            limit: TABLE_PAGE,
            txn_id: None,
        })
    }

    #[test]
    fn config_read_once() {
        let config_key = Key::from(REALM.as_bytes().to_vec());
        let mut rebuild = RebuildTableOperation::default();
        check_trace(
            &mut rebuild,
            &[scan(None)],
            vec![
                (
                    Event::Storage(StorageEvent::IterResult {
                        values: vec![digest_row(node_id(1)), digest_row(node_id(2))],
                        next_start_after: None,
                    }),
                    vec![Effect::Storage(StorageEffect::Read {
                        key_space: REALM_CONFIG_KEYSPACE.to_string(),
                        key: config_key.clone(),
                        txn_id: None,
                    })],
                ),
                (
                    Event::Storage(StorageEvent::ReadResult {
                        key: config_key,
                        value: None,
                    }),
                    vec![],
                ),
            ],
        );
        assert_eq!(rebuild.finalize(), Ok(WatchInterestTable::default()));
    }

    #[test]
    fn pages_continue() {
        let mut rebuild = RebuildTableOperation::default();
        rebuild.start();
        let next = rebuild.step(Event::Storage(StorageEvent::IterResult {
            values: Vec::new(),
            next_start_after: Some(Key::from(vec![1])),
        }));
        assert_eq!(next.as_slice(), [scan(Some(Key::from(vec![1])))]);
    }

    #[test]
    fn scan_error_fails() {
        let mut rebuild = RebuildTableOperation::default();
        rebuild.start();
        rebuild.step(Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        }));
        assert_eq!(
            rebuild.finalize(),
            Err(InterestError::Storage(StorageError::Timeout))
        );
    }

    #[test]
    fn unexpected_events_fail() {
        let make = || {
            let mut rebuild = RebuildTableOperation::default();
            rebuild.start();
            rebuild
        };
        let expected =
            |event: &Event| matches!(event, Event::Storage(StorageEvent::IterResult { .. }));
        assert!(!rejects_events(make, sample_events, expected).is_empty());
    }

    #[test]
    fn abort_and_finalize() {
        let mut rebuild = RebuildTableOperation::default();
        rebuild.start();
        check_abort(&mut rebuild, &[]);
        rejects_finalize(rebuild);
    }
}
