//! Rebuilds the in-memory watch interest table from the replicated digests in storage.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{HashMap, HashSet, VecDeque};

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::{REALM_CONFIG_KEYSPACE, WATCH_INTEREST_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::notification_watch::{
    WatchInterestDigest, WatchInterestTable, interest_node_id, interest_node_prefix,
    interest_realm_id,
};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::types::{Key, Value};
use tracing::warn;

use crate::notifications::unexpected;
use crate::notifications::watch::digest::InterestError;

/// Digest rows read per page.
const TABLE_PAGE: usize = 1_000;

#[derive(Debug, Default, PartialEq)]
enum TableState {
    #[default]
    Scan,
    /// Reads the realm config that decides which digest nodes count.
    Config(RealmId),
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
            state: TableState::Scan,
        }
    }
}

impl RebuildTableOperation {
    fn scan(&mut self, after: Option<Key>) -> Next<WatchInterestTable> {
        self.state = TableState::Scan;
        Next::Call(IoCall::Page {
            space: WATCH_INTEREST_KEYSPACE.to_string(),
            prefix: Key::from(interest_node_prefix()),
            after,
            limit: TABLE_PAGE,
        })
    }

    /// Adds queued digests until one needs a realm config not read yet.
    fn apply(&mut self) -> Next<WatchInterestTable> {
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
                let key = Key::from(realm_id.as_bytes().to_vec());
                return Next::Call(IoCall::Read(vec![(REALM_CONFIG_KEYSPACE.to_string(), key)]));
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
            None => Next::Done(std::mem::take(&mut self.table)),
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

impl Operation for RebuildTableOperation {
    type Output = WatchInterestTable;
    type Error = InterestError;

    fn first(&mut self) -> Result<Next<Self::Output>, Self::Error> {
        Ok(self.scan(None))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, Self::Error> {
        match (std::mem::take(&mut self.state), result) {
            (TableState::Scan, IoResult::Page { rows, next }) => {
                self.rows.extend(rows);
                self.next = next;
                Ok(self.apply())
            }
            (TableState::Config(realm_id), IoResult::Values(values)) => {
                let value = values.into_iter().flatten().next();
                let eligible = match eligible_nodes(realm_id, value.as_deref()) {
                    Ok(nodes) => Some(nodes),
                    Err(error) => {
                        warn!(%realm_id, %error, "Ignoring watch interest without a current realm config");
                        None
                    }
                };
                self.eligible.insert(realm_id, eligible);
                Ok(self.apply())
            }
            (_, other) => Err(unexpected("a digest page or a realm config", &other).into()),
        }
    }
}

#[cfg(test)]
mod call_tests {
    use super::*;
    use crate::tests::locations::node_id;
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

    fn scan(after: Option<Key>) -> Next<WatchInterestTable> {
        Next::Call(IoCall::Page {
            space: WATCH_INTEREST_KEYSPACE.to_string(),
            prefix: Key::from(interest_node_prefix()),
            after,
            limit: TABLE_PAGE,
        })
    }

    #[test]
    fn config_read_once() {
        let config_key = Key::from(REALM.as_bytes().to_vec());
        let mut rebuild = RebuildTableOperation::default();
        assert_eq!(rebuild.first(), Ok(scan(None)));
        let page = IoResult::Page {
            rows: vec![digest_row(node_id(1)), digest_row(node_id(2))],
            next: None,
        };
        let config = IoCall::Read(vec![(REALM_CONFIG_KEYSPACE.to_string(), config_key)]);
        assert_eq!(rebuild.next(page), Ok(Next::Call(config)));
        assert_eq!(
            rebuild.next(IoResult::Values(vec![None])),
            Ok(Next::Done(WatchInterestTable::default()))
        );
    }

    #[test]
    fn pages_continue() {
        let mut rebuild = RebuildTableOperation::default();
        rebuild.first().unwrap();
        let page = IoResult::Page {
            rows: Vec::new(),
            next: Some(Key::from(vec![1])),
        };
        assert_eq!(rebuild.next(page), Ok(scan(Some(Key::from(vec![1])))));
        assert!(rebuild.next(IoResult::Done).is_err());
    }
}
