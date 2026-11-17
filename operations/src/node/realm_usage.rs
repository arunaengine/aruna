//! Sums realm-wide usage from the local counters and the remote node snapshots.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashSet;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::DocumentTarget;
use aruna_core::errors::ConversionError;
use aruna_core::keyspaces::{NODE_STATS_KEYSPACE, REALM_CONFIG_KEYSPACE, USAGE_STATS_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::storage::usage::{
    NodeUsageSnapshot, SUMMARY_GLOBAL_KEY, USAGE_GLOBAL_PREFIX, UsageCounterError, UsageCounters,
    usage_group_key, usage_group_prefix, usage_node_id, usage_summary_key,
};
use aruna_core::types::{GroupId, Key, Value};
use thiserror::Error;

use crate::s3::usage::{counted, global_shard_rows, global_sum};

/// Snapshot rows one page reads.
const PAGE_LIMIT: usize = 1_000;

/// Which realm-wide counter a caller wants to read from the summed cache.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RealmUsageScope {
    Global,
    Group(GroupId),
}

/// A realm-wide usage total.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RealmUsage {
    /// Objects and data counted once, a distributed range at its coordinator; `stored_*` and
    /// `buckets` over every node.
    pub counters: UsageCounters,
    /// The copies of distributed ranges left out of `counters`.
    pub replicas: UsageCounters,
}

impl RealmUsage {
    pub(crate) fn of(
        local: UsageCounters,
        (remote, replicas): (UsageCounters, UsageCounters),
    ) -> Result<Self, UsageCounterError> {
        let mut total = local;
        total.add(&remote)?;
        Ok(Self {
            counters: total.without_replicas(&replicas),
            replicas,
        })
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum RealmUsageError {
    #[error(transparent)]
    Io(#[from] IoError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Counter(#[from] UsageCounterError),
}

#[derive(Debug, PartialEq)]
enum Phase {
    Config,
    Local,
    Snapshots {
        local: UsageCounters,
        remote: UsageCounters,
        replicas: UsageCounters,
    },
    Summary,
}

/// Sums each scope's realm-wide total from the live local counters and the remote snapshots,
/// the same rows quota admission counts; with a realm, only its sync-eligible nodes count.
/// The summary form also writes the totals into the persisted realm summary cache.
#[derive(Debug, PartialEq)]
pub struct RealmUsageOperation {
    local_node_id: NodeId,
    realm_id: Option<RealmId>,
    scopes: Vec<RealmUsageScope>,
    write_summary: bool,
    active: Option<HashSet<NodeId>>,
    totals: Vec<RealmUsage>,
    phase: Phase,
}

type Turn = Result<Next<Vec<RealmUsage>>, RealmUsageError>;

impl RealmUsageOperation {
    /// Sums one scope; its output holds exactly one total.
    pub fn new(local_node_id: NodeId, realm_id: Option<RealmId>, scope: RealmUsageScope) -> Self {
        Self::with_scopes(local_node_id, realm_id, vec![scope], false)
    }

    /// Recomputes the summary cache of the global total, if asked, and of each group.
    pub fn summary(
        local_node_id: NodeId,
        realm_id: Option<RealmId>,
        include_global: bool,
        groups: Vec<GroupId>,
    ) -> Self {
        let global = include_global.then_some(RealmUsageScope::Global);
        let scopes = global
            .into_iter()
            .chain(groups.into_iter().map(RealmUsageScope::Group))
            .collect();
        Self::with_scopes(local_node_id, realm_id, scopes, true)
    }

    fn with_scopes(
        local_node_id: NodeId,
        realm_id: Option<RealmId>,
        scopes: Vec<RealmUsageScope>,
        write_summary: bool,
    ) -> Self {
        Self {
            local_node_id,
            realm_id,
            totals: Vec::with_capacity(scopes.len()),
            scopes,
            write_summary,
            active: None,
            phase: Phase::Config,
        }
    }

    fn read_config(&mut self, value: Option<Value>) -> Turn {
        if let Some(bytes) = value {
            let config = RealmConfigDocument::from_bytes(bytes.as_ref())?;
            self.active = Some(config.sync_eligible_nodes()?.into_iter().collect());
        }
        self.next_scope()
    }

    fn next_scope(&mut self) -> Turn {
        let Some(scope) = self.scopes.get(self.totals.len()) else {
            return self.finish_scopes();
        };
        self.phase = Phase::Local;
        let reads = match scope {
            RealmUsageScope::Global => global_shard_rows(),
            RealmUsageScope::Group(group_id) => {
                let key = Key::from(usage_group_key(*group_id));
                vec![(USAGE_STATS_KEYSPACE.to_string(), key)]
            }
        };
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn local_counters(&self, values: Vec<Option<Value>>) -> Result<UsageCounters, RealmUsageError> {
        match self.scopes.get(self.totals.len()) {
            Some(RealmUsageScope::Global) => global_sum(values),
            _ => (values.into_iter().next().flatten())
                .map(|bytes| UsageCounters::from_bytes(bytes.as_ref()))
                .transpose()
                .map(Option::unwrap_or_default)
                .map_err(RealmUsageError::from),
        }
    }

    fn snapshot_page(&self, after: Option<Key>) -> Turn {
        let prefix = match self.scopes.get(self.totals.len()) {
            Some(RealmUsageScope::Group(group_id)) => usage_group_prefix(*group_id),
            _ => USAGE_GLOBAL_PREFIX.to_vec(),
        };
        Ok(Next::Call(IoCall::Page {
            space: NODE_STATS_KEYSPACE.to_string(),
            prefix: Key::from(prefix),
            after,
            limit: PAGE_LIMIT,
        }))
    }

    /// Adds every node's replica share and each counted remote node's counters. The local
    /// snapshot is skipped for the counters, which come from the live local rows.
    fn add_page(
        &self,
        rows: Vec<(Key, Value)>,
        remote: &mut UsageCounters,
        replicas: &mut UsageCounters,
    ) -> Result<(), RealmUsageError> {
        for (key, value) in rows {
            let key_node_id = usage_node_id(key.as_ref());
            let local = key_node_id == Some(self.local_node_id);
            if !local && !counted(self.active.as_ref(), key_node_id) {
                continue;
            }
            let snapshot = NodeUsageSnapshot::from_bytes(value.as_ref())?;
            // Snapshots whose embedded node differs from the storage key are rejected.
            if key_node_id != Some(snapshot.node_id) {
                continue;
            }
            replicas.add(&snapshot.replicas)?;
            if !local {
                remote.add(&snapshot.counters)?;
            }
        }
        Ok(())
    }

    fn finish_scopes(&mut self) -> Turn {
        let mut writes = Vec::new();
        if self.write_summary {
            for (scope, total) in self.scopes.iter().zip(&self.totals) {
                let key = match scope {
                    RealmUsageScope::Global => SUMMARY_GLOBAL_KEY.to_vec(),
                    RealmUsageScope::Group(group_id) => usage_summary_key(*group_id),
                };
                let value = Value::from(total.counters.to_bytes()?);
                writes.push((NODE_STATS_KEYSPACE.to_string(), Key::from(key), value));
            }
        }
        self.phase = Phase::Summary;
        Ok(Next::Call(IoCall::Write(writes)))
    }
}

impl Operation for RealmUsageOperation {
    /// One total per scope, in scope order.
    type Output = Vec<RealmUsage>;
    type Error = RealmUsageError;

    fn first(&mut self) -> Turn {
        let Some(realm_id) = self.realm_id else {
            return self.next_scope();
        };
        let key = DocumentTarget::RealmConfig { realm_id }.storage_key();
        Ok(Next::Call(IoCall::Read(vec![(
            REALM_CONFIG_KEYSPACE.to_string(),
            key,
        )])))
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match (std::mem::replace(&mut self.phase, Phase::Config), result) {
            (Phase::Config, IoResult::Values(values)) => {
                self.read_config(values.into_iter().next().flatten())
            }
            (Phase::Local, IoResult::Values(values)) => {
                let local = self.local_counters(values)?;
                self.phase = Phase::Snapshots {
                    local,
                    remote: UsageCounters::default(),
                    replicas: UsageCounters::default(),
                };
                self.snapshot_page(None)
            }
            (
                Phase::Snapshots {
                    local,
                    mut remote,
                    mut replicas,
                },
                IoResult::Page { rows, next },
            ) => {
                self.add_page(rows, &mut remote, &mut replicas)?;
                if next.is_some() {
                    self.phase = Phase::Snapshots {
                        local,
                        remote,
                        replicas,
                    };
                    return self.snapshot_page(next);
                }
                self.totals.push(RealmUsage::of(local, (remote, replicas))?);
                self.next_scope()
            }
            (Phase::Summary, IoResult::Done) => Ok(Next::Done(std::mem::take(&mut self.totals))),
            _ => Err(IoError::Unexpected {
                expected: "the answer to the realm usage call",
                got: "another result",
            }
            .into()),
        }
    }
}

#[cfg(test)]
mod state_machine_tests {
    use std::collections::BTreeMap;

    use aruna_core::NodeId;
    use aruna_core::calls::{IoCall, IoResult, Next};
    use aruna_core::document::DocumentTarget;
    use aruna_core::keyspaces::{NODE_STATS_KEYSPACE, REALM_CONFIG_KEYSPACE, USAGE_STATS_KEYSPACE};
    use aruna_core::operation::Operation;
    use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId, RealmNodeKind};
    use aruna_core::structs::storage::usage::{
        NodeUsageSnapshot, SUMMARY_GLOBAL_KEY, USAGE_GLOBAL_PREFIX, UsageCounters,
        global_shard_keys, usage_global_key, usage_group_key, usage_group_prefix,
        usage_summary_key,
    };
    use aruna_core::types::{GroupId, Key, Value};
    use ulid::Ulid;

    use super::{RealmUsage, RealmUsageError, RealmUsageOperation, RealmUsageScope};
    use crate::s3::usage::global_shard_rows;

    const REALM: RealmId = RealmId([7; 32]);
    const GROUP: GroupId = Ulid::from_parts(4, 4);

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn counters(objects: u64, logical_bytes: u64) -> UsageCounters {
        UsageCounters {
            objects,
            logical_bytes,
            ..UsageCounters::default()
        }
    }

    /// A realm whose sync-eligible nodes are 1 and 2; node 3 is a user device.
    fn config_bytes() -> Value {
        let mut config = RealmConfigDocument::default_for_realm(REALM, Vec::new());
        config.seed_default_placement();
        config.ensure_node(node(1), RealmNodeKind::Server);
        config.ensure_node(node(2), RealmNodeKind::Server);
        let owner = aruna_core::UserId::new(Ulid::from_bytes([9; 16]), REALM);
        config.ensure_node(node(3), RealmNodeKind::User { owner });
        Value::from(postcard::to_allocvec(&config).unwrap())
    }

    fn snapshot_row(
        key: Vec<u8>,
        node_id: NodeId,
        used: UsageCounters,
        replicas: u64,
    ) -> (Key, Value) {
        let snapshot = NodeUsageSnapshot {
            node_id,
            counters: used,
            replicas: counters(replicas, replicas),
            revisions: BTreeMap::new(),
        };
        (Key::from(key), Value::from(snapshot.to_bytes().unwrap()))
    }

    type Turn = Result<Next<Vec<RealmUsage>>, RealmUsageError>;

    fn call(call: IoCall) -> Turn {
        Ok(Next::Call(call))
    }

    fn page(rows: Vec<(Key, Value)>, next: Option<Key>) -> IoResult {
        IoResult::Page { rows, next }
    }

    fn snapshots(prefix: Vec<u8>, after: Option<Key>) -> Turn {
        call(IoCall::Page {
            space: NODE_STATS_KEYSPACE.to_string(),
            prefix: Key::from(prefix),
            after,
            limit: 1_000,
        })
    }

    fn config_read() -> Turn {
        let key = DocumentTarget::RealmConfig { realm_id: REALM }.storage_key();
        call(IoCall::Read(vec![(REALM_CONFIG_KEYSPACE.to_string(), key)]))
    }

    fn group_read() -> Turn {
        let key = Key::from(usage_group_key(GROUP));
        call(IoCall::Read(vec![(USAGE_STATS_KEYSPACE.to_string(), key)]))
    }

    /// Shard 0 and 2 hold counters, every other shard is absent.
    fn shard_values() -> IoResult {
        let value = |used: UsageCounters| Some(Value::from(used.to_bytes().unwrap()));
        let mut values = vec![None; global_shard_keys().len()];
        values[0] = value(counters(1, 10));
        values[2] = value(counters(2, 5));
        IoResult::Values(values)
    }

    /// Node 1 is local, node 2 counts, node 3 is a device and a row of node 4 names node 2.
    fn global_page() -> Vec<(Key, Value)> {
        vec![
            snapshot_row(usage_global_key(node(1)), node(1), counters(50, 50), 1),
            snapshot_row(usage_global_key(node(2)), node(2), counters(4, 40), 2),
            snapshot_row(usage_global_key(node(3)), node(3), counters(9, 9), 9),
            snapshot_row(usage_global_key(node(4)), node(2), counters(9, 9), 9),
        ]
    }

    fn summary_write(global: &RealmUsage, group: &RealmUsage) -> Turn {
        let row = |key: Vec<u8>, total: &RealmUsage| {
            let value = Value::from(total.counters.to_bytes().unwrap());
            (NODE_STATS_KEYSPACE.to_string(), Key::from(key), value)
        };
        call(IoCall::Write(vec![
            row(SUMMARY_GLOBAL_KEY.to_vec(), global),
            row(usage_summary_key(GROUP), group),
        ]))
    }

    #[test]
    fn summary_sums_scopes() {
        let mut operation = RealmUsageOperation::summary(node(1), Some(REALM), true, vec![GROUP]);
        let continued = Key::from(usage_global_key(node(2)));
        let global = RealmUsage::of(counters(3, 15), (counters(4, 40), counters(3, 3))).unwrap();
        let group = RealmUsage::of(counters(0, 0), (counters(0, 0), counters(0, 0))).unwrap();
        let global_prefix = || USAGE_GLOBAL_PREFIX.to_vec();
        assert_eq!(operation.first(), config_read());
        let config = IoResult::Values(vec![Some(config_bytes())]);
        assert_eq!(
            operation.next(config),
            call(IoCall::Read(global_shard_rows()))
        );
        assert_eq!(
            operation.next(shard_values()),
            snapshots(global_prefix(), None)
        );
        let first = page(global_page(), Some(continued.clone()));
        assert_eq!(
            operation.next(first),
            snapshots(global_prefix(), Some(continued))
        );
        assert_eq!(operation.next(page(Vec::new(), None)), group_read());
        let absent = IoResult::Values(vec![None]);
        let group_prefix = usage_group_prefix(GROUP);
        assert_eq!(operation.next(absent), snapshots(group_prefix, None));
        let last = operation.next(page(Vec::new(), None));
        assert_eq!(last, summary_write(&global, &group));
        assert_eq!(
            operation.next(IoResult::Done),
            Ok(Next::Done(vec![global, group]))
        );
    }

    #[test]
    fn all_nodes_count() {
        // Without a realm config every node's snapshot counts, and a sum writes nothing.
        let mut operation = RealmUsageOperation::new(node(1), None, RealmUsageScope::Global);
        assert_eq!(operation.first(), call(IoCall::Read(global_shard_rows())));
        let empty = IoResult::Values(vec![None; global_shard_keys().len()]);
        let global_prefix = USAGE_GLOBAL_PREFIX.to_vec();
        assert_eq!(operation.next(empty), snapshots(global_prefix, None));
        let last = operation.next(page(global_page(), None));
        assert_eq!(last, call(IoCall::Write(Vec::new())));
        let total = RealmUsage::of(counters(0, 0), (counters(13, 49), counters(12, 12))).unwrap();
        assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(vec![total])));
    }

    #[test]
    fn rejects_bad_answers() {
        let mut garbled = RealmUsageOperation::new(node(1), Some(REALM), RealmUsageScope::Global);
        let config = IoResult::Values(vec![Some(Value::from(vec![0xff; 3]))]);
        assert!(matches!(
            garbled.next(config),
            Err(RealmUsageError::Conversion(_))
        ));
        let mut early = RealmUsageOperation::new(node(1), None, RealmUsageScope::Group(GROUP));
        assert!(matches!(
            early.next(page(Vec::new(), None)),
            Err(RealmUsageError::Io(_))
        ));
    }
}
