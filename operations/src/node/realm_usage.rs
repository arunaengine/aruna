//! Sums realm-wide usage from local counters and remote snapshots, and reads the rebuild marker.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashSet;

use aruna_core::NodeId;
use aruna_core::document::DocumentTarget;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{NODE_STATS_KEYSPACE, REALM_CONFIG_KEYSPACE, USAGE_STATS_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::storage::usage::{
    NodeUsageSnapshot, SUMMARY_GLOBAL_KEY, USAGE_GLOBAL_PREFIX, USAGE_REBUILD_KEY,
    UsageCounterError, UsageCounters, global_shard_keys, usage_group_key, usage_group_prefix,
    usage_node_id, usage_summary_key,
};
use aruna_core::types::{Effects, GroupId, Key, Value};
use smallvec::smallvec;
use thiserror::Error;

use crate::node::usage_stats::{RealmUsage, RealmUsageScope};

/// Snapshot rows one page reads.
const PAGE_LIMIT: usize = 1_000;

#[derive(Debug, Error, PartialEq)]
pub enum RealmUsageError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Counter(#[from] UsageCounterError),
    #[error("unexpected event while {0}")]
    UnexpectedEvent(&'static str),
    #[error("the realm usage sum has not finished")]
    Unfinished,
}

#[derive(Debug, PartialEq)]
enum Phase {
    Idle,
    Config,
    Local,
    Snapshots {
        local: UsageCounters,
        remote: UsageCounters,
        replicas: UsageCounters,
    },
    Summary,
    Done(Result<Vec<RealmUsage>, RealmUsageError>),
}

impl Phase {
    fn label(&self) -> &'static str {
        match self {
            Phase::Idle => "idle",
            Phase::Config => "reading the realm config",
            Phase::Local => "reading the local counters",
            Phase::Snapshots { .. } => "reading the node snapshots",
            Phase::Summary => "writing the realm summary",
            Phase::Done(_) => "done",
        }
    }
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
            phase: Phase::Idle,
        }
    }

    fn fail(&mut self, error: RealmUsageError) -> Effects {
        self.phase = Phase::Done(Err(error));
        smallvec![]
    }

    fn unexpected(&mut self) -> Effects {
        let error = RealmUsageError::UnexpectedEvent(self.phase.label());
        self.fail(error)
    }

    fn read_config(&mut self, value: Option<Value>) -> Result<Effects, RealmUsageError> {
        if let Some(bytes) = value {
            let config = RealmConfigDocument::from_bytes(bytes.as_ref())?;
            self.active = Some(config.sync_eligible_nodes()?.into_iter().collect());
        }
        Ok(self.next_scope())
    }

    fn next_scope(&mut self) -> Effects {
        let Some(scope) = self.scopes.get(self.totals.len()) else {
            return self.finish_scopes();
        };
        self.phase = Phase::Local;
        let effect = match scope {
            RealmUsageScope::Global => StorageEffect::BatchRead {
                reads: global_shard_keys()
                    .into_iter()
                    .map(|key| (USAGE_STATS_KEYSPACE.to_string(), Key::from(key)))
                    .collect(),
                txn_id: None,
            },
            RealmUsageScope::Group(group_id) => StorageEffect::Read {
                key_space: USAGE_STATS_KEYSPACE.to_string(),
                key: Key::from(usage_group_key(*group_id)),
                txn_id: None,
            },
        };
        smallvec![Effect::Storage(effect)]
    }

    fn local_counters(&self, event: StorageEvent) -> Result<UsageCounters, RealmUsageError> {
        let scope = self.scopes.get(self.totals.len());
        match (scope, event) {
            (Some(RealmUsageScope::Global), StorageEvent::BatchReadResult { values }) => {
                let mut total = UsageCounters::default();
                for bytes in values.into_iter().filter_map(|(_, value)| value) {
                    total.add(&UsageCounters::from_bytes(bytes.as_ref())?)?;
                }
                Ok(total)
            }
            (Some(RealmUsageScope::Group(_)), StorageEvent::ReadResult { value, .. }) => value
                .map(|bytes| UsageCounters::from_bytes(bytes.as_ref()))
                .transpose()
                .map(Option::unwrap_or_default)
                .map_err(RealmUsageError::from),
            _ => Err(RealmUsageError::UnexpectedEvent(self.phase.label())),
        }
    }

    fn snapshot_page(&self, start: Option<Key>) -> Effects {
        let prefix = match self.scopes.get(self.totals.len()) {
            Some(RealmUsageScope::Group(group_id)) => usage_group_prefix(*group_id),
            _ => USAGE_GLOBAL_PREFIX.to_vec(),
        };
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: NODE_STATS_KEYSPACE.to_string(),
            prefix: Some(Key::from(prefix)),
            start: start.map(IterStart::After),
            limit: PAGE_LIMIT,
            txn_id: None,
        })]
    }

    /// Adds every node's replica share and each counted remote node's counters. The local
    /// snapshot is skipped for the counters, which come from the live local rows.
    fn add_page(&mut self, values: Vec<(Key, Value)>) -> Result<(), RealmUsageError> {
        let Phase::Snapshots {
            remote, replicas, ..
        } = &mut self.phase
        else {
            return Err(RealmUsageError::UnexpectedEvent("adding a snapshot page"));
        };
        for (key, value) in values {
            let key_node_id = usage_node_id(key.as_ref());
            let local = key_node_id == Some(self.local_node_id);
            let counted = match (&self.active, key_node_id) {
                (Some(active), Some(node_id)) => active.contains(&node_id),
                _ => true,
            };
            if !local && !counted {
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

    fn finish_scope(&mut self) -> Result<Effects, RealmUsageError> {
        let Phase::Snapshots {
            local,
            remote,
            replicas,
        } = std::mem::replace(&mut self.phase, Phase::Idle)
        else {
            return Err(RealmUsageError::UnexpectedEvent("finishing a scope"));
        };
        let total = RealmUsage::of(local, (remote, replicas))?;
        self.totals.push(total);
        Ok(self.next_scope())
    }

    fn finish_scopes(&mut self) -> Effects {
        let mut writes = Vec::new();
        if self.write_summary {
            for (scope, total) in self.scopes.iter().zip(&self.totals) {
                let key = match scope {
                    RealmUsageScope::Global => SUMMARY_GLOBAL_KEY.to_vec(),
                    RealmUsageScope::Group(group_id) => usage_summary_key(*group_id),
                };
                let value = match total.counters.to_bytes() {
                    Ok(value) => value,
                    Err(error) => return self.fail(error.into()),
                };
                writes.push((
                    NODE_STATS_KEYSPACE.to_string(),
                    Key::from(key),
                    Value::from(value),
                ));
            }
        }
        if writes.is_empty() {
            self.phase = Phase::Done(Ok(std::mem::take(&mut self.totals)));
            return smallvec![];
        }
        self.phase = Phase::Summary;
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })]
    }

    fn handle(&mut self, event: StorageEvent) -> Result<Effects, RealmUsageError> {
        if let StorageEvent::Error { error } = event {
            return Err(error.into());
        }
        let unexpected = RealmUsageError::UnexpectedEvent(self.phase.label());
        match self.phase {
            Phase::Config => match event {
                StorageEvent::ReadResult { value, .. } => self.read_config(value),
                _ => Err(unexpected),
            },
            Phase::Local => {
                let local = self.local_counters(event)?;
                self.phase = Phase::Snapshots {
                    local,
                    remote: UsageCounters::default(),
                    replicas: UsageCounters::default(),
                };
                Ok(self.snapshot_page(None))
            }
            Phase::Snapshots { .. } => match event {
                StorageEvent::IterResult {
                    values,
                    next_start_after,
                } => {
                    self.add_page(values)?;
                    match next_start_after {
                        Some(start) => Ok(self.snapshot_page(Some(start))),
                        None => self.finish_scope(),
                    }
                }
                _ => Err(unexpected),
            },
            Phase::Summary => match event {
                StorageEvent::BatchWriteResult { .. } => {
                    self.phase = Phase::Done(Ok(std::mem::take(&mut self.totals)));
                    Ok(smallvec![])
                }
                _ => Err(unexpected),
            },
            Phase::Idle | Phase::Done(_) => Err(unexpected),
        }
    }
}

impl Operation for RealmUsageOperation {
    /// One total per scope, in scope order.
    type Output = Vec<RealmUsage>;
    type Error = RealmUsageError;

    fn start(&mut self) -> Effects {
        let Some(realm_id) = self.realm_id else {
            return self.next_scope();
        };
        self.phase = Phase::Config;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key: DocumentTarget::RealmConfig { realm_id }.storage_key(),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        let Event::Storage(event) = event else {
            return self.unexpected();
        };
        match self.handle(event) {
            Ok(effects) => effects,
            Err(error) => self.fail(error),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.phase, Phase::Done(_))
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.phase {
            Phase::Done(result) => result,
            _ => Err(RealmUsageError::Unfinished),
        }
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

/// Reads whether the local usage counters are complete: false while a usage rebuild runs,
/// when they must not count as free capacity and do not yet back quota decisions.
#[derive(Debug, Default, PartialEq)]
pub struct UsageCompleteOperation {
    started: bool,
    result: Option<Result<bool, StorageError>>,
}

impl UsageCompleteOperation {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Operation for UsageCompleteOperation {
    type Output = bool;
    type Error = StorageError;

    fn start(&mut self) -> Effects {
        self.started = true;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: USAGE_STATS_KEYSPACE.to_string(),
            key: Key::from(USAGE_REBUILD_KEY.to_vec()),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        self.result = Some(match (self.started && self.result.is_none(), event) {
            (true, Event::Storage(StorageEvent::ReadResult { value, .. })) => Ok(value.is_none()),
            (true, Event::Storage(StorageEvent::Error { error })) => Err(error),
            _ => Err(StorageError::ReadError(
                "unexpected event for the usage rebuild marker".to_string(),
            )),
        });
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        self.result.is_some()
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.result.unwrap_or(Err(StorageError::ReadError(
            "the usage rebuild marker was not read".to_string(),
        )))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
#[path = "realm_usage_tests.rs"]
mod state_machine_tests;
