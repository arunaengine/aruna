//! Timer input, memory, plan and trait shared between data timers and the trigger dispatch table.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use aruna_core::backoff::capped_backoff;
use aruna_core::calls::IoError;
use aruna_core::compute::offer::BackendLimits;
use aruna_core::heartbeat::{HeldHeartbeat, RunningJobs};
use aruna_core::id::NodeId;
use aruna_core::node::facts::NodeFacts;
use aruna_core::structs::execution::job::input::RoCrateLimits;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::routing::NodeRouting;
use aruna_core::task::TaskKey;
use aruna_ops_identity::sync::outbox::batches::OutboxLimits;
use aruna_ops_identity::sync::outbox::cleanup::Publication;
use aruna_ops_identity::sync::outbox::rotation::OutboxRotation;
use thiserror::Error;

use crate::replication::catch_up::CatchUpCursors;
use crate::transfer::sharding::MaintainCursors;

/// What a timer keeps on this node between its runs, in memory only: its attempts since the last
/// clean run and where its work goes on. The shell stores it per key and hands it back unread, so
/// every decision about it stays with the timer's operation; a restart starts it afresh.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TimerMemory {
    pub attempts: u32,
    pub cursor: Option<Vec<u8>>,
    /// A count the timer advances on every run, such as a heartbeat sequence.
    pub sequence: u64,
    /// Where the replication drain's bootstrap work goes on.
    pub catch_up: CatchUpCursors,
    /// Where range maintenance goes on.
    pub ranges: MaintainCursors,
    /// The open document sync outbox rotation.
    pub rotation: OutboxRotation,
    /// Outbox records whose op this process already created, with the lineage of the topic and
    /// the row they were published from, so a retry of that row on that lineage only syncs.
    pub published: Arc<BTreeMap<Vec<u8>, Publication>>,
    /// Earliest timestamp in milliseconds when a throttled pass may run again.
    pub due_ms: u64,
}

/// Retry steps after which a timer's retry ladder stops doubling.
const LADDER_STEPS: u32 = 7;

impl TimerMemory {
    /// The wait after a failed run: `base` for the first, then doubled per further attempt up to
    /// `max`. The returned memory counts the attempt and keeps the cursor.
    pub fn next_retry(&self, base: Duration, max: Duration) -> (Duration, TimerMemory) {
        let after = capped_backoff(base, max, self.attempts.min(LADDER_STEPS), 0);
        let memory = TimerMemory {
            attempts: self.attempts.saturating_add(1),
            ..self.clone()
        };
        (after, memory)
    }
}

/// When a timer runs again, `None` while it waits for work, and what it keeps until then.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TimerPlan {
    pub after: Option<Duration>,
    pub memory: TimerMemory,
}

impl From<Option<Duration>> for TimerPlan {
    fn from(after: Option<Duration>) -> Self {
        Self {
            after,
            memory: TimerMemory::default(),
        }
    }
}

/// What the timer shell knows when a timer fires.
#[derive(Clone, Debug, PartialEq)]
pub struct TimerInput {
    pub now_ms: u64,
    /// This node, `None` on a node without a net transport.
    pub local_node: Option<NodeId>,
    /// This node and its realm while a net transport runs.
    pub serving: Option<(NodeId, RealmId)>,
    /// What the timer kept from its last run.
    pub memory: TimerMemory,
    /// What only this node's handles and host know, filled only where [`timer_needs`] says.
    pub facts: NodeFacts,
    /// This node's routing rules and backend catalog, filled like `facts`.
    pub routing: NodeRouting,
    /// Whether this node has blob storage.
    pub has_blob: bool,
    /// The jobs running on this node, for its heartbeat, filled like `facts`.
    pub running: Option<RunningJobs>,
    /// Whether this node runs a task handle.
    pub has_tasks: bool,
    /// The logical cores the host offers, filled like `facts`.
    pub host_cores: usize,
    /// The fresh heartbeats this node holds, filled like `facts`.
    pub heartbeats: BTreeMap<NodeId, HeldHeartbeat>,
    /// What each local executor backend advertises, filled like `heartbeats`.
    pub limits: BackendLimits,
    /// This node's ROCrate limits, such as the artifact retention and the holder refresh.
    pub rocrate_limits: RoCrateLimits,
    /// The bounds of one document sync outbox invocation.
    pub outbox: OutboxLimits,
}

/// Which costly inputs a timer reads; the shell builds only these, since most runs never look
/// at host telemetry, routing, job counts or other nodes' heartbeats.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TimerNeeds {
    pub facts: bool,
    pub routing: bool,
    pub running: bool,
    /// The fresh heartbeats and executor limits.
    pub cluster: bool,
    pub cores: bool,
}

/// Whether the timer of `key` rearms after a run only without a row stored while it ran, so a
/// newer wake, such as the prompt publication of a settings change, is never postponed.
pub fn keeps_earlier(key: &TaskKey) -> bool {
    matches!(key, TaskKey::PublishRegistration)
}

/// The costly inputs the timer of `key` reads.
pub fn timer_needs(key: &TaskKey) -> TimerNeeds {
    let routing = matches!(
        key,
        TaskKey::MigrateCompression
            | TaskKey::DrainCaptures
            | TaskKey::DrainCleanupQueue
            | TaskKey::DrainReplicationQueue
            | TaskKey::DrainMaterializationQueue
            | TaskKey::MaintainRanges
    );
    TimerNeeds {
        facts: matches!(
            key,
            TaskKey::SyncPlacements { .. } | TaskKey::PublishNodeInfo
        ),
        routing,
        running: matches!(key, TaskKey::PublishNodeInfo),
        cluster: matches!(key, TaskKey::DrainWitnessQueue | TaskKey::MaintainRanges),
        cores: matches!(key, TaskKey::DrainMaterializationQueue),
    }
}

impl TimerInput {
    /// A timer firing with nothing kept from an earlier run and no node facts.
    pub fn new(now_ms: u64, local_node: Option<NodeId>) -> Self {
        Self {
            now_ms,
            local_node,
            serving: None,
            memory: TimerMemory::default(),
            facts: NodeFacts::default(),
            routing: NodeRouting::default(),
            has_blob: false,
            running: None,
            has_tasks: false,
            host_cores: 1,
            heartbeats: BTreeMap::new(),
            limits: BTreeMap::new(),
            rocrate_limits: RoCrateLimits::default(),
            outbox: OutboxLimits::default(),
        }
    }
}

/// A timer run that failed; the shell logs it and runs the timer again after `retry_after`.
#[derive(Debug, Error, PartialEq, Eq)]
#[error("{reason}")]
pub struct TimerFailure {
    pub retry_after: Duration,
    pub reason: String,
}

/// When a timer runs again after the runner failed its run, for example on a storage error.
pub const TIMER_RETRY_AFTER: Duration = Duration::from_secs(30);

impl From<IoError> for TimerFailure {
    fn from(error: IoError) -> Self {
        Self {
            retry_after: TIMER_RETRY_AFTER,
            reason: error.to_string(),
        }
    }
}
