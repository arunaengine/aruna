//! Runs an operation's effect loop, nests suboperations and owns open transactions.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_blob::blob::BlobHandle;
use aruna_blob::blob::group::GroupHold;
use aruna_clock::unix_timestamp_millis;
use aruna_compute::registry::ExecutorRegistry;
use aruna_core::backoff::conflict_backoff;
use aruna_core::calls::CallRun;
use aruna_core::compute::offer::BackendLimits;
use aruna_core::effects::{BlobEffect, Effect, EffectGroup, GroupForm, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, GroupEvent, GroupResult, StorageEvent};
use aruna_core::heartbeat::{Heard, HeldHeartbeat, Here, MAX_HEARTBEAT_AGE};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{
    NODE_STATE_KEY, NODE_STATE_KEYSPACE, REALM_CONFIG_KEYSPACE, SYNC_OUTBOX_KEYSPACE,
};
use aruna_core::node::facts::{NodeFacts, class_labels, permille_of};
use aruna_core::operation::SubOperation;
use aruna_core::operation::{AfterConflict, HoldScope, Operation};
use aruna_core::structs::info::{BlobState, NetState, RequestSummaryState, Status};
use aruna_core::structs::storage::routing::NodeRouting;
use aruna_core::types::{Effects, TxnId};
use aruna_p2p::NetHandle;
use aruna_rdf_graph::GraphHandle;
use aruna_scheduler::TaskHandle;
use aruna_storage::storage;
use futures_util::StreamExt;
use futures_util::future::BoxFuture;
use futures_util::stream::FuturesUnordered;
use std::any::{type_name, type_name_of_val};
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::Instant;
use tracing::{Instrument, debug, debug_span, error, trace, warn};

use crate::clock;
use crate::effect_adapters;
use crate::recorder;

/// Node-local routing inputs for a caller assembling an operation config.
/// Pure in-memory state: operations never fetch this from inside a step.
pub fn node_routing(context: &DriverContext) -> NodeRouting {
    context
        .blob_handle
        .as_ref()
        .map(|handle| handle.routing())
        .unwrap_or_default()
}

/// What only this node's handles and host know, for a caller assembling node info.
pub fn node_facts(context: &DriverContext) -> NodeFacts {
    NodeFacts {
        offers: (context.compute_handle.as_ref())
            .map(|registry| registry.offers())
            .unwrap_or_default(),
        class_labels: (context.blob_handle.as_ref())
            .map(|blob| class_labels(&blob.routing().catalog))
            .unwrap_or_default(),
        load_permille: read_load_permille(),
        storage_capacity: node_routing(context).catalog.capacity(),
        download_slots: (context.net_handle.as_ref()).map(|net| net.download_gauge().snapshot()),
        serving: context.node_and_realm(),
    }
}

/// 1-minute OS load average scaled to permille of logical core capacity, or
/// `None` when the core count (with a warning) or the load average is unavailable.
fn read_load_permille() -> Option<u32> {
    let cores = match std::thread::available_parallelism() {
        Ok(cores) => cores,
        Err(error) => {
            warn!(%error, "failed to read logical core count for node info load");
            return None;
        }
    };
    match sysinfo::System::load_average() {
        Ok(load) => Some(permille_of(load.one, cores.get() as u64)),
        // Windows has no load average; logged at debug so each node info read stays quiet.
        Err(error) => {
            debug!(%error, "no load average for node info load");
            None
        }
    }
}

/// Handles and capabilities one operation run may use. Only `storage_handle` is
/// required; an absent plane keeps its adapter's explicit outcome, and governed
/// writes fail closed through the gate operation without an advertised subject.
#[derive(Clone)]
pub struct DriverContext {
    pub storage_handle: storage::handle::StorageHandle,
    pub net_handle: Option<NetHandle>,
    pub blob_handle: Option<BlobHandle>,
    pub graph_handle: Option<GraphHandle>,
    pub task_handle: Option<TaskHandle>,
    /// Enabled executor backends; `None` on nodes with no compute plane.
    pub compute_handle: Option<std::sync::Arc<ExecutorRegistry>>,
    /// HTTP and mail clients; `None` where outbound calls answer as unavailable.
    pub outbound_handle: Option<aruna_egress::OutboundHandle>,
}

impl DriverContext {
    /// A context with storage only, for work before or after the node's other planes run.
    pub fn storage_only(storage_handle: storage::handle::StorageHandle) -> Self {
        Self {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        }
    }

    /// Reads the node state row, so a readiness check sees whether storage answers reads.
    pub async fn probe_storage(&self) -> Event {
        self.storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: NODE_STATE_KEYSPACE.to_string(),
                key: NODE_STATE_KEY.into(),
                txn_id: None,
            })
            .await
    }

    /// Reads one row of the document sync outbox, so a readiness check sees it is readable.
    pub async fn probe_outbox(&self) -> Event {
        self.storage_handle
            .send_storage_effect(StorageEffect::Iter {
                key_space: SYNC_OUTBOX_KEYSPACE.to_string(),
                prefix: None,
                start: None,
                limit: 1,
                txn_id: None,
            })
            .await
    }

    /// A context with storage and a fresh task handle, for operator tools that schedule tasks.
    pub fn storage_and_tasks(storage_handle: storage::handle::StorageHandle) -> Self {
        Self {
            task_handle: Some(TaskHandle::new()),
            ..Self::storage_only(storage_handle)
        }
    }

    /// The realm this node serves; `fallback` on a node without a net transport.
    pub fn serving_realm(
        &self,
        fallback: aruna_core::structs::identity::realm::RealmId,
    ) -> aruna_core::structs::identity::realm::RealmId {
        self.net_handle
            .as_ref()
            .map_or(fallback, |net| *net.realm_id())
    }

    /// This node; `None` on a node without a net transport.
    pub fn node_id(&self) -> Option<aruna_core::id::NodeId> {
        self.net_handle.as_ref().map(|net| net.node_id())
    }

    /// This node and the realm it serves; `None` on a node without a net transport.
    pub fn node_and_realm(
        &self,
    ) -> Option<(
        aruna_core::id::NodeId,
        aruna_core::structs::identity::realm::RealmId,
    )> {
        self.net_handle
            .as_ref()
            .map(|net| (net.node_id(), *net.realm_id()))
    }

    /// What repository and harvest operations need to know about this node.
    pub fn node_scope(&self) -> aruna_core::repository::remote::NodeScope {
        aruna_core::repository::remote::NodeScope {
            node: self.node_and_realm(),
            has_blob: self.blob_handle.is_some(),
            has_tasks: self.task_handle.is_some(),
        }
    }

    /// The key this node seals repository tokens with, held by its net transport.
    pub fn credential_key(
        &self,
    ) -> Option<aruna_core::credential_encryption::CredentialEncryptionKey> {
        self.net_handle
            .as_ref()
            .map(|net| net.credential_encryption_key())
    }

    /// This context with its storage effects on the bulk lane, so
    /// background work never delays request traffic.
    pub fn bulk(&self) -> Self {
        let mut context = self.clone();
        context.storage_handle = context.storage_handle.bulk();
        context
    }

    /// The dashboard epoch and a receiver of its change counter; `None` without a net transport.
    pub fn subscribe_dashboard_changes(&self) -> Option<(String, watch::Receiver<u64>)> {
        self.net_handle.as_ref().map(|net_handle| {
            (
                net_handle.dashboard_epoch().to_string(),
                net_handle.subscribe_dashboard_changes(),
            )
        })
    }

    /// Heartbeats held for each node, fresh or stale; none without a net transport.
    pub fn held_heartbeats(&self) -> BTreeMap<NodeId, HeldHeartbeat> {
        let held = self.net_handle.as_ref().map(NetHandle::held_heartbeats);
        (held.unwrap_or_default().into_iter())
            .map(|held| (held.node_id, held))
            .collect()
    }

    /// Who this node hears from now, from its heartbeats.
    pub fn heard(&self) -> Heard {
        let held = self.held_heartbeats();
        Heard::of(
            self.node_id(),
            held.iter().map(|(node, held)| (*node, held)),
        )
    }

    /// This node as distribution operations see it.
    pub fn here(&self) -> Here {
        Here {
            ids: self.node_and_realm(),
            capacity: node_routing(self).catalog.capacity(),
            heard: self.heard(),
        }
    }

    /// The wire kind and limits of each local executor backend by its name; empty without
    /// compute.
    pub fn compute_limits(&self) -> BackendLimits {
        (self.compute_handle.as_deref())
            .map(ExecutorRegistry::limits)
            .unwrap_or_default()
    }

    /// Fresh heartbeats held for each node; none without a net transport.
    pub fn fresh_heartbeats(&self) -> BTreeMap<NodeId, HeldHeartbeat> {
        let max_age_ms = MAX_HEARTBEAT_AGE.as_millis() as u64;
        let mut held = self.held_heartbeats();
        held.retain(|_, held| held.age_ms <= max_age_ms);
        held
    }

    /// Skips holder `node_id` in holder choices for a while after it failed or refused a slot.
    pub fn avoid_holder(&self, node_id: NodeId, now_ms: u64) {
        if let Some(net_handle) = self.net_handle.as_ref() {
            net_handle.avoided_holders().avoid(node_id, now_ms);
        }
    }

    /// Tells dashboard subscribers that something they show changed.
    pub fn notify_dashboard_change(&self) {
        if let Some(net_handle) = self.net_handle.as_ref() {
            net_handle.notify_dashboard_change();
        }
    }

    /// One status snapshot of the net, blob and storage backends this context holds.
    pub async fn status(&self) -> NodeSnapshot {
        let network = match &self.net_handle {
            Some(net) => Some(net.get_status().await),
            None => None,
        };
        let blob = match &self.blob_handle {
            Some(blob) => Some(blob.get_status().await),
            None => None,
        };
        let metrics = self.storage_handle.snapshot_metrics();
        NodeSnapshot {
            network,
            blob,
            database: StorageSnapshot {
                status: if metrics.channel_closed {
                    Status::Unavailable
                } else {
                    Status::Available
                },
                requests: RequestSummaryState {
                    total: metrics.requests_total,
                    failures: metrics.failed_total,
                    last_error: metrics.last_error,
                },
            },
        }
    }
}

/// The backend status of one node, from [`DriverContext::status`].
pub struct NodeSnapshot {
    pub network: Option<NetState>,
    pub blob: Option<BlobState>,
    pub database: StorageSnapshot,
}

pub struct StorageSnapshot {
    pub status: Status,
    pub requests: RequestSummaryState,
}

impl std::fmt::Debug for DriverContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DriverContext")
            .field("net_handle", &self.net_handle)
            .field("blob_handle", &self.blob_handle.is_some())
            .field("graph_handle", &self.graph_handle.is_some())
            .field("task_handle", &self.task_handle.is_some())
            .field("compute_handle", &self.compute_handle.is_some())
            .field("outbound_handle", &self.outbound_handle.is_some())
            .finish_non_exhaustive()
    }
}

pub(crate) const MAX_SUBOP_DEPTH: usize = 32;
const SUBOP_CLEANUP_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_TRACKED_TRANSACTIONS: usize = 32;

/// Reserves every tenant backend an effect names for the rest of the operation.
/// The bytes land inside one effect, but the transaction naming them commits
/// after it returns, and that commit's rollback runs later still.
fn hold_backends(context: &DriverContext, effect: &Effect, holds: &mut Vec<GroupHold>) {
    let (Effect::Blob(blob_effect), Some(blob_handle)) = (effect, context.blob_handle.as_ref())
    else {
        return;
    };
    if let Ok(Some(hold)) = blob_handle.hold_backends(blob_effect) {
        holds.push(hold);
    }
}

/// The per-transaction lifecycle the tracker keeps explicit. A state is never
/// collapsed into a finished boolean: an unknown commit outcome stays unknown.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TransactionState {
    /// Started and neither committed nor aborted yet.
    Open,
    /// A commit returned an error that leaves the outcome unknown; no later
    /// abort may run, and storage owns the handoff.
    CommitUnknown,
    /// An abort failed; cleanup may retry it once.
    AbortFailed,
}

#[derive(Clone, Copy)]
enum TransactionEffect {
    Start,
    Commit(TxnId),
    Abort(TxnId),
}

fn transaction_effect(effect: &Effect) -> Option<TransactionEffect> {
    let Effect::Storage(storage_effect) = effect else {
        return None;
    };
    match storage_effect {
        // A usage rebuild's begin answers the snapshot it opened.
        StorageEffect::StartTransaction { .. } | StorageEffect::BeginUsageRebuild { .. } => {
            Some(TransactionEffect::Start)
        }
        StorageEffect::CommitTransaction { txn_id } => Some(TransactionEffect::Commit(*txn_id)),
        StorageEffect::AbortTransaction { txn_id } => Some(TransactionEffect::Abort(*txn_id)),
        _ => None,
    }
}

/// Whether one effect/event pair is a commit acknowledgement for that effect's
/// own transaction. An acknowledgement is what later allows an abort after
/// commit, so it is recorded separately from the tracker's states.
fn commit_done(transaction: Option<TransactionEffect>, event: &Event) -> bool {
    matches!(
        (transaction, event),
        (
            Some(TransactionEffect::Commit(txn_id)),
            Event::Storage(StorageEvent::TransactionCommitted {
                txn_id: committed,
                ..
            })
        ) if txn_id == *committed
    )
}

/// Whether an effect owns its own timeout and must not be canceled by the
/// runner deadline. Managed effects finish their storage/blob round-trip so a
/// commit acknowledgement is never lost.
pub(crate) fn managed_effect(effect: &Effect) -> bool {
    match effect {
        Effect::Blob(blob) => matches!(
            blob.as_ref(),
            BlobEffect::ReleaseReservation { .. }
                | BlobEffect::SpoolHidden {
                    deadline: Some(_),
                    ..
                }
        ),
        Effect::Storage(StorageEffect::CommitTransaction { .. }) => true,
        Effect::Storage(StorageEffect::Write {
            key_space,
            txn_id: None,
            ..
        }) => key_space == REALM_CONFIG_KEYSPACE,
        _ => false,
    }
}

#[derive(Default)]
struct TransactionTracker {
    states: BTreeMap<TxnId, TransactionState>,
    owner: Option<storage::handle::StorageHandle>,
}

impl TransactionTracker {
    fn new(owner: storage::handle::StorageHandle) -> Self {
        Self {
            states: BTreeMap::new(),
            owner: Some(owner),
        }
    }

    fn reject_start(&self, effect: Option<TransactionEffect>) -> bool {
        matches!(effect, Some(TransactionEffect::Start))
            && self.states.len() >= MAX_TRACKED_TRANSACTIONS
    }

    fn observe(&mut self, effect: Option<TransactionEffect>, event: &Event) {
        match (effect, event) {
            (
                Some(TransactionEffect::Start),
                Event::Storage(StorageEvent::TransactionStarted { txn_id }),
            ) => {
                if self.states.len() < MAX_TRACKED_TRANSACTIONS {
                    self.states.insert(*txn_id, TransactionState::Open);
                } else {
                    warn!(%txn_id, "Transaction tracker observed an unexpected start");
                }
            }
            (
                Some(TransactionEffect::Commit(txn_id)),
                Event::Storage(StorageEvent::TransactionCommitted {
                    txn_id: committed, ..
                }),
            ) if txn_id == *committed => {
                self.states.remove(&txn_id);
            }
            (
                Some(TransactionEffect::Commit(txn_id)),
                Event::Storage(StorageEvent::Error {
                    error: StorageError::TransactionConflict | StorageError::TransactionNotFound,
                }),
            ) => {
                self.states.remove(&txn_id);
            }
            (
                Some(TransactionEffect::Commit(txn_id)),
                Event::Storage(StorageEvent::Error {
                    error: StorageError::QueueFull | StorageError::CleanupCapacity,
                }),
            ) => {
                if self.states.contains_key(&txn_id) {
                    self.states.insert(txn_id, TransactionState::Open);
                }
            }
            (
                Some(TransactionEffect::Commit(txn_id)),
                Event::Storage(StorageEvent::Error { .. }),
            ) => {
                if self.states.contains_key(&txn_id) {
                    self.states.insert(txn_id, TransactionState::CommitUnknown);
                }
            }
            (
                Some(TransactionEffect::Abort(txn_id)),
                Event::Storage(StorageEvent::TransactionAborted { txn_id: aborted }),
            ) if txn_id == *aborted => {
                self.states.remove(&txn_id);
            }
            (
                Some(TransactionEffect::Abort(txn_id)),
                Event::Storage(StorageEvent::Error {
                    error: StorageError::TransactionNotFound,
                }),
            ) => {
                self.states.remove(&txn_id);
            }
            (
                Some(TransactionEffect::Abort(txn_id)),
                Event::Storage(StorageEvent::Error { .. }),
            ) if self.states.contains_key(&txn_id) => {
                self.states.insert(txn_id, TransactionState::AbortFailed);
            }
            _ => {}
        }
    }

    fn pending(&self) -> Vec<(TxnId, TransactionState)> {
        self.states
            .iter()
            .filter_map(|(txn_id, state)| {
                (!self.blocked_abort(*txn_id)).then_some((*txn_id, *state))
            })
            .collect()
    }

    fn blocked_abort(&self, txn_id: TxnId) -> bool {
        matches!(
            self.states.get(&txn_id),
            Some(TransactionState::CommitUnknown)
        ) || self
            .owner
            .as_ref()
            .is_some_and(|owner| owner.commit_unknown(txn_id))
    }

    fn retain(&self, txn_id: TxnId, state: TransactionState) {
        let Some(owner) = self.owner.as_ref() else {
            return;
        };
        let commit_unknown = matches!(state, TransactionState::CommitUnknown);
        if !owner.retain_transaction(txn_id, commit_unknown) {
            error!(%txn_id, commit_unknown, "Transaction cleanup handoff capacity reached");
        }
    }
}

impl Drop for TransactionTracker {
    fn drop(&mut self) {
        for (txn_id, state) in self.states.iter() {
            self.retain(*txn_id, *state);
        }
    }
}

async fn abort_leaked_transaction(
    tracker: &mut TransactionTracker,
    context: &DriverContext,
    depth: usize,
    deadline: Option<tokio::time::Instant>,
) {
    let cleanup_deadline =
        deadline.unwrap_or_else(|| tokio::time::Instant::now() + SUBOP_CLEANUP_TIMEOUT);
    for (txn_id, state) in tracker.pending() {
        let attempts = match state {
            TransactionState::Open => 2,
            TransactionState::AbortFailed => 1,
            TransactionState::CommitUnknown => 0,
        };
        for attempt in 0..attempts {
            if tokio::time::Instant::now() >= cleanup_deadline {
                warn!(%txn_id, "Transaction cleanup deadline expired");
                break;
            }
            let effect = Effect::Storage(StorageEffect::AbortTransaction { txn_id });
            let Ok(event) = tokio::time::timeout_at(
                cleanup_deadline,
                effect_adapters::dispatch_effect(effect, context, depth),
            )
            .await
            else {
                warn!(%txn_id, "Transaction cleanup deadline expired");
                break;
            };
            tracker.observe(Some(TransactionEffect::Abort(txn_id)), &event);
            if !tracker.states.contains_key(&txn_id) {
                break;
            }
            if attempt + 1 < attempts {
                warn!(%txn_id, "Retrying failed transaction cleanup");
            }
        }
        if let Some(state) = tracker.states.get(&txn_id).copied() {
            warn!(%txn_id, ?state, "Transaction cleanup handed off");
            tracker.retain(txn_id, state);
        }
    }
}

/// Adds effects the runner still has to dispatch, minus aborts whose commit
/// outcome is unknown. Such an abort could roll back an already committed
/// transaction, so storage keeps ownership instead.
fn extend_unblocked(queue: &mut VecDeque<Effect>, effects: Effects, tracker: &TransactionTracker) {
    queue.extend(effects.into_iter().filter(|effect| {
        !matches!(
            transaction_effect(effect),
            Some(TransactionEffect::Abort(txn_id)) if tracker.blocked_abort(txn_id)
        )
    }));
}

/// State the effect loop carries between dispatches: the tracker owns every
/// started transaction, `committed` stops a deadline's abort, and `expired`
/// plus `cleanup_deadline` bound cleanup. `holds` pin backend reservations.
struct RunState {
    tracker: TransactionTracker,
    holds: Vec<GroupHold>,
    committed: bool,
    expired: bool,
    cleanup_deadline: Option<tokio::time::Instant>,
}

impl RunState {
    fn new(context: &DriverContext) -> Self {
        Self {
            tracker: TransactionTracker::new(context.storage_handle.clone()),
            holds: Vec::new(),
            committed: false,
            expired: false,
            cleanup_deadline: None,
        }
    }

    /// Records one effect/event outcome and the commit acknowledgement it may
    /// carry, without collapsing either into a single finished flag.
    fn record_outcome(&mut self, transaction: Option<TransactionEffect>, event: &Event) {
        self.tracker.observe(transaction, event);
        self.committed |= commit_done(transaction, event);
    }

    /// The deadline stopped normal effects; only bounded cleanup may run now.
    fn mark_expired(&mut self) {
        self.expired = true;
        self.cleanup_deadline = Some(tokio::time::Instant::now() + SUBOP_CLEANUP_TIMEOUT);
    }
}

/// Stops normal effects after the run deadline and queues the operation's cleanup unless a
/// commit was acknowledged: committed work is the outcome, not something to clean up.
fn expire(executable: &mut dyn Drive, queue: &mut VecDeque<Effect>, state: &mut RunState) {
    state.mark_expired();
    queue.clear();
    if !state.committed {
        extend_unblocked(queue, executable.abort(), &state.tracker);
    }
}

/// The effect-loop half of an operation, shared by parent operations and
/// suboperations. Finalization stays with each entry point because a parent
/// yields a typed result while a suboperation yields an event.
trait Drive: Send {
    fn start(&mut self) -> Effects;
    fn step(&mut self, event: Event) -> Effects;
    fn is_complete(&self) -> bool;
    fn abort(&mut self) -> Effects;
}

/// The runner's clock for one event delivery, recording the event when the run is recorded.
fn delivery(record: Option<u64>, event: &Event) -> u64 {
    let now = clock::stamp_time();
    if let Some(run) = record {
        recorder::event(run, now, event);
    }
    now
}

/// Parent adapter: the operation borrows, while typed finalization stays in the
/// parent entry points. The second field is the run's record number when recording is on.
struct ParentRun<'a, P: Operation>(&'a mut CallRun<P>, Option<u64>);

impl<P: Operation> Drive for ParentRun<'_, P> {
    fn start(&mut self) -> Effects {
        let (now, entropy) = clock::start_stamp();
        self.1 = recorder::begin(type_name::<CallRun<P>>(), now, &*self.0, entropy);
        self.0.stamp(now);
        self.0.seed(entropy);
        self.0.start()
    }

    fn step(&mut self, event: Event) -> Effects {
        let now = delivery(self.1, &event);
        self.0.stamp(now);
        self.0.step(event)
    }

    fn is_complete(&self) -> bool {
        self.0.is_complete()
    }

    fn abort(&mut self) -> Effects {
        self.0.abort()
    }
}

/// Suboperation adapter: a boxed child yields an event from `finalize` instead
/// of a typed result. The second field is the run's record number when recording is on.
struct SubRun<'a>(&'a mut dyn SubOperation, Option<u64>);

impl Drive for SubRun<'_> {
    fn start(&mut self) -> Effects {
        let (now, entropy) = clock::start_stamp();
        let name = type_name_of_val(&*self.0);
        self.1 = recorder::begin(name, now, &*self.0, entropy);
        self.0.stamp(now);
        self.0.seed(entropy);
        self.0.start()
    }

    fn step(&mut self, event: Event) -> Effects {
        let now = delivery(self.1, &event);
        self.0.stamp(now);
        self.0.step(event)
    }

    fn is_complete(&self) -> bool {
        self.0.is_complete()
    }

    fn abort(&mut self) -> Effects {
        self.0.abort()
    }
}

/// Whether a group refuses `effect`: storage must see transaction work in emission order, and a
/// group runs only plain effects and sub operations.
fn refused_member(effect: &Effect) -> bool {
    match effect {
        Effect::Group(_) => true,
        Effect::Storage(storage_effect) => match storage_effect {
            StorageEffect::StartTransaction { .. }
            | StorageEffect::CommitTransaction { .. }
            | StorageEffect::AbortTransaction { .. }
            | StorageEffect::AddUsage { .. }
            | StorageEffect::AddCounters { .. }
            | StorageEffect::BeginUsageRebuild { .. }
            | StorageEffect::FinishUsageRebuild { .. } => true,
            StorageEffect::Read { txn_id, .. }
            | StorageEffect::BatchRead { txn_id, .. }
            | StorageEffect::Write { txn_id, .. }
            | StorageEffect::BatchWrite { txn_id, .. }
            | StorageEffect::Delete { txn_id, .. }
            | StorageEffect::BatchDelete { txn_id, .. }
            | StorageEffect::Iter { txn_id, .. }
            | StorageEffect::IterRange { txn_id, .. }
            | StorageEffect::Last { txn_id, .. }
            | StorageEffect::VaultWrite { txn_id, .. }
            | StorageEffect::VaultRead { txn_id, .. }
            | StorageEffect::VaultDelete { txn_id, .. } => txn_id.is_some(),
            StorageEffect::SyncAll
            | StorageEffect::RealmOpen(_)
            | StorageEffect::CompilePolicies(_) => false,
        },
        _ => false,
    }
}

/// Starts one effect with an optional deadline and answers its event.
type Dispatch<'a> = dyn Fn(Effect, Option<Instant>) -> BoxFuture<'a, Event> + Send + Sync + 'a;

/// Dispatches one effect through the adapters, as the effect loop does for a single effect.
fn adapter_dispatch(context: &DriverContext, depth: usize) -> Box<Dispatch<'_>> {
    let limits = hang_limits();
    Box::new(move |effect, until| Box::pin(dispatch_bounded(effect, context, depth, until, limits)))
}

pub use aruna_core::calls::HangLimits;

static HANG_LIMITS: std::sync::RwLock<HangLimits> = std::sync::RwLock::new(HangLimits::DEFAULT);

/// Sets the hang limits every later effect of this process uses.
pub fn set_hang_limits(limits: HangLimits) {
    if let Ok(mut current) = HANG_LIMITS.write() {
        *current = limits;
    }
}

fn hang_limits() -> HangLimits {
    HANG_LIMITS
        .read()
        .map_or(HangLimits::DEFAULT, |limits| *limits)
}

fn hang_limit(effect: &Effect, limits: HangLimits) -> Option<Duration> {
    if managed_effect(effect) {
        return None;
    }
    match effect {
        Effect::Storage(_) => limits.storage,
        Effect::Net(_) => limits.net,
        Effect::Metadata(_) => limits.metadata,
        Effect::Task(_) => limits.task,
        Effect::StagingSource(_) => limits.staging_source,
        Effect::Blob(_)
        | Effect::LocalFile(_)
        | Effect::Index(_)
        | Effect::Session(_)
        | Effect::Compute(_)
        | Effect::Outbound(_)
        | Effect::Plugin(_)
        | Effect::SubOperation(_)
        | Effect::Background(_)
        | Effect::Detached(_)
        | Effect::Group(_)
        | Effect::Delay(_)
        | Effect::Job(_)
        | Effect::Git(_) => None,
    }
}

/// Dispatches one effect; an adapter that outlasts the hang limit of its kind is answered with
/// `Event::Hung`, which fails the run with a typed error.
async fn dispatch_bounded(
    effect: Effect,
    context: &DriverContext,
    depth: usize,
    until: Option<Instant>,
    limits: HangLimits,
) -> Event {
    let limit = hang_limit(&effect, limits);
    let kind = effect_adapters::effect_kind(&effect);
    let dispatch = effect_adapters::dispatch_effect_until(effect, context, depth, until);
    match limit {
        Some(limit) => tokio::time::timeout(limit, dispatch)
            .await
            .unwrap_or(Event::Hung { effect: kind }),
        None => dispatch.await,
    }
}

/// When group members stop: at `at` every member still running ends as unfinished, except a
/// managed one while `protect_managed` holds, as for a single effect.
#[derive(Clone, Copy)]
struct MemberStop {
    at: Option<Instant>,
    protect_managed: bool,
}

/// The members of one group: waiting ones in emission order and at most `limit` running. Each
/// started member keeps its backend hold in the run state until the parent operation ends.
struct GroupRun<'a> {
    dispatch: Box<Dispatch<'a>>,
    context: &'a DriverContext,
    stop: MemberStop,
    limit: usize,
    added: usize,
    waiting: VecDeque<(usize, Effect)>,
    running: FuturesUnordered<BoxFuture<'a, (usize, GroupResult)>>,
}

impl<'a> GroupRun<'a> {
    fn new(
        dispatch: Box<Dispatch<'a>>,
        context: &'a DriverContext,
        limit: usize,
        stop: MemberStop,
    ) -> Self {
        Self {
            dispatch,
            context,
            stop,
            limit: limit.max(1),
            added: 0,
            waiting: VecDeque::new(),
            running: FuturesUnordered::new(),
        }
    }

    /// Queues members; their indices continue the emission order.
    fn add(&mut self, members: impl IntoIterator<Item = Effect>) {
        for member in members {
            self.waiting.push_back((self.added, member));
            self.added += 1;
        }
    }

    /// Starts waiting members while fewer than `limit` run.
    fn fill(&mut self, holds: &mut Vec<GroupHold>) {
        while self.running.len() < self.limit
            && let Some((index, member)) = self.waiting.pop_front()
        {
            let run = self.start(index, member, holds);
            self.running.push(run);
        }
    }

    fn start(
        &self,
        index: usize,
        member: Effect,
        holds: &mut Vec<GroupHold>,
    ) -> BoxFuture<'a, (usize, GroupResult)> {
        if refused_member(&member) {
            warn!(
                index,
                "Effect group refused a transaction effect or nested group"
            );
            return Box::pin(std::future::ready((index, GroupResult::Refused)));
        }
        let protected = self.stop.protect_managed && managed_effect(&member);
        let cut_at = self.stop.at.filter(|_| !protected);
        if cut_at.is_some_and(|at| at <= Instant::now()) {
            return Box::pin(std::future::ready((index, GroupResult::Unfinished)));
        }
        hold_backends(self.context, &member, holds);
        // A started child applies the cut itself and then runs its cleanup, which a timeout here
        // would drop. A child that ends at or past the cut still counts as unfinished.
        let child = matches!(member, Effect::SubOperation(_));
        let dispatch = (self.dispatch)(member, self.stop.at);
        Box::pin(async move {
            let result = match cut_at {
                Some(at) if child => {
                    let event = dispatch.await;
                    (Instant::now() < at).then_some(event)
                }
                Some(at) => tokio::time::timeout_at(at, dispatch).await.ok(),
                None => Some(dispatch.await),
            };
            let result = result.map_or(GroupResult::Unfinished, |event| {
                GroupResult::Done(Box::new(event))
            });
            (index, result)
        })
    }

    /// The next finished member; `None` once no member runs.
    async fn next(&mut self) -> Option<(usize, GroupResult)> {
        self.running.next().await
    }

    /// Starts nothing more and waits for the started members, which always finish or stop at
    /// their deadline, like a single effect.
    async fn drain(&mut self) {
        self.waiting.clear();
        while self.next().await.is_some() {}
    }
}

/// How a group ended for the effect loop.
enum GroupEnd {
    /// The operation saw the group's last event and asked for these effects.
    Stepped(Effects),
    /// The run deadline cut members off; the loop runs the expiry path.
    Expired,
    /// The cleanup deadline cut members off; cleanup stops.
    CleanupExpired,
}

/// Runs one group for the effect loop and steps the operation with its events. Members stop at
/// the cleanup deadline after expiry, otherwise at the earlier of the run and group deadline.
async fn run_group<'a>(
    executable: &mut dyn Drive,
    group: EffectGroup,
    dispatch: Box<Dispatch<'a>>,
    context: &'a DriverContext,
    deadline: Option<tokio::time::Instant>,
    state: &mut RunState,
) -> GroupEnd {
    let EffectGroup {
        members,
        limit,
        form,
    } = group;
    let stop = match (state.expired, form) {
        (true, _) => MemberStop {
            at: state.cleanup_deadline,
            protect_managed: false,
        },
        (false, GroupForm::All { deadline: wait }) => MemberStop {
            at: [
                deadline,
                wait.and_then(|wait| Instant::now().checked_add(wait)),
            ]
            .into_iter()
            .flatten()
            .min(),
            protect_managed: true,
        },
        (false, GroupForm::Window) => MemberStop {
            at: deadline,
            protect_managed: true,
        },
    };
    let mut run = GroupRun::new(dispatch, context, limit, stop);
    run.add(members);
    trace!(members = run.added, "Running effect group");
    let expired = state.expired;
    let cut_end = match expired {
        true => GroupEnd::CleanupExpired,
        false => GroupEnd::Expired,
    };
    if let GroupForm::Window = form {
        return run_window(executable, &mut run, &mut state.holds, cut_end).await;
    }
    let results = run_all(&mut run, &mut state.holds).await;
    // An unfinished member past the group deadline is a result; past the run deadline the
    // run has expired, as after a single effect that timed out.
    let unfinished = results
        .iter()
        .any(|result| matches!(result, GroupResult::Unfinished));
    if unfinished && (expired || deadline.is_some_and(|deadline| deadline <= Instant::now())) {
        return cut_end;
    }
    match executable.is_complete() {
        true => GroupEnd::Stepped(Effects::new()),
        false => GroupEnd::Stepped(executable.step(Event::Group(GroupEvent::Finished { results }))),
    }
}

/// Runs every member of an all-at-once group and returns the results in emission order.
async fn run_all(run: &mut GroupRun<'_>, holds: &mut Vec<GroupHold>) -> Vec<GroupResult> {
    let mut results: Vec<GroupResult> = std::iter::repeat_with(|| GroupResult::Unfinished)
        .take(run.added)
        .collect();
    loop {
        run.fill(holds);
        let Some((index, result)) = run.next().await else {
            return results;
        };
        if let Some(slot) = results.get_mut(index) {
            *slot = result;
        }
    }
}

/// Steps the operation with each window member result as it arrives and adds the effects it
/// returns as new members. Once no member runs or waits, steps it with `GroupEvent::Closed`.
/// A complete operation is not stepped again, but the members it asked for still run.
async fn run_window(
    executable: &mut dyn Drive,
    run: &mut GroupRun<'_>,
    holds: &mut Vec<GroupHold>,
    cut_end: GroupEnd,
) -> GroupEnd {
    loop {
        run.fill(holds);
        let Some((index, result)) = run.next().await else {
            break;
        };
        // Window members stop only at the run or cleanup deadline.
        if matches!(result, GroupResult::Unfinished) {
            run.drain().await;
            return cut_end;
        }
        if !executable.is_complete() {
            let effects = executable.step(Event::Group(GroupEvent::Member { index, result }));
            run.add(effects);
        }
    }
    match executable.is_complete() {
        true => GroupEnd::Stepped(Effects::new()),
        false => GroupEnd::Stepped(executable.step(Event::Group(GroupEvent::Closed))),
    }
}

/// Runs the effect queue to completion or to its deadline and returns the run
/// state for finalization. Only a parent names `expiry_log`, so suboperation
/// expiry stays quiet.
async fn drive_effects(
    executable: &mut dyn Drive,
    context: &DriverContext,
    depth: usize,
    deadline: Option<tokio::time::Instant>,
    expiry_log: Option<&str>,
) -> RunState {
    let mut queue: VecDeque<Effect> = executable.start().into_iter().collect();
    let mut state = RunState::new(context);
    let limits = hang_limits();

    // A run that fails in its first step is complete at once; its cleanup effects still run.
    while !executable.is_complete() || !queue.is_empty() {
        while let Some(effect) = queue.pop_front() {
            let transaction = transaction_effect(&effect);
            if let Some(TransactionEffect::Abort(txn_id)) = transaction
                && state.tracker.blocked_abort(txn_id)
            {
                warn!(%txn_id, "Skipping abort after an unknown commit outcome");
                continue;
            }
            // The run deadline applies once; after it, cleanup runs under the cleanup deadline.
            if !state.expired
                && deadline.is_some_and(|deadline| deadline <= tokio::time::Instant::now())
                && !managed_effect(&effect)
            {
                // Suppress aborts after commit. Dropping `state.holds` releases
                // backend reservations.
                expire(executable, &mut queue, &mut state);
                continue;
            }
            if let Effect::Group(group) = effect {
                let dispatch = adapter_dispatch(context, depth);
                let run = run_group(executable, group, dispatch, context, deadline, &mut state);
                match Box::pin(run).await {
                    GroupEnd::Stepped(effects) => {
                        extend_unblocked(&mut queue, effects, &state.tracker);
                    }
                    GroupEnd::Expired => {
                        if let Some(operation) = expiry_log {
                            warn!(
                                operation = %operation,
                                "Operation deadline expired in an effect group; running its abort path"
                            );
                        }
                        expire(executable, &mut queue, &mut state);
                    }
                    GroupEnd::CleanupExpired => {
                        queue.clear();
                        break;
                    }
                }
                continue;
            }
            hold_backends(context, &effect, &mut state.holds);
            let commit = matches!(transaction, Some(TransactionEffect::Commit(_)));
            let event = if state.tracker.reject_start(transaction) {
                warn!("Transaction tracker capacity reached");
                Event::Storage(StorageEvent::Error {
                    error: StorageError::TransactionConflict,
                })
            } else if state.expired {
                let Some(cleanup_deadline) = state.cleanup_deadline else {
                    break;
                };
                let Ok(event) = tokio::time::timeout_at(
                    cleanup_deadline,
                    Box::pin(effect_adapters::dispatch_effect(effect, context, depth)),
                )
                .await
                else {
                    queue.clear();
                    break;
                };
                event
            } else {
                // Managed effects own their timeout, and a child applies this deadline itself and
                // then runs its cleanup; cutting either here would drop that work.
                let managed = managed_effect(&effect) || matches!(effect, Effect::SubOperation(_));
                let dispatch = dispatch_bounded(effect, context, depth, deadline, limits);
                if let Some(deadline) = deadline.filter(|_| !managed) {
                    match tokio::time::timeout_at(deadline, dispatch).await {
                        Ok(event) => event,
                        Err(_) => {
                            if let Some(operation) = expiry_log {
                                warn!(
                                    operation = %operation,
                                    "Operation deadline expired; running its abort path"
                                );
                            }
                            if commit {
                                state.mark_expired();
                                queue.clear();
                                Event::Storage(StorageEvent::Error {
                                    error: StorageError::CommitFailed,
                                })
                            } else {
                                expire(executable, &mut queue, &mut state);
                                continue;
                            }
                        }
                    }
                } else {
                    dispatch.await
                }
            };
            state.record_outcome(transaction, &event);
            if !executable.is_complete() {
                extend_unblocked(&mut queue, executable.step(event), &state.tracker);
            }
        }

        if queue.is_empty() && !executable.is_complete() {
            if state.expired {
                break;
            }
            extend_unblocked(&mut queue, executable.abort(), &state.tracker);
            if queue.is_empty() {
                break;
            }
        }
    }

    state
}

/// Suboperation execution: its own depth, deadline, and event finalization, but
/// the same effect loop and transaction ownership as a parent. A depth limit is
/// enforced by the dispatch overview before this is reached.
pub(crate) fn drive_suboperation<'a>(
    mut operation: Box<dyn SubOperation>,
    context: &'a DriverContext,
    depth: usize,
    deadline: Option<tokio::time::Instant>,
) -> Pin<Box<dyn Future<Output = Event> + Send + 'a>> {
    let operation_name = type_name_of_val(&*operation);
    Box::pin(async move {
        let span = debug_span!("suboperation", operation = %operation_name, depth);
        async move {
            trace!(
                event = "suboperation.started",
                operation = %operation_name,
                depth,
                "Starting suboperation"
            );
            let scope = operation.hold_scope();
            let hold = take_hold(context, scope);
            let held = match deadline {
                Some(deadline) => tokio::time::timeout_at(deadline, hold).await,
                None => Ok(hold.await),
            };
            // A child that never got its hold never started, so it has nothing to clean up.
            let Ok(_hold) = held else {
                warn!(
                    operation = %operation_name,
                    "Suboperation deadline expired while it waited for its backend hold"
                );
                let _ = operation.abort();
                return operation.finalize();
            };
            let mut run = SubRun(operation.as_mut(), None);
            let effects = drive_effects(&mut run, context, depth, deadline, None);
            let mut state = holding(scope, effects).await;

            let record = run.1;
            abort_leaked_transaction(&mut state.tracker, context, depth, state.cleanup_deadline)
                .await;
            trace!(
                event = "suboperation.completed",
                operation = %operation_name,
                depth,
                "Completed suboperation"
            );
            let event = operation.finalize();
            if let Some(run) = record {
                recorder::end(run, unix_timestamp_millis(), event.label(), &event);
            }
            event
        }
        .instrument(span)
        .await
    })
}

/// Records the outcome of a recorded run.
fn record_end<T: std::fmt::Debug, E: std::fmt::Debug>(record: Option<u64>, result: &Result<T, E>) {
    if let Some(run) = record {
        let outcome = if result.is_ok() { "Ok" } else { "Err" };
        recorder::end(run, unix_timestamp_millis(), outcome, result);
    }
}

/// Waits for the backend hold of `scope`; the run keeps it until it returns or is dropped. A
/// node without the backend that owns the scope has nothing to exclude.
async fn take_hold(
    context: &DriverContext,
    scope: Option<HoldScope>,
) -> Option<tokio::sync::OwnedMutexGuard<()>> {
    let scope = scope?;
    if HELD.try_with(|held| held.contains(&scope)).unwrap_or(false) {
        return None;
    }
    match scope {
        HoldScope::DocumentReconcile => Some(context.net_handle.as_ref()?.hold_reconcile().await),
        HoldScope::DocumentWrite(document_id) => Some(
            context
                .graph_handle
                .as_ref()?
                .hold_document(document_id)
                .await,
        ),
        HoldScope::GitRepository(document_id) => {
            Some(context.blob_handle.as_ref()?.git()?.lock(document_id).await)
        }
        scope => Some(local_hold(scope).lock_owned().await),
    }
}

tokio::task_local! {
    /// The scopes the runs of this task hold. A child of a holder runs under that hold, as the
    /// helpers of an async function ran under its lock, instead of waiting for it forever.
    static HELD: Vec<HoldScope>;
}

/// Runs `effects` with `scope` marked as held, so the children it drives do not wait for it.
async fn holding<F: Future>(scope: Option<HoldScope>, effects: F) -> F::Output {
    let mut held = HELD.try_with(Clone::clone).unwrap_or_default();
    match scope {
        Some(scope) if !held.contains(&scope) => {
            held.push(scope);
            HELD.scope(held, effects).await
        }
        _ => effects.await,
    }
}

/// The lock of a scope no backend owns. A lock only the table still references guards nothing,
/// so a new scope first drops those, which keeps per-install scopes from piling up.
fn local_hold(scope: HoldScope) -> std::sync::Arc<tokio::sync::Mutex<()>> {
    type Holds = std::collections::HashMap<HoldScope, std::sync::Arc<tokio::sync::Mutex<()>>>;
    static HOLDS: std::sync::LazyLock<std::sync::Mutex<Holds>> =
        std::sync::LazyLock::new(Default::default);
    let mut holds = HOLDS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if !holds.contains_key(&scope) {
        holds.retain(|_, lock| std::sync::Arc::strong_count(lock) > 1);
    }
    holds.entry(scope).or_default().clone()
}

/// Drives an operation under one wall-clock deadline. Cleanup after expiry is
/// bounded and unresolved transaction ownership is handed to storage.
#[tracing::instrument(
    name = "operation",
    level = "debug",
    skip(operation, context),
    fields(operation = type_name::<O>())
)]
pub async fn drive_until<O: Operation>(
    operation: O,
    context: &DriverContext,
    deadline: tokio::time::Instant,
) -> Result<O::Output, O::Error> {
    run_machine(CallRun::new(operation), context, Some(deadline))
        .await
        .0
}

/// Drives an operation without a deadline. Transaction ownership and the effect
/// loop are the same as [`drive_until`]; only the deadline and its logging differ.
#[tracing::instrument(
    name = "operation",
    level = "debug",
    skip(operation, context),
    fields(operation = type_name::<O>())
)]
pub async fn drive<O: Operation>(
    operation: O,
    context: &DriverContext,
) -> Result<O::Output, O::Error> {
    run_machine(CallRun::new(operation), context, None).await.0
}

/// Drives one `CallRun`, returning its outcome and whether the runner saw a storage conflict.
async fn run_machine<P: Operation>(
    mut operation: CallRun<P>,
    context: &DriverContext,
    deadline: Option<tokio::time::Instant>,
) -> (Result<P::Output, P::Error>, bool) {
    let operation_name = type_name::<CallRun<P>>();
    if deadline.is_none() {
        trace!(
            event = "operation.started",
            operation = %operation_name,
            "Starting operation"
        );
    }

    let scope = operation.hold_scope();
    let _hold = match deadline {
        Some(limit) => match tokio::time::timeout_at(limit, take_hold(context, scope)).await {
            Ok(hold) => hold,
            Err(_) => {
                warn!(
                    operation = %operation_name,
                    "Operation deadline expired while it waited for its backend hold"
                );
                let _ = operation.abort();
                let conflicted = operation.lost_conflict();
                return (operation.finalize(), conflicted);
            }
        },
        None => take_hold(context, scope).await,
    };
    let mut run = ParentRun(&mut operation, None);
    let label = deadline.map(|_| operation_name);
    // Heap the effect loop: an adapter may drive another operation inline, and
    // nested effect-loop futures otherwise stack up in one caller's future.
    let mut state = Box::pin(holding(
        scope,
        drive_effects(&mut run, context, 0, deadline, label),
    ))
    .await;
    let record = run.1;
    if deadline.is_some() && !operation.is_complete() {
        // Nonterminal operations cannot finalize. Drop late cleanup effects and let the
        // tracker resolve any transaction they name.
        let _ = operation.abort();
    }
    let cleanup_until = deadline.map(|limit| state.cleanup_deadline.unwrap_or(limit));
    abort_leaked_transaction(&mut state.tracker, context, 0, cleanup_until).await;
    let conflicted = operation.lost_conflict();
    let result = operation.finalize();
    record_end(record, &result);
    match &result {
        Ok(_) if deadline.is_none() => trace!(
            event = "operation.completed",
            operation = %operation_name,
            "Completed operation"
        ),
        Ok(_) => {}
        Err(error) if P::expected_error(error) => debug!(
            event = "operation.rejected",
            operation = %operation_name,
            error = ?error,
            "Operation rejected"
        ),
        Err(error) => error!(
            event = "operation.failed",
            operation = %operation_name,
            error = ?error,
            "Operation failed"
        ),
    }
    (result, conflicted)
}

/// Drives a fresh operation from `build`; after a conflict (`lost_conflict` or `is_conflict`) it
/// builds the next attempt and asks its `on_conflict` to rerun after a backoff seeded by `seed`.
pub async fn drive_retry<O: Operation>(
    mut build: impl FnMut() -> O,
    context: &DriverContext,
    seed: &[u8],
) -> Result<O::Output, O::Error> {
    retry_machine(move || CallRun::new(build()), context, seed, None).await
}

/// [`drive_retry`] with every attempt and backoff under one wall-clock deadline, as
/// [`drive_until`] runs one attempt.
pub async fn drive_retry_until<O: Operation>(
    mut build: impl FnMut() -> O,
    context: &DriverContext,
    seed: &[u8],
    deadline: tokio::time::Instant,
) -> Result<O::Output, O::Error> {
    let build = move || CallRun::new(build());
    retry_machine(build, context, seed, Some(deadline)).await
}

async fn retry_machine<P: Operation>(
    mut build: impl FnMut() -> CallRun<P>,
    context: &DriverContext,
    seed: &[u8],
    deadline: Option<tokio::time::Instant>,
) -> Result<P::Output, P::Error> {
    let mut operation = build();
    let mut attempt: u32 = 0;
    loop {
        let (result, conflicted) = run_machine(operation, context, deadline).await;
        let error = match result {
            Err(error) if conflicted || P::is_conflict(&error) => error,
            result => return result,
        };
        attempt = attempt.saturating_add(1);
        operation = build();
        match operation.on_conflict(attempt) {
            AfterConflict::Rerun => {
                let index = usize::try_from(attempt - 1).unwrap_or(usize::MAX);
                let wake = tokio::time::Instant::now() + conflict_backoff(index, seed);
                // A backoff past the deadline ends there; the next attempt then expires at once.
                tokio::time::sleep_until(deadline.map_or(wake, |deadline| deadline.min(wake)))
                    .await;
            }
            AfterConflict::Finish(output) => return Ok(output),
            AfterConflict::Fail => return Err(error),
        }
    }
}

#[cfg(test)]
#[path = "driver_tests.rs"]
mod test;
