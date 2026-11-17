//! Runs an operation's effect loop, nests suboperations and owns open transactions.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_blob::blob::{BlobHandle, GroupHold};
use aruna_compute::ExecutorRegistry;
use aruna_core::backoff::conflict_backoff;
use aruna_core::effects::{BlobEffect, Effect, EffectGroup, GroupForm, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, GroupEvent, GroupResult, StorageEvent};
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::operation::{HoldScope, Operation, SubOperation};
use aruna_core::types::{Effects, TxnId};
use aruna_net::NetHandle;
use aruna_storage::storage;
use aruna_tasks::TaskHandle;
use futures_util::StreamExt;
use futures_util::future::BoxFuture;
use futures_util::stream::FuturesUnordered;
use std::any::{type_name, type_name_of_val};
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use tokio::time::Instant;
use tracing::{Instrument, debug, debug_span, error, trace, warn};

use crate::effect_adapters;
use crate::metadata::MetadataHandle;

pub use effect_adapters::routing::{
    GateContextError, RoutingInputsError, backend_used_bytes, bucket_snapshot, gate_context,
    node_routing, now_ms, quota_marked_routing, routing_snapshot,
};

/// Handles and capabilities one operation run may use. Only `storage_handle` is
/// required; an absent plane keeps its adapter's explicit outcome, and governed
/// writes fail closed through [`gate_context`] without an advertised subject.
#[derive(Clone)]
pub struct DriverContext {
    pub storage_handle: storage::StorageHandle,
    pub net_handle: Option<NetHandle>,
    pub blob_handle: Option<BlobHandle>,
    pub metadata_handle: Option<MetadataHandle>,
    pub task_handle: Option<TaskHandle>,
    /// Enabled executor backends; `None` on nodes with no compute plane.
    pub compute_handle: Option<std::sync::Arc<ExecutorRegistry>>,
}

impl DriverContext {
    /// The realm this node serves; `fallback` on a node without a net transport.
    pub fn serving_realm(
        &self,
        fallback: aruna_core::structs::identity::realm::RealmId,
    ) -> aruna_core::structs::identity::realm::RealmId {
        self.net_handle
            .as_ref()
            .map_or(fallback, |net| *net.realm_id())
    }

    /// This node and the realm it serves; `None` on a node without a net transport.
    pub fn node_and_realm(
        &self,
    ) -> Option<(
        aruna_core::NodeId,
        aruna_core::structs::identity::realm::RealmId,
    )> {
        self.net_handle
            .as_ref()
            .map(|net| (net.node_id(), *net.realm_id()))
    }

    /// This context with its storage effects and metadata reads on the bulk lane, so
    /// background work never delays request traffic.
    pub fn bulk(&self) -> Self {
        let mut context = self.clone();
        context.storage_handle = context.storage_handle.bulk();
        context.metadata_handle = context.metadata_handle.as_ref().map(MetadataHandle::bulk);
        context
    }
}

impl std::fmt::Debug for DriverContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DriverContext")
            .field("net_handle", &self.net_handle)
            .field("blob_handle", &self.blob_handle.is_some())
            .field("metadata_handle", &self.metadata_handle.is_some())
            .field("task_handle", &self.task_handle.is_some())
            .field("compute_handle", &self.compute_handle.is_some())
            .finish_non_exhaustive()
    }
}

pub(crate) const MAX_SUBOP_DEPTH: usize = 32;
const SUBOP_CLEANUP_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_TRACKED_TRANSACTIONS: usize = 32;
/// Reruns after a transaction conflict before [`drive_retry`] returns the conflict.
const CONFLICT_RETRIES: usize = 3;

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
            Event::Storage(StorageEvent::TransactionCommitted { txn_id: committed })
        ) if txn_id == *committed
    )
}

/// Whether an effect owns its own timeout and must not be canceled by the
/// runner deadline. Managed effects finish their storage/blob round-trip so a
/// commit acknowledgement is never lost.
pub(crate) fn managed_effect(effect: &Effect) -> bool {
    match effect {
        Effect::Blob(BlobEffect::ReleaseReservation { .. }) => true,
        Effect::Blob(BlobEffect::SpoolHidden {
            deadline: Some(_), ..
        }) => true,
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
    owner: Option<storage::StorageHandle>,
}

impl TransactionTracker {
    fn new(owner: storage::StorageHandle) -> Self {
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
                Event::Storage(StorageEvent::TransactionCommitted { txn_id: committed }),
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

// Deferred (#336): an active-transaction gauge and a time-based reaper for
// stray transactions, observability on top of this closed leak path.
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
/// started transaction, `committed` gates `abort_after_commit`, and `expired`
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

/// Whether a deadline may still run `abort` after a commit was acknowledged.
#[derive(Clone, Copy)]
enum DeadlineAbort {
    /// Suboperations always stop at an acknowledged commit.
    BeforeCommitOnly,
    /// Parents keep the operation's opt-in: only an `abort` that protects
    /// already committed work may run after one.
    OperationOptIn,
}

impl DeadlineAbort {
    fn allows(self, committed: bool, abort_after_commit: bool) -> bool {
        match self {
            Self::BeforeCommitOnly => !committed,
            Self::OperationOptIn => !committed || abort_after_commit,
        }
    }
}

/// Stops normal effects after the run deadline and queues the operation's cleanup when the
/// policy allows it.
fn expire(
    executable: &mut dyn Drive,
    queue: &mut VecDeque<Effect>,
    state: &mut RunState,
    abort_policy: DeadlineAbort,
) {
    state.mark_expired();
    queue.clear();
    if abort_policy.allows(state.committed, executable.abort_after_commit()) {
        extend_unblocked(queue, executable.abort(), &state.tracker);
    }
}

/// How often the effect loop re-reads the deadline. The suboperation loop
/// re-checks every effect; the parent marks expiry once and then dispatches its
/// cleanup effects under the cleanup deadline.
#[derive(Clone, Copy)]
enum ExpiryRecheck {
    Once,
    EveryEffect,
}

/// The effect-loop half of an operation, shared by parent operations and
/// suboperations. Finalization stays with each entry point because a parent
/// yields a typed result while a suboperation yields an event.
trait Drive: Send {
    fn start(&mut self) -> Effects;
    fn step(&mut self, event: Event) -> Effects;
    fn is_complete(&self) -> bool;
    fn abort(&mut self) -> Effects;
    /// Read at each deadline decision: an operation may opt in only once it has
    /// committed work to protect.
    fn abort_after_commit(&self) -> bool {
        false
    }
}

/// Parent adapter: the operation borrows, while typed finalization stays in the
/// parent entry points.
struct ParentRun<'a, O: Operation>(&'a mut O);

impl<O: Operation> Drive for ParentRun<'_, O> {
    fn start(&mut self) -> Effects {
        self.0.start()
    }

    fn step(&mut self, event: Event) -> Effects {
        self.0.step(event)
    }

    fn is_complete(&self) -> bool {
        self.0.is_complete()
    }

    fn abort(&mut self) -> Effects {
        self.0.abort()
    }

    fn abort_after_commit(&self) -> bool {
        self.0.abort_after_commit()
    }
}

/// Suboperation adapter: a boxed child yields an event from `finalize` instead
/// of a typed result.
struct SubRun<'a>(&'a mut dyn SubOperation);

impl Drive for SubRun<'_> {
    fn start(&mut self) -> Effects {
        self.0.start()
    }

    fn step(&mut self, event: Event) -> Effects {
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
            StorageEffect::SyncAll => false,
        },
        _ => false,
    }
}

/// Starts one effect with an optional deadline and answers its event.
type Dispatch<'a> = dyn Fn(Effect, Option<Instant>) -> BoxFuture<'a, Event> + Send + Sync + 'a;

/// Dispatches one effect through the adapters, as the effect loop does for a single effect.
fn adapter_dispatch(context: &DriverContext, depth: usize) -> Box<Dispatch<'_>> {
    Box::new(move |effect, until| {
        Box::pin(effect_adapters::dispatch_effect_until(
            effect, context, depth, until,
        ))
    })
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
        let dispatch = (self.dispatch)(member, self.stop.at);
        Box::pin(async move {
            let result = match cut_at {
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
    abort_policy: DeadlineAbort,
    expiry_recheck: ExpiryRecheck,
    expiry_log: Option<&str>,
) -> RunState {
    let mut queue: VecDeque<Effect> = executable.start().into_iter().collect();
    let mut state = RunState::new(context);

    while !executable.is_complete() {
        while let Some(effect) = queue.pop_front() {
            let transaction = transaction_effect(&effect);
            if let Some(TransactionEffect::Abort(txn_id)) = transaction
                && state.tracker.blocked_abort(txn_id)
            {
                warn!(%txn_id, "Skipping abort after an unknown commit outcome");
                continue;
            }
            let recheck = match expiry_recheck {
                ExpiryRecheck::Once => !state.expired,
                ExpiryRecheck::EveryEffect => true,
            };
            if recheck
                && deadline.is_some_and(|deadline| deadline <= tokio::time::Instant::now())
                && !managed_effect(&effect)
            {
                // Suppress aborts after commit. Dropping `state.holds` releases
                // backend reservations.
                expire(executable, &mut queue, &mut state, abort_policy);
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
                        expire(executable, &mut queue, &mut state, abort_policy);
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
                    effect_adapters::dispatch_effect(effect, context, depth),
                )
                .await
                else {
                    queue.clear();
                    break;
                };
                event
            } else if let Some(deadline) = deadline {
                let managed = managed_effect(&effect);
                // Managed effects own their timeout and must not be canceled here.
                let dispatch = Box::pin(effect_adapters::dispatch_effect_until(
                    effect,
                    context,
                    depth,
                    Some(deadline),
                ));
                if managed {
                    dispatch.await
                } else {
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
                                expire(executable, &mut queue, &mut state, abort_policy);
                                continue;
                            }
                        }
                    }
                }
            } else {
                Box::pin(effect_adapters::dispatch_effect(effect, context, depth)).await
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
    let operation_name = type_name_of_val(&*operation).to_string();
    Box::pin(async move {
        let span = debug_span!("suboperation", operation = %operation_name, depth);
        async move {
            trace!(
                event = "suboperation.started",
                operation = %operation_name,
                depth,
                "Starting suboperation"
            );
            let mut run = SubRun(operation.as_mut());
            let mut state = drive_effects(
                &mut run,
                context,
                depth,
                deadline,
                DeadlineAbort::BeforeCommitOnly,
                ExpiryRecheck::EveryEffect,
                None,
            )
            .await;

            abort_leaked_transaction(&mut state.tracker, context, depth, state.cleanup_deadline)
                .await;
            trace!(
                event = "suboperation.completed",
                operation = %operation_name,
                depth,
                "Completed suboperation"
            );
            operation.finalize()
        }
        .instrument(span)
        .await
    })
}

/// Waits for the backend hold of `scope`; the run keeps it until it returns or is dropped. A
/// node without the backend that owns the scope has nothing to exclude.
async fn take_hold(
    context: &DriverContext,
    scope: Option<HoldScope>,
) -> Option<tokio::sync::OwnedMutexGuard<()>> {
    match scope? {
        HoldScope::DocumentReconcile => Some(context.net_handle.as_ref()?.hold_reconcile().await),
    }
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
    mut operation: O,
    context: &DriverContext,
    deadline: tokio::time::Instant,
) -> Result<O::Output, O::Error> {
    let hold = take_hold(context, operation.hold_scope());
    let Ok(_hold) = tokio::time::timeout_at(deadline, hold).await else {
        warn!(
            operation = %type_name::<O>(),
            "Operation deadline expired while it waited for its backend hold"
        );
        let _ = operation.abort();
        return operation.finalize();
    };
    let mut run = ParentRun(&mut operation);
    let mut state = drive_effects(
        &mut run,
        context,
        0,
        Some(deadline),
        DeadlineAbort::OperationOptIn,
        ExpiryRecheck::Once,
        Some(type_name::<O>()),
    )
    .await;
    if !operation.is_complete() {
        // Nonterminal operations cannot finalize. Drop late cleanup effects and let the
        // tracker resolve any transaction they name.
        let _ = operation.abort();
    }
    abort_leaked_transaction(
        &mut state.tracker,
        context,
        0,
        Some(state.cleanup_deadline.unwrap_or(deadline)),
    )
    .await;
    let result = operation.finalize();
    if let Err(error) = &result {
        if O::expected_error(error) {
            debug!(
                event = "operation.rejected",
                operation = %type_name::<O>(),
                error = ?error,
                "Operation rejected"
            );
        } else {
            error!(
                event = "operation.failed",
                operation = %type_name::<O>(),
                error = ?error,
                "Operation failed"
            );
        }
    }
    result
}

/// Drives an operation without a deadline. Transaction ownership and the effect
/// loop are the same as [`drive_until`]; only the deadline and its logging
/// differ.
#[tracing::instrument(
    name = "operation",
    level = "debug",
    skip(operation, context),
    fields(operation = type_name::<O>())
)]
pub async fn drive<O: Operation>(
    mut operation: O,
    context: &DriverContext,
) -> Result<O::Output, O::Error> {
    let operation_name = type_name::<O>();

    trace!(
        event = "operation.started",
        operation = %operation_name,
        "Starting operation"
    );

    let _hold = take_hold(context, operation.hold_scope()).await;
    let mut run = ParentRun(&mut operation);
    // Heap the effect loop: an adapter may drive another operation inline, and
    // nested effect-loop futures otherwise stack up in one caller's future.
    let mut state = Box::pin(drive_effects(
        &mut run,
        context,
        0,
        None,
        DeadlineAbort::BeforeCommitOnly,
        ExpiryRecheck::Once,
        None,
    ))
    .await;
    abort_leaked_transaction(&mut state.tracker, context, 0, None).await;
    let result = operation.finalize();
    match &result {
        Ok(_) => trace!(
            event = "operation.completed",
            operation = %operation_name,
            "Completed operation"
        ),
        Err(error) if O::expected_error(error) => debug!(
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
    result
}

/// Drives a fresh operation from `build` and, after an error that [`Operation::is_conflict`]
/// names, waits a backoff seeded by `seed` and drives a new one, at most [`CONFLICT_RETRIES`]
/// times. The same seed gives the same waits; distinct seeds spread conflicting writers out.
pub async fn drive_retry<O: Operation>(
    mut build: impl FnMut() -> O,
    context: &DriverContext,
    seed: &[u8],
) -> Result<O::Output, O::Error> {
    let mut attempt = 0;
    loop {
        match drive(build(), context).await {
            Err(error) if O::is_conflict(&error) && attempt < CONFLICT_RETRIES => {
                tokio::time::sleep(conflict_backoff(attempt, seed)).await;
                attempt += 1;
            }
            result => return result,
        }
    }
}

#[cfg(test)]
mod test {
    use crate::driver::{
        DeadlineAbort, DriverContext, GroupEnd, GroupRun, Instant, MAX_TRACKED_TRANSACTIONS,
        MemberStop, ParentRun, TransactionEffect, TransactionState, TransactionTracker, drive,
        drive_retry, drive_until, extend_unblocked, run_all, run_window,
    };
    use aruna_core::effects::{
        BlobEffect, Effect, EffectGroup, GroupForm, StagingSourceEffect, StorageEffect,
    };
    use aruna_core::errors::StorageError;
    use aruna_core::events::{
        BlobEvent, Event, GroupEvent, GroupResult, StagingSourceEvent, StorageEvent,
        SubOperationEvent,
    };
    use aruna_core::operation::{HoldScope, Operation, boxed_suboperation};
    use aruna_core::structs::execution::source_access::ResolvedSourceAccess;
    use aruna_core::structs::execution::source_connector::SourceConnectorKind;
    use aruna_core::structs::storage::blob::{BackendLocation, BackendRef};
    use aruna_core::task::{TaskEffect, TaskKey};
    use aruna_core::types::TxnId;
    use aruna_storage::storage;
    use byteview::ByteView;
    use futures_util::FutureExt;
    use futures_util::future::BoxFuture;
    use std::collections::VecDeque;
    use std::convert::Infallible;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
    use std::time::Duration;
    use tempfile::tempdir;

    #[derive(Debug, PartialEq)]
    pub struct TestOperation {
        pub state: u8,
        pub txn_id: Option<aruna_core::types::TxnId>,
    }

    impl TestOperation {
        pub fn new() -> Self {
            TestOperation {
                state: 0,
                txn_id: None,
            }
        }
    }

    impl Operation for TestOperation {
        type Output = ();
        type Error = ();

        fn start(&mut self) -> aruna_core::types::Effects {
            self.state = 1;

            smallvec::smallvec![Effect::Storage(StorageEffect::StartTransaction {
                read: false
            })]
        }

        fn step(&mut self, events: aruna_core::events::Event) -> aruna_core::types::Effects {
            match (events, self.state) {
                (Event::Storage(StorageEvent::TransactionStarted { txn_id }), 1) => {
                    self.state = 2;
                    self.txn_id = Some(txn_id);
                    eprintln!("Transaction started with id {:?}", txn_id);
                    smallvec::smallvec![Effect::Storage(StorageEffect::Write {
                        key_space: "default".to_string(),
                        key: ByteView::from(*b"key1"),
                        value: ByteView::from(*b"value1"),
                        txn_id: Some(txn_id),
                    })]
                }
                (Event::Storage(StorageEvent::WriteResult { key: _ }), 2) => {
                    self.state = 3;
                    eprintln!("Write completed, committing transaction.");
                    smallvec::smallvec![Effect::Storage(StorageEffect::CommitTransaction {
                        txn_id: self.txn_id.unwrap(),
                    })]
                }
                (Event::Storage(StorageEvent::TransactionCommitted { txn_id: _ }), 3) => {
                    self.state = 4;
                    eprintln!("Transaction committed, reading back value.");
                    smallvec::smallvec![Effect::Storage(StorageEffect::Read {
                        key_space: "default".to_string(),
                        key: ByteView::from(*b"key1"),
                        txn_id: None,
                    })]
                }
                (Event::Storage(StorageEvent::ReadResult { key, value }), 4) => {
                    self.state = 5;

                    eprintln!("Read key: {:?}, value: {:?}", key, value);
                    assert_eq!(key, ByteView::from(*b"key1"));
                    assert_eq!(value, Some(ByteView::from(*b"value1")));
                    self.state = 6;
                    smallvec::smallvec![]
                }

                a => {
                    eprintln!("Unexpected event/state combination {:?}", a);
                    smallvec::smallvec![]
                }
            }
        }

        fn is_complete(&self) -> bool {
            self.state == 6
        }

        fn finalize(self) -> Result<Self::Output, Self::Error> {
            Ok(())
        }

        fn abort(&mut self) -> aruna_core::types::Effects {
            smallvec::smallvec![]
        }
    }

    #[derive(Debug, PartialEq)]
    struct MarkerAbortOperation {
        state: u8,
        fail: bool,
        txn_id: Option<TxnId>,
    }

    impl Operation for MarkerAbortOperation {
        type Output = ();
        type Error = ();

        fn start(&mut self) -> aruna_core::types::Effects {
            self.state = 1;
            smallvec::smallvec![Effect::Storage(StorageEffect::StartTransaction {
                read: false
            })]
        }

        fn step(&mut self, event: Event) -> aruna_core::types::Effects {
            match (event, self.state) {
                (Event::Storage(StorageEvent::TransactionStarted { txn_id }), 1) => {
                    self.txn_id = Some(txn_id);
                    self.state = 2;
                    smallvec::smallvec![Effect::Storage(StorageEffect::Write {
                        key_space: "default".to_string(),
                        key: ByteView::from(*b"staged-marker"),
                        value: ByteView::from(*b"staged"),
                        txn_id: Some(txn_id),
                    })]
                }
                (Event::Storage(StorageEvent::WriteResult { .. }), 2) if self.fail => {
                    self.state = 4;
                    smallvec::smallvec![]
                }
                (Event::Storage(StorageEvent::WriteResult { .. }), 2) => {
                    self.state = 3;
                    smallvec::smallvec![Effect::Storage(StorageEffect::CommitTransaction {
                        txn_id: self.txn_id.expect("transaction id recorded")
                    })]
                }
                (Event::Storage(StorageEvent::TransactionCommitted { .. }), 3) => {
                    self.state = 4;
                    smallvec::smallvec![]
                }
                _ => smallvec::smallvec![],
            }
        }

        fn is_complete(&self) -> bool {
            self.state == 4
        }

        fn finalize(self) -> Result<Self::Output, Self::Error> {
            if self.fail { Err(()) } else { Ok(()) }
        }

        fn abort(&mut self) -> aruna_core::types::Effects {
            smallvec::smallvec![Effect::Storage(StorageEffect::Write {
                key_space: "default".to_string(),
                key: ByteView::from(*b"abort-marker"),
                value: ByteView::from(*b"ran"),
                txn_id: None,
            })]
        }
    }

    #[derive(Debug)]
    struct DeadlineOperation {
        state: u8,
        txn_id: Option<TxnId>,
        ready: Arc<tokio::sync::Notify>,
    }

    impl PartialEq for DeadlineOperation {
        fn eq(&self, other: &Self) -> bool {
            self.state == other.state && self.txn_id == other.txn_id
        }
    }

    impl Operation for DeadlineOperation {
        type Output = bool;
        type Error = ();

        fn start(&mut self) -> aruna_core::types::Effects {
            self.state = 1;
            smallvec::smallvec![Effect::Storage(StorageEffect::StartTransaction {
                read: false
            })]
        }

        fn step(&mut self, event: Event) -> aruna_core::types::Effects {
            match (event, self.state) {
                (Event::Storage(StorageEvent::TransactionStarted { txn_id }), 1) => {
                    self.txn_id = Some(txn_id);
                    self.state = 2;
                    smallvec::smallvec![Effect::Storage(StorageEffect::Write {
                        key_space: "default".to_string(),
                        key: ByteView::from(*b"staged-marker"),
                        value: ByteView::from(*b"staged"),
                        txn_id: Some(txn_id),
                    })]
                }
                (Event::Storage(StorageEvent::WriteResult { .. }), 2) => {
                    self.ready.notify_one();
                    self.state = 3;
                    smallvec::smallvec![Effect::Blob(BlobEffect::SpoolHidden {
                        namespace: ulid::Ulid::from_bytes([7u8; 16]),
                        name: "deadline".to_string(),
                        created_by: aruna_core::UserId::default(),
                        max_bytes: None,
                        deadline: None,
                        blob: aruna_core::stream::BackendStream::new(
                            futures_util::stream::pending::<
                                Result<bytes::Bytes, aruna_core::stream::StreamError>,
                            >(),
                        ),
                    })]
                }
                (Event::Blob(_), 3) => {
                    self.state = 4;
                    smallvec::smallvec![]
                }
                _ => smallvec::smallvec![],
            }
        }

        fn is_complete(&self) -> bool {
            self.state == 4
        }

        fn finalize(self) -> Result<Self::Output, Self::Error> {
            Ok(self.state == 4)
        }

        fn abort(&mut self) -> aruna_core::types::Effects {
            self.state = 4;
            smallvec::smallvec![]
        }
    }

    async fn marker_absent(context: &DriverContext) -> bool {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: "default".to_string(),
                key: ByteView::from(*b"abort-marker"),
                txn_id: None,
            })
            .await
        else {
            panic!("unexpected storage event");
        };
        value.is_none()
    }

    async fn staged_value(context: &DriverContext) -> Option<ByteView> {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: "default".to_string(),
                key: ByteView::from(*b"staged-marker"),
                txn_id: None,
            })
            .await
        else {
            panic!("unexpected storage event");
        };
        value
    }

    async fn transaction_reopens(context: &DriverContext) -> bool {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::StartTransaction { read: false })
            .await
        else {
            return false;
        };
        matches!(
            context
                .storage_handle
                .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                .await,
            Event::Storage(StorageEvent::TransactionAborted { txn_id: aborted })
                if aborted == txn_id
        )
    }

    fn test_context() -> (tempfile::TempDir, DriverContext) {
        let directory = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(directory.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        (directory, context)
    }

    async fn blob_context() -> (tempfile::TempDir, DriverContext) {
        let (directory, mut context) = net_context().await;
        context.net_handle = None;
        (directory, context)
    }

    /// Storage, a file system blob backend and a net handle in one temporary folder.
    async fn net_context() -> (tempfile::TempDir, DriverContext) {
        let directory = tempdir().unwrap();
        let root = directory.path().to_str().unwrap().to_string();
        let blob_root = format!("{root}/blobstore");
        std::fs::create_dir_all(&blob_root).unwrap();
        let storage_handle = storage::FjallStorage::open(&root).unwrap();
        let net_handle =
            aruna_net::NetHandle::new(aruna_net::NetConfig::default(), storage_handle.clone())
                .await
                .unwrap();
        let blob_handle = aruna_blob::blob::BlobHandler::new(
            aruna_core::structs::storage::blob::BackendConfig {
                backend_type: aruna_core::structs::storage::blob::Backend::FileSystem,
                root: blob_root,
                service_config: std::collections::HashMap::new(),
                bucket_prefix: Some("aruna-test-".to_string()),
                max_bucket_size: Some(1),
                multipart_bucket: Some("uploaded-parts".to_string()),
                timeouts: Default::default(),
            },
            storage_handle.clone(),
            net_handle.clone(),
        )
        .await
        .unwrap();
        (
            directory,
            DriverContext {
                storage_handle,
                net_handle: Some(net_handle),
                blob_handle: Some(blob_handle),
                metadata_handle: None,
                task_handle: None,
                compute_handle: None,
            },
        )
    }

    #[test]
    fn commit_unknown_safe() {
        let id = ulid::Ulid::generate();
        let mut tracker = TransactionTracker::default();
        let started = Event::Storage(StorageEvent::TransactionStarted { txn_id: id });
        let failed = Event::Storage(StorageEvent::Error {
            error: StorageError::CommitFailed,
        });
        tracker.observe(Some(TransactionEffect::Start), &started);
        tracker.observe(Some(TransactionEffect::Commit(id)), &failed);
        assert_eq!(
            tracker.states.get(&id),
            Some(&TransactionState::CommitUnknown)
        );
        assert!(tracker.pending().is_empty());
    }

    #[test]
    fn commit_failure_kept() {
        let (handle, _receivers) = storage::StorageHandle::new();
        let id = ulid::Ulid::generate();
        let mut tracker = TransactionTracker::new(handle.clone());
        tracker.observe(
            Some(TransactionEffect::Start),
            &Event::Storage(StorageEvent::TransactionStarted { txn_id: id }),
        );
        tracker.observe(
            Some(TransactionEffect::Commit(id)),
            &Event::Storage(StorageEvent::Error {
                error: StorageError::CommitFailed,
            }),
        );
        assert_eq!(
            tracker.states.get(&id),
            Some(&TransactionState::CommitUnknown)
        );
        assert!(!handle.commit_unknown(id));
    }

    #[test]
    fn commit_queue_kept() {
        let id = ulid::Ulid::generate();
        let mut tracker = TransactionTracker::default();
        let started = Event::Storage(StorageEvent::TransactionStarted { txn_id: id });
        let queued = Event::Storage(StorageEvent::Error {
            error: StorageError::QueueFull,
        });
        tracker.observe(Some(TransactionEffect::Start), &started);
        tracker.observe(Some(TransactionEffect::Commit(id)), &queued);
        assert_eq!(tracker.states.get(&id), Some(&TransactionState::Open));
    }

    #[test]
    fn commit_capacity_kept() {
        // Cleanup capacity proves no commit, so the transaction stays open, not uncertain.
        let id = ulid::Ulid::generate();
        let mut tracker = TransactionTracker::default();
        let started = Event::Storage(StorageEvent::TransactionStarted { txn_id: id });
        let exhausted = Event::Storage(StorageEvent::Error {
            error: StorageError::CleanupCapacity,
        });
        tracker.observe(Some(TransactionEffect::Start), &started);
        tracker.observe(Some(TransactionEffect::Commit(id)), &exhausted);
        assert_eq!(tracker.states.get(&id), Some(&TransactionState::Open));
    }

    #[test]
    fn abort_failure_kept() {
        let id = ulid::Ulid::generate();
        let mut tracker = TransactionTracker::default();
        let started = Event::Storage(StorageEvent::TransactionStarted { txn_id: id });
        let failed = Event::Storage(StorageEvent::Error {
            error: StorageError::WriteError("boom".to_string()),
        });
        tracker.observe(Some(TransactionEffect::Start), &started);
        tracker.observe(Some(TransactionEffect::Abort(id)), &failed);
        assert_eq!(
            tracker.states.get(&id),
            Some(&TransactionState::AbortFailed)
        );
        assert_eq!(tracker.pending(), vec![(id, TransactionState::AbortFailed)]);
    }

    #[test]
    fn tracker_bounds() {
        let mut tracker = TransactionTracker::default();
        for _ in 0..MAX_TRACKED_TRANSACTIONS {
            let id = ulid::Ulid::generate();
            let started = Event::Storage(StorageEvent::TransactionStarted { txn_id: id });
            assert!(!tracker.reject_start(Some(TransactionEffect::Start)));
            tracker.observe(Some(TransactionEffect::Start), &started);
        }
        let id = ulid::Ulid::generate();
        let started = Event::Storage(StorageEvent::TransactionStarted { txn_id: id });
        assert!(tracker.reject_start(Some(TransactionEffect::Start)));
        tracker.observe(Some(TransactionEffect::Start), &started);
        assert_eq!(tracker.states.len(), MAX_TRACKED_TRANSACTIONS);
    }

    #[test]
    fn deadline_abort_policy() {
        // A suboperation stops at an acknowledged commit; a parent continues
        // only through the operation's opt-in.
        assert!(DeadlineAbort::BeforeCommitOnly.allows(false, false));
        assert!(!DeadlineAbort::BeforeCommitOnly.allows(true, true));
        assert!(DeadlineAbort::OperationOptIn.allows(false, false));
        assert!(!DeadlineAbort::OperationOptIn.allows(true, false));
        assert!(DeadlineAbort::OperationOptIn.allows(true, true));
    }

    #[test]
    fn transaction_aborts_filtered() {
        // A transaction whose commit outcome is unknown must not be aborted by
        // later effects: storage owns its resolution.
        let id = ulid::Ulid::generate();
        let mut tracker = TransactionTracker::default();
        tracker.observe(
            Some(TransactionEffect::Start),
            &Event::Storage(StorageEvent::TransactionStarted { txn_id: id }),
        );
        tracker.observe(
            Some(TransactionEffect::Commit(id)),
            &Event::Storage(StorageEvent::Error {
                error: StorageError::CommitFailed,
            }),
        );
        let mut queue = VecDeque::new();
        extend_unblocked(
            &mut queue,
            smallvec::smallvec![
                Effect::Storage(StorageEffect::AbortTransaction { txn_id: id }),
                Effect::Search(),
            ],
            &tracker,
        );
        assert_eq!(queue.len(), 1);
        assert!(matches!(queue.pop_front(), Some(Effect::Search())));
    }

    #[tokio::test]
    async fn drive_commit_safe() {
        let directory = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(directory.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let result = drive(
            MarkerAbortOperation {
                state: 0,
                fail: false,
                txn_id: None,
            },
            &context,
        )
        .await;

        assert!(result.is_ok());
        assert!(marker_absent(&context).await);
        assert_eq!(
            staged_value(&context).await,
            Some(ByteView::from(*b"staged"))
        );
        assert!(transaction_reopens(&context).await);
    }

    #[tokio::test]
    async fn drive_error_cleanup() {
        let directory = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(directory.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let result = drive(
            MarkerAbortOperation {
                state: 0,
                fail: true,
                txn_id: None,
            },
            &context,
        )
        .await;

        assert!(result.is_err());
        assert!(marker_absent(&context).await);
        assert_eq!(staged_value(&context).await, None);
        assert!(transaction_reopens(&context).await);
    }

    #[tokio::test]
    async fn deadline_commit_safe() {
        let (_directory, context) = test_context();
        let result = crate::driver::drive_until(
            MarkerAbortOperation {
                state: 0,
                fail: false,
                txn_id: None,
            },
            &context,
            tokio::time::Instant::now() + std::time::Duration::from_secs(30),
        )
        .await;

        assert!(result.is_ok());
        assert!(marker_absent(&context).await);
        assert_eq!(
            staged_value(&context).await,
            Some(ByteView::from(*b"staged"))
        );
        assert!(transaction_reopens(&context).await);
    }

    #[tokio::test]
    async fn dropped_drive_aborts() {
        let (_directory, context) = blob_context().await;
        let ready = Arc::new(tokio::sync::Notify::new());
        let task_ready = ready.clone();
        let task_context = context.clone();
        let task = tokio::spawn(async move {
            drive(
                DeadlineOperation {
                    state: 0,
                    txn_id: None,
                    ready: task_ready,
                },
                &task_context,
            )
            .await
        });

        // The transaction is open while the blob effect is waiting.
        // Cancellation must transfer it to storage rather than strand it.
        ready.notified().await;
        task.abort();
        let _ = task.await;

        assert_eq!(staged_value(&context).await, None);
        assert!(transaction_reopens(&context).await);
    }

    #[tokio::test]
    async fn deadline_rollback() {
        let temp_dir = tempdir().unwrap();
        let temp_root = temp_dir.path().to_str().unwrap().to_string();
        let blob_root = format!("{temp_root}/blobstore");
        std::fs::create_dir_all(&blob_root).unwrap();
        let storage_handle = storage::FjallStorage::open(&temp_root).unwrap();
        let net_handle =
            aruna_net::NetHandle::new(aruna_net::NetConfig::default(), storage_handle.clone())
                .await
                .unwrap();
        let blob_handle = aruna_blob::blob::BlobHandler::new(
            aruna_core::structs::storage::blob::BackendConfig {
                backend_type: aruna_core::structs::storage::blob::Backend::FileSystem,
                root: blob_root,
                service_config: std::collections::HashMap::new(),
                bucket_prefix: Some("aruna-test-".to_string()),
                max_bucket_size: Some(1),
                multipart_bucket: Some("uploaded-parts".to_string()),
                timeouts: Default::default(),
            },
            storage_handle.clone(),
            net_handle,
        )
        .await
        .unwrap();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: Some(blob_handle),
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        // Paused only now: the handles above wait on real I/O, and auto-advance would fire
        // their setup timeouts on a slow machine.
        tokio::time::pause();
        let ready = Arc::new(tokio::sync::Notify::new());
        let operation = DeadlineOperation {
            state: 0,
            txn_id: None,
            ready: ready.clone(),
        };
        // Shorter than STORAGE_REQUEST_TIMEOUT so the advance below can only
        // fire the drive deadline, never a tied storage request timeout.
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        let task_context = context.clone();
        let task = tokio::spawn(async move {
            crate::driver::drive_until(operation, &task_context, deadline).await
        });

        ready.notified().await;
        tokio::time::advance(std::time::Duration::from_secs(6)).await;
        assert!(task.await.unwrap().unwrap());
        // Real time again: auto-advance would fire the request timeouts of the
        // storage roundtrips below before the worker thread can answer.
        tokio::time::resume();
        assert_eq!(staged_value(&context).await, None);
        assert!(transaction_reopens(&context).await);
    }

    #[derive(Debug, PartialEq)]
    struct CommitDeadline {
        state: u8,
        txn_id: TxnId,
        output: Option<Result<(), u8>>,
    }

    impl Operation for CommitDeadline {
        type Output = ();
        type Error = u8;

        fn start(&mut self) -> aruna_core::types::Effects {
            self.state = 1;
            smallvec::smallvec![Effect::Storage(StorageEffect::CommitTransaction {
                txn_id: self.txn_id,
            })]
        }

        fn step(&mut self, event: Event) -> aruna_core::types::Effects {
            if matches!(
                event,
                Event::Storage(StorageEvent::TransactionCommitted { .. })
            ) {
                self.state = 2;
                return smallvec::smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: "default".to_string(),
                    key: ByteView::from(*b"after-commit"),
                    txn_id: None,
                })];
            }
            smallvec::smallvec![]
        }

        fn is_complete(&self) -> bool {
            self.output.is_some()
        }

        fn finalize(self) -> Result<Self::Output, Self::Error> {
            // Mirrors the operations whose finalize assumes a terminal state.
            self.output.expect("operation must set output")
        }

        fn abort(&mut self) -> aruna_core::types::Effects {
            self.output.get_or_insert(Err(self.state));
            smallvec::smallvec![]
        }
    }

    #[tokio::test]
    async fn deadline_after_commit() {
        // The commit suppresses the abort effects, so the driver still has to
        // hand finalize a terminal operation instead of an unset output.
        let (_directory, context) = test_context();
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::StartTransaction { read: false })
            .await
        else {
            panic!("transaction starts");
        };

        let result = crate::driver::drive_until(
            CommitDeadline {
                state: 0,
                txn_id,
                output: None,
            },
            &context,
            tokio::time::Instant::now(),
        )
        .await;

        assert_eq!(result, Err(2));
    }

    /// Opts in only after its commit, like a multipart completion that must
    /// reopen the upload its committed mark left `Completing`.
    #[derive(Debug)]
    struct LateOptIn {
        state: u8,
        ready: Arc<tokio::sync::Notify>,
    }

    impl PartialEq for LateOptIn {
        fn eq(&self, other: &Self) -> bool {
            self.state == other.state
        }
    }

    impl Operation for LateOptIn {
        type Output = ();
        type Error = ();

        fn start(&mut self) -> aruna_core::types::Effects {
            self.state = 1;
            smallvec::smallvec![Effect::Storage(StorageEffect::StartTransaction {
                read: false
            })]
        }

        fn step(&mut self, event: Event) -> aruna_core::types::Effects {
            match (event, self.state) {
                (Event::Storage(StorageEvent::TransactionStarted { txn_id }), 1) => {
                    self.state = 2;
                    smallvec::smallvec![Effect::Storage(StorageEffect::CommitTransaction {
                        txn_id
                    })]
                }
                (Event::Storage(StorageEvent::TransactionCommitted { .. }), 2) => {
                    self.ready.notify_one();
                    self.state = 3;
                    smallvec::smallvec![pending_blob()]
                }
                (Event::Storage(StorageEvent::WriteResult { .. }), 4) => {
                    self.state = 5;
                    smallvec::smallvec![]
                }
                _ => smallvec::smallvec![],
            }
        }

        fn is_complete(&self) -> bool {
            self.state == 5
        }

        fn finalize(self) -> Result<Self::Output, Self::Error> {
            if self.state == 5 { Ok(()) } else { Err(()) }
        }

        fn abort_after_commit(&self) -> bool {
            self.state >= 3
        }

        fn abort(&mut self) -> aruna_core::types::Effects {
            self.state = 4;
            smallvec::smallvec![Effect::Storage(StorageEffect::Write {
                key_space: "default".to_string(),
                key: ByteView::from(*b"abort-marker"),
                value: ByteView::from(*b"ran"),
                txn_id: None,
            })]
        }
    }

    #[tokio::test]
    async fn late_opt_in() {
        // The storage roundtrips run in real time; only the deadline uses virtual time.
        let (_directory, context) = blob_context().await;
        let ready = Arc::new(tokio::sync::Notify::new());
        let operation = LateOptIn {
            state: 0,
            ready: ready.clone(),
        };
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3600);
        let task_context = context.clone();
        let task = tokio::spawn(async move {
            crate::driver::drive_until(operation, &task_context, deadline).await
        });

        ready.notified().await;
        tokio::time::pause();
        tokio::time::advance(std::time::Duration::from_secs(3601)).await;
        tokio::time::resume();

        // The opt-in turned on after the commit, so the abort effect must still run.
        assert_eq!(task.await.unwrap(), Ok(()));
        assert!(!marker_absent(&context).await);
    }

    #[derive(Debug, PartialEq)]
    struct NestedTransactionOperation {
        done: bool,
    }

    impl Operation for NestedTransactionOperation {
        type Output = ();
        type Error = ();

        fn start(&mut self) -> aruna_core::types::Effects {
            smallvec::smallvec![Effect::SubOperation(boxed_suboperation(
                MarkerAbortOperation {
                    state: 0,
                    fail: true,
                    txn_id: None,
                },
                |_| Event::SubOperation(SubOperationEvent::DepthLimitExceeded { max_depth: 0 }),
            ))]
        }

        fn step(&mut self, _: Event) -> aruna_core::types::Effects {
            self.done = true;
            smallvec::smallvec![]
        }

        fn is_complete(&self) -> bool {
            self.done
        }

        fn finalize(self) -> Result<Self::Output, Self::Error> {
            Ok(())
        }

        fn abort(&mut self) -> aruna_core::types::Effects {
            smallvec::smallvec![]
        }
    }

    fn pending_blob() -> Effect {
        Effect::Blob(BlobEffect::SpoolHidden {
            namespace: ulid::Ulid::from_bytes([9u8; 16]),
            name: "nested-deadline".to_string(),
            created_by: aruna_core::UserId::default(),
            max_bytes: None,
            deadline: None,
            blob: aruna_core::stream::BackendStream::new(futures_util::stream::pending::<
                Result<bytes::Bytes, aruna_core::stream::StreamError>,
            >()),
        })
    }

    #[derive(Debug)]
    struct PendingTxn {
        state: u8,
        txn_id: Option<TxnId>,
        commit: bool,
        seen: Arc<std::sync::Mutex<Option<TxnId>>>,
        aborted: Arc<std::sync::atomic::AtomicBool>,
        ready: Arc<tokio::sync::Notify>,
    }

    impl PartialEq for PendingTxn {
        fn eq(&self, other: &Self) -> bool {
            self.state == other.state && self.txn_id == other.txn_id && self.commit == other.commit
        }
    }

    impl Operation for PendingTxn {
        type Output = bool;
        type Error = ();

        fn start(&mut self) -> aruna_core::types::Effects {
            self.state = 1;
            smallvec::smallvec![Effect::Storage(StorageEffect::StartTransaction {
                read: false
            })]
        }

        fn step(&mut self, event: Event) -> aruna_core::types::Effects {
            match (event, self.state) {
                (Event::Storage(StorageEvent::TransactionStarted { txn_id }), 1) => {
                    self.txn_id = Some(txn_id);
                    *self.seen.lock().unwrap() = Some(txn_id);
                    self.state = 2;
                    smallvec::smallvec![Effect::Storage(StorageEffect::Write {
                        key_space: "default".to_string(),
                        key: ByteView::from(*b"nested-staged"),
                        value: ByteView::from(*b"staged"),
                        txn_id: Some(txn_id),
                    })]
                }
                (Event::Storage(StorageEvent::WriteResult { .. }), 2) if self.commit => {
                    self.state = 3;
                    smallvec::smallvec![Effect::Storage(StorageEffect::CommitTransaction {
                        txn_id: self.txn_id.expect("transaction id recorded"),
                    })]
                }
                (Event::Storage(StorageEvent::WriteResult { .. }), 2) => {
                    self.ready.notify_one();
                    self.state = 4;
                    smallvec::smallvec![pending_blob()]
                }
                (Event::Storage(StorageEvent::TransactionCommitted { .. }), 3) => {
                    self.ready.notify_one();
                    self.state = 4;
                    smallvec::smallvec![pending_blob()]
                }
                (Event::Blob(_), 4) => {
                    self.state = 5;
                    smallvec::smallvec![]
                }
                _ => smallvec::smallvec![],
            }
        }

        fn is_complete(&self) -> bool {
            self.state == 5
        }

        fn finalize(self) -> Result<Self::Output, Self::Error> {
            Ok(self.state == 5)
        }

        fn abort(&mut self) -> aruna_core::types::Effects {
            self.aborted
                .store(true, std::sync::atomic::Ordering::SeqCst);
            self.state = 5;
            smallvec::smallvec![]
        }
    }

    #[derive(Debug)]
    struct NestedDeadline {
        commit: bool,
        done: bool,
        seen: Arc<std::sync::Mutex<Option<TxnId>>>,
        aborted: Arc<std::sync::atomic::AtomicBool>,
        ready: Arc<tokio::sync::Notify>,
    }

    impl PartialEq for NestedDeadline {
        fn eq(&self, other: &Self) -> bool {
            self.commit == other.commit && self.done == other.done
        }
    }

    impl Operation for NestedDeadline {
        type Output = ();
        type Error = ();

        fn start(&mut self) -> aruna_core::types::Effects {
            smallvec::smallvec![Effect::SubOperation(boxed_suboperation(
                PendingTxn {
                    state: 0,
                    txn_id: None,
                    commit: self.commit,
                    seen: self.seen.clone(),
                    aborted: self.aborted.clone(),
                    ready: self.ready.clone(),
                },
                |_| Event::SubOperation(SubOperationEvent::NotificationsEmitted),
            ))]
        }

        fn step(&mut self, _: Event) -> aruna_core::types::Effects {
            self.done = true;
            smallvec::smallvec![]
        }

        fn is_complete(&self) -> bool {
            self.done
        }

        fn finalize(self) -> Result<Self::Output, Self::Error> {
            Ok(())
        }

        fn abort(&mut self) -> aruna_core::types::Effects {
            self.done = true;
            smallvec::smallvec![]
        }
    }

    #[tokio::test]
    async fn subop_error_cleanup() {
        let (_directory, context) = test_context();
        let result = drive(NestedTransactionOperation { done: false }, &context).await;

        assert!(result.is_ok());
        assert!(marker_absent(&context).await);
        assert_eq!(staged_value(&context).await, None);
        assert!(transaction_reopens(&context).await);
    }

    #[tokio::test(start_paused = true)]
    async fn nested_deadline_cleanup() {
        tokio::time::resume();
        let (_directory, context) = blob_context().await;
        tokio::time::pause();
        let seen = Arc::new(std::sync::Mutex::new(None));
        let aborted = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let ready = Arc::new(tokio::sync::Notify::new());
        let operation = NestedDeadline {
            commit: false,
            done: false,
            seen: seen.clone(),
            aborted: aborted.clone(),
            ready: ready.clone(),
        };
        // Shorter than STORAGE_REQUEST_TIMEOUT so the advance below can only
        // fire the drive deadline, never a tied storage request timeout.
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        let task_context = context.clone();
        let task = tokio::spawn(async move {
            crate::driver::drive_until(operation, &task_context, deadline).await
        });

        ready.notified().await;
        tokio::task::yield_now().await;
        tokio::time::advance(std::time::Duration::from_secs(6)).await;
        assert!(task.await.unwrap().is_ok());
        // Real time again: auto-advance would fire the request timeouts of the
        // storage roundtrips below before the worker thread can answer.
        tokio::time::resume();
        assert!(aborted.load(std::sync::atomic::Ordering::SeqCst));

        let txn_id = seen.lock().unwrap().expect("child transaction recorded");
        assert!(matches!(
            context
                .storage_handle
                .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                .await,
            Event::Storage(StorageEvent::Error {
                error: StorageError::TransactionNotFound
            })
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn nested_commit_survives() {
        tokio::time::resume();
        let (_directory, context) = blob_context().await;
        tokio::time::pause();
        let seen = Arc::new(std::sync::Mutex::new(None));
        let aborted = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let ready = Arc::new(tokio::sync::Notify::new());
        let operation = NestedDeadline {
            commit: true,
            done: false,
            seen: seen.clone(),
            aborted: aborted.clone(),
            ready: ready.clone(),
        };
        // Shorter than STORAGE_REQUEST_TIMEOUT so the advance below can only
        // fire the drive deadline, never a tied storage request timeout.
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        let task_context = context.clone();
        let task = tokio::spawn(async move {
            crate::driver::drive_until(operation, &task_context, deadline).await
        });

        ready.notified().await;
        tokio::task::yield_now().await;
        tokio::time::advance(std::time::Duration::from_secs(6)).await;
        assert!(task.await.unwrap().is_ok());
        // Real time again: auto-advance would fire the request timeouts of the
        // storage roundtrips below before the worker thread can answer.
        tokio::time::resume();
        assert!(!aborted.load(std::sync::atomic::Ordering::SeqCst));
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: "default".to_string(),
                key: ByteView::from(*b"nested-staged"),
                txn_id: None,
            })
            .await
        else {
            panic!("unexpected storage event");
        };
        assert_eq!(value, Some(ByteView::from(*b"staged")));

        let txn_id = seen.lock().unwrap().expect("child transaction recorded");
        assert!(matches!(
            context
                .storage_handle
                .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                .await,
            Event::Storage(StorageEvent::Error {
                error: StorageError::TransactionNotFound
            })
        ));
    }

    /// Never finishes on its own, so only the deadline can end it. The step cap
    /// keeps a failing test from spinning instead of hanging.
    #[derive(Debug, PartialEq)]
    struct StallingOperation {
        aborted: bool,
        steps: usize,
    }

    impl Operation for StallingOperation {
        type Output = bool;
        type Error = Infallible;

        fn start(&mut self) -> aruna_core::types::Effects {
            smallvec::smallvec![Effect::Storage(StorageEffect::Read {
                key_space: "default".to_string(),
                key: ByteView::from(*b"stall"),
                txn_id: None,
            })]
        }

        fn step(&mut self, _: Event) -> aruna_core::types::Effects {
            self.steps += 1;
            if self.is_complete() {
                return smallvec::smallvec![];
            }
            self.start()
        }

        fn is_complete(&self) -> bool {
            self.aborted || self.steps > 1_000
        }

        fn finalize(self) -> Result<Self::Output, Self::Error> {
            Ok(self.aborted)
        }

        fn abort(&mut self) -> aruna_core::types::Effects {
            self.aborted = true;
            smallvec::smallvec![Effect::Storage(StorageEffect::Write {
                key_space: "default".to_string(),
                key: ByteView::from(*b"cleanup"),
                value: ByteView::from(*b"done"),
                txn_id: None,
            })]
        }
    }

    #[tokio::test]
    async fn deadline_runs_abort() {
        // Racing a timeout against the whole drive would drop the abort path.
        let directory = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(directory.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let aborted = crate::driver::drive_until(
            StallingOperation {
                aborted: false,
                steps: 0,
            },
            &context,
            tokio::time::Instant::now(),
        )
        .await
        .unwrap();

        assert!(aborted, "the deadline must end the operation");
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: "default".to_string(),
                key: ByteView::from(*b"cleanup"),
                txn_id: None,
            })
            .await
        else {
            panic!("unexpected storage event")
        };
        assert_eq!(value, Some(ByteView::from(*b"done")));
    }

    #[tokio::test]
    pub async fn test_driver() {
        let random_path = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(random_path.path().to_str().unwrap()).unwrap();

        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let operation = TestOperation::new();
        let result = drive(operation, &context).await;
        assert!(result.is_ok());
    }

    #[derive(Debug, PartialEq)]
    struct EffectOrderOperation {
        observed: Vec<&'static str>,
    }

    impl EffectOrderOperation {
        fn new() -> Self {
            Self {
                observed: Vec::new(),
            }
        }
    }

    impl Operation for EffectOrderOperation {
        type Output = Vec<&'static str>;
        type Error = ();

        fn start(&mut self) -> aruna_core::types::Effects {
            smallvec::smallvec![
                Effect::Task(TaskEffect::CancelTimer {
                    key: TaskKey::RealmPresence {
                        realm_id: aruna_core::structs::identity::realm::RealmId::from_bytes(
                            [0u8; 32]
                        ),
                        node_id: iroh::SecretKey::from_bytes(&[1u8; 32]).public(),
                    },
                }),
                Effect::Search()
            ]
        }

        fn step(&mut self, event: Event) -> aruna_core::types::Effects {
            match event {
                Event::Task(_) => self.observed.push("task"),
                Event::Search() => self.observed.push("search"),
                _ => {}
            }
            smallvec::smallvec![]
        }

        fn is_complete(&self) -> bool {
            self.observed.len() == 2
        }

        fn finalize(self) -> Result<Self::Output, Self::Error> {
            Ok(self.observed)
        }

        fn abort(&mut self) -> aruna_core::types::Effects {
            smallvec::smallvec![]
        }
    }

    #[tokio::test]
    async fn driver_preserves_fifo() {
        let random_path = tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(random_path.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let operation = EffectOrderOperation::new();
        let observed = drive(operation, &context)
            .await
            .expect("drive should succeed");
        assert_eq!(observed, vec!["task", "search"]);
    }

    #[derive(Debug, PartialEq)]
    struct StagingDispatchOperation {
        observed_staging_source: bool,
    }

    impl StagingDispatchOperation {
        fn new() -> Self {
            Self {
                observed_staging_source: false,
            }
        }
    }

    impl Operation for StagingDispatchOperation {
        type Output = bool;
        type Error = ();

        fn start(&mut self) -> aruna_core::types::Effects {
            smallvec::smallvec![Effect::StagingSource(StagingSourceEffect::Head {
                access: ResolvedSourceAccess::OpenDal {
                    kind: SourceConnectorKind::Http,
                    config: std::collections::HashMap::from([(
                        "endpoint".to_string(),
                        "https://missing.example.org".to_string(),
                    )]),
                    path: "file.txt".to_string(),
                    version: None,
                },
            })]
        }

        fn step(&mut self, event: Event) -> aruna_core::types::Effects {
            self.observed_staging_source = matches!(
                event,
                Event::StagingSource(StagingSourceEvent::Error { .. })
            );
            smallvec::smallvec![]
        }

        fn is_complete(&self) -> bool {
            self.observed_staging_source
        }

        fn finalize(self) -> Result<Self::Output, Self::Error> {
            Ok(self.observed_staging_source)
        }

        fn abort(&mut self) -> aruna_core::types::Effects {
            smallvec::smallvec![]
        }
    }

    #[tokio::test]
    async fn driver_dispatches_staging() {
        let temp_dir = tempdir().unwrap();
        let temp_root = temp_dir.path().to_str().unwrap().to_string();
        let blob_root = format!("{temp_root}/blobstore");
        std::fs::create_dir_all(&blob_root).unwrap();
        let storage_handle = storage::FjallStorage::open(&temp_root).unwrap();
        let net_handle =
            aruna_net::NetHandle::new(aruna_net::NetConfig::default(), storage_handle.clone())
                .await
                .unwrap();
        let blob_handle = aruna_blob::blob::BlobHandler::new(
            aruna_core::structs::storage::blob::BackendConfig {
                backend_type: aruna_core::structs::storage::blob::Backend::FileSystem,
                root: blob_root,
                service_config: std::collections::HashMap::new(),
                bucket_prefix: Some("aruna-test-".to_string()),
                max_bucket_size: Some(1),
                multipart_bucket: Some("uploaded-parts".to_string()),
                timeouts: Default::default(),
            },
            storage_handle.clone(),
            net_handle,
        )
        .await
        .unwrap();

        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: Some(blob_handle),
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let observed = drive(StagingDispatchOperation::new(), &context)
            .await
            .expect("staging source effect should be dispatched");
        assert!(observed);
    }

    #[derive(Debug, PartialEq)]
    struct RecursiveSubOperation {
        observed: Option<Event>,
    }

    impl RecursiveSubOperation {
        fn new() -> Self {
            Self { observed: None }
        }
    }

    impl Operation for RecursiveSubOperation {
        type Output = Event;
        type Error = Infallible;

        fn start(&mut self) -> aruna_core::types::Effects {
            smallvec::smallvec![Effect::SubOperation(boxed_suboperation(
                RecursiveSubOperation::new(),
                |result| match result {
                    Ok(event) => event,
                    Err(never) => match never {},
                },
            ))]
        }

        fn step(&mut self, event: Event) -> aruna_core::types::Effects {
            self.observed = Some(event);
            smallvec::smallvec![]
        }

        fn is_complete(&self) -> bool {
            self.observed.is_some()
        }

        fn finalize(self) -> Result<Self::Output, Self::Error> {
            Ok(self
                .observed
                .expect("recursive suboperation should produce an event"))
        }

        fn abort(&mut self) -> aruna_core::types::Effects {
            smallvec::smallvec![]
        }
    }

    #[test]
    fn suboperation_depth_enforced() {
        // Driving to the depth limit nests deep futures; the default test
        // thread stack overflows, so the drive runs on a dedicated big stack.
        std::thread::Builder::new()
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                runtime.block_on(async {
                    let random_path = tempdir().unwrap();
                    let storage_handle =
                        storage::FjallStorage::open(random_path.path().to_str().unwrap()).unwrap();
                    let context = DriverContext {
                        storage_handle,
                        net_handle: None,
                        blob_handle: None,
                        metadata_handle: None,
                        task_handle: None,
                        compute_handle: None,
                    };

                    let event = drive(RecursiveSubOperation::new(), &context)
                        .await
                        .expect("recursive suboperation should resolve to depth-limit event");

                    assert!(matches!(
                        event,
                        Event::SubOperation(SubOperationEvent::DepthLimitExceeded { max_depth })
                            if max_depth == super::MAX_SUBOP_DEPTH
                    ));
                });
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[tokio::test]
    async fn bulk_skips_foreground() {
        // Background work on a bulk context never queues ahead of request effects.
        let (storage_handle, receivers) = storage::StorageHandle::new();
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        }
        .bulk();
        let read = StorageEffect::Read {
            key_space: "bulk".to_string(),
            key: ByteView::from(*b"k"),
            txn_id: None,
        };
        let pending =
            tokio::spawn(async move { context.storage_handle.send_storage_effect(read).await });

        let bulk = receivers.bulk;
        let (effect, ..) = tokio::task::spawn_blocking(move || bulk.recv())
            .await
            .unwrap()
            .expect("the effect reaches the bulk lane");
        assert!(matches!(effect, StorageEffect::Read { key_space, .. } if key_space == "bulk"));
        assert!(receivers.foreground.try_recv().is_err());
        pending.abort();
    }

    /// Members in flight now and at most, as a fake dispatch counts them.
    #[derive(Default)]
    struct Flight {
        now: AtomicUsize,
        peak: AtomicUsize,
    }

    /// Answers a storage read the seconds its first key byte names after it starts, and a
    /// reservation release after three seconds; tests run it on paused time only.
    fn slow_dispatch<'a>(
        flight: Arc<Flight>,
    ) -> impl Fn(Effect, Option<Instant>) -> BoxFuture<'a, Event> + Send + Sync {
        move |effect, _| {
            let flight = flight.clone();
            Box::pin(async move {
                let now = flight.now.fetch_add(1, SeqCst) + 1;
                flight.peak.fetch_max(now, SeqCst);
                let (seconds, event) = match effect {
                    Effect::Storage(StorageEffect::Read { key, .. }) => (
                        u64::from(key[0]),
                        Event::Storage(StorageEvent::ReadResult { key, value: None }),
                    ),
                    Effect::Blob(BlobEffect::ReleaseReservation { id }) => {
                        (3, Event::Blob(BlobEvent::ReservationReleased { id }))
                    }
                    other => panic!("unexpected member {other:?}"),
                };
                tokio::time::sleep_until(Instant::now() + Duration::from_secs(seconds)).await;
                flight.now.fetch_sub(1, SeqCst);
                event
            })
        }
    }

    fn slow_read(seconds: u8) -> Effect {
        Effect::Storage(StorageEffect::Read {
            key_space: "default".to_string(),
            key: ByteView::from([seconds]),
            txn_id: None,
        })
    }

    fn read_seconds(result: &GroupResult) -> Option<u8> {
        match result {
            GroupResult::Done(event) => match event.as_ref() {
                Event::Storage(StorageEvent::ReadResult { key, .. }) => Some(key[0]),
                _ => None,
            },
            _ => None,
        }
    }

    /// A context whose storage has no worker, for groups that only use a fake dispatch.
    fn bare_context() -> DriverContext {
        let (storage_handle, _) = storage::StorageHandle::new();
        DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        }
    }

    const UNBOUNDED: MemberStop = MemberStop {
        at: None,
        protect_managed: true,
    };

    #[tokio::test(start_paused = true)]
    async fn group_waits_slowest() {
        let context = bare_context();
        let flight = Arc::new(Flight::default());
        let dispatch = Box::new(slow_dispatch(flight.clone()));
        let mut run = GroupRun::new(dispatch, &context, 8, UNBOUNDED);
        run.add([slow_read(3), slow_read(1), slow_read(2)]);
        let started = Instant::now();
        let results = run_all(&mut run, &mut Vec::new()).await;
        // Paused time jumps from timer to timer: the group takes its slowest member, not the
        // six seconds of the same reads one after another.
        assert_eq!(started.elapsed(), Duration::from_secs(3));
        let order: Vec<_> = results.iter().map(read_seconds).collect();
        assert_eq!(order, [Some(3), Some(1), Some(2)]);
        assert_eq!(flight.peak.load(SeqCst), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_spares_managed() {
        let context = bare_context();
        let stop = MemberStop {
            at: Some(Instant::now() + Duration::from_secs(1)),
            protect_managed: true,
        };
        let dispatch = Box::new(slow_dispatch(Arc::default()));
        let mut run = GroupRun::new(dispatch, &context, 8, stop);
        let id = ulid::Ulid::from_bytes([3u8; 16]);
        run.add([
            slow_read(5),
            Effect::Blob(BlobEffect::ReleaseReservation { id }),
        ]);
        let started = Instant::now();
        let results = run_all(&mut run, &mut Vec::new()).await;
        assert_eq!(
            results,
            [
                GroupResult::Unfinished,
                GroupResult::Done(Box::new(Event::Blob(BlobEvent::ReservationReleased { id }))),
            ]
        );
        // The managed release finishes after the deadline instead of being cut off.
        assert_eq!(started.elapsed(), Duration::from_secs(3));
    }

    /// Records window results as (index, seconds) and asks for one more member per result
    /// while it has any left.
    #[derive(Debug, PartialEq)]
    struct WindowRows {
        rest: Vec<Effect>,
        seen: Vec<(usize, Option<u8>)>,
        closed: bool,
    }

    impl Operation for WindowRows {
        type Output = ();
        type Error = ();

        fn start(&mut self) -> aruna_core::types::Effects {
            smallvec::smallvec![]
        }

        fn step(&mut self, event: Event) -> aruna_core::types::Effects {
            match event {
                Event::Group(GroupEvent::Member { index, result }) => {
                    self.seen.push((index, read_seconds(&result)));
                    self.rest.pop().into_iter().collect()
                }
                Event::Group(GroupEvent::Closed) => {
                    self.closed = true;
                    smallvec::smallvec![]
                }
                other => panic!("unexpected event {other:?}"),
            }
        }

        fn is_complete(&self) -> bool {
            self.closed
        }

        fn finalize(self) -> Result<Self::Output, Self::Error> {
            Ok(())
        }

        fn abort(&mut self) -> aruna_core::types::Effects {
            smallvec::smallvec![]
        }
    }

    #[tokio::test(start_paused = true)]
    async fn window_refills_slots() {
        let context = bare_context();
        let flight = Arc::new(Flight::default());
        let dispatch = Box::new(slow_dispatch(flight.clone()));
        let mut run = GroupRun::new(dispatch, &context, 2, UNBOUNDED);
        run.add([slow_read(4), slow_read(1)]);
        let mut operation = WindowRows {
            rest: vec![slow_read(1)],
            seen: Vec::new(),
            closed: false,
        };
        let end = run_window(
            &mut ParentRun(&mut operation),
            &mut run,
            &mut Vec::new(),
            GroupEnd::Expired,
        )
        .await;
        assert!(matches!(end, GroupEnd::Stepped(effects) if effects.is_empty()));
        // Member 1 frees its slot after one second, so member 2 runs before member 0 ends.
        assert_eq!(operation.seen, [(1, Some(1)), (2, Some(1)), (0, Some(4))]);
        assert!(operation.closed);
        assert_eq!(flight.peak.load(SeqCst), 2);
    }

    /// Emits one group and keeps its results.
    #[derive(Debug, PartialEq)]
    struct OneGroup {
        members: Vec<Effect>,
        results: Option<Vec<GroupResult>>,
    }

    impl Operation for OneGroup {
        type Output = Vec<GroupResult>;
        type Error = ();

        fn start(&mut self) -> aruna_core::types::Effects {
            smallvec::smallvec![Effect::Group(EffectGroup {
                members: std::mem::take(&mut self.members),
                limit: 4,
                form: GroupForm::All { deadline: None },
            })]
        }

        fn step(&mut self, event: Event) -> aruna_core::types::Effects {
            if let Event::Group(GroupEvent::Finished { results }) = event {
                self.results = Some(results);
            }
            smallvec::smallvec![]
        }

        fn is_complete(&self) -> bool {
            self.results.is_some()
        }

        fn finalize(self) -> Result<Self::Output, Self::Error> {
            self.results.ok_or(())
        }

        fn abort(&mut self) -> aruna_core::types::Effects {
            smallvec::smallvec![]
        }
    }

    #[tokio::test]
    async fn group_refuses_transaction() {
        let (_directory, context) = test_context();
        let read = StorageEffect::Read {
            key_space: "default".to_string(),
            key: ByteView::from(*b"absent"),
            txn_id: None,
        };
        let operation = OneGroup {
            members: vec![
                Effect::Storage(StorageEffect::StartTransaction { read: false }),
                Effect::Storage(read),
            ],
            results: None,
        };
        let read = Event::Storage(StorageEvent::ReadResult {
            key: ByteView::from(*b"absent"),
            value: None,
        });
        assert_eq!(
            drive(operation, &context).await,
            Ok(vec![
                GroupResult::Refused,
                GroupResult::Done(Box::new(read))
            ])
        );
    }

    /// Runs one member that names a tenant backend and asks `idle` after the group ended.
    struct HeldGroup {
        backend_id: ulid::Ulid,
        idle: Box<dyn Fn() -> bool + Send>,
        idle_after: Option<bool>,
    }

    impl std::fmt::Debug for HeldGroup {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("HeldGroup")
                .field("idle_after", &self.idle_after)
                .finish_non_exhaustive()
        }
    }

    impl PartialEq for HeldGroup {
        fn eq(&self, other: &Self) -> bool {
            self.idle_after == other.idle_after
        }
    }

    impl Operation for HeldGroup {
        type Output = bool;
        type Error = ();

        fn start(&mut self) -> aruna_core::types::Effects {
            let location = BackendLocation {
                backend: BackendRef::Group(self.backend_id),
                storage_class: None,
                root: "/tmp".to_string(),
                storage_bucket: "bucket".to_string(),
                backend_path: "blob/held".to_string(),
                ulid: self.backend_id,
                format: Default::default(),
                created_by: aruna_core::UserId::default(),
                created_at: std::time::SystemTime::UNIX_EPOCH,
                staging: false,
                partial: false,
                blob_size: 1,
                hashes: Default::default(),
            };
            smallvec::smallvec![Effect::Group(EffectGroup {
                members: vec![Effect::Blob(BlobEffect::Delete { location })],
                limit: 1,
                form: GroupForm::All { deadline: None },
            })]
        }

        fn step(&mut self, _: Event) -> aruna_core::types::Effects {
            self.idle_after = Some((self.idle)());
            smallvec::smallvec![]
        }

        fn is_complete(&self) -> bool {
            self.idle_after.is_some()
        }

        fn finalize(self) -> Result<Self::Output, Self::Error> {
            self.idle_after.ok_or(())
        }

        fn abort(&mut self) -> aruna_core::types::Effects {
            smallvec::smallvec![]
        }
    }

    #[tokio::test]
    async fn hold_outlives_member() {
        let (_directory, context) = blob_context().await;
        let blob = context.blob_handle.clone().expect("blob handle");
        let backend_id = ulid::Ulid::from_bytes([4u8; 16]);
        let probe = blob.clone();
        let operation = HeldGroup {
            backend_id,
            idle: Box::new(move || probe.idle_generation(backend_id).is_some()),
            idle_after: None,
        };
        // Removal needs an idle generation: the member ended, but its parent still holds.
        assert_eq!(drive(operation, &context).await, Ok(false));
        assert!(blob.idle_generation(backend_id).is_some());
    }

    /// Fails with a conflict until attempt `conflicts`, then answers its attempt.
    #[derive(Debug, PartialEq)]
    struct Conflicted {
        attempt: usize,
        conflicts: usize,
    }

    impl Operation for Conflicted {
        type Output = usize;
        type Error = StorageError;

        fn start(&mut self) -> aruna_core::types::Effects {
            smallvec::smallvec![]
        }

        fn step(&mut self, _: Event) -> aruna_core::types::Effects {
            smallvec::smallvec![]
        }

        fn is_complete(&self) -> bool {
            true
        }

        fn finalize(self) -> Result<Self::Output, Self::Error> {
            match self.attempt < self.conflicts {
                true => Err(StorageError::TransactionConflict),
                false => Ok(self.attempt),
            }
        }

        fn abort(&mut self) -> aruna_core::types::Effects {
            smallvec::smallvec![]
        }

        fn is_conflict(error: &Self::Error) -> bool {
            matches!(error, StorageError::TransactionConflict)
        }
    }

    #[tokio::test(start_paused = true)]
    async fn retry_reruns_conflict() {
        let context = bare_context();
        for (conflicts, expected, attempts) in [
            (2, Ok(2), 3),
            (usize::MAX, Err(StorageError::TransactionConflict), 4),
        ] {
            let mut built = 0;
            let build = || {
                built += 1;
                Conflicted {
                    attempt: built - 1,
                    conflicts,
                }
            };
            assert_eq!(drive_retry(build, &context, &[7u8; 16]).await, expected);
            assert_eq!(built, attempts);
        }
    }

    /// Logs its name at start, then spools a stream that ends once `gate` opens, or without a
    /// gate ends at once when `finish` holds and otherwise stays open until the runner aborts.
    #[derive(Debug)]
    struct Holding {
        name: &'static str,
        scope: Option<HoldScope>,
        log: Arc<std::sync::Mutex<Vec<&'static str>>>,
        started: Arc<tokio::sync::Notify>,
        gate: Option<tokio::sync::oneshot::Receiver<()>>,
        finish: bool,
        done: bool,
    }

    impl Holding {
        fn new(name: &'static str, log: &Arc<std::sync::Mutex<Vec<&'static str>>>) -> Self {
            Self {
                name,
                scope: Some(HoldScope::DocumentReconcile),
                log: log.clone(),
                started: Arc::default(),
                gate: None,
                finish: true,
                done: false,
            }
        }
    }

    impl PartialEq for Holding {
        fn eq(&self, other: &Self) -> bool {
            self.name == other.name && self.done == other.done
        }
    }

    impl Operation for Holding {
        type Output = ();
        type Error = ();

        fn hold_scope(&self) -> Option<HoldScope> {
            self.scope
        }

        fn start(&mut self) -> aruna_core::types::Effects {
            self.log.lock().unwrap().push(self.name);
            self.started.notify_one();
            let Some(gate) = self.gate.take() else {
                self.done = self.finish;
                return smallvec::smallvec![];
            };
            let blob = futures_util::stream::once(async move {
                let _ = gate.await;
                Ok::<_, aruna_core::stream::StreamError>(bytes::Bytes::new())
            });
            smallvec::smallvec![Effect::Blob(BlobEffect::SpoolHidden {
                namespace: ulid::Ulid::from_bytes([5u8; 16]),
                name: self.name.to_string(),
                created_by: aruna_core::UserId::default(),
                max_bytes: None,
                deadline: None,
                blob: aruna_core::stream::BackendStream::new(blob),
            })]
        }

        fn step(&mut self, _: Event) -> aruna_core::types::Effects {
            self.done = true;
            smallvec::smallvec![]
        }

        fn is_complete(&self) -> bool {
            self.done
        }

        fn finalize(self) -> Result<Self::Output, Self::Error> {
            if self.done { Ok(()) } else { Err(()) }
        }

        fn abort(&mut self) -> aruna_core::types::Effects {
            smallvec::smallvec![]
        }
    }

    /// Whether nothing holds the reconcile scope right now.
    fn reconcile_free(context: &DriverContext) -> bool {
        let net = context.net_handle.as_ref().expect("net handle");
        net.hold_reconcile().now_or_never().is_some()
    }

    #[tokio::test]
    async fn scope_waits_holder() {
        let (_directory, context) = net_context().await;
        let log = Arc::default();
        let (open, gate) = tokio::sync::oneshot::channel();
        let mut first = Holding::new("first", &log);
        first.gate = Some(gate);
        let first_started = first.started.clone();
        let task_context = context.clone();
        let first_task = tokio::spawn(async move { drive(first, &task_context).await });
        first_started.notified().await;

        let second = Holding::new("second", &log);
        let task_context = context.clone();
        let second_task = tokio::spawn(async move { drive(second, &task_context).await });
        let mut other = Holding::new("other", &log);
        other.scope = None;
        assert_eq!(drive(other, &context).await, Ok(()));
        let late = Holding::new("late", &log);
        assert_eq!(drive_until(late, &context, Instant::now()).await, Err(()));
        // Paused time first runs every ready task, so the second run is parked on the hold
        // before the clock jumps an hour.
        tokio::time::pause();
        tokio::time::sleep(Duration::from_secs(3600)).await;
        tokio::time::resume();
        assert_eq!(*log.lock().unwrap(), ["first", "other"]);

        open.send(()).unwrap();
        assert_eq!(first_task.await.unwrap(), Ok(()));
        assert_eq!(second_task.await.unwrap(), Ok(()));
        assert_eq!(*log.lock().unwrap(), ["first", "other", "second"]);
        assert!(reconcile_free(&context));
    }

    #[tokio::test]
    async fn hold_released_finally() {
        let (_directory, context) = net_context().await;
        let log = Arc::default();

        // Abort: no effects and never complete, so the runner aborts it.
        let mut aborted = Holding::new("aborted", &log);
        aborted.finish = false;
        assert_eq!(drive(aborted, &context).await, Err(()));
        assert!(reconcile_free(&context));

        // Cancel: the caller drops the run while its spool waits.
        let (_open, gate) = tokio::sync::oneshot::channel::<()>();
        let mut cancelled = Holding::new("cancelled", &log);
        cancelled.gate = Some(gate);
        let started = cancelled.started.clone();
        let task_context = context.clone();
        let task = tokio::spawn(async move { drive(cancelled, &task_context).await });
        started.notified().await;
        assert!(!reconcile_free(&context));
        task.abort();
        let _ = task.await;
        assert!(reconcile_free(&context));

        // Expiry: the deadline passes while the spool waits.
        let (_open, gate) = tokio::sync::oneshot::channel::<()>();
        let mut expired = Holding::new("expired", &log);
        expired.gate = Some(gate);
        let started = expired.started.clone();
        let deadline = Instant::now() + Duration::from_secs(5);
        let task_context = context.clone();
        let task = tokio::spawn(async move { drive_until(expired, &task_context, deadline).await });
        started.notified().await;
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(6)).await;
        assert_eq!(task.await.unwrap(), Err(()));
        tokio::time::resume();
        assert!(reconcile_free(&context));
    }
}
