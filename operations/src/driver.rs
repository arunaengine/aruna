//! Runs an operation's effect loop, nests suboperations and owns open transactions.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_blob::blob::{BlobHandle, GroupHold};
use aruna_compute::ExecutorRegistry;
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::operation::{Operation, SubOperation};
use aruna_core::types::{Effects, TxnId};
use aruna_net::NetHandle;
use aruna_storage::storage;
use aruna_tasks::TaskHandle;
use std::any::{type_name, type_name_of_val};
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
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
        StorageEffect::StartTransaction { .. } => Some(TransactionEffect::Start),
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
                state.mark_expired();
                queue.clear();
                // Suppress aborts after commit. Dropping `state.holds` releases
                // backend reservations.
                if abort_policy.allows(state.committed, executable.abort_after_commit()) {
                    extend_unblocked(&mut queue, executable.abort(), &state.tracker);
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
                            state.mark_expired();
                            if let Some(operation) = expiry_log {
                                warn!(
                                    operation = %operation,
                                    "Operation deadline expired; running its abort path"
                                );
                            }
                            queue.clear();
                            if commit {
                                Event::Storage(StorageEvent::Error {
                                    error: StorageError::CommitFailed,
                                })
                            } else {
                                if abort_policy
                                    .allows(state.committed, executable.abort_after_commit())
                                {
                                    extend_unblocked(
                                        &mut queue,
                                        executable.abort(),
                                        &state.tracker,
                                    );
                                }
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

#[cfg(test)]
mod test {
    use crate::driver::{
        DeadlineAbort, DriverContext, MAX_TRACKED_TRANSACTIONS, TransactionEffect,
        TransactionState, TransactionTracker, drive, extend_unblocked,
    };
    use aruna_core::effects::{BlobEffect, Effect, StagingSourceEffect, StorageEffect};
    use aruna_core::errors::StorageError;
    use aruna_core::events::{Event, StagingSourceEvent, StorageEvent, SubOperationEvent};
    use aruna_core::operation::{Operation, boxed_suboperation};
    use aruna_core::structs::execution::source_access::ResolvedSourceAccess;
    use aruna_core::structs::execution::source_connector::SourceConnectorKind;
    use aruna_core::task::{TaskEffect, TaskKey};
    use aruna_core::types::TxnId;
    use aruna_storage::storage;
    use byteview::ByteView;
    use std::collections::VecDeque;
    use std::convert::Infallible;
    use std::sync::Arc;
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
            net_handle,
        )
        .await
        .unwrap();
        (
            directory,
            DriverContext {
                storage_handle,
                net_handle: None,
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

    #[tokio::test(start_paused = true)]
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
}
