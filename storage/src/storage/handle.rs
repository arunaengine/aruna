//! Fronts the storage worker: admits effects to a lane, fences on close and maps replies.
//! Aborts take the foreground lane, and cleanup registers before an effect is queued.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use aruna_core::effects::{Effect, StorageEffect, StoragePriority};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::telemetry::record_stage;
use async_trait::async_trait;
use crossfire::{TrySendError, mpsc};
use tokio::sync::oneshot;
use tracing::{Span, warn};
use ulid::Ulid;

use super::metrics::{InFlightGuard, StorageMetrics, StorageMetricsSnapshot};
use super::owner::TransactionOwner;
use super::telemetry::{effect_kind, storage_effect_kind, storage_effect_span, storage_event_kind};

pub type EffectHandle = (StorageEffect, ResponseSender, Span, Instant, InFlightGuard);
pub type EffectSender = crossfire::MTx<mpsc::Array<EffectHandle>>;
pub(super) type AsyncEffectSender = crossfire::MAsyncTx<mpsc::Array<EffectHandle>>;
pub type EffectReceiver = crossfire::Rx<mpsc::Array<EffectHandle>>;
pub(super) type StorageReply = (StorageEvent, ResponseToken);

pub(super) const QUEUE_CAPACITY: usize = 65_536;
// Bulk lane queues are small so background work hits QueueFull backpressure early
// instead of building an unbounded backlog ahead of foreground sync traffic.
pub(super) const EFFECT_QUEUE_CAPACITY: usize = 4_096;
pub(super) const MAX_TRANSACTION_CLEANUP: usize = 1024;
pub(super) const MAX_CLEANUP_ATTEMPTS: u8 = 2;
#[derive(Debug, Clone, Copy)]
pub(super) enum CleanupKind {
    Open,
    Abort,
    Aborted,
    CommitQueued,
    CommitUnknown,
    Committed,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct CleanupEntry {
    pub(super) kind: CleanupKind,
    pub(super) attempts: u8,
    /// An effect for this entry is in the worker queue; retries must not race it.
    pub(super) queued: bool,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct CleanupAdmission {
    txn_id: Ulid,
    requested: CleanupKind,
    previous: Option<CleanupKind>,
}

/// Named dispatch lanes for the handle's three contractual routes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EffectRoute {
    Foreground,
    Background,
    Abort,
}
pub(super) const STORAGE_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// Write-actor receivers, one per dispatch lane. Foreground is drained before
/// bulk so background work never starves sync traffic.
pub struct StorageReceivers {
    pub foreground: EffectReceiver,
    pub bulk: EffectReceiver,
}

#[derive(Clone, Debug)]
pub struct StorageHandle {
    pub(super) write_channel: EffectSender,
    pub(super) bulk_channel: EffectSender,
    pub(super) write_async: AsyncEffectSender,
    pub(super) bulk_async: AsyncEffectSender,
    pub(super) priority: StoragePriority,
    pub(super) metrics: Arc<StorageMetrics>,
    pub(super) transaction_cleanup: Arc<Mutex<BTreeMap<Ulid, CleanupEntry>>>,
    pub(super) worker: Arc<Mutex<Option<thread::JoinHandle<()>>>>,
}

#[derive(Debug)]
enum ResponseCleanup {
    Start(Option<Ulid>),
    Abort(Ulid, bool),
    Commit(Ulid),
    Terminal(Ulid),
}

#[derive(Debug)]
pub(super) struct ResponseToken {
    handle: Option<StorageHandle>,
    cleanup: Option<ResponseCleanup>,
}

#[doc(hidden)]
pub struct ResponseSender {
    sender: oneshot::Sender<StorageReply>,
    token: ResponseToken,
}

impl ResponseSender {
    pub(super) fn new(sender: oneshot::Sender<StorageReply>, token: ResponseToken) -> Self {
        Self { sender, token }
    }

    pub fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }

    pub(super) fn observe(&mut self, event: &StorageEvent) {
        self.token.observe(event);
    }

    pub fn send(mut self, event: StorageEvent) -> bool {
        self.token.observe(&event);
        self.sender.send((event, self.token)).is_ok()
    }
}
impl StorageHandle {
    pub fn new() -> (Self, StorageReceivers) {
        let (sender, foreground) = mpsc::bounded_blocking(QUEUE_CAPACITY);
        let (bulk_sender, bulk) = mpsc::bounded_blocking(EFFECT_QUEUE_CAPACITY);
        (
            StorageHandle {
                write_async: sender.clone().into_async(),
                bulk_async: bulk_sender.clone().into_async(),
                write_channel: sender,
                bulk_channel: bulk_sender,
                priority: StoragePriority::Foreground,
                metrics: Arc::new(StorageMetrics::default()),
                transaction_cleanup: Arc::new(Mutex::new(BTreeMap::new())),
                worker: Arc::new(Mutex::new(None)),
            },
            StorageReceivers { foreground, bulk },
        )
    }

    /// A handle whose effects dispatch on the bulk lane, served only when the
    /// foreground lane is idle.
    pub fn bulk(&self) -> StorageHandle {
        let mut handle = self.clone();
        handle.priority = StoragePriority::Bulk;
        handle
    }

    /// Contractual dispatch route for an effect. Aborts always route foreground
    /// so cleanup never waits behind bulk backpressure; everything else follows
    /// the handle's priority. The routes map onto distinct queues below.
    fn route_for(&self, effect: &StorageEffect) -> EffectRoute {
        if matches!(effect, StorageEffect::AbortTransaction { .. }) {
            return EffectRoute::Abort;
        }
        match self.priority {
            StoragePriority::Foreground => EffectRoute::Foreground,
            StoragePriority::Bulk => EffectRoute::Background,
        }
    }

    fn channel_for(&self, effect: &StorageEffect) -> &EffectSender {
        match self.route_for(effect) {
            EffectRoute::Foreground | EffectRoute::Abort => &self.write_channel,
            EffectRoute::Background => &self.bulk_channel,
        }
    }

    fn async_channel_for(&self, effect: &StorageEffect) -> &AsyncEffectSender {
        match self.route_for(effect) {
            EffectRoute::Foreground | EffectRoute::Abort => &self.write_async,
            EffectRoute::Background => &self.bulk_async,
        }
    }

    pub fn get_errors(&self) -> u64 {
        self.metrics.errors_total.load(Ordering::Relaxed)
    }

    /// Number of storage effects currently enqueued or being processed.
    pub fn in_flight(&self) -> u64 {
        self.metrics.in_flight.load(Ordering::Acquire)
    }

    pub fn snapshot_metrics(&self) -> StorageMetricsSnapshot {
        StorageMetricsSnapshot {
            requests_total: self.metrics.requests_total.load(Ordering::Relaxed),
            errors_total: self.metrics.errors_total.load(Ordering::Relaxed),
            conflicts_total: self.metrics.conflicts_total.load(Ordering::Relaxed),
            failed_total: self.metrics.failed_total.load(Ordering::Relaxed),
            channel_closed: self.metrics.channel_closed.load(Ordering::Relaxed),
            closed: self.writes_closed(),
            rejected_writes: self.rejected_writes(),
            last_error: self
                .metrics
                .last_error
                .lock()
                .expect("storage metrics mutex poisoned")
                .clone(),
        }
    }

    /// Transfers transaction ownership to the storage node when the caller is
    /// cancelled. Unknown commits are retained for reconciliation and never
    /// aborted; other states receive bounded local abort retries.
    pub fn retain_transaction(&self, txn_id: Ulid, commit_unknown: bool) -> bool {
        let kind = if commit_unknown {
            CleanupKind::CommitUnknown
        } else {
            CleanupKind::Abort
        };
        let mut pending = self
            .transaction_cleanup
            .lock()
            .expect("transaction cleanup mutex poisoned");
        if pending.get(&txn_id).is_some_and(|entry| {
            matches!(entry.kind, CleanupKind::Committed | CleanupKind::Aborted)
        }) {
            pending.remove(&txn_id);
            return true;
        }
        if pending.get(&txn_id).is_none() {
            return true;
        }
        if commit_unknown
            && pending
                .get(&txn_id)
                .is_some_and(|entry| matches!(entry.kind, CleanupKind::Open))
        {
            return false;
        }
        let admission = match reserve_cleanup(&mut pending, txn_id, kind) {
            Ok(admission) => admission,
            Err(_) => return false,
        };
        let enqueue = !commit_unknown
            && admission
                .previous
                .is_none_or(|previous| matches!(previous, CleanupKind::Open));
        drop(pending);
        if enqueue {
            self.send_abort(txn_id, "owner_handoff");
        }
        true
    }

    /// Number of transaction owners retained by this storage node.
    pub fn pending_transactions(&self) -> usize {
        self.transaction_cleanup
            .lock()
            .expect("transaction cleanup mutex poisoned")
            .len()
    }

    /// True after a commit was retained as possibly accepted by storage.
    pub fn commit_unknown(&self, txn_id: Ulid) -> bool {
        self.transaction_cleanup
            .lock()
            .expect("transaction cleanup mutex poisoned")
            .get(&txn_id)
            .is_some_and(|entry| {
                matches!(
                    entry.kind,
                    CleanupKind::CommitQueued
                        | CleanupKind::CommitUnknown
                        | CleanupKind::Committed
                        | CleanupKind::Aborted
                )
            })
    }

    #[tracing::instrument(
        name = "storage.handle.send_storage_effect",
        level = "debug",
        skip(self, effect),
        fields(operation = storage_effect_kind(&effect))
    )]
    pub async fn send_storage_effect(&self, effect: StorageEffect) -> Event {
        Event::Storage(self.dispatch_storage_effect(effect).await)
    }

    pub async fn start_transaction(&self, read: bool) -> Result<TransactionOwner, StorageError> {
        match self
            .dispatch_storage_effect(StorageEffect::StartTransaction { read })
            .await
        {
            StorageEvent::TransactionStarted { txn_id } => {
                Ok(TransactionOwner::new(self.clone(), txn_id))
            }
            StorageEvent::Error { error } => Err(error),
            _ => Err(StorageError::InvalidEffect),
        }
    }

    #[tracing::instrument(
        name = "storage.handle.dispatch",
        level = "debug",
        skip(self, effect),
        fields(operation = storage_effect_kind(&effect))
    )]
    pub(super) async fn dispatch_storage_effect(&self, effect: StorageEffect) -> StorageEvent {
        self.metrics.requests_total.fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();
        let event = self.dispatch_queued(effect).await;
        record_stage("storage", started.elapsed());
        event
    }

    async fn dispatch_queued(&self, effect: StorageEffect) -> StorageEvent {
        let (sender, response_rx) = oneshot::channel();
        let operation = storage_effect_kind(&effect);
        let active_txn_id = effect_txn_id(&effect);
        let cleanup = cleanup_effect(&effect);
        let cleanup_write = is_cleanup_write(&effect);
        if let StorageEffect::CommitTransaction { txn_id } = &effect
            && self
                .transaction_cleanup
                .lock()
                .expect("transaction cleanup mutex poisoned")
                .get(txn_id)
                .is_some_and(|entry| matches!(entry.kind, CleanupKind::Committed))
        {
            return self
                .observe_storage_event(StorageEvent::TransactionCommitted { txn_id: *txn_id });
        }
        let mut deferred = None;
        let send_result: Result<(), StorageError> = {
            // Guard held across the close check and the queue send (never an
            // await), so a concurrent `close_writes` cannot slip between them.
            let _close_guard = storage_effect_mutates(&effect).then(|| {
                self.metrics
                    .close_lock
                    .read()
                    .expect("storage close lock poisoned")
            });
            if _close_guard.is_some() && self.writes_closed() {
                self.metrics.rejected_writes.fetch_add(1, Ordering::Relaxed);
                warn!(
                    event = "storage.write.after_close",
                    operation, "Rejected a storage write issued after the shutdown close"
                );
                return self.observe_storage_event(StorageEvent::Error {
                    error: StorageError::Closed,
                });
            }
            if let Some((txn_id, kind)) = cleanup {
                let mut pending = self
                    .transaction_cleanup
                    .lock()
                    .expect("transaction cleanup mutex poisoned");
                let admission = match reserve_cleanup(&mut pending, txn_id, kind) {
                    Ok(admission) => admission,
                    Err(error) => {
                        return self.observe_storage_event(StorageEvent::Error { error });
                    }
                };
                let response_tx = ResponseSender::new(sender, ResponseToken::new(self, &effect));
                let span = storage_effect_span(&effect);
                let in_flight = InFlightGuard::acquire(&self.metrics);
                match self.channel_for(&effect).try_send((
                    effect,
                    response_tx,
                    span,
                    Instant::now(),
                    in_flight,
                )) {
                    Ok(()) => {
                        if let Some(entry) = pending.get_mut(&txn_id) {
                            entry.queued = true;
                        }
                        Ok(())
                    }
                    Err(TrySendError::Full(item)) => {
                        rollback_cleanup(&mut pending, admission);
                        // The item's ResponseToken re-locks the cleanup mutex on drop.
                        drop(pending);
                        drop(item);
                        Err(StorageError::QueueFull)
                    }
                    Err(TrySendError::Disconnected(item)) => {
                        rollback_cleanup(&mut pending, admission);
                        drop(pending);
                        drop(item);
                        Err(StorageError::ChannelClosed)
                    }
                }
            } else {
                let response_tx = ResponseSender::new(sender, ResponseToken::new(self, &effect));
                let span = storage_effect_span(&effect);
                let in_flight = InFlightGuard::acquire(&self.metrics);
                let item = (effect, response_tx, span, Instant::now(), in_flight);
                match self.channel_for(&item.0).try_send(item) {
                    Ok(()) => Ok(()),
                    Err(TrySendError::Full(item)) if cleanup_write => {
                        // Release the close guard before waiting; the worker fence rejects a late write.
                        deferred = Some(item);
                        Ok(())
                    }
                    Err(TrySendError::Full(_)) => Err(StorageError::QueueFull),
                    Err(TrySendError::Disconnected(_)) => Err(StorageError::ChannelClosed),
                }
            }
        };
        let send_result = match deferred {
            Some(item) => {
                let channel = self.async_channel_for(&item.0).clone();
                match channel.send(item).await {
                    Ok(()) => Ok(()),
                    Err(_) => Err(StorageError::ChannelClosed),
                }
            }
            None => send_result,
        };
        match send_result {
            Ok(()) => {}
            Err(error) => {
                return self.observe_storage_event(StorageEvent::Error { error });
            }
        }

        match tokio::time::timeout(STORAGE_REQUEST_TIMEOUT, response_rx).await {
            Ok(Ok((event, response_token))) => {
                response_token.claim();
                self.observe_cleanup(cleanup, &event);
                self.observe_storage_event(event)
            }
            Ok(Err(_)) => {
                let event = StorageEvent::Error {
                    error: StorageError::ChannelClosed,
                };
                self.observe_cleanup(cleanup, &event);
                self.observe_storage_event(event)
            }
            Err(error) => {
                if let Some(txn_id) = active_txn_id
                    && !matches!(
                        cleanup,
                        Some((_, CleanupKind::CommitQueued | CleanupKind::CommitUnknown))
                    )
                {
                    self.enqueue_abort_transaction(txn_id, "request_timeout");
                }
                warn!(
                    event = "storage.request.timeout",
                    operation,
                    timeout_ms = STORAGE_REQUEST_TIMEOUT.as_millis() as u64,
                    error = %error,
                    "Timed out waiting for storage response"
                );
                let event = StorageEvent::Error {
                    error: StorageError::Timeout,
                };
                self.observe_cleanup(cleanup, &event);
                self.observe_storage_event(event)
            }
        }
    }

    pub(super) fn enqueue_abort_transaction(&self, txn_id: Ulid, reason: &'static str) {
        let mut pending = self
            .transaction_cleanup
            .lock()
            .expect("transaction cleanup mutex poisoned");
        let admission = match reserve_cleanup(&mut pending, txn_id, CleanupKind::Abort) {
            Ok(admission) => admission,
            Err(_) => {
                warn!(%txn_id, reason, "Skipping abort after an accepted commit or full cleanup");
                return;
            }
        };
        let enqueue = !matches!(admission.previous, Some(CleanupKind::Abort));
        drop(pending);
        if enqueue {
            self.send_abort(txn_id, reason);
        }
    }

    fn send_abort(&self, txn_id: Ulid, reason: &'static str) {
        let effect = StorageEffect::AbortTransaction { txn_id };
        let (response_tx, _response_rx) = response_channel(ResponseToken::abort(self, txn_id));
        let span = storage_effect_span(&effect);
        let in_flight = InFlightGuard::acquire(&self.metrics);
        self.mark_queued(txn_id, true);
        match self.channel_for(&effect).try_send((
            effect,
            response_tx,
            span,
            Instant::now(),
            in_flight,
        )) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => self.requeue_warn(txn_id, reason),
            Err(TrySendError::Disconnected(_)) => {
                self.transaction_cleanup
                    .lock()
                    .expect("transaction cleanup mutex poisoned")
                    .remove(&txn_id);
                warn!(
                    event = "storage.transaction.abort_enqueue_closed",
                    txn_id = %txn_id,
                    reason,
                    "Failed to enqueue storage transaction abort: channel closed"
                );
            }
        }
    }

    fn mark_queued(&self, txn_id: Ulid, queued: bool) {
        if let Some(entry) = self
            .transaction_cleanup
            .lock()
            .expect("transaction cleanup mutex poisoned")
            .get_mut(&txn_id)
        {
            entry.queued = queued;
        }
    }

    fn requeue_warn(&self, txn_id: Ulid, reason: &'static str) {
        self.mark_queued(txn_id, false);
        warn!(
            event = "storage.transaction.abort_enqueue_full",
            txn_id = %txn_id,
            reason,
            "Failed to enqueue storage transaction abort: queue full"
        );
    }

    fn retry_abort(&self, txn_id: Ulid, reason: &'static str) {
        let retry = self
            .transaction_cleanup
            .lock()
            .expect("transaction cleanup mutex poisoned")
            .get(&txn_id)
            .is_some_and(|entry| matches!(entry.kind, CleanupKind::Abort));
        if retry {
            self.send_abort(txn_id, reason);
        }
    }

    fn observe_cleanup(&self, cleanup: Option<(Ulid, CleanupKind)>, event: &StorageEvent) {
        if let Some((txn_id, _)) = cleanup
            && observe_cleanup(&self.transaction_cleanup, cleanup, event)
        {
            finish_cleanup(&self.transaction_cleanup, txn_id);
        }
    }

    #[tracing::instrument(
        name = "storage.handle.observe_event",
        level = "trace",
        skip(self, event),
        fields(event = storage_event_kind(&event))
    )]
    fn observe_storage_event(&self, event: StorageEvent) -> StorageEvent {
        if let StorageEvent::Error { error } = &event {
            self.observe_storage_error(error);
        }

        event
    }

    #[tracing::instrument(name = "storage.handle.observe_error", level = "debug", skip(self), fields(error = %error))]
    fn observe_storage_error(&self, error: &StorageError) {
        self.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
        *self
            .metrics
            .last_error
            .lock()
            .expect("storage metrics mutex poisoned") = Some(error.to_string());

        if matches!(error, StorageError::TransactionConflict) {
            self.metrics.conflicts_total.fetch_add(1, Ordering::Relaxed);
        } else {
            self.metrics.failed_total.fetch_add(1, Ordering::Relaxed);
        }

        if matches!(error, StorageError::ChannelClosed) {
            self.metrics.channel_closed.store(true, Ordering::Relaxed);
        }
    }
}
/// Effects that can commit durable state. Reads, iterations, transaction aborts
/// and `SyncAll` stay open after the close.
pub(super) fn storage_effect_mutates(effect: &StorageEffect) -> bool {
    match effect {
        StorageEffect::Write { .. }
        | StorageEffect::BatchWrite { .. }
        | StorageEffect::Delete { .. }
        | StorageEffect::BatchDelete { .. }
        | StorageEffect::CommitTransaction { .. } => true,
        StorageEffect::StartTransaction { read } => !read,
        StorageEffect::Read { .. }
        | StorageEffect::BatchRead { .. }
        | StorageEffect::Iter { .. }
        | StorageEffect::Last { .. }
        | StorageEffect::AbortTransaction { .. }
        | StorageEffect::SyncAll => false,
    }
}

pub(super) fn effect_txn_id(effect: &StorageEffect) -> Option<Ulid> {
    match effect {
        StorageEffect::Read {
            txn_id: Some(txn_id),
            ..
        }
        | StorageEffect::BatchRead {
            txn_id: Some(txn_id),
            ..
        }
        | StorageEffect::Write {
            txn_id: Some(txn_id),
            ..
        }
        | StorageEffect::BatchWrite {
            txn_id: Some(txn_id),
            ..
        }
        | StorageEffect::Delete {
            txn_id: Some(txn_id),
            ..
        }
        | StorageEffect::BatchDelete {
            txn_id: Some(txn_id),
            ..
        }
        | StorageEffect::Iter {
            txn_id: Some(txn_id),
            ..
        }
        | StorageEffect::Last {
            txn_id: Some(txn_id),
            ..
        }
        | StorageEffect::CommitTransaction { txn_id }
        | StorageEffect::AbortTransaction { txn_id } => Some(*txn_id),
        StorageEffect::StartTransaction { .. }
        | StorageEffect::SyncAll
        | StorageEffect::Read { txn_id: None, .. }
        | StorageEffect::BatchRead { txn_id: None, .. }
        | StorageEffect::Write { txn_id: None, .. }
        | StorageEffect::BatchWrite { txn_id: None, .. }
        | StorageEffect::Delete { txn_id: None, .. }
        | StorageEffect::BatchDelete { txn_id: None, .. }
        | StorageEffect::Iter { txn_id: None, .. }
        | StorageEffect::Last { txn_id: None, .. } => None,
    }
}

pub(super) fn cleanup_effect(effect: &StorageEffect) -> Option<(Ulid, CleanupKind)> {
    match effect {
        StorageEffect::AbortTransaction { txn_id } => Some((*txn_id, CleanupKind::Abort)),
        StorageEffect::CommitTransaction { txn_id } => Some((*txn_id, CleanupKind::CommitQueued)),
        _ => None,
    }
}

pub(super) fn is_cleanup_write(effect: &StorageEffect) -> bool {
    matches!(
        effect,
        StorageEffect::Write { key_space, .. } if aruna_core::keyspaces::is_cleanup_keyspace(key_space)
    )
}

pub(super) fn reserve_cleanup(
    pending: &mut BTreeMap<Ulid, CleanupEntry>,
    txn_id: Ulid,
    kind: CleanupKind,
) -> Result<CleanupAdmission, StorageError> {
    let previous = pending.get(&txn_id).map(|entry| entry.kind);
    match (kind, previous) {
        (
            CleanupKind::Abort,
            Some(CleanupKind::CommitQueued | CleanupKind::CommitUnknown | CleanupKind::Committed),
        )
        | (CleanupKind::CommitQueued, Some(CleanupKind::Abort | CleanupKind::Aborted))
        | (CleanupKind::CommitUnknown, Some(CleanupKind::Abort | CleanupKind::Aborted)) => {
            return Err(StorageError::TransactionConflict);
        }
        (
            CleanupKind::CommitQueued,
            Some(CleanupKind::CommitQueued | CleanupKind::CommitUnknown),
        ) => {
            return Err(StorageError::CommitFailed);
        }
        _ => {}
    }

    if previous.is_none() {
        if pending.len() >= MAX_TRANSACTION_CLEANUP {
            return Err(StorageError::CleanupCapacity);
        }
        pending.insert(
            txn_id,
            CleanupEntry {
                kind,
                attempts: 0,
                queued: false,
            },
        );
    } else if matches!(
        (kind, previous),
        (CleanupKind::Abort, Some(CleanupKind::Open))
            | (CleanupKind::CommitQueued, Some(CleanupKind::Open))
            | (CleanupKind::CommitUnknown, Some(CleanupKind::CommitQueued))
    ) && let Some(entry) = pending.get_mut(&txn_id)
    {
        entry.kind = kind;
        entry.attempts = 0;
    }

    Ok(CleanupAdmission {
        txn_id,
        requested: kind,
        previous,
    })
}

fn rollback_cleanup(pending: &mut BTreeMap<Ulid, CleanupEntry>, admission: CleanupAdmission) {
    if !matches!(admission.requested, CleanupKind::CommitQueued) {
        return;
    }
    let Some(entry) = pending.get(&admission.txn_id) else {
        return;
    };
    if !matches!(entry.kind, CleanupKind::CommitQueued) {
        return;
    }
    match admission.previous {
        Some(previous) => {
            if let Some(entry) = pending.get_mut(&admission.txn_id) {
                entry.kind = previous;
                entry.attempts = 0;
            }
        }
        None => {
            pending.remove(&admission.txn_id);
        }
    }
}

pub(super) fn observe_cleanup(
    pending: &Arc<Mutex<BTreeMap<Ulid, CleanupEntry>>>,
    cleanup: Option<(Ulid, CleanupKind)>,
    event: &StorageEvent,
) -> bool {
    let Some((txn_id, kind)) = cleanup else {
        return false;
    };
    let mut pending = pending.lock().expect("transaction cleanup mutex poisoned");
    if let Some(entry) = pending.get_mut(&txn_id) {
        entry.queued = false;
    }
    if pending
        .get(&txn_id)
        .is_some_and(|entry| matches!(entry.kind, CleanupKind::Committed | CleanupKind::Aborted))
    {
        return true;
    }
    let terminal_kind = match (kind, event) {
        (CleanupKind::Abort, StorageEvent::TransactionAborted { txn_id: aborted })
            if txn_id == *aborted =>
        {
            Some(CleanupKind::Aborted)
        }
        (
            CleanupKind::Abort,
            StorageEvent::Error {
                error: StorageError::TransactionNotFound,
            },
        ) => Some(CleanupKind::Aborted),
        (
            CleanupKind::CommitQueued | CleanupKind::CommitUnknown,
            StorageEvent::TransactionCommitted { txn_id: committed },
        ) if txn_id == *committed => Some(CleanupKind::Committed),
        (
            CleanupKind::CommitQueued | CleanupKind::CommitUnknown,
            StorageEvent::Error {
                error: StorageError::TransactionConflict | StorageError::TransactionNotFound,
            },
        ) => Some(CleanupKind::Aborted),
        _ => None,
    };
    if let Some(terminal_kind) = terminal_kind {
        if let Some(entry) = pending.get_mut(&txn_id) {
            entry.kind = terminal_kind;
            entry.attempts = 0;
        }
        return true;
    } else if matches!(
        (kind, event),
        (
            CleanupKind::CommitQueued | CleanupKind::CommitUnknown,
            StorageEvent::Error {
                error: StorageError::CommitFailed
            }
        )
    ) {
        if let Some(entry) = pending.get_mut(&txn_id)
            && matches!(
                entry.kind,
                CleanupKind::CommitQueued | CleanupKind::CommitUnknown
            )
        {
            // An unknown commit cannot progress after the worker forgets its transaction.
            // Count retries so the cleanup slot is eventually freed.
            if matches!(entry.kind, CleanupKind::CommitUnknown) {
                entry.attempts = entry.attempts.saturating_add(1);
                return entry.attempts >= MAX_CLEANUP_ATTEMPTS;
            }
            entry.kind = CleanupKind::CommitUnknown;
            entry.attempts = 0;
        }
    } else if matches!(
        (kind, event),
        (
            CleanupKind::CommitQueued,
            StorageEvent::Error {
                error: StorageError::Timeout | StorageError::ChannelClosed
            }
        )
    ) {
        if let Some(entry) = pending.get_mut(&txn_id)
            && matches!(entry.kind, CleanupKind::CommitQueued)
        {
            entry.kind = CleanupKind::CommitUnknown;
            entry.attempts = 0;
        }
    } else if let Some(entry) = pending.get_mut(&txn_id)
        && matches!(entry.kind, CleanupKind::Abort | CleanupKind::CommitUnknown)
    {
        entry.attempts = entry.attempts.saturating_add(1);
    }
    false
}

pub(super) fn finish_cleanup(pending: &Arc<Mutex<BTreeMap<Ulid, CleanupEntry>>>, txn_id: Ulid) {
    pending
        .lock()
        .expect("transaction cleanup mutex poisoned")
        .remove(&txn_id);
}

impl ResponseToken {
    #[cfg(test)]
    pub(super) fn empty() -> Self {
        Self {
            handle: None,
            cleanup: None,
        }
    }

    pub(super) fn new(handle: &StorageHandle, effect: &StorageEffect) -> Self {
        let cleanup = match effect {
            StorageEffect::StartTransaction { .. } => Some(ResponseCleanup::Start(None)),
            StorageEffect::CommitTransaction { txn_id } => Some(ResponseCleanup::Commit(*txn_id)),
            StorageEffect::AbortTransaction { txn_id } => {
                Some(ResponseCleanup::Abort(*txn_id, true))
            }
            _ => effect_txn_id(effect).map(|txn_id| ResponseCleanup::Abort(txn_id, false)),
        };
        Self {
            handle: cleanup.as_ref().map(|_| handle.clone()),
            cleanup,
        }
    }

    fn abort(handle: &StorageHandle, txn_id: Ulid) -> Self {
        Self {
            handle: Some(handle.clone()),
            cleanup: Some(ResponseCleanup::Abort(txn_id, false)),
        }
    }

    fn observe(&mut self, event: &StorageEvent) {
        let Some(cleanup) = self.cleanup.take() else {
            return;
        };
        let cleanup = match cleanup {
            ResponseCleanup::Start(None) => match event {
                StorageEvent::TransactionStarted { txn_id } => {
                    ResponseCleanup::Start(Some(*txn_id))
                }
                _ => ResponseCleanup::Start(None),
            },
            ResponseCleanup::Abort(txn_id, force) => {
                if matches!(
                    event,
                    StorageEvent::TransactionAborted { txn_id: aborted }
                        if txn_id == *aborted
                ) || matches!(
                    event,
                    StorageEvent::Error {
                        error: StorageError::TransactionNotFound
                    }
                ) {
                    ResponseCleanup::Terminal(txn_id)
                } else {
                    ResponseCleanup::Abort(txn_id, force)
                }
            }
            ResponseCleanup::Commit(txn_id) => {
                if matches!(
                    event,
                    StorageEvent::TransactionCommitted { txn_id: committed }
                        if txn_id == *committed
                ) || matches!(
                    event,
                    StorageEvent::Error {
                        error: StorageError::TransactionConflict
                            | StorageError::TransactionNotFound
                    }
                ) {
                    ResponseCleanup::Terminal(txn_id)
                } else {
                    ResponseCleanup::Commit(txn_id)
                }
            }
            cleanup => cleanup,
        };
        self.cleanup = Some(cleanup);
    }

    fn claim(mut self) {
        self.disarm();
    }

    fn disarm(&mut self) {
        let cleanup = self.cleanup.take();
        let handle = self.handle.take();
        if let (Some(ResponseCleanup::Terminal(txn_id)), Some(handle)) = (cleanup, handle) {
            finish_cleanup(&handle.transaction_cleanup, txn_id);
        }
    }
}

impl Drop for ResponseToken {
    fn drop(&mut self) {
        let cleanup = self.cleanup.take();
        let Some(cleanup) = cleanup else {
            self.handle = None;
            return;
        };
        let Some(handle) = self.handle.take() else {
            return;
        };
        match cleanup {
            ResponseCleanup::Start(Some(txn_id)) => {
                let _ = handle.retain_transaction(txn_id, false);
            }
            ResponseCleanup::Abort(txn_id, force) => {
                let retry = force
                    && handle
                        .transaction_cleanup
                        .lock()
                        .expect("transaction cleanup mutex poisoned")
                        .get(&txn_id)
                        .is_some_and(|entry| matches!(entry.kind, CleanupKind::Abort));
                let retained = handle.retain_transaction(txn_id, false);
                if retry && retained {
                    handle.retry_abort(txn_id, "response_abandoned");
                }
            }
            ResponseCleanup::Commit(txn_id) => {
                if !handle.retain_transaction(txn_id, true) {
                    let _ = handle.retain_transaction(txn_id, false);
                }
            }
            ResponseCleanup::Terminal(txn_id) => {
                finish_cleanup(&handle.transaction_cleanup, txn_id);
            }
            ResponseCleanup::Start(None) => {}
        }
    }
}

pub(super) fn response_channel(
    token: ResponseToken,
) -> (ResponseSender, oneshot::Receiver<StorageReply>) {
    let (sender, receiver) = oneshot::channel();
    (ResponseSender::new(sender, token), receiver)
}
#[async_trait]
impl Handle for StorageHandle {
    #[tracing::instrument(
        name = "storage.handle.send_effect",
        level = "debug",
        skip(self, effect),
        fields(effect = effect_kind(&effect))
    )]
    async fn send_effect(&self, effect: Effect) -> Event {
        match effect {
            Effect::Storage(storage_effect) => {
                Event::Storage(self.dispatch_storage_effect(storage_effect).await)
            }
            _ => {
                self.metrics.requests_total.fetch_add(1, Ordering::Relaxed);
                let error = StorageError::InvalidEffect;
                self.observe_storage_error(&error);
                Event::Storage(StorageEvent::Error { error })
            }
        }
    }
}
