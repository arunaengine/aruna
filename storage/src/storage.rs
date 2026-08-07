use std::collections::{BTreeMap, HashMap};
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use aruna_core::effects::{Effect, IterStart, StorageEffect, StoragePriority};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::telemetry::{LatencyAggregator, duration_ms, record_stage};
use aruna_core::util::prefix_upper_bound;
use async_trait::async_trait;
use byteview::ByteView;
use crossfire::select::Select;
use crossfire::{RecvError, TryRecvError, TrySendError, mpsc, oneshot};
use fjall::{
    KeyspaceCreateOptions, OptimisticTxDatabase, OptimisticTxKeyspace, PersistMode, Readable,
};
use tracing::{Span, debug_span, field, warn};
use ulid::Ulid;

use crate::errors::StorageLibError;
pub type EffectHandle = (
    StorageEffect,
    oneshot::TxOneshot<StorageEvent>,
    Span,
    Instant,
    InFlightGuard,
);
pub type EffectSender = crossfire::MTx<mpsc::Array<EffectHandle>>;
pub type EffectReceiver = crossfire::Rx<mpsc::Array<EffectHandle>>;

const STORAGE_EFFECT_QUEUE_CAPACITY: usize = 65_536;
// Bulk lane queues are small so background work hits QueueFull backpressure early
// instead of building an unbounded backlog ahead of foreground sync traffic.
const BULK_EFFECT_QUEUE_CAPACITY: usize = 4_096;
const MAX_TRANSACTION_CLEANUP: usize = 1024;
const MAX_CLEANUP_ATTEMPTS: u8 = 2;

enum Txn {
    Read(fjall::Snapshot),
    Write(Box<fjall::OptimisticWriteTx>),
}

#[derive(Debug, Clone, Copy)]
enum CleanupKind {
    Abort,
    CommitUnknown,
}

#[derive(Debug, Clone, Copy)]
struct CleanupEntry {
    kind: CleanupKind,
    attempts: u8,
}

type PageResult = (Vec<(ByteView, ByteView)>, Option<ByteView>);
const STORAGE_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const SLOW_STORAGE_EFFECT_THRESHOLD: Duration = Duration::from_millis(50);
const SLOW_QUEUE_LOG_INTERVAL: Duration = Duration::from_secs(1);

// Unbiased queue-wait vs service histograms for every storage effect, keyed
// by operation kind and keyspace, flushed as `latency.summary` INFO lines.
static STORAGE_LATENCY: LazyLock<LatencyAggregator> =
    LazyLock::new(|| LatencyAggregator::new("storage"));

fn record_storage_call(
    operation: &'static str,
    key_space: Option<&str>,
    queue_wait: Duration,
    service: Duration,
) {
    match key_space {
        Some(key_space) => {
            STORAGE_LATENCY.record_split(&format!("{operation}:{key_space}"), queue_wait, service)
        }
        None => STORAGE_LATENCY.record_split(operation, queue_wait, service),
    }
}

fn storage_effect_key_space(effect: &StorageEffect) -> Option<&str> {
    match effect {
        StorageEffect::Read { key_space, .. }
        | StorageEffect::Write { key_space, .. }
        | StorageEffect::Delete { key_space, .. }
        | StorageEffect::Iter { key_space, .. } => Some(key_space),
        StorageEffect::BatchRead { reads, .. } => {
            reads.first().map(|(key_space, _)| key_space.as_str())
        }
        StorageEffect::BatchWrite { writes, .. } => {
            writes.first().map(|(key_space, _, _)| key_space.as_str())
        }
        StorageEffect::BatchDelete { deletes, .. } => {
            deletes.first().map(|(key_space, _)| key_space.as_str())
        }
        StorageEffect::StartTransaction { .. }
        | StorageEffect::CommitTransaction { .. }
        | StorageEffect::AbortTransaction { .. }
        | StorageEffect::SyncAll => None,
    }
}
const MAX_GROUP_COMMIT: usize = 256;
const READ_POOL_THREADS: usize = 4;
const BULK_READ_POOL_THREADS: usize = 2;
// Foreground effects served per admitted bulk effect. Counting effects rather
// than batches keeps the bulk share constant under load: a batch may carry up to
// MAX_GROUP_COMMIT effects, so per-batch credit would shrink the share to
// nothing exactly when the queue is deepest.
const FOREGROUND_PER_BULK: usize = 8;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum FjallPersistPolicy {
    #[default]
    Buffer,
    SyncAll,
}

impl FjallPersistPolicy {
    pub fn as_fjall(self) -> PersistMode {
        match self {
            Self::Buffer => PersistMode::Buffer,
            Self::SyncAll => PersistMode::SyncAll,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Buffer => "buffer",
            Self::SyncAll => "sync_all",
        }
    }
}

impl std::str::FromStr for FjallPersistPolicy {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "buffer" => Ok(Self::Buffer),
            "sync_all" => Ok(Self::SyncAll),
            other => Err(format!("unsupported fjall persist policy `{other}`")),
        }
    }
}

#[derive(Clone)]
struct Store {
    db: OptimisticTxDatabase,
    keyspaces: Arc<Mutex<HashMap<String, OptimisticTxKeyspace>>>,
}

impl Store {
    fn new(db: OptimisticTxDatabase) -> Self {
        Self {
            db,
            keyspaces: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn resolve_keyspace(&self, name: &str) -> Result<OptimisticTxKeyspace, StorageError> {
        if let Some(ks) = self
            .keyspaces
            .lock()
            .expect("storage keyspace cache mutex poisoned")
            .get(name)
        {
            return Ok(ks.clone());
        }

        match self.db.keyspace(name, KeyspaceCreateOptions::default) {
            Ok(ks) => {
                let mut keyspaces = self
                    .keyspaces
                    .lock()
                    .expect("storage keyspace cache mutex poisoned");
                Ok(keyspaces
                    .entry(name.to_string())
                    .or_insert_with(|| ks.clone())
                    .clone())
            }
            Err(_) => Err(StorageError::KeyspaceError),
        }
    }
}

pub struct FjallStorage {
    store: Store,
    persist_policy: FjallPersistPolicy,
    txns: HashMap<Ulid, Txn>,
    transaction_cleanup: Arc<Mutex<BTreeMap<Ulid, CleanupEntry>>>,
    read_pool: Vec<EffectSender>,
    next_reader: usize,
    bulk_read_pool: Vec<EffectSender>,
    next_bulk_reader: usize,
}

#[derive(Debug, Default)]
struct StorageMetrics {
    requests_total: AtomicU64,
    errors_total: AtomicU64,
    conflicts_total: AtomicU64,
    in_flight: AtomicU64,
    channel_closed: Arc<AtomicBool>,
    last_error: Mutex<Option<String>>,
}

/// Decrements `in_flight` when an accepted effect completes or is discarded.
#[doc(hidden)]
pub struct InFlightGuard(Arc<StorageMetrics>);

impl InFlightGuard {
    fn acquire(metrics: &Arc<StorageMetrics>) -> Self {
        metrics.in_flight.fetch_add(1, Ordering::Relaxed);
        Self(metrics.clone())
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.0.in_flight.fetch_sub(1, Ordering::Relaxed);
    }
}

struct WorkerLifecycleGuard(Arc<AtomicBool>);

impl Drop for WorkerLifecycleGuard {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StorageMetricsSnapshot {
    pub requests_total: u64,
    pub errors_total: u64,
    pub conflicts_total: u64,
    pub failed_total: u64,
    pub channel_closed: bool,
    pub last_error: Option<String>,
}

/// Write-actor receivers, one per dispatch lane. Foreground is drained before
/// bulk so background work never starves sync traffic.
pub struct StorageReceivers {
    pub foreground: EffectReceiver,
    pub bulk: EffectReceiver,
}

#[derive(Clone, Debug)]
pub struct StorageHandle {
    write_channel: EffectSender,
    bulk_channel: EffectSender,
    priority: StoragePriority,
    metrics: Arc<StorageMetrics>,
    transaction_cleanup: Arc<Mutex<BTreeMap<Ulid, CleanupEntry>>>,
}

/// Cancellation-safe owner for a manually driven storage transaction.
pub struct TransactionOwner {
    handle: StorageHandle,
    txn_id: Option<Ulid>,
}

impl TransactionOwner {
    pub fn new(handle: StorageHandle, txn_id: Ulid) -> Self {
        Self {
            handle,
            txn_id: Some(txn_id),
        }
    }

    pub fn id(&self) -> Option<Ulid> {
        self.txn_id
    }

    /// Releases ownership after storage proved the transaction terminal.
    pub fn finish(&mut self) {
        self.txn_id = None;
    }

    /// Retains an ambiguous commit without issuing an abort.
    pub fn unknown(&mut self) {
        if let Some(txn_id) = self.txn_id.take() {
            if !self.handle.retain_transaction(txn_id, true) {
                warn!(%txn_id, "Transaction cleanup handoff capacity reached");
            }
        }
    }
}

impl Drop for TransactionOwner {
    fn drop(&mut self) {
        if let Some(txn_id) = self.txn_id.take() {
            if !self.handle.retain_transaction(txn_id, false) {
                warn!(%txn_id, "Transaction cleanup handoff capacity reached");
            }
        }
    }
}

impl StorageHandle {
    pub fn new() -> (Self, StorageReceivers) {
        let (sender, foreground) = mpsc::bounded_blocking(STORAGE_EFFECT_QUEUE_CAPACITY);
        let (bulk_sender, bulk) = mpsc::bounded_blocking(BULK_EFFECT_QUEUE_CAPACITY);
        (
            StorageHandle {
                write_channel: sender,
                bulk_channel: bulk_sender,
                priority: StoragePriority::Foreground,
                metrics: Arc::new(StorageMetrics::default()),
                transaction_cleanup: Arc::new(Mutex::new(BTreeMap::new())),
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

    fn channel_for(&self, effect: &StorageEffect) -> &EffectSender {
        // Aborts free resources and must never wait behind bulk backpressure, so
        // they always dispatch on the foreground lane regardless of priority.
        if matches!(effect, StorageEffect::AbortTransaction { .. }) {
            return &self.write_channel;
        }
        match self.priority {
            StoragePriority::Foreground => &self.write_channel,
            StoragePriority::Bulk => &self.bulk_channel,
        }
    }

    pub fn get_errors(&self) -> u64 {
        self.metrics.errors_total.load(Ordering::Relaxed)
    }

    /// Number of storage effects currently enqueued or being processed.
    pub fn in_flight(&self) -> u64 {
        self.metrics.in_flight.load(Ordering::Relaxed)
    }

    /// True once the effect channel has permanently closed because the worker
    /// died. Latched and unrecoverable in-process; a cheap, lock-free read.
    pub fn channel_closed(&self) -> bool {
        self.metrics.channel_closed.load(Ordering::Relaxed)
    }

    pub fn snapshot_metrics(&self) -> StorageMetricsSnapshot {
        let errors_total = self.metrics.errors_total.load(Ordering::Relaxed);
        StorageMetricsSnapshot {
            requests_total: self.metrics.requests_total.load(Ordering::Relaxed),
            errors_total,
            conflicts_total: self.metrics.conflicts_total.load(Ordering::Relaxed),
            failed_total: errors_total,
            channel_closed: self.metrics.channel_closed.load(Ordering::Relaxed),
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
        if !self.retain_cleanup(txn_id, kind) {
            return false;
        }
        if !commit_unknown {
            self.enqueue_abort_transaction(txn_id, "owner_handoff");
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

    fn retain_cleanup(&self, txn_id: Ulid, kind: CleanupKind) -> bool {
        retain_cleanup(&self.transaction_cleanup, txn_id, kind)
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

    pub async fn sync_all(&self) -> Result<(), StorageError> {
        match self.dispatch_storage_effect(StorageEffect::SyncAll).await {
            StorageEvent::SyncAllFinished => Ok(()),
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
    async fn dispatch_storage_effect(&self, effect: StorageEffect) -> StorageEvent {
        self.metrics.requests_total.fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();
        let event = self.dispatch_queued_storage_effect(effect).await;
        record_stage("storage", started.elapsed());
        event
    }

    async fn dispatch_queued_storage_effect(&self, effect: StorageEffect) -> StorageEvent {
        let (response_tx, response_rx) = crossfire::oneshot::oneshot();
        let operation = storage_effect_kind(&effect);
        let active_txn_id = active_txn_id_for_effect(&effect);
        let cleanup = cleanup_effect(&effect);
        if let Some((txn_id, kind)) = cleanup {
            self.retain_cleanup(txn_id, kind);
        }
        let span = storage_effect_span(&effect);
        let in_flight = InFlightGuard::acquire(&self.metrics);
        match self.channel_for(&effect).try_send((
            effect,
            response_tx,
            span,
            Instant::now(),
            in_flight,
        )) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                if let Some(txn_id) = active_txn_id {
                    if !matches!(cleanup, Some((_, CleanupKind::Abort))) {
                        self.enqueue_abort_transaction(txn_id, "request_queue_full");
                    }
                }
                let event = StorageEvent::Error {
                    error: StorageError::QueueFull,
                };
                self.observe_cleanup(cleanup, &event);
                return self.observe_storage_event(event);
            }
            Err(TrySendError::Disconnected(_)) => {
                let event = StorageEvent::Error {
                    error: StorageError::ChannelClosed,
                };
                self.observe_cleanup(cleanup, &event);
                return self.observe_storage_event(event);
            }
        }

        match tokio::time::timeout(STORAGE_REQUEST_TIMEOUT, response_rx).await {
            Ok(Ok(event)) => {
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
                if let Some(txn_id) = active_txn_id {
                    if !matches!(cleanup, Some((_, CleanupKind::CommitUnknown))) {
                        self.enqueue_abort_transaction(txn_id, "request_timeout");
                    }
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

    fn enqueue_abort_transaction(&self, txn_id: Ulid, reason: &'static str) {
        if !self.retain_cleanup(txn_id, CleanupKind::Abort) {
            warn!(%txn_id, reason, "Transaction cleanup capacity reached");
            return;
        }
        let (response_tx, _response_rx) = crossfire::oneshot::oneshot();
        let effect = StorageEffect::AbortTransaction { txn_id };
        let span = storage_effect_span(&effect);
        let in_flight = InFlightGuard::acquire(&self.metrics);
        match self.channel_for(&effect).try_send((
            effect,
            response_tx,
            span,
            Instant::now(),
            in_flight,
        )) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => warn!(
                event = "storage.transaction.abort_enqueue_full",
                txn_id = %txn_id,
                reason,
                "Failed to enqueue storage transaction abort: queue full"
            ),
            Err(TrySendError::Disconnected(_)) => warn!(
                event = "storage.transaction.abort_enqueue_closed",
                txn_id = %txn_id,
                reason,
                "Failed to enqueue storage transaction abort: channel closed"
            ),
        }
    }

    fn observe_cleanup(&self, cleanup: Option<(Ulid, CleanupKind)>, event: &StorageEvent) {
        observe_cleanup(&self.transaction_cleanup, cleanup, event);
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
        }

        if matches!(error, StorageError::ChannelClosed) {
            self.metrics.channel_closed.store(true, Ordering::Relaxed);
        }
    }
}

fn active_txn_id_for_effect(effect: &StorageEffect) -> Option<Ulid> {
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
        | StorageEffect::CommitTransaction { txn_id } => Some(*txn_id),
        StorageEffect::StartTransaction { .. }
        | StorageEffect::AbortTransaction { .. }
        | StorageEffect::SyncAll
        | StorageEffect::Read { txn_id: None, .. }
        | StorageEffect::BatchRead { txn_id: None, .. }
        | StorageEffect::Write { txn_id: None, .. }
        | StorageEffect::BatchWrite { txn_id: None, .. }
        | StorageEffect::Delete { txn_id: None, .. }
        | StorageEffect::BatchDelete { txn_id: None, .. }
        | StorageEffect::Iter { txn_id: None, .. } => None,
    }
}

fn cleanup_effect(effect: &StorageEffect) -> Option<(Ulid, CleanupKind)> {
    match effect {
        StorageEffect::AbortTransaction { txn_id } => Some((*txn_id, CleanupKind::Abort)),
        StorageEffect::CommitTransaction { txn_id } => Some((*txn_id, CleanupKind::CommitUnknown)),
        _ => None,
    }
}

fn retain_cleanup(
    pending: &Arc<Mutex<BTreeMap<Ulid, CleanupEntry>>>,
    txn_id: Ulid,
    kind: CleanupKind,
) -> bool {
    let mut pending = pending.lock().expect("transaction cleanup mutex poisoned");
    if !pending.contains_key(&txn_id) && pending.len() >= MAX_TRANSACTION_CLEANUP {
        return false;
    }
    let entry = pending
        .entry(txn_id)
        .or_insert(CleanupEntry { kind, attempts: 0 });
    if matches!(kind, CleanupKind::CommitUnknown) {
        entry.kind = kind;
    }
    true
}

fn observe_cleanup(
    pending: &Arc<Mutex<BTreeMap<Ulid, CleanupEntry>>>,
    cleanup: Option<(Ulid, CleanupKind)>,
    event: &StorageEvent,
) {
    let Some((txn_id, kind)) = cleanup else {
        return;
    };
    let terminal = match (kind, event) {
        (CleanupKind::Abort, StorageEvent::TransactionAborted { txn_id: aborted })
            if txn_id == *aborted =>
        {
            true
        }
        (
            CleanupKind::Abort,
            StorageEvent::Error {
                error: StorageError::TransactionNotFound,
            },
        ) => true,
        (CleanupKind::CommitUnknown, StorageEvent::TransactionCommitted { txn_id: committed })
            if txn_id == *committed =>
        {
            true
        }
        (CleanupKind::CommitUnknown, StorageEvent::Error { error })
            if matches!(
                error,
                StorageError::TransactionConflict | StorageError::TransactionNotFound
            ) =>
        {
            true
        }
        _ => false,
    };
    let mut pending = pending.lock().expect("transaction cleanup mutex poisoned");
    if terminal {
        pending.remove(&txn_id);
    } else if let Some(entry) = pending.get_mut(&txn_id)
        && matches!(entry.kind, CleanupKind::Abort)
    {
        entry.attempts = entry.attempts.saturating_add(1);
    }
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

impl FjallStorage {
    #[tracing::instrument(name = "storage.open", level = "debug", fields(path = %path))]
    pub fn open(path: &str) -> Result<StorageHandle, StorageLibError> {
        Self::open_with_persist_policy(path, FjallPersistPolicy::default())
    }

    #[tracing::instrument(
        name = "storage.open",
        level = "debug",
        fields(path = %path, persist_policy = policy.label())
    )]
    pub fn open_with_persist_policy(
        path: &str,
        policy: FjallPersistPolicy,
    ) -> Result<StorageHandle, StorageLibError> {
        let db = OptimisticTxDatabase::builder(path)
            .manual_journal_persist(true)
            .open()?;

        let (sender, receivers) = StorageHandle::new();
        let store = Store::new(db);
        let transaction_cleanup = sender.transaction_cleanup.clone();
        let read_pool = spawn_read_pool(
            store.clone(),
            READ_POOL_THREADS,
            STORAGE_EFFECT_QUEUE_CAPACITY,
        );
        let bulk_read_pool = spawn_read_pool(
            store.clone(),
            BULK_READ_POOL_THREADS,
            BULK_EFFECT_QUEUE_CAPACITY,
        );
        let channel_closed = sender.metrics.channel_closed.clone();

        thread::spawn(move || {
            let _lifecycle = WorkerLifecycleGuard(channel_closed);
            let mut storage = FjallStorage {
                store,
                persist_policy: policy,
                txns: HashMap::new(),
                transaction_cleanup,
                read_pool,
                next_reader: 0,
                bulk_read_pool,
                next_bulk_reader: 0,
            };
            storage.receive_loop(receivers);
        });

        Ok(sender)
    }

    fn process_effect(&mut self, effect: StorageEffect) -> StorageEvent {
        match effect {
            StorageEffect::StartTransaction { read } => self.start_transaction(read),
            StorageEffect::AbortTransaction { txn_id } => self.abort_transaction(txn_id),
            StorageEffect::SyncAll => self.sync_all(),
            StorageEffect::Read {
                key_space,
                key,
                txn_id,
            } => self.read(key_space, key, txn_id),
            StorageEffect::BatchRead { reads, txn_id } => self.batch_read(reads, txn_id),
            StorageEffect::Write {
                key_space,
                key,
                value,
                txn_id,
            } => self.write(key_space, key, value, txn_id),
            StorageEffect::BatchWrite { writes, txn_id } => self.batch_write(writes, txn_id),
            StorageEffect::CommitTransaction { txn_id } => self.commit_transaction(txn_id),
            StorageEffect::Delete {
                key_space,
                key,
                txn_id,
            } => self.delete(key_space, key, txn_id),
            StorageEffect::BatchDelete { deletes, txn_id } => self.batch_delete(deletes, txn_id),
            StorageEffect::Iter {
                key_space,
                prefix,
                start,
                limit,
                txn_id,
            } => self.iterate(key_space, prefix, start, limit, txn_id),
        }
    }

    fn observe_cleanup(&self, cleanup: Option<(Ulid, CleanupKind)>, event: &StorageEvent) {
        observe_cleanup(&self.transaction_cleanup, cleanup, event);
    }

    fn retry_cleanup(&mut self) {
        let retry = self
            .transaction_cleanup
            .lock()
            .expect("transaction cleanup mutex poisoned")
            .iter()
            .filter_map(|(txn_id, entry)| {
                (matches!(entry.kind, CleanupKind::Abort) && entry.attempts < MAX_CLEANUP_ATTEMPTS)
                    .then_some(*txn_id)
            })
            .collect::<Vec<_>>();
        for txn_id in retry {
            let event = self.abort_transaction(txn_id);
            self.observe_cleanup(Some((txn_id, CleanupKind::Abort)), &event);
        }
    }

    #[tracing::instrument(name = "storage.receive_loop", level = "debug", skip(self, receivers))]
    pub fn receive_loop(&mut self, receivers: StorageReceivers) {
        let mut slow_queue = SlowQueueAggregator::default();
        let mut lanes = LaneScheduler::default();
        loop {
            let (first, priority) = match lanes.next(&receivers) {
                Ok(pair) => pair,
                Err(_) => {
                    tracing::warn!(
                        "Storage receiver channel closed, shutting down storage thread."
                    );
                    break;
                }
            };
            match priority {
                StoragePriority::Foreground => {
                    let served = self.serve_foreground_batch(&receivers, first, &mut slow_queue);
                    lanes.record_foreground(served);
                }
                StoragePriority::Bulk => {
                    if is_poolable_read(&first.0) {
                        self.forward_to_read_pool(first, StoragePriority::Bulk, &mut slow_queue);
                    } else {
                        self.process_single(first, &mut slow_queue);
                    }
                }
            }
        }
    }

    /// Returns how many foreground effects were served, which is what the lane
    /// scheduler credits the bulk lane against.
    fn serve_foreground_batch(
        &mut self,
        receivers: &StorageReceivers,
        first: EffectHandle,
        slow_queue: &mut SlowQueueAggregator,
    ) -> usize {
        let mut pending = Vec::with_capacity(8);
        pending.push(first);
        while pending.len() < MAX_GROUP_COMMIT {
            match receivers.foreground.try_recv() {
                Ok(item) => pending.push(item),
                Err(_) => break,
            }
        }
        let served = pending.len();

        let mut group: Vec<EffectHandle> = Vec::new();
        let mut group_index: Option<PendingWriteIndex> = None;
        for item in pending {
            if is_groupable_write(&item.0) {
                if let Some(index) = &mut group_index {
                    index.insert(&item.0);
                }
                group.push(item);
                continue;
            }
            if is_poolable_read(&item.0) {
                let conflicts = !group.is_empty()
                    && group_index
                        .get_or_insert_with(|| PendingWriteIndex::from_group(&group))
                        .conflicts_with_read(&item.0);
                if conflicts {
                    self.flush_write_group(&mut group, slow_queue);
                    group_index = None;
                }
                self.forward_to_read_pool(item, StoragePriority::Foreground, slow_queue);
                continue;
            }
            self.flush_write_group(&mut group, slow_queue);
            group_index = None;
            self.process_single(item, slow_queue);
        }
        self.flush_write_group(&mut group, slow_queue);
        served
    }

    fn forward_to_read_pool(
        &mut self,
        item: EffectHandle,
        priority: StoragePriority,
        slow_queue: &mut SlowQueueAggregator,
    ) {
        match priority {
            StoragePriority::Foreground => {
                let reader = self.next_reader % self.read_pool.len();
                self.next_reader = self.next_reader.wrapping_add(1);
                match self.read_pool[reader].try_send(item) {
                    Ok(()) => {}
                    Err(TrySendError::Full(item)) | Err(TrySendError::Disconnected(item)) => {
                        self.process_single(item, slow_queue);
                    }
                }
            }
            StoragePriority::Bulk => {
                let reader = self.next_bulk_reader % self.bulk_read_pool.len();
                self.next_bulk_reader = self.next_bulk_reader.wrapping_add(1);
                match self.bulk_read_pool[reader].try_send(item) {
                    Ok(()) => {}
                    // A full bulk read queue backpressures rather than stealing
                    // the write actor thread from foreground sync traffic.
                    Err(TrySendError::Full(item)) => reject_bulk_read(item),
                    Err(TrySendError::Disconnected(item)) => self.process_single(item, slow_queue),
                }
            }
        }
    }

    fn process_single(&mut self, item: EffectHandle, slow_queue: &mut SlowQueueAggregator) {
        let (effect, response_tx, span, enqueued_at, in_flight) = item;
        let _guard = span.enter();
        let operation = storage_effect_kind(&effect);
        let key_space = storage_effect_key_space(&effect).map(str::to_string);
        let active_txn_id = active_txn_id_for_effect(&effect);
        let cleanup = cleanup_effect(&effect);
        let queue_wait = enqueued_at.elapsed();
        span.record("queue_wait_ms", duration_ms(queue_wait));
        let starts_transaction = matches!(effect, StorageEffect::StartTransaction { .. });
        let completes_transaction = matches!(
            effect,
            StorageEffect::CommitTransaction { .. } | StorageEffect::AbortTransaction { .. }
        );

        if response_tx.is_disconnected()
            && !matches!(
                effect,
                StorageEffect::AbortTransaction { .. } | StorageEffect::CommitTransaction { .. }
            )
        {
            if let Some(txn_id) = active_txn_id {
                self.cleanup_abandoned_transaction(
                    txn_id,
                    operation,
                    "abandoned_before_processing",
                );
            }
            warn!(
                event = "storage.request.abandoned",
                operation, "Skipping abandoned storage request"
            );
            return;
        }

        let service_started = Instant::now();
        let event = self.process_effect(effect);
        self.observe_cleanup(cleanup, &event);
        self.retry_cleanup();
        let service_elapsed = service_started.elapsed();
        let total_elapsed = enqueued_at.elapsed();
        let result = storage_event_kind(&event);
        span.record("service_ms", duration_ms(service_elapsed));
        span.record("total_elapsed_ms", duration_ms(total_elapsed));
        span.record("result", result);
        slow_queue.observe(
            operation,
            key_space.as_deref(),
            queue_wait,
            service_elapsed,
            result,
        );
        if response_tx.is_disconnected() {
            let abandoned_txn_id = if starts_transaction {
                match &event {
                    StorageEvent::TransactionStarted { txn_id } => Some(*txn_id),
                    _ => None,
                }
            } else if completes_transaction {
                None
            } else {
                active_txn_id
            };

            if let Some(txn_id) = abandoned_txn_id {
                self.cleanup_abandoned_transaction(txn_id, operation, "abandoned_after_processing");
            }
            warn!(
                event = "storage.response.abandoned",
                operation,
                result = storage_event_kind(&event),
                "Dropping storage response for abandoned request"
            );
        } else {
            drop(in_flight);
            response_tx.send(event);
        }
    }

    fn flush_write_group(
        &mut self,
        group: &mut Vec<EffectHandle>,
        slow_queue: &mut SlowQueueAggregator,
    ) {
        if group.is_empty() {
            return;
        }
        if group.len() == 1 {
            let item = group.pop().expect("group has one item");
            self.process_single(item, slow_queue);
            return;
        }

        let mut members = std::mem::take(group);
        members.retain(|item| {
            if item.1.is_disconnected() {
                let _guard = item.2.enter();
                warn!(
                    event = "storage.request.abandoned",
                    operation = storage_effect_kind(&item.0),
                    "Skipping abandoned storage request"
                );
                false
            } else {
                true
            }
        });
        if members.is_empty() {
            return;
        }
        if members.len() == 1 {
            let item = members.pop().expect("group has one item");
            self.process_single(item, slow_queue);
            return;
        }

        let service_started = Instant::now();
        let tx = match self.buffered_write_tx() {
            Ok(tx) => tx,
            Err(_) => {
                for item in members {
                    self.process_single(item, slow_queue);
                }
                return;
            }
        };

        let mut tx = tx;
        let mut prepared = Vec::with_capacity(members.len());
        for item in members {
            match self.apply_group_member(&mut tx, &item.0) {
                Ok(event) => prepared.push((item, Ok(event))),
                Err(error) => prepared.push((item, Err(error))),
            }
        }

        let group_error = match self.commit_buffered_write_tx(tx) {
            Ok(()) => self.persist_journal().err(),
            Err(StorageError::TransactionConflict) => {
                for (item, _) in prepared {
                    self.process_single(item, slow_queue);
                }
                return;
            }
            Err(error) => Some(error),
        };

        let service_elapsed = service_started.elapsed();
        for ((effect, response_tx, span, enqueued_at, in_flight), outcome) in prepared {
            let _guard = span.enter();
            let queue_wait = enqueued_at.elapsed().saturating_sub(service_elapsed);
            let event = match outcome {
                Ok(event) => match &group_error {
                    Some(error) => StorageEvent::Error {
                        error: error.clone(),
                    },
                    None => event,
                },
                Err(error) => StorageEvent::Error { error },
            };
            let result = storage_event_kind(&event);
            span.record("queue_wait_ms", duration_ms(queue_wait));
            span.record("service_ms", duration_ms(service_elapsed));
            span.record("result", result);
            span.record("path", "group_commit");
            slow_queue.observe(
                storage_effect_kind(&effect),
                storage_effect_key_space(&effect),
                queue_wait,
                service_elapsed,
                result,
            );
            drop(in_flight);
            if !response_tx.is_disconnected() {
                response_tx.send(event);
            }
        }
    }

    fn apply_group_member(
        &self,
        tx: &mut fjall::OptimisticWriteTx,
        effect: &StorageEffect,
    ) -> Result<StorageEvent, StorageError> {
        match effect {
            StorageEffect::Write {
                key_space,
                key,
                value,
                txn_id: None,
            } => {
                let keyspace = self.store.resolve_keyspace(key_space)?;
                tx.insert(keyspace, key.clone(), value.clone());
                Ok(StorageEvent::WriteResult { key: key.clone() })
            }
            StorageEffect::BatchWrite {
                writes,
                txn_id: None,
            } => {
                let mut entries = Vec::with_capacity(writes.len());
                let mut resolved = Vec::with_capacity(writes.len());
                for (key_space, key, value) in writes {
                    let keyspace = self.store.resolve_keyspace(key_space)?;
                    resolved.push((keyspace, key_space, key, value));
                }
                for (keyspace, key_space, key, value) in resolved {
                    tx.insert(keyspace, key.clone(), value.clone());
                    entries.push((key_space.clone(), key.clone()));
                }
                Ok(StorageEvent::BatchWriteResult { entries })
            }
            StorageEffect::Delete {
                key_space,
                key,
                txn_id: None,
            } => {
                let keyspace = self.store.resolve_keyspace(key_space)?;
                tx.remove(keyspace, key.clone());
                Ok(StorageEvent::DeleteResult { key: key.clone() })
            }
            StorageEffect::BatchDelete {
                deletes,
                txn_id: None,
            } => {
                let mut entries = Vec::with_capacity(deletes.len());
                let mut resolved = Vec::with_capacity(deletes.len());
                for (key_space, key) in deletes {
                    let keyspace = self.store.resolve_keyspace(key_space)?;
                    resolved.push((keyspace, key_space, key));
                }
                for (keyspace, key_space, key) in resolved {
                    tx.remove(keyspace, key.clone());
                    entries.push((key_space.clone(), key.clone()));
                }
                Ok(StorageEvent::BatchDeleteResult { entries })
            }
            _ => Err(StorageError::InvalidEffect),
        }
    }

    fn cleanup_abandoned_transaction(
        &mut self,
        txn_id: Ulid,
        operation: &'static str,
        reason: &'static str,
    ) {
        match self.abort_transaction(txn_id) {
            StorageEvent::TransactionAborted { .. } => warn!(
                event = "storage.transaction.aborted",
                txn_id = %txn_id,
                operation,
                reason,
                "Aborted abandoned storage transaction"
            ),
            StorageEvent::Error { error } => warn!(
                event = "storage.transaction.abort_failed",
                txn_id = %txn_id,
                operation,
                reason,
                error = %error,
                "Failed to abort abandoned storage transaction"
            ),
            other => warn!(
                event = "storage.transaction.abort_unexpected",
                txn_id = %txn_id,
                operation,
                reason,
                result = storage_event_kind(&other),
                "Unexpected result while aborting abandoned storage transaction"
            ),
        }
    }

    fn sync_all(&self) -> StorageEvent {
        match self.persist_with_mode(PersistMode::SyncAll) {
            Ok(()) => StorageEvent::SyncAllFinished,
            Err(error) => StorageEvent::Error { error },
        }
    }

    fn persist_journal(&self) -> Result<(), StorageError> {
        self.persist_with_mode(self.persist_policy.as_fjall())
    }

    fn persist_with_mode(&self, mode: PersistMode) -> Result<(), StorageError> {
        let persist_started = Instant::now();
        self.store
            .db
            .persist(mode)
            .map_err(|error| StorageError::PersistError(error.to_string()))?;
        Span::current().record("persist_ms", duration_ms(persist_started.elapsed()));
        Ok(())
    }

    fn buffered_write_tx(&self) -> Result<fjall::OptimisticWriteTx, StorageError> {
        self.store
            .db
            .write_tx()
            .map(|tx| tx.durability(Some(self.persist_policy.as_fjall())))
            .map_err(|_| StorageError::WriteError)
    }

    fn commit_buffered_write_tx(&self, tx: fjall::OptimisticWriteTx) -> Result<(), StorageError> {
        let commit_started = Instant::now();
        match tx.commit() {
            Ok(Ok(())) => {
                Span::current().record("commit_ms", duration_ms(commit_started.elapsed()));
                Ok(())
            }
            Ok(Err(_)) => {
                Span::current().record("commit_ms", duration_ms(commit_started.elapsed()));
                Err(StorageError::TransactionConflict)
            }
            Err(_) => {
                Span::current().record("commit_ms", duration_ms(commit_started.elapsed()));
                Err(StorageError::WriteError)
            }
        }
    }

    #[tracing::instrument(
        name = "storage.start_transaction",
        level = "debug",
        skip(self),
        fields(read)
    )]
    fn start_transaction(&mut self, read: bool) -> StorageEvent {
        let retained = self
            .transaction_cleanup
            .lock()
            .expect("transaction cleanup mutex poisoned")
            .len();
        if self.txns.len().saturating_add(retained) >= MAX_TRANSACTION_CLEANUP {
            return StorageEvent::Error {
                error: StorageError::TransactionConflict,
            };
        }
        let txn_id = Ulid::generate();

        let txn = if read {
            let txn = self.store.db.read_tx();
            Txn::Read(txn)
        } else {
            match self.store.db.write_tx() {
                Ok(txn) => {
                    let txn = txn.durability(Some(self.persist_policy.as_fjall()));
                    Txn::Write(Box::new(txn))
                }
                Err(_e) => {
                    return StorageEvent::Error {
                        error: StorageError::TransactionConflict,
                    };
                }
            }
        };

        self.txns.insert(txn_id, txn);
        StorageEvent::TransactionStarted { txn_id }
    }

    #[tracing::instrument(name = "storage.abort_transaction", level = "debug", skip(self), fields(txn_id = %txn_id))]
    fn abort_transaction(&mut self, txn_id: Ulid) -> StorageEvent {
        match self.txns.remove(&txn_id) {
            Some(Txn::Write(txn)) => {
                txn.rollback();
                StorageEvent::TransactionAborted { txn_id }
            }
            Some(Txn::Read(_txn)) => StorageEvent::TransactionAborted { txn_id },
            None => StorageEvent::Error {
                error: StorageError::TransactionNotFound,
            },
        }
    }

    #[tracing::instrument(
        name = "storage.read",
        level = "debug",
        skip(self, key),
        fields(key_space = %key_space, key_len = key.as_ref().len(), txn_id = ?txn_id)
    )]
    fn read(&mut self, key_space: String, key: ByteView, txn_id: Option<Ulid>) -> StorageEvent {
        let keyspace = match self.store.resolve_keyspace(&key_space) {
            Ok(ks) => ks,
            Err(e) => return StorageEvent::Error { error: e },
        };

        if let Some(txn_id) = txn_id {
            match self.txns.get(&txn_id) {
                Some(Txn::Read(txn)) => match txn.get(keyspace, &key) {
                    Ok(value_opt) => StorageEvent::ReadResult {
                        key,
                        value: value_opt.map(|v| v.into()),
                    },
                    Err(_e) => StorageEvent::Error {
                        error: StorageError::ReadError,
                    },
                },
                Some(Txn::Write(txn)) => match txn.get(keyspace, &key) {
                    Ok(value_opt) => StorageEvent::ReadResult {
                        key,
                        value: value_opt.map(|v| v.into()),
                    },
                    Err(_e) => StorageEvent::Error {
                        error: StorageError::ReadError,
                    },
                },
                None => StorageEvent::Error {
                    error: StorageError::TransactionNotFound,
                },
            }
        } else {
            store_read(&self.store, keyspace, key)
        }
    }

    fn batch_read(&mut self, reads: Vec<(String, ByteView)>, txn_id: Option<Ulid>) -> StorageEvent {
        if let Some(txn_id) = txn_id {
            match self.txns.get(&txn_id) {
                Some(Txn::Read(txn)) => batch_read_with(&self.store, txn, reads),
                Some(Txn::Write(txn)) => batch_read_with(&self.store, txn.as_ref(), reads),
                None => StorageEvent::Error {
                    error: StorageError::TransactionNotFound,
                },
            }
        } else {
            store_batch_read(&self.store, reads)
        }
    }

    #[tracing::instrument(
        name = "storage.write",
        level = "debug",
        skip(self, key, value),
        fields(key_space = %key_space, key_len = key.as_ref().len(), value_len = value.as_ref().len(), txn_id = ?txn_id)
    )]
    fn write(
        &mut self,
        key_space: String,
        key: ByteView,
        value: ByteView,
        txn_id: Option<Ulid>,
    ) -> StorageEvent {
        let keyspace = match self.store.resolve_keyspace(&key_space) {
            Ok(ks) => ks,
            Err(e) => return StorageEvent::Error { error: e },
        };

        if let Some(txn_id) = txn_id {
            if let Some(Txn::Write(txn)) = self.txns.get_mut(&txn_id) {
                txn.insert(keyspace, key.clone(), value);
                StorageEvent::WriteResult { key }
            } else {
                StorageEvent::Error {
                    error: StorageError::TransactionNotFound,
                }
            }
        } else {
            let result = self.buffered_write_tx().and_then(|mut tx| {
                tx.insert(keyspace, key.clone(), value);
                self.commit_buffered_write_tx(tx)?;
                self.persist_journal()
            });
            if let Err(error) = result {
                return StorageEvent::Error { error };
            }
            StorageEvent::WriteResult { key }
        }
    }

    #[tracing::instrument(
        name = "storage.batch_write",
        level = "debug",
        skip(self, writes),
        fields(write_count = writes.len(), txn_id = ?txn_id)
    )]
    fn batch_write(
        &mut self,
        writes: Vec<(String, ByteView, ByteView)>,
        txn_id: Option<Ulid>,
    ) -> StorageEvent {
        let mut entries = Vec::with_capacity(writes.len());
        let mut resolved = Vec::with_capacity(writes.len());
        for (key_space, key, value) in writes {
            let keyspace = match self.store.resolve_keyspace(&key_space) {
                Ok(ks) => ks,
                Err(error) => return StorageEvent::Error { error },
            };
            resolved.push((keyspace, key_space, key, value));
        }

        if let Some(txn_id) = txn_id {
            let Some(Txn::Write(txn)) = self.txns.get_mut(&txn_id) else {
                return StorageEvent::Error {
                    error: StorageError::TransactionNotFound,
                };
            };

            for (keyspace, key_space, key, value) in resolved {
                txn.insert(keyspace, key.clone(), value);
                entries.push((key_space, key));
            }
        } else {
            let mut tx = match self.buffered_write_tx() {
                Ok(tx) => tx,
                Err(error) => return StorageEvent::Error { error },
            };
            for (keyspace, key_space, key, value) in resolved {
                tx.insert(keyspace, key.clone(), value);
                entries.push((key_space, key));
            }
            if let Err(error) = self
                .commit_buffered_write_tx(tx)
                .and_then(|()| self.persist_journal())
            {
                return StorageEvent::Error { error };
            }
        }

        StorageEvent::BatchWriteResult { entries }
    }

    #[tracing::instrument(name = "storage.commit_transaction", level = "debug", skip(self), fields(txn_id = %txn_id))]
    fn commit_transaction(&mut self, txn_id: Ulid) -> StorageEvent {
        match self.txns.remove(&txn_id) {
            Some(Txn::Read(_txn)) => {
                // Read-only transactions do not need to commit
                StorageEvent::TransactionCommitted { txn_id }
            }

            Some(Txn::Write(txn)) => match txn.commit() {
                Ok(Ok(())) => {
                    if let Err(error) = self.persist_journal() {
                        return StorageEvent::Error { error };
                    }
                    StorageEvent::TransactionCommitted { txn_id }
                }
                Ok(Err(_)) => StorageEvent::Error {
                    error: StorageError::TransactionConflict,
                },
                // fjall only reaches here after its journal write began, so the
                // batch may still be replayed; a conflict never wrote anything.
                Err(error) => {
                    warn!(
                        event = "storage.transaction.commit_failed",
                        txn_id = %txn_id,
                        error = %error,
                        "Storage transaction commit failed with an unknown outcome"
                    );
                    StorageEvent::Error {
                        error: StorageError::CommitFailed,
                    }
                }
            },
            None => StorageEvent::Error {
                error: StorageError::TransactionNotFound,
            },
        }
    }

    #[tracing::instrument(
        name = "storage.delete",
        level = "debug",
        skip(self, key),
        fields(key_space = %key_space, key_len = key.as_ref().len(), txn_id = ?txn_id)
    )]
    fn delete(&mut self, key_space: String, key: ByteView, txn_id: Option<Ulid>) -> StorageEvent {
        let keyspace = match self.store.resolve_keyspace(&key_space) {
            Ok(ks) => ks,
            Err(e) => return StorageEvent::Error { error: e },
        };

        if let Some(txn_id) = txn_id {
            if let Some(Txn::Write(txn)) = self.txns.get_mut(&txn_id) {
                txn.remove(keyspace, key.clone());
                StorageEvent::DeleteResult { key }
            } else {
                StorageEvent::Error {
                    error: StorageError::TransactionNotFound,
                }
            }
        } else {
            let mut tx = match self.buffered_write_tx() {
                Ok(tx) => tx,
                Err(error) => return StorageEvent::Error { error },
            };
            tx.remove(keyspace, key.clone());
            if let Err(error) = self.commit_buffered_write_tx(tx) {
                return StorageEvent::Error { error };
            }
            if let Err(error) = self.persist_journal() {
                return StorageEvent::Error { error };
            }
            StorageEvent::DeleteResult { key }
        }
    }

    #[tracing::instrument(
        name = "storage.batch_delete",
        level = "debug",
        skip(self, deletes),
        fields(delete_count = deletes.len(), txn_id = ?txn_id)
    )]
    fn batch_delete(
        &mut self,
        deletes: Vec<(String, ByteView)>,
        txn_id: Option<Ulid>,
    ) -> StorageEvent {
        let mut entries = Vec::with_capacity(deletes.len());
        let mut resolved = Vec::with_capacity(deletes.len());
        for (key_space, key) in deletes {
            let keyspace = match self.store.resolve_keyspace(&key_space) {
                Ok(ks) => ks,
                Err(error) => return StorageEvent::Error { error },
            };
            resolved.push((keyspace, key_space, key));
        }

        if let Some(txn_id) = txn_id {
            let Some(Txn::Write(txn)) = self.txns.get_mut(&txn_id) else {
                return StorageEvent::Error {
                    error: StorageError::TransactionNotFound,
                };
            };

            for (keyspace, key_space, key) in resolved {
                txn.remove(keyspace, key.clone());
                entries.push((key_space, key));
            }
        } else {
            let mut tx = match self.buffered_write_tx() {
                Ok(tx) => tx,
                Err(error) => return StorageEvent::Error { error },
            };
            for (keyspace, key_space, key) in resolved {
                tx.remove(keyspace, key.clone());
                entries.push((key_space, key));
            }
            if let Err(error) = self
                .commit_buffered_write_tx(tx)
                .and_then(|()| self.persist_journal())
            {
                return StorageEvent::Error { error };
            }
        }

        StorageEvent::BatchDeleteResult { entries }
    }

    #[tracing::instrument(
        name = "storage.iterate",
        level = "debug",
        skip(self, prefix, start),
        fields(key_space = %key_space, has_prefix = prefix.is_some(), has_cursor = start.is_some(), limit, txn_id = ?txn_id)
    )]
    fn iterate(
        &mut self,
        key_space: String,
        prefix: Option<ByteView>,
        start: Option<IterStart>,
        limit: usize,
        txn_id: Option<Ulid>,
    ) -> StorageEvent {
        let keyspace = match self.store.resolve_keyspace(&key_space) {
            Ok(ks) => ks,
            Err(e) => return StorageEvent::Error { error: e },
        };

        if limit == 0 {
            return StorageEvent::IterResult {
                values: Vec::new(),
                next_start_after: None,
            };
        }

        let result = if let Some(txn_id) = txn_id {
            match self.txns.get(&txn_id) {
                Some(Txn::Read(txn)) => {
                    iterate_page(txn, &keyspace, prefix.as_ref(), start.as_ref(), limit)
                }
                Some(Txn::Write(txn)) => iterate_page(
                    txn.as_ref(),
                    &keyspace,
                    prefix.as_ref(),
                    start.as_ref(),
                    limit,
                ),
                None => {
                    return StorageEvent::Error {
                        error: StorageError::TransactionNotFound,
                    };
                }
            }
        } else {
            return store_iterate(&self.store, keyspace, prefix, start, limit);
        };

        match result {
            Ok((values, next_start_after)) => StorageEvent::IterResult {
                values,
                next_start_after,
            },
            Err(error) => StorageEvent::Error { error },
        }
    }
}

fn store_read(store: &Store, keyspace: OptimisticTxKeyspace, key: ByteView) -> StorageEvent {
    let snapshot = store.db.read_tx();
    match snapshot.get(&keyspace, &key) {
        Ok(value_opt) => StorageEvent::ReadResult {
            key,
            value: value_opt.map(|v| v.into()),
        },
        Err(_e) => StorageEvent::Error {
            error: StorageError::ReadError,
        },
    }
}

fn batch_read_with<R: Readable>(
    store: &Store,
    reader: &R,
    reads: Vec<(String, ByteView)>,
) -> StorageEvent {
    let mut values = Vec::with_capacity(reads.len());
    for (key_space, key) in reads {
        let keyspace = match store.resolve_keyspace(&key_space) {
            Ok(ks) => ks,
            Err(error) => return StorageEvent::Error { error },
        };
        match reader.get(&keyspace, &key) {
            Ok(value_opt) => values.push((key, value_opt.map(Into::into))),
            Err(_e) => {
                return StorageEvent::Error {
                    error: StorageError::ReadError,
                };
            }
        }
    }
    StorageEvent::BatchReadResult { values }
}

fn store_batch_read(store: &Store, reads: Vec<(String, ByteView)>) -> StorageEvent {
    let snapshot = store.db.read_tx();
    batch_read_with(store, &snapshot, reads)
}

fn store_iterate(
    store: &Store,
    keyspace: OptimisticTxKeyspace,
    prefix: Option<ByteView>,
    start: Option<IterStart>,
    limit: usize,
) -> StorageEvent {
    let snapshot = store.db.read_tx();
    match iterate_page(&snapshot, &keyspace, prefix.as_ref(), start.as_ref(), limit) {
        Ok((values, next_start_after)) => StorageEvent::IterResult {
            values,
            next_start_after,
        },
        Err(error) => StorageEvent::Error { error },
    }
}

fn is_groupable_write(effect: &StorageEffect) -> bool {
    matches!(
        effect,
        StorageEffect::Write { txn_id: None, .. }
            | StorageEffect::BatchWrite { txn_id: None, .. }
            | StorageEffect::Delete { txn_id: None, .. }
            | StorageEffect::BatchDelete { txn_id: None, .. }
    )
}

fn is_poolable_read(effect: &StorageEffect) -> bool {
    matches!(
        effect,
        StorageEffect::Read { txn_id: None, .. }
            | StorageEffect::BatchRead { txn_id: None, .. }
            | StorageEffect::Iter { txn_id: None, .. }
    )
}

#[derive(Default)]
struct PendingWriteIndex {
    key_spaces: Vec<String>,
    keys: Vec<PendingWriteKey>,
    sorted: bool,
}

struct PendingWriteKey {
    key_space: usize,
    key: ByteView,
}

impl PendingWriteIndex {
    fn from_group(group: &[EffectHandle]) -> Self {
        let mut index = Self {
            key_spaces: Vec::with_capacity(group.len()),
            keys: Vec::with_capacity(group.len()),
            sorted: true,
        };
        for (effect, _, _, _, _) in group {
            index.insert(effect);
        }
        index
    }

    fn insert(&mut self, effect: &StorageEffect) {
        match effect {
            StorageEffect::Write {
                key_space,
                key,
                txn_id: None,
                ..
            }
            | StorageEffect::Delete {
                key_space,
                key,
                txn_id: None,
            } => self.insert_key(key_space, key),
            StorageEffect::BatchWrite {
                writes,
                txn_id: None,
            } => {
                for (key_space, key, _) in writes {
                    self.insert_key(key_space, key);
                }
            }
            StorageEffect::BatchDelete {
                deletes,
                txn_id: None,
            } => {
                for (key_space, key) in deletes {
                    self.insert_key(key_space, key);
                }
            }
            _ => {}
        }
    }

    fn insert_key(&mut self, key_space: &str, key: &ByteView) {
        let key_space = self.key_space_index_or_insert(key_space);
        self.keys.push(PendingWriteKey {
            key_space,
            key: key.clone(),
        });
        self.sorted = false;
    }

    fn conflicts_with_read(&mut self, read: &StorageEffect) -> bool {
        match read {
            StorageEffect::Read {
                key_space,
                key,
                txn_id: None,
            } => self.contains_key(key_space, key),
            StorageEffect::BatchRead {
                reads,
                txn_id: None,
            } => reads
                .iter()
                .any(|(key_space, key)| self.contains_key(key_space, key)),
            StorageEffect::Iter {
                key_space,
                prefix,
                start,
                limit,
                txn_id: None,
            } => *limit != 0 && self.contains_iter_key(key_space, prefix.as_ref(), start.as_ref()),
            _ => false,
        }
    }

    fn contains_key(&mut self, key_space: &str, key: &ByteView) -> bool {
        let Some(key_space) = self.key_space_index(key_space) else {
            return false;
        };
        self.sort_keys();
        self.keys
            .binary_search_by(|pending| compare_pending_key(pending, key_space, key.as_ref()))
            .is_ok()
    }

    fn contains_iter_key(
        &mut self,
        key_space: &str,
        prefix: Option<&ByteView>,
        start: Option<&IterStart>,
    ) -> bool {
        let Some(key_space) = self.key_space_index(key_space) else {
            return false;
        };
        self.sort_keys();
        let start_index = self
            .keys
            .partition_point(|pending| pending.key_space < key_space);
        let end_index = self
            .keys
            .partition_point(|pending| pending.key_space <= key_space);
        self.keys[start_index..end_index]
            .iter()
            .any(|pending| iter_may_include_key(prefix, start, &pending.key))
    }

    fn key_space_index(&self, key_space: &str) -> Option<usize> {
        self.key_spaces
            .iter()
            .position(|existing| existing.as_str() == key_space)
    }

    fn key_space_index_or_insert(&mut self, key_space: &str) -> usize {
        match self.key_space_index(key_space) {
            Some(index) => index,
            None => {
                self.key_spaces.push(key_space.to_string());
                self.key_spaces.len() - 1
            }
        }
    }

    fn sort_keys(&mut self) {
        if self.sorted {
            return;
        }
        self.keys.sort_unstable_by(|left, right| {
            left.key_space
                .cmp(&right.key_space)
                .then_with(|| left.key.as_ref().cmp(right.key.as_ref()))
        });
        self.keys.dedup_by(|left, right| {
            left.key_space == right.key_space && left.key.as_ref() == right.key.as_ref()
        });
        self.sorted = true;
    }
}

fn compare_pending_key(
    pending: &PendingWriteKey,
    key_space: usize,
    key: &[u8],
) -> std::cmp::Ordering {
    pending
        .key_space
        .cmp(&key_space)
        .then_with(|| pending.key.as_ref().cmp(key))
}

fn iter_may_include_key(
    prefix: Option<&ByteView>,
    start: Option<&IterStart>,
    key: &ByteView,
) -> bool {
    let key = key.as_ref();
    if let Some(prefix) = prefix
        && !key.starts_with(prefix.as_ref())
    {
        return false;
    }
    match start {
        Some(IterStart::After(start)) if key <= start.as_ref() => false,
        Some(IterStart::At(start)) if key < start.as_ref() => false,
        _ => true,
    }
}

/// Lane picker for the write actor. Foreground is preferred, and every
/// [`FOREGROUND_PER_BULK`] foreground effects earn one bulk effect, so the drain
/// keeps a fixed share of the actor no matter how deep the foreground queue is.
#[derive(Debug, Default)]
struct LaneScheduler {
    credit: usize,
}

impl LaneScheduler {
    fn record_foreground(&mut self, served: usize) {
        self.credit = self.credit.saturating_add(served);
    }

    fn next(
        &mut self,
        receivers: &StorageReceivers,
    ) -> Result<(EffectHandle, StoragePriority), RecvError> {
        loop {
            if self.credit >= FOREGROUND_PER_BULK
                && let Ok(item) = receivers.bulk.try_recv()
            {
                self.credit = self.credit.saturating_sub(FOREGROUND_PER_BULK);
                return Ok((item, StoragePriority::Bulk));
            }
            let foreground = receivers.foreground.try_recv();
            if let Ok(item) = foreground {
                return Ok((item, StoragePriority::Foreground));
            }
            let bulk = receivers.bulk.try_recv();
            if let Ok(item) = bulk {
                self.credit = 0;
                return Ok((item, StoragePriority::Bulk));
            }
            match (foreground, bulk) {
                (Err(TryRecvError::Disconnected), Err(TryRecvError::Disconnected)) => {
                    return Err(RecvError);
                }
                (Err(TryRecvError::Disconnected), _) => {
                    self.credit = 0;
                    return receivers
                        .bulk
                        .recv()
                        .map(|item| (item, StoragePriority::Bulk));
                }
                (_, Err(TryRecvError::Disconnected)) => {
                    self.credit = 0;
                    return receivers
                        .foreground
                        .recv()
                        .map(|item| (item, StoragePriority::Foreground));
                }
                _ => {}
            }
            // Both lanes are open but empty: block on a biased select so an idle
            // node sleeps with zero wakeups and foreground stays preferred.
            self.credit = 0;
            let mut select = Select::new_bias();
            select.add(&receivers.foreground);
            select.add(&receivers.bulk);
            let received = match select.select() {
                Ok(result) if result == receivers.foreground => receivers
                    .foreground
                    .read_select(result)
                    .map(|item| (item, StoragePriority::Foreground)),
                Ok(result) => receivers
                    .bulk
                    .read_select(result)
                    .map(|item| (item, StoragePriority::Bulk)),
                Err(RecvError) => return Err(RecvError),
            };
            if let Ok(pair) = received {
                return Ok(pair);
            }
        }
    }
}

fn reject_bulk_read(item: EffectHandle) {
    let (effect, response_tx, span, _enqueued_at, in_flight) = item;
    let _guard = span.enter();
    warn!(
        event = "storage.bulk_read.queue_full",
        operation = storage_effect_kind(&effect),
        "Rejecting bulk read: bulk read pool queue full"
    );
    drop(in_flight);
    if !response_tx.is_disconnected() {
        response_tx.send(StorageEvent::Error {
            error: StorageError::QueueFull,
        });
    }
}

fn spawn_read_pool(store: Store, threads: usize, capacity: usize) -> Vec<EffectSender> {
    let mut senders = Vec::with_capacity(threads);
    for _ in 0..threads {
        let (sender, receiver) = mpsc::bounded_blocking(capacity);
        let store = store.clone();
        thread::spawn(move || read_pool_loop(store, receiver));
        senders.push(sender);
    }
    senders
}

fn read_pool_loop(store: Store, receiver: EffectReceiver) {
    while let Ok((effect, response_tx, span, enqueued_at, in_flight)) = receiver.recv() {
        let _guard = span.enter();
        if response_tx.is_disconnected() {
            continue;
        }
        let operation = storage_effect_kind(&effect);
        let key_space = storage_effect_key_space(&effect).map(str::to_string);
        let queue_wait = enqueued_at.elapsed();
        let service_started = Instant::now();
        let event = match effect {
            StorageEffect::Read {
                key_space,
                key,
                txn_id: None,
            } => match store.resolve_keyspace(&key_space) {
                Ok(keyspace) => store_read(&store, keyspace, key),
                Err(error) => StorageEvent::Error { error },
            },
            StorageEffect::BatchRead {
                reads,
                txn_id: None,
            } => store_batch_read(&store, reads),
            StorageEffect::Iter {
                key_space,
                prefix,
                start,
                limit,
                txn_id: None,
            } => match store.resolve_keyspace(&key_space) {
                Ok(keyspace) => {
                    if limit == 0 {
                        StorageEvent::IterResult {
                            values: Vec::new(),
                            next_start_after: None,
                        }
                    } else {
                        store_iterate(&store, keyspace, prefix, start, limit)
                    }
                }
                Err(error) => StorageEvent::Error { error },
            },
            _ => StorageEvent::Error {
                error: StorageError::InvalidEffect,
            },
        };
        let service_elapsed = service_started.elapsed();
        span.record("queue_wait_ms", duration_ms(queue_wait));
        span.record("service_ms", duration_ms(service_elapsed));
        span.record("result", storage_event_kind(&event));
        span.record("path", "read_pool");
        record_storage_call(operation, key_space.as_deref(), queue_wait, service_elapsed);
        if service_elapsed >= SLOW_STORAGE_EFFECT_THRESHOLD {
            warn!(
                event = "storage.effect.slow",
                operation = storage_event_kind(&event),
                service_ms = duration_ms(service_elapsed),
                queue_wait_ms = duration_ms(queue_wait),
                "Slow storage read"
            );
        }
        drop(in_flight);
        if !response_tx.is_disconnected() {
            response_tx.send(event);
        }
    }
}

#[derive(Default)]
struct SlowQueueAggregator {
    queued_count: u64,
    max_queue_wait: Duration,
    last_flush: Option<Instant>,
}

impl SlowQueueAggregator {
    fn observe(
        &mut self,
        operation: &'static str,
        key_space: Option<&str>,
        queue_wait: Duration,
        service_elapsed: Duration,
        result: &'static str,
    ) {
        record_storage_call(operation, key_space, queue_wait, service_elapsed);
        if service_elapsed >= SLOW_STORAGE_EFFECT_THRESHOLD {
            warn!(
                event = "storage.effect.slow",
                operation,
                result,
                queue_wait_ms = duration_ms(queue_wait),
                service_ms = duration_ms(service_elapsed),
                threshold_ms = duration_ms(SLOW_STORAGE_EFFECT_THRESHOLD),
                "Slow storage effect"
            );
        }
        if queue_wait < SLOW_STORAGE_EFFECT_THRESHOLD {
            return;
        }
        self.queued_count += 1;
        self.max_queue_wait = self.max_queue_wait.max(queue_wait);
        let now = Instant::now();
        let due = self
            .last_flush
            .is_none_or(|last| now.duration_since(last) >= SLOW_QUEUE_LOG_INTERVAL);
        if due {
            warn!(
                event = "storage.queue.backlog",
                slow_queued_effects = self.queued_count,
                max_queue_wait_ms = duration_ms(self.max_queue_wait),
                threshold_ms = duration_ms(SLOW_STORAGE_EFFECT_THRESHOLD),
                "Storage effects waited longer than threshold in queue"
            );
            self.queued_count = 0;
            self.max_queue_wait = Duration::ZERO;
            self.last_flush = Some(now);
        }
    }
}

fn storage_effect_span(effect: &StorageEffect) -> Span {
    let span = debug_span!(
        "storage.effect",
        "otel.kind" = "internal",
        operation = storage_effect_kind(effect),
        key_space = field::Empty,
        txn_id = field::Empty,
        key_len = field::Empty,
        value_len = field::Empty,
        cursor_len = field::Empty,
        batch_len = field::Empty,
        limit = field::Empty,
        read = field::Empty,
        queue_wait_ms = field::Empty,
        service_ms = field::Empty,
        total_elapsed_ms = field::Empty,
        path = field::Empty,
        persist_mode = field::Empty,
        commit_ms = field::Empty,
        persist_ms = field::Empty,
        result = field::Empty,
    );
    record_storage_effect_fields(&span, effect);
    span
}

fn record_storage_effect_fields(span: &Span, effect: &StorageEffect) {
    match effect {
        StorageEffect::StartTransaction { read } => {
            span.record("read", *read);
        }
        StorageEffect::CommitTransaction { txn_id }
        | StorageEffect::AbortTransaction { txn_id } => {
            span.record("txn_id", field::display(txn_id));
        }
        StorageEffect::Read {
            key_space,
            key,
            txn_id,
        }
        | StorageEffect::Delete {
            key_space,
            key,
            txn_id,
        } => {
            span.record("key_space", field::display(key_space));
            span.record("key_len", key.as_ref().len() as u64);
            if let Some(txn_id) = txn_id {
                span.record("txn_id", field::display(txn_id));
            }
        }
        StorageEffect::Write {
            key_space,
            key,
            value,
            txn_id,
        } => {
            span.record("key_space", field::display(key_space));
            span.record("key_len", key.as_ref().len() as u64);
            span.record("value_len", value.as_ref().len() as u64);
            if let Some(txn_id) = txn_id {
                span.record("txn_id", field::display(txn_id));
            }
        }
        StorageEffect::BatchRead { reads, txn_id } => {
            span.record("batch_len", reads.len() as u64);
            if let Some(txn_id) = txn_id {
                span.record("txn_id", field::display(txn_id));
            }
        }
        StorageEffect::BatchWrite { writes, txn_id } => {
            span.record("batch_len", writes.len() as u64);
            if let Some(txn_id) = txn_id {
                span.record("txn_id", field::display(txn_id));
            }
        }
        StorageEffect::BatchDelete { deletes, txn_id } => {
            span.record("batch_len", deletes.len() as u64);
            if let Some(txn_id) = txn_id {
                span.record("txn_id", field::display(txn_id));
            }
        }
        StorageEffect::Iter {
            key_space,
            prefix,
            start,
            limit,
            txn_id,
        } => {
            span.record("key_space", field::display(key_space));
            if let Some(prefix) = prefix {
                span.record("key_len", prefix.as_ref().len() as u64);
            }
            if let Some(start) = start {
                span.record("cursor_len", start.key().as_ref().len() as u64);
            }
            span.record("limit", *limit as u64);
            if let Some(txn_id) = txn_id {
                span.record("txn_id", field::display(txn_id));
            }
        }
        StorageEffect::SyncAll => {}
    }
}

fn storage_effect_kind(effect: &StorageEffect) -> &'static str {
    match effect {
        StorageEffect::StartTransaction { .. } => "start_transaction",
        StorageEffect::CommitTransaction { .. } => "commit_transaction",
        StorageEffect::Read { .. } => "read",
        StorageEffect::BatchRead { .. } => "batch_read",
        StorageEffect::Write { .. } => "write",
        StorageEffect::BatchWrite { .. } => "batch_write",
        StorageEffect::Delete { .. } => "delete",
        StorageEffect::BatchDelete { .. } => "batch_delete",
        StorageEffect::AbortTransaction { .. } => "abort_transaction",
        StorageEffect::SyncAll => "sync_all",
        StorageEffect::Iter { .. } => "iter",
    }
}

fn effect_kind(effect: &Effect) -> &'static str {
    match effect {
        Effect::Storage(storage_effect) => storage_effect_kind(storage_effect),
        Effect::Blob(_) => "blob",
        Effect::StagingSource(_) => "staging_source",
        Effect::Net(_) => "net",
        Effect::Metadata(_) => "metadata",
        Effect::SubOperation(_) => "suboperation",
        Effect::Task(_) => "task",
        Effect::Search() => "search",
        Effect::Stream() => "stream",
    }
}

fn storage_event_kind(event: &StorageEvent) -> &'static str {
    match event {
        StorageEvent::TransactionStarted { .. } => "transaction_started",
        StorageEvent::TransactionCommitted { .. } => "transaction_committed",
        StorageEvent::TransactionAborted { .. } => "transaction_aborted",
        StorageEvent::ReadResult { .. } => "read_result",
        StorageEvent::BatchReadResult { .. } => "batch_read_result",
        StorageEvent::WriteResult { .. } => "write_result",
        StorageEvent::BatchWriteResult { .. } => "batch_write_result",
        StorageEvent::DeleteResult { .. } => "delete_result",
        StorageEvent::BatchDeleteResult { .. } => "batch_delete_result",
        StorageEvent::SyncAllFinished => "sync_all_finished",
        StorageEvent::IterResult { .. } => "iter_result",
        StorageEvent::Error { .. } => "error",
    }
}

fn iterate_page<R: Readable>(
    reader: &R,
    keyspace: &OptimisticTxKeyspace,
    prefix: Option<&ByteView>,
    start: Option<&IterStart>,
    limit: usize,
) -> Result<PageResult, StorageError> {
    let prefix_bytes = prefix.map(|p| p.as_ref().to_vec());
    let start_bound = start.map(|start| match start {
        IterStart::After(key) => Excluded(key.as_ref().to_vec()),
        IterStart::At(key) => Included(key.as_ref().to_vec()),
    });

    let iter = match (prefix_bytes.as_ref(), start_bound) {
        (Some(prefix), Some(start_bound)) => {
            let start_bound = match start_bound {
                Excluded(key) | Included(key) if &key < prefix => Included(prefix.clone()),
                bound => bound,
            };

            match prefix_upper_bound(prefix) {
                Some(end) => reader.range(keyspace, (start_bound, Excluded(end))),
                None => reader.range(keyspace, (start_bound, Unbounded::<Vec<u8>>)),
            }
        }
        (Some(prefix), None) => match prefix_upper_bound(prefix) {
            Some(end) => reader.range(keyspace, (Included(prefix.clone()), Excluded(end))),
            None => reader.range(keyspace, (Included(prefix.clone()), Unbounded::<Vec<u8>>)),
        },
        (None, Some(start_bound)) => reader.range(keyspace, (start_bound, Unbounded::<Vec<u8>>)),
        (None, None) => reader.iter(keyspace),
    };

    collect_page(iter, limit)
}

fn collect_page(iter: fjall::Iter, limit: usize) -> Result<PageResult, StorageError> {
    let mut iter = iter.peekable();
    let mut values: Vec<(ByteView, ByteView)> = Vec::with_capacity(limit.min(1024));

    while let Some(guard) = iter.next() {
        let (key, value) = guard.into_inner().map_err(|_| StorageError::ReadError)?;
        values.push((ByteView::from(key.as_ref()), ByteView::from(value.as_ref())));

        if values.len() == limit {
            let next_start_after = if iter.peek().is_some() {
                values.last().map(|(k, _)| k.clone())
            } else {
                None
            };
            return Ok((values, next_start_after));
        }
    }

    Ok((values, None))
}

#[cfg(test)]
mod tests {
    use super::{FjallPersistPolicy, FjallStorage, StorageHandle};
    use aruna_core::effects::{Effect, IterStart, StorageEffect, StoragePriority};
    use aruna_core::errors::StorageError;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::handle::Handle;
    use std::sync::atomic::Ordering;
    use std::time::{Duration, Instant};
    use std::{env, process::Command, thread};
    use tempfile::tempdir;
    use ulid::Ulid;

    const RESTART_CHILD_PATH_ENV: &str = "ARUNA_STORAGE_RESTART_CHILD_PATH";
    const RESTART_CHILD_MODE_ENV: &str = "ARUNA_STORAGE_RESTART_CHILD_MODE";
    const RESTART_CHILD_TEST: &str = "storage::tests::buffered_persistence_restart_child_process";

    #[test]
    fn foreground_precedes_bulk() {
        // All four effects are enqueued before the first recv, so lane order is
        // deterministic: the single foreground effect precedes the three bulk.
        let (handle, receivers) = StorageHandle::new();
        let read = |key_space: &str| StorageEffect::Read {
            key_space: key_space.to_string(),
            key: b"k".to_vec().into(),
            txn_id: None,
        };
        let mut keep = Vec::new();
        for key_space in ["b1", "b2", "b3"] {
            let (tx, rx) = crossfire::oneshot::oneshot();
            let effect = read(key_space);
            let span = super::storage_effect_span(&effect);
            let in_flight = super::InFlightGuard::acquire(&handle.metrics);
            assert!(
                handle
                    .bulk_channel
                    .try_send((effect, tx, span, Instant::now(), in_flight))
                    .is_ok(),
                "bulk enqueue"
            );
            keep.push(rx);
        }
        let (tx, rx) = crossfire::oneshot::oneshot();
        let effect = read("fg");
        let span = super::storage_effect_span(&effect);
        let in_flight = super::InFlightGuard::acquire(&handle.metrics);
        assert!(
            handle
                .write_channel
                .try_send((effect, tx, span, Instant::now(), in_flight))
                .is_ok(),
            "foreground enqueue"
        );
        keep.push(rx);

        let mut lanes = super::LaneScheduler::default();
        let (first, priority) = lanes.next(&receivers).expect("first item");
        assert_eq!(priority, StoragePriority::Foreground);
        assert_eq!(super::storage_effect_key_space(&first.0), Some("fg"));
        for _ in 0..3 {
            let (_, priority) = lanes.next(&receivers).expect("bulk item");
            assert_eq!(priority, StoragePriority::Bulk);
        }
        drop(keep);
    }

    #[test]
    fn bulk_keeps_share() {
        // Credit is earned per foreground effect, not per batch: one batch may
        // swallow the whole foreground queue, and it must still buy the bulk
        // lane its proportional slots or a deep queue starves the drain.
        let (handle, receivers) = StorageHandle::new();
        let read = |key_space: &str| StorageEffect::Read {
            key_space: key_space.to_string(),
            key: b"k".to_vec().into(),
            txn_id: None,
        };
        let mut keep = Vec::new();
        let mut enqueue = |channel: &super::EffectSender, key_space: &str| {
            let (tx, rx) = crossfire::oneshot::oneshot();
            let effect = read(key_space);
            let span = super::storage_effect_span(&effect);
            let in_flight = super::InFlightGuard::acquire(&handle.metrics);
            assert!(
                channel
                    .try_send((effect, tx, span, Instant::now(), in_flight))
                    .is_ok(),
                "enqueue {key_space}"
            );
            keep.push(rx);
        };
        let batch = super::MAX_GROUP_COMMIT;
        let expected_bulk = batch / super::FOREGROUND_PER_BULK;
        for _ in 0..batch {
            enqueue(&handle.write_channel, "fg");
        }
        for _ in 0..expected_bulk {
            enqueue(&handle.bulk_channel, "bulk");
        }

        let mut lanes = super::LaneScheduler::default();
        let (_, priority) = lanes.next(&receivers).expect("first item");
        assert_eq!(priority, StoragePriority::Foreground);
        // The actor drains the rest of the foreground queue as one batch.
        lanes.record_foreground(batch);

        let mut bulk_served = 0usize;
        for _ in 0..expected_bulk {
            let (_, priority) = lanes.next(&receivers).expect("item");
            if priority == StoragePriority::Bulk {
                bulk_served += 1;
            }
        }
        assert_eq!(
            bulk_served, expected_bulk,
            "a full foreground batch buys {expected_bulk} bulk slots"
        );
        drop(keep);
    }

    #[test]
    fn abort_routes_foreground() {
        // A saturated bulk lane must not swallow a bulk handle's abort; aborts
        // free resources and always dispatch on the foreground lane.
        let (handle, receivers) = StorageHandle::new();
        let mut keep = Vec::new();
        loop {
            let (tx, rx) = crossfire::oneshot::oneshot();
            let effect = StorageEffect::Read {
                key_space: "b".to_string(),
                key: b"k".to_vec().into(),
                txn_id: None,
            };
            let span = super::storage_effect_span(&effect);
            let in_flight = super::InFlightGuard::acquire(&handle.metrics);
            if handle
                .bulk_channel
                .try_send((effect, tx, span, Instant::now(), in_flight))
                .is_err()
            {
                break;
            }
            keep.push(rx);
        }

        let txn_id = Ulid::from_parts(1, 1);
        handle.bulk().enqueue_abort_transaction(txn_id, "test");

        let (effect, ..) = receivers
            .foreground
            .try_recv()
            .expect("abort routed to foreground");
        assert!(matches!(
            effect,
            StorageEffect::AbortTransaction { txn_id: got } if got == txn_id
        ));
        assert!(
            receivers.bulk.try_recv().is_ok(),
            "bulk lane still saturated"
        );
        drop(keep);
    }

    #[tokio::test]
    async fn bulk_lane_works() {
        let dir = tempdir().unwrap();
        let handle = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let bulk = handle.bulk();
        let write = bulk
            .send_storage_effect(StorageEffect::Write {
                key_space: "node_state".to_string(),
                key: b"k".to_vec().into(),
                value: b"v".to_vec().into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            write,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
        let read = bulk
            .send_storage_effect(StorageEffect::Read {
                key_space: "node_state".to_string(),
                key: b"k".to_vec().into(),
                txn_id: None,
            })
            .await;
        match read {
            Event::Storage(StorageEvent::ReadResult {
                value: Some(value), ..
            }) => assert_eq!(value.as_ref(), b"v"),
            other => panic!("unexpected storage event: {other:?}"),
        }
    }

    #[test]
    fn bulk_full_rejects() {
        // A saturated bulk read pool rejects with QueueFull instead of running
        // the read inline on the write actor thread.
        let dir = tempdir().unwrap();
        let db = fjall::OptimisticTxDatabase::builder(dir.path())
            .manual_journal_persist(true)
            .open()
            .unwrap();
        let (bulk_sender, _bulk_receiver) = crossfire::mpsc::bounded_blocking(1);
        let mut storage = super::FjallStorage {
            store: super::Store::new(db),
            persist_policy: FjallPersistPolicy::default(),
            txns: std::collections::HashMap::new(),
            transaction_cleanup: std::sync::Arc::new(std::sync::Mutex::new(
                std::collections::BTreeMap::new(),
            )),
            read_pool: Vec::new(),
            next_reader: 0,
            bulk_read_pool: vec![bulk_sender],
            next_bulk_reader: 0,
        };
        let metrics = std::sync::Arc::new(super::StorageMetrics::default());
        let read_effect = || StorageEffect::Read {
            key_space: "node_state".to_string(),
            key: b"missing".to_vec().into(),
            txn_id: None,
        };

        let filler = read_effect();
        let span = super::storage_effect_span(&filler);
        let guard = super::InFlightGuard::acquire(&metrics);
        let (filler_tx, _filler_rx) = crossfire::oneshot::oneshot();
        assert!(
            storage.bulk_read_pool[0]
                .try_send((filler, filler_tx, span, Instant::now(), guard))
                .is_ok(),
            "saturate bulk read pool"
        );

        let target = read_effect();
        let span = super::storage_effect_span(&target);
        let guard = super::InFlightGuard::acquire(&metrics);
        let (target_tx, mut target_rx) = crossfire::oneshot::oneshot();
        let mut slow = super::SlowQueueAggregator::default();
        storage.forward_to_read_pool(
            (target, target_tx, span, Instant::now(), guard),
            StoragePriority::Bulk,
            &mut slow,
        );

        match target_rx.try_recv() {
            Ok(StorageEvent::Error {
                error: StorageError::QueueFull,
            }) => {}
            other => panic!("expected QueueFull rejection, got {other:?}"),
        }
    }

    fn assert_write_result(event: Event, expected_key: &[u8]) {
        match event {
            Event::Storage(StorageEvent::WriteResult { key }) => {
                assert_eq!(key.as_ref(), expected_key);
            }
            other => panic!("unexpected storage event: {other:?}"),
        }
    }

    fn assert_batch_write_result(event: Event, expected: &[(&str, &[u8])]) {
        match event {
            Event::Storage(StorageEvent::BatchWriteResult { entries }) => {
                let actual = entries
                    .iter()
                    .map(|(key_space, key)| (key_space.as_str(), key.as_ref()))
                    .collect::<Vec<_>>();
                assert_eq!(actual, expected);
            }
            other => panic!("unexpected storage event: {other:?}"),
        }
    }

    fn assert_batch_delete_result(event: Event, expected: &[(&str, &[u8])]) {
        match event {
            Event::Storage(StorageEvent::BatchDeleteResult { entries }) => {
                let actual = entries
                    .iter()
                    .map(|(key_space, key)| (key_space.as_str(), key.as_ref()))
                    .collect::<Vec<_>>();
                assert_eq!(actual, expected);
            }
            other => panic!("unexpected storage event: {other:?}"),
        }
    }

    fn assert_read_result(event: Event, expected_key: &[u8], expected_value: &[u8]) {
        match event {
            Event::Storage(StorageEvent::ReadResult {
                key,
                value: Some(value),
            }) => {
                assert_eq!(key.as_ref(), expected_key);
                assert_eq!(value.as_ref(), expected_value);
            }
            other => panic!("unexpected storage event: {other:?}"),
        }
    }

    #[test]
    fn persist_policy_defaults_to_buffer() {
        assert_eq!(FjallPersistPolicy::default(), FjallPersistPolicy::Buffer);
        assert_eq!(FjallPersistPolicy::default().label(), "buffer");
    }

    #[test]
    fn persist_policy_accepts_canonical_values() {
        assert_eq!(
            "sync_all".parse::<FjallPersistPolicy>().unwrap(),
            FjallPersistPolicy::SyncAll
        );
        assert_eq!(
            "buffer".parse::<FjallPersistPolicy>().unwrap(),
            FjallPersistPolicy::Buffer
        );
    }

    #[test]
    fn persist_policy_rejects_invalid_values() {
        assert!("always".parse::<FjallPersistPolicy>().is_err());
        assert!("sync".parse::<FjallPersistPolicy>().is_err());
        assert!("sync-all".parse::<FjallPersistPolicy>().is_err());
        assert!("syncall".parse::<FjallPersistPolicy>().is_err());
        assert!("buffered".parse::<FjallPersistPolicy>().is_err());
    }

    #[tokio::test]
    async fn open_with_persist_policy_accepts_sync_all() {
        let dir = tempdir().expect("temp dir");
        let handle = FjallStorage::open_with_persist_policy(
            dir.path().to_str().expect("utf-8 path"),
            FjallPersistPolicy::SyncAll,
        )
        .expect("storage opens");

        assert_write_result(
            handle
                .send_effect(Effect::Storage(StorageEffect::Write {
                    key_space: "persist_policy".to_string(),
                    key: b"key".to_vec().into(),
                    value: b"value".to_vec().into(),
                    txn_id: None,
                }))
                .await,
            b"key",
        );
    }

    #[tokio::test]
    async fn sync_all_effect_returns_success() {
        let dir = tempdir().unwrap();
        let handle = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();

        let event = handle.send_storage_effect(StorageEffect::SyncAll).await;

        assert!(matches!(
            event,
            Event::Storage(StorageEvent::SyncAllFinished)
        ));
    }

    #[tokio::test]
    async fn cancelled_stays_counted() {
        let (handle, receivers) = StorageHandle::new();
        let receiver = receivers.foreground;
        let mut probe = Box::pin(handle.send_storage_effect(StorageEffect::Read {
            key_space: "node_state".to_string(),
            key: b"node_state".to_vec().into(),
            txn_id: None,
        }));

        // No worker consumes the effect, so the external timeout fires and the
        // probe future is dropped mid-await, as in a readiness probe timeout.
        let timed_out =
            tokio::time::timeout(std::time::Duration::from_millis(50), probe.as_mut()).await;
        assert!(timed_out.is_err());
        assert_eq!(handle.in_flight(), 1);

        drop(probe);
        assert_eq!(handle.in_flight(), 1);

        let queued = receiver.recv().expect("cancelled effect remains queued");
        assert_eq!(handle.in_flight(), 1);
        drop(queued);
        assert_eq!(handle.in_flight(), 0);
    }

    #[tokio::test]
    async fn failed_enqueue_balances() {
        let (handle, receivers) = StorageHandle::new();
        let receiver = receivers.foreground;
        drop(receiver);

        let event = handle
            .send_storage_effect(StorageEffect::Read {
                key_space: "node_state".to_string(),
                key: b"node_state".to_vec().into(),
                txn_id: None,
            })
            .await;

        assert!(matches!(
            event,
            Event::Storage(StorageEvent::Error {
                error: StorageError::ChannelClosed
            })
        ));
        assert_eq!(handle.in_flight(), 0);
    }

    #[test]
    fn worker_exit_latches() {
        let dir = tempdir().expect("temp dir");
        let handle =
            FjallStorage::open(dir.path().to_str().expect("utf-8 path")).expect("storage opens");
        let StorageHandle {
            write_channel,
            bulk_channel,
            priority: _,
            metrics,
            transaction_cleanup: _,
        } = handle;

        assert!(!metrics.channel_closed.load(Ordering::Relaxed));
        drop(write_channel);
        drop(bulk_channel);

        let deadline = Instant::now() + Duration::from_secs(5);
        while !metrics.channel_closed.load(Ordering::Relaxed) && Instant::now() < deadline {
            thread::yield_now();
        }

        assert!(metrics.channel_closed.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn sync_all_handle_surfaces_persist_errors() {
        let (handle, receivers) = StorageHandle::new();
        let receiver = receivers.foreground;
        thread::spawn(move || {
            let (effect, response_tx, _span, _enqueued_at, _in_flight) =
                receiver.recv().expect("sync_all effect should arrive");
            assert!(matches!(effect, StorageEffect::SyncAll));
            response_tx.send(StorageEvent::Error {
                error: StorageError::PersistError("boom".to_string()),
            });
        });

        let error = handle.sync_all().await.expect_err("sync_all should fail");

        assert_eq!(error, StorageError::PersistError("boom".to_string()));
        assert_eq!(handle.get_errors(), 1);
    }

    async fn start_write_transaction(handle: &StorageHandle) -> Ulid {
        match handle
            .send_storage_effect(StorageEffect::StartTransaction { read: false })
            .await
        {
            Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
            other => panic!("unexpected storage event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn owner_aborts_drop() {
        let dir = tempdir().unwrap();
        let handle = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let owner = handle.start_transaction(true).await.unwrap();
        let txn_id = owner.id().unwrap();
        drop(owner);

        assert!(matches!(
            handle
                .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                .await,
            Event::Storage(StorageEvent::Error {
                error: StorageError::TransactionNotFound
            })
        ));
        assert_eq!(handle.pending_transactions(), 0);
    }

    #[tokio::test]
    async fn transaction_cap() {
        let dir = tempdir().unwrap();
        let handle = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let mut owners = Vec::with_capacity(super::MAX_TRANSACTION_CLEANUP);
        for _ in 0..super::MAX_TRANSACTION_CLEANUP {
            let owner = handle.start_transaction(true).await.unwrap();
            owners.push(owner);
        }

        assert!(matches!(
            handle
                .send_storage_effect(StorageEffect::StartTransaction { read: true })
                .await,
            Event::Storage(StorageEvent::Error {
                error: StorageError::TransactionConflict
            })
        ));

        for mut owner in owners {
            let txn_id = owner.id().unwrap();
            match handle
                .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                .await
            {
                Event::Storage(StorageEvent::TransactionAborted { .. }) => owner.finish(),
                other => panic!("unexpected storage event: {other:?}"),
            }
        }
        assert_eq!(handle.pending_transactions(), 0);
        assert!(handle.start_transaction(true).await.is_ok());
    }

    async fn commit_transaction(handle: &StorageHandle, txn_id: Ulid) {
        match handle
            .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
            .await
        {
            Event::Storage(StorageEvent::TransactionCommitted { txn_id: committed }) => {
                assert_eq!(committed, txn_id);
            }
            other => panic!("unexpected storage event: {other:?}"),
        }
    }

    async fn abort_transaction(handle: &StorageHandle, txn_id: Ulid) {
        match handle
            .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
            .await
        {
            Event::Storage(StorageEvent::TransactionAborted { txn_id: aborted }) => {
                assert_eq!(aborted, txn_id);
            }
            other => panic!("unexpected storage event: {other:?}"),
        }
    }

    fn run_buffered_persistence_restart_child(mode: &str, path: &str) {
        let status = Command::new(env::current_exe().expect("test binary path"))
            .arg(RESTART_CHILD_TEST)
            .arg("--exact")
            .arg("--nocapture")
            .env(RESTART_CHILD_PATH_ENV, path)
            .env(RESTART_CHILD_MODE_ENV, mode)
            .status()
            .expect("restart child process should run");

        assert!(status.success(), "restart child process failed: {status}");
    }

    #[test]
    fn buffered_persistence_restart_child_process() {
        let Ok(mode) = env::var(RESTART_CHILD_MODE_ENV) else {
            return;
        };
        let path = env::var(RESTART_CHILD_PATH_ENV).expect("restart child path");
        let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");

        runtime.block_on(async {
            let handle = FjallStorage::open_with_persist_policy(&path, FjallPersistPolicy::Buffer)
                .expect("storage opens in restart child");

            match mode.as_str() {
                "write" => {
                    assert_write_result(
                        handle
                            .send_storage_effect(StorageEffect::Write {
                                key_space: "restart_write".to_string(),
                                key: b"key".to_vec().into(),
                                value: b"value".to_vec().into(),
                                txn_id: None,
                            })
                            .await,
                        b"key",
                    );
                }
                "transaction" => {
                    let txn_id = start_write_transaction(&handle).await;
                    assert_write_result(
                        handle
                            .send_storage_effect(StorageEffect::Write {
                                key_space: "restart_transaction".to_string(),
                                key: b"key".to_vec().into(),
                                value: b"transaction".to_vec().into(),
                                txn_id: Some(txn_id),
                            })
                            .await,
                        b"key",
                    );
                    commit_transaction(&handle, txn_id).await;
                }
                other => panic!("unsupported restart child mode: {other}"),
            }
        });

        // Skip Rust destructors so the parent verifies the restart contract, not shutdown cleanup.
        std::process::exit(0);
    }

    #[tokio::test]
    async fn buffered_write_survives_process_restart() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_str().unwrap();

        run_buffered_persistence_restart_child("write", path);
        let handle = FjallStorage::open_with_persist_policy(path, FjallPersistPolicy::Buffer)
            .expect("storage reopens after restart");

        assert_read_result(
            handle
                .send_storage_effect(StorageEffect::Read {
                    key_space: "restart_write".to_string(),
                    key: b"key".to_vec().into(),
                    txn_id: None,
                })
                .await,
            b"key",
            b"value",
        );
    }

    #[tokio::test]
    async fn buffered_committed_transaction_survives_process_restart() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_str().unwrap();

        run_buffered_persistence_restart_child("transaction", path);
        let handle = FjallStorage::open_with_persist_policy(path, FjallPersistPolicy::Buffer)
            .expect("storage reopens after restart");

        assert_read_result(
            handle
                .send_storage_effect(StorageEffect::Read {
                    key_space: "restart_transaction".to_string(),
                    key: b"key".to_vec().into(),
                    txn_id: None,
                })
                .await,
            b"key",
            b"transaction",
        );
    }

    #[tokio::test]
    async fn non_transactional_raw_write_round_trips() {
        let dir = tempdir().unwrap();
        let handle = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();

        assert_write_result(
            handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: "raw_write".to_string(),
                    key: b"key".to_vec().into(),
                    value: b"value".to_vec().into(),
                    txn_id: None,
                })
                .await,
            b"key",
        );

        assert_read_result(
            handle
                .send_storage_effect(StorageEffect::Read {
                    key_space: "raw_write".to_string(),
                    key: b"key".to_vec().into(),
                    txn_id: None,
                })
                .await,
            b"key",
            b"value",
        );
    }

    #[tokio::test]
    async fn non_transactional_raw_batch_write_round_trips_in_order() {
        let dir = tempdir().unwrap();
        let handle = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();

        assert_batch_write_result(
            handle
                .send_storage_effect(StorageEffect::BatchWrite {
                    writes: vec![
                        (
                            "raw_batch".to_string(),
                            b"a".to_vec().into(),
                            b"1".to_vec().into(),
                        ),
                        (
                            "raw_batch".to_string(),
                            b"b".to_vec().into(),
                            b"2".to_vec().into(),
                        ),
                    ],
                    txn_id: None,
                })
                .await,
            &[("raw_batch", b"a"), ("raw_batch", b"b")],
        );

        assert_read_result(
            handle
                .send_storage_effect(StorageEffect::Read {
                    key_space: "raw_batch".to_string(),
                    key: b"a".to_vec().into(),
                    txn_id: None,
                })
                .await,
            b"a",
            b"1",
        );
        assert_read_result(
            handle
                .send_storage_effect(StorageEffect::Read {
                    key_space: "raw_batch".to_string(),
                    key: b"b".to_vec().into(),
                    txn_id: None,
                })
                .await,
            b"b",
            b"2",
        );
    }

    fn assert_batch_read_result(event: Event, expected: &[(&[u8], Option<&[u8]>)]) {
        match event {
            Event::Storage(StorageEvent::BatchReadResult { values }) => {
                let actual = values
                    .iter()
                    .map(|(key, value)| (key.as_ref(), value.as_ref().map(|v| v.as_ref())))
                    .collect::<Vec<_>>();
                assert_eq!(actual, expected);
            }
            other => panic!("unexpected storage event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn non_transactional_batch_read_returns_values_in_request_order() {
        let dir = tempdir().unwrap();
        let handle = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();

        for (key, value) in [(b"a", b"1"), (b"b", b"2")] {
            assert_write_result(
                handle
                    .send_storage_effect(StorageEffect::Write {
                        key_space: "batch_read".to_string(),
                        key: key.to_vec().into(),
                        value: value.to_vec().into(),
                        txn_id: None,
                    })
                    .await,
                key,
            );
        }

        assert_batch_read_result(
            handle
                .send_storage_effect(StorageEffect::BatchRead {
                    reads: vec![
                        ("batch_read".to_string(), b"b".to_vec().into()),
                        ("batch_read".to_string(), b"missing".to_vec().into()),
                        ("batch_read".to_string(), b"a".to_vec().into()),
                    ],
                    txn_id: None,
                })
                .await,
            &[(b"b", Some(b"2")), (b"missing", None), (b"a", Some(b"1"))],
        );
    }

    #[tokio::test]
    async fn transactional_batch_read_sees_uncommitted_writes() {
        let dir = tempdir().unwrap();
        let handle = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();

        let txn_id = start_write_transaction(&handle).await;
        assert_write_result(
            handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: "batch_read_txn".to_string(),
                    key: b"key".to_vec().into(),
                    value: b"txn".to_vec().into(),
                    txn_id: Some(txn_id),
                })
                .await,
            b"key",
        );

        assert_batch_read_result(
            handle
                .send_storage_effect(StorageEffect::BatchRead {
                    reads: vec![("batch_read_txn".to_string(), b"key".to_vec().into())],
                    txn_id: Some(txn_id),
                })
                .await,
            &[(b"key", Some(b"txn"))],
        );

        assert_batch_read_result(
            handle
                .send_storage_effect(StorageEffect::BatchRead {
                    reads: vec![("batch_read_txn".to_string(), b"key".to_vec().into())],
                    txn_id: None,
                })
                .await,
            &[(b"key", None)],
        );
    }

    #[tokio::test]
    async fn transactional_batch_write_and_batch_delete_commit_atomically() {
        let dir = tempdir().unwrap();
        let handle = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();

        assert_write_result(
            handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: "batch_write_delete_commit".to_string(),
                    key: b"delete".to_vec().into(),
                    value: b"old".to_vec().into(),
                    txn_id: None,
                })
                .await,
            b"delete",
        );

        let txn_id = start_write_transaction(&handle).await;
        assert_batch_write_result(
            handle
                .send_storage_effect(StorageEffect::BatchWrite {
                    writes: vec![(
                        "batch_write_delete_commit".to_string(),
                        b"write".to_vec().into(),
                        b"new".to_vec().into(),
                    )],
                    txn_id: Some(txn_id),
                })
                .await,
            &[("batch_write_delete_commit", b"write")],
        );
        assert_batch_delete_result(
            handle
                .send_storage_effect(StorageEffect::BatchDelete {
                    deletes: vec![(
                        "batch_write_delete_commit".to_string(),
                        b"delete".to_vec().into(),
                    )],
                    txn_id: Some(txn_id),
                })
                .await,
            &[("batch_write_delete_commit", b"delete")],
        );

        assert_batch_read_result(
            handle
                .send_storage_effect(StorageEffect::BatchRead {
                    reads: vec![
                        (
                            "batch_write_delete_commit".to_string(),
                            b"write".to_vec().into(),
                        ),
                        (
                            "batch_write_delete_commit".to_string(),
                            b"delete".to_vec().into(),
                        ),
                    ],
                    txn_id: None,
                })
                .await,
            &[(b"write", None), (b"delete", Some(b"old"))],
        );

        commit_transaction(&handle, txn_id).await;

        assert_batch_read_result(
            handle
                .send_storage_effect(StorageEffect::BatchRead {
                    reads: vec![
                        (
                            "batch_write_delete_commit".to_string(),
                            b"write".to_vec().into(),
                        ),
                        (
                            "batch_write_delete_commit".to_string(),
                            b"delete".to_vec().into(),
                        ),
                    ],
                    txn_id: None,
                })
                .await,
            &[(b"write", Some(b"new")), (b"delete", None)],
        );
    }

    #[tokio::test]
    async fn transactional_batch_write_and_batch_delete_abort_discards_all_changes() {
        let dir = tempdir().unwrap();
        let handle = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();

        assert_write_result(
            handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: "batch_write_delete_abort".to_string(),
                    key: b"delete".to_vec().into(),
                    value: b"old".to_vec().into(),
                    txn_id: None,
                })
                .await,
            b"delete",
        );

        let txn_id = start_write_transaction(&handle).await;
        assert_batch_write_result(
            handle
                .send_storage_effect(StorageEffect::BatchWrite {
                    writes: vec![(
                        "batch_write_delete_abort".to_string(),
                        b"write".to_vec().into(),
                        b"new".to_vec().into(),
                    )],
                    txn_id: Some(txn_id),
                })
                .await,
            &[("batch_write_delete_abort", b"write")],
        );
        assert_batch_delete_result(
            handle
                .send_storage_effect(StorageEffect::BatchDelete {
                    deletes: vec![(
                        "batch_write_delete_abort".to_string(),
                        b"delete".to_vec().into(),
                    )],
                    txn_id: Some(txn_id),
                })
                .await,
            &[("batch_write_delete_abort", b"delete")],
        );

        abort_transaction(&handle, txn_id).await;

        assert_batch_read_result(
            handle
                .send_storage_effect(StorageEffect::BatchRead {
                    reads: vec![
                        (
                            "batch_write_delete_abort".to_string(),
                            b"write".to_vec().into(),
                        ),
                        (
                            "batch_write_delete_abort".to_string(),
                            b"delete".to_vec().into(),
                        ),
                    ],
                    txn_id: None,
                })
                .await,
            &[(b"write", None), (b"delete", Some(b"old"))],
        );
    }

    async fn iter_keys(
        handle: &StorageHandle,
        key_space: &str,
        prefix: Option<&[u8]>,
        start: Option<IterStart>,
    ) -> Vec<Vec<u8>> {
        match handle
            .send_storage_effect(StorageEffect::Iter {
                key_space: key_space.to_string(),
                prefix: prefix.map(|p| p.to_vec().into()),
                start,
                limit: 100,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::IterResult { values, .. }) => {
                values.into_iter().map(|(k, _)| k.to_vec()).collect()
            }
            other => panic!("unexpected storage event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn iter_start_bound_controls_inclusivity() {
        let dir = tempdir().unwrap();
        let handle = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();

        for key in [b"p/a", b"p/b", b"p/c"] {
            assert_write_result(
                handle
                    .send_storage_effect(StorageEffect::Write {
                        key_space: "iter_start".to_string(),
                        key: key.to_vec().into(),
                        value: b"v".to_vec().into(),
                        txn_id: None,
                    })
                    .await,
                key,
            );
        }

        let keys = iter_keys(
            &handle,
            "iter_start",
            None,
            Some(IterStart::After(b"p/b".to_vec().into())),
        )
        .await;
        assert_eq!(keys, vec![b"p/c".to_vec()]);

        let keys = iter_keys(
            &handle,
            "iter_start",
            None,
            Some(IterStart::At(b"p/b".to_vec().into())),
        )
        .await;
        assert_eq!(keys, vec![b"p/b".to_vec(), b"p/c".to_vec()]);

        let keys = iter_keys(
            &handle,
            "iter_start",
            Some(b"p/"),
            Some(IterStart::At(b"a".to_vec().into())),
        )
        .await;
        assert_eq!(
            keys,
            vec![b"p/a".to_vec(), b"p/b".to_vec(), b"p/c".to_vec()]
        );
    }

    #[tokio::test]
    async fn non_transactional_write_works_while_write_transaction_is_active() {
        let dir = tempdir().unwrap();
        let handle = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();

        assert_write_result(
            handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: "raw_conflict".to_string(),
                    key: b"key".to_vec().into(),
                    value: b"before".to_vec().into(),
                    txn_id: None,
                })
                .await,
            b"key",
        );

        let txn_id = start_write_transaction(&handle).await;
        assert_write_result(
            handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: "raw_conflict".to_string(),
                    key: b"txn-key".to_vec().into(),
                    value: b"txn".to_vec().into(),
                    txn_id: Some(txn_id),
                })
                .await,
            b"txn-key",
        );

        assert_write_result(
            handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: "raw_conflict".to_string(),
                    key: b"key".to_vec().into(),
                    value: b"after".to_vec().into(),
                    txn_id: None,
                })
                .await,
            b"key",
        );

        match handle
            .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
            .await
        {
            Event::Storage(StorageEvent::TransactionCommitted { txn_id: committed }) => {
                assert_eq!(committed, txn_id);
            }
            other => panic!("unexpected storage event: {other:?}"),
        }

        assert_read_result(
            handle
                .send_storage_effect(StorageEffect::Read {
                    key_space: "raw_conflict".to_string(),
                    key: b"txn-key".to_vec().into(),
                    txn_id: None,
                })
                .await,
            b"txn-key",
            b"txn",
        );
    }

    #[tokio::test]
    async fn send_storage_effect_counts_requests_and_errors() {
        let dir = tempdir().unwrap();
        let handle = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();

        let event = handle
            .send_storage_effect(StorageEffect::Read {
                key_space: "missing".to_string(),
                key: b"key".to_vec().into(),
                txn_id: Some(Ulid::generate()),
            })
            .await;

        assert!(matches!(event, Event::Storage(StorageEvent::Error { .. })));
        assert_eq!(
            handle.snapshot_metrics(),
            super::StorageMetricsSnapshot {
                requests_total: 1,
                errors_total: 1,
                conflicts_total: 0,
                failed_total: 1,
                channel_closed: false,
                last_error: Some("Transaction not found".to_string()),
            }
        );
    }

    #[tokio::test]
    async fn send_effect_counts_conflicts_separately_from_errors() {
        let (handle, receivers) = StorageHandle::new();
        let receiver = receivers.foreground;
        thread::spawn(move || {
            let (effect, response_tx, _span, _enqueued_at, _in_flight) =
                receiver.recv().expect("first effect should arrive");
            assert!(matches!(effect, StorageEffect::CommitTransaction { .. }));
            response_tx.send(StorageEvent::Error {
                error: StorageError::TransactionNotFound,
            });

            let (effect, response_tx, _span, _enqueued_at, _in_flight) =
                receiver.recv().expect("second effect should arrive");
            assert!(matches!(
                effect,
                StorageEffect::StartTransaction { read: false }
            ));
            response_tx.send(StorageEvent::Error {
                error: StorageError::TransactionConflict,
            });
        });

        let event = handle
            .send_effect(Effect::Storage(StorageEffect::CommitTransaction {
                txn_id: Ulid::generate(),
            }))
            .await;

        assert!(matches!(event, Event::Storage(StorageEvent::Error { .. })));

        let metrics_after_not_found = handle.snapshot_metrics();
        assert_eq!(metrics_after_not_found.requests_total, 1);
        assert_eq!(metrics_after_not_found.errors_total, 1);
        assert_eq!(metrics_after_not_found.conflicts_total, 0);
        assert_eq!(metrics_after_not_found.failed_total, 1);
        assert_eq!(
            metrics_after_not_found.last_error,
            Some("Transaction not found".to_string())
        );

        let event = handle
            .send_storage_effect(StorageEffect::StartTransaction { read: false })
            .await;

        assert!(matches!(
            event,
            Event::Storage(StorageEvent::Error {
                error: StorageError::TransactionConflict,
            })
        ));

        let metrics_after_conflict = handle.snapshot_metrics();
        assert_eq!(metrics_after_conflict.requests_total, 2);
        assert_eq!(metrics_after_conflict.errors_total, 2);
        assert_eq!(metrics_after_conflict.conflicts_total, 1);
        assert_eq!(metrics_after_conflict.failed_total, 2);
        assert_eq!(
            metrics_after_conflict.last_error,
            Some("Transaction conflict".to_string())
        );
    }
}
