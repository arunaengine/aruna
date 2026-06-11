use std::collections::HashMap;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::telemetry::{LatencyAggregator, record_stage};
use async_trait::async_trait;
use byteview::ByteView;
use crossfire::{TrySendError, mpsc, oneshot};
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
);
pub type EffectSender = crossfire::MTx<mpsc::Array<EffectHandle>>;
pub type EffectReceiver = crossfire::Rx<mpsc::Array<EffectHandle>>;

const STORAGE_EFFECT_QUEUE_CAPACITY: usize = 65_536;

enum Txn {
    Read(fjall::Snapshot),
    Write(Box<fjall::OptimisticWriteTx>),
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
        Some(key_space) => STORAGE_LATENCY.record_split(
            &format!("{operation}:{key_space}"),
            queue_wait,
            service,
        ),
        None => STORAGE_LATENCY.record_split(operation, queue_wait, service),
    }
}

fn storage_effect_key_space(effect: &StorageEffect) -> Option<&str> {
    match effect {
        StorageEffect::Read { key_space, .. }
        | StorageEffect::Write { key_space, .. }
        | StorageEffect::Delete { key_space, .. }
        | StorageEffect::Iter { key_space, .. } => Some(key_space),
        StorageEffect::BatchWrite { writes, .. } => {
            writes.first().map(|(key_space, _, _)| key_space.as_str())
        }
        StorageEffect::BatchDelete { deletes, .. } => {
            deletes.first().map(|(key_space, _)| key_space.as_str())
        }
        StorageEffect::StartTransaction { .. }
        | StorageEffect::CommitTransaction { .. }
        | StorageEffect::AbortTransaction { .. } => None,
    }
}
const MAX_GROUP_COMMIT: usize = 256;
const READ_POOL_THREADS: usize = 4;

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
    txns: HashMap<Ulid, Txn>,
    read_pool: Vec<EffectSender>,
    next_reader: usize,
}

#[derive(Debug, Default)]
struct StorageMetrics {
    requests_total: AtomicU64,
    errors_total: AtomicU64,
    conflicts_total: AtomicU64,
    in_flight: AtomicU64,
    channel_closed: AtomicBool,
    last_error: Mutex<Option<String>>,
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

#[derive(Clone, Debug)]
pub struct StorageHandle {
    write_channel: EffectSender,
    metrics: Arc<StorageMetrics>,
}

impl StorageHandle {
    pub fn new() -> (Self, EffectReceiver) {
        let (sender, receiver) = mpsc::bounded_blocking(STORAGE_EFFECT_QUEUE_CAPACITY);
        (
            StorageHandle {
                write_channel: sender,
                metrics: Arc::new(StorageMetrics::default()),
            },
            receiver,
        )
    }

    pub fn get_errors(&self) -> u64 {
        self.metrics.errors_total.load(Ordering::Relaxed)
    }

    /// Number of storage effects currently enqueued or being processed.
    pub fn in_flight(&self) -> u64 {
        self.metrics.in_flight.load(Ordering::Relaxed)
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

    #[tracing::instrument(
        name = "storage.handle.send_storage_effect",
        level = "debug",
        skip(self, effect),
        fields(operation = storage_effect_kind(&effect))
    )]
    pub async fn send_storage_effect(&self, effect: StorageEffect) -> Event {
        Event::Storage(self.dispatch_storage_effect(effect).await)
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
        let span = storage_effect_span(&effect);
        match self
            .write_channel
            .try_send((effect, response_tx, span, Instant::now()))
        {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                if let Some(txn_id) = active_txn_id {
                    self.enqueue_abort_transaction(txn_id, "request_queue_full");
                }
                return self.observe_storage_event(StorageEvent::Error {
                    error: StorageError::QueueFull,
                });
            }
            Err(TrySendError::Disconnected(_)) => {
                return self.observe_storage_event(StorageEvent::Error {
                    error: StorageError::ChannelClosed,
                });
            }
        }

        self.metrics.in_flight.fetch_add(1, Ordering::Relaxed);
        let event = match tokio::time::timeout(STORAGE_REQUEST_TIMEOUT, response_rx).await {
            Ok(Ok(event)) => self.observe_storage_event(event),
            Ok(Err(_)) => self.observe_storage_event(StorageEvent::Error {
                error: StorageError::ChannelClosed,
            }),
            Err(error) => {
                if let Some(txn_id) = active_txn_id {
                    self.enqueue_abort_transaction(txn_id, "request_timeout");
                }
                warn!(
                    event = "storage.request.timeout",
                    operation,
                    timeout_ms = STORAGE_REQUEST_TIMEOUT.as_millis() as u64,
                    error = %error,
                    "Timed out waiting for storage response"
                );
                self.observe_storage_event(StorageEvent::Error {
                    error: StorageError::Timeout,
                })
            }
        };
        self.metrics.in_flight.fetch_sub(1, Ordering::Relaxed);
        event
    }

    fn enqueue_abort_transaction(&self, txn_id: Ulid, reason: &'static str) {
        let (response_tx, _response_rx) = crossfire::oneshot::oneshot();
        let effect = StorageEffect::AbortTransaction { txn_id };
        let span = storage_effect_span(&effect);
        match self
            .write_channel
            .try_send((effect, response_tx, span, Instant::now()))
        {
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
        | StorageEffect::Read { txn_id: None, .. }
        | StorageEffect::Write { txn_id: None, .. }
        | StorageEffect::BatchWrite { txn_id: None, .. }
        | StorageEffect::Delete { txn_id: None, .. }
        | StorageEffect::BatchDelete { txn_id: None, .. }
        | StorageEffect::Iter { txn_id: None, .. } => None,
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
        let db = OptimisticTxDatabase::builder(path)
            .manual_journal_persist(true)
            .open()?;

        let (sender, receiver) = StorageHandle::new();
        let store = Store::new(db);
        let read_pool = spawn_read_pool(store.clone(), READ_POOL_THREADS);

        thread::spawn(move || {
            let mut storage = FjallStorage {
                store,
                txns: HashMap::new(),
                read_pool,
                next_reader: 0,
            };
            storage.receive_loop(receiver);
        });

        Ok(sender)
    }

    fn process_effect(&mut self, effect: StorageEffect) -> StorageEvent {
        match effect {
            StorageEffect::StartTransaction { read } => self.start_transaction(read),
            StorageEffect::AbortTransaction { txn_id } => self.abort_transaction(txn_id),
            StorageEffect::Read {
                key_space,
                key,
                txn_id,
            } => self.read(key_space, key, txn_id),
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
                start_after,
                limit,
                txn_id,
            } => self.iterate(key_space, prefix, start_after, limit, txn_id),
        }
    }

    #[tracing::instrument(name = "storage.receive_loop", level = "debug", skip(self, receiver))]
    pub fn receive_loop(&mut self, receiver: EffectReceiver) {
        let mut slow_queue = SlowQueueAggregator::default();
        let mut group: Vec<EffectHandle> = Vec::new();
        loop {
            let Ok(first) = receiver.recv() else {
                tracing::warn!("Storage receiver channel closed, shutting down storage thread.");
                break;
            };
            let mut pending = Vec::with_capacity(8);
            pending.push(first);
            while pending.len() < MAX_GROUP_COMMIT {
                match receiver.try_recv() {
                    Ok(item) => pending.push(item),
                    Err(_) => break,
                }
            }

            for item in pending {
                if is_groupable_write(&item.0) {
                    group.push(item);
                    continue;
                }
                if is_poolable_read(&item.0) {
                    self.forward_to_read_pool(item, &mut slow_queue);
                    continue;
                }
                self.flush_write_group(&mut group, &mut slow_queue);
                self.process_single(item, &mut slow_queue);
            }
            self.flush_write_group(&mut group, &mut slow_queue);
        }
    }

    fn forward_to_read_pool(&mut self, item: EffectHandle, slow_queue: &mut SlowQueueAggregator) {
        let reader = self.next_reader % self.read_pool.len();
        self.next_reader = self.next_reader.wrapping_add(1);
        match self.read_pool[reader].try_send(item) {
            Ok(()) => {}
            Err(TrySendError::Full(item)) | Err(TrySendError::Disconnected(item)) => {
                self.process_single(item, slow_queue);
            }
        }
    }

    fn process_single(&mut self, item: EffectHandle, slow_queue: &mut SlowQueueAggregator) {
        let (effect, response_tx, span, enqueued_at) = item;
        let _guard = span.enter();
        let operation = storage_effect_kind(&effect);
        let key_space = storage_effect_key_space(&effect).map(str::to_string);
        let active_txn_id = active_txn_id_for_effect(&effect);
        let queue_wait = enqueued_at.elapsed();
        span.record("queue_wait_ms", duration_ms(queue_wait));
        let starts_transaction = matches!(effect, StorageEffect::StartTransaction { .. });
        let completes_transaction = matches!(
            effect,
            StorageEffect::CommitTransaction { .. } | StorageEffect::AbortTransaction { .. }
        );

        if response_tx.is_disconnected()
            && !matches!(effect, StorageEffect::AbortTransaction { .. })
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

        let members = std::mem::take(group);
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

        let commit_result = self
            .commit_buffered_write_tx(tx)
            .and_then(|()| self.persist_journal());

        match commit_result {
            Ok(()) => {
                let service_elapsed = service_started.elapsed();
                for ((effect, response_tx, span, enqueued_at), outcome) in prepared {
                    let _guard = span.enter();
                    let queue_wait = enqueued_at.elapsed().saturating_sub(service_elapsed);
                    let event = match outcome {
                        Ok(event) => event,
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
                    if !response_tx.is_disconnected() {
                        response_tx.send(event);
                    }
                }
            }
            Err(_) => {
                // Conflict with a held transaction: retry each member alone so
                // only genuinely conflicting writes fail.
                for (item, _) in prepared {
                    self.process_single(item, slow_queue);
                }
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
                for (key_space, key, value) in writes {
                    let keyspace = self.store.resolve_keyspace(key_space)?;
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
                for (key_space, key) in deletes {
                    let keyspace = self.store.resolve_keyspace(key_space)?;
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

    fn persist_journal(&self) -> Result<(), StorageError> {
        let persist_started = Instant::now();
        self.store
            .db
            .persist(PersistMode::Buffer)
            .map_err(|error| StorageError::PersistError(error.to_string()))?;
        Span::current().record("persist_ms", duration_ms(persist_started.elapsed()));
        Ok(())
    }

    fn buffered_write_tx(&self) -> Result<fjall::OptimisticWriteTx, StorageError> {
        self.store
            .db
            .write_tx()
            .map(|tx| tx.durability(Some(PersistMode::Buffer)))
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
        let txn_id = Ulid::new();

        let txn = if read {
            let txn = self.store.db.read_tx();
            Txn::Read(txn)
        } else {
            match self.store.db.write_tx() {
                Ok(txn) => {
                    let txn = txn.durability(Some(PersistMode::Buffer));
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
                Err(_e) => StorageEvent::Error {
                    error: StorageError::TransactionConflict,
                },
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
        skip(self, prefix, start_after),
        fields(key_space = %key_space, has_prefix = prefix.is_some(), has_cursor = start_after.is_some(), limit, txn_id = ?txn_id)
    )]
    fn iterate(
        &mut self,
        key_space: String,
        prefix: Option<ByteView>,
        start_after: Option<ByteView>,
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
                    iterate_page(txn, &keyspace, prefix.as_ref(), start_after.as_ref(), limit)
                }
                Some(Txn::Write(txn)) => iterate_page(
                    txn.as_ref(),
                    &keyspace,
                    prefix.as_ref(),
                    start_after.as_ref(),
                    limit,
                ),
                None => {
                    return StorageEvent::Error {
                        error: StorageError::TransactionNotFound,
                    };
                }
            }
        } else {
            return store_iterate(&self.store, keyspace, prefix, start_after, limit);
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

fn store_iterate(
    store: &Store,
    keyspace: OptimisticTxKeyspace,
    prefix: Option<ByteView>,
    start_after: Option<ByteView>,
    limit: usize,
) -> StorageEvent {
    let snapshot = store.db.read_tx();
    match iterate_page(
        &snapshot,
        &keyspace,
        prefix.as_ref(),
        start_after.as_ref(),
        limit,
    ) {
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
        StorageEffect::Read { txn_id: None, .. } | StorageEffect::Iter { txn_id: None, .. }
    )
}

fn spawn_read_pool(store: Store, threads: usize) -> Vec<EffectSender> {
    let mut senders = Vec::with_capacity(threads);
    for _ in 0..threads {
        let (sender, receiver) = mpsc::bounded_blocking(STORAGE_EFFECT_QUEUE_CAPACITY);
        let store = store.clone();
        thread::spawn(move || read_pool_loop(store, receiver));
        senders.push(sender);
    }
    senders
}

fn read_pool_loop(store: Store, receiver: EffectReceiver) {
    while let Ok((effect, response_tx, span, enqueued_at)) = receiver.recv() {
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
            StorageEffect::Iter {
                key_space,
                prefix,
                start_after,
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
                        store_iterate(&store, keyspace, prefix, start_after, limit)
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
            start_after,
            limit,
            txn_id,
        } => {
            span.record("key_space", field::display(key_space));
            if let Some(prefix) = prefix {
                span.record("key_len", prefix.as_ref().len() as u64);
            }
            if let Some(start_after) = start_after {
                span.record("cursor_len", start_after.as_ref().len() as u64);
            }
            span.record("limit", *limit as u64);
            if let Some(txn_id) = txn_id {
                span.record("txn_id", field::display(txn_id));
            }
        }
    }
}

fn storage_effect_kind(effect: &StorageEffect) -> &'static str {
    match effect {
        StorageEffect::StartTransaction { .. } => "start_transaction",
        StorageEffect::CommitTransaction { .. } => "commit_transaction",
        StorageEffect::Read { .. } => "read",
        StorageEffect::Write { .. } => "write",
        StorageEffect::BatchWrite { .. } => "batch_write",
        StorageEffect::Delete { .. } => "delete",
        StorageEffect::BatchDelete { .. } => "batch_delete",
        StorageEffect::AbortTransaction { .. } => "abort_transaction",
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
        StorageEvent::WriteResult { .. } => "write_result",
        StorageEvent::BatchWriteResult { .. } => "batch_write_result",
        StorageEvent::DeleteResult { .. } => "delete_result",
        StorageEvent::BatchDeleteResult { .. } => "batch_delete_result",
        StorageEvent::IterResult { .. } => "iter_result",
        StorageEvent::Error { .. } => "error",
    }
}

fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

fn iterate_page<R: Readable>(
    reader: &R,
    keyspace: &OptimisticTxKeyspace,
    prefix: Option<&ByteView>,
    start_after: Option<&ByteView>,
    limit: usize,
) -> Result<PageResult, StorageError> {
    let prefix_bytes = prefix.map(|p| p.as_ref().to_vec());
    let start_after_bytes = start_after.map(|s| s.as_ref().to_vec());

    let iter = match (prefix_bytes.as_ref(), start_after_bytes.as_ref()) {
        (Some(prefix), Some(start_after)) => {
            let start_bound = if start_after < prefix {
                Included(prefix.clone())
            } else {
                Excluded(start_after.clone())
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
        (None, Some(start_after)) => reader.range(
            keyspace,
            (Excluded(start_after.clone()), Unbounded::<Vec<u8>>),
        ),
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

fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper = prefix.to_vec();
    for idx in (0..upper.len()).rev() {
        if upper[idx] != u8::MAX {
            upper[idx] = upper[idx].saturating_add(1);
            upper.truncate(idx + 1);
            return Some(upper);
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::{FjallStorage, StorageHandle};
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::errors::StorageError;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::handle::Handle;
    use std::thread;
    use tempfile::tempdir;
    use ulid::Ulid;

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
                txn_id: Some(Ulid::new()),
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
        let (handle, receiver) = StorageHandle::new();
        thread::spawn(move || {
            let (effect, response_tx, _span, _enqueued_at) =
                receiver.recv().expect("first effect should arrive");
            assert!(matches!(effect, StorageEffect::CommitTransaction { .. }));
            response_tx.send(StorageEvent::Error {
                error: StorageError::TransactionNotFound,
            });

            let (effect, response_tx, _span, _enqueued_at) =
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
                txn_id: Ulid::new(),
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
