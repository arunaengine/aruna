use std::collections::HashMap;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use async_trait::async_trait;
use byteview::ByteView;
use crossfire::{mpsc, oneshot};
use fjall::{KeyspaceCreateOptions, OptimisticTxDatabase, OptimisticTxKeyspace, Readable};
use tracing::{Span, debug_span, field};
use ulid::Ulid;

use crate::errors::StorageLibError;
pub type EffectHandle = (StorageEffect, oneshot::TxOneshot<StorageEvent>, Span);
pub type EffectSender = crossfire::MTx<mpsc::Array<EffectHandle>>;
pub type EffectReceiver = crossfire::Rx<mpsc::Array<EffectHandle>>;

enum Txn {
    Read(fjall::Snapshot),
    Write(Box<fjall::OptimisticWriteTx>),
}
type PageResult = (Vec<(ByteView, ByteView)>, Option<ByteView>);

struct Store {
    db: OptimisticTxDatabase,
    keyspaces: HashMap<String, OptimisticTxKeyspace>,
}

impl Store {
    fn new(db: OptimisticTxDatabase) -> Self {
        Self {
            db,
            keyspaces: HashMap::new(),
        }
    }

    fn resolve_keyspace(&mut self, name: &str) -> Result<OptimisticTxKeyspace, StorageError> {
        if let Some(ks) = self.keyspaces.get(name) {
            return Ok(ks.clone());
        }

        match self.db.keyspace(name, KeyspaceCreateOptions::default) {
            Ok(ks) => {
                self.keyspaces.insert(name.to_string(), ks.clone());
                Ok(ks)
            }
            Err(_) => Err(StorageError::KeyspaceError),
        }
    }
}

pub struct FjallStorage {
    store: Store,
    txns: HashMap<Ulid, Txn>,
}

#[derive(Debug, Default)]
struct StorageMetrics {
    requests_total: AtomicU64,
    errors_total: AtomicU64,
    conflicts_total: AtomicU64,
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
        let (sender, receiver) = mpsc::bounded_blocking(2048);
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

        let (response_tx, response_rx) = crossfire::oneshot::oneshot();
        let span = storage_effect_span(&effect);
        if self
            .write_channel
            .send((effect, response_tx, span))
            .is_err()
        {
            return self.observe_storage_event(StorageEvent::Error {
                error: StorageError::ChannelClosed,
            });
        }

        match response_rx.await {
            Ok(event) => self.observe_storage_event(event),
            Err(_) => self.observe_storage_event(StorageEvent::Error {
                error: StorageError::ChannelClosed,
            }),
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
        let db = OptimisticTxDatabase::builder(path).open()?;

        let (sender, receiver) = StorageHandle::new();

        thread::spawn(move || {
            let mut storage = FjallStorage {
                store: Store::new(db),
                txns: HashMap::new(),
            };
            storage.receive_loop(receiver);
        });

        Ok(sender)
    }

    #[tracing::instrument(name = "storage.receive_loop", level = "debug", skip(self, receiver))]
    pub fn receive_loop(&mut self, receiver: EffectReceiver) {
        loop {
            match receiver.recv() {
                Ok((effect, response_tx, span)) => {
                    let _guard = span.enter();
                    let event = match effect {
                        StorageEffect::StartTransaction { read } => self.start_transaction(read),
                        StorageEffect::AbortTransaction { txn_id } => {
                            self.abort_transaction(txn_id)
                        }
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
                        StorageEffect::BatchWrite { writes, txn_id } => {
                            self.batch_write(writes, txn_id)
                        }
                        StorageEffect::CommitTransaction { txn_id } => {
                            self.commit_transaction(txn_id)
                        }
                        StorageEffect::Delete {
                            key_space,
                            key,
                            txn_id,
                        } => self.delete(key_space, key, txn_id),
                        StorageEffect::BatchDelete { deletes, txn_id } => {
                            self.batch_delete(deletes, txn_id)
                        }
                        StorageEffect::Iter {
                            key_space,
                            prefix,
                            start_after,
                            limit,
                            txn_id,
                        } => self.iterate(key_space, prefix, start_after, limit, txn_id),
                    };
                    response_tx.send(event);
                }
                Err(_) => {
                    tracing::warn!(
                        "Storage receiver channel closed, shutting down storage thread."
                    );
                    break;
                }
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

        if read {
            let txn = self.store.db.read_tx();
            self.txns.insert(txn_id, Txn::Read(txn));
        } else {
            match self.store.db.write_tx() {
                Ok(txn) => {
                    self.txns.insert(txn_id, Txn::Write(Box::new(txn)));
                }
                Err(_e) => {
                    return StorageEvent::Error {
                        error: StorageError::TransactionConflict,
                    };
                }
            };
        }
        StorageEvent::TransactionStarted { txn_id }
    }

    #[tracing::instrument(name = "storage.abort_transaction", level = "debug", skip(self), fields(txn_id = %txn_id))]
    fn abort_transaction(&mut self, txn_id: Ulid) -> StorageEvent {
        let txn = self.txns.remove(&txn_id);

        match txn {
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
            if let Some(txn) = self.txns.get(&txn_id) {
                match txn {
                    Txn::Read(txn) => match txn.get(keyspace, &key) {
                        Ok(value_opt) => StorageEvent::ReadResult {
                            key,
                            value: value_opt.map(|v| v.into()),
                        },
                        Err(_e) => StorageEvent::Error {
                            error: StorageError::ReadError,
                        },
                    },
                    Txn::Write(txn) => match txn.get(keyspace, &key) {
                        Ok(value_opt) => StorageEvent::ReadResult {
                            key,
                            value: value_opt.map(|v| v.into()),
                        },
                        Err(_e) => StorageEvent::Error {
                            error: StorageError::ReadError,
                        },
                    },
                }
            } else {
                StorageEvent::Error {
                    error: StorageError::TransactionNotFound,
                }
            }
        } else {
            // Non-transactional read
            let snapshot = self.store.db.read_tx();
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
            match keyspace.insert(key.clone(), value) {
                Ok(_) => StorageEvent::WriteResult { key },
                Err(_e) => StorageEvent::Error {
                    error: StorageError::WriteError,
                },
            }
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

        if let Some(txn_id) = txn_id {
            let Some(Txn::Write(txn)) = self.txns.get_mut(&txn_id) else {
                return StorageEvent::Error {
                    error: StorageError::TransactionNotFound,
                };
            };

            for (key_space, key, value) in writes {
                let keyspace = match self.store.resolve_keyspace(&key_space) {
                    Ok(ks) => ks,
                    Err(error) => return StorageEvent::Error { error },
                };
                txn.insert(keyspace, key.clone(), value);
                entries.push((key_space, key));
            }
        } else {
            for (key_space, key, value) in writes {
                let keyspace = match self.store.resolve_keyspace(&key_space) {
                    Ok(ks) => ks,
                    Err(error) => return StorageEvent::Error { error },
                };

                if keyspace.insert(key.clone(), value).is_err() {
                    return StorageEvent::Error {
                        error: StorageError::WriteError,
                    };
                }
                entries.push((key_space, key));
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
                Ok(_) => StorageEvent::TransactionCommitted { txn_id },
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
            match keyspace.remove(key.clone()) {
                Ok(_) => StorageEvent::DeleteResult { key },
                Err(_e) => StorageEvent::Error {
                    error: StorageError::DeleteError,
                },
            }
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

        if let Some(txn_id) = txn_id {
            let Some(Txn::Write(txn)) = self.txns.get_mut(&txn_id) else {
                return StorageEvent::Error {
                    error: StorageError::TransactionNotFound,
                };
            };

            for (key_space, key) in deletes {
                let keyspace = match self.store.resolve_keyspace(&key_space) {
                    Ok(ks) => ks,
                    Err(error) => return StorageEvent::Error { error },
                };
                txn.remove(keyspace, key.clone());
                entries.push((key_space, key));
            }
        } else {
            for (key_space, key) in deletes {
                let keyspace = match self.store.resolve_keyspace(&key_space) {
                    Ok(ks) => ks,
                    Err(error) => return StorageEvent::Error { error },
                };

                if keyspace.remove(key.clone()).is_err() {
                    return StorageEvent::Error {
                        error: StorageError::DeleteError,
                    };
                }
                entries.push((key_space, key));
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
            let snapshot = self.store.db.read_tx();
            iterate_page(
                &snapshot,
                &keyspace,
                prefix.as_ref(),
                start_after.as_ref(),
                limit,
            )
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
        Effect::Automerge(_) => "automerge",
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
            let (effect, response_tx, _span) = receiver.recv().expect("first effect should arrive");
            assert!(matches!(effect, StorageEffect::CommitTransaction { .. }));
            response_tx.send(StorageEvent::Error {
                error: StorageError::TransactionNotFound,
            });

            let (effect, response_tx, _span) =
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
