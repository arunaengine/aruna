//! Tests the effect loop: nested operations, transactions, retries and hang limits.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::driver::{
    DriverContext, GroupEnd, GroupRun, HangLimits, Instant, MAX_TRACKED_TRANSACTIONS, MemberStop,
    ParentRun, TransactionEffect, TransactionState, TransactionTracker, dispatch_bounded, drive,
    drive_retry, drive_retry_until, drive_until, extend_unblocked, run_all, run_window,
};
use crate::testing::freeze_clock;
use aruna_core::calls::{CallRun, IoCall, IoError, IoResult, Next};
use aruna_core::effects::{
    BlobEffect, Effect, EffectGroup, GroupForm, StagingSourceEffect, StorageEffect,
};
use aruna_core::errors::StorageError;
use aruna_core::events::{
    BlobEvent, Event, GroupEvent, GroupResult, StagingSourceEvent, StorageEvent, SubOperationEvent,
};
use aruna_core::operation::{AfterConflict, HoldScope, Operation, TransactionMode};
use aruna_core::structs::execution::source_access::ResolvedSourceAccess;
use aruna_core::structs::execution::source_connector::SourceConnectorKind;
use aruna_core::structs::storage::blob::{BackendLocation, BackendRef};
use aruna_core::types::TxnId;
use aruna_storage::storage;
use byteview::ByteView;
use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::time::Duration;
use tempfile::tempdir;

/// Writes one row in a write transaction and reads it back inside it.
#[derive(Debug, PartialEq)]
pub struct TestOperation {
    read: bool,
}

impl TestOperation {
    pub fn new() -> Self {
        TestOperation { read: false }
    }
}

impl Operation for TestOperation {
    type Output = ();
    type Error = IoError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<()>, IoError> {
        Ok(Next::Call(IoCall::Write(vec![row(b"key1", b"value1")])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        if self.read {
            assert_eq!(result.value()?, Some(ByteView::from(*b"value1")));
            return Ok(Next::Done(()));
        }
        result.done()?;
        self.read = true;
        let key = ("default".to_string(), ByteView::from(*b"key1"));
        Ok(Next::Call(IoCall::Read(vec![key])))
    }
}

/// Stages a marker in a write transaction and fails when `fail` holds. Its cleanup writes
/// an abort marker outside any transaction.
#[derive(Debug, PartialEq)]
struct MarkerAbortOperation {
    fail: bool,
}

impl Operation for MarkerAbortOperation {
    type Output = ();
    type Error = IoError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<()>, IoError> {
        Ok(Next::Call(IoCall::Write(vec![row(
            b"staged-marker",
            b"staged",
        )])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        result.done()?;
        match self.fail {
            true => Err(refused()),
            false => Ok(Next::Done(())),
        }
    }

    fn abort(&mut self) -> Vec<IoCall> {
        vec![IoCall::Write(vec![row(b"abort-marker", b"ran")])]
    }
}

/// Stages a marker in a write transaction, then waits on a stalled read.
#[derive(Debug, PartialEq)]
struct DeadlineOperation {
    staged: bool,
}

impl Operation for DeadlineOperation {
    type Output = bool;
    type Error = IoError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<bool>, IoError> {
        Ok(Next::Call(IoCall::Write(vec![row(
            b"staged-marker",
            b"staged",
        )])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, IoError> {
        if self.staged {
            return Ok(Next::Done(true));
        }
        result.done()?;
        self.staged = true;
        Ok(Next::Call(parked_read()))
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
    let storage_handle = storage::memory::MemoryStorage::open();
    let context = DriverContext {
        storage_handle,
        net_handle: None,
        blob_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
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
    let storage_handle = storage::memory::MemoryStorage::open();
    let net_handle = aruna_p2p::NetHandle::new(crate::testing::local_net(), storage_handle.clone())
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
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
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
    let (handle, _receivers) = storage::handle::StorageHandle::new();
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
            Effect::Delay(Duration::ZERO),
        ],
        &tracker,
    );
    assert_eq!(queue.len(), 1);
    assert!(matches!(queue.pop_front(), Some(Effect::Delay(_))));
}

#[tokio::test]
async fn drive_commit_safe() {
    let storage_handle = storage::memory::MemoryStorage::open();
    let context = DriverContext {
        storage_handle,
        net_handle: None,
        blob_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    };

    let result = drive(MarkerAbortOperation { fail: false }, &context).await;

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
    let storage_handle = storage::memory::MemoryStorage::open();
    let context = DriverContext {
        storage_handle,
        net_handle: None,
        blob_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    };

    let result = drive(MarkerAbortOperation { fail: true }, &context).await;

    assert!(result.is_err());
    // An operation's own error runs its cleanup calls after the rollback.
    assert!(!marker_absent(&context).await);
    assert_eq!(staged_value(&context).await, None);
    assert!(transaction_reopens(&context).await);
}

#[tokio::test]
async fn deadline_commit_safe() {
    let (_directory, context) = test_context();
    let result = crate::driver::drive_until(
        MarkerAbortOperation { fail: false },
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
    let (_directory, mut context) = test_context();
    let ready = Arc::new(tokio::sync::Notify::new());
    context.storage_handle = stalling_storage(context.storage_handle, ready.clone());
    let task_context = context.clone();
    let task =
        tokio::spawn(
            async move { drive(DeadlineOperation { staged: false }, &task_context).await },
        );

    // The transaction is open while the storage read is stalled.
    // Cancellation must transfer it to storage rather than strand it.
    ready.notified().await;
    task.abort();
    let _ = task.await;

    assert_eq!(staged_value(&context).await, None);
    assert!(transaction_reopens(&context).await);
}

#[tokio::test]
async fn deadline_rollback() {
    let (_directory, mut context) = test_context();
    let ready = Arc::new(tokio::sync::Notify::new());
    context.storage_handle = stalling_storage(context.storage_handle, ready.clone());
    // Paused only now: the handles above wait on real I/O, and auto-advance would fire
    // their setup timeouts on a slow machine.
    tokio::time::pause();
    let _clock = freeze_clock();
    let operation = DeadlineOperation { staged: false };
    // Shorter than STORAGE_REQUEST_TIMEOUT so the advance below can only
    // fire the drive deadline, never a tied storage request timeout.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    let task_context = context.clone();
    let task = tokio::spawn(async move {
        crate::driver::drive_until(operation, &task_context, deadline).await
    });

    ready.notified().await;
    tokio::time::advance(std::time::Duration::from_secs(6)).await;
    assert_eq!(task.await.unwrap(), Err(IoError::Aborted));
    // Real time again: auto-advance would fire the request timeouts of the
    // storage roundtrips below before the worker thread can answer.
    tokio::time::resume();
    assert_eq!(staged_value(&context).await, None);
    assert!(transaction_reopens(&context).await);
}

/// Commits a staged row, then waits after the commit on a spool that never ends; its
/// stream tells `ready` once the adapter reads it.
#[derive(Debug)]
struct CommitDeadline {
    ready: Arc<tokio::sync::Notify>,
}

impl PartialEq for CommitDeadline {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

impl Operation for CommitDeadline {
    type Output = ();
    type Error = IoError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<()>, IoError> {
        Ok(Next::Call(IoCall::Write(vec![row(b"committed", b"kept")])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        result.done()?;
        let wait = IoCall::from(announced_blob(self.ready.clone()));
        Ok(Next::Then {
            output: (),
            calls: vec![wait],
        })
    }
}

#[tokio::test]
async fn deadline_after_commit() {
    // A deadline after the commit leaves the committed outcome: no cleanup runs, and the
    // runner still hands back a finished result. Only the deadline uses virtual time.
    let (_directory, context) = blob_context().await;
    let ready = Arc::new(tokio::sync::Notify::new());
    let operation = CommitDeadline {
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

    assert_eq!(task.await.unwrap(), Ok(()));
    let Event::Storage(StorageEvent::ReadResult { value, .. }) = context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: "default".to_string(),
            key: ByteView::from(*b"committed"),
            txn_id: None,
        })
        .await
    else {
        panic!("unexpected storage event");
    };
    assert_eq!(value, Some(ByteView::from(*b"kept")));
}

/// Runs a failing marker stage as a child and finishes whatever the child answers.
#[derive(Debug, PartialEq)]
struct NestedTransactionOperation;

impl Operation for NestedTransactionOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Result<Next<()>, IoError> {
        Ok(Next::Call(IoCall::child(MarkerAbortOperation {
            fail: true,
        })))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        assert!(result.child::<(), IoError>()?.is_err(), "the child fails");
        Ok(Next::Done(()))
    }
}

/// One row of the test keyspace.
fn row(key: &'static [u8], value: &'static [u8]) -> (String, ByteView, ByteView) {
    (
        "default".to_string(),
        ByteView::from(key),
        ByteView::from(value),
    )
}

/// The error a test operation fails with on purpose.
fn refused() -> IoError {
    IoError::Unexpected {
        expected: "a passing test step",
        got: "a failing one",
    }
}

/// A spool that never ends; it tells `ready` once the blob adapter reads its stream.
fn announced_blob(ready: Arc<tokio::sync::Notify>) -> Effect {
    let blob = futures_util::stream::once(async move {
        ready.notify_one();
        let wait = futures_util::future::pending::<
            Result<bytes::Bytes, aruna_core::stream::StreamError>,
        >();
        wait.await
    });
    Effect::blob(BlobEffect::SpoolHidden {
        namespace: ulid::Ulid::from_bytes([8u8; 16]),
        name: "after-commit".to_string(),
        created_by: aruna_core::user::id::UserId::default(),
        max_bytes: None,
        deadline: None,
        blob: aruna_core::stream::BackendStream::new(blob),
    })
}

type Started = Arc<std::sync::Mutex<Vec<TxnId>>>;

/// A storage that forwards every effect to `real` and keeps the ids of the transactions
/// it saw start.
fn started_ids(real: storage::handle::StorageHandle) -> (storage::handle::StorageHandle, Started) {
    let (handle, receivers) = storage::handle::StorageHandle::new();
    let started = Arc::new(std::sync::Mutex::new(Vec::new()));
    for receiver in [receivers.foreground, receivers.bulk] {
        let (real, started) = (real.clone(), started.clone());
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            while let Ok((effect, response, ..)) = receiver.recv() {
                let Event::Storage(event) = runtime.block_on(real.send_storage_effect(effect))
                else {
                    panic!("storage answers with storage events");
                };
                if let StorageEvent::TransactionStarted { txn_id } = &event {
                    started.lock().unwrap().push(*txn_id);
                }
                response.send(event);
            }
        });
    }
    (handle, started)
}

/// Whether storage no longer knows any of `ids`: each ended by commit or abort.
async fn all_closed(context: &DriverContext, ids: &[TxnId]) -> bool {
    let mut closed = true;
    for txn_id in ids {
        let abort = StorageEffect::AbortTransaction { txn_id: *txn_id };
        closed &= matches!(
            context.storage_handle.send_storage_effect(abort).await,
            Event::Storage(StorageEvent::Error {
                error: StorageError::TransactionNotFound
            })
        );
    }
    closed
}

/// A storage read key that [`stalling_storage`] holds without answering.
const STALL_KEY: &[u8] = b"wait-stall";

fn parked_read() -> IoCall {
    IoCall::Read(vec![("default".to_string(), ByteView::from(STALL_KEY))])
}

/// A storage proxy that stalls reads of [`STALL_KEY`] after notifying `ready`, while
/// forwarding every other storage effect so abort and cleanup paths still run.
fn stalling_storage(
    real: storage::handle::StorageHandle,
    ready: Arc<tokio::sync::Notify>,
) -> storage::handle::StorageHandle {
    let (handle, receivers) = storage::handle::StorageHandle::new();
    for receiver in [receivers.foreground, receivers.bulk] {
        let (real, ready) = (real.clone(), ready.clone());
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let mut parked = Vec::new();
            while let Ok((effect, response, ..)) = receiver.recv() {
                let stalls = matches!(
                    &effect,
                    StorageEffect::BatchRead { reads, .. }
                        if reads.iter().any(|(_, key)| key.as_ref() == STALL_KEY)
                );
                if stalls {
                    parked.push(response);
                    ready.notify_one();
                    continue;
                }
                let Event::Storage(event) = runtime.block_on(real.send_storage_effect(effect))
                else {
                    panic!("storage answers with storage events");
                };
                response.send(event);
            }
        });
    }
    handle
}

/// Stages a row in a write transaction and waits on a stalled read before the commit, or on
/// a spool after it when `commit` holds. Its cleanup sets `aborted` and writes `nested-cleanup`.
#[derive(Debug)]
struct PendingTxn {
    staged: bool,
    commit: bool,
    aborted: Arc<std::sync::atomic::AtomicBool>,
    ready: Arc<tokio::sync::Notify>,
}

impl PartialEq for PendingTxn {
    fn eq(&self, other: &Self) -> bool {
        self.staged == other.staged && self.commit == other.commit
    }
}

impl Operation for PendingTxn {
    type Output = bool;
    type Error = IoError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<bool>, IoError> {
        Ok(Next::Call(IoCall::Write(vec![row(
            b"nested-staged",
            b"staged",
        )])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, IoError> {
        if self.staged {
            return Ok(Next::Done(true));
        }
        result.done()?;
        self.staged = true;
        if self.commit {
            let wait = IoCall::from(announced_blob(self.ready.clone()));
            return Ok(Next::Then {
                output: true,
                calls: vec![wait],
            });
        }
        Ok(Next::Call(parked_read()))
    }

    fn abort(&mut self) -> Vec<IoCall> {
        self.aborted
            .store(true, std::sync::atomic::Ordering::SeqCst);
        vec![IoCall::Write(vec![row(b"nested-cleanup", b"ran")])]
    }
}

/// Runs a `PendingTxn` as a child and finishes whatever the child answers.
#[derive(Debug)]
struct NestedDeadline {
    commit: bool,
    aborted: Arc<std::sync::atomic::AtomicBool>,
    ready: Arc<tokio::sync::Notify>,
}

impl PartialEq for NestedDeadline {
    fn eq(&self, other: &Self) -> bool {
        self.commit == other.commit
    }
}

impl Operation for NestedDeadline {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Result<Next<()>, IoError> {
        let child = PendingTxn {
            staged: false,
            commit: self.commit,
            aborted: self.aborted.clone(),
            ready: self.ready.clone(),
        };
        Ok(Next::Call(IoCall::child(child)))
    }

    fn next(&mut self, _: IoResult) -> Result<Next<()>, IoError> {
        Ok(Next::Done(()))
    }
}

#[tokio::test]
async fn subop_error_cleanup() {
    let (_directory, context) = test_context();
    let result = drive(NestedTransactionOperation, &context).await;

    assert!(result.is_ok());
    // The failing child runs its cleanup calls after its rollback.
    assert!(!marker_absent(&context).await);
    assert_eq!(staged_value(&context).await, None);
    assert!(transaction_reopens(&context).await);
}

/// The row the cleanup of `PendingTxn` writes.
async fn cleanup_row(context: &DriverContext) -> Option<ByteView> {
    let read = StorageEffect::Read {
        key_space: "default".to_string(),
        key: ByteView::from(*b"nested-cleanup"),
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
        other => panic!("unexpected storage event {other:?}"),
    }
}

#[tokio::test(start_paused = true)]
async fn nested_deadline_cleanup() {
    tokio::time::resume();
    let (_directory, mut context) = test_context();
    let ready = Arc::new(tokio::sync::Notify::new());
    let (storage, started) = started_ids(context.storage_handle.clone());
    context.storage_handle = stalling_storage(storage, ready.clone());
    tokio::time::pause();
    let _clock = freeze_clock();
    let aborted = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let operation = NestedDeadline {
        commit: false,
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
    // Real time again: auto-advance would fire the cleanup deadline while the storage
    // worker threads still answer the child's cleanup.
    tokio::time::resume();
    // The child applies the deadline, runs its cleanup, and the parent ends on its answer.
    assert_eq!(task.await.unwrap(), Ok(()));
    assert!(aborted.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(cleanup_row(&context).await, Some(ByteView::from(*b"ran")));

    let ids = started.lock().unwrap().clone();
    assert!(!ids.is_empty());
    assert!(all_closed(&context, &ids).await);
}

/// Runs a `PendingTxn` as the one member of a group and answers the group's results.
#[derive(Debug)]
struct GroupedChild {
    aborted: Arc<std::sync::atomic::AtomicBool>,
    ready: Arc<tokio::sync::Notify>,
}

impl PartialEq for GroupedChild {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

impl Operation for GroupedChild {
    type Output = Vec<GroupResult>;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<Vec<GroupResult>>, IoError> {
        let child = PendingTxn {
            staged: false,
            commit: false,
            aborted: self.aborted.clone(),
            ready: self.ready.clone(),
        };
        let IoCall::Child(child) = IoCall::child(child) else {
            unreachable!("a child call holds its suboperation");
        };
        Ok(Next::Call(IoCall::Group(EffectGroup {
            members: vec![Effect::SubOperation(child)],
            limit: 1,
            form: GroupForm::All { deadline: None },
        })))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Vec<GroupResult>>, IoError> {
        match result {
            IoResult::Group(GroupEvent::Finished { results }) => Ok(Next::Done(results)),
            _ => Err(refused()),
        }
    }
}

// A started group member cut by the run deadline still runs its cleanup to storage.
#[tokio::test(start_paused = true)]
async fn group_child_cleanup() {
    tokio::time::resume();
    let (_directory, mut context) = test_context();
    let ready = Arc::new(tokio::sync::Notify::new());
    context.storage_handle = stalling_storage(context.storage_handle, ready.clone());
    tokio::time::pause();
    let _clock = freeze_clock();
    let aborted = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let operation = GroupedChild {
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
    // Real time again: auto-advance would fire the cleanup deadline while the storage
    // worker threads still answer the member's cleanup.
    tokio::time::resume();
    // The member past the run deadline is unfinished, so the run itself expires.
    assert_eq!(task.await.unwrap(), Err(IoError::Aborted));
    assert!(aborted.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(cleanup_row(&context).await, Some(ByteView::from(*b"ran")));
}

#[tokio::test(start_paused = true)]
async fn nested_commit_survives() {
    tokio::time::resume();
    let (_directory, mut context) = blob_context().await;
    let (storage, started) = started_ids(context.storage_handle.clone());
    context.storage_handle = storage;
    tokio::time::pause();
    let _clock = freeze_clock();
    let aborted = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let ready = Arc::new(tokio::sync::Notify::new());
    let operation = NestedDeadline {
        commit: true,
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
    assert_eq!(cleanup_row(&context).await, None);
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

    let ids = started.lock().unwrap().clone();
    assert!(!ids.is_empty());
    assert!(all_closed(&context, &ids).await);
}

/// Never finishes on its own, so only the deadline can end it. The step cap keeps a
/// failing test from spinning instead of hanging. Its cleanup writes a marker.
#[derive(Debug, PartialEq)]
struct StallingOperation {
    steps: usize,
}

fn stall_read() -> IoCall {
    IoCall::Read(vec![("default".to_string(), ByteView::from(*b"stall"))])
}

impl Operation for StallingOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Result<Next<()>, IoError> {
        Ok(Next::Call(stall_read()))
    }

    fn next(&mut self, _: IoResult) -> Result<Next<()>, IoError> {
        self.steps += 1;
        match self.steps > 1_000 {
            true => Ok(Next::Done(())),
            false => Ok(Next::Call(stall_read())),
        }
    }

    fn abort(&mut self) -> Vec<IoCall> {
        vec![IoCall::Write(vec![row(b"cleanup", b"done")])]
    }
}

#[tokio::test]
async fn deadline_runs_abort() {
    // Racing a timeout against the whole drive would drop the abort path.
    let storage_handle = storage::memory::MemoryStorage::open();
    let context = DriverContext {
        storage_handle,
        net_handle: None,
        blob_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    };

    let result = crate::driver::drive_until(
        StallingOperation { steps: 0 },
        &context,
        tokio::time::Instant::now(),
    )
    .await;

    assert_eq!(
        result,
        Err(IoError::Aborted),
        "the deadline must end the operation"
    );
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
    let storage_handle = storage::memory::MemoryStorage::open();

    let context = DriverContext {
        storage_handle,
        net_handle: None,
        blob_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    };

    let operation = TestOperation::new();
    let result = drive(operation, &context).await;
    assert!(result.is_ok());
}

/// Fails at once; its cleanup writes one key twice, so only first in, first out order
/// leaves the second value.
#[derive(Debug, PartialEq)]
struct EffectOrderOperation;

impl Operation for EffectOrderOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Result<Next<()>, IoError> {
        Err(refused())
    }

    fn next(&mut self, _: IoResult) -> Result<Next<()>, IoError> {
        Err(refused())
    }

    fn abort(&mut self) -> Vec<IoCall> {
        vec![
            IoCall::Write(vec![row(b"order", b"first")]),
            IoCall::Write(vec![row(b"order", b"second")]),
        ]
    }
}

#[tokio::test]
async fn driver_preserves_fifo() {
    let storage_handle = storage::memory::MemoryStorage::open();
    let context = DriverContext {
        storage_handle,
        net_handle: None,
        blob_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    };

    assert!(drive(EffectOrderOperation, &context).await.is_err());
    let Event::Storage(StorageEvent::ReadResult { value, .. }) = context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: "default".to_string(),
            key: ByteView::from(*b"order"),
            txn_id: None,
        })
        .await
    else {
        panic!("unexpected storage event")
    };
    assert_eq!(value, Some(ByteView::from(*b"second")));
}

/// Heads a missing HTTP source and reports whether the staging source adapter answered.
#[derive(Debug, PartialEq)]
struct StagingDispatchOperation;

impl Operation for StagingDispatchOperation {
    type Output = bool;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<bool>, IoError> {
        let head = StagingSourceEffect::Head {
            access: ResolvedSourceAccess::OpenDal {
                kind: SourceConnectorKind::Http,
                config: std::collections::HashMap::from([(
                    "endpoint".to_string(),
                    "https://missing.example.org".to_string(),
                )]),
                path: "file.txt".to_string(),
                version: None,
            },
        };
        Ok(Next::Call(IoCall::staging(head)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, IoError> {
        let answered = matches!(result.staging()?, StagingSourceEvent::Error { .. });
        Ok(Next::Done(answered))
    }
}

#[tokio::test]
async fn driver_dispatches_staging() {
    let temp_dir = tempdir().unwrap();
    let temp_root = temp_dir.path().to_str().unwrap().to_string();
    let blob_root = format!("{temp_root}/blobstore");
    std::fs::create_dir_all(&blob_root).unwrap();
    let storage_handle = storage::memory::MemoryStorage::open();
    let net_handle = aruna_p2p::NetHandle::new(crate::testing::local_net(), storage_handle.clone())
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
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    };

    let observed = drive(StagingDispatchOperation, &context)
        .await
        .expect("staging source effect should be dispatched");
    assert!(observed);
}

/// Runs itself as a child until the depth limit answers, and hands that event up.
#[derive(Debug, PartialEq)]
struct RecursiveSubOperation;

impl Operation for RecursiveSubOperation {
    type Output = Event;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<Event>, IoError> {
        Ok(Next::Call(IoCall::child(RecursiveSubOperation)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Event>, IoError> {
        match result {
            IoResult::Event(event) => Ok(Next::Done(*event)),
            result => Ok(Next::Done(result.child::<Event, IoError>()??)),
        }
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
                let storage_handle = storage::memory::MemoryStorage::open();
                let context = DriverContext {
                    storage_handle,
                    net_handle: None,
                    blob_handle: None,
                    graph_handle: None,
                    task_handle: None,
                    compute_handle: None,
                    outbound_handle: None,
                };

                let event = drive(RecursiveSubOperation, &context)
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
    let (storage_handle, receivers) = storage::handle::StorageHandle::new();
    let context = DriverContext {
        storage_handle,
        net_handle: None,
        blob_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
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
                Effect::Blob(blob) => match *blob {
                    BlobEffect::ReleaseReservation { id } => {
                        (3, Event::blob(BlobEvent::ReservationReleased { id }))
                    }
                    other => panic!("unexpected member {other:?}"),
                },
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
    let (storage_handle, _) = storage::handle::StorageHandle::new();
    DriverContext {
        storage_handle,
        net_handle: None,
        blob_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
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
        Effect::blob(BlobEffect::ReleaseReservation { id }),
    ]);
    let started = Instant::now();
    let results = run_all(&mut run, &mut Vec::new()).await;
    assert_eq!(
        results,
        [
            GroupResult::Unfinished,
            GroupResult::Done(Box::new(Event::blob(BlobEvent::ReservationReleased { id }))),
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
}

impl Operation for WindowRows {
    type Output = Vec<(usize, Option<u8>)>;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<Self::Output>, IoError> {
        Ok(Next::Call(IoCall::Group(EffectGroup {
            members: Vec::new(),
            limit: 2,
            form: GroupForm::Window,
        })))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, IoError> {
        match result {
            IoResult::Group(GroupEvent::Member { index, result }) => {
                self.seen.push((index, read_seconds(&result)));
                match self.rest.pop() {
                    Some(effect) => Ok(Next::Call(IoCall::from(effect))),
                    None => Ok(Next::Wait),
                }
            }
            IoResult::Group(GroupEvent::Closed) => Ok(Next::Done(std::mem::take(&mut self.seen))),
            other => panic!("unexpected result {other:?}"),
        }
    }
}

#[tokio::test(start_paused = true)]
async fn window_refills_slots() {
    let context = bare_context();
    let flight = Arc::new(Flight::default());
    let dispatch = Box::new(slow_dispatch(flight.clone()));
    let mut run = GroupRun::new(dispatch, &context, 2, UNBOUNDED);
    run.add([slow_read(4), slow_read(1)]);
    let mut operation = CallRun::new(WindowRows {
        rest: vec![slow_read(1)],
        seen: Vec::new(),
    });
    // The opening window has no members; the group run above holds them.
    assert_eq!(operation.start().len(), 1);
    let end = run_window(
        &mut ParentRun(&mut operation, None),
        &mut run,
        &mut Vec::new(),
        GroupEnd::Expired,
    )
    .await;
    assert!(matches!(end, GroupEnd::Stepped(effects) if effects.is_empty()));
    // Member 1 frees its slot after one second, so member 2 runs before member 0 ends.
    assert_eq!(
        operation.finalize(),
        Ok(vec![(1, Some(1)), (2, Some(1)), (0, Some(4))])
    );
    assert_eq!(flight.peak.load(SeqCst), 2);
}

/// Emits one group and keeps its results.
#[derive(Debug, PartialEq)]
struct OneGroup {
    members: Vec<Effect>,
}

impl Operation for OneGroup {
    type Output = Vec<GroupResult>;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<Vec<GroupResult>>, IoError> {
        Ok(Next::Call(IoCall::Group(EffectGroup {
            members: std::mem::take(&mut self.members),
            limit: 4,
            form: GroupForm::All { deadline: None },
        })))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Vec<GroupResult>>, IoError> {
        match result {
            IoResult::Group(GroupEvent::Finished { results }) => Ok(Next::Done(results)),
            other => panic!("unexpected result {other:?}"),
        }
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
}

impl std::fmt::Debug for HeldGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HeldGroup")
            .field("backend_id", &self.backend_id)
            .finish_non_exhaustive()
    }
}

impl PartialEq for HeldGroup {
    fn eq(&self, other: &Self) -> bool {
        self.backend_id == other.backend_id
    }
}

impl Operation for HeldGroup {
    type Output = bool;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<bool>, IoError> {
        let location = BackendLocation {
            backend: BackendRef::Group(self.backend_id),
            storage_class: None,
            root: "/tmp".to_string(),
            storage_bucket: "bucket".to_string(),
            backend_path: "blob/held".to_string(),
            ulid: self.backend_id,
            format: Default::default(),
            created_by: aruna_core::user::id::UserId::default(),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 1,
            hashes: Default::default(),
        };
        Ok(Next::Call(IoCall::Group(EffectGroup {
            members: vec![Effect::blob(BlobEffect::Delete { location })],
            limit: 1,
            form: GroupForm::All { deadline: None },
        })))
    }

    fn next(&mut self, _: IoResult) -> Result<Next<bool>, IoError> {
        Ok(Next::Done((self.idle)()))
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
    };
    // Removal needs an idle generation: the member ended, but its parent still holds.
    assert_eq!(drive(operation, &context).await, Ok(false));
    assert!(blob.idle_generation(backend_id).is_some());
}

/// Fails with a conflict until attempt `conflicts`, then answers its attempt; the runner
/// reruns it at most `RETRIES` times.
#[derive(Debug, PartialEq)]
struct Conflicted<const RETRIES: u32> {
    attempt: usize,
    conflicts: usize,
}

impl<const RETRIES: u32> Operation for Conflicted<RETRIES> {
    type Output = usize;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<usize>, IoError> {
        match self.attempt < self.conflicts {
            true => Err(IoError::Conflict),
            false => Ok(Next::Done(self.attempt)),
        }
    }

    fn next(&mut self, _: IoResult) -> Result<Next<usize>, IoError> {
        Err(IoError::Conflict)
    }

    fn conflict_retries() -> u32 {
        RETRIES
    }
}

/// The result of a conflict retry and how many operations it built.
async fn retried<const RETRIES: u32>(
    context: &DriverContext,
    conflicts: usize,
) -> (Result<usize, IoError>, usize) {
    let mut built = 0;
    let build = || {
        built += 1;
        Conflicted::<RETRIES> {
            attempt: built - 1,
            conflicts,
        }
    };
    let result = drive_retry(build, context, &[7u8; 16]).await;
    (result, built)
}

#[tokio::test(start_paused = true)]
async fn retry_reruns_conflict() {
    let context = bare_context();
    let conflict = Err(IoError::Conflict);
    // The hook of the attempt after the last run is asked too, so one more is built.
    assert_eq!(retried::<3>(&context, 2).await, (Ok(2), 3));
    assert_eq!(
        retried::<3>(&context, usize::MAX).await,
        (conflict.clone(), 5)
    );
    assert_eq!(
        retried::<5>(&context, usize::MAX).await,
        (conflict.clone(), 7)
    );
    assert_eq!(retried::<0>(&context, usize::MAX).await, (conflict, 2));
}

/// Loses a conflict until its hook sets `recheck`; finishes on the attempt `finish_at`.
#[derive(Debug, PartialEq)]
struct Rechecking {
    recheck: bool,
    finish_at: Option<u32>,
}

impl Operation for Rechecking {
    type Output = &'static str;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<Self::Output>, IoError> {
        match self.recheck {
            true => Ok(Next::Done("rechecked")),
            false => Err(IoError::Conflict),
        }
    }

    fn next(&mut self, _: IoResult) -> Result<Next<Self::Output>, IoError> {
        Err(IoError::Conflict)
    }

    fn on_conflict(&mut self, attempt: u32) -> AfterConflict<Self::Output> {
        if self.finish_at == Some(attempt) {
            return AfterConflict::Finish("finished");
        }
        self.recheck = attempt >= 2;
        AfterConflict::Rerun
    }
}

#[tokio::test(start_paused = true)]
async fn conflict_hook_decides() {
    let context = bare_context();
    for (finish_at, expected) in [(None, ("rechecked", 3)), (Some(1), ("finished", 2))] {
        let mut built = 0;
        let build = || {
            built += 1;
            Rechecking {
                recheck: false,
                finish_at,
            }
        };
        let result = drive_retry(build, &context, &[7u8; 16]).await;
        assert_eq!((result, built), (Ok(expected.0), expected.1));
    }
}

/// Under a deadline the hook still reruns a lost conflict; a backoff past the deadline
/// ends the retries there.
#[tokio::test(start_paused = true)]
async fn retry_until_deadline() {
    let context = bare_context();
    let mut built = 0;
    let build = || {
        built += 1;
        Rechecking {
            recheck: false,
            finish_at: None,
        }
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let result = drive_retry_until(build, &context, &[7u8; 16], deadline).await;
    assert_eq!((result, built), (Ok("rechecked"), 3));

    let past = tokio::time::Instant::now();
    let build = || Conflicted::<3> {
        attempt: 0,
        conflicts: usize::MAX,
    };
    let result = drive_retry_until(build, &context, &[7u8; 16], past).await;
    assert!(result.is_err());
    assert_eq!(tokio::time::Instant::now(), past);
}

/// Logs its name at start, then waits out `delay`, or spools a stream that ends once
/// `gate` opens; without either it ends at once when `finish` holds and otherwise fails.
#[derive(Debug)]
struct Holding {
    name: &'static str,
    scope: Option<HoldScope>,
    log: Arc<std::sync::Mutex<Vec<&'static str>>>,
    started: Arc<tokio::sync::Notify>,
    gate: Option<tokio::sync::oneshot::Receiver<()>>,
    delay: Option<Duration>,
    finish: bool,
}

impl Holding {
    fn new(name: &'static str, log: &Arc<std::sync::Mutex<Vec<&'static str>>>) -> Self {
        Self {
            name,
            scope: Some(HoldScope::DocumentReconcile),
            log: log.clone(),
            started: Arc::default(),
            gate: None,
            delay: None,
            finish: true,
        }
    }
}

impl PartialEq for Holding {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Operation for Holding {
    type Output = ();
    type Error = IoError;

    fn hold_scope(&self) -> Option<HoldScope> {
        self.scope
    }

    fn first(&mut self) -> Result<Next<()>, IoError> {
        self.log.lock().unwrap().push(self.name);
        self.started.notify_one();
        if let Some(after) = self.delay {
            return Ok(Next::Call(IoCall::Delay(after)));
        }
        let Some(gate) = self.gate.take() else {
            // Waiting outside a window is refused, so a run that does not finish fails.
            return match self.finish {
                true => Ok(Next::Done(())),
                false => Ok(Next::Wait),
            };
        };
        let blob = futures_util::stream::once(async move {
            let _ = gate.await;
            Ok::<_, aruna_core::stream::StreamError>(bytes::Bytes::new())
        });
        Ok(Next::Call(IoCall::blob(BlobEffect::SpoolHidden {
            namespace: ulid::Ulid::from_bytes([5u8; 16]),
            name: self.name.to_string(),
            created_by: aruna_core::user::id::UserId::default(),
            max_bytes: None,
            deadline: None,
            blob: aruna_core::stream::BackendStream::new(blob),
        })))
    }

    fn next(&mut self, _: IoResult) -> Result<Next<()>, IoError> {
        Ok(Next::Done(()))
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
    let mut first = Holding::new("first", &log);
    // Outlasts the hour below; no adapter limit ends a delay, unlike a spool's idle timeout.
    first.delay = Some(Duration::from_secs(7200));
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
    assert!(drive_until(late, &context, Instant::now()).await.is_err());
    // Paused time first runs every ready task, so the second run is parked on the hold
    // before the clock jumps an hour.
    tokio::time::pause();
    tokio::time::sleep(Duration::from_secs(3600)).await;
    assert_eq!(*log.lock().unwrap(), ["first", "other"]);

    // Paused time runs on to the end of the first run's delay.
    assert_eq!(first_task.await.unwrap(), Ok(()));
    assert_eq!(second_task.await.unwrap(), Ok(()));
    tokio::time::resume();
    assert_eq!(*log.lock().unwrap(), ["first", "other", "second"]);
    assert!(reconcile_free(&context));
}

/// Holds the reconcile scope and runs a child that names the same scope.
#[derive(Debug, PartialEq)]
struct HoldingParent {
    child: Option<Holding>,
}

impl Operation for HoldingParent {
    type Output = ();
    type Error = IoError;

    fn hold_scope(&self) -> Option<HoldScope> {
        Some(HoldScope::DocumentReconcile)
    }

    fn first(&mut self) -> Result<Next<()>, IoError> {
        match self.child.take() {
            Some(child) => Ok(Next::Call(IoCall::child(child))),
            None => Ok(Next::Done(())),
        }
    }

    fn next(&mut self, _: IoResult) -> Result<Next<()>, IoError> {
        Ok(Next::Done(()))
    }
}

// A child runs under its parent's hold of the same scope; waiting for it would never end.
#[tokio::test]
async fn child_shares_hold() {
    let (_directory, context) = net_context().await;
    let log = Arc::default();
    let parent = HoldingParent {
        child: Some(Holding::new("child", &log)),
    };
    let deadline = Instant::now() + Duration::from_secs(60);
    assert_eq!(drive_until(parent, &context, deadline).await, Ok(()));
    assert_eq!(*log.lock().unwrap(), ["child"]);
    assert!(reconcile_free(&context));
}

/// Runs a child without a hold of its own and answers whether the child failed.
#[derive(Debug, PartialEq)]
struct WaitingParent {
    child: Option<Holding>,
}

impl Operation for WaitingParent {
    type Output = bool;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<bool>, IoError> {
        let child = self.child.take().ok_or_else(refused)?;
        Ok(Next::Call(IoCall::child(child)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, IoError> {
        Ok(Next::Done(result.child::<(), IoError>()?.is_err()))
    }
}

// A child that waits for a hold another run keeps ends at the deadline, without starting.
#[tokio::test]
async fn child_hold_capped() {
    let (_directory, context) = net_context().await;
    let log = Arc::default();
    let mut holder = Holding::new("holder", &log);
    holder.delay = Some(Duration::from_secs(7200));
    let holder_started = holder.started.clone();
    let task_context = context.clone();
    let holder_task = tokio::spawn(async move { drive(holder, &task_context).await });
    holder_started.notified().await;

    // Paused time runs on to the deadline at once, before the hang cap and the holder.
    tokio::time::pause();
    let parent = WaitingParent {
        child: Some(Holding::new("child", &log)),
    };
    let deadline = Instant::now() + Duration::from_secs(5);
    let run = drive_until(parent, &context, deadline);
    let ended = tokio::time::timeout(Duration::from_secs(60), run).await;
    tokio::time::resume();
    assert_eq!(
        ended.expect("the child stops waiting at the deadline"),
        Ok(true)
    );
    assert_eq!(*log.lock().unwrap(), ["holder"]);
    holder_task.abort();
}

#[test]
fn unused_holds_dropped() {
    // An install scope nobody holds leaves the table once another scope is taken.
    let first = HoldScope::PluginInstall(ulid::Ulid::from_bytes([41u8; 16]));
    let released = std::sync::Arc::downgrade(&super::local_hold(first));
    let other = super::local_hold(HoldScope::PluginInstall(ulid::Ulid::from_bytes([42u8; 16])));
    assert!(released.upgrade().is_none());
    drop(other);
}

#[tokio::test]
async fn hold_released_finally() {
    let (_directory, context) = net_context().await;
    let log = Arc::default();

    // Failure: the run waits outside a window, which its call run refuses.
    let mut aborted = Holding::new("aborted", &log);
    aborted.finish = false;
    assert!(drive(aborted, &context).await.is_err());
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
    assert_eq!(task.await.unwrap(), Err(IoError::Aborted));
    tokio::time::resume();
    assert!(reconcile_free(&context));
}

/// Asks the runner to wake it after `after` and completes on the wake.
#[derive(Debug, PartialEq)]
struct Sleeper {
    after: Duration,
}

impl Operation for Sleeper {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Result<Next<()>, IoError> {
        Ok(Next::Call(IoCall::Delay(self.after)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        match result {
            IoResult::Woke => Ok(Next::Done(())),
            _ => Err(refused()),
        }
    }
}

#[tokio::test(start_paused = true)]
async fn delay_wakes_operation() {
    let context = bare_context();
    let after = Duration::from_secs(30);
    let started = Instant::now();
    let sleeper = Sleeper { after };
    assert_eq!(drive(sleeper, &context).await, Ok(()));
    assert_eq!(started.elapsed(), after);

    // A run deadline before the wake cuts the delay off like any other effect.
    let started = Instant::now();
    let deadline = started + Duration::from_secs(10);
    let sleeper = Sleeper { after };
    assert_eq!(
        drive_until(sleeper, &context, deadline).await,
        Err(IoError::Aborted)
    );
    assert_eq!(started.elapsed(), Duration::from_secs(10));
}

/// Waits one second and reports whether the runner stamped a time and seeded it.
#[derive(Debug, Default, PartialEq)]
struct Echo {
    stamped: Option<u64>,
    seeded: bool,
}

impl Operation for Echo {
    type Output = bool;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<bool>, IoError> {
        Ok(Next::Call(IoCall::Delay(Duration::from_secs(1))))
    }

    fn next(&mut self, _: IoResult) -> Result<Next<bool>, IoError> {
        Ok(Next::Done(self.stamped.is_some() && self.seeded))
    }

    fn stamp(&mut self, now_ms: u64) {
        self.stamped = Some(now_ms);
    }

    fn seed(&mut self, _entropy: u128) {
        self.seeded = true;
    }
}

/// Mints one id from the runner's stamp and entropy and ends with it.
#[derive(Debug, Default, PartialEq)]
struct Minter {
    now_ms: u64,
    ids: aruna_core::id_mint::IdMint,
}

impl Operation for Minter {
    type Output = ulid::Ulid;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<ulid::Ulid>, IoError> {
        Ok(Next::Done(self.ids.mint(self.now_ms)))
    }

    fn next(&mut self, _: IoResult) -> Result<Next<ulid::Ulid>, IoError> {
        Err(IoError::Unfinished)
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn seed(&mut self, entropy: u128) {
        self.ids = aruna_core::id_mint::IdMint::new(entropy);
    }
}

/// The recorder is global, so its tests take turns.
static RECORDING: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test]
async fn recorded_ids_reproduce() {
    use crate::recorder::{Entry, RecordLimits};
    let _turn = RECORDING.lock().await;
    let limits = RecordLimits {
        total_bytes: 1 << 20,
        entry_bytes: 4096,
    };
    crate::recorder::enable(limits);
    let minted = drive(Minter::default(), &bare_context()).await.unwrap();
    crate::recorder::disable();
    // Other tests may record at the same time, so only this run's header counts.
    let header = crate::recorder::take()
        .records
        .into_iter()
        .find_map(|record| match record.entry {
            Entry::Started {
                operation, entropy, ..
            } if operation.contains("::Minter>") => Some((record.at_ms, entropy)),
            _ => None,
        });
    let (at_ms, entropy) = header.expect("the run was recorded");
    let again = aruna_core::id_mint::IdMint::new(entropy).mint(at_ms);
    assert_eq!(again, minted, "the header gives the run's ids again");
}

#[tokio::test(start_paused = true)]
async fn recording_keeps_order() {
    use crate::recorder::{Entry, RecordLimits};
    let _turn = RECORDING.lock().await;
    let limits = RecordLimits {
        total_bytes: 1 << 20,
        entry_bytes: 4096,
    };
    crate::recorder::enable(limits);
    let result = drive(Echo::default(), &bare_context()).await;
    crate::recorder::disable();
    assert_eq!(
        result,
        Ok(true),
        "the runner stamps the time and the entropy on the operation"
    );
    // Other tests may record at the same time, so only this run's records count.
    let records = crate::recorder::take().records;
    let run = records
        .iter()
        .find_map(|record| match &record.entry {
            Entry::Started { operation, .. } if operation.contains("::Echo>") => Some(record.run),
            _ => None,
        })
        .expect("the run was recorded");
    let entries: Vec<&Entry> = records
        .iter()
        .filter(|record| record.run == run)
        .map(|record| &record.entry)
        .collect();
    assert!(matches!(
        entries.as_slice(),
        [
            Entry::Started { .. },
            Entry::Event {
                label: "Event::Delayed",
                ..
            },
            Entry::Ended { .. },
        ]
    ));
}

#[tokio::test(start_paused = true)]
async fn hang_limit_answers() {
    // A storage handle without a worker never answers; its own request timeout is longer.
    let (storage_handle, _receivers) = storage::handle::StorageHandle::new();
    let context = DriverContext {
        storage_handle,
        net_handle: None,
        blob_handle: None,
        task_handle: None,
        compute_handle: None,
        graph_handle: None,
        outbound_handle: None,
    };
    let limits = HangLimits {
        storage: Some(Duration::from_secs(1)),
        ..HangLimits::DEFAULT
    };
    let read = Effect::Storage(StorageEffect::Read {
        key_space: "default".to_string(),
        key: ByteView::from(*b"hung"),
        txn_id: None,
    });
    let started = Instant::now();
    let event = dispatch_bounded(read, &context, 0, None, limits).await;
    assert_eq!(event, Event::Hung { effect: "storage" });
    assert_eq!(started.elapsed(), Duration::from_secs(1));
}
