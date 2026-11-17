//! Prunes stale connector reference rows in the background, one bounded page per transaction,
//! for connectors a dependency check queued. The queue row holds the position to continue at.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_VERSIONS_KEYSPACE, CONNECTOR_PRUNE_KEYSPACE, CONNECTOR_REFERENCE_KEYSPACE,
};
use aruna_core::operation::{EffectOperation, Operation};
use aruna_core::rows;
use aruna_core::structs::storage::blob::BlobVersion;
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::types::{Effects, Key, KeySpace, TxnId, Value};
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::connectors::repository::references_connector;

/// Reference rows one pruning transaction checks.
const PRUNE_PAGE: usize = 128;
/// Queued connectors one run advances by a page each.
const PRUNE_CONNECTORS: usize = 16;
/// Connector id bytes ahead of the version key in a reference row.
const ROW_PREFIX: usize = 16;

/// Queues pruning of `connector_id`'s rows after `cursor`, the last row a check covered.
pub(crate) fn prune_queue_write(connector_id: Ulid, cursor: Key) -> (KeySpace, Key, Value) {
    (
        CONNECTOR_PRUNE_KEYSPACE.to_string(),
        ByteView::from(connector_id.to_bytes().to_vec()),
        cursor,
    )
}

pub(crate) fn schedule_prune() -> Effect {
    Effect::Task(TaskEffect::ShortenTimer {
        key: TaskKey::PruneConnectorReferences,
        after: Duration::ZERO,
    })
}

/// The queue rows to advance, `limit` at most; the queue is read outside any transaction.
fn queue_read(limit: usize) -> Effect {
    Effect::Storage(StorageEffect::Iter {
        key_space: CONNECTOR_PRUNE_KEYSPACE.to_string(),
        prefix: None,
        start: None,
        limit,
        txn_id: None,
    })
}

/// Re-arms the pruning timer while queued work remains; the queue rows are the durable state.
#[derive(Debug, PartialEq)]
pub struct RestorePruneOperation {
    scheduling: bool,
}

impl RestorePruneOperation {
    pub fn new() -> Self {
        Self { scheduling: false }
    }
}

impl Default for RestorePruneOperation {
    fn default() -> Self {
        Self::new()
    }
}

impl Operation for RestorePruneOperation {
    type Output = ();
    type Error = PruneError;

    fn first(&mut self) -> Result<Next<()>, PruneError> {
        Ok(Next::Call(IoCall::Effect(Box::new(queue_read(1)))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, PruneError> {
        match (self.scheduling, rows::event(result)?) {
            (false, Event::Storage(StorageEvent::IterResult { values, .. })) => {
                if values.is_empty() {
                    return Ok(Next::Done(()));
                }
                self.scheduling = true;
                Ok(Next::Call(IoCall::Effect(Box::new(schedule_prune()))))
            }
            (true, Event::Task(TaskEvent::Error { message, .. })) => {
                Err(PruneError::Schedule(message))
            }
            (true, Event::Task(_)) => Ok(Next::Done(())),
            (_, event) => {
                let expected = "StorageEvent::IterResult or a TaskEvent";
                Err(IoError::Unexpected {
                    expected,
                    got: event.label(),
                }
                .into())
            }
        }
    }
}

/// Advances every queued connector by one page, one page operation after another. Outputs
/// whether queued work remains; the first failed page ends the run with its error.
#[derive(Debug, PartialEq)]
pub struct PruneReferencesOperation {
    state: PruneState,
    rows: VecDeque<(Key, Value)>,
    page: Option<PrunePageOperation>,
    remaining: bool,
    output: Option<Result<bool, PruneError>>,
}

impl PruneReferencesOperation {
    pub fn new() -> Self {
        Self {
            state: PruneState::Init,
            rows: VecDeque::new(),
            page: None,
            remaining: false,
            output: None,
        }
    }

    fn next_page(&mut self) -> Effects {
        match self.rows.pop_front() {
            Some((connector, cursor)) => {
                self.state = PruneState::Page;
                self.page
                    .insert(PrunePageOperation::new(connector, cursor))
                    .start()
            }
            None => {
                self.state = PruneState::Finish;
                self.output = Some(Ok(self.remaining));
                smallvec![]
            }
        }
    }

    fn fail(&mut self, error: PruneError) -> Effects {
        self.state = PruneState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn step_page(&mut self, event: Event) -> Effects {
        let Some(page) = self.page.as_mut() else {
            return self.fail(PruneError::NotFinished);
        };
        let effects = page.step(event);
        if !page.is_complete() {
            return effects;
        }
        match self.page.take().map(PrunePageOperation::finalize) {
            Some(Ok(queued)) => {
                self.remaining |= queued;
                self.next_page()
            }
            Some(Err(error)) => self.fail(error),
            None => self.fail(PruneError::NotFinished),
        }
    }
}

impl Default for PruneReferencesOperation {
    fn default() -> Self {
        Self::new()
    }
}

impl EffectOperation for PruneReferencesOperation {
    type Output = bool;
    type Error = PruneError;

    fn start(&mut self) -> Effects {
        self.state = PruneState::Queue;
        smallvec![queue_read(PRUNE_CONNECTORS)]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (self.state, event) {
            (PruneState::Queue, Event::Storage(StorageEvent::IterResult { values, .. })) => {
                self.remaining = values.len() == PRUNE_CONNECTORS;
                self.rows = values.into();
                self.next_page()
            }
            (PruneState::Queue, Event::Storage(StorageEvent::Error { error })) => {
                self.fail(error.into())
            }
            (PruneState::Page, event) => self.step_page(event),
            (state, event) => self.fail(PruneError::UnexpectedEvent {
                state,
                received: event.label(),
            }),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, PruneState::Finish | PruneState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(PruneError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        match self.page.as_mut() {
            Some(page) => page.abort(),
            None => smallvec![],
        }
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum PruneError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("unexpected event in state {state:?}: {received}")]
    UnexpectedEvent {
        state: PruneState,
        received: &'static str,
    },
    #[error("connector reference pruning did not finish")]
    NotFinished,
    #[error("connector reference pruning was not scheduled: {0}")]
    Schedule(String),
}

impl From<IoError> for PruneError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.into())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PruneState {
    Init,
    Queue,
    Page,
    StartTransaction,
    ReadRows,
    ReadVersions,
    WriteQueue,
    DeleteRows,
    Commit,
    Abort,
    Finish,
    Error,
}

/// Prunes one page of one queued connector's stale rows in a transaction. A live reference
/// ends the pruning, since the next check meets it at once. Outputs whether the connector
/// stays queued.
#[derive(Debug, PartialEq)]
struct PrunePageOperation {
    queue_key: Key,
    cursor: Value,
    state: PruneState,
    txn_id: Option<TxnId>,
    rows: Vec<Key>,
    next: Option<Key>,
    deletes: Vec<(KeySpace, Key)>,
    queued: bool,
    output: Option<Result<bool, PruneError>>,
}

impl PrunePageOperation {
    fn new(queue_key: Key, cursor: Value) -> Self {
        Self {
            queue_key,
            cursor,
            state: PruneState::Init,
            txn_id: None,
            rows: Vec::new(),
            next: None,
            deletes: Vec::new(),
            queued: false,
            output: None,
        }
    }

    fn txn(&self) -> Result<TxnId, PruneError> {
        self.txn_id.ok_or(StorageError::TransactionNotFound.into())
    }

    fn fail(&mut self, error: PruneError) -> Effects {
        self.output = Some(Err(error));
        match self.txn_id.take() {
            Some(txn_id) => {
                self.state = PruneState::Abort;
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            None => {
                self.state = PruneState::Error;
                smallvec![]
            }
        }
    }

    fn delete_rows(&mut self, txn_id: TxnId) -> Effects {
        self.state = PruneState::DeleteRows;
        smallvec![Effect::Storage(StorageEffect::BatchDelete {
            deletes: std::mem::take(&mut self.deletes),
            txn_id: Some(txn_id),
        })]
    }

    fn step_inner(&mut self, event: Event) -> Result<Effects, PruneError> {
        match (self.state, event) {
            (
                PruneState::StartTransaction,
                Event::Storage(StorageEvent::TransactionStarted { txn_id }),
            ) => {
                self.txn_id = Some(txn_id);
                let Ok(connector) = <[u8; 16]>::try_from(self.queue_key.as_ref()) else {
                    // A malformed queue row names no connector and is only removed.
                    let queue_row = (CONNECTOR_PRUNE_KEYSPACE.to_string(), self.queue_key.clone());
                    self.deletes.push(queue_row);
                    return Ok(self.delete_rows(txn_id));
                };
                self.state = PruneState::ReadRows;
                let cursor = std::mem::take(&mut self.cursor);
                Ok(smallvec![Effect::Storage(StorageEffect::Iter {
                    key_space: CONNECTOR_REFERENCE_KEYSPACE.to_string(),
                    prefix: Some(ByteView::from(connector.to_vec())),
                    start: (!cursor.is_empty()).then_some(IterStart::After(cursor)),
                    limit: PRUNE_PAGE,
                    txn_id: Some(txn_id),
                })])
            }
            (
                PruneState::ReadRows,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => {
                self.rows = values.into_iter().map(|(row, _)| row).collect();
                self.next = next_start_after;
                let reads = self
                    .rows
                    .iter()
                    .map(|row| {
                        let version_key = row.get(ROW_PREFIX..).unwrap_or_default();
                        (
                            BLOB_VERSIONS_KEYSPACE.to_string(),
                            ByteView::from(version_key.to_vec()),
                        )
                    })
                    .collect();
                self.state = PruneState::ReadVersions;
                Ok(smallvec![Effect::Storage(StorageEffect::BatchRead {
                    reads,
                    txn_id: Some(self.txn()?),
                })])
            }
            (
                PruneState::ReadVersions,
                Event::Storage(StorageEvent::BatchReadResult { values }),
            ) => self.plan_page(values),
            (PruneState::WriteQueue, Event::Storage(StorageEvent::WriteResult { .. })) => {
                Ok(self.delete_rows(self.txn()?))
            }
            (PruneState::DeleteRows, Event::Storage(StorageEvent::BatchDeleteResult { .. })) => {
                self.state = PruneState::Commit;
                let txn_id = self.txn()?;
                Ok(smallvec![Effect::Storage(
                    StorageEffect::CommitTransaction { txn_id }
                )])
            }
            (PruneState::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.txn_id = None;
                self.state = PruneState::Finish;
                self.output = Some(Ok(self.queued));
                Ok(smallvec![])
            }
            (PruneState::Commit, Event::Storage(StorageEvent::Error { error })) => {
                // A failed commit leaves no transaction to abort.
                self.txn_id = None;
                Err(error.into())
            }
            (PruneState::Abort, Event::Storage(_)) => {
                self.state = PruneState::Error;
                Ok(smallvec![])
            }
            (_, Event::Storage(StorageEvent::Error { error })) => Err(error.into()),
            (state, event) => Err(PruneError::UnexpectedEvent {
                state,
                received: event.label(),
            }),
        }
    }

    /// Stale rows up to the first live reference are deleted; the queue row moves on when more
    /// rows follow and no live reference was met, else it is deleted.
    fn plan_page(&mut self, values: Vec<(Key, Option<Value>)>) -> Result<Effects, PruneError> {
        let txn_id = self.txn()?;
        let connector_id = <[u8; 16]>::try_from(self.queue_key.as_ref())
            .map(Ulid::from_bytes)
            .map_err(|_| StorageError::InvalidEffect)?;
        let mut live = false;
        for (row, (_, value)) in std::mem::take(&mut self.rows).into_iter().zip(values) {
            let version = value
                .map(|value| BlobVersion::from_bytes(value.as_ref()))
                .transpose()?;
            if version.is_some_and(|version| references_connector(&version, connector_id)) {
                live = true;
                break;
            }
            self.deletes
                .push((CONNECTOR_REFERENCE_KEYSPACE.to_string(), row));
        }
        self.queued = !live && self.next.is_some();
        match self.next.take().filter(|_| self.queued) {
            Some(next) => {
                let (key_space, key, value) = prune_queue_write(connector_id, next);
                self.state = PruneState::WriteQueue;
                Ok(smallvec![Effect::Storage(StorageEffect::Write {
                    key_space,
                    key,
                    value,
                    txn_id: Some(txn_id),
                })])
            }
            None => {
                let queue_row = (CONNECTOR_PRUNE_KEYSPACE.to_string(), self.queue_key.clone());
                self.deletes.push(queue_row);
                Ok(self.delete_rows(txn_id))
            }
        }
    }
}

impl EffectOperation for PrunePageOperation {
    type Output = bool;
    type Error = PruneError;

    fn start(&mut self) -> Effects {
        self.state = PruneState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if matches!(self.state, PruneState::Finish | PruneState::Error) {
            return smallvec![];
        }
        match self.step_inner(event) {
            Ok(effects) => effects,
            Err(error) => self.fail(error),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, PruneState::Finish | PruneState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(PruneError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => {
                self.state = PruneState::Abort;
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            None => smallvec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectors::repository::{connector_reference_key, connector_reference_write};
    use crate::driver::{DriverContext, drive};
    use aruna_core::operation_check::{
        check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
    };
    use aruna_core::structs::execution::source_access::SourceMetadata;
    use aruna_core::structs::execution::source_connector::SourceConnectorKind;
    use aruna_core::structs::execution::staging::{
        PortableSourceDescriptor, StagingStrategy, VersionSourceBinding,
    };
    use aruna_core::structs::storage::blob::VersionKey;
    use std::collections::HashMap;
    use std::time::SystemTime;

    async fn write_rows(context: &DriverContext, writes: Vec<(KeySpace, Key, Value)>) {
        let event = context
            .storage_handle
            .send_storage_effect(StorageEffect::BatchWrite {
                writes,
                txn_id: None,
            })
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::BatchWriteResult { .. })
        ));
    }

    async fn count(context: &DriverContext, key_space: &str) -> usize {
        let iter = StorageEffect::Iter {
            key_space: key_space.to_string(),
            prefix: None,
            start: None,
            limit: 1000,
            txn_id: None,
        };
        match context.storage_handle.send_storage_effect(iter).await {
            Event::Storage(StorageEvent::IterResult { values, .. }) => values.len(),
            other => panic!("unexpected iteration event: {other:?}"),
        }
    }

    fn live_reference(connector_id: Ulid) -> BlobVersion {
        let binding = VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: PortableSourceDescriptor {
                kind: SourceConnectorKind::S3,
                public_config: HashMap::new(),
                source_path: "a".to_string(),
                version_selector: None,
                capabilities: Vec::new(),
                origin_node_id: None,
                namespace: None,
                root: None,
            },
            connector_id: Some(connector_id),
            mirrors: Vec::new(),
        };
        let metadata = SourceMetadata {
            content_length: 1,
            content_type: None,
            etag: None,
            last_modified: None,
            source_version: None,
            digests: Default::default(),
        };
        let now = SystemTime::UNIX_EPOCH;
        BlobVersion::reference(binding, metadata, now, Default::default(), now)
    }

    // A queue row that names no connector is removed in its own transaction.
    #[test]
    fn malformed_queue_dropped() {
        let mut operation = PrunePageOperation::new(
            ByteView::from(b"short".to_vec()),
            ByteView::from(Vec::new()),
        );
        operation.start();
        let txn_id = Ulid::from_bytes([1; 16]);

        let effects = operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::BatchDelete { deletes, .. })]
                if deletes.len() == 1 && deletes[0].0 == CONNECTOR_PRUNE_KEYSPACE
        ));
        operation.step(Event::Storage(StorageEvent::BatchDeleteResult {
            entries: Vec::new(),
        }));
        operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id,
        }));
        assert_eq!(operation.finalize(), Ok(false));
    }

    // Queued stale rows are pruned page by page until the queue row is gone; a live reference
    // stays and ends the pruning.
    #[tokio::test]
    async fn background_prunes_stale() {
        let dir = tempfile::tempdir().unwrap();
        let storage = aruna_storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = crate::distribution::test_store::context(&storage);
        let connector_id = Ulid::from_bytes([2; 16]);
        let mut writes: Vec<_> = (0..PRUNE_PAGE + 5)
            .map(|index| {
                let key = VersionKey::new("bucket", format!("gone-{index}"), Ulid::nil());
                (
                    CONNECTOR_REFERENCE_KEYSPACE.to_string(),
                    connector_reference_key(connector_id, &key).unwrap(),
                    ByteView::from(Vec::new()),
                )
            })
            .collect();
        writes.push(prune_queue_write(connector_id, ByteView::from(Vec::new())));
        write_rows(&context, writes).await;

        let mut runs = 0;
        while drive(PruneReferencesOperation::new(), &context)
            .await
            .unwrap()
        {
            runs += 1;
            assert!(runs < 10, "pruning must finish");
        }
        assert_eq!(count(&context, CONNECTOR_REFERENCE_KEYSPACE).await, 0);
        assert_eq!(count(&context, CONNECTOR_PRUNE_KEYSPACE).await, 0);

        let version = live_reference(connector_id);
        let key = VersionKey::new("bucket", "live", Ulid::nil());
        let mut writes = vec![(
            BLOB_VERSIONS_KEYSPACE.to_string(),
            key.to_bytes().unwrap().into(),
            version.to_bytes().unwrap().into(),
        )];
        writes.extend(connector_reference_write(&version, &key).unwrap());
        writes.push(prune_queue_write(connector_id, ByteView::from(Vec::new())));
        write_rows(&context, writes).await;

        assert!(
            !drive(PruneReferencesOperation::new(), &context)
                .await
                .unwrap()
        );
        assert_eq!(count(&context, CONNECTOR_REFERENCE_KEYSPACE).await, 1);
        assert_eq!(count(&context, CONNECTOR_PRUNE_KEYSPACE).await, 0);
    }
    fn queued_row(seed: u8) -> (Key, Value) {
        (ByteView::from(vec![seed; 16]), ByteView::from(Vec::new()))
    }

    fn iter_result(values: Vec<(Key, Value)>) -> Event {
        Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after: None,
        })
    }

    fn storage_error() -> Event {
        Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        })
    }

    fn started(seed: u8) -> Event {
        Event::Storage(StorageEvent::TransactionStarted {
            txn_id: Ulid::from_bytes([seed; 16]),
        })
    }

    fn page_iter(seed: u8) -> Effect {
        Effect::Storage(StorageEffect::Iter {
            key_space: CONNECTOR_REFERENCE_KEYSPACE.to_string(),
            prefix: Some(ByteView::from(vec![seed; 16])),
            start: None,
            limit: PRUNE_PAGE,
            txn_id: Some(Ulid::from_bytes([seed; 16])),
        })
    }

    fn begin() -> Effect {
        Effect::Storage(StorageEffect::StartTransaction { read: false })
    }

    // An empty queue finishes at once with no work left.
    #[test]
    fn empty_queue_finishes() {
        let mut operation = PruneReferencesOperation::new();
        check_trace(
            &mut operation,
            &[queue_read(PRUNE_CONNECTORS)],
            vec![(iter_result(Vec::new()), Vec::new())],
        );
        assert_eq!(operation.finalize(), Ok(false));
    }

    // Each queued connector gets its own page, one after another, and a full queue read
    // reports remaining work even when every page empties its connector.
    #[test]
    fn pages_run_ordered() {
        let mut operation = PruneReferencesOperation::new();
        let rows: Vec<_> = (1..=PRUNE_CONNECTORS as u8).map(queued_row).collect();
        check_trace(
            &mut operation,
            &[queue_read(PRUNE_CONNECTORS)],
            vec![
                (iter_result(rows), vec![begin()]),
                (started(1), vec![page_iter(1)]),
            ],
        );
        let empty = |seed: u8| {
            vec![
                iter_result(Vec::new()),
                Event::Storage(StorageEvent::BatchReadResult { values: Vec::new() }),
                Event::Storage(StorageEvent::BatchDeleteResult {
                    entries: Vec::new(),
                }),
                Event::Storage(StorageEvent::TransactionCommitted {
                    txn_id: Ulid::from_bytes([seed; 16]),
                }),
            ]
        };
        for seed in 1..=PRUNE_CONNECTORS as u8 {
            let mut last = Effects::new();
            for event in empty(seed) {
                last = operation.step(event);
            }
            if seed < PRUNE_CONNECTORS as u8 {
                assert_eq!(last.as_slice(), [begin()], "page {seed} starts the next");
                assert_eq!(
                    operation.step(started(seed + 1)).as_slice(),
                    [page_iter(seed + 1)]
                );
            } else {
                assert!(last.is_empty());
            }
        }
        assert!(operation.is_complete());
        assert_eq!(operation.finalize(), Ok(true));
    }

    // A failed queue read and a failed page both end the run with their error; a failed page
    // first aborts its transaction.
    #[test]
    fn failures_end_run() {
        let mut operation = PruneReferencesOperation::new();
        operation.start();
        assert!(operation.step(storage_error()).is_empty());
        assert_eq!(operation.finalize(), Err(StorageError::Timeout.into()));

        let mut operation = PruneReferencesOperation::new();
        operation.start();
        operation.step(iter_result(vec![queued_row(1)]));
        operation.step(started(1));
        let aborted = operation.step(storage_error());
        assert_eq!(
            aborted.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction {
                txn_id: Ulid::from_bytes([1; 16])
            })]
        );
        assert!(!operation.is_complete());
        let txn_id = Ulid::from_bytes([1; 16]);
        operation.step(Event::Storage(StorageEvent::TransactionAborted { txn_id }));
        assert_eq!(operation.finalize(), Err(StorageError::Timeout.into()));
    }

    // Events outside the expected set are refused, before start and while reading the queue.
    #[test]
    fn rejects_unexpected() {
        let refusals = rejects_events(PruneReferencesOperation::new, sample_events, |_| false);
        assert_eq!(refusals.len(), sample_events().len());
        let refusals = rejects_events(
            || {
                let mut operation = PruneReferencesOperation::new();
                operation.start();
                operation
            },
            sample_events,
            |event| {
                matches!(
                    event,
                    Event::Storage(StorageEvent::IterResult { .. } | StorageEvent::Error { .. })
                )
            },
        );
        assert!(refusals.iter().all(|(_, effects, _)| effects.is_empty()));
    }

    // Abort aborts the running page's transaction and nothing before it.
    #[test]
    fn abort_cleans_page() {
        let mut operation = PruneReferencesOperation::new();
        check_abort(&mut operation, &[]);
        operation.start();
        check_abort(&mut operation, &[]);
        operation.step(iter_result(vec![queued_row(1)]));
        operation.step(started(1));
        let txn_id = Ulid::from_bytes([1; 16]);
        check_abort(
            &mut operation,
            &[Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
        );
    }

    #[test]
    fn rejects_early_finalize() {
        rejects_finalize(PruneReferencesOperation::new());
        let mut operation = PruneReferencesOperation::new();
        operation.start();
        rejects_finalize(operation);
    }

    // The restore schedules pruning only while a queue row exists; a failed timer is reported.
    #[test]
    fn restore_schedules_queued() {
        let event = |event| IoResult::Event(Box::new(event));
        let schedule = || Next::Call(IoCall::Effect(Box::new(schedule_prune())));
        let mut restore = RestorePruneOperation::new();
        let read = Next::Call(IoCall::Effect(Box::new(queue_read(1))));
        assert_eq!(restore.first(), Ok(read));
        let empty = restore.next(event(iter_result(Vec::new())));
        assert_eq!(empty, Ok(Next::Done(())));

        let mut restore = RestorePruneOperation::new();
        restore.first().unwrap();
        let queued = restore.next(event(iter_result(vec![queued_row(1)])));
        assert_eq!(queued, Ok(schedule()));
        let scheduled = Event::Task(TaskEvent::TimerScheduled {
            key: TaskKey::PruneConnectorReferences,
            after: Duration::ZERO,
        });
        assert_eq!(restore.next(event(scheduled)), Ok(Next::Done(())));

        let mut restore = RestorePruneOperation::new();
        restore.first().unwrap();
        restore
            .next(event(iter_result(vec![queued_row(1)])))
            .unwrap();
        let failed = Event::Task(TaskEvent::Error {
            key: None,
            message: "down".to_string(),
        });
        let failed = restore.next(event(failed));
        assert_eq!(failed, Err(PruneError::Schedule("down".to_string())));
    }
}
