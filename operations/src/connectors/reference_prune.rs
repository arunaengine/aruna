//! Prunes stale connector reference rows in the background, one bounded page per transaction,
//! for connectors a dependency check queued. The queue row holds the position to continue at.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_VERSIONS_KEYSPACE, CONNECTOR_PRUNE_KEYSPACE, CONNECTOR_REFERENCE_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::rows;
use aruna_core::structs::storage::blob::BlobVersion;
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::types::{Key, KeySpace, Value};
use byteview::ByteView;
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
    rows: VecDeque<(Key, Value)>,
    remaining: bool,
    listed: bool,
}

impl PruneReferencesOperation {
    pub fn new() -> Self {
        Self {
            rows: VecDeque::new(),
            remaining: false,
            listed: false,
        }
    }

    fn next_page(&mut self) -> Next<bool> {
        match self.rows.pop_front() {
            Some((connector, cursor)) => {
                Next::Call(IoCall::child(PrunePageOperation::new(connector, cursor)))
            }
            None => Next::Done(self.remaining),
        }
    }
}

impl Default for PruneReferencesOperation {
    fn default() -> Self {
        Self::new()
    }
}

impl Operation for PruneReferencesOperation {
    type Output = bool;
    type Error = PruneError;

    fn first(&mut self) -> Result<Next<bool>, PruneError> {
        let read = queue_read(PRUNE_CONNECTORS);
        Ok(Next::Call(IoCall::Effect(Box::new(read))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, PruneError> {
        if self.listed {
            self.remaining |= result.child::<bool, PruneError>()??;
            return Ok(self.next_page());
        }
        let event = rows::event(result)?;
        let Event::Storage(StorageEvent::IterResult { values, .. }) = event else {
            let expected = "StorageEvent::IterResult";
            return Err(IoError::Unexpected {
                expected,
                got: event.label(),
            }
            .into());
        };
        self.listed = true;
        self.remaining = values.len() == PRUNE_CONNECTORS;
        self.rows = values.into();
        Ok(self.next_page())
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum PruneError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("connector reference pruning was not scheduled: {0}")]
    Schedule(String),
}

impl From<IoError> for PruneError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.into())
    }
}

/// The call a page prune waits for.
#[derive(Debug, PartialEq)]
enum Stage {
    Rows,
    Versions,
    Queue,
    Delete,
}

/// Prunes one page of one queued connector's stale rows in a transaction. A live reference
/// ends the pruning, since the next check meets it at once. Outputs whether the connector
/// stays queued.
#[derive(Debug, PartialEq)]
struct PrunePageOperation {
    queue_key: Key,
    cursor: Value,
    stage: Stage,
    rows: Vec<Key>,
    next: Option<Key>,
    deletes: Vec<(KeySpace, Key)>,
    queued: bool,
}

impl PrunePageOperation {
    fn new(queue_key: Key, cursor: Value) -> Self {
        Self {
            queue_key,
            cursor,
            stage: Stage::Rows,
            rows: Vec::new(),
            next: None,
            deletes: Vec::new(),
            queued: false,
        }
    }

    fn delete_rows(&mut self) -> Next<bool> {
        self.stage = Stage::Delete;
        Next::Call(IoCall::Delete(std::mem::take(&mut self.deletes)))
    }

    fn queue_row(&self) -> (KeySpace, Key) {
        (CONNECTOR_PRUNE_KEYSPACE.to_string(), self.queue_key.clone())
    }

    /// Stale rows up to the first live reference are deleted; the queue row moves on when more
    /// rows follow and no live reference was met, else it is deleted.
    fn plan_page(&mut self, values: Vec<Option<Value>>) -> Result<Next<bool>, PruneError> {
        let connector_id = <[u8; 16]>::try_from(self.queue_key.as_ref())
            .map(Ulid::from_bytes)
            .map_err(|_| StorageError::InvalidEffect)?;
        let mut live = false;
        for (row, value) in std::mem::take(&mut self.rows).into_iter().zip(values) {
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
                self.stage = Stage::Queue;
                let write = prune_queue_write(connector_id, next);
                Ok(Next::Call(IoCall::Write(vec![write])))
            }
            None => {
                self.deletes.push(self.queue_row());
                Ok(self.delete_rows())
            }
        }
    }
}

impl Operation for PrunePageOperation {
    type Output = bool;
    type Error = PruneError;

    fn first(&mut self) -> Result<Next<bool>, PruneError> {
        let Ok(connector) = <[u8; 16]>::try_from(self.queue_key.as_ref()) else {
            // A malformed queue row names no connector and is only removed.
            self.deletes.push(self.queue_row());
            return Ok(self.delete_rows());
        };
        let cursor = std::mem::take(&mut self.cursor);
        Ok(Next::Call(IoCall::Page {
            space: CONNECTOR_REFERENCE_KEYSPACE.to_string(),
            prefix: ByteView::from(connector.to_vec()),
            after: (!cursor.is_empty()).then_some(cursor),
            limit: PRUNE_PAGE,
        }))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, PruneError> {
        match self.stage {
            Stage::Rows => {
                let (values, next) = rows::page(result)?;
                self.rows = values.into_iter().map(|(row, _)| row).collect();
                self.next = next;
                let reads = (self.rows.iter())
                    .map(|row| {
                        let version_key = row.get(ROW_PREFIX..).unwrap_or_default();
                        (
                            BLOB_VERSIONS_KEYSPACE.to_string(),
                            ByteView::from(version_key.to_vec()),
                        )
                    })
                    .collect();
                self.stage = Stage::Versions;
                Ok(Next::Call(IoCall::Read(reads)))
            }
            Stage::Versions => self.plan_page(rows::all_values(result)?),
            Stage::Queue => {
                rows::done(result)?;
                Ok(self.delete_rows())
            }
            Stage::Delete => {
                rows::done(result)?;
                Ok(Next::Done(self.queued))
            }
        }
    }

    fn transaction(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectors::repository::{connector_reference_key, connector_reference_write};
    use crate::driver::{DriverContext, drive};
    use aruna_core::calls::ChildOutcome;
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
        assert!(operation.transaction());
        let first = operation.first();
        assert!(matches!(
            &first,
            Ok(Next::Call(IoCall::Delete(deletes)))
                if deletes.len() == 1 && deletes[0].0 == CONNECTOR_PRUNE_KEYSPACE
        ));
        assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(false)));
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

    fn page_done(result: Result<bool, PruneError>) -> IoResult {
        IoResult::Child(ChildOutcome::new(result))
    }

    // An empty queue finishes at once with no work left.
    #[test]
    fn empty_queue_finishes() {
        let mut operation = PruneReferencesOperation::new();
        let read = Next::Call(IoCall::Effect(Box::new(queue_read(PRUNE_CONNECTORS))));
        assert_eq!(operation.first(), Ok(read));
        let listed = operation.next(IoResult::Event(Box::new(iter_result(Vec::new()))));
        assert_eq!(listed, Ok(Next::Done(false)));
    }

    // Each queued connector gets its own page, one after another, and a full queue read
    // reports remaining work even when every page empties its connector.
    #[test]
    fn pages_run_ordered() {
        let mut operation = PruneReferencesOperation::new();
        operation.first().unwrap();
        let rows: Vec<_> = (1..=PRUNE_CONNECTORS as u8).map(queued_row).collect();
        let mut next = operation.next(IoResult::Event(Box::new(iter_result(rows))));
        for seed in 1..=PRUNE_CONNECTORS as u8 {
            assert!(
                matches!(next, Ok(Next::Call(IoCall::Child(_)))),
                "page {seed} runs as a child"
            );
            next = operation.next(page_done(Ok(false)));
        }
        assert_eq!(next, Ok(Next::Done(true)));
    }

    // A failed page ends the run with its error.
    #[test]
    fn failures_end_run() {
        let mut operation = PruneReferencesOperation::new();
        operation.first().unwrap();
        let rows = vec![queued_row(1), queued_row(2)];
        operation
            .next(IoResult::Event(Box::new(iter_result(rows))))
            .unwrap();
        let failed = operation.next(page_done(Err(StorageError::Timeout.into())));
        assert_eq!(failed, Err(StorageError::Timeout.into()));
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
