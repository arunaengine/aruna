//! Stores immutable repository bindings and exact LFS versions in local transactions.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::fmt::Debug;
use std::marker::PhantomData;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::git::{GitRecord, MAX_RECORDS, git_record_prefix};
use aruna_core::keyspaces::GIT_RECORD_KEYSPACE;
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::placement::fence::WriteFence;
use aruna_core::rows::{done, read_row, value};
use aruna_core::types::{Key, Value};
use serde::{Serialize, de::DeserializeOwned};
use ulid::Ulid;

use super::GitError;

/// A type a stored row decodes into.
pub(super) trait Row: DeserializeOwned + Send + Debug + PartialEq + 'static {}
impl<T: DeserializeOwned + Send + Debug + PartialEq + 'static> Row for T {}

/// Rows written together: keyspace, key and value.
pub(super) type Writes = Vec<(String, Key, Value)>;

fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, GitError> {
    postcard::from_bytes(bytes).map_err(|_| GitError::Unavailable)
}

fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, GitError> {
    postcard::to_allocvec(value).map_err(|_| GitError::Invalid)
}

fn page(space: &str, prefix: &Key, after: Option<Key>, limit: usize) -> IoCall {
    IoCall::Page {
        space: space.to_string(),
        prefix: prefix.clone(),
        after,
        limit,
    }
}

/// Writes one rebuildable row without a disk sync: a sync on the shared storage worker
/// stalls every read of the node while it runs.
pub(super) fn save<T: Serialize>(space: &str, key: Vec<u8>, value: &T) -> SaveRowOperation {
    SaveRowOperation {
        row: encode(value).map(|bytes| (space.to_string(), key.into(), bytes.into())),
    }
}

#[derive(Debug, PartialEq)]
pub(super) struct SaveRowOperation {
    row: Result<(String, Key, Value), GitError>,
}

impl Operation for SaveRowOperation {
    type Output = ();
    type Error = GitError;

    fn first(&mut self) -> Result<Next<()>, GitError> {
        let row = std::mem::replace(&mut self.row, Err(GitError::Unavailable))?;
        Ok(Next::Call(IoCall::Write(vec![row])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, GitError> {
        done(result)?;
        Ok(Next::Done(()))
    }
}

/// One stored row, if it exists.
pub(super) fn load<T: Row>(space: &str, key: Vec<u8>) -> LoadRowOperation<T> {
    LoadRowOperation {
        space: space.to_string(),
        key,
        row: PhantomData,
    }
}

#[derive(Debug, PartialEq)]
pub(super) struct LoadRowOperation<T> {
    space: String,
    key: Vec<u8>,
    row: PhantomData<fn() -> T>,
}

impl<T: Row> Operation for LoadRowOperation<T> {
    type Output = Option<T>;
    type Error = GitError;

    fn first(&mut self) -> Result<Next<Option<T>>, GitError> {
        Ok(Next::Call(read_row(&self.space, self.key.clone())))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<T>>, GitError> {
        let row = value(result)?.map(|bytes| decode(&bytes)).transpose()?;
        Ok(Next::Done(row))
    }
}

/// Runs `inner`, then syncs storage, so its commit is on disk before the output is used.
#[derive(Debug, PartialEq)]
pub(super) struct SyncedOperation<O: Operation> {
    inner: Option<O>,
    output: Option<O::Output>,
}

impl<O: Operation> SyncedOperation<O> {
    fn new(inner: O) -> Self {
        Self {
            inner: Some(inner),
            output: None,
        }
    }
}

impl<O> Operation for SyncedOperation<O>
where
    O: Operation<Error = GitError> + 'static,
    O::Output: PartialEq + 'static,
{
    type Output = O::Output;
    type Error = GitError;

    fn first(&mut self) -> Result<Next<O::Output>, GitError> {
        let inner = self.inner.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Call(IoCall::child(inner)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<O::Output>, GitError> {
        if let Some(output) = self.output.take() {
            done(result)?;
            return Ok(Next::Done(output));
        }
        self.output = Some(result.child::<O::Output, GitError>()??);
        Ok(Next::Call(IoCall::Sync))
    }
}

/// Stores `value` unless the row exists and answers what the row holds, synced to disk.
pub(super) fn insert<T: Row + Serialize>(
    space: &str,
    key: Vec<u8>,
    value: &T,
) -> SyncedOperation<InsertRowOperation<T>> {
    SyncedOperation::new(InsertRowOperation {
        space: space.to_string(),
        key,
        bytes: encode(value),
        kept: None,
        row: PhantomData,
    })
}

#[derive(Debug, PartialEq)]
pub(super) struct InsertRowOperation<T> {
    space: String,
    key: Vec<u8>,
    bytes: Result<Vec<u8>, GitError>,
    kept: Option<Vec<u8>>,
    row: PhantomData<fn() -> T>,
}

impl<T: Row> Operation for InsertRowOperation<T> {
    type Output = T;
    type Error = GitError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<T>, GitError> {
        Ok(Next::Call(read_row(&self.space, self.key.clone())))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<T>, GitError> {
        if let Some(kept) = self.kept.take() {
            done(result)?;
            return Ok(Next::Done(decode(&kept)?));
        }
        if let Some(old) = value(result)? {
            return Ok(Next::Done(decode(&old)?));
        }
        let bytes = std::mem::replace(&mut self.bytes, Err(GitError::Unavailable))?;
        self.kept = Some(bytes.clone());
        let row = (self.space.clone(), self.key.clone().into(), bytes.into());
        Ok(Next::Call(IoCall::Write(vec![row])))
    }
}

/// Every stored Git record of one document, in record id order.
pub(super) fn scan(document_id: Ulid) -> ScanRecordsOperation {
    ScanRecordsOperation {
        prefix: git_record_prefix(document_id),
        records: Vec::new(),
    }
}

#[derive(Debug, PartialEq)]
pub(super) struct ScanRecordsOperation {
    prefix: Key,
    records: Vec<GitRecord>,
}

impl ScanRecordsOperation {
    fn page(&self, after: Option<Key>) -> Next<Vec<GitRecord>> {
        Next::Call(page(GIT_RECORD_KEYSPACE, &self.prefix, after, 256))
    }
}

impl Operation for ScanRecordsOperation {
    type Output = Vec<GitRecord>;
    type Error = GitError;

    fn first(&mut self) -> Result<Next<Vec<GitRecord>>, GitError> {
        Ok(self.page(None))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Vec<GitRecord>>, GitError> {
        let (rows, next) = result.page()?;
        for (_, row) in rows {
            self.records.push(decode(&row)?);
        }
        // Checkpoints stay small, but every record is still read; this bounds memory use.
        if self.records.len() > 1024 * MAX_RECORDS {
            return Err(GitError::Unavailable);
        }
        match next {
            Some(_) => Ok(self.page(next)),
            None => Ok(Next::Done(std::mem::take(&mut self.records))),
        }
    }
}

/// Writes all entries in one transaction and syncs them before answering.
pub(super) fn commit(writes: Writes, fence: &WriteFence) -> SyncedOperation<CommitRowsOperation> {
    SyncedOperation::new(CommitRowsOperation {
        writes: Some(writes),
        fence: fence.clone(),
        writing: false,
    })
}

#[derive(Debug, PartialEq)]
pub(super) struct CommitRowsOperation {
    writes: Option<Writes>,
    fence: WriteFence,
    writing: bool,
}

impl CommitRowsOperation {
    fn write(&mut self) -> Result<Next<()>, GitError> {
        let writes = self.writes.take().ok_or(IoError::Unfinished)?;
        self.writing = true;
        Ok(Next::Call(IoCall::Write(writes)))
    }
}

impl Operation for CommitRowsOperation {
    type Output = ();
    type Error = GitError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    // A holder's close during a handover either refuses this write or conflicts its commit.
    fn first(&mut self) -> Result<Next<()>, GitError> {
        if self.fence.is_empty() {
            return self.write();
        }
        Ok(Next::Call(IoCall::Read(self.fence.reads())))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, GitError> {
        if self.writing {
            done(result)?;
            return Ok(Next::Done(()));
        }
        let keys = self.fence.reads().into_iter().map(|(_, key)| key);
        let rows: Vec<_> = keys.zip(result.values()?).collect();
        if !self.fence.admits(&rows) {
            return Err(GitError::Unavailable);
        }
        self.write()
    }
}

/// Every row in `space` whose key starts with `prefix`, in key order.
pub(super) fn prefixed<T: Row>(space: &str, prefix: Vec<u8>) -> PrefixRowsOperation<T> {
    PrefixRowsOperation {
        space: space.to_string(),
        prefix: prefix.into(),
        rows: Vec::new(),
    }
}

#[derive(Debug, PartialEq)]
pub(super) struct PrefixRowsOperation<T> {
    space: String,
    prefix: Key,
    rows: Vec<(Vec<u8>, T)>,
}

impl<T: Row> Operation for PrefixRowsOperation<T> {
    type Output = Vec<(Vec<u8>, T)>;
    type Error = GitError;

    fn first(&mut self) -> Result<Next<Self::Output>, GitError> {
        Ok(Next::Call(page(&self.space, &self.prefix, None, 1024)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, GitError> {
        let (rows, next) = result.page()?;
        for (key, row) in rows {
            self.rows.push((key.to_vec(), decode(&row)?));
        }
        match next {
            Some(_) => Ok(Next::Call(page(&self.space, &self.prefix, next, 1024))),
            None => Ok(Next::Done(std::mem::take(&mut self.rows))),
        }
    }
}

/// Deletes one row.
pub(super) fn remove(space: &str, key: Vec<u8>) -> RemoveRowOperation {
    RemoveRowOperation {
        row: (space.to_string(), key.into()),
    }
}

#[derive(Debug, PartialEq)]
pub(super) struct RemoveRowOperation {
    row: (String, Key),
}

impl Operation for RemoveRowOperation {
    type Output = ();
    type Error = GitError;

    fn first(&mut self) -> Result<Next<()>, GitError> {
        Ok(Next::Call(IoCall::Delete(vec![self.row.clone()])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, GitError> {
        done(result)?;
        Ok(Next::Done(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
    use aruna_core::structs::placement::record::{PlacementRef, PlacementStrategy};
    use ulid::Ulid;

    #[tokio::test]
    async fn closed_fence_refuses() {
        let directory = tempfile::tempdir().expect("directory");
        let path = directory.path().to_str().expect("path");
        let context = DriverContext {
            storage_handle: aruna_storage::FjallStorage::open(path).expect("storage"),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };
        let realm_id = RealmId::from_bytes([13; 32]);
        let placement = PlacementRef {
            strategy_id: Ulid::from_bytes([7; 16]),
            shard: 3,
        };
        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        config.strategies.push(PlacementStrategy {
            strategy_id: placement.strategy_id,
            name: "default".to_string(),
            replica_count: Some(1),
            distinct_locations: false,
            affinity: Vec::new(),
            shard_count: 16,
        });
        config.snapshot_candidate_map();
        let mut fence = WriteFence::default();
        fence.add(realm_id, &config, [placement]);
        let row = |key: &[u8]| {
            (
                "git_test".to_string(),
                key.to_vec().into(),
                vec![1u8].into(),
            )
        };

        drive(commit(vec![row(b"open")], &fence), &context)
            .await
            .expect("an open fence admits the write");
        // The departing holder reported the bucket drained at this write's generation.
        crate::driver::drive(
            crate::placement::process_transitions::CloseFenceOperation::new(realm_id, placement, 1),
            &crate::driver::DriverContext::storage_only(context.storage_handle.clone()),
        )
        .await
        .expect("close");
        assert!(matches!(
            drive(commit(vec![row(b"late")], &fence), &context).await,
            Err(GitError::Unavailable)
        ));
        let stored = |key: &'static [u8]| drive(load::<u8>("git_test", key.to_vec()), &context);
        assert!(matches!(stored(b"open").await, Ok(Some(1))));
        assert!(matches!(stored(b"late").await, Ok(None)));
    }

    fn child<T, E>(result: Result<T, E>) -> IoResult
    where
        T: Send + Debug + PartialEq + 'static,
        E: Send + Debug + PartialEq + 'static,
    {
        IoResult::Child(aruna_core::calls::ChildOutcome::new(result))
    }

    fn encoded(number: u8) -> Value {
        postcard::to_allocvec(&number).unwrap().into()
    }

    // An existing row wins over the value offered, and nothing is written.
    #[test]
    fn insert_keeps_existing() {
        let SyncedOperation { inner, .. } = insert("space", vec![1], &7u8);
        let mut row = inner.unwrap();
        row.first().unwrap();
        let answer = row.next(IoResult::Values(vec![Some(encoded(9))]));
        assert_eq!(answer, Ok(Next::Done(9)));
    }

    // A new row is written and answered as stored.
    #[test]
    fn insert_writes_missing() {
        let SyncedOperation { inner, .. } = insert("space", vec![1], &7u8);
        let mut row = inner.unwrap();
        row.first().unwrap();
        let answer = row.next(IoResult::Values(vec![None]));
        assert!(matches!(answer, Ok(Next::Call(IoCall::Write(_)))));
        assert_eq!(row.next(IoResult::Done), Ok(Next::Done(7)));
    }

    // The output waits for the storage sync.
    #[test]
    fn synced_waits_sync() {
        let mut synced = commit(Vec::new(), &WriteFence::default());
        synced.first().unwrap();
        let answer = synced.next(child(Ok::<_, GitError>(())));
        assert_eq!(answer, Ok(Next::Call(IoCall::Sync)));
        assert_eq!(synced.next(IoResult::Done), Ok(Next::Done(())));
    }

    // Prefix rows follow the cursor across pages.
    #[test]
    fn prefixed_follows_pages() {
        let mut rows = prefixed::<u8>("space", vec![1]);
        rows.first().unwrap();
        let key = Key::from(vec![1, 2]);
        let first = vec![(key.clone(), encoded(5))];
        let answer = rows.next(IoResult::Page {
            rows: first,
            next: Some(key),
        });
        assert!(matches!(answer, Ok(Next::Call(IoCall::Page { .. }))));
        let answer = rows.next(IoResult::Page {
            rows: Vec::new(),
            next: None,
        });
        assert_eq!(answer, Ok(Next::Done(vec![(vec![1, 2], 5)])));
    }
}
