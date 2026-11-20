//! Reads and writes the device replica rows and measures the replica cache.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::{DEVICE_REPLICA_KEYSPACE, NODE_STATE_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::rows::read_row;
use aruna_core::rows::{outside_answer, outside_call};
use tracing::warn;
use ulid::Ulid;

use super::replica::{
    CacheUsageOperation, DEFAULT_CACHE_BYTES, ListReplicasOperation, ReplicaRecord, replica_entry,
    replica_key,
};

/// The row of `NODE_STATE_KEYSPACE` that holds the owner's cache budget in bytes.
const CACHE_BUDGET_KEY: &[u8] = b"device_cache_budget";

/// One replica, or `None` for both an absent row and an unreadable one: every caller treats an
/// unreadable row as a document this device does not hold.
#[derive(Debug, PartialEq)]
pub struct ReadReplicaOperation {
    document_id: Ulid,
}

/// The read of one replica; see [`ReadReplicaOperation`].
pub fn read_replica(document_id: Ulid) -> ReadReplicaOperation {
    ReadReplicaOperation { document_id }
}

impl Operation for ReadReplicaOperation {
    type Output = Option<ReplicaRecord>;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<Option<ReplicaRecord>>, IoError> {
        let read = read_row(
            DEVICE_REPLICA_KEYSPACE,
            replica_key(self.document_id).to_vec(),
        );
        Ok(Next::Call(outside_call(read)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<ReplicaRecord>>, IoError> {
        let stored = outside_answer(result).and_then(|read| read.value());
        let replica =
            (stored.ok().flatten()).and_then(|value| ReplicaRecord::from_bytes(&value).ok());
        Ok(Next::Done(replica))
    }
}

/// Writes one replica row.
#[derive(Debug, PartialEq)]
pub struct StoreReplicaOperation {
    replica: ReplicaRecord,
}

/// The write of one replica; see [`StoreReplicaOperation`].
pub fn store_replica(replica: &ReplicaRecord) -> StoreReplicaOperation {
    StoreReplicaOperation {
        replica: replica.clone(),
    }
}

impl Operation for StoreReplicaOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Result<Next<()>, IoError> {
        let row = replica_entry(&self.replica).map_err(|error| {
            let document_id = self.replica.document_id;
            warn!(%document_id, "Failed to encode a device replica");
            IoError::Codec(error.to_string())
        })?;
        Ok(Next::Call(IoCall::Write(vec![row])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        result.done()?;
        Ok(Next::Done(()))
    }
}

/// Deletes one replica row.
#[derive(Debug, PartialEq)]
pub struct DeleteReplicaOperation {
    document_id: Ulid,
}

/// The removal of one replica; see [`DeleteReplicaOperation`].
pub fn delete_replica(document_id: Ulid) -> DeleteReplicaOperation {
    DeleteReplicaOperation { document_id }
}

impl Operation for DeleteReplicaOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Result<Next<()>, IoError> {
        let row = (
            DEVICE_REPLICA_KEYSPACE.to_string(),
            replica_key(self.document_id),
        );
        Ok(Next::Call(IoCall::Delete(vec![row])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        result.done()?;
        Ok(Next::Done(()))
    }
}

/// The bytes this device's replicas and their installed graphs take. A graph whose replica
/// row is gone still counts: nothing removes it locally yet.
pub fn cache_usage() -> CacheUsageOperation {
    CacheUsageOperation::new()
}

/// The owner's cache budget; a node that never stored one keeps the default.
pub fn cache_budget() -> CacheBudgetOperation {
    CacheBudgetOperation
}

#[derive(Debug, PartialEq)]
pub struct CacheBudgetOperation;

impl Operation for CacheBudgetOperation {
    type Output = u64;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<u64>, IoError> {
        Ok(Next::Call(read_row(
            NODE_STATE_KEYSPACE,
            CACHE_BUDGET_KEY.to_vec(),
        )))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<u64>, IoError> {
        let stored = result
            .value()?
            .and_then(|bytes| <[u8; 8]>::try_from(bytes.as_ref()).ok());
        Ok(Next::Done(
            stored.map_or(DEFAULT_CACHE_BYTES, u64::from_be_bytes),
        ))
    }
}

/// Stores the owner's cache budget; startup writes it each time a device starts.
pub fn store_cache_budget(bytes: u64) -> StoreBudgetOperation {
    StoreBudgetOperation(bytes)
}

#[derive(Debug, PartialEq)]
pub struct StoreBudgetOperation(u64);

impl Operation for StoreBudgetOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Result<Next<()>, IoError> {
        let row = (
            NODE_STATE_KEYSPACE.to_string(),
            CACHE_BUDGET_KEY.to_vec().into(),
            self.0.to_be_bytes().to_vec().into(),
        );
        Ok(Next::Call(IoCall::Write(vec![row])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        result.done()?;
        Ok(Next::Done(()))
    }
}

/// Every replica this device holds, in document order.
pub fn list_replicas() -> ListReplicasOperation {
    ListReplicasOperation::new()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::device::replica::{ReplicaOrigin, replica_key, replica_size};
    use aruna_core::calls::ChildOutcome;
    use aruna_core::effects::StorageEffect;
    use aruna_core::keyspaces::{DEVICE_GRAPH_KEYSPACE, DEVICE_REPLICA_KEYSPACE};
    use aruna_drivers::driver::drive;

    // A graph stays counted after its replica row goes: nothing removes it locally yet.
    #[tokio::test]
    async fn counts_installed_graphs() {
        let (_dir, context) = crate::tests::device::context().await;
        let context = Arc::new(context);
        let mut held = ReplicaRecord::new(
            Ulid::from_bytes([2u8; 16]),
            Ulid::from_bytes([1u8; 16]),
            "notes".to_string(),
            ReplicaOrigin::Realm,
        );
        held.displayed_jsonld = "x".repeat(100);
        assert!(drive(store_replica(&held), &context).await.is_ok());
        let size = StorageEffect::Write {
            key_space: DEVICE_GRAPH_KEYSPACE.to_string(),
            key: replica_key(held.document_id),
            value: 4_000u64.to_be_bytes().to_vec().into(),
            txn_id: None,
        };
        context.storage_handle.send_storage_effect(size).await;
        assert_eq!(
            drive(cache_usage(), &context).await,
            Ok(replica_size(&held) + 4_000)
        );
        assert!(
            drive(delete_replica(held.document_id), &context)
                .await
                .is_ok()
        );
        assert_eq!(drive(cache_usage(), &context).await, Ok(4_000));
    }

    fn replica() -> ReplicaRecord {
        crate::tests::device::replica()
    }

    // A row that cannot be read or decoded counts as a document this device does not hold.
    #[test]
    fn unreadable_replica_absent() {
        let id = replica().document_id;
        let mut read = read_replica(id);
        assert!(matches!(read.first(), Ok(Next::Call(IoCall::Child(_)))));
        let row = IoResult::Values(vec![Some(replica().to_bytes().unwrap().into())]);
        let found = read.next(IoResult::Child(ChildOutcome::new(Ok::<_, IoError>(row))));
        assert_eq!(found, Ok(Next::Done(Some(replica()))));
        let failed = Err::<IoResult, _>(IoError::Unfinished);
        let answer = IoResult::Child(ChildOutcome::new(failed));
        assert_eq!(read_replica(id).next(answer), Ok(Next::Done(None)));
    }

    // Storing writes the encoded row; deleting removes it.
    #[test]
    fn writes_replica_rows() {
        let held = replica();
        let row = replica_entry(&held).unwrap();
        let mut store = store_replica(&held);
        assert_eq!(store.first(), Ok(Next::Call(IoCall::Write(vec![row]))));
        assert_eq!(store.next(IoResult::Done), Ok(Next::Done(())));
        let mut delete = delete_replica(held.document_id);
        let row = (
            DEVICE_REPLICA_KEYSPACE.to_string(),
            replica_key(held.document_id),
        );
        assert_eq!(delete.first(), Ok(Next::Call(IoCall::Delete(vec![row]))));
        assert_eq!(delete.next(IoResult::Done), Ok(Next::Done(())));
    }
}

#[cfg(test)]
mod budget_tests {
    use super::*;

    // A device that never stored a budget keeps the default.
    #[test]
    fn missing_budget_default() {
        let mut read = cache_budget();
        assert!(matches!(read.first(), Ok(Next::Call(IoCall::Read(_)))));
        let answer = read.next(IoResult::Values(vec![None]));
        assert_eq!(answer, Ok(Next::Done(DEFAULT_CACHE_BYTES)));
    }

    // A stored budget is read back as written.
    #[test]
    fn stored_budget_read() {
        let mut store = store_cache_budget(7);
        let Ok(Next::Call(IoCall::Write(rows))) = store.first() else {
            panic!("expected the write");
        };
        let mut read = cache_budget();
        read.first().unwrap();
        let written = rows.into_iter().next().map(|(_, _, value)| value);
        assert_eq!(
            read.next(IoResult::Values(vec![written])),
            Ok(Next::Done(7))
        );
        assert_eq!(store.next(IoResult::Done), Ok(Next::Done(())));
    }
}
