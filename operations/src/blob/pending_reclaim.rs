//! Frees pending archives that no version owns, after the backend's reclaim grace.
//! The grace runs from the first sweep that finds the archive without owners.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::{
    BLOB_CLEANUP_KEYSPACE, COPY_OWNER_KEYSPACE, PENDING_LOCATION_KEYSPACE, PENDING_RECLAIM_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::read_row;
use aruna_core::structs::storage::blob::{ArchiveKey, BackendLocation, BlobCleanupWork, CopyOwner};
use aruna_core::types::{Key, Value};
use ulid::Ulid;

use crate::blob::reclaim::{ReclaimBlobError, ReclaimVerdict};
use crate::s3::usage::{StoredDelta, UsageCounterUpdate};

#[derive(Debug, PartialEq)]
enum State {
    ReadPending,
    ScanOwners,
    ReadMarker,
    WriteRows,
    QueueCleanup,
    UpdateUsage,
}

type Step = Result<Next<ReclaimVerdict>, ReclaimBlobError>;

/// Decides one pending archive inside one transaction: owners, marker and free commit together.
#[derive(Debug, PartialEq)]
pub struct ReclaimPendingOperation {
    archive: ArchiveKey,
    grace: Duration,
    now: SystemTime,
    state: State,
    location: Option<BackendLocation>,
    usage: Option<UsageCounterUpdate>,
    verdict: Option<ReclaimVerdict>,
}

impl ReclaimPendingOperation {
    pub fn new(archive: ArchiveKey, grace: Duration, now: SystemTime) -> Self {
        Self {
            archive,
            grace,
            now,
            state: State::ReadPending,
            location: None,
            usage: None,
            verdict: None,
        }
    }

    fn key(&self) -> Key {
        self.archive.to_bytes().into()
    }

    fn verdict(&mut self) -> Result<ReclaimVerdict, ReclaimBlobError> {
        self.verdict.take().ok_or(ReclaimBlobError::Failed)
    }

    /// Stages one row change; the verdict decides whether the archive is freed after it.
    fn settle(&mut self, verdict: ReclaimVerdict, call: IoCall) -> Step {
        self.verdict = Some(verdict);
        self.state = State::WriteRows;
        Ok(Next::Call(call))
    }

    fn handle_marker(&mut self, value: Option<Value>) -> Step {
        let since = value
            .and_then(|value| <[u8; 8]>::try_from(value.as_ref()).ok())
            .map(|millis| UNIX_EPOCH + Duration::from_millis(u64::from_be_bytes(millis)));
        let Some(since) = since else {
            let millis = self
                .now
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            let row = (
                PENDING_RECLAIM_KEYSPACE.to_string(),
                self.key(),
                millis.to_be_bytes().to_vec().into(),
            );
            return self.settle(ReclaimVerdict::NotDue, IoCall::Write(vec![row]));
        };
        if since
            .checked_add(self.grace)
            .is_none_or(|due| due > self.now)
        {
            return Ok(Next::Done(ReclaimVerdict::NotDue));
        }
        let bytes = self
            .location
            .as_ref()
            .map_or(0, |location| location.stored_size());
        let deletes = vec![
            (PENDING_LOCATION_KEYSPACE.to_string(), self.key()),
            (PENDING_RECLAIM_KEYSPACE.to_string(), self.key()),
        ];
        self.settle(ReclaimVerdict::Freed { bytes }, IoCall::Delete(deletes))
    }

    fn handle_rows(&mut self) -> Step {
        let freed = matches!(self.verdict, Some(ReclaimVerdict::Freed { .. }));
        let Some(location) = self.location.clone().filter(|_| freed) else {
            return Ok(Next::Done(self.verdict()?));
        };
        let work = (BlobCleanupWork::DeleteBlob { location }).to_bytes()?;
        self.state = State::QueueCleanup;
        let row = (
            BLOB_CLEANUP_KEYSPACE.to_string(),
            Ulid::generate().to_bytes().to_vec().into(),
            work.into(),
        );
        Ok(Next::Call(IoCall::Write(vec![row])))
    }

    /// The archive was charged once on its archive shard; freeing it debits that charge.
    fn start_usage(&mut self) -> Step {
        let location = self.location.as_ref().ok_or(ReclaimBlobError::Failed)?;
        let bytes = -i128::from(location.stored_size());
        let Some(stored) = StoredDelta::of_copy(location, -1, bytes) else {
            return Ok(Next::Done(self.verdict()?));
        };
        let mut update = UsageCounterUpdate::for_stored(stored);
        self.state = State::UpdateUsage;
        let call = update.start_calls().into_call();
        self.usage = Some(update);
        Ok(Next::Call(call))
    }
}

impl Operation for ReclaimPendingOperation {
    type Output = ReclaimVerdict;
    type Error = ReclaimBlobError;

    fn first(&mut self) -> Step {
        Ok(Next::Call(read_row(PENDING_LOCATION_KEYSPACE, self.key())))
    }

    fn next(&mut self, result: IoResult) -> Step {
        match self.state {
            State::ReadPending => {
                let Some(value) = result.value()? else {
                    let delete = (PENDING_RECLAIM_KEYSPACE.to_string(), self.key());
                    return self.settle(ReclaimVerdict::Dropped, IoCall::Delete(vec![delete]));
                };
                self.location = Some(BackendLocation::from_bytes(&value)?);
                self.state = State::ScanOwners;
                Ok(Next::Call(IoCall::Page {
                    space: COPY_OWNER_KEYSPACE.to_string(),
                    prefix: CopyOwner::prefix(&self.archive).into(),
                    after: None,
                    limit: 2,
                }))
            }
            State::ScanOwners => {
                let (owners, _) = result.page()?;
                if !owners.is_empty() {
                    // An owner returned: the grace starts again when it leaves.
                    let delete = (PENDING_RECLAIM_KEYSPACE.to_string(), self.key());
                    return self.settle(ReclaimVerdict::Pinned, IoCall::Delete(vec![delete]));
                }
                self.state = State::ReadMarker;
                Ok(Next::Call(read_row(PENDING_RECLAIM_KEYSPACE, self.key())))
            }
            State::ReadMarker => self.handle_marker(result.value()?),
            State::WriteRows => {
                result.done()?;
                self.handle_rows()
            }
            State::QueueCleanup => {
                result.done()?;
                self.start_usage()
            }
            State::UpdateUsage => {
                let update = self.usage.as_mut().ok_or(ReclaimBlobError::Failed)?;
                match update.step_calls(result)? {
                    Some(call) => Ok(Next::Call(call.into_call())),
                    None => Ok(Next::Done(self.verdict()?)),
                }
            }
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::structs::storage::blob::{BackendRef, VersionKey};
    use aruna_core::structs::storage::encryption::BucketKeyRef;
    use aruna_core::structs::storage::format::{PithosLayout, StoredFormat};
    use std::collections::HashMap;

    async fn put(context: &DriverContext, key_space: &str, key: Vec<u8>, value: Vec<u8>) {
        let effect = StorageEffect::Write {
            key_space: key_space.to_string(),
            key: key.into(),
            value: value.into(),
            txn_id: None,
        };
        context.storage_handle.send_storage_effect(effect).await;
    }

    async fn present(context: &DriverContext, key_space: &str, key: Vec<u8>) -> bool {
        let effect = StorageEffect::Read {
            key_space: key_space.to_string(),
            key: key.into(),
            txn_id: None,
        };
        matches!(
            context.storage_handle.send_storage_effect(effect).await,
            Event::Storage(StorageEvent::ReadResult { value: Some(_), .. })
        )
    }

    #[tokio::test]
    async fn ownerless_after_grace() {
        let dir = tempfile::tempdir().unwrap();
        let context = DriverContext {
            storage_handle: aruna_storage::FjallStorage::open(dir.path().to_str().unwrap())
                .unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };
        let layout = PithosLayout {
            stored_size: 130,
            metadata_digest: [4; 32],
            storage_generation: 0,
        };
        let location = BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/tmp".to_string(),
            storage_bucket: "bucket".to_string(),
            backend_path: "path".to_string(),
            ulid: Ulid::from_bytes([3; 16]),
            format: StoredFormat::pithos(layout, BucketKeyRef::new(Ulid::from_bytes([5; 16]), 1)),
            created_at: UNIX_EPOCH,
            created_by: Default::default(),
            staging: false,
            partial: false,
            blob_size: 100,
            hashes: HashMap::new(),
        };
        let archive = ArchiveKey::of(&location);
        let row = archive.to_bytes();
        put(
            &context,
            PENDING_LOCATION_KEYSPACE,
            row.clone(),
            location.to_bytes().unwrap(),
        )
        .await;
        let grace = Duration::from_secs(60);
        let at = |secs| UNIX_EPOCH + Duration::from_secs(secs);
        let run = |now| {
            drive(
                ReclaimPendingOperation::new(archive.clone(), grace, now),
                &context,
            )
        };

        // The first ownerless sweep starts the grace; it has not passed one second before its end.
        assert_eq!(run(at(1_000)).await, Ok(ReclaimVerdict::NotDue));
        assert_eq!(run(at(1_059)).await, Ok(ReclaimVerdict::NotDue));
        // A returning owner pins the archive and resets the grace.
        let version = VersionKey::new("b", "k", Ulid::from_bytes([6; 16]));
        let owner = CopyOwner::new(archive.clone(), version).key().unwrap();
        put(&context, COPY_OWNER_KEYSPACE, owner.clone(), Vec::new()).await;
        assert_eq!(run(at(1_100)).await, Ok(ReclaimVerdict::Pinned));
        assert!(!present(&context, PENDING_RECLAIM_KEYSPACE, row.clone()).await);
        let effect = StorageEffect::Delete {
            key_space: COPY_OWNER_KEYSPACE.to_string(),
            key: owner.into(),
            txn_id: None,
        };
        context.storage_handle.send_storage_effect(effect).await;
        assert_eq!(run(at(2_000)).await, Ok(ReclaimVerdict::NotDue));
        assert_eq!(
            run(at(2_060)).await,
            Ok(ReclaimVerdict::Freed { bytes: 130 })
        );
        assert!(!present(&context, PENDING_LOCATION_KEYSPACE, row.clone()).await);
        assert!(!present(&context, PENDING_RECLAIM_KEYSPACE, row).await);
        let (queued, _) = crate::storage_read::iter_prefix_page(
            &context.storage_handle,
            BLOB_CLEANUP_KEYSPACE,
            None,
            None,
            8,
            None,
        )
        .await
        .unwrap();
        assert_eq!(queued.len(), 1);
    }
}
