//! Frees pending archives that no version owns, after the backend's reclaim grace.
//! The grace runs from the first sweep that finds the archive without owners.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_CLEANUP_KEYSPACE, PENDING_LOCATION_KEYSPACE, PENDING_RECLAIM_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::blob::{ArchiveKey, BackendLocation, BlobCleanupWork};
use aruna_core::types::{Effects, TxnId};
use smallvec::smallvec;
use ulid::Ulid;

use crate::blob::reclaim::{ReclaimBlobError, ReclaimVerdict};
use crate::blob::records::owners_scan_effect;
use crate::node::usage_stats::{StoredDelta, UsageCounterUpdate};

#[derive(Debug, PartialEq)]
enum State {
    Init,
    StartTransaction,
    ReadPending,
    ScanOwners,
    ReadMarker,
    WriteRows,
    QueueCleanup,
    UpdateUsage,
    Commit,
    Abort,
    Finish,
    Error,
}

/// Decides one pending archive inside one transaction: owners, marker and free commit together.
#[derive(Debug, PartialEq)]
pub struct ReclaimPendingOperation {
    archive: ArchiveKey,
    grace: Duration,
    now: SystemTime,
    state: State,
    txn_id: Option<TxnId>,
    location: Option<BackendLocation>,
    usage: Option<UsageCounterUpdate>,
    output: Option<Result<ReclaimVerdict, ReclaimBlobError>>,
}

impl ReclaimPendingOperation {
    pub fn new(archive: ArchiveKey, grace: Duration, now: SystemTime) -> Self {
        Self {
            archive,
            grace,
            now,
            state: State::Init,
            txn_id: None,
            location: None,
            usage: None,
            output: None,
        }
    }

    fn key(&self) -> aruna_core::types::Key {
        self.archive.to_bytes().into()
    }

    fn fail(&mut self, error: ReclaimBlobError) -> Effects {
        self.output = Some(Err(error));
        self.abort_or_finish()
    }

    fn abort_or_finish(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => {
                self.state = State::Abort;
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            None => {
                self.state = State::Error;
                smallvec![]
            }
        }
    }

    fn unexpected(&mut self, state: &'static str, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }
        self.fail(ReclaimBlobError::InvalidStateEvent {
            state,
            expected: "the storage result of the last effect",
            received: event,
        })
    }

    /// Stages one row change; the verdict decides whether the archive is freed after it.
    fn settle(&mut self, verdict: ReclaimVerdict, effect: Effect) -> Effects {
        self.output = Some(Ok(verdict));
        self.state = State::WriteRows;
        smallvec![effect]
    }

    fn commit(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id.take() else {
            return self.fail(ReclaimBlobError::Failed);
        };
        self.state = State::Commit;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_marker(&mut self, value: Option<aruna_core::types::Value>) -> Effects {
        let since = value
            .and_then(|value| <[u8; 8]>::try_from(value.as_ref()).ok())
            .map(|millis| UNIX_EPOCH + Duration::from_millis(u64::from_be_bytes(millis)));
        let Some(since) = since else {
            let millis = self
                .now
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            let effect = Effect::Storage(StorageEffect::Write {
                key_space: PENDING_RECLAIM_KEYSPACE.to_string(),
                key: self.key(),
                value: millis.to_be_bytes().to_vec().into(),
                txn_id: self.txn_id,
            });
            return self.settle(ReclaimVerdict::NotDue, effect);
        };
        if since
            .checked_add(self.grace)
            .is_none_or(|due| due > self.now)
        {
            self.output = Some(Ok(ReclaimVerdict::NotDue));
            return self.commit();
        }
        let bytes = self
            .location
            .as_ref()
            .map_or(0, |location| location.stored_size());
        let effect = Effect::Storage(StorageEffect::BatchDelete {
            deletes: vec![
                (PENDING_LOCATION_KEYSPACE.to_string(), self.key()),
                (PENDING_RECLAIM_KEYSPACE.to_string(), self.key()),
            ],
            txn_id: self.txn_id,
        });
        self.settle(ReclaimVerdict::Freed { bytes }, effect)
    }

    fn handle_rows(&mut self) -> Effects {
        let freed = matches!(self.output, Some(Ok(ReclaimVerdict::Freed { .. })));
        let Some(location) = self.location.clone().filter(|_| freed) else {
            return self.commit();
        };
        let work = match (BlobCleanupWork::DeleteBlob { location }).to_bytes() {
            Ok(work) => work,
            Err(error) => return self.fail(error.into()),
        };
        self.state = State::QueueCleanup;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: BLOB_CLEANUP_KEYSPACE.to_string(),
            key: Ulid::generate().to_bytes().to_vec().into(),
            value: work.into(),
            txn_id: self.txn_id,
        })]
    }

    /// The archive was charged once on its archive shard; freeing it debits that charge.
    fn start_usage(&mut self) -> Effects {
        let (Some(txn_id), Some(location)) = (self.txn_id, self.location.as_ref()) else {
            return self.fail(ReclaimBlobError::Failed);
        };
        let bytes = -i128::from(location.stored_size());
        let Some(stored) = StoredDelta::of_copy(location, -1, bytes) else {
            return self.commit();
        };
        let mut update = UsageCounterUpdate::for_stored(stored);
        self.state = State::UpdateUsage;
        let effects = update.start(txn_id);
        self.usage = Some(update);
        effects
    }
}

impl Operation for ReclaimPendingOperation {
    type Output = ReclaimVerdict;
    type Error = ReclaimBlobError;

    fn start(&mut self) -> Effects {
        self.state = State::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (&self.state, event) {
            (
                State::StartTransaction,
                Event::Storage(StorageEvent::TransactionStarted { txn_id }),
            ) => {
                self.txn_id = Some(txn_id);
                self.state = State::ReadPending;
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: PENDING_LOCATION_KEYSPACE.to_string(),
                    key: self.key(),
                    txn_id: self.txn_id,
                })]
            }
            (State::ReadPending, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                let Some(value) = value else {
                    self.output = Some(Ok(ReclaimVerdict::Dropped));
                    let effect = Effect::Storage(StorageEffect::Delete {
                        key_space: PENDING_RECLAIM_KEYSPACE.to_string(),
                        key: self.key(),
                        txn_id: self.txn_id,
                    });
                    self.state = State::WriteRows;
                    return smallvec![effect];
                };
                match BackendLocation::from_bytes(&value) {
                    Ok(location) => self.location = Some(location),
                    Err(error) => return self.fail(error.into()),
                }
                self.state = State::ScanOwners;
                smallvec![owners_scan_effect(&self.archive, self.txn_id)]
            }
            (State::ScanOwners, Event::Storage(StorageEvent::IterResult { values, .. })) => {
                if !values.is_empty() {
                    // An owner returned: the grace starts again when it leaves.
                    let effect = Effect::Storage(StorageEffect::Delete {
                        key_space: PENDING_RECLAIM_KEYSPACE.to_string(),
                        key: self.key(),
                        txn_id: self.txn_id,
                    });
                    return self.settle(ReclaimVerdict::Pinned, effect);
                }
                self.state = State::ReadMarker;
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: PENDING_RECLAIM_KEYSPACE.to_string(),
                    key: self.key(),
                    txn_id: self.txn_id,
                })]
            }
            (State::ReadMarker, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                self.handle_marker(value)
            }
            (
                State::WriteRows,
                Event::Storage(
                    StorageEvent::WriteResult { .. }
                    | StorageEvent::DeleteResult { .. }
                    | StorageEvent::BatchDeleteResult { .. },
                ),
            ) => self.handle_rows(),
            (State::QueueCleanup, Event::Storage(StorageEvent::WriteResult { .. })) => {
                self.start_usage()
            }
            (State::UpdateUsage, event) => {
                let (Some(txn_id), Some(update)) = (self.txn_id, self.usage.as_mut()) else {
                    return self.fail(ReclaimBlobError::Failed);
                };
                match update.step(event, txn_id) {
                    Ok(Some(effects)) => effects,
                    Ok(None) => self.commit(),
                    Err(error) => self.fail(error.into()),
                }
            }
            (State::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.state = State::Finish;
                smallvec![]
            }
            (State::Abort, _) => {
                self.state = State::Error;
                smallvec![]
            }
            (State::Finish | State::Error, _) => smallvec![],
            (_, event) => self.unexpected("ReclaimPending", event),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, State::Finish | State::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.state {
            State::Finish => self.output.unwrap_or(Err(ReclaimBlobError::Failed)),
            _ => Err(self
                .output
                .and_then(Result::err)
                .unwrap_or(ReclaimBlobError::Failed)),
        }
    }

    fn abort(&mut self) -> Effects {
        self.abort_or_finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use aruna_core::keyspaces::COPY_OWNER_KEYSPACE;
    use aruna_core::structs::storage::blob::{BackendRef, CopyOwner, VersionKey};
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
    async fn ownerless_freed_after_grace() {
        let dir = tempfile::tempdir().unwrap();
        let context = DriverContext {
            storage_handle: aruna_storage::FjallStorage::open(dir.path().to_str().unwrap())
                .unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
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
        let (queued, _) = crate::jobs::store::iter_prefix_page(
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
