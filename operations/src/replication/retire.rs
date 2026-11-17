//! Retires a processed replication job and journals the plugin event of a finished sync job.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect, abort_effects};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    EVENT_JOURNAL_KEYSPACE, RELATIONSHIP_OUT_KEYSPACE, REPLICATION_JOB_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::EffectOperation;
use aruna_core::plugin_runs::{SourceEvent, SourceKind, journal_entries};
use aruna_core::structs::storage::blob::{BucketInfo, object_permission_path};
use aruna_core::structs::storage::encryption::BucketKeyRef;
use aruna_core::structs::storage::replication_queue::{BlobJobRecord, ReplicateScopeTarget};
use aruna_core::structs::{SyncRelationship, SyncState, sync_relationship_key};
use aruna_core::types::{Effects, TxnId, Value};
use smallvec::smallvec;

use super::queue::BlobQueueError;

/// How a processed job ends. The first three retire it; only `Completed` and `Failed` report.
pub(crate) enum BlobJobOutcome {
    /// The run copied or confirmed every version.
    Completed,
    /// A permanent failure; a member publication retries instead.
    Failed,
    /// Nothing to report: the relationship was not enabled or is gone, or nothing was owed.
    Retired,
    /// The job waits, parked, for one of these locked source keys.
    AwaitingKey(Vec<BucketKeyRef>),
}

#[derive(Debug, PartialEq)]
enum RetireState {
    Init,
    StartTransaction,
    ReadJob,
    ReadRelationship,
    ReadBucket(Box<SyncRelationship>),
    DeleteJob(Option<SourceEvent>),
    WriteEvent,
    Commit,
    Finish,
    Error,
}

/// Retires a processed job in one transaction: its row goes only while it still holds this job,
/// so an enqueue that wrote the row again during the run keeps its own run. The plugin event of
/// a completed or failed sync job joins the same transaction.
#[derive(Debug, PartialEq)]
pub(crate) struct RetireJobOperation {
    key: Vec<u8>,
    job: BlobJobRecord,
    kind: Option<SourceKind>,
    now_ms: u64,
    delete: bool,
    txn_id: Option<TxnId>,
    state: RetireState,
    error: Option<BlobQueueError>,
}

impl RetireJobOperation {
    pub(crate) fn new(
        key: Vec<u8>,
        job: BlobJobRecord,
        outcome: &BlobJobOutcome,
        now_ms: u64,
    ) -> Self {
        let kind = match outcome {
            BlobJobOutcome::Completed => Some(SourceKind::SyncCompleted),
            BlobJobOutcome::Failed => Some(SourceKind::SyncFailed),
            BlobJobOutcome::Retired | BlobJobOutcome::AwaitingKey(_) => None,
        };
        Self {
            key,
            job,
            kind,
            now_ms,
            delete: false,
            txn_id: None,
            state: RetireState::Init,
            error: None,
        }
    }

    fn read(&mut self, state: RetireState, key_space: &str, key: Vec<u8>) -> Effects {
        self.state = state;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: key_space.to_string(),
            key: key.into(),
            txn_id: self.txn_id,
        })]
    }

    fn fail(&mut self, error: BlobQueueError) -> Effects {
        self.state = RetireState::Error;
        self.error = Some(error);
        abort_effects(&mut self.txn_id)
    }

    fn job_read(&mut self, value: Option<Value>) -> Effects {
        let stored = value.and_then(|value| BlobJobRecord::from_bytes(&value).ok());
        self.delete = stored.is_some_and(|stored| stored.job_id == self.job.job_id);
        match (self.kind, self.job.relationship_id) {
            (Some(_), Some(relationship_id)) => {
                let key = sync_relationship_key(&self.job.input.bucket, relationship_id);
                self.read(
                    RetireState::ReadRelationship,
                    RELATIONSHIP_OUT_KEYSPACE,
                    key,
                )
            }
            _ => self.delete_job(None),
        }
    }

    fn relationship_read(&mut self, value: Option<Value>) -> Effects {
        let Some(value) = value else {
            return self.delete_job(None);
        };
        let relationship = match SyncRelationship::from_bytes(&value) {
            Ok(relationship) => relationship,
            Err(error) => return self.fail(error.into()),
        };
        let bucket = self.job.input.bucket.as_bytes().to_vec();
        let state = RetireState::ReadBucket(Box::new(relationship));
        self.read(state, S3_BUCKET_KEYSPACE, bucket)
    }

    /// A completed job reports while the relationship exists and is not detached, a failed one
    /// only while it is enabled (a stopped one reports through its status); none without bucket.
    fn bucket_read(&mut self, relationship: SyncRelationship, value: Option<Value>) -> Effects {
        let (Some(kind), Some(value)) = (self.kind, value) else {
            return self.delete_job(None);
        };
        let reports = match kind {
            SourceKind::SyncCompleted => relationship.state != SyncState::Detached,
            _ => relationship.state == SyncState::Enabled,
        };
        if !reports {
            return self.delete_job(None);
        }
        let group_id = match BucketInfo::from_bytes(&value) {
            Ok(info) => info.group_id,
            Err(error) => return self.fail(error.into()),
        };
        let bucket = &self.job.input.bucket;
        let object = match &self.job.input.target {
            ReplicateScopeTarget::Version { key, .. } | ReplicateScopeTarget::Object { key } => key,
            ReplicateScopeTarget::Prefix(prefix) => prefix,
            ReplicateScopeTarget::Bucket => "",
        };
        let source = &relationship.source;
        let path =
            object_permission_path(source.realm_id, group_id, source.node_id, bucket, object);
        let event = SourceEvent::sync_job(
            kind,
            (group_id, path),
            (relationship.id, relationship.created_by),
            (source.node_id, self.job.job_id),
            self.now_ms,
        );
        self.delete_job(Some(event))
    }

    fn delete_job(&mut self, event: Option<SourceEvent>) -> Effects {
        if !self.delete {
            return self.write_event(event);
        }
        self.state = RetireState::DeleteJob(event);
        smallvec![Effect::Storage(StorageEffect::BatchDelete {
            deletes: vec![(
                REPLICATION_JOB_KEYSPACE.to_string(),
                self.key.clone().into()
            )],
            txn_id: self.txn_id,
        })]
    }

    fn write_event(&mut self, event: Option<SourceEvent>) -> Effects {
        let Some(event) = event else {
            return self.commit();
        };
        let writes = match journal_entries(&event) {
            Ok(writes) => writes,
            Err(error) => return self.fail(error.into()),
        };
        self.state = RetireState::WriteEvent;
        let writes = (writes.into_iter())
            .map(|(_, key, value)| (EVENT_JOURNAL_KEYSPACE.to_string(), key, value))
            .collect();
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: self.txn_id,
        })]
    }

    fn commit(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(BlobQueueError::UnexpectedEvent("no transaction".into()));
        };
        self.state = RetireState::Commit;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }
}

impl EffectOperation for RetireJobOperation {
    type Output = ();
    type Error = BlobQueueError;

    fn start(&mut self) -> Effects {
        self.state = RetireState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (
            std::mem::replace(&mut self.state, RetireState::Error),
            event,
        ) {
            (RetireState::Commit, Event::Storage(StorageEvent::Error { error })) => {
                // The failed commit closed the transaction.
                self.txn_id = None;
                self.fail(error.into())
            }
            (
                RetireState::StartTransaction,
                Event::Storage(StorageEvent::TransactionStarted { txn_id }),
            ) => {
                self.txn_id = Some(txn_id);
                let key = self.key.clone();
                self.read(RetireState::ReadJob, REPLICATION_JOB_KEYSPACE, key)
            }
            (RetireState::ReadJob, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                self.job_read(value)
            }
            (
                RetireState::ReadRelationship,
                Event::Storage(StorageEvent::ReadResult { value, .. }),
            ) => self.relationship_read(value),
            (
                RetireState::ReadBucket(relationship),
                Event::Storage(StorageEvent::ReadResult { value, .. }),
            ) => self.bucket_read(*relationship, value),
            (
                RetireState::DeleteJob(event),
                Event::Storage(StorageEvent::BatchDeleteResult { .. }),
            ) => self.write_event(event),
            (RetireState::WriteEvent, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                self.commit()
            }
            (RetireState::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.txn_id = None;
                self.state = RetireState::Finish;
                smallvec![]
            }
            (_, Event::Storage(StorageEvent::Error { error })) => self.fail(error.into()),
            (_, event) => self.fail(BlobQueueError::UnexpectedEvent(event.label().to_string())),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, RetireState::Finish | RetireState::Error)
    }

    fn finalize(self) -> Result<(), BlobQueueError> {
        match (self.state, self.error) {
            (RetireState::Finish, _) => Ok(()),
            (_, Some(error)) => Err(error),
            _ => Err(BlobQueueError::UnexpectedEvent(
                "retire did not finish".into(),
            )),
        }
    }

    fn abort(&mut self) -> Effects {
        abort_effects(&mut self.txn_id)
    }
}

#[cfg(test)]
#[path = "retire_tests.rs"]
mod tests;
