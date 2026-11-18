//! Retires a processed replication job and journals the plugin event of a finished sync job.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::{
    EVENT_JOURNAL_KEYSPACE, RELATIONSHIP_OUT_KEYSPACE, REPLICATION_JOB_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::plugin_runs::{SourceEvent, SourceKind, journal_entries};
use aruna_core::rows::read_row;
use aruna_core::structs::storage::blob::{BucketInfo, object_permission_path};
use aruna_core::structs::storage::encryption::BucketKeyRef;
use aruna_core::structs::storage::replication_queue::{BlobJobRecord, ReplicateScopeTarget};
use aruna_core::structs::{SyncRelationship, SyncState, sync_relationship_key};
use aruna_core::types::Value;

use super::queue::BlobQueueError;

/// How a processed job ends. The first three retire it; only `Completed` and `Failed` report.
#[derive(Debug, PartialEq)]
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
    ReadJob,
    ReadRelationship,
    ReadBucket(Box<SyncRelationship>),
    DeleteJob(Option<SourceEvent>),
    WriteEvent,
}

type Step = Result<Next<()>, BlobQueueError>;

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
    state: RetireState,
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
            state: RetireState::ReadJob,
        }
    }

    fn read(&mut self, state: RetireState, key_space: &str, key: Vec<u8>) -> Step {
        self.state = state;
        Ok(Next::Call(read_row(key_space, key)))
    }

    fn job_read(&mut self, value: Option<Value>) -> Step {
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

    fn relationship_read(&mut self, value: Option<Value>) -> Step {
        let Some(value) = value else {
            return self.delete_job(None);
        };
        let relationship = SyncRelationship::from_bytes(&value)?;
        let bucket = self.job.input.bucket.as_bytes().to_vec();
        let state = RetireState::ReadBucket(Box::new(relationship));
        self.read(state, S3_BUCKET_KEYSPACE, bucket)
    }

    /// A completed job reports while the relationship exists and is not detached, a failed one
    /// only while it is enabled (a stopped one reports through its status); none without bucket.
    fn bucket_read(&mut self, relationship: SyncRelationship, value: Option<Value>) -> Step {
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
        let group_id = BucketInfo::from_bytes(&value)?.group_id;
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

    fn delete_job(&mut self, event: Option<SourceEvent>) -> Step {
        if !self.delete {
            return self.write_event(event);
        }
        self.state = RetireState::DeleteJob(event);
        let row = (
            REPLICATION_JOB_KEYSPACE.to_string(),
            self.key.clone().into(),
        );
        Ok(Next::Call(IoCall::Delete(vec![row])))
    }

    fn write_event(&mut self, event: Option<SourceEvent>) -> Step {
        let Some(event) = event else {
            return Ok(Next::Done(()));
        };
        let writes = journal_entries(&event)?;
        self.state = RetireState::WriteEvent;
        let writes = (writes.into_iter())
            .map(|(_, key, value)| (EVENT_JOURNAL_KEYSPACE.to_string(), key, value))
            .collect();
        Ok(Next::Call(IoCall::Write(writes)))
    }
}

impl Operation for RetireJobOperation {
    type Output = ();
    type Error = BlobQueueError;

    fn first(&mut self) -> Step {
        let key = self.key.clone();
        self.read(RetireState::ReadJob, REPLICATION_JOB_KEYSPACE, key)
    }

    fn next(&mut self, result: IoResult) -> Step {
        match std::mem::replace(&mut self.state, RetireState::WriteEvent) {
            RetireState::ReadJob => self.job_read(result.value()?),
            RetireState::ReadRelationship => self.relationship_read(result.value()?),
            RetireState::ReadBucket(relationship) => {
                self.bucket_read(*relationship, result.value()?)
            }
            RetireState::DeleteJob(event) => {
                result.done()?;
                self.write_event(event)
            }
            RetireState::WriteEvent => {
                result.done()?;
                Ok(Next::Done(()))
            }
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

#[cfg(test)]
#[path = "retire_tests.rs"]
mod tests;
