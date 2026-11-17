//! Queues blob replication jobs and live copy obligations for committed versions.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{REPLICATION_JOB_KEYSPACE, REPLICATION_OBLIGATION_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::notification_watch::{
    WatchEvent, WatchEventDetail, WatchEventKind, watch_resource_path,
};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::{Effects, GroupId, Key, Value};
use aruna_core::{NodeId, UserId};
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use super::plaintext::{consent_delete, consent_write, job_consent};
use super::version_replication::ReplicateScopeError;
use crate::driver::{DriverContext, drive};
use crate::notifications::watch::emit::emit_watch_event;
use aruna_core::structs::storage::replication_queue::ReferenceAdvance;
use aruna_core::structs::storage::replication_queue::ReplicateScopeInput;
use aruna_core::structs::storage::replication_queue::{
    BlobJobRecord, LiveObligationRecord, LiveReplicationContinuation, blob_job_entry, blob_job_key,
    blob_job_preferred, live_obligation_entry,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueBlobResult {
    pub queued: usize,
    pub scheduled: bool,
}

#[derive(Debug, Error, PartialEq)]
pub enum BlobQueueError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Replication(#[from] ReplicateScopeError),
    #[error("unexpected event while processing blob replication queue: {0}")]
    UnexpectedEvent(String),
    #[error("a parked copy job could not wake: {0}")]
    Wake(String),
}

pub fn schedule_blob_drain() -> Effect {
    Effect::Task(TaskEffect::ResetTimer {
        key: TaskKey::DrainReplicationQueue,
        after: Duration::ZERO,
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum QueueBlobState {
    Init,
    ClearConsent,
    ReadExisting,
    WriteJob,
    ScheduleDrain,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct QueueBlobOperation {
    job: BlobJobRecord,
    state: QueueBlobState,
    output: Option<Result<QueueBlobResult, BlobQueueError>>,
    /// The plaintext request this queueing records: `Some(None)` withdraws an earlier one.
    consent: Option<Option<UserId>>,
}

impl QueueBlobOperation {
    pub fn new(input: ReplicateScopeInput, source_delete_marker: Option<bool>) -> Self {
        Self {
            job: BlobJobRecord::new(input, source_delete_marker, unix_timestamp_millis()),
            state: QueueBlobState::Init,
            output: None,
            consent: None,
        }
    }

    /// Records whether the requester of this explicit copy asked for plaintext. A withdrawn
    /// request is deleted before the job is written; a new one commits with it.
    pub fn with_plaintext(mut self, plaintext: bool) -> Self {
        let requester = self.job.input.auth_context.user_id;
        self.consent = Some(plaintext.then_some(requester));
        self
    }

    pub fn new_relationship(
        input: ReplicateScopeInput,
        source_delete_marker: Option<bool>,
        relationship_id: Ulid,
    ) -> Self {
        Self {
            job: BlobJobRecord::new_relationship(
                input,
                source_delete_marker,
                relationship_id,
                unix_timestamp_millis(),
            ),
            state: QueueBlobState::Init,
            output: None,
            consent: None,
        }
    }

    fn fail(&mut self, error: BlobQueueError) -> Effects {
        self.state = QueueBlobState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn read_existing(&mut self) -> Effects {
        let key = match blob_job_key(&self.job) {
            Ok(key) => key,
            Err(error) => return self.fail(error.into()),
        };
        self.state = QueueBlobState::ReadExisting;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: REPLICATION_JOB_KEYSPACE.to_string(),
            key,
            txn_id: None,
        })]
    }

    fn write_job(&mut self) -> Effects {
        let (key_space, key, value) = match blob_job_entry(&self.job) {
            Ok(entry) => entry,
            Err(error) => return self.fail(error.into()),
        };
        self.state = QueueBlobState::WriteJob;
        let Some(Effect::Storage(StorageEffect::Write {
            key_space: consent_space,
            key: consent_key,
            value: consent_value,
            ..
        })) = self.consent_effect(&key)
        else {
            return smallvec![Effect::Storage(StorageEffect::Write {
                key_space,
                key,
                value,
                txn_id: None,
            })];
        };
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes: vec![
                (key_space, key, value),
                (consent_space, consent_key, consent_value)
            ],
            txn_id: None,
        })]
    }

    /// The write of a granted plaintext request for the job stored under `key`.
    fn consent_effect(&self, key: &Key) -> Option<Effect> {
        let requester = self.consent.flatten()?;
        consent_write(job_consent(key), requester).ok()
    }

    /// Withdraws an earlier plaintext request before the job is written again.
    fn clear_consent(&mut self) -> Effects {
        let key = match blob_job_key(&self.job) {
            Ok(key) => key,
            Err(error) => return self.fail(error.into()),
        };
        self.state = QueueBlobState::ClearConsent;
        smallvec![consent_delete(job_consent(&key))]
    }

    /// A preferred existing job stays, but a granted request is still recorded for it.
    fn keep_existing(&mut self) -> Effects {
        let key = match blob_job_key(&self.job) {
            Ok(key) => key,
            Err(error) => return self.fail(error.into()),
        };
        match self.consent_effect(&key) {
            Some(effect) => {
                self.state = QueueBlobState::WriteJob;
                smallvec![effect]
            }
            None => self.schedule_drain(),
        }
    }

    fn schedule_drain(&mut self) -> Effects {
        self.state = QueueBlobState::ScheduleDrain;
        smallvec![schedule_blob_drain()]
    }

    fn finish(&mut self, scheduled: bool) -> Effects {
        self.state = QueueBlobState::Finish;
        self.output = Some(Ok(QueueBlobResult {
            queued: 1,
            scheduled,
        }));
        smallvec![]
    }
}

impl Operation for QueueBlobOperation {
    type Output = QueueBlobResult;
    type Error = BlobQueueError;

    fn start(&mut self) -> Effects {
        match self.consent {
            Some(None) => self.clear_consent(),
            _ => self.read_existing(),
        }
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            QueueBlobState::Init => self.start(),
            QueueBlobState::ClearConsent => match event {
                Event::Storage(StorageEvent::DeleteResult { .. }) => self.read_existing(),
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.fail(BlobQueueError::UnexpectedEvent(other.label().to_string())),
            },
            QueueBlobState::ReadExisting => match event {
                Event::Storage(StorageEvent::ReadResult {
                    value: Some(value), ..
                }) => match BlobJobRecord::from_bytes(&value) {
                    Ok(existing) if blob_job_preferred(&existing, &self.job) => {
                        self.keep_existing()
                    }
                    Ok(_) | Err(_) => self.write_job(),
                },
                Event::Storage(StorageEvent::ReadResult { value: None, .. }) => self.write_job(),
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.fail(BlobQueueError::UnexpectedEvent(other.label().to_string())),
            },
            QueueBlobState::WriteJob => match event {
                Event::Storage(
                    StorageEvent::WriteResult { .. } | StorageEvent::BatchWriteResult { .. },
                ) => self.schedule_drain(),
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.fail(BlobQueueError::UnexpectedEvent(other.label().to_string())),
            },
            QueueBlobState::ScheduleDrain => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => self.finish(true),
                Event::Task(TaskEvent::Error { .. }) => self.finish(false),
                other => {
                    warn!(event = ?other, "Blob replication job persisted but drain scheduling returned an unexpected event");
                    self.finish(false)
                }
            },
            QueueBlobState::Finish => smallvec![],
            QueueBlobState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, QueueBlobState::Finish | QueueBlobState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.output {
            Some(Ok(result)) => Ok(result),
            Some(Err(error)) => Err(error),
            None => Err(BlobQueueError::UnexpectedEvent(
                "queue operation finished without output".to_string(),
            )),
        }
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveVersionInput {
    pub local_node_id: NodeId,
    pub auth_context: AuthContext,
    pub bucket: String,
    pub key: String,
    pub version_id: Ulid,
    pub delete_marker: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum LiveVersionState {
    Init,
    StartObligation,
    ReadObligation,
    WriteObligation,
    CommitObligation,
    AbortObligation,
    ScheduleDrain,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct LiveVersionOperation {
    input: LiveVersionInput,
    reference_advance: Option<ReferenceAdvance>,
    state: LiveVersionState,
    seed_key: Option<Key>,
    seed_value: Option<ByteView>,
    txn_id: Option<Ulid>,
    abort_error: Option<BlobQueueError>,
    output: Option<Result<QueueBlobResult, BlobQueueError>>,
}

impl LiveVersionOperation {
    pub fn new(input: LiveVersionInput) -> Self {
        Self {
            input,
            reference_advance: None,
            state: LiveVersionState::Init,
            seed_key: None,
            seed_value: None,
            txn_id: None,
            abort_error: None,
            output: None,
        }
    }

    pub fn with_reference_advance(mut self, advance: ReferenceAdvance) -> Self {
        self.reference_advance = Some(advance);
        self
    }

    fn fail(&mut self, error: BlobQueueError) -> Effects {
        self.state = LiveVersionState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn seed_record(&self) -> LiveObligationRecord {
        let mut record = LiveObligationRecord::new(
            self.input.local_node_id,
            self.input.auth_context.clone(),
            self.input.bucket.clone(),
            self.input.key.clone(),
            self.input.version_id,
            self.input.delete_marker,
        );
        if let Some(advance) = self.reference_advance {
            record = record.with_reference_advance(advance);
        }
        record.continuation = Some(LiveReplicationContinuation::default());
        record
    }

    fn start_obligation(&mut self) -> Effects {
        let record = self.seed_record();
        let (_, key, value) = match live_obligation_entry(&record) {
            Ok(entry) => entry,
            Err(error) => return self.fail(error.into()),
        };
        self.seed_key = Some(key);
        self.seed_value = Some(value);
        self.state = LiveVersionState::StartObligation;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn abort_obligation(&mut self, error: Option<BlobQueueError>) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(BlobQueueError::UnexpectedEvent(
                "obligation transaction is not active".to_string(),
            ));
        };
        self.abort_error = error;
        self.state = LiveVersionState::AbortObligation;
        smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
    }

    fn write_obligation(&mut self) -> Effects {
        let (Some(key), Some(value), Some(txn_id)) =
            (self.seed_key.clone(), self.seed_value.clone(), self.txn_id)
        else {
            return self.fail(BlobQueueError::UnexpectedEvent(
                "obligation write is not prepared".to_string(),
            ));
        };
        self.state = LiveVersionState::WriteObligation;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: REPLICATION_OBLIGATION_KEYSPACE.to_string(),
            key,
            value,
            txn_id: Some(txn_id),
        })]
    }

    fn read_obligation(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(BlobQueueError::UnexpectedEvent(
                "obligation transaction is not active".to_string(),
            ));
        };
        let Some(key) = self.seed_key.clone() else {
            return self.fail(BlobQueueError::UnexpectedEvent(
                "obligation key is not prepared".to_string(),
            ));
        };
        self.state = LiveVersionState::ReadObligation;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: REPLICATION_OBLIGATION_KEYSPACE.to_string(),
            key,
            txn_id: Some(txn_id),
        })]
    }

    fn commit_obligation(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(BlobQueueError::UnexpectedEvent(
                "obligation transaction is not active".to_string(),
            ));
        };
        self.state = LiveVersionState::CommitObligation;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn schedule_drain(&mut self) -> Effects {
        self.state = LiveVersionState::ScheduleDrain;
        smallvec![schedule_blob_drain()]
    }

    fn finish(&mut self, queued: usize, scheduled: bool) -> Effects {
        self.state = LiveVersionState::Finish;
        self.output = Some(Ok(QueueBlobResult { queued, scheduled }));
        smallvec![]
    }
}

impl Operation for LiveVersionOperation {
    type Output = QueueBlobResult;
    type Error = BlobQueueError;

    fn start(&mut self) -> Effects {
        self.start_obligation()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            LiveVersionState::Init => self.start_obligation(),
            LiveVersionState::StartObligation => match event {
                Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                    self.txn_id = Some(txn_id);
                    self.read_obligation()
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.fail(BlobQueueError::UnexpectedEvent(other.label().to_string())),
            },
            LiveVersionState::ReadObligation => match event {
                Event::Storage(StorageEvent::ReadResult { value: None, .. }) => {
                    self.write_obligation()
                }
                Event::Storage(StorageEvent::ReadResult {
                    value: Some(value), ..
                }) => match LiveObligationRecord::from_bytes(&value) {
                    Ok(_) => self.abort_obligation(None),
                    Err(_) => self.write_obligation(),
                },
                Event::Storage(StorageEvent::Error { error }) => {
                    self.abort_obligation(Some(error.into()))
                }
                other => self.abort_obligation(Some(BlobQueueError::UnexpectedEvent(
                    other.label().to_string(),
                ))),
            },
            LiveVersionState::WriteObligation => match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => self.commit_obligation(),
                Event::Storage(StorageEvent::Error { error }) => {
                    self.abort_obligation(Some(error.into()))
                }
                other => self.abort_obligation(Some(BlobQueueError::UnexpectedEvent(
                    other.label().to_string(),
                ))),
            },
            LiveVersionState::CommitObligation => match event {
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => self.schedule_drain(),
                Event::Storage(StorageEvent::Error {
                    error: StorageError::TransactionConflict,
                }) => self.schedule_drain(),
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.fail(BlobQueueError::UnexpectedEvent(other.label().to_string())),
            },
            LiveVersionState::AbortObligation => match event {
                Event::Storage(StorageEvent::TransactionAborted { .. }) => {
                    match self.abort_error.take() {
                        Some(error) => self.fail(error),
                        None => self.schedule_drain(),
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.fail(BlobQueueError::UnexpectedEvent(other.label().to_string())),
            },
            LiveVersionState::ScheduleDrain => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => self.finish(0, true),
                Event::Task(TaskEvent::Error { .. }) => self.finish(0, false),
                other => {
                    warn!(event = ?other, "Durable replication obligation persisted but drain scheduling returned an unexpected event");
                    self.finish(0, false)
                }
            },
            LiveVersionState::Finish => smallvec![],
            LiveVersionState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            LiveVersionState::Finish | LiveVersionState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.output {
            Some(Ok(result)) => Ok(result),
            Some(Err(error)) => Err(error),
            None => Err(BlobQueueError::UnexpectedEvent(
                "live replication queue operation finished without output".to_string(),
            )),
        }
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn complete_put(
    context: &DriverContext,
    realm_id: RealmId,
    node_id: NodeId,
    auth: AuthContext,
    group_id: GroupId,
    bucket: String,
    key: String,
    version_id: Ulid,
    size_bytes: u64,
) {
    let actor = auth.user_id;
    queue_live(
        context,
        node_id,
        auth,
        bucket.clone(),
        key.clone(),
        version_id,
        false,
    )
    .await;

    let path = watch_resource_path(group_id, node_id, &bucket, &key);
    let event = WatchEvent {
        event_id: Ulid::generate(),
        realm_id,
        kind: WatchEventKind::DataUploaded,
        path,
        actor,
        occurred_at_ms: unix_timestamp_millis(),
        detail: WatchEventDetail::DataUploaded {
            group_id,
            node_id,
            bucket,
            key,
            size_bytes,
        },
    };
    emit_watch_event(context, event).await;
}

/// Queues continuous sync for one committed version or delete marker and wakes the drain.
pub async fn queue_live(
    context: &DriverContext,
    node_id: NodeId,
    auth: AuthContext,
    bucket: String,
    key: String,
    version_id: Ulid,
    delete_marker: bool,
) {
    match drive(
        LiveVersionOperation::new(LiveVersionInput {
            local_node_id: node_id,
            auth_context: auth,
            bucket: bucket.clone(),
            key: key.clone(),
            version_id,
            delete_marker,
        }),
        context,
    )
    .await
    {
        Ok(result) => {
            if result.queued > 0 && !result.scheduled {
                warn!(bucket, key, version_id = %version_id, queued = result.queued, "Live replication jobs persisted but drain scheduling was not acknowledged");
            }
        }
        Err(error) => {
            warn!(
                error = %error,
                bucket,
                key,
                version_id = %version_id,
                delete_marker,
                "Failed to queue live replication after committed write; durable obligation remains for repair"
            );
        }
    }
}

/// Rows of the job queue one page of the statistics reads.
const STATS_PAGE_SIZE: usize = 256;

/// Counts queued replication jobs bound to a relationship and the oldest enqueue time among
/// them, in Unix milliseconds.
#[derive(Debug, PartialEq)]
pub struct JobStatsOperation {
    relationship_id: Ulid,
    pending: usize,
    oldest: Option<u64>,
    output: Option<Result<(usize, Option<u64>), BlobQueueError>>,
}

impl JobStatsOperation {
    pub fn new(relationship_id: Ulid) -> Self {
        Self {
            relationship_id,
            pending: 0,
            oldest: None,
            output: None,
        }
    }

    fn read_page(start: Option<Key>) -> Effects {
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: REPLICATION_JOB_KEYSPACE.to_string(),
            prefix: None,
            start: start.map(IterStart::After),
            limit: STATS_PAGE_SIZE,
            txn_id: None,
        })]
    }

    fn page_read(&mut self, values: Vec<(Key, Value)>, next: Option<Key>) -> Effects {
        for (_, value) in values {
            let record = match BlobJobRecord::from_bytes(value.as_ref()) {
                Ok(record) => record,
                Err(error) => {
                    self.output = Some(Err(error.into()));
                    return smallvec![];
                }
            };
            if record.relationship_id == Some(self.relationship_id) {
                self.pending = self.pending.saturating_add(1);
                let enqueued = record.enqueued_at_ms;
                self.oldest = Some(self.oldest.map_or(enqueued, |oldest| oldest.min(enqueued)));
            }
        }
        match next {
            Some(next) => Self::read_page(Some(next)),
            None => {
                self.output = Some(Ok((self.pending, self.oldest)));
                smallvec![]
            }
        }
    }
}

impl Operation for JobStatsOperation {
    type Output = (usize, Option<u64>);
    type Error = BlobQueueError;

    fn start(&mut self) -> Effects {
        Self::read_page(None)
    }

    fn step(&mut self, event: Event) -> Effects {
        match (&self.output, event) {
            (
                None,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => self.page_read(values, next_start_after),
            (None, Event::Storage(StorageEvent::Error { error })) => {
                self.output = Some(Err(error.into()));
                smallvec![]
            }
            (_, event) => {
                self.output = Some(Err(BlobQueueError::UnexpectedEvent(
                    event.label().to_string(),
                )));
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<Self::Output, BlobQueueError> {
        self.output
            .unwrap_or_else(|| Err(BlobQueueError::UnexpectedEvent("not finished".into())))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::keyspaces::{
        NODE_STATE_KEYSPACE, RELATIONSHIP_IN_KEYSPACE, RELATIONSHIP_OUT_KEYSPACE,
        S3_BUCKET_KEYSPACE,
    };
    use aruna_core::plugin_runs::{SourceEvent, SourceKind};
    use aruna_core::structs::{SyncMode, SyncRelationship, SyncState};
    use aruna_storage::StorageHandle;
    use tracing::info;

    use super::super::drain::{
        REPLICATION_CURSOR_KEY, ReplicationScanCursor, advance_replication_cursor, job_source_auth,
        load_source_authorization, mark_failure, mark_progress, mark_success, process_blob_batch,
        read_job_relationships, read_relationships, read_replication_cursor, reschedule_blob_job,
        scan_due_jobs, store_relationship, write_blob_job,
    };
    use super::super::retire::{BlobJobOutcome, RetireJobOperation};

    /// Whether any job or live obligation is queued.
    async fn blob_jobs_exist(storage: &StorageHandle) -> Result<bool, BlobQueueError> {
        for key_space in [REPLICATION_OBLIGATION_KEYSPACE, REPLICATION_JOB_KEYSPACE] {
            let (rows, _) =
                crate::storage_read::iter_prefix_page(storage, key_space, None, None, 1, None)
                    .await
                    .map_err(|error| BlobQueueError::Storage(StorageError::ReadError(error)))?;
            if !rows.is_empty() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Retires through the operation at the current time, as the drain does.
    async fn retire_job(
        context: &DriverContext,
        key: Vec<u8>,
        job: &BlobJobRecord,
        outcome: &BlobJobOutcome,
    ) -> Result<(), BlobQueueError> {
        let now_ms = unix_timestamp_millis();
        drive(
            RetireJobOperation::new(key, job.clone(), outcome, now_ms),
            context,
        )
        .await
    }
    use super::super::obligations::{
        OBLIGATION_BATCH_SIZE, REPLICATION_JOB_LIMIT, REPLICATION_RELATIONSHIP_LIMIT,
        delete_live_obligation, process_live_obligations, read_relationships_limit,
        retire_obligations, write_live_jobs, write_live_obligation,
    };
    use super::super::version_replication::SourceAuthorizationError;
    use crate::plugins::store::Txn;
    use crate::sync::mirror_repair::store_sync_status;
    use aruna_core::UserId;
    use aruna_core::keyspaces::{
        AUTH_KEYSPACE, BLOB_VERSIONS_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE,
    };
    use aruna_core::request_policy::{PolicyKind, RequestPolicy};
    use aruna_core::structs::identity::auth::{Actor, PathRestriction, Permission};
    use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
    use aruna_core::structs::identity::realm::{
        RealmAuthorizationDocument, RealmConfigDocument, RealmId,
    };
    use aruna_core::structs::storage::blob::{
        BackendRef, BlobVersion, BucketInfo, VersionKey, object_permission_path,
    };
    use aruna_core::structs::storage::format::Compression;
    use aruna_core::structs::storage::format::EncodingClass;
    use aruna_core::structs::storage::replication::{ArunaArn, ReplicationItemError};
    use aruna_core::structs::storage::replication_queue::MemberTarget;
    use aruna_core::structs::storage::replication_queue::ReplicateScopeTarget;
    use aruna_core::structs::storage::replication_queue::{
        BlobJobRecord, LiveObligationRecord, LiveReplicationContinuation, RelationshipJobTarget,
        blob_job_entry, blob_job_key, build_live_obligation, continuation_newer,
        live_obligation_entry, live_obligation_key, queued_prefixes, relationship_job,
    };
    use aruna_core::structs::storage::replication_queue::{ReplicationMode, SyncOrigin};
    use aruna_core::structs::{ReferenceHandling, SyncStatusSnapshot, sync_relationship_key};
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_storage::FjallStorage;
    use std::time::SystemTime;
    use tempfile::tempdir;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn realm() -> RealmId {
        RealmId::from_bytes([9u8; 32])
    }

    fn user() -> UserId {
        UserId::local(Ulid::from_parts(1, 1), realm())
    }

    fn auth_context() -> AuthContext {
        AuthContext {
            user_id: user(),
            realm_id: realm(),
            path_restrictions: None,
            session: None,
        }
    }

    fn reference_advance() -> ReferenceAdvance {
        ReferenceAdvance {
            generation: 2,
            predecessor: Ulid::from_parts(1, 1),
        }
    }

    fn on_demand_input() -> ReplicateScopeInput {
        ReplicateScopeInput {
            bucket: "bucket".to_string(),
            target: ReplicateScopeTarget::Object {
                key: "key".to_string(),
            },
            target_node_id: node(2),
            auth_context: auth_context(),
            replicate_delete_markers: true,
            mode: ReplicationMode::OnDemand,
        }
    }

    #[test]
    fn queued_prefixes_match() {
        let [live, on_demand, obligations] = queued_prefixes("bucket").unwrap();
        let job = blob_job_key(&BlobJobRecord::new(on_demand_input(), None, 42)).unwrap();
        assert!(job.starts_with(&on_demand.1) && !job.starts_with(&live.1));
        let other = queued_prefixes("bucke").unwrap();
        assert!(!job.starts_with(&other[1].1));
        let obligation = LiveObligationRecord {
            local_node_id: node(1),
            auth_context: auth_context(),
            bucket: "bucket".to_string(),
            key: "key".to_string(),
            version_id: Ulid::from_parts(1, 1),
            delete_marker: false,
            origin: None,
            upstream_sources: Vec::new(),
            continuation: None,
            reference_advance: None,
        };
        let key = live_obligation_key(&obligation).unwrap();
        assert!(key.starts_with(&obligations.1));
    }

    async fn read_jobs(storage: &StorageHandle) -> Vec<(Vec<u8>, BlobJobRecord)> {
        match storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: REPLICATION_JOB_KEYSPACE.to_string(),
                prefix: None,
                start: None,
                limit: REPLICATION_RELATIONSHIP_LIMIT,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::IterResult { values, .. }) => values
                .into_iter()
                .map(|(key, value)| {
                    (
                        key.to_vec(),
                        BlobJobRecord::from_bytes(&value).expect("replication job decodes"),
                    )
                })
                .collect(),
            other => panic!("unexpected storage event: {other:?}"),
        }
    }

    async fn write_queue_record(
        storage: &StorageHandle,
        key_space: &str,
        key: Vec<u8>,
        value: Vec<u8>,
    ) {
        match storage
            .send_storage_effect(StorageEffect::Write {
                key_space: key_space.to_string(),
                key: ByteView::from(key),
                value: ByteView::from(value),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected raw queue write event: {other:?}"),
        }
    }

    async fn write_corrupt_job(storage: &StorageHandle, key: &str) {
        write_queue_record(
            storage,
            REPLICATION_JOB_KEYSPACE,
            key.as_bytes().to_vec(),
            Vec::new(),
        )
        .await;
    }

    async fn write_corrupt_obligation(storage: &StorageHandle, key: &str) {
        write_queue_record(
            storage,
            REPLICATION_OBLIGATION_KEYSPACE,
            key.as_bytes().to_vec(),
            Vec::new(),
        )
        .await;
    }

    async fn write_bucket(storage: &StorageHandle, bucket: &str) {
        let info = BucketInfo {
            group_id: Ulid::from_parts(2, 2),
            created_at: SystemTime::UNIX_EPOCH,
            created_by: user(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: None,
        };
        match storage
            .send_storage_effect(StorageEffect::Write {
                key_space: aruna_core::keyspaces::S3_BUCKET_KEYSPACE.to_string(),
                key: bucket.as_bytes().to_vec().into(),
                value: info.to_bytes().expect("bucket info serializes").into(),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected bucket write event: {other:?}"),
        }
    }

    async fn write_auth_docs(storage: &StorageHandle, group_id: Ulid) {
        let actor = Actor {
            node_id: node(1),
            user_id: user(),
            realm_id: realm(),
        };
        // Policy loading fails closed without the realm config document.
        write_queue_record(
            storage,
            REALM_CONFIG_KEYSPACE,
            realm().as_bytes().to_vec(),
            RealmConfigDocument::default_for_realm(realm(), Vec::new())
                .to_bytes(&actor)
                .unwrap(),
        )
        .await;
        write_queue_record(
            storage,
            AUTH_KEYSPACE,
            realm().as_bytes().to_vec(),
            RealmAuthorizationDocument::default_realm_doc(realm())
                .to_bytes(&actor)
                .unwrap(),
        )
        .await;
        write_queue_record(
            storage,
            AUTH_KEYSPACE,
            group_id.to_bytes().to_vec(),
            GroupAuthorizationDocument::default_group_doc(user(), realm(), group_id)
                .to_bytes(&actor)
                .unwrap(),
        )
        .await;
    }

    // Policy loading resolves the group record before group policies apply.
    async fn write_group(storage: &StorageHandle, group_id: Ulid) {
        let actor = Actor {
            node_id: node(1),
            user_id: user(),
            realm_id: realm(),
        };
        let auth = GroupAuthorizationDocument::default_group_doc(user(), realm(), group_id);
        let group = Group {
            display_name: "replication-group".to_string(),
            group_id,
            realm_id: realm(),
            roles: auth.roles.keys().copied().collect(),
            owner: user(),
        };
        write_queue_record(
            storage,
            GROUP_KEYSPACE,
            group_id.to_bytes().to_vec(),
            group.to_bytes(&actor).unwrap(),
        )
        .await;
    }

    async fn write_group_policy(storage: &StorageHandle, group_id: Ulid, expression: &str) {
        let actor = Actor {
            node_id: node(1),
            user_id: user(),
            realm_id: realm(),
        };
        let mut document = GroupAuthorizationDocument::default_group_doc(user(), realm(), group_id);
        document.policies.push(RequestPolicy {
            policy_id: Ulid::from_parts(7, 7),
            name: "replication-policy".to_string(),
            kind: PolicyKind::Deny,
            when: None,
            expression: expression.to_string(),
            enabled: true,
        });
        write_queue_record(
            storage,
            AUTH_KEYSPACE,
            group_id.to_bytes().to_vec(),
            document.to_bytes(&actor).unwrap(),
        )
        .await;
    }

    fn relationship(
        id: u128,
        target: u8,
        prefix: Option<&str>,
        replicate_deletes: bool,
    ) -> SyncRelationship {
        let source = match prefix {
            Some(prefix) => ArunaArn::s3_object_prefix(realm(), node(1), "bucket", prefix).unwrap(),
            None => ArunaArn::s3_bucket(realm(), node(1), "bucket").unwrap(),
        };
        SyncRelationship {
            id: Ulid::from(id),
            source,
            target: ArunaArn::s3_bucket(realm(), node(target), "bucket").unwrap(),
            mode: SyncMode::Continuous,
            reference_handling: Default::default(),
            reference_serving: false,
            replicate_deletes,
            created_by: user(),
            created_at: SystemTime::UNIX_EPOCH,
            state: SyncState::Enabled,
            status: SyncStatusSnapshot::default(),
        }
    }

    fn sync_link(
        id: u128,
        source_node: u8,
        source_bucket: &str,
        target_node: u8,
        target_bucket: &str,
    ) -> SyncRelationship {
        SyncRelationship {
            id: Ulid::from(id),
            source: ArunaArn::s3_bucket(realm(), node(source_node), source_bucket).unwrap(),
            target: ArunaArn::s3_bucket(realm(), node(target_node), target_bucket).unwrap(),
            mode: SyncMode::Continuous,
            reference_handling: Default::default(),
            reference_serving: false,
            replicate_deletes: true,
            created_by: user(),
            created_at: SystemTime::UNIX_EPOCH,
            state: SyncState::Enabled,
            status: SyncStatusSnapshot::default(),
        }
    }

    async fn write_relationship(storage: &StorageHandle, relationship: &SyncRelationship) {
        let bucket = relationship.source.bucket().unwrap();
        match storage
            .send_storage_effect(StorageEffect::Write {
                key_space: RELATIONSHIP_OUT_KEYSPACE.to_string(),
                key: sync_relationship_key(bucket, relationship.id).into(),
                value: relationship.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected relationship write event: {other:?}"),
        }
    }

    async fn write_inbound(storage: &StorageHandle, relationship: &SyncRelationship) {
        let bucket = relationship.target.bucket().unwrap();
        match storage
            .send_storage_effect(StorageEffect::Write {
                key_space: RELATIONSHIP_IN_KEYSPACE.to_string(),
                key: sync_relationship_key(bucket, relationship.id).into(),
                value: relationship.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected inbound relationship write event: {other:?}"),
        }
    }

    async fn write_materialized_version(
        storage: &StorageHandle,
        bucket: &str,
        key: &str,
        version_id: Ulid,
    ) {
        let version = BlobVersion::materialized(
            [7u8; 32],
            BackendRef::node_default(),
            EncodingClass::Raw,
            SystemTime::UNIX_EPOCH,
            user(),
            None,
        );
        match storage
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: VersionKey::new(bucket, key, version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                value: version.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected version write event: {other:?}"),
        }
    }

    async fn write_live_obligation(storage: &StorageHandle, version_id: Ulid) {
        let Effect::Storage(effect) = build_live_obligation(
            node(1),
            auth_context(),
            "bucket".to_string(),
            "key".to_string(),
            version_id,
            false,
            None,
        )
        .expect("obligation effect builds") else {
            panic!("expected storage effect");
        };
        match storage.send_storage_effect(effect).await {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected obligation write event: {other:?}"),
        }
    }

    async fn read_obligations(storage: &StorageHandle) -> Vec<LiveObligationRecord> {
        match storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: REPLICATION_OBLIGATION_KEYSPACE.to_string(),
                prefix: None,
                start: None,
                limit: 16,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::IterResult { values, .. }) => values
                .into_iter()
                .map(|(_, value)| {
                    LiveObligationRecord::from_bytes(&value).expect("obligation decodes")
                })
                .collect(),
            other => panic!("unexpected storage event: {other:?}"),
        }
    }

    async fn repair_live(storage: &StorageHandle) {
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        process_live_obligations(&context)
            .await
            .expect("live obligation repairs");
    }

    #[tokio::test]
    async fn queue_persists_first() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let result = drive(QueueBlobOperation::new(on_demand_input(), None), &context)
            .await
            .expect("queue operation succeeds after durable write");

        assert_eq!(result.queued, 1);
        assert!(!result.scheduled);
        let jobs = read_jobs(&storage).await;
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].1.input.mode, ReplicationMode::OnDemand);
    }

    #[tokio::test]
    async fn duplicate_requests_coalesce() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        drive(QueueBlobOperation::new(on_demand_input(), None), &context)
            .await
            .expect("first queue succeeds");
        drive(QueueBlobOperation::new(on_demand_input(), None), &context)
            .await
            .expect("second queue succeeds");

        assert_eq!(read_jobs(&storage).await.len(), 1);
    }

    #[tokio::test]
    async fn enqueue_renews_id() {
        // Jobs built in one millisecond differ by id; an enqueue that writes the row again gets
        // a new one, while a preferred queued job keeps its own (`duplicate_preserves_retry`).
        let first = BlobJobRecord::new(on_demand_input(), None, 7);
        let second = BlobJobRecord::new(on_demand_input(), None, 7);
        assert_ne!(first.job_id, second.job_id);
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let queue = || drive(QueueBlobOperation::new(on_demand_input(), None), &context);
        queue().await.expect("first queue succeeds");
        let queued = read_jobs(&storage).await[0].1.job_id;
        queue().await.expect("second queue succeeds");
        let jobs = read_jobs(&storage).await;
        assert_eq!(jobs.len(), 1);
        assert_ne!(jobs[0].1.job_id, queued);
    }

    #[tokio::test]
    async fn retire_keeps_replaced() {
        // A retirement deletes the row only while it holds the retired job: a row an enqueue
        // wrote again during the run stays for its own run.
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let retired = BlobJobRecord::new(on_demand_input(), None, 7);
        let replaced = BlobJobRecord::new(on_demand_input(), None, 7);
        let key = blob_job_key(&retired).unwrap().to_vec();
        write_queue_record(
            &storage,
            REPLICATION_JOB_KEYSPACE,
            key.clone(),
            replaced.to_bytes().unwrap(),
        )
        .await;

        retire_job(&context, key.clone(), &retired, &BlobJobOutcome::Retired)
            .await
            .expect("retires");
        assert_eq!(
            read_jobs(&storage).await,
            vec![(key.clone(), replaced.clone())]
        );

        retire_job(&context, key, &replaced, &BlobJobOutcome::Retired)
            .await
            .expect("retires");
        assert!(read_jobs(&storage).await.is_empty());
    }

    fn plain_context(storage: &StorageHandle) -> DriverContext {
        DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        }
    }

    /// The plugin journal rows this node has not matched yet.
    async fn journaled(context: &DriverContext) -> Vec<SourceEvent> {
        let txn = Txn::direct(context);
        let pending = aruna_core::plugin_runs::PENDING_ROW;
        let page = crate::plugins::journal::journal_page(&txn, pending, None, 100).await;
        page.unwrap()
            .0
            .into_iter()
            .map(|(_, event)| event)
            .collect()
    }

    #[tokio::test]
    async fn completed_journals_once() {
        // A completed job journals one `sync_completed` row as it retires, also after a stopped
        // run had already stored the relationship's success; another drain adds none.
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let group_id = Ulid::from_parts(2, 2);
        write_bucket(&storage, "bucket").await;
        write_auth_docs(&storage, group_id).await;
        write_group(&storage, group_id).await;
        let mut relationship = relationship(80, 2, None, true);
        relationship.status.last_synced_at = Some(SystemTime::now());
        write_relationship(&storage, &relationship).await;
        let context = plain_context(&storage);
        let input = ReplicateScopeInput {
            target: ReplicateScopeTarget::Bucket,
            ..on_demand_input()
        };
        let queue = QueueBlobOperation::new_relationship(input, None, relationship.id);
        drive(queue, &context).await.expect("queue succeeds");
        let job_id = read_jobs(&storage).await[0].1.job_id;

        process_blob_batch(&context).await.expect("drain succeeds");
        process_blob_batch(&context).await.expect("drain succeeds");

        assert!(read_jobs(&storage).await.is_empty());
        let events = journaled(&context).await;
        assert_eq!(events.len(), 1);
        let event = &events[0];
        let path = object_permission_path(realm(), group_id, node(1), "bucket", "");
        assert_eq!(event.kind, SourceKind::SyncCompleted);
        assert_eq!((event.group_id, &event.path), (group_id, &path));
        assert_eq!(event.resource, relationship.id.to_string());
        assert_eq!(event.revision, job_id.to_string());
        assert_eq!(event.source, format!("sync_completed/{}/{job_id}", node(1)));
        assert_eq!(event.actor, Some(user()));
    }

    #[tokio::test]
    async fn retire_reports_rule() {
        // What a retirement journals, by outcome and by the relationship and bucket rows its
        // transaction reads. A failure before any relationship check, such as an undecodable
        // routing input, reaches retirement as `Failed` and follows the same rows.
        let failed = SyncState::Failed {
            reason: "access_denied".to_string(),
        };
        let (completed, failure) = (
            Some(SourceKind::SyncCompleted),
            Some(SourceKind::SyncFailed),
        );
        let cases = [
            (
                BlobJobOutcome::Completed,
                Some(SyncState::Enabled),
                true,
                completed,
            ),
            (
                BlobJobOutcome::Completed,
                Some(SyncState::Paused),
                true,
                completed,
            ),
            (
                BlobJobOutcome::Completed,
                Some(SyncState::Detached),
                true,
                None,
            ),
            (BlobJobOutcome::Completed, None, true, None),
            (
                BlobJobOutcome::Completed,
                Some(SyncState::Enabled),
                false,
                None,
            ),
            (
                BlobJobOutcome::Failed,
                Some(SyncState::Enabled),
                true,
                failure,
            ),
            (BlobJobOutcome::Failed, Some(SyncState::Paused), true, None),
            (BlobJobOutcome::Failed, Some(failed), true, None),
            (BlobJobOutcome::Failed, None, true, None),
            (
                BlobJobOutcome::Retired,
                Some(SyncState::Enabled),
                true,
                None,
            ),
        ];
        for (index, (outcome, state, bucket, expected)) in cases.into_iter().enumerate() {
            let temp_dir = tempdir().expect("temp dir");
            let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
                .expect("storage opens");
            let context = plain_context(&storage);
            if bucket {
                write_bucket(&storage, "bucket").await;
            }
            let mut link = relationship(90, 2, None, true);
            if let Some(state) = state {
                link.state = state;
                write_relationship(&storage, &link).await;
            }
            let job = BlobJobRecord::new_relationship(on_demand_input(), None, link.id, 7);
            write_blob_job(&storage, &job).await.expect("job writes");
            let key = blob_job_key(&job).unwrap().to_vec();

            retire_job(&context, key, &job, &outcome)
                .await
                .expect("retires");

            let kinds: Vec<_> = (journaled(&context).await.iter())
                .map(|event| event.kind)
                .collect();
            assert_eq!(kinds, Vec::from_iter(expected), "case {index}");
            assert!(read_jobs(&storage).await.is_empty(), "case {index}");
        }
        // A job without a relationship never reports.
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let context = plain_context(&storage);
        write_bucket(&storage, "bucket").await;
        let job = BlobJobRecord::new(on_demand_input(), None, 7);
        let key = blob_job_key(&job).unwrap().to_vec();
        retire_job(&context, key, &job, &BlobJobOutcome::Failed)
            .await
            .expect("retires");
        assert!(journaled(&context).await.is_empty());
    }

    #[tokio::test]
    async fn rejected_advance_journals() {
        // A reference advance on an enabled relationship that does not keep references is
        // refused before authorization; its retirement journals one job `sync_failed`.
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let context = plain_context(&storage);
        write_bucket(&storage, "bucket").await;
        let link = relationship(91, 2, None, true);
        write_relationship(&storage, &link).await;
        let job = BlobJobRecord::new_relationship(on_demand_input(), None, link.id, 0)
            .with_reference_advance(reference_advance());
        write_blob_job(&storage, &job).await.expect("job writes");

        let result = process_blob_batch(&context).await.expect("drain succeeds");

        assert_eq!(result.failed, 1);
        assert!(read_jobs(&storage).await.is_empty());
        let events = journaled(&context).await;
        let fields: Vec<_> = (events.iter())
            .map(|event| (event.kind, event.revision.clone()))
            .collect();
        assert_eq!(
            fields,
            vec![(SourceKind::SyncFailed, job.job_id.to_string())]
        );
        let path =
            object_permission_path(realm(), Ulid::from_parts(2, 2), node(1), "bucket", "key");
        assert_eq!(events[0].path, path);
    }

    #[test]
    fn queue_rejects_event() {
        let mut operation = QueueBlobOperation::new(on_demand_input(), None);
        operation.start();

        let effects = operation.step(Event::Search());

        assert!(effects.is_empty());
        assert!(operation.is_complete());
        assert!(matches!(
            operation.finalize(),
            Err(BlobQueueError::UnexpectedEvent(_))
        ));
    }

    #[tokio::test]
    async fn retry_preserves_fields() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let relationship_id = Ulid::from(77u128);
        let mut relationship = relationship(77, 2, None, true);
        relationship.target =
            ArunaArn::s3_object_prefix(realm(), node(2), "target-bucket", "mapped/").unwrap();
        write_relationship(&storage, &relationship).await;
        drive(
            QueueBlobOperation::new_relationship(on_demand_input(), None, relationship_id),
            &context,
        )
        .await
        .expect("queue succeeds");
        let queued = read_jobs(&storage).await[0].1.clone();

        let result = process_blob_batch(&context)
            .await
            .expect("drain stores retry metadata");

        assert_eq!(result.failed, 1);
        let jobs = read_jobs(&storage).await;
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].1.relationship_id, Some(relationship_id));
        assert_eq!(jobs[0].1.enqueued_at_ms, queued.enqueued_at_ms);
        assert_eq!(jobs[0].1.job_id, queued.job_id);
        assert_eq!(jobs[0].1.attempts, 1);
        let stored = read_relationships(&storage, "bucket")
            .await
            .unwrap()
            .into_iter()
            .find(|stored| stored.id == relationship_id)
            .unwrap();
        assert!(stored.status.last_error.is_some());
        assert_eq!(stored.status.counters.failures, 1);
        assert_eq!(stored.status.counters.consecutive_failures, 1);
    }

    #[tokio::test]
    async fn retry_keeps_advance() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let advance = reference_advance();
        let mut job =
            BlobJobRecord::new_relationship(on_demand_input(), None, Ulid::from_parts(78, 1), 42)
                .with_reference_advance(advance);
        job.writer_auth_context = None;
        let key = blob_job_key(&job).unwrap().to_vec();

        reschedule_blob_job(&storage, key, &job, "retry".to_string())
            .await
            .unwrap();

        let jobs = read_jobs(&storage).await;
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].1.reference_advance, Some(advance));
        assert_eq!(jobs[0].1.writer_auth_context, None);
    }

    #[tokio::test]
    async fn missing_relationship_skips() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        drive(
            QueueBlobOperation::new_relationship(on_demand_input(), None, Ulid::from(78u128)),
            &context,
        )
        .await
        .expect("queue succeeds");

        let result = process_blob_batch(&context)
            .await
            .expect("missing relationship is terminal");

        assert_eq!(result.succeeded, 1);
        assert!(read_jobs(&storage).await.is_empty());
    }

    #[tokio::test]
    async fn refused_member_retries() {
        // A member job that fails terminally stays queued, so a bootstrap never counts it
        // as delivered.
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let mut job = BlobJobRecord::new(on_demand_input(), None, 0).with_member(MemberTarget {
            set_id: Ulid::from_parts(3, 3),
            member_id: Ulid::from_parts(4, 4),
        });
        job.reference_advance = Some(reference_advance());
        let (key_space, key, value) = blob_job_entry(&job).unwrap();
        let write = StorageEffect::Write {
            key_space,
            key,
            value,
            txn_id: None,
        };
        storage.send_storage_effect(write).await;

        let result = process_blob_batch(&context).await.expect("batch runs");

        assert_eq!(result.failed, 1);
        let jobs = read_jobs(&storage).await;
        assert_eq!(jobs.len(), 1, "the refused member job stays queued");
        assert_eq!(jobs[0].1.attempts, 1);
    }

    #[tokio::test]
    async fn denied_job_fails() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let group_id = Ulid::from_parts(2, 2);
        write_bucket(&storage, "bucket").await;
        write_auth_docs(&storage, group_id).await;
        write_group(&storage, group_id).await;
        let path = object_permission_path(realm(), group_id, node(1), "bucket", "key");
        write_group_policy(&storage, group_id, &format!("path == '{path}'")).await;
        write_materialized_version(&storage, "bucket", "key", Ulid::from_parts(5, 5)).await;
        let relationship = relationship(79, 2, None, true);
        write_relationship(&storage, &relationship).await;
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        drive(
            QueueBlobOperation::new_relationship(on_demand_input(), None, relationship.id),
            &context,
        )
        .await
        .expect("queue succeeds");

        let result = process_blob_batch(&context)
            .await
            .expect("denied relationship is terminal");

        assert_eq!(result.succeeded, 0);
        assert_eq!(result.failed, 1);
        assert!(read_jobs(&storage).await.is_empty());
        let stored = read_relationships(&storage, "bucket")
            .await
            .unwrap()
            .into_iter()
            .find(|stored| stored.id == relationship.id)
            .unwrap();
        assert_eq!(
            stored.state,
            SyncState::Failed {
                reason: "access_denied".to_string()
            }
        );
        assert_eq!(stored.status.last_error.as_deref(), Some("access_denied"));
        assert_eq!(stored.status.counters.failures, 1);
        assert_eq!(stored.status.counters.consecutive_failures, 1);
        // The status write journaled the relationship's `sync_failed`; the job adds no row.
        let events = journaled(&context).await;
        assert_eq!(events.len(), 1);
        let transition = format!("sync_failed/{}/{}/", node(1), relationship.id);
        assert!(events[0].source.starts_with(&transition));
    }

    #[tokio::test]
    async fn stopped_status_recovers() {
        // The status write commits and the drain stops before retiring the job: the next drain
        // retires it as the relationship stands, without a second row.
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let context = plain_context(&storage);
        write_bucket(&storage, "bucket").await;
        let mut relationship = relationship(78, 2, None, true);
        write_relationship(&storage, &relationship).await;
        let queue = QueueBlobOperation::new_relationship(on_demand_input(), None, relationship.id);
        drive(queue, &context).await.expect("queue succeeds");
        relationship.state = SyncState::Failed {
            reason: "access_denied".to_string(),
        };
        assert!(store_sync_status(&context, &relationship).await.unwrap());
        assert_eq!(journaled(&context).await.len(), 1);

        let result = process_blob_batch(&context).await.expect("drain succeeds");

        assert_eq!((result.succeeded, result.failed), (1, 0));
        assert!(read_jobs(&storage).await.is_empty());
        assert_eq!(journaled(&context).await.len(), 1);
    }

    #[tokio::test]
    async fn success_updates_status() {
        // Queue actor must not replace the relationship creator.
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let group_id = Ulid::from_parts(2, 2);
        write_bucket(&storage, "bucket").await;
        write_auth_docs(&storage, group_id).await;
        write_group(&storage, group_id).await;
        let mut relationship = relationship(80, 2, None, true);
        relationship.status.last_error = Some("old error".to_string());
        relationship.status.counters.consecutive_failures = 2;
        write_relationship(&storage, &relationship).await;
        let queued_by = AuthContext {
            user_id: UserId::local(Ulid::from_parts(9, 9), realm()),
            realm_id: realm(),
            path_restrictions: None,
            session: None,
        };
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        drive(
            QueueBlobOperation::new_relationship(
                ReplicateScopeInput {
                    target: ReplicateScopeTarget::Bucket,
                    auth_context: queued_by,
                    ..on_demand_input()
                },
                None,
                relationship.id,
            ),
            &context,
        )
        .await
        .expect("queue succeeds");

        let result = process_blob_batch(&context)
            .await
            .expect("empty relationship scope succeeds");

        assert_eq!(result.succeeded, 1);
        assert!(read_jobs(&storage).await.is_empty());
        let stored = read_relationships(&storage, "bucket")
            .await
            .unwrap()
            .into_iter()
            .find(|stored| stored.id == relationship.id)
            .unwrap();
        assert!(stored.status.last_synced_at.is_some());
        assert_eq!(stored.status.last_error, None);
        assert_eq!(stored.status.counters.versions_synced, 0);
        assert_eq!(stored.status.counters.consecutive_failures, 0);
    }

    #[test]
    fn success_counts_bytes() {
        let mut relationship = relationship(81, 2, None, true);

        mark_success(&mut relationship, 2, 42);

        assert_eq!(relationship.status.counters.versions_synced, 2);
        assert_eq!(relationship.status.counters.bytes_synced, 42);
    }

    #[test]
    fn partial_counts_progress() {
        let mut relationship = relationship(82, 2, None, true);

        mark_progress(&mut relationship, 2, 42);
        mark_failure(&mut relationship, "one version failed");

        assert!(relationship.status.last_synced_at.is_some());
        assert_eq!(relationship.status.counters.versions_synced, 2);
        assert_eq!(relationship.status.counters.bytes_synced, 42);
        assert_eq!(relationship.status.counters.failures, 1);
        assert_eq!(relationship.status.counters.consecutive_failures, 1);
        assert_eq!(
            relationship.status.last_error.as_deref(),
            Some("one version failed")
        );
    }

    #[tokio::test]
    async fn revalidation_keeps_group() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        write_bucket(&storage, "bucket").await;
        let context = DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let relationship = relationship(83, 2, None, true);
        let source_bucket = relationship.source.bucket().unwrap();
        let creator = AuthContext {
            user_id: relationship.created_by,
            realm_id: relationship.source.realm_id,
            path_restrictions: None,
            session: None,
        };
        let Err((SourceAuthorizationError::Denied, group_id)) = load_source_authorization(
            &context,
            creator,
            relationship.source.node_id,
            source_bucket,
        )
        .await
        else {
            panic!("missing auth documents must fail revalidation")
        };

        assert_eq!(group_id, Some(Ulid::from_parts(2, 2)));
    }

    #[tokio::test]
    async fn deleted_status_skips() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let context = DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let stored = store_relationship(&context, relationship(84, 2, None, true))
            .await
            .expect("status check succeeds");

        assert!(!stored);
    }

    #[test]
    fn continuation_order() {
        let current = LiveReplicationContinuation {
            relationship_after: Some(vec![2]),
            relationships_complete: false,
            members_done: false,
        };
        let older = LiveReplicationContinuation {
            relationship_after: Some(vec![1]),
            ..current.clone()
        };
        let newer = LiveReplicationContinuation {
            relationship_after: Some(vec![3]),
            ..current.clone()
        };
        let complete = LiveReplicationContinuation {
            relationships_complete: true,
            ..current.clone()
        };
        assert!(!continuation_newer(&older, &current));
        assert!(continuation_newer(&newer, &current));
        assert!(continuation_newer(&complete, &current));
    }

    #[tokio::test]
    async fn duplicate_preserves_retry() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let due_at_ms = unix_timestamp_millis().saturating_add(60_000);
        let future_job = BlobJobRecord {
            input: on_demand_input(),
            source_delete_marker: None,
            due_at_ms,
            attempts: 1,
            last_error: Some("transient".to_string()),
            relationship_id: None,
            enqueued_at_ms: due_at_ms,
            origin: None,
            upstream_sources: Vec::new(),
            writer_auth_context: Some(auth_context()),
            reference_advance: None,
            distribution: None,
            job_id: Ulid::generate(),
        };
        let (key_space, key, value) = blob_job_entry(&future_job).unwrap();
        match storage
            .send_storage_effect(StorageEffect::Write {
                key_space,
                key,
                value,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected future job write event: {other:?}"),
        }
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        drive(QueueBlobOperation::new(on_demand_input(), None), &context)
            .await
            .expect("duplicate queue succeeds");

        let jobs = read_jobs(&storage).await;
        assert_eq!(
            jobs,
            vec![(blob_job_key(&future_job).unwrap().to_vec(), future_job)]
        );
    }

    #[test]
    fn job_requires_writer() {
        let mut record = BlobJobRecord::new(on_demand_input(), None, 42);
        record.writer_auth_context = None;
        let bytes = record.to_bytes().unwrap();
        assert!(BlobJobRecord::from_bytes(&bytes).is_err());
        record.reference_advance = Some(reference_advance());
        let bytes = record.to_bytes().unwrap();
        assert_eq!(BlobJobRecord::from_bytes(&bytes).unwrap(), record);

        let first = BlobJobRecord::new_relationship(on_demand_input(), None, Ulid::from(1u128), 42);
        let second =
            BlobJobRecord::new_relationship(on_demand_input(), None, Ulid::from(2u128), 42);
        assert_ne!(
            blob_job_key(&first).unwrap(),
            blob_job_key(&second).unwrap()
        );
        assert!(
            ReplicationItemError::from_peer_reason("access_denied")
                .failure
                .is_access_denied()
        );
        assert!(
            ReplicationItemError::from_peer_reason("writer_access_denied")
                .failure
                .is_writer_denied()
        );
        assert!(
            !ReplicationItemError::from_peer_reason("quota")
                .failure
                .is_denied()
        );
    }

    #[test]
    fn advance_auth_hops() {
        let mut input = on_demand_input();
        input.auth_context.path_restrictions = Some(vec![PathRestriction {
            pattern: "**".to_string(),
            permission: Permission::READ,
        }]);
        let reader = input.auth_context.clone();
        let creator = auth_context();
        let mut job = BlobJobRecord::new_relationship(input, None, Ulid::from_parts(3, 3), 42)
            .with_origin(Some(SyncOrigin {
                relationship_id: Ulid::from_parts(3, 3),
                hop_count: 0,
            }))
            .with_reference_advance(reference_advance());
        job.writer_auth_context = None;

        assert_eq!(job_source_auth(&job, &creator), &reader);
        job.origin.as_mut().unwrap().hop_count = 1;
        assert_eq!(job_source_auth(&job, &creator), &creator);
        assert_eq!(job.input.auth_context, reader);
        job.reference_advance = None;
        assert_eq!(job_source_auth(&job, &creator), &creator);
    }

    #[test]
    fn job_rejects_malformed() {
        // Greenfield: pre-advance record bytes decode strictly or not at all.
        let record = BlobJobRecord::new(on_demand_input(), None, 42);
        let encoded = record.to_bytes().unwrap();
        assert_eq!(BlobJobRecord::from_bytes(&encoded).unwrap(), record);

        let mut trailing = encoded.clone();
        trailing.push(0xff);
        assert!(BlobJobRecord::from_bytes(&trailing).is_err());
        let mut short = encoded;
        short.pop().unwrap();
        assert!(BlobJobRecord::from_bytes(&short).is_err());
        let mut truncated = record
            .with_reference_advance(reference_advance())
            .to_bytes()
            .unwrap();
        truncated.pop().unwrap();
        assert!(BlobJobRecord::from_bytes(&truncated).is_err());
    }

    #[test]
    fn obligation_rejects_malformed() {
        // Greenfield: pre-advance record bytes decode strictly or not at all.
        let record = LiveObligationRecord::new(
            node(1),
            auth_context(),
            "bucket".to_string(),
            "key".to_string(),
            Ulid::from_parts(2, 2),
            false,
        );
        let encoded = record.to_bytes().unwrap();
        assert_eq!(LiveObligationRecord::from_bytes(&encoded).unwrap(), record);

        let mut trailing = encoded.clone();
        trailing.push(0xff);
        assert!(LiveObligationRecord::from_bytes(&trailing).is_err());
        let mut short = encoded;
        short.pop().unwrap();
        assert!(LiveObligationRecord::from_bytes(&short).is_err());
        let mut truncated = record
            .with_reference_advance(reference_advance())
            .to_bytes()
            .unwrap();
        truncated.pop().unwrap();
        assert!(LiveObligationRecord::from_bytes(&truncated).is_err());
    }

    #[test]
    fn serves_source_realm() {
        // A link created by a user of another realm replicates in the realm it lives in.
        let foreign = RealmId::from_bytes([9; 32]);
        let mut link = relationship(10, 2, None, true);
        link.created_by = UserId::local(Ulid::from(30u128), foreign);
        let target = RelationshipJobTarget {
            bucket: "bucket",
            key: "key",
            version_id: Ulid::from(8u128),
            delete_marker: false,
        };
        let job = relationship_job(node(1), target, link, None, &[]).unwrap();
        assert_eq!(job.input.auth_context.realm_id, realm());
        assert_eq!(job.input.auth_context.user_id.realm_id, foreign);
    }

    #[test]
    fn hop_limit_stops() {
        let job = relationship_job(
            node(1),
            RelationshipJobTarget {
                bucket: "bucket",
                key: "key",
                version_id: Ulid::from(8u128),
                delete_marker: false,
            },
            relationship(10, 2, None, true),
            Some(&SyncOrigin {
                relationship_id: Ulid::from(9u128),
                hop_count: 4,
            }),
            &[],
        );

        assert!(job.is_none());
    }

    #[test]
    fn paused_link_stops() {
        // A paused relationship queues nothing until it is resumed.
        let mut paused = relationship(13, 2, None, true);
        paused.state = SyncState::Paused;

        let job = relationship_job(
            node(1),
            RelationshipJobTarget {
                bucket: "bucket",
                key: "key",
                version_id: Ulid::from(14u128),
                delete_marker: false,
            },
            paused,
            None,
            &[],
        );

        assert!(job.is_none());
    }

    #[test]
    fn reference_job_queues() {
        let mut reference = relationship(11, 2, Some("photos/"), true);
        reference.mode = SyncMode::Reference;
        reference.set_reference_handling(ReferenceHandling::Preserve);

        let job = relationship_job(
            node(1),
            RelationshipJobTarget {
                bucket: "bucket",
                key: "photos/image.jpg",
                version_id: Ulid::from(12u128),
                delete_marker: true,
            },
            reference,
            None,
            &[],
        )
        .unwrap();

        assert_eq!(job.relationship_id, Some(Ulid::from(11u128)));
        assert_eq!(job.source_delete_marker, Some(true));
    }

    #[test]
    fn cycle_target_stops() {
        let version_id = Ulid::from(20u128);
        let first = relationship_job(
            node(1),
            RelationshipJobTarget {
                bucket: "a",
                key: "key",
                version_id,
                delete_marker: false,
            },
            sync_link(21, 1, "a", 2, "b"),
            None,
            &[],
        )
        .unwrap();
        let second = relationship_job(
            node(2),
            RelationshipJobTarget {
                bucket: "b",
                key: "key",
                version_id,
                delete_marker: false,
            },
            sync_link(22, 2, "b", 3, "c"),
            first.origin.as_ref(),
            &first.upstream_sources,
        )
        .unwrap();

        let cycle = relationship_job(
            node(3),
            RelationshipJobTarget {
                bucket: "c",
                key: "key",
                version_id,
                delete_marker: false,
            },
            sync_link(23, 3, "c", 1, "a"),
            second.origin.as_ref(),
            &second.upstream_sources,
        );

        assert_eq!(first.upstream_sources.len(), 1);
        assert_eq!(second.upstream_sources.len(), 2);
        assert!(cycle.is_none());
    }

    #[test]
    fn distinct_prefix_allowed() {
        let upstream = ArunaArn::s3_object_prefix(realm(), node(1), "shared", "upstream/").unwrap();
        let mut relationship = sync_link(24, 2, "source", 1, "shared");
        relationship.target =
            ArunaArn::s3_object_prefix(realm(), node(1), "shared", "downstream/").unwrap();

        let job = relationship_job(
            node(2),
            RelationshipJobTarget {
                bucket: "source",
                key: "key",
                version_id: Ulid::from(25u128),
                delete_marker: false,
            },
            relationship,
            Some(&SyncOrigin {
                relationship_id: Ulid::from(23u128),
                hop_count: 1,
            }),
            &[upstream],
        );

        assert!(job.is_some());
    }

    #[tokio::test]
    async fn reverse_hop_suppressed() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let inbound_id = Ulid::from(11u128);
        let inbound = SyncRelationship {
            id: inbound_id,
            source: ArunaArn::s3_bucket(realm(), node(2), "upstream").unwrap(),
            target: ArunaArn::s3_bucket(realm(), node(1), "bucket").unwrap(),
            mode: SyncMode::Continuous,
            reference_handling: Default::default(),
            reference_serving: false,
            replicate_deletes: true,
            created_by: user(),
            created_at: SystemTime::UNIX_EPOCH,
            state: SyncState::Enabled,
            status: SyncStatusSnapshot::default(),
        };
        write_inbound(&storage, &inbound).await;
        let mut reverse = relationship(12, 2, None, true);
        reverse.target = ArunaArn::s3_bucket(realm(), node(2), "upstream").unwrap();
        let forward = relationship(13, 3, None, true);
        write_relationship(&storage, &reverse).await;
        write_relationship(&storage, &forward).await;
        let obligation = LiveObligationRecord::new(
            node(1),
            auth_context(),
            "bucket".to_string(),
            "key".to_string(),
            Ulid::from(14u128),
            false,
        )
        .with_origin(Some(SyncOrigin {
            relationship_id: inbound_id,
            hop_count: 0,
        }));

        let relationships = read_relationships_limit(
            &storage,
            &obligation.bucket,
            None,
            REPLICATION_RELATIONSHIP_LIMIT,
        )
        .await
        .unwrap();
        assert_eq!(
            write_live_jobs(&storage, &obligation, Some(&relationships), Vec::new())
                .await
                .unwrap()
                .queued,
            1
        );
        let jobs = read_jobs(&storage).await;
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].1.relationship_id, Some(forward.id));
        assert_eq!(
            jobs[0].1.origin,
            Some(SyncOrigin {
                relationship_id: forward.id,
                hop_count: 1,
            })
        );
        assert_eq!(
            jobs[0].1.upstream_sources,
            vec![inbound.source, forward.source]
        );
    }

    #[tokio::test]
    async fn relationship_prefix_filters() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let included = relationship(1, 2, Some("included/"), true);
        let excluded = relationship(2, 3, Some("excluded/"), true);
        let mut same_node = relationship(3, 1, Some("included/"), true);
        same_node.target = ArunaArn::s3_bucket(realm(), node(1), "same-node-target").unwrap();
        write_relationship(&storage, &included).await;
        write_relationship(&storage, &excluded).await;
        write_relationship(&storage, &same_node).await;
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let result = drive(
            LiveVersionOperation::new(LiveVersionInput {
                local_node_id: node(1),
                auth_context: auth_context(),
                bucket: "bucket".to_string(),
                key: "included/key".to_string(),
                version_id: Ulid::from_parts(31, 1),
                delete_marker: false,
            }),
            &context,
        )
        .await
        .expect("relationship queue succeeds");

        assert_eq!(result.queued, 0);
        assert_eq!(read_obligations(&storage).await.len(), 1);
        repair_live(&storage).await;
        let jobs = read_jobs(&storage).await;
        assert_eq!(jobs.len(), 2);
        assert!(jobs.iter().any(|(_, job)| {
            job.relationship_id == Some(included.id) && job.input.target_node_id == node(2)
        }));
        assert!(jobs.iter().any(|(_, job)| {
            job.origin
                == Some(SyncOrigin {
                    relationship_id: included.id,
                    hop_count: 0,
                })
        }));
        assert!(jobs.iter().any(|(_, job)| {
            job.relationship_id == Some(same_node.id) && job.input.target_node_id == node(1)
        }));
    }

    #[tokio::test]
    async fn relationship_delete_filters() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let excluded = relationship(3, 2, None, false);
        let included = relationship(4, 3, None, true);
        write_relationship(&storage, &excluded).await;
        write_relationship(&storage, &included).await;
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let result = drive(
            LiveVersionOperation::new(LiveVersionInput {
                local_node_id: node(1),
                auth_context: auth_context(),
                bucket: "bucket".to_string(),
                key: "key".to_string(),
                version_id: Ulid::from_parts(32, 1),
                delete_marker: true,
            }),
            &context,
        )
        .await
        .expect("relationship queue succeeds");

        assert_eq!(result.queued, 0);
        repair_live(&storage).await;
        let jobs = read_jobs(&storage).await;
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].1.relationship_id, Some(included.id));
        assert_eq!(jobs[0].1.source_delete_marker, Some(true));
    }

    #[tokio::test]
    async fn advance_filters_targets() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let reader = AuthContext {
            user_id: user(),
            realm_id: realm(),
            path_restrictions: Some(vec![PathRestriction {
                pattern: "**".to_string(),
                permission: Permission::READ,
            }]),
            session: None,
        };
        let skipped = relationship(6, 2, None, true);
        let mut included = relationship(7, 4, None, true);
        included.set_reference_handling(ReferenceHandling::Preserve);
        write_relationship(&storage, &skipped).await;
        write_relationship(&storage, &included).await;
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let advance = reference_advance();

        drive(
            LiveVersionOperation::new(LiveVersionInput {
                local_node_id: node(1),
                auth_context: reader.clone(),
                bucket: "bucket".to_string(),
                key: "key".to_string(),
                version_id: Ulid::from_parts(34, 1),
                delete_marker: false,
            })
            .with_reference_advance(advance),
            &context,
        )
        .await
        .expect("advance queue succeeds");

        let obligations = read_obligations(&storage).await;
        assert_eq!(obligations.len(), 1);
        assert_eq!(obligations[0].auth_context, reader);
        assert_eq!(obligations[0].reference_advance, Some(advance));
        repair_live(&storage).await;
        let jobs = read_jobs(&storage).await;
        assert_eq!(jobs.len(), 1);
        let job = &jobs[0].1;
        assert_eq!(job.relationship_id, Some(included.id));
        assert_eq!(job.input.auth_context, obligations[0].auth_context);
        assert_eq!(job.writer_auth_context, None);
        assert_eq!(job.reference_advance, Some(advance));
    }

    #[tokio::test]
    async fn live_queue_retry() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let version_id = Ulid::from_parts(33, 3);
        let link = relationship(34, 3, None, true);
        write_relationship(&storage, &link).await;
        let future_job = relationship_job(
            node(1),
            RelationshipJobTarget {
                bucket: "bucket",
                key: "key",
                version_id,
                delete_marker: true,
            },
            link,
            None,
            &[],
        )
        .expect("future job builds")
        .with_writer_auth(auth_context());
        let future_job = BlobJobRecord {
            due_at_ms: unix_timestamp_millis().saturating_add(60_000),
            attempts: 1,
            last_error: Some("transient".to_string()),
            ..future_job
        };
        let (key_space, key, value) = blob_job_entry(&future_job).unwrap();
        match storage
            .send_storage_effect(StorageEffect::Write {
                key_space,
                key,
                value,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected future job write event: {other:?}"),
        }
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let result = drive(
            LiveVersionOperation::new(LiveVersionInput {
                local_node_id: node(1),
                auth_context: auth_context(),
                bucket: "bucket".to_string(),
                key: "key".to_string(),
                version_id,
                delete_marker: true,
            }),
            &context,
        )
        .await
        .expect("live queue succeeds");

        repair_live(&storage).await;
        assert_eq!(result.queued, 0);
        let jobs = read_jobs(&storage).await;
        assert_eq!(
            jobs,
            vec![(blob_job_key(&future_job).unwrap().to_vec(), future_job)]
        );
    }

    #[tokio::test]
    async fn repair_continues_jobs() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        for id in 100..165 {
            write_relationship(&storage, &relationship(id, 2, None, true)).await;
        }
        write_live_obligation(&storage, Ulid::from_parts(91, 1)).await;

        repair_live(&storage).await;
        assert_eq!(read_jobs(&storage).await.len(), REPLICATION_JOB_LIMIT);
        assert_eq!(read_obligations(&storage).await.len(), 1);

        repair_live(&storage).await;
        assert_eq!(read_jobs(&storage).await.len(), 65);
        assert!(read_obligations(&storage).await.is_empty());
    }

    #[tokio::test]
    async fn relationship_page_caps() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let writes = (200..1225)
            .map(|id| {
                let relationship = relationship(id, 2, None, true);
                (
                    RELATIONSHIP_OUT_KEYSPACE.to_string(),
                    sync_relationship_key("bucket", relationship.id).into(),
                    relationship.to_bytes().unwrap().into(),
                )
            })
            .collect();
        match storage
            .send_storage_effect(StorageEffect::BatchWrite {
                writes,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::BatchWriteResult { .. }) => {}
            other => panic!("unexpected relationship batch write event: {other:?}"),
        }

        let first =
            read_relationships_limit(&storage, "bucket", None, REPLICATION_RELATIONSHIP_LIMIT)
                .await
                .unwrap();
        assert_eq!(first.values.len(), REPLICATION_RELATIONSHIP_LIMIT);
        let second = read_relationships_limit(
            &storage,
            "bucket",
            Some(first.next.clone().expect("relationship page continues")),
            REPLICATION_RELATIONSHIP_LIMIT,
        )
        .await
        .unwrap();
        assert_eq!(second.values.len(), 1);
        assert!(second.next.is_none());
    }

    #[tokio::test]
    async fn future_job_pending() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let due_at_ms = unix_timestamp_millis().saturating_add(60_000);
        let record = BlobJobRecord::new(on_demand_input(), None, due_at_ms);
        let (key_space, key, value) = blob_job_entry(&record).unwrap();
        match storage
            .send_storage_effect(StorageEffect::Write {
                key_space,
                key,
                value,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected job write event: {other:?}"),
        }
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let result = process_blob_batch(&context)
            .await
            .expect("future-only drain succeeds");

        assert_eq!(result.processed, 0);
        assert!(!result.has_more_due);
        assert!(result.next_due_after.is_some());
    }

    #[tokio::test]
    async fn past_due_settles() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        // A finished pass remembered a retry that has since run and left the queue empty.
        let stale = ReplicationScanCursor {
            generation: 1,
            after: None,
            next_due_ms: Some(1),
        };
        advance_replication_cursor(&storage, stale)
            .await
            .expect("stale cursor persists");
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        for _ in 0..2 {
            let result = process_blob_batch(&context)
                .await
                .expect("empty drain succeeds");
            assert!(!result.has_more_due);
            assert_eq!(
                result.next_due_after, None,
                "an empty queue must not re-arm"
            );
        }
    }

    #[tokio::test]
    async fn canonical_scan_once() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let job = BlobJobRecord::new(on_demand_input(), None, unix_timestamp_millis());
        write_blob_job(&storage, &job).await.expect("job writes");

        let before = storage.snapshot_metrics().requests_total;
        let scan = scan_due_jobs(&storage, unix_timestamp_millis(), 1)
            .await
            .expect("job scan succeeds");

        assert_eq!(scan.jobs.len(), 1);
        assert_eq!(
            storage.snapshot_metrics().requests_total - before,
            2,
            "canonical jobs need one bounded iteration"
        );
    }

    #[tokio::test]
    async fn cursor_wraps_queue() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let first = BlobJobRecord::new(on_demand_input(), None, unix_timestamp_millis());
        let mut second_input = on_demand_input();
        second_input.target = ReplicateScopeTarget::Object {
            key: "other".to_string(),
        };
        let second = BlobJobRecord::new(second_input, None, unix_timestamp_millis());
        write_blob_job(&storage, &first)
            .await
            .expect("first job writes");
        write_blob_job(&storage, &second)
            .await
            .expect("second job writes");

        let first_scan = scan_due_jobs(&storage, unix_timestamp_millis(), 1)
            .await
            .expect("first cursor scan succeeds");
        advance_replication_cursor(&storage, first_scan.next_cursor.clone())
            .await
            .expect("first cursor persists");
        let before = storage.snapshot_metrics().requests_total;
        let second_scan = scan_due_jobs(&storage, unix_timestamp_millis(), 1)
            .await
            .expect("second cursor scan succeeds");
        advance_replication_cursor(&storage, second_scan.next_cursor.clone())
            .await
            .expect("second cursor persists");
        assert_eq!(
            storage.snapshot_metrics().requests_total - before,
            6,
            "resumed scans use one page and one monotonic cursor commit"
        );
        assert_eq!(first_scan.jobs.len(), 1);
        assert_eq!(second_scan.jobs.len(), 1);
        assert_ne!(first_scan.jobs[0].0, second_scan.jobs[0].0);

        let wrapped = scan_due_jobs(&storage, unix_timestamp_millis(), 1)
            .await
            .expect("cursor wrap succeeds");
        assert!(wrapped.jobs.is_empty());
        assert!(!wrapped.has_more_due);
        advance_replication_cursor(&storage, wrapped.next_cursor)
            .await
            .expect("wrapped cursor persists");
        let restarted = scan_due_jobs(&storage, unix_timestamp_millis(), 1)
            .await
            .expect("wrapped scan restarts");
        assert_eq!(restarted.jobs.len(), 1);
    }

    #[tokio::test]
    async fn cursor_malformed_resets() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        write_queue_record(
            &storage,
            NODE_STATE_KEYSPACE,
            REPLICATION_CURSOR_KEY.to_vec(),
            vec![0xff],
        )
        .await;
        write_blob_job(
            &storage,
            &BlobJobRecord::new(on_demand_input(), None, unix_timestamp_millis()),
        )
        .await
        .expect("job writes");

        let scan = scan_due_jobs(&storage, unix_timestamp_millis(), 1)
            .await
            .expect("malformed cursor resets");
        assert_eq!(scan.jobs.len(), 1);
        advance_replication_cursor(&storage, scan.next_cursor)
            .await
            .expect("reset cursor persists");
        assert!(read_replication_cursor(&storage).await.is_ok());
    }

    #[tokio::test]
    async fn repairs_duplicate_job() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let job = BlobJobRecord::new(on_demand_input(), None, unix_timestamp_millis());
        let preferred = BlobJobRecord {
            attempts: 1,
            last_error: Some("retry".to_string()),
            ..job.clone()
        };
        write_blob_job(&storage, &job)
            .await
            .expect("canonical job writes");
        write_queue_record(
            &storage,
            REPLICATION_JOB_KEYSPACE,
            b"legacy-job".to_vec(),
            preferred.to_bytes().expect("job serializes"),
        )
        .await;

        let scan = scan_due_jobs(&storage, unix_timestamp_millis(), 2)
            .await
            .expect("duplicate repair succeeds");

        assert_eq!(scan.jobs.len(), 1);
        assert_eq!(scan.jobs[0].1.attempts, 1);
        assert_eq!(scan.jobs[0].0, blob_job_key(&preferred).unwrap().as_ref());
    }

    #[tokio::test]
    async fn relationship_reads_batch() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let relationship = relationship(90, 2, None, true);
        write_relationship(&storage, &relationship).await;
        let jobs = vec![
            (
                Vec::new(),
                BlobJobRecord::new_relationship(
                    on_demand_input(),
                    None,
                    relationship.id,
                    unix_timestamp_millis(),
                ),
            ),
            (
                Vec::new(),
                BlobJobRecord::new_relationship(
                    on_demand_input(),
                    None,
                    relationship.id,
                    unix_timestamp_millis(),
                ),
            ),
        ];

        let before = storage.snapshot_metrics().requests_total;
        let found = read_job_relationships(&storage, &jobs)
            .await
            .expect("relationship read succeeds");

        assert_eq!(found.len(), 1);
        assert_eq!(
            found[&("bucket".to_string(), relationship.id)],
            relationship
        );
        assert_eq!(
            storage.snapshot_metrics().requests_total - before,
            1,
            "duplicate jobs share one bounded relationship batch read"
        );
    }

    #[tokio::test]
    async fn deletes_corrupt_job() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        write_corrupt_job(&storage, "000-corrupt-blob-job").await;
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let result = process_blob_batch(&context)
            .await
            .expect("corrupt-only drain succeeds");

        assert_eq!(result.processed, 0);
        assert!(!result.has_more_due);
        assert!(result.next_due_after.is_none());
        assert!(!blob_jobs_exist(&storage).await.unwrap());
    }

    #[tokio::test]
    async fn corrupt_precedes_valid() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        write_corrupt_job(&storage, "000-corrupt-blob-job").await;
        drive(QueueBlobOperation::new(on_demand_input(), None), &context)
            .await
            .expect("queue succeeds");

        let result = process_blob_batch(&context)
            .await
            .expect("mixed corrupt/valid drain succeeds");

        assert_eq!(result.processed, 1);
        assert_eq!(result.failed, 1);
        let jobs = read_jobs(&storage).await;
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].1.attempts, 1);
    }

    #[tokio::test]
    async fn deletes_corrupt_obligation() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        write_corrupt_obligation(&storage, "000-corrupt-live-obligation").await;
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let result = process_blob_batch(&context)
            .await
            .expect("corrupt-only live drain succeeds");

        assert_eq!(result.processed, 0);
        assert!(!result.has_more_due);
        assert!(result.next_due_after.is_none());
        assert!(!blob_jobs_exist(&storage).await.unwrap());
    }

    #[tokio::test]
    async fn deletes_corrupt_obligations() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let version_id = Ulid::from_parts(5, 5);
        write_bucket(&storage, "bucket").await;
        write_relationship(&storage, &relationship(20, 2, None, true)).await;
        write_relationship(&storage, &relationship(21, 3, None, true)).await;
        write_materialized_version(&storage, "bucket", "key", version_id).await;
        for index in 0..OBLIGATION_BATCH_SIZE {
            let key = format!("000-corrupt-live-{index:03}");
            write_corrupt_obligation(&storage, &key).await;
        }
        write_live_obligation(&storage, version_id).await;
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let first = process_blob_batch(&context)
            .await
            .expect("corrupt live page drain succeeds");

        assert_eq!(first.processed, 0);
        assert!(first.has_more_due);
        assert_eq!(read_obligations(&storage).await.len(), 1);

        let second = process_blob_batch(&context)
            .await
            .expect("valid live obligation drains after corrupt page");

        assert_eq!(second.processed, 2);
        assert_eq!(second.failed, 2);
        assert!(read_obligations(&storage).await.is_empty());
    }

    /// Writes one obligation per key of `keys` in one batch.
    async fn write_obligations(storage: &StorageHandle, keys: impl Iterator<Item = String>) {
        let writes = keys
            .enumerate()
            .map(|(index, key)| {
                let id = Ulid::from_parts(1_700_000_000_000, index as u128);
                let obligation = LiveObligationRecord::new(
                    node(1),
                    auth_context(),
                    "bucket".to_string(),
                    key,
                    id,
                    false,
                );
                live_obligation_entry(&obligation).expect("obligation encodes")
            })
            .collect();
        let write = StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        };
        let event = storage.send_storage_effect(write).await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::BatchWriteResult { .. })
        ));
    }

    async fn obligation_keys(storage: &StorageHandle) -> Vec<String> {
        let iter = StorageEffect::Iter {
            key_space: REPLICATION_OBLIGATION_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit: 2 * OBLIGATION_BATCH_SIZE,
            txn_id: None,
        };
        let Event::Storage(StorageEvent::IterResult { values, .. }) =
            storage.send_storage_effect(iter).await
        else {
            panic!("obligations are readable");
        };
        let records = values
            .iter()
            .map(|(_, value)| LiveObligationRecord::from_bytes(value).expect("obligation decodes"));
        records.map(|obligation| obligation.key).collect()
    }

    #[tokio::test]
    async fn obligation_scan_rotates() {
        // A pass continues after the last obligation the previous one read, so obligations that
        // sort first and keep arriving never hold back later ones.
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        write_bucket(&storage, "bucket").await;
        let late = (0..=OBLIGATION_BATCH_SIZE).map(|index| format!("z/{index:05}"));
        write_obligations(&storage, late).await;
        repair_live(&storage).await;
        let last = format!("z/{OBLIGATION_BATCH_SIZE:05}");
        assert_eq!(obligation_keys(&storage).await, vec![last]);
        let early = (0..OBLIGATION_BATCH_SIZE).map(|index| format!("a/{index:05}"));
        write_obligations(&storage, early).await;
        repair_live(&storage).await;
        let keys = obligation_keys(&storage).await;
        assert_eq!(
            keys.len(),
            OBLIGATION_BATCH_SIZE,
            "the later one went first"
        );
        assert!(keys.iter().all(|key| key.starts_with("a/")));
        // The next pass wraps to the start.
        repair_live(&storage).await;
        assert!(obligation_keys(&storage).await.is_empty());
    }

    #[test]
    fn lag_dates_obligation() {
        // A retried job's lag counts from when its version was written, not from its due time.
        let version_id = Ulid::from_parts(1_000, 7);
        let mut input = on_demand_input();
        input.target = ReplicateScopeTarget::Version {
            key: "key".to_string(),
            version_id,
        };
        let job = BlobJobRecord::new(input, Some(false), 9_000);
        assert_eq!(job.owed_since_ms(), 1_000);
    }

    // A large import into a bucket nothing replicates to leaves one obligation per file; they
    // are retired in one pass with a fixed number of requests, not one transaction each.
    #[tokio::test]
    async fn idle_obligations_retired() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        write_bucket(&storage, "bucket").await;
        for index in 0..300u64 {
            let Effect::Storage(effect) = build_live_obligation(
                node(1),
                auth_context(),
                "bucket".to_string(),
                format!("key-{index:03}"),
                Ulid::from_parts(index, 1),
                false,
                None,
            )
            .expect("obligation effect builds") else {
                panic!("expected storage effect");
            };
            storage.send_storage_effect(effect).await;
        }

        let before = storage.snapshot_metrics().requests_total;
        repair_live(&storage).await;

        assert!(storage.snapshot_metrics().requests_total - before < 16);
        let remaining = storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: REPLICATION_OBLIGATION_KEYSPACE.to_string(),
                prefix: None,
                start: None,
                limit: 1,
                txn_id: None,
            })
            .await;
        assert!(matches!(
            remaining,
            Event::Storage(StorageEvent::IterResult { values, .. }) if values.is_empty()
        ));
        assert!(read_jobs(&storage).await.is_empty());
    }

    // Retiring keeps a stored job that is retrying, whether its range holds only the queued jobs
    // or other jobs too, and keeps an obligation whose continuation changed since it was read.
    #[tokio::test]
    async fn retire_keeps_retries() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let job = |key: &str, attempts: u32| {
            let target = ReplicateScopeTarget::Object {
                key: key.to_string(),
            };
            let input = ReplicateScopeInput {
                target,
                ..on_demand_input()
            };
            let mut job = BlobJobRecord::new(input, None, 5);
            job.attempts = attempts;
            job
        };
        let obligation = |key: &str| {
            LiveObligationRecord::new(
                node(1),
                auth_context(),
                "bucket".to_string(),
                key.to_string(),
                Ulid::from_parts(1, 1),
                false,
            )
        };
        let mut continued = obligation("continued");
        continued.continuation = Some(LiveReplicationContinuation {
            relationships_complete: true,
            ..Default::default()
        });
        for record in [obligation("fresh"), continued.clone()] {
            super::write_live_obligation(&storage, &record)
                .await
                .unwrap();
        }
        write_blob_job(&storage, &job("a", 3)).await.unwrap();
        let retired = vec![
            (
                live_obligation_key(&obligation("fresh")).unwrap().to_vec(),
                None,
            ),
            (
                live_obligation_key(&continued).unwrap().to_vec(),
                Some(LiveReplicationContinuation::default()),
            ),
        ];
        let queued = || vec![job("a", 0), job("c", 0)];
        retire_obligations(&storage, retired, queued(), (None, None))
            .await
            .unwrap();
        write_blob_job(&storage, &job("b", 0)).await.unwrap();
        write_blob_job(&storage, &job("c", 2)).await.unwrap();
        retire_obligations(&storage, Vec::new(), queued(), (None, None))
            .await
            .unwrap();

        let attempts: Vec<_> = (read_jobs(&storage).await.into_iter())
            .map(|(_, job)| job.attempts)
            .collect();
        assert_eq!(attempts, [3, 0, 2], "retrying jobs a and c are kept");
        let remaining = storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: REPLICATION_OBLIGATION_KEYSPACE.to_string(),
                prefix: None,
                start: None,
                limit: 8,
                txn_id: None,
            })
            .await;
        let Event::Storage(StorageEvent::IterResult { values, .. }) = remaining else {
            panic!("obligations are readable");
        };
        let kept: Vec<_> = (values.iter())
            .map(|(_, value)| LiveObligationRecord::from_bytes(value).unwrap().key)
            .collect();
        assert_eq!(kept, ["continued"], "only the changed obligation stays");
    }

    #[tokio::test]
    async fn hop_limit_retires() {
        // A hop-exhausted obligation is never scanned for relationships, so it
        // must be deleted instead of blocking the head of the scan forever.
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let record = LiveObligationRecord::new(
            node(1),
            auth_context(),
            "bucket".to_string(),
            "key".to_string(),
            Ulid::from_parts(6, 6),
            false,
        )
        .with_origin(Some(SyncOrigin {
            relationship_id: Ulid::from(9u128),
            hop_count: 4,
        }));
        super::write_live_obligation(&storage, &record)
            .await
            .expect("obligation persists");

        repair_live(&storage).await;

        assert!(read_obligations(&storage).await.is_empty());
    }

    #[tokio::test]
    async fn delete_keeps_requeued() {
        // A repair pass must not delete an obligation a concurrent writer
        // re-enqueued: the newer version would never replicate.
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let mut record = LiveObligationRecord::new(
            node(1),
            auth_context(),
            "bucket".to_string(),
            "key".to_string(),
            Ulid::from_parts(7, 7),
            false,
        );
        record.continuation = Some(LiveReplicationContinuation::default());
        super::write_live_obligation(&storage, &record)
            .await
            .expect("obligation persists");
        let key = live_obligation_key(&record)
            .expect("obligation key builds")
            .to_vec();
        let observed = LiveReplicationContinuation {
            relationship_after: Some(vec![1]),
            relationships_complete: false,
            members_done: false,
        };

        delete_live_obligation(&storage, key.clone(), Some(&observed))
            .await
            .expect("stale delete is a no-op");
        assert_eq!(read_obligations(&storage).await.len(), 1);

        delete_live_obligation(&storage, key, record.continuation.as_ref())
            .await
            .expect("matching delete succeeds");
        assert!(read_obligations(&storage).await.is_empty());
    }

    #[tokio::test]
    async fn stale_write_ignored() {
        // Rewinding the stored scan position would re-queue work the drain
        // already finished, so an older continuation must not win.
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let mut record = LiveObligationRecord::new(
            node(1),
            auth_context(),
            "bucket".to_string(),
            "key".to_string(),
            Ulid::from_parts(8, 8),
            false,
        );
        record.continuation = Some(LiveReplicationContinuation {
            relationship_after: Some(vec![2]),
            relationships_complete: false,
            members_done: false,
        });
        super::write_live_obligation(&storage, &record)
            .await
            .expect("obligation persists");
        let mut stale = record.clone();
        stale.continuation = Some(LiveReplicationContinuation {
            relationship_after: Some(vec![1]),
            relationships_complete: false,
            members_done: false,
        });

        super::write_live_obligation(&storage, &stale)
            .await
            .expect("stale write is a no-op");

        let stored = read_obligations(&storage).await;
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].continuation, record.continuation);
    }

    #[tokio::test]
    async fn denied_standalone_drops() {
        // A job without a relationship revalidates its own writer; a denied
        // writer must retire the job instead of retrying it forever.
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let group_id = Ulid::from_parts(2, 2);
        write_bucket(&storage, "bucket").await;
        write_auth_docs(&storage, group_id).await;
        write_group(&storage, group_id).await;
        write_materialized_version(&storage, "bucket", "key", Ulid::from_parts(5, 5)).await;
        let net_handle = NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().expect("bind addr"),
                discovery_method: DiscoveryMethod::None,
                relay_method: RelayMethod::None,
                ..NetConfig::default()
            },
            storage.clone(),
        )
        .await
        .expect("net handle starts");
        let path = object_permission_path(realm(), group_id, net_handle.node_id(), "bucket", "key");
        write_group_policy(&storage, group_id, &format!("path == '{path}'")).await;
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: Some(net_handle),
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        drive(QueueBlobOperation::new(on_demand_input(), None), &context)
            .await
            .expect("queue succeeds");

        let result = process_blob_batch(&context)
            .await
            .expect("denied writer is terminal");

        assert_eq!(result.succeeded, 0);
        assert_eq!(result.failed, 1);
        assert!(read_jobs(&storage).await.is_empty());
    }

    #[tokio::test]
    async fn live_obligation_repairs() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let version_id = Ulid::from_parts(4, 4);
        write_bucket(&storage, "bucket").await;
        write_relationship(&storage, &relationship(22, 2, None, true)).await;
        write_relationship(&storage, &relationship(23, 3, None, true)).await;
        write_materialized_version(&storage, "bucket", "key", version_id).await;
        write_live_obligation(&storage, version_id).await;

        repair_live(&storage).await;

        assert!(read_obligations(&storage).await.is_empty());
        let jobs = read_jobs(&storage).await;
        assert_eq!(jobs.len(), 2);
        assert!(jobs.iter().all(|(_, job)| job.relationship_id.is_some()));
    }

    #[tokio::test]
    async fn failed_drain_retains() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        drive(QueueBlobOperation::new(on_demand_input(), None), &context)
            .await
            .expect("queue succeeds");

        let result = process_blob_batch(&context)
            .await
            .expect("drain stores retry metadata");

        assert_eq!(result.failed, 1);
        let jobs = read_jobs(&storage).await;
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].1.attempts, 1);
        assert!(jobs[0].1.last_error.is_some());
    }

    #[tokio::test]
    async fn empty_drain_deletes() {
        // Standalone jobs revalidate the writer against the local node.
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        write_bucket(&storage, "bucket").await;
        write_auth_docs(&storage, Ulid::from_parts(2, 2)).await;
        write_group(&storage, Ulid::from_parts(2, 2)).await;
        let net_handle = NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().expect("bind addr"),
                discovery_method: DiscoveryMethod::None,
                relay_method: RelayMethod::None,
                ..NetConfig::default()
            },
            storage.clone(),
        )
        .await
        .expect("net handle starts");
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: Some(net_handle),
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        drive(
            QueueBlobOperation::new(
                ReplicateScopeInput {
                    target: ReplicateScopeTarget::Bucket,
                    ..on_demand_input()
                },
                None,
            ),
            &context,
        )
        .await
        .expect("queue succeeds");

        let result = process_blob_batch(&context).await.expect("drain succeeds");

        assert_eq!(result.succeeded, 1);
        assert!(read_jobs(&storage).await.is_empty());
    }

    #[test]
    fn job_stats_pages() {
        use aruna_core::operation_check::{
            check_abort, rejects_events, rejects_finalize, sample_events,
        };
        let relationship = Ulid::from_parts(7, 7);
        let row = |relationship_id, enqueued_at_ms| {
            let mut record = BlobJobRecord::new(on_demand_input(), None, enqueued_at_ms);
            record.relationship_id = relationship_id;
            (
                Key::from(b"row".to_vec()),
                Value::from(record.to_bytes().unwrap()),
            )
        };
        let mut operation = JobStatsOperation::new(relationship);
        assert_eq!(operation.start().len(), 1);
        let first = Event::Storage(StorageEvent::IterResult {
            values: vec![row(Some(relationship), 30), row(None, 5)],
            next_start_after: Some(b"row".to_vec().into()),
        });
        assert_eq!(operation.step(first).len(), 1);
        let last = Event::Storage(StorageEvent::IterResult {
            values: vec![row(Some(relationship), 20)],
            next_start_after: None,
        });
        assert!(operation.step(last).is_empty());
        assert_eq!(operation.finalize(), Ok((2, Some(20))));
        let reading = || {
            let mut operation = JobStatsOperation::new(relationship);
            operation.start();
            operation
        };
        let refused = rejects_events(reading, sample_events, |event| {
            matches!(event, Event::Storage(StorageEvent::IterResult { .. }))
        });
        assert_eq!(refused.len(), sample_events().len() - 1);
        let mut open = reading();
        check_abort(&mut open, &[]);
        rejects_finalize(open);
    }
}
