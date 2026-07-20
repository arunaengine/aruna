use std::time::{Duration, Instant, SystemTime};

use aruna_core::NodeId;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{
    BLOB_LIVE_REPLICATION_OBLIGATION_KEYSPACE, BLOB_REPLICATION_JOB_KEYSPACE,
    S3_BUCKET_REPLICATION_KEYSPACE, SYNC_RELATIONSHIP_IN_KEYSPACE, SYNC_RELATIONSHIP_OUT_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::{
    ArunaArn, AuthContext, BucketReplicationConfig, Permission, SyncMode, SyncRelationship,
    SyncState, WatchEvent, WatchEventDetail, WatchEventKind, blob_bucket_permission_path,
    data_watch_resource_path, sync_relationship_key, sync_relationship_prefix,
};
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::telemetry::duration_ms;
use aruna_core::types::{Effects, GroupId, Key};
use aruna_core::util::unix_timestamp_millis;
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;
use byteview::ByteView;
use serde::{Deserialize, Serialize};
use smallvec::smallvec;
use thiserror::Error;
use tracing::{info, warn};
use ulid::Ulid;

use super::protocol::{ReplicationMode, SyncOrigin};
use super::version_replication::{
    ReplicateScopeError, ReplicateScopeInput, ReplicateScopeOperation, ReplicateScopeTarget,
};
use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::driver::{DriverContext, drive};
use crate::notifications::watch::emit::emit_resource_watch_event;
use crate::queue_backoff::queue_retry_after_ms;
use crate::s3::get_bucket_info::GetBucketInfoOperation;
use crate::sync_mirror_repair::{kick_mirror_repair, store_sync_status};

const REPLICATION_SCAN_PAGE_SIZE: usize = 512;
const REPLICATION_BATCH_SIZE: usize = 64;
const RELATIONSHIP_STATS_PAGE_SIZE: usize = 256;
const LIVE_REPLICATION_OBLIGATION_BATCH_SIZE: usize = 64;

pub const BLOB_REPLICATION_POLL_AFTER: Duration = Duration::from_secs(5);
pub const BLOB_REPLICATION_RETRY_AFTER: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobReplicationJobRecord {
    pub input: ReplicateScopeInput,
    pub source_delete_marker: Option<bool>,
    pub due_at_ms: u64,
    pub attempts: u32,
    pub last_error: Option<String>,
    pub relationship_id: Option<Ulid>,
    pub enqueued_at_ms: u64,
    pub origin: Option<SyncOrigin>,
    pub upstream_sources: Vec<ArunaArn>,
    pub writer_auth_context: Option<AuthContext>,
}

#[derive(Deserialize, Serialize)]
struct OriginBlobReplicationJobRecord {
    input: ReplicateScopeInput,
    source_delete_marker: Option<bool>,
    due_at_ms: u64,
    attempts: u32,
    last_error: Option<String>,
    relationship_id: Option<Ulid>,
    enqueued_at_ms: u64,
    origin: Option<SyncOrigin>,
}

#[derive(Deserialize, Serialize)]
struct PreviousBlobReplicationJobRecord {
    input: ReplicateScopeInput,
    source_delete_marker: Option<bool>,
    due_at_ms: u64,
    attempts: u32,
    last_error: Option<String>,
    relationship_id: Option<Ulid>,
    enqueued_at_ms: u64,
}

#[derive(Deserialize, Serialize)]
struct LegacyBlobReplicationJobRecord {
    input: ReplicateScopeInput,
    source_delete_marker: Option<bool>,
    due_at_ms: u64,
    attempts: u32,
    last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueBlobReplicationResult {
    pub queued: usize,
    pub scheduled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobReplicationDrainResult {
    pub processed: usize,
    pub succeeded: usize,
    pub failed: usize,
    pub has_more_due: bool,
    pub next_due_after: Option<Duration>,
}

#[derive(Debug, Error, PartialEq)]
pub enum BlobReplicationQueueError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Replication(#[from] ReplicateScopeError),
    #[error("unexpected event while processing blob replication queue: {0}")]
    UnexpectedEvent(String),
}

#[derive(Serialize)]
struct BlobReplicationJobIdentity<'a> {
    mode: ReplicationMode,
    bucket: &'a str,
    target: &'a ReplicateScopeTarget,
    target_node_id: &'a NodeId,
    source_delete_marker: Option<bool>,
    relationship_id: Option<Ulid>,
    origin: Option<&'a SyncOrigin>,
    upstream_sources: &'a [ArunaArn],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveReplicationObligationRecord {
    pub local_node_id: NodeId,
    pub auth_context: AuthContext,
    pub bucket: String,
    pub key: String,
    pub version_id: Ulid,
    pub delete_marker: bool,
    pub origin: Option<SyncOrigin>,
    pub upstream_sources: Vec<ArunaArn>,
}

#[derive(Deserialize, Serialize)]
struct OriginLiveReplicationObligationRecord {
    local_node_id: NodeId,
    auth_context: AuthContext,
    bucket: String,
    key: String,
    version_id: Ulid,
    delete_marker: bool,
    origin: Option<SyncOrigin>,
}

#[derive(Deserialize, Serialize)]
struct LegacyLiveReplicationObligationRecord {
    local_node_id: NodeId,
    auth_context: AuthContext,
    bucket: String,
    key: String,
    version_id: Ulid,
    delete_marker: bool,
}

#[derive(Serialize)]
struct LiveReplicationObligationIdentity<'a> {
    bucket: &'a str,
    key: &'a str,
    version_id: Ulid,
}

struct BlobReplicationJobScan {
    jobs: Vec<(Vec<u8>, BlobReplicationJobRecord)>,
    has_more_due: bool,
    next_due_at_ms: Option<u64>,
}

enum BlobReplicationJobOutcome {
    Succeeded,
    TerminalFailure,
}

#[derive(Default)]
struct LiveReplicationRepairResult {
    processed: usize,
    queued: usize,
    has_more: bool,
}

impl BlobReplicationJobRecord {
    pub fn new(
        input: ReplicateScopeInput,
        source_delete_marker: Option<bool>,
        due_at_ms: u64,
    ) -> Self {
        Self {
            input,
            source_delete_marker,
            due_at_ms,
            attempts: 0,
            last_error: None,
            relationship_id: None,
            enqueued_at_ms: due_at_ms,
            origin: None,
            upstream_sources: Vec::new(),
            writer_auth_context: None,
        }
    }

    pub fn new_relationship(
        input: ReplicateScopeInput,
        source_delete_marker: Option<bool>,
        relationship_id: Ulid,
        due_at_ms: u64,
    ) -> Self {
        Self {
            relationship_id: Some(relationship_id),
            ..Self::new(input, source_delete_marker, due_at_ms)
        }
    }

    pub fn with_origin(mut self, origin: Option<SyncOrigin>) -> Self {
        self.origin = origin;
        self
    }

    pub fn with_sources(mut self, upstream_sources: Vec<ArunaArn>) -> Self {
        self.upstream_sources = upstream_sources;
        self
    }

    pub fn with_writer_auth(mut self, auth_context: AuthContext) -> Self {
        self.writer_auth_context = Some(auth_context);
        self
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        match postcard::from_bytes(bytes) {
            Ok(record) => Ok(record),
            Err(postcard::Error::DeserializeUnexpectedEnd) => {
                let mut previous = bytes.to_vec();
                previous.extend(postcard::to_allocvec(&Option::<AuthContext>::None)?);
                if let Ok(record) = postcard::from_bytes(&previous) {
                    return Ok(record);
                }
                if let Ok(previous) = postcard::from_bytes::<OriginBlobReplicationJobRecord>(bytes)
                {
                    return Ok(Self {
                        input: previous.input,
                        source_delete_marker: previous.source_delete_marker,
                        due_at_ms: previous.due_at_ms,
                        attempts: previous.attempts,
                        last_error: previous.last_error,
                        relationship_id: previous.relationship_id,
                        enqueued_at_ms: previous.enqueued_at_ms,
                        origin: previous.origin,
                        upstream_sources: Vec::new(),
                        writer_auth_context: None,
                    });
                }
                if let Ok(previous) =
                    postcard::from_bytes::<PreviousBlobReplicationJobRecord>(bytes)
                {
                    return Ok(Self {
                        input: previous.input,
                        source_delete_marker: previous.source_delete_marker,
                        due_at_ms: previous.due_at_ms,
                        attempts: previous.attempts,
                        last_error: previous.last_error,
                        relationship_id: previous.relationship_id,
                        enqueued_at_ms: previous.enqueued_at_ms,
                        origin: None,
                        upstream_sources: Vec::new(),
                        writer_auth_context: None,
                    });
                }
                let legacy: LegacyBlobReplicationJobRecord = postcard::from_bytes(bytes)?;
                Ok(Self {
                    input: legacy.input,
                    source_delete_marker: legacy.source_delete_marker,
                    due_at_ms: legacy.due_at_ms,
                    attempts: legacy.attempts,
                    last_error: legacy.last_error,
                    relationship_id: None,
                    enqueued_at_ms: legacy.due_at_ms,
                    origin: None,
                    upstream_sources: Vec::new(),
                    writer_auth_context: None,
                })
            }
            Err(error) => Err(error.into()),
        }
    }
}

impl LiveReplicationObligationRecord {
    pub fn new(
        local_node_id: NodeId,
        auth_context: AuthContext,
        bucket: String,
        key: String,
        version_id: Ulid,
        delete_marker: bool,
    ) -> Self {
        Self {
            local_node_id,
            auth_context,
            bucket,
            key,
            version_id,
            delete_marker,
            origin: None,
            upstream_sources: Vec::new(),
        }
    }

    pub fn with_origin(mut self, origin: Option<SyncOrigin>) -> Self {
        self.origin = origin;
        self
    }

    pub fn with_sources(mut self, upstream_sources: Vec<ArunaArn>) -> Self {
        self.upstream_sources = upstream_sources;
        self
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        match postcard::from_bytes(bytes) {
            Ok(record) => Ok(record),
            Err(postcard::Error::DeserializeUnexpectedEnd) => {
                if let Ok(previous) =
                    postcard::from_bytes::<OriginLiveReplicationObligationRecord>(bytes)
                {
                    return Ok(Self {
                        local_node_id: previous.local_node_id,
                        auth_context: previous.auth_context,
                        bucket: previous.bucket,
                        key: previous.key,
                        version_id: previous.version_id,
                        delete_marker: previous.delete_marker,
                        origin: previous.origin,
                        upstream_sources: Vec::new(),
                    });
                }
                let legacy: LegacyLiveReplicationObligationRecord = postcard::from_bytes(bytes)?;
                Ok(Self {
                    local_node_id: legacy.local_node_id,
                    auth_context: legacy.auth_context,
                    bucket: legacy.bucket,
                    key: legacy.key,
                    version_id: legacy.version_id,
                    delete_marker: legacy.delete_marker,
                    origin: None,
                    upstream_sources: Vec::new(),
                })
            }
            Err(error) => Err(error.into()),
        }
    }
}

pub fn blob_replication_job_key(record: &BlobReplicationJobRecord) -> Result<Key, ConversionError> {
    let identity = BlobReplicationJobIdentity {
        mode: record.input.mode,
        bucket: &record.input.bucket,
        target: &record.input.target,
        target_node_id: &record.input.target_node_id,
        source_delete_marker: record.source_delete_marker,
        relationship_id: record.relationship_id,
        origin: record.origin.as_ref(),
        upstream_sources: &record.upstream_sources,
    };
    let mut key = b"v1".to_vec();
    key.extend(postcard::to_allocvec(&identity)?);
    Ok(ByteView::from(key))
}

fn blob_replication_job_write_entry(
    record: &BlobReplicationJobRecord,
) -> Result<(String, Key, ByteView), ConversionError> {
    Ok((
        BLOB_REPLICATION_JOB_KEYSPACE.to_string(),
        blob_replication_job_key(record)?,
        ByteView::from(record.to_bytes()?),
    ))
}

fn blob_replication_job_preferred(
    candidate: &BlobReplicationJobRecord,
    current: &BlobReplicationJobRecord,
) -> bool {
    (candidate.attempts, candidate.due_at_ms) > (current.attempts, current.due_at_ms)
}

pub fn live_replication_obligation_key(
    record: &LiveReplicationObligationRecord,
) -> Result<Key, ConversionError> {
    let identity = LiveReplicationObligationIdentity {
        bucket: &record.bucket,
        key: &record.key,
        version_id: record.version_id,
    };
    let mut key = b"v1".to_vec();
    key.extend(postcard::to_allocvec(&identity)?);
    Ok(ByteView::from(key))
}

fn live_replication_obligation_write_entry(
    record: &LiveReplicationObligationRecord,
) -> Result<(String, Key, ByteView), ConversionError> {
    Ok((
        BLOB_LIVE_REPLICATION_OBLIGATION_KEYSPACE.to_string(),
        live_replication_obligation_key(record)?,
        ByteView::from(record.to_bytes()?),
    ))
}

pub(crate) fn live_obligation_effect(
    record: LiveReplicationObligationRecord,
    txn_id: Option<Ulid>,
) -> Result<Effect, ConversionError> {
    let (key_space, key, value) = live_replication_obligation_write_entry(&record)?;
    Ok(Effect::Storage(StorageEffect::Write {
        key_space,
        key,
        value,
        txn_id,
    }))
}

pub fn write_live_replication_obligation_effect(
    local_node_id: NodeId,
    auth_context: AuthContext,
    bucket: String,
    key: String,
    version_id: Ulid,
    delete_marker: bool,
    txn_id: Option<Ulid>,
) -> Result<Effect, ConversionError> {
    let record = LiveReplicationObligationRecord::new(
        local_node_id,
        auth_context,
        bucket,
        key,
        version_id,
        delete_marker,
    );
    live_obligation_effect(record, txn_id)
}

pub fn schedule_blob_replication_drain_effect() -> Effect {
    Effect::Task(TaskEffect::ResetTimer {
        key: TaskKey::DrainBlobReplicationQueue,
        after: Duration::ZERO,
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum QueueBlobReplicationState {
    Init,
    ReadExisting,
    WriteJob,
    ScheduleDrain,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct QueueBlobReplicationOperation {
    job: BlobReplicationJobRecord,
    state: QueueBlobReplicationState,
    output: Option<Result<QueueBlobReplicationResult, BlobReplicationQueueError>>,
}

impl QueueBlobReplicationOperation {
    pub fn new(input: ReplicateScopeInput, source_delete_marker: Option<bool>) -> Self {
        Self {
            job: BlobReplicationJobRecord::new(
                input,
                source_delete_marker,
                unix_timestamp_millis(),
            ),
            state: QueueBlobReplicationState::Init,
            output: None,
        }
    }

    pub fn new_relationship(
        input: ReplicateScopeInput,
        source_delete_marker: Option<bool>,
        relationship_id: Ulid,
    ) -> Self {
        Self {
            job: BlobReplicationJobRecord::new_relationship(
                input,
                source_delete_marker,
                relationship_id,
                unix_timestamp_millis(),
            ),
            state: QueueBlobReplicationState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: BlobReplicationQueueError) -> Effects {
        self.state = QueueBlobReplicationState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn read_existing(&mut self) -> Effects {
        let key = match blob_replication_job_key(&self.job) {
            Ok(key) => key,
            Err(error) => return self.fail(error.into()),
        };
        self.state = QueueBlobReplicationState::ReadExisting;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BLOB_REPLICATION_JOB_KEYSPACE.to_string(),
            key,
            txn_id: None,
        })]
    }

    fn write_job(&mut self) -> Effects {
        let (key_space, key, value) = match blob_replication_job_write_entry(&self.job) {
            Ok(entry) => entry,
            Err(error) => return self.fail(error.into()),
        };
        self.state = QueueBlobReplicationState::WriteJob;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space,
            key,
            value,
            txn_id: None,
        })]
    }

    fn schedule_drain(&mut self) -> Effects {
        self.state = QueueBlobReplicationState::ScheduleDrain;
        smallvec![schedule_blob_replication_drain_effect()]
    }

    fn finish(&mut self, scheduled: bool) -> Effects {
        self.state = QueueBlobReplicationState::Finish;
        self.output = Some(Ok(QueueBlobReplicationResult {
            queued: 1,
            scheduled,
        }));
        smallvec![]
    }
}

impl Operation for QueueBlobReplicationOperation {
    type Output = QueueBlobReplicationResult;
    type Error = BlobReplicationQueueError;

    fn start(&mut self) -> Effects {
        self.read_existing()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            QueueBlobReplicationState::Init => self.read_existing(),
            QueueBlobReplicationState::ReadExisting => match event {
                Event::Storage(StorageEvent::ReadResult {
                    value: Some(value), ..
                }) => match BlobReplicationJobRecord::from_bytes(&value) {
                    Ok(existing) if blob_replication_job_preferred(&existing, &self.job) => {
                        self.schedule_drain()
                    }
                    Ok(_) | Err(_) => self.write_job(),
                },
                Event::Storage(StorageEvent::ReadResult { value: None, .. }) => self.write_job(),
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.fail(BlobReplicationQueueError::UnexpectedEvent(format!(
                    "{other:?}"
                ))),
            },
            QueueBlobReplicationState::WriteJob => match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => self.schedule_drain(),
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.fail(BlobReplicationQueueError::UnexpectedEvent(format!(
                    "{other:?}"
                ))),
            },
            QueueBlobReplicationState::ScheduleDrain => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => self.finish(true),
                Event::Task(TaskEvent::Error { .. }) => self.finish(false),
                other => {
                    warn!(event = ?other, "Blob replication job persisted but drain scheduling returned an unexpected event");
                    self.finish(false)
                }
            },
            QueueBlobReplicationState::Finish => smallvec![],
            QueueBlobReplicationState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            QueueBlobReplicationState::Finish | QueueBlobReplicationState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.output {
            Some(Ok(result)) => Ok(result),
            Some(Err(error)) => Err(error),
            None => Err(BlobReplicationQueueError::UnexpectedEvent(
                "queue operation finished without output".to_string(),
            )),
        }
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueLiveVersionReplicationInput {
    pub local_node_id: NodeId,
    pub auth_context: AuthContext,
    pub bucket: String,
    pub key: String,
    pub version_id: Ulid,
    pub delete_marker: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum QueueLiveVersionReplicationState {
    Init,
    ReadRelationships,
    ReadConfig,
    ReadExistingJobs,
    WriteJobs,
    DeleteObligation,
    ScheduleDrain,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct QueueLiveVersionReplicationOperation {
    input: QueueLiveVersionReplicationInput,
    state: QueueLiveVersionReplicationState,
    queued: usize,
    relationship_jobs: Vec<BlobReplicationJobRecord>,
    relationship_targets: Vec<(NodeId, String)>,
    pending_jobs: Vec<BlobReplicationJobRecord>,
    output: Option<Result<QueueBlobReplicationResult, BlobReplicationQueueError>>,
}

impl QueueLiveVersionReplicationOperation {
    pub fn new(input: QueueLiveVersionReplicationInput) -> Self {
        Self {
            input,
            state: QueueLiveVersionReplicationState::Init,
            queued: 0,
            relationship_jobs: Vec::new(),
            relationship_targets: Vec::new(),
            pending_jobs: Vec::new(),
            output: None,
        }
    }

    fn fail(&mut self, error: BlobReplicationQueueError) -> Effects {
        self.state = QueueLiveVersionReplicationState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn read_relationships(&mut self, start: Option<Key>) -> Effects {
        self.state = QueueLiveVersionReplicationState::ReadRelationships;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: SYNC_RELATIONSHIP_OUT_KEYSPACE.to_string(),
            prefix: Some(sync_relationship_prefix(&self.input.bucket).into()),
            start: start.map(IterStart::After),
            limit: REPLICATION_SCAN_PAGE_SIZE,
            txn_id: None,
        })]
    }

    fn read_config(&mut self) -> Effects {
        self.state = QueueLiveVersionReplicationState::ReadConfig;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_BUCKET_REPLICATION_KEYSPACE.to_string(),
            key: self.input.bucket.as_bytes().to_vec().into(),
            txn_id: None,
        })]
    }

    fn merge_config(
        &mut self,
        config: Option<BucketReplicationConfig>,
    ) -> Vec<BlobReplicationJobRecord> {
        let jobs = std::mem::take(&mut self.relationship_jobs);
        let Some(config) = config else {
            return jobs;
        };
        let config = filter_config(config, &self.relationship_targets);
        let legacy = live_replication_jobs_from_config(
            self.input.local_node_id,
            &self.input.auth_context,
            &self.input.bucket,
            &self.input.key,
            self.input.version_id,
            self.input.delete_marker,
            config,
        );
        jobs.into_iter().chain(legacy).collect()
    }

    fn read_existing_jobs(&mut self, jobs: Vec<BlobReplicationJobRecord>) -> Effects {
        if jobs.is_empty() {
            return self.delete_obligation();
        }
        let reads = match jobs
            .iter()
            .map(|job| {
                Ok((
                    BLOB_REPLICATION_JOB_KEYSPACE.to_string(),
                    blob_replication_job_key(job)?,
                ))
            })
            .collect::<Result<Vec<_>, ConversionError>>()
        {
            Ok(reads) => reads,
            Err(error) => return self.fail(error.into()),
        };
        self.pending_jobs = jobs;
        self.state = QueueLiveVersionReplicationState::ReadExistingJobs;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        })]
    }

    fn write_jobs(&mut self, jobs: Vec<BlobReplicationJobRecord>) -> Effects {
        if jobs.is_empty() {
            self.queued = 0;
            return self.delete_obligation();
        }

        let mut writes = Vec::with_capacity(jobs.len());
        for job in &jobs {
            let write = match blob_replication_job_write_entry(job) {
                Ok(write) => write,
                Err(error) => return self.fail(error.into()),
            };
            writes.push(write);
        }
        self.queued = writes.len();
        self.state = QueueLiveVersionReplicationState::WriteJobs;
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })]
    }

    fn delete_obligation(&mut self) -> Effects {
        let record = LiveReplicationObligationRecord::new(
            self.input.local_node_id,
            self.input.auth_context.clone(),
            self.input.bucket.clone(),
            self.input.key.clone(),
            self.input.version_id,
            self.input.delete_marker,
        );
        let key = match live_replication_obligation_key(&record) {
            Ok(key) => key,
            Err(error) => return self.fail(error.into()),
        };
        self.state = QueueLiveVersionReplicationState::DeleteObligation;
        smallvec![Effect::Storage(StorageEffect::Delete {
            key_space: BLOB_LIVE_REPLICATION_OBLIGATION_KEYSPACE.to_string(),
            key,
            txn_id: None,
        })]
    }

    fn finish_after_obligation_delete(&mut self) -> Effects {
        if self.queued > 0 {
            self.schedule_drain()
        } else {
            self.finish(0, false)
        }
    }

    fn schedule_drain(&mut self) -> Effects {
        self.state = QueueLiveVersionReplicationState::ScheduleDrain;
        smallvec![schedule_blob_replication_drain_effect()]
    }

    fn finish(&mut self, queued: usize, scheduled: bool) -> Effects {
        self.state = QueueLiveVersionReplicationState::Finish;
        self.output = Some(Ok(QueueBlobReplicationResult { queued, scheduled }));
        smallvec![]
    }
}

impl Operation for QueueLiveVersionReplicationOperation {
    type Output = QueueBlobReplicationResult;
    type Error = BlobReplicationQueueError;

    fn start(&mut self) -> Effects {
        self.read_relationships(None)
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            QueueLiveVersionReplicationState::Init => self.read_relationships(None),
            QueueLiveVersionReplicationState::ReadRelationships => match event {
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }) => {
                    for (key, value) in values {
                        let relationship = match SyncRelationship::from_bytes(&value) {
                            Ok(relationship) => relationship,
                            Err(error) => return self.fail(error.into()),
                        };
                        if let Err(error) =
                            validate_sync_key(&self.input.bucket, &key, &relationship)
                        {
                            return self.fail(error.into());
                        }
                        let target = relationship
                            .target
                            .bucket()
                            .map(|bucket| (relationship.target.node_id, bucket.to_string()));
                        if let Some(job) = relationship_job(
                            self.input.local_node_id,
                            RelationshipJobTarget {
                                bucket: &self.input.bucket,
                                key: &self.input.key,
                                version_id: self.input.version_id,
                                delete_marker: self.input.delete_marker,
                            },
                            relationship,
                            None,
                            &[],
                        )
                        .map(|job| job.with_writer_auth(self.input.auth_context.clone()))
                        {
                            self.relationship_jobs.push(job);
                            if let Some(target) = target {
                                self.relationship_targets.push(target);
                            }
                        }
                    }
                    match next_start_after {
                        Some(start) => self.read_relationships(Some(start)),
                        None => self.read_config(),
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.fail(BlobReplicationQueueError::UnexpectedEvent(format!(
                    "{other:?}"
                ))),
            },
            QueueLiveVersionReplicationState::ReadConfig => match event {
                Event::Storage(StorageEvent::ReadResult { value: None, .. }) => {
                    let jobs = self.merge_config(None);
                    self.read_existing_jobs(jobs)
                }
                Event::Storage(StorageEvent::ReadResult {
                    value: Some(value), ..
                }) => match BucketReplicationConfig::from_bytes(value.as_ref()) {
                    Ok(config) => {
                        let jobs = self.merge_config(Some(config));
                        self.read_existing_jobs(jobs)
                    }
                    Err(error) => self.fail(error.into()),
                },
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.fail(BlobReplicationQueueError::UnexpectedEvent(format!(
                    "{other:?}"
                ))),
            },
            QueueLiveVersionReplicationState::ReadExistingJobs => match event {
                Event::Storage(StorageEvent::BatchReadResult { values }) => {
                    if values.len() != self.pending_jobs.len() {
                        return self.fail(BlobReplicationQueueError::UnexpectedEvent(
                            "blob replication existing job read count mismatch".to_string(),
                        ));
                    }
                    let mut jobs = Vec::new();
                    let pending_jobs = std::mem::take(&mut self.pending_jobs);
                    for (job, (_, value)) in pending_jobs.into_iter().zip(values) {
                        match value {
                            Some(value) => match BlobReplicationJobRecord::from_bytes(&value) {
                                Ok(existing) if blob_replication_job_preferred(&existing, &job) => {
                                }
                                Ok(_) | Err(_) => jobs.push(job),
                            },
                            None => jobs.push(job),
                        }
                    }
                    self.write_jobs(jobs)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.fail(BlobReplicationQueueError::UnexpectedEvent(format!(
                    "{other:?}"
                ))),
            },
            QueueLiveVersionReplicationState::WriteJobs => match event {
                Event::Storage(StorageEvent::BatchWriteResult { .. }) => self.delete_obligation(),
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.fail(BlobReplicationQueueError::UnexpectedEvent(format!(
                    "{other:?}"
                ))),
            },
            QueueLiveVersionReplicationState::DeleteObligation => match event {
                Event::Storage(StorageEvent::DeleteResult { .. }) => {
                    self.finish_after_obligation_delete()
                }
                Event::Storage(StorageEvent::Error { error }) => {
                    warn!(error = ?error, "Live replication jobs persisted but obligation cleanup failed");
                    self.finish_after_obligation_delete()
                }
                other => {
                    warn!(event = ?other, "Live replication jobs persisted but obligation cleanup returned an unexpected event");
                    self.finish_after_obligation_delete()
                }
            },
            QueueLiveVersionReplicationState::ScheduleDrain => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => self.finish(self.queued, true),
                Event::Task(TaskEvent::Error { .. }) => self.finish(self.queued, false),
                other => {
                    warn!(event = ?other, "Live replication jobs persisted but drain scheduling returned an unexpected event");
                    self.finish(self.queued, false)
                }
            },
            QueueLiveVersionReplicationState::Finish => smallvec![],
            QueueLiveVersionReplicationState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            QueueLiveVersionReplicationState::Finish | QueueLiveVersionReplicationState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.output {
            Some(Ok(result)) => Ok(result),
            Some(Err(error)) => Err(error),
            None => Err(BlobReplicationQueueError::UnexpectedEvent(
                "live replication queue operation finished without output".to_string(),
            )),
        }
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

/// Identifies the object version a replication job is being derived for.
struct RelationshipJobTarget<'a> {
    bucket: &'a str,
    key: &'a str,
    version_id: Ulid,
    delete_marker: bool,
}

fn relationship_job(
    local_node_id: NodeId,
    target: RelationshipJobTarget<'_>,
    relationship: SyncRelationship,
    inbound_origin: Option<&SyncOrigin>,
    upstream_sources: &[ArunaArn],
) -> Option<BlobReplicationJobRecord> {
    let RelationshipJobTarget {
        bucket,
        key,
        version_id,
        delete_marker,
    } = target;
    if !matches!(
        relationship.mode,
        SyncMode::Continuous | SyncMode::Reference
    ) || relationship.state != SyncState::Enabled
        || relationship.source.node_id != local_node_id
        || relationship.source.bucket() != Some(bucket)
        || relationship
            .source
            .key_prefix()
            .is_some_and(|prefix| !key.starts_with(prefix))
        || (delete_marker && !relationship.replicate_deletes)
        || inbound_origin.is_some_and(|origin| origin.hop_count >= 4)
        || upstream_sources
            .iter()
            .any(|source| reverse_target(&relationship, source))
    {
        return None;
    }

    let now_ms = unix_timestamp_millis();
    let origin = Some(match inbound_origin {
        Some(origin) => SyncOrigin {
            relationship_id: relationship.id,
            hop_count: origin.hop_count.saturating_add(1),
        },
        None => SyncOrigin {
            relationship_id: relationship.id,
            hop_count: 0,
        },
    });
    let mut next_sources = upstream_sources.to_vec();
    if !next_sources
        .iter()
        .any(|source| same_endpoint(source, &relationship.source))
    {
        next_sources.push(relationship.source.clone());
    }
    Some(
        BlobReplicationJobRecord::new_relationship(
            ReplicateScopeInput {
                bucket: bucket.to_string(),
                target: ReplicateScopeTarget::Version {
                    key: key.to_string(),
                    version_id,
                },
                target_node_id: relationship.target.node_id,
                auth_context: AuthContext {
                    user_id: relationship.created_by,
                    realm_id: relationship.created_by.realm_id,
                    path_restrictions: None,
                },
                replicate_delete_markers: relationship.replicate_deletes,
                mode: ReplicationMode::Live,
            },
            Some(delete_marker),
            relationship.id,
            now_ms,
        )
        .with_origin(origin)
        .with_sources(next_sources),
    )
}

fn reverse_target(relationship: &SyncRelationship, source: &ArunaArn) -> bool {
    same_endpoint(&relationship.target, source)
}

fn same_endpoint(left: &ArunaArn, right: &ArunaArn) -> bool {
    left == right
}

fn filter_config(
    mut config: BucketReplicationConfig,
    relationship_targets: &[(NodeId, String)],
) -> BucketReplicationConfig {
    config.targets.retain(|legacy_target| {
        !relationship_targets.iter().any(|(node_id, bucket)| {
            node_id == &legacy_target.node_id && bucket == &legacy_target.bucket
        })
    });
    config
}

fn validate_sync_key(
    bucket: &str,
    key: &Key,
    relationship: &SyncRelationship,
) -> Result<(), ConversionError> {
    if key.as_ref() != sync_relationship_key(bucket, relationship.id).as_slice() {
        return Err(ConversionError::FromStrError(
            "sync relationship key does not match payload".to_string(),
        ));
    }
    Ok(())
}

fn live_replication_jobs_from_config(
    local_node_id: NodeId,
    auth_context: &AuthContext,
    bucket: &str,
    key: &str,
    version_id: Ulid,
    delete_marker: bool,
    config: BucketReplicationConfig,
) -> Vec<BlobReplicationJobRecord> {
    let now_ms = unix_timestamp_millis();
    config
        .targets
        .into_iter()
        .filter(|target| target.node_id != local_node_id)
        .filter(|target| !delete_marker || target.replicate_delete_markers)
        .map(|target| {
            BlobReplicationJobRecord::new(
                ReplicateScopeInput {
                    bucket: bucket.to_string(),
                    target: ReplicateScopeTarget::Version {
                        key: key.to_string(),
                        version_id,
                    },
                    target_node_id: target.node_id,
                    auth_context: auth_context.clone(),
                    replicate_delete_markers: target.replicate_delete_markers,
                    mode: ReplicationMode::Live,
                },
                Some(delete_marker),
                now_ms,
            )
        })
        .collect()
}

pub async fn restore_blob_replication_timer(storage: &StorageHandle, task_handle: &TaskHandle) {
    match next_blob_replication_timer_after(storage).await {
        Ok(None) => {}
        Ok(Some(after)) => {
            let event = task_handle
                .send_effect(Effect::Task(TaskEffect::ResetTimer {
                    key: TaskKey::DrainBlobReplicationQueue,
                    after,
                }))
                .await;
            if let Event::Task(TaskEvent::Error { message, .. }) = event {
                warn!(message = %message, "Failed to restore blob replication timer");
            }
        }
        Err(error) => warn!(error = ?error, "Failed to scan blob replication jobs"),
    }
}

pub async fn blob_replication_jobs_exist(
    storage: &StorageHandle,
) -> Result<bool, BlobReplicationQueueError> {
    if live_replication_obligations_exist(storage).await? {
        return Ok(true);
    }

    match storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: BLOB_REPLICATION_JOB_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit: 1,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => Ok(!values.is_empty()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(BlobReplicationQueueError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

/// Counts queued replication jobs bound to a relationship and reports the
/// oldest enqueue timestamp among them in unix milliseconds.
pub async fn relationship_job_stats(
    context: &DriverContext,
    relationship_id: Ulid,
) -> Result<(usize, Option<u64>), BlobReplicationQueueError> {
    let mut start = None;
    let mut pending = 0usize;
    let mut oldest = None::<u64>;
    loop {
        match context
            .storage_handle
            .send_storage_effect(StorageEffect::Iter {
                key_space: BLOB_REPLICATION_JOB_KEYSPACE.to_string(),
                prefix: None,
                start: start.map(IterStart::After),
                limit: RELATIONSHIP_STATS_PAGE_SIZE,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => {
                for (_, value) in values {
                    let record = BlobReplicationJobRecord::from_bytes(value.as_ref())?;
                    if record.relationship_id == Some(relationship_id) {
                        pending = pending.saturating_add(1);
                        oldest = Some(oldest.map_or(record.enqueued_at_ms, |current: u64| {
                            current.min(record.enqueued_at_ms)
                        }));
                    }
                }
                let Some(next_start_after) = next_start_after else {
                    return Ok((pending, oldest));
                };
                start = Some(next_start_after);
            }
            Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
            other => {
                return Err(BlobReplicationQueueError::UnexpectedEvent(format!(
                    "{other:?}"
                )));
            }
        }
    }
}

pub async fn next_blob_replication_timer_after(
    storage: &StorageHandle,
) -> Result<Option<Duration>, BlobReplicationQueueError> {
    if live_replication_obligations_exist(storage).await? {
        return Ok(Some(Duration::ZERO));
    }

    let now_ms = unix_timestamp_millis();
    let scan = scan_due_blob_replication_jobs(storage, now_ms, 1).await?;
    if !scan.jobs.is_empty() || scan.has_more_due {
        return Ok(Some(Duration::ZERO));
    }

    Ok(scan
        .next_due_at_ms
        .map(|due_at_ms| due_after(now_ms, due_at_ms)))
}

async fn live_replication_obligations_exist(
    storage: &StorageHandle,
) -> Result<bool, BlobReplicationQueueError> {
    match storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: BLOB_LIVE_REPLICATION_OBLIGATION_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit: 1,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => Ok(!values.is_empty()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(BlobReplicationQueueError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

pub async fn process_blob_replication_batch(
    context: &DriverContext,
) -> Result<BlobReplicationDrainResult, BlobReplicationQueueError> {
    let batch_started = Instant::now();
    let repair = process_live_replication_obligations(context).await?;
    let now_ms = unix_timestamp_millis();
    let scan =
        scan_due_blob_replication_jobs(&context.storage_handle, now_ms, REPLICATION_BATCH_SIZE)
            .await?;
    let mut next_due_at_ms = scan.next_due_at_ms;
    let has_more_due = repair.has_more || scan.has_more_due;
    let scan_elapsed = batch_started.elapsed();
    let job_count = scan.jobs.len();
    let oldest_lag_ms = scan
        .jobs
        .iter()
        .map(|(_, job)| now_ms.saturating_sub(job.due_at_ms))
        .max()
        .unwrap_or(0);

    let mut succeeded = 0usize;
    let mut failed = 0usize;
    for (job_key, job) in scan.jobs {
        match process_blob_replication_job(context, &job).await {
            Ok(BlobReplicationJobOutcome::Succeeded) => {
                delete_blob_replication_job(&context.storage_handle, job_key).await?;
                succeeded = succeeded.saturating_add(1);
            }
            Ok(BlobReplicationJobOutcome::TerminalFailure) => {
                delete_blob_replication_job(&context.storage_handle, job_key).await?;
                failed = failed.saturating_add(1);
            }
            Err(error) => {
                let retry_due_at =
                    reschedule_blob_replication_job(&context.storage_handle, job_key, &job, error)
                        .await?;
                next_due_at_ms = min_due_at(next_due_at_ms, retry_due_at);
                failed = failed.saturating_add(1);
            }
        }
    }

    if job_count > 0 || repair.processed > 0 {
        info!(
            event = "pipeline.blob_replication.summary",
            jobs = job_count,
            repaired_obligations = repair.processed,
            repaired_jobs = repair.queued,
            succeeded,
            failed,
            scan_ms = duration_ms(scan_elapsed),
            total_ms = duration_ms(batch_started.elapsed()),
            oldest_lag_ms,
            has_more_due,
            "Blob replication batch summary"
        );
    }

    Ok(BlobReplicationDrainResult {
        processed: job_count,
        succeeded,
        failed,
        has_more_due,
        next_due_after: if has_more_due {
            None
        } else {
            next_due_at_ms.map(|due_at_ms| due_after(unix_timestamp_millis(), due_at_ms))
        },
    })
}

async fn creator_can_read(
    context: &DriverContext,
    relationship: &SyncRelationship,
) -> Result<(bool, GroupId), (String, Option<GroupId>)> {
    let Some(bucket) = relationship.source.bucket() else {
        return Err(("sync source is not an S3 bucket".to_string(), None));
    };
    let bucket_info = match drive(GetBucketInfoOperation::new(bucket.to_string()), context).await {
        Ok(Some(Ok(bucket_info))) => bucket_info,
        Ok(Some(Err(error))) => return Err((error.to_string(), None)),
        Ok(None) => {
            return Err(("source bucket lookup produced no result".to_string(), None));
        }
        Err(error) => return Err((error.to_string(), None)),
    };
    let group_id = bucket_info.group_id;
    let allowed = drive(
        CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: AuthContext {
                user_id: relationship.created_by,
                realm_id: relationship.source.realm_id,
                path_restrictions: None,
            },
            path: blob_bucket_permission_path(
                relationship.source.realm_id,
                group_id,
                relationship.source.node_id,
                bucket,
            ),
            required_permission: Permission::READ,
        }),
        context,
    )
    .await
    .map_err(|error| (error.to_string(), Some(group_id)))?;
    Ok((allowed, group_id))
}

fn mark_failure(relationship: &mut SyncRelationship, error: &str) {
    relationship.status.last_error = Some(error.to_string());
    relationship.status.counters.failures = relationship.status.counters.failures.saturating_add(1);
    relationship.status.counters.consecutive_failures = relationship
        .status
        .counters
        .consecutive_failures
        .saturating_add(1);
}

fn mark_progress(relationship: &mut SyncRelationship, replicated: u64, bytes: u64) {
    relationship.status.last_synced_at = Some(SystemTime::now());
    relationship.status.counters.versions_synced = relationship
        .status
        .counters
        .versions_synced
        .saturating_add(replicated);
    relationship.status.counters.bytes_synced = relationship
        .status
        .counters
        .bytes_synced
        .saturating_add(bytes);
}

fn mark_success(relationship: &mut SyncRelationship, replicated: u64, bytes: u64) {
    mark_progress(relationship, replicated, bytes);
    relationship.status.last_error = None;
    relationship.status.counters.consecutive_failures = 0;
}

fn is_access_denied(error: &str) -> bool {
    error.contains("Replication requires WRITE permission") || error.contains("access_denied")
}

fn is_writer_denied(error: &str) -> bool {
    error.contains("writer_access_denied")
}

async fn store_relationship(
    context: &DriverContext,
    relationship: SyncRelationship,
) -> Result<bool, String> {
    let stored = store_sync_status(context, &relationship)
        .await
        .map_err(|error| error.to_string())?;
    if stored {
        kick_mirror_repair(context).await;
    }
    Ok(stored)
}

async fn emit_sync_watch(
    context: &DriverContext,
    relationship: &SyncRelationship,
    group_id: GroupId,
    versions_synced: u64,
    error: Option<&str>,
) {
    let Some(bucket) = relationship.source.bucket() else {
        return;
    };
    let node_id = relationship.source.node_id;
    let (kind, detail) = match error {
        Some(error) => (
            WatchEventKind::SyncFailed,
            WatchEventDetail::SyncFailed {
                group_id,
                node_id,
                bucket: bucket.to_string(),
                relationship_id: relationship.id,
                error: error.to_string(),
            },
        ),
        None => (
            WatchEventKind::SyncCompleted,
            WatchEventDetail::SyncCompleted {
                group_id,
                node_id,
                bucket: bucket.to_string(),
                relationship_id: relationship.id,
                versions_synced,
            },
        ),
    };
    emit_resource_watch_event(
        context,
        WatchEvent {
            event_id: Ulid::generate(),
            realm_id: relationship.source.realm_id,
            kind,
            path: data_watch_resource_path(
                group_id,
                node_id,
                bucket,
                relationship.source.key_prefix().unwrap_or_default(),
            ),
            actor: relationship.created_by,
            occurred_at_ms: unix_timestamp_millis(),
            detail,
        },
    )
    .await;
}

async fn process_blob_replication_job(
    context: &DriverContext,
    job: &BlobReplicationJobRecord,
) -> Result<BlobReplicationJobOutcome, String> {
    let mut operation = ReplicateScopeOperation::new(job.input.clone());
    let mut watch_group_id = None;
    let mut relationship = if let Some(relationship_id) = job.relationship_id {
        let relationship = read_relationships(&context.storage_handle, &job.input.bucket)
            .await
            .map_err(|error| error.to_string())?
            .into_iter()
            .find(|relationship| relationship.id == relationship_id);
        let Some(relationship) = relationship else {
            info!(
                relationship_id = %relationship_id,
                "Skipping replication job for missing sync relationship"
            );
            return Ok(BlobReplicationJobOutcome::Succeeded);
        };
        if relationship.state != SyncState::Enabled {
            info!(
                relationship_id = %relationship_id,
                state = ?relationship.state,
                "Skipping replication job for disabled sync relationship"
            );
            return Ok(BlobReplicationJobOutcome::Succeeded);
        }
        match creator_can_read(context, &relationship).await {
            Ok((true, group_id)) => watch_group_id = Some(group_id),
            Ok((false, group_id)) => {
                let mut relationship = relationship;
                relationship.state = SyncState::Failed {
                    reason: "access_denied".to_string(),
                };
                mark_failure(&mut relationship, "access_denied");
                if store_relationship(context, relationship.clone()).await? {
                    emit_sync_watch(context, &relationship, group_id, 0, Some("access_denied"))
                        .await;
                }
                return Ok(BlobReplicationJobOutcome::TerminalFailure);
            }
            Err((error, group_id)) => {
                let mut relationship = relationship;
                mark_failure(&mut relationship, &error);
                if store_relationship(context, relationship.clone()).await?
                    && let Some(group_id) = group_id
                {
                    emit_sync_watch(context, &relationship, group_id, 0, Some(&error)).await;
                }
                return Err(error);
            }
        }
        operation = operation.with_relationship(
            relationship.clone(),
            job.origin.clone(),
            job.upstream_sources.clone(),
            job.writer_auth_context.clone(),
        );
        Some(relationship)
    } else {
        None
    };

    let error = match drive(operation, context).await {
        Ok(Some(Ok(result))) if result.failed == 0 => {
            if let Some(relationship) = relationship.as_mut() {
                mark_success(relationship, result.replicated, result.replicated_bytes);
                let stored = store_relationship(context, relationship.clone()).await?;
                if stored && let Some(group_id) = watch_group_id {
                    emit_sync_watch(context, relationship, group_id, result.replicated, None).await;
                }
            }
            return Ok(BlobReplicationJobOutcome::Succeeded);
        }
        Ok(Some(Ok(result))) => {
            if result.last_error.as_deref().is_some_and(is_writer_denied) {
                return Ok(BlobReplicationJobOutcome::TerminalFailure);
            }
            if result.replicated > 0
                && let Some(relationship) = relationship.as_mut()
            {
                mark_progress(relationship, result.replicated, result.replicated_bytes);
            }
            let error = match result.last_error.as_deref() {
                Some(last_error) => format!(
                    "replication completed with {} replicated, {} skipped, {} failed: {}",
                    result.replicated, result.skipped, result.failed, last_error
                ),
                None => format!(
                    "replication completed with {} replicated, {} skipped, {} failed",
                    result.replicated, result.skipped, result.failed
                ),
            };
            if result.last_error.as_deref().is_some_and(is_access_denied)
                && let Some(relationship) = relationship.as_mut()
            {
                relationship.state = SyncState::Failed {
                    reason: "access_denied".to_string(),
                };
                mark_failure(relationship, "access_denied");
                let stored = store_relationship(context, relationship.clone()).await?;
                if stored && let Some(group_id) = watch_group_id {
                    emit_sync_watch(context, relationship, group_id, 0, Some("access_denied"))
                        .await;
                }
                return Ok(BlobReplicationJobOutcome::TerminalFailure);
            }
            error
        }
        Ok(Some(Err(error))) => error.to_string(),
        Ok(None) => "replication produced no result".to_string(),
        Err(error) => error.to_string(),
    };
    if let Some(relationship) = relationship.as_mut() {
        if is_writer_denied(&error) {
            return Ok(BlobReplicationJobOutcome::TerminalFailure);
        }
        mark_failure(relationship, &error);
        let stored = store_relationship(context, relationship.clone()).await?;
        if stored && let Some(group_id) = watch_group_id {
            emit_sync_watch(context, relationship, group_id, 0, Some(&error)).await;
        }
    }
    Err(error)
}

async fn process_live_replication_obligations(
    context: &DriverContext,
) -> Result<LiveReplicationRepairResult, BlobReplicationQueueError> {
    let (obligations, has_more) =
        read_live_replication_obligations(&context.storage_handle).await?;
    let mut result = LiveReplicationRepairResult {
        has_more,
        ..Default::default()
    };

    for (obligation_key, obligation) in obligations {
        let queued = write_live_jobs(&context.storage_handle, &obligation).await?;
        delete_live_obligation(&context.storage_handle, obligation_key).await?;
        result.processed = result.processed.saturating_add(1);
        result.queued = result.queued.saturating_add(queued);
    }

    Ok(result)
}

async fn read_live_replication_obligations(
    storage: &StorageHandle,
) -> Result<(Vec<(Vec<u8>, LiveReplicationObligationRecord)>, bool), BlobReplicationQueueError> {
    match storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: BLOB_LIVE_REPLICATION_OBLIGATION_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit: LIVE_REPLICATION_OBLIGATION_BATCH_SIZE,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) => {
            let mut obligations = Vec::with_capacity(values.len());
            for (key, value) in values {
                match LiveReplicationObligationRecord::from_bytes(&value) {
                    Ok(record) => obligations.push((key.to_vec(), record)),
                    Err(error) => {
                        let key = key.to_vec();
                        warn!(error = %error, key = ?key, "Deleting malformed live replication obligation");
                        delete_live_obligation(storage, key).await?;
                    }
                }
            }
            Ok((obligations, next_start_after.is_some()))
        }
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(BlobReplicationQueueError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

async fn read_bucket_replication_config(
    storage: &StorageHandle,
    bucket: &str,
) -> Result<Option<BucketReplicationConfig>, BlobReplicationQueueError> {
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: S3_BUCKET_REPLICATION_KEYSPACE.to_string(),
            key: bucket.as_bytes().to_vec().into(),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Ok(None),
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => Ok(Some(BucketReplicationConfig::from_bytes(value.as_ref())?)),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(BlobReplicationQueueError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

async fn read_relationships(
    storage: &StorageHandle,
    bucket: &str,
) -> Result<Vec<SyncRelationship>, BlobReplicationQueueError> {
    let mut start_after = None;
    let mut relationships = Vec::new();
    loop {
        let event = storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: SYNC_RELATIONSHIP_OUT_KEYSPACE.to_string(),
                prefix: Some(sync_relationship_prefix(bucket).into()),
                start: start_after.take().map(IterStart::After),
                limit: REPLICATION_SCAN_PAGE_SIZE,
                txn_id: None,
            })
            .await;
        let (values, next_start_after) = match event {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => (values, next_start_after),
            Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
            other => {
                return Err(BlobReplicationQueueError::UnexpectedEvent(format!(
                    "{other:?}"
                )));
            }
        };
        for (key, value) in values {
            let relationship = SyncRelationship::from_bytes(&value)?;
            validate_sync_key(bucket, &key, &relationship)?;
            relationships.push(relationship);
        }
        match next_start_after {
            Some(next) => start_after = Some(next),
            None => return Ok(relationships),
        }
    }
}

async fn write_live_jobs(
    storage: &StorageHandle,
    obligation: &LiveReplicationObligationRecord,
) -> Result<usize, BlobReplicationQueueError> {
    if obligation
        .origin
        .as_ref()
        .is_some_and(|origin| origin.hop_count >= 4)
    {
        return Ok(0);
    }
    let inbound_source = match obligation.origin.as_ref() {
        Some(origin) => read_inbound_source(storage, &obligation.bucket, origin).await?,
        None => None,
    };
    let mut upstream_sources = obligation.upstream_sources.clone();
    if let Some(source) = inbound_source
        && !upstream_sources
            .iter()
            .any(|existing| same_endpoint(existing, &source))
    {
        upstream_sources.push(source);
    }
    let mut relationship_jobs = Vec::new();
    let mut relationship_targets = Vec::new();
    for relationship in read_relationships(storage, &obligation.bucket).await? {
        let target = relationship
            .target
            .bucket()
            .map(|bucket| (relationship.target.node_id, bucket.to_string()));
        if let Some(job) = relationship_job(
            obligation.local_node_id,
            RelationshipJobTarget {
                bucket: &obligation.bucket,
                key: &obligation.key,
                version_id: obligation.version_id,
                delete_marker: obligation.delete_marker,
            },
            relationship,
            obligation.origin.as_ref(),
            &upstream_sources,
        )
        .map(|job| job.with_writer_auth(obligation.auth_context.clone()))
        {
            relationship_jobs.push(job);
            if let Some(target) = target {
                relationship_targets.push(target);
            }
        }
    }
    let legacy_jobs = match (
        obligation.origin.is_none(),
        read_bucket_replication_config(storage, &obligation.bucket).await?,
    ) {
        (true, Some(config)) => {
            let config = filter_config(config, &relationship_targets);
            live_replication_jobs_from_config(
                obligation.local_node_id,
                &obligation.auth_context,
                &obligation.bucket,
                &obligation.key,
                obligation.version_id,
                obligation.delete_marker,
                config,
            )
        }
        (false, _) | (_, None) => Vec::new(),
    };
    relationship_jobs.extend(legacy_jobs);
    let jobs = relationship_jobs;
    if jobs.is_empty() {
        return Ok(0);
    }

    let reads = jobs
        .iter()
        .map(|job| {
            Ok((
                BLOB_REPLICATION_JOB_KEYSPACE.to_string(),
                blob_replication_job_key(job)?,
            ))
        })
        .collect::<Result<Vec<_>, ConversionError>>()?;
    let values = match storage
        .send_storage_effect(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchReadResult { values }) => values,
        Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
        other => {
            return Err(BlobReplicationQueueError::UnexpectedEvent(format!(
                "{other:?}"
            )));
        }
    };
    if values.len() != jobs.len() {
        return Err(BlobReplicationQueueError::UnexpectedEvent(
            "blob replication existing job read count mismatch".to_string(),
        ));
    }

    let mut writes = Vec::new();
    for (job, (_, value)) in jobs.iter().zip(values) {
        match value {
            Some(value) => match BlobReplicationJobRecord::from_bytes(&value) {
                Ok(existing) if blob_replication_job_preferred(&existing, job) => {}
                Ok(_) | Err(_) => writes.push(blob_replication_job_write_entry(job)?),
            },
            None => writes.push(blob_replication_job_write_entry(job)?),
        }
    }
    if writes.is_empty() {
        return Ok(0);
    }
    match storage
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchWriteResult { entries }) => Ok(entries.len()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(BlobReplicationQueueError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

async fn read_inbound_source(
    storage: &StorageHandle,
    bucket: &str,
    origin: &SyncOrigin,
) -> Result<Option<ArunaArn>, BlobReplicationQueueError> {
    let key = sync_relationship_key(bucket, origin.relationship_id);
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: SYNC_RELATIONSHIP_IN_KEYSPACE.to_string(),
            key: ByteView::from(key.clone()),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Ok(None),
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => {
            let relationship = SyncRelationship::from_bytes(&value)?;
            if relationship.id != origin.relationship_id
                || relationship.target.bucket() != Some(bucket)
                || sync_relationship_key(bucket, relationship.id) != key
            {
                return Err(ConversionError::FromStrError(
                    "incoming sync relationship key does not match payload".to_string(),
                )
                .into());
            }
            Ok(Some(relationship.source))
        }
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(BlobReplicationQueueError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

async fn delete_live_obligation(
    storage: &StorageHandle,
    key: Vec<u8>,
) -> Result<(), BlobReplicationQueueError> {
    match storage
        .send_storage_effect(StorageEffect::Delete {
            key_space: BLOB_LIVE_REPLICATION_OBLIGATION_KEYSPACE.to_string(),
            key: ByteView::from(key),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::DeleteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(BlobReplicationQueueError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

async fn scan_due_blob_replication_jobs(
    storage: &StorageHandle,
    now_ms: u64,
    limit: usize,
) -> Result<BlobReplicationJobScan, BlobReplicationQueueError> {
    let mut start_after = None;
    let mut jobs = Vec::new();
    let mut next_due_at_ms = None;
    loop {
        let event = storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: BLOB_REPLICATION_JOB_KEYSPACE.to_string(),
                prefix: None,
                start: start_after.take().map(IterStart::After),
                limit: REPLICATION_SCAN_PAGE_SIZE,
                txn_id: None,
            })
            .await;
        let (values, next_start_after) = match event {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => (values, next_start_after),
            Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
            other => {
                return Err(BlobReplicationQueueError::UnexpectedEvent(format!(
                    "{other:?}"
                )));
            }
        };

        for (key, value) in values {
            let mut key = key.to_vec();
            let job = match BlobReplicationJobRecord::from_bytes(&value) {
                Ok(job) => job,
                Err(error) => {
                    warn!(error = %error, key = ?key, "Deleting malformed blob replication job");
                    delete_blob_replication_job(storage, key).await?;
                    continue;
                }
            };
            let canonical_key = blob_replication_job_key(&job)?.to_vec();
            if canonical_key.as_slice() != key.as_slice() {
                warn!(key = ?key, "Repairing blob replication job stored under non-canonical key");
                if let Some(existing) =
                    read_blob_replication_job_at_key(storage, &canonical_key).await?
                    && blob_replication_job_preferred(&existing, &job)
                {
                    delete_blob_replication_job(storage, key).await?;
                    if existing.due_at_ms > now_ms {
                        next_due_at_ms = min_due_at(next_due_at_ms, existing.due_at_ms);
                        continue;
                    }
                    jobs.push((canonical_key, existing));
                    if jobs.len() >= limit {
                        return Ok(BlobReplicationJobScan {
                            jobs,
                            has_more_due: true,
                            next_due_at_ms,
                        });
                    }
                    continue;
                }
                write_blob_replication_job(storage, &job).await?;
                delete_blob_replication_job(storage, key).await?;
                key = canonical_key;
            }
            if let Some((existing_key, existing)) =
                find_decoded_blob_replication_duplicate(storage, &job, Some(key.as_slice())).await?
                && blob_replication_job_preferred(&existing, &job)
            {
                let existing_canonical_key = blob_replication_job_key(&existing)?.to_vec();
                write_blob_replication_job(storage, &existing).await?;
                if existing_key.as_slice() != existing_canonical_key.as_slice() {
                    delete_blob_replication_job(storage, existing_key).await?;
                }
                if key.as_slice() != existing_canonical_key.as_slice() {
                    delete_blob_replication_job(storage, key).await?;
                }
                if existing.due_at_ms > now_ms {
                    next_due_at_ms = min_due_at(next_due_at_ms, existing.due_at_ms);
                    continue;
                }
                jobs.push((existing_canonical_key, existing));
                if jobs.len() >= limit {
                    return Ok(BlobReplicationJobScan {
                        jobs,
                        has_more_due: true,
                        next_due_at_ms,
                    });
                }
                continue;
            }
            if job.due_at_ms > now_ms {
                next_due_at_ms = min_due_at(next_due_at_ms, job.due_at_ms);
                continue;
            }
            jobs.push((key, job));
            if jobs.len() >= limit {
                return Ok(BlobReplicationJobScan {
                    jobs,
                    has_more_due: true,
                    next_due_at_ms,
                });
            }
        }

        match next_start_after {
            Some(next) => start_after = Some(next),
            None => {
                return Ok(BlobReplicationJobScan {
                    jobs,
                    has_more_due: false,
                    next_due_at_ms,
                });
            }
        }
    }
}

async fn find_decoded_blob_replication_duplicate(
    storage: &StorageHandle,
    job: &BlobReplicationJobRecord,
    skip_key: Option<&[u8]>,
) -> Result<Option<(Vec<u8>, BlobReplicationJobRecord)>, BlobReplicationQueueError> {
    let canonical_key = blob_replication_job_key(job)?.to_vec();
    let mut selected = None;
    let mut start_after = None;
    loop {
        let event = storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: BLOB_REPLICATION_JOB_KEYSPACE.to_string(),
                prefix: None,
                start: start_after.take().map(IterStart::After),
                limit: REPLICATION_SCAN_PAGE_SIZE,
                txn_id: None,
            })
            .await;
        let (values, next_start_after) = match event {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => (values, next_start_after),
            Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
            other => {
                return Err(BlobReplicationQueueError::UnexpectedEvent(format!(
                    "{other:?}"
                )));
            }
        };

        for (key, value) in values {
            if skip_key.is_some_and(|skip_key| key.as_ref() == skip_key) {
                continue;
            }
            let Ok(candidate) = BlobReplicationJobRecord::from_bytes(&value) else {
                continue;
            };
            if blob_replication_job_key(&candidate)?.as_ref() != canonical_key.as_slice() {
                continue;
            }
            match selected.as_mut() {
                Some((_, selected_job))
                    if blob_replication_job_preferred(&candidate, selected_job) =>
                {
                    selected = Some((key.to_vec(), candidate));
                }
                Some(_) => {}
                None => selected = Some((key.to_vec(), candidate)),
            }
        }

        match next_start_after {
            Some(next) => start_after = Some(next),
            None => return Ok(selected),
        }
    }
}

async fn read_blob_replication_job_at_key(
    storage: &StorageHandle,
    key: &[u8],
) -> Result<Option<BlobReplicationJobRecord>, BlobReplicationQueueError> {
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: BLOB_REPLICATION_JOB_KEYSPACE.to_string(),
            key: ByteView::from(key.to_vec()),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => Ok(Some(BlobReplicationJobRecord::from_bytes(&value)?)),
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Ok(None),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(BlobReplicationQueueError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

async fn write_blob_replication_job(
    storage: &StorageHandle,
    job: &BlobReplicationJobRecord,
) -> Result<(), BlobReplicationQueueError> {
    let (key_space, key, value) = blob_replication_job_write_entry(job)?;
    match storage
        .send_storage_effect(StorageEffect::Write {
            key_space,
            key,
            value,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(BlobReplicationQueueError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

async fn delete_blob_replication_job(
    storage: &StorageHandle,
    key: Vec<u8>,
) -> Result<(), BlobReplicationQueueError> {
    match storage
        .send_storage_effect(StorageEffect::Delete {
            key_space: BLOB_REPLICATION_JOB_KEYSPACE.to_string(),
            key: ByteView::from(key),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::DeleteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(BlobReplicationQueueError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

async fn reschedule_blob_replication_job(
    storage: &StorageHandle,
    key: Vec<u8>,
    job: &BlobReplicationJobRecord,
    error: String,
) -> Result<u64, BlobReplicationQueueError> {
    let attempts = job.attempts.saturating_add(1);
    let due_at_ms = unix_timestamp_millis().saturating_add(queue_retry_after_ms(attempts));
    let next_job = BlobReplicationJobRecord {
        input: job.input.clone(),
        source_delete_marker: job.source_delete_marker,
        due_at_ms,
        attempts,
        last_error: Some(error),
        relationship_id: job.relationship_id,
        enqueued_at_ms: job.enqueued_at_ms,
        origin: job.origin.clone(),
        upstream_sources: job.upstream_sources.clone(),
        writer_auth_context: job.writer_auth_context.clone(),
    };
    match storage
        .send_storage_effect(StorageEffect::Write {
            key_space: BLOB_REPLICATION_JOB_KEYSPACE.to_string(),
            key: ByteView::from(key),
            value: ByteView::from(next_job.to_bytes()?),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(due_at_ms),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(BlobReplicationQueueError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

fn min_due_at(current: Option<u64>, due_at_ms: u64) -> Option<u64> {
    Some(current.map_or(due_at_ms, |current| current.min(due_at_ms)))
}

fn due_after(now_ms: u64, due_at_ms: u64) -> Duration {
    Duration::from_millis(due_at_ms.saturating_sub(now_ms))
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::keyspaces::{AUTH_KEYSPACE, BLOB_VERSIONS_KEYSPACE};
    use aruna_core::structs::{
        Actor, ArunaArn, BlobVersion, BucketInfo, BucketReplicationTarget,
        GroupAuthorizationDocument, RealmAuthorizationDocument, RealmId, ReferenceHandling,
        SyncStatusSnapshot, VersionKey, sync_relationship_key,
    };
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

    async fn read_jobs(storage: &StorageHandle) -> Vec<(Vec<u8>, BlobReplicationJobRecord)> {
        match storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: BLOB_REPLICATION_JOB_KEYSPACE.to_string(),
                prefix: None,
                start: None,
                limit: 16,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::IterResult { values, .. }) => values
                .into_iter()
                .map(|(key, value)| {
                    (
                        key.to_vec(),
                        BlobReplicationJobRecord::from_bytes(&value)
                            .expect("replication job decodes"),
                    )
                })
                .collect(),
            other => panic!("unexpected storage event: {other:?}"),
        }
    }

    async fn write_raw_queue_record(
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

    async fn write_corrupt_blob_job(storage: &StorageHandle, key: &str) {
        write_raw_queue_record(
            storage,
            BLOB_REPLICATION_JOB_KEYSPACE,
            key.as_bytes().to_vec(),
            Vec::new(),
        )
        .await;
    }

    async fn write_corrupt_live_obligation(storage: &StorageHandle, key: &str) {
        write_raw_queue_record(
            storage,
            BLOB_LIVE_REPLICATION_OBLIGATION_KEYSPACE,
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
        write_raw_queue_record(
            storage,
            AUTH_KEYSPACE,
            realm().as_bytes().to_vec(),
            RealmAuthorizationDocument::new_default_realm_doc(realm())
                .to_bytes(&actor)
                .unwrap(),
        )
        .await;
        write_raw_queue_record(
            storage,
            AUTH_KEYSPACE,
            group_id.to_bytes().to_vec(),
            GroupAuthorizationDocument::new_default_group_doc(user(), realm(), group_id)
                .to_bytes(&actor)
                .unwrap(),
        )
        .await;
    }

    async fn write_replication_config(storage: &StorageHandle, bucket: &str) {
        let config = BucketReplicationConfig {
            targets: vec![
                BucketReplicationTarget {
                    node_id: node(1),
                    realm_id: realm(),
                    bucket: bucket.to_string(),
                    arn: "local".to_string(),
                    replicate_delete_markers: true,
                },
                BucketReplicationTarget {
                    node_id: node(2),
                    realm_id: realm(),
                    bucket: bucket.to_string(),
                    arn: "remote-a".to_string(),
                    replicate_delete_markers: false,
                },
                BucketReplicationTarget {
                    node_id: node(3),
                    realm_id: realm(),
                    bucket: bucket.to_string(),
                    arn: "remote-b".to_string(),
                    replicate_delete_markers: true,
                },
            ],
        };
        match storage
            .send_storage_effect(StorageEffect::Write {
                key_space: S3_BUCKET_REPLICATION_KEYSPACE.to_string(),
                key: bucket.as_bytes().to_vec().into(),
                value: config.to_bytes().expect("config serializes").into(),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected replication config write event: {other:?}"),
        }
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
                key_space: SYNC_RELATIONSHIP_OUT_KEYSPACE.to_string(),
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
                key_space: SYNC_RELATIONSHIP_IN_KEYSPACE.to_string(),
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
        let version = BlobVersion::materialized([7u8; 32], SystemTime::UNIX_EPOCH, user(), None);
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
        let Effect::Storage(effect) = write_live_replication_obligation_effect(
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

    async fn read_obligations(storage: &StorageHandle) -> Vec<LiveReplicationObligationRecord> {
        match storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: BLOB_LIVE_REPLICATION_OBLIGATION_KEYSPACE.to_string(),
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
                    LiveReplicationObligationRecord::from_bytes(&value).expect("obligation decodes")
                })
                .collect(),
            other => panic!("unexpected storage event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn queue_blob_replication_persists_before_returning() {
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

        let result = drive(
            QueueBlobReplicationOperation::new(on_demand_input(), None),
            &context,
        )
        .await
        .expect("queue operation succeeds after durable write");

        assert_eq!(result.queued, 1);
        assert!(!result.scheduled);
        let jobs = read_jobs(&storage).await;
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].1.input.mode, ReplicationMode::OnDemand);
    }

    #[tokio::test]
    async fn duplicate_blob_replication_requests_coalesce() {
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
            QueueBlobReplicationOperation::new(on_demand_input(), None),
            &context,
        )
        .await
        .expect("first queue succeeds");
        drive(
            QueueBlobReplicationOperation::new(on_demand_input(), None),
            &context,
        )
        .await
        .expect("second queue succeeds");

        assert_eq!(read_jobs(&storage).await.len(), 1);
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
            QueueBlobReplicationOperation::new_relationship(
                on_demand_input(),
                None,
                relationship_id,
            ),
            &context,
        )
        .await
        .expect("queue succeeds");
        let enqueued_at_ms = read_jobs(&storage).await[0].1.enqueued_at_ms;

        let result = process_blob_replication_batch(&context)
            .await
            .expect("drain stores retry metadata");

        assert_eq!(result.failed, 1);
        let jobs = read_jobs(&storage).await;
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].1.relationship_id, Some(relationship_id));
        assert_eq!(jobs[0].1.enqueued_at_ms, enqueued_at_ms);
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
            QueueBlobReplicationOperation::new_relationship(
                on_demand_input(),
                None,
                Ulid::from(78u128),
            ),
            &context,
        )
        .await
        .expect("queue succeeds");

        let result = process_blob_replication_batch(&context)
            .await
            .expect("missing relationship is terminal");

        assert_eq!(result.succeeded, 1);
        assert!(read_jobs(&storage).await.is_empty());
    }

    #[tokio::test]
    async fn denied_job_fails() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let group_id = Ulid::from_parts(2, 2);
        write_bucket(&storage, "bucket").await;
        write_auth_docs(&storage, group_id).await;
        let mut relationship = relationship(79, 2, None, true);
        relationship.created_by = UserId::local(Ulid::from_parts(79, 1), realm());
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
            QueueBlobReplicationOperation::new_relationship(
                on_demand_input(),
                None,
                relationship.id,
            ),
            &context,
        )
        .await
        .expect("queue succeeds");

        let result = process_blob_replication_batch(&context)
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
    }

    #[tokio::test]
    async fn success_updates_status() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let group_id = Ulid::from_parts(2, 2);
        write_bucket(&storage, "bucket").await;
        write_auth_docs(&storage, group_id).await;
        let mut relationship = relationship(80, 2, None, true);
        relationship.status.last_error = Some("old error".to_string());
        relationship.status.counters.consecutive_failures = 2;
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
            QueueBlobReplicationOperation::new_relationship(
                ReplicateScopeInput {
                    target: ReplicateScopeTarget::Bucket,
                    ..on_demand_input()
                },
                None,
                relationship.id,
            ),
            &context,
        )
        .await
        .expect("queue succeeds");

        let result = process_blob_replication_batch(&context)
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

        let Err((_, group_id)) = creator_can_read(&context, &relationship(83, 2, None, true)).await
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

    #[tokio::test]
    async fn duplicate_blob_replication_request_preserves_future_retry() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let due_at_ms = unix_timestamp_millis().saturating_add(60_000);
        let future_job = BlobReplicationJobRecord {
            input: on_demand_input(),
            source_delete_marker: None,
            due_at_ms,
            attempts: 1,
            last_error: Some("transient".to_string()),
            relationship_id: None,
            enqueued_at_ms: due_at_ms,
            origin: None,
            upstream_sources: Vec::new(),
            writer_auth_context: None,
        };
        let (key_space, key, value) = blob_replication_job_write_entry(&future_job).unwrap();
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

        drive(
            QueueBlobReplicationOperation::new(on_demand_input(), None),
            &context,
        )
        .await
        .expect("duplicate queue succeeds");

        let jobs = read_jobs(&storage).await;
        assert_eq!(
            jobs,
            vec![(
                blob_replication_job_key(&future_job).unwrap().to_vec(),
                future_job
            )]
        );
    }

    #[test]
    fn legacy_job_decodes() {
        let origin_record = OriginBlobReplicationJobRecord {
            input: on_demand_input(),
            source_delete_marker: Some(false),
            due_at_ms: 96,
            attempts: 2,
            last_error: None,
            relationship_id: Some(Ulid::from(10u128)),
            enqueued_at_ms: 12,
            origin: Some(SyncOrigin {
                relationship_id: Ulid::from(10u128),
                hop_count: 1,
            }),
        };
        let origin_bytes = postcard::to_allocvec(&origin_record).unwrap();
        let origin_decoded = BlobReplicationJobRecord::from_bytes(&origin_bytes).unwrap();
        assert_eq!(origin_decoded.origin, origin_record.origin);
        assert!(origin_decoded.upstream_sources.is_empty());

        let previous = PreviousBlobReplicationJobRecord {
            input: on_demand_input(),
            source_delete_marker: Some(true),
            due_at_ms: 84,
            attempts: 4,
            last_error: None,
            relationship_id: Some(Ulid::from(9u128)),
            enqueued_at_ms: 21,
        };
        let previous_bytes = postcard::to_allocvec(&previous).unwrap();
        let previous_decoded = BlobReplicationJobRecord::from_bytes(&previous_bytes).unwrap();
        assert_eq!(previous_decoded.relationship_id, previous.relationship_id);
        assert_eq!(previous_decoded.enqueued_at_ms, previous.enqueued_at_ms);
        assert_eq!(previous_decoded.origin, None);
        assert!(previous_decoded.upstream_sources.is_empty());

        let legacy = LegacyBlobReplicationJobRecord {
            input: on_demand_input(),
            source_delete_marker: Some(false),
            due_at_ms: 42,
            attempts: 3,
            last_error: Some("retry".to_string()),
        };
        let bytes = postcard::to_allocvec(&legacy).unwrap();

        let decoded = BlobReplicationJobRecord::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.input, legacy.input);
        assert_eq!(decoded.source_delete_marker, Some(false));
        assert_eq!(decoded.due_at_ms, 42);
        assert_eq!(decoded.attempts, 3);
        assert_eq!(decoded.last_error.as_deref(), Some("retry"));
        assert_eq!(decoded.relationship_id, None);
        assert_eq!(decoded.enqueued_at_ms, 42);
        assert!(decoded.upstream_sources.is_empty());

        let first = BlobReplicationJobRecord::new_relationship(
            on_demand_input(),
            None,
            Ulid::from(1u128),
            42,
        );
        let second = BlobReplicationJobRecord::new_relationship(
            on_demand_input(),
            None,
            Ulid::from(2u128),
            42,
        );
        assert_ne!(
            blob_replication_job_key(&first).unwrap(),
            blob_replication_job_key(&second).unwrap()
        );
        assert!(is_access_denied("Replication requires WRITE permission"));
        assert!(is_access_denied("access_denied"));
        assert!(!is_access_denied("quota"));
    }

    #[test]
    fn legacy_obligation_decodes() {
        let origin_record = OriginLiveReplicationObligationRecord {
            local_node_id: node(1),
            auth_context: auth_context(),
            bucket: "bucket".to_string(),
            key: "key".to_string(),
            version_id: Ulid::from(8u128),
            delete_marker: false,
            origin: Some(SyncOrigin {
                relationship_id: Ulid::from(9u128),
                hop_count: 2,
            }),
        };
        let origin_bytes = postcard::to_allocvec(&origin_record).unwrap();
        let origin_decoded = LiveReplicationObligationRecord::from_bytes(&origin_bytes).unwrap();
        assert_eq!(origin_decoded.origin, origin_record.origin);
        assert!(origin_decoded.upstream_sources.is_empty());

        let legacy = LegacyLiveReplicationObligationRecord {
            local_node_id: node(1),
            auth_context: auth_context(),
            bucket: "bucket".to_string(),
            key: "key".to_string(),
            version_id: Ulid::from(7u128),
            delete_marker: true,
        };
        let bytes = postcard::to_allocvec(&legacy).unwrap();

        let decoded = LiveReplicationObligationRecord::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.local_node_id, legacy.local_node_id);
        assert_eq!(decoded.auth_context, legacy.auth_context);
        assert_eq!(decoded.bucket, legacy.bucket);
        assert_eq!(decoded.key, legacy.key);
        assert_eq!(decoded.version_id, legacy.version_id);
        assert!(decoded.delete_marker);
        assert_eq!(decoded.origin, None);
        assert!(decoded.upstream_sources.is_empty());
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
        let obligation = LiveReplicationObligationRecord::new(
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

        assert_eq!(write_live_jobs(&storage, &obligation).await.unwrap(), 1);
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
            QueueLiveVersionReplicationOperation::new(QueueLiveVersionReplicationInput {
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

        assert_eq!(result.queued, 2);
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
            QueueLiveVersionReplicationOperation::new(QueueLiveVersionReplicationInput {
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

        assert_eq!(result.queued, 1);
        let jobs = read_jobs(&storage).await;
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].1.relationship_id, Some(included.id));
        assert_eq!(jobs[0].1.source_delete_marker, Some(true));
    }

    #[tokio::test]
    async fn legacy_fallback_dedupes() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let mut relationship = relationship(5, 2, None, true);
        relationship.created_by = UserId::local(Ulid::from_parts(9, 9), realm());
        write_relationship(&storage, &relationship).await;
        write_replication_config(&storage, "bucket").await;
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let result = drive(
            QueueLiveVersionReplicationOperation::new(QueueLiveVersionReplicationInput {
                local_node_id: node(1),
                auth_context: auth_context(),
                bucket: "bucket".to_string(),
                key: "key".to_string(),
                version_id: Ulid::from_parts(33, 1),
                delete_marker: false,
            }),
            &context,
        )
        .await
        .expect("merged queue succeeds");

        assert_eq!(result.queued, 2);
        let jobs = read_jobs(&storage).await;
        assert_eq!(jobs.len(), 2);
        let relationship_job = jobs
            .iter()
            .find(|(_, job)| job.input.target_node_id == node(2))
            .map(|(_, job)| job)
            .unwrap();
        assert_eq!(relationship_job.relationship_id, Some(relationship.id));
        assert_eq!(
            relationship_job.input.auth_context.user_id,
            relationship.created_by
        );
        let legacy_job = jobs
            .iter()
            .find(|(_, job)| job.input.target_node_id == node(3))
            .map(|(_, job)| job)
            .unwrap();
        assert_eq!(legacy_job.relationship_id, None);
    }

    #[tokio::test]
    async fn live_version_queue_expands_targets_and_skips_disabled_delete_markers() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        write_replication_config(&storage, "bucket").await;
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let result = drive(
            QueueLiveVersionReplicationOperation::new(QueueLiveVersionReplicationInput {
                local_node_id: node(1),
                auth_context: auth_context(),
                bucket: "bucket".to_string(),
                key: "key".to_string(),
                version_id: Ulid::from_parts(3, 3),
                delete_marker: true,
            }),
            &context,
        )
        .await
        .expect("live queue succeeds");

        assert_eq!(result.queued, 1);
        let jobs = read_jobs(&storage).await;
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].1.input.target_node_id, node(3));
        assert_eq!(jobs[0].1.source_delete_marker, Some(true));
    }

    #[tokio::test]
    async fn live_version_queue_preserves_future_retry_job() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let version_id = Ulid::from_parts(33, 3);
        write_replication_config(&storage, "bucket").await;
        let future_job = live_replication_jobs_from_config(
            node(1),
            &auth_context(),
            "bucket",
            "key",
            version_id,
            true,
            BucketReplicationConfig {
                targets: vec![BucketReplicationTarget {
                    node_id: node(3),
                    realm_id: realm(),
                    bucket: "bucket".to_string(),
                    arn: "remote-b".to_string(),
                    replicate_delete_markers: true,
                }],
            },
        )
        .pop()
        .expect("future job builds");
        let future_job = BlobReplicationJobRecord {
            due_at_ms: unix_timestamp_millis().saturating_add(60_000),
            attempts: 1,
            last_error: Some("transient".to_string()),
            ..future_job
        };
        let (key_space, key, value) = blob_replication_job_write_entry(&future_job).unwrap();
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
            QueueLiveVersionReplicationOperation::new(QueueLiveVersionReplicationInput {
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

        assert_eq!(result.queued, 0);
        let jobs = read_jobs(&storage).await;
        assert_eq!(
            jobs,
            vec![(
                blob_replication_job_key(&future_job).unwrap().to_vec(),
                future_job
            )]
        );
    }

    #[tokio::test]
    async fn future_blob_replication_job_does_not_report_more_due() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let due_at_ms = unix_timestamp_millis().saturating_add(60_000);
        let record = BlobReplicationJobRecord::new(on_demand_input(), None, due_at_ms);
        let (key_space, key, value) = blob_replication_job_write_entry(&record).unwrap();
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

        let result = process_blob_replication_batch(&context)
            .await
            .expect("future-only drain succeeds");

        assert_eq!(result.processed, 0);
        assert!(!result.has_more_due);
        assert!(result.next_due_after.is_some());
    }

    #[tokio::test]
    async fn corrupt_blob_replication_job_only_is_deleted() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        write_corrupt_blob_job(&storage, "000-corrupt-blob-job").await;
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let result = process_blob_replication_batch(&context)
            .await
            .expect("corrupt-only drain succeeds");

        assert_eq!(result.processed, 0);
        assert!(!result.has_more_due);
        assert!(result.next_due_after.is_none());
        assert!(!blob_replication_jobs_exist(&storage).await.unwrap());
    }

    #[tokio::test]
    async fn corrupt_blob_replication_job_before_valid_is_deleted() {
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
        write_corrupt_blob_job(&storage, "000-corrupt-blob-job").await;
        drive(
            QueueBlobReplicationOperation::new(on_demand_input(), None),
            &context,
        )
        .await
        .expect("queue succeeds");

        let result = process_blob_replication_batch(&context)
            .await
            .expect("mixed corrupt/valid drain succeeds");

        assert_eq!(result.processed, 1);
        assert_eq!(result.failed, 1);
        let jobs = read_jobs(&storage).await;
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].1.attempts, 1);
    }

    #[tokio::test]
    async fn corrupt_live_replication_obligation_only_is_deleted() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        write_corrupt_live_obligation(&storage, "000-corrupt-live-obligation").await;
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let result = process_blob_replication_batch(&context)
            .await
            .expect("corrupt-only live drain succeeds");

        assert_eq!(result.processed, 0);
        assert!(!result.has_more_due);
        assert!(result.next_due_after.is_none());
        assert!(!blob_replication_jobs_exist(&storage).await.unwrap());
    }

    #[tokio::test]
    async fn corrupt_live_replication_obligations_before_valid_are_deleted() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let version_id = Ulid::from_parts(5, 5);
        write_bucket(&storage, "bucket").await;
        write_replication_config(&storage, "bucket").await;
        write_materialized_version(&storage, "bucket", "key", version_id).await;
        for index in 0..LIVE_REPLICATION_OBLIGATION_BATCH_SIZE {
            let key = format!("000-corrupt-live-{index:03}");
            write_corrupt_live_obligation(&storage, &key).await;
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

        let first = process_blob_replication_batch(&context)
            .await
            .expect("corrupt live page drain succeeds");

        assert_eq!(first.processed, 0);
        assert!(first.has_more_due);
        assert_eq!(read_obligations(&storage).await.len(), 1);

        let second = process_blob_replication_batch(&context)
            .await
            .expect("valid live obligation drains after corrupt page");

        assert_eq!(second.processed, 2);
        assert_eq!(second.failed, 2);
        assert!(read_obligations(&storage).await.is_empty());
    }

    #[tokio::test]
    async fn live_replication_obligation_repairs_missing_jobs() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let version_id = Ulid::from_parts(4, 4);
        write_bucket(&storage, "bucket").await;
        write_replication_config(&storage, "bucket").await;
        write_materialized_version(&storage, "bucket", "key", version_id).await;
        write_live_obligation(&storage, version_id).await;
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let result = process_blob_replication_batch(&context)
            .await
            .expect("repair drain succeeds");

        assert_eq!(result.processed, 2);
        assert_eq!(result.failed, 2);
        assert!(read_obligations(&storage).await.is_empty());
        let jobs = read_jobs(&storage).await;
        assert_eq!(jobs.len(), 2);
        assert!(jobs.iter().all(|(_, job)| job.attempts == 1));
    }

    #[tokio::test]
    async fn failed_replication_drain_retains_job_with_retry_metadata() {
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
            QueueBlobReplicationOperation::new(on_demand_input(), None),
            &context,
        )
        .await
        .expect("queue succeeds");

        let result = process_blob_replication_batch(&context)
            .await
            .expect("drain stores retry metadata");

        assert_eq!(result.failed, 1);
        let jobs = read_jobs(&storage).await;
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].1.attempts, 1);
        assert!(jobs[0].1.last_error.is_some());
    }

    #[tokio::test]
    async fn successful_empty_scope_drain_deletes_job() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        write_bucket(&storage, "bucket").await;
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        drive(
            QueueBlobReplicationOperation::new(
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

        let result = process_blob_replication_batch(&context)
            .await
            .expect("drain succeeds");

        assert_eq!(result.succeeded, 1);
        assert!(read_jobs(&storage).await.is_empty());
    }
}
