use std::time::{Duration, Instant};

use aruna_core::NodeId;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{
    BLOB_LIVE_REPLICATION_OBLIGATION_KEYSPACE, BLOB_REPLICATION_JOB_KEYSPACE,
    S3_BUCKET_REPLICATION_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::{AuthContext, BucketReplicationConfig};
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::telemetry::duration_ms;
use aruna_core::types::{Effects, Key};
use aruna_core::util::unix_timestamp_millis;
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;
use byteview::ByteView;
use serde::{Deserialize, Serialize};
use smallvec::smallvec;
use thiserror::Error;
use tracing::{info, warn};
use ulid::Ulid;

use super::protocol::ReplicationMode;
use super::version_replication::{
    ReplicateScopeError, ReplicateScopeInput, ReplicateScopeOperation, ReplicateScopeTarget,
};
use crate::driver::{DriverContext, drive};
use crate::queue_backoff::queue_retry_after_ms;

const REPLICATION_SCAN_PAGE_SIZE: usize = 512;
const REPLICATION_BATCH_SIZE: usize = 64;
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveReplicationObligationRecord {
    pub local_node_id: NodeId,
    pub auth_context: AuthContext,
    pub bucket: String,
    pub key: String,
    pub version_id: Ulid,
    pub delete_marker: bool,
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
        ByteView::from(postcard::to_allocvec(record)?),
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
        ByteView::from(postcard::to_allocvec(record)?),
    ))
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
    let (key_space, key, value) = live_replication_obligation_write_entry(&record)?;
    Ok(Effect::Storage(StorageEffect::Write {
        key_space,
        key,
        value,
        txn_id,
    }))
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
                }) => match postcard::from_bytes::<BlobReplicationJobRecord>(&value) {
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
    pending_jobs: Vec<BlobReplicationJobRecord>,
    output: Option<Result<QueueBlobReplicationResult, BlobReplicationQueueError>>,
}

impl QueueLiveVersionReplicationOperation {
    pub fn new(input: QueueLiveVersionReplicationInput) -> Self {
        Self {
            input,
            state: QueueLiveVersionReplicationState::Init,
            queued: 0,
            pending_jobs: Vec::new(),
            output: None,
        }
    }

    fn fail(&mut self, error: BlobReplicationQueueError) -> Effects {
        self.state = QueueLiveVersionReplicationState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn read_config(&mut self) -> Effects {
        self.state = QueueLiveVersionReplicationState::ReadConfig;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_BUCKET_REPLICATION_KEYSPACE.to_string(),
            key: self.input.bucket.as_bytes().to_vec().into(),
            txn_id: None,
        })]
    }

    fn jobs_from_config(&self, config: BucketReplicationConfig) -> Vec<BlobReplicationJobRecord> {
        live_replication_jobs_from_config(
            self.input.local_node_id,
            &self.input.auth_context,
            &self.input.bucket,
            &self.input.key,
            self.input.version_id,
            self.input.delete_marker,
            config,
        )
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
        self.read_config()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            QueueLiveVersionReplicationState::Init => self.read_config(),
            QueueLiveVersionReplicationState::ReadConfig => match event {
                Event::Storage(StorageEvent::ReadResult { value: None, .. }) => {
                    self.delete_obligation()
                }
                Event::Storage(StorageEvent::ReadResult {
                    value: Some(value), ..
                }) => match BucketReplicationConfig::from_bytes(value.as_ref()) {
                    Ok(config) => self.read_existing_jobs(self.jobs_from_config(config)),
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
                            Some(value) => {
                                match postcard::from_bytes::<BlobReplicationJobRecord>(&value) {
                                    Ok(existing)
                                        if blob_replication_job_preferred(&existing, &job) => {}
                                    Ok(_) | Err(_) => jobs.push(job),
                                }
                            }
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
            Ok(()) => {
                delete_blob_replication_job(&context.storage_handle, job_key).await?;
                succeeded = succeeded.saturating_add(1);
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

async fn process_blob_replication_job(
    context: &DriverContext,
    job: &BlobReplicationJobRecord,
) -> Result<(), String> {
    match drive(ReplicateScopeOperation::new(job.input.clone()), context).await {
        Ok(Some(Ok(result))) if result.failed == 0 => Ok(()),
        Ok(Some(Ok(result))) => Err(format!(
            "replication completed with {} replicated, {} skipped, {} failed",
            result.replicated, result.skipped, result.failed
        )),
        Ok(Some(Err(error))) => Err(error.to_string()),
        Ok(None) => Err("replication produced no result".to_string()),
        Err(error) => Err(error.to_string()),
    }
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
        let queued =
            match read_bucket_replication_config(&context.storage_handle, &obligation.bucket)
                .await?
            {
                Some(config) => {
                    write_live_replication_jobs(&context.storage_handle, &obligation, config)
                        .await?
                }
                None => 0,
            };
        delete_live_replication_obligation(&context.storage_handle, obligation_key).await?;
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
                match postcard::from_bytes::<LiveReplicationObligationRecord>(&value) {
                    Ok(record) => obligations.push((key.to_vec(), record)),
                    Err(error) => {
                        let key = key.to_vec();
                        warn!(error = %error, key = ?key, "Deleting malformed live replication obligation");
                        delete_live_replication_obligation(storage, key).await?;
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

async fn write_live_replication_jobs(
    storage: &StorageHandle,
    obligation: &LiveReplicationObligationRecord,
    config: BucketReplicationConfig,
) -> Result<usize, BlobReplicationQueueError> {
    let jobs = live_replication_jobs_from_config(
        obligation.local_node_id,
        &obligation.auth_context,
        &obligation.bucket,
        &obligation.key,
        obligation.version_id,
        obligation.delete_marker,
        config,
    );
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
            Some(value) => match postcard::from_bytes::<BlobReplicationJobRecord>(&value) {
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

async fn delete_live_replication_obligation(
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
            let job = match postcard::from_bytes::<BlobReplicationJobRecord>(&value) {
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
            let Ok(candidate) = postcard::from_bytes::<BlobReplicationJobRecord>(&value) else {
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
        }) => Ok(Some(
            postcard::from_bytes(&value).map_err(ConversionError::from)?,
        )),
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
    };
    match storage
        .send_storage_effect(StorageEffect::Write {
            key_space: BLOB_REPLICATION_JOB_KEYSPACE.to_string(),
            key: ByteView::from(key),
            value: ByteView::from(postcard::to_allocvec(&next_job).map_err(ConversionError::from)?),
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
    use aruna_core::keyspaces::BLOB_VERSIONS_KEYSPACE;
    use aruna_core::structs::{
        BlobVersion, BucketInfo, BucketReplicationTarget, RealmId, VersionKey,
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
                        postcard::from_bytes(&value).expect("replication job decodes"),
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
                .map(|(_, value)| postcard::from_bytes(&value).expect("obligation decodes"))
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
    async fn duplicate_blob_replication_request_preserves_future_retry() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let future_job = BlobReplicationJobRecord {
            input: on_demand_input(),
            source_delete_marker: None,
            due_at_ms: unix_timestamp_millis().saturating_add(60_000),
            attempts: 1,
            last_error: Some("transient".to_string()),
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
