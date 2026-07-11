use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{BLOB_VERSIONS_KEYSPACE, REFERENCE_METADATA_REFRESH_JOB_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::{BlobVersion, BlobVersionState, SourceMetadata, VersionKey};
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

use crate::driver::DriverContext;
use crate::queue_backoff::queue_retry_after_ms;

const REFRESH_SCAN_PAGE_SIZE: usize = 512;
const REFRESH_BATCH_SIZE: usize = 64;

pub const REFERENCE_METADATA_REFRESH_POLL_AFTER: Duration = Duration::from_secs(5);
pub const REFERENCE_METADATA_REFRESH_RETRY_AFTER: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReferenceMetadataRefresh {
    pub bucket: String,
    pub key: String,
    pub version_id: Ulid,
    pub metadata: SourceMetadata,
    pub refreshed_at: SystemTime,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReferenceMetadataRefreshJobRecord {
    pub refresh: ReferenceMetadataRefresh,
    pub due_at_ms: u64,
    pub attempts: u32,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueReferenceMetadataRefreshResult {
    pub queued: bool,
    pub scheduled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReferenceMetadataRefreshDrainResult {
    pub processed: usize,
    pub succeeded: usize,
    pub failed: usize,
    pub has_more_due: bool,
    pub next_due_after: Option<Duration>,
}

#[derive(Debug, Error, PartialEq)]
pub enum ReferenceMetadataRefreshQueueError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("reference metadata refresh failed: {0}")]
    Refresh(String),
    #[error("unexpected event while processing reference metadata refresh queue: {0}")]
    UnexpectedEvent(String),
}

#[derive(Serialize)]
struct ReferenceMetadataRefreshJobIdentity<'a> {
    bucket: &'a str,
    key: &'a str,
    version_id: Ulid,
    refreshed_at: SystemTime,
}

struct ReferenceMetadataRefreshJobScan {
    jobs: Vec<(Vec<u8>, ReferenceMetadataRefreshJobRecord)>,
    has_more_due: bool,
    next_due_at_ms: Option<u64>,
}

impl ReferenceMetadataRefreshJobRecord {
    pub fn new(refresh: ReferenceMetadataRefresh, due_at_ms: u64) -> Self {
        Self {
            refresh,
            due_at_ms,
            attempts: 0,
            last_error: None,
        }
    }
}

pub fn reference_metadata_refresh_job_key(
    refresh: &ReferenceMetadataRefresh,
) -> Result<Key, ConversionError> {
    let identity = ReferenceMetadataRefreshJobIdentity {
        bucket: &refresh.bucket,
        key: &refresh.key,
        version_id: refresh.version_id,
        refreshed_at: refresh.refreshed_at,
    };
    let mut key = b"v1".to_vec();
    key.extend(postcard::to_allocvec(&identity)?);
    Ok(ByteView::from(key))
}

fn reference_metadata_refresh_job_write_entry(
    record: &ReferenceMetadataRefreshJobRecord,
) -> Result<(String, Key, ByteView), ConversionError> {
    Ok((
        REFERENCE_METADATA_REFRESH_JOB_KEYSPACE.to_string(),
        reference_metadata_refresh_job_key(&record.refresh)?,
        ByteView::from(postcard::to_allocvec(record)?),
    ))
}

fn reference_metadata_refresh_job_preferred(
    candidate: &ReferenceMetadataRefreshJobRecord,
    current: &ReferenceMetadataRefreshJobRecord,
) -> bool {
    (candidate.attempts, candidate.due_at_ms) > (current.attempts, current.due_at_ms)
}

pub fn schedule_reference_metadata_refresh_drain_effect() -> Effect {
    Effect::Task(TaskEffect::ResetTimer {
        key: TaskKey::DrainReferenceMetadataRefreshQueue,
        after: Duration::ZERO,
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum QueueReferenceMetadataRefreshState {
    Init,
    ReadExisting,
    WriteJob,
    ScheduleDrain,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct QueueReferenceMetadataRefreshOperation {
    refresh: ReferenceMetadataRefresh,
    state: QueueReferenceMetadataRefreshState,
    output: Option<Result<QueueReferenceMetadataRefreshResult, ReferenceMetadataRefreshQueueError>>,
}

impl QueueReferenceMetadataRefreshOperation {
    pub fn new(refresh: ReferenceMetadataRefresh) -> Self {
        Self {
            refresh,
            state: QueueReferenceMetadataRefreshState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: ReferenceMetadataRefreshQueueError) -> Effects {
        self.state = QueueReferenceMetadataRefreshState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn read_existing(&mut self) -> Effects {
        let key = match reference_metadata_refresh_job_key(&self.refresh) {
            Ok(key) => key,
            Err(error) => return self.fail(error.into()),
        };
        self.state = QueueReferenceMetadataRefreshState::ReadExisting;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: REFERENCE_METADATA_REFRESH_JOB_KEYSPACE.to_string(),
            key,
            txn_id: None,
        })]
    }

    fn write_job(&mut self) -> Effects {
        let record =
            ReferenceMetadataRefreshJobRecord::new(self.refresh.clone(), unix_timestamp_millis());
        let (key_space, key, value) = match reference_metadata_refresh_job_write_entry(&record) {
            Ok(entry) => entry,
            Err(error) => return self.fail(error.into()),
        };
        self.state = QueueReferenceMetadataRefreshState::WriteJob;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space,
            key,
            value,
            txn_id: None,
        })]
    }

    fn schedule_drain(&mut self) -> Effects {
        self.state = QueueReferenceMetadataRefreshState::ScheduleDrain;
        smallvec![schedule_reference_metadata_refresh_drain_effect()]
    }

    fn finish(&mut self, queued: bool, scheduled: bool) -> Effects {
        self.state = QueueReferenceMetadataRefreshState::Finish;
        self.output = Some(Ok(QueueReferenceMetadataRefreshResult {
            queued,
            scheduled,
        }));
        smallvec![]
    }
}

impl Operation for QueueReferenceMetadataRefreshOperation {
    type Output = QueueReferenceMetadataRefreshResult;
    type Error = ReferenceMetadataRefreshQueueError;

    fn start(&mut self) -> Effects {
        self.read_existing()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            QueueReferenceMetadataRefreshState::Init => self.read_existing(),
            QueueReferenceMetadataRefreshState::ReadExisting => match event {
                Event::Storage(StorageEvent::ReadResult {
                    value: Some(value), ..
                }) => {
                    let record = ReferenceMetadataRefreshJobRecord::new(
                        self.refresh.clone(),
                        unix_timestamp_millis(),
                    );
                    match postcard::from_bytes::<ReferenceMetadataRefreshJobRecord>(&value) {
                        Ok(existing)
                            if reference_metadata_refresh_job_preferred(&existing, &record) =>
                        {
                            self.schedule_drain()
                        }
                        Ok(_) | Err(_) => self.write_job(),
                    }
                }
                Event::Storage(StorageEvent::ReadResult { value: None, .. }) => self.write_job(),
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.fail(ReferenceMetadataRefreshQueueError::UnexpectedEvent(
                    format!("{other:?}"),
                )),
            },
            QueueReferenceMetadataRefreshState::WriteJob => match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => self.schedule_drain(),
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.fail(ReferenceMetadataRefreshQueueError::UnexpectedEvent(
                    format!("{other:?}"),
                )),
            },
            QueueReferenceMetadataRefreshState::ScheduleDrain => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => self.finish(true, true),
                Event::Task(TaskEvent::Error { .. }) => self.finish(true, false),
                other => {
                    warn!(event = ?other, "Reference metadata refresh job persisted but drain scheduling returned an unexpected event");
                    self.finish(true, false)
                }
            },
            QueueReferenceMetadataRefreshState::Finish => smallvec![],
            QueueReferenceMetadataRefreshState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            QueueReferenceMetadataRefreshState::Finish | QueueReferenceMetadataRefreshState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.output {
            Some(Ok(result)) => Ok(result),
            Some(Err(error)) => Err(error),
            None => Err(ReferenceMetadataRefreshQueueError::UnexpectedEvent(
                "reference metadata refresh queue operation finished without output".to_string(),
            )),
        }
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

pub async fn refresh_reference_metadata(
    context: Arc<DriverContext>,
    refresh: ReferenceMetadataRefresh,
) -> Result<(), String> {
    refresh_reference_metadata_with_context(context.as_ref(), refresh).await
}

pub async fn refresh_reference_metadata_with_context(
    context: &DriverContext,
    refresh: ReferenceMetadataRefresh,
) -> Result<(), String> {
    let version_key = VersionKey::new(&refresh.bucket, &refresh.key, refresh.version_id)
        .to_bytes()
        .map_err(|err| err.to_string())?;
    let txn_id = match context
        .storage_handle
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        Event::Storage(StorageEvent::Error { error }) => return Err(error.to_string()),
        other => return Err(format!("unexpected start transaction event: {other:?}")),
    };

    let refreshed_value = match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: version_key.clone().into(),
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => {
            let version = match BlobVersion::from_bytes(value.as_ref()) {
                Ok(version) => version,
                Err(err) => {
                    abort_reference_refresh(context, txn_id).await;
                    return Err(err.to_string());
                }
            };
            let BlobVersion {
                created_at,
                created_by,
                state,
            } = version;
            let BlobVersionState::Reference {
                source,
                last_refresh,
                ..
            } = state
            else {
                abort_reference_refresh(context, txn_id).await;
                return Ok(());
            };
            if refresh.refreshed_at <= last_refresh {
                None
            } else {
                Some(
                    match BlobVersion::reference(
                        source,
                        refresh.metadata,
                        created_at,
                        created_by,
                        refresh.refreshed_at,
                    )
                    .to_bytes()
                    {
                        Ok(value) => value,
                        Err(err) => {
                            abort_reference_refresh(context, txn_id).await;
                            return Err(err.to_string());
                        }
                    },
                )
            }
        }
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => {
            abort_reference_refresh(context, txn_id).await;
            return Ok(());
        }
        Event::Storage(StorageEvent::Error { error }) => {
            abort_reference_refresh(context, txn_id).await;
            return Err(error.to_string());
        }
        other => {
            abort_reference_refresh(context, txn_id).await;
            return Err(format!("unexpected version read event: {other:?}"));
        }
    };

    if let Some(refreshed_value) = refreshed_value {
        match context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: version_key.into(),
                value: refreshed_value.into(),
                txn_id: Some(txn_id),
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            Event::Storage(StorageEvent::Error { error }) => {
                abort_reference_refresh(context, txn_id).await;
                return Err(error.to_string());
            }
            other => {
                abort_reference_refresh(context, txn_id).await;
                return Err(format!("unexpected version write event: {other:?}"));
            }
        }
    }

    match context
        .storage_handle
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => {
            abort_reference_refresh(context, txn_id).await;
            Err(error.to_string())
        }
        other => {
            abort_reference_refresh(context, txn_id).await;
            Err(format!("unexpected commit event: {other:?}"))
        }
    }
}

async fn abort_reference_refresh(context: &DriverContext, txn_id: Ulid) {
    let _ = context
        .storage_handle
        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
        .await;
}

pub async fn restore_reference_metadata_refresh_timer(
    storage: &StorageHandle,
    task_handle: &TaskHandle,
) {
    match next_reference_metadata_refresh_timer_after(storage).await {
        Ok(None) => {}
        Ok(Some(after)) => {
            let event = task_handle
                .send_effect(Effect::Task(TaskEffect::ResetTimer {
                    key: TaskKey::DrainReferenceMetadataRefreshQueue,
                    after,
                }))
                .await;
            if let Event::Task(TaskEvent::Error { message, .. }) = event {
                warn!(message = %message, "Failed to restore reference metadata refresh timer");
            }
        }
        Err(error) => warn!(error = ?error, "Failed to scan reference metadata refresh jobs"),
    }
}

pub async fn reference_metadata_refresh_jobs_exist(
    storage: &StorageHandle,
) -> Result<bool, ReferenceMetadataRefreshQueueError> {
    match storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: REFERENCE_METADATA_REFRESH_JOB_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit: 1,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => Ok(!values.is_empty()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(ReferenceMetadataRefreshQueueError::UnexpectedEvent(
            format!("{other:?}"),
        )),
    }
}

pub async fn next_reference_metadata_refresh_timer_after(
    storage: &StorageHandle,
) -> Result<Option<Duration>, ReferenceMetadataRefreshQueueError> {
    let now_ms = unix_timestamp_millis();
    let scan = scan_due_reference_metadata_refresh_jobs(storage, now_ms, 1).await?;
    if !scan.jobs.is_empty() || scan.has_more_due {
        return Ok(Some(Duration::ZERO));
    }

    Ok(scan
        .next_due_at_ms
        .map(|due_at_ms| due_after(now_ms, due_at_ms)))
}

pub async fn process_reference_metadata_refresh_batch(
    context: &DriverContext,
) -> Result<ReferenceMetadataRefreshDrainResult, ReferenceMetadataRefreshQueueError> {
    let batch_started = Instant::now();
    let now_ms = unix_timestamp_millis();
    let scan = scan_due_reference_metadata_refresh_jobs(
        &context.storage_handle,
        now_ms,
        REFRESH_BATCH_SIZE,
    )
    .await?;
    let mut next_due_at_ms = scan.next_due_at_ms;
    let has_more_due = scan.has_more_due;
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
        match refresh_reference_metadata_with_context(context, job.refresh.clone()).await {
            Ok(()) => {
                delete_reference_metadata_refresh_job(&context.storage_handle, job_key).await?;
                succeeded = succeeded.saturating_add(1);
            }
            Err(error) => {
                let retry_due_at = reschedule_reference_metadata_refresh_job(
                    &context.storage_handle,
                    job_key,
                    &job,
                    error,
                )
                .await?;
                next_due_at_ms = min_due_at(next_due_at_ms, retry_due_at);
                failed = failed.saturating_add(1);
            }
        }
    }

    if job_count > 0 {
        info!(
            event = "pipeline.reference_metadata_refresh.summary",
            jobs = job_count,
            succeeded,
            failed,
            scan_ms = duration_ms(scan_elapsed),
            total_ms = duration_ms(batch_started.elapsed()),
            oldest_lag_ms,
            has_more_due,
            "Reference metadata refresh batch summary"
        );
    }

    Ok(ReferenceMetadataRefreshDrainResult {
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

async fn scan_due_reference_metadata_refresh_jobs(
    storage: &StorageHandle,
    now_ms: u64,
    limit: usize,
) -> Result<ReferenceMetadataRefreshJobScan, ReferenceMetadataRefreshQueueError> {
    let mut start_after = None;
    let mut jobs = Vec::new();
    let mut next_due_at_ms = None;
    loop {
        let event = storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: REFERENCE_METADATA_REFRESH_JOB_KEYSPACE.to_string(),
                prefix: None,
                start: start_after.take().map(IterStart::After),
                limit: REFRESH_SCAN_PAGE_SIZE,
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
                return Err(ReferenceMetadataRefreshQueueError::UnexpectedEvent(
                    format!("{other:?}"),
                ));
            }
        };

        for (key, value) in values {
            let mut key = key.to_vec();
            let job = match postcard::from_bytes::<ReferenceMetadataRefreshJobRecord>(&value) {
                Ok(job) => job,
                Err(error) => {
                    warn!(error = %error, key = ?key, "Deleting malformed reference metadata refresh job");
                    delete_reference_metadata_refresh_job(storage, key).await?;
                    continue;
                }
            };
            let canonical_key = reference_metadata_refresh_job_key(&job.refresh)?.to_vec();
            if canonical_key.as_slice() != key.as_slice() {
                warn!(key = ?key, "Repairing reference metadata refresh job stored under non-canonical key");
                if let Some(existing) =
                    read_reference_metadata_refresh_job_at_key(storage, &canonical_key).await?
                    && reference_metadata_refresh_job_preferred(&existing, &job)
                {
                    delete_reference_metadata_refresh_job(storage, key).await?;
                    if existing.due_at_ms > now_ms {
                        next_due_at_ms = min_due_at(next_due_at_ms, existing.due_at_ms);
                        continue;
                    }
                    jobs.push((canonical_key, existing));
                    if jobs.len() >= limit {
                        return Ok(ReferenceMetadataRefreshJobScan {
                            jobs,
                            has_more_due: true,
                            next_due_at_ms,
                        });
                    }
                    continue;
                }
                write_reference_metadata_refresh_job(storage, &job).await?;
                delete_reference_metadata_refresh_job(storage, key).await?;
                key = canonical_key;
            }
            if let Some((existing_key, existing)) =
                find_decoded_reference_metadata_refresh_duplicate(
                    storage,
                    &job,
                    Some(key.as_slice()),
                )
                .await?
                && reference_metadata_refresh_job_preferred(&existing, &job)
            {
                let existing_canonical_key =
                    reference_metadata_refresh_job_key(&existing.refresh)?.to_vec();
                write_reference_metadata_refresh_job(storage, &existing).await?;
                if existing_key.as_slice() != existing_canonical_key.as_slice() {
                    delete_reference_metadata_refresh_job(storage, existing_key).await?;
                }
                if key.as_slice() != existing_canonical_key.as_slice() {
                    delete_reference_metadata_refresh_job(storage, key).await?;
                }
                if existing.due_at_ms > now_ms {
                    next_due_at_ms = min_due_at(next_due_at_ms, existing.due_at_ms);
                    continue;
                }
                jobs.push((existing_canonical_key, existing));
                if jobs.len() >= limit {
                    return Ok(ReferenceMetadataRefreshJobScan {
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
                return Ok(ReferenceMetadataRefreshJobScan {
                    jobs,
                    has_more_due: true,
                    next_due_at_ms,
                });
            }
        }

        match next_start_after {
            Some(next) => start_after = Some(next),
            None => {
                return Ok(ReferenceMetadataRefreshJobScan {
                    jobs,
                    has_more_due: false,
                    next_due_at_ms,
                });
            }
        }
    }
}

async fn find_decoded_reference_metadata_refresh_duplicate(
    storage: &StorageHandle,
    job: &ReferenceMetadataRefreshJobRecord,
    skip_key: Option<&[u8]>,
) -> Result<Option<(Vec<u8>, ReferenceMetadataRefreshJobRecord)>, ReferenceMetadataRefreshQueueError>
{
    let canonical_key = reference_metadata_refresh_job_key(&job.refresh)?.to_vec();
    let mut selected = None;
    let mut start_after = None;
    loop {
        let event = storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: REFERENCE_METADATA_REFRESH_JOB_KEYSPACE.to_string(),
                prefix: None,
                start: start_after.take().map(IterStart::After),
                limit: REFRESH_SCAN_PAGE_SIZE,
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
                return Err(ReferenceMetadataRefreshQueueError::UnexpectedEvent(
                    format!("{other:?}"),
                ));
            }
        };

        for (key, value) in values {
            if skip_key.is_some_and(|skip_key| key.as_ref() == skip_key) {
                continue;
            }
            let Ok(candidate) = postcard::from_bytes::<ReferenceMetadataRefreshJobRecord>(&value)
            else {
                continue;
            };
            if reference_metadata_refresh_job_key(&candidate.refresh)?.as_ref()
                != canonical_key.as_slice()
            {
                continue;
            }
            match selected.as_mut() {
                Some((_, selected_job))
                    if reference_metadata_refresh_job_preferred(&candidate, selected_job) =>
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

async fn read_reference_metadata_refresh_job_at_key(
    storage: &StorageHandle,
    key: &[u8],
) -> Result<Option<ReferenceMetadataRefreshJobRecord>, ReferenceMetadataRefreshQueueError> {
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: REFERENCE_METADATA_REFRESH_JOB_KEYSPACE.to_string(),
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
        other => Err(ReferenceMetadataRefreshQueueError::UnexpectedEvent(
            format!("{other:?}"),
        )),
    }
}

async fn write_reference_metadata_refresh_job(
    storage: &StorageHandle,
    job: &ReferenceMetadataRefreshJobRecord,
) -> Result<(), ReferenceMetadataRefreshQueueError> {
    let (key_space, key, value) = reference_metadata_refresh_job_write_entry(job)?;
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
        other => Err(ReferenceMetadataRefreshQueueError::UnexpectedEvent(
            format!("{other:?}"),
        )),
    }
}

async fn delete_reference_metadata_refresh_job(
    storage: &StorageHandle,
    key: Vec<u8>,
) -> Result<(), ReferenceMetadataRefreshQueueError> {
    match storage
        .send_storage_effect(StorageEffect::Delete {
            key_space: REFERENCE_METADATA_REFRESH_JOB_KEYSPACE.to_string(),
            key: ByteView::from(key),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::DeleteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(ReferenceMetadataRefreshQueueError::UnexpectedEvent(
            format!("{other:?}"),
        )),
    }
}

async fn reschedule_reference_metadata_refresh_job(
    storage: &StorageHandle,
    key: Vec<u8>,
    job: &ReferenceMetadataRefreshJobRecord,
    error: String,
) -> Result<u64, ReferenceMetadataRefreshQueueError> {
    let attempts = job.attempts.saturating_add(1);
    let due_at_ms = unix_timestamp_millis().saturating_add(queue_retry_after_ms(attempts));
    let next_job = ReferenceMetadataRefreshJobRecord {
        refresh: job.refresh.clone(),
        due_at_ms,
        attempts,
        last_error: Some(error),
    };
    match storage
        .send_storage_effect(StorageEffect::Write {
            key_space: REFERENCE_METADATA_REFRESH_JOB_KEYSPACE.to_string(),
            key: ByteView::from(key),
            value: ByteView::from(postcard::to_allocvec(&next_job).map_err(ConversionError::from)?),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(due_at_ms),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(ReferenceMetadataRefreshQueueError::UnexpectedEvent(
            format!("{other:?}"),
        )),
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
        PortableSourceDescriptor, RealmId, SourceConnectorKind, StagingStrategy,
        VersionSourceBinding,
    };
    use aruna_storage::FjallStorage;
    use tempfile::tempdir;

    struct TestState {
        _storage_dir: tempfile::TempDir,
        context: DriverContext,
        bucket: String,
        key: String,
        version_id: Ulid,
        created_by: UserId,
    }

    fn setup_state() -> TestState {
        let storage_dir = tempdir().unwrap();
        let storage_handle = FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let realm_id = RealmId::from_bytes([9u8; 32]);
        TestState {
            _storage_dir: storage_dir,
            context: DriverContext {
                storage_handle,
                net_handle: None,
                blob_handle: None,
                metadata_handle: None,
                task_handle: None,
            },
            bucket: "bucket".to_string(),
            key: "key".to_string(),
            version_id: Ulid::r#gen(),
            created_by: UserId::local(Ulid::r#gen(), realm_id),
        }
    }

    fn source_binding() -> VersionSourceBinding {
        VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: PortableSourceDescriptor {
                kind: SourceConnectorKind::Http,
                public_config: std::collections::HashMap::new(),
                source_path: "source/path".to_string(),
                version_selector: None,
                capabilities: Vec::new(),
                origin_node_id: None,
            },
            connector_id: None,
        }
    }

    fn source_metadata(content_length: u64, etag: &str) -> SourceMetadata {
        SourceMetadata {
            content_length,
            content_type: Some("application/octet-stream".to_string()),
            etag: Some(etag.to_string()),
            last_modified: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(content_length)),
            source_version: None,
        }
    }

    fn refresh(
        test: &TestState,
        metadata: SourceMetadata,
        refreshed_at: SystemTime,
    ) -> ReferenceMetadataRefresh {
        ReferenceMetadataRefresh {
            bucket: test.bucket.clone(),
            key: test.key.clone(),
            version_id: test.version_id,
            metadata,
            refreshed_at,
        }
    }

    async fn write_reference_version(
        test: &TestState,
        cached_metadata: SourceMetadata,
        last_refresh: SystemTime,
    ) {
        let version = BlobVersion::reference(
            source_binding(),
            cached_metadata,
            SystemTime::UNIX_EPOCH,
            test.created_by,
            last_refresh,
        );
        match test
            .context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: VersionKey::new(&test.bucket, &test.key, test.version_id)
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

    async fn assert_reference_state(
        test: &TestState,
        expected_metadata: &SourceMetadata,
        expected_last_refresh: SystemTime,
    ) {
        let Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) = test
            .context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: VersionKey::new(&test.bucket, &test.key, test.version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                txn_id: None,
            })
            .await
        else {
            panic!("unexpected version read event");
        };
        let version = BlobVersion::from_bytes(value.as_ref()).unwrap();
        let BlobVersionState::Reference {
            cached_metadata,
            last_refresh,
            ..
        } = version.state
        else {
            panic!("version was not a reference");
        };
        assert_eq!(cached_metadata, *expected_metadata);
        assert_eq!(last_refresh, expected_last_refresh);
    }

    async fn read_jobs(storage: &StorageHandle) -> Vec<ReferenceMetadataRefreshJobRecord> {
        match storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: REFERENCE_METADATA_REFRESH_JOB_KEYSPACE.to_string(),
                prefix: None,
                start: None,
                limit: 16,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::IterResult { values, .. }) => values
                .into_iter()
                .map(|(_, value)| postcard::from_bytes(&value).expect("refresh job decodes"))
                .collect(),
            other => panic!("unexpected storage event: {other:?}"),
        }
    }

    async fn write_corrupt_job(storage: &StorageHandle, key: &str) {
        match storage
            .send_storage_effect(StorageEffect::Write {
                key_space: REFERENCE_METADATA_REFRESH_JOB_KEYSPACE.to_string(),
                key: ByteView::from(key.as_bytes().to_vec()),
                value: ByteView::from(Vec::new()),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected corrupt refresh job write event: {other:?}"),
        }
    }

    #[tokio::test]
    async fn queued_reference_metadata_refresh_drains_and_preserves_stale_guard() {
        let test = setup_state();
        let last_refresh = SystemTime::UNIX_EPOCH + Duration::from_secs(20);
        let original_metadata = source_metadata(10, "original");
        write_reference_version(&test, original_metadata.clone(), last_refresh).await;

        crate::driver::drive(
            QueueReferenceMetadataRefreshOperation::new(refresh(
                &test,
                source_metadata(20, "older"),
                SystemTime::UNIX_EPOCH + Duration::from_secs(10),
            )),
            &test.context,
        )
        .await
        .expect("queue succeeds");
        let result = process_reference_metadata_refresh_batch(&test.context)
            .await
            .expect("drain succeeds");

        assert_eq!(result.succeeded, 1);
        assert!(read_jobs(&test.context.storage_handle).await.is_empty());
        assert_reference_state(&test, &original_metadata, last_refresh).await;
    }

    #[tokio::test]
    async fn queued_reference_metadata_refresh_updates_newer_cache() {
        let test = setup_state();
        let last_refresh = SystemTime::UNIX_EPOCH + Duration::from_secs(20);
        let refreshed_at = SystemTime::UNIX_EPOCH + Duration::from_secs(30);
        let new_metadata = source_metadata(20, "newer");
        write_reference_version(&test, source_metadata(10, "original"), last_refresh).await;

        crate::driver::drive(
            QueueReferenceMetadataRefreshOperation::new(refresh(
                &test,
                new_metadata.clone(),
                refreshed_at,
            )),
            &test.context,
        )
        .await
        .expect("queue succeeds");
        process_reference_metadata_refresh_batch(&test.context)
            .await
            .expect("drain succeeds");

        assert_reference_state(&test, &new_metadata, refreshed_at).await;
    }

    #[tokio::test]
    async fn duplicate_reference_metadata_refresh_keeps_newest_job() {
        let test = setup_state();
        let last_refresh = SystemTime::UNIX_EPOCH;
        let newer = SystemTime::UNIX_EPOCH + Duration::from_secs(30);
        let older = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        let new_metadata = source_metadata(30, "newer");
        write_reference_version(&test, source_metadata(1, "original"), last_refresh).await;

        crate::driver::drive(
            QueueReferenceMetadataRefreshOperation::new(refresh(
                &test,
                new_metadata.clone(),
                newer,
            )),
            &test.context,
        )
        .await
        .expect("newer queue succeeds");
        crate::driver::drive(
            QueueReferenceMetadataRefreshOperation::new(refresh(
                &test,
                source_metadata(10, "older"),
                older,
            )),
            &test.context,
        )
        .await
        .expect("older queue succeeds");

        let jobs = read_jobs(&test.context.storage_handle).await;
        assert_eq!(jobs.len(), 2);

        let result = process_reference_metadata_refresh_batch(&test.context)
            .await
            .expect("drain succeeds");
        assert_eq!(result.succeeded, 2);
        assert_reference_state(&test, &new_metadata, newer).await;
    }

    #[tokio::test]
    async fn duplicate_reference_metadata_refresh_preserves_future_retry() {
        let test = setup_state();
        let refreshed_at = SystemTime::UNIX_EPOCH + Duration::from_secs(30);
        let refresh = refresh(&test, source_metadata(30, "future"), refreshed_at);
        let future_job = ReferenceMetadataRefreshJobRecord {
            refresh: refresh.clone(),
            due_at_ms: unix_timestamp_millis().saturating_add(60_000),
            attempts: 1,
            last_error: Some("transient".to_string()),
        };
        let (key_space, key, value) =
            reference_metadata_refresh_job_write_entry(&future_job).expect("future job serializes");
        match test
            .context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space,
                key,
                value,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected future refresh job write event: {other:?}"),
        }

        crate::driver::drive(
            QueueReferenceMetadataRefreshOperation::new(refresh),
            &test.context,
        )
        .await
        .expect("duplicate queue succeeds");

        assert_eq!(
            read_jobs(&test.context.storage_handle).await,
            vec![future_job]
        );
    }

    #[tokio::test]
    async fn future_reference_metadata_refresh_does_not_report_more_due() {
        let test = setup_state();
        let due_at_ms = unix_timestamp_millis().saturating_add(60_000);
        let record = ReferenceMetadataRefreshJobRecord::new(
            refresh(
                &test,
                source_metadata(20, "future"),
                SystemTime::UNIX_EPOCH + Duration::from_secs(20),
            ),
            due_at_ms,
        );
        let (key_space, key, value) = reference_metadata_refresh_job_write_entry(&record).unwrap();
        match test
            .context
            .storage_handle
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

        let result = process_reference_metadata_refresh_batch(&test.context)
            .await
            .expect("future-only drain succeeds");

        assert_eq!(result.processed, 0);
        assert!(!result.has_more_due);
        assert!(result.next_due_after.is_some());
    }

    #[tokio::test]
    async fn corrupt_reference_metadata_refresh_job_only_is_deleted() {
        let test = setup_state();
        write_corrupt_job(&test.context.storage_handle, "000-corrupt-refresh-job").await;

        let result = process_reference_metadata_refresh_batch(&test.context)
            .await
            .expect("corrupt-only refresh drain succeeds");

        assert_eq!(result.processed, 0);
        assert!(!result.has_more_due);
        assert!(result.next_due_after.is_none());
        assert!(
            !reference_metadata_refresh_jobs_exist(&test.context.storage_handle)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn corrupt_reference_metadata_refresh_job_before_valid_is_deleted() {
        let test = setup_state();
        write_corrupt_job(&test.context.storage_handle, "000-corrupt-refresh-job").await;
        crate::driver::drive(
            QueueReferenceMetadataRefreshOperation::new(refresh(
                &test,
                source_metadata(20, "valid"),
                SystemTime::UNIX_EPOCH + Duration::from_secs(20),
            )),
            &test.context,
        )
        .await
        .expect("queue succeeds");

        let result = process_reference_metadata_refresh_batch(&test.context)
            .await
            .expect("mixed corrupt/valid refresh drain succeeds");

        assert_eq!(result.processed, 1);
        assert_eq!(result.succeeded, 1);
        assert!(read_jobs(&test.context.storage_handle).await.is_empty());
    }
}
