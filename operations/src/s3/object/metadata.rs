//! Queues, schedules and drains reference metadata refresh jobs for object versions.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{BLOB_VERSIONS_KEYSPACE, REFRESH_DUE_KEYSPACE, REFRESH_JOB_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::source_access::{ObservationMatch, SourceMetadata};
use aruna_core::structs::storage::blob::{BlobVersion, BlobVersionState, VersionKey};
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::telemetry::duration_ms;
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::{Effects, Key};
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;
use byteview::ByteView;
use serde::{Deserialize, Serialize};
use smallvec::smallvec;
use thiserror::Error;
use tracing::{info, warn};
use ulid::Ulid;

use crate::driver::DriverContext;
use crate::driver::drive;
use crate::node::usage_stats::schedule_snapshot_publish;
use crate::replication::queue::schedule_blob_drain;
use crate::s3::object::refresh_mint::MintSuccessorOperation;
use crate::s3::object::refresh_observe::ObserveSourceOperation;
use crate::tasks::queue_backoff::{due_after, min_due_at, retry_delay_ms};
use crate::tasks::task_persistence::persist_task_effect;
use aruna_core::id::NodeId;
use aruna_core::structs::identity::auth::AuthContext;

const REFRESH_SCAN_PAGE: usize = 512;
const REFRESH_BATCH_SIZE: usize = 64;
// Queue writes that lose to a racing drain or request reread the job, up to this often.
const QUEUE_CONFLICT_RETRIES: u32 = 8;

pub const REFRESH_RETRY_AFTER: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReferenceRefresh {
    pub bucket: String,
    pub key: String,
    pub version_id: Ulid,
    /// `None` asks the worker to observe the source first.
    pub metadata: Option<SourceMetadata>,
    pub refreshed_at: SystemTime,
    /// The node and caller a confirmed change is recorded and replicated for.
    pub node_id: NodeId,
    pub auth_context: AuthContext,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReferenceRefreshRecord {
    pub refresh: ReferenceRefresh,
    pub due_at_ms: u64,
    pub attempts: u32,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueRefreshResult {
    pub queued: bool,
    pub scheduled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReferenceDrainResult {
    pub processed: usize,
    pub succeeded: usize,
    pub failed: usize,
    pub has_more_due: bool,
    pub next_due_after: Option<Duration>,
}

#[derive(Debug, Error, PartialEq)]
pub enum ReferenceRefreshError {
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
struct ReferenceRefreshIdentity<'a> {
    bucket: &'a str,
    key: &'a str,
    version_id: Ulid,
}

struct ReferenceRefreshScan {
    jobs: Vec<(Vec<u8>, ReferenceRefreshRecord)>,
    has_more_due: bool,
    next_due_ms: Option<u64>,
}

impl ReferenceRefreshRecord {
    pub fn new(refresh: ReferenceRefresh, due_at_ms: u64) -> Self {
        Self {
            refresh,
            due_at_ms,
            attempts: 0,
            last_error: None,
        }
    }
}

/// One job per version: a newer request replaces the payload of the queued one.
pub fn job_key(refresh: &ReferenceRefresh) -> Result<Key, ConversionError> {
    let identity = ReferenceRefreshIdentity {
        bucket: &refresh.bucket,
        key: &refresh.key,
        version_id: refresh.version_id,
    };
    let mut key = b"v2".to_vec();
    key.extend(postcard::to_allocvec(&identity)?);
    Ok(ByteView::from(key))
}

/// Big-endian due milliseconds, then the job key, so the due index sorts by due time.
fn due_key(due_at_ms: u64, job_key: &[u8]) -> Key {
    let mut key = due_at_ms.to_be_bytes().to_vec();
    key.extend_from_slice(job_key);
    ByteView::from(key)
}

fn split_due(row: &[u8]) -> Option<(u64, &[u8])> {
    let (due, job_key) = row.split_first_chunk::<8>()?;
    Some((u64::from_be_bytes(*due), job_key))
}

/// The job row and its due index row.
fn job_entries(
    record: &ReferenceRefreshRecord,
) -> Result<Vec<(String, Key, ByteView)>, ConversionError> {
    let key = job_key(&record.refresh)?;
    Ok(vec![
        (
            REFRESH_DUE_KEYSPACE.to_string(),
            due_key(record.due_at_ms, &key),
            ByteView::from(Vec::new()),
        ),
        (
            REFRESH_JOB_KEYSPACE.to_string(),
            key,
            ByteView::from(postcard::to_allocvec(record)?),
        ),
    ])
}

pub fn schedule_drain() -> Effect {
    Effect::Task(TaskEffect::ResetTimer {
        key: TaskKey::DrainRefreshQueue,
        after: Duration::ZERO,
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum QueueRefreshState {
    Init,
    StartTransaction,
    ReadExisting,
    WriteJob,
    Commit,
    ScheduleDrain,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct QueueRefreshOperation {
    refresh: ReferenceRefresh,
    txn_id: Option<Ulid>,
    conflicts: u32,
    state: QueueRefreshState,
    output: Option<Result<QueueRefreshResult, ReferenceRefreshError>>,
}

impl QueueRefreshOperation {
    pub fn new(refresh: ReferenceRefresh) -> Self {
        Self {
            refresh,
            txn_id: None,
            conflicts: 0,
            state: QueueRefreshState::Init,
            output: None,
        }
    }

    fn fail(&mut self, error: ReferenceRefreshError) -> Effects {
        self.state = QueueRefreshState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn start_transaction(&mut self) -> Effects {
        self.state = QueueRefreshState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn read_existing(&mut self, txn_id: Ulid) -> Effects {
        let key = match job_key(&self.refresh) {
            Ok(key) => key,
            Err(error) => return self.fail(error.into()),
        };
        self.txn_id = Some(txn_id);
        self.state = QueueRefreshState::ReadExisting;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: REFRESH_JOB_KEYSPACE.to_string(),
            key,
            txn_id: Some(txn_id),
        })]
    }

    /// A queued job keeps its due time and attempts; only a newer request replaces its payload.
    fn merge_job(&mut self, existing: Option<&[u8]>) -> Effects {
        let record = match existing.map(postcard::from_bytes::<ReferenceRefreshRecord>) {
            Some(Ok(existing)) if existing.refresh.refreshed_at >= self.refresh.refreshed_at => {
                return self.commit();
            }
            Some(Ok(existing)) => ReferenceRefreshRecord {
                refresh: self.refresh.clone(),
                ..existing
            },
            Some(Err(_)) | None => {
                ReferenceRefreshRecord::new(self.refresh.clone(), unix_timestamp_millis())
            }
        };
        let writes = match job_entries(&record) {
            Ok(writes) => writes,
            Err(error) => return self.fail(error.into()),
        };
        self.state = QueueRefreshState::WriteJob;
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: self.txn_id,
        })]
    }

    fn commit(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(ReferenceRefreshError::UnexpectedEvent(
                "refresh queue transaction is missing".to_string(),
            ));
        };
        self.state = QueueRefreshState::Commit;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn schedule_drain(&mut self) -> Effects {
        self.txn_id = None;
        self.state = QueueRefreshState::ScheduleDrain;
        smallvec![schedule_drain()]
    }

    fn finish(&mut self, queued: bool, scheduled: bool) -> Effects {
        self.state = QueueRefreshState::Finish;
        self.output = Some(Ok(QueueRefreshResult { queued, scheduled }));
        smallvec![]
    }
}

impl Operation for QueueRefreshOperation {
    type Output = QueueRefreshResult;
    type Error = ReferenceRefreshError;

    fn start(&mut self) -> Effects {
        self.start_transaction()
    }

    fn step(&mut self, event: Event) -> Effects {
        let event = match (&self.state, event) {
            (QueueRefreshState::Finish | QueueRefreshState::Error, _) => return smallvec![],
            (QueueRefreshState::Init, _) => return self.start_transaction(),
            (QueueRefreshState::ScheduleDrain, event) => {
                return match event {
                    Event::Task(TaskEvent::TimerScheduled { .. }) => self.finish(true, true),
                    Event::Task(TaskEvent::Error { .. }) => self.finish(true, false),
                    other => {
                        warn!(event = ?other, "Reference metadata refresh job persisted but drain scheduling returned an unexpected event");
                        self.finish(true, false)
                    }
                };
            }
            (
                QueueRefreshState::Commit,
                Event::Storage(StorageEvent::Error {
                    error: StorageError::TransactionConflict,
                }),
            ) if self.conflicts < QUEUE_CONFLICT_RETRIES => {
                self.txn_id = None;
                self.conflicts += 1;
                return self.start_transaction();
            }
            (_, Event::Storage(StorageEvent::Error { error })) => return self.fail(error.into()),
            (_, Event::Storage(event)) => event,
            (_, other) => {
                return self.fail(ReferenceRefreshError::UnexpectedEvent(format!("{other:?}")));
            }
        };
        match (&self.state, event) {
            (QueueRefreshState::StartTransaction, StorageEvent::TransactionStarted { txn_id }) => {
                self.read_existing(txn_id)
            }
            (QueueRefreshState::ReadExisting, StorageEvent::ReadResult { value, .. }) => {
                self.merge_job(value.as_deref())
            }
            (QueueRefreshState::WriteJob, StorageEvent::BatchWriteResult { .. }) => self.commit(),
            (QueueRefreshState::Commit, StorageEvent::TransactionCommitted { .. }) => {
                self.schedule_drain()
            }
            (_, other) => self.fail(ReferenceRefreshError::UnexpectedEvent(format!("{other:?}"))),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            QueueRefreshState::Finish | QueueRefreshState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.output {
            Some(Ok(result)) => Ok(result),
            Some(Err(error)) => Err(error),
            None => Err(ReferenceRefreshError::UnexpectedEvent(
                "reference metadata refresh queue operation finished without output".to_string(),
            )),
        }
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

pub async fn refresh_reference_metadata(
    context: Arc<DriverContext>,
    refresh: ReferenceRefresh,
) -> Result<(), String> {
    refresh_with_context(context.as_ref(), refresh).await
}

pub async fn refresh_with_context(
    context: &DriverContext,
    refresh: ReferenceRefresh,
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

    let mut minted = false;
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
            let predecessor = version.clone();
            let BlobVersion {
                created_at,
                created_by,
                state,
                metadata,
                published_by,
                placement_policies,
                original_epoch,
                generation,
                identity,
            } = version;
            let BlobVersionState::Reference {
                source,
                cached_metadata,
                last_refresh,
                advance_count,
            } = state
            else {
                abort_reference_refresh(context, txn_id).await;
                return Ok(());
            };
            // Only a confirmed observation counts as a refresh; fields under the version id never
            // change, so a confirmed change becomes a successor and an unknown one is dropped.
            let Some(observation) = refresh.metadata.clone() else {
                abort_reference_refresh(context, txn_id).await;
                return Err("the refresh carries no observation".to_string());
            };
            let comparison = cached_metadata.compare(&observation);
            if refresh.refreshed_at > last_refresh && comparison == ObservationMatch::Changed {
                let mint = MintSuccessorOperation::new(
                    txn_id,
                    refresh.clone(),
                    observation.clone(),
                    predecessor,
                );
                match drive(mint, context).await {
                    Ok(written) => minted = written,
                    Err(error) => {
                        abort_reference_refresh(context, txn_id).await;
                        return Err(error.to_string());
                    }
                }
            }
            if refresh.refreshed_at <= last_refresh || comparison != ObservationMatch::Same {
                None
            } else {
                Some(
                    match (BlobVersion {
                        published_by,
                        placement_policies,
                        original_epoch,
                        generation,
                        identity,
                        ..BlobVersion::reference(
                            source,
                            cached_metadata,
                            created_at,
                            created_by,
                            refresh.refreshed_at,
                        )
                        .with_metadata(metadata)
                        .with_advance_count(advance_count)
                    })
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
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
            if minted {
                // The dirty markers and the obligation committed above are the durable work.
                for effect in [schedule_snapshot_publish(), schedule_blob_drain()] {
                    if let Effect::Task(effect) = effect {
                        schedule_task(context, effect).await;
                    }
                }
            }
            Ok(())
        }
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

/// Observes the source first when the refresh carries no observation; a failed observation
/// fails the job, which the queue retries later.
async fn observe_and_refresh(
    context: &DriverContext,
    mut refresh: ReferenceRefresh,
) -> Result<(), String> {
    if refresh.metadata.is_none() {
        let version_key = VersionKey::new(&refresh.bucket, &refresh.key, refresh.version_id);
        let observed = match drive(ObserveSourceOperation::new(version_key), context).await {
            Ok(observed) => observed,
            // Work for a version that is gone or no longer resolvable is finished, not retried.
            Err(error) if error.is_obsolete() => return Ok(()),
            Err(error) => return Err(error.to_string()),
        };
        refresh.metadata = Some(observed);
        refresh.refreshed_at = SystemTime::now().max(refresh.refreshed_at);
    }
    refresh_with_context(context, refresh).await
}

async fn schedule_task(context: &DriverContext, effect: TaskEffect) {
    if persist_task_effect(&context.storage_handle, &effect)
        .await
        .is_err()
    {
        return;
    }
    if let Some(task_handle) = &context.task_handle {
        let _ = task_handle.send_effect(Effect::Task(effect)).await;
    }
}

async fn abort_reference_refresh(context: &DriverContext, txn_id: Ulid) {
    let _ = context
        .storage_handle
        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
        .await;
}

pub async fn restore_timer(storage: &StorageHandle, task_handle: &TaskHandle) {
    match next_timer(storage).await {
        Ok(None) => {}
        Ok(Some(after)) => {
            let event = task_handle
                .send_effect(Effect::Task(TaskEffect::ResetTimer {
                    key: TaskKey::DrainRefreshQueue,
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

pub async fn jobs_exist(storage: &StorageHandle) -> Result<bool, ReferenceRefreshError> {
    Ok(!first_due(storage, 1).await?.0.is_empty())
}

pub async fn next_timer(
    storage: &StorageHandle,
) -> Result<Option<Duration>, ReferenceRefreshError> {
    let (rows, _) = first_due(storage, 1).await?;
    Ok(rows
        .first()
        .and_then(|(row, _)| split_due(row))
        .map(|(due_at_ms, _)| due_after(unix_timestamp_millis(), due_at_ms)))
}

pub async fn process_batch(
    context: &DriverContext,
) -> Result<ReferenceDrainResult, ReferenceRefreshError> {
    let batch_started = Instant::now();
    let now_ms = unix_timestamp_millis();
    let scan = scan_due_jobs(&context.storage_handle, now_ms, REFRESH_BATCH_SIZE).await?;
    let mut next_due_ms = scan.next_due_ms;
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
        match observe_and_refresh(context, job.refresh.clone()).await {
            Ok(()) => {
                settle_job(&context.storage_handle, &job_key, &job, None).await?;
                succeeded = succeeded.saturating_add(1);
            }
            Err(error) => {
                let retry_due_at =
                    reschedule_job(&context.storage_handle, &job_key, &job, error).await?;
                next_due_ms = min_due_at(next_due_ms, retry_due_at);
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

    Ok(ReferenceDrainResult {
        processed: job_count,
        succeeded,
        failed,
        has_more_due,
        next_due_after: if has_more_due {
            None
        } else {
            next_due_ms.map(|due_at_ms| due_after(unix_timestamp_millis(), due_at_ms))
        },
    })
}

/// The first `limit` due index rows and whether more follow.
async fn first_due(
    storage: &StorageHandle,
    limit: usize,
) -> Result<(Vec<(Key, ByteView)>, bool), ReferenceRefreshError> {
    let event = send(
        storage,
        StorageEffect::Iter {
            key_space: REFRESH_DUE_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit,
            txn_id: None,
        },
    )
    .await?;
    match event {
        StorageEvent::IterResult {
            values,
            next_start_after,
        } => Ok((values, next_start_after.is_some())),
        other => Err(ReferenceRefreshError::UnexpectedEvent(format!("{other:?}"))),
    }
}

/// Walks one page of the due index in due order, with a point read per due row.
async fn scan_due_jobs(
    storage: &StorageHandle,
    now_ms: u64,
    limit: usize,
) -> Result<ReferenceRefreshScan, ReferenceRefreshError> {
    let (rows, more_rows) = first_due(storage, REFRESH_SCAN_PAGE).await?;
    let mut jobs = Vec::new();
    for (row, _) in rows {
        let Some((due_at_ms, key)) = split_due(&row) else {
            delete_rows(storage, vec![(REFRESH_DUE_KEYSPACE.to_string(), row)], None).await?;
            continue;
        };
        if due_at_ms > now_ms {
            return Ok(ReferenceRefreshScan {
                jobs,
                has_more_due: false,
                next_due_ms: Some(due_at_ms),
            });
        }
        if jobs.len() >= limit {
            return Ok(ReferenceRefreshScan {
                jobs,
                has_more_due: true,
                next_due_ms: None,
            });
        }
        let mut stale = vec![(REFRESH_DUE_KEYSPACE.to_string(), row.clone())];
        match read_job(storage, key, None).await? {
            Some(Ok(job)) if job.due_at_ms == due_at_ms => {
                jobs.push((key.to_vec(), job));
                continue;
            }
            Some(Ok(_)) | None => {}
            Some(Err(error)) => {
                warn!(error = %error, key = ?key, "Deleting malformed reference metadata refresh job");
                let key = ByteView::from(key.to_vec());
                stale.push((REFRESH_JOB_KEYSPACE.to_string(), key));
            }
        }
        delete_rows(storage, stale, None).await?;
    }
    Ok(ReferenceRefreshScan {
        jobs,
        has_more_due: more_rows,
        next_due_ms: None,
    })
}

async fn send(
    storage: &StorageHandle,
    effect: StorageEffect,
) -> Result<StorageEvent, ReferenceRefreshError> {
    match storage.send_storage_effect(effect).await {
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        Event::Storage(event) => Ok(event),
        other => Err(ReferenceRefreshError::UnexpectedEvent(format!("{other:?}"))),
    }
}

async fn read_job(
    storage: &StorageHandle,
    key: &[u8],
    txn_id: Option<Ulid>,
) -> Result<Option<Result<ReferenceRefreshRecord, postcard::Error>>, ReferenceRefreshError> {
    let effect = StorageEffect::Read {
        key_space: REFRESH_JOB_KEYSPACE.to_string(),
        key: ByteView::from(key.to_vec()),
        txn_id,
    };
    match send(storage, effect).await? {
        StorageEvent::ReadResult { value, .. } => {
            Ok(value.map(|value| postcard::from_bytes(&value)))
        }
        other => Err(ReferenceRefreshError::UnexpectedEvent(format!("{other:?}"))),
    }
}

async fn delete_rows(
    storage: &StorageHandle,
    deletes: Vec<(String, Key)>,
    txn_id: Option<Ulid>,
) -> Result<(), ReferenceRefreshError> {
    match send(storage, StorageEffect::BatchDelete { deletes, txn_id }).await? {
        StorageEvent::BatchDeleteResult { .. } => Ok(()),
        other => Err(ReferenceRefreshError::UnexpectedEvent(format!("{other:?}"))),
    }
}

/// Removes a finished job, or replaces it with `next`, unless a newer request replaced it.
async fn settle_job(
    storage: &StorageHandle,
    key: &[u8],
    job: &ReferenceRefreshRecord,
    next: Option<&ReferenceRefreshRecord>,
) -> Result<(), ReferenceRefreshError> {
    let txn_id = match send(storage, StorageEffect::StartTransaction { read: false }).await? {
        StorageEvent::TransactionStarted { txn_id } => txn_id,
        other => return Err(ReferenceRefreshError::UnexpectedEvent(format!("{other:?}"))),
    };
    let result = async {
        if !matches!(read_job(storage, key, Some(txn_id)).await?, Some(Ok(stored)) if stored == *job)
        {
            return Ok(());
        }
        let mut deletes = vec![(
            REFRESH_DUE_KEYSPACE.to_string(),
            due_key(job.due_at_ms, key),
        )];
        let Some(next) = next else {
            let key = ByteView::from(key.to_vec());
            deletes.push((REFRESH_JOB_KEYSPACE.to_string(), key));
            return delete_rows(storage, deletes, Some(txn_id)).await;
        };
        delete_rows(storage, deletes, Some(txn_id)).await?;
        let writes = job_entries(next)?;
        match send(storage, StorageEffect::BatchWrite { writes, txn_id: Some(txn_id) }).await? {
            StorageEvent::BatchWriteResult { .. } => Ok(()),
            other => Err(ReferenceRefreshError::UnexpectedEvent(format!("{other:?}"))),
        }
    }
    .await;
    let effect = match result {
        Ok(()) => StorageEffect::CommitTransaction { txn_id },
        Err(error) => {
            let _ = send(storage, StorageEffect::AbortTransaction { txn_id }).await;
            return Err(error);
        }
    };
    match send(storage, effect).await? {
        StorageEvent::TransactionCommitted { .. } => Ok(()),
        other => Err(ReferenceRefreshError::UnexpectedEvent(format!("{other:?}"))),
    }
}

async fn reschedule_job(
    storage: &StorageHandle,
    key: &[u8],
    job: &ReferenceRefreshRecord,
    error: String,
) -> Result<u64, ReferenceRefreshError> {
    let attempts = job.attempts.saturating_add(1);
    let due_at_ms = unix_timestamp_millis().saturating_add(retry_delay_ms(attempts));
    let next_job = ReferenceRefreshRecord {
        refresh: job.refresh.clone(),
        due_at_ms,
        attempts,
        last_error: Some(error),
    };
    settle_job(storage, key, job, Some(&next_job)).await?;
    Ok(due_at_ms)
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::keyspaces::BLOB_VERSIONS_KEYSPACE;
    use aruna_core::structs::execution::source_connector::SourceConnectorKind;
    use aruna_core::structs::execution::staging::{
        PortableSourceDescriptor, StagingStrategy, VersionSourceBinding,
    };
    use aruna_core::structs::identity::realm::RealmId;
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
                compute_handle: None,
            },
            bucket: "bucket".to_string(),
            key: "key".to_string(),
            version_id: Ulid::generate(),
            created_by: UserId::local(Ulid::generate(), realm_id),
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
            digests: Default::default(),
        }
    }

    fn refresh(
        test: &TestState,
        metadata: SourceMetadata,
        refreshed_at: SystemTime,
    ) -> ReferenceRefresh {
        ReferenceRefresh {
            bucket: test.bucket.clone(),
            key: test.key.clone(),
            version_id: test.version_id,
            metadata: Some(metadata),
            refreshed_at,
            node_id: iroh::SecretKey::from_bytes(&[4; 32]).public(),
            auth_context: aruna_core::structs::identity::auth::AuthContext {
                user_id: Default::default(),
                realm_id: aruna_core::structs::identity::realm::RealmId([9u8; 32]),
                path_restrictions: None,
                session: None,
            },
        }
    }

    async fn write_reference_version(
        test: &TestState,
        cached_metadata: SourceMetadata,
        last_refresh: SystemTime,
        advance_count: u16,
    ) {
        let version = BlobVersion::reference(
            source_binding(),
            cached_metadata,
            SystemTime::UNIX_EPOCH,
            test.created_by,
            last_refresh,
        )
        .with_metadata(std::collections::HashMap::from([(
            "mtime".to_string(),
            "1753272000.123456789".to_string(),
        )]))
        .with_advance_count(advance_count);
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

    async fn read_reference(test: &TestState) -> BlobVersion {
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
        BlobVersion::from_bytes(value.as_ref()).unwrap()
    }

    async fn assert_reference_state(
        test: &TestState,
        expected_metadata: &SourceMetadata,
        expected_last_refresh: SystemTime,
    ) {
        let version = read_reference(test).await;
        assert_eq!(
            version.metadata.get("mtime").map(String::as_str),
            Some("1753272000.123456789")
        );
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

    async fn read_jobs(storage: &StorageHandle) -> Vec<ReferenceRefreshRecord> {
        match storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: REFRESH_JOB_KEYSPACE.to_string(),
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

    async fn write_rows(storage: &StorageHandle, writes: Vec<(String, Key, ByteView)>) {
        match storage
            .send_storage_effect(StorageEffect::BatchWrite {
                writes,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::BatchWriteResult { .. }) => {}
            other => panic!("unexpected refresh job write event: {other:?}"),
        }
    }

    async fn write_corrupt_job(storage: &StorageHandle, key: &str) {
        let writes = vec![
            (
                REFRESH_DUE_KEYSPACE.to_string(),
                due_key(0, key.as_bytes()),
                ByteView::from(Vec::new()),
            ),
            (
                REFRESH_JOB_KEYSPACE.to_string(),
                ByteView::from(key.as_bytes().to_vec()),
                ByteView::from(Vec::new()),
            ),
        ];
        write_rows(storage, writes).await;
    }

    #[tokio::test]
    async fn drain_preserves_guard() {
        let test = setup_state();
        let last_refresh = SystemTime::UNIX_EPOCH + Duration::from_secs(20);
        let original_metadata = source_metadata(10, "original");
        write_reference_version(&test, original_metadata.clone(), last_refresh, 0).await;

        crate::driver::drive(
            QueueRefreshOperation::new(refresh(
                &test,
                source_metadata(20, "older"),
                SystemTime::UNIX_EPOCH + Duration::from_secs(10),
            )),
            &test.context,
        )
        .await
        .expect("queue succeeds");
        let result = process_batch(&test.context).await.expect("drain succeeds");

        assert_eq!(result.succeeded, 1);
        assert!(read_jobs(&test.context.storage_handle).await.is_empty());
        assert_reference_state(&test, &original_metadata, last_refresh).await;
    }

    // Fields under a version id never change: a changed observation is no refresh.
    #[tokio::test]
    async fn changed_refresh_ignored() {
        let test = setup_state();
        let last_refresh = SystemTime::UNIX_EPOCH + Duration::from_secs(20);
        let refreshed_at = SystemTime::UNIX_EPOCH + Duration::from_secs(30);
        let original = source_metadata(10, "original");
        write_reference_version(&test, original.clone(), last_refresh, 0).await;

        crate::driver::drive(
            QueueRefreshOperation::new(refresh(&test, source_metadata(20, "newer"), refreshed_at)),
            &test.context,
        )
        .await
        .expect("queue succeeds");
        process_batch(&test.context).await.expect("drain succeeds");

        assert_reference_state(&test, &original, last_refresh).await;
    }

    // A confirmed change of the current version is recorded as a successor that becomes the
    // head; the refreshed version keeps its fields.
    #[tokio::test]
    async fn change_mints_successor() {
        use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, S3_BUCKET_KEYSPACE};
        use aruna_core::structs::storage::blob::{BlobHeadKey, BucketInfo, CurrentVersionPointer};
        let test = setup_state();
        let original = source_metadata(10, "original");
        write_reference_version(&test, original.clone(), SystemTime::UNIX_EPOCH, 0).await;
        let bucket = BucketInfo {
            group_id: Ulid::from_bytes([1; 16]),
            created_at: SystemTime::UNIX_EPOCH,
            created_by: test.created_by,
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: aruna_core::structs::storage::format::Compression::Off,
            distribution: None,
        };
        let head_key = BlobHeadKey::new(&test.bucket, &test.key)
            .to_bytes()
            .unwrap();
        let writes = vec![
            (
                BLOB_HEAD_KEYSPACE.to_string(),
                head_key.clone().into(),
                CurrentVersionPointer::new(test.version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
            ),
            (
                S3_BUCKET_KEYSPACE.to_string(),
                test.bucket.as_bytes().to_vec().into(),
                bucket.to_bytes().unwrap().into(),
            ),
        ];
        test.context
            .storage_handle
            .send_storage_effect(StorageEffect::BatchWrite {
                writes,
                txn_id: None,
            })
            .await;
        let changed = source_metadata(20, "changed");
        let refreshed_at = SystemTime::UNIX_EPOCH + Duration::from_secs(30);

        refresh_with_context(&test.context, refresh(&test, changed.clone(), refreshed_at))
            .await
            .unwrap();

        assert_reference_state(&test, &original, SystemTime::UNIX_EPOCH).await;
        let Event::Storage(StorageEvent::ReadResult {
            value: Some(head), ..
        }) = test
            .context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: BLOB_HEAD_KEYSPACE.to_string(),
                key: head_key.into(),
                txn_id: None,
            })
            .await
        else {
            panic!("the head must exist");
        };
        let head = CurrentVersionPointer::from_bytes(&head).unwrap();
        assert_ne!(head.version_id, test.version_id);
        assert_eq!(head.generation, 2);
    }

    // A request for a version that no longer exists is finished instead of retried forever.
    #[tokio::test]
    async fn obsolete_request_finished() {
        let test = setup_state();
        let refreshed_at = SystemTime::UNIX_EPOCH + Duration::from_secs(30);
        let mut request = refresh(&test, source_metadata(1, "unused"), refreshed_at);
        request.metadata = None;
        crate::driver::drive(QueueRefreshOperation::new(request), &test.context)
            .await
            .expect("queue succeeds");

        let result = process_batch(&test.context).await.expect("drain runs");

        assert_eq!((result.succeeded, result.failed), (1, 0));
        assert!(read_jobs(&test.context.storage_handle).await.is_empty());
    }

    #[tokio::test]
    async fn same_refresh_advances() {
        let test = setup_state();
        let refreshed_at = SystemTime::UNIX_EPOCH + Duration::from_secs(30);
        let original = source_metadata(10, "original");
        write_reference_version(&test, original.clone(), SystemTime::UNIX_EPOCH, 0).await;
        let mut unknown = original.clone();
        unknown.etag = None;
        let mut listed = original.clone();
        listed.content_type = None;

        for (observation, expected_refresh) in
            [(unknown, SystemTime::UNIX_EPOCH), (listed, refreshed_at)]
        {
            crate::driver::drive(
                QueueRefreshOperation::new(refresh(&test, observation, refreshed_at)),
                &test.context,
            )
            .await
            .expect("queue succeeds");
            process_batch(&test.context).await.expect("drain succeeds");

            assert_reference_state(&test, &original, expected_refresh).await;
        }
    }

    // A metadata refresh is not a rebind, so it must not hand the binding a fresh
    // automatic advance budget.
    #[tokio::test]
    async fn refresh_keeps_count() {
        let test = setup_state();
        let refreshed_at = SystemTime::UNIX_EPOCH + Duration::from_secs(30);
        let original = source_metadata(10, "original");
        write_reference_version(&test, original.clone(), SystemTime::UNIX_EPOCH, 7).await;

        crate::driver::drive(
            QueueRefreshOperation::new(refresh(&test, original.clone(), refreshed_at)),
            &test.context,
        )
        .await
        .expect("queue succeeds");
        process_batch(&test.context).await.expect("drain succeeds");

        assert_reference_state(&test, &original, refreshed_at).await;
        assert_eq!(read_reference(&test).await.advance_count(), Some(7));
    }

    #[tokio::test]
    async fn newest_job_kept() {
        let test = setup_state();
        let last_refresh = SystemTime::UNIX_EPOCH;
        let newer = SystemTime::UNIX_EPOCH + Duration::from_secs(30);
        let older = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        let original = source_metadata(1, "original");
        write_reference_version(&test, original.clone(), last_refresh, 0).await;

        crate::driver::drive(
            QueueRefreshOperation::new(refresh(&test, original.clone(), newer)),
            &test.context,
        )
        .await
        .expect("newer queue succeeds");
        crate::driver::drive(
            QueueRefreshOperation::new(refresh(&test, original.clone(), older)),
            &test.context,
        )
        .await
        .expect("older queue succeeds");

        let jobs = read_jobs(&test.context.storage_handle).await;
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].refresh.refreshed_at, newer);

        let result = process_batch(&test.context).await.expect("drain succeeds");
        assert_eq!(result.succeeded, 1);
        assert_reference_state(&test, &original, newer).await;
    }

    // A queue write that loses to a racing commit rereads the job instead of dropping the request.
    #[tokio::test]
    async fn queue_conflict_retried() {
        let test = setup_state();
        let mut operation = QueueRefreshOperation::new(refresh(
            &test,
            source_metadata(1, "original"),
            SystemTime::UNIX_EPOCH,
        ));
        let conflict = || {
            Event::Storage(StorageEvent::Error {
                error: StorageError::TransactionConflict,
            })
        };
        operation.start();
        for _ in 0..=QUEUE_CONFLICT_RETRIES {
            let txn_id = Ulid::generate();
            operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
            operation.step(Event::Storage(StorageEvent::ReadResult {
                key: ByteView::from(Vec::new()),
                value: None,
            }));
            operation.step(Event::Storage(StorageEvent::BatchWriteResult {
                entries: Vec::new(),
            }));
            assert_eq!(operation.state, QueueRefreshState::Commit);
            let effects = operation.step(conflict());
            let retried = matches!(
                effects.as_slice(),
                [Effect::Storage(StorageEffect::StartTransaction {
                    read: false
                })]
            );
            assert_eq!(retried, !operation.is_complete());
        }

        assert_eq!(
            operation.finalize(),
            Err(ReferenceRefreshError::Storage(
                StorageError::TransactionConflict
            ))
        );
    }

    // A drain that finishes a request a newer one replaced keeps the newer job queued.
    #[tokio::test]
    async fn replaced_job_kept() {
        let test = setup_state();
        let original = source_metadata(1, "original");
        let older = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        let newer = SystemTime::UNIX_EPOCH + Duration::from_secs(30);
        for refreshed_at in [older, newer] {
            crate::driver::drive(
                QueueRefreshOperation::new(refresh(&test, original.clone(), refreshed_at)),
                &test.context,
            )
            .await
            .expect("queue succeeds");
        }
        let storage = &test.context.storage_handle;
        let queued = read_jobs(storage).await;
        let older_job = ReferenceRefreshRecord {
            refresh: refresh(&test, original, older),
            ..queued[0].clone()
        };
        let key = job_key(&older_job.refresh).unwrap();

        settle_job(storage, &key, &older_job, None).await.unwrap();

        assert_eq!(read_jobs(storage).await, queued);
        assert_eq!(queued[0].refresh.refreshed_at, newer);
        assert!(jobs_exist(storage).await.unwrap());
    }

    #[tokio::test]
    async fn future_retry_preserved() {
        let test = setup_state();
        let refreshed_at = SystemTime::UNIX_EPOCH + Duration::from_secs(30);
        let refresh = refresh(&test, source_metadata(30, "future"), refreshed_at);
        let future_job = ReferenceRefreshRecord {
            refresh: refresh.clone(),
            due_at_ms: unix_timestamp_millis().saturating_add(60_000),
            attempts: 1,
            last_error: Some("transient".to_string()),
        };
        let writes = job_entries(&future_job).expect("future job serializes");
        write_rows(&test.context.storage_handle, writes).await;

        crate::driver::drive(QueueRefreshOperation::new(refresh), &test.context)
            .await
            .expect("duplicate queue succeeds");

        assert_eq!(
            read_jobs(&test.context.storage_handle).await,
            vec![future_job]
        );
    }

    #[tokio::test]
    async fn future_not_due() {
        let test = setup_state();
        let due_at_ms = unix_timestamp_millis().saturating_add(60_000);
        let record = ReferenceRefreshRecord::new(
            refresh(
                &test,
                source_metadata(20, "future"),
                SystemTime::UNIX_EPOCH + Duration::from_secs(20),
            ),
            due_at_ms,
        );
        write_rows(&test.context.storage_handle, job_entries(&record).unwrap()).await;

        let result = process_batch(&test.context)
            .await
            .expect("future-only drain succeeds");

        assert_eq!(result.processed, 0);
        assert!(!result.has_more_due);
        assert!(result.next_due_after.is_some());
    }

    #[tokio::test]
    async fn corrupt_job_deleted() {
        let test = setup_state();
        write_corrupt_job(&test.context.storage_handle, "000-corrupt-refresh-job").await;

        let result = process_batch(&test.context)
            .await
            .expect("corrupt-only refresh drain succeeds");

        assert_eq!(result.processed, 0);
        assert!(!result.has_more_due);
        assert!(result.next_due_after.is_none());
        assert!(!jobs_exist(&test.context.storage_handle).await.unwrap());
    }

    #[tokio::test]
    async fn corrupt_precedes_valid() {
        let test = setup_state();
        write_corrupt_job(&test.context.storage_handle, "000-corrupt-refresh-job").await;
        crate::driver::drive(
            QueueRefreshOperation::new(refresh(
                &test,
                source_metadata(20, "valid"),
                SystemTime::UNIX_EPOCH + Duration::from_secs(20),
            )),
            &test.context,
        )
        .await
        .expect("queue succeeds");

        let result = process_batch(&test.context)
            .await
            .expect("mixed corrupt/valid refresh drain succeeds");

        assert_eq!(result.processed, 1);
        assert_eq!(result.succeeded, 1);
        assert!(read_jobs(&test.context.storage_handle).await.is_empty());
    }
}
