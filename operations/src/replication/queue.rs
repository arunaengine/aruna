use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant, SystemTime};

use aruna_core::NodeId;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{
    BLOB_LIVE_REPLICATION_OBLIGATION_KEYSPACE, BLOB_REPLICATION_JOB_KEYSPACE, NODE_STATE_KEYSPACE,
    SYNC_RELATIONSHIP_IN_KEYSPACE, SYNC_RELATIONSHIP_OUT_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::{
    ArunaArn, AuthContext, ReferenceHandling, SyncMode, SyncRelationship, SyncState, WatchEvent,
    WatchEventDetail, WatchEventKind, data_watch_resource_path, sync_relationship_key,
    sync_relationship_prefix,
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
use tracing::{error, info, warn};
use ulid::Ulid;

use super::protocol::{ReferenceAdvance, ReplicationMode, SyncOrigin};
use super::version_replication::{
    ReplicateScopeError, ReplicateScopeInput, ReplicateScopeOperation, ReplicateScopeTarget,
    SourceAuthorization, SourceAuthorizationError,
};
use crate::driver::{DriverContext, drive, gate_context, now_ms, quota_marked_routing};
use crate::notifications::watch::emit::emit_resource_watch_event;
use crate::queue_backoff::queue_retry_after_ms;
use crate::s3::get_bucket_info::GetBucketInfoOperation;
use crate::sync_mirror_repair::{kick_mirror_repair, store_sync_status};

const REPLICATION_SCAN_PAGE_SIZE: usize = 512;
const REPLICATION_BATCH_SIZE: usize = 64;
const RELATIONSHIP_STATS_PAGE_SIZE: usize = 256;
const LIVE_REPLICATION_OBLIGATION_BATCH_SIZE: usize = 64;
const LIVE_REPLICATION_JOB_LIMIT: usize = 64;
const LIVE_REPLICATION_RELATIONSHIP_LIMIT: usize = 1024;
const REPLICATION_CURSOR_KEY: &[u8] = b"blob_replication_cursor";

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
    pub reference_advance: Option<ReferenceAdvance>,
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

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveReplicationContinuation {
    pub relationship_after: Option<Vec<u8>>,
    pub relationships_complete: bool,
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
    pub continuation: Option<LiveReplicationContinuation>,
    pub reference_advance: Option<ReferenceAdvance>,
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
    next_cursor: ReplicationScanCursor,
}

struct RelationshipPage {
    values: Vec<(Vec<u8>, SyncRelationship)>,
    next: Option<Vec<u8>>,
}

struct LiveRepairWrite {
    queued: usize,
    continuation: Option<LiveReplicationContinuation>,
    progressed: bool,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
struct ReplicationScanCursor {
    generation: u64,
    after: Option<Vec<u8>>,
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
        let writer_auth_context = Some(input.auth_context.clone());
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
            writer_auth_context,
            reference_advance: None,
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

    pub fn with_reference_advance(mut self, advance: ReferenceAdvance) -> Self {
        self.reference_advance = Some(advance);
        self
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        let record: Self = match postcard::take_from_bytes(bytes) {
            Ok((record, [])) => record,
            Ok(_) => return Err(postcard::Error::DeserializeBadEncoding.into()),
            Err(error) => return Err(error.into()),
        };
        if record.writer_auth_context.is_none() && record.reference_advance.is_none() {
            return Err(ConversionError::FromStrError(
                "replication job is missing its source writer".to_string(),
            ));
        }
        Ok(record)
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
            continuation: None,
            reference_advance: None,
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

    pub fn with_reference_advance(mut self, advance: ReferenceAdvance) -> Self {
        self.reference_advance = Some(advance);
        self
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        match postcard::take_from_bytes(bytes) {
            Ok((record, [])) => Ok(record),
            Ok(_) => Err(postcard::Error::DeserializeBadEncoding.into()),
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

pub(crate) fn live_obligation_entry(
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
    let (key_space, key, value) = live_obligation_entry(&record)?;
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
pub struct QueueLiveVersionReplicationOperation {
    input: QueueLiveVersionReplicationInput,
    reference_advance: Option<ReferenceAdvance>,
    state: QueueLiveVersionReplicationState,
    seed_key: Option<Key>,
    seed_value: Option<ByteView>,
    txn_id: Option<Ulid>,
    abort_error: Option<BlobReplicationQueueError>,
    output: Option<Result<QueueBlobReplicationResult, BlobReplicationQueueError>>,
}

impl QueueLiveVersionReplicationOperation {
    pub fn new(input: QueueLiveVersionReplicationInput) -> Self {
        Self {
            input,
            reference_advance: None,
            state: QueueLiveVersionReplicationState::Init,
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

    fn fail(&mut self, error: BlobReplicationQueueError) -> Effects {
        self.state = QueueLiveVersionReplicationState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn seed_record(&self) -> LiveReplicationObligationRecord {
        let mut record = LiveReplicationObligationRecord::new(
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
        self.state = QueueLiveVersionReplicationState::StartObligation;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn abort_obligation(&mut self, error: Option<BlobReplicationQueueError>) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(BlobReplicationQueueError::UnexpectedEvent(
                "obligation transaction is not active".to_string(),
            ));
        };
        self.abort_error = error;
        self.state = QueueLiveVersionReplicationState::AbortObligation;
        smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
    }

    fn write_obligation(&mut self) -> Effects {
        let (Some(key), Some(value), Some(txn_id)) =
            (self.seed_key.clone(), self.seed_value.clone(), self.txn_id)
        else {
            return self.fail(BlobReplicationQueueError::UnexpectedEvent(
                "obligation write is not prepared".to_string(),
            ));
        };
        self.state = QueueLiveVersionReplicationState::WriteObligation;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: BLOB_LIVE_REPLICATION_OBLIGATION_KEYSPACE.to_string(),
            key,
            value,
            txn_id: Some(txn_id),
        })]
    }

    fn read_obligation(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(BlobReplicationQueueError::UnexpectedEvent(
                "obligation transaction is not active".to_string(),
            ));
        };
        let Some(key) = self.seed_key.clone() else {
            return self.fail(BlobReplicationQueueError::UnexpectedEvent(
                "obligation key is not prepared".to_string(),
            ));
        };
        self.state = QueueLiveVersionReplicationState::ReadObligation;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BLOB_LIVE_REPLICATION_OBLIGATION_KEYSPACE.to_string(),
            key,
            txn_id: Some(txn_id),
        })]
    }

    fn commit_obligation(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(BlobReplicationQueueError::UnexpectedEvent(
                "obligation transaction is not active".to_string(),
            ));
        };
        self.state = QueueLiveVersionReplicationState::CommitObligation;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
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
        self.start_obligation()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            QueueLiveVersionReplicationState::Init => self.start_obligation(),
            QueueLiveVersionReplicationState::StartObligation => match event {
                Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                    self.txn_id = Some(txn_id);
                    self.read_obligation()
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.fail(BlobReplicationQueueError::UnexpectedEvent(format!(
                    "{other:?}"
                ))),
            },
            QueueLiveVersionReplicationState::ReadObligation => match event {
                Event::Storage(StorageEvent::ReadResult { value: None, .. }) => {
                    self.write_obligation()
                }
                Event::Storage(StorageEvent::ReadResult {
                    value: Some(value), ..
                }) => match LiveReplicationObligationRecord::from_bytes(&value) {
                    Ok(_) => self.abort_obligation(None),
                    Err(_) => self.write_obligation(),
                },
                Event::Storage(StorageEvent::Error { error }) => {
                    self.abort_obligation(Some(error.into()))
                }
                other => self.abort_obligation(Some(BlobReplicationQueueError::UnexpectedEvent(
                    format!("{other:?}"),
                ))),
            },
            QueueLiveVersionReplicationState::WriteObligation => match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => self.commit_obligation(),
                Event::Storage(StorageEvent::Error { error }) => {
                    self.abort_obligation(Some(error.into()))
                }
                other => self.abort_obligation(Some(BlobReplicationQueueError::UnexpectedEvent(
                    format!("{other:?}"),
                ))),
            },
            QueueLiveVersionReplicationState::CommitObligation => match event {
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => self.schedule_drain(),
                Event::Storage(StorageEvent::Error {
                    error: StorageError::TransactionConflict,
                }) => self.schedule_drain(),
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.fail(BlobReplicationQueueError::UnexpectedEvent(format!(
                    "{other:?}"
                ))),
            },
            QueueLiveVersionReplicationState::AbortObligation => match event {
                Event::Storage(StorageEvent::TransactionAborted { .. }) => {
                    match self.abort_error.take() {
                        Some(error) => self.fail(error),
                        None => self.schedule_drain(),
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.fail(BlobReplicationQueueError::UnexpectedEvent(format!(
                    "{other:?}"
                ))),
            },
            QueueLiveVersionReplicationState::ScheduleDrain => match event {
                Event::Task(TaskEvent::TimerScheduled { .. }) => self.finish(0, true),
                Event::Task(TaskEvent::Error { .. }) => self.finish(0, false),
                other => {
                    warn!(event = ?other, "Durable replication obligation persisted but drain scheduling returned an unexpected event");
                    self.finish(0, false)
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
    let scan = scan_due_jobs(storage, now_ms, 1).await?;
    if !scan.jobs.is_empty() || scan.has_more_due {
        if scan.jobs.is_empty() {
            advance_replication_cursor(storage, scan.next_cursor).await?;
        }
        return Ok(Some(Duration::ZERO));
    }

    advance_replication_cursor(storage, scan.next_cursor.clone()).await?;

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
    let repair = process_live_obligations(context).await?;
    let now_ms = unix_timestamp_millis();
    let scan = scan_due_jobs(&context.storage_handle, now_ms, REPLICATION_BATCH_SIZE).await?;
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
    let mut relationships = read_job_relationships(&context.storage_handle, &scan.jobs).await?;
    for (job_key, job) in scan.jobs {
        let relationship = job.relationship_id.and_then(|relationship_id| {
            relationships
                .get(&(job.input.bucket.clone(), relationship_id))
                .cloned()
        });
        match process_blob_replication_job(context, &job, relationship, &mut relationships).await {
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

    advance_replication_cursor(&context.storage_handle, scan.next_cursor).await?;

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

async fn load_source_authorization(
    context: &DriverContext,
    auth_context: AuthContext,
    source_node_id: NodeId,
    bucket: &str,
) -> Result<SourceAuthorization, (SourceAuthorizationError, Option<GroupId>)> {
    let bucket_info = match drive(GetBucketInfoOperation::new(bucket.to_string()), context).await {
        Ok(Some(Ok(bucket_info))) => bucket_info,
        Ok(Some(Err(error))) => {
            return Err((
                SourceAuthorizationError::Unavailable(error.to_string()),
                None,
            ));
        }
        Ok(None) => {
            return Err((
                SourceAuthorizationError::Unavailable(
                    "source bucket lookup produced no result".to_string(),
                ),
                None,
            ));
        }
        Err(error) => {
            return Err((
                SourceAuthorizationError::Unavailable(error.to_string()),
                None,
            ));
        }
    };
    let group_id = bucket_info.group_id;
    SourceAuthorization::load(context, auth_context, group_id, source_node_id)
        .await
        .map_err(|error| (error, Some(group_id)))
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
    error.contains("Replication requires WRITE permission")
        || error.contains("access_denied")
        || error.contains("source access denied")
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

fn job_source_auth<'a>(
    job: &'a BlobReplicationJobRecord,
    creator: &'a AuthContext,
) -> &'a AuthContext {
    if job.reference_advance.is_some()
        && job
            .origin
            .as_ref()
            .is_some_and(|origin| origin.hop_count == 0)
    {
        &job.input.auth_context
    } else {
        creator
    }
}

async fn process_blob_replication_job(
    context: &DriverContext,
    job: &BlobReplicationJobRecord,
    stored_relationship: Option<SyncRelationship>,
    relationships: &mut HashMap<(String, Ulid), SyncRelationship>,
) -> Result<BlobReplicationJobOutcome, String> {
    if job.reference_advance.is_some() && job.relationship_id.is_none() {
        return Ok(BlobReplicationJobOutcome::TerminalFailure);
    }
    let routing = match quota_marked_routing(context).await {
        Ok(routing) => routing,
        // Retrying a record that will never decode only pins the queue at its
        // backoff ceiling; the drain repair re-enqueues once it is repaired.
        Err(error) if error.storage().is_none() => {
            error!(error = %error, "Dropping blob replication job with undecodable routing inputs");
            return Ok(BlobReplicationJobOutcome::TerminalFailure);
        }
        Err(error) => return Err(error.to_string()),
    };
    // Reference materialization writes real bytes on this node, so the job
    // carries this node's destination facts; a node mid-transition refuses.
    let gate = match gate_context(context, job.input.auth_context.realm_id, now_ms()).await {
        Ok(gate) => gate,
        Err(error) => return Err(error.to_string()),
    };
    let mut operation = ReplicateScopeOperation::new(job.input.clone()).with_routing(routing);
    if let Some(gate) = gate {
        operation = operation.with_gate(gate);
    }
    let mut watch_group_id = None;
    let mut relationship = if let Some(relationship_id) = job.relationship_id {
        let Some(relationship) = stored_relationship else {
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
        if job.reference_advance.is_some()
            && relationship.mode != SyncMode::Reference
            && relationship.reference_handling != ReferenceHandling::Preserve
        {
            return Ok(BlobReplicationJobOutcome::TerminalFailure);
        }
        let Some(bucket) = relationship.source.bucket() else {
            return Ok(BlobReplicationJobOutcome::TerminalFailure);
        };
        let creator = AuthContext {
            user_id: relationship.created_by,
            realm_id: relationship.created_by.realm_id,
            path_restrictions: None,
        };
        let source_auth_context = job_source_auth(job, &creator).clone();
        let source_authorization = match load_source_authorization(
            context,
            source_auth_context,
            relationship.source.node_id,
            bucket,
        )
        .await
        {
            Ok(authorization) => {
                watch_group_id = Some(authorization.group_id());
                authorization
            }
            Err((SourceAuthorizationError::Denied, group_id)) => {
                if job.reference_advance.is_some() {
                    return Ok(BlobReplicationJobOutcome::TerminalFailure);
                }
                let mut relationship = relationship;
                relationship.state = SyncState::Failed {
                    reason: "access_denied".to_string(),
                };
                mark_failure(&mut relationship, "access_denied");
                let stored = store_relationship(context, relationship.clone()).await?;
                cache_relationship(relationships, job, &relationship, stored);
                if stored && let Some(group_id) = group_id {
                    emit_sync_watch(context, &relationship, group_id, 0, Some("access_denied"))
                        .await;
                }
                return Ok(BlobReplicationJobOutcome::TerminalFailure);
            }
            Err((SourceAuthorizationError::Unavailable(error), group_id)) => {
                let mut relationship = relationship;
                mark_failure(&mut relationship, &error);
                let stored = store_relationship(context, relationship.clone()).await?;
                cache_relationship(relationships, job, &relationship, stored);
                if stored && let Some(group_id) = group_id {
                    emit_sync_watch(context, &relationship, group_id, 0, Some(&error)).await;
                }
                return Err(error);
            }
        };
        let writer_auth_context = if job.reference_advance.is_some() {
            None
        } else {
            job.writer_auth_context.clone().or(Some(creator))
        };
        operation = operation
            .with_relationship(
                relationship.clone(),
                job.origin.clone(),
                job.upstream_sources.clone(),
                writer_auth_context,
            )
            .with_source_authorization(source_authorization);
        if let Some(advance) = job.reference_advance {
            operation = operation.with_reference_advance(advance);
        }
        Some(relationship)
    } else {
        let Some(writer) = job.writer_auth_context.as_ref() else {
            error!(
                bucket = %job.input.bucket,
                "Dropping replication job without durable source writer"
            );
            return Ok(BlobReplicationJobOutcome::TerminalFailure);
        };
        let Some(source_node_id) = context.net_handle.as_ref().map(|net| net.node_id()) else {
            return Err("source node identity unavailable".to_string());
        };
        let source_authorization = match load_source_authorization(
            context,
            writer.clone(),
            source_node_id,
            &job.input.bucket,
        )
        .await
        {
            Ok(authorization) => authorization,
            Err((SourceAuthorizationError::Denied, _)) => {
                return Ok(BlobReplicationJobOutcome::TerminalFailure);
            }
            Err((SourceAuthorizationError::Unavailable(error), _)) => return Err(error),
        };
        operation = operation
            .with_source_authorization(source_authorization)
            .with_writer_auth(writer.clone());
        None
    };

    let error = match drive(operation, context).await {
        Ok(Some(Ok(result))) if result.failed == 0 => {
            if let Some(relationship) = relationship.as_mut() {
                mark_success(relationship, result.replicated, result.replicated_bytes);
                let stored = store_relationship(context, relationship.clone()).await?;
                cache_relationship(relationships, job, relationship, stored);
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
            if job.reference_advance.is_some()
                && result.last_error.as_deref().is_some_and(is_access_denied)
            {
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
                cache_relationship(relationships, job, relationship, stored);
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
    if is_access_denied(&error) || is_writer_denied(&error) {
        return Ok(BlobReplicationJobOutcome::TerminalFailure);
    }
    if let Some(relationship) = relationship.as_mut() {
        if is_writer_denied(&error) {
            return Ok(BlobReplicationJobOutcome::TerminalFailure);
        }
        mark_failure(relationship, &error);
        let stored = store_relationship(context, relationship.clone()).await?;
        cache_relationship(relationships, job, relationship, stored);
        if stored && let Some(group_id) = watch_group_id {
            emit_sync_watch(context, relationship, group_id, 0, Some(&error)).await;
        }
    }
    Err(error)
}

fn cache_relationship(
    relationships: &mut HashMap<(String, Ulid), SyncRelationship>,
    job: &BlobReplicationJobRecord,
    relationship: &SyncRelationship,
    stored: bool,
) {
    let Some(relationship_id) = job.relationship_id else {
        return;
    };
    let key = (job.input.bucket.clone(), relationship_id);
    if stored {
        relationships.insert(key, relationship.clone());
    } else {
        relationships.remove(&key);
    }
}

async fn read_job_relationships(
    storage: &StorageHandle,
    jobs: &[(Vec<u8>, BlobReplicationJobRecord)],
) -> Result<HashMap<(String, Ulid), SyncRelationship>, BlobReplicationQueueError> {
    let mut requested = HashSet::new();
    let mut keys = Vec::new();
    for (_, job) in jobs {
        let Some(relationship_id) = job.relationship_id else {
            continue;
        };
        let identity = (job.input.bucket.clone(), relationship_id);
        if requested.insert(identity.clone()) {
            keys.push(identity);
        }
    }
    if keys.is_empty() {
        return Ok(HashMap::new());
    }

    let reads = keys
        .iter()
        .map(|(bucket, relationship_id)| {
            (
                SYNC_RELATIONSHIP_OUT_KEYSPACE.to_string(),
                sync_relationship_key(bucket, *relationship_id).into(),
            )
        })
        .collect();
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
    if values.len() != keys.len() {
        return Err(BlobReplicationQueueError::UnexpectedEvent(
            "blob replication relationship read count mismatch".to_string(),
        ));
    }

    let mut relationships = HashMap::with_capacity(keys.len());
    for ((bucket, relationship_id), (key, value)) in keys.into_iter().zip(values) {
        let Some(value) = value else {
            continue;
        };
        let relationship = SyncRelationship::from_bytes(&value)?;
        validate_sync_key(&bucket, &key, &relationship)?;
        if relationship.id != relationship_id {
            return Err(ConversionError::FromStrError(
                "sync relationship id does not match requested id".to_string(),
            )
            .into());
        }
        relationships.insert((bucket, relationship_id), relationship);
    }
    Ok(relationships)
}

async fn process_live_obligations(
    context: &DriverContext,
) -> Result<LiveReplicationRepairResult, BlobReplicationQueueError> {
    let (obligations, has_more) = read_live_obligations(&context.storage_handle).await?;
    let mut starts = HashMap::<String, Option<Vec<u8>>>::new();
    for (_, obligation) in &obligations {
        if obligation
            .origin
            .as_ref()
            .is_some_and(|origin| origin.hop_count >= 4)
            || obligation
                .continuation
                .as_ref()
                .is_some_and(|cursor| cursor.relationships_complete)
        {
            continue;
        }
        let start = obligation
            .continuation
            .as_ref()
            .and_then(|cursor| cursor.relationship_after.clone());
        starts
            .entry(obligation.bucket.clone())
            .and_modify(|current| {
                let replace = match (current.as_ref(), start.as_ref()) {
                    (Some(current), Some(start)) => start < current,
                    (Some(_), None) => true,
                    _ => false,
                };
                if replace {
                    *current = start.clone();
                }
            })
            .or_insert(start);
    }
    let mut relationship_cache = HashMap::<String, RelationshipPage>::new();
    let mut relationship_work = 0usize;
    for (bucket, start) in starts {
        let remaining = LIVE_REPLICATION_RELATIONSHIP_LIMIT.saturating_sub(relationship_work);
        if remaining == 0 {
            break;
        }
        let page =
            read_relationships_limit(&context.storage_handle, &bucket, start, remaining).await?;
        relationship_work = relationship_work.saturating_add(page.values.len());
        relationship_cache.insert(bucket, page);
    }
    let mut result = LiveReplicationRepairResult {
        has_more,
        ..Default::default()
    };

    for (obligation_key, obligation) in obligations {
        let cursor = obligation.continuation.clone().unwrap_or_default();
        // A hop-exhausted obligation is never scanned for relationships, so it
        // must reach write_live_jobs to be retired instead of blocking the scan.
        let hop_exhausted = obligation
            .origin
            .as_ref()
            .is_some_and(|origin| origin.hop_count >= 4);
        let relationships = if cursor.relationships_complete {
            None
        } else {
            relationship_cache.get(&obligation.bucket)
        };
        if !hop_exhausted && !cursor.relationships_complete && relationships.is_none() {
            result.has_more = true;
            continue;
        }
        let write = write_live_jobs(&context.storage_handle, &obligation, relationships).await?;
        if !write.progressed {
            result.has_more = true;
            continue;
        }
        if let Some(continuation) = write.continuation {
            let mut next = obligation.clone();
            next.continuation = Some(continuation);
            write_live_obligation(&context.storage_handle, &next).await?;
            result.has_more = true;
        } else {
            delete_live_obligation(
                &context.storage_handle,
                obligation_key,
                obligation.continuation.as_ref(),
            )
            .await?;
        }
        result.processed = result.processed.saturating_add(1);
        result.queued = result.queued.saturating_add(write.queued);
    }

    Ok(result)
}

async fn read_live_obligations(
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
                        delete_live_obligation(storage, key, None).await?;
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

fn continuation_newer(
    candidate: &LiveReplicationContinuation,
    current: &LiveReplicationContinuation,
) -> bool {
    if candidate.relationships_complete != current.relationships_complete {
        return candidate.relationships_complete;
    }
    if candidate.relationships_complete {
        return false;
    }
    match (&candidate.relationship_after, &current.relationship_after) {
        (Some(candidate), Some(current)) => candidate > current,
        (Some(_), None) => true,
        (None, Some(_)) | (None, None) => false,
    }
}

async fn write_live_obligation(
    storage: &StorageHandle,
    record: &LiveReplicationObligationRecord,
) -> Result<(), BlobReplicationQueueError> {
    let (key_space, key, value) = live_obligation_entry(record)?;
    let txn_id = match storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
        other => {
            return Err(BlobReplicationQueueError::UnexpectedEvent(format!(
                "{other:?}"
            )));
        }
    };
    let (current, valid) = match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: key_space.clone(),
            key: key.clone(),
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => (None, true),
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => match LiveReplicationObligationRecord::from_bytes(&value) {
            Ok(record) => (Some(record), true),
            Err(_) => (None, false),
        },
        Event::Storage(StorageEvent::Error { error }) => {
            abort_cursor(storage, txn_id).await;
            return Err(error.into());
        }
        other => {
            abort_cursor(storage, txn_id).await;
            return Err(BlobReplicationQueueError::UnexpectedEvent(format!(
                "{other:?}"
            )));
        }
    };
    let should_write = !valid
        || match current.as_ref() {
            None => true,
            Some(current) => match (record.continuation.as_ref(), current.continuation.as_ref()) {
                (Some(candidate), Some(current)) => continuation_newer(candidate, current),
                (Some(_), None) => true,
                (None, None) | (None, Some(_)) => false,
            },
        };
    if !should_write {
        abort_cursor(storage, txn_id).await;
        return Ok(());
    }
    match storage
        .send_storage_effect(StorageEffect::Write {
            key_space,
            key,
            value,
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        Event::Storage(StorageEvent::Error { error }) => {
            abort_cursor(storage, txn_id).await;
            return Err(error.into());
        }
        other => {
            abort_cursor(storage, txn_id).await;
            return Err(BlobReplicationQueueError::UnexpectedEvent(format!(
                "{other:?}"
            )));
        }
    }
    match storage
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(BlobReplicationQueueError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

async fn read_relationships_limit(
    storage: &StorageHandle,
    bucket: &str,
    start: Option<Vec<u8>>,
    limit: usize,
) -> Result<RelationshipPage, BlobReplicationQueueError> {
    let mut start_after = start.clone();
    let mut relationships = Vec::with_capacity(limit.min(REPLICATION_SCAN_PAGE_SIZE));
    if limit == 0 {
        return Ok(RelationshipPage {
            values: relationships,
            next: None,
        });
    }
    loop {
        let page_limit = (limit - relationships.len()).min(REPLICATION_SCAN_PAGE_SIZE);
        let event = storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: SYNC_RELATIONSHIP_OUT_KEYSPACE.to_string(),
                prefix: Some(sync_relationship_prefix(bucket).into()),
                start: start_after.take().map(|key| IterStart::After(key.into())),
                limit: page_limit,
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
            relationships.push((key.to_vec(), relationship));
        }
        if relationships.len() == limit {
            return Ok(RelationshipPage {
                values: relationships,
                next: next_start_after.map(|key| key.to_vec()),
            });
        }
        match next_start_after {
            Some(next) => start_after = Some(next.to_vec()),
            None => {
                return Ok(RelationshipPage {
                    values: relationships,
                    next: None,
                });
            }
        }
    }
}

async fn write_live_jobs(
    storage: &StorageHandle,
    obligation: &LiveReplicationObligationRecord,
    relationships: Option<&RelationshipPage>,
) -> Result<LiveRepairWrite, BlobReplicationQueueError> {
    if obligation
        .origin
        .as_ref()
        .is_some_and(|origin| origin.hop_count >= 4)
    {
        warn!(
            bucket = %obligation.bucket,
            key = %obligation.key,
            version_id = %obligation.version_id,
            "Retiring live replication obligation at the hop limit"
        );
        return Ok(LiveRepairWrite {
            queued: 0,
            continuation: None,
            progressed: true,
        });
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
    let mut cursor = obligation.continuation.clone().unwrap_or_default();
    let mut relationship_jobs = Vec::with_capacity(LIVE_REPLICATION_JOB_LIMIT);
    if !cursor.relationships_complete {
        let Some(page) = relationships else {
            return Ok(LiveRepairWrite {
                queued: 0,
                continuation: None,
                progressed: false,
            });
        };
        let mut found = false;
        let mut complete = page.next.is_none();
        for (index, (key, relationship)) in page.values.iter().enumerate() {
            if cursor
                .relationship_after
                .as_ref()
                .is_some_and(|after| key <= after)
            {
                continue;
            }
            found = true;
            cursor.relationship_after = Some(key.clone());
            if obligation.reference_advance.is_some()
                && relationship.mode != SyncMode::Reference
                && relationship.reference_handling != ReferenceHandling::Preserve
            {
                continue;
            }
            if let Some(job) = relationship_job(
                obligation.local_node_id,
                RelationshipJobTarget {
                    bucket: &obligation.bucket,
                    key: &obligation.key,
                    version_id: obligation.version_id,
                    delete_marker: obligation.delete_marker,
                },
                relationship.clone(),
                obligation.origin.as_ref(),
                &upstream_sources,
            )
            .map(|mut job| {
                if let Some(advance) = obligation.reference_advance {
                    job.input.auth_context = obligation.auth_context.clone();
                    job.writer_auth_context = None;
                    job.with_reference_advance(advance)
                } else {
                    job.with_writer_auth(obligation.auth_context.clone())
                }
            }) {
                if relationship_jobs.len() == LIVE_REPLICATION_JOB_LIMIT {
                    complete = false;
                    break;
                }
                relationship_jobs.push(job);
                if relationship_jobs.len() == LIVE_REPLICATION_JOB_LIMIT {
                    complete = index + 1 == page.values.len() && page.next.is_none();
                    break;
                }
            }
        }
        if !found {
            if page.next.is_none() {
                cursor.relationship_after = None;
                cursor.relationships_complete = true;
            } else {
                return Ok(LiveRepairWrite {
                    queued: 0,
                    continuation: None,
                    progressed: false,
                });
            }
        } else if complete {
            cursor.relationship_after = None;
            cursor.relationships_complete = true;
        } else {
            cursor.relationships_complete = false;
        }
    }
    let continuation = (!cursor.relationships_complete).then_some(cursor);
    let jobs = relationship_jobs;
    if jobs.is_empty() {
        return Ok(LiveRepairWrite {
            queued: 0,
            continuation,
            progressed: true,
        });
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

    let mut writes = Vec::with_capacity(jobs.len());
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
        return Ok(LiveRepairWrite {
            queued: 0,
            continuation,
            progressed: true,
        });
    }
    match storage
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchWriteResult { entries }) => Ok(LiveRepairWrite {
            queued: entries.len(),
            continuation,
            progressed: true,
        }),
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
    expected: Option<&LiveReplicationContinuation>,
) -> Result<(), BlobReplicationQueueError> {
    let txn_id = match storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
        other => {
            return Err(BlobReplicationQueueError::UnexpectedEvent(format!(
                "{other:?}"
            )));
        }
    };
    let current = match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: BLOB_LIVE_REPLICATION_OBLIGATION_KEYSPACE.to_string(),
            key: ByteView::from(key.clone()),
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
        Event::Storage(StorageEvent::Error { error }) => {
            abort_cursor(storage, txn_id).await;
            return Err(error.into());
        }
        other => {
            abort_cursor(storage, txn_id).await;
            return Err(BlobReplicationQueueError::UnexpectedEvent(format!(
                "{other:?}"
            )));
        }
    };
    let should_delete = match current {
        None => false,
        Some(value) => match LiveReplicationObligationRecord::from_bytes(&value) {
            Ok(record) => record.continuation.as_ref() == expected,
            Err(_) => expected.is_none(),
        },
    };
    if !should_delete {
        abort_cursor(storage, txn_id).await;
        return Ok(());
    }
    match storage
        .send_storage_effect(StorageEffect::Delete {
            key_space: BLOB_LIVE_REPLICATION_OBLIGATION_KEYSPACE.to_string(),
            key: ByteView::from(key),
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::DeleteResult { .. }) => {}
        Event::Storage(StorageEvent::Error { error }) => {
            abort_cursor(storage, txn_id).await;
            return Err(error.into());
        }
        other => {
            abort_cursor(storage, txn_id).await;
            return Err(BlobReplicationQueueError::UnexpectedEvent(format!(
                "{other:?}"
            )));
        }
    }
    match storage
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(BlobReplicationQueueError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

async fn read_replication_cursor(
    storage: &StorageHandle,
) -> Result<ReplicationScanCursor, BlobReplicationQueueError> {
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: NODE_STATE_KEYSPACE.to_string(),
            key: ByteView::from(REPLICATION_CURSOR_KEY.to_vec()),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => {
            Ok(ReplicationScanCursor::default())
        }
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => Ok(postcard::from_bytes(value.as_ref()).unwrap_or_default()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(BlobReplicationQueueError::UnexpectedEvent(format!(
            "{other:?}"
        ))),
    }
}

fn cursor_newer(candidate: &ReplicationScanCursor, current: &ReplicationScanCursor) -> bool {
    if candidate.generation != current.generation {
        return candidate.generation > current.generation;
    }
    match (&candidate.after, &current.after) {
        (Some(candidate), Some(current)) => candidate > current,
        (Some(_), None) => true,
        (None, Some(_)) => false,
        (None, None) => false,
    }
}

fn cursor_merge(
    candidate: ReplicationScanCursor,
    current: ReplicationScanCursor,
) -> ReplicationScanCursor {
    if cursor_newer(&candidate, &current) {
        candidate
    } else {
        ReplicationScanCursor {
            next_due_at_ms: match (candidate.next_due_at_ms, current.next_due_at_ms) {
                (Some(candidate), Some(current)) => Some(candidate.min(current)),
                (Some(candidate), None) => Some(candidate),
                (None, Some(current)) => Some(current),
                (None, None) => None,
            },
            ..current
        }
    }
}

async fn abort_cursor(storage: &StorageHandle, txn_id: Ulid) {
    let _ = storage
        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
        .await;
}

async fn advance_replication_cursor(
    storage: &StorageHandle,
    candidate: ReplicationScanCursor,
) -> Result<(), BlobReplicationQueueError> {
    let txn_id = match storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
        other => {
            return Err(BlobReplicationQueueError::UnexpectedEvent(format!(
                "{other:?}"
            )));
        }
    };
    let (current, valid) = match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: NODE_STATE_KEYSPACE.to_string(),
            key: ByteView::from(REPLICATION_CURSOR_KEY.to_vec()),
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => {
            (ReplicationScanCursor::default(), true)
        }
        Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) => match postcard::from_bytes(value.as_ref()) {
            Ok(cursor) => (cursor, true),
            Err(_) => (ReplicationScanCursor::default(), false),
        },
        Event::Storage(StorageEvent::Error { error }) => {
            abort_cursor(storage, txn_id).await;
            return Err(error.into());
        }
        other => {
            abort_cursor(storage, txn_id).await;
            return Err(BlobReplicationQueueError::UnexpectedEvent(format!(
                "{other:?}"
            )));
        }
    };
    let merged = cursor_merge(candidate, current.clone());
    if valid && merged == current {
        match storage
            .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
            .await
        {
            Event::Storage(StorageEvent::TransactionAborted { .. }) => Ok(()),
            Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
            other => Err(BlobReplicationQueueError::UnexpectedEvent(format!(
                "{other:?}"
            ))),
        }
    } else {
        let value = postcard::to_allocvec(&merged).map_err(ConversionError::from)?;
        match storage
            .send_storage_effect(StorageEffect::Write {
                key_space: NODE_STATE_KEYSPACE.to_string(),
                key: ByteView::from(REPLICATION_CURSOR_KEY.to_vec()),
                value: ByteView::from(value),
                txn_id: Some(txn_id),
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            Event::Storage(StorageEvent::Error { error }) => {
                abort_cursor(storage, txn_id).await;
                return Err(error.into());
            }
            other => {
                abort_cursor(storage, txn_id).await;
                return Err(BlobReplicationQueueError::UnexpectedEvent(format!(
                    "{other:?}"
                )));
            }
        }
        match storage
            .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
            .await
        {
            Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(()),
            Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
            other => Err(BlobReplicationQueueError::UnexpectedEvent(format!(
                "{other:?}"
            ))),
        }
    }
}

async fn scan_due_jobs(
    storage: &StorageHandle,
    now_ms: u64,
    limit: usize,
) -> Result<BlobReplicationJobScan, BlobReplicationQueueError> {
    let cursor = read_replication_cursor(storage).await?;
    let start_after = cursor.after.clone();
    let mut jobs = Vec::new();
    let mut next_due_at_ms = cursor.next_due_at_ms;
    let mut canonical_changed = false;
    let event = storage
        .send_storage_effect(StorageEffect::Iter {
            key_space: BLOB_REPLICATION_JOB_KEYSPACE.to_string(),
            prefix: None,
            start: start_after.map(|key| IterStart::After(ByteView::from(key))),
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
    let mut last_key;
    let page_empty = values.is_empty();

    for (key, value) in values {
        let mut key = key.to_vec();
        last_key = Some(key.clone());
        let mut job = match BlobReplicationJobRecord::from_bytes(&value) {
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
                job = existing;
            } else {
                write_blob_replication_job(storage, &job).await?;
                delete_blob_replication_job(storage, key).await?;
                canonical_changed = true;
                jobs.retain(|(existing_key, _)| existing_key != &canonical_key);
            }
            key = canonical_key;
        } else if canonical_changed
            && let Some(existing) =
                read_blob_replication_job_at_key(storage, &canonical_key).await?
        {
            job = existing;
        }
        if job.due_at_ms > now_ms {
            next_due_at_ms = min_due_at(next_due_at_ms, job.due_at_ms);
            continue;
        }
        if merge_due_job(&mut jobs, key, job, limit) {
            return Ok(BlobReplicationJobScan {
                next_cursor: ReplicationScanCursor {
                    generation: cursor.generation,
                    after: last_key,
                    next_due_at_ms,
                },
                jobs,
                has_more_due: true,
                next_due_at_ms,
            });
        }
    }

    if let Some(next) = next_start_after {
        return Ok(BlobReplicationJobScan {
            next_cursor: ReplicationScanCursor {
                generation: cursor.generation,
                after: Some(next.to_vec()),
                next_due_at_ms,
            },
            jobs,
            has_more_due: true,
            next_due_at_ms,
        });
    }

    Ok(BlobReplicationJobScan {
        next_cursor: ReplicationScanCursor {
            generation: if page_empty && cursor.after.is_none() {
                cursor.generation
            } else {
                cursor.generation.saturating_add(1)
            },
            after: None,
            next_due_at_ms,
        },
        jobs,
        has_more_due: false,
        next_due_at_ms,
    })
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
        reference_advance: job.reference_advance,
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

fn merge_due_job(
    jobs: &mut Vec<(Vec<u8>, BlobReplicationJobRecord)>,
    key: Vec<u8>,
    job: BlobReplicationJobRecord,
    limit: usize,
) -> bool {
    if let Some((_, existing)) = jobs
        .iter_mut()
        .find(|(existing_key, _)| existing_key.as_slice() == key.as_slice())
    {
        if blob_replication_job_preferred(&job, existing) {
            *existing = job;
        }
        false
    } else {
        jobs.push((key, job));
        jobs.len() >= limit
    }
}

fn due_after(now_ms: u64, due_at_ms: u64) -> Duration {
    Duration::from_millis(due_at_ms.saturating_sub(now_ms))
}

#[cfg(test)]
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

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::keyspaces::{
        AUTH_KEYSPACE, BLOB_VERSIONS_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE,
    };
    use aruna_core::request_policy::{PolicyKind, RequestPolicy};
    use aruna_core::structs::{
        Actor, ArunaArn, BackendRef, BlobVersion, BucketInfo, Group, GroupAuthorizationDocument,
        PathRestriction, Permission, RealmAuthorizationDocument, RealmConfigDocument, RealmId,
        ReferenceHandling, SyncStatusSnapshot, VersionKey, blob_object_permission_path,
        sync_relationship_key,
    };
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

    async fn read_jobs(storage: &StorageHandle) -> Vec<(Vec<u8>, BlobReplicationJobRecord)> {
        match storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: BLOB_REPLICATION_JOB_KEYSPACE.to_string(),
                prefix: None,
                start: None,
                limit: LIVE_REPLICATION_RELATIONSHIP_LIMIT,
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
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
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
        write_raw_queue_record(
            storage,
            REALM_CONFIG_KEYSPACE,
            realm().as_bytes().to_vec(),
            RealmConfigDocument::default_for_realm(realm(), Vec::new())
                .to_bytes(&actor)
                .unwrap(),
        )
        .await;
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

    // Policy loading resolves the group record before group policies apply.
    async fn write_group(storage: &StorageHandle, group_id: Ulid) {
        let actor = Actor {
            node_id: node(1),
            user_id: user(),
            realm_id: realm(),
        };
        let auth = GroupAuthorizationDocument::new_default_group_doc(user(), realm(), group_id);
        let group = Group {
            display_name: "replication-group".to_string(),
            group_id,
            realm_id: realm(),
            roles: auth.roles.keys().copied().collect(),
            owner: user(),
        };
        write_raw_queue_record(
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
        let mut document =
            GroupAuthorizationDocument::new_default_group_doc(user(), realm(), group_id);
        document.policies.push(RequestPolicy {
            policy_id: Ulid::from_parts(7, 7),
            name: "replication-policy".to_string(),
            kind: PolicyKind::Deny,
            when: None,
            expression: expression.to_string(),
            enabled: true,
        });
        write_raw_queue_record(
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
        let version = BlobVersion::materialized(
            [7u8; 32],
            BackendRef::node_default(),
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
    async fn retry_keeps_advance() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let advance = reference_advance();
        let mut job = BlobReplicationJobRecord::new_relationship(
            on_demand_input(),
            None,
            Ulid::from_parts(78, 1),
            42,
        )
        .with_reference_advance(advance);
        job.writer_auth_context = None;
        let key = blob_replication_job_key(&job).unwrap().to_vec();

        reschedule_blob_replication_job(&storage, key, &job, "retry".to_string())
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
        write_group(&storage, group_id).await;
        let path = blob_object_permission_path(realm(), group_id, node(1), "bucket", "key");
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
            QueueBlobReplicationOperation::new_relationship(
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

        let relationship = relationship(83, 2, None, true);
        let source_bucket = relationship.source.bucket().unwrap();
        let creator = AuthContext {
            user_id: relationship.created_by,
            realm_id: relationship.source.realm_id,
            path_restrictions: None,
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
            writer_auth_context: Some(auth_context()),
            reference_advance: None,
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
    fn job_requires_writer() {
        let mut record = BlobReplicationJobRecord::new(on_demand_input(), None, 42);
        record.writer_auth_context = None;
        let bytes = record.to_bytes().unwrap();
        assert!(BlobReplicationJobRecord::from_bytes(&bytes).is_err());
        record.reference_advance = Some(reference_advance());
        let bytes = record.to_bytes().unwrap();
        assert_eq!(
            BlobReplicationJobRecord::from_bytes(&bytes).unwrap(),
            record
        );

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
    fn advance_auth_hops() {
        let mut input = on_demand_input();
        input.auth_context.path_restrictions = Some(vec![PathRestriction {
            pattern: "**".to_string(),
            permission: Permission::READ,
        }]);
        let reader = input.auth_context.clone();
        let creator = auth_context();
        let mut job =
            BlobReplicationJobRecord::new_relationship(input, None, Ulid::from_parts(3, 3), 42)
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
        let record = BlobReplicationJobRecord::new(on_demand_input(), None, 42);
        let encoded = record.to_bytes().unwrap();
        assert_eq!(
            BlobReplicationJobRecord::from_bytes(&encoded).unwrap(),
            record
        );

        let mut trailing = encoded.clone();
        trailing.push(0xff);
        assert!(BlobReplicationJobRecord::from_bytes(&trailing).is_err());
        let mut short = encoded;
        short.pop().unwrap();
        assert!(BlobReplicationJobRecord::from_bytes(&short).is_err());
        let mut truncated = record
            .with_reference_advance(reference_advance())
            .to_bytes()
            .unwrap();
        truncated.pop().unwrap();
        assert!(BlobReplicationJobRecord::from_bytes(&truncated).is_err());
    }

    #[test]
    fn obligation_rejects_malformed() {
        // Greenfield: pre-advance record bytes decode strictly or not at all.
        let record = LiveReplicationObligationRecord::new(
            node(1),
            auth_context(),
            "bucket".to_string(),
            "key".to_string(),
            Ulid::from_parts(2, 2),
            false,
        );
        let encoded = record.to_bytes().unwrap();
        assert_eq!(
            LiveReplicationObligationRecord::from_bytes(&encoded).unwrap(),
            record
        );

        let mut trailing = encoded.clone();
        trailing.push(0xff);
        assert!(LiveReplicationObligationRecord::from_bytes(&trailing).is_err());
        let mut short = encoded;
        short.pop().unwrap();
        assert!(LiveReplicationObligationRecord::from_bytes(&short).is_err());
        let mut truncated = record
            .with_reference_advance(reference_advance())
            .to_bytes()
            .unwrap();
        truncated.pop().unwrap();
        assert!(LiveReplicationObligationRecord::from_bytes(&truncated).is_err());
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

        let relationships = read_relationships_limit(
            &storage,
            &obligation.bucket,
            None,
            LIVE_REPLICATION_RELATIONSHIP_LIMIT,
        )
        .await
        .unwrap();
        assert_eq!(
            write_live_jobs(&storage, &obligation, Some(&relationships), None)
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
            QueueLiveVersionReplicationOperation::new(QueueLiveVersionReplicationInput {
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
    async fn live_version_queue_preserves_future_retry_job() {
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

        repair_live(&storage).await;
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
    async fn repair_continues_jobs() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        for id in 100..165 {
            write_relationship(&storage, &relationship(id, 2, None, true)).await;
        }
        write_live_obligation(&storage, Ulid::from_parts(91, 1)).await;

        repair_live(&storage).await;
        assert_eq!(read_jobs(&storage).await.len(), LIVE_REPLICATION_JOB_LIMIT);
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
                    SYNC_RELATIONSHIP_OUT_KEYSPACE.to_string(),
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

        let first = read_relationships_limit(
            &storage,
            "bucket",
            None,
            LIVE_REPLICATION_RELATIONSHIP_LIMIT,
        )
        .await
        .unwrap();
        assert_eq!(first.values.len(), LIVE_REPLICATION_RELATIONSHIP_LIMIT);
        let second = read_relationships_limit(
            &storage,
            "bucket",
            Some(first.next.clone().expect("relationship page continues")),
            LIVE_REPLICATION_RELATIONSHIP_LIMIT,
        )
        .await
        .unwrap();
        assert_eq!(second.values.len(), 1);
        assert!(second.next.is_none());
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
    async fn canonical_scan_once() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let job = BlobReplicationJobRecord::new(on_demand_input(), None, unix_timestamp_millis());
        write_blob_replication_job(&storage, &job)
            .await
            .expect("job writes");

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
        let first = BlobReplicationJobRecord::new(on_demand_input(), None, unix_timestamp_millis());
        let mut second_input = on_demand_input();
        second_input.target = ReplicateScopeTarget::Object {
            key: "other".to_string(),
        };
        let second = BlobReplicationJobRecord::new(second_input, None, unix_timestamp_millis());
        write_blob_replication_job(&storage, &first)
            .await
            .expect("first job writes");
        write_blob_replication_job(&storage, &second)
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
        write_raw_queue_record(
            &storage,
            NODE_STATE_KEYSPACE,
            REPLICATION_CURSOR_KEY.to_vec(),
            vec![0xff],
        )
        .await;
        write_blob_replication_job(
            &storage,
            &BlobReplicationJobRecord::new(on_demand_input(), None, unix_timestamp_millis()),
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
        let job = BlobReplicationJobRecord::new(on_demand_input(), None, unix_timestamp_millis());
        let preferred = BlobReplicationJobRecord {
            attempts: 1,
            last_error: Some("retry".to_string()),
            ..job.clone()
        };
        write_blob_replication_job(&storage, &job)
            .await
            .expect("canonical job writes");
        write_raw_queue_record(
            &storage,
            BLOB_REPLICATION_JOB_KEYSPACE,
            b"legacy-job".to_vec(),
            preferred.to_bytes().expect("job serializes"),
        )
        .await;

        let scan = scan_due_jobs(&storage, unix_timestamp_millis(), 2)
            .await
            .expect("duplicate repair succeeds");

        assert_eq!(scan.jobs.len(), 1);
        assert_eq!(scan.jobs[0].1.attempts, 1);
        assert_eq!(
            scan.jobs[0].0,
            blob_replication_job_key(&preferred).unwrap().as_ref()
        );
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
                BlobReplicationJobRecord::new_relationship(
                    on_demand_input(),
                    None,
                    relationship.id,
                    unix_timestamp_millis(),
                ),
            ),
            (
                Vec::new(),
                BlobReplicationJobRecord::new_relationship(
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
        write_relationship(&storage, &relationship(20, 2, None, true)).await;
        write_relationship(&storage, &relationship(21, 3, None, true)).await;
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
    async fn hop_limit_retires() {
        // A hop-exhausted obligation is never scanned for relationships, so it
        // must be deleted instead of blocking the head of the scan forever.
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let record = LiveReplicationObligationRecord::new(
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
        let mut record = LiveReplicationObligationRecord::new(
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
        let key = live_replication_obligation_key(&record)
            .expect("obligation key builds")
            .to_vec();
        let observed = LiveReplicationContinuation {
            relationship_after: Some(vec![1]),
            relationships_complete: false,
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
        let mut record = LiveReplicationObligationRecord::new(
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
        });
        super::write_live_obligation(&storage, &record)
            .await
            .expect("obligation persists");
        let mut stale = record.clone();
        stale.continuation = Some(LiveReplicationContinuation {
            relationship_after: Some(vec![1]),
            relationships_complete: false,
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
        let path =
            blob_object_permission_path(realm(), group_id, net_handle.node_id(), "bucket", "key");
        write_group_policy(&storage, group_id, &format!("path == '{path}'")).await;
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: Some(net_handle),
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
            .expect("denied writer is terminal");

        assert_eq!(result.succeeded, 0);
        assert_eq!(result.failed, 1);
        assert!(read_jobs(&storage).await.is_empty());
    }

    #[tokio::test]
    async fn live_replication_obligation_repairs_missing_jobs() {
        let temp_dir = tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("temp path"))
            .expect("storage opens");
        let version_id = Ulid::from_parts(4, 4);
        write_bucket(&storage, "bucket").await;
        write_relationship(&storage, &relationship(22, 2, None, true)).await;
        write_relationship(&storage, &relationship(23, 3, None, true)).await;
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
