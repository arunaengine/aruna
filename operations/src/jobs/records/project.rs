//! Rebuilding one family's projection from its immutable records.
//!
//! The projection is a cache with a bounded revision, never authority: it is
//! deleted-and-rebuilt safe, it is invalidated by every append, and the mutable
//! job row it bridges into is a local view of it, not its owner.

use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{JOB_FAMILY_ALIAS_KEYSPACE, JOB_FAMILY_PROJECTION_KEYSPACE};
use aruna_core::keyspaces::{JOB_FAMILY_RECORD_KEYSPACE, JOB_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::{
    JobFamilyId, JobId, JobProjection, JobRecord, JobRecordEnvelope, JobRecordKey, JobState,
    LogicalJobState, job_record_key, validate_transition,
};
use aruna_core::types::{Effects, Key, TxnId, Value};
use smallvec::smallvec;
use tracing::debug;

use super::keys::{alias_family, alias_prefix, family_prefix, record_key};
use super::reduce::reduce_family;
use super::rows::{ProjectionCache, from_bytes, to_bytes};
use super::{MAX_PROJECTION_RECORDS, RECORD_PAGE_SIZE, RecordStoreError};
use crate::jobs::store::{JobDeletes, JobWrites, index_deltas};

/// Aliases one family may bridge into local job rows.
const MAX_BRIDGED_ALIASES: usize = 64;
/// Families one alias may resolve to. Two families claiming one id is an
/// anomaly that stays visible instead of rebinding the first one.
const MAX_ALIAS_FAMILIES: usize = 8;

/// How a caller names the family to project.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FamilyRef {
    Family(JobFamilyId),
    /// Any accepted alias resolves to the request family that admitted it.
    Alias(JobId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProjectFamilyConfig {
    pub family: FamilyRef,
    pub now_ms: u64,
    /// Rebuild even when the cached revision is current.
    pub rebuild: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedFamily {
    pub family: JobFamilyId,
    pub revision: u64,
    /// `None` while the family has no accepted alias yet.
    pub projection: Option<JobProjection>,
    /// True when the family holds more records than one projection may reduce.
    pub truncated: bool,
    pub cached: bool,
}

#[derive(Debug, PartialEq)]
pub struct ProjectFamilyOperation {
    config: ProjectFamilyConfig,
    family: Option<JobFamilyId>,
    cache: Option<ProjectionCache>,
    records: Vec<JobRecordEnvelope>,
    cursor: Option<JobRecordKey>,
    truncated: bool,
    projection: Option<JobProjection>,
    bridged: JobDeletes,
    state: ProjectState,
    outcome: Option<Result<ProjectedFamily, RecordStoreError>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProjectState {
    Init,
    ResolveAlias,
    ReadCache,
    Begin,
    Page { txn_id: TxnId },
    ReadJobs { txn_id: TxnId },
    Write { txn_id: TxnId },
    Clear { txn_id: TxnId },
    Commit { txn_id: TxnId },
    Finish,
    Error,
}

impl ProjectFamilyOperation {
    pub fn new(config: ProjectFamilyConfig) -> Self {
        Self {
            config,
            family: match config.family {
                FamilyRef::Family(family) => Some(family),
                FamilyRef::Alias(_) => None,
            },
            cache: None,
            records: Vec::new(),
            cursor: None,
            truncated: false,
            projection: None,
            bridged: Vec::new(),
            state: ProjectState::Init,
            outcome: None,
        }
    }

    fn read_cache(&mut self) -> Effects {
        let Some(family) = self.family else {
            return self.fail(RecordStoreError::UnknownAlias);
        };
        self.state = ProjectState::ReadCache;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: JOB_FAMILY_PROJECTION_KEYSPACE.to_string(),
            key: family_prefix(&family),
            txn_id: None,
        })]
    }

    fn page(&mut self, txn_id: TxnId) -> Effects {
        let Some(family) = self.family else {
            return self.fail(RecordStoreError::UnknownAlias);
        };
        self.state = ProjectState::Page { txn_id };
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: JOB_FAMILY_RECORD_KEYSPACE.to_string(),
            prefix: Some(family_prefix(&family)),
            start: self.cursor.map(|key| IterStart::After(record_key(&key))),
            limit: RECORD_PAGE_SIZE,
            txn_id: Some(txn_id),
        })]
    }

    fn keep_page(&mut self, values: Vec<(Key, Value)>) -> bool {
        let full = values.len() >= RECORD_PAGE_SIZE;
        for (key, value) in values {
            if let Ok(record_key) = JobRecordKey::from_bytes(&key) {
                self.cursor = Some(record_key);
            }
            if let Ok(envelope) = from_bytes::<JobRecordEnvelope>(&value) {
                self.records.push(envelope);
            }
        }
        if self.records.len() >= MAX_PROJECTION_RECORDS {
            self.truncated = true;
            return false;
        }
        full
    }

    /// Reduces the loaded records, then reads the local rows of every alias the
    /// projection names so they can be bridged in the same transaction.
    fn reduce(&mut self, txn_id: TxnId) -> Effects {
        let Some(family) = self.family else {
            return self.fail(RecordStoreError::UnknownAlias);
        };
        self.projection = match reduce_family(family, &self.records) {
            Ok(projection) => projection,
            Err(error) => return self.fail(error.into()),
        };
        let reads: Vec<(String, Key)> = self
            .projection
            .iter()
            .flat_map(|projection| projection.aliases.iter())
            .take(MAX_BRIDGED_ALIASES)
            .map(|job_id| (JOB_KEYSPACE.to_string(), job_record_key(*job_id)))
            .collect();
        if reads.is_empty() {
            return self.write(txn_id, Vec::new(), Vec::new());
        }
        self.state = ProjectState::ReadJobs { txn_id };
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: Some(txn_id),
        })]
    }

    /// Bridges the reduced state into the mutable job rows this node keeps as a
    /// local cache. A row that already settled terminally, or whose local state
    /// machine forbids the transition, is left untouched, and the schedule
    /// indexes move with the row through the shared job-store deltas.
    fn bridge(&self, values: Vec<(Key, Option<Value>)>) -> (JobWrites, JobDeletes) {
        let mut writes: JobWrites = Vec::new();
        let mut deletes: JobDeletes = Vec::new();
        let Some(projection) = self.projection.as_ref() else {
            return (writes, deletes);
        };
        let Some(state) = bridged_state(projection.state) else {
            return (writes, deletes);
        };
        for (_, value) in values {
            let Some(value) = value else {
                continue;
            };
            let Ok(record) = JobRecord::from_bytes(&value) else {
                continue;
            };
            if record.state.is_terminal()
                || (record.state == state && record.cancel_requested == projection.cancel_requested)
            {
                continue;
            }
            if record.state != state
                && validate_transition(record.execution_class, record.state, state).is_err()
            {
                continue;
            }
            let mut bridged = record.clone();
            bridged.state = state;
            bridged.cancel_requested |= projection.cancel_requested;
            bridged.updated_at_ms = self.config.now_ms;
            if state.is_terminal() && bridged.finished_at_ms.is_none() {
                bridged.finished_at_ms = Some(self.config.now_ms);
            }
            let Ok((row_writes, row_deletes)) = index_deltas(&record, &bridged) else {
                continue;
            };
            writes.extend(row_writes);
            deletes.extend(row_deletes);
        }
        (writes, deletes)
    }

    fn write(&mut self, txn_id: TxnId, mut writes: JobWrites, deletes: JobDeletes) -> Effects {
        let Some(family) = self.family else {
            return self.fail(RecordStoreError::UnknownAlias);
        };
        let cache = self
            .cache
            .clone()
            .unwrap_or_else(|| ProjectionCache {
                revision: 0,
                stale: true,
                projection: None,
            })
            .updated(self.projection.clone());
        let bytes = match to_bytes(&cache) {
            Ok(bytes) => bytes,
            Err(error) => return self.fail(error.into()),
        };
        writes.push((
            JOB_FAMILY_PROJECTION_KEYSPACE.to_string(),
            family_prefix(&family),
            Value::from(bytes.as_slice()),
        ));
        self.cache = Some(cache);
        self.bridged = deletes;
        self.state = ProjectState::Write { txn_id };
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })]
    }

    /// Index rows the bridged job states left behind, cleared in the same
    /// transaction as the row and cache writes.
    fn clear(&mut self, txn_id: TxnId) -> Effects {
        let deletes = std::mem::take(&mut self.bridged);
        if deletes.is_empty() {
            return self.commit(txn_id);
        }
        self.state = ProjectState::Clear { txn_id };
        smallvec![Effect::Storage(StorageEffect::BatchDelete {
            deletes,
            txn_id: Some(txn_id),
        })]
    }

    fn commit(&mut self, txn_id: TxnId) -> Effects {
        self.state = ProjectState::Commit { txn_id };
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn settle(&mut self, cached: bool) -> Effects {
        let Some(family) = self.family else {
            return self.fail(RecordStoreError::UnknownAlias);
        };
        let revision = self.cache.as_ref().map_or(0, |cache| cache.revision);
        debug!(
            revision,
            records = self.records.len(),
            cached,
            "Job family projection resolved"
        );
        self.outcome = Some(Ok(ProjectedFamily {
            family,
            revision,
            projection: self.projection.clone(),
            truncated: self.truncated,
            cached,
        }));
        self.state = ProjectState::Finish;
        smallvec![]
    }

    fn fail(&mut self, error: RecordStoreError) -> Effects {
        self.outcome = Some(Err(error));
        self.state = ProjectState::Error;
        smallvec![]
    }

    fn unexpected(&mut self, expected: &'static str, got: String) -> Effects {
        let state = format!("{:?}", self.state);
        self.fail(RecordStoreError::UnexpectedEvent {
            state,
            expected,
            got,
        })
    }

    fn txn(&self) -> Option<TxnId> {
        match self.state {
            ProjectState::Page { txn_id }
            | ProjectState::ReadJobs { txn_id }
            | ProjectState::Write { txn_id }
            | ProjectState::Clear { txn_id }
            | ProjectState::Commit { txn_id } => Some(txn_id),
            _ => None,
        }
    }
}

impl Operation for ProjectFamilyOperation {
    type Output = ProjectedFamily;
    type Error = RecordStoreError;

    fn start(&mut self) -> Effects {
        match self.config.family {
            FamilyRef::Family(_) => self.read_cache(),
            FamilyRef::Alias(job_id) => {
                self.state = ProjectState::ResolveAlias;
                smallvec![Effect::Storage(StorageEffect::Iter {
                    key_space: JOB_FAMILY_ALIAS_KEYSPACE.to_string(),
                    prefix: Some(alias_prefix(job_id)),
                    start: None,
                    limit: MAX_ALIAS_FAMILIES,
                    txn_id: None,
                })]
            }
        }
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            ProjectState::ResolveAlias => match event {
                Event::Storage(StorageEvent::IterResult { values, .. }) => {
                    // Ordered by key, so two families claiming one alias resolve
                    // identically on every replica.
                    self.family = values.iter().filter_map(|(key, _)| alias_family(key)).min();
                    match self.family {
                        Some(_) => self.read_cache(),
                        None => self.fail(RecordStoreError::UnknownAlias),
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected("alias scan", format!("{other:?}")),
            },
            ProjectState::ReadCache => match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                    self.cache = value
                        .as_ref()
                        .and_then(|value| from_bytes::<ProjectionCache>(value).ok());
                    let fresh = self
                        .cache
                        .as_ref()
                        .is_some_and(|cache| !cache.stale && !self.config.rebuild);
                    if fresh {
                        self.projection = self
                            .cache
                            .as_ref()
                            .and_then(|cache| cache.projection.clone());
                        return self.settle(true);
                    }
                    self.state = ProjectState::Begin;
                    smallvec![Effect::Storage(StorageEffect::StartTransaction {
                        read: false
                    })]
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected("projection cache read", format!("{other:?}")),
            },
            ProjectState::Begin => match event {
                Event::Storage(StorageEvent::TransactionStarted { txn_id }) => self.page(txn_id),
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected("transaction start", format!("{other:?}")),
            },
            ProjectState::Page { txn_id } => match event {
                Event::Storage(StorageEvent::IterResult { values, .. }) => {
                    match self.keep_page(values) {
                        true => self.page(txn_id),
                        false => self.reduce(txn_id),
                    }
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected("record page", format!("{other:?}")),
            },
            ProjectState::ReadJobs { txn_id } => match event {
                Event::Storage(StorageEvent::BatchReadResult { values }) => {
                    let (writes, deletes) = self.bridge(values);
                    self.write(txn_id, writes, deletes)
                }
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected("job row read", format!("{other:?}")),
            },
            ProjectState::Write { txn_id } => match event {
                Event::Storage(StorageEvent::BatchWriteResult { .. }) => self.clear(txn_id),
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected("projection write", format!("{other:?}")),
            },
            ProjectState::Clear { txn_id } => match event {
                Event::Storage(StorageEvent::BatchDeleteResult { .. }) => self.commit(txn_id),
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected("index clear", format!("{other:?}")),
            },
            ProjectState::Commit { .. } => match event {
                Event::Storage(StorageEvent::TransactionCommitted { .. }) => self.settle(false),
                Event::Storage(StorageEvent::Error { error }) => self.fail(error.into()),
                other => self.unexpected("transaction commit", format!("{other:?}")),
            },
            ProjectState::Init | ProjectState::Finish | ProjectState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, ProjectState::Finish | ProjectState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.outcome.unwrap_or(Err(RecordStoreError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        match self.txn() {
            Some(txn_id) => {
                self.state = ProjectState::Error;
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            None => smallvec![],
        }
    }

    fn expected_error(error: &Self::Error) -> bool {
        matches!(error, RecordStoreError::UnknownAlias)
    }
}

/// Local job state the replicated projection implies. `Queued` is not bridged:
/// a family without executions says nothing about a local row's own progress.
fn bridged_state(state: LogicalJobState) -> Option<JobState> {
    match state {
        LogicalJobState::Queued => None,
        LogicalJobState::Running => Some(JobState::Running),
        LogicalJobState::Indeterminate => Some(JobState::Indeterminate),
        LogicalJobState::Succeeded => Some(JobState::Succeeded),
        LogicalJobState::Cancelled => Some(JobState::Cancelled),
    }
}
