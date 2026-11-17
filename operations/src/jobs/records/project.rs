//! Rebuilds one job family projection cache from its records and bridges the local job row.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{FAMILY_ALIAS_KEYSPACE, FAMILY_PROJECTION_KEYSPACE};
use aruna_core::keyspaces::{FAMILY_RECORD_KEYSPACE, JOB_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::execution::job::{
    JobFamilyId, JobId, JobProjection, JobRecord, JobRecordEnvelope, JobRecordKey, JobState,
    LogicalJobState, job_record_key, validate_transition,
};
use aruna_core::types::{Key, Value};
use tracing::debug;

use super::reduce::reduce_family;
use super::{MAX_PROJECTION_RECORDS, RECORD_PAGE_SIZE, RecordStoreError};
use aruna_core::structs::execution::job::JobWrites;
use aruna_core::structs::execution::job::record_keys::{
    MAX_ALIAS_FAMILIES, alias_family, alias_prefix, family_prefix, record_key,
};
use aruna_core::structs::execution::job::record_rows::{ProjectionCache, from_bytes, to_bytes};

/// Aliases one family may bridge into local job rows.
const MAX_BRIDGED_ALIASES: usize = 64;

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
    /// Such a projection is partial: it is never bridged and never cached, so a
    /// caller may inspect it but must not decide scheduling or admission on it.
    pub truncated: bool,
    pub cached: bool,
}

/// Reads the cache outside any transaction; a rebuild pages, bridges and caches the family in
/// one write transaction, which the runner opens at the first row call and commits on success.
#[derive(Debug, PartialEq)]
pub struct ProjectFamilyOperation {
    config: ProjectFamilyConfig,
    family: Option<JobFamilyId>,
    cache: Option<ProjectionCache>,
    records: Vec<JobRecordEnvelope>,
    cursor: Option<JobRecordKey>,
    truncated: bool,
    projection: Option<JobProjection>,
    state: ProjectState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProjectState {
    ResolveAlias,
    ReadCache,
    Page,
    ReadJobs,
    Write,
}

type Step = Result<Next<ProjectedFamily>, RecordStoreError>;

/// A storage effect outside the operation's transaction, answered by its event.
fn outside(effect: StorageEffect) -> Next<ProjectedFamily> {
    Next::Call(IoCall::Effect(Box::new(Effect::Storage(effect))))
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
            state: ProjectState::ResolveAlias,
        }
    }

    fn family(&self) -> Result<JobFamilyId, RecordStoreError> {
        self.family.ok_or(RecordStoreError::UnknownAlias)
    }

    fn read_cache(&mut self) -> Step {
        let family = self.family()?;
        self.state = ProjectState::ReadCache;
        Ok(outside(StorageEffect::Read {
            key_space: FAMILY_PROJECTION_KEYSPACE.to_string(),
            key: family_prefix(&family),
            txn_id: None,
        }))
    }

    fn page(&mut self) -> Step {
        let family = self.family()?;
        self.state = ProjectState::Page;
        Ok(Next::Call(IoCall::Page {
            space: FAMILY_RECORD_KEYSPACE.to_string(),
            prefix: family_prefix(&family),
            after: self.cursor.map(|key| record_key(&key)),
            limit: RECORD_PAGE_SIZE,
        }))
    }

    /// Keeps one page of records. A key or envelope this node cannot read is a
    /// corrupt row, never a record to skip: projecting without it would reduce
    /// a set that is silently short.
    fn keep_page(&mut self, values: Vec<(Key, Value)>) -> Result<bool, RecordStoreError> {
        let full = values.len() >= RECORD_PAGE_SIZE;
        for (key, value) in values {
            self.cursor = Some(JobRecordKey::from_bytes(&key)?);
            self.records.push(from_bytes::<JobRecordEnvelope>(&value)?);
        }
        if self.records.len() >= MAX_PROJECTION_RECORDS {
            self.truncated = true;
            return Ok(false);
        }
        Ok(full)
    }

    /// Reduces the loaded records, then reads the local rows of every alias the
    /// projection names so they can be bridged in the same transaction.
    fn reduce(&mut self) -> Step {
        self.projection = reduce_family(self.family()?, &self.records)?;
        // A truncated projection is partial: it is neither bridged into the
        // mutable rows nor cached, so a later read reduces the family again.
        if self.truncated {
            return self.settle(false);
        }
        let reads: Vec<(String, Key)> = self
            .projection
            .iter()
            .flat_map(|projection| projection.aliases.iter())
            .take(MAX_BRIDGED_ALIASES)
            .map(|job_id| (JOB_KEYSPACE.to_string(), job_record_key(*job_id)))
            .collect();
        if reads.is_empty() {
            return self.write(Vec::new());
        }
        self.state = ProjectState::ReadJobs;
        Ok(Next::Call(IoCall::Read(reads)))
    }

    /// Bridges the reduced state into the unscheduled logical cache rows. A row
    /// that already settled terminally, or whose state machine forbids the
    /// transition, is left untouched.
    fn bridge(&self, values: Vec<Option<Value>>) -> JobWrites {
        let mut writes: JobWrites = Vec::new();
        let Some(projection) = self.projection.as_ref() else {
            return writes;
        };
        let Some(state) = bridged_state(projection.state) else {
            return writes;
        };
        for value in values.into_iter().flatten() {
            let Ok(record) = JobRecord::from_bytes(&value) else {
                continue;
            };
            if record.state.is_terminal()
                || (record.state == state && record.cancel_requested == projection.cancel_requested)
            {
                continue;
            }
            if !projection.aliases.contains(&record.job_id)
                && record.state != state
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
            let Ok(bytes) = bridged.to_bytes() else {
                continue;
            };
            writes.push((
                JOB_KEYSPACE.to_string(),
                job_record_key(bridged.job_id),
                Value::from(bytes.as_slice()),
            ));
        }
        writes
    }

    fn write(&mut self, mut writes: JobWrites) -> Step {
        let family = self.family()?;
        let cache = self
            .cache
            .clone()
            .unwrap_or_else(|| ProjectionCache::invalidated(None))
            .updated(self.projection.clone());
        let bytes = to_bytes(&cache)?;
        writes.push((
            FAMILY_PROJECTION_KEYSPACE.to_string(),
            family_prefix(&family),
            Value::from(bytes.as_slice()),
        ));
        self.cache = Some(cache);
        self.state = ProjectState::Write;
        Ok(Next::Call(IoCall::Write(writes)))
    }

    fn settle(&mut self, cached: bool) -> Step {
        let family = self.family()?;
        let revision = self.cache.as_ref().map_or(0, |cache| cache.revision);
        let duplicates = self
            .projection
            .as_ref()
            .map(|projection| {
                projection
                    .executions
                    .iter()
                    .filter(|execution| {
                        execution.role
                            == aruna_core::structs::execution::job::ExecutionRole::DuplicateSuccess
                    })
                    .count()
            })
            .unwrap_or_default();
        debug!(
            revision,
            records = self.records.len(),
            cached,
            executions = self
                .projection
                .as_ref()
                .map(|projection| projection.executions.len())
                .unwrap_or_default(),
            duplicate_successes = duplicates,
            truncated = self.truncated,
            "Job family projection resolved"
        );
        Ok(Next::Done(ProjectedFamily {
            family,
            revision,
            projection: self.projection.clone(),
            truncated: self.truncated,
            cached,
        }))
    }

    fn unexpected(&self, expected: &'static str, got: &'static str) -> RecordStoreError {
        RecordStoreError::unexpected(&self.state, expected, got)
    }

    /// The answer to a read outside the transaction: its event, or the storage error it carried.
    fn outside_event(
        &self,
        expected: &'static str,
        result: IoResult,
    ) -> Result<StorageEvent, RecordStoreError> {
        let IoResult::Event(event) = result else {
            return Err(self.unexpected(expected, result.label()));
        };
        match *event {
            Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
            Event::Storage(event) => Ok(event),
            other => Err(self.unexpected(expected, other.label())),
        }
    }
}

impl Operation for ProjectFamilyOperation {
    type Output = ProjectedFamily;
    type Error = RecordStoreError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn is_conflict(error: &Self::Error) -> bool {
        error.is_conflict()
    }

    /// The bounded attempts the hand-written loop made before: one try and seven retries.
    fn conflict_retries() -> u32 {
        7
    }

    fn expected_error(error: &Self::Error) -> bool {
        matches!(error, RecordStoreError::UnknownAlias)
    }

    fn first(&mut self) -> Step {
        match self.config.family {
            FamilyRef::Family(_) => self.read_cache(),
            FamilyRef::Alias(job_id) => Ok(outside(StorageEffect::Iter {
                key_space: FAMILY_ALIAS_KEYSPACE.to_string(),
                prefix: Some(alias_prefix(job_id)),
                start: None,
                limit: MAX_ALIAS_FAMILIES,
                txn_id: None,
            })),
        }
    }

    fn next(&mut self, result: IoResult) -> Step {
        match self.state {
            ProjectState::ResolveAlias => match self.outside_event("alias scan", result)? {
                StorageEvent::IterResult { values, .. } => {
                    // Ordered by key, so two families claiming one alias resolve
                    // identically on every replica.
                    self.family = values.iter().filter_map(|(key, _)| alias_family(key)).min();
                    self.read_cache()
                }
                other => Err(self.unexpected("alias scan", Event::Storage(other).label())),
            },
            ProjectState::ReadCache => match self.outside_event("projection cache read", result)? {
                StorageEvent::ReadResult { value, .. } => {
                    self.cache = value
                        .as_ref()
                        .and_then(|value| ProjectionCache::decode(value));
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
                    self.page()
                }
                other => {
                    let got = Event::Storage(other).label();
                    Err(self.unexpected("projection cache read", got))
                }
            },
            ProjectState::Page => match result {
                IoResult::Page { rows, .. } => match self.keep_page(rows)? {
                    true => self.page(),
                    false => self.reduce(),
                },
                other => Err(self.unexpected("record page", other.label())),
            },
            ProjectState::ReadJobs => match result {
                IoResult::Values(values) => {
                    let writes = self.bridge(values);
                    self.write(writes)
                }
                other => Err(self.unexpected("job row read", other.label())),
            },
            ProjectState::Write => match result {
                IoResult::Done => self.settle(false),
                other => Err(self.unexpected("projection write", other.label())),
            },
        }
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
        LogicalJobState::Failed => Some(JobState::Failed),
    }
}
