//! Parks replication jobs whose source key is locked and wakes them when that key unlocks. A
//! parked job keeps its record, attempts and error; only its due time changes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::storage::replication_queue::BlobJobRecord;

use super::queue::BlobQueueError;
use aruna_core::structs::storage::replication_queue::schedule_blob_drain;
use std::collections::VecDeque;
use std::time::Duration;

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::errors::ConversionError;
use aruna_core::events::BlobEvent;
#[cfg(test)]
use aruna_core::events::Event;
use aruna_core::keyspaces::{
    BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, BUCKET_ENCRYPTION_KEYSPACE,
    BUCKET_KEY_KEYSPACE, COPY_WAIT_KEYSPACE, PENDING_LOCATION_KEYSPACE, REPLICATION_JOB_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::storage::blob::{BackendLocation, BlobVersion, VersionKey};
use aruna_core::structs::storage::encryption::{
    BucketEncryption, BucketKeyRecord, BucketKeyRef, KeyState,
};
use aruna_core::structs::storage::replication_queue::ReplicateScopeTarget;
use aruna_core::types::{Key, Value};
use std::collections::BTreeSet;
use ulid::Ulid;

/// How long a job that no longer waits for a key, and an idle drain, wait before the next look.
const REPLICATION_POLL_AFTER: Duration = Duration::from_secs(5);
/// Due time of a parked job: the drain never runs it and never times a wake for it.
pub(crate) const PARKED_DUE: u64 = u64::MAX;
const WAIT_PAGE: usize = 256;
/// Commit conflicts a wake retries before it reports an error.
const WAKE_ATTEMPTS: usize = 3;
/// Length of a key reference at the start of each wait row.
const REF_LEN: usize = 24;

fn wait_row(key: BucketKeyRef, job_key: &[u8]) -> Vec<u8> {
    [&key.key()[..], job_key].concat()
}

/// Checks that a job still waits for `keys`: each key is the bucket's active or retiring key,
/// and every one of them seals a version the job copies. Inside a transaction it reads one view.
#[derive(Debug, PartialEq)]
struct WaitCheck {
    keys: Vec<BucketKeyRef>,
    bucket: String,
    target: ReplicateScopeTarget,
    phase: CheckPhase,
    settings: Option<BucketEncryption>,
    prefix: Vec<u8>,
    after: Option<Key>,
    found: BTreeSet<BucketKeyRef>,
}

/// The read a wait check waits for.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CheckPhase {
    Records,
    Versions,
    Locations,
}

/// What one step of a wait check asks for next.
enum CheckStep {
    Read(IoCall),
    Done(bool),
}

impl WaitCheck {
    fn new(job: &BlobJobRecord, keys: &[BucketKeyRef]) -> Self {
        Self {
            keys: keys.to_vec(),
            bucket: job.input.bucket.clone(),
            target: job.input.target.clone(),
            phase: CheckPhase::Records,
            settings: None,
            prefix: Vec::new(),
            after: None,
            found: BTreeSet::new(),
        }
    }

    fn start(&self) -> IoCall {
        let mut reads = vec![(
            BUCKET_ENCRYPTION_KEYSPACE.to_string(),
            self.bucket.as_bytes().into(),
        )];
        reads.extend(
            (self.keys.iter()).map(|key| (BUCKET_KEY_KEYSPACE.to_string(), key.key().into())),
        );
        IoCall::Read(reads)
    }

    fn step(&mut self, result: IoResult) -> Result<CheckStep, BlobQueueError> {
        match self.phase {
            CheckPhase::Records => self.records_read(result.values()?),
            CheckPhase::Versions => {
                let (values, next) = result.page()?;
                self.versions_read(values, next)
            }
            CheckPhase::Locations => self.locations_read(result.values()?),
        }
    }

    fn records_read(&mut self, values: Vec<Option<Value>>) -> Result<CheckStep, BlobQueueError> {
        if values.len() != self.keys.len() + 1 {
            return Err(BlobQueueError::UnexpectedEvent(
                "short key record read".to_string(),
            ));
        }
        let mut values = values.into_iter();
        let settings = BucketEncryption::from_row(values.next().flatten().as_deref())?;
        for (key, value) in self.keys.iter().zip(values) {
            let record = value
                .map(|value| BucketKeyRecord::from_bytes(&value))
                .transpose()?;
            let current = record.is_some_and(|record| {
                record.key == *key
                    && match record.state {
                        KeyState::Active => settings.active_key() == Some(*key),
                        KeyState::Retiring => settings.bucket_id == Some(key.bucket_id),
                        KeyState::Retired => false,
                    }
            });
            if !current {
                return Ok(CheckStep::Done(false));
            }
        }
        self.settings = Some(settings);
        self.prefix = match &self.target {
            ReplicateScopeTarget::Version { key, version_id } => {
                VersionKey::new(&self.bucket, key, *version_id).to_bytes()?
            }
            ReplicateScopeTarget::Object { key } => VersionKey::object_prefix(&self.bucket, key)?,
            ReplicateScopeTarget::Bucket | ReplicateScopeTarget::Prefix(_) => {
                VersionKey::bucket_prefix(&self.bucket)?
            }
        };
        Ok(self.read_versions(None))
    }

    fn read_versions(&mut self, after: Option<Key>) -> CheckStep {
        self.phase = CheckPhase::Versions;
        CheckStep::Read(IoCall::Page {
            space: BLOB_VERSIONS_KEYSPACE.to_string(),
            prefix: self.prefix.clone().into(),
            after,
            limit: WAIT_PAGE,
        })
    }

    /// The location rows of one page are read in one batch, in the same read view.
    fn versions_read(
        &mut self,
        values: Vec<(Key, Value)>,
        next: Option<Key>,
    ) -> Result<CheckStep, BlobQueueError> {
        let mut reads = Vec::new();
        for (row, value) in values {
            let version_key = VersionKey::from_bytes(&row)?;
            if matches!(&self.target, ReplicateScopeTarget::Prefix(prefix) if !version_key.key.starts_with(prefix))
            {
                continue;
            }
            let version = BlobVersion::from_bytes(&value)?;
            match (version.location_key(), version.state.pending_archive()) {
                (Some(location), _) => reads.push((
                    BLOB_LOCATIONS_KEYSPACE.to_string(),
                    location.to_bytes().into(),
                )),
                (_, Some(archive)) => reads.push((
                    PENDING_LOCATION_KEYSPACE.to_string(),
                    archive.to_bytes().into(),
                )),
                _ => {}
            }
        }
        self.after = next;
        if reads.is_empty() {
            return Ok(self.page_done());
        }
        self.phase = CheckPhase::Locations;
        Ok(CheckStep::Read(IoCall::Read(reads)))
    }

    fn locations_read(&mut self, values: Vec<Option<Value>>) -> Result<CheckStep, BlobQueueError> {
        let active = self
            .settings
            .as_ref()
            .and_then(BucketEncryption::active_key);
        for value in values.into_iter().flatten() {
            if let Some(key) = BackendLocation::from_bytes(&value)?
                .format
                .bucket_key()
                .or(active)
            {
                self.found.insert(key);
            }
        }
        Ok(self.page_done())
    }

    fn page_done(&mut self) -> CheckStep {
        if self.keys.iter().all(|key| self.found.contains(key)) {
            return CheckStep::Done(true);
        }
        match self.after.take() {
            Some(after) => self.read_versions(Some(after)),
            None => CheckStep::Done(false),
        }
    }
}

/// Writes the parked job, and its wait rows while it still waits, in one transaction after a
/// wait check in the same view. Answers whether it waits and its new due time.
#[derive(Debug, PartialEq)]
struct ParkRowsOperation {
    job_key: Vec<u8>,
    job: BlobJobRecord,
    keys: Vec<BucketKeyRef>,
    now_ms: u64,
    check: WaitCheck,
    /// Whether the job still waits and its due time, once the rows were asked to be written.
    parked: Option<(bool, u64)>,
}

impl ParkRowsOperation {
    fn write_job(&mut self, current: bool) -> Result<Next<(bool, u64)>, BlobQueueError> {
        let mut parked = self.job.clone();
        parked.due_at_ms = if current {
            PARKED_DUE
        } else {
            (self.now_ms).saturating_add(REPLICATION_POLL_AFTER.as_millis() as u64)
        };
        let relationship =
            postcard::to_allocvec(&self.job.relationship_id).map_err(ConversionError::from)?;
        let mut writes: Vec<(String, Key, Value)> = vec![(
            REPLICATION_JOB_KEYSPACE.to_string(),
            self.job_key.clone().into(),
            parked.to_bytes()?.into(),
        )];
        for key in self.keys.iter().filter(|_| current) {
            writes.push((
                COPY_WAIT_KEYSPACE.to_string(),
                wait_row(*key, &self.job_key).into(),
                relationship.clone().into(),
            ));
        }
        self.parked = Some((current, parked.due_at_ms));
        Ok(Next::Call(IoCall::Write(writes)))
    }
}

impl Operation for ParkRowsOperation {
    type Output = (bool, u64);
    type Error = BlobQueueError;

    fn first(&mut self) -> Result<Next<(bool, u64)>, BlobQueueError> {
        Ok(Next::Call(self.check.start()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<(bool, u64)>, BlobQueueError> {
        if let Some(parked) = self.parked {
            result.done()?;
            return Ok(Next::Done(parked));
        }
        match self.check.step(result)? {
            CheckStep::Read(call) => Ok(Next::Call(call)),
            CheckStep::Done(current) => self.write_job(current),
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

#[derive(Debug, PartialEq)]
enum ParkState {
    Park,
    Recheck(WaitCheck),
    KeyStatus { index: usize },
    Wake(WakeTry),
}

type ParkStep = Result<Next<Option<u64>>, BlobQueueError>;

/// Parks `job` until one of `keys` unlocks: the job and its wait rows commit together, with no
/// attempt counted and no error recorded. A key unlocked meanwhile makes it due at once, and
/// that due time is the output.
#[derive(Debug, PartialEq)]
pub(crate) struct ParkJobOperation {
    job_key: Vec<u8>,
    job: BlobJobRecord,
    keys: Vec<BucketKeyRef>,
    now_ms: u64,
    state: ParkState,
}

impl ParkJobOperation {
    pub(crate) fn new(
        job_key: Vec<u8>,
        job: BlobJobRecord,
        keys: Vec<BucketKeyRef>,
        now_ms: u64,
    ) -> Self {
        Self {
            job_key,
            job,
            keys,
            now_ms,
            state: ParkState::Park,
        }
    }

    /// The rows commit as their own run; the recheck after it reads outside any transaction.
    fn parked(&mut self, (current, due): (bool, u64)) -> ParkStep {
        if !current {
            return Ok(Next::Done(Some(due)));
        }
        let check = WaitCheck::new(&self.job, &self.keys);
        let read = check.start();
        self.state = ParkState::Recheck(check);
        Ok(Next::Call(read))
    }

    /// A key unlocked while the job parked wakes it here; no unlock call would.
    fn after_recheck(&mut self, current: bool) -> Next<Option<u64>> {
        match self.keys.first() {
            Some(key) if !current => self.wake(*key),
            Some(_) => self.key_status(0),
            None => Next::Done(None),
        }
    }

    fn key_status(&mut self, index: usize) -> Next<Option<u64>> {
        let Some(key) = self.keys.get(index) else {
            return Next::Done(None);
        };
        self.state = ParkState::KeyStatus { index };
        let read = Effect::blob(BlobEffect::ReadKeyStatus {
            bucket_id: key.bucket_id,
        });
        Next::Call(IoCall::Effect(Box::new(read)))
    }

    fn status_read(&mut self, index: usize, unlocked: bool) -> Next<Option<u64>> {
        match self.keys.get(index) {
            Some(key) if unlocked => self.wake(*key),
            _ => self.key_status(index + 1),
        }
    }

    fn wake(&mut self, key: BucketKeyRef) -> Next<Option<u64>> {
        let wake = WakeTry::new(wait_row(key, &self.job_key), self.now_ms);
        let call = wake.call();
        self.state = ParkState::Wake(wake);
        Next::Call(call)
    }
}

impl Operation for ParkJobOperation {
    type Output = Option<u64>;
    type Error = BlobQueueError;

    fn first(&mut self) -> ParkStep {
        let rows = ParkRowsOperation {
            job_key: self.job_key.clone(),
            job: self.job.clone(),
            keys: self.keys.clone(),
            now_ms: self.now_ms,
            check: WaitCheck::new(&self.job, &self.keys),
            parked: None,
        };
        Ok(Next::Call(IoCall::child(rows)))
    }

    fn next(&mut self, result: IoResult) -> ParkStep {
        match std::mem::replace(&mut self.state, ParkState::Park) {
            ParkState::Park => self.parked(result.child::<(bool, u64), BlobQueueError>()??),
            ParkState::Recheck(mut check) => match check.step(result)? {
                CheckStep::Read(call) => {
                    self.state = ParkState::Recheck(check);
                    Ok(Next::Call(call))
                }
                CheckStep::Done(current) => Ok(self.after_recheck(current)),
            },
            ParkState::KeyStatus { index } => match result.event()?.into_blob() {
                Ok(BlobEvent::KeyStatus { generations }) => {
                    let key = self.keys.get(index).copied();
                    let unlocked =
                        (generations.iter()).any(|status| Some(status.key) == key && status.active);
                    Ok(self.status_read(index, unlocked))
                }
                // Without a blob backend no key counts as unlocked here.
                Ok(_) => Ok(self.status_read(index, false)),
                Err(other) => Err(BlobQueueError::UnexpectedEvent(other.label().to_string())),
            },
            ParkState::Wake(mut wake) => match wake.answer(result)? {
                WakeAnswer::Woke(_) => Ok(Next::Done(Some(self.now_ms))),
                WakeAnswer::Again(call) => {
                    self.state = ParkState::Wake(wake);
                    Ok(Next::Call(call))
                }
            },
        }
    }
}

/// Makes the job of one wait row due at `now_ms` and drops the row, in one transaction. A job
/// that is gone or already due only loses the row. True when a parked job woke.
#[derive(Debug, PartialEq)]
pub(crate) struct WakeJobOperation {
    wait: Vec<u8>,
    job_key: Vec<u8>,
    now_ms: u64,
    /// Whether the job woke, once the job row was read.
    woke: Option<bool>,
    /// Whether the wait row delete was asked for.
    deleting: bool,
}

type WakeStep = Result<Next<bool>, BlobQueueError>;

impl WakeJobOperation {
    pub(crate) fn new(wait: Vec<u8>, now_ms: u64) -> Self {
        let job_key = wait.get(REF_LEN..).map(<[u8]>::to_vec).unwrap_or_default();
        Self {
            wait,
            job_key,
            now_ms,
            woke: None,
            deleting: false,
        }
    }

    fn job_read(&mut self, value: Option<Value>) -> WakeStep {
        let job = value
            .map(|value| BlobJobRecord::from_bytes(&value))
            .transpose()?;
        let Some(mut job) = job.filter(|job| job.due_at_ms == PARKED_DUE) else {
            self.woke = Some(false);
            return Ok(self.delete_wait());
        };
        self.woke = Some(true);
        job.due_at_ms = self.now_ms;
        let row = (
            REPLICATION_JOB_KEYSPACE.to_string(),
            self.job_key.clone().into(),
            job.to_bytes()?.into(),
        );
        Ok(Next::Call(IoCall::Write(vec![row])))
    }

    fn delete_wait(&mut self) -> Next<bool> {
        self.deleting = true;
        let row = (COPY_WAIT_KEYSPACE.to_string(), self.wait.clone().into());
        Next::Call(IoCall::Delete(vec![row]))
    }
}

impl Operation for WakeJobOperation {
    type Output = bool;
    type Error = BlobQueueError;

    fn first(&mut self) -> WakeStep {
        if self.job_key.is_empty() {
            return Ok(Next::Done(false));
        }
        let read = (
            REPLICATION_JOB_KEYSPACE.to_string(),
            self.job_key.clone().into(),
        );
        Ok(Next::Call(IoCall::Read(vec![read])))
    }

    fn next(&mut self, result: IoResult) -> WakeStep {
        match (self.woke, self.deleting) {
            (None, _) => self.job_read(result.value()?),
            (Some(_), false) => {
                result.done()?;
                Ok(self.delete_wait())
            }
            (Some(woke), true) => {
                result.done()?;
                Ok(Next::Done(woke))
            }
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

/// One wake of a wait row as a child, run again after a lost commit race, at most
/// `WAKE_ATTEMPTS` times in all.
#[derive(Debug, PartialEq)]
struct WakeTry {
    row: Vec<u8>,
    now_ms: u64,
    attempt: usize,
}

/// What a wake answer leads to.
enum WakeAnswer {
    Woke(bool),
    Again(IoCall),
}

impl WakeTry {
    fn new(row: Vec<u8>, now_ms: u64) -> Self {
        Self {
            row,
            now_ms,
            attempt: 1,
        }
    }

    fn call(&self) -> IoCall {
        IoCall::child(WakeJobOperation::new(self.row.clone(), self.now_ms))
    }

    fn answer(&mut self, result: IoResult) -> Result<WakeAnswer, BlobQueueError> {
        match result.child::<bool, BlobQueueError>()? {
            Ok(woke) => Ok(WakeAnswer::Woke(woke)),
            Err(error) if WakeJobOperation::is_conflict(&error) && self.attempt < WAKE_ATTEMPTS => {
                self.attempt += 1;
                Ok(WakeAnswer::Again(self.call()))
            }
            Err(error) => Err(BlobQueueError::Wake(error.to_string())),
        }
    }
}

/// Makes every job parked for `key` due now and schedules a drain once one woke. Answers how
/// many woke.
#[derive(Debug, PartialEq)]
pub struct WakeParkedOperation {
    key: BucketKeyRef,
    now_ms: u64,
    rows: VecDeque<Vec<u8>>,
    after: Option<Key>,
    woken: usize,
    /// The wake that runs; otherwise a page of wait rows is read.
    waking: Option<WakeTry>,
}

type CountStep = Result<Next<usize>, BlobQueueError>;

impl WakeParkedOperation {
    pub fn new(key: BucketKeyRef, now_ms: u64) -> Self {
        Self {
            key,
            now_ms,
            rows: VecDeque::new(),
            after: None,
            woken: 0,
            waking: None,
        }
    }

    fn read_waits(&mut self, after: Option<Key>) -> Next<usize> {
        self.waking = None;
        Next::Call(IoCall::Page {
            space: COPY_WAIT_KEYSPACE.to_string(),
            prefix: self.key.key().into(),
            after,
            limit: WAIT_PAGE,
        })
    }

    fn waits_read(&mut self, values: Vec<(Key, Value)>) -> Next<usize> {
        // A full page may have more rows behind it; a short one is the last.
        self.after = (values.len() == WAIT_PAGE)
            .then(|| values.last().map(|(row, _)| row.clone()))
            .flatten();
        self.rows = values.into_iter().map(|(row, _)| row.to_vec()).collect();
        self.wake_next()
    }

    /// The drain runs the woken jobs; a failed schedule is logged by the runner.
    fn wake_next(&mut self) -> Next<usize> {
        let Some(row) = self.rows.pop_front() else {
            return match self.after.take() {
                Some(after) => self.read_waits(Some(after)),
                None if self.woken > 0 => Next::Then {
                    output: self.woken,
                    calls: vec![IoCall::Effect(Box::new(schedule_blob_drain()))],
                },
                None => Next::Done(0),
            };
        };
        let wake = WakeTry::new(row, self.now_ms);
        let call = wake.call();
        self.waking = Some(wake);
        Next::Call(call)
    }
}

impl Operation for WakeParkedOperation {
    type Output = usize;
    type Error = BlobQueueError;

    fn first(&mut self) -> CountStep {
        Ok(self.read_waits(None))
    }

    fn next(&mut self, result: IoResult) -> CountStep {
        let Some(mut wake) = self.waking.take() else {
            let (values, _) = result.page()?;
            return Ok(self.waits_read(values));
        };
        match wake.answer(result)? {
            WakeAnswer::Woke(woke) => {
                self.woken += usize::from(woke);
                Ok(self.wake_next())
            }
            WakeAnswer::Again(call) => {
                self.waking = Some(wake);
                Ok(Next::Call(call))
            }
        }
    }
}

/// Copy jobs of a relationship that wait for a source key now.
#[derive(Debug, PartialEq)]
pub struct AwaitingJobsOperation {
    relationship_id: Ulid,
    jobs: BTreeSet<Vec<u8>>,
    /// Whether the job rows were asked for; otherwise a page of wait rows is read.
    reading_jobs: bool,
}

impl AwaitingJobsOperation {
    pub fn new(relationship_id: Ulid) -> Self {
        Self {
            relationship_id,
            jobs: BTreeSet::new(),
            reading_jobs: false,
        }
    }

    fn read_waits(&self, after: Option<Key>) -> Next<usize> {
        Next::Call(IoCall::Page {
            space: COPY_WAIT_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after,
            limit: WAIT_PAGE,
        })
    }

    fn waits_read(&mut self, values: Vec<(Key, Value)>) -> CountStep {
        let after = (values.len() == WAIT_PAGE)
            .then(|| values.last().map(|(row, _)| row.clone()))
            .flatten();
        for (row, value) in &values {
            let owner: Option<Ulid> = postcard::from_bytes(value).map_err(ConversionError::from)?;
            if owner == Some(self.relationship_id) && row.len() > REF_LEN {
                self.jobs.insert(row[REF_LEN..].to_vec());
            }
        }
        if after.is_some() {
            return Ok(self.read_waits(after));
        }
        if self.jobs.is_empty() {
            return Ok(Next::Done(0));
        }
        // All jobs of the relationship are read in one batch instead of one read each.
        self.reading_jobs = true;
        let reads = (self.jobs.iter())
            .map(|job_key| (REPLICATION_JOB_KEYSPACE.to_string(), job_key.clone().into()))
            .collect();
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn jobs_read(&self, values: Vec<Option<Value>>) -> CountStep {
        let mut parked = 0;
        for value in values {
            let job = value
                .map(|value| BlobJobRecord::from_bytes(&value))
                .transpose()?;
            parked += usize::from(job.is_some_and(|job| job.due_at_ms == PARKED_DUE));
        }
        Ok(Next::Done(parked))
    }
}

impl Operation for AwaitingJobsOperation {
    type Output = usize;
    type Error = BlobQueueError;

    fn first(&mut self) -> CountStep {
        Ok(self.read_waits(None))
    }

    fn next(&mut self, result: IoResult) -> CountStep {
        match self.reading_jobs {
            true => self.jobs_read(result.values()?),
            false => self.waits_read(result.page()?.0),
        }
    }
}

#[cfg(test)]
#[path = "parking_tests.rs"]
mod tests;
