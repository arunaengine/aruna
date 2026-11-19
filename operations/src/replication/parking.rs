//! Parks replication jobs whose source key is locked and wakes them when that key unlocks. A
//! parked job keeps its record, attempts and error; only its due time changes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::storage::replication_queue::BlobJobRecord;

use super::queue::{BlobQueueError, schedule_blob_drain};
use std::collections::VecDeque;
use std::time::Duration;

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::errors::ConversionError;
use aruna_core::events::{BlobEvent, Event};
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
        let read = Effect::Blob(BlobEffect::ReadKeyStatus {
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
            ParkState::KeyStatus { index } => match result.event()? {
                Event::Blob(BlobEvent::KeyStatus { generations }) => {
                    let key = self.keys.get(index).copied();
                    let unlocked =
                        (generations.iter()).any(|status| Some(status.key) == key && status.active);
                    Ok(self.status_read(index, unlocked))
                }
                // Without a blob backend no key counts as unlocked here.
                Event::Blob(_) => Ok(self.status_read(index, false)),
                other => Err(BlobQueueError::UnexpectedEvent(other.label().to_string())),
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
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use crate::jobs::key_locks::KeyUnlockedOperation;
    use crate::testing::test_context;
    use aruna_storage::StorageHandle;

    async fn next_blob_timer(storage: &StorageHandle) -> Result<Option<Duration>, BlobQueueError> {
        let context = test_context(storage.clone());
        drive(NextTimerOperation::new(), &context).await
    }

    async fn process_transitions(
        context: &crate::driver::DriverContext,
    ) -> Result<Option<Duration>, String> {
        let process = crate::blob::migration::queue::ProcessTransitionsOperation::new(
            context.node_and_realm(),
        );
        crate::driver::drive(process, context)
            .await
            .map_err(|error| error.to_string())
    }

    /// Parks through the operation at the current time, as the drain does.
    async fn park_job(
        context: &DriverContext,
        job_key: Vec<u8>,
        job: &BlobJobRecord,
        keys: &[BucketKeyRef],
    ) -> Result<Option<u64>, BlobQueueError> {
        let now_ms = aruna_core::time::unix_timestamp_millis();
        let park = ParkJobOperation::new(job_key, job.clone(), keys.to_vec(), now_ms);
        drive(park, context).await
    }

    /// Sends one storage effect and keeps only its error.
    async fn run(storage: &StorageHandle, effect: StorageEffect) -> Result<(), StorageError> {
        match storage.send_storage_effect(effect).await {
            Event::Storage(StorageEvent::Error { error }) => Err(error),
            _ => Ok(()),
        }
    }
    use crate::replication::drain::NextTimerOperation;
    use aruna_core::UserId;
    use aruna_core::compute::{SecretBytes, SharedSecret};
    use aruna_core::effects::{BlobEffect, StorageEffect};
    use aruna_core::errors::StorageError;
    use aruna_core::events::{BlobEvent, StorageEvent};
    use aruna_core::structs::identity::auth::AuthContext;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::storage::blob::BackendRef;
    use aruna_core::structs::storage::encryption::{EncryptionMode, public_key_of};
    use aruna_core::structs::storage::format::{PithosLayout, StoredFormat};
    use aruna_core::structs::storage::replication_queue::ReplicationMode;
    use aruna_core::structs::storage::replication_queue::blob_job_key;
    use aruna_core::structs::storage::replication_queue::{
        ReplicateScopeInput, ReplicateScopeTarget,
    };
    use aruna_core::types::TxnId;
    use aruna_storage::scan::iter_prefix_page;
    use std::time::SystemTime;
    use tempfile::TempDir;

    /// A node with a blob adapter, so key states are real.
    async fn keyed_context() -> (TempDir, DriverContext) {
        use aruna_core::structs::storage::blob::{Backend, BackendConfig};
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        let blob_root = format!("{root}/blobstore");
        std::fs::create_dir_all(&blob_root).unwrap();
        let storage = aruna_storage::FjallStorage::open(root).unwrap();
        let net = aruna_net::NetHandle::new(crate::testing::local_net(), storage.clone())
            .await
            .unwrap();
        let config = BackendConfig {
            backend_type: Backend::FileSystem,
            bucket_prefix: Some("aruna_".to_string()),
            max_bucket_size: Some(100_000),
            multipart_bucket: Some("multipart".to_string()),
            root: blob_root,
            service_config: std::collections::HashMap::new(),
            timeouts: Default::default(),
        };
        let blob = aruna_blob::blob::BlobHandler::new(config, storage.clone(), net.clone())
            .await
            .unwrap();
        let mut context = test_context(storage);
        context.net_handle = Some(net);
        context.blob_handle = Some(blob);
        (dir, context)
    }

    async fn unlock(context: &DriverContext, key: BucketKeyRef) {
        let private = [9u8; 32];
        let public_key = public_key_of(&SecretBytes::new(private.to_vec())).unwrap();
        let blob = context.blob_handle.as_ref().unwrap();
        let prepare = BlobEffect::PrepareKey {
            key,
            public_key,
            private_key: SharedSecret::new(SecretBytes::new(private.to_vec())),
            duration: None,
            max: None,
        };
        let Event::Blob(BlobEvent::KeyPrepared { ticket }) = blob.send_blob_effect(prepare).await
        else {
            panic!("the key is prepared")
        };
        let activated = blob
            .send_blob_effect(BlobEffect::ActivateKey { ticket })
            .await;
        assert!(matches!(
            activated,
            Event::Blob(BlobEvent::KeyActivated { .. })
        ));
    }

    fn job(relationship_id: Ulid) -> BlobJobRecord {
        let realm_id = RealmId::from_bytes([2; 32]);
        let input = ReplicateScopeInput {
            bucket: "source".to_string(),
            target: ReplicateScopeTarget::Bucket,
            target_node_id: iroh::SecretKey::from_bytes(&[3; 32]).public(),
            auth_context: AuthContext {
                user_id: UserId::nil(realm_id),
                realm_id,
                path_restrictions: None,
                session: None,
            },
            replicate_delete_markers: false,
            mode: ReplicationMode::OnDemand,
        };
        BlobJobRecord::new_relationship(input, None, relationship_id, 1_000)
    }

    async fn store(storage: &StorageHandle, job: &BlobJobRecord) -> Vec<u8> {
        let key = blob_job_key(job).unwrap().to_vec();
        let write = StorageEffect::Write {
            key_space: REPLICATION_JOB_KEYSPACE.to_string(),
            key: key.clone().into(),
            value: job.to_bytes().unwrap().into(),
            txn_id: None,
        };
        run(storage, write).await.unwrap();
        key
    }

    async fn source(context: &DriverContext, key: BucketKeyRef) -> BackendLocation {
        let settings = BucketEncryption {
            mode: EncryptionMode::VaultLocked,
            bucket_id: Some(key.bucket_id),
            key_generation: key.generation,
            ..Default::default()
        };
        let record =
            BucketKeyRecord::new(key, Ulid::from_parts(1, key.generation.into()), [9; 32], 0);
        let location = BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/tmp".to_string(),
            storage_bucket: "source".to_string(),
            backend_path: "object".to_string(),
            ulid: Ulid::from_parts(2, key.generation.into()),
            format: StoredFormat::pithos(
                PithosLayout {
                    stored_size: 99,
                    metadata_digest: [key.generation as u8; 32],
                    storage_generation: key.generation,
                },
                key,
            ),
            created_by: job(Ulid::nil()).input.auth_context.user_id,
            created_at: SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 42,
            hashes: std::collections::HashMap::from([("blake3".to_string(), vec![4; 32])]),
        };
        let version = BlobVersion::materialized(
            [4; 32],
            location.backend.clone(),
            location.format.encoding(),
            location.created_at,
            location.created_by,
            None,
        );
        for (space, row, value) in [
            (
                BUCKET_ENCRYPTION_KEYSPACE,
                b"source".to_vec(),
                settings.to_bytes().unwrap(),
            ),
            (
                BUCKET_KEY_KEYSPACE,
                key.key().to_vec(),
                record.to_bytes().unwrap(),
            ),
            (
                BLOB_LOCATIONS_KEYSPACE,
                location.location_key().unwrap().to_bytes(),
                location.to_bytes().unwrap(),
            ),
            (
                BLOB_VERSIONS_KEYSPACE,
                VersionKey::new("source", "object", Ulid::from_parts(3, 3))
                    .to_bytes()
                    .unwrap(),
                version.to_bytes().unwrap(),
            ),
        ] {
            run(
                &context.storage_handle,
                StorageEffect::Write {
                    key_space: space.to_string(),
                    key: row.into(),
                    value: value.into(),
                    txn_id: None,
                },
            )
            .await
            .unwrap();
        }
        location
    }

    #[tokio::test]
    async fn rotated_copy_retries() {
        for state in [KeyState::Retiring, KeyState::Retired] {
            let (_dir, context) = keyed_context().await;
            let storage = &context.storage_handle;
            let old = BucketKeyRef::new(Ulid::from_parts(9, 9), 1);
            let location = source(&context, old).await;
            let Event::Storage(StorageEvent::ReadResult {
                value: Some(captured),
                ..
            }) = storage
                .send_storage_effect(StorageEffect::Read {
                    key_space: BLOB_LOCATIONS_KEYSPACE.to_string(),
                    key: location.location_key().unwrap().to_bytes().into(),
                    txn_id: None,
                })
                .await
            else {
                panic!("source location not read");
            };
            let captured = BackendLocation::from_bytes(&captured).unwrap();
            source(&context, BucketKeyRef::new(old.bucket_id, 2)).await;
            let mut old_record = BucketKeyRecord::new(old, Ulid::from_parts(1, 1), [9; 32], 0);
            old_record.state = state;
            run(
                storage,
                StorageEffect::Write {
                    key_space: BUCKET_KEY_KEYSPACE.to_string(),
                    key: old.key().into(),
                    value: old_record.to_bytes().unwrap().into(),
                    txn_id: None,
                },
            )
            .await
            .unwrap();
            let admission = context
                .blob_handle
                .as_ref()
                .unwrap()
                .send_blob_effect(BlobEffect::AdmitRead {
                    key: old,
                    archive: aruna_core::structs::storage::blob::ArchiveKey::of(&captured),
                })
                .await;
            assert!(matches!(
                admission,
                Event::Blob(BlobEvent::Error(aruna_core::errors::BlobError::BucketKey(
                    aruna_core::structs::storage::encryption::BucketKeyError::Locked(_)
                )))
            ));
            let mut record = job(Ulid::from_parts(10, 10));
            record.input.target = ReplicateScopeTarget::Version {
                key: "object".to_string(),
                version_id: Ulid::from_parts(3, 3),
            };
            record.attempts = 2;
            let row = store(storage, &record).await;
            let due = park_job(&context, row.clone(), &record, &[old])
                .await
                .unwrap();
            let stored = read_job(storage, &row, None).await.unwrap().unwrap();
            assert_eq!(due, Some(stored.due_at_ms));
            assert_ne!(stored.due_at_ms, PARKED_DUE);
            assert_eq!(stored.attempts, record.attempts);
            assert_eq!(stored.last_error, record.last_error);
            assert_eq!(
                drive(
                    AwaitingJobsOperation::new(Ulid::from_parts(10, 10)),
                    &context
                )
                .await
                .unwrap(),
                0
            );
            assert!(next_blob_timer(storage).await.unwrap().is_some());
        }
    }

    #[tokio::test]
    async fn parking_fences_rotation() {
        let (_dir, storage) = crate::tests::s3::test_storage();
        let context = test_context(storage.clone());
        let old = BucketKeyRef::new(Ulid::from_parts(11, 11), 1);
        source(&context, old).await;
        let record = job(Ulid::from_parts(12, 12));
        let row = store(&storage, &record).await;
        let started = StorageEffect::StartTransaction { read: false };
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) =
            storage.send_storage_effect(started).await
        else {
            panic!("the transaction starts")
        };
        // The parking reads of the key rows join the transaction, so a rotation conflicts.
        let reads = vec![
            (
                BUCKET_ENCRYPTION_KEYSPACE.to_string(),
                record.input.bucket.as_bytes().into(),
            ),
            (BUCKET_KEY_KEYSPACE.to_string(), old.key().into()),
        ];
        let read = StorageEffect::BatchRead {
            reads,
            txn_id: Some(txn_id),
        };
        assert!(matches!(
            storage.send_storage_effect(read).await,
            Event::Storage(StorageEvent::BatchReadResult { .. })
        ));
        source(&context, BucketKeyRef::new(old.bucket_id, 2)).await;
        let mut parked = record;
        parked.due_at_ms = PARKED_DUE;
        run(
            &storage,
            StorageEffect::Write {
                key_space: REPLICATION_JOB_KEYSPACE.to_string(),
                key: row.clone().into(),
                value: parked.to_bytes().unwrap().into(),
                txn_id: Some(txn_id),
            },
        )
        .await
        .unwrap();
        assert_eq!(
            run(&storage, StorageEffect::CommitTransaction { txn_id }).await,
            Err(StorageError::TransactionConflict)
        );
        assert_ne!(
            read_job(&storage, &row, None)
                .await
                .unwrap()
                .unwrap()
                .due_at_ms,
            PARKED_DUE
        );
    }

    #[tokio::test]
    async fn parked_job_wakes() {
        // A locked source parks the job without an attempt; the unlock makes it due again.
        let dir = tempfile::tempdir().unwrap();
        let storage = aruna_storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = test_context(storage.clone());
        let relationship = Ulid::from_parts(5, 5);
        let mut record = job(relationship);
        record.attempts = 2;
        let key = store(&storage, &record).await;
        let locked = BucketKeyRef::new(Ulid::from_parts(6, 6), 1);
        source(&context, locked).await;

        let due = park_job(&context, key.clone(), &record, &[locked])
            .await
            .unwrap();
        assert_eq!(due, None);
        let parked = read_job(&storage, &key, None).await.unwrap().unwrap();
        assert_eq!(parked.due_at_ms, PARKED_DUE);
        assert_eq!(parked.attempts, 2);
        assert_eq!(parked.last_error, None);
        assert_eq!(parked.job_id, record.job_id);
        assert_eq!(
            drive(AwaitingJobsOperation::new(relationship), &context)
                .await
                .unwrap(),
            1
        );
        // The drain neither runs a parked job nor times a wake for it.
        assert_eq!(next_blob_timer(&storage).await.unwrap(), None);

        // Another key's unlock leaves it parked.
        let other = BucketKeyRef::new(locked.bucket_id, 2);
        assert_eq!(
            drive(WakeParkedOperation::new(other, 5_000), &context)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            drive(WakeParkedOperation::new(locked, 5_000), &context)
                .await
                .unwrap(),
            1
        );
        let woken = read_job(&storage, &key, None).await.unwrap().unwrap();
        assert_eq!(woken.due_at_ms, 5_000);
        assert_eq!((woken.attempts, woken.job_id), (2, record.job_id));
        assert_eq!(
            drive(AwaitingJobsOperation::new(relationship), &context)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            drive(WakeParkedOperation::new(locked, 6_000), &context)
                .await
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn plain_archive_waits() {
        let (_dir, storage) = crate::tests::s3::test_storage();
        let context = test_context(storage.clone());
        let key = BucketKeyRef::new(Ulid::from_parts(13, 13), 1);
        let mut location = source(&context, key).await;
        location.format = StoredFormat::default();
        let version = BlobVersion::materialized(
            [4; 32],
            location.backend.clone(),
            location.format.encoding(),
            location.created_at,
            location.created_by,
            None,
        );
        for (space, row, value) in [
            (
                BLOB_LOCATIONS_KEYSPACE,
                location.location_key().unwrap().to_bytes(),
                location.to_bytes().unwrap(),
            ),
            (
                BLOB_VERSIONS_KEYSPACE,
                VersionKey::new("source", "object", Ulid::from_parts(3, 3))
                    .to_bytes()
                    .unwrap(),
                version.to_bytes().unwrap(),
            ),
        ] {
            run(
                &storage,
                StorageEffect::Write {
                    key_space: space.to_string(),
                    key: row.into(),
                    value: value.into(),
                    txn_id: None,
                },
            )
            .await
            .unwrap();
        }
        let record = job(Ulid::from_parts(14, 14));
        let row = store(&storage, &record).await;
        assert_eq!(
            park_job(&context, row.clone(), &record, &[key])
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            read_job(&storage, &row, None)
                .await
                .unwrap()
                .unwrap()
                .due_at_ms,
            PARKED_DUE
        );
    }

    #[tokio::test]
    async fn unlock_before_park() {
        // The unlock ran before the wait row existed: the recheck after parking wakes the job.
        let (_dir, context) = keyed_context().await;
        let storage = context.storage_handle.clone();
        let record = job(Ulid::from_parts(7, 7));
        let key = store(&storage, &record).await;
        let unlocked = BucketKeyRef::new(Ulid::from_parts(8, 8), 1);
        source(&context, unlocked).await;
        unlock(&context, unlocked).await;
        assert_eq!(
            drive(WakeParkedOperation::new(unlocked, 1), &context)
                .await
                .unwrap(),
            0
        );

        let due = park_job(&context, key.clone(), &record, &[unlocked])
            .await
            .unwrap();

        let woken = read_job(&storage, &key, None).await.unwrap().unwrap();
        assert_eq!(due, Some(woken.due_at_ms));
        assert_ne!(woken.due_at_ms, PARKED_DUE);
        assert_eq!(
            drive(AwaitingJobsOperation::new(Ulid::from_parts(7, 7)), &context)
                .await
                .unwrap(),
            0
        );
    }

    async fn retiring_wait(mode: EncryptionMode) {
        let (_dir, context) = keyed_context().await;
        let storage = &context.storage_handle;
        let old = BucketKeyRef::new(Ulid::from_parts(15, 15), 1);
        let location = source(&context, old).await;
        let settings = BucketEncryption {
            mode,
            bucket_id: Some(old.bucket_id),
            key_generation: 2,
            ..Default::default()
        };
        let mut retiring = BucketKeyRecord::new(old, Ulid::from_parts(1, 1), [9; 32], 0);
        retiring.state = KeyState::Retiring;
        run(
            storage,
            StorageEffect::BatchWrite {
                writes: vec![
                    (
                        BUCKET_ENCRYPTION_KEYSPACE.to_string(),
                        b"source".to_vec().into(),
                        settings.to_bytes().unwrap().into(),
                    ),
                    (
                        BUCKET_KEY_KEYSPACE.to_string(),
                        old.key().into(),
                        retiring.to_bytes().unwrap().into(),
                    ),
                ],
                txn_id: None,
            },
        )
        .await
        .unwrap();
        let admission = context
            .blob_handle
            .as_ref()
            .unwrap()
            .send_blob_effect(BlobEffect::AdmitRead {
                key: old,
                archive: aruna_core::structs::storage::blob::ArchiveKey::of(&location),
            })
            .await;
        assert!(matches!(
            admission,
            Event::Blob(BlobEvent::Error(aruna_core::errors::BlobError::BucketKey(
                aruna_core::structs::storage::encryption::BucketKeyError::Locked(_)
            )))
        ));
        let mut record = job(Ulid::from_parts(16, 16));
        record.input.target = ReplicateScopeTarget::Version {
            key: "object".to_string(),
            version_id: Ulid::from_parts(3, 3),
        };
        record.attempts = 2;
        record.last_error = Some("previous copy failure".to_string());
        let row = store(storage, &record).await;
        assert_eq!(
            park_job(&context, row.clone(), &record, &[old])
                .await
                .unwrap(),
            None
        );
        let parked = read_job(storage, &row, None).await.unwrap().unwrap();
        assert_eq!(parked.due_at_ms, PARKED_DUE);
        assert_eq!(parked.attempts, record.attempts);
        assert_eq!(parked.last_error, record.last_error);
        let wait = storage
            .send_storage_effect(StorageEffect::Read {
                key_space: COPY_WAIT_KEYSPACE.to_string(),
                key: wait_row(old, &row).into(),
                txn_id: None,
            })
            .await;
        assert!(matches!(
            wait,
            Event::Storage(StorageEvent::ReadResult { value: Some(_), .. })
        ));
        assert_eq!(
            drive(
                AwaitingJobsOperation::new(record.relationship_id.unwrap()),
                &context
            )
            .await
            .unwrap(),
            1
        );
        assert_eq!(next_blob_timer(storage).await.unwrap(), None);
        assert_eq!(
            drive(
                WakeParkedOperation::new(BucketKeyRef::new(old.bucket_id, 2), 5_000),
                &context
            )
            .await
            .unwrap(),
            0
        );
        unlock(&context, old).await;
        assert_eq!(
            drive(WakeParkedOperation::new(old, 5_000), &context)
                .await
                .unwrap(),
            1
        );
        let woken = read_job(storage, &row, None).await.unwrap().unwrap();
        assert_eq!(woken.due_at_ms, 5_000);
        assert_eq!(woken.attempts, record.attempts);
        assert_eq!(woken.last_error, record.last_error);
        assert_eq!(
            drive(
                AwaitingJobsOperation::new(record.relationship_id.unwrap()),
                &context
            )
            .await
            .unwrap(),
            0
        );
        assert!(next_blob_timer(storage).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn rotation_key_waits() {
        retiring_wait(EncryptionMode::VaultLocked).await;
    }

    #[tokio::test]
    async fn decrypt_key_waits() {
        retiring_wait(EncryptionMode::Off).await;
    }

    #[tokio::test]
    async fn retirement_wakes_copy() {
        use aruna_core::keyspaces::{TRANSITION_KEYSPACE, TRANSITION_QUEUE_KEYSPACE};
        use aruna_core::structs::storage::encryption::SealPlan;
        use aruna_core::structs::storage::format::Compression;
        use aruna_core::structs::storage::transition::{
            EncryptionTransition, TransitionKind, TransitionState, TransitionTarget,
        };

        let (_dir, context) = keyed_context().await;
        let storage = &context.storage_handle;
        let old = BucketKeyRef::new(Ulid::from_parts(17, 17), 1);
        source(&context, old).await;
        unlock(&context, old).await;
        assert_eq!(
            drive(WakeParkedOperation::new(old, 1), &context)
                .await
                .unwrap(),
            0
        );
        let record = job(Ulid::from_parts(18, 18));
        let row = store(storage, &record).await;
        let (proxy, receivers) = StorageHandle::new();
        let (read_tx, read_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let direct = storage.clone();
        let actor = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let mut read_tx = Some(read_tx);
            while let Ok((effect, response, _span, _queued, _in_flight)) =
                receivers.foreground.recv()
            {
                // The recheck reads the location rows of a page in one batch, outside the
                // transaction.
                let gated = read_tx.is_some()
                    && matches!(&effect, StorageEffect::BatchRead { reads, txn_id: None }
                        if reads.iter().any(|(space, _)| space == BLOB_LOCATIONS_KEYSPACE));
                let Event::Storage(event) = runtime.block_on(direct.send_storage_effect(effect))
                else {
                    panic!("expected a storage event");
                };
                if gated {
                    assert!(matches!(
                        &event,
                        StorageEvent::BatchReadResult { values }
                            if values.iter().any(|(_, value)| value.is_some())
                    ));
                    read_tx.take().unwrap().send(()).unwrap();
                    release_rx.recv().unwrap();
                }
                assert!(response.send(event));
            }
        });
        let mut parking = context.clone();
        parking.storage_handle = proxy;
        let parked_row = row.clone();
        let parked_record = record.clone();
        let task =
            tokio::spawn(
                async move { park_job(&parking, parked_row, &parked_record, &[old]).await },
            );
        read_rx.await.unwrap();
        assert_eq!(
            read_job(storage, &row, None)
                .await
                .unwrap()
                .unwrap()
                .due_at_ms,
            PARKED_DUE
        );

        // The source read saw generation 1; retirement finishes before the registry recheck.
        let new = BucketKeyRef::new(old.bucket_id, 2);
        source(&context, new).await;
        let target = TransitionTarget {
            compression: Compression::Off,
            plan: Some(SealPlan {
                key: new,
                public_key: [9; 32],
                cipher: Default::default(),
                block_keys: Default::default(),
                storage_generation: 2,
            }),
        };
        let mut transition =
            EncryptionTransition::new(TransitionKind::Rotate, Some(old), target, 2, 1);
        transition.state = TransitionState::Cleanup;
        run(
            storage,
            StorageEffect::BatchWrite {
                writes: vec![
                    (
                        TRANSITION_KEYSPACE.to_string(),
                        b"source".to_vec().into(),
                        transition.to_bytes().unwrap().into(),
                    ),
                    (
                        TRANSITION_QUEUE_KEYSPACE.to_string(),
                        b"source".to_vec().into(),
                        Vec::new().into(),
                    ),
                ],
                txn_id: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(process_transitions(&context).await.unwrap(), None);
        assert!(
            !drive(KeyUnlockedOperation::new(old), &context)
                .await
                .unwrap()
        );
        release_tx.send(()).unwrap();
        assert_eq!(task.await.unwrap().unwrap(), None);
        actor.join().unwrap();
        let woken = read_job(storage, &row, None).await.unwrap().unwrap();
        assert_ne!(woken.due_at_ms, PARKED_DUE);
        assert_eq!(woken.attempts, record.attempts);
        assert_eq!(woken.last_error, record.last_error);
        assert_eq!(
            drive(
                AwaitingJobsOperation::new(record.relationship_id.unwrap()),
                &context
            )
            .await
            .unwrap(),
            0
        );
        assert!(next_blob_timer(storage).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn retirement_wake_recovers() {
        use aruna_core::keyspaces::{TRANSITION_KEYSPACE, TRANSITION_QUEUE_KEYSPACE};
        use aruna_core::structs::storage::encryption::SealPlan;
        use aruna_core::structs::storage::format::Compression;
        use aruna_core::structs::storage::transition::{
            EncryptionTransition, TransitionKind, TransitionState, TransitionTarget,
        };
        use aruna_core::task::TaskKey;

        struct DrainObserver(tokio::sync::mpsc::UnboundedSender<TaskKey>);

        #[async_trait::async_trait]
        impl aruna_tasks::InboundTaskHandler for DrainObserver {
            async fn handle_timer(&self, key: TaskKey) {
                self.0.send(key).unwrap();
            }
        }

        for (failed_page, job_count) in [(1, WAIT_PAGE + 1), (2, WAIT_PAGE + 1), (2, WAIT_PAGE)] {
            let dir = tempfile::tempdir().unwrap();
            let mut context = test_context(
                aruna_storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap(),
            );
            let tasks = aruna_tasks::TaskHandle::new();
            let (scheduled, mut drains) = tokio::sync::mpsc::unbounded_channel();
            tasks
                .set_inbound_handler(std::sync::Arc::new(DrainObserver(scheduled)))
                .await;
            context.task_handle = Some(tasks.clone());
            let storage = &context.storage_handle;
            let old = BucketKeyRef::new(Ulid::from_parts(19, 19), 1);
            source(&context, old).await;
            let mut jobs = Vec::new();
            let mut writes = Vec::new();
            for index in 0..job_count {
                let mut record = job(Ulid::from_parts(20, index as u128));
                record.due_at_ms = PARKED_DUE;
                record.attempts = 2;
                record.last_error = Some("previous copy failure".to_string());
                let row = blob_job_key(&record).unwrap().to_vec();
                writes.push((
                    REPLICATION_JOB_KEYSPACE.to_string(),
                    row.clone().into(),
                    record.to_bytes().unwrap().into(),
                ));
                writes.push((
                    COPY_WAIT_KEYSPACE.to_string(),
                    wait_row(old, &row).into(),
                    postcard::to_allocvec(&record.relationship_id)
                        .unwrap()
                        .into(),
                ));
                jobs.push((row, record));
            }
            let new = BucketKeyRef::new(old.bucket_id, 2);
            source(&context, new).await;
            let target = TransitionTarget {
                compression: Compression::Off,
                plan: Some(SealPlan {
                    key: new,
                    public_key: [9; 32],
                    cipher: Default::default(),
                    block_keys: Default::default(),
                    storage_generation: 2,
                }),
            };
            let mut transition =
                EncryptionTransition::new(TransitionKind::Rotate, Some(old), target, 2, 1);
            transition.state = TransitionState::Cleanup;
            writes.extend([
                (
                    TRANSITION_KEYSPACE.to_string(),
                    b"source".to_vec().into(),
                    transition.to_bytes().unwrap().into(),
                ),
                (
                    TRANSITION_QUEUE_KEYSPACE.to_string(),
                    b"source".to_vec().into(),
                    Vec::new().into(),
                ),
            ]);
            run(
                storage,
                StorageEffect::BatchWrite {
                    writes,
                    txn_id: None,
                },
            )
            .await
            .unwrap();

            let (proxy, receivers) = StorageHandle::new();
            let direct = storage.clone();
            let actor = std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                let mut page = 0;
                while let Ok((effect, response, _span, _queued, _in_flight)) =
                    receivers.foreground.recv()
                {
                    if let StorageEffect::Write {
                        key_space, value, ..
                    } = &effect
                        && key_space == TRANSITION_KEYSPACE
                        && EncryptionTransition::from_bytes(value)
                            .unwrap()
                            .finished_at_ms
                            .is_some()
                    {
                        let drain = runtime.block_on(async {
                            tokio::time::timeout(std::time::Duration::from_secs(300), drains.recv())
                                .await
                                .unwrap()
                        });
                        assert_eq!(drain, Some(TaskKey::DrainReplicationQueue));
                    }
                    let wake_scan = matches!(&effect, StorageEffect::Iter { key_space, .. }
                        if key_space == COPY_WAIT_KEYSPACE);
                    if wake_scan {
                        page += 1;
                    }
                    let event = if wake_scan && page == failed_page {
                        StorageEvent::Error {
                            error: StorageError::ReadError("injected retirement wake".to_string()),
                        }
                    } else {
                        let Event::Storage(event) =
                            runtime.block_on(direct.send_storage_effect(effect))
                        else {
                            panic!("expected a storage event")
                        };
                        event
                    };
                    assert!(response.send(event));
                }
            });
            let mut interrupted = context.clone();
            interrupted.storage_handle = proxy;
            let error = process_transitions(&interrupted).await.unwrap_err();
            assert!(error.contains("injected retirement wake"));

            let mut parked = 0;
            for (row, original) in &jobs {
                let record = read_job(storage, row, None).await.unwrap().unwrap();
                parked += usize::from(record.due_at_ms == PARKED_DUE);
                assert_eq!(record.attempts, original.attempts);
                assert_eq!(record.last_error, original.last_error);
            }
            assert_eq!(parked, job_count - (failed_page - 1) * WAIT_PAGE);
            let Event::Storage(StorageEvent::ReadResult {
                value: Some(value), ..
            }) = storage
                .send_storage_effect(StorageEffect::Read {
                    key_space: BUCKET_KEY_KEYSPACE.to_string(),
                    key: old.key().into(),
                    txn_id: None,
                })
                .await
            else {
                panic!("source key missing")
            };
            assert_eq!(
                BucketKeyRecord::from_bytes(&value).unwrap().state,
                KeyState::Retired
            );
            let Event::Storage(StorageEvent::ReadResult {
                value: Some(value), ..
            }) = storage
                .send_storage_effect(StorageEffect::Read {
                    key_space: TRANSITION_KEYSPACE.to_string(),
                    key: b"source".to_vec().into(),
                    txn_id: None,
                })
                .await
            else {
                panic!("transition missing")
            };
            let pending = EncryptionTransition::from_bytes(&value).unwrap();
            assert_eq!(pending.state, TransitionState::Finished);
            assert_eq!(pending.finished_at_ms, None);
            let queued = storage
                .send_storage_effect(StorageEffect::Read {
                    key_space: TRANSITION_QUEUE_KEYSPACE.to_string(),
                    key: b"source".to_vec().into(),
                    txn_id: None,
                })
                .await;
            assert!(matches!(
                queued,
                Event::Storage(StorageEvent::ReadResult { value: Some(_), .. })
            ));
            assert!(
                !drive(KeyUnlockedOperation::new(old), &context)
                    .await
                    .unwrap()
            );

            let mut unavailable = interrupted.clone();
            unavailable.task_handle = Some(aruna_tasks::TaskHandle::inactive());
            let error = process_transitions(&unavailable).await.unwrap_err();
            assert!(error.contains("task scheduler unavailable"));
            drop(unavailable);
            for (space, expected) in [
                (TRANSITION_KEYSPACE, pending.to_bytes().unwrap()),
                (TRANSITION_QUEUE_KEYSPACE, Vec::new()),
            ] {
                let event = storage
                    .send_storage_effect(StorageEffect::Read {
                        key_space: space.to_string(),
                        key: b"source".to_vec().into(),
                        txn_id: None,
                    })
                    .await;
                assert!(matches!(
                    event,
                    Event::Storage(StorageEvent::ReadResult { value: Some(value), .. })
                        if value.as_ref() == expected.as_slice()
                ));
            }
            assert_eq!(process_transitions(&interrupted).await.unwrap(), None);
            drop(interrupted);
            actor.join().unwrap();
            for (row, original) in &jobs {
                let record = read_job(storage, row, None).await.unwrap().unwrap();
                assert_ne!(record.due_at_ms, PARKED_DUE);
                assert_eq!(record.attempts, original.attempts);
                assert_eq!(record.last_error, original.last_error);
            }
            let (waits, _) = iter_prefix_page(
                storage,
                COPY_WAIT_KEYSPACE,
                Some(old.key().into()),
                None,
                WAIT_PAGE,
                None,
            )
            .await
            .unwrap();
            assert!(waits.is_empty());
            let queued = storage
                .send_storage_effect(StorageEffect::Read {
                    key_space: TRANSITION_QUEUE_KEYSPACE.to_string(),
                    key: b"source".to_vec().into(),
                    txn_id: None,
                })
                .await;
            assert!(matches!(
                queued,
                Event::Storage(StorageEvent::ReadResult { value: None, .. })
            ));
            let Event::Storage(StorageEvent::ReadResult {
                value: Some(value), ..
            }) = storage
                .send_storage_effect(StorageEffect::Read {
                    key_space: TRANSITION_KEYSPACE.to_string(),
                    key: b"source".to_vec().into(),
                    txn_id: None,
                })
                .await
            else {
                panic!("transition missing")
            };
            assert!(
                EncryptionTransition::from_bytes(&value)
                    .unwrap()
                    .finished_at_ms
                    .is_some()
            );
            assert!(next_blob_timer(storage).await.unwrap().is_some());
        }
    }

    async fn read_job(
        storage: &StorageHandle,
        job_key: &[u8],
        txn_id: Option<TxnId>,
    ) -> Result<Option<BlobJobRecord>, BlobQueueError> {
        let read = StorageEffect::Read {
            key_space: REPLICATION_JOB_KEYSPACE.to_string(),
            key: job_key.to_vec().into(),
            txn_id,
        };
        match storage.send_storage_effect(read).await {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => value
                .map(|value| BlobJobRecord::from_bytes(&value))
                .transpose()
                .map_err(Into::into),
            Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
            other => Err(BlobQueueError::UnexpectedEvent(other.label().to_string())),
        }
    }

    mod transitions {
        use super::*;
        use aruna_core::calls::{ChildOutcome, IoError};

        fn key() -> BucketKeyRef {
            BucketKeyRef::new(Ulid::from_parts(3, 3), 1)
        }

        fn wait() -> Vec<u8> {
            wait_row(key(), b"job")
        }

        fn parked() -> BlobJobRecord {
            let mut record = job(Ulid::from_parts(1, 1));
            record.due_at_ms = PARKED_DUE;
            record
        }

        fn job_value(record: Option<&BlobJobRecord>) -> IoResult {
            IoResult::Values(vec![record.map(|record| record.to_bytes().unwrap().into())])
        }

        fn delete_wait() -> Result<Next<bool>, BlobQueueError> {
            let row = (COPY_WAIT_KEYSPACE.to_string(), wait().into());
            Ok(Next::Call(IoCall::Delete(vec![row])))
        }

        #[test]
        fn wakes_parked_job() {
            let mut operation = WakeJobOperation::new(wait(), 7);
            assert_eq!(operation.transaction(), TransactionMode::Write);
            let parked = parked();
            let mut woken = parked.clone();
            woken.due_at_ms = 7;
            let read = IoCall::Read(vec![(
                REPLICATION_JOB_KEYSPACE.to_string(),
                b"job".to_vec().into(),
            )]);
            assert_eq!(operation.first(), Ok(Next::Call(read)));
            let write = IoCall::Write(vec![(
                REPLICATION_JOB_KEYSPACE.to_string(),
                b"job".to_vec().into(),
                woken.to_bytes().unwrap().into(),
            )]);
            assert_eq!(
                operation.next(job_value(Some(&parked))),
                Ok(Next::Call(write))
            );
            assert_eq!(operation.next(IoResult::Done), delete_wait());
            assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(true)));
        }

        #[test]
        fn stale_job_skipped() {
            let mut due = parked();
            due.due_at_ms = 3;
            for record in [None, Some(&due)] {
                let mut operation = WakeJobOperation::new(wait(), 7);
                let _ = operation.first();
                assert_eq!(operation.next(job_value(record)), delete_wait());
                assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(false)));
            }
            // A row too short to name a job is done without storage.
            let mut short = WakeJobOperation::new(vec![1, 2], 7);
            assert_eq!(short.first(), Ok(Next::Done(false)));
        }

        #[test]
        fn conflicts_retry_bounded() {
            // A wake that lost its commit race runs again, at most `WAKE_ATTEMPTS` times.
            let conflict = || {
                let lost: Result<bool, BlobQueueError> = Err(IoError::Conflict.into());
                IoResult::Child(ChildOutcome::new(lost))
            };
            let mut wake = WakeTry::new(wait(), 7);
            for _ in 1..WAKE_ATTEMPTS {
                assert!(matches!(wake.answer(conflict()), Ok(WakeAnswer::Again(_))));
            }
            let lost = BlobQueueError::from(IoError::Conflict).to_string();
            assert!(matches!(
                wake.answer(conflict()),
                Err(BlobQueueError::Wake(reason)) if reason == lost
            ));
        }

        fn waits(rows: usize) -> IoResult {
            IoResult::Page {
                rows: (0..rows)
                    .map(|index| (wait_row(key(), &[index as u8; 4]).into(), Vec::new().into()))
                    .collect(),
                next: None,
            }
        }

        fn woken(result: Result<bool, BlobQueueError>) -> IoResult {
            IoResult::Child(ChildOutcome::new(result))
        }

        fn wakes_job(step: &Result<Next<usize>, BlobQueueError>) -> bool {
            matches!(step, Ok(Next::Call(IoCall::Child(_))))
        }

        #[test]
        fn wakes_then_schedules() {
            let mut operation = WakeParkedOperation::new(key(), 5);
            assert!(matches!(
                operation.first(),
                Ok(Next::Call(IoCall::Page { .. }))
            ));
            assert!(wakes_job(&operation.next(waits(2))));
            assert!(wakes_job(&operation.next(woken(Ok(true)))));
            let drain = IoCall::Effect(Box::new(schedule_blob_drain()));
            assert_eq!(
                operation.next(woken(Ok(false))),
                Ok(Next::Then {
                    output: 1,
                    calls: vec![drain],
                })
            );
            // Nothing woken schedules nothing.
            let mut quiet = WakeParkedOperation::new(key(), 5);
            let _ = quiet.first();
            assert_eq!(quiet.next(waits(0)), Ok(Next::Done(0)));
        }

        #[test]
        fn full_page_continues() {
            let mut operation = WakeParkedOperation::new(key(), 5);
            let _ = operation.first();
            let _ = operation.next(waits(WAIT_PAGE));
            for _ in 1..WAIT_PAGE {
                let _ = operation.next(woken(Ok(false)));
            }
            let next = operation.next(woken(Ok(false)));
            assert!(matches!(
                next,
                Ok(Next::Call(IoCall::Page { after: Some(_), .. }))
            ));
        }

        #[test]
        fn parked_contract() {
            let mut failed = WakeParkedOperation::new(key(), 5);
            let _ = failed.first();
            let _ = failed.next(waits(1));
            let busy = BlobQueueError::Wake("busy".to_string());
            let reason = busy.to_string();
            assert_eq!(
                failed.next(woken(Err(busy))),
                Err(BlobQueueError::Wake(reason))
            );
            let mut reading = WakeParkedOperation::new(key(), 5);
            let _ = reading.first();
            assert!(matches!(
                reading.next(IoResult::Done),
                Err(BlobQueueError::Io(_))
            ));
        }

        #[test]
        fn counts_parked_batched() {
            let relationship = Ulid::from_parts(1, 1);
            let owner = postcard::to_allocvec(&Some(relationship)).unwrap();
            let other = postcard::to_allocvec(&Some(Ulid::from_parts(2, 2))).unwrap();
            let mut operation = AwaitingJobsOperation::new(relationship);
            let _ = operation.first();
            let page = IoResult::Page {
                rows: vec![
                    (wait_row(key(), b"one").into(), owner.clone().into()),
                    (wait_row(key(), b"two").into(), owner.into()),
                    (wait_row(key(), b"three").into(), other.into()),
                ],
                next: None,
            };
            let batch = IoCall::Read(vec![
                (REPLICATION_JOB_KEYSPACE.to_string(), b"one".to_vec().into()),
                (REPLICATION_JOB_KEYSPACE.to_string(), b"two".to_vec().into()),
            ]);
            assert_eq!(operation.next(page), Ok(Next::Call(batch)));
            let jobs = IoResult::Values(vec![Some(parked().to_bytes().unwrap().into()), None]);
            assert_eq!(operation.next(jobs), Ok(Next::Done(1)));
        }

        #[test]
        fn awaiting_contract() {
            let mut reading = AwaitingJobsOperation::new(Ulid::from_parts(1, 1));
            let _ = reading.first();
            assert!(matches!(
                reading.next(IoResult::Done),
                Err(BlobQueueError::Io(_))
            ));
            let mut empty = AwaitingJobsOperation::new(Ulid::from_parts(1, 1));
            let _ = empty.first();
            assert_eq!(empty.next(waits(0)), Ok(Next::Done(0)));
        }

        fn park(keys: Vec<BucketKeyRef>) -> ParkJobOperation {
            ParkJobOperation::new(b"job".to_vec(), job(Ulid::from_parts(1, 1)), keys, 100)
        }

        fn records_read(settings: &BucketEncryption, record: Option<BucketKeyRecord>) -> IoResult {
            IoResult::Values(vec![
                Some(settings.to_bytes().unwrap().into()),
                record.map(|record| record.to_bytes().unwrap().into()),
            ])
        }

        fn rows(keys: Vec<BucketKeyRef>) -> ParkRowsOperation {
            let job = job(Ulid::from_parts(1, 1));
            ParkRowsOperation {
                job_key: b"job".to_vec(),
                check: WaitCheck::new(&job, &keys),
                job,
                keys,
                now_ms: 100,
                parked: None,
            }
        }

        fn rows_answer(result: Result<(bool, u64), BlobQueueError>) -> IoResult {
            IoResult::Child(ChildOutcome::new(result))
        }

        #[test]
        fn stale_key_polls() {
            // A key that is no longer the bucket's key leaves the job due after the poll delay.
            let mut operation = rows(vec![key()]);
            assert_eq!(operation.transaction(), TransactionMode::Write);
            assert!(matches!(operation.first(), Ok(Next::Call(IoCall::Read(_)))));
            let written = operation.next(records_read(&BucketEncryption::default(), None));
            let Ok(Next::Call(IoCall::Write(writes))) = &written else {
                panic!("expected the job write, got {written:?}");
            };
            assert_eq!(writes.len(), 1, "no wait row for a stale key");
            let due = 100 + REPLICATION_POLL_AFTER.as_millis() as u64;
            assert_eq!(operation.next(IoResult::Done), Ok(Next::Done((false, due))));
            // The parent ends with that due time, without a recheck.
            let mut park = park(vec![key()]);
            assert!(matches!(park.first(), Ok(Next::Call(IoCall::Child(_)))));
            assert_eq!(
                park.next(rows_answer(Ok((false, due)))),
                Ok(Next::Done(Some(due)))
            );
        }

        #[test]
        fn park_errors_fail() {
            let mut park = park(vec![key()]);
            let _ = park.first();
            let timeout = BlobQueueError::Storage(StorageError::Timeout);
            assert_eq!(
                park.next(rows_answer(Err(timeout))),
                Err(BlobQueueError::Storage(StorageError::Timeout))
            );
            // A waiting job is checked again after its commit.
            let mut waiting = self::park(vec![key()]);
            let _ = waiting.first();
            let recheck = waiting.next(rows_answer(Ok((true, PARKED_DUE))));
            assert!(matches!(recheck, Ok(Next::Call(IoCall::Read(_)))));
        }

        #[test]
        fn locked_keys_stay() {
            // After the commit each key's status is read; no unlocked key leaves the job parked.
            let blob = |event| IoResult::Event(Box::new(Event::Blob(event)));
            let mut operation = park(vec![key()]);
            operation.state = ParkState::KeyStatus { index: 0 };
            let locked = BlobEvent::KeyStatus {
                generations: Vec::new(),
            };
            assert_eq!(operation.next(blob(locked)), Ok(Next::Done(None)));
            let mut unlocked = park(vec![key()]);
            unlocked.state = ParkState::KeyStatus { index: 0 };
            let missing = BlobEvent::Error(aruna_core::errors::BlobError::HandleMissing);
            assert_eq!(unlocked.next(blob(missing)), Ok(Next::Done(None)));
        }
    }
}
