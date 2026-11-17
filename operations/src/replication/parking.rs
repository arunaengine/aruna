//! Parks replication jobs whose source key is locked and wakes them when that key unlocks. A
//! parked job keeps its record, attempts and error; only its due time changes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::storage::replication_queue::BlobJobRecord;

use super::queue::{BlobQueueError, schedule_blob_drain};
use std::collections::VecDeque;
use std::time::Duration;

use aruna_core::effects::{BlobEffect, Effect, IterStart, StorageEffect, abort_effects};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::{
    BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, BUCKET_ENCRYPTION_KEYSPACE,
    BUCKET_KEY_KEYSPACE, COPY_WAIT_KEYSPACE, PENDING_LOCATION_KEYSPACE, REPLICATION_JOB_KEYSPACE,
};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::storage::blob::{BackendLocation, BlobVersion, VersionKey};
use aruna_core::structs::storage::encryption::{
    BucketEncryption, BucketKeyRecord, BucketKeyRef, KeyState,
};
use aruna_core::structs::storage::replication_queue::ReplicateScopeTarget;
use aruna_core::task::TaskEvent;
use aruna_core::types::{Effects, Key, TxnId, Value};
use smallvec::smallvec;
use std::collections::BTreeSet;
use tracing::warn;
use ulid::Ulid;

/// How long a job that no longer waits for a key, and an idle drain, wait before the next look.
pub const REPLICATION_POLL_AFTER: Duration = Duration::from_secs(5);
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

/// Checks inside one read view that a job still waits for `keys`: each key is the bucket's active
/// or retiring key, and every one of them seals a version the job copies.
#[derive(Debug, PartialEq)]
struct WaitCheck {
    keys: Vec<BucketKeyRef>,
    bucket: String,
    target: ReplicateScopeTarget,
    txn_id: Option<TxnId>,
    settings: Option<BucketEncryption>,
    prefix: Vec<u8>,
    after: Option<Key>,
    found: BTreeSet<BucketKeyRef>,
}

/// What one step of a wait check asks for next.
enum CheckStep {
    Read(Effect),
    Done(bool),
}

impl WaitCheck {
    fn new(job: &BlobJobRecord, keys: &[BucketKeyRef], txn_id: Option<TxnId>) -> Self {
        Self {
            keys: keys.to_vec(),
            bucket: job.input.bucket.clone(),
            target: job.input.target.clone(),
            txn_id,
            settings: None,
            prefix: Vec::new(),
            after: None,
            found: BTreeSet::new(),
        }
    }

    fn start(&self) -> Effect {
        let mut reads = vec![(
            BUCKET_ENCRYPTION_KEYSPACE.to_string(),
            self.bucket.as_bytes().into(),
        )];
        reads.extend(
            (self.keys.iter()).map(|key| (BUCKET_KEY_KEYSPACE.to_string(), key.key().into())),
        );
        Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: self.txn_id,
        })
    }

    fn step(&mut self, event: Event) -> Result<CheckStep, BlobQueueError> {
        match event {
            Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
            Event::Storage(StorageEvent::BatchReadResult { values }) if self.settings.is_none() => {
                self.records_read(values)
            }
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) if self.settings.is_some() => self.versions_read(values, next_start_after),
            Event::Storage(StorageEvent::BatchReadResult { values }) => self.locations_read(values),
            other => Err(BlobQueueError::UnexpectedEvent(other.label().to_string())),
        }
    }

    fn records_read(
        &mut self,
        values: Vec<(Key, Option<Value>)>,
    ) -> Result<CheckStep, BlobQueueError> {
        if values.len() != self.keys.len() + 1 {
            return Err(BlobQueueError::UnexpectedEvent(
                "short key record read".to_string(),
            ));
        }
        let mut values = values.into_iter().map(|(_, value)| value);
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

    fn read_versions(&self, start: Option<Key>) -> CheckStep {
        CheckStep::Read(Effect::Storage(StorageEffect::Iter {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            prefix: Some(self.prefix.clone().into()),
            start: start.map(IterStart::After),
            limit: WAIT_PAGE,
            txn_id: self.txn_id,
        }))
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
        Ok(CheckStep::Read(Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: self.txn_id,
        })))
    }

    fn locations_read(
        &mut self,
        values: Vec<(Key, Option<Value>)>,
    ) -> Result<CheckStep, BlobQueueError> {
        let active = self
            .settings
            .as_ref()
            .and_then(BucketEncryption::active_key);
        for value in values.into_iter().filter_map(|(_, value)| value) {
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

#[derive(Debug, PartialEq)]
enum ParkState {
    Init,
    StartTransaction,
    Check(WaitCheck),
    WriteJob { current: bool },
    Commit { current: bool },
    Recheck(WaitCheck),
    KeyStatus { index: usize },
    Wake,
    Finish,
    Error,
}

/// Parks `job` until one of `keys` unlocks: the job and its wait rows commit together, with no
/// attempt counted and no error recorded. A key unlocked meanwhile makes it due at once, and
/// that due time is the output.
#[derive(Debug, PartialEq)]
pub(crate) struct ParkJobOperation {
    job_key: Vec<u8>,
    job: BlobJobRecord,
    keys: Vec<BucketKeyRef>,
    now_ms: u64,
    txn_id: Option<TxnId>,
    due: Option<u64>,
    state: ParkState,
    error: Option<BlobQueueError>,
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
            txn_id: None,
            due: None,
            state: ParkState::Init,
            error: None,
        }
    }

    fn fail(&mut self, error: BlobQueueError) -> Effects {
        self.state = ParkState::Error;
        self.error = Some(error);
        abort_effects(&mut self.txn_id)
    }

    fn finish(&mut self, due: Option<u64>) -> Effects {
        self.due = due;
        self.state = ParkState::Finish;
        smallvec![]
    }

    fn check_step(&mut self, event: Event) -> Effects {
        let (mut check, first) = match std::mem::replace(&mut self.state, ParkState::Error) {
            ParkState::Check(check) => (check, true),
            ParkState::Recheck(check) => (check, false),
            _ => return self.fail(BlobQueueError::UnexpectedEvent("no wait check".into())),
        };
        match check.step(event) {
            Err(error) => self.fail(error),
            Ok(CheckStep::Read(effect)) => {
                self.state = if first {
                    ParkState::Check(check)
                } else {
                    ParkState::Recheck(check)
                };
                smallvec![effect]
            }
            Ok(CheckStep::Done(current)) if first => self.write_job(current),
            Ok(CheckStep::Done(current)) => self.after_recheck(current),
        }
    }

    fn write_job(&mut self, current: bool) -> Effects {
        let mut parked = self.job.clone();
        parked.due_at_ms = if current {
            PARKED_DUE
        } else {
            (self.now_ms).saturating_add(REPLICATION_POLL_AFTER.as_millis() as u64)
        };
        let relationship = match postcard::to_allocvec(&self.job.relationship_id) {
            Ok(relationship) => relationship,
            Err(error) => return self.fail(ConversionError::from(error).into()),
        };
        let value = match parked.to_bytes() {
            Ok(value) => value,
            Err(error) => return self.fail(error.into()),
        };
        let mut writes: Vec<(String, Key, Value)> = vec![(
            REPLICATION_JOB_KEYSPACE.to_string(),
            self.job_key.clone().into(),
            value.into(),
        )];
        for key in self.keys.iter().filter(|_| current) {
            writes.push((
                COPY_WAIT_KEYSPACE.to_string(),
                wait_row(*key, &self.job_key).into(),
                relationship.clone().into(),
            ));
        }
        self.due = Some(parked.due_at_ms);
        self.state = ParkState::WriteJob { current };
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: self.txn_id,
        })]
    }

    /// A key unlocked while the job parked wakes it here; no unlock call would.
    fn after_recheck(&mut self, current: bool) -> Effects {
        match self.keys.first() {
            Some(key) if !current => self.wake(*key),
            Some(_) => self.key_status(0),
            None => self.finish(None),
        }
    }

    fn key_status(&mut self, index: usize) -> Effects {
        let Some(key) = self.keys.get(index) else {
            return self.finish(None);
        };
        self.state = ParkState::KeyStatus { index };
        smallvec![Effect::Blob(BlobEffect::ReadKeyStatus {
            bucket_id: key.bucket_id,
        })]
    }

    fn status_read(&mut self, index: usize, unlocked: bool) -> Effects {
        match self.keys.get(index) {
            Some(key) if unlocked => self.wake(*key),
            _ => self.key_status(index + 1),
        }
    }

    fn wake(&mut self, key: BucketKeyRef) -> Effects {
        self.state = ParkState::Wake;
        let wake = WakeJobOperation::new(wait_row(key, &self.job_key), self.now_ms);
        smallvec![Effect::SubOperation(boxed_suboperation(wake, |result| {
            Event::SubOperation(SubOperationEvent::CopyWoken {
                result: result.map_err(|error| error.to_string()),
            })
        }))]
    }
}

impl Operation for ParkJobOperation {
    type Output = Option<u64>;
    type Error = BlobQueueError;

    fn start(&mut self) -> Effects {
        self.state = ParkState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (&self.state, event) {
            (
                ParkState::StartTransaction,
                Event::Storage(StorageEvent::TransactionStarted { txn_id }),
            ) => {
                self.txn_id = Some(txn_id);
                let check = WaitCheck::new(&self.job, &self.keys, Some(txn_id));
                let read = check.start();
                self.state = ParkState::Check(check);
                smallvec![read]
            }
            (ParkState::Check(_) | ParkState::Recheck(_), event) => self.check_step(event),
            (
                &ParkState::WriteJob { current },
                Event::Storage(StorageEvent::BatchWriteResult { .. }),
            ) => {
                let Some(txn_id) = self.txn_id else {
                    return self.fail(BlobQueueError::UnexpectedEvent("no transaction".into()));
                };
                self.state = ParkState::Commit { current };
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            (
                &ParkState::Commit { current },
                Event::Storage(StorageEvent::TransactionCommitted { .. }),
            ) => {
                self.txn_id = None;
                if !current {
                    return self.finish(self.due);
                }
                let check = WaitCheck::new(&self.job, &self.keys, None);
                let read = check.start();
                self.state = ParkState::Recheck(check);
                smallvec![read]
            }
            (ParkState::Commit { .. }, Event::Storage(StorageEvent::Error { error })) => {
                // The failed commit closed the transaction.
                self.txn_id = None;
                self.fail(error.into())
            }
            (
                ParkState::StartTransaction | ParkState::WriteJob { .. },
                Event::Storage(StorageEvent::Error { error }),
            ) => self.fail(error.into()),
            (
                &ParkState::KeyStatus { index },
                Event::Blob(BlobEvent::KeyStatus { generations }),
            ) => {
                let key = self.keys.get(index).copied();
                let unlocked =
                    (generations.iter()).any(|status| Some(status.key) == key && status.active);
                self.status_read(index, unlocked)
            }
            // Without a blob backend no key counts as unlocked here.
            (&ParkState::KeyStatus { index }, Event::Blob(_)) => self.status_read(index, false),
            (ParkState::Wake, Event::SubOperation(SubOperationEvent::CopyWoken { result })) => {
                match result {
                    Ok(_) => self.finish(Some(self.now_ms)),
                    Err(error) => self.fail(BlobQueueError::Wake(error)),
                }
            }
            (_, event) => self.fail(BlobQueueError::UnexpectedEvent(event.label().to_string())),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, ParkState::Finish | ParkState::Error)
    }

    fn finalize(self) -> Result<Option<u64>, BlobQueueError> {
        match (self.state, self.error) {
            (ParkState::Finish, _) => Ok(self.due),
            (_, Some(error)) => Err(error),
            _ => Err(BlobQueueError::UnexpectedEvent(
                "park did not finish".into(),
            )),
        }
    }

    fn abort(&mut self) -> Effects {
        abort_effects(&mut self.txn_id)
    }
}

#[derive(Debug, PartialEq)]
enum WakeState {
    Init,
    StartTransaction,
    ReadJob,
    WriteJob,
    DeleteWait,
    Commit,
    Finish,
    Error,
}

/// Makes the job of one wait row due at `now_ms` and drops the row, in one transaction. A job
/// that is gone or already due only loses the row. True when a parked job woke; a commit
/// conflict starts over, at most `WAKE_ATTEMPTS` times in all.
#[derive(Debug, PartialEq)]
pub(crate) struct WakeJobOperation {
    wait: Vec<u8>,
    job_key: Vec<u8>,
    now_ms: u64,
    attempt: usize,
    txn_id: Option<TxnId>,
    woke: bool,
    state: WakeState,
    error: Option<BlobQueueError>,
}

impl WakeJobOperation {
    pub(crate) fn new(wait: Vec<u8>, now_ms: u64) -> Self {
        let job_key = wait.get(REF_LEN..).map(<[u8]>::to_vec).unwrap_or_default();
        Self {
            wait,
            job_key,
            now_ms,
            attempt: 0,
            txn_id: None,
            woke: false,
            state: WakeState::Init,
            error: None,
        }
    }

    fn begin(&mut self) -> Effects {
        self.attempt += 1;
        self.woke = false;
        self.state = WakeState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn fail(&mut self, error: BlobQueueError) -> Effects {
        self.state = WakeState::Error;
        self.error = Some(error);
        abort_effects(&mut self.txn_id)
    }

    fn job_read(&mut self, value: Option<Value>) -> Effects {
        let job = match value
            .map(|value| BlobJobRecord::from_bytes(&value))
            .transpose()
        {
            Ok(job) => job,
            Err(error) => return self.fail(error.into()),
        };
        let Some(mut job) = job.filter(|job| job.due_at_ms == PARKED_DUE) else {
            return self.delete_wait();
        };
        self.woke = true;
        job.due_at_ms = self.now_ms;
        let value = match job.to_bytes() {
            Ok(value) => value,
            Err(error) => return self.fail(error.into()),
        };
        self.state = WakeState::WriteJob;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: REPLICATION_JOB_KEYSPACE.to_string(),
            key: self.job_key.clone().into(),
            value: value.into(),
            txn_id: self.txn_id,
        })]
    }

    fn delete_wait(&mut self) -> Effects {
        self.state = WakeState::DeleteWait;
        smallvec![Effect::Storage(StorageEffect::Delete {
            key_space: COPY_WAIT_KEYSPACE.to_string(),
            key: self.wait.clone().into(),
            txn_id: self.txn_id,
        })]
    }

    fn commit_failed(&mut self, error: StorageError) -> Effects {
        // The failed commit closed the transaction.
        self.txn_id = None;
        if error == StorageError::TransactionConflict && self.attempt < WAKE_ATTEMPTS {
            return self.begin();
        }
        self.fail(error.into())
    }
}

impl Operation for WakeJobOperation {
    type Output = bool;
    type Error = BlobQueueError;

    fn start(&mut self) -> Effects {
        if self.job_key.is_empty() {
            self.state = WakeState::Finish;
            return smallvec![];
        }
        self.begin()
    }

    fn step(&mut self, event: Event) -> Effects {
        match (&self.state, event) {
            (WakeState::Commit, Event::Storage(StorageEvent::Error { error })) => {
                self.commit_failed(error)
            }
            (
                WakeState::StartTransaction
                | WakeState::ReadJob
                | WakeState::WriteJob
                | WakeState::DeleteWait,
                Event::Storage(StorageEvent::Error { error }),
            ) => self.fail(error.into()),
            (
                WakeState::StartTransaction,
                Event::Storage(StorageEvent::TransactionStarted { txn_id }),
            ) => {
                self.txn_id = Some(txn_id);
                self.state = WakeState::ReadJob;
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: REPLICATION_JOB_KEYSPACE.to_string(),
                    key: self.job_key.clone().into(),
                    txn_id: Some(txn_id),
                })]
            }
            (WakeState::ReadJob, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                self.job_read(value)
            }
            (WakeState::WriteJob, Event::Storage(StorageEvent::WriteResult { .. })) => {
                self.delete_wait()
            }
            (WakeState::DeleteWait, Event::Storage(StorageEvent::DeleteResult { .. })) => {
                let Some(txn_id) = self.txn_id else {
                    return self.fail(BlobQueueError::UnexpectedEvent("no transaction".into()));
                };
                self.state = WakeState::Commit;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            (WakeState::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.txn_id = None;
                self.state = WakeState::Finish;
                smallvec![]
            }
            (_, event) => self.fail(BlobQueueError::UnexpectedEvent(event.label().to_string())),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, WakeState::Finish | WakeState::Error)
    }

    fn finalize(self) -> Result<bool, BlobQueueError> {
        match (self.state, self.error) {
            (WakeState::Finish, _) => Ok(self.woke),
            (_, Some(error)) => Err(error),
            _ => Err(BlobQueueError::UnexpectedEvent(
                "wake did not finish".into(),
            )),
        }
    }

    fn abort(&mut self) -> Effects {
        abort_effects(&mut self.txn_id)
    }
}

#[derive(Debug, PartialEq)]
enum ParkedState {
    Init,
    ReadWaits,
    Wake,
    Schedule,
    Finish,
    Error,
}

/// Makes every job parked for `key` due now and schedules a drain. Answers how many woke.
#[derive(Debug, PartialEq)]
pub struct WakeParkedOperation {
    key: BucketKeyRef,
    now_ms: u64,
    rows: VecDeque<Vec<u8>>,
    after: Option<Key>,
    woken: usize,
    state: ParkedState,
    error: Option<BlobQueueError>,
}

impl WakeParkedOperation {
    pub fn new(key: BucketKeyRef, now_ms: u64) -> Self {
        Self {
            key,
            now_ms,
            rows: VecDeque::new(),
            after: None,
            woken: 0,
            state: ParkedState::Init,
            error: None,
        }
    }

    fn read_waits(&mut self, after: Option<Key>) -> Effects {
        self.state = ParkedState::ReadWaits;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: COPY_WAIT_KEYSPACE.to_string(),
            prefix: Some(self.key.key().into()),
            start: after.map(IterStart::After),
            limit: WAIT_PAGE,
            txn_id: None,
        })]
    }

    fn waits_read(&mut self, values: Vec<(Key, Value)>) -> Effects {
        // A full page may have more rows behind it; a short one is the last.
        self.after = (values.len() == WAIT_PAGE)
            .then(|| values.last().map(|(row, _)| row.clone()))
            .flatten();
        self.rows = values.into_iter().map(|(row, _)| row.to_vec()).collect();
        self.wake_next()
    }

    fn wake_next(&mut self) -> Effects {
        let Some(row) = self.rows.pop_front() else {
            return match self.after.take() {
                Some(after) => self.read_waits(Some(after)),
                None if self.woken > 0 => {
                    self.state = ParkedState::Schedule;
                    smallvec![schedule_blob_drain()]
                }
                None => self.finish(),
            };
        };
        self.state = ParkedState::Wake;
        let wake = WakeJobOperation::new(row, self.now_ms);
        smallvec![Effect::SubOperation(boxed_suboperation(wake, |result| {
            Event::SubOperation(SubOperationEvent::CopyWoken {
                result: result.map_err(|error| error.to_string()),
            })
        }))]
    }

    fn finish(&mut self) -> Effects {
        self.state = ParkedState::Finish;
        smallvec![]
    }

    fn fail(&mut self, error: BlobQueueError) -> Effects {
        self.state = ParkedState::Error;
        self.error = Some(error);
        smallvec![]
    }
}

impl Operation for WakeParkedOperation {
    type Output = usize;
    type Error = BlobQueueError;

    fn start(&mut self) -> Effects {
        self.read_waits(None)
    }

    fn step(&mut self, event: Event) -> Effects {
        match (&self.state, event) {
            (ParkedState::ReadWaits, Event::Storage(StorageEvent::Error { error })) => {
                self.fail(error.into())
            }
            (ParkedState::ReadWaits, Event::Storage(StorageEvent::IterResult { values, .. })) => {
                self.waits_read(values)
            }
            (ParkedState::Wake, Event::SubOperation(SubOperationEvent::CopyWoken { result })) => {
                match result {
                    Ok(woke) => {
                        self.woken += usize::from(woke);
                        self.wake_next()
                    }
                    Err(error) => self.fail(BlobQueueError::Wake(error)),
                }
            }
            (ParkedState::Schedule, Event::Task(TaskEvent::Error { message, .. })) => {
                warn!(message = %message, "Failed to schedule the drain of woken copy jobs");
                self.finish()
            }
            (ParkedState::Schedule, Event::Task(_)) => self.finish(),
            (_, event) => self.fail(BlobQueueError::UnexpectedEvent(event.label().to_string())),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, ParkedState::Finish | ParkedState::Error)
    }

    fn finalize(self) -> Result<usize, BlobQueueError> {
        match (self.state, self.error) {
            (ParkedState::Finish, _) => Ok(self.woken),
            (_, Some(error)) => Err(error),
            _ => Err(BlobQueueError::UnexpectedEvent(
                "wake did not finish".into(),
            )),
        }
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[derive(Debug, PartialEq)]
enum AwaitingState {
    Init,
    ReadWaits,
    ReadJobs,
    Finish,
    Error,
}

/// Copy jobs of a relationship that wait for a source key now.
#[derive(Debug, PartialEq)]
pub struct AwaitingJobsOperation {
    relationship_id: Ulid,
    jobs: BTreeSet<Vec<u8>>,
    parked: usize,
    state: AwaitingState,
    error: Option<BlobQueueError>,
}

impl AwaitingJobsOperation {
    pub fn new(relationship_id: Ulid) -> Self {
        Self {
            relationship_id,
            jobs: BTreeSet::new(),
            parked: 0,
            state: AwaitingState::Init,
            error: None,
        }
    }

    fn read_waits(&mut self, after: Option<Key>) -> Effects {
        self.state = AwaitingState::ReadWaits;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: COPY_WAIT_KEYSPACE.to_string(),
            prefix: None,
            start: after.map(IterStart::After),
            limit: WAIT_PAGE,
            txn_id: None,
        })]
    }

    fn waits_read(&mut self, values: Vec<(Key, Value)>) -> Result<Effects, BlobQueueError> {
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
            self.state = AwaitingState::Finish;
            return Ok(smallvec![]);
        }
        // All jobs of the relationship are read in one batch instead of one read each.
        self.state = AwaitingState::ReadJobs;
        let reads = (self.jobs.iter())
            .map(|job_key| (REPLICATION_JOB_KEYSPACE.to_string(), job_key.clone().into()))
            .collect();
        Ok(smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        })])
    }

    fn jobs_read(&mut self, values: Vec<(Key, Option<Value>)>) -> Result<Effects, BlobQueueError> {
        for (_, value) in values {
            let job = value
                .map(|value| BlobJobRecord::from_bytes(&value))
                .transpose()?;
            self.parked += usize::from(job.is_some_and(|job| job.due_at_ms == PARKED_DUE));
        }
        self.state = AwaitingState::Finish;
        Ok(smallvec![])
    }
}

impl Operation for AwaitingJobsOperation {
    type Output = usize;
    type Error = BlobQueueError;

    fn start(&mut self) -> Effects {
        self.read_waits(None)
    }

    fn step(&mut self, event: Event) -> Effects {
        let result = match (&self.state, event) {
            (
                AwaitingState::ReadWaits | AwaitingState::ReadJobs,
                Event::Storage(StorageEvent::Error { error }),
            ) => Err(error.into()),
            (AwaitingState::ReadWaits, Event::Storage(StorageEvent::IterResult { values, .. })) => {
                self.waits_read(values)
            }
            (AwaitingState::ReadJobs, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                self.jobs_read(values)
            }
            (_, event) => Err(BlobQueueError::UnexpectedEvent(event.label().to_string())),
        };
        result.unwrap_or_else(|error| {
            self.state = AwaitingState::Error;
            self.error = Some(error);
            smallvec![]
        })
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, AwaitingState::Finish | AwaitingState::Error)
    }

    fn finalize(self) -> Result<usize, BlobQueueError> {
        match (self.state, self.error) {
            (AwaitingState::Finish, _) => Ok(self.parked),
            (_, Some(error)) => Err(error),
            _ => Err(BlobQueueError::UnexpectedEvent(
                "count did not finish".into(),
            )),
        }
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use crate::jobs::runtime::key_unlocked;
    use aruna_storage::StorageHandle;

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
    use crate::replication::drain::next_blob_timer;
    use aruna_core::UserId;
    use aruna_core::compute::{SecretBytes, SharedSecret};
    use aruna_core::effects::BlobEffect;
    use aruna_core::events::BlobEvent;
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
    use std::time::SystemTime;
    use tempfile::TempDir;

    fn context(storage: StorageHandle) -> DriverContext {
        DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        }
    }

    /// A node with a blob adapter, so key states are real.
    async fn keyed_context() -> (TempDir, DriverContext) {
        use aruna_core::structs::storage::blob::{Backend, BackendConfig};
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        let blob_root = format!("{root}/blobstore");
        std::fs::create_dir_all(&blob_root).unwrap();
        let storage = aruna_storage::FjallStorage::open(root).unwrap();
        let net = aruna_net::NetHandle::new(aruna_net::NetConfig::default(), storage.clone())
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
        let mut context = context(storage);
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
        let context = context(storage.clone());
        let old = BucketKeyRef::new(Ulid::from_parts(11, 11), 1);
        source(&context, old).await;
        let record = job(Ulid::from_parts(12, 12));
        let row = store(&storage, &record).await;
        let txn_id = start(&storage).await.unwrap();
        assert!(
            wait_current(&storage, &record, &[old], Some(txn_id))
                .await
                .unwrap()
        );
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
            commit(&storage, txn_id).await,
            Err(BlobQueueError::Storage(StorageError::TransactionConflict))
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
        let context = context(storage.clone());
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
        let context = context(storage.clone());
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
        assert_eq!(
            crate::blob::migration::queue::process_transitions(&context)
                .await
                .unwrap(),
            None
        );
        assert!(!key_unlocked(&context, old).await);
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
            let mut context =
                context(aruna_storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap());
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
            let error = crate::blob::migration::queue::process_transitions(&interrupted)
                .await
                .unwrap_err();
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
            assert!(!key_unlocked(&context, old).await);

            let mut unavailable = interrupted.clone();
            unavailable.task_handle = Some(aruna_tasks::TaskHandle::inactive());
            let error = crate::blob::migration::queue::process_transitions(&unavailable)
                .await
                .unwrap_err();
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
            assert_eq!(
                crate::blob::migration::queue::process_transitions(&interrupted)
                    .await
                    .unwrap(),
                None
            );
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
        use aruna_core::operation_check::{
            check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
        };

        fn key() -> BucketKeyRef {
            BucketKeyRef::new(Ulid::from_parts(3, 3), 1)
        }

        fn wait() -> Vec<u8> {
            wait_row(key(), b"job")
        }

        fn txn() -> TxnId {
            Ulid::from_parts(8, 8)
        }

        fn parked() -> BlobJobRecord {
            let mut record = job(Ulid::from_parts(1, 1));
            record.due_at_ms = PARKED_DUE;
            record
        }

        fn started() -> Event {
            Event::Storage(StorageEvent::TransactionStarted { txn_id: txn() })
        }

        fn job_read(record: Option<&BlobJobRecord>) -> Event {
            Event::Storage(StorageEvent::ReadResult {
                key: b"job".to_vec().into(),
                value: record.map(|record| record.to_bytes().unwrap().into()),
            })
        }

        fn deleted() -> Event {
            Event::Storage(StorageEvent::DeleteResult { key: wait().into() })
        }

        fn delete_wait() -> Effect {
            Effect::Storage(StorageEffect::Delete {
                key_space: COPY_WAIT_KEYSPACE.to_string(),
                key: wait().into(),
                txn_id: Some(txn()),
            })
        }

        fn commit() -> Effect {
            Effect::Storage(StorageEffect::CommitTransaction { txn_id: txn() })
        }

        fn committed() -> Event {
            Event::Storage(StorageEvent::TransactionCommitted { txn_id: txn() })
        }

        #[test]
        fn wakes_parked_job() {
            let mut operation = WakeJobOperation::new(wait(), 7);
            let mut woken = parked();
            woken.due_at_ms = 7;
            let read = Effect::Storage(StorageEffect::Read {
                key_space: REPLICATION_JOB_KEYSPACE.to_string(),
                key: b"job".to_vec().into(),
                txn_id: Some(txn()),
            });
            let write = Effect::Storage(StorageEffect::Write {
                key_space: REPLICATION_JOB_KEYSPACE.to_string(),
                key: b"job".to_vec().into(),
                value: woken.to_bytes().unwrap().into(),
                txn_id: Some(txn()),
            });
            let written = Event::Storage(StorageEvent::WriteResult {
                key: b"job".to_vec().into(),
            });
            let begin = [Effect::Storage(StorageEffect::StartTransaction {
                read: false,
            })];
            let steps = vec![
                (started(), vec![read]),
                (job_read(Some(&parked())), vec![write]),
                (written, vec![delete_wait()]),
                (deleted(), vec![commit()]),
                (committed(), vec![]),
            ];
            check_trace(&mut operation, &begin, steps);
            assert_eq!(operation.finalize(), Ok(true));
        }

        #[test]
        fn stale_job_skipped() {
            let mut due = parked();
            due.due_at_ms = 3;
            for record in [None, Some(&due)] {
                let mut operation = WakeJobOperation::new(wait(), 7);
                operation.start();
                operation.step(started());
                assert_eq!(operation.step(job_read(record)).as_slice(), [delete_wait()]);
                operation.step(deleted());
                operation.step(committed());
                assert_eq!(operation.finalize(), Ok(false));
            }
            // A row too short to name a job is done without storage.
            let mut short = WakeJobOperation::new(vec![1, 2], 7);
            assert!(short.start().is_empty());
            assert_eq!(short.finalize(), Ok(false));
        }

        #[test]
        fn conflicts_retry_bounded() {
            let conflict = || {
                Event::Storage(StorageEvent::Error {
                    error: StorageError::TransactionConflict,
                })
            };
            let mut operation = WakeJobOperation::new(wait(), 7);
            operation.start();
            for attempt in 1..=WAKE_ATTEMPTS {
                operation.step(started());
                operation.step(job_read(None));
                operation.step(deleted());
                let effects = operation.step(conflict());
                assert_eq!(effects.is_empty(), attempt == WAKE_ATTEMPTS);
            }
            assert_eq!(
                operation.finalize(),
                Err(BlobQueueError::Storage(StorageError::TransactionConflict))
            );
        }

        #[test]
        fn wake_contract() {
            let reading = || {
                let mut operation = WakeJobOperation::new(wait(), 7);
                operation.start();
                operation.step(started());
                operation
            };
            let refused = rejects_events(reading, sample_events, |event| {
                matches!(event, Event::Storage(StorageEvent::ReadResult { .. }))
            });
            let abort = Effect::Storage(StorageEffect::AbortTransaction { txn_id: txn() });
            assert!(
                refused
                    .iter()
                    .all(|(_, effects, _)| effects.as_slice() == [abort.clone()])
            );
            let mut open = reading();
            check_abort(&mut open, &[abort]);
            rejects_finalize(open);
        }

        fn waits(rows: usize) -> Event {
            Event::Storage(StorageEvent::IterResult {
                values: (0..rows)
                    .map(|index| (wait_row(key(), &[index as u8; 4]).into(), Vec::new().into()))
                    .collect(),
                next_start_after: None,
            })
        }

        fn woken(result: Result<bool, String>) -> Event {
            Event::SubOperation(SubOperationEvent::CopyWoken { result })
        }

        #[test]
        fn wakes_then_schedules() {
            let mut operation = WakeParkedOperation::new(key(), 5);
            assert_eq!(operation.start().len(), 1);
            assert!(matches!(
                operation.step(waits(2)).as_slice(),
                [Effect::SubOperation(_)]
            ));
            assert!(matches!(
                operation.step(woken(Ok(true))).as_slice(),
                [Effect::SubOperation(_)]
            ));
            assert_eq!(
                operation.step(woken(Ok(false))).as_slice(),
                [schedule_blob_drain()]
            );
            let scheduled = Event::Task(TaskEvent::Error {
                key: None,
                message: "no task runtime".to_string(),
            });
            assert!(operation.step(scheduled).is_empty());
            assert_eq!(operation.finalize(), Ok(1));
            // Nothing woken schedules nothing.
            let mut quiet = WakeParkedOperation::new(key(), 5);
            quiet.start();
            assert!(quiet.step(waits(0)).is_empty());
            assert_eq!(quiet.finalize(), Ok(0));
        }

        #[test]
        fn full_page_continues() {
            let mut operation = WakeParkedOperation::new(key(), 5);
            operation.start();
            let page = waits(WAIT_PAGE);
            operation.step(page);
            for _ in 1..WAIT_PAGE {
                operation.step(woken(Ok(false)));
            }
            let effects = operation.step(woken(Ok(false)));
            assert!(matches!(
                effects.as_slice(),
                [Effect::Storage(StorageEffect::Iter { start: Some(_), .. })]
            ));
        }

        #[test]
        fn parked_contract() {
            let mut failed = WakeParkedOperation::new(key(), 5);
            failed.start();
            failed.step(waits(1));
            failed.step(woken(Err("busy".to_string())));
            assert_eq!(
                failed.finalize(),
                Err(BlobQueueError::Wake("busy".to_string()))
            );
            let reading = || {
                let mut operation = WakeParkedOperation::new(key(), 5);
                operation.start();
                operation
            };
            assert!(
                !rejects_events(reading, sample_events, |event| {
                    matches!(
                        event,
                        Event::Storage(
                            StorageEvent::IterResult { .. } | StorageEvent::Error { .. }
                        )
                    )
                })
                .is_empty()
            );
            let mut open = reading();
            check_abort(&mut open, &[]);
            rejects_finalize(open);
        }

        #[test]
        fn counts_parked_batched() {
            let relationship = Ulid::from_parts(1, 1);
            let owner = postcard::to_allocvec(&Some(relationship)).unwrap();
            let other = postcard::to_allocvec(&Some(Ulid::from_parts(2, 2))).unwrap();
            let mut operation = AwaitingJobsOperation::new(relationship);
            operation.start();
            let page = Event::Storage(StorageEvent::IterResult {
                values: vec![
                    (wait_row(key(), b"one").into(), owner.clone().into()),
                    (wait_row(key(), b"two").into(), owner.into()),
                    (wait_row(key(), b"three").into(), other.into()),
                ],
                next_start_after: None,
            });
            let batch = Effect::Storage(StorageEffect::BatchRead {
                reads: vec![
                    (REPLICATION_JOB_KEYSPACE.to_string(), b"one".to_vec().into()),
                    (REPLICATION_JOB_KEYSPACE.to_string(), b"two".to_vec().into()),
                ],
                txn_id: None,
            });
            assert_eq!(operation.step(page).as_slice(), [batch]);
            let jobs = Event::Storage(StorageEvent::BatchReadResult {
                values: vec![
                    (
                        b"one".to_vec().into(),
                        Some(parked().to_bytes().unwrap().into()),
                    ),
                    (b"two".to_vec().into(), None),
                ],
            });
            assert!(operation.step(jobs).is_empty());
            assert_eq!(operation.finalize(), Ok(1));
        }

        #[test]
        fn awaiting_contract() {
            let reading = || {
                let mut operation = AwaitingJobsOperation::new(Ulid::from_parts(1, 1));
                operation.start();
                operation
            };
            assert!(
                !rejects_events(reading, sample_events, |event| {
                    matches!(
                        event,
                        Event::Storage(
                            StorageEvent::IterResult { .. } | StorageEvent::Error { .. }
                        )
                    )
                })
                .is_empty()
            );
            let mut open = reading();
            check_abort(&mut open, &[]);
            rejects_finalize(open);
            let mut empty = reading();
            assert!(empty.step(waits(0)).is_empty());
            assert_eq!(empty.finalize(), Ok(0));
        }

        fn park(keys: Vec<BucketKeyRef>) -> ParkJobOperation {
            ParkJobOperation::new(b"job".to_vec(), job(Ulid::from_parts(1, 1)), keys, 100)
        }

        fn records_read(settings: &BucketEncryption, record: Option<BucketKeyRecord>) -> Event {
            Event::Storage(StorageEvent::BatchReadResult {
                values: vec![
                    (
                        b"bucket".to_vec().into(),
                        Some(settings.to_bytes().unwrap().into()),
                    ),
                    (
                        key().key().into(),
                        record.map(|record| record.to_bytes().unwrap().into()),
                    ),
                ],
            })
        }

        #[test]
        fn stale_key_polls() {
            // A key that is no longer the bucket's key leaves the job due after the poll delay.
            let mut operation = park(vec![key()]);
            operation.start();
            let read = operation.step(started());
            assert!(matches!(
                read.as_slice(),
                [Effect::Storage(StorageEffect::BatchRead {
                    txn_id: Some(_),
                    ..
                })]
            ));
            let written = operation.step(records_read(&BucketEncryption::default(), None));
            let [Effect::Storage(StorageEffect::BatchWrite { writes, .. })] = written.as_slice()
            else {
                panic!("expected the job write, got {written:?}");
            };
            assert_eq!(writes.len(), 1, "no wait row for a stale key");
            let batch = Event::Storage(StorageEvent::BatchWriteResult {
                entries: Vec::new(),
            });
            assert_eq!(operation.step(batch).as_slice(), [commit()]);
            assert!(operation.step(committed()).is_empty());
            let due = 100 + REPLICATION_POLL_AFTER.as_millis() as u64;
            assert_eq!(operation.finalize(), Ok(Some(due)));
        }

        #[test]
        fn park_errors_abort() {
            let abort = Effect::Storage(StorageEffect::AbortTransaction { txn_id: txn() });
            let timeout = || {
                Event::Storage(StorageEvent::Error {
                    error: StorageError::Timeout,
                })
            };
            let mut checking = park(vec![key()]);
            checking.start();
            checking.step(started());
            assert_eq!(checking.step(timeout()).as_slice(), [abort.clone()]);
            assert_eq!(
                checking.finalize(),
                Err(BlobQueueError::Storage(StorageError::Timeout))
            );
            let mut starting = park(vec![key()]);
            starting.start();
            assert!(starting.step(timeout()).is_empty());
            assert!(starting.finalize().is_err());
        }

        #[test]
        fn locked_keys_stay() {
            // After the commit each key's status is read; no unlocked key leaves the job parked.
            let mut operation = park(vec![key()]);
            operation.state = ParkState::KeyStatus { index: 0 };
            let locked = Event::Blob(BlobEvent::KeyStatus {
                generations: Vec::new(),
            });
            assert!(operation.step(locked).is_empty());
            assert_eq!(operation.finalize(), Ok(None));
            let mut unlocked = park(vec![key()]);
            unlocked.state = ParkState::KeyStatus { index: 0 };
            let missing = Event::Blob(BlobEvent::Error(
                aruna_core::errors::BlobError::HandleMissing,
            ));
            assert!(unlocked.step(missing).is_empty());
            assert_eq!(unlocked.finalize(), Ok(None));
        }

        #[test]
        fn park_contract() {
            let checking = || {
                let mut operation = park(vec![key()]);
                operation.start();
                operation.step(started());
                operation
            };
            let refused = rejects_events(checking, sample_events, |event| {
                matches!(event, Event::Storage(StorageEvent::BatchReadResult { .. }))
            });
            let abort = Effect::Storage(StorageEffect::AbortTransaction { txn_id: txn() });
            assert!(
                refused
                    .iter()
                    .all(|(_, effects, _)| effects.as_slice() == [abort.clone()])
            );
            let mut open = checking();
            check_abort(&mut open, &[abort]);
            rejects_finalize(open);
            let mut fresh = park(vec![key()]);
            check_abort(&mut fresh, &[]);
            rejects_finalize(fresh);
        }
    }
}
