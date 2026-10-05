//! Finds the buckets whose holder-unlocked keys were unlocked before this start, also in mode off
//! while a decryption keeps its source key, with those generations and the current holders.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::s3::bucket::key_rows::{SettingsError, authority_read, parse_authority};
use crate::s3::restart_notice::RestartedBucket;
use aruna_core::UserId;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BUCKET_AUDIT_KEYSPACE, BUCKET_ENCRYPTION_KEYSPACE, BUCKET_HOLDER_KEYSPACE, BUCKET_KEY_KEYSPACE,
    KEY_COPY_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::encryption::{
    BucketEncryption, BucketHolder, BucketKeyRecord, KeyState, SealedCopy,
};
use aruna_core::structs::storage::holders::resolve_holders;
use aruna_core::structs::storage::key_audit::{AuditAction, AuditOutcome, BucketAuditRecord};
use aruna_core::types::{Effects, Key, Value};
use smallvec::smallvec;
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;
use ulid::Ulid;

const SCAN_LIMIT: usize = 1_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ScanStep {
    Init,
    ScanSettings,
    ScanKeys,
    ScanAudit,
    ReadBucket,
    ReadAuthority,
    ScanGrants,
    ScanCopies,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum RestartScanError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Settings(#[from] SettingsError),
    #[error("unexpected event in state {state}: expected {expected}, got {received:?}")]
    InvalidStateEvent {
        state: String,
        expected: &'static str,
        received: Event,
    },
    #[error("the restart scan did not finish")]
    NotFinished,
}

/// One unlock session as the audit trail describes it.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Session {
    id: Option<Ulid>,
    deadline_ms: Option<u64>,
    /// The intent that set the deadline, so only its failure undoes it.
    source: Option<Ulid>,
}

/// An intent whose outcome is not read yet: the session it opens or extends, and the state it
/// replaced, restored if the outcome says it failed.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Pending {
    session: Option<Ulid>,
    before: Option<Session>,
    /// The lock reason the intent cleared, restored with the session.
    lock: Option<Lock>,
}

/// The action and time of the lock that ended a generation's last session.
pub(crate) type Lock = (AuditAction, u64);

/// The unlock state of one generation.
#[derive(Debug, Default, PartialEq)]
struct Trail {
    open: Option<Session>,
    intents: BTreeMap<Ulid, Pending>,
    lock: Option<Lock>,
}

/// A record without a session applies to every session of its generation.
fn same(left: Option<Ulid>, right: Option<Ulid>) -> bool {
    left.is_none() || right.is_none() || left == right
}

impl Trail {
    fn opens(&self, session: Option<Ulid>) -> bool {
        self.open.is_some_and(|open| same(open.id, session))
    }

    /// Intents that would restore the deadline of failed intent `source` restore `instead`, as
    /// that deadline never applied.
    fn unset(&mut self, source: Ulid, instead: Option<Session>) {
        for pending in self.intents.values_mut() {
            if pending
                .before
                .is_some_and(|before| before.source == Some(source))
            {
                pending.before = instead;
            }
        }
    }

    /// Intents that would restore `ended` restore `instead`, as `ended` no longer exists.
    fn forget(&mut self, ended: Option<Ulid>, instead: Option<Session>) {
        for pending in self.intents.values_mut() {
            if pending.before.is_some_and(|before| same(before.id, ended)) {
                pending.before = instead;
            }
        }
    }
}

/// The unlock state an audit trail describes, rebuilt record by record in storage order.
/// Outcomes name their intents, so a failure only undoes its own intent and session. An intent
/// without a stored outcome counts as applied: a crash may have lost the outcome after the key
/// was in use.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct Replay {
    trails: BTreeMap<u64, Trail>,
}

impl Replay {
    pub(crate) fn apply(&mut self, record: &BucketAuditRecord) {
        let Some(generation) = record.generation else {
            return;
        };
        let trail = self.trails.entry(generation).or_default();
        let session = record.session_id;
        let current = Session {
            id: session,
            deadline_ms: record.deadline_ms,
            source: match record.outcome {
                AuditOutcome::Intent => Some(record.event_id),
                AuditOutcome::Applied | AuditOutcome::Failed => record.intent_id,
            },
        };
        let pending = record.intent_id.and_then(|id| trail.intents.remove(&id));
        match (record.action, record.outcome) {
            (AuditAction::Unlock, AuditOutcome::Intent) => {
                let (before, lock) = (trail.open, trail.lock.take());
                let pending = Pending {
                    session,
                    before,
                    lock,
                };
                trail.intents.insert(record.event_id, pending);
                trail.open = Some(current);
            }
            // A confirmed unlock never replaces a newer session's intent.
            (AuditAction::Unlock, AuditOutcome::Applied) => {
                if pending.is_none() || trail.opens(session) {
                    (trail.open, trail.lock) = (Some(current), None);
                }
            }
            (AuditAction::Unlock, AuditOutcome::Failed) => {
                if let Some(pending) = pending {
                    if trail.opens(pending.session) {
                        (trail.open, trail.lock) = (pending.before, pending.lock);
                    }
                    trail.forget(pending.session, pending.before);
                }
            }
            // An extension only moves the deadline of the session it names.
            (AuditAction::Extend, AuditOutcome::Intent) if trail.opens(session) => {
                let pending = Pending {
                    session,
                    before: trail.open,
                    lock: trail.lock,
                };
                trail.intents.insert(record.event_id, pending);
                trail.open = Some(current);
            }
            // A confirmed extension never replaces the deadline of a later intent.
            (AuditAction::Extend, AuditOutcome::Applied) if trail.opens(session) => {
                let from = |open: Session| open.source == record.intent_id;
                if pending.is_none() || trail.open.is_some_and(from) {
                    trail.open = Some(current);
                }
            }
            // A failed extension undoes only the deadline it set itself.
            (AuditAction::Extend, AuditOutcome::Failed) => {
                if let (Some(pending), Some(intent)) = (pending, record.intent_id) {
                    let own = |open: Session| open.source == Some(intent);
                    if trail.opens(pending.session) && trail.open.is_some_and(own) {
                        trail.open = pending.before;
                    }
                    trail.unset(intent, pending.before);
                }
            }
            // A delayed lock of an older session leaves a newer session unlocked.
            (AuditAction::Lock | AuditAction::TimedLock, AuditOutcome::Applied)
                if trail.open.is_none() || trail.opens(session) =>
            {
                (trail.open, trail.lock) = (None, Some((record.action, record.at_ms)));
                trail.forget(session, None);
            }
            (AuditAction::RestartLock, AuditOutcome::Applied) => {
                (trail.open, trail.lock) = (None, Some((record.action, record.at_ms)));
                trail.intents.clear();
            }
            _ => {}
        }
    }

    /// The lock that ended each generation's last session, for the generations it applies to.
    pub(crate) fn locks(&self) -> BTreeMap<u64, Lock> {
        (self.trails.iter())
            .filter_map(|(generation, trail)| trail.lock.map(|lock| (*generation, lock)))
            .collect()
    }

    /// The generations still unlocked at `now_ms`.
    pub(crate) fn open(&self, now_ms: u64) -> Vec<u64> {
        let live = |open: &Session| open.deadline_ms.is_none_or(|deadline| deadline > now_ms);
        self.trails
            .iter()
            .filter(|(_, trail)| trail.open.as_ref().is_some_and(live))
            .map(|(generation, _)| *generation)
            .collect()
    }
}

/// The generations `records`, in storage order, leave unlocked at `now_ms`.
pub fn replay(records: &[BucketAuditRecord], now_ms: u64) -> Vec<u64> {
    let mut replay = Replay::default();
    records.iter().for_each(|record| replay.apply(record));
    replay.open(now_ms)
}

#[derive(Debug, PartialEq)]
pub struct RestartScanOperation {
    /// The time of this start; a session whose deadline passed before it was not unlocked.
    now_ms: u64,
    realm_id: RealmId,
    step: ScanStep,
    /// Buckets with keys still to scan, by name and stable id.
    pending: Vec<(String, Ulid)>,
    current: Option<RestartedBucket>,
    /// Retained generations of the current bucket that only a holder can unlock.
    locked_keys: BTreeSet<u64>,
    replay: Replay,
    creator: Option<UserId>,
    /// Current group admins, read from the authorization documents.
    admins: BTreeSet<UserId>,
    grants: Vec<BucketHolder>,
    /// Copies of the unlocked generations.
    copies: Vec<SealedCopy>,
    found: Vec<RestartedBucket>,
    output: Option<Result<Vec<RestartedBucket>, RestartScanError>>,
}

impl RestartScanOperation {
    pub fn new(now_ms: u64, realm_id: RealmId) -> Self {
        Self {
            now_ms,
            realm_id,
            step: ScanStep::Init,
            pending: Vec::new(),
            current: None,
            locked_keys: BTreeSet::new(),
            replay: Replay::default(),
            creator: None,
            admins: BTreeSet::new(),
            grants: Vec::new(),
            copies: Vec::new(),
            found: Vec::new(),
            output: None,
        }
    }

    fn fail(&mut self, error: impl Into<RestartScanError>) -> Effects {
        self.output = Some(Err(error.into()));
        self.step = ScanStep::Error;
        smallvec![]
    }

    fn scan(&mut self, step: ScanStep, key_space: &str, prefix: Option<Vec<u8>>) -> Effects {
        self.step = step;
        iter(key_space, prefix, None, SCAN_LIMIT)
    }

    /// Grants and copies of one bucket are read whole.
    fn scan_rows(&mut self, step: ScanStep, key_space: &str, bucket_id: Ulid) -> Effects {
        self.step = step;
        iter(
            key_space,
            Some(bucket_id.to_bytes().to_vec()),
            None,
            usize::MAX,
        )
    }

    fn next_bucket(&mut self) -> Effects {
        let Some((bucket, bucket_id)) = self.pending.pop() else {
            self.step = ScanStep::Finish;
            self.output = Some(Ok(std::mem::take(&mut self.found)));
            return smallvec![];
        };
        self.locked_keys.clear();
        self.replay = Replay::default();
        self.grants.clear();
        self.copies.clear();
        self.current = Some(RestartedBucket {
            bucket,
            bucket_id,
            group_id: Ulid::nil(),
            generations: Vec::new(),
            holders: Vec::new(),
        });
        self.scan_rows(ScanStep::ScanKeys, BUCKET_KEY_KEYSPACE, bucket_id)
    }

    /// A retained generation without a node copy stays locked until a holder unlocks it, also
    /// in mode off while a decryption still needs its source key.
    fn keys_scanned(&mut self, values: Vec<(Key, Value)>) -> Effects {
        for (_, value) in values {
            let Ok(record) = BucketKeyRecord::from_bytes(value.as_ref()) else {
                continue;
            };
            if record.state != KeyState::Retired && record.vault_entry.is_none() {
                self.locked_keys.insert(record.key.generation);
            }
        }
        let Some(current) = self.current.as_ref() else {
            return self.fail(RestartScanError::NotFinished);
        };
        if self.locked_keys.is_empty() {
            return self.next_bucket();
        }
        let prefix = current.bucket_id.to_bytes().to_vec();
        self.scan(ScanStep::ScanAudit, BUCKET_AUDIT_KEYSPACE, Some(prefix))
    }

    fn settings_scanned(&mut self, values: Vec<(Key, Value)>, next: Option<Key>) -> Effects {
        for (key, value) in values {
            let Ok(settings) = BucketEncryption::from_bytes(value.as_ref()) else {
                continue;
            };
            let bucket = String::from_utf8_lossy(key.as_ref()).into_owned();
            if let Some(bucket_id) = settings.bucket_id {
                self.pending.push((bucket, bucket_id));
            }
        }
        match next {
            Some(start) => iter(BUCKET_ENCRYPTION_KEYSPACE, None, Some(start), SCAN_LIMIT),
            None => self.next_bucket(),
        }
    }

    fn audit_scanned(&mut self, values: Vec<(Key, Value)>, next: Option<Key>) -> Effects {
        for (_, value) in values {
            if let Ok(record) = BucketAuditRecord::from_bytes(value.as_ref()) {
                self.replay.apply(&record);
            }
        }
        let Some(current) = self.current.as_mut() else {
            return self.fail(RestartScanError::NotFinished);
        };
        if let Some(start) = next {
            let prefix = current.bucket_id.to_bytes().to_vec();
            return iter(BUCKET_AUDIT_KEYSPACE, Some(prefix), Some(start), SCAN_LIMIT);
        }
        let locked_keys = &self.locked_keys;
        current.generations = (self.replay.open(self.now_ms).into_iter())
            .filter(|generation| locked_keys.contains(generation))
            .collect();
        if current.generations.is_empty() {
            return self.next_bucket();
        }
        self.step = ScanStep::ReadBucket;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: current.bucket.as_bytes().into(),
            txn_id: None,
        })]
    }

    fn bucket_read(&mut self, value: Option<Value>) -> Effects {
        // A bucket deleted meanwhile has nobody to tell.
        let Some(value) = value else {
            return self.next_bucket();
        };
        let info = match BucketInfo::from_bytes(value.as_ref()) {
            Ok(info) => info,
            Err(error) => return self.fail(error),
        };
        let Some(current) = self.current.as_mut() else {
            return self.fail(RestartScanError::NotFinished);
        };
        current.group_id = info.group_id;
        self.step = ScanStep::ReadAuthority;
        let realm_id = self.realm_id;
        smallvec![authority_read(
            &current.bucket,
            realm_id,
            info.group_id,
            None
        )]
    }

    /// Admin rights come from the current authorization documents, not from stored copies.
    fn authority_scanned(&mut self, values: Vec<(Key, Option<Value>)>) -> Effects {
        let Some(current) = self.current.as_ref() else {
            return self.fail(RestartScanError::NotFinished);
        };
        match parse_authority(values, self.realm_id, current.group_id) {
            Ok(state) => {
                self.creator = Some(state.info.created_by);
                self.admins = state.admins;
            }
            Err(error) => return self.fail(error),
        }
        let bucket_id = current.bucket_id;
        self.scan_rows(ScanStep::ScanGrants, BUCKET_HOLDER_KEYSPACE, bucket_id)
    }

    fn rows_scanned(&mut self, values: Vec<(Key, Value)>) -> Effects {
        let Some(current) = self.current.as_mut() else {
            return self.fail(RestartScanError::NotFinished);
        };
        for (_, value) in values {
            match self.step {
                ScanStep::ScanGrants => {
                    let grant = BucketHolder::from_bytes(value.as_ref()).ok();
                    self.grants.extend(grant);
                }
                _ => {
                    let copy = SealedCopy::from_bytes(value.as_ref())
                        .ok()
                        .filter(|copy| current.generations.contains(&copy.key.generation));
                    self.copies.extend(copy);
                }
            }
        }
        if self.step == ScanStep::ScanGrants {
            let bucket_id = current.bucket_id;
            return self.scan_rows(ScanStep::ScanCopies, KEY_COPY_KEYSPACE, bucket_id);
        }
        // The holders now: creator, current admins and explicit grants; former admins are not.
        let Some(creator) = self.creator.take() else {
            return self.fail(RestartScanError::NotFinished);
        };
        let lookups = BTreeMap::new();
        let report = resolve_holders(creator, &self.admins, &self.grants, &lookups, &self.copies);
        let mut bucket = self.current.take();
        if let Some(bucket) = bucket.as_mut() {
            bucket.holders = report.holders.iter().map(|holder| holder.user_id).collect();
        }
        self.found.extend(bucket);
        self.next_bucket()
    }
}

fn iter(key_space: &str, prefix: Option<Vec<u8>>, start: Option<Key>, limit: usize) -> Effects {
    smallvec![Effect::Storage(StorageEffect::Iter {
        key_space: key_space.to_string(),
        prefix: prefix.map(Into::into),
        start: start.map(IterStart::After),
        limit,
        txn_id: None,
    })]
}

impl Operation for RestartScanOperation {
    type Output = Vec<RestartedBucket>;
    type Error = RestartScanError;

    fn start(&mut self) -> Effects {
        self.scan(ScanStep::ScanSettings, BUCKET_ENCRYPTION_KEYSPACE, None)
    }

    fn step(&mut self, event: Event) -> Effects {
        match (self.step, event) {
            (_, Event::Storage(StorageEvent::Error { error })) => self.fail(error),
            (
                ScanStep::ScanSettings,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => self.settings_scanned(values, next_start_after),
            (ScanStep::ScanKeys, Event::Storage(StorageEvent::IterResult { values, .. })) => {
                self.keys_scanned(values)
            }
            (
                ScanStep::ScanAudit,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => self.audit_scanned(values, next_start_after),
            (ScanStep::ReadBucket, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                self.bucket_read(value)
            }
            (ScanStep::ReadAuthority, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                self.authority_scanned(values)
            }
            (
                ScanStep::ScanGrants | ScanStep::ScanCopies,
                Event::Storage(StorageEvent::IterResult { values, .. }),
            ) => self.rows_scanned(values),
            (ScanStep::Finish | ScanStep::Error, _) => smallvec![],
            (state, received) => self.fail(RestartScanError::InvalidStateEvent {
                state: format!("{state:?}"),
                expected: "the event of the current step",
                received,
            }),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.step, ScanStep::Finish | ScanStep::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(RestartScanError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
#[path = "key_restart_tests.rs"]
mod tests;
