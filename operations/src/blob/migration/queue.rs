//! Advances queued encryption transitions: one page of versions per bucket and run, then the
//! wait for old copies to be removed. Only then the source key retires and its node copy goes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{HashSet, VecDeque};
use std::time::{Duration, SystemTime};

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{
    ABE_COPY_KEYSPACE, ABE_ENVELOPE_KEYSPACE, ABE_PENDING_KEYSPACE, ABE_VERSION_KEYSPACE,
    BLOB_CLEANUP_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, BUCKET_KEY_KEYSPACE,
    PENDING_LOCATION_KEYSPACE, TRANSITION_CLEANUP_KEYSPACE, TRANSITION_KEYSPACE,
    TRANSITION_QUEUE_KEYSPACE,
};
use aruna_core::node_vault::{VaultEntry, VaultPurpose};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{PrefixPages, read_row};
use aruna_core::structs::identity::realm::{QuotaConfig, RealmConfigDocument, RealmId};
use aruna_core::structs::storage::abe::{ObjectEnvelope, PendingCopy};
use aruna_core::structs::storage::blob::{
    BackendLocation, BlobCleanupWork, BlobVersion, VersionKey,
};
use aruna_core::structs::storage::encryption::{
    BucketKeyRecord, BucketKeyRef, KeyState, KeyTicket, UnlockStatus,
};
use aruna_core::structs::storage::transition::{
    EncryptionTransition, TransitionState, cleanup_prefix,
};
use aruna_core::task::{NO_TASK_HANDLE, TaskEffect, TaskEvent, TaskKey};
use aruna_core::types::{Key, Value};

use super::shared::MigrationError;
use crate::blob::migration::drain::MIGRATION_CONTINUE;
use crate::blob::migration::rewrite::{RewriteError, RewriteOutcome, RewriteVersionOperation};
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::jobs::key_locks::KeyUnlockedOperation;
use crate::replication::parking::WakeParkedOperation;
use crate::replication::queue::{BlobQueueError, schedule_blob_drain};

/// Versions, or old copies, one run checks per bucket.
const PAGE: usize = 64;
/// Queued buckets read per queue page.
const BUCKET_PAGE: usize = 16;
/// Wait before a transition looks again for an unlocked key or removed old copies.
pub const RECHECK: Duration = Duration::from_secs(60);
/// Passes with failures started again before the transition reports itself blocked.
const RETRIES: u32 = 10;
/// Why a transition waits after every other copy moved.
const PENDING_REASON: &str = "pending archives, envelopes or copies still use the source key";

type Turn<T> = Result<Next<T>, MigrationError>;
/// This node and its realm, where it serves one.
type Serving = Option<(NodeId, RealmId)>;
type QuotaOrigin = Option<(QuotaConfig, RealmId, NodeId)>;

fn empty_prefix() -> Key {
    Key::from(Vec::new())
}

fn soonest(next: &mut Option<Duration>, after: Duration) {
    *next = Some(next.map_or(after, |next| next.min(after)));
}

fn read_transition(bucket: &str) -> IoCall {
    read_row(TRANSITION_KEYSPACE, bucket.as_bytes().to_vec())
}

fn decode_transition(value: Option<Value>) -> Result<Option<EncryptionTransition>, MigrationError> {
    Ok(value
        .map(|value| EncryptionTransition::from_bytes(value.as_ref()))
        .transpose()?)
}

/// Takes the answer to a timer effect; a node without a task handler has nothing to wake.
fn timer_set(result: IoResult) -> Result<(), MigrationError> {
    match result.event()? {
        Event::Task(TaskEvent::TimerScheduled { .. }) => Ok(()),
        Event::Task(TaskEvent::Error { message, .. }) if message == NO_TASK_HANDLE => Ok(()),
        Event::Task(TaskEvent::Error { message, .. }) => Err(MigrationError::Failed(message)),
        other => Err(MigrationError::Failed(format!(
            "could not set a timer: {}",
            other.label()
        ))),
    }
}

#[derive(Debug, PartialEq)]
enum ResumeStep {
    Page,
    Wake,
}

/// Clears matching retry deadlines and wakes the worker when a source key becomes usable.
#[derive(Debug, PartialEq)]
pub struct ResumeTransitionsOperation {
    key: BucketKeyRef,
    step: ResumeStep,
    resumed: bool,
}

impl ResumeTransitionsOperation {
    pub fn new(key: BucketKeyRef) -> Self {
        Self {
            key,
            step: ResumeStep::Page,
            resumed: false,
        }
    }

    fn page(&self, after: Option<Key>) -> Turn<()> {
        let page = ResumePageOperation::new(self.key, after);
        Ok(Next::Call(IoCall::child(page)))
    }
}

impl Operation for ResumeTransitionsOperation {
    type Output = ();
    type Error = MigrationError;

    fn first(&mut self) -> Turn<()> {
        self.page(None)
    }

    fn next(&mut self, result: IoResult) -> Turn<()> {
        match self.step {
            ResumeStep::Page => {
                let (resumed, cursor) = result.child::<(bool, Option<Key>), MigrationError>()??;
                self.resumed |= resumed;
                if cursor.is_some() {
                    return self.page(cursor);
                }
                if !self.resumed {
                    return Ok(Next::Done(()));
                }
                self.step = ResumeStep::Wake;
                let wake = Effect::Task(TaskEffect::ShortenTimer {
                    key: TaskKey::MigrateCompression,
                    after: Duration::ZERO,
                });
                Ok(Next::Call(wake.into()))
            }
            ResumeStep::Wake => {
                timer_set(result)?;
                Ok(Next::Done(()))
            }
        }
    }
}

/// Clears the deadlines of one page of transitions in one transaction. Outputs whether any
/// matched and the key to continue after.
#[derive(Debug, PartialEq)]
struct ResumePageOperation {
    key: BucketKeyRef,
    after: Option<Key>,
    written: Option<(bool, Option<Key>)>,
}

impl ResumePageOperation {
    fn new(key: BucketKeyRef, after: Option<Key>) -> Self {
        Self {
            key,
            after,
            written: None,
        }
    }
}

impl Operation for ResumePageOperation {
    type Output = (bool, Option<Key>);
    type Error = MigrationError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Turn<(bool, Option<Key>)> {
        Ok(Next::Call(IoCall::Page {
            space: TRANSITION_KEYSPACE.to_string(),
            prefix: empty_prefix(),
            after: self.after.take(),
            limit: BUCKET_PAGE,
        }))
    }

    fn next(&mut self, result: IoResult) -> Turn<(bool, Option<Key>)> {
        if let Some(written) = self.written.take() {
            result.done()?;
            return Ok(Next::Done(written));
        }
        let (rows, cursor) = result.page()?;
        let mut writes = Vec::new();
        for (row, value) in rows {
            let mut record = EncryptionTransition::from_bytes(&value)?;
            if record.source != Some(self.key) || record.finished_at_ms.is_some() {
                continue;
            }
            record.retry_at_ms = None;
            writes.push((
                TRANSITION_KEYSPACE.to_string(),
                row,
                record.to_bytes()?.into(),
            ));
        }
        if writes.is_empty() {
            return Ok(Next::Done((false, cursor)));
        }
        self.written = Some((true, cursor));
        Ok(Next::Call(IoCall::Write(writes)))
    }
}

#[derive(Debug, PartialEq)]
enum QueueStep {
    Page,
    Record(String),
    Advance,
}

/// Advances every queued transition by one page. Outputs when the task must run again.
#[derive(Debug, PartialEq)]
pub struct ProcessTransitionsOperation {
    serving: Serving,
    now_ms: u64,
    pages: PrefixPages,
    buckets: VecDeque<Key>,
    step: QueueStep,
    next: Option<Duration>,
}

impl ProcessTransitionsOperation {
    pub fn new(serving: Serving) -> Self {
        Self {
            serving,
            now_ms: 0,
            pages: PrefixPages::new(TRANSITION_QUEUE_KEYSPACE, empty_prefix(), BUCKET_PAGE),
            buckets: VecDeque::new(),
            step: QueueStep::Page,
            next: None,
        }
    }

    fn next_bucket(&mut self) -> Turn<Option<Duration>> {
        let Some(key) = self.buckets.pop_front() else {
            if self.pages.done() {
                return Ok(Next::Done(self.next));
            }
            self.step = QueueStep::Page;
            return Ok(Next::Call(self.pages.call()));
        };
        let bucket = String::from_utf8(key.to_vec()).map_err(MigrationError::failed)?;
        let read = read_transition(&bucket);
        self.step = QueueStep::Record(bucket);
        Ok(Next::Call(read))
    }

    fn record_read(
        &mut self,
        bucket: String,
        record: Option<EncryptionTransition>,
    ) -> Turn<Option<Duration>> {
        let Some(record) = record.filter(|record| record.finished_at_ms.is_none()) else {
            return self.next_bucket();
        };
        if let Some(at) = record.retry_at_ms.filter(|at| *at > self.now_ms) {
            soonest(&mut self.next, Duration::from_millis(at - self.now_ms));
            return self.next_bucket();
        }
        self.step = QueueStep::Advance;
        let advance = AdvanceTransitionOperation::new(bucket, record, self.serving);
        Ok(Next::Call(IoCall::child(advance)))
    }
}

impl Operation for ProcessTransitionsOperation {
    type Output = Option<Duration>;
    type Error = MigrationError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Turn<Option<Duration>> {
        Ok(Next::Call(self.pages.call()))
    }

    fn next(&mut self, result: IoResult) -> Turn<Option<Duration>> {
        match std::mem::replace(&mut self.step, QueueStep::Advance) {
            QueueStep::Page => {
                let rows = self.pages.take(result)?;
                self.buckets = rows.into_iter().map(|(key, _)| key).collect();
                self.next_bucket()
            }
            QueueStep::Record(bucket) => {
                let record = decode_transition(result.value()?)?;
                self.record_read(bucket, record)
            }
            QueueStep::Advance => {
                let wait = result.child::<Option<Duration>, MigrationError>()??;
                if let Some(wait) = wait {
                    soonest(&mut self.next, wait);
                }
                self.next_bucket()
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum AdvanceStep {
    Page,
    Quota,
    Rewrite(Key),
    /// A settle or finish child whose answer is this operation's.
    Tail,
    StoredMore,
    StoredWait(Option<Duration>),
    Unlocked(BucketKeyRef, Option<Duration>),
    Resumed,
}

/// One step of a bucket's transition: a page of versions, or a cleanup check after the pass.
#[derive(Debug, PartialEq)]
struct AdvanceTransitionOperation {
    bucket: String,
    record: EncryptionTransition,
    serving: Serving,
    now_ms: u64,
    step: AdvanceStep,
    versions: VecDeque<Key>,
    more: bool,
    quota: QuotaOrigin,
}

impl AdvanceTransitionOperation {
    fn new(bucket: String, record: EncryptionTransition, serving: Serving) -> Self {
        Self {
            bucket,
            record,
            serving,
            now_ms: 0,
            step: AdvanceStep::Page,
            versions: VecDeque::new(),
            more: false,
            quota: None,
        }
    }

    fn call(&mut self, step: AdvanceStep, call: IoCall) -> Turn<Option<Duration>> {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn settle(&mut self) -> Turn<Option<Duration>> {
        let settle = SettleTransitionOperation::new(&self.bucket, self.record.clone());
        self.call(AdvanceStep::Tail, IoCall::child(settle))
    }

    fn store(&mut self, step: AdvanceStep) -> Turn<Option<Duration>> {
        let store = StoreTransitionOperation::new(&self.bucket, self.record.clone());
        self.call(step, IoCall::child(store))
    }

    fn next_version(&mut self) -> Turn<Option<Duration>> {
        let Some(key) = self.versions.pop_front() else {
            return self.page_done();
        };
        let version = VersionKey::from_bytes(key.as_ref())?;
        let now = SystemTime::UNIX_EPOCH + Duration::from_millis(self.now_ms);
        let mut rewrite = RewriteVersionOperation::new(version, self.record.clone(), now);
        if let Some((quota, realm, node)) = &self.quota {
            rewrite = rewrite.with_quota(quota.clone(), *realm, *node);
        }
        self.call(AdvanceStep::Rewrite(key), IoCall::child(rewrite))
    }

    fn rewritten(
        &mut self,
        key: Key,
        outcome: Result<RewriteOutcome, RewriteError>,
    ) -> Turn<Option<Duration>> {
        match outcome {
            Ok(RewriteOutcome::Moved) => self.record.done += 1,
            // Only a re-key returns `Unfinished`.
            Ok(RewriteOutcome::Skipped | RewriteOutcome::Unfinished) => {}
            Ok(RewriteOutcome::AwaitingKey) => self.record.remaining += 1,
            Err(error) => {
                self.record.failed += 1;
                tracing::warn!(
                    bucket = %self.bucket,
                    %error,
                    "Failed to move a version to the new encryption"
                );
            }
        }
        self.record.cursor = Some(key.to_vec());
        self.next_version()
    }

    fn page_done(&mut self) -> Turn<Option<Duration>> {
        if self.more {
            return self.store(AdvanceStep::StoredMore);
        }
        let record = &mut self.record;
        record.cursor = None;
        let wait = match (record.failed > 0, record.remaining > 0) {
            (true, _) if record.retries < RETRIES => {
                record.retries += 1;
                Some(RECHECK.saturating_mul(1 << record.retries.min(6)))
            }
            (true, _) => {
                record.state = TransitionState::Blocked;
                record.blocked_reason = Some("versions failed to move in every retry".to_string());
                Some(RECHECK.saturating_mul(60))
            }
            (false, true) => {
                record.state = TransitionState::AwaitingKey;
                Some(RECHECK)
            }
            (false, false) => {
                record.state = TransitionState::Cleanup;
                return self.settle();
            }
        };
        let now = self.now_ms;
        record.retry_at_ms = wait.map(|wait| now.saturating_add(wait.as_millis() as u64));
        self.store(AdvanceStep::StoredWait(wait))
    }

    fn stored(&mut self, result: IoResult, wait: Option<Duration>) -> Turn<Option<Duration>> {
        let stored = result.child::<StoredTransition, MigrationError>()??;
        self.record = stored.record;
        if self.record.state == TransitionState::AwaitingKey
            && let Some(key) = self.record.source
        {
            let unlocked = KeyUnlockedOperation::new(key);
            return self.call(AdvanceStep::Unlocked(key, wait), IoCall::child(unlocked));
        }
        Ok(Next::Done(wait))
    }
}

impl Operation for AdvanceTransitionOperation {
    type Output = Option<Duration>;
    type Error = MigrationError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Turn<Option<Duration>> {
        let record = &mut self.record;
        record.retry_at_ms = None;
        if record.state == TransitionState::Finished {
            let finish = FinishTransitionOperation::new(&self.bucket, self.record.clone());
            return self.call(AdvanceStep::Tail, IoCall::child(finish));
        }
        if record.state == TransitionState::Cleanup {
            return self.settle();
        }
        if record.cursor.is_none() {
            // A new pass counts its own waits and failures.
            record.remaining = 0;
            record.failed = 0;
            record.state = TransitionState::Running;
        }
        Ok(Next::Call(IoCall::Page {
            space: BLOB_VERSIONS_KEYSPACE.to_string(),
            prefix: VersionKey::bucket_prefix(&self.bucket)?.into(),
            after: record.cursor.clone().map(Into::into),
            limit: PAGE,
        }))
    }

    fn next(&mut self, result: IoResult) -> Turn<Option<Duration>> {
        match std::mem::replace(&mut self.step, AdvanceStep::Tail) {
            AdvanceStep::Page => {
                let (rows, next) = result.page()?;
                self.more = next.is_some();
                self.versions = rows.into_iter().map(|(key, _)| key).collect();
                if self.versions.is_empty() {
                    return self.page_done();
                }
                let quota = QuotaOriginOperation::new(self.serving);
                self.call(AdvanceStep::Quota, IoCall::child(quota))
            }
            AdvanceStep::Quota => {
                self.quota = result.child::<QuotaOrigin, MigrationError>()??;
                self.next_version()
            }
            AdvanceStep::Rewrite(key) => self.rewritten(key, result.child()?),
            AdvanceStep::Tail => {
                let wait = result.child::<Option<Duration>, MigrationError>()??;
                Ok(Next::Done(wait))
            }
            AdvanceStep::StoredMore => {
                result.child::<StoredTransition, MigrationError>()??;
                Ok(Next::Done(Some(MIGRATION_CONTINUE)))
            }
            AdvanceStep::StoredWait(wait) => self.stored(result, wait),
            AdvanceStep::Unlocked(key, wait) => {
                // A node that cannot tell holds no unlock.
                if !result.child::<bool, IoError>()?.unwrap_or(false) {
                    return Ok(Next::Done(wait));
                }
                let resume = ResumeTransitionsOperation::new(key);
                self.call(AdvanceStep::Resumed, IoCall::child(resume))
            }
            AdvanceStep::Resumed => {
                result.child::<(), MigrationError>()??;
                Ok(Next::Done(Some(Duration::ZERO)))
            }
        }
    }
}

/// The realm quota and origin that the envelope charges of rewritten versions must fit.
#[derive(Debug, PartialEq)]
struct QuotaOriginOperation {
    serving: Serving,
}

impl QuotaOriginOperation {
    fn new(serving: Serving) -> Self {
        Self { serving }
    }
}

impl Operation for QuotaOriginOperation {
    type Output = QuotaOrigin;
    type Error = MigrationError;

    fn first(&mut self) -> Turn<QuotaOrigin> {
        match self.serving {
            Some((_, realm)) => Ok(Next::Call(IoCall::child(GetConfigOperation::new(realm)))),
            None => Ok(Next::Done(None)),
        }
    }

    fn next(&mut self, result: IoResult) -> Turn<QuotaOrigin> {
        let Some((node, realm)) = self.serving else {
            return Ok(Next::Done(None));
        };
        // A realm without a config document has no quota.
        match result.child::<RealmConfigDocument, GetConfigError>()? {
            Ok(config) => Ok(Next::Done(Some((config.quota, realm, node)))),
            Err(GetConfigError::DocumentNotFound) => Ok(Next::Done(None)),
            Err(error) => Err(MigrationError::failed(error)),
        }
    }
}

#[derive(Debug, PartialEq)]
enum SettleStep {
    Page,
    Forget,
    Reclaim,
    StoredLeft,
    StoredFinal,
    Finish,
}

/// Counts old copies whose location rows still exist and forgets removed ones. With none
/// left the transition finishes.
#[derive(Debug, PartialEq)]
struct SettleTransitionOperation {
    bucket: String,
    record: EncryptionTransition,
    prefix_len: usize,
    pages: PrefixPages,
    left: u64,
    now_ms: u64,
    step: SettleStep,
}

impl SettleTransitionOperation {
    fn new(bucket: &str, record: EncryptionTransition) -> Self {
        let prefix: Key = cleanup_prefix(bucket).into();
        Self {
            bucket: bucket.to_string(),
            record,
            prefix_len: prefix.len(),
            pages: PrefixPages::new(TRANSITION_CLEANUP_KEYSPACE, prefix, PAGE),
            left: 0,
            now_ms: 0,
            step: SettleStep::Page,
        }
    }

    fn call(&mut self, step: SettleStep, call: IoCall) -> Turn<Option<Duration>> {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn store(&mut self, step: SettleStep) -> Turn<Option<Duration>> {
        let store = StoreTransitionOperation::new(&self.bucket, self.record.clone());
        self.call(step, IoCall::child(store))
    }

    fn next_page(&mut self) -> Turn<Option<Duration>> {
        if !self.pages.done() {
            let page = self.pages.call();
            return self.call(SettleStep::Page, page);
        }
        self.record.cleanup_remaining = self.left;
        if self.left > 0 {
            let wait = RECHECK.as_millis() as u64;
            self.record.retry_at_ms = Some(self.now_ms.saturating_add(wait));
            return self.store(SettleStep::StoredLeft);
        }
        let reclaim = ReclaimUploadsOperation::new(&self.record);
        self.call(SettleStep::Reclaim, IoCall::child(reclaim))
    }
}

impl Operation for SettleTransitionOperation {
    type Output = Option<Duration>;
    type Error = MigrationError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Turn<Option<Duration>> {
        Ok(Next::Call(self.pages.call()))
    }

    fn next(&mut self, result: IoResult) -> Turn<Option<Duration>> {
        match self.step {
            SettleStep::Page => {
                let keys = self.pages.take(result)?;
                let keys = keys.into_iter().map(|(key, _)| key).collect();
                let forget = ForgetRemovedOperation::new(self.prefix_len, keys);
                self.call(SettleStep::Forget, IoCall::child(forget))
            }
            SettleStep::Forget => {
                self.left += result.child::<u64, MigrationError>()??;
                self.next_page()
            }
            SettleStep::Reclaim => {
                result.child::<u64, MigrationError>()??;
                self.record.remaining = 0;
                self.record.blocked_reason = None;
                self.record.state = TransitionState::Finished;
                self.store(SettleStep::StoredFinal)
            }
            SettleStep::StoredLeft => {
                result.child::<StoredTransition, MigrationError>()??;
                Ok(Next::Done(Some(RECHECK)))
            }
            SettleStep::StoredFinal => {
                let stored = result.child::<StoredTransition, MigrationError>()??;
                if !stored.stored {
                    return Ok(Next::Done(None));
                }
                if stored.record.state != TransitionState::Finished {
                    return Ok(Next::Done(Some(RECHECK)));
                }
                let finish = FinishTransitionOperation::new(&self.bucket, stored.record);
                self.call(SettleStep::Finish, IoCall::child(finish))
            }
            SettleStep::Finish => {
                let wait = result.child::<Option<Duration>, MigrationError>()??;
                Ok(Next::Done(wait))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum FinishStep {
    Wake,
    Drain,
    Forget,
    Store,
}

/// Wakes the copies parked on a retired source, drops its unlock and ends the transition.
#[derive(Debug, PartialEq)]
struct FinishTransitionOperation {
    bucket: String,
    record: EncryptionTransition,
    now_ms: u64,
    step: FinishStep,
}

impl FinishTransitionOperation {
    fn new(bucket: &str, record: EncryptionTransition) -> Self {
        Self {
            bucket: bucket.to_string(),
            record,
            now_ms: 0,
            step: FinishStep::Wake,
        }
    }

    fn forget(&mut self) -> Turn<Option<Duration>> {
        self.step = FinishStep::Forget;
        let forget = ForgetRetiredOperation::new(&self.record);
        Ok(Next::Call(IoCall::child(forget)))
    }
}

impl Operation for FinishTransitionOperation {
    type Output = Option<Duration>;
    type Error = MigrationError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Turn<Option<Duration>> {
        match retiring(&self.record) {
            Some(source) => {
                let wake = WakeParkedOperation::new(source, self.now_ms);
                Ok(Next::Call(IoCall::child(wake)))
            }
            None => self.forget(),
        }
    }

    fn next(&mut self, result: IoResult) -> Turn<Option<Duration>> {
        match self.step {
            FinishStep::Wake => {
                (result.child::<usize, BlobQueueError>()?).map_err(MigrationError::failed)?;
                self.step = FinishStep::Drain;
                Ok(Next::Call(schedule_blob_drain().into()))
            }
            FinishStep::Drain => {
                timer_set(result)?;
                self.forget()
            }
            FinishStep::Forget => {
                // A failure only leaves the key until the next lock or restart.
                if let Err(error) = result.child::<(), IoError>()? {
                    tracing::warn!(%error, "Could not drop a retired key generation");
                }
                self.record.finished_at_ms = Some(self.now_ms);
                self.step = FinishStep::Store;
                let store = StoreTransitionOperation::new(&self.bucket, self.record.clone());
                Ok(Next::Call(IoCall::child(store)))
            }
            FinishStep::Store => {
                let stored = result.child::<StoredTransition, MigrationError>()??;
                let again = stored.stored && stored.record.finished_at_ms.is_none();
                Ok(Next::Done(again.then_some(RECHECK)))
            }
        }
    }
}

/// The sessions of a retired source generation; admitted leases keep their own key handle.
fn retired_sessions(generations: &[UnlockStatus], record: &EncryptionTransition) -> Vec<KeyTicket> {
    let Some(source) = record.source else {
        return Vec::new();
    };
    if record.target.plan.map(|plan| plan.key) == Some(source) {
        return Vec::new();
    }
    let sessions = generations.iter().filter(|status| status.key == source);
    sessions
        .map(|status| KeyTicket {
            key: status.key,
            session_id: status.session_id,
        })
        .collect()
}

/// Drops a retired generation from the unlock registry, so it no longer takes a generation
/// slot.
#[derive(Debug, PartialEq)]
struct ForgetRetiredOperation {
    record: EncryptionTransition,
    tickets: Option<VecDeque<KeyTicket>>,
}

impl ForgetRetiredOperation {
    fn new(record: &EncryptionTransition) -> Self {
        Self {
            record: record.clone(),
            tickets: None,
        }
    }

    fn next_ticket(&mut self) -> ForgetTurn {
        match self.tickets.as_mut().and_then(VecDeque::pop_front) {
            Some(ticket) => {
                let discard = Effect::Blob(BlobEffect::DiscardKey { ticket });
                Ok(Next::Call(discard.into()))
            }
            None => Ok(Next::Done(())),
        }
    }
}

type ForgetTurn = Result<Next<()>, IoError>;

impl Operation for ForgetRetiredOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> ForgetTurn {
        let Some(source) = self.record.source else {
            return Ok(Next::Done(()));
        };
        let status = Effect::Blob(BlobEffect::ReadKeyStatus {
            bucket_id: source.bucket_id,
        });
        Ok(Next::Call(status.into()))
    }

    fn next(&mut self, result: IoResult) -> ForgetTurn {
        if self.tickets.is_some() {
            return self.next_ticket();
        }
        let Event::Blob(BlobEvent::KeyStatus { generations }) = result.event()? else {
            tracing::warn!("Could not read the unlock state of a retired key generation");
            return Ok(Next::Done(()));
        };
        self.tickets = Some(retired_sessions(&generations, &self.record).into());
        self.next_ticket()
    }
}

/// How many of `locations` are sealed to `source` and still need `record`.
fn uses_key(
    locations: &[BackendLocation],
    source: BucketKeyRef,
    record: &EncryptionTransition,
) -> u64 {
    let sealed = locations
        .iter()
        .filter(|location| location.format.bucket_key() == Some(source) && record.needs(location));
    sealed.count() as u64
}

#[derive(Debug, PartialEq)]
enum ForgetStep {
    Queued,
    Locations(Vec<Key>),
    Deleted,
}

/// Forgets deleted copies in one transaction; concurrent reclaim is in its snapshot or fails
/// commit. Reclaim drops the location row before the backend delete, so only finished delete
/// work proves removal. Outputs the count still present, with copies awaiting deletion or retry.
#[derive(Debug, PartialEq)]
struct ForgetRemovedOperation {
    prefix_len: usize,
    keys: Vec<Key>,
    pages: PrefixPages,
    queued: HashSet<Vec<u8>>,
    left: u64,
    step: ForgetStep,
}

impl ForgetRemovedOperation {
    fn new(prefix_len: usize, keys: Vec<Key>) -> Self {
        Self {
            prefix_len,
            keys,
            pages: PrefixPages::new(BLOB_CLEANUP_KEYSPACE, empty_prefix(), PAGE),
            queued: HashSet::new(),
            left: 0,
            step: ForgetStep::Queued,
        }
    }

    fn check_locations(&mut self) -> Turn<u64> {
        let mut checked = Vec::new();
        let mut reads = Vec::new();
        for key in std::mem::take(&mut self.keys) {
            let location = key[self.prefix_len..].to_vec();
            if self.queued.contains(&location) {
                self.left += 1;
                continue;
            }
            reads.push((BLOB_LOCATIONS_KEYSPACE.to_string(), location.into()));
            checked.push(key);
        }
        if reads.is_empty() {
            return Ok(Next::Done(self.left));
        }
        self.step = ForgetStep::Locations(checked);
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn locations_read(&mut self, keys: Vec<Key>, values: Vec<Option<Value>>) -> Turn<u64> {
        let mut deletes = Vec::new();
        for (key, value) in keys.into_iter().zip(values) {
            match value {
                Some(_) => self.left += 1,
                None => deletes.push((TRANSITION_CLEANUP_KEYSPACE.to_string(), key)),
            }
        }
        if deletes.is_empty() {
            return Ok(Next::Done(self.left));
        }
        self.step = ForgetStep::Deleted;
        Ok(Next::Call(IoCall::Delete(deletes)))
    }
}

impl Operation for ForgetRemovedOperation {
    type Output = u64;
    type Error = MigrationError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Turn<u64> {
        if self.keys.is_empty() {
            return Ok(Next::Done(0));
        }
        Ok(Next::Call(self.pages.call()))
    }

    fn next(&mut self, result: IoResult) -> Turn<u64> {
        match std::mem::replace(&mut self.step, ForgetStep::Deleted) {
            ForgetStep::Queued => {
                self.step = ForgetStep::Queued;
                for (_, value) in self.pages.take(result)? {
                    let work = BlobCleanupWork::from_bytes(value.as_ref())?;
                    if let Some(Ok(key)) = work.location().map(BackendLocation::location_key) {
                        self.queued.insert(key.to_bytes());
                    }
                }
                if self.pages.done() {
                    return self.check_locations();
                }
                Ok(Next::Call(self.pages.call()))
            }
            ForgetStep::Locations(keys) => self.locations_read(keys, result.values()?),
            ForgetStep::Deleted => {
                result.done()?;
                Ok(Next::Done(self.left))
            }
        }
    }
}

/// The retiring source generation of `record`, if it has one.
fn retiring(record: &EncryptionTransition) -> Option<BucketKeyRef> {
    let source = record.source?;
    (record.target.plan.map(|plan| plan.key) != Some(source)).then_some(source)
}

fn envelope_key(bytes: &[u8]) -> Option<BucketKeyRef> {
    let envelope = postcard::from_bytes::<ObjectEnvelope>(bytes).ok()?;
    Some(envelope.context.parameters.key)
}

#[derive(Debug, PartialEq)]
enum ScanPhase {
    Pending,
    Envelopes,
    EnvelopeRows,
    Copies,
    Versions,
    VersionRows,
}

/// Walks, inside the caller's transaction, what still needs the source generation: pending
/// archives on every backend, envelopes and pending copies of the bucket, and its versions.
#[derive(Debug, PartialEq)]
struct SourceScan {
    prefix: Key,
    record: EncryptionTransition,
    phase: ScanPhase,
    pages: PrefixPages,
    /// Archives, envelopes and copies that use the source.
    pending: u64,
    /// Versions whose copy still needs the transition.
    versions: u64,
}

impl SourceScan {
    fn new(bucket: &str, record: &EncryptionTransition) -> Result<Self, MigrationError> {
        Ok(Self {
            prefix: VersionKey::bucket_prefix(bucket)?.into(),
            record: record.clone(),
            phase: ScanPhase::Pending,
            pages: PrefixPages::new(PENDING_LOCATION_KEYSPACE, empty_prefix(), PAGE),
            pending: 0,
            versions: 0,
        })
    }

    fn enter(&mut self, phase: ScanPhase, space: &str, prefix: Key) -> IoCall {
        self.phase = phase;
        self.pages = PrefixPages::new(space, prefix, PAGE);
        self.pages.call()
    }

    fn start(&mut self) -> IoCall {
        match self.record.source {
            Some(_) => self.pages.call(),
            None => self.after_pending(),
        }
    }

    fn after_pending(&mut self) -> IoCall {
        match retiring(&self.record) {
            Some(_) => {
                let prefix = self.prefix.clone();
                self.enter(ScanPhase::Envelopes, ABE_VERSION_KEYSPACE, prefix)
            }
            None => self.enter_versions(),
        }
    }

    fn enter_versions(&mut self) -> IoCall {
        let prefix = self.prefix.clone();
        self.enter(ScanPhase::Versions, BLOB_VERSIONS_KEYSPACE, prefix)
    }

    fn after_envelopes(&mut self) -> IoCall {
        match retiring(&self.record) {
            Some(source) => {
                let prefix = source.bucket_id.to_bytes().to_vec().into();
                self.enter(ScanPhase::Copies, ABE_COPY_KEYSPACE, prefix)
            }
            None => self.enter_versions(),
        }
    }

    fn envelopes_next(&mut self) -> IoCall {
        match self.pages.done() {
            true => self.after_envelopes(),
            false => self.pages.call(),
        }
    }

    fn versions_next(&self) -> Option<IoCall> {
        (!self.pages.done()).then(|| self.pages.call())
    }

    /// Takes the answer to the last call and names the next one; `None` ends the walk.
    fn step(&mut self, result: IoResult) -> Result<Option<IoCall>, MigrationError> {
        match self.phase {
            ScanPhase::Pending => {
                let rows = self.pages.take(result)?;
                let locations = rows
                    .iter()
                    .map(|(_, value)| BackendLocation::from_bytes(value.as_ref()))
                    .collect::<Result<Vec<_>, _>>()?;
                if let Some(source) = self.record.source {
                    self.pending += uses_key(&locations, source, &self.record);
                }
                Ok(Some(match self.pages.done() {
                    true => self.after_pending(),
                    false => self.pages.call(),
                }))
            }
            ScanPhase::Envelopes => {
                let rows = self.pages.take(result)?;
                let reads: Vec<_> = rows
                    .into_iter()
                    .map(|(_, id)| (ABE_ENVELOPE_KEYSPACE.to_string(), id))
                    .collect();
                if reads.is_empty() {
                    return Ok(Some(self.envelopes_next()));
                }
                self.phase = ScanPhase::EnvelopeRows;
                Ok(Some(IoCall::Read(reads)))
            }
            ScanPhase::EnvelopeRows => {
                let source = retiring(&self.record);
                let values = result.values()?;
                let used = values.iter().filter_map(|value| value.as_deref());
                self.pending += used.filter(|value| envelope_key(value) == source).count() as u64;
                self.phase = ScanPhase::Envelopes;
                Ok(Some(self.envelopes_next()))
            }
            ScanPhase::Copies => {
                let source = retiring(&self.record);
                for (_, row) in self.pages.take(result)? {
                    let pending = PendingCopy::from_bytes(&row).map_err(MigrationError::failed)?;
                    let key = pending.source.context.parameters.key;
                    self.pending += u64::from(Some(key) == source);
                }
                Ok(Some(match self.pages.done() {
                    true => self.enter_versions(),
                    false => self.pages.call(),
                }))
            }
            ScanPhase::Versions => {
                let mut reads = Vec::new();
                for (_, value) in self.pages.take(result)? {
                    let version = BlobVersion::from_bytes(&value)?;
                    if let Some(key) = version.location_key() {
                        reads.push((BLOB_LOCATIONS_KEYSPACE.to_string(), key.to_bytes().into()));
                    }
                }
                if reads.is_empty() {
                    return Ok(self.versions_next());
                }
                self.phase = ScanPhase::VersionRows;
                Ok(Some(IoCall::Read(reads)))
            }
            ScanPhase::VersionRows => {
                for value in result.values()? {
                    let Some(value) = value else {
                        return Err(MigrationError::Failed(
                            "could not read a remaining version's location".to_string(),
                        ));
                    };
                    let location = BackendLocation::from_bytes(&value)?;
                    self.versions += u64::from(self.record.needs(&location));
                }
                self.phase = ScanPhase::Versions;
                Ok(self.versions_next())
            }
        }
    }
}

/// A transition after its store, and whether the store still owned its row.
#[derive(Debug, PartialEq)]
struct StoredTransition {
    stored: bool,
    record: EncryptionTransition,
}

#[derive(Debug, PartialEq)]
enum StoreStep {
    Current,
    Scan(Box<SourceScan>),
    SourceKey,
    Written(Option<VaultEntry>),
    Left,
    Vault,
}

/// Stores progress unless a newer change replaced it; retirement keeps the queue until copies
/// wake.
#[derive(Debug, PartialEq)]
struct StoreTransitionOperation {
    key: Key,
    bucket: String,
    record: EncryptionTransition,
    now_ms: u64,
    step: StoreStep,
}

impl StoreTransitionOperation {
    fn new(bucket: &str, record: EncryptionTransition) -> Self {
        Self {
            key: bucket.as_bytes().to_vec().into(),
            bucket: bucket.to_string(),
            record,
            now_ms: 0,
            step: StoreStep::Current,
        }
    }

    fn done(&mut self, stored: bool) -> Turn<StoredTransition> {
        let record = self.record.clone();
        Ok(Next::Done(StoredTransition { stored, record }))
    }

    fn progress_row(&self) -> Result<(String, Key, Value), MigrationError> {
        let value = self.record.to_bytes()?.into();
        Ok((TRANSITION_KEYSPACE.to_string(), self.key.clone(), value))
    }

    fn current(&mut self, stored: Option<EncryptionTransition>) -> Turn<StoredTransition> {
        let record = &self.record;
        let owned = stored.is_some_and(|stored| {
            stored.started_at_ms == record.started_at_ms && stored.kind == record.kind
        });
        if !owned {
            return self.done(false);
        }
        if self.record.state != TransitionState::Finished {
            return self.write(Vec::new(), None);
        }
        let mut scan = SourceScan::new(&self.bucket, &self.record)?;
        let first = scan.start();
        self.step = StoreStep::Scan(Box::new(scan));
        Ok(Next::Call(first))
    }

    fn scanned(&mut self, scan: &SourceScan) -> Turn<StoredTransition> {
        let (pending, versions) = (scan.pending, scan.versions);
        if pending + versions > 0 {
            let record = &mut self.record;
            record.finished_at_ms = None;
            record.cursor = None;
            record.remaining = pending + versions;
            record.state = match versions > 0 {
                true => TransitionState::Running,
                false => TransitionState::Blocked,
            };
            record.blocked_reason = (versions == 0).then(|| PENDING_REASON.to_string());
            let wait = RECHECK.as_millis() as u64;
            record.retry_at_ms = Some(self.now_ms.saturating_add(wait));
        }
        let unfinished = self.record.finished_at_ms.is_none();
        if unfinished && self.record.state == TransitionState::Finished {
            if let Some(source) = retiring(&self.record) {
                self.step = StoreStep::SourceKey;
                return Ok(Next::Call(read_row(BUCKET_KEY_KEYSPACE, source.key())));
            }
        }
        self.write(Vec::new(), None)
    }

    /// No copy needs the source generation any more: it retires and its node copy is removed.
    fn source_read(&mut self, value: Option<Value>) -> Turn<StoredTransition> {
        let Some(value) = value else {
            return self.write(Vec::new(), None);
        };
        let Some(source) = self.record.source else {
            return self.write(Vec::new(), None);
        };
        let mut key = BucketKeyRecord::from_bytes(value.as_ref())?;
        let vault = key.vault_entry.take().or(self.record.source_vault);
        key.state = KeyState::Retired;
        let row = (
            BUCKET_KEY_KEYSPACE.to_string(),
            source.key().into(),
            key.to_bytes()?.into(),
        );
        let vault = vault.map(|id| VaultEntry::new(VaultPurpose::BucketKey, id));
        self.write(vec![row], vault)
    }

    fn write(
        &mut self,
        mut rows: Vec<(String, Key, Value)>,
        vault: Option<VaultEntry>,
    ) -> Turn<StoredTransition> {
        rows.insert(0, self.progress_row()?);
        self.step = StoreStep::Written(vault);
        Ok(Next::Call(IoCall::Write(rows)))
    }

    fn written(&mut self, vault: Option<VaultEntry>) -> Turn<StoredTransition> {
        if self.record.finished_at_ms.is_some() {
            self.step = StoreStep::Left;
            let row = (TRANSITION_QUEUE_KEYSPACE.to_string(), self.key.clone());
            return Ok(Next::Call(IoCall::Delete(vec![row])));
        }
        match vault {
            Some(entry) => {
                self.step = StoreStep::Vault;
                let delete = StorageEffect::VaultDelete {
                    entry,
                    txn_id: None,
                };
                Ok(Next::Call(Effect::Storage(delete).into()))
            }
            None => self.done(true),
        }
    }
}

impl Operation for StoreTransitionOperation {
    type Output = StoredTransition;
    type Error = MigrationError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Turn<StoredTransition> {
        Ok(Next::Call(read_row(TRANSITION_KEYSPACE, self.key.clone())))
    }

    fn next(&mut self, result: IoResult) -> Turn<StoredTransition> {
        match std::mem::replace(&mut self.step, StoreStep::Left) {
            StoreStep::Current => self.current(decode_transition(result.value()?)?),
            StoreStep::Scan(mut scan) => match scan.step(result)? {
                Some(call) => {
                    self.step = StoreStep::Scan(scan);
                    Ok(Next::Call(call))
                }
                None => self.scanned(&scan),
            },
            StoreStep::SourceKey => self.source_read(result.value()?),
            StoreStep::Written(vault) => {
                result.done()?;
                self.written(vault)
            }
            StoreStep::Left => {
                result.done()?;
                self.done(true)
            }
            StoreStep::Vault => match result.event()? {
                Event::Storage(
                    StorageEvent::WriteResult { .. } | StorageEvent::DeleteResult { .. },
                ) => self.done(true),
                other => Err(MigrationError::Failed(format!(
                    "could not remove the source key copy: {}",
                    other.label()
                ))),
            },
        }
    }
}

/// Uploads that captured the retiring source can never complete, so their envelopes go, one
/// page per atomic batch. Outputs how many went.
#[derive(Debug, PartialEq)]
struct ReclaimUploadsOperation {
    source: Option<BucketKeyRef>,
    pages: PrefixPages,
    count: u64,
    deleting: bool,
}

impl ReclaimUploadsOperation {
    fn new(record: &EncryptionTransition) -> Self {
        Self {
            source: retiring(record),
            pages: PrefixPages::new(ABE_PENDING_KEYSPACE, empty_prefix(), PAGE),
            count: 0,
            deleting: false,
        }
    }

    fn next_page(&mut self) -> Turn<u64> {
        match self.pages.done() {
            true => Ok(Next::Done(self.count)),
            false => Ok(Next::Call(self.pages.call())),
        }
    }
}

impl Operation for ReclaimUploadsOperation {
    type Output = u64;
    type Error = MigrationError;

    fn first(&mut self) -> Turn<u64> {
        match self.source {
            Some(_) => Ok(Next::Call(self.pages.call())),
            None => Ok(Next::Done(0)),
        }
    }

    fn next(&mut self, result: IoResult) -> Turn<u64> {
        if std::mem::take(&mut self.deleting) {
            result.done()?;
            return self.next_page();
        }
        let source = self.source;
        let deletes: Vec<_> = (self.pages.take(result)?.into_iter())
            .filter(|(_, value)| envelope_key(value) == source)
            .map(|(upload, _)| (ABE_PENDING_KEYSPACE.to_string(), upload))
            .collect();
        if deletes.is_empty() {
            return self.next_page();
        }
        self.count += deletes.len() as u64;
        self.deleting = true;
        Ok(Next::Call(IoCall::Delete(deletes)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::storage::blob::BackendRef;
    use aruna_core::structs::storage::encryption::SealPlan;
    use aruna_core::structs::storage::format::Compression;
    use aruna_core::structs::storage::format::{PithosLayout, StoredFormat};
    use aruna_core::structs::storage::transition::{TransitionKind, TransitionTarget};
    use aruna_storage::StorageHandle;
    use std::time::SystemTime;
    use ulid::Ulid;

    fn pending(key: Option<BucketKeyRef>) -> BackendLocation {
        let layout = PithosLayout {
            stored_size: 9,
            metadata_digest: [1; 32],
            storage_generation: 0,
        };
        let format = key.map_or_else(StoredFormat::default, |key| {
            StoredFormat::pithos(layout, key)
        });
        BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: String::new(),
            storage_bucket: String::new(),
            backend_path: String::new(),
            ulid: Ulid::from_bytes([2; 16]),
            format,
            created_by: Default::default(),
            created_at: SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 9,
            hashes: Default::default(),
        }
    }

    #[test]
    fn pending_keeps_source() {
        let source = BucketKeyRef::new(Ulid::from_bytes([3; 16]), 1);
        let newer = BucketKeyRef::new(source.bucket_id, 2);
        let other = BucketKeyRef::new(Ulid::from_bytes([4; 16]), 1);
        let locations = [
            pending(Some(source)),
            pending(Some(newer)),
            pending(Some(other)),
            pending(None),
            pending(Some(source)),
        ];
        let target = TransitionTarget {
            compression: Compression::Off,
            plan: None,
        };
        let record = EncryptionTransition::new(TransitionKind::Decrypt, Some(source), target, 2, 1);
        assert_eq!(uses_key(&locations, source, &record), 2);
        assert_eq!(uses_key(&locations[1..4], source, &record), 0);
    }

    async fn settle(
        context: &DriverContext,
        bucket: &str,
        record: EncryptionTransition,
    ) -> Result<Option<Duration>, MigrationError> {
        drive(SettleTransitionOperation::new(bucket, record), context).await
    }

    async fn read_record(
        storage: &StorageHandle,
        bucket: &str,
    ) -> Result<Option<EncryptionTransition>, String> {
        let read = StorageEffect::Read {
            key_space: TRANSITION_KEYSPACE.to_string(),
            key: bucket.as_bytes().to_vec().into(),
            txn_id: None,
        };
        match storage.send_storage_effect(read).await {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => {
                decode_transition(value).map_err(|error| error.to_string())
            }
            other => Err(other.label().to_string()),
        }
    }

    async fn exists(
        storage: &StorageHandle,
        key_space: &str,
        key: Vec<u8>,
        txn_id: Option<aruna_core::types::TxnId>,
    ) -> Result<bool, String> {
        let read = StorageEffect::Read {
            key_space: key_space.to_string(),
            key: key.into(),
            txn_id,
        };
        match storage.send_storage_effect(read).await {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value.is_some()),
            other => Err(other.label().to_string()),
        }
    }

    /// Runs a [`SourceScan`] alone, in a snapshot of its own.
    #[derive(Debug, PartialEq)]
    struct ScanOperation(SourceScan);

    impl Operation for ScanOperation {
        type Output = u64;
        type Error = MigrationError;

        fn transaction(&self) -> TransactionMode {
            TransactionMode::ReadSnapshot
        }

        fn first(&mut self) -> Turn<u64> {
            Ok(Next::Call(self.0.start()))
        }

        fn next(&mut self, result: IoResult) -> Turn<u64> {
            match self.0.step(result)? {
                Some(call) => Ok(Next::Call(call)),
                None => Ok(Next::Done(self.0.pending)),
            }
        }
    }

    fn transition() -> EncryptionTransition {
        let source = BucketKeyRef::new(Ulid::from_bytes([3; 16]), 1);
        let target = TransitionTarget {
            compression: Compression::Off,
            plan: None,
        };
        EncryptionTransition::new(TransitionKind::Decrypt, Some(source), target, 2, 1)
    }

    fn stored_child(record: EncryptionTransition) -> IoResult {
        let stored = StoredTransition {
            stored: true,
            record,
        };
        let outcome: Result<StoredTransition, MigrationError> = Ok(stored);
        IoResult::Child(ChildOutcome::new(outcome))
    }

    #[test]
    fn empty_queue_ends() {
        let mut process = ProcessTransitionsOperation::new(None);
        assert!(matches!(
            process.first(),
            Ok(Next::Call(IoCall::Page { .. }))
        ));
        let page = IoResult::Page {
            rows: Vec::new(),
            next: None,
        };
        assert_eq!(process.next(page), Ok(Next::Done(None)));
    }

    #[test]
    fn waiting_transition_skipped() {
        let mut process = ProcessTransitionsOperation::new(None);
        process.stamp(1_000);
        process.first().unwrap();
        let row = (Key::from(b"b".to_vec()), Value::from(Vec::new()));
        let page = IoResult::Page {
            rows: vec![row],
            next: None,
        };
        assert!(matches!(
            process.next(page),
            Ok(Next::Call(IoCall::Read(_)))
        ));
        let mut record = transition();
        record.retry_at_ms = Some(3_000);
        let value = Value::from(record.to_bytes().unwrap());
        let wait = Duration::from_secs(2);
        let answer = IoResult::Values(vec![Some(value)]);
        assert_eq!(process.next(answer), Ok(Next::Done(Some(wait))));
    }

    #[test]
    fn empty_pass_settles() {
        let mut advance = AdvanceTransitionOperation::new("b".to_string(), transition(), None);
        assert!(matches!(
            advance.first(),
            Ok(Next::Call(IoCall::Page { .. }))
        ));
        let page = IoResult::Page {
            rows: Vec::new(),
            next: None,
        };
        let turn = advance.next(page);
        assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
        assert_eq!(advance.record.state, TransitionState::Cleanup);
    }

    #[test]
    fn failed_pass_retries() {
        let mut advance = AdvanceTransitionOperation::new("b".to_string(), transition(), None);
        advance.stamp(5);
        advance.first().unwrap();
        advance.record.failed = 1;
        let page = IoResult::Page {
            rows: Vec::new(),
            next: None,
        };
        assert!(matches!(
            advance.next(page),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        assert_eq!(advance.record.retries, 1);
        let wait = RECHECK.saturating_mul(2);
        let answer = stored_child(advance.record.clone());
        assert_eq!(advance.next(answer), Ok(Next::Done(Some(wait))));
    }

    #[test]
    fn replaced_record_kept() {
        let mut store = StoreTransitionOperation::new("b", transition());
        store.first().unwrap();
        let answer = IoResult::Values(vec![None]);
        let Ok(Next::Done(stored)) = store.next(answer) else {
            panic!("a missing record ends the store");
        };
        assert!(!stored.stored);
    }

    #[test]
    fn present_copy_kept() {
        let key = Key::from(vec![0, 1]);
        let mut forget = ForgetRemovedOperation::new(1, vec![key]);
        assert!(matches!(
            forget.first(),
            Ok(Next::Call(IoCall::Page { .. }))
        ));
        let page = IoResult::Page {
            rows: Vec::new(),
            next: None,
        };
        assert!(matches!(forget.next(page), Ok(Next::Call(IoCall::Read(_)))));
        let answer = IoResult::Values(vec![Some(Value::from(vec![1]))]);
        assert_eq!(forget.next(answer), Ok(Next::Done(1)));
    }

    #[test]
    fn unretired_reclaims_none() {
        let mut record = transition();
        record.source = None;
        let mut reclaim = ReclaimUploadsOperation::new(&record);
        assert_eq!(reclaim.first(), Ok(Next::Done(0)));
    }

    fn context(root: &std::path::Path) -> DriverContext {
        DriverContext {
            storage_handle: aruna_storage::FjallStorage::open(root.to_str().unwrap()).unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        }
    }

    async fn put(context: &DriverContext, key_space: &str, key: Vec<u8>, value: Vec<u8>) {
        let write = StorageEffect::Write {
            key_space: key_space.to_string(),
            key: key.into(),
            value: value.into(),
            txn_id: None,
        };
        let event = context.storage_handle.send_storage_effect(write).await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    #[tokio::test]
    async fn pending_target_settles() {
        use aruna_core::structs::storage::format::StoredLayout;
        for generation in [1, 2] {
            let directory = tempfile::tempdir().unwrap();
            let context = context(directory.path());
            let source = BucketKeyRef::new(Ulid::from_bytes([3; 16]), 1);
            let plan = SealPlan {
                key: source,
                public_key: [4; 32],
                cipher: Default::default(),
                block_keys: Default::default(),
                storage_generation: 2,
            };
            let target = TransitionTarget {
                compression: Compression::Off,
                plan: Some(plan),
            };
            let mut record =
                EncryptionTransition::new(TransitionKind::Reencode, Some(source), target, 2, 1);
            record.state = TransitionState::Cleanup;
            put(
                &context,
                TRANSITION_KEYSPACE,
                b"b".to_vec(),
                record.to_bytes().unwrap(),
            )
            .await;
            put(
                &context,
                TRANSITION_QUEUE_KEYSPACE,
                b"b".to_vec(),
                Vec::new(),
            )
            .await;
            let mut location = pending(Some(source));
            let StoredLayout::Pithos(layout) = &mut location.format.layout else {
                panic!("expected Pithos")
            };
            layout.storage_generation = generation;
            let archive = aruna_core::structs::storage::blob::ArchiveKey::of(&location);
            put(
                &context,
                PENDING_LOCATION_KEYSPACE,
                archive.to_bytes(),
                location.to_bytes().unwrap(),
            )
            .await;
            let wait = settle(&context, "b", record).await.unwrap();
            let stored = read_record(&context.storage_handle, "b")
                .await
                .unwrap()
                .unwrap();
            if generation == 2 {
                assert_eq!(wait, None);
                assert_eq!(stored.reported_state(), TransitionState::Finished);
                assert!(stored.finished_at_ms.is_some());
            } else {
                assert_eq!(wait, Some(RECHECK));
                assert_eq!(
                    (stored.state, stored.remaining, stored.finished_at_ms),
                    (TransitionState::Blocked, 1, None)
                );
            }
        }
    }

    #[tokio::test]
    async fn delete_failure_retained() {
        let directory = tempfile::tempdir().unwrap();
        let context = context(directory.path());
        let target = TransitionTarget {
            compression: Compression::Off,
            plan: None,
        };
        let mut record = EncryptionTransition::new(TransitionKind::Decrypt, None, target, 2, 1);
        record.state = TransitionState::Cleanup;
        put(
            &context,
            TRANSITION_KEYSPACE,
            b"b".to_vec(),
            record.to_bytes().unwrap(),
        )
        .await;
        let mut old = pending(None);
        let hash = aruna_core::structs::checksum::HASH_BLAKE3.to_string();
        old.hashes.insert(hash, vec![7; 32]);
        let old_key = old.location_key().unwrap().to_bytes();
        let cleanup = aruna_core::structs::storage::transition::cleanup_key("b", &old_key);
        put(&context, TRANSITION_CLEANUP_KEYSPACE, cleanup, Vec::new()).await;
        // Reclaim removed the location row, but the backend delete is still queued or failed.
        let work = BlobCleanupWork::DeleteBlob { location: old }
            .to_bytes()
            .unwrap();
        let work_key = Ulid::generate().to_bytes().to_vec();
        put(&context, BLOB_CLEANUP_KEYSPACE, work_key.clone(), work).await;

        let wait = settle(&context, "b", record.clone()).await.unwrap();

        assert_eq!(wait, Some(RECHECK));
        let stored = read_record(&context.storage_handle, "b")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.cleanup_remaining, 1);
        assert_eq!(stored.finished_at_ms, None);

        // Only a finished delete removes the work row; then the transition may finish.
        let delete = StorageEffect::Delete {
            key_space: BLOB_CLEANUP_KEYSPACE.to_string(),
            key: work_key.into(),
            txn_id: None,
        };
        context.storage_handle.send_storage_effect(delete).await;
        assert_eq!(settle(&context, "b", stored).await.unwrap(), None);
        let stored = read_record(&context.storage_handle, "b")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.reported_state(), TransitionState::Finished);
    }

    #[tokio::test]
    async fn promoted_source_retried() {
        let directory = tempfile::tempdir().unwrap();
        let context = context(directory.path());
        let source = BucketKeyRef::new(Ulid::from_bytes([3; 16]), 1);
        let mut key = BucketKeyRecord::new(source, Ulid::nil(), [4; 32], 0);
        key.state = KeyState::Retiring;
        put(
            &context,
            BUCKET_KEY_KEYSPACE,
            source.key(),
            key.to_bytes().unwrap(),
        )
        .await;
        let target = TransitionTarget {
            compression: Compression::Off,
            plan: None,
        };
        let mut record =
            EncryptionTransition::new(TransitionKind::Decrypt, Some(source), target, 2, 1);
        record.state = TransitionState::Cleanup;
        put(
            &context,
            TRANSITION_KEYSPACE,
            b"b".to_vec(),
            record.to_bytes().unwrap(),
        )
        .await;
        put(
            &context,
            TRANSITION_QUEUE_KEYSPACE,
            b"b".to_vec(),
            Vec::new(),
        )
        .await;
        let mut location = pending(Some(source));
        location.hashes.insert("blake3".to_string(), vec![7; 32]);
        let version = BlobVersion::materialized(
            [7; 32],
            location.backend.clone(),
            location.format.encoding(),
            SystemTime::UNIX_EPOCH,
            Default::default(),
            None,
        );
        let version_key = VersionKey::new("b", "promoted", Ulid::generate())
            .to_bytes()
            .unwrap();
        put(
            &context,
            BLOB_VERSIONS_KEYSPACE,
            version_key.clone(),
            version.to_bytes().unwrap(),
        )
        .await;
        put(
            &context,
            BLOB_LOCATIONS_KEYSPACE,
            location.location_key().unwrap().to_bytes(),
            location.to_bytes().unwrap(),
        )
        .await;
        assert_eq!(settle(&context, "b", record).await.unwrap(), Some(RECHECK));
        let stored = read_record(&context.storage_handle, "b")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (stored.state, stored.remaining, stored.finished_at_ms),
            (TransitionState::Running, 1, None)
        );
        assert!(
            exists(
                &context.storage_handle,
                TRANSITION_QUEUE_KEYSPACE,
                b"b".to_vec(),
                None
            )
            .await
            .unwrap()
        );
        let read = StorageEffect::Read {
            key_space: BUCKET_KEY_KEYSPACE.to_string(),
            key: source.key().into(),
            txn_id: None,
        };
        let Event::Storage(StorageEvent::ReadResult {
            value: Some(value), ..
        }) = context.storage_handle.send_storage_effect(read).await
        else {
            panic!("source key missing")
        };
        assert_eq!(
            BucketKeyRecord::from_bytes(&value).unwrap().state,
            KeyState::Retiring
        );
        context
            .storage_handle
            .send_storage_effect(StorageEffect::Delete {
                key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                key: version_key.into(),
                txn_id: None,
            })
            .await;
        assert_eq!(settle(&context, "b", stored).await.unwrap(), None);
    }

    #[tokio::test]
    async fn promoted_reencode_retried() {
        use aruna_core::calls::{IoResult, Next};
        use aruna_core::operation::Operation;
        use aruna_core::structs::storage::blob::ArchiveKey;
        use aruna_core::structs::storage::format::StoredLayout;
        for kind in [
            TransitionKind::Reencode,
            TransitionKind::Encrypt,
            TransitionKind::Rotate,
            TransitionKind::Decrypt,
        ] {
            let directory = tempfile::tempdir().unwrap();
            let context = context(directory.path());
            let source = BucketKeyRef::new(Ulid::from_bytes([3; 16]), 1);
            let plan = SealPlan {
                key: if kind == TransitionKind::Rotate {
                    BucketKeyRef::new(source.bucket_id, 2)
                } else {
                    source
                },
                public_key: [4; 32],
                cipher: Default::default(),
                block_keys: Default::default(),
                storage_generation: 2,
            };
            let target = TransitionTarget {
                compression: Compression::Off,
                plan: (kind != TransitionKind::Decrypt).then_some(plan),
            };
            let mut record = EncryptionTransition::new(
                kind,
                (kind != TransitionKind::Encrypt).then_some(source),
                target,
                2,
                1,
            );
            let mut location = pending(Some(source));
            let version_key = VersionKey::new("b", "promoted", Ulid::generate());
            let version = BlobVersion::pending(
                ArchiveKey::of(&location),
                SystemTime::UNIX_EPOCH,
                Default::default(),
                None,
            );
            let mut rewrite = RewriteVersionOperation::new(
                version_key.clone(),
                record.clone(),
                SystemTime::UNIX_EPOCH,
            );
            rewrite.first().unwrap();
            let read = IoResult::Values(vec![Some(version.to_bytes().unwrap().into())]);
            let step = rewrite.next(read);
            assert!(matches!(
                step,
                Ok(Next::Then {
                    output: RewriteOutcome::Skipped,
                    ..
                })
            ));
            let pending_key = ArchiveKey::of(&location).to_bytes();
            put(
                &context,
                PENDING_LOCATION_KEYSPACE,
                pending_key.clone(),
                location.to_bytes().unwrap(),
            )
            .await;
            record.state = TransitionState::Cleanup;
            put(
                &context,
                TRANSITION_KEYSPACE,
                b"b".to_vec(),
                record.to_bytes().unwrap(),
            )
            .await;
            put(
                &context,
                TRANSITION_QUEUE_KEYSPACE,
                b"b".to_vec(),
                Vec::new(),
            )
            .await;
            location.hashes.insert("blake3".to_string(), vec![7; 32]);
            let version = BlobVersion::materialized(
                [7; 32],
                location.backend.clone(),
                location.format.encoding(),
                SystemTime::UNIX_EPOCH,
                Default::default(),
                None,
            );
            put(
                &context,
                BLOB_VERSIONS_KEYSPACE,
                version_key.to_bytes().unwrap(),
                version.to_bytes().unwrap(),
            )
            .await;
            put(
                &context,
                BLOB_LOCATIONS_KEYSPACE,
                location.location_key().unwrap().to_bytes(),
                location.to_bytes().unwrap(),
            )
            .await;
            context
                .storage_handle
                .send_storage_effect(StorageEffect::Delete {
                    key_space: PENDING_LOCATION_KEYSPACE.to_string(),
                    key: pending_key.into(),
                    txn_id: None,
                })
                .await;
            assert_eq!(settle(&context, "b", record).await.unwrap(), Some(RECHECK));
            let stored = read_record(&context.storage_handle, "b")
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                (stored.state, stored.remaining, stored.finished_at_ms),
                (TransitionState::Running, 1, None)
            );
            assert!(
                exists(
                    &context.storage_handle,
                    TRANSITION_QUEUE_KEYSPACE,
                    b"b".to_vec(),
                    None
                )
                .await
                .unwrap()
            );
            if kind == TransitionKind::Decrypt {
                location.format = StoredFormat::default();
            } else {
                let StoredLayout::Pithos(layout) = &mut location.format.layout else {
                    panic!("expected Pithos")
                };
                layout.storage_generation = 2;
                location.format.encryption =
                    aruna_core::structs::storage::format::StoredEncryption::Pithos(Box::new(
                        plan.key,
                    ));
            }
            put(
                &context,
                BLOB_LOCATIONS_KEYSPACE,
                location.location_key().unwrap().to_bytes(),
                location.to_bytes().unwrap(),
            )
            .await;
            let version = BlobVersion::materialized(
                [7; 32],
                location.backend.clone(),
                location.format.encoding(),
                SystemTime::UNIX_EPOCH,
                Default::default(),
                None,
            );
            put(
                &context,
                BLOB_VERSIONS_KEYSPACE,
                version_key.to_bytes().unwrap(),
                version.to_bytes().unwrap(),
            )
            .await;
            assert_eq!(settle(&context, "b", stored).await.unwrap(), None);
        }
    }

    #[test]
    fn retired_sessions_only() {
        let source = BucketKeyRef::new(Ulid::from_bytes([3; 16]), 1);
        let newer = BucketKeyRef::new(source.bucket_id, 2);
        let status = |key, seed| UnlockStatus {
            key,
            session_id: Ulid::from_bytes([seed; 16]),
            sequence: ulid::Ulid::from_parts(1, 1),
            deadline_ms: None,
            active: true,
            unlocked_at: SystemTime::UNIX_EPOCH,
            remaining: None,
            max_remaining: None,
        };
        let generations = [status(source, 5), status(newer, 6)];
        let plan = |key| SealPlan {
            key,
            public_key: [2; 32],
            cipher: Default::default(),
            block_keys: Default::default(),
            storage_generation: 3,
        };
        let rotate = TransitionTarget {
            compression: Compression::Off,
            plan: Some(plan(newer)),
        };
        let kind = TransitionKind::Rotate;
        let rotation = EncryptionTransition::new(kind, Some(source), rotate, 3, 1);
        let retired = retired_sessions(&generations, &rotation);
        assert_eq!(retired.len(), 1);
        assert_eq!(retired[0].key, source);

        // A re-encode keeps its key, so nothing retires.
        let same = TransitionTarget {
            compression: Compression::Off,
            plan: Some(plan(source)),
        };
        let kind = TransitionKind::Reencode;
        let reencode = EncryptionTransition::new(kind, Some(source), same, 3, 1);
        assert!(retired_sessions(&generations, &reencode).is_empty());
    }

    #[tokio::test]
    async fn reclaim_between_reads() {
        // A reclaim commits after the cleanup check began: the check must still see the copy.
        let directory = tempfile::tempdir().unwrap();
        let context = context(directory.path());
        let storage = &context.storage_handle;
        let mut old = pending(None);
        let hash = aruna_core::structs::checksum::HASH_BLAKE3.to_string();
        old.hashes.insert(hash, vec![7; 32]);
        let old_key = old.location_key().unwrap().to_bytes();
        let cleanup = aruna_core::structs::storage::transition::cleanup_key("b", &old_key);
        put(
            &context,
            TRANSITION_CLEANUP_KEYSPACE,
            cleanup.clone(),
            Vec::new(),
        )
        .await;
        let row = old.to_bytes().unwrap();
        put(&context, BLOB_LOCATIONS_KEYSPACE, old_key.clone(), row).await;
        let delete = StorageEffect::Delete {
            key_space: BLOB_LOCATIONS_KEYSPACE.to_string(),
            key: old_key.into(),
            txn_id: None,
        };
        storage.send_storage_effect(delete).await;
        let work = BlobCleanupWork::DeleteBlob { location: old }
            .to_bytes()
            .unwrap();
        put(
            &context,
            BLOB_CLEANUP_KEYSPACE,
            Ulid::generate().to_bytes().to_vec(),
            work,
        )
        .await;
        let prefix = aruna_core::structs::storage::transition::cleanup_prefix("b").len();
        assert!(
            exists(storage, TRANSITION_CLEANUP_KEYSPACE, cleanup.clone(), None)
                .await
                .unwrap()
        );
        // A later check sees the queued delete instead and still keeps the copy.
        let forget = ForgetRemovedOperation::new(prefix, vec![cleanup.into()]);
        let left = drive(forget, &context).await;
        assert_eq!(left, Ok(1));
    }

    fn source_envelope(source: BucketKeyRef) -> ObjectEnvelope {
        use aruna_core::compute::SecretBytes;
        use aruna_core::structs::storage::abe::{EnvelopePlan, create_envelope, create_parameters};
        let private = SecretBytes::new(vec![9; 32]);
        let realm = RealmId::from_bytes([3; 32]);
        let node = iroh::SecretKey::from_bytes(&[7; 32]).public();
        let parameters = create_parameters(&private, realm, node, source);
        let plan = EnvelopePlan {
            parameters: parameters.unwrap(),
            epoch: 1,
            write_id: Ulid::generate(),
            object_key: "k".to_string(),
            bucket_public: [4; 32],
        };
        create_envelope(plan).unwrap().0
    }

    #[tokio::test]
    async fn retirement_pages_rows() {
        let directory = tempfile::tempdir().unwrap();
        let context = context(directory.path());
        let storage = &context.storage_handle;
        let source = BucketKeyRef::new(Ulid::from_bytes([3; 16]), 1);
        let target = TransitionTarget {
            compression: Compression::Off,
            plan: None,
        };
        let record = EncryptionTransition::new(TransitionKind::Decrypt, Some(source), target, 2, 1);
        let envelope = source_envelope(source).to_bytes().unwrap();
        let id = Ulid::generate().to_bytes().to_vec();
        put(
            &context,
            ABE_ENVELOPE_KEYSPACE,
            id.clone(),
            envelope.clone(),
        )
        .await;
        let rows = 2 * PAGE + 1;
        for _ in 0..rows {
            let version = VersionKey::new("b", "k", Ulid::generate())
                .to_bytes()
                .unwrap();
            put(&context, ABE_VERSION_KEYSPACE, version, id.clone()).await;
            let upload = Ulid::generate().to_bytes().to_vec();
            put(&context, ABE_PENDING_KEYSPACE, upload, envelope.clone()).await;
        }
        let scan = SourceScan::new("b", &record).unwrap();
        let counted = drive(ScanOperation(scan), &context).await;
        let reclaimed = drive(ReclaimUploadsOperation::new(&record), &context).await;
        let (left, _) = aruna_storage::scan::iter_prefix_page(
            storage,
            ABE_PENDING_KEYSPACE,
            None,
            None,
            PAGE,
            None,
        )
        .await
        .unwrap();

        // Both walks cross two page boundaries; reclaim deletes one page per batch.
        assert_eq!(counted, Ok(rows as u64));
        assert_eq!(reclaimed, Ok(rows as u64));
        assert!(left.is_empty());
    }

    #[tokio::test]
    async fn retirement_waits_envelopes() {
        let directory = tempfile::tempdir().unwrap();
        let context = context(directory.path());
        let source = BucketKeyRef::new(Ulid::from_bytes([3; 16]), 1);
        let mut key = BucketKeyRecord::new(source, Ulid::nil(), [4; 32], 0);
        key.state = KeyState::Retiring;
        let key_row = key.to_bytes().unwrap();
        put(&context, BUCKET_KEY_KEYSPACE, source.key(), key_row).await;
        let plan = SealPlan {
            key: BucketKeyRef::new(source.bucket_id, 2),
            public_key: [4; 32],
            cipher: Default::default(),
            block_keys: Default::default(),
            storage_generation: 2,
        };
        let target = TransitionTarget {
            compression: Compression::Off,
            plan: Some(plan),
        };
        let mut record =
            EncryptionTransition::new(TransitionKind::Rotate, Some(source), target, 2, 1);
        record.state = TransitionState::Cleanup;
        let row = record.to_bytes().unwrap();
        put(&context, TRANSITION_KEYSPACE, b"b".to_vec(), row).await;
        put(
            &context,
            TRANSITION_QUEUE_KEYSPACE,
            b"b".to_vec(),
            Vec::new(),
        )
        .await;
        let envelope = source_envelope(source);
        // An unfinished upload that captured the source never completes and never blocks.
        let upload = Ulid::generate().to_bytes().to_vec();
        let upload_row = envelope.to_bytes().unwrap();
        put(&context, ABE_PENDING_KEYSPACE, upload.clone(), upload_row).await;
        // A pending copy still needs the source key's recovery wrap.
        let copy = PendingCopy {
            source: envelope,
            archive: aruna_core::structs::storage::blob::ArchiveKey::of(&pending(Some(source))),
        };
        let version = VersionKey::new("b", "copy", Ulid::generate());
        let copy_row = crate::s3::abe_copies::copy_row(source.bucket_id, &version).unwrap();
        let value = copy.to_bytes().unwrap();
        put(&context, ABE_COPY_KEYSPACE, copy_row.clone(), value).await;
        let storage = &context.storage_handle;

        assert_eq!(settle(&context, "b", record).await.unwrap(), Some(RECHECK));
        let stored = read_record(storage, "b").await.unwrap().unwrap();
        assert_eq!(
            (stored.state, stored.remaining),
            (TransitionState::Blocked, 1)
        );

        let delete = StorageEffect::Delete {
            key_space: ABE_COPY_KEYSPACE.to_string(),
            key: copy_row.into(),
            txn_id: None,
        };
        storage.send_storage_effect(delete).await;
        assert_eq!(settle(&context, "b", stored).await.unwrap(), None);
        let stored = read_record(storage, "b").await.unwrap().unwrap();
        assert_eq!(stored.reported_state(), TransitionState::Finished);
        let read = StorageEffect::Read {
            key_space: BUCKET_KEY_KEYSPACE.to_string(),
            key: source.key().into(),
            txn_id: None,
        };
        let Event::Storage(StorageEvent::ReadResult {
            value: Some(row), ..
        }) = storage.send_storage_effect(read).await
        else {
            panic!("source key missing")
        };
        assert_eq!(
            BucketKeyRecord::from_bytes(&row).unwrap().state,
            KeyState::Retired
        );
        assert!(
            !exists(storage, ABE_PENDING_KEYSPACE, upload, None)
                .await
                .unwrap()
        );
    }
}
