//! Bootstraps a catching-up member of a distributed bucket from the coordinators of its ranges.
//! Each range is walked over the ordered inventory in bounded pages with a durable cursor; a page
//! waits until its jobs were delivered, live publications overlap the walk, and a coordinator
//! whose walks for the member all ended states the catch-up to the record's publisher.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::effects::{Effect, IterEnd, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StatementSignEvent, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{
    BOOTSTRAP_KEYSPACE, CATCH_UP_KEYSPACE, INVENTORY_KEYSPACE, REPLICATION_JOB_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::distribution::bootstrap::{
    BootstrapCursor, bootstrap_key, bootstrap_prefix,
};
use aruna_core::structs::storage::distribution::inventory::{
    Entry, Position, VersionEntry, range_interval,
};
use aruna_core::structs::storage::distribution::{
    DistributionMember, DistributionSnapshot, KeyRange, MAX_RANGES, Statement, StatementClaim,
};
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::{Effects, Key, TxnId, Value};
use aruna_storage::StorageHandle;
use smallvec::smallvec;
use thiserror::Error;
use tracing::{debug, warn};
use ulid::Ulid;

use super::members::{MemberTarget, read_record};
use super::protocol::ReplicationMode;
use super::queue::{BlobJobRecord, blob_job_key, schedule_blob_drain};
use super::version_replication::{ReplicateScopeInput, ReplicateScopeTarget};
use crate::distribution::create::submit;
use crate::distribution::sign_statement;
use crate::driver::{DriverContext, drive};
use crate::s3::bucket::get::GetBucketOperation;

/// Inventory rows one page queues at most; the next page waits until they are delivered.
const PAGE_ROWS: usize = 64;
const PAGE_BYTES: usize = 1 << 20;
/// Sets and pages one drain works on at most.
const SET_BATCH: usize = 16;
const PAGE_BUDGET: usize = 32;
/// Rows one drain deletes at most while it clears the cursors of a settled set.
const CLEAR_BATCH: usize = 1024;
/// How soon the drain looks again while a bootstrap waits for its jobs.
pub const BOOTSTRAP_POLL: Duration = Duration::from_secs(5);

/// One page of one range's walk for one catching-up member.
#[derive(Clone, Debug, PartialEq)]
pub struct PageConfig {
    pub realm_id: RealmId,
    pub bucket: String,
    pub set_id: Ulid,
    pub member_id: Ulid,
    pub node_id: NodeId,
    pub intent: [u8; 32],
    pub range: KeyRange,
    pub now_ms: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PageProgress {
    /// The previous page is still being delivered.
    Waiting,
    Queued(usize),
    /// Everything up to the watermark was delivered.
    Done {
        watermark: u64,
    },
}

#[derive(Debug, Error, PartialEq)]
pub enum BootstrapError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("operation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: got {got}")]
    UnexpectedEvent { state: &'static str, got: String },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PageState {
    Init,
    Start,
    ReadCursor,
    ReadPending,
    ReadPage,
    Write,
    Commit,
    Abort,
    Finish,
    Error,
}

/// Advances one cursor by one page in one transaction: the cursor and the page's jobs commit
/// together, so a crash repeats at most the last page.
#[derive(Debug, PartialEq)]
pub struct BootstrapPage {
    config: PageConfig,
    state: PageState,
    txn_id: Option<TxnId>,
    cursor: BootstrapCursor,
    progress: Option<PageProgress>,
    output: Option<Result<PageProgress, BootstrapError>>,
}

impl BootstrapPage {
    pub fn new(config: PageConfig) -> Self {
        let cursor = BootstrapCursor::start(config.intent, config.now_ms);
        Self {
            config,
            state: PageState::Init,
            txn_id: None,
            cursor,
            progress: None,
            output: None,
        }
    }

    fn key(&self) -> Vec<u8> {
        let config = &self.config;
        bootstrap_key(config.set_id, config.member_id, config.range.range_id)
    }

    fn fail(&mut self, error: BootstrapError) -> Effects {
        self.state = PageState::Error;
        self.output = Some(Err(error));
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }

    fn unexpected(&mut self, event: Event) -> Effects {
        let state = match self.state {
            PageState::Start => "Start",
            PageState::ReadCursor => "ReadCursor",
            PageState::ReadPending => "ReadPending",
            PageState::ReadPage => "ReadPage",
            PageState::Write => "Write",
            PageState::Commit => "Commit",
            PageState::Abort => "Abort",
            PageState::Init | PageState::Finish | PageState::Error => "Idle",
        };
        let got = format!("{event:?}");
        self.fail(BootstrapError::UnexpectedEvent { state, got })
    }

    /// Ends without a write, keeping the cursor as it is.
    fn settle(&mut self, progress: PageProgress) -> Effects {
        self.progress = Some(progress);
        match self.txn_id.take() {
            Some(txn_id) => {
                self.state = PageState::Abort;
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            None => self.finish(),
        }
    }

    fn finish(&mut self) -> Effects {
        self.state = PageState::Finish;
        self.output = self.progress.map(Ok);
        smallvec![]
    }

    fn accept_cursor(&mut self, value: Option<Value>) -> Result<Effects, BootstrapError> {
        if let Some(stored) = value
            .map(|value| BootstrapCursor::from_bytes(&value))
            .transpose()?
        {
            // A cursor of an earlier intent belongs to a former catch-up; this one starts over.
            if stored.intent == self.config.intent {
                self.cursor = stored;
            }
        }
        if self.cursor.done {
            let watermark = self.cursor.watermark;
            return Ok(self.settle(PageProgress::Done { watermark }));
        }
        if self.cursor.pending.is_empty() {
            return self.next_page();
        }
        self.state = PageState::ReadPending;
        let reads = (self.cursor.pending.iter())
            .map(|key| (REPLICATION_JOB_KEYSPACE.to_string(), Key::from(key.clone())))
            .collect();
        Ok(smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        })])
    }

    fn accept_pending(
        &mut self,
        values: Vec<(Key, Option<Value>)>,
    ) -> Result<Effects, BootstrapError> {
        if values.iter().any(|(_, value)| value.is_some()) {
            return Ok(self.settle(PageProgress::Waiting));
        }
        self.cursor.pending.clear();
        self.next_page()
    }

    /// Reads the next page outside the transaction: live writes into the range never conflict
    /// a page, because the comparator orders whatever arrives twice.
    fn next_page(&mut self) -> Result<Effects, BootstrapError> {
        if self.cursor.walked {
            self.cursor.done = true;
            let watermark = self.cursor.watermark;
            self.progress = Some(PageProgress::Done { watermark });
            return self.write(Vec::new());
        }
        let range = &self.config.range;
        let (lower, upper) =
            range_interval(self.config.set_id, &range.lower, range.upper.as_deref());
        let start = match &self.cursor.after {
            Some(after) => IterStart::After(after.clone().into()),
            None => IterStart::At(lower.into()),
        };
        self.state = PageState::ReadPage;
        Ok(smallvec![Effect::Storage(StorageEffect::IterRange {
            key_space: INVENTORY_KEYSPACE.to_string(),
            start,
            end: IterEnd::Before(upper.into()),
            limit: PAGE_ROWS,
            max_bytes: PAGE_BYTES,
            txn_id: None,
        })])
    }

    fn accept_page(
        &mut self,
        values: Vec<(Key, Value)>,
        more: bool,
    ) -> Result<Effects, BootstrapError> {
        let mut writes = Vec::new();
        let mut pending = Vec::new();
        for (key, value) in &values {
            let position = Position::decode(key)?;
            let Entry::Version { version_id, .. } = position.entry else {
                continue;
            };
            let marker = VersionEntry::from_bytes(value)?.marker;
            let job = self.job(position.key, version_id, marker);
            let job_key = blob_job_key(&job)?;
            pending.push(job_key.to_vec());
            writes.push((
                REPLICATION_JOB_KEYSPACE.to_string(),
                job_key,
                job.to_bytes()?.into(),
            ));
        }
        if let Some((last, _)) = values.last() {
            self.cursor.after = Some(last.to_vec());
        }
        self.progress = Some(PageProgress::Queued(pending.len()));
        self.cursor.pending = pending;
        self.cursor.walked = !more;
        self.write(writes)
    }

    fn job(&self, key: String, version_id: Ulid, marker: bool) -> BlobJobRecord {
        let config = &self.config;
        let input = ReplicateScopeInput {
            bucket: config.bucket.clone(),
            target: ReplicateScopeTarget::Version { key, version_id },
            target_node_id: config.node_id,
            auth_context: AuthContext::anonymous(config.realm_id),
            replicate_delete_markers: true,
            mode: ReplicationMode::Live,
        };
        BlobJobRecord::new(input, Some(marker), config.now_ms).with_member(MemberTarget {
            set_id: config.set_id,
            member_id: config.member_id,
        })
    }

    fn write(&mut self, mut writes: Vec<(String, Key, Value)>) -> Result<Effects, BootstrapError> {
        let Some(txn_id) = self.txn_id else {
            return Err(StorageError::TransactionNotFound.into());
        };
        let cursor = self.cursor.to_bytes()?;
        writes.push((
            BOOTSTRAP_KEYSPACE.to_string(),
            self.key().into(),
            cursor.into(),
        ));
        self.state = PageState::Write;
        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        })])
    }
}

impl Operation for BootstrapPage {
    type Output = PageProgress;
    type Error = BootstrapError;

    fn start(&mut self) -> Effects {
        self.state = PageState::Start;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        let result = match (self.state, event) {
            (_, Event::Storage(StorageEvent::Error { error })) => Err(error.into()),
            (PageState::Start, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.txn_id = Some(txn_id);
                self.state = PageState::ReadCursor;
                Ok(smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: BOOTSTRAP_KEYSPACE.to_string(),
                    key: self.key().into(),
                    txn_id: Some(txn_id),
                })])
            }
            (PageState::ReadCursor, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                self.accept_cursor(value)
            }
            (PageState::ReadPending, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                self.accept_pending(values)
            }
            (
                PageState::ReadPage,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => self.accept_page(values, next_start_after.is_some()),
            (PageState::Write, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                match self.txn_id.take() {
                    Some(txn_id) => {
                        self.state = PageState::Commit;
                        Ok(smallvec![Effect::Storage(
                            StorageEffect::CommitTransaction { txn_id }
                        )])
                    }
                    None => Err(StorageError::TransactionNotFound.into()),
                }
            }
            (PageState::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. }))
            | (PageState::Abort, Event::Storage(StorageEvent::TransactionAborted { .. })) => {
                Ok(self.finish())
            }
            (_, event) => return self.unexpected(event),
        };
        result.unwrap_or_else(|error| self.fail(error))
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, PageState::Finish | PageState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(BootstrapError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }
}

/// Rows of one keyspace under `prefix`, at most `limit`.
async fn scan(
    context: &DriverContext,
    key_space: &str,
    prefix: Option<Vec<u8>>,
    limit: usize,
) -> Result<Vec<(Vec<u8>, Value)>, String> {
    let iter = StorageEffect::Iter {
        key_space: key_space.to_string(),
        prefix: prefix.map(Key::from),
        start: None,
        limit,
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(iter).await {
        Event::Storage(StorageEvent::IterResult { values, .. }) => Ok(values
            .into_iter()
            .map(|(key, value)| (key.to_vec(), value))
            .collect()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected scan answer {other:?}")),
    }
}

async fn delete(context: &DriverContext, deletes: Vec<(String, Key)>) -> Result<(), String> {
    if deletes.is_empty() {
        return Ok(());
    }
    let effect = StorageEffect::BatchDelete {
        deletes,
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(effect).await {
        Event::Storage(StorageEvent::BatchDeleteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.to_string()),
        other => Err(format!("unexpected delete answer {other:?}")),
    }
}

/// Wakes the replication drain when a set waits for its bootstrap.
pub async fn wake_bootstrap(context: &DriverContext) {
    if let Some(task) = context.task_handle.as_ref()
        && bootstrap_waiting(&context.storage_handle).await
    {
        task.send_effect(schedule_blob_drain()).await;
    }
}

/// Whether any set still waits for its bootstrap here.
pub async fn bootstrap_waiting(storage: &StorageHandle) -> bool {
    let iter = StorageEffect::Iter {
        key_space: CATCH_UP_KEYSPACE.to_string(),
        prefix: None,
        start: None,
        limit: 1,
        txn_id: None,
    };
    matches!(
        storage.send_storage_effect(iter).await,
        Event::Storage(StorageEvent::IterResult { values, .. }) if !values.is_empty()
    )
}

/// Works on the sets that name catching-up members, within the drain's page budget. Returns
/// whether some bootstrap still waits, so the drain looks again soon.
pub async fn process_bootstrap(context: &DriverContext) -> Result<bool, String> {
    let Some(net) = context.net_handle.as_ref() else {
        return Ok(false);
    };
    let (realm_id, local) = (*net.realm_id(), net.node_id());
    let mut budget = PAGE_BUDGET;
    let mut waiting = false;
    for (key, _) in scan(context, CATCH_UP_KEYSPACE, None, SET_BATCH).await? {
        let Ok(set_id) = <[u8; 16]>::try_from(key.as_slice()).map(Ulid::from_bytes) else {
            delete(context, vec![(CATCH_UP_KEYSPACE.to_string(), key.into())]).await?;
            continue;
        };
        let walk = SetWalk {
            realm_id,
            local,
            set_id,
        };
        match walk.run(context, &mut budget).await {
            Ok(Outcome::Settled) => {
                delete(context, vec![(CATCH_UP_KEYSPACE.to_string(), key.into())]).await?
            }
            Ok(Outcome::Waiting) => waiting = true,
            Ok(Outcome::Stated) => {}
            Err(error) => {
                warn!(%set_id, %error, "Distributed bucket bootstrap failed; retrying");
                waiting = true;
            }
        }
    }
    Ok(waiting)
}

/// What a drain left of one set's bootstraps on this node.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Outcome {
    /// Nothing is left to do; the set's row goes.
    Settled,
    /// A walk waits for its jobs or the drain's budget; the drain looks again soon.
    Waiting,
    /// The catch-up was stated; the record that makes the member ready settles the set.
    Stated,
}

impl Outcome {
    fn and(self, other: Self) -> Self {
        match (self, other) {
            (Self::Waiting, _) | (_, Self::Waiting) => Self::Waiting,
            (Self::Stated, _) | (_, Self::Stated) => Self::Stated,
            (Self::Settled, Self::Settled) => Self::Settled,
        }
    }
}

/// One set's bootstraps as seen by this node.
struct SetWalk {
    realm_id: RealmId,
    local: NodeId,
    set_id: Ulid,
}

impl SetWalk {
    async fn run(&self, context: &DriverContext, budget: &mut usize) -> Result<Outcome, String> {
        let Some(view) = read_record(context, self.set_id).await? else {
            return Ok(Outcome::Settled);
        };
        let snapshot = view.head.snapshot;
        let targets = snapshot.catching_up();
        let local = snapshot
            .config
            .members
            .iter()
            .find(|m| m.node_id == self.local);
        let (Some(local), false) = (local, targets.is_empty()) else {
            return self.clear(context).await;
        };
        // Only the admitted incarnation of this member's bucket publishes.
        let info = drive(
            GetBucketOperation::new(snapshot.config.bucket.clone()),
            context,
        )
        .await;
        let bound = info
            .ok()
            .and_then(|info| info.distribution)
            .is_some_and(|binding| {
                binding.member_id == local.member_id
                    && local.incarnation == Some(binding.incarnation)
            });
        if !bound {
            return self.clear(context).await;
        }
        let mut outcome = Outcome::Settled;
        for target in targets {
            let walked = self.walk(context, &snapshot, local, target, budget).await?;
            outcome = outcome.and(walked);
        }
        Ok(outcome)
    }

    /// Walks the ranges this node publishes to `target`; when every walk ended, states the
    /// catch-up.
    async fn walk(
        &self,
        context: &DriverContext,
        snapshot: &DistributionSnapshot,
        local: &DistributionMember,
        target: &DistributionMember,
        budget: &mut usize,
    ) -> Result<Outcome, String> {
        let ranges = snapshot.catch_up_ranges(local.member_id, target.member_id);
        if ranges.is_empty() {
            return Ok(Outcome::Settled);
        }
        let mut prefix = bootstrap_prefix(self.set_id);
        prefix.extend_from_slice(&target.member_id.to_bytes());
        let cursors: HashMap<Vec<u8>, BootstrapCursor> =
            (scan(context, BOOTSTRAP_KEYSPACE, Some(prefix), MAX_RANGES + 1).await?)
                .into_iter()
                .filter_map(|(key, value)| Some((key, BootstrapCursor::from_bytes(&value).ok()?)))
                .collect();
        let mut watermark = u64::MAX;
        let mut done = true;
        for range in ranges {
            let key = bootstrap_key(self.set_id, target.member_id, range.range_id);
            if let Some(cursor) = cursors.get(&key)
                && cursor.done
                && cursor.intent == target.intent
            {
                watermark = watermark.min(cursor.watermark);
                continue;
            }
            if *budget == 0 {
                done = false;
                continue;
            }
            *budget -= 1;
            let page = BootstrapPage::new(PageConfig {
                realm_id: self.realm_id,
                bucket: snapshot.config.bucket.clone(),
                set_id: self.set_id,
                member_id: target.member_id,
                node_id: target.node_id,
                intent: target.intent,
                range: range.clone(),
                now_ms: unix_timestamp_millis(),
            });
            match drive(page, context)
                .await
                .map_err(|error| error.to_string())?
            {
                PageProgress::Done { watermark: ended } => watermark = watermark.min(ended),
                PageProgress::Waiting | PageProgress::Queued(_) => done = false,
            }
        }
        if !done {
            return Ok(Outcome::Waiting);
        }
        self.state(context, snapshot, local, target, watermark)
            .await
    }

    /// Hands the signed catch-up to the publisher; the next record makes the member ready.
    async fn state(
        &self,
        context: &DriverContext,
        snapshot: &DistributionSnapshot,
        local: &DistributionMember,
        target: &DistributionMember,
        watermark: u64,
    ) -> Result<Outcome, String> {
        let statement = Statement::caught_up(self.realm_id, self.set_id, local, target, watermark);
        let claim = StatementClaim {
            signer: self.local,
            statement,
        };
        let signed = match sign_statement(context, claim) {
            StatementSignEvent::Signed(signed) => *signed,
            StatementSignEvent::Unavailable(reason) => return Err(reason),
        };
        let publisher = snapshot.root.publisher;
        let shared = Arc::new(context.clone());
        match submit(&shared, publisher, self.local, signed).await {
            Ok(()) => Ok(Outcome::Stated),
            Err(refusal) => {
                debug!(set_id = %self.set_id, member_id = %target.member_id, %refusal, "Catch-up statement not taken yet");
                Ok(Outcome::Waiting)
            }
        }
    }

    /// Deletes the set's cursors in bounded batches; settled once none is left.
    async fn clear(&self, context: &DriverContext) -> Result<Outcome, String> {
        let prefix = bootstrap_prefix(self.set_id);
        let rows = scan(context, BOOTSTRAP_KEYSPACE, Some(prefix), CLEAR_BATCH).await?;
        let settled = rows.len() < CLEAR_BATCH;
        let deletes = (rows.into_iter())
            .map(|(key, _)| (BOOTSTRAP_KEYSPACE.to_string(), Key::from(key)))
            .collect();
        delete(context, deletes).await?;
        Ok(match settled {
            true => Outcome::Settled,
            false => Outcome::Waiting,
        })
    }
}

#[cfg(test)]
#[path = "bootstrap_tests.rs"]
mod tests;
