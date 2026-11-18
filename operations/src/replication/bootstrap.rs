//! Bootstraps a catching-up member or a new range copy from its coordinators: bounded pages over
//! the ordered inventory with a durable cursor, each waiting for its jobs while publications go on.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, IterEnd, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{BOOTSTRAP_KEYSPACE, INVENTORY_KEYSPACE, REPLICATION_JOB_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::distribution::KeyRange;
use aruna_core::structs::storage::distribution::bootstrap::{
    BootstrapCursor, bootstrap_key, bootstrap_prefix,
};
use aruna_core::structs::storage::distribution::inventory::{
    Entry, Position, VersionEntry, range_interval,
};
use aruna_core::structs::storage::replication_queue::{
    BlobJobRecord, MemberTarget, ReplicateScopeInput, ReplicateScopeTarget, ReplicationMode,
    blob_job_key,
};
use aruna_core::types::{Key, Value};
use thiserror::Error;
use ulid::Ulid;

/// Inventory rows one page queues at most, one reference page; the next page waits until they
/// are delivered.
pub(super) const PAGE_ROWS: usize = super::page::PAGE_RECORDS;

const PAGE_BYTES: usize = 8 << 20;

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
    /// Inventory rows the page reads at most.
    pub rows: usize,
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
    #[error(transparent)]
    Io(IoError),
}

impl From<IoError> for BootstrapError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::Storage(error),
            other => Self::Io(other),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PageState {
    ReadCursor,
    ReadPending,
    ReadPage,
    Write,
}

type Step = Result<Next<PageProgress>, BootstrapError>;

/// Advances one cursor by one page. Its jobs and page are read outside any transaction, so live
/// writes never conflict it; cursor and jobs commit in a child that checks the cursor is
/// unchanged, so a crash repeats at most the last page.
#[derive(Debug, PartialEq)]
pub struct BootstrapPage {
    config: PageConfig,
    state: PageState,
    cursor: BootstrapCursor,
    /// The cursor row as read, which the store checks before it writes.
    seen: Option<Value>,
    progress: Option<PageProgress>,
}

impl BootstrapPage {
    pub fn new(config: PageConfig) -> Self {
        let cursor = BootstrapCursor::start(config.intent, config.now_ms);
        Self {
            config,
            state: PageState::ReadCursor,
            cursor,
            seen: None,
            progress: None,
        }
    }

    fn key(&self) -> Vec<u8> {
        let config = &self.config;
        bootstrap_key(config.set_id, config.member_id, config.range.range_id)
    }

    fn accept_cursor(&mut self, value: Option<Value>) -> Step {
        self.seen = value.clone();
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
            return Ok(Next::Done(PageProgress::Done { watermark }));
        }
        if self.cursor.pending.is_empty() {
            return self.next_page();
        }
        self.state = PageState::ReadPending;
        let reads = (self.cursor.pending.iter())
            .map(|key| (REPLICATION_JOB_KEYSPACE.to_string(), Key::from(key.clone())))
            .collect();
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn accept_pending(&mut self, values: Vec<Option<Value>>) -> Step {
        if values.iter().any(Option::is_some) {
            return Ok(Next::Done(PageProgress::Waiting));
        }
        self.cursor.pending.clear();
        self.next_page()
    }

    /// Reads the next page; live writes into the range never conflict a page, because the
    /// comparator orders whatever arrives twice.
    fn next_page(&mut self) -> Step {
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
        let page = StorageEffect::IterRange {
            key_space: INVENTORY_KEYSPACE.to_string(),
            start,
            end: IterEnd::Before(upper.into()),
            limit: self.config.rows,
            max_bytes: PAGE_BYTES,
            txn_id: None,
        };
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::Storage(page)))))
    }

    fn accept_page(&mut self, values: Vec<(Key, Value)>, more: bool) -> Step {
        let mut writes = Vec::new();
        let mut pending = Vec::new();
        for (key, value) in &values {
            let position = Position::decode(key)?;
            let Entry::Version { version_id, .. } = position.entry else {
                continue;
            };
            let entry = VersionEntry::from_bytes(value)?;
            self.cursor.versions += 1;
            self.cursor.bytes = self.cursor.bytes.saturating_add(entry.size);
            let job = self.job(position.key, version_id, entry.marker);
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

    fn write(&mut self, mut writes: Vec<(String, Key, Value)>) -> Step {
        let cursor = self.cursor.to_bytes()?;
        let key: Key = self.key().into();
        writes.push((BOOTSTRAP_KEYSPACE.to_string(), key.clone(), cursor.into()));
        self.state = PageState::Write;
        let store = StorePageOperation {
            key,
            seen: self.seen.take(),
            writes,
            written: false,
        };
        Ok(Next::Call(IoCall::child(store)))
    }

    fn storage_answer(result: IoResult) -> Result<StorageEvent, BootstrapError> {
        match result.event()? {
            Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
            Event::Storage(event) => Ok(event),
            other => Err(IoError::Unexpected {
                expected: "a storage event",
                got: other.label(),
            }
            .into()),
        }
    }
}

impl Operation for BootstrapPage {
    type Output = PageProgress;
    type Error = BootstrapError;

    fn first(&mut self) -> Step {
        let read = IoCall::Read(vec![(BOOTSTRAP_KEYSPACE.to_string(), self.key().into())]);
        Ok(Next::Call(read))
    }

    fn next(&mut self, result: IoResult) -> Step {
        match self.state {
            PageState::ReadCursor => self.accept_cursor(result.value()?),
            PageState::ReadPending => self.accept_pending(result.values()?),
            PageState::ReadPage => match Self::storage_answer(result)? {
                StorageEvent::IterResult {
                    values,
                    next_start_after,
                } => self.accept_page(values, next_start_after.is_some()),
                other => Err(unexpected("StorageEvent::IterResult", other)),
            },
            PageState::Write => {
                result.child::<(), BootstrapError>()??;
                let progress = self.progress.ok_or(IoError::Unfinished)?;
                Ok(Next::Done(progress))
            }
        }
    }
}

/// Writes a page's jobs and its cursor in one transaction, while the cursor row is still the
/// one the page was read from; another run that moved it meanwhile makes this a conflict.
#[derive(Debug, PartialEq)]
struct StorePageOperation {
    key: Key,
    seen: Option<Value>,
    writes: Vec<(String, Key, Value)>,
    written: bool,
}

impl Operation for StorePageOperation {
    type Output = ();
    type Error = BootstrapError;

    fn first(&mut self) -> Result<Next<()>, BootstrapError> {
        let read = (BOOTSTRAP_KEYSPACE.to_string(), self.key.clone());
        Ok(Next::Call(IoCall::Read(vec![read])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, BootstrapError> {
        if self.written {
            result.done()?;
            return Ok(Next::Done(()));
        }
        if result.value()? != self.seen {
            return Err(IoError::Conflict.into());
        }
        self.written = true;
        Ok(Next::Call(IoCall::Write(std::mem::take(&mut self.writes))))
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

fn unexpected(expected: &'static str, event: StorageEvent) -> BootstrapError {
    let got = Event::Storage(event).label();
    IoError::Unexpected { expected, got }.into()
}

/// One stored bootstrap cursor, read on its own key.
#[derive(Debug, PartialEq)]
pub struct ReadCursorOperation {
    key: Vec<u8>,
}

impl ReadCursorOperation {
    pub fn new(key: Vec<u8>) -> Self {
        Self { key }
    }
}

impl Operation for ReadCursorOperation {
    type Output = Option<BootstrapCursor>;
    type Error = BootstrapError;

    fn first(&mut self) -> Result<Next<Option<BootstrapCursor>>, BootstrapError> {
        let read = (BOOTSTRAP_KEYSPACE.to_string(), self.key.clone().into());
        Ok(Next::Call(IoCall::Read(vec![read])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<BootstrapCursor>>, BootstrapError> {
        let cursor = (result.value()?)
            .map(|value| BootstrapCursor::from_bytes(&value))
            .transpose()?;
        Ok(Next::Done(cursor))
    }
}

/// Deletes one page of at most `limit` bootstrap cursors of a set: all of them, or with
/// `walked` only those of members no longer walked. Answers whether the page held all of them.
#[derive(Debug, PartialEq)]
pub struct ClearCursorsOperation {
    set_id: Ulid,
    walked: Option<Vec<Ulid>>,
    limit: usize,
    all_read: Option<bool>,
}

impl ClearCursorsOperation {
    pub fn new(set_id: Ulid, walked: Option<Vec<Ulid>>, limit: usize) -> Self {
        Self {
            set_id,
            walked,
            limit,
            all_read: None,
        }
    }

    /// Whether the cursor row `key` goes: every row, or one of a member no longer walked.
    fn goes(&self, key: &[u8]) -> bool {
        let Some(walked) = &self.walked else {
            return true;
        };
        let member = key.get(16..32).and_then(|id| <[u8; 16]>::try_from(id).ok());
        member.is_some_and(|id| !walked.iter().any(|walked| walked.to_bytes() == id))
    }
}

impl Operation for ClearCursorsOperation {
    type Output = bool;
    type Error = BootstrapError;

    fn first(&mut self) -> Result<Next<bool>, BootstrapError> {
        Ok(Next::Call(IoCall::Page {
            space: BOOTSTRAP_KEYSPACE.to_string(),
            prefix: bootstrap_prefix(self.set_id).into(),
            after: None,
            limit: self.limit,
        }))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, BootstrapError> {
        if let Some(all_read) = self.all_read {
            result.done()?;
            return Ok(Next::Done(all_read));
        }
        let (rows, _) = result.page()?;
        self.all_read = Some(rows.len() < self.limit);
        let deletes = (rows.into_iter())
            .filter(|(key, _)| self.goes(key))
            .map(|(key, _)| (BOOTSTRAP_KEYSPACE.to_string(), key))
            .collect();
        Ok(Next::Call(IoCall::Delete(deletes)))
    }
}

#[cfg(test)]
#[path = "bootstrap_tests.rs"]
mod tests;
