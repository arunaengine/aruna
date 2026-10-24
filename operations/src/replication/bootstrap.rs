//! Bootstraps a catching-up member of a distributed bucket from the coordinators of its ranges.
//! Each range is walked over the ordered inventory in bounded pages with a durable cursor; a page
//! waits until its jobs were delivered, and live publications overlap the walk.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::effects::{Effect, IterEnd, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{BOOTSTRAP_KEYSPACE, INVENTORY_KEYSPACE, REPLICATION_JOB_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::distribution::KeyRange;
use aruna_core::structs::storage::distribution::bootstrap::{BootstrapCursor, bootstrap_key};
use aruna_core::structs::storage::distribution::inventory::{
    Entry, Position, VersionEntry, range_interval,
};
use aruna_core::types::{Effects, Key, TxnId, Value};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use super::members::MemberTarget;
use super::protocol::ReplicationMode;
use super::queue::{BlobJobRecord, blob_job_key};
use super::version_replication::{ReplicateScopeInput, ReplicateScopeTarget};

/// Inventory rows one page queues at most; the next page waits until they are delivered.
const PAGE_ROWS: usize = 64;
const PAGE_BYTES: usize = 1 << 20;
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

#[cfg(test)]
#[path = "bootstrap_tests.rs"]
mod tests;
