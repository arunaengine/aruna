//! Publishes the bytes a member keeps on use. A fill first claims its copy, so concurrent reads
//! fetch it once; a verified copy is then published in one transaction: an attachment row, or
//! a cache entry that evicts the oldest entries until the cache has room.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    ATTACHMENT_KEYSPACE, CACHE_ENTRY_KEYSPACE, CACHE_ORDER_KEYSPACE, FILL_CLAIM_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::blob::BackendLocation;
use aruna_core::structs::storage::distribution::on_use::{
    Attachment, CACHE_TOTALS_KEY, CacheEntry, CacheTotals, FillClaim, KeptKind, attachment_key,
};
use aruna_core::types::{Effects, Key, TxnId};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

/// Order rows one admission reads per page while it looks for room.
const EVICTION_PAGE: usize = 64;
/// Pages one admission may read; a cache that needs more evictions keeps nothing this time.
const EVICTION_PAGES: usize = 16;

#[derive(Debug, Error, PartialEq)]
pub enum KeepError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("operation did not finish")]
    NotFinished,
    #[error("unexpected event in state {state}: got {got}")]
    UnexpectedEvent { state: &'static str, got: String },
}

fn unexpected(state: &'static str, event: Event) -> KeepError {
    KeepError::UnexpectedEvent {
        state,
        got: format!("{event:?}"),
    }
}

/// Whether a fill may start.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Claimed {
    Claimed,
    /// Another fill of the same copy runs and is not stale yet.
    Busy,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ClaimState {
    Init,
    Start,
    Read,
    Write,
    Commit,
    Abort,
    Done,
}

/// Claims one fill: absent or stale claims are taken, a fresh one of another fill is not.
#[derive(Debug, PartialEq)]
pub struct ClaimFillOperation {
    key: Vec<u8>,
    claim: FillClaim,
    state: ClaimState,
    txn_id: Option<TxnId>,
    output: Option<Result<Claimed, KeepError>>,
}

impl ClaimFillOperation {
    pub fn new(key: Vec<u8>, claim: FillClaim) -> Self {
        Self {
            key,
            claim,
            state: ClaimState::Init,
            txn_id: None,
            output: None,
        }
    }

    fn fail(&mut self, error: KeepError) -> Effects {
        self.output = Some(Err(error));
        self.state = ClaimState::Done;
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }

    fn state_name(&self) -> &'static str {
        match self.state {
            ClaimState::Init => "init",
            ClaimState::Start => "start",
            ClaimState::Read => "read",
            ClaimState::Write => "write",
            ClaimState::Commit => "commit",
            ClaimState::Abort => "abort",
            ClaimState::Done => "done",
        }
    }
}

impl Operation for ClaimFillOperation {
    type Output = Claimed;
    type Error = KeepError;

    fn start(&mut self) -> Effects {
        self.state = ClaimState::Start;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }
        match (self.state, event) {
            (ClaimState::Start, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.txn_id = Some(txn_id);
                self.state = ClaimState::Read;
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: FILL_CLAIM_KEYSPACE.to_string(),
                    key: self.key.clone().into(),
                    txn_id: Some(txn_id),
                })]
            }
            (ClaimState::Read, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                let held = match value.as_deref().map(FillClaim::from_bytes).transpose() {
                    Ok(held) => held,
                    Err(error) => return self.fail(error.into()),
                };
                let busy = held.is_some_and(|held| {
                    held.claim_id != self.claim.claim_id && !held.stale(self.claim.claimed_at_ms)
                });
                let Some(txn_id) = self.txn_id else {
                    return self.fail(KeepError::NotFinished);
                };
                if busy {
                    self.output = Some(Ok(Claimed::Busy));
                    self.state = ClaimState::Abort;
                    self.txn_id = None;
                    return smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })];
                }
                let value = match self.claim.to_bytes() {
                    Ok(value) => value,
                    Err(error) => return self.fail(error.into()),
                };
                self.state = ClaimState::Write;
                smallvec![Effect::Storage(StorageEffect::Write {
                    key_space: FILL_CLAIM_KEYSPACE.to_string(),
                    key: self.key.clone().into(),
                    value: value.into(),
                    txn_id: Some(txn_id),
                })]
            }
            (ClaimState::Write, Event::Storage(StorageEvent::WriteResult { .. })) => {
                let Some(txn_id) = self.txn_id.take() else {
                    return self.fail(KeepError::NotFinished);
                };
                self.state = ClaimState::Commit;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            (ClaimState::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.output = Some(Ok(Claimed::Claimed));
                self.state = ClaimState::Done;
                smallvec![]
            }
            (ClaimState::Abort, Event::Storage(StorageEvent::TransactionAborted { .. })) => {
                self.state = ClaimState::Done;
                smallvec![]
            }
            (_, event) => {
                let error = unexpected(self.state_name(), event);
                self.fail(error)
            }
        }
    }

    fn is_complete(&self) -> bool {
        self.state == ClaimState::Done
    }

    fn finalize(self) -> Result<Claimed, KeepError> {
        self.output.unwrap_or(Err(KeepError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        self.fail(KeepError::NotFinished)
    }
}

/// One verified copy to publish.
#[derive(Clone, Debug, PartialEq)]
pub struct KeptCopy {
    pub kind: KeptKind,
    pub set_id: Ulid,
    pub version_id: Ulid,
    /// The cache entry key: the version's logical identity.
    pub identity: [u8; 32],
    pub blake3: [u8; 32],
    pub size: u64,
    pub location: BackendLocation,
    /// The claim the fill holds; publishing releases it.
    pub claim_key: Vec<u8>,
    /// Cache capacity in bytes; attachments ignore it.
    pub capacity: u64,
    /// Admission order of a new cache entry.
    pub order: Ulid,
}

/// What publishing did.
#[derive(Clone, Debug, PartialEq)]
pub enum Published {
    /// The copy is published; the evicted cache entries' copies are to be deleted.
    Kept { evicted: Vec<BackendLocation> },
    /// The cache keeps the same bytes already or has no room; the new copy is to be deleted.
    Unkept,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PublishState {
    Init,
    Start,
    ReadCache,
    Evict,
    Write,
    Commit,
    Done,
}

/// Publishes a verified copy and releases its fill claim in one transaction.
#[derive(Debug, PartialEq)]
pub struct PublishKeptOperation {
    copy: KeptCopy,
    state: PublishState,
    txn_id: Option<TxnId>,
    totals: CacheTotals,
    /// Bytes the cache still has to free, and what this admission frees.
    needed: u64,
    evicted: Vec<CacheEntry>,
    pages: usize,
    output: Option<Result<Published, KeepError>>,
}

impl PublishKeptOperation {
    pub fn new(copy: KeptCopy) -> Self {
        Self {
            copy,
            state: PublishState::Init,
            txn_id: None,
            totals: CacheTotals::default(),
            needed: 0,
            evicted: Vec::new(),
            pages: 0,
            output: None,
        }
    }

    fn fail(&mut self, error: KeepError) -> Effects {
        self.output = Some(Err(error));
        self.state = PublishState::Done;
        match self.txn_id.take() {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
        }
    }

    /// Leaves the cache unchanged; the claim is released without a trace.
    fn unkept(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(KeepError::NotFinished);
        };
        self.output = Some(Ok(Published::Unkept));
        self.state = PublishState::Write;
        smallvec![Effect::Storage(StorageEffect::Delete {
            key_space: FILL_CLAIM_KEYSPACE.to_string(),
            key: self.copy.claim_key.clone().into(),
            txn_id: Some(txn_id),
        })]
    }

    fn read_order(&mut self, after: Option<Key>) -> Effects {
        if self.pages >= EVICTION_PAGES {
            return self.unkept();
        }
        self.pages += 1;
        self.state = PublishState::Evict;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: CACHE_ORDER_KEYSPACE.to_string(),
            prefix: None,
            start: after.map(IterStart::After),
            limit: EVICTION_PAGE,
            txn_id: self.txn_id,
        })]
    }

    fn handle_cache(&mut self, values: Vec<(Key, Option<aruna_core::types::Value>)>) -> Effects {
        let (Some((_, existing)), Some((_, totals))) = (values.first(), values.get(1)) else {
            return self.fail(KeepError::NotFinished);
        };
        if existing.is_some() {
            return self.unkept();
        }
        self.totals = match totals.as_deref().map(CacheTotals::from_bytes).transpose() {
            Ok(totals) => totals.unwrap_or_default(),
            Err(error) => return self.fail(error.into()),
        };
        if self.copy.size > self.copy.capacity {
            return self.unkept();
        }
        let after = self.totals.bytes.saturating_add(self.copy.size);
        self.needed = after.saturating_sub(self.copy.capacity);
        match self.needed {
            0 => self.write_kept(),
            _ => self.read_order(None),
        }
    }

    fn handle_order(
        &mut self,
        values: Vec<(Key, aruna_core::types::Value)>,
        next: Option<Key>,
    ) -> Effects {
        for (_, value) in values {
            if self.needed == 0 {
                break;
            }
            let entry = match CacheEntry::from_bytes(&value) {
                Ok(entry) => entry,
                Err(error) => return self.fail(error.into()),
            };
            self.needed = self.needed.saturating_sub(entry.size);
            self.evicted.push(entry);
        }
        match (self.needed, next) {
            (0, _) => self.write_kept(),
            (_, Some(next)) => self.read_order(Some(next)),
            // The totals promise more bytes than the order rows hold; keep nothing.
            (_, None) => self.unkept(),
        }
    }

    fn write_kept(&mut self) -> Effects {
        let copy = &self.copy;
        let mut writes = Vec::new();
        let mut deletes = vec![(
            FILL_CLAIM_KEYSPACE.to_string(),
            copy.claim_key.clone().into(),
        )];
        let encoded = match copy.kind {
            KeptKind::Attachment => Attachment {
                blake3: copy.blake3,
                location: copy.location.clone(),
            }
            .to_bytes()
            .map(|value| {
                let key = attachment_key(copy.set_id, copy.version_id);
                writes.push((ATTACHMENT_KEYSPACE.to_string(), key.into(), value.into()));
            }),
            KeptKind::Cache => self.cache_rows(&mut writes, &mut deletes),
        };
        if let Err(error) = encoded {
            return self.fail(error.into());
        }
        let Some(txn_id) = self.txn_id else {
            return self.fail(KeepError::NotFinished);
        };
        self.output = Some(Ok(Published::Kept {
            evicted: self
                .evicted
                .iter()
                .map(|entry| entry.location.clone())
                .collect(),
        }));
        self.state = PublishState::Write;
        smallvec![
            Effect::Storage(StorageEffect::BatchDelete {
                deletes,
                txn_id: Some(txn_id),
            }),
            Effect::Storage(StorageEffect::BatchWrite {
                writes,
                txn_id: Some(txn_id),
            }),
        ]
    }

    /// The rows of a new cache entry, the evictions and the new totals.
    fn cache_rows(
        &self,
        writes: &mut Vec<(String, Key, aruna_core::types::Value)>,
        deletes: &mut Vec<(String, Key)>,
    ) -> Result<(), ConversionError> {
        let copy = &self.copy;
        let mut totals = self.totals;
        for entry in &self.evicted {
            deletes.push((CACHE_ENTRY_KEYSPACE.to_string(), entry.key.to_vec().into()));
            deletes.push((
                CACHE_ORDER_KEYSPACE.to_string(),
                entry.order.to_bytes().to_vec().into(),
            ));
            totals.bytes = totals.bytes.saturating_sub(entry.size);
            totals.entries = totals.entries.saturating_sub(1);
        }
        let entry = CacheEntry {
            key: copy.identity,
            blake3: copy.blake3,
            location: copy.location.clone(),
            size: copy.size,
            order: copy.order,
        }
        .to_bytes()?;
        totals.bytes = totals.bytes.saturating_add(copy.size);
        totals.entries = totals.entries.saturating_add(1);
        let order = copy.order.to_bytes().to_vec();
        writes.push((
            CACHE_ENTRY_KEYSPACE.to_string(),
            copy.identity.to_vec().into(),
            entry.clone().into(),
        ));
        writes.push((CACHE_ORDER_KEYSPACE.to_string(), order.into(), entry.into()));
        writes.push((
            CACHE_ENTRY_KEYSPACE.to_string(),
            CACHE_TOTALS_KEY.to_vec().into(),
            totals.to_bytes()?.into(),
        ));
        Ok(())
    }

    fn state_name(&self) -> &'static str {
        match self.state {
            PublishState::Init => "init",
            PublishState::Start => "start",
            PublishState::ReadCache => "read_cache",
            PublishState::Evict => "evict",
            PublishState::Write => "write",
            PublishState::Commit => "commit",
            PublishState::Done => "done",
        }
    }
}

impl Operation for PublishKeptOperation {
    type Output = Published;
    type Error = KeepError;

    fn start(&mut self) -> Effects {
        self.state = PublishState::Start;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }
        match (self.state, event) {
            (PublishState::Start, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.txn_id = Some(txn_id);
                if self.copy.kind == KeptKind::Attachment {
                    return self.write_kept();
                }
                self.state = PublishState::ReadCache;
                smallvec![Effect::Storage(StorageEffect::BatchRead {
                    reads: vec![
                        (
                            CACHE_ENTRY_KEYSPACE.to_string(),
                            self.copy.identity.to_vec().into()
                        ),
                        (
                            CACHE_ENTRY_KEYSPACE.to_string(),
                            CACHE_TOTALS_KEY.to_vec().into()
                        ),
                    ],
                    txn_id: Some(txn_id),
                })]
            }
            (PublishState::ReadCache, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                self.handle_cache(values)
            }
            (
                PublishState::Evict,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => self.handle_order(values, next_start_after),
            // The batch delete and batch write each answer; the commit follows the write.
            (PublishState::Write, Event::Storage(StorageEvent::BatchDeleteResult { .. })) => {
                smallvec![]
            }
            (
                PublishState::Write,
                Event::Storage(
                    StorageEvent::BatchWriteResult { .. } | StorageEvent::DeleteResult { .. },
                ),
            ) => {
                let Some(txn_id) = self.txn_id.take() else {
                    return self.fail(KeepError::NotFinished);
                };
                self.state = PublishState::Commit;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            (PublishState::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.state = PublishState::Done;
                smallvec![]
            }
            (_, event) => {
                let error = unexpected(self.state_name(), event);
                self.fail(error)
            }
        }
    }

    fn is_complete(&self) -> bool {
        self.state == PublishState::Done
    }

    fn finalize(self) -> Result<Published, KeepError> {
        self.output.unwrap_or(Err(KeepError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        self.fail(KeepError::NotFinished)
    }
}

#[cfg(test)]
#[path = "keep_tests.rs"]
mod tests;
