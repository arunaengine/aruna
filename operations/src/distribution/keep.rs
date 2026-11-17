//! Publishes the bytes a member keeps on use. A fill first claims its copy, so concurrent reads
//! fetch it once, then publishes the verified copy in one transaction.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// The copy becomes an attachment row, or a cache entry that evicts the oldest entries for room.

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::{
    ATTACHMENT_KEYSPACE, CACHE_ENTRY_KEYSPACE, CACHE_ORDER_KEYSPACE, DISTRIBUTION_FENCE_KEYSPACE,
    DISTRIBUTION_KEYSPACE, FILL_CLAIM_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::rows::{done, read_row, values};
use aruna_core::structs::storage::blob::BackendLocation;
use aruna_core::structs::storage::distribution::DistributionView;
use aruna_core::structs::storage::distribution::fence::LocalFence;
use aruna_core::structs::storage::distribution::on_use::{
    Attachment, CACHE_TOTALS_KEY, CacheEntry, CacheTotals, FillClaim, KeptKind, attachment_key,
};
use aruna_core::types::{Key, Value};
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
}

impl From<IoError> for KeepError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.into())
    }
}

/// Whether a fill may start.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Claimed {
    Claimed,
    /// Another fill of the same copy runs and is not stale yet.
    Busy,
}

/// Claims one fill in one transaction: absent or stale claims are taken, a fresh one of another
/// fill is not.
#[derive(Debug, PartialEq)]
pub struct ClaimFillOperation {
    key: Vec<u8>,
    claim: FillClaim,
    written: bool,
}

impl ClaimFillOperation {
    pub fn new(key: Vec<u8>, claim: FillClaim) -> Self {
        Self {
            key,
            claim,
            written: false,
        }
    }

    fn claim_held(&mut self, value: Option<Value>) -> Result<Next<Claimed>, KeepError> {
        let held = value.as_deref().map(FillClaim::from_bytes).transpose()?;
        let busy = held.is_some_and(|held| {
            held.claim_id != self.claim.claim_id && !held.stale(self.claim.claimed_at_ms)
        });
        if busy {
            return Ok(Next::Done(Claimed::Busy));
        }
        self.written = true;
        let row = (
            FILL_CLAIM_KEYSPACE.to_string(),
            self.key.clone().into(),
            self.claim.to_bytes()?.into(),
        );
        Ok(Next::Call(IoCall::Write(vec![row])))
    }
}

impl Operation for ClaimFillOperation {
    type Output = Claimed;
    type Error = KeepError;

    fn first(&mut self) -> Result<Next<Claimed>, KeepError> {
        Ok(Next::Call(read_row(FILL_CLAIM_KEYSPACE, self.key.clone())))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Claimed>, KeepError> {
        if self.written {
            done(result)?;
            return Ok(Next::Done(Claimed::Claimed));
        }
        let [value] = values::<1>(result)?;
        self.claim_held(value)
    }

    fn transaction(&self) -> bool {
        true
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
    /// The member keeping an attachment; a member gone from the set's record keeps nothing.
    pub member_id: Ulid,
}

/// What publishing did.
#[derive(Clone, Debug, PartialEq)]
pub enum Published {
    /// The copy is published; the evicted cache entries' copies are to be deleted.
    Kept { evicted: Vec<BackendLocation> },
    /// The cache keeps the same bytes already or has no room; the new copy is to be deleted.
    Unkept,
}

/// What the pending call of a publish does.
#[derive(Clone, Debug, PartialEq)]
enum PublishStep {
    /// Reads the set's fence row and record: a deleted, departing or departed member keeps
    /// nothing new.
    ReadFence,
    ReadCache,
    Evict,
    /// Deletes the claim and the evicted rows, then writes these rows, if any.
    Delete(Vec<(String, Key, Value)>),
    Write,
}

type PublishNext = Result<Next<Published>, KeepError>;

/// Publishes a verified copy and releases its fill claim in one transaction.
#[derive(Debug, PartialEq)]
pub struct PublishKeptOperation {
    copy: KeptCopy,
    step: PublishStep,
    totals: CacheTotals,
    /// Bytes the cache still has to free, and what this admission frees.
    needed: u64,
    evicted: Vec<CacheEntry>,
    pages: usize,
    /// The order page to read after, while evicting.
    after: Option<Key>,
    output: Option<Published>,
}

impl PublishKeptOperation {
    pub fn new(copy: KeptCopy) -> Self {
        Self {
            copy,
            step: PublishStep::ReadFence,
            totals: CacheTotals::default(),
            needed: 0,
            evicted: Vec::new(),
            pages: 0,
            after: None,
            output: None,
        }
    }

    /// Leaves the cache unchanged; the claim is released without a trace.
    fn unkept(&mut self) -> PublishNext {
        self.output = Some(Published::Unkept);
        self.step = PublishStep::Write;
        let claim = (
            FILL_CLAIM_KEYSPACE.to_string(),
            self.copy.claim_key.clone().into(),
        );
        Ok(Next::Call(IoCall::Delete(vec![claim])))
    }

    fn read_order(&mut self) -> PublishNext {
        if self.pages >= EVICTION_PAGES {
            return self.unkept();
        }
        self.pages += 1;
        self.step = PublishStep::Evict;
        Ok(Next::Call(IoCall::Page {
            space: CACHE_ORDER_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after: self.after.take(),
            limit: EVICTION_PAGE,
        }))
    }

    fn handle_fence(&mut self, fence: Option<Value>, record: Option<Value>) -> PublishNext {
        if !LocalFence::decode(fence.as_deref())?.receiving() {
            return self.unkept();
        }
        // Departure cleanup drops the fence row last; the record no longer names us then.
        let view = record
            .as_deref()
            .map(DistributionView::from_bytes)
            .transpose()?;
        let member = view.is_some_and(|view| view.snapshot().member(self.copy.member_id).is_some());
        if self.copy.kind == KeptKind::Attachment {
            return match member {
                true => self.write_kept(),
                false => self.unkept(),
            };
        }
        self.step = PublishStep::ReadCache;
        Ok(Next::Call(IoCall::Read(vec![
            (
                CACHE_ENTRY_KEYSPACE.to_string(),
                self.copy.identity.to_vec().into(),
            ),
            (
                CACHE_ENTRY_KEYSPACE.to_string(),
                CACHE_TOTALS_KEY.to_vec().into(),
            ),
        ])))
    }

    fn handle_cache(&mut self, existing: Option<Value>, totals: Option<Value>) -> PublishNext {
        if existing.is_some() {
            return self.unkept();
        }
        self.totals = totals
            .as_deref()
            .map(CacheTotals::from_bytes)
            .transpose()?
            .unwrap_or_default();
        if self.copy.size > self.copy.capacity {
            return self.unkept();
        }
        let after = self.totals.bytes.saturating_add(self.copy.size);
        self.needed = after.saturating_sub(self.copy.capacity);
        match self.needed {
            0 => self.write_kept(),
            _ => self.read_order(),
        }
    }

    fn handle_order(&mut self, values: Vec<(Key, Value)>, next: Option<Key>) -> PublishNext {
        for (_, value) in values {
            if self.needed == 0 {
                break;
            }
            let entry = CacheEntry::from_bytes(&value)?;
            self.needed = self.needed.saturating_sub(entry.size);
            self.evicted.push(entry);
        }
        match (self.needed, next) {
            (0, _) => self.write_kept(),
            (_, Some(next)) => {
                self.after = Some(next);
                self.read_order()
            }
            // The totals promise more bytes than the order rows hold; keep nothing.
            (_, None) => self.unkept(),
        }
    }

    fn write_kept(&mut self) -> PublishNext {
        let copy = &self.copy;
        let mut writes = Vec::new();
        let mut deletes = vec![(
            FILL_CLAIM_KEYSPACE.to_string(),
            copy.claim_key.clone().into(),
        )];
        match copy.kind {
            KeptKind::Attachment => {
                let value = Attachment {
                    blake3: copy.blake3,
                    location: copy.location.clone(),
                }
                .to_bytes()?;
                let key = attachment_key(copy.set_id, copy.version_id);
                writes.push((ATTACHMENT_KEYSPACE.to_string(), key.into(), value.into()));
            }
            KeptKind::Cache => self.cache_rows(&mut writes, &mut deletes)?,
        }
        let evicted = (self.evicted.iter())
            .map(|entry| entry.location.clone())
            .collect();
        self.output = Some(Published::Kept { evicted });
        self.step = PublishStep::Delete(writes);
        Ok(Next::Call(IoCall::Delete(deletes)))
    }

    /// The rows of a new cache entry, the evictions and the new totals.
    fn cache_rows(
        &self,
        writes: &mut Vec<(String, Key, Value)>,
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
}

impl Operation for PublishKeptOperation {
    type Output = Published;
    type Error = KeepError;

    fn first(&mut self) -> PublishNext {
        let set_key = LocalFence::key(self.copy.set_id);
        Ok(Next::Call(IoCall::Read(vec![
            (
                DISTRIBUTION_FENCE_KEYSPACE.to_string(),
                set_key.clone().into(),
            ),
            (DISTRIBUTION_KEYSPACE.to_string(), set_key.into()),
        ])))
    }

    fn next(&mut self, result: IoResult) -> PublishNext {
        match std::mem::replace(&mut self.step, PublishStep::Write) {
            PublishStep::ReadFence => {
                let [fence, record] = values::<2>(result)?;
                self.handle_fence(fence, record)
            }
            PublishStep::ReadCache => {
                let [existing, totals] = values::<2>(result)?;
                self.handle_cache(existing, totals)
            }
            PublishStep::Evict => {
                let (rows, next) = result.page()?;
                self.handle_order(rows, next)
            }
            PublishStep::Delete(writes) => {
                done(result)?;
                Ok(Next::Call(IoCall::Write(writes)))
            }
            PublishStep::Write => {
                done(result)?;
                let output = self.output.take().ok_or(KeepError::NotFinished)?;
                Ok(Next::Done(output))
            }
        }
    }

    fn transaction(&self) -> bool {
        true
    }
}

#[cfg(test)]
#[path = "keep_tests.rs"]
mod tests;
