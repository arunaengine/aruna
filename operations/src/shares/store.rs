//! Reads and writes share link records, the owner index and the per-link access log.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::{
    SHARE_EVENT_KEYSPACE, SHARE_LINK_KEYSPACE, SHARE_OWNER_KEYSPACE, SHARE_USE_KEYSPACE,
};
use aruna_core::operation::{AfterConflict, Operation, TransactionMode};
use aruna_core::rows;
use aruna_core::structs::storage::share::{ShareEvent, ShareLink, ShareUses, event_key, owner_key};
use aruna_core::types::{Key, Value};
use thiserror::Error;
use ulid::Ulid;

/// Owner index rows read per page while listing.
const LIST_PAGE: usize = 500;

#[derive(Debug, Error, PartialEq)]
pub enum ShareStoreError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("unexpected storage event")]
    UnexpectedEvent,
    #[error("share store operation did not finish")]
    NotFinished,
}

impl From<IoError> for ShareStoreError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.into())
    }
}

/// The rows of one link record and its owner index row.
pub fn save_rows(link: &ShareLink) -> Result<Vec<(String, Key, Value)>, ConversionError> {
    Ok(vec![
        (
            SHARE_LINK_KEYSPACE.to_string(),
            link.id.to_bytes().to_vec().into(),
            link.to_bytes()?.into(),
        ),
        (
            SHARE_OWNER_KEYSPACE.to_string(),
            owner_key(&link.created_by, link.id).into(),
            Vec::new().into(),
        ),
    ])
}

/// Reads one link record; `None` when the id is unknown.
#[derive(Debug, PartialEq)]
pub struct GetShareOperation {
    id: Ulid,
    owner: Option<UserId>,
}

impl GetShareOperation {
    pub fn new(id: Ulid) -> Self {
        Self { id, owner: None }
    }

    /// Reads the link only when `owner` created it; another user's link reads as `None`.
    pub fn for_owner(id: Ulid, owner: UserId) -> Self {
        Self {
            owner: Some(owner),
            ..Self::new(id)
        }
    }
}

impl Operation for GetShareOperation {
    type Output = Option<ShareLink>;
    type Error = ShareStoreError;

    fn first(&mut self) -> Result<Next<Self::Output>, ShareStoreError> {
        Ok(Next::Call(rows::read_row(
            SHARE_LINK_KEYSPACE,
            self.id.to_bytes().to_vec(),
        )))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, ShareStoreError> {
        let link = rows::value(result)?.map(|bytes| ShareLink::from_bytes(&bytes));
        let link = link.transpose()?;
        if link.as_ref().is_some_and(|link| link.id != self.id) {
            return Err(ShareStoreError::UnexpectedEvent);
        }
        let owned = link.filter(|link| self.owner.is_none_or(|owner| link.created_by == owner));
        Ok(Next::Done(owned))
    }
}

/// Lists every link one user created on this node, oldest first.
#[derive(Debug, PartialEq)]
pub struct ListSharesOperation {
    owner: UserId,
    next: Option<Key>,
    links: Vec<ShareLink>,
    reading: bool,
}

impl ListSharesOperation {
    pub fn new(owner: UserId) -> Self {
        Self {
            owner,
            next: None,
            links: Vec::new(),
            reading: false,
        }
    }

    fn page(&mut self) -> Next<Vec<ShareLink>> {
        self.reading = false;
        Next::Call(IoCall::Page {
            space: SHARE_OWNER_KEYSPACE.to_string(),
            prefix: self.owner.to_storage_key().into(),
            after: self.next.clone(),
            limit: LIST_PAGE,
        })
    }

    /// The link rows the owner index rows of one page name.
    fn index_page(&mut self, values: Vec<(Key, Value)>) -> Result<IoCall, ShareStoreError> {
        let prefix = self.owner.to_storage_key().len();
        let mut reads = Vec::with_capacity(values.len());
        for (key, _) in values {
            match key.get(prefix..) {
                Some(id) if id.len() == 16 => {
                    reads.push((SHARE_LINK_KEYSPACE.to_string(), id.to_vec().into()));
                }
                _ => return Err(ShareStoreError::UnexpectedEvent),
            }
        }
        self.reading = true;
        Ok(IoCall::Read(reads))
    }
}

impl Operation for ListSharesOperation {
    type Output = Vec<ShareLink>;
    type Error = ShareStoreError;

    fn first(&mut self) -> Result<Next<Self::Output>, ShareStoreError> {
        Ok(self.page())
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, ShareStoreError> {
        if !self.reading {
            let (values, next) = rows::page(result)?;
            self.next = next;
            return Ok(Next::Call(self.index_page(values)?));
        }
        // An index row whose record is gone is skipped.
        for value in rows::all_values(result)?.into_iter().flatten() {
            let link = ShareLink::from_bytes(&value)?;
            if link.created_by != self.owner {
                return Err(ShareStoreError::UnexpectedEvent);
            }
            self.links.push(link);
        }
        match self.next.is_some() {
            true => Ok(self.page()),
            false => Ok(Next::Done(std::mem::take(&mut self.links))),
        }
    }
}

/// Appends one access log entry and counts it in the link's use row, in one transaction. Run it
/// with a conflict retry: a lost conflict starts over, so concurrent visits never lose a count.
#[derive(Debug, PartialEq)]
pub struct LogShareOperation {
    share_id: Ulid,
    entry_id: Ulid,
    event: ShareEvent,
    written: bool,
}

impl LogShareOperation {
    /// `entry_id` orders the log; a fresh ULID of the event time.
    pub fn new(share_id: Ulid, entry_id: Ulid, event: ShareEvent) -> Self {
        Self {
            share_id,
            entry_id,
            event,
            written: false,
        }
    }

    fn writes(&self, uses: Option<&[u8]>) -> Result<Vec<(String, Key, Value)>, ConversionError> {
        let mut counted = uses
            .map(ShareUses::from_bytes)
            .transpose()?
            .unwrap_or_default();
        counted.record(&self.event);
        Ok(vec![
            (
                SHARE_EVENT_KEYSPACE.to_string(),
                event_key(self.share_id, self.entry_id).into(),
                self.event.to_bytes()?.into(),
            ),
            (
                SHARE_USE_KEYSPACE.to_string(),
                self.share_id.to_bytes().to_vec().into(),
                counted.to_bytes()?.into(),
            ),
        ])
    }
}

impl Operation for LogShareOperation {
    type Output = ();
    type Error = ShareStoreError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<()>, ShareStoreError> {
        Ok(Next::Call(rows::read_row(
            SHARE_USE_KEYSPACE,
            self.share_id.to_bytes().to_vec(),
        )))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, ShareStoreError> {
        if self.written {
            rows::done(result)?;
            return Ok(Next::Done(()));
        }
        let uses = rows::value(result)?;
        self.written = true;
        Ok(Next::Call(IoCall::Write(self.writes(uses.as_deref())?)))
    }

    fn on_conflict(&mut self, _attempt: u32) -> AfterConflict<()> {
        AfterConflict::Rerun
    }
}

/// Reads the use counters of `ids`, in order; a link never used counts zero.
#[derive(Debug, PartialEq)]
pub struct ReadUsesOperation {
    ids: Vec<Ulid>,
}

impl ReadUsesOperation {
    pub fn new(ids: Vec<Ulid>) -> Self {
        Self { ids }
    }
}

impl Operation for ReadUsesOperation {
    type Output = Vec<ShareUses>;
    type Error = ShareStoreError;

    fn first(&mut self) -> Result<Next<Self::Output>, ShareStoreError> {
        let reads = (self.ids.iter())
            .map(|id| {
                (
                    SHARE_USE_KEYSPACE.to_string(),
                    id.to_bytes().to_vec().into(),
                )
            })
            .collect();
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, ShareStoreError> {
        let values = rows::all_values(result)?;
        if values.len() != self.ids.len() {
            return Err(ShareStoreError::UnexpectedEvent);
        }
        let uses = (values.into_iter())
            .map(|value| match value {
                Some(bytes) => ShareUses::from_bytes(&bytes).map_err(Into::into),
                None => Ok(ShareUses::default()),
            })
            .collect::<Result<_, ShareStoreError>>()?;
        Ok(Next::Done(uses))
    }
}

/// One page of a link's access log, oldest first, and the entry id the next page starts after.
pub type EventPage = (Vec<(Ulid, ShareEvent)>, Option<Ulid>);

/// Reads one page of a link's access log after `after`.
#[derive(Debug, PartialEq)]
pub struct ShareEventsOperation {
    share_id: Ulid,
    after: Option<Ulid>,
    limit: usize,
}

impl ShareEventsOperation {
    pub fn new(share_id: Ulid, after: Option<Ulid>, limit: usize) -> Self {
        Self {
            share_id,
            after,
            limit,
        }
    }

    fn page(&self, values: Vec<(Key, Value)>, more: bool) -> Result<EventPage, ShareStoreError> {
        let mut events = Vec::with_capacity(values.len());
        for (key, value) in values {
            let entry: [u8; 16] = key
                .get(16..)
                .and_then(|bytes| bytes.try_into().ok())
                .ok_or(ShareStoreError::UnexpectedEvent)?;
            events.push((Ulid::from_bytes(entry), ShareEvent::from_bytes(&value)?));
        }
        let next = more.then(|| events.last().map(|(id, _)| *id)).flatten();
        Ok((events, next))
    }
}

impl Operation for ShareEventsOperation {
    type Output = EventPage;
    type Error = ShareStoreError;

    fn first(&mut self) -> Result<Next<Self::Output>, ShareStoreError> {
        Ok(Next::Call(IoCall::Page {
            space: SHARE_EVENT_KEYSPACE.to_string(),
            prefix: self.share_id.to_bytes().to_vec().into(),
            after: (self.after).map(|after| event_key(self.share_id, after).into()),
            limit: self.limit,
        }))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, ShareStoreError> {
        let (values, next) = rows::page(result)?;
        Ok(Next::Done(self.page(values, next.is_some())?))
    }
}

#[cfg(test)]
#[path = "store_tests.rs"]
mod tests;
