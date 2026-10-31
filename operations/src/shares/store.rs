//! Reads and writes share link records, the owner index and the per-link access log.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{SHARE_EVENT_KEYSPACE, SHARE_LINK_KEYSPACE, SHARE_OWNER_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::share::{ShareEvent, ShareLink, event_key, owner_key};
use aruna_core::types::{Effects, Key};
use smallvec::smallvec;
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

fn storage_error(event: Event) -> ShareStoreError {
    match event {
        Event::Storage(StorageEvent::Error { error }) => error.into(),
        _ => ShareStoreError::UnexpectedEvent,
    }
}

/// Writes one link record and its owner index row in one batch.
#[derive(Debug, PartialEq)]
pub struct SaveShareOperation {
    link: ShareLink,
    output: Option<Result<(), ShareStoreError>>,
}

impl SaveShareOperation {
    pub fn new(link: ShareLink) -> Self {
        Self { link, output: None }
    }
}

impl Operation for SaveShareOperation {
    type Output = ();
    type Error = ShareStoreError;

    fn start(&mut self) -> Effects {
        let value = match self.link.to_bytes() {
            Ok(value) => value,
            Err(error) => {
                self.output = Some(Err(error.into()));
                return smallvec![];
            }
        };
        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes: vec![
                (
                    SHARE_LINK_KEYSPACE.to_string(),
                    self.link.id.to_bytes().to_vec().into(),
                    value.into(),
                ),
                (
                    SHARE_OWNER_KEYSPACE.to_string(),
                    owner_key(&self.link.created_by, self.link.id).into(),
                    Vec::new().into(),
                ),
            ],
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if self.output.is_none() {
            self.output = Some(match event {
                Event::Storage(StorageEvent::BatchWriteResult { .. }) => Ok(()),
                event => Err(storage_error(event)),
            });
        }
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<(), ShareStoreError> {
        self.output.unwrap_or(Err(ShareStoreError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

/// Reads one link record; `None` when the id is unknown.
#[derive(Debug, PartialEq)]
pub struct GetShareOperation {
    id: Ulid,
    output: Option<Result<Option<ShareLink>, ShareStoreError>>,
}

impl GetShareOperation {
    pub fn new(id: Ulid) -> Self {
        Self { id, output: None }
    }
}

impl Operation for GetShareOperation {
    type Output = Option<ShareLink>;
    type Error = ShareStoreError;

    fn start(&mut self) -> Effects {
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: SHARE_LINK_KEYSPACE.to_string(),
            key: self.id.to_bytes().to_vec().into(),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if self.output.is_none() {
            self.output = Some(match event {
                Event::Storage(StorageEvent::ReadResult { value, .. }) => value
                    .map(|bytes| ShareLink::from_bytes(&bytes))
                    .transpose()
                    .map_err(Into::into)
                    .and_then(|link| match link {
                        Some(link) if link.id != self.id => Err(ShareStoreError::UnexpectedEvent),
                        link => Ok(link),
                    }),
                event => Err(storage_error(event)),
            });
        }
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<Option<ShareLink>, ShareStoreError> {
        self.output.unwrap_or(Err(ShareStoreError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

/// Lists every link one user created on this node, oldest first.
#[derive(Debug, PartialEq)]
pub struct ListSharesOperation {
    owner: UserId,
    next: Option<Key>,
    links: Vec<ShareLink>,
    reading: bool,
    output: Option<Result<Vec<ShareLink>, ShareStoreError>>,
}

impl ListSharesOperation {
    pub fn new(owner: UserId) -> Self {
        Self {
            owner,
            next: None,
            links: Vec::new(),
            reading: false,
            output: None,
        }
    }

    fn page(&mut self) -> Effects {
        self.reading = false;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: SHARE_OWNER_KEYSPACE.to_string(),
            prefix: Some(self.owner.to_storage_key().into()),
            start: self.next.clone().map(IterStart::After),
            limit: LIST_PAGE,
            txn_id: None,
        })]
    }

    fn fail(&mut self, error: ShareStoreError) -> Effects {
        self.output = Some(Err(error));
        smallvec![]
    }

    fn index_page(
        &mut self,
        values: Vec<(Key, aruna_core::types::Value)>,
        next: Option<Key>,
    ) -> Effects {
        let prefix = self.owner.to_storage_key().len();
        let mut reads = Vec::with_capacity(values.len());
        for (key, _) in values {
            match key.get(prefix..) {
                Some(id) if id.len() == 16 => {
                    reads.push((SHARE_LINK_KEYSPACE.to_string(), id.to_vec().into()));
                }
                _ => return self.fail(ShareStoreError::UnexpectedEvent),
            }
        }
        self.next = next;
        if reads.is_empty() {
            return self.finish_page();
        }
        self.reading = true;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        })]
    }

    fn finish_page(&mut self) -> Effects {
        if self.next.is_some() {
            return self.page();
        }
        self.output = Some(Ok(std::mem::take(&mut self.links)));
        smallvec![]
    }
}

impl Operation for ListSharesOperation {
    type Output = Vec<ShareLink>;
    type Error = ShareStoreError;

    fn start(&mut self) -> Effects {
        self.page()
    }

    fn step(&mut self, event: Event) -> Effects {
        if self.output.is_some() {
            return smallvec![];
        }
        match (self.reading, event) {
            (
                false,
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }),
            ) => self.index_page(values, next_start_after),
            (true, Event::Storage(StorageEvent::BatchReadResult { values })) => {
                for (_, value) in values {
                    // An index row whose record is gone is skipped.
                    let Some(value) = value else { continue };
                    match ShareLink::from_bytes(&value) {
                        Ok(link) if link.created_by == self.owner => self.links.push(link),
                        Ok(_) => return self.fail(ShareStoreError::UnexpectedEvent),
                        Err(error) => return self.fail(error.into()),
                    }
                }
                self.finish_page()
            }
            (_, event) => self.fail(storage_error(event)),
        }
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<Vec<ShareLink>, ShareStoreError> {
        self.output.unwrap_or(Err(ShareStoreError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

/// Appends one access log entry to a link.
#[derive(Debug, PartialEq)]
pub struct LogShareOperation {
    share_id: Ulid,
    entry_id: Ulid,
    event: ShareEvent,
    output: Option<Result<(), ShareStoreError>>,
}

impl LogShareOperation {
    /// `entry_id` orders the log; a fresh ULID of the event time.
    pub fn new(share_id: Ulid, entry_id: Ulid, event: ShareEvent) -> Self {
        Self {
            share_id,
            entry_id,
            event,
            output: None,
        }
    }
}

impl Operation for LogShareOperation {
    type Output = ();
    type Error = ShareStoreError;

    fn start(&mut self) -> Effects {
        let value = match self.event.to_bytes() {
            Ok(value) => value,
            Err(error) => {
                self.output = Some(Err(error.into()));
                return smallvec![];
            }
        };
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: SHARE_EVENT_KEYSPACE.to_string(),
            key: event_key(self.share_id, self.entry_id).into(),
            value: value.into(),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if self.output.is_none() {
            self.output = Some(match event {
                Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
                event => Err(storage_error(event)),
            });
        }
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<(), ShareStoreError> {
        self.output.unwrap_or(Err(ShareStoreError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
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
    output: Option<Result<EventPage, ShareStoreError>>,
}

impl ShareEventsOperation {
    pub fn new(share_id: Ulid, after: Option<Ulid>, limit: usize) -> Self {
        Self {
            share_id,
            after,
            limit,
            output: None,
        }
    }

    fn page(
        &self,
        values: Vec<(Key, aruna_core::types::Value)>,
        more: bool,
    ) -> Result<EventPage, ShareStoreError> {
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

    fn start(&mut self) -> Effects {
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: SHARE_EVENT_KEYSPACE.to_string(),
            prefix: Some(self.share_id.to_bytes().to_vec().into()),
            start: self
                .after
                .map(|after| IterStart::After(event_key(self.share_id, after).into())),
            limit: self.limit,
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if self.output.is_none() {
            self.output = Some(match event {
                Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }) => self.page(values, next_start_after.is_some()),
                event => Err(storage_error(event)),
            });
        }
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<EventPage, ShareStoreError> {
        self.output.unwrap_or(Err(ShareStoreError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
#[path = "store_tests.rs"]
mod tests;
