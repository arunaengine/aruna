//! Reads and writes share link records, the owner index and the per-link access log.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    SHARE_EVENT_KEYSPACE, SHARE_LINK_KEYSPACE, SHARE_OWNER_KEYSPACE, SHARE_USE_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::share::{ShareEvent, ShareLink, ShareUses, event_key, owner_key};
use aruna_core::types::{Effects, Key, TxnId, Value};
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

/// The batch that writes one link record and its owner index row.
pub fn save_effect(link: &ShareLink) -> Result<Effect, ConversionError> {
    Ok(Effect::Storage(StorageEffect::BatchWrite {
        writes: vec![
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
        ],
        txn_id: None,
    }))
}

/// Reads one link record; `None` when the id is unknown.
#[derive(Debug, PartialEq)]
pub struct GetShareOperation {
    id: Ulid,
    owner: Option<UserId>,
    output: Option<Result<Option<ShareLink>, ShareStoreError>>,
}

impl GetShareOperation {
    pub fn new(id: Ulid) -> Self {
        Self {
            id,
            owner: None,
            output: None,
        }
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
                        link => Ok(link.filter(|link| {
                            self.owner.is_none_or(|owner| link.created_by == owner)
                        })),
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LogState {
    Begin,
    Read(TxnId),
    Write(TxnId),
    Commit,
    Done,
    Failed,
}

/// Appends one access log entry and counts it in the link's use row, in one transaction; a
/// conflicting writer makes it start over, so concurrent visits never lose a count.
#[derive(Debug, PartialEq)]
pub struct LogShareOperation {
    share_id: Ulid,
    entry_id: Ulid,
    event: ShareEvent,
    state: LogState,
    error: Option<ShareStoreError>,
}

impl LogShareOperation {
    /// `entry_id` orders the log; a fresh ULID of the event time.
    pub fn new(share_id: Ulid, entry_id: Ulid, event: ShareEvent) -> Self {
        Self {
            share_id,
            entry_id,
            event,
            state: LogState::Begin,
            error: None,
        }
    }

    fn begin(&mut self) -> Effects {
        self.state = LogState::Begin;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn fail(&mut self, error: ShareStoreError, txn_id: Option<TxnId>) -> Effects {
        self.state = LogState::Failed;
        self.error = Some(error);
        match txn_id {
            Some(txn_id) => smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })],
            None => smallvec![],
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

    fn start(&mut self) -> Effects {
        self.begin()
    }

    fn step(&mut self, event: Event) -> Effects {
        match (self.state, event) {
            (LogState::Begin, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.state = LogState::Read(txn_id);
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: SHARE_USE_KEYSPACE.to_string(),
                    key: self.share_id.to_bytes().to_vec().into(),
                    txn_id: Some(txn_id),
                })]
            }
            (LogState::Read(txn_id), Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                match self.writes(value.as_deref()) {
                    Ok(writes) => {
                        self.state = LogState::Write(txn_id);
                        smallvec![Effect::Storage(StorageEffect::BatchWrite {
                            writes,
                            txn_id: Some(txn_id),
                        })]
                    }
                    Err(error) => self.fail(error.into(), Some(txn_id)),
                }
            }
            (LogState::Write(txn_id), Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                self.state = LogState::Commit;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            (LogState::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.state = LogState::Done;
                smallvec![]
            }
            (
                LogState::Commit,
                Event::Storage(StorageEvent::Error {
                    error: StorageError::TransactionConflict,
                }),
            ) => self.begin(),
            (LogState::Read(txn_id) | LogState::Write(txn_id), event) => {
                self.fail(storage_error(event), Some(txn_id))
            }
            (_, event) => self.fail(storage_error(event), None),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, LogState::Done | LogState::Failed)
    }

    fn finalize(self) -> Result<(), ShareStoreError> {
        match (self.state, self.error) {
            (LogState::Done, _) => Ok(()),
            (_, Some(error)) => Err(error),
            _ => Err(ShareStoreError::NotFinished),
        }
    }

    fn abort(&mut self) -> Effects {
        match self.state {
            LogState::Read(txn_id) | LogState::Write(txn_id) => {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            _ => smallvec![],
        }
    }
}

/// Reads the use counters of `ids`, in order; a link never used counts zero.
#[derive(Debug, PartialEq)]
pub struct ReadUsesOperation {
    ids: Vec<Ulid>,
    output: Option<Result<Vec<ShareUses>, ShareStoreError>>,
}

impl ReadUsesOperation {
    pub fn new(ids: Vec<Ulid>) -> Self {
        Self { ids, output: None }
    }
}

impl Operation for ReadUsesOperation {
    type Output = Vec<ShareUses>;
    type Error = ShareStoreError;

    fn start(&mut self) -> Effects {
        if self.ids.is_empty() {
            self.output = Some(Ok(Vec::new()));
            return smallvec![];
        }
        let reads = self
            .ids
            .iter()
            .map(|id| {
                (
                    SHARE_USE_KEYSPACE.to_string(),
                    id.to_bytes().to_vec().into(),
                )
            })
            .collect();
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if self.output.is_none() {
            self.output = Some(match event {
                Event::Storage(StorageEvent::BatchReadResult { values })
                    if values.len() == self.ids.len() =>
                {
                    values
                        .into_iter()
                        .map(|(_, value)| match value {
                            Some(bytes) => ShareUses::from_bytes(&bytes).map_err(Into::into),
                            None => Ok(ShareUses::default()),
                        })
                        .collect()
                }
                event => Err(storage_error(event)),
            });
        }
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<Vec<ShareUses>, ShareStoreError> {
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
