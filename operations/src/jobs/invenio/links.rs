//! Stores Invenio links with their sealed token, connector index and push queue entry.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::{Duration, SystemTime};

use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::invenio::{
    InvenioCredential, InvenioLink, LinkBusy, LinkPatch, LinkQueueEntry, LinkStatus, PushOutcome,
    connector_link_key, link_key, link_prefix,
};
use aruna_core::keyspaces::{
    INVENIO_LINK_KEYSPACE, LINK_CONNECTOR_KEYSPACE, LINK_QUEUE_KEYSPACE, LINK_SECRET_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::JobId;
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::{Effects, Key, TxnId, Value};
use aruna_storage::StorageHandle;
use byteview::ByteView;
use smallvec::smallvec;
use ulid::Ulid;

const LINK_PAGE: usize = 256;

#[derive(Clone, Debug, PartialEq)]
pub enum LinkChange {
    Create {
        link: Box<InvenioLink>,
        secret: InvenioCredential,
    },
    Patch(LinkPatch),
    Rotate(InvenioCredential),
    Begin(JobId),
    /// `requeue` asks for another comparison because the dataset moved on during the push.
    Finish {
        job_id: JobId,
        outcome: Box<PushOutcome>,
        requeue: bool,
    },
    Delete,
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum LinkError {
    #[error("repository link not found")]
    NotFound,
    #[error("repository link already exists")]
    Exists,
    #[error("the token belongs to another link")]
    ForeignToken,
    #[error(transparent)]
    Busy(#[from] LinkBusy),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("push job could not start: {0}")]
    Submit(String),
    #[error("the dataset has no revision to push")]
    NoRevision,
    #[error("unexpected link storage event: {0}")]
    Unexpected(String),
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum State {
    Init,
    Start,
    Read,
    Write,
    Delete,
    Commit,
    Schedule,
    Aborting,
    Done,
}

/// Applies one change to a link and its rows in a single transaction.
#[derive(Debug, PartialEq)]
pub struct ChangeLinkOperation {
    document_id: Ulid,
    link_id: Ulid,
    change: LinkChange,
    now: SystemTime,
    state: State,
    txn_id: Option<TxnId>,
    deletes: Vec<(String, Key)>,
    schedule: bool,
    output: Option<Result<Option<InvenioLink>, LinkError>>,
}

impl ChangeLinkOperation {
    pub fn new(document_id: Ulid, link_id: Ulid, change: LinkChange) -> Self {
        Self {
            document_id,
            link_id,
            change,
            now: SystemTime::now(),
            state: State::Init,
            txn_id: None,
            deletes: Vec::new(),
            schedule: false,
            output: None,
        }
    }

    fn fail(&mut self, error: LinkError) -> Effects {
        self.output = Some(Err(error));
        self.abort()
    }

    fn plan(&mut self, stored: Option<InvenioLink>) -> Result<Effects, LinkError> {
        let link_row = (
            INVENIO_LINK_KEYSPACE.to_string(),
            ByteView::from(link_key(self.document_id, self.link_id)),
        );
        let queue_row = (LINK_QUEUE_KEYSPACE.to_string(), id_key(self.link_id));
        let mut writes = Vec::new();
        let mut queue = false;
        let result = match (
            std::mem::replace(&mut self.change, LinkChange::Delete),
            stored,
        ) {
            (LinkChange::Create { link, secret }, None) => {
                if secret.link_id != Some(link.link_id) {
                    return Err(LinkError::ForeignToken);
                }
                writes.push(secret_row(self.link_id, &secret)?);
                writes.push((
                    LINK_CONNECTOR_KEYSPACE.to_string(),
                    ByteView::from(connector_link_key(link.connector_id, link.link_id)),
                    ByteView::from(link.document_id.to_bytes().to_vec()),
                ));
                queue = true;
                Some(*link)
            }
            (LinkChange::Create { .. }, Some(_)) => return Err(LinkError::Exists),
            (_, None) => return Err(LinkError::NotFound),
            (LinkChange::Patch(patch), Some(mut link)) => {
                queue = link.patch(&patch, self.now);
                Some(link)
            }
            (LinkChange::Rotate(secret), Some(mut link)) => {
                if secret.link_id != Some(link.link_id) {
                    return Err(LinkError::ForeignToken);
                }
                writes.push(secret_row(self.link_id, &secret)?);
                queue = link.rotate(self.now);
                Some(link)
            }
            (LinkChange::Begin(job_id), Some(mut link)) => {
                link.begin(job_id, self.now)?;
                self.deletes.push(queue_row.clone());
                Some(link)
            }
            (
                LinkChange::Finish {
                    job_id,
                    outcome,
                    requeue,
                },
                Some(mut link),
            ) => {
                let applied = link.finish(job_id, &outcome, self.now);
                queue = applied && requeue && link.status == LinkStatus::Enabled;
                Some(link)
            }
            (LinkChange::Delete, Some(link)) => {
                self.deletes.extend([
                    link_row.clone(),
                    queue_row.clone(),
                    (LINK_SECRET_KEYSPACE.to_string(), id_key(self.link_id)),
                    (
                        LINK_CONNECTOR_KEYSPACE.to_string(),
                        ByteView::from(connector_link_key(link.connector_id, link.link_id)),
                    ),
                ]);
                None
            }
        };
        if let Some(link) = &result {
            writes.push((link_row.0, link_row.1, ByteView::from(link.to_bytes()?)));
        }
        if queue {
            let entry = LinkQueueEntry {
                document_id: self.document_id,
                due_at_ms: unix_timestamp_millis(),
            };
            let value = postcard::to_allocvec(&entry).map_err(ConversionError::from)?;
            writes.push((queue_row.0, queue_row.1, ByteView::from(value)));
            self.schedule = true;
        }
        self.output = Some(Ok(result));
        Ok(if writes.is_empty() {
            self.delete_rows()
        } else {
            self.state = State::Write;
            smallvec![Effect::Storage(StorageEffect::BatchWrite {
                writes,
                txn_id: self.txn_id,
            })]
        })
    }

    fn delete_rows(&mut self) -> Effects {
        if self.deletes.is_empty() {
            return self.commit();
        }
        self.state = State::Delete;
        smallvec![Effect::Storage(StorageEffect::BatchDelete {
            deletes: std::mem::take(&mut self.deletes),
            txn_id: self.txn_id,
        })]
    }

    fn commit(&mut self) -> Effects {
        match self.txn_id {
            Some(txn_id) => {
                self.state = State::Commit;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            None => self.fail(LinkError::Unexpected("missing transaction".into())),
        }
    }
}

impl Operation for ChangeLinkOperation {
    type Output = Option<InvenioLink>;
    type Error = LinkError;

    fn start(&mut self) -> Effects {
        self.state = State::Start;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        if self.state == State::Aborting {
            self.state = State::Done;
            return smallvec![];
        }
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }
        match (self.state, event) {
            (State::Start, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.txn_id = Some(txn_id);
                self.state = State::Read;
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: INVENIO_LINK_KEYSPACE.to_string(),
                    key: ByteView::from(link_key(self.document_id, self.link_id)),
                    txn_id: Some(txn_id),
                })]
            }
            (State::Read, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                let planned = value
                    .map(|bytes| InvenioLink::from_bytes(&bytes))
                    .transpose()
                    .map_err(LinkError::from)
                    .and_then(|stored| self.plan(stored));
                planned.unwrap_or_else(|error| self.fail(error))
            }
            (State::Write, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                self.delete_rows()
            }
            (State::Delete, Event::Storage(StorageEvent::BatchDeleteResult { .. })) => {
                self.commit()
            }
            (State::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.txn_id = None;
                if !self.schedule {
                    self.state = State::Done;
                    return smallvec![];
                }
                self.state = State::Schedule;
                smallvec![schedule_drain(Duration::ZERO)]
            }
            // The queue row is durable, so the periodic rearm covers a lost timer.
            (State::Schedule, Event::Task(_)) => {
                self.state = State::Done;
                smallvec![]
            }
            (_, other) => self.fail(LinkError::Unexpected(format!("{other:?}"))),
        }
    }

    fn is_complete(&self) -> bool {
        self.state == State::Done
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match (self.state, self.output) {
            (State::Done, Some(output)) => output,
            (_, Some(Err(error))) => Err(error),
            _ => Err(LinkError::Unexpected("link change did not finish".into())),
        }
    }

    fn abort(&mut self) -> Effects {
        if matches!(self.output, Some(Ok(_))) && self.state != State::Commit {
            self.output = Some(Err(LinkError::Unexpected("link change aborted".into())));
        }
        match self.txn_id.take() {
            Some(txn_id) if self.state != State::Commit => {
                self.state = State::Aborting;
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            _ => {
                self.state = State::Done;
                smallvec![]
            }
        }
    }
}

pub fn schedule_drain(after: Duration) -> Effect {
    Effect::Task(TaskEffect::ResetTimer {
        key: TaskKey::DrainLinkQueue,
        after,
    })
}

pub(crate) fn id_key(id: Ulid) -> Key {
    ByteView::from(id.to_bytes().to_vec())
}

fn secret_row(
    link_id: Ulid,
    secret: &InvenioCredential,
) -> Result<(String, Key, Value), ConversionError> {
    Ok((
        LINK_SECRET_KEYSPACE.to_string(),
        id_key(link_id),
        ByteView::from(postcard::to_allocvec(secret)?),
    ))
}

async fn send(storage: &StorageHandle, effect: StorageEffect) -> Result<Event, LinkError> {
    match storage.send_storage_effect(effect).await {
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        event => Ok(event),
    }
}

/// The document's links with whether each still has a push check queued.
pub async fn list_links(
    storage: &StorageHandle,
    document_id: Ulid,
) -> Result<Vec<(InvenioLink, bool)>, LinkError> {
    let mut links = Vec::new();
    let mut start = None;
    loop {
        let event = send(
            storage,
            StorageEffect::Iter {
                key_space: INVENIO_LINK_KEYSPACE.to_string(),
                prefix: Some(ByteView::from(link_prefix(document_id))),
                start: start.take().map(IterStart::After),
                limit: LINK_PAGE,
                txn_id: None,
            },
        )
        .await?;
        let Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) = event
        else {
            return Err(LinkError::Unexpected(format!("{event:?}")));
        };
        for (_, value) in values {
            links.push(InvenioLink::from_bytes(&value)?);
        }
        match next_start_after {
            Some(next) => start = Some(next),
            None => break,
        }
    }
    let reads = links
        .iter()
        .map(|link| (LINK_QUEUE_KEYSPACE.to_string(), id_key(link.link_id)))
        .collect::<Vec<_>>();
    if reads.is_empty() {
        return Ok(Vec::new());
    }
    let event = send(
        storage,
        StorageEffect::BatchRead {
            reads,
            txn_id: None,
        },
    )
    .await?;
    let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
        return Err(LinkError::Unexpected(format!("{event:?}")));
    };
    Ok(links
        .into_iter()
        .zip(values)
        .map(|(link, (_, queued))| (link, queued.is_some()))
        .collect())
}

pub async fn read_link(
    storage: &StorageHandle,
    document_id: Ulid,
    link_id: Ulid,
) -> Result<Option<InvenioLink>, LinkError> {
    let event = send(
        storage,
        StorageEffect::Read {
            key_space: INVENIO_LINK_KEYSPACE.to_string(),
            key: ByteView::from(link_key(document_id, link_id)),
            txn_id: None,
        },
    )
    .await?;
    match event {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value
            .map(|bytes| InvenioLink::from_bytes(&bytes))
            .transpose()?),
        other => Err(LinkError::Unexpected(format!("{other:?}"))),
    }
}

pub(crate) async fn read_secret(
    storage: &StorageHandle,
    link_id: Ulid,
) -> Result<Option<InvenioCredential>, LinkError> {
    let event = send(
        storage,
        StorageEffect::Read {
            key_space: LINK_SECRET_KEYSPACE.to_string(),
            key: id_key(link_id),
            txn_id: None,
        },
    )
    .await?;
    match event {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value
            .map(|bytes| postcard::from_bytes(&bytes).map_err(ConversionError::from))
            .transpose()?),
        other => Err(LinkError::Unexpected(format!("{other:?}"))),
    }
}

#[cfg(test)]
#[path = "links_tests.rs"]
mod tests;
