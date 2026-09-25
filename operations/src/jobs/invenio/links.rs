//! Stores Invenio links with their sealed token, connector index and push queue entry.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::{Duration, SystemTime};

use aruna_core::document::{DocumentChange, DocumentOutboxEvent};
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::invenio::{
    InvenioCredential, InvenioLink, InvenioRecord, LinkBusy, LinkFailure, LinkPatch,
    LinkQueueEntry, LinkReview, LinkStatus, PullCheck, PushOutcome, REVIEW_POLL_MS, RemoteState,
    connector_link_key, link_key, link_prefix,
};
use aruna_core::keyspaces::{
    INVENIO_LINK_KEYSPACE, LINK_CONNECTOR_KEYSPACE, LINK_QUEUE_KEYSPACE, LINK_SECRET_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::storage_entries::{shard_manifest_entry, sync_revision_entry};
use aruna_core::structs::execution::job::JobId;
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::types::{Effects, Key, TxnId, Value};
use aruna_storage::StorageHandle;
use byteview::ByteView;
use smallvec::smallvec;
use ulid::Ulid;

use crate::driver::{DriverContext, drive};
use crate::metadata::persistent_id::{MappingRoute, mapping_route};
use crate::placement::fence;
use crate::sync::document_outbox::{new_outbox_record, outbox_write_entry, schedule_drain_effect};

const LINK_PAGE: usize = 256;

#[derive(Clone, Debug, PartialEq)]
pub enum LinkChange {
    /// Pull links read with the connector's token and have no secret of their own.
    Create {
        link: Box<InvenioLink>,
        secret: Option<InvenioCredential>,
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
    /// Stops pushing with this reason; only an enabled link changes.
    Fail(LinkFailure),
    /// Stores the draft the running push created, before it uploads anything.
    Draft {
        job_id: JobId,
        record: Box<InvenioRecord>,
    },
    /// Takes the repository's current state as the base, after a review or remote edits.
    Accept(Box<RemoteState>),
    /// Records what a pull check found.
    Checked(PullCheck),
    /// Records the version the running pull imported and the dataset revision it wrote.
    Pulled {
        job_id: JobId,
        record: Box<InvenioRecord>,
        revision: Ulid,
    },
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
    #[error("this link is managed on its owner node {0}")]
    NotOwner(String),
    #[error("the dataset placement is moving; retry the link change")]
    Fenced,
    #[error("the link creator has {0} active jobs; push again once one has finished")]
    JobLimit(u32),
    #[error("the link's node no longer holds the dataset (owner_not_holder)")]
    NotHolder,
    #[error("an enabled link already pushes or pulls this record lineage in the other direction")]
    Lineage,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum State {
    Init,
    Start,
    Read,
    Fence,
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
    /// The change needs a pull check soon, so the pull timer fires now.
    pull_check: bool,
    route: Option<MappingRoute>,
    stored: Option<InvenioLink>,
    queued: Option<LinkQueueEntry>,
    pending: usize,
    output: Option<Result<Option<InvenioLink>, LinkError>>,
}

impl ChangeLinkOperation {
    pub fn new(document_id: Ulid, link_id: Ulid, change: LinkChange, now: SystemTime) -> Self {
        Self {
            document_id,
            link_id,
            change,
            now,
            state: State::Init,
            txn_id: None,
            deletes: Vec::new(),
            schedule: false,
            pull_check: false,
            route: None,
            stored: None,
            queued: None,
            pending: 0,
            output: None,
        }
    }

    /// Replicates the change to the document's holders along `route`.
    pub fn routed(mut self, route: Option<MappingRoute>) -> Self {
        self.route = route;
        self
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
        let now_ms = millis(self.now);
        let mut writes = Vec::new();
        // When the drain should look at the link next, if this change needs a look.
        let mut queue = None;
        let result = match (
            std::mem::replace(&mut self.change, LinkChange::Delete),
            stored,
        ) {
            (LinkChange::Create { link, secret }, None) => {
                if let Some(secret) = secret {
                    if secret.link_id != Some(link.link_id) {
                        return Err(LinkError::ForeignToken);
                    }
                    writes.push(secret_row(self.link_id, &secret)?);
                }
                writes.push((
                    LINK_CONNECTOR_KEYSPACE.to_string(),
                    ByteView::from(connector_link_key(link.connector_id, link.link_id)),
                    ByteView::from(link.document_id.to_bytes().to_vec()),
                ));
                queue = link.pull().is_none().then_some(now_ms);
                // A new pull link arms the check, which then waits for its first due time.
                self.pull_check = link.pull().is_some();
                Some(*link)
            }
            (LinkChange::Create { .. }, Some(_)) => return Err(LinkError::Exists),
            (_, None) => return Err(LinkError::NotFound),
            (LinkChange::Patch(patch), Some(mut link)) => {
                queue = if link.patch(&patch, self.now) {
                    Some(now_ms)
                } else {
                    link.publish_due_ms()
                        .filter(|_| patch.auto_publish == Some(true))
                };
                Some(link)
            }
            (LinkChange::Rotate(secret), Some(mut link)) => {
                if secret.link_id != Some(link.link_id) {
                    return Err(LinkError::ForeignToken);
                }
                writes.push(secret_row(self.link_id, &secret)?);
                queue = link.rotate(self.now).then_some(now_ms);
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
                if applied && link.status == LinkStatus::Enabled {
                    let review = (link.remote.review == LinkReview::Pending)
                        .then(|| now_ms.saturating_add(REVIEW_POLL_MS));
                    queue = requeue
                        .then_some(now_ms)
                        .or(review)
                        .or(link.publish_due_ms());
                }
                Some(link)
            }
            (LinkChange::Draft { job_id, record }, Some(mut link)) => {
                if !link.draft(job_id, &record, self.now) {
                    return Err(LinkError::Busy(LinkBusy));
                }
                Some(link)
            }
            (LinkChange::Accept(state), Some(mut link)) => {
                link.accept(&state, self.now);
                queue = Some(now_ms);
                Some(link)
            }
            (LinkChange::Checked(check), Some(mut link)) => {
                link.checked(&check, self.now);
                Some(link)
            }
            (
                LinkChange::Pulled {
                    job_id,
                    record,
                    revision,
                },
                Some(mut link),
            ) => {
                link.pulled(job_id, &record, revision, self.now);
                Some(link)
            }
            (LinkChange::Fail(reason), Some(mut link)) => {
                if link.status == LinkStatus::Enabled {
                    link.status = LinkStatus::Failed { reason };
                    link.updated_at = self.now;
                }
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
                if let Some(route) = &self.route {
                    let change = link.delete_change(route.placement);
                    writes.extend(sync_rows(route, &link, change, None)?);
                }
                None
            }
        };
        let mut result = result.map(|mut link| {
            link.stamp(now_ms);
            link
        });
        // Pull links never push; a change that would look for work checks the repository instead.
        if let Some(pull) = result.as_mut().and_then(InvenioLink::pull_mut)
            && queue.take().is_some()
        {
            pull.next_check_ms = pull.next_check_ms.min(now_ms);
            self.pull_check = true;
        }
        if let Some(link) = &result {
            let bytes = link.to_bytes()?;
            if let Some(route) = &self.route {
                let change = link.sync_change(route.placement);
                writes.extend(sync_rows(route, link, change, Some(bytes.clone()))?);
            }
            writes.push((link_row.0, link_row.1, ByteView::from(bytes)));
        }
        if let Some(due_at_ms) = queue {
            // A change queued meanwhile keeps its earlier due time.
            let queued = self.queued.take();
            let entry = LinkQueueEntry {
                document_id: self.document_id,
                due_at_ms: queued
                    .as_ref()
                    .map_or(due_at_ms, |q| q.due_at_ms.min(due_at_ms)),
                first_at_ms: queued.map_or(now_ms, |queued| queued.first_at_ms),
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
            return match event {
                Event::Storage(
                    StorageEvent::TransactionAborted { .. } | StorageEvent::Error { .. },
                ) => {
                    self.state = State::Done;
                    smallvec![]
                }
                other => {
                    self.state = State::Done;
                    self.output = Some(Err(LinkError::Unexpected(format!("{other:?}"))));
                    smallvec![]
                }
            };
        }
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }
        match (self.state, event) {
            (State::Start, Event::Storage(StorageEvent::TransactionStarted { txn_id })) => {
                self.txn_id = Some(txn_id);
                self.state = State::Read;
                smallvec![Effect::Storage(StorageEffect::BatchRead {
                    reads: vec![
                        (
                            INVENIO_LINK_KEYSPACE.to_string(),
                            ByteView::from(link_key(self.document_id, self.link_id)),
                        ),
                        (LINK_QUEUE_KEYSPACE.to_string(), id_key(self.link_id)),
                    ],
                    txn_id: Some(txn_id),
                })]
            }
            (State::Read, Event::Storage(StorageEvent::BatchReadResult { mut values }))
                if values.len() == 2 =>
            {
                let queued = values.pop().and_then(|(_, value)| value);
                let link = values.pop().and_then(|(_, value)| value);
                let stored = match link
                    .map(|bytes| InvenioLink::from_bytes(&bytes))
                    .transpose()
                {
                    Ok(stored) => stored,
                    Err(error) => return self.fail(error.into()),
                };
                // An unreadable queue row is replaced like a missing one.
                self.queued = queued.and_then(|bytes| postcard::from_bytes(&bytes).ok());
                // A departing holder's close either rejects this write or conflicts with it.
                match self.route.as_ref().filter(|route| route.generation > 0) {
                    Some(route) => {
                        let (key_space, key) = fence::fence_read(&route.realm_id, &route.placement);
                        self.stored = stored;
                        self.state = State::Fence;
                        smallvec![Effect::Storage(StorageEffect::Read {
                            key_space,
                            key,
                            txn_id: self.txn_id,
                        })]
                    }
                    None => self.plan(stored).unwrap_or_else(|error| self.fail(error)),
                }
            }
            (State::Fence, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                let generation = self.route.as_ref().map_or(0, |route| route.generation);
                if !fence::admits(value.as_ref(), generation) {
                    return self.fail(LinkError::Fenced);
                }
                let stored = self.stored.take();
                self.plan(stored).unwrap_or_else(|error| self.fail(error))
            }
            (State::Write, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                self.delete_rows()
            }
            (State::Delete, Event::Storage(StorageEvent::BatchDeleteResult { .. })) => {
                self.commit()
            }
            (State::Commit, Event::Storage(StorageEvent::TransactionCommitted { .. })) => {
                self.txn_id = None;
                let mut effects = Effects::new();
                if self.schedule {
                    effects.push(schedule_drain(Duration::ZERO));
                }
                if self.pull_check {
                    effects.push(schedule_pulls(Duration::ZERO));
                }
                if self
                    .route
                    .as_ref()
                    .is_some_and(|route| !route.peers.is_empty())
                {
                    effects.push(schedule_drain_effect());
                }
                self.pending = effects.len();
                self.state = if effects.is_empty() {
                    State::Done
                } else {
                    State::Schedule
                };
                effects
            }
            // Queue and outbox rows are durable, so the periodic rearm covers a lost timer.
            (State::Schedule, Event::Task(_)) => {
                self.pending = self.pending.saturating_sub(1);
                if self.pending == 0 {
                    self.state = State::Done;
                }
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

pub fn schedule_pulls(after: Duration) -> Effect {
    Effect::Task(TaskEffect::ResetTimer {
        key: TaskKey::CheckPullLinks,
        after,
    })
}

fn millis(time: SystemTime) -> u64 {
    time.duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
}

/// Sidecar, manifest entry and outbox publish that replicate one stored link change.
/// Without `bytes` the change removes the row.
fn sync_rows(
    route: &MappingRoute,
    link: &InvenioLink,
    change: DocumentChange,
    bytes: Option<Vec<u8>>,
) -> Result<Vec<(String, Key, Value)>, ConversionError> {
    let event = match bytes {
        Some(bytes) => DocumentOutboxEvent::Upsert { bytes, change },
        None => DocumentOutboxEvent::Delete { change },
    };
    let target = link.target();
    let mut rows = vec![sync_revision_entry(&target, &change)?];
    rows.extend(shard_manifest_entry(&target, &change)?);
    if !route.peers.is_empty() {
        let record = new_outbox_record(
            route.actor,
            target,
            route.peers.clone(),
            event,
            route.placement,
            false,
        )
        .fenced_at(route.generation);
        rows.push(outbox_write_entry(&record)?);
    }
    Ok(rows)
}

/// Applies a change on the link's owner node and replicates it to the document's holders.
pub async fn change_link(
    context: &DriverContext,
    link: &InvenioLink,
    change: LinkChange,
) -> Result<Option<InvenioLink>, LinkError> {
    let route = match context.net_handle.as_ref() {
        Some(net) if net.node_id() != link.owner_node => {
            return Err(LinkError::NotOwner(link.owner_node_url.clone()));
        }
        Some(net) => mapping_route(context, *net.realm_id(), link.document_id)
            .await
            .map_err(|error| LinkError::Unexpected(error.to_string()))?,
        None => None,
    };
    let enabled = match &change {
        LinkChange::Create { link, .. } => Some(link.as_ref()),
        LinkChange::Patch(patch) if patch.paused == Some(false) => Some(link),
        _ => None,
    };
    if let Some(enabled) = enabled {
        Box::pin(ensure_lineage(&context.storage_handle, enabled)).await?;
    }
    let operation =
        ChangeLinkOperation::new(link.document_id, link.link_id, change, SystemTime::now())
            .routed(route);
    drive(operation, context).await
}

/// One lineage cannot have an enabled push link and an enabled pull link on one dataset.
async fn ensure_lineage(storage: &StorageHandle, link: &InvenioLink) -> Result<(), LinkError> {
    let pulls = link.pull().is_some();
    let conflict = list_links(storage, link.document_id)
        .await?
        .into_iter()
        .any(|(other, _)| {
            other.link_id != link.link_id
                && other.status == LinkStatus::Enabled
                && other.pull().is_some() != pulls
                && other.same_lineage(link)
        });
    if conflict {
        return Err(LinkError::Lineage);
    }
    Ok(())
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
