//! Stores repository links with their sealed token, connector index and push queue entry.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::{Duration, SystemTime};

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::{DocumentChange, DocumentOutboxEvent};
use aruna_core::effects::Effect;
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::{
    LINK_CONNECTOR_KEYSPACE, LINK_QUEUE_KEYSPACE, LINK_SECRET_KEYSPACE, REPOSITORY_LINK_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::repository::{
    LinkBusy, LinkFailure, LinkPatch, LinkQueueEntry, LinkReview, LinkStatus, PullCheck,
    PushOutcome, REVIEW_POLL_MS, RemoteState, RepositoryCredential, RepositoryLink,
    RepositoryRecord, connector_link_key, link_key,
};
use aruna_core::rows::{done, values};
use aruna_core::storage_entries::{shard_manifest_entry, sync_revision_entry};
use aruna_core::structs::execution::job::JobId;
use aruna_core::task::{TaskEffect, TaskKey};
use aruna_core::types::{Key, Value};
use byteview::ByteView;
use ulid::Ulid;

use crate::metadata::id_mapping::MappingRoute;
use crate::sync::outbox_record::{new_outbox_record, outbox_write_entry, schedule_drain_effect};
use aruna_core::placement::fence;

/// How often a check waits for a running push before it looks again.
pub(super) const ACTIVE_RETRY_MS: u64 = 30_000;
/// How long a pull check waits for a running pull before it looks again.
const PULL_ACTIVE_MS: u64 = 60_000;

#[derive(Clone, Debug, PartialEq)]
pub enum LinkChange {
    /// Pull links read with the connector's token and have no secret of their own.
    Create {
        link: Box<RepositoryLink>,
        secret: Option<RepositoryCredential>,
    },
    Patch(LinkPatch),
    Rotate(RepositoryCredential),
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
        record: Box<RepositoryRecord>,
    },
    /// Takes the repository's current state as the base, after a review or remote edits.
    Accept(Box<RemoteState>),
    /// Records what a pull check found.
    Checked(PullCheck),
    /// Records the version the running pull imported and the dataset revision it wrote.
    Pulled {
        job_id: JobId,
        record: Box<RepositoryRecord>,
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

impl From<IoError> for LinkError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::Storage(StorageError::TransactionConflict),
            IoError::Storage(error) => Self::Storage(error),
            other => Self::Unexpected(other.to_string()),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum State {
    Read,
    Fence,
    Write,
    Delete,
}

type Step = Result<Next<Option<RepositoryLink>>, LinkError>;

/// Applies one change to a link and its rows in a single transaction, then wakes the link
/// drain and the sync outbox drain when the change queued work for them.
#[derive(Debug, PartialEq)]
pub struct ChangeLinkOperation {
    document_id: Ulid,
    link_id: Ulid,
    change: LinkChange,
    now: SystemTime,
    state: State,
    deletes: Vec<(String, Key)>,
    schedule: bool,
    route: Option<MappingRoute>,
    stored: Option<RepositoryLink>,
    queued: Option<LinkQueueEntry>,
    /// The link as the change leaves it, `None` once deleted.
    changed: Option<Option<RepositoryLink>>,
}

impl ChangeLinkOperation {
    pub fn new(document_id: Ulid, link_id: Ulid, change: LinkChange, now: SystemTime) -> Self {
        Self {
            document_id,
            link_id,
            change,
            now,
            state: State::Read,
            deletes: Vec::new(),
            schedule: false,
            route: None,
            stored: None,
            queued: None,
            changed: None,
        }
    }

    /// Replicates the change to the document's holders along `route`.
    pub fn routed(mut self, route: Option<MappingRoute>) -> Self {
        self.route = route;
        self
    }

    /// Keeps the stored link and queue row, then reads the placement fence when the change
    /// replicates under a generation.
    fn keep_rows(&mut self, result: IoResult) -> Step {
        let [link, queued] = values::<2>(result)?;
        let stored = link
            .map(|bytes| RepositoryLink::from_bytes(&bytes))
            .transpose()?;
        // An unreadable queue row is replaced like a missing one.
        self.queued = queued.and_then(|bytes| postcard::from_bytes(&bytes).ok());
        // A departing holder's close either rejects this write or conflicts with it.
        match self.route.as_ref().filter(|route| route.generation > 0) {
            Some(route) => {
                let row = fence::fence_read(&route.realm_id, &route.placement);
                self.stored = stored;
                self.state = State::Fence;
                Ok(Next::Call(IoCall::Read(vec![row])))
            }
            None => self.plan(stored),
        }
    }

    /// The drains this change woke, run once it committed.
    fn finish(&mut self) -> Step {
        let changed = self.changed.take().flatten();
        let mut calls = Vec::new();
        if self.schedule {
            calls.push(IoCall::Effect(Box::new(schedule_drain(Duration::ZERO))));
        }
        if self
            .route
            .as_ref()
            .is_some_and(|route| !route.peers.is_empty())
        {
            calls.push(IoCall::Effect(Box::new(schedule_drain_effect())));
        }
        // Queue and outbox rows are durable, so the periodic rearm covers a lost timer.
        Ok(match calls.is_empty() {
            true => Next::Done(changed),
            false => Next::Then {
                output: changed,
                calls,
            },
        })
    }

    fn plan(&mut self, stored: Option<RepositoryLink>) -> Step {
        let link_row = (
            REPOSITORY_LINK_KEYSPACE.to_string(),
            ByteView::from(link_key(self.document_id, self.link_id)),
        );
        let queue_row = (LINK_QUEUE_KEYSPACE.to_string(), id_key(self.link_id));
        let now_ms = millis(self.now);
        let mut writes = Vec::new();
        // When the drain should look at the link next, if this change needs a look.
        let mut queue = None;
        let mut settle_only = false;
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
                // A push keeps its check queued, so a job that ends without running still settles.
                if link.pull().is_none() {
                    self.queued = None;
                    settle_only = true;
                    queue = Some(now_ms.saturating_add(ACTIVE_RETRY_MS));
                } else {
                    self.deletes.push(queue_row.clone());
                }
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
                // The check kept at Begin ends with the push it waited for.
                let waited = self
                    .queued
                    .as_ref()
                    .is_some_and(|queued| queued.settle_only);
                if applied && queue.is_none() && waited {
                    self.deletes.push(queue_row.clone());
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
        // Pull links never push: their queue row holds the next repository check, and a change
        // that would look for work checks now. A running pull is looked at again later.
        let mut replace = false;
        if let Some(link) = result.as_mut().filter(|link| link.pull().is_some()) {
            let look_now = queue.take().is_some();
            let running = link.active_job.is_some();
            let enabled = link.status == LinkStatus::Enabled;
            if let Some(pull) = link.pull_mut().filter(|_| enabled) {
                if look_now {
                    pull.next_check_ms = pull.next_check_ms.min(now_ms);
                }
                queue = Some(if running {
                    now_ms.saturating_add(PULL_ACTIVE_MS)
                } else {
                    pull.next_check_ms
                });
                replace = true;
                self.deletes.retain(|row| row != &queue_row);
            } else if !self.deletes.contains(&queue_row) {
                self.deletes.push(queue_row.clone());
            }
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
            // A change queued meanwhile keeps its earlier due time; a pull check is replaced.
            let queued = self.queued.take().filter(|_| !replace);
            let entry = LinkQueueEntry {
                document_id: self.document_id,
                due_at_ms: queued
                    .as_ref()
                    .map_or(due_at_ms, |q| q.due_at_ms.min(due_at_ms)),
                first_at_ms: queued.map_or(now_ms, |queued| queued.first_at_ms),
                settle_only,
            };
            let value = postcard::to_allocvec(&entry).map_err(ConversionError::from)?;
            writes.push((queue_row.0, queue_row.1, ByteView::from(value)));
            self.schedule = true;
        }
        self.changed = Some(result);
        self.state = State::Write;
        Ok(Next::Call(IoCall::Write(writes)))
    }
}

impl Operation for ChangeLinkOperation {
    type Output = Option<RepositoryLink>;
    type Error = LinkError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Step {
        Ok(Next::Call(IoCall::Read(vec![
            (
                REPOSITORY_LINK_KEYSPACE.to_string(),
                ByteView::from(link_key(self.document_id, self.link_id)),
            ),
            (LINK_QUEUE_KEYSPACE.to_string(), id_key(self.link_id)),
        ])))
    }

    fn next(&mut self, result: IoResult) -> Step {
        match (self.state, result) {
            (State::Read, result) => self.keep_rows(result),
            (State::Fence, result) => {
                let [value] = values::<1>(result)?;
                let generation = self.route.as_ref().map_or(0, |route| route.generation);
                if !fence::admits(value.as_ref(), generation) {
                    return Err(LinkError::Fenced);
                }
                let stored = self.stored.take();
                self.plan(stored)
            }
            (State::Write, IoResult::Done) => {
                self.state = State::Delete;
                Ok(Next::Call(IoCall::Delete(std::mem::take(
                    &mut self.deletes,
                ))))
            }
            (State::Delete, IoResult::Done) => self.finish(),
            (_, other) => Err(LinkError::Unexpected(other.label().to_string())),
        }
    }
}

pub fn schedule_drain(after: Duration) -> Effect {
    Effect::Task(TaskEffect::ResetTimer {
        key: TaskKey::DrainLinkQueue,
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
    link: &RepositoryLink,
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

pub(crate) fn id_key(id: Ulid) -> Key {
    ByteView::from(id.to_bytes().to_vec())
}

fn secret_row(
    link_id: Ulid,
    secret: &RepositoryCredential,
) -> Result<(String, Key, Value), ConversionError> {
    Ok((
        LINK_SECRET_KEYSPACE.to_string(),
        id_key(link_id),
        ByteView::from(postcard::to_allocvec(secret)?),
    ))
}

/// The push-check row of `link_id`.
pub(super) fn queue_row(
    link_id: Ulid,
    entry: &LinkQueueEntry,
) -> Result<(String, Key, Value), ConversionError> {
    Ok((
        LINK_QUEUE_KEYSPACE.to_string(),
        id_key(link_id),
        ByteView::from(postcard::to_allocvec(entry)?),
    ))
}

/// Deletes the queued push check of `link_id`, or with `retry_at` moves it there, in one write
/// transaction. A change written since `queued` was read stays, due no later than `retry_at`;
/// a removed check stays removed.
#[derive(Debug, PartialEq)]
pub(super) struct SettleCheckOperation {
    link_id: Ulid,
    queued: Value,
    retry_at: Option<u64>,
    written: bool,
}

impl SettleCheckOperation {
    pub(super) fn new(link_id: Ulid, queued: Value, retry_at: Option<u64>) -> Self {
        Self {
            link_id,
            queued,
            retry_at,
            written: false,
        }
    }

    fn settle(&mut self, current: Option<Value>) -> Result<Next<()>, LinkError> {
        let unchanged = current.as_ref() == Some(&self.queued);
        let entry = |bytes: &Value| postcard::from_bytes::<LinkQueueEntry>(bytes).ok();
        let call = match (current.as_ref(), self.retry_at) {
            (Some(_), None) if unchanged => {
                let row = (LINK_QUEUE_KEYSPACE.to_string(), id_key(self.link_id));
                IoCall::Delete(vec![row])
            }
            (Some(current), Some(retry_at)) => {
                let base = if unchanged { None } else { entry(current) };
                let Some(mut next) = base.clone().or_else(|| entry(&self.queued)) else {
                    return Err(LinkError::Unexpected("unreadable link queue entry".into()));
                };
                next.due_at_ms = base.map_or(retry_at, |base| base.due_at_ms.min(retry_at));
                IoCall::Write(vec![queue_row(self.link_id, &next)?])
            }
            _ => return Ok(Next::Done(())),
        };
        self.written = true;
        Ok(Next::Call(call))
    }
}

impl Operation for SettleCheckOperation {
    type Output = ();
    type Error = LinkError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<()>, LinkError> {
        let row = (LINK_QUEUE_KEYSPACE.to_string(), id_key(self.link_id));
        Ok(Next::Call(IoCall::Read(vec![row])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, LinkError> {
        if self.written {
            done(result)?;
            return Ok(Next::Done(()));
        }
        let [current] = values::<1>(result)?;
        self.settle(current)
    }
}

#[cfg(test)]
#[path = "links_tests.rs"]
mod tests;
