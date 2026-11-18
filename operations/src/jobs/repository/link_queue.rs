//! Queues link push checks after dataset changes and starts one push job per changed revision.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::time::Duration;

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::errors::StorageError;
use aruna_core::events::Event;
use aruna_core::keyspaces::LINK_QUEUE_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::placement::holds_placement;
use aruna_core::repository::{
    LinkFailure, LinkQueueEntry, LinkReview, LinkStatus, PushOutcome, REVIEW_POLL_MS,
    RemoteState, RepositoryLink,
};
use aruna_core::rows::{PrefixPages, all_values, done, event};
use aruna_core::structs::execution::job::{ExportRoCrateSpec, JobId, JobRecord, JobState};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::task::TaskEvent;
use aruna_core::types::{Key, Value};
use byteview::ByteView;
use tracing::warn;
use ulid::Ulid;

use super::invenio::remote::ReviewStateOperation;
use super::link_store::{
    ApplyChangeOperation, DocumentLinksOperation, EnsureLineageOperation, link_row, parse_link,
    time_of,
};
use super::links::{
    ACTIVE_RETRY_MS, ChangeLinkOperation, LinkChange, LinkError, SettleCheckOperation, id_key,
    queue_row, schedule_drain,
};
use super::pull::CheckDueOperation;
use super::push::{creator_auth, parse_checkpoint, read_checkpoint};
use super::submit::{LinkJob, SubmitLinkJobOperation};
use super::{NodeScope, TransferError};
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::jobs::store::job_rows::ReadJobOperation;
use crate::jobs::submit::{SubmitJobError, SubmitJobResult};
use crate::metadata::create_document::resolve_metadata_id;
use crate::metadata::raw_events::RawReadError;
use crate::metadata::repository::{parse_registry_read, read_document_registry};
use crate::metadata::revision_read::{RawViewOperation, RawViewOutput};
use crate::tasks::queue_backoff::{due_after, min_due_at};

const QUEUE_PAGE: usize = 256;
const ERROR_RETRY_MS: u64 = 60_000;

type Turn<T> = Result<Next<T>, LinkError>;

fn child<T>(call: IoCall) -> Turn<T> {
    Ok(Next::Call(call))
}

fn registry_read(document_id: Ulid) -> IoCall {
    read_document_registry(document_id, None).into()
}

fn registry_row(result: IoResult) -> Result<Option<MetadataRegistryRecord>, LinkError> {
    parse_registry_read(event(result)?).map_err(|error| LinkError::Unexpected(format!("{error:?}")))
}

fn settle_call(link_id: Ulid, queued: &Value, retry_at: Option<u64>) -> IoCall {
    IoCall::child(SettleCheckOperation::new(link_id, queued.clone(), retry_at))
}

/// A conflicting write queued a newer change; its check runs later.
fn settled(result: IoResult) -> Result<(), LinkError> {
    match result.child::<(), LinkError>()? {
        Err(LinkError::Storage(StorageError::TransactionConflict)) => Ok(()),
        other => other,
    }
}

#[derive(Debug, PartialEq)]
enum QueueStep {
    Links,
    Registry(Ulid, Vec<RepositoryLink>),
    Queued,
}

/// Push-check rows for the push links of changed documents that push or retry after unmet
/// requirements; each change moves the due time. A deleted document queues all its links, so
/// the check removes them.
#[derive(Debug, PartialEq)]
pub(crate) struct QueueRowsOperation {
    documents: VecDeque<Ulid>,
    candidates: Vec<(Ulid, Ulid)>,
    now_ms: u64,
    step: QueueStep,
}

impl QueueRowsOperation {
    pub(crate) fn new(documents: impl IntoIterator<Item = Ulid>) -> Self {
        Self {
            documents: documents.into_iter().collect(),
            candidates: Vec::new(),
            now_ms: 0,
            step: QueueStep::Links,
        }
    }

    fn next_document(&mut self) -> Turn<Vec<(String, Key, Value)>> {
        if let Some(document_id) = self.documents.front() {
            self.step = QueueStep::Links;
            return child(IoCall::child(DocumentLinksOperation::new(*document_id)));
        }
        if self.candidates.is_empty() {
            return Ok(Next::Done(Vec::new()));
        }
        let reads = self
            .candidates
            .iter()
            .map(|(link_id, _)| (LINK_QUEUE_KEYSPACE.to_string(), id_key(*link_id)))
            .collect();
        self.step = QueueStep::Queued;
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn add_links(&mut self, document_id: Ulid, links: Vec<RepositoryLink>, gone: bool) {
        for link in links {
            let pushes = link.status == LinkStatus::Enabled || link.retries_on_change();
            if gone || (pushes && link.pull().is_none()) {
                self.candidates.push((link.link_id, document_id));
            }
        }
    }
}

impl Operation for QueueRowsOperation {
    type Output = Vec<(String, Key, Value)>;
    type Error = LinkError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Turn<Self::Output> {
        self.next_document()
    }

    fn next(&mut self, result: IoResult) -> Turn<Self::Output> {
        match std::mem::replace(&mut self.step, QueueStep::Links) {
            QueueStep::Links => {
                let links = result.child::<Vec<RepositoryLink>, LinkError>()??;
                let document_id = self
                    .documents
                    .pop_front()
                    .ok_or_else(|| LinkError::Unexpected("no document queued".into()))?;
                if links.is_empty() {
                    return self.next_document();
                }
                self.step = QueueStep::Registry(document_id, links);
                Ok(Next::Call(registry_read(document_id)))
            }
            QueueStep::Registry(document_id, links) => {
                let gone = registry_row(result)?.is_none();
                self.add_links(document_id, links, gone);
                self.next_document()
            }
            QueueStep::Queued => {
                let queued = all_values(result)?;
                let mut rows = Vec::with_capacity(self.candidates.len());
                for ((link_id, document_id), existing) in self.candidates.drain(..).zip(queued) {
                    let existing = existing.and_then(|bytes| postcard::from_bytes(&bytes).ok());
                    let entry =
                        LinkQueueEntry::debounce(document_id, existing.as_ref(), self.now_ms);
                    rows.push(queue_row(link_id, &entry)?);
                }
                Ok(Next::Done(rows))
            }
        }
    }
}

fn queue_pages() -> PrefixPages {
    PrefixPages::new(LINK_QUEUE_KEYSPACE, ByteView::from(Vec::new()), QUEUE_PAGE)
}

#[derive(Debug, PartialEq)]
enum DrainStep {
    Page,
    Removed,
    Check(Ulid, Value),
    Settle(u64),
}

/// Runs every due check and returns the wait until the next one.
#[derive(Debug, PartialEq)]
pub struct DrainLinksOperation {
    scope: NodeScope,
    pages: PrefixPages,
    rows: VecDeque<(Key, Value)>,
    next_due: Option<u64>,
    started_ms: Option<u64>,
    now_ms: u64,
    step: DrainStep,
}

impl DrainLinksOperation {
    pub fn new(scope: NodeScope) -> Self {
        Self {
            scope,
            pages: queue_pages(),
            rows: VecDeque::new(),
            next_due: None,
            started_ms: None,
            now_ms: 0,
            step: DrainStep::Page,
        }
    }

    fn started(&self) -> u64 {
        self.started_ms.unwrap_or(self.now_ms)
    }

    fn advance(&mut self) -> Turn<Option<Duration>> {
        let now = self.started();
        while let Some((key, value)) = self.rows.pop_front() {
            let parsed = <[u8; 16]>::try_from(key.as_ref())
                .ok()
                .zip(postcard::from_bytes::<LinkQueueEntry>(&value).ok());
            let Some((link_id, entry)) = parsed else {
                warn!(key = ?key.to_vec(), "Deleting malformed link queue entry");
                self.step = DrainStep::Removed;
                let row = (LINK_QUEUE_KEYSPACE.to_string(), key);
                return Ok(Next::Call(IoCall::Delete(vec![row])));
            };
            if entry.due_at_ms > now {
                self.next_due = min_due_at(self.next_due, entry.due_at_ms);
                continue;
            }
            let link_id = Ulid::from_bytes(link_id);
            let check = CheckLinkOperation::new(self.scope.clone(), link_id, entry, value.clone());
            self.step = DrainStep::Check(link_id, value);
            return child(IoCall::child(check));
        }
        if self.pages.done() {
            let wait = self.next_due.map(|due| due_after(self.now_ms, due));
            return Ok(Next::Done(wait));
        }
        self.step = DrainStep::Page;
        Ok(Next::Call(self.pages.call()))
    }
}

impl Operation for DrainLinksOperation {
    type Output = Option<Duration>;
    type Error = LinkError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
        self.started_ms.get_or_insert(now_ms);
    }

    fn first(&mut self) -> Turn<Option<Duration>> {
        Ok(Next::Call(self.pages.call()))
    }

    fn next(&mut self, result: IoResult) -> Turn<Option<Duration>> {
        match std::mem::replace(&mut self.step, DrainStep::Page) {
            DrainStep::Page => {
                self.rows = self.pages.take(result)?.into();
                self.advance()
            }
            DrainStep::Removed => {
                done(result)?;
                self.advance()
            }
            DrainStep::Check(link_id, queued) => {
                let checked = result.child::<Option<u64>, LinkError>()?;
                let retry_at = checked.unwrap_or_else(|error| {
                    warn!(%link_id, %error, "Repository link push check failed");
                    Some(self.started().saturating_add(ERROR_RETRY_MS))
                });
                match retry_at {
                    Some(retry_at) => {
                        self.step = DrainStep::Settle(retry_at);
                        child(settle_call(link_id, &queued, Some(retry_at)))
                    }
                    None => self.advance(),
                }
            }
            DrainStep::Settle(retry_at) => {
                settled(result)?;
                self.next_due = min_due_at(self.next_due, retry_at);
                self.advance()
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum CheckStep {
    Link,
    Registry,
    Delete,
    Holder,
    Job(JobId),
    Stale,
    Pull,
    Review,
    Revision,
    Push,
    Drop,
}

/// Starts a push when the dataset moved on; answers when to look again if it must wait.
#[derive(Debug, PartialEq)]
pub(crate) struct CheckLinkOperation {
    scope: NodeScope,
    link_id: Ulid,
    entry: LinkQueueEntry,
    queued: Value,
    link: Option<RepositoryLink>,
    now_ms: u64,
    step: CheckStep,
}

impl CheckLinkOperation {
    pub(crate) fn new(
        scope: NodeScope,
        link_id: Ulid,
        entry: LinkQueueEntry,
        queued: Value,
    ) -> Self {
        Self {
            scope,
            link_id,
            entry,
            queued,
            link: None,
            now_ms: 0,
            step: CheckStep::Link,
        }
    }

    fn link(&self) -> Result<&RepositoryLink, LinkError> {
        self.link
            .as_ref()
            .ok_or_else(|| LinkError::Unexpected("the checked link is missing".into()))
    }

    /// Deletes the queued check unless a change rewrote it since it was read.
    fn drop_entry(&mut self) -> Turn<Option<u64>> {
        self.step = CheckStep::Drop;
        child(settle_call(self.link_id, &self.queued, None))
    }

    fn retry_after(&self, wait_ms: u64) -> Turn<Option<u64>> {
        Ok(Next::Done(Some(self.now_ms.saturating_add(wait_ms))))
    }

    fn after_holder(&mut self) -> Turn<Option<u64>> {
        let link = self.link()?;
        // A paused link still settles its push, for example one cancelled while queued.
        if let Some(job_id) = link.active_job.filter(|_| link.pull().is_none()) {
            self.step = CheckStep::Job(job_id);
            return child(IoCall::child(ReadJobOperation::new(job_id)));
        }
        if link.status != LinkStatus::Enabled && !link.retries_on_change() {
            return self.drop_entry();
        }
        if link.pull().is_some() {
            let due = CheckDueOperation::new(self.scope.clone(), link.clone());
            self.step = CheckStep::Pull;
            return child(IoCall::child(due));
        }
        // A decided review stores the repository's answer, which queues another check.
        let review = RefreshReviewOperation::new(self.scope.clone(), link.clone());
        self.step = CheckStep::Review;
        child(IoCall::child(review))
    }

    fn decide_push(&mut self, event_id: Ulid, digest: Option<[u8; 32]>) -> Turn<Option<u64>> {
        let link = self.link()?;
        let publish = match link.publish_due_ms() {
            _ if link.changed(event_id, digest) => false,
            Some(due) if due <= self.now_ms => true,
            Some(due) => return Ok(Next::Done(Some(due))),
            None if link.remote.review == LinkReview::Pending => {
                return self.retry_after(REVIEW_POLL_MS);
            }
            None => return self.drop_entry(),
        };
        let push = StartPushOperation::new(self.scope.clone(), link.clone(), event_id, publish);
        self.step = CheckStep::Push;
        child(IoCall::child(push))
    }
}

impl Operation for CheckLinkOperation {
    type Output = Option<u64>;
    type Error = LinkError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Turn<Option<u64>> {
        let row = link_row(self.entry.document_id, self.link_id);
        Ok(Next::Call(IoCall::Read(vec![row])))
    }

    fn next(&mut self, result: IoResult) -> Turn<Option<u64>> {
        match self.step {
            CheckStep::Link => {
                let [row] = aruna_core::rows::values::<1>(result)?;
                let local = self.scope.node.map(|(node, _)| node);
                match parse_link(row)? {
                    Some(link) if local.is_none_or(|node| node == link.owner_node) => {
                        self.link = Some(link);
                        self.step = CheckStep::Registry;
                        Ok(Next::Call(registry_read(self.entry.document_id)))
                    }
                    _ => self.drop_entry(),
                }
            }
            // A deleted dataset takes its links and their sealed tokens along; remote records
            // stay.
            CheckStep::Registry => {
                let link = self.link()?.clone();
                if registry_row(result)?.is_none() {
                    self.step = CheckStep::Delete;
                    let delete =
                        ApplyChangeOperation::new(self.scope.clone(), link, LinkChange::Delete);
                    return child(IoCall::child(delete));
                }
                self.step = CheckStep::Holder;
                child(IoCall::child(EnsureHolderOperation::new(
                    self.scope.clone(),
                    link,
                )))
            }
            CheckStep::Delete => {
                result.child::<Option<RepositoryLink>, LinkError>()??;
                Ok(Next::Done(None))
            }
            CheckStep::Holder => {
                if let Err(LinkError::NotHolder) = result.child::<(), LinkError>()? {
                    return self.drop_entry();
                }
                self.after_holder()
            }
            CheckStep::Job(job_id) => {
                let record = result
                    .child::<Option<JobRecord>, aruna_core::calls::IoError>()?
                    .map_err(|error| LinkError::Unexpected(error.to_string()))?;
                if record.as_ref().is_some_and(|record| !record.state.is_terminal()) {
                    return self.retry_after(ACTIVE_RETRY_MS);
                }
                let stale = SettleStaleOperation::new(
                    self.scope.clone(),
                    self.link()?.clone(),
                    job_id,
                    record,
                    self.queued.clone(),
                );
                self.step = CheckStep::Stale;
                child(IoCall::child(stale))
            }
            CheckStep::Stale | CheckStep::Pull => {
                let retry_at = result.child::<Option<u64>, LinkError>()??;
                Ok(Next::Done(retry_at))
            }
            CheckStep::Review => {
                let stored = result.child::<RepositoryLink, LinkError>()??;
                if stored != *self.link()? {
                    return Ok(Next::Done(None));
                }
                self.step = CheckStep::Revision;
                child(IoCall::child(PushRevisionOperation::new(
                    self.entry.document_id,
                )))
            }
            CheckStep::Revision => match result.child::<Option<Revision>, LinkError>()?? {
                Some((event_id, digest)) => self.decide_push(event_id, digest),
                None => self.drop_entry(),
            },
            CheckStep::Push => match result.child::<JobId, LinkError>()? {
                Ok(_) => Ok(Next::Done(None)),
                // The check stays queued, so the link shows pending until a job slot frees up.
                Err(LinkError::JobLimit(_)) => self.retry_after(ACTIVE_RETRY_MS),
                Err(error) => Err(error),
            },
            CheckStep::Drop => {
                settled(result)?;
                Ok(Next::Done(None))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum StaleStep {
    Checkpoint,
    Change,
    Drop,
}

/// A push job ended without recording its outcome, for example cancelled while queued.
#[derive(Debug, PartialEq)]
struct SettleStaleOperation {
    scope: NodeScope,
    link: RepositoryLink,
    job_id: JobId,
    record: Option<JobRecord>,
    queued: Value,
    now_ms: u64,
    step: StaleStep,
}

impl SettleStaleOperation {
    fn new(
        scope: NodeScope,
        link: RepositoryLink,
        job_id: JobId,
        record: Option<JobRecord>,
        queued: Value,
    ) -> Self {
        Self {
            scope,
            link,
            job_id,
            record,
            queued,
            now_ms: 0,
            step: StaleStep::Checkpoint,
        }
    }

    fn outcome(&self, result: IoResult) -> Result<PushOutcome, LinkError> {
        // A push that finished remotely but failed to record it still counts as pushed.
        let checkpoint = parse_checkpoint(result).map_err(LinkError::Unexpected)?;
        let pushed = checkpoint
            .as_ref()
            .and_then(|checkpoint| checkpoint.pushed_outcome());
        Ok(match (pushed, &self.record) {
            (Some(pushed), _) => pushed,
            (None, Some(record)) if record.state == JobState::Failed => {
                let message = record
                    .last_error
                    .as_ref()
                    .map_or("push job failed", |error| error.message.as_str());
                PushOutcome::Failed(match &checkpoint {
                    Some(checkpoint) => checkpoint.push_failure(message),
                    None => LinkFailure::Other(message.to_string()),
                })
            }
            (None, Some(_)) => PushOutcome::Cancelled,
            (None, None) => PushOutcome::Failed(LinkFailure::Other("push job missing".into())),
        })
    }
}

impl Operation for SettleStaleOperation {
    type Output = Option<u64>;
    type Error = LinkError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Turn<Option<u64>> {
        Ok(Next::Call(read_checkpoint(self.job_id)))
    }

    fn next(&mut self, result: IoResult) -> Turn<Option<u64>> {
        match self.step {
            StaleStep::Checkpoint => {
                let change = LinkChange::Finish {
                    job_id: self.job_id,
                    outcome: Box::new(self.outcome(result)?),
                    requeue: false,
                };
                self.step = StaleStep::Change;
                child(IoCall::child(ApplyChangeOperation::new(
                    self.scope.clone(),
                    self.link.clone(),
                    change,
                )))
            }
            StaleStep::Change => {
                result.child::<Option<RepositoryLink>, LinkError>()??;
                // A cancelled push skips its change; the next change queues a new check.
                let cancelled = self
                    .record
                    .as_ref()
                    .is_some_and(|record| record.state == JobState::Cancelled);
                if cancelled {
                    self.step = StaleStep::Drop;
                    return child(settle_call(self.link.link_id, &self.queued, None));
                }
                Ok(Next::Done(Some(self.now_ms)))
            }
            StaleStep::Drop => {
                settled(result)?;
                Ok(Next::Done(None))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum ReviewStep {
    Remote,
    Change,
}

/// Asks the repository about a pending review and stores a decided one; answers the link as
/// stored afterwards. A refused request fails the link.
#[derive(Debug, PartialEq)]
pub struct RefreshReviewOperation {
    scope: NodeScope,
    link: RepositoryLink,
    step: ReviewStep,
}

impl RefreshReviewOperation {
    pub fn new(scope: NodeScope, link: RepositoryLink) -> Self {
        Self {
            scope,
            link,
            step: ReviewStep::Remote,
        }
    }
}

impl Operation for RefreshReviewOperation {
    type Output = RepositoryLink;
    type Error = LinkError;

    fn first(&mut self) -> Turn<RepositoryLink> {
        if self.link.remote.review != LinkReview::Pending {
            return Ok(Next::Done(self.link.clone()));
        }
        child(IoCall::child(ReviewStateOperation::new(
            self.scope.clone(),
            self.link.clone(),
        )))
    }

    fn next(&mut self, result: IoResult) -> Turn<RepositoryLink> {
        match self.step {
            ReviewStep::Remote => {
                let change = match result.child::<Option<RemoteState>, TransferError>()? {
                    Ok(Some(state)) => LinkChange::Accept(Box::new(state)),
                    Ok(None) => return Ok(Next::Done(self.link.clone())),
                    Err(TransferError::Refused(reason)) => LinkChange::Fail(reason),
                    Err(error) => return Err(LinkError::Unexpected(error.to_string())),
                };
                self.step = ReviewStep::Change;
                child(IoCall::child(ApplyChangeOperation::new(
                    self.scope.clone(),
                    self.link.clone(),
                    change,
                )))
            }
            ReviewStep::Change => {
                let stored = result
                    .child::<Option<RepositoryLink>, LinkError>()??
                    .ok_or(LinkError::NotFound)?;
                Ok(Next::Done(stored))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum PushStep {
    Holder,
    Lineage,
    Submit,
    Begin(JobId),
}

/// Submits the push job as the link's creator and records it as the running push.
#[derive(Debug, PartialEq)]
pub struct StartPushOperation {
    scope: NodeScope,
    link: RepositoryLink,
    event_id: Ulid,
    publish: bool,
    step: PushStep,
}

impl StartPushOperation {
    pub fn new(scope: NodeScope, link: RepositoryLink, event_id: Ulid, publish: bool) -> Self {
        Self {
            scope,
            link,
            event_id,
            publish,
            step: PushStep::Holder,
        }
    }

    fn submit(&mut self) -> Turn<JobId> {
        let spec = ExportRoCrateSpec {
            selection: None,
            destination: Some(self.link.destination(self.publish)),
            auth_context: creator_auth(&self.scope, &self.link),
            document_id: self.link.document_id,
            limits: self.link.limits.clone(),
        };
        self.step = PushStep::Submit;
        child(IoCall::child(SubmitLinkJobOperation::new(
            self.scope.clone(),
            self.link.owner_node,
            LinkJob::Export(spec),
            self.link.push_key(self.event_id, self.publish),
        )))
    }
}

/// A job start refused by the creator's job limit stays apart from other failures.
pub(super) fn submit_failure(error: SubmitJobError) -> LinkError {
    match error {
        SubmitJobError::ActiveJobLimit { limit } => LinkError::JobLimit(limit),
        error => LinkError::Submit(error.to_string()),
    }
}

impl Operation for StartPushOperation {
    type Output = JobId;
    type Error = LinkError;

    fn first(&mut self) -> Turn<JobId> {
        child(IoCall::child(EnsureHolderOperation::new(
            self.scope.clone(),
            self.link.clone(),
        )))
    }

    fn next(&mut self, result: IoResult) -> Turn<JobId> {
        match self.step {
            PushStep::Holder => {
                result.child::<(), LinkError>()??;
                // A failed link runs again only if no enabled link follows its lineage the
                // other way.
                if matches!(self.link.status, LinkStatus::Failed { .. }) {
                    self.step = PushStep::Lineage;
                    return child(IoCall::child(EnsureLineageOperation::new(
                        self.link.clone(),
                    )));
                }
                self.submit()
            }
            PushStep::Lineage => {
                result.child::<(), LinkError>()??;
                self.submit()
            }
            PushStep::Submit => {
                let submitted = result
                    .child::<SubmitJobResult, SubmitJobError>()?
                    .map_err(submit_failure)?;
                self.step = PushStep::Begin(submitted.job_id);
                child(IoCall::child(ApplyChangeOperation::new(
                    self.scope.clone(),
                    self.link.clone(),
                    LinkChange::Begin(submitted.job_id),
                )))
            }
            PushStep::Begin(job_id) => {
                result.child::<Option<RepositoryLink>, LinkError>()??;
                Ok(Next::Done(job_id))
            }
        }
    }
}

/// Whether the link's owner still holds the dataset; an unknown placement counts as held.
#[derive(Debug, PartialEq)]
pub struct OwnerHoldsOperation {
    scope: NodeScope,
    realm_id: aruna_core::structs::identity::realm::RealmId,
    document_id: Ulid,
    owner_node: aruna_core::NodeId,
}

impl OwnerHoldsOperation {
    pub fn new(scope: NodeScope, link: &RepositoryLink) -> Self {
        Self {
            realm_id: scope.realm_or(link.created_by.realm_id),
            scope,
            document_id: link.document_id,
            owner_node: link.owner_node,
        }
    }
}

impl Operation for OwnerHoldsOperation {
    type Output = bool;
    type Error = LinkError;

    fn first(&mut self) -> Turn<bool> {
        child(IoCall::child(GetConfigOperation::new(self.realm_id)))
    }

    fn next(&mut self, result: IoResult) -> Turn<bool> {
        let config = match result.child::<RealmConfigDocument, GetConfigError>()? {
            Ok(config) => config,
            Err(error) => {
                warn!(%error, "realm config unavailable; querying the local replica only");
                return Ok(Next::Done(true));
            }
        };
        let holds = resolve_metadata_id(&config, self.realm_id, None, self.document_id)
            .map_or(true, |placement| {
                holds_placement(&config, &placement, self.owner_node)
            });
        Ok(Next::Done(holds))
    }
}

#[derive(Debug, PartialEq)]
enum HolderStep {
    Holds,
    Fail,
}

/// Fails the link with owner_not_holder once this node lost the dataset. The change stays local:
/// a node outside the holder set cannot publish to them, and holders derive the same state.
#[derive(Debug, PartialEq)]
pub(crate) struct EnsureHolderOperation {
    scope: NodeScope,
    link: RepositoryLink,
    now_ms: u64,
    step: HolderStep,
}

impl EnsureHolderOperation {
    pub(crate) fn new(scope: NodeScope, link: RepositoryLink) -> Self {
        Self {
            scope,
            link,
            now_ms: 0,
            step: HolderStep::Holds,
        }
    }
}

impl Operation for EnsureHolderOperation {
    type Output = ();
    type Error = LinkError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Turn<()> {
        child(IoCall::child(OwnerHoldsOperation::new(
            self.scope.clone(),
            &self.link,
        )))
    }

    fn next(&mut self, result: IoResult) -> Turn<()> {
        match self.step {
            HolderStep::Holds => {
                if result.child::<bool, LinkError>()?? {
                    return Ok(Next::Done(()));
                }
                self.step = HolderStep::Fail;
                child(IoCall::child(ChangeLinkOperation::new(
                    self.link.document_id,
                    self.link.link_id,
                    LinkChange::Fail(LinkFailure::OwnerNotHolder),
                    time_of(self.now_ms),
                )))
            }
            HolderStep::Fail => {
                result.child::<Option<RepositoryLink>, LinkError>()??;
                Err(LinkError::NotHolder)
            }
        }
    }
}

/// The revision event a push exports, with the dataset digest when one is known.
pub type Revision = (Ulid, Option<[u8; 32]>);

/// The event of a current revision, for push keys and change checks.
pub fn revision_event(revision: Option<Revision>) -> Result<Ulid, LinkError> {
    revision
        .map(|(event_id, _)| event_id)
        .ok_or(LinkError::NoRevision)
}

#[derive(Debug, PartialEq)]
enum RevisionStep {
    Raw,
    Registry,
}

/// The revision a push exports: the raw revision, else a scaffold's rendered graph at its last
/// event, the same crate the export job and the dataset view use.
#[derive(Debug, PartialEq)]
pub struct PushRevisionOperation {
    document_id: Ulid,
    step: RevisionStep,
}

impl PushRevisionOperation {
    pub fn new(document_id: Ulid) -> Self {
        Self {
            document_id,
            step: RevisionStep::Raw,
        }
    }
}

impl Operation for PushRevisionOperation {
    type Output = Option<Revision>;
    type Error = LinkError;

    fn first(&mut self) -> Turn<Option<Revision>> {
        child(IoCall::child(RawViewOperation::new(
            self.document_id,
            None,
            false,
        )))
    }

    fn next(&mut self, result: IoResult) -> Turn<Option<Revision>> {
        match self.step {
            RevisionStep::Raw => {
                let (_, raw) = result
                    .child::<RawViewOutput, RawReadError>()?
                    .map_err(|error| LinkError::Unexpected(error.to_string()))?;
                if let Some(revision) = raw {
                    return Ok(Next::Done(Some((
                        revision.winning_event_id,
                        revision.dataset_digest,
                    ))));
                }
                self.step = RevisionStep::Registry;
                Ok(Next::Call(registry_read(self.document_id)))
            }
            RevisionStep::Registry => {
                let record = registry_row(result)?;
                Ok(Next::Done(record.map(|record| (record.last_event_id, None))))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum DeletedStep {
    Rows,
    Write,
    Timer,
}

/// Queues checks for the links of the deleted document behind `graph_iri`; each check then
/// removes its link and token.
#[derive(Debug, PartialEq)]
pub(crate) struct QueueDeletedOperation {
    scope: NodeScope,
    graph_iri: String,
    step: DeletedStep,
}

impl QueueDeletedOperation {
    pub(crate) fn new(scope: NodeScope, graph_iri: String) -> Self {
        Self {
            scope,
            graph_iri,
            step: DeletedStep::Rows,
        }
    }
}

impl Operation for QueueDeletedOperation {
    type Output = ();
    type Error = LinkError;

    fn first(&mut self) -> Turn<()> {
        let document_id = self
            .graph_iri
            .rsplit('/')
            .next()
            .and_then(|id| Ulid::from_string(id).ok())
            .filter(|id| MetadataRegistryRecord::graph_iri_for(*id) == self.graph_iri);
        match document_id {
            Some(document_id) => child(IoCall::child(QueueRowsOperation::new([document_id]))),
            None => Ok(Next::Done(())),
        }
    }

    fn next(&mut self, result: IoResult) -> Turn<()> {
        match self.step {
            DeletedStep::Rows => {
                let rows = result.child::<Vec<(String, Key, Value)>, LinkError>()??;
                if rows.is_empty() {
                    return Ok(Next::Done(()));
                }
                self.step = DeletedStep::Write;
                Ok(Next::Call(IoCall::Write(rows)))
            }
            DeletedStep::Write => {
                done(result)?;
                if !self.scope.has_tasks {
                    return Ok(Next::Done(()));
                }
                self.step = DeletedStep::Timer;
                child(IoCall::child(LinkTimerOperation::new()))
            }
            DeletedStep::Timer => {
                if let Err(error) = result.child::<(), LinkError>()? {
                    warn!(%error, "Failed to arm the repository link queue");
                }
                Ok(Next::Done(()))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum TimerStep {
    Page,
    Arm,
}

/// Arms the drain for the earliest queued check; the queue itself is the durable state.
#[derive(Debug, PartialEq)]
pub struct LinkTimerOperation {
    pages: PrefixPages,
    next_due: Option<u64>,
    now_ms: u64,
    step: TimerStep,
}

impl LinkTimerOperation {
    pub fn new() -> Self {
        Self {
            pages: queue_pages(),
            next_due: None,
            now_ms: 0,
            step: TimerStep::Page,
        }
    }
}

impl Default for LinkTimerOperation {
    fn default() -> Self {
        Self::new()
    }
}

impl Operation for LinkTimerOperation {
    type Output = ();
    type Error = LinkError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Turn<()> {
        Ok(Next::Call(self.pages.call()))
    }

    fn next(&mut self, result: IoResult) -> Turn<()> {
        match self.step {
            TimerStep::Page => {
                for (_, value) in self.pages.take(result)? {
                    let due = postcard::from_bytes::<LinkQueueEntry>(&value)
                        .map_or(self.now_ms, |entry| entry.due_at_ms);
                    self.next_due = min_due_at(self.next_due, due);
                }
                if !self.pages.done() {
                    return Ok(Next::Call(self.pages.call()));
                }
                let Some(due) = self.next_due else {
                    return Ok(Next::Done(()));
                };
                self.step = TimerStep::Arm;
                Ok(Next::Call(schedule_drain(due_after(self.now_ms, due)).into()))
            }
            TimerStep::Arm => {
                if let Event::Task(TaskEvent::Error { message, .. }) = event(result)? {
                    warn!(%message, "Failed to arm the repository link queue");
                }
                Ok(Next::Done(()))
            }
        }
    }
}

#[cfg(test)]
#[path = "link_queue_tests.rs"]
mod tests;
