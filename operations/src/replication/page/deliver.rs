//! Sends due member jobs of reference records as pages and retires each acknowledged page's
//! jobs in one storage batch; other jobs stay for single delivery.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::{Effect, MetadataRequestEffect, NetEffect};
use aruna_core::events::{Event, MetadataAnswer, NetEvent};
use aruna_core::keyspaces::REPLICATION_JOB_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::distribution::Publication;
use tracing::{info, warn};

use super::apply::{ApplyInput, PageApplyOperation, PageError, page_answer};
use super::build::{BuildError, BuildInput, Built, PageBuildOperation};
use super::{PAGE_RECORDS, PageAnswer, ReferencePage};
use crate::metadata::protocol::{MetadataTransportMessage, decode_message, encode_message};
use crate::replication::members::{
    BoundRecord, BoundRecordOperation, RecordError, member_publication,
};
use crate::replication::queue::BlobQueueError;
use crate::tasks::queue_backoff::{min_due_at, retry_delay_ms};
use aruna_core::structs::storage::replication_queue::BlobJobRecord;
use aruna_core::structs::storage::replication_queue::MemberTarget;
use aruna_core::structs::storage::replication_queue::ReplicateScopeTarget;

/// The error a job carries while single delivery owes it instead of a page.
const PAGE_SINGLE: &str = "left to single delivery by a reference page";
/// Failed attempts that each halve the pages a job travels in.
const PAGE_SHRINKS: u32 = 4;
/// Upper bound of one page request; a member answers only after its one commit.
const PAGE_DEADLINE: Duration = Duration::from_secs(150);

type Turn<T> = Result<Next<T>, BlobQueueError>;

/// How a page reaches a member: the call that sends it and the reading of its answer. An error
/// leaves the outcome unknown.
pub trait PageTransport: Clone + std::fmt::Debug + PartialEq + Send + 'static {
    fn request(&self, node: NodeId, page: ReferencePage) -> Result<IoCall, String>;
    fn answer(&self, result: IoResult) -> Result<PageAnswer, String>;
}

/// Sends pages over the metadata transport, never over a blob stream.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct MetadataPages;

impl PageTransport for MetadataPages {
    fn request(&self, node: NodeId, page: ReferencePage) -> Result<IoCall, String> {
        let message = MetadataTransportMessage::ReferencePage {
            page: Box::new(page),
        };
        let frame = encode_message(&message).map_err(|error| error.to_string())?;
        let request = MetadataRequestEffect {
            peer: node,
            frame,
            timeout: PAGE_DEADLINE,
        };
        let effect = Effect::Net(NetEffect::MetadataRequest(Box::new(request)));
        Ok(effect.into())
    }

    fn answer(&self, result: IoResult) -> Result<PageAnswer, String> {
        let event = result.event().map_err(|error| error.to_string())?;
        let Event::Net(NetEvent::MetadataAnswer(answer)) = event else {
            return Err(format!("unexpected page answer: {}", event.label()));
        };
        let MetadataAnswer { result, .. } = *answer;
        let bytes = result.map_err(|error| format!("{error:?}"))?;
        match decode_message(&bytes).map_err(|error| error.to_string())? {
            MetadataTransportMessage::ReferencePageAnswer { answer } => Ok(answer),
            _ => Err("the member answered something else".to_string()),
        }
    }
}

type Job = (Vec<u8>, BlobJobRecord);

/// What one drain's pages did with their jobs.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct PageReport {
    pub delivered: usize,
    pub failed: usize,
    /// Jobs held back for a later page.
    pub later: usize,
    /// Jobs single delivery sends, in the order they came.
    pub single: Vec<Job>,
    pub next_due_ms: Option<u64>,
}

impl PageReport {
    fn note_due(&mut self, due: Option<u64>) {
        if let Some(due) = due {
            self.next_due_ms = min_due_at(self.next_due_ms, due);
        }
    }

    fn absorb(&mut self, other: PageReport) {
        self.delivered += other.delivered;
        self.failed += other.failed;
        self.later += other.later;
        self.single.extend(other.single);
        self.note_due(other.next_due_ms);
    }
}

/// Whether a page may carry the job: a member publication of one version, not held back for
/// single delivery.
pub(crate) fn pageable(job: &BlobJobRecord) -> bool {
    job.distribution.is_some()
        && job.relationship_id.is_none()
        && job.reference_advance.is_none()
        && matches!(job.input.target, ReplicateScopeTarget::Version { .. })
        && job.last_error.as_deref() != Some(PAGE_SINGLE)
}

/// Records a page of `job` carries at most: each failed attempt halves it, so a page too slow
/// for its member returns smaller instead of failing the same way again.
fn page_limit(job: &BlobJobRecord) -> usize {
    PAGE_RECORDS >> job.attempts.min(PAGE_SHRINKS)
}

fn publication(record: Option<&BoundRecord>, job: &BlobJobRecord) -> Option<Publication> {
    let (binding, snapshot) = record?;
    let (ReplicateScopeTarget::Version { key, .. }, Some(target)) =
        (&job.input.target, job.distribution.as_ref())
    else {
        return None;
    };
    let target: &MemberTarget = target;
    member_publication(binding, snapshot, key, target)
        .filter(|(_, node)| *node == job.input.target_node_id)
        .map(|(publication, _)| publication)
}

fn version_of(job: &BlobJobRecord) -> Option<ulid::Ulid> {
    match job.input.target {
        ReplicateScopeTarget::Version { version_id, .. } => Some(version_id),
        _ => None,
    }
}

/// The batch delete of finished jobs; the jobs of a page go together, never one by one.
fn delete_call(keys: Vec<Vec<u8>>) -> Option<IoCall> {
    let deletes: Vec<_> = (keys.into_iter())
        .map(|key| (REPLICATION_JOB_KEYSPACE.to_string(), key.into()))
        .collect();
    (!deletes.is_empty()).then_some(IoCall::Delete(deletes))
}

/// Writes the jobs back in one batch: with an error after a backoff, without one due now for
/// single delivery. Also gives the earliest due time.
fn reschedule_call(
    jobs: Vec<Job>,
    error: Option<&str>,
    now_ms: u64,
) -> Result<(Option<IoCall>, Option<u64>), BlobQueueError> {
    let mut due = None;
    let mut writes = Vec::with_capacity(jobs.len());
    for (key, mut job) in jobs {
        match error {
            Some(error) => {
                job.attempts = job.attempts.saturating_add(1);
                job.due_at_ms = now_ms.saturating_add(retry_delay_ms(job.attempts));
                job.last_error = Some(error.to_string());
            }
            None => {
                job.due_at_ms = now_ms;
                job.last_error = Some(PAGE_SINGLE.to_string());
            }
        }
        due = min_due_at(due, job.due_at_ms);
        writes.push((
            REPLICATION_JOB_KEYSPACE.to_string(),
            key.into(),
            job.to_bytes()?.into(),
        ));
    }
    Ok(((!writes.is_empty()).then_some(IoCall::Write(writes)), due))
}

#[derive(Debug, PartialEq)]
enum DeliverStep {
    Record(Box<Job>),
    Deleted,
    Page,
}

/// Delivers jobs in pages per bucket, member, publication and page size.
#[derive(Debug, PartialEq)]
pub(crate) struct DeliverPagesOperation<T> {
    transport: T,
    jobs: VecDeque<Job>,
    records: HashMap<String, Option<BoundRecord>>,
    pages: Vec<(Publication, Vec<Job>)>,
    retire: Vec<Vec<u8>>,
    report: PageReport,
    step: DeliverStep,
}

impl<T: PageTransport> DeliverPagesOperation<T> {
    pub(crate) fn new(transport: T, jobs: Vec<Job>) -> Self {
        Self {
            transport,
            jobs: jobs.into(),
            records: HashMap::new(),
            pages: Vec::new(),
            retire: Vec::new(),
            report: PageReport::default(),
            step: DeliverStep::Page,
        }
    }

    fn next_job(&mut self) -> Turn<PageReport> {
        while let Some((key, job)) = self.jobs.pop_front() {
            if !self.records.contains_key(&job.input.bucket) {
                let read = BoundRecordOperation::new(&job.input.bucket);
                self.step = DeliverStep::Record(Box::new((key, job)));
                return Ok(Next::Call(IoCall::child(read)));
            }
            self.place((key, job));
        }
        self.report.delivered += self.retire.len();
        self.step = DeliverStep::Deleted;
        match delete_call(std::mem::take(&mut self.retire)) {
            Some(call) => Ok(Next::Call(call)),
            None => self.next_page(),
        }
    }

    fn place(&mut self, (key, job): Job) {
        let record = self.records.get(&job.input.bucket).and_then(Option::as_ref);
        let Some(publication) = publication(record, &job) else {
            // This node no longer owes it: the current coordinator does.
            self.retire.push(key);
            return;
        };
        let limit = page_limit(&job);
        let target = (&job.input.bucket, job.input.target_node_id);
        let page = (self.pages.iter_mut()).find(|(other, jobs)| {
            let first = &jobs[0].1;
            *other == publication
                && jobs.len() < limit
                && page_limit(first) == limit
                && (&first.input.bucket, first.input.target_node_id) == target
        });
        match page {
            Some((_, jobs)) => jobs.push((key, job)),
            None => self.pages.push((publication, vec![(key, job)])),
        }
    }

    fn next_page(&mut self) -> Turn<PageReport> {
        if self.pages.is_empty() {
            return Ok(Next::Done(std::mem::take(&mut self.report)));
        }
        let (publication, jobs) = self.pages.remove(0);
        let page = DeliverPageOperation::new(self.transport.clone(), publication, jobs);
        self.step = DeliverStep::Page;
        Ok(Next::Call(IoCall::child(page)))
    }
}

impl<T: PageTransport> Operation for DeliverPagesOperation<T> {
    type Output = PageReport;
    type Error = BlobQueueError;

    fn first(&mut self) -> Turn<PageReport> {
        self.next_job()
    }

    fn next(&mut self, result: IoResult) -> Turn<PageReport> {
        match std::mem::replace(&mut self.step, DeliverStep::Page) {
            DeliverStep::Record(job) => {
                let record = (result.child::<Option<BoundRecord>, RecordError>()?)
                    .map_err(|error| BlobQueueError::UnexpectedEvent(error.to_string()))?;
                self.records.insert(job.1.input.bucket.clone(), record);
                self.place(*job);
                self.next_job()
            }
            DeliverStep::Deleted => {
                result.done()?;
                self.next_page()
            }
            DeliverStep::Page => {
                let page = result.child::<PageReport, BlobQueueError>()??;
                self.report.absorb(page);
                self.next_page()
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum PageStep {
    Build,
    Sent,
    Stored,
}

/// Builds and sends one page, then settles its jobs by the member's answer.
#[derive(Debug, PartialEq)]
struct DeliverPageOperation<T> {
    transport: T,
    publication: Option<Publication>,
    bucket: String,
    node: NodeId,
    jobs: Vec<Job>,
    paged: Vec<Job>,
    retire: Vec<Vec<u8>>,
    count: usize,
    calls: VecDeque<IoCall>,
    report: PageReport,
    now_ms: u64,
    step: PageStep,
}

impl<T: PageTransport> DeliverPageOperation<T> {
    fn new(transport: T, publication: Publication, jobs: Vec<Job>) -> Self {
        let first = &jobs[0].1.input;
        let (bucket, node) = (first.bucket.clone(), first.target_node_id);
        Self {
            transport,
            publication: Some(publication),
            bucket,
            node,
            jobs,
            paged: Vec::new(),
            retire: Vec::new(),
            count: 0,
            calls: VecDeque::new(),
            report: PageReport::default(),
            now_ms: 0,
            step: PageStep::Build,
        }
    }

    fn next_store(&mut self) -> Turn<PageReport> {
        self.step = PageStep::Stored;
        match self.calls.pop_front() {
            Some(call) => Ok(Next::Call(call)),
            None => Ok(Next::Done(std::mem::take(&mut self.report))),
        }
    }

    fn queue_delete(&mut self) {
        self.calls
            .extend(delete_call(std::mem::take(&mut self.retire)));
    }

    /// The page could not go out or was not acknowledged: its jobs retry after a backoff.
    fn retry(&mut self, jobs: Vec<Job>, reason: &str) -> Result<(), BlobQueueError> {
        let (write, due) = reschedule_call(jobs, Some(reason), self.now_ms)?;
        self.calls.extend(write);
        self.report.note_due(due);
        Ok(())
    }

    fn built(&mut self, built: Result<Vec<Built>, BuildError>) -> Turn<PageReport> {
        let jobs = std::mem::take(&mut self.jobs);
        let built = match built {
            Ok(built) => built,
            Err(error) => {
                self.report.failed += jobs.len();
                self.retry(jobs, &error.to_string())?;
                return self.next_store();
            }
        };
        let mut manifests = Vec::new();
        for ((key, job), built) in jobs.into_iter().zip(built) {
            match built {
                Built::Record(manifest) => {
                    manifests.push(*manifest);
                    self.paged.push((key, job));
                }
                Built::Gone => self.retire.push(key),
                Built::Single => self.report.single.push((key, job)),
                // Still due; a later drain pages it.
                Built::Later => self.report.later += 1,
            }
        }
        self.report.delivered += self.retire.len();
        let publication = self.publication.take();
        let (true, Some(publication)) = (!manifests.is_empty(), publication) else {
            self.queue_delete();
            return self.next_store();
        };
        self.count = manifests.len();
        let page = ReferencePage {
            bucket: self.bucket.clone(),
            publication,
            manifests,
        };
        match self.transport.request(self.node, page) {
            Ok(call) => {
                self.step = PageStep::Sent;
                Ok(Next::Call(call))
            }
            Err(error) => self.answered(Err(error)),
        }
    }

    fn answered(&mut self, answer: Result<PageAnswer, String>) -> Turn<PageReport> {
        let (bucket, node, count) = (&self.bucket, self.node, self.count);
        let (single, retry) = match answer {
            Ok(PageAnswer::Applied { applied, single }) => {
                let left = single.len();
                info!(%bucket, %node, records = count, applied, single = left, "Delivered a reference page");
                (single, None)
            }
            Ok(PageAnswer::Refused {
                reason,
                retry: false,
            }) => {
                warn!(%bucket, %node, %reason, "A member refused a reference page; sending its records singly");
                let all = self.paged.iter().filter_map(|(_, job)| version_of(job));
                (all.collect(), None)
            }
            Ok(PageAnswer::Refused {
                reason,
                retry: true,
            }) => (Vec::new(), Some(reason)),
            Err(error) => (Vec::new(), Some(error)),
        };
        let paged = std::mem::take(&mut self.paged);
        if let Some(reason) = retry {
            warn!(%bucket, %node, %reason, "A reference page was not acknowledged; retrying later");
            self.report.failed += paged.len();
            self.retry(paged, &reason)?;
            self.queue_delete();
            return self.next_store();
        }
        let (left, taken): (Vec<_>, Vec<_>) = (paged.into_iter())
            .partition(|(_, job)| version_of(job).is_some_and(|id| single.contains(&id)));
        self.report.delivered += taken.len();
        self.retire.extend(taken.into_iter().map(|(key, _)| key));
        // The acknowledged page's jobs go in one batch, never one delete per record.
        self.queue_delete();
        let (write, _) = reschedule_call(left, None, self.now_ms)?;
        self.calls.extend(write);
        self.next_store()
    }
}

impl<T: PageTransport> Operation for DeliverPageOperation<T> {
    type Output = PageReport;
    type Error = BlobQueueError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Turn<PageReport> {
        let publication = self.publication.clone();
        let publication =
            publication.ok_or(BlobQueueError::Io(aruna_core::calls::IoError::Unfinished))?;
        let input = BuildInput {
            bucket: self.bucket.clone(),
            publication,
            jobs: self.jobs.iter().map(|(_, job)| job.clone()).collect(),
        };
        Ok(Next::Call(IoCall::child(PageBuildOperation::new(input))))
    }

    fn next(&mut self, result: IoResult) -> Turn<PageReport> {
        match self.step {
            PageStep::Build => self.built(result.child()?),
            PageStep::Sent => {
                let answer = self.transport.answer(result);
                self.answered(answer)
            }
            PageStep::Stored => {
                result.done()?;
                self.next_store()
            }
        }
    }
}

/// The member's side of a page: applied under the authenticated sender.
#[derive(Debug, PartialEq)]
pub(crate) struct ServePageOperation {
    input: Option<ApplyInput>,
}

impl ServePageOperation {
    pub(crate) fn new(
        serving: Option<(NodeId, RealmId)>,
        peer: NodeId,
        page: ReferencePage,
    ) -> Self {
        let input = serving.map(|(local, realm_id)| ApplyInput {
            page,
            publisher: peer,
            realm_id,
            local,
        });
        Self { input }
    }
}

impl Operation for ServePageOperation {
    type Output = PageAnswer;
    type Error = BlobQueueError;

    fn first(&mut self) -> Turn<PageAnswer> {
        match self.input.take() {
            Some(input) => Ok(Next::Call(IoCall::child(PageApplyOperation::new(input)))),
            None => Ok(Next::Done(PageAnswer::Refused {
                reason: "the member's network is not ready".to_string(),
                retry: true,
            })),
        }
    }

    fn next(&mut self, result: IoResult) -> Turn<PageAnswer> {
        let applied = result.child::<PageAnswer, PageError>()?;
        Ok(Next::Done(page_answer(applied)))
    }
}

#[cfg(test)]
#[path = "deliver_tests.rs"]
mod tests;
