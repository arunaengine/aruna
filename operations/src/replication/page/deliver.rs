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
    }
}

/// The member's side of a page: applied under the authenticated sender.
pub(crate) async fn serve_page(
    context: &DriverContext,
    peer: NodeId,
    page: ReferencePage,
) -> PageAnswer {
    let Some(net) = context.net_handle.as_ref() else {
        let reason = "the member's network is not ready".to_string();
        return PageAnswer::Refused {
            reason,
            retry: true,
        };
    };
    let input = ApplyInput {
        page,
        publisher: peer,
        realm_id: *net.realm_id(),
        local: net.node_id(),
    };
    page_answer(drive(PageApplyOperation::new(input), context).await)
}

#[cfg(test)]
#[path = "deliver_tests.rs"]
mod tests;
