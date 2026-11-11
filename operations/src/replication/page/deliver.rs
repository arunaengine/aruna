//! Sends due member jobs of reference records as pages and retires each acknowledged page's
//! jobs in one storage batch; other jobs stay for single delivery.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::future::Future;

use aruna_core::NodeId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::REPLICATION_JOB_KEYSPACE;
use aruna_core::structs::storage::distribution::{
    DistributionBinding, DistributionSnapshot, Publication,
};
use aruna_core::time::unix_timestamp_millis;
use tracing::{info, warn};

use super::apply::{ApplyInput, PageApplyOperation, page_answer};
use super::build::{BuildInput, Built, PageBuildOperation};
use super::{PAGE_RECORDS, PageAnswer, ReferencePage};
use crate::driver::{DriverContext, drive};
use crate::replication::members::{MemberTarget, bound_record, member_publication};
use crate::replication::queue::{BlobJobRecord, BlobQueueError};
use crate::replication::version_replication::ReplicateScopeTarget;
use crate::tasks::queue_backoff::{min_due_at, retry_delay_ms};

/// The error a job carries while single delivery owes it instead of a page.
pub(crate) const PAGE_SINGLE: &str = "left to single delivery by a reference page";
/// Failed attempts that each halve the pages a job travels in.
const PAGE_SHRINKS: u32 = 4;

/// Sends one page to a member and returns its answer; an error leaves the outcome unknown.
pub trait PageSender {
    fn send(
        &self,
        node: NodeId,
        page: ReferencePage,
    ) -> impl Future<Output = Result<PageAnswer, String>> + Send;
}

/// Sends pages over the metadata transport, never over a blob stream.
pub struct MetadataSender<'a>(pub &'a DriverContext);

impl PageSender for MetadataSender<'_> {
    async fn send(&self, node: NodeId, page: ReferencePage) -> Result<PageAnswer, String> {
        let metadata = (self.0.metadata_handle.as_ref()).ok_or("no transport to the member")?;
        metadata.request_reference_page(node, page).await
    }
}

/// What one drain's pages did with their jobs.
#[derive(Debug, Default)]
pub(crate) struct PageReport {
    pub delivered: usize,
    pub failed: usize,
    /// Jobs held back for a later page.
    pub later: usize,
    /// Jobs single delivery sends, in the order they came.
    pub single: Vec<(Vec<u8>, BlobJobRecord)>,
    pub next_due_ms: Option<u64>,
}

impl PageReport {
    fn note_due(&mut self, due: Option<u64>) {
        if let Some(due) = due {
            self.next_due_ms = min_due_at(self.next_due_ms, due);
        }
    }
}

type Job = (Vec<u8>, BlobJobRecord);

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

/// Delivers `jobs` in pages per bucket, member, publication and page size.
pub(crate) async fn deliver_pages(
    context: &DriverContext,
    jobs: Vec<Job>,
    sender: &impl PageSender,
) -> Result<PageReport, BlobQueueError> {
    let mut report = PageReport::default();
    let mut records = HashMap::<String, Option<(DistributionBinding, DistributionSnapshot)>>::new();
    let mut pages: Vec<(Publication, Vec<Job>)> = Vec::new();
    let mut retire = Vec::new();
    for (key, job) in jobs {
        if !records.contains_key(&job.input.bucket) {
            let record = bound_record(context, &job.input.bucket).await;
            let record = record.map_err(BlobQueueError::UnexpectedEvent)?;
            records.insert(job.input.bucket.clone(), record);
        }
        let publication = publication(records.get(&job.input.bucket), &job);
        let Some(publication) = publication else {
            // This node no longer owes it: the current coordinator does.
            retire.push(key);
            continue;
        };
        let limit = page_limit(&job);
        let target = (&job.input.bucket, job.input.target_node_id);
        let page = (pages.iter_mut()).find(|(other, jobs)| {
            let first = &jobs[0].1;
            *other == publication
                && jobs.len() < limit
                && page_limit(first) == limit
                && (&first.input.bucket, first.input.target_node_id) == target
        });
        match page {
            Some((_, jobs)) => jobs.push((key, job)),
            None => pages.push((publication, vec![(key, job)])),
        }
    }
    report.delivered += retire.len();
    delete_jobs(context, retire).await?;
    for (publication, jobs) in pages {
        deliver_page(context, publication, jobs, sender, &mut report).await?;
    }
    Ok(report)
}

fn publication(
    record: Option<&Option<(DistributionBinding, DistributionSnapshot)>>,
    job: &BlobJobRecord,
) -> Option<Publication> {
    let (binding, snapshot) = record?.as_ref()?;
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

async fn deliver_page(
    context: &DriverContext,
    publication: Publication,
    jobs: Vec<Job>,
    sender: &impl PageSender,
    report: &mut PageReport,
) -> Result<(), BlobQueueError> {
    let (bucket, node) = (
        jobs[0].1.input.bucket.clone(),
        jobs[0].1.input.target_node_id,
    );
    let input = BuildInput {
        bucket: bucket.clone(),
        publication: publication.clone(),
        jobs: jobs.iter().map(|(_, job)| job.clone()).collect(),
    };
    let built = match drive(PageBuildOperation::new(input), context).await {
        Ok(built) => built,
        Err(error) => {
            report.failed += jobs.len();
            let due = reschedule(context, jobs, Some(error.to_string())).await?;
            report.note_due(due);
            return Ok(());
        }
    };
    let (mut retire, mut paged, mut manifests) = (Vec::new(), Vec::new(), Vec::new());
    for ((key, job), built) in jobs.into_iter().zip(built) {
        match built {
            Built::Record(manifest) => {
                manifests.push(*manifest);
                paged.push((key, job));
            }
            Built::Gone => retire.push(key),
            Built::Single => report.single.push((key, job)),
            // Still due; a later drain pages it.
            Built::Later => report.later += 1,
        }
    }
    report.delivered += retire.len();
    if manifests.is_empty() {
        return delete_jobs(context, retire).await;
    }
    let count = manifests.len();
    let page = ReferencePage {
        bucket: bucket.clone(),
        publication,
        manifests,
    };
    let answer = sender.send(node, page).await;
    let (single, retry) = match answer {
        Ok(PageAnswer::Applied { applied, single }) => {
            info!(%bucket, %node, records = count, applied, single = single.len(), "Delivered a reference page");
            (single, None)
        }
        Ok(PageAnswer::Refused {
            reason,
            retry: false,
        }) => {
            warn!(%bucket, %node, %reason, "A member refused a reference page; sending its records singly");
            (
                paged
                    .iter()
                    .filter_map(|(_, job)| version_of(job))
                    .collect(),
                None,
            )
        }
        Ok(PageAnswer::Refused {
            reason,
            retry: true,
        }) => (Vec::new(), Some(reason)),
        Err(error) => (Vec::new(), Some(error)),
    };
    if let Some(reason) = retry {
        warn!(%bucket, %node, %reason, "A reference page was not acknowledged; retrying later");
        report.failed += paged.len();
        let due = reschedule(context, paged, Some(reason)).await?;
        report.note_due(due);
        return delete_jobs(context, retire).await;
    }
    let (left, taken): (Vec<_>, Vec<_>) = (paged.into_iter())
        .partition(|(_, job)| version_of(job).is_some_and(|id| single.contains(&id)));
    report.delivered += taken.len();
    retire.extend(taken.into_iter().map(|(key, _)| key));
    // The acknowledged page's jobs go in one batch, never one delete per record.
    delete_jobs(context, retire).await?;
    reschedule(context, left, None).await?;
    Ok(())
}

fn version_of(job: &BlobJobRecord) -> Option<ulid::Ulid> {
    match job.input.target {
        ReplicateScopeTarget::Version { version_id, .. } => Some(version_id),
        _ => None,
    }
}

async fn delete_jobs(context: &DriverContext, keys: Vec<Vec<u8>>) -> Result<(), BlobQueueError> {
    if keys.is_empty() {
        return Ok(());
    }
    let deletes = (keys.into_iter())
        .map(|key| (REPLICATION_JOB_KEYSPACE.to_string(), key.into()))
        .collect();
    let delete = StorageEffect::BatchDelete {
        deletes,
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(delete).await {
        Event::Storage(StorageEvent::BatchDeleteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(BlobQueueError::UnexpectedEvent(format!("{other:?}"))),
    }
}

/// Writes the jobs back in one batch: with an error after a backoff, without one due now for
/// single delivery. Returns the earliest due time.
async fn reschedule(
    context: &DriverContext,
    jobs: Vec<Job>,
    error: Option<String>,
) -> Result<Option<u64>, BlobQueueError> {
    if jobs.is_empty() {
        return Ok(None);
    }
    let now_ms = unix_timestamp_millis();
    let mut due = None;
    let mut writes = Vec::with_capacity(jobs.len());
    for (key, mut job) in jobs {
        match &error {
            Some(error) => {
                job.attempts = job.attempts.saturating_add(1);
                job.due_at_ms = now_ms.saturating_add(retry_delay_ms(job.attempts));
                job.last_error = Some(error.clone());
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
    let write = StorageEffect::BatchWrite {
        writes,
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(write).await {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => Ok(due),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(BlobQueueError::UnexpectedEvent(format!("{other:?}"))),
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
