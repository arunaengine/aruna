//! Checks a link's repository lineage before a push and records the push on the link after it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
use aruna_core::events::Event;
use aruna_core::keyspaces::JOB_STATE_KEYSPACE;
use aruna_core::operation::{Operation, boxed_retry};
use aruna_core::repository::{
    LinkStatus, LinkTarget, PushOutcome, RepositoryDestination, RepositoryLink, RepositoryRecord,
};
use aruna_core::rows::{decode, encode, value, values};
use aruna_core::structs::execution::job::{ExportRoCrateSpec, JobError, JobErrorKind, JobId};
use aruna_core::structs::identity::auth::AuthContext;
use byteview::ByteView;
use ulid::Ulid;

use super::invenio::remote::CheckLineageOperation;
use super::link_queue::{PushRevisionOperation, Revision};
use super::link_store::{ApplyChangeOperation, ReadLinkOperation, link_row, parse_link, parse_secret, secret_row};
use super::links::{LinkChange, LinkError};
use super::{JobScope, NodeScope, TransferError};
use crate::jobs::executor::JobRunOutcome;
use crate::jobs::export::ExportCheckpoint;
use crate::jobs::store::{JobMutationError, job_rows::PutRowsOperation};

/// The row of an export job's checkpoint.
fn checkpoint_row(job_id: JobId) -> (String, aruna_core::types::Key) {
    (
        JOB_STATE_KEYSPACE.to_string(),
        ByteView::from(job_id.to_bytes().to_vec()),
    )
}

/// The call that reads an export job's checkpoint.
pub(crate) fn read_checkpoint(job_id: JobId) -> IoCall {
    IoCall::Read(vec![checkpoint_row(job_id)])
}

/// The checkpoint a [`read_checkpoint`] call found.
pub(crate) fn parse_checkpoint(result: IoResult) -> Result<Option<ExportCheckpoint>, String> {
    let row = value(result).map_err(|error| error.to_string())?;
    row.map(|bytes| decode(&bytes).map_err(|error| error.to_string()))
        .transpose()
}

/// The child that stores `checkpoint` under the job's claim, rerun after a lost conflict.
pub(crate) fn save_checkpoint(
    job: &JobScope,
    checkpoint: &ExportCheckpoint,
) -> Result<IoCall, TransferError> {
    let bytes = encode(checkpoint).map_err(|error| TransferError::Retryable(error.to_string()))?;
    let (space, key) = checkpoint_row(job.job_id);
    let writes = vec![(space, key, bytes)];
    let (job_id, token) = (job.job_id, job.claim_token);
    let build = move || PutRowsOperation::new(job_id, token, writes.clone(), Vec::new());
    Ok(IoCall::Child(boxed_retry(
        build,
        &job_id.to_bytes(),
        |result| Event::Child(ChildOutcome::new(result)),
    )))
}

/// The end of a [`save_checkpoint`] child.
pub(crate) fn checkpoint_saved(result: IoResult) -> Result<(), TransferError> {
    result
        .child::<(), JobMutationError>()?
        .map_err(|error| match error {
            JobMutationError::Conflict => TransferError::Retryable(
                "checkpoint write exhausted conflict retries".to_string(),
            ),
            error => TransferError::Retryable(error.to_string()),
        })
}

fn permanent(message: &str) -> TransferError {
    TransferError::Permanent(message.into())
}

fn stored(error: LinkError) -> TransferError {
    TransferError::Retryable(error.to_string())
}

#[derive(Debug, PartialEq)]
enum PrepareStep {
    Rows,
    Lineage,
    Save,
}

/// The destination with the link's token, after the link confirmed this job and lineage. A
/// parent-only link keeps the published version it continues in the checkpoint.
#[derive(Debug, PartialEq)]
pub(crate) struct PrepareLinkOperation {
    scope: NodeScope,
    job: JobScope,
    spec: ExportRoCrateSpec,
    destination: RepositoryDestination,
    target: LinkTarget,
    checkpoint: ExportCheckpoint,
    step: PrepareStep,
}

impl PrepareLinkOperation {
    pub(crate) fn new(
        scope: NodeScope,
        job: JobScope,
        spec: ExportRoCrateSpec,
        destination: RepositoryDestination,
        target: LinkTarget,
        checkpoint: ExportCheckpoint,
    ) -> Self {
        Self {
            scope,
            job,
            spec,
            destination,
            target,
            checkpoint,
            step: PrepareStep::Rows,
        }
    }

    fn finish(&mut self) -> Result<Next<Prepared>, TransferError> {
        let mut destination = self.destination.clone();
        destination.published_id = destination
            .published_id
            .or_else(|| self.checkpoint.repository_base.clone());
        Ok(Next::Done(Prepared {
            destination,
            base: self.checkpoint.repository_base.clone(),
        }))
    }
}

/// A prepared link push: the destination and the lineage base the checkpoint now holds.
#[derive(Debug, PartialEq)]
pub(crate) struct Prepared {
    pub(crate) destination: RepositoryDestination,
    pub(crate) base: Option<String>,
}

impl Operation for PrepareLinkOperation {
    type Output = Prepared;
    type Error = TransferError;

    fn first(&mut self) -> Result<Next<Prepared>, TransferError> {
        Ok(Next::Call(IoCall::Read(vec![
            link_row(self.spec.document_id, self.target.link_id),
            secret_row(self.target.link_id),
        ])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Prepared>, TransferError> {
        match self.step {
            PrepareStep::Rows => {
                let [link, secret] = values::<2>(result)?;
                let link = parse_link(link)
                    .map_err(stored)?
                    .filter(|link| link.status != LinkStatus::Paused)
                    .ok_or(TransferError::Cancelled)?;
                match link.active_job {
                    Some(job_id) if job_id == self.job.job_id => {}
                    None => {
                        return Err(TransferError::Retryable(
                            "the link has not started this push".into(),
                        ));
                    }
                    Some(_) => return Err(permanent("a newer push of this link replaced this job")),
                }
                let credential = parse_secret(secret)
                    .map_err(stored)?
                    .filter(|secret| {
                        secret.link_id == Some(link.link_id) && secret.endpoint == link.endpoint
                    })
                    .ok_or_else(|| permanent("the link token was removed"))?;
                self.destination.credential = Some(credential);
                if self.checkpoint.repository.is_some()
                    || self.checkpoint.repository_base.is_some()
                {
                    return self.finish();
                }
                self.step = PrepareStep::Lineage;
                Ok(Next::Call(IoCall::child(CheckLineageOperation::new(
                    link.kind,
                    self.scope.clone(),
                    self.spec.auth_context.clone(),
                    self.spec.limits.metadata_bytes,
                    self.destination.clone(),
                    self.target.clone(),
                ))))
            }
            PrepareStep::Lineage => {
                let Some(base) = result.child::<Option<String>, TransferError>()?? else {
                    return self.finish();
                };
                self.checkpoint.repository_base = Some(base);
                self.step = PrepareStep::Save;
                Ok(Next::Call(save_checkpoint(&self.job, &self.checkpoint)?))
            }
            PrepareStep::Save => {
                checkpoint_saved(result)?;
                self.finish()
            }
        }
    }
}

/// How an export job ended, as the link records it.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ExportEnd {
    Succeeded,
    Cancelled,
    /// A failure that ends the job, with its message.
    Failed(String),
    /// A retry or another end the link does not record.
    Open,
}

impl ExportEnd {
    pub(crate) fn of(outcome: &JobRunOutcome) -> Self {
        match outcome {
            JobRunOutcome::Succeeded(_) => Self::Succeeded,
            JobRunOutcome::Cancelled => Self::Cancelled,
            JobRunOutcome::Failed(error) if error.kind == JobErrorKind::Permanent => {
                Self::Failed(error.message.clone())
            }
            _ => Self::Open,
        }
    }
}

/// `outcome`, or a retry when the link could not record it.
pub(crate) fn settled_outcome(
    outcome: JobRunOutcome,
    settled: Result<Option<String>, LinkError>,
) -> JobRunOutcome {
    match settled {
        Ok(None) => outcome,
        Ok(Some(message)) => JobRunOutcome::Failed(JobError::retryable(message)),
        Err(error) => JobRunOutcome::Failed(JobError::retryable(error.to_string())),
    }
}

#[derive(Debug, PartialEq)]
enum SettleStep {
    Link,
    Checkpoint(RepositoryLink),
    Revision(RepositoryLink, PushOutcome),
    Change,
}

/// Records the end of a push on its link. Answers the message of a retry when the record failed,
/// so a failed record keeps the job retrying.
#[derive(Debug, PartialEq)]
pub(crate) struct SettlePushOperation {
    scope: NodeScope,
    job_id: JobId,
    document_id: Ulid,
    target: LinkTarget,
    end: ExportEnd,
    step: SettleStep,
}

impl SettlePushOperation {
    pub(crate) fn new(
        scope: NodeScope,
        job_id: JobId,
        document_id: Ulid,
        target: LinkTarget,
        end: ExportEnd,
    ) -> Self {
        Self {
            scope,
            job_id,
            document_id,
            target,
            end,
            step: SettleStep::Link,
        }
    }

    fn change(
        &mut self,
        link: RepositoryLink,
        push: PushOutcome,
        requeue: bool,
    ) -> Result<Next<Option<String>>, LinkError> {
        let change = LinkChange::Finish {
            job_id: self.job_id,
            outcome: Box::new(push),
            requeue,
        };
        self.step = SettleStep::Change;
        Ok(Next::Call(IoCall::child(ApplyChangeOperation::new(
            self.scope.clone(),
            link,
            change,
        ))))
    }
}

impl Operation for SettlePushOperation {
    type Output = Option<String>;
    type Error = LinkError;

    fn first(&mut self) -> Result<Next<Option<String>>, LinkError> {
        let row = link_row(self.document_id, self.target.link_id);
        Ok(Next::Call(IoCall::Read(vec![row])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<String>>, LinkError> {
        match std::mem::replace(&mut self.step, SettleStep::Change) {
            SettleStep::Link => {
                let link = match value(result).map_err(LinkError::from).and_then(parse_link) {
                    Ok(Some(link)) if link.active_job == Some(self.job_id) => link,
                    Ok(_) => return Ok(Next::Done(None)),
                    Err(error) => return Ok(Next::Done(Some(error.to_string()))),
                };
                self.step = SettleStep::Checkpoint(link);
                Ok(Next::Call(read_checkpoint(self.job_id)))
            }
            SettleStep::Checkpoint(link) => {
                let checkpoint = match parse_checkpoint(result) {
                    Ok(checkpoint) => checkpoint.unwrap_or_default(),
                    Err(error) => return Ok(Next::Done(Some(error))),
                };
                // A push that reached the repository counts as pushed, even when a later step
                // failed.
                let push = match (&self.end, checkpoint.pushed_outcome()) {
                    (ExportEnd::Open, _) => return Ok(Next::Done(None)),
                    (_, Some(pushed)) => pushed,
                    (ExportEnd::Failed(message), None) => {
                        PushOutcome::Failed(checkpoint.push_failure(message))
                    }
                    (ExportEnd::Cancelled, None) => PushOutcome::Cancelled,
                    (ExportEnd::Succeeded, None) => return Ok(Next::Done(None)),
                };
                if matches!(push, PushOutcome::Pushed { .. }) {
                    self.step = SettleStep::Revision(link, push);
                    return Ok(Next::Call(IoCall::child(PushRevisionOperation::new(
                        self.document_id,
                    ))));
                }
                self.change(link, push, false)
            }
            SettleStep::Revision(link, push) => {
                let current = result.child::<Option<Revision>, LinkError>()?;
                let requeue = match (&push, current) {
                    (PushOutcome::Pushed { event_id, .. }, Ok(Some((current, _)))) => {
                        current != *event_id
                    }
                    _ => true,
                };
                self.change(link, push, requeue)
            }
            SettleStep::Change => match result.child::<Option<RepositoryLink>, LinkError>()? {
                Ok(_) | Err(LinkError::NotFound) => Ok(Next::Done(None)),
                // A cancelled job cannot retry; the queue drain settles its link later.
                Err(_) if self.end == ExportEnd::Cancelled => Ok(Next::Done(None)),
                Err(error) => Ok(Next::Done(Some(format!(
                    "recording the link push failed: {error}"
                )))),
            },
        }
    }
}

/// Stops a push once its link is gone, paused or runs another job; checked before remote writes.
#[derive(Debug, PartialEq)]
pub(crate) struct GuardOperation {
    job_id: JobId,
    document_id: Ulid,
    link_id: Option<Ulid>,
}

impl GuardOperation {
    pub(crate) fn new(
        job_id: JobId,
        document_id: Ulid,
        destination: &RepositoryDestination,
    ) -> Self {
        Self {
            job_id,
            document_id,
            link_id: destination.link.as_ref().map(|target| target.link_id),
        }
    }
}

impl Operation for GuardOperation {
    type Output = ();
    type Error = TransferError;

    fn first(&mut self) -> Result<Next<()>, TransferError> {
        Ok(match self.link_id {
            Some(link_id) => Next::Call(IoCall::Read(vec![link_row(self.document_id, link_id)])),
            None => Next::Done(()),
        })
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, TransferError> {
        let link = parse_link(value(result)?).map_err(stored)?;
        match link {
            Some(link)
                if link.active_job == Some(self.job_id) && link.status != LinkStatus::Paused =>
            {
                Ok(Next::Done(()))
            }
            _ => Err(TransferError::Cancelled),
        }
    }
}

#[derive(Debug, PartialEq)]
enum DraftStep {
    Link,
    Change,
}

/// Stores the job's draft on the link, so a failed push continues the same draft.
#[derive(Debug, PartialEq)]
pub(crate) struct RecordDraftOperation {
    scope: NodeScope,
    job_id: JobId,
    document_id: Ulid,
    link_id: Ulid,
    record: RepositoryRecord,
    step: DraftStep,
}

impl RecordDraftOperation {
    pub(crate) fn new(
        scope: NodeScope,
        job_id: JobId,
        document_id: Ulid,
        link_id: Ulid,
        record: RepositoryRecord,
    ) -> Self {
        Self {
            scope,
            job_id,
            document_id,
            link_id,
            record,
            step: DraftStep::Link,
        }
    }
}

impl Operation for RecordDraftOperation {
    type Output = ();
    type Error = TransferError;

    fn first(&mut self) -> Result<Next<()>, TransferError> {
        Ok(Next::Call(IoCall::child(ReadLinkOperation::new(
            self.document_id,
            self.link_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, TransferError> {
        match self.step {
            DraftStep::Link => {
                let link = result
                    .child::<Option<RepositoryLink>, LinkError>()?
                    .map_err(stored)?
                    .ok_or(TransferError::Cancelled)?;
                let change = LinkChange::Draft {
                    job_id: self.job_id,
                    record: Box::new(self.record.clone()),
                };
                self.step = DraftStep::Change;
                Ok(Next::Call(IoCall::child(ApplyChangeOperation::new(
                    self.scope.clone(),
                    link,
                    change,
                ))))
            }
            DraftStep::Change => match result.child::<Option<RepositoryLink>, LinkError>()? {
                Ok(_) => Ok(Next::Done(())),
                Err(LinkError::NotFound | LinkError::Busy(_)) => Err(TransferError::Cancelled),
                Err(error) => Err(stored(error)),
            },
        }
    }
}

/// The identity pushes of a link run as: its creator, signed in to the realm this node serves.
pub(crate) fn creator_auth(scope: &NodeScope, link: &RepositoryLink) -> AuthContext {
    AuthContext {
        user_id: link.created_by,
        realm_id: scope.realm_or(link.created_by.realm_id),
        path_restrictions: None,
        session: None,
    }
}

#[cfg(test)]
#[path = "push_tests.rs"]
mod tests;
