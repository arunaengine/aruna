//! Checks pull links against their record lineage and starts imports of new versions.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::repository::{
    LinkDirection, LinkFailure, LinkPull, LinkRemote, LinkStatus, PULL_CHECK_MS, PullCheck,
    PushOutcome, RepositoryLink, RepositoryPull, RepositoryRecord,
};
use aruna_core::rows::{event, value};
use aruna_core::structs::execution::harvest::RepositoryConnectorKind;
use aruna_core::structs::execution::job::{
    ImportMetadataTarget, ImportRoCrateSource, ImportRoCrateSpec, JobId, JobRecord, JobState,
};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::invenio::remote::LatestVersionOperation;
use super::link_queue::{EnsureHolderOperation, submit_failure};
use super::link_store::{
    ApplyChangeOperation, EnsureLineageOperation, ReadLinkOperation, link_row, parse_link, time_of,
};
use super::links::{LinkChange, LinkError};
use super::push::creator_auth;
use super::submit::{LinkJob, LinkJobOperation};
use super::{NodeScope, TransferError};
use crate::jobs::store::job_rows::ReadJobOperation;
use crate::jobs::submit::{SubmitJobError, SubmitJobResult};
use crate::metadata::repository::{parse_registry_read, read_document_registry};

/// How often a check waits for a running pull before it looks again.
const ACTIVE_RETRY_MS: u64 = 60_000;

type Turn<T> = Result<Next<T>, LinkError>;

#[derive(Debug, PartialEq)]
enum DueStep {
    Job(JobId),
    Change,
    Check,
    Pull,
}

/// Checks one due pull link and starts an automatic update; answers when to look again if no
/// link change moved its queued check.
#[derive(Debug, PartialEq)]
pub(crate) struct CheckDueOperation {
    scope: NodeScope,
    link: RepositoryLink,
    now_ms: u64,
    step: DueStep,
}

impl CheckDueOperation {
    pub(crate) fn new(scope: NodeScope, link: RepositoryLink) -> Self {
        Self {
            scope,
            link,
            now_ms: 0,
            step: DueStep::Check,
        }
    }

    fn retry(&self) -> Turn<Option<u64>> {
        Ok(Next::Done(Some(
            self.now_ms.saturating_add(ACTIVE_RETRY_MS),
        )))
    }
}

/// How a pull job that ended without recording its outcome counts on the link.
fn stale_outcome(record: Option<JobRecord>) -> PushOutcome {
    match record {
        Some(record) if record.state == JobState::Failed => {
            let message = record
                .last_error
                .map_or("pull job failed".to_string(), |error| error.message);
            PushOutcome::Failed(LinkFailure::Other(message))
        }
        Some(_) => PushOutcome::Cancelled,
        None => PushOutcome::Failed(LinkFailure::Other("pull job missing".into())),
    }
}

impl Operation for CheckDueOperation {
    type Output = Option<u64>;
    type Error = LinkError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Turn<Option<u64>> {
        if let Some(job_id) = self.link.active_job {
            self.step = DueStep::Job(job_id);
            return Ok(Next::Call(IoCall::child(ReadJobOperation::new(job_id))));
        }
        Ok(Next::Call(IoCall::child(CheckNowOperation::new(
            self.scope.clone(),
            self.link.clone(),
        ))))
    }

    fn next(&mut self, result: IoResult) -> Turn<Option<u64>> {
        match self.step {
            // A pull job that ended without recording its outcome, for example cancelled while
            // queued.
            DueStep::Job(job_id) => {
                let record = result
                    .child::<Option<JobRecord>, aruna_core::calls::IoError>()?
                    .map_err(|error| LinkError::Unexpected(error.to_string()))?;
                if record
                    .as_ref()
                    .is_some_and(|record| !record.state.is_terminal())
                {
                    return self.retry();
                }
                let change = LinkChange::Finish {
                    job_id,
                    outcome: Box::new(stale_outcome(record)),
                    requeue: false,
                };
                self.step = DueStep::Change;
                Ok(Next::Call(IoCall::child(ApplyChangeOperation::new(
                    self.scope.clone(),
                    self.link.clone(),
                    change,
                ))))
            }
            DueStep::Change => {
                result.child::<Option<RepositoryLink>, LinkError>()??;
                Ok(Next::Done(None))
            }
            DueStep::Check => {
                let Some(stored) = result.child::<Option<RepositoryLink>, LinkError>()?? else {
                    return Ok(Next::Done(None));
                };
                if !stored.auto_pulls() {
                    return Ok(Next::Done(None));
                }
                self.step = DueStep::Pull;
                Ok(Next::Call(IoCall::child(StartPullOperation::new(
                    self.scope.clone(),
                    stored,
                ))))
            }
            DueStep::Pull => match result.child::<JobId, LinkError>()? {
                Ok(_) => Ok(Next::Done(None)),
                Err(LinkError::JobLimit(_)) => self.retry(),
                Err(error) => Err(error),
            },
        }
    }
}

#[derive(Debug, PartialEq)]
enum NowStep {
    Remote,
    Change,
}

/// Asks the repository for the lineage's latest version and stores the answer on the link. A
/// refusal, such as a withdrawn record, fails the link. Answers the stored link.
#[derive(Debug, PartialEq)]
pub struct CheckNowOperation {
    scope: NodeScope,
    link: RepositoryLink,
    step: NowStep,
}

impl CheckNowOperation {
    pub fn new(scope: NodeScope, link: RepositoryLink) -> Self {
        Self {
            scope,
            link,
            step: NowStep::Remote,
        }
    }
}

impl Operation for CheckNowOperation {
    type Output = Option<RepositoryLink>;
    type Error = LinkError;

    fn first(&mut self) -> Turn<Option<RepositoryLink>> {
        Ok(Next::Call(IoCall::child(LatestVersionOperation::new(
            self.scope.clone(),
            self.link.clone(),
        ))))
    }

    fn next(&mut self, result: IoResult) -> Turn<Option<RepositoryLink>> {
        match self.step {
            NowStep::Remote => {
                let change = match result.child::<PullCheck, TransferError>()? {
                    Ok(check) => LinkChange::Checked(check),
                    Err(TransferError::Refused(reason)) => LinkChange::Fail(reason),
                    Err(TransferError::Permanent(message)) => {
                        LinkChange::Fail(LinkFailure::Other(message))
                    }
                    Err(_) => LinkChange::Checked(PullCheck::Unavailable),
                };
                self.step = NowStep::Change;
                Ok(Next::Call(IoCall::child(ApplyChangeOperation::new(
                    self.scope.clone(),
                    self.link.clone(),
                    change,
                ))))
            }
            NowStep::Change => {
                let stored = result.child::<Option<RepositoryLink>, LinkError>()??;
                Ok(Next::Done(stored))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum PullStep {
    Holder,
    Lineage,
    Document,
    Submit,
    Begin(JobId),
}

/// Submits an import of the lineage's latest version into the linked dataset, as the creator.
#[derive(Debug, PartialEq)]
pub struct StartPullOperation {
    scope: NodeScope,
    link: RepositoryLink,
    record_id: String,
    step: PullStep,
}

impl StartPullOperation {
    pub fn new(scope: NodeScope, link: RepositoryLink) -> Self {
        Self {
            scope,
            link,
            record_id: String::new(),
            step: PullStep::Holder,
        }
    }

    fn pull(&self) -> Result<&LinkPull, LinkError> {
        self.link
            .pull()
            .ok_or_else(|| LinkError::Unexpected("the link does not pull".into()))
    }

    fn read_document(&mut self) -> Turn<JobId> {
        self.step = PullStep::Document;
        Ok(Next::Call(
            read_document_registry(self.link.document_id, None).into(),
        ))
    }
}

impl Operation for StartPullOperation {
    type Output = JobId;
    type Error = LinkError;

    fn first(&mut self) -> Turn<JobId> {
        self.record_id = self
            .pull()?
            .latest_remote_id
            .clone()
            .or_else(|| self.link.remote.record_id.clone())
            .ok_or(LinkError::NoRevision)?;
        Ok(Next::Call(IoCall::child(EnsureHolderOperation::new(
            self.scope.clone(),
            self.link.clone(),
        ))))
    }

    fn next(&mut self, result: IoResult) -> Turn<JobId> {
        match self.step {
            PullStep::Holder => {
                result.child::<(), LinkError>()??;
                // A failed link runs again only if no enabled link follows its lineage the
                // other way.
                if matches!(self.link.status, LinkStatus::Failed { .. }) {
                    self.step = PullStep::Lineage;
                    return Ok(Next::Call(IoCall::child(EnsureLineageOperation::new(
                        self.link.clone(),
                    ))));
                }
                self.read_document()
            }
            PullStep::Lineage => {
                result.child::<(), LinkError>()??;
                self.read_document()
            }
            PullStep::Document => {
                let document = parse_registry_read(event(result)?)
                    .map_err(|error| LinkError::Unexpected(format!("{error:?}")))?
                    .ok_or(LinkError::NotHolder)?;
                let pull = self.pull()?;
                let spec = ImportRoCrateSpec {
                    auth_context: creator_auth(&self.scope, &self.link),
                    source: ImportRoCrateSource::Repository {
                        group_id: self.link.group_id,
                        connector_id: self.link.connector_id,
                        record_id: self.record_id.clone(),
                        options: pull.options.clone(),
                        pull: Some(RepositoryPull::Update {
                            link_id: self.link.link_id,
                        }),
                    },
                    target: pull.target.clone(),
                    metadata: ImportMetadataTarget {
                        group_id: document.group_id,
                        path: document.document_path,
                        public: document.public,
                    },
                    limits: self.link.limits.clone(),
                    document_id: self.link.document_id,
                };
                let key = format!(
                    "repository-pull/{}/{}/{}",
                    self.link.link_id, self.record_id, self.link.sequence
                );
                self.step = PullStep::Submit;
                Ok(Next::Call(IoCall::child(LinkJobOperation::new(
                    self.scope.clone(),
                    self.link.owner_node,
                    LinkJob::Import(spec),
                    key,
                ))))
            }
            PullStep::Submit => {
                let submitted = result
                    .child::<SubmitJobResult, SubmitJobError>()?
                    .map_err(submit_failure)?;
                self.step = PullStep::Begin(submitted.job_id);
                Ok(Next::Call(IoCall::child(ApplyChangeOperation::new(
                    self.scope.clone(),
                    self.link.clone(),
                    LinkChange::Begin(submitted.job_id),
                ))))
            }
            PullStep::Begin(job_id) => {
                result.child::<Option<RepositoryLink>, LinkError>()??;
                Ok(Next::Done(job_id))
            }
        }
    }
}

/// What an import that keeps or updates a pull link carries to its cleanup.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct PullProgress {
    /// The version the dataset holds after the import.
    pub(crate) record: RepositoryRecord,
    pub(crate) endpoint: String,
    /// The dataset revision an update merged into.
    pub(crate) base: Option<Ulid>,
    /// The dataset revision the import wrote.
    pub(crate) revision: Option<Ulid>,
    pub(crate) kind: RepositoryConnectorKind,
}

/// Stops an update once its link is gone, paused or runs another job.
#[derive(Debug, PartialEq)]
pub(crate) struct PullRunningOperation {
    job_id: JobId,
    document_id: Ulid,
    link_id: Ulid,
}

impl PullRunningOperation {
    pub(crate) fn new(job_id: JobId, document_id: Ulid, link_id: Ulid) -> Self {
        Self {
            job_id,
            document_id,
            link_id,
        }
    }
}

impl Operation for PullRunningOperation {
    type Output = ();
    type Error = TransferError;

    fn first(&mut self) -> Result<Next<()>, TransferError> {
        let row = link_row(self.document_id, self.link_id);
        Ok(Next::Call(IoCall::Read(vec![row])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, TransferError> {
        let link = parse_link(value(result)?)
            .map_err(|error| TransferError::Retryable(error.to_string()))?
            .filter(|link| link.status != LinkStatus::Paused && link.pull().is_some())
            .ok_or(TransferError::Cancelled)?;
        match link.active_job {
            Some(job_id) if job_id == self.job_id => Ok(Next::Done(())),
            None => Err(TransferError::Retryable(
                "the link has not started this pull".into(),
            )),
            Some(_) => Err(TransferError::Permanent(
                "a newer pull of this link replaced this job".into(),
            )),
        }
    }
}

#[derive(Debug, PartialEq)]
enum SettleStep {
    Create,
    Read,
    Update,
}

/// Records the end of an import on its pull link: a new link after an import that keeps one,
/// the held version after an update, or the failed or cancelled update.
#[derive(Debug, PartialEq)]
pub(crate) struct SettleImportOperation {
    scope: NodeScope,
    job_id: JobId,
    owner_node: aruna_core::NodeId,
    spec: ImportRoCrateSpec,
    progress: Option<PullProgress>,
    failure: Option<String>,
    now_ms: u64,
    step: SettleStep,
}

impl SettleImportOperation {
    pub(crate) fn new(
        scope: NodeScope,
        job_id: JobId,
        owner_node: aruna_core::NodeId,
        spec: ImportRoCrateSpec,
        progress: Option<PullProgress>,
        failure: Option<String>,
    ) -> Self {
        Self {
            scope,
            job_id,
            owner_node,
            spec,
            progress,
            failure,
            now_ms: 0,
            step: SettleStep::Create,
        }
    }

    fn done_version(&self) -> Option<(&PullProgress, Ulid)> {
        let progress = self.progress.as_ref()?;
        Some((progress, progress.revision?))
    }

    /// The link an import that keeps one leaves behind, already holding the imported version.
    fn kept_link(&self, auto_update: bool, owner_node_url: &str) -> Option<RepositoryLink> {
        let ImportRoCrateSource::Repository {
            group_id,
            connector_id,
            options,
            ..
        } = &self.spec.source
        else {
            return None;
        };
        let (progress, revision) = self.done_version()?;
        let now = time_of(self.now_ms);
        let mut link = RepositoryLink {
            link_id: self.job_id.as_ulid(),
            document_id: self.spec.document_id,
            group_id: *group_id,
            connector_id: *connector_id,
            endpoint: progress.endpoint.clone(),
            owner_node: self.owner_node,
            owner_node_url: owner_node_url.to_string(),
            created_by: self.spec.auth_context.user_id,
            status: LinkStatus::Enabled,
            auto_publish: false,
            public_files: false,
            metadata_json: "{}".into(),
            remote: LinkRemote::default(),
            last_push: None,
            active_job: None,
            sequence: 0,
            limits: self.spec.limits.clone(),
            created_at: now,
            updated_at: now,
            generation: 0,
            warning: None,
            kind: progress.kind,
            direction: LinkDirection::Pull(Box::new(LinkPull {
                auto_update,
                options: options.clone(),
                target: self.spec.target.clone(),
                latest_remote_id: None,
                latest_revision: None,
                last_checked_at: None,
                next_check_ms: self.now_ms.saturating_add(PULL_CHECK_MS),
                failures: 0,
                revision: None,
                local_changed: false,
            })),
        };
        link.hold(&progress.record, revision, now);
        Some(link)
    }

    fn update_change(&self) -> LinkChange {
        match self.done_version() {
            Some((progress, revision)) => LinkChange::Pulled {
                job_id: self.job_id,
                record: Box::new(progress.record.clone()),
                revision,
            },
            None => LinkChange::Finish {
                job_id: self.job_id,
                outcome: Box::new(match &self.failure {
                    Some(message) => PushOutcome::Failed(LinkFailure::Other(message.clone())),
                    None => PushOutcome::Cancelled,
                }),
                requeue: false,
            },
        }
    }
}

impl Operation for SettleImportOperation {
    type Output = ();
    type Error = LinkError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Turn<()> {
        let ImportRoCrateSource::Repository {
            pull: Some(pull), ..
        } = &self.spec.source
        else {
            return Ok(Next::Done(()));
        };
        match pull.clone() {
            RepositoryPull::Keep {
                auto_update,
                owner_node_url,
            } => {
                let Some(link) = self.kept_link(auto_update, &owner_node_url) else {
                    return Ok(Next::Done(()));
                };
                let change = LinkChange::Create {
                    link: Box::new(link.clone()),
                    secret: None,
                };
                self.step = SettleStep::Create;
                Ok(Next::Call(IoCall::child(ApplyChangeOperation::new(
                    self.scope.clone(),
                    link,
                    change,
                ))))
            }
            RepositoryPull::Update { link_id } => {
                self.step = SettleStep::Read;
                Ok(Next::Call(IoCall::child(ReadLinkOperation::new(
                    self.spec.document_id,
                    link_id,
                ))))
            }
        }
    }

    fn next(&mut self, result: IoResult) -> Turn<()> {
        match self.step {
            SettleStep::Create => match result.child::<Option<RepositoryLink>, LinkError>()? {
                Ok(_) | Err(LinkError::Exists) => Ok(Next::Done(())),
                Err(error) => Err(error),
            },
            SettleStep::Read => {
                let link = result
                    .child::<Option<RepositoryLink>, LinkError>()??
                    .filter(|link| link.active_job == Some(self.job_id));
                let Some(link) = link else {
                    return Ok(Next::Done(()));
                };
                self.step = SettleStep::Update;
                Ok(Next::Call(IoCall::child(ApplyChangeOperation::new(
                    self.scope.clone(),
                    link,
                    self.update_change(),
                ))))
            }
            SettleStep::Update => match result.child::<Option<RepositoryLink>, LinkError>()? {
                Ok(_) | Err(LinkError::NotFound) => Ok(Next::Done(())),
                Err(error) => Err(error),
            },
        }
    }
}

#[cfg(test)]
#[path = "pull_tests.rs"]
mod tests;
