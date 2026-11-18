//! Reads the job records and status a caller may see on this node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::jobs::JobStatusView;
use aruna_core::keyspaces::DOCUMENT_INDEX_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::rows::{decoded, read_row};
use aruna_core::storage_entries::metadata_document_key;
use aruna_core::structs::execution::job::{
    JobId, JobPayload, JobRecord, JobState, KeyWait, RunCrateStatus, StagingJobCheckpoint,
};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::{JobOwnerError, RealmConfigDocument};
use aruna_core::structs::placement::policy::document::group_admin_path;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::types::GroupId;
use thiserror::Error;
use ulid::Ulid;

use super::records::pages::ServingNode;
use super::route::{JobRouteError, RoutedJobStatus};
use super::staging::{ReadCheckpointOperation, StagingStoreError};
use super::store::mutation_error::JobMutationError;
use super::store::row_calls::{read_crate_status, read_job_record, read_key_waits};
use crate::auth::error::AuthorizeError;
use crate::auth::request_authorization::AuthorizeOperation;
use crate::auth::request_policy::PolicyRequestExtras;
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::realm::load_config::config_unavailable;

/// A local job read failed; the text is what the caller reports.
#[derive(Debug, Error, PartialEq)]
#[error("{0}")]
pub struct JobReadError(pub String);

impl From<IoError> for JobReadError {
    fn from(error: IoError) -> Self {
        Self(error.to_string())
    }
}

impl From<JobMutationError> for JobReadError {
    fn from(error: JobMutationError) -> Self {
        Self(error.to_string())
    }
}

pub(super) fn internal(error: impl std::fmt::Display) -> JobRouteError {
    JobRouteError::Internal(error.to_string())
}

pub(super) fn unavailable(reason: impl std::fmt::Display) -> JobRouteError {
    JobRouteError::Unavailable(reason.to_string())
}

/// The record a job-record child answered.
pub(super) fn job_row(result: IoResult) -> Result<Option<JobRecord>, JobReadError> {
    Ok(result.child::<Option<JobRecord>, JobMutationError>()??)
}

pub(super) fn document_read(document_id: Ulid) -> IoCall {
    read_row(DOCUMENT_INDEX_KEYSPACE, metadata_document_key(document_id))
}

pub(super) fn document_row(
    result: IoResult,
) -> Result<Option<MetadataRegistryRecord>, JobReadError> {
    Ok(decoded(result)?)
}

/// The caller's own job. Internal jobs read as absent.
#[derive(Debug, PartialEq)]
pub struct OwnedJobOperation {
    user_id: UserId,
    job_id: JobId,
}

impl OwnedJobOperation {
    pub fn new(user_id: UserId, job_id: JobId) -> Self {
        Self { user_id, job_id }
    }
}

fn is_owned(record: &JobRecord, user_id: UserId) -> bool {
    record.created_by == user_id && !record.payload.is_internal()
}

impl Operation for OwnedJobOperation {
    type Output = Option<JobRecord>;
    type Error = JobReadError;

    fn first(&mut self) -> Result<Next<Self::Output>, JobReadError> {
        Ok(Next::Call(IoCall::child(read_job_record(self.job_id))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, JobReadError> {
        let user_id = self.user_id;
        let record = job_row(result)?.filter(|record| is_owned(record, user_id));
        Ok(Next::Done(record))
    }
}

/// What a foreign job needs before the caller may read it as their own.
#[derive(Debug, PartialEq)]
enum ReadGrant {
    JoinedMint,
    LinkedPush(GroupId),
}

#[derive(Debug, PartialEq)]
enum ReadableStep {
    Record,
    Document(Box<JobRecord>, ReadGrant),
    Check(Box<JobRecord>, Vec<(String, Permission)>),
}

/// The caller's own job, a PID mint job it joined, or a repository push of a link it manages.
/// The last two are served as the caller's own, so the first submitter stays undisclosed.
#[derive(Debug, PartialEq)]
pub struct ReadableJobOperation {
    auth: AuthContext,
    job_id: JobId,
    serving: ServingNode,
    step: ReadableStep,
}

type ReadableNext = Result<Next<Option<JobRecord>>, JobReadError>;

impl ReadableJobOperation {
    pub fn new(auth: &AuthContext, job_id: JobId, serving: ServingNode) -> Self {
        Self {
            auth: auth.clone(),
            job_id,
            serving,
            step: ReadableStep::Record,
        }
    }

    fn foreign(&mut self, record: JobRecord) -> ReadableNext {
        let (document_id, grant) = match &record.payload {
            JobPayload::MintPersistentId(spec) => (spec.document_id, ReadGrant::JoinedMint),
            JobPayload::ExportRoCrate(spec) => {
                let Some(destination) = spec.destination.as_ref().filter(|d| d.link.is_some())
                else {
                    return Ok(Next::Done(None));
                };
                let realm_id = self
                    .serving
                    .map_or(record.created_by.realm_id, |(_, realm_id)| realm_id);
                if realm_id != self.auth.realm_id {
                    return Ok(Next::Done(None));
                }
                let grant = ReadGrant::LinkedPush(destination.group_id);
                (spec.document_id, grant)
            }
            _ => return Ok(Next::Done(None)),
        };
        self.step = ReadableStep::Document(Box::new(record), grant);
        Ok(Next::Call(document_read(document_id)))
    }

    /// A joined mint needs the WRITE its route required. A push report lists the dataset's
    /// files, so its reader must also hold READ on the dataset.
    fn checks(
        &self,
        document: MetadataRegistryRecord,
        grant: ReadGrant,
    ) -> Vec<(String, Permission)> {
        match grant {
            ReadGrant::JoinedMint => vec![(document.permission_path, Permission::WRITE)],
            ReadGrant::LinkedPush(group_id) => vec![
                (document.permission_path, Permission::READ),
                (
                    group_admin_path(self.auth.realm_id, group_id),
                    Permission::WRITE,
                ),
            ],
        }
    }

    fn check_next(
        &mut self,
        mut record: Box<JobRecord>,
        mut checks: Vec<(String, Permission)>,
    ) -> ReadableNext {
        let Some((path, permission)) = checks.pop() else {
            record.created_by = self.auth.user_id;
            return Ok(Next::Done(Some(*record)));
        };
        self.step = ReadableStep::Check(record, checks);
        Ok(Next::Call(IoCall::child(AuthorizeOperation::new(
            self.auth.realm_id,
            &self.auth,
            &path,
            &permission,
            PolicyRequestExtras::rest(),
        ))))
    }
}

impl Operation for ReadableJobOperation {
    type Output = Option<JobRecord>;
    type Error = JobReadError;

    fn first(&mut self) -> ReadableNext {
        Ok(Next::Call(IoCall::child(read_job_record(self.job_id))))
    }

    fn next(&mut self, result: IoResult) -> ReadableNext {
        match std::mem::replace(&mut self.step, ReadableStep::Record) {
            ReadableStep::Record => match job_row(result)? {
                None => Ok(Next::Done(None)),
                Some(record) if is_owned(&record, self.auth.user_id) => {
                    Ok(Next::Done(Some(record)))
                }
                Some(record) => self.foreign(record),
            },
            ReadableStep::Document(record, grant) => match document_row(result)? {
                None => Ok(Next::Done(None)),
                Some(document) => {
                    let checks = self.checks(document, grant);
                    self.check_next(record, checks)
                }
            },
            ReadableStep::Check(record, checks) => match result.child::<(), AuthorizeError>()? {
                Ok(()) => self.check_next(record, checks),
                Err(AuthorizeError::PermissionDenied | AuthorizeError::Policy(_)) => {
                    Ok(Next::Done(None))
                }
                Err(AuthorizeError::CheckFailed(error)) => Err(JobReadError(error)),
                Err(AuthorizeError::Storage(error)) => Err(JobReadError(error.to_string())),
            },
        }
    }
}

#[derive(Debug, PartialEq)]
enum LocalStatusStep {
    Job,
    Crate(Box<JobRecord>),
    Waits(Box<RoutedJobStatus>),
}

/// The status of a job this node holds, with its run crate and the keys it waits for.
#[derive(Debug, PartialEq)]
pub struct LocalStatusOperation {
    auth: AuthContext,
    job_id: JobId,
    serving: ServingNode,
    step: LocalStatusStep,
}

impl LocalStatusOperation {
    pub fn new(auth: &AuthContext, job_id: JobId, serving: ServingNode) -> Self {
        Self {
            auth: auth.clone(),
            job_id,
            serving,
            step: LocalStatusStep::Job,
        }
    }
}

impl Operation for LocalStatusOperation {
    type Output = RoutedJobStatus;
    type Error = JobRouteError;

    fn first(&mut self) -> Result<Next<RoutedJobStatus>, JobRouteError> {
        let read = ReadableJobOperation::new(&self.auth, self.job_id, self.serving);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<RoutedJobStatus>, JobRouteError> {
        match std::mem::replace(&mut self.step, LocalStatusStep::Job) {
            LocalStatusStep::Job => {
                let record = result
                    .child::<Option<JobRecord>, JobReadError>()?
                    .map_err(internal)?
                    .ok_or(JobRouteError::NotFound)?;
                self.step = LocalStatusStep::Crate(Box::new(record));
                Ok(Next::Call(IoCall::child(read_crate_status(self.job_id))))
            }
            LocalStatusStep::Crate(record) => {
                let run_crate = result
                    .child::<Option<RunCrateStatus>, JobMutationError>()?
                    .map_err(internal)?
                    .map(|status| status.to_public_json());
                let status = RoutedJobStatus {
                    job: JobStatusView::from(&*record),
                    run_crate,
                };
                if record.state != JobState::AwaitingKey {
                    return Ok(Next::Done(status));
                }
                self.step = LocalStatusStep::Waits(Box::new(status));
                Ok(Next::Call(IoCall::child(read_key_waits(self.job_id))))
            }
            LocalStatusStep::Waits(mut status) => {
                status.job.awaiting_keys = result
                    .child::<Vec<KeyWait>, JobMutationError>()?
                    .map_err(internal)?;
                Ok(Next::Done(*status))
            }
        }
    }

    fn expected_error(error: &JobRouteError) -> bool {
        matches!(error, JobRouteError::NotFound)
    }
}

/// A job record with the staging checkpoint it carries.
pub type RecordData = (JobRecord, Option<StagingJobCheckpoint>);

/// The caller's own job here, with the checkpoint of a staging job.
#[derive(Debug, PartialEq)]
pub struct RecordDataOperation {
    user_id: UserId,
    job_id: JobId,
    record: Option<Box<JobRecord>>,
}

impl RecordDataOperation {
    pub fn new(user_id: UserId, job_id: JobId) -> Self {
        Self {
            user_id,
            job_id,
            record: None,
        }
    }
}

impl Operation for RecordDataOperation {
    type Output = Option<RecordData>;
    type Error = JobReadError;

    fn first(&mut self) -> Result<Next<Self::Output>, JobReadError> {
        let read = OwnedJobOperation::new(self.user_id, self.job_id);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, JobReadError> {
        if let Some(record) = self.record.take() {
            let checkpoint = result
                .child::<Option<StagingJobCheckpoint>, StagingStoreError>()?
                .map_err(|error| JobReadError(error.0))?;
            return Ok(Next::Done(Some((*record, checkpoint))));
        }
        let Some(record) = result.child::<Option<JobRecord>, JobReadError>()?? else {
            return Ok(Next::Done(None));
        };
        if !matches!(&record.payload, JobPayload::Staging(_)) {
            return Ok(Next::Done(Some((record, None))));
        }
        self.record = Some(Box::new(record));
        let read = ReadCheckpointOperation::new(self.job_id);
        Ok(Next::Call(IoCall::child(read)))
    }
}

/// Derives a job's immutable owner from its id and replicated placement, so no other node is
/// asked. A missing or unsynced binding is `Unavailable`; only an invalid id is `NotFound`.
#[derive(Debug, PartialEq)]
pub struct JobOwnerOperation {
    job_id: JobId,
    serving: ServingNode,
}

impl JobOwnerOperation {
    pub fn new(job_id: JobId, serving: ServingNode) -> Self {
        Self { job_id, serving }
    }
}

impl Operation for JobOwnerOperation {
    type Output = NodeId;
    type Error = JobRouteError;

    fn first(&mut self) -> Result<Next<NodeId>, JobRouteError> {
        let (_, realm_id) = self
            .serving
            .ok_or_else(|| unavailable("network handle unavailable"))?;
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(realm_id))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<NodeId>, JobRouteError> {
        let config = result
            .child::<RealmConfigDocument, GetConfigError>()?
            .inspect_err(config_unavailable)
            .map_err(|_| unavailable("realm config unavailable"))?;
        match config.job_owner(self.job_id) {
            Ok(owner) => Ok(Next::Done(owner)),
            Err(JobOwnerError::NotJobControl) => Err(JobRouteError::NotFound),
            Err(JobOwnerError::Unavailable(message)) => Err(JobRouteError::Unavailable(message)),
        }
    }

    fn expected_error(error: &JobRouteError) -> bool {
        matches!(error, JobRouteError::NotFound)
    }
}

#[cfg(test)]
#[path = "job_reads_tests.rs"]
pub(crate) mod tests;
