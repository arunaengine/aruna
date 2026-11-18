//! Reads the report pages of finished jobs, here or at the job's owner.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::jobs::{JobKind, JobReportView, JobRequest, JobResponse};
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{
    JobFamilyId, JobId, JobPayload, JobRecord, JobState, SessionReportDetail, SessionReportRow,
};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::types::Value;

use super::job_reads::{JobReadError, OwnedJobOperation, ReadableJobOperation, internal};
use super::job_routes::{FamilyRouteOperation, refused};
use super::lifecycle::alias::AliasFamilyOperation;
use super::lifecycle::family_reads::SessionJobOperation;
use super::lifecycle::ids::session_of;
use super::records::pages::ServingNode;
use super::route::{JobRouteError, JobRouteOperation, JobRouteOutcome};
use super::store::JobMutationError;
use super::store::row_calls::{EntryPage, list_job_entries};
use super::{REPORT_MAX_ROWS, SESSION_REPORT_ROWS};

/// What a report read found.
#[derive(Debug, PartialEq)]
pub enum JobReportLookup {
    NotFound,
    Pending(JobState),
    CursorConflict,
    Ready {
        job: JobReportView,
        rows: Vec<(Vec<u8>, Value)>,
        next_key: Option<Vec<u8>>,
    },
}

/// Which page of a report to read, and the report it must still belong to.
#[derive(Clone, Debug, PartialEq)]
pub struct ReportQuery {
    pub expected_digest: Option<[u8; 32]>,
    pub last_key: Option<Vec<u8>>,
    pub limit: usize,
}

#[derive(Debug, PartialEq)]
enum ReportReader {
    Owned(UserId),
    Visible(Box<AuthContext>, ServingNode),
}

/// Reads one report page of a job the caller owns, or of any job it may read.
#[derive(Debug, PartialEq)]
pub struct JobReportOperation {
    reader: ReportReader,
    job_id: JobId,
    query: ReportQuery,
    now_ms: u64,
    view: Option<JobReportView>,
}

type ReportNext = Result<Next<JobReportLookup>, JobReadError>;

impl JobReportOperation {
    pub fn owned(user_id: UserId, job_id: JobId, query: ReportQuery, now_ms: u64) -> Self {
        Self::new(ReportReader::Owned(user_id), job_id, query, now_ms)
    }

    pub fn visible(
        auth: &AuthContext,
        job_id: JobId,
        query: ReportQuery,
        serving: ServingNode,
        now_ms: u64,
    ) -> Self {
        let reader = ReportReader::Visible(Box::new(auth.clone()), serving);
        Self::new(reader, job_id, query, now_ms)
    }

    fn new(reader: ReportReader, job_id: JobId, query: ReportQuery, now_ms: u64) -> Self {
        Self {
            reader,
            job_id,
            query,
            now_ms,
            view: None,
        }
    }

    fn found(&mut self, record: Option<JobRecord>) -> ReportNext {
        let Some(record) = record else {
            return Ok(Next::Done(JobReportLookup::NotFound));
        };
        let key_limit = match &record.payload {
            JobPayload::ImportRoCrate(spec) => spec.limits.key_bytes,
            JobPayload::ExportRoCrate(spec) => spec.limits.key_bytes,
            JobPayload::Execution(spec) if session_of(spec).is_some() => 64,
            _ => return Ok(Next::Done(JobReportLookup::NotFound)),
        };
        let too_long = |key: &Vec<u8>| u64::try_from(key.len()).unwrap_or(u64::MAX) > key_limit;
        if self.query.last_key.as_ref().is_some_and(too_long) {
            return Ok(Next::Done(JobReportLookup::CursorConflict));
        }
        if !record.state.is_terminal() {
            return Ok(Next::Done(JobReportLookup::Pending(record.state)));
        }
        let finished_at_ms = record.finished_at_ms.unwrap_or(record.updated_at_ms);
        if finished_at_ms.saturating_add(record.retention_ms) <= self.now_ms {
            return Ok(Next::Done(JobReportLookup::NotFound));
        }
        let report_digest = record
            .report_digest
            .ok_or_else(|| JobReadError("terminal job is missing its report digest".to_string()))?;
        if self
            .query
            .expected_digest
            .is_some_and(|expected| expected != report_digest)
        {
            return Ok(Next::Done(JobReportLookup::CursorConflict));
        }
        self.view = Some(JobReportView {
            job_id: record.job_id,
            created_by: record.created_by,
            kind: JobKind::from(&record.payload),
            report_digest,
        });
        let limit = self.query.limit.clamp(1, usize::from(REPORT_MAX_ROWS));
        let last_key = self.query.last_key.take();
        let entries = list_job_entries(self.job_id, last_key, limit);
        Ok(Next::Call(IoCall::child(entries)))
    }
}

impl Operation for JobReportOperation {
    type Output = JobReportLookup;
    type Error = JobReadError;

    fn first(&mut self) -> ReportNext {
        Ok(Next::Call(match &self.reader {
            ReportReader::Owned(user_id) => {
                IoCall::child(OwnedJobOperation::new(*user_id, self.job_id))
            }
            ReportReader::Visible(auth, serving) => {
                IoCall::child(ReadableJobOperation::new(auth, self.job_id, *serving))
            }
        }))
    }

    fn next(&mut self, result: IoResult) -> ReportNext {
        let Some(job) = self.view.take() else {
            let record = result.child::<Option<JobRecord>, JobReadError>()??;
            return self.found(record);
        };
        let (rows, next_key) = result.child::<EntryPage, JobMutationError>()??;
        Ok(Next::Done(JobReportLookup::Ready {
            job,
            rows,
            next_key,
        }))
    }
}

/// Why a finished session stopped, from the report row its workflow wrote. Self-scoped like
/// every other job read, and node local: session routes are served by the node that ran it.
#[derive(Debug, PartialEq)]
pub struct SessionReasonOperation {
    user_id: UserId,
    job_id: JobId,
    reading: bool,
}

impl SessionReasonOperation {
    pub fn new(user_id: UserId, job_id: JobId) -> Self {
        Self {
            user_id,
            job_id,
            reading: false,
        }
    }
}

fn end_reason(rows: &[(Vec<u8>, Value)]) -> Option<String> {
    rows.iter().rev().find_map(|(_, value)| {
        let row: SessionReportRow = postcard::from_bytes(value).ok()?;
        match row.detail {
            SessionReportDetail::End { reason } => Some(reason),
            _ => None,
        }
    })
}

impl Operation for SessionReasonOperation {
    type Output = Option<String>;
    type Error = JobReadError;

    fn first(&mut self) -> Result<Next<Option<String>>, JobReadError> {
        let read = OwnedJobOperation::new(self.user_id, self.job_id);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<String>>, JobReadError> {
        if self.reading {
            let (rows, _) = result.child::<EntryPage, JobMutationError>()??;
            return Ok(Next::Done(end_reason(&rows)));
        }
        let record = result.child::<Option<JobRecord>, JobReadError>()??;
        if !record.is_some_and(|record| record.state.is_terminal()) {
            return Ok(Next::Done(None));
        }
        self.reading = true;
        let entries = list_job_entries(self.job_id, None, SESSION_REPORT_ROWS);
        Ok(Next::Call(IoCall::child(entries)))
    }
}

fn report_job_matches(
    job: &JobReportView,
    user_id: UserId,
    job_id: JobId,
    expected_digest: Option<[u8; 32]>,
) -> bool {
    job.job_id == job_id
        && job.created_by == user_id
        && (job.kind.is_report() || job.kind == JobKind::Execution)
        && expected_digest.is_none_or(|digest| digest == job.report_digest)
}

#[derive(Debug, PartialEq)]
enum RouteStep {
    Alias,
    Session,
    Route,
    Local,
}

/// A report page from the node that answers for the job. A session's alias reads the report of
/// its physical job, which exists once the session has one.
#[derive(Debug, PartialEq)]
pub struct ReportRouteOperation {
    auth: AuthContext,
    job_id: JobId,
    query: ReportQuery,
    token: Option<AuthToken>,
    serving: ServingNode,
    now_ms: u64,
    step: RouteStep,
}

type RouteNext = Result<Next<JobReportLookup>, JobRouteError>;

impl ReportRouteOperation {
    pub fn new(
        auth: &AuthContext,
        job_id: JobId,
        query: ReportQuery,
        token: Option<AuthToken>,
        serving: ServingNode,
        now_ms: u64,
    ) -> Self {
        Self {
            auth: auth.clone(),
            job_id,
            query,
            token,
            serving,
            now_ms,
            step: RouteStep::Alias,
        }
    }

    fn local(&mut self) -> RouteNext {
        self.step = RouteStep::Local;
        let read = JobReportOperation::visible(
            &self.auth,
            self.job_id,
            self.query.clone(),
            self.serving,
            self.now_ms,
        );
        Ok(Next::Call(IoCall::child(read)))
    }

    fn route(&mut self) -> RouteNext {
        let Some(node) = self.serving else {
            return self.local();
        };
        let limit = self.query.limit.min(usize::from(REPORT_MAX_ROWS));
        let limit = u16::try_from(limit).map_err(internal)?;
        let (job_id, query) = (self.job_id, &self.query);
        let request = self.token.take().map(|auth_token| JobRequest::Report {
            auth_token,
            job_id,
            expected_digest: query.expected_digest,
            last_key: query.last_key.clone(),
            limit,
        });
        self.step = RouteStep::Route;
        let route = FamilyRouteOperation::new(job_id, request, node, self.now_ms);
        Ok(Next::Call(IoCall::child(route)))
    }

    fn answered(&self, response: JobResponse) -> RouteNext {
        let lookup = match response {
            JobResponse::ReportPending(state) => JobReportLookup::Pending(state),
            JobResponse::ReportConflict => JobReportLookup::CursorConflict,
            JobResponse::ReportReady {
                job,
                rows,
                next_key,
            } if report_job_matches(
                &job,
                self.auth.user_id,
                self.job_id,
                self.query.expected_digest,
            ) =>
            {
                JobReportLookup::Ready {
                    job,
                    rows: rows
                        .into_iter()
                        .map(|(key, value)| (key, Value::from(value)))
                        .collect(),
                    next_key,
                }
            }
            JobResponse::NotFound => JobReportLookup::NotFound,
            response => return Err(refused(response, "report")),
        };
        Ok(Next::Done(lookup))
    }
}

impl Operation for ReportRouteOperation {
    type Output = JobReportLookup;
    type Error = JobRouteError;

    fn first(&mut self) -> RouteNext {
        let alias = AliasFamilyOperation::new(self.job_id);
        Ok(Next::Call(IoCall::child(alias)))
    }

    fn next(&mut self, result: IoResult) -> RouteNext {
        match self.step {
            RouteStep::Alias => {
                if result
                    .child::<Option<JobFamilyId>, JobRouteError>()??
                    .is_none()
                {
                    return self.route();
                }
                self.step = RouteStep::Session;
                let session = SessionJobOperation::new(self.auth.user_id, self.job_id, self.now_ms);
                Ok(Next::Call(IoCall::child(session)))
            }
            RouteStep::Session => {
                match result.child::<(JobRecord, Option<JobId>), JobRouteError>()? {
                    Ok((_, Some(physical))) => {
                        self.job_id = physical;
                        self.route()
                    }
                    Ok((record, None)) => Ok(Next::Done(JobReportLookup::Pending(record.state))),
                    Err(JobRouteError::NotFound) => Ok(Next::Done(JobReportLookup::NotFound)),
                    Err(error) => Err(error),
                }
            }
            RouteStep::Route => match result.child::<JobRouteOutcome, JobRouteError>()?? {
                JobRouteOutcome::Local => self.local(),
                JobRouteOutcome::Remote(response) => self.answered(response),
            },
            RouteStep::Local => {
                let lookup = result
                    .child::<JobReportLookup, JobReadError>()?
                    .map_err(internal)?;
                Ok(Next::Done(lookup))
            }
        }
    }

    fn expected_error(error: &JobRouteError) -> bool {
        JobRouteOperation::expected_error(error)
    }
}

#[cfg(test)]
#[path = "job_reports_tests.rs"]
mod tests;
