//! Routes job record and status reads to the node that answers for the job.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::jobs::{JobRequest, JobResponse, JobStatusView};
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::JobId;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;

use super::job_reads::{
    JobReadError, LocalStatusOperation, RecordData, RecordDataOperation, internal,
};
use super::lifecycle::family_reads::{FamilyResponderOperation, FamilyStatusOperation};
use super::records::pages::ServingNode;
use super::route::{JobRouteError, JobRouteOperation, JobRouteOutcome, RoutedJobStatus};

/// Routes a request to the family's responder, or to the owner its id names.
#[derive(Debug, PartialEq)]
pub(crate) struct FamilyRouteOperation {
    job_id: JobId,
    request: Option<JobRequest>,
    node: (NodeId, RealmId),
    now_ms: u64,
    routing: bool,
}

impl FamilyRouteOperation {
    pub(crate) fn new(
        job_id: JobId,
        request: Option<JobRequest>,
        node: (NodeId, RealmId),
        now_ms: u64,
    ) -> Self {
        Self {
            job_id,
            request,
            node,
            now_ms,
            routing: false,
        }
    }
}

impl Operation for FamilyRouteOperation {
    type Output = JobRouteOutcome;
    type Error = JobRouteError;

    fn first(&mut self) -> Result<Next<JobRouteOutcome>, JobRouteError> {
        let responder = FamilyResponderOperation::new(self.job_id, self.now_ms);
        Ok(Next::Call(IoCall::child(responder)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<JobRouteOutcome>, JobRouteError> {
        if self.routing {
            return Ok(Next::Done(
                result.child::<JobRouteOutcome, JobRouteError>()??,
            ));
        }
        let responder = result.child::<Option<NodeId>, JobRouteError>()??;
        self.routing = true;
        let (local, realm_id) = self.node;
        let route = JobRouteOperation::new(realm_id, local, self.job_id, self.request.take())
            .with_responder(responder);
        Ok(Next::Call(IoCall::child(route)))
    }

    fn expected_error(error: &JobRouteError) -> bool {
        JobRouteOperation::expected_error(error)
    }
}

/// The answer every routed read gives for a response that is no verdict on the job itself.
pub(super) fn refused(response: JobResponse, what: &str) -> JobRouteError {
    match response {
        JobResponse::Unauthorized => JobRouteError::Unauthorized,
        JobResponse::Forbidden => JobRouteError::Forbidden,
        JobResponse::Unavailable(error) => JobRouteError::Unavailable(error),
        response => JobRouteError::Unavailable(format!(
            "unexpected {what} response from the job owner: {response:?}"
        )),
    }
}

#[derive(Debug, PartialEq)]
enum RecordStep {
    Route,
    Local,
}

/// The full owner record for projections the status view cannot rebuild elsewhere. An
/// unreachable owner is `Unavailable`; only the owner answers absence.
#[derive(Debug, PartialEq)]
pub struct RecordRouteOperation {
    user_id: UserId,
    job_id: JobId,
    token: Option<AuthToken>,
    serving: ServingNode,
    now_ms: u64,
    step: RecordStep,
}

type RecordNext = Result<Next<Option<RecordData>>, JobRouteError>;

impl RecordRouteOperation {
    pub fn new(
        user_id: UserId,
        job_id: JobId,
        token: Option<AuthToken>,
        serving: ServingNode,
        now_ms: u64,
    ) -> Self {
        Self {
            user_id,
            job_id,
            token,
            serving,
            now_ms,
            step: RecordStep::Route,
        }
    }

    fn local(&mut self) -> RecordNext {
        self.step = RecordStep::Local;
        let read = RecordDataOperation::new(self.user_id, self.job_id);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn answered(&self, response: JobResponse) -> RecordNext {
        match response {
            JobResponse::Record { record, checkpoint }
                if record.job_id == self.job_id
                    && record.created_by == self.user_id
                    && !record.payload.is_internal() =>
            {
                Ok(Next::Done(Some((*record, checkpoint))))
            }
            JobResponse::Record { .. } => Err(JobRouteError::NotFound),
            JobResponse::NotFound => Ok(Next::Done(None)),
            response => Err(refused(response, "record")),
        }
    }
}

impl Operation for RecordRouteOperation {
    type Output = Option<RecordData>;
    type Error = JobRouteError;

    fn first(&mut self) -> RecordNext {
        let Some(node) = self.serving else {
            return self.local();
        };
        let job_id = self.job_id;
        let request = self
            .token
            .take()
            .map(|auth_token| JobRequest::Record { auth_token, job_id });
        let route = FamilyRouteOperation::new(job_id, request, node, self.now_ms);
        Ok(Next::Call(IoCall::child(route)))
    }

    fn next(&mut self, result: IoResult) -> RecordNext {
        match self.step {
            RecordStep::Route => match result.child::<JobRouteOutcome, JobRouteError>()?? {
                JobRouteOutcome::Local => self.local(),
                JobRouteOutcome::Remote(response) => self.answered(response),
            },
            RecordStep::Local => {
                let data = result
                    .child::<Option<RecordData>, JobReadError>()?
                    .map_err(internal)?;
                Ok(Next::Done(data))
            }
        }
    }

    fn expected_error(error: &JobRouteError) -> bool {
        JobRouteOperation::expected_error(error)
    }
}

pub(super) fn routed_job_matches(job: &JobStatusView, user_id: UserId, job_id: JobId) -> bool {
    job.job_id == job_id && job.created_by == user_id && !job.kind.is_internal()
}

#[derive(Debug, PartialEq)]
enum StatusStep {
    Family,
    Route,
    Local,
}

/// A job's status. An external job is answered from its family projection, which any node that
/// reduced the family can do; every other job is answered by its owner.
#[derive(Debug, PartialEq)]
pub struct StatusRouteOperation {
    auth: AuthContext,
    job_id: JobId,
    token: Option<AuthToken>,
    serving: ServingNode,
    now_ms: u64,
    step: StatusStep,
}

type StatusNext = Result<Next<RoutedJobStatus>, JobRouteError>;

impl StatusRouteOperation {
    pub fn new(
        auth: &AuthContext,
        job_id: JobId,
        token: Option<AuthToken>,
        serving: ServingNode,
        now_ms: u64,
    ) -> Self {
        Self {
            auth: auth.clone(),
            job_id,
            token,
            serving,
            now_ms,
            step: StatusStep::Family,
        }
    }

    fn local(&mut self) -> StatusNext {
        self.step = StatusStep::Local;
        let read = LocalStatusOperation::new(&self.auth, self.job_id, self.serving);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn route(&mut self) -> StatusNext {
        let Some((local, realm_id)) = self.serving else {
            return self.local();
        };
        let job_id = self.job_id;
        let request = self
            .token
            .take()
            .map(|auth_token| JobRequest::Status { auth_token, job_id });
        self.step = StatusStep::Route;
        let route = JobRouteOperation::new(realm_id, local, job_id, request);
        Ok(Next::Call(IoCall::child(route)))
    }

    fn answered(&self, response: JobResponse) -> StatusNext {
        match response {
            JobResponse::Status { job, run_crate }
                if routed_job_matches(&job, self.auth.user_id, self.job_id) =>
            {
                let run_crate = run_crate
                    .map(|value| serde_json::from_str(&value))
                    .transpose()
                    .map_err(internal)?;
                Ok(Next::Done(RoutedJobStatus { job, run_crate }))
            }
            JobResponse::NotFound => Err(JobRouteError::NotFound),
            response => Err(refused(response, "status")),
        }
    }
}

impl Operation for StatusRouteOperation {
    type Output = RoutedJobStatus;
    type Error = JobRouteError;

    fn first(&mut self) -> StatusNext {
        let read = FamilyStatusOperation::new(&self.auth, self.job_id, self.now_ms);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> StatusNext {
        match self.step {
            StatusStep::Family => {
                match result.child::<Option<RoutedJobStatus>, JobRouteError>()?? {
                    Some(status) => Ok(Next::Done(status)),
                    None => self.route(),
                }
            }
            StatusStep::Route => match result.child::<JobRouteOutcome, JobRouteError>()?? {
                JobRouteOutcome::Local => self.local(),
                JobRouteOutcome::Remote(response) => self.answered(response),
            },
            StatusStep::Local => Ok(Next::Done(
                result.child::<RoutedJobStatus, JobRouteError>()??,
            )),
        }
    }

    fn expected_error(error: &JobRouteError) -> bool {
        JobRouteOperation::expected_error(error)
    }
}

#[cfg(test)]
#[path = "job_routes_tests.rs"]
mod tests;
