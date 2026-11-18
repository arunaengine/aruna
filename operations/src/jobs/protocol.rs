//! Answers job-control requests from other nodes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::events::PeerAuthRefusal;
use aruna_core::jobs::{JobRequest, JobResponse, JobStatusView, WireArtifact, WireRange};
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::execution::job::{JobFamilyId, JobId, JobRecord, KeyWait};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use bytes::Bytes;

use super::job_artifacts::{
    ArtifactLookup, ArtifactRangeOperation, ArtifactRead, OwnedArtifact, OwnedArtifactOperation,
};
use super::job_cancel::{
    CancelJobOutcome, CancelOwnedOperation, CancelReport, MatchingRunsOperation,
};
use super::job_reads::{
    JobOwnerOperation, JobReadError, LocalStatusOperation, RecordData, RecordDataOperation,
};
use super::job_reports::{JobReportLookup, JobReportOperation, ReportQuery};
use super::key_locks::KeyCheckError;
use super::lifecycle::alias::AliasFamilyOperation;
use super::lifecycle::cancel::CancelFamilyOperation;
use super::lifecycle::family_reads::FamilyStatusOperation;
use super::records::pages::ServingNode;
use super::records::verify::FamilyView;
use super::remote_key::{AcceptWakeOperation, RegisterWaitsOperation};
use super::route::{JobRouteError, RoutedJobStatus};
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::metadata::write_peer::WritePeerOperation;
use crate::realm::load_config::config_unavailable;

/// What a job-control request is answered with. `running` names the local attempts the
/// receiving node must signal, because a cancel reached them.
#[derive(Debug, PartialEq)]
pub struct JobAnswer {
    pub response: JobResponse,
    pub body: Option<BackendStream<Result<Bytes, StreamError>>>,
    pub running: Vec<JobId>,
}

impl JobAnswer {
    pub fn new(response: JobResponse) -> Self {
        Self {
            response,
            body: None,
            running: Vec::new(),
        }
    }

    pub fn unavailable(reason: impl std::fmt::Display) -> Self {
        Self::new(JobResponse::Unavailable(reason.to_string()))
    }
}

type AnswerNext = Result<Next<JobAnswer>, IoError>;

fn answer(response: JobResponse) -> AnswerNext {
    Ok(Next::Done(JobAnswer::new(response)))
}

fn refuse(reason: impl std::fmt::Display) -> AnswerNext {
    Ok(Next::Done(JobAnswer::unavailable(reason)))
}

/// Opens the requested artifact range, which a plain lookup leaves closed.
#[derive(Debug, PartialEq)]
pub struct ServeArtifactOperation {
    user_id: UserId,
    job_id: JobId,
    now_ms: u64,
    range: Option<WireRange>,
    ready: Option<WireArtifact>,
}

impl ServeArtifactOperation {
    pub fn new(user_id: UserId, job_id: JobId, now_ms: u64, range: Option<WireRange>) -> Self {
        Self {
            user_id,
            job_id,
            now_ms,
            range,
            ready: None,
        }
    }

    fn found(&mut self, owned: OwnedArtifact) -> AnswerNext {
        let wire = WireArtifact::from(&owned);
        let Some(range) = self.range.as_ref() else {
            return answer(JobResponse::ArtifactReady {
                owned: wire,
                stream_size: 0,
            });
        };
        if range.start >= range.end || range.end > owned.size {
            return refuse("invalid job artifact range");
        }
        let Some(artifact) = owned.source() else {
            return refuse("local artifact source is unavailable");
        };
        let read = ArtifactRangeOperation::new(artifact, range.start..range.end);
        self.ready = Some(wire);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn opened(&self, wire: WireArtifact, read: ArtifactRead) -> AnswerNext {
        let expected = self.range.as_ref().map(|range| range.end - range.start);
        if Some(read.stream_size) != expected {
            return refuse("artifact reader returned an unexpected range size");
        }
        Ok(Next::Done(JobAnswer {
            response: JobResponse::ArtifactReady {
                owned: wire,
                stream_size: read.stream_size,
            },
            body: Some(read.blob),
            running: Vec::new(),
        }))
    }
}

impl Operation for ServeArtifactOperation {
    type Output = JobAnswer;
    type Error = IoError;

    fn first(&mut self) -> AnswerNext {
        let read = OwnedArtifactOperation::new(self.user_id, self.job_id, self.now_ms);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> AnswerNext {
        if let Some(wire) = self.ready.take() {
            return match result.child::<ArtifactRead, JobReadError>()? {
                Ok(read) => self.opened(wire, read),
                Err(error) => refuse(error),
            };
        }
        match result.child::<ArtifactLookup, JobReadError>()? {
            Ok(ArtifactLookup::NotFound) => answer(JobResponse::NotFound),
            Ok(ArtifactLookup::Pending(state)) => answer(JobResponse::ArtifactPending(state)),
            Ok(ArtifactLookup::Gone) => answer(JobResponse::ArtifactGone),
            Ok(ArtifactLookup::Ready(owned)) => self.found(owned),
            Err(error) => refuse(error),
        }
    }
}

#[derive(Debug, PartialEq)]
enum CancelStep {
    Alias,
    Config,
    Publish,
    Runs,
    Stop,
    Status,
    Local,
}

/// Serves a cancel that reached this node. A family holder publishes the record and stops its
/// own executions. Any other node answers only for its executions and never forwards again,
/// since divergent holder views would bounce the cancel.
#[derive(Debug, PartialEq)]
pub struct ServeCancelOperation {
    auth: AuthContext,
    token: AuthToken,
    job_id: JobId,
    serving: ServingNode,
    now_ms: u64,
    step: CancelStep,
    family: Option<JobFamilyId>,
    holds: bool,
    /// Physical jobs still to stop, the next one last.
    pending: Vec<JobId>,
    stopped: Option<(Box<JobRecord>, bool)>,
    running: Vec<JobId>,
}

fn route_refusal(error: JobRouteError) -> JobResponse {
    match error {
        JobRouteError::Forbidden => JobResponse::Forbidden,
        JobRouteError::Unauthorized => JobResponse::Unauthorized,
        JobRouteError::NotFound => JobResponse::NotFound,
        error => JobResponse::Unavailable(error.to_string()),
    }
}

impl ServeCancelOperation {
    pub fn new(
        auth: &AuthContext,
        token: AuthToken,
        job_id: JobId,
        serving: ServingNode,
        now_ms: u64,
    ) -> Self {
        Self {
            auth: auth.clone(),
            token,
            job_id,
            serving,
            now_ms,
            step: CancelStep::Alias,
            family: None,
            holds: false,
            pending: Vec::new(),
            stopped: None,
            running: Vec::new(),
        }
    }

    fn done(&mut self, response: JobResponse) -> AnswerNext {
        Ok(Next::Done(JobAnswer {
            response,
            body: None,
            running: std::mem::take(&mut self.running),
        }))
    }

    fn matching_runs(&mut self) -> AnswerNext {
        self.step = CancelStep::Runs;
        let runs = MatchingRunsOperation::new(self.job_id, self.family);
        Ok(Next::Call(IoCall::child(runs)))
    }

    fn cancel_local(&mut self, job_id: JobId, step: CancelStep) -> AnswerNext {
        self.step = step;
        let cancel = CancelOwnedOperation::new(self.auth.user_id, job_id, self.now_ms);
        Ok(Next::Call(IoCall::child(cancel)))
    }

    fn configured(&mut self, config: &RealmConfigDocument) -> AnswerNext {
        let (Some((local, realm_id)), Some(family)) = (self.serving, self.family) else {
            return self.done(JobResponse::Unavailable(
                JobRouteError::Unavailable("network handle unavailable".to_string()).to_string(),
            ));
        };
        let Some(view) = FamilyView::resolve(config, realm_id, family) else {
            let error = JobRouteError::Unavailable("job family holder view unavailable".into());
            return self.done(JobResponse::Unavailable(error.to_string()));
        };
        self.holds = view.holds(local);
        if !self.holds {
            return self.matching_runs();
        }
        self.step = CancelStep::Publish;
        let cancel = CancelFamilyOperation::new(
            &self.auth,
            self.job_id,
            Some(self.token.clone()),
            self.serving,
            self.now_ms,
        );
        Ok(Next::Call(IoCall::child(cancel)))
    }

    fn stop_next(&mut self) -> AnswerNext {
        if let Some(job_id) = self.pending.pop() {
            return self.cancel_local(job_id, CancelStep::Stop);
        }
        if self.holds {
            self.step = CancelStep::Status;
            let status = FamilyStatusOperation::new(&self.auth, self.job_id, self.now_ms);
            return Ok(Next::Call(IoCall::child(status)));
        }
        if let Some((record, terminal)) = self.stopped.take() {
            let mut job = JobStatusView::from(&*record);
            job.job_id = self.job_id;
            return self.done(JobResponse::Cancelled { job, terminal });
        }
        if self.family.is_some() {
            // Availability, never a verdict: the caller keeps asking the holders its own view
            // names instead of this node forwarding a second hop.
            return self.done(JobResponse::Unavailable(
                "this node does not hold the job family".to_string(),
            ));
        }
        self.cancel_local(self.job_id, CancelStep::Local)
    }

    fn stopped_run(&mut self, result: IoResult) -> AnswerNext {
        let report = match result.child::<CancelReport<CancelJobOutcome>, JobReadError>()? {
            Ok(report) => report,
            Err(error) => return self.done(JobResponse::Unavailable(error.0)),
        };
        self.running.extend(report.running);
        match report.outcome {
            CancelJobOutcome::AlreadyTerminal(record) => {
                self.stopped = Some((Box::new(record), true));
            }
            CancelJobOutcome::Requested(record) => {
                self.stopped = Some((Box::new(record), false));
            }
            CancelJobOutcome::NotFound => {}
        }
        self.stop_next()
    }

    fn local_answer(&mut self, result: IoResult) -> AnswerNext {
        let report = match result.child::<CancelReport<CancelJobOutcome>, JobReadError>()? {
            Ok(report) => report,
            Err(error) => return self.done(JobResponse::Unavailable(error.0)),
        };
        self.running.extend(report.running);
        self.done(match report.outcome {
            CancelJobOutcome::NotFound => JobResponse::NotFound,
            CancelJobOutcome::AlreadyTerminal(record) => JobResponse::Cancelled {
                job: JobStatusView::from(&record),
                terminal: true,
            },
            CancelJobOutcome::Requested(record) => JobResponse::Cancelled {
                job: JobStatusView::from(&record),
                terminal: false,
            },
        })
    }
}

impl Operation for ServeCancelOperation {
    type Output = JobAnswer;
    type Error = IoError;

    fn first(&mut self) -> AnswerNext {
        let alias = AliasFamilyOperation::new(self.job_id);
        Ok(Next::Call(IoCall::child(alias)))
    }

    fn next(&mut self, result: IoResult) -> AnswerNext {
        match self.step {
            CancelStep::Alias => match result.child::<Option<JobFamilyId>, JobRouteError>()? {
                Ok(None) => self.matching_runs(),
                Ok(Some(family)) => {
                    let Some((_, realm_id)) = self.serving else {
                        let error = JobRouteError::Unavailable("network handle unavailable".into());
                        return self.done(JobResponse::Unavailable(error.to_string()));
                    };
                    self.family = Some(family);
                    self.step = CancelStep::Config;
                    Ok(Next::Call(IoCall::child(GetConfigOperation::new(realm_id))))
                }
                Err(error) => self.done(JobResponse::Unavailable(error.to_string())),
            },
            CancelStep::Config => match result.child::<RealmConfigDocument, GetConfigError>()? {
                Ok(config) => self.configured(&config),
                Err(error) => {
                    config_unavailable(&error);
                    let error = JobRouteError::Unavailable("realm config unavailable".into());
                    self.done(JobResponse::Unavailable(error.to_string()))
                }
            },
            CancelStep::Publish => match result.child::<Option<()>, JobRouteError>()? {
                Ok(Some(())) => self.matching_runs(),
                Ok(None) => self.cancel_local(self.job_id, CancelStep::Local),
                Err(error) => self.done(route_refusal(error)),
            },
            CancelStep::Runs => match result.child::<Vec<JobId>, JobRouteError>()? {
                Ok(mut runs) => {
                    runs.reverse();
                    self.pending = runs;
                    self.stop_next()
                }
                Err(error) => self.done(JobResponse::Unavailable(error.to_string())),
            },
            CancelStep::Stop => self.stopped_run(result),
            CancelStep::Status => {
                match result.child::<Option<RoutedJobStatus>, JobRouteError>()? {
                    Ok(Some(status)) => self.done(JobResponse::Cancelled {
                        terminal: status.job.state.is_terminal(),
                        job: status.job,
                    }),
                    Ok(None) => self.done(JobResponse::NotFound),
                    Err(error) => self.done(route_refusal(error)),
                }
            }
            CancelStep::Local => self.local_answer(result),
        }
    }
}

/// A user of another realm signed in here matches; an anonymous identity only in its own realm.
fn auth_realm_matches(auth: &AuthContext, realm_id: RealmId) -> bool {
    auth.realm_id == realm_id && (auth.user_id.realm_id == realm_id || !auth.user_id.is_nil())
}

fn status_answer(result: Result<RoutedJobStatus, JobRouteError>) -> JobResponse {
    match result {
        Ok(status) => JobResponse::Status {
            job: status.job,
            run_crate: status.run_crate.map(|value| value.to_string()),
        },
        Err(JobRouteError::NotFound) => JobResponse::NotFound,
        Err(error) => JobResponse::Unavailable(error.to_string()),
    }
}

fn report_answer(result: Result<JobReportLookup, JobReadError>) -> JobResponse {
    match result {
        Ok(JobReportLookup::NotFound) => JobResponse::NotFound,
        Ok(JobReportLookup::Pending(state)) => JobResponse::ReportPending(state),
        Ok(JobReportLookup::CursorConflict) => JobResponse::ReportConflict,
        Ok(JobReportLookup::Ready {
            job,
            rows,
            next_key,
        }) => JobResponse::ReportReady {
            job,
            rows: rows
                .into_iter()
                .map(|(key, value)| (key, value.as_ref().to_vec()))
                .collect(),
            next_key,
        },
        Err(error) => JobResponse::Unavailable(error.0),
    }
}

fn record_answer(result: Result<Option<RecordData>, JobReadError>) -> JobResponse {
    match result {
        Ok(Some((record, checkpoint))) => JobResponse::Record {
            record: Box::new(record),
            checkpoint,
        },
        Ok(None) => JobResponse::NotFound,
        Err(error) => JobResponse::Unavailable(error.0),
    }
}

fn waits_answer(result: Result<Vec<KeyWait>, KeyCheckError>) -> JobResponse {
    match result {
        Ok(locked) => JobResponse::KeysLocked(locked),
        Err(error) => JobResponse::Unavailable(error.0),
    }
}

fn wake_answer(result: Result<bool, KeyCheckError>) -> JobResponse {
    match result {
        Ok(true) => JobResponse::KeyWakeAcked,
        Ok(false) => JobResponse::Unavailable("job is still parking".to_string()),
        Err(error) => JobResponse::Unavailable(error.0),
    }
}

#[derive(Debug, PartialEq)]
enum RequestStep {
    Auth,
    Alias,
    Owner,
    Status,
    Report,
    Served,
    Record,
    Waits,
    Wake,
}

/// Authenticates one job-control request and answers it. Owner-directed requests are answered
/// by the immutable owner or a node that knows the job's family; key waits address the node
/// that holds the content or the job instead. Every refusal is an answer to the peer.
#[derive(Debug, PartialEq)]
pub struct JobRequestOperation {
    peer: NodeId,
    request: Option<JobRequest>,
    serving: ServingNode,
    now_ms: u64,
    auth: Option<AuthContext>,
    step: RequestStep,
}

impl JobRequestOperation {
    pub fn new(peer: NodeId, request: JobRequest, serving: ServingNode, now_ms: u64) -> Self {
        Self {
            peer,
            request: Some(request),
            serving,
            now_ms,
            auth: None,
            step: RequestStep::Auth,
        }
    }

    fn job_id(&self) -> Option<JobId> {
        match self.request.as_ref()? {
            JobRequest::Status { job_id, .. }
            | JobRequest::Report { job_id, .. }
            | JobRequest::Artifact { job_id, .. }
            | JobRequest::Cancel { job_id, .. }
            | JobRequest::Record { job_id, .. } => Some(*job_id),
            JobRequest::AwaitKeys { .. } | JobRequest::KeyWake { .. } => None,
        }
    }

    fn authenticated(&mut self, auth: AuthContext) -> AnswerNext {
        let Some((_, realm_id)) = self.serving else {
            return refuse("job-control network handle unavailable");
        };
        if !auth_realm_matches(&auth, realm_id) {
            return answer(JobResponse::Forbidden);
        }
        self.auth = Some(auth);
        match self.job_id() {
            Some(job_id) => {
                self.step = RequestStep::Alias;
                let alias = AliasFamilyOperation::new(job_id);
                Ok(Next::Call(IoCall::child(alias)))
            }
            None => self.serve(),
        }
    }

    fn serve(&mut self) -> AnswerNext {
        let (Some(request), Some(auth), Some((local, _))) =
            (self.request.take(), self.auth.as_ref(), self.serving)
        else {
            return refuse("job-control network handle unavailable");
        };
        let serving = self.serving;
        let now_ms = self.now_ms;
        let (step, call) = match request {
            JobRequest::Status { job_id, .. } => (
                RequestStep::Status,
                IoCall::child(LocalStatusOperation::new(auth, job_id, serving)),
            ),
            JobRequest::Report {
                job_id,
                expected_digest,
                last_key,
                limit,
                ..
            } => {
                let query = ReportQuery {
                    expected_digest,
                    last_key,
                    limit: usize::from(limit),
                };
                let read = JobReportOperation::visible(auth, job_id, query, serving, now_ms);
                (RequestStep::Report, IoCall::child(read))
            }
            JobRequest::Artifact { job_id, range, .. } => {
                let serve = ServeArtifactOperation::new(auth.user_id, job_id, now_ms, range);
                (RequestStep::Served, IoCall::child(serve))
            }
            JobRequest::Cancel {
                job_id, auth_token, ..
            } => {
                let serve = ServeCancelOperation::new(auth, auth_token, job_id, serving, now_ms);
                (RequestStep::Served, IoCall::child(serve))
            }
            JobRequest::Record { job_id, .. } => (
                RequestStep::Record,
                IoCall::child(RecordDataOperation::new(auth.user_id, job_id)),
            ),
            JobRequest::AwaitKeys {
                job_id, targets, ..
            } => {
                let nodes = (local, self.peer);
                let register = RegisterWaitsOperation::new(nodes, auth, job_id, &targets);
                (RequestStep::Waits, IoCall::child(register))
            }
            JobRequest::KeyWake { job_id, key, .. } => {
                let accept = AcceptWakeOperation::new(self.peer, auth.user_id, job_id, key, now_ms);
                (RequestStep::Wake, IoCall::child(accept))
            }
        };
        self.step = step;
        Ok(Next::Call(call))
    }

    fn owner_known(&mut self, owner: Result<NodeId, JobRouteError>) -> AnswerNext {
        match owner {
            Ok(owner) if self.serving.map(|(local, _)| local) == Some(owner) => self.serve(),
            Ok(_) => refuse("receiving node does not own this job"),
            Err(JobRouteError::NotFound) => answer(JobResponse::NotFound),
            Err(error) => refuse(error),
        }
    }
}

impl Operation for JobRequestOperation {
    type Output = JobAnswer;
    type Error = IoError;

    fn first(&mut self) -> AnswerNext {
        let Some(request) = self.request.as_ref() else {
            return Err(IoError::Unfinished);
        };
        let check = WritePeerOperation::new(self.peer, request.auth_token());
        Ok(Next::Call(IoCall::child(check)))
    }

    fn next(&mut self, result: IoResult) -> AnswerNext {
        match self.step {
            RequestStep::Auth => match result.child::<AuthContext, PeerAuthRefusal>()? {
                Ok(auth) => self.authenticated(auth),
                Err(PeerAuthRefusal::Unauthorized) => answer(JobResponse::Unauthorized),
                Err(PeerAuthRefusal::Unavailable(reason)) => {
                    answer(JobResponse::Unavailable(reason))
                }
            },
            RequestStep::Alias => match result.child::<Option<JobFamilyId>, JobRouteError>()? {
                Ok(Some(_)) => self.serve(),
                Ok(None) => {
                    let Some(job_id) = self.job_id() else {
                        return Err(IoError::Unfinished);
                    };
                    self.step = RequestStep::Owner;
                    let owner = JobOwnerOperation::new(job_id, self.serving);
                    Ok(Next::Call(IoCall::child(owner)))
                }
                Err(error) => refuse(error),
            },
            RequestStep::Owner => {
                let owner = result.child::<NodeId, JobRouteError>()?;
                self.owner_known(owner)
            }
            RequestStep::Status => answer(status_answer(result.child()?)),
            RequestStep::Report => answer(report_answer(result.child()?)),
            RequestStep::Record => answer(record_answer(result.child()?)),
            RequestStep::Waits => answer(waits_answer(result.child()?)),
            RequestStep::Wake => answer(wake_answer(result.child()?)),
            RequestStep::Served => match result.child::<JobAnswer, IoError>()? {
                Ok(served) => Ok(Next::Done(served)),
                Err(error) => refuse(error),
            },
        }
    }
}

#[cfg(test)]
mod pure_tests {
    use super::*;
    use crate::jobs::job_reads::tests::{auth_of, child, is_child, job_id, node, owner, realm};
    use aruna_core::structs::execution::job::{JobProgress, JobState, WorkspaceMode};
    use ulid::Ulid;

    use aruna_core::jobs::JobKind;

    fn user(realm_id: RealmId) -> UserId {
        UserId::new(Ulid::from_bytes([2u8; 16]), realm_id)
    }

    fn serving() -> ServingNode {
        Some((node(1), realm()))
    }

    fn token() -> AuthToken {
        AuthToken::internal(auth_of(owner()))
    }

    fn status_request() -> JobRequest {
        JobRequest::Status {
            auth_token: token(),
            job_id: job_id(),
        }
    }

    fn authed(request: JobRequest) -> JobRequestOperation {
        let mut serve = JobRequestOperation::new(node(2), request, serving(), 5);
        assert!(is_child(&serve.first().unwrap()));
        assert!(is_child(
            &serve
                .next(child(Ok::<_, PeerAuthRefusal>(auth_of(owner()))))
                .unwrap()
        ));
        serve
    }

    fn done_response(next: AnswerNext) -> JobResponse {
        match next.unwrap() {
            Next::Done(answer) => answer.response,
            other => panic!("unexpected step: {other:?}"),
        }
    }

    // A refused peer gets its verdict, and no network means no answer for it.
    #[test]
    fn peer_refusals_answered() {
        let mut serve = JobRequestOperation::new(node(2), status_request(), serving(), 5);
        let denied = child(Err::<AuthContext, _>(PeerAuthRefusal::Unauthorized));
        assert_eq!(done_response(serve.next(denied)), JobResponse::Unauthorized);
        let mut offline = JobRequestOperation::new(node(2), status_request(), None, 5);
        let accepted = child(Ok::<_, PeerAuthRefusal>(auth_of(owner())));
        assert!(matches!(
            done_response(offline.next(accepted)),
            JobResponse::Unavailable(_)
        ));
    }

    // A known family skips the owner check, and a foreign owner is refused.
    #[test]
    fn owner_gate_applies() {
        let family = JobFamilyId {
            submission_id: aruna_core::structs::execution::job::SubmissionId([7u8; 32]),
            request_digest: [8u8; 32],
        };
        let mut serve = authed(status_request());
        let known = child(Ok::<_, JobRouteError>(Some(family)));
        assert!(is_child(&serve.next(known).unwrap()));
        assert_eq!(serve.step, RequestStep::Status);

        let mut serve = authed(status_request());
        let unknown = child(Ok::<Option<JobFamilyId>, JobRouteError>(None));
        assert!(is_child(&serve.next(unknown).unwrap()));
        assert_eq!(serve.step, RequestStep::Owner);
        let foreign = child(Ok::<_, JobRouteError>(node(9)));
        assert!(matches!(
            done_response(serve.next(foreign)),
            JobResponse::Unavailable(reason) if reason.contains("does not own")
        ));

        let mut serve = authed(status_request());
        serve
            .next(child(Ok::<Option<JobFamilyId>, JobRouteError>(None)))
            .unwrap();
        let invalid = child(Err::<NodeId, _>(JobRouteError::NotFound));
        assert_eq!(done_response(serve.next(invalid)), JobResponse::NotFound);
    }

    // Key waits are answered without the owner check.
    #[test]
    fn key_wake_ungated() {
        let wake = JobRequest::KeyWake {
            auth_token: token(),
            job_id: job_id(),
            key: aruna_core::structs::storage::encryption::BucketKeyRef::new(
                Ulid::from_bytes([8u8; 16]),
                1,
            ),
        };
        let mut serve = authed(wake);
        assert_eq!(serve.step, RequestStep::Wake);
        let parking = child(Ok::<_, KeyCheckError>(false));
        assert!(matches!(
            done_response(serve.next(parking)),
            JobResponse::Unavailable(reason) if reason.contains("parking")
        ));
    }

    fn artifact(range: Option<WireRange>) -> ServeArtifactOperation {
        ServeArtifactOperation::new(owner(), job_id(), 5, range)
    }

    fn ready() -> OwnedArtifact {
        OwnedArtifact::from(WireArtifact {
            job_id: job_id(),
            created_by: owner(),
            blake3: [8u8; 32],
            size: 4,
            filename: "artifact.zip".to_string(),
        })
    }

    // A lookup answers without bytes, and a range outside the artifact is refused.
    #[test]
    fn artifact_ranges_checked() {
        let found = || child(Ok::<_, JobReadError>(ArtifactLookup::Ready(ready())));
        assert!(matches!(
            done_response(artifact(None).next(found())),
            JobResponse::ArtifactReady { stream_size: 0, .. }
        ));
        let beyond = WireRange { start: 2, end: 9 };
        assert!(matches!(
            done_response(artifact(Some(beyond)).next(found())),
            JobResponse::Unavailable(reason) if reason.contains("invalid")
        ));
        let inside = WireRange { start: 1, end: 2 };
        assert!(matches!(
            done_response(artifact(Some(inside)).next(found())),
            JobResponse::Unavailable(reason) if reason.contains("source")
        ));
    }

    fn cancel() -> ServeCancelOperation {
        ServeCancelOperation::new(&auth_of(owner()), token(), job_id(), serving(), 5)
    }

    fn record() -> JobRecord {
        crate::jobs::job_reads::tests::record_of(crate::jobs::job_reads::tests::probe())
    }

    fn report(outcome: CancelJobOutcome, running: Vec<JobId>) -> IoResult {
        child(Ok::<_, JobReadError>(CancelReport { outcome, running }))
    }

    // A non-holder stops its runs and answers under the requested id.
    #[test]
    fn nonholder_stops_runs() {
        let physical = JobId::from_bytes([9u8; 16]);
        let mut serve = cancel();
        assert!(is_child(&serve.first().unwrap()));
        let plain = child(Ok::<Option<JobFamilyId>, JobRouteError>(None));
        assert!(is_child(&serve.next(plain).unwrap()));
        assert_eq!(serve.step, CancelStep::Runs);
        let runs = child(Ok::<_, JobRouteError>(vec![physical]));
        assert!(is_child(&serve.next(runs).unwrap()));
        let stopped = report(CancelJobOutcome::Requested(record()), vec![physical]);
        let Next::Done(answer) = serve.next(stopped).unwrap() else {
            panic!("expected the answer");
        };
        assert_eq!(answer.running, vec![physical]);
        assert!(matches!(
            answer.response,
            JobResponse::Cancelled { job, terminal: false } if job.job_id == job_id()
        ));
    }

    // Without a run or a family the job itself is cancelled here.
    #[test]
    fn plain_job_cancelled() {
        let mut serve = cancel();
        serve.step = CancelStep::Runs;
        let none = child(Ok::<_, JobRouteError>(Vec::<JobId>::new()));
        assert!(is_child(&serve.next(none).unwrap()));
        assert_eq!(serve.step, CancelStep::Local);
        let absent = report(CancelJobOutcome::NotFound, Vec::new());
        assert_eq!(done_response(serve.next(absent)), JobResponse::NotFound);
    }

    // A non-holder of a known family gives availability, never a verdict.
    #[test]
    fn nonholder_family_unavailable() {
        let mut serve = cancel();
        serve.step = CancelStep::Runs;
        serve.family = Some(JobFamilyId {
            submission_id: aruna_core::structs::execution::job::SubmissionId([7u8; 32]),
            request_digest: [8u8; 32],
        });
        let none = child(Ok::<_, JobRouteError>(Vec::<JobId>::new()));
        assert!(matches!(
            done_response(serve.next(none)),
            JobResponse::Unavailable(reason) if reason.contains("does not hold")
        ));
    }

    // A holder answers from the family projection after stopping its runs.
    #[test]
    fn holder_reports_status() {
        let mut serve = cancel();
        serve.holds = true;
        serve.step = CancelStep::Publish;
        let published = child(Ok::<_, JobRouteError>(Some(())));
        assert!(is_child(&serve.next(published).unwrap()));
        let none = child(Ok::<_, JobRouteError>(Vec::<JobId>::new()));
        assert!(is_child(&serve.next(none).unwrap()));
        assert_eq!(serve.step, CancelStep::Status);
        let gone = child(Err::<Option<RoutedJobStatus>, _>(JobRouteError::Forbidden));
        assert_eq!(done_response(serve.next(gone)), JobResponse::Forbidden);
    }

    #[test]
    fn rejects_foreign_realm() {
        let local = RealmId([1u8; 32]);
        let foreign = RealmId([2u8; 32]);
        let mut auth = AuthContext {
            user_id: user(local),
            realm_id: local,
            path_restrictions: None,
            session: None,
        };
        assert!(auth_realm_matches(&auth, local));
        auth.realm_id = foreign;
        assert!(!auth_realm_matches(&auth, local));
        // A user of another realm signed in here matches; an anonymous one does not.
        auth.realm_id = local;
        auth.user_id = user(foreign);
        assert!(auth_realm_matches(&auth, local));
        auth.user_id = UserId::nil(foreign);
        assert!(!auth_realm_matches(&auth, local));
    }

    #[test]
    fn status_wire_roundtrips() {
        let realm_id = RealmId([1u8; 32]);
        let response = JobResponse::Status {
            job: JobStatusView {
                job_id: JobId::from_bytes([3u8; 16]),
                created_by: user(realm_id),
                kind: JobKind::Execution,
                state: JobState::Succeeded,
                attempts: 1,
                cancel_requested: false,
                created_at_ms: 1,
                updated_at_ms: 2,
                finished_at_ms: Some(2),
                progress: JobProgress::new("items"),
                last_error: None,
                result: Some(serde_json::json!({ "exit_code": 0 })),
                workspace_bucket: Some("workspace".to_string()),
                workspace_mode: WorkspaceMode::None,
                locally_exhausted: false,
                session_runtime: None,
                awaiting_keys: Vec::new(),
            },
            run_crate: Some(r#"{"status":"pending"}"#.to_string()),
        };
        let encoded = postcard::to_allocvec(&response).unwrap();
        let decoded: JobResponse = postcard::from_bytes(&encoded).unwrap();
        let JobResponse::Status { job, run_crate } = decoded else {
            panic!("status response roundtrip changed variants");
        };
        assert_eq!(job.result, Some(serde_json::json!({ "exit_code": 0 })));
        assert_eq!(run_crate.as_deref(), Some(r#"{"status":"pending"}"#));
    }
}
