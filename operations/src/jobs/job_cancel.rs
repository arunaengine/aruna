//! Cancels a caller's jobs, here or on the node that answers for them.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::compute::quota::JobReservationRecord;
use aruna_core::jobs::{JobRequest, JobResponse, JobStatusView};
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{JobFamilyId, JobId, JobPayload, JobRecord};
use aruna_core::structs::identity::auth::AuthContext;

use super::job_reads::{JobReadError, OwnedJobOperation, internal, unavailable};
use super::job_routes::{FamilyRouteOperation, refused, routed_job_matches};
use super::lifecycle::LifecycleError;
use super::lifecycle::alias::AliasFamilyOperation;
use super::lifecycle::cancel::CancelFamilyOperation;
use super::lifecycle::family_reads::FamilyStatusOperation;
use super::lifecycle::held::HeldReservations;
use super::records::pages::ServingNode;
use super::route::{JobRouteError, JobRouteOperation, JobRouteOutcome, RoutedJobStatus};
use super::store::JobMutationError;
use super::store::job_calls::{CancelRequestOutcome, set_cancel_requested};
use super::submit::schedule_drain_effect;

fn drain_call() -> IoCall {
    IoCall::Effect(Box::new(schedule_drain_effect()))
}

/// What a cancel found, with the running jobs whose in-process attempts the caller must signal.
#[derive(Debug, PartialEq)]
pub struct CancelReport<T> {
    pub outcome: T,
    pub running: Vec<JobId>,
}

/// What cancelling a job on this node found.
#[derive(Debug, PartialEq)]
pub enum CancelJobOutcome {
    NotFound,
    AlreadyTerminal(JobRecord),
    Requested(JobRecord),
}

/// What cancelling a job on the node that answers for it found.
#[derive(Debug, PartialEq)]
pub enum RoutedCancelOutcome {
    NotFound,
    AlreadyTerminal(JobStatusView),
    Requested(JobStatusView),
}

impl From<CancelJobOutcome> for RoutedCancelOutcome {
    fn from(outcome: CancelJobOutcome) -> Self {
        match outcome {
            CancelJobOutcome::NotFound => Self::NotFound,
            CancelJobOutcome::AlreadyTerminal(record) => {
                Self::AlreadyTerminal(JobStatusView::from(&record))
            }
            CancelJobOutcome::Requested(record) => Self::Requested(JobStatusView::from(&record)),
        }
    }
}

/// Cancels the caller's own job on this node. A job pruned after the ownership read is absent.
/// A job that terminalized here wakes the drain for its run crate.
#[derive(Debug, PartialEq)]
pub struct CancelOwnedOperation {
    user_id: UserId,
    job_id: JobId,
    now_ms: u64,
    flagging: bool,
}

type OwnedReport = CancelReport<CancelJobOutcome>;
type OwnedNext = Result<Next<OwnedReport>, JobReadError>;

impl CancelOwnedOperation {
    pub fn new(user_id: UserId, job_id: JobId, now_ms: u64) -> Self {
        Self {
            user_id,
            job_id,
            now_ms,
            flagging: false,
        }
    }

    fn settled(&self, outcome: CancelRequestOutcome) -> OwnedNext {
        let (outcome, running, wake) = match outcome {
            CancelRequestOutcome::AlreadyTerminal(record) => {
                (CancelJobOutcome::AlreadyTerminal(record), Vec::new(), false)
            }
            CancelRequestOutcome::Cancelled(record) => {
                let wake = matches!(&record.payload, JobPayload::Execution(_));
                (CancelJobOutcome::Requested(record), Vec::new(), wake)
            }
            CancelRequestOutcome::Flagged(record) => {
                (CancelJobOutcome::Requested(record), vec![self.job_id], true)
            }
        };
        let output = CancelReport { outcome, running };
        Ok(match wake {
            true => Next::Then {
                output,
                calls: vec![drain_call()],
            },
            false => Next::Done(output),
        })
    }
}

fn not_found() -> OwnedReport {
    CancelReport {
        outcome: CancelJobOutcome::NotFound,
        running: Vec::new(),
    }
}

impl Operation for CancelOwnedOperation {
    type Output = OwnedReport;
    type Error = JobReadError;

    fn first(&mut self) -> OwnedNext {
        let read = OwnedJobOperation::new(self.user_id, self.job_id);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> OwnedNext {
        if self.flagging {
            return match result.child::<CancelRequestOutcome, JobMutationError>()? {
                Ok(outcome) => self.settled(outcome),
                Err(JobMutationError::NotFound) => Ok(Next::Done(not_found())),
                Err(error) => Err(error.into()),
            };
        }
        if result
            .child::<Option<JobRecord>, JobReadError>()??
            .is_none()
        {
            return Ok(Next::Done(not_found()));
        }
        self.flagging = true;
        let request = set_cancel_requested(self.job_id, self.now_ms);
        Ok(Next::Call(IoCall::child(request)))
    }
}

/// The physical jobs this node holds a reservation for that belong to the requested job: every
/// reservation of its family, or without a family those of the alias itself.
#[derive(Debug, PartialEq)]
pub struct MatchingRunsOperation {
    alias: JobId,
    family: Option<JobFamilyId>,
    /// Reservations still to compare, the next one last; `None` until they are read.
    pending: Option<Vec<JobReservationRecord>>,
    current: Option<JobId>,
    matched: Vec<JobId>,
}

type RunsNext = Result<Next<Vec<JobId>>, JobRouteError>;

impl MatchingRunsOperation {
    pub fn new(alias: JobId, family: Option<JobFamilyId>) -> Self {
        Self {
            alias,
            family,
            pending: None,
            current: None,
            matched: Vec::new(),
        }
    }

    fn compare_next(&mut self) -> RunsNext {
        match self.pending.as_mut().and_then(Vec::pop) {
            Some(reservation) => {
                self.current = Some(reservation.job_id);
                let alias = AliasFamilyOperation::new(reservation.logical_job_id);
                Ok(Next::Call(IoCall::child(alias)))
            }
            None => Ok(Next::Done(std::mem::take(&mut self.matched))),
        }
    }
}

impl Operation for MatchingRunsOperation {
    type Output = Vec<JobId>;
    type Error = JobRouteError;

    fn first(&mut self) -> RunsNext {
        Ok(Next::Call(IoCall::child(HeldReservations::all())))
    }

    fn next(&mut self, result: IoResult) -> RunsNext {
        if let Some(job_id) = self.current.take() {
            let family = result.child::<Option<JobFamilyId>, JobRouteError>()??;
            if family.is_some() && family == self.family {
                self.matched.push(job_id);
            }
            return self.compare_next();
        }
        let mut held = result
            .child::<Vec<JobReservationRecord>, LifecycleError>()?
            .map_err(unavailable)?;
        if self.family.is_none() {
            let alias = self.alias;
            held.retain(|reservation| reservation.logical_job_id == alias);
            let matched = held.into_iter().map(|held| held.job_id).collect();
            return Ok(Next::Done(matched));
        }
        held.reverse();
        self.pending = Some(held);
        self.compare_next()
    }
}

#[derive(Debug, PartialEq)]
enum RoutedStep {
    Family,
    Status,
    Alias(Box<RoutedCancelOutcome>),
    Runs(Box<RoutedCancelOutcome>),
    Route,
    Local,
}

/// Cancels a job where it is answered for. An external job gets an append-only family record,
/// and the local rows of its receipted executions are stopped. Any other job is cancelled by
/// its immutable owner or fails `Unavailable`; no passive copy is terminalized.
#[derive(Debug, PartialEq)]
pub struct RoutedCancelOperation {
    auth: AuthContext,
    job_id: JobId,
    token: Option<AuthToken>,
    serving: ServingNode,
    now_ms: u64,
    step: RoutedStep,
}

type RoutedReport = CancelReport<RoutedCancelOutcome>;
type RoutedNext = Result<Next<RoutedReport>, JobRouteError>;

impl RoutedCancelOperation {
    pub fn new(
        user_id: UserId,
        job_id: JobId,
        token: Option<AuthToken>,
        serving: ServingNode,
        now_ms: u64,
    ) -> Self {
        let auth = AuthContext {
            user_id,
            realm_id: serving.map_or(user_id.realm_id, |(_, realm_id)| realm_id),
            path_restrictions: None,
            session: None,
        };
        Self {
            auth,
            job_id,
            token,
            serving,
            now_ms,
            step: RoutedStep::Family,
        }
    }

    fn local(&mut self) -> RoutedNext {
        self.step = RoutedStep::Local;
        let cancel = CancelOwnedOperation::new(self.auth.user_id, self.job_id, self.now_ms);
        Ok(Next::Call(IoCall::child(cancel)))
    }

    fn route(&mut self) -> RoutedNext {
        let Some(node) = self.serving else {
            return self.local();
        };
        let job_id = self.job_id;
        let request = self
            .token
            .take()
            .map(|auth_token| JobRequest::Cancel { auth_token, job_id });
        self.step = RoutedStep::Route;
        let route = FamilyRouteOperation::new(job_id, request, node, self.now_ms);
        Ok(Next::Call(IoCall::child(route)))
    }

    fn published(&mut self, status: Option<RoutedJobStatus>) -> RoutedNext {
        let status =
            status.ok_or_else(|| unavailable("cancelled family is no longer projectable"))?;
        let outcome = match status.job.state.is_terminal() {
            true => RoutedCancelOutcome::AlreadyTerminal(status.job),
            false => RoutedCancelOutcome::Requested(status.job),
        };
        self.step = RoutedStep::Alias(Box::new(outcome));
        let alias = AliasFamilyOperation::new(self.job_id);
        Ok(Next::Call(IoCall::child(alias)))
    }

    fn answered(&self, response: JobResponse) -> RoutedNext {
        match response {
            JobResponse::Cancelled { job, terminal }
                if routed_job_matches(&job, self.auth.user_id, self.job_id) =>
            {
                let outcome = match terminal {
                    true => RoutedCancelOutcome::AlreadyTerminal(job),
                    false => RoutedCancelOutcome::Requested(job),
                };
                Ok(Next::Done(CancelReport {
                    outcome,
                    running: Vec::new(),
                }))
            }
            JobResponse::NotFound => Err(JobRouteError::NotFound),
            response => Err(refused(response, "cancel")),
        }
    }
}

impl Operation for RoutedCancelOperation {
    type Output = RoutedReport;
    type Error = JobRouteError;

    fn first(&mut self) -> RoutedNext {
        if self.serving.is_none() {
            return self.local();
        }
        let cancel = CancelFamilyOperation::new(
            &self.auth,
            self.job_id,
            self.token.clone(),
            self.serving,
            self.now_ms,
        );
        Ok(Next::Call(IoCall::child(cancel)))
    }

    fn next(&mut self, result: IoResult) -> RoutedNext {
        match std::mem::replace(&mut self.step, RoutedStep::Local) {
            RoutedStep::Family => match result.child::<Option<()>, JobRouteError>()?? {
                None => self.route(),
                Some(()) => {
                    self.step = RoutedStep::Status;
                    let status = FamilyStatusOperation::new(&self.auth, self.job_id, self.now_ms);
                    Ok(Next::Call(IoCall::child(status)))
                }
            },
            RoutedStep::Status => {
                let status = result.child::<Option<RoutedJobStatus>, JobRouteError>()??;
                self.published(status)
            }
            RoutedStep::Alias(outcome) => {
                let family = result
                    .child::<Option<JobFamilyId>, JobRouteError>()??
                    .ok_or_else(|| unavailable("cancelled family alias is no longer resolvable"))?;
                self.step = RoutedStep::Runs(outcome);
                let runs = MatchingRunsOperation::new(self.job_id, Some(family));
                Ok(Next::Call(IoCall::child(runs)))
            }
            RoutedStep::Runs(outcome) => {
                let running = result.child::<Vec<JobId>, JobRouteError>()??;
                Ok(Next::Then {
                    output: CancelReport {
                        outcome: *outcome,
                        running,
                    },
                    calls: vec![drain_call()],
                })
            }
            RoutedStep::Route => match result.child::<JobRouteOutcome, JobRouteError>()?? {
                JobRouteOutcome::Local => self.local(),
                JobRouteOutcome::Remote(response) => self.answered(response),
            },
            RoutedStep::Local => {
                let report = result
                    .child::<OwnedReport, JobReadError>()?
                    .map_err(internal)?;
                Ok(Next::Done(CancelReport {
                    outcome: report.outcome.into(),
                    running: report.running,
                }))
            }
        }
    }

    fn expected_error(error: &JobRouteError) -> bool {
        JobRouteOperation::expected_error(error)
    }
}

#[cfg(test)]
#[path = "job_cancel_tests.rs"]
mod tests;
