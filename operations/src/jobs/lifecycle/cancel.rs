//! Cancels one request family by signing a cancel record and asking executions to stop.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, JobRecordFrame, NetEffect};
use aruna_core::events::{Event, ExecutionSignEvent, NetEvent};
use aruna_core::id::NodeId;
use aruna_core::jobs::{JobRequest, JobResponse};
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{
    CancelAuthority, ExecutionClaim, JobCancelRecord, JobFamilyId, JobFamilyRecord, JobId,
    JobProjection, JobRecordEnvelope, JobRecordKind, LogicalJobSpec,
};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use tracing::{debug, warn};
use ulid::Ulid;

use super::LifecycleError;
use super::admit_forwarded::GroupWriteOperation;
use super::alias::AliasFamilyOperation;
use super::family_reads::{FamilyProjectionOperation, FamilyView};
use super::routing::decidable;
use super::updates::{PublishTerminalOperation, SETTLE_RETRY_AFTER, schedule_terminal_settle};
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::jobs::records::RecordStoreError;
use crate::jobs::records::admit::Admission;
use crate::jobs::records::append::{
    AppendOutcome, AppendRecordConfig, AppendRecordOperation, RecordOrigin,
};
use crate::jobs::records::family::{FamilyReadError, LoadFamilyOperation};
use crate::jobs::records::pages::ServingNode;
use crate::jobs::records::verify::FamilyView as HolderView;
use crate::jobs::route::{JobRouteError, JobRouteOperation, JobRouteOutcome};
use crate::jobs::store::JobMutationError;
use crate::jobs::store::job_calls::{CancelRequestOutcome, set_cancel_requested};
use crate::jobs::submit::schedule_drain_effect;

fn unavailable(reason: impl std::fmt::Display) -> JobRouteError {
    JobRouteError::Unavailable(reason.to_string())
}

fn internal(reason: impl std::fmt::Display) -> JobRouteError {
    JobRouteError::Internal(reason.to_string())
}

fn effect_call(effect: Effect) -> IoCall {
    IoCall::Effect(Box::new(effect))
}

#[derive(Debug, PartialEq)]
enum FlagStep {
    Request,
    Publish,
    Timer,
}

/// Requests cancellation of one local physical row and answers whether the drain must look at
/// it. A settled or pruned row is a no-op, and a repeated request leaves the row untouched. A
/// terminal publication that stays pending is retried by the settle timer.
#[derive(Debug, PartialEq)]
pub struct FlagLocalRunOperation {
    physical: JobId,
    serving: ServingNode,
    now_ms: u64,
    step: FlagStep,
}

impl FlagLocalRunOperation {
    pub fn new(physical: JobId, serving: ServingNode, now_ms: u64) -> Self {
        Self {
            physical,
            serving,
            now_ms,
            step: FlagStep::Request,
        }
    }
}

impl Operation for FlagLocalRunOperation {
    type Output = bool;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<bool>, IoError> {
        let request = set_cancel_requested(self.physical, self.now_ms);
        Ok(Next::Call(IoCall::child(request)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, IoError> {
        let physical = self.physical;
        match self.step {
            FlagStep::Request => match result.child::<CancelRequestOutcome, JobMutationError>()? {
                Ok(CancelRequestOutcome::Cancelled(record)) => {
                    self.step = FlagStep::Publish;
                    let publish = PublishTerminalOperation::new(record, self.serving, self.now_ms);
                    Ok(Next::Call(IoCall::child(publish)))
                }
                Ok(CancelRequestOutcome::Flagged(_)) => {
                    debug!(job_id = %physical, "Local execution cancelled by its family");
                    Ok(Next::Done(true))
                }
                Ok(CancelRequestOutcome::AlreadyTerminal(_)) | Err(JobMutationError::NotFound) => {
                    Ok(Next::Done(false))
                }
                Err(error) => {
                    warn!(job_id = %physical, error = %error, "Local execution cancel flag failed");
                    Ok(Next::Done(false))
                }
            },
            FlagStep::Publish => {
                let published = match result.child::<bool, LifecycleError>()? {
                    Ok(published) => published,
                    Err(error) => {
                        warn!(error = %error, "Execution publication failed");
                        false
                    }
                };
                if published {
                    return Ok(Next::Done(true));
                }
                self.step = FlagStep::Timer;
                let timer = schedule_terminal_settle(SETTLE_RETRY_AFTER);
                Ok(Next::Call(effect_call(timer)))
            }
            FlagStep::Timer => Ok(Next::Done(true)),
        }
    }
}

/// Physical rows of the executions this node signed a receipt for.
fn local_physical_jobs(receipts: &[JobRecordEnvelope], local: NodeId) -> Vec<JobId> {
    receipts
        .iter()
        .filter_map(|envelope| match &envelope.record {
            JobFamilyRecord::Receipt(receipt) if receipt.executor_node_id == local => {
                Some(receipt.physical_job_id)
            }
            _ => None,
        })
        .collect()
}

/// Stops every physical execution of one family this node runs. The replicated record reaches
/// no local attempt by itself, so each local row is flagged here and the drain is woken once.
#[derive(Debug, PartialEq)]
pub struct CancelLocalRunsOperation {
    family: JobFamilyId,
    serving: ServingNode,
    now_ms: u64,
    /// Rows still to flag, the next one last; `None` until the receipts are read.
    pending: Option<Vec<JobId>>,
    flagged: bool,
}

type LocalRunsNext = Result<Next<()>, IoError>;

impl CancelLocalRunsOperation {
    pub fn new(family: JobFamilyId, serving: ServingNode, now_ms: u64) -> Self {
        Self {
            family,
            serving,
            now_ms,
            pending: None,
            flagged: false,
        }
    }

    fn flag_next(&mut self) -> LocalRunsNext {
        match self.pending.as_mut().and_then(Vec::pop) {
            Some(physical) => {
                let flag = FlagLocalRunOperation::new(physical, self.serving, self.now_ms);
                Ok(Next::Call(IoCall::child(flag)))
            }
            None if self.flagged => Ok(Next::Then {
                output: (),
                calls: vec![effect_call(schedule_drain_effect())],
            }),
            None => Ok(Next::Done(())),
        }
    }
}

impl Operation for CancelLocalRunsOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> LocalRunsNext {
        if self.serving.is_none() {
            return Ok(Next::Done(()));
        }
        let read = LoadFamilyOperation::kind(self.family, JobRecordKind::Receipt);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> LocalRunsNext {
        if self.pending.is_some() {
            self.flagged |= result.child::<bool, IoError>()?.unwrap_or(false);
            return self.flag_next();
        }
        let receipts = match result.child::<Vec<JobRecordEnvelope>, FamilyReadError>()? {
            Ok(receipts) => receipts,
            Err(error) => {
                warn!(error = %error, "Family receipts unreadable for a local cancel");
                return Ok(Next::Done(()));
            }
        };
        let Some((local, _)) = self.serving else {
            return Ok(Next::Done(()));
        };
        let mut pending = local_physical_jobs(&receipts, local);
        pending.reverse();
        self.pending = Some(pending);
        self.flag_next()
    }
}

#[derive(Debug, PartialEq)]
enum PublishStep {
    Config,
    Forward(Vec<NodeId>),
    Sign,
    Append,
}

/// Signs and appends the cancellation. Only a current family holder may author it; another node
/// forwards a cancel to the holders, and a deletion needs a holder here.
#[derive(Debug, PartialEq)]
pub struct PublishCancelOperation {
    spec: LogicalJobSpec,
    auth: AuthContext,
    authority: CancelAuthority,
    serving: ServingNode,
    cancel_id: Ulid,
    now_ms: u64,
    step: PublishStep,
}

type PublishNext = Result<Next<()>, JobRouteError>;

impl PublishCancelOperation {
    pub fn new(
        spec: LogicalJobSpec,
        auth: &AuthContext,
        authority: CancelAuthority,
        serving: ServingNode,
        now_ms: u64,
    ) -> Self {
        Self {
            spec,
            auth: auth.clone(),
            authority,
            serving,
            cancel_id: Ulid::generate(),
            now_ms,
            step: PublishStep::Config,
        }
    }

    fn node(
        &self,
    ) -> Result<(NodeId, aruna_core::structs::identity::realm::RealmId), JobRouteError> {
        self.serving
            .ok_or_else(|| unavailable("network handle unavailable"))
    }

    fn configured(&mut self, config: &RealmConfigDocument) -> PublishNext {
        let (local, realm_id) = self.node()?;
        let family = JobFamilyId {
            submission_id: self.spec.submission_id,
            request_digest: self.spec.request_digest,
        };
        let view = HolderView::resolve(config, realm_id, family)
            .ok_or_else(|| unavailable("job family holder view unavailable"))?;
        if view.holds(local) {
            return self.sign();
        }
        // A forwarded cancel names no deletion, so only a holder records one.
        if self.authority == CancelAuthority::Deletion {
            return Err(unavailable("this node does not hold the job family"));
        }
        let mut holders: Vec<NodeId> = view
            .holders()
            .iter()
            .copied()
            .filter(|holder| view.holds(*holder))
            .collect();
        holders.reverse();
        self.forward_next(holders)
    }

    fn forward_next(&mut self, mut holders: Vec<NodeId>) -> PublishNext {
        let Some(holder) = holders.pop() else {
            return Err(unavailable("job family has no reachable holder"));
        };
        let (local, realm_id) = self.node()?;
        let request = JobRequest::Cancel {
            auth_token: AuthToken::internal(self.auth.clone()),
            job_id: self.spec.job_id,
        };
        let route = JobRouteOperation::new(realm_id, local, self.spec.job_id, Some(request))
            .with_responder(Some(holder));
        self.step = PublishStep::Forward(holders);
        Ok(Next::Call(IoCall::child(route)))
    }

    fn forwarded(
        &mut self,
        holders: Vec<NodeId>,
        answer: Result<JobRouteOutcome, JobRouteError>,
    ) -> PublishNext {
        let response = match answer {
            Ok(JobRouteOutcome::Remote(response)) => response,
            Ok(JobRouteOutcome::Local) => return self.forward_next(holders),
            Err(error) => {
                debug!(error = %error, "Family cancellation forwarding failed");
                return self.forward_next(holders);
            }
        };
        match response {
            JobResponse::Cancelled { .. } => Ok(Next::Done(())),
            JobResponse::Unauthorized => Err(JobRouteError::Unauthorized),
            JobResponse::Forbidden => Err(JobRouteError::Forbidden),
            JobResponse::NotFound => Err(JobRouteError::NotFound),
            JobResponse::Unavailable(error) => {
                debug!(error, "Family cancellation holder unavailable");
                self.forward_next(holders)
            }
            response => Err(unavailable(format!(
                "unexpected family cancel response: {response:?}"
            ))),
        }
    }

    fn sign(&mut self) -> PublishNext {
        let (publisher, realm_id) = self.node()?;
        let spec = &self.spec;
        let record = JobCancelRecord {
            cancel_id: self.cancel_id,
            submission_id: spec.submission_id,
            request_digest: spec.request_digest,
            job_id: spec.job_id,
            spec_digest: spec.spec_digest,
            requested_by: self.auth.user_id,
            authority: self.authority,
            requested_at_ms: self.now_ms,
        };
        let claim = ExecutionClaim {
            realm_id,
            record: JobFamilyRecord::Cancel(record),
            publisher,
        };
        self.step = PublishStep::Sign;
        let effect = Effect::Net(NetEffect::ExecutionSign(Box::new(claim)));
        Ok(Next::Call(effect_call(effect)))
    }

    fn signed(&mut self, result: IoResult) -> PublishNext {
        let expected = "Event::Net(NetEvent::ExecutionSign)";
        let IoResult::Event(event) = result else {
            let got = result.label();
            return Err(IoError::Unexpected { expected, got }.into());
        };
        let envelope = match *event {
            Event::Net(NetEvent::ExecutionSign(ExecutionSignEvent::Signed(envelope))) => *envelope,
            Event::Net(NetEvent::ExecutionSign(ExecutionSignEvent::Unavailable(error))) => {
                return Err(internal(error));
            }
            _ => {
                let got = "another event";
                return Err(IoError::Unexpected { expected, got }.into());
            }
        };
        let (local_node_id, realm_id) = self.node()?;
        let record = JobRecordFrame::new(envelope).map_err(internal)?;
        self.step = PublishStep::Append;
        let append = AppendRecordOperation::new(AppendRecordConfig {
            realm_id,
            local_node_id,
            record,
            local: None,
            origin: RecordOrigin::Local,
            now_ms: self.now_ms,
        });
        Ok(Next::Call(IoCall::child(append)))
    }
}

impl Operation for PublishCancelOperation {
    type Output = ();
    type Error = JobRouteError;

    fn first(&mut self) -> PublishNext {
        let (_, realm_id) = self.node()?;
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(realm_id))))
    }

    fn next(&mut self, result: IoResult) -> PublishNext {
        match std::mem::replace(&mut self.step, PublishStep::Append) {
            PublishStep::Config => {
                let config = result.child::<RealmConfigDocument, GetConfigError>()?;
                let config = config.map_err(|_| unavailable("realm config unavailable"))?;
                self.configured(&config)
            }
            PublishStep::Forward(holders) => {
                let answer = result.child::<JobRouteOutcome, JobRouteError>()?;
                self.forwarded(holders, answer)
            }
            PublishStep::Sign => self.signed(result),
            PublishStep::Append => {
                let outcome = result.child::<AppendOutcome, RecordStoreError>()?;
                let outcome = outcome.map_err(unavailable)?;
                if !matches!(
                    outcome.admission,
                    Admission::Authentic | Admission::Duplicate
                ) {
                    return Err(unavailable(
                        "job cancellation is awaiting authentic admission",
                    ));
                }
                debug!(job_id = %self.spec.job_id, "Job cancellation published");
                Ok(Next::Done(()))
            }
        }
    }
}

/// Records that the submitter deleted a finished run, so every holder answers it as absent. A
/// job without a family here records nothing.
#[derive(Debug, PartialEq)]
pub struct PublishDeletionOperation {
    auth: AuthContext,
    job_id: JobId,
    serving: ServingNode,
    now_ms: u64,
    publishing: bool,
}

impl PublishDeletionOperation {
    pub fn new(auth: &AuthContext, job_id: JobId, serving: ServingNode, now_ms: u64) -> Self {
        Self {
            auth: auth.clone(),
            job_id,
            serving,
            now_ms,
            publishing: false,
        }
    }
}

impl Operation for PublishDeletionOperation {
    type Output = ();
    type Error = JobRouteError;

    fn first(&mut self) -> PublishNext {
        let read = FamilyProjectionOperation::new(self.job_id, self.now_ms);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> PublishNext {
        if self.publishing {
            result.child::<(), JobRouteError>()??;
            return Ok(Next::Done(()));
        }
        let Some((_, spec)) = result.child::<Option<FamilyView>, JobRouteError>()?? else {
            return Ok(Next::Done(()));
        };
        if spec.created_by != self.auth.user_id {
            return Err(JobRouteError::NotFound);
        }
        self.publishing = true;
        let publish = PublishCancelOperation::new(
            spec,
            &self.auth,
            CancelAuthority::Deletion,
            self.serving,
            self.now_ms,
        );
        Ok(Next::Call(IoCall::child(publish)))
    }
}

#[derive(Debug, PartialEq)]
enum CancelStep {
    Alias,
    Projection(JobFamilyId),
    Authority(Box<CancelTarget>),
    Publish(Box<CancelTarget>),
    Local(Box<JobProjection>),
    Stop(Vec<NodeId>),
}

/// What a cancel of one family found.
#[derive(Debug, PartialEq)]
struct CancelTarget {
    family: JobFamilyId,
    projection: JobProjection,
    spec: LogicalJobSpec,
}

/// Cancels one external job through its family; `None` means the alias names no family here, so
/// the caller keeps its ordinary local cancellation. Every known active execution is asked to
/// stop; a partitioned one may still finish and stays visible as a late completion.
#[derive(Debug, PartialEq)]
pub struct CancelFamilyOperation {
    auth: AuthContext,
    job_id: JobId,
    token: Option<AuthToken>,
    serving: ServingNode,
    now_ms: u64,
    step: CancelStep,
}

type CancelNext = Result<Next<Option<()>>, JobRouteError>;

impl CancelFamilyOperation {
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
            step: CancelStep::Alias,
        }
    }

    fn projected(&mut self, family: JobFamilyId, read: Option<FamilyView>) -> CancelNext {
        let Some((projected, spec)) = read else {
            return Ok(Next::Done(None));
        };
        // Cancel authority is a decision: a truncated projection proves nothing.
        decidable(&projected)?;
        let projection = projected
            .projection
            .ok_or_else(|| unavailable("job family has no projection"))?;
        let target = Box::new(CancelTarget {
            family,
            projection,
            spec,
        });
        // The submitter is checked by every holder again; a group admin's permission is
        // checked here and the signature is that statement.
        if self.auth.user_id == target.spec.created_by {
            return self.publish(target, CancelAuthority::Submitter);
        }
        let (local, _) = self.serving.ok_or(JobRouteError::Forbidden)?;
        let check = GroupWriteOperation::new(&self.auth, target.spec.group_id, local);
        self.step = CancelStep::Authority(target);
        Ok(Next::Call(IoCall::child(check)))
    }

    fn publish(&mut self, target: Box<CancelTarget>, authority: CancelAuthority) -> CancelNext {
        let publish = PublishCancelOperation::new(
            target.spec.clone(),
            &self.auth,
            authority,
            self.serving,
            self.now_ms,
        );
        self.step = CancelStep::Publish(target);
        Ok(Next::Call(IoCall::child(publish)))
    }

    fn stop_next(&mut self, mut executors: Vec<NodeId>) -> CancelNext {
        let (Some(token), Some((local, realm_id))) = (self.token.clone(), self.serving) else {
            return Ok(Next::Done(Some(())));
        };
        // A local execution has no network cancel: the local runs were flagged already.
        let Some(executor) = executors.pop().filter(|executor| *executor != local) else {
            return match executors.is_empty() {
                true => Ok(Next::Done(Some(()))),
                false => self.stop_next(executors),
            };
        };
        let request = JobRequest::Cancel {
            auth_token: token,
            job_id: self.job_id,
        };
        let route = JobRouteOperation::new(realm_id, local, self.job_id, Some(request))
            .with_responder(Some(executor));
        self.step = CancelStep::Stop(executors);
        Ok(Next::Call(IoCall::child(route)))
    }
}

impl Operation for CancelFamilyOperation {
    type Output = Option<()>;
    type Error = JobRouteError;

    fn first(&mut self) -> CancelNext {
        Ok(Next::Call(IoCall::child(AliasFamilyOperation::new(
            self.job_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> CancelNext {
        match std::mem::replace(&mut self.step, CancelStep::Alias) {
            CancelStep::Alias => {
                let Some(family) = result.child::<Option<JobFamilyId>, JobRouteError>()?? else {
                    return Ok(Next::Done(None));
                };
                self.step = CancelStep::Projection(family);
                let read = FamilyProjectionOperation::new(self.job_id, self.now_ms);
                Ok(Next::Call(IoCall::child(read)))
            }
            CancelStep::Projection(family) => {
                let read = result.child::<Option<FamilyView>, JobRouteError>()??;
                self.projected(family, read)
            }
            CancelStep::Authority(target) => match result.child::<bool, IoError>()? {
                Ok(true) => self.publish(target, CancelAuthority::GroupAdmin),
                Ok(false) | Err(_) => Err(JobRouteError::Forbidden),
            },
            CancelStep::Publish(target) => {
                result.child::<(), JobRouteError>()??;
                let local = CancelLocalRunsOperation::new(target.family, self.serving, self.now_ms);
                self.step = CancelStep::Local(Box::new(target.projection));
                Ok(Next::Call(IoCall::child(local)))
            }
            CancelStep::Local(projection) => {
                let _ = result.child::<(), IoError>()?;
                let mut executors: Vec<NodeId> = projection
                    .executions
                    .iter()
                    .filter(|execution| !execution.state.is_terminal())
                    .map(|execution| execution.executor_node_id)
                    .collect();
                executors.reverse();
                self.stop_next(executors)
            }
            CancelStep::Stop(executors) => {
                if let Err(error) = result.child::<JobRouteOutcome, JobRouteError>()? {
                    warn!(error = %error, "Cancel delivery to an executor failed");
                }
                self.stop_next(executors)
            }
        }
    }
}
