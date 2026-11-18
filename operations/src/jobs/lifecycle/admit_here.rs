//! Admits one submission on the node that holds its family: signs the spec and its claim,
//! decides the standing quota and commits the admission.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;
use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, JobRecordFrame, NetEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, ExecutionSignEvent, NetEvent};
use aruna_core::heartbeat::HeldHeartbeat;
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{FAMILY_PROJECTION_KEYSPACE, FAMILY_RECORD_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::rows::page;
use aruna_core::structs::execution::job::record_keys::{family_prefix, kind_prefix};
use aruna_core::structs::execution::job::record_rows::ProjectionCache;
use aruna_core::structs::execution::job::{
    ExecutionClaim, JobAdmissionRecord, JobFamilyId, JobFamilyRecord, JobId, JobRecordKind,
    JobRetryPolicy, LogicalJobSpec, LogicalJobState, SubmissionClaim, SubmissionId,
};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use tracing::{debug, warn};

use super::LifecycleError;
use super::admit::{
    AdmissionCandidate, AdmitSubmissionConfig, AdmitSubmissionOperation, AdmittedSubmission,
};
use super::ids::{RequestIdentity, SubmissionRequest, effective_resources};
use super::outbox::schedule_outbox_drain;
use super::witness_arm::ArmFamilyOperation;
use crate::jobs::quota::{QuotaCheckOperation, QuotaError, QuotaVerdict};
use crate::jobs::records::RecordStoreError;
use crate::jobs::records::project::{
    FamilyRef, ProjectFamilyConfig, ProjectFamilyOperation, ProjectedFamily,
};
use crate::jobs::submit::SubmitJobError;

/// Launches one witness may spend on a request over its whole lifetime. It is
/// stored in the immutable spec, so a later config change cannot widen it.
const LAUNCHES_PER_WITNESS: u32 = 3;

/// Admission transactions one submission runs before it reports the group as
/// unavailable. Only a conflict with a concurrent admission is retried.
const ADMISSION_ATTEMPTS: usize = 3;

/// What the caller learns about an accepted submission.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AcceptedSubmission {
    /// The canonical alias of this request family at the accepting holder.
    pub job_id: JobId,
    /// False when a matching claim already existed.
    pub created: bool,
    pub submission_id: SubmissionId,
    /// The family's state at this accept: `Queued` for a fresh admission, and
    /// what this holder currently reduces for a replay of a request that may
    /// already be running or finished.
    pub state: LogicalJobState,
}

/// Maps a failed admission to what the submitter is told.
pub(super) fn admission_error(error: LifecycleError) -> SubmitJobError {
    match error {
        LifecycleError::IdempotencyConflict { existing_job_id } => {
            SubmitJobError::JobPlanConflict { existing_job_id }
        }
        LifecycleError::QuotaDenied(reason) => SubmitJobError::QuotaDenied(reason),
        LifecycleError::GroupWrite(error) => SubmitJobError::GroupWrite(error),
        error => SubmitJobError::PlacementUnavailable(error.to_string()),
    }
}

fn unavailable(error: impl std::fmt::Display) -> SubmitJobError {
    SubmitJobError::PlacementUnavailable(error.to_string())
}

/// The state this holder currently reduces for `family`. The cached projection answers when
/// current; otherwise the family is reduced once from its records. A family this node cannot
/// fully reduce is reported as indeterminate.
#[derive(Debug, PartialEq)]
pub(super) struct ObservedStateOperation {
    family: JobFamilyId,
    now_ms: u64,
    reducing: bool,
}

impl ObservedStateOperation {
    pub(super) fn new(family: JobFamilyId, now_ms: u64) -> Self {
        Self {
            family,
            now_ms,
            reducing: false,
        }
    }
}

fn state_of(cache: Option<aruna_core::structs::execution::job::JobProjection>) -> LogicalJobState {
    cache.map_or(LogicalJobState::Indeterminate, |projection| {
        projection.state
    })
}

impl Operation for ObservedStateOperation {
    type Output = LogicalJobState;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<LogicalJobState>, IoError> {
        Ok(Next::Call(IoCall::Page {
            space: FAMILY_PROJECTION_KEYSPACE.to_string(),
            prefix: family_prefix(&self.family),
            after: None,
            limit: 1,
        }))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<LogicalJobState>, IoError> {
        if self.reducing {
            let projection = match result.child::<ProjectedFamily, RecordStoreError>()? {
                Ok(projected) if projected.truncated => {
                    warn!("Replayed submission reduced only part of its family");
                    None
                }
                Ok(projected) => projected.projection,
                Err(error) => {
                    warn!(error = %error, "Replayed submission could not reduce its family");
                    None
                }
            };
            return Ok(Next::Done(state_of(projection)));
        }
        let (rows, _) = page(result)?;
        let cached = rows
            .first()
            .and_then(|(_, value)| ProjectionCache::decode(value));
        if let Some(cache) = cached.filter(|cache| !cache.stale) {
            return Ok(Next::Done(state_of(cache.projection)));
        }
        self.reducing = true;
        Ok(Next::Call(IoCall::child(ProjectFamilyOperation::new(
            ProjectFamilyConfig {
                family: FamilyRef::Family(self.family),
                now_ms: self.now_ms,
                rebuild: false,
            },
        ))))
    }
}

/// One submission this node admits as the holder of its family.
#[derive(Debug, PartialEq)]
pub(super) struct LocalAdmission {
    pub(super) request: SubmissionRequest,
    pub(super) identity: RequestIdentity,
    pub(super) config: RealmConfigDocument,
    pub(super) local: NodeId,
    /// The alias minted for this accept.
    pub(super) job_id: JobId,
    pub(super) now_ms: u64,
    /// The fresh heartbeats the quota's group view merges.
    pub(super) fresh: BTreeMap<NodeId, HeldHeartbeat>,
}

#[derive(Debug, PartialEq)]
enum AdmitStep {
    SignSpec,
    SignClaim,
    Claims,
    Quota,
    Admit,
    Observed,
}

/// Signs the immutable spec and its claim for the minted alias and commits them. The answered
/// alias is canonical at this accept: a fresh admission holds the only claim, and a replay
/// settles on the already-reduced claim. A replay never reads the quota view, and a lost
/// transaction against a concurrent submission of the group decides the quota again.
/// Scheduling and replication are armed only after the claim is durable, so a failed commit
/// never leaves a round pointing at a family that is absent.
#[derive(Debug, PartialEq)]
pub(super) struct AdmitHereOperation {
    admission: LocalAdmission,
    claim: Option<SubmissionClaim>,
    spec_frame: Option<JobRecordFrame>,
    candidate: Option<AdmissionCandidate>,
    attempt: usize,
    admitted: Option<AdmittedSubmission>,
    step: AdmitStep,
}

type AdmitNext = Result<Next<AcceptedSubmission>, SubmitJobError>;

impl AdmitHereOperation {
    pub(super) fn new(admission: LocalAdmission) -> Self {
        Self {
            admission,
            claim: None,
            spec_frame: None,
            candidate: None,
            attempt: 0,
            admitted: None,
            step: AdmitStep::SignSpec,
        }
    }

    fn spec(&self) -> Result<LogicalJobSpec, SubmitJobError> {
        let LocalAdmission {
            request,
            identity,
            config,
            local,
            job_id,
            now_ms,
            ..
        } = &self.admission;
        let placement = config
            .family_placement(identity.submission_id)
            .map_err(unavailable)?;
        LogicalJobSpec {
            submission_id: identity.submission_id,
            job_id: *job_id,
            origin_node_id: *local,
            ingress_node_id: request.ingress_node_id,
            realm_id: config.realm_id,
            group_id: request.spec.group_id,
            created_by: request.created_by,
            created_at_ms: *now_ms,
            payload: request.spec.clone(),
            request_digest: identity.request_digest,
            spec_digest: [0u8; 32],
            resources: effective_resources(&request.spec),
            retention_ms: request.retention_ms,
            retry: JobRetryPolicy {
                launches_per_witness: LAUNCHES_PER_WITNESS,
            },
            admission: JobAdmissionRecord {
                submission_id: identity.submission_id,
                request_digest: identity.request_digest,
                job_id: *job_id,
                group_id: request.spec.group_id,
                admitting_node_id: *local,
                // The realm config publishes no membership generation yet; the
                // departure round fills it. Zero is unstamped, not an epoch claim.
                membership_generation: 0,
                resources: effective_resources(&request.spec),
                admitted_at_ms: *now_ms,
            },
            captured_inputs: request.captured_inputs.clone(),
            input_pages: request.input_pages,
            output_policies: request.output_policies.clone(),
            placement,
        }
        .store_digest()
        .map_err(unavailable)
    }

    fn sign(&self, record: JobFamilyRecord) -> AdmitNext {
        let claim = ExecutionClaim {
            realm_id: self.admission.config.realm_id,
            record,
            publisher: self.admission.local,
        };
        let sign = Effect::Net(NetEffect::ExecutionSign(Box::new(claim)));
        Ok(Next::Call(IoCall::Effect(Box::new(sign))))
    }

    /// Whether this node already holds a claim for the family, which makes the admission a
    /// replay that settles inside its own transaction.
    fn claims(&mut self) -> AdmitNext {
        self.step = AdmitStep::Claims;
        let family = self.admission.identity.family();
        Ok(Next::Call(IoCall::Page {
            space: FAMILY_RECORD_KEYSPACE.to_string(),
            prefix: kind_prefix(&family, JobRecordKind::Claim),
            after: None,
            limit: 1,
        }))
    }

    fn quota(&mut self) -> AdmitNext {
        self.step = AdmitStep::Quota;
        let admission = &self.admission;
        let check = QuotaCheckOperation::new(
            admission.config.clone(),
            admission.local,
            admission.request.spec.group_id,
            effective_resources(&admission.request.spec),
            admission.fresh.clone(),
        );
        Ok(Next::Call(IoCall::child(check)))
    }

    fn admit(&mut self, (quota_refusal, quota_revision): QuotaVerdict) -> AdmitNext {
        let candidate = self
            .candidate
            .clone()
            .ok_or_else(|| unavailable("the signed admission was lost"))?;
        self.step = AdmitStep::Admit;
        let admission = &self.admission;
        let admit = AdmitSubmissionOperation::new(AdmitSubmissionConfig {
            realm_id: admission.config.realm_id,
            local_node_id: admission.local,
            submission_id: admission.identity.submission_id,
            request_digest: admission.identity.request_digest,
            candidate: Box::new(candidate),
            now_ms: admission.now_ms,
            quota_refusal,
            quota_revision,
        });
        Ok(Next::Call(IoCall::child(admit)))
    }

    fn admitted(&mut self, admitted: AdmittedSubmission) -> AdmitNext {
        if admitted.created {
            return self.accept(admitted, LogicalJobState::Queued);
        }
        self.step = AdmitStep::Observed;
        let observed = ObservedStateOperation::new(admitted.family, self.admission.now_ms);
        self.admitted = Some(admitted);
        Ok(Next::Call(IoCall::child(observed)))
    }

    fn accept(&self, admitted: AdmittedSubmission, state: LogicalJobState) -> AdmitNext {
        let admission = &self.admission;
        let arm = ArmFamilyOperation::new(
            admission.identity.family(),
            admission.local,
            admission.config.realm_id,
            admission.now_ms,
        );
        Ok(Next::Then {
            output: AcceptedSubmission {
                job_id: admitted.job_id,
                created: admitted.created,
                submission_id: admission.identity.submission_id,
                state,
            },
            calls: vec![
                IoCall::child(arm),
                IoCall::Effect(Box::new(schedule_outbox_drain(Duration::ZERO))),
            ],
        })
    }
}

/// The record frame a sign effect answered.
fn signed_frame(result: IoResult) -> Result<JobRecordFrame, SubmitJobError> {
    let IoResult::Event(event) = result else {
        let got = result.label();
        let expected = "Event::Net(NetEvent::ExecutionSign)";
        return Err(IoError::Unexpected { expected, got }.into());
    };
    match *event {
        Event::Net(NetEvent::ExecutionSign(ExecutionSignEvent::Signed(envelope))) => {
            JobRecordFrame::new(*envelope)
                .map_err(|error| SubmitJobError::InvalidWorkspace(error.to_string()))
        }
        Event::Net(NetEvent::ExecutionSign(ExecutionSignEvent::Unavailable(error))) => {
            Err(unavailable(error))
        }
        other => Err(unavailable(format!(
            "unexpected signing answer: {}",
            other.label()
        ))),
    }
}

impl Operation for AdmitHereOperation {
    type Output = AcceptedSubmission;
    type Error = SubmitJobError;

    fn first(&mut self) -> AdmitNext {
        let spec = self.spec()?;
        let admission = &self.admission;
        self.claim = Some(SubmissionClaim {
            submission_id: admission.identity.submission_id,
            job_id: admission.job_id,
            request_digest: admission.identity.request_digest,
            spec_digest: spec.spec_digest,
            committing_node_id: admission.local,
            accepted_at_ms: admission.now_ms,
        });
        self.sign(JobFamilyRecord::Spec(Box::new(spec)))
    }

    fn next(&mut self, result: IoResult) -> AdmitNext {
        match self.step {
            AdmitStep::SignSpec => {
                self.spec_frame = Some(signed_frame(result)?);
                let claim = self
                    .claim
                    .take()
                    .ok_or_else(|| unavailable("the claim was lost"))?;
                self.step = AdmitStep::SignClaim;
                self.sign(JobFamilyRecord::Claim(claim))
            }
            AdmitStep::SignClaim => {
                let claim = signed_frame(result)?;
                let spec = self
                    .spec_frame
                    .take()
                    .ok_or_else(|| unavailable("the signed spec was lost"))?;
                self.candidate = Some(AdmissionCandidate {
                    job_id: self.admission.job_id,
                    spec,
                    claim,
                });
                self.claims()
            }
            AdmitStep::Claims => {
                let (rows, _) = page(result)?;
                match rows.is_empty() {
                    true => self.quota(),
                    false => self.admit((None, None)),
                }
            }
            AdmitStep::Quota => {
                let verdict = result
                    .child::<QuotaVerdict, QuotaError>()?
                    .map_err(unavailable)?;
                self.admit(verdict)
            }
            AdmitStep::Admit => match result.child::<AdmittedSubmission, LifecycleError>()? {
                Ok(admitted) => self.admitted(admitted),
                Err(LifecycleError::Storage(StorageError::TransactionConflict))
                    if self.attempt + 1 < ADMISSION_ATTEMPTS =>
                {
                    self.attempt += 1;
                    let group_id = self.admission.request.spec.group_id;
                    debug!(group_id = %group_id, "Retrying an admission a concurrent one won");
                    self.claims()
                }
                Err(error) => Err(admission_error(error)),
            },
            AdmitStep::Observed => {
                let state = result.child::<LogicalJobState, IoError>()?;
                let state = state.unwrap_or_else(|error| {
                    warn!(error = %error, "Replayed submission could not read its state");
                    LogicalJobState::Indeterminate
                });
                let admitted = self
                    .admitted
                    .take()
                    .ok_or_else(|| unavailable("the admission was lost"))?;
                self.accept(admitted, state)
            }
        }
    }
}

