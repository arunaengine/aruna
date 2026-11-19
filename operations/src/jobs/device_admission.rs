//! Chooses the backend of a device job by the rules planning applies, and checks the backend
//! values the job was admitted under again before it starts or resumes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::compute::effect::{ComputeAnswer, ComputeEffect};
use aruna_core::compute::identity::BackendConfigRecord;
use aruna_core::compute::offer::ExecutorOffer;
use aruna_core::compute::{ExecutorKind, NetworkAccess};
use aruna_core::operation::Operation;
use aruna_core::scheduling::{RejectionVerdict, backend_verdict, hosts_session};
use aruna_core::structs::execution::job::{
    ExecutionSpec, JobError, JobId, JobRecord, LocalAdmission,
};
use aruna_core::structs::placement::node_subject::NodeSubjectRecord;
use tracing::warn;
use ulid::Ulid;

use crate::jobs::lifecycle::ids::{effective_resources, session_of};
use crate::jobs::local_run::LocalExecutionError;
use crate::jobs::store::job_calls::store_admission;
use crate::jobs::store::mutation_error::JobMutationError;
use crate::jobs::workflow::DEFAULT_WALLTIME;
use crate::jobs::workflow::compute::{compute_answer, compute_call};
use crate::jobs::workflow::prepare::NETWORK_TAG_KEY;
use crate::node::backend_record::{BackendRecordError, BackendRecordOperation};
use crate::placement::policy::{LocalSubjectOperation, SubjectScanError};

/// The network a session run asks for; `None` for a run that is no session.
pub(crate) fn session_network(spec: &ExecutionSpec) -> Option<NetworkAccess> {
    session_of(spec)?;
    Some(match spec.tags.get(NETWORK_TAG_KEY).map(String::as_str) {
        Some("open") => NetworkAccess::Open,
        _ => NetworkAccess::Isolated,
    })
}

/// Why `offer` cannot take the run now, by the rules planning applies: session support,
/// envelope, group allow-list, walltime maximum and suitability.
pub fn backend_refusal(offer: &ExecutorOffer, spec: &ExecutionSpec) -> Option<RejectionVerdict> {
    let caps = offer.caps();
    let network = session_network(spec);
    if network.is_some_and(|network| !hosts_session(offer.kind(), caps.session, network)) {
        return Some(RejectionVerdict::Session);
    }
    // Docker grants an explicit request as asked, so the envelope is enforced here.
    if !caps.limits.fits(&effective_resources(spec)) {
        return Some(RejectionVerdict::Resources);
    }
    let walltime_ms =
        (spec.resources.max_walltime_ms).unwrap_or(DEFAULT_WALLTIME.as_millis() as u64);
    backend_verdict(
        &caps.groups,
        caps.max_walltime_ms,
        &offer.suitability(),
        spec.group_id,
        network.is_some(),
        walltime_ms,
    )
}

/// The first offer in name order that takes the run, narrowed by the named backend and the
/// kind constraint. With none, the refusal of the first one passed over, if any was.
pub fn select_offer<'a>(
    offers: &'a [ExecutorOffer],
    spec: &ExecutionSpec,
) -> Result<&'a ExecutorOffer, Option<RejectionVerdict>> {
    let constraint = (spec.executor_constraint.as_deref()).map(ExecutorKind::from_wire);
    let mut refusal = None;
    for offer in offers {
        let other_kind = (constraint.as_ref())
            .is_some_and(|kind| *kind != ExecutorKind::from_wire(offer.kind()));
        let other_name = (spec.backend.as_deref()).is_some_and(|name| name != offer.name());
        if other_kind || other_name {
            continue;
        }
        match backend_refusal(offer, spec) {
            Some(verdict) => {
                refusal.get_or_insert(verdict);
            }
            None => return Ok(offer),
        }
    }
    Err(refusal)
}

#[derive(Debug, PartialEq)]
enum AdmissionStep {
    Record,
    Subject(u64),
}

/// The backend values a device admits a run under: the config generation of `offer`, and the
/// execution subject it advertises at this device's placement subject, when it has one.
#[derive(Debug, PartialEq)]
pub struct BackendAdmission {
    offer: ExecutorOffer,
    step: AdmissionStep,
}

type AdmissionNext = Result<Next<LocalAdmission>, LocalExecutionError>;

impl BackendAdmission {
    pub fn new(offer: ExecutorOffer) -> Self {
        Self {
            offer,
            step: AdmissionStep::Record,
        }
    }

    fn admission(&self, generation: u64, subject: Option<NodeSubjectRecord>) -> AdmissionNext {
        let unavailable = |error: String| LocalExecutionError::Unavailable(error);
        let subject = match subject {
            Some(record) => {
                let advertised = self.offer.capability(&record.subject, false);
                let advertised = advertised.map_err(|error| unavailable(error.to_string()))?;
                Some((advertised.subject.generation, advertised.subject_digest))
            }
            None => None,
        };
        Ok(Next::Done(LocalAdmission {
            backend: self.offer.name().to_string(),
            config_generation: generation,
            config_digest: self.offer.config_digest(),
            home_digest: self.offer.home(),
            subject,
        }))
    }
}

impl Operation for BackendAdmission {
    type Output = LocalAdmission;
    type Error = LocalExecutionError;

    fn first(&mut self) -> AdmissionNext {
        let digests = (self.offer.home(), self.offer.config_digest());
        let record = BackendRecordOperation::new(self.offer.name(), digests);
        Ok(Next::Call(IoCall::child(record)))
    }

    fn next(&mut self, result: IoResult) -> AdmissionNext {
        let unavailable = |error: String| LocalExecutionError::Unavailable(error);
        match self.step {
            AdmissionStep::Record => {
                let record = result.child::<BackendConfigRecord, BackendRecordError>()?;
                let record = record.map_err(|error| unavailable(error.to_string()))?;
                self.step = AdmissionStep::Subject(record.config_generation);
                Ok(Next::Call(IoCall::child(LocalSubjectOperation::new())))
            }
            AdmissionStep::Subject(generation) => {
                let subject = result.child::<Option<NodeSubjectRecord>, SubjectScanError>()?;
                let subject = subject.map_err(|error| unavailable(error.to_string()))?;
                self.admission(generation, subject)
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum RecheckStep {
    Offers,
    Admission(Box<ExecutorOffer>),
    Stored,
}

type RecheckNext = Result<Next<()>, JobError>;

/// A device job starts under the backend values it was admitted under. A difference reruns the
/// selection rules for its backend: a pass stores the new values under the claim, a fail
/// refuses the start as retryable, so nothing is prepared or created on the backend.
#[derive(Debug, PartialEq)]
pub struct RecheckAdmission {
    job_id: JobId,
    token: Ulid,
    stored: LocalAdmission,
    spec: Arc<ExecutionSpec>,
    step: RecheckStep,
}

impl RecheckAdmission {
    /// `None` for a job without stored admission values: a receipted job is fenced by its
    /// reservation instead.
    pub fn of_job(record: &JobRecord, token: Ulid, spec: Arc<ExecutionSpec>) -> Option<Self> {
        Some(Self {
            job_id: record.job_id,
            token,
            stored: record.local_admission.clone()?,
            spec,
            step: RecheckStep::Offers,
        })
    }

    fn compared(&mut self, offer: &ExecutorOffer, current: LocalAdmission) -> RecheckNext {
        if current == self.stored {
            return Ok(Next::Done(()));
        }
        let name = &self.stored.backend;
        if let Some(verdict) = backend_refusal(offer, &self.spec) {
            warn!(job_id = %self.job_id, backend = %name, "Device job refused: its backend changed");
            return Err(JobError::retryable(format!(
                "compute backend {name} changed since admission and no longer takes the job: {}",
                verdict.reason()
            )));
        }
        let store = store_admission(self.job_id, self.token, current);
        self.step = RecheckStep::Stored;
        Ok(Next::Call(IoCall::child(store)))
    }
}

impl Operation for RecheckAdmission {
    type Output = ();
    type Error = JobError;

    fn first(&mut self) -> RecheckNext {
        Ok(Next::Call(compute_call(ComputeEffect::Offers)))
    }

    fn next(&mut self, result: IoResult) -> RecheckNext {
        let retryable = |error: String| JobError::retryable(error);
        match std::mem::replace(&mut self.step, RecheckStep::Stored) {
            RecheckStep::Offers => {
                let answer =
                    compute_answer(result).map_err(|error| retryable(error.to_string()))?;
                let ComputeAnswer::Offers(offers) = answer else {
                    return Err(retryable("no compute backend configured".to_string()));
                };
                let name = &self.stored.backend;
                let Some(offer) = offers.into_iter().find(|offer| offer.name() == name) else {
                    let message = format!("compute backend {name} is not available here");
                    return Err(retryable(message));
                };
                let admission = BackendAdmission::new(offer.clone());
                self.step = RecheckStep::Admission(Box::new(offer));
                Ok(Next::Call(IoCall::child(admission)))
            }
            RecheckStep::Admission(offer) => {
                let current = result.child::<LocalAdmission, LocalExecutionError>();
                let current = current.map_err(|error| retryable(error.to_string()))?;
                self.compared(
                    &offer,
                    current.map_err(|error| retryable(error.to_string()))?,
                )
            }
            RecheckStep::Stored => {
                let stored = result.child::<JobRecord, JobMutationError>();
                let stored = stored.map_err(|error| retryable(error.to_string()))?;
                stored
                    .map_err(|error| retryable(format!("admission values not stored: {error}")))?;
                Ok(Next::Done(()))
            }
        }
    }
}

#[cfg(test)]
#[path = "device_admission_tests.rs"]
mod tests;
