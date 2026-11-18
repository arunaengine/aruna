//! Plans the interactive session of a session job and writes its session report.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::str::FromStr;
use std::sync::Arc;
use std::time::SystemTime;

use aruna_compute::ExecutorBackend;
use aruna_compute::session::{Session, SessionConfig};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::FenceContext;
use aruna_core::compute::quota::JobReservationRecord;
use aruna_core::compute::session::{EndReason, PendingInput, StagedInput, TouchedObject};
use aruna_core::credential_encryption::CredentialEncryptionKey;
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{
    ExecutionSpec, JobError, JobId, JobRecord, JobResultPayload, SessionReportDetail,
    SessionReportRow,
};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::placement::compute_config::IDLE_AFTER_MS;
use tracing::warn;
use ulid::Ulid;

use super::credential::{MintCredentialOperation, RunIdentity, WorkspaceCredential};
use super::input_view::{view_bucket, view_entries};
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::driver::{DriverContext, drive};
use crate::jobs::lifecycle::LifecycleError;
use crate::jobs::lifecycle::held::HeldReservations;
use crate::jobs::lifecycle::ids::session_of;
use crate::jobs::store::JobMutationError;
use crate::jobs::store::job_calls::put_job_entry;
use crate::jobs::store::row_calls::read_job_record;

/// What a session's start needs to know beyond the job itself.
#[derive(Debug, PartialEq)]
pub struct SessionInputs {
    pub realm_id: RealmId,
    pub node_id: NodeId,
    /// Whether the run's backend mounts inputs.
    pub mounts: bool,
    pub encryption_key: Option<CredentialEncryptionKey>,
    pub now: SystemTime,
}

/// The settings a session opens with.
#[derive(Debug, PartialEq)]
pub struct SessionPlan {
    pub public_job_id: JobId,
    pub credential_expires_ms: u64,
    pub idle_after_ms: u64,
}

#[derive(Debug, PartialEq)]
enum PlanStep {
    Held,
    Record,
    Credential,
    Realm,
}

/// Gathers what a session job's session opens with. `None` when the job's reservations cannot
/// be read. The credential was minted while the task was prepared; this reads the same one
/// back, so the client sees when it really expires.
#[derive(Debug, PartialEq)]
pub struct SessionPlanOperation {
    job_id: JobId,
    spec: Box<ExecutionSpec>,
    bucket: String,
    asked_idle_ms: Option<u64>,
    inputs: SessionInputs,
    public_job_id: JobId,
    credential_expires_ms: u64,
    step: PlanStep,
}

impl SessionPlanOperation {
    pub fn new(
        job_id: JobId,
        spec: &ExecutionSpec,
        bucket: &str,
        asked_idle_ms: Option<u64>,
        inputs: SessionInputs,
    ) -> Self {
        Self {
            job_id,
            spec: Box::new(spec.clone()),
            bucket: bucket.to_string(),
            asked_idle_ms,
            inputs,
            public_job_id: job_id,
            credential_expires_ms: 0,
            step: PlanStep::Held,
        }
    }

    fn realm(&mut self) -> Next<Option<SessionPlan>> {
        self.step = PlanStep::Realm;
        let read = GetConfigOperation::new(self.inputs.realm_id);
        Next::Call(IoCall::child(read))
    }

    fn mint(&mut self, record: &JobRecord) -> Next<Option<SessionPlan>> {
        let inputs = &self.inputs;
        let run = RunIdentity::new(&self.spec, record, inputs.realm_id, inputs.node_id);
        let shows_view =
            inputs.mounts && view_entries(record).is_ok_and(|entries| !entries.is_empty());
        let view = shows_view.then(|| view_bucket(record.job_id));
        let mint = MintCredentialOperation::workspace(
            (&self.spec, record),
            run,
            (&self.bucket, view.as_deref()),
            (inputs.encryption_key.clone(), inputs.now),
        );
        self.step = PlanStep::Credential;
        Next::Call(IoCall::child(mint))
    }
}

impl Operation for SessionPlanOperation {
    type Output = Option<SessionPlan>;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<Self::Output>, IoError> {
        let held = HeldReservations::of_job(self.job_id);
        Ok(Next::Call(IoCall::child(held)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, IoError> {
        match self.step {
            PlanStep::Held => {
                let held = result.child::<Vec<JobReservationRecord>, LifecycleError>()?;
                let Ok(held) = held else {
                    return Ok(Next::Done(None));
                };
                if let Some(reservation) = held.first() {
                    self.public_job_id = reservation.logical_job_id;
                }
                self.step = PlanStep::Record;
                Ok(Next::Call(IoCall::child(read_job_record(self.job_id))))
            }
            PlanStep::Record => match result.child::<Option<JobRecord>, JobMutationError>()? {
                Ok(Some(record)) => Ok(self.mint(&record)),
                _ => Ok(self.realm()),
            },
            PlanStep::Credential => {
                let minted = result.child::<WorkspaceCredential, JobError>()?;
                self.credential_expires_ms =
                    minted.map_or(0, |credential| credential.expires_at_ms);
                Ok(self.realm())
            }
            PlanStep::Realm => {
                // The realm's idle wait, which the request may only shorten.
                let realm_idle = match result.child::<RealmConfigDocument, GetConfigError>()? {
                    Ok(config) => Some(config.compute.session_idle_ms),
                    Err(error) => {
                        warn!(error = %error, "Realm config unavailable for the session idle wait");
                        None
                    }
                }
                .filter(|value| *value > 0)
                .unwrap_or(IDLE_AFTER_MS);
                Ok(Next::Done(Some(SessionPlan {
                    public_job_id: self.public_job_id,
                    credential_expires_ms: self.credential_expires_ms,
                    idle_after_ms: self
                        .asked_idle_ms
                        .map_or(realm_idle, |asked| asked.min(realm_idle)),
                })))
            }
        }
    }
}

/// Registers the interactive session of a session job. `None` is an ordinary
/// run, or a node with no compute plane.
pub(super) async fn start_session(
    context: &Arc<DriverContext>,
    backend: &Arc<dyn ExecutorBackend>,
    fence: &FenceContext,
    job_id: JobId,
    spec: &ExecutionSpec,
    bucket: &str,
) -> Option<Arc<Session>> {
    let requested = session_of(spec)?;
    let registry = context.compute_handle.as_ref()?.sessions().clone();
    let net = context.net_handle.as_ref()?;
    let node_id = net.node_id();
    let inputs = SessionInputs {
        realm_id: *net.realm_id(),
        node_id,
        mounts: super::prepare::backend_mounts(context, spec),
        encryption_key: Some(net.credential_encryption_key()),
        now: SystemTime::now(),
    };
    let plan = SessionPlanOperation::new(job_id, spec, bucket, requested.idle_after_ms, inputs);
    let plan = Box::pin(drive(plan, context)).await.ok()??;
    Some(registry.open(
        SessionConfig {
            job_id: job_id.to_string(),
            public_job_id: plan.public_job_id.to_string(),
            runtime: requested.runtime,
            workspace_bucket: bucket.to_string(),
            executor_node_id: node_id.to_string(),
            idle_after_ms: plan.idle_after_ms,
            credential_expires_ms: plan.credential_expires_ms,
        },
        backend.clone(),
        fence.clone(),
    ))
}

/// What a session recorded about the objects it brought in and touched.
#[derive(Debug, Default, PartialEq)]
pub struct SessionLedger {
    pub inputs: Vec<StagedInput>,
    pub pending: Vec<PendingInput>,
    pub touched: Vec<TouchedObject>,
}

impl SessionLedger {
    pub(super) fn of(session: &Session) -> Self {
        Self {
            inputs: session.inventory(),
            pending: session.pending(),
            touched: session.touched(),
        }
    }
}

#[derive(Debug, PartialEq)]
enum ReportStep {
    Pending(PendingInput),
    Write,
}

/// Lists what the session brought into its workspace bucket and why it stopped. Cell traffic is
/// never recorded: the family log is capped per family. A write that fails ends the report.
#[derive(Debug, PartialEq)]
pub struct SessionReportOperation {
    job_id: JobId,
    token: Ulid,
    pending: VecDeque<PendingInput>,
    touched: Vec<TouchedObject>,
    reason: EndReason,
    next_input: usize,
    rows: VecDeque<SessionReportRow>,
    step: ReportStep,
}

impl SessionReportOperation {
    pub fn new(job_id: JobId, token: Ulid, ledger: SessionLedger, reason: EndReason) -> Self {
        let next_input = ledger.inputs.len();
        let rows = (ledger.inputs.into_iter().enumerate())
            .map(|(index, input)| SessionReportRow {
                entry_key: format!("input/{index:04}"),
                detail: SessionReportDetail::Input {
                    dest_key: input.dest_key,
                    bytes: input.bytes,
                    blake3: input.blake3,
                    source_node_id: input.source_node_id,
                    version_id: input.version_id,
                },
            })
            .collect();
        Self {
            job_id,
            token,
            pending: ledger.pending.into(),
            touched: ledger.touched,
            reason,
            next_input,
            rows,
            step: ReportStep::Write,
        }
    }

    /// Reads the next queued copy whose id parses, or starts writing the rows.
    fn advance(&mut self) -> Result<Next<()>, IoError> {
        while let Some(pending) = self.pending.pop_front() {
            let Ok(copy) = JobId::from_str(&pending.job_id) else {
                continue;
            };
            self.step = ReportStep::Pending(pending);
            return Ok(Next::Call(IoCall::child(read_job_record(copy))));
        }
        for (index, object) in std::mem::take(&mut self.touched).into_iter().enumerate() {
            self.rows.push_back(SessionReportRow {
                entry_key: format!("touched/{index:04}"),
                detail: SessionReportDetail::Touched {
                    bucket: object.bucket,
                    key: object.key,
                    operation: object.operation,
                },
            });
        }
        self.rows.push_back(SessionReportRow {
            entry_key: "end".to_string(),
            detail: SessionReportDetail::End {
                reason: self.reason.as_str().to_string(),
            },
        });
        self.step = ReportStep::Write;
        Ok(self.write())
    }

    fn write(&mut self) -> Next<()> {
        let Some(row) = self.rows.pop_front() else {
            return Next::Done(());
        };
        let entry = put_job_entry(self.job_id, self.token, row.entry_key.as_bytes(), &row);
        Next::Call(IoCall::child(entry))
    }

    /// A queued copy that finished counts like an inline one. One still running never reached
    /// the kernel, so it is left out.
    fn add_copy(&mut self, pending: PendingInput, record: Option<JobRecord>) {
        let Some(JobResultPayload::CopyObject {
            version_id,
            bytes,
            blake3,
        }) = record.and_then(|record| record.result)
        else {
            return;
        };
        self.rows.push_back(SessionReportRow {
            entry_key: format!("input/{:04}", self.next_input),
            detail: SessionReportDetail::Input {
                dest_key: pending.dest_key,
                bytes,
                blake3,
                source_node_id: pending.source_node_id,
                version_id,
            },
        });
        self.next_input += 1;
    }
}

impl Operation for SessionReportOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Result<Next<()>, IoError> {
        self.advance()
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        match std::mem::replace(&mut self.step, ReportStep::Write) {
            ReportStep::Pending(pending) => {
                let record = result.child::<Option<JobRecord>, JobMutationError>()?;
                self.add_copy(pending, record.ok().flatten());
                self.advance()
            }
            ReportStep::Write => match result.child::<(), JobMutationError>()? {
                Ok(()) => Ok(self.write()),
                Err(error) => {
                    let job_id = self.job_id;
                    warn!(job_id = %job_id, error = %error, "Session report write failed");
                    Ok(Next::Done(()))
                }
            },
        }
    }
}

/// Writes the report of `session`, or of a run that kept none.
pub(super) async fn report_session(
    context: &DriverContext,
    job_id: JobId,
    token: Ulid,
    session: Option<&Arc<Session>>,
    reason: EndReason,
) {
    let ledger = session.map_or_else(SessionLedger::default, |session| SessionLedger::of(session));
    let report = SessionReportOperation::new(job_id, token, ledger, reason);
    if let Err(error) = Box::pin(drive(report, context)).await {
        warn!(job_id = %job_id, error = %error, "Session report failed");
    }
}

#[cfg(test)]
#[path = "session_tests.rs"]
mod tests;
