//! Plans one execution spec the way its launch would, without admitting it: the inputs resolve
//! at their holders and one planning round screens the realm's targets. Nothing is written.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::job_request::{self, SubmissionScope, SubmitJobError};
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::scheduling::ExecutionPlan;
use aruna_core::structs::execution::job::family::{
    JobAdmissionRecord, JobRetryPolicy, LogicalJobSpec,
};
use aruna_core::structs::execution::job::identity::{JobId, RETENTION_MS};
use aruna_core::structs::execution::job::input::ExecutionSpec;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::structured_id::{BucketId, FieldError, PlacementHandle};
use aruna_core::user::id::UserId;
use aruna_ops_identity::document_repository::{GetConfigError, GetConfigOperation};
use thiserror::Error;
use ulid::Ulid;

use super::plan::{BuildPlanOperation, PlanBuildError, PlanRound};
use super::resolve_inputs::{ResolveInputsOperation, ResolvedInputs};
use super::submit_external::IngressNode;

/// Why a preview found no plan.
#[derive(Debug, PartialEq, Error)]
pub enum PreviewError {
    #[error("the realm config is unavailable: {0}")]
    Config(String),
    #[error(transparent)]
    Inputs(#[from] SubmitJobError),
    #[error(transparent)]
    Plan(#[from] PlanBuildError),
    #[error("the preview names no job: {0}")]
    Id(String),
}

impl From<IoError> for PreviewError {
    fn from(error: IoError) -> Self {
        Self::Config(error.to_string())
    }
}

#[derive(Debug, PartialEq)]
enum PreviewStep {
    Config,
    Resolve(Box<RealmConfigDocument>),
    Plan,
}

/// Resolves the inputs of `spec` for `created_by` and plans it once, as a launch of the same
/// spec would; it admits nothing and stores nothing.
#[derive(Debug, PartialEq)]
pub struct PreviewPlanOperation {
    node: IngressNode,
    spec: ExecutionSpec,
    created_by: UserId,
    token: Option<AuthToken>,
    step: PreviewStep,
}

type PreviewNext = Result<Next<ExecutionPlan>, PreviewError>;

impl PreviewPlanOperation {
    pub fn new(node: IngressNode, spec: ExecutionSpec, created_by: UserId) -> Self {
        Self {
            node,
            spec,
            created_by,
            token: None,
            step: PreviewStep::Config,
        }
    }

    pub fn with_token(mut self, token: Option<AuthToken>) -> Self {
        self.token = token;
        self
    }

    /// The stored spec a launch of these inputs would plan from; its ids name no stored job.
    fn logical(&self, resolved: ResolvedInputs) -> Result<LogicalJobSpec, PreviewError> {
        let node = &self.node;
        let mut payload = self.spec.clone();
        payload.inputs = resolved.inputs;
        let resources = job_request::effective_resources(&payload);
        let invalid = |error: FieldError| PreviewError::Id(error.to_string());
        let handle = PlacementHandle::new(1).map_err(invalid)?;
        let bucket = BucketId::new(0).map_err(invalid)?;
        let job_id = JobId::from_parts(node.now_ms, handle, bucket, 0).map_err(invalid)?;
        let scope = SubmissionScope::Unkeyed(Ulid::nil());
        let submission_id = job_request::submission_of(self.created_by, &scope);
        Ok(LogicalJobSpec {
            submission_id,
            job_id,
            origin_node_id: node.local,
            ingress_node_id: node.local,
            realm_id: node.realm_id,
            group_id: payload.group_id,
            created_by: self.created_by,
            created_at_ms: node.now_ms,
            payload,
            request_digest: [0; 32],
            spec_digest: [0; 32],
            resources,
            retention_ms: RETENTION_MS,
            retry: JobRetryPolicy {
                launches_per_witness: 1,
            },
            admission: JobAdmissionRecord {
                submission_id,
                request_digest: [0; 32],
                job_id,
                group_id: self.spec.group_id,
                admitting_node_id: node.local,
                membership_generation: 0,
                resources,
                admitted_at_ms: node.now_ms,
            },
            captured_inputs: resolved.captured,
            input_pages: None,
            output_policies: resolved.policies,
            placement: PlacementRef {
                strategy_id: Ulid::nil(),
                shard: 0,
            },
        })
    }
}

impl Operation for PreviewPlanOperation {
    type Output = ExecutionPlan;
    type Error = PreviewError;

    fn first(&mut self) -> PreviewNext {
        let read = GetConfigOperation::new(self.node.realm_id);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> PreviewNext {
        match std::mem::replace(&mut self.step, PreviewStep::Plan) {
            PreviewStep::Config => {
                let config = result.child::<RealmConfigDocument, GetConfigError>()?;
                let config = config.map_err(|error| PreviewError::Config(error.to_string()))?;
                let scope = SubmissionScope::Unkeyed(Ulid::nil());
                let resolve_scope = (self.node)
                    .resolve_scope(self.created_by, &scope, RETENTION_MS, self.token.clone())
                    .with_config(&config);
                let resolve = ResolveInputsOperation::new(&self.spec, resolve_scope);
                self.step = PreviewStep::Resolve(Box::new(config));
                Ok(Next::Call(IoCall::child(resolve)))
            }
            PreviewStep::Resolve(config) => {
                let resolved = result.child::<ResolvedInputs, SubmitJobError>()??;
                let round = PlanRound {
                    config: *config,
                    spec: self.logical(resolved)?,
                    excluded: Vec::new(),
                    deferred: Vec::new(),
                    now_ms: self.node.now_ms,
                    local: self.node.local,
                    fresh: self.node.fresh.clone(),
                };
                self.step = PreviewStep::Plan;
                Ok(Next::Call(IoCall::child(BuildPlanOperation::new(round))))
            }
            PreviewStep::Plan => Ok(Next::Done(
                result.child::<ExecutionPlan, PlanBuildError>()??,
            )),
        }
    }

    fn expected_error(error: &PreviewError) -> bool {
        matches!(error, PreviewError::Inputs(_))
    }
}
