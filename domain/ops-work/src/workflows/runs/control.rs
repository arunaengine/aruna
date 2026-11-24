//! Resumes and repeats an ended run as a new run of its lineage: a resume replays the lineage's
//! run log so finished tasks are not run again, a repeat runs everything again in its own cache.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::workflow_run::{CacheScope, WorkflowRunRecord};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use serde_json::Value;
use ulid::Ulid;

use super::admit::{AdmittedRun, Lineage, PinnedInput, RunRequest, StartRunOperation};
use super::{RunAccessOperation, access_of};
use crate::workflows::{CatalogError, Need};

/// What a new run of a lineage does with the run it follows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Follow {
    /// Replays the lineage's run log; tasks that finished there are not run again.
    Resume,
    /// Runs the same version, values, inputs and images again, reusing no earlier result.
    Repeat,
}

/// Starts a new run that resumes or repeats an ended run. The caller needs WRITE on the group's
/// workflows and every right a start needs; a run that has not ended is a conflict.
#[derive(Debug, PartialEq)]
pub struct FollowRunOperation {
    auth: AuthContext,
    node: (NodeId, RealmId),
    source: Ulid,
    follow: Follow,
    accessed: bool,
}

impl FollowRunOperation {
    pub fn new(auth: &AuthContext, node: (NodeId, RealmId), source: Ulid, follow: Follow) -> Self {
        Self {
            auth: auth.clone(),
            node,
            source,
            follow,
            accessed: false,
        }
    }

    fn start(&self, source: &WorkflowRunRecord) -> Result<StartRunOperation, CatalogError> {
        let values: Value = crate::workflows::from_text(&source.values_json)?;
        let inputs = match source.inputs_json.as_deref() {
            Some(text) => pins_of(&crate::workflows::from_text(text)?)?,
            None => Vec::new(),
        };
        let request = RunRequest {
            group_id: source.group_id,
            workflow_id: source.workflow_id,
            version: source.version,
            values,
            inputs,
            inputs_json: source.inputs_json.clone(),
            output: source.output.clone(),
            provenance_crate: source.provenance_crate,
            idempotency_key: None,
        };
        let lineage = match self.follow {
            Follow::Resume => Lineage {
                first: Some(source.lineage),
                resumes: Some(source.run_id),
                repeats: None,
                cache_scope: source.cache_scope,
            },
            Follow::Repeat => Lineage {
                first: Some(source.lineage),
                resumes: None,
                repeats: Some(source.run_id),
                cache_scope: CacheScope::Run,
            },
        };
        Ok(StartRunOperation::new(&self.auth, self.node, request).in_lineage(lineage))
    }
}

/// The pinned inputs of a stored request: one pin or a list of pins per input name.
pub(crate) fn pins_of(inputs: &Value) -> Result<Vec<PinnedInput>, CatalogError> {
    let invalid = || CatalogError::Storage("the run's stored inputs do not read".into());
    let mut pins = Vec::new();
    for (name, value) in inputs.as_object().ok_or_else(invalid)? {
        let listed = match value {
            Value::Array(items) => items.iter().collect(),
            other => vec![other],
        };
        for pin in listed {
            let text = |field: &str| pin.get(field).and_then(Value::as_str);
            let version = text("version_id").and_then(|version| version.parse().ok());
            let (Some(bucket), Some(key), Some(version_id)) =
                (text("bucket"), text("key"), version)
            else {
                return Err(invalid());
            };
            let source_node_id = match text("source_node_id") {
                Some(node) => Some(node.parse().map_err(|_| invalid())?),
                None => None,
            };
            pins.push(PinnedInput {
                name: name.clone(),
                bucket: bucket.to_owned(),
                key: key.to_owned(),
                version_id,
                source_node_id,
            });
        }
    }
    Ok(pins)
}

impl Operation for FollowRunOperation {
    type Output = WorkflowRunRecord;
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<WorkflowRunRecord>, CatalogError> {
        let access = RunAccessOperation::new(&self.auth, self.source, Need::Write);
        Ok(Next::Call(IoCall::child(access)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<WorkflowRunRecord>, CatalogError> {
        if !std::mem::replace(&mut self.accessed, true) {
            let (source, _) = access_of(result)?;
            if source.state.is_active() {
                return Err(CatalogError::Conflict(format!(
                    "the run is still {}",
                    source.state.as_str()
                )));
            }
            let start = self.start(&source)?;
            return Ok(Next::Call(IoCall::child(start)));
        }
        let admitted = result.child::<AdmittedRun, CatalogError>()??;
        Ok(Next::Done(admitted.record))
    }

    fn expected_error(error: &CatalogError) -> bool {
        matches!(
            error,
            CatalogError::NotFound
                | CatalogError::Forbidden
                | CatalogError::Conflict(_)
                | CatalogError::Invalid(_)
        )
    }
}
