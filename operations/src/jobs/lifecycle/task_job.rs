//! Prepares and sends a plugin task's submission from its install's receiver: keyed, without a
//! workspace bucket and admissible only until a deadline; preparing commits nothing.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::id_mint::IdMint;
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::input::ExecutionSpec;
use aruna_core::structs::execution::job::local::WorkspaceMode;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::placement::record::LabelMatch;
use aruna_core::user::id::UserId;

use super::admit::here::placement_failure;
use super::advertised::{Advertised, AdvertisementsOperation};
use super::forward::ForwardSubmissionOperation;
use super::plan::missing_label;
use super::resolve_inputs::{ResolveInputsOperation, ResolvedInputs};
use super::submission_rules::{family_view, is_device, pin_outputs};
use super::submit_external::IngressNode;
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::jobs::submit::execution::validate_execution;
use aruna_core::job_request::{
    self, RequestIdentity, SubmissionRequest, SubmissionScope, SubmitJobError, store_workspace,
};

/// A plugin task's request, resolved at the receiver and not yet paged, with its identity and
/// the nodes it goes to: this node when it holds the family, else the holders.
#[derive(Clone, Debug, PartialEq)]
pub struct TaskSubmission {
    pub request: SubmissionRequest,
    pub identity: RequestIdentity,
    pub targets: Vec<NodeId>,
}

/// The nodes a request goes to: this node when it holds the family, else the other holders.
pub fn task_targets(
    config: &RealmConfigDocument,
    identity: &RequestIdentity,
    local: NodeId,
) -> Result<Vec<NodeId>, SubmitJobError> {
    let view = family_view(config, identity)?;
    Ok(match view.holds(local) {
        true => vec![local],
        false => (view.holders().iter())
            .copied()
            .filter(|holder| *holder != local)
            .collect(),
    })
}

/// The call that sends a prepared task to exactly `targets`, in their order: admitted here when
/// this node is the one target and holds the family, else forwarded with the install account
/// vouched for by this node. `ids` mints the alias of an admission here.
pub fn send_task_job(
    submission: &TaskSubmission,
    (config, node): (&RealmConfigDocument, &IngressNode),
    token: AuthToken,
    ids: &mut IdMint,
) -> Result<IoCall, SubmitJobError> {
    let (request, identity) = (&submission.request, &submission.identity);
    let here =
        submission.targets == [node.local] && family_view(config, identity)?.holds(node.local);
    if here {
        return Ok(IoCall::child(
            node.admission(request, identity, config, ids)?,
        ));
    }
    let holders = (submission.targets.iter())
        .copied()
        .filter(|target| *target != node.local)
        .collect();
    let forward = ForwardSubmissionOperation::new(request, identity, holders, Some(token));
    Ok(IoCall::child(forward))
}

#[derive(Debug, PartialEq)]
enum PrepareStep {
    Config,
    Labels(Box<RealmConfigDocument>),
    Resolve(Box<RealmConfigDocument>),
}

/// Resolves one plugin task's submission at the receiver: its labels, its inputs at the nodes
/// that hold them, its identity and its targets. Inputs are paged only after the plugin host
/// checked the complete lists.
#[derive(Debug, PartialEq)]
pub struct PrepareTaskJob {
    spec: ExecutionSpec,
    created_by: UserId,
    scope: SubmissionScope,
    retention_ms: u64,
    admit_by: u64,
    node: IngressNode,
    labels: Vec<LabelMatch>,
    step: PrepareStep,
}

type PrepareNext = Result<Next<TaskSubmission>, SubmitJobError>;

impl PrepareTaskJob {
    /// `key` names the submission across sends, so every send is the same request.
    pub fn new(
        spec: ExecutionSpec,
        (created_by, key): (UserId, String),
        (retention_ms, admit_by): (u64, u64),
        node: IngressNode,
    ) -> Self {
        Self {
            spec,
            created_by,
            scope: SubmissionScope::Keyed(key),
            retention_ms,
            admit_by,
            node,
            labels: Vec::new(),
            step: PrepareStep::Config,
        }
    }

    fn resolve(&mut self, config: RealmConfigDocument) -> PrepareNext {
        pin_outputs(&mut self.spec, self.node.local);
        let scope =
            (self.node).resolve_scope(self.created_by, &self.scope, self.retention_ms, None);
        let resolve = ResolveInputsOperation::new(&self.spec, scope);
        self.step = PrepareStep::Resolve(Box::new(config));
        Ok(Next::Call(IoCall::child(resolve)))
    }

    fn resolved(&mut self, config: &RealmConfigDocument, resolved: ResolvedInputs) -> PrepareNext {
        // Directory inputs are stored as the files they expanded to.
        self.spec.inputs = resolved.inputs;
        let request = SubmissionRequest {
            created_by: self.created_by,
            spec: self.spec.clone(),
            scope: self.scope.clone(),
            retention_ms: self.retention_ms,
            ingress_node_id: self.node.local,
            captured_inputs: resolved.captured,
            output_policies: resolved.policies,
            input_pages: None,
            admit_by: Some(self.admit_by),
        };
        let identity = request.identity().map_err(SubmitJobError::Conversion)?;
        let targets = task_targets(config, &identity, self.node.local)?;
        Ok(Next::Done(TaskSubmission {
            request,
            identity,
            targets,
        }))
    }
}

impl Operation for PrepareTaskJob {
    type Output = TaskSubmission;
    type Error = SubmitJobError;

    fn first(&mut self) -> PrepareNext {
        validate_execution(&mut self.spec, WorkspaceMode::None, None)?;
        store_workspace(&mut self.spec, WorkspaceMode::None, None)
            .map_err(|error| SubmitJobError::InvalidWorkspace(error.to_string()))?;
        self.labels = job_request::required_labels(&self.spec)
            .map_err(|error| SubmitJobError::InvalidWorkspace(error.to_string()))?;
        let read = GetConfigOperation::new(self.node.realm_id);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> PrepareNext {
        match std::mem::replace(&mut self.step, PrepareStep::Config) {
            PrepareStep::Config => {
                let config = result.child::<RealmConfigDocument, GetConfigError>()?;
                let config = config.map_err(|_| {
                    SubmitJobError::PlacementUnavailable("realm config unavailable".to_string())
                })?;
                if is_device(&config, self.node.local) {
                    let refused = "a user device runs no plugin task".to_string();
                    return Err(SubmitJobError::InvalidWorkspace(refused));
                }
                if self.labels.is_empty() {
                    return self.resolve(config);
                }
                // Only a complete read of the advertisements proves that no executor has a label.
                let read = AdvertisementsOperation::new(&config);
                self.step = PrepareStep::Labels(Box::new(config));
                Ok(Next::Call(IoCall::child(read)))
            }
            PrepareStep::Labels(config) => {
                let read = result.child::<Advertised, IoError>()?;
                let (documents, unread) = read.map_err(SubmitJobError::from)?;
                if let Some(label) = missing_label(&documents, &self.labels).filter(|_| !unread) {
                    return Err(SubmitJobError::InvalidWorkspace(format!(
                        "no executor in the realm carries the label `{}={}`",
                        label.key, label.value
                    )));
                }
                self.resolve(*config)
            }
            PrepareStep::Resolve(config) => {
                let resolved = result.child::<ResolvedInputs, SubmitJobError>()?;
                self.resolved(&config, resolved.map_err(placement_failure)?)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::records::{Family, REALM, local_ingress, node, payload, user};
    use aruna_core::calls::ChildOutcome;

    fn child<T, E>(result: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(ChildOutcome::new(result))
    }

    fn prepare() -> PrepareTaskJob {
        let key = ("plugin-work/1".to_string(), 1000, 9_000);
        PrepareTaskJob::new(payload(), (user(), key.0), (key.1, key.2), local_ingress())
    }

    // A device runs no plugin task.
    #[test]
    fn device_refused() {
        let mut task = prepare();
        assert!(matches!(task.first(), Ok(Next::Call(IoCall::Child(_)))));
        let config = RealmConfigDocument::new(REALM, Vec::new(), 1);
        assert!(matches!(
            task.next(child(Ok::<_, GetConfigError>(config))),
            Err(SubmitJobError::InvalidWorkspace(_))
        ));
    }

    // The resolved request carries its deadline and the targets a send goes to.
    #[test]
    fn resolved_names_targets() {
        let mut config = Family::new([4; 32]).config;
        config.seed_job_control(node(1), 0);
        let mut task = prepare();
        task.first().unwrap();
        let resolve = task.next(child(Ok::<_, GetConfigError>(config)));
        assert!(matches!(resolve, Ok(Next::Call(IoCall::Child(_)))));
        let resolved = ResolvedInputs {
            inputs: Vec::new(),
            captured: Vec::new(),
            policies: Vec::new(),
        };
        let Ok(Next::Done(submission)) = task.next(child(Ok::<_, SubmitJobError>(resolved))) else {
            panic!("the request is prepared");
        };
        assert_eq!(submission.request.admit_by, Some(9_000));
        assert!(!submission.targets.is_empty());
        let scope = SubmissionScope::Keyed("plugin-work/1".to_string());
        assert_eq!(submission.request.scope, scope);
    }
}
