//! Takes in one external submission: the receiver commits it when its view makes it a holder.
//! Otherwise it forwards the submission one hop and writes nothing; a device always forwards.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::heartbeat::HeldHeartbeat;
use aruna_core::id::NodeId;
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{ExecutionSpec, PageManifest, WorkspaceMode};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::placement::record::LabelMatch;

use super::admit_here::{
    AcceptedSubmission, AdmitHereOperation, LocalAdmission, placement_failure,
};
use super::advertised::{Advertised, AdvertisementsOperation};
use super::forward::ForwardSubmissionOperation;
use super::ids::{self, RequestIdentity, SubmissionRequest, SubmissionScope, store_workspace};
use super::input_walk::InputCaller;
use super::plan::missing_label;
use super::resolve_inputs::{ResolveInputsOperation, ResolveScope, ResolvedInputs};
use super::submission_rules::{caller_of, family_view, is_device, pin_outputs, reference_shape};
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::jobs::records::pages::{PageReadError, StoreEntriesOperation};
use crate::jobs::records::verify::FamilyView;
use crate::jobs::submit::{SubmitJobError, mint_configured_job};
use crate::jobs::submit_execution::validate_execution;

/// This node as a submission sees it: its identity, its clock and its fresh heartbeats.
#[derive(Clone, Debug, PartialEq)]
pub struct IngressNode {
    pub local: NodeId,
    pub realm_id: RealmId,
    pub now_ms: u64,
    pub fresh: BTreeMap<NodeId, HeldHeartbeat>,
}

impl IngressNode {
    /// The walk's caller for `created_by` on this node.
    pub(super) fn input_caller(&self, created_by: UserId) -> InputCaller {
        InputCaller {
            created_by,
            realm_id: self.realm_id,
            local: self.local,
        }
    }

    /// The resolve of `request`'s inputs here for its submitter.
    pub(super) fn resolve_scope(
        &self,
        created_by: UserId,
        scope: &SubmissionScope,
        retention_ms: u64,
        token: Option<AuthToken>,
    ) -> ResolveScope {
        let (_, submission_id, expires_at) =
            caller_of(created_by, scope, retention_ms, self.now_ms);
        ResolveScope {
            caller: self.input_caller(created_by),
            submission_id,
            expires_at,
            token,
        }
    }

    /// Mints the alias of this accept and admits the submission here.
    pub(super) fn admission(
        &self,
        request: &SubmissionRequest,
        identity: &RequestIdentity,
        config: &RealmConfigDocument,
    ) -> Result<AdmitHereOperation, SubmitJobError> {
        let subject = identity.submission_id.0.as_slice();
        let job_id = mint_configured_job(config, self.local, subject)?;
        Ok(AdmitHereOperation::new(LocalAdmission {
            request: request.clone(),
            identity: *identity,
            config: config.clone(),
            local: self.local,
            job_id,
            now_ms: self.now_ms,
            fresh: self.fresh.clone(),
        }))
    }
}

/// One external execution request as its route received it.
#[derive(Debug, PartialEq)]
pub struct ExternalSubmission {
    pub spec: ExecutionSpec,
    pub created_by: UserId,
    pub scope: SubmissionScope,
    pub workspace_mode: WorkspaceMode,
    pub workspace_bucket: Option<String>,
    pub retention_ms: u64,
    pub auth_token: Option<AuthToken>,
}

#[derive(Debug, PartialEq)]
enum SubmitStep {
    Config,
    Labels(Box<RealmConfigDocument>),
    Resolve(Box<RealmConfigDocument>),
    Store(Box<(RealmConfigDocument, SubmissionRequest, RequestIdentity)>),
    Accept,
}

/// Normalizes, authorizes and admits one external execution request.
#[derive(Debug, PartialEq)]
pub struct SubmitExternalOperation {
    submission: ExternalSubmission,
    node: Option<IngressNode>,
    labels: Vec<LabelMatch>,
    step: SubmitStep,
}

type SubmitNext = Result<Next<AcceptedSubmission>, SubmitJobError>;

fn unavailable(reason: &str) -> SubmitJobError {
    SubmitJobError::PlacementUnavailable(reason.to_string())
}

impl SubmitExternalOperation {
    /// `node` is `None` on a node without a network, which cannot admit anything.
    pub fn new(submission: ExternalSubmission, node: Option<IngressNode>) -> Self {
        Self {
            submission,
            node,
            labels: Vec::new(),
            step: SubmitStep::Config,
        }
    }

    fn node(&self) -> Result<&IngressNode, SubmitJobError> {
        self.node
            .as_ref()
            .ok_or_else(|| unavailable("network handle unavailable"))
    }

    fn request(&self, local: NodeId) -> SubmissionRequest {
        let submission = &self.submission;
        SubmissionRequest {
            created_by: submission.created_by,
            spec: submission.spec.clone(),
            scope: submission.scope.clone(),
            retention_ms: submission.retention_ms,
            ingress_node_id: local,
            captured_inputs: Vec::new(),
            output_policies: Vec::new(),
            input_pages: None,
        }
    }

    fn forward(
        &mut self,
        request: &SubmissionRequest,
        identity: &RequestIdentity,
        view: &FamilyView,
    ) -> SubmitNext {
        let local = self.node()?.local;
        let holders = view.holders().iter().copied();
        let holders = holders.filter(|holder| *holder != local).collect();
        let token = self.submission.auth_token.clone();
        self.step = SubmitStep::Accept;
        let forward = ForwardSubmissionOperation::new(request, identity, holders, token);
        Ok(Next::Call(IoCall::child(forward)))
    }

    /// A device forwards without resolving anything: it holds none of the objects it names, so
    /// only the reference shape is checked and the holder resolves the rest.
    fn forward_device(&mut self, config: &RealmConfigDocument) -> SubmitNext {
        let request = self.request(self.node()?.local);
        reference_shape(&request.spec)?;
        // A device may not assert an auth context for the realm, so the caller's own
        // bearer token is the only credential a holder accepts from it.
        if !matches!(self.submission.auth_token, Some(AuthToken::Bearer(_))) {
            return Err(SubmitJobError::AuthorityDenied);
        }
        let identity = request.identity().map_err(SubmitJobError::Conversion)?;
        let view = family_view(config, &identity)?;
        self.forward(&request, &identity, &view)
    }

    fn configured(&mut self, config: RealmConfigDocument) -> SubmitNext {
        if is_device(&config, self.node()?.local) {
            return self.forward_device(&config);
        }
        if self.labels.is_empty() {
            return self.resolve(config);
        }
        // Only a complete read of the advertisements proves that no executor carries a label.
        let read = AdvertisementsOperation::new(&config);
        self.step = SubmitStep::Labels(Box::new(config));
        Ok(Next::Call(IoCall::child(read)))
    }

    fn labelled(&mut self, config: RealmConfigDocument, read: Advertised) -> SubmitNext {
        let (documents, unread) = read;
        if let Some(label) = missing_label(&documents, &self.labels).filter(|_| !unread) {
            return Err(SubmitJobError::InvalidWorkspace(format!(
                "no executor in the realm carries the label `{}={}`",
                label.key, label.value
            )));
        }
        self.resolve(config)
    }

    fn resolve(&mut self, config: RealmConfigDocument) -> SubmitNext {
        let node = self.node()?.clone();
        pin_outputs(&mut self.submission.spec, node.local);
        let submission = &self.submission;
        let scope = node.resolve_scope(
            submission.created_by,
            &submission.scope,
            submission.retention_ms,
            submission.auth_token.clone(),
        );
        let resolve = ResolveInputsOperation::new(&submission.spec, scope);
        self.step = SubmitStep::Resolve(Box::new(config));
        Ok(Next::Call(IoCall::child(resolve)))
    }

    fn resolved(&mut self, config: RealmConfigDocument, resolved: ResolvedInputs) -> SubmitNext {
        // Directory inputs are stored as the files they expanded to.
        self.submission.spec.inputs = resolved.inputs;
        let mut request = self.request(self.node()?.local);
        request.captured_inputs = resolved.captured;
        request.output_policies = resolved.policies;
        let identity = request.identity().map_err(SubmitJobError::Conversion)?;
        let store = StoreEntriesOperation::request(
            identity.submission_id,
            &request.spec.inputs,
            &request.captured_inputs,
        );
        self.step = SubmitStep::Store(Box::new((config, request, identity)));
        Ok(Next::Call(IoCall::child(store)))
    }

    fn stored(
        &mut self,
        (config, mut request, identity): (RealmConfigDocument, SubmissionRequest, RequestIdentity),
        manifest: Option<PageManifest>,
    ) -> SubmitNext {
        if manifest.is_some() {
            request.spec.inputs.clear();
            request.captured_inputs.clear();
        }
        request.input_pages = manifest;
        let view = family_view(&config, &identity)?;
        let node = self.node()?;
        if !view.holds(node.local) {
            return self.forward(&request, &identity, &view);
        }
        let admit = node.admission(&request, &identity, &config)?;
        self.step = SubmitStep::Accept;
        Ok(Next::Call(IoCall::child(admit)))
    }
}

impl Operation for SubmitExternalOperation {
    type Output = AcceptedSubmission;
    type Error = SubmitJobError;

    fn first(&mut self) -> SubmitNext {
        let submission = &mut self.submission;
        let bucket = submission.workspace_bucket.clone();
        let mode = submission.workspace_mode;
        validate_execution(&mut submission.spec, mode, bucket.as_deref())?;
        store_workspace(&mut submission.spec, mode, bucket)
            .map_err(|error| SubmitJobError::InvalidWorkspace(error.to_string()))?;
        self.labels = ids::required_labels(&self.submission.spec)
            .map_err(|error| SubmitJobError::InvalidWorkspace(error.to_string()))?;
        let read = GetConfigOperation::new(self.node()?.realm_id);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> SubmitNext {
        match std::mem::replace(&mut self.step, SubmitStep::Accept) {
            SubmitStep::Config => {
                let config = result.child::<RealmConfigDocument, GetConfigError>()?;
                let config = config.map_err(|_| unavailable("realm config unavailable"))?;
                self.configured(config)
            }
            SubmitStep::Labels(config) => {
                let read = result.child::<Advertised, IoError>()?;
                let read = read
                    .map_err(SubmitJobError::from)
                    .map_err(placement_failure)?;
                self.labelled(*config, read)
            }
            SubmitStep::Resolve(config) => {
                let resolved = result.child::<ResolvedInputs, SubmitJobError>()?;
                self.resolved(*config, resolved.map_err(placement_failure)?)
            }
            SubmitStep::Store(state) => {
                let stored = result.child::<Option<PageManifest>, PageReadError>()?;
                let manifest = stored
                    .map_err(|error| SubmitJobError::PlacementUnavailable(error.to_string()))?;
                self.stored(*state, manifest)
            }
            SubmitStep::Accept => {
                let accepted = result.child::<AcceptedSubmission, SubmitJobError>()?;
                Ok(Next::Done(accepted.map_err(placement_failure)?))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::records::{Family, REALM, node, payload, user};
    use aruna_core::calls::ChildOutcome;
    use aruna_core::errors::StorageError;

    fn submission(token: Option<AuthToken>) -> ExternalSubmission {
        ExternalSubmission {
            spec: payload(),
            created_by: user(),
            scope: SubmissionScope::Keyed("k".to_string()),
            workspace_mode: WorkspaceMode::None,
            workspace_bucket: None,
            retention_ms: 1000,
            auth_token: token,
        }
    }

    fn here() -> IngressNode {
        IngressNode {
            local: node(1),
            realm_id: REALM,
            now_ms: 0,
            fresh: BTreeMap::new(),
        }
    }

    fn operation() -> SubmitExternalOperation {
        SubmitExternalOperation::new(submission(None), Some(here()))
    }

    fn child<T, E>(result: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(ChildOutcome::new(result))
    }

    fn calls_child(next: SubmitNext) {
        assert!(matches!(next, Ok(Next::Call(IoCall::Child(_)))), "{next:?}");
    }

    // A node without a network admits nothing, and says so as a retryable failure.
    #[test]
    fn needs_network() {
        let mut submit = SubmitExternalOperation::new(submission(None), None);
        assert!(matches!(
            submit.first(),
            Err(SubmitJobError::PlacementUnavailable(_))
        ));
    }

    // An unreadable realm config is retryable.
    #[test]
    fn config_failure_retries() {
        let mut submit = operation();
        calls_child(submit.first());
        let failed = GetConfigError::StorageError(StorageError::Timeout);
        assert!(matches!(
            submit.next(child(Err::<RealmConfigDocument, _>(failed))),
            Err(SubmitJobError::PlacementUnavailable(_))
        ));
    }

    // A device needs the caller's own bearer, the only credential a holder accepts from it.
    #[test]
    fn device_needs_bearer() {
        let mut submit = operation();
        calls_child(submit.first());
        let config = RealmConfigDocument::new(REALM, Vec::new(), 1);
        assert_eq!(
            submit.next(child(Ok::<_, GetConfigError>(config))),
            Err(SubmitJobError::AuthorityDenied)
        );
    }

    // A realm node resolves the inputs, stores what does not fit, then accepts or forwards.
    #[test]
    fn realm_node_flow() {
        let mut config = Family::new([4; 32]).config;
        config.seed_job_control(node(1), 0);
        let mut submit = operation();
        calls_child(submit.first());
        calls_child(submit.next(child(Ok::<_, GetConfigError>(config))));
        assert!(matches!(submit.step, SubmitStep::Resolve(_)));
        let resolved = ResolvedInputs {
            inputs: Vec::new(),
            captured: Vec::new(),
            policies: Vec::new(),
        };
        calls_child(submit.next(child(Ok::<_, SubmitJobError>(resolved))));
        assert!(matches!(submit.step, SubmitStep::Store(_)));
        calls_child(submit.next(child(Ok::<Option<PageManifest>, PageReadError>(None))));
        assert_eq!(submit.step, SubmitStep::Accept);
    }

    // Only a complete read proves that no executor carries a required label.
    #[test]
    fn label_needs_full_read() {
        let config = Family::new([4; 32]).config;
        let answer = |unread| {
            let mut submit = operation();
            submit.labels = vec![LabelMatch {
                key: "gpu".to_string(),
                value: "yes".to_string(),
            }];
            submit.step = SubmitStep::Labels(Box::new(config.clone()));
            submit.next(child(Ok::<Advertised, IoError>((BTreeMap::new(), unread))))
        };
        assert!(matches!(
            answer(false),
            Err(SubmitJobError::InvalidWorkspace(_))
        ));
        calls_child(answer(true));
    }
}
