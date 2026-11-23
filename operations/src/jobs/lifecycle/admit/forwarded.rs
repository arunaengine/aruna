//! Serves one forwarded submission: token, identity and this node's holder standing are
//! revalidated before any commit, and the request is never forwarded a second hop.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::events::PeerAuthRefusal;
use aruna_core::id::NodeId;
use aruna_core::id_mint::IdMint;
use aruna_core::jobs::{SubmissionAck, SubmissionRefusal};
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::plugin_installs::PluginInstall;
use aruna_core::structs::execution::job::family::SubmissionId;
use aruna_core::structs::execution::job::input::{plugin_tag, tagged};
use aruna_core::structs::execution::job::pages::{InputPage, PageManifest, spec_entries};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::storage::blob::group_permission_path;
use aruna_core::types::GroupId;
use tracing::warn;

use super::super::resolve_inputs::{ResolveInputsOperation, ResolvedInputs};
use super::super::submission_rules::{
    forwarded_request_fits, is_device, peer_refusal, pin_outputs, refusal_of,
};
use super::super::submit_external::IngressNode;
use super::here::{AcceptedSubmission, placement_failure};
use crate::jobs::lifecycle::screen::{ScreenError, ScreenTask};
use crate::jobs::records::pages::{LoadChainOperation, PageReadError, StoreEntriesOperation};
use crate::jobs::records::verify::FamilyView;
use aruna_core::job_request::{RequestIdentity, SubmissionRequest, SubmitJobError};
use aruna_core::request_policy::{PolicyRequestExtras, policy_request_with};
use aruna_ops_identity::auth::check_permissions::CheckPermissionsConfig;
use aruna_ops_identity::auth::enforcement::{compiled_policies, policy_reads, read_policies};
use aruna_ops_identity::document_repository::{GetConfigError, GetConfigOperation};
use aruna_ops_identity::forward::write_peer::WritePeerOperation;
use aruna_ops_identity::realm::peer_trust::peer_acts_for;

/// Whether the caller may write in a group: its grants, then the request policies.
#[derive(Debug, PartialEq)]
pub(in crate::jobs::lifecycle) struct GroupWriteOperation {
    check: CheckPermissionsConfig,
    group_id: GroupId,
    compiling: bool,
}

impl GroupWriteOperation {
    pub(in crate::jobs::lifecycle) fn new(
        auth: &AuthContext,
        group_id: GroupId,
        local: NodeId,
    ) -> Self {
        let check = CheckPermissionsConfig {
            auth_context: auth.clone(),
            path: group_permission_path(auth.realm_id, group_id, local),
            required_permission: Permission::WRITE,
        };
        Self {
            check,
            group_id,
            compiling: false,
        }
    }
}

impl Operation for GroupWriteOperation {
    type Output = bool;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<bool>, IoError> {
        let Ok(mut reads) = self.check.reads() else {
            return Ok(Next::Done(false));
        };
        let realm_id = self.check.auth_context.realm_id;
        reads.extend(policy_reads(realm_id, Some(self.group_id)));
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, IoError> {
        if self.compiling {
            return Ok(Next::Done(self.policies_allow(result)));
        }
        let IoResult::Values(mut rows) = result else {
            let got = result.label();
            let expected = "IoResult::Values";
            return Err(IoError::Unexpected { expected, got });
        };
        let Ok(split) = self.check.reads().map(|reads| reads.len()) else {
            return Ok(Next::Done(false));
        };
        let policies = rows.split_off(split.min(rows.len()));
        if !self.check.allowed(rows).unwrap_or(false) {
            return Ok(Next::Done(false));
        }
        let realm_id = self.check.auth_context.realm_id;
        let Ok(sets) = read_policies(realm_id, Some(self.group_id), policies) else {
            return Ok(Next::Done(false));
        };
        self.compiling = true;
        Ok(Next::Call(sets.compile()))
    }
}

impl GroupWriteOperation {
    /// Whether the compiled request policies allow the write.
    fn policies_allow(&self, result: IoResult) -> bool {
        let Ok(evaluator) = compiled_policies(result) else {
            return false;
        };
        let request = policy_request_with(
            &self.check.path,
            &Permission::WRITE,
            Some(&self.check.auth_context),
            PolicyRequestExtras::rest(),
        );
        evaluator.evaluate(&request).is_ok()
    }
}

#[derive(Debug, PartialEq)]
enum ForwardedStep {
    Token,
    Config(Box<AuthContext>),
    Group(Box<RealmConfigDocument>),
    Pages(Box<RealmConfigDocument>),
    Device(Box<RealmConfigDocument>),
    Screen(Box<RealmConfigDocument>),
    /// A plugin task's grants checked again on the inputs its pages brought.
    Rescreen(Box<RealmConfigDocument>),
    Store(Box<(RealmConfigDocument, RequestIdentity)>),
    Admit,
}

/// Admits one submission another node forwarded here.
#[derive(Debug, PartialEq)]
pub struct AdmitForwardedOperation {
    peer: NodeId,
    token: AuthToken,
    submission_id: SubmissionId,
    request: SubmissionRequest,
    node: Option<IngressNode>,
    ids: IdMint,
    step: ForwardedStep,
}

type ForwardedNext = Result<Next<SubmissionAck>, SubmissionRefusal>;

impl AdmitForwardedOperation {
    pub fn new(
        peer: NodeId,
        token: AuthToken,
        submission_id: SubmissionId,
        request: SubmissionRequest,
        node: Option<IngressNode>,
    ) -> Self {
        Self {
            peer,
            token,
            submission_id,
            request,
            node,
            ids: IdMint::default(),
            step: ForwardedStep::Token,
        }
    }

    fn node(&self) -> Result<&IngressNode, SubmissionRefusal> {
        self.node.as_ref().ok_or(SubmissionRefusal::Unavailable)
    }

    fn authorized(&mut self, auth: AuthContext) -> ForwardedNext {
        // The forwarded request keeps its own submitter: a relay may not re-attribute
        // a plan to another caller, and the identity is recomputed from that caller.
        if auth.user_id != self.request.created_by {
            return Err(SubmissionRefusal::Unauthorized);
        }
        let read = GetConfigOperation::new(self.node()?.realm_id);
        self.step = ForwardedStep::Config(Box::new(auth));
        Ok(Next::Call(IoCall::child(read)))
    }

    fn configured(&mut self, auth: &AuthContext, config: RealmConfigDocument) -> ForwardedNext {
        // The submitter may belong to another realm, but must be signed in to this one.
        if auth.realm_id != config.realm_id {
            return Err(SubmissionRefusal::Unauthorized);
        }
        // A user device is owner-bound: whatever token it presents, the submission it
        // forwards must be its owner's own. Its inputs resolve unrestricted, so the token must be.
        if !peer_acts_for(&config, self.peer, auth.user_id) || auth.path_restrictions.is_some() {
            return Err(SubmissionRefusal::Unauthorized);
        }
        // A plugin task passes the replicated task check, sent by its install's receiver only.
        if tagged(&self.request.spec).is_some() {
            let request = &self.request;
            let task = (&request.spec, request.created_by, request.ingress_node_id);
            let screen = ScreenTask::new(std::sync::Arc::new(config.clone()), task);
            self.step = ForwardedStep::Screen(Box::new(config));
            return Ok(Next::Call(IoCall::child(screen)));
        }
        let group_id = self.request.spec.group_id;
        let check = GroupWriteOperation::new(auth, group_id, self.node()?.local);
        self.step = ForwardedStep::Group(Box::new(config));
        Ok(Next::Call(IoCall::child(check)))
    }

    fn screened(&mut self, config: RealmConfigDocument, install: PluginInstall) -> ForwardedNext {
        if install.receiver != self.peer || self.request.ingress_node_id != self.peer {
            return Err(SubmissionRefusal::Unauthorized);
        }
        self.load_pages(config)
    }

    /// Paged inputs come from the forwarding node, checked page by page against the manifest.
    fn load_pages(&mut self, config: RealmConfigDocument) -> ForwardedNext {
        let Some(manifest) = self.request.input_pages.take() else {
            return self.normalize(config);
        };
        let load = LoadChainOperation::new(self.submission_id, manifest, vec![self.peer]);
        self.step = ForwardedStep::Pages(Box::new(config));
        Ok(Next::Call(IoCall::child(load)))
    }

    fn with_pages(&mut self, config: RealmConfigDocument, pages: Vec<InputPage>) -> ForwardedNext {
        let entries = spec_entries(pages).map_err(|_| SubmissionRefusal::IdentityMismatch)?;
        let (inputs, captured): (Vec<_>, Vec<_>) = entries.into_iter().unzip();
        self.request
            .spec
            .inputs
            .retain(|input| input.source.is_inline());
        self.request.spec.inputs.extend(inputs);
        self.request.captured_inputs = captured;
        // The first check saw only the inline inputs; the paged ones need their grants too.
        if tagged(&self.request.spec).is_some() {
            let request = &self.request;
            let task = (&request.spec, request.created_by, request.ingress_node_id);
            let screen = ScreenTask::new(std::sync::Arc::new(config.clone()), task);
            self.step = ForwardedStep::Rescreen(Box::new(config));
            return Ok(Next::Call(IoCall::child(screen)));
        }
        self.normalize(config)
    }

    /// The holder owns what it writes and reads, so a device submission is normalized here
    /// rather than trusted from a node that holds none of it.
    fn normalize(&mut self, config: RealmConfigDocument) -> ForwardedNext {
        if !is_device(&config, self.peer) {
            return self.check_request(config, self.peer);
        }
        let node = self.node()?.clone();
        let request = &mut self.request;
        pin_outputs(&mut request.spec, node.local);
        let scope = node.resolve_scope(
            request.created_by,
            &request.scope,
            request.retention_ms,
            Some(self.token.clone()),
        );
        let resolve = ResolveInputsOperation::new(&request.spec, scope);
        self.step = ForwardedStep::Device(Box::new(config));
        Ok(Next::Call(IoCall::child(resolve)))
    }

    fn device_resolved(
        &mut self,
        config: RealmConfigDocument,
        resolved: ResolvedInputs,
    ) -> ForwardedNext {
        let local = self.node()?.local;
        let request = &mut self.request;
        request.spec.inputs = resolved.inputs;
        request.captured_inputs = resolved.captured;
        request.output_policies = resolved.policies;
        request.ingress_node_id = local;
        self.check_request(config, local)
    }

    fn check_request(&mut self, config: RealmConfigDocument, ingress: NodeId) -> ForwardedNext {
        if !forwarded_request_fits(&self.request, ingress) {
            return Err(SubmissionRefusal::IdentityMismatch);
        }
        let identity = self
            .request
            .identity()
            .map_err(|error| SubmissionRefusal::Invalid {
                reason: error.to_string(),
            })?;
        if identity.submission_id != self.submission_id {
            return Err(SubmissionRefusal::IdentityMismatch);
        }
        let view = FamilyView::resolve(&config, config.realm_id, identity.family())
            .ok_or(SubmissionRefusal::Unavailable)?;
        if !view.holds(self.node()?.local) {
            return Err(SubmissionRefusal::NotHolder);
        }
        let store = StoreEntriesOperation::request(
            identity.submission_id,
            &self.request.spec.inputs,
            &self.request.captured_inputs,
        );
        self.step = ForwardedStep::Store(Box::new((config, identity)));
        Ok(Next::Call(IoCall::child(store)))
    }

    fn admit(
        &mut self,
        config: &RealmConfigDocument,
        identity: &RequestIdentity,
        manifest: Option<PageManifest>,
    ) -> ForwardedNext {
        if manifest.is_some() {
            self.request
                .spec
                .inputs
                .retain(|input| input.source.is_inline());
            self.request.captured_inputs.clear();
        }
        self.request.input_pages = manifest;
        let plugin_task = plugin_tag(&self.request.spec.tags).is_some();
        let mut mint = self.ids;
        let admit = self
            .node()?
            .admission(&self.request, identity, config, &mut mint);
        self.ids = mint;
        let admit = admit.map_err(|error| admission_refusal(error, plugin_task))?;
        self.step = ForwardedStep::Admit;
        Ok(Next::Call(IoCall::child(admit)))
    }
}

/// A failed task check commits nothing, so the sender may count it as a refusal.
fn screen_refusal(error: ScreenError) -> SubmissionRefusal {
    match error {
        ScreenError::Unavailable(_) => SubmissionRefusal::Unavailable,
        refused => SubmissionRefusal::Invalid {
            reason: refused.to_string(),
        },
    }
}

fn admission_refusal(error: SubmitJobError, plugin_task: bool) -> SubmissionRefusal {
    match placement_failure(error) {
        SubmitJobError::JobPlanConflict { existing_job_id } => {
            SubmissionRefusal::Conflict { existing_job_id }
        }
        // A plugin task's sender must tell a refusal that committed nothing from a lost answer.
        error @ (SubmitJobError::QuotaDenied(_) | SubmitJobError::InvalidWorkspace(_))
            if plugin_task =>
        {
            SubmissionRefusal::Invalid {
                reason: error.to_string(),
            }
        }
        error => {
            warn!(error = %error, "Forwarded submission could not be admitted");
            SubmissionRefusal::Unavailable
        }
    }
}

impl Operation for AdmitForwardedOperation {
    type Output = SubmissionAck;
    type Error = SubmissionRefusal;

    fn seed(&mut self, entropy: u128) {
        self.ids = IdMint::new(entropy);
    }

    fn first(&mut self) -> ForwardedNext {
        let check = WritePeerOperation::new(self.peer, self.token.clone());
        Ok(Next::Call(IoCall::child(check)))
    }

    fn next(&mut self, result: IoResult) -> ForwardedNext {
        match std::mem::replace(&mut self.step, ForwardedStep::Admit) {
            ForwardedStep::Token => {
                let auth = result.child::<AuthContext, PeerAuthRefusal>()?;
                self.authorized(auth.map_err(peer_refusal)?)
            }
            ForwardedStep::Config(auth) => {
                let config = result.child::<RealmConfigDocument, GetConfigError>()?;
                let config = config.map_err(|_| SubmissionRefusal::Unavailable)?;
                self.configured(&auth, config)
            }
            ForwardedStep::Screen(config) => {
                let install = result.child::<PluginInstall, ScreenError>()?;
                self.screened(*config, install.map_err(screen_refusal)?)
            }
            ForwardedStep::Rescreen(config) => {
                let install = result.child::<PluginInstall, ScreenError>()?;
                install.map_err(screen_refusal)?;
                self.normalize(*config)
            }
            ForwardedStep::Group(config) => match result.child::<bool, IoError>()? {
                Ok(true) => self.load_pages(*config),
                Ok(false) | Err(_) => Err(SubmissionRefusal::Unauthorized),
            },
            ForwardedStep::Pages(config) => {
                let pages = result.child::<Vec<InputPage>, PageReadError>()?;
                let pages = pages.map_err(|_| SubmissionRefusal::Unavailable)?;
                self.with_pages(*config, pages)
            }
            ForwardedStep::Device(config) => {
                let resolved = result.child::<ResolvedInputs, SubmitJobError>()?;
                let resolved = resolved.map_err(|error| refusal_of(placement_failure(error)))?;
                self.device_resolved(*config, resolved)
            }
            ForwardedStep::Store(state) => {
                let stored = result.child::<Option<PageManifest>, PageReadError>()?;
                let manifest = stored.map_err(|_| SubmissionRefusal::Unavailable)?;
                let (config, identity) = *state;
                self.admit(&config, &identity, manifest)
            }
            ForwardedStep::Admit => {
                let accepted = result.child::<AcceptedSubmission, SubmitJobError>()?;
                let plugin_task = plugin_tag(&self.request.spec.tags).is_some();
                let accepted = accepted.map_err(|error| admission_refusal(error, plugin_task))?;
                Ok(Next::Done(SubmissionAck {
                    job_id: accepted.job_id,
                    created: accepted.created,
                    state: accepted.state,
                }))
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::tests::records::{Family, REALM, local_ingress, node, payload, user};
    use aruna_core::calls::ChildOutcome;
    use aruna_core::job_request::SubmissionScope;
    use aruna_core::structs::execution::job::identity::JobId;
    use aruna_core::user::id::UserId;
    use ulid::Ulid;

    fn request() -> SubmissionRequest {
        SubmissionRequest {
            created_by: user(),
            spec: payload(),
            scope: SubmissionScope::Keyed("k".to_string()),
            retention_ms: 1000,
            ingress_node_id: node(2),
            captured_inputs: Vec::new(),
            output_policies: Vec::new(),
            input_pages: None,
            admit_by: None,
        }
    }

    fn auth(user_id: UserId) -> AuthContext {
        AuthContext::new(user_id, REALM)
    }

    fn operation(node_view: Option<IngressNode>) -> AdmitForwardedOperation {
        let token = AuthToken::bearer("token").unwrap();
        let id = request().identity().unwrap().submission_id;
        AdmitForwardedOperation::new(node(2), token, id, request(), node_view)
    }

    fn child<T, E>(result: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(ChildOutcome::new(result))
    }

    fn calls_child(next: ForwardedNext) {
        assert!(matches!(next, Ok(Next::Call(IoCall::Child(_)))), "{next:?}");
    }

    // A relay may not attribute the plan to another caller.
    #[test]
    fn refuses_other_submitter() {
        let mut admit = operation(Some(local_ingress()));
        calls_child(admit.first());
        let other = auth(UserId::local(Ulid::from_parts(9, 9), REALM));
        assert_eq!(
            admit.next(child(Ok::<_, PeerAuthRefusal>(other))),
            Err(SubmissionRefusal::Unauthorized)
        );
    }

    // A node without a network cannot take the submission.
    #[test]
    fn needs_network() {
        let mut admit = operation(None);
        calls_child(admit.first());
        assert_eq!(
            admit.next(child(Ok::<_, PeerAuthRefusal>(auth(user())))),
            Err(SubmissionRefusal::Unavailable)
        );
    }

    // Without write access to the group, or when that cannot be decided, the caller is refused.
    #[test]
    fn group_check_refuses() {
        let answer = |result: Result<bool, IoError>| {
            let mut admit = operation(Some(local_ingress()));
            let config = Family::new([4; 32]).config;
            admit.step = ForwardedStep::Group(Box::new(config));
            admit.next(child(result))
        };
        assert_eq!(answer(Ok(false)), Err(SubmissionRefusal::Unauthorized));
        assert_eq!(
            answer(Err(IoError::Unfinished)),
            Err(SubmissionRefusal::Unauthorized)
        );
    }

    // A recomputed identity that differs from the forwarded one is refused.
    #[test]
    fn identity_must_match() {
        let mut admit = operation(Some(local_ingress()));
        admit.submission_id = SubmissionId([9; 32]);
        let config = Family::new([4; 32]).config;
        assert_eq!(
            admit.check_request(config, node(2)),
            Err(SubmissionRefusal::IdentityMismatch)
        );
    }

    // A plan conflict is told to the forwarder; other failures only say unavailable.
    #[test]
    fn admission_outcomes() {
        let answer = |error| {
            let mut admit = operation(Some(local_ingress()));
            admit.step = ForwardedStep::Admit;
            admit.next(child(Err::<AcceptedSubmission, _>(error)))
        };
        let existing_job_id = JobId::from_bytes([7; 16]);
        assert_eq!(
            answer(SubmitJobError::JobPlanConflict { existing_job_id }),
            Err(SubmissionRefusal::Conflict { existing_job_id })
        );
        assert_eq!(
            answer(SubmitJobError::UnexpectedEvent("lost".to_string())),
            Err(SubmissionRefusal::Unavailable)
        );
    }

    // A plugin task's paged inputs pass the task check again once they are loaded, so a grant
    // withdrawn since the sender prepared them refuses the submission before admission.
    #[test]
    fn paged_task_rescreened() {
        use aruna_core::structs::execution::job::input::{PLUGIN_INSTALL_TAG, PLUGIN_WORK_TAG};
        let config = Family::new([4; 32]).config;
        let mut admit = operation(Some(local_ingress()));
        let tags = &mut admit.request.spec.tags;
        tags.insert(
            PLUGIN_INSTALL_TAG.to_string(),
            Ulid::from_parts(1, 1).to_string(),
        );
        tags.insert(
            PLUGIN_WORK_TAG.to_string(),
            Ulid::from_parts(2, 2).to_string(),
        );
        let next = admit.with_pages(config.clone(), Vec::new());
        let request = &admit.request;
        let task = (&request.spec, request.created_by, request.ingress_node_id);
        let screen = ScreenTask::new(std::sync::Arc::new(config.clone()), task);
        assert_eq!(next, Ok(Next::Call(IoCall::child(screen))));
        assert_eq!(
            admit.step,
            ForwardedStep::Rescreen(Box::new(config.clone()))
        );
        let withdrawn = ScreenError::Refused("objects.read is not granted".to_string());
        let reason = withdrawn.to_string();
        let refused = admit.next(child(Err::<PluginInstall, _>(withdrawn)));
        assert_eq!(refused, Err(SubmissionRefusal::Invalid { reason }));
        // An ordinary job's pages need no task check.
        let mut plain = operation(Some(local_ingress()));
        let _ = plain.with_pages(config, Vec::new());
        assert!(!matches!(plain.step, ForwardedStep::Rescreen(_)));
    }

    // Missing grant rows deny the write; a caller is never allowed by default.
    #[test]
    fn missing_rows_deny() {
        let mut check = GroupWriteOperation::new(&auth(user()), Ulid::from_parts(2, 2), node(1));
        let Ok(Next::Call(IoCall::Read(reads))) = check.first() else {
            panic!("the grants and policies are read");
        };
        let rows = IoResult::Values(vec![None; reads.len()]);
        assert_eq!(check.next(rows), Ok(Next::Done(false)));
    }
}
