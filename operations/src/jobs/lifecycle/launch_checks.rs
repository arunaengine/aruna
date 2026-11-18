//! Decides whether this target may run one offered launch: its own capability, the drain
//! state, the governed-data gate, the submitter's grant, and the placement policies.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::{ExecutorCapability, NetworkAccess};
use aruna_core::events::LaunchDecline;
use aruna_core::id::NodeId;
use aruna_core::node_state::{
    DrainState, DrainStateError, DrainStateOperation, NodeInfoError, ReadInfoOperation,
};
use aruna_core::operation::Operation;
use aruna_core::scheduling::PlannedInput;
use aruna_core::structs::execution::job::{
    CapturedInput, InputSource, LaunchIntent, LogicalJobSpec,
};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::placement::policy::{
    PlacementDecision, PlacementPolicyRef, evaluate_placement,
};
use aruna_core::structs::storage::blob::group_permission_path;
use aruna_core::structs::storage::node_info::NodeInfoDocument;
use aruna_core::types::Value;
use ulid::Ulid;

use super::ids;
use super::plan::{REALM_STAGING, network_access};
use super::policies::{Resolutions, ResolvePoliciesOperation};
use crate::auth::check_permissions::CheckPermissionsConfig;
use crate::auth::enforcement::{policy_reads, read_policies};
use crate::auth::request_policy::{PolicyRequestExtras, policy_request_with};
use crate::placement::policy::{GateContext, GateContextError, GateOperation};

/// What a check answers: the capability that runs the launch, or why this target refuses it.
pub(crate) type Verdict = Result<ExecutorCapability, LaunchDecline>;

/// Whether one pinned input still describes the captured input it names, resolved at
/// `source`. Any node may be the pinned holder, because a registered copy serves the read;
/// version, content and size bind the bytes, and the local target is not a remote read.
pub(crate) fn pin_matches(
    captured: &CapturedInput,
    pin: &PlannedInput,
    source: NodeId,
    local: NodeId,
) -> bool {
    captured.source_node_id == source
        && captured.version_id == pin.version_id
        && captured.content == pin.content
        && captured.bytes == pin.bytes
        && pin.source_node_id != Some(local)
}

#[derive(Debug, PartialEq)]
enum CapabilityStep {
    Advertisement,
    Drain,
}

/// The executor capability of this node that runs `intent`. The node must be a sync-eligible
/// realm member that is not leaving or draining, and its executor must fit the spec. The
/// durable operator flag is read directly: a stale heartbeat document must never re-enable
/// offers this node was drained out of.
#[derive(Debug, PartialEq)]
pub(crate) struct LocalCapabilityOperation {
    config: RealmConfigDocument,
    local: NodeId,
    intent: LaunchIntent,
    spec: LogicalJobSpec,
    document: Option<NodeInfoDocument>,
    step: CapabilityStep,
}

type CapabilityNext = Result<Next<Verdict>, IoError>;

impl LocalCapabilityOperation {
    pub(crate) fn new(
        config: RealmConfigDocument,
        local: NodeId,
        intent: LaunchIntent,
        spec: LogicalJobSpec,
    ) -> Self {
        Self {
            config,
            local,
            intent,
            spec,
            document: None,
            step: CapabilityStep::Advertisement,
        }
    }

    fn member(&mut self, document: NodeInfoDocument) -> CapabilityNext {
        let (config, local) = (&self.config, self.local);
        if !config
            .sync_eligible_nodes()
            .is_ok_and(|members| members.contains(&local))
        {
            return Ok(Next::Done(Err(LaunchDecline::Unauthorized)));
        }
        let kind = config
            .nodes
            .iter()
            .find(|node| node.node_id == local.to_string())
            .map(|node| &node.kind);
        if !kind.is_some_and(|kind| kind.is_sync_eligible()) {
            return Ok(Next::Done(Err(LaunchDecline::Unauthorized)));
        }
        self.document = Some(document);
        self.step = CapabilityStep::Drain;
        Ok(Next::Call(IoCall::child(DrainStateOperation::new())))
    }

    fn capability(&self, operator_draining: bool) -> Verdict {
        let (spec, local) = (&self.spec, self.local);
        let document = self.document.as_ref().ok_or(LaunchDecline::Draining)?;
        if document.leaving
            || document.compute_draining
            || operator_draining
            || self
                .config
                .placement_entry(local)
                .is_some_and(|entry| entry.draining)
        {
            return Err(LaunchDecline::Draining);
        }
        let capability = document
            .executors
            .iter()
            .find(|capability| capability.kind == self.intent.target.executor_kind)
            .cloned()
            .ok_or(LaunchDecline::Unauthorized)?;
        if capability.policy_draining {
            return Err(LaunchDecline::Draining);
        }
        if capability.validate(local).is_err()
            || spec
                .payload
                .executor_constraint
                .as_deref()
                .is_some_and(|kind| kind.trim() != capability.kind.trim())
            || !capability.supports(REALM_STAGING)
            || (ids::session_of(&spec.payload).is_some()
                && network_access(spec) != NetworkAccess::Open
                && !capability.session)
            || !capability.limits.fits(&spec.resources)
        {
            return Err(LaunchDecline::Unauthorized);
        }
        let labels =
            ids::required_labels(&spec.payload).map_err(|_| LaunchDecline::Unauthorized)?;
        let carried = |key: &str| capability.subject.labels.get(key).map(|value| value.trim());
        if !labels
            .iter()
            .all(|label| carried(label.key.trim()) == Some(label.value.trim()))
        {
            return Err(LaunchDecline::Unauthorized);
        }
        let intent = &self.intent;
        let protected = !intent.output_policies.is_empty()
            || intent.inputs.iter().any(|input| !input.policies.is_empty());
        if protected && network_access(spec) == NetworkAccess::Open && !capability.network_policy {
            return Err(LaunchDecline::Unauthorized);
        }
        Ok(capability)
    }
}

impl Operation for LocalCapabilityOperation {
    type Output = Verdict;
    type Error = IoError;

    fn first(&mut self) -> CapabilityNext {
        Ok(Next::Call(IoCall::child(ReadInfoOperation::new(vec![
            self.local,
        ]))))
    }

    fn next(&mut self, result: IoResult) -> CapabilityNext {
        match self.step {
            CapabilityStep::Advertisement => {
                let read = result.child::<BTreeMap<NodeId, NodeInfoDocument>, NodeInfoError>()?;
                let document = read
                    .ok()
                    .and_then(|mut documents| documents.remove(&self.local));
                match document {
                    Some(document) => self.member(document),
                    None => Ok(Next::Done(Err(LaunchDecline::Draining))),
                }
            }
            CapabilityStep::Drain => {
                let drain = result.child::<DrainState, DrainStateError>()?;
                let draining = drain.map_or(true, |drain| drain.operator_draining);
                Ok(Next::Done(self.capability(draining)))
            }
        }
    }
}

/// One offered launch with everything this target checks it against.
#[derive(Debug, PartialEq)]
pub(crate) struct LaunchCheck {
    pub config: RealmConfigDocument,
    pub local: NodeId,
    /// The launch with its planned inputs inline.
    pub planned: LaunchIntent,
    /// The stored spec with its paged inputs read in.
    pub spec: LogicalJobSpec,
    pub now_ms: u64,
}

/// Why the checks left an offer undecided.
#[derive(Debug, PartialEq, thiserror::Error)]
pub(crate) enum CheckError {
    #[error(transparent)]
    Io(#[from] IoError),
    #[error("the governed-data gate is unavailable: {0}")]
    Gate(String),
}

#[derive(Debug, PartialEq)]
enum CheckStep {
    Capability,
    Gate,
    Grant,
    Policies,
}

/// Checks one offered launch in order: this node's capability, the governed-data gate (a
/// node that stopped admitting governed data declines every new execution target), the
/// stored submitter's WRITE grant on the group here, and the placement policies this target
/// can resolve against its own subject. No bearer token replicates, so the grant is read from
/// this node's replicated permissions. An error leaves the offer undecided.
#[derive(Debug, PartialEq)]
pub(crate) struct LaunchChecksOperation {
    check: LaunchCheck,
    capability: Option<ExecutorCapability>,
    /// The policy sets to evaluate, outputs first, once the pins are checked.
    policy_sets: Vec<Vec<PlacementPolicyRef>>,
    step: CheckStep,
}

type CheckNext = Result<Next<Verdict>, CheckError>;

impl LaunchChecksOperation {
    pub(crate) fn new(check: LaunchCheck) -> Self {
        Self {
            check,
            capability: None,
            policy_sets: Vec::new(),
            step: CheckStep::Capability,
        }
    }

    fn grant_check(&self) -> CheckPermissionsConfig {
        let spec = &self.check.spec;
        CheckPermissionsConfig {
            auth_context: AuthContext {
                user_id: spec.created_by,
                realm_id: spec.realm_id,
                path_restrictions: None,
                session: None,
            },
            path: group_permission_path(spec.realm_id, spec.group_id, self.check.local),
            required_permission: Permission::WRITE,
        }
    }

    /// Reads the request policies of the group and the submitter's grants at once.
    fn grant(&mut self) -> CheckNext {
        let spec = &self.check.spec;
        let Ok(mut reads) = self.grant_check().reads() else {
            return Ok(Next::Done(Err(LaunchDecline::Unauthorized)));
        };
        reads.extend(policy_reads(spec.realm_id, Some(spec.group_id)));
        self.step = CheckStep::Grant;
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn granted(&self, mut rows: Vec<Option<Value>>) -> bool {
        let check = self.grant_check();
        let Ok(split) = check.reads().map(|reads| reads.len()) else {
            return false;
        };
        let policies = rows.split_off(split.min(rows.len()));
        let spec = &self.check.spec;
        let Ok(evaluator) = read_policies(spec.realm_id, Some(spec.group_id), policies) else {
            return false;
        };
        if !check.allowed(rows).unwrap_or(false) {
            return false;
        }
        let request = policy_request_with(
            &check.path,
            &Permission::WRITE,
            Some(&check.auth_context),
            PolicyRequestExtras::rest(),
        );
        evaluator.evaluate(&request).is_ok()
    }

    /// Checks every pin against the stored spec, then resolves the policies it names. Refs of
    /// an input this target cannot resolve yet are enforced again when the bytes are staged.
    fn policies(&mut self) -> CheckNext {
        let local = self.check.local;
        let (inputs, outputs) = match pinned_policies(&self.check.spec, &self.check.planned, local)
        {
            Ok(sets) => sets,
            Err(decline) => return Ok(Next::Done(Err(decline))),
        };
        let mut refs = outputs.clone();
        refs.extend(inputs.iter().flatten().copied());
        self.policy_sets = std::iter::once(outputs).chain(inputs).collect();
        self.step = CheckStep::Policies;
        let (realm_id, now_ms) = (self.check.spec.realm_id, self.check.now_ms);
        let resolve = ResolvePoliciesOperation::new(realm_id, local, now_ms, refs);
        Ok(Next::Call(IoCall::child(resolve)))
    }

    fn placement(&mut self, policies: Resolutions) -> Verdict {
        let capability = self.capability.take().ok_or(LaunchDecline::Draining)?;
        for policy_set in &self.policy_sets {
            match evaluate_placement(policy_set, &policies, &capability.subject) {
                PlacementDecision::Allowed => {}
                decision => {
                    return Err(match aruna_core::events::DeclinedPolicy::new(decision) {
                        Ok(policy) => LaunchDecline::Policy(policy),
                        Err(_) => LaunchDecline::Unauthorized,
                    });
                }
            }
        }
        Ok(capability)
    }
}

impl Operation for LaunchChecksOperation {
    type Output = Verdict;
    type Error = CheckError;

    fn first(&mut self) -> CheckNext {
        let check = &self.check;
        let capability = LocalCapabilityOperation::new(
            check.config.clone(),
            check.local,
            check.planned.clone(),
            check.spec.clone(),
        );
        Ok(Next::Call(IoCall::child(capability)))
    }

    fn next(&mut self, result: IoResult) -> CheckNext {
        match self.step {
            CheckStep::Capability => match result.child::<Verdict, IoError>()?? {
                Ok(capability) => {
                    self.capability = Some(capability);
                    self.step = CheckStep::Gate;
                    let gate = GateOperation::new(self.check.spec.realm_id, self.check.now_ms);
                    Ok(Next::Call(IoCall::child(gate)))
                }
                Err(decline) => Ok(Next::Done(Err(decline))),
            },
            CheckStep::Gate => {
                let gate = result.child::<Option<GateContext>, GateContextError>()?;
                match gate {
                    Ok(Some(gate)) if !gate.admitting => {
                        Ok(Next::Done(Err(LaunchDecline::Draining)))
                    }
                    Err(error) => Err(CheckError::Gate(error.to_string())),
                    Ok(_) => self.grant(),
                }
            }
            CheckStep::Grant => {
                let IoResult::Values(rows) = result else {
                    let got = result.label();
                    let expected = "IoResult::Values";
                    return Err(IoError::Unexpected { expected, got }.into());
                };
                if !self.granted(rows) {
                    return Ok(Next::Done(Err(LaunchDecline::Unauthorized)));
                }
                self.policies()
            }
            CheckStep::Policies => {
                let policies = result.child::<Resolutions, IoError>()?.unwrap_or_default();
                Ok(Next::Done(self.placement(policies)))
            }
        }
    }
}

/// The policy refs of every pinned input, and the output refs the launch must carry. Each pin
/// must name the stored input version, content and policies at the source the spec captured.
fn pinned_policies(
    spec: &LogicalJobSpec,
    intent: &LaunchIntent,
    local: NodeId,
) -> Result<(Vec<Vec<PlacementPolicyRef>>, Vec<PlacementPolicyRef>), LaunchDecline> {
    let inputs_count = spec.payload.inputs.len();
    if intent.inputs.len() != inputs_count || spec.captured_inputs.len() != inputs_count {
        return Err(LaunchDecline::Unauthorized);
    }
    let mut inputs: Vec<Vec<PlacementPolicyRef>> = Vec::new();
    for input in &spec.payload.inputs {
        let InputSource::S3 { version_id, .. } = &input.source;
        let pin = intent
            .inputs
            .iter()
            .find(|pin| pin.destination_key == input.dest_key)
            .ok_or(LaunchDecline::Unauthorized)?;
        let requested = version_id
            .as_deref()
            .map(Ulid::from_string)
            .transpose()
            .map_err(|_| LaunchDecline::Unauthorized)?;
        if requested.is_some_and(|requested| requested != pin.version_id) {
            return Err(LaunchDecline::Unauthorized);
        }
        let captured = spec
            .captured_inputs
            .iter()
            .find(|captured| captured.destination_key == input.dest_key)
            .ok_or(LaunchDecline::Unauthorized)?;
        let source = input.source_node_id.unwrap_or(spec.ingress_node_id);
        if !pin_matches(captured, pin, source, local) {
            return Err(LaunchDecline::Unauthorized);
        }
        let mut policies = captured.policies.clone();
        policies.sort_unstable();
        policies.dedup();
        if captured.version_id != pin.version_id
            || captured.content != pin.content
            || policies != pin.policies
        {
            return Err(LaunchDecline::Unauthorized);
        }
        inputs.push(policies);
    }
    let mut outputs = inputs.iter().flatten().copied().collect::<Vec<_>>();
    outputs.extend(spec.output_policies.clone());
    outputs.sort_unstable();
    outputs.dedup();
    if outputs != intent.output_policies {
        return Err(LaunchDecline::Unauthorized);
    }
    Ok((inputs, outputs))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::records::{Family, node};
    use aruna_core::calls::ChildOutcome;

    fn capability_of(family: &Family, local: NodeId) -> LocalCapabilityOperation {
        let spec = family.spec();
        let intent = family.launch(&spec, local, 0);
        LocalCapabilityOperation::new(family.config.clone(), local, intent, spec)
    }

    // A node without its own advertisement drains; one outside the realm members is refused.
    #[test]
    fn capability_needs_membership() {
        let family = Family::new([5u8; 32]);
        let mut unread = capability_of(&family, node(9));
        assert!(matches!(unread.first(), Ok(Next::Call(IoCall::Child(_)))));
        let missing = Ok::<_, NodeInfoError>(BTreeMap::<NodeId, NodeInfoDocument>::new());
        let answer = IoResult::Child(ChildOutcome::new(missing));
        assert_eq!(
            unread.next(answer),
            Ok(Next::Done(Err(LaunchDecline::Draining)))
        );
    }

    fn check(family: &Family) -> LaunchChecksOperation {
        let spec = family.spec();
        let planned = family.launch(&spec, node(9), 0);
        LaunchChecksOperation::new(LaunchCheck {
            config: family.config.clone(),
            local: node(9),
            planned,
            spec,
            now_ms: 9,
        })
    }

    // A refused capability answers at once, before the gate or any grant is read.
    #[test]
    fn checks_stop_early() {
        let family = Family::new([6u8; 32]);
        let mut refused = check(&family);
        assert!(matches!(refused.first(), Ok(Next::Call(IoCall::Child(_)))));
        let verdict: Verdict = Err(LaunchDecline::Unauthorized);
        let answer = IoResult::Child(ChildOutcome::new(Ok::<_, IoError>(verdict)));
        assert_eq!(
            refused.next(answer),
            Ok(Next::Done(Err(LaunchDecline::Unauthorized)))
        );
    }
}
