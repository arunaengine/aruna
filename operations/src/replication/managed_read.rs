//! Runs a governed remote Bao read: the destination challenge with taught placement rules, or
//! one plain attempt on a device.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::errors::ConversionError;
use aruna_core::events::{Event, NetEvent};
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::rows::read_row;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::placement::policy::PlacementPolicyRef;

use super::bao_read::{BaoReadError, BaoReadOperation, BaoReadOutput};
use super::protocol::BaoReadRequest;
use crate::placement::policy::{
    GateContext, GateContextError, GateOperation, PolicyGateError, PolicyGateOutcome,
    ReadPolicyError, gate_decision, union_refs, write_gate,
};
use crate::realm::mutate_placement::node_kind;

/// One teach-then-retry round. The source only ever teaches the refs it needs,
/// so a second `Required` for the same set is a protocol dead end, not a loop.
const CHALLENGE_ATTEMPTS: usize = 2;

#[derive(Clone, Copy, Debug, PartialEq)]
enum ManagedStep {
    Role,
    Device,
    Gate,
    Read,
    Teach,
}

type ManagedTurn = Result<Next<BaoReadOutput>, BaoReadError>;

/// A governed remote read with the plan's destination challenge (5.6/10): the
/// request carries this node's subject, refs resolve and cache locally, retries
/// need local compliance, and echoed refs are never authority.
#[derive(Debug, PartialEq)]
pub struct ManagedReadOperation {
    node_id: NodeId,
    request: BaoReadRequest,
    step: ManagedStep,
    destination: Option<GateContext>,
    taught: Vec<PlacementPolicyRef>,
    /// The refs the running gate checks, taught once it allows them.
    teaching: Vec<PlacementPolicyRef>,
    attempts: usize,
}

impl ManagedReadOperation {
    pub fn new(node_id: NodeId, request: BaoReadRequest) -> Self {
        Self {
            node_id,
            request,
            step: ManagedStep::Role,
            destination: None,
            taught: Vec::new(),
            teaching: Vec::new(),
            attempts: 0,
        }
    }

    fn call(&mut self, step: ManagedStep, call: IoCall) -> ManagedTurn {
        self.step = step;
        Ok(Next::Call(call))
    }

    fn bao_read(&self) -> IoCall {
        IoCall::child(BaoReadOperation::new(self.node_id, self.request.clone()))
    }

    /// A device gets one attempt with no destination at all. It never admits governed data,
    /// so teaching it rules would only dress a refusal up as a negotiation.
    fn role_known(&mut self, is_user: bool) -> ManagedTurn {
        if is_user {
            self.request.destination = None;
            let read = self.bao_read();
            return self.call(ManagedStep::Device, read);
        }
        // An admission stop is not decided here: `write_gate` refuses once the source has
        // taught the refs that make this read a governed one.
        let now_ms = aruna_core::time::unix_timestamp_millis();
        let gate = GateOperation::new(self.request.realm_id, now_ms);
        self.call(ManagedStep::Gate, IoCall::child(gate))
    }

    fn gate_read(&mut self, gate: Result<Option<GateContext>, GateContextError>) -> ManagedTurn {
        let destination = gate.map_err(|_| BaoReadError::NoDestination)?;
        self.request.destination = destination.as_ref().map(|gate| gate.subject.clone());
        self.destination = destination;
        self.attempt()
    }

    fn attempt(&mut self) -> ManagedTurn {
        self.attempts += 1;
        let read = self.bao_read();
        self.call(ManagedStep::Read, read)
    }

    fn answered(&mut self, answer: Result<BaoReadOutput, BaoReadError>) -> ManagedTurn {
        let refs = match answer {
            Err(BaoReadError::PolicyRequired { refs }) => refs,
            other => return other.map(Next::Done),
        };
        let refs = PlacementPolicyRef::canonical_set(&refs).map_err(ConversionError::from)?;
        if refs.is_empty() || refs == self.taught {
            return Err(BaoReadError::PolicyRequired { refs });
        }
        // The refs are only a hint; this node resolves its own view and caches the
        // verified publication. The write checks the owning group, not the read.
        let gate = write_gate(self.destination.as_ref(), &refs, None)?;
        let gate = gate.ok_or(BaoReadError::NoDestination)?;
        self.teaching = refs;
        self.call(ManagedStep::Teach, IoCall::child(gate))
    }

    fn gate_checked(&mut self, gate: Result<PolicyGateOutcome, ReadPolicyError>) -> ManagedTurn {
        gate_decision(gate.map_err(PolicyGateError::from)?)?;
        let refs = std::mem::take(&mut self.teaching);
        self.request.known_refs = union_refs(&self.request.known_refs, &refs)?;
        self.taught = refs;
        if self.attempts >= CHALLENGE_ATTEMPTS {
            let refs = std::mem::take(&mut self.taught);
            return Err(BaoReadError::PolicyRequired { refs });
        }
        self.attempt()
    }
}

impl Operation for ManagedReadOperation {
    type Output = BaoReadOutput;
    type Error = BaoReadError;

    fn first(&mut self) -> ManagedTurn {
        let role = LocalUserOperation::new(self.request.realm_id);
        self.call(ManagedStep::Role, IoCall::child(role))
    }

    fn next(&mut self, result: IoResult) -> ManagedTurn {
        match self.step {
            ManagedStep::Role => {
                // A role that cannot be read is not a device's.
                let is_user = result.child::<bool, IoError>()?.unwrap_or(false);
                self.role_known(is_user)
            }
            ManagedStep::Device => {
                device_answer(result.child::<BaoReadOutput, BaoReadError>()?).map(Next::Done)
            }
            ManagedStep::Gate => {
                self.gate_read(result.child::<Option<GateContext>, GateContextError>()?)
            }
            ManagedStep::Read => self.answered(result.child::<BaoReadOutput, BaoReadError>()?),
            ManagedStep::Teach => {
                self.gate_checked(result.child::<PolicyGateOutcome, ReadPolicyError>()?)
            }
        }
    }
}

/// Every placement answer is terminal here: a device can neither resolve a rule
/// nor become a destination, so a retry would only repeat the same refusal.
pub(super) fn device_answer(
    result: Result<BaoReadOutput, BaoReadError>,
) -> Result<BaoReadOutput, BaoReadError> {
    match result {
        Err(BaoReadError::PolicyRequired { .. })
        | Err(BaoReadError::PolicyDenied { .. })
        | Err(BaoReadError::NoDestination)
        | Err(BaoReadError::Gate(_)) => Err(BaoReadError::GovernedUnavailable),
        other => other,
    }
}

/// Whether this realm records the local node as an owner-bound User node. A node without a net
/// handle is none; a caller treats a failed read as no as well.
#[derive(Debug, PartialEq)]
pub struct LocalUserOperation {
    realm_id: RealmId,
    node: Option<NodeId>,
}

impl LocalUserOperation {
    pub fn new(realm_id: RealmId) -> Self {
        Self {
            realm_id,
            node: None,
        }
    }

    fn node_known(&mut self, event: Event) -> Result<Next<bool>, IoError> {
        match event {
            Event::Net(NetEvent::LocalNode(Some(node))) => {
                self.node = Some(node);
                let key = self.realm_id.as_bytes().to_vec();
                Ok(Next::Call(read_row(REALM_CONFIG_KEYSPACE, key)))
            }
            Event::Net(NetEvent::LocalNode(None)) => Ok(Next::Done(false)),
            other => Err(IoError::Unexpected {
                expected: "NetEvent::LocalNode",
                got: other.label(),
            }),
        }
    }
}

impl Operation for LocalUserOperation {
    type Output = bool;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<bool>, IoError> {
        Ok(Next::Call(Effect::Net(NetEffect::LocalNode).into()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, IoError> {
        let Some(node) = self.node else {
            return self.node_known(result.event()?);
        };
        let document = result.value()?;
        let document = document.map(|value| RealmConfigDocument::from_bytes(&value));
        let is_user = document.is_some_and(|document| {
            document.is_ok_and(|document| {
                node_kind(&document, node).is_some_and(|kind| kind.owner().is_some())
            })
        });
        Ok(Next::Done(is_user))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::placement::policy::cache::PolicyCacheStats;
    use crate::replication::protocol::BaoReadTarget;
    use aruna_core::UserId;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::identity::auth::AuthContext;
    use aruna_core::structs::placement::policy::{PlacementDecision, PlacementSubject};
    use aruna_core::structs::storage::replication::VersionedObjectArn;
    use std::collections::{BTreeMap, HashMap};
    use ulid::Ulid;

    fn node() -> NodeId {
        iroh::SecretKey::from_bytes(&[2; 32]).public()
    }

    fn realm() -> RealmId {
        RealmId::from_bytes([1; 32])
    }

    fn request() -> BaoReadRequest {
        let arn = VersionedObjectArn::new(realm(), node(), "bucket", "file", Ulid::from(4u128));
        BaoReadRequest {
            auth_context: AuthContext {
                user_id: UserId::nil(realm()),
                realm_id: realm(),
                path_restrictions: None,
                session: None,
            },
            realm_id: realm(),
            target: BaoReadTarget::ExactVersion(arn.unwrap()),
            expected_blake3: None,
            metadata_only: true,
            destination: None,
            known_refs: Vec::new(),
            range: None,
        }
    }

    fn gate() -> GateContext {
        GateContext {
            realm_id: realm(),
            admitting: true,
            subject: PlacementSubject {
                node_id: node(),
                generation: 1,
                location: "here".to_string(),
                labels: BTreeMap::new(),
                executor_kind: None,
                local_to_controller: false,
            },
            now_ms: 1,
        }
    }

    fn output() -> BaoReadOutput {
        BaoReadOutput::Metadata {
            size: 1,
            blake3: [1; 32],
            etag: None,
            hashes: HashMap::new(),
        }
    }

    fn policy(seed: u8) -> PlacementPolicyRef {
        PlacementPolicyRef {
            policy_id: Ulid::from_bytes([seed; 16]),
            digest: [seed; 32],
        }
    }

    fn child<T, E>(result: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(ChildOutcome::new(result))
    }

    fn required(refs: Vec<PlacementPolicyRef>) -> IoResult {
        child(Err::<BaoReadOutput, _>(BaoReadError::PolicyRequired {
            refs,
        }))
    }

    fn allowed() -> IoResult {
        child(Ok::<_, ReadPolicyError>(PolicyGateOutcome {
            decision: PlacementDecision::Allowed,
            foreign_owner: None,
            stats: PolicyCacheStats::default(),
        }))
    }

    fn is_child(turn: &ManagedTurn) -> bool {
        matches!(turn, Ok(Next::Call(IoCall::Child(_))))
    }

    /// A server node's read that holds its destination and waits for the first answer.
    fn reading() -> ManagedReadOperation {
        let mut operation = ManagedReadOperation::new(node(), request());
        assert!(is_child(&operation.first()));
        assert!(is_child(&operation.next(child(Ok::<_, IoError>(false)))));
        let turn = operation.next(child(Ok::<_, GateContextError>(Some(gate()))));
        assert!(is_child(&turn));
        assert_eq!(operation.request.destination, Some(gate().subject));
        operation
    }

    #[test]
    fn plain_read_answers() {
        let mut operation = reading();
        let turn = operation.next(child(Ok::<_, BaoReadError>(output())));
        assert_eq!(turn, Ok(Next::Done(output())));
    }

    #[test]
    fn taught_refs_retry() {
        let mut operation = reading();
        assert!(is_child(&operation.next(required(vec![policy(1)]))));
        assert!(is_child(&operation.next(allowed())));
        assert_eq!(operation.request.known_refs, vec![policy(1)]);
        let turn = operation.next(child(Ok::<_, BaoReadError>(output())));
        assert_eq!(turn, Ok(Next::Done(output())));
    }

    #[test]
    fn repeated_refs_stop() {
        let mut operation = reading();
        operation.next(required(vec![policy(1)])).unwrap();
        operation.next(allowed()).unwrap();
        assert_eq!(
            operation.next(required(vec![policy(1)])),
            Err(BaoReadError::PolicyRequired {
                refs: vec![policy(1)]
            })
        );
    }

    #[test]
    fn new_refs_exhaust() {
        // The second answer may name other refs; they are checked, but no third read follows.
        let mut operation = reading();
        operation.next(required(vec![policy(1)])).unwrap();
        operation.next(allowed()).unwrap();
        operation.next(required(vec![policy(2)])).unwrap();
        assert_eq!(
            operation.next(allowed()),
            Err(BaoReadError::PolicyRequired {
                refs: vec![policy(2)]
            })
        );
    }

    #[test]
    fn empty_refs_stop() {
        let mut operation = reading();
        assert_eq!(
            operation.next(required(Vec::new())),
            Err(BaoReadError::PolicyRequired { refs: Vec::new() })
        );
    }

    #[test]
    fn unreadable_gate_refuses() {
        let mut operation = ManagedReadOperation::new(node(), request());
        operation.first().unwrap();
        // A role that cannot be read falls through to the server path.
        let role = operation.next(child(Err::<bool, _>(IoError::Conflict)));
        assert!(is_child(&role));
        let gate = Err::<Option<GateContext>, _>(GateContextError::from(IoError::Conflict));
        assert_eq!(
            operation.next(child(gate)),
            Err(BaoReadError::NoDestination)
        );
    }

    #[test]
    fn device_read_final() {
        let mut operation = ManagedReadOperation::new(node(), request());
        operation.request.destination = Some(gate().subject);
        operation.first().unwrap();
        assert!(is_child(&operation.next(child(Ok::<_, IoError>(true)))));
        assert_eq!(operation.request.destination, None);
        assert_eq!(
            operation.next(required(vec![policy(1)])),
            Err(BaoReadError::GovernedUnavailable)
        );
    }

    fn net(event: NetEvent) -> IoResult {
        IoResult::Event(Box::new(Event::Net(event)))
    }

    #[test]
    fn no_net_no_user() {
        let mut operation = LocalUserOperation::new(realm());
        let asked: IoCall = Effect::Net(NetEffect::LocalNode).into();
        assert_eq!(operation.first(), Ok(Next::Call(asked)));
        let turn = operation.next(net(NetEvent::LocalNode(None)));
        assert_eq!(turn, Ok(Next::Done(false)));
    }

    #[test]
    fn unlisted_node_no_user() {
        let mut operation = LocalUserOperation::new(realm());
        operation.first().unwrap();
        let read = operation.next(net(NetEvent::LocalNode(Some(node()))));
        assert!(matches!(read, Ok(Next::Call(IoCall::Read(_)))));
        let document = RealmConfigDocument::new(realm(), Vec::new(), 1);
        let value = document.to_bytes().unwrap().into();
        let turn = operation.next(IoResult::Values(vec![Some(value)]));
        assert_eq!(turn, Ok(Next::Done(false)));
    }

    #[test]
    fn missing_config_no_user() {
        let mut operation = LocalUserOperation::new(realm());
        operation.first().unwrap();
        operation
            .next(net(NetEvent::LocalNode(Some(node()))))
            .unwrap();
        let turn = operation.next(IoResult::Values(vec![None]));
        assert_eq!(turn, Ok(Next::Done(false)));
    }
}
