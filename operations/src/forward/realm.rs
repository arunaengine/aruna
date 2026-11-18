//! Relays an origin-signed realm admin envelope to a holder of its shard, and accepts relays.
//! Receivers still authorize the origin of the envelope, never the relaying node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::forward::fetch::{peer_call, peer_reply};
use crate::forward::transport::{ForwardError, reject};
use crate::metadata::protocol::{MetadataTransportMessage, WriteAuthError, encode_message};
use crate::realm::peer_trust::is_sync_eligible;
use crate::sync::outbox_record::{new_outbox_record, outbox_write_entry, schedule_drain_effect};
use aruna_core::NodeId;
use aruna_core::admin_documents::AdminDocumentEvent;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::{DocumentOutboxEvent, DocumentTarget};
use aruna_core::operation::Operation;
use aruna_core::placement::holds_placement;
use aruna_core::rows::done;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::placement::record::PlacementRef;
use std::time::Duration;
use tracing::warn;

const RELAY_PEER_LIMIT: usize = 3;

const RELAY_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);

/// Hands an origin-signed administrative envelope to a holder of its shard: the
/// origin holds none of that shard so cannot publish; the holder republishes the
/// exact envelope and receivers still authorize the origin, never the relay.
#[derive(Debug, PartialEq)]
pub struct RelayAdminOperation {
    /// Reversed, so the next holder pops off the end.
    peers: Vec<NodeId>,
    frame: Result<Vec<u8>, String>,
    asked: Option<NodeId>,
}

impl RelayAdminOperation {
    pub fn new(
        local: Option<NodeId>,
        holders: &[NodeId],
        target: DocumentTarget,
        event: Box<AdminDocumentEvent>,
        placement: PlacementRef,
        origin_signature: iroh::Signature,
    ) -> Self {
        let mut peers: Vec<_> = (holders.iter().copied())
            .filter(|peer| Some(*peer) != local)
            .take(RELAY_PEER_LIMIT)
            .collect();
        peers.reverse();
        let frame = encode_message(&MetadataTransportMessage::ForwardAdminEvent {
            target,
            event,
            placement,
            origin_signature,
        });
        Self {
            peers,
            frame,
            asked: None,
        }
    }

    fn ask(&mut self) -> Result<Next<()>, ForwardError> {
        let frame = self.frame.as_ref().map_err(|error| {
            warn!(%error, "A relayed admin event does not encode");
            ForwardError::Unavailable
        })?;
        let peer = self.peers.pop().ok_or(ForwardError::Unavailable)?;
        self.asked = Some(peer);
        let call = peer_call(peer, frame.clone(), RELAY_ATTEMPT_TIMEOUT);
        Ok(Next::Call(call))
    }
}

impl Operation for RelayAdminOperation {
    type Output = ();
    type Error = ForwardError;

    fn first(&mut self) -> Result<Next<()>, ForwardError> {
        self.ask()
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, ForwardError> {
        let peer = self.asked.take().ok_or(ForwardError::Unavailable)?;
        match peer_reply(peer, result)? {
            Ok(MetadataTransportMessage::AdminEventQueued) => Ok(Next::Done(())),
            Ok(MetadataTransportMessage::Reject(error)) => {
                // A rejection is a verdict on the envelope, not on this peer.
                warn!(%peer, %error, "Holder rejected a relayed admin event");
                Err(ForwardError::Unavailable)
            }
            Ok(_) | Err(_) => self.ask(),
        }
    }
}

/// Whether this node may take custody of a relayed administrative envelope.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RelayAdmission {
    Accept,
    /// The sending peer has no business relaying realm administration.
    Forbidden,
    /// The envelope itself is invalid; no other holder would accept it either.
    Reject(String),
    /// This node cannot publish the record, but another holder can.
    Unavailable,
}

/// Admission decision for a relayed envelope. Authority is the origin
/// signature, never the relaying peer and never a caller token; this node only
/// proves the envelope is sound and that it can publish it at all.
pub(crate) fn admit_relayed_admin(
    config: &RealmConfigDocument,
    local_node_id: NodeId,
    peer: NodeId,
    event: &AdminDocumentEvent,
    placement: &PlacementRef,
    origin_signature: &iroh::Signature,
) -> RelayAdmission {
    if !is_sync_eligible(config, peer) {
        return RelayAdmission::Forbidden;
    }
    if !is_sync_eligible(config, event.origin_node_id) {
        return RelayAdmission::Reject("relayed admin event origin may not publish".to_string());
    }
    if event.actor.realm_id != config.realm_id || event.origin_node_id != event.actor.node_id {
        return RelayAdmission::Reject("relayed admin event identity does not match".to_string());
    }
    if !event.origin_signed(placement, origin_signature) {
        return RelayAdmission::Reject(
            "relayed admin event is not signed by its origin".to_string(),
        );
    }
    if !holds_placement(config, placement, local_node_id) {
        return RelayAdmission::Unavailable;
    }
    RelayAdmission::Accept
}

#[derive(Debug, PartialEq)]
struct AdminRelay {
    target: DocumentTarget,
    event: Box<AdminDocumentEvent>,
    placement: PlacementRef,
    origin_signature: iroh::Signature,
}

/// Accepts a relayed administrative envelope and takes custody of publishing it. A failed write
/// ends the run, and the caller answers that this holder is unavailable.
#[derive(Debug, PartialEq)]
pub(crate) struct ApplyRelayOperation {
    local: Option<(NodeId, RealmId)>,
    peer: NodeId,
    relay: Option<AdminRelay>,
    writing: bool,
}

type RelayTurn = Result<Next<MetadataTransportMessage>, IoError>;

impl ApplyRelayOperation {
    pub(crate) fn new(
        local: Option<(NodeId, RealmId)>,
        peer: NodeId,
        message: MetadataTransportMessage,
    ) -> Self {
        let relay = match message {
            MetadataTransportMessage::ForwardAdminEvent {
                target,
                event,
                placement,
                origin_signature,
            } => Some(AdminRelay {
                target,
                event,
                placement,
                origin_signature,
            }),
            _ => None,
        };
        Self {
            local,
            peer,
            relay,
            writing: false,
        }
    }

    fn admit(&mut self, config: &RealmConfigDocument) -> RelayTurn {
        let unavailable = MetadataTransportMessage::ForwardedWriteUnavailable;
        let (Some((local, _)), Some(relay)) = (self.local, self.relay.take()) else {
            return Ok(Next::Done(unavailable));
        };
        let admission = admit_relayed_admin(
            config,
            local,
            self.peer,
            &relay.event,
            &relay.placement,
            &relay.origin_signature,
        );
        match admission {
            RelayAdmission::Accept => {}
            RelayAdmission::Forbidden => {
                return Ok(Next::Done(MetadataTransportMessage::ForwardedWriteDenied {
                    error: WriteAuthError::Forbidden,
                }));
            }
            RelayAdmission::Reject(reason) => return Ok(Next::Done(reject(reason))),
            RelayAdmission::Unavailable => return Ok(Next::Done(unavailable)),
        }
        // The relay never mints a genesis for another origin's document.
        let record = new_outbox_record(
            local,
            relay.target,
            Vec::new(),
            DocumentOutboxEvent::relayed_admin(*relay.event, relay.origin_signature),
            relay.placement,
            false,
        );
        match outbox_write_entry(&record) {
            Ok(row) => {
                self.writing = true;
                Ok(Next::Call(IoCall::Write(vec![row])))
            }
            Err(error) => Ok(Next::Done(reject(format!(
                "relayed admin event does not encode: {error}"
            )))),
        }
    }
}

impl Operation for ApplyRelayOperation {
    type Output = MetadataTransportMessage;
    type Error = IoError;

    fn first(&mut self) -> RelayTurn {
        if self.relay.is_none() {
            return Ok(Next::Done(reject("unexpected admin relay message")));
        }
        let Some((_, realm_id)) = self.local else {
            return Ok(Next::Done(
                MetadataTransportMessage::ForwardedWriteUnavailable,
            ));
        };
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(realm_id))))
    }

    fn next(&mut self, result: IoResult) -> RelayTurn {
        if self.writing {
            done(result)?;
            return Ok(Next::Then {
                output: MetadataTransportMessage::AdminEventQueued,
                calls: vec![IoCall::Effect(Box::new(schedule_drain_effect()))],
            });
        }
        match result.child::<RealmConfigDocument, GetConfigError>()? {
            Ok(config) => self.admit(&config),
            Err(_) => Ok(Next::Done(
                MetadataTransportMessage::ForwardedWriteUnavailable,
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::peers::{failed_answer, peer_answer};
    use aruna_core::UserId;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::events::PeerRequestError;
    use aruna_core::placement::resolve_shard_holders;
    use aruna_core::structs::identity::auth::Actor;
    use aruna_core::structs::identity::realm::RealmNodeKind;
    use aruna_core::structs::placement::record::PlacementStrategy;
    use ulid::Ulid;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn config_and_placement() -> (RealmConfigDocument, PlacementRef) {
        let mut config = RealmConfigDocument::new(RealmId::from_bytes([7u8; 32]), Vec::new(), 3);
        let strategy = PlacementStrategy {
            strategy_id: Ulid::from_bytes([4u8; 16]),
            name: "default".to_string(),
            replica_count: Some(2),
            distinct_locations: false,
            affinity: Vec::new(),
            shard_count: 64,
        };
        config.default_strategy_id = Some(strategy.strategy_id);
        config.strategies = vec![strategy.clone()];
        for seed in 1..=4u8 {
            config.ensure_node(node(seed), RealmNodeKind::Server);
        }
        (
            config,
            PlacementRef {
                strategy_id: strategy.strategy_id,
                shard: 9,
            },
        )
    }

    fn relay_fixture() -> (RealmConfigDocument, PlacementRef, NodeId) {
        let (mut config, placement) = config_and_placement();
        let device = node(9);
        let owner = UserId::nil(config.realm_id);
        config.ensure_node(device, RealmNodeKind::User { owner });
        let holder = resolve_shard_holders(&config, &placement)
            .into_iter()
            .next()
            .expect("fixture has holders");
        (config, placement, holder)
    }

    fn signed_event(
        config: &RealmConfigDocument,
        placement: &PlacementRef,
        origin_seed: u8,
    ) -> (AdminDocumentEvent, iroh::Signature) {
        let secret = iroh::SecretKey::from_bytes(&[origin_seed; 32]);
        let user_id = UserId::nil(config.realm_id);
        let event = AdminDocumentEvent {
            event_id: Ulid::from_bytes([5u8; 16]),
            target: aruna_core::admin_documents::AdminDocumentTarget::Group {
                group_id: Ulid::from_bytes([6u8; 16]),
            },
            origin_node_id: secret.public(),
            origin_seq: 1,
            observed: Default::default(),
            actor: Actor {
                node_id: secret.public(),
                user_id,
                realm_id: config.realm_id,
            },
            op: aruna_core::admin_documents::AdminDocumentOperation::GroupCreated {
                realm_id: config.realm_id,
                display_name: "Engineering".to_string(),
                owner: user_id,
            },
        };
        let signature = secret.sign(&event.signing_bytes(placement).expect("event signs"));
        (event, signature)
    }

    #[test]
    fn holder_accepts_relay() {
        let (config, placement, holder) = relay_fixture();
        let (event, signature) = signed_event(&config, &placement, 1);

        assert_eq!(
            admit_relayed_admin(&config, holder, node(2), &event, &placement, &signature),
            RelayAdmission::Accept
        );
    }

    #[test]
    fn relay_rejects_forged() {
        // A relay that rewrites the actor invalidates the origin signature.
        let (config, placement, holder) = relay_fixture();
        let (event, signature) = signed_event(&config, &placement, 1);
        let mut forged = event;
        forged.actor.user_id = UserId::local(Ulid::from_bytes([8u8; 16]), config.realm_id);

        assert!(matches!(
            admit_relayed_admin(&config, holder, node(2), &forged, &placement, &signature),
            RelayAdmission::Reject(reason)
                if reason == "relayed admin event is not signed by its origin"
        ));
    }

    #[test]
    fn rejects_device_origin() {
        // A relayed admin event originated by a device may never be published.
        let (config, placement, holder) = relay_fixture();
        let (event, signature) = signed_event(&config, &placement, 9);

        assert!(matches!(
            admit_relayed_admin(&config, holder, node(2), &event, &placement, &signature),
            RelayAdmission::Reject(reason)
                if reason == "relayed admin event origin may not publish"
        ));
    }

    #[test]
    fn rejects_device_peer() {
        // A device is not a relay: it may not hand an admin event to a holder.
        let (config, placement, holder) = relay_fixture();
        let (event, signature) = signed_event(&config, &placement, 1);

        assert_eq!(
            admit_relayed_admin(&config, holder, node(9), &event, &placement, &signature),
            RelayAdmission::Forbidden
        );
    }

    #[test]
    fn nonholder_defers_relay() {
        // Another holder can still take it, so this is unavailable, not a reject.
        let (config, placement, holder) = relay_fixture();
        let (event, signature) = signed_event(&config, &placement, 1);
        let non_holder = (1..=4u8)
            .map(node)
            .find(|candidate| {
                *candidate != holder
                    && !resolve_shard_holders(&config, &placement).contains(candidate)
            })
            .expect("fixture has a non-holder");

        assert_eq!(
            admit_relayed_admin(&config, non_holder, node(2), &event, &placement, &signature),
            RelayAdmission::Unavailable
        );
    }

    fn relay(local: Option<NodeId>, holders: &[NodeId]) -> RelayAdminOperation {
        let (config, placement, _) = relay_fixture();
        let (event, signature) = signed_event(&config, &placement, 1);
        let target = DocumentTarget::RealmConfig {
            realm_id: config.realm_id,
        };
        RelayAdminOperation::new(
            local,
            holders,
            target,
            Box::new(event),
            placement,
            signature,
        )
    }

    fn asked(turn: Result<Next<()>, ForwardError>) -> (NodeId, Duration) {
        let Ok(Next::Call(IoCall::Effect(effect))) = turn else {
            panic!("expected a peer request");
        };
        let aruna_core::effects::Effect::Net(aruna_core::effects::NetEffect::MetadataRequest(
            request,
        )) = *effect
        else {
            panic!("expected a metadata request");
        };
        (request.peer, request.timeout)
    }

    #[test]
    fn relay_tries_holders() {
        // The local node is skipped, and a failed or unrelated answer moves to the next holder.
        let holders = [node(1), node(2), node(3)];
        let mut operation = relay(Some(node(1)), &holders);
        assert_eq!(asked(operation.first()), (node(2), RELAY_ATTEMPT_TIMEOUT));
        let failed = failed_answer(node(2), PeerRequestError::TimedOut);
        assert_eq!(asked(operation.next(failed)).0, node(3));
        let queued = peer_answer(node(3), &MetadataTransportMessage::AdminEventQueued);
        assert_eq!(operation.next(queued), Ok(Next::Done(())));
    }

    #[test]
    fn relay_bounds_peers() {
        let holders: Vec<_> = (1..=5).map(node).collect();
        let mut operation = relay(None, &holders);
        let mut turn = operation.first();
        for seed in 1..=RELAY_PEER_LIMIT as u8 {
            assert_eq!(asked(turn).0, node(seed));
            let answer = MetadataTransportMessage::ForwardedWriteUnavailable;
            turn = operation.next(peer_answer(node(seed), &answer));
        }
        assert_eq!(turn, Err(ForwardError::Unavailable));
    }

    #[test]
    fn relay_rejection_ends() {
        // A rejection judges the envelope, so no other holder is asked.
        let holders = [node(1), node(2)];
        let mut operation = relay(None, &holders);
        operation.first().unwrap();
        let rejected = peer_answer(node(1), &reject("invalid"));
        assert_eq!(operation.next(rejected), Err(ForwardError::Unavailable));
    }

    /// An apply operation at `local` for a signed relay from `peer`, past its first call.
    fn applying(local: NodeId, peer: NodeId) -> (ApplyRelayOperation, RealmConfigDocument) {
        let (config, placement, _) = relay_fixture();
        let (event, origin_signature) = signed_event(&config, &placement, 1);
        let message = MetadataTransportMessage::ForwardAdminEvent {
            target: DocumentTarget::RealmConfig {
                realm_id: config.realm_id,
            },
            event: Box::new(event),
            placement,
            origin_signature,
        };
        let mut operation = ApplyRelayOperation::new(Some((local, config.realm_id)), peer, message);
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        (operation, config)
    }

    fn config_child(config: RealmConfigDocument) -> IoResult {
        IoResult::Child(ChildOutcome::new(Ok::<_, GetConfigError>(config)))
    }

    #[test]
    fn apply_queues_relay() {
        // A holder writes the envelope to its outbox and wakes the drain after the commit.
        let (_, _, holder) = relay_fixture();
        let (mut operation, config) = applying(holder, node(2));
        let Ok(Next::Call(IoCall::Write(rows))) = operation.next(config_child(config)) else {
            panic!("expected the outbox row");
        };
        assert_eq!(rows.len(), 1);
        let Ok(Next::Then { output, calls }) = operation.next(IoResult::Done) else {
            panic!("expected the answer with the drain wake");
        };
        assert_eq!(output, MetadataTransportMessage::AdminEventQueued);
        assert!(matches!(calls.as_slice(), [IoCall::Effect(_)]));
    }

    #[test]
    fn apply_refuses_device() {
        let (_, _, holder) = relay_fixture();
        let (mut operation, config) = applying(holder, node(9));
        assert_eq!(
            operation.next(config_child(config)),
            Ok(Next::Done(MetadataTransportMessage::ForwardedWriteDenied {
                error: WriteAuthError::Forbidden,
            }))
        );
    }

    #[test]
    fn apply_needs_config() {
        // Without its realm view this holder is unavailable; another one may still take it.
        let (_, _, holder) = relay_fixture();
        let (mut operation, _) = applying(holder, node(2));
        let failed = Err::<RealmConfigDocument, _>(GetConfigError::DocumentNotFound);
        assert_eq!(
            operation.next(IoResult::Child(ChildOutcome::new(failed))),
            Ok(Next::Done(
                MetadataTransportMessage::ForwardedWriteUnavailable
            ))
        );
    }

    #[test]
    fn apply_refuses_others() {
        let mut other =
            ApplyRelayOperation::new(None, node(2), MetadataTransportMessage::AdminEventQueued);
        assert_eq!(
            other.first(),
            Ok(Next::Done(reject("unexpected admin relay message")))
        );
    }
}
