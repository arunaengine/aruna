//! Promotes a server node to a management node: hands it the realm key, then marks it in config.
//! The key travels only over the node-to-node metadata stream; the node applies it at its next start.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::StorageError;
use aruna_core::events::PeerRequestError;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::{
    METADATA_REPLICATION_FACTOR, RealmConfigDocument, RealmNodeKind,
};
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::EncodePrivateKey;
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use thiserror::Error;

use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::forward::fetch::{peer_call, peer_reply};
use crate::metadata::protocol::{
    MetadataReadError, MetadataTransportMessage, RealmKeyPem, encode_message,
};
use crate::metadata::transport_message_kind;
use crate::realm::ensure_config::{EnsureConfigError, EnsureConfigOperation, EnsureConfigParams};
use crate::realm::mutate_placement::{is_management, node_kind};

#[derive(Debug, Clone, PartialEq)]
pub struct PromoteNodeConfig {
    /// The serving management node and the realm admin calling it.
    pub actor: Actor,
    pub node_id: NodeId,
    pub realm_signing_key: SigningKey,
}

#[derive(Debug, Error, PartialEq)]
pub enum PromoteNodeError {
    #[error("realm config document missing")]
    ConfigMissing,
    #[error("this node is not a realm management node")]
    NotManagementNode,
    #[error("no realm node {node_id}")]
    NodeNotFound { node_id: NodeId },
    #[error("only another server node can be promoted")]
    NotServer,
    #[error(
        "the node would take over the database requests the current owner keeps, \
         and moving them is not supported yet"
    )]
    OwnerChange,
    #[error("the node could not be reached: {0}")]
    Unreachable(String),
    #[error("the answer of the node was lost; it may hold the realm key: {0}")]
    KeyUncertain(String),
    #[error("the node refused the realm key: {0:?}")]
    Refused(MetadataReadError),
    #[error("the realm key could not be encoded")]
    KeyEncoding,
    #[error(transparent)]
    Ensure(#[from] EnsureConfigError),
}

/// Time one promotion call may take, readiness checks included. It stays 10 seconds under the
/// 30 second management relay timeout, so a relayed caller gets the answer.
pub const CALL_BUDGET: Duration = Duration::from_secs(20);
const READY_INTERVAL: Duration = Duration::from_millis(500);
/// Listing attempts of one call; each one that loses its commit to another writer runs again.
const LIST_ATTEMPTS: usize = 5;

impl From<IoError> for PromoteNodeError {
    fn from(error: IoError) -> Self {
        Self::Unreachable(error.to_string())
    }
}

#[derive(Debug, PartialEq)]
enum Stage {
    Config,
    Handoff,
    List { attempts: usize },
    Check,
    Pause,
}

type PromoteTurn = Result<Next<bool>, PromoteNodeError>;

/// Hands the key first: an unreachable node leaves the realm unchanged and the call can be
/// repeated. Only a confirmed hand-off marks the node as management. `true` means the node's
/// own config lists it, so its next start applies the promotion.
#[derive(Debug, PartialEq)]
pub struct PromoteNodeOperation {
    config: PromoteNodeConfig,
    stage: Stage,
    now_ms: u64,
    deadline_ms: u64,
}

impl PromoteNodeOperation {
    pub fn new(config: PromoteNodeConfig) -> Self {
        Self {
            config,
            stage: Stage::Config,
            now_ms: 0,
            deadline_ms: 0,
        }
    }

    fn remaining(&self) -> Duration {
        Duration::from_millis(self.deadline_ms.saturating_sub(self.now_ms))
    }

    fn ask(&self, message: &MetadataTransportMessage) -> Result<IoCall, PromoteNodeError> {
        let frame = encode_message(message).map_err(PromoteNodeError::Unreachable)?;
        Ok(peer_call(self.config.node_id, frame, self.remaining()))
    }

    fn hand_key(&mut self, document: &RealmConfigDocument) -> PromoteTurn {
        check_target(document, self.config.actor.node_id, self.config.node_id)?;
        let pem = (self.config.realm_signing_key)
            .to_pkcs8_pem(LineEnding::default())
            .map_err(|_| PromoteNodeError::KeyEncoding)?;
        let message = MetadataTransportMessage::HandRealmKey {
            key: RealmKeyPem(pem.to_string()),
        };
        self.stage = Stage::Handoff;
        Ok(Next::Call(self.ask(&message)?))
    }

    fn list(&mut self, attempts: usize) -> PromoteTurn {
        self.stage = Stage::List { attempts };
        Ok(Next::Call(IoCall::child(EnsureConfigOperation::new(
            EnsureConfigParams {
                actor: self.config.actor.clone(),
                target_node_id: self.config.node_id,
                target_node_kind: RealmNodeKind::Management,
                metadata_replication_factor: METADATA_REPLICATION_FACTOR,
                realm_description: String::new(),
                create_if_missing: false,
                reject_kind_mismatch: false,
            },
        ))))
    }

    /// The config change reaches the node by replication, which may lag behind the local write.
    /// Checks and pauses all end at the deadline; a check still waiting then is not ready.
    fn check(&mut self) -> PromoteTurn {
        if self.remaining().is_zero() {
            return Ok(Next::Done(false));
        }
        self.stage = Stage::Check;
        Ok(Next::Call(
            self.ask(&MetadataTransportMessage::PromotionCheck)?,
        ))
    }

    fn pause(&mut self) -> PromoteTurn {
        if self.remaining() <= READY_INTERVAL {
            return Ok(Next::Done(false));
        }
        self.stage = Stage::Pause;
        Ok(Next::Call(IoCall::Delay(READY_INTERVAL)))
    }
}

impl Operation for PromoteNodeOperation {
    type Output = bool;
    type Error = PromoteNodeError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> PromoteTurn {
        self.deadline_ms = self.now_ms.saturating_add(CALL_BUDGET.as_millis() as u64);
        let realm_id = self.config.actor.realm_id;
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(realm_id))))
    }

    fn next(&mut self, result: IoResult) -> PromoteTurn {
        match self.stage {
            Stage::Config => {
                let document = result
                    .child::<RealmConfigDocument, GetConfigError>()?
                    .map_err(|_| PromoteNodeError::ConfigMissing)?;
                self.hand_key(&document)
            }
            Stage::Handoff => {
                handoff_outcome(peer_reply(self.config.node_id, result)?)?;
                self.list(1)
            }
            // Other writers of the realm configuration, such as a joining node's sync, can win
            // the commit; a refused commit wrote nothing, so the listing runs again.
            Stage::List { attempts } => {
                match result.child::<RealmConfigDocument, EnsureConfigError>()? {
                    Err(EnsureConfigError::StorageError(StorageError::TransactionConflict))
                        if attempts < LIST_ATTEMPTS =>
                    {
                        self.list(attempts + 1)
                    }
                    listed => {
                        listed?;
                        self.check()
                    }
                }
            }
            Stage::Check => match peer_reply(self.config.node_id, result)? {
                Ok(MetadataTransportMessage::PromotionReady { result: Ok(true) }) => {
                    Ok(Next::Done(true))
                }
                _ => self.pause(),
            },
            Stage::Pause => self.check(),
        }
    }
}

/// A lost answer can follow a stored key, which no later call can take back.
fn handoff_outcome(
    reply: Result<MetadataTransportMessage, PeerRequestError>,
) -> Result<(), PromoteNodeError> {
    match reply {
        Ok(MetadataTransportMessage::RealmKeyStored { result: Ok(()) }) => Ok(()),
        Ok(MetadataTransportMessage::RealmKeyStored { result: Err(error) }) => {
            Err(PromoteNodeError::Refused(error))
        }
        Ok(other) => Err(PromoteNodeError::Unreachable(format!(
            "unexpected answer {}",
            transport_message_kind(&other)
        ))),
        Err(PeerRequestError::NotSent(error)) => Err(PromoteNodeError::Unreachable(error)),
        Err(PeerRequestError::Failed(error)) => Err(PromoteNodeError::KeyUncertain(error)),
        Err(PeerRequestError::TimedOut) => Err(PromoteNodeError::KeyUncertain(
            "no answer before the deadline".to_string(),
        )),
    }
}

/// A listed management node is accepted again, so a retry after a lost answer succeeds.
fn check_target(
    document: &RealmConfigDocument,
    actor: NodeId,
    target: NodeId,
) -> Result<(), PromoteNodeError> {
    if !is_management(document, actor) {
        return Err(PromoteNodeError::NotManagementNode);
    }
    match node_kind(document, target) {
        None => Err(PromoteNodeError::NodeNotFound { node_id: target }),
        Some(RealmNodeKind::User { .. }) => Err(PromoteNodeError::NotServer),
        Some(_) if target == actor => Err(PromoteNodeError::NotServer),
        Some(RealmNodeKind::Server)
            if request_owner(document).is_some_and(|owner| target < owner) =>
        {
            Err(PromoteNodeError::OwnerChange)
        }
        Some(RealmNodeKind::Server | RealmNodeKind::Management) => Ok(()),
    }
}

/// The lowest management node id keeps every database request (see the management relay).
fn request_owner(document: &RealmConfigDocument) -> Option<NodeId> {
    document
        .nodes
        .iter()
        .filter(|node| node.kind == RealmNodeKind::Management)
        .filter_map(|node| node.node_id.parse::<NodeId>().ok())
        .min()
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::effects::{Effect, NetEffect};
    use aruna_core::events::{Event, MetadataAnswer, NetEvent};
    use aruna_core::keys::generate_signing_key;
    use aruna_core::structs::identity::realm::RealmId;
    use crate::metadata::protocol::{decode_message};

    /// Node ids in ascending order of `rank`, so a test knows which one owns database requests.
    fn node(rank: u8) -> NodeId {
        let mut nodes: Vec<NodeId> = (1..=10)
            .map(|seed| iroh::SecretKey::from_bytes(&[seed; 32]).public())
            .collect();
        nodes.sort();
        nodes[usize::from(rank)]
    }

    fn realm() -> (SigningKey, RealmId, RealmConfigDocument) {
        let key = generate_signing_key();
        let realm_id = RealmId::from_bytes(key.verifying_key().to_bytes());
        let mut document = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        document.ensure_node(node(1), RealmNodeKind::Management);
        document.ensure_node(node(2), RealmNodeKind::Server);
        let owner = UserId::nil(realm_id);
        document.ensure_node(node(3), RealmNodeKind::User { owner });
        (key, realm_id, document)
    }

    fn pem(key: &SigningKey) -> RealmKeyPem {
        RealmKeyPem(key.to_pkcs8_pem(LineEnding::default()).unwrap().to_string())
    }

    #[test]
    fn server_only_target() {
        let (_, _, document) = realm();
        assert!(check_target(&document, node(1), node(2)).is_ok());
        assert!(matches!(
            check_target(&document, node(1), node(3)),
            Err(PromoteNodeError::NotServer)
        ));
        assert!(matches!(
            check_target(&document, node(1), node(1)),
            Err(PromoteNodeError::NotServer)
        ));
        assert!(matches!(
            check_target(&document, node(1), node(9)),
            Err(PromoteNodeError::NodeNotFound { .. })
        ));
        assert!(matches!(
            check_target(&document, node(2), node(1)),
            Err(PromoteNodeError::NotManagementNode)
        ));
    }

    #[test]
    fn owner_change_refused() {
        let (_, _, mut document) = realm();
        document.ensure_node(node(0), RealmNodeKind::Server);
        assert!(matches!(
            check_target(&document, node(1), node(0)),
            Err(PromoteNodeError::OwnerChange)
        ));
        document.ensure_node(node(0), RealmNodeKind::Management);
        assert!(
            check_target(&document, node(1), node(0)).is_ok(),
            "a node that already owns the requests is accepted again"
        );
    }

    #[test]
    fn retry_accepts_promoted() {
        let (_, _, mut document) = realm();
        document.ensure_node(node(2), RealmNodeKind::Management);
        assert!(check_target(&document, node(1), node(2)).is_ok());
    }

    #[test]
    fn lost_answer_uncertain() {
        assert!(matches!(
            handoff_outcome(Err(PeerRequestError::TimedOut)),
            Err(PromoteNodeError::KeyUncertain(_))
        ));
        assert!(matches!(
            handoff_outcome(Err(PeerRequestError::NotSent("no route".to_string()))),
            Err(PromoteNodeError::Unreachable(_))
        ));
        let refused = MetadataTransportMessage::RealmKeyStored {
            result: Err(MetadataReadError::Forbidden),
        };
        assert!(matches!(
            handoff_outcome(Ok(refused)),
            Err(PromoteNodeError::Refused(MetadataReadError::Forbidden))
        ));
    }

    const START_MS: u64 = 1_000_000;

    fn operation() -> (PromoteNodeOperation, RealmConfigDocument) {
        let (key, realm_id, document) = realm();
        let config = PromoteNodeConfig {
            actor: Actor {
                node_id: node(1),
                user_id: aruna_core::UserId::nil(realm_id),
                realm_id,
            },
            node_id: node(2),
            realm_signing_key: key,
        };
        let mut operation = PromoteNodeOperation::new(config);
        operation.stamp(START_MS);
        (operation, document)
    }

    fn config_answer(document: RealmConfigDocument) -> IoResult {
        IoResult::Child(ChildOutcome::new(Ok::<_, GetConfigError>(document)))
    }

    fn listed(result: Result<RealmConfigDocument, EnsureConfigError>) -> IoResult {
        IoResult::Child(ChildOutcome::new(result))
    }

    fn peer_says(message: &MetadataTransportMessage) -> IoResult {
        let answer = MetadataAnswer {
            peer: node(2),
            result: Ok(encode_message(message).unwrap()),
            elapsed: Duration::ZERO,
        };
        let event = Event::Net(NetEvent::MetadataAnswer(Box::new(answer)));
        IoResult::Event(Box::new(event))
    }

    fn sent(turn: PromoteTurn) -> (MetadataTransportMessage, Duration) {
        let Ok(Next::Call(IoCall::Effect(effect))) = turn else {
            panic!("expected a peer request");
        };
        let Effect::Net(NetEffect::MetadataRequest(request)) = *effect else {
            panic!("expected a peer request");
        };
        (decode_message(&request.frame).unwrap(), request.timeout)
    }

    fn stored() -> MetadataTransportMessage {
        MetadataTransportMessage::RealmKeyStored { result: Ok(()) }
    }

    fn ready(answer: bool) -> MetadataTransportMessage {
        MetadataTransportMessage::PromotionReady {
            result: Ok(answer),
        }
    }

    /// Runs up to the first readiness check.
    fn listed_operation() -> (PromoteNodeOperation, RealmConfigDocument) {
        let (mut operation, document) = operation();
        operation.first().unwrap();
        let handed = sent(operation.next(config_answer(document.clone())));
        assert!(matches!(
            handed.0,
            MetadataTransportMessage::HandRealmKey { .. }
        ));
        assert_eq!(handed.1, CALL_BUDGET);
        let turn = operation.next(peer_says(&stored()));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
        (operation, document)
    }

    #[test]
    fn waits_for_replication() {
        let (mut operation, document) = listed_operation();
        let check = sent(operation.next(listed(Ok(document))));
        assert_eq!(check.0, MetadataTransportMessage::PromotionCheck);
        let pause = operation.next(peer_says(&ready(false)));
        assert!(matches!(
            pause,
            Ok(Next::Call(IoCall::Delay(READY_INTERVAL)))
        ));
        operation.stamp(START_MS + 5_000);
        let again = sent(operation.next(IoResult::Woke));
        assert_eq!(again.1, CALL_BUDGET - Duration::from_secs(5));
        assert_eq!(
            operation.next(peer_says(&ready(true))),
            Ok(Next::Done(true))
        );
    }

    #[test]
    fn slow_checks_bounded() {
        let (mut operation, document) = listed_operation();
        sent(operation.next(listed(Ok(document))));
        operation.stamp(START_MS + CALL_BUDGET.as_millis() as u64 - 100);
        assert_eq!(
            operation.next(peer_says(&ready(false))),
            Ok(Next::Done(false)),
            "no pause fits before the deadline"
        );
    }

    #[test]
    fn conflicts_list_again() {
        let (mut operation, _) = listed_operation();
        let lost = || Err(EnsureConfigError::StorageError(StorageError::TransactionConflict));
        for _ in 1..LIST_ATTEMPTS {
            let turn = operation.next(listed(lost()));
            assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
        }
        assert_eq!(
            operation.next(listed(lost())),
            Err(PromoteNodeError::Ensure(EnsureConfigError::StorageError(
                StorageError::TransactionConflict
            )))
        );
    }

    #[test]
    fn refused_target_sends_nothing() {
        let (mut operation, mut document) = operation();
        document.nodes.clear();
        operation.first().unwrap();
        assert_eq!(
            operation.next(config_answer(document)),
            Err(PromoteNodeError::NotManagementNode)
        );
    }

    #[test]
    fn key_debug_redacted() {
        let (key, _, _) = realm();
        let message = MetadataTransportMessage::HandRealmKey { key: pem(&key) };
        assert!(!format!("{message:?}").contains("PRIVATE KEY"));
    }
}
