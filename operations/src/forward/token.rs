//! Forwards a token revoke to ranked realm peers until one accepts or the deadline passes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::forward::fetch::{peer_call, peer_reply};
use crate::forward::routing::distinct_holders;
use crate::forward::transport::ForwardError;
use crate::metadata::handle::transport_message_kind;
use crate::metadata::protocol::{MetadataTransportMessage, WriteAuthError, encode_message};
use aruna_core::NodeId;
use aruna_core::auth::bearer_token_hash;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::events::PeerRequestError;
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::placement::selector::select_top_peers;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use std::str::FromStr;
use std::time::Duration;
use tracing::warn;
use ulid::Ulid;

const REVOKE_PEER_LIMIT: usize = 4;

const REVOKE_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(3);

const TOKEN_REVOKE_DEADLINE: Duration = Duration::from_secs(15);

type RevokeTurn = Result<Next<()>, ForwardError>;

/// Asks ranked realm peers to revoke a token until one accepts or the deadline passes. A
/// revocation is keyed by token hash, so an ambiguous attempt is safe to replay on the next peer.
#[derive(Debug, PartialEq)]
pub struct ForwardRevokeOperation {
    realm_id: RealmId,
    local: Option<NodeId>,
    subject: Vec<u8>,
    frame: Result<Vec<u8>, String>,
    now_ms: u64,
    deadline_ms: u64,
    /// Reversed, so the next peer pops off the end.
    peers: Vec<NodeId>,
    asked: Option<NodeId>,
}

impl ForwardRevokeOperation {
    /// `salt` varies the peer order between attempts at the same token.
    pub fn new(
        realm_id: RealmId,
        local: Option<NodeId>,
        auth_token: AuthToken,
        token: String,
        salt: Ulid,
    ) -> Self {
        let mut subject = bearer_token_hash(&token).into_bytes();
        subject.extend_from_slice(&salt.to_bytes());
        let message = MetadataTransportMessage::ForwardTokenRevocation { auth_token, token };
        Self {
            realm_id,
            local,
            subject,
            frame: encode_message(&message),
            now_ms: 0,
            deadline_ms: 0,
            peers: Vec::new(),
            asked: None,
        }
    }

    fn plan(&mut self, config: &RealmConfigDocument) -> RevokeTurn {
        let candidates = (config.nodes.iter())
            .filter(|node| node.kind.is_sync_eligible())
            .filter_map(|node| NodeId::from_str(&node.node_id).ok())
            .filter(|peer| Some(*peer) != self.local);
        self.set_peers(rank_revoke_peers(candidates, &self.subject));
        self.ask()
    }

    fn set_peers(&mut self, ranked: Vec<NodeId>) {
        let mut peers = distinct_holders(&ranked);
        peers.truncate(REVOKE_PEER_LIMIT);
        peers.reverse();
        self.peers = peers;
    }

    /// Asks the next peer with what is left of the deadline, at most one attempt's share.
    fn ask(&mut self) -> RevokeTurn {
        let remaining = self.deadline_ms.saturating_sub(self.now_ms);
        let frame = self.frame.as_ref().map_err(|error| {
            warn!(%error, "A forwarded token revocation does not encode");
            ForwardError::Unavailable
        })?;
        let (Some(peer), true) = (self.peers.pop(), remaining > 0) else {
            return Err(ForwardError::Unavailable);
        };
        self.asked = Some(peer);
        let timeout = Duration::from_millis(remaining).min(REVOKE_ATTEMPT_TIMEOUT);
        Ok(Next::Call(peer_call(peer, frame.clone(), timeout)))
    }

    fn answered(
        &mut self,
        peer: NodeId,
        reply: Result<MetadataTransportMessage, PeerRequestError>,
    ) -> RevokeTurn {
        match reply {
            Ok(MetadataTransportMessage::ForwardedTokenRevoked) => Ok(Next::Done(())),
            Ok(MetadataTransportMessage::ForwardedWriteDenied {
                error: WriteAuthError::Unauthorized,
            }) => Err(ForwardError::Unauthorized),
            Ok(MetadataTransportMessage::ForwardedWriteDenied {
                error: WriteAuthError::Forbidden,
            }) => Err(ForwardError::Forbidden),
            Ok(
                MetadataTransportMessage::ForwardedWriteUnavailable
                | MetadataTransportMessage::TokenRevocationCapacity,
            ) => self.ask(),
            Ok(MetadataTransportMessage::Reject(error)) => {
                warn!(%peer, %error, "Peer rejected a forwarded token revocation");
                Err(ForwardError::Unavailable)
            }
            Ok(response) => {
                let response = transport_message_kind(&response);
                warn!(%peer, response, "Peer returned an unexpected token revocation response");
                Err(ForwardError::Unavailable)
            }
            Err(PeerRequestError::TimedOut) => {
                warn!(%peer, "Token revocation forwarding attempt timed out");
                self.ask()
            }
            Err(PeerRequestError::Failed(error) | PeerRequestError::NotSent(error)) => {
                warn!(%peer, %error, "Failed to forward a token revocation");
                self.ask()
            }
        }
    }
}

impl Operation for ForwardRevokeOperation {
    type Output = ();
    type Error = ForwardError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> RevokeTurn {
        self.deadline_ms = self.now_ms + TOKEN_REVOKE_DEADLINE.as_millis() as u64;
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(
            self.realm_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> RevokeTurn {
        if let Some(peer) = self.asked.take() {
            let reply = peer_reply(peer, result)?;
            return self.answered(peer, reply);
        }
        let config = result
            .child::<RealmConfigDocument, GetConfigError>()?
            .map_err(|_| ForwardError::Unavailable)?;
        self.plan(&config)
    }
}

fn rank_revoke_peers(peers: impl IntoIterator<Item = NodeId>, subject: &[u8]) -> Vec<NodeId> {
    select_top_peers(peers, subject, REVOKE_PEER_LIMIT, |_| {})
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::peers::{failed_answer, peer_answer};

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    /// A revocation that planned `peers` and has its full deadline left at time 1000.
    fn planned(peers: &[NodeId]) -> ForwardRevokeOperation {
        let mut operation = ForwardRevokeOperation::new(
            RealmId::from_bytes([1; 32]),
            None,
            AuthToken::bearer("caller-token").unwrap(),
            "target-token".to_string(),
            Ulid::from_bytes([2; 16]),
        );
        operation.stamp(1_000);
        assert!(matches!(
            operation.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        operation.set_peers(peers.to_vec());
        operation
    }

    fn asked(turn: RevokeTurn) -> (NodeId, Duration) {
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
    fn plans_ranked_peers() {
        // Only sync-eligible peers other than this node are asked.
        use aruna_core::structs::identity::realm::RealmNodeKind;
        let realm_id = RealmId::from_bytes([1; 32]);
        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 2);
        config.ensure_node(node(1), RealmNodeKind::Server);
        config.ensure_node(node(2), RealmNodeKind::Server);
        config.ensure_node(
            node(3),
            RealmNodeKind::User {
                owner: aruna_core::UserId::nil(realm_id),
            },
        );
        let mut operation = ForwardRevokeOperation::new(
            realm_id,
            Some(node(1)),
            AuthToken::bearer("caller-token").unwrap(),
            "target-token".to_string(),
            Ulid::from_bytes([2; 16]),
        );
        operation.first().unwrap();
        operation.stamp(1);
        let child = aruna_core::calls::ChildOutcome::new(Ok::<_, GetConfigError>(config));
        assert_eq!(asked(operation.next(IoResult::Child(child))).0, node(2));
        assert!(operation.peers.is_empty());
    }

    #[test]
    fn missing_config_unavailable() {
        let mut operation = planned(&[]);
        let failed = Err::<RealmConfigDocument, _>(GetConfigError::DocumentNotFound);
        let child = aruna_core::calls::ChildOutcome::new(failed);
        assert_eq!(
            operation.next(IoResult::Child(child)),
            Err(ForwardError::Unavailable)
        );
    }

    #[test]
    fn capacity_then_success() {
        let mut operation = planned(&[node(1), node(2)]);
        let (peer, timeout) = asked(operation.ask());
        assert_eq!((peer, timeout), (node(1), REVOKE_ATTEMPT_TIMEOUT));
        let full = peer_answer(node(1), &MetadataTransportMessage::TokenRevocationCapacity);
        assert_eq!(asked(operation.next(full)).0, node(2));
        let revoked = peer_answer(node(2), &MetadataTransportMessage::ForwardedTokenRevoked);
        assert_eq!(operation.next(revoked), Ok(Next::Done(())));
    }

    #[test]
    fn retries_failed_send() {
        // A failed or timed out attempt moves on: replaying a revocation is safe.
        let mut operation = planned(&[node(1), node(2), node(3)]);
        operation.ask().unwrap();
        let failed = failed_answer(
            node(1),
            PeerRequestError::Failed(aruna_core::metadata::MetadataError::Backend(
                "gone".to_string(),
            )),
        );
        assert_eq!(asked(operation.next(failed)).0, node(2));
        let late = failed_answer(node(2), PeerRequestError::TimedOut);
        assert_eq!(asked(operation.next(late)).0, node(3));
    }

    #[test]
    fn all_capacity_unavailable() {
        let mut operation = planned(&[node(1)]);
        operation.ask().unwrap();
        let full = peer_answer(node(1), &MetadataTransportMessage::TokenRevocationCapacity);
        assert_eq!(operation.next(full), Err(ForwardError::Unavailable));
    }

    #[test]
    fn verdicts_stop_retry() {
        // A refusal or a rejection is final, whatever peers remain.
        let denied = |error| MetadataTransportMessage::ForwardedWriteDenied { error };
        let cases = [
            (
                denied(WriteAuthError::Unauthorized),
                ForwardError::Unauthorized,
            ),
            (denied(WriteAuthError::Forbidden), ForwardError::Forbidden),
            (
                MetadataTransportMessage::Reject("invalid token".to_string()),
                ForwardError::Unavailable,
            ),
            (
                MetadataTransportMessage::AdminEventQueued,
                ForwardError::Unavailable,
            ),
        ];
        for (answer, expected) in cases {
            let mut operation = planned(&[node(1), node(2)]);
            operation.ask().unwrap();
            let answer = peer_answer(node(1), &answer);
            assert_eq!(operation.next(answer), Err(expected));
        }
    }

    #[test]
    fn asks_peer_once() {
        let mut operation = planned(&[node(1), node(1)]);
        operation.ask().unwrap();
        let full = peer_answer(node(1), &MetadataTransportMessage::TokenRevocationCapacity);
        assert_eq!(operation.next(full), Err(ForwardError::Unavailable));
    }

    #[test]
    fn bounded_peer_order() {
        let peers = (1..=16).map(node).collect::<Vec<_>>();
        let reversed = peers.iter().copied().rev().collect::<Vec<_>>();
        let subject = bearer_token_hash("target-token");
        let first = rank_revoke_peers(peers.iter().copied(), subject.as_bytes());
        let second = rank_revoke_peers(reversed.iter().copied(), subject.as_bytes());

        assert_eq!(first, second);
        assert_eq!(first.len(), REVOKE_PEER_LIMIT);
        assert!(first.iter().all(|peer| peers.contains(peer)));
    }

    #[test]
    fn deadline_limits_attempts() {
        // The last attempt gets what remains, and none starts once the deadline passed.
        let mut operation = planned(&[node(1), node(2)]);
        operation.stamp(operation.deadline_ms - 1_000);
        assert_eq!(asked(operation.ask()).1, Duration::from_secs(1));
        operation.stamp(operation.deadline_ms);
        let late = failed_answer(node(1), PeerRequestError::TimedOut);
        assert_eq!(operation.next(late), Err(ForwardError::Unavailable));
    }
}
