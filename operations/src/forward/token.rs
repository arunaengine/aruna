//! Forwards a token revoke to ranked realm peers until one accepts or the deadline passes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::error::AuthorizeError;
use crate::auth::revoke_token::{
    RevokeTokenAdmission, RevokeTokenConfig, RevokeTokenError, RevokeTokenOperation,
};
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::forward::authorize::{
    ForwardAuthError, ForwardedCallerOperation, forward_auth_error, write_check,
};
use crate::forward::fetch::{peer_call, peer_reply};
use crate::forward::routing::distinct_holders;
use crate::forward::transport::{ForwardError, reject};
use crate::metadata::message_kind::transport_message_kind;
use crate::metadata::protocol::{MetadataTransportMessage, WriteAuthError, encode_message};
use aruna_core::NodeId;
use aruna_core::auth::{bearer_token_hash, valid_revocation_expiry};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::events::{Event, NetEvent, PeerRequestError};
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::placement::selector::select_top_peers;
use aruna_core::rows::event;
use aruna_core::structs::identity::auth::{Actor, AuthContext, TokenClaims};
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

#[derive(Debug, PartialEq)]
enum ApplyStage {
    Config,
    Caller,
    Claims,
    Write,
    Revoke,
}

type ApplyTurn = Result<Next<MetadataTransportMessage>, IoError>;

fn done(message: MetadataTransportMessage) -> ApplyTurn {
    Ok(Next::Done(message))
}

fn denied(error: WriteAuthError) -> ApplyTurn {
    done(MetadataTransportMessage::ForwardedWriteDenied { error })
}

fn unavailable() -> ApplyTurn {
    done(MetadataTransportMessage::ForwardedWriteUnavailable)
}

/// Runs a token revocation a peer forwarded: the caller may always retire its own token, and
/// needs realm admin rights for anyone else's.
#[derive(Debug, PartialEq)]
pub(crate) struct ApplyRevokeOperation {
    local: Option<(NodeId, RealmId)>,
    peer: NodeId,
    message: MetadataTransportMessage,
    now_ms: u64,
    stage: ApplyStage,
    auth: Option<AuthContext>,
    revoke: Option<RevokeTokenConfig>,
}

impl ApplyRevokeOperation {
    pub(crate) fn new(
        local: Option<(NodeId, RealmId)>,
        peer: NodeId,
        message: MetadataTransportMessage,
    ) -> Self {
        Self {
            local,
            peer,
            message,
            now_ms: 0,
            stage: ApplyStage::Config,
            auth: None,
            revoke: None,
        }
    }

    fn check_caller(&mut self, config: &RealmConfigDocument) -> ApplyTurn {
        let Some((node_id, realm_id)) = self.local else {
            return unavailable();
        };
        let local = (config.nodes.iter()).find(|node| node.node_id == node_id.to_string());
        if !local.is_some_and(|node| node.kind.is_sync_eligible()) {
            return unavailable();
        }
        let MetadataTransportMessage::ForwardTokenRevocation { auth_token, .. } = &self.message
        else {
            return done(reject("unexpected token revocation message"));
        };
        if !matches!(auth_token, AuthToken::Bearer(_)) {
            return denied(WriteAuthError::Unauthorized);
        }
        self.stage = ApplyStage::Caller;
        Ok(Next::Call(IoCall::child(ForwardedCallerOperation::new(
            self.peer,
            realm_id,
            &self.message,
        ))))
    }

    fn read_claims(&mut self, auth: AuthContext) -> ApplyTurn {
        let MetadataTransportMessage::ForwardTokenRevocation { token, .. } = &self.message else {
            return done(reject("unexpected token revocation message"));
        };
        let target = match AuthToken::bearer(token) {
            Ok(target) => target,
            Err(error) => return done(reject(format!("invalid token revocation target: {error}"))),
        };
        self.auth = Some(auth);
        self.stage = ApplyStage::Claims;
        let effect = Effect::Net(NetEffect::RevocationClaims(Box::new(target)));
        Ok(Next::Call(IoCall::Effect(Box::new(effect))))
    }

    /// A path-restricted caller may retire only the token it presented, never another one.
    fn admit(&mut self, claims: TokenClaims) -> ApplyTurn {
        let (Some((node_id, realm_id)), Some(auth)) = (self.local, self.auth.take()) else {
            return unavailable();
        };
        let MetadataTransportMessage::ForwardTokenRevocation { auth_token, token } = &self.message
        else {
            return done(reject("unexpected token revocation message"));
        };
        let AuthToken::Bearer(caller_token) = auth_token else {
            return denied(WriteAuthError::Unauthorized);
        };
        let token_hash = bearer_token_hash(token);
        let self_hash = bearer_token_hash(caller_token.as_str()) == token_hash;
        let expires_at = claims.exp;
        let now = self.now_ms / 1000;
        if !valid_revocation_expiry(expires_at, now) {
            return done(reject(
                "token revocation expiry is outside the supported window",
            ));
        }
        let subject: AuthContext = match claims.try_into() {
            Ok(subject) => subject,
            Err(error) => {
                return done(reject(format!("invalid token revocation subject: {error}")));
            }
        };
        if subject.realm_id != realm_id {
            return denied(WriteAuthError::Forbidden);
        }
        let self_service =
            auth.user_id == subject.user_id && (auth.path_restrictions.is_none() || self_hash);
        self.revoke = Some(RevokeTokenConfig {
            actor: Actor {
                node_id,
                user_id: auth.user_id,
                realm_id,
            },
            token_hash,
            expires_at,
            token_owner: subject.user_id,
            admission: match self_service {
                true => RevokeTokenAdmission::SelfService,
                false => RevokeTokenAdmission::Privileged,
            },
            now,
        });
        if self_service {
            return self.revoke_token();
        }
        self.stage = ApplyStage::Write;
        let path = format!("/{realm_id}/admin/u/{}", subject.user_id);
        Ok(Next::Call(IoCall::child(write_check(&auth, &path))))
    }

    fn revoke_token(&mut self) -> ApplyTurn {
        let Some(config) = self.revoke.take() else {
            return unavailable();
        };
        self.stage = ApplyStage::Revoke;
        Ok(Next::Call(IoCall::child(RevokeTokenOperation::new(config))))
    }
}

impl Operation for ApplyRevokeOperation {
    type Output = MetadataTransportMessage;
    type Error = IoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> ApplyTurn {
        let Some((_, realm_id)) = self.local else {
            return unavailable();
        };
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(realm_id))))
    }

    fn next(&mut self, result: IoResult) -> ApplyTurn {
        match self.stage {
            ApplyStage::Config => match result.child::<RealmConfigDocument, GetConfigError>()? {
                Ok(config) => self.check_caller(&config),
                Err(_) => unavailable(),
            },
            ApplyStage::Caller => match result.child::<AuthContext, ForwardAuthError>()? {
                Ok(auth) => self.read_claims(auth),
                Err(error) => done(forward_auth_error(error)),
            },
            ApplyStage::Claims => match event(result)? {
                Event::Net(NetEvent::RevocationClaims(Ok(claims))) => self.admit(*claims),
                Event::Net(NetEvent::RevocationClaims(Err(error))) => {
                    done(reject(format!("invalid token revocation target: {error}")))
                }
                other => Err(IoError::Unexpected {
                    expected: "the claims of a token",
                    got: other.label(),
                }),
            },
            ApplyStage::Write => match result.child::<(), AuthorizeError>()? {
                Ok(()) => self.revoke_token(),
                Err(error) => done(forward_auth_error(error.into())),
            },
            ApplyStage::Revoke => done(
                match result.child::<RealmConfigDocument, RevokeTokenError>()? {
                    Ok(_) => MetadataTransportMessage::ForwardedTokenRevoked,
                    Err(RevokeTokenError::CapacityReached) => {
                        MetadataTransportMessage::TokenRevocationCapacity
                    }
                    Err(error) => reject(format!("token revocation failed: {error}")),
                },
            ),
        }
    }
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

    fn apply_message(token: &str) -> MetadataTransportMessage {
        MetadataTransportMessage::ForwardTokenRevocation {
            auth_token: AuthToken::bearer("caller-token").unwrap(),
            token: token.to_string(),
        }
    }

    fn eligible_config(realm_id: RealmId) -> RealmConfigDocument {
        use aruna_core::structs::identity::realm::RealmNodeKind;
        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 2);
        config.ensure_node(node(1), RealmNodeKind::Server);
        config
    }

    fn config_answer(config: RealmConfigDocument) -> IoResult {
        let answer = aruna_core::calls::ChildOutcome::new(Ok::<_, GetConfigError>(config));
        IoResult::Child(answer)
    }

    #[test]
    fn apply_needs_server() {
        // A node that is no realm member takes no revocation.
        let realm_id = RealmId::from_bytes([1; 32]);
        let mut orphan = ApplyRevokeOperation::new(None, node(2), apply_message("target"));
        assert_eq!(
            orphan.first(),
            Ok(Next::Done(
                MetadataTransportMessage::ForwardedWriteUnavailable
            ))
        );
        let local = Some((node(5), realm_id));
        let mut outsider = ApplyRevokeOperation::new(local, node(2), apply_message("target"));
        outsider.first().unwrap();
        assert_eq!(
            outsider.next(config_answer(eligible_config(realm_id))),
            Ok(Next::Done(
                MetadataTransportMessage::ForwardedWriteUnavailable
            ))
        );
    }

    #[test]
    fn apply_checks_caller() {
        // A refused caller is answered with its refusal, and nothing is revoked.
        let realm_id = RealmId::from_bytes([1; 32]);
        let local = Some((node(1), realm_id));
        let mut operation = ApplyRevokeOperation::new(local, node(2), apply_message("target"));
        operation.first().unwrap();
        let turn = operation.next(config_answer(eligible_config(realm_id)));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
        let refused = Err::<AuthContext, _>(ForwardAuthError::Forbidden);
        let answer = IoResult::Child(aruna_core::calls::ChildOutcome::new(refused));
        assert_eq!(operation.next(answer), denied(WriteAuthError::Forbidden));
    }
}

#[cfg(test)]
#[path = "revoke_tests.rs"]
mod apply_tests;
