//! Tests forwarding a group create and applying one as calls and results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::tests::peers::peer_answer;
use aruna_core::UserId;
use aruna_core::calls::ChildOutcome;
use aruna_core::events::{Event, MetadataAnswer, NetEvent};
use aruna_core::structs::identity::realm::RealmNodeKind;

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn realm() -> RealmId {
    RealmId::from_bytes([1; 32])
}

fn config() -> RealmConfigDocument {
    let mut config = RealmConfigDocument::new(realm(), Vec::new(), 2);
    config.ensure_node(node(1), RealmNodeKind::Server);
    config.ensure_node(node(2), RealmNodeKind::Server);
    config
}

fn config_answer() -> IoResult {
    IoResult::Child(ChildOutcome::new(Ok::<_, GetConfigError>(config())))
}

fn peer_failed(peer: NodeId, error: PeerRequestError) -> IoResult {
    let answer = MetadataAnswer {
        peer,
        result: Err(error),
        elapsed: Duration::ZERO,
    };
    IoResult::Event(Box::new(Event::Net(NetEvent::MetadataAnswer(Box::new(
        answer,
    )))))
}

fn forwarding() -> ForwardGroupOperation {
    let mut operation = ForwardGroupOperation::new(
        realm(),
        Some(node(9)),
        AuthToken::bearer("caller-token").unwrap(),
        "Group".to_string(),
        Ulid::from_bytes([7; 16]),
    );
    operation.first().unwrap();
    assert!(matches!(
        operation.next(config_answer()),
        Ok(Next::Call(IoCall::Effect(_)))
    ));
    operation
}

#[test]
fn unsent_tries_next() {
    let mut operation = forwarding();
    let peer = operation.asked.unwrap();
    let failed = peer_failed(peer, PeerRequestError::NotSent("no route".to_string()));
    assert!(matches!(
        operation.next(failed),
        Ok(Next::Call(IoCall::Effect(_)))
    ));
    assert_ne!(operation.asked, Some(peer));
}

#[test]
fn sent_failure_stops() {
    // The ingress may have created the group, so no other one is asked.
    for error in [
        PeerRequestError::Failed("cut".to_string()),
        PeerRequestError::TimedOut,
    ] {
        let mut operation = forwarding();
        let peer = operation.asked.unwrap();
        assert_eq!(
            operation.next(peer_failed(peer, error)),
            Err(ForwardGroupError::Unavailable)
        );
    }
}

#[test]
fn every_peer_unreachable() {
    let mut operation = forwarding();
    for _ in 0..2 {
        let peer = operation.asked.unwrap();
        let failed = peer_failed(peer, PeerRequestError::NotSent("no route".to_string()));
        let turn = operation.next(failed);
        if operation.asked.is_none() {
            assert_eq!(turn, Err(ForwardGroupError::Unavailable));
            return;
        }
    }
    panic!("the walk must end after the last peer");
}

#[test]
fn conflict_is_callers() {
    let mut operation = forwarding();
    let peer = operation.asked.unwrap();
    let conflict = MetadataTransportMessage::GroupCreateConflict {
        reason: "limit".to_string(),
    };
    assert_eq!(
        operation.next(peer_answer(peer, &conflict)),
        Err(ForwardGroupError::Conflict("limit".to_string()))
    );
}

fn create_message() -> MetadataTransportMessage {
    MetadataTransportMessage::ForwardGroupCreate {
        auth_token: Some(AuthToken::bearer("caller-token").unwrap()),
        display_name: "Group".to_string(),
    }
}

fn applying(local: NodeId) -> (ApplyGroupOperation, ApplyTurn) {
    let mut operation = ApplyGroupOperation::new(Some((local, realm())), node(2), create_message());
    operation.first().unwrap();
    let turn = operation.next(config_answer());
    (operation, turn)
}

#[test]
fn apply_needs_server() {
    let (_, turn) = applying(node(9));
    assert_eq!(turn, unavailable());
}

#[test]
fn restricted_caller_denied() {
    let (mut operation, _) = applying(node(1));
    let restricted = AuthContext {
        user_id: UserId::local(Ulid::from_bytes([4; 16]), realm()),
        realm_id: realm(),
        path_restrictions: Some(Vec::new()),
        session: None,
    };
    let answer = Ok::<_, ForwardAuthError>(restricted);
    assert_eq!(
        operation.next(IoResult::Child(ChildOutcome::new(answer))),
        Ok(Next::Done(MetadataTransportMessage::ForwardedWriteDenied {
            error: WriteAuthError::Forbidden,
        }))
    );
}

#[test]
fn refused_caller_answered() {
    let (mut operation, _) = applying(node(1));
    let refused = Err::<AuthContext, _>(ForwardAuthError::Unauthorized);
    assert_eq!(
        operation.next(IoResult::Child(ChildOutcome::new(refused))),
        Ok(Next::Done(MetadataTransportMessage::ForwardedWriteDenied {
            error: WriteAuthError::Unauthorized,
        }))
    );
}

#[test]
fn limit_is_conflict() {
    let (mut operation, _) = applying(node(1));
    operation.stage = ApplyStage::Create;
    let limited = Err::<CreatedGroup, _>(CreateGroupError::GroupLimitReached { limit: 3 });
    assert_eq!(
        operation.next(IoResult::Child(ChildOutcome::new(limited))),
        Ok(Next::Done(MetadataTransportMessage::GroupCreateConflict {
            reason: "owned group limit reached (3)".to_string(),
        }))
    );
}
