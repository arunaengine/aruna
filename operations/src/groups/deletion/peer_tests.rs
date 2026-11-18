//! Tests the peer side of group deletion as calls and results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::UserId;
use aruna_core::calls::ChildOutcome;
use aruna_core::events::MetadataAnswer;
use aruna_core::structs::identity::realm::RealmNodeKind;
use std::collections::BTreeSet;

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn realm() -> RealmId {
    RealmId::from_bytes([1; 32])
}

fn local() -> Option<(NodeId, RealmId)> {
    Some((node(1), realm()))
}

fn config() -> RealmConfigDocument {
    let mut config = RealmConfigDocument::new(realm(), Vec::new(), 2);
    config.ensure_node(node(1), RealmNodeKind::Server);
    config.ensure_node(node(2), RealmNodeKind::Server);
    config
}

fn plan(coordinator: NodeId) -> Box<GroupDeletePlan> {
    Box::new(GroupDeletePlan {
        request_id: Ulid::from_bytes([5; 16]),
        group_id: Ulid::from_bytes([3; 16]),
        realm_id: realm(),
        owner: UserId::nil(realm()),
        requested_by: UserId::nil(realm()),
        coordinator,
        nodes: BTreeSet::from([node(1), node(2)]),
    })
}

fn config_answer(config: RealmConfigDocument) -> IoResult {
    use crate::document_repository::GetConfigError;
    IoResult::Child(ChildOutcome::new(Ok::<_, GetConfigError>(config)))
}

fn done() -> IoResult {
    IoResult::Child(ChildOutcome::new(Ok::<(), GroupDeletionError>(())))
}

fn abort(plan: Box<GroupDeletePlan>) -> GroupDeleteAction {
    GroupDeleteAction::Abort { plan }
}

fn control(peer: NodeId, action: GroupDeleteAction) -> ApplyControlOperation {
    let mut operation = ApplyControlOperation::new(local(), peer, action);
    operation.first().unwrap();
    operation
}

#[test]
fn control_needs_network() {
    let action = abort(plan(node(2)));
    let mut operation = ApplyControlOperation::new(None, node(2), action);
    assert_eq!(operation.first(), Err(network_missing()));
}

#[test]
fn outsider_refused() {
    let mut operation = control(node(9), abort(plan(node(9))));
    assert_eq!(
        operation.next(config_answer(config())),
        Err(GroupDeletionError::Unauthorized)
    );
}

#[test]
fn abort_only_coordinator() {
    let mut operation = control(node(2), abort(plan(node(1))));
    assert_eq!(
        operation.next(config_answer(config())),
        Err(GroupDeletionError::Unauthorized)
    );
}

#[test]
fn abort_applies() {
    let mut operation = control(node(2), abort(plan(node(2))));
    let turn = operation.next(config_answer(config()));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(operation.next(done()), Ok(Next::Done(None)));
}

#[test]
fn prepare_signs_plan() {
    let action = GroupDeleteAction::Prepare {
        plan: plan(node(1)),
    };
    let mut operation = control(node(1), action);
    operation.next(config_answer(config())).unwrap();
    let Ok(Next::Call(IoCall::Effect(effect))) = operation.next(done()) else {
        panic!("expected the signing effect");
    };
    assert!(matches!(*effect, Effect::Net(NetEffect::SignDeletePlan(_))));
    let proof = GroupDeleteProof {
        node_id: node(1),
        signature: iroh::SecretKey::from_bytes(&[1; 32]).sign(b"plan"),
    };
    let signed = Event::Net(NetEvent::DeletePlanSigned(Ok(proof.clone())));
    assert_eq!(
        operation.next(IoResult::Event(Box::new(signed))),
        Ok(Next::Done(Some(proof)))
    );
}

#[test]
fn start_needs_request() {
    let mut operation = ApplyControlOperation::new(
        local(),
        node(2),
        GroupDeleteAction::Start {
            group_id: Ulid::from_bytes([3; 16]),
        },
    );
    operation.first().unwrap();
    assert!(matches!(
        operation.next(config_answer(config())),
        Err(GroupDeletionError::Invalid(_))
    ));
}

#[test]
fn request_stays_local() {
    let mut operation = RequestNodeOperation::new(local(), node(1), None, abort(plan(node(1))));
    assert!(matches!(
        operation.first(),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let reply = IoResult::Child(ChildOutcome::new(Ok::<_, GroupDeletionError>(
        None::<GroupDeleteProof>,
    )));
    assert_eq!(operation.next(reply), Ok(Next::Done(None)));
}

fn peer_says(peer: NodeId, message: &MetadataTransportMessage) -> IoResult {
    let answer = MetadataAnswer {
        peer,
        result: Ok(encode_message(message).unwrap()),
        elapsed: Duration::ZERO,
    };
    IoResult::Event(Box::new(Event::Net(NetEvent::MetadataAnswer(Box::new(
        answer,
    )))))
}

#[test]
fn remote_refusal_named() {
    let mut operation = RequestNodeOperation::new(local(), node(2), None, abort(plan(node(1))));
    assert!(matches!(
        operation.first(),
        Ok(Next::Call(IoCall::Effect(_)))
    ));
    let turn = operation.next(peer_says(node(2), &MetadataTransportMessage::WriteNotFound));
    assert_eq!(
        turn,
        Err(GroupDeletionError::Unavailable(format!(
            "node {} refused the deletion request",
            node(2)
        )))
    );
}

#[test]
fn only_local_finishes() {
    let action = abort(plan(node(1)));
    let mut operation = RequestNodesOperation::new(local(), vec![node(1)], action);
    assert!(matches!(
        operation.first(),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let reply = IoResult::Child(ChildOutcome::new(Ok::<_, GroupDeletionError>(
        None::<GroupDeleteProof>,
    )));
    assert_eq!(
        operation.next(reply),
        Ok(Next::Done(vec![(node(1), Ok(None))]))
    );
}

#[test]
fn failed_fanout_marked() {
    let action = abort(plan(node(1)));
    let mut operation = RequestNodesOperation::new(None, vec![node(2)], action);
    assert!(matches!(
        operation.first(),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let failed = Err::<Vec<PeerAnswer>, _>(FetchError::Unfinished);
    let turn = operation.next(IoResult::Child(ChildOutcome::new(failed)));
    assert!(matches!(
        turn,
        Ok(Next::Done(replies)) if matches!(replies[0].1, Err(GroupDeletionError::Unavailable(_)))
    ));
}
