//! Tests the peer side of group deletion as calls and results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::tests::calls::caller_of;
use crate::tests::peers::peer_answer;
use aruna_core::UserId;
use aruna_core::calls::ChildOutcome;
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
        owner: UserId::local(Ulid::from_bytes([6; 16]), realm()),
        requested_by: UserId::local(Ulid::from_bytes([7; 16]), realm()),
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
    // Only the realm's coordinator, its lowest server node, may ask for a confirmation.
    let coordinator = node(1).min(node(2));
    let action = GroupDeleteAction::Prepare {
        plan: plan(coordinator),
    };
    let mut operation = control(coordinator, action);
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

#[test]
fn remote_refusal_named() {
    let mut operation = RequestNodeOperation::new(local(), node(2), None, abort(plan(node(1))));
    assert!(matches!(
        operation.first(),
        Ok(Next::Call(IoCall::Effect(_)))
    ));
    let turn = operation.next(peer_answer(
        node(2),
        &MetadataTransportMessage::WriteNotFound,
    ));
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

fn start_message() -> MetadataTransportMessage {
    MetadataTransportMessage::GroupDeletion {
        auth_token: None,
        action: GroupDeleteAction::Start {
            group_id: Ulid::from_bytes([3; 16]),
        },
    }
}

fn starting() -> ApplyRequestOperation {
    let request = Ulid::from_bytes([6; 16]);
    let mut operation = ApplyRequestOperation::new(local(), node(2), start_message(), request);
    assert!(matches!(
        operation.first(),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    operation
}

fn caller_refused(error: ForwardAuthError) -> IoResult {
    IoResult::Child(ChildOutcome::new(Err::<AuthContext, _>(error)))
}

#[test]
fn request_needs_network() {
    let request = Ulid::from_bytes([6; 16]);
    let mut operation = ApplyRequestOperation::new(None, node(2), start_message(), request);
    assert_eq!(operation.first(), Err(network_missing()));
}

#[test]
fn other_message_invalid() {
    let message = MetadataTransportMessage::WriteNotFound;
    let request = Ulid::from_bytes([6; 16]);
    let mut operation = ApplyRequestOperation::new(local(), node(2), message, request);
    assert!(matches!(
        operation.first(),
        Err(GroupDeletionError::Invalid(_))
    ));
}

#[test]
fn start_runs_deletion() {
    let mut operation = starting();
    let accepted = IoResult::Child(ChildOutcome::new(Ok::<_, ForwardAuthError>(caller_of(
        realm(),
    ))));
    assert!(matches!(
        operation.next(accepted),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(operation.stage, RequestStage::Run);
    assert_eq!(operation.next(done()), Ok(Next::Done(None)));
}

#[test]
fn refused_start_unauthorized() {
    assert_eq!(
        starting().next(caller_refused(ForwardAuthError::Forbidden)),
        Err(GroupDeletionError::Unauthorized)
    );
}

#[test]
fn unavailable_start_kept() {
    let error = ForwardAuthError::Unavailable("down".to_string());
    assert_eq!(
        starting().next(caller_refused(error)),
        Err(GroupDeletionError::Unavailable("down".to_string()))
    );
}

#[test]
fn control_request_answers() {
    let message = MetadataTransportMessage::GroupDeletion {
        auth_token: None,
        action: abort(plan(node(2))),
    };
    let request = Ulid::from_bytes([6; 16]);
    let mut operation = ApplyRequestOperation::new(local(), node(2), message, request);
    assert!(matches!(
        operation.first(),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let reply = IoResult::Child(ChildOutcome::new(Ok::<_, GroupDeletionError>(
        None::<GroupDeleteProof>,
    )));
    assert_eq!(operation.next(reply), Ok(Next::Done(None)));
}
