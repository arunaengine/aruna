//! Tests the deletion coordinator's decisions as calls and results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::UserId;
use aruna_core::calls::ChildOutcome;
use aruna_core::structs::identity::realm::RealmNodeKind;

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn realm() -> RealmId {
    RealmId::from_bytes([1; 32])
}

fn group_id() -> GroupId {
    Ulid::from_bytes([3; 16])
}

fn request() -> Ulid {
    Ulid::from_bytes([6; 16])
}

fn auth() -> AuthContext {
    AuthContext {
        user_id: UserId::local(Ulid::from_bytes([4; 16]), realm()),
        realm_id: realm(),
        path_restrictions: None,
        session: None,
    }
}

/// The lowest of the two server nodes coordinates, so tests pick their side by id order.
fn ordered() -> (NodeId, NodeId) {
    let (one, two) = (node(1), node(2));
    (one.min(two), one.max(two))
}

fn config() -> RealmConfigDocument {
    let (low, high) = ordered();
    let mut config = RealmConfigDocument::new(realm(), Vec::new(), 2);
    config.ensure_node(low, RealmNodeKind::Server);
    config.ensure_node(high, RealmNodeKind::Server);
    config
}

fn group() -> Group {
    Group::new(group_id(), realm(), "group".to_string(), auth().user_id)
}

fn answer<T: Send + 'static, E: Send + 'static>(result: Result<T, E>) -> IoResult {
    IoResult::Child(ChildOutcome::new(result))
}

fn ok() -> IoResult {
    answer(Ok::<(), GroupDeletionError>(()))
}

fn config_answer() -> IoResult {
    answer(Ok::<_, GetConfigError>(config()))
}

/// Runs up to the first decision after the caller was authorized.
fn authorized(local: NodeId) -> (DeleteGroupOperation, DeleteTurn) {
    let local = Some((local, realm()));
    let mut operation = DeleteGroupOperation::new(local, auth(), None, group_id(), request());
    operation.stamp(1_000);
    operation.first().unwrap();
    operation.next(config_answer()).unwrap();
    let loaded: LoadedGroup = (Some(group()), None);
    operation
        .next(answer(Ok::<_, GroupDeletionError>(loaded)))
        .unwrap();
    let turn = operation.next(ok());
    (operation, turn)
}

#[test]
fn needs_network() {
    let mut operation = DeleteGroupOperation::new(None, auth(), None, group_id(), request());
    assert_eq!(operation.first(), Err(network_missing()));
}

#[test]
fn foreign_realm_refused() {
    let local = Some((node(1), RealmId::from_bytes([9; 32])));
    let mut operation = DeleteGroupOperation::new(local, auth(), None, group_id(), request());
    assert_eq!(operation.first(), Err(GroupDeletionError::Unauthorized));
}

#[test]
fn other_node_coordinates() {
    let (low, high) = ordered();
    let (mut operation, turn) = authorized(high);
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(operation.stage, DeleteStage::Forward);
    let _ = low;
    let reply = answer(Ok::<_, GroupDeletionError>(None::<GroupDeleteProof>));
    assert_eq!(operation.next(reply), Ok(Next::Done(())));
}

#[test]
fn coordinator_prepares_plan() {
    let (low, high) = ordered();
    let (operation, turn) = authorized(low);
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(operation.stage, DeleteStage::Prepare);
    let plan = operation.plan.unwrap();
    assert_eq!(plan.coordinator, low);
    assert_eq!(plan.request_id, request());
    assert_eq!(plan.nodes, BTreeSet::from([low, high]));
}

#[test]
fn failed_prepare_cancels() {
    let (low, _) = ordered();
    let (mut operation, _) = authorized(low);
    let conflict = GroupDeletionError::Conflict("busy".to_string());
    let failed = answer(Err::<(), _>(conflict.clone()));
    assert!(matches!(
        operation.next(failed),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(operation.stage, DeleteStage::CancelBlocked(low, conflict.clone()));
    assert!(matches!(
        operation.next(ok()),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let worded = answer(Ok::<_, GroupDeletionError>(conflict.clone()));
    assert_eq!(operation.next(worded), Err(conflict));
}

fn cancelling() -> CancelPlanOperation {
    let (low, high) = ordered();
    let plan = GroupDeletePlan {
        request_id: Ulid::from_bytes([5; 16]),
        group_id: group_id(),
        realm_id: realm(),
        owner: auth().user_id,
        requested_by: auth().user_id,
        coordinator: low,
        nodes: BTreeSet::from([low, high]),
    };
    let mut operation = CancelPlanOperation::new(Some((low, realm())), plan);
    operation.first().unwrap();
    operation.next(ok()).unwrap();
    assert!(matches!(
        operation.next(config_answer()),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    operation
}

#[test]
fn cancel_keeps_failure() {
    let mut operation = cancelling();
    let down = GroupDeletionError::Unavailable("down".to_string());
    let failed = answer(Err::<Option<GroupDeleteProof>, _>(down.clone()));
    assert_eq!(operation.next(failed), Err(down));
}

#[test]
fn cancel_records_outcome() {
    let mut operation = cancelling();
    let told = answer(Ok::<_, GroupDeletionError>(None::<GroupDeleteProof>));
    assert!(matches!(
        operation.next(told),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(operation.stage, CancelStage::Cancelled);
    assert_eq!(operation.next(ok()), Ok(Next::Done(())));
}

#[test]
fn sync_decision_waits() {
    let mut operation = SyncDecisionOperation;
    assert_eq!(operation.first(), Ok(Next::Call(IoCall::Sync)));
    assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(())));
}
