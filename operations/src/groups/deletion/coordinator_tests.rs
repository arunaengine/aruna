//! Tests the deletion coordinator's decisions as calls and results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::tests::calls::{caller_of, owned_group};
use aruna_core::admin_documents::{
    AdminDocumentClock, AdminDocumentOperation, AdminDocumentTarget,
};
use aruna_core::calls::ChildOutcome;
use aruna_core::structs::identity::realm::RealmNodeKind;
use ed25519_dalek::SigningKey;

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

fn answer<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
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
    assert_eq!(
        operation.stage,
        DeleteStage::CancelBlocked(low, conflict.clone())
    );
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

fn key_of(node_id: NodeId) -> iroh::SecretKey {
    let seed = if node_id == node(1) { 1 } else { 2 };
    iroh::SecretKey::from_bytes(&[seed; 32])
}

fn proof_of(plan: &GroupDeletePlan, node_id: NodeId) -> GroupDeleteProof {
    GroupDeleteProof {
        node_id,
        signature: key_of(node_id).sign(&plan.signing_bytes().unwrap()),
    }
}

fn replies(replies: NodeReplies) -> IoResult {
    answer(Ok::<_, GroupDeletionError>(replies))
}

fn failure(error: GroupDeletionError) -> IoResult {
    answer(Err::<(), _>(error))
}

fn conflict() -> GroupDeletionError {
    GroupDeletionError::Conflict("busy".to_string())
}

/// Runs a coordinator up to the answers of the prepare requests.
fn prepared() -> DeleteGroupOperation {
    let (low, _) = ordered();
    let (mut operation, turn) = authorized(low);
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    assert!(matches!(
        operation.next(ok()),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(operation.stage, DeleteStage::Requests);
    operation
}

/// Runs a coordinator past valid confirmations of every node.
fn confirmed() -> DeleteGroupOperation {
    let (low, high) = ordered();
    let mut operation = prepared();
    let plan = operation.plan.clone().unwrap();
    let confirmations = vec![
        (low, Ok(Some(proof_of(&plan, low)))),
        (high, Ok(Some(proof_of(&plan, high)))),
    ];
    assert!(matches!(
        operation.next(replies(confirmations)),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(operation.stage, DeleteStage::Recheck);
    operation
}

fn committing() -> DeleteGroupOperation {
    let mut operation = confirmed();
    assert!(matches!(
        operation.next(ok()),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(operation.stage, DeleteStage::Commit);
    operation
}

fn deletion_plan() -> GroupDeletePlan {
    let (low, high) = ordered();
    GroupDeletePlan {
        request_id: request(),
        group_id: group_id(),
        realm_id: realm(),
        owner: auth().user_id,
        requested_by: auth().user_id,
        coordinator: low,
        nodes: BTreeSet::from([low, high]),
    }
}

fn decision() -> AdminDocumentEvent {
    let plan = deletion_plan();
    AdminDocumentEvent {
        event_id: Ulid::from_bytes([7; 16]),
        target: AdminDocumentTarget::RealmConfig { realm_id: realm() },
        origin_node_id: plan.coordinator,
        origin_seq: 1,
        observed: AdminDocumentClock::default(),
        actor: Actor {
            node_id: plan.coordinator,
            user_id: auth().user_id,
            realm_id: realm(),
        },
        op: AdminDocumentOperation::GroupDeleted {
            certificate: Box::new(GroupDeleteCertificate {
                plan,
                proofs: Vec::new(),
            }),
        },
    }
}

fn expect_cancel(operation: &mut DeleteGroupOperation, error: GroupDeletionError) {
    assert_eq!(operation.stage, DeleteStage::Cancel(error.clone()));
    assert_eq!(operation.next(ok()), Err(error));
}

#[test]
fn unconfirmed_node_cancels() {
    let (low, _) = ordered();
    let mut operation = prepared();
    assert!(matches!(
        operation.next(replies(vec![(low, Ok(None))])),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let error = GroupDeletionError::Unavailable(format!("node {low} did not confirm emptiness"));
    expect_cancel(&mut operation, error);
}

#[test]
fn node_refusal_worded() {
    let (low, _) = ordered();
    let mut operation = prepared();
    operation
        .next(replies(vec![(low, Err(conflict()))]))
        .unwrap();
    assert_eq!(operation.stage, DeleteStage::Blocked);
    let worded = GroupDeletionError::Conflict("worded".to_string());
    let answer = answer(Ok::<_, GroupDeletionError>(worded.clone()));
    assert!(matches!(
        operation.next(answer),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    expect_cancel(&mut operation, worded);
}

#[test]
fn lost_permission_cancels() {
    let mut operation = confirmed();
    operation
        .next(failure(GroupDeletionError::Unauthorized))
        .unwrap();
    expect_cancel(&mut operation, GroupDeletionError::Unauthorized);
}

#[test]
fn forged_proof_cancels() {
    let (low, high) = ordered();
    let mut operation = prepared();
    let other = deletion_plan();
    let mut forged = other.clone();
    forged.request_id = Ulid::from_bytes([8; 16]);
    let plan = operation.plan.clone().unwrap();
    let confirmations = vec![
        (low, Ok(Some(proof_of(&plan, low)))),
        (high, Ok(Some(proof_of(&forged, high)))),
    ];
    operation.next(replies(confirmations)).unwrap();
    operation.next(ok()).unwrap();
    let error =
        GroupDeletionError::Unavailable("a node supplied an invalid confirmation".to_string());
    expect_cancel(&mut operation, error);
}

#[test]
fn commit_publishes_decision() {
    let mut operation = committing();
    let committed = answer(Ok::<_, GroupDeletionError>(decision()));
    assert!(matches!(
        operation.next(committed),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(operation.stage, DeleteStage::Publish);
    assert_eq!(operation.next(ok()), Ok(Next::Done(())));
}

#[test]
fn refused_commit_cancels() {
    let mut operation = committing();
    let refused = answer(Err::<AdminDocumentEvent, _>(conflict()));
    operation.next(refused).unwrap();
    expect_cancel(&mut operation, conflict());
}

#[test]
fn uncertain_commit_kept() {
    // The commit may have landed, so the plan is not cancelled.
    let error = GroupDeletionError::Unavailable("down".to_string());
    let failed = answer(Err::<AdminDocumentEvent, _>(error.clone()));
    assert_eq!(committing().next(failed), Err(error));
}

/// Runs a coordinator past the caller check with these stored rows.
fn routed(loaded: LoadedGroup) -> (DeleteGroupOperation, DeleteTurn) {
    let (low, _) = ordered();
    let mut operation =
        DeleteGroupOperation::new(Some((low, realm())), auth(), None, group_id(), request());
    operation.stamp(1_000);
    operation.first().unwrap();
    operation.next(config_answer()).unwrap();
    operation
        .next(answer(Ok::<_, GroupDeletionError>(loaded)))
        .unwrap();
    let turn = operation.next(ok());
    (operation, turn)
}

fn stored_record(phase: GroupDeletePhase, event: bool) -> GroupDeleteRecord {
    GroupDeleteRecord {
        plan: deletion_plan(),
        phase,
        deleted_by: None,
        event: event.then(|| Box::new(decision())),
    }
}

#[test]
fn deleted_group_republished() {
    let record = stored_record(GroupDeletePhase::Deleted, true);
    let (operation, turn) = routed((None, Some(record)));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(operation.stage, DeleteStage::Publish);
}

#[test]
fn lost_decision_unavailable() {
    let record = stored_record(GroupDeletePhase::Deleted, false);
    let (_, turn) = routed((None, Some(record)));
    assert!(matches!(turn, Err(GroupDeletionError::Unavailable(_))));
}

#[test]
fn aborting_plan_finished() {
    let record = stored_record(GroupDeletePhase::Aborting, false);
    let (mut operation, turn) = routed((Some(group()), Some(record)));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    let retry = GroupDeletionError::Conflict("previous attempt cancelled; retry deletion".into());
    expect_cancel(&mut operation, retry);
}

#[test]
fn running_plan_resumed() {
    let record = stored_record(GroupDeletePhase::Preparing, false);
    let (operation, turn) = routed((Some(group()), Some(record)));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(operation.stage, DeleteStage::Prepare);
    assert_eq!(operation.plan, Some(deletion_plan()));
}

#[test]
fn missing_group_refused() {
    let (_, turn) = routed((None, None));
    assert_eq!(turn, Err(GroupDeletionError::NotFound));
}

fn publishing(plan: GroupDeletePlan) -> PublishDecisionOperation {
    let (low, _) = ordered();
    let mut operation = PublishDecisionOperation::new(Some((low, realm())), plan, decision());
    assert_eq!(operation.first(), Ok(Next::Call(IoCall::Sync)));
    assert!(matches!(
        operation.next(IoResult::Done),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    operation
}

fn no_proof() -> IoResult {
    answer(Ok::<_, GroupDeletionError>(None::<GroupDeleteProof>))
}

#[test]
fn decision_reaches_members() {
    let mut operation = publishing(deletion_plan());
    assert!(matches!(
        operation.next(config_answer()),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(operation.next(no_proof()), Ok(Next::Done(())));
}

#[test]
fn departed_member_skipped() {
    let mut plan = deletion_plan();
    plan.nodes.insert(node(9));
    plan.nodes.remove(&ordered().1);
    assert_eq!(publishing(plan).next(config_answer()), Ok(Next::Done(())));
}

#[test]
fn refused_publish_stops() {
    let mut operation = publishing(deletion_plan());
    operation.next(config_answer()).unwrap();
    let refused = answer(Err::<Option<GroupDeleteProof>, _>(conflict()));
    assert_eq!(operation.next(refused), Err(conflict()));
}

fn ticket_key() -> SigningKey {
    SigningKey::from_bytes(&[3; 32])
}

fn ticket_realm() -> RealmId {
    RealmId::from_bytes(ticket_key().verifying_key().to_bytes())
}

fn ticket(records: Vec<GroupDeleteRecord>) -> OnboardingTicket {
    let realm_id = ticket_realm();
    OnboardingTicket::issue_with_deletions(
        &ticket_key(),
        &realm_id,
        node(1),
        100,
        vec![DocumentTarget::RealmAuthorization { realm_id }],
        records,
    )
    .unwrap()
}

fn ticket_record(realm_id: RealmId) -> GroupDeleteRecord {
    let mut record = stored_record(GroupDeletePhase::Deleted, false);
    record.plan.realm_id = realm_id;
    record
}

#[test]
fn ticket_records_installed() {
    let ticket = ticket(vec![ticket_record(ticket_realm())]);
    let mut operation = InstallOnboardingOperation::new(&ticket, ticket_realm(), node(1), 10);
    assert!(matches!(
        operation.first(),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(operation.next(ok()), Ok(Next::Call(IoCall::Sync)));
    assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(())));
}

#[test]
fn foreign_ticket_refused() {
    let mut operation = InstallOnboardingOperation::new(&ticket(Vec::new()), realm(), node(1), 10);
    assert!(matches!(
        operation.first(),
        Err(GroupDeletionError::Invalid(_))
    ));
}

#[test]
fn expired_ticket_refused() {
    let ticket = ticket(Vec::new());
    let mut operation = InstallOnboardingOperation::new(&ticket, ticket_realm(), node(1), 401);
    assert!(matches!(
        operation.first(),
        Err(GroupDeletionError::Invalid(_))
    ));
}

#[test]
fn foreign_record_refused() {
    let ticket = ticket(vec![ticket_record(realm())]);
    let mut operation = InstallOnboardingOperation::new(&ticket, ticket_realm(), node(1), 10);
    assert!(matches!(
        operation.first(),
        Err(GroupDeletionError::Invalid(_))
    ));
}

fn auth() -> AuthContext {
    caller_of(realm())
}

fn group() -> Group {
    owned_group(group_id(), &auth())
}
