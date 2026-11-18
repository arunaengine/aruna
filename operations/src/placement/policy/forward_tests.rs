//! Tests routed policy publications as calls and results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::tests::calls::{child, node, runs_child};
use crate::tests::policy::signed_document;
use aruna_core::UserId;
use aruna_core::structs::placement::policy::{PlacementPolicy, PlacementSelector, VerifiedPolicy};
use ulid::Ulid;

fn realm() -> RealmId {
    RealmId::from_bytes([8; 32])
}

fn policy() -> PlacementPolicy {
    let selector = PlacementSelector {
        node_id: None,
        location: Some("eu".to_string()),
        labels: Vec::new(),
        executor_kind: None,
    };
    let id = Ulid::from_bytes([3; 16]);
    PlacementPolicy::new(id, "residency".to_string(), vec![selector]).unwrap()
}

fn document() -> PlacementPolicyDocument {
    let verified = VerifiedPolicy::verify(policy()).unwrap();
    signed_document(realm(), &verified, 1)
}

fn auth() -> AuthContext {
    AuthContext {
        user_id: UserId::local(Ulid::from_bytes([4; 16]), realm()),
        realm_id: realm(),
        path_restrictions: None,
        session: None,
    }
}

fn config() -> CreatePolicyConfig {
    CreatePolicyConfig {
        actor: Actor {
            node_id: node(1),
            user_id: auth().user_id,
            realm_id: realm(),
        },
        auth_context: auth(),
        policy: policy(),
        created_at_ms: 5,
    }
}

fn created() -> IoResult {
    child(Ok::<_, CreatePolicyError>(document()))
}

fn create_failed(error: CreatePolicyError) -> IoResult {
    child(Err::<PlacementPolicyDocument, _>(error))
}

fn routing() -> CreateRoutedOperation {
    let mut operation = CreateRoutedOperation::new(config(), None, Some(node(1)));
    assert!(runs_child(operation.first()));
    operation
}

fn forwarding() -> CreateRoutedOperation {
    let mut operation = routing();
    let missed = CreatePolicyError::NotHolder {
        holders: vec![node(2)],
    };
    assert!(runs_child(operation.next(create_failed(missed))));
    operation
}

#[test]
fn holder_creates_here() {
    assert_eq!(routing().next(created()), Ok(Next::Done(document())));
}

#[test]
fn local_refusal_kept() {
    assert_eq!(
        routing().next(create_failed(CreatePolicyError::Unauthorized)),
        Err(PolicyForwardError::Create(CreatePolicyError::Unauthorized))
    );
}

#[test]
fn holder_document_returned() {
    let answer = MetadataTransportMessage::PlacementPolicyCreated {
        document: Box::new(document()),
    };
    assert_eq!(
        forwarding().next(child(Ok::<_, HolderWriteError>(answer))),
        Ok(Next::Done(document()))
    );
}

#[test]
fn delivery_failure_kept() {
    let failed = Err::<MetadataTransportMessage, _>(HolderWriteError::Forbidden);
    assert_eq!(
        forwarding().next(child(failed)),
        Err(PolicyForwardError::Forward(HolderWriteError::Forbidden))
    );
}

#[test]
fn odd_answer_undeliverable() {
    let answer = MetadataTransportMessage::WriteNotFound;
    assert!(matches!(
        forwarding().next(child(Ok::<_, HolderWriteError>(answer))),
        Err(PolicyForwardError::Forward(
            HolderWriteError::Undeliverable(_)
        ))
    ));
}

fn publication() -> MetadataTransportMessage {
    MetadataTransportMessage::ForwardCreatePolicy {
        auth_token: None,
        policy: Box::new(policy()),
        created_at_ms: 5,
    }
}

fn applying() -> ApplyPolicyOperation {
    let local = Some((node(1), realm()));
    let mut operation = ApplyPolicyOperation::new(local, node(2), publication());
    assert!(runs_child(operation.first()));
    operation
}

fn accepted() -> ApplyPolicyOperation {
    let mut operation = applying();
    assert!(runs_child(
        operation.next(child(Ok::<_, ForwardAuthError>(auth())))
    ));
    operation
}

#[test]
fn apply_needs_network() {
    let mut operation = ApplyPolicyOperation::new(None, node(2), publication());
    assert_eq!(
        operation.first(),
        Ok(Next::Done(
            MetadataTransportMessage::ForwardedWriteUnavailable
        ))
    );
}

#[test]
fn refused_caller_answered() {
    let refused = Err::<AuthContext, _>(ForwardAuthError::Unauthorized);
    assert_eq!(
        applying().next(child(refused)),
        Ok(Next::Done(MetadataTransportMessage::ForwardedWriteDenied {
            error: WriteAuthError::Unauthorized,
        }))
    );
}

#[test]
fn foreign_caller_denied() {
    let foreign = AuthContext {
        realm_id: RealmId::from_bytes([9; 32]),
        ..auth()
    };
    assert_eq!(
        applying().next(child(Ok::<_, ForwardAuthError>(foreign))),
        Ok(Next::Done(denied()))
    );
}

#[test]
fn other_message_rejected() {
    let local = Some((node(1), realm()));
    let message = MetadataTransportMessage::WriteNotFound;
    let mut operation = ApplyPolicyOperation::new(local, node(2), message);
    operation.first().unwrap();
    assert!(matches!(
        operation.next(child(Ok::<_, ForwardAuthError>(auth()))),
        Ok(Next::Done(MetadataTransportMessage::Reject(_)))
    ));
}

#[test]
fn created_document_answered() {
    assert_eq!(
        accepted().next(created()),
        Ok(Next::Done(
            MetadataTransportMessage::PlacementPolicyCreated {
                document: Box::new(document()),
            }
        ))
    );
}

#[test]
fn unauthorized_create_denied() {
    assert_eq!(
        accepted().next(create_failed(CreatePolicyError::Unauthorized)),
        Ok(Next::Done(denied()))
    );
}

#[test]
fn moved_holder_unavailable() {
    let missed = CreatePolicyError::NotHolder {
        holders: Vec::new(),
    };
    assert_eq!(
        accepted().next(create_failed(missed)),
        Ok(Next::Done(
            MetadataTransportMessage::ForwardedWriteUnavailable
        ))
    );
}
