//! Drives the routed Profile status through its record, config and holder answers without I/O.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::events::{MetadataAnswer, NetEvent};
use aruna_core::metadata::{
    ProfileValidationCompleteness, ProfileValidationState, ProfileValidationStatus,
};
use std::time::Duration;

fn realm() -> RealmId {
    RealmId::from_bytes([1; 32])
}

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn request() -> GetVisibleRequest {
    GetVisibleRequest {
        document_id: Ulid::from_parts(3, 3),
        auth: None,
    }
}

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

fn record() -> MetadataRegistryRecord {
    let document_id = Ulid::from_parts(3, 3);
    MetadataRegistryRecord {
        realm_id: realm(),
        group_id: Ulid::from_parts(2, 2),
        document_id,
        document_path: "datasets/status".to_string(),
        graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
        public: true,
        permission_path: String::new(),
        placement: aruna_core::structs::placement::record::PlacementRef::NIL,
        holder_node_ids: Vec::new(),
        created_at_ms: 1,
        updated_at_ms: 1,
        establishing_event_id: Ulid::from_parts(4, 4),
        last_event_id: Ulid::from_parts(5, 5),
    }
}

fn status(revision: Ulid) -> ProfileValidationStatus {
    ProfileValidationStatus {
        document_id: record().document_id,
        dataset_revision: revision,
        state: ProfileValidationState::Valid,
        profile_id: None,
        profile_iri: None,
        profile_revision: None,
        evaluator: "builtin".to_string(),
        validated_at_ms: Some(1),
        findings: Vec::new(),
        completeness: ProfileValidationCompleteness::Complete,
        stale_reason: None,
        dataset_digest: None,
    }
}

fn forwarded(
    peer: NodeId,
    result: Result<ProfileValidationStatus, MetadataReadError>,
) -> GroupResult {
    let message = MetadataTransportMessage::ForwardedValidationStatus {
        result: result.map(Box::new),
    };
    let answer = MetadataAnswer {
        peer,
        elapsed: Duration::ZERO,
        result: Ok(encode_message(&message).unwrap()),
    };
    GroupResult::Done(Box::new(Event::Net(NetEvent::MetadataAnswer(Box::new(
        answer,
    )))))
}

#[test]
fn document_not_found() {
    let mut status = RoutedStatusOperation::new((Some(node(1)), realm()), request(), None, false);
    assert!(matches!(status.first(), Ok(Next::Call(IoCall::Child(_)))));
    let missing = child(Ok::<Option<MetadataRegistryRecord>, LiveRecordsError>(None));
    assert_eq!(status.next(missing), Err(MetadataApiError::NotFound));
}

#[test]
fn offline_answers_here() {
    let mut routed = RoutedStatusOperation::new((None, realm()), request(), None, false);
    routed.first().unwrap();
    let found = child(Ok::<_, LiveRecordsError>(Some(record())));
    assert!(matches!(
        routed.next(found),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let local = child(Ok::<_, MetadataApiError>(status(record().last_event_id)));
    assert_eq!(
        routed.next(local),
        Ok(Next::Done(status(record().last_event_id)))
    );
}

#[test]
fn current_revision_wins() {
    let mut routed = RoutedStatusOperation::new((Some(node(1)), realm()), request(), None, false);
    routed.expected_revision = record().last_event_id;
    routed.holders = vec![node(2), node(3)];
    let older = status(Ulid::from_parts(1, 1));
    let current = status(record().last_event_id);
    let results = vec![
        forwarded(node(2), Ok(older)),
        forwarded(node(3), Ok(current.clone())),
    ];
    assert_eq!(routed.collect(results), Ok(Next::Done(current)));
}

#[test]
fn split_answer_unavailable() {
    let mut routed = RoutedStatusOperation::new((Some(node(1)), realm()), request(), None, false);
    routed.holders = vec![node(2), node(3)];
    let results = vec![
        forwarded(node(2), Ok(status(Ulid::from_parts(1, 1)))),
        forwarded(node(3), Err(MetadataReadError::NotFound)),
    ];
    assert_eq!(
        routed.collect(results),
        Err(MetadataApiError::ServiceUnavailable)
    );
}

#[test]
fn local_checks_reader() {
    let node = (realm(), Some(node(1)));
    let mut local = LocalStatusOperation::new(node, request(), Some(record()), true);
    assert!(matches!(local.first(), Ok(Next::Call(IoCall::Child(_)))));
    let denied = child(Err::<(), _>(MetadataApiError::NotFound));
    assert_eq!(local.next(denied), Err(MetadataApiError::NotFound));
}

#[test]
fn failed_status_unavailable() {
    let node = (realm(), Some(node(1)));
    let mut local = LocalStatusOperation::new(node, request(), Some(record()), false);
    local.first().unwrap();
    local.next(child(Ok::<_, MetadataApiError>(()))).unwrap();
    let failed = child(Err::<ProfileValidationStatus, _>(
        MetadataError::HandleMissing,
    ));
    assert_eq!(
        local.next(failed),
        Err(MetadataApiError::ServiceUnavailable)
    );
}
