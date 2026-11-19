//! Drives the routed read through its config, holder and fallback answers without I/O.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::events::{MetadataAnswer, NetEvent};
use std::time::Duration;
use ulid::Ulid;

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

fn forwarded(
    peer: NodeId,
    result: Result<Box<MetadataRegistryRecord>, MetadataReadError>,
) -> GroupResult {
    let message = MetadataTransportMessage::ForwardedRead { result };
    let answer = MetadataAnswer {
        peer,
        elapsed: Duration::ZERO,
        result: Ok(encode_message(&message).unwrap()),
    };
    GroupResult::Done(Box::new(Event::Net(NetEvent::MetadataAnswer(Box::new(
        answer,
    )))))
}

/// A read past its config whose holders are the given peers.
fn routed(holders: Vec<NodeId>) -> RoutedReadOperation {
    let mut read = RoutedReadOperation::new(Some(node(1)), realm(), request(), None);
    read.config = Some(RealmConfigDocument::default_for_realm(realm(), Vec::new()));
    read.placement = Some(PlacementRef::NIL);
    read.holders = holders;
    read
}

#[test]
fn offline_reads_here() {
    let mut read = RoutedReadOperation::new(None, realm(), request(), None);
    assert!(matches!(read.first(), Ok(Next::Call(IoCall::Child(_)))));
    let missing = child(Err::<MetadataRegistryRecord, _>(MetadataApiError::NotFound));
    assert_eq!(read.next(missing), Err(MetadataApiError::NotFound));
}

#[test]
fn unreadable_config_unavailable() {
    let mut read = RoutedReadOperation::new(Some(node(1)), realm(), request(), None);
    read.first().unwrap();
    let missing = child(Err::<RealmConfigDocument, _>(
        GetConfigError::DocumentNotFound,
    ));
    assert_eq!(
        read.next(missing),
        Err(MetadataApiError::ServiceUnavailable)
    );
}

#[test]
fn every_holder_missing() {
    let mut read = routed(vec![node(2), node(3)]);
    let results = vec![
        forwarded(node(2), Err(MetadataReadError::NotFound)),
        forwarded(node(3), Err(MetadataReadError::NotFound)),
    ];
    assert_eq!(read.collect(results), Err(MetadataApiError::NotFound));
}

#[test]
fn refusal_wins() {
    let mut read = routed(vec![node(2), node(3)]);
    let results = vec![
        forwarded(node(2), Err(MetadataReadError::NotFound)),
        forwarded(node(3), Err(MetadataReadError::Forbidden)),
    ];
    assert_eq!(read.collect(results), Err(MetadataApiError::Forbidden));
}

#[test]
fn late_holder_unavailable() {
    let mut read = routed(vec![node(2), node(3)]);
    let results = vec![
        forwarded(node(2), Err(MetadataReadError::NotFound)),
        GroupResult::Unfinished,
    ];
    assert_eq!(
        read.collect(results),
        Err(MetadataApiError::ServiceUnavailable)
    );
}

#[test]
fn local_holder_mapped() {
    let read = routed(vec![node(1)]);
    let denied = Err::<MetadataRegistryRecord, _>(MetadataApiError::Forbidden);
    let member = GroupResult::Done(Box::new(Event::Child(ChildOutcome::new(denied))));
    assert_eq!(
        read.holder_record(node(1), member),
        Err(MetadataReadError::Forbidden)
    );
}
