//! Drives the path operations through their children and holder answers without I/O.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::metadata::api::read_access::tests::{access_for, realm};
use ulid::Ulid;

fn group() -> GroupId {
    Ulid::from_parts(2, 2)
}

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn record(document: u64, path: &str) -> MetadataRegistryRecord {
    let document_id = Ulid::from_parts(document, 3);
    MetadataRegistryRecord {
        realm_id: realm(),
        group_id: group(),
        document_id,
        document_path: path.to_string(),
        graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
        public: true,
        permission_path: String::new(),
        placement: PlacementRef::NIL,
        holder_node_ids: Vec::new(),
        created_at_ms: 1,
        updated_at_ms: 1,
        establishing_event_id: Ulid::from_parts(4, u128::from(document)),
        last_event_id: Ulid::from_parts(5, u128::from(document)),
    }
}

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

fn request(path: &str, auth: Option<AuthContext>) -> MetadataLookupRequest {
    MetadataLookupRequest {
        group_id: group(),
        document_path: path.to_string(),
        auth,
    }
}

#[test]
fn local_keeps_path_claims() {
    let mut local = LocalPathOperation::new(realm(), (group(), "datasets/a"), None, None);
    assert!(matches!(local.first(), Ok(Next::Call(IoCall::Child(_)))));
    let claims = vec![
        record(2, "datasets/a"),
        record(1, "datasets/a"),
        record(3, "datasets/b"),
    ];
    let turn = local.next(child(Ok::<_, MetadataApiError>(claims)));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    let kept = [record(2, "datasets/a"), record(1, "datasets/a")];
    let access = access_for(&kept, None);
    let Ok(Next::Done(candidates)) = local.next(child(Ok::<_, MetadataApiError>(access))) else {
        panic!("expected the candidates");
    };
    let ids = candidates
        .iter()
        .map(|candidate| candidate.claim.document_id.as_ulid());
    assert_eq!(
        ids.collect::<Vec<_>>(),
        vec![kept[1].document_id, kept[0].document_id]
    );
    assert!(
        candidates
            .iter()
            .all(|candidate| candidate.record.is_some())
    );
}

#[test]
fn local_needs_config() {
    let mut local = LocalPathOperation::new(realm(), (group(), "datasets/a"), None, Some(node(1)));
    local.first().unwrap();
    let claims = vec![record(1, "datasets/a")];
    let turn = local.next(child(Ok::<_, MetadataApiError>(claims)));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    let missing = child(Err::<RealmConfigDocument, _>(
        GetConfigError::DocumentNotFound,
    ));
    assert_eq!(
        local.next(missing),
        Err(MetadataApiError::ServiceUnavailable)
    );
}

#[test]
fn empty_path_rejected() {
    let mut lookup = LookupPathOperation::new(realm(), request("/", None), None, Some(node(1)));
    assert_eq!(lookup.first(), Err(MetadataApiError::BadRequest));
}

#[test]
fn local_lookup_resolves() {
    let mut lookup = LookupPathOperation::local(realm(), request("datasets/a", None), None);
    assert!(matches!(lookup.first(), Ok(Next::Call(IoCall::Child(_)))));
    let none = child(Ok::<Candidates, MetadataApiError>(Vec::new()));
    assert_eq!(lookup.next(none), Err(MetadataApiError::NotFound));
}

#[test]
fn foreign_origin_forwards() {
    let auth = AuthContext {
        user_id: aruna_core::UserId::local(Ulid::from_parts(8, 8), realm()),
        realm_id: realm(),
        path_restrictions: None,
        session: None,
    };
    let mut lookup = LookupPathOperation::new(
        realm(),
        request("datasets/a", Some(auth)),
        None,
        Some(node(1)),
    );
    lookup.stamp(1_000);
    assert!(matches!(lookup.first(), Ok(Next::Call(IoCall::Child(_)))));
    // A node outside the sync set forwards, which needs the caller's own token.
    let config = RealmConfigDocument::default_for_realm(realm(), Vec::new());
    let turn = lookup.next(child(Ok::<_, GetConfigError>(config)));
    assert_eq!(turn, Err(MetadataApiError::Unauthorized));
}

#[test]
fn deadline_shrinks() {
    let mut lookup = LookupPathOperation::new(realm(), request("datasets/a", None), None, None);
    lookup.stamp(1_000);
    lookup.stamp(3_000);
    let spent = Duration::from_secs(2);
    assert_eq!(lookup.remaining(), DISTRIBUTED_QUERY_DEADLINE - spent);
}

#[test]
fn holder_answers_merge() {
    let selection = PathHolderSelection {
        node_id: node(2),
        shards: vec![0],
    };
    let denied = MetadataTransportMessage::ForwardedPathLookup {
        result: Err(MetadataReadError::Forbidden),
    };
    let answer = aruna_core::events::MetadataAnswer {
        peer: node(2),
        elapsed: Duration::ZERO,
        result: Ok(encode_message(&denied).unwrap()),
    };
    let member = GroupResult::Done(Box::new(Event::Net(
        aruna_core::events::NetEvent::MetadataAnswer(Box::new(answer)),
    )));
    let read = LookupPathOperation::holder_answer(&selection, member);
    assert_eq!(read, Err(MetadataApiError::Forbidden));
    let late = LookupPathOperation::holder_answer(&selection, GroupResult::Unfinished);
    assert_eq!(late, Err(MetadataApiError::ServiceUnavailable));
}
