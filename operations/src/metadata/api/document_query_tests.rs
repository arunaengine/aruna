//! Drives the document query through its record, config, local and replica answers without I/O.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::calls::ChildOutcome;
use aruna_core::events::{MetadataAnswer, NetEvent, PeerRequestError};
use aruna_core::metadata::MetadataError;
use ulid::Ulid;

fn realm() -> RealmId {
    RealmId::from_bytes([1; 32])
}

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn record() -> MetadataRegistryRecord {
    let document_id = Ulid::from_parts(3, 3);
    MetadataRegistryRecord {
        realm_id: realm(),
        group_id: Ulid::from_parts(2, 2),
        document_id,
        document_path: "datasets/query".to_string(),
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

fn request(mode: Option<ApiQueryMode>, allow_partial: bool) -> DocumentQueryRequest {
    DocumentQueryRequest {
        document_id: record().document_id,
        auth: None,
        bearer_token: None,
        query: "SELECT * WHERE { ?s ?p ?o }".to_string(),
        mode,
        allow_partial,
    }
}

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

fn results() -> MetadataQueryResults {
    MetadataQueryResults::Boolean(true)
}

fn query_answer(result: Result<MetadataQueryResults, MetadataError>) -> IoResult {
    let event = match result {
        Ok(results) => MetadataEvent::QueryResult { results },
        Err(error) => MetadataEvent::Error {
            graph_iri: None,
            error,
        },
    };
    IoResult::Event(Box::new(Event::Metadata(event)))
}

/// A query answered up to its readable check.
fn readable(request: DocumentQueryRequest) -> (DocumentQueryOperation, QueryTurn) {
    let mut query = DocumentQueryOperation::new(realm(), node(1), request);
    assert!(matches!(query.first(), Ok(Next::Call(IoCall::Child(_)))));
    let found = child(Ok::<_, LiveRecordsError>(Some(record())));
    assert!(matches!(
        query.next(found),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let turn = query.next(child(Ok::<_, MetadataApiError>(())));
    (query, turn)
}

#[test]
fn missing_document_not_found() {
    let mut query = DocumentQueryOperation::new(realm(), node(1), request(None, false));
    query.first().unwrap();
    let missing = child(Ok::<Option<MetadataRegistryRecord>, LiveRecordsError>(None));
    assert_eq!(query.next(missing), Err(MetadataApiError::NotFound));
}

#[test]
fn local_mode_queries_here() {
    let (mut query, turn) = readable(request(Some(ApiQueryMode::Local), false));
    assert!(matches!(turn, Ok(Next::Call(IoCall::Child(_)))));
    let turn = query.next(child(Ok::<_, StorageReadError>(true)));
    let effect = MetadataEffect::QueryGraphs {
        auth_context: None,
        graph_iris: Some(vec![record().graph_iri]),
        sparql: request(None, false).query,
    };
    let call = IoCall::Effect(Box::new(Effect::Metadata(effect)));
    assert_eq!(turn, Ok(Next::Call(call)));
    let Ok(Next::Done(done)) = query.next(query_answer(Ok(results()))) else {
        panic!("expected the results");
    };
    assert_eq!(done.results, results());
    assert_eq!(done.fanout_stats.nodes_queried, 1);
}

#[test]
fn unreadable_config_partial() {
    let (mut strict, _) = readable(request(None, false));
    let missing = || {
        child(Err::<RealmConfigDocument, _>(
            GetConfigError::DocumentNotFound,
        ))
    };
    assert_eq!(
        strict.next(missing()),
        Err(MetadataApiError::ServiceUnavailable)
    );

    let (mut partial, _) = readable(request(None, true));
    partial.next(missing()).unwrap();
    partial
        .next(child(Ok::<_, StorageReadError>(true)))
        .unwrap();
    let Ok(Next::Done(done)) = partial.next(query_answer(Ok(results()))) else {
        panic!("expected the partial results");
    };
    assert!(done.fanout_stats.discovery_failed);
    assert_eq!(done.fanout_stats.nodes_failed, 1);
}

#[test]
fn replicas_tried_in_order() {
    let (mut query, _) = readable(request(None, false));
    query.holders = VecDeque::from([node(2), node(1)]);
    let Ok(Next::Call(IoCall::Effect(effect))) = query.next_holder() else {
        panic!("expected the replica request");
    };
    assert!(matches!(*effect, Effect::Net(_)));
    let refused = MetadataAnswer {
        peer: node(2),
        elapsed: Duration::ZERO,
        result: Err(PeerRequestError::TimedOut),
    };
    let refused = IoResult::Event(Box::new(Event::Net(NetEvent::MetadataAnswer(Box::new(
        refused,
    )))));
    // The unavailable replica is skipped; this node answers next.
    assert!(matches!(
        query.next(refused),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    query.next(child(Ok::<_, StorageReadError>(true))).unwrap();
    let Ok(Next::Done(done)) = query.next(query_answer(Ok(results()))) else {
        panic!("expected the local results");
    };
    assert_eq!(done.fanout_stats.nodes_queried, 2);
    assert_eq!(done.fanout_stats.nodes_failed, 0);
}

#[test]
fn refusal_outlasts_misses() {
    let (mut query, _) = readable(request(None, false));
    let forbidden = query.answered(node(2), Err(MetadataReadError::Forbidden));
    assert_eq!(forbidden, Err(MetadataApiError::Forbidden));
}
