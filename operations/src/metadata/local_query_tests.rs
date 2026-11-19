//! Drives the local query through its cache, registry, scope and graph answers without I/O.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use super::*;
use crate::metadata::api::access::tests::realm;
use crate::metadata::visible_scope::call_tests::{answer, record};
use crate::tests::calls::{child, runs_child};

const ASK: &str = "ASK { ?s ?p ?o }";

fn entry(entry: Option<(MetadataQueryResults, usize)>) -> IoResult {
    answer(MetadataEvent::QueryCacheEntry {
        stamp: [4, 5],
        entry,
    })
}

// Walks a query to its cache lookup, with `visible` as the caller's read scope.
fn looked_up(
    graph_iris: Option<Vec<String>>,
    visible: Vec<MetadataRegistryRecord>,
) -> (LocalQueryOperation, Turn) {
    let mut query = LocalQueryOperation::new((realm(), None), graph_iris, ASK.to_string());
    let stamp = graph_call(MetadataEffect::QueryCacheStamp);
    assert_eq!(query.first(), Ok(Next::Call(stamp)));
    assert!(runs_child(query.next(entry(None))));
    let registry = Arc::new(vec![record(1), record(2)]);
    assert!(runs_child(
        query.next(child(Ok::<_, MetadataError>(registry)))
    ));
    let lookup = query.next(child(Ok::<_, MetadataError>(visible)));
    (query, lookup)
}

#[test]
fn query_validation_allows() {
    parse_metadata_query("SELECT ?s WHERE { ?s a schema:Dataset }")
        .expect("common metadata prefixes are available");
    parse_metadata_query("ASK WHERE { ?s ?p ?o }").expect("ASK is supported");

    for query in [
        "CONSTRUCT WHERE { ?s ?p ?o }",
        "INSERT DATA { <urn:s> <urn:p> <urn:o> }",
        "SELECT * WHERE { SERVICE <https://example.com/sparql> { ?s ?p ?o } }",
        "SELECT * WHERE { FILTER EXISTS { SERVICE <https://example.com/sparql> { ?s ?p ?o } } }",
    ] {
        assert!(
            matches!(
                parse_metadata_query(query),
                Err(MetadataError::InvalidInput(_))
            ),
            "query should be rejected: {query}"
        );
    }
}

#[test]
fn query_validation_rejects() {
    assert!(matches!(
        parse_metadata_query(&" ".repeat(QUERY_MAX_BYTES + 1)),
        Err(MetadataError::InvalidInput(_))
    ));
    let mut query =
        LocalQueryOperation::new((realm(), None), None, "CONSTRUCT WHERE { ?s ?p ?o }".into());
    assert!(matches!(query.first(), Err(MetadataError::InvalidInput(_))));
}

#[test]
fn filtered_query_caches() {
    let graph_iri = record(1).graph_iri;
    let (mut query, lookup) = looked_up(Some(vec![graph_iri.clone()]), vec![record(1)]);
    let key = local_key(
        LocalScopeKind::Eager,
        &graphs_digest(&[graph_iri.clone()]),
        ASK,
    )
    .bytes();
    let get = graph_call(MetadataEffect::QueryCacheGet { key });
    assert_eq!(lookup, Ok(Next::Call(get)));
    let read = graph_call(MetadataEffect::QueryScope {
        graph_iris: vec![graph_iri],
        exact: true,
        sparql: ASK.to_string(),
    });
    assert_eq!(query.next(entry(None)), Ok(Next::Call(read)));
    let results = MetadataQueryResults::Boolean(true);
    let store = graph_call(MetadataEffect::QueryCachePut {
        key,
        stamp: [4, 5],
        results: results.clone(),
        nodes_queried: 0,
    });
    let answered = answer(MetadataEvent::QueryResult {
        results: results.clone(),
    });
    assert_eq!(query.next(answered), Ok(Next::Call(store)));
    let stored = answer(MetadataEvent::QueryCacheStored { accepted: true });
    assert_eq!(query.next(stored), Ok(Next::Done(results)));
}

#[test]
fn open_query_reads() {
    let (mut query, lookup) = looked_up(None, vec![record(1), record(2)]);
    assert!(matches!(lookup, Ok(Next::Call(IoCall::Effect(_)))));
    let read = graph_call(MetadataEffect::QueryScope {
        graph_iris: vec![record(1).graph_iri, record(2).graph_iri],
        exact: false,
        sparql: ASK.to_string(),
    });
    assert_eq!(query.next(entry(None)), Ok(Next::Call(read)));
}

#[test]
fn cached_query_answers() {
    let (mut query, _) = looked_up(None, vec![record(1)]);
    let cached = Some((MetadataQueryResults::Boolean(false), 0));
    let done = query.next(entry(cached));
    assert_eq!(done, Ok(Next::Done(MetadataQueryResults::Boolean(false))));
}

#[test]
fn hidden_graph_fails() {
    let filter = vec![record(1).graph_iri, record(2).graph_iri];
    let (_, lookup) = looked_up(Some(filter), vec![record(1)]);
    assert_eq!(lookup, Err(MetadataError::GraphNotFound));
}
