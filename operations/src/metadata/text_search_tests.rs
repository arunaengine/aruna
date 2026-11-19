//! Drives the local search through its registry, reference, scope and graph answers without I/O.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use super::*;
use crate::metadata::api::access::tests::realm;
use crate::metadata::visible_scope::call_tests::{answer, record};
use crate::tests::calls::{child, runs_child};

fn search(filter: (Option<Vec<String>>, Option<GroupId>), query: &str) -> LocalSearchOperation {
    LocalSearchOperation::new((realm(), None), filter, (query.to_string(), 5), None)
}

fn hit(record: &MetadataRegistryRecord, score: f32) -> MetadataSearchHit {
    MetadataSearchHit {
        document_id: record.document_id.to_string(),
        group_id: record.group_id.to_string(),
        document_path: record.document_path.clone(),
        graph_iri: record.graph_iri.clone(),
        subject_iri: record.graph_iri.clone(),
        score,
        title: String::new(),
        snippet: None,
        subject_types: Vec::new(),
    }
}

// Walks a registry search to its graph call, with `visible` as the caller's read scope.
fn scoped(operation: &mut LocalSearchOperation, visible: Vec<MetadataRegistryRecord>) -> Turn {
    assert!(runs_child(operation.first()));
    let registry = Arc::new(vec![record(1), record(2)]);
    assert!(runs_child(
        operation.next(child(Ok::<_, MetadataError>(registry)))
    ));
    operation.next(child(Ok::<_, MetadataError>(visible)))
}

#[test]
fn realm_search_sorts() {
    let mut realm_wide = search((None, None), "data");
    let searched = graph_call(MetadataEffect::SearchScope {
        records: vec![record(1), record(2)],
        exact: false,
        query: "data".to_string(),
        limit: 5,
    });
    let turn = scoped(&mut realm_wide, vec![record(1), record(2)]);
    assert_eq!(turn, Ok(Next::Call(searched)));
    let hits = vec![hit(&record(1), 0.2), hit(&record(2), 0.9)];
    let done = realm_wide.next(answer(MetadataEvent::SearchResult { hits }));
    let ranked = vec![hit(&record(2), 0.9), hit(&record(1), 0.2)];
    assert_eq!(done, Ok(Next::Done(ranked)));
}

#[test]
fn group_search_exact() {
    let mut grouped = search((None, Some(record(1).group_id)), "data");
    let turn = scoped(&mut grouped, vec![record(1)]);
    let Ok(Next::Call(IoCall::Effect(effect))) = turn else {
        panic!("expected the search effect, got {turn:?}");
    };
    let Effect::Metadata(MetadataEffect::SearchScope { exact, .. }) = *effect else {
        panic!("expected a scoped search");
    };
    assert!(exact);
    let hits = vec![hit(&record(1), 0.2), hit(&record(1), 0.9)];
    let done = grouped.next(answer(MetadataEvent::SearchResult { hits: hits.clone() }));
    assert_eq!(done, Ok(Next::Done(hits)));
}

#[test]
fn reference_listing_describes() {
    let reference = Some(("urn:p".to_string(), "urn:o".to_string()));
    let mut listing = LocalSearchOperation::new(
        (realm(), None),
        (None, None),
        (" ".to_string(), 1),
        reference,
    );
    assert!(runs_child(listing.first()));
    let (first, second) = (record(1), record(2));
    let matches = Matches::from([
        (first.document_id, vec!["urn:s:b".to_string()]),
        (second.document_id, vec!["urn:s:a".to_string()]),
    ]);
    let found = (vec![first.clone(), second.clone()], matches);
    assert!(runs_child(
        listing.next(child(Ok::<_, MetadataIriError>(found)))
    ));
    let visible = child(Ok::<_, MetadataError>(vec![first.clone(), second.clone()]));
    let mut targets = vec![
        (first, "urn:s:b".to_string()),
        (second, "urn:s:a".to_string()),
    ];
    targets.sort_by(|(left, left_subject), (right, right_subject)| {
        tie_order(
            (&left.graph_iri, left_subject),
            (&right.graph_iri, right_subject),
        )
    });
    targets.truncate(1);
    let described = graph_call(MetadataEffect::DescribeHits { targets });
    assert_eq!(listing.next(visible), Ok(Next::Call(described)));
}

#[test]
fn empty_scope_answers() {
    let mut nothing = search((None, None), "data");
    assert_eq!(scoped(&mut nothing, Vec::new()), Ok(Next::Done(Vec::new())));
}

#[test]
fn oversized_filter_fails() {
    let graphs = vec![String::new(); REGISTRY_CANDIDATE_LIMIT + 1];
    let mut oversized = search((Some(graphs), None), "data");
    assert!(matches!(oversized.first(), Err(MetadataError::Backend(_))));
}
