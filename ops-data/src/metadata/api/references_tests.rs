//! Drives the references read through its backlink page and document checks without I/O.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

fn realm() -> RealmId {
    RealmId::from_bytes([1; 32])
}

fn request(iri: &str) -> MetadataReferencesRequest {
    MetadataReferencesRequest {
        auth: None,
        iri: iri.to_string(),
        predicate: None,
        limit: None,
        resolve: false,
        after: None,
    }
}

fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + std::fmt::Debug + PartialEq + 'static,
    E: Send + std::fmt::Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

#[test]
fn malformed_iri_refused() {
    let mut references = ReferencesOperation::new(realm(), request("not an iri"));
    assert_eq!(references.first(), Err(MetadataApiError::BadRequest));
}

#[test]
fn empty_page_summarizes() {
    let iri = MetadataRegistryRecord::graph_iri_for(Ulid::from_parts(3, 3));
    let mut references = ReferencesOperation::new(realm(), request(&iri));
    assert!(matches!(
        references.first(),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let empty = child(Ok::<_, BacklinkError>((
        BacklinkPage::default(),
        REFERENCE_BUDGET,
    )));
    assert!(matches!(
        references.next(empty),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let none = child(Ok::<Vec<MetadataReferenceEntry>, MetadataApiError>(
        Vec::new(),
    ));
    let Ok(Next::Done(page)) = references.next(none) else {
        panic!("expected the page");
    };
    assert!(page.references.is_empty() && page.next_after.is_none());
}

#[test]
fn spent_budget_unavailable() {
    let iri = "https://example.test/object";
    let mut references = ReferencesOperation::new(realm(), request(iri));
    references.first().unwrap();
    let spent = child(Err::<(BacklinkPage, LinkBudget), _>(
        BacklinkError::BudgetExhausted,
    ));
    assert_eq!(
        references.next(spent),
        Err(MetadataApiError::ServiceUnavailable)
    );
}

#[test]
fn unknown_graph_omitted() {
    let graph = ReferenceEntriesOperation::graph((realm(), None), "https://example.test/x");
    let mut graph = graph;
    assert_eq!(graph.first(), Ok(Next::Done(Vec::new())));
}
