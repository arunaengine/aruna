//! Tests merging of query results across nodes, select limits and query form validation.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

use super::super::distributed::{order_solutions, query_order};

const INTEGER: &str = "http://www.w3.org/2001/XMLSchema#integer";

fn solution_row(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect()
}

fn merge_parts(
    query: &str,
    parts: Vec<Vec<BTreeMap<String, String>>>,
) -> Vec<BTreeMap<String, String>> {
    let parts = parts
        .into_iter()
        .map(MetadataQueryResults::Solutions)
        .collect();
    let limit = query_select_limit(query);
    let merged = match query_order(query) {
        Some(order) => order_solutions(parts, &order)
            .and_then(|rows| aggregate_query_results(vec![rows], MetadataQueryForm::Select, limit)),
        None => aggregate_query_results(parts, MetadataQueryForm::Select, limit),
    };
    let Ok(MetadataQueryResults::Solutions(rows)) = merged else {
        panic!("expected solutions");
    };
    rows
}

#[test]
fn deduplicates_select_rows() {
    let results = aggregate_query_results(
        vec![
            MetadataQueryResults::Solutions(vec![
                BTreeMap::from([(String::from("s"), String::from("<urn:a>"))]),
                BTreeMap::from([(String::from("s"), String::from("<urn:b>"))]),
            ]),
            MetadataQueryResults::Solutions(vec![BTreeMap::from([(
                String::from("s"),
                String::from("<urn:a>"),
            )])]),
        ],
        MetadataQueryForm::Select,
        None,
    )
    .unwrap();

    let MetadataQueryResults::Solutions(rows) = results else {
        panic!("expected solutions");
    };
    assert_eq!(rows.len(), 2);
}

#[test]
fn reapplies_select_limit() {
    let results = aggregate_query_results(
        vec![
            MetadataQueryResults::Solutions(vec![
                BTreeMap::from([(String::from("s"), String::from("<urn:a>"))]),
                BTreeMap::from([(String::from("s"), String::from("<urn:b>"))]),
            ]),
            MetadataQueryResults::Solutions(vec![
                BTreeMap::from([(String::from("s"), String::from("<urn:c>"))]),
                BTreeMap::from([(String::from("s"), String::from("<urn:d>"))]),
            ]),
        ],
        MetadataQueryForm::Select,
        Some(3),
    )
    .unwrap();

    let MetadataQueryResults::Solutions(rows) = results else {
        panic!("expected solutions");
    };
    assert_eq!(rows.len(), 3);
}

#[test]
fn reads_outermost_limit() {
    assert_eq!(
        query_select_limit("SELECT ?s WHERE { ?s ?p ?o } LIMIT 5"),
        Some(5)
    );
    assert_eq!(
        query_select_limit("SELECT ?s WHERE { ?s ?p ?o } LIMIT 7 OFFSET 3"),
        Some(7)
    );
    assert_eq!(query_select_limit("SELECT ?s WHERE { ?s ?p ?o }"), None);
    assert_eq!(
        query_select_limit("SELECT ?s WHERE { { SELECT ?s WHERE { ?s ?p ?o } LIMIT 5 } ?s ?p ?o }"),
        None
    );
    assert_eq!(query_select_limit("ASK WHERE { ?s ?p ?o }"), None);
    assert_eq!(query_select_limit("not sparql"), None);
}

#[test]
fn query_form_accepts() {
    assert_eq!(
        query_form("PREFIX ex: <https://example.org/> SELECT ?s WHERE { ?s ?p ?o }").unwrap(),
        MetadataQueryForm::Select
    );
    assert_eq!(
        query_form("BASE <https://example.org/> ASK WHERE { ?s ?p ?o }").unwrap(),
        MetadataQueryForm::Ask
    );
    assert_eq!(query_form("CONSTRUCT WHERE { ?s ?p ?o }"), None);
}

#[test]
fn query_validation_rejects() {
    assert!(ensure_query_form("SELECT ?s WHERE { ?s ?p ?o }").is_ok());
    assert!(ensure_query_form("ASK WHERE { ?s ?p ?o }").is_ok());
    assert!(ensure_query_form("INSERT DATA { <urn:s> <urn:p> <urn:o> }").is_err());
    assert!(
        ensure_query_form("SELECT ?s WHERE { SERVICE <https://example.org/sparql> { ?s ?p ?o } }")
            .is_err()
    );
    assert!(
        ensure_query_form("ASK WHERE { FILTER EXISTS { SERVICE SILENT ?endpoint { ?s ?p ?o } } }")
            .is_err()
    );
}

#[test]
fn query_validation_accepts() {
    assert!(query_union_safe("ASK WHERE { ?s ?p ?o }"));
    assert!(!query_union_safe("ASK WHERE { ?s ?p ?o . ?s ?p2 ?o2 }"));
    assert!(query_union_safe(
        "SELECT DISTINCT ?s WHERE { ?s ?p ?o } LIMIT 10"
    ));
    assert!(!query_union_safe("SELECT ?s WHERE { ?s ?p ?o }"));
    assert!(!query_union_safe(
        "SELECT DISTINCT ?s WHERE { ?s ?p ?o . ?s ?p2 ?o2 }"
    ));
    assert!(!query_union_safe(
        "SELECT DISTINCT ?s WHERE { ?s ?p ?o } OFFSET 1"
    ));
    assert!(!query_union_safe(
        "SELECT (COUNT(*) AS ?count) WHERE { ?s ?p ?o }"
    ));
    assert!(query_union_safe(
        "SELECT DISTINCT ?s ?o WHERE { ?s ?p ?o } ORDER BY DESC(?o) ?s LIMIT 10"
    ));
    assert!(!query_union_safe(
        "SELECT DISTINCT ?s WHERE { ?s ?p ?o } ORDER BY ?o"
    ));
    assert!(!query_union_safe(
        "SELECT DISTINCT ?s WHERE { ?s ?p ?o } ORDER BY STR(?s)"
    ));
    assert!(!query_union_safe(
        "SELECT ?s WHERE { ?s ?p ?o } ORDER BY ?s"
    ));
}

#[test]
fn orders_numeric_desc() {
    let query = "SELECT DISTINCT ?s ?n WHERE { ?s <urn:n> ?n } ORDER BY DESC(?n) LIMIT 3";
    let number = |value: u32| format!("\"{value}\"^^<{INTEGER}>");
    let first = vec![
        solution_row(&[("s", "<urn:a>"), ("n", &number(9))]),
        solution_row(&[("s", "<urn:b>"), ("n", &number(10))]),
    ];
    let second = vec![
        solution_row(&[("s", "<urn:c>"), ("n", &number(100))]),
        solution_row(&[("s", "<urn:b>"), ("n", &number(10))]),
        solution_row(&[("s", "<urn:d>"), ("n", &number(2))]),
    ];
    let expected = vec![
        solution_row(&[("s", "<urn:c>"), ("n", &number(100))]),
        solution_row(&[("s", "<urn:b>"), ("n", &number(10))]),
        solution_row(&[("s", "<urn:a>"), ("n", &number(9))]),
    ];
    assert_eq!(
        merge_parts(query, vec![first.clone(), second.clone()]),
        expected
    );
    assert_eq!(merge_parts(query, vec![second, first]), expected);
}

#[test]
fn orders_mixed_terms() {
    let query = "SELECT DISTINCT ?k ?s WHERE { ?s <urn:k> ?k } ORDER BY ?k DESC(?s)";
    let first = vec![
        solution_row(&[("k", "\"b\""), ("s", "<urn:a>")]),
        solution_row(&[("k", "<urn:z>"), ("s", "<urn:a>")]),
        solution_row(&[("s", "<urn:a>")]),
    ];
    let second = vec![
        solution_row(&[("k", "\"a\""), ("s", "<urn:a>")]),
        solution_row(&[("k", "_:b1"), ("s", "<urn:a>")]),
        solution_row(&[("k", "\"a\""), ("s", "<urn:b>")]),
    ];
    let expected = vec![
        solution_row(&[("s", "<urn:a>")]),
        solution_row(&[("k", "_:b1"), ("s", "<urn:a>")]),
        solution_row(&[("k", "<urn:z>"), ("s", "<urn:a>")]),
        solution_row(&[("k", "\"a\""), ("s", "<urn:b>")]),
        solution_row(&[("k", "\"a\""), ("s", "<urn:a>")]),
        solution_row(&[("k", "\"b\""), ("s", "<urn:a>")]),
    ];
    assert_eq!(
        merge_parts(query, vec![first.clone(), second.clone()]),
        expected
    );
    assert_eq!(merge_parts(query, vec![second, first]), expected);
}

#[test]
fn unordered_keeps_arrival() {
    let query = "SELECT DISTINCT ?s WHERE { ?s ?p ?o } LIMIT 3";
    let first = vec![
        solution_row(&[("s", "<urn:z>")]),
        solution_row(&[("s", "<urn:a>")]),
    ];
    let second = vec![
        solution_row(&[("s", "<urn:m>")]),
        solution_row(&[("s", "<urn:b>")]),
    ];
    assert!(query_order(query).is_none());
    assert_eq!(
        merge_parts(query, vec![first, second]),
        vec![
            solution_row(&[("s", "<urn:z>")]),
            solution_row(&[("s", "<urn:a>")]),
            solution_row(&[("s", "<urn:m>")]),
        ]
    );
}

#[test]
fn query_validation_enforces() {
    assert!(ensure_query_form(&" ".repeat(QUERY_MAX_BYTES + 1)).is_err());
    assert!(
        ensure_query_form(&format!(
            "SELECT ?s WHERE {{ ?s ?p ?o }} LIMIT {}",
            QUERY_MAX_ROWS + 1
        ))
        .is_err()
    );
}
