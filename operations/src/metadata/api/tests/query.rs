//! Tests merging of query results across nodes, select limits and query form validation.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

use super::super::distributed::{fetch_ties, merge_ordered, order_solutions, ordered_query};

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
    let merged = match ordered_query(query).unwrap() {
        Some((_, order)) => order_solutions(parts, &order, &Default::default())
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

fn partition_rows(query: &str, objects: &[(&str, String)]) -> Vec<BTreeMap<String, String>> {
    let mut dataset = oxrdf::Dataset::new();
    for (subject, object) in objects {
        dataset.insert(&oxrdf::Quad::new(
            oxrdf::NamedNode::new_unchecked(format!("urn:{subject}")),
            oxrdf::NamedNode::new_unchecked("urn:k"),
            object.parse::<oxrdf::Term>().unwrap(),
            oxrdf::GraphName::DefaultGraph,
        ));
    }
    let query = spargebra::SparqlParser::new().parse_query(query).unwrap();
    let evaluator = spareval::QueryEvaluator::new();
    let Ok(spareval::QueryResults::Solutions(solutions)) =
        evaluator.prepare(&query).execute(&dataset)
    else {
        panic!("expected solutions");
    };
    solutions
        .map(|solution| {
            let solution = solution.unwrap();
            solution
                .iter()
                .map(|(variable, term)| (variable.as_str().to_string(), term.to_string()))
                .collect()
        })
        .collect()
}

#[test]
fn ties_cut_consistently() {
    let query = "SELECT DISTINCT ?s ?k WHERE { ?s <urn:k> ?k } ORDER BY ?k LIMIT 1";
    let (partition, _) = ordered_query(query).unwrap().unwrap();
    assert!(ensure_query_form(&partition).is_ok());
    let one = format!("\"1\"^^<{INTEGER}>");
    let first: Vec<_> = ["z", "y", "x", "w", "a"]
        .map(|subject| (subject, one.clone()))
        .into();
    let second: Vec<_> = ["v", "u", "t", "b"]
        .map(|subject| (subject, one.clone()))
        .into();
    let first = partition_rows(&partition, &first);
    let second = partition_rows(&partition, &second);
    assert_eq!((first.len(), second.len()), (1, 1));
    assert_eq!(
        merge_parts(query, vec![first, second]),
        vec![solution_row(&[("s", "<urn:a>"), ("k", &one)])]
    );
}

#[test]
fn equal_values_cut_consistently() {
    // Tied engine picks vary per process, so distinct trials make a wrong cut show reliably.
    let query = "SELECT DISTINCT ?n WHERE { ?s <urn:k> ?n } ORDER BY ?n LIMIT 1";
    let (partition, _) = ordered_query(query).unwrap().unwrap();
    assert!(ensure_query_form(&partition).is_ok());
    let xsd = "http://www.w3.org/2001/XMLSchema#";
    for value in 1..=20 {
        let numbers = [
            format!("\"{value}\"^^<{xsd}integer>"),
            format!("\"{value}.0E0\"^^<{xsd}double>"),
            format!("\"{value}.0\"^^<{xsd}decimal>"),
        ];
        let texts = [
            format!("\"t{value}\"@en--rtl"),
            format!("\"t{value}\"@en--ltr"),
        ];
        for terms in [&numbers[..], &texts[..]] {
            let first: Vec<_> = terms
                .iter()
                .enumerate()
                .map(|(index, term)| (["a", "b", "c"][index], term.clone()))
                .collect();
            let second = vec![("d", terms[0].clone())];
            let first = partition_rows(&partition, &first);
            let second = partition_rows(&partition, &second);
            let expected = if terms.len() == 3 {
                &terms[2]
            } else {
                &terms[1]
            };
            assert_eq!(
                merge_parts(query, vec![first, second]),
                vec![solution_row(&[("n", expected)])]
            );
        }
    }
}

#[test]
fn partition_query_bounded() {
    let name = "v".repeat(20_000);
    let query = format!("SELECT DISTINCT ?{name} WHERE {{ ?{name} ?p ?o }} ORDER BY ?{name}");
    assert!(ensure_query_form(&query).is_ok());
    let Err(MetadataApiError::InvalidQuery(message)) = ordered_query(&query) else {
        panic!("expected a refused partition query");
    };
    assert!(message.contains("too long to run across the realm"));
}

fn large_partition(prefix: &str) -> MetadataQueryResults {
    let value = format!("\"{}\"", "x".repeat(1024 * 1024));
    MetadataQueryResults::Solutions(
        (0..5)
            .map(|index| solution_row(&[("s", &format!("<urn:{prefix}{index}>")), ("v", &value)]))
            .collect(),
    )
}

#[test]
fn merge_refuses_oversize() {
    let order = ordered_query("SELECT DISTINCT ?s ?v WHERE { ?s <urn:v> ?v } ORDER BY ?s LIMIT 5")
        .unwrap()
        .unwrap()
        .1;
    let parts = vec![large_partition("a"), large_partition("b")];
    let merged = order_solutions(parts, &order, &Default::default());
    assert!(matches!(merged, Err(MetadataApiError::BadRequest)));
}

#[test]
fn merge_stops_cancelled() {
    let order = ordered_query("SELECT DISTINCT ?s WHERE { ?s ?p ?o } ORDER BY ?s")
        .unwrap()
        .unwrap()
        .1;
    let parts = vec![MetadataQueryResults::Solutions(vec![solution_row(&[(
        "s", "<urn:a>",
    )])])];
    let cancel = spareval::CancellationToken::new();
    cancel.cancel();
    let merged = order_solutions(parts, &order, &cancel);
    assert!(matches!(merged, Err(MetadataApiError::ServiceUnavailable)));
}

#[tokio::test]
async fn merge_past_deadline() {
    let order = ordered_query("SELECT DISTINCT ?s WHERE { ?s ?p ?o } ORDER BY ?s")
        .unwrap()
        .unwrap()
        .1;
    let parts = vec![MetadataQueryResults::Solutions(vec![solution_row(&[(
        "s", "<urn:a>",
    )])])];
    let permits = std::sync::Arc::new(tokio::sync::Semaphore::new(1));
    let merged = merge_ordered(parts, order, tokio::time::Instant::now(), permits).await;
    assert!(matches!(merged, Err(MetadataApiError::ServiceUnavailable)));
}

#[tokio::test]
async fn merge_waits_permit() {
    let order = ordered_query("SELECT DISTINCT ?s WHERE { ?s ?p ?o } ORDER BY ?s")
        .unwrap()
        .unwrap()
        .1;
    // A row the merge cannot parse would answer an internal error if the merge ever ran.
    let parts = vec![MetadataQueryResults::Solutions(vec![solution_row(&[(
        "s",
        "not a term",
    )])])];
    let permits = std::sync::Arc::new(tokio::sync::Semaphore::new(0));
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(200);
    let merged = merge_ordered(parts, order, deadline, permits).await;
    assert!(matches!(merged, Err(MetadataApiError::ServiceUnavailable)));
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
    assert!(ordered_query(query).unwrap().is_none());
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

async fn fetch_partition(query: &str, objects: &[(&str, String)]) -> Vec<BTreeMap<String, String>> {
    let (partition, order) = ordered_query(query).unwrap().unwrap();
    let ties = query_select_limit(query).map(|limit| (order, limit));
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(600);
    let permits = std::sync::Arc::new(tokio::sync::Semaphore::new(1));
    let fetched = fetch_ties(partition, ties, deadline, permits, |text| async move {
        Ok(MetadataQueryResults::Solutions(partition_rows(
            &text, objects,
        )))
    })
    .await;
    let Ok(MetadataQueryResults::Solutions(rows)) = fetched else {
        panic!("expected solutions");
    };
    rows
}

#[tokio::test]
async fn canonical_ties_merge() {
    // spareval reads these pairs as one value and datatype, but results keep each original term.
    let query = "SELECT DISTINCT ?k WHERE { ?s <urn:k> ?k } ORDER BY ?k LIMIT 1";
    let xsd = "http://www.w3.org/2001/XMLSchema#";
    for value in 1..=20 {
        for pair in [
            [
                format!("\"{value}\"^^<{xsd}int>"),
                format!("\"{value}\"^^<{xsd}integer>"),
            ],
            [
                format!("\"{value}\"^^<{xsd}integer>"),
                format!("\"0{value}\"^^<{xsd}integer>"),
            ],
        ] {
            let preferred = pair.iter().min().unwrap();
            let other = pair.iter().max().unwrap();
            for subjects in [["a", "z"], ["z", "a"]] {
                let objects = [
                    (subjects[0], preferred.clone()),
                    (subjects[1], other.clone()),
                ];
                let first = fetch_partition(query, &objects).await;
                let second = fetch_partition(query, &[("m", other.clone())]).await;
                for parts in [vec![first.clone(), second.clone()], vec![second, first]] {
                    assert_eq!(
                        merge_parts(query, parts),
                        vec![solution_row(&[("k", preferred)])]
                    );
                }
            }
        }
    }
}

#[tokio::test]
async fn tie_group_whole() {
    let query = "SELECT DISTINCT ?k WHERE { ?s <urn:k> ?k } ORDER BY ?k LIMIT 2";
    let xsd = "http://www.w3.org/2001/XMLSchema#";
    let tied = [
        format!("\"1\"^^<{xsd}int>"),
        format!("\"1\"^^<{xsd}integer>"),
        format!("\"01\"^^<{xsd}integer>"),
        format!("\"001\"^^<{xsd}integer>"),
    ];
    let mut objects: Vec<_> = ["a", "b", "c", "d"]
        .into_iter()
        .zip(tied.iter().cloned())
        .collect();
    objects.push(("e", format!("\"2\"^^<{xsd}integer>")));
    let mut rows = fetch_partition(query, &objects).await;
    rows.sort();
    let mut expected: Vec<_> = tied
        .iter()
        .map(|term| solution_row(&[("k", term)]))
        .collect();
    expected.sort();
    assert_eq!(rows, expected);
}

#[test]
fn merge_refuses_ties() {
    // Every partition may now return a tie group past LIMIT, so only the byte budget bounds it.
    let order = ordered_query("SELECT DISTINCT ?s ?v WHERE { ?s <urn:v> ?v } ORDER BY ?s LIMIT 1")
        .unwrap()
        .unwrap()
        .1;
    let parts = vec![large_partition("a"), large_partition("b")];
    let merged = order_solutions(parts, &order, &Default::default());
    assert!(matches!(merged, Err(MetadataApiError::BadRequest)));
}

#[tokio::test]
async fn unordered_runs_once() {
    let query = "SELECT DISTINCT ?s WHERE { ?s ?p ?o } LIMIT 1".to_string();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(600);
    let permits = std::sync::Arc::new(tokio::sync::Semaphore::new(1));
    let runs = std::sync::Mutex::new(Vec::new());
    let rows = vec![
        solution_row(&[("s", "<urn:a>")]),
        solution_row(&[("s", "<urn:b>")]),
    ];
    let fetched = fetch_ties(query.clone(), None, deadline, permits, |text| {
        runs.lock().unwrap().push(text);
        let rows = rows.clone();
        async move { Ok(MetadataQueryResults::Solutions(rows)) }
    })
    .await;
    assert_eq!(*runs.lock().unwrap(), vec![query]);
    assert_eq!(fetched, Ok(MetadataQueryResults::Solutions(rows)));
}
