//! Tests merging of query results across nodes, select limits and query form validation.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

use super::super::distributed::ordered_query;
use super::super::query_fanout::{Partition, TieFetchOperation};
use crate::metadata::api::access::tests::realm;
use crate::metadata::local_query::LocalQueryOperation;
use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::metadata::{MergeJob, MergeOutput, MetadataEffect, MetadataError, MetadataEvent};
use aruna_core::operation::Operation;

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
        Some(partition) => {
            let job = MergeJob::Order {
                partition,
                results: parts,
            };
            let Ok(MergeOutput::Rows(rows)) = aruna_graph::merge::merge(job, &Default::default())
            else {
                panic!("expected merged rows");
            };
            aggregate_query_results(vec![rows], MetadataQueryForm::Select, limit)
        }
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
    let partition = ordered_query(query).unwrap().unwrap();
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
fn equal_values_cut() {
    // Tied engine picks vary per process, so distinct trials make a wrong cut show reliably.
    let query = "SELECT DISTINCT ?n WHERE { ?s <urn:k> ?n } ORDER BY ?n LIMIT 1";
    let partition = ordered_query(query).unwrap().unwrap();
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

#[test]
fn partition_query_widest() {
    // Tie fetches raise LIMIT up to the row limit, so the widest partition text must still fit.
    let query = |fill: usize| {
        format!(
            "SELECT DISTINCT ?s WHERE {{ ?s ?p ?o FILTER(?o != \"{}\") }} ORDER BY ?s LIMIT 1",
            "x".repeat(fill)
        )
    };
    let base = ordered_query(&query(0)).unwrap().unwrap().len();
    let fill = QUERY_MAX_BYTES - base;
    assert!(matches!(
        ordered_query(&query(fill)),
        Err(MetadataApiError::InvalidQuery(_))
    ));
    let widest = QUERY_MAX_ROWS.to_string().len() - 1;
    let partition = ordered_query(&query(fill - widest)).unwrap().unwrap();
    assert_eq!(partition.len() + widest, QUERY_MAX_BYTES);
}

// The query with its `LIMIT` set to `fetch`, as a tie fetch asks it.
fn with_limit(query: &str, fetch: usize) -> Option<String> {
    let mut parsed = spargebra::SparqlParser::new().parse_query(query).ok()?;
    let spargebra::Query::Select {
        pattern: spargebra::algebra::GraphPattern::Slice { length, .. },
        ..
    } = &mut parsed
    else {
        return None;
    };
    *length = Some(fetch);
    Some(parsed.to_string())
}

/// The text of a local partition query, among the texts a tie fetch of `query` may run.
fn local_text(call: &IoCall, query: &str, limit: Option<usize>) -> String {
    let fetches = (limit.into_iter())
        .flat_map(|limit| (0..32).map(move |shift| limit.saturating_add(1 << shift)))
        .filter_map(|fetch| with_limit(query, fetch.min(QUERY_MAX_ROWS)));
    let mut texts = std::iter::once(query.to_string()).chain(fetches);
    texts
        .find(|text| {
            let local = LocalQueryOperation::new((realm(), None), None, text.clone());
            *call == IoCall::child(local)
        })
        .expect("a text of the tie fetch")
}

/// Runs a tie fetch on one partition, answering each query with `answer` and each tie count with
/// the graph merge, and returns its result with the query texts it ran.
fn run_fetch(
    query: &str,
    limit: Option<usize>,
    answer: impl Fn(&str) -> Result<MetadataQueryResults, MetadataError>,
) -> (Result<MetadataQueryResults, MetadataReadError>, Vec<String>) {
    let partition = Partition::Local(realm(), None);
    let mut fetch = TieFetchOperation::new(partition, (None, query.to_string()), limit, u64::MAX);
    let mut runs = Vec::new();
    fetch.stamp(0);
    let mut next = fetch.first();
    loop {
        let call = match next {
            Ok(Next::Done(results)) => return (Ok(results), runs),
            Ok(Next::Call(call)) => call,
            Err(error) => return (Err(error), runs),
            Ok(_) => panic!("expected one call per step"),
        };
        let result = match call {
            IoCall::Effect(effect) => {
                let Effect::Metadata(MetadataEffect::MergeRows { job, .. }) = *effect else {
                    panic!("expected a merge");
                };
                let merged = MetadataEvent::RowsMerged {
                    result: aruna_graph::merge::merge(job, &Default::default()),
                };
                IoResult::Event(Box::new(Event::Metadata(merged)))
            }
            call => {
                let sparql = local_text(&call, query, limit);
                let result = answer(&sparql);
                runs.push(sparql);
                IoResult::Child(ChildOutcome::new(result))
            }
        };
        fetch.stamp(0);
        next = fetch.next(result);
    }
}

fn fetch_partition(query: &str, objects: &[(&str, String)]) -> Vec<BTreeMap<String, String>> {
    let partition = ordered_query(query).unwrap().unwrap();
    let limit = query_select_limit(query);
    let (fetched, _) = run_fetch(&partition, limit, |text| {
        Ok(MetadataQueryResults::Solutions(partition_rows(
            text, objects,
        )))
    });
    let Ok(MetadataQueryResults::Solutions(rows)) = fetched else {
        panic!("expected solutions");
    };
    rows
}

#[test]
fn canonical_ties_merge() {
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
                let first = fetch_partition(query, &objects);
                let second = fetch_partition(query, &[("m", other.clone())]);
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

#[test]
fn tie_group_whole() {
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
    let mut rows = fetch_partition(query, &objects);
    rows.sort();
    let mut expected: Vec<_> = tied
        .iter()
        .map(|term| solution_row(&[("k", term)]))
        .collect();
    expected.sort();
    assert_eq!(rows, expected);
}

#[test]
fn unordered_runs_once() {
    let query = "SELECT DISTINCT ?s WHERE { ?s ?p ?o } LIMIT 1";
    let rows = vec![
        solution_row(&[("s", "<urn:a>")]),
        solution_row(&[("s", "<urn:b>")]),
    ];
    let (fetched, runs) = run_fetch(query, None, |_| {
        Ok(MetadataQueryResults::Solutions(rows.clone()))
    });
    assert_eq!(runs, vec![query.to_string()]);
    assert_eq!(fetched, Ok(MetadataQueryResults::Solutions(rows)));
}

#[test]
fn failed_fetch_unavailable() {
    let query = ordered_query("SELECT DISTINCT ?s WHERE { ?s ?p ?o } ORDER BY ?s LIMIT 1");
    let (fetched, runs) = run_fetch(&query.unwrap().unwrap(), Some(1), |_| {
        Err(MetadataError::Backend("storage".to_string()))
    });
    assert_eq!(runs.len(), 1);
    assert_eq!(fetched, Err(MetadataReadError::Unavailable));
}

/// Runs a partition like craqle, which refuses results over its row limit instead of cutting them.
fn capped_partition(query: &str, objects: &[(&str, String)]) -> MetadataQueryResults {
    let partition = ordered_query(query).unwrap().unwrap();
    let limit = query_select_limit(query);
    let (fetched, _) = run_fetch(&partition, limit, |text| {
        let rows = partition_rows(text, objects);
        match rows.len() > QUERY_MAX_ROWS {
            true => Err(MetadataError::Backend("too many rows".to_string())),
            false => Ok(MetadataQueryResults::Solutions(rows)),
        }
    });
    fetched.unwrap()
}

#[test]
fn row_limit_fetch() {
    let query =
        format!("SELECT DISTINCT ?k WHERE {{ ?s <urn:k> ?k }} ORDER BY ?k LIMIT {QUERY_MAX_ROWS}");
    let objects: Vec<_> = (0..QUERY_MAX_ROWS + 5)
        .map(|index| ("s", format!("\"{index}\"^^<{INTEGER}>")))
        .collect();
    let MetadataQueryResults::Solutions(rows) = capped_partition(&query, &objects) else {
        panic!("expected solutions");
    };
    assert_eq!(rows.len(), QUERY_MAX_ROWS);
}

#[test]
fn ties_stop_limit() {
    // The tie group at the cut runs past the row limit, so the partition keeps what fits.
    let limit = QUERY_MAX_ROWS - 1;
    let query = format!("SELECT DISTINCT ?k WHERE {{ ?s <urn:k> ?k }} ORDER BY ?k LIMIT {limit}");
    let xsd = "http://www.w3.org/2001/XMLSchema#";
    let mut objects: Vec<_> = (1..limit)
        .map(|index| ("s", format!("\"-{index}\"^^<{INTEGER}>")))
        .collect();
    objects.extend([
        ("s", format!("\"1\"^^<{xsd}int>")),
        ("s", format!("\"1\"^^<{xsd}integer>")),
        ("s", format!("\"01\"^^<{xsd}integer>")),
    ]);
    let MetadataQueryResults::Solutions(rows) = capped_partition(&query, &objects) else {
        panic!("expected solutions");
    };
    assert_eq!(rows.len(), QUERY_MAX_ROWS);
}
