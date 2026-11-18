//! Tests the merge jobs: ordering, tie counts, bounded joins, cancellation and merge permits.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

const ORDERED: &str = "SELECT DISTINCT ?s WHERE { ?s ?p ?o } ORDER BY ?s";

const JOIN_QUERY: &str = "SELECT DISTINCT ?dataset ?name WHERE { \
    ?dataset <urn:hasPart> ?file . ?file <urn:name> ?name } LIMIT 10";

fn solution_row(pairs: &[(&str, &str)]) -> Row {
    pairs
        .iter()
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect()
}

fn subjects(values: &[&str]) -> MetadataQueryResults {
    let rows = values.iter().map(|value| solution_row(&[("s", value)]));
    MetadataQueryResults::Solutions(rows.collect())
}

/// The rows `query` returns on one shard holding `triples`.
fn shard_rows(query: &str, triples: &[(&str, &str, &str)]) -> Vec<Row> {
    let mut dataset = oxrdf::Dataset::new();
    for (subject, predicate, object) in triples {
        dataset.insert(&oxrdf::Quad::new(
            oxrdf::NamedNode::new_unchecked(*subject),
            oxrdf::NamedNode::new_unchecked(*predicate),
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
fn orders_partitions() {
    let job = MergeJob::Order {
        partition: ORDERED.to_string(),
        results: vec![
            subjects(&["<urn:c>", "<urn:a>"]),
            subjects(&["<urn:b>", "<urn:a>"]),
        ],
    };
    let merged = merge(job, &Default::default());
    let expected = subjects(&["<urn:a>", "<urn:b>", "<urn:c>"]);
    assert_eq!(merged, Ok(MergeOutput::Rows(expected)));
}

#[test]
fn counts_tied_rows() {
    let partition = "SELECT DISTINCT ?s ?k WHERE { ?s <urn:k> ?k } ORDER BY ?k";
    let rows = vec![
        solution_row(&[("s", "<urn:a>"), ("k", "\"1\"")]),
        solution_row(&[("s", "<urn:b>"), ("k", "\"1\"")]),
        solution_row(&[("s", "<urn:c>"), ("k", "\"2\"")]),
    ];
    let job = MergeJob::Ties {
        partition: partition.to_string(),
        rows,
    };
    assert_eq!(merge(job, &Default::default()), Ok(MergeOutput::Tied(2)));
}

#[test]
fn unordered_partition_refused() {
    let job = MergeJob::Order {
        partition: "SELECT DISTINCT ?s WHERE { ?s ?p ?o }".to_string(),
        results: vec![subjects(&["<urn:a>"])],
    };
    assert!(matches!(
        merge(job, &Default::default()),
        Err(MergeError::Failed(_))
    ));
}

#[test]
fn merge_refuses_oversize() {
    let value = format!("\"{}\"", "x".repeat(1024 * 1024));
    let large = |prefix: &str| {
        MetadataQueryResults::Solutions(
            (0..5)
                .map(|index| {
                    solution_row(&[("s", &format!("<urn:{prefix}{index}>")), ("v", &value)])
                })
                .collect(),
        )
    };
    let job = MergeJob::Order {
        partition: "SELECT DISTINCT ?s ?v WHERE { ?s <urn:v> ?v } ORDER BY ?s LIMIT 5".to_string(),
        results: vec![large("a"), large("b")],
    };
    assert_eq!(merge(job, &Default::default()), Err(MergeError::TooLarge));
}

#[test]
fn merge_stops_cancelled() {
    let job = MergeJob::Order {
        partition: ORDERED.to_string(),
        results: vec![subjects(&["<urn:a>"])],
    };
    let cancel = spareval::CancellationToken::new();
    cancel.cancel();
    assert_eq!(merge(job, &cancel), Err(MergeError::Cancelled));
}

#[tokio::test]
async fn merge_past_deadline() {
    let job = MergeJob::Order {
        partition: ORDERED.to_string(),
        results: vec![subjects(&["<urn:a>"])],
    };
    let merged = run_merge(job, Duration::ZERO).await;
    assert_eq!(merged, Err(MergeError::Cancelled));
}

#[tokio::test]
async fn merge_waits_permit() {
    // A row the merge cannot parse would answer a failure if the merge ever ran.
    let job = MergeJob::Order {
        partition: ORDERED.to_string(),
        results: vec![subjects(&["not a term"])],
    };
    let permits = Arc::new(Semaphore::new(0));
    let merged = run_permitted(job, Duration::from_millis(200), permits).await;
    assert_eq!(merged, Err(MergeError::Cancelled));
}

#[test]
fn join_finds_split() {
    // A dataset on one shard names a file whose name lives on another shard.
    let first = [
        ("urn:d1", "urn:hasPart", "<urn:f1>"),
        ("urn:d2", "urn:hasPart", "<urn:f2>"),
    ];
    let second = [
        ("urn:f1", "urn:name", "\"reads.fastq\""),
        ("urn:f3", "urn:name", "\"other.txt\""),
    ];
    assert!(shard_rows(JOIN_QUERY, &first).is_empty());
    assert!(shard_rows(JOIN_QUERY, &second).is_empty());
    let union = shard_rows(JOIN_QUERY, &[first.as_slice(), &second].concat());
    assert_eq!(union.len(), 1);

    let plan = join_plan(JOIN_QUERY).unwrap();
    let side = |query: &str| [shard_rows(query, &first), shard_rows(query, &second)].concat();
    let (left, right) = (side(&plan.sides[0]), side(&plan.sides[1]));
    let job = MergeJob::Join {
        query: JOIN_QUERY.to_string(),
        left: left.clone(),
        right: right.clone(),
    };
    let joined = merge(job, &Default::default());
    assert_eq!(
        joined,
        Ok(MergeOutput::Rows(MetadataQueryResults::Solutions(union)))
    );
    // A join past its deadline stops instead of holding the merge permit.
    let cancel = spareval::CancellationToken::new();
    cancel.cancel();
    let stopped = join_rows(&plan, left, right, &cancel);
    assert_eq!(stopped, Err(MergeError::Cancelled));
}

#[test]
fn join_plan_rejects() {
    for query in [
        "SELECT ?s WHERE { ?s <urn:a> ?o . ?o <urn:b> ?x }",
        "SELECT DISTINCT ?s WHERE { ?s <urn:a> ?o . ?x <urn:b> ?y }",
        "SELECT DISTINCT ?s WHERE { ?s <urn:a> ?o . ?o <urn:b> ?x . ?x <urn:c> ?y }",
        "SELECT DISTINCT ?s WHERE { ?s <urn:a> ?o . ?o <urn:b> ?x } ORDER BY ?s",
        "SELECT DISTINCT ?s WHERE { ?s <urn:a> ?o . ?o <urn:b> ?x } OFFSET 1",
        "SELECT DISTINCT ?s WHERE { ?s <urn:a> _:b . _:b <urn:b> ?x }",
        "SELECT DISTINCT ?s WHERE { ?s <urn:a> ?o OPTIONAL { ?o <urn:b> ?x } }",
        "SELECT DISTINCT ?s FROM <urn:g> WHERE { ?s <urn:a> ?o . ?o <urn:b> ?x }",
        "ASK WHERE { ?s <urn:a> ?o . ?o <urn:b> ?x }",
    ] {
        assert!(join_plan(query).is_none(), "{query}");
    }
}

#[test]
fn join_refuses_oversize() {
    // A side past its bound refuses the join rather than answering from a cut input.
    let plan = join_plan(JOIN_QUERY).unwrap();
    let rows = (0..=JOIN_SIDE_ROWS)
        .map(|index| solution_row(&[("dataset", &format!("<urn:d{index}>")), ("file", "<urn:f>")]))
        .collect();
    let refused = join_rows(&plan, rows, Vec::new(), &Default::default());
    assert!(matches!(refused, Err(MergeError::Refused(_))));
}
