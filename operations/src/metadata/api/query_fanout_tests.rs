//! Drives the distributed query through its cache, fanout, merge and join answers without I/O.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::effects::NetEffect;
use aruna_core::metadata::SolutionRow;

const ASK: &str = "ASK WHERE { ?s ?p ?o }";
const ORDERED: &str = "SELECT DISTINCT ?s WHERE { ?s ?p ?o } ORDER BY ?s LIMIT 2";
const JOIN_QUERY: &str = "SELECT DISTINCT ?dataset ?name WHERE { \
    ?dataset <urn:hasPart> ?file . ?file <urn:name> ?name } LIMIT 10";

fn realm() -> RealmId {
    RealmId::from_bytes([1; 32])
}

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn subjects(values: &[&str]) -> MetadataQueryResults {
    let row = |value: &&str| SolutionRow::from([("s".to_string(), value.to_string())]);
    MetadataQueryResults::Solutions(values.iter().map(row).collect())
}

fn stats(nodes_queried: usize, nodes_failed: usize) -> MetadataFanoutStats {
    MetadataFanoutStats {
        nodes_queried,
        nodes_failed,
        ..MetadataFanoutStats::default()
    }
}

/// A query in `mode`, anonymous unless `bearer` is given, so only anonymous ones use the cache.
fn query(mode: ApiQueryMode, text: &str, bearer: Option<&str>) -> DistributedQueryOperation {
    let scope = MetadataFanoutScope::new(Some(mode), None, true);
    let caller = (None, bearer.map(str::to_string));
    let mut query = DistributedQueryOperation::request(
        (realm(), node(1)),
        caller,
        (None, text.to_string()),
        scope,
    );
    query.stamp(1_000);
    query
}

fn metadata(event: MetadataEvent) -> IoResult {
    IoResult::Event(Box::new(Event::Metadata(event)))
}

fn fanout(parts: Vec<MetadataQueryResults>, stats: MetadataFanoutStats) -> IoResult {
    let parts: Vec<_> = parts.into_iter().map(|part| (node(1), part)).collect();
    let answer: Result<FanoutParts<MetadataQueryResults>, MetadataApiError> = Ok((parts, stats));
    IoResult::Child(ChildOutcome::new(answer))
}

fn effect(next: Result<Next<QueryAnswer>, MetadataApiError>) -> MetadataEffect {
    match next {
        Ok(Next::Call(IoCall::Effect(effect))) => match *effect {
            Effect::Metadata(effect) => effect,
            _ => panic!("expected a metadata effect"),
        },
        _ => panic!("expected one effect"),
    }
}

fn done(next: Result<Next<QueryAnswer>, MetadataApiError>) -> QueryAnswer {
    match next {
        Ok(Next::Done(answer)) => answer,
        _ => panic!("expected an answer"),
    }
}

/// An anonymous query that missed the cache with `stamp` and now waits for its fanout.
fn missed(mode: ApiQueryMode, text: &str) -> DistributedQueryOperation {
    let mut query = query(mode, text, None);
    assert!(matches!(
        effect(query.first()),
        MetadataEffect::QueryCacheGet { .. }
    ));
    let miss = MetadataEvent::QueryCacheEntry {
        stamp: [4, 5],
        entry: None,
    };
    assert!(matches!(
        query.next(metadata(miss)),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    query
}

#[test]
fn cache_hit_answers() {
    let mut query = query(ApiQueryMode::Local, ASK, None);
    assert!(matches!(
        effect(query.first()),
        MetadataEffect::QueryCacheGet { .. }
    ));
    let hit = MetadataEvent::QueryCacheEntry {
        stamp: [4, 5],
        entry: Some((MetadataQueryResults::Boolean(true), 3)),
    };
    let answer = done(query.next(metadata(hit)));
    assert_eq!(
        answer,
        execution(MetadataQueryResults::Boolean(true), stats(3, 0))
    );
}

#[test]
fn complete_answer_cached() {
    let mut query = missed(ApiQueryMode::Local, ASK);
    let answered = query.next(fanout(
        vec![MetadataQueryResults::Boolean(true)],
        stats(1, 0),
    ));
    let Ok(Next::Then { output, calls }) = answered else {
        panic!("expected the answer and its cache store");
    };
    assert_eq!(
        output,
        execution(MetadataQueryResults::Boolean(true), stats(1, 0))
    );
    let [IoCall::Effect(store)] = calls.as_slice() else {
        panic!("expected one store");
    };
    let Effect::Metadata(MetadataEffect::QueryCachePut {
        stamp,
        results,
        nodes_queried,
        ..
    }) = store.as_ref()
    else {
        panic!("expected a cache store");
    };
    assert_eq!(
        (*stamp, results, *nodes_queried),
        ([4, 5], &MetadataQueryResults::Boolean(true), 1)
    );
}

#[test]
fn partial_answer_uncached() {
    let mut query = missed(ApiQueryMode::Distributed, ASK);
    let parts = vec![MetadataQueryResults::Boolean(false)];
    let answer = done(query.next(fanout(parts, stats(2, 1))));
    assert_eq!(
        answer,
        execution(MetadataQueryResults::Boolean(false), stats(2, 1))
    );
}

#[test]
fn bearer_skips_cache() {
    let mut query = query(ApiQueryMode::Local, ASK, Some("token"));
    assert!(matches!(query.first(), Ok(Next::Call(IoCall::Child(_)))));
}

#[test]
fn ordered_parts_merge() {
    let mut query = query(ApiQueryMode::Distributed, ORDERED, Some("token"));
    assert!(matches!(query.first(), Ok(Next::Call(IoCall::Child(_)))));
    let parts = vec![subjects(&["<urn:c>", "<urn:a>"]), subjects(&["<urn:b>"])];
    let MetadataEffect::MergeRows {
        job: MergeJob::Order { partition, results },
        budget,
    } = effect(query.next(fanout(parts.clone(), stats(2, 0))))
    else {
        panic!("expected an ordered merge");
    };
    assert_eq!(Some(partition), ordered_query(ORDERED).unwrap());
    assert_eq!(results, parts);
    assert_eq!(budget, DISTRIBUTED_QUERY_DEADLINE);
    let merged = MetadataEvent::RowsMerged {
        result: Ok(MergeOutput::Rows(subjects(&[
            "<urn:a>", "<urn:b>", "<urn:c>",
        ]))),
    };
    let answer = done(query.next(metadata(merged)));
    assert_eq!(
        answer,
        execution(subjects(&["<urn:a>", "<urn:b>"]), stats(2, 0))
    );
}

#[test]
fn merge_refusal_maps() {
    let mut query = query(ApiQueryMode::Distributed, ORDERED, Some("token"));
    query.first().unwrap();
    effect(query.next(fanout(vec![subjects(&["<urn:a>"])], stats(1, 0))));
    let refused = MetadataEvent::RowsMerged {
        result: Err(MergeError::Refused("narrow it".to_string())),
    };
    let answer = query.next(metadata(refused));
    assert!(
        matches!(answer, Err(MetadataApiError::InvalidQuery(message)) if message == "narrow it")
    );
}

#[test]
fn join_runs_sides() {
    let mut query = query(ApiQueryMode::Distributed, JOIN_QUERY, None);
    let Ok(Next::Call(IoCall::Group(group))) = query.first() else {
        panic!("expected both sides at once");
    };
    assert_eq!(group.members.len(), 2);
    let side = |rows: MetadataQueryResults, failed: Vec<NodeId>| {
        let answer: Result<QueryAnswer, MetadataApiError> = Ok(MetadataQueryExecution {
            results: rows,
            fanout_stats: MetadataFanoutStats {
                nodes_queried: 2,
                nodes_failed: failed.len(),
                failed_partitions: failed,
                ..MetadataFanoutStats::default()
            },
        });
        GroupResult::Done(Box::new(Event::Child(ChildOutcome::new(answer))))
    };
    let left = SolutionRow::from([("dataset".to_string(), "<urn:d>".to_string())]);
    let results = vec![
        side(
            MetadataQueryResults::Solutions(vec![left.clone()]),
            vec![node(2)],
        ),
        side(MetadataQueryResults::Boolean(true), vec![node(2), node(3)]),
    ];
    let sides = IoResult::Group(GroupEvent::Finished { results });
    let MetadataEffect::MergeRows {
        job:
            MergeJob::Join {
                query: text,
                left: rows,
                right,
            },
        ..
    } = effect(query.next(sides))
    else {
        panic!("expected a join merge");
    };
    assert_eq!(
        (text.as_str(), rows, right),
        (JOIN_QUERY, vec![left], Vec::new())
    );
    let joined = MetadataEvent::RowsMerged {
        result: Ok(MergeOutput::Rows(subjects(&["<urn:d>"]))),
    };
    let answer = done(query.next(metadata(joined)));
    let expected = MetadataFanoutStats {
        nodes_queried: 2,
        nodes_failed: 2,
        failed_partitions: vec![node(2), node(3)],
        ..MetadataFanoutStats::default()
    };
    assert_eq!(answer, execution(subjects(&["<urn:d>"]), expected));
}

#[test]
fn join_sides_safe() {
    let plan = aruna_graph::merge::join_plan(JOIN_QUERY).unwrap();
    assert!(plan.sides.iter().all(|side| query_union_safe(side)));
}

#[test]
fn request_checks_form() {
    let mut query = query(ApiQueryMode::Local, "CONSTRUCT WHERE { ?s ?p ?o }", None);
    assert!(matches!(query.first(), Err(MetadataApiError::BadRequest)));
}

#[test]
fn remote_asks_peer() {
    let partition = Partition::Remote {
        node: node(2),
        auth_token: None,
    };
    let mut fetch = TieFetchOperation::new(partition, (None, ASK.to_string()), None, 5_000);
    fetch.stamp(1_000);
    let Ok(Next::Call(IoCall::Effect(call))) = fetch.first() else {
        panic!("expected one peer request");
    };
    let Effect::Net(NetEffect::MetadataRequest(request)) = *call else {
        panic!("expected a metadata request");
    };
    assert_eq!(
        (request.peer, request.timeout),
        (node(2), Duration::from_millis(4_000))
    );
}
