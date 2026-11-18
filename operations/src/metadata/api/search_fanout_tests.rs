//! Drives the metadata search through its checks, cursor, discovery, fanout and signature.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::events::GroupResult;

fn realm() -> RealmId {
    RealmId::from_bytes([1; 32])
}

fn secret(seed: u8) -> iroh::SecretKey {
    iroh::SecretKey::from_bytes(&[seed; 32])
}

fn node(seed: u8) -> NodeId {
    secret(seed).public()
}

fn hit(subject: &str, score: f32) -> MetadataSearchHit {
    MetadataSearchHit {
        document_id: "01D".to_string(),
        group_id: "01G".to_string(),
        document_path: "datasets/d".to_string(),
        graph_iri: "https://w3id.org/aruna/d".to_string(),
        subject_iri: subject.to_string(),
        score,
        title: subject.to_string(),
        snippet: None,
        subject_types: Vec::new(),
    }
}

fn request(mode: ApiQueryMode, limit: usize, cursor: Option<String>) -> MetadataSearchRequest {
    MetadataSearchRequest {
        auth: None,
        bearer_token: None,
        graph_iris: None,
        query: "reads".to_string(),
        conforms_to: None,
        group_id: None,
        limit: Some(limit),
        cursor,
        mode: Some(mode),
        target_nodes: None,
    }
}

fn started(request: MetadataSearchRequest) -> MetadataSearchOperation {
    let mut search = search_metadata(realm(), node(1), request);
    search.stamp(1_000);
    search
}

/// A cursor for the `reads` query in `mode`, signed by `signer`, resuming `resume`.
fn cursor(mode: ApiQueryMode, signer: u8, resume: Vec<(NodeId, u32)>) -> String {
    let fingerprint = query_fingerprint("reads", None, Some(mode), None, None);
    let watermark = SearchWatermark {
        score: 0.5,
        graph_iri: "https://w3id.org/aruna/d".to_string(),
        subject_iri: "./a".to_string(),
    };
    let key = secret(signer);
    let signed = SearchCursor::new_signed(fingerprint, watermark, resume, key.public(), |bytes| {
        key.sign(bytes)
    });
    signed.unwrap().encode().unwrap()
}

fn fanout(parts: Vec<(NodeId, NodeHits)>) -> IoResult {
    let answer: Result<FanoutParts<NodeHits>, MetadataApiError> =
        Ok((parts, MetadataFanoutStats::default()));
    IoResult::Child(ChildOutcome::new(answer))
}

fn net(event: NetEvent) -> IoResult {
    IoResult::Event(Box::new(Event::Net(event)))
}

#[test]
fn empty_search_refused() {
    let mut search = started(MetadataSearchRequest {
        query: " ".to_string(),
        ..request(ApiQueryMode::Local, 5, None)
    });
    assert!(matches!(search.first(), Err(MetadataApiError::BadRequest)));
}

#[test]
fn bad_profile_refused() {
    let mut search = started(MetadataSearchRequest {
        conforms_to: Some("not an iri".to_string()),
        ..request(ApiQueryMode::Local, 5, None)
    });
    assert!(matches!(search.first(), Err(MetadataApiError::BadRequest)));
}

#[test]
fn short_page_ends() {
    let mut search = started(request(ApiQueryMode::Local, 5, None));
    assert!(matches!(search.first(), Ok(Next::Call(IoCall::Child(_)))));
    let parts = vec![(node(1), (vec![hit("./a", 0.9)], 5))];
    let Ok(Next::Done(page)) = search.next(fanout(parts)) else {
        panic!("expected the last page");
    };
    assert_eq!((page.hits.len(), page.next_cursor), (1, None));
}

#[test]
fn full_page_signs_cursor() {
    let mut search = started(request(ApiQueryMode::Local, 1, None));
    search.first().unwrap();
    let parts = vec![(node(1), (vec![hit("./a", 0.9), hit("./b", 0.8)], 2))];
    let Ok(Next::Call(IoCall::Effect(call))) = search.next(fanout(parts)) else {
        panic!("expected the cursor signature");
    };
    let Effect::Net(NetEffect::CursorSign(bytes)) = *call else {
        panic!("expected a cursor signature");
    };
    assert!(aruna_core::cursor::names_signer(&bytes, node(1).as_bytes()));
    let signed = net(NetEvent::CursorSigned(Some(secret(1).sign(&bytes))));
    let Ok(Next::Done(page)) = search.next(signed) else {
        panic!("expected a page with its cursor");
    };
    assert_eq!(page.hits, vec![hit("./a", 0.9)]);
    let next = page.next_cursor.expect("a cursor");
    assert!(SearchCursor::decode(&next, &[node(1)]).is_ok());
}

#[test]
fn unsigned_cursor_fails() {
    let mut search = started(request(ApiQueryMode::Local, 1, None));
    search.first().unwrap();
    let parts = vec![(node(1), (vec![hit("./a", 0.9), hit("./b", 0.8)], 2))];
    search.next(fanout(parts)).unwrap();
    let failed = search.next(net(NetEvent::CursorSigned(None)));
    assert!(matches!(failed, Err(MetadataApiError::Internal(_))));
}

#[test]
fn cursor_resumes_nodes() {
    let raw = cursor(ApiQueryMode::Distributed, 1, vec![(node(3), 4)]);
    let mut search = started(MetadataSearchRequest {
        target_nodes: Some(vec![node(2)]),
        ..request(ApiQueryMode::Distributed, 5, Some(raw))
    });
    assert!(matches!(search.first(), Ok(Next::Call(IoCall::Child(_)))));
    assert_eq!(search.resume, HashMap::from([(node(3), 4)]));
    assert!(search.watermark.is_some());
}

#[test]
fn foreign_cursor_refused() {
    let raw = cursor(ApiQueryMode::Local, 9, Vec::new());
    let mut search = started(request(ApiQueryMode::Local, 5, Some(raw)));
    assert!(matches!(
        search.first(),
        Err(MetadataApiError::InvalidCursor(_))
    ));
}

#[test]
fn cursor_discovers_signers() {
    let raw = cursor(ApiQueryMode::Distributed, 1, Vec::new());
    let mut search = started(request(ApiQueryMode::Distributed, 5, Some(raw)));
    let Ok(Next::Call(IoCall::Group(group))) = search.first() else {
        panic!("expected the bounded discovery");
    };
    assert_eq!(group.members.len(), 1);
    let late = IoResult::Group(GroupEvent::Finished {
        results: vec![GroupResult::Unfinished],
    });
    assert!(matches!(
        search.next(late),
        Ok(Next::Call(IoCall::Child(_)))
    ));
}

fn search_answer(hits: Vec<MetadataSearchHit>) -> IoResult {
    IoResult::Event(Box::new(Event::Metadata(MetadataEvent::SearchResult {
        hits,
    })))
}

fn profile(call: Result<Next<NodeHits>, MetadataReadError>) -> Option<String> {
    let Ok(Next::Call(IoCall::Effect(effect))) = call else {
        panic!("expected one search");
    };
    let Effect::Metadata(MetadataEffect::SearchGraphs { conforms_to, .. }) = *effect else {
        panic!("expected a local search");
    };
    conforms_to
}

#[test]
fn filtered_search_merges() {
    let ask = SearchAsk {
        graph_iris: None,
        query: "reads".to_string(),
        group_id: None,
        profiles: vec!["urn:a".to_string(), "urn:b".to_string()],
    };
    let mut search = NodeSearchOperation::new(SearchTarget::Local(None), ask, 2, 5_000);
    assert_eq!(profile(search.first()), Some("urn:a".to_string()));
    let second = search.next(search_answer(vec![hit("./a", 0.2)]));
    assert_eq!(profile(second), Some("urn:b".to_string()));
    let answered = search.next(search_answer(vec![hit("./a", 0.7), hit("./b", 0.5)]));
    let Ok(Next::Done((hits, limit))) = answered else {
        panic!("expected the merged hits");
    };
    assert_eq!((hits, limit), (vec![hit("./a", 0.7), hit("./b", 0.5)], 2));
}

#[test]
fn remote_search_asks_peer() {
    let ask = SearchAsk {
        graph_iris: None,
        query: "reads".to_string(),
        group_id: None,
        profiles: Vec::new(),
    };
    let target = SearchTarget::Remote {
        node: node(2),
        auth_token: None,
    };
    let mut search = NodeSearchOperation::new(target, ask, 5, 4_000);
    search.stamp(1_000);
    let Ok(Next::Call(IoCall::Effect(call))) = search.first() else {
        panic!("expected one peer request");
    };
    let Effect::Net(NetEffect::MetadataRequest(request)) = *call else {
        panic!("expected a metadata request");
    };
    assert_eq!(
        (request.peer, request.timeout),
        (node(2), Duration::from_secs(3))
    );
}
