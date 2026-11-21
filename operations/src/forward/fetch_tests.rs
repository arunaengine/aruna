//! Tests the holder fetch with fixed answers: order, deadline budgets, refusals and errors.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::metadata::MetadataError;
use aruna_core::operation_check::sample_events;

#[test]
fn frame_debug_hidden() {
    let frame = PeerFrame(vec![201; 32]);
    assert!(!format!("{frame:?}").contains("201, 201"));
}

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn request() -> MetadataTransportMessage {
    MetadataTransportMessage::Reject("ask".to_string())
}

/// Reads a `Reject` text as the answer kind, so tests need no real record.
impl HolderRecord for String {
    const LABEL: &'static str = "test";

    fn read(message: MetadataTransportMessage) -> Answer<String> {
        match message {
            MetadataTransportMessage::Reject(text) if text == "missing" => Answer::Missing,
            MetadataTransportMessage::Reject(text) if text == "denied" => Answer::Denied,
            MetadataTransportMessage::Reject(text) if text.starts_with("found") => {
                Answer::Found(text)
            }
            _ => Answer::Skip,
        }
    }
}

fn fetch(budget: Budget) -> HolderFetchOperation<String> {
    let holders = [node(1), node(2), node(3)];
    HolderFetchOperation::new(&holders, &request(), Duration::from_secs(9), budget)
}

fn asked(peer: NodeId, timeout: Duration) -> FetchTurn<String> {
    let frame = encode_message(&request()).unwrap();
    Ok(Next::Call(peer_call(peer, frame, timeout)))
}

fn done(outcome: Fetched<String>) -> FetchTurn<String> {
    Ok(Next::Done(outcome))
}

fn answer(peer: NodeId, seconds: u64, text: &str) -> IoResult {
    let frame = encode_message(&MetadataTransportMessage::Reject(text.to_string())).unwrap();
    reply(peer, seconds, Ok(frame))
}

fn reply(peer: NodeId, seconds: u64, result: Result<Vec<u8>, PeerRequestError>) -> IoResult {
    let event = Event::Net(NetEvent::MetadataAnswer(Box::new(MetadataAnswer {
        peer,
        elapsed: Duration::from_secs(seconds),
        result,
    })));
    IoResult::Event(Box::new(event))
}

fn down() -> Result<Vec<u8>, PeerRequestError> {
    Err(PeerRequestError::Failed(MetadataError::Backend(
        "down".into(),
    )))
}

fn seconds(count: u64) -> Duration {
    Duration::from_secs(count)
}

#[test]
fn asks_in_order() {
    // A miss and a useless answer move on with the time left; the first record wins.
    let mut operation = fetch(Budget::Shared);
    assert_eq!(operation.first(), asked(node(1), seconds(9)));
    let missing = operation.next(answer(node(1), 2, "missing"));
    assert_eq!(missing, asked(node(2), seconds(7)));
    let other = operation.next(answer(node(2), 3, "other"));
    assert_eq!(other, asked(node(3), seconds(4)));
    assert_eq!(
        operation.next(answer(node(3), 1, "found it")),
        done(Fetched::Found {
            holder: node(3),
            value: "found it".to_string(),
        })
    );
}

#[test]
fn misses_not_found() {
    // Only a fetch where nobody answered is unavailable; answered misses are a miss.
    let mut operation = fetch(Budget::Shared);
    operation.first().unwrap();
    operation.next(answer(node(1), 1, "missing")).unwrap();
    operation.next(reply(node(2), 1, down())).unwrap();
    let last = operation.next(answer(node(3), 1, "missing"));
    assert_eq!(last, done(Fetched::NotFound));

    let mut operation = fetch(Budget::Shared);
    operation.first().unwrap();
    operation.next(reply(node(1), 1, down())).unwrap();
    operation.next(reply(node(2), 1, down())).unwrap();
    assert_eq!(
        operation.next(reply(node(3), 1, down())),
        done(Fetched::Unavailable("no test holder answered".to_string()))
    );
}

#[test]
fn denial_stops_fetch() {
    // A refusal ends the fetch at once; no later holder is asked.
    let mut operation = fetch(Budget::Split);
    operation.first().unwrap();
    let denied = operation.next(answer(node(1), 1, "denied"));
    assert_eq!(denied, done(Fetched::Denied));
}

#[test]
fn budgets_spend_deadline() {
    // A shared budget ends on a timeout; a split budget gives each holder its share and moves on.
    let mut shared = fetch(Budget::Shared);
    shared.first().unwrap();
    assert_eq!(
        shared.next(reply(node(1), 9, Err(PeerRequestError::TimedOut))),
        done(Fetched::Unavailable("no test holder answered".to_string()))
    );

    let mut split = fetch(Budget::Split);
    assert_eq!(split.first(), asked(node(1), seconds(3)));
    let timed_out = split.next(reply(node(1), 3, Err(PeerRequestError::TimedOut)));
    assert_eq!(timed_out, asked(node(2), seconds(3)));
    let other = split.next(answer(node(2), 1, "other"));
    assert_eq!(other, asked(node(3), seconds(5)));
    let missing = split.next(answer(node(3), 5, "missing"));
    assert_eq!(missing, done(Fetched::NotFound));

    // A spent deadline asks nobody else.
    let mut spent = fetch(Budget::Shared);
    spent.first().unwrap();
    let missing = spent.next(answer(node(1), 9, "missing"));
    assert_eq!(missing, done(Fetched::NotFound));
}

#[test]
fn no_holders_unavailable() {
    // Without holders the fetch ends at once without a request.
    let mut operation = HolderFetchOperation::<String>::new(
        &[],
        &request(),
        Duration::from_secs(1),
        Budget::Shared,
    );
    assert!(matches!(
        operation.first(),
        Ok(Next::Done(Fetched::Unavailable(_)))
    ));
}

#[test]
fn rejects_unexpected_events() {
    // Before the first call, for another result kind and for an answer from a holder that was
    // not asked, the fetch fails.
    for event in sample_events() {
        let label = event.label();
        let mut operation = fetch(Budget::Shared);
        let refused = operation.next(IoResult::Event(Box::new(event)));
        assert_eq!(refused, Err(FetchError::Unexpected), "{label}");
    }
    let mut operation = fetch(Budget::Shared);
    operation.first().unwrap();
    assert_eq!(operation.next(IoResult::Woke), Err(FetchError::Unexpected));
    let mut operation = fetch(Budget::Shared);
    operation.first().unwrap();
    let stranger = operation.next(answer(node(2), 1, "found"));
    assert_eq!(stranger, Err(FetchError::Unexpected));
}

#[test]
fn abort_cleans_nothing() {
    // A fetch only reads, so a stopped fetch has nothing to clean up.
    let mut operation = fetch(Budget::Shared);
    operation.first().unwrap();
    assert!(operation.abort().is_empty());
}
