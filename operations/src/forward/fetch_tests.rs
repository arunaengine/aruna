//! Tests the holder fetch with fixed answers: order, deadline budgets, refusals and errors.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::operation_check::{check_trace, rejects_events, rejects_finalize, sample_events};

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

fn asked(peer: NodeId, timeout: Duration) -> Effect {
    Effect::Net(NetEffect::MetadataRequest(Box::new(
        MetadataRequestEffect {
            peer,
            frame: encode_message(&request()).unwrap(),
            timeout,
        },
    )))
}

fn answer(peer: NodeId, seconds: u64, text: &str) -> Event {
    let frame = encode_message(&MetadataTransportMessage::Reject(text.to_string())).unwrap();
    reply(peer, seconds, Ok(frame))
}

fn reply(peer: NodeId, seconds: u64, result: Result<Vec<u8>, PeerRequestError>) -> Event {
    Event::Net(NetEvent::MetadataAnswer(Box::new(MetadataAnswer {
        peer,
        elapsed: Duration::from_secs(seconds),
        result,
    })))
}

#[test]
fn asks_holders_in_order() {
    // A miss and a useless answer move on with the time left; the first record wins.
    let mut operation = fetch(Budget::Shared);
    check_trace(
        &mut operation,
        &[asked(node(1), Duration::from_secs(9))],
        vec![
            (
                answer(node(1), 2, "missing"),
                vec![asked(node(2), Duration::from_secs(7))],
            ),
            (
                answer(node(2), 3, "other"),
                vec![asked(node(3), Duration::from_secs(4))],
            ),
            (answer(node(3), 1, "found it"), vec![]),
        ],
    );
    assert_eq!(
        operation.finalize(),
        Ok(Fetched::Found {
            holder: node(3),
            value: "found it".to_string(),
        })
    );
}

#[test]
fn misses_are_not_found() {
    // Only a fetch where nobody answered is unavailable; answered misses are a miss.
    let mut operation = fetch(Budget::Shared);
    operation.start();
    operation.step(answer(node(1), 1, "missing"));
    operation.step(reply(
        node(2),
        1,
        Err(PeerRequestError::Failed("down".into())),
    ));
    operation.step(answer(node(3), 1, "missing"));
    assert_eq!(operation.finalize(), Ok(Fetched::NotFound));

    let mut operation = fetch(Budget::Shared);
    operation.start();
    for peer in [node(1), node(2), node(3)] {
        operation.step(reply(peer, 1, Err(PeerRequestError::Failed("down".into()))));
    }
    assert_eq!(
        operation.finalize(),
        Ok(Fetched::Unavailable("no test holder answered".to_string()))
    );
}

#[test]
fn denial_stops_fetch() {
    // A refusal ends the fetch at once; no later holder is asked.
    let mut operation = fetch(Budget::Split);
    operation.start();
    assert!(operation.step(answer(node(1), 1, "denied")).is_empty());
    assert_eq!(operation.finalize(), Ok(Fetched::Denied));
}

#[test]
fn budgets_spend_deadline() {
    // A shared budget ends on a timeout; a split budget gives each holder its share and moves on.
    let mut shared = fetch(Budget::Shared);
    shared.start();
    assert!(
        shared
            .step(reply(node(1), 9, Err(PeerRequestError::TimedOut)))
            .is_empty()
    );
    assert_eq!(
        shared.finalize(),
        Ok(Fetched::Unavailable("no test holder answered".to_string()))
    );

    let mut split = fetch(Budget::Split);
    check_trace(
        &mut split,
        &[asked(node(1), Duration::from_secs(3))],
        vec![
            (
                reply(node(1), 3, Err(PeerRequestError::TimedOut)),
                vec![asked(node(2), Duration::from_secs(3))],
            ),
            (
                answer(node(2), 1, "other"),
                vec![asked(node(3), Duration::from_secs(5))],
            ),
            (answer(node(3), 5, "missing"), vec![]),
        ],
    );
    assert_eq!(split.finalize(), Ok(Fetched::NotFound));

    // A spent deadline asks nobody else.
    let mut spent = fetch(Budget::Shared);
    spent.start();
    assert!(spent.step(answer(node(1), 9, "missing")).is_empty());
    assert_eq!(spent.finalize(), Ok(Fetched::NotFound));
}

#[test]
fn no_holders_unavailable() {
    // Without holders the fetch ends at start without a request.
    let mut operation = HolderFetchOperation::<String>::new(
        &[],
        &request(),
        Duration::from_secs(1),
        Budget::Shared,
    );
    assert!(operation.start().is_empty());
    assert!(operation.is_complete());
    assert!(matches!(operation.finalize(), Ok(Fetched::Unavailable(_))));
}

#[test]
fn rejects_unexpected_events() {
    // Before start, and for an answer from a holder that was not asked, the fetch fails.
    let make = || fetch(Budget::Shared);
    for (label, effects, error) in rejects_events(make, sample_events, |_| false) {
        assert!(effects.is_empty(), "{label} emitted effects");
        assert_eq!(error, FetchError::Unexpected, "{label}");
    }
    let mut operation = fetch(Budget::Shared);
    operation.start();
    assert!(operation.step(answer(node(2), 1, "found")).is_empty());
    assert_eq!(operation.finalize(), Err(FetchError::Unexpected));
}

#[test]
fn abort_and_finalize() {
    // A fetch only reads: abort has nothing to clean up, and an early finalize fails.
    let mut operation = fetch(Budget::Shared);
    operation.start();
    assert!(operation.abort().is_empty());
    rejects_finalize(operation);
}
