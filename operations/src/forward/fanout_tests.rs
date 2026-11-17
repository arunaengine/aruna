//! Tests the peer fan-out: one effect group, answers in peer order, and refused or late members.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::events::PeerRequestError;
use aruna_core::operation_check::{check_trace, rejects_events, rejects_finalize, sample_events};

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn message(text: &str) -> MetadataTransportMessage {
    MetadataTransportMessage::Reject(text.to_string())
}

fn fanout() -> FanoutOperation {
    let requests = vec![(node(1), message("one")), (node(2), message("two"))];
    FanoutOperation::new(requests, 4, Duration::from_secs(5))
}

fn request(peer: NodeId, text: &str) -> Effect {
    Effect::Net(NetEffect::MetadataRequest(Box::new(
        MetadataRequestEffect {
            peer,
            frame: encode_message(&message(text)).unwrap(),
            timeout: Duration::from_secs(5),
        },
    )))
}

fn done(peer: NodeId, result: Result<Vec<u8>, PeerRequestError>) -> GroupResult {
    GroupResult::Done(Box::new(Event::Net(NetEvent::MetadataAnswer(Box::new(
        MetadataAnswer {
            peer,
            elapsed: Duration::ZERO,
            result,
        },
    )))))
}

#[test]
fn asks_at_once() {
    // Every peer is one member of one group; the answers come back in peer order.
    let mut operation = fanout();
    let group = Effect::Group(EffectGroup {
        members: vec![request(node(1), "one"), request(node(2), "two")],
        limit: 4,
        form: GroupForm::All { deadline: None },
    });
    let answer = encode_message(&message("fine")).unwrap();
    let results = vec![
        done(node(1), Ok(answer)),
        done(node(2), Err(PeerRequestError::TimedOut)),
    ];
    check_trace(
        &mut operation,
        &[group],
        vec![(Event::Group(GroupEvent::Finished { results }), vec![])],
    );
    let answers = operation.finalize().unwrap();
    assert_eq!(answers[0], (node(1), Ok(message("fine"))));
    assert_eq!(answers[1].0, node(2));
    assert!(answers[1].1.is_err());
}

#[test]
fn late_members_fail() {
    // A member cut off, refused, missing or answered by another peer is an error for that peer.
    let mut operation = fanout();
    operation.start();
    let results = vec![GroupResult::Unfinished, done(node(9), Ok(Vec::new()))];
    operation.step(Event::Group(GroupEvent::Finished { results }));
    let answers = operation.finalize().unwrap();
    assert!(answers.iter().all(|(_, answer)| answer.is_err()));

    let mut operation = fanout();
    operation.start();
    operation.step(Event::Group(GroupEvent::Finished {
        results: vec![GroupResult::Refused],
    }));
    assert!(
        operation
            .finalize()
            .unwrap()
            .iter()
            .all(|(_, answer)| answer.is_err())
    );
}

#[test]
fn no_peers_empty() {
    // Without peers the fan-out ends at start without a group.
    let mut operation = FanoutOperation::new(Vec::new(), 4, Duration::from_secs(1));
    assert!(operation.start().is_empty());
    assert_eq!(operation.finalize(), Ok(Vec::new()));
}

#[test]
fn rejects_unexpected_events() {
    // Before start every event is refused, and a started fan-out accepts only its group answer.
    for (label, effects, error) in rejects_events(fanout, sample_events, |_| false) {
        assert!(effects.is_empty(), "{label} emitted effects");
        assert_eq!(error, FetchError::Unexpected, "{label}");
    }
    let mut operation = fanout();
    operation.start();
    operation.step(Event::Group(GroupEvent::Closed));
    assert_eq!(operation.finalize(), Err(FetchError::Unexpected));
}

#[test]
fn abort_and_finalize() {
    // The fan-out only reads: abort has nothing to clean up, and an early finalize fails.
    let mut operation = fanout();
    operation.start();
    assert!(operation.abort().is_empty());
    rejects_finalize(operation);
}
