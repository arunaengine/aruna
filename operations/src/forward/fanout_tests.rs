//! Tests the peer fan-out: one effect group, answers in peer order, and refused or late members.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::events::PeerRequestError;

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
    let group = IoCall::Group(EffectGroup {
        members: vec![request(node(1), "one"), request(node(2), "two")],
        limit: 4,
        form: GroupForm::All { deadline: None },
    });
    assert_eq!(operation.first(), Ok(Next::Call(group)));
    let answer = encode_message(&message("fine")).unwrap();
    let results = vec![
        done(node(1), Ok(answer)),
        done(node(2), Err(PeerRequestError::TimedOut)),
    ];
    let finished = IoResult::Group(GroupEvent::Finished { results });
    let Ok(Next::Done(answers)) = operation.next(finished) else {
        panic!("expected the answers");
    };
    assert_eq!(answers[0], (node(1), Ok(message("fine"))));
    assert_eq!(answers[1].0, node(2));
    assert!(answers[1].1.is_err());
}

#[test]
fn late_members_fail() {
    // A member cut off, refused, missing or answered by another peer is an error for that peer.
    let mut operation = fanout();
    operation.first().unwrap();
    let results = vec![GroupResult::Unfinished, done(node(9), Ok(Vec::new()))];
    let Ok(Next::Done(answers)) = operation.next(IoResult::Group(GroupEvent::Finished { results }))
    else {
        panic!("expected the answers");
    };
    assert!(answers.iter().all(|(_, answer)| answer.is_err()));

    let mut operation = fanout();
    operation.first().unwrap();
    let results = vec![GroupResult::Refused];
    let Ok(Next::Done(answers)) = operation.next(IoResult::Group(GroupEvent::Finished { results }))
    else {
        panic!("expected the answers");
    };
    assert!(answers.iter().all(|(_, answer)| answer.is_err()));
}

#[test]
fn no_peers_empty() {
    // Without peers the fan-out ends at once without a group.
    let mut operation = FanoutOperation::new(Vec::new(), 4, Duration::from_secs(1));
    assert_eq!(operation.first(), Ok(Next::Done(Vec::new())));
}

#[test]
fn rejects_other_answers() {
    // Only the group's final answer completes the fan-out.
    let mut operation = fanout();
    operation.first().unwrap();
    let closed = IoResult::Group(GroupEvent::Closed);
    assert_eq!(operation.next(closed), Err(FetchError::Unexpected));
}
