//! Tests the answers to other nodes' plugin requests as calls and results.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::plugins::fixtures::{self, is_child, node};
use crate::plugins::rows::tests::child_result;

fn place() -> Option<ServingNode> {
    Some((fixtures::realm(), node(1)))
}

fn clock(serving: Option<ServingNode>) -> AnswerPeer {
    let probe = MetadataTransportMessage::ProbeClock;
    AnswerPeer::new(serving, node(3), probe, 7).unwrap()
}

#[test]
fn other_messages_returned() {
    let message = MetadataTransportMessage::ClockProbed { result: Ok(1) };
    let back = AnswerPeer::new(place(), node(3), message, 7);
    assert!(matches!(
        back,
        Err(MetadataTransportMessage::ClockProbed { .. })
    ));
}

#[test]
fn clock_answers_child() {
    let mut answer = clock(place());
    assert_eq!(answer.answer(), PeerAnswer::Clock);
    assert!(is_child(&answer.first()));
    let served = child_result(Ok::<u64, MetadataReadError>(7));
    assert!(matches!(
        answer.next(served),
        Ok(Next::Done(MetadataTransportMessage::ClockProbed {
            result: Ok(7)
        }))
    ));
}

#[test]
fn no_network_unavailable() {
    assert!(matches!(
        clock(None).first(),
        Ok(Next::Done(MetadataTransportMessage::ClockProbed {
            result: Err(MetadataReadError::Unavailable)
        }))
    ));
}

#[test]
fn lost_child_unavailable() {
    let mut answer = clock(place());
    answer.first().unwrap();
    assert!(matches!(
        answer.next(IoResult::Done),
        Ok(Next::Done(MetadataTransportMessage::ClockProbed {
            result: Err(MetadataReadError::Unavailable)
        }))
    ));
}
