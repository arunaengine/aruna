//! Tests one metadata peer request as a call and its answer.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoResult};
use aruna_core::effects::{Effect, MetadataRequestEffect, NetEffect};
use aruna_core::events::{Event, MetadataAnswer, NetEvent, PeerRequestError};

use super::*;
use crate::metadata::protocol::encode_message;

fn peer() -> NodeId {
    iroh::SecretKey::from_bytes(&[1; 32]).public()
}

fn message() -> MetadataTransportMessage {
    reject("refused")
}

fn answered(result: Result<Vec<u8>, PeerRequestError>) -> IoResult {
    let answer = MetadataAnswer {
        peer: peer(),
        elapsed: Duration::ZERO,
        result,
    };
    let event = Event::Net(NetEvent::MetadataAnswer(Box::new(answer)));
    IoResult::Event(Box::new(event))
}

// The message goes out encoded, and the encoded answer comes back decoded.
#[test]
fn carries_encoded_message() {
    let frame = encode_message(&message()).unwrap();
    let request = MetadataRequestEffect {
        peer: peer(),
        frame: frame.clone(),
        timeout: PEER_DEADLINE,
    };
    let effect = Effect::Net(NetEffect::MetadataRequest(Box::new(request)));
    assert_eq!(peer_call(peer(), &message()), Ok(IoCall::from(effect)));
    assert_eq!(peer_answer(answered(Ok(frame))), Ok(Ok(message())));
}

// A failed request and undecodable bytes are answers without a message.
#[test]
fn reports_missing_answer() {
    let failed = answered(Err(PeerRequestError::TimedOut));
    assert!(matches!(peer_answer(failed), Ok(Err(_))));
    assert!(matches!(peer_answer(answered(Ok(vec![0xFF]))), Ok(Err(_))));
    assert!(peer_answer(IoResult::Done).is_err());
}
