//! Builds the answers peers give to encoded metadata requests.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::IoResult;
use aruna_core::events::{Event, MetadataAnswer, NetEvent, PeerRequestError};
use aruna_core::id::NodeId;
use aruna_core::metadata::protocol::{MetadataTransportMessage, encode_message};
use std::time::Duration;

/// A metadata answer from `peer` with `result`.
pub fn answer(peer: NodeId, result: Result<Vec<u8>, PeerRequestError>) -> IoResult {
    let answer = MetadataAnswer {
        peer,
        elapsed: Duration::ZERO,
        result,
    };
    IoResult::Event(Box::new(Event::Net(NetEvent::MetadataAnswer(Box::new(
        answer,
    )))))
}

/// `peer` answered with `message`.
pub fn peer_answer(peer: NodeId, message: &MetadataTransportMessage) -> IoResult {
    answer(peer, Ok(encode_message(message).expect("message encodes")))
}

/// The request to `peer` ended without an answer.
pub fn failed_answer(peer: NodeId, error: PeerRequestError) -> IoResult {
    answer(peer, Err(error))
}
