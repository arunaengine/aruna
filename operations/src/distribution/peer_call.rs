//! One request of the metadata peer protocol as an operation call, and its answer.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult};
use aruna_core::effects::{Effect, MetadataRequestEffect, NetEffect};
use aruna_core::events::{Event, NetEvent};

use crate::metadata::protocol::{MetadataTransportMessage, decode_message, encode_message};

/// Upper bound of one request; the transport's own step limits end it earlier.
const PEER_DEADLINE: Duration = Duration::from_secs(60);

/// The call that sends `message` to `peer`, or why it does not encode.
pub(crate) fn peer_call(
    peer: NodeId,
    message: &MetadataTransportMessage,
) -> Result<IoCall, String> {
    peer_call_within(peer, message, PEER_DEADLINE)
}

/// [`peer_call`] with an answer due within `timeout`.
pub(crate) fn peer_call_within(
    peer: NodeId,
    message: &MetadataTransportMessage,
    timeout: Duration,
) -> Result<IoCall, String> {
    let frame = encode_message(message).map_err(|error| error.to_string())?;
    let request = MetadataRequestEffect {
        peer,
        frame,
        timeout,
    };
    let effect = Effect::Net(NetEffect::MetadataRequest(Box::new(request)));
    Ok(IoCall::from(effect))
}

/// The peer's decoded answer, or why there is none.
pub(crate) fn peer_answer(
    result: IoResult,
) -> Result<Result<MetadataTransportMessage, String>, IoError> {
    Ok(timed_answer(result)?.0)
}

/// The peer's decoded answer and how long the request took.
pub(crate) fn timed_answer(
    result: IoResult,
) -> Result<(Result<MetadataTransportMessage, String>, Duration), IoError> {
    let answer = match result.event()? {
        Event::Net(NetEvent::MetadataAnswer(answer)) => answer,
        other => {
            return Err(IoError::Unexpected {
                expected: "Event::Net(NetEvent::MetadataAnswer)",
                got: other.label(),
            });
        }
    };
    let decoded = match answer.result {
        Ok(bytes) => decode_message(&bytes).map_err(|error| error.to_string()),
        Err(error) => Err(format!("{error:?}")),
    };
    Ok((decoded, answer.elapsed))
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::events::{MetadataAnswer, PeerRequestError};

    fn peer() -> NodeId {
        iroh::SecretKey::from_bytes(&[1; 32]).public()
    }

    fn message() -> MetadataTransportMessage {
        crate::forward::transport::reject("refused")
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
}
