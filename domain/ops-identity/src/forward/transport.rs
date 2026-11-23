//! Sends encoded metadata requests to peers and builds the refusals forwarded requests answer.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::IoError;
use aruna_core::metadata::protocol::{
    MetadataReadError, MetadataTransportMessage, decode_message, encode_message, metadata_request,
};
use thiserror::Error;

/// Why a request forwarded to peers ended without being accepted.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum ForwardError {
    #[error("the forwarded request needs authentication")]
    Unauthorized,
    #[error("the forwarded request is forbidden")]
    Forbidden,
    #[error("no peer accepted the forwarded request")]
    Unavailable,
}

impl From<IoError> for ForwardError {
    fn from(_: IoError) -> Self {
        Self::Unavailable
    }
}

pub fn reject(error: impl Into<String>) -> MetadataTransportMessage {
    MetadataTransportMessage::Reject(error.into())
}

pub fn forwarded_unavailable(message: &MetadataTransportMessage) -> MetadataTransportMessage {
    match message {
        MetadataTransportMessage::ForwardReadDocument { .. } => {
            MetadataTransportMessage::ForwardedRead {
                result: Err(MetadataReadError::Unavailable),
            }
        }
        MetadataTransportMessage::ForwardValidationStatus { .. } => {
            MetadataTransportMessage::ForwardedValidationStatus {
                result: Err(MetadataReadError::Unavailable),
            }
        }
        MetadataTransportMessage::ForwardCreateDocument { .. }
        | MetadataTransportMessage::ForwardUpdateDocument { .. }
        | MetadataTransportMessage::ForwardDeleteDocument { .. }
        | MetadataTransportMessage::ForwardTokenRevocation { .. } => {
            MetadataTransportMessage::ForwardedWriteUnavailable
        }
        _ => reject("unexpected forwarded metadata message"),
    }
}

/// Upper bound of one request; the transport's own step limits end it earlier.
const PEER_DEADLINE: std::time::Duration = std::time::Duration::from_secs(60);

/// The call that sends `message` to `peer`, or why it does not encode.
pub fn peer_call(
    peer: aruna_core::id::NodeId,
    message: &MetadataTransportMessage,
) -> Result<aruna_core::calls::IoCall, String> {
    peer_call_within(peer, message, PEER_DEADLINE)
}

/// [`peer_call`] with an answer due within `timeout`.
pub fn peer_call_within(
    peer: aruna_core::id::NodeId,
    message: &MetadataTransportMessage,
    timeout: std::time::Duration,
) -> Result<aruna_core::calls::IoCall, String> {
    let frame = encode_message(message).map_err(|error| error.to_string())?;
    let request = metadata_request(peer, frame, timeout);
    let effect = aruna_core::effects::Effect::Net(aruna_core::effects::NetEffect::MetadataRequest(
        Box::new(request),
    ));
    Ok(aruna_core::calls::IoCall::from(effect))
}

/// The peer's decoded answer, or why there is none.
pub fn peer_answer(
    result: aruna_core::calls::IoResult,
) -> Result<Result<MetadataTransportMessage, String>, aruna_core::calls::IoError> {
    Ok(timed_answer(result)?.0)
}

/// Whether a peer request failed because this node has no metadata handle to send it with.
pub fn no_peer_transport(result: &aruna_core::calls::IoResult) -> bool {
    use aruna_core::events::{Event, NetEvent, PeerRequestError};
    use aruna_core::metadata::MetadataError;
    let aruna_core::calls::IoResult::Event(event) = result else {
        return false;
    };
    let Event::Net(NetEvent::MetadataAnswer(answer)) = &**event else {
        return false;
    };
    matches!(
        answer.result,
        Err(PeerRequestError::Failed(MetadataError::HandleMissing))
    )
}

/// The peer's decoded answer and how long the request took.
pub fn timed_answer(
    result: aruna_core::calls::IoResult,
) -> Result<
    (
        Result<MetadataTransportMessage, String>,
        std::time::Duration,
    ),
    aruna_core::calls::IoError,
> {
    let answer = match result.event()? {
        aruna_core::events::Event::Net(aruna_core::events::NetEvent::MetadataAnswer(answer)) => {
            answer
        }
        other => {
            return Err(aruna_core::calls::IoError::Unexpected {
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
#[path = "peer_call_tests.rs"]
mod peer_call_tests;
