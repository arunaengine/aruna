//! Forwards a metadata write to holder nodes in rank order and maps their replies to errors.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::metadata::api::MetadataApiError;
use crate::metadata::create_document::CreateDocumentError;
use crate::metadata::delete_document::DeleteDocumentError;
use crate::metadata::message_kind::MetadataRequestDelivery;
use crate::metadata::protocol::MetadataReadError;
use crate::metadata::protocol::MetadataTransportMessage;
use crate::metadata::update_document::UpdateDocumentError;
use aruna_core::calls::IoError;
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

impl From<ForwardError> for MetadataApiError {
    fn from(error: ForwardError) -> Self {
        match error {
            ForwardError::Unauthorized => Self::Unauthorized,
            ForwardError::Forbidden => Self::Forbidden,
            ForwardError::Unavailable => Self::ServiceUnavailable,
        }
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum MetadataWriteError {
    #[error("metadata write requires authentication")]
    Unauthorized,
    #[error("metadata write is forbidden")]
    Forbidden,
    #[error("metadata document not found")]
    NotFound,
    #[error(transparent)]
    Create(#[from] CreateDocumentError),
    #[error(transparent)]
    Update(#[from] UpdateDocumentError),
    #[error(transparent)]
    Delete(#[from] DeleteDocumentError),
    /// The write reached a node that cannot publish it and no holder accepted
    /// the forward. Loud by construction: never accepted, never deferred into an
    /// outbox that can only drain to a topic this node may not join.
    #[error("metadata write is undeliverable: {0}")]
    Undeliverable(String),
}

/// A runner failure leaves the write undelivered.
impl From<aruna_core::calls::IoError> for MetadataWriteError {
    fn from(error: aruna_core::calls::IoError) -> Self {
        Self::Undeliverable(error.to_string())
    }
}

pub(crate) fn write_error(error: MetadataWriteError) -> MetadataApiError {
    match error {
        MetadataWriteError::Unauthorized => MetadataApiError::Unauthorized,
        MetadataWriteError::Forbidden => MetadataApiError::Forbidden,
        MetadataWriteError::NotFound => MetadataApiError::NotFound,
        _ => MetadataApiError::ServiceUnavailable,
    }
}

pub(crate) fn read_error(error: MetadataApiError) -> MetadataReadError {
    match error {
        MetadataApiError::Unauthorized => MetadataReadError::Unauthorized,
        MetadataApiError::Forbidden => MetadataReadError::Forbidden,
        MetadataApiError::NotFound => MetadataReadError::NotFound,
        MetadataApiError::TooLarge => MetadataReadError::TooLarge,
        MetadataApiError::BadRequest
        | MetadataApiError::ServiceUnavailable
        | MetadataApiError::SearchIncomplete(_)
        | MetadataApiError::PlacementUnavailable(_)
        | MetadataApiError::InvalidCursor(_)
        | MetadataApiError::InvalidQuery(_)
        | MetadataApiError::Internal(_) => MetadataReadError::Unavailable,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RetryDisposition {
    TryNext,
    Stop,
}

pub(crate) fn retry_disposition(delivery: MetadataRequestDelivery) -> RetryDisposition {
    match delivery {
        MetadataRequestDelivery::DefinitelyNotSent => RetryDisposition::TryNext,
        MetadataRequestDelivery::PossiblySent => RetryDisposition::Stop,
    }
}

pub(crate) fn unexpected_response(response: MetadataTransportMessage) -> MetadataWriteError {
    MetadataWriteError::Undeliverable(format!(
        "unexpected forwarded metadata response: {}",
        crate::metadata::message_kind::transport_message_kind(&response)
    ))
}

pub(crate) fn reject(error: impl Into<String>) -> MetadataTransportMessage {
    MetadataTransportMessage::Reject(error.into())
}

pub(crate) fn forwarded_unavailable(
    message: &MetadataTransportMessage,
) -> MetadataTransportMessage {
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
pub(crate) fn peer_call(
    peer: aruna_core::NodeId,
    message: &MetadataTransportMessage,
) -> Result<aruna_core::calls::IoCall, String> {
    peer_call_within(peer, message, PEER_DEADLINE)
}

/// [`peer_call`] with an answer due within `timeout`.
pub(crate) fn peer_call_within(
    peer: aruna_core::NodeId,
    message: &MetadataTransportMessage,
    timeout: std::time::Duration,
) -> Result<aruna_core::calls::IoCall, String> {
    let frame = crate::metadata::protocol::encode_message(message).map_err(|error| error.to_string())?;
    let request = aruna_core::effects::MetadataRequestEffect {
        peer,
        frame,
        timeout,
    };
    let effect = aruna_core::effects::Effect::Net(aruna_core::effects::NetEffect::MetadataRequest(Box::new(request)));
    Ok(aruna_core::calls::IoCall::from(effect))
}

/// The peer's decoded answer, or why there is none.
pub(crate) fn peer_answer(
    result: aruna_core::calls::IoResult,
) -> Result<Result<MetadataTransportMessage, String>, aruna_core::calls::IoError> {
    Ok(timed_answer(result)?.0)
}

/// The peer's decoded answer and how long the request took.
pub(crate) fn timed_answer(
    result: aruna_core::calls::IoResult,
) -> Result<(Result<MetadataTransportMessage, String>, std::time::Duration), aruna_core::calls::IoError> {
    let answer = match result.event()? {
        aruna_core::events::Event::Net(aruna_core::events::NetEvent::MetadataAnswer(answer)) => answer,
        other => {
            return Err(aruna_core::calls::IoError::Unexpected {
                expected: "Event::Net(NetEvent::MetadataAnswer)",
                got: other.label(),
            });
        }
    };
    let decoded = match answer.result {
        Ok(bytes) => crate::metadata::protocol::decode_message(&bytes).map_err(|error| error.to_string()),
        Err(error) => Err(format!("{error:?}")),
    };
    Ok((decoded, answer.elapsed))
}

#[cfg(test)]
#[path = "peer_call_tests.rs"]
mod peer_call_tests;
