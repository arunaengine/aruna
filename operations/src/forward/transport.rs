//! Forwards a metadata write to holder nodes in rank order and maps their replies to errors.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::metadata::api::MetadataApiError;
use crate::metadata::create_document::CreateDocumentError;
use crate::metadata::delete_document::DeleteDocumentError;
use crate::metadata::handle::MetadataRequestDelivery;
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


#[derive(Debug, Error)]
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
        crate::metadata::handle::transport_message_kind(&response)
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
