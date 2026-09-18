//! Forwards a metadata write to holder nodes in rank order and maps their replies to errors.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::routing::distinct_holders;
use crate::driver::DriverContext;
use crate::metadata::api::MetadataApiError;
use crate::metadata::create_document::CreateDocumentError;
use crate::metadata::delete_document::DeleteDocumentError;
use crate::metadata::handle::MetadataRequestDelivery;
use crate::metadata::protocol::MetadataReadError;
use crate::metadata::protocol::MetadataTransportMessage;
use crate::metadata::protocol::WriteAuthError;
use crate::metadata::update_document::UpdateDocumentError;
use aruna_core::NodeId;
use std::sync::Arc;
use thiserror::Error;
use tracing::error;
use tracing::warn;

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
        MetadataApiError::BadRequest
        | MetadataApiError::ServiceUnavailable
        | MetadataApiError::PlacementUnavailable(_)
        | MetadataApiError::InvalidCursor(_)
        | MetadataApiError::Internal(_) => MetadataReadError::Unavailable,
    }
}

pub(crate) async fn forward_to_holders(
    context: &Arc<DriverContext>,
    holders: &[NodeId],
    message: MetadataTransportMessage,
    local_miss: Option<NodeId>,
    local_capacity: bool,
) -> Result<MetadataTransportMessage, MetadataWriteError> {
    let Some(metadata_handle) = context.metadata_handle.as_ref() else {
        return Err(MetadataWriteError::Undeliverable(
            "no metadata handle to forward with".to_string(),
        ));
    };
    let holders = distinct_holders(holders);
    let local_node_id = local_miss.or_else(|| context.net_handle.as_ref().map(|net| net.node_id()));
    let tracks_not_found = matches!(
        &message,
        MetadataTransportMessage::ForwardUpdateDocument { .. }
            | MetadataTransportMessage::ForwardDeleteDocument { .. }
            | MetadataTransportMessage::ForwardPersistentId { .. }
    );

    let mut failures: Vec<String> = Vec::new();
    let mut not_found = usize::from(local_miss.is_some_and(|local| holders.contains(&local)));
    let mut capacity =
        usize::from(local_capacity && local_miss.is_some_and(|local| holders.contains(&local)));
    for holder in holders
        .iter()
        .filter(|holder| Some(**holder) != local_node_id)
    {
        match metadata_handle
            .request_forwarded_write(*holder, message.clone())
            .await
        {
            Ok(MetadataTransportMessage::ForwardedWriteDenied {
                error: WriteAuthError::Unauthorized,
            }) => return Err(MetadataWriteError::Unauthorized),
            Ok(MetadataTransportMessage::ForwardedWriteDenied {
                error: WriteAuthError::Forbidden,
            }) => return Err(MetadataWriteError::Forbidden),
            Ok(MetadataTransportMessage::WriteNotFound) if tracks_not_found => {
                not_found += 1;
            }
            Ok(MetadataTransportMessage::WriteNotFound) => {
                failures.push(format!(
                    "{holder}: holder returned not found for a forwarded create"
                ));
            }
            Ok(MetadataTransportMessage::ForwardedWriteUnavailable) => {
                failures.push(format!("{holder}: holder placement view is unavailable"));
            }
            Ok(MetadataTransportMessage::MetadataHistoryCapacity)
                if matches!(
                    &message,
                    MetadataTransportMessage::ForwardUpdateDocument { .. }
                ) =>
            {
                capacity += 1;
            }
            Ok(MetadataTransportMessage::MetadataHistoryCapacity) => {
                failures.push(format!(
                    "{holder}: holder returned metadata history capacity for a non-update"
                ));
            }
            Ok(MetadataTransportMessage::Reject(error)) => {
                warn!(holder = %holder, error = %error, "Holder rejected a forwarded metadata write");
                return Err(MetadataWriteError::Undeliverable(format!(
                    "holder `{holder}` rejected the forwarded metadata write; refusing to replay it: {error}"
                )));
            }
            Ok(response) => return Ok(response),
            Err(error) => {
                warn!(holder = %holder, error = %error, "Failed to forward a metadata write to holder");
                if retry_disposition(error.delivery()) == RetryDisposition::Stop {
                    return Err(MetadataWriteError::Undeliverable(format!(
                        "forward to holder `{holder}` may have applied the metadata write before failing; refusing to replay it: {error}"
                    )));
                }
                failures.push(format!("{holder}: {error}"));
            }
        }
    }

    if !holders.is_empty() && capacity == holders.len() {
        return Err(MetadataWriteError::Undeliverable(
            "metadata history capacity reached on every holder".to_string(),
        ));
    }

    if tracks_not_found && !holders.is_empty() && not_found == holders.len() {
        return Err(MetadataWriteError::NotFound);
    }

    let detail = if failures.is_empty() {
        "the document's bucket has no reachable holder".to_string()
    } else {
        failures.join("; ")
    };
    error!(
        holders = holders.len(),
        detail = %detail,
        "Metadata write reached a non-holder and no holder accepted the forward"
    );
    Err(MetadataWriteError::Undeliverable(detail))
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
