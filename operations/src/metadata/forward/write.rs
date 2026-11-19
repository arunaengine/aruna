//! Defines the error a routed metadata write reports and maps forward failures, device edit
//! verdicts and holder refusals to it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::IoError;
use aruna_core::metadata::MetadataError;
use aruna_core::metadata::protocol::{
    MetadataReadError, MetadataTransportMessage, transport_message_kind,
};
use aruna_core::structs::SyncRefusal;
use thiserror::Error;

use crate::forward::routing::HolderWriteError;
use crate::forward::transport::ForwardError;
use crate::metadata::api::MetadataApiError;
use crate::metadata::create_document::CreateDocumentError;
use crate::metadata::delete_document::DeleteDocumentError;
use crate::metadata::device::edit::DeviceEditError;
use crate::metadata::update_document::UpdateDocumentError;

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
impl From<IoError> for MetadataWriteError {
    fn from(error: IoError) -> Self {
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

pub(crate) fn unexpected_response(response: MetadataTransportMessage) -> MetadataWriteError {
    MetadataWriteError::Undeliverable(format!(
        "unexpected forwarded metadata response: {}",
        transport_message_kind(&response)
    ))
}

impl From<HolderWriteError> for MetadataWriteError {
    fn from(error: HolderWriteError) -> Self {
        match error {
            HolderWriteError::Unauthorized => Self::Unauthorized,
            HolderWriteError::Forbidden => Self::Forbidden,
            HolderWriteError::NotFound => Self::NotFound,
            HolderWriteError::Undeliverable(detail) => Self::Undeliverable(detail),
        }
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

/// An offline edit's verdict, as the metadata routes report it.
pub(super) fn device_edit_error(error: DeviceEditError) -> MetadataWriteError {
    match error {
        DeviceEditError::Invalid(message) => {
            UpdateDocumentError::MetadataError(MetadataError::InvalidInput(message)).into()
        }
        DeviceEditError::NoReplica => MetadataWriteError::NotFound,
        other => MetadataWriteError::Undeliverable(other.to_string()),
    }
}

/// A holder's verdict on a device edit, as the drain classifies it.
pub(super) fn batch_refusal(refusal: SyncRefusal) -> MetadataWriteError {
    match refusal {
        SyncRefusal::Unauthorized => MetadataWriteError::Unauthorized,
        SyncRefusal::Forbidden => MetadataWriteError::Forbidden,
        SyncRefusal::NotFound => MetadataWriteError::NotFound,
        SyncRefusal::Invalid(message) => {
            UpdateDocumentError::MetadataError(MetadataError::InvalidInput(message)).into()
        }
        SyncRefusal::Unavailable => {
            MetadataWriteError::Undeliverable("no holder could apply the device edit".to_string())
        }
        SyncRefusal::Coordinator(node) => {
            MetadataWriteError::Undeliverable(format!("node {node} coordinates this key"))
        }
    }
}
