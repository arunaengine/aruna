//! Maps device edit verdicts and holder refusals to the errors routed metadata writes report.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::device::edit::DeviceEditError;
use crate::metadata::update_document::UpdateDocumentError;
use aruna_core::metadata::MetadataError;
use aruna_core::structs::SyncRefusal;

use crate::forward::transport::MetadataWriteError;

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
