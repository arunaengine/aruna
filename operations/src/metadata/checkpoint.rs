//! Opens a new metadata history window once the current one fills up.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::calls::IoError;
use aruna_core::metadata::{EVENT_LIMIT, RAW_BYTES_LIMIT};
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use thiserror::Error;

use crate::driver::{DriverContext, drive};
use crate::metadata::checkpoint_window::{CheckpointWindowOperation, due};
use crate::metadata::update_document::{
    UpdateDocumentConfig, UpdateDocumentError, UpdateDocumentMutation, UpdateDocumentOperation,
};
use crate::metadata::update_entry::update_metadata_document;

#[derive(Debug, Error)]
pub enum CheckpointError {
    #[error(transparent)]
    Window(#[from] IoError),
    #[error(transparent)]
    Update(#[from] UpdateDocumentError),
}

/// Checks the document's current window after an event materialized here and writes a
/// checkpoint when it is due. Nodes that are not origins of the document never write one.
pub(crate) async fn after_materialization(
    context: &DriverContext,
    record: &MetadataRegistryRecord,
) -> Result<(), CheckpointError> {
    let Some(node_id) = context.net_handle.as_ref().map(|net| net.node_id()) else {
        return Ok(());
    };
    let Some(position) = record
        .holder_node_ids
        .iter()
        .position(|holder| *holder == node_id)
    else {
        return Ok(());
    };
    let origins = record.holder_node_ids.len() as u64;
    let share = (
        u64::from(EVENT_LIMIT - 1) / origins,
        RAW_BYTES_LIMIT / origins,
    );
    let used = drive(CheckpointWindowOperation::new(record.document_id), context).await?;
    if !due(used, share, position == 0) {
        return Ok(());
    }
    let operation = UpdateDocumentOperation::new(UpdateDocumentConfig {
        actor: Actor {
            node_id,
            user_id: UserId::nil(record.realm_id),
            realm_id: record.realm_id,
        },
        group_id: record.group_id,
        document_id: record.document_id,
        public: record.public,
        mutation: UpdateDocumentMutation::Checkpoint,
        expected_revision: None,
    });
    update_metadata_document(operation, context).await?;
    Ok(())
}
