//! Reads a document's registry record and whether its graph matches that record.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::handle::Handle;
use aruna_core::metadata::MaterializationState;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use ulid::Ulid;

use crate::driver::DriverContext;
use crate::metadata::repository::{
    StorageReadError, parse_registry_read, parse_status_read, read_document_registry,
    read_status_effect,
};

pub async fn load_document_record(
    context: &DriverContext,
    document_id: Ulid,
) -> Result<Option<MetadataRegistryRecord>, StorageReadError> {
    let event = context
        .storage_handle
        .send_effect(read_document_registry(document_id, None))
        .await;
    parse_registry_read(event)
}

pub(crate) async fn record_materialized_read(
    context: &DriverContext,
    record: &MetadataRegistryRecord,
) -> Result<bool, StorageReadError> {
    let event = context
        .storage_handle
        .send_effect(read_status_effect(record.document_id, None))
        .await;
    let Some(status) = parse_status_read(event)? else {
        return Ok(true);
    };
    // Registry rows can replicate ahead of the document event, so only a status
    // recorded for exactly this cursor proves the graph matches the record.
    Ok(status.event_id == record.last_event_id
        && matches!(status.state, MaterializationState::Materialized))
}
